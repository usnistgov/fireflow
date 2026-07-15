use fireflow_core::api;
use fireflow_core::config::{self as cfg, ByteordOverride, FixIntWidths, HasStrategy as _};
use fireflow_core::core::AnyCoreDataset;
use fireflow_core::segment::read::OffsetsCorrection;
use fireflow_core::selector::{AppendableSelector, Selector};
use fireflow_core::text::byteord::Bytes;
use fireflow_core::text::keywords::{AlphaNumType, ByteOrd2_0, Timestep};
use fireflow_core::validated::ascii_range::OtherWidth;
use fireflow_core::validated::datepattern::DatePattern;
use fireflow_core::validated::keys::{KeyString, KeyStringOrPattern};
use fireflow_core::validated::read_state::DatasetOffset;
use fireflow_core::validated::sub_pattern::SubPattern;
use fireflow_core::validated::textdelim::TEXTDelim;
use fireflow_core::validated::timepattern::TimePattern;
use fireflow_types::config as tc;
use fireflow_types::keywords as tk;
use fireflow_types::nonempty_string::NEString;

use ansi_term::{ANSIString, Style};
use clap::{
    Arg, ArgAction, ArgMatches, Command,
    builder::{IntoResettable, StyledStr, ValueParser},
    error::ErrorKind,
    value_parser,
};
use const_format::str_replace;
use hashbrown::HashMap;
use itertools::Itertools as _;
use itoa::Buffer as IBuf;
use regex::Regex;
use serde::Serialize;
use serde_json::{json, to_writer};
use zmij::Buffer as FBuf;

use std::collections::HashSet;
use std::error::Error;
use std::fmt::Display;
use std::io::{self, Write};
use std::iter::once;
use std::path::PathBuf;
use std::process::ExitCode;
use std::str::FromStr;

/// Helper macro to keep args in sync with config fields
macro_rules! cli_arg {
    ($conf:ident :: $field:ident) => {{
        // compile-time existence check; fails if field is renamed
        const _: usize = std::mem::offset_of!(fireflow_core::config::$conf, $field);
        str_replace!(stringify!($field), "_", "-")
    }};
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}

#[allow(clippy::too_many_lines)]
fn run() -> AppResult<()> {
    let mut stdout = io::BufWriter::new(io::stdout().lock());
    let mut stderr = io::BufWriter::new(io::stderr().lock());

    let kw_style = Style::new().italic();
    let seg_style = Style::new().italic();
    let arg_style = Style::new().bold();

    let header_seg = seg_style.paint("HEADER");
    let text_seg = seg_style.paint("TEXT");
    let prim_text_seg = seg_style.paint("primary TEXT");
    let supp_text_seg = seg_style.paint("supplemental TEXT");
    let data_seg = seg_style.paint("DATA");
    let analysis_seg = seg_style.paint("ANALYSIS");
    let other_seg = seg_style.paint("OTHER");

    let fmt_arg = |arg| arg_style.paint(format!("--{arg}"));

    let par = kw_style.paint(tk::PAR);
    let tot = kw_style.paint(tk::TOT);
    let byteord = kw_style.paint(tk::BYTEORD);
    let datatype = kw_style.paint(tk::DATATYPE);
    let timestep = kw_style.paint(tk::TIMESTEP);
    let date = kw_style.paint(tk::DATE);
    let btim = kw_style.paint(tk::BTIM);
    let etim = kw_style.paint(tk::ETIM);
    let last_modified = kw_style.paint(tk::LAST_MODIFIED);
    let begindatetime = kw_style.paint(tk::BEGINDATETIME);
    let enddatetime = kw_style.paint(tk::ENDDATETIME);
    let nextdata = kw_style.paint(tk::NEXTDATA);
    let spillover = kw_style.paint(tk::SPILLOVER);
    let pnfeature = kw_style.paint(tk::PNFEATURE);
    let pn_b = kw_style.paint(tk::PNB);
    let pn_r = kw_style.paint(tk::PNR);
    let pn_n = kw_style.paint(tk::PNN);
    let pn_e = kw_style.paint(tk::PNE);
    let pndatatype = kw_style.paint(tk::PNDATATYPE);

    let pn_any = kw_style.paint("$PnX");

    let (delim_header, delim_help) = format_section(
        "DELIMITER ESCAPING",
        [
            "The standard allows delimiters to be included in keys or values \
             (tokens) if they are \"escaped\" with another delimiter. This also \
             implies that delimiters can never start or end a token since it is \
             impossible to unambiguously assign such escaped delimiters to \
             either side of the real delimiter. This also means empty tokens are \
             not allowed."
                .into(),
            "In reality, many files use delimiters as if they are not supposed \
             to be escaped."
                .into(),
            format!(
                "If {esc} or {unesc}, escape or do not escape delimiters respectively.",
                esc = fmt_val(tc::DELIM_ESCAPED_LEVEL),
                unesc = fmt_val(tc::DELIM_UNESCAPED_LEVEL)
            ),
            format!(
                "If {esc} or {unesc} attempt to guess how delimiters should be \
                 treated, falling back to escaped or unescaped mode respectively \
                 if the choice is ambiguous. The determination will be made by \
                 first scanning {text_seg} to find all delimiter positions and \
                 choosing the mode which results in an even number of tokens \
                 with no delimiters in keys (escaped mode) and no blank keys \
                 (unescaped mode).",
                esc = fmt_val(tc::DELIM_ESCAPED_LEVEL),
                unesc = fmt_val(tc::DELIM_UNESCAPED_LEVEL)
            ),
            format!(
                "Using the guessing algorithm has a significant performance penalty \
                 since {text_seg} needs to be parsed twice. Furthermore, this \
                 algorithm is heuristic and not guaranteed to succeed. An uneven \
                 number of tokens implies that {text_seg} is malformed which will \
                 likely be the case assuming that the ending offset for {text_seg} \
                 will be too high (if at all) therefore all delimiters should be in \
                 the {text_seg} segment and can be counted. Keys likely will not have \
                 escaped delimiters in them. Keys should almost never be blank in \
                 unescaped mode since '\"\"' is almost never a sensible key value."
            ),
            format!(
                "The guessing algorithm is independent of {odd} and {final} \
                 which will trigger as normal if their respective violations \
                 are found.",
                odd = fmt_arg(ALLOW_ODD_TOKENS),
                final = fmt_arg(ALLOW_EVEN_DELIMS),
            ),
        ],
    );

    let (sub_header, sub_help) = format_section(
        "SUBSTITUTION",
        [format!(
            "The SUB part in {pat} is a sed-like pattern which will \
             be used to edit the value of KEY. It must be a string like \
             's<D><FROM><D><TO>[<D>g]' where 'D' is a delimiter (any character), \
             FROM is a regular expression and TO is a replacement pattern. FROM \
             and TO must follow the syntax outlined in {REGEXP_REF} and \
             {REGEXP_REP_REF} respectively, with the caveat that only bracketed \
             replacement syntax is allowed.",
            pat = fmt_arg(SUB_STD_KEY_VALS),
        )],
    );

    let (date_header, date_help) = format_section(
        "DATE PATTERN",
        [format!(
            "The value for {pat} will be used as an alternative pattern when \
             parsing {date}. It should have specifiers for year, month, and \
             day as outlined in {CHRONO_REF}. If not supplied, {date} will \
             be parsed according to the standard pattern which is \
             {fmt}.",
            pat = fmt_arg(DATE_PATTERN),
            fmt = fmt_val(tc::DEFAULT_DATE_FORMAT),
        )],
    );

    let (time_header, time_help) = format_section(
        "TIME PATTERN",
        [format!(
            "If supplied, will be used as an alternative pattern when parsing \
             {btim} and {etim} It should have specifiers for hours, minutes, and \
             seconds as outlined in {CHRONO_REF}. It may optionally also have a \
             sub-seconds specifier as shown in the same link. Furthermore, the \
             specifiers {base60} and {base100} may be used to match 1/60 and \
             centiseconds respectively. If not supplied, {btim} and {etim} will \
             be parsed according to the standard pattern which is {fmt2_0} for \
             2.0, {fmt3_0} for 3.0, and {fmt3_1} for 3.1 and up.",
            base60 = fmt_val(tc::BASE60_SECOND_SPEC),
            base100 = fmt_val(tc::BASE100_SECOND_SPEC),
            fmt2_0 = fmt_val(tc::DEFAULT_TIME_FORMAT_2_0),
            fmt3_0 = fmt_val(tc::DEFAULT_TIME_FORMAT_2_0),
            fmt3_1 = fmt_val(tc::DEFAULT_TIME_FORMAT_3_1)
        )],
    );

    let flat_long_help = [&delim_help, &sub_help].iter().join("\n\n");
    let std_long_help = [&delim_help, &sub_help, &date_help, &time_help]
        .iter()
        .join("\n\n");

    let correction_arg = |long: &'static str, in_header: bool, seg: &ANSIString| {
        let src = if in_header { &header_seg } else { &text_seg };
        let h = format!("Correction for {seg} offsets from {src}.");
        Arg::new(long)
            .long(long)
            .value_name("BEGIN,END")
            .help(h)
            .value_parser(ValueParser::new(parse_offsets))
    };

    // header args

    let text_correction = correction_arg(TEXT_CORR, true, &text_seg);
    let data_correction = correction_arg(DATA_CORR, true, &data_seg);
    let analysis_correction = correction_arg(ANALYSIS_CORR, true, &analysis_seg);

    let other_correction = Arg::new(OTHER_CORR)
        .long(OTHER_CORR)
        .value_name("BEGIN,END")
        .action(ArgAction::Append)
        .value_parser(ValueParser::new(parse_offsets))
        .help(format!(
            "Correction for {other_seg} offsets. This can be given multiple \
             times and will be applied to offsets in the order given."
        ));

    let max_other = opt_arg::<usize>(
        MAX_OTHER,
        "BYTES",
        format!("Max number of {other_seg} segments to parse."),
    );

    let other_width = Arg::new(OTHER_WIDTH)
        .long(OTHER_WIDTH)
        .value_name("WIDTH")
        .help(format!("Width of {other_seg} segments."))
        .value_parser(ValueParser::new(parse_other_width));

    let guess_other_width = Arg::new(GUESS_OTHER_WIDTH)
        .long(GUESS_OTHER_WIDTH)
        .value_name("LEVEL")
        .value_parser(value_parser!(tc::GuessOtherWidth))
        .help(format!(
            "Guess the width of {other_seg} segments. Valid values are {none} \
             (no guessing) or {error}, {warn} or {silent} which will guess and \
             throw an error, warning, or nothing on failure. For {warn} and \
             {silent}, failure will fall back to 8 or whatever was given in {arg}.",
            none = fmt_val(tc::OTHER_WIDTH_NONE_LEVEL),
            error = fmt_val(tc::OTHER_WIDTH_ERROR_LEVEL),
            warn = fmt_val(tc::OTHER_WIDTH_WARN_LEVEL),
            silent = fmt_val(tc::OTHER_WIDTH_SILENT_LEVEL),
            arg = fmt_arg(OTHER_WIDTH),
        ));

    let squish_offsets = override_flag_arg(
        SQUISH_OFFSETS,
        format!(
            "If {data_seg}/{analysis_seg} end in 0, use 0 for start as well. \
             Should not be used for FCS 2.0 files."
        ),
    );

    let all_read_header_args = [
        text_correction,
        data_correction,
        analysis_correction,
        other_correction,
        max_other,
        other_width,
        guess_other_width,
        squish_offsets,
    ];

    // offset args

    let allow_pseudoempty = override_flag_arg(
        ALLOW_PSEUDOEMPTY,
        "Treat offsets like '0,-1' or '1000,999' as '0,0'.",
    );

    let truncate_offset_limit = Arg::new(TRUNCATE_OFFSET_LIMIT)
        .long(TRUNCATE_OFFSET_LIMIT)
        .value_name("LIMIT")
        .value_parser(value_parser!(cfg::DatasetOverflowLimit))
        .help("Limit by which offsets can be truncated if they exceed end of file.");

    let overlap_correction_limit = Arg::new(OVERLAP_CORRECTION_LIMIT)
        .long(OVERLAP_CORRECTION_LIMIT)
        .value_name("LIMIT")
        .value_parser(value_parser!(cfg::OverlapCorrectionLimit))
        .help(
            "Limit by which ending segment offset can be truncated if they overlap another offset.",
        );

    let data_remainder_limit = Arg::new(DATA_REMAINDER_LIMIT)
        .long(DATA_REMAINDER_LIMIT)
        .value_name("LIMIT")
        .value_parser(value_parser!(cfg::DataRemainderLimit))
        .help(format!(
            "Limit by which ending {data_seg} offset can be truncated if \
             its length modulo event width produces a remainder."
        ));

    let all_read_offset_args = [
        allow_pseudoempty,
        truncate_offset_limit,
        overlap_correction_limit,
        data_remainder_limit,
    ];

    // "flat" args

    let version_override = opt_arg::<cfg::VersionOverride>(
        VERSION_OVERRIDE,
        "OVERRIDE",
        format!(
            "Override the FCS version from {header_seg}. Can be an FCS version \
             string (like 'FCS3.2') which will force to a fixed version. Can \
             also autodetect version with one of {latest} or {earliest} (the \
             latest or earliest available version respectively) or {loose} or \
             {strict} (the available version with the most or least optional \
             keywords respectively). Append \"current_or\" to any of these \
             to prioritize the current version before ranking others.",
            latest = fmt_val(tc::VERSION_LATEST_LEVEL),
            earliest = fmt_val(tc::VERSION_EARLIEST_LEVEL),
            loose = fmt_val(tc::VERSION_LOOSE_LEVEL),
            strict = fmt_val(tc::VERSION_STRICT_LEVEL),
        ),
    );

    let supp_text_correction = correction_arg(SUPP_TEXT_COR, false, &supp_text_seg);

    let nextdata_correction = Arg::new(NEXTDATA_COR)
        .long(NEXTDATA_COR)
        .value_name("INT")
        .value_parser(value_parser!(i32))
        .help(format!("Correction for {nextdata}"));

    let allow_overlapping_supp_text = tri_flag_arg::<cfg::AllowDuplicatedSuppTEXT>(
        ALLOW_DUPLICATED_SUPP_TEXT,
        format!(
            "Allow {supp_text_seg} offsets to overlap those for \
             {prim_text_seg} or the boundaries of {header_seg}."
        ),
    );

    let ignore_supp_text = override_flag_arg(
        IGNORE_SUPP_TEXT,
        format!("Ignore {supp_text_seg} entirely."),
    );

    let lit_delims = Arg::new(DELIM_ESCAPE_MODE)
        .long(DELIM_ESCAPE_MODE)
        .value_name("MODE")
        .value_parser(value_parser!(tc::DelimEscapeMode))
        .help(format!(
            "Choose how to escape delimiters in {text_seg}. \
             See {delim_header} for details."
        ));

    let non_ascii_delim = tri_flag_arg::<cfg::AllowNonAsciiDelim>(
        ALLOW_NON_ASCII_DELIM,
        format!("Allow {text_seg} delimiter to be non-ASCII character."),
    );

    let missing_final_delim = tri_flag_arg::<cfg::AllowEvenDelims>(
        ALLOW_EVEN_DELIMS,
        format!("Allow final {text_seg} delimiter to be missing."),
    );

    let allow_non_unique = tri_flag_arg::<cfg::AllowNonunique>(
        ALLOW_NONUNIQUE,
        format!("Allow non-unique keys to exist in {text_seg}."),
    );

    let allow_odd =
        tri_flag_arg::<cfg::AllowOddTokens>(ALLOW_ODD_TOKENS, "Allow odd number of tokens.");

    let allow_empty_keys = tri_flag_arg::<cfg::AllowEmptyKeys>(
        ALLOW_EMPTY_KEYS,
        "Allow keys to be blank (relatively rare).",
    );

    let allow_delim_at_bound = tri_flag_arg::<cfg::AllowDelimAtBoundary>(
        ALLOW_DELIM_AT_BOUNDARY,
        format!("Allow {text_seg} delimiter(s) to be at token boundaries."),
    );

    let use_encoding = Arg::new(USE_ENCODING)
        .long(USE_ENCODING)
        .value_name("ENC")
        .value_parser(value_parser!(tc::UseEncoding))
        .help(format!(
            "Choose how to interpret characters in {text_seg}. Choose {single}, \
             {utf8}, or {guess} to interpret bytes as IANA ISO/IEC-8859-1 UTF-8, \
             or first as UTF-8 and falling back to IANA ISO/IEC-8859-1 if a \
             non-UTF-8 byte is found.",
            single = fmt_val(tc::ENCODING_SINGLE_LEVEL),
            utf8 = fmt_val(tc::ENCODING_UTF8_LEVEL),
            guess = fmt_val(tc::ENCODING_GUESS_LEVEL)
        ));

    let allow_non_ascii_keywords = tri_flag_arg::<cfg::AllowNonAsciiKeywords>(
        ALLOW_NON_ASCII_KEYS,
        "Allow non-ASCII characters in keys.",
    );

    let allow_non_utf8 = tri_flag_arg::<cfg::AllowNonUtf8>(
        ALLOW_NON_UTF8_VALUES,
        format!("Allow non-UTF8 characters in {text_seg} segment."),
    );

    let allow_missing_supp_text = tri_flag_arg::<cfg::AllowMissingSuppTEXT>(
        ALLOW_MISSING_SUPP_TEXT,
        format!("Allow {supp_text_seg} offsets to be missing."),
    );

    let allow_supp_text_own_delim = tri_flag_arg::<cfg::AllowSuppTEXTOwnDelim>(
        ALLOW_SUPP_TEXT_OWN_DELIM,
        format!("Allow delimiters in {prim_text_seg} and {supp_text_seg} to differ."),
    );

    let allow_missing_nextdata = tri_flag_arg::<cfg::AllowMissingNextdata>(
        ALLOW_MISSING_NEXTDATA,
        format!("Allow {nextdata} to be missing."),
    );

    let trim_value_whitespace = Arg::new(TRIM_VALUE_WHITESPACE)
        .long(TRIM_VALUE_WHITESPACE)
        .value_name("LEVEL")
        .value_parser(value_parser!(tc::TrimValueWhitespace))
        .help(format!(
            "Trim whitespace from beginning and end of all values. This may \
             create blank values if the starting string is entirely whitespace. \
             Set to {none} to not trim at all (default). Set to {error}, {warn}, \
             or {silent} to enable trimming and throw error, warning, or nothing \
             when trimming results in a blank.",
            none = fmt_val(tc::TRIM_NONE_LEVEL),
            error = fmt_val(tc::TRIM_ERROR_LEVEL),
            warn = fmt_val(tc::TRIM_BLANK_WARN_LEVEL),
            silent = fmt_val(tc::TRIM_BLANK_SILENT_LEVEL),
        ));

    let make_key_str_args = |name, help| {
        let more = format!(
            "Values that start and end with {delim} will be \
             interpreted as regular expressions.",
            delim = fmt_val(tc::PATTERN_DELIMITER)
        );
        let more_help = format!("{help} {more}");
        Arg::new(name)
            .long(name)
            .action(ArgAction::Append)
            .value_name("KEY_OR_PAT")
            .help(more_help)
            .value_parser(value_parser!(KeyStringOrPattern))
    };

    let all_read_flat_args = vec![
        version_override,
        supp_text_correction,
        nextdata_correction,
        allow_overlapping_supp_text,
        ignore_supp_text,
        lit_delims,
        non_ascii_delim,
        missing_final_delim,
        allow_non_unique,
        allow_odd,
        allow_empty_keys,
        allow_delim_at_bound,
        allow_non_utf8,
        use_encoding,
        allow_non_ascii_keywords,
        allow_missing_supp_text,
        allow_supp_text_own_delim,
        allow_missing_nextdata,
        trim_value_whitespace,
    ];

    // std args

    let dedup_meas_names = override_flag_arg(
        DEDUP_MEAS_NAMES,
        format!(
            "Force all {pn_n} to be unique by appending {} \
             to each duplicate and appending 'X' (starting at 0)",
            fmt_val(format!("{}X", tc::DEDUP_PNN_SEP)),
        ),
    );

    let trim_intra_value_whitespace = override_flag_arg(
        TRIM_INTRA_VALUE_WHITESPACE,
        "Remove spaces between comma-separated values.",
    );

    let time_meas_pattern = Arg::new(TIME_MEAS_PATTERN)
        .long(TIME_MEAS_PATTERN)
        .value_name("REGEXP")
        .help(format!(
            "Use REGEXP when matching time measurement (defaults to \
             {default}, pass {none} to not look for a time channel).",
            default = fmt_val(tc::TIME_MEAS_NAME_PATTERN_DEFAULT),
            none = fmt_val(tc::TIME_MEAS_NAME_PATTERN_NONE),
        ))
        .value_parser(value_parser!(cfg::TimeMeasNamePattern));

    let allow_missing_time = tri_flag_arg::<cfg::AllowMissingTime>(
        ALLOW_MISSING_TIME,
        "Allow time measurement to be missing.",
    );

    let add_missing_timestep = opt_arg::<Timestep>(
        ADD_MISSING_TIMESTEP,
        "TIMESTEP",
        "Add {timestep) if it is missing.",
    );

    let force_linear_scale = Arg::new(FORCE_LINEAR_SCALE)
        .long(FORCE_LINEAR_SCALE)
        .value_name("WHICH")
        .value_parser(value_parser!(tc::ForceLinearScale))
        .help(format!(
            "Force {pn_e} keywords to be linear. Pass {time} to only set the \
             temporal measurement, {non_int} to set temporal measurements and \
             non-integer measurements, {all} to set all measurements, and {none} \
             for no measurements. Affected columns will never fail.",
            time = fmt_val(tc::FORCE_LINEAR_TIME_LEVEL),
            non_int = fmt_val(tc::FORCE_LINEAR_NON_INT_LEVEL),
            all = fmt_val(tc::FORCE_LINEAR_ALL_LEVEL),
            none = fmt_val(tc::FORCE_LINEAR_NONE_LEVEL),
        ));

    let ignore_time_optical_keys = Arg::new(IGNORE_TIME_OPTICAL_KEYS)
        .long(IGNORE_TIME_OPTICAL_KEYS)
        .action(ArgAction::Append)
        .value_name("SYMS")
        .help(format!(
            "Ignore optical keywords for temporal measurement. Must be a \
             comma-separated list of strings like the X in {pn_any}.",
        ))
        .value_delimiter(',')
        .value_parser(value_parser!(tc::TemporalOpticalKey));

    let process_time_optical_keys = Arg::new(PROCESS_TIME_OPTICAL_KEYS)
        .long(PROCESS_TIME_OPTICAL_KEYS)
        .value_name("LEVEL")
        .value_parser(value_parser!(tc::ProcessTemporalOpticalKeys))
        .help(format!(
            "Choose how to handle optical keys found in temporal measurements. \
             Does nothing unless keys are specified in {arg}. Pass \
             {demote_warn}, {demote_silent}, {drop_warn}, or {drop_silent} to \
             demote found keys to nonstandard (with or without warning) or drop \
             keys entirely (with or without warning) respectively.",
            arg = fmt_arg(IGNORE_TIME_OPTICAL_KEYS),
            demote_warn = fmt_val(tc::TMP_OPT_DEMOTE_WARN_LEVEL),
            demote_silent = fmt_val(tc::TMP_OPT_DEMOTE_SILENT_LEVEL),
            drop_warn = fmt_val(tc::TMP_OPT_DROP_WARN_LEVEL),
            drop_silent = fmt_val(tc::TMP_OPT_DROP_SILENT_LEVEL),
        ));

    let spillover_measurement_mode = Arg::new(SPILLOVER_MEASUREMENT_MODE)
        .long(SPILLOVER_MEASUREMENT_MODE)
        .value_name("MODE")
        .value_parser(value_parser!(tc::SpilloverMeasurementMode))
        .help(format!(
            "Choose how to interpret measurement strings in {spillover}. Set to \
             {named} to interpret as names which link to {pn_n}. Set to \
             {indexed} to interpret as 1-indices which point to measurements. \
             Set to {guess} to automatically choose the prior two modes.",
            named = fmt_val(tc::SPILLOVER_NAMED_LEVEL),
            indexed = fmt_val(tc::SPILLOVER_INDEXED_LEVEL),
            guess = fmt_val(tc::SPILLOVER_GUESS_LEVEL)
        ));

    let allow_other_feature = override_flag_arg(
        ALLOW_OTHER_FEATURE,
        format!("Allow {pnfeature} to be a value other than \"Area\", \"Width\", or \"Height\""),
    );

    let process_pseudostandard = proc_kw_fail_arg(
        PROCESS_PSEUDOSTANDARD,
        "Process non-standard keywords that start with a '$'.",
    )
    .value_parser(value_parser!(cfg::ProcessPseudostandard));

    let process_hyper_par = proc_kw_fail_arg(
        PROCESS_HYPER_PAR,
        format!("Process measurement keywords whose index is greater than {par}."),
    )
    .value_parser(value_parser!(cfg::ProcessHyperPar));

    let process_other_version = proc_kw_fail_arg(
        PROCESS_OTHER_VERSION,
        "Process standard keywords from different FCS version.",
    )
    .value_parser(value_parser!(cfg::ProcessOtherVersion));

    let process_extra_timestep = proc_kw_fail_arg(
        PROCESS_EXTRA_TIMESTEP,
        format!(
            "Process unused {timestep}, which may indicate that a time measurement \
             is present but not identified.",
        ),
    )
    .value_parser(value_parser!(cfg::ProcessExtraTimestep));

    let fix_log_scale_offset = override_flag_arg(
        FIX_LOG_SCALE_OFFSETS,
        format!(
            "Fix {pn_e} keys that have log scaling with zero offset. \
             Specifically, this will replace values like 'X,0.0' with 'X,1.0' \
             where 'X' is a positive decimal number. Having '0.0' for log offset \
             is mathematical nonsense.",
        ),
    );

    let disallow_localtime = override_flag_arg(
        DISALLOW_LOCALTIME,
        format!(
            "Require that {begindatetime} and {enddatetime} have a timezone if \
             provided. This is not required by the standard, but not having a \
             timezone is ambiguous since the absolute value of the timestamp is \
             dependent on localtime and therefore is location-dependent. Only \
             affects FCS 3.2.",
        ),
    );

    let date_pattern = opt_arg::<DatePattern>(
        DATE_PATTERN,
        "PATTERN",
        format!("Pattern to match {date} keyword. See {date_header}."),
    );

    let time_pattern = opt_arg::<TimePattern>(
        TIME_PATTERN,
        "PATTERN",
        format!("Pattern to match {btim}/{etim} keywords. See {time_header}."),
    );

    let datetime_pattern = opt_arg::<String>(
        DATETIME_PATTERN,
        "PATTERN",
        format!(
            "If supplied, will be used as an alternative pattern when parsing \
             {begindatetime} and {enddatetime}. It should follow the format \
             outline in {CHRONO_REF}.",
        ),
    );

    let last_modified_pattern = opt_arg::<String>(
        LAST_MODIFIED_PATTERN,
        "PATTERN",
        format!(
            "If supplied, will be used as an alternative pattern when parsing \
             {last_modified}. It should follow the format outline in {CHRONO_REF}.",
        ),
    );

    let all_read_std_args = [
        dedup_meas_names,
        trim_intra_value_whitespace,
        time_meas_pattern,
        allow_missing_time,
        add_missing_timestep,
        force_linear_scale,
        ignore_time_optical_keys,
        process_time_optical_keys,
        spillover_measurement_mode,
        date_pattern,
        time_pattern,
        datetime_pattern,
        last_modified_pattern,
        allow_other_feature,
        process_pseudostandard,
        process_hyper_par,
        process_other_version,
        process_extra_timestep,
        fix_log_scale_offset,
        disallow_localtime,
    ];

    // read dataset args

    let ignore_std_key = make_key_str_args(
        IGNORE_STD_KEYS,
        "Ignore standard keys exactly matching KEY_OR_PAT. The leading '$' is implied.",
    );

    let promote_to_std = make_key_str_args(
        PROMOTE_TO_STD,
        "Promote non-standard keys matching KEY_OR_PAT to standard.",
    );

    let demote_from_std = make_key_str_args(
        DEMOTE_FROM_STD,
        "Demote standard keys matching KEY_OR_PAT to non-standard. The leading '$' is implied.",
    );

    let rename_standard_keys = Arg::new(RENAME_STD_KEYS)
        .long(RENAME_STD_KEYS)
        .action(ArgAction::Append)
        .value_name("OLD,NEW")
        .value_parser(ValueParser::new(parse_two_keystring_pair))
        .help("Rename standard keys from OLD to NEW. The leading '$' is implied.");

    let replace_std_key_vals = Arg::new(REPLACE_STD_KEY_VALS)
        .long(REPLACE_STD_KEY_VALS)
        .action(ArgAction::Append)
        .value_name("KEY,VAL")
        .help(
            "Replace values of standard keys matching KEY with VAl. \
             The leading '$' is implied for the key.",
        )
        .value_parser(ValueParser::new(parse_keystring_string_pair));

    let append_std_key_vals = Arg::new(APPEND_STD_KEY_VALS)
        .long(APPEND_STD_KEY_VALS)
        .action(ArgAction::Append)
        .value_name("KEY,VAL")
        .help(
            "Append standard keys with KEY and VAL to list of existing standard \
             keys. The leading '$' is implied for KEY.",
        )
        .value_parser(ValueParser::new(parse_keystring_string_pair));

    let sub_key_vals = Arg::new(SUB_STD_KEY_VALS)
        .long(SUB_STD_KEY_VALS)
        .action(ArgAction::Append)
        .value_name("KEY,SUB")
        .help(format!(
            "Edit standard key values using KEY and SUB. The leading '$' \
             is implied for KEY. See {sub_header} for details."
        ))
        .value_parser(ValueParser::new(parse_sub_pattern_pair));

    let allow_repair_non_unique = tri_flag_arg::<cfg::AllowRepairNonUnique>(
        ALLOW_REPAIR_NON_UNIQUE,
        "Choose how to handle key collisions when repairing keywords. \
         Non-unique keywords will not be kept in the final FCS file since each \
         list of standard and non-standard keywords must be unique.",
    );

    let text_data_correction = correction_arg(TEXT_DATA_COR, false, &data_seg);
    let text_analysis_correction = correction_arg(TEXT_ANALYSIS_COR, false, &analysis_seg);

    let ignore_text_data_offsets = override_flag_arg(
        IGNORE_TEXT_DATA_OFFSETS,
        format!("Ignore offsets for {data_seg} from {text_seg}."),
    );

    let ignore_text_analysis_offsets = override_flag_arg(
        IGNORE_TEXT_ANALYSIS_OFFSETS,
        format!("Ignore offsets for {analysis_seg} from {text_seg}."),
    );

    let allow_header_text_offset_mismatch = Arg::new(ALLOW_HEADER_TEXT_OFFSET_MISMATCH)
        .long(ALLOW_HEADER_TEXT_OFFSET_MISMATCH)
        .value_name("LEVEL")
        .value_parser(value_parser!(tc::AllowHeaderTEXTOffsetMismatch))
        .help(format!(
            "Allow {header_seg} and {text_seg} offsets to be different. If \
             {hdr_warn} or {hdr_silent}, choose {header_seg} and throw a warning \
             or nothing on mismatch. If {text_warn} or {text_silent} behave \
             analogously for {text_seg}. If {error} (default) throw error.",
            hdr_warn = fmt_val(tc::MISMATCH_HEADER_WARN_LEVEL),
            hdr_silent = fmt_val(tc::MISMATCH_HEADER_SILENT_LEVEL),
            text_warn = fmt_val(tc::MISMATCH_TEXT_WARN_LEVEL),
            text_silent = fmt_val(tc::MISMATCH_TEXT_SILENT_LEVEL),
            error = fmt_val(tc::MISMATCH_ERROR_LEVEL),
        ));

    let allow_missing_required_offsets = tri_flag_arg::<cfg::AllowMissingRequiredOffsets>(
        ALLOW_MISSING_REQUIRED_OFFSETS,
        format!(
            "Allow required offsets to be missing from {text_seg}. \
             Only applies to FCS 3.0/3.1."
        ),
    );

    let process_optional_failure = proc_kw_fail_arg(
        PROCESS_OPTIONAL_FAILURE,
        "Process optional keys if they cause an error.",
    )
    .value_parser(value_parser!(cfg::ProcessOptionalFailure));

    let int_widths_from_byteord = Arg::new(FIX_INT_WIDTHS)
        .long(FIX_INT_WIDTHS)
        .value_name("INT_OR_FLAG")
        .value_parser(parse_fix_int_widths)
        .help(format!(
            "Fix {pn_b}. Only has effect on integer layouts in FCS 2.0/3.0. \
             Set to {} or {} to round up to next multiple of 8 or do nothing. \
             Set to an integer 1-8 to override all {pn_b} explicitly.",
            fmt_val(tc::FIX_INT_WIDTH_NEXT_BYTE_LEVEL),
            fmt_val(tc::FIX_INT_WIDTH_NEVER_LEVEL)
        ));

    let int_byteord_override = Arg::new(BYTEORD_OVERRIDE)
        .long(BYTEORD_OVERRIDE)
        .value_name("BYTEORD")
        .value_parser(parse_byteord_override)
        .help(format!(
            "Override the value of {byteord}. Set to {} or {} to do nothing \
             or interpret {byteord} based on its endian-ness (ie without its \
             length). Set to an explicit comma-separated integer sequence to \
             set {byteord} directly.",
            fmt_val(tc::BYTEORD_OVERRIDE_NONE_LEVEL),
            fmt_val(tc::BYTEORD_OVERRIDE_ENDIAN_LEVEL)
        ));

    let disallow_range_truncation = tri_flag_arg::<cfg::DisallowRangeTrunc>(
        DISALLOW_RANGE_TRUNCATION,
        format!(
            "Disallow {pn_r} values which need to be truncated to fit in type \
             dictated by {datatype} (and {pndatatype} for FCS 3.2) and {pn_b} \
             for a given measurement."
        ),
    );

    let all_read_dataset_kws_args = [
        ignore_std_key,
        promote_to_std,
        demote_from_std,
        rename_standard_keys,
        replace_std_key_vals,
        append_std_key_vals,
        sub_key_vals,
        allow_repair_non_unique,
        text_data_correction,
        text_analysis_correction,
        ignore_text_data_offsets,
        ignore_text_analysis_offsets,
        allow_header_text_offset_mismatch,
        allow_missing_required_offsets,
        process_optional_failure,
        int_widths_from_byteord,
        int_byteord_override,
        disallow_range_truncation,
    ];

    // dataset args

    let allow_uneven_event_width = tri_flag_arg::<cfg::AllowUnevenEventWidth>(
        ALLOW_UNEVEN_EVENT_WIDTH,
        format!("Allow event width to not evenly divide length of {data_seg}."),
    );

    let allow_tot_mismatch = tri_flag_arg::<cfg::AllowTotMismatch>(
        ALLOW_TOT_MISMATCH,
        format!("Allow {tot} to mismatch the number of events that are actually in {data_seg}."),
    );

    let over_bitmask_action = Arg::new(OVER_BITMASK_ACTION)
        .long(OVER_BITMASK_ACTION)
        .value_name("WHICH")
        .value_parser(value_parser!(tc::OverBitmaskAction))
        .help(format!(
            "Choose how to handle integer values in {data_seg} to exceed bitmask. \
             Pass {error} to emit error, {warn} to emit warning, {silent} to do \
             nothing, {trunc_warn} to truncate and emit warning, and \
             {trunc_silent} to truncate with no warning.",
            error = fmt_val(tc::OVER_LIMIT_ACTION_ERROR_LEVEL),
            warn = fmt_val(tc::OVER_LIMIT_ACTION_WARN_LEVEL),
            silent = fmt_val(tc::OVER_LIMIT_ACTION_SILENT_LEVEL),
            trunc_warn = fmt_val(tc::OVER_LIMIT_ACTION_TRUNCATE_WARN_LEVEL),
            trunc_silent = fmt_val(tc::OVER_LIMIT_ACTION_TRUNCATE_SILENT_LEVEL),
        ));

    let over_range_action = Arg::new(OVER_RANGE_ACTION)
        .long(OVER_RANGE_ACTION)
        .value_name("ACTION")
        .value_parser(value_parser!(tc::OverLimitAction))
        .help(format!(
            "Choose how to handle values in {data_seg} to exceed {pn_r}. Pass \
             {error} to emit error, {warn} to emit warning, {silent} to do \
             nothing, {trunc_warn} to truncate and emit warning, and \
             {trunc_silent} to truncate with no warning.",
            error = fmt_val(tc::OVER_LIMIT_ACTION_ERROR_LEVEL),
            warn = fmt_val(tc::OVER_LIMIT_ACTION_WARN_LEVEL),
            silent = fmt_val(tc::OVER_LIMIT_ACTION_SILENT_LEVEL),
            trunc_warn = fmt_val(tc::OVER_LIMIT_ACTION_TRUNCATE_WARN_LEVEL),
            trunc_silent = fmt_val(tc::OVER_LIMIT_ACTION_TRUNCATE_SILENT_LEVEL),
        ));

    let allow_missing_crc = tri_flag_arg::<cfg::AllowMissingCRC>(
        ALLOW_MISSING_CRC,
        "Allow CRC word at the end of the dataset to be missing.",
    );

    let allow_mismatch_crc = tri_flag_arg::<cfg::AllowMismatchCRC>(
        ALLOW_MISMATCH_CRC,
        "Allow computed CRC to not match the CRC word at the end of the dataset.",
    );

    let compute_crc = Arg::new(COMPUTE_CRC)
        .long(COMPUTE_CRC)
        .value_name("LEVEL")
        .value_parser(value_parser!(tc::ComputeCRC))
        .help(format!(
            "When to compute the CRC of the dataset. \
             Pass {} to never compute, pass {} to always compute, and pass {} \
             to compute only when the CRC word is available.",
            tc::COMPUTE_CRC_NEVER_LEVEL,
            tc::COMPUTE_CRC_ALWAYS_LEVEL,
            tc::COMPUTE_CRC_TEST_LEVEL,
        ));

    let row_buffer_size = Arg::new(ROW_BUFFER_SIZE)
        .long(ROW_BUFFER_SIZE)
        .value_name("BYTES")
        .value_parser(value_parser!(tc::RowBufferSize))
        .help(format!(
            "Set the size in bytes for the internal buffer used to read {data_seg}. \
             This is a performance parameter that balances read syscalls (too low) \
             and cache misses (too high). It should generally be 90% of the CPU's \
             L1D cache size. Defaults to {}.",
            fmt_val(tc::RowBufferSize::default())
        ));

    let all_read_dataset_args = [
        allow_uneven_event_width,
        allow_tot_mismatch,
        over_bitmask_action,
        over_range_action,
        allow_missing_crc,
        allow_mismatch_crc,
        compute_crc,
        row_buffer_size,
    ];

    // shared args

    let warnings_are_errors = flag_arg(WARNINGS_ARE_ERRORS, "Treat all warnings as fatal errors.");

    let hide_warnings = flag_arg(HIDE_WARNINGS, "Hide all warnings.");

    let all_read_shared_args = [warnings_are_errors, hide_warnings];

    // write args

    let write_delim = Arg::new(WRITE_DELIM)
        .long(WRITE_DELIM)
        .value_name("CHAR")
        .value_parser(value_parser!(TEXTDelim))
        .help(format!(
            "The delimiter to use when writing {text_seg} to file. \
             Must be an ASCII character 1-31. Defaults to 30 (record separator)."
        ));

    let big_other = flag_arg(
        BIG_OTHER,
        format!("If set, use 20 for {other_seg} offset width, otherwise 8."),
    );

    let all_write_args = [write_delim, big_other];

    // other args

    let delim_arg = Arg::new(PRINT_DELIM)
        .long(PRINT_DELIM)
        .short('d')
        .value_name("CHAR")
        .help("Delimiter to use for tabular output.")
        .value_parser(ValueParser::new(parse_delim))
        .default_value("\t");

    let dataset_index_arg = Arg::new(DATASET_INDEX)
        .long(DATASET_INDEX)
        .short('I')
        .value_name("INDEX")
        .value_parser(value_parser!(usize))
        .help("Index of the dataset to parse (starting from 0)");

    let skip_arg = Arg::new(SKIP)
        .long(SKIP)
        .value_name("INT")
        .value_parser(value_parser!(usize))
        .help("Number of datasets to skip");

    let limit_arg = Arg::new(LIMIT)
        .long(LIMIT)
        .value_name("INT")
        .value_parser(value_parser!(usize))
        .help("Number of datasets to return");

    let scan_arg = Arg::new(SCAN)
        .long(SCAN)
        .action(ArgAction::SetTrue)
        .help(format!(
            "If given, scan for the next dataset by looking for version \
             tags rather then relying on {nextdata}"
        ));

    let input_arg = Arg::new(INPUT_PATH)
        .short('i')
        .long(INPUT_PATH)
        .value_name("PATH")
        .value_parser(value_parser!(PathBuf))
        .help("Path to FCS file to parse.")
        .required(true);

    let output_arg = Arg::new(OUTPUT_PATH)
        .short('o')
        .long(OUTPUT_PATH)
        .value_name("PATH")
        .value_parser(value_parser!(PathBuf))
        .help("Path to FCS file to write.")
        .required(true);

    let strategy_arg = Arg::new(STRATEGY)
        .short('s')
        .long(STRATEGY)
        .value_name("WHICH")
        .value_parser(value_parser!(tc::ReadStrategy))
        .help(format!(
            "Overall strategy to use when parsing file. This will enable many \
             options by default which can be overridden. Set to {scalpal} to \
             parse non-compliant files while attempting to preserve metadata. \
             Set to {sledge} to parse non-compliant files while dropping \
             non-compliant metadata that cannot be fixed. Set to {strict} to \
             enforce the standard (default).",
            scalpal = fmt_val(tc::READ_STRATEGY_SCALPAL_LEVEL),
            sledge = fmt_val(tc::READ_STRATEGY_SLEDGEHAMMER_LEVEL),
            strict = fmt_val(tc::READ_STRATEGY_STRICT_LEVEL),
        ));

    let vendor_heuristics_arg = Arg::new(VENDOR_HEURISTICS)
        .short('H')
        .long(VENDOR_HEURISTICS)
        .action(ArgAction::SetTrue)
        .help("Enable vendor-specific heuristics when parsing files.");

    let header_cmd = Command::new(SUBCMD_HEADER)
        .about("Show header as JSON.")
        .arg(&input_arg)
        .arg(&strategy_arg)
        .args(&all_read_header_args)
        .args(&all_read_offset_args);

    let flat_cmd = Command::new(SUBCMD_FLAT)
        .about("Show flat keywords as JSON.")
        .arg(&input_arg)
        .arg(&strategy_arg)
        .arg(&dataset_index_arg)
        .args(&all_read_header_args)
        .args(&all_read_offset_args)
        .args(&all_read_flat_args)
        .args(&all_read_shared_args)
        .after_long_help(flat_long_help);

    let std_cmd = Command::new(SUBCMD_STD)
        .about("Dump standardized keywords as JSON.")
        .arg(&input_arg)
        .arg(&strategy_arg)
        .arg(&vendor_heuristics_arg)
        .arg(&dataset_index_arg)
        .args(&all_read_header_args)
        .args(&all_read_offset_args)
        .args(&all_read_flat_args)
        .args(&all_read_std_args)
        .args(&all_read_dataset_kws_args)
        .args(&all_read_shared_args)
        .after_long_help(&std_long_help);

    let meas_cmd = Command::new(SUBCMD_MEAS)
        .about("Show a table of standardized measurement values.")
        .arg(&input_arg)
        .arg(&strategy_arg)
        .arg(&vendor_heuristics_arg)
        .arg(&dataset_index_arg)
        .arg(&delim_arg)
        .args(&all_read_header_args)
        .args(&all_read_offset_args)
        .args(&all_read_flat_args)
        .args(&all_read_std_args)
        .args(&all_read_dataset_kws_args)
        .args(&all_read_shared_args)
        .after_long_help(&std_long_help);

    let spill_cmd = Command::new(SUBCMD_SPILL)
        .about("Dump the spillover matrix if present.")
        .arg(&input_arg)
        .arg(&strategy_arg)
        .arg(&vendor_heuristics_arg)
        .arg(&dataset_index_arg)
        .arg(&delim_arg)
        .args(&all_read_header_args)
        .args(&all_read_offset_args)
        .args(&all_read_flat_args)
        .args(&all_read_std_args)
        .args(&all_read_dataset_kws_args)
        .args(&all_read_shared_args)
        .after_long_help(&std_long_help);

    let data_cmd = Command::new(SUBCMD_DATA)
        .about(format!("Show a table of the {data_seg} segment."))
        .arg(&input_arg)
        .arg(&strategy_arg)
        .arg(&vendor_heuristics_arg)
        .arg(&dataset_index_arg)
        .arg(&delim_arg)
        .args(&all_read_header_args)
        .args(&all_read_offset_args)
        .args(&all_read_flat_args)
        .args(&all_read_std_args)
        .args(&all_read_dataset_kws_args)
        .args(&all_read_dataset_args)
        .args(&all_read_shared_args)
        .after_long_help(&std_long_help);

    let repair_cmd = Command::new(SUBCMD_REPAIR)
        .about("Read a non-compliant FCS file and save as a compliant FCS file.")
        .arg(&input_arg)
        .arg(&output_arg)
        .arg(&strategy_arg)
        .arg(&vendor_heuristics_arg)
        .args(&all_read_header_args)
        .args(&all_read_offset_args)
        .args(&all_read_flat_args)
        .args(&all_read_std_args)
        .args(&all_read_dataset_kws_args)
        .args(&all_read_dataset_args)
        .args(&all_read_shared_args)
        .args(&all_write_args)
        .arg(&skip_arg)
        .arg(&limit_arg)
        .arg(&scan_arg)
        .after_long_help(&std_long_help);

    let summarize_cmd = Command::new(SUBCMD_SUMMARIZE)
        .about("Summarize datasets in FCS file")
        .arg(&input_arg)
        .arg(&strategy_arg)
        .args(&all_read_header_args)
        .args(&all_read_offset_args)
        .args(&all_read_flat_args)
        .args(&all_read_dataset_kws_args)
        .args(&all_read_dataset_args)
        .args(&all_read_shared_args)
        .arg(&skip_arg)
        .arg(&limit_arg)
        .arg(&scan_arg);

    let scan_cmd = Command::new(SUBCMD_SCAN)
        .about("Scan FCS file for dataset boundaries")
        .arg(&input_arg);

    let mut cmd = Command::new("fireflow")
        .about("Read FCS files in standards-compliant manner")
        .arg_required_else_help(true)
        .next_line_help(true)
        .max_term_width(80)
        .subcommand(header_cmd)
        .subcommand(flat_cmd)
        .subcommand(std_cmd)
        .subcommand(meas_cmd)
        .subcommand(spill_cmd)
        .subcommand(data_cmd)
        .subcommand(repair_cmd)
        .subcommand(summarize_cmd)
        .subcommand(scan_cmd);

    let args = cmd.clone().get_matches();

    match args.subcommand() {
        Some((SUBCMD_HEADER, sargs)) => {
            let conf = get_header_config(sargs);
            let filepath = get_path(sargs, INPUT_PATH);
            let (ws, res) = api::fcs_read_header(filepath, DatasetOffset(0), &conf)
                .resolve_commutative(|ws| ws, |s| s);
            print_warnings(ws, &mut stderr)?;
            to_writer(stdout, &res?)?;
            Ok(())
        }

        Some((SUBCMD_FLAT, sargs)) => {
            let conf = get_read_flat_text_config(sargs);
            let filepath = get_path(sargs, INPUT_PATH);
            let skip = get_dataset_index(sargs);
            let (ws, res) = api::fcs_read_flat_texts(filepath, skip, Some(1), false, &conf)
                .resolve_commutative(|ws| ws, |s| s);
            print_warnings(ws, &mut stderr)?;
            to_writer(stdout, &res?[0])?;
            Ok(())
        }

        Some((SUBCMD_SPILL, sargs)) => {
            let subcmd = cmd.find_subcommand_mut(SUBCMD_SPILL).unwrap();
            let conf = get_read_std_text_config(subcmd, sargs);
            let delim = get_delim(sargs);
            let filepath = get_path(sargs, INPUT_PATH);
            let skip = get_dataset_index(sargs);
            let (ws, res) = api::fcs_read_std_texts(filepath, skip, Some(1), false, &conf)
                .resolve_commutative(|ws| ws, |s| s);
            print_warnings(ws, &mut stderr)?;
            let (core, _) = &res?[0];
            core.print_comp_or_spillover_table(&mut stdout, delim)?;
            Ok(())
        }

        Some((SUBCMD_MEAS, sargs)) => {
            let subcmd = cmd.find_subcommand_mut(SUBCMD_MEAS).unwrap();
            let conf = get_read_std_text_config(subcmd, sargs);
            let delim = get_delim(sargs);
            let filepath = get_path(sargs, INPUT_PATH);
            let skip = get_dataset_index(sargs);
            let (ws, res) = api::fcs_read_std_texts(filepath, skip, Some(1), false, &conf)
                .resolve_commutative(|ws| ws, |s| s);
            print_warnings(ws, &mut stderr)?;
            let (core, _) = &res?[0];
            core.print_meas_table(&mut stdout, delim)?;
            Ok(())
        }

        Some((SUBCMD_STD, sargs)) => {
            let subcmd = cmd.find_subcommand_mut(SUBCMD_STD).unwrap();
            let conf = get_read_std_text_config(subcmd, sargs);
            let filepath = get_path(sargs, INPUT_PATH);
            let skip = get_dataset_index(sargs);
            let (ws, res) = api::fcs_read_std_texts(filepath, skip, Some(1), false, &conf)
                .resolve_commutative(|ws| ws, |s| s);
            print_warnings(ws, &mut stderr)?;
            let (core, uncore) = &res?[0];
            let obj = json!({"core": core, "uncore": uncore});
            to_writer(stdout, &obj)?;
            Ok(())
        }

        Some((SUBCMD_DATA, sargs)) => {
            let subcmd = cmd.find_subcommand_mut(SUBCMD_DATA).unwrap();
            let conf = get_read_std_dataset_config(subcmd, sargs);
            let delim = get_delim(sargs);
            let filepath = get_path(sargs, INPUT_PATH);
            let skip = get_dataset_index(sargs);
            let (ws, res) = api::fcs_read_std_datasets(filepath, skip, Some(1), false, &conf)
                .resolve_commutative(|ws| ws, |s| s);
            print_warnings(ws, &mut stderr)?;
            let (core, _) = &res?[0];
            print_parsed_data(&mut stdout, core, delim)?;
            Ok(())
        }

        Some((SUBCMD_REPAIR, sargs)) => {
            let subcmd = cmd.find_subcommand_mut(SUBCMD_REPAIR).unwrap();
            let read_conf = get_read_std_dataset_config(subcmd, sargs);
            let write_conf = get_write_std_dataset_config(sargs);
            let ipath = get_path(sargs, INPUT_PATH);
            let opath = get_path(sargs, OUTPUT_PATH);
            let skip = get_skip(sargs);
            let scan = get_scan(sargs);
            let limit = get_limit(sargs);
            let (read_ws, read_res) =
                api::fcs_read_std_datasets(ipath, skip, limit, scan, &read_conf)
                    .resolve_commutative(|ws| ws, |s| s);
            let (cores, outs): (Vec<_>, Vec<_>) = read_res?.into_iter().unzip();
            let (write_ws, write_res) = api::fcs_write_datasets(opath, &cores[..], &write_conf)
                .resolve_commutative(|ws| ws, |e| e);
            print_warnings(read_ws, &mut stderr)?;
            print_warnings(write_ws, &mut stderr)?;
            let _ = write_res?;
            to_writer(stdout, &outs[..])?;
            Ok(())
        }

        Some((SUBCMD_SUMMARIZE, sargs)) => {
            let subcmd = cmd.find_subcommand_mut(SUBCMD_REPAIR).unwrap();
            let conf = get_read_flat_dataset_config(subcmd, sargs);
            let filepath = get_path(sargs, INPUT_PATH);
            let skip = get_skip(sargs);
            let limit = get_limit(sargs);
            let scan = get_scan(sargs);
            let (ws, res) = api::fcs_summarize(filepath, skip, limit, scan, &conf)
                .resolve_commutative(|ws| ws, |s| s);
            print_warnings(ws, &mut stderr)?;
            to_writer(stdout, &res?)?;
            Ok(())
        }

        Some((SUBCMD_SCAN, sargs)) => {
            #[derive(Serialize)]
            struct Bounds {
                version: tk::Version,
                offset: DatasetOffset,
            }
            let filepath = get_path(sargs, INPUT_PATH);
            let bounds = api::fcs_scan_dataset_boundaries(filepath)?;
            let arr: Vec<_> = bounds
                .into_iter()
                .map(|(version, offset)| Bounds { version, offset })
                .collect();
            to_writer(stdout, &arr)?;
            Ok(())
        }

        _ => Ok(()),
    }
}

fn flag_arg(long: &'static str, help: impl IntoResettable<StyledStr>) -> Arg {
    Arg::new(long)
        .long(long)
        .help(help)
        .action(ArgAction::SetTrue)
}

fn override_flag_arg(long: &'static str, help: impl IntoResettable<StyledStr>) -> Arg {
    // unlike the default flags in clap, this can be overridden by passing true
    // or false. The flat without any value is true, and no flag is false.
    Arg::new(long)
        .long(long)
        .help(help)
        .value_name("BOOL")
        .default_missing_value("true")
        .value_parser(value_parser!(bool))
        .require_equals(true)
        .num_args(0..=1)
}

fn opt_arg<T>(long: &'static str, name: &'static str, help: impl Display) -> Arg
where
    T: FromStr + Clone + Send + Sync + 'static,
    T::Err: Error + 'static,
{
    Arg::new(long)
        .long(long)
        .value_name(name)
        .value_parser(ValueParser::new(parse_opt::<T>))
        .help(format!("{help} Set to 'none' to supply no value."))
}

fn proc_kw_fail_arg(long: &'static str, help_front: impl Display) -> Arg {
    Arg::new(long).long(long).value_name("LEVEL").help(format!(
        "{help_front} Must be one of {error}, {demote_warn}, {demote_silent}, \
         {drop_warn}, or {drop_silent} which will throw an error, demote to \
         non-standard with warning, demote to non-standard silently, drop with \
         warning, or drop silently respectively",
        error = fmt_val(tc::KW_ERROR_LEVEL),
        demote_warn = fmt_val(tc::KW_DEMOTE_WARN_LEVEL),
        demote_silent = fmt_val(tc::KW_DEMOTE_SILENT_LEVEL),
        drop_warn = fmt_val(tc::KW_DROP_WARN_LEVEL),
        drop_silent = fmt_val(tc::KW_DROP_SILENT_LEVEL)
    ))
}

fn tri_flag_arg<T>(long: &'static str, help_front: impl Display) -> Arg
where
    T: From<tc::TriFlag> + Clone + Send + Sync + 'static + cfg::TriErrorFlag,
{
    let parser = ValueParser::new(T::from_partial_str);
    let (what_true, what_false) = if T::FALSE_IS_ERROR {
        ("warning", "error")
    } else {
        ("error", "warning")
    };
    let h = format!(
        "{help_front} If {true_}, throw {what_true}. If {false_}, throw \
         {what_false}. If {silent}, ignore completely.",
        false_ = fmt_val(tc::TRI_FALSE_LEVEL),
        true_ = fmt_val(tc::TRI_TRUE_LEVEL),
        silent = fmt_val(tc::TRI_SILENT_LEVEL),
    );
    Arg::new(long)
        .long(long)
        .value_name("LEVEL")
        .value_parser(parser)
        .help(h)
}

fn format_section(
    header: &'_ str,
    paragraphs: impl IntoIterator<Item = impl Display>,
) -> (ANSIString<'_>, String) {
    let header_style = Style::new().bold();
    let h = header_style.paint(header);
    let s = once(format!("{h}:"))
        .chain(paragraphs.into_iter().map(|s| s.to_string()))
        .join("\n\n    ");
    (h, s)
}

fn get_header_config(sargs: &ArgMatches) -> cfg::ReadHeaderConfig {
    cfg::ReadHeaderConfig {
        header: get_header_inner_config(sargs),
        offset: get_offsets_config(sargs),
    }
}

fn get_header_inner_config(sargs: &ArgMatches) -> cfg::ReadHeaderInnerConfig {
    let strat = get_strategy(sargs);
    let mut conf = cfg::ReadHeaderInnerConfig::new_with_strategy(strat);

    get_correction(sargs, TEXT_CORR, |x| conf.text_correction = x);
    get_correction(sargs, DATA_CORR, |x| conf.data_correction = x);
    get_correction(sargs, ANALYSIS_CORR, |x| conf.analysis_correction = x);

    if let Some(xs) = sargs.get_many::<(i32, i32)>(OTHER_CORR) {
        conf.other_corrections = xs
            .into_iter()
            .copied()
            .map(OffsetsCorrection::from)
            .collect();
    }

    get_opt(sargs, MAX_OTHER, |x| conf.max_other = x);
    get_opt(sargs, OTHER_WIDTH, |x| conf.other_width = x);
    get_opt(sargs, GUESS_OTHER_WIDTH, |x| conf.guess_other_width = x);
    get_flag(sargs, SQUISH_OFFSETS, |x| conf.squish_offsets = x);

    conf
}

fn get_offsets_config(s: &ArgMatches) -> cfg::ReadOffsetConfig {
    let strat = get_strategy(s);
    let mut c = cfg::ReadOffsetConfig::new_with_strategy(strat);

    get_flag(s, ALLOW_PSEUDOEMPTY, |x| c.allow_pseudoempty = x);
    get_opt(s, TRUNCATE_OFFSET_LIMIT, |x| c.dataset_overflow_limit = x);
    get_opt(s, OVERLAP_CORRECTION_LIMIT, |x| {
        c.overlap_correction_limit = x;
    });

    c
}

fn get_header_and_text_config(s: &ArgMatches) -> cfg::ReadHeaderAndTEXTConfig {
    let strat = get_strategy(s);
    let mut c = cfg::ReadHeaderAndTEXTConfig::new_with_strategy(strat);

    get_opt(s, VERSION_OVERRIDE, |x| c.version_override = x);
    get_correction(s, SUPP_TEXT_COR, |x| c.supp_text_correction = x);
    get_opt(s, NEXTDATA_COR, |x| c.nextdata_correction = x);

    get_opt(s, ALLOW_DUPLICATED_SUPP_TEXT, |x| {
        c.allow_duplicated_supp_text = x;
    });
    get_flag(s, IGNORE_SUPP_TEXT, |x| c.ignore_supp_text = x);
    get_opt(s, DELIM_ESCAPE_MODE, |x| c.delim_escape_mode = x);
    get_opt(s, ALLOW_NON_ASCII_DELIM, |x| c.allow_non_ascii_delim = x);
    get_opt(s, ALLOW_EVEN_DELIMS, |x| {
        c.allow_even_delims = x;
    });
    get_opt(s, ALLOW_NONUNIQUE, |x| c.allow_nonunique = x);
    get_opt(s, ALLOW_ODD_TOKENS, |x| c.allow_odd_tokens = x);
    get_opt(s, ALLOW_EMPTY_KEYS, |x| c.allow_empty_keys = x);
    get_opt(s, ALLOW_DELIM_AT_BOUNDARY, |x| {
        c.allow_delim_at_boundary = x;
    });
    get_opt(s, USE_ENCODING, |x| c.use_encoding = x);
    get_opt(s, ALLOW_NON_ASCII_KEYS, |x| {
        c.allow_non_ascii_keys = x;
    });
    get_opt(s, ALLOW_NON_UTF8_VALUES, |x| c.allow_non_utf8_values = x);
    get_opt(s, ALLOW_MISSING_SUPP_TEXT, |x| {
        c.allow_missing_supp_text = x;
    });
    get_opt(s, ALLOW_SUPP_TEXT_OWN_DELIM, |x| {
        c.allow_supp_text_own_delim = x;
    });
    get_opt(s, ALLOW_MISSING_NEXTDATA, |x| {
        c.allow_missing_nextdata = x;
    });
    get_opt(s, TRIM_VALUE_WHITESPACE, |x| {
        c.trim_value_whitespace = x;
    });

    c
}

fn get_std_kws_config(s: &ArgMatches) -> cfg::ReadStdKeywordsConfig {
    let strat = get_strategy(s);
    let _vendor_heuristics = get_vendor_heuristics(s);
    let mut c = cfg::ReadStdKeywordsConfig::new_with_strategy(strat);

    get_flag(s, DEDUP_MEAS_NAMES, |x| c.dedup_measurement_names = x);
    get_flag(s, TRIM_INTRA_VALUE_WHITESPACE, |x| {
        c.trim_intra_value_whitespace = x;
    });
    get_opt(s, TIME_MEAS_PATTERN, |x| {
        c.time_meas_pattern = Selector::Root(x);
    });

    get_opt(s, FORCE_LINEAR_SCALE, |x| c.force_linear_scale = x);

    if let Some(xs) = s.get_many::<tc::TemporalOpticalKey>(IGNORE_TIME_OPTICAL_KEYS) {
        c.ignore_time_optical_keys = xs.copied().collect::<HashSet<_>>().into();
    }

    get_opt(s, PROCESS_TIME_OPTICAL_KEYS, |x| {
        c.process_time_optical_keys = x;
    });
    get_opt(s, ALLOW_MISSING_TIME, |x| c.allow_missing_time = x);
    get_opt(s, ADD_MISSING_TIMESTEP, |x| c.add_missing_timestep = x);
    get_opt(s, SPILLOVER_MEASUREMENT_MODE, |x| {
        c.spillover_measurement_mode = x;
    });
    get_opt(s, DATE_PATTERN, |x| c.date_pattern = Selector::Root(x));
    get_opt(s, TIME_PATTERN, |x| c.time_pattern = Selector::Root(x));
    get_opt(s, DATETIME_PATTERN, |x| {
        c.datetime_pattern = Selector::Root(x);
    });
    get_opt(s, LAST_MODIFIED_PATTERN, |x| {
        c.last_modified_pattern = Selector::Root(x);
    });
    get_flag(s, ALLOW_OTHER_FEATURE, |x| c.allow_other_feature = x);
    get_opt(s, PROCESS_PSEUDOSTANDARD, |x| c.process_pseudostandard = x);
    get_opt(s, PROCESS_HYPER_PAR, |x| c.process_hyper_par = x);
    get_opt(s, PROCESS_OTHER_VERSION, |x| c.process_other_version = x);
    get_opt(s, PROCESS_EXTRA_TIMESTEP, |x| c.process_extra_timestep = x);
    get_flag(s, FIX_LOG_SCALE_OFFSETS, |x| c.fix_log_scale_offsets = x);
    get_flag(s, DISALLOW_LOCALTIME, |x| c.disallow_localtime = x);

    c
}

fn get_data_kws_config(cmd: &Command, s: &ArgMatches) -> cfg::ReadDataKeywordsConfig {
    let strat = get_strategy(s);
    let mut c = cfg::ReadDataKeywordsConfig::new_with_strategy(strat);

    get_many::<KeyStringOrPattern, _, _>(s, IGNORE_STD_KEYS, |xs| {
        c.ignore_standard_keys = AppendableSelector::root(xs);
    });
    get_many::<KeyStringOrPattern, _, _>(s, PROMOTE_TO_STD, |xs| {
        c.promote_to_standard = AppendableSelector::root(xs);
    });
    get_many::<KeyStringOrPattern, _, _>(s, DEMOTE_FROM_STD, |xs| {
        c.demote_from_standard = AppendableSelector::root(xs);
    });

    if let Some(xs) = s.get_many::<BiKeystringPair>(RENAME_STD_KEYS) {
        let Ok(ys) = xs
            .cloned()
            .collect::<HashMap<_, _>>()
            .try_into()
            .map_err(|e| post_validation_error(cmd, RENAME_STD_KEYS, e).exit());
        c.rename_standard_keys = AppendableSelector::root(ys);
    }

    let parse_keystring_pair = |name: &str| {
        s.get_many::<KeystringStringPair>(name)
            .map(|xs| xs.cloned().collect())
    };

    let _ = parse_keystring_pair(REPLACE_STD_KEY_VALS)
        .map(|x| c.replace_standard_key_values = AppendableSelector::root(x));
    let _ = parse_keystring_pair(APPEND_STD_KEY_VALS)
        .map(|x| c.append_standard_keywords = AppendableSelector::root(x));

    get_many(s, SUB_STD_KEY_VALS, |xs| {
        c.substitute_standard_key_values = AppendableSelector::root(xs);
    });

    get_opt(s, ALLOW_REPAIR_NON_UNIQUE, |x| {
        c.allow_repair_non_unique = x;
    });

    get_correction(s, TEXT_DATA_COR, |x| c.text_data_correction = x);
    get_correction(s, TEXT_ANALYSIS_COR, |x| c.text_analysis_correction = x);
    get_flag(s, IGNORE_TEXT_DATA_OFFSETS, |x| {
        c.ignore_text_data_offsets = x;
    });
    get_flag(s, IGNORE_TEXT_ANALYSIS_OFFSETS, |x| {
        c.ignore_text_analysis_offsets = x;
    });
    get_opt(s, ALLOW_HEADER_TEXT_OFFSET_MISMATCH, |x| {
        c.allow_header_text_offset_mismatch = x;
    });
    get_opt(s, ALLOW_MISSING_REQUIRED_OFFSETS, |x| {
        c.allow_missing_required_offsets = x;
    });
    get_opt(s, PROCESS_OPTIONAL_FAILURE, |x| {
        c.process_optional_failure = x;
    });
    get_opt(s, FIX_INT_WIDTHS, |x| c.fix_int_widths = x);
    get_opt(s, BYTEORD_OVERRIDE, |x| c.byteord_override = x);
    get_opt(s, DISALLOW_RANGE_TRUNCATION, |x| {
        c.disallow_range_truncation = x;
    });

    c
}

fn get_dataset_config(s: &ArgMatches) -> cfg::ReadDatasetConfig {
    let strat = get_strategy(s);
    let mut c = cfg::ReadDatasetConfig::new_with_strategy(strat);

    get_opt(s, DATA_REMAINDER_LIMIT, |x| c.data_remainder_limit = x);
    get_opt(s, ALLOW_TOT_MISMATCH, |x| c.allow_tot_mismatch = x);
    get_opt(s, ALLOW_UNEVEN_EVENT_WIDTH, |x| {
        c.allow_uneven_event_width = x;
    });
    get_opt(s, OVER_BITMASK_ACTION, |x| {
        c.over_bitmask_action = x;
    });
    get_opt(s, OVER_RANGE_ACTION, |x| c.over_range_action = x);
    get_opt(s, ALLOW_MISSING_CRC, |x| c.allow_missing_crc = x);
    get_opt(s, ALLOW_MISMATCH_CRC, |x| c.allow_mismatch_crc = x);
    get_opt(s, COMPUTE_CRC, |x| c.compute_crc = x);
    get_opt(s, ROW_BUFFER_SIZE, |x| c.row_buffer_size = x);

    c
}

fn get_read_shared_config(sargs: &ArgMatches) -> cfg::ReadSharedConfig {
    cfg::ReadSharedConfig {
        warnings_are_errors: sargs.get_flag(WARNINGS_ARE_ERRORS),
        hide_warnings: sargs.get_flag(HIDE_WARNINGS),
    }
}

fn get_read_flat_text_config(sargs: &ArgMatches) -> cfg::ReadFlatTEXTConfig {
    cfg::ReadFlatTEXTConfig {
        header: get_header_inner_config(sargs),
        flat: get_header_and_text_config(sargs),
        offset: get_offsets_config(sargs),
        shared: get_read_shared_config(sargs),
    }
}

fn get_read_std_text_config(cmd: &Command, sargs: &ArgMatches) -> cfg::ReadStdTEXTConfig {
    cfg::ReadStdTEXTConfig {
        header: get_header_inner_config(sargs),
        flat: get_header_and_text_config(sargs),
        offset: get_offsets_config(sargs),
        standard: get_std_kws_config(sargs),
        layout: get_data_kws_config(cmd, sargs),
        shared: get_read_shared_config(sargs),
    }
}

fn get_read_flat_dataset_config(cmd: &Command, sargs: &ArgMatches) -> cfg::ReadFlatDatasetConfig {
    cfg::ReadFlatDatasetConfig {
        header: get_header_inner_config(sargs),
        flat: get_header_and_text_config(sargs),
        offset: get_offsets_config(sargs),
        layout: get_data_kws_config(cmd, sargs),
        data: get_dataset_config(sargs),
        shared: get_read_shared_config(sargs),
    }
}

fn get_read_std_dataset_config(cmd: &Command, sargs: &ArgMatches) -> cfg::ReadStdDatasetConfig {
    cfg::ReadStdDatasetConfig {
        header: get_header_inner_config(sargs),
        flat: get_header_and_text_config(sargs),
        offset: get_offsets_config(sargs),
        standard: get_std_kws_config(sargs),
        layout: get_data_kws_config(cmd, sargs),
        data: get_dataset_config(sargs),
        shared: get_read_shared_config(sargs),
    }
}

fn get_write_std_dataset_config(sargs: &ArgMatches) -> cfg::WriteDatasetInnerConfig {
    let mut conf = cfg::WriteDatasetInnerConfig::default();

    get_opt(sargs, WRITE_DELIM, |x| conf.text.delim = x);
    get_opt(sargs, BIG_OTHER, |x: bool| conf.text.big_other = x.into());
    conf
}

fn get_path<'a>(sargs: &'a ArgMatches, name: &'a str) -> &'a PathBuf {
    sargs.get_one::<PathBuf>(name).expect("path is required")
}

fn get_strategy(sargs: &ArgMatches) -> tc::ReadStrategy {
    sargs.get_one(STRATEGY).copied().unwrap_or_default()
}

fn get_vendor_heuristics(sargs: &ArgMatches) -> bool {
    sargs.get_flag(VENDOR_HEURISTICS)
}

fn get_dataset_index(sargs: &ArgMatches) -> Option<usize> {
    sargs.get_one::<usize>(DATASET_INDEX).copied()
}

fn get_skip(sargs: &ArgMatches) -> Option<usize> {
    sargs.get_one::<usize>(SKIP).copied()
}

fn get_limit(sargs: &ArgMatches) -> Option<usize> {
    sargs.get_one::<usize>(LIMIT).copied()
}

fn get_scan(sargs: &ArgMatches) -> bool {
    sargs.get_flag(SCAN)
}

fn get_delim(sargs: &ArgMatches) -> u8 {
    sargs.get_one::<u8>(PRINT_DELIM).copied().unwrap()
}

fn get_opt<T, F>(sargs: &ArgMatches, name: &str, f: F)
where
    F: FnMut(T),
    T: Clone + Sync + Send + 'static,
{
    let _ = sargs.get_one(name).cloned().map(f);
}

fn get_many<T, F, X>(sargs: &ArgMatches, name: &str, mut f: F)
where
    X: FromIterator<T>,
    F: FnMut(X),
    T: Clone + Sync + Send + 'static,
{
    let _ = sargs.get_many::<T>(name).map(|xs| f(xs.cloned().collect()));
}

fn get_flag<T, F>(sargs: &ArgMatches, name: &str, f: F)
where
    F: FnMut(T),
    T: Copy + Sync + Send + 'static + From<bool>,
{
    let _ = sargs.get_one::<bool>(name).copied().map(T::from).map(f);
}

fn get_correction<I, S, F>(sargs: &ArgMatches, name: &str, f: F)
where
    F: FnMut(OffsetsCorrection<I, S>),
{
    let _ = sargs
        .get_one::<(i32, i32)>(name)
        .copied()
        .map(Into::into)
        .map(f);
}

fn parse_opt<T>(s: &str) -> StrResult<Option<T>>
where
    T: FromStr,
    T::Err: Error + 'static,
{
    if s == "none" {
        return Ok(None);
    }
    Ok(Some(s.parse::<T>().map_err(|e| e.to_string())?))
}

fn parse_delim(s: &str) -> StrResult<u8> {
    let c = s.parse::<char>().map_err(|e| e.to_string())?;
    c.try_into()
        .map_err(|_| "must be a single ASCII character".into())
}

fn parse_offsets(s: &str) -> StrResult<(i32, i32)> {
    let ss = s.split(',').collect::<Vec<_>>();
    match &ss[..] {
        [a, b] => {
            let aa = a.parse::<i32>().map_err(|e| e.to_string())?;
            let bb = b.parse::<i32>().map_err(|e| e.to_string())?;
            Ok((aa, bb))
        }
        _ => Err("offsets must be a pair of integers like 'X,Y'".into()),
    }
}

fn parse_other_width(s: &str) -> StrResult<OtherWidth> {
    let x = s.parse::<u8>().map_err(|e| e.to_string())?;
    OtherWidth::try_from(x).map_err(|e| e.to_string())
}

fn parse_two_keystring_pair(s: &str) -> StrResult<BiKeystringPair> {
    let (k, v) = s.split_once(',').ok_or("must be a comma separated pair")?;
    let kf = k.parse::<KeyString>().map_err(|e| e.to_string())?;
    let vf = v.parse::<KeyString>().map_err(|e| e.to_string())?;
    Ok((kf, vf))
}

fn parse_keystring_string_pair(s: &str) -> StrResult<KeystringStringPair> {
    let (k, v) = s.split_once(',').ok_or("must be a comma separated pair")?;
    let kf = k.parse::<KeyString>().map_err(|e| e.to_string())?;
    let vf = v.parse::<NEString>().map_err(|e| e.to_string())?;
    Ok((kf, vf))
}

fn parse_sub_pattern_pair(s: &str) -> StrResult<SubPatternPair> {
    let (k, v) = s.split_once(',').ok_or("must be a comma separated pair")?;
    let kf = k.parse::<KeyStringOrPattern>().map_err(|e| e.to_string())?;
    let vf = parse_sub_pattern_inner(v).map_err(|e| e.to_string())?;
    Ok((kf, vf))
}

fn parse_sub_pattern_inner(s: &str) -> AppResult<SubPattern> {
    let (op, r0) = s
        .split_at_checked(1)
        .ok_or("sub pattern must not be empty")?;
    if op != "s" {
        return Err(format!("sub pattern must start with 's', got {op}").into());
    }
    if r0.is_empty() {
        return Err("no delimiter found".into());
    }
    let (delim, r1) = r0
        .split_at_checked(1)
        .ok_or("sub pattern delimiter is not a valid UTF-8 byte")?;
    let parts: Vec<_> = r1.split(delim).collect();
    let (from, to, global) = match &parts[..] {
        [x, y] | [x, y, ""] => (*x, *y, false),
        [x, y, "g"] => (*x, *y, true),
        _ => {
            let msg = "sub pattern string must be like 's<D><FROM><D><TO>[<D>g]' \
                       where 'D' is a delimiter (any character), FROM is a \
                       regular expression and TO is a replacement pattern";
            return Err(msg.into());
        }
    };
    let r = Regex::new(from)?;
    Ok(SubPattern::try_new(r, to.to_owned(), global)?)
}

fn parse_fix_int_widths(s: &str) -> StrResult<FixIntWidths> {
    if s == tc::FIX_INT_WIDTH_NEVER_LEVEL.as_str() {
        return Ok(FixIntWidths::Never);
    } else if s == tc::FIX_INT_WIDTH_NEXT_BYTE_LEVEL.as_str() {
        return Ok(FixIntWidths::NextByte);
    }
    Ok(FixIntWidths::Explicit(
        s.parse::<Bytes>().map_err(|e| e.to_string())?,
    ))
}

fn parse_byteord_override(s: &str) -> StrResult<ByteordOverride> {
    if s == tc::BYTEORD_OVERRIDE_ENDIAN_LEVEL.as_str() {
        return Ok(ByteordOverride::Endian);
    } else if s == tc::BYTEORD_OVERRIDE_NONE_LEVEL.as_str() {
        return Ok(ByteordOverride::None);
    }
    Ok(ByteordOverride::Explicit(
        s.parse::<ByteOrd2_0>().map_err(|e| e.to_string())?,
    ))
}

pub fn print_parsed_data<W: Write>(w: &mut W, core: &AnyCoreDataset, delim: u8) -> io::Result<()> {
    // 32k / 8 bytes since we are storing everything as u64
    const BUF_SIZE: usize = 1 << 12;

    let mut ibuf = IBuf::new();
    let mut fbuf = FBuf::new();

    let df = core.as_data();
    let nrows = df.nrows();
    let cols: Vec<_> = df.iter().collect();
    let ncols = cols.len();
    let dtypes = core.datatypes();
    assert_eq!(dtypes.len(), ncols, "datatypes are wrong length");

    if ncols == 0 {
        return Ok(());
    }

    let mut first = true;
    for n in core.shortnames() {
        if !first {
            w.write_all(&[delim])?;
        }
        first = false;
        w.write_all(AsRef::<str>::as_ref(&n).as_bytes())?;
    }
    writeln!(w)?;

    let rows_per_buf = BUF_SIZE / ncols;
    let real_buf_size = ncols * rows_per_buf;
    let buf_reads = nrows.div_ceil(rows_per_buf);
    let buf_tail_rows = nrows % rows_per_buf;

    let mut buf = Vec::with_capacity(real_buf_size);

    for b in 0..buf_reads {
        buf.clear();
        let next_rows = if b + 1 == buf_reads && buf_tail_rows > 0 {
            buf_tail_rows
        } else {
            rows_per_buf
        };
        for col in &cols {
            for r in 0..next_rows {
                buf.push(col.as_u64(r + rows_per_buf * b));
            }
        }
        for r in 0..next_rows {
            first = true;
            for (c, d) in dtypes.iter().enumerate() {
                let i = r + c * next_rows;
                let v = buf[i];
                let bs = match d {
                    AlphaNumType::Float => {
                        let vv = u32::try_from(v).expect("f32 should be encoded as u32");
                        fbuf.format(f32::from_bits(vv)).as_bytes()
                    }
                    AlphaNumType::Double => fbuf.format(f64::from_bits(v)).as_bytes(),
                    _ => ibuf.format(v).as_bytes(),
                };
                if !first {
                    w.write_all(&[delim])?;
                }
                first = false;
                w.write_all(bs)?;
            }
            writeln!(w)?;
        }
    }
    Ok(())
}

fn print_warnings<W: Write, Warn: Display>(
    ws: impl IntoIterator<Item = Warn>,
    w: &mut W,
) -> io::Result<()> {
    for warn in ws {
        writeln!(w, "WARNING: {warn}")?;
    }
    Ok(())
}

fn post_validation_error(cmd: &Command, arg_name: &str, msg: impl Display) -> clap::Error {
    let lit = cmd.get_styles().get_literal();
    clap::Error::raw(
        ErrorKind::ValueValidation,
        format!("validation failed for '{lit}--{arg_name}{lit:#}': {msg}\n"),
    )
    .with_cmd(cmd)
}

fn fmt_val(val: impl Display) -> String {
    format!("'{val}'")
}

type AppResult<T> = Result<T, Box<dyn Error>>;

type StrResult<T> = Result<T, String>;

type BiKeystringPair = (KeyString, KeyString);

type KeystringStringPair = (KeyString, NEString);

type SubPatternPair = (KeyStringOrPattern, SubPattern);

const SUBCMD_HEADER: &str = "header";

const SUBCMD_FLAT: &str = "flat";

const SUBCMD_STD: &str = "std";

const SUBCMD_DATA: &str = "data";

const SUBCMD_SUMMARIZE: &str = "summarize";

const SUBCMD_SCAN: &str = "scan";

const SUBCMD_MEAS: &str = "measurements";

const SUBCMD_SPILL: &str = "spillover";

const SUBCMD_REPAIR: &str = "repair";

// header config flags

const TEXT_CORR: &str = cli_arg!(ReadHeaderInnerConfig::text_correction);
const DATA_CORR: &str = cli_arg!(ReadHeaderInnerConfig::data_correction);
const ANALYSIS_CORR: &str = cli_arg!(ReadHeaderInnerConfig::analysis_correction);
const OTHER_CORR: &str = cli_arg!(ReadHeaderInnerConfig::other_corrections);
const MAX_OTHER: &str = cli_arg!(ReadHeaderInnerConfig::max_other);
const OTHER_WIDTH: &str = cli_arg!(ReadHeaderInnerConfig::other_width);
const GUESS_OTHER_WIDTH: &str = cli_arg!(ReadHeaderInnerConfig::guess_other_width);
const SQUISH_OFFSETS: &str = cli_arg!(ReadHeaderInnerConfig::squish_offsets);

// offset config flags

const ALLOW_PSEUDOEMPTY: &str = cli_arg!(ReadOffsetConfig::allow_pseudoempty);
const TRUNCATE_OFFSET_LIMIT: &str = cli_arg!(ReadOffsetConfig::dataset_overflow_limit);
const OVERLAP_CORRECTION_LIMIT: &str = cli_arg!(ReadOffsetConfig::overlap_correction_limit);

// flat text config flags

const VERSION_OVERRIDE: &str = cli_arg!(ReadHeaderAndTEXTConfig::version_override);

const SUPP_TEXT_COR: &str = cli_arg!(ReadHeaderAndTEXTConfig::supp_text_correction);

const NEXTDATA_COR: &str = cli_arg!(ReadHeaderAndTEXTConfig::nextdata_correction);

const ALLOW_DUPLICATED_SUPP_TEXT: &str =
    cli_arg!(ReadHeaderAndTEXTConfig::allow_duplicated_supp_text);

const IGNORE_SUPP_TEXT: &str = cli_arg!(ReadHeaderAndTEXTConfig::ignore_supp_text);

const DELIM_ESCAPE_MODE: &str = cli_arg!(ReadHeaderAndTEXTConfig::delim_escape_mode);

const ALLOW_NON_ASCII_DELIM: &str = cli_arg!(ReadHeaderAndTEXTConfig::allow_non_ascii_delim);

const ALLOW_EVEN_DELIMS: &str = cli_arg!(ReadHeaderAndTEXTConfig::allow_even_delims);

const ALLOW_NONUNIQUE: &str = cli_arg!(ReadHeaderAndTEXTConfig::allow_nonunique);

const ALLOW_ODD_TOKENS: &str = cli_arg!(ReadHeaderAndTEXTConfig::allow_odd_tokens);

const ALLOW_EMPTY_KEYS: &str = cli_arg!(ReadHeaderAndTEXTConfig::allow_empty_keys);

const ALLOW_DELIM_AT_BOUNDARY: &str = cli_arg!(ReadHeaderAndTEXTConfig::allow_delim_at_boundary);

const USE_ENCODING: &str = cli_arg!(ReadHeaderAndTEXTConfig::use_encoding);

const ALLOW_NON_ASCII_KEYS: &str = cli_arg!(ReadHeaderAndTEXTConfig::allow_non_ascii_keys);

const ALLOW_NON_UTF8_VALUES: &str = cli_arg!(ReadHeaderAndTEXTConfig::allow_non_utf8_values);

const ALLOW_MISSING_SUPP_TEXT: &str = cli_arg!(ReadHeaderAndTEXTConfig::allow_missing_supp_text);

const ALLOW_SUPP_TEXT_OWN_DELIM: &str =
    cli_arg!(ReadHeaderAndTEXTConfig::allow_supp_text_own_delim);

const ALLOW_MISSING_NEXTDATA: &str = cli_arg!(ReadHeaderAndTEXTConfig::allow_missing_nextdata);

const TRIM_VALUE_WHITESPACE: &str = cli_arg!(ReadHeaderAndTEXTConfig::trim_value_whitespace);

// std keyword config flags

const DEDUP_MEAS_NAMES: &str = cli_arg!(ReadStdKeywordsConfig::dedup_measurement_names);

const TRIM_INTRA_VALUE_WHITESPACE: &str =
    cli_arg!(ReadStdKeywordsConfig::trim_intra_value_whitespace);

const TIME_MEAS_PATTERN: &str = cli_arg!(ReadStdKeywordsConfig::time_meas_pattern);

const ALLOW_MISSING_TIME: &str = cli_arg!(ReadStdKeywordsConfig::allow_missing_time);

const ADD_MISSING_TIMESTEP: &str = cli_arg!(ReadStdKeywordsConfig::add_missing_timestep);

const FORCE_LINEAR_SCALE: &str = cli_arg!(ReadStdKeywordsConfig::force_linear_scale);

const IGNORE_TIME_OPTICAL_KEYS: &str = cli_arg!(ReadStdKeywordsConfig::ignore_time_optical_keys);

const PROCESS_TIME_OPTICAL_KEYS: &str = cli_arg!(ReadStdKeywordsConfig::process_time_optical_keys);

const SPILLOVER_MEASUREMENT_MODE: &str =
    cli_arg!(ReadStdKeywordsConfig::spillover_measurement_mode);

const DATE_PATTERN: &str = cli_arg!(ReadStdKeywordsConfig::date_pattern);

const TIME_PATTERN: &str = cli_arg!(ReadStdKeywordsConfig::time_pattern);

const DATETIME_PATTERN: &str = cli_arg!(ReadStdKeywordsConfig::datetime_pattern);

const LAST_MODIFIED_PATTERN: &str = cli_arg!(ReadStdKeywordsConfig::last_modified_pattern);

const ALLOW_OTHER_FEATURE: &str = cli_arg!(ReadStdKeywordsConfig::allow_other_feature);

const PROCESS_PSEUDOSTANDARD: &str = cli_arg!(ReadStdKeywordsConfig::process_pseudostandard);

const PROCESS_HYPER_PAR: &str = cli_arg!(ReadStdKeywordsConfig::process_hyper_par);

const PROCESS_OTHER_VERSION: &str = cli_arg!(ReadStdKeywordsConfig::process_other_version);

const PROCESS_EXTRA_TIMESTEP: &str = cli_arg!(ReadStdKeywordsConfig::process_extra_timestep);

const FIX_LOG_SCALE_OFFSETS: &str = cli_arg!(ReadStdKeywordsConfig::fix_log_scale_offsets);

const DISALLOW_LOCALTIME: &str = cli_arg!(ReadStdKeywordsConfig::disallow_localtime);

// data keyword config flags

const IGNORE_STD_KEYS: &str = cli_arg!(ReadDataKeywordsConfig::ignore_standard_keys);

const PROMOTE_TO_STD: &str = cli_arg!(ReadDataKeywordsConfig::promote_to_standard);

const DEMOTE_FROM_STD: &str = cli_arg!(ReadDataKeywordsConfig::demote_from_standard);

const RENAME_STD_KEYS: &str = cli_arg!(ReadDataKeywordsConfig::rename_standard_keys);

const REPLACE_STD_KEY_VALS: &str = cli_arg!(ReadDataKeywordsConfig::replace_standard_key_values);

const APPEND_STD_KEY_VALS: &str = cli_arg!(ReadDataKeywordsConfig::append_standard_keywords);

const SUB_STD_KEY_VALS: &str = cli_arg!(ReadDataKeywordsConfig::substitute_standard_key_values);

const ALLOW_REPAIR_NON_UNIQUE: &str = cli_arg!(ReadDataKeywordsConfig::allow_repair_non_unique);

const TEXT_DATA_COR: &str = cli_arg!(ReadDataKeywordsConfig::text_data_correction);

const TEXT_ANALYSIS_COR: &str = cli_arg!(ReadDataKeywordsConfig::text_analysis_correction);

const IGNORE_TEXT_DATA_OFFSETS: &str = cli_arg!(ReadDataKeywordsConfig::ignore_text_data_offsets);

const IGNORE_TEXT_ANALYSIS_OFFSETS: &str =
    cli_arg!(ReadDataKeywordsConfig::ignore_text_analysis_offsets);

const ALLOW_HEADER_TEXT_OFFSET_MISMATCH: &str =
    cli_arg!(ReadDataKeywordsConfig::allow_header_text_offset_mismatch);

const ALLOW_MISSING_REQUIRED_OFFSETS: &str =
    cli_arg!(ReadDataKeywordsConfig::allow_missing_required_offsets);

const PROCESS_OPTIONAL_FAILURE: &str = cli_arg!(ReadDataKeywordsConfig::process_optional_failure);

const FIX_INT_WIDTHS: &str = cli_arg!(ReadDataKeywordsConfig::fix_int_widths);

const BYTEORD_OVERRIDE: &str = cli_arg!(ReadDataKeywordsConfig::byteord_override);

const DISALLOW_RANGE_TRUNCATION: &str = cli_arg!(ReadDataKeywordsConfig::disallow_range_truncation);

// read data config flags

const DATA_REMAINDER_LIMIT: &str = cli_arg!(ReadDatasetConfig::data_remainder_limit);
const ALLOW_UNEVEN_EVENT_WIDTH: &str = cli_arg!(ReadDatasetConfig::allow_uneven_event_width);
const ALLOW_TOT_MISMATCH: &str = cli_arg!(ReadDatasetConfig::allow_tot_mismatch);
const OVER_BITMASK_ACTION: &str = cli_arg!(ReadDatasetConfig::over_bitmask_action);
const OVER_RANGE_ACTION: &str = cli_arg!(ReadDatasetConfig::over_range_action);
const ALLOW_MISSING_CRC: &str = cli_arg!(ReadDatasetConfig::allow_missing_crc);
const ALLOW_MISMATCH_CRC: &str = cli_arg!(ReadDatasetConfig::allow_mismatch_crc);
const COMPUTE_CRC: &str = cli_arg!(ReadDatasetConfig::compute_crc);
const ROW_BUFFER_SIZE: &str = cli_arg!(ReadDatasetConfig::row_buffer_size);

// shared config flags

const WARNINGS_ARE_ERRORS: &str = cli_arg!(ReadSharedConfig::warnings_are_errors);
const HIDE_WARNINGS: &str = cli_arg!(ReadSharedConfig::hide_warnings);

// other flags

const PRINT_DELIM: &str = "print-delim";

const DATASET_INDEX: &str = "dataset-index";

const WRITE_DELIM: &str = "write-delim";

const BIG_OTHER: &str = "skip-conversion-check";

const SKIP: &str = "skip";

const LIMIT: &str = "limit";

const SCAN: &str = "scan";

const INPUT_PATH: &str = "input-path";

const OUTPUT_PATH: &str = "output-path";

const STRATEGY: &str = "strategy";

const VENDOR_HEURISTICS: &str = "vendor-heuristics";

const CHRONO_REF: &str = "https://docs.rs/chrono/latest/chrono/format/strftime/index.html";

const REGEXP_REF: &str = "https://docs.rs/regex/latest/regex/#syntax";

const REGEXP_REP_REF: &str = "https://docs.rs/regex/latest/regex/struct.Regex.html#method.replace";

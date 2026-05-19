use fireflow_core::api::{
    fcs_read_flat_texts, fcs_read_header, fcs_read_std_datasets, fcs_read_std_texts, fcs_summarize,
    fcs_write_datasets,
};
use fireflow_core::config::{
    self, AllowDelimAtBoundary, AllowDuplicatedSuppTEXT, AllowEmptyKeys, AllowEvenDelims,
    AllowMissingNextdata, AllowMissingRequiredOffsets, AllowMissingSuppTEXT, AllowMissingTime,
    AllowNonAsciiDelim, AllowNonAsciiKeywords, AllowNonUtf8, AllowNonunique, AllowOddTokens,
    AllowSuppTEXTOwnDelim, AllowTotMismatch, AllowUnevenEventWidth, DataRemainderLimit,
    DatasetOffset, DisallowOverRange, DisallowRangeTrunc, HasStrategy as _, NonStdMeasPatternOpt,
    OverlapCorrectionLimit, ProcessExtraTimestep, ProcessHyperPar, ProcessOptionalFailure,
    ProcessOtherVersion, ProcessPseudostandard, TimeMeasNamePattern, TriErrorFlag,
    TruncateOffsetLimit, VersionOverride, WriteDatasetInnerConfig,
};
use fireflow_core::core::AnyCoreDataset;
use fireflow_core::segment::OffsetCorrection;
use fireflow_core::text::keywords::{AlphaNumType, ByteOrd2_0};
use fireflow_core::validated::ascii_range::OtherWidth;
use fireflow_core::validated::datepattern::DatePattern;
use fireflow_core::validated::keys::{KeyString, KeyStringOrPattern};
use fireflow_core::validated::nonstd_meas_pattern::NonStdMeasPattern;
use fireflow_core::validated::sub_pattern::SubPattern;
use fireflow_core::validated::textdelim::TEXTDelim;
use fireflow_core::validated::timepattern::TimePattern;
use fireflow_types::config::{
    self as tc, BASE60_SECOND_SPEC, BASE100_SECOND_SPEC, CHECK_RANGE_ALL_LEVEL,
    CHECK_RANGE_BITMASK_ONLY_LEVEL, CHECK_RANGE_INT_ONLY_LEVEL, CHECK_RANGE_NONE_LEVEL,
    DEDUP_PNN_SEP, DEFAULT_DATE_FORMAT, DEFAULT_TIME_FORMAT_2_0, DEFAULT_TIME_FORMAT_3_0,
    DEFAULT_TIME_FORMAT_3_1, DELIM_ESCAPED_LEVEL, DELIM_GUESS_ESCAPED_LEVEL,
    DELIM_GUESS_UNESCAPED_LEVEL, DELIM_UNESCAPED_LEVEL, ENCODING_GUESS_LEVEL,
    ENCODING_SINGLE_LEVEL, ENCODING_UTF8_LEVEL, FORCE_LINEAR_ALL_LEVEL, FORCE_LINEAR_NON_INT_LEVEL,
    FORCE_LINEAR_NONE_LEVEL, FORCE_LINEAR_TIME_LEVEL, KW_DEMOTE_SILENT_LEVEL, KW_DEMOTE_WARN_LEVEL,
    KW_DROP_SILENT_LEVEL, KW_DROP_WARN_LEVEL, KW_ERROR_LEVEL, MISMATCH_ERROR_LEVEL,
    MISMATCH_HEADER_SILENT_LEVEL, MISMATCH_HEADER_WARN_LEVEL, MISMATCH_TEXT_SILENT_LEVEL,
    MISMATCH_TEXT_WARN_LEVEL, NON_STD_MEAS_INDEX_PAT, NON_STD_MEAS_PAT_DEFAULT,
    OTHER_WIDTH_ERROR_LEVEL, OTHER_WIDTH_NONE_LEVEL, OTHER_WIDTH_SILENT_LEVEL,
    OTHER_WIDTH_WARN_LEVEL, PATTERN_DELIMITER, READ_STRATEGY_SCALPAL_LEVEL,
    READ_STRATEGY_SLEDGEHAMMER_LEVEL, READ_STRATEGY_STRICT_LEVEL, ReadStrategy, RowBufferSize,
    SPILLOVER_GUESS_LEVEL, SPILLOVER_INDEXED_LEVEL, SPILLOVER_NAMED_LEVEL,
    TIME_MEAS_NAME_PATTERN_DEFAULT, TIME_MEAS_NAME_PATTERN_NONE, TMP_OPT_DEMOTE_SILENT_LEVEL,
    TMP_OPT_DEMOTE_WARN_LEVEL, TMP_OPT_DROP_SILENT_LEVEL, TMP_OPT_DROP_WARN_LEVEL,
    TRI_SILENT_LEVEL, TRI_TRUE_LEVEL, TRIM_BLANK_SILENT_LEVEL, TRIM_BLANK_WARN_LEVEL,
    TRIM_ERROR_LEVEL, TRIM_NONE_LEVEL, VERSION_EARLIEST_LEVEL, VERSION_LATEST_LEVEL,
    VERSION_LOOSE_LEVEL, VERSION_STRICT_LEVEL,
};
use fireflow_types::config::{
    AllowHeaderTEXTOffsetMismatch, CheckedRangeDatatypes, DelimEscapeMode, ForceLinearScale,
    GuessOtherWidth, ProcessTemporalOpticalKeys, SpilloverMeasurementMode, TemporalOpticalKey,
    TriFlag, TrimValueWhitespace, UseEncoding,
};
use fireflow_types::keywords as tk;
use fireflow_types::nonempty_string::NEString;

use ansi_term::{ANSIString, Style};
use clap::{
    Arg, ArgAction, ArgMatches, Command,
    builder::{IntoResettable, StyledStr, ValueParser},
    error::ErrorKind,
    value_parser,
};
use itertools::Itertools as _;
use itoa::Buffer as IBuf;
use regex::Regex;
use serde_json::{json, to_writer};
use zmij::Buffer as FBuf;

use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::Display;
use std::io::{self, Write};
use std::iter::once;
use std::path::PathBuf;
use std::process::ExitCode;
use std::str::FromStr;

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
                "If '{DELIM_ESCAPED_LEVEL}' or '{DELIM_UNESCAPED_LEVEL}', escape \
                 or do not escape delimiters respectively."
            ),
            format!(
                "If {DELIM_GUESS_ESCAPED_LEVEL} or {DELIM_GUESS_UNESCAPED_LEVEL} attempt to \
                 guess how delimiters should be treated, falling back to escaped \
                 or unescaped mode respectively if the choice is ambiguous. The \
                 determination will be made by first scanning {text_seg} to find \
                 all delimiter positions and choosing the mode which results in \
                 an even number of tokens with no delimiters in keys (escaped \
                 mode) and no blank keys (unescaped mode)."
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
                odd = fmt_arg(ALLOW_ODD),
                final = fmt_arg(ALLOW_MISSING_FINAL_DELIM),
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
             '{DEFAULT_DATE_FORMAT}'.",
            pat = fmt_arg(DATE_PATTERN)
        )],
    );

    let (time_header, time_help) = format_section(
        "TIME PATTERN",
        [format!(
            "If supplied, will be used as an alternative pattern when \
             parsing {btim} and {etim} It should have specifiers for \
             hours, minutes, and seconds as outlined in {CHRONO_REF}. It may \
             optionally also have a sub-seconds specifier as shown in the \
             same link. Furthermore, the specifiers '{BASE60_SECOND_SPEC}' and \
             '{BASE100_SECOND_SPEC}' may be used to match 1/60 and centiseconds \
             respectively. If not supplied, {btim} and {etim} will be parsed according \
             to the standard pattern which is '{DEFAULT_TIME_FORMAT_2_0}' for 2.0, \
             '{DEFAULT_TIME_FORMAT_3_0}' for 3.0, and '{DEFAULT_TIME_FORMAT_3_1}' \
             for 3.1 and up."
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

    let text_correction = correction_arg(TEXT_COR, true, &text_seg);
    let data_correction = correction_arg(DATA_COR, true, &data_seg);
    let analysis_correction = correction_arg(ANALYSIS_COR, true, &analysis_seg);

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
        .value_parser(value_parser!(GuessOtherWidth))
        .help(format!(
            "Guess the width of {other_seg} segments. Valid values are \
             '{OTHER_WIDTH_NONE_LEVEL}' (no guessing) or '{OTHER_WIDTH_ERROR_LEVEL}', \
             '{OTHER_WIDTH_WARN_LEVEL}' or '{OTHER_WIDTH_SILENT_LEVEL}' which will \
             guess and throw an error, warning, or nothing on failure. For 'warn' \
             and 'silent', failure will fall back to 8 or whatever was given in {}",
            fmt_arg(OTHER_WIDTH),
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
        .value_parser(value_parser!(TruncateOffsetLimit))
        .help("Limit by which offsets can be truncated if they exceed end of file.");

    let overlap_correction_limit = Arg::new(OVERLAP_CORRECTION_LIMIT)
        .long(OVERLAP_CORRECTION_LIMIT)
        .value_name("LIMIT")
        .value_parser(value_parser!(OverlapCorrectionLimit))
        .help(
            "Limit by which ending segment offset can be truncated if they overlap another offset.",
        );

    let data_remainder_limit = Arg::new(DATA_REMAINDER_LIMIT)
        .long(DATA_REMAINDER_LIMIT)
        .value_name("LIMIT")
        .value_parser(value_parser!(DataRemainderLimit))
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

    let version_override = opt_arg::<VersionOverride>(
        VERSION_OVERRIDE,
        "OVERRIDE",
        format!(
            "Override the FCS version from {header_seg}. Can be an FCS \
             version string (like 'FCS3.2') which will force to a fixed version. \
             Can also autodetect version with one of '{VERSION_LATEST_LEVEL}' or \
             '{VERSION_EARLIEST_LEVEL}' (the latest or earliest available version \
             respectively) or '{VERSION_LOOSE_LEVEL}' or '{VERSION_STRICT_LEVEL}' \
             (the available version with the most or least optional keywords \
             respectively)."
        ),
    );

    let supp_text_correction = correction_arg(SUPP_TEXT_COR, false, &supp_text_seg);

    let nextdata_correction = Arg::new(NEXTDATA_COR)
        .long(NEXTDATA_COR)
        .value_name("INT")
        .value_parser(value_parser!(i32))
        .help(format!("Correction for {nextdata}"));

    let allow_overlapping_supp_text = tri_flag_arg::<AllowDuplicatedSuppTEXT>(
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
        .value_parser(value_parser!(DelimEscapeMode))
        .help(format!(
            "Choose how to escape delimiters in {text_seg}. \
             See {delim_header} for details."
        ));

    let non_ascii_delim = tri_flag_arg::<AllowNonAsciiDelim>(
        ALLOW_NON_ASCII_DELIM,
        format!("Allow {text_seg} delimiter to be non-ASCII character."),
    );

    let missing_final_delim = tri_flag_arg::<AllowEvenDelims>(
        ALLOW_MISSING_FINAL_DELIM,
        format!("Allow final {text_seg} delimiter to be missing."),
    );

    let allow_non_unique = tri_flag_arg::<AllowNonunique>(
        ALLOW_NON_UNIQUE,
        format!("Allow non-unique keys to exist in {text_seg}."),
    );

    let allow_odd = tri_flag_arg::<AllowOddTokens>(ALLOW_ODD, "Allow odd number of tokens.");

    let allow_empty_keys = tri_flag_arg::<AllowEmptyKeys>(
        ALLOW_EMPTY_KEYS,
        "Allow keys to be blank (relatively rare).",
    );

    let allow_delim_at_bound = tri_flag_arg::<AllowDelimAtBoundary>(
        ALLOW_DELIM_AT_BOUNDARY,
        format!("Allow {text_seg} delimiter(s) to be at token boundaries."),
    );

    let use_encoding = Arg::new(USE_ENCODING)
        .long(USE_ENCODING)
        .value_name("ENC")
        .value_parser(value_parser!(UseEncoding))
        .help(format!(
            "Choose how to interpret characters in {text_seg}. Choose \
             '{ENCODING_SINGLE_LEVEL}', '{ENCODING_UTF8_LEVEL}', or \
             '{ENCODING_GUESS_LEVEL}' to interpret bytes as IANA ISO/IEC-8859-1 \
             UTF-8, or first as UTF-8 and falling back to IANA ISO/IEC-8859-1 \
             if a non-UTF-8 byte is found."
        ));

    let allow_non_ascii_keywords = tri_flag_arg::<AllowNonAsciiKeywords>(
        ALLOW_NON_ASCII_KEYS,
        "Allow non-ASCII characters in keys.",
    );

    let allow_non_utf8 = tri_flag_arg::<AllowNonUtf8>(
        ALLOW_NON_UTF8_VALUES,
        format!("Allow non-UTF8 characters in {text_seg} segment."),
    );

    let allow_missing_supp_text = tri_flag_arg::<AllowMissingSuppTEXT>(
        ALLOW_MISSING_SUPP_TEXT,
        format!("Allow {supp_text_seg} offsets to be missing."),
    );

    let allow_supp_text_own_delim = tri_flag_arg::<AllowSuppTEXTOwnDelim>(
        ALLOW_SUPP_TEXT_OWN_DELIM,
        format!("Allow delimiters in {prim_text_seg} and {supp_text_seg} to differ."),
    );

    let allow_missing_nextdata = tri_flag_arg::<AllowMissingNextdata>(
        ALLOW_MISSING_NEXTDATA,
        format!("Allow {nextdata} to be missing."),
    );

    let trim_value_whitespace = Arg::new(TRIM_VALUE_WHITESPACE)
        .long(TRIM_VALUE_WHITESPACE)
        .value_name("LEVEL")
        .value_parser(value_parser!(TrimValueWhitespace))
        .help(format!(
            "Trim whitespace from beginning and end of all values. This may \
             create blank values if the starting string is entirely whitespace. \
             Set to '{TRIM_NONE_LEVEL}' to not trim at all (default). Set to \
             '{TRIM_ERROR_LEVEL}', '{TRIM_BLANK_WARN_LEVEL}', or \
             '{TRIM_BLANK_SILENT_LEVEL}' to enable trimming and throw error, \
             warning, or nothing when trimming results in a blank.",
        ));

    let make_key_str_args = |name, help| {
        let more = format!(
            "Values that start and end with {PATTERN_DELIMITER} will be \
             interpreted as regular expressions."
        );
        let more_help = format!("{help} {more}");
        Arg::new(name)
            .long(name)
            .action(ArgAction::Append)
            .value_name("KEY_OR_PAT")
            .help(more_help)
            .value_parser(value_parser!(KeyStringOrPattern))
    };

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
        ignore_std_key,
        promote_to_std,
        demote_from_std,
        rename_standard_keys,
        replace_std_key_vals,
        append_std_key_vals,
        sub_key_vals,
    ];

    // std args

    let dedup_meas_names = override_flag_arg(
        DEDUP_MEAS_NAMES,
        format!(
            "Force all {pn_n} to be unique by appending '{DEDUP_PNN_SEP}X' \
             to each duplicate and appending 'X' (starting at 0)",
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
             '{TIME_MEAS_NAME_PATTERN_DEFAULT}', pass \
             '{TIME_MEAS_NAME_PATTERN_NONE}' to not look for a time channel)."
        ))
        .value_parser(value_parser!(TimeMeasNamePattern));

    let allow_missing_time = tri_flag_arg::<AllowMissingTime>(
        ALLOW_MISSING_TIME,
        "Allow time measurement to be missing.",
    );

    let force_linear_scale = Arg::new(FORCE_LINEAR_SCALE)
        .long(FORCE_LINEAR_SCALE)
        .value_name("WHICH")
        .value_parser(value_parser!(ForceLinearScale))
        .help(format!(
            "Force {pn_e} keywords to be linear. Pass '{FORCE_LINEAR_TIME_LEVEL}' \
             to only set the temporal measurement, '{FORCE_LINEAR_NON_INT_LEVEL}' \
             to set temporal measurements and non-integer measurements, \
             '{FORCE_LINEAR_ALL_LEVEL}' to set all measurements, and \
             '{FORCE_LINEAR_NONE_LEVEL}' for no measurements.",
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
        .value_parser(value_parser!(TemporalOpticalKey));

    let process_time_optical_keys = Arg::new(PROCESS_TIME_OPTICAL_KEYS)
        .long(PROCESS_TIME_OPTICAL_KEYS)
        .value_name("LEVEL")
        .value_parser(value_parser!(ProcessTemporalOpticalKeys))
        .help(format!(
            "Choose how to handle optical keys found in temporal measurements. \
             Does nothing unless keys are specified in {}. Pass \
             '{TMP_OPT_DEMOTE_WARN_LEVEL}', '{TMP_OPT_DEMOTE_SILENT_LEVEL}', \
             '{TMP_OPT_DROP_WARN_LEVEL}', or '{TMP_OPT_DROP_SILENT_LEVEL}' to \
             demote found keys to nonstandard (with or without warning) or drop \
             keys entirely (with or without warning) respectively.",
            fmt_arg(IGNORE_TIME_OPTICAL_KEYS)
        ));

    let spillover_measurement_mode = Arg::new(SPILLOVER_MEASUREMENT_MODE)
        .long(SPILLOVER_MEASUREMENT_MODE)
        .value_name("MODE")
        .value_parser(value_parser!(SpilloverMeasurementMode))
        .help(format!(
            "Choose how to interpret measurement strings in {spillover}. Set to \
             '{SPILLOVER_NAMED_LEVEL}' to interpret as names which link to {pn_n}. \
             Set to '{SPILLOVER_INDEXED_LEVEL}' to interpret as 1-indices which \
             point to measurements. Set to '{SPILLOVER_GUESS_LEVEL}' to \
             automatically choose the prior two modes."
        ));

    let allow_other_feature = override_flag_arg(
        ALLOW_OTHER_FEATURE,
        format!("Allow {pnfeature} to be a value other than \"Area\", \"Width\", or \"Height\"",),
    );

    let process_pseudostandard = proc_kw_fail_arg(
        PROCESS_PSEUDOSTANDARD,
        "Process non-standard keywords that start with a '$'.",
    )
    .value_parser(value_parser!(ProcessPseudostandard));

    let process_hyper_par = proc_kw_fail_arg(
        PROCESS_HYPER_PAR,
        format!("Process measurement keywords whose index is greater than {par}."),
    )
    .value_parser(value_parser!(ProcessHyperPar));

    let process_other_version = proc_kw_fail_arg(
        PROCESS_OTHER_VERSION,
        "Process standard keywords from different FCS version.",
    )
    .value_parser(value_parser!(ProcessOtherVersion));

    let process_extra_timestep = proc_kw_fail_arg(
        PROCESS_EXTRA_TIMESTEP,
        format!(
            "Process unused {timestep}, which may indicate that a time measurement \
             is present but not identified.",
        ),
    )
    .value_parser(value_parser!(ProcessExtraTimestep));

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
        format!("Pattern to match {btim}/{etim} keywords. See {time_header}.",),
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

    let ns_meas_pattern = opt_arg::<NonStdMeasPattern>(
        NS_MEAS_PATTERN,
        "LIT_OR_PAT",
        format!(
            "Pattern to use when matching non-standard measurement keywords. \
             Values that start and end with {PATTERN_DELIMITER} will be \
             interpreted as regular expressions, otherwise as a literal string \
             to be used as a prefix matcher. It must include \
             '{NON_STD_MEAS_INDEX_PAT}' which will be replaced with measurement \
             index. Defaults to '{NON_STD_MEAS_PAT_DEFAULT}'.",
        ),
    );

    let all_read_std_args = [
        dedup_meas_names,
        trim_intra_value_whitespace,
        time_meas_pattern,
        allow_missing_time,
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
        ns_meas_pattern,
    ];

    // layout args

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
        .value_parser(value_parser!(AllowHeaderTEXTOffsetMismatch))
        .help(format!(
            "Allow {header_seg} and {text_seg} offsets to be different. If \
             {MISMATCH_HEADER_WARN_LEVEL} or {MISMATCH_HEADER_SILENT_LEVEL}, \
             choose {header_seg} and throw a warning or nothing on mismatch. \
             If {MISMATCH_TEXT_WARN_LEVEL} or {MISMATCH_TEXT_SILENT_LEVEL} \
             behave analogously for {text_seg}. If {MISMATCH_ERROR_LEVEL} \
             (default) throw error."
        ));

    let allow_missing_required_offsets = tri_flag_arg::<AllowMissingRequiredOffsets>(
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
    .value_parser(value_parser!(ProcessOptionalFailure));

    let int_widths_from_byteord = override_flag_arg(
        INT_WIDTHS_FROM_BYTEORD,
        format!(
            "Set {pn_b} based on length of {byteord}. Only has effect \
             on integer layouts in FCS 2.0/3.0."
        ),
    );

    let int_byteord_override = Arg::new(INT_BYTEORD_OVERRIDE)
        .long(INT_BYTEORD_OVERRIDE)
        .value_name("BYTEORD")
        .value_parser(value_parser!(ByteOrd2_0))
        .help(format!(
            "Override the value of {byteord}. \
             Only has effect on integer layouts in FCS 2.0/3.0.",
        ));

    let disallow_range_truncation = tri_flag_arg::<DisallowRangeTrunc>(
        DISALLOW_RANGE_TRUNCATION,
        format!(
            "Disallow {pn_r} values which need to be truncated to fit in type \
             dictated by {datatype} (and {pndatatype} for FCS 3.2) and {pn_b} \
             for a given measurement."
        ),
    );

    let all_read_layout_args = [
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

    let allow_uneven_event_width = tri_flag_arg::<AllowUnevenEventWidth>(
        ALLOW_UNEVEN_EVENT_WIDTH,
        format!("Allow event width to not evenly divide length of {data_seg}."),
    );

    let allow_tot_mismatch = tri_flag_arg::<AllowTotMismatch>(
        ALLOW_TOT_MISMATCH,
        format!("Allow {tot} to mismatch the number of events that are actually in {data_seg}."),
    );

    let checked_range_datatypes = Arg::new(CHECKED_RANGE_DATATYPES)
        .long(CHECKED_RANGE_DATATYPES)
        .value_name("WHICH")
        .value_parser(value_parser!(CheckedRangeDatatypes))
        .help(format!(
            "Truncate values exceeding {pn_r}. \
             Must be one of '{CHECK_RANGE_BITMASK_ONLY_LEVEL}' (default), \
             '{CHECK_RANGE_INT_ONLY_LEVEL}' '{CHECK_RANGE_ALL_LEVEL}', or \
             '{CHECK_RANGE_NONE_LEVEL}'.",
        ));

    let over_range_action = tri_flag_arg::<DisallowOverRange>(
        OVER_RANGE_ACTION,
        format!(
            "Choose how to handle values in DATA to exceed {pn_r}. Only applies \
             to columns that were checked according to '{CHECKED_RANGE_DATATYPES}'. \
             Pass {error} to emit error, {warn} to emit \
             warning, {silent} to do nothing, {trunc_warn} to truncate and emit \
             warning, and {trunc_silent} to truncate with no warning.",
            error = tc::OVERRANGE_ACTION_ERROR_LEVEL,
            warn = tc::OVERRANGE_ACTION_WARN_LEVEL,
            silent = tc::OVERRANGE_ACTION_SILENT_LEVEL,
            trunc_warn = tc::OVERRANGE_ACTION_TRUNCATE_WARN_LEVEL,
            trunc_silent = tc::OVERRANGE_ACTION_TRUNCATE_SILENT_LEVEL,
        ),
    );

    let row_buffer_size = Arg::new(ROW_BUFFER_SIZE)
        .long(ROW_BUFFER_SIZE)
        .value_name("BYTES")
        .value_parser(value_parser!(RowBufferSize))
        .help(format!(
            "Set the size in bytes for the internal buffer used to read {data_seg}. \
             This is a performance parameter that balances read syscalls (too low) \
             and cache misses (too high). It should generally be 90% of the CPU's \
             L1D cache size. Defaults to {}.",
            RowBufferSize::default()
        ));

    let all_read_dataset_args = [
        allow_uneven_event_width,
        allow_tot_mismatch,
        checked_range_datatypes,
        over_range_action,
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
        .value_parser(value_parser!(ReadStrategy))
        .help(format!(
            "Overall strategy to use when parsing file. This will enable many \
             options by default which can be overridden. Set to \
             {READ_STRATEGY_SCALPAL_LEVEL} to parse non-compliant files while \
             attempting to preserve metadata. Set to \
             {READ_STRATEGY_SLEDGEHAMMER_LEVEL} to parse non-compliant files \
             while dropping non-compliant metadata that cannot be fixed. Set to \
             {READ_STRATEGY_STRICT_LEVEL} to enforce the standard (default)."
        ));

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
        .arg(&dataset_index_arg)
        .args(&all_read_header_args)
        .args(&all_read_offset_args)
        .args(&all_read_flat_args)
        .args(&all_read_std_args)
        .args(&all_read_layout_args)
        .args(&all_read_shared_args)
        .after_long_help(&std_long_help);

    let meas_cmd = Command::new(SUBCMD_MEAS)
        .about("Show a table of standardized measurement values.")
        .arg(&input_arg)
        .arg(&strategy_arg)
        .arg(&dataset_index_arg)
        .arg(&delim_arg)
        .args(&all_read_header_args)
        .args(&all_read_offset_args)
        .args(&all_read_flat_args)
        .args(&all_read_std_args)
        .args(&all_read_layout_args)
        .args(&all_read_shared_args)
        .after_long_help(&std_long_help);

    let spill_cmd = Command::new(SUBCMD_SPILL)
        .about("Dump the spillover matrix if present.")
        .arg(&input_arg)
        .arg(&strategy_arg)
        .arg(&dataset_index_arg)
        .arg(&delim_arg)
        .args(&all_read_header_args)
        .args(&all_read_offset_args)
        .args(&all_read_flat_args)
        .args(&all_read_std_args)
        .args(&all_read_layout_args)
        .args(&all_read_shared_args)
        .after_long_help(&std_long_help);

    let data_cmd = Command::new(SUBCMD_DATA)
        .about(format!("Show a table of the {data_seg} segment."))
        .arg(&input_arg)
        .arg(&strategy_arg)
        .arg(&dataset_index_arg)
        .arg(&delim_arg)
        .args(&all_read_header_args)
        .args(&all_read_offset_args)
        .args(&all_read_flat_args)
        .args(&all_read_std_args)
        .args(&all_read_layout_args)
        .args(&all_read_dataset_args)
        .args(&all_read_shared_args)
        .after_long_help(&std_long_help);

    let repair_cmd = Command::new(SUBCMD_REPAIR)
        .about("Read a non-compliant FCS file and save as a compliant FCS file.")
        .arg(&input_arg)
        .arg(&output_arg)
        .arg(&strategy_arg)
        .args(&all_read_header_args)
        .args(&all_read_offset_args)
        .args(&all_read_flat_args)
        .args(&all_read_std_args)
        .args(&all_read_layout_args)
        .args(&all_read_dataset_args)
        .args(&all_read_shared_args)
        .args(&all_write_args)
        .arg(&skip_arg)
        .arg(&limit_arg)
        .after_long_help(&std_long_help);

    let summarize_cmd = Command::new(SUBCMD_SUMMARIZE)
        .about("Summarize datasets in FCS file")
        .arg(&input_arg)
        .arg(&strategy_arg)
        .args(&all_read_header_args)
        .args(&all_read_offset_args)
        .args(&all_read_flat_args)
        .args(&all_read_layout_args)
        .args(&all_read_dataset_args)
        .args(&all_read_shared_args)
        .arg(&skip_arg)
        .arg(&limit_arg);

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
        .subcommand(summarize_cmd);

    let args = cmd.clone().get_matches();

    match args.subcommand() {
        Some((SUBCMD_HEADER, sargs)) => {
            let conf = get_header_config(sargs);
            let filepath = get_path(sargs, INPUT_PATH);
            let (ws, res) = fcs_read_header(filepath, DatasetOffset(0), &conf)
                .resolve_commutative(|ws| ws, |s| s);
            print_warnings(ws, &mut stderr)?;
            to_writer(stdout, &res?)?;
            Ok(())
        }

        Some((SUBCMD_FLAT, sargs)) => {
            let subcmd = cmd.find_subcommand_mut(SUBCMD_FLAT).unwrap();
            let conf = get_read_flat_text_config(subcmd, sargs);
            let filepath = get_path(sargs, INPUT_PATH);
            let skip = get_dataset_index(sargs);
            let (ws, res) = fcs_read_flat_texts(filepath, skip, Some(1), &conf)
                .resolve_commutative(|ws| ws, |s| s);
            print_warnings(ws, &mut stderr)?;
            to_writer(stdout, &res?[0])?;
            Ok(())
        }

        Some((SUBCMD_SPILL, sargs)) => {
            let conf = get_read_std_text_config(&cmd, sargs);
            let delim = get_delim(sargs);
            let filepath = get_path(sargs, INPUT_PATH);
            let skip = get_dataset_index(sargs);
            let (ws, res) = fcs_read_std_texts(filepath, skip, Some(1), &conf)
                .resolve_commutative(|ws| ws, |s| s);
            print_warnings(ws, &mut stderr)?;
            let (core, _) = &res?[0];
            core.print_comp_or_spillover_table(&mut stdout, delim)?;
            Ok(())
        }

        Some((SUBCMD_MEAS, sargs)) => {
            let conf = get_read_std_text_config(&cmd, sargs);
            let delim = get_delim(sargs);
            let filepath = get_path(sargs, INPUT_PATH);
            let skip = get_dataset_index(sargs);
            let (ws, res) = fcs_read_std_texts(filepath, skip, Some(1), &conf)
                .resolve_commutative(|ws| ws, |s| s);
            print_warnings(ws, &mut stderr)?;
            let (core, _) = &res?[0];
            core.print_meas_table(&mut stdout, delim)?;
            Ok(())
        }

        Some((SUBCMD_STD, sargs)) => {
            let conf = get_read_std_text_config(&cmd, sargs);
            let filepath = get_path(sargs, INPUT_PATH);
            let skip = get_dataset_index(sargs);
            let (ws, res) = fcs_read_std_texts(filepath, skip, Some(1), &conf)
                .resolve_commutative(|ws| ws, |s| s);
            print_warnings(ws, &mut stderr)?;
            let (core, uncore) = &res?[0];
            let obj = json!({"core": core, "uncore": uncore});
            to_writer(stdout, &obj)?;
            Ok(())
        }

        Some((SUBCMD_DATA, sargs)) => {
            let conf = get_read_std_dataset_config(&cmd, sargs);
            let delim = get_delim(sargs);
            let filepath = get_path(sargs, INPUT_PATH);
            let skip = get_dataset_index(sargs);
            let (ws, res) = fcs_read_std_datasets(filepath, skip, Some(1), &conf)
                .resolve_commutative(|ws| ws, |s| s);
            print_warnings(ws, &mut stderr)?;
            let (core, _) = &res?[0];
            print_parsed_data(&mut stdout, core, delim)?;
            Ok(())
        }

        Some((SUBCMD_REPAIR, sargs)) => {
            let read_conf = get_read_std_dataset_config(&cmd, sargs);
            let write_conf = get_write_std_dataset_config(sargs);
            let ipath = get_path(sargs, INPUT_PATH);
            let opath = get_path(sargs, OUTPUT_PATH);
            let skip = get_skip(sargs);
            let limit = get_limit(sargs);
            let (read_ws, read_res) = fcs_read_std_datasets(ipath, skip, limit, &read_conf)
                .resolve_commutative(|ws| ws, |s| s);
            let (cores, outs): (Vec<_>, Vec<_>) = read_res?.into_iter().unzip();
            let (write_ws, write_res) = fcs_write_datasets(opath, &cores[..], &write_conf)
                .resolve_commutative(|ws| ws, |e| e);
            print_warnings(read_ws, &mut stderr)?;
            print_warnings(write_ws, &mut stderr)?;
            let _ = write_res?;
            to_writer(stdout, &outs[..])?;
            Ok(())
        }

        Some((SUBCMD_SUMMARIZE, sargs)) => {
            let conf = get_read_flat_dataset_config(&cmd, sargs);
            let filepath = get_path(sargs, INPUT_PATH);
            let skip = get_skip(sargs);
            let limit = get_limit(sargs);
            let (ws, res) =
                fcs_summarize(filepath, skip, limit, &conf).resolve_commutative(|ws| ws, |s| s);
            print_warnings(ws, &mut stderr)?;
            to_writer(stdout, &res?)?;
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
        "{help_front} Must be one of '{KW_ERROR_LEVEL}', '{KW_DEMOTE_WARN_LEVEL}', \
         '{KW_DEMOTE_SILENT_LEVEL}', '{KW_DROP_WARN_LEVEL}', or \
         '{KW_DROP_SILENT_LEVEL}' which will throw an error, demote to \
         non-standard with warning, demote to non-standard silently, drop with \
         warning, or drop silently respectively"
    ))
}

fn tri_flag_arg<T>(long: &'static str, help_front: impl Display) -> Arg
where
    T: From<TriFlag> + Clone + Send + Sync + 'static + TriErrorFlag,
{
    let parser = ValueParser::new(T::from_partial_str);
    let what = if T::FALSE_IS_ERROR {
        "warning"
    } else {
        "error"
    };
    let h = format!(
        "{help_front} If '{TRI_TRUE_LEVEL}', throw {what}. \
         If '{TRI_SILENT_LEVEL}', ignore completely."
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

fn get_header_config(sargs: &ArgMatches) -> config::ReadHeaderConfig {
    config::ReadHeaderConfig {
        header: get_header_inner_config(sargs),
        offset: get_offsets_config(sargs),
    }
}

fn get_header_inner_config(sargs: &ArgMatches) -> config::ReadHeaderInnerConfig {
    let strat = get_strategy(sargs);
    let mut conf = config::ReadHeaderInnerConfig::new_with_strategy(strat);

    get_correction(sargs, TEXT_COR, |x| conf.text_correction = x);
    get_correction(sargs, DATA_COR, |x| conf.data_correction = x);
    get_correction(sargs, ANALYSIS_COR, |x| conf.analysis_correction = x);

    if let Some(xs) = sargs.get_many::<(i32, i32)>(OTHER_CORR) {
        conf.other_corrections = xs
            .into_iter()
            .copied()
            .map(OffsetCorrection::from)
            .collect();
    }

    get_opt(sargs, MAX_OTHER, |x| conf.max_other = x);
    get_opt(sargs, OTHER_WIDTH, |x| conf.other_width = x);
    get_opt(sargs, GUESS_OTHER_WIDTH, |x| conf.guess_other_width = x);
    get_flag(sargs, SQUISH_OFFSETS, |x| conf.squish_offsets = x);

    conf
}

fn get_offsets_config(s: &ArgMatches) -> config::ReadOffsetConfig {
    let strat = get_strategy(s);
    let mut c = config::ReadOffsetConfig::new_with_strategy(strat);

    get_flag(s, ALLOW_PSEUDOEMPTY, |x| c.allow_pseudoempty = x);
    get_opt(s, TRUNCATE_OFFSET_LIMIT, |x| c.truncate_offset_limit = x);
    get_opt(s, OVERLAP_CORRECTION_LIMIT, |x| {
        c.overlap_correction_limit = x;
    });

    c
}

fn get_header_and_text_config(cmd: &Command, s: &ArgMatches) -> config::ReadHeaderAndTEXTConfig {
    let strat = get_strategy(s);
    let mut c = config::ReadHeaderAndTEXTConfig::new_with_strategy(strat);

    get_opt(s, VERSION_OVERRIDE, |x| c.version_override = x);
    get_correction(s, SUPP_TEXT_COR, |x| c.supp_text_correction = x);
    get_opt(s, NEXTDATA_COR, |x| c.nextdata_correction = x);

    get_opt(s, ALLOW_DUPLICATED_SUPP_TEXT, |x| {
        c.allow_duplicated_supp_text = x;
    });
    get_flag(s, IGNORE_SUPP_TEXT, |x| c.ignore_supp_text = x);
    get_opt(s, DELIM_ESCAPE_MODE, |x| c.delim_escape_mode = x);
    get_opt(s, ALLOW_NON_ASCII_DELIM, |x| c.allow_non_ascii_delim = x);
    get_opt(s, ALLOW_MISSING_FINAL_DELIM, |x| {
        c.allow_even_delims = x;
    });
    get_opt(s, ALLOW_NON_UNIQUE, |x| c.allow_nonunique = x);
    get_opt(s, ALLOW_ODD, |x| c.allow_odd_tokens = x);
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

    get_many::<KeyStringOrPattern, _, _>(s, IGNORE_STD_KEYS, |xs| c.ignore_standard_keys = xs);
    get_many::<KeyStringOrPattern, _, _>(s, PROMOTE_TO_STD, |xs| c.promote_to_standard = xs);
    get_many::<KeyStringOrPattern, _, _>(s, DEMOTE_FROM_STD, |xs| c.demote_from_standard = xs);

    if let Some(xs) = s.get_many::<BiKeystringPair>(RENAME_STD_KEYS) {
        let Ok(ys) = xs
            .cloned()
            .collect::<HashMap<_, _>>()
            .try_into()
            .map_err(|e| post_validation_error(cmd, RENAME_STD_KEYS, e).exit());
        c.rename_standard_keys = ys;
    }

    let parse_keystring_pair = |name: &str| {
        s.get_many::<KeystringStringPair>(name)
            .map(|xs| xs.cloned().collect())
    };

    let _ = parse_keystring_pair(REPLACE_STD_KEY_VALS).map(|x| c.replace_standard_key_values = x);
    let _ = parse_keystring_pair(APPEND_STD_KEY_VALS).map(|x| c.append_standard_keywords = x);

    get_many(s, SUB_STD_KEY_VALS, |xs| {
        c.substitute_standard_key_values = xs;
    });

    c
}

fn get_std_kws_config(s: &ArgMatches) -> config::ReadStdKeywordsConfig {
    let strat = get_strategy(s);
    let mut c = config::ReadStdKeywordsConfig::new_with_strategy(strat);

    get_flag(s, DEDUP_MEAS_NAMES, |x| c.dedup_measurement_names = x);
    get_flag(s, TRIM_INTRA_VALUE_WHITESPACE, |x| {
        c.trim_intra_value_whitespace = x;
    });
    get_opt(s, TIME_MEAS_PATTERN, |x| c.time_meas_pattern = x);

    get_opt(s, FORCE_LINEAR_SCALE, |x| c.force_linear_scale = x);

    if let Some(xs) = s.get_many::<TemporalOpticalKey>(IGNORE_TIME_OPTICAL_KEYS) {
        c.ignore_time_optical_keys = xs.copied().collect::<HashSet<_>>().into();
    }

    get_opt(s, PROCESS_TIME_OPTICAL_KEYS, |x| {
        c.process_time_optical_keys = x;
    });
    get_opt(s, ALLOW_MISSING_TIME, |x| c.allow_missing_time = x);
    get_opt(s, SPILLOVER_MEASUREMENT_MODE, |x| {
        c.spillover_measurement_mode = x;
    });
    get_opt(s, DATE_PATTERN, |x| c.date_pattern = x);
    get_opt(s, TIME_PATTERN, |x| c.time_pattern = x);
    get_opt(s, DATETIME_PATTERN, |x| c.datetime_pattern = x);
    get_opt(s, LAST_MODIFIED_PATTERN, |x| c.last_modified_pattern = x);
    get_flag(s, ALLOW_OTHER_FEATURE, |x| c.allow_other_feature = x);
    get_opt(s, PROCESS_PSEUDOSTANDARD, |x| c.process_pseudostandard = x);
    get_opt(s, PROCESS_HYPER_PAR, |x| c.process_hyper_par = x);
    get_opt(s, PROCESS_OTHER_VERSION, |x| c.process_other_version = x);
    get_opt(s, PROCESS_EXTRA_TIMESTEP, |x| c.process_extra_timestep = x);
    get_flag(s, FIX_LOG_SCALE_OFFSETS, |x| c.fix_log_scale_offsets = x);
    get_flag(s, DISALLOW_LOCALTIME, |x| c.disallow_localtime = x);

    get_opt(s, NS_MEAS_PATTERN, |x| {
        c.nonstandard_measurement_pattern = NonStdMeasPatternOpt(x);
    });
    c
}

fn get_data_kws_config(s: &ArgMatches) -> config::ReadDataKeywordsConfig {
    let strat = get_strategy(s);
    let mut c = config::ReadDataKeywordsConfig::new_with_strategy(strat);

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
    get_flag(s, INT_WIDTHS_FROM_BYTEORD, |x| {
        c.integer_widths_from_byteord = x;
    });
    get_opt(s, INT_BYTEORD_OVERRIDE, |x| {
        c.integer_byteord_override = Some(x);
    });
    get_opt(s, DISALLOW_RANGE_TRUNCATION, |x| {
        c.disallow_range_truncation = x;
    });

    c
}

fn get_events_config(s: &ArgMatches) -> config::ReadEventsConfig {
    let strat = get_strategy(s);
    let mut c = config::ReadEventsConfig::new_with_strategy(strat);

    get_opt(s, DATA_REMAINDER_LIMIT, |x| c.data_remainder_limit = x);
    get_opt(s, ALLOW_TOT_MISMATCH, |x| c.allow_tot_mismatch = x);
    get_opt(s, ALLOW_UNEVEN_EVENT_WIDTH, |x| {
        c.allow_uneven_event_width = x;
    });
    get_opt(s, CHECKED_RANGE_DATATYPES, |x| {
        c.checked_range_datatypes = x;
    });
    get_opt(s, OVER_RANGE_ACTION, |x| c.over_range_action = x);
    get_opt(s, ROW_BUFFER_SIZE, |x| c.row_buffer_size = x);

    c
}

fn get_read_shared_config(sargs: &ArgMatches) -> config::ReadSharedConfig {
    config::ReadSharedConfig {
        warnings_are_errors: sargs.get_flag(WARNINGS_ARE_ERRORS),
        hide_warnings: sargs.get_flag(HIDE_WARNINGS),
    }
}

fn get_read_flat_text_config(cmd: &Command, sargs: &ArgMatches) -> config::ReadFlatTEXTConfig {
    config::ReadFlatTEXTConfig {
        header: get_header_inner_config(sargs),
        flat: get_header_and_text_config(cmd, sargs),
        offset: get_offsets_config(sargs),
        shared: get_read_shared_config(sargs),
    }
}

fn get_read_std_text_config(cmd: &Command, sargs: &ArgMatches) -> config::ReadStdTEXTConfig {
    config::ReadStdTEXTConfig {
        header: get_header_inner_config(sargs),
        flat: get_header_and_text_config(cmd, sargs),
        offset: get_offsets_config(sargs),
        standard: get_std_kws_config(sargs),
        layout: get_data_kws_config(sargs),
        shared: get_read_shared_config(sargs),
    }
}

fn get_read_flat_dataset_config(
    cmd: &Command,
    sargs: &ArgMatches,
) -> config::ReadFlatDatasetConfig {
    config::ReadFlatDatasetConfig {
        header: get_header_inner_config(sargs),
        flat: get_header_and_text_config(cmd, sargs),
        offset: get_offsets_config(sargs),
        layout: get_data_kws_config(sargs),
        data: get_events_config(sargs),
        shared: get_read_shared_config(sargs),
    }
}

fn get_read_std_dataset_config(cmd: &Command, sargs: &ArgMatches) -> config::ReadStdDatasetConfig {
    config::ReadStdDatasetConfig {
        header: get_header_inner_config(sargs),
        flat: get_header_and_text_config(cmd, sargs),
        offset: get_offsets_config(sargs),
        standard: get_std_kws_config(sargs),
        layout: get_data_kws_config(sargs),
        data: get_events_config(sargs),
        shared: get_read_shared_config(sargs),
    }
}

fn get_write_std_dataset_config(sargs: &ArgMatches) -> config::WriteDatasetInnerConfig {
    let mut conf = WriteDatasetInnerConfig::default();

    get_opt(sargs, PRINT_DELIM, |x| conf.text.delim = x);
    get_opt(sargs, BIG_OTHER, |x: bool| conf.text.big_other = x.into());
    // ranges are checked once when reading the dataframe so no need to check
    // them again; if anything they might be fixed
    conf.checked_range_datatypes = CheckedRangeDatatypes::None;
    conf
}

fn get_path<'a>(sargs: &'a ArgMatches, name: &'a str) -> &'a PathBuf {
    sargs.get_one::<PathBuf>(name).expect("path is required")
}

fn get_strategy(sargs: &ArgMatches) -> ReadStrategy {
    sargs.get_one(STRATEGY).copied().unwrap_or_default()
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
    F: FnMut(OffsetCorrection<I, S>),
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

const SUBCMD_MEAS: &str = "measurements";

const SUBCMD_SPILL: &str = "spillover";

const SUBCMD_REPAIR: &str = "repair";

const TEXT_COR: &str = "text-correction";

const DATA_COR: &str = "data-correction";

const ANALYSIS_COR: &str = "analysis-correction";

const OTHER_CORR: &str = "other-correction";

const MAX_OTHER: &str = "max-other";

const OTHER_WIDTH: &str = "other-width";

const GUESS_OTHER_WIDTH: &str = "guess-other-width";

const SQUISH_OFFSETS: &str = "squish-offsets";

const ALLOW_PSEUDOEMPTY: &str = "allow-pseudoempty";

const TRUNCATE_OFFSET_LIMIT: &str = "truncate-offset-limit";

const OVERLAP_CORRECTION_LIMIT: &str = "overlap-correction-limit";

const DATA_REMAINDER_LIMIT: &str = "data-remainder-limit";

const VERSION_OVERRIDE: &str = "version-override";

const SUPP_TEXT_COR: &str = "supp-text-correction";

const NEXTDATA_COR: &str = "nextdata-correction";

const ALLOW_DUPLICATED_SUPP_TEXT: &str = "allow-duplicated-supp-text";

const IGNORE_SUPP_TEXT: &str = "ignore-supp-text";

const DELIM_ESCAPE_MODE: &str = "delim-escape-mode";

const ALLOW_NON_ASCII_DELIM: &str = "allow-non-ascii-delim";

const ALLOW_MISSING_FINAL_DELIM: &str = "allow-missing-final-delim";

const ALLOW_NON_UNIQUE: &str = "allow-non-unique";

const ALLOW_ODD: &str = "allow-odd";

const ALLOW_EMPTY_KEYS: &str = "allow-empty-keys";

const ALLOW_DELIM_AT_BOUNDARY: &str = "allow-delim-at-boundary";

const USE_ENCODING: &str = "use-encoding";

const ALLOW_NON_ASCII_KEYS: &str = "allow-non-ascii-keys";

const ALLOW_NON_UTF8_VALUES: &str = "allow-non-utf8-values";

const ALLOW_MISSING_SUPP_TEXT: &str = "allow-missing-supp-text";

const ALLOW_SUPP_TEXT_OWN_DELIM: &str = "allow-supp-text-own-delim";

const ALLOW_MISSING_NEXTDATA: &str = "allow-missing-nextdata";

const TRIM_VALUE_WHITESPACE: &str = "trim-value-whitespace";

const IGNORE_STD_KEYS: &str = "ignore-std-keys";

const PROMOTE_TO_STD: &str = "promote-to-std";

const DEMOTE_FROM_STD: &str = "demote-from-std";

const RENAME_STD_KEYS: &str = "rename-std-keys";

const REPLACE_STD_KEY_VALS: &str = "replace-std-key-vals";

const APPEND_STD_KEY_VALS: &str = "append-std-key-vals";

const SUB_STD_KEY_VALS: &str = "sub-std-key-vals";

const DATE_PATTERN: &str = "date-pattern";

const TIME_PATTERN: &str = "time-pattern";

const DATETIME_PATTERN: &str = "datetime-pattern";

const LAST_MODIFIED_PATTERN: &str = "last-modified-pattern";

const WARNINGS_ARE_ERRORS: &str = "warnings-are-errors";

const HIDE_WARNINGS: &str = "hide-warnings";

const DEDUP_MEAS_NAMES: &str = "dedup-measurement-names";

const TRIM_INTRA_VALUE_WHITESPACE: &str = "trim-intra-value-whitespace";

const TIME_MEAS_PATTERN: &str = "time-meas-pattern";

const ALLOW_MISSING_TIME: &str = "allow-missing-time";

const SPILLOVER_MEASUREMENT_MODE: &str = "spillover-measurement-mode";

const FORCE_LINEAR_SCALE: &str = "force-time-linear";

const IGNORE_TIME_OPTICAL_KEYS: &str = "ignore-time-optical-keys";

const PROCESS_TIME_OPTICAL_KEYS: &str = "process-time-optical-keys";

const ALLOW_OTHER_FEATURE: &str = "allow-other-feature";

const PROCESS_PSEUDOSTANDARD: &str = "process-pseudostandard";

const PROCESS_HYPER_PAR: &str = "process-hyper-par";

const PROCESS_OTHER_VERSION: &str = "process-other-version";

const PROCESS_EXTRA_TIMESTEP: &str = "process-extra-timestep";

const PROCESS_OPTIONAL_FAILURE: &str = "process-optional-failure";

const FIX_LOG_SCALE_OFFSETS: &str = "fix-log-scale-offsets";

const DISALLOW_LOCALTIME: &str = "disallow-localtime";

const NS_MEAS_PATTERN: &str = "non-std-meas-pattern";

const TEXT_DATA_COR: &str = "text-data-correction";

const TEXT_ANALYSIS_COR: &str = "text-analysis-correction";

const IGNORE_TEXT_DATA_OFFSETS: &str = "ignore-text-data-offsets";

const IGNORE_TEXT_ANALYSIS_OFFSETS: &str = "ignore-text-analysis-offsets";

const ALLOW_HEADER_TEXT_OFFSET_MISMATCH: &str = "allow-text-offset-mismatch";

const ALLOW_MISSING_REQUIRED_OFFSETS: &str = "allow-missing-required-offsets";

const INT_WIDTHS_FROM_BYTEORD: &str = "integer-widths-from-byteord";

const INT_BYTEORD_OVERRIDE: &str = "integer-byteord-override";

const DISALLOW_RANGE_TRUNCATION: &str = "disallow-range-truncation";

const ALLOW_UNEVEN_EVENT_WIDTH: &str = "allow-uneven-event-width";

const CHECKED_RANGE_DATATYPES: &str = "checked-range-datatypes";

const OVER_RANGE_ACTION: &str = "over-range-action";

const ROW_BUFFER_SIZE: &str = "row-buffer-size";

const ALLOW_TOT_MISMATCH: &str = "allow-tot-mismatch";

const PRINT_DELIM: &str = "print-delim";

const DATASET_INDEX: &str = "dataset-index";

const WRITE_DELIM: &str = "write-delim";

const BIG_OTHER: &str = "skip-conversion-check";

const SKIP: &str = "skip";

const LIMIT: &str = "limit";

const INPUT_PATH: &str = "input-path";

const OUTPUT_PATH: &str = "output-path";

const STRATEGY: &str = "strategy";

const CHRONO_REF: &str = "https://docs.rs/chrono/latest/chrono/format/strftime/index.html";

const REGEXP_REF: &str = "https://docs.rs/regex/latest/regex/#syntax";

const REGEXP_REP_REF: &str = "https://docs.rs/regex/latest/regex/struct.Regex.html#method.replace";

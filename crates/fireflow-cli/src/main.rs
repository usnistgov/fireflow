use fireflow_core::api::{
    fcs_read_flat_texts, fcs_read_header, fcs_read_std_datasets, fcs_read_std_texts, fcs_summarize,
};
use fireflow_core::config::{
    self, AllowDelimAtBoundary, AllowDuplicatedSuppTEXT, AllowEmptyKeys,
    AllowHeaderTEXTOffsetMismatch, AllowMissingFinalDelim, AllowMissingNextdata,
    AllowMissingRequiredOffsets, AllowMissingSuppTEXT, AllowMissingTime, AllowNonAsciiDelim,
    AllowNonAsciiKeywords, AllowNonUtf8, AllowNonunique, AllowOdd, AllowSuppTEXTOwnDelim,
    AllowTotMismatch, AllowUnevenEventWidth, DataRemainderLimit, DatasetOffset, DelimEscapeMode,
    DisallowDeprecated, DisallowOverRange, DisallowRangeTrunc, ForceLinearScale, GuessOtherWidth,
    NonStdMeasPatternOpt, OverlapCorrectionLimit, ProcessExtraTimestep, ProcessHyperPar,
    ProcessOptionalFailure, ProcessOtherVersion, ProcessPseudostandard, ProcessTemporalOpticalKeys,
    SpilloverMeasurementMode, TemporalOpticalKey, TimeMeasNamePattern, TriErrorFlag, TriFlag,
    TrimValueWhitespace, TruncateEventValues, TruncateOffsetLimit, VersionOverride,
};
use fireflow_core::core::AnyCoreDataset;
use fireflow_core::segment::OffsetCorrection;
use fireflow_core::text::keywords::ByteOrd2_0;
use fireflow_core::validated::ascii_range::OtherWidth;
use fireflow_core::validated::case_ins_regex::CaseInsRegex;
use fireflow_core::validated::datepattern::DatePattern;
use fireflow_core::validated::keys::{
    AsciiStringError, KeyRegexError, KeyString, KeyStringOrPattern,
};
use fireflow_core::validated::keystring_pairs::KeyStringPairs;
use fireflow_core::validated::nonstd_meas_pattern::NonStdMeasPattern;
use fireflow_core::validated::sub_pattern::SubPattern;
use fireflow_core::validated::timepattern::TimePattern;
use fireflow_types::config::{
    BASE60_SECOND_SPEC, BASE100_SECOND_SPEC, DEDUP_PNN_SEP, DEFAULT_DATE_FORMAT,
    DEFAULT_TIME_FORMAT_2_0, DEFAULT_TIME_FORMAT_3_0, DEFAULT_TIME_FORMAT_3_1, DELIM_ESCAPED_LEVEL,
    DELIM_GUESS_ESCAPED_LEVEL, DELIM_GUESS_UNESCAPED_LEVEL, DELIM_UNESCAPED_LEVEL,
    FORCE_LINEAR_ALL_LEVEL, FORCE_LINEAR_NONE_LEVEL, FORCE_LINEAR_TIME_LEVEL,
    KW_DEMOTE_SILENT_LEVEL, KW_DEMOTE_WARN_LEVEL, KW_DROP_SILENT_LEVEL, KW_DROP_WARN_LEVEL,
    KW_ERROR_LEVEL, MISMATCH_ERROR_LEVEL, MISMATCH_HEADER_SILENT_LEVEL, MISMATCH_HEADER_WARN_LEVEL,
    MISMATCH_TEXT_SILENT_LEVEL, MISMATCH_TEXT_WARN_LEVEL, NON_STD_MEAS_INDEX_PAT,
    NON_STD_MEAS_PAT_DEFAULT, OTHER_WIDTH_ERROR_LEVEL, OTHER_WIDTH_NONE_LEVEL,
    OTHER_WIDTH_SILENT_LEVEL, OTHER_WIDTH_WARN_LEVEL, SPILLOVER_GUESS_LEVEL,
    SPILLOVER_INDEXED_LEVEL, SPILLOVER_NAMED_LEVEL, TIME_MEAS_NAME_PATTERN_DEFAULT,
    TIME_MEAS_NAME_PATTERN_NONE, TMP_OPT_DEMOTE_SILENT_LEVEL, TMP_OPT_DEMOTE_WARN_LEVEL,
    TMP_OPT_DROP_SILENT_LEVEL, TMP_OPT_DROP_WARN_LEVEL, TRI_SILENT_LEVEL, TRI_TRUE_LEVEL,
    TRIM_BLANK_SILENT_LEVEL, TRIM_BLANK_WARN_LEVEL, TRIM_ERROR_LEVEL, TRIM_NONE_LEVEL,
    TRUNCATE_ALL_LEVEL, TRUNCATE_INT_ONLY_LEVEL, TRUNCATE_NONE_LEVEL, VERSION_EARLIEST_LEVEL,
    VERSION_LATEST_LEVEL, VERSION_LOOSE_LEVEL, VERSION_STRICT_LEVEL,
};
use fireflow_types::keywords as tk;

use ansi_term::{ANSIString, Style};
use clap::{
    Arg, ArgAction, ArgMatches, Command,
    builder::{IntoResettable, StyledStr, ValueParser},
    error::ErrorKind,
    value_parser,
};
use itertools::Itertools as _;
use regex::Regex;
use serde::ser::Serialize;
use serde_json::json;

use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::error::Error;
use std::fmt::Display;
use std::iter::once;
use std::path::PathBuf;

fn main() -> Result<(), i32> {
    run().map_err(|e| {
        eprintln!("{e}");
        1_i32
    })
}

#[allow(clippy::too_many_lines)]
fn run() -> AppResult<()> {
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
    let pnb = kw_style.paint(tk::PNB);
    let pnr = kw_style.paint(tk::PNR);
    let pnn = kw_style.paint(tk::PNN);
    let pne = kw_style.paint(tk::PNE);
    let pndatatype = kw_style.paint(tk::PNDATATYPE);

    let pnx = kw_style.paint("$PnX");

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
                "The guessing algorithm is independent of {trim} since it will ignore \
                 everything after the last delimiter. It is also independent of {odd} \
                 and {final} which will trigger as normal if their respective violations \
                 are found.",
                trim = fmt_arg(TRIM_TEXT_END),
                odd = fmt_arg(ALLOW_ODD),
                final = fmt_arg(ALLOW_MISSING_FINAL_DELIM),
            ),
        ],
    );

    let (sub_header, sub_help) = format_section(
        "SUBSTITUTION",
        [format!(
            "The SUB part in {lit} and {pat} is a sed-like pattern which will \
             be used to edit the value of KEY. It must be a string like \
             's<D><FROM><D><TO>[<D>g]' where 'D' is a delimiter (any character), \
             FROM is a regular expression and TO is a replacement pattern. FROM \
             and TO must follow the syntax outlined in {REGEXP_REF} and \
             {REGEXP_REP_REF} respectively, with the caveat that only bracketed \
             replacement syntax is allowed.",
            lit = fmt_arg(SUB_STD_LIT_KEY_VALS),
            pat = fmt_arg(SUB_STD_PAT_KEY_VALS),
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
        let h = format!("Adjustment for {seg} offsets from {src}.");
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

    let max_other = Arg::new(MAX_OTHER)
        .long(MAX_OTHER)
        .value_name("BYTES")
        .help(format!("Max number of {other_seg} segments to parse."))
        .value_parser(value_parser!(usize));

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

    let squish_offsets = flag_arg(
        SQUISH_OFFSETS,
        format!(
            "If {data_seg}/{analysis_seg} end in 0, use 0 for start as well. \
             Should not be used for FCS 2.0 files."
        ),
    );

    let all_header_args = [
        text_correction,
        data_correction,
        analysis_correction,
        max_other,
        other_width,
        guess_other_width,
        squish_offsets,
    ];

    // offset args

    let allow_pseudoempty = flag_arg(
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

    let all_offset_args = [
        allow_pseudoempty,
        truncate_offset_limit,
        overlap_correction_limit,
        data_remainder_limit,
    ];

    // "flat" args

    let version_override = Arg::new(VERSION_OVERRIDE)
        .long(VERSION_OVERRIDE)
        .value_name("OVERRIDE")
        .value_parser(value_parser!(VersionOverride))
        .help(format!(
            "Override the FCS version from {header_seg}. Can be an FCS \
             version string (like 'FCS3.2') which will force to a fixed version. \
             Can also autodetect version with one of '{VERSION_LATEST_LEVEL}' or \
             '{VERSION_EARLIEST_LEVEL}' (the latest or earliest available version \
             respectively) or '{VERSION_LOOSE_LEVEL}' or '{VERSION_STRICT_LEVEL}' \
             (the available version with the most or least optional keywords \
             respectively)."
        ));

    let supp_text_correction = correction_arg(SUPP_TEXT_COR, false, &supp_text_seg);

    let nextdata_correction = Arg::new(NEXTDATA_COR)
        .long(NEXTDATA_COR)
        .value_name("INT")
        .help(format!("Correction for {nextdata}"));

    let allow_overlapping_supp_text = tri_flag_arg::<AllowDuplicatedSuppTEXT>(
        ALLOW_OVERLAPPING_SUPP_TEXT,
        format!(
            "Allow {supp_text_seg} offsets to overlap those for \
             {prim_text_seg} or the boundaries of {header_seg}."
        ),
    );

    let ignore_supp_text = flag_arg(
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

    let missing_final_delim = tri_flag_arg::<AllowMissingFinalDelim>(
        ALLOW_MISSING_FINAL_DELIM,
        format!("Allow final {text_seg} delimiter to be missing."),
    );

    let allow_non_unique = tri_flag_arg::<AllowNonunique>(
        ALLOW_NON_UNIQUE,
        format!("Allow non-unique keys to exist in {text_seg}."),
    );

    let allow_odd = tri_flag_arg::<AllowOdd>(ALLOW_ODD, "Allow odd number of tokens.");

    let allow_empty_keys = tri_flag_arg::<AllowEmptyKeys>(
        ALLOW_EMPTY_KEYS,
        "Allow keys to be blank (relatively rare).",
    );

    let allow_delim_at_bound = tri_flag_arg::<AllowDelimAtBoundary>(
        ALLOW_DELIM_AT_BOUNDARY,
        format!("Allow {text_seg} delimiter(s) to be at token boundaries."),
    );

    let allow_non_utf8 = tri_flag_arg::<AllowNonUtf8>(
        ALLOW_NON_UTF8,
        format!("Allow non-UTF8 characters in {text_seg} segment."),
    );

    let use_latin1 = flag_arg(
        USE_LATIN1,
        format!("Interpret all characters in {text_seg} as Latin-1 (aka ISO/IEC 8859-1)."),
    );

    let allow_non_ascii_keywords = tri_flag_arg::<AllowNonAsciiKeywords>(
        ALLOW_NON_ASCII_KEYWORDS,
        "Allow non-ASCII characters in keys.",
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

    let trim_text_end = flag_arg(
        TRIM_TEXT_END,
        format!(
            "Decrease the final offset of {text_seg} based on delimiter count \
             and trailing non-delimiter characters after {text_seg}."
        ),
    );

    let make_key_str_args = |lit_flag, pat_flag, lit_help, pat_help| {
        let lit_arg = Arg::new(lit_flag)
            .long(lit_flag)
            .action(ArgAction::Append)
            .value_name("KEY")
            .help(lit_help)
            .value_parser(ValueParser::new(parse_keystring_literal));
        let pat_arg = Arg::new(pat_flag)
            .long(pat_flag)
            .action(ArgAction::Append)
            .value_name("REGEXP")
            .help(pat_help)
            .value_parser(ValueParser::new(parse_keystring_pattern));
        (lit_arg, pat_arg)
    };

    let (ignore_std_lit_key, ignore_std_pat_key) = make_key_str_args(
        IGNORE_STD_LIT_KEY,
        IGNORE_STD_PAT_KEY,
        "Ignore standard keys exactly matching KEY. The leading '$' is implied.",
        "Ignore standard keys matching REGEXP. The leading '$' is implied.",
    );

    let (promote_lit_to_std, promote_pat_to_std) = make_key_str_args(
        PROMOTE_LIT_TO_STD,
        PROMOTE_PAT_TO_STD,
        "Promote non-standard keys matching KEY to standard.",
        "Promote non-standard keys matching REGEXP to standard.",
    );

    let (demote_lit_from_std, demote_pat_from_std) = make_key_str_args(
        DEMOTE_LIT_FROM_STD,
        DEMOTE_PAT_FROM_STD,
        "Demote standard keys matching KEY to non-standard. The leading '$' is implied.",
        "Demote standard keys matching REGEXP to non-standard. The leading '$' is implied.",
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

    let sub_std_lit_key_vals = Arg::new(SUB_STD_LIT_KEY_VALS)
        .long(SUB_STD_LIT_KEY_VALS)
        .action(ArgAction::Append)
        .value_name("KEY,SUB")
        .help(format!(
            "Edit standard key values using KEY and SUB. The leading '$' \
             is implied for KEY. See {sub_header} for details."
        ))
        .value_parser(ValueParser::new(parse_sub_pattern_literal));

    let sub_std_pat_key_vals = Arg::new(SUB_STD_PAT_KEY_VALS)
        .long(SUB_STD_PAT_KEY_VALS)
        .action(ArgAction::Append)
        .value_name("REGEXP,SUB")
        .help(format!(
            "Edit standard keys matching REGEXP with SUB. The leading '$' is \
             implied for KEY. See {sub_header} for details."
        ))
        .value_parser(ValueParser::new(parse_sub_pattern_pattern));

    let all_flat_args = vec![
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
        use_latin1,
        allow_non_ascii_keywords,
        allow_missing_supp_text,
        allow_supp_text_own_delim,
        allow_missing_nextdata,
        trim_value_whitespace,
        trim_text_end,
        ignore_std_lit_key,
        ignore_std_pat_key,
        promote_lit_to_std,
        promote_pat_to_std,
        demote_lit_from_std,
        demote_pat_from_std,
        rename_standard_keys,
        replace_std_key_vals,
        append_std_key_vals,
        sub_std_lit_key_vals,
        sub_std_pat_key_vals,
    ];

    // std args

    let dedup_meas_names = flag_arg(
        DEDUP_MEAS_NAMES,
        format!(
            "Force all {pnn} to be unique by appending '{DEDUP_PNN_SEP}X' \
             to each duplicate and appending 'X' (starting at 0)",
        ),
    );

    let trim_intra_value_whitespace = flag_arg(
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
            "Force {pne} keywords to be linear. Pass '{FORCE_LINEAR_TIME_LEVEL}' \
             to only set the temporal measurement, '{FORCE_LINEAR_ALL_LEVEL}' to \
             set all measurements, and '{FORCE_LINEAR_NONE_LEVEL}' for no \
             measurements.",
        ));

    let ignore_time_optical_keys = Arg::new(IGNORE_TIME_OPTICAL_KEYS)
        .long(IGNORE_TIME_OPTICAL_KEYS)
        .action(ArgAction::Append)
        .value_name("SYMS")
        .help(format!(
            "Ignore optical keywords for temporal measurement. Must be a \
             comma-separated list of strings like the X in {pnx}.",
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
             '{SPILLOVER_NAMED_LEVEL}' to interpret as names which link to {pnn}. \
             Set to '{SPILLOVER_INDEXED_LEVEL}' to interpret as 1-indices which \
             point to measurements. Set to '{SPILLOVER_GUESS_LEVEL}' to \
             automatically choose the prior two modes."
        ));

    let allow_other_feature = flag_arg(
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

    let disallow_deprecated = tri_flag_arg::<DisallowDeprecated>(
        DISALLOW_DEPRECATED,
        "Disallow any deprecated keywords are present.",
    );

    let fix_log_scale_offset = flag_arg(
        FIX_LOG_SCALE_OFFSETS,
        format!(
            "Fix {pne} keys that have log scaling with zero offset. \
             Specifically, this will replace values like 'X,0.0' with 'X,1.0' \
             where 'X' is a positive decimal number. Having '0.0' for log offset \
             is mathematical nonsense.",
        ),
    );

    let disallow_localtime = flag_arg(
        DISALLOW_LOCALTIME,
        format!(
            "Require that {begindatetime} and {enddatetime} have a timezone if \
             provided. This is not required by the standard, but not having a \
             timezone is ambiguous since the absolute value of the timestamp is \
             dependent on localtime and therefore is location-dependent. Only \
             affects FCS 3.2.",
        ),
    );

    let date_pattern = Arg::new(DATE_PATTERN)
        .long(DATE_PATTERN)
        .value_name("PATTERN")
        .value_parser(value_parser!(DatePattern))
        .help(format!(
            "Pattern to match {date} keyword. See {date_header}."
        ));

    let time_pattern = Arg::new(TIME_PATTERN)
        .long(TIME_PATTERN)
        .value_name("PATTERN")
        .value_parser(value_parser!(TimePattern))
        .help(format!(
            "Pattern to match {btim}/{etim} keywords. See {time_header}.",
        ));

    let datetime_pattern = Arg::new(DATETIME_PATTERN)
        .long(DATETIME_PATTERN)
        .value_name("PATTERN")
        .help(format!(
            "If supplied, will be used as an alternative pattern when parsing \
             {begindatetime} and {enddatetime}. It should follow the format \
             outline in {CHRONO_REF}.",
        ));

    let last_modified_pattern = Arg::new(LAST_MODIFIED_PATTERN)
        .long(LAST_MODIFIED_PATTERN)
        .value_name("PATTERN")
        .help(format!(
            "If supplied, will be used as an alternative pattern when parsing \
             {last_modified}. It should follow the format outline in {CHRONO_REF}.",
        ));

    let ns_meas_pattern = Arg::new(NS_MEAS_PATTERN)
        .long(NS_MEAS_PATTERN)
        .value_name("REGEXP")
        .value_parser(value_parser!(NonStdMeasPattern))
        .help(format!(
            "Pattern to use when matching non-standard measurement keywords. \
             It must include '{NON_STD_MEAS_INDEX_PAT}' which will be \
             replaced with measurement index. Defaults to \
             '{NON_STD_MEAS_PAT_DEFAULT}'.",
        ));

    let all_std_args = [
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
        disallow_deprecated,
        fix_log_scale_offset,
        disallow_localtime,
        ns_meas_pattern,
    ];

    // layout args

    let text_data_correction = correction_arg(TEXT_DATA_COR, false, &data_seg);
    let text_analysis_correction = correction_arg(TEXT_ANALYSIS_COR, false, &analysis_seg);

    let ignore_text_data_offsets = flag_arg(
        IGNORE_TEXT_DATA_OFFSETS,
        format!("Ignore offsets for {data_seg} from {text_seg}."),
    );

    let ignore_text_analysis_offsets = flag_arg(
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

    let int_widths_from_byteord = flag_arg(
        INT_WIDTHS_FROM_BYTEORD,
        format!(
            "Set {pnb} based on length of {byteord}. Only has effect \
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
            "Disallow {pnr} values which need to be truncated to fit in type \
             dictated by {datatype} (and {pndatatype} for FCS 3.2) and {pnb} \
             for a given measurement."
        ),
    );

    let all_layout_args = [
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

    let truncate_event_values = Arg::new(TRUNCATE_EVENT_VALUES)
        .long(TRUNCATE_EVENT_VALUES)
        .value_name("WHICH")
        .value_parser(value_parser!(TruncateEventValues))
        .help(format!(
            "Truncate values exceeding {pnr}. \
             Must be one of '{TRUNCATE_INT_ONLY_LEVEL}' (default), \
             '{TRUNCATE_ALL_LEVEL}', or '{TRUNCATE_NONE_LEVEL}'.",
        ));

    let disallow_over_range = tri_flag_arg::<DisallowOverRange>(
        DISALLOW_OVER_RANGE,
        format!(
            "Forbid values in DATA to exceed {pnr}. Does nothing if column \
             was truncated according to '{TRUNCATE_EVENT_VALUES}'."
        ),
    );

    let all_dataset_args = [
        allow_uneven_event_width,
        allow_tot_mismatch,
        truncate_event_values,
        disallow_over_range,
    ];

    // shared args

    let warnings_are_errors = flag_arg(WARNINGS_ARE_ERRORS, "Treat all warnings as fatal errors.");

    let hide_warnings = flag_arg(HIDE_WARNINGS, "Hide all warnings.");

    let all_shared_args = [warnings_are_errors, hide_warnings];

    // other args

    let delim_arg = Arg::new(DELIM)
        .long(DELIM)
        .short('d')
        .value_name("CHAR")
        .help("Delimiter to use for tabular output.")
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

    let header_cmd = Command::new(SUBCMD_HEADER)
        .about("Show header as JSON.")
        .arg(&input_arg)
        .args(&all_header_args)
        .args(&all_offset_args);

    let flat_cmd = Command::new(SUBCMD_FLAT)
        .about("Show flat keywords as JSON.")
        .arg(&input_arg)
        .arg(&dataset_index_arg)
        .args(&all_header_args)
        .args(&all_offset_args)
        .args(&all_flat_args)
        .args(&all_shared_args)
        .after_long_help(flat_long_help);

    let std_cmd = Command::new(SUBCMD_STD)
        .about("Dump standardized keywords as JSON.")
        .arg(&input_arg)
        .arg(&dataset_index_arg)
        .args(&all_header_args)
        .args(&all_offset_args)
        .args(&all_flat_args)
        .args(&all_std_args)
        .args(&all_layout_args)
        .args(&all_shared_args)
        .after_long_help(&std_long_help);

    let meas_cmd = Command::new(SUBCMD_MEAS)
        .about("Show a table of standardized measurement values.")
        .arg(&input_arg)
        .arg(&dataset_index_arg)
        .args(&all_header_args)
        .args(&all_offset_args)
        .args(&all_flat_args)
        .args(&all_std_args)
        .args(&all_layout_args)
        .args(&all_shared_args)
        .arg(&delim_arg)
        .after_long_help(&std_long_help);

    let spill_cmd = Command::new(SUBCMD_SPILL)
        .about("Dump the spillover matrix if present.")
        .arg(&input_arg)
        .arg(&dataset_index_arg)
        .args(&all_header_args)
        .args(&all_offset_args)
        .args(&all_flat_args)
        .args(&all_std_args)
        .args(&all_layout_args)
        .args(&all_shared_args)
        .arg(&delim_arg)
        .after_long_help(&std_long_help);

    let data_cmd = Command::new(SUBCMD_DATA)
        .about(format!("Show a table of the {data_seg} segment."))
        .arg(&input_arg)
        .arg(&dataset_index_arg)
        .args(&all_header_args)
        .args(&all_offset_args)
        .args(&all_flat_args)
        .args(&all_std_args)
        .args(&all_layout_args)
        .args(&all_dataset_args)
        .args(&all_shared_args)
        .arg(&delim_arg)
        .after_long_help(&std_long_help);

    let summarize_cmd = Command::new(SUBCMD_SUMMARIZE)
        .about("Summarize datasets in FCS file")
        .arg(&input_arg)
        .args(&all_header_args)
        .args(&all_offset_args)
        .args(&all_flat_args)
        .args(&all_layout_args)
        .args(&all_dataset_args)
        .args(&all_shared_args)
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
        .subcommand(summarize_cmd);

    let args = cmd.clone().get_matches();

    match args.subcommand() {
        Some((SUBCMD_HEADER, sargs)) => {
            let conf = get_header_config(sargs);
            let filepath = get_input_path(sargs);
            let ((), res) = fcs_read_header(filepath, DatasetOffset(0), &conf)
                .resolve_commutative(print_warnings, |s| s);
            print_json(&res?);
            Ok(())
        }

        Some((SUBCMD_FLAT, sargs)) => {
            let conf = get_flat_config(cmd.find_subcommand_mut(SUBCMD_FLAT).unwrap(), sargs);
            let filepath = get_input_path(sargs);
            let skip = get_dataset_index(sargs);
            let ((), res) = fcs_read_flat_texts(filepath, skip, Some(1), &conf)
                .resolve_commutative(print_warnings, |s| s);
            print_json(&res?[0]);
            Ok(())
        }

        Some((SUBCMD_SPILL, sargs)) => {
            let conf = get_std_config(&cmd, sargs);
            let delim = get_delim(sargs);
            let filepath = get_input_path(sargs);
            let skip = get_dataset_index(sargs);
            let ((), res) = fcs_read_std_texts(filepath, skip, Some(1), &conf)
                .resolve_commutative(print_warnings, |s| s);
            let (core, _) = &res?[0];
            core.print_comp_or_spillover_table(delim);
            Ok(())
        }

        Some((SUBCMD_MEAS, sargs)) => {
            let conf = get_std_config(&cmd, sargs);
            let delim = get_delim(sargs);
            let filepath = get_input_path(sargs);
            let skip = get_dataset_index(sargs);
            let ((), res) = fcs_read_std_texts(filepath, skip, Some(1), &conf)
                .resolve_commutative(print_warnings, |s| s);
            let (core, _) = &res?[0];
            core.print_meas_table(delim);
            Ok(())
        }

        Some((SUBCMD_STD, sargs)) => {
            let conf = get_std_config(&cmd, sargs);
            let filepath = get_input_path(sargs);
            let skip = get_dataset_index(sargs);
            let ((), res) = fcs_read_std_texts(filepath, skip, Some(1), &conf)
                .resolve_commutative(print_warnings, |s| s);
            let (core, uncore) = &res?[0];
            let obj = json!({"core": core, "uncore": uncore});
            print_json(&obj);
            Ok(())
        }

        Some((SUBCMD_DATA, sargs)) => {
            let conf = get_std_dataset_config(&cmd, sargs);
            let delim = get_delim(sargs);
            let filepath = get_input_path(sargs);
            let skip = get_dataset_index(sargs);
            let ((), res) = fcs_read_std_datasets(filepath, skip, Some(1), &conf)
                .resolve_commutative(print_warnings, |s| s);
            let (core, _) = &res?[0];
            print_parsed_data(core, delim);
            Ok(())
        }

        Some((SUBCMD_SUMMARIZE, sargs)) => {
            let conf = get_flat_dataset_config(&cmd, sargs);
            let filepath = get_input_path(sargs);
            let skip = get_skip(sargs);
            let limit = get_limit(sargs);
            let ((), res) = fcs_summarize(filepath, skip, limit, &conf)
                .resolve_commutative(print_warnings, |s| s);
            print_json(&res?);
            Ok(())
        }

        _ => Ok(()),
    }
}

fn flag_arg(long: &'static str, help: impl IntoResettable<StyledStr>) -> Arg {
    Arg::new(long)
        .long(long)
        .action(ArgAction::SetTrue)
        .help(help)
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
    config::ReadHeaderInnerConfig {
        text_correction: get_correction(sargs, TEXT_COR),
        data_correction: get_correction(sargs, DATA_COR),
        analysis_correction: get_correction(sargs, ANALYSIS_COR),
        // don't add other corrections since these aren't used in this api (yet)
        other_corrections: vec![],
        max_other: sargs.get_one::<usize>(MAX_OTHER).copied(),
        other_width: get_def(sargs, OTHER_WIDTH),
        guess_other_width: get_def(sargs, GUESS_OTHER_WIDTH),
        squish_offsets: sargs.get_flag(SQUISH_OFFSETS).into(),
    }
}

fn get_offsets_config(sargs: &ArgMatches) -> config::ReadOffsetConfig {
    config::ReadOffsetConfig {
        allow_pseudoempty: sargs.get_flag(ALLOW_PSEUDOEMPTY).into(),
        truncate_offset_limit: get_def(sargs, TRUNCATE_OFFSET_LIMIT),
        overlap_correction_limit: get_def(sargs, OVERLAP_CORRECTION_LIMIT),
        data_remainder_limit: get_def(sargs, DATA_REMAINDER_LIMIT),
    }
}

fn get_header_and_text_config(
    cmd: &Command,
    sargs: &ArgMatches,
) -> config::ReadHeaderAndTEXTConfig {
    let version_override = sargs.get_one(VERSION_OVERRIDE).copied();

    let nextdata_correction = sargs.get_one(NEXTDATA_COR).copied().unwrap_or_default();

    let parse_key_or_pat = |lit_flag: &str, pat_flag: &str| {
        let lits = sargs
            .get_many::<KeyStringOrPattern>(lit_flag)
            .unwrap_or_default();
        let pats = sargs
            .get_many::<KeyStringOrPattern>(pat_flag)
            .unwrap_or_default();
        lits.chain(pats).cloned().map(|x| (x, ())).collect()
    };

    let ignore_standard_keys = parse_key_or_pat(IGNORE_STD_LIT_KEY, IGNORE_STD_PAT_KEY);
    let promote_to_standard = parse_key_or_pat(PROMOTE_LIT_TO_STD, PROMOTE_PAT_TO_STD);
    let demote_from_standard = parse_key_or_pat(DEMOTE_LIT_FROM_STD, DEMOTE_PAT_FROM_STD);

    let Ok(rename_standard_keys): Result<KeyStringPairs, Infallible> = sargs
        .get_many::<BiKeystringPair>(RENAME_STD_KEYS)
        .unwrap_or_default()
        .cloned()
        .collect::<HashMap<_, _>>()
        .try_into()
        .map_err(|e| post_validation_error(cmd, RENAME_STD_KEYS, e).exit());

    let parse_keystring_pair = |name: &str| {
        sargs
            .get_many::<KeystringStringPair>(name)
            .unwrap_or_default()
            .cloned()
            .collect()
    };

    let parse_subpattern = |name: &str| sargs.get_many::<SubPatternPair>(name).unwrap_or_default();

    let sub_lits = parse_subpattern(SUB_STD_LIT_KEY_VALS);
    let sub_pats = parse_subpattern(SUB_STD_PAT_KEY_VALS);

    let substitute_standard_key_values = sub_lits.chain(sub_pats).cloned().collect();

    config::ReadHeaderAndTEXTConfig {
        version_override,
        supp_text_correction: get_correction(sargs, SUPP_TEXT_COR),
        nextdata_correction,
        allow_duplicated_supp_text: get_def(sargs, ALLOW_OVERLAPPING_SUPP_TEXT),
        ignore_supp_text: sargs.get_flag(IGNORE_SUPP_TEXT).into(),
        delim_escape_mode: get_def(sargs, DELIM_ESCAPE_MODE),
        allow_non_ascii_delim: get_def(sargs, ALLOW_NON_ASCII_DELIM),
        allow_missing_final_delim: get_def(sargs, ALLOW_MISSING_FINAL_DELIM),
        allow_nonunique: get_def(sargs, ALLOW_NON_UNIQUE),
        allow_odd: get_def(sargs, ALLOW_ODD),
        allow_empty_keys: get_def(sargs, ALLOW_EMPTY_KEYS),
        allow_delim_at_boundary: get_def(sargs, ALLOW_DELIM_AT_BOUNDARY),
        allow_non_utf8: get_def(sargs, ALLOW_NON_UTF8),
        use_latin1: sargs.get_flag(USE_LATIN1).into(),
        allow_non_ascii_keywords: get_def(sargs, ALLOW_NON_ASCII_KEYWORDS),
        allow_missing_supp_text: get_def(sargs, ALLOW_MISSING_SUPP_TEXT),
        allow_supp_text_own_delim: get_def(sargs, ALLOW_SUPP_TEXT_OWN_DELIM),
        allow_missing_nextdata: get_def(sargs, ALLOW_MISSING_NEXTDATA),
        trim_value_whitespace: get_def(sargs, TRIM_VALUE_WHITESPACE),
        trim_text_end: sargs.get_flag(TRIM_TEXT_END).into(),
        ignore_standard_keys,
        rename_standard_keys,
        promote_to_standard,
        demote_from_standard,
        replace_standard_key_values: parse_keystring_pair(REPLACE_STD_KEY_VALS),
        append_standard_keywords: parse_keystring_pair(APPEND_STD_KEY_VALS),
        substitute_standard_key_values,
    }
}

fn get_std_inner_config(sargs: &ArgMatches) -> config::ReadStdKeywordsConfig {
    let time_meas_pattern = sargs
        .get_one(TIME_MEAS_PATTERN)
        .cloned()
        .unwrap_or_default();

    let ignore_time_optical_keys = sargs
        .get_many::<TemporalOpticalKey>(IGNORE_TIME_OPTICAL_KEYS)
        .unwrap_or_default()
        .copied()
        .collect::<HashSet<_>>()
        .into();

    let ns_meas_pat = sargs.get_one::<NonStdMeasPattern>(NS_MEAS_PATTERN).cloned();

    config::ReadStdKeywordsConfig {
        dedup_measurement_names: sargs.get_flag(DEDUP_MEAS_NAMES).into(),
        trim_intra_value_whitespace: sargs.get_flag(TRIM_INTRA_VALUE_WHITESPACE).into(),
        time_meas_pattern,
        force_linear_scale: get_def(sargs, FORCE_LINEAR_SCALE),
        ignore_time_optical_keys,
        process_time_optical_keys: get_def(sargs, PROCESS_TIME_OPTICAL_KEYS),
        allow_missing_time: get_def(sargs, ALLOW_MISSING_TIME),
        spillover_measurement_mode: get_def(sargs, SPILLOVER_MEASUREMENT_MODE),
        date_pattern: sargs.get_one(DATE_PATTERN).cloned(),
        time_pattern: sargs.get_one(TIME_PATTERN).cloned(),
        datetime_pattern: sargs.get_one::<String>(DATETIME_PATTERN).cloned(),
        last_modified_pattern: sargs.get_one::<String>(LAST_MODIFIED_PATTERN).cloned(),
        allow_other_feature: sargs.get_flag(ALLOW_OTHER_FEATURE).into(),
        process_pseudostandard: get_def(sargs, PROCESS_PSEUDOSTANDARD),
        process_hyper_par: get_def(sargs, PROCESS_HYPER_PAR),
        process_other_version: get_def(sargs, PROCESS_OTHER_VERSION),
        process_extra_timestep: get_def(sargs, PROCESS_EXTRA_TIMESTEP),
        disallow_deprecated: get_def(sargs, DISALLOW_DEPRECATED),
        fix_log_scale_offsets: sargs.get_flag(FIX_LOG_SCALE_OFFSETS).into(),
        disallow_localtime: sargs.get_flag(DISALLOW_LOCALTIME).into(),
        nonstandard_measurement_pattern: NonStdMeasPatternOpt(ns_meas_pat),
    }
}

fn get_flat_config(cmd: &Command, sargs: &ArgMatches) -> config::ReadFlatTEXTConfig {
    config::ReadFlatTEXTConfig {
        header: get_header_inner_config(sargs),
        flat: get_header_and_text_config(cmd, sargs),
        offset: get_offsets_config(sargs),
        shared: get_shared_config(sargs),
    }
}

fn get_std_config(cmd: &Command, sargs: &ArgMatches) -> config::ReadStdTEXTConfig {
    config::ReadStdTEXTConfig {
        header: get_header_inner_config(sargs),
        flat: get_header_and_text_config(cmd, sargs),
        offset: get_offsets_config(sargs),
        standard: get_std_inner_config(sargs),
        layout: get_layout_config(sargs),
        shared: get_shared_config(sargs),
    }
}

fn get_flat_dataset_config(cmd: &Command, sargs: &ArgMatches) -> config::ReadFlatDatasetConfig {
    config::ReadFlatDatasetConfig {
        header: get_header_inner_config(sargs),
        flat: get_header_and_text_config(cmd, sargs),
        offset: get_offsets_config(sargs),
        layout: get_layout_config(sargs),
        data: get_dataset_inner_config(sargs),
        shared: get_shared_config(sargs),
    }
}

fn get_std_dataset_config(cmd: &Command, sargs: &ArgMatches) -> config::ReadStdDatasetConfig {
    config::ReadStdDatasetConfig {
        header: get_header_inner_config(sargs),
        flat: get_header_and_text_config(cmd, sargs),
        offset: get_offsets_config(sargs),
        standard: get_std_inner_config(sargs),
        layout: get_layout_config(sargs),
        data: get_dataset_inner_config(sargs),
        shared: get_shared_config(sargs),
    }
}

fn get_layout_config(sargs: &ArgMatches) -> config::ReadDataKeywordsConfig {
    config::ReadDataKeywordsConfig {
        text_data_correction: get_correction(sargs, TEXT_DATA_COR),
        text_analysis_correction: get_correction(sargs, TEXT_ANALYSIS_COR),
        ignore_text_data_offsets: sargs.get_flag(IGNORE_TEXT_DATA_OFFSETS).into(),
        ignore_text_analysis_offsets: sargs.get_flag(IGNORE_TEXT_ANALYSIS_OFFSETS).into(),
        allow_header_text_offset_mismatch: get_def(sargs, ALLOW_HEADER_TEXT_OFFSET_MISMATCH),
        allow_missing_required_offsets: get_def(sargs, ALLOW_MISSING_REQUIRED_OFFSETS),
        process_optional_failure: get_def(sargs, PROCESS_OPTIONAL_FAILURE),
        integer_widths_from_byteord: sargs.get_flag(INT_WIDTHS_FROM_BYTEORD).into(),
        integer_byteord_override: get_opt(sargs, INT_BYTEORD_OVERRIDE),
        disallow_range_truncation: get_def(sargs, DISALLOW_RANGE_TRUNCATION),
    }
}

fn get_dataset_inner_config(sargs: &ArgMatches) -> config::ReadEventsConfig {
    config::ReadEventsConfig {
        allow_tot_mismatch: get_def(sargs, ALLOW_TOT_MISMATCH),
        allow_uneven_event_width: get_def(sargs, ALLOW_UNEVEN_EVENT_WIDTH),
        truncate_event_values: get_def(sargs, TRUNCATE_EVENT_VALUES),
        disallow_over_range: get_def(sargs, DISALLOW_OVER_RANGE),
    }
}

fn get_shared_config(sargs: &ArgMatches) -> config::ReadSharedConfig {
    config::ReadSharedConfig {
        warnings_are_errors: sargs.get_flag(WARNINGS_ARE_ERRORS),
        hide_warnings: sargs.get_flag(HIDE_WARNINGS),
    }
}

fn get_input_path(sargs: &ArgMatches) -> &PathBuf {
    sargs
        .get_one::<PathBuf>(INPUT_PATH)
        .expect("path is required")
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

fn get_delim(sargs: &ArgMatches) -> &String {
    sargs.get_one::<String>(DELIM).unwrap()
}

fn get_def<T>(sargs: &ArgMatches, name: &str) -> T
where
    T: Default + Copy + Sync + Send + 'static,
{
    sargs.get_one(name).copied().unwrap_or_default()
}

fn get_correction<I, S>(sargs: &ArgMatches, name: &str) -> OffsetCorrection<I, S> {
    sargs
        .get_one::<(i32, i32)>(name)
        .copied()
        .unwrap_or_default()
        .into()
}

fn get_opt<T>(sargs: &ArgMatches, name: &str) -> Option<T>
where
    T: Default + Copy + Sync + Send + 'static,
{
    sargs.get_one(name).copied()
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

fn parse_keystring_literal(s: &str) -> Result<KeyStringOrPattern, AsciiStringError> {
    s.parse::<KeyString>().map(KeyStringOrPattern::Literal)
}

fn parse_keystring_pattern(s: &str) -> Result<KeyStringOrPattern, KeyRegexError> {
    Ok(s.parse::<CaseInsRegex>().map(KeyStringOrPattern::Pattern)?)
}

fn parse_sub_pattern_literal(s: &str) -> Result<SubPatternPair, String> {
    parse_sub_pattern_pair(s, |k| Ok(parse_keystring_literal(k)?))
}

fn parse_sub_pattern_pattern(s: &str) -> Result<SubPatternPair, String> {
    parse_sub_pattern_pair(s, |k| Ok(parse_keystring_pattern(k)?))
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
    Ok((kf, v.to_owned()))
}

fn parse_sub_pattern_pair<F>(s: &str, f: F) -> Result<SubPatternPair, String>
where
    F: FnOnce(&str) -> AppResult<KeyStringOrPattern>,
{
    let (k, v) = s.split_once(',').ok_or("must be a comma separated pair")?;
    let kf = f(k).map_err(|e| e.to_string())?;
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

fn print_json<T: Serialize>(j: &T) {
    println!("{}", serde_json::to_string(j).unwrap());
}

pub fn print_parsed_data(core: &AnyCoreDataset, delim: &str) {
    let df = core.as_data();
    let nrows = df.nrows();
    let cols: Vec<_> = df.iter_columns().collect();
    let ncols = cols.len();
    if ncols == 0 {
        return;
    }
    let mut ns = core.shortnames().into_iter();
    print!("{}", ns.next().unwrap());
    for n in ns {
        print!("{delim}{n}");
    }
    for r in 0..nrows {
        println!();
        print!("{}", cols[0].pos_to_string(r));
        (1..ncols).for_each(|c| print!("{delim}{}", cols[c].pos_to_string(r)));
    }
}

fn print_warnings<W: Display>(ws: impl IntoIterator<Item = W>) {
    for w in ws {
        eprintln!("WARNING: {w}");
    }
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

type KeystringStringPair = (KeyString, String);

type SubPatternPair = (KeyStringOrPattern, SubPattern);

const SUBCMD_HEADER: &str = "header";

const SUBCMD_FLAT: &str = "flat";

const SUBCMD_STD: &str = "std";

const SUBCMD_DATA: &str = "data";

const SUBCMD_SUMMARIZE: &str = "summarize";

const SUBCMD_MEAS: &str = "measurements";

const SUBCMD_SPILL: &str = "spillover";

const TEXT_COR: &str = "text-correction";

const DATA_COR: &str = "data-correction";

const ANALYSIS_COR: &str = "analysis-correction";

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

const ALLOW_OVERLAPPING_SUPP_TEXT: &str = "allow-overlapping-supp-text";

const IGNORE_SUPP_TEXT: &str = "ignore-supp-text";

const DELIM_ESCAPE_MODE: &str = "delim-escape-mode";

const ALLOW_NON_ASCII_DELIM: &str = "allow-non-ascii-delim";

const ALLOW_MISSING_FINAL_DELIM: &str = "allow-missing-final-delim";

const ALLOW_NON_UNIQUE: &str = "allow-non-unique";

const ALLOW_ODD: &str = "allow-odd";

const ALLOW_EMPTY_KEYS: &str = "allow-empty-keys";

const ALLOW_DELIM_AT_BOUNDARY: &str = "allow-delim-at-boundary";

const ALLOW_NON_UTF8: &str = "allow-non-utf8";

const USE_LATIN1: &str = "use-latin1";

const ALLOW_NON_ASCII_KEYWORDS: &str = "allow-non-ascii-keywords";

const ALLOW_MISSING_SUPP_TEXT: &str = "allow-missing-supp-text";

const ALLOW_SUPP_TEXT_OWN_DELIM: &str = "allow-supp-text-own-delim";

const ALLOW_MISSING_NEXTDATA: &str = "allow-missing-nextdata";

const TRIM_VALUE_WHITESPACE: &str = "trim-value-whitespace";

const TRIM_TEXT_END: &str = "trim-text-end";

const IGNORE_STD_LIT_KEY: &str = "ignore-std-lit-key";

const IGNORE_STD_PAT_KEY: &str = "ignore-std-pat-key";

const PROMOTE_LIT_TO_STD: &str = "promote-lit-to-std";

const PROMOTE_PAT_TO_STD: &str = "promote-pat-to-std";

const DEMOTE_LIT_FROM_STD: &str = "demote-lit-from-std";

const DEMOTE_PAT_FROM_STD: &str = "demote-pat-from-std";

const RENAME_STD_KEYS: &str = "rename-std-keys";

const REPLACE_STD_KEY_VALS: &str = "replace-std-key-vals";

const APPEND_STD_KEY_VALS: &str = "append-std-key-vals";

const SUB_STD_LIT_KEY_VALS: &str = "sub-std-lit-key-vals";

const SUB_STD_PAT_KEY_VALS: &str = "sub-std-pat-key-vals";

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

const DISALLOW_DEPRECATED: &str = "disallow-deprecated";

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

const TRUNCATE_EVENT_VALUES: &str = "truncate-event-values";

const DISALLOW_OVER_RANGE: &str = "disallow-over-range";

const ALLOW_TOT_MISMATCH: &str = "allow-tot-mismatch";

const DELIM: &str = "delimiter";

const DATASET_INDEX: &str = "dataset-index";

const SKIP: &str = "skip";

const LIMIT: &str = "limit";

const INPUT_PATH: &str = "input-path";

const CHRONO_REF: &str = "https://docs.rs/chrono/latest/chrono/format/strftime/index.html";

const REGEXP_REF: &str = "https://docs.rs/regex/latest/regex/#syntax";

const REGEXP_REP_REF: &str = "https://docs.rs/regex/latest/regex/struct.Regex.html#method.replace";

use clap::builder::ValueParser;
use fireflow_core::api::{
    fcs_read_flat_texts, fcs_read_header, fcs_read_std_datasets, fcs_read_std_texts, fcs_summarize,
};
use fireflow_core::config::{
    self, DatasetOffset, DelimEscapeMode, ForceLinearScale, ParseTemporalOpticalKeyError,
    ProcessExtraTimestep, ProcessHyperPar, ProcessOptionalFailure, ProcessOtherVersion,
    ProcessPseudostandard, TemporalOpticalKey, TimeMeasNamePattern, TriFlag, TruncateEventValues,
    VersionOverride,
};
use fireflow_core::core::AnyCoreDataset;
use fireflow_core::segment::HeaderCorrection;
use fireflow_core::text::keywords::ByteOrd2_0;
use fireflow_core::validated::ascii_range::OtherWidth;
use fireflow_core::validated::datepattern::DatePattern;
use fireflow_core::validated::keys::{AsciiStringError, KeyString, KeyStringsOrPatterns};
use fireflow_core::validated::keystring_pairs::KeyStringPairs;
use fireflow_core::validated::nonstd_meas_pattern::NonStdMeasPattern;
use fireflow_core::validated::sub_pattern::SubPattern;
use fireflow_core::validated::timepattern::TimePattern;
use regex::Regex;

use ansi_term::{ANSIString, Style};
use clap::{
    Arg, ArgAction, ArgMatches, Command,
    builder::{IntoResettable, StyledStr},
    error::ErrorKind,
    value_parser,
};
use itertools::Itertools as _;
use serde::ser::Serialize;
use serde_json::json;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::Display;
use std::iter::once;
use std::path::PathBuf;
use std::process::exit;

fn main() {
    match run() {
        Ok(()) => (),
        Err(e) => {
            eprintln!("{e}");
            exit(1)
        }
    }
}

#[allow(clippy::too_many_lines)]
fn run() -> AppResult<()> {
    let kw_style = Style::new().italic();
    let seg_style = Style::new().italic();

    let header_seg = seg_style.paint("HEADER");
    let text_seg = seg_style.paint("TEXT");
    let prim_text_seg = seg_style.paint("primary TEXT");
    let supp_text_seg = seg_style.paint("supplemental TEXT");
    let data_seg = seg_style.paint("DATA");
    let analysis_seg = seg_style.paint("ANALYSIS");
    let other_seg = seg_style.paint("OTHER");

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
            "If \"escaped\" or \"unescaped\", escape or do not escape \
             delimiters respectively."
                .into(),
            format!(
                "If \"guess_escaped\" or \"guess_unescaped\" attempt to guess how \
                 delimiters should be treated, falling back to escaped or unescaped \
                 mode respectively if the choice is ambiguous. The determination \
                 will be made by first scanning {text_seg} to find all delimiter \
                 positions and choosing the mode which results in an even number of \
                 tokens with no delimiters in keys (escaped mode) and no blank keys \
                 (unescaped mode)."
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
                 unescaped mode since `\"\"` is almost never a sensible key value."
            ),
            format!(
                "The guessing algorithm is independent of \
                 --{TRIM_TRAILING_WHITESPACE} since it will ignore everything \
                 after the last delimiter. It is also independent of --{ALLOW_ODD} \
                 and --{ALLOW_MISSING_FINAL_DELIM} which will trigger as normal if \
                 their respective violations are found."
            ),
            format!(
                "If unescaped mode ends up be used, then --{ALLOW_EMPTY_VALUES} is \
                 implied to be set."
            ),
        ],
    );

    let (sub_header, sub_help) = format_section(
        "SUBSTITUTION",
        [format!(
            "The SUB part in --{SUB_STD_LIT_KEY_VALS} and --{SUB_STD_PAT_KEY_VALS} \
             is a sed-like pattern which will be used to edit the value of KEY. \
             It must be a string like 's<D><FROM><D><TO>[<D>g]' where 'D' is a \
             delimiter (any character), FROM is a regular expression and TO is a \
             replacement pattern. FROM and TO must follow the syntax outlined in \
             {REGEXP_REF} and {REGEXP_REP_REF} respectively, with the caveat that \
             only bracketed replacement syntax is allowed."
        )],
    );

    let (date_header, date_help) = format_section(
        "DATE PATTERN",
        [format!(
            "The value for --{DATE_PATTERN} will be used as an alternative pattern when \
             parsing {kw}. It should have specifiers for year, month, and \
             day as outlined in {CHRONO_REF}. If not supplied, {kw} will \
             be parsed according to the standard pattern which is \
             '%d-%b-%Y'.",
            kw = kw_style.paint("$DATE"),
        )],
    );

    let (time_header, time_help) = format_section(
        "TIME PATTERN",
        [format!(
            "If supplied, will be used as an alternative pattern when \
             parsing {b} and {e} It should have specifiers for \
             hours, minutes, and seconds as outlined in {CHRONO_REF}. It may \
             optionally also have a sub-seconds specifier as shown in the \
             same link. Furthermore, the specifiers '%!' and %@' may be used \
             to match 1/60 and centiseconds respectively. If not supplied, \
             {b} and {e} will be parsed according to the standard \
             pattern which is version-specific.",
            b = kw_style.paint("$BTIM"),
            e = kw_style.paint("$ETIM"),
        )],
    );

    let flat_long_help = [&delim_help, &sub_help].iter().join("\n\n");
    let std_long_help = [&delim_help, &sub_help, &date_help, &time_help]
        .iter()
        .join("\n\n");

    let correction_arg = |long: &'static str, is_begin: bool, seg: &ANSIString| {
        let loc = if is_begin { "begin" } else { "end" };
        let h = format!("Adjustment for {loc} {seg} offset.");
        Arg::new(long)
            .long(long)
            .value_name("OFFSET")
            .help(h)
            .value_parser(value_parser!(i32))
    };

    // header args

    let text_correction_begin = correction_arg(TEXT_COR_BEGIN, true, &text_seg);
    let text_correction_end = correction_arg(TEXT_COR_END, false, &text_seg);

    let data_correction_begin = correction_arg(DATA_COR_BEGIN, true, &data_seg);
    let data_correction_end = correction_arg(DATA_COR_END, false, &data_seg);

    let analysis_correction_begin = correction_arg(ANALYSIS_COR_BEGIN, true, &analysis_seg);
    let analysis_correction_end = correction_arg(ANALYSIS_COR_END, false, &analysis_seg);

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

    let squish_offsets = flag_arg(
        SQUISH_OFFSETS,
        format!(
            "If {data_seg}/{analysis_seg} end in 0, use 0 for start as well. \
             Should not be used for FCS 2.0 files."
        ),
    );

    let allow_negative = flag_arg(ALLOW_NEGATIVE, "Substitute 0 for negative offsets.");

    let truncate_offsets = flag_arg(TRUNCATE_OFFSETS, "Truncate offsets that exceed file size.");

    let all_header_args = [
        text_correction_begin,
        text_correction_end,
        data_correction_begin,
        data_correction_end,
        analysis_correction_begin,
        analysis_correction_end,
        max_other,
        other_width,
        squish_offsets,
        allow_negative,
        truncate_offsets,
    ];

    // "flat" args

    let version_override = Arg::new(VERSION_OVERRIDE)
        .long(VERSION_OVERRIDE)
        .value_name("OVERRIDE")
        .value_parser(value_parser!(VersionOverride))
        .help(format!(
            "Override the FCS version from {header_seg}. Can be an FCS \
             version string (like 'FCS3.2') which will force to a fixed version. \
             Can also autodetect version with one of 'latest' or 'earliest' \
             (the latest or earliest available version respectively) or 'loose' \
             or 'strict' (the available version with the most or least optional \
             keywords respectively)."
        ));

    let supp_text_correction_begin = correction_arg(SUPP_TEXT_COR_BEGIN, true, &supp_text_seg);
    let supp_text_correction_end = correction_arg(SUPP_TEXT_COR_END, false, &supp_text_seg);

    let nextdata_correction = Arg::new(NEXTDATA_COR)
        .long(NEXTDATA_COR)
        .value_name("INT")
        .help(format!("Correction for {}", kw_style.paint("$NEXTDATA")));

    let allow_overlapping_supp_text = tri_flag_arg(
        ALLOW_OVERLAPPING_SUPP_TEXT,
        true,
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

    let non_ascii_delim = tri_flag_arg(
        ALLOW_NON_ASCII_DELIM,
        true,
        format!("Allow {text_seg} delimiter to be non-ASCII character."),
    );

    let missing_final_delim = tri_flag_arg(
        ALLOW_MISSING_FINAL_DELIM,
        true,
        format!("Allow final {text_seg} delimiter to be missing."),
    );

    let allow_non_unique = tri_flag_arg(
        ALLOW_NON_UNIQUE,
        true,
        format!("Allow non-unique keys to exist in {text_seg}."),
    );

    let allow_odd = tri_flag_arg(ALLOW_ODD, true, "Allow odd number of tokens.");

    let allow_empty_keys = tri_flag_arg(
        ALLOW_EMPTY_KEYS,
        true,
        "Allow keys to be blank (relatively rare).",
    );

    let allow_empty_values = tri_flag_arg(
        ALLOW_EMPTY_VALUES,
        true,
        format!(
            "Allow values to be blank if --{TRIM_VALUE_WHITESPACE} is set \
             and values are entirely whitespace (relatively common)."
        ),
    );

    let allow_delim_at_bound = tri_flag_arg(
        ALLOW_DELIM_AT_BOUNDARY,
        true,
        format!("Allow {text_seg} delimiter(s) to be at token boundaries."),
    );

    let allow_non_utf8 = tri_flag_arg(
        ALLOW_NON_UTF8,
        true,
        format!("Allow non-UTF8 characters in {text_seg} segment."),
    );

    let use_latin1 = flag_arg(
        USE_LATIN1,
        format!("Interpret all characters in {text_seg} as Latin-1 (aka ISO/IEC 8859-1)."),
    );

    let allow_non_ascii_keywords = tri_flag_arg(
        ALLOW_NON_ASCII_KEYWORDS,
        true,
        "Allow non-ASCII characters in keys.",
    );

    let allow_missing_supp_text = tri_flag_arg(
        ALLOW_MISSING_SUPP_TEXT,
        true,
        format!("Allow {supp_text_seg} offsets to be missing."),
    );

    let allow_supp_text_own_delim = tri_flag_arg(
        ALLOW_SUPP_TEXT_OWN_DELIM,
        true,
        format!("Allow delimiters in {prim_text_seg} and {supp_text_seg} to differ."),
    );

    let allow_missing_nextdata = tri_flag_arg(
        ALLOW_MISSING_NEXTDATA,
        true,
        format!("Allow {} to be missing.", kw_style.paint("$NEXTDATA")),
    );

    let trim_value_whitespace = flag_arg(
        TRIM_VALUE_WHITESPACE,
        "Trim whitespace from beginning and end of all values.",
    );

    let trim_trailing_whitespace = flag_arg(
        TRIM_TRAILING_WHITESPACE,
        "Trim whitespace from end of TEXT.",
    );

    let make_key_str_args = |lit_flag, pat_flag, lit_help, pat_help| {
        let lit_arg = Arg::new(lit_flag)
            .long(lit_flag)
            .action(ArgAction::Append)
            .value_name("KEY")
            .help(lit_help);
        let pat_arg = Arg::new(pat_flag)
            .long(pat_flag)
            .action(ArgAction::Append)
            .value_name("REGEXP")
            .help(pat_help);
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
        .value_parser(ValueParser::new(parse_rename_std_keys))
        .help("Rename standard keys from OLD to NEW. The leading '$' is implied.");

    let replace_std_key_vals = Arg::new(REPLACE_STD_KEY_VALS)
        .long(REPLACE_STD_KEY_VALS)
        .action(ArgAction::Append)
        .value_name("KEY,VAL")
        .help(
            "Replace values of standard keys matching KEY with VAl. \
             The leading '$' is implied for the key.",
        );

    let append_std_key_vals = Arg::new(APPEND_STD_KEY_VALS)
        .long(APPEND_STD_KEY_VALS)
        .action(ArgAction::Append)
        .value_name("KEY,VAL")
        .help(
            "Append standard keys with KEY and VAL to list of existing standard \
             keys. The leading '$' is implied for KEY.",
        );

    let sub_std_lit_key_vals = Arg::new(SUB_STD_LIT_KEY_VALS)
        .long(SUB_STD_LIT_KEY_VALS)
        .action(ArgAction::Append)
        .value_name("KEY,SUB")
        .help(format!(
            "Edit standard key values using KEY and SUB. The leading '$' \
             is implied for KEY. See {sub_header} for details."
        ));

    let sub_std_pat_key_vals = Arg::new(SUB_STD_PAT_KEY_VALS)
        .long(SUB_STD_PAT_KEY_VALS)
        .action(ArgAction::Append)
        .value_name("REGEXP,SUB")
        .help(format!(
            "Edit standard keys matching REGEXP with SUB. The leading '$' is \
             implied for KEY. See {sub_header} for details."
        ));

    let all_flat_args = vec![
        version_override,
        supp_text_correction_begin,
        supp_text_correction_end,
        nextdata_correction,
        allow_overlapping_supp_text,
        ignore_supp_text,
        lit_delims,
        non_ascii_delim,
        missing_final_delim,
        allow_non_unique,
        allow_odd,
        allow_empty_keys,
        allow_empty_values,
        allow_delim_at_bound,
        allow_non_utf8,
        use_latin1,
        allow_non_ascii_keywords,
        allow_missing_supp_text,
        allow_supp_text_own_delim,
        allow_missing_nextdata,
        trim_value_whitespace,
        trim_trailing_whitespace,
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
        "Force all $PnN to be unique by appending '~X' to each duplicate \
         and appending 'X' (starting at 0)",
    );

    let trim_intra_value_whitespace = flag_arg(
        TRIM_INTRA_VALUE_WHITESPACE,
        "Remove spaces between comma-separated values.",
    );

    let time_meas_pattern = Arg::new(TIME_MEAS_PATTERN)
        .long(TIME_MEAS_PATTERN)
        .value_name("REGEXP")
        .help(
            "Use REGEXP when matching time measurement (defaults to \
             '^Time|TIME$', pass 'NoTime' to not look for a time channel).",
        )
        .value_parser(ValueParser::new(parse_time_meas_pattern));

    let allow_missing_time = tri_flag_arg(
        ALLOW_MISSING_TIME,
        true,
        "Allow time measurement to be missing.",
    );

    let force_linear_scale = Arg::new(FORCE_LINEAR_SCALE)
        .long(FORCE_LINEAR_SCALE)
        .value_name("WHICH")
        .value_parser(value_parser!(ForceLinearScale))
        .help(format!(
            "Force {} keywords to be linear. Pass 'time_only' to only set the \
             temporal measurement, 'all' to set all measurements, and 'none' \
             for no measurements.",
            kw_style.paint("$PnE")
        ));

    let ignore_time_optical_keys = Arg::new(IGNORE_TIME_OPTICAL_KEYS)
        .long(IGNORE_TIME_OPTICAL_KEYS)
        .action(ArgAction::Append)
        .value_name("SYMS")
        .help(format!(
            "Ignore optical keywords for temporal measurement. Must be a \
             comma-separated list of strings like the X in {}.",
            kw_style.paint("$PnX")
        ))
        .value_parser(ValueParser::new(parse_time_optical_keys));

    let parse_indexed_spillover = flag_arg(
        PARSE_INDEXED_SPILLOVER,
        format!(
            "Parse numeric indices for {} rather than string names ({}).",
            kw_style.paint("$SPILLOVER"),
            kw_style.paint("$PnN")
        ),
    );

    let allow_other_feature = flag_arg(
        ALLOW_OTHER_FEATURE,
        format!(
            "Allow {} to be a value other than \"Area\", \"Width\", or \"Height\"",
            kw_style.paint("$PnFEATURE")
        ),
    );

    let process_pseudostandard = proc_kw_fail_arg(
        PROCESS_PSEUDOSTANDARD,
        "Process non-standard keywords that start with a '$'.",
    )
    .value_parser(value_parser!(ProcessPseudostandard));

    let process_hyper_par = proc_kw_fail_arg(
        PROCESS_HYPER_PAR,
        "Process measurement keywords whose index is greater than $PAR.",
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
            "Process unused {}, which may indicate that a time measurement \
             is present but not identified.",
            kw_style.paint("TIMESTEP")
        ),
    )
    .value_parser(value_parser!(ProcessExtraTimestep));

    let disallow_deprecated = tri_flag_arg(
        DISALLOW_DEPRECATED,
        false,
        "Disallow any deprecated keywords are present.",
    );

    let fix_log_scale_offset = flag_arg(
        FIX_LOG_SCALE_OFFSETS,
        format!(
            "Fix {} keys that have log scaling with zero offset. \
             Specifically, this will replace values like 'X,0.0' with 'X,1.0' \
             where 'X' is a positive decimal number. Having '0.0' for log offset \
             is mathematical nonsense.",
            kw_style.paint("$PnE")
        ),
    );

    let disallow_localtime = flag_arg(
        DISALLOW_LOCALTIME,
        format!(
            "Require that {} and {} have a timezone if provided. This is not \
             required by the standard, but not having a timezone is ambiguous \
             since the absolute value of the timestamp is dependent on localtime \
             and therefore is location-dependent. Only affects FCS 3.2.",
            kw_style.paint("$BEGINDATETIME"),
            kw_style.paint("$ENDDATETIME")
        ),
    );

    let date_pattern = Arg::new(DATE_PATTERN)
        .long(DATE_PATTERN)
        .value_name("PATTERN")
        .value_parser(value_parser!(DatePattern))
        .help(format!(
            "Pattern to match {} keyword. See {date_header}.",
            kw_style.paint("$DATE")
        ));

    let time_pattern = Arg::new(TIME_PATTERN)
        .long(TIME_PATTERN)
        .value_name("PATTERN")
        .value_parser(value_parser!(TimePattern))
        .help(format!(
            "Pattern to match {}/{} keywords. See {time_header}.",
            kw_style.paint("$BTIM"),
            kw_style.paint("$ETIM"),
        ));

    let datetime_pattern = Arg::new(DATETIME_PATTERN)
        .long(DATETIME_PATTERN)
        .value_name("PATTERN")
        .help(format!(
            "If supplied, will be used as an alternative pattern when parsing \
             {} and {}. It should follow the format outline in {CHRONO_REF}.",
            kw_style.paint("$BEGINDATETIME"),
            kw_style.paint("ENDDATETIME"),
        ));

    let last_modified_pattern = Arg::new(LAST_MODIFIED_PATTERN)
        .long(LAST_MODIFIED_PATTERN)
        .value_name("PATTERN")
        .help(format!(
            "If supplied, will be used as an alternative pattern when parsing \
             {}. It should follow the format outline in {CHRONO_REF}.",
            kw_style.paint("LAST_MODIFIED"),
        ));

    let ns_meas_pattern = Arg::new(NS_MEAS_PATTERN)
        .long(NS_MEAS_PATTERN)
        .value_name("REGEXP")
        .value_parser(value_parser!(NonStdMeasPattern))
        .help(
            "Pattern to use when matching non-standard measurement keywords. \
             It must include '%n' which will be replaced with measurement index.",
        );

    let all_std_args = [
        dedup_meas_names,
        trim_intra_value_whitespace,
        time_meas_pattern,
        allow_missing_time,
        force_linear_scale,
        ignore_time_optical_keys,
        parse_indexed_spillover,
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

    let text_data_correction_begin = correction_arg(TEXT_DATA_COR_BEGIN, true, &data_seg);
    let text_data_correction_end = correction_arg(TEXT_DATA_COR_END, false, &data_seg);

    let text_analysis_correction_begin =
        correction_arg(TEXT_ANALYSIS_COR_BEGIN, true, &analysis_seg);
    let text_analysis_correction_end = correction_arg(TEXT_ANALYSIS_COR_END, false, &analysis_seg);

    let ignore_text_data_offsets = flag_arg(
        IGNORE_TEXT_DATA_OFFSETS,
        format!("Ignore offsets for {data_seg} from {text_seg}."),
    );

    let ignore_text_analysis_offsets = flag_arg(
        IGNORE_TEXT_ANALYSIS_OFFSETS,
        format!("Ignore offsets for {analysis_seg} from {text_seg}."),
    );

    let allow_header_text_offset_mismatch = tri_flag_arg(
        ALLOW_HEADER_TEXT_OFFSET_MISMATCH,
        true,
        format!(
            "Allow {header_seg} and {text_seg} offsets to be different, \
             in which case {header_seg} will be used."
        ),
    );

    let allow_missing_required_offsets = tri_flag_arg(
        ALLOW_MISSING_REQUIRED_OFFSETS,
        true,
        format!(
            "Allow required offsets to be missing from {text_seg}. \
             Only applies to FCS 3.0/3.1."
        ),
    );

    let truncate_text_offsets = flag_arg(
        TRUNCATE_TEXT_OFFSETS,
        format!("Truncate offsets in {text_seg} if they exceed end of file."),
    );

    let process_optional_failure = proc_kw_fail_arg(
        PROCESS_OPTIONAL_FAILURE,
        "Process optional keys if they cause an error.",
    )
    .value_parser(value_parser!(ProcessOptionalFailure));

    let int_widths_from_byteord = flag_arg(
        INT_WIDTHS_FROM_BYTEORD,
        format!(
            "Set {} based on length of {}. Only has effect \
             on integer layouts in FCS 2.0/3.0.",
            kw_style.paint("$PnB"),
            kw_style.paint("$BYTEORD"),
        ),
    );

    let int_byteord_override = Arg::new(INT_BYTEORD_OVERRIDE)
        .long(INT_BYTEORD_OVERRIDE)
        .value_name("BYTEORD")
        .value_parser(value_parser!(ByteOrd2_0))
        .help(format!(
            "Override the value of {}. \
             Only has effect on integer layouts in FCS 2.0/3.0.",
            kw_style.paint("$BYTEORD"),
        ));

    let disallow_range_truncation = tri_flag_arg(
        DISALLOW_RANGE_TRUNCATION,
        false,
        format!(
            "Disallow {} values which need to be truncated to fit in type \
             dictated by {} (and {} for FCS 3.2) and {} for a given measurement.",
            kw_style.paint("$PnR"),
            kw_style.paint("$DATATYPE"),
            kw_style.paint("$PnDATATYPE"),
            kw_style.paint("$PnB"),
        ),
    );

    let all_layout_args = [
        text_data_correction_begin,
        text_data_correction_end,
        text_analysis_correction_begin,
        text_analysis_correction_end,
        ignore_text_data_offsets,
        ignore_text_analysis_offsets,
        allow_header_text_offset_mismatch,
        allow_missing_required_offsets,
        truncate_text_offsets,
        process_optional_failure,
        int_widths_from_byteord,
        int_byteord_override,
        disallow_range_truncation,
    ];

    // dataset args

    let allow_uneven_event_width = tri_flag_arg(
        ALLOW_UNEVEN_EVENT_WIDTH,
        true,
        format!("Allow event width to not evenly divide length of {data_seg}."),
    );

    let allow_tot_mismatch = tri_flag_arg(
        ALLOW_TOT_MISMATCH,
        true,
        format!(
            "Allow {} to mismatch the number of events that are actually in {data_seg}.",
            kw_style.paint("$TOT")
        ),
    );

    let truncate_event_values = Arg::new(TRUNCATE_EVENT_VALUES)
        .long(TRUNCATE_EVENT_VALUES)
        .value_name("WHICH")
        .value_parser(value_parser!(TruncateEventValues))
        .help(format!(
            "Truncate values exceeding {}. \
             Must be one of 'int_only' (default), 'all', or 'none'.",
            kw_style.paint("$PnR"),
        ));

    let disallow_over_range = tri_flag_arg(
        DISALLOW_OVER_RANGE,
        false,
        format!(
            "Forbid values in DATA to exceed {}. Does nothing if column \
             was truncated according to '{}'.",
            kw_style.paint("$PnR"),
            TRUNCATE_EVENT_VALUES
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
        .help("Delimiter to use for tabular output.")
        .default_value("\t");

    let dataset_index_arg = Arg::new(DATASET_INDEX)
        .long(DATASET_INDEX)
        .short('I')
        .value_parser(value_parser!(usize))
        .help("Index of the dataset to parse (starting from 0)");

    let skip_arg = Arg::new(SKIP)
        .long(SKIP)
        .value_parser(value_parser!(usize))
        .help("Number of datasets to skip");

    let limit_arg = Arg::new(LIMIT)
        .long(LIMIT)
        .value_parser(value_parser!(usize))
        .help("Number of datasets to return");

    let input_arg = Arg::new(INPUT_PATH)
        .short('i')
        .long(INPUT_PATH)
        .value_parser(value_parser!(PathBuf))
        .help("Path to FCS file to parse.")
        .required(true);

    let header_cmd = Command::new(SUBCMD_HEADER)
        .about("Show header as JSON.")
        .arg(&input_arg)
        .args(&all_header_args);

    let flat_cmd = Command::new(SUBCMD_FLAT)
        .about("Show flat keywords as JSON.")
        .arg(&input_arg)
        .arg(&dataset_index_arg)
        .args(&all_header_args)
        .args(&all_flat_args)
        .args(&all_shared_args)
        .after_long_help(flat_long_help);

    let std_cmd = Command::new(SUBCMD_STD)
        .about("Dump standardized keywords as JSON.")
        .arg(&input_arg)
        .arg(&dataset_index_arg)
        .args(&all_header_args)
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
        .args(&all_flat_args)
        .args(&all_layout_args)
        .args(&all_dataset_args)
        .args(&all_shared_args)
        .arg(&skip_arg)
        .arg(&limit_arg);

    let mut cmd = Command::new("fireflow")
        .about("read and write FCS files")
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
            let conf = parse_header_config(sargs);
            let filepath = parse_input_path(sargs);
            let h = fcs_read_header(filepath, DatasetOffset(0), &conf.into())?;
            print_json(&h);
            Ok(())
        }

        Some((SUBCMD_FLAT, sargs)) => {
            let conf = parse_flat_config(cmd.find_subcommand_mut(SUBCMD_FLAT).unwrap(), sargs)?;
            let filepath = parse_input_path(sargs);
            let skip = parse_dataset_index(sargs);
            let ((), res) = fcs_read_flat_texts(filepath, skip, Some(1), &conf)
                .resolve_commutative(print_warnings, |s| s);
            // ASSUME this won't fail because we ask for one dataset
            print_json(&res?[0]);
            Ok(())
        }

        Some((SUBCMD_SPILL, sargs)) => {
            let conf = parse_std_config(&cmd, sargs)?;
            let delim = parse_delim(sargs);
            let filepath = parse_input_path(sargs);
            let skip = parse_dataset_index(sargs);
            let ((), res) = fcs_read_std_texts(filepath, skip, Some(1), &conf)
                .resolve_commutative(print_warnings, |s| s);
            // ASSUME this won't fail because we ask for one dataset
            let (core, _) = &res?[0];
            core.print_comp_or_spillover_table(delim);
            Ok(())
        }

        Some((SUBCMD_MEAS, sargs)) => {
            let conf = parse_std_config(&cmd, sargs)?;
            let delim = parse_delim(sargs);
            let filepath = parse_input_path(sargs);
            let skip = parse_dataset_index(sargs);
            let ((), res) = fcs_read_std_texts(filepath, skip, Some(1), &conf)
                .resolve_commutative(print_warnings, |s| s);
            // ASSUME this won't fail because we ask for one dataset
            let (core, _) = &res?[0];
            core.print_meas_table(delim);
            Ok(())
        }

        Some((SUBCMD_STD, sargs)) => {
            let conf = parse_std_config(&cmd, sargs)?;
            let filepath = parse_input_path(sargs);
            let skip = parse_dataset_index(sargs);
            let ((), res) = fcs_read_std_texts(filepath, skip, Some(1), &conf)
                .resolve_commutative(print_warnings, |s| s);
            // ASSUME this won't fail because we ask for one dataset
            let (core, uncore) = &res?[0];
            let obj = json!({"core": core, "uncore": uncore});
            print_json(&obj);
            Ok(())
        }

        Some((SUBCMD_DATA, sargs)) => {
            let conf = parse_std_dataset_config(&cmd, sargs)?;
            let delim = parse_delim(sargs);
            let filepath = parse_input_path(sargs);
            let skip = parse_dataset_index(sargs);
            let ((), res) = fcs_read_std_datasets(filepath, skip, Some(1), &conf)
                .resolve_commutative(print_warnings, |s| s);
            // ASSUME this won't fail because we ask for one dataset
            let (core, _) = &res?[0];
            print_parsed_data(core, delim);
            Ok(())
        }

        Some((SUBCMD_SUMMARIZE, sargs)) => {
            let conf = parse_flat_dataset_config(&cmd, sargs)?;
            let filepath = parse_input_path(sargs);
            let skip = parse_skip(sargs);
            let limit = parse_limit(sargs);
            let ((), res) = fcs_summarize(filepath, skip, limit, &conf)
                .resolve_commutative(print_warnings, |s| s);
            let _: () = print_json(&res?);
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
    Arg::new(long).long(long).help(format!(
        "{help_front} Must be one of 'error', 'demote', 'drop', or \
         'drop_silent' which will throw an error, demote to non-standard, \
         drop with warning, or drop silently respectively"
    ))
}

fn tri_flag_arg(long: &'static str, false_is_error: bool, help_front: impl Display) -> Arg {
    let parse_false_is_err = |s: &str| match s {
        "silent" => Ok(TriFlag::Noop),
        "warn" => Ok(TriFlag::True),
        _ => Err("Must be one of 'silent' or 'warn'"),
    };

    let parse_true_is_err = |s: &str| match s {
        "silent" => Ok(TriFlag::Noop),
        "error" => Ok(TriFlag::True),
        _ => Err("Must be one of 'silent' or 'error'"),
    };

    let (x, y, p) = if false_is_error {
        ("warn", "warning", ValueParser::new(parse_false_is_err))
    } else {
        ("error", "error", ValueParser::new(parse_true_is_err))
    };
    Arg::new(long).long(long).value_parser(p).help(format!(
        "{help_front} If '{x}', throw {y}. If 'silent', ignore completely."
    ))
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

fn parse_header_config(sargs: &ArgMatches) -> config::ReadHeaderInnerConfig {
    fn get_correction<I>(am: &ArgMatches, x0: &str, x1: &str) -> HeaderCorrection<I> {
        let y0 = am.get_one(x0).copied();
        let y1 = am.get_one(x1).copied();
        (y0, y1).into()
    }
    let text_correction = get_correction(sargs, TEXT_COR_BEGIN, TEXT_COR_END);
    let data_correction = get_correction(sargs, DATA_COR_BEGIN, DATA_COR_END);
    let analysis_correction = get_correction(sargs, ANALYSIS_COR_BEGIN, ANALYSIS_COR_END);
    config::ReadHeaderInnerConfig {
        text_correction,
        data_correction,
        analysis_correction,
        // don't add other corrections since these aren't used in this api (yet)
        other_corrections: vec![],
        max_other: sargs.get_one::<usize>(MAX_OTHER).copied(),
        other_width: parse_def(sargs, OTHER_WIDTH),
        squish_offsets: sargs.get_flag(SQUISH_OFFSETS).into(),
        allow_negative: sargs.get_flag(ALLOW_NEGATIVE).into(),
        truncate_offsets: sargs.get_flag(TRUNCATE_OFFSETS).into(),
    }
}

fn parse_header_and_text_config(
    cmd: &Command,
    sargs: &ArgMatches,
) -> AppResult<config::ReadHeaderAndTEXTConfig> {
    let version_override = sargs.get_one(VERSION_OVERRIDE).copied();
    let stext0 = sargs.get_one(SUPP_TEXT_COR_BEGIN).copied();
    let stext1 = sargs.get_one(SUPP_TEXT_COR_END).copied();
    let supp_text_correction = (stext0, stext1).into();

    let nextdata_correction = sargs.get_one(NEXTDATA_COR).copied().unwrap_or_default();

    let to_blank = |s: &str| Ok((s.to_owned(), ()));

    let ignore_standard_keys =
        parse_key_or_pat(sargs, IGNORE_STD_LIT_KEY, IGNORE_STD_PAT_KEY, to_blank)?;

    let promote_to_standard =
        parse_key_or_pat(sargs, PROMOTE_LIT_TO_STD, PROMOTE_PAT_TO_STD, to_blank)?;
    let demote_from_standard =
        parse_key_or_pat(sargs, DEMOTE_LIT_FROM_STD, DEMOTE_PAT_FROM_STD, to_blank)?;

    let rename_standard_keys: KeyStringPairs = sargs
        .get_many::<(KeyString, KeyString)>(RENAME_STD_KEYS)
        .unwrap_or_default()
        .cloned()
        .collect::<HashMap<_, _>>()
        .try_into()
        .map_err(|e| post_validation_error(cmd, RENAME_STD_KEYS, e).exit())?;
    // parse_hashmap(sargs, RENAME_STD_KEYS, |s| Ok(s.parse::<KeyString>()?))?;

    let replace_standard_key_values =
        parse_hashmap(sargs, REPLACE_STD_KEY_VALS, |x| Ok(Into::into(x)))?;
    let append_standard_keywords =
        parse_hashmap(sargs, APPEND_STD_KEY_VALS, |x| Ok(Into::into(x)))?;

    let to_sub = |s: &str| {
        let (k, v) = s.split_once(',').unwrap();
        Ok((k.to_owned(), parse_sub_pattern(v)?))
    };

    let substitute_standard_key_values =
        parse_key_or_pat(sargs, SUB_STD_LIT_KEY_VALS, SUB_STD_PAT_KEY_VALS, to_sub)?;

    let ret = config::ReadHeaderAndTEXTConfig {
        header: parse_header_config(sargs),
        version_override,
        supp_text_correction,
        nextdata_correction,
        allow_overlapping_supp_text: parse_tri_flag(sargs, ALLOW_OVERLAPPING_SUPP_TEXT),
        ignore_supp_text: sargs.get_flag(IGNORE_SUPP_TEXT).into(),
        delim_escape_mode: parse_def(sargs, DELIM_ESCAPE_MODE),
        allow_non_ascii_delim: parse_tri_flag(sargs, ALLOW_NON_ASCII_DELIM),
        allow_missing_final_delim: parse_tri_flag(sargs, ALLOW_MISSING_FINAL_DELIM),
        allow_nonunique: parse_tri_flag(sargs, ALLOW_NON_UNIQUE),
        allow_odd: parse_tri_flag(sargs, ALLOW_ODD),
        allow_empty_keys: parse_tri_flag(sargs, ALLOW_EMPTY_KEYS),
        allow_empty_values: parse_tri_flag(sargs, ALLOW_EMPTY_VALUES),
        allow_delim_at_boundary: parse_tri_flag(sargs, ALLOW_DELIM_AT_BOUNDARY),
        allow_non_utf8: parse_tri_flag(sargs, ALLOW_NON_UTF8),
        use_latin1: sargs.get_flag(USE_LATIN1).into(),
        allow_non_ascii_keywords: parse_tri_flag(sargs, ALLOW_NON_ASCII_KEYWORDS),
        allow_missing_supp_text: parse_tri_flag(sargs, ALLOW_MISSING_SUPP_TEXT),
        allow_supp_text_own_delim: parse_tri_flag(sargs, ALLOW_SUPP_TEXT_OWN_DELIM),
        allow_missing_nextdata: parse_tri_flag(sargs, ALLOW_MISSING_NEXTDATA),
        trim_value_whitespace: sargs.get_flag(TRIM_VALUE_WHITESPACE).into(),
        trim_trailing_whitespace: sargs.get_flag(TRIM_TRAILING_WHITESPACE).into(),
        ignore_standard_keys,
        rename_standard_keys,
        promote_to_standard,
        demote_from_standard,
        replace_standard_key_values,
        append_standard_keywords,
        substitute_standard_key_values,
    };
    Ok(ret)
}

fn parse_std_inner_config(sargs: &ArgMatches) -> config::ReadStdKeywordsConfig {
    let time_meas_pattern = sargs
        .get_one::<Option<TimeMeasNamePattern>>(TIME_MEAS_PATTERN)
        .cloned()
        .unwrap_or_default();

    let ignore_time_optical_keys = sargs
        .get_many::<Vec<TemporalOpticalKey>>(IGNORE_TIME_OPTICAL_KEYS)
        .unwrap_or_default()
        .flatten()
        .copied()
        .collect();

    config::ReadStdKeywordsConfig {
        dedup_measurement_names: sargs.get_flag(DEDUP_MEAS_NAMES).into(),
        trim_intra_value_whitespace: sargs.get_flag(TRIM_INTRA_VALUE_WHITESPACE).into(),
        time_meas_pattern,
        force_linear_scale: parse_def(sargs, FORCE_LINEAR_SCALE),
        ignore_time_optical_keys,
        allow_missing_time: parse_tri_flag(sargs, ALLOW_MISSING_TIME),
        parse_indexed_spillover: sargs.get_flag(PARSE_INDEXED_SPILLOVER).into(),
        date_pattern: sargs.get_one(DATE_PATTERN).cloned(),
        time_pattern: sargs.get_one(TIME_PATTERN).cloned(),
        datetime_pattern: sargs.get_one::<String>(DATETIME_PATTERN).cloned(),
        last_modified_pattern: sargs.get_one::<String>(LAST_MODIFIED_PATTERN).cloned(),
        allow_other_feature: sargs.get_flag(ALLOW_OTHER_FEATURE).into(),
        process_pseudostandard: parse_def(sargs, PROCESS_PSEUDOSTANDARD),
        process_hyper_par: parse_def(sargs, PROCESS_HYPER_PAR),
        process_other_version: parse_def(sargs, PROCESS_OTHER_VERSION),
        process_extra_timestep: parse_def(sargs, PROCESS_EXTRA_TIMESTEP),
        disallow_deprecated: parse_tri_flag(sargs, DISALLOW_DEPRECATED),
        fix_log_scale_offsets: sargs.get_flag(FIX_LOG_SCALE_OFFSETS).into(),
        disallow_localtime: sargs.get_flag(DISALLOW_LOCALTIME).into(),
        nonstandard_measurement_pattern: sargs.get_one(NS_MEAS_PATTERN).cloned(),
    }
}

fn parse_flat_config(cmd: &Command, sargs: &ArgMatches) -> AppResult<config::ReadFlatTEXTConfig> {
    let ret = config::ReadFlatTEXTConfig {
        flat: parse_header_and_text_config(cmd, sargs)?,
        shared: parse_shared_config(sargs),
    };
    Ok(ret)
}

fn parse_std_config(cmd: &Command, sargs: &ArgMatches) -> AppResult<config::ReadStdTEXTConfig> {
    let ret = config::ReadStdTEXTConfig {
        flat: parse_header_and_text_config(cmd, sargs)?,
        standard: parse_std_inner_config(sargs),
        layout: parse_layout_config(sargs),
        shared: parse_shared_config(sargs),
    };
    Ok(ret)
}

fn parse_flat_dataset_config(
    cmd: &Command,
    sargs: &ArgMatches,
) -> AppResult<config::ReadFlatDatasetConfig> {
    let ret = config::ReadFlatDatasetConfig {
        flat: parse_header_and_text_config(cmd, sargs)?,
        layout: parse_layout_config(sargs),
        data: parse_dataset_inner_config(sargs),
        shared: parse_shared_config(sargs),
    };
    Ok(ret)
}

fn parse_std_dataset_config(
    cmd: &Command,
    sargs: &ArgMatches,
) -> AppResult<config::ReadStdDatasetConfig> {
    let ret = config::ReadStdDatasetConfig {
        flat: parse_header_and_text_config(cmd, sargs)?,
        standard: parse_std_inner_config(sargs),
        layout: parse_layout_config(sargs),
        data: parse_dataset_inner_config(sargs),
        shared: parse_shared_config(sargs),
    };
    Ok(ret)
}

fn parse_layout_config(sargs: &ArgMatches) -> config::ReadDataKeywordsConfig {
    let data_corr0 = sargs.get_one(TEXT_DATA_COR_BEGIN).copied();
    let data_corr1 = sargs.get_one(TEXT_DATA_COR_END).copied();
    let text_data_correction = (data_corr0, data_corr1).into();

    let anal_corr0 = sargs.get_one(TEXT_ANALYSIS_COR_BEGIN).copied();
    let anal_corr1 = sargs.get_one(TEXT_ANALYSIS_COR_END).copied();
    let text_analysis_correction = (anal_corr0, anal_corr1).into();

    config::ReadDataKeywordsConfig {
        text_data_correction,
        text_analysis_correction,
        ignore_text_data_offsets: sargs.get_flag(IGNORE_TEXT_DATA_OFFSETS).into(),
        ignore_text_analysis_offsets: sargs.get_flag(IGNORE_TEXT_ANALYSIS_OFFSETS).into(),
        allow_header_text_offset_mismatch: parse_tri_flag(sargs, ALLOW_HEADER_TEXT_OFFSET_MISMATCH),
        allow_missing_required_offsets: parse_tri_flag(sargs, ALLOW_MISSING_REQUIRED_OFFSETS),
        truncate_text_offsets: sargs.get_flag(TRUNCATE_TEXT_OFFSETS).into(),
        process_optional_failure: parse_def(sargs, PROCESS_OPTIONAL_FAILURE),
        integer_widths_from_byteord: sargs.get_flag(INT_WIDTHS_FROM_BYTEORD).into(),
        integer_byteord_override: parse_opt(sargs, INT_BYTEORD_OVERRIDE),
        disallow_range_truncation: parse_tri_flag(sargs, DISALLOW_RANGE_TRUNCATION),
    }
}

fn parse_dataset_inner_config(sargs: &ArgMatches) -> config::ReadEventsConfig {
    config::ReadEventsConfig {
        allow_tot_mismatch: parse_tri_flag(sargs, ALLOW_TOT_MISMATCH),
        allow_uneven_event_width: parse_tri_flag(sargs, ALLOW_UNEVEN_EVENT_WIDTH),
        truncate_event_values: parse_def(sargs, TRUNCATE_EVENT_VALUES),
        disallow_over_range: parse_tri_flag(sargs, DISALLOW_OVER_RANGE),
    }
}

fn parse_shared_config(sargs: &ArgMatches) -> config::ReadSharedConfig {
    config::ReadSharedConfig {
        warnings_are_errors: sargs.get_flag(WARNINGS_ARE_ERRORS),
        hide_warnings: sargs.get_flag(HIDE_WARNINGS),
    }
}

fn parse_key_or_pat<'a, 'b, 'c, T, F: Fn(&'a str) -> AppResult<(String, T)>>(
    sargs: &'a ArgMatches,
    lit_flag: &'b str,
    pat_flag: &'c str,
    f: F,
) -> AppResult<KeyStringsOrPatterns<T>> {
    let ignore_std_lit_keys = sargs
        .get_many::<String>(lit_flag)
        .unwrap_or_default()
        .map(|s| f(s.as_str()).map_err(|e| fmt_arg_error(lit_flag, e)))
        .collect::<Result<Vec<_>, _>>()?;
    let ignore_std_pat_keys = sargs
        .get_many::<String>(pat_flag)
        .unwrap_or_default()
        .map(|s| f(s.as_str()).map_err(|e| fmt_arg_error(pat_flag, e)))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(KeyStringsOrPatterns::try_from_literals_and_patterns(
        ignore_std_lit_keys,
        ignore_std_pat_keys,
    )?)
}

fn parse_hashmap<'a, 'b, T, F: Fn(&'a str) -> AppResult<T>>(
    sargs: &'a ArgMatches,
    flag: &'b str,
    f: F,
) -> Result<HashMap<KeyString, T>, String> {
    sargs
        .get_many::<String>(flag)
        .unwrap_or_default()
        .map(|s| {
            // NOTE we can get away with this because we know that keys in FCS
            // cannot contain commas, and we are only using these as the keys
            // in this particular hash table
            let (k, v) = s.split_once(',').unwrap();
            f(v).map(|x| (k.parse::<KeyString>().unwrap(), x))
        })
        .collect::<Result<HashMap<_, _>, _>>()
        .map_err(|e| fmt_arg_error(flag, e))
}

fn parse_sub_pattern(s: &str) -> AppResult<SubPattern> {
    let (op, r0) = s
        .split_at_checked(1)
        .expect("sub pattern string must start with 's'");
    assert!(op == "s", "sub pattern string must start with 's'");
    let (delim, r1) = r0
        .split_at_checked(1)
        .expect("sub pattern delimiter is not a valid UTF-8 byte");
    let parts: Vec<_> = r1.split(delim).collect();
    let (from, to, global) = match &parts[..] {
        [x, y] | [x, y, ""] => (*x, *y, false),
        [x, y, "g"] => (*x, *y, true),
        _ => panic!(
            "sub pattern string must be like 's<D><FROM><D><TO>[<D>g]' \
             where 'D' is a delimiter (any character), FROM is a \
             regular expression and TO is a replacement pattern"
        ),
    };
    let r = Regex::new(from)?;
    Ok(SubPattern::try_new(r, to.to_owned(), global)?)
}

fn parse_input_path(sargs: &ArgMatches) -> &PathBuf {
    sargs
        .get_one::<PathBuf>(INPUT_PATH)
        .expect("path is required")
}

fn parse_dataset_index(sargs: &ArgMatches) -> Option<usize> {
    sargs.get_one::<usize>(DATASET_INDEX).copied()
}

fn parse_skip(sargs: &ArgMatches) -> Option<usize> {
    sargs.get_one::<usize>(SKIP).copied()
}

fn parse_limit(sargs: &ArgMatches) -> Option<usize> {
    sargs.get_one::<usize>(LIMIT).copied()
}

fn parse_delim(sargs: &ArgMatches) -> &String {
    sargs.get_one::<String>(DELIM).unwrap()
}

fn parse_def<T>(sargs: &ArgMatches, name: &str) -> T
where
    T: Default + Copy + Sync + Send + 'static,
{
    sargs.get_one(name).copied().unwrap_or_default()
}

fn parse_opt<T>(sargs: &ArgMatches, name: &str) -> Option<T>
where
    T: Default + Copy + Sync + Send + 'static,
{
    sargs.get_one(name).copied()
}

fn parse_tri_flag<T>(sargs: &ArgMatches, name: &str) -> T
where
    T: From<TriFlag>,
{
    sargs
        .get_one::<TriFlag>(name)
        .copied()
        .unwrap_or_default()
        .into()
}

fn parse_other_width(s: &str) -> Result<OtherWidth, String> {
    let x = s.parse::<u8>().map_err(|e| e.to_string())?;
    OtherWidth::try_from(x).map_err(|e| e.to_string())
}

fn parse_time_meas_pattern(s: &str) -> Result<Option<TimeMeasNamePattern>, regex::Error> {
    if s == "NoTime" {
        Ok(None)
    } else {
        Ok(Some(s.parse::<config::TimeMeasNamePattern>()?))
    }
}

fn parse_time_optical_keys(
    s: &str,
) -> Result<Vec<TemporalOpticalKey>, ParseTemporalOpticalKeyError> {
    s.split(',')
        .map(str::parse::<config::TemporalOpticalKey>)
        .collect()
}

fn parse_rename_std_keys(s: &str) -> Result<(KeyString, KeyString), AsciiStringError> {
    // NOTE we can get away with this because we know that keys in FCS
    // cannot contain commas, and we are only using these as the keys
    // in this particular hash table
    let (k, v) = s.split_once(',').unwrap();
    Ok((k.parse::<KeyString>()?, v.parse::<KeyString>()?))
}

fn print_json<T: Serialize>(j: &T) {
    println!("{}", serde_json::to_string(j).unwrap());
}

fn fmt_arg_error(name: &str, e: impl Display) -> String {
    format!("ERROR [{name}] {e}")
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

const SUBCMD_HEADER: &str = "header";

const SUBCMD_FLAT: &str = "flat";

const SUBCMD_STD: &str = "std";

const SUBCMD_DATA: &str = "data";

const SUBCMD_SUMMARIZE: &str = "summarize";

const SUBCMD_MEAS: &str = "measurements";

const SUBCMD_SPILL: &str = "spillover";

const TEXT_COR_BEGIN: &str = "text-correction-begin";
const TEXT_COR_END: &str = "text-correction-end";

const DATA_COR_BEGIN: &str = "data-correction-begin";
const DATA_COR_END: &str = "data-correction-end";

const ANALYSIS_COR_BEGIN: &str = "analysis-correction-begin";
const ANALYSIS_COR_END: &str = "analysis-correction-end";

const MAX_OTHER: &str = "max-other";

const OTHER_WIDTH: &str = "other-width";

const SQUISH_OFFSETS: &str = "squish-offsets";

const ALLOW_NEGATIVE: &str = "allow-negative";

const TRUNCATE_OFFSETS: &str = "truncate-offsets";

const VERSION_OVERRIDE: &str = "version-override";

const SUPP_TEXT_COR_BEGIN: &str = "supp-text-correction-begin";
const SUPP_TEXT_COR_END: &str = "supp-text-correction-end";

const NEXTDATA_COR: &str = "nextdata-correction";

const ALLOW_OVERLAPPING_SUPP_TEXT: &str = "allow-overlapping-supp-text";

const IGNORE_SUPP_TEXT: &str = "ignore-supp-text";

const DELIM_ESCAPE_MODE: &str = "delim-escape-mode";

const ALLOW_NON_ASCII_DELIM: &str = "allow-non-ascii-delim";

const ALLOW_MISSING_FINAL_DELIM: &str = "allow-missing-final-delim";

const ALLOW_NON_UNIQUE: &str = "allow-non-unique";

const ALLOW_ODD: &str = "allow-odd";

const ALLOW_EMPTY_KEYS: &str = "allow-empty-keys";

const ALLOW_EMPTY_VALUES: &str = "allow-empty-values";

const ALLOW_DELIM_AT_BOUNDARY: &str = "allow-delim-at-boundary";

const ALLOW_NON_UTF8: &str = "allow-non-utf8";

const USE_LATIN1: &str = "use-latin1";

const ALLOW_NON_ASCII_KEYWORDS: &str = "allow-non-ascii-keywords";

const ALLOW_MISSING_SUPP_TEXT: &str = "allow-missing-supp-text";

const ALLOW_SUPP_TEXT_OWN_DELIM: &str = "allow-supp-text-own-delim";

const ALLOW_MISSING_NEXTDATA: &str = "allow-missing-nextdata";

const TRIM_VALUE_WHITESPACE: &str = "trim-value-whitespace";

const TRIM_TRAILING_WHITESPACE: &str = "trim-trailing-whitespace";

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

const PARSE_INDEXED_SPILLOVER: &str = "parse-indexed-spillover";

const FORCE_LINEAR_SCALE: &str = "force-time-linear";

const IGNORE_TIME_OPTICAL_KEYS: &str = "ignore-time-optical-keys";

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

const TEXT_DATA_COR_BEGIN: &str = "text-data-correction-begin";
const TEXT_DATA_COR_END: &str = "text-data-correction-end";

const TEXT_ANALYSIS_COR_BEGIN: &str = "text-analysis-correction-begin";
const TEXT_ANALYSIS_COR_END: &str = "text-analysis-correction-end";

const IGNORE_TEXT_DATA_OFFSETS: &str = "ignore-text-data-offsets";

const IGNORE_TEXT_ANALYSIS_OFFSETS: &str = "ignore-text-analysis-offsets";

const ALLOW_HEADER_TEXT_OFFSET_MISMATCH: &str = "allow-text-offset-mismatch";

const ALLOW_MISSING_REQUIRED_OFFSETS: &str = "allow-missing-required-offsets";

const TRUNCATE_TEXT_OFFSETS: &str = "truncate-text-offsets";

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

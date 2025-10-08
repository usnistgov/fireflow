use fireflow_core::api::{
    fcs_read_header, fcs_read_raw_text, fcs_read_std_dataset, fcs_read_std_text,
};
use fireflow_core::config;
use fireflow_core::core::AnyCoreDataset;
use fireflow_core::error::{Terminal, TerminalFailure};
use fireflow_core::header::Version;
use fireflow_core::segment::HeaderCorrection;
use fireflow_core::text::byteord::ByteOrd2_0;
use fireflow_core::validated::datepattern::DatePattern;
use fireflow_core::validated::keys::{
    KeyOrStringPatterns, KeyOrStringPatternsError, KeyString, NonStdMeasPattern,
};
use fireflow_core::validated::sub_pattern::SubPattern;
use fireflow_core::validated::timepattern::TimePattern;
use regex::Regex;

use clap::{value_parser, Arg, ArgAction, ArgMatches, Command};
use nonempty::NonEmpty;
use serde::ser::Serialize;
use std::collections::HashMap;
use std::convert::Infallible;
use std::fmt::Display;
use std::path::PathBuf;

#[allow(clippy::too_many_lines)]
fn main() -> Result<(), ()> {
    let correction_arg = |long: &'static str, help: &'static str| {
        Arg::new(long)
            .long(long)
            .value_name("OFFSET")
            .help(help)
            .value_parser(value_parser!(i32))
    };

    let flag_arg = |long: &'static str, help: &'static str| {
        Arg::new(long)
            .long(long)
            .action(ArgAction::SetTrue)
            .help(help)
    };

    // header args

    let text_correction_begin = correction_arg(TEXT_COR_BEGIN, "adjustment for begin TEXT offset");
    let text_correction_end = correction_arg(TEXT_COR_END, "adjustment for end TEXT offset");

    let data_correction_begin = correction_arg(DATA_COR_BEGIN, "adjustment for begin DATA offset");
    let data_correction_end = correction_arg(DATA_COR_END, "adjustment for end DATA offset");

    let analysis_correction_begin =
        correction_arg(ANALYSIS_COR_BEGIN, "adjustment for begin ANALYSIS offset");
    let analysis_correction_end =
        correction_arg(ANALYSIS_COR_END, "adjustment for end ANALYSIS offset");

    let max_other = Arg::new(MAX_OTHER)
        .long(MAX_OTHER)
        .value_name("BYTES")
        .help("max number of OTHER segments to parse")
        .value_parser(value_parser!(usize));

    let other_width = Arg::new(OTHER_WIDTH)
        .long(OTHER_WIDTH)
        .value_name("WIDTH")
        .help("width of OTHER segments")
        .value_parser(value_parser!(u8));

    let squish_offsets = flag_arg(
        SQUISH_OFFSETS,
        "squish DATA/ANALYSIS, offsets that end in 0",
    );

    let allow_negative = flag_arg(ALLOW_NEGATIVE, "substitute 0 for negative offsets");

    let truncate_offsets = flag_arg(TRUNCATE_OFFSETS, "truncate offsets that exceed file size");

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

    // "raw" args

    let version_override = Arg::new(VERSION_OVERRIDE)
        .long(VERSION_OVERRIDE)
        .value_name("VERSION")
        .help("override the FCS version from HEADER");

    let supp_text_correction_begin = correction_arg(
        SUPP_TEXT_COR_BEGIN,
        "adjustment for begin supplemental TEXT offset",
    );
    let supp_text_correction_end = correction_arg(
        SUPP_TEXT_COR_END,
        "adjustment for end supplemental TEXT offset",
    );

    let allow_dup_supp_text = flag_arg(
        ALLOW_DUP_SUPP_TEXT,
        "only throw warning if supplemental TEXT is same as TEXT",
    );

    let ignore_supp_text = flag_arg(IGNORE_SUPP_TEXT, "ignore supplemental TEXT entirely");

    let lit_delims = flag_arg(LIT_DELIMS, "treat every delim as literal (no escaping)");

    let non_ascii_delim = flag_arg(
        ALLOW_NON_ASCII_DELIM,
        "allow delim to be non-ascii character",
    );

    let missing_final_delim = flag_arg(
        ALLOW_MISSING_FINAL_DELIM,
        "allow final delimiter to be missing from TEXT",
    );

    let allow_non_unique = flag_arg(ALLOW_NON_UNIQUE, "allow non-unique keys to exist");

    let allow_odd = flag_arg(ALLOW_ODD, "allow odd number of words in TEXT");

    let allow_empty = flag_arg(ALLOW_EMPTY, "allow keys to have blank values");

    let allow_delim_at_bound = flag_arg(
        ALLOW_DELIM_AT_BOUNDARY,
        "allow delims to be at word boundaries",
    );

    let allow_non_utf8 = flag_arg(ALLOW_NON_UTF8, "allow non-utf8 characters in TEXT");

    let use_latin1 = flag_arg(
        USE_LATIN1,
        "interpret all characters in TEXT as Latin-1 (aka ISO/IEC 8859-1)",
    );

    let allow_non_ascii_keywords = flag_arg(ALLOW_NON_ASCII_KEYWORDS, "allow non-ascii keys");

    let allow_missing_supp_text = flag_arg(
        ALLOW_MISSING_SUPP_TEXT,
        "allow supplemental TEXT offsets to be missing",
    );

    let allow_supp_text_own_delim = flag_arg(
        ALLOW_SUPP_TEXT_OWN_DELIM,
        "allow delimiters in primary and supplemental TEXT to differ",
    );

    let allow_missing_nextdata = flag_arg(ALLOW_MISSING_NEXTDATA, "allow $NEXTDATA to be missing");

    let trim_value_whitespace = flag_arg(TRIM_VALUE_WHITESPACE, "trim whitespace from all values");

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
        "standard key to ignore; the leading '$' is implied",
        "ignore standard keys matching the given pattern; the leading '$' is implied",
    );

    let (promote_lit_to_std, promote_pat_to_std) = make_key_str_args(
        PROMOTE_LIT_TO_STD,
        PROMOTE_PAT_TO_STD,
        "promote key to standard; the leading '$' is implied",
        "promote keys matching the given pattern to standard; the leading '$' is implied",
    );

    let (demote_lit_from_std, demote_pat_from_std) = make_key_str_args(
        DEMOTE_LIT_FROM_STD,
        DEMOTE_PAT_FROM_STD,
        "demote key from standard; the leading '$' is implied",
        "demote keys matching the given pattern from standard; the leading '$' is implied",
    );

    let rename_standard_keys = Arg::new(RENAME_STD_KEYS)
        .long(RENAME_STD_KEYS)
        .action(ArgAction::Append)
        .value_name("OLD,NEW")
        .help(
            "rename standard keys from OLD to NEW (separated by comma); \
             leading '$' is implied",
        );

    let replace_std_key_vals = Arg::new(REPLACE_STD_KEY_VALS)
        .long(REPLACE_STD_KEY_VALS)
        .action(ArgAction::Append)
        .value_name("KEY,VAL")
        .help(
            "replace values of standard keys matching KEY with VAl \
             (separated by comma); leading '$' is implied for the key",
        );

    let append_std_key_vals = Arg::new(APPEND_STD_KEY_VALS)
        .long(APPEND_STD_KEY_VALS)
        .action(ArgAction::Append)
        .value_name("KEY,VAL")
        .help(
            "append standard keys with KEY and VAL (separated by comma) \
             to list of standard keys; leading '$' is implied for KEY",
        );

    let sub_std_lit_key_vals = Arg::new(SUB_STD_LIT_KEY_VALS)
        .long(SUB_STD_LIT_KEY_VALS)
        .action(ArgAction::Append)
        .value_name("KEY,SUB")
        .help(format!(
            "Edit standard key values using KEY and SUB (separated by comma). \
             Leading '$' is implied for KEY. See '{SUB_SECTION}' below for details."
        ));

    let sub_std_pat_key_vals = Arg::new(SUB_STD_PAT_KEY_VALS)
        .long(SUB_STD_PAT_KEY_VALS)
        .action(ArgAction::Append)
        .value_name("REGEXP,SUB")
        .help(format!(
            "Edit standard keys matching REGEXP with SUB (separated by comma). \
             Leading '$' is implied for KEY. See '{SUB_SECTION}' below for details."
        ));

    let all_raw_args = vec![
        version_override,
        supp_text_correction_begin,
        supp_text_correction_end,
        allow_dup_supp_text,
        ignore_supp_text,
        lit_delims,
        non_ascii_delim,
        missing_final_delim,
        allow_non_unique,
        allow_odd,
        allow_empty,
        allow_delim_at_bound,
        allow_non_utf8,
        use_latin1,
        allow_non_ascii_keywords,
        allow_missing_supp_text,
        allow_supp_text_own_delim,
        allow_missing_nextdata,
        trim_value_whitespace,
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

    let trim_intra_value_whitespace = flag_arg(
        TRIM_INTRA_VALUE_WHITESPACE,
        "remove spaces between comma-separated values",
    );

    let time_meas_pattern = Arg::new(TIME_MEAS_PATTERN)
        .long(TIME_MEAS_PATTERN)
        .value_name("REGEXP")
        .help(
            "pattern to use when matching time measurement (defaults to \
             '^Time|TIME$', pass 'NoTime' to not look for a time channel)",
        );

    let allow_missing_time = flag_arg(ALLOW_MISSING_TIME, "allow time measurement to be missing");

    let force_time_linear = flag_arg(
        FORCE_TIME_LINEAR,
        "force $PnE for time measurement to be linear",
    );

    let ignore_time_gain = flag_arg(IGNORE_TIME_GAIN, "ignore $PnG for time measurement");

    let ignore_time_optical_keys = Arg::new(IGNORE_TIME_OPTICAL_KEYS)
        .long(IGNORE_TIME_OPTICAL_KEYS)
        .action(ArgAction::Append)
        .value_name("SYMS")
        .help(
            "optical keywords to ignore on temporal measurement, must be a \
             comma-separated list of strings like the X in 'PnX'",
        );

    let parse_indexed_spillover = flag_arg(
        PARSE_INDEXED_SPILLOVER,
        "parse numeric indices for $SPILLOVER rather than string names ($PnN)",
    );

    let allow_pseudostandard = flag_arg(
        ALLOW_PSEUDOSTANDARD,
        "allow non-standard keywords that start with a '$'",
    );

    let allow_unused_standard = flag_arg(ALLOW_UNUSED_STANDARD, "allow unused standard keywords");

    let allow_optional_dropping = flag_arg(
        ALLOW_OPTIONAL_DROPPING,
        "drop optional keys if they cause an error",
    );

    let disallow_deprecated = flag_arg(
        DISALLOW_DEPRECATED,
        "throw error if any deprecated keywords are present",
    );

    let fix_log_scale_offset = flag_arg(
        FIX_LOG_SCALE_OFFSETS,
        "fix PnE keys that have log scaling with zero offset (ie 'X,0.0')",
    );

    let date_pattern = Arg::new(DATE_PATTERN)
        .long(DATE_PATTERN)
        .value_name("PATTERN")
        .help("pattern to match $DATE keyword if it is non-conferment");

    let time_pattern = Arg::new(TIME_PATTERN)
        .long(TIME_PATTERN)
        .value_name("PATTERN")
        .help("pattern to match $BTIM/$ETIM keywords if non-conferment");

    let ns_meas_pattern = Arg::new(NS_MEAS_PATTERN)
        .long(NS_MEAS_PATTERN)
        .value_name("REGEXP")
        .help(
            "pattern to use when matching non-standard measurement keywords, \
             must include '%n' which will be replaced with measurement index",
        );

    let all_std_args = [
        trim_intra_value_whitespace,
        time_meas_pattern,
        allow_missing_time,
        force_time_linear,
        ignore_time_gain,
        ignore_time_optical_keys,
        parse_indexed_spillover,
        date_pattern,
        time_pattern,
        allow_pseudostandard,
        allow_unused_standard,
        allow_optional_dropping,
        disallow_deprecated,
        fix_log_scale_offset,
        ns_meas_pattern,
    ];

    // offset args

    let text_data_correction_begin = correction_arg(
        TEXT_DATA_COR_BEGIN,
        "adjustment for begin DATA offset from TEXT",
    );
    let text_data_correction_end = correction_arg(
        TEXT_DATA_COR_END,
        "adjustment for end DATA offset from TEXT",
    );

    let text_analysis_correction_begin = correction_arg(
        TEXT_ANALYSIS_COR_BEGIN,
        "adjustment for begin ANALYSIS offset from TEXT",
    );
    let text_analysis_correction_end = correction_arg(
        TEXT_ANALYSIS_COR_END,
        "adjustment for end ANALYSIS offset from TEXT",
    );

    let ignore_text_data_offsets = flag_arg(
        IGNORE_TEXT_DATA_OFFSETS,
        "ignore offsets for DATA from TEXT",
    );

    let ignore_text_analysis_offsets = flag_arg(
        IGNORE_TEXT_ANALYSIS_OFFSETS,
        "ignore offsets for ANALYSIS from TEXT",
    );

    let allow_header_text_offset_mismatch = flag_arg(
        ALLOW_HEADER_TEXT_OFFSET_MISMATCH,
        "allow HEADER and TEXT offsets to be different, in which case HEADER will be used",
    );

    let allow_missing_required_offsets = flag_arg(
        ALLOW_MISSING_REQUIRED_OFFSETS,
        "allow required offsets to be missing from TEXT (3.0/3.1)",
    );

    let truncate_text_offsets = flag_arg(
        TRUNCATE_TEXT_OFFSETS,
        "truncate offsets in TEXT if they exceed end of file",
    );

    let all_offset_args = [
        text_data_correction_begin,
        text_data_correction_end,
        text_analysis_correction_begin,
        text_analysis_correction_end,
        ignore_text_data_offsets,
        ignore_text_analysis_offsets,
        allow_header_text_offset_mismatch,
        allow_missing_required_offsets,
        truncate_text_offsets,
    ];

    // layout args

    let int_widths_from_byteord = flag_arg(
        INT_WIDTHS_FROM_BYTEORD,
        "set $PnB based on length of $BYTEORD; \
         only has effect on integer layouts in 2.0/3.0",
    );

    let int_byteord_override = Arg::new(INT_BYTEORD_OVERRIDE)
        .long(INT_BYTEORD_OVERRIDE)
        .value_name("BYTEORD")
        .help("override the value of $BYTEORD; only has effect on integer layouts in 2.0/3.0");

    let disallow_range_truncation = flag_arg(
        DISALLOW_RANGE_TRUNCATION,
        "throw error if $PnR values need to be truncated to fit in type \
         dictated by $DATATYPE and $PnB.",
    );

    let all_layout_args = [
        int_widths_from_byteord,
        int_byteord_override,
        disallow_range_truncation,
    ];

    // dataset args

    let allow_uneven_event_width = flag_arg(
        ALLOW_UNEVEN_EVENT_WIDTH,
        "allow event width to not evenly divide length of DATA",
    );

    let allow_tot_mismatch = flag_arg(
        ALLOW_TOT_MISMATCH,
        "allow $TOT to mismatch the number of events that are actually in DATA",
    );

    let all_dataset_args = [allow_uneven_event_width, allow_tot_mismatch];

    // shared args

    let warnings_are_errors = flag_arg(WARNINGS_ARE_ERRORS, "treat all warnings as fatal errors");

    let hide_warnings = flag_arg(HIDE_WARNINGS, "hide all warnings");

    let all_shared_args = [warnings_are_errors, hide_warnings];

    // other args

    let delim_arg = Arg::new(DELIM)
        .long(DELIM)
        .short('d')
        .help("delimiter to use for the table")
        .default_value("\t");

    let input_arg = Arg::new(INPUT_PATH)
        .short('i')
        .long(INPUT_PATH)
        .value_parser(value_parser!(PathBuf))
        .help("path to FCS file to parse")
        .required(true);

    let substitution_help = format!(
        "{SUB_SECTION}:\n\n    \
         The SUB part in --{SUB_STD_LIT_KEY_VALS} and --{SUB_STD_PAT_KEY_VALS} \
         is a sed-like pattern which will be used to edit the value of KEY. \
         It must be a string like 's<D><FROM><D><TO>[<D>g]' where 'D' is a \
         delimiter (any character), FROM is a regular expression and TO is a \
         replacement pattern. FROM and TO must follow the syntax outlined in \
         {REGEXP_REF} and {REGEXP_REP_REF} respectively, with the caveat that \
         only bracketed replacement syntax is allowed."
    );

    let cmd = Command::new("fireflow")
        .about("read and write FCS files")
        .arg_required_else_help(true)
        .next_line_help(true)
        .max_term_width(80)
        .subcommand(
            Command::new(SUBCMD_HEADER)
                .about("show header as JSON")
                .arg(&input_arg)
                .args(&all_header_args),
        )
        .subcommand(
            Command::new(SUBCMD_RAW)
                .about("show raw keywords as JSON")
                .arg(&input_arg)
                .args(&all_header_args)
                .args(&all_raw_args)
                .args(&all_shared_args)
                .after_long_help(&substitution_help),
        )
        .subcommand(
            Command::new(SUBCMD_STD)
                .about("dump standardized keywords as JSON")
                .arg(&input_arg)
                .args(&all_header_args)
                .args(&all_raw_args)
                .args(&all_std_args)
                .args(&all_offset_args)
                .args(&all_layout_args)
                .args(&all_shared_args)
                .after_long_help(&substitution_help),
        )
        .subcommand(
            Command::new(SUBCMD_MEAS)
                .about("show a table of standardized measurement values")
                .arg(&input_arg)
                .args(&all_header_args)
                .args(&all_raw_args)
                .args(&all_std_args)
                .args(&all_offset_args)
                .args(&all_layout_args)
                .args(&all_shared_args)
                .arg(&delim_arg)
                .after_long_help(&substitution_help),
        )
        .subcommand(
            Command::new(SUBCMD_SPILL)
                .about("dump the spillover matrix if present")
                .arg(&input_arg)
                .args(&all_header_args)
                .args(&all_raw_args)
                .args(&all_std_args)
                .args(&all_offset_args)
                .args(&all_layout_args)
                .args(&all_shared_args)
                .arg(&delim_arg)
                .after_long_help(&substitution_help),
        )
        .subcommand(
            Command::new(SUBCMD_DATA)
                .about("show a table of the DATA segment")
                .arg(&input_arg)
                .args(&all_header_args)
                .args(&all_raw_args)
                .args(&all_std_args)
                .args(&all_offset_args)
                .args(&all_layout_args)
                .args(&all_dataset_args)
                .args(&all_shared_args)
                .arg(&delim_arg)
                .after_long_help(&substitution_help),
        );

    let args = cmd.get_matches();

    match args.subcommand() {
        Some((SUBCMD_HEADER, sargs)) => {
            let conf = parse_header_config(sargs);
            let filepath = parse_input_path(sargs);
            fcs_read_header(filepath, &conf.into())
                .map(|h| print_json(&h.inner()))
                .map_err(handle_failure_nowarn)
        }

        Some((SUBCMD_RAW, sargs)) => {
            let conf = parse_raw_config(sargs);
            let filepath = parse_input_path(sargs);
            fcs_read_raw_text(filepath, &conf)
                .map(handle_warnings)
                .map(|raw| print_json(&raw))
                .map_err(handle_failure)
        }

        Some((SUBCMD_SPILL, sargs)) => {
            let conf = parse_std_config(sargs);
            let delim = parse_delim(sargs);
            let filepath = parse_input_path(sargs);
            fcs_read_std_text(filepath, &conf)
                .map(handle_warnings)
                .map(|(core, _)| core.print_comp_or_spillover_table(delim))
                .map_err(handle_failure)
        }

        Some((SUBCMD_MEAS, sargs)) => {
            let conf = parse_std_config(sargs);
            let delim = parse_delim(sargs);
            let filepath = parse_input_path(sargs);
            fcs_read_std_text(filepath, &conf)
                .map(handle_warnings)
                .map(|(core, _)| core.print_meas_table(delim))
                .map_err(handle_failure)
        }

        Some((SUBCMD_STD, sargs)) => {
            let conf = parse_std_config(sargs);
            let filepath = parse_input_path(sargs);
            fcs_read_std_text(filepath, &conf)
                .map(handle_warnings)
                .map(|(core, _)| print_json(&core))
                .map_err(handle_failure)
        }

        Some((SUBCMD_DATA, sargs)) => {
            let conf = parse_dataset_config(sargs);
            let delim = parse_delim(sargs);
            let filepath = parse_input_path(sargs);
            fcs_read_std_dataset(filepath, &conf)
                .map(handle_warnings)
                .map(|(core, _)| print_parsed_data(&core, delim))
                .map_err(handle_failure)
        }

        _ => Ok(()),
    }
}

fn parse_header_config(sargs: &ArgMatches) -> config::HeaderConfigInner {
    fn get_correction<I>(am: &ArgMatches, x0: &str, x1: &str) -> HeaderCorrection<I> {
        let y0 = am.get_one(x0).copied();
        let y1 = am.get_one(x1).copied();
        (y0, y1).into()
    }
    let text_correction = get_correction(sargs, TEXT_COR_BEGIN, TEXT_COR_END);
    let data_correction = get_correction(sargs, DATA_COR_BEGIN, DATA_COR_END);
    let analysis_correction = get_correction(sargs, ANALYSIS_COR_BEGIN, ANALYSIS_COR_END);
    let other_width = sargs
        .get_one::<u8>(OTHER_WIDTH)
        .copied()
        .map(|x| x.try_into().unwrap())
        .unwrap_or_default();
    config::HeaderConfigInner {
        text_correction,
        data_correction,
        analysis_correction,
        // don't add other corrections since these aren't used in this api (yet)
        other_corrections: vec![],
        max_other: sargs.get_one::<usize>(MAX_OTHER).copied(),
        other_width,
        squish_offsets: sargs.get_flag(SQUISH_OFFSETS),
        allow_negative: sargs.get_flag(ALLOW_NEGATIVE),
        truncate_offsets: sargs.get_flag(TRUNCATE_OFFSETS),
    }
}

fn parse_header_and_text_config(sargs: &ArgMatches) -> config::ReadHeaderAndTEXTConfig {
    let version_override = sargs
        .get_one::<String>(VERSION_OVERRIDE)
        .map(|s| s.parse::<Version>().unwrap());
    let stext0 = sargs.get_one(SUPP_TEXT_COR_BEGIN).copied();
    let stext1 = sargs.get_one(SUPP_TEXT_COR_END).copied();
    let supp_text_correction = (stext0, stext1).into();

    let to_blank = |s: &str| (s.to_owned(), ());

    let ignore_standard_keys =
        parse_key_or_pat(sargs, IGNORE_STD_LIT_KEY, IGNORE_STD_PAT_KEY, to_blank).unwrap();

    let promote_to_standard =
        parse_key_or_pat(sargs, PROMOTE_LIT_TO_STD, PROMOTE_PAT_TO_STD, to_blank).unwrap();
    let demote_from_standard =
        parse_key_or_pat(sargs, DEMOTE_LIT_FROM_STD, DEMOTE_PAT_FROM_STD, to_blank).unwrap();

    let rename_standard_keys =
        parse_hashmap(sargs, RENAME_STD_KEYS, |s| s.parse::<KeyString>().unwrap())
            .try_into()
            .unwrap();

    let replace_standard_key_values = parse_hashmap(sargs, REPLACE_STD_KEY_VALS, Into::into);
    let append_standard_keywords = parse_hashmap(sargs, APPEND_STD_KEY_VALS, Into::into);

    let to_sub = |s: &str| {
        let (k, v) = s.split_once(',').unwrap();
        (k.to_owned(), parse_sub_pattern(v))
    };

    let substitute_standard_key_values =
        parse_key_or_pat(sargs, SUB_STD_LIT_KEY_VALS, SUB_STD_PAT_KEY_VALS, to_sub).unwrap();

    config::ReadHeaderAndTEXTConfig {
        header: parse_header_config(sargs),
        version_override,
        supp_text_correction,
        allow_duplicated_supp_text: sargs.get_flag(ALLOW_DUP_SUPP_TEXT),
        ignore_supp_text: sargs.get_flag(IGNORE_SUPP_TEXT),
        use_literal_delims: sargs.get_flag(LIT_DELIMS),
        allow_non_ascii_delim: sargs.get_flag(ALLOW_NON_ASCII_DELIM),
        allow_missing_final_delim: sargs.get_flag(ALLOW_MISSING_FINAL_DELIM),
        allow_nonunique: sargs.get_flag(ALLOW_NON_UNIQUE),
        allow_odd: sargs.get_flag(ALLOW_ODD),
        allow_empty: sargs.get_flag(ALLOW_EMPTY),
        allow_delim_at_boundary: sargs.get_flag(ALLOW_DELIM_AT_BOUNDARY),
        allow_non_utf8: sargs.get_flag(ALLOW_NON_UTF8),
        use_latin1: sargs.get_flag(USE_LATIN1),
        allow_non_ascii_keywords: sargs.get_flag(ALLOW_NON_ASCII_KEYWORDS),
        allow_missing_supp_text: sargs.get_flag(ALLOW_MISSING_SUPP_TEXT),
        allow_supp_text_own_delim: sargs.get_flag(ALLOW_SUPP_TEXT_OWN_DELIM),
        allow_missing_nextdata: sargs.get_flag(ALLOW_MISSING_NEXTDATA),
        trim_value_whitespace: sargs.get_flag(TRIM_VALUE_WHITESPACE),
        ignore_standard_keys,
        rename_standard_keys,
        promote_to_standard,
        demote_from_standard,
        replace_standard_key_values,
        append_standard_keywords,
        substitute_standard_key_values,
    }
}

fn parse_std_inner_config(sargs: &ArgMatches) -> config::StdTextReadConfig {
    let time_meas_pattern = if let Some(t) = sargs.get_one::<String>(TIME_MEAS_PATTERN) {
        if t.as_str() == "NoTime" {
            None
        } else {
            Some(t.parse::<config::TimeMeasNamePattern>().unwrap())
        }
    } else {
        Some(config::TimeMeasNamePattern::default())
    };

    let nonstandard_measurement_pattern = sargs
        .get_one::<String>(NS_MEAS_PATTERN)
        .cloned()
        .map(|s| s.parse::<NonStdMeasPattern>().unwrap());
    let ignore_time_optical_keys = sargs
        .get_one::<String>(IGNORE_TIME_OPTICAL_KEYS)
        .into_iter()
        .flat_map(|s| s.split(','))
        .map(|s| s.parse::<config::TemporalOpticalKey>().unwrap())
        .collect();
    let date_pattern = sargs
        .get_one::<String>(DATE_PATTERN)
        .cloned()
        .map(|d| d.parse::<DatePattern>().unwrap());
    let time_pattern = sargs
        .get_one::<String>(TIME_PATTERN)
        .cloned()
        .map(|d| d.parse::<TimePattern>().unwrap());
    config::StdTextReadConfig {
        trim_intra_value_whitespace: sargs.get_flag(TRIM_INTRA_VALUE_WHITESPACE),
        time_meas_pattern,
        force_time_linear: sargs.get_flag(FORCE_TIME_LINEAR),
        ignore_time_gain: sargs.get_flag(IGNORE_TIME_GAIN),
        ignore_time_optical_keys,
        allow_missing_time: sargs.get_flag(ALLOW_MISSING_TIME),
        parse_indexed_spillover: sargs.get_flag(PARSE_INDEXED_SPILLOVER),
        date_pattern,
        time_pattern,
        allow_pseudostandard: sargs.get_flag(ALLOW_PSEUDOSTANDARD),
        allow_unused_standard: sargs.get_flag(ALLOW_UNUSED_STANDARD),
        allow_optional_dropping: sargs.get_flag(ALLOW_OPTIONAL_DROPPING),
        disallow_deprecated: sargs.get_flag(DISALLOW_DEPRECATED),
        fix_log_scale_offsets: sargs.get_flag(FIX_LOG_SCALE_OFFSETS),
        nonstandard_measurement_pattern,
    }
}

fn parse_raw_config(sargs: &ArgMatches) -> config::ReadRawTEXTConfig {
    config::ReadRawTEXTConfig {
        raw: parse_header_and_text_config(sargs),
        shared: parse_shared_config(sargs),
    }
}

fn parse_std_config(sargs: &ArgMatches) -> config::ReadStdTEXTConfig {
    config::ReadStdTEXTConfig {
        raw: parse_header_and_text_config(sargs),
        standard: parse_std_inner_config(sargs),
        offsets: parse_offsets_config(sargs),
        layout: parse_layout_config(sargs),
        shared: parse_shared_config(sargs),
    }
}

fn parse_dataset_config(sargs: &ArgMatches) -> config::ReadStdDatasetConfig {
    config::ReadStdDatasetConfig {
        raw: parse_header_and_text_config(sargs),
        standard: parse_std_inner_config(sargs),
        offsets: parse_offsets_config(sargs),
        layout: parse_layout_config(sargs),
        data: parse_dataset_inner_config(sargs),
        shared: parse_shared_config(sargs),
    }
}

fn parse_offsets_config(sargs: &ArgMatches) -> config::ReadTEXTOffsetsConfig {
    let data_corr0 = sargs.get_one(TEXT_DATA_COR_BEGIN).copied();
    let data_corr1 = sargs.get_one(TEXT_DATA_COR_END).copied();
    let text_data_correction = (data_corr0, data_corr1).into();

    let anal_corr0 = sargs.get_one(TEXT_ANALYSIS_COR_BEGIN).copied();
    let anal_corr1 = sargs.get_one(TEXT_ANALYSIS_COR_END).copied();
    let text_analysis_correction = (anal_corr0, anal_corr1).into();

    config::ReadTEXTOffsetsConfig {
        text_data_correction,
        text_analysis_correction,
        ignore_text_data_offsets: sargs.get_flag(IGNORE_TEXT_DATA_OFFSETS),
        ignore_text_analysis_offsets: sargs.get_flag(IGNORE_TEXT_ANALYSIS_OFFSETS),
        allow_header_text_offset_mismatch: sargs.get_flag(ALLOW_HEADER_TEXT_OFFSET_MISMATCH),
        allow_missing_required_offsets: sargs.get_flag(ALLOW_MISSING_REQUIRED_OFFSETS),
        truncate_text_offsets: sargs.get_flag(TRUNCATE_TEXT_OFFSETS),
    }
}

fn parse_layout_config(sargs: &ArgMatches) -> config::ReadLayoutConfig {
    let integer_byteord_override = sargs
        .get_one::<String>(INT_BYTEORD_OVERRIDE)
        .map(|s| s.parse::<ByteOrd2_0>().unwrap());
    config::ReadLayoutConfig {
        integer_widths_from_byteord: sargs.get_flag(INT_WIDTHS_FROM_BYTEORD),
        integer_byteord_override,
        disallow_range_truncation: sargs.get_flag(DISALLOW_RANGE_TRUNCATION),
    }
}

fn parse_dataset_inner_config(sargs: &ArgMatches) -> config::ReaderConfig {
    config::ReaderConfig {
        allow_tot_mismatch: sargs.get_flag(ALLOW_TOT_MISMATCH),
        allow_uneven_event_width: sargs.get_flag(ALLOW_UNEVEN_EVENT_WIDTH),
    }
}

fn parse_shared_config(sargs: &ArgMatches) -> config::SharedConfig {
    config::SharedConfig {
        warnings_are_errors: sargs.get_flag(WARNINGS_ARE_ERRORS),
        hide_warnings: sargs.get_flag(HIDE_WARNINGS),
    }
}

fn parse_key_or_pat<'a, 'b, 'c, T, F: Fn(&'a str) -> (String, T)>(
    sargs: &'a ArgMatches,
    lit_flag: &'b str,
    pat_flag: &'c str,
    f: F,
) -> Result<KeyOrStringPatterns<T>, KeyOrStringPatternsError> {
    let ignore_std_lit_keys = sargs
        .get_many::<String>(lit_flag)
        .unwrap_or_default()
        .map(|s| f(s.as_str()));
    let ignore_std_pat_keys = sargs
        .get_many::<String>(pat_flag)
        .unwrap_or_default()
        .map(|s| f(s.as_str()));
    KeyOrStringPatterns::try_from_literals_and_patterns(ignore_std_lit_keys, ignore_std_pat_keys)
}

fn parse_hashmap<'a, 'b, T, F: Fn(&'a str) -> T>(
    sargs: &'a ArgMatches,
    flag: &'b str,
    f: F,
) -> HashMap<KeyString, T> {
    sargs
        .get_many::<String>(flag)
        .unwrap_or_default()
        .map(|s| {
            // NOTE we can get away with this because we know that keys in FCS
            // cannot contain commas, and we are only using these as the keys
            // in this particular hash table
            let (k, v) = s.split_once(',').unwrap();
            (k.parse::<KeyString>().unwrap(), f(v))
        })
        .collect::<HashMap<_, _>>()
}

fn parse_sub_pattern(s: &str) -> SubPattern {
    // TODO maybe should handle errors like an adult (eventually)
    let (op, r0) = s
        .split_at_checked(1)
        .expect("sub pattern string must start with 's'");
    assert!(op == "s", "sub pattern string must start with 's'");
    let (delim, r1) = r0
        .split_at_checked(1)
        .expect("sub pattern delimiter is not a valid UTF-8 byte");
    let parts: Vec<_> = r1.split(delim).collect();
    let (from, to, global) = match &parts[..] {
        [x, y] => (*x, *y, false),
        [x, y, "g"] => (*x, *y, true),
        _ => panic!(
            "sub pattern string must be like 's<D><FROM><D><TO>[<D>g]' \
             where 'D' is a delimiter (any character), FROM is a \
             regular expression and TO is a replacement pattern"
        ),
    };
    SubPattern::try_new(Regex::new(from).unwrap(), to.to_owned(), global).unwrap()
}

fn parse_input_path(sargs: &ArgMatches) -> &PathBuf {
    sargs.get_one::<PathBuf>(INPUT_PATH).unwrap()
}

fn parse_delim(sargs: &ArgMatches) -> &String {
    sargs.get_one::<String>(DELIM).unwrap()
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

fn handle_warnings<X, W>(t: Terminal<X, W>) -> X
where
    W: Display,
{
    t.resolve(print_warnings).0
}

fn print_warnings<W>(ws: Vec<W>)
where
    W: Display,
{
    for w in ws {
        eprintln!("WARNING: {w}");
    }
}

fn handle_failure<W, E, T>(f: TerminalFailure<W, E, T>)
where
    E: Display,
    T: Display,
    W: Display,
{
    f.resolve(print_warnings, print_errors);
}

fn handle_failure_nowarn<E, T>(f: TerminalFailure<Infallible, E, T>)
where
    E: Display,
    T: Display,
{
    f.resolve(|_| (), print_errors);
}

fn print_errors<E: Display, T: Display>(es: NonEmpty<E>, r: T) {
    eprintln!("TOPLEVEL ERROR: {r}");
    for e in es {
        eprintln!("  ERROR: {e}");
    }
}

const SUBCMD_HEADER: &str = "header";

const SUBCMD_RAW: &str = "raw";

const SUBCMD_STD: &str = "std";

const SUBCMD_DATA: &str = "data";

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

const ALLOW_DUP_SUPP_TEXT: &str = "allow-duplicated-supp-text";

const IGNORE_SUPP_TEXT: &str = "ignore-supp-text";

const LIT_DELIMS: &str = "use-literal-delims";

const ALLOW_NON_ASCII_DELIM: &str = "allow-non-ascii-delim";

const ALLOW_MISSING_FINAL_DELIM: &str = "allow-missing-final-delim";

const ALLOW_NON_UNIQUE: &str = "allow-non-unique";

const ALLOW_ODD: &str = "allow-odd";

const ALLOW_EMPTY: &str = "allow-empty";

const ALLOW_DELIM_AT_BOUNDARY: &str = "allow-delim-at-boundary";

const ALLOW_NON_UTF8: &str = "allow-non-utf8";

const USE_LATIN1: &str = "use-latin1";

const ALLOW_NON_ASCII_KEYWORDS: &str = "allow-non-ascii-keywords";

const ALLOW_MISSING_SUPP_TEXT: &str = "allow-missing-supp-text";

const ALLOW_SUPP_TEXT_OWN_DELIM: &str = "allow-supp-text-own-delim";

const ALLOW_MISSING_NEXTDATA: &str = "allow-missing-nextdata";

const TRIM_VALUE_WHITESPACE: &str = "trim-value-whitespace";

const IGNORE_STD_LIT_KEY: &str = "ignore-std-literal-key";

const IGNORE_STD_PAT_KEY: &str = "ignore-std-pattern-key";

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

const WARNINGS_ARE_ERRORS: &str = "warnings-are-errors";

const HIDE_WARNINGS: &str = "hide-warnings";

const TRIM_INTRA_VALUE_WHITESPACE: &str = "trim-intra-value-whitespace";

const TIME_MEAS_PATTERN: &str = "time-meas-pattern";

const ALLOW_MISSING_TIME: &str = "allow-missing-time";

const PARSE_INDEXED_SPILLOVER: &str = "parse-indexed-spillover";

const FORCE_TIME_LINEAR: &str = "force-time-linear";

const IGNORE_TIME_GAIN: &str = "ignore-time-gain";

const IGNORE_TIME_OPTICAL_KEYS: &str = "ignore-time-optical-keys";

const ALLOW_PSEUDOSTANDARD: &str = "allow-pseudostandard";

const ALLOW_UNUSED_STANDARD: &str = "allow-unused-standard";

const ALLOW_OPTIONAL_DROPPING: &str = "allow-optional-dropping";

const DISALLOW_DEPRECATED: &str = "disallow-deprecated";

const FIX_LOG_SCALE_OFFSETS: &str = "fix-log-scale-offsets";

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

const ALLOW_TOT_MISMATCH: &str = "allow-tot-mismatch";

const SUB_SECTION: &str = "SUBSTITUTION";

const DELIM: &str = "delimiter";

const INPUT_PATH: &str = "input-path";

const REGEXP_REF: &str = "https://docs.rs/regex/latest/regex/#syntax";

const REGEXP_REP_REF: &str = "https://docs.rs/regex/latest/regex/struct.Regex.html#method.replace";

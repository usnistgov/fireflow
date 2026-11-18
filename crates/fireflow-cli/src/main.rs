use fireflow_core::api;
use fireflow_core::config::{self as cfg, StdKeyValues};
use fireflow_core::core::{AnyCoreDataset, AnyCoreTEXT};
use fireflow_core::selector::{AppendableSelector, Selector};
use fireflow_core::text::keywords::AlphaNumType;
use fireflow_core::validated::read_state::DatasetOffset;

use fireflow_core::segment::read::{HasRegion, HasSource};

use fireflow_types::config::{
    self as tc, ByteordOverride, HasStrategy as _, IntWidthOverride, ParseOpticalOnlyKeysError,
};
use fireflow_types::keys::{AnyKey, StdKey};
use fireflow_types::other_width::{OTHER_WIDTH_DEFAULT, OtherWidth};
use fireflow_types::ranged_float::PositiveFloat;
use fireflow_types::segment as ts;
use fireflow_types::segment::OffsetsCorrection;
use fireflow_types::segment::{HeaderCorrection, OtherSegmentId};
use fireflow_types::std_pattern::{PATTERN_DELIMITER, StdKeyOrPattern, StdKeyPatterns};
use fireflow_types::sub_pattern::SubPattern;
use fireflow_types::textdelim::TEXTDelim;
use fireflow_types::timepattern::TimePattern;
use fireflow_types::{args::dash as ta, config::OpticalOnlyKeys};
use fireflow_types::{config::TriFlag, keywords as tk};
use fireflow_types::{
    config::{PartialTriErrorFlagError, TriErrorFlag},
    keystring_pairs::KeyPairs,
};
use fireflow_types::{datepattern::DatePattern, std_pattern::SubPatterns};
use fireflow_types::{keys::raw_std as sk, std_pattern::StdKeysOrPatterns};

use nonempty::NEString;

use clap::{ArgAction, Args, Parser};
use const_format::{concatcp, formatcp};
use hashbrown::HashMap;
use itertools::Itertools as _;
use itoa::Buffer as IBuf;
use regex::Regex;
use serde::Serialize;
use serde_json::{Value, json, to_writer};
use zmij::Buffer as FBuf;

use std::error::Error;
use std::fmt;
use std::fmt::Display;
use std::io::{self, Write};
use std::iter::once;
use std::path::PathBuf;
use std::process::ExitCode;
use std::str::FromStr;

fn main() -> ExitCode {
    match Cli::run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}

macro_rules! fmt_arg {
    ($s:expr) => {
        formatcp!("{BOLD}--{}{RESET}", $s)
    };
}

macro_rules! fmt_val {
    ($s:expr) => {
        formatcp!("'{}'", $s)
    };
}

macro_rules! fmt_config_val {
    ($s:ident) => {
        formatcp!("'{}'", tc::$s.as_str())
    };
}

#[derive(Parser)]
#[command(name = "fireflow")]
#[command(about = "Read FCS files in standards-compliant manner")]
#[command(arg_required_else_help(true))]
#[command(next_line_help(true))]
#[command(max_term_width(80))]
enum Cli {
    #[command(about = "Show version information.")]
    Version,

    #[command(about = "Show header as JSON.")]
    Header(ReadHeaderArgs),

    #[command(about = "Show flat keywords as JSON.")]
    #[command(after_long_help(FLAT_LONG_HELP))]
    Flat(ReadFlatArgs),

    #[command(about = "Dump standardized keywords as JSON.")]
    #[command(after_long_help(STD_LONG_HELP))]
    Std(ReadStdArgs),

    #[command(about = "Show a table of standardized measurement values.")]
    #[command(after_long_help(STD_LONG_HELP))]
    Measurements(ReadStdTabularArgs),

    #[command(about = "Dump the spillover matrix if present.")]
    #[command(after_long_help(STD_LONG_HELP))]
    Spillover(ReadStdTabularArgs),

    #[command(about = formatcp!("Show a table of the {DATA_SEG} segment."))]
    #[command(after_long_help(STD_LONG_HELP))]
    Data(ReadDatasetArgs),

    #[command(about = "Read a non-compliant FCS file and save as a compliant FCS file.")]
    #[command(after_long_help(STD_LONG_HELP))]
    Repair(RepairFileArgs),

    #[command(about = "Show configuration arguments for a given strategy.")]
    #[command(after_long_help(STD_LONG_HELP))]
    Strategies(StrategyArg),

    #[command(about = "Summarize datasets in FCS file")]
    Summarize(SummarizeArgs),

    #[command(about = "Scan FCS file for dataset boundaries")]
    Scan(ScanArgs),
}

impl Cli {
    fn run() -> AppResult<()> {
        let stdout = io::BufWriter::new(io::stdout().lock());
        let stderr = io::BufWriter::new(io::stderr().lock());

        // NOTE this will call exit if arg parsing fails; it is the "simple" way
        // to return colored output. Using try_parse() requires manually rendering
        // the string to get color.
        match Self::parse() {
            Self::Version => {
                let vi = api::build_info();
                to_writer(stdout, &vi)?;
            }

            Self::Header(sargs) => sargs.run(stdout, stderr)?,

            Self::Flat(sargs) => sargs.run(stdout, stderr)?,

            Self::Std(sargs) => sargs.run(stdout, stderr)?,

            Self::Measurements(sargs) => sargs.run_meas(stdout, stderr)?,

            Self::Spillover(sargs) => sargs.run_spillover(stdout, stderr)?,

            Self::Data(sargs) => sargs.run(stdout, stderr)?,

            Self::Summarize(sargs) => sargs.run(stdout, stderr)?,

            Self::Repair(sargs) => sargs.run(stdout, stderr)?,

            Self::Scan(sargs) => {
                #[derive(Serialize)]
                struct Bounds {
                    version: tk::Version,
                    offset: DatasetOffset,
                }
                let filepath = sargs.input.inner;
                let mmap = sargs.mmap.inner;
                let bounds = api::fcs_scan_dataset_boundaries(&filepath, mmap)?;
                let arr: Vec<_> = bounds
                    .into_iter()
                    .map(|(version, offset)| Bounds { version, offset })
                    .collect();
                to_writer(stdout, &arr)?;
            }

            Self::Strategies(sargs) => {
                let strat = sargs.inner.unwrap_or_default();
                let conf = cfg::ReadStdDatasetConfig::new_with_strategy(strat);
                to_writer(stdout, &conf)?;
            }
        }
        Ok(())
    }
}

#[derive(Args)]
struct ReadHeaderArgs {
    #[command(flatten)]
    input: InputArg,

    #[command(flatten)]
    strategy: StrategyArg,

    #[command(flatten)]
    header: ReadHeaderArgs_,

    #[command(flatten)]
    offset: ReadOffsetArgs_,
}

#[derive(Args)]
struct ReadFlatArgs {
    #[command(flatten)]
    input: InputArg,

    #[command(flatten)]
    strategy: StrategyArg,

    #[command(flatten)]
    dataset_index: DatasetIndexArg,

    #[command(flatten)]
    mmap: MmapArg,

    #[command(flatten)]
    flat: ReadFlatKwsArgs_,
}

#[derive(Args)]
struct ReadStdArgs {
    #[command(flatten)]
    input: InputArg,

    #[command(flatten)]
    strategy: StrategyArg,

    #[command(flatten)]
    dataset_index: DatasetIndexArg,

    #[command(flatten)]
    vendor_heuristics: VendorHeuristicsArg,

    #[command(flatten)]
    mmap: MmapArg,

    #[command(flatten)]
    std: ReadStdKwsArgs_,
}

#[derive(Args)]
struct ReadStdTabularArgs {
    #[command(flatten)]
    input: InputArg,

    #[command(flatten)]
    strategy: StrategyArg,

    #[command(flatten)]
    dataset_index: DatasetIndexArg,

    #[command(flatten)]
    vendor_heuristics: VendorHeuristicsArg,

    #[command(flatten)]
    delim: DelimArg,

    #[command(flatten)]
    mmap: MmapArg,

    #[command(flatten)]
    std: ReadStdKwsArgs_,
}

#[derive(Args)]
struct ReadDatasetArgs {
    #[command(flatten)]
    input: InputArg,

    #[command(flatten)]
    strategy: StrategyArg,

    #[command(flatten)]
    dataset_index: DatasetIndexArg,

    #[command(flatten)]
    vendor_heuristics: VendorHeuristicsArg,

    #[command(flatten)]
    delim: DelimArg,

    #[command(flatten)]
    mmap: MmapArg,

    #[command(flatten)]
    dataset: ReadStdDatasetArgs_,
}

#[derive(Args)]
struct RepairFileArgs {
    #[command(flatten)]
    input: InputArg,

    #[command(flatten)]
    output: OutputArg,

    #[command(flatten)]
    strategy: StrategyArg,

    #[command(flatten)]
    vendor_heuristics: VendorHeuristicsArg,

    #[command(flatten)]
    mmap: MmapArg,

    #[command(flatten)]
    multi: MultiDatasetArgs,

    #[command(flatten)]
    write: WriteArgs,

    #[command(flatten)]
    dataset: ReadStdDatasetArgs_,
}

#[derive(Args)]
struct SummarizeArgs {
    #[command(flatten)]
    input: InputArg,

    #[command(flatten)]
    strategy: StrategyArg,

    #[command(flatten)]
    mmap: MmapArg,

    #[command(flatten)]
    multi: MultiDatasetArgs,

    #[command(flatten)]
    flat: ReadFlatKwsArgs_,

    #[command(flatten)]
    repair: ReadRepairKwsArgs_,

    #[command(flatten)]
    data: ReadDataKwsArgs_,

    #[command(flatten)]
    dataset: ReadDatasetArgsInner_,
}

#[derive(Args)]
struct ScanArgs {
    #[command(flatten)]
    input: InputArg,

    #[command(flatten)]
    mmap: MmapArg,
}

#[derive(Args)]
struct ReadFlatKwsArgs_ {
    #[command(flatten)]
    header: ReadHeaderArgs_,

    #[command(flatten)]
    offset: ReadOffsetArgs_,

    #[command(flatten)]
    flat: ReadFlatKwsArgsInner_,

    #[command(flatten)]
    shared: ReadSharedArgs_,
}

#[derive(Args)]
struct ReadStdKwsArgs_ {
    #[command(flatten)]
    flat: ReadFlatKwsArgs_,

    #[command(flatten)]
    std: ReadStdKwsArgsInner_,

    #[command(flatten)]
    repair: ReadRepairKwsArgs_,

    #[command(flatten)]
    data: ReadDataKwsArgs_,
}

#[derive(Args)]
struct ReadStdDatasetArgs_ {
    #[command(flatten)]
    std: ReadStdKwsArgs_,

    #[command(flatten)]
    dataset: ReadDatasetArgsInner_,
}

#[derive(Args)]
struct ReadHeaderArgs_ {
    #[command(flatten)]
    text_corr: HdrTextCorrArg,

    #[command(flatten)]
    data_corr: HdrDataCorrArg,

    #[command(flatten)]
    anal_corr: HdrAnalCorrArg,

    #[arg(long = ta::OTHER_CORRS)]
    #[arg(name = formatcp!("BEGIN,END[{OFFSET_DELIM}..]"))]
    #[arg(help = format!(
        "Correction for {OTHER_SEG} offsets. \
         Pairs are separated by a comma. \
         Multiple pairs are separated by a {delim}. \
         An empty string represents an empty list.",
        delim = fmt_val!(OFFSET_DELIM)
    ))]
    other_correction: Option<CliOtherOffsetCorrections>,

    #[command(flatten)]
    max_other: MaxOtherArg,

    #[arg(long = ta::OTHER_WIDTH)]
    #[arg(value_name = "WIDTH")]
    #[arg(help = format!("Width of {OTHER_SEG} segments. Defaults to {OTHER_WIDTH_DEFAULT}"))]
    other_width: Option<OtherWidth>,

    #[arg(long = ta::GUESS_OTHER_WIDTH)]
    #[arg(value_name = "LEVEL")]
    #[arg(
        help = formatcp!(
            "Guess the width of {OTHER_SEG} segments. Valid values are {none} \
             (no guessing) or {error}, {warn} or {silent} which will guess and \
             throw an error, warning, or nothing on failure. For {warn} and \
             {silent}, failure will fall back to 8 or whatever was given in {arg}.",
            none = fmt_config_val!(OTHER_WIDTH_NONE_LEVEL),
            error = fmt_config_val!(OTHER_WIDTH_ERROR_LEVEL),
            warn = fmt_config_val!(OTHER_WIDTH_WARN_LEVEL),
            silent = fmt_config_val!(OTHER_WIDTH_SILENT_LEVEL),
            arg = fmt_arg!(ta::OTHER_WIDTH),
        )
    )]
    guess_other_width: Option<tc::GuessOtherWidth>,

    #[command(flatten)]
    squish_offsets: SquishOffsetsArg,
}

#[derive(Args)]
struct ReadOffsetArgs_ {
    #[command(flatten)]
    allow_pseudoempty: AllowPseudoEmptyArg,

    #[arg(long = ta::DATASET_OVERFLOW_LIMIT)]
    #[arg(value_name = "LIMIT")]
    #[arg(help = "Limit by which offsets can be truncated if they exceed end of file.")]
    dataset_overflow_limit: Option<tc::DatasetOverflowLimit>,

    #[arg(long = ta::OVERLAP_CORRECTION_LIMIT)]
    #[arg(value_name = "LIMIT")]
    #[arg(help = "Limit by which ending segment offset can be \
                  truncated if they overlap another offset.")]
    overlap_correction_limit: Option<tc::OverlapCorrectionLimit>,
}

#[derive(Args)]
struct ReadFlatKwsArgsInner_ {
    #[command(flatten)]
    version_override: VersionOverrideArg,

    #[command(flatten)]
    supp_text_correction: SuppTextCorrArg,

    #[arg(long = ta::NEXTDATA_COR)]
    #[arg(value_name = "INT")]
    #[arg(help = formatcp!("Correction for {NEXTDATA}"))]
    nextdata_correction: Option<i32>,

    #[command(flatten)]
    allow_duplicated_supp_text: AllowDuplicatedSuppTextArg,

    #[arg(long = ta::PROCESS_SUPP_TEXT)]
    #[arg(value_name = "LEVEL")]
    #[arg(help = formatcp!(
        "Choose how to process {SUPP_TEXT_SEG}. \
         Set to {asis} to process as normal, {other} to convert \
         it to an {OTHER_SEG}, or {ignore} to ignore entirely",
        asis = fmt_config_val!(PROCESS_SUPP_TEXT_ASIS_LEVEL),
        other = fmt_config_val!(PROCESS_SUPP_TEXT_OTHER_LEVEL),
        ignore = fmt_config_val!(PROCESS_SUPP_TEXT_IGNORE_LEVEL),
    ))]
    process_supp_text: Option<tc::ProcessSuppText>,

    #[arg(long = ta::DELIM_ESCAPE_MODE)]
    #[arg(value_name = "MODE")]
    #[arg(help = formatcp!(
        "Choose how to escape delimiters in {TEXT_SEG}. See {} for details.",
        DELIM_SECTION.header,
    ))]
    delim_escape_mode: Option<tc::DelimEscapeMode>,

    #[command(flatten)]
    allow_non_ascii_delim: AllowNonAsciiDelimArg,

    #[command(flatten)]
    allow_even_delims: AllowEvenDelimsArg,

    #[command(flatten)]
    allow_nonunique: AllowNonUniqueArg,

    #[command(flatten)]
    allow_odd_tokens: AllowOddTokensArg,

    #[command(flatten)]
    allow_empty_keys: AllowEmptyKeysArg,

    #[command(flatten)]
    allow_delim_at_boundary: AllowDelimAtBoundaryArg,

    #[arg(long = ta::USE_ENCODING)]
    #[arg(value_name = "ENC")]
    #[arg(help = formatcp!(
        "Choose how to interpret characters in {TEXT_SEG}. Choose {single}, \
         {utf8}, or {guess} to interpret bytes as IANA ISO/IEC-8859-1 UTF-8, \
         or first as UTF-8 and falling back to IANA ISO/IEC-8859-1 if a \
         non-UTF-8 byte is found.",
        single = fmt_config_val!(ENCODING_SINGLE_LEVEL),
        utf8 = fmt_config_val!(ENCODING_UTF8_LEVEL),
        guess = fmt_config_val!(ENCODING_GUESS_LEVEL)
    ))]
    use_encoding: Option<tc::UseEncoding>,

    #[command(flatten)]
    allow_non_ascii_keys: AllowNonAsciiKeysArg,

    #[command(flatten)]
    allow_non_utf8_values: AllowNonUtf8ValuesArg,

    #[command(flatten)]
    allow_missing_supp_text: AllowMissingSuppTextArg,

    #[command(flatten)]
    allow_supp_text_own_delim: AllowSuppTextOwnDelimArg,

    #[command(flatten)]
    allow_missing_nextdata: AllowMissingNextdataArg,

    #[arg(long = ta::TRIM_VALUE_WHITESPACE)]
    #[arg(value_name = "LEVEL")]
    #[arg(help = formatcp!(
        "Trim whitespace from beginning and end of all values. This may \
         create blank values if the starting string is entirely whitespace. \
         Set to {none} to not trim at all (default). Set to {error}, {warn}, \
         or {silent} to enable trimming and throw error, warning, or nothing \
         when trimming results in a blank.",
        none = fmt_config_val!(TRIM_NONE_LEVEL),
        error = fmt_config_val!(TRIM_ERROR_LEVEL),
        warn = fmt_config_val!(TRIM_BLANK_WARN_LEVEL),
        silent = fmt_config_val!(TRIM_BLANK_SILENT_LEVEL),
    ))]
    trim_value_whitespace: Option<tc::TrimValueWhitespace>,
}

#[derive(Args)]
struct ReadStdKwsArgsInner_ {
    #[command(flatten)]
    dedup_meas_names: DedupMeasNamesArg,

    #[command(flatten)]
    trim_intra_value_whitespace: TrimIntraValueWhitespaceArg,

    #[arg(long = ta::TIME_MEAS_PATTERN)]
    #[arg(value_name = "REGEXP")]
    #[arg(help = formatcp!(
        "Use REGEXP when matching time measurement (defaults to {default}). \
         Set to blank string to not look for a time measurement.",
        default = fmt_val!(tc::TIME_MEAS_NAME_PATTERN_DEFAULT),
    ))]
    time_meas_pattern: Option<tc::TimeMeasNamePattern>,

    #[command(flatten)]
    allow_missing_time: AllowMissingTimeArg,

    #[command(flatten)]
    add_missing_timestep: AddMissingTimestepArg,

    #[arg(long = ta::FORCE_LINEAR_SCALE)]
    #[arg(value_name = "WHICH")]
    #[arg(help = formatcp!(
        "Force {PN_E} keywords to be linear. Pass {time} to only set the \
         temporal measurement, {non_int} to set temporal measurements and \
         non-integer measurements, {all} to set all measurements, and {none} \
         for no measurements. Affected columns will never fail.",
        time = fmt_config_val!(FORCE_LINEAR_TIME_LEVEL),
        non_int = fmt_config_val!(FORCE_LINEAR_NON_INT_LEVEL),
        all = fmt_config_val!(FORCE_LINEAR_ALL_LEVEL),
        none = fmt_config_val!(FORCE_LINEAR_NONE_LEVEL),
    ))]
    force_linear_scale: Option<tc::ForceLinearScale>,

    #[arg(long = ta::IGNORE_OPTICAL_ONLY_KEYS)]
    #[arg(value_name = "SYMS")]
    #[arg(help = formatcp!(
        "Ignore optical keyword for temporal measurement. Must be a \
         comma-separated list of strings like the X in {PN_ANY}.",
    ))]
    ignore_time_optical_keys: Option<CliOpticalOnlyKeys>,

    #[command(flatten)]
    process_pseudostandard: ProcessPseudostandardArg,

    #[arg(long = ta::PROCESS_OPTICAL_ONLY_KEYS)]
    #[arg(value_name = "LEVEL")]
    #[arg(help = formatcp!(
        "Choose how to handle optical keys found in temporal measurements. \
         Does nothing unless keys are specified with {arg}. Pass \
         {demote_warn}, {demote_silent}, {drop_warn}, or {drop_silent} to \
         demote found keys to nonstandard (with or without warning) or drop \
         keys entirely (with or without warning) respectively.",
        arg = fmt_arg!(ta::IGNORE_OPTICAL_ONLY_KEYS),
        demote_warn = fmt_config_val!(TMP_OPT_DEMOTE_WARN_LEVEL),
        demote_silent = fmt_config_val!(TMP_OPT_DEMOTE_SILENT_LEVEL),
        drop_warn = fmt_config_val!(TMP_OPT_DROP_WARN_LEVEL),
        drop_silent = fmt_config_val!(TMP_OPT_DROP_SILENT_LEVEL),
    ))]
    process_optical_only_keys: Option<tc::ProcessOpticalOnlyKeys>,

    #[arg(long = ta::SPILLOVER_MEASUREMENT_MODE)]
    #[arg(value_name = "MODE")]
    #[arg(help = formatcp!(
        "Choose how to interpret measurement strings in {SPILLOVER}. Set to \
         {named} to interpret as names which link to {PN_N}. Set to \
         {indexed} to interpret as 1-indices which point to measurements. \
         Set to {guess} to automatically choose the prior two modes.",
        named = fmt_config_val!(SPILLOVER_NAMED_LEVEL),
        indexed = fmt_config_val!(SPILLOVER_INDEXED_LEVEL),
        guess = fmt_config_val!(SPILLOVER_GUESS_LEVEL)
    ))]
    spillover_measurement_mode: Option<tc::SpilloverMeasurementMode>,

    #[command(flatten)]
    date_pattern: DatePatternArg,

    #[command(flatten)]
    time_pattern: TimePatternArg,

    #[command(flatten)]
    datetime_pattern: DatetimePatternArg,

    #[command(flatten)]
    last_modified_pattern: LastModifiedPatternArg,

    #[command(flatten)]
    allow_other_feature: AllowOtherFeatureArg,

    #[command(flatten)]
    process_hyper_par: ProcessHyperParArg,

    #[command(flatten)]
    process_other_version: ProcessOtherVersionArg,

    #[command(flatten)]
    process_extra_timestep: ProcessExtraTimestepArg,

    #[command(flatten)]
    fix_log_scale_offset: FixLogScaleOffsetArg,

    #[command(flatten)]
    disallow_localtime: DisallowLocaltimeArg,
}

#[derive(Args)]
struct ReadRepairKwsArgs_ {
    #[command(flatten)]
    ignore_std_keys: IgnoreStdKeysArg,

    #[command(flatten)]
    promote_nonstandard_keys: PromoteNonStdKeysArg,

    #[command(flatten)]
    demote_standard_keys: DemoteStdKeysArg,

    #[arg(long = ta::RENAME_STD_KEYS)]
    #[arg(value_name = "<#>OLD<#>NEW[<#>OLD<#>NEW]<#>")]
    #[arg(help = "Rename standard keys from OLD to NEW. First character is \
                  considered to be the delimiter (<#>) for separating OLD and \
                  NEW as well as multiple pairs of OLD/NEW. An empty list or \
                  single delimiter encodes an empty list. Delimiters must be \
                  ASCII characters. If duplicate OLD keys are found, only the \
                  last is kept.")]
    rename_standard_keys: Option<CliKeyPairs>,

    #[arg(long = ta::REPLACE_STD_KEY_VALS)]
    #[arg(value_name = "<#>KEY<#>VAL[<#>KEY<#>VAL..]<#>")]
    #[arg(help = "Replace values of standard keys matching KEY with VAl. \
                  First character is considered to be the delimiter (<#>) for \
                  separating KEY and VAL as well as multiple pairs of KEY/VAL. \
                  An empty list or single delimiter encodes an empty list. \
                  Delimiters must be ASCII characters. \
                  If duplicate keys are found, only the last is kept.")]
    replace_std_key_vals: Option<CliStdKeyValues>,

    #[arg(long = ta::APPEND_STD_KEYWORDS)]
    #[arg(value_name = "<#>KEY<#>VAL[<#>KEY<#>VAL..]<#>")]
    #[arg(help = "Append standard keys with KEY and VAL to list of existing \
                  standard keys. First character is considered to be the \
                  delimiter (<#>) for separating KEY and VAL as well as multiple \
                  pairs of KEY/VAL. An empty list or single delimiter encodes an \
                  empty list. Delimiters must be ASCII characters. If duplicate \
                  keys are found, only the last is kept.")]
    append_std_keywords: Option<CliStdKeyValues>,

    #[arg(long = ta::SUB_STD_KEY_VALS)]
    #[arg(value_name = "<#>KEY<#>SUB[<#>KEY<#>SUB..]<#>")]
    #[arg(help = formatcp!(
            "Edit standard key values using KEY and SUB. See {} for details. \
             First character is considered to be the delimiter (<#>) for \
             separating KEY and SUB as well as multiple pairs of KEY/SUB. \
             An empty list or single delimiter encodes an empty list. \
             Delimiters must be ASCII characters. \
             If duplicate keys are found, only the last is kept.",
            SUB_SECTION.header,
    ))]
    substitute_standard_key_vals: Option<CliSubPatterns>,

    #[arg(long = ta::PROMOTE_ALL_TO_STD)]
    #[arg(action = ArgAction::SetTrue)]
    #[arg(help = "Promote all non-standard keys to standard where possible.")]
    promote_all_nonstandard_keys: Option<bool>,

    #[command(flatten)]
    allow_repair_non_unique: AllowRepairNonUniqueArg,
}

#[derive(Args)]
struct ReadDataKwsArgs_ {
    #[command(flatten)]
    text_data_correction: TextDataCorrArg,

    #[command(flatten)]
    text_analysis_correction: TextAnalCorrArg,

    #[command(flatten)]
    ignore_text_data_offsets: IgnoreTextDataOffsetsArg,

    #[command(flatten)]
    ignore_text_analysis_offsets: IgnoreTextAnalysisOffsetsArg,

    #[arg(long = ta::ALLOW_HEADER_TEXT_OFFSET_MISMATCH)]
    #[arg(value_name = "LEVEL")]
    #[arg(help = formatcp!(
        "Allow {HEADER_SEG} and {TEXT_SEG} offsets to be different. If \
         {hdr_warn} or {hdr_silent}, choose {HEADER_SEG} and throw a warning \
         or nothing on mismatch. If {text_warn} or {text_silent} behave \
         analogously for {TEXT_SEG}. If {error} (default) throw error.",
        hdr_warn = fmt_config_val!(MISMATCH_HEADER_WARN_LEVEL),
        hdr_silent = fmt_config_val!(MISMATCH_HEADER_SILENT_LEVEL),
        text_warn = fmt_config_val!(MISMATCH_TEXT_WARN_LEVEL),
        text_silent = fmt_config_val!(MISMATCH_TEXT_SILENT_LEVEL),
        error = fmt_config_val!(MISMATCH_ERROR_LEVEL),
    ))]
    allow_header_text_offset_mismatch: Option<tc::AllowHeaderTEXTOffsetMismatch>,

    #[command(flatten)]
    allow_missing_required_offsets: AllowMissingRequiredOffsetsArg,

    #[command(flatten)]
    process_optional_failure: ProcessOptionalFailureArg,

    #[arg(long = ta::INT_WIDTH_OVERRIDE)]
    #[arg(value_name = "INT_OR_FLAG")]
    #[arg(help = formatcp!(
        "Fix {PN_B}. Only has effect on integer layouts in FCS 2.0/3.0. \
         Set to {} or {} to round up to next multiple of 8 or do nothing. \
         Set to an integer 1-8 to override all {PN_B} explicitly.",
        fmt_config_val!(FIX_INT_WIDTH_NEXT_BYTE_LEVEL),
        fmt_config_val!(FIX_INT_WIDTH_NEVER_LEVEL)
    ))]
    int_width_override: Option<IntWidthOverride>,

    #[arg(long = ta::BYTEORD_OVERRIDE)]
    #[arg(value_name = "BYTEORD")]
    #[arg(help = formatcp!(
        "Override the value of {BYTEORD}. Set to {} or {} to do nothing \
         or interpret {BYTEORD} based on its endian-ness (ie without its \
         length). Set to an explicit comma-separated integer sequence to \
         set {BYTEORD} directly.",
        fmt_config_val!(BYTEORD_OVERRIDE_NONE_LEVEL),
        fmt_config_val!(BYTEORD_OVERRIDE_ENDIAN_LEVEL)
    ))]
    byteord_override: Option<ByteordOverride>,

    #[command(flatten)]
    disallow_range_truncation: DisallowRangeTruncationArg,
}

const OVER_LIMIT_HELP_COMMON: &str = formatcp!(
    "Pass {error} to emit error, {warn} to emit warning, {silent} to do \
     nothing, {trunc_warn} to truncate and emit warning, and \
     {trunc_silent} to truncate with no warning.",
    error = fmt_config_val!(OVER_LIMIT_ACTION_ERROR_LEVEL),
    warn = fmt_config_val!(OVER_LIMIT_ACTION_WARN_LEVEL),
    silent = fmt_config_val!(OVER_LIMIT_ACTION_SILENT_LEVEL),
    trunc_warn = fmt_config_val!(OVER_LIMIT_ACTION_TRUNCATE_WARN_LEVEL),
    trunc_silent = fmt_config_val!(OVER_LIMIT_ACTION_TRUNCATE_SILENT_LEVEL)
);

#[derive(Args)]
struct ReadDatasetArgsInner_ {
    #[arg(long = ta::DATA_REMAINDER_LIMIT)]
    #[arg(value_name = "LIMIT")]
    #[arg(help = formatcp!(
        "Limit by which ending {DATA_SEG} offset can be truncated if \
         its length modulo event width produces a remainder."
    ))]
    data_remainder_limit: Option<tc::DataRemainderLimit>,

    #[command(flatten)]
    allow_uneven_event_width: AllowUnevenEventWidthArg,

    #[command(flatten)]
    allow_tot_mismatch: AllowTotMismatchArg,

    #[arg(long = ta::OVER_BITMASK_ACTION)]
    #[arg(value_name = "WHICH")]
    #[arg(help = concatcp!(
        "Choose how to handle integer values in {DATA_SEG} to exceed bitmask. ",
        OVER_LIMIT_HELP_COMMON
    ))]
    over_bitmask_action: Option<tc::OverBitmaskAction>,

    #[arg(long = ta::OVER_RANGE_ACTION)]
    #[arg(value_name = "WHICH")]
    #[arg(help = concatcp!(
        "Choose how to handle values in {DATA_SEG} to exceed {PN_R}. ",
        OVER_LIMIT_HELP_COMMON
    ))]
    over_range_action: Option<tc::OverRangeAction>,

    #[command(flatten)]
    allow_missing_crc: AllowMissingCRCArg,

    #[command(flatten)]
    allow_mismatch_crc: AllowMismatchCRCArg,

    #[arg(long = ta::COMPUTE_CRC)]
    #[arg(value_name = "LEVEL")]
    #[arg(help = formatcp!(
        "When to compute the CRC of the dataset. \
         Pass {} to never compute, pass {} to always compute, and pass {} \
         to compute only when the CRC word is available.",
        tc::COMPUTE_CRC_NEVER_LEVEL.as_str(),
        tc::COMPUTE_CRC_ALWAYS_LEVEL.as_str(),
        tc::COMPUTE_CRC_TEST_LEVEL.as_str(),
    ))]
    compute_crc: Option<tc::ComputeCRC>,

    #[arg(long = ta::ROW_BUFFER_SIZE)]
    #[arg(value_name = "BYTES")]
    #[arg(help = formatcp!(
        "Set the size in bytes for the internal buffer used to read {DATA_SEG}. \
         This is a performance parameter that balances read syscalls (too low) \
         and cache misses (too high). It should generally be 90% of the CPU's \
         L1D cache size. Defaults to {}.",
        fmt_val!(tc::DEFAULT_ROW_BUFFER_SIZE)
    ))]
    row_buffer_size: Option<tc::RowBufferSize>,
}

#[derive(Args)]
struct ReadSharedArgs_ {
    #[arg(long = ta::WARNINGS_ARE_ERRORS)]
    #[arg(action = ArgAction::SetTrue)]
    #[arg(help = "Treat all warnings as fatal errors.")]
    warnings_are_errors: bool,

    #[arg(long = ta::HIDE_WARNINGS)]
    #[arg(action = ArgAction::SetTrue)]
    #[arg(help = "Hide all warnings.")]
    hide_warnings: bool,
}

#[derive(Args)]
struct InputArg {
    #[arg(id = "input")]
    #[arg(short = 'i')]
    #[arg(long = "input")]
    #[arg(value_name = "PATH")]
    #[arg(help = "Path to FCS file to parse.")]
    inner: PathBuf,
}

#[derive(Args)]
struct OutputArg {
    #[arg(id = "output")]
    #[arg(short = 'o')]
    #[arg(long = "output")]
    #[arg(value_name = "PATH")]
    #[arg(help = "Path to FCS file to write.")]
    inner: PathBuf,
}

#[derive(Args)]
struct StrategyArg {
    #[arg(id = "strategy")]
    #[arg(short = 's')]
    #[arg(long = "strategy")]
    #[arg(value_name = "WHICH")]
    #[arg(
        help = formatcp!(
            "Overall strategy to use when parsing file. This will enable many \
             options by default which can be overridden. Set to {scalpel} to \
             parse non-compliant files while attempting to preserve metadata. \
             Set to {sledge} to parse non-compliant files while dropping \
             non-compliant metadata that cannot be fixed. Set to {strict} to \
             enforce the standard (default).",
            scalpel = fmt_config_val!(READ_STRATEGY_SCALPEL_LEVEL),
            sledge = fmt_config_val!(READ_STRATEGY_SLEDGEHAMMER_LEVEL),
            strict = fmt_config_val!(READ_STRATEGY_STRICT_LEVEL),
        )
    )]
    inner: Option<tc::ReadStrategy>,
}

#[derive(Args)]
struct DatasetIndexArg {
    #[arg(id = "dataset-index")]
    #[arg(short = 'I')]
    #[arg(long = "dataset-index")]
    #[arg(value_name = "INDEX")]
    #[arg(help = "Index of the dataset to parse (starting from 0).")]
    inner: Option<usize>,
}

#[derive(Args)]
struct VendorHeuristicsArg {
    #[arg(id = "vendor-heuristics")]
    #[arg(short = 'H')]
    #[arg(long = "vendor-heuristics")]
    #[arg(action = ArgAction::SetTrue)]
    #[arg(help = "Enable vendor-specific heuristics when parsing files.")]
    inner: bool,
}

#[derive(Args)]
struct DelimArg {
    #[arg(id = "delim")]
    #[arg(short = 'd')]
    #[arg(long = "delim")]
    #[arg(value_name = "DELIM")]
    #[arg(help = "Delimiter to use for tabular output. If not provided, defaults to tab.")]
    inner: Option<CliDelim>,
}

#[derive(Args)]
struct MmapArg {
    #[arg(id = "mmap")]
    #[arg(short = 'm')]
    #[arg(long = "mmap")]
    #[arg(action = ArgAction::SetTrue)]
    #[arg(
        help = "Memory-map the file while reading. This is likely to be faster. \
                The user must ensure that the file is not modified while being \
                read, otherwise the mapping will be invalid and terrible things \
                will happen."
    )]
    inner: bool,
}

#[derive(Args)]
struct MultiDatasetArgs {
    #[arg(long = "skip")]
    #[arg(value_name = "INT")]
    #[arg(help = "Number of datasets to skip.")]
    skip: Option<usize>,

    #[arg(long = "limit")]
    #[arg(value_name = "INT")]
    #[arg(help = "Number of datasets to return.")]
    limit: Option<usize>,

    #[arg(long = "scan")]
    #[arg(action = ArgAction::SetTrue)]
    #[arg(help = formatcp!(
        "If given, scan for the next dataset by looking for version \
         tags rather then relying on {NEXTDATA}"
    ))]
    scan: Option<bool>,
}

#[derive(Args)]
struct WriteArgs {
    #[arg(long = "write-delim")]
    #[arg(value_name = "CHAR")]
    #[arg(help = formatcp!(
        "The delimiter to use when writing {TEXT_SEG} to file. \
         Must be an ASCII character 1-31. Defaults to 30 (record separator)."
    ))]
    write_delim: Option<TEXTDelim>,

    #[arg(long = "big-other")]
    #[arg(action = ArgAction::SetTrue)]
    #[arg(help = formatcp!("If set, use 20 for {OTHER_SEG} offset width, otherwise 8."))]
    big_other: Option<bool>,
}

const KEY_STR_OR_PAT_HELP_COMMON: &str = formatcp!(
    "If first and last character are the same, they are treated as \
     delimiters (<#>) which can be used to separate multiple values. \
     If a single value starts and ends with the same value, encapsulate \
     this with two delimiters. Delimiters must be ASCII characters. Values \
     that start and end with {delim} will be interpreted as a regexp-like \
     pattern where '[n-m]' matches indices from 'n' to 'm' (inclusive to \
     exclusive) in such cases {delim} is not interpreted as a delimiter. \
     An empty string encodes an empty list of values.",
    delim = fmt_val!(PATTERN_DELIMITER)
);

macro_rules! key_str_or_pat_arg {
    ($t:ident, $long:expr, $has_pre:expr, $help:expr) => {
        #[derive(Args)]
        struct $t {
            #[arg(id = $long)]
            #[arg(long = $long)]
            #[arg(value_name = "<#>KEY_OR_PAT[<#>KEY_OR_PAT..]<#>")]
            #[arg(help = concatcp!($help, " ", KEY_STR_OR_PAT_HELP_COMMON))]
            inner: Option<CliStdKeyPatterns<$has_pre>>,
        }
    };
}

key_str_or_pat_arg!(
    IgnoreStdKeysArg,
    ta::IGNORE_STD_KEYS,
    true,
    "Ignore standard keys exactly matching KEY_OR_PAT."
);

key_str_or_pat_arg!(
    PromoteNonStdKeysArg,
    ta::PROMOTE_TO_STD,
    false,
    "Promote non-standard keys matching KEY_OR_PAT to standard."
);

key_str_or_pat_arg!(
    DemoteStdKeysArg,
    ta::DEMOTE_FROM_STD,
    true,
    "Demote standard keys matching KEY_OR_PAT to non-standard."
);

macro_rules! corr_arg {
    ($t:ident, $long:expr, $seg:path, $src:path) => {
        #[derive(Args)]
        struct $t {
            #[arg(id = $long)]
            #[arg(long = $long)]
            #[arg(value_name = "BEGIN,END")]
            #[arg(help = format!("Correction for {seg} offsets from {src}.", seg = <$seg as HasRegion>::REGION, src = <$src as HasSource>::SRC))]
            inner: Option<StrOffsetsCorrection<$seg, $src>>,
        }
    };
}

corr_arg!(
    HdrTextCorrArg,
    ta::TEXT_CORR,
    ts::PrimaryTextSegmentId,
    ts::OffsetsFromHeader
);

corr_arg!(
    HdrDataCorrArg,
    ta::DATA_CORR,
    ts::DataSegmentId,
    ts::OffsetsFromHeader
);

corr_arg!(
    HdrAnalCorrArg,
    ta::ANALYSIS_CORR,
    ts::AnalysisSegmentId,
    ts::OffsetsFromHeader
);

corr_arg!(
    SuppTextCorrArg,
    ta::SUPP_TEXT_COR,
    ts::SupplementalTextSegmentId,
    ts::OffsetsFromTEXT
);

corr_arg!(
    TextDataCorrArg,
    ta::TEXT_DATA_CORR,
    ts::DataSegmentId,
    ts::OffsetsFromTEXT
);

corr_arg!(
    TextAnalCorrArg,
    ta::TEXT_ANALYSIS_CORR,
    ts::AnalysisSegmentId,
    ts::OffsetsFromTEXT
);

macro_rules! ne_nullable_arg {
    ($t:ident, $long:expr, $valuename:expr, $inner:path, $help:expr) => {
        #[derive(Args)]
        struct $t {
            #[arg(id = $long)]
            #[arg(long = $long)]
            #[arg(value_name = $valuename)]
            #[arg(help = concatcp!($help, " Set to blank string to supply no value."))]
            inner: Option<NENullable<$inner>>,
        }
    };
}

ne_nullable_arg!(
    DatePatternArg,
    ta::DATE_PATTERN,
    "PATTERN",
    DatePattern,
    formatcp!(
        "Pattern to match {DATE} keyword. See {}.",
        DATE_SECTION.header
    )
);

ne_nullable_arg!(
    TimePatternArg,
    ta::TIME_PATTERN,
    "PATTERN",
    TimePattern,
    formatcp!(
        "Pattern to match {BTIM}/{ETIM} keywords. See {}.",
        TIME_SECTION.header
    )
);

ne_nullable_arg!(
    DatetimePatternArg,
    ta::DATETIME_PATTERN,
    "PATTERN",
    String,
    formatcp!(
        "If supplied, will be used as an alternative pattern when parsing \
         {BEGINDATETIME} and {ENDDATETIME}. It should follow the format \
         outline in {CHRONO_REF}.",
    )
);

ne_nullable_arg!(
    LastModifiedPatternArg,
    ta::LAST_MODIFIED_PATTERN,
    "PATTERN",
    String,
    formatcp!(
        "If supplied, will be used as an alternative pattern when parsing \
         {LAST_MODIFIED}. It should follow the format outline in {CHRONO_REF}.",
    )
);

macro_rules! nullable_arg {
    ($t:ident, $long:expr, $valuename:expr, $inner:path, $help:expr) => {
        #[derive(Args)]
        struct $t {
            #[arg(id = $long)]
            #[arg(long = $long)]
            #[arg(value_name = $valuename)]
            #[arg(help = concatcp!($help, " Set to {none} to supply no value."))]
            inner: Option<Nullable<$inner>>,
        }
    };
}

nullable_arg!(
    VersionOverrideArg,
    ta::VERSION_OVERRIDE,
    "OVERRIDE",
    tc::VersionOverride,
    formatcp!(
        "Override the FCS version from {HEADER_SEG}. Can be an FCS version \
         string (like 'FCS3.2') which will force to a fixed version. Can \
         also autodetect version with one of {latest} or {earliest} (the \
         latest or earliest available version respectively) or {loose} or \
         {strict} (the available version with the most or least optional \
         keywords respectively). Append \"current_or\" to any of these \
         to prioritize the current version before ranking others.",
        latest = fmt_config_val!(VERSION_LATEST_LEVEL),
        earliest = fmt_config_val!(VERSION_EARLIEST_LEVEL),
        loose = fmt_config_val!(VERSION_LOOSE_LEVEL),
        strict = fmt_config_val!(VERSION_STRICT_LEVEL),
    )
);

nullable_arg!(
    MaxOtherArg,
    ta::MAX_OTHER,
    "BYTES",
    usize,
    formatcp!("Max number of {OTHER_SEG} segments to parse.")
);

nullable_arg!(
    AddMissingTimestepArg,
    ta::ADD_MISSING_TIMESTEP,
    "TIMESTEP",
    PositiveFloat,
    formatcp!("Add {TIMESTEP} if it is missing.")
);

const PROC_KW_FAIL_HELP_COMMON: &str = formatcp!(
    "Must be one of {error}, {demote_warn}, {demote_silent}, \
     {drop_warn}, or {drop_silent} which will throw an error, demote to \
     non-standard with warning, demote to non-standard silently, drop with \
     warning, or drop silently respectively",
    error = fmt_config_val!(KW_ERROR_LEVEL),
    demote_warn = fmt_config_val!(KW_DEMOTE_WARN_LEVEL),
    demote_silent = fmt_config_val!(KW_DEMOTE_SILENT_LEVEL),
    drop_warn = fmt_config_val!(KW_DROP_WARN_LEVEL),
    drop_silent = fmt_config_val!(KW_DROP_SILENT_LEVEL)
);

macro_rules! proc_kw_fail_arg {
    ($t:ident, $long:expr, $inner:path, $help:expr) => {
        #[derive(Args)]
        struct $t {
            #[arg(id = $long)]
            #[arg(long = $long)]
            #[arg(value_name = "LEVEL")]
            #[arg(help = concatcp!($help, " ", PROC_KW_FAIL_HELP_COMMON))]
            inner: Option<$inner>,
        }
    };
}

proc_kw_fail_arg!(
    ProcessPseudostandardArg,
    ta::PROCESS_PSEUDOSTANDARD,
    tc::ProcessPseudostandard,
    "Process non-standard keywords that start with a '$'."
);

proc_kw_fail_arg!(
    ProcessHyperParArg,
    ta::PROCESS_HYPER_PAR,
    tc::ProcessHyperPar,
    formatcp!("Process measurement keywords whose index is greater than {PAR}.")
);

proc_kw_fail_arg!(
    ProcessOtherVersionArg,
    ta::PROCESS_OTHER_VERSION,
    tc::ProcessOtherVersion,
    "Process standard keywords from different FCS version."
);

proc_kw_fail_arg!(
    ProcessExtraTimestepArg,
    ta::PROCESS_EXTRA_TIMESTEP,
    tc::ProcessExtraTimestep,
    formatcp!(
        "Process unused {TIMESTEP}, which may indicate that a time measurement \
         is present but not identified."
    )
);

proc_kw_fail_arg!(
    ProcessOptionalFailureArg,
    ta::PROCESS_OPTIONAL_FAILURE,
    tc::ProcessOptionalFailure,
    "Process optional keys if they cause an error."
);

macro_rules! tri_flag_arg {
    ($t:ident, $long:expr, $inner:path, $help:expr) => {
        #[derive(Args)]
        struct $t {
            #[arg(id = $long)]
            #[arg(long = $long)]
            #[arg(value_name = "LEVEL")]
            #[arg(help = tri_flag_help::<$inner>($help))]
            inner: Option<CliTriFlag<$inner>>,
        }
    };
}

tri_flag_arg!(
    AllowDuplicatedSuppTextArg,
    ta::ALLOW_DUPLICATED_SUPP_TEXT,
    tc::AllowDuplicatedSuppTEXT,
    formatcp!(
        "Allow {SUPP_TEXT_SEG} offsets to overlap those for \
         {PRIM_TEXT_SEG} or the boundaries of {HEADER_SEG}."
    )
);

tri_flag_arg!(
    AllowNonAsciiDelimArg,
    ta::ALLOW_NON_ASCII_DELIM,
    tc::AllowNonAsciiDelim,
    formatcp!("Allow {TEXT_SEG} delimiter to be non-ASCII character.")
);

tri_flag_arg!(
    AllowEvenDelimsArg,
    ta::ALLOW_EVEN_DELIMS,
    tc::AllowEvenDelims,
    formatcp!("Allow final {TEXT_SEG} delimiter to be missing.")
);

tri_flag_arg!(
    AllowNonUniqueArg,
    ta::ALLOW_NONUNIQUE,
    tc::AllowNonunique,
    formatcp!("Allow non-unique keys to exist in {TEXT_SEG}.")
);

tri_flag_arg!(
    AllowOddTokensArg,
    ta::ALLOW_ODD_TOKENS,
    tc::AllowOddTokens,
    "Allow odd number of tokens."
);

tri_flag_arg!(
    AllowEmptyKeysArg,
    ta::ALLOW_EMPTY_KEYS,
    tc::AllowEmptyKeys,
    "Allow keys to be blank (relatively rare)."
);

tri_flag_arg!(
    AllowDelimAtBoundaryArg,
    ta::ALLOW_DELIM_AT_BOUNDARY,
    tc::AllowDelimAtBoundary,
    formatcp!("Allow {TEXT_SEG} delimiter(s) to be at token boundaries.")
);

tri_flag_arg!(
    AllowNonAsciiKeysArg,
    ta::ALLOW_NON_ASCII_KEYS,
    tc::AllowNonAsciiKeywords,
    "Allow non-ASCII characters in keys."
);

tri_flag_arg!(
    AllowNonUtf8ValuesArg,
    ta::ALLOW_NON_UTF8_VALUES,
    tc::AllowNonUtf8,
    formatcp!("Allow non-UTF8 characters in {TEXT_SEG} segment.")
);

tri_flag_arg!(
    AllowMissingSuppTextArg,
    ta::ALLOW_MISSING_SUPP_TEXT,
    tc::AllowMissingSuppTEXT,
    formatcp!("Allow {SUPP_TEXT_SEG} offsets to be missing.")
);

tri_flag_arg!(
    AllowSuppTextOwnDelimArg,
    ta::ALLOW_SUPP_TEXT_OWN_DELIM,
    tc::AllowSuppTEXTOwnDelim,
    formatcp!("Allow delimiters in {PRIM_TEXT_SEG} and {SUPP_TEXT_SEG} to differ.")
);

tri_flag_arg!(
    AllowMissingNextdataArg,
    ta::ALLOW_MISSING_NEXTDATA,
    tc::AllowMissingNextdata,
    formatcp!("Allow {NEXTDATA} to be missing.")
);

tri_flag_arg!(
    AllowMissingTimeArg,
    ta::ALLOW_MISSING_TIME,
    tc::AllowMissingTime,
    "Allow time measurement to be missing."
);

tri_flag_arg!(
    AllowRepairNonUniqueArg,
    ta::ALLOW_REPAIR_NON_UNIQUE,
    tc::AllowRepairNonUnique,
    "Choose how to handle key collisions when repairing keywords. \
     Non-unique keywords will not be kept in the final FCS file since each \
     list of standard and non-standard keywords must be unique."
);

tri_flag_arg!(
    AllowMissingRequiredOffsetsArg,
    ta::ALLOW_MISSING_REQUIRED_OFFSETS,
    tc::AllowMissingRequiredOffsets,
    formatcp!(
        "Allow required offsets to be missing from {TEXT_SEG}. \
             Only applies to FCS 3.0/3.1."
    )
);

tri_flag_arg!(
    DisallowRangeTruncationArg,
    ta::DISALLOW_RANGE_TRUNCATION,
    tc::DisallowRangeTrunc,
    formatcp!(
        "Disallow {PN_R} values which need to be truncated to fit in type \
         dictated by {DATATYPE} (and {PNDATATYPE} for FCS 3.2) and {PN_B} \
         for a given measurement."
    )
);

tri_flag_arg!(
    AllowUnevenEventWidthArg,
    ta::ALLOW_UNEVEN_EVENT_WIDTH,
    tc::AllowUnevenEventWidth,
    formatcp!("Allow event width to not evenly divide length of {DATA_SEG}.")
);

tri_flag_arg!(
    AllowTotMismatchArg,
    ta::ALLOW_TOT_MISMATCH,
    tc::AllowTotMismatch,
    formatcp!("Allow {TOT} to mismatch the number of events that are actually in {DATA_SEG}.")
);

tri_flag_arg!(
    AllowMissingCRCArg,
    ta::ALLOW_MISSING_CRC,
    tc::AllowMissingCRC,
    "Allow CRC word at the end of the dataset to be missing."
);

tri_flag_arg!(
    AllowMismatchCRCArg,
    ta::ALLOW_MISMATCH_CRC,
    tc::AllowMismatchCRC,
    "Allow computed CRC to not match the CRC word at the end of the dataset."
);

macro_rules! override_flag_arg {
    ($t:ident, $long:expr, $help:expr) => {
        #[derive(Args)]
        struct $t {
            #[arg(id = $long)]
            #[arg(long = $long)]
            #[arg(value_name = "BOOL")]
            #[arg(default_missing_value("true"))]
            #[arg(require_equals(true))]
            #[arg(num_args(0..=1))]
            #[arg(help = $help)]
            inner: Option<bool>,
        }
    };
}

override_flag_arg!(
    SquishOffsetsArg,
    ta::SQUISH_OFFSETS,
    formatcp!(
        "If {DATA_SEG}/{ANALYSIS_SEG} end in 0, use 0 for start as well. \
             Should not be used for FCS 2.0 files."
    )
);

override_flag_arg!(
    AllowPseudoEmptyArg,
    ta::ALLOW_PSEUDOEMPTY,
    "Treat offsets like '0,-1' or '1000,999' as '0,0'."
);

override_flag_arg!(
    DedupMeasNamesArg,
    ta::DEDUP_MEAS_NAMES,
    formatcp!(
        "Force all {PN_N} to be unique by appending '{}X' \
         to each duplicate and appending 'X' (starting at 0)",
        tc::DEDUP_PNN_SEP,
    )
);

override_flag_arg!(
    TrimIntraValueWhitespaceArg,
    ta::TRIM_INTRA_VALUE_WHITESPACE,
    "Remove spaces between comma-separated values."
);

override_flag_arg!(
    AllowOtherFeatureArg,
    ta::ALLOW_OTHER_FEATURE,
    formatcp!("Allow {PNFEATURE} to be a value other than \"Area\", \"Width\", or \"Height\"")
);

override_flag_arg!(
    FixLogScaleOffsetArg,
    ta::FIX_LOG_SCALE_OFFSETS,
    formatcp!(
        "Fix {PN_E} keys that have log scaling with zero offset. \
         Specifically, this will replace values like 'X,0.0' with 'X,1.0' \
         where 'X' is a positive decimal number. Having '0.0' for log offset \
         is mathematical nonsense.",
    )
);

override_flag_arg!(
    DisallowLocaltimeArg,
    ta::DISALLOW_LOCALTIME,
    formatcp!(
        "Require that {BEGINDATETIME} and {ENDDATETIME} have a timezone if \
         provided. This is not required by the standard, but not having a \
         timezone is ambiguous since the absolute value of the timestamp is \
         dependent on localtime and therefore is location-dependent. Only \
         affects FCS 3.2.",
    )
);

override_flag_arg!(
    IgnoreTextDataOffsetsArg,
    ta::IGNORE_TEXT_DATA_OFFSETS,
    formatcp!("Ignore offsets for {DATA_SEG} from {TEXT_SEG}.")
);

override_flag_arg!(
    IgnoreTextAnalysisOffsetsArg,
    ta::IGNORE_TEXT_ANALYSIS_OFFSETS,
    formatcp!("Ignore offsets for {ANALYSIS_SEG} from {TEXT_SEG}.")
);

fn tri_flag_help<T>(help_front: impl Display) -> String
where
    T: tc::TriErrorFlag,
{
    let (what_true, what_false) = if T::FALSE_IS_ERROR {
        ("warning", "error")
    } else {
        ("error", "warning")
    };
    format!(
        "{help_front} If {true_}, throw {what_true}. If {false_}, throw \
         {what_false}. If {silent}, ignore completely.",
        false_ = fmt_config_val!(TRI_FALSE_LEVEL),
        true_ = fmt_config_val!(TRI_TRUE_LEVEL),
        silent = fmt_config_val!(TRI_SILENT_LEVEL),
    )
}

impl ReadHeaderArgs {
    fn run<Wout: Write, Werr: Write>(&self, stdout: Wout, mut stderr: Werr) -> AppResult<()> {
        let conf = self.config();
        let filepath = &self.input.inner;
        let (ws, res) = api::fcs_read_header(filepath, DatasetOffset(0), &conf)
            .resolve_commutative(|ws| ws, |s| s);
        print_warnings(ws, &mut stderr)?;
        to_writer(stdout, &res?)?;
        Ok(())
    }

    fn config(&self) -> cfg::ReadHeaderConfig {
        let strat = self.strategy.inner.unwrap_or_default();
        cfg::ReadHeaderConfig {
            header: self.header.config(strat),
            offset: self.offset.config(strat),
        }
    }
}

impl ReadFlatArgs {
    fn run<Wout: Write, Werr: Write>(&self, stdout: Wout, mut stderr: Werr) -> AppResult<()> {
        let conf = self.config();
        let filepath = &self.input.inner;
        let skip = self.dataset_index.inner;
        let mmap = self.mmap.inner;
        let (ws, res) = api::fcs_read_flat_texts(filepath, skip, Some(1), false, mmap, &conf)
            .resolve_commutative(|ws| ws, |s| s);
        print_warnings(ws, &mut stderr)?;
        to_writer(stdout, &res?.first())?;
        Ok(())
    }

    fn config(&self) -> cfg::ReadFlatTEXTConfig {
        let strat = self.strategy.inner.unwrap_or_default();
        cfg::ReadFlatTEXTConfig {
            header: self.flat.header.config(strat),
            flat: self.flat.flat.config(strat),
            offset: self.flat.offset.config(strat),
            shared: self.flat.shared.config(),
        }
    }
}

impl ReadStdArgs {
    fn run<Wout: Write, Werr: Write>(&self, stdout: Wout, mut stderr: Werr) -> AppResult<()> {
        let conf = self.config();
        let filepath = &self.input.inner;
        let skip = self.dataset_index.inner;
        let mmap = self.mmap.inner;
        let (ws, res) = api::fcs_read_std_texts(filepath, skip, Some(1), false, mmap, &conf)
            .resolve_commutative(|ws| ws, |s| s);
        print_warnings(ws, &mut stderr)?;
        if let Some((core, uncore)) = res?.first() {
            let obj = json!({"core": core, "uncore": uncore});
            to_writer(stdout, &obj)?;
        } else {
            to_writer(stdout, &Value::Null)?;
        }
        Ok(())
    }

    fn config(&self) -> cfg::ReadStdTEXTConfig {
        let strat = self.strategy.inner.unwrap_or_default();
        self.std.config(strat)
    }
}

impl ReadStdTabularArgs {
    fn run_meas<Wout: Write, Werr: Write>(&self, stdout: Wout, stderr: Werr) -> AppResult<()> {
        self.run_inner(stdout, stderr, |core, out, delim| {
            Ok(core.print_meas_table(out, delim.0)?)
        })
    }

    fn run_spillover<Wout: Write, Werr: Write>(&self, stdout: Wout, stderr: Werr) -> AppResult<()> {
        self.run_inner(stdout, stderr, |core, out, delim| {
            Ok(core.print_comp_or_spillover_table(out, delim.0)?)
        })
    }

    fn run_inner<F, Wout: Write, Werr: Write>(
        &self,
        mut stdout: Wout,
        mut stderr: Werr,
        f: F,
    ) -> AppResult<()>
    where
        F: FnOnce(&AnyCoreTEXT, &mut Wout, CliDelim) -> AppResult<()>,
    {
        let conf = self.config();
        let delim = self.delim.inner.unwrap_or_default();
        let filepath = &self.input.inner;
        let skip = self.dataset_index.inner;
        let mmap = self.mmap.inner;
        let (ws, res) = api::fcs_read_std_texts(filepath, skip, Some(1), false, mmap, &conf)
            .resolve_commutative(|ws| ws, |s| s);
        print_warnings(ws, &mut stderr)?;
        if let Some((core, _)) = res?.first() {
            f(core, &mut stdout, delim)?;
        } else {
            to_writer(stdout, &Value::Null)?;
        }
        Ok(())
    }

    fn config(&self) -> cfg::ReadStdTEXTConfig {
        let strat = self.strategy.inner.unwrap_or_default();
        self.std.config(strat)
    }
}

impl ReadDatasetArgs {
    fn run<Wout: Write, Werr: Write>(&self, mut stdout: Wout, mut stderr: Werr) -> AppResult<()> {
        let conf = self.config();
        let delim = self.delim.inner.unwrap_or_default();
        let filepath = &self.input.inner;
        let skip = self.dataset_index.inner;
        let mmap = self.mmap.inner;
        let (ws, res) = api::fcs_read_std_datasets(filepath, skip, Some(1), false, mmap, &conf)
            .resolve_commutative(|ws| ws, |s| s);
        print_warnings(ws, &mut stderr)?;
        if let Some((core, _)) = res?.first() {
            print_parsed_data(&mut stdout, core, delim.0)?;
        } else {
            to_writer(stdout, &Value::Null)?;
        }
        Ok(())
    }

    fn config(&self) -> cfg::ReadStdDatasetConfig {
        let strat = self.strategy.inner.unwrap_or_default();
        self.dataset.config(strat)
    }
}

impl RepairFileArgs {
    fn run<Wout: Write, Werr: Write>(&self, stdout: Wout, mut stderr: Werr) -> AppResult<()> {
        let read_conf = self.config();
        let write_conf = self.write.config();
        let ipath = &self.input.inner;
        let opath = &self.output.inner;
        let skip = self.multi.skip;
        let limit = self.multi.limit;
        let scan = self.multi.scan.unwrap_or_default();
        let mmap = self.mmap.inner;
        let (read_ws, read_res) =
            api::fcs_read_std_datasets(ipath, skip, limit, scan, mmap, &read_conf)
                .resolve_commutative(|ws| ws, |s| s);
        // order matters, print warnings before unpacking the result which
        // will short-circuit on error
        print_warnings(read_ws, &mut stderr)?;
        let (cores, outs): (Vec<_>, Vec<_>) = read_res?.into_iter().unzip();
        let (write_ws, write_res) = api::fcs_write_datasets(opath, &cores[..], &write_conf)
            .resolve_commutative(|ws| ws, |e| e);
        print_warnings(write_ws, &mut stderr)?;
        let _ = write_res?;
        to_writer(stdout, &outs[..])?;
        Ok(())
    }

    fn config(&self) -> cfg::ReadStdDatasetConfig {
        let strat = self.strategy.inner.unwrap_or_default();
        self.dataset.config(strat)
    }
}

impl SummarizeArgs {
    fn run<Wout: Write, Werr: Write>(&self, stdout: Wout, mut stderr: Werr) -> AppResult<()> {
        let conf = self.config();
        let filepath = &self.input.inner;
        let skip = self.multi.skip;
        let limit = self.multi.limit;
        let scan = self.multi.scan.unwrap_or_default();
        let mmap = self.mmap.inner;
        let (ws, res) = api::fcs_summarize(filepath, skip, limit, scan, mmap, &conf)
            .resolve_commutative(|ws| ws, |s| s);
        print_warnings(ws, &mut stderr)?;
        to_writer(stdout, &res?)?;
        Ok(())
    }

    fn config(&self) -> cfg::ReadFlatDatasetConfig {
        let strat = self.strategy.inner.unwrap_or_default();
        cfg::ReadFlatDatasetConfig {
            header: self.flat.header.config(strat),
            flat: self.flat.flat.config(strat),
            offset: self.flat.offset.config(strat),
            repair: self.repair.config(strat),
            layout: self.data.config(strat),
            data: self.dataset.config(strat),
            shared: self.flat.shared.config(),
        }
    }
}

impl ReadHeaderArgs_ {
    fn config(&self, strat: tc::ReadStrategy) -> tc::ReadHeaderInnerConfig {
        let mut conf = tc::ReadHeaderInnerConfig::new_with_strategy(strat);

        let _ = self.text_corr.inner.map(|x| conf.text_correction = x.0);
        let _ = self.data_corr.inner.map(|x| conf.data_correction = x.0);
        let _ = self.anal_corr.inner.map(|x| conf.analysis_correction = x.0);

        let _ = self
            .other_correction
            .as_ref()
            .map(|xs| conf.other_corrections.clone_from(&xs.0));

        let _ = self.max_other.inner.map(|x| conf.max_other = x.0);
        let _ = self.other_width.map(|x| conf.other_width = x);
        let _ = self.guess_other_width.map(|x| conf.guess_other_width = x);
        let _ = self
            .squish_offsets
            .inner
            .map(|x| conf.squish_offsets = x.into());

        conf
    }
}

impl ReadOffsetArgs_ {
    fn config(&self, strat: tc::ReadStrategy) -> tc::ReadOffsetConfig {
        let mut conf = tc::ReadOffsetConfig::new_with_strategy(strat);

        let _ = self
            .allow_pseudoempty
            .inner
            .map(|x| conf.allow_pseudoempty = x.into());
        let _ = self
            .dataset_overflow_limit
            .map(|x| conf.dataset_overflow_limit = x);
        let _ = self
            .overlap_correction_limit
            .map(|x| conf.overlap_correction_limit = x);

        conf
    }
}

impl ReadFlatKwsArgsInner_ {
    fn config(&self, strat: tc::ReadStrategy) -> tc::ReadHeaderAndTEXTConfig {
        let mut c = tc::ReadHeaderAndTEXTConfig::new_with_strategy(strat);

        macro_rules! assign_opt {
            ($($field:ident),*) => {
                $(
                    let _ = self.$field.inner.map(|x| c.$field = x.0);
                )*
            };
        }

        assign_opt!(
            version_override,
            supp_text_correction,
            allow_duplicated_supp_text,
            allow_non_ascii_delim,
            allow_even_delims,
            allow_nonunique,
            allow_odd_tokens,
            allow_empty_keys,
            allow_delim_at_boundary,
            allow_non_ascii_keys,
            allow_non_utf8_values,
            allow_missing_supp_text,
            allow_supp_text_own_delim,
            allow_missing_nextdata
        );

        let _ = self.nextdata_correction.map(|x| c.nextdata_correction = x);
        let _ = self.process_supp_text.map(|x| c.process_supp_text = x);
        let _ = self.delim_escape_mode.map(|x| c.delim_escape_mode = x);
        let _ = self.use_encoding.map(|x| c.use_encoding = x);
        let _ = self
            .trim_value_whitespace
            .map(|x| c.trim_value_whitespace = x);

        c
    }
}

impl ReadStdKwsArgsInner_ {
    fn config(&self, strat: tc::ReadStrategy) -> cfg::ReadStdKeywordsConfig {
        let mut c = cfg::ReadStdKeywordsConfig::new_with_strategy(strat);

        let _ = self
            .dedup_meas_names
            .inner
            .map(|x| c.dedup_measurement_names = x.into());
        let _ = self
            .trim_intra_value_whitespace
            .inner
            .map(|x| c.trim_intra_value_whitespace = x.into());

        let _ = self
            .time_meas_pattern
            .as_ref()
            .map(|x| c.time_meas_pattern = Selector::Root(x.clone()));

        let _ = self.force_linear_scale.map(|x| c.force_linear_scale = x);

        let _ = self
            .ignore_time_optical_keys
            .as_ref()
            .map(|ks| c.ignore_optical_only_keys = ks.0.clone());

        let _ = self
            .process_optical_only_keys
            .map(|x| c.process_optical_only_keys = x);
        let _ = self
            .allow_missing_time
            .inner
            .map(|x| c.allow_missing_time = x.0);
        let _ = self
            .add_missing_timestep
            .inner
            .map(|x| c.add_missing_timestep = x.0);

        let _ = self
            .spillover_measurement_mode
            .map(|x| c.spillover_measurement_mode = x);

        let _ = self
            .date_pattern
            .inner
            .as_ref()
            .map(|x| c.date_pattern = Selector::Root(x.0.clone()));

        let _ = self
            .time_pattern
            .inner
            .as_ref()
            .map(|x| c.time_pattern = Selector::Root(x.0.clone()));

        let _ = self
            .datetime_pattern
            .inner
            .as_ref()
            .map(|x| c.datetime_pattern = Selector::Root(x.0.clone()));

        let _ = self
            .last_modified_pattern
            .inner
            .as_ref()
            .map(|x| c.last_modified_pattern = Selector::Root(x.0.clone()));

        let _ = self
            .allow_other_feature
            .inner
            .map(|x| c.allow_other_feature = x.into());

        let _ = self
            .process_pseudostandard
            .inner
            .map(|x| c.process_pseudostandard = x);

        let _ = self
            .process_hyper_par
            .inner
            .map(|x| c.process_hyper_par = x);
        let _ = self
            .process_other_version
            .inner
            .map(|x| c.process_other_version = x);
        let _ = self
            .process_extra_timestep
            .inner
            .map(|x| c.process_extra_timestep = x);

        let _ = self
            .fix_log_scale_offset
            .inner
            .map(|x| c.fix_log_scale_offsets = x.into());

        let _ = self
            .disallow_localtime
            .inner
            .map(|x| c.disallow_localtime = x.into());

        c
    }
}

impl ReadRepairKwsArgs_ {
    fn config(&self, strat: tc::ReadStrategy) -> cfg::ReadRepairKeywordsConfig {
        let mut c = cfg::ReadRepairKeywordsConfig::new_with_strategy(strat);

        let _ = self
            .ignore_std_keys
            .inner
            .as_ref()
            .map(|x| c.ignore_standard_keys = AppendableSelector::root(x.0.clone()));

        let _ = self
            .promote_nonstandard_keys
            .inner
            .as_ref()
            .map(|x| c.promote_nonstandard_keys = AppendableSelector::root(x.0.clone()));

        let _ = self
            .demote_standard_keys
            .inner
            .as_ref()
            .map(|x| c.demote_standard_keys = AppendableSelector::root(x.0.clone()));

        let _ = self
            .rename_standard_keys
            .as_ref()
            .map(|x| c.rename_standard_keys = AppendableSelector::root(x.0.clone()));

        let _ = self
            .replace_std_key_vals
            .as_ref()
            .map(|x| c.replace_standard_key_values = AppendableSelector::root(x.0.clone()));

        let _ = self
            .append_std_keywords
            .as_ref()
            .map(|x| c.append_standard_keywords = AppendableSelector::root(x.0.clone()));

        let _ = self
            .substitute_standard_key_vals
            .as_ref()
            .map(|x| c.substitute_standard_key_values = AppendableSelector::root(x.0.clone()));

        let _ = self
            .promote_all_nonstandard_keys
            .map(|x| c.promote_all_nonstandard_keys = x.into());

        let _ = self
            .allow_repair_non_unique
            .inner
            .map(|x| c.allow_repair_non_unique = x.0);

        c
    }
}

impl ReadDataKwsArgs_ {
    fn config(&self, strat: tc::ReadStrategy) -> tc::ReadDataKeywordsConfig {
        let mut c = tc::ReadDataKeywordsConfig::new_with_strategy(strat);

        let _ = self
            .text_data_correction
            .inner
            .map(|x| c.text_data_correction = x.0);
        let _ = self
            .text_analysis_correction
            .inner
            .map(|x| c.text_analysis_correction = x.0);

        let _ = self
            .ignore_text_data_offsets
            .inner
            .map(|x| c.ignore_text_data_offsets = x.into());
        let _ = self
            .ignore_text_analysis_offsets
            .inner
            .map(|x| c.ignore_text_analysis_offsets = x.into());

        let _ = self
            .allow_header_text_offset_mismatch
            .map(|x| c.allow_header_text_offset_mismatch = x);

        let _ = self
            .allow_missing_required_offsets
            .inner
            .map(|x| c.allow_missing_required_offsets = x.0);

        let _ = self
            .process_optional_failure
            .inner
            .map(|x| c.process_optional_failure = x);

        let _ = self.int_width_override.map(|x| c.int_width_override = x);
        let _ = self
            .byteord_override
            .as_ref()
            .map(|x| c.byteord_override = x.clone());
        let _ = self
            .disallow_range_truncation
            .inner
            .map(|x| c.disallow_range_truncation = x.0);

        c
    }
}

impl ReadDatasetArgsInner_ {
    fn config(&self, strat: tc::ReadStrategy) -> tc::ReadDatasetConfig {
        let mut c = tc::ReadDatasetConfig::new_with_strategy(strat);

        let _ = self
            .data_remainder_limit
            .map(|x| c.data_remainder_limit = x);
        let _ = self
            .allow_tot_mismatch
            .inner
            .map(|x| c.allow_tot_mismatch = x.0);
        let _ = self
            .allow_uneven_event_width
            .inner
            .map(|x| c.allow_uneven_event_width = x.0);

        let _ = self.over_bitmask_action.map(|x| c.over_bitmask_action = x);
        let _ = self.over_range_action.map(|x| c.over_range_action = x);

        let _ = self
            .allow_missing_crc
            .inner
            .map(|x| c.allow_missing_crc = x.0);
        let _ = self
            .allow_mismatch_crc
            .inner
            .map(|x| c.allow_mismatch_crc = x.0);
        let _ = self.compute_crc.map(|x| c.compute_crc = x);
        let _ = self.row_buffer_size.map(|x| c.row_buffer_size = x);

        c
    }
}

impl ReadSharedArgs_ {
    fn config(&self) -> tc::ReadSharedConfig {
        tc::ReadSharedConfig {
            warnings_are_errors: self.warnings_are_errors,
            hide_warnings: self.hide_warnings,
        }
    }
}

impl ReadStdKwsArgs_ {
    fn config(&self, strat: tc::ReadStrategy) -> cfg::ReadStdTEXTConfig {
        cfg::ReadStdTEXTConfig {
            header: self.flat.header.config(strat),
            flat: self.flat.flat.config(strat),
            offset: self.flat.offset.config(strat),
            standard: self.std.config(strat),
            repair: self.repair.config(strat),
            layout: self.data.config(strat),
            shared: self.flat.shared.config(),
        }
    }
}

impl ReadStdDatasetArgs_ {
    fn config(&self, strat: tc::ReadStrategy) -> cfg::ReadStdDatasetConfig {
        cfg::ReadStdDatasetConfig {
            header: self.std.flat.header.config(strat),
            flat: self.std.flat.flat.config(strat),
            offset: self.std.flat.offset.config(strat),
            standard: self.std.std.config(strat),
            repair: self.std.repair.config(strat),
            layout: self.std.data.config(strat),
            data: self.dataset.config(strat),
            shared: self.std.flat.shared.config(),
        }
    }
}

impl WriteArgs {
    fn config(&self) -> tc::WriteDatasetInnerConfig {
        let mut conf = tc::WriteDatasetInnerConfig::default();

        let _ = self.write_delim.map(|x| conf.text.delim = x);
        let _ = self.big_other.map(|x| conf.text.big_other = x.into());

        conf
    }
}

#[derive(Clone, Copy)]
struct CliTriFlag<T>(T);

impl<T> FromStr for CliTriFlag<T>
where
    T: TriErrorFlag,
    TriFlag: Into<T>,
{
    type Err = PartialTriErrorFlagError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        T::from_partial_str(s).map(Self)
    }
}

#[derive(Clone)]
struct CliOpticalOnlyKeys(OpticalOnlyKeys);

impl FromStr for CliOpticalOnlyKeys {
    type Err = ParseOpticalOnlyKeysError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            Ok(Self(OpticalOnlyKeys::default()))
        } else {
            OpticalOnlyKeys::try_from_iter(s.split(',')).map(Self)
        }
    }
}

/// An optional non-empty string where empty string on CLI maps to "none".
#[derive(Clone, Copy)]
struct NENullable<T>(Option<T>);

impl<T: FromStr> FromStr for NENullable<T> {
    type Err = T::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let ret = if s.is_empty() {
            None
        } else {
            Some(s.parse::<T>()?)
        };
        Ok(Self(ret))
    }
}

#[derive(Clone, Copy)]
struct Nullable<T>(Option<T>);

impl<T: FromStr> FromStr for Nullable<T> {
    type Err = T::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let ret = if s == "none" {
            None
        } else {
            Some(s.parse::<T>()?)
        };
        Ok(Self(ret))
    }
}

#[derive(Clone, Copy)]
struct StrOffsetsCorrection<I, S>(OffsetsCorrection<I, S>);

impl<I, S> FromStr for StrOffsetsCorrection<I, S>
where
    (i32, i32): Into<OffsetsCorrection<I, S>>,
{
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let xs = s.split(',').collect::<Vec<_>>();
        match &xs[..] {
            [a, b] => {
                let aa = a.parse::<i32>().map_err(|e| e.to_string())?;
                let bb = b.parse::<i32>().map_err(|e| e.to_string())?;
                Ok(Self((aa, bb).into()))
            }
            _ => Err("offsets must be a pair of integers like 'X,Y'".into()),
        }
    }
}

#[derive(Clone, Copy)]
struct CliDelim(u8);

impl Default for CliDelim {
    fn default() -> Self {
        Self(b'\t')
    }
}

impl FromStr for CliDelim {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let c = s.parse::<char>().map_err(|e| e.to_string())?;
        c.try_into()
            .map_err(|_| "must be a single ASCII character".into())
            .map(Self)
    }
}

#[derive(Clone)]
struct CliOtherOffsetCorrections(Vec<HeaderCorrection<OtherSegmentId>>);

impl FromStr for CliOtherOffsetCorrections {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let go = |x: &str| -> Result<HeaderCorrection<OtherSegmentId>, String> {
            let xs = x.split(',').collect::<Vec<_>>();
            match &xs[..] {
                [a, b] => {
                    let aa = a.parse::<i32>().map_err(|e| e.to_string())?;
                    let bb = b.parse::<i32>().map_err(|e| e.to_string())?;
                    Ok((aa, bb).into())
                }
                _ => Err("offsets must be a pair of integers like 'X,Y'".into()),
            }
        };

        let ret = if s.is_empty() {
            vec![]
        } else {
            s.split(OFFSET_DELIM).map(go).collect::<Result<_, _>>()?
        };
        Ok(Self(ret))
    }
}

#[derive(Clone, Default)]
struct CliStdKeyPatterns<const HAS_PRE: bool>(StdKeyPatterns<HAS_PRE>);

impl<const HAS_PRE: bool> FromStr for CliStdKeyPatterns<HAS_PRE>
where
    StdKeyOrPattern<HAS_PRE>: FromStr,
    <StdKeyOrPattern<HAS_PRE> as FromStr>::Err: fmt::Display,
{
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let go = |ss: &str| {
            ss.parse::<StdKeyOrPattern<HAS_PRE>>()
                .map_err(|e| e.to_string())
        };
        let single = || Ok(Self(once(go(s)?).collect()));
        if let Some(b0) = s.as_bytes().first() {
            let delim = char::from(*b0);
            let rest = s.strip_prefix(delim).unwrap();
            if !b0.is_ascii() {
                Err(format!("delimiter must be an ASCII character, got {delim}"))
            } else if rest.is_empty() {
                Ok(Self::default())
            } else if rest.strip_suffix(delim).is_some() {
                if b0 == &PATTERN_DELIMITER {
                    // String starts and ends with delimiter but delimiter is the
                    // same as the regex pattern specifier; treat as a single
                    // pattern.
                    single()
                } else {
                    // Otherwise delimiters should be treated as real delimiters,
                    // strip off the start and end delimiter and split on whatever
                    // other delimiters we find.
                    let mid = s.strip_prefix(delim).unwrap().strip_suffix(delim).unwrap();
                    mid.split(delim)
                        .map(go)
                        .collect::<Result<StdKeyPatterns<HAS_PRE>, _>>()
                        .map(Self)
                }
            } else {
                // String is either a single character or does not start/end with
                // the same character, interpret as a single value.
                single()
            }
        } else {
            Ok(Self::default())
        }
    }
}

#[derive(Clone)]
struct CliKeyPairs(KeyPairs);

impl FromStr for CliKeyPairs {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let go_k = |x: &str| x.parse::<AnyKey>().map_err(|e| e.to_string());
        let go_v = |x: &str| x.parse::<AnyKey>().map_err(|e| e.to_string());
        let xs = parse_pairs(s, go_k, go_v)?;
        xs.iter()
            .cloned()
            .collect::<HashMap<_, _>>()
            .try_into()
            .map(Self)
            .map_err(|e| e.to_string())
    }
}

#[derive(Clone)]
struct CliStdKeyValues(StdKeyValues);

impl FromStr for CliStdKeyValues {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let go_k = |x: &str| x.parse::<StdKey>().map_err(|e| e.to_string());
        let go_v = |x: &str| x.parse::<NEString>().map_err(|e| e.to_string());
        let xs = parse_pairs(s, go_k, go_v)?;
        let ret = xs.iter().cloned().collect::<HashMap<_, _>>();
        Ok(Self(ret))
    }
}

#[derive(Clone)]
struct CliSubPatterns(SubPatterns);

impl FromStr for CliSubPatterns {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let go_k = |x: &str| {
            x.parse::<StdKeyOrPattern<true>>()
                .map_err(|e| e.to_string())
        };
        let go_v = |x: &str| parse_sub_pattern_inner(x).map_err(|e| e.to_string());
        let xs = parse_pairs(s, go_k, go_v)?;
        let ret = xs.iter().cloned().collect::<HashMap<_, _>>();
        Ok(Self(StdKeysOrPatterns(ret)))
    }
}

fn parse_pairs<K, V, Fk, Fv>(s: &str, fk: Fk, fv: Fv) -> StrResult<Vec<(K, V)>>
where
    Fk: Fn(&str) -> StrResult<K>,
    Fv: Fn(&str) -> StrResult<V>,
{
    if let Some(b0) = s.as_bytes().first() {
        let delim = char::from(*b0);
        let rest = s.strip_prefix(delim).unwrap();
        if !b0.is_ascii() {
            Err(format!("delimiter must be an ASCII character, got {delim}"))
        } else if rest.is_empty() {
            Ok(vec![])
        } else if let Some(mid) = rest.strip_suffix(delim) {
            let go = |a: &str, b: &str| Ok((fk(a)?, fv(b)?));
            let mut it = mid.split(delim).tuples();
            let out = it.by_ref().map(|(a, b)| go(a, b)).collect();
            if it.into_buffer().next().is_some() {
                Err("number of delimited values must be even".into())
            } else {
                out
            }
        } else {
            Err("First and last character must be the same".into())
        }
    } else {
        Ok(vec![])
    }
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

    // Max to always ensure there is one row, which only matters if we ever
    // encounter a machine with more than 4096 parameters.
    let rows_per_buf = (BUF_SIZE / ncols).max(1);
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

type AppResult<T> = Result<T, Box<dyn Error>>;

type StrResult<T> = Result<T, String>;

// formatted segments

macro_rules! fmt_seg {
    ($s:expr) => {
        formatcp!("{ITALIC}{}{RESET}", $s)
    };
}

const HEADER_SEG: &str = fmt_seg!("HEADER");
const TEXT_SEG: &str = fmt_seg!("TEXT");
const PRIM_TEXT_SEG: &str = fmt_seg!("primary TEXT");
const SUPP_TEXT_SEG: &str = fmt_seg!("supplemental TEXT");
const DATA_SEG: &str = fmt_seg!("DATA");
const ANALYSIS_SEG: &str = fmt_seg!("ANALYSIS");
const OTHER_SEG: &str = fmt_seg!("OTHER");

// formatted keywords

macro_rules! fmt_kw {
    ($s:expr) => {
        formatcp!("{ITALIC}{}{RESET}", $s)
    };
}

const PAR: &str = fmt_kw!(sk::PAR.as_str());
const TOT: &str = fmt_kw!(sk::TOT.as_str());
const BYTEORD: &str = fmt_kw!(sk::BYTEORD.as_str());
const DATATYPE: &str = fmt_kw!(sk::DATATYPE.as_str());
const TIMESTEP: &str = fmt_kw!(sk::TIMESTEP.as_str());
const DATE: &str = fmt_kw!(sk::DATE.as_str());
const BTIM: &str = fmt_kw!(sk::BTIM.as_str());
const ETIM: &str = fmt_kw!(sk::ETIM.as_str());
const LAST_MODIFIED: &str = fmt_kw!(sk::LAST_MODIFIED.as_str());
const BEGINDATETIME: &str = fmt_kw!(sk::BEGINDATETIME.as_str());
const ENDDATETIME: &str = fmt_kw!(sk::ENDDATETIME.as_str());
const NEXTDATA: &str = fmt_kw!(sk::NEXTDATA.as_str());
const SPILLOVER: &str = fmt_kw!(sk::SPILLOVER.as_str());
const PNFEATURE: &str = fmt_kw!(sk::PNFEATURE.as_str());
const PN_B: &str = fmt_kw!(sk::PNB.as_str());
const PN_R: &str = fmt_kw!(sk::PNR.as_str());
const PN_N: &str = fmt_kw!(sk::PNN.as_str());
const PN_E: &str = fmt_kw!(sk::PNE.as_str());
const PNDATATYPE: &str = fmt_kw!(sk::PNDATATYPE.as_str());
const PN_ANY: &str = fmt_kw!("$PnX");

// formatted subsections

macro_rules! fmt_section {
    ($header:expr, $($para:expr),*) => {{
        const HEADER: &str = formatcp!("{BOLD}{}{RESET}", $header);
        let p = concatcp!(HEADER, $("\n\n    ", $para),*);
        Section { header: HEADER, paragraphs: p}
    }};
}

struct Section {
    header: &'static str,
    paragraphs: &'static str,
}

const DELIM_SECTION: Section = fmt_section!(
    "DELIMITER ESCAPING",
    "The standard allows delimiters to be included in keys or values (tokens) if \
     they are \"escaped\" with another delimiter. This also implies that \
     delimiters can never start or end a token since it is impossible to \
     unambiguously assign such escaped delimiters to either side of the real \
     delimiter. This also means empty tokens are not allowed.",
    "In reality, many files use delimiters as if they are not supposed \
     to be escaped.",
    formatcp!(
        "If {esc} or {unesc}, escape or do not escape delimiters respectively.",
        esc = fmt_config_val!(DELIM_ESCAPED_LEVEL),
        unesc = fmt_config_val!(DELIM_UNESCAPED_LEVEL),
    ),
    formatcp!(
        "If {esc} or {unesc} attempt to guess how delimiters should be treated, \
         falling back to escaped or unescaped mode respectively if the choice is \
         ambiguous. The determination will be made by first scanning {TEXT_SEG} \
         to find all delimiter positions and choosing the mode which results in \
         an even number of tokens with no delimiters in keys (escaped mode) and \
         no blank keys (unescaped mode).",
        esc = fmt_config_val!(DELIM_ESCAPED_LEVEL),
        unesc = fmt_config_val!(DELIM_UNESCAPED_LEVEL),
    ),
    formatcp!(
        "Using the guessing algorithm has a significant performance penalty \
         since {TEXT_SEG} needs to be parsed twice. Furthermore, this \
         algorithm is heuristic and not guaranteed to succeed. An uneven \
         number of tokens implies that {TEXT_SEG} is malformed which will \
         likely be the case assuming that the ending offset for {TEXT_SEG} \
         will be too high (if at all) therefore all delimiters should be in \
         the {TEXT_SEG} segment and can be counted. Keys likely will not have \
         escaped delimiters in them. Keys should almost never be blank in \
         unescaped mode since '\"\"' is almost never a sensible key value."
    ),
    formatcp!(
        "The guessing algorithm is independent of {odd} and {final_} which will \
         trigger as normal if their respective violations are found.",
        odd = fmt_arg!(ta::ALLOW_ODD_TOKENS),
        final_ = fmt_arg!(ta::ALLOW_EVEN_DELIMS),
    )
);

const SUB_SECTION: Section = fmt_section!(
    "SUBSTITUTION",
    formatcp!(
        "The SUB part in {pat} is a sed-like pattern which will be used to edit \
         the value of KEY. It must be a string like 's<D><FROM><D><TO>[<D>g]' \
         where 'D' is a delimiter (any character), FROM is a regular expression \
         and TO is a replacement pattern. FROM and TO must follow the syntax \
         outlined in {REGEXP_REF} and {REGEXP_REP_REF} respectively, with the \
         caveat that only bracketed replacement syntax is allowed.",
        pat = fmt_arg!(ta::SUB_STD_KEY_VALS),
    )
);

const DATE_SECTION: Section = fmt_section!(
    "DATE PATTERN",
    formatcp!(
        "The value for {pat} will be used as an alternative pattern when parsing \
         {DATE}. It should have specifiers for year, month, and day as outlined \
         in {CHRONO_REF}. If not supplied, {DATE} will be parsed according to \
         the standard pattern which is {fmt}.",
        pat = fmt_arg!(ta::DATE_PATTERN),
        fmt = fmt_val!(tc::DEFAULT_DATE_FORMAT),
    )
);

const TIME_SECTION: Section = fmt_section!(
    "TIME PATTERN",
    formatcp!(
        "If supplied, will be used as an alternative pattern when parsing {BTIM} \
         and {ETIM} It should have specifiers for hours, minutes, and seconds as \
         outlined in {CHRONO_REF}. It may optionally also have a sub-seconds \
         specifier as shown in the same link. Furthermore, the specifiers \
         {base60} and {base100} may be used to match 1/60 and centiseconds \
         respectively. If not supplied, {BTIM} and {ETIM} will be parsed \
         according to the standard pattern which is {fmt2_0} for 2.0, {fmt3_0} \
         for 3.0, and {fmt3_1} for 3.1 and up.",
        base60 = fmt_val!(tc::BASE60_SECOND_SPEC),
        base100 = fmt_val!(tc::BASE100_SECOND_SPEC),
        fmt2_0 = fmt_val!(tc::DEFAULT_TIME_FORMAT_2_0),
        fmt3_0 = fmt_val!(tc::DEFAULT_TIME_FORMAT_3_0),
        fmt3_1 = fmt_val!(tc::DEFAULT_TIME_FORMAT_3_1)
    )
);

macro_rules! joincp {
    ($s0:ident, $($s:ident),*) => {
        concatcp!($s0.paragraphs, $("\n\n", $s.paragraphs),*)
    };
}

const FLAT_LONG_HELP: &str = joincp!(DELIM_SECTION, SUB_SECTION);

const STD_LONG_HELP: &str = joincp!(DELIM_SECTION, SUB_SECTION, DATE_SECTION, TIME_SECTION);

// formatting primitives

pub static BOLD: &str = "\x1B[1m";

pub static ITALIC: &str = "\x1B[3m";

pub static RESET: &str = "\x1B[0m";

// static urls

const CHRONO_REF: &str = "https://docs.rs/chrono/latest/chrono/format/strftime/index.html";

const REGEXP_REF: &str = "https://docs.rs/regex/latest/regex/#syntax";

const REGEXP_REP_REF: &str = "https://docs.rs/regex/latest/regex/struct.Regex.html#method.replace";

// misc

const OFFSET_DELIM: char = '/';

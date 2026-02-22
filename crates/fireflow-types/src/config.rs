use const_format::formatcp;
use derive_more::Display;
use itertools::Itertools as _;
use thiserror::Error;

#[cfg(feature = "python")]
use fireflow_core_proc::{DisplayAsPyErr, FromPyString, IntoPyString};

pub trait EnumStrIter: Sized {
    fn into_str(self) -> &'static str;

    fn iter() -> impl Iterator<Item = Self>;

    fn iter_str() -> impl Iterator<Item = &'static str> {
        Self::iter().map(EnumStrIter::into_str)
    }
}

/// Implement a enum with variants that map to defined string literals.
///
/// This will make 4 things:
/// 1. the enum itself (with docs as given)
/// 2. a FromStr impl that maps each variant to a string literal
/// 3. an error for FromStr that lists each string variant
/// 4. an array that contains all string literals in the order given
#[macro_export]
macro_rules! impl_str_enum {
    ($(#[$flag_meta:meta])* $flag_name:ident,
     $(#[$error_meta:meta])* $error_name:ident,
     $($(#[$var_meta:meta])* $var:ident => $strlit:tt)*
    ) => {
        $(#[$flag_meta])*
        #[derive(Clone, Copy)]
        pub enum $flag_name {
            $(
                $(#[$var_meta])*
                $var,
            )*
        }

        impl std::str::FromStr for $flag_name {
            type Err = $error_name;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                match s {
                    $($strlit => Ok(Self::$var),)*
                    _ => Err($error_name),
                }
            }
        }

        impl $crate::config::EnumStrIter for $flag_name {
            fn into_str(self) -> &'static str {
                match self {
                    $(Self::$var => $strlit,)*
                }
            }

            fn iter() -> impl Iterator<Item = Self> {
                [$(Self::$var),*].into_iter()
            }
        }

        $(#[$error_meta])*
        #[derive(Error, Debug)]
        pub struct $error_name;

        impl std::fmt::Display for $error_name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
                let all: Vec<_> = <$flag_name as $crate::config::EnumStrIter>::iter_str().collect();
                let (last, rest) = all.split_last().expect("should have at least 2 levels");
                let ys = rest.iter().map(|x| format!("'{x}'")).join(", ");
                write!(f, "must be one of {ys}, or '{last}'")
            }
        }
    };
}

/// Make an enum string literal to be used as a configuration flag.
///
/// In addition to that described in [`impl_str_enum`], this will add:
/// * Default trait for first variant
/// * Display trait for enum
/// * Python to/from traits for both enum and parse error
#[macro_export]
macro_rules! impl_config_flag {
    ($(#[$flag_meta:meta])* $flag_name:ident,
     $(#[$error_meta:meta])* $error_name:ident,
     $(#[$var_meta0:meta])* $var0:ident => $strlit0:tt
     $($(#[$var_meta:meta])* $var:ident => $strlit:tt)*
    ) => {
        impl_str_enum!(
            #[derive(Display, Default)]
            #[display("{}", self.into_str())]
            #[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
            $(#[$flag_meta])* $flag_name,

            #[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
            #[cfg_attr(feature = "python", pyerr($crate::python::ConfigError))]
            $(#[$error_meta])* $error_name,

            #[default]
            $(#[$var_meta0])* $var0 => $strlit0

            $($(#[$var_meta])* $var => $strlit)*
        );
    }
}

pub const TRI_FALSE_LEVEL: &str = FALSE_LEVEL;
pub const TRI_TRUE_LEVEL: &str = TRUE_LEVEL;
pub const TRI_SILENT_LEVEL: &str = SILENT_LEVEL;

impl_config_flag!(
    /// Tri-state flag to throw warning, throw error, or do nothing
    TriFlag,
    /// Error when parsing [`TriFlag`] from [`String`]
    TriFlagError,
    False  => TRI_FALSE_LEVEL
    True   => TRI_TRUE_LEVEL
    Silent => TRI_SILENT_LEVEL
);

pub const OTHER_WIDTH_NONE_LEVEL: &str = NONE_LEVEL;
pub const OTHER_WIDTH_ERROR_LEVEL: &str = ERROR_LEVEL;
pub const OTHER_WIDTH_WARN_LEVEL: &str = WARN_LEVEL;
pub const OTHER_WIDTH_SILENT_LEVEL: &str = SILENT_LEVEL;

impl_config_flag!(
    /// Choose how to guess the width for OTHER segments.
    GuessOtherWidth,
    /// Error when parsing [`GuessOtherWidth`] from [`String`]
    GuessOtherWidthError,
    None   => OTHER_WIDTH_NONE_LEVEL
    Error  => OTHER_WIDTH_ERROR_LEVEL
    Warn   => OTHER_WIDTH_WARN_LEVEL
    Silent => OTHER_WIDTH_SILENT_LEVEL
);

pub const KW_ERROR_LEVEL: &str = ERROR_LEVEL;
pub const KW_DEMOTE_WARN_LEVEL: &str = DEMOTE_WARN_LEVEL;
pub const KW_DEMOTE_SILENT_LEVEL: &str = DEMOTE_SILENT_LEVEL;
pub const KW_DROP_WARN_LEVEL: &str = DROP_WARN_LEVEL;
pub const KW_DROP_SILENT_LEVEL: &str = DROP_SILENT_LEVEL;

impl_config_flag!(
    /// Configuration to deal with optional standard keywords that cause errors.
    ProcessKeywordFailure,
    /// Error when parsing [`ProcessKeywordFailure`] from [`String`]
    ProcessKeywordFailureError,
    Error        => KW_ERROR_LEVEL
    DemoteWarn   => KW_DEMOTE_WARN_LEVEL
    DemoteSilent => KW_DEMOTE_SILENT_LEVEL
    DropWarn     => KW_DROP_WARN_LEVEL
    DropSilent   => KW_DROP_SILENT_LEVEL
);

pub const DELIM_ESCAPED_LEVEL: &str = "escaped";
pub const DELIM_UNESCAPED_LEVEL: &str = "unescaped";
pub const DELIM_GUESS_ESCAPED_LEVEL: &str = "guess_escaped";
pub const DELIM_GUESS_UNESCAPED_LEVEL: &str = "guess_unescaped";

impl_config_flag!(
    /// Choose how to escape delims in TEXT segment.
    DelimEscapeMode,
    /// Error when parsing [`DelimEscapeMode`] from [`String`]
    DelimEscapeModeError,
    /// Use escaped delimiters.
    Escaped        => DELIM_ESCAPED_LEVEL
    /// Use unescaped delimiters.
    Unescaped      => DELIM_UNESCAPED_LEVEL
    /// Guess      => falling back to escaped mode.
    GuessEscaped   => DELIM_GUESS_ESCAPED_LEVEL
    /// Guess      => falling back to unescaped mode.
    GuessUnescaped => DELIM_GUESS_UNESCAPED_LEVEL
);

pub const TRIM_NONE_LEVEL: &str = "notrim";
pub const TRIM_ERROR_LEVEL: &str = "trim";
pub const TRIM_BLANK_WARN_LEVEL: &str = "trim_blank_warn";
pub const TRIM_BLANK_SILENT_LEVEL: &str = "trim_blank_silent";

impl_config_flag!(
    /// Choose how to trim values and deal with blanks that may result.
    TrimValueWhitespace,
    /// Error when parsing [`TrimValueWhitespace`] from [`String`]
    TrimValueWhitespaceError,
    /// Do not trim at all.
    Notrim          => TRIM_NONE_LEVEL
    /// Trim whitespace and throw error if blank is created.
    Trim            => TRIM_ERROR_LEVEL
    /// Trim whitespace and throw warning if blank is created.
    TrimBlankWarn   => TRIM_BLANK_WARN_LEVEL
    /// Trim whitespace and do nothing if blank is created.
    TrimBlankSilent => TRIM_BLANK_SILENT_LEVEL
);

pub const FORCE_LINEAR_NONE_LEVEL: &str = NONE_LEVEL;
pub const FORCE_LINEAR_TIME_LEVEL: &str = "time_only";
pub const FORCE_LINEAR_NON_INT_LEVEL: &str = "all_non_int";
pub const FORCE_LINEAR_ALL_LEVEL: &str = ALL_LEVEL;

impl_config_flag!(
    /// Choose which $PnE to force as linear.
    ForceLinearScale,
    /// Error when parsing [`ForceLinearScale`] from [`String`]
    ForceLinearScaleError,
    /// Do not force.
    None      => FORCE_LINEAR_NONE_LEVEL
    /// Only force the temporal measurement.
    TimeOnly  => FORCE_LINEAR_TIME_LEVEL
    /// Force all non-integer measurements and temporal measurement.
    AllNonInt => FORCE_LINEAR_NON_INT_LEVEL
    /// Force all measurements.
    All       => FORCE_LINEAR_ALL_LEVEL
);

impl ForceLinearScale {
    #[must_use]
    pub fn time_selected(self) -> bool {
        !matches!(self, Self::None)
    }
}

pub const TMP_OPT_DEMOTE_WARN_LEVEL: &str = DEMOTE_WARN_LEVEL;
pub const TMP_OPT_DEMOTE_SILENT_LEVEL: &str = DEMOTE_SILENT_LEVEL;
pub const TMP_OPT_DROP_WARN_LEVEL: &str = DROP_WARN_LEVEL;
pub const TMP_OPT_DROP_SILENT_LEVEL: &str = DROP_SILENT_LEVEL;

impl_config_flag!(
    /// Choose what to do with optical keys in time measurement when found.
    ProcessTemporalOpticalKeys,
    /// Error when parsing [`ForceLinearScale`] from [`String`]
    ProcessTemporalOpticalKeysError,
    /// Demote to nonstandard with warning
    DemoteWarn   => TMP_OPT_DEMOTE_WARN_LEVEL
    /// Demote to nonstandard with no warning
    DemoteSilent => TMP_OPT_DEMOTE_SILENT_LEVEL
    /// Drop with warning
    DropWarn     => TMP_OPT_DROP_WARN_LEVEL
    /// Drop with no warning
    DropSilent   => TMP_OPT_DROP_SILENT_LEVEL
);

pub const SPILLOVER_NAMED_LEVEL: &str = "named";
pub const SPILLOVER_INDEXED_LEVEL: &str = "indexed";
pub const SPILLOVER_GUESS_LEVEL: &str = "guess";

impl_config_flag!(
    /// Choose how to parse measurements for $SPILLOVER key
    SpilloverMeasurementMode,
    /// Error when parsing [`ForceLinearScale`] from [`String`]
    SpilloverMeasurementModeError,
    /// Interpret measurements as names which match $PnN.
    Named   => SPILLOVER_NAMED_LEVEL
    /// Interpret measurements as 1-indices (numbers) which point to measurements.
    Indexed => SPILLOVER_INDEXED_LEVEL
    /// Guess how measurements should be interpreted.
    ///
    /// If they are all numbers and all do not point to $PnN, interpret as
    /// indices, otherwise names.
    Guess   => SPILLOVER_GUESS_LEVEL
);

pub const TRUNCATE_NONE_LEVEL: &str = NONE_LEVEL;
pub const TRUNCATE_INT_ONLY_LEVEL: &str = "int_only";
pub const TRUNCATE_ALL_LEVEL: &str = ALL_LEVEL;

impl_config_flag!(
    /// Choose which event types are truncated.
    ///
    /// By default only truncate when $DATATYPE (or $PnDATATYPE) is "I".
    TruncateEventValues,
    /// Error when parsing [`TruncateEventValues`] from [`String`]
    TruncateEventValuesError,
    /// Only truncate integer events.
    IntOnly => TRUNCATE_INT_ONLY_LEVEL
    /// Truncate all events.
    All     => TRUNCATE_ALL_LEVEL
    /// Truncate no events.
    None    => TRUNCATE_NONE_LEVEL
);

pub const MISMATCH_ERROR_LEVEL: &str = ERROR_LEVEL;
pub const MISMATCH_HEADER_WARN_LEVEL: &str = "header_warn";
pub const MISMATCH_HEADER_SILENT_LEVEL: &str = "header_silent";
pub const MISMATCH_TEXT_WARN_LEVEL: &str = "text_warn";
pub const MISMATCH_TEXT_SILENT_LEVEL: &str = "text_silent";

impl_config_flag!(
    /// Choose which offsets to use between TEXT and HEADER if they mismatch.
    ///
    /// Only applies to DATA and ANALYSIS offsets in 3.0+
    AllowHeaderTEXTOffsetMismatch,
    /// Error when parsing [`AllowHeaderTEXTOffsetMismatch`] from [`String`]
    AllowHeaderTEXTOffsetMismatchError,
    /// Throw error on mismatch.
    Error        => MISMATCH_ERROR_LEVEL
    /// Choose HEADER on mismatch and throw warning.
    HeaderWarn   => MISMATCH_HEADER_WARN_LEVEL
    /// Choose HEADER on mismatch and do nothing.
    HeaderSilent => MISMATCH_HEADER_SILENT_LEVEL
    /// Choose TEXT on mismatch and throw warning.
    TextWarn     => MISMATCH_TEXT_WARN_LEVEL
    /// Choose TEXT on mismatch and do nothing.
    TextSilent   => MISMATCH_TEXT_SILENT_LEVEL
);

impl AllowHeaderTEXTOffsetMismatch {
    /// Return bool matrix representing chosen segment and warning.
    ///
    /// First bool is true if we want HEADER, otherwise TEXT. Second boolean
    /// is true if we want a warning, false for no warning.
    ///
    /// None means throw an error and none of the above matters.
    #[must_use]
    pub fn is_warning(self) -> Option<(bool, bool)> {
        match self {
            Self::Error => None,
            Self::HeaderWarn => Some((true, true)),
            Self::HeaderSilent => Some((true, false)),
            Self::TextWarn => Some((false, true)),
            Self::TextSilent => Some((false, false)),
        }
    }
}

const GAIN_LEVEL: &str = "G";
const FILTER_LEVEL: &str = "F";
const WAVELENGTH_LEVEL: &str = "L";
const POWER_LEVEL: &str = "O";
const DET_TYPE_LEVEL: &str = "T";
const DET_VOLTAGE_LEVEL: &str = "V";
const PCNT_EMIT_LEVEL: &str = "P";
const CALIBRATION_LEVEL: &str = "CALIBRATION";
const DET_NAME_LEVEL: &str = "DET";
const TAG_LEVEL: &str = "TAG";
const FEATURE_LEVEL: &str = "FEATURE";
const ANALYTE_LEVEL: &str = "ANALYTE";

impl_str_enum!(
    /// Disallowed and ignorable optical keywords for temporal measurements.
    #[derive(PartialEq, Eq, Debug, Hash, Display)]
    #[display("{}", self.into_str())]
    #[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
    TemporalOpticalKey,
    /// Error when creating [`TemporalOpticalKey`] from [`String`]
    #[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
    #[cfg_attr(feature = "python", pyerr(crate::python::ConfigError))]
    TemporalOpticalKeyError,
    /// Ignore $PnG
    Gain            => GAIN_LEVEL
    /// Ignore $PnF
    Filter          => FILTER_LEVEL
    /// Ignore $PnL
    Wavelength      => WAVELENGTH_LEVEL
    /// Ignore $PnO
    Power           => POWER_LEVEL
    /// Ignore $PnT
    DetectorType    => DET_TYPE_LEVEL
    /// Ignore $PnV
    DetectorVoltage => DET_VOLTAGE_LEVEL
    /// Ignore $PnP
    PercentEmitted  => PCNT_EMIT_LEVEL
    /// Ignore $PnCALIBRATION
    Calibration     => CALIBRATION_LEVEL
    /// Ignore $PnDET
    DetectorName    => DET_NAME_LEVEL
    /// Ignore $PnTAG
    Tag             => TAG_LEVEL
    /// Ignore $PnFEATURE
    Feature         => FEATURE_LEVEL
    /// Ignore $PnANALYTE
    Analyte         => ANALYTE_LEVEL
);

impl TemporalOpticalKey {
    pub const TARGETS_2_0: [Self; 6] = [
        Self::DetectorType,
        Self::DetectorVoltage,
        Self::Filter,
        Self::PercentEmitted,
        Self::Power,
        Self::Wavelength,
    ];

    pub const TARGETS_3_0: [Self; 7] = [
        Self::Gain,
        Self::DetectorType,
        Self::DetectorVoltage,
        Self::Filter,
        Self::PercentEmitted,
        Self::Power,
        Self::Wavelength,
    ];

    pub const TARGETS_3_1: [Self; 8] = [
        Self::Gain,
        Self::Calibration,
        Self::DetectorType,
        Self::DetectorVoltage,
        Self::Filter,
        Self::PercentEmitted,
        Self::Power,
        Self::Wavelength,
    ];

    pub const TARGETS_3_2: [Self; 12] = [
        Self::Gain,
        Self::Analyte,
        Self::Calibration,
        Self::DetectorName,
        Self::DetectorType,
        Self::DetectorVoltage,
        Self::Feature,
        Self::Filter,
        Self::PercentEmitted,
        Self::Power,
        Self::Tag,
        Self::Wavelength,
    ];
}

// version strategy strings, the enum itself isn't defined here because it
// has more than these options and thus breaks the pattern

pub const VERSION_LATEST_LEVEL: &str = "latest";
pub const VERSION_EARLIEST_LEVEL: &str = "earliest";
pub const VERSION_LOOSE_LEVEL: &str = "loose";
pub const VERSION_STRICT_LEVEL: &str = "strict";

pub const VERSION_STRATEGY_ALL_LEVELS: [&str; 4] = [
    VERSION_LATEST_LEVEL,
    VERSION_EARLIEST_LEVEL,
    VERSION_STRICT_LEVEL,
    VERSION_LOOSE_LEVEL,
];

pub const STD_KW_REQ_LEVEL: &str = "req_only";
pub const STD_KW_OPT_LEVEL: &str = "opt_opt";
pub const STD_KW_REQ_AND_OPT_LEVEL: &str = "both";

impl_str_enum!(
    /// Choose what kind of keywords to return (required vs optional).
    #[cfg_attr(feature = "python", derive(FromPyString))]
    IncludeReqOrOpt,
    /// Error when parsing [`IncludeReqOrOpt`] from [`String`]
    #[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
    #[cfg_attr(feature = "python", pyerr(crate::python::ConfigError))]
    IncludeReqOrOptError,
    /// Return required.
    Req_ => STD_KW_REQ_LEVEL
    /// Return optional.
    Opt_ => STD_KW_OPT_LEVEL
    /// Return both.
    Both => STD_KW_REQ_AND_OPT_LEVEL
);

pub const STD_KW_ROOT_LEVEL: &str = "req_only";
pub const STD_KW_MEAS_LEVEL: &str = "opt_opt";
pub const STD_KW_ROOT_AND_MEAS_LEVEL: &str = "both";

impl_str_enum!(
    /// Choose what kind of keywords to return (required vs optional).
    #[cfg_attr(feature = "python", derive(FromPyString))]
    IncludeRootOrMeas,
    /// Error when parsing [`IncludeRootOrMeas`] from [`String`]
    #[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
    #[cfg_attr(feature = "python", pyerr(crate::python::ConfigError))]
    IncludeRootOrMeasError,
    /// Return root.
    Root => STD_KW_ROOT_LEVEL
    /// Return meas.
    Meas => STD_KW_MEAS_LEVEL
    /// Return both.
    Both => STD_KW_ROOT_AND_MEAS_LEVEL
);

pub const READ_STRATEGY_STRICT_LEVEL: &str = "strict";
pub const READ_STRATEGY_SCALPAL_LEVEL: &str = "scalpal";
pub const READ_STRATEGY_SLEDGEHAMMER_LEVEL: &str = "sledgehammer";

// TODO the docstrings here are a bit awkward since we refer to things in child
// crates implicitly
impl_str_enum!(
    /// Overall strategy to read FCS files.
    ///
    /// This is a "metaflag" which will activate individual flags in each
    /// configuration struct. The exact flags to be activated will depend on the
    /// struct. In all cases, this will activate the flags which emit warnings
    /// where applicable. If one does not desire warnings, they can be
    /// suppressed elsewhere in the config.
    ///
    /// In general, the different levels for this are a tradeoff between the ability
    /// to read events from DATA vs preserving metadata.
    #[derive(Default)]
    ReadStrategy,
    /// Error when parsing [`ReadStrategy`] from [`String`]
    ReadStrategyError,
    /// Follow the standard fully (configuration is totally default).
    ///
    /// Many files will fail this, but it is useful for validation.
    #[default]
    Strict       => READ_STRATEGY_STRICT_LEVEL
    /// Use "safe" non-compliant parsing that is unlikely to result in data loss.
    ///
    /// This is likely a good option for many files.
    Scalpal      => READ_STRATEGY_SCALPAL_LEVEL
    /// Use "unsafe" non-compliant parsing.
    ///
    /// This is the best option when all one cares about is reading DATA.
    /// Non-compliant metadata in TEXT will be skipped.
    Sledgehammer => READ_STRATEGY_SLEDGEHAMMER_LEVEL
);

// internal constants, many are shared between enums to keep the API simpler

const NONE_LEVEL: &str = "none";
const FALSE_LEVEL: &str = "false";
const TRUE_LEVEL: &str = "true";
const SILENT_LEVEL: &str = "silent";
const ERROR_LEVEL: &str = "error";
const WARN_LEVEL: &str = "warn";
const ALL_LEVEL: &str = "all";
const DEMOTE_WARN_LEVEL: &str = "demote_warn";
const DEMOTE_SILENT_LEVEL: &str = "demote_silent";
const DROP_WARN_LEVEL: &str = "drop_warn";
const DROP_SILENT_LEVEL: &str = "drop_silent";

// other config constants

pub const TIME_MEAS_NAME_PATTERN_NONE: &str = "NoTime";

pub const TIME_MEAS_NAME_PATTERN_DEFAULT: &str = "^(TIME|Time)$";

// must be valid regexp
pub const NON_STD_MEAS_PAT_DEFAULT: &str = formatcp!("^P{NON_STD_MEAS_INDEX_PAT}");

pub const NON_STD_MEAS_INDEX_PAT: &str = "%n";

pub const DEDUP_PNN_SEP: char = '~';

// the "%b" format is case-insensitive so this should work for "Jan", "JAN",
// "jan", "jaN", etc
pub const DEFAULT_DATE_FORMAT: &str = "%d-%b-%Y";

pub const DEFAULT_LAST_MODIFIED_FORMAT: &str = "%d-%b-%Y %H:%M:%S";

pub const DEFAULT_TIME_FORMAT_2_0: &str = "%H:%M:%S";

pub const DEFAULT_TIME_FORMAT_3_0: &str =
    formatcp!("{DEFAULT_TIME_FORMAT_2_0}:{BASE60_SECOND_SPEC}");

pub const DEFAULT_TIME_FORMAT_3_1: &str =
    formatcp!("{DEFAULT_TIME_FORMAT_2_0}.{BASE100_SECOND_SPEC}");

pub const BASE60_SECOND_SPEC: &str = "%!";

pub const BASE100_SECOND_SPEC: &str = "%@";

pub const PATTERN_DELIMITER: char = '/';

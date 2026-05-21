use crate::ne_str;
use crate::nonempty_string::NEStr;

use const_format::formatcp;
use derive_more::{Display, FromStr, Into};
use thiserror::Error;

use std::str::FromStr;

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{DisplayAsPyErr, FromPyString, IntoPyString, TryFromPyObject},
    pyo3::prelude::*,
};

pub trait EnumStrIter<const LEN: usize>: Sized {
    const ITEMS: [Self; LEN];

    fn as_ne_str(&self) -> &'static NEStr;

    fn as_str(&self) -> &'static str {
        self.as_ne_str().as_ref()
    }

    fn iter() -> impl Iterator<Item = Self> {
        Self::ITEMS.into_iter()
    }

    #[must_use]
    fn iter_str() -> impl Iterator<Item = &'static str> {
        Self::iter().map(|x| Self::as_str(&x))
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
    (@count) => { 0_usize };

    (@count $head:expr $(, $tail:expr)*) => {
        1_usize + impl_str_enum!(@count $($tail),*)
    };

    ($(#[$flag_meta:meta])* $flag_vis:vis $flag_name:ident,
     $(#[$error_meta:meta])* $error_vis:vis $error_name:ident,
     $($(#[$var_meta:meta])* $var:ident => $strlit:expr),+
    ) => {
        $(#[$flag_meta])*
        #[derive(Clone, Copy)]
        $flag_vis enum $flag_name {
            $(
                $(#[$var_meta])*
                $var,
            )*
        }

        impl std::str::FromStr for $flag_name {
            type Err = $error_name;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                $(
                    if $strlit.as_ref() == s {
                        return Ok(Self::$var);
                    }
                )*
                    Err($error_name(s.to_owned()))
            }
        }

        impl $crate::config::EnumStrIter<{ impl_str_enum!(@count $($var),*) }> for $flag_name {
            const ITEMS: [Self; { impl_str_enum!(@count $($var),*) }] = [$(Self::$var),*];

            fn as_ne_str(&self) -> &'static $crate::nonempty_string::NEStr {
                match self {
                    $(Self::$var => $strlit,)*
                }
            }
        }

        $(#[$error_meta])*
        #[derive(thiserror::Error, Debug)]
        $error_vis struct $error_name(String);

        impl std::fmt::Display for $error_name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
                // TODO what is this string is really really long?
                let original = &self.0;
                let all: Vec<_> = <$flag_name as $crate::config::EnumStrIter<_>>::iter_str().collect();
                let ne = nonempty_collections::NESlice::try_from_slice(&all[..])
                    .expect("macro should require at least one flag so this should never fail");
                let (last, rest) = $crate::nonempty_string::NESliceExt::split_last(&ne);
                if rest.is_empty() {
                    write!(f, "must be '{last}', got '{original}'")
                } else {
                    write!(f, "must be one of ")?;
                    for r in rest {
                        write!(f, "'{r}', ")?;
                    }
                    write!(f, "or '{last}', got '{original}'")
                }
            }
        }
    };
}

/// Make enum string enum literal to be used as a keyword value.
///
/// This will impl the enum literal and add a ToDisplayNE trait.
#[macro_export]
macro_rules! impl_str_enum_kw {
    ($(#[$flag_meta:meta])* $flag_vis:vis $flag_name:ident,
     $(#[$error_meta:meta])* $error_vis:vis $error_name:ident,
     $($(#[$var_meta:meta])* $var:ident => $strlit:expr),+
    ) => {
        impl_str_enum!(
            $(#[$flag_meta])* $flag_vis $flag_name,
            $(#[$error_meta])* $error_vis $error_name,
            $($(#[$var_meta])* $var => $strlit),*
        );

        impl $crate::nonempty_string::ToDisplayNE<'_> for $flag_name {
            type NE = &'static $crate::nonempty_string::NEStr;
            fn to_ne(&self) -> Self::NE {
                $crate::config::EnumStrIter::as_ne_str(self)
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
    ($(#[$flag_meta:meta])* $flag_vis:vis $flag_name:ident,
     $(#[$error_meta:meta])* $error_vis:vis $error_name:ident,
     $(#[$var_meta0:meta])* $var0:ident => $strlit0:expr,
     $($(#[$var_meta:meta])* $var:ident => $strlit:expr),*
    ) => {
        impl_str_enum!(
            #[derive(Display, Default)]
            #[display("{}", self.as_str())]
            #[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
            $(#[$flag_meta])* $flag_vis $flag_name,

            #[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
            #[cfg_attr(feature = "python", pyerr($crate::python::ConfigError))]
            $(#[$error_meta])* $error_vis $error_name,

            #[default]
            $(#[$var_meta0])* $var0 => $strlit0,

            $($(#[$var_meta])* $var => $strlit),*
        );
    }
}

pub const TRI_FALSE_LEVEL: &NEStr = FALSE_LEVEL;
pub const TRI_TRUE_LEVEL: &NEStr = TRUE_LEVEL;
pub const TRI_SILENT_LEVEL: &NEStr = SILENT_LEVEL;

impl_config_flag!(
    /// Tri-state flag to throw warning, throw error, or do nothing
    pub TriFlag,
    /// Error when parsing [`TriFlag`] from [`String`]
    pub TriFlagError,
    False  => TRI_FALSE_LEVEL,
    True   => TRI_TRUE_LEVEL,
    Silent => TRI_SILENT_LEVEL
);

pub const ENCODING_UTF8_LEVEL: &NEStr = ne_str!("utf8");
pub const ENCODING_SINGLE_LEVEL: &NEStr = ne_str!("single");
pub const ENCODING_GUESS_LEVEL: &NEStr = ne_str!("guess");

impl_config_flag!(
    /// Choose how to interpret the characters in TEXT.
    ///
    /// If `utf8`, use UTF8. If `single`, use IANA ISO/IEC-8859-1), which will
    /// map each byte to a character, including those outside ASCII. This is
    /// useful if TEXT is encoded with non-UTF8 characters. If `guess`, assume
    /// UTF8 and fall back to IANA ISO/IEC-8859-1 if a non-UTF8 character is
    /// encountered.
    pub UseEncoding,
    /// Error when parsing [`UseEncoding`] from [`String`]
    pub UseEncodingError,
    Utf8   =>  ENCODING_UTF8_LEVEL,
    Single =>  ENCODING_SINGLE_LEVEL,
    Guess  => ENCODING_GUESS_LEVEL
);

/// The encoding to use when reading TEXT.
#[derive(Clone, Copy, Default)]
pub enum Encoding {
    #[default]
    Utf8,
    Single,
}

impl UseEncoding {
    /// Choose encoding to use to read `bytes`.
    ///
    /// Only read `bytes` if `Guess` is selected, in which case
    /// [`Encoding::Single`] will be returned if any bytes have the most
    /// significant bit set to 1.
    #[must_use]
    pub fn choose(&self, bytes: &[u8]) -> Encoding {
        match self {
            Self::Utf8 => Encoding::Utf8,
            Self::Single => Encoding::Single,
            Self::Guess => {
                if str::from_utf8(bytes).is_ok() {
                    Encoding::Utf8
                } else {
                    Encoding::Single
                }
            }
        }
    }
}

pub const OTHER_WIDTH_NONE_LEVEL: &NEStr = NONE_LEVEL;
pub const OTHER_WIDTH_ERROR_LEVEL: &NEStr = ERROR_LEVEL;
pub const OTHER_WIDTH_WARN_LEVEL: &NEStr = WARN_LEVEL;
pub const OTHER_WIDTH_SILENT_LEVEL: &NEStr = SILENT_LEVEL;

impl_config_flag!(
    /// Choose how to guess the width for OTHER segments.
    pub GuessOtherWidth,
    /// Error when parsing [`GuessOtherWidth`] from [`String`]
    pub GuessOtherWidthError,
    None   => OTHER_WIDTH_NONE_LEVEL,
    Error  => OTHER_WIDTH_ERROR_LEVEL,
    Warn   => OTHER_WIDTH_WARN_LEVEL,
    Silent => OTHER_WIDTH_SILENT_LEVEL
);

pub const KW_ERROR_LEVEL: &NEStr = ERROR_LEVEL;
pub const KW_DEMOTE_WARN_LEVEL: &NEStr = DEMOTE_WARN_LEVEL;
pub const KW_DEMOTE_SILENT_LEVEL: &NEStr = DEMOTE_SILENT_LEVEL;
pub const KW_DROP_WARN_LEVEL: &NEStr = DROP_WARN_LEVEL;
pub const KW_DROP_SILENT_LEVEL: &NEStr = DROP_SILENT_LEVEL;

impl_config_flag!(
    /// Configuration to deal with optional standard keywords that cause errors.
    pub ProcessKeywordFailure,
    /// Error when parsing [`ProcessKeywordFailure`] from [`String`]
    pub ProcessKeywordFailureError,
    Error        => KW_ERROR_LEVEL,
    DemoteWarn   => KW_DEMOTE_WARN_LEVEL,
    DemoteSilent => KW_DEMOTE_SILENT_LEVEL,
    DropWarn     => KW_DROP_WARN_LEVEL,
    DropSilent   => KW_DROP_SILENT_LEVEL
);

pub const DELIM_ESCAPED_LEVEL: &NEStr = ne_str!("escaped");
pub const DELIM_UNESCAPED_LEVEL: &NEStr = ne_str!("unescaped");
pub const DELIM_GUESS_ESCAPED_LEVEL: &NEStr = ne_str!("guess_escaped");
pub const DELIM_GUESS_UNESCAPED_LEVEL: &NEStr = ne_str!("guess_unescaped");

impl_config_flag!(
    /// Choose how to escape delims in TEXT segment.
    pub DelimEscapeMode,
    /// Error when parsing [`DelimEscapeMode`] from [`String`]
    pub DelimEscapeModeError,
    /// Use escaped delimiters.
    Escaped        => DELIM_ESCAPED_LEVEL,
    /// Use unescaped delimiters.
    Unescaped      => DELIM_UNESCAPED_LEVEL,
    /// Guess      => falling back to escaped mode.
    GuessEscaped   => DELIM_GUESS_ESCAPED_LEVEL,
    /// Guess      => falling back to unescaped mode.
    GuessUnescaped => DELIM_GUESS_UNESCAPED_LEVEL
);

pub const TRIM_NONE_LEVEL: &NEStr = ne_str!("notrim");
pub const TRIM_ERROR_LEVEL: &NEStr = ne_str!("trim");
pub const TRIM_BLANK_WARN_LEVEL: &NEStr = ne_str!("trim_blank_warn");
pub const TRIM_BLANK_SILENT_LEVEL: &NEStr = ne_str!("trim_blank_silent");

impl_config_flag!(
    /// Choose how to trim values and deal with blanks that may result.
    pub TrimValueWhitespace,
    /// Error when parsing [`TrimValueWhitespace`] from [`String`]
    pub TrimValueWhitespaceError,
    /// Do not trim at all.
    Notrim          => TRIM_NONE_LEVEL,
    /// Trim whitespace and throw error if blank is created.
    Trim            => TRIM_ERROR_LEVEL,
    /// Trim whitespace and throw warning if blank is created.
    TrimBlankWarn   => TRIM_BLANK_WARN_LEVEL,
    /// Trim whitespace and do nothing if blank is created.
    TrimBlankSilent => TRIM_BLANK_SILENT_LEVEL
);

pub const FORCE_LINEAR_NONE_LEVEL: &NEStr = NONE_LEVEL;
pub const FORCE_LINEAR_TIME_LEVEL: &NEStr = ne_str!("time_only");
pub const FORCE_LINEAR_NON_INT_LEVEL: &NEStr = ne_str!("all_non_int");
pub const FORCE_LINEAR_ALL_LEVEL: &NEStr = ALL_LEVEL;

impl_config_flag!(
    /// Choose which $PnE to force as linear.
    pub ForceLinearScale,
    /// Error when parsing [`ForceLinearScale`] from [`String`]
    pub ForceLinearScaleError,
    /// Do not force.
    None      => FORCE_LINEAR_NONE_LEVEL,
    /// Only force the temporal measurement.
    TimeOnly  => FORCE_LINEAR_TIME_LEVEL,
    /// Force all non-integer measurements and temporal measurement.
    AllNonInt => FORCE_LINEAR_NON_INT_LEVEL,
    /// Force all measurements.
    All       => FORCE_LINEAR_ALL_LEVEL
);

impl ForceLinearScale {
    #[must_use]
    pub fn time_selected(self) -> bool {
        !matches!(self, Self::None)
    }
}

pub const TMP_OPT_DEMOTE_WARN_LEVEL: &NEStr = DEMOTE_WARN_LEVEL;
pub const TMP_OPT_DEMOTE_SILENT_LEVEL: &NEStr = DEMOTE_SILENT_LEVEL;
pub const TMP_OPT_DROP_WARN_LEVEL: &NEStr = DROP_WARN_LEVEL;
pub const TMP_OPT_DROP_SILENT_LEVEL: &NEStr = DROP_SILENT_LEVEL;

impl_config_flag!(
    /// Choose what to do with optical keys in time measurement when found.
    pub ProcessTemporalOpticalKeys,
    /// Error when parsing [`ForceLinearScale`] from [`String`]
    pub ProcessTemporalOpticalKeysError,
    /// Demote to nonstandard with warning
    DemoteWarn   => TMP_OPT_DEMOTE_WARN_LEVEL,
    /// Demote to nonstandard with no warning
    DemoteSilent => TMP_OPT_DEMOTE_SILENT_LEVEL,
    /// Drop with warning
    DropWarn     => TMP_OPT_DROP_WARN_LEVEL,
    /// Drop with no warning
    DropSilent   => TMP_OPT_DROP_SILENT_LEVEL
);

pub const SPILLOVER_NAMED_LEVEL: &NEStr = ne_str!("named");
pub const SPILLOVER_INDEXED_LEVEL: &NEStr = ne_str!("indexed");
pub const SPILLOVER_GUESS_LEVEL: &NEStr = ne_str!("guess");

impl_config_flag!(
    /// Choose how to parse measurements for $SPILLOVER key
    pub SpilloverMeasurementMode,
    /// Error when parsing [`ForceLinearScale`] from [`String`]
    pub SpilloverMeasurementModeError,
    /// Interpret measurements as names which match $PnN.
    Named   => SPILLOVER_NAMED_LEVEL,
    /// Interpret measurements as 1-indices (numbers) which point to measurements.
    Indexed => SPILLOVER_INDEXED_LEVEL,
    /// Guess how measurements should be interpreted.
    ///
    /// If they are all numbers and all do not point to $PnN, interpret as
    /// indices, otherwise names.
    Guess   => SPILLOVER_GUESS_LEVEL
);

pub const OVER_LIMIT_ACTION_ERROR_LEVEL: &NEStr = ERROR_LEVEL;
pub const OVER_LIMIT_ACTION_WARN_LEVEL: &NEStr = WARN_LEVEL;
pub const OVER_LIMIT_ACTION_SILENT_LEVEL: &NEStr = SILENT_LEVEL;
pub const OVER_LIMIT_ACTION_TRUNCATE_SILENT_LEVEL: &NEStr = ne_str!("trunc_silent");
pub const OVER_LIMIT_ACTION_TRUNCATE_WARN_LEVEL: &NEStr = ne_str!("trunc_warn");

impl_str_enum!(
    #[derive(Display)]
    #[display("{}", self.as_str())]
    #[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
    pub OverLimitAction,
    /// Error when parsing [`OverLimitAction`] from [`String`]
    #[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
    #[cfg_attr(feature = "python", pyerr(crate::python::ConfigError))]
    pub OverLimitActionError,
    /// Warn for values over limit
    Warn => OVER_LIMIT_ACTION_WARN_LEVEL,
    /// Error for values over limit.
    Error => OVER_LIMIT_ACTION_ERROR_LEVEL,
    /// Do nothing.
    Silent => OVER_LIMIT_ACTION_SILENT_LEVEL,
    /// Truncate and throw warning.
    TruncateSilent => OVER_LIMIT_ACTION_TRUNCATE_SILENT_LEVEL,
    /// Truncate and throw warning.
    TruncateWarn => OVER_LIMIT_ACTION_TRUNCATE_WARN_LEVEL
);

/// Choose what to do with values that exceed $PnR.
#[derive(Display, FromStr, Clone, Copy)]
#[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
pub struct OverRangeAction(pub OverLimitAction);

impl Default for OverRangeAction {
    fn default() -> Self {
        Self(OverLimitAction::Warn)
    }
}

/// Choose what to do with integer values that exceed their bitmask set by $PnR.
#[derive(Display, FromStr, Clone, Copy)]
#[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
pub struct OverBitmaskAction(pub OverLimitAction);

impl Default for OverBitmaskAction {
    fn default() -> Self {
        Self(OverLimitAction::TruncateWarn)
    }
}

/// The action that will be used to deal with values that exceed a range.
#[derive(Debug, Clone, Copy)]
pub enum OverLimitMode {
    /// Do nothing.
    None,
    /// Truncate values that are over and emit warning or error
    Truncate,
    /// Scan for values that are over and emit warning or error.
    ScanOnly,
}

impl OverLimitAction {
    #[must_use]
    pub fn mode(&self) -> OverLimitMode {
        match self {
            Self::Error | Self::Warn => OverLimitMode::ScanOnly,
            Self::TruncateSilent | Self::TruncateWarn => OverLimitMode::Truncate,
            Self::Silent => OverLimitMode::None,
        }
    }
}

pub const MISMATCH_ERROR_LEVEL: &NEStr = ERROR_LEVEL;
pub const MISMATCH_HEADER_WARN_LEVEL: &NEStr = ne_str!("header_warn");
pub const MISMATCH_HEADER_SILENT_LEVEL: &NEStr = ne_str!("header_silent");
pub const MISMATCH_TEXT_WARN_LEVEL: &NEStr = ne_str!("text_warn");
pub const MISMATCH_TEXT_SILENT_LEVEL: &NEStr = ne_str!("text_silent");

impl_config_flag!(
    /// Choose which offsets to use between TEXT and HEADER if they mismatch.
    ///
    /// Only applies to DATA and ANALYSIS offsets in 3.0+
    pub AllowHeaderTEXTOffsetMismatch,
    /// Error when parsing [`AllowHeaderTEXTOffsetMismatch`] from [`String`]
    pub AllowHeaderTEXTOffsetMismatchError,
    /// Throw error on mismatch.
    Error        => MISMATCH_ERROR_LEVEL,
    /// Choose HEADER on mismatch and throw warning.
    HeaderWarn   => MISMATCH_HEADER_WARN_LEVEL,
    /// Choose HEADER on mismatch and do nothing.
    HeaderSilent => MISMATCH_HEADER_SILENT_LEVEL,
    /// Choose TEXT on mismatch and throw warning.
    TextWarn     => MISMATCH_TEXT_WARN_LEVEL,
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

const GAIN_LEVEL: &NEStr = ne_str!("G");
const FILTER_LEVEL: &NEStr = ne_str!("F");
const WAVELENGTH_LEVEL: &NEStr = ne_str!("L");
const POWER_LEVEL: &NEStr = ne_str!("O");
const DET_TYPE_LEVEL: &NEStr = ne_str!("T");
const DET_VOLTAGE_LEVEL: &NEStr = ne_str!("V");
const PCNT_EMIT_LEVEL: &NEStr = ne_str!("P");
const CALIBRATION_LEVEL: &NEStr = ne_str!("CALIBRATION");
const DET_NAME_LEVEL: &NEStr = ne_str!("DET");
const TAG_LEVEL: &NEStr = ne_str!("TAG");
const FEATURE_LEVEL: &NEStr = ne_str!("FEATURE");
const ANALYTE_LEVEL: &NEStr = ne_str!("ANALYTE");

impl_str_enum!(
    /// Disallowed and ignorable optical keywords for temporal measurements.
    #[derive(PartialEq, Eq, Debug, Hash, Display)]
    #[display("{}", self.as_str())]
    #[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
    pub TemporalOpticalKey,
    /// Error when creating [`TemporalOpticalKey`] from [`String`]
    #[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
    #[cfg_attr(feature = "python", pyerr(crate::python::ConfigError))]
    pub TemporalOpticalKeyError,
    /// Ignore $PnG
    Gain            => GAIN_LEVEL,
    /// Ignore $PnF
    Filter          => FILTER_LEVEL,
    /// Ignore $PnL
    Wavelength      => WAVELENGTH_LEVEL,
    /// Ignore $PnO
    Power           => POWER_LEVEL,
    /// Ignore $PnT
    DetectorType    => DET_TYPE_LEVEL,
    /// Ignore $PnV
    DetectorVoltage => DET_VOLTAGE_LEVEL,
    /// Ignore $PnP
    PercentEmitted  => PCNT_EMIT_LEVEL,
    /// Ignore $PnCALIBRATION
    Calibration     => CALIBRATION_LEVEL,
    /// Ignore $PnDET
    DetectorName    => DET_NAME_LEVEL,
    /// Ignore $PnTAG
    Tag             => TAG_LEVEL,
    /// Ignore $PnFEATURE
    Feature         => FEATURE_LEVEL,
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

pub const STD_KW_REQ_LEVEL: &NEStr = ne_str!("req_only");
pub const STD_KW_OPT_LEVEL: &NEStr = ne_str!("opt_opt");
pub const STD_KW_REQ_AND_OPT_LEVEL: &NEStr = ne_str!("both");

impl_str_enum!(
    /// Choose what kind of keywords to return (required vs optional).
    #[cfg_attr(feature = "python", derive(FromPyString))]
    pub IncludeReqOrOpt,
    /// Error when parsing [`IncludeReqOrOpt`] from [`String`]
    #[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
    #[cfg_attr(feature = "python", pyerr(crate::python::ConfigError))]
    pub IncludeReqOrOptError,
    /// Return required.
    Req_ => STD_KW_REQ_LEVEL,
    /// Return optional.
    Opt_ => STD_KW_OPT_LEVEL,
    /// Return both.
    Both => STD_KW_REQ_AND_OPT_LEVEL
);

pub const STD_KW_ROOT_LEVEL: &NEStr = ne_str!("req_only");
pub const STD_KW_MEAS_LEVEL: &NEStr = ne_str!("opt_opt");
pub const STD_KW_ROOT_AND_MEAS_LEVEL: &NEStr = ne_str!("both");

impl_str_enum!(
    /// Choose what kind of keywords to return (required vs optional).
    #[cfg_attr(feature = "python", derive(FromPyString))]
    pub IncludeRootOrMeas,
    /// Error when parsing [`IncludeRootOrMeas`] from [`String`]
    #[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
    #[cfg_attr(feature = "python", pyerr(crate::python::ConfigError))]
    pub IncludeRootOrMeasError,
    /// Return root.
    Root => STD_KW_ROOT_LEVEL,
    /// Return meas.
    Meas => STD_KW_MEAS_LEVEL,
    /// Return both.
    Both => STD_KW_ROOT_AND_MEAS_LEVEL
);

pub const READ_STRATEGY_STRICT_LEVEL: &NEStr = ne_str!("strict");
pub const READ_STRATEGY_SCALPAL_LEVEL: &NEStr = ne_str!("scalpal");
pub const READ_STRATEGY_SLEDGEHAMMER_LEVEL: &NEStr = ne_str!("sledgehammer");

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
    pub ReadStrategy,
    /// Error when parsing [`ReadStrategy`] from [`String`]
    pub ReadStrategyError,
    /// Follow the standard fully (configuration is totally default).
    ///
    /// Many files will fail this, but it is useful for validation.
    #[default]
    Strict       => READ_STRATEGY_STRICT_LEVEL,
    /// Use "safe" non-compliant parsing that is unlikely to result in data loss.
    ///
    /// This is likely a good option for many files.
    Scalpal      => READ_STRATEGY_SCALPAL_LEVEL,
    /// Use "unsafe" non-compliant parsing.
    ///
    /// This is the best option when all one cares about is reading DATA.
    /// Non-compliant metadata in TEXT will be skipped.
    Sledgehammer => READ_STRATEGY_SLEDGEHAMMER_LEVEL
);

/// The size of the row buffer used to read DATA.
///
/// The minimum size is 4k.
#[derive(Clone, Copy, Into, Display)]
#[cfg_attr(feature = "python", derive(IntoPyObject, TryFromPyObject))]
pub struct RowBufferSize(usize);

impl Default for RowBufferSize {
    fn default() -> Self {
        // 90% of the most common L1D cache size which is 32k
        Self(28_000)
    }
}

/// Error when making new [`RowBufferSize`].
#[derive(Error, Debug)]
#[error("Row buffer size must be greater than {MIN_ROW_BUFFER_SIZE} bytes")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ConfigError))]
pub struct RowBufferSizeError;

impl FromStr for RowBufferSize {
    type Err = RowBufferSizeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_from(s.parse::<usize>().map_err(|_| RowBufferSizeError)?)
    }
}

impl TryFrom<usize> for RowBufferSize {
    type Error = RowBufferSizeError;
    fn try_from(value: usize) -> Result<Self, Self::Error> {
        if value < MIN_ROW_BUFFER_SIZE {
            Err(RowBufferSizeError)
        } else {
            Ok(Self(value))
        }
    }
}

const MIN_ROW_BUFFER_SIZE: usize = 4096;

// internal constants, many are shared between enums to keep the API simpler

const NONE_LEVEL: &NEStr = ne_str!("none");
const FALSE_LEVEL: &NEStr = ne_str!("false");
const TRUE_LEVEL: &NEStr = ne_str!("true");
const SILENT_LEVEL: &NEStr = ne_str!("silent");
const ERROR_LEVEL: &NEStr = ne_str!("error");
const WARN_LEVEL: &NEStr = ne_str!("warn");
const ALL_LEVEL: &NEStr = ne_str!("all");
const DEMOTE_WARN_LEVEL: &NEStr = ne_str!("demote_warn");
const DEMOTE_SILENT_LEVEL: &NEStr = ne_str!("demote_silent");
const DROP_WARN_LEVEL: &NEStr = ne_str!("drop_warn");
const DROP_SILENT_LEVEL: &NEStr = ne_str!("drop_silent");

// other config constants

pub const TIME_MEAS_NAME_PATTERN_NONE: &str = "NoTime";

pub const TIME_MEAS_NAME_PATTERN_DEFAULT: &str = "^(TIME|Time)$";

// a literal string prefix; this could also be a regexp in which case it would
// be like /regexp/ (but that would be slow, so don't use that by default)
pub const NON_STD_MEAS_PAT_DEFAULT: &str = formatcp!("P{NON_STD_MEAS_INDEX_PAT}");

pub const NON_STD_MEAS_INDEX_PAT: &str = "%n";

pub const DEDUP_PNN_SEP: char = '~';

// the "%b" format is case-insensitive so this should work for "Jan", "JAN",
// "jan", "jaN", etc
pub const DEFAULT_DATE_FORMAT: &str = "%d-%b-%Y";

pub const DEFAULT_LAST_MODIFIED_FORMAT: &str = "%d-%b-%Y %H:%M:%S";

pub const BASE_TIME_FORMAT: &str = "%H:%M:%S";

pub const DEFAULT_TIME_FORMAT_2_0: &str = BASE_TIME_FORMAT;

pub const DEFAULT_TIME_FORMAT_3_0: &str = formatcp!("{BASE_TIME_FORMAT}:{BASE60_SECOND_SPEC}");

pub const DEFAULT_TIME_FORMAT_3_1: &str = formatcp!("{BASE_TIME_FORMAT}.{BASE100_SECOND_SPEC}");

pub const BASE60_SECOND_SPEC: &str = "%!";

pub const BASE100_SECOND_SPEC: &str = "%@";

pub const PATTERN_DELIMITER: char = '/';

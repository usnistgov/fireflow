use crate::core::*;
use crate::error::*;
use crate::macros::{enum_from, enum_from_disp, match_many_to_one};
use crate::validated::pattern::*;
use crate::validated::shortname::*;
use crate::validated::standard::*;

use super::byteord::*;
use super::compensation::*;
use super::datetimes::*;
use super::float_or_int::*;
use super::index::*;
use super::keywords::*;
use super::named_vec::*;
use super::optionalkw::*;
use super::ranged_float::RangedFloatError;
use super::scale::*;
use super::spillover::*;
use super::timestamps::*;
use super::unstainedcenters::*;

use nalgebra::DMatrix;
use std::convert::Infallible;
use std::fmt;
use std::num::{ParseFloatError, ParseIntError};
use std::str::FromStr;

pub(crate) type LookupResult<V> = DeferredResult<V, LookupKeysWarning, LookupKeysError>;

pub(crate) type LookupTentative<V, E> = Tentative<V, LookupKeysWarning, E>;

pub(crate) fn lookup_meta_req<V>(kws: &mut StdKeywords) -> LookupResult<V>
where
    V: ReqMetaKey,
    ParseReqKeyError: From<<V as FromStr>::Err>,
{
    V::remove_meta_req(kws)
        .map_err(|e| e.inner_into())
        .map_err(Box::new)
        .into_deferred()
}

pub(crate) fn lookup_indexed_req<V>(kws: &mut StdKeywords, n: IndexFromOne) -> LookupResult<V>
where
    V: ReqIndexedKey,
    ParseReqKeyError: From<<V as FromStr>::Err>,
{
    V::remove_meas_req(kws, n)
        .map_err(|e| e.inner_into())
        .map_err(Box::new)
        .into_deferred()
}

pub(crate) fn lookup_meta_opt<V, E>(
    kws: &mut StdKeywords,
    dep: bool,
) -> LookupTentative<OptionalKw<V>, E>
where
    V: OptMetaKey,
    V: FromStr,
    ParseOptKeyWarning: From<<V as FromStr>::Err>,
{
    let mut x = process_opt(V::remove_meta_opt(kws));
    // TODO toggle
    if dep {
        x.push_warning(DeprecatedError::Key(DepKeyWarning(V::std())).into());
    }
    x
}

pub(crate) fn lookup_indexed_opt<V, E>(
    kws: &mut StdKeywords,
    i: IndexFromOne,
    dep: bool,
) -> LookupTentative<OptionalKw<V>, E>
where
    V: OptIndexedKey,
    ParseOptKeyWarning: From<<V as FromStr>::Err>,
{
    let mut x = process_opt(V::remove_meas_opt(kws, i));
    if dep {
        x.push_warning(DeprecatedError::Key(DepKeyWarning(V::std(i))).into());
    }
    x
}

pub(crate) fn lookup_temporal_gain_3_0(
    kws: &mut StdKeywords,
    i: IndexFromOne,
) -> LookupTentative<OptionalKw<Gain>, LookupKeysError> {
    let mut tnt_gain = lookup_indexed_opt(kws, i, false);
    tnt_gain.eval_error(|gain| {
        if gain.0.is_some() {
            Some(LookupKeysError::Misc(TemporalError::HasGain.into()))
        } else {
            None
        }
    });
    tnt_gain
}

fn process_opt<V, E>(
    res: Result<OptionalKw<V>, ParseKeyError<<V as FromStr>::Err>>,
) -> Tentative<OptionalKw<V>, LookupKeysWarning, E>
where
    V: FromStr,
    ParseOptKeyWarning: From<<V as FromStr>::Err>,
{
    res.map_or_else(
        |e| {
            Tentative::new(
                None.into(),
                vec![LookupKeysWarning::Parse(e.inner_into())],
                vec![],
            )
        },
        Tentative::new1,
    )
}

// TODO this could be nested better
enum_from_disp!(
    pub LookupKeysError,
    [Parse, Box<ReqKeyError<ParseReqKeyError>>],
    // TODO this currently does nothing, need to add a flag to toggle these to
    // errors
    [Dep, DeprecatedError],
    [Linked, LinkedNameError],
    [Misc, LookupMiscError],
    [Deviant, DeviantError]
);

enum_from_disp!(
    pub LookupKeysWarning,
    [Parse, ParseKeyError<ParseOptKeyWarning>],
    [Relation, LookupRelationalWarning],
    [Dep, DeprecatedError]
);

enum_from_disp!(
    pub DeprecatedError,
    [Key, DepKeyWarning],
    [Value, DepValueWarning]
);

enum_from_disp!(
    /// Error encountered when parsing a required key from a string
    pub ParseReqKeyError,
    [FloatOrInt,    ParseFloatOrIntError],
    [AlphaNumType,  AlphaNumTypeError],
    [String,        Infallible],
    [Int,           ParseIntError],
    [Scale,         ScaleError],
    [TemporalScale, TemporalScaleError],
    [RangedFloat,   RangedFloatError],
    [Mode,          ModeError],
    [ByteOrd,       ParseByteOrdError],
    [Endian,        NewEndianError],
    [Shortname,     ShortnameError]
);

enum_from_disp!(
    /// Error encountered when parsing an optional key from a string
    pub ParseOptKeyWarning,
    [NumType,            NumTypeError],
    [Trigger,            TriggerError],
    [Scale,              ScaleError],
    [TemporalScale,      TemporalScaleError],
    [Float,              ParseFloatError],
    [RangedFloat,        RangedFloatError],
    [Feature,            FeatureError],
    [Wavelengths,        WavelengthsError],
    [Calibration3_1,     CalibrationError<CalibrationFormat3_1>],
    [Calibration3_2,     CalibrationError<CalibrationFormat3_2>],
    [Int,                ParseIntError],
    [String,             Infallible],
    [FCSDate,            FCSDateError],
    [FCSTime,            FCSTimeError],
    [FCSTime60,          FCSTime60Error],
    [FCSTime100,         FCSTime100Error],
    [FCSDateTime,        FCSDateTimeError],
    [ModifiedDateTime,   ModifiedDateTimeError],
    [Originality,        OriginalityError],
    [UnstainedCenter,    ParseUnstainedCenterError],
    [Mode3_2,            Mode3_2Error],
    [TemporalType,       TemporalTypeError],
    [OpticalType,        OpticalTypeError],
    [Shortname,          ShortnameError],
    [Display,            DisplayError],
    [Unicode,            UnicodeError],
    [Spillover,          ParseSpilloverError],
    [Compensation,       ParseCompError],
    [FloatOrInt,         ParseFloatOrIntError],
    [GateRegionIndex2_0, RegionGateIndexError<ParseIntError>],
    [GateRegionIndex3_0, RegionGateIndexError<MeasOrGateIndexError>],
    [GateRegionIndex3_2, RegionGateIndexError<PrefixedMeasIndexError>],
    [GateRegionWindow,   GatePairError],
    [Gating,             GatingError]
);

enum_from_disp!(
    /// Misc errors encountered when looking up keywords for standardization
    pub LookupMiscError,
    // TODO this should be a configurable warning
    [Temporal, TemporalError],
    [NamedVec, NewNamedVecError],
    [MissingTime, MissingTime]
);

/// Error triggered when time measurement is missing but required.
pub struct MissingTime(pub TimePattern);

/// Errors triggered when time measurement keyword value is invalid
// TODO add other optical keywords that shouldn't be set for time.
pub enum TemporalError {
    NonLinear,
    HasGain,
}

enum_from_disp!(
    /// Error encountered when relation between two or more keys is invalid
    pub LookupRelationalWarning,
    [Timestamp, ReversedTimestamps],
    [Datetime, ReversedDatetimes],
    [CompShape, NewCompError],
    [GateRegion, MismatchedIndexAndWindowError],
    [GateRegionLink, GateRegionLinkError],
    [GateMeasLink, GateMeasurementLinkError]
);

/// Error/warning triggered when encountering a key which is deprecated
pub struct DepKeyWarning(pub StdKey);

/// Error/warning triggered when encountering a key value which is deprecated
pub enum DepValueWarning {
    DatatypeASCII,
    ModeCorrelated,
    ModeUncorrelated,
}

pub struct MismatchedIndexAndWindowError;

impl fmt::Display for MismatchedIndexAndWindowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "values for $RnI and $RnW must both be univariate or bivariate"
        )
    }
}

impl fmt::Display for DepValueWarning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let s = match self {
            Self::DatatypeASCII => "$DATATYPE=A is deprecated",
            Self::ModeCorrelated => "$MODE=C is deprecated",
            Self::ModeUncorrelated => "$MODE=U is deprecated",
        };
        write!(f, "{s}")
    }
}

impl fmt::Display for DepKeyWarning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "deprecated key: {}", self.0)
    }
}

impl fmt::Display for MissingTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "Could not find time measurement matching {}", self.0)
    }
}

impl fmt::Display for TemporalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        // TODO include meas idx here
        match self {
            TemporalError::NonLinear => write!(f, "$PnE must be '0,0'"),
            TemporalError::HasGain => write!(f, "$PnG must not be set"),
        }
    }
}

/// Error denoting that deviant keywords were found.
// TODO add them here? the only reason we might want this is in the situation
// where we trigger an error for this (rather than a warning) in which case the
// deviant keywords are not returned along with the standardized struct and
// we want to use those deviant keywords somehow.
pub struct DeviantError;

impl fmt::Display for DeviantError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "deviant keywords found")
    }
}

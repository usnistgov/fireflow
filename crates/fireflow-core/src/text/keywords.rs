use crate::config::{AllowOptionalDropping, ReadLayoutConfig, StdTextReadConfig};
use crate::core::UnitaryKeyLossError;
use crate::logging::{
    DeferredError, DeferredSwitchableErrors, LogResult, ResultExt as _, WarningAndErrorResult,
};
use crate::macros::impl_newtype_try_from;
use crate::nonempty::FCSNonEmpty;
use crate::type_families::{impl_functor, impl_functor_common, impl_kind1};
use crate::validated::ascii_range::AsciiRangeValue;
use crate::validated::ascii_uint::UintZeroPad20;
use crate::validated::bitmask::BitmaskValue;
use crate::validated::keys::{
    AnyKey as _, BiIndex, BiIndexedKey, IndexedKey, Key, Key0, Key1, Key2, NonStdKeywords,
    StdKeywords,
};
use crate::validated::keys::{NonStdKeywordsExt as _, StdKey};
use crate::validated::nonempty_string::NonEmptyString;
use crate::validated::shortname::Shortname;

use super::byteord::{BitsOrChars, Endian, NewByteOrdError, NoByteOrd, PrivBytes, SizedByteOrd};
use super::compensation::{Compensation, NewCompError};
use super::datetimes::{BeginDateTime, EndDateTime};
use super::float_decimal::{DecimalToFloatError, FloatDecimal, HasFloatBounds};
use super::index::{GateIndex, MeasIndex, RegionIndex};
use super::lookup::{
    FromStrDelim, FromStrWith, OptIndexedKey, OptIndexedKeyError, OptMetarootKey, Optional,
    ParseKeyError, ReqIndexedKey, ReqIndexedKeyError, ReqKeyError, ReqMetarootKey, Required,
};
use super::named_vec::NameMapping;
use super::optional::{
    CheckMaybe, DisplayMaybe, KeywordPairMaybe, OptionalInt, OptionalString, OptionalZST,
};
use super::ranged_float::{NonNegFloat, PositiveFloat, RangedFloatError};
use super::relational::{
    ExistingNamedLinkError, MeasNamesNoTime, RemovedIndexLink, RemovedNamedLink,
};
use super::spillover::Spillover;
use super::timestamps::{Btim, Etim, FCSDate, FCSTime, FCSTime60, FCSTime100, Xtim};

use bigdecimal::{BigDecimal, ParseBigDecimalError};
use chrono::{NaiveDateTime, NaiveTime, Timelike as _};
use derive_more::{Add, AsMut, AsRef, Display, From, FromStr, Into, Sub};
use itertools::Itertools as _;
use nonempty::NonEmpty;
use num_traits::PrimInt;
use num_traits::cast::ToPrimitive as _;
use num_traits::identities::{One as _, Zero as _};
use thiserror::Error;

use derive_new::new;
use nalgebra::DMatrix;
use std::collections::HashMap;
use std::fmt;
use std::mem::take;
use std::num::{NonZeroU8, ParseFloatError, ParseIntError};
use std::str::FromStr;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{
        AllIntoPyErr, DisplayAsPyErr, FromInnerPyObject, FromPyString, IntoPyString,
    },
    pyo3::prelude::*,
};

/// Value for $NEXTDATA (all versions)
#[derive(From, Into, FromStr, Display, Debug, Clone, Copy)]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
pub struct Nextdata(pub UintZeroPad20);

/// The value for the $PnE key (all versions).
///
/// Format is assumed to be 'f1,f2'
#[derive(Clone, Copy, PartialEq, Debug, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum Scale {
    /// Linear scale (ie '0,0')
    #[display("0,0")]
    Linear,

    /// Log scale, where both numbers are positive
    #[display("{_0}")]
    Log(LogScale),
}

#[derive(Clone, Copy, PartialEq, Debug, Display, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[display("{decades},{offset}")]
pub struct LogScale {
    pub decades: PositiveFloat,
    pub offset: PositiveFloat,
}

impl Scale {
    pub fn try_new_log(decades: f32, offset: f32) -> Result<Self, LogRangeError> {
        (decades, offset).try_into().map(Self::Log)
    }
}

impl TryFrom<(f32, f32)> for LogScale {
    type Error = LogRangeError;

    fn try_from(value: (f32, f32)) -> Result<Self, Self::Error> {
        let (d0, o0) = value;
        if let (Ok(decades), Ok(offset)) =
            (PositiveFloat::try_from(d0), PositiveFloat::try_from(o0))
        {
            Ok(Self::new(decades, offset))
        } else {
            Err(LogRangeError::new(d0, o0))
        }
    }
}

impl FromStrWith for Scale {
    type Err = ScaleError;
    type Payload<'a> = ();

    fn from_str_with(s: &str, (): (), conf: &StdTextReadConfig) -> Result<Self, Self::Err> {
        let res = Self::from_str_delim(s, conf.trim_intra_value_whitespace);
        if conf.fix_log_scale_offsets {
            res.or_else(|e| {
                if let ScaleError::LogRange(le) = e {
                    le.try_fix_offset()
                        .map(Scale::Log)
                        .map_err(ScaleError::LogRange)
                } else {
                    Err(e)
                }
            })
        } else {
            res
        }
    }
}

impl FromStr for Scale {
    type Err = ScaleError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_str_delim(s, false)
    }
}

impl FromStrDelim for Scale {
    type Err = ScaleError;
    const DELIM: char = ',';

    fn from_iter<'a>(iter: impl Iterator<Item = &'a str>) -> Result<Self, Self::Err> {
        let xs: Vec<_> = iter.collect();
        match &xs[..] {
            [ds, os] => {
                let f1 = ds.parse().map_err(ScaleError::FloatError)?;
                let f2 = os.parse().map_err(ScaleError::FloatError)?;
                match (f1, f2) {
                    (0.0, 0.0) => Ok(Self::Linear),
                    (decades, offset) => {
                        Self::try_new_log(decades, offset).map_err(ScaleError::LogRange)
                    }
                }
            }
            _ => Err(ScaleError::WrongFormat),
        }
    }
}

/// Error when parsing $PnE from string
#[derive(Debug, Error)]
pub enum ScaleError {
    #[error("{0}")]
    FloatError(ParseFloatError),
    #[error("{0}")]
    LogRange(LogRangeError),
    #[error("must be like 'f1,f2'")]
    WrongFormat,
}

/// Error when parsing $PnE as log scale from string
#[derive(Debug, Error, new)]
#[error("decades/offset must both be positive, got '{decades},{offset}'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::InvalidKeywordValueError))]
pub struct LogRangeError {
    decades: f32,
    offset: f32,
}

impl LogRangeError {
    /// Try to 'fix' log scales which are 'X,0' where X is positive.
    ///
    /// The 'recommended' way to fix these is to make the 0 and 1, which is
    /// what this does. This is a heuristic hack to get some files to work
    /// which didn't write $PnE correctly.
    pub(crate) fn try_fix_offset(self) -> Result<LogScale, Self> {
        if self.offset.is_zero()
            && let Ok(decades) = PositiveFloat::try_from(self.decades)
        {
            return Ok(LogScale::new(decades, PositiveFloat::one()));
        }
        Err(self)
    }
}

/// The value of the $PnG keyword
#[derive(Clone, Copy, PartialEq, From, Display, FromStr, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct Gain(pub PositiveFloat);

impl Gain {
    pub(crate) fn lookup_temporal_3_0<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        i: MeasIndex,
        conf: &C,
    ) -> DeferredSwitchableErrors<Option<Self>, AllowOptionalDropping, LookupTemporalGainError>
    where
        C: AsRef<ReadLayoutConfig> + AsRef<StdTextReadConfig>,
    {
        if AsRef::<StdTextReadConfig>::as_ref(conf).ignore_time_gain {
            nonstd.transfer_demoted(std, Self::std(i));
            let flag = AsRef::<ReadLayoutConfig>::as_ref(conf).allow_optional_dropping;
            LogResult::new_switchable_ok(None, flag)
        } else {
            Self::remove_or_drop_meas_opt(std, nonstd, i, conf.as_ref())
                .map_switchable_errors(LookupTemporalGainError::from)
                .into_semigroup()
                .eval_deferred_switchable_error(|gain| {
                    (!gain.is_none_or(|g| g.0.is_one())).then_some(TemporalGainError(i).into())
                })
        }
    }
}

/// Error when parsing $PnG from string
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupTemporalGainError {
    Parse(OptIndexedKeyError<Gain>),
    HasGain(TemporalGainError),
}

/// Error triggered when time measurement has $PnG
#[derive(Debug, Error)]
#[error("{} must be 1.0 or not set for temporal measurement", Gain::std(self.0))]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::RelationalError))]
pub struct TemporalGainError(MeasIndex);

/// The value of the $TIMESTEP keyword
#[derive(Clone, Copy, PartialEq, From, Display, FromStr, Into, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
#[into(f32, PositiveFloat)]
pub struct Timestep(pub PositiveFloat);

impl_newtype_try_from!(Timestep, PositiveFloat, f32, RangedFloatError);

impl Default for Timestep {
    fn default() -> Self {
        Self(PositiveFloat::one())
    }
}

impl Timestep {
    pub(crate) fn loss_error(self) -> Option<UnitaryKeyLossError<Self>> {
        (!self.0.is_one()).then_some(UnitaryKeyLossError::default())
    }
}

/// The value of the $VOL keyword
#[derive(Clone, Copy, From, Display, FromStr, Into, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
#[into(NonNegFloat, f32)]
pub struct Vol(pub NonNegFloat);

impl_newtype_try_from!(Vol, NonNegFloat, f32, RangedFloatError);

/// The value of the $TR field (all versions)
///
/// This is formatted as 'string,f' where 'string' is a measurement name.
#[derive(Clone, PartialEq, Display, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[display("{measurement},{threshold}")]
pub struct Trigger {
    /// The measurement name (assumed to match a '$PnN' value).
    pub measurement: Shortname,

    /// The threshold of the trigger.
    pub threshold: u32,
}

impl Trigger {
    pub(crate) fn reassign(&mut self, mapping: &NameMapping) {
        if let Some(new) = mapping.get(&self.measurement) {
            self.measurement = (*new).clone();
        }
    }

    pub(crate) fn remove_invalid_links(
        src: &mut Option<Self>,
        names: &MeasNamesNoTime,
    ) -> Option<RemovedNamedLink<Self>> {
        let tr = src.as_ref()?;
        if names.as_ref().contains(&tr.measurement) {
            None
        } else {
            // ASSUME this won't fail since we filter out None above with ?
            let m = tr.measurement.clone();
            Some(RemovedNamedLink::new(take(src).unwrap(), NonEmpty::new(m)))
        }
    }
}

impl FromStrWith for Trigger {
    type Err = TriggerError;
    type Payload<'a> = ();

    fn from_str_with(s: &str, (): (), conf: &StdTextReadConfig) -> Result<Self, Self::Err> {
        Self::from_str_delim(s, conf.trim_intra_value_whitespace)
    }
}

impl FromStr for Trigger {
    type Err = TriggerError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_str_delim(s, false)
    }
}

impl FromStrDelim for Trigger {
    type Err = TriggerError;
    const DELIM: char = ',';

    fn from_iter<'a>(iter: impl Iterator<Item = &'a str>) -> Result<Self, Self::Err> {
        let xs: Vec<_> = iter.collect();
        match &xs[..] {
            [p, n1] => n1
                .parse()
                .map_err(TriggerError::IntFormat)
                .map(|threshold| Self {
                    measurement: Shortname::new_unchecked(p),
                    threshold,
                }),
            _ => Err(TriggerError::WrongFieldNumber),
        }
    }
}

/// Error when parsing $TR from string
#[derive(Debug, Error)]
pub enum TriggerError {
    #[error("must be like 'string,f'")]
    WrongFieldNumber,
    #[error("{0}")]
    IntFormat(ParseIntError),
}

/// The values used for the $MODE key (up to 3.1)
#[derive(Clone, PartialEq, Eq, Default, Display, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
pub enum Mode {
    #[default]
    #[display("L")]
    List,
    #[display("U")]
    Uncorrelated,
    #[display("C")]
    Correlated,
}

/// Error when $MODE has a deprecated value (FCS 3.1)
#[derive(Debug, Error)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::FCSDeprecatedError))]
pub enum DeprecatedModeWarning {
    #[error("$MODE=C is deprecated")]
    ModeCorrelated,
    #[error("$MODE=U is deprecated")]
    ModeUncorrelated,
}

/// Error when parsing $MODE from string (up to 3.1)
#[derive(Debug, Error)]
#[error("must be one of 'C', 'L', or 'U'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ParseKeywordValueError))]
pub struct ModeError;

impl FromStr for Mode {
    type Err = ModeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "C" => Ok(Self::Correlated),
            "L" => Ok(Self::List),
            "U" => Ok(Self::Uncorrelated),
            _ => Err(ModeError),
        }
    }
}

/// The value for the $MODE key, which can only contain 'L' (3.2)
#[derive(Clone, PartialEq, Display, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
#[display("L")]
pub struct Mode3_2;

impl FromStr for Mode3_2 {
    type Err = Mode3_2Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "L" => Ok(Self),
            _ => Err(Mode3_2Error),
        }
    }
}

impl TryFrom<Mode> for Mode3_2 {
    type Error = ModeUpgradeError;

    fn try_from(value: Mode) -> Result<Self, Self::Error> {
        match value {
            Mode::List => Ok(Self),
            _ => Err(ModeUpgradeError),
        }
    }
}

/// Error when parsing $MODE from string (3.2)
#[derive(Debug, Error)]
#[error("can only be 'L'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ParseKeywordValueError))]
pub struct Mode3_2Error;

/// Error when converting $MODE from pre-3.2 to 3.2
#[derive(Debug, Error)]
#[error("$MODE must be 'L'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ConversionError))]
pub struct ModeUpgradeError;

/// The value for the $PnD key (3.1+)
#[derive(Clone, Copy, PartialEq, Debug, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum Display {
    /// Linear display (value like 'Linear,<lower>,<upper>')
    #[display("Linear,{lower},{upper}")]
    Lin { lower: f32, upper: f32 },

    /// Logarithmic display (value like 'Logarithmic,<offset>,<decades>')
    #[display("Logarithmic,{decades},{offset}")]
    Log {
        offset: PositiveFloat,
        decades: PositiveFloat,
    },
}

impl FromStr for Display {
    type Err = DisplayError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.split(',').collect::<Vec<_>>()[..] {
            [which, s1, s2] => {
                let f1 = s1.parse().map_err(DisplayError::FloatError)?;
                let f2 = s2.parse().map_err(DisplayError::FloatError)?;
                match which {
                    "Linear" => {
                        if f1 > f2 {
                            Err(DisplayError::Linear(f1, f2))
                        } else {
                            Ok(Self::Lin {
                                lower: f1,
                                upper: f2,
                            })
                        }
                    }
                    "Logarithmic" => match (f1.try_into(), f2.try_into()) {
                        (Ok(decades), Ok(offset)) => Ok(Self::Log { decades, offset }),
                        _ => Err(DisplayError::Log(f1, f2)),
                    },
                    _ => Err(DisplayError::InvalidType),
                }
            }
            _ => Err(DisplayError::FormatError),
        }
    }
}

/// Error when parsing $PnD from string
#[derive(Debug, Error)]
pub enum DisplayError {
    #[error("{0}")]
    FloatError(ParseFloatError),
    #[error("Type must be either 'Logarithmic' or 'Linear'")]
    InvalidType,
    #[error("must be like 'string,f1,f2'")]
    FormatError,
    #[error("linear bounds out of order, got 'Linear,{0},{1}'")]
    Linear(f32, f32),
    #[error("log must only use positive floats, got 'Logarithmic,{0},{1}'")]
    Log(f32, f32),
}

/// The three values for the $PnDATATYPE keyword (3.2+)
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Display, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
pub enum NumType {
    #[display("I")]
    Integer,
    #[display("F")]
    Float,
    #[display("D")]
    Double,
}

impl FromStr for NumType {
    type Err = NumTypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "I" => Ok(Self::Integer),
            "F" => Ok(Self::Float),
            "D" => Ok(Self::Double),
            _ => Err(NumTypeError),
        }
    }
}

/// Error when parsing $PnDATATYPE from string
#[derive(Debug, Error)]
#[error("must be one of 'F', 'D', or 'A'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ParseKeywordValueError))]
pub struct NumTypeError;

/// The $BYTEORD field in FCS 2.0 and 3.0
///
/// This must be a list of integers belonging to the unordered set {1..N} where
/// N is the total number of bytes. The numbers will be stored as one less the
/// displayed integers to make array indexing easier.
#[derive(Clone, Copy, From, Display, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum ByteOrd2_0 {
    O1(SizedByteOrd<1>),
    O2(SizedByteOrd<2>),
    O3(SizedByteOrd<3>),
    O4(SizedByteOrd<4>),
    O5(SizedByteOrd<5>),
    O6(SizedByteOrd<6>),
    O7(SizedByteOrd<7>),
    O8(SizedByteOrd<8>),
}

impl FromStr for ByteOrd2_0 {
    type Err = ParseByteOrdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (pass, fail): (Vec<_>, Vec<_>) =
            s.split(',').map(str::parse::<NonZeroU8>).partition_result();
        if fail.is_empty() {
            Self::try_from(&pass[..]).map_err(ParseByteOrdError::Order)
        } else {
            Err(ParseByteOrdError::Digit(ByteordDigitError))
        }
    }
}

/// Error when parsing $BYTEORD from string (2.0/3.0)
#[derive(From, Debug, Display, Error)]
pub enum ParseByteOrdError {
    Order(NewByteOrdError),
    Digit(ByteordDigitError),
}

/// Error when $BYTEORD has invalid digit(s)
#[derive(Debug, Error)]
#[error("could not parse digits from byte order")]
pub struct ByteordDigitError;

impl Default for ByteOrd2_0 {
    fn default() -> Self {
        // Default $BYTEORD for FCS 2.0 is simply 32-bit little endian
        Self::O4(SizedByteOrd::default())
    }
}

impl From<NoByteOrd<true>> for ByteOrd2_0 {
    fn from(_: NoByteOrd<true>) -> Self {
        Self::default()
    }
}

impl ByteOrd2_0 {
    #[must_use]
    pub(crate) fn nbytes(&self) -> PrivBytes {
        match self {
            Self::O1(_) => SizedByteOrd::<1>::nbytes(),
            Self::O2(_) => SizedByteOrd::<2>::nbytes(),
            Self::O3(_) => SizedByteOrd::<3>::nbytes(),
            Self::O4(_) => SizedByteOrd::<4>::nbytes(),
            Self::O5(_) => SizedByteOrd::<5>::nbytes(),
            Self::O6(_) => SizedByteOrd::<6>::nbytes(),
            Self::O7(_) => SizedByteOrd::<7>::nbytes(),
            Self::O8(_) => SizedByteOrd::<8>::nbytes(),
        }
    }
}

/// The $BYTEORD field in FCS 3.1 and 3.2
#[derive(Clone, Copy, From, Display, FromStr, Default, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct ByteOrd3_1(pub Endian);

impl From<NoByteOrd<false>> for ByteOrd3_1 {
    fn from(_: NoByteOrd<false>) -> Self {
        Self::default()
    }
}

/// The four allowed values for the $DATATYPE keyword.
#[derive(Clone, Copy, Eq, PartialEq, PartialOrd, Ord, Hash, Debug, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
pub enum AlphaNumType {
    #[display("A")]
    Ascii,
    #[display("I")]
    Integer,
    #[display("F")]
    Float,
    #[display("D")]
    Double,
}

macro_rules! check_ascii {
    ($res:expr) => {
        if let Ok(dt) = $res
            && dt == Self::Ascii
        {
            let w = Some(DeprecatedDatatypeWarning);
            $res.into_log().set_commutative_warnings(w)
        } else {
            $res.into_log()
        }
    };
}

pub(crate) type LookupDatatypeResult<T> =
    WarningAndErrorResult<T, (), DeprecatedDatatypeWarning, ReqKeyError<T>>;

impl AlphaNumType {
    pub(crate) fn get_req_check_ascii(kws: &StdKeywords) -> LookupDatatypeResult<Self> {
        let res = Self::get_metaroot_req(kws);
        check_ascii!(res)
    }

    pub(crate) fn remove_req_check_ascii(kws: &mut StdKeywords) -> LookupDatatypeResult<Self> {
        let res = Self::remove_metaroot_req(kws);
        check_ascii!(res)
    }
}

impl FromStr for AlphaNumType {
    type Err = AlphaNumTypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "I" => Ok(Self::Integer),
            "F" => Ok(Self::Float),
            "D" => Ok(Self::Double),
            "A" => Ok(Self::Ascii),
            _ => Err(AlphaNumTypeError),
        }
    }
}

/// Error when $DATATYPE is ASCII which is deprecated in 3.1 and 3.2
#[derive(Debug, Error)]
#[error("$DATATYPE=A is deprecated")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::FCSDeprecatedError))]
pub struct DeprecatedDatatypeWarning;

/// Error when parsing $DATATYPE from string
#[derive(Debug, Error)]
#[error("must be one of 'I', 'F', 'D', or 'A'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ParseKeywordValueError))]
pub struct AlphaNumTypeError;

impl From<NumType> for AlphaNumType {
    fn from(value: NumType) -> Self {
        match value {
            NumType::Integer => Self::Integer,
            NumType::Float => Self::Float,
            NumType::Double => Self::Double,
        }
    }
}

impl TryFrom<AlphaNumType> for NumType {
    type Error = ();
    fn try_from(value: AlphaNumType) -> Result<Self, Self::Error> {
        match value {
            AlphaNumType::Integer => Ok(Self::Integer),
            AlphaNumType::Float => Ok(Self::Float),
            AlphaNumType::Double => Ok(Self::Double),
            AlphaNumType::Ascii => Err(()),
        }
    }
}

/// The value of the $PnE key for temporal measurements (all versions)
///
/// This can only be linear (0,0)
#[derive(Clone, PartialEq, Display, Debug, Default)]
#[display("0,0")]
pub struct TemporalScaleInner;

impl FromStr for TemporalScaleInner {
    type Err = TemporalScaleError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.parse::<Scale>() {
            Ok(Scale::Linear) => Ok(Self),
            _ => Err(TemporalScaleError),
        }
    }
}

/// The value of the $PnE key for temporal measurements (3.0+)
#[derive(Clone, PartialEq, Display, Debug, Default, FromStr)]
pub struct TemporalScale3_0(pub TemporalScaleInner);

impl TemporalScale3_0 {
    pub(crate) fn lookup(
        kws: &mut StdKeywords,
        i: MeasIndex,
        nonstd: &mut NonStdKeywords,
        conf: &StdTextReadConfig,
    ) -> Result<(), ReqIndexedKeyError<Self>> {
        if conf.force_time_linear {
            nonstd.transfer_demoted(kws, TemporalScale2_0::std(i));
            Ok(())
        } else {
            Self::remove_meas_req(kws, i).map(|_| ())
        }
    }
}

impl DisplayMaybe for TemporalScale3_0 {
    fn display_maybe(&self) -> Option<String> {
        Some(self.0.to_string())
    }
}

impl KeywordPairMaybe for TemporalScale3_0 {
    type Inner = Self;
}

/// Error when parsing $PnE for temporal measurement (which must always be '0,0')
#[derive(Debug, Error)]
#[error("time measurement must have linear scaling")]
pub struct TemporalScaleError;

/// The value for the $PnCALIBRATION key (3.1 only)
///
/// This should be formatted like '<value>,<unit>'
#[derive(Clone, PartialEq, Debug, Display, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[display("{slope},{unit}")]
pub struct Calibration3_1 {
    pub slope: PositiveFloat,
    pub unit: String,
}

impl FromStr for Calibration3_1 {
    type Err = CalibrationError<CalibrationFormat3_1>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.split(',').collect::<Vec<_>>()[..] {
            [value, unit] => {
                let slope = value.parse().map_err(CalibrationError::Range)?;
                Ok(Self::new(slope, String::from(unit)))
            }
            _ => Err(CalibrationError::Format(CalibrationFormat3_1)),
        }
    }
}

/// Error when $PnCALIBRATION has invalid string format for 3.1
#[derive(Debug, Error)]
#[error("must be like 'f,string'")]
pub struct CalibrationFormat3_1;

#[derive(Debug, Display, Error)]
pub enum CalibrationError<C> {
    Float(ParseFloatError),
    Range(RangedFloatError),
    Format(C),
}

impl From<Calibration3_1> for Calibration3_2 {
    fn from(value: Calibration3_1) -> Self {
        Self::new(value.slope, 0.0, value.unit)
    }
}

/// The value for the $PnCALIBRATION key (3.2+)
///
/// This should be formatted like '<value>,[<offset>,]<unit>' and differs from
/// 3.1 with the optional inclusion of "offset" (assumed 0 if not included).
#[derive(Clone, PartialEq, Debug, Display, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[display("{slope},{offset},{unit}")]
pub struct Calibration3_2 {
    pub slope: PositiveFloat,
    pub offset: f32,
    pub unit: String,
}

impl FromStr for Calibration3_2 {
    type Err = CalibrationError<CalibrationFormat3_2>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (slope, offset, unit) = match s.split(',').collect::<Vec<_>>()[..] {
            [slope, unit] => Ok((slope, 0.0, unit)),
            [slope, soffset, unit] => {
                let f2 = soffset.parse().map_err(CalibrationError::Float)?;
                Ok((slope, f2, unit))
            }
            _ => Err(CalibrationError::Format(CalibrationFormat3_2)),
        }?;
        Ok(Self {
            slope: slope.parse().map_err(CalibrationError::Range)?,
            offset,
            unit: unit.into(),
        })
    }
}

/// Error when $PnCALIBRATION has invalid string format for 3.2
#[derive(Debug, Error)]
#[error("must be like 'f1,[f2],string'")]
pub struct CalibrationFormat3_2;

impl Calibration3_2 {
    pub(crate) fn into_3_1(
        self,
        i: MeasIndex,
    ) -> DeferredError<Calibration3_1, CalibrationLossError> {
        let ret = Calibration3_1::new(self.slope, self.unit);
        let e = (!self.offset.is_zero()).then_some(CalibrationLossError(i, self.offset));
        DeferredError::new_deferred_maybe(ret, e)
    }
}

/// Error when converting $PnCALIBRATION from 3.2 to 3.1
///
/// Loss will occur if the offset is specified, which is not applicable to 3.1
#[derive(Debug, Error)]
#[error(
    "{k} has offset {o} which will be lost upon conversion",
    k = Calibration3_2::std(self.0),
    o = self.1,
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ConversionError))]
pub struct CalibrationLossError(MeasIndex, f32);

/// The value for the $PnL key (2.0/3.0).
#[derive(Clone, Copy, From, FromStr, Display, Into, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
#[into(f32, PositiveFloat)]
pub struct Wavelength(pub PositiveFloat);

impl_newtype_try_from!(Wavelength, PositiveFloat, f32, RangedFloatError);

impl From<Wavelength> for Wavelengths {
    fn from(value: Wavelength) -> Self {
        Self(vec![value.0])
    }
}

/// The value for the $PnL key (3.1).
///
/// Starting in 3.1 this is a vector rather than a scaler.
#[derive(Clone, From, PartialEq, Debug, Default)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
pub struct Wavelengths(pub Vec<PositiveFloat>);

impl DisplayMaybe for Wavelengths {
    fn display_maybe(&self) -> Option<String> {
        if self.0.is_empty() {
            None
        } else {
            Some(self.0.iter().join(","))
        }
    }
}

impl KeywordPairMaybe for Wavelengths {
    type Inner = Self;
}

impl CheckMaybe for Wavelengths {
    type Inner = Self;
}

impl From<Wavelengths> for Vec<f32> {
    fn from(value: Wavelengths) -> Self {
        value.0.into_iter().map(Into::into).collect()
    }
}

impl FromStr for Wavelengths {
    type Err = WavelengthsError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_str_delim(s, false)
    }
}

impl FromStrWith for Wavelengths {
    type Err = WavelengthsError;
    type Payload<'a> = ();

    fn from_str_with(s: &str, (): (), conf: &StdTextReadConfig) -> Result<Self, Self::Err> {
        Self::from_str_delim(s, conf.trim_intra_value_whitespace)
    }
}

impl FromStrDelim for Wavelengths {
    type Err = WavelengthsError;
    const DELIM: char = ',';

    fn from_iter<'a>(iter: impl Iterator<Item = &'a str>) -> Result<Self, Self::Err> {
        let xs = NonEmpty::collect(iter).ok_or(WavelengthsError::Empty)?;
        let ys = xs.try_map(|x| x.parse().map_err(WavelengthsError::Num))?;
        Ok(Self(ys.into()))
    }
}

impl Wavelengths {
    pub(crate) fn into_wavelength(
        self,
        i: MeasIndex,
    ) -> DeferredError<Option<Wavelength>, WavelengthsLossError> {
        NonEmpty::from_vec(self.0).map_or(LogResult::new_ok(None), |ws| {
            let n = ws.len();
            let k = Key1::new_i1(i.into());
            let e = WavelengthsLossError(k, n);
            LogResult::new_deferred_if(n == 1, Some(Wavelength(ws.head)), e)
        })
    }
}

/// Error when converting $PnL from 3.1/3.2 to 2.0/3.0
///
/// Loss may occur in this case because $PnL in later versions allows multiple
/// numbers and earlier versions only allow one.
#[derive(Debug, Error)]
#[error(
    "{0} is {1} elements long and will \
     be reduced to first upon conversion"
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ConversionError))]
pub struct WavelengthsLossError(Key1<Wavelengths>, usize);

/// Error when parsing $PnL from string
#[derive(Debug, Error)]
pub enum WavelengthsError {
    #[error("{0}")]
    Num(RangedFloatError),
    #[error("list must not be empty")]
    Empty,
}

/// A datetime as used in the $LAST_MODIFIED key (3.1+ only)
///
/// Inner value is private to ensure it always gets parsed/printed using the
/// correct format
#[derive(Clone, Copy, From, Into, PartialEq, Debug, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
#[display("{}.{:02}", _0.format(DATETIME_FMT), _0.nanosecond() / 10_000_000)]
pub struct LastModified(pub NaiveDateTime);

const DATETIME_FMT: &str = "%d-%b-%Y %H:%M:%S";

impl FromStr for LastModified {
    type Err = LastModifiedError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (t, cc) = match &s.split('.').collect::<Vec<_>>()[..] {
            [t] => (*t, ""),
            [t, cc] => (*t, *cc),
            _ => return Err(LastModifiedError),
        };
        NaiveDateTime::parse_from_str(t, DATETIME_FMT)
            .or(Err(LastModifiedError))
            .and_then(|dt| {
                if cc.is_empty() {
                    Ok(dt)
                } else {
                    let tt = cc.parse::<u32>().or(Err(LastModifiedError))?;
                    if tt > 100 {
                        Err(LastModifiedError)
                    } else {
                        dt.with_nanosecond(tt * 10_000_000).ok_or(LastModifiedError)
                    }
                }
            })
            .map(Self)
    }
}

/// Error when parsing $LAST_MODIFIED from string
#[derive(Debug, Error)]
#[error("must be like 'dd-mmm-yyyy hh:mm:ss[.cc]'")]
pub struct LastModifiedError;

/// The value for the $ORIGINALITY key (3.1+)
#[derive(Clone, Copy, PartialEq, Debug, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
pub enum Originality {
    #[display("Original")]
    Original,
    #[display("NonDataModified")]
    NonDataModified,
    #[display("Appended")]
    Appended,
    #[display("DataModified")]
    DataModified,
}

impl FromStr for Originality {
    type Err = OriginalityError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Original" => Ok(Self::Original),
            "NonDataModified" => Ok(Self::NonDataModified),
            "Appended" => Ok(Self::Appended),
            "DataModified" => Ok(Self::DataModified),
            _ => Err(OriginalityError),
        }
    }
}

/// Error when parsing $ORIGINALITY from string
#[derive(Debug, Error)]
#[error("must be one of 'Original', 'NonDataModified', 'Appended', or 'DataModified'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ParseKeywordValueError))]
pub struct OriginalityError;

/// The value of the $COMP keyword (3.0 only)
#[derive(Clone, From, Into, Display, AsRef, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromInnerPyObject))]
#[as_ref(DMatrix<f32>, Compensation)]
pub struct Compensation3_0(pub Compensation);

// TODO check that nrows/columns = PAR
impl FromStrWith for Compensation3_0 {
    type Err = ParseCompError;
    type Payload<'a> = ();

    fn from_str_with(s: &str, (): (), conf: &StdTextReadConfig) -> Result<Self, Self::Err> {
        Self::from_str_delim(s, conf.trim_intra_value_whitespace)
    }
}

impl FromStr for Compensation3_0 {
    type Err = ParseCompError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_str_delim(s, false)
    }
}

impl FromStrDelim for Compensation3_0 {
    type Err = ParseCompError;
    const DELIM: char = ',';

    fn from_iter<'a>(mut iter: impl Iterator<Item = &'a str>) -> Result<Self, Self::Err> {
        if let Some(first) = iter.next().and_then(|x| x.parse::<usize>().ok()) {
            let n = first;
            let nn = n * n;
            let values: Vec<_> = iter.by_ref().take(nn).collect();
            let remainder = iter.by_ref().count();
            let total = values.len() + remainder;
            if total == nn {
                if let Ok(fvalues) = values
                    .into_iter()
                    .map(str::parse::<f32>)
                    .collect::<Result<Vec<_>, _>>()
                {
                    let matrix = DMatrix::from_row_iterator(n, n, fvalues);
                    Ok(Compensation::try_from(matrix).map(Self)?)
                } else {
                    Err(ParseCompError::BadFloat)
                }
            } else {
                Err(ParseCompError::WrongLength {
                    expected: nn,
                    total,
                })
            }
        } else {
            Err(ParseCompError::BadLength)
        }
    }
}

impl Compensation3_0 {
    pub(crate) fn remove_invalid_link(
        src: &mut Option<Self>,
        par: Par,
    ) -> Option<RemovedIndexLink<Self>> {
        let c = src.as_ref()?;
        let m: &DMatrix<_> = c.as_ref();
        let js = (par.0..m.nrows()).map(MeasIndex::from);
        NonEmpty::collect(js).map(|xs| {
            // ASSUME this won't fail because we filter with ? above
            let v = take(src).unwrap();
            RemovedIndexLink::new(v, xs)
        })
    }
}

/// Error when parsing $COMP from string
#[derive(Debug, Error)]
pub enum ParseCompError {
    #[error("Expected {expected} entries, found {total}")]
    WrongLength { total: usize, expected: usize },
    #[error("Could not determine length")]
    BadLength,
    #[error("Float could not be parsed")]
    BadFloat,
    #[error("{0}")]
    New(#[from] NewCompError),
}

/// The value of the $UNICODE key (3.0 only)
///
/// Formatted like 'codepage,[keys]'. This key is not actually used for anything
/// in this library and is present to be complete. The original purpose was to
/// indicate keywords which supported UTF-8, but these days it is hard to
/// write a library that does NOT support UTF-8 ;)
#[derive(Clone, PartialEq, Debug, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[display("{page},{}", kws.iter().join(","))]
pub struct Unicode {
    pub page: u32,
    pub kws: Vec<String>,
}

impl FromStrWith for Unicode {
    type Err = UnicodeError;
    type Payload<'a> = ();

    fn from_str_with(s: &str, (): (), conf: &StdTextReadConfig) -> Result<Self, Self::Err> {
        Self::from_str_delim(s, conf.trim_intra_value_whitespace)
    }
}

impl FromStr for Unicode {
    type Err = UnicodeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_str_delim(s, false)
    }
}

impl FromStrDelim for Unicode {
    type Err = UnicodeError;
    const DELIM: char = ',';

    fn from_iter<'a>(mut iter: impl Iterator<Item = &'a str>) -> Result<Self, Self::Err> {
        if let Some(page) = iter.next().and_then(|x| x.parse().ok()) {
            let kws: Vec<String> = iter.map(String::from).collect();
            if kws.is_empty() {
                Err(UnicodeError::Empty)
            } else {
                Ok(Self { page, kws })
            }
        } else {
            Err(UnicodeError::BadFormat)
        }
    }
}

/// Error when parsing $UNICODE from string
#[derive(Debug, Error)]
pub enum UnicodeError {
    #[error("No keywords given")]
    Empty,
    #[error("Must be like 'n,string,[[string],...]'")]
    BadFormat,
}

/// The value of the $PnTYPE key in optical channels (3.2+)
#[derive(Clone, PartialEq, Debug, Default)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromPyString))]
pub struct OpticalType(pub OptionalString);

/// Error when parsing $PnTYPE from string for optical measurement
#[derive(Debug, Error)]
#[error("$PnTYPE for time measurement shall not be 'Time' if given")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ParseKeywordValueError))]
pub struct OpticalTypeError;

const TIME: &str = "Time";

impl FromStr for OpticalType {
    type Err = OpticalTypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            TIME => Err(OpticalTypeError),
            _ => Ok(Self(s.to_owned().into())),
        }
    }
}

/// The value of the $PnTYPE key in temporal channels (3.2+)
#[derive(Clone, PartialEq, Debug, Display, Default)]
#[display("{}", TIME)]
pub struct TemporalTypeInner;

impl FromStr for TemporalTypeInner {
    type Err = TemporalTypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            TIME => Ok(Self),
            _ => Err(TemporalTypeError),
        }
    }
}

/// Error when parsing $PnTYPE from string for temporal measurement
#[derive(Debug, Error)]
#[error("$PnTYPE for time measurement shall be 'Time' if given")]
pub struct TemporalTypeError;

/// The value of the $PnFEATURE key (3.2+)
#[derive(Clone, Copy, PartialEq, Debug, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
pub enum Feature {
    #[display("{}", AREA)]
    Area,
    #[display("{}", WIDTH)]
    Width,
    #[display("{}", HEIGHT)]
    Height,
}

const AREA: &str = "Area";
const WIDTH: &str = "Width";
const HEIGHT: &str = "Height";

impl FromStr for Feature {
    type Err = FeatureError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            AREA => Ok(Self::Area),
            WIDTH => Ok(Self::Width),
            HEIGHT => Ok(Self::Height),
            _ => Err(FeatureError),
        }
    }
}

/// Error when parsing $PnFEATURE from string
#[derive(Debug, Error)]
#[error("must be one of 'Area', 'Width', or 'Height'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ParseKeywordValueError))]
pub struct FeatureError;

/// The value of the $RnI key (all versions)
#[derive(Clone, Copy, Display, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum RegionGateIndex<I> {
    Univariate(I),
    Bivariate(IndexPair<I>),
}

/// The two indices of a bivariate gate
#[derive(Clone, Copy, PartialEq, Display, Debug, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[display("{x},{y}")]
pub struct IndexPair<I> {
    pub x: I,
    pub y: I,
}

impl_kind1!(IndexPairFamily, IndexPair);
impl_functor!(IndexPair, self, mut f, IndexPair::new(f(self.x), f(self.y)));

impl<I> IndexPair<I> {
    pub(crate) fn try_map<F, J, E>(self, mut f: F) -> Result<IndexPair<J>, E>
    where
        F: FnMut(I, I) -> Result<(J, J), E>,
    {
        let (x, y) = f(self.x, self.y)?;
        Ok(IndexPair { x, y })
    }
}

impl<I: FromStr> FromStrWith for RegionGateIndex<I> {
    type Err = RegionGateIndexError<<I as FromStr>::Err>;
    type Payload<'a> = ();

    fn from_str_with(s: &str, (): (), conf: &StdTextReadConfig) -> Result<Self, Self::Err> {
        Self::from_str_delim(s, conf.trim_intra_value_whitespace)
    }
}

impl<I> FromStr for RegionGateIndex<I>
where
    I: FromStr,
{
    type Err = RegionGateIndexError<<I as FromStr>::Err>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_str_delim(s, false)
    }
}

impl<I: FromStr> FromStrDelim for RegionGateIndex<I> {
    type Err = RegionGateIndexError<<I as FromStr>::Err>;
    const DELIM: char = ',';

    fn from_iter<'a>(iter: impl Iterator<Item = &'a str>) -> Result<Self, Self::Err> {
        let xs: Vec<_> = iter.collect();
        match &xs[..] {
            [x] => x
                .parse()
                .map(RegionGateIndex::Univariate)
                .map_err(RegionGateIndexError::Int),
            [x, y] => x
                .parse()
                .and_then(|a| y.parse().map(|b| Self::Bivariate(IndexPair { x: a, y: b })))
                .map_err(RegionGateIndexError::Int),
            _ => Err(RegionGateIndexError::Format),
        }
    }
}

/// Error when parsing $RnI from string
#[derive(Debug, Error)]
pub enum RegionGateIndexError<E> {
    #[error("{0}")]
    Int(E),
    #[error("must be either a single value 'x' or a pair 'x,y'")]
    Format,
}

#[derive(Clone, Copy, From, PartialEq, Display, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
pub enum MeasOrGateIndex {
    #[display("P{_0}")]
    Meas(MeasIndex),
    #[display("G{_0}")]
    Gate(GateIndex),
}

impl FromStr for MeasOrGateIndex {
    type Err = MeasOrGateIndexError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some((prefix, rest)) = s.split_at_checked(1) {
            match prefix {
                "P" => rest
                    .parse::<MeasIndex>()
                    .map(Into::into)
                    .map_err(MeasOrGateIndexError::Int),
                "G" => rest
                    .parse::<GateIndex>()
                    .map(Into::into)
                    .map_err(MeasOrGateIndexError::Int),
                _ => Err(MeasOrGateIndexError::Format),
            }
        } else {
            Err(MeasOrGateIndexError::Format)
        }
    }
}

/// Error when parsing $RnI index from string (3.0/3.1)
#[derive(Debug, Error)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ParseKeywordValueError))]
pub enum MeasOrGateIndexError {
    #[error("{0}")]
    Int(ParseIntError),
    #[error("must be prefixed with either 'P' or 'G'")]
    Format,
}

/// Index for $RnI (3.2)
///
/// This is just a measurement index with 'P' in front of it
#[derive(Clone, Copy, From, PartialEq, Into, AsMut, Debug, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
#[from(MeasIndex, usize)]
#[into(MeasIndex, usize)]
#[display("P{_0}")]
pub struct PrefixedMeasIndex(pub MeasIndex);

impl FromStr for PrefixedMeasIndex {
    type Err = PrefixedMeasIndexError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some((prefix, rest)) = s.split_at_checked(1) {
            match prefix {
                "P" => rest.parse().map_err(PrefixedMeasIndexError::Int).map(Self),
                _ => Err(PrefixedMeasIndexError::Format),
            }
        } else {
            Err(PrefixedMeasIndexError::Format)
        }
    }
}

/// Error when parsing $RnI index from string (3.2)
#[derive(Debug, Error)]
pub enum PrefixedMeasIndexError {
    #[error("{0}")]
    Int(ParseIntError),
    #[error("must be prefixed with 'P'")]
    Format,
}

/// The value of the $RnW key (3.0-3.2)
///
/// This is meant to be used internally to construct a higher-level abstraction
/// over the gating keywords.
#[derive(Display, Debug, PartialEq)]
pub enum RegionWindow {
    #[display("{_0}")]
    Univariate(UniGate),
    #[display("{}", _0.iter().join(";"))]
    Bivariate(NonEmpty<Vertex>),
}

#[derive(Clone, PartialEq, Display, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[display("{x},{y}")]
pub struct Vertex {
    pub x: BigDecimal,
    pub y: BigDecimal,
}

#[derive(Clone, PartialEq, Display, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[display("{lower},{upper}")]
pub struct UniGate {
    pub lower: BigDecimal,
    pub upper: BigDecimal,
}

impl FromStrWith for RegionWindow {
    type Err = GatePairError;
    type Payload<'a> = ();

    fn from_str_with(s: &str, (): (), conf: &StdTextReadConfig) -> Result<Self, Self::Err> {
        Self::from_str_delim(s, conf.trim_intra_value_whitespace)
    }
}

impl FromStr for RegionWindow {
    type Err = GatePairError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_str_delim(s, false)
    }
}

impl FromStrDelim for RegionWindow {
    type Err = GatePairError;
    const DELIM: char = ';';

    fn from_str_delim(s: &str, trim_whitespace: bool) -> Result<Self, Self::Err> {
        let it = s.split(Self::DELIM);
        if trim_whitespace {
            Self::from_iter(it.map(str::trim))
        } else {
            Self::from_iter_inner(
                it,
                |x| UniGate::from_str_delim(x, false),
                |x| Vertex::from_str_delim(x, false),
            )
        }
    }

    fn from_iter<'a>(iter: impl Iterator<Item = &'a str>) -> Result<Self, Self::Err> {
        Self::from_iter_inner(
            iter,
            |x| UniGate::from_str_delim(x, true),
            |x| Vertex::from_str_delim(x, true),
        )
    }
}

impl RegionWindow {
    fn from_iter_inner<'a, F, G>(
        ss: impl Iterator<Item = &'a str>,
        go_uni: F,
        go_bi: G,
    ) -> Result<Self, GatePairError>
    where
        F: FnOnce(&str) -> Result<UniGate, GatePairError>,
        G: Fn(&str) -> Result<Vertex, GatePairError>,
    {
        if let Some(xs) = NonEmpty::collect(ss) {
            if xs.tail.is_empty() {
                go_uni(xs.head).map(RegionWindow::Univariate)
            } else {
                xs.try_map(go_bi).map(Self::Bivariate)
            }
        } else {
            // this will happen if the input string is empty
            Err(GatePairError::Format)
        }
    }
}

impl FromStrDelim for UniGate {
    type Err = GatePairError;
    const DELIM: char = ',';

    fn from_iter<'a>(iter: impl Iterator<Item = &'a str>) -> Result<Self, Self::Err> {
        parse_pair(iter).map(|(lower, upper)| Self { lower, upper })
    }
}

impl FromStrDelim for Vertex {
    type Err = GatePairError;
    const DELIM: char = ',';

    fn from_iter<'a>(iter: impl Iterator<Item = &'a str>) -> Result<Self, Self::Err> {
        parse_pair(iter).map(|(x, y)| Self { x, y })
    }
}

fn parse_pair<'a>(
    ss: impl Iterator<Item = &'a str>,
) -> Result<(BigDecimal, BigDecimal), GatePairError> {
    let xs: Vec<_> = ss.collect();
    match &xs[..] {
        [a, b] => a
            .parse()
            .and_then(|x| b.parse().map(|y| (x, y)))
            .map_err(GatePairError::Num),
        _ => Err(GatePairError::Format),
    }
}

/// Error when parsing an $RnI keyword which has a pari of indices
#[derive(Debug, Error)]
pub enum GatePairError {
    #[error("{0}")]
    Num(ParseBigDecimalError),
    #[error("must be a string like 'f1,f2;[f3,f4;...]'")]
    Format,
}

/// The value of the $GATING key (3.0-3.2)
#[derive(Clone, PartialEq, Display, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
pub enum Gating {
    #[display("R{_0}")]
    Region(RegionIndex),
    #[display("(NOT {_0})")]
    Not(Box<Gating>),
    #[display("({_0} AND {_1})")]
    And(Box<Gating>, Box<Gating>),
    #[display("({_0} OR {_1})")]
    Or(Box<Gating>, Box<Gating>),
}

impl Gating {
    pub(crate) fn region_indices(&self) -> NonEmpty<RegionIndex> {
        let xs = match self {
            Self::Region(x) => NonEmpty::new(*x),
            Self::Not(x) => Self::region_indices(x),
            Self::And(x, y) | Self::Or(x, y) => {
                let mut acc = Self::region_indices(x);
                acc.extend(Self::region_indices(y));
                acc
            }
        };
        FCSNonEmpty::from(xs).unique().0
    }
}

impl FromStr for Gating {
    type Err = GatingError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_ascii() {
            let mut it = tokenize_gating(s);
            match_tokens(&mut it, 0)
        } else {
            Err(GatingError::NonAscii)
        }
    }
}

fn match_tokens(
    rest: &mut impl Iterator<Item = GatingToken>,
    depth: u32,
) -> Result<Gating, GatingError> {
    if let Some(this) = rest.next() {
        match this {
            GatingToken::LParen => match_tokens_new_expr(rest, depth + 1),
            GatingToken::Not => {
                let inner = match_tokens_new_expr(rest, depth)?;
                let new = Gating::Not(Box::new(inner));
                match_tokens_extend_expr(new, rest, depth)
            }
            GatingToken::Region(r) => {
                let new = Gating::Region(r);
                match_tokens_extend_expr(new, rest, depth)
            }
            _ => Err(GatingError::InvalidExprToken),
        }
    } else {
        Err(GatingError::Empty)
    }
}

/// Start a new expression if next token is valid.
///
/// This inclues:
/// - (blabla...
/// - NOT blabla...
/// - RX blabla...
fn match_tokens_new_expr(
    rest: &mut impl Iterator<Item = GatingToken>,
    depth: u32,
) -> Result<Gating, GatingError> {
    if let Some(this) = rest.next() {
        match this {
            GatingToken::LParen => {
                let inner = match_tokens_new_expr(rest, depth + 1)?;
                match_tokens_extend_expr(inner, rest, depth + 1)
            }
            GatingToken::Not => {
                let inner = match_tokens_new_expr(rest, depth)?;
                Ok(Gating::Not(Box::new(inner)))
            }
            GatingToken::Region(r) => Ok(Gating::Region(r)),
            _ => Err(GatingError::InvalidExprToken),
        }
    } else {
        Err(GatingError::ExpectedExpr)
    }
}

/// Extend current expression
fn match_tokens_extend_expr(
    acc: Gating,
    rest: &mut impl Iterator<Item = GatingToken>,
    depth: u32,
) -> Result<Gating, GatingError> {
    if let Some(this) = rest.next() {
        match this {
            GatingToken::And => {
                let right = match_tokens_new_expr(rest, depth)?;
                let new = Gating::And(Box::new(acc), Box::new(right));
                match_tokens_extend_expr(new, rest, depth)
            }
            GatingToken::Or => {
                let right = match_tokens_new_expr(rest, depth)?;
                let new = Gating::Or(Box::new(acc), Box::new(right));
                match_tokens_extend_expr(new, rest, depth)
            }
            GatingToken::RParen => {
                if depth > 0 {
                    match_tokens_extend_expr(acc, rest, depth - 1)
                } else {
                    Err(GatingError::ExtraParen)
                }
            }
            _ => Err(GatingError::InvalidOpToken),
        }
    } else if depth == 0 {
        Ok(acc)
    } else {
        Err(GatingError::MissingParen)
    }
}

fn tokenize_gating(s: &str) -> impl Iterator<Item = GatingToken> {
    s.split(['.', ' ']).filter(|x| !x.is_empty()).flat_map(|x| {
        x.split('(').flat_map(|y| {
            if y.is_empty() {
                vec![GatingToken::LParen]
            } else {
                y.split(')')
                    .map(|z| {
                        if z.is_empty() {
                            GatingToken::RParen
                        } else {
                            match z {
                                "NOT" => GatingToken::Not,
                                "AND" => GatingToken::And,
                                "OR" => GatingToken::Or,
                                _ => match z.split_at(1) {
                                    ("R", rest) => {
                                        rest.parse().map_or(GatingToken::Other, GatingToken::Region)
                                    }
                                    _ => GatingToken::Other,
                                },
                            }
                        }
                    })
                    .collect()
            }
        })
    })
}

#[derive(Debug)]
enum GatingToken {
    RParen,
    LParen,
    Region(RegionIndex),
    And,
    Or,
    Not,
    Other,
}

/// Error when parsing the $GATING keyword
#[derive(Debug, Error)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ParseKeywordValueError))]
pub enum GatingError {
    #[error("gating string is empty")]
    Empty,
    #[error("expected expression which evaluates to a region")]
    ExpectedExpr,
    #[error("must be like 'f,string'")]
    InvalidOpToken,
    #[error("expected 'AND', 'OR', or ')'")]
    InvalidExprToken,
    #[error("extra ')' encountered")]
    ExtraParen,
    #[error("must be like 'f,string'")]
    MissingParen,
    #[error("gating contains invalid bytes")]
    NonAscii,
}

/// The value for the $PnB key (all versions)
///
/// The $PnB key actually stores bits. However, this library only supports
/// widths that are multiples of 8 (ie bytes). Therefore, this key actually
/// stores the number of bytes indicated by $PnB.
///
/// This may also be '*' which means "delimited ASCII" which is only valid when
/// $DATATYPE=A.
#[derive(Clone, Copy, PartialEq, Eq, Hash, From, Debug, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[from(Chars)]
pub enum Width {
    #[display("{_0}")]
    Fixed(BitsOrChars),
    #[display("*")]
    Variable,
}

/// The value of the $PnR key.
#[derive(Clone, From, Display, FromStr, Add, Sub, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
#[from(u8, u16, u32, u64, BigDecimal)]
pub struct Range(pub BigDecimal);

impl Range {
    pub(crate) fn into_uint<T>(self) -> DeferredError<BitmaskValue<T>, RangeToIntError<()>>
    where
        T: TryFrom<Self, Error = RangeToIntError<T>> + PrimInt,
    {
        (self - Self::from(1_u8))
            .into_uint_inner()
            .map_deferred_value(BitmaskValue)
    }

    pub(crate) fn into_ascii_uint(self) -> DeferredError<AsciiRangeValue, RangeToIntError<()>> {
        self.into_uint_inner::<u64>()
            .map_deferred_value(AsciiRangeValue)
    }

    fn into_uint_inner<T>(self) -> DeferredError<T, RangeToIntError<()>>
    where
        T: TryFrom<Self, Error = RangeToIntError<T>> + PrimInt,
    {
        let (b, err) = self.try_into().map_or_else(
            |e: RangeToIntError<T>| match e.error_kind {
                RangeToIntErrorKind::Overrange => (T::max_value(), Some(e.void())),
                RangeToIntErrorKind::Underrange => (T::zero(), Some(e.void())),
                RangeToIntErrorKind::PrecisionLoss(y) => (y, Some(e.void())),
            },
            |x| (x, None),
        );
        LogResult::new_deferred_maybe(b, err)
    }

    pub(crate) fn into_float<T>(self) -> DeferredError<FloatDecimal<T>, DecimalToFloatError>
    where
        FloatDecimal<T>: TryFrom<BigDecimal, Error = DecimalToFloatError>,
        T: HasFloatBounds,
    {
        let (x, err) = FloatDecimal::try_from(self.0).map_or_else(
            |e| {
                let m = if e.over {
                    T::max_decimal()
                } else {
                    T::min_decimal()
                };
                (m, Some(e))
            },
            |x| (x, None),
        );
        LogResult::new_deferred_maybe(x, err)
    }
}

macro_rules! try_from_range_int {
    ($inttype:ident, $to:ident, $ut:ident) => {
        impl TryFrom<Range> for $inttype {
            type Error = RangeToIntError<$inttype>;

            fn try_from(value: Range) -> Result<Self, Self::Error> {
                let x = &value.0;
                let err = |error_kind| RangeToIntError {
                    dest_type: UintType::$ut,
                    src_value: x.clone(),
                    error_kind,
                };
                if let Some(y) = x.$to() {
                    if x.fractional_digit_count() <= 0 {
                        Ok(y)
                    } else {
                        Err(err(RangeToIntErrorKind::PrecisionLoss(y)))
                    }
                } else {
                    if BigDecimal::from($inttype::MAX) < *x {
                        Err(err(RangeToIntErrorKind::Overrange))
                    } else {
                        Err(err(RangeToIntErrorKind::Underrange))
                    }
                }
            }
        }
    };
}

try_from_range_int!(u8, to_u8, U8);
try_from_range_int!(u16, to_u16, U16);
try_from_range_int!(u32, to_u32, U32);
try_from_range_int!(u64, to_u64, U64);

/// Error when converting $PnR to integer.
///
/// This is a helper type to make more specific errors and not meant for
/// external use.
#[derive(Debug)]
pub struct RangeToIntError<T> {
    pub(crate) dest_type: UintType,
    pub(crate) src_value: BigDecimal,
    pub(crate) error_kind: RangeToIntErrorKind<T>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum UintType {
    U8,
    U16,
    U32,
    U64,
}

impl From<UintType> for PrivBytes {
    fn from(value: UintType) -> Self {
        match value {
            UintType::U8 => Self::B1,
            UintType::U16 => Self::B2,
            UintType::U32 => Self::B4,
            UintType::U64 => Self::B8,
        }
    }
}

#[derive(Debug)]
pub(crate) enum RangeToIntErrorKind<T> {
    Overrange,
    Underrange,
    PrecisionLoss(T),
}

impl<T> RangeToIntError<T> {
    pub(crate) fn void(self) -> RangeToIntError<()> {
        RangeToIntError {
            dest_type: self.dest_type,
            src_value: self.src_value,
            error_kind: match self.error_kind {
                RangeToIntErrorKind::Overrange => RangeToIntErrorKind::Overrange,
                RangeToIntErrorKind::Underrange => RangeToIntErrorKind::Underrange,
                RangeToIntErrorKind::PrecisionLoss(_) => RangeToIntErrorKind::PrecisionLoss(()),
            },
        }
    }
}

impl TryFrom<f32> for Range {
    type Error = ParseBigDecimalError;
    fn try_from(value: f32) -> Result<Self, Self::Error> {
        value.try_into().map(Self)
    }
}

impl TryFrom<f64> for Range {
    type Error = ParseBigDecimalError;
    fn try_from(value: f64) -> Result<Self, Self::Error> {
        value.try_into().map(Self)
    }
}

/// The value of the $GmN key
#[derive(Clone, From, Display, FromStr, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
pub struct GateShortname(pub Shortname);

/// The value of the $GmR key
#[derive(Clone, From, Display, FromStr, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
#[from(u64)]
pub struct GateRange(pub Range);

macro_rules! impl_non_neg_float {
    ($(#[$meta:meta])* $t:ident) => {
        $(#[$meta])*
        #[derive(Clone, Copy, From, Display, FromStr, Into, PartialEq, Debug)]
        #[cfg_attr(feature = "serde", derive(Serialize))]
        #[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
        #[into(NonNegFloat, f32)]
        pub struct $t(pub NonNegFloat);

        impl_newtype_try_from!($t, NonNegFloat, f32, RangedFloatError);
    };
}

impl_non_neg_float! {
    /// The value of the $PnO key.
    Power
}

impl_non_neg_float! {
    /// The value of the $PnP key.
    PercentEmitted
}

impl_non_neg_float! {
    /// The value of the $PnV key.
    DetectorVoltage
}

impl_non_neg_float! {
    /// The value of the $GmV key.
    GateDetectorVoltage
}

impl_non_neg_float! {
    /// The value of the $GmP key.
    GatePercentEmitted
}

/// The value of the $GmE key
#[derive(Clone, Copy, Display, FromStr, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
pub struct GateScale(pub Scale);

// use the same fix we use for PnE here
impl FromStrWith for GateScale {
    type Err = ScaleError;
    type Payload<'a> = ();

    fn from_str_with(s: &str, data: (), conf: &StdTextReadConfig) -> Result<Self, Self::Err> {
        Scale::from_str_with(s, data, conf).map(Self)
    }
}

/// The value of the $CYT key (3.2).
///
/// This is not a normal string because it is required in 3.2 and thus cannot
/// be empty.
#[derive(Clone, Display, FromStr, PartialEq, Into, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
pub struct Cyt3_2(pub NonEmptyString);

impl From<Cyt3_2> for Cyt {
    fn from(value: Cyt3_2) -> Self {
        Self(OptionalString(value.0.into()))
    }
}

impl TryFrom<Cyt> for Cyt3_2 {
    type Error = NoCytError;

    fn try_from(value: Cyt) -> Result<Self, Self::Error> {
        (value.0).0.parse().map_err(|_| NoCytError)
    }
}

#[derive(Debug, Error)]
#[error("$CYT is missing")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ConversionError))]
pub struct NoCytError;

/// The value for the $UNSTAINEDCENTERS key (3.2+)
#[derive(Clone, Into, PartialEq, Debug, Default)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
pub struct UnstainedCenters(pub HashMap<Shortname, f32>);

/// Error when parsing $UNSTAINEDCENTERS from string
#[derive(Debug, Error)]
pub enum ParseUnstainedCenterError {
    #[error("Names are not unique")]
    NonUnique,
    #[error("Expected {expected} values, found {total}")]
    BadLength { total: usize, expected: usize },
    #[error("Could not parse N")]
    BadN,
    #[error("Error parsing float value(s)")]
    BadFloat,
}

impl UnstainedCenters {
    pub(crate) fn reassign(&mut self, mapping: &NameMapping) {
        // keys can't be mutated in place so need to rebuild the hashmap with
        // new keys from the mapping
        let new: HashMap<_, _> = self
            .0
            .iter()
            .map(|(k, v)| {
                (
                    mapping.get(k).map(|x| (*x).clone()).unwrap_or(k.clone()),
                    *v,
                )
            })
            .collect();
        self.0 = new;
    }

    pub(crate) fn names_difference(
        &self,
        names: &MeasNamesNoTime,
    ) -> impl Iterator<Item = &Shortname> {
        self.0.keys().filter(|n| !names.as_ref().contains(n))
    }

    pub(crate) fn existing_link_error(
        &self,
        names: &MeasNamesNoTime,
    ) -> Option<ExistingNamedLinkError<Self, ()>> {
        NonEmpty::collect(self.names_difference(names).cloned())
            .map(|js| ExistingNamedLinkError::new(Key0::default(), js))
    }

    pub(crate) fn remove_invalid_links(
        &mut self,
        names: &MeasNamesNoTime,
    ) -> Option<RemovedNamedLink<Self>> {
        let ns = self.names_difference(names).cloned();
        NonEmpty::collect(ns).map(|xs| RemovedNamedLink::new(take(self), xs))
    }
}

impl FromStrWith for UnstainedCenters {
    type Err = ParseUnstainedCenterError;
    type Payload<'a> = ();

    fn from_str_with(s: &str, (): (), conf: &StdTextReadConfig) -> Result<Self, Self::Err> {
        Self::from_str_delim(s, conf.trim_intra_value_whitespace)
    }
}

impl FromStr for UnstainedCenters {
    type Err = ParseUnstainedCenterError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_str_delim(s, false)
    }
}

impl FromStrDelim for UnstainedCenters {
    type Err = ParseUnstainedCenterError;
    const DELIM: char = ',';

    fn from_iter<'a>(mut iter: impl Iterator<Item = &'a str>) -> Result<Self, Self::Err> {
        // NOTE the standard does not say if this is allowed to be empty or not
        // (ie the string "0") so do not enforce here. However, if empty we will
        // not save the keyword when writing the file.
        if let Some(n) = iter.next().and_then(|x| x.parse().ok()) {
            // This should be safe since we are splitting by commas
            let measurements: Vec<_> = iter
                .by_ref()
                .take(n)
                .map(Shortname::new_unchecked)
                .collect();
            if measurements.iter().unique().count() < measurements.len() {
                return Err(ParseUnstainedCenterError::NonUnique);
            }
            let values: Vec<_> = iter
                .by_ref()
                .take(n)
                .map(str::parse::<f32>)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| ParseUnstainedCenterError::BadFloat)?;
            let remainder = iter.by_ref().count();
            let total = values.len() + measurements.len() + remainder;
            let expected = 2 * n;
            if total == expected {
                let ys = measurements.into_iter().zip(values).collect();
                Ok(Self(ys))
            } else {
                Err(ParseUnstainedCenterError::BadLength { total, expected })
            }
        } else {
            Err(ParseUnstainedCenterError::BadN)
        }
    }
}

impl DisplayMaybe for UnstainedCenters {
    fn display_maybe(&self) -> Option<String> {
        if self.0.is_empty() {
            None
        } else {
            let n = self.0.len();
            let k = self.0.keys().join(",");
            let v = self.0.values().join(",");
            Some(format!("{n},{k},{v}"))
        }
    }
}

impl KeywordPairMaybe for UnstainedCenters {
    type Inner = Self;
}

impl CheckMaybe for UnstainedCenters {
    type Inner = Self;
}

/// Leftover standard keyword after parsing
#[derive(Clone, new, PartialEq)]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
pub struct ExtraStdKeywords {
    pub pseudostandard: StdKeywords,
    pub unused: StdKeywords,
}

impl ExtraStdKeywords {
    pub(crate) fn split_2_0(kws: StdKeywords) -> Self {
        Self::split_inner(kws, Self::matches_kw_2_0)
    }

    pub(crate) fn split_3_0(kws: StdKeywords) -> Self {
        Self::split_inner(kws, Self::matches_kw_3_0)
    }

    pub(crate) fn split_3_1(kws: StdKeywords) -> Self {
        Self::split_inner(kws, Self::matches_kw_3_1)
    }

    pub(crate) fn split_3_2(kws: StdKeywords) -> Self {
        Self::split_inner(kws, Self::matches_kw_3_2)
    }

    fn split_inner<F>(mut kws: StdKeywords, mut f: F) -> Self
    where
        F: FnMut(&StdKey) -> bool,
    {
        let unused: HashMap<_, _> = kws.extract_if(|k, _| f(k)).collect();
        Self::new(kws, unused)
    }

    fn matches_kw_2_0(k: &StdKey) -> bool {
        let s: &str = k.as_ref();
        s.eq_ignore_ascii_case(Tot::C) || Dfc::matches(k) || Self::matches_meas_kw_common(k)
    }

    fn matches_kw_3_0(k: &StdKey) -> bool {
        let s: &str = k.as_ref();
        Self::matches_offsets(k)
            || s.eq_ignore_ascii_case(Tot::C)
            || s.eq_ignore_ascii_case(Timestep::C)
            || Gain::matches(k)
            || Self::matches_meas_kw_common(k)
    }

    fn matches_kw_3_1(k: &StdKey) -> bool {
        let s: &str = k.as_ref();
        Self::matches_offsets(k)
            || s.eq_ignore_ascii_case(Tot::C)
            || s.eq_ignore_ascii_case(Timestep::C)
            || Gain::matches(k)
            || Display::matches(k)
            || Calibration3_1::matches(k)
            || Self::matches_meas_kw_common(k)
    }

    fn matches_kw_3_2(k: &StdKey) -> bool {
        let s: &str = k.as_ref();
        Self::matches_offsets(k)
            || s.eq_ignore_ascii_case(Tot::C)
            || s.eq_ignore_ascii_case(Timestep::C)
            || Gain::matches(k)
            || Display::matches(k)
            || Calibration3_2::matches(k)
            || NumType::matches(k)
            || DetectorName::matches(k)
            || Tag::matches(k)
            || Analyte::matches(k)
            || Feature::matches(k)
            || OpticalType::matches(k)
            || Self::matches_meas_kw_common(k)
    }

    fn matches_offsets(k: &StdKey) -> bool {
        let s: &str = k.as_ref();
        s.eq_ignore_ascii_case(Beginanalysis::C)
            || s.eq_ignore_ascii_case(Endanalysis::C)
            || s.eq_ignore_ascii_case(Begindata::C)
            || s.eq_ignore_ascii_case(Enddata::C)
    }

    fn matches_meas_kw_common(k: &StdKey) -> bool {
        Width::matches(k)
            || Range::matches(k)
            || Scale::matches(k)
            || Shortname::matches(k)
            || Longname::matches(k)
            || Power::matches(k)
            || DetectorType::matches(k)
            || PercentEmitted::matches(k)
            || DetectorVoltage::matches(k)
    }
}

/// Error denoting that pseudostandard keyword was found.
#[derive(Debug, Error)]
#[error("pseudostandard keyword found: {0}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ExtraKeywordError))]
pub struct PseudostandardError(pub StdKey);

/// Error denoting that unused standard keyword was found.
#[derive(Debug, Error)]
#[error("unused standard keyword found: {0}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ExtraKeywordError))]
pub struct UnusedStandardError(pub StdKey);

macro_rules! newtype_string {
    ($t:ident) => {
        #[derive(Clone, FromStr, From, Into, PartialEq, Debug, Default, AsRef)]
        #[cfg_attr(feature = "serde", derive(Serialize))]
        #[cfg_attr(feature = "python", derive(FromPyObject, IntoPyObject))]
        #[as_ref(str)]
        pub struct $t(pub OptionalString);

        impl DisplayMaybe for $t {
            fn display_maybe(&self) -> Option<String> {
                self.0.display_maybe()
            }
        }

        impl KeywordPairMaybe for $t {
            type Inner = Self;
        }

        impl CheckMaybe for $t {
            type Inner = Self;
        }
    };
}

macro_rules! newtype_int {
    ($t:ident, $type:ty) => {
        #[derive(Clone, Copy, Display, FromStr, From, Into, PartialEq, Debug)]
        #[cfg_attr(feature = "serde", derive(Serialize))]
        #[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
        pub struct $t(pub $type);
    };
}

macro_rules! impl_display_maybe_self {
    ($t:ident) => {
        impl DisplayMaybe for $t {
            fn display_maybe(&self) -> Option<String> {
                self.0.display_maybe()
            }
        }

        impl CheckMaybe for $t {
            type Inner = Self;
        }

        impl KeywordPairMaybe for $t {
            type Inner = Self;
        }
    };
}

macro_rules! newtype_opt_int {
    ($t:ident, $inner:ident) => {
        #[derive(Clone, Default, PartialEq, Eq, FromStr, Debug)]
        #[cfg_attr(feature = "serde", derive(Serialize))]
        #[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
        pub struct $t(pub OptionalInt<$inner>);

        impl_display_maybe_self!($t);
    };
}

macro_rules! newtype_opt_bool {
    ($t:ident, $inner:ident, $err:ident) => {
        #[derive(Clone, PartialEq, Debug, Default, From, Into)]
        #[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
        #[cfg_attr(feature = "serde", derive(Serialize))]
        #[from(bool)]
        #[into(bool)]
        pub struct $t(pub OptionalZST<$inner>);

        impl FromStr for $t {
            type Err = $err;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                s.parse::<$inner>()
                    .map(Some)
                    .map(OptionalZST::from)
                    .map(Self)
            }
        }

        impl_display_maybe_self!($t);
    };
}

macro_rules! kw_meta {
    ($t:ident, $k:expr) => {
        impl Key for $t {
            const C: &'static str = $k;
        }
    };
}

macro_rules! kw_meas {
    ($t:ident, $sfx:expr) => {
        impl IndexedKey for $t {
            const PREFIX: &'static str = "P";
            const SUFFIX: &'static str = $sfx;
        }
    };
}

macro_rules! kw_meta_string {
    ($t:ident, $kw:expr) => {
        newtype_string!($t);

        impl Key for $t {
            const C: &'static str = $kw;
        }
    };
}

macro_rules! kw_meta_int {
    ($t:ident, $type:ident, $kw:expr) => {
        newtype_int!($t, $type);

        impl Key for $t {
            const C: &'static str = $kw;
        }
    };
}

macro_rules! kw_meas_string {
    ($t:ident, $sfx:expr) => {
        newtype_string!($t);
        kw_meas!($t, $sfx);
    };
}

macro_rules! req_meta {
    ($t:ident) => {
        impl Required for $t {}
        impl ReqMetarootKey for $t {}
    };
}

macro_rules! opt_meta {
    ($t:ident, $outer:path) => {
        impl Optional for $t {
            type Outer = $outer;
        }
        impl OptMetarootKey for $t {}
    };
}

macro_rules! req_meas {
    ($t:ident) => {
        impl Required for $t {}
        impl ReqIndexedKey for $t {}
    };
}

macro_rules! opt_meas {
    ($t:ident, $outer:path) => {
        impl Optional for $t {
            type Outer = $outer;
        }
        impl OptIndexedKey for $t {}
    };
}

macro_rules! kw_req_meta {
    ($t:ident, $sfx:expr) => {
        kw_meta!($t, $sfx);
        req_meta!($t);
    };
}

macro_rules! kw_opt_meta {
    ($t:ident, $sfx:expr, $outer:path) => {
        kw_meta!($t, $sfx);
        opt_meta!($t, $outer);
    };
}

macro_rules! kw_req_meas {
    ($t:ident, $sfx:expr) => {
        kw_meas!($t, $sfx);
        req_meas!($t);
    };
}

macro_rules! kw_opt_meas {
    ($t:ident, $sfx:expr, $outer:path) => {
        kw_meas!($t, $sfx);
        opt_meas!($t, $outer);
    };
}

macro_rules! kw_opt_meta_string {
    ($t:ident, $sfx:expr) => {
        kw_meta_string!($t, $sfx);
        opt_meta!($t, Self);
    };
}

macro_rules! kw_opt_meas_string {
    ($t:ident, $sfx:expr) => {
        kw_meas_string!($t, $sfx);
        opt_meas!($t, Self);
    };
}

macro_rules! kw_req_meta_int {
    ($t:ident, $type:ident, $sfx:expr) => {
        kw_meta_int!($t, $type, $sfx);
        req_meta!($t);
    };
}

macro_rules! kw_opt_meta_int {
    ($t:ident, $type:ident, $sfx:expr) => {
        kw_meta_int!($t, $type, $sfx);
        opt_meta!($t, Option<Self>);
    };
}

macro_rules! kw_time {
    ($outer:ident, $wrap:ident, $inner:ident, $err:ident, $key:expr) => {
        type $outer = $wrap<$inner>;

        kw_opt_meta!($outer, $key, Option<Self>);

        impl From<NaiveTime> for $outer {
            fn from(value: NaiveTime) -> Self {
                Xtim($inner(value))
            }
        }
    };
}

macro_rules! kw_opt_gate {
    ($t:ident, $sfx:expr, $outer:path) => {
        impl IndexedKey for $t {
            const PREFIX: &'static str = "G";
            const SUFFIX: &'static str = $sfx;
        }
        opt_meas!($t, $outer);
    };
}

macro_rules! kw_opt_gate_other {
    ($t:ident, $sfx:expr) => {
        kw_opt_gate!($t, $sfx, Option<Self>);
    };
}

macro_rules! kw_opt_gate_string {
    ($t:ident, $sfx:expr) => {
        newtype_string!($t);
        kw_opt_gate!($t, $sfx, Self);
    };
}

macro_rules! kw_opt_region {
    ($t:ident, $sfx:expr) => {
        impl IndexedKey for $t {
            const PREFIX: &'static str = "R";
            const SUFFIX: &'static str = $sfx;
        }
        opt_meas!($t, Option<Self>);
    };
}

macro_rules! meas_opt_zst {
    ($t:ident, $sym:expr, $inner:ident, $err:ident) => {
        newtype_opt_bool!($t, $inner, $err);
        kw_opt_meas!($t, $sym, Self);
    };
}

macro_rules! kw_opt_meta_opt_int {
    ($t:ident, $inner:ident, $sym:expr) => {
        newtype_opt_int!($t, $inner);
        kw_opt_meta!($t, $sym, Self);
    };
}

// all versions
kw_req_meta!(AlphaNumType, "DATATYPE");
kw_opt_meta_int!(Abrt, u32, "ABRT");
kw_opt_meta_string!(Cytsn, "CYTSN");
kw_opt_meta_string!(Com, "COM");
kw_opt_meta_string!(Cells, "CELLS");
kw_opt_meta!(FCSDate, "DATE", Option<Self>);
kw_opt_meta_string!(Exp, "EXP");
kw_opt_meta_string!(Fil, "FIL");
kw_opt_meta_string!(Inst, "INST");
kw_opt_meta_int!(Lost, u32, "LOST");
kw_opt_meta_string!(Op, "OP");
kw_req_meta_int!(Par, usize, "PAR");
kw_opt_meta_string!(Proj, "PROJ");
kw_opt_meta_string!(Smno, "SMNO");
kw_opt_meta_string!(Src, "SRC");
kw_opt_meta_string!(Sys, "SYS");
kw_opt_meta!(Trigger, "TR", Option<Self>);

// time for 2.0
kw_time!(Btim2_0, Btim, FCSTime, FCSTimeError, "BTIM");
kw_time!(Etim2_0, Etim, FCSTime, FCSTimeError, "ETIM");

// time for 3.0
kw_time!(Btim3_0, Btim, FCSTime60, FCSTime60Error, "BTIM");
kw_time!(Etim3_0, Etim, FCSTime60, FCSTime60Error, "ETIM");

// time for 3.1-3.2
kw_time!(Btim3_1, Btim, FCSTime100, FCSTime100Error, "BTIM");
kw_time!(Etim3_1, Etim, FCSTime100, FCSTime100Error, "ETIM");

// 3.0 only
kw_opt_meta!(Compensation3_0, "COMP", Option<Self>);
kw_opt_meta!(Unicode, "UNICODE", Option<Self>);

// for 3.0+
kw_req_meta!(Timestep, "TIMESTEP");

// for 3.1+
kw_opt_meta_string!(LastModifier, "LAST_MODIFIER");
kw_opt_meta!(Originality, "ORIGINALITY", Option<Self>);
kw_opt_meta!(LastModified, "LAST_MODIFIED", Option<Self>);

kw_opt_meta_string!(Plateid, "PLATEID");
kw_opt_meta_string!(Platename, "PLATENAME");
kw_opt_meta_string!(Wellid, "WELLID");

kw_opt_meta!(Spillover, "SPILLOVER", Option<Self>);

kw_opt_meta!(Vol, "VOL", Option<Self>);

// for 3.2+
kw_opt_meta_string!(Carrierid, "CARRIERID");
kw_opt_meta_string!(Carriertype, "CARRIERTYPE");
kw_opt_meta_string!(Locationid, "LOCATIONID");

kw_opt_meta!(BeginDateTime, "BEGINDATETIME", Option<Self>);
kw_opt_meta!(EndDateTime, "ENDDATETIME", Option<Self>);
kw_opt_meta!(UnstainedCenters, "UNSTAINEDCENTERS", Self);

kw_opt_meta_string!(UnstainedInfo, "UNSTAINEDINFO");

kw_opt_meta_string!(Flowrate, "FLOWRATE");

// version-specific
kw_opt_meta_int!(Tot, usize, "TOT"); // optional in 2.0
req_meta!(Tot); // required in 3.0+

kw_req_meta!(Mode, "MODE"); // for 2.0-3.1
kw_opt_meta!(Mode3_2, "MODE", Option<Self>); // for 3.2+

kw_opt_meta_string!(Cyt, "CYT"); // optional for 2.0-3.1
kw_req_meta!(Cyt3_2, "CYT"); // required for 3.2+

kw_req_meta!(ByteOrd2_0, "BYTEORD"); // 2.0/3.0
kw_req_meta!(ByteOrd3_1, "BYTEORD"); // 3.1+

// all versions
kw_req_meas!(Width, "B");
kw_opt_meas_string!(Filter, "F");
kw_opt_meas!(Power, "O", Option<Self>);
kw_opt_meas!(PercentEmitted, "P", Option<Self>);
kw_req_meas!(Range, "R");
kw_opt_meas_string!(Longname, "S");
kw_opt_meas_string!(DetectorType, "T");
kw_opt_meas!(DetectorVoltage, "V", Option<Self>);

// 3.0+
kw_opt_meas!(Gain, "G", Option<Self>);

// 3.1+
kw_opt_meas!(Display, "D", Option<Self>);

// 3.2+
kw_opt_meas!(Feature, "FEATURE", Option<Self>);
meas_opt_zst!(TemporalType, "TYPE", TemporalTypeInner, TemporalTypeError);
kw_opt_meas!(NumType, "DATATYPE", Option<Self>);
kw_opt_meas_string!(Analyte, "ANALYTE");
kw_opt_meas_string!(Tag, "TAG");
kw_opt_meas_string!(DetectorName, "DET");

impl_display_maybe_self!(OpticalType);
kw_opt_meas!(OpticalType, "TYPE", Self);

// version specific
kw_opt_meas!(Shortname, "N", Option<Self>); // optional for 2.0/3.0
req_meas!(Shortname); // required for 3.1+

kw_opt_meas!(Scale, "E", Option<Self>); // optional for 2.0
req_meas!(Scale); // required for 3.0+

meas_opt_zst!(
    TemporalScale2_0,
    "E",
    TemporalScaleInner,
    TemporalScaleError
); // optional for 2.0
kw_req_meas!(TemporalScale3_0, "E"); // required for 3.0+

kw_opt_meas!(Wavelength, "L", Option<Self>); // scaler in 2.0/3.0
kw_opt_meas!(Wavelengths, "L", Self); // vector in 3.1+

kw_opt_meas!(Calibration3_1, "CALIBRATION", Option<Self>); // 3.1 doesn't have offset
kw_opt_meas!(Calibration3_2, "CALIBRATION", Option<Self>); // 3.2+ includes offset

// 2.0 compensation matrix
#[derive(Debug)]
pub struct Dfc;

impl BiIndexedKey for Dfc {
    const PREFIX: &'static str = "DFC";
    const MIDDLE: &'static str = "TO";
    const SUFFIX: &'static str = "";
}

impl Dfc {
    pub(crate) fn lookup(
        kws: &mut StdKeywords,
        k: Key2<Self>,
    ) -> Result<Option<f32>, LookupDfcError> {
        kws.remove(&k.as_std()).map_or(Ok(None), |v| {
            v.parse::<f32>()
                .map_err(|e| ParseKeyError::new(e, k, v.clone()))
                .map(Some)
        })
    }
}

pub type LookupDfcError = ParseKeyError<ParseFloatError, Dfc, BiIndex>;

// 3.0/3.1 subsets
kw_opt_meta_int!(CSMode, usize, "CSMODE");

kw_opt_meta_opt_int!(CSTot, u32, "CSTOT");
kw_opt_meta_opt_int!(CSVBits, u32, "CSVBITS");

// $CSVnFLAG (3.0/3.1)
newtype_int!(CSVFlag, u32);
opt_meas!(CSVFlag, Option<Self>);

impl IndexedKey for CSVFlag {
    const PREFIX: &'static str = "CSV";
    const SUFFIX: &'static str = "FLAG";
}

// $PKn (2.0-3.1)
newtype_int!(PeakBin, u32);
opt_meas!(PeakBin, Option<Self>);

impl IndexedKey for PeakBin {
    const PREFIX: &'static str = "PK";
    const SUFFIX: &'static str = "";
}

// $PKNn (2.0-3.1)
newtype_int!(PeakIndex, MeasIndex);
opt_meas!(PeakIndex, Option<Self>);

impl IndexedKey for PeakIndex {
    const PREFIX: &'static str = "PKN";
    const SUFFIX: &'static str = "";
}

// 2.0-3.1 gating parameters
kw_opt_meta_int!(Gate, usize, "GATE");

kw_opt_gate_other!(GateScale, "E");
kw_opt_gate_string!(GateFilter, "F");
kw_opt_gate_other!(GatePercentEmitted, "P");
kw_opt_gate_other!(GateRange, "R");
kw_opt_gate_other!(GateShortname, "N");
kw_opt_gate_string!(GateLongname, "S");
kw_opt_gate_string!(GateDetectorType, "T");
kw_opt_gate_other!(GateDetectorVoltage, "V");
kw_opt_meta!(Gating, "GATING", Option<Self>);

kw_opt_region!(RegionWindow, "W");

impl<I> IndexedKey for RegionGateIndex<I> {
    const PREFIX: &'static str = "R";
    const SUFFIX: &'static str = "I";
}

impl<I> Optional for RegionGateIndex<I> {
    type Outer = Option<Self>;
}
impl<I> OptIndexedKey for RegionGateIndex<I> where I: fmt::Display + FromStr {}

// offsets for all versions
kw_req_meta!(Nextdata, "NEXTDATA");
opt_meta!(Nextdata, Option<Self>);

macro_rules! kw_offset {
    ($t:ident, $key:expr) => {
        /// Value for $$key (3.0-3.2)
        #[derive(Display, From, Into, FromStr, Debug)]
        pub struct $t(pub UintZeroPad20);

        kw_req_meta!($t, $key);
    };
}

kw_offset!(Beginanalysis, "BEGINANALYSIS");
kw_offset!(Begindata, "BEGINDATA");
kw_offset!(Beginstext, "BEGINSTEXT");
kw_offset!(Endanalysis, "ENDANALYSIS");
kw_offset!(Enddata, "ENDDATA");
kw_offset!(Endstext, "ENDSTEXT");

opt_meta!(Beginanalysis, Option<Self>);
opt_meta!(Endanalysis, Option<Self>);
opt_meta!(Beginstext, Option<Self>);
opt_meta!(Endstext, Option<Self>);

// TODO test error cases here as well
#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;

    #[test]
    fn tr() {
        assert_from_to_str::<Trigger>("Wooden Leg Pt 3,456");
    }

    #[test]
    fn mode() {
        assert_from_to_str::<Mode>("C");
        assert_from_to_str::<Mode>("L");
        assert_from_to_str::<Mode>("U");
    }

    #[test]
    fn mode_3_2() {
        assert_from_to_str::<Mode3_2>("L");
    }

    #[test]
    fn pnd() {
        assert_from_to_str::<Display>("Linear,0,1");
        assert_from_to_str::<Display>("Logarithmic,1,1");
        assert_from_to_str::<Display>("Logarithmic,1,0.1");
    }

    #[test]
    fn datatype() {
        assert_from_to_str::<NumType>("I");
        assert_from_to_str::<NumType>("F");
        assert_from_to_str::<NumType>("D");
    }

    #[test]
    fn pndatetype() {
        assert_from_to_str::<AlphaNumType>("I");
        assert_from_to_str::<AlphaNumType>("F");
        assert_from_to_str::<AlphaNumType>("D");
        assert_from_to_str::<AlphaNumType>("A");
    }

    #[test]
    fn pne_time() {
        assert_from_to_str::<TemporalScale3_0>("0,0");
    }

    #[test]
    fn pncalibration_3_1() {
        assert_from_to_str::<Calibration3_1>("0.1,cubic imperial lightyears");
    }

    #[test]
    fn pncalibration_3_2() {
        assert_from_to_str::<Calibration3_2>("1.1,3.5813,progressive metal albums");
    }

    #[test]
    fn pnl_3_1() {
        assert_from_to_str_maybe::<Wavelengths>("1");
        assert_from_to_str_maybe::<Wavelengths>("1,2");
    }

    #[test]
    fn last_modified() {
        assert_from_to_str_almost::<LastModified>(
            "01-Jan-2112 00:00:00",
            "01-Jan-2112 00:00:00.00",
        );
        assert_from_to_str::<LastModified>("01-Jan-2112 00:00:00.01");
    }

    #[test]
    fn originality() {
        assert_from_to_str::<Originality>("Original");
        assert_from_to_str::<Originality>("NonDataModified");
        assert_from_to_str::<Originality>("Appended");
        assert_from_to_str::<Originality>("DataModified");
    }

    #[test]
    fn unicode() {
        assert_from_to_str::<Unicode>("42,$BYTEORD");
        // we don't actually check that the keyword is valid, likely nobody
        // will notice ;)
        assert_from_to_str::<Unicode>("42,$40DOLLARBILL");
    }

    #[test]
    fn pntype_optical() {
        assert_from_to_str_maybe::<OpticalType>("Forward Scatter");
        assert_from_to_str_maybe::<OpticalType>("Side Scatter");
        assert_from_to_str_maybe::<OpticalType>("Raw Fluorescence");
        assert_from_to_str_maybe::<OpticalType>("Unmixed Fluorescence");
        assert_from_to_str_maybe::<OpticalType>("Mass");
        assert_from_to_str_maybe::<OpticalType>("Electronic Volume");
        assert_from_to_str_maybe::<OpticalType>("Index");
        assert_from_to_str_maybe::<OpticalType>("Classification");
    }

    #[test]
    fn pntype_time() {
        assert_from_to_str_maybe::<TemporalType>("Time");
    }

    #[test]
    fn pnfeature() {
        assert_from_to_str::<Feature>("Area");
        assert_from_to_str::<Feature>("Width");
        assert_from_to_str::<Feature>("Height");
    }

    #[test]
    fn rni_2_0() {
        assert_from_to_str::<RegionGateIndex<GateIndex>>("1");
        assert_from_to_str::<RegionGateIndex<GateIndex>>("1,2");
    }

    #[test]
    fn rni_3_0() {
        assert_from_to_str::<RegionGateIndex<MeasOrGateIndex>>("P1");
        assert_from_to_str::<RegionGateIndex<MeasOrGateIndex>>("P1,P2");
        assert_from_to_str::<RegionGateIndex<MeasOrGateIndex>>("G1");
        assert_from_to_str::<RegionGateIndex<MeasOrGateIndex>>("G1,G2");
    }

    #[test]
    fn rni_3_2() {
        assert_from_to_str::<RegionGateIndex<PrefixedMeasIndex>>("P1");
        assert_from_to_str::<RegionGateIndex<PrefixedMeasIndex>>("P1,P2");
    }

    #[test]
    fn rnw() {
        assert_from_to_str::<RegionWindow>("1,1");
        assert_from_to_str::<RegionWindow>("1,1;2,3;5,8;13,21");
    }

    #[test]
    fn gating() {
        assert_from_to_str::<Gating>("R1");
        assert_from_to_str_almost::<Gating>("R1 AND (R2.OR.R3)", "(R1 AND (R2 OR R3))");
        assert_from_to_str::<Gating>("((NOT R1) AND R2)");
    }

    // TODO this is hard(er) to test since the order will be random
    #[test]
    fn unstained_centers() {
        assert_from_to_str_maybe::<UnstainedCenters>("1,X,0");
    }

    #[test]
    fn unstained_centers_wrong_len() {
        assert!("2,X,0".parse::<UnstainedCenters>().is_err());
    }

    #[test]
    fn unstained_centers_nonunique() {
        assert!("3,Y,Y,Z,0,0,0".parse::<UnstainedCenters>().is_err());
    }

    #[test]
    fn str_compensation() {
        assert_from_to_str::<Compensation3_0>("2,0,0,0,0");
        assert_from_to_str::<Compensation3_0>("3,0,0,0,0,0,0,0,0,0");
        assert_from_to_str::<Compensation3_0>("2,1.1,1,0,-1.5");
    }

    #[test]
    fn str_compensation_too_small() {
        assert!("1,0".parse::<Compensation3_0>().is_err());
    }

    #[test]
    fn str_compensation_mismatch() {
        assert!("2,0,0,0".parse::<Compensation3_0>().is_err());
    }

    #[test]
    fn str_compensation_badfloats() {
        assert!("2,zero,0,coconut".parse::<Compensation3_0>().is_err());
    }

    #[test]
    fn str_to_byteord_valid() {
        assert_from_to_str::<ByteOrd2_0>("1");
        assert_from_to_str::<ByteOrd2_0>("1,2,3,4");
        assert_from_to_str::<ByteOrd2_0>("1,2,3,4");
        assert_from_to_str::<ByteOrd2_0>("4,3,2,1");
        assert_from_to_str::<ByteOrd2_0>("3,4,2,1");
        assert_from_to_str::<ByteOrd2_0>("1,2,3,4,5,6,7,8");
    }

    #[test]
    fn str_to_byteord_tolong() {
        assert!("1,2,3,4,5,6,7,8,9".parse::<ByteOrd2_0>().is_err());
    }

    #[test]
    fn str_to_byteord_bad_digits() {
        assert!("0".parse::<ByteOrd2_0>().is_err());
        assert!("2".parse::<ByteOrd2_0>().is_err());
    }

    #[test]
    fn str_to_byteord_skipped() {
        assert!("1,3".parse::<ByteOrd2_0>().is_err());
    }

    #[test]
    fn str_to_byteord_repeat() {
        assert!("1,1".parse::<ByteOrd2_0>().is_err());
    }

    #[test]
    fn str_to_byteord_garbage() {
        assert!("fortytwo".parse::<ByteOrd2_0>().is_err());
        assert!("".parse::<ByteOrd2_0>().is_err());
        assert!("one,two,three".parse::<ByteOrd2_0>().is_err());
    }

    #[test]
    fn str_to_endian() {
        assert!("1,2,3,4".parse::<ByteOrd3_1>().is_ok());
        assert!("4,3,2,1".parse::<ByteOrd3_1>().is_ok());
        assert!("1,2,3".parse::<ByteOrd3_1>().is_err());
        assert!("5,4,3,2,1".parse::<ByteOrd3_1>().is_err());
    }

    #[test]
    fn scale() {
        assert_from_to_str::<Scale>("0,0");
        assert_from_to_str::<Scale>("4.5,0.01");
    }

    #[test]
    fn scale_invalid() {
        assert!("4.5,0".parse::<Scale>().is_err());
    }
}

#[cfg(feature = "python")]
mod python {
    use crate::text::ranged_float::PositiveFloat;
    use crate::validated::shortname::Shortname;

    use super::{
        ByteOrd2_0, Calibration3_1, Calibration3_2, Display, IndexPair, Scale, Trigger, UniGate,
        Unicode, Vertex,
    };

    use pyo3::conversion::IntoPyObjectExt as _;
    use pyo3::prelude::*;
    use pyo3::types::PyTuple;
    use std::num::NonZeroU8;

    // $BYTEORD is a list of integers
    impl<'py> FromPyObject<'py> for ByteOrd2_0 {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let xs: Vec<NonZeroU8> = ob.extract()?;
            let ret = Self::try_from(&xs[..])?;
            Ok(ret)
        }
    }

    // $PnE (2.0) as either () or (f32, f32) tuples in python
    impl<'py> FromPyObject<'py> for Scale {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            if ob.is_instance_of::<PyTuple>() && ob.len()? == 0 {
                Ok(Self::Linear)
            } else {
                let (decades, offset): (f32, f32) = ob.extract()?;
                let ret = Self::try_new_log(decades, offset)?;
                Ok(ret)
            }
        }
    }

    impl<'py> IntoPyObject<'py> for Scale {
        type Target = PyAny;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            match self {
                Self::Linear => Ok(PyTuple::empty(py).into_any()),
                Self::Log(l) => (f32::from(l.decades), f32::from(l.offset)).into_bound_py_any(py),
            }
        }
    }

    // $PnCALIBRATION (3.1) as (f32, String) tuple in python
    impl<'py> FromPyObject<'py> for Calibration3_1 {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let (slope, unit): (PositiveFloat, String) = ob.extract()?;
            Ok(Self { slope, unit })
        }
    }

    impl<'py> IntoPyObject<'py> for Calibration3_1 {
        type Target = PyTuple;
        type Output = Bound<'py, <(PositiveFloat, String) as IntoPyObject<'py>>::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            (self.slope, self.unit).into_pyobject(py)
        }
    }

    // $PnCALIBRATION (3.2) as (f32, f32, String) tuple in python
    impl<'py> FromPyObject<'py> for Calibration3_2 {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let (slope, offset, unit): (PositiveFloat, f32, String) = ob.extract()?;
            Ok(Self {
                slope,
                offset,
                unit,
            })
        }
    }

    impl<'py> IntoPyObject<'py> for Calibration3_2 {
        type Target = PyTuple;
        type Output = Bound<'py, <(PositiveFloat, f32, String) as IntoPyObject<'py>>::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            (self.slope, self.offset, self.unit).into_pyobject(py)
        }
    }

    // $UNICODE (3.0) as a tuple like (f32, [String]) in python
    impl<'py> FromPyObject<'py> for Unicode {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let (page, kws): (u32, Vec<String>) = ob.extract()?;
            Ok(Self { page, kws })
        }
    }

    impl<'py> IntoPyObject<'py> for Unicode {
        type Target = PyTuple;
        type Output = Bound<'py, <(u32, Vec<String>) as IntoPyObject<'py>>::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            (self.page, self.kws).into_pyobject(py)
        }
    }

    // $PnD (3.1+) as a tuple like (bool, f32, f32) in python where 'bool' is true
    // if linear
    impl<'py> FromPyObject<'py> for Display {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let (is_log, x0, x1): (bool, f32, f32) = ob.extract()?;
            let ret = if is_log {
                Self::Log {
                    offset: x0.try_into()?,
                    decades: x1.try_into()?,
                }
            } else {
                Self::Lin {
                    lower: x0,
                    upper: x1,
                }
            };
            Ok(ret)
        }
    }

    impl<'py> IntoPyObject<'py> for Display {
        type Target = PyTuple;
        type Output = Bound<'py, <(bool, f32, f32) as IntoPyObject<'py>>::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            let ret = match self {
                Self::Lin { lower, upper } => (false, lower, upper),
                Self::Log { offset, decades } => (true, offset.into(), decades.into()),
            };
            ret.into_pyobject(py)
        }
    }

    // $TR as a tuple like (String, u32) in python
    impl<'py> FromPyObject<'py> for Trigger {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let (measurement, threshold): (Shortname, u32) = ob.extract()?;
            Ok(Self {
                measurement,
                threshold,
            })
        }
    }

    impl<'py> IntoPyObject<'py> for Trigger {
        type Target = PyTuple;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            (self.measurement, self.threshold).into_pyobject(py)
        }
    }

    // unigate (for univariate gating regions) is a tuple pair of floats
    impl<'py> FromPyObject<'py> for UniGate {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let (lower, upper) = ob.extract()?;
            Ok(Self { lower, upper })
        }
    }

    impl<'py> IntoPyObject<'py> for UniGate {
        type Target = PyTuple;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            (self.lower, self.upper).into_pyobject(py)
        }
    }

    // vertex (for bivariate gating regions) is a tuple pair of floats
    impl<'py> FromPyObject<'py> for Vertex {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let (x, y) = ob.extract()?;
            Ok(Self { x, y })
        }
    }

    impl<'py> IntoPyObject<'py> for Vertex {
        type Target = PyTuple;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            (self.x, self.y).into_pyobject(py)
        }
    }

    // index pairs are like python tuple pairs
    impl<'py, I> FromPyObject<'py> for IndexPair<I>
    where
        I: FromPyObject<'py>,
    {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let (x, y) = ob.extract()?;
            Ok(Self { x, y })
        }
    }

    impl<'py, I> IntoPyObject<'py> for IndexPair<I>
    where
        I: IntoPyObject<'py>,
    {
        type Target = PyTuple;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            (self.x, self.y).into_pyobject(py)
        }
    }
}

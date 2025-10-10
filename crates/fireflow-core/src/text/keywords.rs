use crate::config::StdTextReadConfig;
use crate::error::{BiTentative, PassthruExt as _, ResultExt as _, Tentative};
use crate::macros::impl_newtype_try_from;
use crate::nonempty::FCSNonEmpty;
use crate::validated::ascii_uint::UintZeroPad20;
use crate::validated::keys::{BiIndexedKey, IndexedKey, Key, StdKeywords};
use crate::validated::nonempty_string::NonEmptyString;
use crate::validated::shortname::Shortname;

use super::byteord::{ByteOrd2_0, ByteOrd3_1, Width};
use super::compensation::Compensation3_0;
use super::datetimes::{BeginDateTime, EndDateTime};
use super::float_decimal::{DecimalToFloatError, FloatDecimal, HasFloatBounds};
use super::gating;
use super::index::{GateIndex, MeasIndex, RegionIndex};
use super::named_vec::NameMapping;
use super::optional::{
    CheckMaybe, DisplayMaybe, KeywordPairMaybe, MaybeValue, OptionalString, OptionalZST,
};
use super::parser::{
    DepValueWarning, DeprecatedError, FromStrDelim, FromStrStateful, LookupKeysWarning,
    LookupResult, LookupTentative, OptIndexedKey, OptLinkedKey, OptMetarootKey, Optional,
    ParseOptKeyError, ReqIndexedKey, ReqMetarootKey, Required,
};
use super::ranged_float::{NonNegFloat, PositiveFloat, RangedFloatError};
use super::scale::{Scale, ScaleError};
use super::spillover::Spillover;
use super::timestamps::{Btim, Etim, FCSDate, FCSTime, FCSTime100, FCSTime60, Xtim};

use bigdecimal::{BigDecimal, ParseBigDecimalError};
use chrono::{NaiveDateTime, NaiveTime, Timelike as _};
use derive_more::{Add, AsMut, AsRef, Display, From, FromStr, Into, Sub};
use itertools::Itertools as _;
use nonempty::NonEmpty;
use num_traits::cast::ToPrimitive as _;
use num_traits::identities::One as _;
use num_traits::PrimInt;
use std::iter::once;
use thiserror::Error;

use std::any::type_name;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::num::{ParseFloatError, ParseIntError};
use std::str::FromStr;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use crate::python::macros::impl_from_py_transparent;

#[cfg(feature = "python")]
use pyo3::prelude::*;

/// Value for $NEXTDATA (all versions)
#[derive(From, Into, FromStr, Display)]
pub struct Nextdata(pub UintZeroPad20);

/// The value of the $PnG keyword
#[derive(Clone, Copy, PartialEq, From, Display, FromStr, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct Gain(pub PositiveFloat);

/// The value of the $TIMESTEP keyword
#[derive(Clone, Copy, PartialEq, From, Display, FromStr, Into, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
#[into(f32, PositiveFloat)]
pub struct Timestep(pub PositiveFloat);

impl_newtype_try_from!(Timestep, PositiveFloat, f32, RangedFloatError);

impl Default for Timestep {
    fn default() -> Self {
        Self(PositiveFloat::one())
    }
}

impl Timestep {
    pub(crate) fn check_conversion(self, allow_loss: bool) -> BiTentative<(), TimestepLossError> {
        let mut tnt = Tentative::default();
        if !self.0.is_one() {
            tnt.push_error_or_warning(TimestepLossError(self), !allow_loss);
        }
        tnt
    }
}

#[derive(Debug, Error)]
#[error("$TIMESTEP is {0} and will be 1.0 after conversion")]
pub struct TimestepLossError(Timestep);

/// The value of the $VOL keyword
#[derive(Clone, Copy, From, Display, FromStr, Into, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
#[into(NonNegFloat, f32)]
pub struct Vol(pub NonNegFloat);

impl_newtype_try_from!(Vol, NonNegFloat, f32, RangedFloatError);

/// The value of the $TR field (all versions)
///
/// This is formatted as 'string,f' where 'string' is a measurement name.
#[derive(Clone, PartialEq, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[display("{measurement},{threshold}")]
pub struct Trigger {
    /// The measurement name (assumed to match a '$PnN' value).
    pub measurement: Shortname,

    /// The threshold of the trigger.
    pub threshold: u32,
}

impl FromStrStateful for Trigger {
    type Err = TriggerError;
    type Payload<'a> = ();

    fn from_str_st(s: &str, (): (), conf: &StdTextReadConfig) -> Result<Self, Self::Err> {
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

#[derive(Debug, Error)]
pub enum TriggerError {
    #[error("must be like 'string,f'")]
    WrongFieldNumber,
    #[error("{0}")]
    IntFormat(ParseIntError),
}

/// The values used for the $MODE key (up to 3.1)
#[derive(Clone, PartialEq, Eq, Default, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum Mode {
    #[default]
    #[display("L")]
    List,
    #[display("U")]
    Uncorrelated,
    #[display("C")]
    Correlated,
}

#[derive(Debug, Error)]
#[error("must be one of 'C', 'L', or 'U'")]
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
#[derive(Clone, PartialEq, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
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

#[derive(Debug, Error)]
#[error("can only be 'L'")]
pub struct Mode3_2Error;

#[derive(Debug, Error)]
#[error("pre-3.2 $MODE must be 'L' to upgrade to 3.2 $MODE")]
pub struct ModeUpgradeError;

/// The value for the $PnDISPLAY key (3.1+)
#[derive(Clone, Copy, PartialEq, Debug, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum Display {
    /// Linear display (value like 'Linear,<lower>,<upper>')
    #[display("Linear,{lower},{upper}")]
    Lin { lower: f32, upper: f32 },

    // TODO not clear if these can be <0
    /// Logarithmic display (value like 'Logarithmic,<offset>,<decades>')
    #[display("Logarithmic,{decades},{offset}")]
    Log { offset: f32, decades: f32 },
}

impl FromStr for Display {
    type Err = DisplayError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.split(',').collect::<Vec<_>>()[..] {
            [which, s1, s2] => {
                let f1 = s1.parse().map_err(DisplayError::FloatError)?;
                let f2 = s2.parse().map_err(DisplayError::FloatError)?;
                match which {
                    "Linear" => Ok(Self::Lin {
                        lower: f1,
                        upper: f2,
                    }),
                    "Logarithmic" => Ok(Self::Log {
                        decades: f1,
                        offset: f2,
                    }),
                    _ => Err(DisplayError::InvalidType),
                }
            }
            _ => Err(DisplayError::FormatError),
        }
    }
}

#[derive(Debug, Error)]
pub enum DisplayError {
    #[error("{0}")]
    FloatError(ParseFloatError),
    #[error("Type must be either 'Logarithmic' or 'Linear'")]
    InvalidType,
    #[error("must be like 'string,f1,f2'")]
    FormatError,
}

/// The three values for the $PnDATATYPE keyword (3.2+)
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
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

#[derive(Debug, Error)]
#[error("must be one of 'F', 'D', or 'A'")]
pub struct NumTypeError;

/// The four allowed values for the $DATATYPE keyword.
#[derive(Clone, Copy, Eq, PartialEq, PartialOrd, Ord, Hash, Debug, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
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

impl AlphaNumType {
    pub(crate) fn lookup_req_check_ascii(kws: &mut StdKeywords) -> LookupResult<Self> {
        let mut d = Self::lookup_req(kws);
        d.def_eval_warning(|v| check_datatype_ascii(*v));
        d
    }
}

fn check_datatype_ascii(datatype: AlphaNumType) -> Option<LookupKeysWarning> {
    (datatype == AlphaNumType::Ascii)
        .then(|| DeprecatedError::Value(DepValueWarning::DatatypeASCII).into())
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

#[derive(Debug, Error)]
#[error("must be one of 'I', 'F', 'D', or 'A'")]
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

impl DisplayMaybe for TemporalScale3_0 {
    fn display_maybe(&self) -> Option<String> {
        Some(self.0.to_string())
    }
}

impl KeywordPairMaybe for TemporalScale3_0 {
    type Inner = Self;
}

#[derive(Debug, Error)]
#[error("time measurement must have linear scaling")]
pub struct TemporalScaleError;

/// The value for the $PnCALIBRATION key (3.1 only)
///
/// This should be formatted like '<value>,<unit>'
#[derive(Clone, PartialEq, Debug, Display)]
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
            [value, unit] => Ok(Self {
                slope: value.parse().map_err(CalibrationError::Range)?,
                unit: String::from(unit),
            }),
            _ => Err(CalibrationError::Format(CalibrationFormat3_1)),
        }
    }
}

#[derive(Debug, Error)]
#[error("must be like 'f,string'")]
pub struct CalibrationFormat3_1;

#[derive(Debug, Display, Error)]
pub enum CalibrationError<C> {
    Float(ParseFloatError),
    Range(RangedFloatError),
    Format(C),
}

/// The value for the $PnCALIBRATION key (3.2+)
///
/// This should be formatted like '<value>,[<offset>,]<unit>' and differs from
/// 3.1 with the optional inclusion of "offset" (assumed 0 if not included).
#[derive(Clone, PartialEq, Debug, Display)]
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

#[derive(Debug, Error)]
#[error("must be like 'f1,[f2],string'")]
pub struct CalibrationFormat3_2;

/// The value for the $PnL key (2.0/3.0).
#[derive(Clone, Copy, From, FromStr, Display, Into, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
#[into(f32, PositiveFloat)]
pub struct Wavelength(pub PositiveFloat);

impl_newtype_try_from!(Wavelength, PositiveFloat, f32, RangedFloatError);

/// The value for the $PnL key (3.1).
///
/// Starting in 3.1 this is a vector rather than a scaler.
#[derive(Clone, From, PartialEq, Debug, Default)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
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

impl FromStrStateful for Wavelengths {
    type Err = WavelengthsError;
    type Payload<'a> = ();

    fn from_str_st(s: &str, (): (), conf: &StdTextReadConfig) -> Result<Self, Self::Err> {
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
        allow_loss: bool,
    ) -> Tentative<Option<Wavelength>, WavelengthsLossError, WavelengthsLossError> {
        NonEmpty::from_vec(self.0).map_or(Tentative::default(), |ws| {
            let n = ws.len();
            let mut tnt = Tentative::new1(Some(Wavelength(ws.head)));
            if n > 1 {
                tnt.push_error_or_warning(WavelengthsLossError(n), !allow_loss);
            }
            tnt
        })
    }
}

#[derive(Debug, Error)]
#[error(
    "wavelengths is {0} elements long and will \
     be reduced to first upon conversion"
)]
pub struct WavelengthsLossError(pub usize);

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
#[cfg_attr(feature = "python", derive(IntoPyObject))]
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

#[derive(Debug, Error)]
#[error("must be like 'dd-mmm-yyyy hh:mm:ss[.cc]'")]
pub struct LastModifiedError;

/// The value for the $ORIGINALITY key (3.1+)
#[derive(Clone, Copy, PartialEq, Debug, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
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

#[derive(Debug, Error)]
#[error("must be one of 'Original', 'NonDataModified', 'Appended', or 'DataModified'")]
pub struct OriginalityError;

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

impl FromStrStateful for Unicode {
    type Err = UnicodeError;
    type Payload<'a> = ();

    fn from_str_st(s: &str, (): (), conf: &StdTextReadConfig) -> Result<Self, Self::Err> {
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

#[derive(Debug, Error)]
pub enum UnicodeError {
    #[error("No keywords given")]
    Empty,
    #[error("Must be like 'n,string,[[string],...]'")]
    BadFormat,
}

/// The value of the $PnTYPE key in optical channels (3.2+)
#[derive(Clone, PartialEq, Debug, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum OpticalType {
    #[display("{}", FORWARD_SCATTER)]
    ForwardScatter,
    #[display("{}", SIDE_SCATTER)]
    SideScatter,
    #[display("{}", RAW_FLUORESCENCE)]
    RawFluorescence,
    #[display("{}", UNMIXED_FLUORESCENCE)]
    UnmixedFluorescence,
    #[display("{}", MASS)]
    Mass,
    #[display("{}", ELECTRONIC_VOLUME)]
    ElectronicVolume,
    #[display("{}", CLASSIFICATION)]
    Classification,
    #[display("{}", INDEX)]
    Index,
    #[display("{_0}")]
    Other(String),
}

#[derive(Debug, Error)]
#[error("$PnTYPE for time measurement shall not be 'Time' if given")]
pub struct OpticalTypeError;

const FORWARD_SCATTER: &str = "Forward Scatter";
const SIDE_SCATTER: &str = "Side Scatter";
const RAW_FLUORESCENCE: &str = "Raw Fluorescence";
const UNMIXED_FLUORESCENCE: &str = "Unmixed Fluorescence";
const MASS: &str = "Mass";
const INDEX: &str = "Index";
const ELECTRONIC_VOLUME: &str = "Electronic Volume";
const CLASSIFICATION: &str = "Classification";
const TIME: &str = "Time";

impl FromStr for OpticalType {
    type Err = OpticalTypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            TIME => Err(OpticalTypeError),
            FORWARD_SCATTER => Ok(Self::ForwardScatter),
            SIDE_SCATTER => Ok(Self::SideScatter),
            RAW_FLUORESCENCE => Ok(Self::RawFluorescence),
            UNMIXED_FLUORESCENCE => Ok(Self::UnmixedFluorescence),
            MASS => Ok(Self::Mass),
            ELECTRONIC_VOLUME => Ok(Self::ElectronicVolume),
            INDEX => Ok(Self::Index),
            CLASSIFICATION => Ok(Self::Classification),
            x => Ok(Self::Other(x.into())),
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

#[derive(Debug, Error)]
#[error("$PnTYPE for time measurement shall be 'Time' if given")]
pub struct TemporalTypeError;

/// The value of the $PnFEATURE key (3.2+)
#[derive(Clone, Copy, PartialEq, Debug, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
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

#[derive(Debug, Error)]
#[error("must be one of 'Area', 'Width', or 'Height'")]
pub struct FeatureError;

/// The value of the $RnI key (all versions)
#[derive(Clone, Copy, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub(crate) enum RegionGateIndex<I> {
    Univariate(I),
    Bivariate(IndexPair<I>),
}

/// The two indices of a bivariate gate
#[derive(Clone, Copy, PartialEq, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[display("{x},{y}")]
pub struct IndexPair<I> {
    pub x: I,
    pub y: I,
}

impl<I> IndexPair<I> {
    pub(crate) fn map<F, J>(self, mut f: F) -> IndexPair<J>
    where
        F: FnMut(I) -> J,
    {
        IndexPair {
            x: f(self.x),
            y: f(self.y),
        }
    }

    pub(crate) fn try_map<F, J, E>(self, mut f: F) -> Result<IndexPair<J>, E>
    where
        F: FnMut(I) -> Result<J, E>,
    {
        Ok(IndexPair {
            x: f(self.x)?,
            y: f(self.y)?,
        })
    }
}

impl<I> RegionGateIndex<I> {
    pub(crate) fn lookup_region_opt(
        kws: &mut StdKeywords,
        i: RegionIndex,
        par: Par,
        is_deprecated: bool,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<MaybeValue<Self>>
    where
        I: fmt::Display + FromStr + gating::LinkedMeasIndex,
        for<'a> Self: fmt::Display + FromStrStateful<Payload<'a> = ()>,
        ParseOptKeyError: From<<Self as FromStrStateful>::Err>,
    {
        Self::lookup_meas_opt_st(kws, i, is_deprecated, (), conf).and_tentatively(|maybe| {
            if let Some(x) = maybe.0 {
                Self::check_link(&x, par)
                    .map(|()| x)
                    .into_tentative_opt(!conf.allow_optional_dropping)
                    .inner_into()
            } else {
                Tentative::default()
            }
            .value_into()
        })
    }

    fn check_link(&self, par: Par) -> Result<(), RegionIndexError>
    where
        I: gating::LinkedMeasIndex,
    {
        NonEmpty::collect(
            self.meas_indices()
                .into_iter()
                .filter(|&i| i >= par.0.into()),
        )
        .map_or(Ok(()), |ne| Err(RegionIndexError(ne)))
    }

    fn meas_indices(&self) -> Vec<MeasIndex>
    where
        I: gating::LinkedMeasIndex,
    {
        match self {
            Self::Univariate(i) => i.meas_index().into_iter().collect(),
            Self::Bivariate(i) => [i.x.meas_index(), i.y.meas_index()]
                .into_iter()
                .flatten()
                .collect(),
        }
    }
}

impl<I: FromStr> FromStrStateful for RegionGateIndex<I> {
    type Err = RegionGateIndexError<<I as FromStr>::Err>;
    type Payload<'a> = ();

    fn from_str_st(s: &str, (): (), conf: &StdTextReadConfig) -> Result<Self, Self::Err> {
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

#[derive(Debug, Error)]
pub enum RegionGateIndexError<E> {
    #[error("{0}")]
    Int(E),
    #[error("must be either a single value 'x' or a pair 'x,y'")]
    Format,
}

#[derive(Debug, Error)]
#[error(
    "region index refers to non-existent measurement index: {}",
    .0.iter().join(",")
)]
pub struct RegionIndexError(NonEmpty<MeasIndex>);

#[derive(Clone, Copy, From, PartialEq, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
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

#[derive(Debug, Error)]
pub enum MeasOrGateIndexError {
    #[error("{0}")]
    Int(ParseIntError),
    #[error("must be prefixed with either 'P' or 'G'")]
    Format,
}

#[derive(Clone, Copy, From, PartialEq, Into, AsMut, Debug, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
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
#[derive(Display)]
pub(crate) enum RegionWindow {
    #[display("{_0}")]
    Univariate(UniGate),
    #[display("{}", _0.iter().join(";"))]
    Bivariate(NonEmpty<Vertex>),
}

#[derive(Clone, PartialEq, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[display("{x},{y}")]
pub struct Vertex {
    pub x: BigDecimal,
    pub y: BigDecimal,
}

#[derive(Clone, PartialEq, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[display("{lower},{upper}")]
pub struct UniGate {
    pub lower: BigDecimal,
    pub upper: BigDecimal,
}

impl FromStrStateful for RegionWindow {
    type Err = GatePairError;
    type Payload<'a> = ();

    fn from_str_st(s: &str, (): (), conf: &StdTextReadConfig) -> Result<Self, Self::Err> {
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
        // ASSUME split will always contain one element
        let xs = NonEmpty::collect(ss).unwrap();
        if xs.tail.is_empty() {
            go_uni(xs.head).map(RegionWindow::Univariate)
        } else {
            xs.try_map(go_bi).map(Self::Bivariate)
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

#[derive(Debug, Error)]
pub enum GatePairError {
    #[error("{0}")]
    Num(ParseBigDecimalError),
    #[error("must be a string like 'f1,f2;[f3,f4;...]'")]
    Format,
}

/// The value of the $GATING key (3.0-3.2)
#[derive(Clone, PartialEq, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
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

#[derive(Debug, Error)]
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

/// The value of the $PnR key.
#[derive(Clone, From, Display, FromStr, Add, Sub, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
#[from(u8, u16, u32, u64, BigDecimal)]
pub struct Range(pub BigDecimal);

impl Range {
    pub(crate) fn into_uint<T>(self, disallow_trunc: bool) -> BiTentative<T, IntRangeError<()>>
    where
        T: TryFrom<Self, Error = IntRangeError<T>> + PrimInt,
    {
        let (b, e) = self.try_into().map_or_else(
            |e: IntRangeError<T>| match e.error_kind {
                IntRangeErrorKind::Overrange => (T::max_value(), Some(e.void())),
                IntRangeErrorKind::Underrange => (T::zero(), Some(e.void())),
                IntRangeErrorKind::PrecisionLoss(y) => (y, Some(e.void())),
            },
            |x| (x, None),
        );
        BiTentative::new_either1(b, e, disallow_trunc)
    }

    pub(crate) fn into_float<T>(
        self,
        disallow_trunc: bool,
    ) -> BiTentative<FloatDecimal<T>, DecimalToFloatError>
    where
        FloatDecimal<T>: TryFrom<BigDecimal, Error = DecimalToFloatError>,
        T: HasFloatBounds,
    {
        let (x, e) = FloatDecimal::try_from(self.0).map_or_else(
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
        BiTentative::new_either1(x, e, disallow_trunc)
    }
}

macro_rules! try_from_range_int {
    ($inttype:ident, $to:ident) => {
        impl TryFrom<Range> for $inttype {
            type Error = IntRangeError<$inttype>;

            fn try_from(value: Range) -> Result<Self, Self::Error> {
                let x = &value.0;
                let err = |error_kind| IntRangeError {
                    src_type: type_name::<$inttype>(),
                    src_num: x.clone(),
                    error_kind,
                };
                if let Some(y) = x.$to() {
                    if x.fractional_digit_count() <= 0 {
                        Ok(y)
                    } else {
                        Err(err(IntRangeErrorKind::PrecisionLoss(y)))
                    }
                } else {
                    if BigDecimal::from($inttype::MAX) < *x {
                        Err(err(IntRangeErrorKind::Overrange))
                    } else {
                        Err(err(IntRangeErrorKind::Underrange))
                    }
                }
            }
        }
    };
}

try_from_range_int!(u8, to_u8);
try_from_range_int!(u16, to_u16);
try_from_range_int!(u32, to_u32);
try_from_range_int!(u64, to_u64);

#[derive(Debug, Error)]
pub struct IntRangeError<T> {
    src_type: &'static str,
    src_num: BigDecimal,
    error_kind: IntRangeErrorKind<T>,
}

#[derive(Debug)]
pub enum IntRangeErrorKind<T> {
    Overrange,
    Underrange,
    PrecisionLoss(T),
}

impl<T> IntRangeError<T> {
    pub(crate) fn void(self) -> IntRangeError<()> {
        IntRangeError {
            src_type: self.src_type,
            src_num: self.src_num,
            error_kind: match self.error_kind {
                IntRangeErrorKind::Overrange => IntRangeErrorKind::Overrange,
                IntRangeErrorKind::Underrange => IntRangeErrorKind::Underrange,
                IntRangeErrorKind::PrecisionLoss(_) => IntRangeErrorKind::PrecisionLoss(()),
            },
        }
    }
}

impl<T> fmt::Display for IntRangeError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let t = self.src_type;
        let x = &self.src_num;
        match self.error_kind {
            IntRangeErrorKind::Overrange => {
                write!(f, "{x} is larger than {t} can hold")
            }
            IntRangeErrorKind::Underrange => {
                write!(f, "{x} is less than zero and could not be converted to {t}")
            }
            IntRangeErrorKind::PrecisionLoss(_) => {
                write!(f, "{x} lost precision when converting to {t}")
            }
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
#[derive(Clone, From, Display, FromStr, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
pub struct GateShortname(pub Shortname);

/// The value of the $GmR key
#[derive(Clone, From, Display, FromStr, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
#[from(u64)]
pub struct GateRange(pub Range);

macro_rules! impl_non_neg_float {
    ($(#[$meta:meta])* $t:ident) => {
        $(#[$meta])*
        #[derive(Clone, Copy, From, Display, FromStr, Into, PartialEq, Debug)]
        #[cfg_attr(feature = "serde", derive(Serialize))]
        #[cfg_attr(feature = "python", derive(IntoPyObject))]
        #[into(NonNegFloat, f32)]
        pub struct $t(pub NonNegFloat);

        impl_newtype_try_from!($t, NonNegFloat, f32, RangedFloatError);

        #[cfg(feature = "python")]
        impl_from_py_transparent!($t);
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
#[derive(Clone, Copy, Display, FromStr, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
pub struct GateScale(pub Scale);

// use the same fix we use for PnE here
impl FromStrStateful for GateScale {
    type Err = ScaleError;
    type Payload<'a> = ();

    fn from_str_st(s: &str, data: (), conf: &StdTextReadConfig) -> Result<Self, Self::Err> {
        Scale::from_str_st(s, data, conf).map(Self)
    }
}

/// The value of the $CYT key (3.2).
///
/// This is not a normal string because it is required in 3.2 and thus cannot
/// be empty.
#[derive(Clone, Display, FromStr, PartialEq, Into)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
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
pub struct NoCytError;

/// The value for the $UNSTAINEDCENTERS key (3.2+)
#[derive(Clone, Into, PartialEq, Debug, Default)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
pub struct UnstainedCenters(pub HashMap<Shortname, f32>);

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
    pub(crate) fn names_difference(
        &self,
        names: &HashSet<&Shortname>,
    ) -> impl Iterator<Item = &Shortname> {
        self.0.keys().filter(|n| !names.contains(n))
    }
}

impl FromStrStateful for UnstainedCenters {
    type Err = ParseUnstainedCenterError;
    type Payload<'a> = ();

    fn from_str_st(s: &str, (): (), conf: &StdTextReadConfig) -> Result<Self, Self::Err> {
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

impl OptLinkedKey for UnstainedCenters {
    fn names(&self) -> HashSet<&Shortname> {
        self.0.keys().collect()
    }

    fn reassign(&mut self, mapping: &NameMapping) {
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
}

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
        #[cfg_attr(feature = "python", derive(IntoPyObject))]
        pub struct $t(pub $type);

        #[cfg(feature = "python")]
        impl_from_py_transparent!($t);
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
        opt_meta!($t, MaybeValue<Self>);
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
        kw_opt_gate!($t, $sfx, MaybeValue<Self>);
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
        opt_meas!($t, MaybeValue<Self>);
    };
}

macro_rules! meas_opt_zst {
    ($t:ident, $sym:expr, $inner:ident, $err:ident) => {
        #[derive(Clone, PartialEq, Debug, Default, From, Into)]
        #[cfg_attr(feature = "python", derive(FromPyObject, IntoPyObject))]
        #[cfg_attr(feature = "serde", derive(Serialize))]
        #[from(bool)]
        #[into(bool)]
        pub struct $t(pub OptionalZST<$inner>);

        kw_opt_meas!($t, $sym, Self);

        impl FromStr for $t {
            type Err = $err;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                s.parse::<$inner>()
                    .map(Some)
                    .map(OptionalZST::from)
                    .map(Self)
            }
        }

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

impl Key for Trigger {
    const C: &'static str = "TR";
}

impl Optional for Trigger {
    type Outer = MaybeValue<Self>;
}

impl OptLinkedKey for Trigger {
    fn names(&self) -> HashSet<&Shortname> {
        once(&self.measurement).collect()
    }

    fn reassign(&mut self, mapping: &NameMapping) {
        if let Some(new) = mapping.get(&self.measurement) {
            self.measurement = (*new).clone();
        }
    }
}

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
kw_opt_meta!(Compensation3_0, "COMP", MaybeValue<Self>);
kw_opt_meta!(Unicode, "UNICODE", MaybeValue<Self>);

// for 3.0+
kw_req_meta!(Timestep, "TIMESTEP");

// for 3.1+
kw_opt_meta_string!(LastModifier, "LAST_MODIFIER");
kw_opt_meta!(Originality, "ORIGINALITY", MaybeValue<Self>);
kw_opt_meta!(LastModified, "LAST_MODIFIED", MaybeValue<Self>);

kw_opt_meta_string!(Plateid, "PLATEID");
kw_opt_meta_string!(Platename, "PLATENAME");
kw_opt_meta_string!(Wellid, "WELLID");

// impl Key for Spillover {
//     const C: &'static str = "SPILLOVER";
// }

// impl Optional for Spillover {}
kw_opt_meta!(Spillover, "SPILLOVER", MaybeValue<Self>);

kw_opt_meta!(Vol, "VOL", MaybeValue<Self>);

// for 3.2+
kw_opt_meta_string!(Carrierid, "CARRIERID");
kw_opt_meta_string!(Carriertype, "CARRIERTYPE");
kw_opt_meta_string!(Locationid, "LOCATIONID");

kw_opt_meta!(BeginDateTime, "BEGINDATETIME", Option<Self>);
kw_opt_meta!(EndDateTime, "ENDDATETIME", Option<Self>);

impl Key for UnstainedCenters {
    const C: &'static str = "UNSTAINEDCENTERS";
}

impl Optional for UnstainedCenters {
    type Outer = Self;
}

kw_opt_meta_string!(UnstainedInfo, "UNSTAINEDINFO");

kw_opt_meta_string!(Flowrate, "FLOWRATE");

// version-specific
kw_opt_meta_int!(Tot, usize, "TOT"); // optional in 2.0
req_meta!(Tot); // required in 3.0+

kw_req_meta!(Mode, "MODE"); // for 2.0-3.1
kw_opt_meta!(Mode3_2, "MODE", MaybeValue<Self>); // for 3.2+

kw_opt_meta_string!(Cyt, "CYT"); // optional for 2.0-3.1
kw_req_meta!(Cyt3_2, "CYT"); // required for 3.2+

kw_req_meta!(ByteOrd2_0, "BYTEORD"); // 2.0/3.0
kw_req_meta!(ByteOrd3_1, "BYTEORD"); // 3.1+

// all versions
kw_req_meas!(Width, "B");
kw_opt_meas_string!(Filter, "F");
kw_opt_meas!(Power, "O", MaybeValue<Self>);
kw_opt_meas!(PercentEmitted, "P", MaybeValue<Self>);
kw_req_meas!(Range, "R");
kw_opt_meas_string!(Longname, "S");
kw_opt_meas_string!(DetectorType, "T");
kw_opt_meas!(DetectorVoltage, "V", MaybeValue<Self>);

// 3.0+
kw_opt_meas!(Gain, "G", MaybeValue<Self>);

// 3.1+
kw_opt_meas!(Display, "D", MaybeValue<Self>);

// 3.2+
kw_opt_meas!(Feature, "FEATURE", MaybeValue<Self>);
kw_opt_meas!(OpticalType, "TYPE", MaybeValue<Self>);
meas_opt_zst!(TemporalType, "TYPE", TemporalTypeInner, TemporalTypeError);
kw_opt_meas!(NumType, "DATATYPE", MaybeValue<Self>);
kw_opt_meas_string!(Analyte, "ANALYTE");
kw_opt_meas_string!(Tag, "TAG");
kw_opt_meas_string!(DetectorName, "DET");

// version specific
kw_opt_meas!(Shortname, "N", MaybeValue<Self>); // optional for 2.0/3.0
req_meas!(Shortname); // required for 3.1+

kw_opt_meas!(Scale, "E", MaybeValue<Self>); // optional for 2.0
req_meas!(Scale); // required for 3.0+

meas_opt_zst!(TemporalScale, "E", TemporalScaleInner, TemporalScaleError); // optional for 2.0
kw_req_meas!(TemporalScale3_0, "E"); // required for 3.0+

kw_opt_meas!(Wavelength, "L", MaybeValue<Self>); // scaler in 2.0/3.0
kw_opt_meas!(Wavelengths, "L", Self); // vector in 3.1+

kw_opt_meas!(Calibration3_1, "CALIBRATION", MaybeValue<Self>); // 3.1 doesn't have offset
kw_opt_meas!(Calibration3_2, "CALIBRATION", MaybeValue<Self>); // 3.2+ includes offset

// 2.0 compensation matrix
pub struct Dfc;

impl BiIndexedKey for Dfc {
    const PREFIX: &'static str = "DFC";
    const MIDDLE: &'static str = "TO";
    const SUFFIX: &'static str = "";
}

// 3.0/3.1 subsets
kw_opt_meta_int!(CSMode, usize, "CSMODE");
kw_opt_meta_int!(CSVBits, u32, "CSVBITS");
kw_opt_meta_int!(CSTot, u32, "CSTOT");

// $CSVnFLAG (3.0/3.1)
newtype_int!(CSVFlag, u32);
opt_meas!(CSVFlag, MaybeValue<Self>);

impl IndexedKey for CSVFlag {
    const PREFIX: &'static str = "CSV";
    const SUFFIX: &'static str = "FLAG";
}

// $PKn (2.0-3.1)
newtype_int!(PeakBin, u32);
opt_meas!(PeakBin, MaybeValue<Self>);

impl IndexedKey for PeakBin {
    const PREFIX: &'static str = "PK";
    const SUFFIX: &'static str = "";
}

// $PKNn (2.0-3.1)
newtype_int!(PeakNumber, u32);
opt_meas!(PeakNumber, MaybeValue<Self>);

impl IndexedKey for PeakNumber {
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
kw_opt_meta!(Gating, "GATING", MaybeValue<Self>);

kw_opt_region!(RegionWindow, "W");

impl<I> IndexedKey for RegionGateIndex<I> {
    const PREFIX: &'static str = "R";
    const SUFFIX: &'static str = "I";
}

impl<I> Optional for RegionGateIndex<I> {
    type Outer = MaybeValue<Self>;
}
impl<I> OptIndexedKey for RegionGateIndex<I> where I: fmt::Display + FromStr {}

// offsets for all versions
kw_req_meta!(Nextdata, "NEXTDATA");

macro_rules! kw_offset {
    ($t:ident, $key:expr) => {
        /// Value for $$key (3.0-3.2)
        #[derive(Display, From, Into, FromStr)]
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

opt_meta!(Beginanalysis, MaybeValue<Self>);
opt_meta!(Endanalysis, MaybeValue<Self>);
opt_meta!(Beginstext, MaybeValue<Self>);
opt_meta!(Endstext, MaybeValue<Self>);

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
        // TODO seems like this shouldn't be allowed
        assert_from_to_str::<Display>("Linear,1,0");
        assert_from_to_str::<Display>("Logarithmic,1,1");
        // TODO this also seems like nonsense
        assert_from_to_str::<Display>("Logarithmic,1,0");
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
        assert_from_to_str::<OpticalType>("Forward Scatter");
        assert_from_to_str::<OpticalType>("Side Scatter");
        assert_from_to_str::<OpticalType>("Raw Fluorescence");
        assert_from_to_str::<OpticalType>("Unmixed Fluorescence");
        assert_from_to_str::<OpticalType>("Mass");
        assert_from_to_str::<OpticalType>("Electronic Volume");
        assert_from_to_str::<OpticalType>("Index");
        assert_from_to_str::<OpticalType>("Classification");
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
}

#[cfg(feature = "python")]
mod python {
    use crate::python::macros::{
        impl_from_py_transparent, impl_from_py_via_fromstr, impl_to_py_via_display, impl_value_err,
    };
    use crate::text::ranged_float::PositiveFloat;
    use crate::validated::shortname::Shortname;

    use super::{
        AlphaNumType, AlphaNumTypeError, Calibration3_1, Calibration3_2, Cyt3_2, Display, Feature,
        FeatureError, GateRange, GateScale, GateShortname, IndexPair, LastModified, Mode, Mode3_2,
        Mode3_2Error, ModeError, NumType, NumTypeError, OpticalType, OpticalTypeError, Originality,
        OriginalityError, PrefixedMeasIndex, Range, Timestep, Trigger, UniGate, Unicode,
        UnstainedCenters, Vertex, Vol, Wavelength, Wavelengths,
    };

    use pyo3::prelude::*;
    use pyo3::types::PyTuple;

    macro_rules! impl_str_py {
        ($type:ident, $err:ident) => {
            impl_from_py_via_fromstr!($type);
            impl_to_py_via_display!($type);
            impl_value_err!($err);
        };
    }

    // these should all be interpreted as validated/literal python strings
    impl_str_py!(Originality, OriginalityError);
    impl_str_py!(AlphaNumType, AlphaNumTypeError);
    impl_str_py!(NumType, NumTypeError);
    impl_str_py!(Feature, FeatureError);
    impl_str_py!(Mode, ModeError);
    impl_str_py!(Mode3_2, Mode3_2Error);
    impl_str_py!(OpticalType, OpticalTypeError);

    impl_from_py_transparent!(Cyt3_2);
    impl_from_py_transparent!(UnstainedCenters);

    impl_from_py_transparent!(Wavelength);
    impl_from_py_transparent!(Vol);
    impl_from_py_transparent!(Timestep);
    impl_from_py_transparent!(LastModified);
    impl_from_py_transparent!(Range);
    impl_from_py_transparent!(GateRange);
    impl_from_py_transparent!(GateScale);
    impl_from_py_transparent!(GateShortname);
    impl_from_py_transparent!(PrefixedMeasIndex);
    impl_from_py_transparent!(Wavelengths);

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
                    offset: x0,
                    decades: x1,
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
                Self::Log { offset, decades } => (true, offset, decades),
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

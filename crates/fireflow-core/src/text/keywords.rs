use crate::config::StdTextReadConfig;
use crate::error::*;
use crate::macros::impl_newtype_try_from;
use crate::nonempty::NonEmptyExt;
use crate::validated::ascii_uint::*;
use crate::validated::keys::*;
use crate::validated::shortname::*;

use super::byteord::*;
use super::compensation::*;
use super::datetimes::*;
use super::float_decimal::{DecimalToFloatError, FloatDecimal, HasFloatBounds};
use super::gating;
use super::index::*;
use super::named_vec::NameMapping;
use super::optional::*;
use super::parser::*;
use super::ranged_float::*;
use super::scale::*;
use super::spillover::*;
use super::timestamps::*;
use super::unstainedcenters::*;

use bigdecimal::{BigDecimal, ParseBigDecimalError};
use chrono::{NaiveDateTime, NaiveTime, Timelike};
use derive_more::{Add, AsMut, Display, From, FromStr, Into, Sub};
use itertools::Itertools;
use nonempty::NonEmpty;
use num_traits::cast::ToPrimitive;
use num_traits::PrimInt;
use std::any::type_name;
use std::collections::HashSet;
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
pub struct Nextdata(pub Uint20Char);

/// The value of the $PnG keyword
#[derive(Clone, Copy, PartialEq, From, Display, FromStr)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct Gain(pub PositiveFloat);

/// The value of the $TIMESTEP keyword
#[derive(Clone, Copy, PartialEq, From, Display, FromStr, Into)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
#[into(f32, PositiveFloat)]
pub struct Timestep(pub PositiveFloat);

impl_newtype_try_from!(Timestep, PositiveFloat, f32, RangedFloatError);

impl Default for Timestep {
    fn default() -> Self {
        // ASSUME this will never panic...1.0 is still a positive number, right?
        Timestep(PositiveFloat::try_from(1.0).ok().unwrap())
    }
}

impl Timestep {
    pub(crate) fn check_conversion(&self, force: bool) -> BiTentative<(), TimestepLossError> {
        let mut tnt = Tentative::default();
        if f32::from(self.0) != 1.0 {
            tnt.push_error_or_warning(TimestepLossError(*self), !force);
        }
        tnt
    }
}

pub struct TimestepLossError(Timestep);

impl fmt::Display for TimestepLossError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "$TIMESTEP is {} and will be 1.0 after conversion",
            self.0
        )
    }
}

/// The value of the $VOL keyword
#[derive(Clone, Copy, From, Display, FromStr, Into, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
#[into(NonNegFloat, f32)]
pub struct Vol(pub NonNegFloat);

impl_newtype_try_from!(Vol, NonNegFloat, f32, RangedFloatError);

/// The value of the $TR field (all versions)
///
/// This is formatted as 'string,f' where 'string' is a measurement name.
#[derive(Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct Trigger {
    /// The measurement name (assumed to match a '$PnN' value).
    pub measurement: Shortname,

    /// The threshold of the trigger.
    pub threshold: u32,
}

pub enum TriggerError {
    WrongFieldNumber,
    IntFormat(std::num::ParseIntError),
}

impl FromStr for Trigger {
    type Err = TriggerError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.split(",").collect::<Vec<_>>()[..] {
            [p, n1] => n1
                .parse()
                .map_err(TriggerError::IntFormat)
                .map(|threshold| Trigger {
                    measurement: Shortname::new_unchecked(p),
                    threshold,
                }),
            _ => Err(TriggerError::WrongFieldNumber),
        }
    }
}

impl fmt::Display for Trigger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{},{}", self.measurement, self.threshold)
    }
}

impl fmt::Display for TriggerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            TriggerError::WrongFieldNumber => write!(f, "must be like 'string,f'"),
            TriggerError::IntFormat(i) => write!(f, "{}", i),
        }
    }
}

/// The values used for the $MODE key (up to 3.1)
#[derive(Clone, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum Mode {
    #[default]
    List,
    Uncorrelated,
    Correlated,
}

pub struct ModeError;

impl FromStr for Mode {
    type Err = ModeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "C" => Ok(Mode::Correlated),
            "L" => Ok(Mode::List),
            "U" => Ok(Mode::Uncorrelated),
            _ => Err(ModeError),
        }
    }
}

impl fmt::Display for Mode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let x = match self {
            Mode::Correlated => "C",
            Mode::List => "L",
            Mode::Uncorrelated => "U",
        };
        write!(f, "{}", x)
    }
}

impl fmt::Display for ModeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "must be one of 'C', 'L', or 'U'")
    }
}

/// The value for the $MODE key, which can only contain 'L' (3.2)
#[derive(Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct Mode3_2;

pub struct Mode3_2Error;

impl FromStr for Mode3_2 {
    type Err = Mode3_2Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "L" => Ok(Mode3_2),
            _ => Err(Mode3_2Error),
        }
    }
}

impl fmt::Display for Mode3_2 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "L")
    }
}

impl fmt::Display for Mode3_2Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "can only be 'L'")
    }
}

impl TryFrom<Mode> for Mode3_2 {
    type Error = ModeUpgradeError;

    fn try_from(value: Mode) -> Result<Self, Self::Error> {
        match value {
            Mode::List => Ok(Mode3_2),
            _ => Err(ModeUpgradeError),
        }
    }
}

pub struct ModeUpgradeError;

impl fmt::Display for ModeUpgradeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "pre-3.2 $MODE must be 'L' to upgrade to 3.2 $MODE")
    }
}

/// The value for the $PnDISPLAY key (3.1+)
#[derive(Clone, Copy, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum Display {
    /// Linear display (value like 'Linear,<lower>,<upper>')
    Lin { lower: f32, upper: f32 },

    // TODO not clear if these can be <0
    /// Logarithmic display (value like 'Logarithmic,<offset>,<decades>')
    Log { offset: f32, decades: f32 },
}

impl FromStr for Display {
    type Err = DisplayError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.split(",").collect::<Vec<_>>()[..] {
            [which, s1, s2] => {
                let f1 = s1.parse().map_err(DisplayError::FloatError)?;
                let f2 = s2.parse().map_err(DisplayError::FloatError)?;
                match which {
                    "Linear" => Ok(Display::Lin {
                        lower: f1,
                        upper: f2,
                    }),
                    "Logarithmic" => Ok(Display::Log {
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

impl fmt::Display for Display {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Display::Lin { lower, upper } => write!(f, "Linear,{lower},{upper}"),
            Display::Log { offset, decades } => write!(f, "Logarithmic,{decades},{offset}"),
        }
    }
}

pub enum DisplayError {
    FloatError(ParseFloatError),
    InvalidType,
    FormatError,
}

impl fmt::Display for DisplayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            DisplayError::FloatError(x) => write!(f, "{}", x),
            DisplayError::InvalidType => write!(f, "Type must be either 'Logarithmic' or 'Linear'"),
            DisplayError::FormatError => write!(f, "must be like 'string,f1,f2'"),
        }
    }
}

/// The three values for the $PnDATATYPE keyword (3.2+)
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum NumType {
    Integer,
    Single,
    Double,
}

impl FromStr for NumType {
    type Err = NumTypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "I" => Ok(NumType::Integer),
            "F" => Ok(NumType::Single),
            "D" => Ok(NumType::Double),
            _ => Err(NumTypeError),
        }
    }
}

impl fmt::Display for NumType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            NumType::Integer => write!(f, "I"),
            NumType::Single => write!(f, "F"),
            NumType::Double => write!(f, "D"),
        }
    }
}

pub struct NumTypeError;

impl fmt::Display for NumTypeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "must be one of 'F', 'D', or 'A'")
    }
}

/// The four allowed values for the $DATATYPE keyword.
#[derive(Clone, Copy, Eq, PartialEq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum AlphaNumType {
    Ascii,
    Integer,
    Single,
    Double,
}

impl AlphaNumType {
    pub(crate) fn lookup_req_check_ascii(kws: &mut StdKeywords) -> LookupResult<Self> {
        let mut d = AlphaNumType::lookup_req(kws);
        d.def_eval_warning(check_datatype_ascii);
        d
    }
}

fn check_datatype_ascii(datatype: &AlphaNumType) -> Option<LookupKeysWarning> {
    if *datatype == AlphaNumType::Ascii {
        Some(DeprecatedError::Value(DepValueWarning::DatatypeASCII).into())
    } else {
        None
    }
}

impl FromStr for AlphaNumType {
    type Err = AlphaNumTypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "I" => Ok(AlphaNumType::Integer),
            "F" => Ok(AlphaNumType::Single),
            "D" => Ok(AlphaNumType::Double),
            "A" => Ok(AlphaNumType::Ascii),
            _ => Err(AlphaNumTypeError),
        }
    }
}

impl fmt::Display for AlphaNumType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            AlphaNumType::Ascii => write!(f, "A"),
            AlphaNumType::Integer => write!(f, "I"),
            AlphaNumType::Single => write!(f, "F"),
            AlphaNumType::Double => write!(f, "D"),
        }
    }
}

pub struct AlphaNumTypeError;

impl fmt::Display for AlphaNumTypeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "must be one of 'I', 'F', 'D', or 'A'")
    }
}

impl From<NumType> for AlphaNumType {
    fn from(value: NumType) -> Self {
        match value {
            NumType::Integer => AlphaNumType::Integer,
            NumType::Single => AlphaNumType::Single,
            NumType::Double => AlphaNumType::Double,
        }
    }
}

impl TryFrom<AlphaNumType> for NumType {
    type Error = ();
    fn try_from(value: AlphaNumType) -> Result<Self, Self::Error> {
        match value {
            AlphaNumType::Integer => Ok(NumType::Integer),
            AlphaNumType::Single => Ok(NumType::Single),
            AlphaNumType::Double => Ok(NumType::Double),
            AlphaNumType::Ascii => Err(()),
        }
    }
}

/// The value of the $PnE key for temporal measurements (all versions)
///
/// This can only be linear (0,0)
#[derive(Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct TemporalScale;

impl TemporalScale {
    pub(crate) fn pair(i: IndexFromOne) -> (String, String) {
        (Self::std(i).to_string(), Self.to_string())
    }
}

impl FromStr for TemporalScale {
    type Err = TemporalScaleError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "0,0" => Ok(Self),
            _ => Err(TemporalScaleError),
        }
    }
}

impl fmt::Display for TemporalScale {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "0,0")
    }
}

pub struct TemporalScaleError;

impl fmt::Display for TemporalScaleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "$PnE for time measurement must be '0,0' (linear)")
    }
}

/// The value for the $PnCALIBRATION key (3.1 only)
///
/// This should be formatted like '<value>,<unit>'
#[derive(Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct Calibration3_1 {
    pub slope: PositiveFloat,
    pub unit: String,
}

impl FromStr for Calibration3_1 {
    type Err = CalibrationError<CalibrationFormat3_1>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.split(",").collect::<Vec<_>>()[..] {
            [value, unit] => Ok(Calibration3_1 {
                slope: value.parse().map_err(CalibrationError::Range)?,
                unit: String::from(unit),
            }),
            _ => Err(CalibrationError::Format(CalibrationFormat3_1)),
        }
    }
}

impl fmt::Display for Calibration3_1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{},{}", self.slope, self.unit)
    }
}

pub struct CalibrationFormat3_1;

impl fmt::Display for CalibrationFormat3_1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "must be like 'f,string'")
    }
}

pub enum CalibrationError<C> {
    Float(ParseFloatError),
    Range(RangedFloatError),
    Format(C),
}

impl<C: fmt::Display> fmt::Display for CalibrationError<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            CalibrationError::Float(x) => x.fmt(f),
            CalibrationError::Range(x) => x.fmt(f),
            CalibrationError::Format(x) => x.fmt(f),
        }
    }
}

/// The value for the $PnCALIBRATION key (3.2+)
///
/// This should be formatted like '<value>,[<offset>,]<unit>' and differs from
/// 3.1 with the optional inclusion of "offset" (assumed 0 if not included).
#[derive(Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct Calibration3_2 {
    pub slope: PositiveFloat,
    pub offset: f32,
    pub unit: String,
}

impl FromStr for Calibration3_2 {
    type Err = CalibrationError<CalibrationFormat3_2>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (slope, offset, unit) = match s.split(",").collect::<Vec<_>>()[..] {
            [slope, unit] => Ok((slope, 0.0, unit)),
            [slope, soffset, unit] => {
                let f2 = soffset.parse().map_err(CalibrationError::Float)?;
                Ok((slope, f2, unit))
            }
            _ => Err(CalibrationError::Format(CalibrationFormat3_2)),
        }?;
        Ok(Calibration3_2 {
            slope: slope.parse().map_err(CalibrationError::Range)?,
            offset,
            unit: unit.into(),
        })
    }
}

impl fmt::Display for Calibration3_2 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{},{},{}", self.slope, self.offset, self.unit)
    }
}

pub struct CalibrationFormat3_2;

impl fmt::Display for CalibrationFormat3_2 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "must be like 'f1,[f2],string'")
    }
}

/// The value for the $PnL key (2.0/3.0).
#[derive(Clone, Copy, From, FromStr, Display, Into, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
#[into(f32, PositiveFloat)]
pub struct Wavelength(pub PositiveFloat);

impl_newtype_try_from!(Wavelength, PositiveFloat, f32, RangedFloatError);

/// The value for the $PnL key (3.1).
///
/// Starting in 3.1 this is a vector rather than a scaler.
#[derive(Clone, From, PartialEq)]
pub struct Wavelengths(pub NonEmpty<PositiveFloat>);

impl From<Wavelengths> for Vec<f32> {
    fn from(value: Wavelengths) -> Self {
        Vec::from(value.0).into_iter().map(|x| x.into()).collect()
    }
}

#[cfg(feature = "serde")]
impl Serialize for Wavelengths {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.iter().collect::<Vec<_>>().serialize(serializer)
    }
}

impl fmt::Display for Wavelengths {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", self.0.iter().join(","))
    }
}

impl FromStr for Wavelengths {
    type Err = WavelengthsError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut ws = vec![];
        for x in s.split(",") {
            ws.push(x.parse().map_err(WavelengthsError::Num)?);
        }
        NonEmpty::from_vec(ws)
            .ok_or(WavelengthsError::Empty)
            .map(Wavelengths)
    }
}

impl Wavelengths {
    pub(crate) fn into_wavelength(
        self,
        lossless: bool,
    ) -> Tentative<Wavelength, WavelengthsLossError, WavelengthsLossError> {
        let ws = self.0;
        let n = ws.len();
        let mut tnt = Tentative::new1(Wavelength(ws.head));
        if n > 1 {
            tnt.push_error_or_warning(WavelengthsLossError(n), lossless);
        }
        tnt
    }
}

pub struct WavelengthsLossError(pub usize);

impl fmt::Display for WavelengthsLossError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "wavelengths is {} elements long and will be reduced to first upon conversion",
            self.0
        )
    }
}

pub enum WavelengthsError {
    Num(RangedFloatError),
    Empty,
}

impl fmt::Display for WavelengthsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            WavelengthsError::Num(i) => write!(f, "{}", i),
            WavelengthsError::Empty => write!(f, "list must not be empty"),
        }
    }
}

/// A datetime as used in the $LAST_MODIFIED key (3.1+ only)
///
/// Inner value is private to ensure it always gets parsed/printed using the
/// correct format
#[derive(Clone, Copy, From, Into, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
pub struct LastModified(pub NaiveDateTime);

const DATETIME_FMT: &str = "%d-%b-%Y %H:%M:%S";

impl FromStr for LastModified {
    type Err = LastModifiedError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (t, cc) = match &s.split(".").collect::<Vec<_>>()[..] {
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

impl fmt::Display for LastModified {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let dt = self.0.format(DATETIME_FMT);
        let cc = self.0.nanosecond() / 10_000_000;
        write!(f, "{dt}.{cc:02}")
    }
}

pub struct LastModifiedError;

impl fmt::Display for LastModifiedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "must be like 'dd-mmm-yyyy hh:mm:ss[.cc]'")
    }
}

/// The value for the $ORIGINALITY key (3.1+)
#[derive(Clone, Copy, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum Originality {
    Original,
    NonDataModified,
    Appended,
    DataModified,
}

impl FromStr for Originality {
    type Err = OriginalityError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Original" => Ok(Originality::Original),
            "NonDataModified" => Ok(Originality::NonDataModified),
            "Appended" => Ok(Originality::Appended),
            "DataModified" => Ok(Originality::DataModified),
            _ => Err(OriginalityError),
        }
    }
}

impl fmt::Display for Originality {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let x = match self {
            Originality::Appended => "Appended",
            Originality::Original => "Original",
            Originality::NonDataModified => "NonDataModified",
            Originality::DataModified => "DataModified",
        };
        write!(f, "{x}")
    }
}

pub struct OriginalityError;

impl fmt::Display for OriginalityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "Originality must be one of 'Original', 'NonDataModified', \
                   'Appended', or 'DataModified'"
        )
    }
}

/// The value of the $UNICODE key (3.0 only)
///
/// Formatted like 'codepage,[keys]'. This key is not actually used for anything
/// in this library and is present to be complete. The original purpose was to
/// indicate keywords which supported UTF-8, but these days it is hard to
/// write a library that does NOT support UTF-8 ;)
#[derive(Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct Unicode {
    pub page: u32,
    pub kws: Vec<String>,
}

impl FromStr for Unicode {
    type Err = UnicodeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut xs = s.split(",");
        if let Some(page) = xs.next().and_then(|x| x.parse().ok()) {
            let kws: Vec<String> = xs.map(String::from).collect();
            if kws.is_empty() {
                Err(UnicodeError::Empty)
            } else {
                Ok(Unicode { page, kws })
            }
        } else {
            Err(UnicodeError::BadFormat)
        }
    }
}

impl fmt::Display for Unicode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{},{}", self.page, self.kws.iter().join(","))
    }
}

pub enum UnicodeError {
    Empty,
    BadFormat,
}

impl fmt::Display for UnicodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            UnicodeError::Empty => write!(f, "No keywords given"),
            UnicodeError::BadFormat => write!(f, "Must be like 'n,string,[[string],...]'"),
        }
    }
}

/// The value of the $PnTYPE key in optical channels (3.2+)
#[derive(Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum OpticalType {
    ForwardScatter,
    SideScatter,
    RawFluorescence,
    UnmixedFluorescence,
    Mass,
    ElectronicVolume,
    Classification,
    Index,
    // TODO is isn't clear if this is allowed according to the standard
    Other(String),
}

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
            FORWARD_SCATTER => Ok(OpticalType::ForwardScatter),
            SIDE_SCATTER => Ok(OpticalType::SideScatter),
            RAW_FLUORESCENCE => Ok(OpticalType::RawFluorescence),
            UNMIXED_FLUORESCENCE => Ok(OpticalType::UnmixedFluorescence),
            MASS => Ok(OpticalType::Mass),
            ELECTRONIC_VOLUME => Ok(OpticalType::ElectronicVolume),
            INDEX => Ok(OpticalType::Index),
            CLASSIFICATION => Ok(OpticalType::Classification),
            s => Ok(OpticalType::Other(String::from(s))),
        }
    }
}

impl fmt::Display for OpticalType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            OpticalType::ForwardScatter => f.write_str(FORWARD_SCATTER),
            OpticalType::SideScatter => f.write_str(SIDE_SCATTER),
            OpticalType::RawFluorescence => f.write_str(RAW_FLUORESCENCE),
            OpticalType::UnmixedFluorescence => f.write_str(UNMIXED_FLUORESCENCE),
            OpticalType::Mass => f.write_str(MASS),
            OpticalType::ElectronicVolume => f.write_str(ELECTRONIC_VOLUME),
            OpticalType::Classification => f.write_str(CLASSIFICATION),
            OpticalType::Index => f.write_str(INDEX),
            OpticalType::Other(s) => write!(f, "{}", s),
        }
    }
}

impl fmt::Display for OpticalTypeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "$PnTYPE for time measurement shall not be 'Time' if given"
        )
    }
}

/// The value of the $PnTYPE key in temporal channels (3.2+)
#[derive(Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct TemporalType;

pub struct TemporalTypeError;

impl FromStr for TemporalType {
    type Err = TemporalTypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            TIME => Ok(TemporalType),
            _ => Err(TemporalTypeError),
        }
    }
}

impl fmt::Display for TemporalType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.write_str(TIME)
    }
}

impl fmt::Display for TemporalTypeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "$PnTYPE for time measurement shall be 'Time' if given")
    }
}

/// The value of the $PnFEATURE key (3.2+)
#[derive(Clone, Copy, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum Feature {
    Area,
    Width,
    Height,
}

const AREA: &str = "Area";
const WIDTH: &str = "Width";
const HEIGHT: &str = "Height";

impl FromStr for Feature {
    type Err = FeatureError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            AREA => Ok(Feature::Area),
            WIDTH => Ok(Feature::Width),
            HEIGHT => Ok(Feature::Height),
            _ => Err(FeatureError),
        }
    }
}

impl fmt::Display for Feature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let s = match self {
            Feature::Area => AREA,
            Feature::Width => WIDTH,
            Feature::Height => HEIGHT,
        };
        f.write_str(s)
    }
}

pub struct FeatureError;

impl fmt::Display for FeatureError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "must be one of 'Area', 'Width', or 'Height'")
    }
}

/// The value of the $RnI key (all versions)
#[derive(Clone, Copy)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub(crate) enum RegionGateIndex<I> {
    Univariate(I),
    Bivariate(I, I),
}

impl<I> RegionGateIndex<I> {
    pub(crate) fn lookup_region_opt<E>(
        kws: &mut StdKeywords,
        i: RegionIndex,
        par: Par,
    ) -> LookupTentative<MaybeValue<Self>, E>
    where
        I: fmt::Display + FromStr + gating::LinkedMeasIndex,
        Self: fmt::Display + FromStr,
        ParseOptKeyWarning: From<<Self as FromStr>::Err>,
    {
        process_opt(Self::remove_meas_opt(kws, i.into())).and_tentatively(|maybe| {
            if let Some(x) = maybe.0 {
                Self::check_link(&x, par).map_or_else(
                    |w| Tentative::new(None, vec![w.into()], vec![]),
                    |_| Tentative::new1(Some(x)),
                )
            } else {
                Tentative::default()
            }
            .map(|x| x.into())
        })
    }

    pub(crate) fn lookup_region_opt_dep(
        kws: &mut StdKeywords,
        i: RegionIndex,
        par: Par,
        disallow_dep: bool,
    ) -> LookupTentative<MaybeValue<Self>, DeprecatedError>
    where
        I: fmt::Display + FromStr + gating::LinkedMeasIndex,
        Self: fmt::Display + FromStr,
        ParseOptKeyWarning: From<<Self as FromStr>::Err>,
    {
        let mut x = Self::lookup_region_opt(kws, i, par);
        eval_dep_maybe(&mut x, Self::std(i.into()), disallow_dep);
        x
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
            RegionGateIndex::Univariate(x) => x.meas_index().into_iter().collect(),
            RegionGateIndex::Bivariate(x, y) => [x.meas_index(), y.meas_index()]
                .into_iter()
                .flatten()
                .collect(),
        }
    }
}

impl<I> FromStr for RegionGateIndex<I>
where
    I: FromStr,
{
    type Err = RegionGateIndexError<<I as FromStr>::Err>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.split(",").collect::<Vec<_>>()[..] {
            [x] => x
                .parse()
                .map(RegionGateIndex::Univariate)
                .map_err(RegionGateIndexError::Int),
            [x, y] => x
                .parse()
                .and_then(|a| y.parse().map(|b| RegionGateIndex::Bivariate(a, b)))
                .map_err(RegionGateIndexError::Int),
            _ => Err(RegionGateIndexError::Format),
        }
    }
}

impl<I> fmt::Display for RegionGateIndex<I>
where
    I: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            RegionGateIndex::Univariate(x) => write!(f, "{x}"),
            RegionGateIndex::Bivariate(x, y) => write!(f, "{x},{y}"),
        }
    }
}

pub enum RegionGateIndexError<E> {
    Format,
    Int(E),
}

impl<E> fmt::Display for RegionGateIndexError<E>
where
    E: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Self::Format => write!(f, "must be either a single value 'x' or a pair 'x,y'"),
            Self::Int(e) => e.fmt(f),
        }
    }
}

pub struct RegionIndexError(NonEmpty<MeasIndex>);

impl fmt::Display for RegionIndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "region index refers to non-existent measurement index: {}",
            self.0.iter().join(",")
        )
    }
}

#[derive(Clone, Copy, From, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum MeasOrGateIndex {
    Meas(MeasIndex),
    Gate(GateIndex),
}

impl FromStr for MeasOrGateIndex {
    type Err = MeasOrGateIndexError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some((prefix, rest)) = s.split_at_checked(1) {
            match prefix {
                "P" => rest
                    .parse::<MeasIndex>()
                    .map(|x| x.into())
                    .map_err(MeasOrGateIndexError::Int),
                "G" => rest
                    .parse::<GateIndex>()
                    .map(|x| x.into())
                    .map_err(MeasOrGateIndexError::Int),
                _ => Err(MeasOrGateIndexError::Format),
            }
        } else {
            Err(MeasOrGateIndexError::Format)
        }
    }
}

pub enum MeasOrGateIndexError {
    Int(ParseIntError),
    Format,
}

impl fmt::Display for MeasOrGateIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Self::Meas(x) => write!(f, "P{x}"),
            Self::Gate(x) => write!(f, "G{x}"),
        }
    }
}

impl fmt::Display for MeasOrGateIndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Self::Int(x) => x.fmt(f),
            Self::Format => write!(f, "must be prefixed with either 'P' or 'G'"),
        }
    }
}

#[derive(Clone, Copy, From, PartialEq, Into, AsMut)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[into(MeasIndex, usize)]
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

impl fmt::Display for PrefixedMeasIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "P{}", self.0)
    }
}

pub enum PrefixedMeasIndexError {
    Int(ParseIntError),
    Format,
}

impl fmt::Display for PrefixedMeasIndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Self::Int(x) => x.fmt(f),
            Self::Format => write!(f, "must be prefixed with 'P'"),
        }
    }
}

/// The value of the $RnW key (3.0-3.2)
///
/// This is meant to be used internally to construct a higher-level abstraction
/// over the gating keywords.
pub(crate) enum RegionWindow {
    Univariate(UniGate),
    Bivariate(NonEmpty<Vertex>),
}

#[derive(Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct Vertex {
    pub x: BigDecimal,
    pub y: BigDecimal,
}

#[derive(Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct UniGate {
    pub lower: BigDecimal,
    pub upper: BigDecimal,
}

impl FromStr for RegionWindow {
    type Err = GatePairError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // ASSUME split will always contain one element
        let ss = NonEmpty::collect(s.split(";")).unwrap();
        if ss.tail.is_empty() {
            ss.head.parse().map(RegionWindow::Univariate)
        } else {
            ss.try_map(|sub| sub.parse()).map(Self::Bivariate)
        }
    }
}

impl fmt::Display for RegionWindow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Self::Univariate(x) => x.fmt(f),
            Self::Bivariate(xs) => write!(f, "{}", xs.iter().join(";")),
        }
    }
}

impl FromStr for UniGate {
    type Err = GatePairError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_pair(s).map(|(lower, upper)| Self { lower, upper })
    }
}

impl FromStr for Vertex {
    type Err = GatePairError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_pair(s).map(|(x, y)| Self { x, y })
    }
}

fn parse_pair(s: &str) -> Result<(BigDecimal, BigDecimal), GatePairError> {
    match s.split(",").collect::<Vec<_>>()[..] {
        [a, b] => a
            .parse()
            .and_then(|x| b.parse().map(|y| (x, y)))
            .map_err(GatePairError::Num),
        _ => Err(GatePairError::Format),
    }
}

impl fmt::Display for UniGate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{},{}", self.lower, self.upper)
    }
}

impl fmt::Display for Vertex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{},{}", self.x, self.y)
    }
}

pub enum GatePairError {
    Num(ParseBigDecimalError),
    Format,
}

impl fmt::Display for GatePairError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Self::Num(e) => e.fmt(f),
            Self::Format => write!(f, "must be a string like 'f1,f2;[f3,f4;...]'"),
        }
    }
}

/// The value of the $GATING key (3.0-3.2)
#[derive(Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum Gating {
    Region(RegionIndex),
    Not(Box<Gating>),
    And(Box<Gating>, Box<Gating>),
    Or(Box<Gating>, Box<Gating>),
}

impl Gating {
    pub(crate) fn flatten(&self) -> NonEmpty<RegionIndex> {
        match self {
            Self::Region(x) => NonEmpty::new(*x),
            Self::Not(x) => Self::flatten(x),
            Self::And(x, y) => {
                let mut acc = Self::flatten(x);
                acc.extend(Self::flatten(y));
                acc
            }
            Self::Or(x, y) => {
                let mut acc = Self::flatten(x);
                acc.extend(Self::flatten(y));
                acc
            }
        }
        .unique()
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

impl fmt::Display for Gating {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Self::Region(r) => write!(f, "R{r}"),
            Self::Not(r) => write!(f, "(NOT {r})"),
            Self::And(a, b) => write!(f, "({a} AND {b})"),
            Self::Or(a, b) => write!(f, "({a} OR {b})"),
        }
    }
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

pub enum GatingError {
    Empty,
    ExpectedExpr,
    InvalidOpToken,
    InvalidExprToken,
    ExtraParen,
    MissingParen,
    NonAscii,
}

impl fmt::Display for GatingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let s = match self {
            Self::Empty => "gating string is empty",
            Self::InvalidExprToken => "expected '(', 'NOT', or 'Rn'",
            Self::InvalidOpToken => "expected 'AND', 'OR', or ')'",
            Self::ExpectedExpr => "expected expression which evaluates to a region",
            Self::ExtraParen => "extra ')' encountered",
            Self::MissingParen => "missing ')'",
            Self::NonAscii => "gating contains invalid bytes",
        };
        write!(f, "{s}")
    }
}

/// The value of the $PnR key.
#[derive(Clone, From, Display, FromStr, Add, Sub, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
#[from(u8, u16, u32, u64, BigDecimal)]
pub struct Range(pub BigDecimal);

impl Range {
    pub(crate) fn into_uint<T>(self, notrunc: bool) -> BiTentative<T, IntRangeError<()>>
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
        BiTentative::new_either1(b, e, notrunc)
    }

    pub(crate) fn into_float<T>(
        self,
        notrunc: bool,
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
        BiTentative::new_either1(x, e, notrunc)
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

pub struct IntRangeError<T> {
    src_type: &'static str,
    src_num: BigDecimal,
    error_kind: IntRangeErrorKind<T>,
}

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

/// The value of the $GmR key
#[derive(Clone, From, Display, FromStr, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[from(u64)]
pub struct GateRange(pub Range);

/// The value of the $PnO key
#[derive(Clone, Copy, From, Display, FromStr, Into, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
#[into(NonNegFloat, f32)]
pub struct Power(pub NonNegFloat);

impl_newtype_try_from!(Power, NonNegFloat, f32, RangedFloatError);

/// The value of the $PnV key
#[derive(Clone, Copy, From, Display, FromStr, Into, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
#[into(NonNegFloat, f32)]
pub struct DetectorVoltage(pub NonNegFloat);

impl_newtype_try_from!(DetectorVoltage, NonNegFloat, f32, RangedFloatError);

/// The value of the $GmV key
#[derive(Clone, Copy, Display, FromStr, Into, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[into(f32)]
pub struct GateDetectorVoltage(pub NonNegFloat);

impl_newtype_try_from!(GateDetectorVoltage, NonNegFloat, f32, RangedFloatError);

/// The value of the $GmE key
#[derive(Clone, Copy, Display, FromStr, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct GateScale(pub Scale);

// use the same fix we use for PnE here
impl GateScale {
    pub(crate) fn lookup_fixed_opt<E>(
        kws: &mut StdKeywords,
        i: GateIndex,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<MaybeValue<GateScale>, E> {
        Scale::lookup_fixed_opt(kws, usize::from(i).into(), conf).map(|x| x.map(GateScale))
    }

    pub(crate) fn lookup_fixed_opt_dep(
        kws: &mut StdKeywords,
        i: GateIndex,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<MaybeValue<GateScale>, DeprecatedError> {
        Scale::lookup_fixed_opt_dep(kws, usize::from(i).into(), conf).map(|x| x.map(GateScale))
    }
}

/// The value of the $CSVnFLAG key (2.0-3.0)
#[derive(Clone, Copy, Display, FromStr, Into, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[into(u32)]
pub struct CSVFlag(pub u32);

/// The value of the $PKn key (2.0-3.1)
#[derive(Clone, Copy, Display, FromStr, Into, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
#[into(u32)]
pub struct PeakBin(pub u32);

/// The value of the $PKNn key (2.0-3.1)
#[derive(Clone, Copy, Display, FromStr, Into, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
#[into(u32)]
pub struct PeakNumber(pub u32);

macro_rules! newtype_string {
    ($t:ident) => {
        #[derive(Clone, Display, FromStr, From, Into, PartialEq)]
        #[cfg_attr(feature = "serde", derive(Serialize))]
        #[cfg_attr(feature = "python", derive(FromPyObject, IntoPyObject))]
        pub struct $t(pub String);
    };
}

macro_rules! newtype_int {
    ($t:ident, $type:ty) => {
        #[derive(Clone, Copy, Display, FromStr, From, Into, PartialEq)]
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
    ($t:ident) => {
        impl Optional for $t {}
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
    ($t:ident) => {
        impl Optional for $t {}
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
    ($t:ident, $sfx:expr) => {
        kw_meta!($t, $sfx);
        opt_meta!($t);
    };
}

macro_rules! kw_req_meas {
    ($t:ident, $sfx:expr) => {
        kw_meas!($t, $sfx);
        req_meas!($t);
    };
}

macro_rules! kw_opt_meas {
    ($t:ident, $sfx:expr) => {
        kw_meas!($t, $sfx);
        opt_meas!($t);
    };
}

macro_rules! kw_opt_meta_string {
    ($t:ident, $sfx:expr) => {
        kw_meta_string!($t, $sfx);
        opt_meta!($t);
    };
}

macro_rules! kw_opt_meas_string {
    ($t:ident, $sfx:expr) => {
        kw_meas_string!($t, $sfx);
        opt_meas!($t);
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
        opt_meta!($t);
    };
}

macro_rules! kw_time {
    ($outer:ident, $wrap:ident, $inner:ident, $err:ident, $key:expr) => {
        type $outer = $wrap<$inner>;

        kw_opt_meta!($outer, $key);

        impl From<NaiveTime> for $outer {
            fn from(value: NaiveTime) -> Self {
                Xtim($inner(value))
            }
        }
    };
}

macro_rules! kw_opt_gate {
    ($t:ident, $sfx:expr) => {
        impl IndexedKey for $t {
            const PREFIX: &'static str = "G";
            const SUFFIX: &'static str = $sfx;
        }
        opt_meas!($t);
    };
}

macro_rules! kw_opt_gate_string {
    ($t:ident, $sfx:expr) => {
        newtype_string!($t);
        kw_opt_gate!($t, $sfx);
    };
}

macro_rules! kw_opt_region {
    ($t:ident, $sfx:expr) => {
        impl IndexedKey for $t {
            const PREFIX: &'static str = "R";
            const SUFFIX: &'static str = $sfx;
        }
        opt_meas!($t);
    };
}

// all versions
kw_req_meta!(AlphaNumType, "DATATYPE");
kw_opt_meta_int!(Abrt, u32, "ABRT");
kw_opt_meta_string!(Cytsn, "CYTSN");
kw_opt_meta_string!(Com, "COM");
kw_opt_meta_string!(Cells, "CELLS");
kw_opt_meta!(FCSDate, "DATE");
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

impl Optional for Trigger {}

impl OptLinkedKey for Trigger {
    fn names(&self) -> HashSet<&Shortname> {
        [&self.measurement].into_iter().collect()
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
kw_opt_meta!(Compensation3_0, "COMP");
kw_opt_meta!(Unicode, "UNICODE");

// for 3.0+
kw_req_meta!(Timestep, "TIMESTEP");

// for 3.1+
kw_opt_meta_string!(LastModifier, "LAST_MODIFIER");
kw_opt_meta!(Originality, "ORIGINALITY");
kw_opt_meta!(LastModified, "LAST_MODIFIED");

kw_opt_meta_string!(Plateid, "PLATEID");
kw_opt_meta_string!(Platename, "PLATENAME");
kw_opt_meta_string!(Wellid, "WELLID");

impl Key for Spillover {
    const C: &'static str = "SPILLOVER";
}

impl Optional for Spillover {}

kw_opt_meta!(Vol, "VOL");

// for 3.2+
kw_opt_meta_string!(Carrierid, "CARRIERID");
kw_opt_meta_string!(Carriertype, "CARRIERTYPE");
kw_opt_meta_string!(Locationid, "LOCATIONID");

kw_opt_meta!(BeginDateTime, "BEGINDATETIME");
kw_opt_meta!(EndDateTime, "ENDDATETIME");

impl Key for UnstainedCenters {
    const C: &'static str = "UNSTAINEDCENTERS";
}

impl Optional for UnstainedCenters {}

kw_opt_meta_string!(UnstainedInfo, "UNSTAINEDINFO");

kw_opt_meta_string!(Flowrate, "FLOWRATE");

// version-specific
kw_opt_meta_int!(Tot, usize, "TOT"); // optional in 2.0
req_meta!(Tot); // required in 3.0+

kw_req_meta!(Mode, "MODE"); // for 2.0-3.1
kw_opt_meta!(Mode3_2, "MODE"); // for 3.2+

kw_opt_meta_string!(Cyt, "CYT"); // optional for 2.0-3.1
req_meta!(Cyt); // required for 3.2+

kw_req_meta!(ByteOrd2_0, "BYTEORD"); // 2.0/3.0
kw_req_meta!(ByteOrd3_1, "BYTEORD"); // 3.1+

// all versions
kw_req_meas!(Width, "B");
kw_opt_meas_string!(Filter, "F");
kw_opt_meas!(Power, "O");
// TODO why is this a string?
kw_opt_meas_string!(PercentEmitted, "P");
kw_req_meas!(Range, "R");
kw_opt_meas_string!(Longname, "S");
kw_opt_meas_string!(DetectorType, "T");
kw_opt_meas!(DetectorVoltage, "V");

// 3.0+
kw_opt_meas!(Gain, "G");

// 3.1+
kw_opt_meas!(Display, "D");

// 3.2+
kw_opt_meas!(Feature, "FEATURE");
kw_opt_meas!(OpticalType, "TYPE");
kw_opt_meas!(TemporalType, "TYPE");
kw_opt_meas!(NumType, "DATATYPE");
kw_opt_meas_string!(Analyte, "ANALYTE");
kw_opt_meas_string!(Tag, "TAG");
kw_opt_meas_string!(DetectorName, "DET");

// version specific
kw_opt_meas!(Shortname, "N"); // optional for 2.0/3.0
req_meas!(Shortname); // required for 3.1+

kw_opt_meas!(Scale, "E"); // optional for 2.0
req_meas!(Scale); // required for 3.0+

kw_opt_meas!(TemporalScale, "E"); // optional for 2.0
req_meas!(TemporalScale); // required for 3.0+

kw_opt_meas!(Wavelength, "L"); // scaler in 2.0/3.0
kw_opt_meas!(Wavelengths, "L"); // vector in 3.1+

kw_opt_meas!(Calibration3_1, "CALIBRATION"); // 3.1 doesn't have offset
kw_opt_meas!(Calibration3_2, "CALIBRATION"); // 3.2+ includes offset

// 2.0 compensation matrix
pub struct Dfc;

impl BiIndexedKey for Dfc {
    const PREFIX: &'static str = "DFC";
    const MIDDLE: &'static str = "TO";
    const SUFFIX: &'static str = "";
}

// 3.0/3.1 subsets
kw_opt_meta_int!(CSMode, usize, "CSMODE");
kw_opt_meta_int!(CSVBits, u32, "CSVBits");

impl IndexedKey for CSVFlag {
    const PREFIX: &'static str = "CSV";
    const SUFFIX: &'static str = "FLAG";
}

impl Optional for CSVFlag {}
impl OptIndexedKey for CSVFlag {}

// 2.0-3.1 histogram peaks
impl IndexedKey for PeakBin {
    const PREFIX: &'static str = "PK";
    const SUFFIX: &'static str = "";
}

impl Optional for PeakBin {}
impl OptIndexedKey for PeakBin {}

impl IndexedKey for PeakNumber {
    const PREFIX: &'static str = "PKN";
    const SUFFIX: &'static str = "";
}

impl Optional for PeakNumber {}
impl OptIndexedKey for PeakNumber {}

// 2.0-3.1 gating parameters
kw_opt_meta_int!(Gate, usize, "GATE");

kw_opt_gate!(GateScale, "E");
kw_opt_gate_string!(GateFilter, "F");
kw_opt_gate_string!(GatePercentEmitted, "P");
kw_opt_gate!(GateRange, "R");
kw_opt_gate_string!(GateShortname, "N");
kw_opt_gate_string!(GateLongname, "S");
kw_opt_gate_string!(GateDetectorType, "T");
kw_opt_gate!(GateDetectorVoltage, "V");
kw_opt_meta!(Gating, "GATING");

kw_opt_region!(RegionWindow, "W");

impl<I> IndexedKey for RegionGateIndex<I> {
    const PREFIX: &'static str = "R";
    const SUFFIX: &'static str = "I";
}

impl<I> Optional for RegionGateIndex<I> {}
impl<I> OptIndexedKey for RegionGateIndex<I>
where
    I: fmt::Display,
    I: FromStr,
{
}

// offsets for all versions
kw_req_meta!(Nextdata, "NEXTDATA");

macro_rules! kw_offset {
    ($t:ident, $key:expr) => {
        /// Value for $$key (3.0-3.2)
        #[derive(Display, From, Into, FromStr)]
        pub struct $t(pub Uint20Char);

        kw_req_meta!($t, $key);
    };
}

kw_offset!(Beginanalysis, "BEGINANALYSIS");
kw_offset!(Begindata, "BEGINDATA");
kw_offset!(Beginstext, "BEGINSTEXT");
kw_offset!(Endanalysis, "ENDANALYSIS");
kw_offset!(Enddata, "ENDDATA");
kw_offset!(Endstext, "ENDSTEXT");

opt_meta!(Beginanalysis);
opt_meta!(Endanalysis);
opt_meta!(Beginstext);
opt_meta!(Endstext);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;

    #[test]
    fn test_tr() {
        assert_from_to_str::<Trigger>("Wooden Leg Pt 3,456");
    }

    #[test]
    fn test_mode() {
        assert_from_to_str::<Mode>("C");
        assert_from_to_str::<Mode>("L");
        assert_from_to_str::<Mode>("U");
    }

    #[test]
    fn test_mode_3_2() {
        assert_from_to_str::<Mode3_2>("L");
    }

    #[test]
    fn test_pnd() {
        assert_from_to_str::<Display>("Linear,0,1");
        // TODO seems like this shouldn't be allowed
        assert_from_to_str::<Display>("Linear,1,0");
        assert_from_to_str::<Display>("Logarithmic,1,1");
        // TODO this also seems like nonsense
        assert_from_to_str::<Display>("Logarithmic,1,0");
    }

    #[test]
    fn test_datatype() {
        assert_from_to_str::<NumType>("I");
        assert_from_to_str::<NumType>("F");
        assert_from_to_str::<NumType>("D");
    }

    #[test]
    fn test_pndatetype() {
        assert_from_to_str::<AlphaNumType>("I");
        assert_from_to_str::<AlphaNumType>("F");
        assert_from_to_str::<AlphaNumType>("D");
        assert_from_to_str::<AlphaNumType>("A");
    }

    #[test]
    fn test_pne_time() {
        assert_from_to_str::<TemporalScale>("0,0");
    }

    #[test]
    fn test_pncalibration_3_1() {
        assert_from_to_str::<Calibration3_1>("0.1,cubic imperial lightyears");
    }

    #[test]
    fn test_pncalibration_3_2() {
        assert_from_to_str::<Calibration3_2>("1.1,3.5813,progressive metal albums");
    }

    #[test]
    fn test_pnl_3_1() {
        assert_from_to_str::<Wavelengths>("1");
        assert_from_to_str::<Wavelengths>("1,2");
    }

    #[test]
    fn test_last_modified() {
        assert_from_to_str_almost::<LastModified>(
            "01-Jan-2112 00:00:00",
            "01-Jan-2112 00:00:00.00",
        );
        assert_from_to_str::<LastModified>("01-Jan-2112 00:00:00.01");
    }

    #[test]
    fn test_originality() {
        assert_from_to_str::<Originality>("Original");
        assert_from_to_str::<Originality>("NonDataModified");
        assert_from_to_str::<Originality>("Appended");
        assert_from_to_str::<Originality>("DataModified");
    }

    #[test]
    fn test_unicode() {
        assert_from_to_str::<Unicode>("42,$BYTEORD");
        // we don't actually check that the keyword is valid, likely nobody
        // will notice ;)
        assert_from_to_str::<Unicode>("42,$40DOLLARBILL");
    }

    #[test]
    fn test_pntype_optical() {
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
    fn test_pntype_time() {
        assert_from_to_str::<TemporalType>("Time");
    }

    #[test]
    fn test_pnfeature() {
        assert_from_to_str::<Feature>("Area");
        assert_from_to_str::<Feature>("Width");
        assert_from_to_str::<Feature>("Height");
    }

    #[test]
    fn test_rni_2_0() {
        assert_from_to_str::<RegionGateIndex<GateIndex>>("1");
        assert_from_to_str::<RegionGateIndex<GateIndex>>("1,2");
    }

    #[test]
    fn test_rni_3_0() {
        assert_from_to_str::<RegionGateIndex<MeasOrGateIndex>>("P1");
        assert_from_to_str::<RegionGateIndex<MeasOrGateIndex>>("P1,P2");
        assert_from_to_str::<RegionGateIndex<MeasOrGateIndex>>("G1");
        assert_from_to_str::<RegionGateIndex<MeasOrGateIndex>>("G1,G2");
    }

    #[test]
    fn test_rni_3_2() {
        assert_from_to_str::<RegionGateIndex<PrefixedMeasIndex>>("P1");
        assert_from_to_str::<RegionGateIndex<PrefixedMeasIndex>>("P1,P2");
    }

    #[test]
    fn test_rnw() {
        assert_from_to_str::<RegionWindow>("1,1");
        assert_from_to_str::<RegionWindow>("1,1;2,3;5,8;13,21");
    }

    #[test]
    fn test_gating() {
        assert_from_to_str::<Gating>("R1");
        assert_from_to_str_almost::<Gating>("R1 AND (R2.OR.R3)", "(R1 AND (R2 OR R3))");
        assert_from_to_str::<Gating>("((NOT R1) AND R2)");
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
        AlphaNumType, AlphaNumTypeError, Calibration3_1, Calibration3_2, DetectorVoltage, Display,
        Feature, FeatureError, LastModified, Mode, Mode3_2, Mode3_2Error, ModeError, NumType,
        NumTypeError, OpticalType, OpticalTypeError, Originality, OriginalityError, PeakBin,
        PeakNumber, Power, Range, Timestep, Trigger, Unicode, Vol, Wavelength, Wavelengths,
    };

    use nonempty::NonEmpty;
    use pyo3::exceptions::PyValueError;
    use pyo3::prelude::*;
    use pyo3::types::{PyList, PyTuple};

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

    impl_from_py_transparent!(Wavelength);
    impl_from_py_transparent!(Vol);
    impl_from_py_transparent!(Timestep);
    impl_from_py_transparent!(LastModified);
    impl_from_py_transparent!(Range);
    impl_from_py_transparent!(DetectorVoltage);
    impl_from_py_transparent!(Power);
    impl_from_py_transparent!(PeakBin);
    impl_from_py_transparent!(PeakNumber);

    // $PnL (3.1+) should be represented as a list of floats
    impl<'py> FromPyObject<'py> for Wavelengths {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let xs: Vec<PositiveFloat> = ob.extract()?;
            if let Some(ys) = NonEmpty::from_vec(xs) {
                Ok(ys.into())
            } else {
                Err(PyValueError::new_err("wavelengths must not be empty"))
            }
        }
    }

    impl<'py> IntoPyObject<'py> for Wavelengths {
        type Target = PyList;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            PyList::new(py, Vec::from(self.0))
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
            Ok(Trigger {
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
}

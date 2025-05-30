use crate::macros::{newtype_disp, newtype_from, newtype_from_outer, newtype_fromstr};
use crate::validated::nonstandard::*;
use crate::validated::shortname::*;
use crate::validated::standard::*;

use super::byteord::*;
use super::compensation::*;
use super::datetimes::*;
use super::float_or_int::*;
use super::named_vec::NameMapping;
use super::optionalkw::*;
use super::ranged_float::*;
use super::scale::*;
use super::spillover::*;
use super::timestamps::*;
use super::unstainedcenters::*;

use chrono::{NaiveDateTime, NaiveTime, Timelike};
use itertools::Itertools;
use nonempty::NonEmpty;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::fmt;
use std::num::{ParseFloatError, ParseIntError};
use std::str::FromStr;

/// The value of the $PnG keyword
#[derive(Clone, Copy, Serialize, PartialEq)]
pub struct Gain(pub PositiveFloat);

newtype_from!(Gain, PositiveFloat);
newtype_disp!(Gain);
newtype_fromstr!(Gain, RangedFloatError);

/// The value of the $TIMESTEP keyword
#[derive(Clone, Copy, Serialize)]
pub struct Timestep(pub PositiveFloat);

impl Default for Timestep {
    fn default() -> Self {
        // ASSUME this will never panic...1.0 is still a positive number, right?
        Timestep(PositiveFloat::try_from(1.0).ok().unwrap())
    }
}

newtype_disp!(Timestep);
newtype_fromstr!(Timestep, RangedFloatError);
newtype_from!(Timestep, PositiveFloat);

/// The value of the $VOL keyword
#[derive(Clone, Copy, Serialize)]
pub struct Vol(pub NonNegFloat);

newtype_from!(Vol, NonNegFloat);
newtype_disp!(Vol);
newtype_fromstr!(Vol, RangedFloatError);

/// The value of the $TR field (all versions)
///
/// This is formatted as 'string,f' where 'string' is a measurement name.
#[derive(Clone, Serialize)]
pub(crate) struct Trigger {
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

impl Linked for Trigger {
    fn names(&self) -> HashSet<&Shortname> {
        [&self.measurement].into_iter().collect()
    }

    fn reassign(&mut self, mapping: &NameMapping) {
        if let Some(new) = mapping.get(&self.measurement) {
            self.measurement = (*new).clone();
        }
    }
}

/// The values used for the $MODE key (up to 3.1)
#[derive(Clone, PartialEq, Eq, Serialize)]
pub enum Mode {
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

/// The value for the $PnDISPLAY key (3.1+)
#[derive(Clone, Copy, Serialize, PartialEq)]
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
            Display::Log { offset, decades } => write!(f, "Log,{offset},{decades}"),
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
#[derive(Clone, Copy, Serialize, PartialEq)]
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
#[derive(Clone, Copy, Eq, PartialEq, PartialOrd, Ord, Hash, Serialize)]
pub enum AlphaNumType {
    Ascii,
    Integer,
    Single,
    Double,
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

/// The value for the $PnCALIBRATION key (3.1 only)
///
/// This should be formatted like '<value>,<unit>'
#[derive(Clone, Serialize, PartialEq)]
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
#[derive(Clone, Serialize, PartialEq)]
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

/// The value for the $PnL key (3.1).
///
/// Starting in 3.1 this is a vector rather than a scaler.
#[derive(Clone)]
pub struct Wavelengths(pub NonEmpty<u32>);

impl Serialize for Wavelengths {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.iter().collect::<Vec<_>>().serialize(serializer)
    }
}

newtype_from!(Wavelengths, NonEmpty<u32>);
newtype_from_outer!(Wavelengths, NonEmpty<u32>);

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
            ws.push(x.parse().map_err(WavelengthsError::Int)?);
        }
        NonEmpty::from_vec(ws)
            .ok_or(WavelengthsError::Empty)
            .map(Wavelengths)
    }
}

pub enum WavelengthsError {
    Int(ParseIntError),
    Empty,
}

impl fmt::Display for WavelengthsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            WavelengthsError::Int(i) => write!(f, "{}", i),
            WavelengthsError::Empty => write!(f, "list must not be empty"),
        }
    }
}

/// A datetime as used in the $LAST_MODIFIED key (3.1+ only)
///
/// Inner value is private to ensure it always gets parsed/printed using the
/// correct format
#[derive(Clone, Copy, Serialize)]
pub struct ModifiedDateTime(pub NaiveDateTime);

newtype_from!(ModifiedDateTime, NaiveDateTime);
newtype_from_outer!(ModifiedDateTime, NaiveDateTime);

const DATETIME_FMT: &str = "%d-%b-%Y %H:%M:%S";

impl FromStr for ModifiedDateTime {
    type Err = ModifiedDateTimeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (dt, cc) =
            NaiveDateTime::parse_and_remainder(s, DATETIME_FMT).or(Err(ModifiedDateTimeError))?;
        if cc.is_empty() {
            Ok(ModifiedDateTime(dt))
        } else if cc.len() == 3 && cc.starts_with(".") {
            let tt: u32 = cc[1..3].parse().or(Err(ModifiedDateTimeError))?;
            dt.with_nanosecond(tt * 10000000)
                .map(ModifiedDateTime)
                .ok_or(ModifiedDateTimeError)
        } else {
            Err(ModifiedDateTimeError)
        }
    }
}

impl fmt::Display for ModifiedDateTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let dt = self.0.format(DATETIME_FMT);
        let cc = self.0.nanosecond() / 10000000;
        write!(f, "{dt}.{cc}")
    }
}

pub struct ModifiedDateTimeError;

impl fmt::Display for ModifiedDateTimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "must be like 'dd-mmm-yyyy hh:mm:ss[.cc]'")
    }
}

/// The value for the $ORIGINALITY key (3.1+)
#[derive(Clone, Copy, Serialize, PartialEq)]
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
#[derive(Clone, Serialize, PartialEq)]
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
#[derive(Clone, Serialize, PartialEq)]
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

impl FromStr for OpticalType {
    type Err = OpticalTypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Time" => Err(OpticalTypeError),
            "Forward Scatter" => Ok(OpticalType::ForwardScatter),
            "Side Scatter" => Ok(OpticalType::SideScatter),
            "Raw Fluorescence" => Ok(OpticalType::RawFluorescence),
            "Unmixed Fluorescence" => Ok(OpticalType::UnmixedFluorescence),
            "Mass" => Ok(OpticalType::Mass),
            "Electronic Volume" => Ok(OpticalType::ElectronicVolume),
            "Index" => Ok(OpticalType::Index),
            "Classification" => Ok(OpticalType::Classification),
            s => Ok(OpticalType::Other(String::from(s))),
        }
    }
}

impl fmt::Display for OpticalType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            OpticalType::ForwardScatter => write!(f, "Foward Scatter"),
            OpticalType::SideScatter => write!(f, "Side Scatter"),
            OpticalType::RawFluorescence => write!(f, "Raw Fluorescence"),
            OpticalType::UnmixedFluorescence => write!(f, "Unmixed Fluorescence"),
            OpticalType::Mass => write!(f, "Mass"),
            OpticalType::ElectronicVolume => write!(f, "Electronic Volume"),
            OpticalType::Classification => write!(f, "Classification"),
            OpticalType::Index => write!(f, "Index"),
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
#[derive(Clone, Serialize, PartialEq)]
pub struct TemporalType;

pub struct TemporalTypeError;

impl FromStr for TemporalType {
    type Err = TemporalTypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Time" => Ok(TemporalType),
            _ => Err(TemporalTypeError),
        }
    }
}

impl fmt::Display for TemporalType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "Time")
    }
}

impl fmt::Display for TemporalTypeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "$PnTYPE for time measurement shall be 'Time' if given")
    }
}

/// The value of the $PnFEATURE key (3.2+)
#[derive(Clone, Copy, Serialize, PartialEq)]
pub enum Feature {
    Area,
    Width,
    Height,
}

impl FromStr for Feature {
    type Err = FeatureError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Area" => Ok(Feature::Area),
            "Width" => Ok(Feature::Width),
            "Height" => Ok(Feature::Height),
            _ => Err(FeatureError),
        }
    }
}

impl fmt::Display for Feature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Feature::Area => write!(f, "Area"),
            Feature::Width => write!(f, "Width"),
            Feature::Height => write!(f, "Height"),
        }
    }
}

pub struct FeatureError;

impl fmt::Display for FeatureError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "must be one of 'Area', 'Width', or 'Height'")
    }
}

/// The value of the $RnI key (2.0)
#[derive(Clone, Copy, Serialize)]
pub(crate) struct GateRegionIndex2_0(pub(crate) GateRegionIndex);

newtype_from!(GateRegionIndex2_0, GateRegionIndex);
newtype_from_outer!(GateRegionIndex2_0, GateRegionIndex);

#[derive(Clone, Copy, Serialize)]
pub(crate) enum GateRegionIndex {
    Univariate(u32),
    Bivariate(u32, u32),
}

impl FromStr for GateRegionIndex2_0 {
    type Err = GateRegionIndex2_0Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.split(",").collect::<Vec<_>>()[..] {
            [x] => x
                .parse()
                .map(|a| GateRegionIndex::Univariate(a).into())
                .map_err(GateRegionError::Int),
            [x, y] => x
                .parse()
                .and_then(|a| y.parse().map(|b| GateRegionIndex::Bivariate(a, b).into()))
                .map_err(GateRegionError::Int),
            _ => Err(GateRegionError::Format(GateRegionFormat2_0)),
        }
    }
}

impl fmt::Display for GateRegionIndex2_0 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self.0 {
            GateRegionIndex::Univariate(x) => write!(f, "{x}"),
            GateRegionIndex::Bivariate(x, y) => write!(f, "{x},{y}"),
        }
    }
}

pub(crate) type GateRegionIndex2_0Error = GateRegionError<GateRegionFormat2_0>;
pub(crate) type GateRegionIndex3_0Error = GateRegionError<GateRegionFormat3_0>;

pub(crate) enum GateRegionError<F> {
    Format(F),
    Int(ParseIntError),
}

impl<E> fmt::Display for GateRegionError<E>
where
    E: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Self::Format(e) => e.fmt(f),
            Self::Int(e) => e.fmt(f),
        }
    }
}

pub(crate) struct GateRegionFormat2_0;

impl fmt::Display for GateRegionFormat2_0 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "must be string like 'n' or 'n1,n2'")
    }
}

/// The value of the $RnI key (3.0-3.2)
#[derive(Clone, Copy, Serialize)]
pub(crate) struct GateRegionIndex3_0 {
    /// Numeric links to gates
    pub(crate) index: GateRegionIndex,

    /// True if link points to $Gm* keys, false for $Pn* keys.
    pub(crate) is_gate: bool,
}

impl FromStr for GateRegionIndex3_0 {
    type Err = GateRegionIndex3_0Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let go = |sub: &str| {
            if let Some((prefix, rest)) = sub.split_at_checked(1) {
                match prefix {
                    "P" => Ok(false),
                    "G" => Ok(true),
                    _ => Err(GateRegionError::Format(GateRegionFormat3_0)),
                }
                .and_then(|is_gate| {
                    rest.parse()
                        .map_err(GateRegionError::Int)
                        .map(|x| (x, is_gate))
                })
            } else {
                Err(GateRegionError::Format(GateRegionFormat3_0))
            }
        };
        match s.split(",").collect::<Vec<_>>()[..] {
            [x] => go(x).map(|(a, is_gate)| Self {
                index: GateRegionIndex::Univariate(a),
                is_gate,
            }),
            [x, y] => go(x).and_then(|(a, a_is_gate)| {
                go(y).and_then(|(b, b_is_gate)| {
                    if a_is_gate == b_is_gate {
                        Ok(Self {
                            index: GateRegionIndex::Bivariate(a, b),
                            is_gate: a_is_gate,
                        })
                    } else {
                        Err(GateRegionError::Format(GateRegionFormat3_0))
                    }
                })
            }),
            _ => Err(GateRegionError::Format(GateRegionFormat3_0)),
        }
    }
}

impl fmt::Display for GateRegionIndex3_0 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let prefix = if self.is_gate { "G" } else { "P" };
        match self.index {
            GateRegionIndex::Univariate(x) => write!(f, "{prefix}{x}"),
            GateRegionIndex::Bivariate(x, y) => write!(f, "{prefix}{x},{prefix}{y}"),
        }
    }
}

pub(crate) struct GateRegionFormat3_0;

impl fmt::Display for GateRegionFormat3_0 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "must be string like 'Xn' or 'Xn1,Xn2' where X is either 'P' or 'G'"
        )
    }
}

/// The value of the RnW key (3.0-3.2)
///
/// This is meant to be used internally to construct a higher-level abstraction
/// over the gating keywords.
pub(crate) enum GateRegionWindow {
    Univariate(GatePair),
    Bivariate(NonEmpty<GatePair>),
}

impl FromStr for GateRegionWindow {
    type Err = GatePairError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // ASSUME split will always contain one element
        let ss = NonEmpty::collect(s.split(";")).unwrap();
        if ss.tail.is_empty() {
            ss.head.parse().map(GateRegionWindow::Univariate)
        } else {
            ss.try_map(|sub| sub.parse()).map(Self::Bivariate)
        }
    }
}

impl fmt::Display for GateRegionWindow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Self::Univariate(x) => x.fmt(f),
            Self::Bivariate(xs) => write!(f, "{}", xs.iter().join(";")),
        }
    }
}

pub(crate) struct GatePair {
    pub(crate) x: FloatOrInt,
    pub(crate) y: FloatOrInt,
}

impl FromStr for GatePair {
    type Err = GatePairError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.split(",").collect::<Vec<_>>()[..] {
            [a, b] => a
                .parse()
                .and_then(|x| b.parse().map(|y| GatePair { x, y }))
                .map_err(GatePairError::Num),
            _ => Err(GatePairError::Format),
        }
    }
}

impl fmt::Display for GatePair {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{},{}", self.x, self.y)
    }
}

pub(crate) enum GatePairError {
    Num(ParseFloatOrIntError),
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

/// The value of the $PnR key
#[derive(Clone, Copy, Serialize)]
pub struct Range(pub FloatOrInt);

newtype_from!(Range, FloatOrInt);
newtype_from_outer!(Range, FloatOrInt);
newtype_disp!(Range);
newtype_fromstr!(Range, ParseFloatOrIntError);

impl TryFrom<f64> for Range {
    type Error = NanFloatOrInt;
    fn try_from(value: f64) -> Result<Self, Self::Error> {
        FloatOrInt::try_from(value).map(|x| x.into())
    }
}

impl From<u64> for Range {
    fn from(value: u64) -> Self {
        FloatOrInt::from(value).into()
    }
}

/// The value of the $GmR key
#[derive(Clone, Copy, Serialize)]
pub struct GateRange(pub FloatOrInt);

newtype_from!(GateRange, FloatOrInt);
newtype_from_outer!(GateRange, FloatOrInt);
newtype_disp!(GateRange);
newtype_fromstr!(GateRange, ParseFloatOrIntError);

impl TryFrom<f64> for GateRange {
    type Error = NanFloatOrInt;
    fn try_from(value: f64) -> Result<Self, Self::Error> {
        FloatOrInt::try_from(value).map(|x| x.into())
    }
}

impl From<u64> for GateRange {
    fn from(value: u64) -> Self {
        FloatOrInt::from(value).into()
    }
}

/// The value of the $PnV key
#[derive(Clone, Copy, Serialize)]
pub struct DetectorVoltage(pub NonNegFloat);

newtype_from!(DetectorVoltage, NonNegFloat);
newtype_disp!(DetectorVoltage);
newtype_fromstr!(DetectorVoltage, RangedFloatError);

/// The value of the $GmV key
#[derive(Clone, Copy, Serialize)]
pub struct GateDetectorVoltage(pub NonNegFloat);

newtype_from!(GateDetectorVoltage, NonNegFloat);
newtype_disp!(GateDetectorVoltage);
newtype_fromstr!(GateDetectorVoltage, RangedFloatError);

/// The value of the $GmE key
#[derive(Clone, Copy, Serialize)]
pub struct GateScale(pub Scale);

newtype_from!(GateScale, Scale);
newtype_disp!(GateScale);
newtype_fromstr!(GateScale, ScaleError);

/// The value of the $CSVnFLAG key (2.0-3.0)
#[derive(Clone, Copy, Serialize)]
pub struct CSVFlag(pub u32);

newtype_from!(CSVFlag, u32);
newtype_from_outer!(CSVFlag, u32);
newtype_disp!(CSVFlag);
newtype_fromstr!(CSVFlag, ParseIntError);

/// The value of the $PKn key (2.0-3.1)
#[derive(Clone, Copy, Serialize)]
pub struct PeakBin(pub u32);

newtype_from!(PeakBin, u32);
newtype_from_outer!(PeakBin, u32);
newtype_disp!(PeakBin);
newtype_fromstr!(PeakBin, ParseIntError);

/// The value of the $PKNn key (2.0-3.1)
#[derive(Clone, Copy, Serialize)]
pub struct PeakNumber(pub u32);

newtype_from!(PeakNumber, u32);
newtype_from_outer!(PeakNumber, u32);
newtype_disp!(PeakNumber);
newtype_fromstr!(PeakNumber, ParseIntError);

pub(crate) type RawKeywords = HashMap<String, String>;
pub(crate) type OptKwResult<T> = Result<OptionalKw<T>, ParseKeyError<<T as FromStr>::Err>>;

pub(crate) trait Required {
    fn get_req<V>(kws: &StdKeywords, k: StdKey) -> ReqResult<V>
    where
        V: FromStr,
    {
        get_req(kws, k)
    }

    fn remove_req<V>(kws: &mut StdKeywords, k: StdKey) -> ReqResult<V>
    where
        V: FromStr,
    {
        remove_req(kws, k)
    }
}

pub(crate) trait Optional {
    fn get_opt<V>(kws: &StdKeywords, k: StdKey) -> OptKwResult<V>
    where
        V: FromStr,
    {
        get_opt(kws, k).map(|x| x.into())
    }

    fn remove_opt<V>(
        kws: &mut StdKeywords,
        k: StdKey,
    ) -> Result<OptionalKw<V>, ParseKeyError<V::Err>>
    where
        V: FromStr,
    {
        remove_opt(kws, k).map(|x| x.into())
    }
}

pub(crate) trait ReqMetaKey
where
    Self: Required,
    Self: fmt::Display,
    Self: Key,
    Self: FromStr,
{
    fn get_meta_req(kws: &StdKeywords) -> ReqResult<Self> {
        Self::get_req(kws, Self::std())
    }

    fn remove_meta_req(kws: &mut StdKeywords) -> ReqResult<Self> {
        Self::remove_req(kws, Self::std())
    }

    fn pair(&self) -> (String, String) {
        (Self::std().to_string(), self.to_string())
    }
}

pub(crate) trait ReqMeasKey
where
    Self: Required,
    Self: fmt::Display,
    Self: IndexedKey,
    Self: FromStr,
{
    fn get_meas_req(kws: &StdKeywords, n: MeasIdx) -> ReqResult<Self> {
        Self::get_req(kws, Self::std(n))
    }

    fn remove_meas_req(kws: &mut StdKeywords, n: MeasIdx) -> ReqResult<Self> {
        Self::remove_req(kws, Self::std(n))
    }

    fn triple(&self, n: MeasIdx) -> (String, String, String) {
        (
            Self::std_blank(),
            Self::std(n).to_string(),
            self.to_string(),
        )
    }

    fn pair(&self, n: MeasIdx) -> (String, String) {
        let (_, k, v) = self.triple(n);
        (k, v)
    }
}

pub(crate) trait OptMetaKey
where
    Self: Optional,
    Self: fmt::Display,
    Self: Key,
    Self: FromStr,
{
    fn get_meta_opt(kws: &StdKeywords) -> OptKwResult<Self> {
        Self::get_opt(kws, Self::std())
    }

    fn remove_meta_opt(kws: &mut StdKeywords) -> OptKwResult<Self> {
        Self::remove_opt(kws, Self::std())
    }

    fn pair(opt: &OptionalKw<Self>) -> (String, Option<String>) {
        (
            Self::std().to_string(),
            opt.0.as_ref().map(|s| s.to_string()),
        )
    }
}

pub(crate) trait OptMeasKey
where
    Self: Optional,
    Self: fmt::Display,
    Self: IndexedKey,
    Self: FromStr,
{
    fn get_meas_opt(kws: &StdKeywords, n: MeasIdx) -> OptKwResult<Self> {
        Self::get_opt(kws, Self::std(n))
    }

    fn remove_meas_opt(kws: &mut StdKeywords, n: MeasIdx) -> OptKwResult<Self> {
        Self::remove_opt(kws, Self::std(n))
    }

    fn triple(opt: &OptionalKw<Self>, n: MeasIdx) -> (String, String, Option<String>) {
        (
            Self::std_blank(),
            Self::std(n).to_string(),
            opt.0.as_ref().map(|s| s.to_string()),
        )
    }

    fn pair(opt: &OptionalKw<Self>, n: MeasIdx) -> (String, Option<String>) {
        let (_, k, v) = Self::triple(opt, n);
        (k, v)
    }
}

pub(crate) trait Linked
where
    Self: Key,
    Self: Sized,
{
    fn check_link(&self, names: &HashSet<&Shortname>) -> Result<(), LinkedNameError> {
        NonEmpty::collect(self.names().difference(names).copied().cloned())
            .map(|common_names| LinkedNameError {
                names: common_names,
                key: Self::std(),
            })
            .map(Err)
            .unwrap_or(Ok(()))
    }

    fn reassign(&mut self, mapping: &NameMapping);

    fn names(&self) -> HashSet<&Shortname>;
}

pub struct LinkedNameError {
    pub key: StdKey,
    pub names: NonEmpty<Shortname>,
}

impl fmt::Display for LinkedNameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let ns = if self.names.tail.is_empty() {
            "name"
        } else {
            "names"
        };
        let bad = self.names.iter().join(", ");
        write!(f, "{} references non-existent $PnN {ns}: {bad}", self.key)
    }
}

macro_rules! newtype_string {
    ($t:ident) => {
        #[derive(Clone, Serialize)]
        pub struct $t(pub String);

        newtype_disp!($t);
        newtype_fromstr!($t, Infallible);
        newtype_from!($t, String);
        newtype_from_outer!($t, String);
    };
}

macro_rules! newtype_int {
    ($t:ident, $type:ident) => {
        #[derive(Clone, Copy, Serialize)]
        pub struct $t(pub $type);

        newtype_disp!($t);
        newtype_fromstr!($t, ParseIntError);
        newtype_from!($t, $type);
        newtype_from_outer!($t, $type);
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

macro_rules! kw_meas_int {
    ($t:ident, $type:ident, $sfx:expr) => {
        newtype_int!($t, $type);
        kw_meas!($t, $sfx);
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
        impl ReqMetaKey for $t {}
    };
}

macro_rules! opt_meta {
    ($t:ident) => {
        impl Optional for $t {}
        impl OptMetaKey for $t {}
    };
}

macro_rules! req_meas {
    ($t:ident) => {
        impl Required for $t {}
        impl ReqMeasKey for $t {}
    };
}

macro_rules! opt_meas {
    ($t:ident) => {
        impl Optional for $t {}
        impl OptMeasKey for $t {}
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

macro_rules! kw_opt_meas_int {
    ($t:ident, $type:ident, $sfx:expr) => {
        kw_meas_int!($t, $type, $sfx);
        opt_meas!($t);
    };
}

macro_rules! kw_time {
    ($outer:ident, $wrap:ident, $inner:ident, $err:ident, $key:expr) => {
        type $outer = $wrap<$inner>;

        impl From<$inner> for $outer {
            fn from(value: $inner) -> Self {
                $wrap(value)
            }
        }

        impl FromStr for $outer {
            type Err = $err;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                s.parse().map($wrap)
            }
        }

        newtype_from_outer!($outer, $inner);
        newtype_disp!($outer);
        kw_opt_meta!($outer, $key);

        impl From<NaiveTime> for $outer {
            fn from(value: NaiveTime) -> Self {
                $wrap($inner(value))
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
kw_opt_meta!(Trigger, "TR");

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
kw_opt_meta!(Compensation, "COMP");
kw_opt_meta!(Unicode, "UNICODE");

// for 3.0+
kw_req_meta!(Timestep, "TIMESTEP");

// for 3.1+
kw_opt_meta_string!(LastModifier, "LAST_MODIFIER");
kw_opt_meta!(Originality, "ORIGINALITY");
kw_opt_meta!(ModifiedDateTime, "LAST_MODIFIED");

kw_opt_meta_string!(Plateid, "PLATEID");
kw_opt_meta_string!(Platename, "PLATENAME");
kw_opt_meta_string!(Wellid, "WELLID");

kw_opt_meta!(Spillover, "SPILLOVER");

kw_opt_meta!(Vol, "VOL");

// for 3.2+
kw_opt_meta_string!(Carrierid, "CARRIERID");
kw_opt_meta_string!(Carriertype, "CARRIERTYPE");
kw_opt_meta_string!(Locationid, "LOCATIONID");

kw_opt_meta!(BeginDateTime, "BEGINDATETIME");
kw_opt_meta!(EndDateTime, "ENDDATETIME");

kw_opt_meta!(UnstainedCenters, "UNSTAINEDCENTERS");
kw_opt_meta_string!(UnstainedInfo, "UNSTAINEDINFO");

kw_opt_meta_string!(Flowrate, "FLOWRATE");

// version-specific
kw_opt_meta_int!(Tot, usize, "TOT"); // optional in 2.0
req_meta!(Tot); // required in 3.0+

kw_req_meta!(Mode, "MODE"); // for 2.0-3.1
kw_opt_meta!(Mode3_2, "MODE"); // for 3.2+

kw_opt_meta_string!(Cyt, "CYT"); // optional for 2.0-3.1
req_meta!(Cyt); // required for 3.2+

kw_req_meta!(Endian, "BYTEORD"); // 2.0 to 3.0
kw_req_meta!(ByteOrd, "BYTEORD"); // 3.1+

// all versions
kw_req_meas!(Width, "B");
kw_opt_meas_string!(Filter, "F");
kw_opt_meas_int!(Power, u32, "O");
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

kw_opt_meas_int!(Wavelength, u32, "L"); // scaler in 2.0/3.0
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
kw_opt_meta_int!(CSMode, u32, "CSMODE");
kw_opt_meta_int!(CSVBits, u32, "CSVBits");

impl IndexedKey for CSVFlag {
    const PREFIX: &'static str = "CSV";
    const SUFFIX: &'static str = "FLAG";
}

impl Optional for CSVFlag {}
impl OptMeasKey for CSVFlag {}

// 2.0-3.1 histogram peaks
impl IndexedKey for PeakBin {
    const PREFIX: &'static str = "PK";
    const SUFFIX: &'static str = "";
}

impl Optional for PeakBin {}
impl OptMeasKey for PeakBin {}

impl IndexedKey for PeakNumber {
    const PREFIX: &'static str = "PKK";
    const SUFFIX: &'static str = "";
}

impl Optional for PeakNumber {}
impl OptMeasKey for PeakNumber {}

// 2.0-3.1 gating parameters
kw_opt_meta_int!(Gate, u32, "GATE");

kw_opt_gate!(GateScale, "E");
kw_opt_gate_string!(GateFilter, "F");
kw_opt_gate_string!(GatePercentEmitted, "P");
kw_opt_gate!(GateRange, "R");
kw_opt_gate_string!(GateShortname, "N");
kw_opt_gate_string!(GateLongname, "S");
kw_opt_gate_string!(GateDetectorType, "T");
kw_opt_gate!(GateDetectorVoltage, "V");

kw_opt_region!(GateRegionIndex2_0, "I");
kw_opt_region!(GateRegionIndex3_0, "I");
kw_opt_region!(GateRegionWindow, "W");

// offsets for all versions
kw_req_meta_int!(Beginanalysis, u32, "BEGINANALYSIS");
kw_req_meta_int!(Begindata, u32, "BEGINDATA");
kw_req_meta_int!(Beginstext, u32, "BEGINSTEXT");
kw_req_meta_int!(Endanalysis, u32, "ENDANALYSIS");
kw_req_meta_int!(Enddata, u32, "ENDDATA");
kw_req_meta_int!(Endstext, u32, "ENDSTEXT");
kw_req_meta_int!(Nextdata, u32, "NEXTDATA");

opt_meta!(Beginanalysis);
opt_meta!(Endanalysis);
opt_meta!(Beginstext);
opt_meta!(Endstext);

use crate::macros::{newtype_disp, newtype_from, newtype_from_outer, newtype_fromstr};
use crate::validated::nonstandard::*;
use crate::validated::shortname::*;

use super::byteord::*;
use super::compensation::*;
use super::datetimes::*;
use super::named_vec::NameMapping;
use super::optionalkw::*;
use super::ranged_float::*;
use super::scale::*;
use super::spillover::*;
use super::timestamps::*;
use super::unstainedcenters::*;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use itertools::Itertools;
use nonempty::NonEmpty;
use regex::Regex;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::fmt;
use std::num::{ParseFloatError, ParseIntError};
use std::str::FromStr;

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

/// The value for the $PnB key (all versions)
///
/// The $PnB key actually stores bits. However, this library only supports
/// widths that are multiples of 8 (ie bytes) for now. Therefore, this key
/// actually stores the number of bytes indicated by $PnB.
///
/// This may also be '*' which means "delimited ASCII" which is only valid when
/// $DATATYPE=A.
#[derive(Clone, Copy, Serialize)]
// TODO this will be off by 8x for ascii
pub enum Bytes {
    Fixed(u8),
    Variable,
}

/// The values used for the $MODE key (up to 3.1)
#[derive(Clone, PartialEq, Eq, Serialize)]
pub enum Mode {
    List,
    // TODO I have no idea what these even mean and IDK how to support them
    Uncorrelated,
    Correlated,
}

/// The value for the $MODE key, which can only contain 'L' (3.2)
pub struct Mode3_2;

/// The value for the $PnDISPLAY key (3.1+)
#[derive(Clone, Serialize)]
pub enum Display {
    /// Linear display (value like 'Linear,<lower>,<upper>')
    Lin { lower: f32, upper: f32 },

    // TODO not clear if these can be <0
    /// Logarithmic display (value like 'Logarithmic,<offset>,<decades>')
    Log { offset: f32, decades: f32 },
}

/// The three values for the $PnDATATYPE keyword (3.2+)
#[derive(Clone, Copy, Serialize)]
pub enum NumType {
    Integer,
    Single,
    Double,
}

/// The value of the $PnR key (all versions)
///
/// Technically this should only be an integer, but many versions also store
/// floats which makes sense for cases where $DATATYPE/$PnDATATYPE indicates
/// float or double.
#[derive(Clone, Serialize)]
pub struct Range(pub String);

newtype_disp!(Range);
newtype_fromstr!(Range, Infallible);

/// The value of the $PnV key
#[derive(Clone, Copy, Serialize)]
pub struct DetectorVoltage(pub NonNegFloat);

newtype_from!(DetectorVoltage, NonNegFloat);
newtype_disp!(DetectorVoltage);
newtype_fromstr!(DetectorVoltage, RangedFloatError);

/// The four allowed values for the $DATATYPE keyword.
#[derive(Clone, Copy, Eq, PartialEq, PartialOrd, Ord, Hash, Serialize)]
pub enum AlphaNumType {
    Ascii,
    Integer,
    Single,
    Double,
}

/// A time as used in the $BTIM/ETIM keys without seconds (2.0 only)
#[derive(Clone, Copy, Serialize, PartialEq, Eq, PartialOrd)]
pub struct FCSTime(pub NaiveTime);

newtype_from!(FCSTime, NaiveTime);
newtype_from_outer!(FCSTime, NaiveTime);

/// A time as used in the $BTIM/ETIM keys with 1/60 seconds (3.0 only)
#[derive(Clone, Copy, Serialize, PartialEq, Eq, PartialOrd)]
pub struct FCSTime60(pub NaiveTime);

newtype_from!(FCSTime60, NaiveTime);
newtype_from_outer!(FCSTime60, NaiveTime);

/// A time as used in the $BTIM/ETIM keys with centiseconds (3.1+ only)
#[derive(Clone, Copy, Serialize, PartialEq, Eq, PartialOrd)]
pub struct FCSTime100(pub NaiveTime);

newtype_from!(FCSTime100, NaiveTime);
newtype_from_outer!(FCSTime100, NaiveTime);

/// The value for the $PnCALIBRATION key (3.1 only)
///
/// This should be formatted like '<value>,<unit>'
#[derive(Clone, Serialize)]
pub struct Calibration3_1 {
    pub value: f32,
    pub unit: String,
}

/// The value for the $PnCALIBRATION key (3.2+)
///
/// This should be formatted like '<value>,[<offset>,]<unit>' and differs from
/// 3.1 with the optional inclusion of "offset" (assumed 0 if not included).
#[derive(Clone, Serialize)]
pub struct Calibration3_2 {
    pub value: f32,
    pub offset: f32,
    pub unit: String,
}

/// The value for the $PnL key (3.1).
///
/// This is a list of wavelengths used for the measurement. Starting in 3.1
/// this could be a list, where it needed to be a single number in previous
/// versions.
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

/// The value for the $ORIGINALITY key (3.1+)
#[derive(Clone, Copy, Serialize)]
pub enum Originality {
    Original,
    NonDataModified,
    Appended,
    DataModified,
}

/// A datetime as used in the $LAST_MODIFIED key (3.1+ only)
// TODO this should almost certainly be after $ENDDATETIME if given
#[derive(Clone, Copy, Serialize)]
pub struct ModifiedDateTime(pub NaiveDateTime);

newtype_from!(ModifiedDateTime, NaiveDateTime);
newtype_from_outer!(ModifiedDateTime, NaiveDateTime);

/// The value of the $UNICODE key (3.0 only)
///
/// Formatted like 'codepage,[keys]'. This key is not actually used for anything
/// in this library and is present to be complete. The original purpose was to
/// indicate keywords which supported UTF-8, but these days it is hard to
/// write a library that does NOT support UTF-8 ;)
#[derive(Clone, Serialize)]
pub struct Unicode {
    page: u32,
    // TODO check that these are valid keywords (probably not worth it)
    kws: Vec<String>,
}

// TODO split off Time to time-channel specific struct
/// The value of the $PnTYPE key (3.2+)
#[derive(Clone, Serialize, PartialEq)]
pub enum MeasurementType {
    ForwardScatter,
    SideScatter,
    RawFluorescence,
    UnmixedFluorescence,
    Mass,
    Time,
    ElectronicVolume,
    Classification,
    Index,
    // TODO is isn't clear if this is allowed according to the standard
    Other(String),
}

/// The value of the $PnFEATURE key (3.2+)
#[derive(Clone, Serialize)]
pub enum Feature {
    Area,
    Width,
    Height,
}

pub(crate) trait IndexedKey {
    const PREFIX: &'static str;
    const SUFFIX: &'static str;

    fn fmt(i: MeasIdx) -> String {
        format!("{}{i}{}", Self::PREFIX, Self::SUFFIX)
    }

    fn fmt_blank() -> String {
        format!("{}n{}", Self::PREFIX, Self::SUFFIX)
    }

    // fn fmt_sub() -> String {
    //     format!("{}%n{}", Self::PREFIX, Self::SUFFIX)
    // }

    fn std(i: MeasIdx) -> String {
        format!("${}", Self::fmt(i))
    }

    fn std_blank() -> String {
        format!("${}", Self::fmt_blank())
    }

    // fn nonstd(i: MeasIdx) -> NonStdKey {
    //     NonStdKey::from_unchecked(Self::fmt(i).as_str())
    // }

    // fn nonstd_sub() -> NonStdMeasKey {
    //     NonStdMeasKey::from_unchecked(Self::fmt_sub().as_str())
    // }

    // /// Return true if a key matches the prefix/suffix.
    // ///
    // /// Specifically, test if string is like <PREFIX><N><SUFFIX> where
    // /// N is an integer greater than zero.
    // fn matches(other: &str, std: bool) -> bool {
    //     if std {
    //         other.strip_prefix("$")
    //     } else {
    //         Some(other)
    //     }
    //     .and_then(|s| s.strip_prefix(Self::PREFIX))
    //     .and_then(|s| s.strip_suffix(Self::SUFFIX))
    //     .and_then(|s| s.parse::<u32>().ok())
    //     .is_some_and(|x| x > 0)
    // }
}

pub(crate) trait Key {
    const C: &'static str;

    fn std() -> String {
        format!("${}", Self::C)
    }

    // fn nonstd() -> NonStdKey {
    //     NonStdKey::from_unchecked(Self::C)
    // }
}

/// Raw TEXT key/value pairs
pub(crate) type RawKeywords = HashMap<String, String>;

type ReqResult<T> = Result<T, String>;
type OptResult<T> = Result<OptionalKw<T>, String>;

pub(crate) fn lookup_req<T>(kws: &mut RawKeywords, k: &str) -> Result<T, String>
where
    T: FromStr,
    <T as FromStr>::Err: fmt::Display,
{
    match kws.remove(k) {
        Some(v) => v
            .parse()
            .map_err(|e| format!("{e} (key='{k}', value='{v}')")),
        None => Err(format!("missing required key: {k}")),
    }
}

pub(crate) fn lookup_opt<T>(kws: &mut RawKeywords, k: &str) -> Result<Option<T>, String>
where
    T: FromStr,
    <T as FromStr>::Err: fmt::Display,
{
    match kws.remove(k) {
        Some(v) => v
            .parse()
            .map(Some)
            .map_err(|e| format!("{e} (key='{k}', value='{v}')")),
        None => Ok(None),
    }
}

pub(crate) trait Required {
    fn lookup_req<V>(kws: &mut RawKeywords, k: &str) -> Result<V, String>
    where
        V: FromStr,
        <V as FromStr>::Err: fmt::Display,
    {
        lookup_req(kws, k)
    }
}

pub(crate) trait Optional {
    fn lookup_opt<V>(kws: &mut RawKeywords, k: &str) -> Result<OptionalKw<V>, String>
    where
        V: FromStr,
        <V as FromStr>::Err: fmt::Display,
    {
        lookup_opt(kws, k).map(|x| x.into())
    }
}

pub(crate) trait ReqMetaKey
where
    Self: Required,
    Self: fmt::Display,
    Self: Key,
    Self: FromStr,
    <Self as FromStr>::Err: fmt::Display,
{
    fn lookup_meta_req(kws: &mut RawKeywords) -> ReqResult<Self> {
        Self::lookup_req(kws, Self::std().as_str())
    }

    fn pair(&self) -> (String, String) {
        (Self::std(), self.to_string())
    }
}

pub(crate) trait ReqMeasKey
where
    Self: Required,
    Self: fmt::Display,
    Self: IndexedKey,
    Self: FromStr,
    <Self as FromStr>::Err: fmt::Display,
{
    fn lookup_meas_req(kws: &mut RawKeywords, n: MeasIdx) -> ReqResult<Self> {
        Self::lookup_req(kws, Self::std(n).as_str())
    }

    fn triple(&self, n: MeasIdx) -> (String, String, String) {
        (Self::std_blank(), Self::std(n), self.to_string())
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
    <Self as FromStr>::Err: fmt::Display,
{
    fn lookup_meta_opt(kws: &mut RawKeywords) -> OptResult<Self> {
        Self::lookup_opt(kws, Self::std().as_str())
    }

    fn pair(opt: &OptionalKw<Self>) -> (String, Option<String>) {
        (Self::std(), opt.0.as_ref().map(|s| s.to_string()))
    }
}

pub(crate) trait OptMeasKey
where
    Self: Optional,
    Self: fmt::Display,
    Self: IndexedKey,
    Self: FromStr,
    <Self as FromStr>::Err: fmt::Display,
{
    fn lookup_meas_opt(kws: &mut RawKeywords, n: MeasIdx) -> OptResult<Self> {
        Self::lookup_opt(kws, Self::std(n).as_str())
    }

    fn triple(opt: &OptionalKw<Self>, n: MeasIdx) -> (String, String, Option<String>) {
        (
            Self::std_blank(),
            Self::std(n),
            opt.0.as_ref().map(|s| s.to_string()),
        )
    }

    fn pair(opt: &OptionalKw<Self>, n: MeasIdx) -> (String, Option<String>) {
        let (_, k, v) = Self::triple(opt, n);
        (k, v)
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

kw_opt_meta_string!(Cyt, "CYT");
req_meta!(Cyt);

kw_opt_meta_string!(Cytsn, "CYTSN");
kw_opt_meta_string!(Com, "COM");
kw_opt_meta_string!(Flowrate, "FLOWRATE");
kw_opt_meta_string!(Cells, "CELLS");
kw_opt_meta_string!(Exp, "EXP");
kw_opt_meta_string!(Fil, "FIL");
kw_opt_meta_string!(Inst, "INST");
kw_opt_meta_string!(Op, "OP");
kw_opt_meta_string!(Proj, "PROJ");
kw_opt_meta_string!(Smno, "SMNO");
kw_opt_meta_string!(Src, "SRC");
kw_opt_meta_string!(Sys, "SYS");
kw_opt_meta_string!(LastModifier, "LAST_MODIFIER");
kw_opt_meta_string!(Plateid, "PLATEID");
kw_opt_meta_string!(Platename, "PLATENAME");
kw_opt_meta_string!(Wellid, "WELLID");
kw_opt_meta_string!(UnstainedInfo, "UNSTAINEDINFO");
kw_opt_meta_string!(Carrierid, "CARRIERID");
kw_opt_meta_string!(Carriertype, "CARRIERTYPE");
kw_opt_meta_string!(Locationid, "LOCATIONID");

kw_opt_meas_string!(Analyte, "ANALYTE");
kw_opt_meas_string!(Tag, "TAG");
kw_opt_meas_string!(DetectorName, "DET");
kw_opt_meas_string!(DetectorType, "T");
kw_opt_meas_string!(PercentEmitted, "P");
kw_opt_meas_string!(Longname, "S");
kw_opt_meas_string!(Filter, "F");

kw_opt_meta_int!(Abrt, u32, "ABRT");
kw_opt_meta_int!(Lost, u32, "LOST");
kw_opt_meta_int!(Tot, usize, "TOT");
req_meta!(Tot);
kw_req_meta_int!(Par, usize, "PAR");
kw_opt_meas_int!(Wavelength, u32, "L");
kw_opt_meas_int!(Power, u32, "O");

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

kw_time!(Btim2_0, Btim, FCSTime, FCSTimeError, "BTIM");
kw_time!(Etim2_0, Etim, FCSTime, FCSTimeError, "ETIM");
kw_time!(Btim3_0, Btim, FCSTime60, FCSTime60Error, "BTIM");
kw_time!(Etim3_0, Etim, FCSTime60, FCSTime60Error, "ETIM");
kw_time!(Btim3_1, Btim, FCSTime100, FCSTime100Error, "BTIM");
kw_time!(Etim3_1, Etim, FCSTime100, FCSTime100Error, "ETIM");

kw_opt_meta!(FCSDate, "DATE");

newtype_from!(BeginDateTime, FCSDateTime);
newtype_from_outer!(BeginDateTime, FCSDateTime);
newtype_disp!(BeginDateTime);
newtype_fromstr!(BeginDateTime, FCSDateTimeError);
kw_opt_meta!(BeginDateTime, "BEGINDATETIME");

newtype_from!(EndDateTime, FCSDateTime);
newtype_from_outer!(EndDateTime, FCSDateTime);
newtype_disp!(EndDateTime);
newtype_fromstr!(EndDateTime, FCSDateTimeError);
kw_opt_meta!(EndDateTime, "ENDDATETIME");

impl<T> From<Btim<T>> for NaiveTime
where
    NaiveTime: From<T>,
{
    fn from(value: Btim<T>) -> Self {
        value.0.into()
    }
}

impl<T> From<Etim<T>> for NaiveTime
where
    NaiveTime: From<T>,
{
    fn from(value: Etim<T>) -> Self {
        value.0.into()
    }
}

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

kw_req_meta!(Timestep, "TIMESTEP");

#[derive(Clone, Copy, Serialize)]
pub struct Vol(pub NonNegFloat);

newtype_from!(Vol, NonNegFloat);
newtype_disp!(Vol);
newtype_fromstr!(Vol, RangedFloatError);

kw_opt_meta!(Vol, "VOL");

#[derive(Clone, Copy, Serialize, PartialEq)]
pub struct Gain(pub PositiveFloat);

newtype_from!(Gain, PositiveFloat);
newtype_disp!(Gain);
newtype_fromstr!(Gain, RangedFloatError);

kw_opt_meas!(Gain, "G");

kw_opt_meas!(DetectorVoltage, "V");

kw_req_meta!(Mode, "MODE");
kw_opt_meta!(Mode3_2, "MODE");
kw_req_meta!(AlphaNumType, "DATATYPE");
kw_req_meta!(Endian, "BYTEORD");
kw_req_meta!(ByteOrd, "BYTEORD");

kw_opt_meta!(Spillover, "SPILLOVER");
kw_opt_meta!(Compensation, "COMP");
kw_opt_meta!(Originality, "ORIGINALITY");
kw_opt_meta!(ModifiedDateTime, "LAST_MODIFIED");
kw_opt_meta!(UnstainedCenters, "UNSTAINEDCENTERS");
kw_opt_meta!(Unicode, "UNICODE");
kw_opt_meta!(Trigger, "TR");

kw_opt_meas!(Scale, "E");
req_meas!(Scale);

kw_req_meas!(Range, "R");
kw_req_meas!(Bytes, "B");
kw_opt_meas!(Wavelengths, "W");
kw_opt_meas!(Feature, "FEATURE");
kw_opt_meas!(MeasurementType, "TYPE");
kw_opt_meas!(NumType, "DATATYPE");
kw_opt_meas!(Display, "D");
kw_opt_meas!(Shortname, "N");
req_meas!(Shortname);

kw_opt_meas!(Calibration3_1, "CALIBRATION");
kw_opt_meas!(Calibration3_2, "CALIBRATION");

impl FromStr for FCSDateTime {
    type Err = FCSDateTimeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let formats = [
            "%Y-%m-%dT%H:%M:%S%.f",
            "%Y-%m-%dT%H:%M:%S%.f%#z",
            "%Y-%m-%dT%H:%M:%S%.f%:z",
            "%Y-%m-%dT%H:%M:%S%.f%::z",
            "%Y-%m-%dT%H:%M:%S%.f%:::z",
        ];
        for f in formats {
            if let Ok(t) = DateTime::parse_from_str(s, f) {
                return Ok(FCSDateTime(t));
            }
        }
        Err(FCSDateTimeError)
    }
}

impl fmt::Display for FCSDateTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", self.0.format("%Y-%m-%dT%H:%M:%S%.f%:z"))
    }
}

impl FromStr for FCSTime {
    type Err = FCSTimeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        NaiveTime::parse_from_str(s, "%H:%M:%S")
            .map(FCSTime)
            .or(Err(FCSTimeError))
    }
}

impl fmt::Display for FCSTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", self.0.format("%H:%M:%S"))
    }
}

impl fmt::Display for FCSTimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "must be like 'hh:mm:ss'")
    }
}

impl FromStr for FCSTime60 {
    type Err = FCSTime60Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        NaiveTime::parse_from_str(s, "%H:%M:%S")
            .or_else(|_| match s.split(":").collect::<Vec<_>>()[..] {
                [s1, s2, s3, s4] => {
                    let hh: u32 = s1.parse().or(Err(FCSTime60Error))?;
                    let mm: u32 = s2.parse().or(Err(FCSTime60Error))?;
                    let ss: u32 = s3.parse().or(Err(FCSTime60Error))?;
                    let tt: u32 = s4.parse().or(Err(FCSTime60Error))?;
                    let nn = tt * 1_000_000 / 60;
                    NaiveTime::from_hms_micro_opt(hh, mm, ss, nn).ok_or(FCSTime60Error)
                }
                _ => Err(FCSTime60Error),
            })
            .map(FCSTime60)
    }
}

impl fmt::Display for FCSTime60 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let base = self.0.format("%H:%M:%S");
        let cc = u64::from(self.0.nanosecond()) * 60 / 1_000_000_000;
        write!(f, "{}.{}", base, cc)
    }
}

impl fmt::Display for FCSTime60Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "must be like 'hh:mm:ss[:tt]' where 'tt' is in 1/60th seconds"
        )
    }
}

impl FromStr for FCSTime100 {
    type Err = FCSTime100Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        NaiveTime::parse_from_str(s, "%H:%M:%S")
            .or_else(|_| {
                let re = Regex::new(r"(\d){2}:(\d){2}:(\d){2}.(\d){2}").unwrap();
                let cap = re.captures(s).ok_or(FCSTime100Error)?;
                let [s1, s2, s3, s4] = cap.extract().1;
                let hh: u32 = s1.parse().or(Err(FCSTime100Error))?;
                let mm: u32 = s2.parse().or(Err(FCSTime100Error))?;
                let ss: u32 = s3.parse().or(Err(FCSTime100Error))?;
                let tt: u32 = s4.parse().or(Err(FCSTime100Error))?;
                NaiveTime::from_hms_milli_opt(hh, mm, ss, tt * 10).ok_or(FCSTime100Error)
            })
            .map(FCSTime100)
    }
}

impl fmt::Display for FCSTime100 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let base = self.0.format("%H:%M:%S");
        let cc = self.0.nanosecond() / 10_000_000;
        write!(f, "{}.{}", base, cc)
    }
}

impl fmt::Display for FCSTime100Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "must be like 'hh:mm:ss[.cc]'")
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

impl fmt::Display for AlphaNumTypeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "must be one of 'I', 'F', 'D', or 'A'")
    }
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

impl fmt::Display for NumTypeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "must be one of 'F', 'D', or 'A'")
    }
}

pub struct AlphaNumTypeError;
pub struct NumTypeError;
pub struct ModifiedDateTimeError;
pub struct FeatureError;
pub struct OriginalityError;

pub enum BytesError {
    Int(ParseIntError),
    Range,
    NotOctet,
}

pub enum CalibrationError<C> {
    Float(ParseFloatError),
    Range,
    Format(C),
}

pub struct CalibrationFormat3_1;
pub struct CalibrationFormat3_2;

pub enum UnicodeError {
    Empty,
    BadFormat,
}

pub enum DisplayError {
    FloatError(ParseFloatError),
    InvalidType,
    FormatError,
}

pub struct ModeError;

pub enum RangeError {
    Int(ParseIntError),
    Float(ParseFloatError),
}

pub struct Mode3_2Error;

pub(crate) trait Linked
where
    Self: Key,
    Self: Sized,
{
    fn check_link(&self, names: &HashSet<&Shortname>) -> Result<(), String> {
        let k = Self::std();
        let bad_names: Vec<_> = self.names().difference(names).copied().collect();
        let bad_names_str = bad_names.iter().join(", ");
        if bad_names.is_empty() {
            Ok(())
        } else if bad_names.len() == 1 {
            Err(format!(
                "{k} references non-existent $PnN name: {bad_names_str}"
            ))
        } else {
            Err(format!(
                "{k} references non-existent $PnN names: {bad_names_str}"
            ))
        }
    }

    fn reassign(&mut self, mapping: &NameMapping);

    fn names(&self) -> HashSet<&Shortname>;
}

// TODO define in same mode as type
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

// TODO define in same mode as type
impl Linked for Spillover {
    fn names(&self) -> HashSet<&Shortname> {
        self.measurements().into_iter().collect()
    }

    fn reassign(&mut self, mapping: &NameMapping) {
        self.remap_measurements(mapping)
    }
}

// TODO define in same mode as type
impl Linked for UnstainedCenters {
    fn names(&self) -> HashSet<&Shortname> {
        self.inner().keys().collect()
    }

    fn reassign(&mut self, mapping: &NameMapping) {
        self.rekey(mapping)
    }
}

impl FromStr for ModifiedDateTime {
    type Err = ModifiedDateTimeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (dt, cc) = NaiveDateTime::parse_and_remainder(s, "%d-%b-%Y %H:%M:%S")
            .or(Err(ModifiedDateTimeError))?;
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
        let dt = self.0.format("%d-%b-%Y %H:%M:%S");
        let cc = self.0.nanosecond() / 10000000;
        write!(f, "{dt}.{cc}")
    }
}

impl fmt::Display for ModifiedDateTimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "must be like 'dd-mmm-yyyy hh:mm:ss[.cc]'")
    }
}

// the "%b" format is case-insensitive so this should work for "Jan", "JAN",
// "jan", "jaN", etc
const FCS_DATE_FORMAT: &str = "%d-%b-%Y";

impl FromStr for FCSDate {
    type Err = FCSDateError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        NaiveDate::parse_from_str(s, FCS_DATE_FORMAT)
            .or(Err(FCSDateError))
            .map(FCSDate)
    }
}

impl fmt::Display for FCSDate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", self.0.format(FCS_DATE_FORMAT))
    }
}

impl fmt::Display for FCSDateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "must be like 'dd-mmm-yyyy'")
    }
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

impl fmt::Display for CalibrationFormat3_1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "must be like 'f,string'")
    }
}

impl fmt::Display for CalibrationFormat3_2 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "must be like 'f1,[f2],string'")
    }
}

impl<C: fmt::Display> fmt::Display for CalibrationError<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            CalibrationError::Float(x) => write!(f, "{}", x),
            CalibrationError::Range => write!(f, "must be a positive float"),
            CalibrationError::Format(x) => write!(f, "{}", x),
        }
    }
}

impl FromStr for Calibration3_1 {
    type Err = CalibrationError<CalibrationFormat3_1>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.split(",").collect::<Vec<_>>()[..] {
            [svalue, unit] => {
                let value = svalue.parse().map_err(CalibrationError::Float)?;
                if value >= 0.0 {
                    Ok(Calibration3_1 {
                        value,
                        unit: String::from(unit),
                    })
                } else {
                    Err(CalibrationError::Range)
                }
            }
            _ => Err(CalibrationError::Format(CalibrationFormat3_1)),
        }
    }
}

impl fmt::Display for Calibration3_1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{},{}", self.value, self.unit)
    }
}

impl FromStr for Calibration3_2 {
    type Err = CalibrationError<CalibrationFormat3_2>;

    // TODO not dry
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (value, offset, unit) = match s.split(",").collect::<Vec<_>>()[..] {
            [svalue, unit] => {
                let f1 = svalue.parse().map_err(CalibrationError::Float)?;
                Ok((f1, 0.0, String::from(unit)))
            }
            [svalue, soffset, unit] => {
                let f1 = svalue.parse().map_err(CalibrationError::Float)?;
                let f2 = soffset.parse().map_err(CalibrationError::Float)?;
                Ok((f1, f2, String::from(unit)))
            }
            _ => Err(CalibrationError::Format(CalibrationFormat3_2)),
        }?;
        if value >= 0.0 {
            Ok(Calibration3_2 {
                value,
                offset,
                unit,
            })
        } else {
            Err(CalibrationError::Range)
        }
    }
}

impl fmt::Display for Calibration3_2 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{},{},{}", self.value, self.offset, self.unit)
    }
}

impl FromStr for MeasurementType {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Forward Scatter" => Ok(MeasurementType::ForwardScatter),
            "Side Scatter" => Ok(MeasurementType::SideScatter),
            "Raw Fluorescence" => Ok(MeasurementType::RawFluorescence),
            "Unmixed Fluorescence" => Ok(MeasurementType::UnmixedFluorescence),
            "Mass" => Ok(MeasurementType::Mass),
            "Time" => Ok(MeasurementType::Time),
            "Electronic Volume" => Ok(MeasurementType::ElectronicVolume),
            "Index" => Ok(MeasurementType::Index),
            "Classification" => Ok(MeasurementType::Classification),
            s => Ok(MeasurementType::Other(String::from(s))),
        }
    }
}

impl fmt::Display for MeasurementType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            MeasurementType::ForwardScatter => write!(f, "Foward Scatter"),
            MeasurementType::SideScatter => write!(f, "Side Scatter"),
            MeasurementType::RawFluorescence => write!(f, "Raw Fluorescence"),
            MeasurementType::UnmixedFluorescence => write!(f, "Unmixed Fluorescence"),
            MeasurementType::Mass => write!(f, "Mass"),
            MeasurementType::Time => write!(f, "Time"),
            MeasurementType::ElectronicVolume => write!(f, "Electronic Volume"),
            MeasurementType::Classification => write!(f, "Classification"),
            MeasurementType::Index => write!(f, "Index"),
            MeasurementType::Other(s) => write!(f, "{}", s),
        }
    }
}

impl fmt::Display for FeatureError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "must be one of 'Area', 'Width', or 'Height'")
    }
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

impl fmt::Display for BytesError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            BytesError::Int(i) => write!(f, "{}", i),
            BytesError::Range => write!(f, "bit widths over 64 are not supported"),
            BytesError::NotOctet => write!(f, "bit widths must be octets"),
        }
    }
}

impl FromStr for Bytes {
    type Err = BytesError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "*" => Ok(Bytes::Variable),
            _ => s.parse::<u8>().map_err(BytesError::Int).and_then(|x| {
                if x > 64 {
                    Err(BytesError::Range)
                } else if x % 8 > 1 {
                    Err(BytesError::NotOctet)
                } else {
                    Ok(Bytes::Fixed(x / 8))
                }
            }),
        }
    }
}

impl fmt::Display for Bytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Bytes::Fixed(x) => write!(f, "{}", x * 8),
            Bytes::Variable => write!(f, "*"),
        }
    }
}

impl fmt::Display for RangeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            RangeError::Int(x) => write!(f, "{x}"),
            RangeError::Float(x) => write!(f, "{x}"),
        }
    }
}

impl fmt::Display for OriginalityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "Originality must be one of 'Original', 'NonDataModified', \
                   'Appended', or 'DataModified'"
        )
    }
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

impl fmt::Display for UnicodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            UnicodeError::Empty => write!(f, "No keywords given"),
            UnicodeError::BadFormat => write!(f, "Must be like 'n,string,[[string],...]'"),
        }
    }
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

pub struct FCSDateTimeError;
pub struct FCSTimeError;
pub struct FCSTime60Error;
pub struct FCSTime100Error;
pub struct FCSDateError;

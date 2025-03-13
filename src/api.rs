use crate::numeric::{Endian, IntMath, NumProps, Series};

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
use clap::value_parser;
use itertools::Itertools;
use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fs;
use std::io;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::iter;
use std::num::IntErrorKind;
use std::path;
use std::str;

fn format_standard_kw(kw: &str) -> Key {
    Key(format!("${}", kw.to_ascii_uppercase()))
}

fn format_measurement(n: u32, m: &str) -> String {
    format!("P{}{}", n, m.to_ascii_uppercase())
}

fn parse_endian(s: &str) -> ParseResult<Endian> {
    match s {
        "1,2,3,4" => Ok(Endian::Little),
        "4,3,2,1" => Ok(Endian::Big),
        _ => Err(String::from(
            "could not determine endianness, must be '1,2,3,4' or '4,3,2,1'",
        )),
    }
}

fn parse_bytes(s: &str) -> ParseResult<Bytes> {
    match s {
        "*" => Ok(Bytes::Variable),
        _ => s.parse::<u8>().map_or(
            Err(String::from("must be a positive integer or '*'")),
            |x| {
                if x > 64 {
                    Err(String::from("PnB over 64-bit are not supported"))
                } else if x % 8 > 1 {
                    Err(String::from("only multiples of 8 are supported"))
                } else {
                    Ok(Bytes::Fixed(x / 8))
                }
            },
        ),
    }
}

type ParseResult<T> = Result<T, String>;

fn parse_int(s: &str) -> ParseResult<u32> {
    s.parse::<u32>().map_err(|e| format!("{}", e))
}

fn parse_offset(s: &str) -> ParseResult<u32> {
    s.trim_start().parse().map_err(|e| format!("{}", e))
}

fn parse_offset_or_blank(s: &str) -> ParseResult<u32> {
    let q = s.trim_start();
    if q.is_empty() {
        Ok(0)
    } else {
        q.parse().or(Err(String::from("invalid offset or blank")))
    }
}

fn parse_float(s: &str) -> ParseResult<f32> {
    s.parse().map_err(|e| format!("{}", e))
}

fn parse_str(s: &str) -> ParseResult<String> {
    Ok(String::from(s))
}

fn parse_iso_datetime(s: &str) -> ParseResult<DateTime<FixedOffset>> {
    // TODO missing timezone implies localtime, but this will assume missing
    // means +00:00/UTC
    let formats = [
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S%.f%#z",
        "%Y-%m-%dT%H:%M:%S%.f%:z",
        "%Y-%m-%dT%H:%M:%S%.f%::z",
        "%Y-%m-%dT%H:%M:%S%.f%:::z",
    ];
    for f in formats {
        if let Ok(t) = DateTime::parse_from_str(s, f) {
            return Ok(t);
        }
    }
    Err(String::from(
        "must be formatted like 'yyyy-mm-ddThh:mm:ss[TZD]'",
    ))
}

fn parse_date(s: &str) -> ParseResult<NaiveDate> {
    // the "%b" format is case-insensitive so this should work for "Jan", "JAN",
    // "jan", "jaN", etc
    NaiveDate::parse_from_str(s, "%d-%b-%Y").or(NaiveDate::parse_from_str(s, "%d-%b-%Y")
        .or(Err(String::from("must be formatted like 'dd-mmm-yyyy'"))))
}

fn parse_time60(s: &str) -> ParseResult<NaiveTime> {
    // TODO this will have subseconds in terms of 1/100, need to convert to 1/60
    parse_time100(s)
}

fn parse_time100(s: &str) -> ParseResult<NaiveTime> {
    NaiveTime::parse_from_str(s, "%H:%M:%S.%.3f").or(NaiveTime::parse_from_str(s, "%H:%M:%S")
        .or(Err(String::from("must be formatted like 'hh:mm:ss[.cc]'"))))
}

fn parse_scale(s: &str) -> ParseResult<Scale> {
    let v: Vec<&str> = s.split(",").collect();
    match v[..] {
        [ds, os] => match (ds.parse(), os.parse()) {
            (Ok(0.0), Ok(0.0)) => Ok(Linear),
            (Ok(decades), Ok(offset)) => Ok(Log(LogScale { decades, offset })),
            _ => Err(String::from("invalid floats")),
        },
        _ => Err(String::from("too many fields")),
    }
}

#[derive(Debug, Clone, Copy)]
struct Offsets {
    begin: u32,
    end: u32,
}

impl Offsets {
    fn new(begin: u32, end: u32) -> Option<Offsets> {
        if begin > end {
            None
        } else {
            Some(Self { begin, end })
        }
    }

    fn adjust(&self, begin_delta: u32, end_delta: u32) -> Option<Offsets> {
        Self::new(self.begin + begin_delta, self.end + end_delta)
    }

    fn len(&self) -> u32 {
        self.end - self.begin
    }

    fn num_bytes(&self) -> u32 {
        self.len() + 1
    }

    // unset = both offsets are 0
    fn is_unset(&self) -> bool {
        self.begin == 0 && self.end == 0
    }
}

#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord)]
enum Version {
    FCS2_0,
    FCS3_0,
    FCS3_1,
    FCS3_2,
}

impl Version {
    fn parse(s: &str) -> Option<Version> {
        match s {
            "2.0" => Some(Version::FCS2_0),
            "3.0" => Some(Version::FCS3_0),
            "3.1" => Some(Version::FCS3_1),
            "3.2" => Some(Version::FCS3_2),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
struct Header {
    version: Version,
    text: Offsets,
    data: Offsets,
    analysis: Offsets,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum AlphaNumType {
    Ascii,
    Integer,
    Single,
    Double,
}

impl AlphaNumType {
    fn to_num(&self) -> Option<NumType> {
        match self {
            AlphaNumType::Ascii => None,
            AlphaNumType::Integer => Some(NumType::Integer),
            AlphaNumType::Single => Some(NumType::Single),
            AlphaNumType::Double => Some(NumType::Double),
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

#[derive(Debug, Clone, Copy)]
enum NumType {
    Integer,
    Single,
    Double,
}

impl NumType {
    fn lift(&self) -> AlphaNumType {
        match self {
            NumType::Integer => AlphaNumType::Integer,
            NumType::Single => AlphaNumType::Single,
            NumType::Double => AlphaNumType::Double,
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

#[derive(Debug, Clone)]
enum ByteOrd {
    Endian(Endian),
    Mixed(Vec<u8>),
}

impl ByteOrd {
    // This only makes sense for integer types
    fn num_bytes(&self) -> u8 {
        match self {
            ByteOrd::Endian(_) => 4,
            ByteOrd::Mixed(xs) => xs.len() as u8,
        }
    }
}

#[derive(Debug, Clone)]
struct Trigger {
    measurement: String,
    threshold: u32,
}

#[derive(Debug, Clone)]
struct Timestamps2_0 {
    btim: OptionalKw<NaiveTime>,
    etim: OptionalKw<NaiveTime>,
    date: OptionalKw<NaiveDate>,
}

#[derive(Debug, Clone)]
struct Timestamps3_2 {
    begin: OptionalKw<DateTime<FixedOffset>>,
    end: OptionalKw<DateTime<FixedOffset>>,
}

// TODO this is super messy, see 3.2 spec for restrictions on this we may with
// to use further
#[derive(Debug, Clone, PartialEq)]
struct LogScale {
    decades: f32,
    offset: f32,
}

#[derive(Debug, Clone, PartialEq)]
enum Scale {
    Log(LogScale),
    Linear,
}

use Scale::*;

#[derive(Debug, Clone)]
struct LinDisplay {
    lower: f32,
    upper: f32,
}

#[derive(Debug, Clone)]
struct LogDisplay {
    offset: f32,
    decades: f32,
}

#[derive(Debug, Clone)]
enum Display {
    Lin(LinDisplay),
    Log(LogDisplay),
}

#[derive(Debug, Clone)]
struct Calibration {
    value: f32,
    // TODO add offset (3.2 added a zero offset, which is different from 3.1)
    unit: String,
}

#[derive(Debug, Clone)]
enum MeasurementType {
    ForwardScatter,
    SideScatter,
    RawFluorescence,
    UnmixedFluorescence,
    Mass,
    Time,
    ElectronicVolume,
    Classification,
    Index,
    Other(String),
}

#[derive(Debug, Clone)]
enum Feature {
    Area,
    Width,
    Height,
}

#[derive(Debug, Clone)]
enum OptionalKw<V> {
    Present(V),
    Absent,
}

use OptionalKw::*;

impl<V> OptionalKw<V> {
    fn as_ref(&self) -> OptionalKw<&V> {
        match self {
            OptionalKw::Present(x) => Present(x),
            Absent => Absent,
        }
    }

    fn into_option(self) -> Option<V> {
        match self {
            OptionalKw::Present(x) => Some(x),
            Absent => None,
        }
    }

    fn from_option(x: Option<V>) -> Self {
        x.map_or_else(|| Absent, |y| OptionalKw::Present(y))
    }
}

#[derive(Debug, Clone)]
struct InnerMeasurment2_0 {
    scale: OptionalKw<Scale>,      // PnE
    wavelength: OptionalKw<u32>,   // PnL
    shortname: OptionalKw<String>, // PnN
}

#[derive(Debug, Clone)]
struct InnerMeasurement3_0 {
    scale: Scale,                  // PnE
    wavelength: OptionalKw<u32>,   // PnL
    shortname: OptionalKw<String>, // PnN
    gain: OptionalKw<f32>,         // PnG
}

#[derive(Debug, Clone)]
struct InnerMeasurement3_1 {
    scale: Scale,          // PnE
    wavelength: Vec<u32>,  // PnL
    shortname: String,     // PnN
    gain: OptionalKw<f32>, // PnG
    calibration: OptionalKw<Calibration>,
    display: OptionalKw<Display>,
}

#[derive(Debug, Clone)]
struct InnerMeasurement3_2 {
    scale: Scale,          // PnE
    wavelength: Vec<u32>,  // PnL
    shortname: String,     // PnN
    gain: OptionalKw<f32>, // PnG
    calibration: OptionalKw<Calibration>,
    display: OptionalKw<Display>,
    analyte: OptionalKw<String>,
    feature: OptionalKw<Feature>,
    measurement_type: OptionalKw<MeasurementType>,
    tag: OptionalKw<String>,
    detector_name: OptionalKw<String>,
    datatype: OptionalKw<NumType>,
}

// TODO this will likely need to be a trait in 4.0
impl InnerMeasurement3_2 {
    fn get_column_type(&self, default: AlphaNumType) -> AlphaNumType {
        self.datatype
            .as_ref()
            .into_option()
            .map(|x| x.lift())
            .unwrap_or(default)
    }
}

#[derive(Debug, Clone)]
enum Bytes {
    Fixed(u8),
    Variable,
}

impl Bytes {
    fn len(&self) -> Option<u8> {
        match self {
            Bytes::Fixed(x) => Some(*x),
            Bytes::Variable => None,
        }
    }
}

impl fmt::Display for Bytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Bytes::Fixed(x) => write!(f, "{}", x),
            Bytes::Variable => write!(f, "*"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Range {
    // This will actually store PnR - 1; most cytometers will store this as a
    // power of 2, so in the case of a 64 bit channel this will be 2^64 which is
    // one greater than u64::MAX.
    Int(u64),
    // This stores the value of PnR as-is. Sometimes PnR is actually a float
    // for floating point measurements rather than an int.
    Float(f64),
}

#[derive(Debug, Clone)]
struct Measurement<X> {
    bytes: Bytes,                      // PnB
    range: Range,                      // PnR
    longname: OptionalKw<String>,      // PnS
    filter: OptionalKw<String>,        // PnF there is a loose standard for this
    power: OptionalKw<u32>,            // PnO
    detector_type: OptionalKw<String>, // PnD
    percent_emitted: OptionalKw<u32>,  // PnP (TODO deprecated in 3.2, factor out)
    detector_voltage: OptionalKw<f32>, // PnV
    nonstandard: HashMap<Key, String>,
    specific: X,
}

impl<X> Measurement<X> {
    fn bytes_eq(&self, b: u8) -> bool {
        match self.bytes {
            Bytes::Fixed(x) => x == b,
            _ => false,
        }
    }

    // TODO add measurement index to error message
    fn make_int_parser(&self, o: &ByteOrd, t: usize) -> Result<AnyIntColumn, Vec<String>> {
        match self.bytes {
            Bytes::Fixed(b) => make_int_parser(b, &self.range, o, t),
            _ => Err(vec![String::from("PnB is variable length")]),
        }
    }
}

fn make_int_parser(b: u8, r: &Range, o: &ByteOrd, t: usize) -> Result<AnyIntColumn, Vec<String>> {
    match b {
        1 => u8::to_col_parser(r, o, t).map(AnyIntColumn::Uint8),
        2 => u16::to_col_parser(r, o, t).map(AnyIntColumn::Uint16),
        3 => IntFromBytes::<4, 3>::to_col_parser(r, o, t).map(AnyIntColumn::Uint24),
        4 => IntFromBytes::<4, 4>::to_col_parser(r, o, t).map(AnyIntColumn::Uint32),
        5 => IntFromBytes::<8, 5>::to_col_parser(r, o, t).map(AnyIntColumn::Uint40),
        6 => IntFromBytes::<8, 6>::to_col_parser(r, o, t).map(AnyIntColumn::Uint48),
        7 => IntFromBytes::<8, 7>::to_col_parser(r, o, t).map(AnyIntColumn::Uint56),
        8 => IntFromBytes::<8, 8>::to_col_parser(r, o, t).map(AnyIntColumn::Uint64),
        _ => Err(vec![String::from("PnB has invalid byte length")]),
    }
}

trait Versioned {
    fn fcs_version() -> Version;
}

trait VersionedMeasurement: Sized + Versioned {
    fn build_inner(st: &mut KwState, n: u32) -> Option<Self>;

    fn measurement_name(p: &Measurement<Self>) -> Option<&str>;

    fn has_linear_scale(&self) -> bool;

    fn has_gain(&self) -> bool;

    fn lookup_measurements(st: &mut KwState, par: u32) -> Option<Vec<Measurement<Self>>> {
        let mut ps = vec![];
        let v = Self::fcs_version();
        for n in 1..(par + 1) {
            let p = Measurement {
                bytes: st.lookup_meas_bytes(n)?,
                range: st.lookup_meas_range(n)?,
                longname: st.lookup_meas_longname(n),
                filter: st.lookup_meas_filter(n),
                power: st.lookup_meas_power(n),
                detector_type: st.lookup_meas_detector_type(n),
                percent_emitted: st.lookup_meas_percent_emitted(n, v == Version::FCS3_2),
                detector_voltage: st.lookup_meas_detector_voltage(n),
                specific: Self::build_inner(st, n)?,
                nonstandard: st.lookup_meas_nonstandard(n),
            };
            ps.push(p);
        }
        Some(ps)
    }
}

type Measurement2_0 = Measurement<InnerMeasurment2_0>;
type Measurement3_0 = Measurement<InnerMeasurement3_0>;
type Measurement3_1 = Measurement<InnerMeasurement3_1>;
type Measurement3_2 = Measurement<InnerMeasurement3_2>;

impl Versioned for InnerMeasurment2_0 {
    fn fcs_version() -> Version {
        Version::FCS2_0
    }
}

impl Versioned for InnerMeasurement3_0 {
    fn fcs_version() -> Version {
        Version::FCS3_0
    }
}

impl Versioned for InnerMeasurement3_1 {
    fn fcs_version() -> Version {
        Version::FCS3_1
    }
}

impl Versioned for InnerMeasurement3_2 {
    fn fcs_version() -> Version {
        Version::FCS3_2
    }
}

impl VersionedMeasurement for InnerMeasurment2_0 {
    fn measurement_name(p: &Measurement<Self>) -> Option<&str> {
        p.specific
            .shortname
            .as_ref()
            .into_option()
            .map(|s| s.as_str())
    }

    fn has_linear_scale(&self) -> bool {
        self.scale
            .as_ref()
            .into_option()
            .map(|x| *x == Scale::Linear)
            .unwrap_or(false)
    }

    fn has_gain(&self) -> bool {
        false
    }

    fn build_inner(st: &mut KwState, n: u32) -> Option<InnerMeasurment2_0> {
        Some(InnerMeasurment2_0 {
            scale: st.lookup_meas_scale_opt(n),
            shortname: st.lookup_meas_shortname_opt(n),
            wavelength: st.lookup_meas_wavelength(n),
        })
    }
}

impl VersionedMeasurement for InnerMeasurement3_0 {
    fn measurement_name(p: &Measurement<Self>) -> Option<&str> {
        p.specific
            .shortname
            .as_ref()
            .into_option()
            .map(|s| s.as_str())
    }

    fn has_linear_scale(&self) -> bool {
        self.scale == Scale::Linear
    }

    fn has_gain(&self) -> bool {
        self.gain.as_ref().into_option().is_some()
    }

    fn build_inner(st: &mut KwState, n: u32) -> Option<InnerMeasurement3_0> {
        Some(InnerMeasurement3_0 {
            scale: st.lookup_meas_scale_req(n)?,
            shortname: st.lookup_meas_shortname_opt(n),
            wavelength: st.lookup_meas_wavelength(n),
            gain: st.lookup_meas_gain(n),
        })
    }
}

impl VersionedMeasurement for InnerMeasurement3_1 {
    fn measurement_name(p: &Measurement<Self>) -> Option<&str> {
        Some(p.specific.shortname.as_str())
    }

    fn has_linear_scale(&self) -> bool {
        self.scale == Scale::Linear
    }

    fn has_gain(&self) -> bool {
        self.gain.as_ref().into_option().is_some()
    }

    fn build_inner(st: &mut KwState, n: u32) -> Option<InnerMeasurement3_1> {
        Some(InnerMeasurement3_1 {
            scale: st.lookup_meas_scale_req(n)?,
            shortname: st.lookup_meas_shortname_req(n)?,
            wavelength: st.lookup_meas_wavelengths(n),
            gain: st.lookup_meas_gain(n),
            calibration: st.lookup_meas_calibration(n),
            display: st.lookup_meas_display(n),
        })
    }
}

impl VersionedMeasurement for InnerMeasurement3_2 {
    fn measurement_name(p: &Measurement<Self>) -> Option<&str> {
        Some(p.specific.shortname.as_str())
    }

    fn has_linear_scale(&self) -> bool {
        self.scale == Scale::Linear
    }

    fn has_gain(&self) -> bool {
        self.gain.as_ref().into_option().is_some()
    }

    fn build_inner(st: &mut KwState, n: u32) -> Option<InnerMeasurement3_2> {
        Some(InnerMeasurement3_2 {
            scale: st.lookup_meas_scale_req(n)?,
            shortname: st.lookup_meas_shortname_req(n)?,
            wavelength: st.lookup_meas_wavelengths(n),
            gain: st.lookup_meas_gain(n),
            detector_name: st.lookup_meas_detector(n),
            tag: st.lookup_meas_tag(n),
            measurement_type: st.lookup_meas_type(n),
            feature: st.lookup_meas_feature(n),
            analyte: st.lookup_meas_analyte(n),
            calibration: st.lookup_meas_calibration(n),
            display: st.lookup_meas_display(n),
            datatype: st.lookup_meas_datatype(n),
        })
    }
}

#[derive(Debug, Clone)]
enum Originality {
    Original,
    NonDataModified,
    Appended,
    DataModified,
}

#[derive(Debug, Clone)]
struct ModificationData {
    last_modifier: OptionalKw<String>,
    last_modified: OptionalKw<NaiveDateTime>,
    originality: OptionalKw<Originality>,
}

#[derive(Debug, Clone)]
struct PlateData {
    plateid: OptionalKw<String>,
    platename: OptionalKw<String>,
    wellid: OptionalKw<String>,
}

type UnstainedCenters = HashMap<String, f32>;

#[derive(Debug, Clone)]
struct UnstainedData {
    unstainedcenters: OptionalKw<UnstainedCenters>,
    unstainedinfo: OptionalKw<String>,
}

#[derive(Debug, Clone)]
struct CarrierData {
    carrierid: OptionalKw<String>,
    carriertype: OptionalKw<String>,
    locationid: OptionalKw<String>,
}

#[derive(Debug, Clone)]
struct Unicode {
    page: u32,
    kws: Vec<String>,
}

#[derive(Debug, Clone)]
struct SupplementalOffsets3_0 {
    analysis: Offsets,
    stext: Offsets,
}

#[derive(Debug, Clone)]
struct SupplementalOffsets3_2 {
    analysis: OptionalKw<Offsets>,
    stext: OptionalKw<Offsets>,
}

#[derive(Debug, Clone)]
struct InnerMetadata2_0 {
    tot: OptionalKw<u32>,
    mode: Mode,
    byteord: ByteOrd,
    cyt: OptionalKw<String>,
    timestamps: Timestamps2_0, // BTIM/ETIM/DATE
}

#[derive(Debug, Clone)]
struct InnerMetadata3_0 {
    data: Offsets,
    supplemental: SupplementalOffsets3_0,
    tot: u32,
    mode: Mode,
    byteord: ByteOrd,
    timestamps: Timestamps2_0, // BTIM/ETIM/DATE
    cyt: OptionalKw<String>,
    cytsn: OptionalKw<String>,
    timestep: OptionalKw<f32>,
    unicode: OptionalKw<Unicode>,
}

#[derive(Debug, Clone)]
struct InnerMetadata3_1 {
    data: Offsets,
    supplemental: SupplementalOffsets3_0,
    tot: u32,
    mode: Mode,
    byteord: Endian,
    timestamps: Timestamps2_0, // BTIM/ETIM/DATE
    cyt: OptionalKw<String>,
    cytsn: OptionalKw<String>,
    timestep: OptionalKw<f32>,
    modification: ModificationData,
    plate: PlateData,
    vol: OptionalKw<f32>,
}

#[derive(Debug, Clone)]
struct InnerMetadata3_2 {
    data: Offsets,
    supplemental: SupplementalOffsets3_2,
    tot: u32,
    byteord: Endian,
    timestamps: Timestamps2_0, // BTIM/ETIM/DATE
    datetimes: Timestamps3_2,  // DATETIMESTART/END
    cyt: String,
    cytsn: OptionalKw<String>,
    timestep: OptionalKw<f32>,
    modification: ModificationData,
    plate: PlateData,
    vol: OptionalKw<f32>,
    carrier: CarrierData,
    unstained: UnstainedData,
    flowrate: OptionalKw<String>,
}

#[derive(Debug, Clone)]
struct Metadata<X> {
    par: u32,
    nextdata: u32,
    datatype: AlphaNumType,
    // an abstraction for various kinds of spillover/comp matrices
    // spillover: Spillover,
    abrt: OptionalKw<u32>,
    com: OptionalKw<String>,
    cells: OptionalKw<String>,
    exp: OptionalKw<String>,
    fil: OptionalKw<String>,
    inst: OptionalKw<String>,
    lost: OptionalKw<u32>,
    op: OptionalKw<String>,
    proj: OptionalKw<String>,
    smno: OptionalKw<String>,
    src: OptionalKw<String>,
    sys: OptionalKw<String>,
    tr: OptionalKw<Trigger>,
    specific: X,
}

type Metadata2_0 = Metadata<InnerMetadata2_0>;
type Metadata3_0 = Metadata<InnerMetadata3_0>;
type Metadata3_1 = Metadata<InnerMetadata3_1>;
type Metadata3_2 = Metadata<InnerMetadata3_2>;

#[derive(Debug, Clone, PartialEq, Eq)]
enum Mode {
    List,
    Uncorrelated,
    Correlated,
}

#[derive(Debug, Clone)]
struct StdText<M, P> {
    header: Header,
    raw: RawTEXT,
    metadata: Metadata<M>,
    measurements: Vec<Measurement<P>>,
}

// #[derive(Debug)]
// struct TEXTSuccess<T> {
//     text: T,
//     data_parser: DataParser,
//     // warnings: Vec<KwMsg>,
//     // deprecated: Vec<String>,
// }

#[derive(Debug)]
pub enum AnyStdTEXT {
    FCS2_0(Box<StdText2_0>),
    FCS3_0(Box<StdText3_0>),
    FCS3_1(Box<StdText3_1>),
    FCS3_2(Box<StdText3_2>),
}

#[derive(Debug)]
pub struct ParsedTEXT {
    // TODO add the offsets here as well? offsets are needed before parsing
    // everything else
    standard: AnyStdTEXT,
    data_parser: DataParser,
    nonfatal: NonFatalErrors,
    // nonstandard: HashMap<Key, String>,
    // deviant: HashMap<Key, String>,
}

type TEXTResult = Result<ParsedTEXT, StandardErrors>;

impl<M: VersionedMetadata> StdText<M, M::P> {
    fn get_shortnames(&self) -> Vec<&str> {
        self.measurements
            .iter()
            .filter_map(|p| M::P::measurement_name(p))
            .collect()
    }

    fn raw_to_std_text(header: Header, raw: RawTEXT, conf: &StdTextReader) -> TEXTResult {
        let mut st = raw.to_state(conf);
        // This will fail if a) not all required keywords pass and b) not all
        // required measurement keywords pass (according to $PAR)
        if let Some(s) = M::lookup_metadata(&mut st).and_then(|metadata| {
            M::P::lookup_measurements(&mut st, metadata.par).map(|measurements| StdText {
                header,
                raw,
                metadata,
                measurements,
            })
        }) {
            M::validate(&mut st, &s);
            match M::build_data_parser(&mut st, &s) {
                Some(data_parser) => st.into_result(s, data_parser),
                None => Err(st.into_errors()),
            }
        } else {
            Err(st.into_errors())
        }
    }
}

type StdText2_0 = StdText<InnerMetadata2_0, InnerMeasurment2_0>;
type StdText3_0 = StdText<InnerMetadata3_0, InnerMeasurement3_0>;
type StdText3_1 = StdText<InnerMetadata3_1, InnerMeasurement3_1>;
type StdText3_2 = StdText<InnerMetadata3_2, InnerMeasurement3_2>;

trait OrderedFromBytes<const DTLEN: usize, const OLEN: usize>: NumProps<DTLEN> {
    fn read_from_ordered<R: Read>(h: &mut BufReader<R>, order: &[u8; OLEN]) -> io::Result<Self> {
        let mut tmp = [0; OLEN];
        let mut buf = [0; DTLEN];
        h.read_exact(&mut tmp)?;
        for (i, j) in order.iter().enumerate() {
            buf[usize::from(*j)] = tmp[i];
        }
        Ok(Self::from_little(buf))
    }
}

// TODO where to put this?
fn byteord_to_sized<const LEN: usize>(byteord: &ByteOrd) -> Result<SizedByteOrd<LEN>, String> {
    match byteord {
        ByteOrd::Endian(e) => Ok(SizedByteOrd::Endian(*e)),
        ByteOrd::Mixed(v) => v[..]
            .try_into()
            .map(|order: [u8; LEN]| SizedByteOrd::Order(order))
            .or(Err(String::from(
                "$BYTEORD is mixed but length is {v.len()} and not {INTLEN}",
            ))),
    }
}

trait IntFromBytes<const DTLEN: usize, const INTLEN: usize>:
    NumProps<DTLEN> + OrderedFromBytes<DTLEN, INTLEN> + Ord + IntMath
{
    fn byteord_to_sized(byteord: &ByteOrd) -> Result<SizedByteOrd<INTLEN>, String> {
        byteord_to_sized(byteord)
    }

    fn range_to_bitmask(range: &Range) -> Option<Self> {
        match range {
            Range::Float(_) => None,
            Range::Int(i) => Some(Self::next_power_2(Self::from_u64(*i))),
        }
    }

    fn to_col_parser(
        range: &Range,
        byteord: &ByteOrd,
        total_events: usize,
    ) -> Result<IntColumnParser<Self, INTLEN>, Vec<String>> {
        // TODO be more specific, which means we need the measurement index
        let b =
            Self::range_to_bitmask(range).ok_or(String::from("PnR is float for an integer column"));
        let s = Self::byteord_to_sized(byteord);
        let data = vec![Self::zero(); total_events];
        match (b, s) {
            (Ok(bitmask), Ok(size)) => Ok(IntColumnParser {
                bitmask,
                size,
                data,
            }),
            (Err(x), Err(y)) => Err(vec![x, y]),
            (Err(x), _) => Err(vec![x]),
            (_, Err(y)) => Err(vec![y]),
        }
    }

    fn read_int_masked<R: Read>(
        h: &mut BufReader<R>,
        byteord: &SizedByteOrd<INTLEN>,
        bitmask: Self,
    ) -> io::Result<Self> {
        Self::read_int(h, byteord).map(|x| x.min(bitmask))
    }

    fn read_int<R: Read>(h: &mut BufReader<R>, byteord: &SizedByteOrd<INTLEN>) -> io::Result<Self> {
        // This lovely code will read data that is not a power-of-two
        // bytes long. Start by reading n bytes into a vector, which can
        // take a varying size. Then copy this into the power of 2 buffer
        // and reset all the unused cells to 0. This copy has to go to one
        // or the other end of the buffer depending on endianness.
        //
        // ASSUME for u8 and u16 that these will get heavily optimized away
        // since 'order' is totally meaningless for u8 and the only two possible
        // 'orders' for u16 are big and little.
        let mut tmp = [0; INTLEN];
        let mut buf = [0; DTLEN];
        match byteord {
            SizedByteOrd::Endian(Endian::Big) => {
                let b = DTLEN - INTLEN;
                h.read_exact(&mut tmp)?;
                buf[b..].copy_from_slice(&tmp[b..]);
                Ok(Self::from_big(buf))
            }
            SizedByteOrd::Endian(Endian::Little) => {
                h.read_exact(&mut tmp)?;
                buf[..INTLEN].copy_from_slice(&tmp[..INTLEN]);
                Ok(Self::from_little(buf))
            }
            SizedByteOrd::Order(order) => Self::read_from_ordered(h, order),
        }
    }

    fn assign<R: Read>(
        h: &mut BufReader<R>,
        d: &mut IntColumnParser<Self, INTLEN>,
        row: usize,
    ) -> io::Result<()> {
        d.data[row] = Self::read_int_masked(h, &d.size, d.bitmask)?;
        Ok(())
    }
}

trait FloatFromBytes<const LEN: usize>:
    NumProps<LEN> + OrderedFromBytes<LEN, LEN> + Clone + NumProps<LEN>
{
    fn to_float_byteord(byteord: &ByteOrd) -> Result<SizedByteOrd<LEN>, String> {
        byteord_to_sized(byteord)
    }

    fn make_column_parser(endian: Endian, total_events: usize) -> FloatColumn<Self> {
        FloatColumn {
            data: vec![Self::zero(); total_events],
            endian,
        }
    }

    fn make_matrix_parser(
        byteord: &ByteOrd,
        par: usize,
        total_events: usize,
    ) -> Result<FloatParser<LEN>, String> {
        Self::to_float_byteord(byteord).map(|byteord| FloatParser {
            nrows: total_events,
            ncols: par,
            byteord,
        })
    }

    fn read_float<R: Read>(h: &mut BufReader<R>, byteord: &SizedByteOrd<LEN>) -> io::Result<Self> {
        let mut buf = [0; LEN];
        match byteord {
            SizedByteOrd::Endian(Endian::Big) => {
                h.read_exact(&mut buf)?;
                Ok(Self::from_big(buf))
            }
            SizedByteOrd::Endian(Endian::Little) => {
                h.read_exact(&mut buf)?;
                Ok(Self::from_little(buf))
            }
            SizedByteOrd::Order(order) => Self::read_from_ordered(h, order),
        }
    }

    fn assign_column<R: Read>(
        h: &mut BufReader<R>,
        column: &mut FloatColumn<Self>,
        row: usize,
    ) -> io::Result<()> {
        // TODO endian wrap thing seems unnecessary
        column.data[row] = Self::read_float(h, &SizedByteOrd::Endian(column.endian))?;
        Ok(())
    }

    fn assign_matrix<R: Read + Seek>(
        h: &mut BufReader<R>,
        d: &FloatParser<LEN>,
        column: &mut [Self],
        row: usize,
    ) -> io::Result<()> {
        column[row] = Self::read_float(h, &d.byteord)?;
        Ok(())
    }

    fn parse_matrix<R: Read + Seek>(
        h: &mut BufReader<R>,
        p: FloatParser<LEN>,
    ) -> io::Result<Vec<Series>> {
        let mut columns: Vec<_> = iter::repeat_with(|| vec![Self::zero(); p.nrows])
            .take(p.ncols)
            .collect();
        for r in 0..p.nrows {
            for c in columns.iter_mut() {
                Self::assign_matrix(h, &p, c, r)?;
            }
        }
        Ok(columns.into_iter().map(Self::into_series).collect())
    }
}

impl OrderedFromBytes<1, 1> for u8 {}
impl OrderedFromBytes<2, 2> for u16 {}
impl OrderedFromBytes<4, 3> for u32 {}
impl OrderedFromBytes<4, 4> for u32 {}
impl OrderedFromBytes<8, 5> for u64 {}
impl OrderedFromBytes<8, 6> for u64 {}
impl OrderedFromBytes<8, 7> for u64 {}
impl OrderedFromBytes<8, 8> for u64 {}
impl OrderedFromBytes<4, 4> for f32 {}
impl OrderedFromBytes<8, 8> for f64 {}

impl FloatFromBytes<4> for f32 {}
impl FloatFromBytes<8> for f64 {}

impl IntFromBytes<1, 1> for u8 {}
impl IntFromBytes<2, 2> for u16 {}
impl IntFromBytes<4, 3> for u32 {}
impl IntFromBytes<4, 4> for u32 {}
impl IntFromBytes<8, 5> for u64 {}
impl IntFromBytes<8, 6> for u64 {}
impl IntFromBytes<8, 7> for u64 {}
impl IntFromBytes<8, 8> for u64 {}

#[derive(Debug)]
struct FloatParser<const LEN: usize> {
    nrows: usize,
    ncols: usize,
    byteord: SizedByteOrd<LEN>,
}

#[derive(Debug)]
struct AsciiColumn {
    data: Vec<f64>,
    width: u8,
}

#[derive(Debug)]
struct FloatColumn<T> {
    data: Vec<T>,
    endian: Endian,
}

#[derive(Debug)]
enum MixedColumnType {
    Ascii(AsciiColumn),
    Uint(AnyIntColumn),
    Single(FloatColumn<f32>),
    Double(FloatColumn<f64>),
}

impl MixedColumnType {
    fn into_series(self) -> Series {
        match self {
            MixedColumnType::Ascii(x) => f64::into_series(x.data),
            MixedColumnType::Single(x) => f32::into_series(x.data),
            MixedColumnType::Double(x) => f64::into_series(x.data),
            MixedColumnType::Uint(x) => x.into_series(),
        }
    }
}

#[derive(Debug)]
struct MixedParser {
    nrows: usize,
    columns: Vec<MixedColumnType>,
}

#[derive(Debug)]
enum SizedByteOrd<const LEN: usize> {
    Endian(Endian),
    Order([u8; LEN]),
}

#[derive(Debug)]
struct IntColumnParser<B, const LEN: usize> {
    bitmask: B,
    size: SizedByteOrd<LEN>,
    data: Vec<B>,
}

#[derive(Debug)]
enum AnyIntColumn {
    Uint8(IntColumnParser<u8, 1>),
    Uint16(IntColumnParser<u16, 2>),
    Uint24(IntColumnParser<u32, 3>),
    Uint32(IntColumnParser<u32, 4>),
    Uint40(IntColumnParser<u64, 5>),
    Uint48(IntColumnParser<u64, 6>),
    Uint56(IntColumnParser<u64, 7>),
    Uint64(IntColumnParser<u64, 8>),
}

impl AnyIntColumn {
    fn into_series(self) -> Series {
        match self {
            AnyIntColumn::Uint8(y) => u8::into_series(y.data),
            AnyIntColumn::Uint16(y) => u16::into_series(y.data),
            AnyIntColumn::Uint24(y) => u32::into_series(y.data),
            AnyIntColumn::Uint32(y) => u32::into_series(y.data),
            AnyIntColumn::Uint40(y) => u64::into_series(y.data),
            AnyIntColumn::Uint48(y) => u64::into_series(y.data),
            AnyIntColumn::Uint56(y) => u64::into_series(y.data),
            AnyIntColumn::Uint64(y) => u64::into_series(y.data),
        }
    }

    fn assign<R: Read>(&mut self, h: &mut BufReader<R>, r: usize) -> io::Result<()> {
        match self {
            AnyIntColumn::Uint8(d) => u8::assign(h, d, r)?,
            AnyIntColumn::Uint16(d) => u16::assign(h, d, r)?,
            AnyIntColumn::Uint24(d) => u32::assign(h, d, r)?,
            AnyIntColumn::Uint32(d) => u32::assign(h, d, r)?,
            AnyIntColumn::Uint40(d) => u64::assign(h, d, r)?,
            AnyIntColumn::Uint48(d) => u64::assign(h, d, r)?,
            AnyIntColumn::Uint56(d) => u64::assign(h, d, r)?,
            AnyIntColumn::Uint64(d) => u64::assign(h, d, r)?,
        }
        Ok(())
    }
}

// Integers are complicated because in each version we need to at least deal
// with the possibility that each column has a different bitmask. In addition,
// 3.1+ allows for different widths (even though this likely is used seldom
// if ever) so each series can potentially be a different type. Finally,
// BYTEORD further complicates this because unlike floats which can only have
// widths of 4 or 8 bytes, integers can have any number of bytes up to their
// next power of 2 data type. For example, some cytometers store their values
// in 3-byte segments, which would need to be stored in u32 but are read as
// triples, which in theory could be any byte order.
//
// There may be some small optimizations we can make for the "typical" cases
// where the entire file is u32 with big/little BYTEORD and only a handful
// of different bitmasks. For now, the increased complexity of dealing with this
// is likely no worth it.
#[derive(Debug)]
struct IntParser {
    nrows: usize,
    columns: Vec<AnyIntColumn>,
}

#[derive(Debug)]
struct FixedAsciiParser {
    columns: Vec<u8>,
    nrows: usize,
}

#[derive(Debug)]
struct DelimAsciiParser {
    ncols: usize,
    nrows: Option<usize>,
    nbytes: usize,
}

#[derive(Debug)]
enum ColumnParser {
    // DATATYPE=A where all PnB = *
    DelimitedAscii(DelimAsciiParser),
    // DATATYPE=A where all PnB = number
    FixedWidthAscii(FixedAsciiParser),
    // DATATYPE=F (with no overrides in 3.2+)
    Single(FloatParser<4>),
    // DATATYPE=D (with no overrides in 3.2+)
    Double(FloatParser<8>),
    // DATATYPE=I this is complicated so see struct definition
    Int(IntParser),
    // Mixed column types (3.2+)
    Mixed(MixedParser),
}

#[derive(Debug)]
struct DataParser {
    column_parser: ColumnParser,
    begin: u64,
}

type ParsedData = Vec<Series>;

fn ascii_to_float_io(buf: Vec<u8>) -> io::Result<f64> {
    String::from_utf8(buf)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
        .and_then(|s| parse_f64_io(&s))
}

fn parse_f64_io(s: &str) -> io::Result<f64> {
    s.parse::<f64>()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

fn read_data_delim_ascii<R: Read>(
    h: &mut BufReader<R>,
    p: DelimAsciiParser,
) -> io::Result<ParsedData> {
    let mut buf = Vec::new();
    let mut row = 0;
    let mut col = 0;
    let mut last_was_delim = false;
    // Delimiters are tab, newline, carriage return, space, or comma. Any
    // consecutive delimiter counts as one, and delimiters can be mixed.
    let is_delim = |byte| byte == 9 || byte == 10 || byte == 13 || byte == 32 || byte == 44;
    // FCS 2.0 files have an optional $TOT field, which complicates this a bit
    if let Some(nrows) = p.nrows {
        let mut data: Vec<_> = iter::repeat_with(|| vec![0.0; nrows])
            .take(p.ncols)
            .collect();
        for b in h.bytes().take(p.nbytes) {
            let byte = b?;
            // exit if we encounter more rows than expected.
            if row == nrows {
                let msg = format!("Exceeded expected number of rows: {nrows}");
                return Err(io::Error::new(io::ErrorKind::InvalidData, msg));
            }
            if is_delim(byte) {
                if !last_was_delim {
                    last_was_delim = true;
                    // TODO this will spaz out if we end up reading more
                    // rows than expected
                    data[col][row] = ascii_to_float_io(buf.clone())?;
                    buf.clear();
                    if col == p.ncols - 1 {
                        col = 0;
                        row += 1;
                    } else {
                        col += 1;
                    }
                }
            } else {
                buf.push(byte);
            }
        }
        // The spec isn't clear if the last value should be a delim or
        // not, so flush the buffer if it has anything in it since we
        // only try to parse if we hit a delim above.
        if !buf.is_empty() {
            data[col][row] = ascii_to_float_io(buf.clone())?;
        }
        if !(col == 0 && row == nrows) {
            let msg = format!(
                "Parsing ended in column {col} and row {row}, \
                               where expected number of rows is {nrows}"
            );
            return Err(io::Error::new(io::ErrorKind::InvalidData, msg));
        }
        Ok(data.into_iter().map(f64::into_series).collect())
    } else {
        let mut data: Vec<_> = iter::repeat_with(Vec::new).take(p.ncols).collect();
        for b in h.bytes().take(p.nbytes) {
            let byte = b?;
            // Delimiters are tab, newline, carriage return, space, or
            // comma. Any consecutive delimiter counts as one, and
            // delimiters can be mixed.
            if is_delim(byte) {
                if !last_was_delim {
                    last_was_delim = true;
                    data[col].push(ascii_to_float_io(buf.clone())?);
                    buf.clear();
                    if col == p.ncols - 1 {
                        col = 0;
                    } else {
                        col += 1;
                    }
                }
            } else {
                buf.push(byte);
            }
        }
        // The spec isn't clear if the last value should be a delim or
        // not, so flush the buffer if it has anything in it since we
        // only try to parse if we hit a delim above.
        if !buf.is_empty() {
            data[col][row] = ascii_to_float_io(buf.clone())?;
        }
        // Scream if not all columns are equal in length
        if data.iter().map(|c| c.len()).unique().count() > 1 {
            let msg = "Not all columns are equal length";
            return Err(io::Error::new(io::ErrorKind::InvalidData, msg));
        }
        Ok(data.into_iter().map(f64::into_series).collect())
    }
}

fn read_data_ascii_fixed<R: Read>(
    h: &mut BufReader<R>,
    parser: &FixedAsciiParser,
) -> io::Result<ParsedData> {
    let ncols = parser.columns.len();
    let mut data: Vec<_> = iter::repeat_with(|| vec![0.0; parser.nrows])
        .take(ncols)
        .collect();
    let mut buf = String::new();
    for r in 0..parser.nrows {
        for (c, width) in parser.columns.iter().enumerate() {
            buf.clear();
            h.take(u64::from(*width)).read_to_string(&mut buf)?;
            data[c][r] = parse_f64_io(&buf)?;
        }
    }
    Ok(data.into_iter().map(f64::into_series).collect())
}

fn read_data_mixed<R: Read>(h: &mut BufReader<R>, parser: MixedParser) -> io::Result<ParsedData> {
    let mut p = parser;
    let mut strbuf = String::new();
    for r in 0..p.nrows {
        for c in p.columns.iter_mut() {
            match c {
                MixedColumnType::Single(t) => f32::assign_column(h, t, r)?,
                MixedColumnType::Double(t) => f64::assign_column(h, t, r)?,
                MixedColumnType::Uint(u) => u.assign(h, r)?,
                MixedColumnType::Ascii(d) => {
                    strbuf.clear();
                    h.take(u64::from(d.width)).read_to_string(&mut strbuf)?;
                    d.data[r] = parse_f64_io(&strbuf)?;
                }
            }
        }
    }
    Ok(p.columns.into_iter().map(|c| c.into_series()).collect())
}

fn read_data_int<R: Read>(h: &mut BufReader<R>, parser: IntParser) -> io::Result<ParsedData> {
    let mut p = parser;
    for r in 0..p.nrows {
        for c in p.columns.iter_mut() {
            c.assign(h, r)?;
        }
    }
    Ok(p.columns.into_iter().map(|c| c.into_series()).collect())
}

fn read_data<R: Read + Seek>(h: &mut BufReader<R>, parser: DataParser) -> io::Result<ParsedData> {
    h.seek(SeekFrom::Start(parser.begin))?;
    match parser.column_parser {
        ColumnParser::DelimitedAscii(p) => read_data_delim_ascii(h, p),
        ColumnParser::FixedWidthAscii(p) => read_data_ascii_fixed(h, &p),
        ColumnParser::Single(p) => f32::parse_matrix(h, p),
        ColumnParser::Double(p) => f64::parse_matrix(h, p),
        ColumnParser::Mixed(p) => read_data_mixed(h, p),
        ColumnParser::Int(p) => read_data_int(h, p),
    }
}

enum EventWidth {
    Finite(Vec<u8>),
    Variable,
    Error(Vec<usize>, Vec<usize>),
}

trait VersionedMetadata: Sized {
    type P: VersionedMeasurement;

    fn into_any_text(s: Box<StdText<Self, Self::P>>) -> AnyStdTEXT;

    fn get_data_offsets(s: &StdText<Self, Self::P>) -> Offsets;

    fn get_byteord(&self) -> ByteOrd;

    fn get_tot(&self) -> Option<u32>;

    fn has_timestep(&self) -> bool;

    fn get_event_width(s: &StdText<Self, Self::P>) -> EventWidth {
        let (fixed, variable_indices): (Vec<_>, Vec<_>) = s
            .measurements
            .iter()
            .enumerate()
            .map(|(i, p)| match p.bytes {
                Bytes::Fixed(b) => Ok((i, b)),
                Bytes::Variable => Err(i),
            })
            .partition_result();
        let (fixed_indices, fixed_bytes): (Vec<_>, Vec<_>) = fixed.into_iter().unzip();
        if variable_indices.is_empty() {
            EventWidth::Finite(fixed_bytes)
        } else if fixed_indices.is_empty() {
            EventWidth::Variable
        } else {
            EventWidth::Error(fixed_indices, variable_indices)
        }
    }

    fn get_total_events(
        st: &mut KwState,
        s: &StdText<Self, Self::P>,
        event_width: u32,
    ) -> Option<usize> {
        let nbytes = Self::get_data_offsets(s).num_bytes();
        let remainder = nbytes % event_width;
        let res = nbytes / event_width;
        let total_events = if nbytes % event_width > 0 {
            let msg = format!(
                "Events are {event_width} bytes wide, but this does not evenly \
                 divide DATA segment which is {nbytes} bytes long \
                 (remainder of {remainder})"
            );
            if st.conf.raw.enfore_data_width_divisibility {
                st.push_meta_error(msg);
                None
            } else {
                st.push_meta_warning(msg);
                Some(res)
            }
        } else {
            Some(res)
        };
        total_events.and_then(|x| {
            if let Some(tot) = s.metadata.specific.get_tot() {
                if x != tot {
                    let msg = format!(
                        "$TOT field is {tot} but number of events \
                         that evenly fit into DATA is {x}"
                    );
                    if st.conf.raw.enfore_matching_tot {
                        st.push_meta_error(msg);
                    } else {
                        st.push_meta_warning(msg);
                    }
                }
            }
            usize::try_from(x).ok()
        })
    }

    fn build_int_parser(
        &self,
        st: &mut KwState,
        ps: &[Measurement<Self::P>],
        total_events: usize,
    ) -> Option<IntParser>;

    fn build_mixed_parser(
        &self,
        st: &mut KwState,
        ps: &[Measurement<Self::P>],
        dt: &AlphaNumType,
        total_events: usize,
    ) -> Option<Option<MixedParser>>;

    fn build_float_parser(
        &self,
        st: &mut KwState,
        is_double: bool,
        par: usize,
        total_events: usize,
        ps: &[Measurement<Self::P>],
    ) -> Option<ColumnParser> {
        let (bytes, dt) = if is_double { (8, "D") } else { (4, "F") };
        let remainder: Vec<_> = ps.iter().filter(|p| p.bytes_eq(bytes)).collect();
        if remainder.is_empty() {
            let byteord = &self.get_byteord();
            let res = if is_double {
                f64::make_matrix_parser(byteord, par, total_events).map(ColumnParser::Double)
            } else {
                f32::make_matrix_parser(byteord, par, total_events).map(ColumnParser::Single)
            };
            match res {
                Ok(x) => Some(x),
                Err(e) => {
                    st.push_meta_error(e);
                    None
                }
            }
        } else {
            for e in remainder.iter().enumerate().map(|(i, p)| {
                format!(
                    "Measurment {} uses {} bytes but DATATYPE={}",
                    i, p.bytes, dt
                )
            }) {
                st.push_meta_error(e);
            }
            None
        }
    }

    fn build_fixed_width_parser(
        st: &mut KwState,
        s: &StdText<Self, Self::P>,
        total_events: usize,
        measurement_widths: Vec<u8>,
    ) -> Option<ColumnParser> {
        // TODO fix cast?
        let par = s.metadata.par as usize;
        let ps = &s.measurements;
        let dt = &s.metadata.datatype;
        let specific = &s.metadata.specific;
        if let Some(mixed) = Self::build_mixed_parser(specific, st, ps, dt, total_events) {
            mixed.map(ColumnParser::Mixed)
        } else {
            match dt {
                AlphaNumType::Single => {
                    specific.build_float_parser(st, false, par, total_events, ps)
                }
                AlphaNumType::Double => {
                    specific.build_float_parser(st, true, par, total_events, ps)
                }
                AlphaNumType::Integer => specific
                    .build_int_parser(st, ps, total_events)
                    .map(ColumnParser::Int),
                AlphaNumType::Ascii => Some(ColumnParser::FixedWidthAscii(FixedAsciiParser {
                    columns: measurement_widths,
                    nrows: total_events,
                })),
            }
        }
    }

    fn build_delim_ascii_parser(s: &StdText<Self, Self::P>, tot: Option<usize>) -> ColumnParser {
        let nbytes = Self::get_data_offsets(s).num_bytes();
        ColumnParser::DelimitedAscii(DelimAsciiParser {
            ncols: s.metadata.par as usize,
            nrows: tot,
            nbytes: nbytes as usize,
        })
    }

    fn build_column_parser(st: &mut KwState, s: &StdText<Self, Self::P>) -> Option<ColumnParser> {
        // In order to make a data parser, the $DATATYPE, $BYTEORD, $PnB, and
        // $PnDATATYPE (if present) all need to be a specific relationship to
        // each other, each of which corresponds to the options below.
        if s.metadata.datatype == AlphaNumType::Ascii && Self::P::fcs_version() >= Version::FCS3_1 {
            st.push_meta_deprecated_str("$DATATYPE=A has been deprecated since FCS 3.1");
        }
        match (Self::get_event_width(s), s.metadata.datatype) {
            // Numeric/Ascii (fixed width)
            (EventWidth::Finite(measurement_widths), _) => {
                let event_width = measurement_widths.iter().map(|x| u32::from(*x)).sum();
                Self::get_total_events(st, s, event_width).and_then(|total_events| {
                    Self::build_fixed_width_parser(st, s, total_events, measurement_widths)
                })
            }
            // Ascii (variable width)
            (EventWidth::Variable, AlphaNumType::Ascii) => {
                let tot = s.metadata.specific.get_tot();
                Some(Self::build_delim_ascii_parser(s, tot.map(|x| x as usize)))
            }
            // nonsense...scream at user
            (EventWidth::Error(fixed, variable), _) => {
                st.push_meta_error_str("$PnBs are a mix of numeric and variable");
                for f in fixed {
                    st.push_meta_error(format!("$PnB for measurement {f} is numeric"));
                }
                for v in variable {
                    st.push_meta_error(format!("$PnB for measurement {v} is variable"));
                }
                None
            }
            (EventWidth::Variable, dt) => {
                st.push_meta_error(format!("$DATATYPE is {dt} but all $PnB are '*'"));
                None
            }
        }
    }

    fn build_data_parser(st: &mut KwState, s: &StdText<Self, Self::P>) -> Option<DataParser> {
        Self::build_column_parser(st, s).map(|column_parser| DataParser {
            column_parser,
            begin: u64::from(Self::get_data_offsets(s).begin),
        })
    }

    fn get_shortnames(s: &StdText<Self, Self::P>) -> Vec<&str> {
        s.measurements
            .iter()
            .filter_map(|p| Self::P::measurement_name(p))
            .collect()
    }

    fn validate_specific(st: &mut KwState, s: &StdText<Self, Self::P>, names: &HashSet<&str>);

    fn validate_time_channel(st: &mut KwState, s: &StdText<Self, Self::P>) {
        if let Some(time_name) = st.conf.time_shortname.as_ref() {
            if let Some(tc) = s
                .measurements
                .iter()
                .find(|p| match Self::P::measurement_name(p) {
                    Some(n) => n == time_name,
                    _ => false,
                })
            {
                if !tc.specific.has_linear_scale() {
                    st.push_meta_error_or_warning(
                        st.conf.ensure_time_linear,
                        String::from("Time channel must have linear $PnE"),
                    );
                }
                if tc.specific.has_gain() {
                    st.push_meta_error_or_warning(
                        st.conf.ensure_time_nogain,
                        String::from("Time channel must not have $PnG"),
                    );
                }
                if !s.metadata.specific.has_timestep() {
                    st.push_meta_error_or_warning(
                        st.conf.ensure_time_timestep,
                        String::from("$TIMESTEP must be present if time channel given"),
                    );
                }
            } else {
                st.push_meta_error_or_warning(
                    st.conf.ensure_time,
                    format!("Channel called '{time_name}' not found for time"),
                );
            }
        }
    }

    fn validate(st: &mut KwState, s: &StdText<Self, Self::P>) {
        let shortnames = s.get_shortnames();

        // ensure all $PnN are unique
        if shortnames.iter().unique().count() != shortnames.len() {
            st.push_meta_error_str("All $PnN must be unique");
        }
        let hs_shortnames: HashSet<&str> = s.get_shortnames().into_iter().collect();

        // ensure time channel is valid if present
        Self::validate_time_channel(st, s);

        // validate $TRIGGER with measurement names
        if let OptionalKw::Present(tr) = &s.metadata.tr {
            if !shortnames.contains(&tr.measurement.as_str()) {
                st.push_meta_error(format!(
                    "Trigger measurement '{}' is not in measurement set",
                    tr.measurement
                ));
            }
        }

        // do any version-specific validation
        Self::validate_specific(st, s, &hs_shortnames);
    }

    fn build_inner(st: &mut KwState) -> Option<Self>;

    fn lookup_metadata(st: &mut KwState) -> Option<Metadata<Self>> {
        Some(Metadata {
            par: st.lookup_par()?,
            nextdata: st.lookup_nextdata()?,
            datatype: st.lookup_datatype()?,
            abrt: st.lookup_abrt(),
            com: st.lookup_com(),
            cells: st.lookup_cells(),
            exp: st.lookup_exp(),
            fil: st.lookup_fil(),
            inst: st.lookup_inst(),
            lost: st.lookup_lost(),
            op: st.lookup_op(),
            proj: st.lookup_proj(),
            smno: st.lookup_smno(),
            src: st.lookup_src(),
            sys: st.lookup_sys(),
            tr: st.lookup_trigger(),
            specific: Self::build_inner(st)?,
        })
    }
}

fn build_int_parser_2_0<X>(
    st: &mut KwState,
    byteord: &ByteOrd,
    ps: &[Measurement<X>],
    total_events: usize,
) -> Option<IntParser> {
    let nbytes = byteord.num_bytes();
    let remainder: Vec<_> = ps.iter().filter(|p| !p.bytes_eq(nbytes)).collect();
    if remainder.is_empty() {
        let (columns, fail): (Vec<_>, Vec<_>) = ps
            .iter()
            .map(|p| p.make_int_parser(byteord, total_events))
            .partition_result();
        let errors: Vec<_> = fail.into_iter().flatten().collect();
        if errors.is_empty() {
            Some(IntParser {
                columns,
                nrows: total_events,
            })
        } else {
            for e in errors.into_iter() {
                st.push_meta_error(e);
            }
            None
        }
    } else {
        for e in remainder.iter().enumerate().map(|(i, p)| {
            format!(
                "Measurement {} uses {} bytes when DATATYPE=I \
                         and BYTEORD implies {} bytes",
                i, p.bytes, nbytes
            )
        }) {
            st.push_meta_error(e);
        }
        None
    }
}

impl VersionedMetadata for InnerMetadata2_0 {
    type P = InnerMeasurment2_0;

    fn into_any_text(t: Box<StdText2_0>) -> AnyStdTEXT {
        AnyStdTEXT::FCS2_0(t)
    }

    fn get_data_offsets(s: &StdText<Self, Self::P>) -> Offsets {
        s.header.data
    }

    fn get_tot(&self) -> Option<u32> {
        self.tot.as_ref().into_option().copied()
    }

    fn get_byteord(&self) -> ByteOrd {
        self.byteord.clone()
    }

    fn has_timestep(&self) -> bool {
        false
    }

    fn build_int_parser(
        &self,
        st: &mut KwState,
        ps: &[Measurement<Self::P>],
        total_events: usize,
    ) -> Option<IntParser> {
        build_int_parser_2_0(st, &self.byteord, ps, total_events)
    }

    fn build_mixed_parser(
        &self,
        _: &mut KwState,
        _: &[Measurement<Self::P>],
        _: &AlphaNumType,
        _: usize,
    ) -> Option<Option<MixedParser>> {
        None
    }

    fn validate_specific(st: &mut KwState, s: &StdText<Self, Self::P>, names: &HashSet<&str>) {}

    fn build_inner(st: &mut KwState) -> Option<InnerMetadata2_0> {
        Some(InnerMetadata2_0 {
            tot: st.lookup_tot_opt(),
            mode: st.lookup_mode()?,
            byteord: st.lookup_byteord()?,
            cyt: st.lookup_cyt_opt(),
            timestamps: st.lookup_timestamps2_0(false, false),
        })
    }
}

impl VersionedMetadata for InnerMetadata3_0 {
    type P = InnerMeasurement3_0;

    fn into_any_text(t: Box<StdText3_0>) -> AnyStdTEXT {
        AnyStdTEXT::FCS3_0(t)
    }

    fn get_data_offsets(s: &StdText<Self, Self::P>) -> Offsets {
        let header_offsets = s.header.data;
        if header_offsets.is_unset() {
            s.metadata.specific.data
        } else {
            header_offsets
        }
    }

    fn get_tot(&self) -> Option<u32> {
        Some(self.tot)
    }

    fn get_byteord(&self) -> ByteOrd {
        self.byteord.clone()
    }

    fn has_timestep(&self) -> bool {
        self.timestep.as_ref().into_option().is_some()
    }

    fn build_int_parser(
        &self,
        st: &mut KwState,
        ps: &[Measurement<Self::P>],
        total_events: usize,
    ) -> Option<IntParser> {
        build_int_parser_2_0(st, &self.byteord, ps, total_events)
    }

    fn build_mixed_parser(
        &self,
        _: &mut KwState,
        _: &[Measurement<Self::P>],
        _: &AlphaNumType,
        _: usize,
    ) -> Option<Option<MixedParser>> {
        None
    }

    fn validate_specific(st: &mut KwState, s: &StdText<Self, Self::P>, names: &HashSet<&str>) {}

    fn build_inner(st: &mut KwState) -> Option<InnerMetadata3_0> {
        Some(InnerMetadata3_0 {
            data: st.lookup_data_offsets()?,
            supplemental: st.lookup_supplemental3_0()?,
            tot: st.lookup_tot_req()?,
            mode: st.lookup_mode()?,
            byteord: st.lookup_byteord()?,
            cyt: st.lookup_cyt_opt(),
            timestamps: st.lookup_timestamps2_0(false, false),
            cytsn: st.lookup_cytsn(),
            timestep: st.lookup_timestep(),
            unicode: st.lookup_unicode(),
        })
    }
}

impl VersionedMetadata for InnerMetadata3_1 {
    type P = InnerMeasurement3_1;

    fn into_any_text(t: Box<StdText3_1>) -> AnyStdTEXT {
        AnyStdTEXT::FCS3_1(t)
    }

    fn get_data_offsets(s: &StdText<Self, Self::P>) -> Offsets {
        let header_offsets = s.header.data;
        if header_offsets.is_unset() {
            s.metadata.specific.data
        } else {
            header_offsets
        }
    }

    fn get_tot(&self) -> Option<u32> {
        Some(self.tot)
    }

    fn get_byteord(&self) -> ByteOrd {
        ByteOrd::Endian(self.byteord)
    }

    fn has_timestep(&self) -> bool {
        self.timestep.as_ref().into_option().is_some()
    }

    fn build_int_parser(
        &self,
        st: &mut KwState,
        ps: &[Measurement<Self::P>],
        total_events: usize,
    ) -> Option<IntParser> {
        build_int_parser_2_0(st, &ByteOrd::Endian(self.byteord), ps, total_events)
    }

    fn build_mixed_parser(
        &self,
        _: &mut KwState,
        _: &[Measurement<Self::P>],
        _: &AlphaNumType,
        _: usize,
    ) -> Option<Option<MixedParser>> {
        None
    }
    fn validate_specific(st: &mut KwState, s: &StdText<Self, Self::P>, names: &HashSet<&str>) {}

    fn build_inner(st: &mut KwState) -> Option<InnerMetadata3_1> {
        let mode = st.lookup_mode()?;
        if mode != Mode::List {
            st.push_meta_deprecated_str("$MODE should only be L");
        };
        Some(InnerMetadata3_1 {
            data: st.lookup_data_offsets()?,
            supplemental: st.lookup_supplemental3_0()?,
            tot: st.lookup_tot_req()?,
            mode,
            byteord: st.lookup_endian()?,
            cyt: st.lookup_cyt_opt(),
            timestamps: st.lookup_timestamps2_0(true, false),
            cytsn: st.lookup_cytsn(),
            timestep: st.lookup_timestep(),
            modification: st.lookup_modification(),
            plate: st.lookup_plate(false),
            vol: st.lookup_vol(),
        })
    }
}

impl VersionedMetadata for InnerMetadata3_2 {
    type P = InnerMeasurement3_2;

    fn into_any_text(t: Box<StdText3_2>) -> AnyStdTEXT {
        AnyStdTEXT::FCS3_2(t)
    }

    // TODO not DRY
    fn get_data_offsets(s: &StdText<Self, Self::P>) -> Offsets {
        let header_offsets = s.header.data;
        if header_offsets.is_unset() {
            s.metadata.specific.data
        } else {
            header_offsets
        }
    }

    fn get_tot(&self) -> Option<u32> {
        Some(self.tot)
    }

    fn get_byteord(&self) -> ByteOrd {
        ByteOrd::Endian(self.byteord)
    }

    fn has_timestep(&self) -> bool {
        self.timestep.as_ref().into_option().is_some()
    }

    fn build_int_parser(
        &self,
        st: &mut KwState,
        ps: &[Measurement<Self::P>],
        total_events: usize,
    ) -> Option<IntParser> {
        build_int_parser_2_0(st, &ByteOrd::Endian(self.byteord), ps, total_events)
    }

    fn build_mixed_parser(
        &self,
        st: &mut KwState,
        ps: &[Measurement<Self::P>],
        dt: &AlphaNumType,
        total_events: usize,
    ) -> Option<Option<MixedParser>> {
        let endian = self.byteord;
        // first test if we have any PnDATATYPEs defined, if no then skip this
        // data parser entirely
        if ps
            .iter()
            .filter(|p| p.specific.datatype.as_ref().into_option().is_some())
            .count()
            == 0
        {
            return None;
        }
        let (pass, fail): (Vec<_>, Vec<_>) = ps
            .iter()
            .enumerate()
            .map(|(i, p)| {
                // TODO this range thing seems not necessary
                match (
                    p.specific.get_column_type(*dt),
                    p.specific.datatype.as_ref().into_option().is_some(),
                    p.range,
                    &p.bytes,
                ) {
                    (AlphaNumType::Ascii, _, _, Bytes::Fixed(bytes)) => {
                        Ok(MixedColumnType::Ascii(AsciiColumn {
                            width: *bytes,
                            data: vec![],
                        }))
                    }
                    (AlphaNumType::Single, _, _, Bytes::Fixed(4)) => Ok(MixedColumnType::Single(
                        f32::make_column_parser(endian, total_events),
                    )),
                    (AlphaNumType::Double, _, _, Bytes::Fixed(8)) => Ok(MixedColumnType::Double(
                        f64::make_column_parser(endian, total_events),
                    )),
                    (AlphaNumType::Integer, _, r, Bytes::Fixed(bytes)) => {
                        make_int_parser(*bytes, &r, &ByteOrd::Endian(self.byteord), total_events)
                            .map(MixedColumnType::Uint)
                    }
                    (dt, overridden, _, bytes) => {
                        let sdt = if overridden { "PnDATATYPE" } else { "DATATYPE" };
                        Err(vec![format!(
                            "{}={} but PnB={} for measurement {}",
                            sdt, dt, bytes, i
                        )])
                    }
                }
            })
            .partition_result();
        if fail.is_empty() {
            Some(Some(MixedParser {
                nrows: total_events,
                columns: pass,
            }))
        } else {
            for e in fail.into_iter().flatten() {
                st.push_meta_error(e);
            }
            None
        }
    }

    fn validate_specific(st: &mut KwState, s: &StdText<Self, Self::P>, names: &HashSet<&str>) {
        let spec = &s.metadata.specific;
        // check that BEGINDATETIME is before ENDDATETIME
        if let (OptionalKw::Present(begin), OptionalKw::Present(end)) =
            (&spec.datetimes.begin, &spec.datetimes.end)
        {
            if end > begin {
                st.push_meta_warning_str("$BEGINDATETIME is after $ENDDATETIME");
            }
        }

        // check that all names in $UNSTAINEDCENTERS match one of the channels
        if let OptionalKw::Present(centers) = &spec.unstained.unstainedcenters {
            for u in centers.keys() {
                if !names.contains(u.as_str()) {
                    st.push_meta_error(format!(
                        "Unstained center named {u} is not in measurement set",
                    ));
                }
            }
        }
    }

    fn build_inner(st: &mut KwState) -> Option<InnerMetadata3_2> {
        // Only L is allowed as of 3.2, so pull the value and check it if
        // given. The only thing we care about here is that the value is not
        // invalid, since we don't need to use it anywhere.
        // TODO this makes more sense as a warning since it doesn't really
        // matter.
        let _ = st.lookup_mode3_2();
        Some(InnerMetadata3_2 {
            data: st.lookup_data_offsets()?,
            supplemental: st.lookup_supplemental3_2(),
            tot: st.lookup_tot_req()?,
            byteord: st.lookup_endian()?,
            cyt: st.lookup_cyt_req()?,
            timestamps: st.lookup_timestamps2_0(true, true),
            cytsn: st.lookup_cytsn(),
            timestep: st.lookup_timestep(),
            modification: st.lookup_modification(),
            plate: st.lookup_plate(true),
            vol: st.lookup_vol(),
            carrier: st.lookup_carrier(),
            datetimes: st.lookup_timestamps3_2(),
            unstained: st.lookup_unstained(),
            flowrate: st.lookup_flowrate(),
        })
    }
}

// type ParsedTEXT2_0 = ParsedTEXT<StdText2_0>;
// type ParsedTEXT3_0 = ParsedTEXT<StdText3_0>;
// type ParsedTEXT3_1 = ParsedTEXT<StdText3_1>;
// type ParsedTEXT3_2 = ParsedTEXT<StdText3_2>;

fn parse_raw_text(header: Header, raw: RawTEXT, conf: &StdTextReader) -> TEXTResult {
    match header.version {
        Version::FCS2_0 => StdText2_0::raw_to_std_text(header, raw, conf),
        Version::FCS3_0 => StdText3_0::raw_to_std_text(header, raw, conf),
        Version::FCS3_1 => StdText3_1::raw_to_std_text(header, raw, conf),
        Version::FCS3_2 => StdText3_2::raw_to_std_text(header, raw, conf),
    }
}

#[derive(Debug, Clone)]
struct KwMsg {
    key: Key,
    value: String,
    msg: String,
    is_error: bool,
}

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
struct Key(String);

impl Key {
    fn as_str(&self) -> &str {
        self.0.as_str()
    }

    fn is_standard(&self) -> bool {
        self.0.starts_with("$")
    }
}

#[derive(Eq, PartialEq)]
enum ValueStatus {
    Raw,
    Error(String),
    Warning(String),
    Used,
}

struct KwValue {
    value: String,
    status: ValueStatus,
}

impl KwValue {
    fn into_error(self, key: Key) -> Option<KwMsg> {
        match self.status {
            ValueStatus::Error(msg) => Some(KwMsg {
                key,
                value: self.value,
                msg,
                is_error: true,
            }),

            _ => None,
        }
    }
}

// all hail the almighty state monad :D

// #[derive(Debug, Clone)]
// struct NonFatalError {
//     DeprecatedMeta(String),
//     DeprecatedKey(Key),
//     // TODO this isn't optimal
//     KeyWarning(KwMsg),
//     MetaWarning(String),
//     DeviantKey(Key),
//     NonStandardKey(Key),
// }

struct KwState<'a> {
    raw_keywords: HashMap<Key, KwValue>,
    missing_keywords: Vec<Key>,
    deprecated_keys: Vec<Key>,
    deprecated_features: Vec<String>,
    meta_errors: Vec<String>,
    meta_warnings: Vec<String>,
    conf: &'a StdTextReader,
}

#[derive(Debug, Clone)]
struct KeyWarning {
    key: Key,
    value: String,
    msg: String,
}

#[derive(Debug, Clone, Default)]
struct NonFatalErrors {
    deprecated_keys: Vec<Key>,
    deprecated_features: Vec<String>,
    meta_warnings: Vec<String>,
    deviant_keywords: HashMap<Key, String>,
    nonstandard_keywords: HashMap<Key, String>,
    keyword_warnings: Vec<KeyWarning>,
}

impl NonFatalErrors {
    fn has_error(&self, conf: &StdTextReader) -> bool {
        (!self.deviant_keywords.is_empty() && conf.disallow_deviant)
            || (!self.deprecated_features.is_empty() && conf.disallow_deprecated)
            || (!self.deprecated_keys.is_empty() && conf.disallow_deprecated)
            || (!self.meta_warnings.is_empty() && conf.warnings_are_errors)
            || (!self.keyword_warnings.is_empty() && conf.warnings_are_errors)
            || (!self.nonstandard_keywords.is_empty() && conf.disallow_nonstandard)
    }
}

#[derive(Debug, Clone)]
pub struct StandardErrors {
    /// Required keywords that are missing
    missing_keywords: Vec<Key>,

    /// Errors that pertain to one keyword value
    value_errors: Vec<KwMsg>,

    /// Errors involving multiple keywords, like PnB not matching DATATYPE
    meta_errors: Vec<String>,

    nonfatal: NonFatalErrors,
}

impl KwState<'_> {
    // TODO not DRY (although will likely need HKTs)
    fn lookup_required<V, F>(&mut self, k: &str, f: F, dep: bool) -> Option<V>
    where
        F: FnOnce(&str) -> ParseResult<V>,
    {
        let sk = format_standard_kw(k);
        match self.raw_keywords.get_mut(&sk) {
            Some(v) => match v.status {
                ValueStatus::Raw => {
                    let (s, r) = f(&v.value).map_or_else(
                        |e| (ValueStatus::Error(e), None),
                        |x| (ValueStatus::Used, Some(x)),
                    );
                    if dep {
                        self.deprecated_keys.push(sk);
                    }
                    v.status = s;
                    r
                }
                _ => None,
            },
            None => {
                self.missing_keywords.push(Key(String::from(k)));
                None
            }
        }
    }

    fn lookup_optional<V, F>(&mut self, k: &str, f: F, dep: bool) -> OptionalKw<V>
    where
        F: FnOnce(&str) -> ParseResult<V>,
    {
        let sk = format_standard_kw(k);
        match self.raw_keywords.get_mut(&sk) {
            Some(v) => match v.status {
                ValueStatus::Raw => {
                    let (s, r) = f(&v.value).map_or_else(
                        |w| (ValueStatus::Warning(w), Absent),
                        |x| (ValueStatus::Used, OptionalKw::Present(x)),
                    );
                    if dep {
                        self.deprecated_keys.push(sk);
                    }
                    v.status = s;
                    r
                }
                _ => Absent,
            },
            None => Absent,
        }
    }

    fn build_offsets(&mut self, begin: u32, end: u32, which: &'static str) -> Option<Offsets> {
        Offsets::new(begin, end).or_else(|| {
            let msg = format!("Could not make {} offset: begin > end", which);
            self.meta_errors.push(msg);
            None
        })
    }

    // metadata

    fn lookup_begindata(&mut self) -> Option<u32> {
        self.lookup_required("BEGINDATA", parse_offset, false)
    }

    fn lookup_enddata(&mut self) -> Option<u32> {
        self.lookup_required("ENDDATA", parse_offset, false)
    }

    fn lookup_data_offsets(&mut self) -> Option<Offsets> {
        if let (Some(begin), Some(end)) = (self.lookup_begindata(), self.lookup_enddata()) {
            self.build_offsets(begin, end, "DATA")
        } else {
            None
        }
    }

    fn lookup_supplemental(&mut self) -> (Option<Offsets>, Option<Offsets>) {
        if let (Some(beginstext), Some(endstext), Some(beginanalysis), Some(endanalysis)) = (
            self.lookup_required("BEGINSTEXT", parse_offset, false),
            self.lookup_required("ENDSTEXT", parse_offset, false),
            self.lookup_required("BEGINANALYSIS", parse_offset_or_blank, false),
            self.lookup_required("ENDANALYSIS", parse_offset_or_blank, false),
        ) {
            (
                self.build_offsets(beginstext, endstext, "STEXT"),
                self.build_offsets(beginanalysis, endanalysis, "ANALYSIS"),
            )
        } else {
            (None, None)
        }
    }

    fn lookup_supplemental3_0(&mut self) -> Option<SupplementalOffsets3_0> {
        if let (Some(stext), Some(analysis)) = self.lookup_supplemental() {
            Some(SupplementalOffsets3_0 { stext, analysis })
        } else {
            None
        }
    }

    fn lookup_supplemental3_2(&mut self) -> SupplementalOffsets3_2 {
        let (stext, analysis) = self.lookup_supplemental();
        SupplementalOffsets3_2 {
            stext: OptionalKw::from_option(stext),
            analysis: OptionalKw::from_option(analysis),
        }
    }

    fn lookup_byteord(&mut self) -> Option<ByteOrd> {
        self.lookup_required(
            "BYTEORD",
            |s| match parse_endian(s) {
                Ok(e) => Ok(ByteOrd::Endian(e)),
                _ => {
                    let xs: Vec<&str> = s.split(",").collect();
                    let nxs = xs.len();
                    let xs_num: Vec<u8> =
                        xs.iter().filter_map(|s| s.parse().ok()).unique().collect();
                    if let (Some(min), Some(max)) = (xs_num.iter().min(), xs_num.iter().max()) {
                        if *min == 1 && usize::from(*max) == nxs && xs_num.len() == nxs {
                            Ok(ByteOrd::Mixed(xs_num.iter().map(|x| x - 1).collect()))
                        } else {
                            Err(String::from("invalid byte order"))
                        }
                    } else {
                        Err(String::from("could not parse numbers from byte order"))
                    }
                }
            },
            false,
        )
    }

    fn lookup_endian(&mut self) -> Option<Endian> {
        self.lookup_required("BYTEORD", parse_endian, false)
    }

    fn lookup_datatype(&mut self) -> Option<AlphaNumType> {
        self.lookup_required(
            "DATATYPE",
            |s| match s {
                "I" => Ok(AlphaNumType::Integer),
                "F" => Ok(AlphaNumType::Single),
                "D" => Ok(AlphaNumType::Double),
                "A" => Ok(AlphaNumType::Ascii),
                _ => Err(String::from("unknown datatype")),
            },
            false,
        )
    }

    fn lookup_mode(&mut self) -> Option<Mode> {
        self.lookup_required(
            "MODE",
            |s| match s {
                "C" => Ok(Mode::Correlated),
                "L" => Ok(Mode::List),
                "U" => Ok(Mode::Uncorrelated),
                _ => Err(String::from("unknown mode")),
            },
            false,
        )
    }

    fn lookup_mode3_2(&mut self) -> OptionalKw<Mode> {
        self.lookup_optional(
            "MODE",
            |s| match s {
                "L" => Ok(Mode::List),
                _ => Err(String::from("unknown mode (U and C are no longer valid)")),
            },
            true,
        )
    }

    fn lookup_nextdata(&mut self) -> Option<u32> {
        self.lookup_required("NEXTDATA", parse_offset, false)
    }

    fn lookup_par(&mut self) -> Option<u32> {
        self.lookup_required("PAR", parse_int, false)
    }

    fn lookup_tot_req(&mut self) -> Option<u32> {
        self.lookup_required("TOT", parse_int, false)
    }

    fn lookup_tot_opt(&mut self) -> OptionalKw<u32> {
        self.lookup_optional("TOT", parse_int, false)
    }

    fn lookup_cyt_req(&mut self) -> Option<String> {
        self.lookup_required("CYT", parse_str, false)
    }

    fn lookup_cyt_opt(&mut self) -> OptionalKw<String> {
        self.lookup_optional("CYT", parse_str, false)
    }

    fn lookup_abrt(&mut self) -> OptionalKw<u32> {
        self.lookup_optional("ABRT", parse_int, false)
    }

    fn lookup_cells(&mut self) -> OptionalKw<String> {
        self.lookup_optional("CELLS", parse_str, false)
    }

    fn lookup_com(&mut self) -> OptionalKw<String> {
        self.lookup_optional("COM", parse_str, false)
    }

    fn lookup_exp(&mut self) -> OptionalKw<String> {
        self.lookup_optional("EXP", parse_str, false)
    }

    fn lookup_fil(&mut self) -> OptionalKw<String> {
        self.lookup_optional("FIL", parse_str, false)
    }

    fn lookup_inst(&mut self) -> OptionalKw<String> {
        self.lookup_optional("INST", parse_str, false)
    }

    fn lookup_lost(&mut self) -> OptionalKw<u32> {
        self.lookup_optional("LOST", parse_int, false)
    }

    fn lookup_op(&mut self) -> OptionalKw<String> {
        self.lookup_optional("OP", parse_str, false)
    }

    fn lookup_proj(&mut self) -> OptionalKw<String> {
        self.lookup_optional("PROJ", parse_str, false)
    }

    fn lookup_smno(&mut self) -> OptionalKw<String> {
        self.lookup_optional("SMNO", parse_str, false)
    }

    fn lookup_src(&mut self) -> OptionalKw<String> {
        self.lookup_optional("SRC", parse_str, false)
    }

    fn lookup_sys(&mut self) -> OptionalKw<String> {
        self.lookup_optional("SYS", parse_str, false)
    }

    fn lookup_trigger(&mut self) -> OptionalKw<Trigger> {
        self.lookup_optional(
            "TR",
            |s| match s.split(",").collect::<Vec<&str>>()[..] {
                [p, n1] => parse_int(n1).map(|threshold| Trigger {
                    measurement: String::from(p),
                    threshold,
                }),
                _ => Err(String::from("wrong number of fields")),
            },
            false,
        )
    }

    fn lookup_cytsn(&mut self) -> OptionalKw<String> {
        self.lookup_optional("CYTSN", parse_str, false)
    }

    fn lookup_timestep(&mut self) -> OptionalKw<f32> {
        self.lookup_optional("TIMESTEP", parse_float, false)
    }

    fn lookup_vol(&mut self) -> OptionalKw<f32> {
        self.lookup_optional("VOL", parse_float, false)
    }

    fn lookup_flowrate(&mut self) -> OptionalKw<String> {
        self.lookup_optional("FLOWRATE", parse_str, false)
    }

    fn lookup_unicode(&mut self) -> OptionalKw<Unicode> {
        self.lookup_optional(
            "UNICODE",
            |s| {
                let mut xs = s.split(",");
                if let Some(page) = xs.next().and_then(|s| s.parse().ok()) {
                    let kws: Vec<String> = xs.map(String::from).collect();
                    if kws.is_empty() {
                        Err(String::from("no keywords specified"))
                    } else {
                        Ok(Unicode { page, kws })
                    }
                } else {
                    Err(String::from("unicode must be like 'page,KW1[,KW2...]'"))
                }
            },
            false,
        )
    }

    fn lookup_plateid(&mut self, dep: bool) -> OptionalKw<String> {
        self.lookup_optional("PLATEID", parse_str, dep)
    }

    fn lookup_platename(&mut self, dep: bool) -> OptionalKw<String> {
        self.lookup_optional("PLATENAME", parse_str, dep)
    }

    fn lookup_wellid(&mut self, dep: bool) -> OptionalKw<String> {
        self.lookup_optional("WELLID", parse_str, dep)
    }

    fn lookup_unstainedinfo(&mut self) -> OptionalKw<String> {
        self.lookup_optional("UNSTAINEDINFO", parse_str, false)
    }

    fn lookup_unstainedcenters(&mut self) -> OptionalKw<UnstainedCenters> {
        self.lookup_optional(
            "UNSTAINEDICENTERS",
            |s| {
                let mut xs = s.split(",");
                if let Some(n) = xs.next().and_then(|s| s.parse().ok()) {
                    let rest: Vec<&str> = xs.collect();
                    let mut us = HashMap::new();
                    if rest.len() == 2 * n {
                        for i in 0..n {
                            if let Ok(v) = rest[i + 8].parse() {
                                us.insert(String::from(rest[i]), v);
                            } else {
                                return Err(String::from("invalid numeric encountered"));
                            }
                        }
                        Ok(us)
                    } else {
                        Err(String::from("data fields do not match given dimensions"))
                    }
                } else {
                    Err(String::from("invalid dimension"))
                }
            },
            false,
        )
    }

    fn lookup_last_modifier(&mut self) -> OptionalKw<String> {
        self.lookup_optional("LAST_MODIFIER", parse_str, false)
    }

    fn lookup_last_modified(&mut self) -> OptionalKw<NaiveDateTime> {
        // TODO hopefully case doesn't matter...
        self.lookup_optional(
            "LAST_MODIFIED",
            |s| {
                NaiveDateTime::parse_from_str(s, "%d-%b-%Y %H:%M:%S.%.3f").or(
                    NaiveDateTime::parse_from_str(s, "%d-%b-%Y %H:%M:%S").or(Err(String::from(
                        "must be formatted like 'dd-mmm-yyyy hh:mm:ss[.cc]'",
                    ))),
                )
            },
            false,
        )
    }

    fn lookup_originality(&mut self) -> OptionalKw<Originality> {
        self.lookup_optional(
            "ORIGINALITY",
            |s| match s {
                "Original" => Ok(Originality::Original),
                "NonDataModified" => Ok(Originality::NonDataModified),
                "Appended" => Ok(Originality::Appended),
                "DataModified" => Ok(Originality::DataModified),
                _ => Err(String::from("invalid originality")),
            },
            false,
        )
    }

    fn lookup_carrierid(&mut self) -> OptionalKw<String> {
        self.lookup_optional("CARRIERID", parse_str, false)
    }

    fn lookup_carriertype(&mut self) -> OptionalKw<String> {
        self.lookup_optional("CARRIERTYPE", parse_str, false)
    }

    fn lookup_locationid(&mut self) -> OptionalKw<String> {
        self.lookup_optional("LOCATIONID", parse_str, false)
    }

    fn lookup_begindatetime(&mut self) -> OptionalKw<DateTime<FixedOffset>> {
        self.lookup_optional("BEGINDATETIME", parse_iso_datetime, false)
    }

    fn lookup_enddatetime(&mut self) -> OptionalKw<DateTime<FixedOffset>> {
        self.lookup_optional("ENDDATETIME", parse_iso_datetime, false)
    }

    fn lookup_date(&mut self, dep: bool) -> OptionalKw<NaiveDate> {
        // the "%b" format is case-insensitive so this should work for "Jan", "JAN",
        // "jan", "jaN", etc
        self.lookup_optional(
            "DATE",
            |s| {
                if let Some(pattern) = &self.conf.date_pattern {
                    NaiveDate::parse_from_str(s, pattern.as_str())
                        .or(Err(format!("does not match pattern '{pattern}'")))
                } else {
                    NaiveDate::parse_from_str(s, "%d-%b-%Y")
                        .or(Err(String::from("must be formatted like 'dd-mmm-yyyy'")))
                }
            },
            dep,
        )
    }

    fn lookup_btim60(&mut self, dep: bool) -> OptionalKw<NaiveTime> {
        self.lookup_optional("BTIM", parse_time60, dep)
    }

    fn lookup_etim60(&mut self, dep: bool) -> OptionalKw<NaiveTime> {
        self.lookup_optional("ETIM", parse_time60, dep)
    }

    fn lookup_btim100(&mut self, dep: bool) -> OptionalKw<NaiveTime> {
        self.lookup_optional("BTIM", parse_time100, dep)
    }

    fn lookup_etim100(&mut self, dep: bool) -> OptionalKw<NaiveTime> {
        self.lookup_optional("ETIM", parse_time100, dep)
    }

    fn lookup_timestamps2_0(&mut self, centi: bool, dep: bool) -> Timestamps2_0 {
        let (t0, t1) = if centi {
            (self.lookup_btim60(dep), self.lookup_etim60(dep))
        } else {
            (self.lookup_btim100(dep), self.lookup_etim100(dep))
        };
        Timestamps2_0 {
            btim: t0,
            etim: t1,
            date: self.lookup_date(dep),
        }
    }

    fn lookup_timestamps3_2(&mut self) -> Timestamps3_2 {
        Timestamps3_2 {
            begin: self.lookup_begindatetime(),
            end: self.lookup_enddatetime(),
        }
    }

    fn lookup_modification(&mut self) -> ModificationData {
        ModificationData {
            last_modifier: self.lookup_last_modifier(),
            last_modified: self.lookup_last_modified(),
            originality: self.lookup_originality(),
        }
    }

    fn lookup_plate(&mut self, dep: bool) -> PlateData {
        PlateData {
            wellid: self.lookup_plateid(dep),
            platename: self.lookup_platename(dep),
            plateid: self.lookup_wellid(dep),
        }
    }

    fn lookup_carrier(&mut self) -> CarrierData {
        CarrierData {
            locationid: self.lookup_locationid(),
            carrierid: self.lookup_carrierid(),
            carriertype: self.lookup_carriertype(),
        }
    }

    fn lookup_unstained(&mut self) -> UnstainedData {
        UnstainedData {
            unstainedcenters: self.lookup_unstainedcenters(),
            unstainedinfo: self.lookup_unstainedinfo(),
        }
    }

    // TODO comp matrices

    // measurements

    fn lookup_meas_req<V, F>(&mut self, m: &'static str, n: u32, f: F, dep: bool) -> Option<V>
    where
        F: FnOnce(&str) -> ParseResult<V>,
    {
        self.lookup_required(&format_measurement(n, m), f, dep)
    }

    fn lookup_meas_opt<V, F>(&mut self, m: &'static str, n: u32, f: F, dep: bool) -> OptionalKw<V>
    where
        F: FnOnce(&str) -> ParseResult<V>,
    {
        self.lookup_optional(&format_measurement(n, m), f, dep)
    }

    // this is actually read the PnB field which has "bits" in it, but as
    // far as I know nobody is using anything other than evenly-spaced bytes
    fn lookup_meas_bytes(&mut self, n: u32) -> Option<Bytes> {
        self.lookup_meas_req("B", n, parse_bytes, false)
    }

    fn lookup_meas_range(&mut self, n: u32) -> Option<Range> {
        self.lookup_meas_req(
            "R",
            n,
            |s| match s.parse::<u64>() {
                Ok(x) => Ok(Range::Int(x - 1)),
                Err(e) => match e.kind() {
                    IntErrorKind::InvalidDigit => s
                        .parse::<f64>()
                        .map_or_else(|e| Err(format!("{}", e)), |x| Ok(Range::Float(x))),
                    IntErrorKind::PosOverflow => Ok(Range::Int(u64::MAX)),
                    _ => Err(format!("{}", e)),
                },
            },
            false,
        )
    }

    fn lookup_meas_wavelength(&mut self, n: u32) -> OptionalKw<u32> {
        self.lookup_meas_opt("L", n, parse_int, false)
    }

    fn lookup_meas_power(&mut self, n: u32) -> OptionalKw<u32> {
        self.lookup_meas_opt("O", n, parse_int, false)
    }

    fn lookup_meas_detector_type(&mut self, n: u32) -> OptionalKw<String> {
        self.lookup_meas_opt("T", n, parse_str, false)
    }

    fn lookup_meas_shortname_req(&mut self, n: u32) -> Option<String> {
        self.lookup_meas_req("N", n, parse_str, false)
    }

    fn lookup_meas_shortname_opt(&mut self, n: u32) -> OptionalKw<String> {
        self.lookup_meas_opt(
            "N",
            n,
            |s| {
                if s.contains(',') {
                    Err(String::from("commas are not allowed in PnN"))
                } else {
                    Ok(String::from(s))
                }
            },
            false,
        )
    }

    fn lookup_meas_longname(&mut self, n: u32) -> OptionalKw<String> {
        self.lookup_meas_opt("S", n, parse_str, false)
    }

    fn lookup_meas_filter(&mut self, n: u32) -> OptionalKw<String> {
        self.lookup_meas_opt("F", n, parse_str, false)
    }

    fn lookup_meas_percent_emitted(&mut self, n: u32, dep: bool) -> OptionalKw<u32> {
        self.lookup_meas_opt("P", n, parse_int, dep)
    }

    fn lookup_meas_detector_voltage(&mut self, n: u32) -> OptionalKw<f32> {
        self.lookup_meas_opt("V", n, parse_float, false)
    }

    fn lookup_meas_detector(&mut self, n: u32) -> OptionalKw<String> {
        self.lookup_meas_opt("DET", n, parse_str, false)
    }

    fn lookup_meas_tag(&mut self, n: u32) -> OptionalKw<String> {
        self.lookup_meas_opt("TAG", n, parse_str, false)
    }

    fn lookup_meas_analyte(&mut self, n: u32) -> OptionalKw<String> {
        self.lookup_meas_opt("ANALYTE", n, parse_str, false)
    }

    fn lookup_meas_gain(&mut self, n: u32) -> OptionalKw<f32> {
        self.lookup_meas_opt("G", n, parse_float, false)
    }

    fn lookup_meas_scale_req(&mut self, n: u32) -> Option<Scale> {
        self.lookup_meas_req("E", n, parse_scale, false)
    }

    fn lookup_meas_scale_opt(&mut self, n: u32) -> OptionalKw<Scale> {
        self.lookup_meas_opt("E", n, parse_scale, false)
    }

    fn lookup_meas_calibration(&mut self, n: u32) -> OptionalKw<Calibration> {
        self.lookup_meas_opt(
            "CALIBRATION",
            n,
            |s| {
                let v: Vec<&str> = s.split(",").collect();
                match v[..] {
                    [svalue, unit] => match svalue.parse() {
                        Ok(value) if value >= 0.0 => Ok(Calibration {
                            value,
                            unit: String::from(unit),
                        }),
                        _ => Err(String::from("invalid (positive) float")),
                    },
                    _ => Err(String::from("too many fields")),
                }
            },
            false,
        )
    }

    // for 3.1+ PnL measurements, which can have multiple wavelengths
    fn lookup_meas_wavelengths(&mut self, n: u32) -> Vec<u32> {
        self.lookup_meas_opt(
            "L",
            n,
            |s| {
                let mut ws = vec![];
                for x in s.split(",") {
                    match x.parse() {
                        Ok(y) => ws.push(y),
                        _ => return Err(String::from("invalid float encountered")),
                    };
                }
                Ok(ws)
            },
            false,
        )
        .into_option()
        .unwrap_or_default()
    }

    fn lookup_meas_display(&mut self, n: u32) -> OptionalKw<Display> {
        self.lookup_meas_opt(
            "D",
            n,
            |s| {
                let v: Vec<&str> = s.split(",").collect();
                match v[..] {
                    [which, f1, f2] => match (which, f1.parse(), f2.parse()) {
                        ("Linear", Ok(lower), Ok(upper)) => {
                            Ok(Display::Lin(LinDisplay { lower, upper }))
                        }
                        ("Logarithmic", Ok(decades), Ok(offset)) => {
                            Ok(Display::Log(LogDisplay { decades, offset }))
                        }
                        _ => Err(String::from("invalid floats")),
                    },
                    _ => Err(String::from("too many fields")),
                }
            },
            false,
        )
    }

    fn lookup_meas_datatype(&mut self, n: u32) -> OptionalKw<NumType> {
        self.lookup_meas_opt(
            "DATATYPE",
            n,
            |s| match s {
                "I" => Ok(NumType::Integer),
                "F" => Ok(NumType::Single),
                "D" => Ok(NumType::Double),
                _ => Err(String::from("unknown datatype")),
            },
            false,
        )
    }

    fn lookup_meas_type(&mut self, n: u32) -> OptionalKw<MeasurementType> {
        self.lookup_meas_opt(
            "TYPE",
            n,
            |s| match s {
                "Forward Scatter" => Ok(MeasurementType::ForwardScatter),
                "Raw Fluorescence" => Ok(MeasurementType::RawFluorescence),
                "Mass" => Ok(MeasurementType::Mass),
                "Time" => Ok(MeasurementType::Time),
                "Index" => Ok(MeasurementType::Index),
                "Classification" => Ok(MeasurementType::Classification),
                s => Ok(MeasurementType::Other(String::from(s))),
            },
            false,
        )
    }

    fn lookup_meas_feature(&mut self, n: u32) -> OptionalKw<Feature> {
        self.lookup_meas_opt(
            "FEATURE",
            n,
            |s| match s {
                "Area" => Ok(Feature::Area),
                "Width" => Ok(Feature::Width),
                "Height" => Ok(Feature::Height),
                _ => Err(String::from("unknown measurement feature")),
            },
            false,
        )
    }

    /// Find nonstandard keys that a specific for a given measurement
    fn lookup_meas_nonstandard(&mut self, n: u32) -> HashMap<Key, String> {
        let mut ns = HashMap::new();
        // ASSUME the pattern does not start with "$" and has a %n which will be
        // subbed for the measurement index. The pattern will then be turned
        // into a legit rust regular expression, which may fail depending on
        // what %n does, so check it each time.
        if let Some(p) = &self.conf.nonstandard_measurement_pattern {
            let rep = p.replace("%n", n.to_string().as_str());
            if let Ok(pattern) = Regex::new(rep.as_str()) {
                for (k, v) in self.raw_keywords.iter() {
                    if let ValueStatus::Raw = v.status {
                        if pattern.is_match(k.as_str()) {
                            ns.insert(k.clone(), v.value.clone());
                        }
                    }
                }
            } else {
                self.push_meta_warning(format!(
                    "Could not make regular expression using \
                     pattern '{rep}' for measurement {n}"
                ));
            }
        }
        // TODO it seems like there should be a more efficient way to do this,
        // but the only ways I can think of involve taking ownership of the
        // keywords and then moving matching key/vals into a new hashlist.
        for k in ns.keys() {
            self.raw_keywords.remove(k);
        }
        ns
    }

    fn push_meta_error_str(&mut self, msg: &str) {
        self.push_meta_error(String::from(msg));
    }

    fn push_meta_error(&mut self, msg: String) {
        self.meta_errors.push(msg);
    }

    fn push_meta_warning_str(&mut self, msg: &str) {
        self.push_meta_warning(String::from(msg));
    }

    fn push_meta_warning(&mut self, msg: String) {
        self.meta_warnings.push(msg);
    }

    fn push_meta_deprecated_str(&mut self, msg: &str) {
        self.deprecated_features.push(String::from(msg));
    }

    fn push_meta_error_or_warning(&mut self, is_error: bool, msg: String) {
        if is_error {
            self.meta_errors.push(msg);
        } else {
            self.meta_warnings.push(msg);
        }
    }

    fn split_keywords(
        kws: HashMap<Key, KwValue>,
        deprecated_keys: Vec<Key>,
        deprecated_features: Vec<String>,
        meta_warnings: Vec<String>,
    ) -> (Vec<KwMsg>, NonFatalErrors) {
        let mut nonstandard_keywords = HashMap::new();
        let mut deviant_keywords = HashMap::new();
        let mut keyword_warnings = Vec::new();
        let mut value_errors = Vec::new();
        for (key, v) in kws {
            match v.status {
                ValueStatus::Raw => {
                    if key.is_standard() {
                        deviant_keywords.insert(key, v.value);
                    } else {
                        nonstandard_keywords.insert(key, v.value);
                    }
                }
                ValueStatus::Warning(msg) => keyword_warnings.push(KeyWarning {
                    msg,
                    key,
                    value: v.value,
                }),
                ValueStatus::Error(msg) => value_errors.push(KwMsg {
                    msg,
                    key,
                    value: v.value,
                    is_error: true,
                }),
                ValueStatus::Used => (),
            }
        }
        // let nfk = NonFatalKeyErrors {
        // };

        // let nfm = NonFatalMetaErrors {
        // };
        let nfe = NonFatalErrors {
            deviant_keywords,
            keyword_warnings,
            nonstandard_keywords,
            deprecated_keys,
            deprecated_features,
            meta_warnings,
        };
        (value_errors, nfe)
    }

    fn keys_maybe<K, V>(test: bool, map: HashMap<K, V>) -> Vec<K> {
        if test {
            vec![]
        } else {
            map.into_keys().collect()
        }
    }

    fn into_result<M: VersionedMetadata>(
        self,
        standard: StdText<M, <M as VersionedMetadata>::P>,
        data_parser: DataParser,
    ) -> TEXTResult {
        let (value_errors, nonfatal) = Self::split_keywords(
            self.raw_keywords,
            self.deprecated_keys,
            self.deprecated_features,
            self.meta_warnings,
        );
        if !value_errors.is_empty() || nonfatal.has_error(self.conf) {
            // TODO this doesn't include nonstandard measurements, which is
            // probably fine, because if the user didn't want to include them
            // in the ns measurement field they wouldn't have used that param
            // anyways, in which case we probably need to call them something
            // different (like "upgradable")
            Err(StandardErrors {
                missing_keywords: self.missing_keywords,
                value_errors,
                meta_errors: self.meta_errors,
                nonfatal,
            })
        } else {
            Ok(ParsedTEXT {
                standard: M::into_any_text(Box::new(standard)),
                nonfatal,
                data_parser,
            })
        }
    }

    fn into_errors(self) -> StandardErrors {
        let (value_errors, nonfatal) = Self::split_keywords(
            self.raw_keywords,
            self.deprecated_keys,
            self.deprecated_features,
            self.meta_warnings,
        );
        StandardErrors {
            missing_keywords: self.missing_keywords,
            value_errors,
            meta_errors: self.meta_errors,
            nonfatal,
        }
    }
}

fn parse_header_offset(s: &str, allow_blank: bool) -> Option<u32> {
    if allow_blank && s.trim().is_empty() {
        return Some(0);
    }
    let re = Regex::new(r" *(\d+)").unwrap();
    re.captures(s).map(|c| {
        let [i] = c.extract().1;
        i.parse().unwrap()
    })
}

fn parse_bounds(s0: &str, s1: &str, allow_blank: bool) -> Result<Offsets, &'static str> {
    if let (Some(begin), Some(end)) = (
        parse_header_offset(s0, allow_blank),
        parse_header_offset(s1, allow_blank),
    ) {
        if begin > end {
            Err("beginning is greater than end")
        } else {
            Ok(Offsets { begin, end })
        }
    } else if allow_blank {
        Err("could not make bounds from integers/blanks")
    } else {
        Err("could not make bounds from integers")
    }
}

const hre: &str = r"FCS(.{3})    (.{8})(.{8})(.{8})(.{8})(.{8})(.{8})";

fn parse_header(s: &str) -> Result<Header, &'static str> {
    let re = Regex::new(hre).unwrap();
    re.captures(s)
        .and_then(|c| {
            let [v, t0, t1, d0, d1, a0, a1] = c.extract().1;
            if let (Some(version), Ok(text), Ok(data), Ok(analysis)) = (
                Version::parse(v),
                parse_bounds(t0, t1, false),
                parse_bounds(d0, d1, false),
                parse_bounds(a0, a1, true),
            ) {
                Some(Header {
                    version,
                    text,
                    data,
                    analysis,
                })
            } else {
                None
            }
        })
        .ok_or("malformed header")
}

fn read_header<R: Read>(h: &mut BufReader<R>) -> io::Result<Header> {
    let mut verbuf = [0; 58];
    h.read_exact(&mut verbuf)?;
    if let Ok(hs) = str::from_utf8(&verbuf) {
        parse_header(hs).map_err(io::Error::other)
    } else {
        Err(io::Error::other("header sequence is not valid text"))
    }
}

#[derive(Debug, Clone)]
struct RawTEXT {
    delimiter: u8,
    keywords: HashMap<Key, String>,
    warnings: Vec<String>,
}

impl RawTEXT {
    fn to_state<'a>(&self, conf: &'a StdTextReader) -> KwState<'a> {
        let mut keywords = HashMap::new();
        for (k, v) in self.keywords.iter() {
            keywords.insert(
                k.clone(),
                KwValue {
                    value: v.clone(),
                    status: ValueStatus::Raw,
                },
            );
        }
        KwState {
            raw_keywords: keywords,
            deprecated_keys: vec![],
            deprecated_features: vec![],
            missing_keywords: vec![],
            meta_errors: vec![],
            meta_warnings: vec![],
            conf,
        }
    }
}

pub struct FCSSuccess<T> {
    pub std: T,
    pub data: ParsedData,
}

fn split_raw_text(xs: &[u8], conf: &RawTextReader) -> Result<RawTEXT, String> {
    // this needs the entire TEXT segment to be loaded in memory, which should
    // be fine since the max number of bytes is ~100M given the upper limit
    // imposed by the header
    //
    // 1. bounded by textstart/end
    // 2. first character is delimiter (and so is the last, I think...)
    // 3. two delimiters in a row shall be replaced by one delimiter
    // 4. no value shall be blank, so (3) implies that delimiters can be present in
    //    keys or values
    // 5. there should be an even number of delimited fields (obviously)
    // 6. delimiters should not be at the start or end of word; for instance, if
    //    we encountered XXX (where X is delim) it wouldn't be clear if the first
    //    or third X is the boundary and if the other two belong to the previous or
    //    or next keyword. Implication: keywords can only appear once in a row or
    //    an even number of times in a row.
    let mut keywords: HashMap<_, _> = HashMap::new();
    let mut warnings = vec![];
    let textlen = xs.len();
    let mut warn_or_err2 = |is_error, err, warn| {
        if is_error || conf.warnings_are_errors {
            Err(err)
        } else {
            warnings.push(warn);
            Ok(())
        }
    };

    // First character is the delimiter
    let delimiter: u8 = xs[0];
    let str_delim = String::from_utf8(vec![delimiter]).or(Err(String::from(
        "First character of TEXT segment is not a valid UTF-8 byte, parsing failed",
    )))?;

    // Check that the delim is valid; this is technically only written in the
    // spec for 3.1+ but for older versions this should still be true since
    // these were ASCII-everywhere
    if !(1..=126).contains(&delimiter) {
        warn_or_err2(
            conf.force_ascii_delim,
            String::from("delimiter must be an ASCII character 1-126, got {str_delim}"),
            String::from("delimiter should be an ASCII character 1-126, got {str_delim}"),
        )?
    }

    // Read from the 2nd character to all but the last character and record
    // delimiter positions.
    let delim_positions: Vec<_> = xs
        .iter()
        .enumerate()
        .filter_map(|(i, c)| if *c == delimiter { Some(i) } else { None })
        .collect();

    let raw_boundaries = delim_positions
        // We force-added the first and last delimiter in the TEXT segment, so
        // this is guaranteed to have at least two elements (one pair)
        .windows(2)
        .filter_map(|x| match x {
            [a, b] => Some((*a, b - a)),
            _ => None,
        });

    // Compute word boundaries depending on if we want to "escape" delims or
    // not. Technically all versions of the standard allow double delimiters to
    // be used in a word to represented a single delimiter. However, this means
    // we also can't have blank values. Many FCS files unfortunately use blank
    // values, so we need to be able to toggle this behavior.
    let boundaries = if conf.no_delim_escape {
        raw_boundaries.collect()
    } else {
        // Remove "escaped" delimiters from position vector. Because we disallow
        // blank values and also disallow delimiters at the start/end of words,
        // this implies that we should only see delimiters by themselves or in a
        // consecutive sequence whose length is even.
        let mut filtered_boundaries = vec![];
        for (key, chunk) in raw_boundaries.chunk_by(|(_, x)| *x).into_iter() {
            if key == 1 {
                if chunk.count() % 2 == 1 {
                    return Err(format!("delimiter '{str_delim}' found at word boundary"));
                }
            } else {
                for x in chunk {
                    filtered_boundaries.push(x);
                }
            }
        }

        // If all went well in the previous step, we should have the following:
        // 1. at least one boundary
        // 2. first entry coincides with start of TEXT
        // 3. last entry coincides with end of TEXT
        if let (Some((x0, _)), Some((xf, len))) =
            (filtered_boundaries.first(), filtered_boundaries.last())
        {
            if *x0 > 0 {
                return Err(format!("first key starts with a delim '{str_delim}'"));
            }
            if *xf + len < textlen - 1 {
                return Err(format!("final value ends with a delim '{str_delim}'",));
            }
        } else {
            return Err(String::from("no boundaries found, cannot parse keywords"));
        }
        filtered_boundaries
    };

    // Check that the last char is also a delim, if not file probably sketchy
    // ASSUME this will not fail since we have at least one delim by definition
    if !delim_positions.last().unwrap() == xs.len() - 1 {
        warn_or_err2(
            conf.enforce_final_delim,
            format!("last TEXT char is not {str_delim}"),
            format!("last TEXT char is not delim {str_delim}, parsing may have failed"),
        )?
    }

    let delim2 = [delimiter, delimiter];
    let delim1 = [delimiter];
    // ASSUME these won't fail as we checked the delimiter is an ASCII character
    let escape_from = str::from_utf8(&delim2).unwrap();
    let escape_to = str::from_utf8(&delim1).unwrap();

    let final_boundaries: Vec<_> = boundaries
        .into_iter()
        .map(|(a, b)| (a + 1, a + b))
        .collect();

    for chunk in final_boundaries.chunks(2) {
        if let [(ki, kf), (vi, vf)] = *chunk {
            if let (Ok(k), Ok(v)) = (str::from_utf8(&xs[ki..kf]), str::from_utf8(&xs[vi..vf])) {
                let kupper = k.to_uppercase();
                // test if keyword is ascii
                if !kupper.is_ascii() {
                    warn_or_err2(
                        conf.enfore_keyword_ascii,
                        String::from("keywords must be ASCII"),
                        String::from("keywords should be ASCII"),
                    )?
                }
                // if delimiters were escaped, replace them here
                let res = if conf.no_delim_escape {
                    // Test for empty values if we don't allow delim escaping;
                    // anything empty will either drop or produce an error
                    // depending on user settings
                    if v.is_empty() {
                        let msg = format!("key {kupper} has a blank value");
                        warn_or_err2(
                            conf.enforce_nonempty,
                            msg.clone(),
                            format!("{}, dropping", msg),
                        )?;
                        None
                    } else {
                        keywords.insert(Key(kupper.clone()), v.to_string())
                    }
                } else {
                    let krep = kupper.replace(escape_from, escape_to);
                    let rrep = v.replace(escape_from, escape_to);
                    keywords.insert(Key(krep), rrep)
                };
                // test if the key was inserted already
                if res.is_some() {
                    let msg = format!("key {kupper} is found more than once");
                    warn_or_err2(conf.enforce_unique, msg.clone(), msg)?
                }
            } else {
                let msg = String::from("invalid UTF-8 byte encountered when parsing TEXT");
                warn_or_err2(conf.error_on_invalid_utf8, msg.clone(), msg)?
            }
        } else {
            warn_or_err2(
                conf.enforce_even,
                String::from("number of words is not even"),
                String::from("number of words is not even, parse may have failed"),
            )?
        }
    }

    Ok(RawTEXT {
        delimiter,
        keywords,
        warnings,
    })
}

fn read_raw_text<R: Read + Seek>(
    h: &mut BufReader<R>,
    header: &Header,
    conf: &RawTextReader,
) -> io::Result<RawTEXT> {
    if let Some(adjusted) = header.text.adjust(conf.textstart_delta, conf.textend_delta) {
        let begin = u64::from(adjusted.begin);
        let nbytes = u64::from(adjusted.num_bytes());
        let mut buf = vec![];

        h.seek(SeekFrom::Start(begin))?;
        h.take(nbytes).read_to_end(&mut buf)?;
        split_raw_text(&buf, conf).map_err(io::Error::other)
    } else {
        Err(io::Error::other(
            "Adjusted begin is after adjusted end for TEXT",
        ))
    }
}

/// Instructions for reading the TEXT segment as raw key/value pairs.
pub struct RawTextReader {
    /// Will adjust the offset of the start of the TEXT segment by `offset + n`.
    textstart_delta: u32,

    /// Will adjust the offset of the end of the TEXT segment by `offset + n`.
    textend_delta: u32,

    /// If true, all raw text parsing warnings will be considered fatal errors
    /// which will halt the parsing routine.
    warnings_are_errors: bool,

    /// Will treat every delimiter as a literal delimiter rather than "escaping"
    /// double delimiters
    no_delim_escape: bool,

    /// If true, only ASCII characters 1-126 will be allowed for the delimiter
    force_ascii_delim: bool,

    /// If true, throw an error if the last byte of the TEXT segment is not
    /// a delimiter.
    enforce_final_delim: bool,

    /// If true, throw an error if any key in the TEXT segment is not unique
    enforce_unique: bool,

    /// If true, throw an error if the number or words in the TEXT segment is
    /// not an even number (ie there is a key with no value)
    enforce_even: bool,

    /// If true, throw an error if we encounter a key with a blank value.
    /// Only relevant if [`no_delim_escape`] is also true.
    enforce_nonempty: bool,

    /// If true, throw an error if the parser encounters a bad UTF-8 byte when
    /// creating the key/value list. If false, merely drop the bad pair.
    error_on_invalid_utf8: bool,

    /// If true, throw error when encoutering keyword with non-ASCII characters
    enfore_keyword_ascii: bool,

    /// If true, throw error when total event width does not evenly divide
    /// the DATA segment. Meaningless for delimited ASCII data.
    enfore_data_width_divisibility: bool,

    /// If true, throw error if the total number of events as computed by
    /// dividing DATA segment length event width doesn't match $TOT. Does
    /// nothing if $TOT not given, which may be the case in version 2.0.
    enfore_matching_tot: bool,
    // TODO add keyword and value overrides, something like a list of patterns
    // that can be used to alter each keyword
    // TODO allow lambda function to be supplied which will alter the kv list
}

/// Instructions for reading the TEXT segment in a standardized structure.
pub struct StdTextReader {
    raw: RawTextReader,

    /// If true, all metadata standardization warnings will be considered fatal
    /// errors which will halt the parsing routine.
    warnings_are_errors: bool,

    /// If given, will be the $PnN used to identify the time channel. Means
    /// nothing for 2.0.
    ///
    /// Will be used for the [`ensure_time*`] options below. If not given, skip
    /// time channel checking entirely.
    time_shortname: Option<String>,

    /// If true, will ensure that time channel is present
    ensure_time: bool,

    /// If true, will ensure TIMESTEP is present if time channel is also
    /// present.
    ensure_time_timestep: bool,

    /// If true, will ensure PnE is 0,0 for time channel.
    ensure_time_linear: bool,

    /// If true, will ensure PnG is absent for time channel.
    ensure_time_nogain: bool,

    /// If true, throw an error if TEXT includes any keywords that start with
    /// "$" which are not standard.
    disallow_deviant: bool,

    /// If true, throw an error if TEXT includes any deprecated features
    disallow_deprecated: bool,

    /// If true, throw an error if TEXT includes any keywords that do not
    /// start with "$".
    disallow_nonstandard: bool,

    /// If supplied, will be used as an alternative pattern when parsing $DATE.
    ///
    /// It should have specifiers for year, month, and day as outlined in
    /// https://docs.rs/chrono/latest/chrono/format/strftime/index.html. If not
    /// supplied, $DATE will be parsed according to the standard pattern which
    /// is '%d-%b-%Y'.
    date_pattern: Option<String>,

    /// If supplied, this pattern will be used to group "nonstandard" keywords
    /// with matching measurements.
    ///
    /// Usually this will be something like '^P%n.+' where '%n' will be
    /// substituted with the measurement index before using it as a regular
    /// expression to match keywords. It should not start with a "$".
    ///
    /// This will matching something like 'P7FOO' which would be 'FOO' for
    /// measurement 7. This might be useful in the future when this code offers
    /// "upgrade" routines since these are often used to represent future
    /// keywords in an older version where the newer version cannot be used for
    /// some reason.
    nonstandard_measurement_pattern: Option<String>,
    // TODO add repair stuff
}

/// Instructions for reading the DATA segment.
pub struct DataReader {
    /// Will adjust the offset of the start of the TEXT segment by `offset + n`.
    datastart_delta: u32,
    /// Will adjust the offset of the end of the TEXT segment by `offset + n`.
    dataend_delta: u32,
}

/// Instructions for reading an FCS file.
pub struct Reader {
    text: StdTextReader,
    data: DataReader,
}

pub fn std_reader() -> Reader {
    Reader {
        text: StdTextReader {
            raw: RawTextReader {
                textstart_delta: 0,
                textend_delta: 0,
                warnings_are_errors: false,
                no_delim_escape: false,
                force_ascii_delim: false,
                enforce_final_delim: false,
                enforce_unique: false,
                enforce_even: false,
                enforce_nonempty: false,
                error_on_invalid_utf8: false,
                enfore_keyword_ascii: false,
                enfore_matching_tot: false,
                enfore_data_width_divisibility: false,
            },
            warnings_are_errors: false,
            ensure_time: false,
            ensure_time_timestep: false,
            ensure_time_linear: false,
            ensure_time_nogain: false,
            time_shortname: None,
            disallow_deprecated: false,
            disallow_nonstandard: false,
            disallow_deviant: false,
            date_pattern: None,
            nonstandard_measurement_pattern: None,
        },
        data: DataReader {
            datastart_delta: 0,
            dataend_delta: 0,
        },
    }
}

type FCSResult<T> = Result<FCSSuccess<T>, StandardErrors>;

/// Return header in an FCS file.
///
/// The header contains the version and offsets for the TEXT, DATA, and ANALYSIS
/// segments, all of which are present in fixed byte offset segments. This
/// function will fail and return an error if the file does not follow this
/// structure. Will also check that the begin and end segments are not reversed.
///
/// Depending on the version, all of these except the TEXT offsets might be 0
/// which indicates they are actually stored in TEXT due to size limitations.
pub fn read_fcs_header(p: &path::PathBuf) -> io::Result<Header> {
    let file = fs::File::options().read(true).open(p)?;
    let mut reader = BufReader::new(file);
    read_header(&mut reader)
}

/// Return header and raw key/value metadata pairs in an FCS file.
///
/// First will parse the header according to [`read_fcs_header`]. If this fails
/// an error will be returned.
///
/// Next will use the offset information in the header to parse the TEXT segment
/// for key/value pairs. On success will return these pairs as-is using Strings
/// in a HashMap. No other processing will be performed.
pub fn read_fcs_raw_text(p: &path::PathBuf, conf: &RawTextReader) -> io::Result<(Header, RawTEXT)> {
    let file = fs::File::options().read(true).open(p)?;
    let mut reader = BufReader::new(file);
    let header = read_header(&mut reader)?;
    let raw = read_raw_text(&mut reader, &header, conf)?;
    Ok((header, raw))
}

/// Return header and standardized metadata in an FCS file.
///
/// Begins by parsing header and raw keywords according to [`read_fcs_raw_text`]
/// and will return error if this function fails.
///
/// Next, all keywords in the TEXT segment will be validated to conform to the
/// FCS standard indicated in the header and returned in a struct storing each
/// key/value pair in a standardized manner. This will halt and return any
/// errors encountered during this process.
pub fn read_fcs_text(p: &path::PathBuf, conf: &StdTextReader) -> io::Result<TEXTResult> {
    let file = fs::File::options().read(true).open(p)?;
    let mut reader = BufReader::new(file);
    let header = read_header(&mut reader)?;
    let raw = read_raw_text(&mut reader, &header, &conf.raw)?;
    Ok(parse_raw_text(header, raw, conf))
}

// fn read_fcs_text_2_0(p: path::PathBuf, conf: StdTextReader) -> TEXTResult<TEXT2_0>;
// fn read_fcs_text_3_0(p: path::PathBuf, conf: StdTextReader) -> TEXTResult<TEXT3_0>;
// fn read_fcs_text_3_1(p: path::PathBuf, conf: StdTextReader) -> TEXTResult<TEXT3_1>;
// fn read_fcs_text_3_2(p: path::PathBuf, conf: StdTextReader) -> TEXTResult<TEXT3_2>;

/// Return header, structured metadata, and data in an FCS file.
///
/// Begins by parsing header and raw keywords according to [`read_fcs_text`]
/// and will return error if this function fails.
///
/// Next, the DATA segment will be parsed according to the metadata present
/// in TEXT.
///
/// On success will return all three of the above segments along with any
/// non-critical warnings.
///
/// The [`conf`] argument can be used to control the behavior of each reading
/// step, including the repair of non-conforming files.
pub fn read_fcs_file(p: path::PathBuf, conf: Reader) -> io::Result<FCSResult<AnyStdTEXT>> {
    let file = fs::File::options().read(true).open(p)?;
    let mut reader = BufReader::new(file);
    let header = read_header(&mut reader)?;
    let raw = read_raw_text(&mut reader, &header, &conf.text.raw)?;
    // TODO useless clone?
    match parse_raw_text(header, raw, &conf.text) {
        Ok(std) => {
            let data = read_data(&mut reader, std.data_parser).unwrap();
            Ok(Ok(FCSSuccess {
                std: std.standard,
                data,
            }))
        }
        Err(e) => Ok(Err(e)),
    }
}

// fn read_fcs_file_2_0(p: path::PathBuf, conf: Reader) -> FCSResult<TEXT2_0>;
// fn read_fcs_file_3_0(p: path::PathBuf, conf: Reader) -> FCSResult<TEXT3_0>;
// fn read_fcs_file_3_1(p: path::PathBuf, conf: Reader) -> FCSResult<TEXT3_1>;
// fn read_fcs_file_3_2(p: path::PathBuf, conf: Reader) -> FCSResult<TEXT3_2>;

/// Return header, raw metadata, and data in an FCS file.
///
/// In contrast to [`read_fcs_file`], this will return the keywords as a flat
/// list of key/value pairs. Only the bare minimum of these will be read in
/// order to determine how to parse the DATA segment (including $DATATYPE,
/// $BYTEORD, etc). No other checks will be performed to ensure the metadata
/// conforms to the FCS standard version indicated in the header.
///
/// This might be useful for applications where one does not necessarily need
/// the strict structure of the standardized metadata, or if one does not care
/// too much about the degree to which the metadata conforms to standard.
///
/// Other than this, behavior is identical to [`read_fcs_file`],
pub fn read_fcs_raw_file(p: path::PathBuf, conf: Reader) -> io::Result<FCSResult<()>> {
    let file = fs::File::options().read(true).open(p)?;
    let mut reader = BufReader::new(file);
    let header = read_header(&mut reader)?;
    let raw = read_raw_text(&mut reader, &header, &conf.text.raw)?;
    // TODO need to modify this so it doesn't do the crazy version checking
    // stuff we don't actually want in this case
    match parse_raw_text(header, raw.clone(), &conf.text) {
        Ok(std) => {
            let data = read_data(&mut reader, std.data_parser).unwrap();
            Ok(Ok(FCSSuccess { std: (), data }))
        }
        Err(e) => Ok(Err(e)),
    }
}

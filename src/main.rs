// TODO gating parameters not added (yet)

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
use itertools::Itertools;
use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::env;
use std::fmt;
use std::fs;
use std::io;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::iter;
use std::num::IntErrorKind;
use std::str;

fn format_standard_kw(kw: &str) -> String {
    format!("${}", kw.to_ascii_uppercase())
}

fn format_param(n: u32, param: &str) -> String {
    format!("P{}{}", n, param.to_ascii_uppercase())
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
        _ => s.parse().map_or(
            Err(String::from("must be a positive integer or '*'")),
            |x| {
                if x % 8 == 0 {
                    Ok(Bytes::Fixed(x))
                } else {
                    Err(String::from("only multiples of 8 are supported"))
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

#[derive(Debug)]
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

#[derive(Debug)]
struct Header {
    version: Version,
    text: Offsets,
    data: Offsets,
    analysis: Offsets,
}

#[derive(Debug, Clone, Copy)]
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

#[derive(Debug, Clone, Copy)]
enum Endian {
    Big,
    Little,
}

impl Endian {
    fn is_big(&self) -> bool {
        matches!(self, Endian::Big)
    }
}

#[derive(Debug, Clone)]
enum ByteOrd {
    Endian(Endian),
    Mixed(Vec<u8>),
}

impl ByteOrd {
    fn valid_byte_num(&self, n: u8) -> bool {
        match self {
            ByteOrd::Endian(_) => true,
            ByteOrd::Mixed(xs) => xs.len() == usize::from(n),
        }
    }

    fn to_float_byteord(&self, is_double: bool) -> Option<FloatByteOrd> {
        match self {
            ByteOrd::Endian(e) => {
                let f = if is_double {
                    FloatByteOrd::DoubleBigLittle
                } else {
                    FloatByteOrd::SingleBigLittle
                };
                Some(f(*e))
            }
            ByteOrd::Mixed(xs) => match xs[..] {
                [a, b, c, d] => {
                    if is_double {
                        None
                    } else {
                        Some(FloatByteOrd::SingleOrdered([a, b, c, d]))
                    }
                }
                [a, b, c, d, e, f, g, h] => {
                    if is_double {
                        Some(FloatByteOrd::DoubleOrdered([a, b, c, d, e, f, g, h]))
                    } else {
                        None
                    }
                }
                _ => None,
            },
        }
    }

    // This only makes sense for integer types
    fn num_bytes(&self) -> u8 {
        match self {
            ByteOrd::Endian(_) => 4,
            ByteOrd::Mixed(xs) => xs.len() as u8,
        }
    }
}

#[derive(Debug)]
struct Trigger {
    parameter: String,
    threshold: u32,
}

struct TextOffsets<A, D, T> {
    analysis: A,
    data: D,
    stext: T,
}

type TextOffsets3_0 = TextOffsets<Offsets, Offsets, Offsets>;
type TextOffsets3_2 = TextOffsets<Option<Offsets>, Offsets, Option<Offsets>>;

#[derive(Debug)]
struct Timestamps2_0 {
    btim: OptionalKw<NaiveTime>,
    etim: OptionalKw<NaiveTime>,
    date: OptionalKw<NaiveDate>,
}

#[derive(Debug)]
struct Timestamps3_2 {
    start: OptionalKw<DateTime<FixedOffset>>,
    end: OptionalKw<DateTime<FixedOffset>>,
}

// TODO this is super messy, see 3.2 spec for restrictions on this we may with
// to use further
#[derive(Debug)]
struct LogScale {
    decades: f32,
    offset: f32,
}

#[derive(Debug)]
enum Scale {
    Log(LogScale),
    Linear,
}

use Scale::*;

#[derive(Debug)]
struct LinDisplay {
    lower: f32,
    upper: f32,
}

#[derive(Debug)]
struct LogDisplay {
    offset: f32,
    decades: f32,
}

#[derive(Debug)]
enum Display {
    Lin(LinDisplay),
    Log(LogDisplay),
}

#[derive(Debug)]
struct Calibration {
    value: f32,
    unit: String,
}

#[derive(Debug)]
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
}

#[derive(Debug)]
enum Feature {
    Area,
    Width,
    Height,
}

#[derive(Debug)]
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

    fn to_option(self) -> Option<V> {
        match self {
            OptionalKw::Present(x) => Some(x),
            Absent => None,
        }
    }

    fn from_option(x: Option<V>) -> Self {
        x.map_or_else(|| Absent, |y| OptionalKw::Present(y))
    }
}

#[derive(Debug)]
struct InnerParameter2_0 {
    scale: OptionalKw<Scale>,      // PnE
    wavelength: OptionalKw<u32>,   // PnL
    shortname: OptionalKw<String>, // PnN
}

#[derive(Debug)]
struct InnerParameter3_0 {
    scale: Scale,                  // PnE
    wavelength: OptionalKw<u32>,   // PnL
    shortname: OptionalKw<String>, // PnN
    gain: OptionalKw<f32>,         // PnG
}

#[derive(Debug)]
struct InnerParameter3_1 {
    scale: Scale,          // PnE
    wavelength: Vec<u32>,  // PnL
    shortname: String,     // PnN
    gain: OptionalKw<f32>, // PnG
    calibration: OptionalKw<Calibration>,
    display: OptionalKw<Display>,
}

#[derive(Debug)]
struct InnerParameter3_2 {
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
impl InnerParameter3_2 {
    fn get_column_type(&self, default: AlphaNumType) -> AlphaNumType {
        self.datatype
            .as_ref()
            .to_option()
            .map(|x| x.lift())
            .unwrap_or(default)
    }
}

#[derive(Debug)]
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
    // for floating point parameters rather than an int.
    Float(f64),
}

#[derive(Debug)]
struct Parameter<X> {
    bytes: Bytes,                      // PnB
    range: Range,                      // PnR
    longname: OptionalKw<String>,      // PnS
    filter: OptionalKw<String>,        // PnF
    power: OptionalKw<u32>,            // PnO
    detector_type: OptionalKw<String>, // PnD
    percent_emitted: OptionalKw<u32>,  // PnP (TODO deprecated in 3.2, factor out)
    detector_voltage: OptionalKw<f32>, // PnV
    specific: X,
}

impl<X> Parameter<X> {
    fn bytes_eq(&self, b: u8) -> bool {
        match self.bytes {
            Bytes::Fixed(x) => x == b,
            _ => false,
        }
    }

    fn bytes_are_variable(&self) -> bool {
        matches!(self.bytes, Bytes::Variable)
    }

    // TODO add parameter index to error message
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
        3 => CanParseInt::<4, 3>::to_col_parser(r, o, t).map(AnyIntColumn::Uint24),
        4 => CanParseInt::<4, 4>::to_col_parser(r, o, t).map(AnyIntColumn::Uint32),
        5 => CanParseInt::<8, 5>::to_col_parser(r, o, t).map(AnyIntColumn::Uint40),
        6 => CanParseInt::<8, 6>::to_col_parser(r, o, t).map(AnyIntColumn::Uint48),
        7 => CanParseInt::<8, 7>::to_col_parser(r, o, t).map(AnyIntColumn::Uint56),
        8 => CanParseInt::<8, 8>::to_col_parser(r, o, t).map(AnyIntColumn::Uint64),
        _ => Err(vec![String::from("PnB has invalid byte length")]),
    }
}

// // type Wavelength2_0 = Option<u32>;
// // type Wavelength3_1 = Vec<u32>;

trait ParameterFromKeywords: Sized {
    fn build_inner(st: &mut KwState, n: u32) -> Option<Self>;

    fn parameter_name(p: &Parameter<Self>) -> Option<&str>;

    fn from_kws(st: &mut KwState, par: u32) -> Option<Vec<Parameter<Self>>> {
        let mut ps = vec![];
        for n in 1..(par + 1) {
            if let (Some(bytes), Some(range), Some(specific)) = (
                st.lookup_param_bytes(n),
                st.lookup_param_range(n),
                Self::build_inner(st, n),
            ) {
                let p = Parameter {
                    bytes,
                    range,
                    longname: st.lookup_param_longname(n),
                    filter: st.lookup_param_filter(n),
                    power: st.lookup_param_power(n),
                    detector_type: st.lookup_param_detector_type(n),
                    percent_emitted: st.lookup_param_percent_emitted(n),
                    detector_voltage: st.lookup_param_detector_voltage(n),
                    specific,
                };
                ps.push(p);
            }
        }
        if usize::try_from(par).map(|p| ps.len() < p).unwrap_or(false) {
            None
        } else {
            Some(ps)
        }
    }
}

type Parameter2_0 = Parameter<InnerParameter2_0>;
type Parameter3_0 = Parameter<InnerParameter3_0>;
type Parameter3_1 = Parameter<InnerParameter3_1>;
type Parameter3_2 = Parameter<InnerParameter3_2>;

impl ParameterFromKeywords for InnerParameter2_0 {
    fn parameter_name(p: &Parameter<Self>) -> Option<&str> {
        p.specific
            .shortname
            .as_ref()
            .to_option()
            .map(|s| s.as_str())
    }

    fn build_inner(st: &mut KwState, n: u32) -> Option<InnerParameter2_0> {
        Some(InnerParameter2_0 {
            scale: st.lookup_param_scale_opt(n),
            shortname: st.lookup_param_shortname_opt(n),
            wavelength: st.lookup_param_wavelength(n),
        })
    }
}

impl ParameterFromKeywords for InnerParameter3_0 {
    fn parameter_name(p: &Parameter<Self>) -> Option<&str> {
        p.specific
            .shortname
            .as_ref()
            .to_option()
            .map(|s| s.as_str())
    }

    fn build_inner(st: &mut KwState, n: u32) -> Option<InnerParameter3_0> {
        st.lookup_param_scale_req(n).map(|scale| InnerParameter3_0 {
            scale,
            shortname: st.lookup_param_shortname_opt(n),
            wavelength: st.lookup_param_wavelength(n),
            gain: st.lookup_param_gain(n),
        })
    }
}

impl ParameterFromKeywords for InnerParameter3_1 {
    fn parameter_name(p: &Parameter<Self>) -> Option<&str> {
        Some(p.specific.shortname.as_str())
    }

    fn build_inner(st: &mut KwState, n: u32) -> Option<InnerParameter3_1> {
        if let (Some(scale), Some(shortname)) = (
            st.lookup_param_scale_req(n),
            st.lookup_param_shortname_req(n),
        ) {
            Some(InnerParameter3_1 {
                scale,
                shortname,
                wavelength: st.lookup_param_wavelengths(n),
                gain: st.lookup_param_gain(n),
                calibration: st.lookup_param_calibration(n),
                display: st.lookup_param_display(n),
            })
        } else {
            None
        }
    }
}

impl ParameterFromKeywords for InnerParameter3_2 {
    fn parameter_name(p: &Parameter<Self>) -> Option<&str> {
        Some(p.specific.shortname.as_str())
    }

    fn build_inner(st: &mut KwState, n: u32) -> Option<InnerParameter3_2> {
        if let (Some(scale), Some(shortname)) = (
            st.lookup_param_scale_req(n),
            st.lookup_param_shortname_req(n),
        ) {
            Some(InnerParameter3_2 {
                scale,
                shortname,
                wavelength: st.lookup_param_wavelengths(n),
                gain: st.lookup_param_gain(n),
                detector_name: st.lookup_param_detector(n),
                tag: st.lookup_param_tag(n),
                measurement_type: st.lookup_param_type(n),
                feature: st.lookup_param_feature(n),
                analyte: st.lookup_param_analyte(n),
                calibration: st.lookup_param_calibration(n),
                display: st.lookup_param_display(n),
                datatype: st.lookup_param_datatype(n),
            })
        } else {
            None
        }
    }
}

#[derive(Debug)]
enum Originality {
    Original,
    NonDataModified,
    Appended,
    DataModified,
}

#[derive(Debug)]
struct ModificationData {
    last_modifier: OptionalKw<String>,
    last_modified: OptionalKw<NaiveDateTime>,
    originality: OptionalKw<Originality>,
}

#[derive(Debug)]
struct PlateData {
    plateid: OptionalKw<String>,
    platename: OptionalKw<String>,
    wellid: OptionalKw<String>,
}

type UnstainedCenters = HashMap<String, f32>;

#[derive(Debug)]
struct UnstainedData {
    unstainedcenters: OptionalKw<UnstainedCenters>,
    unstainedinfo: OptionalKw<String>,
}

#[derive(Debug)]
struct CarrierData {
    carrierid: OptionalKw<String>,
    carriertype: OptionalKw<String>,
    locationid: OptionalKw<String>,
}

#[derive(Debug)]
struct Unicode {
    page: u32,
    kws: Vec<String>,
}

#[derive(Debug)]
struct SupplementalOffsets3_0 {
    analysis: Offsets,
    stext: Offsets,
}

#[derive(Debug)]
struct SupplementalOffsets3_2 {
    analysis: OptionalKw<Offsets>,
    stext: OptionalKw<Offsets>,
}

#[derive(Debug)]
struct InnerMetadata2_0 {
    tot: OptionalKw<u32>,
    mode: Mode,
    byteord: ByteOrd,
    cyt: OptionalKw<String>,
    timestamps: Timestamps2_0, // BTIM/ETIM/DATE
}

#[derive(Debug)]
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

#[derive(Debug)]
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

#[derive(Debug)]
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

#[derive(Debug)]
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

struct Spillover {} // TODO, can probably get away with using a matrix for this

struct Cyt(String);

struct Tot(u32);

#[derive(Debug)]
enum Mode {
    List,
    Uncorrelated,
    Correlated,
}

#[derive(Debug)]
struct StdText<M, P> {
    header: Header,
    metadata: Metadata<M>,
    parameters: Vec<Parameter<P>>,
}

#[derive(Debug)]
struct TEXTwithParser<T> {
    text: T,
    data_parser: DataParser,
}

impl<X> TEXTwithParser<X> {
    fn map<Y, F: Fn(X) -> Y>(self, f: F) -> TEXTwithParser<Y> {
        TEXTwithParser {
            text: f(self.text),
            data_parser: self.data_parser,
        }
    }
}

type TEXTResult<T> = Result<TEXTwithParser<T>, StandardErrors>;

impl<M: MetadataFromKeywords> StdText<M, M::P> {
    fn from_kws(header: Header, raw: RawTEXT) -> TEXTResult<TEXT<Self>> {
        let mut st = raw.to_state();
        // This will fail if a) not all required keywords pass and b) not all
        // required parameter keywords pass (according to $PAR)
        if let Some(s) = M::from_kws(&mut st).and_then(|metadata| {
            M::P::from_kws(&mut st, metadata.par).map(|parameters| StdText {
                header,
                metadata,
                parameters,
            })
        }) {
            M::validate(st, s)
        } else {
            Err(st.compile_errors())
        }
    }
}

type StdText2_0 = StdText<InnerMetadata2_0, InnerParameter2_0>;
type StdText3_0 = StdText<InnerMetadata3_0, InnerParameter3_0>;
type StdText3_1 = StdText<InnerMetadata3_1, InnerParameter3_1>;
type StdText3_2 = StdText<InnerMetadata3_2, InnerParameter3_2>;

struct StdTextResult<T> {
    text: T,
    errors: Keywords,
    nonstandard: Keywords,
}

enum FloatByteOrd {
    SingleBigLittle(Endian),
    DoubleBigLittle(Endian),
    SingleOrdered([u8; 4]),
    DoubleOrdered([u8; 8]),
}

impl FloatByteOrd {
    fn is_double(&self) -> bool {
        match self {
            FloatByteOrd::SingleBigLittle(_) => false,
            FloatByteOrd::DoubleBigLittle(_) => true,
            FloatByteOrd::SingleOrdered(_) => false,
            FloatByteOrd::DoubleOrdered(_) => true,
        }
    }

    fn width(&self) -> u8 {
        if self.is_double() {
            8
        } else {
            4
        }
    }
}

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

enum F64DataType {
    Ascii(u8),
    Double,
}

struct F64ColumnData {
    data: Vec<f64>,
    datatype: F64DataType,
}

enum MixedColumnData {
    F32(Vec<f32>),
    F64(F64ColumnData),
    Uint(AnyIntColumn),
}

trait IntMath: Sized {
    fn next_power_2(x: Self) -> Self;

    fn from_u64(x: u64) -> Self;
}

trait NumProps<const DTLEN: usize>: Sized + Copy {
    fn to_series(x: Vec<Self>) -> Series;

    fn zero() -> Self;

    fn from_big(buf: [u8; DTLEN]) -> Self;

    fn from_little(buf: [u8; DTLEN]) -> Self;

    fn read_from_big<R: Read + Seek>(h: &mut BufReader<R>) -> io::Result<Self> {
        let mut buf = [0; DTLEN];
        h.read_exact(&mut buf)?;
        Ok(Self::from_big(buf))
    }

    fn read_from_little<R: Read + Seek>(h: &mut BufReader<R>) -> io::Result<Self> {
        let mut buf = [0; DTLEN];
        h.read_exact(&mut buf)?;
        Ok(Self::from_little(buf))
    }

    fn read_from_endian<R: Read + Seek>(h: &mut BufReader<R>, endian: Endian) -> io::Result<Self> {
        if endian.is_big() {
            Self::read_from_big(h)
        } else {
            Self::read_from_little(h)
        }
    }
}

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

trait CanParseInt<const DTLEN: usize, const INTLEN: usize>:
    Sized + IntMath + NumProps<DTLEN> + Clone
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
        // TODO be more specific, which means we need the parameter index
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
}

trait IntFromBytes<const DTLEN: usize, const INTLEN: usize>:
    NumProps<DTLEN> + OrderedFromBytes<DTLEN, INTLEN> + Ord + CanParseInt<DTLEN, INTLEN>
{
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
            ncols: par as usize,
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
        Ok(columns
            .into_iter()
            .into_iter()
            .map(Self::to_series)
            .collect())
    }
}

impl NumProps<1> for u8 {
    fn to_series(x: Vec<Self>) -> Series {
        Series::U8(x)
    }

    fn zero() -> Self {
        0
    }

    fn from_big(buf: [u8; 1]) -> Self {
        Self::from_be_bytes(buf)
    }

    fn from_little(buf: [u8; 1]) -> Self {
        Self::from_le_bytes(buf)
    }
}

impl NumProps<2> for u16 {
    fn to_series(x: Vec<Self>) -> Series {
        Series::U16(x)
    }

    fn zero() -> Self {
        0
    }

    fn from_big(buf: [u8; 2]) -> Self {
        u16::from_be_bytes(buf)
    }

    fn from_little(buf: [u8; 2]) -> Self {
        u16::from_le_bytes(buf)
    }
}

impl NumProps<4> for u32 {
    fn to_series(x: Vec<Self>) -> Series {
        Series::U32(x)
    }

    fn zero() -> Self {
        0
    }

    fn from_big(buf: [u8; 4]) -> Self {
        u32::from_be_bytes(buf)
    }

    fn from_little(buf: [u8; 4]) -> Self {
        u32::from_le_bytes(buf)
    }
}

impl NumProps<8> for u64 {
    fn to_series(x: Vec<Self>) -> Series {
        Series::U64(x)
    }

    fn zero() -> Self {
        0
    }

    fn from_big(buf: [u8; 8]) -> Self {
        u64::from_be_bytes(buf)
    }

    fn from_little(buf: [u8; 8]) -> Self {
        u64::from_le_bytes(buf)
    }
}

impl NumProps<4> for f32 {
    fn to_series(x: Vec<Self>) -> Series {
        Series::F32(x)
    }

    fn zero() -> Self {
        0.0
    }

    fn from_big(buf: [u8; 4]) -> Self {
        f32::from_be_bytes(buf)
    }

    fn from_little(buf: [u8; 4]) -> Self {
        f32::from_le_bytes(buf)
    }
}

impl NumProps<8> for f64 {
    fn to_series(x: Vec<Self>) -> Series {
        Series::F64(x)
    }

    fn zero() -> Self {
        0.0
    }

    fn from_big(buf: [u8; 8]) -> Self {
        f64::from_be_bytes(buf)
    }

    fn from_little(buf: [u8; 8]) -> Self {
        f64::from_le_bytes(buf)
    }
}

impl IntMath for u8 {
    fn next_power_2(x: Self) -> Self {
        Self::checked_next_power_of_two(x).unwrap_or(Self::MAX)
    }

    fn from_u64(x: u64) -> Self {
        x as Self
    }
}

impl IntMath for u16 {
    fn next_power_2(x: Self) -> Self {
        Self::checked_next_power_of_two(x).unwrap_or(Self::MAX)
    }

    fn from_u64(x: u64) -> Self {
        x as Self
    }
}

impl IntMath for u32 {
    fn next_power_2(x: Self) -> Self {
        Self::checked_next_power_of_two(x).unwrap_or(Self::MAX)
    }

    fn from_u64(x: u64) -> Self {
        x as Self
    }
}

impl IntMath for u64 {
    fn next_power_2(x: Self) -> Self {
        Self::checked_next_power_of_two(x).unwrap_or(Self::MAX)
    }

    fn from_u64(x: u64) -> Self {
        x as Self
    }
}

impl OrderedFromBytes<1, 1> for u8 {}
impl IntFromBytes<1, 1> for u8 {}

impl OrderedFromBytes<2, 2> for u16 {}
impl IntFromBytes<2, 2> for u16 {}

impl OrderedFromBytes<4, 3> for u32 {}
impl IntFromBytes<4, 3> for u32 {}
impl OrderedFromBytes<4, 4> for u32 {}
impl IntFromBytes<4, 4> for u32 {}

impl OrderedFromBytes<8, 5> for u64 {}
impl IntFromBytes<8, 5> for u64 {}
impl OrderedFromBytes<8, 6> for u64 {}
impl IntFromBytes<8, 6> for u64 {}
impl OrderedFromBytes<8, 7> for u64 {}
impl IntFromBytes<8, 7> for u64 {}
impl OrderedFromBytes<8, 8> for u64 {}
impl IntFromBytes<8, 8> for u64 {}

impl CanParseInt<1, 1> for u8 {}
impl CanParseInt<2, 2> for u16 {}
impl CanParseInt<4, 3> for u32 {}
impl CanParseInt<4, 4> for u32 {}
impl CanParseInt<8, 5> for u64 {}
impl CanParseInt<8, 6> for u64 {}
impl CanParseInt<8, 7> for u64 {}
impl CanParseInt<8, 8> for u64 {}

impl FloatFromBytes<4> for f32 {}
impl FloatFromBytes<8> for f64 {}

impl OrderedFromBytes<4, 4> for f32 {}

impl OrderedFromBytes<8, 8> for f64 {}

impl MixedColumnType {
    fn to_series(self) -> Series {
        match self {
            MixedColumnType::Ascii(x) => f64::to_series(x.data),
            MixedColumnType::Single(x) => f32::to_series(x.data),
            MixedColumnType::Double(x) => f64::to_series(x.data),
            MixedColumnType::Uint(x) => x.to_series(),
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
    fn to_series(self) -> Series {
        match self {
            AnyIntColumn::Uint8(y) => u8::to_series(y.data),
            AnyIntColumn::Uint16(y) => u16::to_series(y.data),
            AnyIntColumn::Uint24(y) => u32::to_series(y.data),
            AnyIntColumn::Uint32(y) => u32::to_series(y.data),
            AnyIntColumn::Uint40(y) => u64::to_series(y.data),
            AnyIntColumn::Uint48(y) => u64::to_series(y.data),
            AnyIntColumn::Uint56(y) => u64::to_series(y.data),
            AnyIntColumn::Uint64(y) => u64::to_series(y.data),
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

enum Series {
    F32(Vec<f32>),
    F64(Vec<f64>),
    U8(Vec<u8>),
    U16(Vec<u16>),
    U32(Vec<u32>),
    U64(Vec<u64>),
}

type ParsedData = Vec<Series>;

fn ascii_to_float_io(buf: Vec<u8>) -> io::Result<f64> {
    String::from_utf8(buf)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
        .and_then(|s| parse_f64_io(&s))
}

fn parse_f64_io(s: &String) -> io::Result<f64> {
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
        Ok(data.into_iter().map(|c| f64::to_series(c)).collect())
    } else {
        let mut data: Vec<_> = iter::repeat_with(|| vec![]).take(p.ncols).collect();
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
        Ok(data.into_iter().map(|c| f64::to_series(c)).collect())
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
    Ok(data.into_iter().map(f64::to_series).collect())
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
    Ok(p.columns.into_iter().map(|c| c.to_series()).collect())
}

fn read_data_int<R: Read>(h: &mut BufReader<R>, parser: IntParser) -> io::Result<ParsedData> {
    let mut p = parser;
    for r in 0..p.nrows {
        for c in p.columns.iter_mut() {
            c.assign(h, r)?;
        }
    }
    Ok(p.columns.into_iter().map(|c| c.to_series()).collect())
}

fn read_data<R: Read + Seek>(h: &mut BufReader<R>, parser: DataParser) -> io::Result<ParsedData> {
    h.seek(SeekFrom::Start(u64::from(parser.begin)))?;
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

trait MetadataFromKeywords: Sized {
    type P: ParameterFromKeywords;

    fn get_data_offsets(s: &StdText<Self, Self::P>) -> Offsets;

    fn get_byteord(&self) -> ByteOrd;

    fn get_tot(&self) -> Option<u32>;

    fn get_event_width(s: &StdText<Self, Self::P>) -> EventWidth {
        let (fixed, variable_indices): (Vec<_>, Vec<_>) = s
            .parameters
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

    // TODO this may or may not agree with what is in the TOT field. The offsets
    // themselves are sometimes known to be screwed up. To make this more
    // complex, the TOT field is optional in 2.0. It might make sense to offer
    // various fallback/failure mechanisms in case this issue is encountered.
    fn get_total_events(s: &StdText<Self, Self::P>, event_width: u32) -> Result<usize, String> {
        let nbytes = Self::get_data_offsets(s).num_bytes();
        let remainder = nbytes % event_width;
        if nbytes % event_width > 0 {
            Err(format!(
                "Events are {event_width} bytes wide but does not evenly \
                 divide DATA segment which is {nbytes} bytes long \
                 (remainder of {remainder}) "
            ))
        } else {
            usize::try_from(nbytes / event_width).map_err(|e| format!("{}", e))
        }
    }

    fn build_int_parser(
        &self,
        ps: &[Parameter<Self::P>],
        total_events: usize,
    ) -> Result<IntParser, Vec<String>>;

    fn build_mixed_parser(
        &self,
        ps: &[Parameter<Self::P>],
        dt: &AlphaNumType,
        total_events: usize,
    ) -> Option<Result<MixedParser, Vec<String>>>;

    fn build_float_parser(
        &self,
        is_double: bool,
        par: usize,
        total_events: usize,
        ps: &[Parameter<Self::P>],
    ) -> Result<ColumnParser, Vec<String>> {
        let (bytes, dt) = if is_double { (8, "D") } else { (4, "F") };
        let remainder: Vec<_> = ps.iter().filter(|p| p.bytes_eq(bytes)).collect();
        if remainder.is_empty() {
            let byteord = &self.get_byteord();
            let res = if is_double {
                f64::make_matrix_parser(byteord, par, total_events).map(ColumnParser::Double)
            } else {
                f32::make_matrix_parser(byteord, par, total_events).map(ColumnParser::Single)
            };
            res.map_err(|e| vec![e])
        } else {
            Err(remainder
                .iter()
                .enumerate()
                .map(|(i, p)| format!("Parameter {} uses {} bytes but DATATYPE={}", i, p.bytes, dt))
                .collect())
        }
    }

    fn build_fixed_width_parser(
        s: &StdText<Self, Self::P>,
        total_events: usize,
        parameter_widths: Vec<u8>,
    ) -> Result<ColumnParser, Vec<String>> {
        // TODO fix cast?
        let par = s.metadata.par as usize;
        let ps = &s.parameters;
        let dt = &s.metadata.datatype;
        let specific = &s.metadata.specific;
        if let Some(mixed) = Self::build_mixed_parser(specific, ps, dt, total_events) {
            mixed.map(ColumnParser::Mixed)
        } else {
            match dt {
                AlphaNumType::Single => specific.build_float_parser(false, par, total_events, ps),
                AlphaNumType::Double => specific.build_float_parser(true, par, total_events, ps),
                AlphaNumType::Integer => {
                    Self::build_int_parser(specific, ps, total_events).map(ColumnParser::Int)
                }
                AlphaNumType::Ascii => Ok(ColumnParser::FixedWidthAscii(FixedAsciiParser {
                    columns: parameter_widths,
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

    fn build_column_parser(s: &StdText<Self, Self::P>) -> Result<ColumnParser, Vec<String>> {
        // In order to make a data parser, the $DATATYPE, $BYTEORD, $PnB, and
        // $PnDATATYPE (if present) all need to be a specific relationship to
        // each other, each of which corresponds to the options below.
        match (Self::get_event_width(s), s.metadata.datatype) {
            // Numeric/Ascii (fixed width)
            (EventWidth::Finite(parameter_widths), _) => {
                let event_width = parameter_widths.iter().map(|x| u32::from(*x)).sum();
                Self::get_total_events(s, event_width)
                    .map_err(|e| vec![e])
                    .and_then(|total_events| {
                        Self::build_fixed_width_parser(s, total_events, parameter_widths)
                    })
            }
            // Ascii (variable width)
            (EventWidth::Variable, AlphaNumType::Ascii) => {
                let tot = s.metadata.specific.get_tot();
                Ok(Self::build_delim_ascii_parser(s, tot.map(|x| x as usize)))
            }
            // nonsense...scream at user
            (EventWidth::Error(fixed, variable), _) => {
                let mut errors = vec![];
                errors.push(String::from("$PnBs are a mix of numeric and variable"));
                for f in fixed {
                    errors.push(format!("$PnB for parameter {f} is numeric"));
                }
                for v in variable {
                    errors.push(format!("$PnB for parameter {v} is variable"));
                }
                Err(errors)
            }
            (EventWidth::Variable, dt) => {
                Err(vec![format!("$DATATYPE is {dt} but all $PnB are '*'")])
            }
        }
    }

    fn build_data_parser(s: &StdText<Self, Self::P>) -> Result<DataParser, Vec<String>> {
        Self::build_column_parser(s).map(|column_parser| DataParser {
            column_parser,
            begin: u64::from(Self::get_data_offsets(s).begin),
        })
    }

    fn validate_specific(st: &mut KwState, s: &StdText<Self, Self::P>, names: HashSet<&str>);

    // TODO I may want to be less strict with some of these, Time channel for
    // instance is something some files screw up by either naming the channel
    // something weird or not including TIMESTEP
    fn validate(
        st: KwState,
        s: StdText<Self, Self::P>,
    ) -> TEXTResult<TEXT<StdText<Self, Self::P>>> {
        let mut st = st;
        let shortnames: HashSet<&str> = s
            .parameters
            .iter()
            .filter_map(|p| Self::P::parameter_name(p))
            .collect();

        // TODO validate ranges. In the case of DATATYPE = I, $PnR should be the
        // "max" value. In some cases this is set to some absurdly high number
        // like 2^128 even though PnB is only 32. If I just trust that PnB is
        // correct, then the integer bitmask is totally filled and all u32 ints
        // are valid.

        // TODO validate time channel

        // validate $TRIGGER with parameter names
        if let OptionalKw::Present(tr) = &s.metadata.tr {
            if !shortnames.contains(&tr.parameter.as_str()) {
                st.push_meta_error(format!(
                    "Trigger parameter '{}' is not in parameter set",
                    tr.parameter
                ));
            }
        }

        Self::validate_specific(&mut st, &s, shortnames);

        match Self::build_data_parser(&s) {
            Ok(data_parser) => {
                let mut nonstandard = HashMap::new();
                let mut deviant = HashMap::new();
                for (k, v) in st.keywords {
                    if let ValueStatus::Raw = v.status {
                        if k.starts_with("$") {
                            deviant.insert(k, v.value);
                        } else {
                            nonstandard.insert(k, v.value);
                        }
                    }
                }
                let text = TEXT {
                    standard: s,
                    nonstandard,
                    deviant,
                };
                return Ok(TEXTwithParser { text, data_parser });
            }
            Err(mut errs) => {
                st.meta_errors.append(&mut errs);
            }
        }
        Err(st.compile_errors())
    }

    fn build_inner(st: &mut KwState) -> Option<Self>;

    fn from_kws(st: &mut KwState) -> Option<Metadata<Self>> {
        if let (Some(par), Some(nextdata), Some(datatype), Some(specific)) = (
            st.lookup_par(),
            st.lookup_nextdata(),
            st.lookup_datatype(),
            Self::build_inner(st),
        ) {
            Some(Metadata {
                par,
                nextdata,
                datatype,
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
                specific,
            })
        } else {
            None
        }
    }
}

fn build_int_parser_2_0<X>(
    byteord: &ByteOrd,
    ps: &[Parameter<X>],
    total_events: usize,
) -> Result<IntParser, Vec<String>> {
    let nbytes = byteord.num_bytes();
    let remainder: Vec<_> = ps.iter().filter(|p| !p.bytes_eq(nbytes)).collect();
    if remainder.is_empty() {
        let (columns, fail): (Vec<_>, Vec<_>) = ps
            .into_iter()
            .map(|p| p.make_int_parser(&byteord, total_events))
            .partition_result();
        let errors: Vec<_> = fail.into_iter().flatten().collect();
        if errors.is_empty() {
            Ok(IntParser {
                columns,
                nrows: total_events,
            })
        } else {
            Err(errors)
        }
        // make_int_column_parser(nbytes, &rs, &self.byteord, total_events).map(IntParser::Fixed)
    } else {
        Err(remainder
            .iter()
            .enumerate()
            .map(|(i, p)| {
                format!(
                    "Parameter {} uses {} bytes when DATATYPE=I \
                         and BYTEORD implies {} bytes",
                    i, p.bytes, nbytes
                )
            })
            .collect())
    }
}

impl MetadataFromKeywords for InnerMetadata2_0 {
    type P = InnerParameter2_0;

    fn get_data_offsets(s: &StdText<Self, Self::P>) -> Offsets {
        s.header.data
    }

    fn get_tot(&self) -> Option<u32> {
        self.tot.as_ref().to_option().map(|x| *x)
    }

    fn get_byteord(&self) -> ByteOrd {
        self.byteord.clone()
    }

    fn build_int_parser(
        &self,
        ps: &[Parameter<Self::P>],
        total_events: usize,
    ) -> Result<IntParser, Vec<String>> {
        build_int_parser_2_0(&self.byteord, ps, total_events)
    }

    fn build_mixed_parser(
        &self,
        _: &[Parameter<Self::P>],
        _: &AlphaNumType,
        _: usize,
    ) -> Option<Result<MixedParser, Vec<String>>> {
        None
    }

    fn validate_specific(st: &mut KwState, s: &StdText<Self, Self::P>, names: HashSet<&str>) {}

    fn build_inner(st: &mut KwState) -> Option<InnerMetadata2_0> {
        if let (Some(mode), Some(byteord)) = (st.lookup_mode(), st.lookup_byteord()) {
            Some(InnerMetadata2_0 {
                tot: st.lookup_tot_opt(),
                mode,
                byteord,
                cyt: st.lookup_cyt_opt(),
                timestamps: st.lookup_timestamps2_0(false),
            })
        } else {
            None
        }
    }
}

impl MetadataFromKeywords for InnerMetadata3_0 {
    type P = InnerParameter3_0;

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

    fn build_int_parser(
        &self,
        ps: &[Parameter<Self::P>],
        total_events: usize,
    ) -> Result<IntParser, Vec<String>> {
        build_int_parser_2_0(&self.byteord, ps, total_events)
    }

    fn build_mixed_parser(
        &self,
        _: &[Parameter<Self::P>],
        _: &AlphaNumType,
        _: usize,
    ) -> Option<Result<MixedParser, Vec<String>>> {
        None
    }

    fn validate_specific(st: &mut KwState, s: &StdText<Self, Self::P>, names: HashSet<&str>) {}

    fn build_inner(st: &mut KwState) -> Option<InnerMetadata3_0> {
        if let (Some(data), Some(supplemental), Some(tot), Some(mode), Some(byteord)) = (
            st.lookup_data_offsets(),
            st.lookup_supplemental3_0(),
            st.lookup_tot_req(),
            st.lookup_mode(),
            st.lookup_byteord(),
        ) {
            Some(InnerMetadata3_0 {
                data,
                supplemental,
                tot,
                mode,
                byteord,
                cyt: st.lookup_cyt_opt(),
                timestamps: st.lookup_timestamps2_0(false),
                cytsn: st.lookup_cytsn(),
                timestep: st.lookup_timestep(),
                unicode: st.lookup_unicode(),
            })
        } else {
            None
        }
    }
}

impl MetadataFromKeywords for InnerMetadata3_1 {
    type P = InnerParameter3_1;

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

    fn build_int_parser(
        &self,
        ps: &[Parameter<Self::P>],
        total_events: usize,
    ) -> Result<IntParser, Vec<String>> {
        build_int_parser_2_0(&ByteOrd::Endian(self.byteord), ps, total_events)
    }

    fn build_mixed_parser(
        &self,
        _: &[Parameter<Self::P>],
        _: &AlphaNumType,
        _: usize,
    ) -> Option<Result<MixedParser, Vec<String>>> {
        None
    }

    fn validate_specific(st: &mut KwState, s: &StdText<Self, Self::P>, names: HashSet<&str>) {}

    fn build_inner(st: &mut KwState) -> Option<InnerMetadata3_1> {
        if let (Some(data), Some(supplemental), Some(tot), Some(mode), Some(byteord)) = (
            st.lookup_data_offsets(),
            st.lookup_supplemental3_0(),
            st.lookup_tot_req(),
            st.lookup_mode(),
            st.lookup_endian(),
        ) {
            Some(InnerMetadata3_1 {
                data,
                supplemental,
                tot,
                mode,
                byteord,
                cyt: st.lookup_cyt_opt(),
                timestamps: st.lookup_timestamps2_0(true),
                cytsn: st.lookup_cytsn(),
                timestep: st.lookup_timestep(),
                modification: st.lookup_modification(),
                plate: st.lookup_plate(),
                vol: st.lookup_vol(),
            })
        } else {
            None
        }
    }
}

impl MetadataFromKeywords for InnerMetadata3_2 {
    type P = InnerParameter3_2;

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

    fn build_int_parser(
        &self,
        ps: &[Parameter<Self::P>],
        total_events: usize,
    ) -> Result<IntParser, Vec<String>> {
        build_int_parser_2_0(&ByteOrd::Endian(self.byteord), ps, total_events)
    }

    fn build_mixed_parser(
        &self,
        ps: &[Parameter<Self::P>],
        dt: &AlphaNumType,
        total_events: usize,
    ) -> Option<Result<MixedParser, Vec<String>>> {
        let endian = self.byteord;
        // first test if we have any PnDATATYPEs defined, if no then skip this
        // data parser entirely
        if ps
            .iter()
            .filter(|p| p.specific.datatype.as_ref().to_option().is_some())
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
                    p.specific.datatype.as_ref().to_option().is_some(),
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
                            "{}={} but PnB={} for parameter {}",
                            sdt, dt, bytes, i
                        )])
                    }
                }
            })
            .partition_result();
        if fail.is_empty() {
            Some(Ok(MixedParser {
                nrows: total_events,
                columns: pass,
            }))
        } else {
            Some(Err(fail.into_iter().flatten().collect()))
        }
    }

    fn validate_specific(st: &mut KwState, s: &StdText<Self, Self::P>, names: HashSet<&str>) {
        // check that all names in $UNSTAINEDCENTERS match one of the channels
        if let OptionalKw::Present(centers) = &s.metadata.specific.unstained.unstainedcenters {
            for u in centers.keys() {
                if !names.contains(u.as_str()) {
                    st.push_meta_error(format!(
                        "Unstained center named {u} is not in parameter set",
                    ));
                }
            }
        }
    }

    fn build_inner(st: &mut KwState) -> Option<InnerMetadata3_2> {
        if let (Some(data), Some(tot), Some(_), Some(byteord), Some(cyt)) = (
            st.lookup_data_offsets(),
            st.lookup_tot_req(),
            // only L is allowed as of 3.2, pull the value so it is marked as
            // read and check that its value is valid
            st.lookup_mode3_2(),
            st.lookup_endian(),
            st.lookup_cyt_req(),
        ) {
            Some(InnerMetadata3_2 {
                data,
                supplemental: st.lookup_supplemental3_2(),
                tot,
                byteord,
                cyt,
                timestamps: st.lookup_timestamps2_0(true),
                cytsn: st.lookup_cytsn(),
                timestep: st.lookup_timestep(),
                modification: st.lookup_modification(),
                plate: st.lookup_plate(),
                vol: st.lookup_vol(),
                carrier: st.lookup_carrier(),
                datetimes: st.lookup_timestamps3_2(),
                unstained: st.lookup_unstained(),
                flowrate: st.lookup_flowrate(),
            })
        } else {
            None
        }
    }
}

#[derive(Debug)]
struct TEXT<S> {
    // TODO add the offsets here as well? offsets are needed before parsing
    // everything else
    standard: S,
    nonstandard: HashMap<String, String>,
    deviant: HashMap<String, String>,
}

#[derive(Debug)]
enum AnyTEXT {
    TEXT2_0(TEXT<StdText2_0>),
    TEXT3_0(TEXT<StdText3_0>),
    TEXT3_1(TEXT<StdText3_1>),
    TEXT3_2(TEXT<StdText3_2>),
}

impl AnyTEXT {
    fn from_kws(header: Header, raw: RawTEXT) -> Result<TEXTwithParser<Self>, StandardErrors> {
        match header.version {
            Version::FCS2_0 => StdText::from_kws(header, raw).map(|x| x.map(AnyTEXT::TEXT2_0)),
            Version::FCS3_0 => StdText::from_kws(header, raw).map(|x| x.map(AnyTEXT::TEXT3_0)),
            Version::FCS3_1 => StdText::from_kws(header, raw).map(|x| x.map(AnyTEXT::TEXT3_1)),
            Version::FCS3_2 => StdText::from_kws(header, raw).map(|x| x.map(AnyTEXT::TEXT3_2)),
        }
    }
}

type Keywords = HashMap<String, String>;
type KeywordErrors = HashMap<String, (String, String)>;
type MissingKeywords = HashSet<String>;

#[derive(Debug)]
struct KwError {
    value: String,
    msg: String,
}

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

// all hail the almighty state monad :D

struct KwState {
    keywords: HashMap<String, KwValue>,
    missing: Vec<String>,
    meta_errors: Vec<String>,
}

// TODO use newtype for "Keyword" type so this is less confusing
#[derive(Debug)]
struct StandardErrors {
    missing_keywords: Vec<String>,
    value_errors: HashMap<String, KwError>,
    // TODO, these are errors involving multiple keywords, like PnB not matching DATATYPE
    meta_errors: Vec<String>,
}

// TODO add deprecation warnings somewhere in here
impl KwState {
    // TODO format $param here
    // TODO not DRY (although will likely need HKTs)
    fn get_required<V, F>(&mut self, k: &str, f: F) -> Option<V>
    where
        F: FnOnce(&str) -> ParseResult<V>,
    {
        let sk = format_standard_kw(k);
        match self.keywords.get_mut(&sk) {
            Some(v) => match v.status {
                ValueStatus::Raw => {
                    let (s, r) = f(&v.value).map_or_else(
                        |e| (ValueStatus::Error(e), None),
                        |x| (ValueStatus::Used, Some(x)),
                    );
                    v.status = s;
                    r
                }
                _ => None,
            },
            None => {
                self.missing.push(String::from(k));
                None
            }
        }
    }

    // TODO if we encounter an error the use may want to ignore it and drop
    // the value rather than totally crash. This will require differentiating
    // b/t fatal and non-fatal errors.
    fn get_optional<V, F>(&mut self, k: &str, f: F) -> OptionalKw<V>
    where
        F: FnOnce(&str) -> ParseResult<V>,
    {
        let sk = format_standard_kw(k);
        match self.keywords.get_mut(&sk) {
            Some(v) => match v.status {
                ValueStatus::Raw => {
                    let (s, r) = f(&v.value).map_or_else(
                        |w| (ValueStatus::Warning(w), Absent),
                        |x| (ValueStatus::Used, OptionalKw::Present(x)),
                    );
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
        self.get_required("BEGINDATA", parse_offset)
    }

    fn lookup_enddata(&mut self) -> Option<u32> {
        self.get_required("ENDDATA", parse_offset)
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
            self.get_required("BEGINSTEXT", parse_offset),
            self.get_required("ENDSTEXT", parse_offset),
            self.get_required("BEGINANALYSIS", parse_offset_or_blank),
            self.get_required("ENDANALYSIS", parse_offset_or_blank),
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
        self.get_required("BYTEORD", |s| match parse_endian(s) {
            Ok(e) => Ok(ByteOrd::Endian(e)),
            _ => {
                let xs: Vec<&str> = s.split(",").collect();
                let nxs = xs.len();
                let xs_num: Vec<u8> = xs.iter().filter_map(|s| s.parse().ok()).unique().collect();
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
        })
    }

    fn lookup_endian(&mut self) -> Option<Endian> {
        self.get_required("BYTEORD", parse_endian)
    }

    fn lookup_datatype(&mut self) -> Option<AlphaNumType> {
        self.get_required("DATATYPE", |s| match s {
            "I" => Ok(AlphaNumType::Integer),
            "F" => Ok(AlphaNumType::Single),
            "D" => Ok(AlphaNumType::Double),
            "A" => Ok(AlphaNumType::Ascii),
            _ => Err(String::from("unknown datatype")),
        })
    }

    fn lookup_mode(&mut self) -> Option<Mode> {
        self.get_required("MODE", |s| match s {
            "C" => Ok(Mode::Correlated),
            "L" => Ok(Mode::List),
            "U" => Ok(Mode::Uncorrelated),
            _ => Err(String::from("unknown mode")),
        })
    }

    fn lookup_mode3_2(&mut self) -> Option<Mode> {
        self.get_required("MODE", |s| match s {
            "L" => Ok(Mode::List),
            _ => Err(String::from("unknown mode (U and C are no longer valid)")),
        })
    }

    fn lookup_nextdata(&mut self) -> Option<u32> {
        self.get_required("NEXTDATA", parse_offset)
    }

    fn lookup_par(&mut self) -> Option<u32> {
        self.get_required("PAR", parse_int)
    }

    fn lookup_tot_req(&mut self) -> Option<u32> {
        self.get_required("TOT", parse_int)
    }

    fn lookup_tot_opt(&mut self) -> OptionalKw<u32> {
        self.get_optional("TOT", parse_int)
    }

    fn lookup_cyt_req(&mut self) -> Option<String> {
        self.get_required("CYT", parse_str)
    }

    fn lookup_cyt_opt(&mut self) -> OptionalKw<String> {
        self.get_optional("CYT", parse_str)
    }

    fn lookup_abrt(&mut self) -> OptionalKw<u32> {
        self.get_optional("ABRT", parse_int)
    }

    fn lookup_cells(&mut self) -> OptionalKw<String> {
        self.get_optional("CELLS", parse_str)
    }

    fn lookup_com(&mut self) -> OptionalKw<String> {
        self.get_optional("COM", parse_str)
    }

    fn lookup_exp(&mut self) -> OptionalKw<String> {
        self.get_optional("EXP", parse_str)
    }

    fn lookup_fil(&mut self) -> OptionalKw<String> {
        self.get_optional("FIL", parse_str)
    }

    fn lookup_inst(&mut self) -> OptionalKw<String> {
        self.get_optional("INST", parse_str)
    }

    fn lookup_lost(&mut self) -> OptionalKw<u32> {
        self.get_optional("LOST", parse_int)
    }

    fn lookup_op(&mut self) -> OptionalKw<String> {
        self.get_optional("OP", parse_str)
    }

    fn lookup_proj(&mut self) -> OptionalKw<String> {
        self.get_optional("PROJ", parse_str)
    }

    fn lookup_smno(&mut self) -> OptionalKw<String> {
        self.get_optional("SMNO", parse_str)
    }

    fn lookup_src(&mut self) -> OptionalKw<String> {
        self.get_optional("SRC", parse_str)
    }

    fn lookup_sys(&mut self) -> OptionalKw<String> {
        self.get_optional("SYS", parse_str)
    }

    fn lookup_trigger(&mut self) -> OptionalKw<Trigger> {
        self.get_optional("TR", |s| match s.split(",").collect::<Vec<&str>>()[..] {
            [p, n1] => parse_int(n1).map(|threshold| Trigger {
                parameter: String::from(p),
                threshold,
            }),
            _ => Err(String::from("wrong number of fields")),
        })
    }

    fn lookup_cytsn(&mut self) -> OptionalKw<String> {
        self.get_optional("CYTSN", parse_str)
    }

    fn lookup_timestep(&mut self) -> OptionalKw<f32> {
        self.get_optional("TIMESTEP", parse_float)
    }

    fn lookup_vol(&mut self) -> OptionalKw<f32> {
        self.get_optional("VOL", parse_float)
    }

    fn lookup_flowrate(&mut self) -> OptionalKw<String> {
        self.get_optional("FLOWRATE", parse_str)
    }

    fn lookup_unicode(&mut self) -> OptionalKw<Unicode> {
        self.get_optional("UNICODE", |s| {
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
        })
    }

    fn lookup_plateid(&mut self) -> OptionalKw<String> {
        self.get_optional("PLATEID", parse_str)
    }

    fn lookup_platename(&mut self) -> OptionalKw<String> {
        self.get_optional("PLATENAME", parse_str)
    }

    fn lookup_wellid(&mut self) -> OptionalKw<String> {
        self.get_optional("WELLID", parse_str)
    }

    fn lookup_unstainedinfo(&mut self) -> OptionalKw<String> {
        self.get_optional("UNSTAINEDINFO", parse_str)
    }

    fn lookup_unstainedcenters(&mut self) -> OptionalKw<UnstainedCenters> {
        self.get_optional("UNSTAINEDICENTERS", |s| {
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
        })
    }

    fn lookup_last_modifier(&mut self) -> OptionalKw<String> {
        self.get_optional("LAST_MODIFIER", parse_str)
    }

    fn lookup_last_modified(&mut self) -> OptionalKw<NaiveDateTime> {
        // TODO hopefully case doesn't matter...
        self.get_optional("LAST_MODIFIED", |s| {
            NaiveDateTime::parse_from_str(s, "%d-%b-%Y %H:%M:%S.%.3f").or(
                NaiveDateTime::parse_from_str(s, "%d-%b-%Y %H:%M:%S").or(Err(String::from(
                    "must be formatted like 'dd-mmm-yyyy hh:mm:ss[.cc]'",
                ))),
            )
        })
    }

    fn lookup_originality(&mut self) -> OptionalKw<Originality> {
        self.get_optional("ORIGINALITY", |s| match s {
            "Original" => Ok(Originality::Original),
            "NonDataModified" => Ok(Originality::NonDataModified),
            "Appended" => Ok(Originality::Appended),
            "DataModified" => Ok(Originality::DataModified),
            _ => Err(String::from("invalid originality")),
        })
    }

    fn lookup_carrierid(&mut self) -> OptionalKw<String> {
        self.get_optional("CARRIERID", parse_str)
    }

    fn lookup_carriertype(&mut self) -> OptionalKw<String> {
        self.get_optional("CARRIERTYPE", parse_str)
    }

    fn lookup_locationid(&mut self) -> OptionalKw<String> {
        self.get_optional("LOCATIONID", parse_str)
    }

    fn lookup_begindatetime(&mut self) -> OptionalKw<DateTime<FixedOffset>> {
        self.get_optional("BEGINDATETIME", parse_iso_datetime)
    }

    fn lookup_enddatetime(&mut self) -> OptionalKw<DateTime<FixedOffset>> {
        self.get_optional("ENDDATETIME", parse_iso_datetime)
    }

    fn lookup_date(&mut self) -> OptionalKw<NaiveDate> {
        self.get_optional("DATE", parse_date)
    }

    fn lookup_btim60(&mut self) -> OptionalKw<NaiveTime> {
        self.get_optional("BTIM", parse_time60)
    }

    fn lookup_etim60(&mut self) -> OptionalKw<NaiveTime> {
        self.get_optional("ETIM", parse_time60)
    }

    fn lookup_btim100(&mut self) -> OptionalKw<NaiveTime> {
        self.get_optional("BTIM", parse_time100)
    }

    fn lookup_etim100(&mut self) -> OptionalKw<NaiveTime> {
        self.get_optional("ETIM", parse_time100)
    }

    fn lookup_timestamps2_0(&mut self, centi: bool) -> Timestamps2_0 {
        let (t0, t1) = if centi {
            (self.lookup_btim60(), self.lookup_etim60())
        } else {
            (self.lookup_btim100(), self.lookup_etim100())
        };
        Timestamps2_0 {
            btim: t0,
            etim: t1,
            date: self.lookup_date(),
        }
    }

    fn lookup_timestamps3_2(&mut self) -> Timestamps3_2 {
        Timestamps3_2 {
            start: self.lookup_begindatetime(),
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

    fn lookup_plate(&mut self) -> PlateData {
        PlateData {
            wellid: self.lookup_plateid(),
            platename: self.lookup_platename(),
            plateid: self.lookup_wellid(),
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

    // parameters

    fn lookup_param_req<V, F>(&mut self, param: &'static str, n: u32, f: F) -> Option<V>
    where
        F: FnOnce(&str) -> ParseResult<V>,
    {
        self.get_required(&format_param(n, param), f)
    }

    fn lookup_param_opt<V, F>(&mut self, param: &'static str, n: u32, f: F) -> OptionalKw<V>
    where
        F: FnOnce(&str) -> ParseResult<V>,
    {
        self.get_optional(&format_param(n, param), f)
    }

    // this is actually read the PnB field which has "bits" in it, but as
    // far as I know nobody is using anything other than evenly-spaced bytes
    fn lookup_param_bytes(&mut self, n: u32) -> Option<Bytes> {
        self.lookup_param_req("B", n, parse_bytes)
    }

    fn lookup_param_range(&mut self, n: u32) -> Option<Range> {
        self.lookup_param_req("R", n, |s| match s.parse::<u64>() {
            Ok(x) => Ok(Range::Int(x - 1)),
            Err(e) => match e.kind() {
                IntErrorKind::InvalidDigit => s
                    .parse::<f64>()
                    .map_or_else(|e| Err(format!("{}", e)), |x| Ok(Range::Float(x))),
                IntErrorKind::PosOverflow => Ok(Range::Int(u64::MAX)),
                _ => Err(format!("{}", e)),
            },
        })
    }

    fn lookup_param_wavelength(&mut self, n: u32) -> OptionalKw<u32> {
        self.lookup_param_opt("L", n, parse_int)
    }

    fn lookup_param_power(&mut self, n: u32) -> OptionalKw<u32> {
        self.lookup_param_opt("O", n, parse_int)
    }

    fn lookup_param_detector_type(&mut self, n: u32) -> OptionalKw<String> {
        self.lookup_param_opt("T", n, parse_str)
    }

    fn lookup_param_shortname_req(&mut self, n: u32) -> Option<String> {
        self.lookup_param_req("N", n, parse_str)
    }

    fn lookup_param_shortname_opt(&mut self, n: u32) -> OptionalKw<String> {
        self.lookup_param_opt("N", n, parse_str)
    }

    fn lookup_param_longname(&mut self, n: u32) -> OptionalKw<String> {
        self.lookup_param_opt("S", n, parse_str)
    }

    fn lookup_param_filter(&mut self, n: u32) -> OptionalKw<String> {
        self.lookup_param_opt("F", n, parse_str)
    }

    fn lookup_param_percent_emitted(&mut self, n: u32) -> OptionalKw<u32> {
        self.lookup_param_opt("P", n, parse_int)
    }

    fn lookup_param_detector_voltage(&mut self, n: u32) -> OptionalKw<f32> {
        self.lookup_param_opt("V", n, parse_float)
    }

    fn lookup_param_detector(&mut self, n: u32) -> OptionalKw<String> {
        self.lookup_param_opt("DET", n, parse_str)
    }

    fn lookup_param_tag(&mut self, n: u32) -> OptionalKw<String> {
        self.lookup_param_opt("TAG", n, parse_str)
    }

    fn lookup_param_analyte(&mut self, n: u32) -> OptionalKw<String> {
        self.lookup_param_opt("ANALYTE", n, parse_str)
    }

    fn lookup_param_gain(&mut self, n: u32) -> OptionalKw<f32> {
        self.lookup_param_opt("G", n, parse_float)
    }

    fn lookup_param_scale_req(&mut self, n: u32) -> Option<Scale> {
        self.lookup_param_req("E", n, parse_scale)
    }

    fn lookup_param_scale_opt(&mut self, n: u32) -> OptionalKw<Scale> {
        self.lookup_param_opt("E", n, parse_scale)
    }

    fn lookup_param_calibration(&mut self, n: u32) -> OptionalKw<Calibration> {
        self.lookup_param_opt("CALIBRATION", n, |s| {
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
        })
    }

    // for 3.1+ PnL parameters, which can have multiple wavelengths
    fn lookup_param_wavelengths(&mut self, n: u32) -> Vec<u32> {
        self.lookup_param_opt("L", n, |s| {
            let mut ws = vec![];
            for x in s.split(",") {
                match x.parse() {
                    Ok(y) => ws.push(y),
                    _ => return Err(String::from("invalid float encountered")),
                };
            }
            Ok(ws)
        })
        .to_option()
        .unwrap_or_default()
    }

    fn lookup_param_display(&mut self, n: u32) -> OptionalKw<Display> {
        self.lookup_param_opt("D", n, |s| {
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
        })
    }

    fn lookup_param_datatype(&mut self, n: u32) -> OptionalKw<NumType> {
        self.lookup_param_opt("DATATYPE", n, |s| match s {
            "I" => Ok(NumType::Integer),
            "F" => Ok(NumType::Single),
            "D" => Ok(NumType::Double),
            _ => Err(String::from("unknown datatype")),
        })
    }

    fn lookup_param_type(&mut self, n: u32) -> OptionalKw<MeasurementType> {
        self.lookup_param_opt("TYPE", n, |s| match s {
            "Forward Scatter" => Ok(MeasurementType::ForwardScatter),
            "Raw Fluorescence" => Ok(MeasurementType::RawFluorescence),
            "Mass" => Ok(MeasurementType::Mass),
            "Time" => Ok(MeasurementType::Time),
            "Index" => Ok(MeasurementType::Index),
            "Classification" => Ok(MeasurementType::Classification),
            _ => Err(String::from("unknown measurement type")),
        })
    }

    // TODO some imaging cytometers store "Eccentricity" and friends in here
    fn lookup_param_feature(&mut self, n: u32) -> OptionalKw<Feature> {
        self.lookup_param_opt("FEATURE", n, |s| match s {
            "Area" => Ok(Feature::Area),
            "Width" => Ok(Feature::Width),
            "Height" => Ok(Feature::Height),
            _ => Err(String::from("unknown parameter feature")),
        })
    }

    fn compile_errors(self) -> StandardErrors {
        let mut value_errors: HashMap<String, KwError> = HashMap::new();
        for (k, v) in self.keywords {
            // TODO lots of clones here, not sure if this is necessary to fix
            if let ValueStatus::Error(e) = v.status {
                value_errors.insert(
                    k,
                    KwError {
                        value: v.value,
                        msg: e,
                    },
                );
            }
        }
        StandardErrors {
            missing_keywords: self.missing,
            value_errors,
            meta_errors: self.meta_errors,
        }
    }

    fn push_meta_error(&mut self, msg: String) {
        self.meta_errors.push(msg);
    }

    // ASSUME There aren't any errors recorded in the state hashtable since we
    // don't check them here. This function requires a standardized keyword
    // struct to run, and presumably this won't exist if there are any errors.
    // fn finalize<S>(self, standard: S) -> TEXT<S> {
    //     // TODO this struct shouldn't touch any nonstandard keywords, since it
    //     // isn't supposed to do anything useful with them
    //     let mut nonstandard = HashMap::new();
    //     let mut deviant = HashMap::new();
    //     for (k, v) in self.keywords {
    //         if let ValueStatus::Raw = v.status {
    //             if k.starts_with("$") {
    //                 deviant.insert(k, v.value);
    //             } else {
    //                 nonstandard.insert(k, v.value);
    //             }
    //         }
    //     }
    //     TEXT {
    //         standard,
    //         nonstandard,
    //         deviant,
    //     }
    // }
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
        parse_header(hs).map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    } else {
        Err(io::Error::new(
            io::ErrorKind::Other,
            "header sequence is not valid text",
        ))
    }
}

// read TEXT segment:
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

#[derive(Debug)]
struct RawTEXT {
    delimiter: u8,
    keywords: HashMap<String, String>,
}

impl RawTEXT {
    // fn fix<F: FnMut(&Self) -> ()>(&self, f: F) -> ();

    // fn standardize(self) -> AnyTEXT {
    //     let st = self.to_state();
    // }

    fn to_state(self) -> KwState {
        let mut keywords = HashMap::new();
        for (k, v) in self.keywords {
            keywords.insert(
                k,
                KwValue {
                    value: v,
                    status: ValueStatus::Raw,
                },
            );
        }
        KwState {
            keywords,
            missing: vec![],
            meta_errors: vec![],
        }
    }
}

// TODO possibly not optimal, this will read each byte twice
fn read_text<R: Read + Seek>(h: &mut BufReader<R>, header: &Header) -> io::Result<RawTEXT> {
    h.seek(SeekFrom::Start(u64::from(header.text.begin)))?;
    let x0 = u64::from(header.text.begin);
    let x1 = x0 + 1;
    let xf = u64::from(header.text.end);
    let textlen = u64::from(header.text.num_bytes());

    // Read first character, which should be the delimiter
    let mut dbuf = [0_u8];
    h.read_exact(&mut dbuf)?;
    let delimiter = dbuf[0];

    // Valid delimiters are in the set of {1..126}
    if (1..=126).contains(&delimiter) {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            "delimiter must be an ASCII character 1-126",
        ));
    }

    // Read from the 2nd character to all but the last character and record
    // delimiter positions.
    let mut delim_positions = vec![];
    delim_positions.push(x0);
    for (i, c) in (x1..).zip(h.take(textlen - 2).bytes()) {
        if c? == delimiter {
            delim_positions.push(i);
        }
    }
    delim_positions.push(xf);

    // Read the last character and ensure it is also a delimiter
    h.read_exact(&mut dbuf)?;
    if dbuf[0] != delimiter {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            "final character in TEXT is not the delimiter, file may be corrupt",
        ));
    }

    // TODO pretty sure lots of FCS files actually have blank values which means
    // we will need to deal with double delimiters which are real word
    // boundaries. This can be easily handled by not doing the chunk_by below
    // and simply copying the position/length tuples over. If we allow blank
    // values we disallow escaping, since the premise of escaping assumes that
    // two delimiters in a row are special.

    // Remove "escaped" delimiters from position vector. Because we disallow
    // blank values and also disallow delimiters at the start/end of words, this
    // implies that we should only see delimiters by themselves or in a
    // consecutive sequence whose length is even.
    let mut boundaries: Vec<(u64, u64)> = vec![]; // (position, length)

    for (key, chunk) in delim_positions
        // We force-added the first and last delimiter in the TEXT segment, so
        // this is guaranteed to have at least two elements (one pair)
        .windows(2)
        .filter_map(|x| match x {
            [a, b] => Some((*a, b - a)),
            _ => None,
        })
        .chunk_by(|(x, _)| *x)
        .into_iter()
    {
        if key == 1 {
            if chunk.count() % 2 == 1 {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "delimiter found at start or end of word",
                ));
            }
        } else {
            for x in chunk {
                boundaries.push(x);
            }
        }
    }

    // If all went well in the previous step, we should now have the following:
    // 1. first entry coincides with start of TEXT
    // 2. last entry coincides with end of TEXT
    if let Some((x, _)) = boundaries.first() {
        if *x > x0 {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "first keyword starts with a delimiter",
            ));
        }
    }
    if let Some((x, len)) = boundaries.last() {
        if *x + len < xf {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "final value ends with a delimiter",
            ));
        }
    }

    let mut keywords: HashMap<String, String> = HashMap::new();
    let mut kbuf = String::new();
    let mut vbuf = String::new();

    h.seek(SeekFrom::Start(x0))?;

    let delimiter2 = [delimiter, delimiter];
    let delimiter1 = [delimiter];
    // ASSUME these won't fail as we checked the delimiter is an ASCII character
    let escape_from = str::from_utf8(&delimiter2).unwrap();
    let escape_to = str::from_utf8(&delimiter1).unwrap();
    let fix_word = |s: &str| s.replace(escape_from, escape_to);

    for chunk in boundaries.chunks(2) {
        if let [(_, klen), (_, vlen)] = chunk {
            h.seek(SeekFrom::Current(1))?;
            h.take(*klen - 1).read_to_string(&mut kbuf)?;
            h.seek(SeekFrom::Current(1))?;
            h.take(*vlen - 1).read_to_string(&mut vbuf)?;
            keywords.insert(fix_word(&kbuf), fix_word(&vbuf));
            kbuf.clear();
            vbuf.clear();
        } else {
            // TODO could also ignore this since it isn't necessarily fatal
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "number of words is not even, parsing failed",
            ));
        }
    }

    Ok(RawTEXT {
        delimiter,
        keywords,
    })
}

// TODO this is basically a matrix, probably a crate I can use
struct Comp {}

fn main() {
    let args: Vec<String> = env::args().collect();

    // let args = &args[1..];

    // println!("{}", &args[1]);
    let file = fs::File::open(&args[1]).unwrap();
    let mut reader = BufReader::new(file);
    let header = read_header(&mut reader).unwrap();
    let text = read_text(&mut reader, &header).unwrap();
    // println!("{:#?}", &text.delimiter);
    // for (k, v) in &text.keywords {
    //     if v.len() < 100 {
    //         println!("{}: {}", k, v);
    //     }
    // }
    let stext = AnyTEXT::from_kws(header, text);
    println!("{:#?}", stext);
}

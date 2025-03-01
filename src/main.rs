// TODO gating parameters not added (yet)

use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use regex::Regex;
use std::collections::{HashMap, HashSet};

struct Bounds {
    begin: u32,
    end: u32,
}

enum Version {
    FCS2_0,
    FCS3_0,
    FCS3_1,
    FCS3_2,
}

struct Header {
    version: Version,
    text: Bounds,
    data: Bounds,
    analysis: Bounds,
}

enum AlphaNumTypes {
    Ascii,
    Integer,
    Float,
    Double,
}

enum NumTypes {
    Integer,
    Float,
    Double,
}

enum Endianness {
    BigEndian,
    LittleEndian,
}

enum ByteOrd {
    BitLittle(Endianness),
    Mixed(u8, u8, u8, u8),
}

struct Trigger {
    parameter: String,
    threshhold: u32,
}

struct TextBounds<A, D, T> {
    analysis: A,
    data: D,
    stext: T,
}

type TextBounds3_0 = TextBounds<Bounds, Bounds, Bounds>;
type TextBounds3_2 = TextBounds<Option<Bounds>, Bounds, Option<Bounds>>;

struct Timestamps2_0 {
    btim: Option<NaiveTime>,
    etim: Option<NaiveTime>,
    date: Option<NaiveDate>,
}

struct Timestamps3_2 {
    // TODO local or urc? FCS allows both
    start: Option<DateTime<Utc>>,
    end: Option<DateTime<Utc>>,
}

// TODO this is super messy, see 3.2 spec for restrictions on this we may with
// to use further
struct LogScale {
    decades: f32,
    offset: f32,
}

enum Scale {
    Log(LogScale),
    Linear,
}

use Scale::*;

struct LinDisplay {
    lower: f32,
    upper: f32,
}

struct LogDisplay {
    offset: f32,
    decades: f32,
}

enum Display {
    Lin(LinDisplay),
    Log(LogDisplay),
}

struct Calibration {
    value: f32,
    unit: String,
}

struct InnerParameter3_0 {
    gain: Option<f32>,
}

struct InnerParameter3_1 {
    calibration: Option<Calibration>,
    display: Option<Display>,
    older: InnerParameter3_0,
}

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

enum Feature {
    Area,
    Width,
    Height,
}

struct InnerParameter3_2 {
    analyte: Option<String>,
    feature: Option<Feature>,
    measurement_type: Option<MeasurementType>,
    tag: Option<String>,
    detector_name: Option<String>,
    datatype: Option<NumTypes>,
    older: InnerParameter3_1,
}

struct Parameter<E, L, N, X> {
    bits: u32,                     // PnB
    range: u32,                    // PnR
    scale: E,                      // PnE
    shortname: N,                  // PnN
    longname: Option<String>,      // PnS
    filter: Option<String>,        // PnF
    wavelength: L,                 // PnL
    power: Option<u32>,            // PnO
    detector_type: Option<String>, // PnD
    percent_emitted: Option<u32>,  // PnP (TODO deprecated in 3.2, factor out)
    detector_voltage: Option<f32>, // PnV
    specific: X,
}

type Wavelength2_0 = Option<u32>;
type Wavelength3_1 = Vec<u32>;

trait ParameterFromKeywords: Sized {
    fn build_parameter(st: &mut KwState, n: u32, bits: u32) -> Option<Self>;

    fn from_kws(st: &mut KwState) -> Vec<Self> {
        let mut ps = vec![];
        let mut n = 1;
        loop {
            // lookup bits since this should be present in all versions, if not
            // present then consider the previous index to be the last parameter
            // index
            match lookup_bits(st, n) {
                Some(bits) => match Self::build_parameter(st, n, bits) {
                    Some(p) => ps.push(p),
                    None => break,
                },
                None => break,
            };
            n = n + 1
        }
        return ps;
    }
}

type Parameter2_0 = Parameter<Option<Scale>, Wavelength2_0, Option<String>, ()>;
type Parameter3_0 = Parameter<Scale, Wavelength2_0, Option<String>, InnerParameter3_0>;
type Parameter3_1 = Parameter<Scale, Wavelength3_1, String, InnerParameter3_1>;
type Parameter3_2 = Parameter<Scale, Wavelength3_1, String, InnerParameter3_2>;

fn lookup_param_req<V, F>(st: &mut KwState, param: &'static str, n: u32, f: F) -> Option<V>
where
    F: FnOnce(&str) -> Result<V, &'static str>,
{
    st.get_required(&format!("P{}{}", n, param), f)
}

fn lookup_param_opt<V, F>(st: &mut KwState, param: &'static str, n: u32, f: F) -> Option<Option<V>>
where
    F: FnOnce(&str) -> Result<V, &'static str>,
{
    st.get_optional(&format!("P{}{}", n, param), f)
}

fn parse_int(s: &str) -> Result<u32, &'static str> {
    s.parse().or(Err("invalid integer"))
}

fn parse_float(s: &str) -> Result<f32, &'static str> {
    s.parse().or(Err("invalid float"))
}

fn parse_str(s: &str) -> Result<String, &'static str> {
    Ok(String::from(s))
}

// TODO check that this is in multiples of 8 for relevant specs
fn lookup_bits(st: &mut KwState, n: u32) -> Option<u32> {
    lookup_param_req(st, "B", n, parse_int)
}

fn lookup_range(st: &mut KwState, n: u32) -> Option<u32> {
    lookup_param_req(st, "R", n, parse_int)
}

fn lookup_wavelength(st: &mut KwState, n: u32) -> Option<Option<u32>> {
    lookup_param_opt(st, "L", n, parse_int)
}

fn lookup_power(st: &mut KwState, n: u32) -> Option<Option<u32>> {
    lookup_param_opt(st, "O", n, parse_int)
}

fn lookup_detector_type(st: &mut KwState, n: u32) -> Option<Option<String>> {
    lookup_param_opt(st, "T", n, parse_str)
}

fn lookup_shortname_req(st: &mut KwState, n: u32) -> Option<String> {
    lookup_param_req(st, "N", n, parse_str)
}

fn lookup_shortname_opt(st: &mut KwState, n: u32) -> Option<Option<String>> {
    lookup_param_opt(st, "N", n, parse_str)
}

fn lookup_longname(st: &mut KwState, n: u32) -> Option<Option<String>> {
    lookup_param_opt(st, "S", n, parse_str)
}

fn lookup_filter(st: &mut KwState, n: u32) -> Option<Option<String>> {
    lookup_param_opt(st, "F", n, parse_str)
}

fn lookup_percent_emitted(st: &mut KwState, n: u32) -> Option<Option<u32>> {
    lookup_param_opt(st, "P", n, parse_int)
}

fn lookup_detector_voltage(st: &mut KwState, n: u32) -> Option<Option<f32>> {
    lookup_param_opt(st, "P", n, parse_float)
}

fn lookup_detector(st: &mut KwState, n: u32) -> Option<Option<String>> {
    lookup_param_opt(st, "DET", n, parse_str)
}

fn lookup_tag(st: &mut KwState, n: u32) -> Option<Option<String>> {
    lookup_param_opt(st, "TAG", n, parse_str)
}

fn lookup_analyte(st: &mut KwState, n: u32) -> Option<Option<String>> {
    lookup_param_opt(st, "ANALYTE", n, parse_str)
}

fn lookup_gain(st: &mut KwState, n: u32) -> Option<Option<f32>> {
    lookup_param_opt(st, "G", n, parse_float)
}

fn parse_scale(s: &str) -> Result<Scale, &'static str> {
    let v: Vec<&str> = s.split(",").collect();
    match v[..] {
        [ds, os] => match (ds.parse(), os.parse()) {
            (Ok(0.0), Ok(0.0)) => Ok(Linear),
            (Ok(decades), Ok(offset)) => Ok(Log(LogScale { decades, offset })),
            _ => Err("invalid floats"),
        },
        _ => Err("too many fields"),
    }
}

fn lookup_scale_req(st: &mut KwState, n: u32) -> Option<Scale> {
    lookup_param_req(st, "E", n, parse_scale)
}

fn lookup_scale_opt(st: &mut KwState, n: u32) -> Option<Option<Scale>> {
    lookup_param_opt(st, "E", n, parse_scale)
}

fn lookup_calibration(st: &mut KwState, n: u32) -> Option<Option<Calibration>> {
    lookup_param_opt(st, "CALIBRATION", n, |s| {
        let v: Vec<&str> = s.split(",").collect();
        match v[..] {
            [svalue, unit] => match svalue.parse() {
                Ok(value) if value >= 0.0 => Ok(Calibration {
                    value,
                    unit: String::from(unit),
                }),
                _ => Err("invalid (positive) float"),
            },
            _ => Err("too many fields"),
        }
    })
}

// for 3.1+ PnL parameters, which can have multiple wavelengths
fn lookup_wavelengths(st: &mut KwState, n: u32) -> Option<Vec<u32>> {
    lookup_param_opt(st, "L", n, |s| {
        let mut ws = vec![];
        for x in s.split(",") {
            match x.parse() {
                Ok(y) => ws.push(y),
                _ => return Err("invalid float encountered"),
            };
        }
        return Ok(ws);
    })
    .map(|x| x.unwrap_or(vec![]))
}

fn lookup_display(st: &mut KwState, n: u32) -> Option<Option<Display>> {
    lookup_param_opt(st, "D", n, |s| {
        let v: Vec<&str> = s.split(",").collect();
        match v[..] {
            [which, f1, f2] => match (which, f1.parse(), f2.parse()) {
                ("Linear", Ok(lower), Ok(upper)) => Ok(Display::Lin(LinDisplay { lower, upper })),
                ("Logarithmic", Ok(decades), Ok(offset)) => {
                    Ok(Display::Log(LogDisplay { decades, offset }))
                }
                _ => Err("invalid floats"),
            },
            _ => Err("too many fields"),
        }
    })
}

fn lookup_datatype(st: &mut KwState, n: u32) -> Option<Option<NumTypes>> {
    lookup_param_opt(st, "DATATYPE", n, |s| match s {
        "I" => Ok(NumTypes::Integer),
        "F" => Ok(NumTypes::Float),
        "D" => Ok(NumTypes::Double),
        _ => Err("unknown datatype"),
    })
}

fn lookup_type(st: &mut KwState, n: u32) -> Option<Option<MeasurementType>> {
    lookup_param_opt(st, "TYPE", n, |s| match s {
        "Forward Scatter" => Ok(MeasurementType::ForwardScatter),
        "Raw Fluorescence" => Ok(MeasurementType::RawFluorescence),
        "Mass" => Ok(MeasurementType::Mass),
        "Time" => Ok(MeasurementType::Time),
        "Index" => Ok(MeasurementType::Index),
        "Classification" => Ok(MeasurementType::Classification),
        _ => Err("unknown measurement type"),
    })
}

fn lookup_feature(st: &mut KwState, n: u32) -> Option<Option<Feature>> {
    lookup_param_opt(st, "FEATURE", n, |s| match s {
        "Area" => Ok(Feature::Area),
        "Width" => Ok(Feature::Width),
        "Height" => Ok(Feature::Height),
        _ => Err("unknown parameter feature"),
    })
}

impl ParameterFromKeywords for Parameter2_0 {
    fn build_parameter(st: &mut KwState, n: u32, bits: u32) -> Option<Parameter2_0> {
        let pnr = lookup_range(st, n);
        let pne = lookup_scale_opt(st, n);
        let pnn = lookup_shortname_opt(st, n);
        let pns = lookup_longname(st, n);
        let pnf = lookup_filter(st, n);
        let pnl = lookup_wavelength(st, n);
        let pno = lookup_power(st, n);
        let pnt = lookup_detector_type(st, n);
        let pnp = lookup_percent_emitted(st, n);
        let pnv = lookup_detector_voltage(st, n);
        match (pnr, pne, pnn, pns, pnf, pnl, pno, pnt, pnp, pnv) {
            (
                Some(range),
                Some(scale),
                Some(shortname),
                Some(longname),
                Some(filter),
                Some(wavelength),
                Some(power),
                Some(detector_type),
                Some(percent_emitted),
                Some(detector_voltage),
            ) => Some(Parameter {
                bits,
                range,
                scale,
                shortname,
                longname,
                filter,
                wavelength,
                power,
                detector_type,
                percent_emitted,
                detector_voltage,
                specific: (),
            }),
            _ => None,
        }
    }
}

impl ParameterFromKeywords for Parameter3_0 {
    fn build_parameter(st: &mut KwState, n: u32, bits: u32) -> Option<Parameter3_0> {
        let pnr = lookup_range(st, n);
        let pne = lookup_scale_req(st, n);
        let pnn = lookup_shortname_opt(st, n);
        let pns = lookup_longname(st, n);
        let pnf = lookup_filter(st, n);
        let pnl = lookup_wavelength(st, n);
        let pno = lookup_power(st, n);
        let pnt = lookup_detector_type(st, n);
        let pnp = lookup_percent_emitted(st, n);
        let pnv = lookup_detector_voltage(st, n);
        let png = lookup_gain(st, n);
        match (pnr, pne, pnn, pns, pnf, pnl, pno, pnt, pnp, pnv, png) {
            (
                Some(range),
                Some(scale),
                Some(shortname),
                Some(longname),
                Some(filter),
                Some(wavelength),
                Some(power),
                Some(detector_type),
                Some(percent_emitted),
                Some(detector_voltage),
                Some(gain),
            ) => Some(Parameter {
                bits,
                range,
                scale,
                shortname,
                longname,
                filter,
                wavelength,
                power,
                detector_type,
                percent_emitted,
                detector_voltage,
                specific: InnerParameter3_0 { gain },
            }),
            _ => None,
        }
    }
}

impl ParameterFromKeywords for Parameter3_1 {
    fn build_parameter(st: &mut KwState, n: u32, bits: u32) -> Option<Parameter3_1> {
        let pnr = lookup_range(st, n);
        let pne = lookup_scale_req(st, n);
        let pnn = lookup_shortname_req(st, n);
        let pns = lookup_longname(st, n);
        let pnf = lookup_filter(st, n);
        let pnl = lookup_wavelengths(st, n);
        let pno = lookup_power(st, n);
        let pnt = lookup_detector_type(st, n);
        let pnp = lookup_percent_emitted(st, n);
        let pnv = lookup_detector_voltage(st, n);
        let png = lookup_gain(st, n);
        let pncal = lookup_calibration(st, n);
        let pnd = lookup_display(st, n);
        match (
            pnr, pne, pnn, pns, pnf, pnl, pno, pnt, pnp, pnv, png, pncal, pnd,
        ) {
            (
                Some(range),
                Some(scale),
                Some(shortname),
                Some(longname),
                Some(filter),
                Some(wavelength),
                Some(power),
                Some(detector_type),
                Some(percent_emitted),
                Some(detector_voltage),
                Some(gain),
                Some(calibration),
                Some(display),
            ) => Some(Parameter {
                bits,
                range,
                scale,
                shortname,
                longname,
                filter,
                wavelength,
                power,
                detector_type,
                percent_emitted,
                detector_voltage,
                specific: InnerParameter3_1 {
                    calibration,
                    display,
                    older: InnerParameter3_0 { gain },
                },
            }),
            _ => None,
        }
    }
}

impl ParameterFromKeywords for Parameter3_2 {
    fn build_parameter(st: &mut KwState, n: u32, bits: u32) -> Option<Parameter3_2> {
        let pnr = lookup_range(st, n);
        let pne = lookup_scale_req(st, n);
        let pnn = lookup_shortname_req(st, n);
        let pns = lookup_longname(st, n);
        let pnf = lookup_filter(st, n);
        let pnl = lookup_wavelengths(st, n);
        let pno = lookup_power(st, n);
        let pnt = lookup_detector_type(st, n);
        let pnp = lookup_percent_emitted(st, n);
        let pnv = lookup_detector_voltage(st, n);
        let png = lookup_gain(st, n);
        let pncal = lookup_calibration(st, n);
        let pnd = lookup_display(st, n);
        let pndt = lookup_datatype(st, n);
        let pndet = lookup_detector(st, n);
        let pntag = lookup_tag(st, n);
        let pntype = lookup_type(st, n);
        let pnfeature = lookup_feature(st, n);
        let pnanalyte = lookup_analyte(st, n);
        match (
            pnr, pne, pnn, pns, pnf, pnl, pno, pnt, pnp, pnv, png, pncal, pnd, pndt, pndet, pntag,
            pntype, pnfeature, pnanalyte,
        ) {
            (
                Some(range),
                Some(scale),
                Some(shortname),
                Some(longname),
                Some(filter),
                Some(wavelength),
                Some(power),
                Some(detector_type),
                Some(percent_emitted),
                Some(detector_voltage),
                Some(gain),
                Some(calibration),
                Some(display),
                Some(datatype),
                Some(detector_name),
                Some(tag),
                Some(measurement_type),
                Some(feature),
                Some(analyte),
            ) => Some(Parameter {
                bits,
                range,
                scale,
                shortname,
                longname,
                filter,
                wavelength,
                power,
                detector_type,
                percent_emitted,
                detector_voltage,
                specific: InnerParameter3_2 {
                    datatype,
                    detector_name,
                    tag,
                    measurement_type,
                    feature,
                    analyte,
                    older: InnerParameter3_1 {
                        calibration,
                        display,
                        older: InnerParameter3_0 { gain },
                    },
                },
            }),
            _ => None,
        }
    }
}

enum Originality {
    Original,
    NonDataModified,
    Appended,
    DataModified,
}

struct ModificationData {
    last_modifier: Option<String>,
    list_modified: Option<DateTime<Utc>>,
    originality: Option<Originality>,
}

struct PlateData {
    plateid: Option<String>,
    platename: Option<String>,
    wellid: Option<String>,
}

struct UnstainedData {
    unstainedcenters: HashMap<String, f32>,
    unstainedinfo: Option<String>,
}

struct CarrierData {
    carrierid: Option<String>,
    carriertype: Option<String>,
    locationid: Option<String>,
}

type Timestep = Option<f32>;

type CytSN = Option<String>;

type Vol = Option<f32>;

struct OptionalCommon<C, I, M, P, S, T, U, V> {
    abrt: Option<u32>,
    com: Option<String>,
    cells: Option<String>,
    exp: Option<String>,
    fil: Option<String>,
    inst: Option<String>,
    lost: Option<u32>,
    op: Option<String>,
    proj: Option<String>,
    smno: Option<String>,
    src: Option<String>,
    sys: Option<String>,
    tr: Option<Trigger>,
    carrier: C,
    timestamps: I,
    modified: M,
    plate: P,
    cytsn: S,
    timestep: T,
    unstained: U,
    vol: V,
}

type OptionalCommon2_0 = OptionalCommon<(), Timestamps2_0, (), (), (), (), (), ()>;

type OptionalCommon3_0 = OptionalCommon<(), Timestamps2_0, (), (), CytSN, Timestep, (), ()>;

type OptionalCommon3_1 =
    OptionalCommon<(), Timestamps2_0, ModificationData, PlateData, CytSN, Timestep, (), Vol>;

type OptionalCommon3_2 = OptionalCommon<
    CarrierData,
    Timestamps3_2,
    ModificationData,
    PlateData,
    CytSN,
    Timestep,
    UnstainedData,
    Vol,
>;

struct Spillover {} // TODO, can probably get away with using a matrix for this

struct RequiredCommon<B, C, D, M, P, T> {
    par: u32,
    tot: T, // weirdly not required in 2.0
    mode: M,
    byteord: B,
    datatype: D,
    nextdata: u32,
    cyt: C,
    spillover: Spillover,
    parameters: Vec<P>,
}

struct Cyt(String);

struct Tot(u32);

enum Mode {
    List,
    Uncorrelated,
    Correlated,
}

type RequiredCommon2_0 =
    RequiredCommon<ByteOrd, Option<Cyt>, AlphaNumTypes, Mode, Parameter2_0, Option<Tot>>;

type RequiredCommon3_0 =
    RequiredCommon<ByteOrd, Option<Cyt>, AlphaNumTypes, Mode, Parameter3_0, Tot>;

type RequiredCommon3_1 =
    RequiredCommon<Endianness, Option<Cyt>, AlphaNumTypes, Mode, Parameter3_1, Tot>;

type RequiredCommon3_2 = RequiredCommon<Endianness, Cyt, NumTypes, (), Parameter3_2, Tot>;

struct StdText<O, P, R, X> {
    required: R,
    optional: O,
    parameters: Vec<P>,
    // random place for deprecated kws that I don't feel like putting in the
    // main required/optional structs
    misc: X,
}

struct MiscText3_0 {
    unicode: Unicode,
}

type StdText2_0 = StdText<RequiredCommon2_0, OptionalCommon2_0, Parameter2_0, ()>;
type StdText3_0 = StdText<RequiredCommon3_0, OptionalCommon3_0, Parameter3_0, MiscText3_0>;
type StdText3_1 = StdText<RequiredCommon3_1, OptionalCommon3_1, Parameter3_1, ()>;
type StdText3_2 = StdText<RequiredCommon3_2, OptionalCommon3_2, Parameter3_2, ()>;

struct StdTextResult<T> {
    text: T,
    errors: Keywords,
    nonstandard: Keywords,
}

impl StdText2_0 {
    fn from_kws(kws: Keywords) -> StdTextResult<StdText2_0> {
        unimplemented!()
        // return StdTextResult(text = 0, errors = 1, nonstandard = 2);
    }
}

struct TEXT<S> {
    // TODO add the offsets here as well? offsets are needed before parsing
    // everything else
    standard: S,
    standard_errors: HashMap<String, String>,
    nonstandard: HashMap<String, String>,
    deviant: HashMap<String, String>,
}

type TEXT2_0 = TEXT<StdText2_0>;
type TEXT3_0 = TEXT<StdText3_0>;
type TEXT3_1 = TEXT<StdText3_1>;
type TEXT3_2 = TEXT<StdText3_2>;

type Keywords = HashMap<String, String>;
type KeywordErrors = HashMap<String, (String, String)>;
type MissingKeywords = HashSet<String>;

enum KwStatus {
    Raw(String),
    Error(String, String),
    Missing,
}

// all hail the almighty state monad :D

// main idea: all kws start as "raw" and will then get moved to either missing
// or error categories. Those left in "raw" are nonstandard keywords. If everything
// is perfect this will be totally empty, since all fields in the main struct will be filled
struct KwState {
    keywords: HashMap<String, KwStatus>,
}

impl KwState {
    // TODO not DRY
    fn get_required<V, F>(&mut self, k: &str, f: F) -> Option<V>
    where
        F: FnOnce(&str) -> Result<V, &'static str>,
    {
        match self.keywords.remove(k) {
            Some(KwStatus::Raw(v)) => match f(&v) {
                Ok(x) => Some(x),
                Err(e) => {
                    // TODO string things seems lame
                    self.keywords
                        .insert(String::from(k), KwStatus::Error(v, String::from(e)));
                    None
                }
            },
            // silently ignore attempts to process the same keyword twice
            Some(_) => None,
            None => {
                self.keywords.insert(String::from(k), KwStatus::Missing);
                None
            }
        }
    }

    fn get_optional<V, F>(&mut self, k: &str, f: F) -> Option<Option<V>>
    where
        F: FnOnce(&str) -> Result<V, &'static str>,
    {
        match self.keywords.remove(k) {
            Some(KwStatus::Raw(v)) => match f(&v) {
                Ok(x) => Some(Some(x)),
                Err(e) => {
                    self.keywords
                        .insert(String::from(k), KwStatus::Error(v, String::from(e)));
                    None
                }
            },
            // silently ignore attempts to process the same keyword twice
            Some(_) => None,
            None => Some(None),
        }
    }
}

fn split_nonstandard(kws: Keywords) -> (Keywords, Keywords) {
    unimplemented!()
}

impl<T> TEXT<T> {
    fn from_kws(kws: Keywords) -> TEXT<T> {
        let res = T::from_kws(kws);
        let (ns, dv) = split_nonstandard(res.nonstandard);
        return TEXT {
            standard: res.text,
            standard_errors: res.errors,
            nonstandard: ns,
            deviant: dv,
        };
    }
}

// struct Correction {
//     from: u32,
//     to: u32,
//     frac: u32, // percent
// }

// struct Text2_0 {
//     corrections: Vec<Correction>,
// }

struct Unicode {
    page: u32,
    kws: Vec<String>,
}

// TODO this is basically a matrix, probably a crate I can use
struct Comp {}

struct Text3_0 {
    unicode: Option<Unicode>,
    comp: Comp,
    // TODO pull out
    analysis: Bounds,
    data: Bounds,
    text: Bounds,
}

struct Text3_1 {}

struct Text3_2 {}

// struct Text {
//     little_endian: bool,
//     datatype: Datatype,
// }

fn main() {
    println!("Hello, world!");
}

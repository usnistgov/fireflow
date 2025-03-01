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
            match lookup_bits(st, n).required(st) {
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

enum LookupResult<V> {
    Found(V),
    NotFound(String),
}

impl<V> LookupResult<V> {
    fn required(self, st: &mut KwState) -> Option<V> {
        match self {
            Found(x) => Some(x),
            NotFound(k) => {
                st.missing.insert(k);
                None
            }
        }
    }

    fn optional(self) -> Option<Option<V>> {
        match self {
            Found(x) => Some(Some(x)),
            NotFound(_) => None,
        }
    }
}

impl<V> LookupResult<Vec<V>> {
    fn to_vector(self) -> Option<Vec<V>> {
        match self {
            Found(xs) => Some(xs),
            NotFound(_) => Some(vec![]),
        }
    }
}

use LookupResult::*;

fn lookup_param_value<V, F>(st: &mut KwState, param: &'static str, n: u32, f: F) -> LookupResult<V>
where
    F: FnOnce(&str) -> Result<V, &'static str>,
{
    let k = format!("P{}{}", n, param);
    match st.keywords.remove(&k) {
        Some(v) => match f(&v) {
            Ok(x) => Found(x),
            Err(e) => {
                // TODO string things seems lame
                st.errors.insert(k.clone(), (v, String::from(e)));
                NotFound(String::from(k))
            }
        },
        None => NotFound(String::from(k)),
    }
}

// fn lookup_param_value<V, F: FnOnce(String, String) -> LookupResult<V>>(
//     st: &mut KwState,
//     param: &'static str,
//     n: u32,
//     f: F,
// ) -> LookupResult<V> {
//     let k = format!("P{}{}", n, param);
//     kws.remove(&k).map_or(NotFound, |s| f(k, s))
// }

fn lookup_int(st: &mut KwState, param: &'static str, n: u32) -> LookupResult<u32> {
    lookup_param_value(st, param, n, |s| s.parse().or(Err("invalid integer")))
}

fn lookup_float(st: &mut KwState, param: &'static str, n: u32) -> LookupResult<f32> {
    lookup_param_value(st, param, n, |s| s.parse().or(Err("invalid float")))
}

fn lookup_str(st: &mut KwState, param: &'static str, n: u32) -> LookupResult<String> {
    // TODO this seems lame
    lookup_param_value(st, param, n, |s| Ok(String::from(s)))
}

// TODO check that this is in multiples of 8 for relevant specs
fn lookup_bits(st: &mut KwState, n: u32) -> LookupResult<u32> {
    lookup_int(st, "B", n)
}

fn lookup_range(st: &mut KwState, n: u32) -> LookupResult<u32> {
    lookup_int(st, "R", n)
}

fn lookup_wavelength(st: &mut KwState, n: u32) -> LookupResult<u32> {
    lookup_int(st, "L", n)
}

fn lookup_power(st: &mut KwState, n: u32) -> LookupResult<u32> {
    lookup_int(st, "O", n)
}

fn lookup_detector_type(st: &mut KwState, n: u32) -> LookupResult<String> {
    lookup_str(st, "T", n)
}

fn lookup_shortname(st: &mut KwState, n: u32) -> LookupResult<String> {
    lookup_str(st, "N", n)
}

fn lookup_longname(st: &mut KwState, n: u32) -> LookupResult<String> {
    lookup_str(st, "S", n)
}

fn lookup_filter(st: &mut KwState, n: u32) -> LookupResult<String> {
    lookup_str(st, "F", n)
}

fn lookup_percent_emitted(st: &mut KwState, n: u32) -> LookupResult<u32> {
    lookup_int(st, "P", n)
}

fn lookup_detector_voltage(st: &mut KwState, n: u32) -> LookupResult<f32> {
    lookup_float(st, "V", n)
}

fn lookup_detector(st: &mut KwState, n: u32) -> LookupResult<String> {
    lookup_str(st, "DET", n)
}

fn lookup_tag(st: &mut KwState, n: u32) -> LookupResult<String> {
    lookup_str(st, "TAG", n)
}

fn lookup_analyte(st: &mut KwState, n: u32) -> LookupResult<String> {
    lookup_str(st, "ANALYTE", n)
}

fn lookup_gain(st: &mut KwState, n: u32) -> LookupResult<f32> {
    lookup_float(st, "G", n)
}

fn lookup_scale(st: &mut KwState, n: u32) -> LookupResult<Scale> {
    lookup_param_value(st, "E", n, |s| {
        let v: Vec<&str> = s.split(",").collect();
        match v[..] {
            [ds, os] => match (ds.parse(), os.parse()) {
                (Ok(0.0), Ok(0.0)) => Ok(Linear),
                (Ok(decades), Ok(offset)) => Ok(Log(LogScale { decades, offset })),
                _ => Err("invalid floats"),
            },
            _ => Err("too many fields"),
        }
    })
}

fn lookup_calibration(st: &mut KwState, n: u32) -> LookupResult<Calibration> {
    lookup_param_value(st, "CALIBRATION", n, |s| {
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
fn lookup_wavelengths(st: &mut KwState, n: u32) -> LookupResult<Vec<u32>> {
    lookup_param_value(st, "L", n, |s| {
        let mut ws = vec![];
        for x in s.split(",") {
            match x.parse() {
                Ok(y) => ws.push(y),
                _ => return Err("invalid float encountered"),
            };
        }
        return Ok(ws);
    })
}

fn lookup_display(st: &mut KwState, n: u32) -> LookupResult<Display> {
    lookup_param_value(st, "D", n, |s| {
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

fn lookup_datatype(st: &mut KwState, n: u32) -> LookupResult<NumTypes> {
    lookup_param_value(st, "DATATYPE", n, |s| match s {
        "I" => Ok(NumTypes::Integer),
        "F" => Ok(NumTypes::Float),
        "D" => Ok(NumTypes::Double),
        _ => Err("unknown datatype"),
    })
}

fn lookup_type(st: &mut KwState, n: u32) -> LookupResult<MeasurementType> {
    lookup_param_value(st, "TYPE", n, |s| match s {
        "Forward Scatter" => Ok(MeasurementType::ForwardScatter),
        "Raw Fluorescence" => Ok(MeasurementType::RawFluorescence),
        "Mass" => Ok(MeasurementType::Mass),
        "Time" => Ok(MeasurementType::Time),
        "Index" => Ok(MeasurementType::Index),
        "Classification" => Ok(MeasurementType::Classification),
        _ => Err("unknown measurement type"),
    })
}

fn lookup_feature(st: &mut KwState, n: u32) -> LookupResult<Feature> {
    lookup_param_value(st, "FEATURE", n, |s| match s {
        "Area" => Ok(Feature::Area),
        "Width" => Ok(Feature::Width),
        "Height" => Ok(Feature::Height),
        _ => Err("unknown parameter feature"),
    })
}

// TODO maybe a better way to write all this with macros, since lots of things
// are repeated
// TODO one way to clean this up might be to put the keywords and errors vector
// into a "state blob" and then let all the lookup functions add to the error
// part if they encounter an error. Then on failure they return an option (or
// something similar) which could then be filtered to determine if a value is
// found
impl ParameterFromKeywords for Parameter2_0 {
    fn build_parameter(st: &mut KwState, n: u32, bits: u32) -> Option<Parameter2_0> {
        let pnr = lookup_range(st, n).required(st);
        let pne = lookup_scale(st, n).optional();
        let pnn = lookup_shortname(st, n).optional();
        let pns = lookup_longname(st, n).optional();
        let pnf = lookup_filter(st, n).optional();
        let pnl = lookup_wavelength(st, n).optional();
        let pno = lookup_power(st, n).optional();
        let pnt = lookup_detector_type(st, n).optional();
        let pnp = lookup_percent_emitted(st, n).optional();
        let pnv = lookup_detector_voltage(st, n).optional();
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
        let pnr = lookup_range(st, n).required(st);
        let pne = lookup_scale(st, n).required(st);
        let pnn = lookup_shortname(st, n).optional();
        let pns = lookup_longname(st, n).optional();
        let pnf = lookup_filter(st, n).optional();
        let pnl = lookup_wavelength(st, n).optional();
        let pno = lookup_power(st, n).optional();
        let pnt = lookup_detector_type(st, n).optional();
        let pnp = lookup_percent_emitted(st, n).optional();
        let pnv = lookup_detector_voltage(st, n).optional();
        let png = lookup_gain(st, n).optional();
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
        let pnr = lookup_range(st, n).required(st);
        let pne = lookup_scale(st, n).required(st);
        let pnn = lookup_shortname(st, n).required(st);
        let pns = lookup_longname(st, n).optional();
        let pnf = lookup_filter(st, n).optional();
        let pnl = lookup_wavelengths(st, n).to_vector();
        let pno = lookup_power(st, n).optional();
        let pnt = lookup_detector_type(st, n).optional();
        let pnp = lookup_percent_emitted(st, n).optional();
        let pnv = lookup_detector_voltage(st, n).optional();
        let png = lookup_gain(st, n).optional();
        let pncal = lookup_calibration(st, n).optional();
        let pnd = lookup_display(st, n).optional();
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
        let pnr = lookup_range(st, n).required(st);
        let pne = lookup_scale(st, n).required(st);
        let pnn = lookup_shortname(st, n).required(st);
        let pns = lookup_longname(st, n).optional();
        let pnf = lookup_filter(st, n).optional();
        let pnl = lookup_wavelengths(st, n).to_vector();
        let pno = lookup_power(st, n).optional();
        let pnt = lookup_detector_type(st, n).optional();
        let pnp = lookup_percent_emitted(st, n).optional();
        let pnv = lookup_detector_voltage(st, n).optional();
        let png = lookup_gain(st, n).optional();
        let pncal = lookup_calibration(st, n).optional();
        let pnd = lookup_display(st, n).optional();
        let pndt = lookup_datatype(st, n).optional();
        let pndet = lookup_detector(st, n).optional();
        let pntag = lookup_tag(st, n).optional();
        let pntype = lookup_type(st, n).optional();
        let pnfeature = lookup_feature(st, n).optional();
        let pnanalyte = lookup_analyte(st, n).optional();
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

// all hail the almighty state monad :D
struct KwState {
    keywords: Keywords,
    errors: KeywordErrors,
    missing: MissingKeywords,
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

// TODO gating parameters not added (yet)

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
use itertools::Itertools;
use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::io;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::str;

fn format_standard_kw(kw: &str) -> String {
    format!("${}", kw.to_ascii_uppercase())
}

fn format_param(n: u32, param: &str) -> String {
    format!("P{}{}", n, param.to_ascii_uppercase())
}

fn parse_endian(s: &str) -> Result<Endian, &'static str> {
    match s {
        "1,2,3,4" => Ok(Endian::Little),
        "4,3,2,1" => Ok(Endian::Big),
        _ => Err("could not determine endianness, must be '1,2,3,4' or '4,3,2,1'"),
    }
}

// TODO is this always allowed?
fn parse_int(s: &str) -> Result<u32, &'static str> {
    s.trim().parse().or(Err("invalid integer"))
}

fn parse_int_or_blank(s: &str) -> Result<u32, &'static str> {
    if s.trim().is_empty() {
        Ok(0)
    } else {
        s.parse().or(Err("invalid integer and not a blank"))
    }
}

fn parse_float(s: &str) -> Result<f32, &'static str> {
    s.parse().or(Err("invalid float"))
}

fn parse_str(s: &str) -> Result<String, &'static str> {
    Ok(String::from(s))
}

fn parse_iso_datetime(s: &str) -> Result<DateTime<FixedOffset>, &'static str> {
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
    Err("must be formatted like 'yyyy-mm-ddThh:mm:ss[TZD]'")
}

fn parse_date(s: &str) -> Result<NaiveDate, &'static str> {
    NaiveDate::parse_from_str(s, "%d-%b-%Y")
        .or(NaiveDate::parse_from_str(s, "%d-%b-%Y")
            .or(Err("must be formatted like 'dd-mmm-yyyy'")))
}

fn parse_time60(s: &str) -> Result<NaiveTime, &'static str> {
    // TODO this will have subseconds in terms of 1/100, need to convert to 1/60
    parse_time100(s)
}

fn parse_time100(s: &str) -> Result<NaiveTime, &'static str> {
    NaiveTime::parse_from_str(s, "%H:%M:%S.%.3f")
        .or(NaiveTime::parse_from_str(s, "%H:%M:%S")
            .or(Err("must be formatted like 'hh:mm:ss[.cc]'")))
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

#[derive(Debug)]
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

    fn num_bytes(&self) -> u32 {
        self.end - self.begin + 1
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

#[derive(Debug)]
enum AlphaNumTypes {
    Ascii,
    Integer,
    Float,
    Double,
}

#[derive(Debug)]
enum NumTypes {
    Integer,
    Float,
    Double,
}

#[derive(Debug)]
enum Endian {
    Big,
    Little,
}

// TODO this can vary depending on bit width
#[derive(Debug)]
enum ByteOrd {
    BigLittle(Endian),
    Mixed([u8; 4]),
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

impl<V> OptionalKw<V> {
    fn to_option(self) -> Option<V> {
        match self {
            OptionalKw::Present(x) => Some(x),
            OptionalKw::Absent => None,
        }
    }

    fn from_option(x: Option<V>) -> Self {
        x.map_or_else(|| OptionalKw::Absent, |y| OptionalKw::Present(y))
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
    datatype: OptionalKw<NumTypes>,
}

#[derive(Debug)]
struct Parameter<X> {
    bits: u32,                         // PnB
    range: u32,                        // PnR
    longname: OptionalKw<String>,      // PnS
    filter: OptionalKw<String>,        // PnF
    power: OptionalKw<u32>,            // PnO
    detector_type: OptionalKw<String>, // PnD
    percent_emitted: OptionalKw<u32>,  // PnP (TODO deprecated in 3.2, factor out)
    detector_voltage: OptionalKw<f32>, // PnV
    specific: X,
}

// type Wavelength2_0 = Option<u32>;
// type Wavelength3_1 = Vec<u32>;

trait ParameterFromKeywords: Sized {
    fn build_inner(st: &mut KwState, n: u32) -> Option<Self>;

    fn from_kws(st: &mut KwState, par: u32) -> Option<Vec<Parameter<Self>>> {
        let mut ps = vec![];
        for n in 1..(par + 1) {
            if let (Some(bits), Some(range), Some(specific)) = (
                st.lookup_param_bits(n),
                st.lookup_param_range(n),
                Self::build_inner(st, n),
            ) {
                let p = Parameter {
                    bits,
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
    fn build_inner(st: &mut KwState, n: u32) -> Option<InnerParameter2_0> {
        Some(InnerParameter2_0 {
            scale: st.lookup_param_scale_opt(n),
            shortname: st.lookup_param_shortname_opt(n),
            wavelength: st.lookup_param_wavelength(n),
        })
    }
}

impl ParameterFromKeywords for InnerParameter3_0 {
    fn build_inner(st: &mut KwState, n: u32) -> Option<InnerParameter3_0> {
        if let Some(scale) = st.lookup_param_scale_req(n) {
            Some(InnerParameter3_0 {
                scale,
                shortname: st.lookup_param_shortname_opt(n),
                wavelength: st.lookup_param_wavelength(n),
                gain: st.lookup_param_gain(n),
            })
        } else {
            None
        }
    }
}

impl ParameterFromKeywords for InnerParameter3_1 {
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
}

#[derive(Debug)]
struct Metadata<X> {
    par: u32,
    nextdata: u32,
    datatype: AlphaNumTypes,
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
    metadata: Metadata<M>,
    parameters: Vec<Parameter<P>>,
}

impl<M: MetadataFromKeywords, P: ParameterFromKeywords> StdText<M, P> {
    fn validate(s: &Self) -> Vec<String> {
        let mut meta_errors = vec![];
        check_bits(s, &mut meta_errors);
        meta_errors
    }

    fn from_kws(raw: RawTEXT) -> Result<TEXT<Self>, StandardErrors> {
        let mut st = raw.to_state();
        if let Some(s) = M::from_kws(&mut st).and_then(|metadata| {
            P::from_kws(&mut st, metadata.par).map(|parameters| StdText {
                metadata,
                parameters,
            })
        }) {
            let meta_errors = Self::validate(&s);
            if meta_errors.is_empty() {
                Ok(st.finalize(s))
            } else {
                Err(StandardErrors {
                    meta_errors,
                    value_errors: HashMap::new(),
                    missing_keywords: vec![],
                })
            }
        } else {
            Err(st.pull_errors())
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

trait MetadataFromKeywords: Sized {
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

impl MetadataFromKeywords for InnerMetadata2_0 {
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
            })
        } else {
            None
        }
    }
}

// trait StdTextFromKeywords: Sized {
//     type M: MetadataFromKeywords;
//     type P: ParameterFromKeywords;

//     fn build(m: Metadata<Self::M>, p: Vec<Parameter<Self::P>>) -> Self;

//     fn validate(s: &Self) -> Vec<String>;

//     fn from_kws(raw: RawTEXT) -> Result<TEXT<Self>, StandardErrors> {
//         let mut st = raw.to_state();
//         if let Some(s) = Self::M::from_kws(&mut st).map(|m| {
//             let ps = Self::P::from_kws(&mut st, m.par);
//             Self::build(m, ps)
//         }) {
//             let meta_errors = Self::validate(&s);
//             if meta_errors.is_empty() {
//                 Ok(st.finalize(s))
//             } else {
//                 Err(StandardErrors {
//                     meta_errors,
//                     value_errors: HashMap::new(),
//                     missing_keywords: vec![],
//                 })
//             }
//         } else {
//             Err(st.pull_errors())
//         }
//     }
// }

fn check_bits<M, P>(s: &StdText<M, P>, meta_errors: &mut Vec<String>) -> () {
    // Check that all bit fields match DATATYPE
    match s.metadata.datatype {
        AlphaNumTypes::Float => {
            for (i, p) in s.parameters.iter().enumerate() {
                if p.bits != 32 {
                    meta_errors.push(format!(
                        "Parameter {} uses {} bits when DATATYPE=F",
                        i + 1,
                        p.bits
                    ));
                }
            }
        }
        AlphaNumTypes::Double => {
            for (i, p) in s.parameters.iter().enumerate() {
                if p.bits != 64 {
                    meta_errors.push(format!(
                        "Parameter {} uses {} bits when DATATYPE=D",
                        i + 1,
                        p.bits
                    ));
                }
            }
        }
        _ => (),
    }
}

// TODO not DRY, this is repeated 4x (make a macro, this isn't Haskell)
// impl StdTextFromKeywords for StdText2_0 {
//     type M = InnerMetadata2_0;
//     type P = InnerParameter2_0;

//     fn validate(s: &Self) -> Vec<String> {
//         let mut meta_errors = vec![];
//         check_bits(s, &mut meta_errors);
//         meta_errors
//     }

//     fn build(metadata: Metadata<Self::M>, parameters: Vec<Parameter<Self::P>>) -> Self {
//         StdText {
//             metadata,
//             parameters,
//         }
//     }
// }

// impl StdTextFromKeywords for StdText3_0 {
//     type M = InnerMetadata3_0;
//     type P = InnerParameter3_0;

//     fn validate(s: &Self) -> Vec<String> {
//         let mut meta_errors = vec![];
//         check_bits(s, &mut meta_errors);
//         meta_errors
//     }

//     fn build(metadata: Metadata<Self::M>, parameters: Vec<Parameter<Self::P>>) -> Self {
//         StdText {
//             metadata,
//             parameters,
//         }
//     }
// }

// impl StdTextFromKeywords for StdText3_1 {
//     type M = InnerMetadata3_1;
//     type P = InnerParameter3_1;

//     fn validate(s: &Self) -> Vec<String> {
//         let mut meta_errors = vec![];
//         check_bits(s, &mut meta_errors);
//         meta_errors
//     }

//     fn build(metadata: Metadata<Self::M>, parameters: Vec<Parameter<Self::P>>) -> Self {
//         StdText {
//             metadata,
//             parameters,
//         }
//     }
// }

// impl StdTextFromKeywords for StdText3_2 {
//     type M = InnerMetadata3_2;
//     type P = InnerParameter3_2;

//     fn validate(s: &Self) -> Vec<String> {
//         let mut meta_errors = vec![];
//         check_bits(s, &mut meta_errors);
//         meta_errors
//     }

//     fn build(metadata: Metadata<Self::M>, parameters: Vec<Parameter<Self::P>>) -> Self {
//         StdText {
//             metadata,
//             parameters,
//         }
//     }
// }

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
    fn from_kws(v: Version, raw: RawTEXT) -> Result<Self, StandardErrors> {
        match v {
            Version::FCS2_0 => StdText::from_kws(raw).map(AnyTEXT::TEXT2_0),
            Version::FCS3_0 => StdText::from_kws(raw).map(AnyTEXT::TEXT3_0),
            Version::FCS3_1 => StdText::from_kws(raw).map(AnyTEXT::TEXT3_1),
            Version::FCS3_2 => StdText::from_kws(raw).map(AnyTEXT::TEXT3_2),
        }
    }
}

type Keywords = HashMap<String, String>;
type KeywordErrors = HashMap<String, (String, String)>;
type MissingKeywords = HashSet<String>;

#[derive(Debug)]
struct KwError {
    value: String,
    msg: &'static str,
}

enum ValueStatus {
    Raw,
    Error(&'static str),
    Warning(&'static str),
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

impl KwState {
    // TODO format $param here
    // TODO not DRY (although will likely need HKTs)
    fn get_required<V, F>(&mut self, k: &str, f: F) -> Option<V>
    where
        F: FnOnce(&str) -> Result<V, &'static str>,
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
        F: FnOnce(&str) -> Result<V, &'static str>,
    {
        let sk = format_standard_kw(k);
        match self.keywords.get_mut(&sk) {
            Some(v) => match v.status {
                ValueStatus::Raw => {
                    let (s, r) = f(&v.value).map_or_else(
                        |w| (ValueStatus::Warning(w), OptionalKw::Absent),
                        |x| (ValueStatus::Used, OptionalKw::Present(x)),
                    );
                    v.status = s;
                    r
                }
                _ => OptionalKw::Absent,
            },
            None => OptionalKw::Absent,
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
        self.get_required("BEGINDATA", parse_int)
    }

    fn lookup_enddata(&mut self) -> Option<u32> {
        self.get_required("ENDDATA", parse_int)
    }

    fn lookup_data_offsets(&mut self) -> Option<Offsets> {
        if let (Some(begin), Some(end)) = (self.lookup_begindata(), self.lookup_enddata()) {
            self.build_offsets(begin, end, "DATA")
        } else {
            None
        }
    }

    fn lookup_supplemental3_0(&mut self) -> Option<SupplementalOffsets3_0> {
        if let (Some(beginstext), Some(endstext), Some(beginanalysis), Some(endanalysis)) = (
            self.get_required("BEGINSTEXT", parse_int),
            self.get_required("ENDSTEXT", parse_int),
            self.get_required("BEGINANALYSIS", parse_int),
            self.get_required("ENDANALYSIS", parse_int),
        ) {
            if let (Some(stext), Some(analysis)) = (
                self.build_offsets(beginstext, endstext, "STEXT"),
                self.build_offsets(beginanalysis, endanalysis, "ANALYSIS"),
            ) {
                Some(SupplementalOffsets3_0 { stext, analysis })
            } else {
                None
            }
        } else {
            None
        }
    }

    fn lookup_supplemental3_2(&mut self) -> SupplementalOffsets3_2 {
        if let (Some(beginstext), Some(endstext), Some(beginanalysis), Some(endanalysis)) = (
            self.get_required("BEGINSTEXT", parse_int),
            self.get_required("ENDSTEXT", parse_int),
            self.get_required("BEGINANALYSIS", parse_int),
            self.get_required("ENDANALYSIS", parse_int),
        ) {
            let stext = OptionalKw::from_option(self.build_offsets(beginstext, endstext, "STEXT"));
            let analysis =
                OptionalKw::from_option(self.build_offsets(beginanalysis, endanalysis, "ANALYSIS"));
            SupplementalOffsets3_2 { stext, analysis }
        } else {
            SupplementalOffsets3_2 {
                stext: OptionalKw::Absent,
                analysis: OptionalKw::Absent,
            }
        }
    }

    fn lookup_byteord(&mut self) -> Option<ByteOrd> {
        self.get_required("BYTEORD", |s| match parse_endian(s) {
            Ok(e) => Ok(ByteOrd::BigLittle(e)),
            _ => {
                // ASSUME this will not fail because the regexp will only match
                // four integers within {1,2,3,4}. If the regexp matches, the
                // only thing left to test is that each of the digits is unique.
                let re = Regex::new(r"^([1-4]),([1-4]),([1-4]),([1-4])$").unwrap();
                if let Some(cap) = re.captures(s) {
                    let xs: [u8; 4] = cap.extract().1.map(|s| s.parse().unwrap());
                    let mut flags = [false, false, false, false];
                    for x in xs {
                        flags[usize::from(x) - 1] = true;
                    }
                    if flags.iter().all(|x| *x) {
                        Ok(ByteOrd::Mixed(xs))
                    } else {
                        Err("mixed byte order not unique")
                    }
                } else {
                    Err("invalid mixed byte order format")
                }
            }
        })
    }

    fn lookup_endian(&mut self) -> Option<Endian> {
        self.get_required("BYTEORD", parse_endian)
    }

    fn lookup_datatype(&mut self) -> Option<AlphaNumTypes> {
        self.get_required("DATATYPE", |s| match s {
            "I" => Ok(AlphaNumTypes::Integer),
            "F" => Ok(AlphaNumTypes::Float),
            "D" => Ok(AlphaNumTypes::Double),
            "A" => Ok(AlphaNumTypes::Ascii),
            _ => Err("unknown datatype"),
        })
    }

    fn lookup_mode(&mut self) -> Option<Mode> {
        self.get_required("MODE", |s| match s {
            "C" => Ok(Mode::Correlated),
            "L" => Ok(Mode::List),
            "U" => Ok(Mode::Uncorrelated),
            _ => Err("unknown mode"),
        })
    }

    fn lookup_mode3_2(&mut self) -> Option<Mode> {
        self.get_required("MODE", |s| match s {
            "L" => Ok(Mode::List),
            _ => Err("unknown mode (U and C are no longer valid)"),
        })
    }

    fn lookup_nextdata(&mut self) -> Option<u32> {
        self.get_required("NEXTDATA", parse_int)
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
            _ => Err("wrong number of fields"),
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

    fn lookup_unicode(&mut self) -> OptionalKw<Unicode> {
        self.get_optional("UNICODE", |s| {
            let mut xs = s.split(",");
            if let Some(page) = xs.next().and_then(|s| s.parse().ok()) {
                let kws: Vec<String> = xs.map(String::from).collect();
                if kws.is_empty() {
                    Err("no keywords specified")
                } else {
                    Ok(Unicode { page, kws })
                }
            } else {
                Err("unicode must be like 'page,KW1[,KW2...]'")
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
                        if let Some(v) = rest[i + 8].parse().ok() {
                            us.insert(String::from(rest[i]), v);
                        } else {
                            return Err("invalid numeric encountered");
                        }
                    }
                    Ok(us)
                } else {
                    Err("data fields do not match given dimensions")
                }
            } else {
                Err("invalid dimension")
            }
        })
    }

    fn lookup_last_modifier(&mut self) -> OptionalKw<String> {
        self.get_optional("LAST_MODIFIER", parse_str)
    }

    fn lookup_last_modified(&mut self) -> OptionalKw<NaiveDateTime> {
        // TODO hopefully case doesn't matter...
        self.get_optional("LAST_MODIFIED", |s| {
            NaiveDateTime::parse_from_str(s, "%d-%b-%Y %H:%M:%S.%.3f")
                .or(NaiveDateTime::parse_from_str(s, "%d-%b-%Y %H:%M:%S")
                    .or(Err("must be formatted like 'dd-mmm-yyyy hh:mm:ss[.cc]'")))
        })
    }

    fn lookup_originality(&mut self) -> OptionalKw<Originality> {
        self.get_optional("ORIGINALITY", |s| match s {
            "Original" => Ok(Originality::Original),
            "NonDataModified" => Ok(Originality::NonDataModified),
            "Appended" => Ok(Originality::Appended),
            "DataModified" => Ok(Originality::DataModified),
            _ => Err("invalid originality"),
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
        F: FnOnce(&str) -> Result<V, &'static str>,
    {
        self.get_required(&format_param(n, param), f)
    }

    fn lookup_param_opt<V, F>(&mut self, param: &'static str, n: u32, f: F) -> OptionalKw<V>
    where
        F: FnOnce(&str) -> Result<V, &'static str>,
    {
        self.get_optional(&format_param(n, param), f)
    }

    // TODO check that this is in multiples of 8 for relevant specs
    fn lookup_param_bits(&mut self, n: u32) -> Option<u32> {
        self.lookup_param_req("B", n, parse_int)
    }

    fn lookup_param_range(&mut self, n: u32) -> Option<u32> {
        self.lookup_param_req("R", n, parse_int)
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
                    _ => Err("invalid (positive) float"),
                },
                _ => Err("too many fields"),
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
                    _ => return Err("invalid float encountered"),
                };
            }
            return Ok(ws);
        })
        .to_option()
        .unwrap_or(vec![])
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
                    _ => Err("invalid floats"),
                },
                _ => Err("too many fields"),
            }
        })
    }

    fn lookup_param_datatype(&mut self, n: u32) -> OptionalKw<NumTypes> {
        self.lookup_param_opt("DATATYPE", n, |s| match s {
            "I" => Ok(NumTypes::Integer),
            "F" => Ok(NumTypes::Float),
            "D" => Ok(NumTypes::Double),
            _ => Err("unknown datatype"),
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
            _ => Err("unknown measurement type"),
        })
    }

    fn lookup_param_feature(&mut self, n: u32) -> OptionalKw<Feature> {
        self.lookup_param_opt("FEATURE", n, |s| match s {
            "Area" => Ok(Feature::Area),
            "Width" => Ok(Feature::Width),
            "Height" => Ok(Feature::Height),
            _ => Err("unknown parameter feature"),
        })
    }

    fn pull_errors(self) -> StandardErrors {
        let mut value_errors: HashMap<String, KwError> = HashMap::new();
        for (k, v) in self.keywords {
            // TODO lots of clones here, not sure if this is necessary to fix
            match v.status {
                ValueStatus::Error(e) => {
                    value_errors.insert(
                        k,
                        KwError {
                            value: v.value,
                            msg: e,
                        },
                    );
                }
                _ => (),
            }
        }
        StandardErrors {
            missing_keywords: self.missing,
            value_errors,
            meta_errors: vec![],
        }
    }

    // ASSUME There aren't any errors recorded in the state hashtable since we
    // don't check them here. This function requires a standardized keyword
    // struct to run, and presumably this won't exist if there are any errors.
    fn finalize<S>(self, standard: S) -> TEXT<S> {
        // TODO this struct shouldn't touch any nonstandard keywords, since it
        // isn't supposed to do anything useful with them
        let mut nonstandard = HashMap::new();
        let mut deviant = HashMap::new();
        for (k, v) in self.keywords {
            match v.status {
                ValueStatus::Raw => {
                    if k.starts_with("$") {
                        deviant.insert(k, v.value);
                    } else {
                        nonstandard.insert(k, v.value);
                    }
                }
                _ => (),
            }
        }
        TEXT {
            standard,
            nonstandard,
            deviant,
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
    } else {
        if allow_blank {
            Err("could not make bounds from integers/blanks")
        } else {
            Err("could not make bounds from integers")
        }
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
    h.read(&mut verbuf)?;
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
    h.read(&mut dbuf)?;
    let delimiter = dbuf[0];

    // Valid delimiters are in the set of {1..126}
    if delimiter < 1 || delimiter > 126 {
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
    h.read(&mut dbuf)?;
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
        .map(|x| match x {
            [a, b] => Some((*a, b - a)),
            _ => None,
        })
        .flatten()
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
    let stext = AnyTEXT::from_kws(header.version, text);
    println!("{:#?}", stext);
}

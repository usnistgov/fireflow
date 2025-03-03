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

fn format_kw(kw: &str) -> String {
    format!("${}", kw.to_ascii_uppercase())
}

fn format_param_kw(n: u32, param: &str) -> String {
    format_kw(&format!("P{}{}", n, param.to_ascii_uppercase()))
}

fn parse_endian(s: &str) -> Result<Endian, &'static str> {
    match s {
        "1,2,3,4" => Ok(Endian::Little),
        "4,3,2,1" => Ok(Endian::Big),
        _ => Err("could not determine endianness, must be '1,2,3,4' or '4,3,2,1'"),
    }
}

fn parse_int(s: &str) -> Result<u32, &'static str> {
    s.parse().or(Err("invalid integer"))
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
    DateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f%:z").or(DateTime::parse_from_str(
        s,
        "%Y-%m-%dT%H:%M:%S%.f%:z",
    )
    .or(Err("must be formatted like 'yyyy-mm-ddThh:mm:ss[TZD]'")))
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

enum Endian {
    Big,
    Little,
}

// TODO this can vary depending on bit width
enum ByteOrd {
    BigLittle(Endian),
    Mixed([u8; 4]),
}

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

struct Timestamps2_0 {
    btim: Option<NaiveTime>,
    etim: Option<NaiveTime>,
    date: Option<NaiveDate>,
}

struct Timestamps3_2 {
    start: Option<DateTime<FixedOffset>>,
    end: Option<DateTime<FixedOffset>>,
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

struct InnerParameter2_0 {
    scale: Option<Scale>,      // PnE
    wavelength: Option<u32>,   // PnL
    shortname: Option<String>, // PnN
}

struct InnerParameter3_0 {
    scale: Scale,              // PnE
    wavelength: Option<u32>,   // PnL
    shortname: Option<String>, // PnN
    gain: Option<f32>,         // PnG
}

struct InnerParameter3_1 {
    scale: Scale,         // PnE
    wavelength: Vec<u32>, // PnL
    shortname: String,    // PnN
    gain: Option<f32>,    // PnG
    calibration: Option<Calibration>,
    display: Option<Display>,
}

struct InnerParameter3_2 {
    scale: Scale,         // PnE
    wavelength: Vec<u32>, // PnL
    shortname: String,    // PnN
    gain: Option<f32>,    // PnG
    calibration: Option<Calibration>,
    display: Option<Display>,
    analyte: Option<String>,
    feature: Option<Feature>,
    measurement_type: Option<MeasurementType>,
    tag: Option<String>,
    detector_name: Option<String>,
    datatype: Option<NumTypes>,
}

struct Parameter<X> {
    bits: u32,                     // PnB
    range: u32,                    // PnR
    longname: Option<String>,      // PnS
    filter: Option<String>,        // PnF
    power: Option<u32>,            // PnO
    detector_type: Option<String>, // PnD
    percent_emitted: Option<u32>,  // PnP (TODO deprecated in 3.2, factor out)
    detector_voltage: Option<f32>, // PnV
    specific: X,
}

// type Wavelength2_0 = Option<u32>;
// type Wavelength3_1 = Vec<u32>;

trait ParameterFromKeywords: Sized {
    fn build_inner(st: &mut KwState, n: u32) -> Option<Self>;

    // TODO this should be non-empty
    fn from_kws(st: &mut KwState, par: u32) -> Option<Vec<Parameter<Self>>> {
        let mut ps = vec![];
        for n in 0..par {
            if let (
                Some(bits),
                Some(range),
                Some(longname),
                Some(filter),
                Some(power),
                Some(detector_type),
                Some(percent_emitted),
                Some(detector_voltage),
                Some(specific),
            ) = (
                st.lookup_param_bits(n),
                st.lookup_param_range(n),
                st.lookup_param_longname(n),
                st.lookup_param_filter(n),
                st.lookup_param_power(n),
                st.lookup_param_detector_type(n),
                st.lookup_param_percent_emitted(n),
                st.lookup_param_detector_voltage(n),
                Self::build_inner(st, n),
            ) {
                let p = Parameter {
                    bits,
                    range,
                    longname,
                    filter,
                    power,
                    detector_type,
                    percent_emitted,
                    detector_voltage,
                    specific,
                };
                ps.push(p);
            }
        }
        if ps.is_empty() {
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
        if let (Some(scale), Some(shortname), Some(wavelength)) = (
            st.lookup_param_scale_opt(n),
            st.lookup_param_shortname_opt(n),
            st.lookup_param_wavelength(n),
        ) {
            Some(InnerParameter2_0 {
                scale,
                shortname,
                wavelength,
            })
        } else {
            None
        }
    }
}

impl ParameterFromKeywords for InnerParameter3_0 {
    fn build_inner(st: &mut KwState, n: u32) -> Option<InnerParameter3_0> {
        if let (Some(scale), Some(shortname), Some(wavelength), Some(gain)) = (
            st.lookup_param_scale_req(n),
            st.lookup_param_shortname_opt(n),
            st.lookup_param_wavelength(n),
            st.lookup_param_gain(n),
        ) {
            Some(InnerParameter3_0 {
                gain,
                scale,
                shortname,
                wavelength,
            })
        } else {
            None
        }
    }
}

impl ParameterFromKeywords for InnerParameter3_1 {
    fn build_inner(st: &mut KwState, n: u32) -> Option<InnerParameter3_1> {
        if let (
            Some(scale),
            Some(shortname),
            Some(wavelength),
            Some(gain),
            Some(calibration),
            Some(display),
        ) = (
            st.lookup_param_scale_req(n),
            st.lookup_param_shortname_req(n),
            st.lookup_param_wavelengths(n),
            st.lookup_param_gain(n),
            st.lookup_param_calibration(n),
            st.lookup_param_display(n),
        ) {
            Some(InnerParameter3_1 {
                calibration,
                scale,
                display,
                wavelength,
                shortname,
                gain,
            })
        } else {
            None
        }
    }
}

impl ParameterFromKeywords for InnerParameter3_2 {
    fn build_inner(st: &mut KwState, n: u32) -> Option<InnerParameter3_2> {
        if let (
            Some(scale),
            Some(shortname),
            Some(wavelength),
            Some(gain),
            Some(calibration),
            Some(display),
            Some(datatype),
            Some(detector_name),
            Some(tag),
            Some(measurement_type),
            Some(feature),
            Some(analyte),
        ) = (
            st.lookup_param_scale_req(n),
            st.lookup_param_shortname_req(n),
            st.lookup_param_wavelengths(n),
            st.lookup_param_gain(n),
            st.lookup_param_calibration(n),
            st.lookup_param_display(n),
            st.lookup_param_datatype(n),
            st.lookup_param_detector(n),
            st.lookup_param_tag(n),
            st.lookup_param_type(n),
            st.lookup_param_feature(n),
            st.lookup_param_analyte(n),
        ) {
            Some(InnerParameter3_2 {
                scale,
                shortname,
                wavelength,
                datatype,
                detector_name,
                tag,
                measurement_type,
                feature,
                analyte,
                calibration,
                display,
                gain,
            })
        } else {
            None
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
    last_modified: Option<NaiveDateTime>,
    originality: Option<Originality>,
}

struct PlateData {
    plateid: Option<String>,
    platename: Option<String>,
    wellid: Option<String>,
}

type UnstainedCenters = HashMap<String, f32>;

struct UnstainedData {
    unstainedcenters: Option<UnstainedCenters>,
    unstainedinfo: Option<String>,
}

struct CarrierData {
    carrierid: Option<String>,
    carriertype: Option<String>,
    locationid: Option<String>,
}

struct Unicode {
    page: u32,
    kws: Vec<String>,
}

struct InnerMetadata2_0 {
    tot: Option<u32>,
    mode: Mode,
    byteord: ByteOrd,
    cyt: Option<String>,
    timestamps: Timestamps2_0, // BTIM/ETIM/DATE
}

struct InnerMetadata3_0 {
    tot: u32,
    mode: Mode,
    byteord: ByteOrd,
    timestamps: Timestamps2_0, // BTIM/ETIM/DATE
    cyt: Option<String>,
    cytsn: Option<String>,
    timestep: Option<f32>,
    unicode: Option<Unicode>,
}

struct InnerMetadata3_1 {
    tot: u32,
    mode: Mode,
    byteord: Endian,
    timestamps: Timestamps2_0, // BTIM/ETIM/DATE
    cyt: Option<String>,
    cytsn: Option<String>,
    timestep: Option<f32>,
    modification: ModificationData,
    plate: PlateData,
    vol: Option<f32>,
}

struct InnerMetadata3_2 {
    tot: u32,
    byteord: Endian,
    timestamps: Timestamps2_0, // BTIM/ETIM/DATE
    datetimes: Timestamps3_2,  // DATETIMESTART/END
    cyt: String,
    cytsn: Option<String>,
    timestep: Option<f32>,
    modification: ModificationData,
    plate: PlateData,
    vol: Option<f32>,
    carrier: CarrierData,
    unstained: UnstainedData,
}

struct Metadata<X> {
    par: u32,
    nextdata: u32,
    datatype: AlphaNumTypes,
    // an abstraction for various kinds of spillover/comp matrices
    // spillover: Spillover,
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
    specific: X,
}

type Metadata2_0 = Metadata<InnerMetadata2_0>;
type Metadata3_0 = Metadata<InnerMetadata3_0>;
type Metadata3_1 = Metadata<InnerMetadata3_1>;
type Metadata3_2 = Metadata<InnerMetadata3_2>;

struct Spillover {} // TODO, can probably get away with using a matrix for this

struct Cyt(String);

struct Tot(u32);

enum Mode {
    List,
    Uncorrelated,
    Correlated,
}

struct StdText<M, P> {
    metadata: Metadata<M>,
    parameters: Vec<Parameter<P>>,
}

type StdText2_0 = StdText<Metadata2_0, Parameter2_0>;
type StdText3_0 = StdText<Metadata3_0, Parameter3_0>;
type StdText3_1 = StdText<Metadata3_1, Parameter3_1>;
type StdText3_2 = StdText<Metadata3_2, Parameter3_2>;

struct StdTextResult<T> {
    text: T,
    errors: Keywords,
    nonstandard: Keywords,
}

trait MetadataFromKeywords: Sized {
    fn build_inner(st: &mut KwState) -> Option<Self>;

    fn from_kws(st: &mut KwState) -> Option<Metadata<Self>> {
        if let (
            Some(par),
            Some(nextdata),
            Some(datatype),
            Some(abrt),
            Some(com),
            Some(cells),
            Some(exp),
            Some(fil),
            Some(inst),
            Some(lost),
            Some(op),
            Some(proj),
            Some(smno),
            Some(src),
            Some(sys),
            Some(tr),
            Some(specific),
        ) = (
            st.lookup_par(),
            st.lookup_nextdata(),
            st.lookup_datatype(),
            st.lookup_abrt(),
            st.lookup_com(),
            st.lookup_cells(),
            st.lookup_exp(),
            st.lookup_fil(),
            st.lookup_inst(),
            st.lookup_lost(),
            st.lookup_op(),
            st.lookup_proj(),
            st.lookup_smno(),
            st.lookup_src(),
            st.lookup_sys(),
            st.lookup_trigger(),
            Self::build_inner(st),
        ) {
            Some(Metadata {
                par,
                nextdata,
                datatype,
                abrt,
                com,
                cells,
                exp,
                fil,
                inst,
                lost,
                op,
                proj,
                smno,
                src,
                sys,
                tr,
                specific,
            })
        } else {
            None
        }
    }
}

impl MetadataFromKeywords for InnerMetadata2_0 {
    fn build_inner(st: &mut KwState) -> Option<InnerMetadata2_0> {
        if let (Some(tot), Some(mode), Some(byteord), Some(cyt), Some(timestamps)) = (
            st.lookup_tot_opt(),
            st.lookup_mode(),
            st.lookup_byteord(),
            st.lookup_cyt_opt(),
            st.lookup_timestamps2_0(),
        ) {
            Some(InnerMetadata2_0 {
                tot,
                mode,
                byteord,
                cyt,
                timestamps,
            })
        } else {
            None
        }
    }
}

impl MetadataFromKeywords for InnerMetadata3_0 {
    fn build_inner(st: &mut KwState) -> Option<InnerMetadata3_0> {
        if let (
            Some(tot),
            Some(mode),
            Some(byteord),
            Some(cyt),
            Some(timestamps),
            Some(cytsn),
            Some(timestep),
            Some(unicode),
        ) = (
            st.lookup_tot_req(),
            st.lookup_mode(),
            st.lookup_byteord(),
            st.lookup_cyt_opt(),
            st.lookup_timestamps2_0(),
            st.lookup_cytsn(),
            st.lookup_timestep(),
            st.lookup_unicode(),
        ) {
            Some(InnerMetadata3_0 {
                tot,
                mode,
                byteord,
                cyt,
                timestamps,
                cytsn,
                timestep,
                unicode,
            })
        } else {
            None
        }
    }
}

// struct InnerMetadata3_1 {
//     tot: u32,
//     mode: Mode,
//     byteord: Endian,
//     timestamps: Timestamps2_0, // BTIM/ETIM/DATE
//     cyt: Option<String>,
//     cytsn: CytSN,
//     timestep: Timestep,
//     modification: ModificationData,
//     plate: PlateData,
//     vol: Vol,
//     unicode: Unicode,
// }

impl MetadataFromKeywords for InnerMetadata3_1 {
    fn build_inner(st: &mut KwState) -> Option<InnerMetadata3_1> {
        if let (
            Some(tot),
            Some(mode),
            Some(byteord),
            Some(cyt),
            Some(timestamps),
            Some(cytsn),
            Some(timestep),
            Some(modification),
            Some(plate),
            Some(vol),
        ) = (
            st.lookup_tot_req(),
            st.lookup_mode(),
            st.lookup_endian(),
            st.lookup_cyt_opt(),
            st.lookup_timestamps2_0(),
            st.lookup_cytsn(),
            st.lookup_timestep(),
            st.lookup_modification(),
            st.lookup_plate(),
            st.lookup_vol(),
        ) {
            Some(InnerMetadata3_1 {
                tot,
                mode,
                byteord,
                cyt,
                timestamps,
                cytsn,
                timestep,
                modification,
                plate,
                vol,
            })
        } else {
            None
        }
    }
}

impl MetadataFromKeywords for InnerMetadata3_2 {
    fn build_inner(st: &mut KwState) -> Option<InnerMetadata3_2> {
        if let (
            Some(tot),
            Some(byteord),
            Some(cyt),
            Some(timestamps),
            Some(cytsn),
            Some(timestep),
            Some(modification),
            Some(plate),
            Some(vol),
            Some(carrier),
            Some(datetimes),
            Some(unstained),
        ) = (
            st.lookup_tot_req(),
            st.lookup_endian(),
            st.lookup_cyt_req(),
            st.lookup_timestamps2_0(),
            st.lookup_cytsn(),
            st.lookup_timestep(),
            st.lookup_modification(),
            st.lookup_plate(),
            st.lookup_vol(),
            st.lookup_carrier(),
            st.lookup_timestamps3_2(),
            st.lookup_unstained(),
        ) {
            Some(InnerMetadata3_2 {
                tot,
                byteord,
                cyt,
                timestamps,
                cytsn,
                timestep,
                modification,
                plate,
                vol,
                carrier,
                datetimes,
                unstained,
            })
        } else {
            None
        }
    }
}

trait StdTextFromKeywords: Sized {
    type M: MetadataFromKeywords;
    type P: ParameterFromKeywords;

    fn build(m: Metadata<Self::M>, p: Vec<Parameter<Self::P>>) -> Self;

    fn from_kws(st: &mut KwState) -> Option<Self> {
        Self::M::from_kws(st).and_then(|metadata| {
            Self::P::from_kws(st, metadata.par).map(|parameters| Self::build(metadata, parameters))
        })
    }
}

impl<M: MetadataFromKeywords, P: ParameterFromKeywords> StdTextFromKeywords for StdText<M, P> {
    type M = M;
    type P = P;

    fn build(metadata: Metadata<M>, parameters: Vec<Parameter<P>>) -> StdText<M, P> {
        StdText {
            metadata,
            parameters,
        }
    }
}

struct TEXT<S> {
    // TODO add the offsets here as well? offsets are needed before parsing
    // everything else
    standard: S,
    standard_missing: HashSet<String>,
    standard_errors: HashMap<String, KwError>,
    nonstandard: HashMap<String, String>,
    deviant: HashMap<String, String>,
}

enum AnyTEXT {
    TEXT2_0(TEXT<StdText2_0>),
    TEXT3_0(TEXT<StdText3_0>),
    TEXT3_1(TEXT<StdText3_1>),
    TEXT3_2(TEXT<StdText3_2>),
}

type Keywords = HashMap<String, String>;
type KeywordErrors = HashMap<String, (String, String)>;
type MissingKeywords = HashSet<String>;

struct KwError {
    value: String,
    msg: &'static str,
}

enum KwStatus {
    Raw(String),
    Error(KwError),
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
    // TODO format $param here
    fn get_required<V, F>(&mut self, k: &str, f: F) -> Option<V>
    where
        F: FnOnce(&str) -> Result<V, &'static str>,
    {
        match self.keywords.remove(k) {
            Some(KwStatus::Raw(v)) => match f(&v) {
                Ok(x) => Some(x),
                Err(e) => {
                    // TODO string things seems lame
                    self.keywords.insert(
                        String::from(k),
                        KwStatus::Error(KwError { value: v, msg: e }),
                    );
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
                    self.keywords.insert(
                        String::from(k),
                        KwStatus::Error(KwError { value: v, msg: e }),
                    );
                    None
                }
            },
            // silently ignore attempts to process the same keyword twice
            Some(_) => None,
            None => Some(None),
        }
    }

    // metadata

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

    fn lookup_nextdata(&mut self) -> Option<u32> {
        self.get_required("NEXTDATA", parse_int)
    }

    fn lookup_par(&mut self) -> Option<u32> {
        self.get_required("PAR", parse_int)
    }

    fn lookup_tot_req(&mut self) -> Option<u32> {
        self.get_required("TOT", parse_int)
    }

    fn lookup_tot_opt(&mut self) -> Option<Option<u32>> {
        self.get_optional("TOT", parse_int)
    }

    fn lookup_cyt_req(&mut self) -> Option<String> {
        self.get_required("CYT", parse_str)
    }

    fn lookup_cyt_opt(&mut self) -> Option<Option<String>> {
        self.get_optional("CYT", parse_str)
    }

    fn lookup_abrt(&mut self) -> Option<Option<u32>> {
        self.get_optional("ABRT", parse_int)
    }

    fn lookup_cells(&mut self) -> Option<Option<String>> {
        self.get_optional("CELLS", parse_str)
    }

    fn lookup_com(&mut self) -> Option<Option<String>> {
        self.get_optional("COM", parse_str)
    }

    fn lookup_exp(&mut self) -> Option<Option<String>> {
        self.get_optional("EXP", parse_str)
    }

    fn lookup_fil(&mut self) -> Option<Option<String>> {
        self.get_optional("FIL", parse_str)
    }

    fn lookup_inst(&mut self) -> Option<Option<String>> {
        self.get_optional("INST", parse_str)
    }

    fn lookup_lost(&mut self) -> Option<Option<u32>> {
        self.get_optional("LOST", parse_int)
    }

    fn lookup_op(&mut self) -> Option<Option<String>> {
        self.get_optional("OP", parse_str)
    }

    fn lookup_proj(&mut self) -> Option<Option<String>> {
        self.get_optional("PROJ", parse_str)
    }

    fn lookup_smno(&mut self) -> Option<Option<String>> {
        self.get_optional("SMNO", parse_str)
    }

    fn lookup_src(&mut self) -> Option<Option<String>> {
        self.get_optional("SRC", parse_str)
    }

    fn lookup_sys(&mut self) -> Option<Option<String>> {
        self.get_optional("SYS", parse_str)
    }

    fn lookup_trigger(&mut self) -> Option<Option<Trigger>> {
        self.get_optional("TR", |s| match s.split(",").collect::<Vec<&str>>()[..] {
            [p, n1] => parse_int(n1).map(|threshold| Trigger {
                parameter: String::from(p),
                threshold,
            }),
            _ => Err("wrong number of fields"),
        })
    }

    fn lookup_cytsn(&mut self) -> Option<Option<String>> {
        self.get_optional("CYTSN", parse_str)
    }

    fn lookup_timestep(&mut self) -> Option<Option<f32>> {
        self.get_optional("TIMESTEP", parse_float)
    }

    fn lookup_vol(&mut self) -> Option<Option<f32>> {
        self.get_optional("VOL", parse_float)
    }

    fn lookup_unicode(&mut self) -> Option<Option<Unicode>> {
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

    fn lookup_plateid(&mut self) -> Option<Option<String>> {
        self.get_optional("PLATEID", parse_str)
    }

    fn lookup_platename(&mut self) -> Option<Option<String>> {
        self.get_optional("PLATENAME", parse_str)
    }

    fn lookup_wellid(&mut self) -> Option<Option<String>> {
        self.get_optional("WELLID", parse_str)
    }

    fn lookup_unstainedinfo(&mut self) -> Option<Option<String>> {
        self.get_optional("UNSTAINEDINFO", parse_str)
    }

    fn lookup_unstainedcenters(&mut self) -> Option<Option<UnstainedCenters>> {
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

    fn lookup_last_modifier(&mut self) -> Option<Option<String>> {
        self.get_optional("LAST_MODIFIER", parse_str)
    }

    fn lookup_last_modified(&mut self) -> Option<Option<NaiveDateTime>> {
        // TODO hopefully case doesn't matter...
        self.get_optional("LAST_MODIFIED", |s| {
            NaiveDateTime::parse_from_str(s, "%d-%b-%Y %H:%M:%S.%.3f")
                .or(NaiveDateTime::parse_from_str(s, "%d-%b-%Y %H:%M:%S")
                    .or(Err("must be formatted like 'dd-mmm-yyyy hh:mm:ss[.cc]'")))
        })
    }

    fn lookup_originality(&mut self) -> Option<Option<Originality>> {
        self.get_optional("ORIGINALITY", |s| match s {
            "Original" => Ok(Originality::Original),
            "NonDataModified" => Ok(Originality::NonDataModified),
            "Appended" => Ok(Originality::Appended),
            "DataModified" => Ok(Originality::DataModified),
            _ => Err("invalid originality"),
        })
    }

    fn lookup_carrierid(&mut self) -> Option<Option<String>> {
        self.get_optional("CARRIERID", parse_str)
    }

    fn lookup_carriertype(&mut self) -> Option<Option<String>> {
        self.get_optional("CARRIERTYPE", parse_str)
    }

    fn lookup_locationid(&mut self) -> Option<Option<String>> {
        self.get_optional("LOCATIONID", parse_str)
    }

    fn lookup_begindatetime(&mut self) -> Option<Option<DateTime<FixedOffset>>> {
        self.get_optional("BEGINDATETIME", parse_iso_datetime)
    }

    fn lookup_enddatetime(&mut self) -> Option<Option<DateTime<FixedOffset>>> {
        self.get_optional("ENDDATETIME", parse_iso_datetime)
    }

    fn lookup_date(&mut self) -> Option<Option<NaiveDate>> {
        self.get_optional("DATE", parse_date)
    }

    fn lookup_btim60(&mut self) -> Option<Option<NaiveTime>> {
        self.get_optional("BTIM", parse_time60)
    }

    fn lookup_etim60(&mut self) -> Option<Option<NaiveTime>> {
        self.get_optional("ETIM", parse_time60)
    }

    fn lookup_btim100(&mut self) -> Option<Option<NaiveTime>> {
        self.get_optional("BTIM", parse_time100)
    }

    fn lookup_etim100(&mut self) -> Option<Option<NaiveTime>> {
        self.get_optional("ETIM", parse_time100)
    }

    fn lookup_timestamps2_0(&mut self) -> Option<Timestamps2_0> {
        if let (Some(btim), Some(etim), Some(date)) = (
            self.lookup_btim60(),
            self.lookup_etim60(),
            self.lookup_date(),
        ) {
            Some(Timestamps2_0 { btim, etim, date })
        } else {
            None
        }
    }

    fn lookup_timestamps3_2(&mut self) -> Option<Timestamps3_2> {
        if let (Some(start), Some(end)) = (self.lookup_begindatetime(), self.lookup_enddatetime()) {
            Some(Timestamps3_2 { start, end })
        } else {
            None
        }
    }

    fn lookup_modification(&mut self) -> Option<ModificationData> {
        if let (Some(last_modifier), Some(last_modified), Some(originality)) = (
            self.lookup_last_modifier(),
            self.lookup_last_modified(),
            self.lookup_originality(),
        ) {
            Some(ModificationData {
                last_modifier,
                last_modified,
                originality,
            })
        } else {
            None
        }
    }

    fn lookup_plate(&mut self) -> Option<PlateData> {
        if let (Some(plateid), Some(platename), Some(wellid)) = (
            self.lookup_plateid(),
            self.lookup_platename(),
            self.lookup_wellid(),
        ) {
            Some(PlateData {
                wellid,
                platename,
                plateid,
            })
        } else {
            None
        }
    }

    fn lookup_carrier(&mut self) -> Option<CarrierData> {
        if let (Some(locationid), Some(carrierid), Some(carriertype)) = (
            self.lookup_locationid(),
            self.lookup_carrierid(),
            self.lookup_carriertype(),
        ) {
            Some(CarrierData {
                locationid,
                carrierid,
                carriertype,
            })
        } else {
            None
        }
    }

    fn lookup_unstained(&mut self) -> Option<UnstainedData> {
        if let (Some(unstainedcenters), Some(unstainedinfo)) =
            (self.lookup_unstainedcenters(), self.lookup_unstainedinfo())
        {
            Some(UnstainedData {
                unstainedcenters,
                unstainedinfo,
            })
        } else {
            None
        }
    }

    // TODO comp matrices

    // parameters

    fn lookup_param_req<V, F>(&mut self, param: &'static str, n: u32, f: F) -> Option<V>
    where
        F: FnOnce(&str) -> Result<V, &'static str>,
    {
        self.get_required(&format_param_kw(n, param), f)
    }

    fn lookup_param_opt<V, F>(&mut self, param: &'static str, n: u32, f: F) -> Option<Option<V>>
    where
        F: FnOnce(&str) -> Result<V, &'static str>,
    {
        self.get_optional(&format_param_kw(n, param), f)
    }

    // TODO check that this is in multiples of 8 for relevant specs
    fn lookup_param_bits(&mut self, n: u32) -> Option<u32> {
        self.lookup_param_req("B", n, parse_int)
    }

    fn lookup_param_range(&mut self, n: u32) -> Option<u32> {
        self.lookup_param_req("R", n, parse_int)
    }

    fn lookup_param_wavelength(&mut self, n: u32) -> Option<Option<u32>> {
        self.lookup_param_opt("L", n, parse_int)
    }

    fn lookup_param_power(&mut self, n: u32) -> Option<Option<u32>> {
        self.lookup_param_opt("O", n, parse_int)
    }

    fn lookup_param_detector_type(&mut self, n: u32) -> Option<Option<String>> {
        self.lookup_param_opt("T", n, parse_str)
    }

    fn lookup_param_shortname_req(&mut self, n: u32) -> Option<String> {
        self.lookup_param_req("N", n, parse_str)
    }

    fn lookup_param_shortname_opt(&mut self, n: u32) -> Option<Option<String>> {
        self.lookup_param_opt("N", n, parse_str)
    }

    fn lookup_param_longname(&mut self, n: u32) -> Option<Option<String>> {
        self.lookup_param_opt("S", n, parse_str)
    }

    fn lookup_param_filter(&mut self, n: u32) -> Option<Option<String>> {
        self.lookup_param_opt("F", n, parse_str)
    }

    fn lookup_param_percent_emitted(&mut self, n: u32) -> Option<Option<u32>> {
        self.lookup_param_opt("P", n, parse_int)
    }

    fn lookup_param_detector_voltage(&mut self, n: u32) -> Option<Option<f32>> {
        self.lookup_param_opt("P", n, parse_float)
    }

    fn lookup_param_detector(&mut self, n: u32) -> Option<Option<String>> {
        self.lookup_param_opt("DET", n, parse_str)
    }

    fn lookup_param_tag(&mut self, n: u32) -> Option<Option<String>> {
        self.lookup_param_opt("TAG", n, parse_str)
    }

    fn lookup_param_analyte(&mut self, n: u32) -> Option<Option<String>> {
        self.lookup_param_opt("ANALYTE", n, parse_str)
    }

    fn lookup_param_gain(&mut self, n: u32) -> Option<Option<f32>> {
        self.lookup_param_opt("G", n, parse_float)
    }

    fn lookup_param_scale_req(&mut self, n: u32) -> Option<Scale> {
        self.lookup_param_req("E", n, parse_scale)
    }

    fn lookup_param_scale_opt(&mut self, n: u32) -> Option<Option<Scale>> {
        self.lookup_param_opt("E", n, parse_scale)
    }

    fn lookup_param_calibration(&mut self, n: u32) -> Option<Option<Calibration>> {
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
    fn lookup_param_wavelengths(&mut self, n: u32) -> Option<Vec<u32>> {
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
        .map(|x| x.unwrap_or(vec![]))
    }

    fn lookup_param_display(&mut self, n: u32) -> Option<Option<Display>> {
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

    fn lookup_param_datatype(&mut self, n: u32) -> Option<Option<NumTypes>> {
        self.lookup_param_opt("DATATYPE", n, |s| match s {
            "I" => Ok(NumTypes::Integer),
            "F" => Ok(NumTypes::Float),
            "D" => Ok(NumTypes::Double),
            _ => Err("unknown datatype"),
        })
    }

    fn lookup_param_type(&mut self, n: u32) -> Option<Option<MeasurementType>> {
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

    fn lookup_param_feature(&mut self, n: u32) -> Option<Option<Feature>> {
        self.lookup_param_opt("FEATURE", n, |s| match s {
            "Area" => Ok(Feature::Area),
            "Width" => Ok(Feature::Width),
            "Height" => Ok(Feature::Height),
            _ => Err("unknown parameter feature"),
        })
    }

    fn finalize(
        &self,
    ) -> (
        HashMap<String, String>,
        HashMap<String, String>,
        HashSet<String>,
        HashMap<String, KwError>,
    ) {
        unimplemented!();
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

// TODO possibly not optimal
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
    println!("{:#?}", text.delimiter);
    for (k, v) in text.keywords {
        if v.len() < 100 {
            println!("{}: {}", k, v);
        }
    }
}

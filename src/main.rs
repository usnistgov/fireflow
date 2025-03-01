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

enum Endian {
    Big,
    Little,
}

enum ByteOrd {
    BigLittle(Endian),
    Mixed([u8; 4]),
}

struct Trigger {
    parameter: String,
    threshold: u32,
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
    fn from_kws(st: &mut KwState) -> Option<Vec<Parameter<Self>>> {
        let mut ps = vec![];
        let mut n = 1;
        loop {
            // lookup bits since this should be present in all versions, if not
            // present then consider the previous index to be the last parameter
            // index
            match st.lookup_param_bits(n) {
                Some(bits) => {
                    if let (
                        Some(range),
                        Some(longname),
                        Some(filter),
                        Some(power),
                        Some(detector_type),
                        Some(percent_emitted),
                        Some(detector_voltage),
                        Some(specific),
                    ) = (
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
                    } else {
                        break;
                    }
                }
                None => break,
            };
            n = n + 1
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

fn parse_float(s: &str) -> Result<f32, &'static str> {
    s.parse().or(Err("invalid float"))
}

fn parse_str(s: &str) -> Result<String, &'static str> {
    Ok(String::from(s))
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

struct InnerMetadata2_0 {
    tot: Option<u32>,
    mode: Mode,
    byteord: ByteOrd,
    datatype: AlphaNumTypes,
    cyt: Option<String>,
    timestamps: Timestamps2_0, // BTIM/ETIM/DATE
}

struct InnerMetadata3_0 {
    tot: u32,
    mode: Mode,
    byteord: ByteOrd,
    timestamps: Timestamps2_0, // BTIM/ETIM/DATE
    cyt: Option<String>,
    cytsn: CytSN,
    timestep: Timestep,
}

struct InnerMetadata3_1 {
    tot: u32,
    mode: Mode,
    byteord: Endian,
    timestamps: Timestamps2_0, // BTIM/ETIM/DATE
    cyt: Option<String>,
    cytsn: CytSN,
    timestep: Timestep,
    modification: ModificationData,
    plate: PlateData,
    vol: Vol,
    unicode: Unicode,
}

struct InnerMetadata3_2 {
    tot: u32,
    byteord: Endian,
    timestamps: Timestamps3_2, // DATETIMESTART/END
    cyt: String,
    cytsn: CytSN,
    timestep: Timestep,
    modification: ModificationData,
    plate: PlateData,
    vol: Vol,
    carrier: CarrierData,
    unstained: UnstainedData,
}

struct Metadata<X> {
    par: u32,
    nextdata: u32,
    datatype: AlphaNumTypes,
    // an abstraction for various kinds of spillover/comp matrices
    spillover: Spillover,
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

    fn from_kws(st: &mut KwState) -> Option<Metadata<Self>>;
    // fn from_kws(st: &mut KwState) -> Option<Self> {
    //     let mut ps = vec![];
    //     if let (Some(datatype), Some(specific)) = (st.lookup_datatype(), Self::build_inner(st)) {
    //         Metadata { datatype, specific };
    //     } else {
    //         None
    //     }
    // }
}

trait StdTextFromKeywords: Sized {
    type M: MetadataFromKeywords;
    type P: ParameterFromKeywords;

    fn build(m: Metadata<Self::M>, p: Vec<Parameter<Self::P>>) -> Self;

    fn from_kws(st: &mut KwState) -> Option<Self> {
        if let (Some(metadata), Some(parameters)) = (Self::M::from_kws(st), Self::P::from_kws(st)) {
            Some(Self::build(metadata, parameters))
        } else {
            None
        }
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

type TEXT2_0 = TEXT<StdText2_0>;
type TEXT3_0 = TEXT<StdText3_0>;
type TEXT3_1 = TEXT<StdText3_1>;
type TEXT3_2 = TEXT<StdText3_2>;

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

    // TODO unicode

    // TODO time stuff

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

// fn from_kws<T: StdTextFromKeywords>(st: &mut KwState) -> TEXT<T> {
//     let standard = T::from_kws(st);
//     let (nonstandard, deviant, standard_missing, standard_errors) = st.finalize();
//     return TEXT {
//         standard,
//         standard_missing,
//         standard_errors,
//         nonstandard,
//         deviant,
//     };
// }

// fn test(st: &mut KwState) -> TEXT2_0 {
//     from_kws(st)
// }

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

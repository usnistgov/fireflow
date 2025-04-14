use crate::config::*;
use crate::error::*;
pub use crate::header::*;
use crate::header_text::*;
use crate::keywords::*;
use crate::macros::{newtype_disp, newtype_from, newtype_from_outer, newtype_fromstr};
use crate::optionalkw::OptionalKw;
pub use crate::segment::*;
use crate::validated::nonstandard::{
    DefaultMatrix, DefaultMeasOptional, DefaultMetaOptional, NonStdKey, NonStdKeywords,
    NonStdMeasPattern,
};
use crate::validated::shortname::Shortname;

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use itertools::Itertools;
use nalgebra::DMatrix;
use polars::prelude::*;
use regex::Regex;
use serde::ser::SerializeStruct;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::fmt;
use std::fs;
use std::io;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::iter;
use std::num::{IntErrorKind, ParseFloatError, ParseIntError};
use std::path;
use std::str;
use std::str::FromStr;

macro_rules! match_many_to_one {
    ($value:expr, $root:ident, [$($variant:ident),*], $inner:ident, $action:block) => {
        match $value {
            $(
                $root::$variant($inner) => {
                    $action
                },
            )*
        }
    };
}

// TODO gating parameters not added (yet)

/// Output from parsing the TEXT segment.
///
/// This is derived from the HEADER which should be parsed in order to obtain
/// this.
///
/// The purpose of this is to obtain the TEXT keywords (primary and
/// supplemental) using the least amount of processing, which should increase
/// performance and minimize potential errors thrown if this is what the user
/// desires.
///
/// This will also be used as input downstream to 'standardize' the TEXT segment
/// according to version, and also to parse DATA if either is desired.
#[derive(Debug, Clone, Serialize)]
pub struct RawTEXT {
    /// FCS Version from HEADER
    pub version: Version,

    /// Offsets from the HEADER and partially from TEXT.
    ///
    /// This will include primary TEXT, DATA, and ANALYSIS offsets as seen in
    /// HEADER. It will also include $BEGIN/ENDSTEXT as found in TEXT (if found)
    /// which will be used to parse the supplemental TEXT segment if it exists.
    /// $NEXTDATA will also be included if found.
    ///
    /// This will not include $BEGIN/ENDDATA or $BEGIN/ENDANALYSIS since the
    /// intention of parsing RawTEXT is to minimally process the TEXT segment
    /// such that the fewest possible errors are thrown.
    pub offsets: Offsets,

    /// Delimiter used to parse TEXT.
    ///
    /// Included here for informational purposes.
    pub delimiter: u8,

    /// Keyword pairs
    ///
    /// This does not include $BEGIN/ENDSTEXT and will include supplemental TEXT
    /// keywords if present and the offsets for supplemental TEXT are
    /// successfully found.
    pub keywords: RawKeywords,
}

/// All segment offsets.
///
/// These are derived after parsing TEXT so they include both the HEADER offsets
/// and anything present in TEXT.
///
/// Functionally, this is mostly useful downstream to either parse DATA,
/// ANALYSIS, or the next dataset via NEXTDATA. The other offsets are only
/// valuable for informing the user since by the time this struct will exist the
/// primary (and possibly supplemental) TEXT will have already been parsed.
#[derive(Debug, Clone, Serialize)]
pub struct Offsets {
    /// Primary TEXT offsets
    ///
    /// The offsets that were used to parse the TEXT segment. Included here for
    /// informational purposes.
    pub prim_text: Segment,

    /// Supplemental TEXT offsets
    ///
    /// This is not needed downstream and included here for informational
    /// purposes. It will always be None for 2.0 which does not include this.
    pub supp_text: Option<Segment>,

    /// DATA offsets
    ///
    /// The offsets pointing to the DATA segment. When this struct is present
    /// in [RawTEXT] or [StandardizedTEXT], this will reflect what is in the
    /// HEADER. In [StandardizedDataset], this will reflect the values from
    /// $BEGIN/ENDDATA if applicable.
    ///
    /// This will be 0,0 if DATA has no data or if there was an error acquiring
    /// the offsets (which will be emitted separately depending on
    /// configuration)
    pub data: Segment,

    /// ANALYSIS offsets.
    ///
    /// The meaning of this is analogous to [data] above.
    pub analysis: Segment,

    /// NEXTDATA offset
    ///
    /// This will be copied as represented in TEXT. If it is 0, there is no next
    /// dataset, otherwise it points to the next dataset in the file.
    pub nextdata: Option<u32>,
}

/// Output of parsing the TEXT segment and standardizing keywords.
///
/// This is derived from ['RawTEXT'].
///
/// The process of "standardization" involves gathering version specific
/// keywords in the TEXT segment and parsing their values such that they
/// conform to the types specified in the standard.
///
/// Version is not included since this is implied by the standardized structs
/// used.
#[derive(Clone)]
pub struct StandardizedTEXT {
    pub offsets: Offsets,

    /// Delimiter used to parse TEXT.
    ///
    /// Included here for informational purposes.
    pub delimiter: u8,

    /// Structured data derived from TEXT specific to the indicated FCS version.
    ///
    /// All keywords that were included in the ['RawTEXT'] used to create this
    /// will be included here. Anything standardized will be put into a field
    /// that can be readily accessed directly and returned with the proper type.
    /// Anything nonstandard will be kept in a hash table whose values will
    /// be strings.
    pub standardized: AnyCoreTEXT,

    /// Raw standard keywords remaining after the standardization process
    ///
    /// This only should include $TOT, $BEGINDATA, $ENDDATA, $BEGINANALISYS, and
    /// $ENDANALYSIS. These are only needed to process the data segment and are
    /// not necessary to create the CoreTEXT, and thus are not included.
    pub remainder: RawKeywords,

    /// Raw keywords that are not standard but start with '$'
    pub deviant: RawKeywords,
}

/// Output of parsing one raw dataset (TEXT+DATA) from an FCS file.
///
/// Computationally this will be created by skipping (most of) the
/// standardization step and instead parsing the minimal-required keywords
/// to parse DATA (BYTEORD, DATATYPE, etc).
///
// TODO why is this important? this will likely be used by flowcore (at least
// initially) because this replicates what it would need to do to get a
// dataframe. Furthermore, it could be useful for someone who wishes to parse
// all their data and then repair it, although there should be easier ways to do
// this using the standardized interface.
pub struct RawDataset {
    /// Offsets as parsed from raw TEXT and HEADER
    // TODO the data segment in this should be non-Option since we know it
    // exists if this struct exists.
    pub offsets: Offsets,

    // TODO add keywords
    // TODO add dataset
    /// Delimiter used to parse TEXT.
    ///
    /// Included here for informational purposes.
    pub delimiter: u8,
}

/// Output of parsing one standardized dataset (TEXT+DATA) from an FCS file.
#[derive(Clone)]
pub struct StandardizedDataset {
    pub offsets: Offsets,

    /// Delimiter used to parse TEXT.
    pub delimiter: u8,

    /// Structured data derived from TEXT specific to the indicated FCS version.
    pub dataset: CoreDataset,

    /// Raw standard keyword remaining after processing.
    ///
    /// This should be empty if everything worked. Here for debugging.
    pub remainder: RawKeywords,

    /// Non-standard keywords that start with '$'.
    pub deviant: RawKeywords,
}

/// Represents the minimal data to fully describe one dataset in an FCS file.
///
/// This will include the standardized TEXT keywords as well as its
/// corresponding DATA segment parsed into a dataframe-like structure.
#[derive(Clone)]
pub struct CoreDataset {
    /// Standardized TEXT segment in version specific format
    pub text: AnyCoreTEXT,

    /// DATA segment as a polars DataFrame
    ///
    /// The type of each column is such that each measurement is encoded with
    /// zero loss. This will/should never contain NULL values despite the
    /// underlying arrow framework allowing NULLs to exist.
    pub data: DataFrame,

    /// ANALYSIS segment
    ///
    /// This will be empty if ANALYSIS either doesn't exist or the computation
    /// fails. This has not standard structure, so the best we can capture is a
    /// byte sequence.
    pub analysis: Vec<u8>,
}

/// Critical FCS TEXT data for any supported FCS version
#[derive(Clone)]
pub enum AnyCoreTEXT {
    FCS2_0(Box<CoreTEXT2_0>),
    FCS3_0(Box<CoreTEXT3_0>),
    FCS3_1(Box<CoreTEXT3_1>),
    FCS3_2(Box<CoreTEXT3_2>),
}

/// Minimally-required FCS TEXT data for each version
pub type CoreTEXT2_0 = CoreTEXT<InnerMetadata2_0, InnerMeasurement2_0>;
pub type CoreTEXT3_0 = CoreTEXT<InnerMetadata3_0, InnerMeasurement3_0>;
pub type CoreTEXT3_1 = CoreTEXT<InnerMetadata3_1, InnerMeasurement3_1>;
pub type CoreTEXT3_2 = CoreTEXT<InnerMetadata3_2, InnerMeasurement3_2>;

/// Represents the minimal data required to fully represent the TEXT keywords.
///
/// This is created by further processing ['RawTEXT'] and arranging keywords
/// in a static structure that conforms to each version's FCS standard.
///
/// This includes "measurement data" (bit width, voltage, filter, name, etc) for
/// each measurement (aka "parameter" aka "channel") and "metadata" which is all
/// non-measurement data. Both of these are "standardized" which means they are
/// structured in a manner that reflects the indicated FCS standard in the
/// HEADER. If these structures cannot be created, then an error should be
/// thrown informing the user that their keywords are not standards-compliant.
///
/// Additionally this includes keywords that do not fit into the standard
/// (deviant and nonstandard, which means keys that start with '$' or not
/// respectively).
///
/// Importantly this does NOT include the following:
/// - $TOT (inferred from summed bit width and length of DATA)
/// - $PAR (inferred from length of measurement vector)
/// - $NEXTDATA (handled elsewhere)
/// - $(BEGIN|END)(DATA|ANALYSIS|STEXT) (already parsed when for raw TEXT)
///
/// These are not included because this struct will also be used to encode the
/// TEXT data when writing a new FCS file, and the keywords that are not
/// included can be computed on the fly when writing.
#[derive(Clone, Serialize)]
pub struct CoreTEXT<M, P> {
    /// All "non-measurement" TEXT keywords.
    ///
    /// This is specific to each FCS version, which is encoded in the generic
    /// type variable.
    metadata: Metadata<M>,

    /// All measurement TEXT keywords.
    ///
    /// Specifically these are denoted by "$Pn*" keywords where "n" is the index
    /// of the measurement which also corresponds to its column in the DATA
    /// segment. The order of each measurement in this vector is n - 1.
    ///
    /// This is specific to each FCS version, which is encoded in the generic
    /// type variable.
    measurements: Vec<Measurement<P>>,
}

/// Raw TEXT key/value pairs
type RawKeywords = HashMap<String, String>;

/// A datetime as used in the $(BEGIN|END)DATETIME keys (3.2+ only)
#[derive(Debug, Clone, Serialize)]
struct FCSDateTime(DateTime<FixedOffset>);

/// A time as used in the $BTIM/ETIM keys without seconds (2.0 only)
#[derive(Debug, Clone, Serialize, PartialEq, Eq, PartialOrd)]
struct FCSTime(NaiveTime);

/// A time as used in the $BTIM/ETIM keys with 1/60 seconds (3.0 only)
#[derive(Debug, Clone, Serialize, PartialEq, Eq, PartialOrd)]
struct FCSTime60(NaiveTime);

/// A time as used in the $BTIM/ETIM keys with centiseconds (3.1+ only)
#[derive(Debug, Clone, Serialize, PartialEq, Eq, PartialOrd)]
struct FCSTime100(NaiveTime);

/// A datetime as used in the $LAST_MODIFIED key (3.1+ only)
// TODO this should almost certainly be after $ENDDATETIME if given
#[derive(Debug, Clone, Serialize)]
struct ModifiedDateTime(NaiveDateTime);

/// A date as used in the $DATE key (all versions)
#[derive(Debug, Clone, Serialize)]
struct FCSDate(NaiveDate);

/// A convenient bundle holding data/time keyword values.
///
/// The generic type parameter is meant to account for the fact that the time
/// types for different versions are all slightly different in their treatment
/// of sub-second time.
#[derive(Debug, Clone, Serialize)]
struct Timestamps<T> {
    /// The value of the $BTIM key
    btim: OptionalKw<T>,

    /// The value of the $ETIM key
    etim: OptionalKw<T>,

    /// The value of the $DATE key
    date: OptionalKw<FCSDate>,
}

impl<X> Timestamps<X> {
    fn map<F, Y>(self, f: F) -> Timestamps<Y>
    where
        F: Fn(X) -> Y,
    {
        Timestamps {
            btim: self.btim.map(&f),
            etim: self.etim.map(&f),
            date: self.date,
        }
    }
}

impl<X: PartialOrd> Timestamps<X> {
    fn valid(&self) -> bool {
        if self.date.0.is_some() {
            if let (Some(b), Some(e)) = (&self.btim.0, &self.etim.0) {
                b < e
            } else {
                true
            }
        } else {
            true
        }
    }
}

// TODO this might be useful but there shouldn't be an equivilencey b/t these
// two since the former does not include time zone
fn timestamps_eq_datetimes<T>(ts: &Timestamps<T>, dt: &Datetimes) -> bool
where
    T: Copy,
    NaiveTime: From<T>,
{
    if let (Some(td), Some(tb), Some(te), Some(db), Some(de)) =
        (&ts.date.0, &ts.btim.0, &ts.etim.0, &dt.begin.0, &dt.end.0)
    {
        let dt_d1 = db.0.date_naive();
        let dt_d2 = de.0.date_naive();
        let dt_t1 = db.0.time();
        let dt_t2 = de.0.time();
        let ts_d = td.0;
        let ts_t1: NaiveTime = (*tb).into();
        let ts_t2: NaiveTime = (*te).into();
        dt_d1 == dt_d2 && dt_d2 == ts_d && dt_t1 == ts_t1 && dt_t2 == ts_t2
    } else {
        true
    }
}

/// $BTIM/ETIM/DATE for FCS 2.0
type Timestamps2_0 = Timestamps<FCSTime>;

/// $BTIM/ETIM/DATE for FCS 3.0
type Timestamps3_0 = Timestamps<FCSTime60>;

/// $BTIM/ETIM/DATE for FCS 3.1+
type Timestamps3_1 = Timestamps<FCSTime100>;

/// A convenient bundle for the $BEGINDATETIME and $ENDDATETIME keys (3.2+)
#[derive(Debug, Clone, Serialize, Default)]
struct Datetimes {
    /// Value for the $BEGINDATETIME key.
    begin: OptionalKw<FCSDateTime>,

    /// Value for the $ENDDATETIME key.
    end: OptionalKw<FCSDateTime>,
}

impl Datetimes {
    fn valid(&self) -> bool {
        if let (Some(b), Some(e)) = (&self.begin.0, &self.end.0) {
            b.0 < e.0
        } else {
            true
        }
    }
}

/// The values used for the $MODE key (up to 3.1)
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
enum Mode {
    List,
    // TODO I have no idea what these even mean and IDK how to support them
    Uncorrelated,
    Correlated,
}

/// The value for the $MODE key, which can only contain 'L' (3.2)
struct Mode3_2;

/// The value for the $PnDISPLAY key (3.1+)
#[derive(Debug, Clone, Serialize)]
enum Display {
    /// Linear display (value like 'Linear,<lower>,<upper>')
    Lin { lower: f32, upper: f32 },

    /// Logarithmic display (value like 'Logarithmic,<offset>,<decades>')
    Log { offset: f32, decades: f32 },
}

/// Endianness
///
/// This is also stored in the $BYTEORD key in 3.1+
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq, Hash)]
pub enum Endian {
    Big,
    Little,
}

/// The byte order as shown in the $BYTEORD field in 2.0 and 3.0
///
/// This can be either 1,2,3,4 (little endian), 4,3,2,1 (big endian), or some
/// sequence representing byte order. For 2.0 and 3.0, this sequence is
/// technically allowed to vary in length in the case of $DATATYPE=I since
/// integers do not necessarily need to be 32 or 64-bit.
#[derive(Debug, Clone, Serialize)]
enum ByteOrd {
    // TODO this should also be applied to things like 1,2,3 or 5,4,3,2,1, which
    // are big/little endian but not "traditional" byte widths.
    Endian(Endian),
    Mixed(Vec<u8>),
}

/// The four allowed values for the $DATATYPE keyword.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Serialize)]
enum AlphaNumType {
    Ascii,
    Integer,
    Single,
    Double,
}

/// The three values for the $PnDATATYPE keyword (3.2+)
#[derive(Debug, Clone, Copy, Serialize)]
enum NumType {
    Integer,
    Single,
    Double,
}

/// A compensation matrix.
///
/// This is encoded in the $DFCmTOn keywords in 2.0 and $COMP in 3.0.
#[derive(Debug, Clone, Serialize)]
struct Compensation {
    /// Values in the comp matrix in row-major order. Assumed to be the
    /// same width and height as $PAR
    // TODO just use a DMatrix here?
    matrix: DMatrix<f32>,
}

/// The spillover matrix from the $SPILLOVER keyword (3.1+)
#[derive(Debug, Clone, Serialize)]
struct Spillover {
    /// The measurements in the spillover matrix. Assumed to be a subset of the
    /// values in the $PnN keys.
    measurements: Vec<Shortname>,

    /// Numeric values in the spillover matrix in row-major order.
    matrix: DMatrix<f32>,
}

/// The value of the $TR field (all versions)
///
/// This is formatted as 'string,f' where 'string' is a measurement name.
#[derive(Debug, Clone, Serialize)]
struct Trigger {
    /// The measurement name (assumed to match a '$PnN' value).
    measurement: Shortname,

    /// The threshold of the trigger.
    threshold: u32,
}

/// The value for the $PnE key (all versions).
///
/// Format is assumed to be 'f1,f2'
// TODO this is super messy, see 3.2 spec for restrictions on this we may with
// to use further
#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum Scale {
    /// Linear scale, which maps to the value '0,0'
    Linear,

    /// Log scale, which maps to anything not '0,0' (although decades should be
    /// a positive number presumably)
    Log { decades: f32, offset: f32 },
}

/// The value for the $PnCALIBRATION key (3.1 only)
///
/// This should be formatted like '<value>,<unit>'
#[derive(Debug, Clone, Serialize)]
struct Calibration3_1 {
    value: f32,
    unit: String,
}

/// The value for the $PnCALIBRATION key (3.2+)
///
/// This should be formatted like '<value>,[<offset>,]<unit>' and differs from
/// 3.1 with the optional inclusion of "offset" (assumed 0 if not included).
#[derive(Debug, Clone, Serialize)]
struct Calibration3_2 {
    value: f32,
    offset: f32,
    unit: String,
}

/// The value for the $PnL key (3.1).
///
/// This is a list of wavelengths used for the measurement. Starting in 3.1
/// this could be a list, where it needed to be a single number in previous
/// versions.
#[derive(Debug, Clone, Serialize)]
struct Wavelengths(Vec<u32>);

/// The value for the $PnB key (all versions)
///
/// The $PnB key actually stores bits. However, this library only supports
/// widths that are multiples of 8 (ie bytes) for now. Therefore, this key
/// actually stores the number of bytes indicated by $PnB.
///
/// This may also be '*' which means "delimited ASCII" which is only valid when
/// $DATATYPE=A.
#[derive(Debug, Clone, Copy, Serialize)]
enum Bytes {
    Fixed(u8),
    Variable,
}

/// The value for the $ORIGINALITY key (3.1+)
#[derive(Debug, Clone, Serialize)]
enum Originality {
    Original,
    NonDataModified,
    Appended,
    DataModified,
}

/// A bundle for $ORIGINALITY, $LAST_MODIFIER, and $LAST_MODIFIED (3.1+)
#[derive(Clone, Serialize, Default)]
struct ModificationData {
    last_modifier: OptionalKw<LastModifier>,
    last_modified: OptionalKw<ModifiedDateTime>,
    originality: OptionalKw<Originality>,
}

/// A bundle for $PLATEID, $PLATENAME, and $WELLID (3.1+)
#[derive(Clone, Serialize, Default)]
struct PlateData {
    plateid: OptionalKw<Plateid>,
    platename: OptionalKw<Platename>,
    wellid: OptionalKw<Wellid>,
}

/// The value for the $UNSTAINEDCENTERS key (3.2+)
///
/// This is actually encoded as a string like 'n,[measuremnts,],[values]' but
/// here is more conveniently encoded as a hash table.
#[derive(Clone, Serialize)]
struct UnstainedCenters(HashMap<Shortname, f32>);

/// A bundle for $UNSTAINEDCENTERS and $UNSTAINEDINFO (3.2+)
#[derive(Clone, Serialize, Default)]
struct UnstainedData {
    unstainedcenters: OptionalKw<UnstainedCenters>,
    unstainedinfo: OptionalKw<UnstainedInfo>,
}

/// A bundle for $CARRIERID, $CARRIERTYPE, $LOCATIONID (3.2+)
#[derive(Clone, Serialize, Default)]
struct CarrierData {
    carrierid: OptionalKw<Carrierid>,
    carriertype: OptionalKw<Carriertype>,
    locationid: OptionalKw<Locationid>,
}

/// The value of the $UNICODE key (3.0 only)
///
/// Formatted like 'codepage,[keys]'. This key is not actually used for anything
/// in this library and is present to be complete. The original purpose was to
/// indicate keywords which supported UTF-8, but these days it is hard to
/// write a library that does NOT support UTF-8 ;)
#[derive(Debug, Clone, Serialize)]
struct Unicode {
    page: u32,
    // TODO check that these are valid keywords (probably not worth it)
    kws: Vec<String>,
}

/// The value of the $PnTYPE key (3.2+)
#[derive(Debug, Clone, Serialize)]
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
    // TODO is isn't clear if this is allowed according to the standard
    Other(String),
}

/// The value of the $PnFEATURE key (3.2+)
#[derive(Debug, Clone, Serialize)]
enum Feature {
    Area,
    Width,
    Height,
}

/// The value of the $PnR key (all versions)
///
/// Technically this should only be an integer, but many versions also store
/// floats which makes sense for cases where $DATATYPE/$PnDATATYPE indicates
/// float or double.
#[derive(Debug, Clone, Copy, Serialize)]
enum Range {
    /// The value when stored as an integer.
    ///
    /// This will actually store PnR - 1; most cytometers will store this as a
    /// power of 2, so in the case of a 64 bit channel this will be 2^64 which
    /// is one greater than u64::MAX.
    // TODO what if they store an integer for a float value? Then the range will
    // be off by one.
    Int(u64),

    /// The value when stored as a float.
    ///
    /// Unlike above, this will be parsed and used as-is.
    Float(f64),
}

/// Measurement fields specific to version 2.0
#[derive(Clone, Serialize)]
pub struct InnerMeasurement2_0 {
    /// Value for $PnE
    scale: OptionalKw<Scale>,

    /// Value for $PnL
    wavelength: OptionalKw<Wavelength>,

    /// Value for $PnN
    shortname: OptionalKw<Shortname>,
}

/// Measurement fields specific to version 3.0
#[derive(Clone, Serialize)]
pub struct InnerMeasurement3_0 {
    /// Value for $PnE
    scale: Scale,

    /// Value for $PnL
    wavelength: OptionalKw<Wavelength>,

    /// Value for $PnN
    shortname: OptionalKw<Shortname>,

    /// Value for $PnG
    gain: OptionalKw<Gain>,
}

/// Measurement fields specific to version 3.1
#[derive(Clone, Serialize)]
pub struct InnerMeasurement3_1 {
    /// Value for $PnE
    scale: Scale,

    /// Value for $PnL
    wavelengths: OptionalKw<Wavelengths>,

    /// Value for $PnN
    shortname: Shortname,

    /// Value for $PnG
    gain: OptionalKw<Gain>,

    /// Value for $PnCALIBRATION
    calibration: OptionalKw<Calibration3_1>,

    /// Value for $PnDISPLAY
    display: OptionalKw<Display>,
}

/// Measurement fields specific to version 3.2
#[derive(Clone, Serialize)]
pub struct InnerMeasurement3_2 {
    /// Value for $PnE
    scale: Scale,

    /// Value for $PnL
    wavelengths: OptionalKw<Wavelengths>,

    /// Value for $PnN
    shortname: Shortname,

    /// Value for $PnG
    gain: OptionalKw<Gain>,

    /// Value for $PnCALIBRATION
    calibration: OptionalKw<Calibration3_2>,

    /// Value for $PnDISPLAY
    display: OptionalKw<Display>,

    /// Value for $PnANALYTE
    analyte: OptionalKw<Analyte>,

    /// Value for $PnFEATURE
    feature: OptionalKw<Feature>,

    /// Value for $PnTYPE
    measurement_type: OptionalKw<MeasurementType>,

    /// Value for $PnTAG
    tag: OptionalKw<Tag>,

    /// Value for $PnDET
    detector_name: OptionalKw<DetectorName>,

    /// Value for $PnDATATYPE
    datatype: OptionalKw<NumType>,
}

/// Structured data for measurement keywords.
///
/// Explicit fields are common to all versions. The generic type parameter
/// allows for version-specific information to be encoded.
///
/// To make this struct, all required keys need to be present for the specific
/// version. This is often more than required to parse the DATA segment. (see
/// ['MinimalMeasurement']
#[derive(Clone, Serialize)]
pub struct Measurement<X> {
    /// Value for $PnB
    bytes: Bytes,

    /// Value for $PnR
    range: Range,

    /// Value for $PnS
    longname: OptionalKw<Longname>,

    /// Value for $PnF
    filter: OptionalKw<Filter>,

    /// Value for $PnO
    power: OptionalKw<Power>,

    /// Value for $PnD
    detector_type: OptionalKw<DetectorType>,

    /// Value for $PnP
    percent_emitted: OptionalKw<PercentEmitted>,

    /// Value for $PnV
    detector_voltage: OptionalKw<DetectorVoltage>,

    /// Non standard keywords that belong to this measurement.
    ///
    /// These are found using a configurable pattern to filter matching keys.
    nonstandard_keywords: NonStdKeywords,

    /// Version specific data
    specific: X,
}

/// Version-specific data for one measurement
pub type Measurement2_0 = Measurement<InnerMeasurement2_0>;
pub type Measurement3_0 = Measurement<InnerMeasurement3_0>;
pub type Measurement3_1 = Measurement<InnerMeasurement3_1>;
pub type Measurement3_2 = Measurement<InnerMeasurement3_2>;

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

macro_rules! newtype_u32 {
    ($t:ident) => {
        #[derive(Clone, Copy, Serialize)]
        pub struct $t(pub u32);

        newtype_disp!($t);
        newtype_fromstr!($t, ParseIntError);
        newtype_from!($t, u32);
        newtype_from_outer!($t, u32);
    };
}

newtype_string!(Cyt);
newtype_string!(Cytsn);
newtype_string!(Com);
newtype_string!(Flowrate);
newtype_string!(Cells);
newtype_string!(Exp);
newtype_string!(Fil);
newtype_string!(Inst);
newtype_string!(Op);
newtype_string!(Proj);
newtype_string!(Smno);
newtype_string!(Src);
newtype_string!(Sys);
newtype_string!(LastModifier);
newtype_string!(Plateid);
newtype_string!(Platename);
newtype_string!(Wellid);
newtype_string!(UnstainedInfo);
newtype_string!(Carrierid);
newtype_string!(Carriertype);
newtype_string!(Locationid);
newtype_string!(Analyte);
newtype_string!(Tag);
newtype_string!(DetectorName);
newtype_string!(DetectorType);
newtype_string!(PercentEmitted);
newtype_string!(Longname);
newtype_string!(Filter);

newtype_u32!(Abrt);
newtype_u32!(Lost);
newtype_u32!(Wavelength);
newtype_u32!(Power);

// TODO technically this should be validated to be > 0
#[derive(Clone, Serialize)]
pub struct Timestep(pub f32);

newtype_disp!(Timestep);
newtype_fromstr!(Timestep, ParseFloatError);

// TODO ditto
#[derive(Clone, Serialize)]
pub struct Vol(pub f32);

newtype_disp!(Vol);
newtype_fromstr!(Vol, ParseFloatError);

// TODO ditto
#[derive(Clone, Serialize)]
pub struct Gain(pub f32);

newtype_disp!(Gain);
newtype_fromstr!(Gain, ParseFloatError);

// TODO ditto
#[derive(Clone, Serialize)]
pub struct DetectorVoltage(pub f32);

newtype_disp!(DetectorVoltage);
newtype_fromstr!(DetectorVoltage, ParseFloatError);

/// Metadata fields specific to version 2.0
#[derive(Clone, Serialize)]
pub struct InnerMetadata2_0 {
    /// Value of $MODE
    mode: Mode,

    /// Value of $BYTEORD
    byteord: ByteOrd,

    /// Value of $CYT
    cyt: OptionalKw<Cyt>,

    /// Compensation matrix derived from 'DFCnTOm' key/value pairs
    comp: OptionalKw<Compensation>,

    /// Values of $BTIM/ETIM/$DATE
    timestamps: Timestamps2_0,
}

/// Metadata fields specific to version 3.0
#[derive(Clone, Serialize)]
pub struct InnerMetadata3_0 {
    /// Value of $MODE
    mode: Mode,

    /// Value of $BYTEORD
    byteord: ByteOrd,

    /// Value of $CYT
    cyt: OptionalKw<Cyt>,

    /// Value of $COMP
    comp: OptionalKw<Compensation>,

    /// Values of $BTIM/ETIM/$DATE
    timestamps: Timestamps3_0,

    /// Value of $CYTSN
    cytsn: OptionalKw<Cytsn>,

    /// Value of $TIMESTEP
    timestep: OptionalKw<Timestep>,

    /// Value of $UNICODE
    unicode: OptionalKw<Unicode>,
}

/// Metadata fields specific to version 3.1
#[derive(Clone, Serialize)]
pub struct InnerMetadata3_1 {
    /// Value of $MODE
    mode: Mode,

    /// Value of $BYTEORD
    byteord: Endian,

    /// Value of $CYT
    cyt: OptionalKw<Cyt>,

    /// Values of $BTIM/ETIM/$DATE
    timestamps: Timestamps3_1,

    /// Value of $CYTSN
    cytsn: OptionalKw<Cytsn>,

    /// Value of $TIMESTEP
    timestep: OptionalKw<Timestep>,

    /// Value of $SPILLOVER
    spillover: OptionalKw<Spillover>,

    /// Values of $LAST_MODIFIED/$LAST_MODIFIER/$ORIGINALITY
    modification: ModificationData,

    /// Values of $PLATEID/$PLATENAME/$WELLID
    plate: PlateData,

    /// Value of $VOL
    vol: OptionalKw<Vol>,
}

#[derive(Clone, Serialize)]
pub struct InnerMetadata3_2 {
    /// Value of $BYTEORD
    byteord: Endian,

    /// Values of $BTIM/ETIM/$DATE
    timestamps: Timestamps3_1,

    /// Values of $BEGINDATETIME/$ENDDATETIME
    datetimes: Datetimes,

    /// Value of $CYT
    cyt: Cyt,

    /// Value of $SPILLOVER
    spillover: OptionalKw<Spillover>,

    /// Value of $CYTSN
    cytsn: OptionalKw<Cytsn>,

    /// Value of $TIMESTEP
    timestep: OptionalKw<Timestep>,

    /// Values of $LAST_MODIFIED/$LAST_MODIFIER/$ORIGINALITY
    modification: ModificationData,

    /// Values of $PLATEID/$PLATENAME/$WELLID
    plate: PlateData,

    /// Value of $VOL
    vol: OptionalKw<Vol>,

    /// Values of $CARRIERID/$CARRIERTYPE/$LOCATIONID
    carrier: CarrierData,

    /// Values of $UNSTAINEDINFO/$UNSTAINEDCENTERS
    unstained: UnstainedData,

    /// Value of $FLOWRATE
    flowrate: OptionalKw<Flowrate>,
}

/// Structured non-measurement metadata.
///
/// Explicit below are common to all FCS versions.
///
/// The generic type parameter allows version-specific data to be encoded.
#[derive(Clone, Serialize)]
pub struct Metadata<X> {
    /// Value of $DATATYPE
    datatype: AlphaNumType,

    /// Value of $ABRT
    abrt: OptionalKw<Abrt>,

    /// Value of $COM
    com: OptionalKw<Com>,

    /// Value of $CELLS
    cells: OptionalKw<Cells>,

    /// Value of $EXP
    exp: OptionalKw<Exp>,

    /// Value of $FIL
    fil: OptionalKw<Fil>,

    /// Value of $INST
    inst: OptionalKw<Inst>,

    /// Value of $LOST
    lost: OptionalKw<Lost>,

    /// Value of $OP
    op: OptionalKw<Op>,

    /// Value of $PROJ
    proj: OptionalKw<Proj>,

    /// Value of $SMNO
    smno: OptionalKw<Smno>,

    /// Value of $SRC
    src: OptionalKw<Src>,

    /// Value of $SYS
    sys: OptionalKw<Sys>,

    /// Value of $TR
    tr: OptionalKw<Trigger>,

    /// Version-specific data
    specific: X,

    /// Non-standard keywords.
    ///
    /// This will include all the keywords that do not start with '$'.
    ///
    /// Keywords which do start with '$' but are not part of the standard are
    /// considered 'deviant' and stored elsewhere since this structure will also
    /// be used to write FCS-compliant files (which do not allow nonstandard
    /// keywords starting with '$')
    nonstandard_keywords: NonStdKeywords,
}

/// Version-specific structured metadata derived from TEXT
type Metadata2_0 = Metadata<InnerMetadata2_0>;
type Metadata3_0 = Metadata<InnerMetadata3_0>;
type Metadata3_1 = Metadata<InnerMetadata3_1>;
type Metadata3_2 = Metadata<InnerMetadata3_2>;

/// The bare minimum of measurement data required to parse the DATA segment.
struct DataReadMeasurement<X> {
    /// The value of $PnB
    bytes: Bytes,

    /// The value of $PnR
    range: Range,

    /// Version-specific data
    specific: X,
}

struct InnerDataReadMeasurement3_2 {
    datatype: OptionalKw<NumType>,
}

/// Minimal data to parse one measurement column in FCS 2.0 (up to 3.1)
type DataReadMeasurement2_0 = DataReadMeasurement<()>;

/// Minimal data to parse one measurement column in FCS 3.2+
type DataReadMeasurement3_2 = DataReadMeasurement<NumType>;

struct InnerBareMetadata2_0 {
    tot: OptionalKw<usize>,
    byteord: ByteOrd,
}

struct InnerBareMetadata3_0 {
    tot: usize,
    byteord: ByteOrd,
}

struct InnerBareMetadata3_1 {
    tot: usize,
    byteord: Endian,
}

struct BareMetadata<M> {
    datatype: AlphaNumType,
    specific: M,
}

type BareMetadata2_0 = BareMetadata<InnerBareMetadata2_0>;
type BareMetadata3_0 = BareMetadata<InnerBareMetadata3_0>;
type BareMetadata3_1 = BareMetadata<InnerBareMetadata3_1>;

#[derive(PartialEq, Eq, Hash)]
struct UintType<T, const LEN: usize> {
    bitmask: T,
    size: SizedByteOrd<LEN>,
}

#[derive(PartialEq, Eq, Hash)]
enum AnyUintType {
    Uint8(UintType<u8, 1>),
    Uint16(UintType<u16, 2>),
    Uint24(UintType<u32, 3>),
    Uint32(UintType<u32, 4>),
    Uint40(UintType<u64, 5>),
    Uint48(UintType<u64, 6>),
    Uint56(UintType<u64, 7>),
    Uint64(UintType<u64, 8>),
}

#[derive(PartialEq, Eq, Hash)]
enum ColumnType {
    Ascii { bytes: u8 },
    Integer(AnyUintType),
    Float(SizedByteOrd<4>),
    Double(SizedByteOrd<8>),
}

enum DataLayout<T> {
    AsciiDelimited { nrows: Option<T>, ncols: usize },
    AlphaNum { nrows: T, columns: Vec<ColumnType> },
}

type WriterDataLayout = DataLayout<()>;
type ReaderDataLayout = DataLayout<usize>;

struct NumColumnWriter<T, const LEN: usize> {
    column: Vec<T>,
    size: SizedByteOrd<LEN>,
}

struct AsciiColumnWriter<T> {
    column: Vec<T>,
    bytes: u8,
}

enum ColumnWriter {
    NumU8(NumColumnWriter<u8, 1>),
    NumU16(NumColumnWriter<u16, 2>),
    NumU24(NumColumnWriter<u32, 3>),
    NumU32(NumColumnWriter<u32, 4>),
    NumU40(NumColumnWriter<u64, 5>),
    NumU48(NumColumnWriter<u64, 6>),
    NumU56(NumColumnWriter<u64, 7>),
    NumU64(NumColumnWriter<u64, 8>),
    NumF32(NumColumnWriter<f32, 4>),
    NumF64(NumColumnWriter<f64, 8>),
    AsciiU8(AsciiColumnWriter<u8>),
    AsciiU16(AsciiColumnWriter<u16>),
    AsciiU32(AsciiColumnWriter<u32>),
    AsciiU64(AsciiColumnWriter<u64>),
}

use ColumnWriter::*;

struct AsciiColumnReader {
    column: Vec<u64>,
    width: u8,
}

struct FloatColumnReader<T, const LEN: usize> {
    column: Vec<T>,
    order: SizedByteOrd<LEN>,
}

enum MixedColumnType {
    Ascii(AsciiColumnReader),
    Uint(AnyUintColumnReader),
    Single(FloatColumnReader<f32, 4>),
    Double(FloatColumnReader<f64, 8>),
}

struct MixedParser {
    nrows: usize,
    columns: Vec<MixedColumnType>,
}

#[derive(PartialEq, Eq, Hash, Copy, Clone)]
enum SizedByteOrd<const LEN: usize> {
    Endian(Endian),
    Order([u8; LEN]),
}

struct UintColumnReader<B, const LEN: usize> {
    layout: UintType<B, LEN>,
    column: Vec<B>,
}

enum AnyUintColumnReader {
    Uint8(UintColumnReader<u8, 1>),
    Uint16(UintColumnReader<u16, 2>),
    Uint24(UintColumnReader<u32, 3>),
    Uint32(UintColumnReader<u32, 4>),
    Uint40(UintColumnReader<u64, 5>),
    Uint48(UintColumnReader<u64, 6>),
    Uint56(UintColumnReader<u64, 7>),
    Uint64(UintColumnReader<u64, 8>),
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
// is likely not worth it.
struct UintReader {
    nrows: usize,
    columns: Vec<AnyUintColumnReader>,
}

#[derive(Debug)]
struct FixedAsciiReader {
    widths: Vec<u8>,
    nrows: usize,
}

#[derive(Debug)]
struct DelimAsciiReader {
    ncols: usize,
    nrows: Option<usize>,
    nbytes: usize,
}

struct FloatReader<const LEN: usize> {
    nrows: usize,
    ncols: usize,
    byteord: SizedByteOrd<LEN>,
}

enum ColumnReader {
    // DATATYPE=A where all PnB = *
    DelimitedAscii(DelimAsciiReader),
    // DATATYPE=A where all PnB = number
    FixedWidthAscii(FixedAsciiReader),
    // DATATYPE=F (with no overrides in 3.2+)
    Single(FloatReader<4>),
    // DATATYPE=D (with no overrides in 3.2+)
    Double(FloatReader<8>),
    // DATATYPE=I this is complicated so see struct definition
    Uint(UintReader),
    // Mixed column types (3.2+)
    Mixed(MixedParser),
}

struct DataReader {
    column_reader: ColumnReader,
    begin: u64,
}

struct FCSDateTimeError;
struct FCSTimeError;
struct FCSTime60Error;
struct FCSTime100Error;
struct FCSDateError;

struct AlphaNumTypeError;
struct NumTypeError;
pub struct EndianError;
struct ModifiedDateTimeError;
struct FeatureError;
struct OriginalityError;

enum BytesError {
    Int(ParseIntError),
    Range,
    NotOctet,
}

enum FixedSeqError {
    WrongLength { total: usize, expected: usize },
    BadLength,
    BadFloat,
}

enum NamedFixedSeqError {
    Seq(FixedSeqError),
    NonUnique,
}

enum CalibrationError<C> {
    Float(ParseFloatError),
    Range,
    Format(C),
}

struct CalibrationFormat3_1;
struct CalibrationFormat3_2;

enum UnicodeError {
    Empty,
    BadFormat,
}

enum ParseByteOrdError {
    InvalidOrder,
    InvalidNumbers,
}

enum TriggerError {
    WrongFieldNumber,
    IntFormat(std::num::ParseIntError),
}

pub enum ScaleError {
    FloatError(ParseFloatError),
    WrongFormat,
}

enum DisplayError {
    FloatError(ParseFloatError),
    InvalidType,
    FormatError,
}

struct ModeError;

enum RangeError {
    Int(ParseIntError),
    Float(ParseFloatError),
}

struct Mode3_2Error;

macro_rules! series_cast {
    ($series:expr, $from:ident, $to:ty) => {
        $series
            .$from()
            .unwrap()
            .into_no_null_iter()
            .map(|x| x as $to)
            .collect()
    };
}

fn warn_bitmask<T: Ord + Copy>(xs: Vec<T>, deferred: &mut PureErrorBuf, bitmask: T) -> Vec<T> {
    let mut has_seen = false;
    xs.into_iter()
        .map(|x| {
            if x > bitmask && !has_seen {
                deferred.push_warning("bitmask exceed, value truncated".to_string());
                has_seen = true
            }
            x.min(bitmask)
        })
        .collect()
}

macro_rules! convert_to_uint1 {
    ($series:expr, $deferred:expr, $wrap:ident, $from:ident, $to:ty, $ut:expr) => {
        $wrap(NumColumnWriter {
            column: warn_bitmask(
                series_cast!($series, $from, $to),
                &mut $deferred,
                $ut.bitmask,
            ),
            size: $ut.size,
        })
    };
}

macro_rules! convert_to_uint {
    ($size:expr, $series:expr, $from:ident, $deferred:expr) => {
        match $size {
            AnyUintType::Uint8(ut) => {
                convert_to_uint1!($series, $deferred, NumU8, $from, u8, ut)
            }
            AnyUintType::Uint16(ut) => {
                convert_to_uint1!($series, $deferred, NumU16, $from, u16, ut)
            }
            AnyUintType::Uint24(ut) => {
                convert_to_uint1!($series, $deferred, NumU24, $from, u32, ut)
            }
            AnyUintType::Uint32(ut) => {
                convert_to_uint1!($series, $deferred, NumU32, $from, u32, ut)
            }
            AnyUintType::Uint40(ut) => {
                convert_to_uint1!($series, $deferred, NumU40, $from, u64, ut)
            }
            AnyUintType::Uint48(ut) => {
                convert_to_uint1!($series, $deferred, NumU48, $from, u64, ut)
            }
            AnyUintType::Uint56(ut) => {
                convert_to_uint1!($series, $deferred, NumU56, $from, u64, ut)
            }
            AnyUintType::Uint64(ut) => {
                convert_to_uint1!($series, $deferred, NumU64, $from, u64, ut)
            }
        }
    };
}

macro_rules! convert_to_float {
    ($size:expr, $series:expr, $wrap:ident, $from:ident, $to:ty) => {
        $wrap(NumColumnWriter {
            column: series_cast!($series, $from, $to),
            size: $size,
        })
    };
}

macro_rules! convert_to_f32 {
    ($size:expr, $series:expr, $from:ident) => {
        convert_to_float!($size, $series, NumF32, $from, f32)
    };
}

macro_rules! convert_to_f64 {
    ($size:expr, $series:expr, $from:ident) => {
        convert_to_float!($size, $series, NumF64, $from, f64)
    };
}

pub trait Versioned {
    fn fcs_version() -> Version;
}

pub trait VersionedMetadata: Sized
where
    Self: VersionedParserMetadata,
    Self::P: VersionedMeasurement,
    Self::P: VersionedParserMeasurement,
{
    type P;

    fn timestamps_valid(&self) -> bool;

    fn datetimes_valid(&self) -> bool;

    fn check_unstainedcenters(&self, names: &HashSet<&Shortname>) -> Option<String>;

    fn check_spillover(&self, names: &HashSet<&Shortname>) -> Option<String>;

    fn has_timestep(&self) -> bool;

    fn begin_date(&self) -> Option<NaiveDate>;

    fn end_date(&self) -> Option<NaiveDate>;

    fn begin_time(&self) -> Option<NaiveTime>;

    fn end_time(&self) -> Option<NaiveTime>;

    fn set_datetimes(&mut self, begin: DateTime<FixedOffset>, end: DateTime<FixedOffset>) -> bool {
        if begin > end {
            false
        } else {
            self.set_datetimes_inner(begin, end);
            true
        }
    }

    fn set_datetimes_inner(&mut self, begin: DateTime<FixedOffset>, end: DateTime<FixedOffset>);

    fn clear_datetimes(&mut self);

    fn into_any(s: CoreTEXT<Self, Self::P>) -> AnyCoreTEXT;

    fn as_writer_data_layout(
        m: &Metadata<Self>,
        ms: &[Measurement<Self::P>],
    ) -> Result<WriterDataLayout, Vec<String>> {
        let dt = m.datatype;
        let byteord = Self::byteord(&m.specific);
        let ncols = ms.len();
        let (pass, fail): (Vec<_>, Vec<_>) = ms
            .iter()
            .map(|m| Self::P::as_column_type(m, dt, &byteord))
            .partition_result();
        let mut deferred: Vec<_> = fail.into_iter().flatten().collect();
        if pass.len() == ncols {
            let fixed: Vec<_> = pass.into_iter().flatten().collect();
            let nfixed = fixed.len();
            if nfixed == ncols {
                return Ok(DataLayout::AlphaNum {
                    nrows: (),
                    columns: fixed,
                });
            } else if nfixed == 0 {
                return Ok(DataLayout::AsciiDelimited { nrows: None, ncols });
            } else {
                deferred.push(format!(
                    "{nfixed} out of {ncols} measurements are fixed width"
                ));
            }
        }
        Err(deferred)
    }

    // TODO not DRY
    fn as_reader_data_layout_bare(
        m: &BareMetadata<Self::Target>,
        ms: &[DataReadMeasurement<<Self::P as VersionedParserMeasurement>::Target>],
        data_nbytes: usize,
        conf: &DataReadConfig,
    ) -> PureMaybe<ReaderDataLayout> {
        let dt = m.datatype;
        let byteord = Self::target_byteord(&m.specific);
        let ncols = ms.len();
        let (pass, fail): (Vec<_>, Vec<_>) = ms
            .iter()
            .map(|m| Self::P::as_column_type_from_bare(m, dt, &byteord))
            .partition_result();
        let mut deferred =
            PureErrorBuf::from_many(fail.into_iter().flatten().collect(), PureErrorLevel::Error);
        if pass.len() == ncols {
            let fixed: Vec<_> = pass.into_iter().flatten().collect();
            let nfixed = fixed.len();
            if nfixed == ncols {
                let event_width = fixed.iter().map(|c| c.width()).sum();
                return Self::total_events(&m.specific, data_nbytes, event_width, conf).and_then(
                    |nrows| {
                        PureSuccess::from(Some(DataLayout::AlphaNum {
                            nrows,
                            columns: fixed,
                        }))
                    },
                );
            } else if nfixed == 0 {
                let nrows = Self::tot(&m.specific);
                return PureSuccess::from(Some(DataLayout::AsciiDelimited { nrows, ncols }));
            } else {
                deferred.push_error(format!(
                    "{nfixed} out of {ncols} measurements are fixed width"
                ));
            }
        }
        PureSuccess {
            data: None,
            deferred,
        }
    }

    fn lookup_specific(st: &mut KwParser, par: usize) -> Option<Self>;

    fn lookup_metadata(st: &mut KwParser, ms: &[Measurement<Self::P>]) -> Option<Metadata<Self>> {
        let par = ms.len();
        let maybe_datatype = st.lookup_datatype();
        let maybe_specific = Self::lookup_specific(st, par);
        if let (Some(datatype), Some(specific)) = (maybe_datatype, maybe_specific) {
            Some(Metadata {
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
                nonstandard_keywords: st.lookup_all_nonstandard(),
                specific,
            })
        } else {
            None
        }
    }

    fn keywords_req_inner(&self) -> Vec<(&'static str, String)>;

    fn keywords_opt_inner(&self) -> Vec<(&'static str, String)>;

    fn all_req_keywords(m: &Metadata<Self>, par: usize) -> RawPairs {
        let fixed = [(PAR, par.to_string()), (DATATYPE, m.datatype.to_string())];
        fixed
            .into_iter()
            .chain(m.specific.keywords_req_inner())
            .map(|(k, v)| (k.to_string(), v))
            .collect()
    }

    fn all_opt_keywords(m: &Metadata<Self>) -> RawPairs {
        [
            (ABRT, m.abrt.as_opt_string()),
            (COM, m.com.as_opt_string()),
            (CELLS, m.cells.as_opt_string()),
            (EXP, m.exp.as_opt_string()),
            (FIL, m.fil.as_opt_string()),
            (INST, m.inst.as_opt_string()),
            (LOST, m.lost.as_opt_string()),
            (OP, m.op.as_opt_string()),
            (PROJ, m.proj.as_opt_string()),
            (SMNO, m.smno.as_opt_string()),
            (SRC, m.src.as_opt_string()),
            (SYS, m.sys.as_opt_string()),
            (TR, m.tr.as_opt_string()),
        ]
        .into_iter()
        .flat_map(|(k, v)| v.map(|x| (k, x)))
        .chain(m.specific.keywords_opt_inner())
        .map(|(k, v)| (k.to_string(), v))
        // TODO useless clone
        .chain(
            m.nonstandard_keywords
                .iter()
                .map(|(k, v)| (k.as_ref().to_string(), v.clone())),
        )
        .collect()
    }
}

trait VersionedMeasurement: Sized + Versioned {
    fn lookup_specific(st: &mut KwParser, n: usize) -> Option<Self>;

    fn has_linear_scale(&self) -> bool;

    fn has_gain(&self) -> bool;

    fn maybe_name(p: &Measurement<Self>) -> Option<&Shortname>;

    fn shortname(p: &Measurement<Self>, n: usize) -> Shortname;

    fn set_shortname(m: &mut Measurement<Self>, n: Shortname);

    fn longname(p: &Measurement<Self>, n: usize) -> Longname {
        // TODO not DRY
        p.longname
            .0
            .as_ref()
            .cloned()
            .unwrap_or(Longname(format!("M{n}")))
    }

    fn set_longname(m: &mut Measurement<Self>, n: Option<String>) {
        m.longname = n.map(|y| y.into()).into();
    }

    fn lookup_measurements(st: &mut KwParser, par: usize) -> Option<Vec<Measurement<Self>>> {
        let v = Self::fcs_version();
        let ps: Vec<_> = (1..(par + 1))
            .flat_map(|n| {
                let maybe_bytes = st.lookup_meas_bytes(n);
                let maybe_range = st.lookup_meas_range(n);
                let maybe_specific = Self::lookup_specific(st, n);
                if let (Some(bytes), Some(range), Some(specific)) =
                    (maybe_bytes, maybe_range, maybe_specific)
                {
                    Some(Measurement {
                        bytes,
                        range,
                        longname: st.lookup_meas_longname(n),
                        filter: st.lookup_meas_filter(n),
                        power: st.lookup_meas_power(n),
                        detector_type: st.lookup_meas_detector_type(n),
                        percent_emitted: st.lookup_meas_percent_emitted(n, v == Version::FCS3_2),
                        detector_voltage: st.lookup_meas_detector_voltage(n),
                        specific,
                        nonstandard_keywords: st.lookup_all_meas_nonstandard(n),
                    })
                } else {
                    None
                }
            })
            .collect();
        if ps.len() == par {
            Some(ps)
        } else {
            None
        }
    }

    fn req_suffixes_inner(&self) -> Vec<(&'static str, String)>;

    fn opt_suffixes_inner(&self) -> Vec<(&'static str, Option<String>)>;

    fn req_suffixes(m: &Measurement<Self>) -> Vec<(&'static str, String)> {
        [
            (BYTES_SFX, m.bytes.to_string()),
            (RANGE_SFX, m.range.to_string()),
        ]
        .into_iter()
        .chain(m.specific.req_suffixes_inner())
        .collect()
    }

    fn opt_suffixes(m: &Measurement<Self>) -> Vec<(&'static str, Option<String>)> {
        [
            (LONGNAME_SFX, m.longname.as_opt_string()),
            (FILTER_SFX, m.filter.as_opt_string()),
            (POWER_SFX, m.power.as_opt_string()),
            (DET_TYPE_SFX, m.detector_type.as_opt_string()),
            (PCNT_EMT_SFX, m.percent_emitted.as_opt_string()),
            (DET_VOLT_SFX, m.detector_voltage.as_opt_string()),
        ]
        .into_iter()
        .chain(m.specific.opt_suffixes_inner())
        .collect()
    }

    // for table
    fn keywords(m: &Measurement<Self>, n: &str) -> Vec<(String, Option<String>)> {
        Self::req_suffixes(m)
            .into_iter()
            .map(|(k, v)| (k, Some(v)))
            .chain(Self::opt_suffixes(m))
            .map(|(s, v)| (format_measurement(n, s), v))
            .collect()
    }

    // TODO this name is weird, this is standard+nonstandard keywords
    // after filtering out None values
    fn req_keywords(m: &Measurement<Self>, n: &str) -> RawPairs {
        Self::req_suffixes(m)
            .into_iter()
            .map(|(s, v)| (format_measurement(n, s), v))
            .collect()
    }

    fn opt_keywords(m: &Measurement<Self>, n: &str) -> RawPairs {
        Self::opt_suffixes(m)
            .into_iter()
            .filter_map(|(k, v)| v.map(|x| (k, x)))
            .map(|(s, v)| (format_measurement(n, s), v))
            // TODO useless clone?
            .chain(
                m.nonstandard_keywords
                    .iter()
                    .map(|(k, v)| (k.as_ref().to_string(), v.clone())),
            )
            .collect()
    }
}

trait VersionedParserMeasurement: Sized {
    type Target;

    fn datatype(m: &Measurement<Self>) -> Option<NumType>;

    fn datatype_minimal(m: &DataReadMeasurement<Self::Target>) -> Option<NumType>;

    fn as_minimal_inner(m: &Measurement<Self>) -> Self::Target;

    fn as_minimal(m: &Measurement<Self>) -> DataReadMeasurement<Self::Target> {
        DataReadMeasurement {
            bytes: m.bytes,
            range: m.range,
            specific: Self::as_minimal_inner(m),
        }
    }

    fn as_column_type(
        m: &Measurement<Self>,
        dt: AlphaNumType,
        byteord: &ByteOrd,
    ) -> Result<Option<ColumnType>, Vec<String>> {
        let mdt = Self::datatype(m).map(|d| d.into()).unwrap_or(dt);
        let rng = m.range;
        Self::to_col_type(m.bytes, mdt, byteord, rng)
    }

    // TODO make errors index-specific
    fn as_column_type_from_bare(
        m: &DataReadMeasurement<Self::Target>,
        dt: AlphaNumType,
        byteord: &ByteOrd,
    ) -> Result<Option<ColumnType>, Vec<String>> {
        let mdt = Self::datatype_minimal(m).map(|d| d.into()).unwrap_or(dt);
        let rng = m.range;
        Self::to_col_type(m.bytes, mdt, byteord, rng)
    }

    fn to_col_type(
        b: Bytes,
        dt: AlphaNumType,
        byteord: &ByteOrd,
        rng: Range,
    ) -> Result<Option<ColumnType>, Vec<String>> {
        match b {
            Bytes::Fixed(bytes) => match dt {
                AlphaNumType::Ascii => {
                    if bytes > 20 {
                        Ok(ColumnType::Ascii { bytes })
                    } else {
                        Err(vec![
                            "$DATATYPE=A but $PnB greater than 20 bytes".to_string()
                        ])
                    }
                }
                AlphaNumType::Integer => {
                    make_uint_type(bytes, rng, byteord).map(ColumnType::Integer)
                }
                AlphaNumType::Single => {
                    if bytes == 4 {
                        Float32Type::to_float_byteord(byteord)
                            .map(ColumnType::Float)
                            .map_err(|e| vec![e])
                    } else {
                        Err(vec!["$DATATYPE=F but $PnB=8".to_string()])
                    }
                }
                AlphaNumType::Double => {
                    if bytes == 8 {
                        Float64Type::to_float_byteord(byteord)
                            .map(ColumnType::Double)
                            .map_err(|e| vec![e])
                    } else {
                        Err(vec!["$DATATYPE=D but $PnB=8".to_string()])
                    }
                }
            }
            .map(Some),
            Bytes::Variable => match dt {
                // ASSUME the only way this can happen is if $DATATYPE=A since
                // Ascii is not allowed in $PnDATATYPE.
                AlphaNumType::Ascii => Ok(None),
                _ => Err(vec![format!("variable $PnB not allowed for {dt}")]),
            },
        }
    }
}

trait VersionedParserMetadata: Sized {
    type Target;

    fn as_minimal_inner(&self, st: &mut KwParser) -> Option<Self::Target>;

    fn as_minimal(md: &Metadata<Self>, st: &mut KwParser) -> Option<BareMetadata<Self::Target>> {
        let datatype = md.datatype;
        let specific = Self::as_minimal_inner(&md.specific, st)?;
        Some(BareMetadata { datatype, specific })
    }

    fn byteord(&self) -> ByteOrd;

    fn target_byteord(t: &Self::Target) -> ByteOrd;

    fn tot(t: &Self::Target) -> Option<usize>;

    fn total_events(
        t: &Self::Target,
        data_nbytes: usize,
        event_width: usize,
        conf: &DataReadConfig,
    ) -> PureSuccess<usize> {
        let mut def = PureErrorBuf::default();
        let remainder = data_nbytes % event_width;
        let total_events = data_nbytes / event_width;
        if data_nbytes % event_width > 0 {
            let msg = format!(
                "Events are {event_width} bytes wide, but this does not evenly \
                 divide DATA segment which is {data_nbytes} bytes long \
                 (remainder of {remainder})"
            );
            def.push_msg_leveled(msg, conf.enfore_data_width_divisibility)
        }
        if let Some(tot) = Self::tot(t) {
            if total_events != tot {
                let msg = format!(
                    "$TOT field is {tot} but number of events \
                         that evenly fit into DATA is {total_events}"
                );
                def.push_msg_leveled(msg, conf.enfore_matching_tot);
            }
        }
        PureSuccess {
            data: total_events,
            deferred: def,
        }
    }
}

trait IntMath: Sized + fmt::Display {
    fn next_power_2(x: Self) -> Self;

    fn maxval() -> Self;

    fn write_ascii_int<W: Write>(h: &mut BufWriter<W>, bytes: u8, x: Self) -> io::Result<()> {
        let s = x.to_string();
        // ASSUME bytes has been ensured to be able to hold the largest digit
        // expressible with this type, which means this will never be negative
        let offset = usize::from(bytes) - s.len();
        let mut buf: Vec<u8> = vec![0, bytes];
        for (i, c) in s.bytes().enumerate() {
            buf[offset + i] = c;
        }
        h.write_all(&buf)
    }
}

trait NumProps<const DTLEN: usize>: Sized + Copy {
    fn zero() -> Self;

    fn from_big(buf: [u8; DTLEN]) -> Self;

    fn from_little(buf: [u8; DTLEN]) -> Self;

    fn to_big(self) -> [u8; DTLEN];

    fn to_little(self) -> [u8; DTLEN];

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
        if endian == Endian::Big {
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

    fn write_from_ordered<W: Write>(
        h: &mut BufWriter<W>,
        order: &[u8; OLEN],
        x: Self,
    ) -> io::Result<()> {
        let tmp = Self::to_little(x);
        let mut buf = [0; OLEN];
        for (i, j) in order.iter().enumerate() {
            buf[usize::from(*j)] = tmp[i];
        }
        h.write_all(&tmp)
    }
}

trait IntFromBytes<const DTLEN: usize, const INTLEN: usize>
where
    Self::Native: NumProps<DTLEN>,
    Self::Native: OrderedFromBytes<DTLEN, INTLEN>,
    Self::Native: TryFrom<u64>,
    Self::Native: IntMath,
    Self::Native: Ord,
    Self: PolarsNumericType,
    ChunkedArray<Self>: IntoSeries,
{
    fn byteord_to_sized(byteord: &ByteOrd) -> Result<SizedByteOrd<INTLEN>, String> {
        byteord_to_sized(byteord)
    }

    fn range_to_bitmask(range: Range) -> Result<Self::Native, String> {
        match range {
            Range::Float(_) => Err("$PnR is float for an integer column".to_string()),
            // TODO this seems sloppy
            Range::Int(i) => Ok(Self::Native::try_from(i)
                .map(Self::Native::next_power_2)
                .unwrap_or(Self::Native::maxval())),
        }
    }

    fn to_col(
        range: Range,
        byteord: &ByteOrd,
    ) -> Result<UintType<Self::Native, INTLEN>, Vec<String>> {
        // TODO be more specific, which means we need the measurement index
        let b = Self::range_to_bitmask(range);
        let s = Self::byteord_to_sized(byteord);
        match (b, s) {
            (Ok(bitmask), Ok(size)) => Ok(UintType { bitmask, size }),
            (Err(x), Err(y)) => Err(vec![x, y]),
            (Err(x), _) => Err(vec![x]),
            (_, Err(y)) => Err(vec![y]),
        }
    }

    fn read_int_masked<R: Read>(
        h: &mut BufReader<R>,
        byteord: &SizedByteOrd<INTLEN>,
        bitmask: Self::Native,
    ) -> io::Result<Self::Native> {
        Self::read_int(h, byteord).map(|x| x.min(bitmask))
    }

    fn read_int<R: Read>(
        h: &mut BufReader<R>,
        byteord: &SizedByteOrd<INTLEN>,
    ) -> io::Result<Self::Native> {
        // This lovely code will read data that is not a power-of-two
        // bytes long. Start by reading n bytes into a vector, which can
        // take a varying size. Then copy this into the power of 2 buffer
        // and reset all the unused cells to 0. This copy has to go to one
        // or the other end of the buffer depending on endianness.
        //
        // ASSUME for u8 and u16 that these will get heavily optimized away
        // since 'order' is totally meaningless for u8 and the only two possible
        // 'orders' for u16 are big and little.
        match byteord {
            SizedByteOrd::Endian(e) => {
                let mut tmp = [0; INTLEN];
                let mut buf = [0; DTLEN];
                h.read_exact(&mut tmp)?;
                Ok(if *e == Endian::Big {
                    let b = DTLEN - INTLEN;
                    buf[b..].copy_from_slice(&tmp[b..]);
                    Self::Native::from_big(buf)
                } else {
                    buf[..INTLEN].copy_from_slice(&tmp[..INTLEN]);
                    Self::Native::from_little(buf)
                })
            }
            SizedByteOrd::Order(order) => Self::Native::read_from_ordered(h, order),
        }
    }

    fn read_to_column<R: Read>(
        h: &mut BufReader<R>,
        d: &mut UintColumnReader<Self::Native, INTLEN>,
        row: usize,
    ) -> io::Result<()> {
        d.column[row] = Self::read_int_masked(h, &d.layout.size, d.layout.bitmask)?;
        Ok(())
    }

    fn write_int<W: Write>(
        h: &mut BufWriter<W>,
        byteord: &SizedByteOrd<INTLEN>,
        x: Self::Native,
    ) -> io::Result<()> {
        match byteord {
            SizedByteOrd::Endian(e) => {
                let mut buf = [0; INTLEN];
                let (start, end, tmp) = if *e == Endian::Big {
                    ((DTLEN - INTLEN), DTLEN, Self::Native::to_big(x))
                } else {
                    (0, INTLEN, Self::Native::to_little(x))
                };
                buf[..].copy_from_slice(&tmp[start..end]);
                h.write_all(&buf)
            }
            SizedByteOrd::Order(order) => Self::Native::write_from_ordered(h, order, x),
        }
    }
}

trait FloatFromBytes<const LEN: usize>
where
    Self::Native: NumProps<LEN>,
    Self::Native: OrderedFromBytes<LEN, LEN>,
    Self: Clone,
    Self: PolarsNumericType,
    ChunkedArray<Self>: IntoSeries,
{
    /// Read one sequence of bytes as a float and assign to a column.
    fn read_to_column<R: Read>(
        h: &mut BufReader<R>,
        column: &mut FloatColumnReader<Self::Native, LEN>,
        row: usize,
    ) -> io::Result<()> {
        column.column[row] = Self::read_float(h, &column.order)?;
        Ok(())
    }

    /// Read byte sequence into a matrix of floats
    fn read_matrix<R: Read>(h: &mut BufReader<R>, p: FloatReader<LEN>) -> io::Result<DataFrame> {
        let mut columns: Vec<_> = iter::repeat_with(|| vec![Self::Native::zero(); p.nrows])
            .take(p.ncols)
            .collect();
        for row in 0..p.nrows {
            for column in columns.iter_mut() {
                column[row] = Self::read_float(h, &p.byteord)?;
            }
        }
        let ss: Vec<_> = columns
            .into_iter()
            .enumerate()
            .map(|(i, s)| {
                ChunkedArray::<Self>::from_vec(format!("M{i}").into(), s)
                    .into_series()
                    .into()
            })
            .collect();
        DataFrame::new(ss).map_err(|e| io::Error::other(e.to_string()))
        // Ok(Dataframe::from(
        //     columns.into_iter().map(Vec::<Self>::into).collect(),
        // ))
    }

    /// Make configuration to read one column of floats in a dataset.
    fn make_column_reader(
        order: SizedByteOrd<LEN>,
        total_events: usize,
    ) -> FloatColumnReader<Self::Native, LEN> {
        FloatColumnReader {
            column: vec![Self::Native::zero(); total_events],
            order,
        }
    }

    fn to_float_byteord(byteord: &ByteOrd) -> Result<SizedByteOrd<LEN>, String> {
        byteord_to_sized(byteord)
    }

    fn make_matrix_parser(
        byteord: &ByteOrd,
        par: usize,
        total_events: usize,
    ) -> PureMaybe<FloatReader<LEN>> {
        let res = Self::to_float_byteord(byteord).map(|byteord| FloatReader {
            nrows: total_events,
            ncols: par,
            byteord,
        });
        PureMaybe::from_result_1(res, PureErrorLevel::Error)
    }

    fn read_float<R: Read>(
        h: &mut BufReader<R>,
        byteord: &SizedByteOrd<LEN>,
    ) -> io::Result<Self::Native> {
        match byteord {
            SizedByteOrd::Endian(e) => {
                let mut buf = [0; LEN];
                h.read_exact(&mut buf)?;
                Ok(if *e == Endian::Big {
                    Self::Native::from_big(buf)
                } else {
                    Self::Native::from_little(buf)
                })
            }
            SizedByteOrd::Order(order) => Self::Native::read_from_ordered(h, order),
        }
    }

    fn write_float<W: Write>(
        h: &mut BufWriter<W>,
        byteord: &SizedByteOrd<LEN>,
        x: Self::Native,
    ) -> io::Result<()> {
        match byteord {
            SizedByteOrd::Endian(e) => {
                let buf: [u8; LEN] = if *e == Endian::Big {
                    Self::Native::to_big(x)
                } else {
                    Self::Native::to_little(x)
                };
                h.write_all(&buf)
            }
            SizedByteOrd::Order(order) => Self::Native::write_from_ordered(h, order, x),
        }
    }
}

impl ColumnType {
    fn width(&self) -> usize {
        match self {
            ColumnType::Ascii { bytes } => usize::from(*bytes),
            ColumnType::Integer(ut) => usize::from(ut.nbytes()),
            ColumnType::Float(_) => 4,
            ColumnType::Double(_) => 8,
        }
    }

    fn datatype(&self) -> AlphaNumType {
        match self {
            ColumnType::Ascii { bytes: _ } => AlphaNumType::Ascii,
            ColumnType::Integer(_) => AlphaNumType::Integer,
            ColumnType::Float(_) => AlphaNumType::Single,
            ColumnType::Double(_) => AlphaNumType::Double,
        }
    }
}

impl<T> DataLayout<T> {
    fn ncols(&self) -> usize {
        match self {
            DataLayout::AsciiDelimited { nrows: _, ncols } => *ncols,
            DataLayout::AlphaNum { nrows: _, columns } => columns.len(),
        }
    }
}

impl AnyUintType {
    fn native_nbytes(&self) -> u8 {
        match self {
            AnyUintType::Uint8(_) => 1,
            AnyUintType::Uint16(_) => 2,
            AnyUintType::Uint24(_) => 4,
            AnyUintType::Uint32(_) => 4,
            AnyUintType::Uint40(_) => 8,
            AnyUintType::Uint48(_) => 8,
            AnyUintType::Uint56(_) => 8,
            AnyUintType::Uint64(_) => 8,
        }
    }

    fn nbytes(&self) -> u8 {
        match self {
            AnyUintType::Uint8(_) => 1,
            AnyUintType::Uint16(_) => 2,
            AnyUintType::Uint24(_) => 3,
            AnyUintType::Uint32(_) => 4,
            AnyUintType::Uint40(_) => 5,
            AnyUintType::Uint48(_) => 6,
            AnyUintType::Uint56(_) => 7,
            AnyUintType::Uint64(_) => 8,
        }
    }
}

fn format_measurement(n: &str, m: &str) -> String {
    format!("$P{}{}", n, m)
}

impl fmt::Display for FCSDateTimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "must be formatted like 'yyyy-mm-ddThh:mm:ss[TZD]'")
    }
}

impl str::FromStr for FCSDateTime {
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

impl str::FromStr for FCSTime {
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

impl str::FromStr for FCSTime60 {
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

impl str::FromStr for FCSTime100 {
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

impl From<NumType> for AlphaNumType {
    fn from(value: NumType) -> Self {
        match value {
            NumType::Integer => AlphaNumType::Integer,
            NumType::Single => AlphaNumType::Single,
            NumType::Double => AlphaNumType::Double,
        }
    }
}

impl FromStr for Compensation {
    type Err = FixedSeqError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut xs = s.split(",");
        if let Some(first) = &xs.next().and_then(|x| x.parse::<usize>().ok()) {
            let n = *first;
            let nn = n * n;
            let values: Vec<_> = xs.by_ref().take(nn).collect();
            let remainder = xs.by_ref().count();
            let total = values.len() + remainder;
            if total != nn {
                Err(FixedSeqError::WrongLength {
                    expected: nn,
                    total,
                })
            } else {
                let fvalues: Vec<_> = values
                    .into_iter()
                    .filter_map(|x| x.parse::<f32>().ok())
                    .collect();
                if fvalues.len() != nn {
                    Err(FixedSeqError::BadFloat)
                } else {
                    let matrix = DMatrix::from_row_iterator(n, n, fvalues);
                    Ok(Compensation { matrix })
                }
            }
        } else {
            Err(FixedSeqError::BadLength)
        }
    }
}

impl fmt::Display for Compensation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let n = self.matrix.len();
        // DMatrix slices are column major, so transpose first to output
        // row-major
        let xs = self.matrix.transpose().as_slice().iter().join(",");
        write!(f, "{n},{xs}")
    }
}

impl fmt::Display for FixedSeqError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            FixedSeqError::BadFloat => write!(f, "Float could not be parsed"),
            FixedSeqError::WrongLength { total, expected } => {
                write!(f, "Expected {expected} entries, found {total}")
            }
            FixedSeqError::BadLength => write!(f, "Could not determine length"),
        }
    }
}

impl FromStr for Spillover {
    type Err = NamedFixedSeqError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        {
            let mut xs = s.split(",");
            if let Some(first) = &xs.next().and_then(|x| x.parse::<usize>().ok()) {
                let n = *first;
                let nn = n * n;
                let expected = n + nn;
                // This should be safe since we split on commas
                let measurements: Vec<_> =
                    xs.by_ref().take(n).map(Shortname::new_unchecked).collect();
                let values: Vec<_> = xs.by_ref().take(nn).collect();
                let remainder = xs.by_ref().count();
                let total = measurements.len() + values.len() + remainder;
                if total != expected {
                    Err(NamedFixedSeqError::Seq(FixedSeqError::WrongLength {
                        total,
                        expected,
                    }))
                } else if measurements.iter().unique().count() != n {
                    Err(NamedFixedSeqError::NonUnique)
                } else {
                    let fvalues: Vec<_> = values
                        .into_iter()
                        .filter_map(|x| x.parse::<f32>().ok())
                        .collect();
                    if fvalues.len() != nn {
                        Err(NamedFixedSeqError::Seq(FixedSeqError::BadFloat))
                    } else {
                        let matrix = DMatrix::from_row_iterator(n, n, fvalues);
                        Ok(Spillover {
                            measurements,
                            matrix,
                        })
                    }
                }
            } else {
                Err(NamedFixedSeqError::Seq(FixedSeqError::BadLength))
            }
        }
    }
}

impl fmt::Display for Spillover {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let n = self.measurements.len();
        // DMatrix slices are column major, so transpose first to output
        // row-major
        let xs = self.matrix.transpose().as_slice().iter().join(",");
        write!(f, "{n},{xs}")
    }
}

impl fmt::Display for NamedFixedSeqError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            NamedFixedSeqError::Seq(s) => write!(f, "{}", s),
            NamedFixedSeqError::NonUnique => write!(f, "Names in sequence is not unique"),
        }
    }
}

impl Spillover {
    fn table(&self, delim: &str) -> Vec<String> {
        let header0 = vec!["[-]"];
        let header = header0
            .into_iter()
            .chain(self.measurements.iter().map(|m| m.as_ref()))
            .join(delim);
        let lines = vec![header];
        let rows = self.matrix.row_iter().map(|xs| xs.iter().join(delim));
        lines.into_iter().chain(rows).collect()
    }

    fn print_table(&self, delim: &str) {
        for e in self.table(delim) {
            println!("{}", e);
        }
    }
}

impl fmt::Display for EndianError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "Endian must be either 1,2,3,4 or 4,3,2,1")
    }
}

impl<const LEN: usize> From<Endian> for SizedByteOrd<LEN> {
    fn from(value: Endian) -> Self {
        SizedByteOrd::Endian(value)
    }
}

impl FromStr for Endian {
    type Err = EndianError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "1,2,3,4" => Ok(Endian::Little),
            "4,3,2,1" => Ok(Endian::Big),
            _ => Err(EndianError),
        }
    }
}

impl fmt::Display for Endian {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let x = match self {
            Endian::Big => "4,3,2,1",
            Endian::Little => "1,2,3,4",
        };
        write!(f, "{x}")
    }
}

impl fmt::Display for ParseByteOrdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            ParseByteOrdError::InvalidNumbers => write!(f, "Could not parse numbers in byte order"),
            ParseByteOrdError::InvalidOrder => write!(f, "Byte order must include 1-n uniquely"),
        }
    }
}

impl FromStr for ByteOrd {
    type Err = ParseByteOrdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.parse() {
            Ok(e) => Ok(ByteOrd::Endian(e)),
            _ => {
                let xs: Vec<_> = s.split(",").collect();
                let nxs = xs.len();
                let xs_num: Vec<u8> = xs.iter().filter_map(|s| s.parse().ok()).unique().collect();
                if let (Some(min), Some(max)) = (xs_num.iter().min(), xs_num.iter().max()) {
                    if *min == 1 && usize::from(*max) == nxs && xs_num.len() == nxs {
                        Ok(ByteOrd::Mixed(xs_num.iter().map(|x| x - 1).collect()))
                    } else {
                        Err(ParseByteOrdError::InvalidOrder)
                    }
                } else {
                    Err(ParseByteOrdError::InvalidNumbers)
                }
            }
        }
    }
}

impl fmt::Display for ByteOrd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            ByteOrd::Endian(e) => write!(f, "{}", e),
            ByteOrd::Mixed(xs) => write!(f, "{}", xs.iter().join(",")),
        }
    }
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

use Scale::*;

impl fmt::Display for Scale {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Scale::Log { decades, offset } => write!(f, "{decades},{offset}"),
            Scale::Linear => write!(f, "Lin"),
        }
    }
}

impl fmt::Display for ScaleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            ScaleError::FloatError(x) => write!(f, "{}", x),
            ScaleError::WrongFormat => write!(f, "must be like 'f1,f2'"),
        }
    }
}

impl str::FromStr for Scale {
    type Err = ScaleError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.split(",").collect::<Vec<_>>()[..] {
            [ds, os] => {
                let f1 = ds.parse().map_err(ScaleError::FloatError)?;
                let f2 = os.parse().map_err(ScaleError::FloatError)?;
                match (f1, f2) {
                    (0.0, 0.0) => Ok(Linear),
                    (decades, offset) => Ok(Log { decades, offset }),
                }
            }
            _ => Err(ScaleError::WrongFormat),
        }
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

impl str::FromStr for Display {
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

impl str::FromStr for Calibration3_1 {
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

impl str::FromStr for Calibration3_2 {
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

impl str::FromStr for MeasurementType {
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

impl str::FromStr for Feature {
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

impl str::FromStr for Wavelengths {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut ws = vec![];
        for x in s.split(",") {
            ws.push(x.parse()?);
        }
        Ok(Wavelengths(ws))
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

impl str::FromStr for Range {
    type Err = RangeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.parse::<u64>() {
            Ok(x) => Ok(Range::Int(x - 1)),
            Err(e) => match e.kind() {
                IntErrorKind::InvalidDigit => s
                    .parse::<f64>()
                    .map_or_else(|e| Err(RangeError::Float(e)), |x| Ok(Range::Float(x))),
                IntErrorKind::PosOverflow => Ok(Range::Int(u64::MAX)),
                _ => Err(RangeError::Int(e)),
            },
        }
    }
}

impl fmt::Display for Range {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Range::Int(x) => write!(f, "{x}"),
            Range::Float(x) => write!(f, "{x}"),
        }
    }
}

impl<P: VersionedMeasurement> Measurement<P> {
    fn table_header(&self) -> Vec<String> {
        vec![String::from("index")]
            .into_iter()
            .chain(P::keywords(self, "n").into_iter().map(|(k, _)| k))
            .collect()
    }

    fn table_row(&self, n: usize) -> Vec<Option<String>> {
        vec![Some(n.to_string())]
            .into_iter()
            // NOTE; the "n" is a dummy and never used
            .chain(P::keywords(self, "n").into_iter().map(|(_, v)| v))
            .collect()
    }
}

fn make_uint_type(b: u8, r: Range, o: &ByteOrd) -> Result<AnyUintType, Vec<String>> {
    match b {
        1 => UInt8Type::to_col(r, o).map(AnyUintType::Uint8),
        2 => UInt16Type::to_col(r, o).map(AnyUintType::Uint16),
        3 => <UInt32Type as IntFromBytes<4, 3>>::to_col(r, o).map(AnyUintType::Uint24),
        4 => <UInt32Type as IntFromBytes<4, 4>>::to_col(r, o).map(AnyUintType::Uint32),
        5 => <UInt64Type as IntFromBytes<8, 5>>::to_col(r, o).map(AnyUintType::Uint40),
        6 => <UInt64Type as IntFromBytes<8, 6>>::to_col(r, o).map(AnyUintType::Uint48),
        7 => <UInt64Type as IntFromBytes<8, 7>>::to_col(r, o).map(AnyUintType::Uint56),
        8 => <UInt64Type as IntFromBytes<8, 8>>::to_col(r, o).map(AnyUintType::Uint64),
        _ => Err(vec!["$PnB has invalid byte length".to_string()]),
    }
}

impl Versioned for InnerMeasurement2_0 {
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

impl VersionedMeasurement for InnerMeasurement2_0 {
    fn maybe_name(p: &Measurement<Self>) -> Option<&Shortname> {
        p.specific.shortname.0.as_ref()
    }

    fn has_linear_scale(&self) -> bool {
        self.scale.0.as_ref().map_or(false, |x| *x == Scale::Linear)
    }

    fn has_gain(&self) -> bool {
        false
    }

    fn shortname(p: &Measurement<Self>, n: usize) -> Shortname {
        p.specific
            .shortname
            .0
            .as_ref()
            .cloned()
            .unwrap_or(Shortname::from_index(n))
    }

    fn set_shortname(m: &mut Measurement<Self>, n: Shortname) {
        m.specific.shortname = Some(n).into();
    }

    fn lookup_specific(st: &mut KwParser, n: usize) -> Option<InnerMeasurement2_0> {
        Some(InnerMeasurement2_0 {
            scale: st.lookup_meas_scale_opt(n),
            shortname: st.lookup_meas_shortname_opt(n),
            wavelength: st.lookup_meas_wavelength(n),
        })
    }

    fn req_suffixes_inner(&self) -> Vec<(&'static str, String)> {
        vec![]
    }

    fn opt_suffixes_inner(&self) -> Vec<(&'static str, Option<String>)> {
        [
            (SCALE_SFX, self.scale.as_opt_string()),
            (SHORTNAME_SFX, self.shortname.as_opt_string()),
            (WAVELEN_SFX, self.wavelength.as_opt_string()),
        ]
        .into_iter()
        .collect()
    }
}

impl VersionedMeasurement for InnerMeasurement3_0 {
    fn maybe_name(p: &Measurement<Self>) -> Option<&Shortname> {
        p.specific.shortname.0.as_ref()
    }

    fn has_linear_scale(&self) -> bool {
        self.scale == Scale::Linear
    }

    fn has_gain(&self) -> bool {
        self.gain.0.is_some()
    }

    fn shortname(p: &Measurement<Self>, n: usize) -> Shortname {
        p.specific
            .shortname
            .0
            .as_ref()
            .cloned()
            .unwrap_or(Shortname::from_index(n))
    }

    fn set_shortname(m: &mut Measurement<Self>, n: Shortname) {
        m.specific.shortname = Some(n).into()
    }

    fn lookup_specific(st: &mut KwParser, n: usize) -> Option<InnerMeasurement3_0> {
        let shortname = st.lookup_meas_shortname_opt(n);
        Some(InnerMeasurement3_0 {
            scale: st.lookup_meas_scale_req(n)?,
            gain: st.lookup_meas_gain(n),
            shortname,
            wavelength: st.lookup_meas_wavelength(n),
        })
    }

    fn req_suffixes_inner(&self) -> Vec<(&'static str, String)> {
        [(SCALE_SFX, self.scale.to_string())].into_iter().collect()
    }

    fn opt_suffixes_inner(&self) -> Vec<(&'static str, Option<String>)> {
        [
            (SHORTNAME_SFX, self.shortname.as_opt_string()),
            (WAVELEN_SFX, self.wavelength.as_opt_string()),
            (GAIN_SFX, self.gain.as_opt_string()),
        ]
        .into_iter()
        .collect()
    }
}

impl VersionedMeasurement for InnerMeasurement3_1 {
    fn maybe_name(p: &Measurement<Self>) -> Option<&Shortname> {
        Some(&p.specific.shortname)
    }

    fn has_linear_scale(&self) -> bool {
        self.scale == Scale::Linear
    }

    fn has_gain(&self) -> bool {
        self.gain.0.is_some()
    }

    fn shortname(p: &Measurement<Self>, _: usize) -> Shortname {
        p.specific.shortname.clone()
    }

    fn set_shortname(m: &mut Measurement<Self>, n: Shortname) {
        m.specific.shortname = n
    }

    fn lookup_specific(st: &mut KwParser, n: usize) -> Option<InnerMeasurement3_1> {
        let shortname = st.lookup_meas_shortname_req(n)?;
        Some(InnerMeasurement3_1 {
            scale: st.lookup_meas_scale_req(n)?,
            gain: st.lookup_meas_gain(n),
            shortname,
            wavelengths: st.lookup_meas_wavelengths(n),
            calibration: st.lookup_meas_cal3_1(n),
            display: st.lookup_meas_display(n),
        })
    }

    fn req_suffixes_inner(&self) -> Vec<(&'static str, String)> {
        [
            (SCALE_SFX, self.scale.to_string()),
            (SHORTNAME_SFX, self.shortname.to_string()),
        ]
        .into_iter()
        .collect()
    }

    fn opt_suffixes_inner(&self) -> Vec<(&'static str, Option<String>)> {
        [
            (WAVELEN_SFX, self.wavelengths.as_opt_string()),
            (GAIN_SFX, self.gain.as_opt_string()),
            (CALIBRATION_SFX, self.calibration.as_opt_string()),
            (DISPLAY_SFX, self.display.as_opt_string()),
        ]
        .into_iter()
        .collect()
    }
}

impl VersionedMeasurement for InnerMeasurement3_2 {
    fn maybe_name(p: &Measurement<Self>) -> Option<&Shortname> {
        Some(&p.specific.shortname)
    }

    fn has_linear_scale(&self) -> bool {
        self.scale == Scale::Linear
    }

    fn has_gain(&self) -> bool {
        self.gain.0.is_some()
    }

    fn shortname(p: &Measurement<Self>, _: usize) -> Shortname {
        p.specific.shortname.clone()
    }

    fn set_shortname(m: &mut Measurement<Self>, n: Shortname) {
        m.specific.shortname = n
    }

    fn lookup_specific(st: &mut KwParser, n: usize) -> Option<InnerMeasurement3_2> {
        let shortname = st.lookup_meas_shortname_req(n)?;
        Some(InnerMeasurement3_2 {
            gain: st.lookup_meas_gain(n),
            scale: st.lookup_meas_scale_req(n)?,
            shortname,
            wavelengths: st.lookup_meas_wavelengths(n),
            calibration: st.lookup_meas_cal3_2(n),
            display: st.lookup_meas_display(n),
            detector_name: st.lookup_meas_detector(n),
            tag: st.lookup_meas_tag(n),
            // TODO this should be "Time" if time channel
            measurement_type: st.lookup_meas_type(n),
            feature: st.lookup_meas_feature(n),
            analyte: st.lookup_meas_analyte(n),
            datatype: st.lookup_meas_datatype(n),
        })
    }

    fn req_suffixes_inner(&self) -> Vec<(&'static str, String)> {
        [
            (SCALE_SFX, self.scale.to_string()),
            (SHORTNAME_SFX, self.shortname.to_string()),
        ]
        .into_iter()
        .collect()
    }

    fn opt_suffixes_inner(&self) -> Vec<(&'static str, Option<String>)> {
        [
            (WAVELEN_SFX, self.wavelengths.as_opt_string()),
            (GAIN_SFX, self.gain.as_opt_string()),
            (CALIBRATION_SFX, self.calibration.as_opt_string()),
            (DISPLAY_SFX, self.display.as_opt_string()),
            (DET_NAME_SFX, self.detector_name.as_opt_string()),
            (TAG_SFX, self.tag.as_opt_string()),
            (MEAS_TYPE_SFX, self.measurement_type.as_opt_string()),
            (FEATURE_SFX, self.feature.as_opt_string()),
            (ANALYTE_SFX, self.analyte.as_opt_string()),
            (DATATYPE_SFX, self.datatype.as_opt_string()),
        ]
        .into_iter()
        .collect()
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

impl str::FromStr for Originality {
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

impl FromStr for UnstainedCenters {
    type Err = NamedFixedSeqError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut xs = s.split(",");
        if let Some(n) = xs.next().and_then(|s| s.parse().ok()) {
            // This should be safe since we are splitting by commas
            let measurements: Vec<_> = xs.by_ref().take(n).map(Shortname::new_unchecked).collect();
            let values: Vec<_> = xs.by_ref().take(n).collect();
            let remainder = xs.by_ref().count();
            let total = values.len() + measurements.len() + remainder;
            let expected = 2 * n;
            if total != expected {
                let fvalues: Vec<_> = values
                    .into_iter()
                    .filter_map(|s| s.parse::<f32>().ok())
                    .collect();
                if fvalues.len() != n {
                    Err(NamedFixedSeqError::Seq(FixedSeqError::BadFloat))
                } else if measurements.iter().unique().count() != n {
                    Err(NamedFixedSeqError::NonUnique)
                } else {
                    Ok(UnstainedCenters(
                        measurements.into_iter().zip(fvalues).collect(),
                    ))
                }
            } else {
                Err(NamedFixedSeqError::Seq(FixedSeqError::WrongLength {
                    total,
                    expected,
                }))
            }
        } else {
            Err(NamedFixedSeqError::Seq(FixedSeqError::BadLength))
        }
    }
}

impl fmt::Display for UnstainedCenters {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let n = self.0.len();
        let (ms, vs): (Vec<&Shortname>, Vec<f32>) = self.0.iter().unzip();
        write!(f, "{n},{},{}", ms.iter().join(","), vs.iter().join(","))
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
        if let Some(page) = xs.next().and_then(|s| s.parse().ok()) {
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

macro_rules! match_anycoretext {
    ($self:expr, $bind:ident, $stuff:block) => {
        match_many_to_one!(
            $self,
            AnyCoreTEXT,
            [FCS2_0, FCS3_0, FCS3_1, FCS3_2],
            $bind,
            $stuff
        )
    };
}

macro_rules! any_core_get_set_copied {
    ($get:ident, $set:ident, $t:ty) => {
        pub fn $get(&self) -> Option<$t> {
            match_anycoretext!(self, x, { x.$get() })
        }

        pub fn $set(&mut self, s: Option<$t>) {
            match_anycoretext!(self, x, { x.$set(s) })
        }
    };
}

macro_rules! any_core_get_set_str {
    ($get:ident, $set:ident) => {
        pub fn $get(&self) -> Option<&str> {
            match_anycoretext!(self, x, { x.$get() })
        }

        pub fn $set(&mut self, s: Option<String>) {
            match_anycoretext!(self, x, { x.$set(s) })
        }
    };
}

impl AnyCoreTEXT {
    pub fn version(&self) -> Version {
        match self {
            AnyCoreTEXT::FCS2_0(_) => Version::FCS2_0,
            AnyCoreTEXT::FCS3_0(_) => Version::FCS3_0,
            AnyCoreTEXT::FCS3_1(_) => Version::FCS3_1,
            AnyCoreTEXT::FCS3_2(_) => Version::FCS3_2,
        }
    }

    any_core_get_set_copied!(abrt, set_abrt, Abrt);
    any_core_get_set_copied!(lost, set_lost, Lost);

    any_core_get_set_str!(cells, set_cells);
    any_core_get_set_str!(com, set_com);
    any_core_get_set_str!(exp, set_exp);
    any_core_get_set_str!(fil, set_fil);
    any_core_get_set_str!(inst, set_inst);
    any_core_get_set_str!(op, set_op);
    any_core_get_set_str!(proj, set_proj);
    any_core_get_set_str!(smno, set_smno);
    any_core_get_set_str!(src, set_src);
    any_core_get_set_str!(sys, set_sys);

    pub fn begin_date(&self) -> Option<NaiveDate> {
        match_anycoretext!(self, x, { x.begin_date() })
    }

    pub fn begin_time(&self) -> Option<NaiveTime> {
        match_anycoretext!(self, x, { x.begin_time() })
    }

    pub fn end_date(&self) -> Option<NaiveDate> {
        match_anycoretext!(self, x, { x.end_date() })
    }

    pub fn end_time(&self) -> Option<NaiveTime> {
        match_anycoretext!(self, x, { x.end_time() })
    }

    pub fn set_datetimes(
        &mut self,
        begin: DateTime<FixedOffset>,
        end: DateTime<FixedOffset>,
    ) -> bool {
        match_anycoretext!(self, x, { x.set_datetimes(begin, end) })
    }

    pub fn clear_datetimes(&mut self) {
        match_anycoretext!(self, x, { x.clear_datetimes() })
    }

    pub fn raw_keywords(&self, want_req: Option<bool>, want_meta: Option<bool>) -> RawKeywords {
        match_anycoretext!(self, x, { x.raw_keywords(want_req, want_meta) })
    }

    pub fn shortnames(&self) -> Vec<Shortname> {
        match_anycoretext!(self, x, { x.shortnames() })
    }

    pub fn into_2_0(self, def: CoreDefaultsTo2_0) -> PureSuccess<CoreTEXT2_0> {
        match self {
            AnyCoreTEXT::FCS2_0(c) => CoreTEXT2_0::convert_core(*c, def),
            AnyCoreTEXT::FCS3_0(c) => CoreTEXT3_0::convert_core(*c, def),
            AnyCoreTEXT::FCS3_1(c) => CoreTEXT3_1::convert_core(*c, def),
            AnyCoreTEXT::FCS3_2(c) => CoreTEXT3_2::convert_core(*c, def),
        }
    }

    pub fn into_3_0(self, def: CoreDefaultsTo3_0) -> PureSuccess<CoreTEXT3_0> {
        match self {
            AnyCoreTEXT::FCS2_0(c) => CoreTEXT2_0::convert_core(*c, def),
            AnyCoreTEXT::FCS3_0(c) => CoreTEXT3_0::convert_core(*c, def),
            AnyCoreTEXT::FCS3_1(c) => CoreTEXT3_1::convert_core(*c, def),
            AnyCoreTEXT::FCS3_2(c) => CoreTEXT3_2::convert_core(*c, def),
        }
    }

    pub fn into_3_1(self, def: CoreDefaultsTo3_1) -> PureSuccess<CoreTEXT3_1> {
        match self {
            AnyCoreTEXT::FCS2_0(c) => CoreTEXT2_0::convert_core(*c, def),
            AnyCoreTEXT::FCS3_0(c) => CoreTEXT3_0::convert_core(*c, def),
            AnyCoreTEXT::FCS3_1(c) => CoreTEXT3_1::convert_core(*c, def),
            AnyCoreTEXT::FCS3_2(c) => CoreTEXT3_2::convert_core(*c, def),
        }
    }

    pub fn into_3_2(self, def: CoreDefaultsTo3_2) -> PureSuccess<CoreTEXT3_2> {
        match self {
            AnyCoreTEXT::FCS2_0(c) => CoreTEXT2_0::convert_core(*c, def),
            AnyCoreTEXT::FCS3_0(c) => CoreTEXT3_0::convert_core(*c, def),
            AnyCoreTEXT::FCS3_1(c) => CoreTEXT3_1::convert_core(*c, def),
            AnyCoreTEXT::FCS3_2(c) => CoreTEXT3_2::convert_core(*c, def),
        }
    }

    pub fn text_segment(
        &self,
        tot: usize,
        data_len: usize,
        analysis_len: usize,
    ) -> Option<Vec<String>> {
        match_anycoretext!(self, x, { x.text_segment(tot, data_len, analysis_len) })
    }

    pub fn par(&self) -> usize {
        match_anycoretext!(self, x, { x.par() })
    }

    pub fn print_meas_table(&self, delim: &str) {
        match_anycoretext!(self, x, { x.print_meas_table(delim) })
    }

    pub fn print_spillover_table(&self, delim: &str) {
        let res = match self {
            AnyCoreTEXT::FCS2_0(_) => None,
            AnyCoreTEXT::FCS3_0(_) => None,
            AnyCoreTEXT::FCS3_1(x) => x
                .metadata
                .specific
                .spillover
                .0
                .as_ref()
                .map(|s| s.print_table(delim)),
            AnyCoreTEXT::FCS3_2(x) => x
                .metadata
                .specific
                .spillover
                .0
                .as_ref()
                .map(|s| s.print_table(delim)),
        };
        if res.is_none() {
            println!("None")
        }
    }

    fn set_df_column_names(&self, df: &mut DataFrame) -> PolarsResult<()> {
        match_anycoretext!(self, x, { x.set_df_column_names(df) })
    }

    fn as_writer_data_layout(&self) -> Result<WriterDataLayout, Vec<String>> {
        match_anycoretext!(self, x, { x.as_writer_data_layout() })
    }

    fn as_data_reader(
        &self,
        kws: &mut RawKeywords,
        conf: &DataReadConfig,
        data_seg: &Segment,
    ) -> PureMaybe<DataReader> {
        match_anycoretext!(self, x, { x.as_data_reader(kws, conf, data_seg) })
    }
}

impl Serialize for AnyCoreTEXT {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("AnyStdTEXT", 2)?;
        match self {
            AnyCoreTEXT::FCS2_0(x) => {
                state.serialize_field("version", &Version::FCS2_0)?;
                state.serialize_field("data", &x)?;
            }
            AnyCoreTEXT::FCS3_0(x) => {
                state.serialize_field("version", &Version::FCS3_0)?;
                state.serialize_field("data", &x)?;
            }
            AnyCoreTEXT::FCS3_1(x) => {
                state.serialize_field("version", &Version::FCS3_1)?;
                state.serialize_field("data", &x)?;
            }
            AnyCoreTEXT::FCS3_2(x) => {
                state.serialize_field("version", &Version::FCS3_2)?;
                state.serialize_field("data", &x)?;
            }
        }
        state.end()
    }
}

fn build_mixed_reader(cs: Vec<ColumnType>, total_events: usize) -> MixedParser {
    let columns = cs
        .into_iter()
        .map(|p| match p {
            ColumnType::Ascii { bytes } => MixedColumnType::Ascii(AsciiColumnReader {
                width: bytes,
                column: vec![],
            }),
            ColumnType::Float(order) => {
                MixedColumnType::Single(Float32Type::make_column_reader(order, total_events))
            }
            ColumnType::Double(order) => {
                MixedColumnType::Double(Float64Type::make_column_reader(order, total_events))
            }
            ColumnType::Integer(col) => {
                MixedColumnType::Uint(AnyUintColumnReader::from_column(col, total_events))
            }
        })
        .collect();
    MixedParser {
        columns,
        nrows: total_events,
    }
}

fn build_data_reader(layout: ReaderDataLayout, data_seg: &Segment) -> DataReader {
    let column_parser = match layout {
        DataLayout::AlphaNum { nrows, columns } => {
            ColumnReader::Mixed(build_mixed_reader(columns, nrows))
        }
        DataLayout::AsciiDelimited { nrows, ncols } => {
            let nbytes = data_seg.nbytes() as usize;
            ColumnReader::DelimitedAscii(DelimAsciiReader {
                ncols,
                nrows,
                nbytes,
            })
        }
    };
    DataReader {
        column_reader: column_parser,
        begin: u64::from(data_seg.begin()),
    }
}

macro_rules! get_set_copied {
    ($get:ident, $set:ident, $t:ty) => {
        pub fn $get(&self) -> Option<$t> {
            self.metadata.$get.0.as_ref().copied()
        }

        pub fn $set(&mut self, x: Option<$t>) {
            self.metadata.$get = x.into();
        }
    };
}

macro_rules! get_set_str {
    ($get:ident, $set:ident) => {
        pub fn $get(&self) -> Option<&str> {
            self.metadata.$get.0.as_ref().map(|x| x.0.as_str())
        }

        pub fn $set(&mut self, x: Option<String>) {
            self.metadata.$get = x.map(|y| y.into()).into();
        }
    };
}

impl<M> CoreTEXT<M, M::P>
where
    M: VersionedMetadata,
    M: VersionedParserMetadata,
{
    /// Return HEADER+TEXT as a list of strings
    ///
    /// The first member will be a string exactly 58 bytes long which will be
    /// the HEADER. The next members will be alternating keys and values of the
    /// primary TEXT segment and supplemental if included. Each member should be
    /// separated by a delimiter when writing to a file, and a trailing
    /// delimiter should be added.
    ///
    /// Keywords will be sorted with offsets first, non-measurement keywords
    /// second in alphabetical order, then measurement keywords in alphabetical
    /// order.
    ///
    /// Return None if primary TEXT does not fit into first 99,999,999 bytes.
    fn text_segment(
        &self,
        tot: usize,
        data_len: usize,
        analysis_len: usize,
    ) -> Option<Vec<String>> {
        self.header_and_raw_keywords(tot, data_len, analysis_len)
            .map(|(header, kws)| {
                let version = M::P::fcs_version();
                let flat: Vec<_> = kws.into_iter().flat_map(|(k, v)| [k, v]).collect();
                [format!("{version}{header}")]
                    .into_iter()
                    .chain(flat)
                    .collect()
            })
    }

    /// Return all keywords as an ordered list of pairs
    ///
    /// Thiw will only include keywords that can be directly derived from
    /// [CoreTEXT]. This means it will not include $TOT, since this depends on
    /// the DATA segment.
    pub fn raw_keywords(&self, want_req: Option<bool>, want_meta: Option<bool>) -> RawKeywords {
        let (req_meas, req_meta, _) = self.some_keywords(M::P::req_keywords, M::all_req_keywords);
        let (opt_meas, opt_meta, _) =
            self.some_keywords(M::P::opt_keywords, |m, _| M::all_opt_keywords(m));

        let triop = |op| match op {
            None => (true, true),
            Some(true) => (true, false),
            Some(false) => (false, true),
        };

        let keep = |xs, t1, t2| {
            if t1 && t2 {
                xs
            } else {
                vec![]
            }
        };

        let (keep_req, keep_opt) = triop(want_req);
        let (keep_meta, keep_meas) = triop(want_meta);

        keep(req_meta, keep_req, keep_meta)
            .into_iter()
            .chain(keep(req_meas, keep_req, keep_meas))
            .chain(keep(opt_meta, keep_opt, keep_meta))
            .chain(keep(opt_meas, keep_opt, keep_meas))
            .collect()
    }

    pub fn begin_date(&self) -> Option<NaiveDate> {
        M::begin_date(&self.metadata.specific)
    }

    pub fn end_date(&self) -> Option<NaiveDate> {
        M::end_date(&self.metadata.specific)
    }

    pub fn begin_time(&self) -> Option<NaiveTime> {
        M::begin_time(&self.metadata.specific)
    }

    pub fn end_time(&self) -> Option<NaiveTime> {
        M::end_time(&self.metadata.specific)
    }

    pub fn set_datetimes(
        &mut self,
        begin: DateTime<FixedOffset>,
        end: DateTime<FixedOffset>,
    ) -> bool {
        M::set_datetimes(&mut self.metadata.specific, begin, end)
    }

    pub fn clear_datetimes(&mut self) {
        M::clear_datetimes(&mut self.metadata.specific)
    }

    get_set_copied!(abrt, set_abrt, Abrt);
    get_set_copied!(lost, set_lost, Lost);

    get_set_str!(cells, set_cells);
    get_set_str!(com, set_com);
    get_set_str!(exp, set_exp);
    get_set_str!(fil, set_fil);
    get_set_str!(inst, set_inst);
    get_set_str!(op, set_op);
    get_set_str!(proj, set_proj);
    get_set_str!(smno, set_smno);
    get_set_str!(src, set_src);
    get_set_str!(sys, set_sys);

    // fn btim(&self) -> NaiveDate {}

    fn header_and_raw_keywords(
        &self,
        tot: usize,
        data_len: usize,
        analysis_len: usize,
    ) -> Option<(String, RawKeywords)> {
        let version = M::P::fcs_version();
        let tot_pair = (TOT.to_string(), tot.to_string());

        let (req_meas, req_meta, req_text_len) =
            self.some_keywords(M::P::req_keywords, M::all_req_keywords);
        let (opt_meas, opt_meta, opt_text_len) =
            self.some_keywords(M::P::opt_keywords, |m, _| M::all_opt_keywords(m));

        let offset_result = if version == Version::FCS2_0 {
            make_data_offset_keywords_2_0(req_text_len + opt_text_len, data_len, analysis_len)
        } else {
            make_data_offset_keywords_3_0(req_text_len, opt_text_len, data_len, analysis_len)
        }?;

        let mut meta: Vec<_> = req_meta
            .into_iter()
            .chain(opt_meta)
            .chain([tot_pair])
            .collect();
        let mut meas: Vec<_> = req_meas.into_iter().chain(opt_meas).collect();
        meta.sort_by(|a, b| a.0.cmp(&b.0));
        meas.sort_by(|a, b| a.0.cmp(&b.0));

        let req_opt_kws: Vec<_> = meta.into_iter().chain(meas).collect();
        Some((
            offset_result.header,
            offset_result
                .offsets
                .into_iter()
                .chain(req_opt_kws)
                .collect(),
        ))
    }

    /// Return a list of measurement names as stored in $PnN
    ///
    /// For cases where $PnN is optional and its value is not given, this will
    /// return "Mn" where "n" is the parameter index starting at 0.
    // TODO start at 1?
    pub fn shortnames(&self) -> Vec<Shortname> {
        self.measurements
            .iter()
            .enumerate()
            .map(|(i, p)| M::P::shortname(p, i))
            .collect()
    }

    fn set_df_column_names(&self, df: &mut DataFrame) -> PolarsResult<()> {
        let ns: Vec<PlSmallStr> = self
            .shortnames()
            .into_iter()
            .map(|s| s.as_ref().into())
            .collect();
        df.set_column_names(ns)
    }

    /// Set all $PnN keywords to list of names.
    pub fn set_shortnames(&mut self, ns: Vec<Shortname>) -> bool {
        if self.measurements.len() != ns.len() {
            false
        } else {
            for (m, n) in self.measurements.iter_mut().zip(ns) {
                M::P::set_shortname(m, n)
            }
            true
        }
    }

    /// Return a list of measurement names as stored in $PnS
    ///
    /// If not given, will be replaced by "Mn" where "n" is the measurement
    /// index starting at 1.
    pub fn longnames(&self) -> Vec<Longname> {
        self.measurements
            .iter()
            .enumerate()
            .map(|(i, m)| M::P::longname(m, i))
            .collect()
    }

    /// Set all $PnS keywords to list of names.
    ///
    /// Will return false if length of supplied list does not match length
    /// of measurements; true otherwise. Since $PnS is an optional keyword for
    /// all versions, any name in the list may be None which will blank the
    /// keyword.
    pub fn set_longnames(&mut self, ns: Vec<Option<String>>) -> bool {
        if self.measurements.len() != ns.len() {
            false
        } else {
            for (m, n) in self.measurements.iter_mut().zip(ns) {
                M::P::set_longname(m, n)
            }
            true
        }
    }

    pub fn par(&self) -> usize {
        self.measurements.len()
    }

    fn some_keywords<F, G>(
        &self,
        f: F,
        g: G,
    ) -> (Vec<(String, String)>, Vec<(String, String)>, usize)
    where
        F: Fn(&Measurement<M::P>, &str) -> Vec<(String, String)>,
        G: Fn(&Metadata<M>, usize) -> Vec<(String, String)>,
    {
        let meas: Vec<_> = self
            .measurements
            .iter()
            .enumerate()
            .flat_map(|(i, m)| f(m, &(i + 1).to_string()))
            .collect();
        let meta: Vec<_> = g(&self.metadata, self.par()).into_iter().collect();
        let l = meas.len() + meta.len();
        (meas, meta, l)
    }

    fn meas_table(&self, delim: &str) -> Vec<String> {
        let ms = &self.measurements;
        if ms.is_empty() {
            return vec![];
        }
        let header = ms[0].table_header().join(delim);
        let rows = self.measurements.iter().enumerate().map(|(i, m)| {
            m.table_row(i)
                .into_iter()
                .map(|v| v.unwrap_or(String::from("NA")))
                .join(delim)
        });
        vec![header].into_iter().chain(rows).collect()
    }

    fn print_meas_table(&self, delim: &str) {
        for e in self.meas_table(delim) {
            println!("{}", e);
        }
    }

    fn as_writer_data_layout(&self) -> Result<WriterDataLayout, Vec<String>> {
        M::as_writer_data_layout(&self.metadata, &self.measurements)
    }

    fn as_data_layout_from_raw(
        &self,
        kws: &mut RawKeywords,
        conf: &DataReadConfig,
        data_seg: &Segment,
    ) -> PureMaybe<ReaderDataLayout> {
        let succ = KwParser::run(kws, (&conf.standard).into(), |st| {
            let maybe_md = <M as VersionedParserMetadata>::as_minimal(&self.metadata, st);
            let measurements: Vec<_> = self
                .measurements
                .iter()
                .map(<M::P as VersionedParserMeasurement>::as_minimal)
                .collect();
            maybe_md.map(|metadata| (measurements, metadata))
        });
        let data_nbytes = data_seg.nbytes() as usize;
        succ.and_then_opt(|(measurements, metadata)| {
            M::as_reader_data_layout_bare(&metadata, &measurements, data_nbytes, &conf)
        })
    }

    // TODO this doesn't need to be here
    fn as_data_reader(
        &self,
        kws: &mut RawKeywords,
        conf: &DataReadConfig,
        data_seg: &Segment,
    ) -> PureMaybe<DataReader> {
        self.as_data_layout_from_raw(kws, conf, data_seg)
            .map(|maybe_layout| maybe_layout.map(|layout| build_data_reader(layout, data_seg)))
    }

    fn from_raw(kws: &mut RawKeywords, conf: &StdTextReadConfig) -> PureResult<Self> {
        // Lookup $PAR first; everything depends on this since we need to know
        // the number of measurements to find which are used in turn for
        // validating lots of keywords in metadata. If we fail we need to bail.
        //
        // TODO this isn't entirely true; there are some things that can be
        // tested without $PAR, so if we really wanted to be aggressive we could
        // pass $PAR as an Option and only test if not None when we need it and
        // possibly bail. Might be worth it.
        let par_fail = "could not find $PAR".to_string();
        let c: KwParserConfig = conf.into();
        let par_succ = KwParser::try_run(kws, c.clone(), par_fail, |st| st.lookup_par())?;
        // Lookup measurements+metadata based on $PAR, which also might fail in
        // a zillion ways. If this fails we need to bail since we cannot create
        // a struct with missing fields.
        let md_fail = "could not standardize TEXT".to_string();
        let md_succ = par_succ.try_map(|par| {
            KwParser::try_run(kws, c, md_fail, |st| {
                let ms = M::P::lookup_measurements(st, par);
                let md = ms.as_ref().and_then(|xs| M::lookup_metadata(st, xs));
                if let (Some(measurements), Some(metadata)) = (ms, md) {
                    Some((measurements, metadata))
                } else {
                    None
                }
            })
        })?;
        // hooray, we win and can now make the core struct
        Ok(md_succ.map(|(measurements, metadata)| CoreTEXT {
            metadata,
            measurements,
        }))
    }

    fn any_from_raw(kws: &mut RawKeywords, conf: &StdTextReadConfig) -> PureResult<AnyCoreTEXT> {
        Self::from_raw(kws, conf).map(|succ| {
            succ.and_then(|c| c.validate(&conf.time).map(|_| c))
                .map(M::into_any)
        })
    }

    fn validate(&self, conf: &TimeConfig) -> PureSuccess<()> {
        let mut deferred = PureErrorBuf::default();

        // Ensure $PnN are unique; if this fails we can't make a dataframe, so
        // failure will always be an error and not a warning.
        let names: Vec<_> = self
            .measurements
            .iter()
            .filter_map(|m| M::P::maybe_name(m))
            .collect();
        let n_names = names.len();
        let unique_names: HashSet<_> = names.into_iter().collect();
        if unique_names.len() < n_names {
            deferred.push_error("$PnN are not all unique".into());
        }

        // Ensure $TR refers to a valid measurement
        if let Some(tr) = &self.metadata.tr.0 {
            let p = &tr.measurement;
            if !unique_names.contains(p) {
                let msg = format!("$TR refers to non-existent measurement '{p}'",);
                // TODO toggle this
                deferred.push_error(msg);
            }
        }

        // Ensure $UNSTAINEDCENTERS refers to valid measurements (if applicable)
        if let Some(msg) = self.metadata.specific.check_unstainedcenters(&unique_names) {
            // TODO toggle this
            deferred.push_error(msg);
        }

        // Ensure $SPILLOVER refers to valid measurements (if applicable)
        if let Some(msg) = self.metadata.specific.check_spillover(&unique_names) {
            // TODO toggle this
            deferred.push_error(msg);
        }

        // Ensure time measurement is valid
        if let Some(time_name) = &conf.shortname {
            // Find the time measurement, check lots of stuff if it exists
            if let Some(time_meas) = self
                .measurements
                .iter()
                .find(|m| M::P::maybe_name(m) == Some(time_name))
            {
                // check if $TIMESTEP exists
                if !self.metadata.specific.has_timestep() {
                    let msg = "$TIMESTEP must be present if time measurement present".into();
                    deferred.push_msg_leveled(msg, conf.ensure_timestep)
                }
                // check that $PnE is linear
                if !time_meas.specific.has_linear_scale() {
                    deferred.push_msg_leveled(
                        "Time measurement must have linear $PnE".into(),
                        conf.ensure_linear,
                    );
                }
                // check that $PnG doesn't exist
                if time_meas.specific.has_gain() {
                    deferred.push_msg_leveled(
                        "Time measurement should not have $PnG".into(),
                        conf.ensure_nogain,
                    );
                }
            } else {
                let msg = format!("Measurement '{time_name}' not found for time");
                deferred.push_msg_leveled(msg, conf.ensure);
            }
        }

        // Ensure $BTIM/$ETIM/$DATE are valid
        if !self.metadata.specific.timestamps_valid() {
            let msg = "$ETIM is before $BTIM and $DATE is given".into();
            // TODO toggle this
            deferred.push_error(msg);
        }

        // Ensure $BEGIN/ENDDATETIME are valid (if applicable)
        if !self.metadata.specific.datetimes_valid() {
            let msg = "$BEGINDATETIME is after $ENDDATETIME".into();
            // TODO toggle this
            deferred.push_error(msg);
        }

        PureSuccess { data: (), deferred }
    }
}

enum ValidType {
    U08,
    U16,
    U32,
    U64,
    F32,
    F64,
}

impl ValidType {
    fn from(dt: &DataType) -> Option<Self> {
        match dt {
            DataType::UInt8 => Some(ValidType::U08),
            DataType::UInt16 => Some(ValidType::U16),
            DataType::UInt32 => Some(ValidType::U32),
            DataType::UInt64 => Some(ValidType::U64),
            DataType::Float32 => Some(ValidType::F32),
            DataType::Float64 => Some(ValidType::F64),
            _ => None,
        }
    }
}

/// Convert a series into a writable vector
///
/// Data that is to be written in a different type will be converted. Depending
/// on the start and end types, data loss may occur, in which case the user will
/// be warned.
///
/// For some cases like float->ASCII (bad idea), it is not clear how much space
/// will be needed to represent every possible float in the file, so user will
/// be warned always.
///
/// If the start type will fit into the end type, all is well and nothing bad
/// will happen to user's precious data.
fn series_coerce(
    c: &Column,
    w: ColumnType,
    conf: &WriteConfig,
) -> Option<PureSuccess<ColumnWriter>> {
    let dt = ValidType::from(c.dtype())?;
    let mut deferred = PureErrorBuf::default();

    let ascii_uint_warn = |d: &mut PureErrorBuf, bits, bytes| {
        let msg = format!(
            "writing ASCII as {bits}-bit uint in {bytes} \
                             may result in truncation"
        );
        d.push_msg_leveled(msg, conf.disallow_lossy_conversions);
    };
    let num_warn = |d: &mut PureErrorBuf, from, to| {
        let msg = format!("converting {from} to {to} may truncate data");
        d.push_msg_leveled(msg, conf.disallow_lossy_conversions);
    };

    // TODO this will make a copy of the data within a new vector, which is
    // simply going to be shoved onto disk a few nanoseconds later. Would make
    // more sense to return a lazy iterator which would skip this intermediate.
    let res = match w {
        // For Uint* -> ASCII, warn user if there are not enough bytes to
        // hold the max range of the type being formatted. ASCII shouldn't
        // store floats at all, so warn user if input data is float or
        // double.
        ColumnType::Ascii { bytes } => match dt {
            ValidType::U08 => {
                if bytes < 3 {
                    ascii_uint_warn(&mut deferred, 8, 3);
                }
                AsciiU8(AsciiColumnWriter {
                    column: c.u8().unwrap().into_no_null_iter().collect(),
                    bytes,
                })
            }
            ValidType::U16 => {
                if bytes < 5 {
                    ascii_uint_warn(&mut deferred, 16, 5);
                }
                AsciiU16(AsciiColumnWriter {
                    column: c.u16().unwrap().into_no_null_iter().collect(),
                    bytes,
                })
            }
            ValidType::U32 => {
                if bytes < 10 {
                    ascii_uint_warn(&mut deferred, 32, 10);
                }
                AsciiU32(AsciiColumnWriter {
                    column: c.u32().unwrap().into_no_null_iter().collect(),
                    bytes,
                })
            }
            ValidType::U64 => {
                if bytes < 20 {
                    ascii_uint_warn(&mut deferred, 64, 20);
                }
                AsciiU64(AsciiColumnWriter {
                    column: c.u64().unwrap().into_no_null_iter().collect(),
                    bytes,
                })
            }
            ValidType::F32 => {
                num_warn(&mut deferred, "float", "uint64");
                AsciiU64(AsciiColumnWriter {
                    column: series_cast!(c, f32, u64),
                    bytes,
                })
            }
            ValidType::F64 => {
                num_warn(&mut deferred, "double", "uint64");
                AsciiU64(AsciiColumnWriter {
                    column: series_cast!(c, f32, u64),
                    bytes,
                })
            }
        },

        // Uint* -> Uint* is quite easy, just compare sizes and warn if the
        // target type is too small. Float/double -> Uint always could
        // potentially truncate a fractional value. Also check to see if
        // bitmask is exceeded, and if so truncate and warn user.
        ColumnType::Integer(ut) => {
            match dt {
                ValidType::F32 => num_warn(&mut deferred, "float", "uint"),
                ValidType::F64 => num_warn(&mut deferred, "float", "uint"),
                _ => {
                    let from_size = ut.nbytes();
                    let to_size = ut.native_nbytes();
                    if to_size < from_size {
                        let msg = format!(
                            "converted uint from {from_size} to \
                             {to_size} bytes may truncate data"
                        );
                        deferred.push_warning(msg);
                    }
                }
            }
            match dt {
                ValidType::U08 => convert_to_uint!(ut, c, u8, deferred),
                ValidType::U16 => convert_to_uint!(ut, c, u16, deferred),
                ValidType::U32 => convert_to_uint!(ut, c, u32, deferred),
                ValidType::U64 => convert_to_uint!(ut, c, u64, deferred),
                ValidType::F32 => convert_to_uint!(ut, c, f32, deferred),
                ValidType::F64 => convert_to_uint!(ut, c, f64, deferred),
            }
        }

        // Floats can hold small uints and themselves, anything else might
        // truncate.
        ColumnType::Float(size) => {
            match dt {
                ValidType::U32 => num_warn(&mut deferred, "float", "uint32"),
                ValidType::U64 => num_warn(&mut deferred, "float", "uint64"),
                ValidType::F64 => num_warn(&mut deferred, "float", "double"),
                _ => (),
            }
            match dt {
                ValidType::U08 => convert_to_f32!(size, c, u8),
                ValidType::U16 => convert_to_f32!(size, c, u16),
                ValidType::U32 => convert_to_f32!(size, c, u32),
                ValidType::U64 => convert_to_f32!(size, c, u64),
                ValidType::F32 => convert_to_f32!(size, c, f32),
                ValidType::F64 => convert_to_f32!(size, c, f64),
            }
        }

        // Doubles can hold all but uint64
        ColumnType::Double(size) => {
            if let ValidType::U64 = dt {
                num_warn(&mut deferred, "double", "uint64")
            }
            match dt {
                ValidType::U08 => convert_to_f64!(size, c, u8),
                ValidType::U16 => convert_to_f64!(size, c, u16),
                ValidType::U32 => convert_to_f64!(size, c, u32),
                ValidType::U64 => convert_to_f64!(size, c, u64),
                ValidType::F32 => convert_to_f64!(size, c, f32),
                ValidType::F64 => convert_to_f64!(size, c, f64),
            }
        }
    };
    Some(PureSuccess {
        data: res,
        deferred,
    })
}

/// Convert Series into a u64 vector.
///
/// Used when writing delimited ASCII. This is faster and more convenient
/// than the general coercion function.
fn series_coerce64(s: &Column, conf: &WriteConfig) -> Option<PureSuccess<Vec<u64>>> {
    let dt = ValidType::from(&s.dtype())?;
    let mut deferred = PureErrorBuf::default();

    let num_warn = |d: &mut PureErrorBuf, from, to| {
        let msg = format!("converting {from} to {to} may truncate data");
        d.push_msg_leveled(msg, conf.disallow_lossy_conversions);
    };

    let res = match dt {
        ValidType::U08 => series_cast!(s, u8, u64),
        ValidType::U16 => series_cast!(s, u16, u64),
        ValidType::U32 => series_cast!(s, u32, u64),
        ValidType::U64 => series_cast!(s, u64, u64),
        ValidType::F32 => {
            num_warn(&mut deferred, "float", "uint64");
            series_cast!(s, f32, u64)
        }
        ValidType::F64 => {
            num_warn(&mut deferred, "double", "uint64");
            series_cast!(s, f64, u64)
        }
    };
    Some(PureSuccess {
        data: res,
        deferred,
    })
}

macro_rules! impl_num_props {
    ($size:expr, $zero:expr, $t:ty, $p:ident) => {
        // impl From<Vec<$t>> for Series {
        //     fn from(value: Vec<$t>) -> Self {
        //         Series::$p(value)
        //     }
        // }

        impl NumProps<$size> for $t {
            fn zero() -> Self {
                $zero
            }

            fn to_big(self) -> [u8; $size] {
                <$t>::to_be_bytes(self)
            }

            fn to_little(self) -> [u8; $size] {
                <$t>::to_le_bytes(self)
            }

            fn from_big(buf: [u8; $size]) -> Self {
                <$t>::from_be_bytes(buf)
            }

            fn from_little(buf: [u8; $size]) -> Self {
                <$t>::from_le_bytes(buf)
            }
        }
    };
}

impl_num_props!(1, 0, u8, U08);
impl_num_props!(2, 0, u16, U16);
impl_num_props!(4, 0, u32, U32);
impl_num_props!(8, 0, u64, U64);
impl_num_props!(4, 0.0, f32, F32);
impl_num_props!(8, 0.0, f64, F64);

macro_rules! impl_int_math {
    ($t:ty) => {
        impl IntMath for $t {
            fn next_power_2(x: Self) -> Self {
                Self::checked_next_power_of_two(x).unwrap_or(Self::MAX)
            }

            fn maxval() -> Self {
                Self::MAX
            }
        }
    };
}

impl_int_math!(u8);
impl_int_math!(u16);
impl_int_math!(u32);
impl_int_math!(u64);

// TODO where to put this?
fn byteord_to_sized<const LEN: usize>(byteord: &ByteOrd) -> Result<SizedByteOrd<LEN>, String> {
    match byteord {
        ByteOrd::Endian(e) => Ok(SizedByteOrd::Endian(*e)),
        ByteOrd::Mixed(v) => v[..]
            .try_into()
            .map(|order: [u8; LEN]| SizedByteOrd::Order(order))
            .or(Err(format!(
                "$BYTEORD is mixed but length is {} and not {LEN}",
                v.len()
            ))),
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

impl FloatFromBytes<4> for Float32Type {}
impl FloatFromBytes<8> for Float64Type {}

impl IntFromBytes<1, 1> for UInt8Type {}
impl IntFromBytes<2, 2> for UInt16Type {}
impl IntFromBytes<4, 3> for UInt32Type {}
impl IntFromBytes<4, 4> for UInt32Type {}
impl IntFromBytes<8, 5> for UInt64Type {}
impl IntFromBytes<8, 6> for UInt64Type {}
impl IntFromBytes<8, 7> for UInt64Type {}
impl IntFromBytes<8, 8> for UInt64Type {}

impl MixedColumnType {
    fn into_pl_series(self, name: PlSmallStr) -> Series {
        match self {
            MixedColumnType::Ascii(x) => UInt64Chunked::from_vec(name, x.column).into_series(),
            MixedColumnType::Single(x) => Float32Chunked::from_vec(name, x.column).into_series(),
            MixedColumnType::Double(x) => Float64Chunked::from_vec(name, x.column).into_series(),
            MixedColumnType::Uint(x) => x.into_pl_series(name),
        }
    }
}

impl AnyUintColumnReader {
    fn into_pl_series(self, name: PlSmallStr) -> Series {
        match self {
            AnyUintColumnReader::Uint8(x) => UInt8Chunked::from_vec(name, x.column).into_series(),
            AnyUintColumnReader::Uint16(x) => UInt16Chunked::from_vec(name, x.column).into_series(),
            AnyUintColumnReader::Uint24(x) => UInt32Chunked::from_vec(name, x.column).into_series(),
            AnyUintColumnReader::Uint32(x) => UInt32Chunked::from_vec(name, x.column).into_series(),
            AnyUintColumnReader::Uint40(x) => UInt64Chunked::from_vec(name, x.column).into_series(),
            AnyUintColumnReader::Uint48(x) => UInt64Chunked::from_vec(name, x.column).into_series(),
            AnyUintColumnReader::Uint56(x) => UInt64Chunked::from_vec(name, x.column).into_series(),
            AnyUintColumnReader::Uint64(x) => UInt64Chunked::from_vec(name, x.column).into_series(),
        }
    }
}

impl AnyUintColumnReader {
    // TODO clean this up
    fn from_column(ut: AnyUintType, total_events: usize) -> Self {
        match ut {
            AnyUintType::Uint8(layout) => AnyUintColumnReader::Uint8(UintColumnReader {
                layout,
                column: vec![0; total_events],
            }),
            AnyUintType::Uint16(layout) => AnyUintColumnReader::Uint16(UintColumnReader {
                layout,
                column: vec![0; total_events],
            }),
            AnyUintType::Uint24(layout) => AnyUintColumnReader::Uint24(UintColumnReader {
                layout,
                column: vec![0; total_events],
            }),
            AnyUintType::Uint32(layout) => AnyUintColumnReader::Uint32(UintColumnReader {
                layout,
                column: vec![0; total_events],
            }),
            AnyUintType::Uint40(layout) => AnyUintColumnReader::Uint40(UintColumnReader {
                layout,
                column: vec![0; total_events],
            }),
            AnyUintType::Uint48(layout) => AnyUintColumnReader::Uint48(UintColumnReader {
                layout,
                column: vec![0; total_events],
            }),
            AnyUintType::Uint56(layout) => AnyUintColumnReader::Uint56(UintColumnReader {
                layout,
                column: vec![0; total_events],
            }),
            AnyUintType::Uint64(layout) => AnyUintColumnReader::Uint64(UintColumnReader {
                layout,
                column: vec![0; total_events],
            }),
        }
    }

    fn read_to_column<R: Read>(&mut self, h: &mut BufReader<R>, r: usize) -> io::Result<()> {
        match self {
            AnyUintColumnReader::Uint8(d) => UInt8Type::read_to_column(h, d, r)?,
            AnyUintColumnReader::Uint16(d) => UInt16Type::read_to_column(h, d, r)?,
            AnyUintColumnReader::Uint24(d) => UInt32Type::read_to_column(h, d, r)?,
            AnyUintColumnReader::Uint32(d) => UInt32Type::read_to_column(h, d, r)?,
            AnyUintColumnReader::Uint40(d) => UInt64Type::read_to_column(h, d, r)?,
            AnyUintColumnReader::Uint48(d) => UInt64Type::read_to_column(h, d, r)?,
            AnyUintColumnReader::Uint56(d) => UInt64Type::read_to_column(h, d, r)?,
            AnyUintColumnReader::Uint64(d) => UInt64Type::read_to_column(h, d, r)?,
        }
        Ok(())
    }
}

fn into_writable_columns(
    df: DataFrame,
    cs: Vec<ColumnType>,
    conf: &WriteConfig,
) -> Option<PureSuccess<Vec<ColumnWriter>>> {
    let cols = df.get_columns();
    let (writable_columns, msgs): (Vec<_>, Vec<_>) = cs
        .into_iter()
        // TODO do this without cloning?
        .zip(cols)
        .flat_map(|(w, c)| series_coerce(c, w, conf).map(|succ| (succ.data, succ.deferred)))
        .unzip();
    if df.width() != writable_columns.len() {
        return None;
    }
    Some(PureSuccess {
        data: writable_columns,
        deferred: PureErrorBuf::mconcat(msgs),
    })
}

fn into_writable_matrix64(df: DataFrame, conf: &WriteConfig) -> Option<PureSuccess<Vec<Vec<u64>>>> {
    let (columns, msgs): (Vec<_>, Vec<_>) = df
        .get_columns()
        .iter()
        .flat_map(|c| {
            if let Some(res) = series_coerce64(c, conf) {
                Some((res.data, res.deferred))
            } else {
                None
            }
        })
        .unzip();
    if df.width() != columns.len() {
        return None;
    }
    Some(PureSuccess {
        data: columns,
        deferred: PureErrorBuf::mconcat(msgs),
    })
}

// TODO fix delim
pub fn print_parsed_data(s: &mut StandardizedDataset, _delim: &str) -> PolarsResult<()> {
    let mut fd = std::io::stdout();
    CsvWriter::new(&mut fd)
        .include_header(true)
        .with_separator(b'\t')
        // TODO why does this need to be mutable?
        .finish(&mut s.dataset.data)
}

fn ascii_to_uint_io(buf: Vec<u8>) -> io::Result<u64> {
    String::from_utf8(buf)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
        .and_then(|s| parse_u64_io(&s))
}

fn parse_u64_io(s: &str) -> io::Result<u64> {
    s.parse::<u64>()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

fn read_data_delim_ascii<R: Read>(
    h: &mut BufReader<R>,
    p: DelimAsciiReader,
) -> io::Result<DataFrame> {
    let mut buf = Vec::new();
    let mut row = 0;
    let mut col = 0;
    let mut last_was_delim = false;
    // Delimiters are tab, newline, carriage return, space, or comma. Any
    // consecutive delimiter counts as one, and delimiters can be mixed.
    let is_delim = |byte| byte == 9 || byte == 10 || byte == 13 || byte == 32 || byte == 44;
    let mut data = if let Some(nrows) = p.nrows {
        // FCS 2.0 files have an optional $TOT field, which complicates this a
        // bit. If we know the number of rows, initialize a bunch of zero-ed
        // vectors and fill them sequentially.
        let mut data: Vec<_> = iter::repeat_with(|| vec![0; nrows]).take(p.ncols).collect();
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
                    data[col][row] = ascii_to_uint_io(buf.clone())?;
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
                last_was_delim = false;
            }
        }
        if !(col == 0 && row == nrows) {
            let msg = format!(
                "Parsing ended in column {col} and row {row}, \
                               where expected number of rows is {nrows}"
            );
            return Err(io::Error::new(io::ErrorKind::InvalidData, msg));
        }
        data
    } else {
        // If we don't know the number of rows, the only choice is to push onto
        // the column vectors one at a time. This leads to the possibility that
        // the vectors may not be the same length in the end, in which case,
        // scream loudly and bail.
        let mut data: Vec<_> = iter::repeat_with(Vec::new).take(p.ncols).collect();
        for b in h.bytes().take(p.nbytes) {
            let byte = b?;
            // Delimiters are tab, newline, carriage return, space, or
            // comma. Any consecutive delimiter counts as one, and
            // delimiters can be mixed.
            if is_delim(byte) {
                if !last_was_delim {
                    last_was_delim = true;
                    data[col].push(ascii_to_uint_io(buf.clone())?);
                    buf.clear();
                    if col == p.ncols - 1 {
                        col = 0;
                    } else {
                        col += 1;
                    }
                }
            } else {
                buf.push(byte);
                last_was_delim = false;
            }
        }
        if data.iter().map(|c| c.len()).unique().count() > 1 {
            let msg = "Not all columns are equal length";
            return Err(io::Error::new(io::ErrorKind::InvalidData, msg));
        }
        data
    };
    // The spec isn't clear if the last value should be a delim or
    // not, so flush the buffer if it has anything in it since we
    // only try to parse if we hit a delim above.
    if !buf.is_empty() {
        data[col][row] = ascii_to_uint_io(buf.clone())?;
    }
    let ss: Vec<_> = data
        .into_iter()
        .enumerate()
        .map(|(i, s)| {
            ChunkedArray::<UInt64Type>::from_vec(format!("M{i}").into(), s)
                .into_series()
                .into()
        })
        .collect();
    DataFrame::new(ss).map_err(|e| io::Error::other(e.to_string()))
}

fn read_data_ascii_fixed<R: Read>(
    h: &mut BufReader<R>,
    parser: &FixedAsciiReader,
) -> io::Result<DataFrame> {
    let ncols = parser.widths.len();
    let mut data: Vec<_> = iter::repeat_with(|| vec![0; parser.nrows])
        .take(ncols)
        .collect();
    let mut buf = String::new();
    for r in 0..parser.nrows {
        for (c, width) in parser.widths.iter().enumerate() {
            buf.clear();
            h.take(u64::from(*width)).read_to_string(&mut buf)?;
            data[c][r] = parse_u64_io(&buf)?;
        }
    }
    // TODO not DRY
    let ss: Vec<_> = data
        .into_iter()
        .enumerate()
        .map(|(i, s)| {
            ChunkedArray::<UInt64Type>::from_vec(format!("M{i}").into(), s)
                .into_series()
                .into()
        })
        .collect();
    DataFrame::new(ss).map_err(|e| io::Error::other(e.to_string()))
}

fn read_data_mixed<R: Read>(h: &mut BufReader<R>, parser: MixedParser) -> io::Result<DataFrame> {
    let mut p = parser;
    let mut strbuf = String::new();
    for r in 0..p.nrows {
        for c in p.columns.iter_mut() {
            match c {
                MixedColumnType::Single(t) => Float32Type::read_to_column(h, t, r)?,
                MixedColumnType::Double(t) => Float64Type::read_to_column(h, t, r)?,
                MixedColumnType::Uint(u) => u.read_to_column(h, r)?,
                MixedColumnType::Ascii(d) => {
                    strbuf.clear();
                    h.take(u64::from(d.width)).read_to_string(&mut strbuf)?;
                    d.column[r] = parse_u64_io(&strbuf)?;
                }
            }
        }
    }
    let ss: Vec<_> = p
        .columns
        .into_iter()
        .enumerate()
        .map(|(i, c)| c.into_pl_series(format!("X{i}").into()).into())
        .collect();
    DataFrame::new(ss).map_err(|e| io::Error::other(e.to_string()))
}

fn read_data_int<R: Read>(h: &mut BufReader<R>, parser: UintReader) -> io::Result<DataFrame> {
    let mut p = parser;
    for r in 0..p.nrows {
        for c in p.columns.iter_mut() {
            c.read_to_column(h, r)?;
        }
    }
    let ss: Vec<_> = p
        .columns
        .into_iter()
        .enumerate()
        .map(|(i, c)| c.into_pl_series(format!("X{i}").into()).into())
        .collect();
    DataFrame::new(ss).map_err(|e| io::Error::other(e.to_string()))
}

fn h_read_data_segment<R: Read + Seek>(
    h: &mut BufReader<R>,
    parser: DataReader,
) -> io::Result<DataFrame> {
    h.seek(SeekFrom::Start(parser.begin))?;
    match parser.column_reader {
        ColumnReader::DelimitedAscii(p) => read_data_delim_ascii(h, p),
        ColumnReader::FixedWidthAscii(p) => read_data_ascii_fixed(h, &p),
        ColumnReader::Single(p) => Float32Type::read_matrix(h, p),
        ColumnReader::Double(p) => Float64Type::read_matrix(h, p),
        ColumnReader::Mixed(p) => read_data_mixed(h, p),
        ColumnReader::Uint(p) => read_data_int(h, p),
    }
}

fn h_write_numeric_dataframe<W: Write>(
    h: &mut BufWriter<W>,
    cs: Vec<ColumnType>,
    df: DataFrame,
    conf: &WriteConfig,
) -> ImpureResult<()> {
    let df_nrows = df.height();
    let res = into_writable_columns(df, cs, &conf);
    if let Some(succ) = res {
        succ.try_map(|writable_columns| {
            for r in 0..df_nrows {
                for c in writable_columns.iter() {
                    match c {
                        NumU8(w) => UInt8Type::write_int(h, &w.size, w.column[r]),
                        NumU16(w) => UInt16Type::write_int(h, &w.size, w.column[r]),
                        NumU24(w) => UInt32Type::write_int(h, &w.size, w.column[r]),
                        NumU32(w) => UInt32Type::write_int(h, &w.size, w.column[r]),
                        NumU40(w) => UInt64Type::write_int(h, &w.size, w.column[r]),
                        NumU48(w) => UInt64Type::write_int(h, &w.size, w.column[r]),
                        NumU56(w) => UInt64Type::write_int(h, &w.size, w.column[r]),
                        NumU64(w) => UInt64Type::write_int(h, &w.size, w.column[r]),
                        NumF32(w) => Float32Type::write_float(h, &w.size, w.column[r]),
                        NumF64(w) => Float64Type::write_float(h, &w.size, w.column[r]),
                        AsciiU8(w) => u8::write_ascii_int(h, w.bytes, w.column[r]),
                        AsciiU16(w) => u16::write_ascii_int(h, w.bytes, w.column[r]),
                        AsciiU32(w) => u32::write_ascii_int(h, w.bytes, w.column[r]),
                        AsciiU64(w) => u64::write_ascii_int(h, w.bytes, w.column[r]),
                    }?
                }
            }
            Ok(PureSuccess::from(()))
        })
    } else {
        // TODO lame error message
        Err(io::Error::other(
            "could not get data from dataframe".to_string(),
        ))?
    }
}

fn h_write_delimited_matrix<W: Write>(
    h: &mut BufWriter<W>,
    nrows: usize,
    columns: Vec<Vec<u64>>,
) -> ImpureResult<()> {
    let ncols = columns.len();
    for ri in 0..nrows {
        for (ci, c) in columns.iter().enumerate() {
            let x = c[ri];
            // if zero, just write "0", if anything else convert
            // to a string and write that
            if x == 0 {
                h.write_all(&[48])?; // 48 = "0" in ASCII
            } else {
                let s = x.to_string();
                let t = s.trim_start_matches("0");
                let buf = t.as_bytes();
                h.write_all(buf)?;
            }
            // write delimiter after all but last value
            if !(ci == ncols - 1 && ri == nrows - 1) {
                h.write_all(&[32])?; // 32 = space in ASCII
            }
        }
    }
    Ok(PureSuccess::from(()))
}

fn h_write_dataset<W: Write>(
    h: &mut BufWriter<W>,
    d: CoreDataset,
    conf: &WriteConfig,
) -> ImpureResult<()> {
    let analysis_len = d.analysis.len();
    let df = d.data;
    let df_ncols = df.width();

    // Check that the dataframe isn't "ragged" (columns are different lengths).
    // If this is false, something terrible happened and we need to stop
    // immediately.
    // if df.is_ragged() {
    //     Err(Failure::new(
    //         "dataframe has unequal column lengths".to_string(),
    //     ))?;
    // }

    // We can now confidently count the number of events (rows)
    let nrows = df.height();

    // Get the layout, or bail if we can't
    let layout = d.text.as_writer_data_layout().map_err(|es| Failure {
        reason: "could not create data layout".to_string(),
        deferred: PureErrorBuf::from_many(es, PureErrorLevel::Error),
    })?;

    // Count number of measurements from layout. If the dataframe doesn't match
    // then something terrible happened and we need to escape through the
    // wormhole.
    let par = layout.ncols();
    if df_ncols != par {
        Err(Failure::new(format!(
            "datafame columns ({df_ncols}) unequal to number of measurements ({par})"
        )))?;
    }

    // Make common HEADER+TEXT writing function, for which the only unknown
    // now is the length of DATA.
    let write_text = |h: &mut BufWriter<W>, data_len| -> ImpureResult<()> {
        if let Some(text) = d.text.text_segment(nrows, data_len, analysis_len) {
            for t in text {
                h.write_all(t.as_bytes())?;
                h.write_all(&[conf.delim.inner()])?;
            }
        } else {
            Err(Failure::new(
                "primary TEXT does not fit into first 99,999,999 bytes".to_string(),
            ))?;
        }
        Ok(PureSuccess::from(()))
    };

    let res = if nrows == 0 {
        // Write HEADER+TEXT with no DATA if dataframe is empty. This assumes
        // the dataframe has the proper number of columns, but each column is
        // empty.
        write_text(h, 0)?;
        Ok(PureSuccess::from(()))
    } else {
        match layout {
            // For alphanumeric, only need to coerce the dataframe to the proper
            // types in each column and write these out bit-for-bit. User will
            // be warned if truncation happens.
            DataLayout::AlphaNum {
                nrows: _,
                columns: col_types,
            } => {
                // ASSUME the dataframe will be coerced such that this
                // relationship will hold true
                let event_width: usize = col_types.iter().map(|c| c.width()).sum();
                let data_len = event_width * nrows;
                write_text(h, data_len)?;
                h_write_numeric_dataframe(h, col_types, df, conf)
            }

            // For delimited ASCII, need to first convert dataframe to u64 and
            // then figure out how much space this will take up based on a)
            // number of values in dataframe, and b) number of digits in each
            // value. Then convert values to strings and write byte
            // representation of strings. Fun...
            DataLayout::AsciiDelimited { nrows: _, ncols: _ } => {
                if let Some(succ) = into_writable_matrix64(df, &conf) {
                    succ.try_map(|columns| {
                        let ndelim = df_ncols * nrows - 1;
                        // TODO cast?
                        let value_nbytes: u32 = columns
                            .iter()
                            .flat_map(|rows| rows.iter().map(|x| x.checked_ilog10().unwrap_or(1)))
                            .sum();
                        // compute data length (delimiters + number of digits)
                        let data_len = value_nbytes as usize + ndelim;
                        // write HEADER+TEXT
                        write_text(h, data_len)?;
                        // write DATA
                        h_write_delimited_matrix(h, nrows, columns)
                    })
                } else {
                    // TODO lame...
                    Err(io::Error::other(
                        "could not get data from dataframe".to_string(),
                    ))?
                }
            }
        }
    };

    h.write_all(&d.analysis)?;
    res
}

fn lookup_data_offsets(
    kws: &mut RawKeywords,
    conf: &DataReadConfig,
    version: Version,
    default: &Segment,
) -> PureSuccess<Segment> {
    KwParser::run(kws, (&conf.standard).into(), |st| match version {
        Version::FCS2_0 => *default,
        _ => {
            let b = st.lookup_begindata();
            let e = st.lookup_enddata();
            if let (Some(begin), Some(end)) = (b, e) {
                let res = Segment::try_new(begin, end, conf.data, SegmentId::Data);
                match res {
                    Ok(seg) => seg,
                    Err(err) => {
                        st.push_meta_warning(format!(
                            "defaulting to header DATA offsets due to error: {}",
                            err
                        ));
                        *default
                    }
                }
            } else {
                st.push_meta_warning(
                    "could not find DATA offsets in TEXT, defaulting to HEADER offsets".to_string(),
                );
                *default
            }
        }
    })
}

fn lookup_analysis_offsets(
    kws: &mut RawKeywords,
    conf: &DataReadConfig,
    version: Version,
    default: &Segment,
) -> PureSuccess<Segment> {
    let go = |st: &mut KwParser, b, e| {
        if let (Some(begin), Some(end)) = (b, e) {
            Segment::try_new(begin, end, conf.analysis, SegmentId::Analysis)
        } else {
            let msg = "could not find DATA offsets in TEXT, \
                 defaulting to HEADER offsets"
                .to_string();
            st.push_meta_warning(msg);
            Ok(*default)
        }
    };
    KwParser::run(kws, (&conf.standard).into(), |st| {
        match version {
            Version::FCS2_0 => Ok(*default),
            Version::FCS3_0 | Version::FCS3_1 => {
                let b = st.lookup_beginanalysis_req();
                let e = st.lookup_endanalysis_req();
                go(st, b, e)
            }
            Version::FCS3_2 => {
                let b = st.lookup_beginanalysis_opt();
                let e = st.lookup_endanalysis_opt();
                go(st, b.0, e.0)
            }
        }
        .map_err(|err| {
            let msg = format!("defaulting to header ANALYSIS offsets due to error: {err}",);
            st.push_meta_warning(msg);
        })
        .unwrap_or(*default)
    })
}

fn h_read_analysis<R: Read + Seek>(h: &mut BufReader<R>, seg: &Segment) -> io::Result<Vec<u8>> {
    let mut buf = vec![];
    h.seek(SeekFrom::Start(u64::from(seg.begin())))?;
    h.take(u64::from(seg.nbytes())).read_to_end(&mut buf)?;
    Ok(buf)
}

fn h_read_std_dataset<R: Read + Seek>(
    h: &mut BufReader<R>,
    std: StandardizedTEXT,
    conf: &DataReadConfig,
) -> ImpureResult<StandardizedDataset> {
    let mut kws = std.remainder;
    let version = std.standardized.version();
    let anal_succ = lookup_analysis_offsets(&mut kws, conf, version, &std.offsets.analysis);
    lookup_data_offsets(&mut kws, conf, version, &std.offsets.data)
        .and_then(|data_seg| {
            std.standardized
                .as_data_reader(&mut kws, conf, &data_seg)
                .combine(anal_succ, |data_parser, analysis_seg| {
                    (data_parser, data_seg, analysis_seg)
                })
        })
        .try_map(|(data_maybe, data_seg, analysis_seg)| {
            let dmsg = "could not create data parser".to_string();
            let data_parser = data_maybe.ok_or(Failure::new(dmsg))?;
            let mut data = h_read_data_segment(h, data_parser)?;
            let analysis = h_read_analysis(h, &analysis_seg)?;
            std.standardized.set_df_column_names(&mut data);
            Ok(PureSuccess::from(StandardizedDataset {
                offsets: Offsets {
                    prim_text: std.offsets.prim_text,
                    supp_text: std.offsets.supp_text,
                    nextdata: std.offsets.nextdata,
                    data: data_seg,
                    analysis: analysis_seg,
                },
                delimiter: std.delimiter,
                remainder: kws,
                dataset: CoreDataset {
                    data,
                    text: std.standardized,
                    analysis,
                },
                deviant: std.deviant,
            }))
        })
}

impl VersionedParserMeasurement for InnerMeasurement2_0 {
    type Target = ();

    fn as_minimal_inner(_: &Measurement<Self>) {}

    fn datatype_minimal(_: &DataReadMeasurement<Self::Target>) -> Option<NumType> {
        None
    }

    fn datatype(_: &Measurement<Self>) -> Option<NumType> {
        None
    }
}

impl VersionedParserMeasurement for InnerMeasurement3_0 {
    type Target = ();

    fn as_minimal_inner(_: &Measurement<Self>) {}

    fn datatype_minimal(_: &DataReadMeasurement<Self::Target>) -> Option<NumType> {
        None
    }

    fn datatype(_: &Measurement<Self>) -> Option<NumType> {
        None
    }
}

impl VersionedParserMeasurement for InnerMeasurement3_1 {
    type Target = ();

    fn as_minimal_inner(_: &Measurement<Self>) {}

    fn datatype_minimal(_: &DataReadMeasurement<Self::Target>) -> Option<NumType> {
        None
    }

    fn datatype(_: &Measurement<Self>) -> Option<NumType> {
        None
    }
}

impl VersionedParserMeasurement for InnerMeasurement3_2 {
    type Target = InnerDataReadMeasurement3_2;

    fn as_minimal_inner(m: &Measurement<Self>) -> InnerDataReadMeasurement3_2 {
        InnerDataReadMeasurement3_2 {
            datatype: m.specific.datatype.0.as_ref().copied().into(),
        }
    }

    fn datatype_minimal(m: &DataReadMeasurement<Self::Target>) -> Option<NumType> {
        m.specific.datatype.0.as_ref().copied()
    }

    // TODO lame?
    fn datatype(m: &Measurement<Self>) -> Option<NumType> {
        m.specific.datatype.0.as_ref().copied()
    }
}

impl VersionedParserMetadata for InnerMetadata2_0 {
    type Target = InnerBareMetadata2_0;

    fn byteord(&self) -> ByteOrd {
        self.byteord.clone()
    }

    fn target_byteord(t: &Self::Target) -> ByteOrd {
        t.byteord.clone()
    }

    fn tot(t: &Self::Target) -> Option<usize> {
        t.tot.0.as_ref().copied()
    }

    fn as_minimal_inner(&self, st: &mut KwParser) -> Option<InnerBareMetadata2_0> {
        Some(InnerBareMetadata2_0 {
            byteord: self.byteord.clone(),
            tot: st.lookup_tot_opt(),
        })
    }
}

impl VersionedParserMetadata for InnerMetadata3_0 {
    type Target = InnerBareMetadata3_0;

    fn byteord(&self) -> ByteOrd {
        self.byteord.clone()
    }

    fn target_byteord(t: &Self::Target) -> ByteOrd {
        t.byteord.clone()
    }

    fn tot(t: &Self::Target) -> Option<usize> {
        Some(t.tot)
    }

    fn as_minimal_inner(&self, st: &mut KwParser) -> Option<InnerBareMetadata3_0> {
        st.lookup_tot_req().map(|tot| InnerBareMetadata3_0 {
            byteord: self.byteord.clone(),
            tot,
        })
    }
}

impl VersionedParserMetadata for InnerMetadata3_1 {
    type Target = InnerBareMetadata3_1;

    fn byteord(&self) -> ByteOrd {
        ByteOrd::Endian(self.byteord)
    }

    fn target_byteord(t: &Self::Target) -> ByteOrd {
        ByteOrd::Endian(t.byteord)
    }

    fn tot(t: &Self::Target) -> Option<usize> {
        Some(t.tot)
    }

    fn as_minimal_inner(&self, st: &mut KwParser) -> Option<InnerBareMetadata3_1> {
        st.lookup_tot_req().map(|tot| InnerBareMetadata3_1 {
            byteord: self.byteord,
            tot,
        })
    }
}

impl VersionedParserMetadata for InnerMetadata3_2 {
    type Target = InnerBareMetadata3_1;

    fn byteord(&self) -> ByteOrd {
        ByteOrd::Endian(self.byteord)
    }

    fn target_byteord(t: &Self::Target) -> ByteOrd {
        ByteOrd::Endian(t.byteord)
    }

    fn tot(t: &Self::Target) -> Option<usize> {
        Some(t.tot)
    }

    fn as_minimal_inner(&self, st: &mut KwParser) -> Option<InnerBareMetadata3_1> {
        st.lookup_tot_req().map(|tot| InnerBareMetadata3_1 {
            byteord: self.byteord,
            tot,
        })
    }
}

macro_rules! get_set_pre_3_2_datetime {
    ($fcstime:ident) => {
        fn begin_date(&self) -> Option<NaiveDate> {
            self.timestamps.date.0.as_ref().map(|x| x.0)
        }

        fn end_date(&self) -> Option<NaiveDate> {
            self.begin_date()
        }

        fn begin_time(&self) -> Option<NaiveTime> {
            self.timestamps.btim.0.as_ref().map(|x| x.0)
        }

        fn end_time(&self) -> Option<NaiveTime> {
            self.timestamps.etim.0.as_ref().map(|x| x.0)
        }

        fn set_datetimes_inner(
            &mut self,
            begin: DateTime<FixedOffset>,
            end: DateTime<FixedOffset>,
        ) {
            let d1 = begin.date_naive();
            let d2 = end.date_naive();
            self.timestamps.btim = Some($fcstime(begin.time())).into();
            self.timestamps.etim = Some($fcstime(end.time())).into();
            // If two dates are the same, set $DATE, if not, then keep date
            // unset since pre-3.2 versions do not have a way to store two
            // dates. This is an inherent limitation in these early versions.
            self.timestamps.date = if d1 != d2 { None } else { Some(FCSDate(d1)) }.into();
        }

        fn clear_datetimes(&mut self) {
            self.timestamps.btim = None.into();
            self.timestamps.etim = None.into();
            self.timestamps.date = None.into();
        }
    };
}

impl VersionedMetadata for InnerMetadata2_0 {
    type P = InnerMeasurement2_0;

    fn into_any(t: CoreTEXT2_0) -> AnyCoreTEXT {
        AnyCoreTEXT::FCS2_0(Box::new(t))
    }

    fn timestamps_valid(&self) -> bool {
        self.timestamps.valid()
    }

    fn datetimes_valid(&self) -> bool {
        true
    }

    fn has_timestep(&self) -> bool {
        false
    }

    fn check_unstainedcenters(&self, _: &HashSet<&Shortname>) -> Option<String> {
        None
    }

    fn check_spillover(&self, _: &HashSet<&Shortname>) -> Option<String> {
        None
    }

    get_set_pre_3_2_datetime!(FCSTime);

    fn lookup_specific(st: &mut KwParser, par: usize) -> Option<InnerMetadata2_0> {
        let maybe_mode = st.lookup_mode();
        let maybe_byteord = st.lookup_byteord();
        if let (Some(mode), Some(byteord)) = (maybe_mode, maybe_byteord) {
            Some(InnerMetadata2_0 {
                mode,
                byteord,
                cyt: st.lookup_cyt_opt(),
                comp: st.lookup_compensation_2_0(par),
                timestamps: st.lookup_timestamps2_0(),
            })
        } else {
            None
        }
    }

    fn keywords_req_inner(&self) -> Vec<(&'static str, String)> {
        [
            (MODE, self.mode.to_string()),
            (BYTEORD, self.byteord.to_string()),
        ]
        .into_iter()
        .collect()
    }

    fn keywords_opt_inner(&self) -> Vec<(&'static str, String)> {
        [
            (CYT, self.cyt.as_opt_string()),
            (COMP, self.comp.as_opt_string()),
            (BTIM, self.timestamps.btim.as_opt_string()),
            (ETIM, self.timestamps.etim.as_opt_string()),
            (DATE, self.timestamps.date.as_opt_string()),
        ]
        .into_iter()
        .flat_map(|(k, v)| v.map(|x| (k, x)))
        .collect()
    }
}

impl VersionedMetadata for InnerMetadata3_0 {
    type P = InnerMeasurement3_0;

    fn into_any(t: CoreTEXT3_0) -> AnyCoreTEXT {
        AnyCoreTEXT::FCS3_0(Box::new(t))
    }

    fn timestamps_valid(&self) -> bool {
        self.timestamps.valid()
    }

    fn datetimes_valid(&self) -> bool {
        true
    }

    fn has_timestep(&self) -> bool {
        self.timestep.0.is_some()
    }

    fn check_unstainedcenters(&self, _: &HashSet<&Shortname>) -> Option<String> {
        None
    }

    fn check_spillover(&self, _: &HashSet<&Shortname>) -> Option<String> {
        None
    }

    get_set_pre_3_2_datetime!(FCSTime60);

    fn lookup_specific(st: &mut KwParser, _: usize) -> Option<InnerMetadata3_0> {
        let maybe_mode = st.lookup_mode();
        let maybe_byteord = st.lookup_byteord();
        if let (Some(mode), Some(byteord)) = (maybe_mode, maybe_byteord) {
            Some(InnerMetadata3_0 {
                mode,
                byteord,
                cyt: st.lookup_cyt_opt(),
                comp: st.lookup_compensation_3_0(),
                timestamps: st.lookup_timestamps3_0(),
                cytsn: st.lookup_cytsn(),
                timestep: st.lookup_timestep(),
                unicode: st.lookup_unicode(),
            })
        } else {
            None
        }
    }

    fn keywords_req_inner(&self) -> Vec<(&'static str, String)> {
        [
            (MODE, self.mode.to_string()),
            (BYTEORD, self.byteord.to_string()),
        ]
        .into_iter()
        .collect()
    }

    fn keywords_opt_inner(&self) -> Vec<(&'static str, String)> {
        let ts = &self.timestamps;
        [
            (CYT, self.cyt.as_opt_string()),
            (COMP, self.comp.as_opt_string()),
            (BTIM, ts.btim.as_opt_string()),
            (ETIM, ts.etim.as_opt_string()),
            (DATE, ts.date.as_opt_string()),
            (CYTSN, self.cytsn.as_opt_string()),
            (TIMESTEP, self.timestep.as_opt_string()),
            (UNICODE, self.unicode.as_opt_string()),
        ]
        .into_iter()
        .flat_map(|(k, v)| v.map(|x| (k, x)))
        .collect()
    }
}

impl VersionedMetadata for InnerMetadata3_1 {
    type P = InnerMeasurement3_1;

    fn into_any(t: CoreTEXT3_1) -> AnyCoreTEXT {
        AnyCoreTEXT::FCS3_1(Box::new(t))
    }

    fn timestamps_valid(&self) -> bool {
        self.timestamps.valid()
    }

    fn datetimes_valid(&self) -> bool {
        true
    }

    fn has_timestep(&self) -> bool {
        self.timestep.0.is_some()
    }

    fn check_unstainedcenters(&self, _: &HashSet<&Shortname>) -> Option<String> {
        None
    }

    fn check_spillover(&self, names: &HashSet<&Shortname>) -> Option<String> {
        self.spillover.0.as_ref().and_then(|s| {
            let xs: Vec<_> = s.measurements.iter().collect();
            check_noexist(xs.as_slice(), names, SPILLOVER)
        })
    }

    get_set_pre_3_2_datetime!(FCSTime100);

    fn lookup_specific(st: &mut KwParser, _: usize) -> Option<InnerMetadata3_1> {
        let maybe_mode = st.lookup_mode();
        let maybe_byteord = st.lookup_endian();
        if let (Some(mode), Some(byteord)) = (maybe_mode, maybe_byteord) {
            // TODO make deprecated error
            // if mode != Mode::List {
            //     st.push_meta_deprecated_str("$MODE should only be L");
            // };
            Some(InnerMetadata3_1 {
                mode,
                byteord,
                cyt: st.lookup_cyt_opt(),
                spillover: st.lookup_spillover(),
                timestamps: st.lookup_timestamps3_1(false),
                cytsn: st.lookup_cytsn(),
                timestep: st.lookup_timestep(),
                modification: st.lookup_modification(),
                plate: st.lookup_plate(false),
                vol: st.lookup_vol(),
            })
        } else {
            None
        }
    }

    fn keywords_req_inner(&self) -> Vec<(&'static str, String)> {
        [
            (MODE, self.mode.to_string()),
            (BYTEORD, self.byteord.to_string()),
        ]
        .into_iter()
        .collect()
    }

    fn keywords_opt_inner(&self) -> Vec<(&'static str, String)> {
        let mdn = &self.modification;
        let ts = &self.timestamps;
        let pl = &self.plate;
        [
            (CYT, self.cyt.as_opt_string()),
            (SPILLOVER, self.spillover.as_opt_string()),
            (BTIM, ts.btim.as_opt_string()),
            (ETIM, ts.etim.as_opt_string()),
            (DATE, ts.date.as_opt_string()),
            (CYTSN, self.cytsn.as_opt_string()),
            (TIMESTEP, self.timestep.as_opt_string()),
            (LAST_MODIFIER, mdn.last_modifier.as_opt_string()),
            (LAST_MODIFIED, mdn.last_modified.as_opt_string()),
            (ORIGINALITY, mdn.originality.as_opt_string()),
            (PLATEID, pl.plateid.as_opt_string()),
            (PLATENAME, pl.platename.as_opt_string()),
            (WELLID, pl.wellid.as_opt_string()),
            (VOL, self.vol.as_opt_string()),
        ]
        .into_iter()
        .flat_map(|(k, v)| v.map(|x| (k, x)))
        .collect()
    }
}

// TODO moveme
fn check_noexist(
    xs: &[&Shortname],
    names: &HashSet<&Shortname>,
    which: &'static str,
) -> Option<String> {
    let noexist: Vec<_> = xs.iter().filter(|x| !names.contains(*x)).collect();
    if !noexist.is_empty() {
        let msg = format!(
            "{} refers to non-existent measurements: {}",
            which,
            noexist.iter().join(","),
        );
        return Some(msg);
    }
    None
}

impl VersionedMetadata for InnerMetadata3_2 {
    type P = InnerMeasurement3_2;

    fn into_any(t: CoreTEXT3_2) -> AnyCoreTEXT {
        AnyCoreTEXT::FCS3_2(Box::new(t))
    }

    fn timestamps_valid(&self) -> bool {
        self.timestamps.valid()
    }

    fn datetimes_valid(&self) -> bool {
        self.datetimes.valid()
    }

    fn has_timestep(&self) -> bool {
        self.timestep.0.is_some()
    }

    fn check_unstainedcenters(&self, names: &HashSet<&Shortname>) -> Option<String> {
        self.unstained.unstainedcenters.0.as_ref().and_then(|u| {
            let xs: Vec<_> = u.0.keys().collect();
            check_noexist(xs.as_slice(), names, UNSTAINEDCENTERS)
        })
    }

    fn check_spillover(&self, names: &HashSet<&Shortname>) -> Option<String> {
        self.spillover.0.as_ref().and_then(|s| {
            let xs: Vec<_> = s.measurements.iter().collect();
            check_noexist(xs.as_slice(), names, SPILLOVER)
        })
    }

    // TODO not DRY
    fn begin_date(&self) -> Option<NaiveDate> {
        self.datetimes
            .begin
            .0
            .as_ref()
            .map(|x| x.0.date_naive())
            .or(self.timestamps.date.0.as_ref().map(|x| x.0))
    }

    fn end_date(&self) -> Option<NaiveDate> {
        self.datetimes
            .end
            .0
            .as_ref()
            .map(|x| x.0.date_naive())
            .or(self.timestamps.date.0.as_ref().map(|x| x.0))
    }

    fn begin_time(&self) -> Option<NaiveTime> {
        self.datetimes.begin.0.as_ref().map(|x| x.0.time()).or(self
            .timestamps
            .btim
            .0
            .as_ref()
            .map(|x| x.0))
    }

    fn end_time(&self) -> Option<NaiveTime> {
        self.datetimes.end.0.as_ref().map(|x| x.0.time()).or(self
            .timestamps
            .etim
            .0
            .as_ref()
            .map(|x| x.0))
    }

    fn set_datetimes_inner(&mut self, begin: DateTime<FixedOffset>, end: DateTime<FixedOffset>) {
        self.datetimes.begin = Some(FCSDateTime(begin)).into();
        self.datetimes.end = Some(FCSDateTime(end)).into();
        self.timestamps.btim = None.into();
        self.timestamps.etim = None.into();
        self.timestamps.date = None.into();
    }

    fn clear_datetimes(&mut self) {
        self.datetimes.begin = None.into();
        self.datetimes.end = None.into();
        self.timestamps.btim = None.into();
        self.timestamps.etim = None.into();
        self.timestamps.date = None.into();
    }

    fn lookup_specific(st: &mut KwParser, _: usize) -> Option<InnerMetadata3_2> {
        // Only L is allowed as of 3.2, so pull the value and check it if given.
        // The only thing we care about is that the value is valid, since we
        // don't need to use it anywhere.
        let _ = st.lookup_mode3_2();
        let maybe_byteord = st.lookup_endian();
        let maybe_cyt = st.lookup_cyt_req();
        if let (Some(byteord), Some(cyt)) = (maybe_byteord, maybe_cyt) {
            Some(InnerMetadata3_2 {
                byteord,
                cyt,
                spillover: st.lookup_spillover(),
                timestamps: st.lookup_timestamps3_1(true),
                cytsn: st.lookup_cytsn(),
                timestep: st.lookup_timestep(),
                modification: st.lookup_modification(),
                plate: st.lookup_plate(true),
                vol: st.lookup_vol(),
                carrier: st.lookup_carrier(),
                datetimes: st.lookup_datetimes(),
                unstained: st.lookup_unstained(),
                flowrate: st.lookup_flowrate(),
            })
        } else {
            None
        }
    }

    fn keywords_req_inner(&self) -> Vec<(&'static str, String)> {
        [
            (BYTEORD, self.byteord.to_string()),
            (CYT, self.cyt.to_string()),
        ]
        .into_iter()
        .collect()
    }

    fn keywords_opt_inner(&self) -> Vec<(&'static str, String)> {
        let mdn = &self.modification;
        let ts = &self.timestamps;
        let pl = &self.plate;
        let car = &self.carrier;
        let dt = &self.datetimes;
        let us = &self.unstained;
        [
            (SPILLOVER, self.spillover.as_opt_string()),
            (BTIM, ts.btim.as_opt_string()),
            (ETIM, ts.etim.as_opt_string()),
            (DATE, ts.date.as_opt_string()),
            (CYTSN, self.cytsn.as_opt_string()),
            (TIMESTEP, self.timestep.as_opt_string()),
            (LAST_MODIFIER, mdn.last_modifier.as_opt_string()),
            (LAST_MODIFIED, mdn.last_modified.as_opt_string()),
            (ORIGINALITY, mdn.originality.as_opt_string()),
            (PLATEID, pl.plateid.as_opt_string()),
            (PLATENAME, pl.platename.as_opt_string()),
            (WELLID, pl.wellid.as_opt_string()),
            (VOL, self.vol.as_opt_string()),
            (CARRIERID, car.carrierid.as_opt_string()),
            (CARRIERTYPE, car.carriertype.as_opt_string()),
            (LOCATIONID, car.locationid.as_opt_string()),
            (BEGINDATETIME, dt.begin.as_opt_string()),
            (ENDDATETIME, dt.end.as_opt_string()),
            (UNSTAINEDCENTERS, us.unstainedcenters.as_opt_string()),
            (UNSTAINEDINFO, us.unstainedinfo.as_opt_string()),
            (FLOWRATE, self.flowrate.as_opt_string()),
        ]
        .into_iter()
        .flat_map(|(k, v)| v.map(|x| (k, x)))
        .collect()
    }
}

fn parse_raw_text(
    version: Version,
    kws: &mut RawKeywords,
    conf: &StdTextReadConfig,
) -> PureResult<AnyCoreTEXT> {
    match version {
        Version::FCS2_0 => CoreTEXT2_0::any_from_raw(kws, conf),
        Version::FCS3_0 => CoreTEXT3_0::any_from_raw(kws, conf),
        Version::FCS3_1 => CoreTEXT3_1::any_from_raw(kws, conf),
        Version::FCS3_2 => CoreTEXT3_2::any_from_raw(kws, conf),
    }
}

macro_rules! kws_req {
    ($name:ident, $ret:ty, $key:expr, $dep:expr ) => {
        fn $name(&mut self) -> Option<$ret> {
            self.lookup_required($key, $dep)
        }
    };
}

macro_rules! kws_opt {
    ($name:ident, $ret:ty, $key:expr, $dep:expr ) => {
        fn $name(&mut self) -> OptionalKw<$ret> {
            self.lookup_optional($key, $dep)
        }
    };
}

macro_rules! kws_meas_req {
    ($name:ident, $ret:ty, $key:expr, $dep:expr ) => {
        fn $name(&mut self, n: usize) -> Option<$ret> {
            self.lookup_meas_req($key, n, $dep)
        }
    };
}

macro_rules! kws_meas_opt {
    ($name:ident, $ret:ty, $key:expr, $dep:expr ) => {
        fn $name(&mut self, n: usize) -> OptionalKw<$ret> {
            self.lookup_meas_opt($key, n, $dep)
        }
    };
}

/// A structure to look up and parse keywords in the TEXT segment
///
/// Given a hash table of keywords (as String pairs) and a configuration, this
/// provides an interface to extract keywords. If found, the keyword will be
/// removed from the table and parsed to its native type (number, list, matrix,
/// etc). If lookup or parsing fails, an error/warning will be logged within the
/// state depending on if the key is required or optional (among other things
/// depending on the keyword).
///
/// Errors in all cases are deferred until the end of the state's lifetime, at
/// which point the errors are returned along with the result of the computation
/// or failure (if applicable).
///
/// For Haskell zealots, this is somewhat like a pure state monad in that it has
/// a 'run' interface which takes an input and a function to act within the
/// state. The main difference is that it modifies the keywords in-place
/// and returns the error result (rather than purely returning both).
struct KwParser<'a, 'b> {
    raw_keywords: &'b mut RawKeywords,
    deferred: PureErrorBuf,
    conf: KwParserConfig<'a>,
}

#[derive(Default, Clone)]
struct KwParserConfig<'a> {
    disallow_deprecated: bool,
    nonstandard_measurement_pattern: Option<&'a NonStdMeasPattern>,
}

impl<'a> From<&'a RawTextReadConfig> for KwParserConfig<'a> {
    fn from(value: &'a RawTextReadConfig) -> Self {
        KwParserConfig {
            disallow_deprecated: value.disallow_deprecated,
            nonstandard_measurement_pattern: None,
        }
    }
}

impl<'a> From<&'a StdTextReadConfig> for KwParserConfig<'a> {
    fn from(value: &'a StdTextReadConfig) -> Self {
        KwParserConfig {
            disallow_deprecated: value.raw.disallow_deprecated,
            nonstandard_measurement_pattern: value.nonstandard_measurement_pattern.as_ref(),
        }
    }
}

impl<'a, 'b> KwParser<'a, 'b> {
    /// Run a computation within a keyword lookup context.
    ///
    /// The computation must return a result, although it may record deferred
    /// errors along the way which will be returned.
    ///
    /// Any errors which are logged must be pushed into the state's error buffer
    /// directly, as errors are not allowed to be returned by the inner
    /// computation.
    fn run<X, F>(kws: &'b mut RawKeywords, conf: KwParserConfig<'a>, f: F) -> PureSuccess<X>
    where
        F: FnOnce(&mut Self) -> X,
    {
        let mut st = Self::from(kws, conf);
        let data = f(&mut st);
        PureSuccess {
            data,
            deferred: st.collect(),
        }
    }

    /// Run a computation within a keyword lookup context which may fail.
    ///
    /// This is like 'run' except the computation may not return anything (None)
    /// in which case Err(Failure(...)) will be returned along with the reason
    /// for failure which must be given a priori.
    ///
    /// Any errors which are logged must be pushed into the state's error buffer
    /// directly, as errors are not allowed to be returned by the inner
    /// computation.
    fn try_run<X, F>(
        kws: &'b mut RawKeywords,
        conf: KwParserConfig<'a>,
        reason: String,
        f: F,
    ) -> PureResult<X>
    where
        F: FnOnce(&mut Self) -> Option<X>,
    {
        let mut st = Self::from(kws, conf);
        if let Some(data) = f(&mut st) {
            Ok(PureSuccess {
                data,
                deferred: st.collect(),
            })
        } else {
            Err(st.into_failure(reason))
        }
    }

    // offsets

    kws_req!(lookup_begindata, u32, BEGINDATA, false);
    kws_req!(lookup_enddata, u32, ENDDATA, false);
    kws_req!(lookup_beginstext_req, u32, BEGINSTEXT, false);
    kws_req!(lookup_endstext_req, u32, ENDSTEXT, false);
    kws_req!(lookup_beginanalysis_req, u32, BEGINANALYSIS, false);
    kws_req!(lookup_endanalysis_req, u32, ENDANALYSIS, false);
    kws_opt!(lookup_beginstext_opt, u32, BEGINSTEXT, false);
    kws_opt!(lookup_endstext_opt, u32, ENDSTEXT, false);
    kws_opt!(lookup_beginanalysis_opt, u32, BEGINANALYSIS, false);
    kws_opt!(lookup_endanalysis_opt, u32, ENDANALYSIS, false);
    kws_req!(lookup_nextdata, u32, NEXTDATA, false);

    // TODO add more

    // metadata

    kws_req!(lookup_par, usize, PAR, false);
    kws_req!(lookup_tot_req, usize, TOT, false);
    kws_opt!(lookup_tot_opt, usize, TOT, false);

    kws_req!(lookup_byteord, ByteOrd, BYTEORD, false);
    kws_req!(lookup_endian, Endian, BYTEORD, false);
    kws_req!(lookup_datatype, AlphaNumType, DATATYPE, false);
    kws_req!(lookup_mode, Mode, MODE, false);
    kws_opt!(lookup_mode3_2, Mode3_2, MODE, true);
    kws_req!(lookup_cyt_req, Cyt, CYT, false);
    kws_opt!(lookup_cyt_opt, Cyt, CYT, false);
    kws_opt!(lookup_cytsn, Cytsn, CYTSN, false);
    kws_opt!(lookup_abrt, Abrt, ABRT, false);
    kws_opt!(lookup_cells, Cells, CELLS, false);
    kws_opt!(lookup_com, Com, COM, false);
    kws_opt!(lookup_exp, Exp, EXP, false);
    kws_opt!(lookup_fil, Fil, FIL, false);
    kws_opt!(lookup_inst, Inst, INST, false);
    kws_opt!(lookup_lost, Lost, LOST, false);
    kws_opt!(lookup_op, Op, OP, false);
    kws_opt!(lookup_proj, Proj, PROJ, false);
    kws_opt!(lookup_smno, Smno, SMNO, false);
    kws_opt!(lookup_src, Src, SRC, false);
    kws_opt!(lookup_sys, Sys, SYS, false);
    kws_opt!(lookup_trigger, Trigger, TR, false);
    kws_opt!(lookup_timestep, Timestep, TIMESTEP, false);
    kws_opt!(lookup_vol, Vol, VOL, false);
    kws_opt!(lookup_flowrate, Flowrate, FLOWRATE, false);
    kws_opt!(lookup_unicode, Unicode, UNICODE, false);
    kws_opt!(lookup_unstainedinfo, UnstainedInfo, UNSTAINEDINFO, false);
    kws_opt!(
        lookup_unstainedcenters,
        UnstainedCenters,
        UNSTAINEDCENTERS,
        false
    );
    kws_opt!(lookup_last_modifier, LastModifier, LAST_MODIFIER, false);
    kws_opt!(lookup_last_modified, ModifiedDateTime, LAST_MODIFIED, false);
    kws_opt!(lookup_originality, Originality, ORIGINALITY, false);
    kws_opt!(lookup_carrierid, Carrierid, CARRIERID, false);
    kws_opt!(lookup_carriertype, Carriertype, CARRIERTYPE, false);
    kws_opt!(lookup_locationid, Locationid, LOCATIONID, false);
    kws_opt!(lookup_begindatetime, FCSDateTime, BEGINDATETIME, false);
    kws_opt!(lookup_enddatetime, FCSDateTime, ENDDATETIME, false);
    kws_opt!(lookup_btim, FCSTime, BTIM, false);
    kws_opt!(lookup_etim, FCSTime, ETIM, false);
    kws_opt!(lookup_btim60, FCSTime60, BTIM, false);
    kws_opt!(lookup_etim60, FCSTime60, ETIM, false);
    kws_opt!(lookup_compensation_3_0, Compensation, COMP, false);
    kws_opt!(lookup_spillover, Spillover, SPILLOVER, false);

    fn lookup_date(&mut self, dep: bool) -> OptionalKw<FCSDate> {
        self.lookup_optional(DATE, dep)
    }

    fn lookup_btim100(&mut self, dep: bool) -> OptionalKw<FCSTime100> {
        self.lookup_optional(BTIM, dep)
    }

    fn lookup_etim100(&mut self, dep: bool) -> OptionalKw<FCSTime100> {
        self.lookup_optional(ETIM, dep)
    }

    fn lookup_timestamps2_0(&mut self) -> Timestamps2_0 {
        Timestamps2_0 {
            btim: self.lookup_btim(),
            etim: self.lookup_etim(),
            date: self.lookup_date(false),
        }
    }

    fn lookup_timestamps3_0(&mut self) -> Timestamps3_0 {
        Timestamps3_0 {
            btim: self.lookup_btim60(),
            etim: self.lookup_etim60(),
            date: self.lookup_date(false),
        }
    }

    fn lookup_timestamps3_1(&mut self, dep: bool) -> Timestamps3_1 {
        Timestamps3_1 {
            btim: self.lookup_btim100(dep),
            etim: self.lookup_etim100(dep),
            date: self.lookup_date(dep),
        }
    }

    fn lookup_datetimes(&mut self) -> Datetimes {
        let begin = self.lookup_begindatetime();
        let end = self.lookup_enddatetime();
        Datetimes { begin, end }
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
            wellid: self.lookup_optional(PLATEID, dep),
            platename: self.lookup_optional(PLATENAME, dep),
            plateid: self.lookup_optional(WELLID, dep),
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

    fn lookup_compensation_2_0(&mut self, par: usize) -> OptionalKw<Compensation> {
        // column = src channel
        // row = target channel
        // These are "flipped" in 2.0, where "column" goes TO the "row"
        let mut any_error = false;
        let mut matrix = DMatrix::<f32>::identity(par, par);
        for r in 0..par {
            for c in 0..par {
                let m = format!("DFC{c}TO{r}");
                if let Some(x) = self.lookup_optional(m.as_str(), false).0 {
                    matrix[(r, c)] = x;
                } else {
                    any_error = true;
                }
            }
        }
        if any_error {
            None
        } else {
            Some(Compensation { matrix })
        }
        .into()
    }

    fn lookup_all_nonstandard(&mut self) -> NonStdKeywords {
        let mut ns = HashMap::new();
        self.raw_keywords.retain(|k, v| {
            if let Ok(nk) = k.parse::<NonStdKey>() {
                ns.insert(nk, v.clone());
                false
            } else {
                true
            }
        });
        ns
    }

    // measurements

    kws_meas_req!(lookup_meas_bytes, Bytes, BYTES_SFX, false);
    kws_meas_req!(lookup_meas_range, Range, RANGE_SFX, false);
    kws_meas_opt!(lookup_meas_wavelength, Wavelength, WAVELEN_SFX, false);
    kws_meas_opt!(lookup_meas_wavelengths, Wavelengths, WAVELEN_SFX, false);
    kws_meas_opt!(lookup_meas_power, Power, POWER_SFX, false);
    kws_meas_opt!(lookup_meas_detector_type, DetectorType, DET_TYPE_SFX, false);
    kws_meas_req!(lookup_meas_shortname_req, Shortname, SHORTNAME_SFX, false);
    kws_meas_opt!(lookup_meas_shortname_opt, Shortname, SHORTNAME_SFX, false);
    kws_meas_opt!(lookup_meas_longname, Longname, LONGNAME_SFX, false);
    kws_meas_opt!(lookup_meas_filter, Filter, FILTER_SFX, false);
    kws_meas_opt!(
        lookup_meas_detector_voltage,
        DetectorVoltage,
        DET_VOLT_SFX,
        false
    );
    kws_meas_opt!(lookup_meas_detector, DetectorName, DET_NAME_SFX, false);
    kws_meas_opt!(lookup_meas_tag, Tag, TAG_SFX, false);
    kws_meas_opt!(lookup_meas_analyte, Analyte, ANALYTE_SFX, false);
    kws_meas_opt!(lookup_meas_gain, Gain, GAIN_SFX, false);
    kws_meas_req!(lookup_meas_scale_req, Scale, SCALE_SFX, false);
    kws_meas_opt!(lookup_meas_scale_opt, Scale, SCALE_SFX, false);
    kws_meas_opt!(lookup_meas_cal3_1, Calibration3_1, CALIBRATION_SFX, false);
    kws_meas_opt!(lookup_meas_cal3_2, Calibration3_2, CALIBRATION_SFX, false);
    kws_meas_opt!(lookup_meas_display, Display, DISPLAY_SFX, false);
    kws_meas_opt!(lookup_meas_datatype, NumType, DATATYPE_SFX, false);
    kws_meas_opt!(lookup_meas_type, MeasurementType, DET_TYPE_SFX, false);
    kws_meas_opt!(lookup_meas_feature, Feature, FEATURE_SFX, false);

    fn lookup_meas_percent_emitted(&mut self, n: usize, dep: bool) -> OptionalKw<PercentEmitted> {
        self.lookup_meas_opt(PCNT_EMT_SFX, n, dep)
    }

    fn lookup_all_meas_nonstandard(&mut self, n: usize) -> NonStdKeywords {
        let mut ns = HashMap::new();
        // ASSUME the pattern does not start with "$" and has a %n which will be
        // subbed for the measurement index. The pattern will then be turned
        // into a legit rust regular expression, which may fail depending on
        // what %n does, so check it each time.
        if let Some(p) = &self.conf.nonstandard_measurement_pattern {
            match p.from_index(n) {
                Ok(pattern) => self.raw_keywords.retain(|k, v| {
                    if let Some(nk) = pattern.try_match(k.as_str()) {
                        ns.insert(nk, v.clone());
                        false
                    } else {
                        true
                    }
                }),
                Err(err) => self.push_meta_warning(err.to_string()),
            }
        }
        ns
    }

    fn push_meta_warning(&mut self, msg: String) {
        self.deferred.push_warning(msg);
    }

    // auxiliary functions

    fn collect(self) -> PureErrorBuf {
        self.deferred
    }

    fn into_failure(self, reason: String) -> PureFailure {
        Failure {
            reason,
            deferred: self.collect(),
        }
    }

    fn from(kws: &'b mut RawKeywords, conf: KwParserConfig<'a>) -> Self {
        KwParser {
            raw_keywords: kws,
            deferred: PureErrorBuf::default(),
            conf,
        }
    }

    // TODO not DRY (although will likely need HKTs)
    fn lookup_required<V: FromStr>(&mut self, k: &str, dep: bool) -> Option<V>
    where
        <V as FromStr>::Err: fmt::Display,
    {
        match self.raw_keywords.remove(k) {
            Some(v) => {
                let res = match v.parse() {
                    Err(e) => {
                        let msg = format!("{e} (key='{k}', value='{v}')");
                        self.deferred.push_error(msg);
                        None
                    }
                    Ok(x) => Some(x),
                };
                if dep {
                    self.push_deprecated(k);
                }
                res
            }
            None => {
                let msg = format!("missing required key: {k}");
                self.deferred.push_error(msg);
                None
            }
        }
    }

    fn lookup_optional<V: FromStr>(&mut self, k: &str, dep: bool) -> OptionalKw<V>
    where
        <V as FromStr>::Err: fmt::Display,
    {
        match self.raw_keywords.remove(k) {
            Some(v) => {
                let res = match v.parse() {
                    Err(w) => {
                        let msg = format!("{w} (key={k}, value='{v}')");
                        self.deferred.push_warning(msg);
                        None
                    }
                    Ok(x) => Some(x),
                };
                if dep {
                    self.push_deprecated(k);
                }
                res
            }
            None => None,
        }
        .into()
    }

    fn push_deprecated(&mut self, k: &str) {
        let msg = format!("deprecated key: {k}");
        self.deferred
            .push_msg_leveled(msg, self.conf.disallow_deprecated);
    }

    fn lookup_meas_req<V: FromStr>(&mut self, m: &'static str, n: usize, dep: bool) -> Option<V>
    where
        <V as FromStr>::Err: fmt::Display,
    {
        self.lookup_required(&format_measurement(&n.to_string(), m), dep)
    }

    fn lookup_meas_opt<V: FromStr>(&mut self, m: &'static str, n: usize, dep: bool) -> OptionalKw<V>
    where
        <V as FromStr>::Err: fmt::Display,
    {
        self.lookup_optional(&format_measurement(&n.to_string(), m), dep)
    }
}

struct NSKwParser<'a> {
    raw_keywords: &'a mut NonStdKeywords,
    deferred: PureErrorBuf,
}

impl<'a> NSKwParser<'a> {
    fn run<X, F>(kws: &'a mut NonStdKeywords, f: F) -> PureSuccess<X>
    where
        F: FnOnce(&mut Self) -> X,
    {
        let mut st = Self::from(kws);
        let data = f(&mut st);
        PureSuccess {
            data,
            deferred: st.collect(),
        }
    }

    fn try_convert_lookup_matrix<F, X, Y: FromStr>(
        &mut self,
        dopt: DefaultMatrix<Y>,
        spillover: OptionalKw<X>,
        ns: &[Shortname],
        which: &'static str,
        f: F,
    ) -> OptionalKw<Y>
    where
        F: FnOnce(X, &[Shortname]) -> Option<Y>,
        <Y as FromStr>::Err: fmt::Display,
    {
        let fallback = self.lookup_maybe(dopt.default);
        if dopt.try_convert {
            if let Some(s) = spillover.0 {
                return match f(s, ns) {
                    Some(c) => Some(c).into(),
                    None => {
                        self.deferred.push_warning(format!("{which} not full rank"));
                        fallback
                    }
                };
            }
        }
        fallback
    }

    fn try_convert_lookup_comp(
        &mut self,
        dopt: DefaultMatrix<Compensation>,
        spillover: OptionalKw<Spillover>,
        ns: &[Shortname],
    ) -> OptionalKw<Compensation> {
        self.try_convert_lookup_matrix(dopt, spillover, ns, SPILLOVER, spillover_to_comp)
    }

    fn try_convert_lookup_spillover(
        &mut self,
        dopt: DefaultMatrix<Spillover>,
        comp: OptionalKw<Compensation>,
        ns: &[Shortname],
    ) -> OptionalKw<Spillover> {
        self.try_convert_lookup_matrix(dopt, comp, ns, SPILLOVER, comp_to_spillover)
    }

    fn lookup_modification(&mut self, look: ModificationDefaults) -> ModificationData {
        ModificationData {
            last_modified: self.lookup_maybe(look.last_modified),
            last_modifier: self.lookup_maybe(look.last_modifier),
            originality: self.lookup_maybe(look.originality),
        }
    }

    fn lookup_plate(&mut self, look: PlateDefaults) -> PlateData {
        PlateData {
            plateid: self.lookup_maybe(look.plateid),
            platename: self.lookup_maybe(look.platename),
            wellid: self.lookup_maybe(look.wellid),
        }
    }

    fn lookup_unstained(&mut self, look: UnstainedDefaults) -> UnstainedData {
        UnstainedData {
            unstainedcenters: self.lookup_maybe(look.unstainedcenters),
            unstainedinfo: self.lookup_maybe(look.unstainedinfo),
        }
    }

    fn lookup_carrier(&mut self, look: CarrierDefaults) -> CarrierData {
        CarrierData {
            carrierid: self.lookup_maybe(look.carrierid),
            carriertype: self.lookup_maybe(look.carriertype),
            locationid: self.lookup_maybe(look.locationid),
        }
    }

    fn lookup_datetimes(&mut self, look: DatetimesDefaults) -> Datetimes {
        Datetimes {
            begin: self.lookup_maybe(look.begin),
            end: self.lookup_maybe(look.end),
        }
    }

    // auxiliary functions

    fn collect(self) -> PureErrorBuf {
        self.deferred
    }

    fn from(kws: &'a mut NonStdKeywords) -> Self {
        NSKwParser {
            raw_keywords: kws,
            deferred: PureErrorBuf::default(),
        }
    }

    fn lookup_meas_maybe<V: FromStr>(
        &mut self,
        n: usize,
        dopt: DefaultMeasOptional<V>,
    ) -> OptionalKw<V>
    where
        <V as FromStr>::Err: fmt::Display,
    {
        if let Some(d) = dopt.default {
            Some(d)
        } else {
            dopt.key
                .as_ref()
                .and_then(|kk| self.lookup_opt(&kk.from_index(n)))
        }
        .into()
    }

    fn lookup_maybe<V: FromStr>(&mut self, dopt: DefaultMetaOptional<V>) -> OptionalKw<V>
    where
        <V as FromStr>::Err: fmt::Display,
    {
        if let Some(d) = dopt.default {
            Some(d)
        } else {
            dopt.key.as_ref().and_then(|kk| self.lookup_opt(kk))
        }
        .into()
    }

    fn lookup_opt<V: FromStr>(&mut self, k: &NonStdKey) -> Option<V>
    where
        <V as FromStr>::Err: fmt::Display,
    {
        self.lookup(k).into()
    }

    fn lookup<V: FromStr>(&mut self, k: &NonStdKey) -> OptionalKw<V>
    where
        <V as FromStr>::Err: fmt::Display,
    {
        match self.raw_keywords.get(k) {
            Some(v) => match v.parse() {
                Err(w) => {
                    let msg = format!("{w} for key '{}' with value '{v}'", k.as_ref());
                    self.deferred.push_warning(msg);
                    None
                }
                Ok(x) => {
                    self.raw_keywords.remove(k);
                    Some(x)
                }
            },
            None => None,
        }
        .into()
    }
}

fn verify_delim(xs: &[u8], conf: &RawTextReadConfig) -> PureSuccess<u8> {
    // First character is the delimiter
    let delimiter: u8 = xs[0];

    // Check that it is a valid UTF8 character
    //
    // TODO we technically don't need this to be true in the case of double
    // delimiters, but this is non-standard anyways and probably rare
    let mut res = PureSuccess::from(delimiter);
    if String::from_utf8(vec![delimiter]).is_err() {
        res.push_error(format!(
            "Delimiter {delimiter} is not a valid utf8 character"
        ));
    }

    // Check that the delim is valid; this is technically only written in the
    // spec for 3.1+ but for older versions this should still be true since
    // these were ASCII-everywhere
    if !(1..=126).contains(&delimiter) {
        let msg = format!("Delimiter {delimiter} is not an ASCII character b/t 1-126");
        res.push_msg_leveled(msg, conf.force_ascii_delim);
    }
    res
}

type RawPairs = Vec<(String, String)>;

fn split_raw_text(xs: &[u8], delim: u8, conf: &RawTextReadConfig) -> PureSuccess<RawPairs> {
    let mut res = PureSuccess::from(vec![]);
    let textlen = xs.len();

    // Record delim positions
    let delim_positions: Vec<_> = xs
        .iter()
        .enumerate()
        .filter_map(|(i, c)| if *c == delim { Some(i) } else { None })
        .collect();

    // bail if we only have two positions
    if delim_positions.len() <= 2 {
        return res;
    }

    // Reduce position list to 'boundary list' which will be tuples of position
    // of a given delim and length until next delim.
    let raw_boundaries = delim_positions.windows(2).filter_map(|x| match x {
        [a, b] => Some((*a, b - a)),
        _ => None,
    });

    // Compute word boundaries depending on if we want to "escape" delims or
    // not. Technically all versions of the standard allow double delimiters to
    // be used in a word to represented a single delimiter. However, this means
    // we also can't have blank values. Many FCS files unfortunately use blank
    // values, so we need to be able to toggle this behavior.
    let boundaries = if conf.allow_double_delim {
        raw_boundaries.collect()
    } else {
        // Remove "escaped" delimiters from position vector. Because we disallow
        // blank values and also disallow delimiters at the start/end of words,
        // this implies that we should only see delimiters by themselves or in a
        // consecutive sequence whose length is even. Any odd-length'ed runs will
        // be treated as one delimiter if config permits
        let mut filtered_boundaries = vec![];
        for (key, chunk) in raw_boundaries.chunk_by(|(_, x)| *x).into_iter() {
            if key == 1 {
                if chunk.count() % 2 == 1 {
                    res.push_warning("delim at word boundary".to_string());
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
                let msg = format!("first key starts with a delim '{delim}'");
                res.push_error(msg);
            }
            if *xf + len < textlen - 1 {
                let msg = format!("final value ends with a delim '{delim}'");
                res.push_error(msg);
            }
        } else {
            return res;
        }
        filtered_boundaries
    };

    // Check that the last char is also a delim, if not file probably sketchy
    // ASSUME this will not fail since we have at least one delim by definition
    if !delim_positions.last().unwrap() == xs.len() - 1 {
        res.push_msg_leveled(
            "Last char is not a delimiter".to_string(),
            conf.enforce_final_delim,
        );
    }

    let delim2 = [delim, delim];
    let delim1 = [delim];
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
                    // TODO actually include keyword here
                    res.push_msg_leveled(
                        "keywords must be ASCII".to_string(),
                        conf.enforce_keyword_ascii,
                    )
                }
                // if delimiters were escaped, replace them here
                if conf.allow_double_delim {
                    // Test for empty values if we don't allow delim escaping;
                    // anything empty will either drop or produce an error
                    // depending on user settings
                    if v.is_empty() {
                        // TODO tell the user that this key will be dropped
                        let msg = format!("key {kupper} has a blank value");
                        res.push_msg_leveled(msg, conf.enforce_nonempty);
                    } else {
                        res.data.push((kupper.clone(), v.to_string()));
                    }
                } else {
                    let krep = kupper.replace(escape_from, escape_to);
                    let rrep = v.replace(escape_from, escape_to);
                    res.data.push((krep, rrep))
                };
            } else {
                let msg = "invalid UTF-8 byte encountered when parsing TEXT".to_string();
                res.push_msg_leveled(msg, conf.error_on_invalid_utf8)
            }
        } else {
            res.push_msg_leveled("number of words is not even".to_string(), conf.enforce_even)
        }
    }
    res
}

fn repair_keywords(kws: &mut RawKeywords, conf: &RawTextReadConfig) {
    for (key, v) in kws.iter_mut() {
        let k = key.as_str();
        if k == DATE {
            if let Some(pattern) = &conf.date_pattern {
                if let Ok(d) = NaiveDate::parse_from_str(v, pattern.as_ref()) {
                    *v = format!("{}", FCSDate(d))
                }
            }
        }
    }
}

fn hash_raw_pairs(
    pairs: Vec<(String, String)>,
    conf: &RawTextReadConfig,
) -> PureSuccess<RawKeywords> {
    let standard: HashMap<_, _> = HashMap::new();
    let mut res = PureSuccess::from(standard);
    // TODO filter keywords based on pattern somewhere here
    for (key, value) in pairs.into_iter() {
        let msg = format!("Skipping already-inserted key: {}", key.as_str());
        let ires = res.data.insert(key, value);
        if ires.is_some() {
            res.push_msg_leveled(msg, conf.enforce_unique);
        }
    }
    res
}

fn pad_zeros(s: &str) -> String {
    let len = s.len();
    let trimmed = s.trim_start();
    let newlen = trimmed.len();
    ("0").repeat(len - newlen) + trimmed
}

fn repair_offsets(pairs: &mut RawPairs, conf: &RawTextReadConfig) {
    if conf.repair_offset_spaces {
        for (key, v) in pairs.iter_mut() {
            if key == BEGINDATA
                || key == ENDDATA
                || key == BEGINSTEXT
                || key == ENDSTEXT
                || key == BEGINANALYSIS
                || key == ENDANALYSIS
                || key == NEXTDATA
            {
                *v = pad_zeros(v.as_str())
            }
        }
    }
}

fn lookup_stext_offsets(
    kws: &mut RawKeywords,
    version: Version,
    conf: &RawTextReadConfig,
) -> PureSuccess<Option<Segment>> {
    let offset_succ = KwParser::run(kws, conf.into(), |st| match version {
        Version::FCS2_0 => (None, None, false),
        Version::FCS3_0 | Version::FCS3_1 => {
            let b = st.lookup_beginstext_req();
            let e = st.lookup_endstext_req();
            (b, e, conf.enforce_stext)
        }
        Version::FCS3_2 => {
            let b = st.lookup_beginstext_opt().into();
            let e = st.lookup_endstext_opt().into();
            (b, e, false)
        }
    });
    offset_succ.and_then(|offsets| match offsets {
        (Some(begin), Some(end), enforce) => {
            let seg_res = Segment::try_new(begin, end, conf.stext, SegmentId::SupplementalText);
            let level = if enforce {
                PureErrorLevel::Error
            } else {
                PureErrorLevel::Warning
            };
            PureMaybe::from_result_1(seg_res, level)
        }
        _ => PureMaybe::empty(),
    })
}

fn add_keywords(
    kws: &mut RawKeywords,
    pairs: RawPairs,
    conf: &RawTextReadConfig,
) -> PureSuccess<()> {
    let mut succ = PureSuccess::from(());
    for (k, v) in pairs.into_iter() {
        let msg = format!(
            "Skipping already-inserted key from supplemental TEXT: {}",
            k.as_str()
        );
        if kws.insert(k, v).is_some() {
            succ.push_msg_leveled(msg, conf.enforce_unique);
        }
    }
    succ
}

fn h_read_raw_text_from_header<R: Read + Seek>(
    h: &mut BufReader<R>,
    header: &Header,
    conf: &RawTextReadConfig,
) -> ImpureResult<RawTEXT> {
    let mut buf = vec![];
    header.text.read(h, &mut buf)?;
    let mut kwconf = KwParserConfig::default();
    kwconf.disallow_deprecated = conf.disallow_deprecated;

    verify_delim(&buf, &conf).try_map(|delimiter| {
        let split_succ = split_raw_text(&buf, delimiter, &conf).and_then(|mut pairs| {
            repair_offsets(&mut pairs, conf);
            hash_raw_pairs(pairs, &conf)
        });
        let stext_succ = split_succ.try_map(|mut kws| {
            lookup_stext_offsets(&mut kws, header.version, &conf).try_map(|s| {
                let succ = if let Some(seg) = s {
                    buf.clear();
                    seg.read(h, &mut buf)?;
                    split_raw_text(&buf, delimiter, &conf)
                        .and_then(|pairs| add_keywords(&mut kws, pairs, &conf))
                } else {
                    PureSuccess::from(())
                };
                Ok(succ.map(|_| (kws, s)))
            })
        })?;
        Ok(stext_succ.and_then(|(mut kws, supp_text_seg)| {
            repair_keywords(&mut kws, &conf);
            // TODO this will throw an error if not present, but we may not care
            // so toggle b/t error and warning
            KwParser::run(&mut kws, kwconf, |st| st.lookup_nextdata()).map(|nextdata| RawTEXT {
                version: header.version,
                offsets: Offsets {
                    prim_text: header.text,
                    supp_text: supp_text_seg,
                    data: header.data,
                    analysis: header.analysis,
                    nextdata,
                },
                delimiter,
                keywords: kws,
            })
        }))
    })
}

fn h_read_raw_text<R: Read + Seek>(
    h: &mut BufReader<R>,
    conf: &RawTextReadConfig,
) -> ImpureResult<RawTEXT> {
    h_read_header(h, &conf.header)?.try_map(|header| h_read_raw_text_from_header(h, &header, conf))
}

fn split_remainder(xs: RawKeywords) -> (RawKeywords, RawKeywords) {
    xs.into_iter()
        .map(|(k, v)| {
            if k == TOT || k == BEGINDATA || k == ENDDATA || k == BEGINANALYSIS || k == ENDANALYSIS
            {
                Ok((k, v))
            } else {
                Err((k, v))
            }
        })
        .partition_result()
}

fn raw_to_std(raw: RawTEXT, conf: &StdTextReadConfig) -> PureResult<StandardizedTEXT> {
    let mut kws = raw.keywords;
    parse_raw_text(raw.version, &mut kws, &conf).map(|std_succ| {
        std_succ.map({
            |standardized| {
                let (remainder, deviant) = split_remainder(kws);
                StandardizedTEXT {
                    offsets: raw.offsets,
                    standardized,
                    delimiter: raw.delimiter,
                    remainder,
                    deviant,
                }
            }
        })
    })
}

/// Return header in an FCS file.
///
/// The header contains the version and offsets for the TEXT, DATA, and ANALYSIS
/// segments, all of which are present in fixed byte offset segments. This
/// function will fail and return an error if the file does not follow this
/// structure. Will also check that the begin and end segments are not reversed.
///
/// Depending on the version, all of these except the TEXT offsets might be 0
/// which indicates they are actually stored in TEXT due to size limitations.
pub fn read_fcs_header(p: &path::PathBuf, conf: &HeaderConfig) -> ImpureResult<Header> {
    let file = fs::File::options().read(true).open(p)?;
    let mut reader = BufReader::new(file);
    h_read_header(&mut reader, conf)
}

/// Return header and raw key/value metadata pairs in an FCS file.
///
/// First will parse the header according to [`read_fcs_header`]. If this fails
/// an error will be returned.
///
/// Next will use the offset information in the header to parse the TEXT segment
/// for key/value pairs. On success will return these pairs as-is using Strings
/// in a HashMap. No other processing will be performed.
pub fn read_fcs_raw_text(p: &path::PathBuf, conf: &RawTextReadConfig) -> ImpureResult<RawTEXT> {
    let file = fs::File::options().read(true).open(p)?;
    let mut h = BufReader::new(file);
    h_read_raw_text(&mut h, conf)
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
pub fn read_fcs_std_text(
    p: &path::PathBuf,
    conf: &StdTextReadConfig,
) -> ImpureResult<StandardizedTEXT> {
    let raw_succ = read_fcs_raw_text(p, &conf.raw)?;
    let out = raw_succ.try_map(|raw| raw_to_std(raw, conf))?;
    Ok(out)
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
pub fn read_fcs_file(
    p: &path::PathBuf,
    conf: &DataReadConfig,
) -> ImpureResult<StandardizedDataset> {
    let file = fs::File::options().read(true).open(p)?;
    let mut h = BufReader::new(file);
    h_read_raw_text(&mut h, &conf.standard.raw)?
        .try_map(|raw| raw_to_std(raw, &conf.standard))?
        .try_map(|std| h_read_std_dataset(&mut h, std, conf))
}

// fn read_fcs_file_2_0(p: path::PathBuf, conf: Reader) -> FCSResult<TEXT2_0>;
// fn read_fcs_file_3_0(p: path::PathBuf, conf: Reader) -> FCSResult<TEXT3_0>;
// fn read_fcs_file_3_1(p: path::PathBuf, conf: Reader) -> FCSResult<TEXT3_1>;
// fn read_fcs_file_3_2(p: path::PathBuf, conf: Reader) -> FCSResult<TEXT3_2>;

// /// Return header, raw metadata, and data in an FCS file.
// ///
// /// In contrast to [`read_fcs_file`], this will return the keywords as a flat
// /// list of key/value pairs. Only the bare minimum of these will be read in
// /// order to determine how to parse the DATA segment (including $DATATYPE,
// /// $BYTEORD, etc). No other checks will be performed to ensure the metadata
// /// conforms to the FCS standard version indicated in the header.
// ///
// /// This might be useful for applications where one does not necessarily need
// /// the strict structure of the standardized metadata, or if one does not care
// /// too much about the degree to which the metadata conforms to standard.
// ///
// /// Other than this, behavior is identical to [`read_fcs_file`],
// pub fn read_fcs_raw_file(p: path::PathBuf, conf: Reader) -> io::Result<FCSResult<()>> {
//     let file = fs::File::options().read(true).open(p)?;
//     let mut reader = BufReader::new(file);
//     let header = read_header(&mut reader)?;
//     let raw = read_raw_text(&mut reader, &header, &conf.text.raw)?;
//     // TODO need to modify this so it doesn't do the crazy version checking
//     // stuff we don't actually want in this case
//     match parse_raw_text(header.clone(), raw.clone(), &conf.text) {
//         Ok(std) => {
//             let data = read_data(&mut reader, std.data_parser).unwrap();
//             Ok(Ok(FCSSuccess {
//                 header,
//                 raw,
//                 std: (),
//                 data,
//             }))
//         }
//         Err(e) => Ok(Err(e)),
//     }
// }

impl From<FCSTime> for FCSTime60 {
    fn from(value: FCSTime) -> Self {
        FCSTime60(value.0)
    }
}

impl From<FCSTime> for FCSTime100 {
    fn from(value: FCSTime) -> Self {
        FCSTime100(value.0)
    }
}

impl From<FCSTime60> for FCSTime {
    fn from(value: FCSTime60) -> Self {
        // ASSUME this will never fail, we are just removing nanoseconds
        FCSTime(value.0.with_nanosecond(0).unwrap())
    }
}

impl From<FCSTime100> for FCSTime {
    fn from(value: FCSTime100) -> Self {
        // ASSUME this will never fail, we are just removing nanoseconds
        FCSTime(value.0.with_nanosecond(0).unwrap())
    }
}

impl From<FCSTime60> for FCSTime100 {
    fn from(value: FCSTime60) -> Self {
        FCSTime100(value.0)
    }
}

impl From<FCSTime100> for FCSTime60 {
    fn from(value: FCSTime100) -> Self {
        FCSTime60(value.0)
    }
}

struct FromByteOrdError;

impl TryFrom<ByteOrd> for Endian {
    type Error = FromByteOrdError;

    fn try_from(value: ByteOrd) -> Result<Self, Self::Error> {
        match value {
            ByteOrd::Endian(e) => Ok(e),
            _ => Err(FromByteOrdError),
        }
    }
}

impl From<Wavelength> for Wavelengths {
    fn from(value: Wavelength) -> Self {
        Wavelengths(vec![value.0])
    }
}

impl From<OptionalKw<Wavelengths>> for OptionalKw<Wavelength> {
    fn from(value: OptionalKw<Wavelengths>) -> Self {
        value
            .0
            .map(|x| x.0)
            .unwrap_or_default()
            .first()
            .copied()
            .map(|x| x.into())
            .into()
    }
}

impl From<Calibration3_1> for Calibration3_2 {
    fn from(value: Calibration3_1) -> Self {
        Calibration3_2 {
            unit: value.unit,
            offset: 0.0,
            value: value.value,
        }
    }
}

impl From<Calibration3_2> for Calibration3_1 {
    fn from(value: Calibration3_2) -> Self {
        Calibration3_1 {
            unit: value.unit,
            value: value.value,
        }
    }
}

impl From<Endian> for ByteOrd {
    fn from(value: Endian) -> ByteOrd {
        ByteOrd::Endian(value)
    }
}

pub trait IntoMeasurement<T, Y>: Sized {
    type DefaultsXToY: From<Y>;
    type Req;

    fn defaults(r: Self::Req) -> Self::DefaultsXToY;

    fn convert(
        m: Measurement<Self>,
        n: usize,
        def: Self::DefaultsXToY,
    ) -> PureSuccess<Measurement<T>> {
        let mut m = m;
        Self::convert_inner(m.specific, n, def, &mut m.nonstandard_keywords).map(|specific| {
            Measurement {
                bytes: m.bytes,
                range: m.range,
                longname: m.longname,
                detector_type: m.detector_type,
                detector_voltage: m.detector_voltage,
                filter: m.filter,
                power: m.power,
                percent_emitted: m.percent_emitted,
                nonstandard_keywords: m.nonstandard_keywords,
                specific,
            }
        })
    }

    fn convert_inner(
        self,
        n: usize,
        def: Self::DefaultsXToY,
        ns: &mut NonStdKeywords,
    ) -> PureSuccess<T>;
}

pub trait IntoMetadata<T, D>: Sized {
    type DefaultsXToY: From<D>;
    type Req;

    fn defaults(r: Self::Req) -> Self::DefaultsXToY;

    fn convert(
        m: Metadata<Self>,
        def: Self::DefaultsXToY,
        ms: &[Shortname],
    ) -> PureSuccess<Metadata<T>> {
        let mut m = m;
        Self::convert_inner(m.specific, def, &mut m.nonstandard_keywords, ms).map(|specific| {
            // TODO this seems silly, break struct up into common bits
            Metadata {
                abrt: m.abrt,
                cells: m.cells,
                com: m.com,
                datatype: m.datatype,
                exp: m.exp,
                fil: m.fil,
                inst: m.inst,
                lost: m.lost,
                op: m.op,
                proj: m.proj,
                smno: m.smno,
                sys: m.sys,
                src: m.src,
                tr: m.tr,
                nonstandard_keywords: m.nonstandard_keywords,
                specific,
            }
        })
    }

    fn convert_inner(
        self,
        def: Self::DefaultsXToY,
        ns: &mut NonStdKeywords,
        ms: &[Shortname],
    ) -> PureSuccess<T>;
}

pub trait IntoCore<ToM, ToP, DM, DP>: Sized
where
    ToM: VersionedMetadata,
    ToP: VersionedMeasurement,
    Self::FromM: VersionedMetadata,
    Self::FromP: VersionedMeasurement,
    <Self::FromM as IntoMetadata<ToM, DM>>::DefaultsXToY: From<DM>,
    <Self::FromP as IntoMeasurement<ToP, DP>>::DefaultsXToY: From<DP>,
{
    type FromM: IntoMetadata<ToM, DM>;
    type FromP: IntoMeasurement<ToP, DP>;

    fn defaults(
        req: CoreDefaults<
            <Self::FromM as IntoMetadata<ToM, DM>>::Req,
            <Self::FromP as IntoMeasurement<ToP, DP>>::Req,
        >,
    ) -> CoreDefaults<
        <Self::FromM as IntoMetadata<ToM, DM>>::DefaultsXToY,
        <Self::FromP as IntoMeasurement<ToP, DP>>::DefaultsXToY,
    > {
        let metadata = <Self::FromM as IntoMetadata<ToM, DM>>::defaults(req.metadata);
        let measurements = req
            .measurements
            .into_iter()
            .map(<Self::FromP as IntoMeasurement<ToP, DP>>::defaults)
            .collect();
        CoreDefaults {
            metadata,
            measurements,
        }
    }

    fn convert_core_def(
        c: CoreTEXT<Self::FromM, Self::FromP>,
        def: CoreDefaults<
            <Self::FromM as IntoMetadata<ToM, DM>>::DefaultsXToY,
            <Self::FromP as IntoMeasurement<ToP, DP>>::DefaultsXToY,
        >,
    ) -> PureSuccess<CoreTEXT<ToM, ToP>> {
        let metadata = c.metadata;
        // ASSUME measurement defaults is same length as measurements
        let ms = c
            .measurements
            .into_iter()
            .zip(def.measurements)
            .enumerate()
            .map(|(i, (m, d))| Self::FromP::convert(m, i, d))
            .collect();
        PureSuccess::sequence(ms).and_then(|measurements| {
            let ms = measurement_shortnames(&measurements);
            Self::FromM::convert(metadata, def.metadata, &ms).map(|metadata| CoreTEXT {
                measurements,
                metadata,
            })
        })
    }

    // TODO not DRY, trying to put this in terms of the above function results
    // in a compile-time recursion loop, for reasons that elude me
    fn convert_core(
        c: CoreTEXT<Self::FromM, Self::FromP>,
        def: CoreDefaults<DM, DP>,
    ) -> PureSuccess<CoreTEXT<ToM, ToP>> {
        let metadata = c.metadata;
        // ASSUME measurement defaults is same length as measurements
        let ms = c
            .measurements
            .into_iter()
            .zip(def.measurements)
            .enumerate()
            .map(|(i, (m, d))| Self::FromP::convert(m, i, d.into()))
            .collect();
        PureSuccess::sequence(ms).and_then(|measurements| {
            let ms = measurement_shortnames(&measurements);
            Self::FromM::convert(metadata, def.metadata.into(), &ms).map(|metadata| CoreTEXT {
                measurements,
                metadata,
            })
        })
    }
}

fn measurement_shortnames<P: VersionedMeasurement>(ms: &[Measurement<P>]) -> Vec<Shortname> {
    ms.iter()
        .enumerate()
        .map(|(i, p)| P::shortname(p, i))
        .collect()
}

impl<M, P>
    IntoCore<InnerMetadata2_0, InnerMeasurement2_0, MetadataDefaultsTo2_0, MeasurementDefaultsTo2_0>
    for CoreTEXT<M, P>
where
    M: VersionedMetadata,
    P: VersionedMeasurement,
    M: IntoMetadata<InnerMetadata2_0, MetadataDefaultsTo2_0>,
    P: IntoMeasurement<InnerMeasurement2_0, MeasurementDefaultsTo2_0>,
{
    type FromM = M;
    type FromP = P;
}

impl<M, P>
    IntoCore<InnerMetadata3_0, InnerMeasurement3_0, MetadataDefaultsTo3_0, MeasurementDefaultsTo3_0>
    for CoreTEXT<M, P>
where
    M: VersionedMetadata,
    P: VersionedMeasurement,
    M: IntoMetadata<InnerMetadata3_0, MetadataDefaultsTo3_0>,
    P: IntoMeasurement<InnerMeasurement3_0, MeasurementDefaultsTo3_0>,
{
    type FromM = M;
    type FromP = P;
}

impl<M, P>
    IntoCore<InnerMetadata3_1, InnerMeasurement3_1, MetadataDefaultsTo3_1, MeasurementDefaultsTo3_1>
    for CoreTEXT<M, P>
where
    M: VersionedMetadata,
    P: VersionedMeasurement,
    M: IntoMetadata<InnerMetadata3_1, MetadataDefaultsTo3_1>,
    P: IntoMeasurement<InnerMeasurement3_1, MeasurementDefaultsTo3_1>,
{
    type FromM = M;
    type FromP = P;
}

impl<M, P>
    IntoCore<InnerMetadata3_2, InnerMeasurement3_2, MetadataDefaultsTo3_2, MeasurementDefaultsTo3_2>
    for CoreTEXT<M, P>
where
    M: VersionedMetadata,
    P: VersionedMeasurement,
    M: IntoMetadata<InnerMetadata3_2, MetadataDefaultsTo3_2>,
    P: IntoMeasurement<InnerMeasurement3_2, MeasurementDefaultsTo3_2>,
{
    type FromM = M;
    type FromP = P;
}

trait ConvertConfig {
    type Req;

    fn lookup(r: Self::Req) -> Self;
}

#[derive(Default)]
pub struct ModificationDefaults {
    last_modified: DefaultMetaOptional<ModifiedDateTime>,
    last_modifier: DefaultMetaOptional<LastModifier>,
    originality: DefaultMetaOptional<Originality>,
}

impl ModificationDefaults {
    fn keyed() -> Self {
        Self {
            last_modified: DefaultMetaOptional::init_unchecked(NS_LAST_MODIFIED),
            last_modifier: DefaultMetaOptional::init_unchecked(NS_LAST_MODIFIER),
            originality: DefaultMetaOptional::init_unchecked(NS_ORIGINALITY),
        }
    }
}

#[derive(Default)]
pub struct PlateDefaults {
    plateid: DefaultMetaOptional<Plateid>,
    platename: DefaultMetaOptional<Platename>,
    wellid: DefaultMetaOptional<Wellid>,
}

impl PlateDefaults {
    fn keyed() -> Self {
        Self {
            plateid: DefaultMetaOptional::init_unchecked(NS_PLATEID),
            platename: DefaultMetaOptional::init_unchecked(NS_PLATENAME),
            wellid: DefaultMetaOptional::init_unchecked(NS_WELLID),
        }
    }
}

#[derive(Default)]
pub struct CarrierDefaults {
    carrierid: DefaultMetaOptional<Carrierid>,
    carriertype: DefaultMetaOptional<Carriertype>,
    locationid: DefaultMetaOptional<Locationid>,
}

impl CarrierDefaults {
    fn keyed() -> Self {
        Self {
            carrierid: DefaultMetaOptional::init_unchecked(NS_CARRIERID),
            carriertype: DefaultMetaOptional::init_unchecked(NS_CARRIERTYPE),
            locationid: DefaultMetaOptional::init_unchecked(NS_LOCATIONID),
        }
    }
}

#[derive(Default)]
pub struct UnstainedDefaults {
    unstainedcenters: DefaultMetaOptional<UnstainedCenters>,
    unstainedinfo: DefaultMetaOptional<UnstainedInfo>,
}

impl UnstainedDefaults {
    fn keyed() -> Self {
        Self {
            unstainedcenters: DefaultMetaOptional::init_unchecked(NS_UNSTAINEDCENTERS),
            unstainedinfo: DefaultMetaOptional::init_unchecked(NS_UNSTAINEDINFO),
        }
    }
}

#[derive(Default)]
pub struct DatetimesDefaults {
    begin: DefaultMetaOptional<FCSDateTime>,
    end: DefaultMetaOptional<FCSDateTime>,
}

impl DatetimesDefaults {
    fn keyed() -> Self {
        Self {
            begin: DefaultMetaOptional::init_unchecked(NS_BEGINDATETIME),
            end: DefaultMetaOptional::init_unchecked(NS_ENDDATETIME),
        }
    }
}

pub struct MetadataDefaults2_0To2_0;

pub struct MetadataDefaults3_0To2_0;

#[derive(Default)]
pub struct MetadataDefaults3_1To2_0 {
    // TODO there is no sensible keyword in 2.0 that would correspond to this
    comp: DefaultMatrix<Compensation>,
}

#[derive(Default)]
pub struct MetadataDefaults3_2To2_0 {
    // TODO ditto
    comp: DefaultMatrix<Compensation>,
}

pub struct MetadataDefaults3_0To3_0;

#[derive(Default)]
pub struct MetadataDefaults2_0To3_0 {
    cytsn: DefaultMetaOptional<Cytsn>,
    timestep: DefaultMetaOptional<Timestep>,
    unicode: DefaultMetaOptional<Unicode>,
}

impl MetadataDefaults2_0To3_0 {
    fn keyed() -> Self {
        Self {
            cytsn: DefaultMetaOptional::init_unchecked(NS_CYTSN),
            timestep: DefaultMetaOptional::init_unchecked(NS_TIMESTEP),
            unicode: DefaultMetaOptional::init_unchecked(NS_UNICODE),
        }
    }
}

#[derive(Default)]
pub struct MetadataDefaults3_1To3_0 {
    comp: DefaultMatrix<Compensation>,
    unicode: DefaultMetaOptional<Unicode>,
}

impl MetadataDefaults3_1To3_0 {
    fn keyed() -> Self {
        Self {
            comp: DefaultMatrix::init_unchecked(NS_COMP),
            unicode: DefaultMetaOptional::init_unchecked(NS_UNICODE),
        }
    }
}

#[derive(Default)]
pub struct MetadataDefaults3_2To3_0 {
    comp: DefaultMatrix<Compensation>,
    unicode: DefaultMetaOptional<Unicode>,
}

impl MetadataDefaults3_2To3_0 {
    fn keyed() -> Self {
        Self {
            comp: DefaultMatrix::init_unchecked(NS_COMP),
            unicode: DefaultMetaOptional::init_unchecked(NS_UNICODE),
        }
    }
}

pub struct MetadataDefaults3_1To3_1;

pub struct MetadataDefaults2_0To3_1 {
    endian: Endian,
    cytsn: DefaultMetaOptional<Cytsn>,
    timestep: DefaultMetaOptional<Timestep>,
    vol: DefaultMetaOptional<Vol>,
    spillover: DefaultMatrix<Spillover>,
    modification: ModificationDefaults,
    plate: PlateDefaults,
}

impl MetadataDefaults2_0To3_1 {
    pub fn new(endian: Endian) -> Self {
        MetadataDefaults2_0To3_1 {
            endian,
            cytsn: DefaultMetaOptional::default(),
            timestep: DefaultMetaOptional::default(),
            vol: DefaultMetaOptional::default(),
            spillover: DefaultMatrix::default(),
            modification: ModificationDefaults::default(),
            plate: PlateDefaults::default(),
        }
    }

    fn keyed(endian: Endian) -> Self {
        Self {
            endian,
            cytsn: DefaultMetaOptional::init_unchecked(NS_CYTSN),
            timestep: DefaultMetaOptional::init_unchecked(NS_TIMESTEP),
            vol: DefaultMetaOptional::init_unchecked(NS_VOL),
            spillover: DefaultMatrix::init_unchecked(NS_SPILLOVER),
            modification: ModificationDefaults::keyed(),
            plate: PlateDefaults::keyed(),
        }
    }
}

pub struct MetadataDefaults3_0To3_1 {
    endian: Endian,
    vol: DefaultMetaOptional<Vol>,
    spillover: DefaultMatrix<Spillover>,
    modification: ModificationDefaults,
    plate: PlateDefaults,
}

impl MetadataDefaults3_0To3_1 {
    pub fn new(endian: Endian) -> Self {
        MetadataDefaults3_0To3_1 {
            endian,
            vol: DefaultMetaOptional::default(),
            spillover: DefaultMatrix::default(),
            modification: ModificationDefaults::default(),
            plate: PlateDefaults::default(),
        }
    }

    fn keyed(endian: Endian) -> Self {
        Self {
            endian,
            vol: DefaultMetaOptional::init_unchecked(NS_VOL),
            spillover: DefaultMatrix::init_unchecked(NS_SPILLOVER),
            modification: ModificationDefaults::keyed(),
            plate: PlateDefaults::keyed(),
        }
    }
}

pub struct MetadataDefaults3_2To3_1;

pub struct MetadataDefaults3_2To3_2;

pub struct MetadataDefaults2_0To3_2 {
    endian: Endian,
    cyt: Cyt,
    cytsn: DefaultMetaOptional<Cytsn>,
    timestep: DefaultMetaOptional<Timestep>,
    vol: DefaultMetaOptional<Vol>,
    spillover: DefaultMatrix<Spillover>,
    flowrate: DefaultMetaOptional<Flowrate>,
    modification: ModificationDefaults,
    plate: PlateDefaults,
    unstained: UnstainedDefaults,
    carrier: CarrierDefaults,
    datetimes: DatetimesDefaults,
}

impl MetadataDefaults2_0To3_2 {
    pub fn new(endian: Endian, cyt: Cyt) -> Self {
        MetadataDefaults2_0To3_2 {
            endian,
            cyt,
            cytsn: DefaultMetaOptional::default(),
            timestep: DefaultMetaOptional::default(),
            vol: DefaultMetaOptional::default(),
            spillover: DefaultMatrix::default(),
            flowrate: DefaultMetaOptional::default(),
            modification: ModificationDefaults::default(),
            plate: PlateDefaults::default(),
            unstained: UnstainedDefaults::default(),
            carrier: CarrierDefaults::default(),
            datetimes: DatetimesDefaults::default(),
        }
    }

    fn keyed(endian: Endian, cyt: Cyt) -> Self {
        Self {
            endian,
            cyt,
            cytsn: DefaultMetaOptional::init_unchecked(NS_CYTSN),
            timestep: DefaultMetaOptional::init_unchecked(NS_TIMESTEP),
            flowrate: DefaultMetaOptional::init_unchecked(NS_FLOWRATE),
            vol: DefaultMetaOptional::init_unchecked(NS_VOL),
            spillover: DefaultMatrix::init_unchecked(NS_SPILLOVER),
            modification: ModificationDefaults::keyed(),
            plate: PlateDefaults::keyed(),
            unstained: UnstainedDefaults::keyed(),
            carrier: CarrierDefaults::keyed(),
            datetimes: DatetimesDefaults::keyed(),
        }
    }
}

pub struct MetadataDefaults3_0To3_2 {
    endian: Endian,
    cyt: Cyt,
    vol: DefaultMetaOptional<Vol>,
    spillover: DefaultMatrix<Spillover>,
    flowrate: DefaultMetaOptional<Flowrate>,
    modification: ModificationDefaults,
    plate: PlateDefaults,
    unstained: UnstainedDefaults,
    carrier: CarrierDefaults,
    datetimes: DatetimesDefaults,
}

impl MetadataDefaults3_0To3_2 {
    pub fn new(endian: Endian, cyt: Cyt) -> Self {
        MetadataDefaults3_0To3_2 {
            endian,
            cyt,
            vol: DefaultMetaOptional::default(),
            spillover: DefaultMatrix::default(),
            flowrate: DefaultMetaOptional::default(),
            modification: ModificationDefaults::default(),
            plate: PlateDefaults::default(),
            unstained: UnstainedDefaults::default(),
            carrier: CarrierDefaults::default(),
            datetimes: DatetimesDefaults::default(),
        }
    }

    fn keyed(endian: Endian, cyt: Cyt) -> Self {
        Self {
            endian,
            cyt,
            flowrate: DefaultMetaOptional::init_unchecked(NS_FLOWRATE),
            vol: DefaultMetaOptional::init_unchecked(NS_VOL),
            spillover: DefaultMatrix::init_unchecked(NS_SPILLOVER),
            modification: ModificationDefaults::keyed(),
            plate: PlateDefaults::keyed(),
            unstained: UnstainedDefaults::keyed(),
            carrier: CarrierDefaults::keyed(),
            datetimes: DatetimesDefaults::keyed(),
        }
    }
}

pub struct MetadataDefaults3_1To3_2 {
    cyt: Cyt,
    flowrate: DefaultMetaOptional<Flowrate>,
    unstained: UnstainedDefaults,
    carrier: CarrierDefaults,
    datetimes: DatetimesDefaults,
}

impl MetadataDefaults3_1To3_2 {
    pub fn new(cyt: Cyt) -> Self {
        MetadataDefaults3_1To3_2 {
            cyt,
            flowrate: DefaultMetaOptional::default(),
            unstained: UnstainedDefaults::default(),
            carrier: CarrierDefaults::default(),
            datetimes: DatetimesDefaults::default(),
        }
    }

    fn keyed(cyt: Cyt) -> Self {
        Self {
            cyt,
            flowrate: DefaultMetaOptional::init_unchecked(NS_FLOWRATE),
            unstained: UnstainedDefaults::keyed(),
            carrier: CarrierDefaults::keyed(),
            datetimes: DatetimesDefaults::keyed(),
        }
    }
}

#[derive(Default)]
pub struct MetadataDefaultsTo2_0 {
    // TODO no sensible kw
    comp: DefaultMatrix<Compensation>,
}

#[derive(Default)]
pub struct MetadataDefaultsTo3_0 {
    cytsn: DefaultMetaOptional<Cytsn>,
    timestep: DefaultMetaOptional<Timestep>,
    unicode: DefaultMetaOptional<Unicode>,
    comp: DefaultMatrix<Compensation>,
}

impl MetadataDefaultsTo3_0 {
    fn new() -> Self {
        Self {
            cytsn: DefaultMetaOptional::default(),
            timestep: DefaultMetaOptional::default(),
            unicode: DefaultMetaOptional::default(),
            comp: DefaultMatrix::default(),
        }
    }

    fn keyed() -> Self {
        Self {
            cytsn: DefaultMetaOptional::init_unchecked(NS_CYTSN),
            timestep: DefaultMetaOptional::init_unchecked(NS_TIMESTEP),
            unicode: DefaultMetaOptional::init_unchecked(NS_UNICODE),
            comp: DefaultMatrix::init_unchecked(NS_COMP),
        }
    }
}

pub struct MetadataDefaultsTo3_1 {
    endian: Endian,
    cytsn: DefaultMetaOptional<Cytsn>,
    timestep: DefaultMetaOptional<Timestep>,
    vol: DefaultMetaOptional<Vol>,
    spillover: DefaultMatrix<Spillover>,
    modification: ModificationDefaults,
    plate: PlateDefaults,
}

impl MetadataDefaultsTo3_1 {
    pub fn new(endian: Endian) -> Self {
        Self {
            endian,
            cytsn: DefaultMetaOptional::default(),
            timestep: DefaultMetaOptional::default(),
            vol: DefaultMetaOptional::default(),
            spillover: DefaultMatrix::default(),
            modification: ModificationDefaults::default(),
            plate: PlateDefaults::default(),
        }
    }

    fn keyed(endian: Endian) -> Self {
        Self {
            endian,
            cytsn: DefaultMetaOptional::init_unchecked(NS_CYTSN),
            timestep: DefaultMetaOptional::init_unchecked(NS_TIMESTEP),
            vol: DefaultMetaOptional::init_unchecked(NS_VOL),
            spillover: DefaultMatrix::init_unchecked(NS_SPILLOVER),
            modification: ModificationDefaults::keyed(),
            plate: PlateDefaults::keyed(),
        }
    }
}

pub struct MetadataDefaultsTo3_2 {
    endian: Endian,
    cyt: Cyt,
    cytsn: DefaultMetaOptional<Cytsn>,
    timestep: DefaultMetaOptional<Timestep>,
    vol: DefaultMetaOptional<Vol>,
    spillover: DefaultMatrix<Spillover>,
    flowrate: DefaultMetaOptional<Flowrate>,
    modification: ModificationDefaults,
    plate: PlateDefaults,
    unstained: UnstainedDefaults,
    carrier: CarrierDefaults,
    datetimes: DatetimesDefaults,
}

impl MetadataDefaultsTo3_2 {
    pub fn new(endian: Endian, cyt: Cyt) -> Self {
        Self {
            endian,
            cyt,
            cytsn: DefaultMetaOptional::default(),
            timestep: DefaultMetaOptional::default(),
            vol: DefaultMetaOptional::default(),
            spillover: DefaultMatrix::default(),
            flowrate: DefaultMetaOptional::default(),
            modification: ModificationDefaults::default(),
            plate: PlateDefaults::default(),
            unstained: UnstainedDefaults::default(),
            carrier: CarrierDefaults::default(),
            datetimes: DatetimesDefaults::default(),
        }
    }

    fn keyed(endian: Endian, cyt: Cyt) -> Self {
        Self {
            endian,
            cyt,
            cytsn: DefaultMetaOptional::init_unchecked(NS_CYTSN),
            timestep: DefaultMetaOptional::init_unchecked(NS_TIMESTEP),
            flowrate: DefaultMetaOptional::init_unchecked(NS_FLOWRATE),
            vol: DefaultMetaOptional::init_unchecked(NS_VOL),
            spillover: DefaultMatrix::init_unchecked(NS_SPILLOVER),
            modification: ModificationDefaults::keyed(),
            plate: PlateDefaults::keyed(),
            unstained: UnstainedDefaults::keyed(),
            carrier: CarrierDefaults::keyed(),
            datetimes: DatetimesDefaults::keyed(),
        }
    }
}

pub struct RequiredEndianCyt {
    pub endian: Endian,
    pub cyt: Cyt,
}

macro_rules! txfr_keys {
    ($from:ident, $to:ident, [$($key:ident),*]) => {
        impl From<$from> for $to {
            fn from(_value: $from) -> Self {
                $to {
                    $(
                        $key: _value.$key,
                    )*
                }
            }
        }
    };
}

txfr_keys!(MetadataDefaultsTo2_0, MetadataDefaults2_0To2_0, []);

txfr_keys!(MetadataDefaultsTo2_0, MetadataDefaults3_0To2_0, []);

txfr_keys!(MetadataDefaultsTo2_0, MetadataDefaults3_1To2_0, [comp]);

txfr_keys!(MetadataDefaultsTo2_0, MetadataDefaults3_2To2_0, [comp]);

txfr_keys!(MetadataDefaultsTo3_0, MetadataDefaults3_0To3_0, []);

txfr_keys!(
    MetadataDefaultsTo3_0,
    MetadataDefaults2_0To3_0,
    [cytsn, timestep, unicode]
);

txfr_keys!(
    MetadataDefaultsTo3_0,
    MetadataDefaults3_1To3_0,
    [comp, unicode]
);

txfr_keys!(
    MetadataDefaultsTo3_0,
    MetadataDefaults3_2To3_0,
    [comp, unicode]
);

txfr_keys!(MetadataDefaultsTo3_1, MetadataDefaults3_1To3_1, []);

txfr_keys!(
    MetadataDefaultsTo3_1,
    MetadataDefaults2_0To3_1,
    [endian, cytsn, timestep, vol, spillover, modification, plate]
);

txfr_keys!(
    MetadataDefaultsTo3_1,
    MetadataDefaults3_0To3_1,
    [endian, vol, spillover, modification, plate]
);

txfr_keys!(MetadataDefaultsTo3_1, MetadataDefaults3_2To3_1, []);

txfr_keys!(MetadataDefaultsTo3_2, MetadataDefaults3_2To3_2, []);

txfr_keys!(
    MetadataDefaultsTo3_2,
    MetadataDefaults2_0To3_2,
    [
        endian,
        cyt,
        flowrate,
        cytsn,
        timestep,
        vol,
        spillover,
        modification,
        plate,
        unstained,
        carrier,
        datetimes
    ]
);

txfr_keys!(
    MetadataDefaultsTo3_2,
    MetadataDefaults3_0To3_2,
    [
        endian,
        cyt,
        flowrate,
        vol,
        spillover,
        modification,
        plate,
        unstained,
        carrier,
        datetimes
    ]
);

txfr_keys!(
    MetadataDefaultsTo3_2,
    MetadataDefaults3_1To3_2,
    [cyt, flowrate, unstained, carrier, datetimes]
);

#[derive(Clone)]
pub struct MeasurementDefaults2_0To2_0;

#[derive(Clone)]
pub struct MeasurementDefaults3_0To2_0;

#[derive(Clone)]
pub struct MeasurementDefaults3_1To2_0;

#[derive(Clone)]
pub struct MeasurementDefaults3_2To2_0;

#[derive(Clone)]
pub struct MeasurementDefaultsTo2_0;

#[derive(Clone)]
pub struct MeasurementDefaults3_0To3_0;

#[derive(Clone)]
pub struct MeasurementDefaults2_0To3_0 {
    scale: Scale,
    gain: DefaultMeasOptional<Gain>,
}

impl MeasurementDefaults2_0To3_0 {
    fn new(scale: Scale) -> Self {
        Self {
            scale,
            gain: DefaultMeasOptional::default(),
        }
    }

    fn keyed(scale: Scale, n: usize) -> Self {
        Self {
            scale,
            gain: DefaultMeasOptional::init_unchecked(GAIN_SFX, n),
        }
    }
}

#[derive(Clone)]
pub struct MeasurementDefaults3_1To3_0;

#[derive(Clone)]
pub struct MeasurementDefaults3_2To3_0;

#[derive(Clone)]
pub struct MeasurementDefaultsTo3_0 {
    scale: Scale,
    gain: DefaultMeasOptional<Gain>,
}

impl MeasurementDefaultsTo3_0 {
    fn new(scale: Scale) -> Self {
        Self {
            scale,
            gain: DefaultMeasOptional::default(),
        }
    }

    fn keyed(scale: Scale, n: usize) -> Self {
        Self {
            scale,
            gain: DefaultMeasOptional::init_unchecked(GAIN_SFX, n),
        }
    }
}

#[derive(Clone)]
pub struct MeasurementDefaults3_1To3_1;

#[derive(Clone)]
pub struct MeasurementDefaults2_0To3_1 {
    scale: Scale,
    shortname: Shortname,
    gain: DefaultMeasOptional<Gain>,
    calibration: DefaultMeasOptional<Calibration3_1>,
    display: DefaultMeasOptional<Display>,
}

pub struct RequireScaleShortname {
    scale: Scale,
    shortname: Shortname,
}

impl MeasurementDefaults2_0To3_1 {
    fn new(scale: Scale, shortname: Shortname) -> Self {
        Self {
            scale,
            shortname,
            gain: DefaultMeasOptional::default(),
            calibration: DefaultMeasOptional::default(),
            display: DefaultMeasOptional::default(),
        }
    }

    fn keyed(scale: Scale, shortname: Shortname, n: usize) -> Self {
        Self {
            scale,
            shortname,
            gain: DefaultMeasOptional::init_unchecked(GAIN_SFX, n),
            calibration: DefaultMeasOptional::init_unchecked(CALIBRATION_SFX, n),
            display: DefaultMeasOptional::init_unchecked(DISPLAY_SFX, n),
        }
    }
}

#[derive(Clone)]
pub struct MeasurementDefaults3_0To3_1 {
    shortname: Shortname,
    calibration: DefaultMeasOptional<Calibration3_1>,
    display: DefaultMeasOptional<Display>,
}

impl MeasurementDefaults3_0To3_1 {
    fn new(shortname: Shortname) -> Self {
        Self {
            shortname,
            calibration: DefaultMeasOptional::default(),
            display: DefaultMeasOptional::default(),
        }
    }

    fn keyed(shortname: Shortname, n: usize) -> Self {
        Self {
            shortname,
            calibration: DefaultMeasOptional::init_unchecked(CALIBRATION_SFX, n),
            display: DefaultMeasOptional::init_unchecked(DISPLAY_SFX, n),
        }
    }
}

#[derive(Clone)]
pub struct MeasurementDefaults3_2To3_1;

#[derive(Clone)]
pub struct MeasurementDefaultsTo3_1 {
    scale: Scale,
    shortname: Shortname,
    gain: DefaultMeasOptional<Gain>,
    calibration: DefaultMeasOptional<Calibration3_1>,
    display: DefaultMeasOptional<Display>,
}

impl MeasurementDefaultsTo3_1 {
    fn new(scale: Scale, shortname: Shortname) -> Self {
        Self {
            scale,
            shortname,
            gain: DefaultMeasOptional::default(),
            calibration: DefaultMeasOptional::default(),
            display: DefaultMeasOptional::default(),
        }
    }

    fn keyed(scale: Scale, shortname: Shortname, n: usize) -> Self {
        Self {
            scale,
            shortname,
            gain: DefaultMeasOptional::init_unchecked(GAIN_SFX, n),
            calibration: DefaultMeasOptional::init_unchecked(CALIBRATION_SFX, n),
            display: DefaultMeasOptional::init_unchecked(DISPLAY_SFX, n),
        }
    }
}

#[derive(Clone)]
pub struct MeasurementDefaults3_2To3_2;

#[derive(Clone)]
pub struct MeasurementDefaults2_0To3_2 {
    scale: Scale,
    shortname: Shortname,
    gain: DefaultMeasOptional<Gain>,
    calibration: DefaultMeasOptional<Calibration3_2>,
    display: DefaultMeasOptional<Display>,
    analyte: DefaultMeasOptional<Analyte>,
    tag: DefaultMeasOptional<Tag>,
    detector_name: DefaultMeasOptional<DetectorName>,
    feature: DefaultMeasOptional<Feature>,
    datatype: DefaultMeasOptional<NumType>,
    measurement_type: DefaultMeasOptional<MeasurementType>,
}

impl MeasurementDefaults2_0To3_2 {
    fn new(scale: Scale, shortname: Shortname) -> Self {
        Self {
            scale,
            shortname,
            gain: DefaultMeasOptional::default(),
            calibration: DefaultMeasOptional::default(),
            display: DefaultMeasOptional::default(),
            analyte: DefaultMeasOptional::default(),
            tag: DefaultMeasOptional::default(),
            detector_name: DefaultMeasOptional::default(),
            feature: DefaultMeasOptional::default(),
            datatype: DefaultMeasOptional::default(),
            measurement_type: DefaultMeasOptional::default(),
        }
    }

    fn fixed(scale: Scale, shortname: Shortname, n: usize) -> Self {
        Self {
            scale,
            shortname,
            gain: DefaultMeasOptional::init_unchecked(GAIN_SFX, n),
            calibration: DefaultMeasOptional::init_unchecked(CALIBRATION_SFX, n),
            display: DefaultMeasOptional::init_unchecked(DISPLAY_SFX, n),
            analyte: DefaultMeasOptional::init_unchecked(ANALYTE_SFX, n),
            tag: DefaultMeasOptional::init_unchecked(TAG_SFX, n),
            detector_name: DefaultMeasOptional::init_unchecked(DET_NAME_SFX, n),
            feature: DefaultMeasOptional::init_unchecked(FEATURE_SFX, n),
            datatype: DefaultMeasOptional::init_unchecked(DATATYPE_SFX, n),
            measurement_type: DefaultMeasOptional::init_unchecked(MEAS_TYPE_SFX, n),
        }
    }
}

#[derive(Clone)]
pub struct MeasurementDefaults3_0To3_2 {
    shortname: Shortname,
    calibration: DefaultMeasOptional<Calibration3_2>,
    display: DefaultMeasOptional<Display>,
    analyte: DefaultMeasOptional<Analyte>,
    tag: DefaultMeasOptional<Tag>,
    detector_name: DefaultMeasOptional<DetectorName>,
    feature: DefaultMeasOptional<Feature>,
    datatype: DefaultMeasOptional<NumType>,
    measurement_type: DefaultMeasOptional<MeasurementType>,
}

impl MeasurementDefaults3_0To3_2 {
    fn new(shortname: Shortname) -> Self {
        Self {
            shortname,
            calibration: DefaultMeasOptional::default(),
            display: DefaultMeasOptional::default(),
            analyte: DefaultMeasOptional::default(),
            tag: DefaultMeasOptional::default(),
            detector_name: DefaultMeasOptional::default(),
            feature: DefaultMeasOptional::default(),
            datatype: DefaultMeasOptional::default(),
            measurement_type: DefaultMeasOptional::default(),
        }
    }

    fn fixed(shortname: Shortname, n: usize) -> Self {
        Self {
            shortname,
            calibration: DefaultMeasOptional::init_unchecked(CALIBRATION_SFX, n),
            display: DefaultMeasOptional::init_unchecked(DISPLAY_SFX, n),
            analyte: DefaultMeasOptional::init_unchecked(ANALYTE_SFX, n),
            tag: DefaultMeasOptional::init_unchecked(TAG_SFX, n),
            detector_name: DefaultMeasOptional::init_unchecked(DET_NAME_SFX, n),
            feature: DefaultMeasOptional::init_unchecked(FEATURE_SFX, n),
            datatype: DefaultMeasOptional::init_unchecked(DATATYPE_SFX, n),
            measurement_type: DefaultMeasOptional::init_unchecked(MEAS_TYPE_SFX, n),
        }
    }
}

#[derive(Clone, Default)]
pub struct MeasurementDefaults3_1To3_2 {
    analyte: DefaultMeasOptional<Analyte>,
    tag: DefaultMeasOptional<Tag>,
    detector_name: DefaultMeasOptional<DetectorName>,
    feature: DefaultMeasOptional<Feature>,
    datatype: DefaultMeasOptional<NumType>,
    measurement_type: DefaultMeasOptional<MeasurementType>,
}

impl MeasurementDefaults3_1To3_2 {
    fn fixed(n: usize) -> Self {
        Self {
            analyte: DefaultMeasOptional::init_unchecked(ANALYTE_SFX, n),
            tag: DefaultMeasOptional::init_unchecked(TAG_SFX, n),
            detector_name: DefaultMeasOptional::init_unchecked(DET_NAME_SFX, n),
            feature: DefaultMeasOptional::init_unchecked(FEATURE_SFX, n),
            datatype: DefaultMeasOptional::init_unchecked(DATATYPE_SFX, n),
            measurement_type: DefaultMeasOptional::init_unchecked(MEAS_TYPE_SFX, n),
        }
    }
}

#[derive(Clone)]
pub struct MeasurementDefaultsTo3_2 {
    scale: Scale,
    shortname: Shortname,
    gain: DefaultMeasOptional<Gain>,
    calibration: DefaultMeasOptional<Calibration3_2>,
    display: DefaultMeasOptional<Display>,
    analyte: DefaultMeasOptional<Analyte>,
    tag: DefaultMeasOptional<Tag>,
    detector_name: DefaultMeasOptional<DetectorName>,
    feature: DefaultMeasOptional<Feature>,
    datatype: DefaultMeasOptional<NumType>,
    measurement_type: DefaultMeasOptional<MeasurementType>,
}

impl MeasurementDefaultsTo3_2 {
    fn new(scale: Scale, shortname: Shortname) -> Self {
        Self {
            scale,
            shortname,
            gain: DefaultMeasOptional::default(),
            calibration: DefaultMeasOptional::default(),
            display: DefaultMeasOptional::default(),
            analyte: DefaultMeasOptional::default(),
            tag: DefaultMeasOptional::default(),
            detector_name: DefaultMeasOptional::default(),
            feature: DefaultMeasOptional::default(),
            datatype: DefaultMeasOptional::default(),
            measurement_type: DefaultMeasOptional::default(),
        }
    }

    // TODO lots of this boilerplate could be cleaned up by wrapping each
    // kw value in a newtype (for those that aren't unique) and then
    // assigning a trait which (among other things) would have a const param
    // for the key/suffix/default. This way I would only need to write each
    // kw once and could refer to them using the type.
    fn fixed(scale: Scale, shortname: Shortname, n: usize) -> Self {
        Self {
            scale,
            shortname,
            gain: DefaultMeasOptional::init_unchecked(GAIN_SFX, n),
            calibration: DefaultMeasOptional::init_unchecked(CALIBRATION_SFX, n),
            display: DefaultMeasOptional::init_unchecked(DISPLAY_SFX, n),
            analyte: DefaultMeasOptional::init_unchecked(ANALYTE_SFX, n),
            tag: DefaultMeasOptional::init_unchecked(TAG_SFX, n),
            detector_name: DefaultMeasOptional::init_unchecked(DET_NAME_SFX, n),
            feature: DefaultMeasOptional::init_unchecked(FEATURE_SFX, n),
            datatype: DefaultMeasOptional::init_unchecked(DATATYPE_SFX, n),
            measurement_type: DefaultMeasOptional::init_unchecked(MEAS_TYPE_SFX, n),
        }
    }
}

txfr_keys!(MeasurementDefaultsTo2_0, MeasurementDefaults2_0To2_0, []);

txfr_keys!(MeasurementDefaultsTo2_0, MeasurementDefaults3_0To2_0, []);

txfr_keys!(MeasurementDefaultsTo2_0, MeasurementDefaults3_1To2_0, []);

txfr_keys!(MeasurementDefaultsTo2_0, MeasurementDefaults3_2To2_0, []);

txfr_keys!(
    MeasurementDefaultsTo3_0,
    MeasurementDefaults2_0To3_0,
    [gain, scale]
);

txfr_keys!(MeasurementDefaultsTo3_0, MeasurementDefaults3_0To3_0, []);

txfr_keys!(MeasurementDefaultsTo3_0, MeasurementDefaults3_1To3_0, []);

txfr_keys!(MeasurementDefaultsTo3_0, MeasurementDefaults3_2To3_0, []);

txfr_keys!(
    MeasurementDefaultsTo3_1,
    MeasurementDefaults2_0To3_1,
    [scale, shortname, gain, calibration, display]
);

txfr_keys!(
    MeasurementDefaultsTo3_1,
    MeasurementDefaults3_0To3_1,
    [shortname, calibration, display]
);

txfr_keys!(MeasurementDefaultsTo3_1, MeasurementDefaults3_1To3_1, []);

txfr_keys!(MeasurementDefaultsTo3_1, MeasurementDefaults3_2To3_1, []);

txfr_keys!(
    MeasurementDefaultsTo3_2,
    MeasurementDefaults2_0To3_2,
    [
        scale,
        shortname,
        gain,
        calibration,
        display,
        analyte,
        tag,
        detector_name,
        feature,
        datatype,
        measurement_type
    ]
);

txfr_keys!(
    MeasurementDefaultsTo3_2,
    MeasurementDefaults3_0To3_2,
    [
        shortname,
        calibration,
        display,
        analyte,
        tag,
        detector_name,
        feature,
        datatype,
        measurement_type
    ]
);

txfr_keys!(
    MeasurementDefaultsTo3_2,
    MeasurementDefaults3_1To3_2,
    [
        analyte,
        tag,
        detector_name,
        feature,
        datatype,
        measurement_type
    ]
);

txfr_keys!(MeasurementDefaultsTo3_2, MeasurementDefaults3_2To3_2, []);

pub struct CoreDefaults<X, Y> {
    metadata: X,
    measurements: Vec<Y>,
}

impl<X, Y: Clone> CoreDefaults<X, Y> {
    pub fn new(metadata: X, measurement: Y, par: usize) -> Self {
        CoreDefaults {
            metadata,
            measurements: iter::repeat_n(measurement, par).collect(),
        }
    }
}

pub type CoreDefaults3_0To2_0 = CoreDefaults<MetadataDefaults3_0To2_0, MeasurementDefaults3_0To2_0>;
pub type CoreDefaults3_1To2_0 = CoreDefaults<MetadataDefaults3_1To2_0, MeasurementDefaults3_1To2_0>;
pub type CoreDefaults3_2To2_0 = CoreDefaults<MetadataDefaults3_2To2_0, MeasurementDefaults3_2To2_0>;

pub type CoreDefaults2_0To3_0 = CoreDefaults<MetadataDefaults2_0To3_0, MeasurementDefaults2_0To3_0>;
pub type CoreDefaults3_1To3_0 = CoreDefaults<MetadataDefaults3_1To3_0, MeasurementDefaults3_1To3_0>;
pub type CoreDefaults3_2To3_0 = CoreDefaults<MetadataDefaults3_2To3_0, MeasurementDefaults3_2To3_0>;

pub type CoreDefaults2_0To3_1 = CoreDefaults<MetadataDefaults2_0To3_1, MeasurementDefaults2_0To3_1>;
pub type CoreDefaults3_0To3_1 = CoreDefaults<MetadataDefaults3_0To3_1, MeasurementDefaults3_0To3_1>;
pub type CoreDefaults3_2To3_1 = CoreDefaults<MetadataDefaults3_2To3_1, MeasurementDefaults3_2To3_1>;

pub type CoreDefaults2_0To3_2 = CoreDefaults<MetadataDefaults2_0To3_2, MeasurementDefaults2_0To3_2>;
pub type CoreDefaults3_0To3_2 = CoreDefaults<MetadataDefaults3_0To3_2, MeasurementDefaults3_0To3_2>;
pub type CoreDefaults3_1To3_2 = CoreDefaults<MetadataDefaults3_1To3_2, MeasurementDefaults3_1To3_2>;

pub type CoreDefaultsTo2_0 = CoreDefaults<MetadataDefaultsTo2_0, MeasurementDefaultsTo2_0>;
pub type CoreDefaultsTo3_0 = CoreDefaults<MetadataDefaultsTo3_0, MeasurementDefaultsTo3_0>;
pub type CoreDefaultsTo3_1 = CoreDefaults<MetadataDefaultsTo3_1, MeasurementDefaultsTo3_1>;
pub type CoreDefaultsTo3_2 = CoreDefaults<MetadataDefaultsTo3_2, MeasurementDefaultsTo3_2>;

fn project_defaults<A, B, X, Y>(value: CoreDefaults<A, B>) -> CoreDefaults<X, Y>
where
    A: Into<X>,
    B: Into<Y>,
{
    CoreDefaults {
        metadata: value.metadata.into(),
        measurements: value.measurements.into_iter().map(|m| m.into()).collect(),
    }
}

impl IntoMeasurement<InnerMeasurement2_0, MeasurementDefaultsTo2_0> for InnerMeasurement2_0 {
    type DefaultsXToY = MeasurementDefaults2_0To2_0;
    type Req = ();

    fn defaults(_: Self::Req) -> Self::DefaultsXToY {
        MeasurementDefaults2_0To2_0
    }

    fn convert_inner(
        self,
        _: usize,
        _: Self::DefaultsXToY,
        _: &mut NonStdKeywords,
    ) -> PureSuccess<InnerMeasurement2_0> {
        PureSuccess::from(self)
    }
}

impl IntoMeasurement<InnerMeasurement2_0, MeasurementDefaultsTo2_0> for InnerMeasurement3_0 {
    type DefaultsXToY = MeasurementDefaults3_0To2_0;
    type Req = ();

    fn defaults(_: Self::Req) -> Self::DefaultsXToY {
        MeasurementDefaults3_0To2_0
    }

    fn convert_inner(
        self,
        _: usize,
        _: Self::DefaultsXToY,
        _: &mut NonStdKeywords,
    ) -> PureSuccess<InnerMeasurement2_0> {
        PureSuccess::from(InnerMeasurement2_0 {
            scale: Some(self.scale).into(),
            shortname: self.shortname,
            wavelength: self.wavelength,
        })
    }
}

impl IntoMeasurement<InnerMeasurement2_0, MeasurementDefaultsTo2_0> for InnerMeasurement3_1 {
    type DefaultsXToY = MeasurementDefaults3_1To2_0;
    type Req = ();

    fn defaults(_: Self::Req) -> Self::DefaultsXToY {
        MeasurementDefaults3_1To2_0
    }

    fn convert_inner(
        self,
        _: usize,
        _: Self::DefaultsXToY,
        _: &mut NonStdKeywords,
    ) -> PureSuccess<InnerMeasurement2_0> {
        PureSuccess::from(InnerMeasurement2_0 {
            shortname: Some(self.shortname).into(),
            scale: Some(self.scale).into(),
            wavelength: self.wavelengths.into(),
        })
    }
}

impl IntoMeasurement<InnerMeasurement2_0, MeasurementDefaultsTo2_0> for InnerMeasurement3_2 {
    type DefaultsXToY = MeasurementDefaults3_2To2_0;
    type Req = ();

    fn defaults(_: Self::Req) -> Self::DefaultsXToY {
        MeasurementDefaults3_2To2_0
    }

    fn convert_inner(
        self,
        _: usize,
        _: Self::DefaultsXToY,
        _: &mut NonStdKeywords,
    ) -> PureSuccess<InnerMeasurement2_0> {
        PureSuccess::from(InnerMeasurement2_0 {
            shortname: Some(self.shortname).into(),
            scale: Some(self.scale).into(),
            wavelength: self.wavelengths.into(),
        })
    }
}

impl IntoMeasurement<InnerMeasurement3_0, MeasurementDefaultsTo3_0> for InnerMeasurement2_0 {
    type DefaultsXToY = MeasurementDefaults2_0To3_0;
    type Req = Scale;

    fn defaults(scale: Self::Req) -> Self::DefaultsXToY {
        MeasurementDefaults2_0To3_0::new(scale)
    }

    fn convert_inner(
        self,
        n: usize,
        def: Self::DefaultsXToY,
        ns: &mut NonStdKeywords,
    ) -> PureSuccess<InnerMeasurement3_0> {
        let scale = self.scale.0.unwrap_or(def.scale);
        NSKwParser::run(ns, |st| InnerMeasurement3_0 {
            scale,
            wavelength: self.wavelength,
            shortname: self.shortname,
            gain: st.lookup_meas_maybe(n, def.gain),
        })
    }
}

impl IntoMeasurement<InnerMeasurement3_0, MeasurementDefaultsTo3_0> for InnerMeasurement3_0 {
    type DefaultsXToY = MeasurementDefaults3_0To3_0;
    type Req = ();

    fn defaults(_: Self::Req) -> Self::DefaultsXToY {
        MeasurementDefaults3_0To3_0
    }

    fn convert_inner(
        self,
        _: usize,
        _: Self::DefaultsXToY,
        _: &mut NonStdKeywords,
    ) -> PureSuccess<InnerMeasurement3_0> {
        PureSuccess::from(self)
    }
}

impl IntoMeasurement<InnerMeasurement3_0, MeasurementDefaultsTo3_0> for InnerMeasurement3_1 {
    type DefaultsXToY = MeasurementDefaults3_1To3_0;
    type Req = ();

    fn defaults(_: Self::Req) -> Self::DefaultsXToY {
        MeasurementDefaults3_1To3_0
    }

    fn convert_inner(
        self,
        _: usize,
        _: Self::DefaultsXToY,
        _: &mut NonStdKeywords,
    ) -> PureSuccess<InnerMeasurement3_0> {
        PureSuccess::from(InnerMeasurement3_0 {
            shortname: Some(self.shortname).into(),
            scale: self.scale,
            gain: self.gain,
            wavelength: self.wavelengths.into(),
        })
    }
}

impl IntoMeasurement<InnerMeasurement3_0, MeasurementDefaultsTo3_0> for InnerMeasurement3_2 {
    type DefaultsXToY = MeasurementDefaults3_2To3_0;
    type Req = ();

    fn defaults(_: Self::Req) -> Self::DefaultsXToY {
        MeasurementDefaults3_2To3_0
    }

    fn convert_inner(
        self,
        _: usize,
        _: Self::DefaultsXToY,
        _: &mut NonStdKeywords,
    ) -> PureSuccess<InnerMeasurement3_0> {
        PureSuccess::from(InnerMeasurement3_0 {
            shortname: Some(self.shortname).into(),
            scale: self.scale,
            gain: self.gain,
            wavelength: self.wavelengths.into(),
        })
    }
}

impl IntoMeasurement<InnerMeasurement3_1, MeasurementDefaultsTo3_1> for InnerMeasurement2_0 {
    type DefaultsXToY = MeasurementDefaults2_0To3_1;
    type Req = RequireScaleShortname;

    fn defaults(r: Self::Req) -> Self::DefaultsXToY {
        MeasurementDefaults2_0To3_1::new(r.scale, r.shortname)
    }

    fn convert_inner(
        self,
        n: usize,
        def: Self::DefaultsXToY,
        ns: &mut NonStdKeywords,
    ) -> PureSuccess<InnerMeasurement3_1> {
        let scale = self.scale.0.unwrap_or(def.scale);
        let shortname = self.shortname.0.unwrap_or(def.shortname);
        NSKwParser::run(ns, |st| InnerMeasurement3_1 {
            scale,
            shortname,
            wavelengths: self.wavelength.map(|x| x.into()),
            gain: st.lookup_meas_maybe(n, def.gain),
            calibration: st.lookup_meas_maybe(n, def.calibration),
            display: st.lookup_meas_maybe(n, def.display),
        })
    }
}

impl IntoMeasurement<InnerMeasurement3_1, MeasurementDefaultsTo3_1> for InnerMeasurement3_0 {
    type DefaultsXToY = MeasurementDefaults3_0To3_1;
    type Req = Shortname;

    fn defaults(shortname: Self::Req) -> Self::DefaultsXToY {
        MeasurementDefaults3_0To3_1::new(shortname)
    }

    fn convert_inner(
        self,
        n: usize,
        def: Self::DefaultsXToY,
        ns: &mut NonStdKeywords,
    ) -> PureSuccess<InnerMeasurement3_1> {
        let shortname = self.shortname.0.unwrap_or(def.shortname);
        NSKwParser::run(ns, |st| InnerMeasurement3_1 {
            shortname,
            scale: self.scale,
            gain: self.gain,
            wavelengths: self.wavelength.map(|x| x.into()),
            calibration: st.lookup_meas_maybe(n, def.calibration),
            display: st.lookup_meas_maybe(n, def.display),
        })
    }
}

impl IntoMeasurement<InnerMeasurement3_1, MeasurementDefaultsTo3_1> for InnerMeasurement3_1 {
    type DefaultsXToY = MeasurementDefaults3_1To3_1;
    type Req = ();

    fn defaults(_: Self::Req) -> Self::DefaultsXToY {
        MeasurementDefaults3_1To3_1
    }

    fn convert_inner(
        self,
        _: usize,
        _: Self::DefaultsXToY,
        _: &mut NonStdKeywords,
    ) -> PureSuccess<InnerMeasurement3_1> {
        PureSuccess::from(self)
    }
}

impl IntoMeasurement<InnerMeasurement3_1, MeasurementDefaultsTo3_1> for InnerMeasurement3_2 {
    type DefaultsXToY = MeasurementDefaults3_2To3_1;
    type Req = ();

    fn defaults(_: Self::Req) -> Self::DefaultsXToY {
        MeasurementDefaults3_2To3_1
    }

    fn convert_inner(
        self,
        _: usize,
        _: Self::DefaultsXToY,
        _: &mut NonStdKeywords,
    ) -> PureSuccess<InnerMeasurement3_1> {
        PureSuccess::from(InnerMeasurement3_1 {
            shortname: self.shortname,
            scale: self.scale,
            gain: self.gain,
            wavelengths: self.wavelengths,
            calibration: self.calibration.map(|x| x.into()),
            display: self.display,
        })
    }
}

impl IntoMeasurement<InnerMeasurement3_2, MeasurementDefaultsTo3_2> for InnerMeasurement2_0 {
    type DefaultsXToY = MeasurementDefaults2_0To3_2;
    type Req = RequireScaleShortname;

    fn defaults(r: Self::Req) -> Self::DefaultsXToY {
        MeasurementDefaults2_0To3_2::new(r.scale, r.shortname)
    }

    fn convert_inner(
        self,
        n: usize,
        def: Self::DefaultsXToY,
        ns: &mut NonStdKeywords,
    ) -> PureSuccess<InnerMeasurement3_2> {
        let scale = self.scale.0.unwrap_or(def.scale);
        let shortname = self.shortname.0.unwrap_or(def.shortname);
        NSKwParser::run(ns, |st| InnerMeasurement3_2 {
            scale,
            shortname,
            wavelengths: self.wavelength.map(|x| x.into()),
            gain: st.lookup_meas_maybe(n, def.gain),
            calibration: st.lookup_meas_maybe(n, def.calibration),
            display: st.lookup_meas_maybe(n, def.display),
            analyte: st.lookup_meas_maybe(n, def.analyte),
            feature: st.lookup_meas_maybe(n, def.feature),
            tag: st.lookup_meas_maybe(n, def.tag),
            detector_name: st.lookup_meas_maybe(n, def.detector_name),
            datatype: st.lookup_meas_maybe(n, def.datatype),
            measurement_type: st.lookup_meas_maybe(n, def.measurement_type),
        })
    }
}

impl IntoMeasurement<InnerMeasurement3_2, MeasurementDefaultsTo3_2> for InnerMeasurement3_0 {
    type DefaultsXToY = MeasurementDefaults3_0To3_2;
    type Req = Shortname;

    fn defaults(shortname: Self::Req) -> Self::DefaultsXToY {
        MeasurementDefaults3_0To3_2::new(shortname)
    }

    fn convert_inner(
        self,
        n: usize,
        def: Self::DefaultsXToY,
        ns: &mut NonStdKeywords,
    ) -> PureSuccess<InnerMeasurement3_2> {
        let shortname = self.shortname.0.unwrap_or(def.shortname);
        NSKwParser::run(ns, |st| InnerMeasurement3_2 {
            shortname,
            scale: self.scale,
            wavelengths: self.wavelength.map(|x| x.into()),
            gain: self.gain,
            calibration: st.lookup_meas_maybe(n, def.calibration),
            display: st.lookup_meas_maybe(n, def.display),
            analyte: st.lookup_meas_maybe(n, def.analyte),
            feature: st.lookup_meas_maybe(n, def.feature),
            tag: st.lookup_meas_maybe(n, def.tag),
            detector_name: st.lookup_meas_maybe(n, def.detector_name),
            datatype: st.lookup_meas_maybe(n, def.datatype),
            measurement_type: st.lookup_meas_maybe(n, def.measurement_type),
        })
    }
}

impl IntoMeasurement<InnerMeasurement3_2, MeasurementDefaultsTo3_2> for InnerMeasurement3_1 {
    type DefaultsXToY = MeasurementDefaults3_1To3_2;
    type Req = ();

    fn defaults(_: Self::Req) -> Self::DefaultsXToY {
        MeasurementDefaults3_1To3_2::default()
    }

    fn convert_inner(
        self,
        n: usize,
        def: Self::DefaultsXToY,
        ns: &mut NonStdKeywords,
    ) -> PureSuccess<InnerMeasurement3_2> {
        NSKwParser::run(ns, |st| InnerMeasurement3_2 {
            shortname: self.shortname,
            scale: self.scale,
            wavelengths: self.wavelengths,
            gain: self.gain,
            calibration: self.calibration.map(|x| x.into()),
            display: self.display,
            analyte: st.lookup_meas_maybe(n, def.analyte),
            feature: st.lookup_meas_maybe(n, def.feature),
            tag: st.lookup_meas_maybe(n, def.tag),
            detector_name: st.lookup_meas_maybe(n, def.detector_name),
            datatype: st.lookup_meas_maybe(n, def.datatype),
            measurement_type: st.lookup_meas_maybe(n, def.measurement_type),
        })
    }
}

impl IntoMeasurement<InnerMeasurement3_2, MeasurementDefaultsTo3_2> for InnerMeasurement3_2 {
    type DefaultsXToY = MeasurementDefaults3_2To3_2;
    type Req = ();

    fn defaults(_: Self::Req) -> Self::DefaultsXToY {
        MeasurementDefaults3_2To3_2
    }

    fn convert_inner(
        self,
        _: usize,
        _: Self::DefaultsXToY,
        _: &mut NonStdKeywords,
    ) -> PureSuccess<InnerMeasurement3_2> {
        PureSuccess::from(self)
    }
}

impl IntoMetadata<InnerMetadata2_0, MetadataDefaultsTo2_0> for InnerMetadata2_0 {
    type DefaultsXToY = MetadataDefaults2_0To2_0;
    type Req = ();

    fn defaults(_: Self::Req) -> Self::DefaultsXToY {
        MetadataDefaults2_0To2_0
    }

    fn convert_inner(
        self,
        _: Self::DefaultsXToY,
        _: &mut NonStdKeywords,
        _: &[Shortname],
    ) -> PureSuccess<InnerMetadata2_0> {
        PureSuccess::from(self)
    }
}

impl IntoMetadata<InnerMetadata2_0, MetadataDefaultsTo2_0> for InnerMetadata3_0 {
    type DefaultsXToY = MetadataDefaults3_0To2_0;
    type Req = ();

    fn defaults(_: Self::Req) -> Self::DefaultsXToY {
        MetadataDefaults3_0To2_0
    }

    fn convert_inner(
        self,
        _: Self::DefaultsXToY,
        _: &mut NonStdKeywords,
        _: &[Shortname],
    ) -> PureSuccess<InnerMetadata2_0> {
        PureSuccess::from(InnerMetadata2_0 {
            mode: self.mode,
            byteord: self.byteord,
            cyt: self.cyt,
            comp: self.comp,
            timestamps: self.timestamps.map(|d| d.into()),
        })
    }
}

impl IntoMetadata<InnerMetadata2_0, MetadataDefaultsTo2_0> for InnerMetadata3_1 {
    type DefaultsXToY = MetadataDefaults3_1To2_0;
    type Req = ();

    fn defaults(_: Self::Req) -> Self::DefaultsXToY {
        MetadataDefaults3_1To2_0::default()
    }

    fn convert_inner(
        self,
        def: Self::DefaultsXToY,
        ns: &mut NonStdKeywords,
        ms: &[Shortname],
    ) -> PureSuccess<InnerMetadata2_0> {
        NSKwParser::run(ns, |st| InnerMetadata2_0 {
            mode: self.mode,
            byteord: self.byteord.into(),
            cyt: self.cyt,
            comp: st.try_convert_lookup_comp(def.comp, self.spillover, ms),
            timestamps: self.timestamps.map(|d| d.into()),
        })
    }
}

impl IntoMetadata<InnerMetadata2_0, MetadataDefaultsTo2_0> for InnerMetadata3_2 {
    type DefaultsXToY = MetadataDefaults3_2To2_0;
    type Req = ();

    fn defaults(_: Self::Req) -> Self::DefaultsXToY {
        MetadataDefaults3_2To2_0::default()
    }

    fn convert_inner(
        self,
        def: Self::DefaultsXToY,
        ns: &mut NonStdKeywords,
        ms: &[Shortname],
    ) -> PureSuccess<InnerMetadata2_0> {
        NSKwParser::run(ns, |st| InnerMetadata2_0 {
            mode: Mode::List,
            byteord: self.byteord.into(),
            cyt: Some(self.cyt).into(),
            comp: st.try_convert_lookup_comp(def.comp, self.spillover, ms),
            timestamps: self.timestamps.map(|d| d.into()),
        })
    }
}

impl IntoMetadata<InnerMetadata3_0, MetadataDefaultsTo3_0> for InnerMetadata2_0 {
    type DefaultsXToY = MetadataDefaults2_0To3_0;
    type Req = ();

    fn defaults(_: Self::Req) -> Self::DefaultsXToY {
        MetadataDefaults2_0To3_0::default()
    }

    fn convert_inner(
        self,
        def: Self::DefaultsXToY,
        ns: &mut NonStdKeywords,
        _: &[Shortname],
    ) -> PureSuccess<InnerMetadata3_0> {
        NSKwParser::run(ns, |st| InnerMetadata3_0 {
            mode: self.mode,
            byteord: self.byteord,
            cyt: self.cyt,
            comp: self.comp,
            timestamps: self.timestamps.map(|d| d.into()),
            cytsn: st.lookup_maybe(def.cytsn),
            timestep: st.lookup_maybe(def.timestep),
            unicode: st.lookup_maybe(def.unicode),
        })
    }
}

impl IntoMetadata<InnerMetadata3_0, MetadataDefaultsTo3_0> for InnerMetadata3_0 {
    type DefaultsXToY = MetadataDefaults3_0To3_0;
    type Req = ();

    fn defaults(_: Self::Req) -> Self::DefaultsXToY {
        MetadataDefaults3_0To3_0
    }

    fn convert_inner(
        self,
        _: Self::DefaultsXToY,
        _: &mut NonStdKeywords,
        _: &[Shortname],
    ) -> PureSuccess<InnerMetadata3_0> {
        PureSuccess::from(self)
    }
}

impl IntoMetadata<InnerMetadata3_0, MetadataDefaultsTo3_0> for InnerMetadata3_1 {
    type DefaultsXToY = MetadataDefaults3_1To3_0;
    type Req = ();

    fn defaults(_: Self::Req) -> Self::DefaultsXToY {
        MetadataDefaults3_1To3_0::default()
    }

    fn convert_inner(
        self,
        def: Self::DefaultsXToY,
        ns: &mut NonStdKeywords,
        ms: &[Shortname],
    ) -> PureSuccess<InnerMetadata3_0> {
        NSKwParser::run(ns, |st| InnerMetadata3_0 {
            mode: self.mode,
            byteord: self.byteord.into(),
            cyt: self.cyt,
            cytsn: self.cytsn,
            timestep: self.timestep,
            timestamps: self.timestamps.map(|d| d.into()),
            comp: st.try_convert_lookup_comp(def.comp, self.spillover, ms),
            unicode: st.lookup_maybe(def.unicode),
        })
    }
}

impl IntoMetadata<InnerMetadata3_0, MetadataDefaultsTo3_0> for InnerMetadata3_2 {
    type DefaultsXToY = MetadataDefaults3_2To3_0;
    type Req = ();

    fn defaults(_: Self::Req) -> Self::DefaultsXToY {
        MetadataDefaults3_2To3_0::default()
    }

    fn convert_inner(
        self,
        def: Self::DefaultsXToY,
        ns: &mut NonStdKeywords,
        ms: &[Shortname],
    ) -> PureSuccess<InnerMetadata3_0> {
        NSKwParser::run(ns, |st| InnerMetadata3_0 {
            mode: Mode::List,
            byteord: self.byteord.into(),
            cyt: Some(self.cyt).into(),
            cytsn: self.cytsn,
            timestep: self.timestep,
            timestamps: self.timestamps.map(|d| d.into()),
            comp: st.try_convert_lookup_comp(def.comp, self.spillover, ms),
            unicode: st.lookup_maybe(def.unicode),
        })
    }
}

impl IntoMetadata<InnerMetadata3_1, MetadataDefaultsTo3_1> for InnerMetadata2_0 {
    type DefaultsXToY = MetadataDefaults2_0To3_1;
    type Req = Endian;

    fn defaults(endian: Self::Req) -> Self::DefaultsXToY {
        MetadataDefaults2_0To3_1::new(endian)
    }

    fn convert_inner(
        self,
        def: Self::DefaultsXToY,
        ns: &mut NonStdKeywords,
        ms: &[Shortname],
    ) -> PureSuccess<InnerMetadata3_1> {
        NSKwParser::run(ns, |st| {
            let byteord = self.byteord.try_into().ok().unwrap_or(def.endian);
            InnerMetadata3_1 {
                mode: self.mode,
                byteord,
                cyt: self.cyt,
                timestamps: self.timestamps.map(|d| d.into()),
                spillover: st.try_convert_lookup_spillover(def.spillover, self.comp, ms),
                cytsn: st.lookup_maybe(def.cytsn),
                timestep: st.lookup_maybe(def.timestep),
                modification: st.lookup_modification(def.modification),
                plate: st.lookup_plate(def.plate),
                vol: st.lookup_maybe(def.vol),
            }
        })
    }
}

impl IntoMetadata<InnerMetadata3_1, MetadataDefaultsTo3_1> for InnerMetadata3_0 {
    type DefaultsXToY = MetadataDefaults3_0To3_1;
    type Req = Endian;

    fn defaults(endian: Self::Req) -> Self::DefaultsXToY {
        MetadataDefaults3_0To3_1::new(endian)
    }

    fn convert_inner(
        self,
        def: Self::DefaultsXToY,
        ns: &mut NonStdKeywords,
        ms: &[Shortname],
    ) -> PureSuccess<InnerMetadata3_1> {
        NSKwParser::run(ns, |st| {
            let byteord = self.byteord.try_into().ok().unwrap_or(def.endian);
            InnerMetadata3_1 {
                byteord,
                mode: self.mode,
                cyt: self.cyt,
                timestep: self.timestep,
                cytsn: self.cytsn,
                timestamps: self.timestamps.map(|d| d.into()),
                spillover: st.try_convert_lookup_spillover(def.spillover, self.comp, ms),
                modification: st.lookup_modification(def.modification),
                plate: st.lookup_plate(def.plate),
                vol: st.lookup_maybe(def.vol),
            }
        })
    }
}

impl IntoMetadata<InnerMetadata3_1, MetadataDefaultsTo3_1> for InnerMetadata3_1 {
    type DefaultsXToY = MetadataDefaults3_1To3_1;
    type Req = ();

    fn defaults(_: Self::Req) -> Self::DefaultsXToY {
        MetadataDefaults3_1To3_1
    }

    fn convert_inner(
        self,
        _: Self::DefaultsXToY,
        _: &mut NonStdKeywords,
        _: &[Shortname],
    ) -> PureSuccess<InnerMetadata3_1> {
        PureSuccess::from(self)
    }
}

impl IntoMetadata<InnerMetadata3_1, MetadataDefaultsTo3_1> for InnerMetadata3_2 {
    type DefaultsXToY = MetadataDefaults3_2To3_1;
    type Req = ();

    fn defaults(_: Self::Req) -> Self::DefaultsXToY {
        MetadataDefaults3_2To3_1
    }

    fn convert_inner(
        self,
        _: Self::DefaultsXToY,
        _: &mut NonStdKeywords,
        _: &[Shortname],
    ) -> PureSuccess<InnerMetadata3_1> {
        PureSuccess::from(InnerMetadata3_1 {
            mode: Mode::List,
            byteord: self.byteord,
            cyt: Some(self.cyt).into(),
            cytsn: self.cytsn,
            timestep: self.timestep,
            timestamps: self.timestamps,
            spillover: self.spillover,
            plate: self.plate,
            modification: self.modification,
            vol: self.vol,
        })
    }
}

impl IntoMetadata<InnerMetadata3_2, MetadataDefaultsTo3_2> for InnerMetadata2_0 {
    type DefaultsXToY = MetadataDefaults2_0To3_2;
    type Req = RequiredEndianCyt;

    fn defaults(r: Self::Req) -> Self::DefaultsXToY {
        MetadataDefaults2_0To3_2::new(r.endian, r.cyt)
    }

    fn convert_inner(
        self,
        def: Self::DefaultsXToY,
        ns: &mut NonStdKeywords,
        ms: &[Shortname],
    ) -> PureSuccess<InnerMetadata3_2> {
        NSKwParser::run(ns, |st| {
            let byteord = self.byteord.try_into().ok().unwrap_or(def.endian);
            let cyt = self.cyt.0.unwrap_or(def.cyt);
            // TODO what happens if $MODE is not list?
            InnerMetadata3_2 {
                byteord,
                cyt,
                spillover: st.try_convert_lookup_spillover(def.spillover, self.comp, ms),
                timestamps: self.timestamps.map(|d| d.into()),
                cytsn: st.lookup_maybe(def.cytsn),
                timestep: st.lookup_maybe(def.timestep),
                modification: st.lookup_modification(def.modification),
                plate: st.lookup_plate(def.plate),
                vol: st.lookup_maybe(def.vol),
                flowrate: st.lookup_maybe(def.flowrate),
                carrier: st.lookup_carrier(def.carrier),
                unstained: st.lookup_unstained(def.unstained),
                datetimes: st.lookup_datetimes(def.datetimes),
            }
        })
    }
}

impl IntoMetadata<InnerMetadata3_2, MetadataDefaultsTo3_2> for InnerMetadata3_0 {
    type DefaultsXToY = MetadataDefaults3_0To3_2;
    type Req = RequiredEndianCyt;

    fn defaults(r: Self::Req) -> Self::DefaultsXToY {
        MetadataDefaults3_0To3_2::new(r.endian, r.cyt)
    }

    fn convert_inner(
        self,
        def: Self::DefaultsXToY,
        ns: &mut NonStdKeywords,
        ms: &[Shortname],
    ) -> PureSuccess<InnerMetadata3_2> {
        NSKwParser::run(ns, |st| {
            let byteord = self.byteord.try_into().ok().unwrap_or(def.endian);
            let cyt = self.cyt.0.unwrap_or(def.cyt);
            InnerMetadata3_2 {
                byteord,
                cyt,
                timestep: self.timestep,
                cytsn: self.cytsn,
                timestamps: self.timestamps.map(|d| d.into()),
                modification: st.lookup_modification(def.modification),
                spillover: st.try_convert_lookup_spillover(def.spillover, self.comp, ms),
                plate: st.lookup_plate(def.plate),
                vol: st.lookup_maybe(def.vol),
                flowrate: st.lookup_maybe(def.flowrate),
                carrier: st.lookup_carrier(def.carrier),
                unstained: st.lookup_unstained(def.unstained),
                datetimes: st.lookup_datetimes(def.datetimes),
            }
        })
    }
}

impl IntoMetadata<InnerMetadata3_2, MetadataDefaultsTo3_2> for InnerMetadata3_1 {
    type DefaultsXToY = MetadataDefaults3_1To3_2;
    type Req = Cyt;

    fn defaults(cyt: Self::Req) -> Self::DefaultsXToY {
        MetadataDefaults3_1To3_2::new(cyt)
    }

    fn convert_inner(
        self,
        def: Self::DefaultsXToY,
        ns: &mut NonStdKeywords,
        _: &[Shortname],
    ) -> PureSuccess<InnerMetadata3_2> {
        let cyt = self.cyt.0.unwrap_or(def.cyt);
        NSKwParser::run(ns, |st| InnerMetadata3_2 {
            byteord: self.byteord,
            cyt,
            cytsn: self.cytsn,
            timestep: self.timestep,
            timestamps: self.timestamps,
            spillover: self.spillover,
            modification: self.modification,
            plate: self.plate,
            vol: self.vol,
            flowrate: st.lookup_maybe(def.flowrate),
            carrier: st.lookup_carrier(def.carrier),
            unstained: st.lookup_unstained(def.unstained),
            datetimes: st.lookup_datetimes(def.datetimes),
        })
    }
}

impl IntoMetadata<InnerMetadata3_2, MetadataDefaultsTo3_2> for InnerMetadata3_2 {
    type DefaultsXToY = MetadataDefaults3_2To3_2;
    type Req = ();

    fn defaults(_: Self::Req) -> Self::DefaultsXToY {
        MetadataDefaults3_2To3_2
    }

    fn convert_inner(
        self,
        _: Self::DefaultsXToY,
        _: &mut NonStdKeywords,
        _: &[Shortname],
    ) -> PureSuccess<InnerMetadata3_2> {
        PureSuccess::from(self)
    }
}

fn comp_to_spillover(comp: Compensation, ns: &[Shortname]) -> Option<Spillover> {
    // Matrix should be square, so if inverse fails that means that somehow it
    // isn't full rank
    comp.matrix.try_inverse().map(|matrix| Spillover {
        measurements: ns.to_vec(),
        matrix,
    })
}

fn spillover_to_comp(spillover: Spillover, ns: &[Shortname]) -> Option<Compensation> {
    // Start by making a new square matrix for all measurements, since the older
    // $COMP keyword couldn't specify measurements and thus covered all of them.
    // Then assign the spillover matrix to the bigger full matrix, using the
    // index of the measurement names. This will be a spillover matrix defined
    // for all measurements. Anything absent from the original will have 0 in
    // it's row/column except for the diagonal. Finally, invert this result to
    // get the compensation matrix.
    let n = ns.len();
    let mut full_matrix = DMatrix::<f32>::identity(n, n);
    // ASSUME spillover measurements are a subset of names supplied to function
    let positions: Vec<_> = spillover
        .measurements
        .into_iter()
        .enumerate()
        .flat_map(|(i, m)| ns.iter().position(|x| *x == m).map(|x| (i, x)))
        .collect();
    for r in positions.iter() {
        for c in positions.iter() {
            full_matrix[(r.1, c.1)] = spillover.matrix[(r.0, c.0)]
        }
    }
    // Matrix should be square, so if inverse fails that means that somehow it
    // isn't full rank
    full_matrix
        .try_inverse()
        .map(|matrix| Compensation { matrix })
}

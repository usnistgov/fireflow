use crate::config::*;
use crate::error::*;
use crate::keywords::*;
use crate::segment::*;

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use itertools::Itertools;
use regex::Regex;
use serde::ser::SerializeStruct;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;
use std::fs;
use std::io;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::iter;
use std::num::{IntErrorKind, ParseFloatError, ParseIntError};
use std::path;
use std::str;
use std::str::FromStr;

/// Output from parsing the FCS header.
///
/// Includes version and the three main segments (TEXT, DATA, ANALYSIS). For
/// now, OTHER segments are ignored. This may change in the future. Segments may
/// or may not be adjusted using configuration parameters to correct for errors.
///
/// Only valid segments are to be put in this struct (ie begin <= end).
#[derive(Debug, Clone, Serialize)]
pub struct Header {
    version: Version,
    text: Segment,
    data: Segment,
    analysis: Segment,
}

/// Output from parsing the TEXT segment.
///
/// This is derived from the HEADER which should be parsed in order to obtain
/// this.
///
/// Segment offsets derived from HEADER may be updated depending on keywords
/// present. See fields below for more information on this.
#[derive(Debug, Clone, Serialize)]
struct RawTEXT {
    /// FCS version from header
    version: Version,

    // TODO this is a limitation of the current error handler where it would be
    // nice to evaluate which are warnings and which are errors at the very end
    // when we return a result. In this case, I would need to set the level flag
    // when making the error when parsing the offsets directly; it would be a
    // warning by default if we don't wish to parse DATA, and an error no matter
    // what if we did need to parse DATA. The only way to do this is to "catch"
    // the errors later which means we would need a system of propagating the
    // error identities forward aside from their string message.
    /// All offsets as parsed from raw TEXT and HEADER.
    offsets: Offsets,

    /// Keyword pairs
    ///
    /// This does not include offset keywords (DATA, STEXT, ANALYSIS) and will
    /// include supplemental TEXT keywords if present and the offsets for
    /// supplemental TEXT are successfully found.
    keywords: RawKeywords,

    /// Delimiter used to parse TEXT.
    ///
    /// Included here for informational purposes.
    delimiter: u8,
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
struct Offsets {
    /// Primary TEXT offsets
    ///
    /// The offsets that were used to parse the TEXT segment. Included here for
    /// informational purposes.
    prim_text_seg: Segment,

    /// Supplemental TEXT offsets
    ///
    /// This is not needed downstream and included here for informational
    /// purposes. It will always be None for 2.0 which does not include this.
    supp_text_seg: Option<Segment>,

    /// DATA offsets
    ///
    /// The offsets pointing to the DATA segment. If None then an error occured
    /// when acquiring the offset. If DATA does not exist this will be 0,0.
    ///
    /// This may be used later to acquire the DATA segment.
    data_seg: Option<Segment>,

    /// ANALYSIS offsets.
    ///
    /// The offsets pointing to the ANALYSIS segment. If None then an error
    /// occured when acquiring the offset. If ANALYSIS does not exist this will
    /// be 0,0.
    ///
    /// This may be used later to acquire the ANALYSIS segment.
    analysis_seg: Option<Segment>,

    /// NEXTDATA offset
    ///
    /// This will be copied as represented in TEXT. If it is 0, there is no next
    /// dataset, otherwise it points to the next dataset in the file.
    nextdata: u32,
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
struct StandardizedTEXT {
    /// Offsets as parsed from raw TEXT and HEADER
    offsets: Offsets,

    /// Structured data derived from TEXT specific to the indicated FCS version.
    ///
    /// All keywords that were included in the ['RawTEXT'] used to create this
    /// will be included here. Anything standardized will be put into a field
    /// that can be readily accessed directly and returned with the proper type.
    /// Anything nonstandard will be kept in a hash table whose values will
    /// be strings.
    standardized: AnyCoreTEXT,

    /// Non-standard keywords that start with '$'.
    ///
    /// These are keywords whose values start with '$' but are not part of the
    /// standard. Such keywords are technically not allowed, but they are
    /// included here in case they happen to be useful later.
    deviant_keywords: RawKeywords,

    /// Delimiter used to parse TEXT.
    ///
    /// Included here for informational purposes.
    delimiter: u8,
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
struct RawDataset {
    /// Offsets as parsed from raw TEXT and HEADER
    // TODO the data segment in this should be non-Option since we know it
    // exists if this struct exists.
    offsets: Offsets,

    // TODO add keywords
    // TODO add dataset
    /// Delimiter used to parse TEXT.
    ///
    /// Included here for informational purposes.
    delimiter: u8,
}

/// Output of parsing one standardized dataset (TEXT+DATA) from an FCS file.
struct StandardizedDataset {
    /// Offsets as parsed from raw TEXT and HEADER
    // TODO the data segment in this should be non-Option since we know it
    // exists if this struct exists.
    offsets: Offsets,

    /// Structured data derived from TEXT specific to the indicated FCS version.
    dataset: CoreDataset,

    /// Non-standard keywords that start with '$'.
    ///
    /// When constructing the fields above, any '$'-prefixed key that is necessary
    /// for populating these standardized structures will be removed from the
    /// raw list of key/value pairs. Anything left over that starts with a '$'
    /// will be put here.
    deviant_keywords: StdKeywords,

    /// Delimiter used to parse TEXT.
    ///
    /// Included here for informational purposes.
    delimiter: u8,
}

/// Represents the minimal data to fully describe one dataset in an FCS file.
///
/// This will include the standardized TEXT keywords as well as its
/// corresponding DATA segment parsed into a dataframe-like structure.
struct CoreDataset {
    keywords: AnyCoreTEXT,

    /// DATA segment
    ///
    /// This encodes the bytes from the DATA segment in a dataframe-like
    /// structure whose type will perfectly reflect the underlying data.
    // TODO it isn't clear to what degree this type needs to match the structure
    // imposed by the standardized TEXT type. For early versions, this can only
    // be a matrix (ie same type for every cell) but for later versions which
    // relax byteord and allow column-specific typing, this will be a dataframe.
    // Regardless, it will likely make sense to check/cast user data anyways,
    // since this likely will get very annoying to enforce at the application
    // level. Furthermore, even python has a suboptimal typing interface for
    // pandas dataframes (R is worse), so it's not like this can be checked
    // statically anyways.
    data: ParsedData,

    /// ANALYSIS segment
    ///
    /// This will be empty if ANALYSIS either doesn't exist or the computation
    /// fails. This has not standard structure, so the best we can capture is a
    /// byte sequence.
    analysis: Vec<u8>,
}

/// Represents the values in the DATA segment.
///
/// Each slot in the outer vector is a column, and each column has a type which
/// reflects the underlying data. In Python/R, this is analogous to a dataframe.
/// Each column is assumed to have the same length.
type ParsedData = Vec<Series>;

/// A data column.
///
/// Each column can only be one type, but this can be any one of several.
pub enum Series {
    F32(Vec<f32>),
    F64(Vec<f64>),
    U8(Vec<u8>),
    U16(Vec<u16>),
    U32(Vec<u32>),
    U64(Vec<u64>),
}

/// Critical FCS TEXT data for any supported FCS version
#[derive(Debug)]
pub enum AnyCoreTEXT {
    FCS2_0(Box<CoreText2_0>),
    FCS3_0(Box<CoreText3_0>),
    FCS3_1(Box<CoreText3_1>),
    FCS3_2(Box<CoreText3_2>),
}

/// Minimally-required FCS TEXT data for each version
type CoreText2_0 = CoreTEXT<InnerMetadata2_0, InnerMeasurement2_0>;
type CoreText3_0 = CoreTEXT<InnerMetadata3_0, InnerMeasurement3_0>;
type CoreText3_1 = CoreTEXT<InnerMetadata3_1, InnerMeasurement3_1>;
type CoreText3_2 = CoreTEXT<InnerMetadata3_2, InnerMeasurement3_2>;

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
#[derive(Debug, Clone, Serialize)]
struct CoreTEXT<M, P> {
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

/// TEXT keyword pairs whose key starts with '$'
type StdKeywords = HashMap<StdKey, String>;

/// TEXT keyword pairs whose key does not start with with '$'
type NonStdKeywords = HashMap<NonStdKey, String>;

/// Key that starts with '$'
#[derive(Hash, Eq, PartialEq, Clone, Debug, Serialize)]
struct StdKey(String);

/// Key that does not start with '$'
#[derive(Hash, Eq, PartialEq, Clone, Debug, Serialize)]
struct NonStdKey(String);

/// All FCS versions this library supports.
///
/// This appears as the first 6 bytes of any valid FCS file.
#[derive(Debug, Clone, Copy, Eq, PartialEq, PartialOrd, Ord, Serialize)]
enum Version {
    FCS2_0,
    FCS3_0,
    FCS3_1,
    FCS3_2,
}

/// A datetime as used in the $(BEGIN|END)DATETIME keys (3.2+ only)
#[derive(Debug, Clone, Serialize)]
struct FCSDateTime(DateTime<FixedOffset>);

/// A time as used in the $BTIM/ETIM keys without seconds (2.0 only)
#[derive(Debug, Clone, Serialize)]
struct FCSTime(NaiveTime);

/// A time as used in the $BTIM/ETIM keys with 1/60 seconds (3.0 only)
#[derive(Debug, Clone, Serialize)]
struct FCSTime60(NaiveTime);

/// A time as used in the $BTIM/ETIM keys with centiseconds (3.1+ only)
#[derive(Debug, Clone, Serialize)]
struct FCSTime100(NaiveTime);

/// A datetime as used in the $LAST_MODIFIED key (3.1+ only)
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

/// $BTIM/ETIM/DATE for FCS 2.0
type Timestamps2_0 = Timestamps<FCSTime>;

/// $BTIM/ETIM/DATE for FCS 3.0
type Timestamps3_0 = Timestamps<FCSTime60>;

/// $BTIM/ETIM/DATE for FCS 3.1+
type Timestamps3_1 = Timestamps<FCSTime100>;

/// A convenient bundle for the $BEGINDATETIME and $ENDDATETIME keys (3.2+)
#[derive(Debug, Clone, Serialize)]
struct Datetimes {
    /// Value for the $BEGINDATETIME key.
    begin: OptionalKw<FCSDateTime>,

    /// Value for the $ENDDATETIME key.
    end: OptionalKw<FCSDateTime>,
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
#[derive(Debug, Clone, Copy, Serialize, PartialEq)]
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
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
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
    matrix: Vec<Vec<f32>>,
}

/// The spillover matrix from the $SPILLOVER keyword (3.1+)
#[derive(Debug, Clone, Serialize)]
struct Spillover {
    /// The measurements in the spillover matrix. Assumed to be a subset of the
    /// values in the $PnN keys.
    measurements: Vec<Shortname>,

    /// Numeric values in the spillover matrix in row-major order.
    matrix: Vec<Vec<f32>>,
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
enum Scale {
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

/// The value for the $PnN key (all versions).
///
/// This cannot contain commas.
#[derive(Debug, Clone, Serialize, Eq, PartialEq, Hash)]
struct Shortname(String);

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
#[derive(Debug, Clone, Serialize)]
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
#[derive(Debug, Clone, Serialize)]
struct ModificationData {
    last_modifier: OptionalKw<String>,
    last_modified: OptionalKw<ModifiedDateTime>,
    originality: OptionalKw<Originality>,
}

/// A bundle for $PLATEID, $PLATENAME, and $WELLID (3.1+)
#[derive(Debug, Clone, Serialize)]
struct PlateData {
    plateid: OptionalKw<String>,
    platename: OptionalKw<String>,
    wellid: OptionalKw<String>,
}

/// The value for the $UNSTAINEDCENTERS key (3.2+)
///
/// This is actually encoded as a string like 'n,[measuremnts,],[values]' but
/// here is more conveniently encoded as a hash table.
#[derive(Debug, Clone, Serialize)]
struct UnstainedCenters(HashMap<Shortname, f32>);

/// A bundle for $UNSTAINEDCENTERS and $UNSTAINEDINFO (3.2+)
#[derive(Debug, Clone, Serialize)]
struct UnstainedData {
    unstainedcenters: OptionalKw<UnstainedCenters>,
    unstainedinfo: OptionalKw<String>,
}

/// A bundle for $CARRIERID, $CARRIERTYPE, $LOCATIONID (3.2+)
#[derive(Debug, Clone, Serialize)]
struct CarrierData {
    carrierid: OptionalKw<String>,
    carriertype: OptionalKw<String>,
    locationid: OptionalKw<String>,
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

/// Denotes that the value for a key is optional.
///
/// This is basically an Option but more obvious in what it indicates.
#[derive(Debug, Clone, PartialEq, Eq)]
enum OptionalKw<V> {
    Present(V),
    Absent,
}

/// Measurement fields specific to version 2.0
#[derive(Debug, Clone, Serialize)]
struct InnerMeasurement2_0 {
    /// Value for $PnE
    scale: OptionalKw<Scale>,

    /// Value for $PnL
    wavelength: OptionalKw<u32>,

    /// Value for $PnN
    shortname: OptionalKw<Shortname>,
}

/// Measurement fields specific to version 3.0
#[derive(Debug, Clone, Serialize)]
struct InnerMeasurement3_0 {
    /// Value for $PnE
    scale: Scale,

    /// Value for $PnL
    wavelength: OptionalKw<u32>,

    /// Value for $PnN
    shortname: OptionalKw<Shortname>,

    /// Value for $PnG
    gain: OptionalKw<f32>,
}

/// Measurement fields specific to version 3.1
#[derive(Debug, Clone, Serialize)]
struct InnerMeasurement3_1 {
    /// Value for $PnE
    scale: Scale,

    /// Value for $PnL
    wavelengths: OptionalKw<Wavelengths>,

    /// Value for $PnN
    shortname: Shortname,

    /// Value for $PnG
    gain: OptionalKw<f32>,

    /// Value for $PnCALIBRATION
    calibration: OptionalKw<Calibration3_1>,

    /// Value for $PnDISPLAY
    display: OptionalKw<Display>,
}

/// Measurement fields specific to version 3.2
#[derive(Debug, Clone, Serialize)]
struct InnerMeasurement3_2 {
    /// Value for $PnE
    scale: Scale,

    /// Value for $PnL
    wavelengths: OptionalKw<Wavelengths>,

    /// Value for $PnN
    shortname: Shortname,

    /// Value for $PnG
    gain: OptionalKw<f32>,

    /// Value for $PnCALIBRATION
    calibration: OptionalKw<Calibration3_2>,

    /// Value for $PnDISPLAY
    display: OptionalKw<Display>,

    /// Value for $PnANALYTE
    analyte: OptionalKw<String>,

    /// Value for $PnFEATURE
    feature: OptionalKw<Feature>,

    /// Value for $PnTYPE
    measurement_type: OptionalKw<MeasurementType>,

    /// Value for $PnTAG
    tag: OptionalKw<String>,

    /// Value for $PnDET
    detector_name: OptionalKw<String>,

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
#[derive(Debug, Clone, Serialize)]
struct Measurement<X> {
    /// Value for $PnB
    bytes: Bytes,

    /// Value for $PnR
    range: Range,

    /// Value for $PnS
    longname: OptionalKw<String>,

    /// Value for $PnF
    filter: OptionalKw<String>,

    /// Value for $PnO
    power: OptionalKw<u32>,

    /// Value for $PnD
    detector_type: OptionalKw<String>,

    /// Value for $PnP
    percent_emitted: OptionalKw<u32>,

    /// Value for $PnV
    detector_voltage: OptionalKw<f32>,

    /// Non standard keywords that belong to this measurement.
    ///
    /// These are found using a configurable pattern to filter matching keys.
    nonstandard: NonStdKeywords,

    /// Version specific data
    specific: X,
}

/// FCS 2.0-specific data for one measurement
type Measurement2_0 = Measurement<InnerMeasurement2_0>;

/// FCS 3.0-specific data for one measurement
type Measurement3_0 = Measurement<InnerMeasurement3_0>;

/// FCS 3.1-specific data for one measurement
type Measurement3_1 = Measurement<InnerMeasurement3_1>;

/// FCS 3.2-specific data for one measurement
type Measurement3_2 = Measurement<InnerMeasurement3_2>;

/// Metadata fields specific to version 2.0
#[derive(Debug, Clone, Serialize)]
struct InnerMetadata2_0 {
    /// Value of $MODE
    mode: Mode,

    /// Value of $BYTEORD
    byteord: ByteOrd,

    /// Value of $CYT
    cyt: OptionalKw<String>,

    /// Compensation matrix derived from 'DFCnTOm' key/value pairs
    comp: OptionalKw<Compensation>,

    /// Values of $BTIM/ETIM/$DATE
    timestamps: Timestamps2_0,
}

/// Metadata fields specific to version 3.0
#[derive(Debug, Clone, Serialize)]
struct InnerMetadata3_0 {
    /// Value of $MODE
    mode: Mode,

    /// Value of $BYTEORD
    byteord: ByteOrd,

    /// Value of $CYT
    cyt: OptionalKw<String>,

    /// Value of $COMP
    comp: OptionalKw<Compensation>,

    /// Values of $BTIM/ETIM/$DATE
    timestamps: Timestamps3_0,

    /// Value of $CYTSN
    cytsn: OptionalKw<String>,

    /// Value of $TIMESTEP
    timestep: OptionalKw<f32>,

    /// Value of $UNICODE
    unicode: OptionalKw<Unicode>,
}

/// Metadata fields specific to version 3.1
#[derive(Debug, Clone, Serialize)]
struct InnerMetadata3_1 {
    /// Value of $MODE
    mode: Mode,

    /// Value of $BYTEORD
    byteord: Endian,

    /// Value of $CYT
    cyt: OptionalKw<String>,

    /// Values of $BTIM/ETIM/$DATE
    timestamps: Timestamps3_1,

    /// Value of $CYTSN
    cytsn: OptionalKw<String>,

    /// Value of $TIMESTEP
    timestep: OptionalKw<f32>,

    /// Value of $SPILLOVER
    spillover: OptionalKw<Spillover>,

    /// Values of $LAST_MODIFIED/$LAST_MODIFIER/$ORIGINALITY
    modification: ModificationData,

    /// Values of $PLATEID/$PLATENAME/$WELLID
    plate: PlateData,

    /// Value of $VOL
    vol: OptionalKw<f32>,
}

#[derive(Debug, Clone, Serialize)]
struct InnerMetadata3_2 {
    /// Value of $BYTEORD
    byteord: Endian,

    /// Values of $BTIM/ETIM/$DATE
    timestamps: Timestamps3_1,

    /// Values of $BEGINDATETIME/$ENDDATETIME
    datetimes: Datetimes,

    /// Value of $CYT
    cyt: String,

    /// Value of $SPILLOVER
    spillover: OptionalKw<Spillover>,

    /// Value of $CYTSN
    cytsn: OptionalKw<String>,

    /// Value of $TIMESTEP
    timestep: OptionalKw<f32>,

    /// Values of $LAST_MODIFIED/$LAST_MODIFIER/$ORIGINALITY
    modification: ModificationData,

    /// Values of $PLATEID/$PLATENAME/$WELLID
    plate: PlateData,

    /// Value of $VOL
    vol: OptionalKw<f32>,

    /// Values of $CARRIERID/$CARRIERTYPE/$LOCATIONID
    carrier: CarrierData,

    /// Values of $UNSTAINEDINFO/$UNSTAINEDCENTERS
    unstained: UnstainedData,

    /// Value of $FLOWRATE
    flowrate: OptionalKw<String>,
}

/// Structured non-measurement metadata.
///
/// Explicit below are common to all FCS versions.
///
/// The generic type parameter allows version-specific data to be encoded.
#[derive(Debug, Clone, Serialize)]
struct Metadata<X> {
    /// Value of $DATATYPE
    datatype: AlphaNumType,

    /// Value of $ABRT
    abrt: OptionalKw<u32>,

    /// Value of $COM
    com: OptionalKw<String>,

    /// Value of $CELLS
    cells: OptionalKw<String>,

    /// Value of $EXP
    exp: OptionalKw<String>,

    /// Value of $FIL
    fil: OptionalKw<String>,

    /// Value of $INST
    inst: OptionalKw<String>,

    /// Value of $LOST
    lost: OptionalKw<u32>,

    /// Value of $OP
    op: OptionalKw<String>,

    /// Value of $PROJ
    proj: OptionalKw<String>,

    /// Value of $SMNO
    smno: OptionalKw<String>,

    /// Value of $SRC
    src: OptionalKw<String>,

    /// Value of $SYS
    sys: OptionalKw<String>,

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

/// FCS 2.0-specific structured metadata derived from TEXT
type Metadata2_0 = Metadata<InnerMetadata2_0>;

/// FCS 3.0-specific structured metadata derived from TEXT
type Metadata3_0 = Metadata<InnerMetadata3_0>;

/// FCS 3.1-specific structured metadata derived from TEXT
type Metadata3_1 = Metadata<InnerMetadata3_1>;

/// FCS 3.2-specific structured metadata derived from TEXT
type Metadata3_2 = Metadata<InnerMetadata3_2>;

/// The bare minimum of measurement data required to parse the DATA segment.
struct MinimalMeasurement<X> {
    /// The value of $PnB
    bytes: Bytes,

    /// The value of $PnR
    range: Range,

    /// Version-specific data
    specific: X,
}

/// Minimal data to parse one measurement column in FCS 2.0 (up to 3.1)
type MinimalMeasurement2_0 = MinimalMeasurement<()>;

/// Minimal data to parse one measurement column in FCS 3.2+
type MinimalMeasurement3_2 = MinimalMeasurement<NumType>;

struct InnerMinMetadata2_0 {
    tot: OptionalKw<u32>,
    byteord: ByteOrd,
}

struct InnerMinMetadata3_0 {
    tot: u32,
    byteord: ByteOrd,
}

struct InnerMinMetadata3_1 {
    tot: u32,
    byteord: Endian,
}

struct MinMetadata<X> {
    datatype: AlphaNumType,
    specific: X,
}

type MinMetadata2_0 = MinMetadata<InnerMinMetadata2_0>;
type MinMetadata3_0 = MinMetadata<InnerMinMetadata3_0>;
type MinMetadata3_1 = MinMetadata<InnerMinMetadata3_1>;

struct FCSDateTimeError;
struct FCSTimeError;
struct FCSTime60Error;
struct FCSTime100Error;
struct FCSDateError;

struct VersionError;
struct AlphaNumTypeError;
struct NumTypeError;
pub struct EndianError;
struct ModifiedDateTimeError;
struct ShortnameError;
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

enum ScaleError {
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
                    let nn = tt * 1000000 / 60;
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
        let cc = self.0.nanosecond() / 10000000 * 60;
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
        let cc = self.0.nanosecond() / 10000000;
        write!(f, "{}.{}", base, cc)
    }
}

impl fmt::Display for FCSTime100Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "must be like 'hh:mm:ss[.cc]'")
    }
}

impl str::FromStr for Version {
    type Err = VersionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "FCS2.0" => Ok(Version::FCS2_0),
            "FCS3.0" => Ok(Version::FCS3_0),
            "FCS3.1" => Ok(Version::FCS3_1),
            "FCS3.2" => Ok(Version::FCS3_2),
            _ => Err(VersionError),
        }
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Version::FCS2_0 => write!(f, "FCS2.0"),
            Version::FCS3_0 => write!(f, "FCS3.0"),
            Version::FCS3_1 => write!(f, "FCS3.1"),
            Version::FCS3_2 => write!(f, "FCS3.2"),
        }
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
                    let matrix = fvalues
                        .into_iter()
                        .chunks(n)
                        .into_iter()
                        .map(|c| c.collect())
                        .collect();
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
        let xs = self.matrix.iter().map(|xs| xs.iter().join(",")).join(",");
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
                let measurements: Vec<_> = xs
                    .by_ref()
                    .take(n)
                    .map(|m| Shortname(String::from(m)))
                    .collect();
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
                        let matrix = fvalues
                            .into_iter()
                            .chunks(n)
                            .into_iter()
                            .map(|c| c.collect())
                            .collect();
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
        let xs = self.matrix.iter().map(|ys| ys.iter().join(",")).join(",");
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
        let header0 = vec!["[-]".to_string()];
        let header = header0
            .into_iter()
            .chain(self.measurements.iter().map(|m| m.0.clone()))
            .join(delim);
        let lines = vec![header];
        let rows = self.matrix.iter().map(|xs| xs.iter().join(delim));
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

impl ByteOrd {
    // This only makes sense for pre 3.1 integer types
    fn num_bytes(&self) -> u8 {
        match self {
            ByteOrd::Endian(_) => 4,
            ByteOrd::Mixed(xs) => xs.len() as u8,
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
                    measurement: Shortname(String::from(p)),
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

impl<V: fmt::Display> OptionalKw<V> {
    fn as_opt_string(&self) -> Option<String> {
        self.as_ref().into_option().map(|x| x.to_string())
    }
}

impl<T: Serialize> Serialize for OptionalKw<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self.as_ref() {
            Present(x) => serializer.serialize_some(x),
            Absent => serializer.serialize_none(),
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

impl fmt::Display for ShortnameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "commas are not allowed")
    }
}

impl fmt::Display for Shortname {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", self.0)
    }
}

impl str::FromStr for Shortname {
    type Err = ShortnameError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.contains(',') {
            Err(ShortnameError)
        } else {
            Ok(Shortname(String::from(s)))
        }
    }
}

// TODO this will likely need to be a trait in 4.0
impl InnerMeasurement3_2 {
    fn get_column_type(&self, default: AlphaNumType) -> AlphaNumType {
        self.datatype
            .as_ref()
            .into_option()
            .copied()
            .map(AlphaNumType::from)
            .unwrap_or(default)
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
            Bytes::Fixed(x) => write!(f, "{}", x),
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

    // TODO include nonstandard?
    fn keywords(&self, n: &str) -> Vec<(String, Option<String>)> {
        P::keywords(self, n)
    }

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
        _ => Err(vec![String::from("$PnB has invalid byte length")]),
    }
}

trait Versioned {
    fn fcs_version() -> Version;
}

trait VersionedMeasurement: Sized + Versioned {
    fn lookup_specific(st: &mut KwState, n: usize) -> Option<Self>;

    fn measurement_name(p: &Measurement<Self>) -> Option<&str>;

    fn lookup_measurements(st: &mut KwState, par: usize) -> Option<Vec<Measurement<Self>>> {
        let mut ps = vec![];
        let v = Self::fcs_version();
        for n in 1..(par + 1) {
            let maybe_bytes = st.lookup_meas_bytes(n);
            let maybe_range = st.lookup_meas_range(n);
            let maybe_specific = Self::lookup_specific(st, n);
            if let (Some(bytes), Some(range), Some(specific)) =
                (maybe_bytes, maybe_range, maybe_specific)
            {
                let p = Measurement {
                    bytes,
                    range,
                    longname: st.lookup_meas_longname(n),
                    filter: st.lookup_meas_filter(n),
                    power: st.lookup_meas_power(n),
                    detector_type: st.lookup_meas_detector_type(n),
                    percent_emitted: st.lookup_meas_percent_emitted(n, v == Version::FCS3_2),
                    detector_voltage: st.lookup_meas_detector_voltage(n),
                    specific,
                    nonstandard: st.lookup_meas_nonstandard(n),
                };
                ps.push(p);
            }
        }
        let names: Vec<&str> = ps
            .iter()
            .filter_map(|m| Self::measurement_name(m))
            .collect();
        if let Some(time_name) = &st.conf.time_shortname {
            if !names.iter().copied().contains(time_name.as_str()) {
                st.push_meta_error_or_warning(
                    st.conf.ensure_time,
                    format!("Channel called '{time_name}' not found for time"),
                );
            }
        }
        if names.iter().unique().count() < names.len() {
            st.push_meta_error_str("$PnN are not all unique");
            None
        } else {
            Some(ps)
        }
    }

    fn suffixes_inner(&self) -> Vec<(&'static str, Option<String>)>;

    fn keywords(m: &Measurement<Self>, n: &str) -> Vec<(String, Option<String>)> {
        let fixed = [
            (BYTES_SFX, Some(m.bytes.to_string())),
            (RANGE_SFX, Some(m.range.to_string())),
            (LONGNAME_SFX, m.longname.as_opt_string()),
            (FILTER_SFX, m.filter.as_opt_string()),
            (POWER_SFX, m.power.as_opt_string()),
            (DET_TYPE_SFX, m.detector_type.as_opt_string()),
            (PCNT_EMT_SFX, m.percent_emitted.as_opt_string()),
            (DET_VOLT_SFX, m.detector_voltage.as_opt_string()),
        ];
        fixed
            .into_iter()
            .chain(m.specific.suffixes_inner())
            .map(|(s, v)| (format_measurement(n, s), v))
            .collect()
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
    fn measurement_name(p: &Measurement<Self>) -> Option<&str> {
        p.specific
            .shortname
            .as_ref()
            .into_option()
            .map(|s| s.0.as_str())
    }

    fn lookup_specific(st: &mut KwState, n: usize) -> Option<InnerMeasurement2_0> {
        Some(InnerMeasurement2_0 {
            scale: st.lookup_meas_scale_opt(n),
            shortname: st.lookup_meas_shortname_opt(n),
            wavelength: st.lookup_meas_wavelength(n),
        })
    }

    fn suffixes_inner(&self) -> Vec<(&'static str, Option<String>)> {
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
    fn measurement_name(p: &Measurement<Self>) -> Option<&str> {
        p.specific
            .shortname
            .as_ref()
            .into_option()
            .map(|s| s.0.as_str())
    }

    fn lookup_specific(st: &mut KwState, n: usize) -> Option<InnerMeasurement3_0> {
        let shortname = st.lookup_meas_shortname_opt(n);
        Some(InnerMeasurement3_0 {
            scale: st.lookup_meas_scale_timecheck_opt(n, &shortname)?,
            gain: st.lookup_meas_gain_timecheck_opt(n, &shortname),
            shortname,
            wavelength: st.lookup_meas_wavelength(n),
        })
    }

    fn suffixes_inner(&self) -> Vec<(&'static str, Option<String>)> {
        [
            (SCALE_SFX, Some(self.scale.to_string())),
            (SHORTNAME_SFX, self.shortname.as_opt_string()),
            (WAVELEN_SFX, self.wavelength.as_opt_string()),
            (GAIN_SFX, self.gain.as_opt_string()),
        ]
        .into_iter()
        .collect()
    }
}

impl VersionedMeasurement for InnerMeasurement3_1 {
    fn measurement_name(p: &Measurement<Self>) -> Option<&str> {
        Some(p.specific.shortname.0.as_str())
    }

    fn lookup_specific(st: &mut KwState, n: usize) -> Option<InnerMeasurement3_1> {
        let shortname = st.lookup_meas_shortname_req(n)?;
        Some(InnerMeasurement3_1 {
            scale: st.lookup_meas_scale_timecheck(n, &shortname)?,
            gain: st.lookup_meas_gain_timecheck(n, &shortname),
            shortname,
            wavelengths: st.lookup_meas_wavelengths(n),
            calibration: st.lookup_meas_calibration3_1(n),
            display: st.lookup_meas_display(n),
        })
    }

    fn suffixes_inner(&self) -> Vec<(&'static str, Option<String>)> {
        [
            (SCALE_SFX, Some(self.scale.to_string())),
            (SHORTNAME_SFX, Some(self.shortname.to_string())),
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
    fn measurement_name(p: &Measurement<Self>) -> Option<&str> {
        Some(p.specific.shortname.0.as_str())
    }

    fn lookup_specific(st: &mut KwState, n: usize) -> Option<InnerMeasurement3_2> {
        let shortname = st.lookup_meas_shortname_req(n)?;
        Some(InnerMeasurement3_2 {
            gain: st.lookup_meas_gain_timecheck(n, &shortname),
            scale: st.lookup_meas_scale_timecheck(n, &shortname)?,
            shortname,
            wavelengths: st.lookup_meas_wavelengths(n),
            calibration: st.lookup_meas_calibration3_2(n),
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

    fn suffixes_inner(&self) -> Vec<(&'static str, Option<String>)> {
        [
            (SCALE_SFX, Some(self.scale.to_string())),
            (SHORTNAME_SFX, Some(self.shortname.to_string())),
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
            let measurements: Vec<_> = xs
                .by_ref()
                .take(n)
                .map(|m| Shortname(String::from(m)))
                .collect();
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
        let (measurements, values): (Vec<_>, Vec<_>) =
            self.0.iter().map(|(k, v)| (k.0.clone(), *v)).unzip();
        write!(
            f,
            "{n},{},{}",
            measurements.join(","),
            values.iter().join(",")
        )
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

impl<M: VersionedMetadata> Metadata<M> {
    fn keywords(&self, par: usize, tot: usize, len: KwLengths) -> MaybeKeywords {
        M::keywords(self, par, tot, len)
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

impl AnyCoreTEXT {
    pub fn print_meas_table(&self, delim: &str) {
        match self {
            AnyCoreTEXT::FCS2_0(x) => x.print_meas_table(delim),
            AnyCoreTEXT::FCS3_0(x) => x.print_meas_table(delim),
            AnyCoreTEXT::FCS3_1(x) => x.print_meas_table(delim),
            AnyCoreTEXT::FCS3_2(x) => x.print_meas_table(delim),
        }
    }

    pub fn print_spillover_table(&self, delim: &str) {
        let res = match self {
            AnyCoreTEXT::FCS2_0(_) => None,
            AnyCoreTEXT::FCS3_0(_) => None,
            AnyCoreTEXT::FCS3_1(x) => x
                .metadata
                .specific
                .spillover
                .as_ref()
                .into_option()
                .map(|s| s.print_table(delim)),
            AnyCoreTEXT::FCS3_2(x) => x
                .metadata
                .specific
                .spillover
                .as_ref()
                .into_option()
                .map(|s| s.print_table(delim)),
        };
        if res.is_none() {
            println!("None")
        }
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

impl<M: VersionedMetadata> CoreTEXT<M, M::P> {
    fn get_shortnames(&self) -> Vec<&str> {
        self.measurements
            .iter()
            .filter_map(|p| M::P::measurement_name(p))
            .collect()
    }

    // TODO char should be validated somehow
    fn text_segment(&self, delim: char, data_len: usize) -> String {
        let ms: Vec<_> = self
            .measurements
            .iter()
            .enumerate()
            .flat_map(|(i, m)| m.keywords(&(i + 1).to_string()))
            .flat_map(|(k, v)| v.map(|x| (k, x)))
            .collect();
        let meas_len = ms.iter().map(|(k, v)| k.len() + v.len() + 2).sum();
        let len = KwLengths {
            data: data_len,
            measurements: meas_len,
        };
        // TODO properly populate tot/par here
        let mut meta: Vec<(String, String)> = self
            .metadata
            .keywords(0, 0, len)
            .into_iter()
            .flat_map(|(k, v)| v.map(|x| (String::from(k), x)))
            .chain(ms)
            .collect();

        meta.sort_by(|a, b| a.0.cmp(&b.0));

        let fin = meta
            .into_iter()
            .map(|(k, v)| format!("{}{}{}", k, delim, v))
            .join(&delim.to_string());
        format!("{fin}{delim}")
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

    fn from_raw(kws: RawKeywords, conf: &StdTextReader) -> PureResult<(Self, RawKeywords)> {
        let mut st = KwState::from(kws, conf);
        if let Some(par) = st.lookup_par() {
            let ms = M::P::lookup_measurements(&mut st, par);
            let md = ms.as_ref().and_then(|xs| M::lookup_metadata(&mut st, xs));
            if let (Some(measurements), Some(metadata)) = (ms, md) {
                // TODO add errors if desired informing user that these were
                // found
                let (deferred, deviant_keywords) = st.collect();
                Ok(PureSuccess {
                    data: (
                        CoreTEXT {
                            metadata,
                            measurements,
                        },
                        deviant_keywords,
                    ),
                    deferred,
                })
            } else {
                Err(st.into_errors("could not standardize TEXT".to_string()))
            }
        } else {
            Err(st.into_errors("could not find $PAR".to_string()))
        }
    }

    fn any_from_raw(
        kws: RawKeywords,
        conf: &StdTextReader,
    ) -> PureResult<(AnyCoreTEXT, RawKeywords)> {
        Self::from_raw(kws, conf)
            .map(|succ| succ.map(|(core, deviant)| (M::into_any(core), deviant)))
    }
}

struct IntermediateTEXT<'a, M, P> {
    par: usize,
    data_seg: Segment,
    metadata: MinMetadata<M>,
    measurements: Vec<MinimalMeasurement<P>>,
    conf: &'a StdTextReader,
}

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

impl Series {
    pub fn len(&self) -> usize {
        match_many_to_one!(self, Series, [F32, F64, U8, U16, U32, U64], x, { x.len() })
    }

    pub fn format(&self, r: usize) -> String {
        match_many_to_one!(self, Series, [F32, F64, U8, U16, U32, U64], x, {
            format!("{}", x[r])
        })
    }
}

pub trait IntMath: Sized {
    fn next_power_2(x: Self) -> Self;
}

pub trait NumProps<const DTLEN: usize>: Sized + Copy {
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
        if endian == Endian::Big {
            Self::read_from_big(h)
        } else {
            Self::read_from_little(h)
        }
    }
}

macro_rules! impl_num_props {
    ($size:expr, $zero:expr, $t:ty, $p:ident) => {
        impl From<Vec<$t>> for Series {
            fn from(value: Vec<$t>) -> Self {
                Series::$p(value)
            }
        }

        impl NumProps<$size> for $t {
            fn zero() -> Self {
                $zero
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

impl_num_props!(1, 0, u8, U8);
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
        }
    };
}

impl_int_math!(u8);
impl_int_math!(u16);
impl_int_math!(u32);
impl_int_math!(u64);

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
            .or(Err(format!(
                "$BYTEORD is mixed but length is {} and not {LEN}",
                v.len()
            ))),
    }
}

trait IntFromBytes<const DTLEN: usize, const INTLEN: usize>:
    NumProps<DTLEN> + OrderedFromBytes<DTLEN, INTLEN> + Ord + IntMath + TryFrom<u64>
{
    fn byteord_to_sized(byteord: &ByteOrd) -> Result<SizedByteOrd<INTLEN>, String> {
        byteord_to_sized(byteord)
    }

    fn range_to_bitmask(range: &Range) -> Result<Self, String> {
        match range {
            Range::Float(_) => Err("$PnR is float for an integer column".to_string()),
            // TODO will this error ever happen?
            Range::Int(i) => Self::try_from(*i)
                .map(Self::next_power_2)
                .map_err(|_| "$PnR could not be converted to {INTLEN} bytes".to_string()),
        }
    }

    fn to_col_parser(
        range: &Range,
        byteord: &ByteOrd,
        total_events: usize,
    ) -> Result<IntColumnParser<Self, INTLEN>, Vec<String>> {
        // TODO be more specific, which means we need the measurement index
        let b = Self::range_to_bitmask(range);
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

trait FloatFromBytes<const LEN: usize>: NumProps<LEN> + OrderedFromBytes<LEN, LEN> + Clone
where
    Vec<Self>: Into<Series>,
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
        Ok(columns.into_iter().map(Vec::<Self>::into).collect())
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

impl MixedColumnType {
    fn into_series(self) -> Series {
        match self {
            MixedColumnType::Ascii(x) => Vec::<f64>::into(x.data),
            MixedColumnType::Single(x) => Vec::<f32>::into(x.data),
            MixedColumnType::Double(x) => Vec::<f64>::into(x.data),
            MixedColumnType::Uint(x) => x.into_series(),
        }
    }
}

impl AnyIntColumn {
    fn into_series(self) -> Series {
        match self {
            AnyIntColumn::Uint8(y) => Vec::<u8>::into(y.data),
            AnyIntColumn::Uint16(y) => Vec::<u16>::into(y.data),
            AnyIntColumn::Uint24(y) => Vec::<u32>::into(y.data),
            AnyIntColumn::Uint32(y) => Vec::<u32>::into(y.data),
            AnyIntColumn::Uint40(y) => Vec::<u64>::into(y.data),
            AnyIntColumn::Uint48(y) => Vec::<u64>::into(y.data),
            AnyIntColumn::Uint56(y) => Vec::<u64>::into(y.data),
            AnyIntColumn::Uint64(y) => Vec::<u64>::into(y.data),
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

#[derive(Debug)]
struct DataParser {
    column_parser: ColumnParser,
    begin: u64,
}

fn format_parsed_data(res: &FCSSuccess, delim: &str) -> Vec<String> {
    let shortnames = match &res.std {
        AnyCoreTEXT::FCS2_0(x) => x.get_shortnames(),
        AnyCoreTEXT::FCS3_0(x) => x.get_shortnames(),
        AnyCoreTEXT::FCS3_1(x) => x.get_shortnames(),
        AnyCoreTEXT::FCS3_2(x) => x.get_shortnames(),
    };
    if res.data.is_empty() {
        return vec![];
    }
    let mut buf = vec![];
    let mut lines = vec![];
    let nrows = res.data[0].len();
    let ncols = res.data.len();
    // ASSUME names is the same length as columns
    lines.push(shortnames.join(delim));
    for r in 0..nrows {
        buf.clear();
        for c in 0..ncols {
            buf.push(res.data[c].format(r));
        }
        lines.push(buf.join(delim));
    }
    lines
}

pub fn print_parsed_data(res: &FCSSuccess, delim: &str) {
    for x in format_parsed_data(res, delim) {
        println!("{}", x);
    }
}

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
        Ok(data.into_iter().map(Vec::<f64>::into).collect())
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
        Ok(data.into_iter().map(Vec::<f64>::into).collect())
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
    Ok(data.into_iter().map(Vec::<f64>::into).collect())
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

type MaybeKeyword = (&'static str, Option<String>);

type MaybeKeywords = Vec<MaybeKeyword>;

/// Used to hold critical lengths when calculating the pad for $BEGIN/ENDDATA.
struct KwLengths {
    /// Length of the entire DATA segment when written.
    data: usize,
    /// Length of all the measurement keywords in the TEXT segment.
    ///
    /// This is computed as the sum of all string lengths of each key and
    /// value plus 2*P (number of measurements) which captures the length
    /// of the two delimiters b/t the key and value and the key and previous
    /// value.
    measurements: usize,
}

fn sum_keywords(kws: &[MaybeKeyword]) -> usize {
    kws.iter()
        .map(|(k, v)| v.as_ref().map(|y| y.len() + k.len() + 2).unwrap_or(0))
        .sum()
}

fn n_digits(x: f64) -> f64 {
    // ASSUME this is effectively only going to be used on the u32 range
    // starting at 1; keep in f64 space to minimize casts
    f64::log10(x).floor() + 1.0
}

fn compute_data_offsets(textlen: u32, datalen: u32) -> (u32, u32) {
    let d = f64::from(datalen);
    let t = f64::from(textlen);
    let mut datastart = t;
    let mut dataend = datastart + d;
    let mut ndigits_start = n_digits(datastart);
    let mut ndigits_end = n_digits(dataend);
    let mut tmp_start;
    let mut tmp_end;
    loop {
        datastart = ndigits_start + ndigits_end + t;
        dataend = datastart + d;
        tmp_start = n_digits(datastart);
        tmp_end = n_digits(dataend);
        if tmp_start == ndigits_start && tmp_end == ndigits_end {
            return (datastart as u32, dataend as u32);
        } else {
            ndigits_start = tmp_start;
            ndigits_end = tmp_end;
        }
    }
}

// ASSUME header is always this length
const HEADER_LEN: usize = 58;

// length of BEGIN/ENDDATA keywords (without values), + 4 to account for
// delimiters
const DATALEN_NO_VAL: usize = BEGINDATA.len() + ENDDATA.len() + 4;

fn make_data_offset_keywords(other_textlen: usize, datalen: usize) -> [MaybeKeyword; 2] {
    // add everything up, + 1 at the end to account for the delimiter at
    // the end of TEXT
    let textlen = HEADER_LEN + DATALEN_NO_VAL + other_textlen + 1;
    let (datastart, dataend) = compute_data_offsets(textlen as u32, datalen as u32);
    [
        (BEGINDATA, Some(datastart.to_string())),
        (ENDDATA, Some(dataend.to_string())),
    ]
}

trait VersionedMetadata: Sized {
    type P: VersionedMeasurement;

    fn into_any(s: CoreTEXT<Self, Self::P>) -> AnyCoreTEXT;

    fn get_byteord(&self) -> ByteOrd;

    fn event_width(ms: &[MinimalMeasurement<Self::P>]) -> EventWidth {
        let (fixed, variable_indices): (Vec<_>, Vec<_>) = ms
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

    fn total_events(it: &IntermediateTEXT<Self, Self::P>, event_width: u32) -> PureSuccess<usize> {
        let def = PureErrorBuf::new();
        let nbytes = it.data_seg.nbytes();
        let remainder = nbytes % event_width;
        let total_events = nbytes / event_width;
        if nbytes % event_width > 0 {
            let msg = format!(
                "Events are {event_width} bytes wide, but this does not evenly \
                 divide DATA segment which is {nbytes} bytes long \
                 (remainder of {remainder})"
            );
            def.push_msg_leveled(msg, it.conf.raw.enfore_data_width_divisibility)
        }
        if let Some(tot) = it.metadata.specific.get_tot() {
            if total_events != tot {
                let msg = format!(
                    "$TOT field is {tot} but number of events \
                         that evenly fit into DATA is {total_events}"
                );
                def.push_msg_leveled(msg, it.conf.raw.enfore_matching_tot);
            }
        }
        // TODO fix cast
        PureSuccess {
            data: total_events as usize,
            deferred: def,
        }
    }

    fn build_int_parser(
        &self,
        ps: &[MinimalMeasurement<Self::P>],
        total_events: usize,
    ) -> PureMaybe<IntParser>;

    fn build_mixed_parser(
        &self,
        ps: &[MinimalMeasurement<Self::P>],
        dt: &AlphaNumType,
        total_events: usize,
    ) -> Option<PureMaybe<MixedParser>>;

    fn build_float_parser(
        &self,
        is_double: bool,
        par: usize,
        total_events: usize,
        ps: &[MinimalMeasurement<Self::P>],
    ) -> PureMaybe<ColumnParser> {
        let (bytes, dt) = if is_double { (8, "D") } else { (4, "F") };
        let remainder: Vec<_> = ps.iter().filter(|p| p.bytes_eq(bytes)).collect();
        if remainder.is_empty() {
            let byteord = &self.get_byteord();
            let res = if is_double {
                f64::make_matrix_parser(byteord, par, total_events).map(ColumnParser::Double)
            } else {
                f32::make_matrix_parser(byteord, par, total_events).map(ColumnParser::Single)
            };
            PureSuccess::from_result(res.map_err(|e| PureErrorBuf::from(e, PureErrorLevel::Error)))
        } else {
            let mut res = PureSuccess::from(None);
            for e in remainder.iter().enumerate().map(|(i, p)| {
                format!(
                    "Measurment {} uses {} bytes but DATATYPE={}",
                    i, p.bytes, dt
                )
            }) {
                res.push_error(e);
            }
            res
        }
    }

    fn build_fixed_width_parser(
        it: &IntermediateTEXT<Self, Self::P>,
        total_events: usize,
        measurement_widths: Vec<u8>,
    ) -> PureMaybe<ColumnParser> {
        let par = it.par;
        let ps = &it.measurements;
        let dt = &it.metadata.datatype;
        let specific = &it.metadata.specific;
        if let Some(mixed) = Self::build_mixed_parser(specific, ps, dt, total_events) {
            mixed.map(|c| c.map(ColumnParser::Mixed))
        } else {
            match dt {
                AlphaNumType::Single => specific.build_float_parser(false, par, total_events, ps),
                AlphaNumType::Double => specific.build_float_parser(true, par, total_events, ps),
                AlphaNumType::Integer => specific
                    .build_int_parser(ps, total_events)
                    .map(|c| c.map(ColumnParser::Int)),
                AlphaNumType::Ascii => {
                    PureSuccess::from(Some(ColumnParser::FixedWidthAscii(FixedAsciiParser {
                        columns: measurement_widths,
                        nrows: total_events,
                    })))
                }
            }
        }
    }

    fn build_delim_ascii_parser(
        it: &IntermediateTEXT<Self, Self::P>,
        tot: Option<usize>,
    ) -> ColumnParser {
        let nbytes = it.data_seg.num_bytes();
        ColumnParser::DelimitedAscii(DelimAsciiParser {
            ncols: it.par,
            nrows: tot,
            nbytes: nbytes as usize,
        })
    }

    fn build_column_parser(it: &IntermediateTEXT<Self, Self::P>) -> PureMaybe<ColumnParser> {
        // In order to make a data parser, the $DATATYPE, $BYTEORD, $PnB, and
        // $PnDATATYPE (if present) all need to be a specific relationship to
        // each other, each of which corresponds to the options below.
        let mut res = match (Self::event_width(&it.measurements), it.metadata.datatype) {
            // Numeric/Ascii (fixed width)
            (EventWidth::Finite(measurement_widths), _) => {
                let event_width = measurement_widths.iter().map(|x| u32::from(*x)).sum();
                Self::total_events(it, event_width).and_then(|total_events| {
                    Self::build_fixed_width_parser(it, total_events, measurement_widths)
                })
            }
            // Ascii (variable width)
            (EventWidth::Variable, AlphaNumType::Ascii) => {
                let tot = it.metadata.specific.get_tot();
                PureSuccess::from(Some(Self::build_delim_ascii_parser(
                    it,
                    tot.map(|x| x as usize),
                )))
            }
            // nonsense...scream at user
            (EventWidth::Error(fixed, variable), _) => {
                let mut r = PureSuccess::from(None);
                r.push_error("$PnBs are a mix of numeric and variable".to_string());
                for f in fixed {
                    r.push_error(format!("$PnB for measurement {f} is numeric"));
                }
                for v in variable {
                    r.push_error(format!("$PnB for measurement {v} is variable"));
                }
                r
            }
            (EventWidth::Variable, dt) => {
                let mut r = PureSuccess::from(None);
                r.push_error(format!("$DATATYPE is {dt} but all $PnB are '*'"));
                r
            }
        };
        if it.metadata.datatype == AlphaNumType::Ascii && Self::P::fcs_version() >= Version::FCS3_1
        {
            res.push_error("$DATATYPE=A has been deprecated since FCS 3.1".to_string());
        }
        res
    }

    fn build_data_parser(it: &IntermediateTEXT<Self, Self::P>) -> PureMaybe<DataParser> {
        Self::build_column_parser(it).map(|c| {
            c.map(|column_parser| DataParser {
                column_parser,
                begin: u64::from(it.data_seg.begin),
            })
        })
    }

    fn lookup_specific(st: &mut KwState, par: usize, names: &HashSet<&str>) -> Option<Self>;

    fn lookup_metadata(st: &mut KwState, ms: &[Measurement<Self::P>]) -> Option<Metadata<Self>> {
        let names: HashSet<_> = ms
            .iter()
            .filter_map(|m| Self::P::measurement_name(m))
            .collect();
        let par = ms.len();
        let maybe_datatype = st.lookup_datatype();
        let maybe_specific = Self::lookup_specific(st, par, &names);
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
                tr: st.lookup_trigger_checked(&names),
                nonstandard_keywords: st.lookup_nonstandard(),
                specific,
            })
        } else {
            None
        }
    }

    fn keywords_inner(&self, other_textlen: usize, data_len: usize) -> MaybeKeywords;

    fn keywords(m: &Metadata<Self>, par: usize, tot: usize, len: KwLengths) -> MaybeKeywords {
        let fixed = [
            (PAR, Some(par.to_string())),
            (TOT, Some(tot.to_string())),
            (NEXTDATA, Some("0".to_string())),
            (DATATYPE, Some(m.datatype.to_string())),
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
        ];
        let fixed_len = sum_keywords(&fixed) + len.measurements;
        fixed
            .into_iter()
            .chain(m.specific.keywords_inner(fixed_len, len.data))
            .collect()
    }
}

fn build_int_parser_2_0<P: VersionedMeasurement>(
    byteord: &ByteOrd,
    ps: &[MinimalMeasurement<P>],
    total_events: usize,
) -> PureMaybe<IntParser> {
    let nbytes = byteord.num_bytes();
    let remainder: Vec<_> = ps.iter().filter(|p| !p.bytes_eq(nbytes)).collect();
    if remainder.is_empty() {
        let (columns, fail): (Vec<_>, Vec<_>) = ps
            .iter()
            .map(|p| p.make_int_parser(byteord, total_events))
            .partition_result();
        let errors: Vec<_> = fail.into_iter().flatten().collect();
        if errors.is_empty() {
            PureSuccess::from(Some(IntParser {
                columns,
                nrows: total_events,
            }))
        } else {
            let mut res = PureSuccess::from(None);
            for e in errors.into_iter() {
                res.push_error(e);
            }
            res
        }
    } else {
        let mut res = PureSuccess::from(None);
        for e in remainder.iter().enumerate().map(|(i, p)| {
            format!(
                "Measurement {} uses {} bytes when DATATYPE=I \
                         and BYTEORD implies {} bytes",
                i, p.bytes, nbytes
            )
        }) {
            res.push_error(e);
        }
        res
    }
}

impl VersionedMetadata for InnerMetadata2_0 {
    type P = InnerMeasurement2_0;

    fn into_any(t: CoreText2_0) -> AnyCoreTEXT {
        AnyCoreTEXT::FCS2_0(Box::new(t))
    }

    // fn get_data_offsets(s: &StdText<Self, Self::P, Self::R>) -> Offsets {
    //     s.data_offsets
    // }

    fn get_byteord(&self) -> ByteOrd {
        self.byteord.clone()
    }

    fn build_int_parser(
        &self,
        ps: &[MinimalMeasurement<Self::P>],
        total_events: usize,
    ) -> PureMaybe<IntParser> {
        build_int_parser_2_0(&self.byteord, ps, total_events)
    }

    fn build_mixed_parser(
        &self,
        _: &[MinimalMeasurement<Self::P>],
        _: &AlphaNumType,
        _: usize,
    ) -> Option<PureMaybe<MixedParser>> {
        None
    }

    fn lookup_specific(
        st: &mut KwState,
        par: usize,
        _: &HashSet<&str>,
    ) -> Option<InnerMetadata2_0> {
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

    fn keywords_inner(&self, _: usize, _: usize) -> MaybeKeywords {
        [
            (MODE, Some(self.mode.to_string())),
            (BYTEORD, Some(self.byteord.to_string())),
            (CYT, self.cyt.as_opt_string()),
            (COMP, self.comp.as_opt_string()),
            (BTIM, self.timestamps.btim.as_opt_string()),
            (ETIM, self.timestamps.etim.as_opt_string()),
            (DATE, self.timestamps.date.as_opt_string()),
        ]
        .into_iter()
        .collect()
    }
}

impl VersionedMetadata for InnerMetadata3_0 {
    type P = InnerMeasurement3_0;

    fn into_any(t: CoreText3_0) -> AnyCoreTEXT {
        AnyCoreTEXT::FCS3_0(Box::new(t))
    }

    fn get_byteord(&self) -> ByteOrd {
        self.byteord.clone()
    }

    fn build_int_parser(
        &self,
        ps: &[MinimalMeasurement<Self::P>],
        total_events: usize,
    ) -> PureMaybe<IntParser> {
        build_int_parser_2_0(&self.byteord, ps, total_events)
    }

    fn build_mixed_parser(
        &self,
        _: &[MinimalMeasurement<Self::P>],
        _: &AlphaNumType,
        _: usize,
    ) -> Option<PureMaybe<MixedParser>> {
        None
    }

    fn lookup_specific(
        st: &mut KwState,
        _: usize,
        names: &HashSet<&str>,
    ) -> Option<InnerMetadata3_0> {
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
                timestep: st.lookup_timestep_checked(names),
                unicode: st.lookup_unicode(),
            })
        } else {
            None
        }
    }

    fn keywords_inner(&self, other_textlen: usize, data_len: usize) -> MaybeKeywords {
        let ts = &self.timestamps;
        // TODO set analysis and stext if we have anything
        let zero = Some("0".to_string());
        let kws = [
            (BEGINANALYSIS, zero.clone()),
            (ENDANALYSIS, zero.clone()),
            (BEGINSTEXT, zero.clone()),
            (ENDSTEXT, zero.clone()),
            (MODE, Some(self.mode.to_string())),
            (BYTEORD, Some(self.byteord.to_string())),
            (CYT, self.cyt.as_opt_string()),
            (COMP, self.comp.as_opt_string()),
            (BTIM, ts.btim.as_opt_string()),
            (ETIM, ts.etim.as_opt_string()),
            (DATE, ts.date.as_opt_string()),
            (CYTSN, self.cytsn.as_opt_string()),
            (TIMESTEP, self.timestep.as_opt_string()),
            (UNICODE, self.unicode.as_opt_string()),
        ];
        let text_len = other_textlen + sum_keywords(&kws);
        make_data_offset_keywords(text_len, data_len)
            .into_iter()
            .chain(kws)
            .collect()
    }
}

impl VersionedMetadata for InnerMetadata3_1 {
    type P = InnerMeasurement3_1;

    fn into_any(t: CoreText3_1) -> AnyCoreTEXT {
        AnyCoreTEXT::FCS3_1(Box::new(t))
    }

    fn get_byteord(&self) -> ByteOrd {
        ByteOrd::Endian(self.byteord)
    }

    fn build_int_parser(
        &self,
        ps: &[MinimalMeasurement<Self::P>],
        total_events: usize,
    ) -> PureMaybe<IntParser> {
        build_int_parser_2_0(&ByteOrd::Endian(self.byteord), ps, total_events)
    }

    fn build_mixed_parser(
        &self,
        _: &[MinimalMeasurement<Self::P>],
        _: &AlphaNumType,
        _: usize,
    ) -> Option<PureMaybe<MixedParser>> {
        None
    }

    fn lookup_specific(
        st: &mut KwState,
        _: usize,
        names: &HashSet<&str>,
    ) -> Option<InnerMetadata3_1> {
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
                spillover: st.lookup_spillover_checked(names),
                timestamps: st.lookup_timestamps3_1(false),
                cytsn: st.lookup_cytsn(),
                timestep: st.lookup_timestep_checked(names),
                modification: st.lookup_modification(),
                plate: st.lookup_plate(false),
                vol: st.lookup_vol(),
            })
        } else {
            None
        }
    }

    fn keywords_inner(&self, other_textlen: usize, data_len: usize) -> MaybeKeywords {
        let mdn = &self.modification;
        let ts = &self.timestamps;
        let pl = &self.plate;
        // TODO set analysis and stext if we have anything
        let zero = Some("0".to_string());
        let fixed = [
            (BEGINANALYSIS, zero.clone()),
            (ENDANALYSIS, zero.clone()),
            (BEGINSTEXT, zero.clone()),
            (ENDSTEXT, zero.clone()),
            (MODE, Some(self.mode.to_string())),
            (BYTEORD, Some(self.byteord.to_string())),
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
        ];
        let text_len = sum_keywords(&fixed) + other_textlen;
        make_data_offset_keywords(text_len, data_len)
            .into_iter()
            .chain(fixed)
            .collect()
    }
}

impl VersionedMetadata for InnerMetadata3_2 {
    type P = InnerMeasurement3_2;

    fn into_any(t: CoreText3_2) -> AnyCoreTEXT {
        AnyCoreTEXT::FCS3_2(Box::new(t))
    }

    fn get_byteord(&self) -> ByteOrd {
        ByteOrd::Endian(self.byteord)
    }

    fn build_int_parser(
        &self,
        ps: &[MinimalMeasurement<Self::P>],
        total_events: usize,
    ) -> PureMaybe<IntParser> {
        build_int_parser_2_0(&ByteOrd::Endian(self.byteord), ps, total_events)
    }

    fn build_mixed_parser(
        &self,
        ps: &[MinimalMeasurement<Self::P>],
        dt: &AlphaNumType,
        total_events: usize,
    ) -> Option<PureMaybe<MixedParser>> {
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
            Some(PureSuccess::from(Some(MixedParser {
                nrows: total_events,
                columns: pass,
            })))
        } else {
            // TODO uncanny strange deja vu
            let mut res = PureSuccess::from(None);
            for e in fail.into_iter().flatten() {
                res.push_error(e);
            }
            Some(res)
        }
    }

    fn lookup_specific(
        st: &mut KwState,
        _: usize,
        names: &HashSet<&str>,
    ) -> Option<InnerMetadata3_2> {
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
                spillover: st.lookup_spillover_checked(names),
                timestamps: st.lookup_timestamps3_1(true),
                cytsn: st.lookup_cytsn(),
                timestep: st.lookup_timestep_checked(names),
                modification: st.lookup_modification(),
                plate: st.lookup_plate(true),
                vol: st.lookup_vol(),
                carrier: st.lookup_carrier(),
                datetimes: st.lookup_datetimes(),
                unstained: st.lookup_unstained(names),
                flowrate: st.lookup_flowrate(),
            })
        } else {
            None
        }
    }

    fn keywords_inner(&self, other_textlen: usize, data_len: usize) -> MaybeKeywords {
        let mdn = &self.modification;
        let ts = &self.timestamps;
        let pl = &self.plate;
        let car = &self.carrier;
        let dt = &self.datetimes;
        let us = &self.unstained;
        // TODO set analysis and stext if we have anything
        // let zero = Some("0".to_string());
        let fixed = [
            // (BEGINANALYSIS, zero.clone()),
            // (ENDANALYSIS, zero.clone()),
            // (BEGINSTEXT, zero.clone()),
            // (ENDSTEXT, zero.clone()),
            (BYTEORD, Some(self.byteord.to_string())),
            (CYT, Some(self.cyt.to_string())),
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
        ];
        let text_len = sum_keywords(&fixed) + other_textlen;
        make_data_offset_keywords(text_len, data_len)
            .into_iter()
            .chain(fixed)
            .collect()
    }
}

fn parse_raw_text(
    version: Version,
    kws: RawKeywords,
    conf: &StdTextReader,
) -> PureResult<(AnyCoreTEXT, RawKeywords)> {
    match version {
        Version::FCS2_0 => CoreText2_0::any_from_raw(kws, conf),
        Version::FCS3_0 => CoreText3_0::any_from_raw(kws, conf),
        Version::FCS3_1 => CoreText3_1::any_from_raw(kws, conf),
        Version::FCS3_2 => CoreText3_2::any_from_raw(kws, conf),
    }
}

impl NonStdKey {
    fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Eq, PartialEq)]
enum ValueStatus {
    Raw,
    Error(PureError),
    Used,
}

struct KwValue {
    value: String,
    status: ValueStatus,
}

// all hail the almighty state monad :D

struct KwState<'a> {
    raw_keywords: HashMap<String, KwValue>,
    deferred: PureErrorBuf,
    conf: &'a StdTextReader,
}

// struct DataParserState<'a> {
//     deferred: PureErrorBuf,
//     // std_errors: StdTEXTErrors,
//     conf: &'a StdTextReader,
// }

// #[derive(Debug, Clone)]
// pub struct StdTEXTErrors {
//     /// Required keywords that are missing
//     missing_keywords: Vec<StdKey>,

//     /// Errors that pertain to one keyword value
//     keyword_errors: Vec<KeyError>,

//     /// Errors involving multiple keywords, like PnB not matching DATATYPE
//     meta_errors: Vec<String>,

//     /// Nonstandard keys starting with "$". Error status depends on configuration.
//     deviant_keywords: StdKeywords,

//     /// Nonstandard keys. Error status depends on configuration.
//     nonstandard_keywords: NonStdKeywords,

//     /// Keywords that are deprecated. Error status depends on configuration.
//     deprecated_keys: Vec<StdKey>,

//     /// Features that are deprecated. Error status depends on configuration.
//     deprecated_features: Vec<String>,

//     /// Non-keyword warnings. Error status depends on configuration.
//     meta_warnings: Vec<String>,

//     /// Keyword warnings. Error status depends on configuration.
//     keyword_warnings: Vec<KeyWarning>,
// }

// impl StdTEXTErrors {
//     fn prune_errors(&mut self, conf: &StdTextReader) {
//         if !conf.disallow_deviant {
//             self.deviant_keywords.clear();
//         };
//         if !conf.disallow_nonstandard {
//             self.nonstandard_keywords.clear();
//         }
//         if !conf.disallow_deprecated {
//             self.deprecated_keys.clear();
//             self.deprecated_features.clear();
//         };
//         if !conf.warnings_are_errors {
//             self.meta_warnings.clear();
//             self.keyword_warnings.clear();
//         };
//     }

//     fn into_lines(self) -> Vec<String> {
//         let ks = self
//             .missing_keywords
//             .into_iter()
//             .map(|s| format!("Required keyword is missing: {}", s.0));
//         let vs = self.keyword_errors.into_iter().map(|e| {
//             format!(
//                 "Could not get value for {}. Error was '{}'. Value was '{}'.",
//                 e.key.0, e.msg, e.value
//             )
//         });
//         // TODO add lots of other printing stuff here
//         ks.chain(vs).chain(self.meta_errors).collect()
//     }

//     pub fn print(self) {
//         for e in self.into_lines() {
//             eprintln!("ERROR: {e}");
//         }
//     }
// }

#[derive(Debug, Clone)]
struct KeyError {
    key: StdKey,
    value: String,
    msg: String,
}

// #[derive(Debug, Clone, Serialize)]
// struct KeyWarning {
//     key: StdKey,
//     value: String,
//     msg: String,
// }

impl<'a> KwState<'a> {
    fn from(kws: RawKeywords, conf: &'a StdTextReader) -> Self {
        KwState {
            raw_keywords: kws
                .into_iter()
                .map(|(key, value)| {
                    (
                        key,
                        KwValue {
                            value,
                            status: ValueStatus::Raw,
                        },
                    )
                })
                .collect(),
            deferred: PureErrorBuf::new(),
            conf,
        }
    }

    // TODO not DRY (although will likely need HKTs)
    fn lookup_required<V: FromStr>(&mut self, k: &str, dep: bool) -> Option<V>
    where
        <V as FromStr>::Err: fmt::Display,
    {
        match self.raw_keywords.get_mut(k) {
            Some(v) => match v.status {
                ValueStatus::Raw => {
                    let (s, r) = v.value.parse().map_or_else(
                        |e| {
                            (
                                // TODO this will be awkward for keys with very
                                // large values
                                ValueStatus::Error(PureError {
                                    msg: format!("{e} for key '{k}' with value '{}'", v.value),
                                    level: PureErrorLevel::Error,
                                }),
                                None,
                            )
                        },
                        |x| (ValueStatus::Used, Some(x)),
                    );
                    if dep {
                        self.deferred.push_msg_leveled(
                            format!("deprecated key: {k}"),
                            self.conf.disallow_deprecated,
                        );
                    }
                    v.status = s;
                    r
                }
                _ => None,
            },
            None => {
                self.deferred
                    .push_error(format!("missing required key: {k}"));
                None
            }
        }
    }

    fn lookup_optional<V: FromStr>(&mut self, k: &str, dep: bool) -> OptionalKw<V>
    where
        <V as FromStr>::Err: fmt::Display,
    {
        match self.raw_keywords.get_mut(k) {
            Some(v) => match v.status {
                ValueStatus::Raw => {
                    let (s, r) = v.value.parse().map_or_else(
                        |w| {
                            (
                                ValueStatus::Error(PureError {
                                    msg: format!("{}", w),
                                    level: PureErrorLevel::Warning,
                                }),
                                Absent,
                            )
                        },
                        |x| (ValueStatus::Used, OptionalKw::Present(x)),
                    );
                    if dep {
                        self.deferred.push_msg_leveled(
                            format!("deprecated key: {k}"),
                            self.conf.disallow_deprecated,
                        );
                    }
                    v.status = s;
                    r
                }
                _ => Absent,
            },
            None => Absent,
        }
    }

    // metadata

    fn lookup_byteord(&mut self) -> Option<ByteOrd> {
        self.lookup_required(BYTEORD, false)
    }

    fn lookup_endian(&mut self) -> Option<Endian> {
        self.lookup_required(BYTEORD, false)
    }

    fn lookup_datatype(&mut self) -> Option<AlphaNumType> {
        self.lookup_required(DATATYPE, false)
    }

    fn lookup_mode(&mut self) -> Option<Mode> {
        self.lookup_required(MODE, false)
    }

    fn lookup_mode3_2(&mut self) -> OptionalKw<Mode3_2> {
        self.lookup_optional(MODE, true)
    }

    fn lookup_nextdata(&mut self) -> Option<u32> {
        self.lookup_required(NEXTDATA, false)
    }

    fn lookup_par(&mut self) -> Option<usize> {
        self.lookup_required(PAR, false)
    }

    fn lookup_tot_req(&mut self) -> Option<u32> {
        self.lookup_required(TOT, false)
    }

    fn lookup_tot_opt(&mut self) -> OptionalKw<u32> {
        self.lookup_optional(TOT, false)
    }

    fn lookup_cyt_req(&mut self) -> Option<String> {
        self.lookup_required(CYT, false)
    }

    fn lookup_cyt_opt(&mut self) -> OptionalKw<String> {
        self.lookup_optional(CYT, false)
    }

    fn lookup_abrt(&mut self) -> OptionalKw<u32> {
        self.lookup_optional(ABRT, false)
    }

    fn lookup_cells(&mut self) -> OptionalKw<String> {
        self.lookup_optional(CELLS, false)
    }

    fn lookup_com(&mut self) -> OptionalKw<String> {
        self.lookup_optional(COM, false)
    }

    fn lookup_exp(&mut self) -> OptionalKw<String> {
        self.lookup_optional(EXP, false)
    }

    fn lookup_fil(&mut self) -> OptionalKw<String> {
        self.lookup_optional(FIL, false)
    }

    fn lookup_inst(&mut self) -> OptionalKw<String> {
        self.lookup_optional(INST, false)
    }

    fn lookup_lost(&mut self) -> OptionalKw<u32> {
        self.lookup_optional(LOST, false)
    }

    fn lookup_op(&mut self) -> OptionalKw<String> {
        self.lookup_optional(OP, false)
    }

    fn lookup_proj(&mut self) -> OptionalKw<String> {
        self.lookup_optional(PROJ, false)
    }

    fn lookup_smno(&mut self) -> OptionalKw<String> {
        self.lookup_optional(SMNO, false)
    }

    fn lookup_src(&mut self) -> OptionalKw<String> {
        self.lookup_optional(SRC, false)
    }

    fn lookup_sys(&mut self) -> OptionalKw<String> {
        self.lookup_optional(SYS, false)
    }

    fn lookup_trigger(&mut self) -> OptionalKw<Trigger> {
        self.lookup_optional(TR, false)
    }

    fn lookup_trigger_checked(&mut self, names: &HashSet<&str>) -> OptionalKw<Trigger> {
        if let Present(tr) = self.lookup_trigger() {
            let p = tr.measurement.0.as_str();
            if names.contains(p) {
                self.push_meta_error(format!(
                    "$TRIGGER refers to non-existent measurements '{p}'",
                ));
                Absent
            } else {
                Present(tr)
            }
        } else {
            Absent
        }
    }

    fn lookup_cytsn(&mut self) -> OptionalKw<String> {
        self.lookup_optional(CYTSN, false)
    }

    fn lookup_timestep(&mut self) -> OptionalKw<f32> {
        self.lookup_optional(TIMESTEP, false)
    }

    fn lookup_timestep_checked(&mut self, names: &HashSet<&str>) -> OptionalKw<f32> {
        let ts = self.lookup_timestep();
        if let Some(time_name) = &self.conf.time_shortname {
            if names.contains(time_name.as_str()) && ts == Absent {
                self.push_meta_error_or_warning(
                    self.conf.ensure_time_timestep,
                    String::from("$TIMESTEP must be present if time channel given"),
                )
            }
        }
        ts
    }

    fn lookup_vol(&mut self) -> OptionalKw<f32> {
        self.lookup_optional(VOL, false)
    }

    fn lookup_flowrate(&mut self) -> OptionalKw<String> {
        self.lookup_optional(FLOWRATE, false)
    }

    fn lookup_unicode(&mut self) -> OptionalKw<Unicode> {
        // TODO actually verify that these are real keywords, although this
        // doesn't matter too much since we are going to parse TEXT as utf8
        // anyways since we can, so this keywords isn't that useful.
        self.lookup_optional(UNICODE, false)
    }

    fn lookup_plateid(&mut self, dep: bool) -> OptionalKw<String> {
        self.lookup_optional(PLATEID, dep)
    }

    fn lookup_platename(&mut self, dep: bool) -> OptionalKw<String> {
        self.lookup_optional(PLATENAME, dep)
    }

    fn lookup_wellid(&mut self, dep: bool) -> OptionalKw<String> {
        self.lookup_optional(WELLID, dep)
    }

    fn lookup_unstainedinfo(&mut self) -> OptionalKw<String> {
        self.lookup_optional(UNSTAINEDINFO, false)
    }

    fn lookup_unstainedcenters(&mut self) -> OptionalKw<UnstainedCenters> {
        self.lookup_optional(UNSTAINEDCENTERS, false)
    }

    fn lookup_unstainedcenters_checked(
        &mut self,
        names: &HashSet<&str>,
    ) -> OptionalKw<UnstainedCenters> {
        if let Present(u) = self.lookup_unstainedcenters() {
            let noexist: Vec<_> =
                u.0.keys()
                    .filter(|m| !names.contains(m.0.as_str()))
                    .collect();
            if !noexist.is_empty() {
                let msg = format!(
                    "$UNSTAINEDCENTERS refers to non-existent measurements: {}",
                    noexist.iter().join(","),
                );
                self.push_meta_error(msg);
                Absent
            } else {
                Present(u)
            }
        } else {
            Absent
        }
    }

    fn lookup_last_modifier(&mut self) -> OptionalKw<String> {
        self.lookup_optional(LAST_MODIFIER, false)
    }

    fn lookup_last_modified(&mut self) -> OptionalKw<ModifiedDateTime> {
        self.lookup_optional(LAST_MODIFIED, false)
    }

    fn lookup_originality(&mut self) -> OptionalKw<Originality> {
        self.lookup_optional(ORIGINALITY, false)
    }

    fn lookup_carrierid(&mut self) -> OptionalKw<String> {
        self.lookup_optional(CARRIERID, false)
    }

    fn lookup_carriertype(&mut self) -> OptionalKw<String> {
        self.lookup_optional(CARRIERTYPE, false)
    }

    fn lookup_locationid(&mut self) -> OptionalKw<String> {
        self.lookup_optional(LOCATIONID, false)
    }

    fn lookup_begindatetime(&mut self) -> OptionalKw<FCSDateTime> {
        self.lookup_optional(BEGINDATETIME, false)
    }

    fn lookup_enddatetime(&mut self) -> OptionalKw<FCSDateTime> {
        self.lookup_optional(ENDDATETIME, false)
    }

    fn lookup_date(&mut self, dep: bool) -> OptionalKw<FCSDate> {
        self.lookup_optional(DATE, dep)
    }

    fn lookup_btim(&mut self) -> OptionalKw<FCSTime> {
        self.lookup_optional(BTIM, false)
    }

    fn lookup_etim(&mut self) -> OptionalKw<FCSTime> {
        self.lookup_optional(ETIM, false)
    }

    fn lookup_btim60(&mut self) -> OptionalKw<FCSTime60> {
        self.lookup_optional(BTIM, false)
    }

    fn lookup_etim60(&mut self) -> OptionalKw<FCSTime60> {
        self.lookup_optional(ETIM, false)
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
        // TODO make flag to enforce this as an error or warning
        if let (Present(b), Present(e)) = (&begin, &end) {
            if e.0 < b.0 {
                self.push_meta_warning_str("$BEGINDATETIME is after $ENDDATETIME");
            }
        }
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

    fn lookup_unstained(&mut self, names: &HashSet<&str>) -> UnstainedData {
        UnstainedData {
            unstainedcenters: self.lookup_unstainedcenters_checked(names),
            unstainedinfo: self.lookup_unstainedinfo(),
        }
    }

    fn lookup_compensation_2_0(&mut self, par: usize) -> OptionalKw<Compensation> {
        let mut matrix: Vec<_> = iter::repeat_with(|| vec![0.0; par]).take(par).collect();
        // column = src channel
        // row = target channel
        // These are "flipped" in 2.0, where "column" goes TO the "row"
        let mut any_error = false;
        for r in 0..par {
            for c in 0..par {
                let m = format!("DFC{c}TO{r}");
                if let Present(x) = self.lookup_optional(m.as_str(), false) {
                    matrix[r][c] = x;
                } else {
                    any_error = true;
                }
            }
        }
        if any_error {
            Absent
        } else {
            Present(Compensation { matrix })
        }
    }

    fn lookup_compensation_3_0(&mut self) -> OptionalKw<Compensation> {
        self.lookup_optional(COMP, false)
    }

    fn lookup_spillover(&mut self) -> OptionalKw<Spillover> {
        self.lookup_optional(SPILLOVER, false)
    }

    // TODO this is basically the same as unstained centers
    fn lookup_spillover_checked(&mut self, names: &HashSet<&str>) -> OptionalKw<Spillover> {
        if let Present(s) = self.lookup_spillover() {
            let noexist: Vec<_> = s
                .measurements
                .iter()
                .filter(|m| !names.contains(m.0.as_str()))
                .collect();
            if !noexist.is_empty() {
                let msg = format!(
                    "$SPILLOVER refers to non-existent measurements: {}",
                    noexist.iter().join(", ")
                );
                self.push_meta_error(msg);
            }

            Present(s)
        } else {
            Absent
        }
    }

    fn lookup_nonstandard(&mut self) -> NonStdKeywords {
        let mut ns = HashMap::new();
        self.raw_keywords.retain(|k, v| {
            if v.value.starts_with("$") && v.status == ValueStatus::Raw {
                true
            } else {
                ns.insert(NonStdKey(k.clone()), v.value.clone());
                false
            }
        });
        ns
    }

    // measurements

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

    // this reads the PnB field which has "bits" in it, but as far as I know
    // nobody is using anything other than evenly-spaced bytes
    fn lookup_meas_bytes(&mut self, n: usize) -> Option<Bytes> {
        self.lookup_meas_req(BYTES_SFX, n, false)
    }

    fn lookup_meas_range(&mut self, n: usize) -> Option<Range> {
        self.lookup_meas_req(RANGE_SFX, n, false)
    }

    fn lookup_meas_wavelength(&mut self, n: usize) -> OptionalKw<u32> {
        self.lookup_meas_opt(WAVELEN_SFX, n, false)
    }

    fn lookup_meas_power(&mut self, n: usize) -> OptionalKw<u32> {
        self.lookup_meas_opt(POWER_SFX, n, false)
    }

    fn lookup_meas_detector_type(&mut self, n: usize) -> OptionalKw<String> {
        self.lookup_meas_opt(DET_TYPE_SFX, n, false)
    }

    fn lookup_meas_shortname_req(&mut self, n: usize) -> Option<Shortname> {
        self.lookup_meas_req(SHORTNAME_SFX, n, false)
    }

    fn lookup_meas_shortname_opt(&mut self, n: usize) -> OptionalKw<Shortname> {
        self.lookup_meas_opt(SHORTNAME_SFX, n, false)
    }

    fn lookup_meas_longname(&mut self, n: usize) -> OptionalKw<String> {
        self.lookup_meas_opt(LONGNAME_SFX, n, false)
    }

    fn lookup_meas_filter(&mut self, n: usize) -> OptionalKw<String> {
        self.lookup_meas_opt(FILTER_SFX, n, false)
    }

    fn lookup_meas_percent_emitted(&mut self, n: usize, dep: bool) -> OptionalKw<u32> {
        self.lookup_meas_opt(PCNT_EMT_SFX, n, dep)
    }

    fn lookup_meas_detector_voltage(&mut self, n: usize) -> OptionalKw<f32> {
        self.lookup_meas_opt(DET_VOLT_SFX, n, false)
    }

    fn lookup_meas_detector(&mut self, n: usize) -> OptionalKw<String> {
        self.lookup_meas_opt(DET_NAME_SFX, n, false)
    }

    fn lookup_meas_tag(&mut self, n: usize) -> OptionalKw<String> {
        self.lookup_meas_opt(TAG_SFX, n, false)
    }

    fn lookup_meas_analyte(&mut self, n: usize) -> OptionalKw<String> {
        self.lookup_meas_opt(ANALYTE_SFX, n, false)
    }

    fn lookup_meas_gain(&mut self, n: usize) -> OptionalKw<f32> {
        self.lookup_meas_opt(GAIN_SFX, n, false)
    }

    fn lookup_meas_gain_timecheck(&mut self, n: usize, name: &Shortname) -> OptionalKw<f32> {
        let gain = self.lookup_meas_gain(n);
        if let Present(g) = &gain {
            if self.conf.time_name_matches(name) && *g != 1.0 {
                if self.conf.ensure_time_nogain {
                    self.push_meta_error(String::from("Time channel must not have $PnG"));
                } else {
                    self.push_meta_warning(String::from(
                        "Time channel should not have $PnG, dropping $PnG",
                    ));
                }
                Absent
            } else {
                gain
            }
        } else {
            gain
        }
    }

    fn lookup_meas_gain_timecheck_opt(
        &mut self,
        n: usize,
        name: &OptionalKw<Shortname>,
    ) -> OptionalKw<f32> {
        if let Present(x) = name {
            self.lookup_meas_gain_timecheck(n, x)
        } else {
            self.lookup_meas_gain(n)
        }
    }

    fn lookup_meas_scale_req(&mut self, n: usize) -> Option<Scale> {
        self.lookup_meas_req(SCALE_SFX, n, false)
    }

    fn lookup_meas_scale_timecheck(&mut self, n: usize, name: &Shortname) -> Option<Scale> {
        let scale = self.lookup_meas_scale_req(n);
        if let Some(s) = &scale {
            if self.conf.time_name_matches(name)
                && *s != Scale::Linear
                && self.conf.ensure_time_linear
            {
                self.push_meta_error(String::from("Time channel must have linear $PnE"));
                None
            } else {
                scale
            }
        } else {
            scale
        }
    }

    fn lookup_meas_scale_timecheck_opt(
        &mut self,
        n: usize,
        name: &OptionalKw<Shortname>,
    ) -> Option<Scale> {
        if let Present(x) = name {
            self.lookup_meas_scale_timecheck(n, x)
        } else {
            self.lookup_meas_scale_req(n)
        }
    }

    fn lookup_meas_scale_opt(&mut self, n: usize) -> OptionalKw<Scale> {
        self.lookup_meas_opt(SCALE_SFX, n, false)
    }

    fn lookup_meas_calibration3_1(&mut self, n: usize) -> OptionalKw<Calibration3_1> {
        self.lookup_meas_opt(CALIBRATION_SFX, n, false)
    }

    fn lookup_meas_calibration3_2(&mut self, n: usize) -> OptionalKw<Calibration3_2> {
        self.lookup_meas_opt(CALIBRATION_SFX, n, false)
    }

    // for 3.1+ PnL measurements, which can have multiple wavelengths
    fn lookup_meas_wavelengths(&mut self, n: usize) -> OptionalKw<Wavelengths> {
        self.lookup_meas_opt(WAVELEN_SFX, n, false)
    }

    fn lookup_meas_display(&mut self, n: usize) -> OptionalKw<Display> {
        self.lookup_meas_opt(DISPLAY_SFX, n, false)
    }

    fn lookup_meas_datatype(&mut self, n: usize) -> OptionalKw<NumType> {
        self.lookup_meas_opt(DATATYPE_SFX, n, false)
    }

    fn lookup_meas_type(&mut self, n: usize) -> OptionalKw<MeasurementType> {
        self.lookup_meas_opt(DET_TYPE_SFX, n, false)
    }

    fn lookup_meas_feature(&mut self, n: usize) -> OptionalKw<Feature> {
        self.lookup_meas_opt(FEATURE_SFX, n, false)
    }

    /// Find nonstandard keys that are specific for a given measurement
    fn lookup_meas_nonstandard(&mut self, n: usize) -> NonStdKeywords {
        let mut ns = HashMap::new();
        // ASSUME the pattern does not start with "$" and has a %n which will be
        // subbed for the measurement index. The pattern will then be turned
        // into a legit rust regular expression, which may fail depending on
        // what %n does, so check it each time.
        if let Some(p) = &self.conf.nonstandard_measurement_pattern {
            let rep = p.replace("%n", n.to_string().as_str());
            if let Ok(pattern) = Regex::new(rep.as_str()) {
                self.raw_keywords.retain(|k, v| {
                    if pattern.is_match(k.as_str()) {
                        ns.insert(NonStdKey(k.clone()), v.value.clone());
                        false
                    } else {
                        true
                    }
                });
            } else {
                self.push_meta_warning(format!(
                    "Could not make regular expression using \
                     pattern '{rep}' for measurement {n}"
                ));
            }
        }
        ns
    }

    fn push_meta_error_str(&mut self, msg: &str) {
        self.push_meta_error(String::from(msg));
    }

    fn push_meta_error(&mut self, msg: String) {
        self.deferred.push_error(msg);
    }

    fn push_meta_warning_str(&mut self, msg: &str) {
        self.push_meta_warning(String::from(msg));
    }

    fn push_meta_warning(&mut self, msg: String) {
        self.deferred.push_warning(msg);
    }

    fn push_meta_error_or_warning(&mut self, is_error: bool, msg: String) {
        self.deferred.push_msg_leveled(msg, is_error);
    }

    fn collect(self) -> (PureErrorBuf, RawKeywords) {
        let mut deviant_keywords = HashMap::new();
        let mut deferred = PureErrorBuf::new();
        for (key, v) in self.raw_keywords {
            match v.status {
                // ASSUME anything that is in this position at this point
                // will start with "$" since we pulled out the nonstandard
                // keywords when making the metadata.
                ValueStatus::Raw => {
                    deviant_keywords.insert(key, v.value);
                }
                ValueStatus::Error(err) => deferred.push(err),
                ValueStatus::Used => (),
            }
        }
        (deferred, deviant_keywords)
    }

    fn into_errors(self, reason: String) -> PureFailure {
        let c = self.conf;
        let (mut deferred, dev) = self.collect();
        for k in dev.into_keys() {
            deferred.push_msg_leveled(
                format!("{} starts with '$' but is not standard", k),
                c.disallow_deviant,
            );
        }
        Failure { reason, deferred }
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

fn parse_bounds(s0: &str, s1: &str, allow_blank: bool, id: SegmentId) -> Result<Segment, String> {
    if let (Some(begin), Some(end)) = (
        parse_header_offset(s0, allow_blank),
        parse_header_offset(s1, allow_blank),
    ) {
        Segment::try_new(begin, end, id)
    } else if allow_blank {
        Err("could not make bounds from integers/blanks".to_string())
    } else {
        Err("could not make bounds from integers".to_string())
    }
}

const hre: &str = r"(.{6})    (.{8})(.{8})(.{8})(.{8})(.{8})(.{8})";

fn parse_header(s: &str) -> Result<Header, &'static str> {
    let re = Regex::new(hre).unwrap();
    re.captures(s)
        .and_then(|c| {
            let [v, t0, t1, d0, d1, a0, a1] = c.extract().1;
            // TODO actually gather errors here
            if let (Ok(version), Ok(text), Ok(data), Ok(analysis)) = (
                v.parse(),
                parse_bounds(t0, t1, false, SegmentId::PrimaryText),
                parse_bounds(d0, d1, false, SegmentId::Data),
                parse_bounds(a0, a1, true, SegmentId::Analysis),
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

// #[derive(Debug, Clone, Serialize)]
// struct StdText<M, P> {
//     version: Version,
//     core: CoreTEXT<M, P>,
// }

// #[derive(Debug, Clone, Serialize)]
// pub struct RawTEXT {
//     delimiter: u8,
//     standard_keywords: StdKeywords,
//     nonstandard_keywords: NonStdKeywords,
//     warnings: Vec<String>,
// }

// impl RawTEXT {
//     fn to_state<'a>(&self, conf: &'a StdTextReader) -> KwState<'a> {
//         let mut raw_standard_keywords = HashMap::new();
//         for (k, v) in self.keywords.iter() {
//             raw_standard_keywords.insert(
//                 k.clone(),
//                 KwValue {
//                     value: v.clone(),
//                     status: ValueStatus::Raw,
//                 },
//             );
//         }
//         KwState {
//             raw_keywords: raw_standard_keywords,
//             deferred: PureErrorBuf::new(),
//             conf,
//         }
//     }
// }

// pub struct FCSSuccess {
//     pub header: Header,
//     pub raw: RawTEXT,
//     pub std: AnyStdTEXT,
//     pub data: ParsedData,
// }

// /// Represents result which may fail but still have immediately usable data.
// ///
// /// Useful for situations where the program should try to compute as much as
// /// possible before failing, which entails gather errors and carrying them
// /// forward rather than exiting immediately as would be the case if the error
// /// were wrapped in a Result<_, _>.
// struct Tentative<X> {
//     // ignorable: DeferredErrors,
//     // unignorable: DeferredErrors,
//     errors: AllDeferredErrors,
//     data: X,
// }

// struct AllDeferredErrors {
//     ignorable: DeferredErrors,
//     unignorable: DeferredErrors,
// }

// impl AllDeferredErrors {
//     fn new() -> AllDeferredErrors {
//         AllDeferredErrors {
//             ignorable: DeferredErrors::new(),
//             unignorable: DeferredErrors::new(),
//         }
//     }

//     fn from<E: Error + 'static>(e: E) -> AllDeferredErrors {
//         let mut x = AllDeferredErrors::new();
//         x.push_unignorable(e);
//         x
//     }

//     fn push_ignorable<E: Error + 'static>(&mut self, e: E) {
//         self.ignorable.0.push(Box::new(e))
//     }

//     fn push_unignorable<E: Error + 'static>(&mut self, e: E) {
//         self.unignorable.0.push(Box::new(e))
//     }

//     fn extend_ignorable(&mut self, es: DeferredErrors) {
//         self.ignorable.0.extend(es.0);
//     }

//     fn extend_unignorable(&mut self, es: DeferredErrors) {
//         self.unignorable.0.extend(es.0);
//     }

//     fn extend(&mut self, es: AllDeferredErrors) {
//         self.extend_unignorable(es.unignorable);
//         self.extend_ignorable(es.ignorable);
//     }
// }

// #[derive(Debug)]
// struct DelimError {
//     delimiter: u8,
//     kind: DelimErrorKind,
// }

// impl fmt::Display for DelimError {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
//         let x = match self.kind {
//             DelimErrorKind::NotAscii => "an ASCII character 1-126",
//             DelimErrorKind::NotUTF8 => "a utf8 character",
//         };
//         write!(f, "Delimiter {} is not {}", self.delimiter, x)
//     }
// }

// impl Error for DelimError {}

// #[derive(Debug)]
// enum DelimErrorKind {
//     NotUTF8,
//     NotAscii,
// }

fn verify_delim(xs: &[u8], conf: &RawTextReader) -> PureSuccess<u8> {
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

// enum RawTextError {
//     DelimAtBoundary,
// }

// #[derive(Debug)]
// struct MsgError(String);

// impl Error for MsgError {}

// impl fmt::Display for MsgError {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
//         write!(f, "{}", self.0)
//     }
// }

type RawPairs = Vec<(String, String)>;

fn split_raw_text(xs: &[u8], delim: u8, conf: &RawTextReader) -> PureSuccess<RawPairs> {
    let mut keywords: vec![];
    let mut res = PureSuccess::from(keywords);
    let mut warnings = vec![];
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
    let boundaries = if conf.no_delim_escape {
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
                    res.push_unignorable(RawTextError::DelimAtBoundary);
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
                        conf.enfore_keyword_ascii,
                    )
                }
                // if delimiters were escaped, replace them here
                if conf.no_delim_escape {
                    // Test for empty values if we don't allow delim escaping;
                    // anything empty will either drop or produce an error
                    // depending on user settings
                    if v.is_empty() {
                        // TODO tell the user that this key will be dropped
                        let msg = format!("key {kupper} has a blank value");
                        res.push_msg_leveled(msg, conf.enforce_nonempty);
                        None
                    } else {
                        keywords.push((kupper.clone(), v.to_string()))
                    }
                } else {
                    let krep = kupper.replace(escape_from, escape_to);
                    let rrep = v.replace(escape_from, escape_to);
                    keywords.push((krep, rrep))
                };
                // test if the key was inserted already
                //
                // TODO this will be better assessed when we have both hashmaps
                // from primary and supp text
                // if res.is_some() {
                //     let msg = format!("key {kupper} is found more than once");
                //     res.push_msg_leveled(msg, conf.enforce_unique)
                // }
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

fn repair_keywords(pairs: &mut RawPairs, conf: &RawTextReader) {
    for (key, v) in pairs.iter_mut() {
        let k = key.as_str();
        if k == DATE {
            if let Some(pattern) = &conf.date_pattern {
                if let Ok(d) = NaiveDate::parse_from_str(v, pattern.as_str()) {
                    *v = format!("{}", FCSDate(d))
                }
            }
        }
    }
}

fn hash_raw_pairs(pairs: Vec<(String, String)>, conf: &RawTextReader) -> PureSuccess<RawKeywords> {
    let standard: HashMap<_, _> = HashMap::new();
    // let nonstandard: HashMap<_, _> = HashMap::new();
    let mut res = PureSuccess::from(standard);
    // TODO filter keywords based on pattern somewhere here
    for (key, value) in pairs.into_iter() {
        let oldkey = key.clone(); // TODO this seems lame
        let ires = res.data.insert(key, value);
        if ires.is_some() {
            let msg = format!("Skipping already-inserted key: {oldkey}");
            res.push_msg_leveled(msg, conf.enforce_unique);
        }
    }
    res
}

impl Error for SegmentError {}

// macro_rules! wrap_enum {
//     ($enum_name:ident, $wrapper_name:ident) => {
//         impl From<$enum_name> for $wrapper_name {
//             fn from(enm: $enum_name) -> Self {
//                 Self(enm)
//             }
//         }
//     };
// }

impl StdTextReader {
    fn time_name_matches(&self, name: &Shortname) -> bool {
        self.time_shortname
            .as_ref()
            .map(|n| n == name.0.as_str())
            .unwrap_or(false)
    }
}

impl From<PureFailure> for ImpureFailure {
    fn from(value: PureFailure) -> Self {
        value.map(ImpureError::Pure)
    }
}

impl From<io::Error> for ImpureFailure {
    fn from(value: io::Error) -> Self {
        Failure::new(ImpureError::IO(value))
    }
}

fn pad_zeros(s: &str) -> String {
    let len = s.len();
    let trimmed = s.trim_start();
    let newlen = trimmed.len();
    ("0").repeat(len - newlen) + trimmed
}

fn parse_segment(
    begin: Option<String>,
    end: Option<String>,
    begin_delta: i32,
    end_delta: i32,
    id: SegmentId,
    level: PureErrorLevel,
) -> PureMaybe<Segment> {
    let parse_one = |s: Option<String>, which| {
        s.ok_or(format!("{which} not present for {id}"))
            .and_then(|pass| pass.parse::<u32>().map_err(|e| e.to_string()))
    };
    let b = parse_one(begin, "begin");
    let e = parse_one(end, "end");
    let res = match (b, e) {
        (Ok(bn), Ok(en)) => {
            Segment::try_new_adjusted(bn, en, begin_delta, end_delta, id).map_err(|e| vec![e])
        }
        (Err(be), Err(en)) => Err(vec![be, en]),
        (Err(be), _) => Err(vec![be]),
        (_, Err(en)) => Err(vec![en]),
    };
    PureSuccess::from_result(res.map_err(|es| PureErrorBuf::from_many(es, level)))
}

fn find_raw_segments(
    pairs: RawPairs,
    conf: &RawTextReader,
    header_data_seg: &Segment,
    header_analysis_seg: &Segment,
) -> PureSuccess<(RawPairs, Option<Segment>, Option<Segment>, Option<Segment>)> {
    // iterate through all pairs and strip out the ones that denote an offset
    let mut data0 = None;
    let mut data1 = None;
    let mut stext0 = None;
    let mut stext1 = None;
    let mut analysis0 = None;
    let mut analysis1 = None;
    let mut newpairs = vec![];
    let pad_maybe = |s: String| {
        if conf.repair_offset_spaces {
            pad_zeros(s.as_str())
        } else {
            s
        }
    };
    for (key, v) in pairs.into_iter() {
        match key.as_str() {
            BEGINDATA => data0 = Some(pad_maybe(v)),
            ENDDATA => data1 = Some(pad_maybe(v)),
            BEGINSTEXT => stext0 = Some(pad_maybe(v)),
            ENDSTEXT => stext1 = Some(pad_maybe(v)),
            BEGINANALYSIS => analysis0 = Some(pad_maybe(v)),
            ENDANALYSIS => analysis1 = Some(pad_maybe(v)),
            _ => newpairs.push((key, v)),
        }
    }

    let check_seg_with_header = |res: PureMaybe<Segment>, header_seg: &Segment, what| {
        res.and_then(|seg| {
            // TODO this doesn't seem the most efficient since I make a new
            // PureSuccess object in some branches
            match seg {
                None => {
                    let seg = if header_seg.is_unset() {
                        None
                    } else {
                        Some(*header_seg)
                    };
                    PureSuccess::from(seg)
                }
                Some(seg) => {
                    let mut res = PureSuccess::from(Some(seg));
                    if !header_seg.is_unset() && seg != *header_seg {
                        res.data = Some(*header_seg);
                        // TODO toggle level since this could indicate a sketchy file
                        res.push_msg_leveled(
                            format!("{what} offsets differ in HEADER and TEXT, using HEADER"),
                            false,
                        );
                    }
                    res
                }
            }
        })
    };

    // The DATA segment can be specified in either the header or TEXT. If within
    // offset 99,999,999, then the two should match. if they don't match then
    // trust the header and throw warning/error. If outside this range then the
    // header will be 0,0 and TEXT will have the real offsets.
    let data = check_seg_with_header(
        parse_segment(
            data0,
            data1,
            conf.startdata_delta,
            conf.enddata_delta,
            SegmentId::Data,
            PureErrorLevel::Error,
        ),
        header_data_seg,
        "DATA",
    );

    // Supplemental TEXT offsets are only in TEXT, so just parse and return
    // if found.
    let stext = parse_segment(
        stext0,
        stext1,
        conf.start_stext_delta,
        conf.end_stext_delta,
        SegmentId::SupplementalText,
        PureErrorLevel::Warning,
    );

    // ANALYSIS offsets are analogous to DATA offsets except they are optional.
    let analysis = check_seg_with_header(
        parse_segment(
            analysis0,
            analysis1,
            conf.start_analysis_delta,
            conf.end_analysis_delta,
            SegmentId::Analysis,
            PureErrorLevel::Warning,
        ),
        header_analysis_seg,
        "ANALYSIS",
    );

    data.combine3(stext, analysis, |a, b, c| (newpairs, a, b, c))
}

fn read_raw_text<R: Read + Seek>(
    h: &mut BufReader<R>,
    header: &Header,
    conf: &RawTextReader,
) -> ImpureResult<RawTEXT> {
    let adjusted_text = Failure::from_result(header.text.try_adjust(
        conf.starttext_delta,
        conf.endtext_delta,
        SegmentId::PrimaryText,
    ))?;

    let mut buf = vec![];
    adjusted_text.read(h, &mut buf)?;

    verify_delim(&buf, conf).try_map(|delimiter| {
        let mut res = split_raw_text(&buf, delimiter, conf);
        let pairs_res = if header.version == Version::FCS2_0 {
            repair_keywords(&mut res.data, conf);
            Ok(res.map(|pairs| {
                (
                    pairs,
                    if header.data.is_unset() {
                        None
                    } else {
                        Some(header.data)
                    },
                    None,
                    if header.analysis.is_unset() {
                        None
                    } else {
                        Some(header.analysis)
                    },
                )
            }))
        } else {
            find_raw_segments(res.data, conf, &header.data, &header.analysis).try_map(
                |(mut newpairs, maybe_data, maybe_stext, maybe_anal)| {
                    let io_res: ImpureResult<_> =
                        maybe_stext.map_or(Ok(PureSuccess::from(vec![])), |stext| {
                            buf.clear();
                            stext.read(h, &mut buf)?;
                            Ok(split_raw_text(&buf, delimiter, conf))
                        });
                    let stext_res = io_res?;
                    Ok(stext_res.map(|stext_pairs| {
                        newpairs.extend(stext_pairs);
                        (newpairs, maybe_data, maybe_stext, maybe_anal)
                    }))
                },
            )
        }?;

        Ok(
            pairs_res.and_then(|(pairs, data_seg, stext_seg, analysis_seg)| {
                hash_raw_pairs(pairs, conf).map(|standard_kws| RawTEXT {
                    version: header.version,
                    keywords: standard_kws,
                    offsets: Offsets {
                        data_seg,
                        supp_text_seg: stext_seg,
                        prim_text_seg: adjusted_text,
                        analysis_seg,
                        nextdata: 0, // TODO fix
                    },
                    delimiter,
                })
            }),
        )
    })
}

// type FCSResult = Result<FCSSuccess, Box<StdTEXTErrors>>;

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
pub fn read_fcs_raw_text(p: &path::PathBuf, conf: &Reader) -> ImpureResult<RawTEXT> {
    let file = fs::File::options().read(true).open(p)?;
    let mut reader = BufReader::new(file);
    let header = read_header(&mut reader)?;
    read_raw_text(&mut reader, &header, &conf.text.raw)
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
pub fn read_fcs_text(p: &path::PathBuf, conf: &Reader) -> ImpureResult<StandardizedTEXT> {
    let raw_succ = read_fcs_raw_text(p, conf)?;
    let out = raw_succ.try_map(|raw| {
        parse_raw_text(raw.version, raw.keywords, &conf.text).map(|std_succ| {
            std_succ.map({
                |(standardized, deviant_keywords)| StandardizedTEXT {
                    offsets: raw.offsets,
                    standardized,
                    delimiter: raw.delimiter,
                    deviant_keywords,
                }
            })
        })
    })?;
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
pub fn read_fcs_file(p: &path::PathBuf, conf: &Reader) -> ImpureResult<()> {
    let file = fs::File::options().read(true).open(p)?;
    let mut reader = BufReader::new(file);
    let header = read_header(&mut reader)?;
    let raw = read_raw_text(&mut reader, &header, &conf.text.raw)?;
    // TODO useless clone?
    match parse_raw_text(header, raw, &conf.text) {
        Ok(std) => {
            let data = read_data(&mut reader, std.data_parser).unwrap();
            Ok(Ok(PureSuccess {
                header: std.header,
                raw: std.raw,
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

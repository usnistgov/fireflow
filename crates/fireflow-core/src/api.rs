use crate::config::*;
use crate::error::*;
pub use crate::header::*;
use crate::header_text::*;
use crate::keywords::*;
use crate::macros::{newtype_disp, newtype_from, newtype_from_outer, newtype_fromstr};
use crate::optionalkw::OptionalKw;
pub use crate::segment::*;
use crate::validated::distinct::*;
use crate::validated::nonstandard::*;
use crate::validated::ranged_float::*;
use crate::validated::shortname::*;

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
use std::hash::Hash;
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
#[derive(Clone, Serialize)]
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
#[derive(Clone, Serialize)]
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
    pub dataset: AnyCoreDataset,

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
pub struct CoreDataset<M, P, N> {
    /// Standardized TEXT segment in version specific format
    pub text: Box<CoreTEXT<M, P, N>>,

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
    pub analysis: Analysis,
}

#[derive(Clone)]
pub struct Analysis(pub Vec<u8>);

newtype_from!(Analysis, Vec<u8>);

/// Critical FCS TEXT data for any supported FCS version
#[derive(Clone)]
pub enum AnyCoreTEXT {
    FCS2_0(Box<CoreTEXT2_0>),
    FCS3_0(Box<CoreTEXT3_0>),
    FCS3_1(Box<CoreTEXT3_1>),
    FCS3_2(Box<CoreTEXT3_2>),
}

/// FCS file for any supported FCS version
#[derive(Clone)]
pub enum AnyCoreDataset {
    FCS2_0(CoreDataset2_0),
    FCS3_0(CoreDataset3_0),
    FCS3_1(CoreDataset3_1),
    FCS3_2(CoreDataset3_2),
}

/// Minimally-required FCS TEXT data for each version
pub type CoreTEXT2_0 = CoreTEXT<InnerMetadata2_0, InnerMeasurement2_0, OptionalKw<Shortname>>;
pub type CoreTEXT3_0 = CoreTEXT<InnerMetadata3_0, InnerMeasurement3_0, OptionalKw<Shortname>>;
pub type CoreTEXT3_1 = CoreTEXT<InnerMetadata3_1, InnerMeasurement3_1, Shortname>;
pub type CoreTEXT3_2 = CoreTEXT<InnerMetadata3_2, InnerMeasurement3_2, Shortname>;

pub type CoreDataset2_0 = CoreDataset<InnerMetadata2_0, InnerMeasurement2_0, OptionalKw<Shortname>>;
pub type CoreDataset3_0 = CoreDataset<InnerMetadata3_0, InnerMeasurement3_0, OptionalKw<Shortname>>;
pub type CoreDataset3_1 = CoreDataset<InnerMetadata3_1, InnerMeasurement3_1, Shortname>;
pub type CoreDataset3_2 = CoreDataset<InnerMetadata3_2, InnerMeasurement3_2, Shortname>;

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
/// - $(BEGIN|END)(DATA|ANALYSIS|STEXT) (handled elsewhere)
///
/// These are not included because this struct will also be used to encode the
/// TEXT data when writing a new FCS file, and the keywords that are not
/// included can be computed on the fly when writing.
#[derive(Clone, Serialize)]
pub struct CoreTEXT<M, P, N> {
    /// All "non-measurement" TEXT keywords.
    ///
    /// This is specific to each FCS version, which is encoded in the generic
    /// type variable.
    pub metadata: Metadata<M>,

    /// All measurement TEXT keywords.
    ///
    /// Specifically these are denoted by "$Pn*" keywords where "n" is the index
    /// of the measurement which also corresponds to its column in the DATA
    /// segment. The order of each measurement in this vector is n - 1.
    ///
    /// This is specific to each FCS version, which is encoded in the generic
    /// type variable.
    pub measurements: DistinctVec<N, Measurement<P>>,
}

/// Raw TEXT key/value pairs
type RawKeywords = HashMap<String, String>;

/// A datetime as used in the $(BEGIN|END)DATETIME keys (3.2+ only)
#[derive(Clone, Serialize)]
pub struct FCSDateTime(pub DateTime<FixedOffset>);

/// A time as used in the $BTIM/ETIM keys without seconds (2.0 only)
#[derive(Clone, Serialize, PartialEq, Eq, PartialOrd)]
pub struct FCSTime(pub NaiveTime);

/// A time as used in the $BTIM/ETIM keys with 1/60 seconds (3.0 only)
#[derive(Clone, Serialize, PartialEq, Eq, PartialOrd)]
pub struct FCSTime60(pub NaiveTime);

/// A time as used in the $BTIM/ETIM keys with centiseconds (3.1+ only)
#[derive(Clone, Serialize, PartialEq, Eq, PartialOrd)]
pub struct FCSTime100(pub NaiveTime);

/// A datetime as used in the $LAST_MODIFIED key (3.1+ only)
// TODO this should almost certainly be after $ENDDATETIME if given
#[derive(Clone, Serialize)]
pub struct ModifiedDateTime(pub NaiveDateTime);

/// A date as used in the $DATE key (all versions)
#[derive(Clone, Serialize)]
pub struct FCSDate(pub NaiveDate);

/// A convenient bundle holding data/time keyword values.
///
/// The generic type parameter is meant to account for the fact that the time
/// types for different versions are all slightly different in their treatment
/// of sub-second time.
#[derive(Clone, Serialize)]
pub struct Timestamps<X> {
    /// The value of the $BTIM key
    pub btim: OptionalKw<Btim<X>>,

    /// The value of the $ETIM key
    pub etim: OptionalKw<Etim<X>>,

    /// The value of the $DATE key
    pub date: OptionalKw<FCSDate>,
}

impl<X> Timestamps<X> {
    fn map<F, Y>(self, f: F) -> Timestamps<Y>
    where
        F: Fn(X) -> Y,
    {
        Timestamps {
            btim: self.btim.map(|x| Btim(f(x.0))),
            etim: self.etim.map(|x| Etim(f(x.0))),
            date: self.date,
        }
    }
}

impl<X: PartialOrd> Timestamps<X> {
    fn valid(&self) -> bool {
        if self.date.0.is_some() {
            if let (Some(b), Some(e)) = (&self.btim.0, &self.etim.0) {
                b.0 < e.0
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
// fn timestamps_eq_datetimes<T>(ts: &Timestamps<T>, dt: &Datetimes) -> bool
// where
//     T: Copy,
//     NaiveTime: From<T>,
// {
//     if let (Some(td), Some(tb), Some(te), Some(db), Some(de)) =
//         (&ts.date.0, &ts.btim.0, &ts.etim.0, &dt.begin.0, &dt.end.0)
//     {
//         let dt_d1 = db.0.date_naive();
//         let dt_d2 = de.0.date_naive();
//         let dt_t1 = db.0.time();
//         let dt_t2 = de.0.time();
//         let ts_d = td.0;
//         let ts_t1: NaiveTime = (*tb).into();
//         let ts_t2: NaiveTime = (*te).into();
//         dt_d1 == dt_d2 && dt_d2 == ts_d && dt_t1 == ts_t1 && dt_t2 == ts_t2
//     } else {
//         true
//     }
// }

/// $BTIM/ETIM/DATE for FCS 2.0
pub type Timestamps2_0 = Timestamps<FCSTime>;

/// $BTIM/ETIM/DATE for FCS 3.0
pub type Timestamps3_0 = Timestamps<FCSTime60>;

/// $BTIM/ETIM/DATE for FCS 3.1+
pub type Timestamps3_1 = Timestamps<FCSTime100>;

/// A convenient bundle for the $BEGINDATETIME and $ENDDATETIME keys (3.2+)
#[derive(Clone, Serialize, Default)]
pub struct Datetimes {
    /// Value for the $BEGINDATETIME key.
    pub begin: OptionalKw<BeginDateTime>,

    /// Value for the $ENDDATETIME key.
    pub end: OptionalKw<EndDateTime>,
}

impl Datetimes {
    fn valid(&self) -> bool {
        if let (Some(b), Some(e)) = (&self.begin.0, &self.end.0) {
            (b.0).0 < (e.0).0
        } else {
            true
        }
    }
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
struct Mode3_2;

/// The value for the $PnDISPLAY key (3.1+)
#[derive(Clone, Serialize)]
pub enum Display {
    /// Linear display (value like 'Linear,<lower>,<upper>')
    Lin { lower: f32, upper: f32 },

    /// Logarithmic display (value like 'Logarithmic,<offset>,<decades>')
    Log { offset: f32, decades: f32 },
}

/// Endianness
///
/// This is also stored in the $BYTEORD key in 3.1+
#[derive(Clone, Copy, Serialize, PartialEq, Eq, Hash)]
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
#[derive(Clone, Serialize)]
pub enum ByteOrd {
    // TODO this should also be applied to things like 1,2,3 or 5,4,3,2,1, which
    // are big/little endian but not "traditional" byte widths.
    Endian(Endian),
    Mixed(Vec<u8>),
}

/// The four allowed values for the $DATATYPE keyword.
#[derive(Clone, Copy, Eq, PartialEq, Hash, Serialize)]
pub enum AlphaNumType {
    Ascii,
    Integer,
    Single,
    Double,
}

/// The three values for the $PnDATATYPE keyword (3.2+)
#[derive(Clone, Copy, Serialize)]
pub enum NumType {
    Integer,
    Single,
    Double,
}

/// A compensation matrix.
///
/// This is encoded in the $DFCmTOn keywords in 2.0 and $COMP in 3.0.
#[derive(Clone, Serialize)]
pub struct Compensation {
    /// Values in the comp matrix in row-major order. Assumed to be the
    /// same width and height as $PAR
    pub matrix: DMatrix<f32>,
}

impl Compensation {
    fn remove_by_index(&mut self, i: usize) -> bool {
        if i <= self.matrix.ncols() {
            self.matrix = self.matrix.clone().remove_row(i).remove_column(i);
            true
        } else {
            false
        }
    }
}

/// The spillover matrix from the $SPILLOVER keyword (3.1+)
#[derive(Clone, Serialize)]
pub struct Spillover {
    /// The measurements in the spillover matrix. Assumed to be a subset of the
    /// values in the $PnN keys.
    // TODO use BTreeSet to enforce uniqueness
    pub measurements: Vec<Shortname>,

    /// Numeric values in the spillover matrix in row-major order.
    pub matrix: DMatrix<f32>,
}

impl Spillover {
    fn remove_by_name(&mut self, n: &Shortname) -> bool {
        if let Some(i) = self.measurements.iter().position(|m| m == n) {
            // TODO this looks expensive; it copies basically everything 3x;
            // good thing these matrices aren't that big (usually). The
            // alternative is to iterate over the matrix and populate a new one
            // while skipping certain elements.
            self.matrix = self.matrix.clone().remove_row(i).remove_column(i);
            true
        } else {
            false
        }
    }
}

/// The value of the $TR field (all versions)
///
/// This is formatted as 'string,f' where 'string' is a measurement name.
#[derive(Clone, Serialize)]
pub struct Trigger {
    /// The measurement name (assumed to match a '$PnN' value).
    pub measurement: Shortname,

    /// The threshold of the trigger.
    pub threshold: u32,
}

/// The value for the $PnE key (all versions).
///
/// Format is assumed to be 'f1,f2'
// TODO this is super messy, see 3.2 spec for restrictions on this we may with
// to use further
#[derive(Clone, PartialEq, Serialize)]
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
#[derive(Clone, Serialize)]
pub struct Wavelengths(Vec<u32>);

/// The value for the $PnB key (all versions)
///
/// The $PnB key actually stores bits. However, this library only supports
/// widths that are multiples of 8 (ie bytes) for now. Therefore, this key
/// actually stores the number of bytes indicated by $PnB.
///
/// This may also be '*' which means "delimited ASCII" which is only valid when
/// $DATATYPE=A.
#[derive(Clone, Copy, Serialize)]
pub enum Bytes {
    Fixed(u8),
    Variable,
}

/// The value for the $ORIGINALITY key (3.1+)
#[derive(Clone, Serialize)]
pub enum Originality {
    Original,
    NonDataModified,
    Appended,
    DataModified,
}

/// A bundle for $ORIGINALITY, $LAST_MODIFIER, and $LAST_MODIFIED (3.1+)
#[derive(Clone, Serialize, Default)]
pub struct ModificationData {
    pub last_modifier: OptionalKw<LastModifier>,
    pub last_modified: OptionalKw<ModifiedDateTime>,
    pub originality: OptionalKw<Originality>,
}

/// A bundle for $PLATEID, $PLATENAME, and $WELLID (3.1+)
#[derive(Clone, Serialize, Default)]
pub struct PlateData {
    pub plateid: OptionalKw<Plateid>,
    pub platename: OptionalKw<Platename>,
    pub wellid: OptionalKw<Wellid>,
}

/// The value for the $UNSTAINEDCENTERS key (3.2+)
///
/// This is actually encoded as a string like 'n,[measuremnts,],[values]' but
/// here is more conveniently encoded as a hash table.
#[derive(Clone, Serialize)]
pub struct UnstainedCenters(HashMap<Shortname, f32>);

impl UnstainedCenters {
    fn remove_by_name(&mut self, n: &Shortname) -> bool {
        self.0.remove(n).is_some()
    }
}

/// A bundle for $UNSTAINEDCENTERS and $UNSTAINEDINFO (3.2+)
#[derive(Clone, Serialize, Default)]
pub struct UnstainedData {
    pub unstainedcenters: OptionalKw<UnstainedCenters>,
    pub unstainedinfo: OptionalKw<UnstainedInfo>,
}

/// A bundle for $CARRIERID, $CARRIERTYPE, $LOCATIONID (3.2+)
#[derive(Clone, Serialize, Default)]
pub struct CarrierData {
    pub carrierid: OptionalKw<Carrierid>,
    pub carriertype: OptionalKw<Carriertype>,
    pub locationid: OptionalKw<Locationid>,
}

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

/// The value of the $PnTYPE key (3.2+)
#[derive(Clone, Serialize)]
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

/// The value of the $PnR key (all versions)
///
/// Technically this should only be an integer, but many versions also store
/// floats which makes sense for cases where $DATATYPE/$PnDATATYPE indicates
/// float or double.
#[derive(Clone, Serialize)]
pub struct Range(pub String);

newtype_disp!(Range);
newtype_fromstr!(Range, Infallible);

/// Measurement fields specific to version 2.0
#[derive(Clone, Serialize)]
pub struct InnerMeasurement2_0 {
    /// Value for $PnE
    pub scale: OptionalKw<Scale>,

    /// Value for $PnL
    pub wavelength: OptionalKw<Wavelength>,
    // /// Value for $PnN
    // pub shortname: OptionalKw<Shortname>,
}

/// Measurement fields specific to version 3.0
#[derive(Clone, Serialize)]
pub struct InnerMeasurement3_0 {
    /// Value for $PnE
    pub scale: Scale,

    /// Value for $PnL
    pub wavelength: OptionalKw<Wavelength>,

    // /// Value for $PnN
    // pub shortname: OptionalKw<Shortname>,
    /// Value for $PnG
    pub gain: OptionalKw<Gain>,
}

/// Measurement fields specific to version 3.1
#[derive(Clone, Serialize)]
pub struct InnerMeasurement3_1 {
    /// Value for $PnE
    pub scale: Scale,

    /// Value for $PnL
    pub wavelengths: OptionalKw<Wavelengths>,

    // /// Value for $PnN
    // pub shortname: Shortname,
    /// Value for $PnG
    pub gain: OptionalKw<Gain>,

    /// Value for $PnCALIBRATION
    pub calibration: OptionalKw<Calibration3_1>,

    /// Value for $PnDISPLAY
    pub display: OptionalKw<Display>,
}

/// Measurement fields specific to version 3.2
#[derive(Clone, Serialize)]
pub struct InnerMeasurement3_2 {
    /// Value for $PnE
    pub scale: Scale,

    /// Value for $PnL
    pub wavelengths: OptionalKw<Wavelengths>,

    // /// Value for $PnN
    // pub shortname: Shortname,
    /// Value for $PnG
    pub gain: OptionalKw<Gain>,

    /// Value for $PnCALIBRATION
    pub calibration: OptionalKw<Calibration3_2>,

    /// Value for $PnDISPLAY
    pub display: OptionalKw<Display>,

    /// Value for $PnANALYTE
    pub analyte: OptionalKw<Analyte>,

    /// Value for $PnFEATURE
    pub feature: OptionalKw<Feature>,

    /// Value for $PnTYPE
    pub measurement_type: OptionalKw<MeasurementType>,

    /// Value for $PnTAG
    pub tag: OptionalKw<Tag>,

    /// Value for $PnDET
    pub detector_name: OptionalKw<DetectorName>,

    /// Value for $PnDATATYPE
    pub datatype: OptionalKw<NumType>,
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
    pub bytes: Bytes,

    /// Value for $PnR
    pub range: Range,

    /// Value for $PnS
    pub longname: OptionalKw<Longname>,

    /// Value for $PnF
    pub filter: OptionalKw<Filter>,

    /// Value for $PnO
    pub power: OptionalKw<Power>,

    /// Value for $PnD
    pub detector_type: OptionalKw<DetectorType>,

    /// Value for $PnP
    pub percent_emitted: OptionalKw<PercentEmitted>,

    /// Value for $PnV
    pub detector_voltage: OptionalKw<DetectorVoltage>,

    /// Non standard keywords that belong to this measurement.
    ///
    /// These are found using a configurable pattern to filter matching keys.
    pub nonstandard_keywords: NonStdKeywords,

    /// Version specific data
    pub specific: X,
}

impl IntoShortname for OptionalKw<Shortname> {
    fn as_name_opt(&self) -> Option<&Shortname> {
        self.as_ref_opt()
    }

    fn from_name(n: Shortname) -> Self {
        Some(n).into()
    }
}

impl IntoShortname for Shortname {
    fn as_name_opt(&self) -> Option<&Shortname> {
        Some(self)
    }

    fn from_name(n: Shortname) -> Self {
        n
    }
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

trait Key {
    const C: &'static str;

    fn std() -> String {
        format!("${}", Self::C)
    }

    fn nonstd() -> NonStdKey {
        NonStdKey::from_unchecked(Self::C)
    }
}

trait IndexedKey {
    const PREFIX: &'static str;
    const SUFFIX: &'static str;

    fn fmt(i: usize) -> String {
        format!("{}{i}{}", Self::PREFIX, Self::SUFFIX)
    }

    fn fmt_blank() -> String {
        format!("{}n{}", Self::PREFIX, Self::SUFFIX)
    }

    fn fmt_sub() -> String {
        format!("{}%n{}", Self::PREFIX, Self::SUFFIX)
    }

    fn std(i: MeasIdx) -> String {
        format!("${}", Self::fmt(i.0))
    }

    fn std_blank() -> String {
        format!("${}", Self::fmt_blank())
    }

    fn std_maybe(i: Option<MeasIdx>) -> String {
        i.map(Self::std).unwrap_or(Self::fmt_blank())
    }

    fn nonstd(i: usize) -> NonStdKey {
        NonStdKey::from_unchecked(Self::fmt(i).as_str())
    }

    fn nonstd_sub() -> NonStdMeasKey {
        NonStdMeasKey::from_unchecked(Self::fmt_sub().as_str())
    }

    /// Return true if a key matches the prefix/suffix.
    ///
    /// Specifically, test if string is like <PREFIX><N><SUFFIX> where
    /// N is an integer greater than zero.
    fn matches(other: &str, std: bool) -> bool {
        if std {
            other.strip_prefix("$")
        } else {
            Some(other)
        }
        .and_then(|s| s.strip_prefix(Self::PREFIX))
        .and_then(|s| s.strip_suffix(Self::SUFFIX))
        .and_then(|s| s.parse::<u32>().ok())
        .is_some_and(|x| x > 0)
    }
}

fn lookup_optional<V: FromStr>(kws: &mut RawKeywords, k: &str) -> Result<OptionalKw<V>, String>
where
    <V as FromStr>::Err: fmt::Display,
{
    match kws.remove(k) {
        Some(v) => v
            .parse()
            .map(|x| Some(x).into())
            .map_err(|w| format!("{w} (key={k}, value='{v}')")),
        None => Ok(None.into()),
    }
}

type ReqResult<T> = Result<T, String>;
type OptResult<T> = Result<OptionalKw<T>, String>;

fn lookup_req<T>(kws: &mut RawKeywords, k: &str) -> Result<T, String>
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

fn lookup_opt<T>(kws: &mut RawKeywords, k: &str) -> Result<Option<T>, String>
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

trait Required {
    fn lookup_req<V>(kws: &mut RawKeywords, k: &str) -> Result<V, String>
    where
        V: FromStr,
        <V as FromStr>::Err: fmt::Display,
    {
        lookup_req(kws, k)
    }
}

trait Optional {
    fn lookup_opt<V>(kws: &mut RawKeywords, k: &str) -> Result<OptionalKw<V>, String>
    where
        V: FromStr,
        <V as FromStr>::Err: fmt::Display,
    {
        lookup_opt(kws, k).map(|x| x.into())
    }
}

pub trait ReqMetaKey
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

trait ReqMeasKey
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

    fn pair(&self, n: Option<MeasIdx>) -> (String, String) {
        (Self::std_maybe(n), self.to_string())
    }
}

trait OptMetaKey
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

    // fn setter(default: Option<Self>, def_key: bool, key: Option<NonStdKey>) -> MetaKwSetter<Self> {
    //     if let Some(def) = default {
    //         KwSetter::Default(def)
    //     } else {
    //         let k = if def_key { Some(Self::nonstd()) } else { key };
    //         KwSetter::Key(k)
    //     }
    // }
}

trait OptMeasKey
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

    fn pair(opt: &OptionalKw<Self>, n: Option<MeasIdx>) -> (String, Option<String>) {
        (Self::std_maybe(n), opt.0.as_ref().map(|s| s.to_string()))
    }

    // fn setter(
    //     default: Option<Self>,
    //     def_key: bool,
    //     key: Option<NonStdMeasKey>,
    // ) -> MeasKwSetter<Self> {
    //     if let Some(def) = default {
    //         KwSetter::Default(def)
    //     } else {
    //         let k = if def_key {
    //             Some(Self::nonstd_sub())
    //         } else {
    //             key
    //         };
    //         KwSetter::Key(k)
    //     }
    // }
}

// trait LinkedOptMetaKey
// where
//     Self: Optional,
//     Self: fmt::Display,
//     Self: Key,
//     Self: Sized,
// {
//     fn lookup_linked_meta_opt(kws: &mut RawKeywords, names: &[&Shortname]) -> OptResult<Self> {
//         let k = Self::std();
//         let msg = format!("{k} references non-existent $PnN name");
//         match kws.remove(k.as_str()) {
//             Some(v) => Self::linked_from_str(v.as_str(), names)
//                 .and_then(|opt| opt.ok_or(msg).map(Some))
//                 .map_err(|e| format!("{e} (key='{k}', value='{v}')")),
//             None => Ok(None),
//         }
//         .map(|x| x.into())
//     }

//     fn linked_from_str(s: &str, names: &[&Shortname]) -> Result<Option<Self>, String>;

//     fn pair(opt: &OptionalKw<Self>) -> (String, Option<String>) {
//         (Self::std(), opt.0.as_ref().map(|s| s.to_string()))
//     }
// }

trait Linked
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

impl Linked for Trigger {
    fn names(&self) -> HashSet<&Shortname> {
        [&self.measurement].into_iter().collect()
    }

    fn reassign(&mut self, mapping: &NameMapping) {
        if let Some(new) = mapping.get(&self.measurement) {
            self.measurement = (*new).clone()
        }
    }
}

impl Linked for Spillover {
    fn names(&self) -> HashSet<&Shortname> {
        self.measurements.iter().collect()
    }

    fn reassign(&mut self, mapping: &NameMapping) {
        for n in self.measurements.iter_mut() {
            if let Some(new) = mapping.get(n) {
                *n = (*new).clone();
            }
        }
    }
}

impl Linked for UnstainedCenters {
    fn names(&self) -> HashSet<&Shortname> {
        self.0.keys().collect()
    }

    fn reassign(&mut self, mapping: &NameMapping) {
        // keys can't be mutated in place so need to rebuild the hashmap with
        // new keys from the mapping
        let new: HashMap<_, _> = self
            .0
            .iter()
            .map(|(k, v)| {
                (
                    mapping.get(k).map(|x| (*x).clone()).unwrap_or(k.clone()),
                    *v,
                )
            })
            .collect();
        (*self).0 = new;
    }
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

macro_rules! kw_req_meta_string {
    ($t:ident, $sfx:expr) => {
        kw_meta_string!($t, $sfx);
        req_meta!($t);
    };
}

macro_rules! kw_opt_meta_string {
    ($t:ident, $sfx:expr) => {
        kw_meta_string!($t, $sfx);
        opt_meta!($t);
    };
}

macro_rules! kw_req_meas_string {
    ($t:ident, $sfx:expr) => {
        kw_meas_string!($t, $sfx);
        req_meas!($t);
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

macro_rules! kw_req_meas_int {
    ($t:ident, $type:ident, $sfx:expr) => {
        kw_meas_int!($t, $type, $sfx);
        req_meas!($t);
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
kw_opt_meas_string!(DetectorType, "D");
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

#[derive(Clone, Serialize)]
pub struct Btim<T>(pub T);
#[derive(Clone, Serialize)]
pub struct Etim<T>(pub T);

kw_time!(Btim2_0, Btim, FCSTime, FCSTimeError, "BTIM");
kw_time!(Etim2_0, Etim, FCSTime, FCSTimeError, "ETIM");
kw_time!(Btim3_0, Btim, FCSTime60, FCSTime60Error, "BTIM");
kw_time!(Etim3_0, Etim, FCSTime60, FCSTime60Error, "ETIM");
kw_time!(Btim3_1, Btim, FCSTime100, FCSTime100Error, "BTIM");
kw_time!(Etim3_1, Etim, FCSTime100, FCSTime100Error, "ETIM");

kw_opt_meta!(FCSDate, "DATE");

#[derive(Clone, Serialize)]
struct BeginDateTime(pub FCSDateTime);
newtype_from!(BeginDateTime, FCSDateTime);
newtype_from_outer!(BeginDateTime, FCSDateTime);
newtype_disp!(BeginDateTime);
newtype_fromstr!(BeginDateTime, FCSDateTimeError);
kw_opt_meta!(BeginDateTime, "BEGINDATETIME");

#[derive(Clone, Serialize)]
struct EndDateTime(pub FCSDateTime);
newtype_from!(EndDateTime, FCSDateTime);
newtype_from_outer!(EndDateTime, FCSDateTime);
newtype_disp!(EndDateTime);
newtype_fromstr!(EndDateTime, FCSDateTimeError);
kw_opt_meta!(EndDateTime, "ENDDATETIME");

#[derive(Clone, Serialize)]
pub struct Timestep(pub PositiveFloat);

newtype_disp!(Timestep);
newtype_fromstr!(Timestep, RangedFloatError);

kw_opt_meta!(Timestep, "TIMESTEP");

#[derive(Clone, Serialize)]
pub struct Vol(pub NonNegFloat);

newtype_disp!(Vol);
newtype_fromstr!(Vol, RangedFloatError);

kw_opt_meta!(Vol, "VOL");

#[derive(Clone, Serialize)]
pub struct Gain(pub PositiveFloat);

newtype_disp!(Gain);
newtype_fromstr!(Gain, RangedFloatError);

kw_opt_meas!(Gain, "G");

#[derive(Clone, Serialize)]
pub struct DetectorVoltage(pub NonNegFloat);

newtype_disp!(DetectorVoltage);
newtype_fromstr!(DetectorVoltage, RangedFloatError);

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

// impl Key for Trigger {
//     const C: &'static str = "TR";
// }

// impl Optional for Trigger {}

// impl LinkedOptMetaKey for Trigger {
//     fn linked_from_str(s: &str, names: &[&Shortname]) -> Result<Option<Self>, String> {
//         match s.split(",").collect::<Vec<_>>()[..] {
//             [p, n1] => n1
//                 .parse::<u32>()
//                 .map_err(|e| e.to_string())
//                 .map(|threshold| {
//                     names
//                         .iter()
//                         .find(|x| x.as_ref() == p)
//                         .map(|measurement| Trigger {
//                             measurement: (*measurement).clone(),
//                             threshold,
//                         })
//                 }),
//             _ => Err("must be formatted like 'string,n'".to_string()),
//         }
//     }
// }

kw_opt_meas!(Scale, "E");
req_meas!(Scale);

kw_req_meas!(Range, "R");
kw_req_meas!(Bytes, "B");
kw_opt_meas!(Wavelengths, "W");
kw_opt_meas!(Feature, "FEATURE");
kw_opt_meas!(MeasurementType, "TYPE");
kw_opt_meas!(NumType, "DATATYPE");
kw_opt_meas!(Display, "DISPLAY");
kw_opt_meas!(Shortname, "N");
req_meas!(Shortname);

kw_opt_meas!(Calibration3_1, "CALIBRATION");
kw_opt_meas!(Calibration3_2, "CALIBRATION");

/// Metadata fields specific to version 2.0
#[derive(Clone, Serialize)]
pub struct InnerMetadata2_0 {
    /// Value of $MODE
    pub mode: Mode,

    /// Value of $BYTEORD
    pub byteord: ByteOrd,

    /// Value of $CYT
    pub cyt: OptionalKw<Cyt>,

    /// Compensation matrix derived from 'DFCnTOm' key/value pairs
    pub comp: OptionalKw<Compensation>,

    /// Values of $BTIM/ETIM/$DATE
    pub timestamps: Timestamps2_0,
}

/// Metadata fields specific to version 3.0
#[derive(Clone, Serialize)]
pub struct InnerMetadata3_0 {
    /// Value of $MODE
    pub mode: Mode,

    /// Value of $BYTEORD
    pub byteord: ByteOrd,

    /// Value of $CYT
    pub cyt: OptionalKw<Cyt>,

    /// Value of $COMP
    pub comp: OptionalKw<Compensation>,

    /// Values of $BTIM/ETIM/$DATE
    pub timestamps: Timestamps3_0,

    /// Value of $CYTSN
    pub cytsn: OptionalKw<Cytsn>,

    /// Value of $TIMESTEP
    pub timestep: OptionalKw<Timestep>,

    /// Value of $UNICODE
    pub unicode: OptionalKw<Unicode>,
}

/// Metadata fields specific to version 3.1
#[derive(Clone, Serialize)]
pub struct InnerMetadata3_1 {
    /// Value of $MODE
    pub mode: Mode,

    /// Value of $BYTEORD
    pub byteord: Endian,

    /// Value of $CYT
    pub cyt: OptionalKw<Cyt>,

    /// Values of $BTIM/ETIM/$DATE
    pub timestamps: Timestamps3_1,

    /// Value of $CYTSN
    pub cytsn: OptionalKw<Cytsn>,

    /// Value of $TIMESTEP
    pub timestep: OptionalKw<Timestep>,

    /// Value of $SPILLOVER
    pub spillover: OptionalKw<Spillover>,

    /// Values of $LAST_MODIFIED/$LAST_MODIFIER/$ORIGINALITY
    pub modification: ModificationData,

    /// Values of $PLATEID/$PLATENAME/$WELLID
    pub plate: PlateData,

    /// Value of $VOL
    pub vol: OptionalKw<Vol>,
}

#[derive(Clone, Serialize)]
pub struct InnerMetadata3_2 {
    /// Value of $BYTEORD
    pub byteord: Endian,

    /// Values of $BTIM/ETIM/$DATE
    pub timestamps: Timestamps3_1,

    /// Values of $BEGINDATETIME/$ENDDATETIME
    pub datetimes: Datetimes,

    /// Value of $CYT
    pub cyt: Cyt,

    /// Value of $SPILLOVER
    pub spillover: OptionalKw<Spillover>,

    /// Value of $CYTSN
    pub cytsn: OptionalKw<Cytsn>,

    /// Value of $TIMESTEP
    pub timestep: OptionalKw<Timestep>,

    /// Values of $LAST_MODIFIED/$LAST_MODIFIER/$ORIGINALITY
    pub modification: ModificationData,

    /// Values of $PLATEID/$PLATENAME/$WELLID
    pub plate: PlateData,

    /// Value of $VOL
    pub vol: OptionalKw<Vol>,

    /// Values of $CARRIERID/$CARRIERTYPE/$LOCATIONID
    pub carrier: CarrierData,

    /// Values of $UNSTAINEDINFO/$UNSTAINEDCENTERS
    pub unstained: UnstainedData,

    /// Value of $FLOWRATE
    pub flowrate: OptionalKw<Flowrate>,
}

/// Structured non-measurement metadata.
///
/// Explicit below are common to all FCS versions.
///
/// The generic type parameter allows version-specific data to be encoded.
#[derive(Clone, Serialize)]
pub struct Metadata<X> {
    /// Value of $DATATYPE
    pub datatype: AlphaNumType,

    /// Value of $ABRT
    pub abrt: OptionalKw<Abrt>,

    /// Value of $COM
    pub com: OptionalKw<Com>,

    /// Value of $CELLS
    pub cells: OptionalKw<Cells>,

    /// Value of $EXP
    pub exp: OptionalKw<Exp>,

    /// Value of $FIL
    pub fil: OptionalKw<Fil>,

    /// Value of $INST
    pub inst: OptionalKw<Inst>,

    /// Value of $LOST
    pub lost: OptionalKw<Lost>,

    /// Value of $OP
    pub op: OptionalKw<Op>,

    /// Value of $PROJ
    pub proj: OptionalKw<Proj>,

    /// Value of $SMNO
    pub smno: OptionalKw<Smno>,

    /// Value of $SRC
    pub src: OptionalKw<Src>,

    /// Value of $SYS
    pub sys: OptionalKw<Sys>,

    /// Value of $TR
    pub tr: OptionalKw<Trigger>,

    /// Version-specific data
    pub specific: X,

    /// Non-standard keywords.
    ///
    /// This will include all the keywords that do not start with '$'.
    ///
    /// Keywords which do start with '$' but are not part of the standard are
    /// considered 'deviant' and stored elsewhere since this structure will also
    /// be used to write FCS-compliant files (which do not allow nonstandard
    /// keywords starting with '$')
    pub nonstandard_keywords: NonStdKeywords,
}

/// Version-specific structured metadata derived from TEXT
type Metadata2_0 = Metadata<InnerMetadata2_0>;
type Metadata3_0 = Metadata<InnerMetadata3_0>;
type Metadata3_1 = Metadata<InnerMetadata3_1>;
type Metadata3_2 = Metadata<InnerMetadata3_2>;

#[derive(PartialEq, Clone)]
pub struct UintType<T, const LEN: usize> {
    pub bitmask: T,
    pub size: SizedByteOrd<LEN>,
}

pub type Uint08Type = UintType<u8, 1>;
pub type Uint16Type = UintType<u16, 2>;
pub type Uint24Type = UintType<u32, 3>;
pub type Uint32Type = UintType<u32, 4>;
pub type Uint40Type = UintType<u64, 5>;
pub type Uint48Type = UintType<u64, 6>;
pub type Uint56Type = UintType<u64, 7>;
pub type Uint64Type = UintType<u64, 8>;

#[derive(PartialEq, Clone)]
pub enum AnyUintType {
    Uint08(Uint08Type),
    Uint16(Uint16Type),
    Uint24(Uint24Type),
    Uint32(Uint32Type),
    Uint40(Uint40Type),
    Uint48(Uint48Type),
    Uint56(Uint56Type),
    Uint64(Uint64Type),
}

#[derive(PartialEq, Clone)]
pub struct FloatType<const LEN: usize, T> {
    pub order: SizedByteOrd<LEN>,
    pub range: T,
}

pub type SingleType = FloatType<4, f32>;
pub type DoubleType = FloatType<8, f64>;

#[derive(PartialEq, Clone)]
pub enum ColumnType {
    Ascii { bytes: u8 },
    Integer(AnyUintType),
    Float(SingleType),
    Double(DoubleType),
}

#[derive(Clone)]
pub enum DataLayout<T> {
    AsciiDelimited { nrows: Option<T>, ncols: usize },
    AlphaNum { nrows: T, columns: Vec<ColumnType> },
}

pub type ColumnLayout = DataLayout<()>;
pub type RowColumnLayout = DataLayout<Tot>;

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
    nrows: Tot,
    columns: Vec<MixedColumnType>,
}

#[derive(PartialEq, Eq, Hash, Copy, Clone)]
pub enum SizedByteOrd<const LEN: usize> {
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

struct FixedAsciiReader {
    widths: Vec<u8>,
    nrows: Tot,
}

struct DelimAsciiReader {
    ncols: usize,
    nrows: Option<Tot>,
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

pub struct FCSDateTimeError;
pub struct FCSTimeError;
pub struct FCSTime60Error;
pub struct FCSTime100Error;
pub struct FCSDateError;

pub struct AlphaNumTypeError;
pub struct NumTypeError;
pub struct EndianError;
pub struct ModifiedDateTimeError;
pub struct FeatureError;
pub struct OriginalityError;

pub enum BytesError {
    Int(ParseIntError),
    Range,
    NotOctet,
}

pub enum FixedSeqError {
    WrongLength { total: usize, expected: usize },
    BadLength,
    BadFloat,
}

pub enum NamedFixedSeqError {
    Seq(FixedSeqError),
    NonUnique,
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

pub enum ParseByteOrdError {
    InvalidOrder,
    InvalidNumbers,
}

pub enum TriggerError {
    WrongFieldNumber,
    IntFormat(std::num::ParseIntError),
}

pub enum ScaleError {
    FloatError(ParseFloatError),
    WrongFormat,
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
            AnyUintType::Uint08(ut) => {
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

pub trait VersionedMetadata: Sized {
    type P: VersionedMeasurement;

    fn as_unstainedcenters(&self) -> Option<&UnstainedCenters>;

    fn as_unstainedcenters_mut(&mut self) -> Option<&mut UnstainedCenters>;

    fn as_spillover(&self) -> Option<&Spillover>;

    fn as_spillover_mut(&mut self) -> Option<&mut Spillover>;

    fn as_compensation(&self) -> Option<&Compensation>;

    fn as_compensation_mut(&mut self) -> Option<&mut Compensation>;

    fn timestamps_valid(&self) -> bool;

    fn datetimes_valid(&self) -> bool;

    // fn check_unstainedcenters(&self, names: &HashSet<&Shortname>) -> Result<(), String>;

    // fn check_spillover(&self, names: &HashSet<&Shortname>) -> Result<(), String>;

    // fn reassign_unstainedcenters(&mut self, mapping: ShortnameMap);

    // fn reassign_spillover(&mut self, mapping: ShortnameMap);

    // fn remove_unstainedcenters_by_name(&mut self, n: &str) -> bool;

    // fn remove_spillover_by_name(&mut self, n: &str) -> bool;

    fn check_timestep(&self) -> bool;

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

    fn byteord(&self) -> ByteOrd;

    // TODO fix viz
    fn lookup_specific(st: &mut KwParser, par: Par) -> Option<Self>;

    fn lookup_tot(kws: &mut RawKeywords) -> PureMaybe<Tot>;

    fn keywords_req_inner(&self) -> Vec<(String, String)>;

    fn keywords_opt_inner(&self) -> Vec<(String, String)>;
}

pub trait VersionedMeasurement: Sized + Versioned {
    type N: IntoShortname;

    fn lookup_shortname(st: &mut KwParser, n: MeasIdx) -> Option<Self::N>;

    fn lookup_specific(st: &mut KwParser, n: MeasIdx) -> Option<Self>;

    fn has_linear_scale(&self) -> bool;

    fn has_gain(&self) -> bool;

    fn maybe_name(n: &Self::N) -> Option<&Shortname>;

    // fn shortname(&self, n: usize) -> Shortname;

    // fn set_shortname(&mut self, n: Shortname);

    fn req_suffixes_inner(&self, n: Option<MeasIdx>) -> Vec<(String, String)>;

    fn opt_suffixes_inner(&self, n: Option<MeasIdx>) -> Vec<(String, Option<String>)>;

    fn datatype(&self) -> Option<NumType>;
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
            AlphaNumType::Integer => make_uint_type(bytes, rng, byteord).map(ColumnType::Integer),
            AlphaNumType::Single => {
                if bytes == 4 {
                    Float32Type::to_float_type(byteord, rng).map(ColumnType::Float)
                } else {
                    Err(vec![format!("$DATATYPE=F but $PnB={bytes}")])
                }
            }
            AlphaNumType::Double => {
                if bytes == 8 {
                    Float64Type::to_float_type(byteord, rng).map(ColumnType::Double)
                } else {
                    Err(vec![format!("$DATATYPE=D but $PnB={bytes}")])
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

fn total_events(
    kw_tot: Option<Tot>,
    data_nbytes: usize,
    event_width: usize,
    conf: &DataReadConfig,
) -> PureSuccess<Tot> {
    let mut def = PureErrorBuf::default();
    let remainder = data_nbytes % event_width;
    let total_events = data_nbytes / event_width;
    if data_nbytes % event_width > 0 {
        let msg = format!(
            "Events are {event_width} bytes wide, but this does not evenly \
                 divide DATA segment which is {data_nbytes} bytes long \
                 (remainder of {remainder})"
        );
        def.push_msg_leveled(msg, conf.enforce_data_width_divisibility)
    }
    // TODO it seems like this could be factored out
    if let Some(tot) = kw_tot {
        if total_events != tot.0 {
            let msg = format!(
                "$TOT field is {tot} but number of events \
                         that evenly fit into DATA is {total_events}"
            );
            def.push_msg_leveled(msg, conf.enforce_matching_tot);
        }
    }
    PureSuccess {
        data: Tot(total_events),
        deferred: def,
    }
}

// hack to get bounds on error to work in IntMath trait
trait IntErr: Sized {
    fn err_kind(&self) -> &IntErrorKind;
}

impl IntErr for ParseIntError {
    fn err_kind(&self) -> &IntErrorKind {
        self.kind()
    }
}

trait IntMath: Sized
where
    Self: fmt::Display,
    Self: FromStr,
    <Self as FromStr>::Err: IntErr,
    <Self as FromStr>::Err: fmt::Display,
{
    fn next_power_2(x: Self) -> Self;

    fn int_from_str<'a>(s: &str) -> Result<Self, IntErrorKind> {
        s.parse()
            .map_err(|e| <Self as FromStr>::Err::err_kind(&e).clone())
    }

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
    Self::Native: FromStr,
    <Self::Native as FromStr>::Err: fmt::Display,
    <Self::Native as FromStr>::Err: IntErr,
    Self::Native: FromStr,
    Self: PolarsNumericType,
    ChunkedArray<Self>: IntoSeries,
{
    fn byteord_to_sized(byteord: &ByteOrd) -> Result<SizedByteOrd<INTLEN>, String> {
        byteord_to_sized(byteord)
    }

    fn range_to_bitmask(range: Range) -> Result<Self::Native, String> {
        // TODO add way to control this behavior, we may not always want to
        // truncate an overflowing number, and at the very least may wish to
        // warn the user that truncation happened
        Self::Native::int_from_str(range.0.as_str())
            .map(Self::Native::next_power_2)
            .or_else(|e| match e {
                IntErrorKind::PosOverflow => Ok(Self::Native::maxval()),
                _ => Err(format!("could not convert to u{INTLEN}")),
            })
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
    Self::Native: FromStr,
    <Self::Native as FromStr>::Err: fmt::Display,
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
        total_events: Tot,
    ) -> FloatColumnReader<Self::Native, LEN> {
        FloatColumnReader {
            column: vec![Self::Native::zero(); total_events.0],
            order,
        }
    }

    fn to_float_type(b: &ByteOrd, r: Range) -> Result<FloatType<LEN, Self::Native>, Vec<String>> {
        match (Self::to_float_byteord(b), r.0.parse::<Self::Native>()) {
            (Ok(order), Ok(range)) => Ok(FloatType { order, range }),
            (a, b) => Err([a.err(), b.err().map(|s| s.to_string())]
                .into_iter()
                .flatten()
                .collect()),
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
            AnyUintType::Uint08(_) => 1,
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
            AnyUintType::Uint08(_) => 1,
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

impl<const LEN: usize> fmt::Display for SizedByteOrd<LEN> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let s = match self {
            SizedByteOrd::Endian(e) => e.to_string(),
            SizedByteOrd::Order(o) => o.iter().join(","),
        };
        write!(f, "[{}]", s)
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

impl<P: VersionedMeasurement> Measurement<P> {}

fn make_uint_type(b: u8, r: Range, o: &ByteOrd) -> Result<AnyUintType, Vec<String>> {
    match b {
        1 => UInt8Type::to_col(r, o).map(AnyUintType::Uint08),
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
    type N = OptionalKw<Shortname>;

    fn datatype(&self) -> Option<NumType> {
        None
    }

    fn maybe_name(n: &Self::N) -> Option<&Shortname> {
        n.as_ref_opt()
    }

    fn has_linear_scale(&self) -> bool {
        self.scale.0.as_ref().map_or(false, |x| *x == Scale::Linear)
    }

    fn has_gain(&self) -> bool {
        false
    }

    // fn shortname(&self, n: usize) -> Shortname {
    //     self.shortname
    //         .0
    //         .as_ref()
    //         .cloned()
    //         .unwrap_or(Shortname::from_index(n))
    // }

    // fn set_shortname(&mut self, n: Shortname) {
    //     self.shortname = Some(n).into();
    // }

    fn lookup_shortname(st: &mut KwParser, n: MeasIdx) -> Option<Self::N> {
        Some(st.lookup_meas_opt(n, false))
    }

    fn lookup_specific(st: &mut KwParser, n: MeasIdx) -> Option<Self> {
        Some(Self {
            scale: st.lookup_meas_opt(n, false),
            wavelength: st.lookup_meas_opt(n, false),
        })
    }

    fn req_suffixes_inner(&self, _: Option<MeasIdx>) -> Vec<(String, String)> {
        vec![]
    }

    fn opt_suffixes_inner(&self, n: Option<MeasIdx>) -> Vec<(String, Option<String>)> {
        [
            OptMeasKey::pair(&self.scale, n),
            // OptMeasKey::pair(&self.shortname, n),
            OptMeasKey::pair(&self.wavelength, n),
        ]
        .into_iter()
        .collect()
    }
}

impl VersionedMeasurement for InnerMeasurement3_0 {
    type N = OptionalKw<Shortname>;

    fn datatype(&self) -> Option<NumType> {
        None
    }

    fn maybe_name(n: &Self::N) -> Option<&Shortname> {
        n.as_ref_opt()
    }

    fn has_linear_scale(&self) -> bool {
        self.scale == Scale::Linear
    }

    fn has_gain(&self) -> bool {
        self.gain.0.is_some()
    }

    // fn shortname(&self, n: usize) -> Shortname {
    //     self.shortname
    //         .0
    //         .as_ref()
    //         .cloned()
    //         .unwrap_or(Shortname::from_index(n))
    // }

    // fn set_shortname(&mut self, n: Shortname) {
    //     self.shortname = Some(n).into()
    // }

    fn lookup_shortname(st: &mut KwParser, n: MeasIdx) -> Option<Self::N> {
        Some(st.lookup_meas_opt(n, false))
    }

    fn lookup_specific(st: &mut KwParser, n: MeasIdx) -> Option<Self> {
        Some(Self {
            scale: st.lookup_meas_req(n)?,
            gain: st.lookup_meas_opt(n, false),
            wavelength: st.lookup_meas_opt(n, false),
        })
    }

    fn req_suffixes_inner(&self, n: Option<MeasIdx>) -> Vec<(String, String)> {
        [self.scale.pair(n)].into_iter().collect()
    }

    fn opt_suffixes_inner(&self, n: Option<MeasIdx>) -> Vec<(String, Option<String>)> {
        [
            // OptMeasKey::pair(&self.shortname, n),
            OptMeasKey::pair(&self.wavelength, n),
            OptMeasKey::pair(&self.gain, n),
        ]
        .into_iter()
        .collect()
    }
}

impl VersionedMeasurement for InnerMeasurement3_1 {
    type N = Shortname;

    fn datatype(&self) -> Option<NumType> {
        None
    }

    fn maybe_name(n: &Self::N) -> Option<&Shortname> {
        Some(n)
    }

    fn has_linear_scale(&self) -> bool {
        self.scale == Scale::Linear
    }

    fn has_gain(&self) -> bool {
        self.gain.0.is_some()
    }

    // fn shortname(&self, _: usize) -> Shortname {
    //     self.shortname.clone()
    // }

    // fn set_shortname(&mut self, n: Shortname) {
    //     self.shortname = n
    // }

    fn lookup_shortname(st: &mut KwParser, n: MeasIdx) -> Option<Self::N> {
        st.lookup_meas_req(n)
    }

    fn lookup_specific(st: &mut KwParser, n: MeasIdx) -> Option<Self> {
        if let Some(scale) = st.lookup_meas_req(n) {
            Some(Self {
                scale,
                gain: st.lookup_meas_opt(n, false),
                wavelengths: st.lookup_meas_opt(n, false),
                calibration: st.lookup_meas_opt(n, false),
                display: st.lookup_meas_opt(n, false),
            })
        } else {
            None
        }
    }

    fn req_suffixes_inner(&self, n: Option<MeasIdx>) -> Vec<(String, String)> {
        [
            self.scale.pair(n),
            // self.shortname.pair(n)
        ]
        .into_iter()
        .collect()
    }

    fn opt_suffixes_inner(&self, n: Option<MeasIdx>) -> Vec<(String, Option<String>)> {
        [
            OptMeasKey::pair(&self.wavelengths, n),
            OptMeasKey::pair(&self.gain, n),
            OptMeasKey::pair(&self.calibration, n),
            OptMeasKey::pair(&self.display, n),
        ]
        .into_iter()
        .collect()
    }
}

impl VersionedMeasurement for InnerMeasurement3_2 {
    type N = Shortname;

    fn datatype(&self) -> Option<NumType> {
        self.datatype.0.as_ref().copied()
    }

    fn maybe_name(n: &Self::N) -> Option<&Shortname> {
        Some(n)
    }

    fn has_linear_scale(&self) -> bool {
        self.scale == Scale::Linear
    }

    fn has_gain(&self) -> bool {
        self.gain.0.is_some()
    }

    // fn shortname(&self, _: usize) -> Shortname {
    //     self.shortname.clone()
    // }

    // fn set_shortname(&mut self, n: Shortname) {
    //     self.shortname = n
    // }

    fn lookup_shortname(st: &mut KwParser, n: MeasIdx) -> Option<Self::N> {
        st.lookup_meas_req(n)
    }

    fn lookup_specific(st: &mut KwParser, n: MeasIdx) -> Option<Self> {
        if let Some(scale) = st.lookup_meas_req(n) {
            Some(Self {
                scale,
                gain: st.lookup_meas_opt(n, false),
                wavelengths: st.lookup_meas_opt(n, false),
                calibration: st.lookup_meas_opt(n, false),
                display: st.lookup_meas_opt(n, false),
                detector_name: st.lookup_meas_opt(n, false),
                tag: st.lookup_meas_opt(n, false),
                measurement_type: st.lookup_meas_opt(n, false),
                feature: st.lookup_meas_opt(n, false),
                analyte: st.lookup_meas_opt(n, false),
                datatype: st.lookup_meas_opt(n, false),
            })
        } else {
            None
        }
    }

    fn req_suffixes_inner(&self, n: Option<MeasIdx>) -> Vec<(String, String)> {
        [
            self.scale.pair(n),
            // self.shortname.pair(n)
        ]
        .into_iter()
        .collect()
    }

    fn opt_suffixes_inner(&self, n: Option<MeasIdx>) -> Vec<(String, Option<String>)> {
        [
            OptMeasKey::pair(&self.wavelengths, n),
            OptMeasKey::pair(&self.gain, n),
            OptMeasKey::pair(&self.calibration, n),
            OptMeasKey::pair(&self.display, n),
            OptMeasKey::pair(&self.detector_name, n),
            OptMeasKey::pair(&self.tag, n),
            OptMeasKey::pair(&self.measurement_type, n),
            OptMeasKey::pair(&self.feature, n),
            OptMeasKey::pair(&self.analyte, n),
            OptMeasKey::pair(&self.datatype, n),
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

    // pub fn trigger(&self) -> Option<&Trigger> {
    //     match_anycoretext!(self, x, { x.trigger() })
    // }

    // pub fn set_trigger(&mut self, t: Option<Trigger>) {
    //     match_anycoretext!(self, x, { x.set_trigger(t) })
    // }

    pub fn datatype(&self) -> AlphaNumType {
        match_anycoretext!(self, x, { x.datatype() })
    }

    pub fn set_datatype(&mut self, t: AlphaNumType) {
        match_anycoretext!(self, x, { x.set_datatype(t) })
    }

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

    pub fn into_2_0(self) -> PureMaybe<CoreTEXT2_0> {
        match_anycoretext!(self, x, { x.try_convert() })
    }

    pub fn into_3_0(self) -> PureMaybe<CoreTEXT3_0> {
        match_anycoretext!(self, x, { x.try_convert() })
    }

    pub fn into_3_1(self) -> PureMaybe<CoreTEXT3_1> {
        match_anycoretext!(self, x, { x.try_convert() })
    }

    pub fn into_3_2(self) -> PureMaybe<CoreTEXT3_2> {
        match_anycoretext!(self, x, { x.try_convert() })
    }

    pub fn text_segment(
        &self,
        tot: Tot,
        data_len: usize,
        analysis_len: usize,
    ) -> Option<Vec<String>> {
        match_anycoretext!(self, x, { x.text_segment(tot, data_len, analysis_len) })
    }

    pub fn par(&self) -> Par {
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

    fn set_shortnames(&mut self, names: Vec<Shortname>) -> Result<NameMapping, String> {
        match_anycoretext!(self, x, { x.set_shortnames(names) })
    }

    fn remove_measurement(&mut self, n: &Shortname) -> Option<usize> {
        match_anycoretext!(self, x, { x.remove_measurement(n).map(|x| x.0) })
    }

    // fn set_df_column_names(&self, df: &mut DataFrame) -> PolarsResult<()> {
    //     match_anycoretext!(self, x, { x.set_df_column_names(df) })
    // }

    pub fn as_column_layout(&self) -> Result<ColumnLayout, Vec<String>> {
        match_anycoretext!(self, x, { x.as_column_layout() })
    }

    fn as_data_reader(
        &self,
        kws: &mut RawKeywords,
        conf: &DataReadConfig,
        data_seg: &Segment,
    ) -> PureMaybe<DataReader> {
        match_anycoretext!(self, x, { x.as_data_reader(kws, conf, data_seg) })
    }

    fn into_dataset_unchecked(self, data: DataFrame, analysis: Analysis) -> AnyCoreDataset {
        match self {
            AnyCoreTEXT::FCS2_0(text) => {
                AnyCoreDataset::FCS2_0(text.into_dataset_unchecked(data, analysis))
            }
            AnyCoreTEXT::FCS3_0(text) => {
                AnyCoreDataset::FCS3_0(text.into_dataset_unchecked(data, analysis))
            }
            AnyCoreTEXT::FCS3_1(text) => {
                AnyCoreDataset::FCS3_1(text.into_dataset_unchecked(data, analysis))
            }
            AnyCoreTEXT::FCS3_2(text) => {
                AnyCoreDataset::FCS3_2(text.into_dataset_unchecked(data, analysis))
            }
        }
    }

    fn into_dataset(self, data: DataFrame, analysis: Analysis) -> Result<AnyCoreDataset, String> {
        match self {
            AnyCoreTEXT::FCS2_0(text) => text
                .into_dataset(data, analysis)
                .map(AnyCoreDataset::FCS2_0),
            AnyCoreTEXT::FCS3_0(text) => text
                .into_dataset(data, analysis)
                .map(AnyCoreDataset::FCS3_0),
            AnyCoreTEXT::FCS3_1(text) => text
                .into_dataset(data, analysis)
                .map(AnyCoreDataset::FCS3_1),
            AnyCoreTEXT::FCS3_2(text) => text
                .into_dataset(data, analysis)
                .map(AnyCoreDataset::FCS3_2),
        }
    }
}

impl AnyCoreDataset {
    pub fn version(&self) -> Version {
        match self {
            AnyCoreDataset::FCS2_0(_) => Version::FCS2_0,
            AnyCoreDataset::FCS3_0(_) => Version::FCS3_0,
            AnyCoreDataset::FCS3_1(_) => Version::FCS3_1,
            AnyCoreDataset::FCS3_2(_) => Version::FCS3_2,
        }
    }

    // TODO this would be much more elegant with real rankN support, see:
    // https://github.com/rust-lang/rust/issues/108185
    pub fn into_parts(self) -> (AnyCoreTEXT, DataFrame, Analysis) {
        match self {
            AnyCoreDataset::FCS2_0(x) => (AnyCoreTEXT::FCS2_0(x.text), x.data, x.analysis),
            AnyCoreDataset::FCS3_0(x) => (AnyCoreTEXT::FCS3_0(x.text), x.data, x.analysis),
            AnyCoreDataset::FCS3_1(x) => (AnyCoreTEXT::FCS3_1(x.text), x.data, x.analysis),
            AnyCoreDataset::FCS3_2(x) => (AnyCoreTEXT::FCS3_2(x.text), x.data, x.analysis),
        }
    }

    pub fn as_analysis(&self) -> &Analysis {
        match_many_to_one!(self, AnyCoreDataset, [FCS2_0, FCS3_0, FCS3_1, FCS3_2], x, {
            &x.analysis
        })
    }

    pub fn as_data(&self) -> &DataFrame {
        match_many_to_one!(self, AnyCoreDataset, [FCS2_0, FCS3_0, FCS3_1, FCS3_2], x, {
            &x.data
        })
    }

    pub fn as_data_mut(&mut self) -> &mut DataFrame {
        match_many_to_one!(self, AnyCoreDataset, [FCS2_0, FCS3_0, FCS3_1, FCS3_2], x, {
            &mut x.data
        })
    }
}

macro_rules! coredataset_as_text {
    ($this:expr, $bind:ident, $f:expr) => {
        match_many_to_one!(
            $this,
            AnyCoreDataset,
            [FCS2_0, FCS3_0, FCS3_1, FCS3_2],
            x,
            {
                let $bind = &(*x.text);
                $f
            }
        )
    };
}

macro_rules! coredataset_as_text_mut {
    ($this:expr, $bind:ident, $f:expr) => {
        match_many_to_one!(
            $this,
            AnyCoreDataset,
            [FCS2_0, FCS3_0, FCS3_1, FCS3_2],
            x,
            {
                let $bind = &mut (*x.text);
                $f
            }
        )
    };
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

fn build_mixed_reader(cs: Vec<ColumnType>, total_events: Tot) -> MixedParser {
    let columns = cs
        .into_iter()
        .map(|p| match p {
            ColumnType::Ascii { bytes } => MixedColumnType::Ascii(AsciiColumnReader {
                width: bytes,
                column: vec![],
            }),
            ColumnType::Float(t) => {
                MixedColumnType::Single(Float32Type::make_column_reader(t.order, total_events))
            }
            ColumnType::Double(t) => {
                MixedColumnType::Double(Float64Type::make_column_reader(t.order, total_events))
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

fn build_data_reader(layout: RowColumnLayout, data_seg: &Segment) -> DataReader {
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

impl<P> Measurement<P>
where
    P: VersionedMeasurement,
{
    fn longname(&self, n: usize) -> Longname {
        // TODO not DRY
        self.longname
            .0
            .as_ref()
            .cloned()
            .unwrap_or(Longname(format!("M{n}")))
    }

    fn set_longname(&mut self, n: Option<String>) {
        self.longname = n.map(|y| y.into()).into();
    }

    fn lookup_measurements(st: &mut KwParser, par: Par) -> Option<DistinctVec<P::N, Self>> {
        let v = P::fcs_version();
        let ps: Vec<_> = (1..(par.0 + 1))
            .flat_map(|n| {
                let i = MeasIdx(n);
                let maybe_bytes = st.lookup_meas_req(i);
                let maybe_range = st.lookup_meas_req(i);
                let maybe_specific = P::lookup_specific(st, i);
                let maybe_name = P::lookup_shortname(st, i);
                if let (Some(bytes), Some(range), Some(specific), Some(key)) =
                    (maybe_bytes, maybe_range, maybe_specific, maybe_name)
                {
                    Some((
                        key,
                        Measurement {
                            bytes,
                            range,
                            longname: st.lookup_meas_opt(i, false),
                            filter: st.lookup_meas_opt(i, false),
                            power: st.lookup_meas_opt(i, false),
                            detector_type: st.lookup_meas_opt(i, false),
                            percent_emitted: st.lookup_meas_opt(i, v == Version::FCS3_2),
                            detector_voltage: st.lookup_meas_opt(i, false),
                            specific,
                            nonstandard_keywords: st.lookup_all_meas_nonstandard(i),
                        },
                    ))
                } else {
                    None
                }
            })
            .collect();
        if ps.len() == par.0 {
            let dv = DistinctVec::from_vec(ps, ShortnamePrefix::default());
            if dv.is_none() {
                let msg = "Not all measurement names are unique".to_string();
                st.deferred.push_error(msg);
            }
            dv
        } else {
            // ASSUME errors were capture elsewhere
            None
        }
    }

    fn req_suffixes(&self, n: Option<MeasIdx>) -> Vec<(String, String)> {
        [self.bytes.pair(n), self.range.pair(n)]
            .into_iter()
            .chain(self.specific.req_suffixes_inner(n))
            .collect()
    }

    fn opt_suffixes(&self, n: Option<MeasIdx>) -> Vec<(String, Option<String>)> {
        [
            OptMeasKey::pair(&self.longname, n),
            OptMeasKey::pair(&self.filter, n),
            OptMeasKey::pair(&self.power, n),
            OptMeasKey::pair(&self.detector_type, n),
            OptMeasKey::pair(&self.percent_emitted, n),
            OptMeasKey::pair(&self.detector_voltage, n),
        ]
        .into_iter()
        .chain(self.specific.opt_suffixes_inner(n))
        .collect()
    }

    // for table
    fn keywords(&self, n: Option<MeasIdx>) -> Vec<(String, Option<String>)> {
        self.req_suffixes(n)
            .into_iter()
            .map(|(k, v)| (k, Some(v)))
            .chain(self.opt_suffixes(n))
            .collect()
    }

    fn table_header(&self) -> Vec<String> {
        vec![String::from("index")]
            .into_iter()
            .chain(self.keywords(None).into_iter().map(|(k, _)| k))
            .collect()
    }

    fn table_row(&self, n: usize) -> Vec<Option<String>> {
        vec![Some(n.to_string())]
            .into_iter()
            // NOTE; the None is a dummy and never used
            .chain(self.keywords(None).into_iter().map(|(_, v)| v))
            .collect()
    }

    // TODO this name is weird, this is standard+nonstandard keywords
    // after filtering out None values
    fn req_keywords(&self, n: Option<MeasIdx>) -> RawPairs {
        self.req_suffixes(n).into_iter().collect()
    }

    fn opt_keywords(&self, n: Option<MeasIdx>) -> RawPairs {
        self.opt_suffixes(n)
            .into_iter()
            .filter_map(|(k, v)| v.map(|x| (k, x)))
            .chain(
                self.nonstandard_keywords
                    .iter()
                    .map(|(k, v)| (k.as_ref().to_string(), v.clone())),
            )
            .collect()
    }

    fn as_column_type(
        &self,
        dt: AlphaNumType,
        byteord: &ByteOrd,
    ) -> Result<Option<ColumnType>, Vec<String>> {
        let mdt = self.specific.datatype().map(|d| d.into()).unwrap_or(dt);
        let rng = self.range.clone();
        to_col_type(self.bytes, mdt, byteord, rng)
    }
}

impl<M> Metadata<M>
where
    M: VersionedMetadata,
{
    fn lookup_metadata(
        st: &mut KwParser,
        ms: &DistinctVec<<M::P as VersionedMeasurement>::N, Measurement<M::P>>,
    ) -> Option<Self> {
        let par = Par(ms.len());
        let maybe_datatype = st.lookup_meta_req();
        let maybe_specific = M::lookup_specific(st, par);
        if let (Some(datatype), Some(specific)) = (maybe_datatype, maybe_specific) {
            Some(Metadata {
                datatype,
                abrt: st.lookup_meta_opt(false),
                com: st.lookup_meta_opt(false),
                cells: st.lookup_meta_opt(false),
                exp: st.lookup_meta_opt(false),
                fil: st.lookup_meta_opt(false),
                inst: st.lookup_meta_opt(false),
                lost: st.lookup_meta_opt(false),
                op: st.lookup_meta_opt(false),
                proj: st.lookup_meta_opt(false),
                smno: st.lookup_meta_opt(false),
                src: st.lookup_meta_opt(false),
                sys: st.lookup_meta_opt(false),
                tr: st.lookup_meta_opt(false),
                nonstandard_keywords: st.lookup_all_nonstandard(),
                specific,
            })
        } else {
            None
        }
    }

    fn all_req_keywords(&self, par: Par) -> RawPairs {
        [par.pair(), self.datatype.pair()]
            .into_iter()
            .chain(self.specific.keywords_req_inner())
            .map(|(k, v)| (k.to_string(), v))
            .collect()
    }

    fn all_opt_keywords(&self) -> RawPairs {
        [
            OptMetaKey::pair(&self.abrt),
            OptMetaKey::pair(&self.com),
            OptMetaKey::pair(&self.cells),
            OptMetaKey::pair(&self.exp),
            OptMetaKey::pair(&self.fil),
            OptMetaKey::pair(&self.inst),
            OptMetaKey::pair(&self.lost),
            OptMetaKey::pair(&self.op),
            OptMetaKey::pair(&self.proj),
            OptMetaKey::pair(&self.smno),
            OptMetaKey::pair(&self.src),
            OptMetaKey::pair(&self.sys),
            OptMetaKey::pair(&self.tr),
        ]
        .into_iter()
        .flat_map(|(k, v)| v.map(|x| (k, x)))
        .chain(self.specific.keywords_opt_inner())
        .map(|(k, v)| (k.to_string(), v))
        // TODO useless clone
        .chain(
            self.nonstandard_keywords
                .iter()
                .map(|(k, v)| (k.as_ref().to_string(), v.clone())),
        )
        .collect()
    }

    fn reassign_trigger(&mut self, mapping: &NameMapping) {
        self.tr.0.as_mut().map_or((), |tr| tr.reassign(mapping))
    }

    fn reassign_all(&mut self, mapping: &NameMapping) {
        self.reassign_trigger(mapping);
        self.specific
            .as_spillover_mut()
            .map(|x| x.reassign(mapping));
        self.specific
            .as_unstainedcenters_mut()
            .map(|x| x.reassign(mapping));
    }

    fn check_trigger(&self, names: &HashSet<&Shortname>) -> Result<(), String> {
        self.tr.0.as_ref().map_or(Ok(()), |tr| tr.check_link(names))
    }

    fn check_unstainedcenters(&self, names: &HashSet<&Shortname>) -> Result<(), String> {
        self.specific
            .as_unstainedcenters()
            .map_or(Ok(()), |x| x.check_link(names))
    }

    fn check_spillover(&self, names: &HashSet<&Shortname>) -> Result<(), String> {
        self.specific
            .as_spillover()
            .map_or(Ok(()), |x| x.check_link(names))
    }

    fn remove_trigger_by_name(&mut self, n: &Shortname) -> bool {
        if self.tr.as_ref_opt().is_some_and(|m| &m.measurement == n) {
            self.tr = None.into();
            true
        } else {
            false
        }
    }
}

impl<M> CoreTEXT<M, M::P, <M::P as VersionedMeasurement>::N>
where
    M: VersionedMetadata,
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
    fn text_segment(&self, tot: Tot, data_len: usize, analysis_len: usize) -> Option<Vec<String>> {
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
        let (req_meas, req_meta, _) =
            self.some_keywords(Measurement::req_keywords, Metadata::all_req_keywords);
        let (opt_meas, opt_meta, _) = self.some_keywords(Measurement::opt_keywords, |m, _| {
            Metadata::all_opt_keywords(m)
        });

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

    // TODO do I really need these?
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

    // pub fn trigger(&self) -> Option<&Trigger> {
    //     self.metadata.tr.as_ref().into()
    // }

    // pub fn set_trigger(&mut self, t: Option<Trigger>) {
    //     self.metadata.tr = t.into()
    // }

    pub fn datatype(&self) -> AlphaNumType {
        self.metadata.datatype
    }

    pub fn set_datatype(&mut self, t: AlphaNumType) {
        self.metadata.datatype = t
    }

    fn header_and_raw_keywords(
        &self,
        tot: Tot,
        data_len: usize,
        analysis_len: usize,
    ) -> Option<(String, RawKeywords)> {
        let version = M::P::fcs_version();
        let tot_pair = (Tot::std().to_string(), tot.to_string());

        let (req_meas, req_meta, req_text_len) =
            self.some_keywords(Measurement::req_keywords, Metadata::all_req_keywords);
        let (opt_meas, opt_meta, opt_text_len) = self
            .some_keywords(Measurement::opt_keywords, |m, _| {
                Metadata::all_opt_keywords(m)
            });

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
    pub fn shortnames(&self) -> Vec<Shortname> {
        self.measurements.iter_names().collect()
    }

    /// Set all $PnN keywords to list of names.
    ///
    /// The length of the names must match the number of measurements. Any
    /// keywords refering to the old names will be updated to reflect the new
    /// names. For 2.0 and 3.0 which have optional $PnN, all $PnN will end up
    /// being set.
    pub fn set_shortnames(&mut self, ns: Vec<Shortname>) -> Result<NameMapping, String> {
        let mapping = self.measurements.set_names(ns).map_err(|e| e.to_string())?;
        // TODO reassign names in dataframe
        self.metadata.reassign_all(&mapping);
        Ok(mapping)
    }

    // TODO what if the time channel is removed?
    pub fn remove_measurement(
        &mut self,
        n: &Shortname,
    ) -> Option<(usize, <M::P as VersionedMeasurement>::N, Measurement<M::P>)> {
        if let Some((i, k, v)) = self.measurements.remove_name(n) {
            let m = &mut self.metadata;
            m.remove_trigger_by_name(n);
            let s = &mut m.specific;
            s.as_spillover_mut().map(|x| x.remove_by_name(n));
            s.as_unstainedcenters_mut().map(|x| x.remove_by_name(n));
            s.as_compensation_mut().map(|x| x.remove_by_index(i));
            Some((i, k, v))
        } else {
            None
        }

        // let mi: Option<usize> = self
        //     .measurements
        //     .iter_keys()
        //     .position(|k| M::P::maybe_name(k).is_some_and(|x| x.as_ref() == n));
        // if let Some(i) = mi {
        //     self.measurements.remove_index_unchecked(i);
        //     let m = &mut self.metadata;
        //     m.remove_trigger_by_name(n);
        //     let s = &mut m.specific;
        //     s.as_spillover_mut().map(|x| x.remove_by_name(n));
        //     s.as_unstainedcenters_mut().map(|x| x.remove_by_name(n));
        //     s.as_compensation_mut().map(|x| x.remove_by_index(i));
        //     true
        // } else {
        //     false
        // }
    }

    pub fn check_index(&self, i: usize) -> Result<(), String> {
        let p = self.measurements.len();
        if i > p {
            Err(format!("Index must be {p} or less, got {i}"))
        } else {
            Ok(())
        }
    }

    // TODO might be good to wrap the entire measurement vector into something
    // that can only be updated if it passes some uniqueness test. It needs to
    // be done manually since we can't use a hashset.
    pub fn add_measurement(
        &mut self,
        i: usize,
        n: <M::P as VersionedMeasurement>::N,
        m: Measurement<M::P>,
    ) -> Result<Shortname, String> {
        self.measurements.insert(i, n, m).map_err(|e| e.to_string())
        // self.check_index(i).and_then(|_| {
        //     let newname = m.specific.maybe_name();
        //     if self.measurements.iter().any(|m| {
        //         m.specific
        //             .maybe_name()
        //             .zip(newname)
        //             .is_some_and(|(a, b)| a == b)
        //     }) {
        //         Err("Measurement with same name already present".to_string())
        //     } else {
        //         self.measurements.insert(i, n, m);
        //         Ok(())
        //     }
        // })
    }

    fn df_names(&self) -> Vec<PlSmallStr> {
        self.shortnames()
            .into_iter()
            .map(|s| s.as_ref().into())
            .collect()
    }

    fn set_df_column_names(&self, df: &mut DataFrame) -> PolarsResult<()> {
        df.set_column_names(self.df_names())
    }

    /// Return a list of measurement names as stored in $PnS
    ///
    /// If not given, will be replaced by "Mn" where "n" is the measurement
    /// index starting at 1.
    pub fn longnames(&self) -> Vec<Longname> {
        self.measurements
            .iter_values()
            .enumerate()
            .map(|(i, m)| m.longname(i))
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
            for (m, n) in self.measurements.iter_values_mut().zip(ns) {
                m.set_longname(n)
            }
            true
        }
    }

    pub fn par(&self) -> Par {
        Par(self.measurements.len())
    }

    fn some_keywords<F, G>(&self, f: F, g: G) -> (RawPairs, RawPairs, usize)
    where
        F: Fn(&Measurement<M::P>, Option<MeasIdx>) -> RawPairs,
        G: Fn(&Metadata<M>, Par) -> RawPairs,
    {
        let meas: Vec<_> = self
            .measurements
            .iter_values()
            .enumerate()
            .flat_map(|(i, m)| f(m, Some(MeasIdx(i + 1))))
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
        // TODO add back header
        // let header = ms[0].table_header().join(delim);
        let rows = self.measurements.iter_values().enumerate().map(|(i, m)| {
            m.table_row(i)
                .into_iter()
                .map(|v| v.unwrap_or("NA".into()))
                .join(delim)
        });
        // vec![header].into_iter().chain(rows).collect()
        rows.collect()
    }

    fn print_meas_table(&self, delim: &str) {
        for e in self.meas_table(delim) {
            println!("{}", e);
        }
    }

    // NOTE return vec of strings in error term because these will generally
    // always be errors on failure with no warnings on success.
    pub fn as_column_layout(&self) -> Result<ColumnLayout, Vec<String>> {
        let dt = self.metadata.datatype;
        let byteord = self.metadata.specific.byteord();
        let ncols = self.measurements.len();
        let (pass, fail): (Vec<_>, Vec<_>) = self
            .measurements
            .iter_values()
            .map(|m| m.as_column_type(dt, &byteord))
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

    fn add_tot(
        dl: ColumnLayout,
        kws: &mut RawKeywords,
        conf: &DataReadConfig,
        data_seg: &Segment,
    ) -> PureSuccess<RowColumnLayout> {
        M::lookup_tot(kws).and_then(|tot| match dl {
            DataLayout::AsciiDelimited { nrows: _, ncols } => {
                PureSuccess::from(DataLayout::AsciiDelimited { nrows: tot, ncols })
            }
            DataLayout::AlphaNum { nrows: _, columns } => {
                let data_nbytes = data_seg.nbytes() as usize;
                let event_width = columns.iter().map(|c| c.width()).sum();
                total_events(tot, data_nbytes, event_width, conf)
                    .map(|nrows| DataLayout::AlphaNum { nrows, columns })
            }
        })
    }

    fn as_row_column_layout(
        &self,
        kws: &mut RawKeywords,
        conf: &DataReadConfig,
        data_seg: &Segment,
    ) -> PureMaybe<RowColumnLayout> {
        PureMaybe::from_result_strs(self.as_column_layout(), PureErrorLevel::Error)
            .and_then_opt(|dl| Self::add_tot(dl, kws, conf, data_seg).map(Some))
    }

    fn as_data_reader(
        &self,
        kws: &mut RawKeywords,
        conf: &DataReadConfig,
        data_seg: &Segment,
    ) -> PureMaybe<DataReader> {
        self.as_row_column_layout(kws, conf, data_seg)
            .map(|maybe_layout| maybe_layout.map(|layout| build_data_reader(layout, data_seg)))
    }

    fn new_from_raw(kws: &mut RawKeywords, conf: &StdTextReadConfig) -> PureResult<Self> {
        // Lookup $PAR first; everything depends on this since we need to know
        // the number of measurements to find which are used in turn for
        // validating lots of keywords in metadata. If we fail we need to bail.
        //
        // TODO this isn't entirely true; there are some things that can be
        // tested without $PAR, so if we really wanted to be aggressive we could
        // pass $PAR as an Option and only test if not None when we need it and
        // possibly bail. Might be worth it.
        let par = Failure::from_result(Par::lookup_meta_req(kws))?;
        // Lookup measurements+metadata based on $PAR, which also might fail in
        // a zillion ways. If this fails we need to bail since we cannot create
        // a struct with missing fields.
        let md_fail = "could not standardize TEXT".to_string();
        let c: KwParserConfig = conf.into();
        let md_succ = KwParser::try_run(kws, c, md_fail, |st| {
            if let Some(measurements) = Measurement::lookup_measurements(st, par) {
                Metadata::lookup_metadata(st, &measurements).map(|metadata| CoreTEXT {
                    metadata,
                    measurements,
                })
            } else {
                None
            }
        })?;
        // hooray, we win and can now make the core struct
        Ok(md_succ.and_then(|core| core.validate(&conf.time).map(|_| core)))
    }

    // TODO add non-kw deprecation checker

    fn measurement_names(&self) -> Vec<&Shortname> {
        self.measurements
            .iter_keys()
            .filter_map(|k| M::P::maybe_name(k))
            .collect()
    }

    // fn unique_meaurement_names(&self) -> Option<HashSet<&Shortname>> {
    //     try_unique(self.measurement_names().as_slice())
    // }

    fn validate(&self, conf: &TimeConfig) -> PureSuccess<()> {
        let mut deferred = PureErrorBuf::default();

        // TODO check that measurmenttype is "Time" if time channel

        // Ensure $PnN are unique; if this fails we can't make a dataframe, so
        // failure will always be an error and not a warning.
        let names: HashSet<_> = self.measurement_names().into_iter().collect();
        if let Err(msg) = self.metadata.check_trigger(&names) {
            deferred.push_error(msg);
        }

        if let Err(msg) = self.metadata.check_unstainedcenters(&names) {
            // TODO toggle this
            deferred.push_error(msg);
        }

        if let Err(msg) = self.metadata.check_spillover(&names) {
            // TODO toggle this
            deferred.push_error(msg);
        }

        // Ensure time measurement is valid
        if let Some(time_name) = &conf.shortname {
            // Find the time measurement, check lots of stuff if it exists
            if let Some(time_meas) = self
                .measurements
                .iter()
                .find(|(k, _)| M::P::maybe_name(k) == Some(time_name))
                .map(|x| x.1)
            {
                // check that $TIMESTEP exists
                if !self.metadata.specific.check_timestep() {
                    let msg = "$TIMESTEP must be present if time measurement present".into();
                    deferred.push_msg_leveled(msg, conf.ensure_timestep)
                }
                // check that $PnE is linear
                if !time_meas.specific.has_linear_scale() {
                    let msg = "Time measurement must have linear $PnE".into();
                    deferred.push_msg_leveled(msg, conf.ensure_linear);
                }
                // check that $PnG doesn't exist
                if time_meas.specific.has_gain() {
                    let msg = "Time measurement should not have $PnG".into();
                    deferred.push_msg_leveled(msg, conf.ensure_nogain);
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

    fn into_dataset_unchecked(
        self,
        data: DataFrame,
        analysis: Analysis,
    ) -> CoreDataset<M, M::P, <M::P as VersionedMeasurement>::N> {
        let ns = self.df_names();
        let mut data = data;
        data.set_column_names(ns).unwrap();
        CoreDataset {
            text: Box::new(self),
            data,
            analysis,
        }
    }

    fn into_dataset(
        self,
        data: DataFrame,
        analysis: Analysis,
    ) -> Result<CoreDataset<M, M::P, <M::P as VersionedMeasurement>::N>, String> {
        let w = data.width();
        let p = self.measurements.len();
        if w != p {
            Err(format!(
                "DATA has {w} columns but TEXT has {p} measurements"
            ))
        } else {
            Ok(self.into_dataset_unchecked(data, analysis))
        }
    }

    fn try_convert<ToM>(
        self,
    ) -> PureMaybe<CoreTEXT<ToM, ToM::P, <ToM::P as VersionedMeasurement>::N>>
    where
        M: IntoMetadata<ToM>,
        M::P: IntoMeasurement<ToM::P>,
        ToM: VersionedMetadata,
        ToM::P: VersionedMeasurement,
    {
        let ps = self
            .measurements
            .map_values(|i, v| IntoMeasurement::try_convert(v, MeasIdx(i)));
        let m = IntoMetadata::try_convert(self.metadata);
        let res = match (m, ps) {
            (Ok(metadata), Ok(old_ps)) => {
                if let Some(measurements) = old_ps.try_new_names() {
                    Ok(CoreTEXT {
                        metadata,
                        measurements,
                    })
                } else {
                    Err(vec!["Some $PnN are blank and could not be converted".into()])
                }
            }
            (a, bs) => Err(bs
                .err()
                .unwrap_or_default()
                .concat()
                .into_iter()
                .chain(a.err().unwrap_or_default())
                .collect()),
        };
        PureMaybe::from_result_strs(res, PureErrorLevel::Error)
    }
}

impl<M> CoreDataset<M, M::P, <M::P as VersionedMeasurement>::N>
where
    M: VersionedMetadata,
{
    fn set_shortnames(&mut self, names: Vec<Shortname>) -> Result<NameMapping, String> {
        self.text
            .set_shortnames(names)
            .inspect(|_| self.text.set_df_column_names(&mut self.data).unwrap())
    }

    // TODO also make a version of this that takes an index since not all
    // columns are named or we might not know the name
    fn remove_measurement(&mut self, n: &Shortname) -> Option<usize> {
        let i = self.text.remove_measurement(n)?;
        self.data.drop_in_place(n.as_ref()).unwrap();
        Some(i.0)
    }

    fn add_measurement<T>(
        &mut self,
        i: usize,
        n: <M::P as VersionedMeasurement>::N,
        m: Measurement<M::P>,
        col: Vec<T::Native>,
    ) -> Result<Shortname, String>
    where
        T: PolarsNumericType,
        ChunkedArray<T>: IntoSeries,
    {
        let k = self
            .text
            .measurements
            .insert(i, n, m)
            .map_err(|e| e.to_string())?;
        let ser = ChunkedArray::<T>::from_vec(k.as_ref().into(), col).into_series();
        self.data.insert_column(i, ser).map_err(|e| e.to_string())?;
        Ok(k)
    }
}

fn try_unique<'a, T: Eq + Hash>(xs: &[&'a T]) -> Option<HashSet<&'a T>> {
    let mut unique = HashSet::new();
    for x in xs {
        if unique.contains(x) {
            return None;
        } else {
            unique.insert(*x);
        }
    }
    Some(unique)
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
        ColumnType::Float(t) => {
            match dt {
                ValidType::U32 => num_warn(&mut deferred, "float", "uint32"),
                ValidType::U64 => num_warn(&mut deferred, "float", "uint64"),
                ValidType::F64 => num_warn(&mut deferred, "float", "double"),
                _ => (),
            }
            match dt {
                ValidType::U08 => convert_to_f32!(t.order, c, u8),
                ValidType::U16 => convert_to_f32!(t.order, c, u16),
                ValidType::U32 => convert_to_f32!(t.order, c, u32),
                ValidType::U64 => convert_to_f32!(t.order, c, u64),
                ValidType::F32 => convert_to_f32!(t.order, c, f32),
                ValidType::F64 => convert_to_f32!(t.order, c, f64),
            }
        }

        // Doubles can hold all but uint64
        ColumnType::Double(t) => {
            if let ValidType::U64 = dt {
                num_warn(&mut deferred, "double", "uint64")
            }
            match dt {
                ValidType::U08 => convert_to_f64!(t.order, c, u8),
                ValidType::U16 => convert_to_f64!(t.order, c, u16),
                ValidType::U32 => convert_to_f64!(t.order, c, u32),
                ValidType::U64 => convert_to_f64!(t.order, c, u64),
                ValidType::F32 => convert_to_f64!(t.order, c, f32),
                ValidType::F64 => convert_to_f64!(t.order, c, f64),
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
            // TODO this name is deceptive because it actually returns one less
            // the next power of 2
            fn next_power_2(x: Self) -> Self {
                Self::checked_next_power_of_two(x)
                    .map(|x| x - 1)
                    .unwrap_or(Self::MAX)
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
    fn from_column(ut: AnyUintType, total_events: Tot) -> Self {
        let t = total_events.0;
        match ut {
            AnyUintType::Uint08(layout) => AnyUintColumnReader::Uint8(UintColumnReader {
                layout,
                column: vec![0; t],
            }),
            AnyUintType::Uint16(layout) => AnyUintColumnReader::Uint16(UintColumnReader {
                layout,
                column: vec![0; t],
            }),
            AnyUintType::Uint24(layout) => AnyUintColumnReader::Uint24(UintColumnReader {
                layout,
                column: vec![0; t],
            }),
            AnyUintType::Uint32(layout) => AnyUintColumnReader::Uint32(UintColumnReader {
                layout,
                column: vec![0; t],
            }),
            AnyUintType::Uint40(layout) => AnyUintColumnReader::Uint40(UintColumnReader {
                layout,
                column: vec![0; t],
            }),
            AnyUintType::Uint48(layout) => AnyUintColumnReader::Uint48(UintColumnReader {
                layout,
                column: vec![0; t],
            }),
            AnyUintType::Uint56(layout) => AnyUintColumnReader::Uint56(UintColumnReader {
                layout,
                column: vec![0; t],
            }),
            AnyUintType::Uint64(layout) => AnyUintColumnReader::Uint64(UintColumnReader {
                layout,
                column: vec![0; t],
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
        .finish(&mut s.dataset.as_data_mut())
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
        let mut data: Vec<_> = iter::repeat_with(|| vec![0; nrows.0])
            .take(p.ncols)
            .collect();
        for b in h.bytes().take(p.nbytes) {
            let byte = b?;
            // exit if we encounter more rows than expected.
            if row == nrows.0 {
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
        if !(col == 0 && row == nrows.0) {
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
    let mut data: Vec<_> = iter::repeat_with(|| vec![0; parser.nrows.0])
        .take(ncols)
        .collect();
    let mut buf = String::new();
    for r in 0..parser.nrows.0 {
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
    for r in 0..p.nrows.0 {
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
    d: AnyCoreDataset,
    conf: &WriteConfig,
) -> ImpureResult<()> {
    let (text, df, analysis) = d.into_parts();
    let analysis_len = analysis.0.len();
    let df_ncols = df.width();

    // We can now confidently count the number of events (rows)
    let nrows = df.height();

    // Get the layout, or bail if we can't
    let layout = text.as_column_layout().map_err(|es| Failure {
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
        if let Some(text) = text.text_segment(Tot(nrows), data_len, analysis_len) {
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

    h.write_all(&analysis.0)?;
    res
}

fn h_read_analysis<R: Read + Seek>(h: &mut BufReader<R>, seg: &Segment) -> io::Result<Analysis> {
    let mut buf = vec![];
    h.seek(SeekFrom::Start(u64::from(seg.begin())))?;
    h.take(u64::from(seg.nbytes())).read_to_end(&mut buf)?;
    Ok(buf.into())
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
            let data = h_read_data_segment(h, data_parser)?;
            let analysis = h_read_analysis(h, &analysis_seg)?;
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
                // ASSUME we have checked that the dataframe has the same number
                // of columns as number of measurements, and that all
                // measurement names are unique. Therefore, this should not
                // fail.
                dataset: std.standardized.into_dataset_unchecked(data, analysis),
                deviant: std.deviant,
            }))
        })
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
            self.timestamps.btim.0.as_ref().map(|x| (x.0).0)
        }

        fn end_time(&self) -> Option<NaiveTime> {
            self.timestamps.etim.0.as_ref().map(|x| (x.0).0)
        }

        fn set_datetimes_inner(
            &mut self,
            begin: DateTime<FixedOffset>,
            end: DateTime<FixedOffset>,
        ) {
            let d1 = begin.date_naive();
            let d2 = end.date_naive();
            self.timestamps.btim = Some($fcstime(begin.time()).into()).into();
            self.timestamps.etim = Some($fcstime(end.time()).into()).into();
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

    fn byteord(&self) -> ByteOrd {
        self.byteord.clone()
    }

    fn as_unstainedcenters(&self) -> Option<&UnstainedCenters> {
        None
    }

    fn as_unstainedcenters_mut(&mut self) -> Option<&mut UnstainedCenters> {
        None
    }

    fn as_spillover(&self) -> Option<&Spillover> {
        None
    }

    fn as_spillover_mut(&mut self) -> Option<&mut Spillover> {
        None
    }

    fn as_compensation(&self) -> Option<&Compensation> {
        self.comp.as_ref_opt()
    }

    fn as_compensation_mut(&mut self) -> Option<&mut Compensation> {
        self.comp.0.as_mut()
    }

    fn timestamps_valid(&self) -> bool {
        self.timestamps.valid()
    }

    fn datetimes_valid(&self) -> bool {
        true
    }

    fn check_timestep(&self) -> bool {
        true
    }

    get_set_pre_3_2_datetime!(FCSTime);

    fn lookup_specific(st: &mut KwParser, par: Par) -> Option<InnerMetadata2_0> {
        let maybe_mode = st.lookup_meta_req();
        let maybe_byteord = st.lookup_meta_req();
        if let (Some(mode), Some(byteord)) = (maybe_mode, maybe_byteord) {
            Some(InnerMetadata2_0 {
                mode,
                byteord,
                cyt: st.lookup_meta_opt(false),
                comp: st.lookup_compensation_2_0(par),
                timestamps: st.lookup_timestamps2_0(),
            })
        } else {
            None
        }
    }

    fn lookup_tot(kws: &mut RawKeywords) -> PureMaybe<Tot> {
        Tot::lookup_meta_opt(kws).map_or_else(
            |e| {
                let mut r = PureMaybe::empty();
                r.push_warning(e);
                r
            },
            |s| PureSuccess::from(s.0),
        )
    }

    fn keywords_req_inner(&self) -> Vec<(String, String)> {
        [self.mode.pair(), self.byteord.pair()]
            .into_iter()
            .collect()
    }

    fn keywords_opt_inner(&self) -> Vec<(String, String)> {
        [
            OptMetaKey::pair(&self.cyt),
            OptMetaKey::pair(&self.comp),
            OptMetaKey::pair(&self.timestamps.btim),
            OptMetaKey::pair(&self.timestamps.etim),
            OptMetaKey::pair(&self.timestamps.date),
        ]
        .into_iter()
        .flat_map(|(k, v)| v.map(|x| (k, x)))
        .collect()
    }
}

impl VersionedMetadata for InnerMetadata3_0 {
    type P = InnerMeasurement3_0;

    fn byteord(&self) -> ByteOrd {
        self.byteord.clone()
    }

    fn as_unstainedcenters(&self) -> Option<&UnstainedCenters> {
        None
    }

    fn as_unstainedcenters_mut(&mut self) -> Option<&mut UnstainedCenters> {
        None
    }

    fn as_spillover(&self) -> Option<&Spillover> {
        None
    }

    fn as_spillover_mut(&mut self) -> Option<&mut Spillover> {
        None
    }

    fn as_compensation(&self) -> Option<&Compensation> {
        self.comp.as_ref_opt()
    }

    fn as_compensation_mut(&mut self) -> Option<&mut Compensation> {
        self.comp.0.as_mut()
    }

    fn lookup_tot(kws: &mut RawKeywords) -> PureMaybe<Tot> {
        PureMaybe::from_result_1(Tot::lookup_meta_req(kws), PureErrorLevel::Error)
    }

    fn timestamps_valid(&self) -> bool {
        self.timestamps.valid()
    }

    fn datetimes_valid(&self) -> bool {
        true
    }

    fn check_timestep(&self) -> bool {
        self.timestep.0.is_some()
    }

    get_set_pre_3_2_datetime!(FCSTime60);

    fn lookup_specific(st: &mut KwParser, _: Par) -> Option<InnerMetadata3_0> {
        let maybe_mode = st.lookup_meta_req();
        let maybe_byteord = st.lookup_meta_req();
        if let (Some(mode), Some(byteord)) = (maybe_mode, maybe_byteord) {
            Some(InnerMetadata3_0 {
                mode,
                byteord,
                cyt: st.lookup_meta_opt(false),
                comp: st.lookup_meta_opt(false),
                cytsn: st.lookup_meta_opt(false),
                timestep: st.lookup_meta_opt(false),
                unicode: st.lookup_meta_opt(false),
                timestamps: st.lookup_timestamps3_0(),
            })
        } else {
            None
        }
    }

    fn keywords_req_inner(&self) -> Vec<(String, String)> {
        [self.mode.pair(), self.byteord.pair()]
            .into_iter()
            .collect()
    }

    fn keywords_opt_inner(&self) -> Vec<(String, String)> {
        let ts = &self.timestamps;
        [
            OptMetaKey::pair(&self.cyt),
            OptMetaKey::pair(&self.comp),
            OptMetaKey::pair(&ts.btim),
            OptMetaKey::pair(&ts.etim),
            OptMetaKey::pair(&ts.date),
            OptMetaKey::pair(&self.cytsn),
            OptMetaKey::pair(&self.timestep),
            OptMetaKey::pair(&self.unicode),
        ]
        .into_iter()
        .flat_map(|(k, v)| v.map(|x| (k, x)))
        .collect()
    }
}

impl VersionedMetadata for InnerMetadata3_1 {
    type P = InnerMeasurement3_1;

    fn byteord(&self) -> ByteOrd {
        ByteOrd::Endian(self.byteord)
    }

    fn as_unstainedcenters(&self) -> Option<&UnstainedCenters> {
        None
    }

    fn as_unstainedcenters_mut(&mut self) -> Option<&mut UnstainedCenters> {
        None
    }

    fn as_spillover(&self) -> Option<&Spillover> {
        self.spillover.as_ref_opt()
    }

    fn as_spillover_mut(&mut self) -> Option<&mut Spillover> {
        self.spillover.0.as_mut()
    }

    fn as_compensation(&self) -> Option<&Compensation> {
        None
    }

    fn as_compensation_mut(&mut self) -> Option<&mut Compensation> {
        None
    }

    fn lookup_tot(kws: &mut RawKeywords) -> PureMaybe<Tot> {
        PureMaybe::from_result_1(Tot::lookup_meta_req(kws), PureErrorLevel::Error)
    }

    fn timestamps_valid(&self) -> bool {
        self.timestamps.valid()
    }

    fn datetimes_valid(&self) -> bool {
        true
    }

    fn check_timestep(&self) -> bool {
        self.timestep.0.is_some()
    }

    get_set_pre_3_2_datetime!(FCSTime100);

    fn lookup_specific(st: &mut KwParser, _: Par) -> Option<InnerMetadata3_1> {
        let maybe_mode = st.lookup_meta_req();
        let maybe_byteord = st.lookup_meta_req();
        if let (Some(mode), Some(byteord)) = (maybe_mode, maybe_byteord) {
            Some(InnerMetadata3_1 {
                mode,
                byteord,
                cyt: st.lookup_meta_opt(false),
                spillover: st.lookup_meta_opt(false),
                cytsn: st.lookup_meta_opt(false),
                timestep: st.lookup_meta_opt(false),
                vol: st.lookup_meta_opt(false),
                modification: st.lookup_modification(),
                timestamps: st.lookup_timestamps3_1(false),
                plate: st.lookup_plate(false),
            })
        } else {
            None
        }
    }

    fn keywords_req_inner(&self) -> Vec<(String, String)> {
        [self.mode.pair(), self.byteord.pair()]
            .into_iter()
            .collect()
    }

    fn keywords_opt_inner(&self) -> Vec<(String, String)> {
        let mdn = &self.modification;
        let ts = &self.timestamps;
        let pl = &self.plate;
        [
            OptMetaKey::pair(&self.cyt),
            OptMetaKey::pair(&self.spillover),
            OptMetaKey::pair(&ts.btim),
            OptMetaKey::pair(&ts.etim),
            OptMetaKey::pair(&ts.date),
            OptMetaKey::pair(&self.cytsn),
            OptMetaKey::pair(&self.timestep),
            OptMetaKey::pair(&mdn.last_modifier),
            OptMetaKey::pair(&mdn.last_modified),
            OptMetaKey::pair(&mdn.originality),
            OptMetaKey::pair(&pl.plateid),
            OptMetaKey::pair(&pl.platename),
            OptMetaKey::pair(&pl.wellid),
            OptMetaKey::pair(&self.vol),
        ]
        .into_iter()
        .flat_map(|(k, v)| v.map(|x| (k, x)))
        .collect()
    }
}

impl VersionedMetadata for InnerMetadata3_2 {
    type P = InnerMeasurement3_2;

    fn lookup_tot(kws: &mut RawKeywords) -> PureMaybe<Tot> {
        PureMaybe::from_result_1(Tot::lookup_meta_req(kws), PureErrorLevel::Error)
    }

    fn byteord(&self) -> ByteOrd {
        ByteOrd::Endian(self.byteord)
    }

    fn as_unstainedcenters(&self) -> Option<&UnstainedCenters> {
        self.unstained.unstainedcenters.as_ref_opt()
    }

    fn as_unstainedcenters_mut(&mut self) -> Option<&mut UnstainedCenters> {
        self.unstained.unstainedcenters.0.as_mut()
    }

    fn as_spillover(&self) -> Option<&Spillover> {
        self.spillover.as_ref_opt()
    }

    fn as_spillover_mut(&mut self) -> Option<&mut Spillover> {
        self.spillover.0.as_mut()
    }

    fn as_compensation(&self) -> Option<&Compensation> {
        None
    }

    fn as_compensation_mut(&mut self) -> Option<&mut Compensation> {
        None
    }

    fn timestamps_valid(&self) -> bool {
        self.timestamps.valid()
    }

    fn datetimes_valid(&self) -> bool {
        self.datetimes.valid()
    }

    fn check_timestep(&self) -> bool {
        self.timestep.0.is_some()
    }

    // TODO not DRY
    fn begin_date(&self) -> Option<NaiveDate> {
        self.datetimes
            .begin
            .as_ref_opt()
            .map(|x| (x.0).0.date_naive())
            .or(self.timestamps.date.as_ref_opt().map(|x| x.0))
    }

    fn end_date(&self) -> Option<NaiveDate> {
        self.datetimes
            .end
            .as_ref_opt()
            .map(|x| (x.0).0.date_naive())
            .or(self.timestamps.date.as_ref_opt().map(|x| x.0))
    }

    fn begin_time(&self) -> Option<NaiveTime> {
        self.datetimes
            .begin
            .as_ref_opt()
            .map(|x| (x.0).0.time())
            .or(self.timestamps.btim.as_ref_opt().map(|x| (x.0).0))
    }

    fn end_time(&self) -> Option<NaiveTime> {
        self.datetimes
            .end
            .as_ref_opt()
            .map(|x| (x.0).0.time())
            .or(self.timestamps.etim.as_ref_opt().map(|x| (x.0).0))
    }

    fn set_datetimes_inner(&mut self, begin: DateTime<FixedOffset>, end: DateTime<FixedOffset>) {
        self.datetimes.begin = Some(BeginDateTime(FCSDateTime(begin))).into();
        self.datetimes.end = Some(EndDateTime(FCSDateTime(end))).into();
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

    fn lookup_specific(st: &mut KwParser, _: Par) -> Option<InnerMetadata3_2> {
        // Only L is allowed as of 3.2, so pull the value and check it if given.
        // The only thing we care about is that the value is valid, since we
        // don't need to use it anywhere.
        let _: OptionalKw<Mode3_2> = st.lookup_meta_opt(true);
        let maybe_byteord = st.lookup_meta_req();
        let maybe_cyt = st.lookup_meta_req();
        if let (Some(byteord), Some(cyt)) = (maybe_byteord, maybe_cyt) {
            Some(InnerMetadata3_2 {
                byteord,
                cyt,
                spillover: st.lookup_meta_opt(false),
                cytsn: st.lookup_meta_opt(false),
                timestep: st.lookup_meta_opt(false),
                vol: st.lookup_meta_opt(false),
                flowrate: st.lookup_meta_opt(false),
                modification: st.lookup_modification(),
                plate: st.lookup_plate(true),
                timestamps: st.lookup_timestamps3_1(true),
                carrier: st.lookup_carrier(),
                datetimes: st.lookup_datetimes(),
                unstained: st.lookup_unstained(),
            })
        } else {
            None
        }
    }

    fn keywords_req_inner(&self) -> Vec<(String, String)> {
        [self.byteord.pair(), self.cyt.pair()].into_iter().collect()
    }

    fn keywords_opt_inner(&self) -> Vec<(String, String)> {
        let mdn = &self.modification;
        let ts = &self.timestamps;
        let pl = &self.plate;
        let car = &self.carrier;
        let dt = &self.datetimes;
        let us = &self.unstained;
        [
            OptMetaKey::pair(&self.spillover),
            OptMetaKey::pair(&ts.btim),
            OptMetaKey::pair(&ts.etim),
            OptMetaKey::pair(&ts.date),
            OptMetaKey::pair(&self.cytsn),
            OptMetaKey::pair(&self.timestep),
            OptMetaKey::pair(&mdn.last_modifier),
            OptMetaKey::pair(&mdn.last_modified),
            OptMetaKey::pair(&mdn.originality),
            OptMetaKey::pair(&pl.plateid),
            OptMetaKey::pair(&pl.platename),
            OptMetaKey::pair(&pl.wellid),
            OptMetaKey::pair(&self.vol),
            OptMetaKey::pair(&car.carrierid),
            OptMetaKey::pair(&car.carriertype),
            OptMetaKey::pair(&car.locationid),
            OptMetaKey::pair(&dt.begin),
            OptMetaKey::pair(&dt.end),
            OptMetaKey::pair(&us.unstainedcenters),
            OptMetaKey::pair(&us.unstainedinfo),
            OptMetaKey::pair(&self.flowrate),
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
        Version::FCS2_0 => CoreTEXT2_0::new_from_raw(kws, conf).map(|x| x.map(|y| y.into())),
        Version::FCS3_0 => CoreTEXT3_0::new_from_raw(kws, conf).map(|x| x.map(|y| y.into())),
        Version::FCS3_1 => CoreTEXT3_1::new_from_raw(kws, conf).map(|x| x.map(|y| y.into())),
        Version::FCS3_2 => CoreTEXT3_2::new_from_raw(kws, conf).map(|x| x.map(|y| y.into())),
    }
}

macro_rules! from_anycoretext {
    ($anyvar:ident, $coretype:ident) => {
        impl From<$coretype> for AnyCoreTEXT {
            fn from(value: $coretype) -> Self {
                Self::$anyvar(Box::new(value))
            }
        }
    };
}

from_anycoretext!(FCS2_0, CoreTEXT2_0);
from_anycoretext!(FCS3_0, CoreTEXT3_0);
from_anycoretext!(FCS3_1, CoreTEXT3_1);
from_anycoretext!(FCS3_2, CoreTEXT3_2);

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

    fn lookup_timestamps2_0(&mut self) -> Timestamps2_0 {
        Timestamps2_0 {
            btim: self.lookup_meta_opt(false),
            etim: self.lookup_meta_opt(false),
            date: self.lookup_meta_opt(false),
        }
    }

    fn lookup_timestamps3_0(&mut self) -> Timestamps3_0 {
        Timestamps3_0 {
            btim: self.lookup_meta_opt(false),
            etim: self.lookup_meta_opt(false),
            date: self.lookup_meta_opt(false),
        }
    }

    fn lookup_timestamps3_1(&mut self, dep: bool) -> Timestamps3_1 {
        Timestamps3_1 {
            btim: self.lookup_meta_opt(dep),
            etim: self.lookup_meta_opt(dep),
            date: self.lookup_meta_opt(dep),
        }
    }

    fn lookup_datetimes(&mut self) -> Datetimes {
        let begin = self.lookup_meta_opt(false);
        let end = self.lookup_meta_opt(false);
        Datetimes { begin, end }
    }

    fn lookup_modification(&mut self) -> ModificationData {
        ModificationData {
            last_modifier: self.lookup_meta_opt(false),
            last_modified: self.lookup_meta_opt(false),
            originality: self.lookup_meta_opt(false),
        }
    }

    fn lookup_plate(&mut self, dep: bool) -> PlateData {
        PlateData {
            wellid: self.lookup_meta_opt(dep),
            platename: self.lookup_meta_opt(dep),
            plateid: self.lookup_meta_opt(dep),
        }
    }

    fn lookup_carrier(&mut self) -> CarrierData {
        CarrierData {
            locationid: self.lookup_meta_opt(false),
            carrierid: self.lookup_meta_opt(false),
            carriertype: self.lookup_meta_opt(false),
        }
    }

    fn lookup_unstained(&mut self) -> UnstainedData {
        UnstainedData {
            unstainedcenters: self.lookup_meta_opt(false),
            unstainedinfo: self.lookup_meta_opt(false),
        }
    }

    fn lookup_compensation_2_0(&mut self, par: Par) -> OptionalKw<Compensation> {
        // column = src channel
        // row = target channel
        // These are "flipped" in 2.0, where "column" goes TO the "row"
        let n = par.0;
        let mut any_error = false;
        let mut matrix = DMatrix::<f32>::identity(n, n);
        for r in 0..n {
            for c in 0..n {
                let k = format!("DFC{c}TO{r}");
                if let Some(x) = self.lookup_dfc(k.as_str()) {
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

    fn lookup_dfc(&mut self, k: &str) -> Option<f32> {
        self.raw_keywords.remove(k).and_then(|v| {
            v.parse::<f32>()
                .inspect_err(|e| {
                    let msg = format!("{e} (key={k}, value='{v}'))");
                    self.deferred.push_warning(msg);
                })
                .ok()
        })
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

    fn lookup_all_meas_nonstandard(&mut self, n: MeasIdx) -> NonStdKeywords {
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
                Err(err) => self.deferred.push_warning(err.to_string()),
            }
        }
        ns
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

    fn lookup_meta_req<V>(&mut self) -> Option<V>
    where
        V: ReqMetaKey,
        V: FromStr,
        <V as FromStr>::Err: fmt::Display,
    {
        V::lookup_meta_req(self.raw_keywords)
            .map_err(|e| self.deferred.push_error(e))
            .ok()
    }

    fn lookup_meta_opt<V>(&mut self, dep: bool) -> OptionalKw<V>
    where
        V: OptMetaKey,
        V: FromStr,
        <V as FromStr>::Err: fmt::Display,
    {
        let res = V::lookup_meta_opt(self.raw_keywords);
        self.process_opt(res, V::std(), dep)
    }

    // fn lookup_linked_meta_opt<V>(&mut self, dep: bool, names: &[&Shortname]) -> OptionalKw<V>
    // where
    //     V: LinkedOptMetaKey,
    // {
    //     let res = V::lookup_linked_meta_opt(self.raw_keywords, names);
    //     self.process_opt(res, V::std(), dep)
    // }

    fn lookup_meas_req<V>(&mut self, n: MeasIdx) -> Option<V>
    where
        V: ReqMeasKey,
        V: FromStr,
        <V as FromStr>::Err: fmt::Display,
    {
        V::lookup_meas_req(self.raw_keywords, n)
            .map_err(|e| self.deferred.push_error(e))
            .ok()
    }

    fn lookup_meas_opt<V>(&mut self, n: MeasIdx, dep: bool) -> OptionalKw<V>
    where
        V: OptMeasKey,
        V: FromStr,
        <V as FromStr>::Err: fmt::Display,
    {
        let res = V::lookup_meas_opt(self.raw_keywords, n);
        self.process_opt(res, V::std(n), dep)
    }

    fn process_opt<V>(
        &mut self,
        res: Result<OptionalKw<V>, String>,
        k: String,
        dep: bool,
    ) -> OptionalKw<V> {
        res.inspect(|_| {
            if dep {
                let msg = format!("deprecated key: {k}");
                self.deferred
                    .push_msg_leveled(msg, self.conf.disallow_deprecated);
            }
        })
        .map_err(|e| self.deferred.push_warning(e))
        .unwrap_or(None.into())
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
        // TODO generalized this and possibly put in a trait
        if k == FCSDate::std() {
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

// TODO use non-empty here (and everywhere else we return multiple errors)
fn lookup_req_segment(
    kws: &mut RawKeywords,
    bk: &str,
    ek: &str,
    corr: OffsetCorrection,
    id: SegmentId,
) -> Result<Segment, Vec<String>> {
    let b = lookup_req(kws, bk);
    let e = lookup_req(kws, ek);
    match (b, e) {
        (Ok(begin), Ok(end)) => Segment::try_new(begin, end, corr, id).map_err(|x| vec![x]),
        (a, b) => Err([a.err(), b.err()].into_iter().flatten().collect()),
    }
}

fn lookup_opt_segment(
    kws: &mut RawKeywords,
    bk: &str,
    ek: &str,
    corr: OffsetCorrection,
    id: SegmentId,
) -> Result<Option<Segment>, Vec<String>> {
    let b = lookup_opt(kws, bk);
    let e = lookup_opt(kws, ek);
    match (b, e) {
        (Ok(mb), Ok(me)) => {
            if let (Some(begin), Some(end)) = (mb, me) {
                Segment::try_new(begin, end, corr, id)
                    .map_err(|x| vec![x])
                    .map(Some)
            } else {
                Ok(None)
            }
        }
        (a, b) => Err([a.err(), b.err()].into_iter().flatten().collect()),
    }
}

// TODO unclear if these next two functions should throw errors or warnings
// on failure
fn lookup_data_offsets(
    kws: &mut RawKeywords,
    conf: &DataReadConfig,
    version: Version,
    default: &Segment,
) -> PureSuccess<Segment> {
    match version {
        Version::FCS2_0 => Ok(*default),
        _ => lookup_req_segment(kws, BEGINDATA, ENDDATA, conf.data, SegmentId::Data),
    }
    .map_or_else(
        |es| {
            // TODO toggle this?
            let mut def = PureErrorBuf::from_many(es, PureErrorLevel::Warning);
            let msg =
                "could not use DATA offsets in TEXT, defaulting to HEADER offsets".to_string();
            def.push_warning(msg);
            PureSuccess {
                data: *default,
                deferred: def,
            }
        },
        PureSuccess::from,
    )
}

fn lookup_analysis_offsets(
    kws: &mut RawKeywords,
    conf: &DataReadConfig,
    version: Version,
    default: &Segment,
) -> PureSuccess<Segment> {
    let default_succ = |msgs| {
        // TODO toggle this?
        let mut def = PureErrorBuf::from_many(msgs, PureErrorLevel::Warning);
        let msg =
            "could not use ANALYSIS offsets in TEXT, defaulting to HEADER offsets".to_string();
        def.push_warning(msg);
        PureSuccess {
            data: *default,
            deferred: def,
        }
    };
    match version {
        Version::FCS2_0 => Ok(Some(*default)),
        Version::FCS3_0 | Version::FCS3_1 => lookup_req_segment(
            kws,
            BEGINANALYSIS,
            ENDANALYSIS,
            conf.analysis,
            SegmentId::Analysis,
        )
        .map(Some),
        Version::FCS3_2 => lookup_opt_segment(
            kws,
            BEGINANALYSIS,
            ENDANALYSIS,
            conf.analysis,
            SegmentId::Analysis,
        ),
    }
    .map_or_else(default_succ, |mab_seg| {
        mab_seg.map_or_else(|| default_succ(vec![]), PureSuccess::from)
    })
}

fn lookup_stext_offsets(
    kws: &mut RawKeywords,
    version: Version,
    conf: &RawTextReadConfig,
) -> PureMaybe<Segment> {
    // TODO add another msg explaining that the supp text won't be read if
    // offsets not found
    match version {
        Version::FCS2_0 => PureSuccess::from(None),
        Version::FCS3_0 | Version::FCS3_1 => {
            let res = lookup_req_segment(
                kws,
                BEGINSTEXT,
                ENDSTEXT,
                conf.stext,
                SegmentId::SupplementalText,
            );
            let level = if conf.enforce_stext {
                PureErrorLevel::Error
            } else {
                PureErrorLevel::Warning
            };
            PureMaybe::from_result_strs(res, level)
        }
        Version::FCS3_2 => lookup_opt_segment(
            kws,
            BEGINSTEXT,
            ENDSTEXT,
            conf.stext,
            SegmentId::SupplementalText,
        )
        .map_or_else(
            |es| PureSuccess {
                data: None,
                deferred: PureErrorBuf::from_many(es, PureErrorLevel::Warning),
            },
            PureSuccess::from,
        ),
    }
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

fn lookup_nextdata(kws: &mut RawKeywords, enforce: bool) -> PureMaybe<u32> {
    if enforce {
        PureMaybe::from_result_1(lookup_req(kws, NEXTDATA), PureErrorLevel::Error)
    } else {
        PureMaybe::from_result_1(lookup_opt(kws, NEXTDATA), PureErrorLevel::Warning)
            .map(|x| x.flatten())
    }
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
            let enforce_nextdata = true;
            lookup_nextdata(&mut kws, enforce_nextdata).map(|nextdata| RawTEXT {
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
            if k == Tot::std()
                || k == BEGINDATA
                || k == ENDDATA
                || k == BEGINANALYSIS
                || k == ENDANALYSIS
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

pub struct FromByteOrdError;

impl TryFrom<ByteOrd> for Endian {
    type Error = FromByteOrdError;

    fn try_from(value: ByteOrd) -> Result<Self, Self::Error> {
        match value {
            ByteOrd::Endian(e) => Ok(e),
            _ => Err(FromByteOrdError),
        }
    }
}

impl fmt::Display for FromByteOrdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "could not convert ByteOrd, must be either '1,2,3,4' or '4,3,2,1'",
        )
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

pub trait IntoMeasurement<P>
where
    Self: VersionedMeasurement,
    P: VersionedMeasurement,
{
    fn try_convert(m: Measurement<Self>, n: MeasIdx) -> MsgsResult<Measurement<P>> {
        Self::try_convert_inner(m.specific, n).map(|specific| Measurement {
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
        })
    }

    fn try_convert_inner(self, n: MeasIdx) -> MsgsResult<P>;
}

pub trait IntoMetadata<M>
where
    Self: VersionedMetadata,
    M: VersionedMetadata,
{
    fn try_convert(m: Metadata<Self>) -> Result<Metadata<M>, Vec<String>> {
        // TODO this seems silly, break struct up into common bits
        Self::try_convert_inner(m.specific).map(|specific| Metadata {
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
        })
    }

    fn try_convert_inner(self) -> Result<M, Vec<String>>;
}

impl IntoMeasurement<InnerMeasurement2_0> for InnerMeasurement2_0 {
    fn try_convert_inner(self, _: MeasIdx) -> MsgsResult<InnerMeasurement2_0> {
        Ok(self)
    }
}

impl IntoMeasurement<InnerMeasurement2_0> for InnerMeasurement3_0 {
    fn try_convert_inner(self, _: MeasIdx) -> MsgsResult<InnerMeasurement2_0> {
        Ok(InnerMeasurement2_0 {
            scale: Some(self.scale).into(),
            wavelength: self.wavelength,
        })
    }
}

impl IntoMeasurement<InnerMeasurement2_0> for InnerMeasurement3_1 {
    fn try_convert_inner(self, _: MeasIdx) -> MsgsResult<InnerMeasurement2_0> {
        Ok(InnerMeasurement2_0 {
            scale: Some(self.scale).into(),
            wavelength: self.wavelengths.into(),
        })
    }
}

impl IntoMeasurement<InnerMeasurement2_0> for InnerMeasurement3_2 {
    fn try_convert_inner(self, _: MeasIdx) -> MsgsResult<InnerMeasurement2_0> {
        Ok(InnerMeasurement2_0 {
            scale: Some(self.scale).into(),
            wavelength: self.wavelengths.into(),
        })
    }
}

impl IntoMeasurement<InnerMeasurement3_0> for InnerMeasurement2_0 {
    fn try_convert_inner(self, n: MeasIdx) -> MsgsResult<InnerMeasurement3_0> {
        self.scale
            .try_meas_from(n)
            .map_err(|e| vec![e])
            .map(|scale| InnerMeasurement3_0 {
                scale,
                wavelength: self.wavelength,
                gain: None.into(),
            })
    }
}

impl IntoMeasurement<InnerMeasurement3_0> for InnerMeasurement3_0 {
    fn try_convert_inner(self, _: MeasIdx) -> MsgsResult<InnerMeasurement3_0> {
        Ok(self)
    }
}

impl IntoMeasurement<InnerMeasurement3_0> for InnerMeasurement3_1 {
    fn try_convert_inner(self, _: MeasIdx) -> MsgsResult<InnerMeasurement3_0> {
        Ok(InnerMeasurement3_0 {
            scale: self.scale,
            gain: self.gain,
            wavelength: self.wavelengths.into(),
        })
    }
}

impl IntoMeasurement<InnerMeasurement3_0> for InnerMeasurement3_2 {
    fn try_convert_inner(self, _: MeasIdx) -> MsgsResult<InnerMeasurement3_0> {
        Ok(InnerMeasurement3_0 {
            scale: self.scale,
            gain: self.gain,
            wavelength: self.wavelengths.into(),
        })
    }
}

impl IntoMeasurement<InnerMeasurement3_1> for InnerMeasurement2_0 {
    fn try_convert_inner(self, n: MeasIdx) -> MsgsResult<InnerMeasurement3_1> {
        self.scale
            .try_meas_from(n)
            .map_err(|e| vec![e])
            .map(|scale| InnerMeasurement3_1 {
                scale,
                wavelengths: self.wavelength.map(|x| x.into()),
                gain: None.into(),
                calibration: None.into(),
                display: None.into(),
            })
    }
}

impl IntoMeasurement<InnerMeasurement3_1> for InnerMeasurement3_0 {
    fn try_convert_inner(self, _: MeasIdx) -> MsgsResult<InnerMeasurement3_1> {
        Ok(InnerMeasurement3_1 {
            scale: self.scale,
            gain: self.gain,
            wavelengths: None.into(),
            calibration: None.into(),
            display: None.into(),
        })
    }
}

impl IntoMeasurement<InnerMeasurement3_1> for InnerMeasurement3_1 {
    fn try_convert_inner(self, _: MeasIdx) -> MsgsResult<InnerMeasurement3_1> {
        Ok(self)
    }
}

impl IntoMeasurement<InnerMeasurement3_1> for InnerMeasurement3_2 {
    fn try_convert_inner(self, _: MeasIdx) -> MsgsResult<InnerMeasurement3_1> {
        Ok(InnerMeasurement3_1 {
            scale: self.scale,
            gain: self.gain,
            wavelengths: self.wavelengths,
            calibration: self.calibration.map(|x| x.into()),
            display: self.display,
        })
    }
}

impl IntoMeasurement<InnerMeasurement3_2> for InnerMeasurement2_0 {
    fn try_convert_inner(self, n: MeasIdx) -> MsgsResult<InnerMeasurement3_2> {
        self.scale
            .try_meas_from(n)
            .map_err(|e| vec![e])
            .map(|scale| InnerMeasurement3_2 {
                scale,
                wavelengths: None.into(),
                gain: None.into(),
                calibration: None.into(),
                display: None.into(),
                analyte: None.into(),
                feature: None.into(),
                tag: None.into(),
                detector_name: None.into(),
                datatype: None.into(),
                measurement_type: None.into(),
            })
    }
}

impl IntoMeasurement<InnerMeasurement3_2> for InnerMeasurement3_0 {
    fn try_convert_inner(self, _: MeasIdx) -> MsgsResult<InnerMeasurement3_2> {
        Ok(InnerMeasurement3_2 {
            scale: self.scale,
            wavelengths: self.wavelength.map(|x| x.into()),
            gain: self.gain,
            calibration: None.into(),
            display: None.into(),
            analyte: None.into(),
            feature: None.into(),
            tag: None.into(),
            detector_name: None.into(),
            datatype: None.into(),
            measurement_type: None.into(),
        })
    }
}

impl IntoMeasurement<InnerMeasurement3_2> for InnerMeasurement3_1 {
    fn try_convert_inner(self, _: MeasIdx) -> MsgsResult<InnerMeasurement3_2> {
        Ok(InnerMeasurement3_2 {
            scale: self.scale,
            wavelengths: self.wavelengths,
            gain: self.gain,
            calibration: self.calibration.map(|x| x.into()),
            display: self.display,
            analyte: None.into(),
            feature: None.into(),
            tag: None.into(),
            detector_name: None.into(),
            datatype: None.into(),
            measurement_type: None.into(),
        })
    }
}

impl IntoMeasurement<InnerMeasurement3_2> for InnerMeasurement3_2 {
    fn try_convert_inner(self, _: MeasIdx) -> MsgsResult<InnerMeasurement3_2> {
        Ok(self)
    }
}

type MsgsResult<T> = Result<T, Vec<String>>;

impl IntoMetadata<InnerMetadata2_0> for InnerMetadata2_0 {
    fn try_convert_inner(self) -> MsgsResult<InnerMetadata2_0> {
        Ok(self)
    }
}

impl IntoMetadata<InnerMetadata2_0> for InnerMetadata3_0 {
    fn try_convert_inner(self) -> MsgsResult<InnerMetadata2_0> {
        Ok(InnerMetadata2_0 {
            mode: self.mode,
            byteord: self.byteord,
            cyt: self.cyt,
            comp: self.comp,
            timestamps: self.timestamps.map(|d| d.into()),
        })
    }
}

impl IntoMetadata<InnerMetadata2_0> for InnerMetadata3_1 {
    fn try_convert_inner(self) -> MsgsResult<InnerMetadata2_0> {
        Ok(InnerMetadata2_0 {
            mode: self.mode,
            byteord: self.byteord.into(),
            cyt: self.cyt,
            comp: None.into(),
            timestamps: self.timestamps.map(|d| d.into()),
        })
    }
}

impl IntoMetadata<InnerMetadata2_0> for InnerMetadata3_2 {
    fn try_convert_inner(self) -> MsgsResult<InnerMetadata2_0> {
        Ok(InnerMetadata2_0 {
            mode: Mode::List,
            byteord: self.byteord.into(),
            cyt: Some(self.cyt).into(),
            comp: None.into(),
            timestamps: self.timestamps.map(|d| d.into()),
        })
    }
}

impl IntoMetadata<InnerMetadata3_0> for InnerMetadata2_0 {
    fn try_convert_inner(self) -> MsgsResult<InnerMetadata3_0> {
        Ok(InnerMetadata3_0 {
            mode: self.mode,
            byteord: self.byteord,
            cyt: self.cyt,
            comp: self.comp,
            timestamps: self.timestamps.map(|d| d.into()),
            cytsn: None.into(),
            timestep: None.into(),
            unicode: None.into(),
        })
    }
}

impl IntoMetadata<InnerMetadata3_0> for InnerMetadata3_0 {
    fn try_convert_inner(self) -> MsgsResult<InnerMetadata3_0> {
        Ok(self)
    }
}

impl IntoMetadata<InnerMetadata3_0> for InnerMetadata3_1 {
    fn try_convert_inner(self) -> MsgsResult<InnerMetadata3_0> {
        Ok(InnerMetadata3_0 {
            mode: self.mode,
            byteord: self.byteord.into(),
            cyt: self.cyt,
            cytsn: self.cytsn,
            timestep: self.timestep,
            timestamps: self.timestamps.map(|d| d.into()),
            comp: None.into(),
            unicode: None.into(),
        })
    }
}

impl IntoMetadata<InnerMetadata3_0> for InnerMetadata3_2 {
    fn try_convert_inner(self) -> MsgsResult<InnerMetadata3_0> {
        Ok(InnerMetadata3_0 {
            mode: Mode::List,
            byteord: self.byteord.into(),
            cyt: Some(self.cyt).into(),
            cytsn: self.cytsn,
            timestep: self.timestep,
            timestamps: self.timestamps.map(|d| d.into()),
            comp: None.into(),
            unicode: None.into(),
        })
    }
}

impl IntoMetadata<InnerMetadata3_1> for InnerMetadata2_0 {
    fn try_convert_inner(self) -> MsgsResult<InnerMetadata3_1> {
        self.byteord
            .try_into()
            .map_err(|e: FromByteOrdError| vec![e.to_string()])
            .map(|byteord| InnerMetadata3_1 {
                mode: self.mode,
                byteord,
                cyt: self.cyt,
                timestamps: self.timestamps.map(|d| d.into()),
                cytsn: None.into(),
                timestep: None.into(),
                spillover: None.into(),
                modification: ModificationData::default(),
                plate: PlateData::default(),
                vol: None.into(),
            })
    }
}

impl IntoMetadata<InnerMetadata3_1> for InnerMetadata3_0 {
    fn try_convert_inner(self) -> MsgsResult<InnerMetadata3_1> {
        self.byteord
            .try_into()
            .map_err(|e: FromByteOrdError| vec![e.to_string()])
            .map(|byteord| InnerMetadata3_1 {
                byteord,
                mode: self.mode,
                cyt: self.cyt,
                timestep: self.timestep,
                cytsn: self.cytsn,
                timestamps: self.timestamps.map(|d| d.into()),
                spillover: None.into(),
                modification: ModificationData::default(),
                plate: PlateData::default(),
                vol: None.into(),
            })
    }
}

impl IntoMetadata<InnerMetadata3_1> for InnerMetadata3_1 {
    fn try_convert_inner(self) -> MsgsResult<InnerMetadata3_1> {
        Ok(self)
    }
}

impl IntoMetadata<InnerMetadata3_1> for InnerMetadata3_2 {
    fn try_convert_inner(self) -> MsgsResult<InnerMetadata3_1> {
        Ok(InnerMetadata3_1 {
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

impl IntoMetadata<InnerMetadata3_2> for InnerMetadata2_0 {
    fn try_convert_inner(self) -> MsgsResult<InnerMetadata3_2> {
        // TODO what happens if $MODE is not list?
        let b = self
            .byteord
            .try_into()
            .map_err(|e: FromByteOrdError| e.to_string());
        let c = self.cyt.try_meta_from();
        match (b, c) {
            (Ok(byteord), Ok(cyt)) => Ok(InnerMetadata3_2 {
                byteord,
                cyt,
                timestamps: self.timestamps.map(|d| d.into()),
                timestep: None.into(),
                cytsn: None.into(),
                modification: ModificationData::default(),
                spillover: None.into(),
                plate: PlateData::default(),
                vol: None.into(),
                flowrate: None.into(),
                carrier: CarrierData::default(),
                unstained: UnstainedData::default(),
                datetimes: Datetimes::default(),
            }),
            (a, b) => Err([a.err(), b.err()].into_iter().flatten().collect()),
        }
    }
}

impl IntoMetadata<InnerMetadata3_2> for InnerMetadata3_0 {
    fn try_convert_inner(self) -> MsgsResult<InnerMetadata3_2> {
        let b = self
            .byteord
            .try_into()
            .map_err(|e: FromByteOrdError| e.to_string());
        let c = self.cyt.try_meta_from();
        match (b, c) {
            (Ok(byteord), Ok(cyt)) => Ok({
                InnerMetadata3_2 {
                    byteord,
                    cyt,
                    timestep: self.timestep,
                    cytsn: self.cytsn,
                    timestamps: self.timestamps.map(|d| d.into()),
                    modification: ModificationData::default(),
                    spillover: None.into(),
                    plate: PlateData::default(),
                    vol: None.into(),
                    flowrate: None.into(),
                    carrier: CarrierData::default(),
                    unstained: UnstainedData::default(),
                    datetimes: Datetimes::default(),
                }
            }),
            (a, b) => Err([a.err(), b.err()].into_iter().flatten().collect()),
        }
    }
}

impl IntoMetadata<InnerMetadata3_2> for InnerMetadata3_1 {
    fn try_convert_inner(self) -> MsgsResult<InnerMetadata3_2> {
        self.cyt
            .try_meta_from()
            .map_err(|e| vec![e])
            .map(|cyt| InnerMetadata3_2 {
                byteord: self.byteord,
                cyt,
                cytsn: self.cytsn,
                timestep: self.timestep,
                timestamps: self.timestamps,
                spillover: self.spillover,
                modification: self.modification,
                plate: self.plate,
                vol: self.vol,
                flowrate: None.into(),
                carrier: CarrierData::default(),
                unstained: UnstainedData::default(),
                datetimes: Datetimes::default(),
            })
    }
}

impl IntoMetadata<InnerMetadata3_2> for InnerMetadata3_2 {
    fn try_convert_inner(self) -> MsgsResult<InnerMetadata3_2> {
        Ok(self)
    }
}

impl<V: Key> OptionalKw<V> {
    pub fn try_meta_from(self) -> Result<V, String> {
        self.0
            .ok_or(format!("value for key {} does not exist", V::std()))
    }
}

impl<V: IndexedKey> OptionalKw<V> {
    pub fn try_meas_from(self, n: MeasIdx) -> Result<V, String> {
        self.0
            .ok_or(format!("value for key {} does not exist", V::std(n)))
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

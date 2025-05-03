use crate::config::*;
pub use crate::core::*;
use crate::error::*;
pub use crate::header::*;
use crate::keywords::*;
use crate::macros::newtype_from;
use crate::optionalkw::*;
pub use crate::segment::*;
use crate::validated::byteord::*;
use crate::validated::datetimes::*;
use crate::validated::distinct::*;
use crate::validated::nonstandard::*;
use crate::validated::scale::*;
use crate::validated::shortname::*;
use crate::validated::spillover::*;
use crate::validated::timestamps::*;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use itertools::Itertools;
use polars::prelude::*;
use regex::Regex;
use serde::ser::SerializeStruct;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
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
pub struct CoreDataset<M, T, P, N, W> {
    /// Standardized TEXT segment in version specific format
    pub text: Box<CoreTEXT<M, T, P, N, W>>,

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

pub type CoreDataset2_0 = CoreDataset<
    InnerMetadata2_0,
    InnerTime2_0,
    InnerMeasurement2_0,
    OptionalKwFamily,
    OptionalKw<Shortname>,
>;
pub type CoreDataset3_0 = CoreDataset<
    InnerMetadata3_0,
    InnerTime3_0,
    InnerMeasurement3_0,
    OptionalKwFamily,
    OptionalKw<Shortname>,
>;
pub type CoreDataset3_1 = CoreDataset<
    InnerMetadata3_1,
    InnerTime3_1,
    InnerMeasurement3_1,
    IdentityFamily,
    Identity<Shortname>,
>;
pub type CoreDataset3_2 = CoreDataset<
    InnerMetadata3_2,
    InnerTime3_2,
    InnerMeasurement3_2,
    IdentityFamily,
    Identity<Shortname>,
>;

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

pub type TimeChannel2_0 = TimeChannel<InnerTime2_0>;
pub type TimeChannel3_0 = TimeChannel<InnerTime3_0>;
pub type TimeChannel3_1 = TimeChannel<InnerTime3_1>;
pub type TimeChannel3_2 = TimeChannel<InnerTime3_2>;

/// Version-specific data for one measurement
pub type Measurement2_0 = Measurement<InnerMeasurement2_0>;
pub type Measurement3_0 = Measurement<InnerMeasurement3_0>;
pub type Measurement3_1 = Measurement<InnerMeasurement3_1>;
pub type Measurement3_2 = Measurement<InnerMeasurement3_2>;

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

fn to_col_type(
    b: Bytes,
    dt: AlphaNumType,
    byteord: &ByteOrd,
    rng: &Range,
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

    fn int_from_str(s: &str) -> Result<Self, IntErrorKind> {
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
    fn range_to_bitmask(range: &Range) -> Result<Self::Native, String> {
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
        range: &Range,
        byteord: &ByteOrd,
    ) -> Result<UintType<Self::Native, INTLEN>, Vec<String>> {
        // TODO be more specific, which means we need the measurement index
        let b = Self::range_to_bitmask(range);
        let s = byteord.as_sized();
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

    fn to_float_type(b: &ByteOrd, r: &Range) -> Result<FloatType<LEN, Self::Native>, Vec<String>> {
        match (b.as_sized(), r.0.parse::<Self::Native>()) {
            (Ok(order), Ok(range)) => Ok(FloatType { order, range }),
            (a, b) => Err([a.err(), b.err().map(|s| s.to_string())]
                .into_iter()
                .flatten()
                .collect()),
        }
    }

    fn make_matrix_parser(
        byteord: &ByteOrd,
        par: usize,
        total_events: usize,
    ) -> PureMaybe<FloatReader<LEN>> {
        let res = byteord.as_sized().map(|byteord| FloatReader {
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

impl TryFrom<AlphaNumType> for NumType {
    type Error = ();
    fn try_from(value: AlphaNumType) -> Result<Self, Self::Error> {
        match value {
            AlphaNumType::Integer => Ok(NumType::Integer),
            AlphaNumType::Single => Ok(NumType::Single),
            AlphaNumType::Double => Ok(NumType::Double),
            AlphaNumType::Ascii => Err(()),
        }
    }
}

impl Spillover {
    fn table(&self, delim: &str) -> Vec<String> {
        let header0 = vec!["[-]"];
        let header = header0
            .into_iter()
            .chain(self.measurements().iter().map(|m| m.as_ref()))
            .join(delim);
        let lines = vec![header];
        let rows = self.matrix().row_iter().map(|xs| xs.iter().join(delim));
        lines.into_iter().chain(rows).collect()
    }

    fn print_table(&self, delim: &str) {
        for e in self.table(delim) {
            println!("{}", e);
        }
    }
}

impl<const LEN: usize> From<Endian> for SizedByteOrd<LEN> {
    fn from(value: Endian) -> Self {
        SizedByteOrd::Endian(value)
    }
}

fn make_uint_type(b: u8, r: &Range, o: &ByteOrd) -> Result<AnyUintType, Vec<String>> {
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

    // any_core_get_set_copied!(abrt, set_abrt, Abrt);
    // any_core_get_set_copied!(lost, set_lost, Lost);

    // any_core_get_set_str!(cells, set_cells);
    // any_core_get_set_str!(com, set_com);
    // any_core_get_set_str!(exp, set_exp);
    // any_core_get_set_str!(fil, set_fil);
    // any_core_get_set_str!(inst, set_inst);
    // any_core_get_set_str!(op, set_op);
    // any_core_get_set_str!(proj, set_proj);
    // any_core_get_set_str!(smno, set_smno);
    // any_core_get_set_str!(src, set_src);
    // any_core_get_set_str!(sys, set_sys);

    // pub fn trigger(&self) -> Option<&Trigger> {
    //     match_anycoretext!(self, x, { x.trigger() })
    // }

    // pub fn set_trigger(&mut self, t: Option<Trigger>) {
    //     match_anycoretext!(self, x, { x.set_trigger(t) })
    // }

    // pub fn raw_keywords(&self, want_req: Option<bool>, want_meta: Option<bool>) -> RawKeywords {
    //     match_anycoretext!(self, x, { x.raw_keywords(want_req, want_meta) })
    // }

    // pub fn shortnames(&self) -> Vec<Shortname> {
    //     match_anycoretext!(self, x, { x.all_shortnames() })
    // }

    // pub fn into_2_0(self) -> PureResult<CoreTEXT2_0> {
    //     match_anycoretext!(self, x, { x.try_convert() })
    // }

    // pub fn into_3_0(self) -> PureResult<CoreTEXT3_0> {
    //     match_anycoretext!(self, x, { x.try_convert() })
    // }

    // pub fn into_3_1(self) -> PureResult<CoreTEXT3_1> {
    //     match_anycoretext!(self, x, { x.try_convert() })
    // }

    // pub fn into_3_2(self) -> PureResult<CoreTEXT3_2> {
    //     match_anycoretext!(self, x, { x.try_convert() })
    // }

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
            AnyCoreTEXT::FCS2_0(x) => x.metadata.specific.as_spillover(),
            AnyCoreTEXT::FCS3_0(x) => x.metadata.specific.as_spillover(),
            AnyCoreTEXT::FCS3_1(x) => x.metadata.specific.as_spillover(),
            AnyCoreTEXT::FCS3_2(x) => x.metadata.specific.as_spillover(),
        }
        .as_ref()
        .map(|s| s.print_table(delim));
        if res.is_none() {
            println!("None")
        }
    }

    // fn set_shortnames(&mut self, names: Vec<Shortname>) -> Result<NameMapping, String> {
    //     match_anycoretext!(self, x, { x.set_shortnames(names) })
    // }

    // TODO this is an instance where I may want to return different errors
    // that can be caught in python or whatever
    // fn remove_measurement(&mut self, n: &Shortname) -> Result<Option<MeasIdx>, String> {
    //     match_anycoretext!(self, x, {
    //         x.remove_measurement_by_name(n).map(|x| x.map(|y| y.0))
    //     })
    // }

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

impl InnerTime2_0 {
    fn new() -> Self {
        Self
    }
}

impl TimeChannel2_0 {
    pub fn new(bytes: Bytes, range: Range) -> Self {
        let specific = InnerTime2_0::new();
        TimeChannel::new_common(bytes, range, specific)
    }
}

impl TimeChannel3_0 {
    pub fn new(bytes: Bytes, range: Range, timestep: Timestep) -> Self {
        let specific = InnerTime3_0::new(timestep);
        TimeChannel::new_common(bytes, range, specific)
    }
}

impl TimeChannel3_1 {
    pub fn new(bytes: Bytes, range: Range, timestep: Timestep) -> Self {
        let specific = InnerTime3_1::new(timestep);
        TimeChannel::new_common(bytes, range, specific)
    }
}

impl TimeChannel3_2 {
    pub fn new(bytes: Bytes, range: Range, timestep: Timestep) -> Self {
        let specific = InnerTime3_2::new(timestep);
        TimeChannel::new_common(bytes, range, specific)
    }
}

impl Measurement2_0 {
    pub fn new(bytes: Bytes, range: Range) -> Self {
        let specific = InnerMeasurement2_0::new();
        Measurement::new_common(bytes, range, specific)
    }
}

impl Measurement3_0 {
    pub fn new(bytes: Bytes, range: Range, scale: Scale) -> Self {
        let specific = InnerMeasurement3_0::new(scale);
        Measurement::new_common(bytes, range, specific)
    }
}

impl Measurement3_1 {
    pub fn new(bytes: Bytes, range: Range, scale: Scale) -> Self {
        let specific = InnerMeasurement3_1::new(scale);
        Measurement::new_common(bytes, range, specific)
    }
}

impl Measurement3_2 {
    pub fn new(bytes: Bytes, range: Range, scale: Scale) -> Self {
        let specific = InnerMeasurement3_2::new(scale);
        Measurement::new_common(bytes, range, specific)
    }
}

// impl CoreDataset3_1 {
//     spillover_methods!(text);
// }

// impl CoreDataset3_2 {
//     spillover_methods!(text);
// }

impl From<MixedColumnSetter> for AlphaNumType {
    fn from(value: MixedColumnSetter) -> Self {
        match value {
            MixedColumnSetter::Float(_) => AlphaNumType::Single,
            MixedColumnSetter::Double(_) => AlphaNumType::Double,
            MixedColumnSetter::Ascii(_) => AlphaNumType::Ascii,
            MixedColumnSetter::Uint(_) => AlphaNumType::Integer,
        }
    }
}

impl<M> CoreDataset<M, M::T, M::P, M::N, <M::N as MightHave>::Wrapper<Shortname>>
where
    M: VersionedMetadata,
    M::N: Clone,
{
    pub fn try_convert<ToM>(
        self,
    ) -> PureResult<
        CoreDataset<ToM, ToM::T, ToM::P, ToM::N, <ToM::N as MightHave>::Wrapper<Shortname>>,
    >
    where
        M::N: Clone,
        ToM: VersionedMetadata,
        ToM::P: VersionedMeasurement,
        ToM::T: VersionedTime,
        ToM::N: MightHave,
        ToM::N: Clone,
        ToM: TryFrom<M, Error = MetaConvertErrors>,
        ToM::P: TryFrom<M::P, Error = MeasConvertError>,
        ToM::T: From<M::T>,
        <ToM::N as MightHave>::Wrapper<Shortname>: TryFrom<<M::N as MightHave>::Wrapper<Shortname>>,
    {
        self.text.try_convert().map(|res| {
            res.map(|newtext| CoreDataset {
                text: Box::new(newtext),
                data: self.data,
                analysis: self.analysis,
            })
        })
    }

    // fn set_shortnames(&mut self, names: Vec<Shortname>) -> Result<NameMapping, String> {
    //     self.text
    //         .set_shortnames(names)
    //         .inspect(|_| self.text.set_df_column_names(&mut self.data).unwrap())
    // }

    // TODO also make a version of this that takes an index since not all
    // columns are named or we might not know the name
    fn remove_measurement(&mut self, n: &Shortname) -> Result<Option<MeasIdx>, String> {
        let i = self.text.remove_measurement_by_name(n)?;
        self.data.drop_in_place(n.as_ref()).unwrap();
        Ok(i.map(|x| x.0))
    }

    fn push_measurement<T>(
        &mut self,
        n: <M::N as MightHave>::Wrapper<Shortname>,
        m: Measurement<M::P>,
        col: Vec<T::Native>,
    ) -> Result<Shortname, String>
    where
        T: PolarsNumericType,
        ChunkedArray<T>: IntoSeries,
    {
        let k = self.text.push_measurement(n, m)?;
        let ser = ChunkedArray::<T>::from_vec(k.as_ref().into(), col).into_series();
        self.data.with_column(ser).map_err(|e| e.to_string())?;
        Ok(k)
    }

    fn insert_measurement<T>(
        &mut self,
        i: MeasIdx,
        n: <M::N as MightHave>::Wrapper<Shortname>,
        m: Measurement<M::P>,
        col: Vec<T::Native>,
    ) -> Result<Shortname, String>
    where
        T: PolarsNumericType,
        ChunkedArray<T>: IntoSeries,
    {
        let k = self.text.insert_measurement(i, n, m)?;
        let ser = ChunkedArray::<T>::from_vec(k.as_ref().into(), col).into_series();
        self.data
            .insert_column(i.into(), ser)
            .map_err(|e| e.to_string())?;
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

impl<P> Measurement<P>
where
    P: VersionedMeasurement,
{
    fn as_column_type(
        &self,
        dt: AlphaNumType,
        byteord: &ByteOrd,
    ) -> Result<Option<ColumnType>, Vec<String>> {
        let mdt = self.specific.datatype().map(|d| d.into()).unwrap_or(dt);
        let rng = self.range();
        to_col_type(self.bytes().clone(), mdt, byteord, rng)
    }
}

from_anycoretext!(FCS2_0, CoreTEXT2_0);
from_anycoretext!(FCS3_0, CoreTEXT3_0);
from_anycoretext!(FCS3_1, CoreTEXT3_1);
from_anycoretext!(FCS3_2, CoreTEXT3_2);

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

fn hash_raw_pairs(pairs: RawPairs, conf: &RawTextReadConfig) -> PureSuccess<RawKeywords> {
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

// impl<V: IndexedKey> OptionalKw<V> {
//     // pub fn try_meas_from(self, n: MeasIdx) -> Result<V, String> {
//     //     self.0
//     //         .ok_or(format!("value for key {} does not exist", V::std(n)))
//     // }

//     pub fn try_meas_from(self) -> Result<V, NoExist> {
//         self.0.ok_or(NoExist(V::std(n)))
//     }
// }

// fn comp_to_spillover(comp: Compensation, ns: &[Shortname]) -> Option<Spillover> {
//     // Matrix should be square, so if inverse fails that means that somehow it
//     // isn't full rank
//     comp.matrix.try_inverse().map(|matrix| Spillover {
//         measurements: ns.to_vec(),
//         matrix,
//     })
// }

// // TODO doesn't this need to be transposed also?
// fn spillover_to_comp(spillover: Spillover, ns: &[Shortname]) -> Option<Compensation> {
//     // Start by making a new square matrix for all measurements, since the older
//     // $COMP keyword couldn't specify measurements and thus covered all of them.
//     // Then assign the spillover matrix to the bigger full matrix, using the
//     // index of the measurement names. This will be a spillover matrix defined
//     // for all measurements. Anything absent from the original will have 0 in
//     // it's row/column except for the diagonal. Finally, invert this result to
//     // get the compensation matrix.
//     let n = ns.len();
//     let mut full_matrix = DMatrix::<f32>::identity(n, n);
//     // ASSUME spillover measurements are a subset of names supplied to function
//     let positions: Vec<_> = spillover
//         .measurements
//         .into_iter()
//         .enumerate()
//         .flat_map(|(i, m)| ns.iter().position(|x| *x == m).map(|x| (i, x)))
//         .collect();
//     for r in positions.iter() {
//         for c in positions.iter() {
//             full_matrix[(r.1, c.1)] = spillover.matrix[(r.0, c.0)]
//         }
//     }
//     // Matrix should be square, so if inverse fails that means that somehow it
//     // isn't full rank
//     full_matrix
//         .try_inverse()
//         .map(|matrix| Compensation { matrix })
// }

impl<M> CoreTEXT<M, M::T, M::P, M::N, <M::N as MightHave>::Wrapper<Shortname>>
where
    M: VersionedMetadata,
    M::N: Clone,
{
    // NOTE return vec of strings in error term because these will generally
    // always be errors on failure with no warnings on success.
    pub fn as_column_layout(&self) -> Result<ColumnLayout, Vec<String>> {
        let dt = self.metadata.datatype();
        let byteord = self.metadata.specific.byteord();
        let ncols = self.measurements_named_vec().len();
        let (pass, fail): (Vec<_>, Vec<_>) = self
            .measurements_named_vec()
            .iter_non_center_values()
            .map(|(_, m)| m.as_column_type(dt, &byteord))
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

    fn df_names(&self) -> Vec<PlSmallStr> {
        self.all_shortnames()
            .into_iter()
            .map(|s| s.as_ref().into())
            .collect()
    }

    fn set_df_column_names(&self, df: &mut DataFrame) -> PolarsResult<()> {
        df.set_column_names(self.df_names())
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

    fn into_dataset_unchecked(
        self,
        data: DataFrame,
        analysis: Analysis,
    ) -> CoreDataset<M, M::T, M::P, M::N, <M::N as MightHave>::Wrapper<Shortname>> {
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
    ) -> Result<CoreDataset<M, M::T, M::P, M::N, <M::N as MightHave>::Wrapper<Shortname>>, String>
    {
        let w = data.width();
        let p = self.par().0;
        if w != p {
            Err(format!(
                "DATA has {w} columns but TEXT has {p} measurements"
            ))
        } else {
            Ok(self.into_dataset_unchecked(data, analysis))
        }
    }
}

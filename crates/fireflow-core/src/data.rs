use crate::config::WriteConfig;
use crate::error::*;
use crate::macros::{match_many_to_one, newtype_from};
use crate::segment::*;
use crate::text::byteord::*;
use crate::text::core::*;
use crate::text::keywords::*;
use crate::text::named_vec::MightHave;
use crate::text::optionalkw::*;
use crate::text::range::*;
use crate::validated::dataframe::*;
use crate::validated::nonstandard::MeasIdx;
use crate::validated::shortname::*;

use itertools::Itertools;
use nalgebra::DMatrix;
use polars::prelude::*;
use std::fmt;
use std::io;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::iter;
use std::num::{IntErrorKind, ParseIntError};
use std::str::FromStr;

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

pub(crate) type VersionedCoreDataset<M> = CoreDataset<
    M,
    <M as VersionedMetadata>::T,
    <M as VersionedMetadata>::P,
    <M as VersionedMetadata>::N,
    <<M as VersionedMetadata>::N as MightHave>::Wrapper<Shortname>,
>;

impl<M> VersionedCoreDataset<M>
where
    M: VersionedMetadata,
    M::N: Clone,
    M::L: VersionedDataLayout,
{
    /// Write this dataset (HEADER+TEXT+DATA+ANALYSIS) to a handle
    pub fn h_write<W>(&self, h: &mut BufWriter<W>, conf: &WriteConfig) -> ImpureResult<()>
    where
        W: Write,
    {
        let df = &self.data;

        let valid_df = FCSDataFrame::try_from(df.clone()).map_err(|_| Failure {
            reason: "dataframe is invalid".to_string(),
            deferred: PureErrorBuf::default(),
        })?;

        // TODO make sure all columns in dataframe are valid, and bail if not

        // Get the layout, or bail if we can't
        let layout = self.text.as_column_layout().map_err(|es| Failure {
            reason: "could not create data layout".to_string(),
            deferred: PureErrorBuf::from_many(es, PureErrorLevel::Error),
        })?;

        // Count number of measurements from layout. If the dataframe doesn't match
        // then something terrible happened and we need to escape through the
        // wormhole.
        let par = layout.ncols();
        let df_ncols = df.width();
        if df_ncols != par {
            Err(Failure::new(format!(
                "datafame columns ({df_ncols}) unequal to number of measurements ({par})"
            )))?;
        }

        // write HEADER+TEXT first
        let data_len = layout.nbytes(&valid_df, conf);
        let tot = Tot(df.height());
        let analysis_len = self.analysis.0.len();
        self.text.h_write(h, tot, data_len, analysis_len, conf)?;
        // write DATA
        layout.h_write(h, &valid_df, conf)?;
        // write ANALYSIS
        h.write_all(&self.analysis.0)?;
        Ok(PureSuccess::from(()))
    }

    /// Convert this dataset into a different FCS version
    pub fn try_convert<ToM>(self) -> PureResult<VersionedCoreDataset<ToM>>
    where
        M::N: Clone,
        ToM: VersionedMetadata,
        ToM: TryFromMetadata<M>,
        ToM::P: VersionedMeasurement,
        ToM::T: VersionedTime,
        ToM::N: MightHave,
        ToM::N: Clone,
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

// TODO add type-specific methods

// impl CoreDataset3_1 {
//     spillover_methods!(text);
// }

// impl CoreDataset3_2 {
//     spillover_methods!(text);
// }

/// The ANALYSIS segment, which is just a string of bytes
#[derive(Clone)]
pub struct Analysis(pub Vec<u8>);

newtype_from!(Analysis, Vec<u8>);

/// Read the analysis segment
pub(crate) fn h_read_analysis<R: Read + Seek>(
    h: &mut BufReader<R>,
    seg: &Segment,
) -> io::Result<Analysis> {
    let mut buf = vec![];
    h.seek(SeekFrom::Start(u64::from(seg.begin())))?;
    h.take(u64::from(seg.nbytes())).read_to_end(&mut buf)?;
    Ok(buf.into())
}

/// FCS file for any supported FCS version
#[derive(Clone)]
pub enum AnyCoreDataset {
    FCS2_0(CoreDataset2_0),
    FCS3_0(CoreDataset3_0),
    FCS3_1(CoreDataset3_1),
    FCS3_2(CoreDataset3_2),
}

impl AnyCoreDataset {
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

pub trait VersionedDataLayout: Sized {
    type S;
    type D;

    fn try_new(
        dt: AlphaNumType,
        size: Self::S,
        cs: Vec<ColumnLayoutData<Self::D>>,
    ) -> Result<Self, Vec<String>>;

    fn ncols(&self) -> usize;

    fn nbytes(&self, df: &FCSDataFrame, conf: &WriteConfig) -> usize;

    fn h_write<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        df: &FCSDataFrame,
        conf: &WriteConfig,
    ) -> ImpureResult<()>;

    fn into_reader(self, kws: &RawKeywords, data_seg: Segment) -> PureMaybe<ColumnReader>;

    fn try_new_from_raw(kws: &RawKeywords) -> Result<Self, Vec<String>>;
}

pub enum DataLayout2_0 {
    Ascii(AsciiLayout),
    Integer(AnyUintLayout),
    Float(FloatLayout),
}

pub enum DataLayout3_0 {
    Ascii(AsciiLayout),
    Integer(AnyUintLayout),
    Float(FloatLayout),
}

pub enum DataLayout3_1 {
    Ascii(AsciiLayout),
    Integer(FixedLayout<AnyUintType>),
    Float(FloatLayout),
}

pub enum DataLayout3_2 {
    // TODO we could just use delimited and mixed for this
    Ascii(AsciiLayout),
    Integer(FixedLayout<AnyUintType>),
    Float(FloatLayout),
    Mixed(FixedLayout<MixedType>),
}

pub enum AsciiLayout {
    Delimited(DelimitedLayout),
    Fixed(FixedLayout<AsciiType>),
}

pub enum FloatLayout {
    F32(FixedLayout<F32Type>),
    F64(FixedLayout<F64Type>),
}

pub struct DelimitedLayout {
    pub ncols: usize,
}

pub struct FixedLayout<C> {
    pub columns: Vec<C>,
}

pub enum AnyUintLayout {
    Uint08(FixedLayout<Uint08Type>),
    Uint16(FixedLayout<Uint16Type>),
    Uint24(FixedLayout<Uint24Type>),
    Uint32(FixedLayout<Uint32Type>),
    Uint40(FixedLayout<Uint40Type>),
    Uint48(FixedLayout<Uint48Type>),
    Uint56(FixedLayout<Uint56Type>),
    Uint64(FixedLayout<Uint64Type>),
}

/// The layout of the DATA segment
///
/// There are only two main configurations; delimited ASCII which is variable
/// width and fixed width which may contain ASCII or numeric bytes. In the
/// latter case, store the layout of each column, which may be a different
/// type (as given by the $DATATYPE/$PnDATATYPE keywords).
///
/// Generic parameter allows number of rows to also be encoded, which is
/// necessary for reading data. This is optional for the delimited case since
/// version 2.0 has $TOT as optional and it is impossible to back-calculate the
/// number of rows using $PnB in this case.
///
/// This will cover all possible data layouts, although not all possible layouts
/// will be valid for each version, so this is enforced elsewhere.
#[derive(Clone)]
pub enum DataLayout<T> {
    AsciiDelimited { nrows: Option<T>, ncols: usize },
    AlphaNum { nrows: T, columns: Vec<MixedType> },
}

/// The type of a non-delimited column in the DATA segment
#[derive(PartialEq, Clone, Copy)]
pub enum MixedType {
    Ascii(AsciiType),
    Integer(AnyUintType),
    Float(F32Type),
    Double(F64Type),
}

#[derive(PartialEq, Clone, Copy)]
pub struct AsciiType {
    pub(crate) chars: Chars,
}

impl From<AsciiType> for MixedType {
    fn from(value: AsciiType) -> Self {
        MixedType::Ascii(value)
    }
}

/// An f32 column
type F32Type = FloatType<4, f32>;

/// An f64 column
type F64Type = FloatType<8, f64>;

impl From<F32Type> for MixedType {
    fn from(value: F32Type) -> Self {
        MixedType::Float(value)
    }
}

impl From<F64Type> for MixedType {
    fn from(value: F64Type) -> Self {
        MixedType::Double(value)
    }
}

/// A floating point column (to be further constained)
#[derive(PartialEq, Clone, Copy)]
pub struct FloatType<const LEN: usize, T> {
    pub order: SizedByteOrd<LEN>,
    // TODO why is this here?
    pub range: T,
}

/// An integer column of some size (1-8 bytes)
#[derive(PartialEq, Clone, Copy)]
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

impl From<AnyUintType> for MixedType {
    fn from(value: AnyUintType) -> Self {
        MixedType::Integer(value)
    }
}

macro_rules! uint_to_mixed {
    ($uint:ident, $wrap:ident) => {
        impl From<$uint> for MixedType {
            fn from(value: $uint) -> Self {
                MixedType::Integer(AnyUintType::$wrap(value))
            }
        }
    };
}

uint_to_mixed!(Uint08Type, Uint08);
uint_to_mixed!(Uint16Type, Uint16);
uint_to_mixed!(Uint24Type, Uint24);
uint_to_mixed!(Uint32Type, Uint32);
uint_to_mixed!(Uint40Type, Uint40);
uint_to_mixed!(Uint48Type, Uint48);
uint_to_mixed!(Uint56Type, Uint56);
uint_to_mixed!(Uint64Type, Uint64);

type Uint08Type = UintType<u8, 1>;
type Uint16Type = UintType<u16, 2>;
type Uint24Type = UintType<u32, 3>;
type Uint32Type = UintType<u32, 4>;
type Uint40Type = UintType<u64, 5>;
type Uint48Type = UintType<u64, 6>;
type Uint56Type = UintType<u64, 7>;
type Uint64Type = UintType<u64, 8>;

/// A generic integer column type with a byte-layout and bitmask.
#[derive(PartialEq, Clone, Copy)]
pub struct UintType<T, const LEN: usize> {
    pub bitmask: T,
    pub size: SizedByteOrd<LEN>,
}

/// Instructions and data to write one column of the DATA segment
///
/// Each column contains a buffer with the data to be written (in the correct
/// type) and other type-specific information.
enum FixedColumnWriter {
    NumU8(NumColumnWriter<u8, SizedByteOrd<1>>),
    NumU16(NumColumnWriter<u16, SizedByteOrd<2>>),
    NumU24(NumColumnWriter<u32, SizedByteOrd<3>>),
    NumU32(NumColumnWriter<u32, SizedByteOrd<4>>),
    NumU40(NumColumnWriter<u64, SizedByteOrd<5>>),
    NumU48(NumColumnWriter<u64, SizedByteOrd<6>>),
    NumU56(NumColumnWriter<u64, SizedByteOrd<7>>),
    NumU64(NumColumnWriter<u64, SizedByteOrd<8>>),
    NumF32(NumColumnWriter<f32, SizedByteOrd<4>>),
    NumF64(NumColumnWriter<f64, SizedByteOrd<8>>),
    Ascii(NumColumnWriter<u64, Chars>),
}

struct NumColumnWriter<T, S> {
    data: Vec<T>,
    size: S,
}

/// Instructions and buffers to read the DATA segment
pub(crate) struct DataReader {
    pub(crate) column_reader: ColumnReader,
    pub(crate) begin: u64,
}

/// Instructions to read one column in the DATA segment.
///
/// Each "column" contains a vector to hold the numbers read from DATA. In all
/// but the case of delimited ASCII, this is pre-allocated with the number of
/// rows to make reading faster. Each column has other information necessary to
/// read the column (bitmask, width, etc).
pub enum ColumnReader {
    DelimitedAscii(DelimAsciiReader),
    AlphaNum(AlphaNumReader),
}

pub struct DelimAsciiReader {
    pub ncols: usize,
    pub nrows: Option<Tot>,
    pub nbytes: usize,
}

pub struct AlphaNumReader {
    pub nrows: Tot,
    pub columns: Vec<AlphaNumColumnReader>,
}

pub enum AlphaNumColumnReader {
    Ascii(AsciiColumnReader),
    Uint(AnyUintColumnReader),
    Float(FloatReader),
}

pub enum FloatReader {
    F32(FloatColumnReader<f32, 4>),
    F64(FloatColumnReader<f64, 8>),
}

pub struct FloatColumnReader<T, const LEN: usize> {
    pub column: Vec<T>,
    pub size: SizedByteOrd<LEN>,
}

pub struct AsciiColumnReader {
    pub column: Vec<u64>,
    pub width: Chars,
}

pub struct UintColumnReader<B, const LEN: usize> {
    pub column: Vec<B>,
    pub uint_type: UintType<B, LEN>,
}

pub enum AnyUintColumnReader {
    Uint08(UintColumnReader<u8, 1>),
    Uint16(UintColumnReader<u16, 2>),
    Uint24(UintColumnReader<u32, 3>),
    Uint32(UintColumnReader<u32, 4>),
    Uint40(UintColumnReader<u64, 5>),
    Uint48(UintColumnReader<u64, 6>),
    Uint56(UintColumnReader<u64, 7>),
    Uint64(UintColumnReader<u64, 8>),
}

impl FloatReader {
    fn h_read<R: Read>(&mut self, h: &mut BufReader<R>, r: usize) -> io::Result<()> {
        match self {
            FloatReader::F32(t) => Float32Type::h_read_to_column(h, t, r),
            FloatReader::F64(t) => Float64Type::h_read_to_column(h, t, r),
        }
    }

    fn into_pl_series(self, name: PlSmallStr) -> Series {
        match self {
            FloatReader::F32(x) => Float32Chunked::from_vec(name, x.column).into_series(),
            FloatReader::F64(x) => Float64Chunked::from_vec(name, x.column).into_series(),
        }
    }
}

impl DelimAsciiReader {
    fn h_read<R: Read>(self, h: &mut BufReader<R>) -> io::Result<DataFrame> {
        let mut buf = Vec::new();
        let mut row = 0;
        let mut col = 0;
        let mut last_was_delim = false;
        // Delimiters are tab, newline, carriage return, space, or comma. Any
        // consecutive delimiter counts as one, and delimiters can be mixed.
        let is_delim = |byte| byte == 9 || byte == 10 || byte == 13 || byte == 32 || byte == 44;
        let mut data = if let Some(nrows) = self.nrows {
            // FCS 2.0 files have an optional $TOT field, which complicates this a
            // bit. If we know the number of rows, initialize a bunch of zero-ed
            // vectors and fill them sequentially.
            let mut data: Vec<_> = iter::repeat_with(|| vec![0; nrows.0])
                .take(self.ncols)
                .collect();
            for b in h.bytes().take(self.nbytes) {
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
                        if col == self.ncols - 1 {
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
            let mut data: Vec<_> = iter::repeat_with(Vec::new).take(self.ncols).collect();
            for b in h.bytes().take(self.nbytes) {
                let byte = b?;
                // Delimiters are tab, newline, carriage return, space, or
                // comma. Any consecutive delimiter counts as one, and
                // delimiters can be mixed.
                if is_delim(byte) {
                    if !last_was_delim {
                        last_was_delim = true;
                        data[col].push(ascii_to_uint_io(buf.clone())?);
                        buf.clear();
                        if col == self.ncols - 1 {
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
}

impl AlphaNumReader {
    fn h_read<R: Read>(mut self, h: &mut BufReader<R>) -> io::Result<DataFrame> {
        let mut strbuf = String::new();
        for r in 0..self.nrows.0 {
            for c in self.columns.iter_mut() {
                match c {
                    AlphaNumColumnReader::Float(f) => f.h_read(h, r)?,
                    AlphaNumColumnReader::Uint(u) => u.h_read_to_column(h, r)?,
                    AlphaNumColumnReader::Ascii(d) => {
                        strbuf.clear();
                        h.take(u64::from(u8::from(d.width)))
                            .read_to_string(&mut strbuf)?;
                        d.column[r] = parse_u64_io(&strbuf)?;
                    }
                }
            }
        }
        // TODO get real column names here
        let ss: Vec<_> = self
            .columns
            .into_iter()
            .enumerate()
            .map(|(i, c)| c.into_pl_series(format!("X{i}").into()).into())
            .collect();
        DataFrame::new(ss).map_err(|e| io::Error::other(e.to_string()))
    }
}

/// Read the DATA segment and return a polars dataframe
pub(crate) fn h_read_data_segment<R: Read + Seek>(
    h: &mut BufReader<R>,
    r: DataReader,
) -> io::Result<DataFrame> {
    h.seek(SeekFrom::Start(r.begin))?;
    match r.column_reader {
        ColumnReader::DelimitedAscii(p) => p.h_read(h),
        ColumnReader::AlphaNum(p) => p.h_read(h),
        // ColumnReader::FixedWidthAscii(p) => read_data_ascii_fixed(h, &p),
        // ColumnReader::Single(p) => Float32Type::read_matrix(h, p),
        // ColumnReader::Double(p) => Float64Type::read_matrix(h, p),
        // ColumnReader::Uint(p) => read_data_int(h, p),
    }
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

macro_rules! series_cast {
    ($series:expr, $to:ty) => {
        $series.iter_native().map(|x| x as $to).collect()
    };
}

fn warn_bitmask<T: Ord + Copy>(xs: Vec<T>, deferred: &mut PureErrorBuf, bitmask: T) -> Vec<T> {
    let mut has_seen = false;
    // TODO can't I just find the max of the vector and use that?
    xs.into_iter()
        .map(|x| {
            if !has_seen && x > bitmask {
                deferred.push_warning("bitmask exceed, value truncated".to_string());
                has_seen = true
            }
            x.min(bitmask)
        })
        .collect()
}

macro_rules! convert_to_uint1 {
    ($series:expr, $deferred:expr, $wrap:ident, $to:ty, $ut:expr) => {
        FixedColumnWriter::$wrap(NumColumnWriter {
            data: warn_bitmask(series_cast!($series, $to), &mut $deferred, $ut.bitmask),
            size: $ut.size,
        })
    };
}

macro_rules! convert_to_uint {
    ($size:expr, $series:expr, $deferred:expr) => {
        match $size {
            AnyUintType::Uint08(ut) => {
                convert_to_uint1!($series, $deferred, NumU8, u8, ut)
            }
            AnyUintType::Uint16(ut) => {
                convert_to_uint1!($series, $deferred, NumU16, u16, ut)
            }
            AnyUintType::Uint24(ut) => {
                convert_to_uint1!($series, $deferred, NumU24, u32, ut)
            }
            AnyUintType::Uint32(ut) => {
                convert_to_uint1!($series, $deferred, NumU32, u32, ut)
            }
            AnyUintType::Uint40(ut) => {
                convert_to_uint1!($series, $deferred, NumU40, u64, ut)
            }
            AnyUintType::Uint48(ut) => {
                convert_to_uint1!($series, $deferred, NumU48, u64, ut)
            }
            AnyUintType::Uint56(ut) => {
                convert_to_uint1!($series, $deferred, NumU56, u64, ut)
            }
            AnyUintType::Uint64(ut) => {
                convert_to_uint1!($series, $deferred, NumU64, u64, ut)
            }
        }
    };
}

macro_rules! convert_to_float {
    ($size:expr, $series:expr, $wrap:ident, $to:ty) => {
        FixedColumnWriter::$wrap(NumColumnWriter {
            data: series_cast!($series, $to),
            size: $size,
        })
    };
}

macro_rules! convert_to_f32 {
    ($size:expr, $series:expr) => {
        convert_to_float!($size, $series, NumF32, f32)
    };
}

macro_rules! convert_to_f64 {
    ($size:expr, $series:expr) => {
        convert_to_float!($size, $series, NumF64, f64)
    };
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

fn make_uint_type_inner(bytes: Bytes, r: &Range, e: Endian) -> Result<AnyUintType, String> {
    // ASSUME this can only be 1-8
    match u8::from(bytes) {
        1 => UInt8Type::column_type_endian(r, e).map(AnyUintType::Uint08),
        2 => UInt16Type::column_type_endian(r, e).map(AnyUintType::Uint16),
        3 => <UInt32Type as IntFromBytes<4, 3>>::column_type_endian(r, e).map(AnyUintType::Uint24),
        4 => <UInt32Type as IntFromBytes<4, 4>>::column_type_endian(r, e).map(AnyUintType::Uint32),
        5 => <UInt64Type as IntFromBytes<8, 5>>::column_type_endian(r, e).map(AnyUintType::Uint40),
        6 => <UInt64Type as IntFromBytes<8, 6>>::column_type_endian(r, e).map(AnyUintType::Uint48),
        7 => <UInt64Type as IntFromBytes<8, 7>>::column_type_endian(r, e).map(AnyUintType::Uint56),
        8 => <UInt64Type as IntFromBytes<8, 8>>::column_type_endian(r, e).map(AnyUintType::Uint64),
        _ => unreachable!(),
    }
}

fn make_uint_type(b: BitsOrChars, r: &Range, e: Endian) -> Result<AnyUintType, String> {
    if let Some(bytes) = b.bytes() {
        make_uint_type_inner(bytes, r, e)
    } else {
        Err("$PnB is not an octet".to_string())
    }
}

impl FixedLayout<AnyUintType> {
    pub(crate) fn try_new<D>(cs: Vec<ColumnLayoutData<D>>, o: Endian) -> Result<Self, Vec<String>> {
        let ncols = cs.len();
        let (ws, rs): (Vec<_>, Vec<_>) = cs.into_iter().map(|c| (c.width, c.range)).unzip();
        let fixed: Vec<_> = ws.into_iter().flat_map(|w: Width| w.as_fixed()).collect();
        if fixed.len() < ncols {
            return Err(vec!["not all fixed width".into()]);
        }
        let bytes: Vec<_> = fixed.into_iter().flat_map(|f| f.bytes()).collect();
        if bytes.len() < ncols {
            return Err(vec!["bad stuff".into()]);
        }
        let (columns, fail): (Vec<_>, Vec<_>) = bytes
            .into_iter()
            .zip(rs)
            .map(|(b, r)| make_uint_type_inner(b, &r, o))
            .partition_result();
        if fail.is_empty() {
            Ok(FixedLayout { columns })
        } else {
            Err(fail)
        }
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
{
    fn next_power_2(x: Self) -> Self;

    fn int_from_str(s: &str) -> Result<Self, IntErrorKind> {
        s.parse()
            .map_err(|e| <Self as FromStr>::Err::err_kind(&e).clone())
    }

    fn maxval() -> Self;
}

trait NumProps<const DTLEN: usize>: Sized + Copy {
    fn from_big(buf: [u8; DTLEN]) -> Self;

    fn from_little(buf: [u8; DTLEN]) -> Self;

    fn to_big(self) -> [u8; DTLEN];

    fn to_little(self) -> [u8; DTLEN];
}

trait OrderedFromBytes<const DTLEN: usize, const OLEN: usize>: NumProps<DTLEN> {
    fn h_read_from_ordered<R: Read>(h: &mut BufReader<R>, order: &[u8; OLEN]) -> io::Result<Self> {
        let mut tmp = [0; OLEN];
        let mut buf = [0; DTLEN];
        h.read_exact(&mut tmp)?;
        for (i, j) in order.iter().enumerate() {
            buf[usize::from(*j)] = tmp[i];
        }
        Ok(Self::from_little(buf))
    }

    fn h_write_from_ordered<W: Write>(
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
        Self::Native::int_from_str(range.as_ref())
            .map(Self::Native::next_power_2)
            .or_else(|e| match e {
                IntErrorKind::PosOverflow => Ok(Self::Native::maxval()),
                _ => Err(format!("could not convert to u{INTLEN}")),
            })
    }

    fn column_type_endian(
        range: &Range,
        endian: Endian,
    ) -> Result<UintType<Self::Native, INTLEN>, String> {
        // TODO be more specific, which means we need the measurement index
        Self::range_to_bitmask(range).map(|bitmask| UintType {
            bitmask,
            size: endian.into(),
        })
    }

    fn column_type(
        range: &Range,
        byteord: &ByteOrd,
        // index: MeasIdx,
    ) -> Result<UintType<Self::Native, INTLEN>, Vec<String>> {
        // TODO be more specific, which means we need the measurement index
        let m = Self::range_to_bitmask(range);
        let s = byteord.as_sized();
        match (m, s) {
            (Ok(bitmask), Ok(size)) => Ok(UintType { bitmask, size }),
            (a, b) => Err([a.err(), b.err()].into_iter().flatten().collect()),
        }
    }

    fn layout(
        rs: Vec<Range>,
        byteord: &ByteOrd,
    ) -> Result<FixedLayout<UintType<Self::Native, INTLEN>>, Vec<String>> {
        let (columns, fail): (Vec<_>, Vec<_>) = rs
            .into_iter()
            .map(|r| Self::column_type(&r, byteord))
            .partition_result();
        if fail.is_empty() {
            Ok(FixedLayout { columns })
        } else {
            Err(fail.into_iter().flatten().collect())
        }
    }

    fn h_read_int_masked<R: Read>(
        h: &mut BufReader<R>,
        byteord: &SizedByteOrd<INTLEN>,
        bitmask: Self::Native,
    ) -> io::Result<Self::Native> {
        Self::h_read_int(h, byteord).map(|x| x.min(bitmask))
    }

    fn h_read_int<R: Read>(
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
            SizedByteOrd::Order(order) => Self::Native::h_read_from_ordered(h, order),
        }
    }

    fn h_read_to_column<R: Read>(
        h: &mut BufReader<R>,
        d: &mut UintColumnReader<Self::Native, INTLEN>,
        row: usize,
    ) -> io::Result<()> {
        d.column[row] = Self::h_read_int_masked(h, &d.uint_type.size, d.uint_type.bitmask)?;
        Ok(())
    }

    fn h_write_int<W: Write>(
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
            SizedByteOrd::Order(order) => Self::Native::h_write_from_ordered(h, order, x),
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
    fn h_read_to_column<R: Read>(
        h: &mut BufReader<R>,
        column: &mut FloatColumnReader<Self::Native, LEN>,
        row: usize,
    ) -> io::Result<()> {
        column.column[row] = Self::h_read_float(h, &column.size)?;
        Ok(())
    }

    // /// Read byte sequence into a matrix of floats
    // fn read_matrix<R: Read>(h: &mut BufReader<R>, p: FloatReader<LEN>) -> io::Result<DataFrame> {
    //     let mut columns: Vec<_> = iter::repeat_with(|| vec![Self::Native::zero(); p.nrows])
    //         .take(p.ncols)
    //         .collect();
    //     for row in 0..p.nrows {
    //         for column in columns.iter_mut() {
    //             column[row] = Self::read_float(h, &p.byteord)?;
    //         }
    //     }
    //     let ss: Vec<_> = columns
    //         .into_iter()
    //         .enumerate()
    //         .map(|(i, s)| {
    //             ChunkedArray::<Self>::from_vec(format!("M{i}").into(), s)
    //                 .into_series()
    //                 .into()
    //         })
    //         .collect();
    //     DataFrame::new(ss).map_err(|e| io::Error::other(e.to_string()))
    //     // Ok(Dataframe::from(
    //     //     columns.into_iter().map(Vec::<Self>::into).collect(),
    //     // ))
    // }

    /// Make configuration to read one column of floats in a dataset.
    fn column_reader(
        order: SizedByteOrd<LEN>,
        total_events: Tot,
    ) -> FloatColumnReader<Self::Native, LEN> {
        FloatColumnReader {
            column: vec![Self::Native::default(); total_events.0],
            size: order,
        }
    }

    fn column_type_endian(o: Endian, r: &Range) -> Result<FloatType<LEN, Self::Native>, String> {
        r.as_ref()
            .parse::<Self::Native>()
            .map(|range| FloatType {
                order: o.into(),
                range,
            })
            .map_err(|e| e.to_string())
    }

    // TODO what happens if byteord is endian which is only 4 bytes wide by
    // definition but we want a 64bit float? probably bad stuff
    fn column_type(o: &ByteOrd, r: &Range) -> Result<FloatType<LEN, Self::Native>, Vec<String>> {
        match (o.as_sized(), r.as_ref().parse::<Self::Native>()) {
            (Ok(order), Ok(range)) => Ok(FloatType { order, range }),
            (a, b) => Err([a.err(), b.err().map(|s| s.to_string())]
                .into_iter()
                .flatten()
                .collect()),
        }
    }

    fn layout_endian<D>(
        cs: Vec<ColumnLayoutData<D>>,
        endian: Endian,
    ) -> Result<FixedLayout<FloatType<LEN, Self::Native>>, Vec<String>> {
        let (pass, fail): (Vec<_>, Vec<_>) = cs
            .into_iter()
            .map(|c| {
                if c.width
                    .as_fixed()
                    .and_then(|f| f.bytes())
                    .is_some_and(|b| u8::from(b) == 4)
                {
                    Self::column_type_endian(endian, &c.range)
                } else {
                    // TODO which one?
                    Err(format!("$PnB is not {} bytes wide", 4))
                }
            })
            .partition_result();
        if fail.is_empty() {
            Ok(FixedLayout { columns: pass })
        } else {
            Err(fail)
        }
    }

    fn layout<D>(
        cs: Vec<ColumnLayoutData<D>>,
        byteord: &ByteOrd,
    ) -> Result<FixedLayout<FloatType<LEN, Self::Native>>, String> {
        let (pass, fail): (Vec<_>, Vec<_>) = cs
            .into_iter()
            .map(|c| {
                if c.width
                    .as_fixed()
                    .and_then(|f| f.bytes())
                    .is_some_and(|b| u8::from(b) == 4)
                {
                    Self::column_type(byteord, &c.range)
                } else {
                    // TODO which one?
                    Err(vec![format!("$PnB is not {} bytes wide", 4)])
                }
            })
            .partition_result();
        if fail.is_empty() {
            Ok(FixedLayout { columns: pass })
        } else {
            Err(fail.into_iter().flatten().collect())
        }
    }

    // fn make_matrix_parser(
    //     byteord: &ByteOrd,
    //     par: usize,
    //     total_events: usize,
    // ) -> PureMaybe<FloatReader<LEN>> {
    //     let res = byteord.as_sized().map(|b| FloatReader {
    //         nrows: total_events,
    //         ncols: par,
    //         byteord: b,
    //     });
    //     PureMaybe::from_result_1(res, PureErrorLevel::Error)
    // }

    fn h_read_float<R: Read>(
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
            SizedByteOrd::Order(order) => Self::Native::h_read_from_ordered(h, order),
        }
    }

    fn h_write_float<W: Write>(
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
            SizedByteOrd::Order(order) => Self::Native::h_write_from_ordered(h, order, x),
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
    c: &AnyFCSColumn,
    w: MixedType,
    conf: &WriteConfig,
) -> PureSuccess<FixedColumnWriter> {
    // ASSUME this won't fail
    //
    // TODO could make this more robust by wrapping the Column in an
    // encapsulated newtype that only has valid types
    let mut deferred = PureErrorBuf::default();

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
        MixedType::Ascii(a) => {
            let chars = a.chars;
            let data: Vec<_> = match c {
                AnyFCSColumn::U08(xs) => series_cast!(xs, u64),
                AnyFCSColumn::U16(xs) => series_cast!(xs, u64),
                AnyFCSColumn::U32(xs) => series_cast!(xs, u64),
                AnyFCSColumn::U64(xs) => xs.iter_native().collect(),
                AnyFCSColumn::F32(xs) => {
                    num_warn(&mut deferred, "float", "uint64");
                    series_cast!(xs, u64)
                }
                AnyFCSColumn::F64(xs) => {
                    num_warn(&mut deferred, "double", "uint64");
                    series_cast!(xs, u64)
                }
            };
            let maxdigits = data
                .iter()
                .max()
                .and_then(|x| x.checked_ilog10().map(|y| y + 1))
                .unwrap_or(1);
            if maxdigits > u8::from(a.chars).into() {
                let msg = format!(
                    "Largest value has {maxdigits} digits but only {chars} \
                     characters are allocated, data will be truncated.",
                );
                deferred.push_msg_leveled(msg, conf.disallow_lossy_conversions);
            }
            FixedColumnWriter::Ascii(NumColumnWriter { data, size: chars })
        }

        // Uint* -> Uint* is quite easy, just compare sizes and warn if the
        // target type is too small. Float/double -> Uint always could
        // potentially truncate a fractional value. Also check to see if
        // bitmask is exceeded, and if so truncate and warn user.
        MixedType::Integer(ut) => {
            match c {
                AnyFCSColumn::F32(_) => num_warn(&mut deferred, "float", "uint"),
                AnyFCSColumn::F64(_) => num_warn(&mut deferred, "float", "uint"),
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
            match c {
                AnyFCSColumn::U08(xs) => convert_to_uint!(ut, xs, deferred),
                AnyFCSColumn::U16(xs) => convert_to_uint!(ut, xs, deferred),
                AnyFCSColumn::U32(xs) => convert_to_uint!(ut, xs, deferred),
                AnyFCSColumn::U64(xs) => convert_to_uint!(ut, xs, deferred),
                AnyFCSColumn::F32(xs) => convert_to_uint!(ut, xs, deferred),
                AnyFCSColumn::F64(xs) => convert_to_uint!(ut, xs, deferred),
            }
        }

        // Floats can hold small uints and themselves, anything else might
        // truncate.
        MixedType::Float(t) => {
            match c {
                AnyFCSColumn::U32(_) => num_warn(&mut deferred, "float", "uint32"),
                AnyFCSColumn::U64(_) => num_warn(&mut deferred, "float", "uint64"),
                AnyFCSColumn::F64(_) => num_warn(&mut deferred, "float", "double"),
                _ => (),
            }
            match c {
                AnyFCSColumn::U08(xs) => convert_to_f32!(t.order, xs),
                AnyFCSColumn::U16(xs) => convert_to_f32!(t.order, xs),
                AnyFCSColumn::U32(xs) => convert_to_f32!(t.order, xs),
                AnyFCSColumn::U64(xs) => convert_to_f32!(t.order, xs),
                AnyFCSColumn::F32(xs) => convert_to_f32!(t.order, xs),
                AnyFCSColumn::F64(xs) => convert_to_f32!(t.order, xs),
            }
        }

        // Doubles can hold all but uint64
        MixedType::Double(t) => {
            if let AnyFCSColumn::U64(_) = c {
                num_warn(&mut deferred, "double", "uint64")
            }
            match c {
                AnyFCSColumn::U08(xs) => convert_to_f64!(t.order, xs),
                AnyFCSColumn::U16(xs) => convert_to_f64!(t.order, xs),
                AnyFCSColumn::U32(xs) => convert_to_f64!(t.order, xs),
                AnyFCSColumn::U64(xs) => convert_to_f64!(t.order, xs),
                AnyFCSColumn::F32(xs) => convert_to_f64!(t.order, xs),
                AnyFCSColumn::F64(xs) => convert_to_f64!(t.order, xs),
            }
        }
    };
    PureSuccess {
        data: res,
        deferred,
    }
}

/// Convert Series into a u64 vector.
///
/// Used when writing delimited ASCII. This is faster and more convenient
/// than the general coercion function.
fn series_coerce64(c: &AnyFCSColumn, conf: &WriteConfig) -> PureSuccess<Vec<u64>> {
    // ASSUME this won't fail
    let mut deferred = PureErrorBuf::default();

    let num_warn = |d: &mut PureErrorBuf, from, to| {
        let msg = format!("converting {from} to {to} may truncate data");
        d.push_msg_leveled(msg, conf.disallow_lossy_conversions);
    };

    let res = match c {
        AnyFCSColumn::U08(xs) => series_cast!(xs, u64),
        AnyFCSColumn::U16(xs) => series_cast!(xs, u64),
        AnyFCSColumn::U32(xs) => series_cast!(xs, u64),
        AnyFCSColumn::U64(xs) => series_cast!(xs, u64),
        AnyFCSColumn::F32(xs) => {
            num_warn(&mut deferred, "float", "uint64");
            series_cast!(xs, u64)
        }
        AnyFCSColumn::F64(xs) => {
            num_warn(&mut deferred, "double", "uint64");
            series_cast!(xs, u64)
        }
    };
    PureSuccess {
        data: res,
        deferred,
    }
}

macro_rules! impl_num_props {
    ($size:expr, $t:ty) => {
        impl NumProps<$size> for $t {
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

impl_num_props!(1, u8);
impl_num_props!(2, u16);
impl_num_props!(4, u32);
impl_num_props!(8, u64);
impl_num_props!(4, f32);
impl_num_props!(8, f64);

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

impl AlphaNumColumnReader {
    fn into_pl_series(self, name: PlSmallStr) -> Series {
        match self {
            AlphaNumColumnReader::Ascii(x) => UInt64Chunked::from_vec(name, x.column).into_series(),
            AlphaNumColumnReader::Float(x) => x.into_pl_series(name),
            AlphaNumColumnReader::Uint(x) => x.into_pl_series(name),
        }
    }
}

impl AnyUintColumnReader {
    fn into_pl_series(self, name: PlSmallStr) -> Series {
        match self {
            AnyUintColumnReader::Uint08(x) => UInt8Chunked::from_vec(name, x.column).into_series(),
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

macro_rules! uint_reader_from_column {
    ($x:ident, $t:expr, $($a:ident),+) => {
        match $x {
            $(
                AnyUintType::$a(uint_type) => AnyUintColumnReader::$a(UintColumnReader {
                    uint_type,
                    column: vec![0; $t],
                }),
            )+
        }
    };
}

impl AnyUintColumnReader {
    fn from_column(ut: AnyUintType, total_events: Tot) -> Self {
        let t = total_events.0;
        uint_reader_from_column!(
            ut, t, Uint08, Uint16, Uint24, Uint32, Uint40, Uint48, Uint56, Uint64
        )
    }

    fn h_read_to_column<R: Read>(&mut self, h: &mut BufReader<R>, r: usize) -> io::Result<()> {
        match self {
            AnyUintColumnReader::Uint08(d) => UInt8Type::h_read_to_column(h, d, r)?,
            AnyUintColumnReader::Uint16(d) => UInt16Type::h_read_to_column(h, d, r)?,
            AnyUintColumnReader::Uint24(d) => UInt32Type::h_read_to_column(h, d, r)?,
            AnyUintColumnReader::Uint32(d) => UInt32Type::h_read_to_column(h, d, r)?,
            AnyUintColumnReader::Uint40(d) => UInt64Type::h_read_to_column(h, d, r)?,
            AnyUintColumnReader::Uint48(d) => UInt64Type::h_read_to_column(h, d, r)?,
            AnyUintColumnReader::Uint56(d) => UInt64Type::h_read_to_column(h, d, r)?,
            AnyUintColumnReader::Uint64(d) => UInt64Type::h_read_to_column(h, d, r)?,
        }
        Ok(())
    }
}

fn into_writable_columns(
    df: &FCSDataFrame,
    cs: Vec<MixedType>,
    conf: &WriteConfig,
) -> PureSuccess<Vec<FixedColumnWriter>> {
    let cols = df.columns();
    let (writable_columns, msgs): (Vec<_>, Vec<_>) = cs
        .into_iter()
        .zip(cols)
        .map(|(w, c)| {
            let succ = series_coerce(&c, w, conf);
            (succ.data, succ.deferred)
        })
        .unzip();
    PureSuccess {
        data: writable_columns,
        deferred: PureErrorBuf::mconcat(msgs),
    }
}

fn into_writable_matrix64(df: &FCSDataFrame, conf: &WriteConfig) -> PureSuccess<DMatrix<u64>> {
    let mut it = df.columns().into_iter().map(|c| {
        let res = series_coerce64(&c, conf);
        (res.data, res.deferred)
    });
    let m = DMatrix::from_iterator(
        df.as_ref().height(),
        df.as_ref().width(),
        it.by_ref().flat_map(|x| x.0),
    );
    let msgs = it.map(|x| x.1).collect();
    PureSuccess {
        data: m,
        deferred: PureErrorBuf::mconcat(msgs),
    }
}

fn h_write_numeric_dataframe<W: Write>(
    h: &mut BufWriter<W>,
    cs: Vec<MixedType>,
    df: &FCSDataFrame,
    conf: &WriteConfig,
) -> ImpureResult<()> {
    let df_nrows = df.as_ref().height();
    into_writable_columns(df, cs, conf).try_map(|writable_columns| {
        for r in 0..df_nrows {
            for c in writable_columns.iter() {
                match c {
                    FixedColumnWriter::NumU8(w) => UInt8Type::h_write_int(h, &w.size, w.data[r]),
                    FixedColumnWriter::NumU16(w) => UInt16Type::h_write_int(h, &w.size, w.data[r]),
                    FixedColumnWriter::NumU24(w) => UInt32Type::h_write_int(h, &w.size, w.data[r]),
                    FixedColumnWriter::NumU32(w) => UInt32Type::h_write_int(h, &w.size, w.data[r]),
                    FixedColumnWriter::NumU40(w) => UInt64Type::h_write_int(h, &w.size, w.data[r]),
                    FixedColumnWriter::NumU48(w) => UInt64Type::h_write_int(h, &w.size, w.data[r]),
                    FixedColumnWriter::NumU56(w) => UInt64Type::h_write_int(h, &w.size, w.data[r]),
                    FixedColumnWriter::NumU64(w) => UInt64Type::h_write_int(h, &w.size, w.data[r]),
                    FixedColumnWriter::NumF32(w) => {
                        Float32Type::h_write_float(h, &w.size, w.data[r])
                    }
                    FixedColumnWriter::NumF64(w) => {
                        Float64Type::h_write_float(h, &w.size, w.data[r])
                    }
                    FixedColumnWriter::Ascii(w) => h_write_ascii_int(h, w.size, w.data[r]),
                }?
            }
        }
        Ok(PureSuccess::from(()))
    })
}

fn h_write_delimited_matrix<W: Write>(h: &mut BufWriter<W>, m: DMatrix<u64>) -> ImpureResult<()> {
    for r in 0..m.nrows() {
        for c in 0..m.ncols() {
            let x = m[(r, c)];
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
            if !(c == m.ncols() - 1 && r == m.nrows() - 1) {
                h.write_all(&[32])?; // 32 = space in ASCII
            }
        }
    }
    Ok(PureSuccess::from(()))
}

// TODO also check scale here?
impl MixedType {
    pub(crate) fn try_new(
        b: Width,
        dt: AlphaNumType,
        // TODO this should only take an endian since it only applies to 3.2
        // and has no chance in hogwarts of being in anything earlier
        endian: Endian,
        rng: &Range,
    ) -> Result<Option<Self>, String> {
        match b {
            Width::Fixed(f) => match dt {
                AlphaNumType::Ascii => {
                    if let Some(chars) = f.chars() {
                        Ok(Self::Ascii(AsciiType { chars }))
                    } else {
                        Err("$DATATYPE=A but $PnB greater than 20 chars".to_string())
                    }
                }
                AlphaNumType::Integer => make_uint_type(f, rng, endian).map(Self::Integer),
                AlphaNumType::Single => {
                    if let Some(bytes) = f.bytes() {
                        if u8::from(bytes) == 4 {
                            Float32Type::column_type_endian(endian, rng).map(Self::Float)
                        } else {
                            Err(format!("$DATATYPE=F but $PnB={}", f.inner()))
                        }
                    } else {
                        Err(format!("$PnB is not an octet, got {}", f.inner()))
                    }
                }
                AlphaNumType::Double => {
                    if let Some(bytes) = f.bytes() {
                        if u8::from(bytes) == 8 {
                            Float64Type::column_type_endian(endian, rng).map(Self::Double)
                        } else {
                            Err(format!("$DATATYPE=D but $PnB={}", f.inner()))
                        }
                    } else {
                        Err(format!("$PnB is not an octet, got {}", f.inner()))
                    }
                }
            }
            .map(Some),
            Width::Variable => match dt {
                // ASSUME the only way this can happen is if $DATATYPE=A since
                // Ascii is not allowed in $PnDATATYPE.
                AlphaNumType::Ascii => Ok(None),
                _ => Err(format!("variable $PnB not allowed for {dt}")),
            },
        }
    }
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

pub struct ColumnLayoutData<D> {
    pub width: Width,
    pub range: Range,
    pub datatype: D,
}

impl<C> FixedLayout<C>
where
    C: IsFixed,
{
    fn event_width(&self) -> usize {
        self.columns.iter().map(|c| c.width()).sum()
    }

    fn ncols(&self) -> usize {
        self.columns.len()
    }

    fn nbytes(&self, nrows: usize) -> usize {
        self.event_width() * nrows
    }

    pub fn into_reader(self, seg: Segment, kw_tot: Option<Tot>) -> PureSuccess<AlphaNumReader> {
        let n = seg.nbytes() as usize;
        let w = self.event_width();
        let total_events = n / w;
        let remainder = n % w;
        let mut deferred = PureErrorBuf::default();
        if let Some(t) = kw_tot {
            if t.0 != total_events {
                let msg = format!(
                    "$TOT field is {t} but number of events \
                     that evenly fit into DATA is {total_events}",
                );
                // TODO toggle
                deferred.push_warning(msg);
            }
        }
        if remainder > 0 {
            let msg = format!(
                "Events are {w} bytes wide, but this does not evenly \
                     divide DATA segment which is {n} bytes long \
                     (remainder of {remainder})",
            );
            // TODO toggle
            deferred.push_warning(msg);
        }
        let columns = self
            .columns
            .into_iter()
            .map(|c| c.into_reader(total_events))
            .collect();
        let r = AlphaNumReader {
            columns,
            nrows: Tot(total_events),
        };
        PureSuccess { data: r, deferred }
    }

    fn h_write<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        df: &FCSDataFrame,
        conf: &WriteConfig,
    ) -> ImpureResult<()>
    where
        MixedType: From<C>,
        C: Copy,
    {
        // ASSUME the dataframe will be coerced such that this is valid
        // let event_width = self.event_width();
        // let data_len = event_width * df.height();
        // write_text(h, data_len)?;
        let col_types = self.columns.iter().map(|c| (*c).into()).collect();
        h_write_numeric_dataframe(h, col_types, df, conf)
    }
}

pub trait IsFixed {
    fn width(&self) -> usize;

    fn into_reader(self, nrows: usize) -> AlphaNumColumnReader;
}

impl<X, const LEN: usize> IsFixed for UintType<X, LEN>
where
    X: Clone,
    X: Default,
    AlphaNumColumnReader: From<UintColumnReader<X, LEN>>,
{
    fn width(&self) -> usize {
        LEN
    }

    fn into_reader(self, nrows: usize) -> AlphaNumColumnReader {
        UintColumnReader {
            column: vec![X::default(); nrows],
            uint_type: self,
        }
        .into()
    }
}

impl IsFixed for AnyUintType {
    fn width(&self) -> usize {
        match_many_to_one!(
            self,
            AnyUintType,
            [Uint08, Uint16, Uint24, Uint32, Uint40, Uint48, Uint56, Uint64],
            x,
            { (*x).width() }
        )
    }

    fn into_reader(self, nrows: usize) -> AlphaNumColumnReader {
        match_many_to_one!(
            self,
            AnyUintType,
            [Uint08, Uint16, Uint24, Uint32, Uint40, Uint48, Uint56, Uint64],
            x,
            { x.into_reader(nrows) }
        )
    }
}

macro_rules! uint_from_reader {
    ($from:path, $wrap:ident) => {
        impl From<$from> for AlphaNumColumnReader {
            fn from(value: $from) -> Self {
                AlphaNumColumnReader::Uint(AnyUintColumnReader::$wrap(value))
            }
        }
    };
}

uint_from_reader!(UintColumnReader<u8, 1>, Uint08);
uint_from_reader!(UintColumnReader<u16, 2>, Uint16);
uint_from_reader!(UintColumnReader<u32, 3>, Uint24);
uint_from_reader!(UintColumnReader<u32, 4>, Uint32);
uint_from_reader!(UintColumnReader<u64, 5>, Uint40);
uint_from_reader!(UintColumnReader<u64, 6>, Uint48);
uint_from_reader!(UintColumnReader<u64, 7>, Uint56);
uint_from_reader!(UintColumnReader<u64, 8>, Uint64);

// TODO flip args to make more consistent
impl<X, const LEN: usize> IsFixed for FloatType<LEN, X>
where
    X: Clone,
    X: Default,
    AlphaNumColumnReader: From<FloatColumnReader<X, LEN>>,
{
    fn width(&self) -> usize {
        LEN
    }

    fn into_reader(self, nrows: usize) -> AlphaNumColumnReader {
        FloatColumnReader {
            column: vec![X::default(); nrows],
            size: self.order,
        }
        .into()
    }
}

impl From<FloatColumnReader<f32, 4>> for AlphaNumColumnReader {
    fn from(value: FloatColumnReader<f32, 4>) -> Self {
        AlphaNumColumnReader::Float(FloatReader::F32(value))
    }
}

impl From<FloatColumnReader<f64, 8>> for AlphaNumColumnReader {
    fn from(value: FloatColumnReader<f64, 8>) -> Self {
        AlphaNumColumnReader::Float(FloatReader::F64(value))
    }
}

impl IsFixed for AsciiType {
    fn width(&self) -> usize {
        u8::from(self.chars).into()
    }

    fn into_reader(self, nrows: usize) -> AlphaNumColumnReader {
        AlphaNumColumnReader::Ascii(AsciiColumnReader {
            column: vec![0; nrows],
            width: self.chars,
        })
    }
}

impl IsFixed for MixedType {
    fn width(&self) -> usize {
        match self {
            MixedType::Ascii(a) => u8::from(a.chars).into(),
            MixedType::Integer(i) => i.width(),
            MixedType::Float(f) => f.width(),
            MixedType::Double(d) => d.width(),
        }
    }

    fn into_reader(self, nrows: usize) -> AlphaNumColumnReader {
        match self {
            MixedType::Ascii(a) => a.into_reader(nrows),
            MixedType::Integer(i) => i.into_reader(nrows),
            MixedType::Float(f) => f.into_reader(nrows),
            MixedType::Double(d) => d.into_reader(nrows),
        }
    }
}

macro_rules! uint_with_tot {
    ($self:expr, $data_seg:expr, $tot:expr, $($m:ident),*) => {
        match $self {
            $(
                AnyUintLayout::$m(fl) => fl.into_reader($data_seg, $tot),
            )*
        }
    };
}

impl AnyUintLayout {
    pub(crate) fn try_new<D>(
        cs: Vec<ColumnLayoutData<D>>,
        o: &ByteOrd,
    ) -> Result<Self, Vec<String>> {
        let ncols = cs.len();
        let (ws, rs): (Vec<_>, Vec<_>) = cs.into_iter().map(|c| (c.width, c.range)).unzip();
        let fixed: Vec<_> = ws.into_iter().flat_map(|w: Width| w.as_fixed()).collect();
        if fixed.len() < ncols {
            return Err(vec!["not all fixed width".into()]);
        }
        let bytes: Vec<_> = fixed.into_iter().flat_map(|f| f.bytes()).collect();
        if bytes.len() < ncols {
            return Err(vec!["bad stuff".into()]);
        }
        let mut fixed_unique = bytes.iter().unique();
        // ASSUME this won't fail
        let bytes1 = fixed_unique.next().unwrap();
        if fixed_unique.count() > 0 {
            return Err(vec!["bad stuff".into()]);
        }
        match u8::from(*bytes1) {
            1 => UInt8Type::layout(rs, o).map(AnyUintLayout::Uint08),
            2 => UInt16Type::layout(rs, o).map(AnyUintLayout::Uint16),
            3 => <UInt32Type as IntFromBytes<4, 3>>::layout(rs, o).map(AnyUintLayout::Uint24),
            4 => <UInt32Type as IntFromBytes<4, 4>>::layout(rs, o).map(AnyUintLayout::Uint32),
            5 => <UInt64Type as IntFromBytes<8, 5>>::layout(rs, o).map(AnyUintLayout::Uint40),
            6 => <UInt64Type as IntFromBytes<8, 6>>::layout(rs, o).map(AnyUintLayout::Uint48),
            7 => <UInt64Type as IntFromBytes<8, 7>>::layout(rs, o).map(AnyUintLayout::Uint56),
            8 => <UInt64Type as IntFromBytes<8, 8>>::layout(rs, o).map(AnyUintLayout::Uint64),
            _ => unreachable!(),
        }
    }

    fn ncols(&self) -> usize {
        match_many_to_one!(
            self,
            AnyUintLayout,
            [Uint08, Uint16, Uint24, Uint32, Uint40, Uint48, Uint56, Uint64],
            l,
            { l.columns.len() }
        )
    }

    fn nbytes(&self, nrows: usize) -> usize {
        match_many_to_one!(
            self,
            AnyUintLayout,
            [Uint08, Uint16, Uint24, Uint32, Uint40, Uint48, Uint56, Uint64],
            l,
            { l.nbytes(nrows) }
        )
    }

    fn into_reader(self, data_seg: Segment, tot: Option<Tot>) -> PureSuccess<AlphaNumReader> {
        uint_with_tot!(
            self, data_seg, tot, Uint08, Uint16, Uint24, Uint32, Uint40, Uint48, Uint56, Uint64
        )
    }

    fn h_write<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        df: &FCSDataFrame,
        conf: &WriteConfig,
    ) -> ImpureResult<()> {
        match_many_to_one!(
            self,
            AnyUintLayout,
            [Uint08, Uint16, Uint24, Uint32, Uint40, Uint48, Uint56, Uint64],
            l,
            { l.h_write(h, df, conf) }
        )
    }
}

impl AsciiLayout {
    pub(crate) fn try_new<D>(cs: Vec<ColumnLayoutData<D>>) -> Result<Self, String> {
        let ncols = cs.len();
        let fixed: Vec<_> = cs.into_iter().flat_map(|c| c.width.as_fixed()).collect();
        if fixed.len() == ncols {
            Ok(AsciiLayout::Delimited(DelimitedLayout { ncols }))
        } else if fixed.len() == ncols {
            let columns: Vec<_> = fixed
                .into_iter()
                .flat_map(|f| f.chars())
                .map(|chars| AsciiType { chars })
                .collect();
            if columns.len() == ncols {
                Ok(AsciiLayout::Fixed(FixedLayout { columns }))
            } else {
                Err("some columns greater than 20 chars wide".into())
            }
        } else {
            Err("mixed of variable and fixed column widths".into())
        }
    }

    fn ncols(&self) -> usize {
        match self {
            AsciiLayout::Delimited(a) => a.ncols,
            AsciiLayout::Fixed(l) => l.columns.len(),
        }
    }

    fn nbytes(&self, df: &FCSDataFrame, conf: &WriteConfig) -> usize {
        match self {
            AsciiLayout::Fixed(a) => a.nbytes(df.as_ref().height()),
            AsciiLayout::Delimited(_) => {
                let matrix = into_writable_matrix64(df, conf).data;
                let ndelim = df.as_ref().width() * df.as_ref().height() - 1;
                // TODO cast?
                let value_nbytes = matrix.map(|x| x.checked_ilog10().unwrap_or(1)).sum();
                // compute data length (delimiters + number of digits)
                value_nbytes as usize + ndelim
            }
        }
    }

    fn h_write<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        df: &FCSDataFrame,
        conf: &WriteConfig,
    ) -> ImpureResult<()> {
        match self {
            AsciiLayout::Fixed(a) => a.h_write(h, df, conf),
            AsciiLayout::Delimited(_) => into_writable_matrix64(df, conf)
                .try_map(|matrix| h_write_delimited_matrix(h, matrix)),
        }
    }

    fn into_reader(self, seg: Segment, kw_tot: Option<Tot>) -> PureMaybe<ColumnReader> {
        let nbytes = seg.nbytes() as usize;
        match self {
            AsciiLayout::Delimited(l) => PureSuccess::from(kw_tot.map(|tot| {
                ColumnReader::DelimitedAscii(DelimAsciiReader {
                    ncols: l.ncols,
                    // TODO wut???
                    nrows: Some(tot),
                    nbytes,
                })
            })),
            AsciiLayout::Fixed(fl) => fl
                .into_reader(seg, kw_tot)
                .map(|x| Some(ColumnReader::AlphaNum(x))),
        }
    }
}

impl FloatLayout {
    fn ncols(&self) -> usize {
        match self {
            FloatLayout::F32(l) => l.columns.len(),
            FloatLayout::F64(l) => l.columns.len(),
        }
    }

    fn into_reader(self, seg: Segment, kw_tot: Option<Tot>) -> PureSuccess<AlphaNumReader> {
        match self {
            FloatLayout::F32(l) => l.into_reader(seg, kw_tot),
            FloatLayout::F64(l) => l.into_reader(seg, kw_tot),
        }
    }

    fn nbytes(&self, nrows: usize) -> usize {
        match self {
            FloatLayout::F32(l) => l.nbytes(nrows),
            FloatLayout::F64(l) => l.nbytes(nrows),
        }
    }

    fn h_write<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        df: &FCSDataFrame,
        conf: &WriteConfig,
    ) -> ImpureResult<()> {
        match self {
            FloatLayout::F32(l) => l.h_write(h, df, conf),
            FloatLayout::F64(l) => l.h_write(h, df, conf),
        }
    }
}

impl VersionedDataLayout for DataLayout2_0 {
    type S = ByteOrd;
    type D = ();

    fn try_new(
        datatype: AlphaNumType,
        byteord: Self::S,
        columns: Vec<ColumnLayoutData<Self::D>>,
    ) -> Result<Self, Vec<String>> {
        match datatype {
            AlphaNumType::Ascii => AsciiLayout::try_new(columns)
                .map(DataLayout2_0::Ascii)
                .map_err(|e| vec![e]),
            AlphaNumType::Integer => {
                AnyUintLayout::try_new(columns, &byteord).map(DataLayout2_0::Integer)
            }
            AlphaNumType::Single => Float32Type::layout(columns, &byteord)
                .map(|x| DataLayout2_0::Float(FloatLayout::F32(x)))
                .map_err(|e| vec![e]),
            AlphaNumType::Double => Float64Type::layout(columns, &byteord)
                .map(|x| DataLayout2_0::Float(FloatLayout::F64(x)))
                .map_err(|e| vec![e]),
        }
    }

    fn try_new_from_raw(kws: &RawKeywords) -> Result<Self, Vec<String>> {
        let cs = Par::get_meta_req(kws).map_err(|e| vec![e]).and_then(|par| {
            let (pass, fail): (Vec<_>, Vec<_>) = (0..par.0)
                .map(|i| {
                    let w = Width::get_meas_req(kws, i.into());
                    let r = Range::get_meas_req(kws, i.into());
                    match (w, r) {
                        (Ok(width), Ok(range)) => Ok(ColumnLayoutData {
                            width,
                            range,
                            datatype: (),
                        }),
                        (x, y) => Err([x.err(), y.err()].into_iter().flatten().collect()),
                    }
                })
                .partition_result();
            if fail.is_empty() {
                Ok(pass)
            } else {
                Err(fail)
            }
        });
        let d = AlphaNumType::get_meta_req(kws);
        let b = ByteOrd::get_meta_req(kws);
        match (d, b, cs) {
            (Ok(datatype), Ok(byteord), Ok(columns)) => Self::try_new(datatype, byteord, columns),
            (x, y, zs) => Err([x.err(), y.err()]
                .into_iter()
                .flatten()
                .chain(zs.err().unwrap_or_default())
                .collect()),
        }
    }

    fn ncols(&self) -> usize {
        match self {
            DataLayout2_0::Ascii(a) => a.ncols(),
            DataLayout2_0::Integer(i) => i.ncols(),
            DataLayout2_0::Float(f) => f.ncols(),
        }
    }

    fn nbytes(&self, df: &FCSDataFrame, conf: &WriteConfig) -> usize {
        match self {
            DataLayout2_0::Ascii(a) => a.nbytes(df, conf),
            DataLayout2_0::Integer(i) => i.nbytes(df.as_ref().height()),
            DataLayout2_0::Float(f) => f.nbytes(df.as_ref().height()),
        }
    }

    fn h_write<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        df: &FCSDataFrame,
        conf: &WriteConfig,
    ) -> ImpureResult<()> {
        match self {
            DataLayout2_0::Ascii(a) => a.h_write(h, df, conf),
            DataLayout2_0::Integer(i) => i.h_write(h, df, conf),
            DataLayout2_0::Float(f) => f.h_write(h, df, conf),
        }
    }

    fn into_reader(self, kws: &RawKeywords, data_seg: Segment) -> PureMaybe<ColumnReader> {
        let res = Tot::get_meta_opt(kws).map(|tot| tot.0);
        let nbytes = data_seg.nbytes() as usize;
        PureMaybe::from_result_1(res, PureErrorLevel::Error)
            .map(|tot| tot.flatten())
            .and_then(|tot| match self {
                DataLayout2_0::Ascii(a) => match a {
                    AsciiLayout::Delimited(dl) => {
                        PureSuccess::from(ColumnReader::DelimitedAscii(DelimAsciiReader {
                            nbytes,
                            nrows: tot,
                            ncols: dl.ncols,
                        }))
                    }
                    AsciiLayout::Fixed(fl) => {
                        fl.into_reader(data_seg, tot).map(ColumnReader::AlphaNum)
                    }
                },
                DataLayout2_0::Integer(fl) => {
                    fl.into_reader(data_seg, tot).map(ColumnReader::AlphaNum)
                }
                DataLayout2_0::Float(fl) => {
                    fl.into_reader(data_seg, tot).map(ColumnReader::AlphaNum)
                }
            })
            .map(Some)
    }
}

impl VersionedDataLayout for DataLayout3_0 {
    type S = ByteOrd;
    type D = ();

    fn try_new(
        datatype: AlphaNumType,
        byteord: Self::S,
        columns: Vec<ColumnLayoutData<Self::D>>,
    ) -> Result<Self, Vec<String>> {
        match datatype {
            AlphaNumType::Ascii => AsciiLayout::try_new(columns)
                .map(DataLayout3_0::Ascii)
                .map_err(|e| vec![e]),
            AlphaNumType::Integer => {
                AnyUintLayout::try_new(columns, &byteord).map(DataLayout3_0::Integer)
            }
            AlphaNumType::Single => Float32Type::layout(columns, &byteord)
                .map(|x| DataLayout3_0::Float(FloatLayout::F32(x)))
                .map_err(|e| vec![e]),
            AlphaNumType::Double => Float64Type::layout(columns, &byteord)
                .map(|x| DataLayout3_0::Float(FloatLayout::F64(x)))
                .map_err(|e| vec![e]),
        }
    }

    // TODO very wetttt.....
    fn try_new_from_raw(kws: &RawKeywords) -> Result<Self, Vec<String>> {
        let cs = Par::get_meta_req(kws).map_err(|e| vec![e]).and_then(|par| {
            let (pass, fail): (Vec<_>, Vec<_>) = (0..par.0)
                .map(|i| {
                    let w = Width::get_meas_req(kws, i.into());
                    let r = Range::get_meas_req(kws, i.into());
                    match (w, r) {
                        (Ok(width), Ok(range)) => Ok(ColumnLayoutData {
                            width,
                            range,
                            datatype: (),
                        }),
                        (x, y) => Err([x.err(), y.err()].into_iter().flatten().collect()),
                    }
                })
                .partition_result();
            if fail.is_empty() {
                Ok(pass)
            } else {
                Err(fail)
            }
        });
        let d = AlphaNumType::get_meta_req(kws);
        let b = ByteOrd::get_meta_req(kws);
        match (d, b, cs) {
            (Ok(datatype), Ok(byteord), Ok(columns)) => Self::try_new(datatype, byteord, columns),
            (x, y, zs) => Err([x.err(), y.err()]
                .into_iter()
                .flatten()
                .chain(zs.err().unwrap_or_default())
                .collect()),
        }
    }

    fn ncols(&self) -> usize {
        match self {
            DataLayout3_0::Ascii(a) => a.ncols(),
            DataLayout3_0::Integer(i) => i.ncols(),
            DataLayout3_0::Float(f) => f.ncols(),
        }
    }

    fn nbytes(&self, df: &FCSDataFrame, conf: &WriteConfig) -> usize {
        match self {
            DataLayout3_0::Ascii(a) => a.nbytes(df, conf),
            DataLayout3_0::Integer(i) => i.nbytes(df.as_ref().height()),
            DataLayout3_0::Float(f) => f.nbytes(df.as_ref().height()),
        }
    }

    fn h_write<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        df: &FCSDataFrame,
        conf: &WriteConfig,
    ) -> ImpureResult<()> {
        match self {
            DataLayout3_0::Ascii(a) => a.h_write(h, df, conf),
            DataLayout3_0::Integer(i) => i.h_write(h, df, conf),
            DataLayout3_0::Float(f) => f.h_write(h, df, conf),
        }
    }

    fn into_reader(self, kws: &RawKeywords, data_seg: Segment) -> PureMaybe<ColumnReader> {
        let res = Tot::get_meta_req(kws);
        PureMaybe::from_result_1(res, PureErrorLevel::Error).and_then(|tot| match self {
            DataLayout3_0::Ascii(a) => a.into_reader(data_seg, tot),
            DataLayout3_0::Integer(fl) => fl
                .into_reader(data_seg, tot)
                .map(|x| Some(ColumnReader::AlphaNum(x))),
            DataLayout3_0::Float(fl) => fl
                .into_reader(data_seg, tot)
                .map(|x| Some(ColumnReader::AlphaNum(x))),
        })
    }
}

impl VersionedDataLayout for DataLayout3_1 {
    type S = Endian;
    type D = ();

    fn try_new(
        datatype: AlphaNumType,
        endian: Self::S,
        columns: Vec<ColumnLayoutData<Self::D>>,
    ) -> Result<Self, Vec<String>> {
        match datatype {
            AlphaNumType::Ascii => {
                let l = AsciiLayout::try_new(columns).map_err(|e| vec![e])?;
                Ok(DataLayout3_1::Ascii(l))
            }
            AlphaNumType::Integer => {
                let l = FixedLayout::try_new(columns, endian)?;
                Ok(DataLayout3_1::Integer(l))
            }
            AlphaNumType::Single => {
                let l = Float32Type::layout_endian(columns, endian)?;
                Ok(DataLayout3_1::Float(FloatLayout::F32(l)))
            }
            AlphaNumType::Double => {
                let l = Float64Type::layout_endian(columns, endian)?;
                Ok(DataLayout3_1::Float(FloatLayout::F64(l)))
            }
        }
    }

    fn try_new_from_raw(kws: &RawKeywords) -> Result<Self, Vec<String>> {
        let cs = Par::get_meta_req(kws).map_err(|e| vec![e]).and_then(|par| {
            let (pass, fail): (Vec<_>, Vec<_>) = (0..par.0)
                .map(|i| {
                    let w = Width::get_meas_req(kws, i.into());
                    let r = Range::get_meas_req(kws, i.into());
                    match (w, r) {
                        (Ok(width), Ok(range)) => Ok(ColumnLayoutData {
                            width,
                            range,
                            datatype: (),
                        }),
                        (x, y) => Err([x.err(), y.err()].into_iter().flatten().collect()),
                    }
                })
                .partition_result();
            if fail.is_empty() {
                Ok(pass)
            } else {
                Err(fail)
            }
        });
        let d = AlphaNumType::get_meta_req(kws);
        let e = Endian::get_meta_req(kws);
        match (d, e, cs) {
            (Ok(datatype), Ok(byteord), Ok(columns)) => Self::try_new(datatype, byteord, columns),
            (a, b, xs) => Err([a.err(), b.err()]
                .into_iter()
                .flatten()
                .chain(xs.err().unwrap_or_default())
                .collect()),
        }
    }

    fn ncols(&self) -> usize {
        match self {
            DataLayout3_1::Ascii(a) => a.ncols(),
            DataLayout3_1::Integer(i) => i.ncols(),
            DataLayout3_1::Float(f) => f.ncols(),
        }
    }

    fn nbytes(&self, df: &FCSDataFrame, conf: &WriteConfig) -> usize {
        match self {
            DataLayout3_1::Ascii(a) => a.nbytes(df, conf),
            DataLayout3_1::Integer(i) => i.nbytes(df.as_ref().height()),
            DataLayout3_1::Float(f) => f.nbytes(df.as_ref().height()),
        }
    }

    fn h_write<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        df: &FCSDataFrame,
        conf: &WriteConfig,
    ) -> ImpureResult<()> {
        match self {
            DataLayout3_1::Ascii(a) => a.h_write(h, df, conf),
            DataLayout3_1::Integer(i) => i.h_write(h, df, conf),
            DataLayout3_1::Float(f) => f.h_write(h, df, conf),
        }
    }

    fn into_reader(self, kws: &RawKeywords, data_seg: Segment) -> PureMaybe<ColumnReader> {
        let res = Tot::get_meta_req(kws);
        PureMaybe::from_result_1(res, PureErrorLevel::Error).and_then(|tot| match self {
            DataLayout3_1::Ascii(a) => a.into_reader(data_seg, tot),
            DataLayout3_1::Integer(fl) => fl
                .into_reader(data_seg, tot)
                .map(|x| Some(ColumnReader::AlphaNum(x))),
            DataLayout3_1::Float(fl) => fl
                .into_reader(data_seg, tot)
                .map(|x| Some(ColumnReader::AlphaNum(x))),
        })
    }
}

impl VersionedDataLayout for DataLayout3_2 {
    type S = Endian;
    type D = AlphaNumType;

    fn try_new(
        _: AlphaNumType,
        endian: Self::S,
        columns: Vec<ColumnLayoutData<Self::D>>,
    ) -> Result<Self, Vec<String>> {
        let unique_dt: Vec<_> = columns.iter().map(|c| c.datatype).unique().collect();
        match unique_dt[..] {
            [dt] => match dt {
                AlphaNumType::Ascii => {
                    let l = AsciiLayout::try_new(columns).map_err(|e| vec![e])?;
                    Ok(DataLayout3_2::Ascii(l))
                }
                AlphaNumType::Integer => {
                    let l = FixedLayout::try_new(columns, endian)?;
                    Ok(DataLayout3_2::Integer(l))
                }
                AlphaNumType::Single => {
                    let l = Float32Type::layout_endian(columns, endian)?;
                    Ok(DataLayout3_2::Float(FloatLayout::F32(l)))
                }
                AlphaNumType::Double => {
                    let l = Float64Type::layout_endian(columns, endian)?;
                    Ok(DataLayout3_2::Float(FloatLayout::F64(l)))
                }
            },
            _ => {
                let ncols = columns.len();
                let (pass, fail): (Vec<_>, Vec<_>) = columns
                    .into_iter()
                    .map(|c| MixedType::try_new(c.width, c.datatype, endian, &c.range))
                    .partition_result();
                if fail.is_empty() {
                    let cs: Vec<_> = pass.into_iter().flatten().collect();
                    if cs.len() == ncols {
                        Ok(DataLayout3_2::Mixed(FixedLayout { columns: cs }))
                    } else {
                        Err(vec![
                            "columns contain mix of variable and fixed widths".into()
                        ])
                    }
                } else {
                    Err(fail)
                }
            }
        }
    }

    fn try_new_from_raw(kws: &RawKeywords) -> Result<Self, Vec<String>> {
        let p = Par::get_meta_req(kws);
        let d = AlphaNumType::get_meta_req(kws);
        let (par, datatype) = match (p, d) {
            (Ok(par), Ok(datatype)) => Ok((par, datatype)),
            (x, y) => {
                let es: Vec<_> = [x.err(), y.err()].into_iter().flatten().collect();
                Err(es)
            }
        }?;
        let cs = {
            let (pass, fail): (Vec<_>, Vec<_>) = (0..par.0)
                .map(|i| {
                    let w = Width::get_meas_req(kws, i.into());
                    let r = Range::get_meas_req(kws, i.into());
                    let pnd = NumType::get_meas_opt(kws, i.into())
                        .map(|x| x.0.map(|y| y.into()).unwrap_or(datatype));
                    match (w, r, pnd) {
                        (Ok(width), Ok(range), Ok(pndatatype)) => Ok(ColumnLayoutData {
                            width,
                            range,
                            datatype: pndatatype,
                        }),
                        (x, y, z) => {
                            Err([x.err(), y.err(), z.err()].into_iter().flatten().collect())
                        }
                    }
                })
                .partition_result();
            if fail.is_empty() {
                Ok(pass)
            } else {
                Err(fail)
            }
        };
        let e = Endian::get_meta_req(kws);
        match (e, cs) {
            (Ok(endian), Ok(columns)) => Self::try_new(datatype, endian, columns),
            (a, xs) => Err([a.err()]
                .into_iter()
                .flatten()
                .chain(xs.err().unwrap_or_default())
                .collect()),
        }
    }

    fn ncols(&self) -> usize {
        match self {
            DataLayout3_2::Ascii(a) => a.ncols(),
            DataLayout3_2::Integer(i) => i.ncols(),
            DataLayout3_2::Float(f) => f.ncols(),
            DataLayout3_2::Mixed(m) => m.ncols(),
        }
    }

    fn nbytes(&self, df: &FCSDataFrame, conf: &WriteConfig) -> usize {
        match self {
            DataLayout3_2::Ascii(a) => a.nbytes(df, conf),
            DataLayout3_2::Integer(i) => i.nbytes(df.as_ref().height()),
            DataLayout3_2::Float(f) => f.nbytes(df.as_ref().height()),
            DataLayout3_2::Mixed(m) => m.nbytes(df.as_ref().height()),
        }
    }

    fn h_write<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        df: &FCSDataFrame,
        conf: &WriteConfig,
    ) -> ImpureResult<()> {
        match self {
            DataLayout3_2::Ascii(a) => a.h_write(h, df, conf),
            DataLayout3_2::Integer(i) => i.h_write(h, df, conf),
            DataLayout3_2::Float(f) => f.h_write(h, df, conf),
            DataLayout3_2::Mixed(m) => m.h_write(h, df, conf),
        }
    }

    fn into_reader(self, kws: &RawKeywords, data_seg: Segment) -> PureMaybe<ColumnReader> {
        let res = Tot::get_meta_req(kws);
        PureMaybe::from_result_1(res, PureErrorLevel::Error).and_then(|tot| match self {
            DataLayout3_2::Ascii(a) => a.into_reader(data_seg, tot),
            DataLayout3_2::Integer(fl) => fl
                .into_reader(data_seg, tot)
                .map(|x| Some(ColumnReader::AlphaNum(x))),
            DataLayout3_2::Float(fl) => fl
                .into_reader(data_seg, tot)
                .map(|x| Some(ColumnReader::AlphaNum(x))),
            DataLayout3_2::Mixed(fl) => fl
                .into_reader(data_seg, tot)
                .map(|x| Some(ColumnReader::AlphaNum(x))),
        })
    }
}

fn h_write_ascii_int<W: Write>(h: &mut BufWriter<W>, chars: Chars, x: u64) -> io::Result<()> {
    let s = x.to_string();
    // ASSUME bytes has been ensured to be able to hold the largest digit
    // in this column, which means this will never be negative
    let w = u8::from(chars);
    let offset = usize::from(w) - s.len();
    let mut buf: Vec<u8> = vec![0, w];
    for (i, c) in s.bytes().enumerate() {
        buf[offset + i] = c;
    }
    h.write_all(&buf)
}

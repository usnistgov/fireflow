use crate::config::{ReaderConfig, SharedConfig, WriteConfig};
use crate::core::*;
use crate::error::*;
use crate::macros::{enum_from, enum_from_disp, match_many_to_one, newtype_disp, newtype_from};
use crate::segment::*;
use crate::text::byteord::*;
use crate::text::float_or_int::*;
use crate::text::keywords::*;
use crate::validated::dataframe::*;
use crate::validated::nonstandard::*;
use crate::validated::standard::*;

use itertools::repeat_n;
use itertools::Itertools;
use nonempty::NonEmpty;
use std::convert::Infallible;
use std::fmt;
use std::io;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::num::ParseIntError;
use std::str;
use std::str::FromStr;

pub struct AnalysisReader {
    pub seg: AnyAnalysisSegment,
}

impl AnalysisReader {
    pub(crate) fn h_read<R: Read + Seek>(&self, h: &mut BufReader<R>) -> io::Result<Analysis> {
        let mut buf = vec![];
        h.seek(SeekFrom::Start(u64::from(self.seg.inner.begin())))?;
        h.take(u64::from(self.seg.inner.len()))
            .read_to_end(&mut buf)?;
        Ok(buf.into())
    }
}

pub trait VersionedDataLayout: Sized {
    type S;
    type D;

    fn try_new(
        dt: AlphaNumType,
        size: Self::S,
        cs: Vec<ColumnLayoutData<Self::D>>,
        conf: &SharedConfig,
    ) -> DeferredResult<Self, NewDataLayoutWarning, NewDataLayoutError>;

    fn try_new_from_raw(kws: &StdKeywords, conf: &SharedConfig) -> FromRawResult<Self>;

    fn ncols(&self) -> usize;

    fn into_data_reader(
        self,
        kws: &mut StdKeywords,
        seg: HeaderDataSegment,
        conf: &ReaderConfig,
    ) -> DataReaderResult<DataReader>;

    fn into_data_reader_raw(
        self,
        kws: &StdKeywords,
        seg: HeaderDataSegment,
        conf: &ReaderConfig,
    ) -> DataReaderResult<DataReader>;

    fn as_analysis_reader(
        kws: &mut StdKeywords,
        seg: HeaderAnalysisSegment,
        conf: &ReaderConfig,
    ) -> AnalysisReaderResult<AnalysisReader>;

    fn as_analysis_reader_raw(
        kws: &StdKeywords,
        seg: HeaderAnalysisSegment,
        conf: &ReaderConfig,
    ) -> AnalysisReaderResult<AnalysisReader>;

    fn as_writer<'a>(
        &self,
        df: &'a FCSDataFrame,
        conf: &WriteConfig,
    ) -> MultiResult<DataWriter<'a>, ColumnWriterError> {
        // The dataframe should be encapsulated such that a) the column number
        // matches the number of measurements. If these are not true, the code
        // is wrong.
        let par = self.ncols();
        let ncols = df.ncols();
        if ncols != par {
            panic!("datafame columns ({ncols}) unequal to number of measurements ({par})");
        }
        self.as_writer_inner(df, conf)
    }

    fn as_writer_inner<'a>(
        &self,
        df: &'a FCSDataFrame,
        conf: &WriteConfig,
    ) -> MultiResult<DataWriter<'a>, ColumnWriterError>;
}

pub enum DataLayout2_0 {
    Ascii(AsciiLayout),
    Integer(AnyUintLayout),
    Float(FloatLayout),
    Empty,
}

pub enum DataLayout3_0 {
    Ascii(AsciiLayout),
    Integer(AnyUintLayout),
    Float(FloatLayout),
    Empty,
}

pub enum DataLayout3_1 {
    Ascii(AsciiLayout),
    Integer(FixedLayout<AnyUintType>),
    Float(FloatLayout),
    Empty,
}

pub enum DataLayout3_2 {
    Ascii(AsciiLayout),
    Integer(FixedLayout<AnyUintType>),
    Float(FloatLayout),
    Mixed(FixedLayout<MixedType>),
    Empty,
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
    pub columns: NonEmpty<C>,
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
type F32Type = FloatType<f32, 4>;

/// An f64 column
type F64Type = FloatType<f64, 8>;

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
pub struct FloatType<T, const LEN: usize> {
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

impl AnyUintType {
    fn try_new(
        w: Width,
        r: Range,
        n: Endian,
        notrunc: bool,
    ) -> DeferredResult<Self, BitmaskError, NewUintTypeError> {
        w.try_into()
            .into_deferred()
            .def_and_tentatively(|bytes: Bytes| {
                // ASSUME this can only be 1-8
                match u8::from(bytes) {
                    1 => u8::column_type_endian(r, n, notrunc).map(Self::Uint08),
                    2 => u16::column_type_endian(r, n, notrunc).map(Self::Uint16),
                    3 => u32::column_type_endian(r, n, notrunc).map(Self::Uint24),
                    4 => u32::column_type_endian(r, n, notrunc).map(Self::Uint32),
                    5 => u64::column_type_endian(r, n, notrunc).map(Self::Uint40),
                    6 => u64::column_type_endian(r, n, notrunc).map(Self::Uint48),
                    7 => u64::column_type_endian(r, n, notrunc).map(Self::Uint56),
                    8 => u64::column_type_endian(r, n, notrunc).map(Self::Uint64),
                    _ => unreachable!(),
                }
                .errors_into()
            })
    }
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
    pub byteord: SizedByteOrd<LEN>,
}

/// Instructions for writing measurements to a file.
///
/// This structure can be used with all FCS versions, as each column is treated
/// as it's own separate type. This is in contrast to some FCS versions where
/// we could think of the DATA as a matrix of one uniform type.
///
/// This doesn't store data, but rather stores an iterator pointing to a column
/// which is then called with 'next()' for each row/event. The dataframe is
/// validated to store only u8-64 and f32/64, which are then "coerced" to
/// whatever type the measurement column requires. This might cause data loss,
/// in which case the user will be warned (for instance, a f32 column might be
/// stored as a u32 on disk, which will likely cause loss of precision and/or
/// truncation).
///
/// We could be more draconian about ensuring the column type matches the
/// measurement type, but this would complicate many other operations such as
/// adding/removing columns or changing a measurement type/size/range. The price
/// to pay with this approach is that each combination of to/from types needs to
/// be enumerated (6 and 11 types respectively).
pub enum DataWriter<'a> {
    Delim(DelimWriter<'a>),
    Fixed(FixedWriter<'a>),
    Empty,
}

pub type DelimWriter<'a> = DataWriterInner<AnyDelimColumnWriter<'a>>;
pub type FixedWriter<'a> = DataWriterInner<AnyFixedColumnWriter<'a>>;

pub struct DataWriterInner<C> {
    columns: NonEmpty<C>,
    nrows: usize,
    nbytes: usize,
}

/// Writer for ASCII delimited layout.
///
/// Each variant represents a type in the original column, which is necessary
/// due to the use of lazy iterators for each type the original column may hold.
///
/// The type within the variant will only convert to a u64 without any size
/// information.
pub enum AnyDelimColumnWriter<'a> {
    FromU08(DelimColumnWriter<'a, u8>),
    FromU16(DelimColumnWriter<'a, u16>),
    FromU32(DelimColumnWriter<'a, u32>),
    FromU64(DelimColumnWriter<'a, u64>),
    FromF32(DelimColumnWriter<'a, f32>),
    FromF64(DelimColumnWriter<'a, f64>),
}

/// Writer for any fixed numeric/ascii layout
///
/// Each variant represents a type in the original column, which is necessary
/// due to the use of lazy iterators for each type the original column may hold.
///
/// The type within each variant can hold any target representation for a
/// measurement, which includes type and size information.
pub enum AnyFixedColumnWriter<'a> {
    FromU08(AnyColumnWriter<'a, u8>),
    FromU16(AnyColumnWriter<'a, u16>),
    FromU32(AnyColumnWriter<'a, u32>),
    FromU64(AnyColumnWriter<'a, u64>),
    FromF32(AnyColumnWriter<'a, f32>),
    FromF64(AnyColumnWriter<'a, f64>),
}

/// Writer for one column.
///
/// This encodes the target type for encoding any FCS measurement, which may be
/// integers of varying sizes, floats, doubles, or a string of numbers. Each
/// type has its own size information. The generic type encodes the source type
/// within the polars column from which the written type will be converted,
/// possibly with data loss.
pub enum AnyColumnWriter<'a, X> {
    U08(IntColumnWriter<'a, X, u8, 1>),
    U16(IntColumnWriter<'a, X, u16, 2>),
    U24(IntColumnWriter<'a, X, u32, 3>),
    U32(IntColumnWriter<'a, X, u32, 4>),
    U40(IntColumnWriter<'a, X, u64, 5>),
    U48(IntColumnWriter<'a, X, u64, 6>),
    U56(IntColumnWriter<'a, X, u64, 7>),
    U64(IntColumnWriter<'a, X, u64, 8>),
    F32(FloatColumnWriter<'a, X, f32, 4>),
    F64(FloatColumnWriter<'a, X, f64, 8>),
    Ascii(AsciiColumnWriter<'a, X>),
}

pub type IntColumnWriter<'a, X, T, const LEN: usize> = ColumnWriter<'a, X, T, UintType<T, LEN>>;

pub type FloatColumnWriter<'a, X, T, const LEN: usize> = ColumnWriter<'a, X, T, SizedByteOrd<LEN>>;

pub type AsciiColumnWriter<'a, X> = ColumnWriter<'a, X, u64, Chars>;

pub type DelimColumnWriter<'a, X> = ColumnWriter<'a, X, u64, ()>;

pub struct ColumnWriter<'a, X, Y, S> {
    pub(crate) data: FCSColIter<'a, X, Y>,
    pub(crate) size: S,
}

impl DataWriter<'_> {
    pub(crate) fn h_write<W: Write>(&mut self, h: &mut BufWriter<W>) -> io::Result<()> {
        match self {
            Self::Delim(d) => d.h_write(h),
            Self::Fixed(f) => f.h_write(h),
            Self::Empty => Ok(()),
        }
    }

    pub(crate) fn nbytes(&self) -> usize {
        match self {
            Self::Delim(d) => d.nbytes,
            Self::Fixed(f) => f.nbytes,
            Self::Empty => 0,
        }
    }
}

impl<C> DataWriterInner<C> {
    fn try_new(cs: Vec<C>, nrows: usize, nbytes: usize) -> Option<Self> {
        NonEmpty::from_vec(cs).map(|columns| Self {
            columns,
            nrows,
            nbytes,
        })
    }
}

impl DelimWriter<'_> {
    fn h_write<W: Write>(&mut self, h: &mut BufWriter<W>) -> io::Result<()> {
        let ncols = self.columns.len();
        let nrows = self.nrows;
        for i in 0..nrows {
            for (j, c) in self.columns.iter_mut().enumerate() {
                c.h_write(h)?;
                // write delimiter after all but last value
                if !(i == nrows - 1 && j == ncols - 1) {
                    h.write_all(&[32])?; // 32 = space in ASCII
                }
            }
        }
        Ok(())
    }
}

impl FixedWriter<'_> {
    fn h_write<W: Write>(&mut self, h: &mut BufWriter<W>) -> io::Result<()> {
        for _ in 0..self.nrows {
            for c in self.columns.iter_mut() {
                c.h_write(h)?;
            }
        }
        Ok(())
    }
}

impl AnyDelimColumnWriter<'_> {
    fn h_write<W: Write>(&mut self, h: &mut BufWriter<W>) -> io::Result<()> {
        match_many_to_one!(
            self,
            AnyDelimColumnWriter,
            [FromU08, FromU16, FromU32, FromU64, FromF32, FromF64],
            c,
            { c.h_write_delim_ascii(h) }
        )
    }
}

impl AnyFixedColumnWriter<'_> {
    fn h_write<W: Write>(&mut self, h: &mut BufWriter<W>) -> io::Result<()> {
        match_many_to_one!(
            self,
            AnyFixedColumnWriter,
            [FromU08, FromU16, FromU32, FromU64, FromF32, FromF64],
            c,
            { c.h_write(h) }
        )
    }
}

impl<X> AnyColumnWriter<'_, X> {
    fn h_write<W: Write>(&mut self, h: &mut BufWriter<W>) -> io::Result<()>
    where
        X: Copy,
    {
        match self {
            AnyColumnWriter::U08(c) => c.h_write_int(h),
            AnyColumnWriter::U16(c) => c.h_write_int(h),
            AnyColumnWriter::U24(c) => c.h_write_int(h),
            AnyColumnWriter::U32(c) => c.h_write_int(h),
            AnyColumnWriter::U40(c) => c.h_write_int(h),
            AnyColumnWriter::U48(c) => c.h_write_int(h),
            AnyColumnWriter::U56(c) => c.h_write_int(h),
            AnyColumnWriter::U64(c) => c.h_write_int(h),
            AnyColumnWriter::F32(c) => c.h_write_float(h),
            AnyColumnWriter::F64(c) => c.h_write_float(h),
            AnyColumnWriter::Ascii(c) => c.h_write_ascii(h),
        }
    }
}

impl<X, Y, const INTLEN: usize> IntColumnWriter<'_, X, Y, INTLEN> {
    fn h_write_int<W: Write, const DTLEN: usize>(&mut self, h: &mut BufWriter<W>) -> io::Result<()>
    where
        X: Copy,
        Y: IntFromBytes<DTLEN, INTLEN>,
        <Y as FromStr>::Err: fmt::Display,
        // <Y as FromStr>::Err: IntErr,
        Y: Ord,
    {
        let x = self.data.next().unwrap();
        x.new
            .min(self.size.bitmask)
            .h_write_int(h, &self.size.byteord)
    }
}

impl<X, Y, const DTLEN: usize> FloatColumnWriter<'_, X, Y, DTLEN> {
    fn h_write_float<W: Write>(&mut self, h: &mut BufWriter<W>) -> io::Result<()>
    where
        X: Copy,
        Y: FloatFromBytes<DTLEN>,
        <Y as FromStr>::Err: fmt::Display,
    {
        self.data.next().unwrap().new.h_write_float(h, &self.size)
    }
}

impl<X> AsciiColumnWriter<'_, X> {
    fn h_write_ascii<W: Write>(&mut self, h: &mut BufWriter<W>) -> io::Result<()>
    where
        X: Copy,
    {
        let x = self.data.next().unwrap();
        let s = x.new.to_string();
        let w: usize = u8::from(self.size).into();
        if s.len() > w {
            // if string is greater than allocated chars, only write a fraction
            // starting from the left
            let offset = s.len() - w;
            h.write_all(&s.as_bytes()[offset..])
        } else {
            // if string less than allocated chars, pad left side with zero before
            // writing number
            for _ in 0..(w - s.len()) {
                h.write_all(&[30])?;
            }
            h.write_all(s.as_bytes())
        }
    }
}

impl<X> DelimColumnWriter<'_, X> {
    fn h_write_delim_ascii<W: Write>(&mut self, h: &mut BufWriter<W>) -> io::Result<()>
    where
        X: Copy,
    {
        let x = self.data.next().unwrap();
        let s = x.new.to_string();
        let buf = s.as_bytes();
        h.write_all(buf)
    }
}

/// Instructions and buffers to read the DATA segment
pub struct DataReader {
    pub column_reader: ColumnReader,
    pub seg: AnyDataSegment,
}

/// Instructions to read one column in the DATA segment.
///
/// Each "column" contains a vector to hold the numbers read from DATA. In all
/// but the case of delimited ASCII, this is pre-allocated with the number of
/// rows to make reading faster. Each column has other information necessary to
/// read the column (bitmask, width, etc).
// TODO add method to check the $TOT field in case this is available
pub enum ColumnReader {
    DelimitedAsciiNoRows(DelimAsciiReaderNoRows),
    DelimitedAscii(DelimAsciiReader),
    AlphaNum(AlphaNumReader),
    Empty,
}

// The only difference b/t these two is that the no-rows version will be
// initialized with zero-length vectors, and the rows version will be
// initialized with row-length vectors. The only purpose of the former is the
// deal with the case in 2.0 where $TOT isn't given
pub struct DelimAsciiReaderNoRows(DelimAsciiReaderInner);
pub struct DelimAsciiReader(DelimAsciiReaderInner);

pub struct DelimAsciiReaderInner {
    pub columns: NonEmpty<Vec<u64>>,
    pub nbytes: usize,
}

pub struct AlphaNumReader {
    pub columns: NonEmpty<AlphaNumColumnReader>,
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

impl DataReader {
    pub(crate) fn h_read<R>(self, h: &mut BufReader<R>) -> IOResult<FCSDataFrame, ReadDataError>
    where
        R: Read + Seek,
    {
        h.seek(SeekFrom::Start(self.seg.inner.begin().into()))?;
        match self.column_reader {
            ColumnReader::DelimitedAscii(p) => p.h_read(h).map_err(|e| e.inner_into()),
            ColumnReader::DelimitedAsciiNoRows(p) => p.h_read(h).map_err(|e| e.inner_into()),
            ColumnReader::AlphaNum(p) => p.h_read(h).map_err(|e| e.inner_into()),
            ColumnReader::Empty => Ok(FCSDataFrame::default()),
        }
    }
}

impl ColumnReader {
    fn into_data_reader(self, seg: AnyDataSegment) -> DataReader {
        DataReader {
            column_reader: self,
            seg,
        }
    }
}

impl FloatReader {
    fn h_read<R: Read>(&mut self, h: &mut BufReader<R>, r: usize) -> io::Result<()> {
        match self {
            Self::F32(t) => t.h_read(h, r),
            Self::F64(t) => t.h_read(h, r),
        }
    }

    fn into_fcs_column(self) -> AnyFCSColumn {
        match self {
            Self::F32(x) => F32Column::from(x.column).into(),
            Self::F64(x) => F64Column::from(x.column).into(),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::F32(x) => x.column.len(),
            Self::F64(x) => x.column.len(),
        }
    }
}

impl DelimAsciiReader {
    fn h_read<R: Read>(self, h: &mut BufReader<R>) -> IOResult<FCSDataFrame, ReadDelimAsciiError> {
        // FCS 2.0 files have an optional $TOT field, which complicates this a
        // bit. If in this case we have $TOT so the columns have been
        // initialized to the number of rows.
        let mut buf = Vec::new();
        let mut last_was_delim = false;
        let mut data = self.0.columns;
        let nrows = data.head.len();
        let ncols = data.len();
        let mut row = 0;
        let mut col = 0;
        // Delimiters are tab, newline, carriage return, space, or comma. Any
        // consecutive delimiter counts as one, and delimiters can be mixed.
        for b in h.bytes().take(self.0.nbytes) {
            let byte = b?;
            // exit if we encounter more rows than expected.
            if row == nrows {
                let e = ReadDelimAsciiError::RowsExceeded(RowsExceededError(nrows));
                return Err(ImpureError::Pure(e));
            }
            if is_ascii_delim(byte) {
                if !last_was_delim {
                    last_was_delim = true;
                    data[col][row] = ascii_to_uint(&buf)
                        .map_err(ReadDelimAsciiError::Parse)
                        .map_err(ImpureError::Pure)?;
                    buf.clear();
                    if col == ncols - 1 {
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
            let e = DelimIncompleteError { col, row, nrows };
            return Err(ImpureError::Pure(ReadDelimAsciiError::Incomplete(e)));
        }
        // The spec isn't clear if the last value should be a delim or
        // not, so flush the buffer if it has anything in it since we
        // only try to parse if we hit a delim above.
        if !buf.is_empty() {
            data[col][row] = ascii_to_uint(&buf)
                .map_err(ReadDelimAsciiError::Parse)
                .map_err(ImpureError::Pure)?;
        }
        let cs: Vec<_> = data
            .into_iter()
            .map(FCSColumn::from)
            .map(AnyFCSColumn::from)
            .collect();
        // ASSUME this will never fail because all columns should be the same
        // length
        Ok(FCSDataFrame::try_new(cs).unwrap())
    }
}

fn is_ascii_delim(x: u8) -> bool {
    // tab, newline, carriage return, space, or comma
    x == 9 || x == 10 || x == 13 || x == 32 || x == 44
}

impl DelimAsciiReaderNoRows {
    fn h_read<R: Read>(
        self,
        h: &mut BufReader<R>,
    ) -> IOResult<FCSDataFrame, ReadDelimAsciiNoRowsError> {
        let mut buf = Vec::new();
        let mut data = self.0.columns;
        let ncols = data.len();
        let mut col = 0;
        let mut last_was_delim = false;
        let go = |_data: &mut NonEmpty<Vec<u64>>, _col, _buf: &[u8]| {
            ascii_to_uint(_buf)
                .map_err(ReadDelimAsciiNoRowsError::Parse)
                .map_err(ImpureError::Pure)
                .map(|x| _data[_col].push(x))
        };
        // Delimiters are tab, newline, carriage return, space, or comma. Any
        // consecutive delimiter counts as one, and delimiters can be mixed.
        // If we don't know the number of rows, the only choice is to push onto
        // the column vectors one at a time. This leads to the possibility that
        // the vectors may not be the same length in the end, in which case,
        // scream loudly and bail.
        for b in h.bytes().take(self.0.nbytes) {
            let byte = b?;
            if is_ascii_delim(byte) {
                if !last_was_delim {
                    last_was_delim = true;
                    buf.clear();
                    go(&mut data, col, &buf)?;
                    if col == ncols - 1 {
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
            return Err(ImpureError::Pure(ReadDelimAsciiNoRowsError::Unequal));
        }
        // The spec isn't clear if the last value should be a delim or
        // not, so flush the buffer if it has anything in it since we
        // only try to parse if we hit a delim above.
        if !buf.is_empty() {
            go(&mut data, col, &buf)?;
        }
        let cs: Vec<_> = data
            .into_iter()
            .map(FCSColumn::from)
            .map(AnyFCSColumn::from)
            .collect();
        // ASSUME this will never fail because all columns should be the same
        // length
        Ok(FCSDataFrame::try_new(cs).unwrap())
    }
}

impl AlphaNumReader {
    fn h_read<R: Read>(mut self, h: &mut BufReader<R>) -> IOResult<FCSDataFrame, AsciiToUintError> {
        let mut buf: Vec<u8> = vec![];
        let nrows = self.columns.head.len();
        for r in 0..nrows {
            for c in self.columns.iter_mut() {
                match c {
                    AlphaNumColumnReader::Float(f) => f.h_read(h, r)?,
                    AlphaNumColumnReader::Uint(u) => u.h_read(h, r)?,
                    AlphaNumColumnReader::Ascii(d) => {
                        buf.clear();
                        h.take(u8::from(d.width).into()).read_to_end(&mut buf)?;
                        d.column[r] = ascii_to_uint(&buf).map_err(ImpureError::Pure)?;
                    }
                }
            }
        }
        let cs: Vec<_> = self
            .columns
            .into_iter()
            .map(|c| c.into_fcs_column())
            .collect();
        Ok(FCSDataFrame::try_new(cs).unwrap())
    }

    fn check_tot(
        &self,
        tot: Tot,
        enforce: bool,
    ) -> Tentative<(), TotEventMismatch, TotEventMismatch> {
        let total_events = self.columns.head.len();
        if tot.0 != total_events {
            let i = TotEventMismatch { tot, total_events };
            Tentative::new_either((), vec![i], enforce)
        } else {
            Tentative::new1(())
        }
    }
}

impl FixedLayout<AnyUintType> {
    pub(crate) fn try_new<D>(
        cs: Vec<ColumnLayoutData<D>>,
        e: Endian,
        notrunc: bool,
    ) -> DeferredResult<Option<Self>, UintColumnWarning, UintColumnError> {
        cs.into_iter()
            .enumerate()
            .map(|(i, c)| {
                AnyUintType::try_new(c.width, c.range, e, notrunc)
                    .def_map_errors(|error| {
                        ColumnError {
                            error,
                            index: i.into(),
                        }
                        .into()
                    })
                    .def_map_warnings(|error| {
                        ColumnError {
                            error,
                            index: i.into(),
                        }
                        .into()
                    })
            })
            .gather()
            .map_err(DeferredFailure::mconcat)
            .map(Tentative::mconcat)
            .def_map_value(FixedLayout::from_vec)
    }
}

trait IntMath: Sized
where
    Self: fmt::Display,
    Self: FromStr,
{
    fn next_bitmask(x: Self) -> Self;
}

// TODO clean this up with https://github.com/rust-lang/rust/issues/76560 once
// it lands in a stable compiler, in theory there is no reason to put the length
// of the type as a parameter, but the current compiler is not smart enough
trait NumProps<const DTLEN: usize>: Sized + Copy + Default {
    fn from_big(buf: [u8; DTLEN]) -> Self;

    fn from_little(buf: [u8; DTLEN]) -> Self;

    fn to_big(self) -> [u8; DTLEN];

    fn to_little(self) -> [u8; DTLEN];

    fn maxval() -> Self;
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
        self,
        h: &mut BufWriter<W>,
        order: &[u8; OLEN],
    ) -> io::Result<()> {
        let tmp = Self::to_little(self);
        let mut buf = [0; OLEN];
        for (i, j) in order.iter().enumerate() {
            buf[usize::from(*j)] = tmp[i];
        }
        h.write_all(&tmp)
    }
}

trait IntFromBytes<const DTLEN: usize, const INTLEN: usize>
where
    Self: OrderedFromBytes<DTLEN, INTLEN>,
    Self: TryFrom<FloatOrInt, Error = ToIntError<Self>>,
    Self: IntMath,
    <Self as FromStr>::Err: fmt::Display,
{
    fn range_to_bitmask(r: Range, notrunc: bool) -> Tentative<Self, BitmaskError, BitmaskError> {
        let go = |x, e| {
            let y = Self::next_bitmask(x);
            if notrunc {
                Tentative::new(y, vec![], vec![e])
            } else {
                Tentative::new(y, vec![e], vec![])
            }
        };
        r.0.try_into().map_or_else(
            |e| match e {
                ToIntError::IntOverrange(x) => go(Self::maxval(), BitmaskError::IntOverrange(x)),
                ToIntError::FloatOverrange(x) => {
                    go(Self::maxval(), BitmaskError::FloatOverrange(x))
                }
                ToIntError::FloatUnderrange(x) => {
                    go(Self::default(), BitmaskError::FloatUnderrange(x))
                }
                ToIntError::FloatPrecisionLoss(x, y) => go(y, BitmaskError::FloatPrecisionLoss(x)),
            },
            Tentative::new1,
        )
    }

    fn column_type_endian(
        r: Range,
        e: Endian,
        notrunc: bool,
    ) -> Tentative<UintType<Self, INTLEN>, BitmaskError, BitmaskError> {
        // TODO be more specific, which means we need the measurement index
        Self::range_to_bitmask(r, notrunc).map(|bitmask| UintType {
            bitmask,
            byteord: e.into(),
        })
    }

    fn column_type_ordered(
        r: Range,
        o: &ByteOrd,
        notrunc: bool,
    ) -> DeferredResult<UintType<Self, INTLEN>, BitmaskError, IntOrderedColumnError> {
        // TODO be more specific, which means we need the measurement index
        Self::range_to_bitmask(r, notrunc)
            .errors_into()
            .and_maybe(|bitmask| {
                o.as_sized()
                    .map(|size| UintType {
                        bitmask,
                        byteord: size,
                    })
                    .into_deferred()
            })
    }

    fn layout_ordered(
        rs: Vec<Range>,
        byteord: &ByteOrd,
        notrunc: bool,
    ) -> DeferredResult<
        Option<FixedLayout<UintType<Self, INTLEN>>>,
        BitmaskError,
        IntOrderedColumnError,
    > {
        // TODO add index to bitmask warning output
        rs.into_iter()
            .map(|r| Self::column_type_ordered(r, byteord, notrunc))
            .gather()
            .map_err(DeferredFailure::mconcat)
            .map(Tentative::mconcat)
            .def_map_value(FixedLayout::from_vec)
    }

    fn h_read_int<R: Read>(
        h: &mut BufReader<R>,
        byteord: &SizedByteOrd<INTLEN>,
    ) -> io::Result<Self> {
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
                    Self::from_big(buf)
                } else {
                    buf[..INTLEN].copy_from_slice(&tmp[..INTLEN]);
                    Self::from_little(buf)
                })
            }
            SizedByteOrd::Order(order) => Self::h_read_from_ordered(h, order),
        }
    }

    fn h_write_int<W: Write>(
        self,
        h: &mut BufWriter<W>,
        byteord: &SizedByteOrd<INTLEN>,
    ) -> io::Result<()> {
        match byteord {
            SizedByteOrd::Endian(e) => {
                let mut buf = [0; INTLEN];
                let (start, end, tmp) = if *e == Endian::Big {
                    ((DTLEN - INTLEN), DTLEN, Self::to_big(self))
                } else {
                    (0, INTLEN, Self::to_little(self))
                };
                buf[..].copy_from_slice(&tmp[start..end]);
                h.write_all(&buf)
            }
            SizedByteOrd::Order(order) => self.h_write_from_ordered(h, order),
        }
    }
}

trait FloatFromBytes<const LEN: usize>
where
    Self: NumProps<LEN>,
    Self: OrderedFromBytes<LEN, LEN>,
    Self: FromStr,
    Self: TryFrom<FloatOrInt, Error = ToFloatError<Self>>,
    <Self as FromStr>::Err: fmt::Display,
    Self: Clone,
{
    fn range(r: Range) -> Self {
        // TODO control how this works and/or warn user if we truncate
        r.0.try_into().unwrap_or_else(|e| match e {
            ToFloatError::IntPrecisionLoss(_, x) => x,
            ToFloatError::FloatOverrange(_) => Self::maxval(),
            ToFloatError::FloatUnderrange(_) => Self::default(),
        })
    }

    fn column_type_endian(
        w: Width,
        n: Endian,
        r: Range,
    ) -> Result<FloatType<Self, LEN>, FloatWidthError> {
        Bytes::try_from(w).map_err(|e| e.into()).and_then(|bytes| {
            if usize::from(u8::from(bytes)) == LEN {
                let range = Self::range(r);
                Ok(FloatType {
                    order: n.into(),
                    range,
                })
            } else {
                Err(FloatWidthError::WrongWidth(WrongFloatWidth {
                    expected: LEN,
                    width: bytes,
                }))
            }
        })
    }

    fn column_type_ordered(
        w: Width,
        o: &ByteOrd,
        r: Range,
    ) -> Result<FloatType<Self, LEN>, OrderedFloatError> {
        Bytes::try_from(w)
            .map_err(|e| e.into())
            .map_err(OrderedFloatError::WrongWidth)
            .and_then(|bytes| {
                if usize::from(u8::from(bytes)) == LEN {
                    let range = Self::range(r);
                    o.as_sized()
                        .map(|order| FloatType { order, range })
                        .map_err(OrderedFloatError::Order)
                } else {
                    Err(FloatWidthError::WrongWidth(WrongFloatWidth {
                        expected: LEN,
                        width: bytes,
                    })
                    .into())
                }
            })
    }

    fn layout_endian<D>(
        cs: Vec<ColumnLayoutData<D>>,
        endian: Endian,
    ) -> MultiResult<Option<FixedLayout<FloatType<Self, LEN>>>, EndianFloatColumnError> {
        cs.into_iter()
            .enumerate()
            .map(|(i, c)| {
                Self::column_type_endian(c.width, endian, c.range).map_err(|error| ColumnError {
                    error,
                    index: i.into(),
                })
            })
            .gather()
            .map_err(|es| es.map(EndianFloatColumnError))
            .map(FixedLayout::from_vec)
    }

    fn layout<D>(
        cs: Vec<ColumnLayoutData<D>>,
        byteord: &ByteOrd,
    ) -> MultiResult<Option<FixedLayout<FloatType<Self, LEN>>>, OrderedFloatColumnError> {
        cs.into_iter()
            .enumerate()
            .map(|(i, c)| {
                Self::column_type_ordered(c.width, byteord, c.range).map_err(|error| ColumnError {
                    error,
                    index: i.into(),
                })
            })
            .gather()
            .map_err(|es| es.map(OrderedFloatColumnError))
            .map(FixedLayout::from_vec)
    }

    fn h_read_float<R: Read>(
        h: &mut BufReader<R>,
        byteord: &SizedByteOrd<LEN>,
    ) -> io::Result<Self> {
        match byteord {
            SizedByteOrd::Endian(e) => {
                let mut buf = [0; LEN];
                h.read_exact(&mut buf)?;
                Ok(if *e == Endian::Big {
                    Self::from_big(buf)
                } else {
                    Self::from_little(buf)
                })
            }
            SizedByteOrd::Order(order) => Self::h_read_from_ordered(h, order),
        }
    }

    fn h_write_float<W: Write>(
        self,
        h: &mut BufWriter<W>,
        byteord: &SizedByteOrd<LEN>,
    ) -> io::Result<()> {
        match byteord {
            SizedByteOrd::Endian(e) => {
                let buf: [u8; LEN] = if *e == Endian::Big {
                    Self::to_big(self)
                } else {
                    Self::to_little(self)
                };
                h.write_all(&buf)
            }
            SizedByteOrd::Order(order) => self.h_write_from_ordered(h, order),
        }
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

            fn maxval() -> Self {
                Self::MAX
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
            fn next_bitmask(x: Self) -> Self {
                Self::checked_next_power_of_two(x)
                    .map(|x| x - 1)
                    .unwrap_or(Self::MAX)
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

impl AlphaNumColumnReader {
    fn into_fcs_column(self) -> AnyFCSColumn {
        match self {
            Self::Ascii(x) => U64Column::from(x.column).into(),
            Self::Float(x) => x.into_fcs_column(),
            Self::Uint(x) => x.into_fcs_column(),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Ascii(x) => x.column.len(),
            Self::Float(x) => x.len(),
            Self::Uint(x) => x.len(),
        }
    }
}

impl AnyUintColumnReader {
    fn into_fcs_column(self) -> AnyFCSColumn {
        match self {
            AnyUintColumnReader::Uint08(x) => U08Column::from(x.column).into(),
            AnyUintColumnReader::Uint16(x) => U16Column::from(x.column).into(),
            AnyUintColumnReader::Uint24(x) => U32Column::from(x.column).into(),
            AnyUintColumnReader::Uint32(x) => U32Column::from(x.column).into(),
            AnyUintColumnReader::Uint40(x) => U64Column::from(x.column).into(),
            AnyUintColumnReader::Uint48(x) => U64Column::from(x.column).into(),
            AnyUintColumnReader::Uint56(x) => U64Column::from(x.column).into(),
            AnyUintColumnReader::Uint64(x) => U64Column::from(x.column).into(),
        }
    }

    fn len(&self) -> usize {
        match_many_to_one!(
            self,
            Self,
            [Uint08, Uint16, Uint24, Uint32, Uint40, Uint48, Uint56, Uint64],
            x,
            { x.column.len() }
        )
    }
}

impl AnyUintColumnReader {
    fn h_read<R: Read>(&mut self, h: &mut BufReader<R>, r: usize) -> io::Result<()> {
        match_many_to_one!(
            self,
            AnyUintColumnReader,
            [Uint08, Uint16, Uint24, Uint32, Uint40, Uint48, Uint56, Uint64],
            d,
            { d.h_read(h, r)? }
        );
        Ok(())
    }
}

// TODO also check scale here?
impl MixedType {
    pub(crate) fn try_new(
        w: Width,
        dt: AlphaNumType,
        n: Endian,
        r: Range,
        notrunc: bool,
    ) -> DeferredResult<Self, BitmaskError, NewMixedTypeError> {
        match dt {
            AlphaNumType::Ascii => w
                .try_into()
                .map(|chars| Self::Ascii(AsciiType { chars }))
                .into_deferred(),
            AlphaNumType::Integer => AnyUintType::try_new(w, r, n, notrunc)
                .def_map_value(Self::Integer)
                .def_errors_into(),
            AlphaNumType::Single => f32::column_type_endian(w, n, r)
                .map(Self::Float)
                .into_deferred(),
            AlphaNumType::Double => f64::column_type_endian(w, n, r)
                .map(Self::Double)
                .into_deferred(),
        }
    }
}

fn ascii_to_uint(buf: &[u8]) -> Result<u64, AsciiToUintError> {
    if buf.is_ascii() {
        let s = unsafe { str::from_utf8_unchecked(buf) };
        s.parse().map_err(AsciiToUintError::from)
    } else {
        Err(NotAsciiError(buf.to_vec()).into())
    }
}

pub struct ColumnLayoutData<D> {
    pub width: Width,
    pub range: Range,
    pub datatype: D,
}

impl<C> FixedLayout<C> {
    fn from_vec(xs: Vec<C>) -> Option<Self> {
        NonEmpty::from_vec(xs).map(|columns| FixedLayout { columns })
    }
}

impl DelimitedLayout {
    fn into_col_reader_maybe_rows(self, nbytes: usize, kw_tot: Option<Tot>) -> ColumnReader {
        if self.ncols == 0 {
            ColumnReader::Empty
        } else {
            match kw_tot {
                // TODO not DRY
                Some(tot) => {
                    ColumnReader::DelimitedAscii(DelimAsciiReader(DelimAsciiReaderInner {
                        columns: NonEmpty::collect(repeat_n(vec![0; tot.0], self.ncols)).unwrap(),
                        nbytes,
                    }))
                }
                None => ColumnReader::DelimitedAsciiNoRows(DelimAsciiReaderNoRows(
                    DelimAsciiReaderInner {
                        columns: NonEmpty::collect(repeat_n(vec![], self.ncols)).unwrap(),
                        nbytes,
                    },
                )),
            }
        }
    }

    fn into_col_reader(self, nbytes: usize, tot: Tot) -> ColumnReader {
        if self.ncols == 0 {
            ColumnReader::Empty
        } else {
            ColumnReader::DelimitedAscii(DelimAsciiReader(DelimAsciiReaderInner {
                columns: NonEmpty::collect(repeat_n(vec![0; tot.0], self.ncols)).unwrap(),
                nbytes,
            }))
        }
    }
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

    pub fn into_col_reader_inner(
        self,
        seg: AnyDataSegment,
        conf: &ReaderConfig,
    ) -> Tentative<AlphaNumReader, UnevenEventWidth, UnevenEventWidth> {
        let n = seg.inner.len() as usize;
        let w = self.event_width();
        let total_events = n / w;
        let remainder = n % w;
        let columns = self.columns.map(|c| c.into_col_reader(total_events));
        let r = AlphaNumReader { columns };
        if remainder > 0 {
            let i = UnevenEventWidth {
                event_width: w,
                nbytes: n,
                remainder,
            };
            Tentative::new_either(r, vec![i], conf.enforce_data_width_divisibility)
        } else {
            Tentative::new1(r)
        }
    }

    pub fn into_col_reader<W, E>(
        self,
        seg: AnyDataSegment,
        tot: Tot,
        conf: &ReaderConfig,
    ) -> Tentative<ColumnReader, W, E>
    where
        W: From<UnevenEventWidth>,
        E: From<UnevenEventWidth>,
        W: From<TotEventMismatch>,
        E: From<TotEventMismatch>,
    {
        self.into_col_reader_inner(seg, conf)
            .inner_into()
            .and_tentatively(|reader| {
                reader
                    .check_tot(tot, conf.enforce_matching_tot)
                    .map(|_| reader)
                    .inner_into()
            })
            .map(ColumnReader::AlphaNum)
    }

    fn as_writer<'a>(
        &self,
        df: &'a FCSDataFrame,
        conf: &WriteConfig,
    ) -> MultiResult<Option<FixedWriter<'a>>, ColumnWriterError>
    where
        MixedType: From<C>,
        C: Copy,
    {
        let check = conf.check_conversion;
        self.columns
            .iter()
            .map(|c| (*c).into())
            .zip(df.iter_columns())
            .enumerate()
            .map(|(i, (t, c)): (usize, (MixedType, &AnyFCSColumn))| {
                t.into_writer(c, check).map_err(|error| {
                    ColumnWriterError(ColumnError {
                        index: i.into(),
                        error,
                    })
                })
            })
            .gather()
            .map(|columns| {
                FixedWriter::try_new(columns, df.nrows(), self.event_width() * df.nrows())
            })
    }
}

pub trait IsFixed {
    fn width(&self) -> usize;

    fn into_col_reader(self, nrows: usize) -> AlphaNumColumnReader;

    fn into_writer(
        self,
        c: &AnyFCSColumn,
        check: bool,
    ) -> Result<AnyFixedColumnWriter, AnyLossError>;
}

impl<T, const LEN: usize> IsFixed for UintType<T, LEN>
where
    T: Clone,
    T: Copy,
    T: Default,
    T: Ord,
    AlphaNumColumnReader: From<UintColumnReader<T, LEN>>,
    T: NumCast<u8>,
    T: NumCast<u16>,
    T: NumCast<u32>,
    T: NumCast<u64>,
    T: NumCast<f32>,
    T: NumCast<f64>,
    u64: From<T>,
    for<'b> AnyFixedColumnWriter<'b>: From<IntColumnWriter<'b, u8, T, LEN>>,
    for<'b> AnyFixedColumnWriter<'b>: From<IntColumnWriter<'b, u16, T, LEN>>,
    for<'b> AnyFixedColumnWriter<'b>: From<IntColumnWriter<'b, u32, T, LEN>>,
    for<'b> AnyFixedColumnWriter<'b>: From<IntColumnWriter<'b, u64, T, LEN>>,
    for<'b> AnyFixedColumnWriter<'b>: From<IntColumnWriter<'b, f32, T, LEN>>,
    for<'b> AnyFixedColumnWriter<'b>: From<IntColumnWriter<'b, f64, T, LEN>>,
{
    fn width(&self) -> usize {
        LEN
    }

    fn into_col_reader(self, nrows: usize) -> AlphaNumColumnReader {
        UintColumnReader {
            column: vec![T::default(); nrows],
            uint_type: self,
        }
        .into()
    }

    fn into_writer(
        self,
        c: &AnyFCSColumn,
        check: bool,
    ) -> Result<AnyFixedColumnWriter, AnyLossError> {
        let bitmask = self.bitmask;
        match_many_to_one!(c, AnyFCSColumn, [U08, U16, U32, U64, F32, F64], xs, {
            FCSDataType::into_writer(xs, self, check, |x: T| {
                if x > bitmask {
                    Some(BitmaskLossError(u64::from(bitmask)))
                } else {
                    None
                }
            })
            .map(|w| w.into())
            .map_err(|e| e.into())
        })
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

    fn into_col_reader(self, nrows: usize) -> AlphaNumColumnReader {
        match_many_to_one!(
            self,
            AnyUintType,
            [Uint08, Uint16, Uint24, Uint32, Uint40, Uint48, Uint56, Uint64],
            x,
            { x.into_col_reader(nrows) }
        )
    }

    fn into_writer(
        self,
        c: &AnyFCSColumn,
        check: bool,
    ) -> Result<AnyFixedColumnWriter, AnyLossError> {
        match_many_to_one!(
            self,
            AnyUintType,
            [Uint08, Uint16, Uint24, Uint32, Uint40, Uint48, Uint56, Uint64],
            x,
            { x.into_writer(c, check) }
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

macro_rules! uint_from_writer {
    ($fromtype:ident, $totype:ident, $len:expr, $fromwrap:ident, $wrap:ident) => {
        impl<'a> From<IntColumnWriter<'a, $fromtype, $totype, $len>> for AnyFixedColumnWriter<'a> {
            fn from(value: IntColumnWriter<'a, $fromtype, $totype, $len>) -> Self {
                AnyFixedColumnWriter::$fromwrap(AnyColumnWriter::$wrap(value))
            }
        }
    };
}

uint_from_writer!(u8, u8, 1, FromU08, U08);
uint_from_writer!(u8, u16, 2, FromU08, U16);
uint_from_writer!(u8, u32, 3, FromU08, U24);
uint_from_writer!(u8, u32, 4, FromU08, U32);
uint_from_writer!(u8, u64, 5, FromU08, U40);
uint_from_writer!(u8, u64, 6, FromU08, U48);
uint_from_writer!(u8, u64, 7, FromU08, U56);
uint_from_writer!(u8, u64, 8, FromU08, U64);

uint_from_writer!(u16, u8, 1, FromU16, U08);
uint_from_writer!(u16, u16, 2, FromU16, U16);
uint_from_writer!(u16, u32, 3, FromU16, U24);
uint_from_writer!(u16, u32, 4, FromU16, U32);
uint_from_writer!(u16, u64, 5, FromU16, U40);
uint_from_writer!(u16, u64, 6, FromU16, U48);
uint_from_writer!(u16, u64, 7, FromU16, U56);
uint_from_writer!(u16, u64, 8, FromU16, U64);

uint_from_writer!(u32, u8, 1, FromU32, U08);
uint_from_writer!(u32, u16, 2, FromU32, U16);
uint_from_writer!(u32, u32, 3, FromU32, U24);
uint_from_writer!(u32, u32, 4, FromU32, U32);
uint_from_writer!(u32, u64, 5, FromU32, U40);
uint_from_writer!(u32, u64, 6, FromU32, U48);
uint_from_writer!(u32, u64, 7, FromU32, U56);
uint_from_writer!(u32, u64, 8, FromU32, U64);

uint_from_writer!(u64, u8, 1, FromU64, U08);
uint_from_writer!(u64, u16, 2, FromU64, U16);
uint_from_writer!(u64, u32, 3, FromU64, U24);
uint_from_writer!(u64, u32, 4, FromU64, U32);
uint_from_writer!(u64, u64, 5, FromU64, U40);
uint_from_writer!(u64, u64, 6, FromU64, U48);
uint_from_writer!(u64, u64, 7, FromU64, U56);
uint_from_writer!(u64, u64, 8, FromU64, U64);

uint_from_writer!(f32, u8, 1, FromF32, U08);
uint_from_writer!(f32, u16, 2, FromF32, U16);
uint_from_writer!(f32, u32, 3, FromF32, U24);
uint_from_writer!(f32, u32, 4, FromF32, U32);
uint_from_writer!(f32, u64, 5, FromF32, U40);
uint_from_writer!(f32, u64, 6, FromF32, U48);
uint_from_writer!(f32, u64, 7, FromF32, U56);
uint_from_writer!(f32, u64, 8, FromF32, U64);

uint_from_writer!(f64, u8, 1, FromF64, U08);
uint_from_writer!(f64, u16, 2, FromF64, U16);
uint_from_writer!(f64, u32, 3, FromF64, U24);
uint_from_writer!(f64, u32, 4, FromF64, U32);
uint_from_writer!(f64, u64, 5, FromF64, U40);
uint_from_writer!(f64, u64, 6, FromF64, U48);
uint_from_writer!(f64, u64, 7, FromF64, U56);
uint_from_writer!(f64, u64, 8, FromF64, U64);

macro_rules! float_from_writer {
    ($fromtype:ident, $totype:ident, $len:expr, $fromwrap:ident, $wrap:ident) => {
        impl<'a> From<FloatColumnWriter<'a, $fromtype, $totype, $len>>
            for AnyFixedColumnWriter<'a>
        {
            fn from(value: FloatColumnWriter<'a, $fromtype, $totype, $len>) -> Self {
                AnyFixedColumnWriter::$fromwrap(AnyColumnWriter::$wrap(value))
            }
        }
    };
}

float_from_writer!(u8, f32, 4, FromU08, F32);
float_from_writer!(u8, f64, 8, FromU08, F64);

float_from_writer!(u16, f32, 4, FromU16, F32);
float_from_writer!(u16, f64, 8, FromU16, F64);

float_from_writer!(u32, f32, 4, FromU32, F32);
float_from_writer!(u32, f64, 8, FromU32, F64);

float_from_writer!(u64, f32, 4, FromU64, F32);
float_from_writer!(u64, f64, 8, FromU64, F64);

float_from_writer!(f32, f32, 4, FromF32, F32);
float_from_writer!(f32, f64, 8, FromF32, F64);

float_from_writer!(f64, f32, 4, FromF64, F32);
float_from_writer!(f64, f64, 8, FromF64, F64);

impl<T, const LEN: usize> IsFixed for FloatType<T, LEN>
where
    T: Clone,
    T: Default,
    AlphaNumColumnReader: From<FloatColumnReader<T, LEN>>,
    T: NumCast<u8>,
    T: NumCast<u16>,
    T: NumCast<u32>,
    T: NumCast<u64>,
    T: NumCast<f32>,
    T: NumCast<f64>,
    for<'b> AnyFixedColumnWriter<'b>: From<FloatColumnWriter<'b, u8, T, LEN>>,
    for<'b> AnyFixedColumnWriter<'b>: From<FloatColumnWriter<'b, u16, T, LEN>>,
    for<'b> AnyFixedColumnWriter<'b>: From<FloatColumnWriter<'b, u32, T, LEN>>,
    for<'b> AnyFixedColumnWriter<'b>: From<FloatColumnWriter<'b, u64, T, LEN>>,
    for<'b> AnyFixedColumnWriter<'b>: From<FloatColumnWriter<'b, f32, T, LEN>>,
    for<'b> AnyFixedColumnWriter<'b>: From<FloatColumnWriter<'b, f64, T, LEN>>,
{
    fn width(&self) -> usize {
        LEN
    }

    fn into_col_reader(self, nrows: usize) -> AlphaNumColumnReader {
        FloatColumnReader {
            column: vec![T::default(); nrows],
            size: self.order,
        }
        .into()
    }

    fn into_writer(
        self,
        c: &AnyFCSColumn,
        check: bool,
    ) -> Result<AnyFixedColumnWriter, AnyLossError> {
        match_many_to_one!(c, AnyFCSColumn, [U08, U16, U32, U64, F32, F64], xs, {
            FCSDataType::into_writer(xs, self.order, check, |_| None)
                .map(|w| w.into())
                .map_err(AnyLossError::Int)
        })
    }
}

impl<T, const INTLEN: usize> UintColumnReader<T, INTLEN> {
    fn h_read<R: Read, const DTLEN: usize>(
        &mut self,
        h: &mut BufReader<R>,
        row: usize,
    ) -> io::Result<()>
    where
        T: IntFromBytes<DTLEN, INTLEN>,
        <T as FromStr>::Err: fmt::Display,
        T: Ord,
    {
        let x = T::h_read_int(h, &self.uint_type.byteord)?;
        self.column[row] = x.min(self.uint_type.bitmask);
        Ok(())
    }
}

impl<T, const LEN: usize> FloatColumnReader<T, LEN> {
    fn h_read<R: Read>(&mut self, h: &mut BufReader<R>, row: usize) -> io::Result<()>
    where
        T: FloatFromBytes<LEN>,
        <T as FromStr>::Err: fmt::Display,
    {
        self.column[row] = T::h_read_float(h, &self.size)?;
        Ok(())
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

    fn into_col_reader(self, nrows: usize) -> AlphaNumColumnReader {
        AlphaNumColumnReader::Ascii(AsciiColumnReader {
            column: vec![0; nrows],
            width: self.chars,
        })
    }

    fn into_writer(
        self,
        col: &AnyFCSColumn,
        check: bool,
    ) -> Result<AnyFixedColumnWriter, AnyLossError> {
        let c = self.chars;
        let width = u8::from(c);
        let go = |x: u64| {
            if ascii_nbytes(x) > width.into() {
                Some(AsciiLossError(width))
            } else {
                None
            }
        };
        match col {
            AnyFCSColumn::U08(xs) => FCSDataType::into_writer(xs, c, check, go)
                .map(|w| AnyFixedColumnWriter::FromU08(AnyColumnWriter::Ascii(w))),
            AnyFCSColumn::U16(xs) => FCSDataType::into_writer(xs, c, check, go)
                .map(|w| AnyFixedColumnWriter::FromU16(AnyColumnWriter::Ascii(w))),
            AnyFCSColumn::U32(xs) => FCSDataType::into_writer(xs, c, check, go)
                .map(|w| AnyFixedColumnWriter::FromU32(AnyColumnWriter::Ascii(w))),
            AnyFCSColumn::U64(xs) => FCSDataType::into_writer(xs, c, check, go)
                .map(|w| AnyFixedColumnWriter::FromU64(AnyColumnWriter::Ascii(w))),
            AnyFCSColumn::F32(xs) => FCSDataType::into_writer(xs, c, check, go)
                .map(|w| AnyFixedColumnWriter::FromF32(AnyColumnWriter::Ascii(w))),
            AnyFCSColumn::F64(xs) => FCSDataType::into_writer(xs, c, check, go)
                .map(|w| AnyFixedColumnWriter::FromF64(AnyColumnWriter::Ascii(w))),
        }
        .map_err(|e| e.into())
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

    fn into_col_reader(self, nrows: usize) -> AlphaNumColumnReader {
        match self {
            MixedType::Ascii(a) => a.into_col_reader(nrows),
            MixedType::Integer(i) => i.into_col_reader(nrows),
            MixedType::Float(f) => f.into_col_reader(nrows),
            MixedType::Double(d) => d.into_col_reader(nrows),
        }
    }

    fn into_writer(
        self,
        c: &AnyFCSColumn,
        check: bool,
    ) -> Result<AnyFixedColumnWriter, AnyLossError> {
        match self {
            MixedType::Ascii(a) => a.into_writer(c, check),
            MixedType::Integer(i) => i.into_writer(c, check),
            MixedType::Float(f) => f.into_writer(c, check),
            MixedType::Double(d) => d.into_writer(c, check),
        }
    }
}

fn widths_to_single_fixed_bytes(ws: &[Width]) -> MultiResult<Option<Bytes>, SingleFixedWidthError> {
    let bs = ws
        .iter()
        .copied()
        .map(Bytes::try_from)
        .gather()
        .map_err(|es| es.map(SingleFixedWidthError::Bytes))?;
    NonEmpty::collect(bs.into_iter().unique()).map_or(Ok(None), |us| {
        if us.tail.is_empty() {
            Ok(Some(us.head))
        } else {
            Err(NonEmpty::new(SingleFixedWidthError::Multi(
                MultiWidthsError(us),
            )))
        }
    })
}

impl AnyUintLayout {
    pub(crate) fn try_new<D>(
        cs: Vec<ColumnLayoutData<D>>,
        o: &ByteOrd,
        notrunc: bool,
    ) -> DeferredResult<Option<Self>, BitmaskError, NewFixedIntLayoutError> {
        let (ws, rs): (Vec<_>, Vec<_>) = cs.into_iter().map(|c| (c.width, c.range)).unzip();
        widths_to_single_fixed_bytes(&ws[..])
            .mult_to_deferred()
            .def_and_maybe(|b| {
                if let Some(bytes) = b {
                    match u8::from(bytes) {
                        1 => u8::layout_ordered(rs, o, notrunc)
                            .def_map_value(|x| x.map(Self::Uint08)),
                        2 => u16::layout_ordered(rs, o, notrunc)
                            .def_map_value(|x| x.map(Self::Uint16)),
                        3 => u32::layout_ordered(rs, o, notrunc)
                            .def_map_value(|x| x.map(Self::Uint24)),
                        4 => u32::layout_ordered(rs, o, notrunc)
                            .def_map_value(|x| x.map(Self::Uint32)),
                        5 => u64::layout_ordered(rs, o, notrunc)
                            .def_map_value(|x| x.map(Self::Uint40)),
                        6 => u64::layout_ordered(rs, o, notrunc)
                            .def_map_value(|x| x.map(Self::Uint48)),
                        7 => u64::layout_ordered(rs, o, notrunc)
                            .def_map_value(|x| x.map(Self::Uint56)),
                        8 => u64::layout_ordered(rs, o, notrunc)
                            .def_map_value(|x| x.map(Self::Uint64)),
                        _ => unreachable!(),
                    }
                    .def_errors_into()
                } else {
                    Ok(Tentative::new1(None))
                }
            })
    }

    fn ncols(&self) -> usize {
        match_many_to_one!(
            self,
            Self,
            [Uint08, Uint16, Uint24, Uint32, Uint40, Uint48, Uint56, Uint64],
            l,
            { l.columns.len() }
        )
    }

    fn into_col_reader_inner(
        self,
        seg: AnyDataSegment,
        conf: &ReaderConfig,
    ) -> Tentative<AlphaNumReader, UnevenEventWidth, UnevenEventWidth> {
        match_many_to_one!(
            self,
            Self,
            [Uint08, Uint16, Uint24, Uint32, Uint40, Uint48, Uint56, Uint64],
            l,
            { l.into_col_reader_inner(seg, conf) }
        )
    }

    fn into_col_reader<W, E>(
        self,
        seg: AnyDataSegment,
        tot: Tot,
        conf: &ReaderConfig,
    ) -> Tentative<ColumnReader, W, E>
    where
        W: From<UnevenEventWidth>,
        E: From<UnevenEventWidth>,
        W: From<TotEventMismatch>,
        E: From<TotEventMismatch>,
    {
        match_many_to_one!(
            self,
            Self,
            [Uint08, Uint16, Uint24, Uint32, Uint40, Uint48, Uint56, Uint64],
            l,
            { l.into_col_reader(seg, tot, conf) }
        )
    }

    fn as_writer<'a>(
        &self,
        df: &'a FCSDataFrame,
        conf: &WriteConfig,
    ) -> MultiResult<Option<FixedWriter<'a>>, ColumnWriterError> {
        match_many_to_one!(
            self,
            Self,
            [Uint08, Uint16, Uint24, Uint32, Uint40, Uint48, Uint56, Uint64],
            l,
            { l.as_writer(df, conf) }
        )
    }
}

impl AsciiLayout {
    pub(crate) fn try_new<D>(
        cs: Vec<ColumnLayoutData<D>>,
    ) -> MultiResult<Option<Self>, NewAsciiLayoutError> {
        let ncols = cs.len();
        if cs.iter().all(|c| c.width == Width::Variable) {
            Ok(Some(AsciiLayout::Delimited(DelimitedLayout { ncols })))
        } else {
            cs.into_iter()
                .enumerate()
                .map(|(i, c)| {
                    c.width
                        .try_into()
                        .map(|chars| AsciiType { chars })
                        .map_err(|error| {
                            ColumnError {
                                error,
                                index: i.into(),
                            }
                            .into()
                        })
                })
                .gather()
                .map(|columns| FixedLayout::from_vec(columns).map(AsciiLayout::Fixed))
        }
    }

    fn ncols(&self) -> usize {
        match self {
            AsciiLayout::Delimited(a) => a.ncols,
            AsciiLayout::Fixed(l) => l.columns.len(),
        }
    }

    fn as_writer<'a>(
        &self,
        df: &'a FCSDataFrame,
        conf: &WriteConfig,
    ) -> MultiResult<DataWriter<'a>, ColumnWriterError> {
        match self {
            AsciiLayout::Fixed(a) => a
                .as_writer(df, conf)
                .map(|x| x.map_or(DataWriter::Empty, DataWriter::Fixed)),
            AsciiLayout::Delimited(_) => {
                let ch = conf.check_conversion;
                let go = |c: &'a AnyFCSColumn| match c {
                    AnyFCSColumn::U08(xs) => FCSDataType::into_writer(xs, (), ch, |_| None)
                        .map(AnyDelimColumnWriter::FromU08),
                    AnyFCSColumn::U16(xs) => FCSDataType::into_writer(xs, (), ch, |_| None)
                        .map(AnyDelimColumnWriter::FromU16),
                    AnyFCSColumn::U32(xs) => FCSDataType::into_writer(xs, (), ch, |_| None)
                        .map(AnyDelimColumnWriter::FromU32),
                    AnyFCSColumn::U64(xs) => FCSDataType::into_writer(xs, (), ch, |_| None)
                        .map(AnyDelimColumnWriter::FromU64),
                    AnyFCSColumn::F32(xs) => FCSDataType::into_writer(xs, (), ch, |_| None)
                        .map(AnyDelimColumnWriter::FromF32),
                    AnyFCSColumn::F64(xs) => FCSDataType::into_writer(xs, (), ch, |_| None)
                        .map(AnyDelimColumnWriter::FromF64),
                };
                df.iter_columns()
                    .enumerate()
                    .map(|(i, c)| {
                        go(c).map_err(|error| {
                            ColumnWriterError(ColumnError {
                                index: i.into(),
                                error: AnyLossError::Int(error),
                            })
                        })
                    })
                    .gather()
                    .map(|columns| {
                        DelimWriter::try_new(columns, df.nrows(), df.ascii_nbytes())
                            .map_or(DataWriter::Empty, DataWriter::Delim)
                    })
            }
        }
    }

    fn into_col_reader_maybe_rows(
        self,
        seg: AnyDataSegment,
        kw_tot: Option<Tot>,
        conf: &ReaderConfig,
    ) -> Tentative<ColumnReader, UnevenEventWidth, UnevenEventWidth> {
        let nbytes = seg.inner.len() as usize;
        match self {
            AsciiLayout::Delimited(dl) => {
                Tentative::new1(dl.into_col_reader_maybe_rows(nbytes, kw_tot))
            }
            AsciiLayout::Fixed(fl) => fl
                .into_col_reader_inner(seg, conf)
                .map(ColumnReader::AlphaNum),
        }
    }

    fn into_col_reader<W, E>(
        self,
        seg: AnyDataSegment,
        tot: Tot,
        conf: &ReaderConfig,
    ) -> Tentative<ColumnReader, W, E>
    where
        W: From<UnevenEventWidth>,
        E: From<UnevenEventWidth>,
        W: From<TotEventMismatch>,
        E: From<TotEventMismatch>,
    {
        let nbytes = seg.inner.len() as usize;
        match self {
            AsciiLayout::Delimited(dl) => Tentative::new1(dl.into_col_reader(nbytes, tot)),
            AsciiLayout::Fixed(fl) => fl.into_col_reader(seg, tot, conf),
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

    fn into_col_reader_inner(
        self,
        seg: AnyDataSegment,
        conf: &ReaderConfig,
    ) -> Tentative<AlphaNumReader, UnevenEventWidth, UnevenEventWidth> {
        match_many_to_one!(self, Self, [F32, F64], l, {
            l.into_col_reader_inner(seg, conf)
        })
    }

    fn into_col_reader<W, E>(
        self,
        seg: AnyDataSegment,
        tot: Tot,
        conf: &ReaderConfig,
    ) -> Tentative<ColumnReader, W, E>
    where
        W: From<UnevenEventWidth>,
        E: From<UnevenEventWidth>,
        W: From<TotEventMismatch>,
        E: From<TotEventMismatch>,
    {
        match_many_to_one!(self, Self, [F32, F64], l, {
            l.into_col_reader(seg, tot, conf)
        })
    }

    fn as_writer<'a>(
        &self,
        df: &'a FCSDataFrame,
        conf: &WriteConfig,
    ) -> MultiResult<Option<FixedWriter<'a>>, ColumnWriterError> {
        match self {
            FloatLayout::F32(l) => l.as_writer(df, conf),
            FloatLayout::F64(l) => l.as_writer(df, conf),
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
        conf: &SharedConfig,
    ) -> DeferredResult<Self, NewDataLayoutWarning, NewDataLayoutError> {
        match datatype {
            AlphaNumType::Ascii => AsciiLayout::try_new(columns)
                .map(|x| x.map_or(Self::Empty, Self::Ascii))
                .mult_to_deferred(),
            AlphaNumType::Integer => {
                AnyUintLayout::try_new(columns, &byteord, conf.bitmask_notruncate)
                    .def_map_value(|x| x.map_or(Self::Empty, Self::Integer))
                    .def_inner_into()
            }
            AlphaNumType::Single => f32::layout(columns, &byteord)
                .map(|x| x.map_or(Self::Empty, |y| Self::Float(FloatLayout::F32(y))))
                .mult_to_deferred(),
            AlphaNumType::Double => f64::layout(columns, &byteord)
                .map(|x| x.map_or(Self::Empty, |y| Self::Float(FloatLayout::F64(y))))
                .mult_to_deferred(),
        }
    }

    fn try_new_from_raw(kws: &StdKeywords, conf: &SharedConfig) -> FromRawResult<Self> {
        kws_get_layout_2_0(kws)
            .mult_to_deferred()
            .def_and_maybe(|(datatype, byteord, columns)| {
                Self::try_new(datatype, byteord, columns, conf).def_inner_into()
            })
    }

    fn ncols(&self) -> usize {
        match self {
            Self::Ascii(a) => a.ncols(),
            Self::Integer(i) => i.ncols(),
            Self::Float(f) => f.ncols(),
            Self::Empty => 0,
        }
    }

    fn as_writer_inner<'a>(
        &self,
        df: &'a FCSDataFrame,
        conf: &WriteConfig,
    ) -> MultiResult<DataWriter<'a>, ColumnWriterError> {
        match self {
            Self::Ascii(a) => a.as_writer(df, conf),
            Self::Integer(i) => i
                .as_writer(df, conf)
                .map(|x| x.map_or(DataWriter::Empty, DataWriter::Fixed)),
            Self::Float(f) => f
                .as_writer(df, conf)
                .map(|x| x.map_or(DataWriter::Empty, DataWriter::Fixed)),
            Self::Empty => Ok(DataWriter::Empty),
        }
    }

    fn into_data_reader(
        self,
        kws: &mut StdKeywords,
        seg: HeaderDataSegment,
        conf: &ReaderConfig,
    ) -> DataReaderResult<DataReader> {
        let out = Tot::remove_meta_opt(kws)
            .map(|x| x.0)
            .map_or_else(
                |w| Tentative::new(None, vec![w.into()], vec![]),
                Tentative::new1,
            )
            .and_tentatively(|maybe_tot| self.into_reader(maybe_tot, seg.into_any(), conf));
        Ok(out)
    }

    fn into_data_reader_raw(
        self,
        kws: &StdKeywords,
        seg: HeaderDataSegment,
        conf: &ReaderConfig,
    ) -> DataReaderResult<DataReader> {
        let out = Tot::get_meta_opt(kws)
            .map(|x| x.0)
            .map_or_else(
                |w| Tentative::new(None, vec![w.into()], vec![]),
                Tentative::new1,
            )
            .and_tentatively(|maybe_tot| self.into_reader(maybe_tot, seg.into_any(), conf));
        Ok(out)
    }

    fn as_analysis_reader(
        _: &mut StdKeywords,
        seg: HeaderAnalysisSegment,
        _: &ReaderConfig,
    ) -> AnalysisReaderResult<AnalysisReader> {
        Ok(Tentative::new1(AnalysisReader {
            seg: seg.into_any(),
        }))
    }

    fn as_analysis_reader_raw(
        _: &StdKeywords,
        seg: HeaderAnalysisSegment,
        _: &ReaderConfig,
    ) -> AnalysisReaderResult<AnalysisReader> {
        Ok(Tentative::new1(AnalysisReader {
            seg: seg.into_any(),
        }))
    }
}

impl VersionedDataLayout for DataLayout3_0 {
    type S = ByteOrd;
    type D = ();

    fn try_new(
        datatype: AlphaNumType,
        byteord: Self::S,
        columns: Vec<ColumnLayoutData<Self::D>>,
        conf: &SharedConfig,
    ) -> DeferredResult<Self, NewDataLayoutWarning, NewDataLayoutError> {
        match datatype {
            AlphaNumType::Ascii => AsciiLayout::try_new(columns)
                .map(|x| x.map_or(Self::Empty, Self::Ascii))
                .mult_to_deferred(),
            AlphaNumType::Integer => {
                AnyUintLayout::try_new(columns, &byteord, conf.bitmask_notruncate)
                    .def_map_value(|x| x.map_or(Self::Empty, Self::Integer))
                    .def_inner_into()
            }
            AlphaNumType::Single => f32::layout(columns, &byteord)
                .map(|x| x.map_or(Self::Empty, |y| Self::Float(FloatLayout::F32(y))))
                .mult_to_deferred(),
            AlphaNumType::Double => f64::layout(columns, &byteord)
                .map(|x| x.map_or(Self::Empty, |y| Self::Float(FloatLayout::F64(y))))
                .mult_to_deferred(),
        }
    }

    fn try_new_from_raw(kws: &StdKeywords, conf: &SharedConfig) -> FromRawResult<Self> {
        kws_get_layout_2_0(kws)
            .mult_to_deferred()
            .def_and_maybe(|(datatype, byteord, columns)| {
                Self::try_new(datatype, byteord, columns, conf).def_inner_into()
            })
    }

    fn ncols(&self) -> usize {
        match self {
            Self::Ascii(a) => a.ncols(),
            Self::Integer(i) => i.ncols(),
            Self::Float(f) => f.ncols(),
            Self::Empty => 0,
        }
    }

    fn as_writer_inner<'a>(
        &self,
        df: &'a FCSDataFrame,
        conf: &WriteConfig,
    ) -> MultiResult<DataWriter<'a>, ColumnWriterError> {
        match self {
            Self::Ascii(a) => a.as_writer(df, conf),
            Self::Integer(i) => i
                .as_writer(df, conf)
                .map(|x| x.map_or(DataWriter::Empty, DataWriter::Fixed)),
            Self::Float(f) => f
                .as_writer(df, conf)
                .map(|x| x.map_or(DataWriter::Empty, DataWriter::Fixed)),
            Self::Empty => Ok(DataWriter::Empty),
        }
    }

    fn into_data_reader(
        self,
        kws: &mut StdKeywords,
        seg: HeaderDataSegment,
        conf: &ReaderConfig,
    ) -> DataReaderResult<DataReader> {
        remove_tot_data_seg(kws, seg, conf)
            .def_and_tentatively(|(tot, any_seg)| self.into_reader(tot, any_seg, conf))
    }

    fn into_data_reader_raw(
        self,
        kws: &StdKeywords,
        seg: HeaderDataSegment,
        conf: &ReaderConfig,
    ) -> DataReaderResult<DataReader> {
        get_tot_data_seg(kws, seg, conf)
            .def_and_tentatively(|(tot, any_seg)| self.into_reader(tot, any_seg, conf))
    }

    fn as_analysis_reader(
        kws: &mut StdKeywords,
        seg: HeaderAnalysisSegment,
        conf: &ReaderConfig,
    ) -> AnalysisReaderResult<AnalysisReader> {
        remove_analysis_seg_req(kws, seg, conf)
    }

    fn as_analysis_reader_raw(
        kws: &StdKeywords,
        seg: HeaderAnalysisSegment,
        conf: &ReaderConfig,
    ) -> AnalysisReaderResult<AnalysisReader> {
        get_analysis_seg_req(kws, seg, conf)
    }
}

impl VersionedDataLayout for DataLayout3_1 {
    type S = Endian;
    type D = ();

    fn try_new(
        datatype: AlphaNumType,
        endian: Self::S,
        columns: Vec<ColumnLayoutData<Self::D>>,
        conf: &SharedConfig,
    ) -> DeferredResult<Self, NewDataLayoutWarning, NewDataLayoutError> {
        match datatype {
            AlphaNumType::Ascii => AsciiLayout::try_new(columns)
                .map(|x| x.map_or(Self::Empty, Self::Ascii))
                .mult_to_deferred(),
            AlphaNumType::Integer => FixedLayout::try_new(columns, endian, conf.bitmask_notruncate)
                .def_map_value(|x| x.map_or(Self::Empty, Self::Integer))
                .def_inner_into(),
            AlphaNumType::Single => f32::layout_endian(columns, endian)
                .map(|x| x.map_or(Self::Empty, |y| Self::Float(FloatLayout::F32(y))))
                .mult_to_deferred(),
            AlphaNumType::Double => f64::layout_endian(columns, endian)
                .map(|x| x.map_or(Self::Empty, |y| Self::Float(FloatLayout::F64(y))))
                .mult_to_deferred(),
        }
    }

    fn try_new_from_raw(kws: &StdKeywords, conf: &SharedConfig) -> FromRawResult<Self> {
        let cs = kws_get_columns(kws);
        let d = AlphaNumType::get_meta_req(kws).into_mult::<RawParsedError>();
        let n = Endian::get_meta_req(kws).into_mult();
        d.mult_zip3(n, cs)
            .mult_to_deferred()
            .def_and_maybe(|(datatype, byteord, columns)| {
                Self::try_new(datatype, byteord, columns, conf).def_inner_into()
            })
    }

    fn ncols(&self) -> usize {
        match self {
            Self::Ascii(a) => a.ncols(),
            Self::Integer(i) => i.ncols(),
            Self::Float(f) => f.ncols(),
            Self::Empty => 0,
        }
    }

    fn as_writer_inner<'a>(
        &self,
        df: &'a FCSDataFrame,
        conf: &WriteConfig,
    ) -> MultiResult<DataWriter<'a>, ColumnWriterError> {
        match self {
            Self::Ascii(a) => a.as_writer(df, conf),
            Self::Integer(i) => i
                .as_writer(df, conf)
                .map(|x| x.map_or(DataWriter::Empty, DataWriter::Fixed)),
            Self::Float(f) => f
                .as_writer(df, conf)
                .map(|x| x.map_or(DataWriter::Empty, DataWriter::Fixed)),
            Self::Empty => Ok(DataWriter::Empty),
        }
    }

    fn into_data_reader(
        self,
        kws: &mut StdKeywords,
        seg: HeaderDataSegment,
        conf: &ReaderConfig,
    ) -> DataReaderResult<DataReader> {
        remove_tot_data_seg(kws, seg, conf)
            .def_and_tentatively(|(tot, any_seg)| self.into_reader(tot, any_seg, conf))
    }

    fn into_data_reader_raw(
        self,
        kws: &StdKeywords,
        seg: HeaderDataSegment,
        conf: &ReaderConfig,
    ) -> DataReaderResult<DataReader> {
        get_tot_data_seg(kws, seg, conf)
            .def_and_tentatively(|(tot, any_seg)| self.into_reader(tot, any_seg, conf))
    }

    fn as_analysis_reader(
        kws: &mut StdKeywords,
        seg: HeaderAnalysisSegment,
        conf: &ReaderConfig,
    ) -> AnalysisReaderResult<AnalysisReader> {
        remove_analysis_seg_req(kws, seg, conf)
    }

    fn as_analysis_reader_raw(
        kws: &StdKeywords,
        seg: HeaderAnalysisSegment,
        conf: &ReaderConfig,
    ) -> AnalysisReaderResult<AnalysisReader> {
        get_analysis_seg_req(kws, seg, conf)
    }
}

impl VersionedDataLayout for DataLayout3_2 {
    type S = Endian;
    type D = Option<NumType>;

    fn try_new(
        datatype: AlphaNumType,
        endian: Self::S,
        clayouts: Vec<ColumnLayoutData<Self::D>>,
        conf: &SharedConfig,
    ) -> DeferredResult<Self, NewDataLayoutWarning, NewDataLayoutError> {
        let dt_columns: Vec<_> = clayouts
            .into_iter()
            .map(|c| ColumnLayoutData {
                width: c.width,
                range: c.range,
                datatype: c.datatype.map(|x| x.into()).unwrap_or(datatype),
            })
            .collect();
        let unique_dt: Vec<_> = dt_columns.iter().map(|c| c.datatype).unique().collect();
        match unique_dt[..] {
            [dt] => match dt {
                AlphaNumType::Ascii => AsciiLayout::try_new(dt_columns)
                    .map(|x| x.map_or(Self::Empty, Self::Ascii))
                    .mult_to_deferred(),
                AlphaNumType::Integer => {
                    FixedLayout::try_new(dt_columns, endian, conf.bitmask_notruncate)
                        .def_map_value(|x| x.map_or(Self::Empty, Self::Integer))
                        .def_inner_into()
                }
                AlphaNumType::Single => f32::layout_endian(dt_columns, endian)
                    .map(|x| x.map_or(Self::Empty, |y| Self::Float(FloatLayout::F32(y))))
                    .mult_to_deferred(),
                AlphaNumType::Double => f64::layout_endian(dt_columns, endian)
                    .map(|x| x.map_or(Self::Empty, |y| Self::Float(FloatLayout::F64(y))))
                    .mult_to_deferred(),
            },
            _ => dt_columns
                .into_iter()
                .enumerate()
                .map(|(i, c)| {
                    MixedType::try_new(
                        c.width,
                        c.datatype,
                        endian,
                        c.range,
                        conf.bitmask_notruncate,
                    )
                    .def_map_errors(|error| {
                        ColumnError {
                            error,
                            index: i.into(),
                        }
                        .into()
                    })
                    .def_map_errors(NewDataLayoutError::Mixed)
                    .def_map_warnings(|error| {
                        ColumnError {
                            error,
                            index: i.into(),
                        }
                        .into()
                    })
                    .def_map_warnings(NewDataLayoutWarning::VariableInt)
                })
                .gather()
                .map(Tentative::mconcat)
                .map_err(DeferredFailure::mconcat)
                .def_map_value(|mcs| {
                    NonEmpty::from_vec(mcs)
                        .map_or(Self::Empty, |columns| Self::Mixed(FixedLayout { columns }))
                }),
        }
    }

    fn try_new_from_raw(kws: &StdKeywords, conf: &SharedConfig) -> FromRawResult<Self> {
        let d = AlphaNumType::get_meta_req(kws)
            .map_err(RawParsedError::from)
            .into_deferred();
        let e = Endian::get_meta_req(kws)
            .map_err(RawParsedError::from)
            .into_deferred();
        let cs = kws_get_columns_3_2(kws).def_inner_into();
        d.def_zip3(e, cs)
            .def_and_maybe(|(datatype, endian, columns)| {
                Self::try_new(datatype, endian, columns, conf).def_inner_into()
            })
    }

    fn ncols(&self) -> usize {
        match self {
            Self::Ascii(a) => a.ncols(),
            Self::Integer(i) => i.ncols(),
            Self::Float(f) => f.ncols(),
            Self::Mixed(m) => m.ncols(),
            Self::Empty => 0,
        }
    }

    fn as_writer_inner<'a>(
        &self,
        df: &'a FCSDataFrame,
        conf: &WriteConfig,
    ) -> MultiResult<DataWriter<'a>, ColumnWriterError> {
        match self {
            Self::Ascii(a) => a.as_writer(df, conf),
            Self::Integer(i) => i
                .as_writer(df, conf)
                .map(|x| x.map_or(DataWriter::Empty, DataWriter::Fixed)),
            Self::Float(f) => f
                .as_writer(df, conf)
                .map(|x| x.map_or(DataWriter::Empty, DataWriter::Fixed)),
            Self::Mixed(m) => m
                .as_writer(df, conf)
                .map(|x| x.map_or(DataWriter::Empty, DataWriter::Fixed)),
            Self::Empty => Ok(DataWriter::Empty),
        }
    }

    fn into_data_reader(
        self,
        kws: &mut StdKeywords,
        seg: HeaderDataSegment,
        conf: &ReaderConfig,
    ) -> DataReaderResult<DataReader> {
        remove_tot_data_seg(kws, seg, conf)
            .def_and_tentatively(|(tot, any_seg)| self.into_reader(tot, any_seg, conf))
    }

    fn into_data_reader_raw(
        self,
        kws: &StdKeywords,
        seg: HeaderDataSegment,
        conf: &ReaderConfig,
    ) -> DataReaderResult<DataReader> {
        get_tot_data_seg(kws, seg, conf)
            .def_and_tentatively(|(tot, any_seg)| self.into_reader(tot, any_seg, conf))
    }

    fn as_analysis_reader(
        kws: &mut StdKeywords,
        seg: HeaderAnalysisSegment,
        conf: &ReaderConfig,
    ) -> AnalysisReaderResult<AnalysisReader> {
        let ret = LookupOptSegment::remove_or(kws, conf.analysis, seg, conf.enforce_offset_match)
            .map(|s| AnalysisReader { seg: s })
            .inner_into();
        Ok(ret)
    }

    fn as_analysis_reader_raw(
        kws: &StdKeywords,
        seg: HeaderAnalysisSegment,
        conf: &ReaderConfig,
    ) -> AnalysisReaderResult<AnalysisReader> {
        let ret = LookupOptSegment::get_or(kws, conf.analysis, seg, conf.enforce_offset_match)
            .map(|s| AnalysisReader { seg: s })
            .inner_into();
        Ok(ret)
    }
}

fn remove_analysis_seg_req(
    kws: &mut StdKeywords,
    seg: HeaderAnalysisSegment,
    conf: &ReaderConfig,
) -> AnalysisReaderResult<AnalysisReader> {
    LookupReqSegment::remove_or(
        kws,
        conf.analysis,
        seg,
        conf.enforce_offset_match,
        conf.enforce_required_offsets,
    )
    .def_inner_into()
    .def_map_value(|s| AnalysisReader { seg: s })
}

fn get_analysis_seg_req(
    kws: &StdKeywords,
    seg: HeaderAnalysisSegment,
    conf: &ReaderConfig,
) -> AnalysisReaderResult<AnalysisReader> {
    LookupReqSegment::get_or(
        kws,
        conf.analysis,
        seg,
        conf.enforce_offset_match,
        conf.enforce_required_offsets,
    )
    .def_inner_into()
    .def_map_value(|s| AnalysisReader { seg: s })
}

fn remove_tot_data_seg(
    kws: &mut StdKeywords,
    seg: HeaderDataSegment,
    conf: &ReaderConfig,
) -> DataReaderResult<(Tot, AnyDataSegment)> {
    let tot_res = Tot::remove_meta_req(kws).into_deferred();
    let seg_res = LookupReqSegment::remove_or(
        kws,
        conf.data,
        seg,
        conf.enforce_offset_match,
        conf.enforce_required_offsets,
    )
    .def_inner_into();
    tot_res.def_zip(seg_res)
}

impl DataLayout2_0 {
    fn into_reader<W, E>(
        self,
        tot: Option<Tot>,
        seg: AnyDataSegment,
        conf: &ReaderConfig,
    ) -> Tentative<DataReader, W, E>
    where
        W: From<UnevenEventWidth>,
        E: From<UnevenEventWidth>,
        W: From<TotEventMismatch>,
        E: From<TotEventMismatch>,
    {
        let go = |tnt: Tentative<AlphaNumReader, _, _>, maybe_tot| {
            tnt.inner_into()
                .and_tentatively(|reader| {
                    if let Some(_tot) = maybe_tot {
                        reader
                            .check_tot(_tot, conf.enforce_matching_tot)
                            .inner_into()
                            .map(|_| reader)
                    } else {
                        Tentative::new1(reader)
                    }
                })
                .map(ColumnReader::AlphaNum)
        };
        match self {
            Self::Ascii(a) => a.into_col_reader_maybe_rows(seg, tot, conf).inner_into(),
            Self::Integer(fl) => go(fl.into_col_reader_inner(seg, conf), tot),
            Self::Float(fl) => go(fl.into_col_reader_inner(seg, conf), tot),
            Self::Empty => Tentative::new1(ColumnReader::Empty),
        }
        .map(|r| r.into_data_reader(seg))
    }
}

impl DataLayout3_0 {
    fn into_reader<W, E>(
        self,
        tot: Tot,
        seg: AnyDataSegment,
        conf: &ReaderConfig,
    ) -> Tentative<DataReader, W, E>
    where
        W: From<UnevenEventWidth>,
        E: From<UnevenEventWidth>,
        W: From<TotEventMismatch>,
        E: From<TotEventMismatch>,
    {
        match self {
            Self::Ascii(a) => a.into_col_reader(seg, tot, conf),
            Self::Integer(fl) => fl.into_col_reader(seg, tot, conf),
            Self::Float(fl) => fl.into_col_reader(seg, tot, conf),
            Self::Empty => Tentative::new1(ColumnReader::Empty),
        }
        .map(|r| r.into_data_reader(seg))
    }
}

impl DataLayout3_1 {
    fn into_reader<W, E>(
        self,
        tot: Tot,
        seg: AnyDataSegment,
        conf: &ReaderConfig,
    ) -> Tentative<DataReader, W, E>
    where
        W: From<UnevenEventWidth>,
        E: From<UnevenEventWidth>,
        W: From<TotEventMismatch>,
        E: From<TotEventMismatch>,
    {
        match self {
            Self::Ascii(a) => a.into_col_reader(seg, tot, conf),
            Self::Integer(fl) => fl.into_col_reader(seg, tot, conf),
            Self::Float(fl) => fl.into_col_reader(seg, tot, conf),
            Self::Empty => Tentative::new1(ColumnReader::Empty),
        }
        .map(|r| r.into_data_reader(seg))
    }
}

impl DataLayout3_2 {
    fn into_reader<W, E>(
        self,
        tot: Tot,
        seg: AnyDataSegment,
        conf: &ReaderConfig,
    ) -> Tentative<DataReader, W, E>
    where
        W: From<UnevenEventWidth>,
        E: From<UnevenEventWidth>,
        W: From<TotEventMismatch>,
        E: From<TotEventMismatch>,
    {
        match self {
            Self::Ascii(a) => a.into_col_reader(seg, tot, conf),
            Self::Integer(fl) => fl.into_col_reader(seg, tot, conf),
            Self::Float(fl) => fl.into_col_reader(seg, tot, conf),
            Self::Mixed(fl) => fl.into_col_reader(seg, tot, conf),
            Self::Empty => Tentative::new1(ColumnReader::Empty),
        }
        .map(|r| r.into_data_reader(seg))
    }
}

fn get_tot_data_seg(
    kws: &StdKeywords,
    seg: HeaderDataSegment,
    conf: &ReaderConfig,
) -> DataReaderResult<(Tot, AnyDataSegment)> {
    let tot_res = Tot::get_meta_req(kws).into_deferred();
    let seg_res = LookupReqSegment::get_or(
        kws,
        conf.data,
        seg,
        conf.enforce_offset_match,
        conf.enforce_required_offsets,
    )
    .def_inner_into();
    tot_res.def_zip(seg_res)
}

#[allow(clippy::type_complexity)]
fn kws_get_layout_2_0(
    kws: &StdKeywords,
) -> MultiResult<(AlphaNumType, ByteOrd, Vec<ColumnLayoutData<()>>), RawParsedError> {
    let cs = kws_get_columns(kws);
    let d = AlphaNumType::get_meta_req(kws).into_mult();
    let b = ByteOrd::get_meta_req(kws).into_mult();
    d.mult_zip3(b, cs)
}

fn kws_get_columns(kws: &StdKeywords) -> MultiResult<Vec<ColumnLayoutData<()>>, RawParsedError> {
    let par = Par::get_meta_req(kws).into_mult()?;
    (0..par.0)
        .map(|i| {
            let w = Width::get_meas_req(kws, i.into()).map_err(|e| e.into());
            let r = Range::get_meas_req(kws, i.into()).map_err(|e| e.into());
            w.zip(r).map(|(width, range)| ColumnLayoutData {
                width,
                range,
                datatype: (),
            })
        })
        .gather()
        .map_err(NonEmpty::flatten)
}

fn kws_get_columns_3_2(
    kws: &StdKeywords,
) -> DeferredResult<
    Vec<ColumnLayoutData<Option<NumType>>>,
    ParseKeyError<NumTypeError>,
    RawParsedError,
> {
    let par = Par::get_meta_req(kws)
        .map_err(|e| e.into())
        .map_err(DeferredFailure::new1)?;
    (0..par.0)
        .map(|i| {
            let index = i.into();
            match NumType::get_meas_opt(kws, index) {
                Ok(x) => Tentative::new1(x.0),
                Err(e) => Tentative::new(None, vec![e], vec![]),
            }
            .and_maybe(|pn_datatype| {
                let w = Width::get_meas_req(kws, index).map_err(RawParsedError::from);
                let r = Range::get_meas_req(kws, index).map_err(|e| e.into());
                w.zip(r)
                    .map(|(width, range)| ColumnLayoutData {
                        width,
                        range,
                        datatype: pn_datatype,
                    })
                    .mult_to_deferred()
            })
        })
        .gather()
        .map_err(DeferredFailure::mconcat)
        .map(Tentative::mconcat)
}

pub(crate) fn h_read_data_and_analysis<R: Read + Seek>(
    h: &mut BufReader<R>,
    data_reader: DataReader,
    analysis_reader: AnalysisReader,
) -> IOResult<(FCSDataFrame, Analysis, AnyDataSegment, AnyAnalysisSegment), ReadDataError> {
    let dseg = data_reader.seg;
    let data = data_reader.h_read(h)?;
    let analysis = analysis_reader.h_read(h)?;
    Ok((data, analysis, dseg, analysis_reader.seg))
}

enum_from_disp!(
    pub AsciiToUintError,
    [NotAscii, NotAsciiError],
    [Int, ParseIntError]
);

pub struct NotAsciiError(Vec<u8>);

enum_from_disp!(
    pub NewDataLayoutError,
    [Ascii,        NewAsciiLayoutError],
    [FixedInt,     NewFixedIntLayoutError],
    [OrderedFloat, OrderedFloatColumnError],
    [EndianFloat,  EndianFloatColumnError],
    [VariableInt,  UintColumnError],
    [Mixed,        MixedColumnError]
);

enum_from_disp!(
    pub NewDataLayoutWarning,
    [FixedInt,     BitmaskError],
    [VariableInt,  UintColumnWarning]
);

pub struct NewAsciiLayoutError(ColumnError<WidthToCharsError>);

newtype_from!(NewAsciiLayoutError, ColumnError<WidthToCharsError>);
newtype_disp!(NewAsciiLayoutError);

enum_from_disp!(
    pub NewFixedIntLayoutError,
    [Width, SingleFixedWidthError],
    [Column, IntOrderedColumnError]
);

pub struct UintColumnError(ColumnError<NewUintTypeError>);

newtype_disp!(UintColumnError);
newtype_from!(UintColumnError, ColumnError<NewUintTypeError>);

// TODO this will make the warning look like an error
pub struct UintColumnWarning(ColumnError<BitmaskError>);

newtype_disp!(UintColumnWarning);
newtype_from!(UintColumnWarning, ColumnError<BitmaskError>);

enum_from_disp!(
    pub IntOrderedColumnError,
    [Order, ByteOrdToSizedError],
    [Size,  BitmaskError]
);

pub enum BitmaskError {
    IntOverrange(u64),
    FloatOverrange(f64),
    FloatUnderrange(f64),
    FloatPrecisionLoss(f64),
}

enum_from_disp!(
    pub SingleFixedWidthError,
    [Bytes, WidthToBytesError],
    [Multi, MultiWidthsError]
);

pub struct MultiWidthsError(pub NonEmpty<Bytes>);

pub struct MixedColumnError(ColumnError<NewMixedTypeError>);

newtype_from!(MixedColumnError, ColumnError<NewMixedTypeError>);
newtype_disp!(MixedColumnError);

enum_from_disp!(
    pub NewMixedTypeError,
    [Ascii, WidthToCharsError],
    [Uint, NewUintTypeError],
    [Float, FloatWidthError]
);

enum_from_disp!(
    pub NewUintTypeError,
    [Bitmask, BitmaskError],
    [Bytes, WidthToBytesError]
);

pub struct EndianFloatColumnError(ColumnError<FloatWidthError>);

newtype_disp!(EndianFloatColumnError);

pub struct OrderedFloatColumnError(ColumnError<OrderedFloatError>);

newtype_disp!(OrderedFloatColumnError);

enum_from_disp!(
    pub OrderedFloatError,
    [Order,      ByteOrdToSizedError],
    [WrongWidth, FloatWidthError]
);

enum_from_disp!(
    pub FloatWidthError,
    [Bytes,      WidthToBytesError],
    [WrongWidth, WrongFloatWidth]
);

pub struct WrongFloatWidth {
    pub width: Bytes,
    pub expected: usize,
}

pub type DataReaderResult<T> = DeferredResult<T, NewDataReaderWarning, NewDataReaderError>;

enum_from_disp!(
    pub NewDataReaderError,
    [TotMismatch, TotEventMismatch],
    [ParseTot, ReqKeyError<ParseIntError>],
    [ParseSeg, ReqSegmentWithDefaultError<DataSegmentId>],
    [Width, UnevenEventWidth],
    [Mismatch, SegmentMismatchWarning<DataSegmentId>]
);

enum_from_disp!(
    pub NewDataReaderWarning,
    [TotMismatch, TotEventMismatch],
    [ParseTot, ParseKeyError<ParseIntError>],
    [Layout, NewDataLayoutWarning],
    [Width, UnevenEventWidth],
    [Segment, ReqSegmentWithDefaultWarning<DataSegmentId>]
);

pub(crate) type AnalysisReaderResult<T> =
    DeferredResult<T, NewAnalysisReaderWarning, NewAnalysisReaderError>;

enum_from_disp!(
    pub NewAnalysisReaderError,
    [ParseSeg, ReqSegmentWithDefaultError<AnalysisSegmentId>],
    [Mismatch, SegmentMismatchWarning<AnalysisSegmentId>]
);

enum_from_disp!(
    pub NewAnalysisReaderWarning,
    [Opt, OptSegmentWithDefaultWarning<AnalysisSegmentId>],
    [Req, ReqSegmentWithDefaultWarning<AnalysisSegmentId>]
);

pub struct TotEventMismatch {
    tot: Tot,
    total_events: usize,
}

pub struct UnevenEventWidth {
    event_width: usize,
    nbytes: usize,
    remainder: usize,
}

pub struct ColumnWriterError(ColumnError<AnyLossError>);

newtype_disp!(ColumnWriterError);

enum_from_disp!(
    pub AnyLossError,
    [Int, LossError<BitmaskLossError>],
    [Float, LossError<Infallible>],
    [Ascii, LossError<AsciiLossError>]
);

pub struct AsciiLossError(u8);

impl fmt::Display for AsciiLossError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "ASCII data was too big and truncated into {} chars",
            self.0
        )
    }
}

pub struct BitmaskLossError(u64);

impl fmt::Display for BitmaskLossError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "integer data was too big and truncated to bitmask {}",
            self.0
        )
    }
}

pub struct ColumnError<E> {
    index: MeasIdx,
    error: E,
}

type FromRawResult<T> = DeferredResult<T, RawToLayoutWarning, RawToLayoutError>;

enum_from_disp!(
    pub RawToLayoutError,
    [New, NewDataLayoutError],
    [Raw, RawParsedError]
);

enum_from_disp!(
    pub RawToLayoutWarning,
    [New, NewDataLayoutWarning],
    [Raw, ParseKeyError<NumTypeError>]
);

enum_from_disp!(
    pub RawParsedError,
    [AlphaNumType, ReqKeyError<AlphaNumTypeError>],
    [Endian, ReqKeyError<NewEndianError>],
    [ByteOrd, ReqKeyError<ParseByteOrdError>],
    [Int, ReqKeyError<ParseIntError>],
    [Range, ReqKeyError<ParseFloatOrIntError>]
);

enum_from_disp!(
    pub ReadDataError,
    [Delim, ReadDelimAsciiError],
    [DelimNoRows, ReadDelimAsciiNoRowsError],
    [AlphaNum, AsciiToUintError]
);

enum_from_disp!(
    pub ReadDelimAsciiError,
    [RowsExceeded, RowsExceededError],
    [Incomplete, DelimIncompleteError],
    [Parse, AsciiToUintError]
);

// signify that parsing exceeded max rows
pub struct RowsExceededError(usize);

// signify that a parsing ended in the middle of a row
pub struct DelimIncompleteError {
    col: usize,
    row: usize,
    nrows: usize,
}

pub enum ReadDelimAsciiNoRowsError {
    Unequal,
    Parse(AsciiToUintError),
}

impl fmt::Display for BitmaskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            // TODO what is the target type?
            Self::IntOverrange(x) => {
                write!(
                    f,
                    "integer range {x} is larger than target unsigned integer can hold"
                )
            }
            Self::FloatOverrange(x) => {
                write!(
                    f,
                    "float range {x} is larger than target unsigned integer can hold"
                )
            }
            Self::FloatUnderrange(x) => {
                write!(
                    f,
                    "float range {x} is less than zero and \
                     could not be converted to unsigned integer"
                )
            }
            Self::FloatPrecisionLoss(x) => {
                write!(
                    f,
                    "float range {x} lost precision when converting to unsigned integer"
                )
            }
        }
    }
}

impl fmt::Display for RowsExceededError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "Exceeded expected number of rows: {}", self.0)
    }
}

impl fmt::Display for DelimIncompleteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "Parsing ended in column {} and row {}, where expected number of rows is {}",
            self.col, self.row, self.nrows
        )
    }
}

impl<E> fmt::Display for ColumnError<E>
where
    E: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "error when processing measurement {}: {}",
            self.index, self.error
        )
    }
}

impl fmt::Display for ReadDelimAsciiNoRowsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Self::Unequal => write!(
                f,
                "parsing delimited ASCII without $TOT \
                 resulted in columns with unequal length"
            ),
            Self::Parse(x) => x.fmt(f),
        }
    }
}

impl fmt::Display for NotAsciiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "bytestring is not valid ASCII: {}",
            self.0.iter().join(",")
        )
    }
}

impl fmt::Display for MultiWidthsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "multiple measurement widths found when only one is needed: {}",
            self.0.iter().join(", ")
        )
    }
}

impl fmt::Display for WrongFloatWidth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "expected width to be {} but got {} when determining float type",
            self.expected, self.width,
        )
    }
}

impl fmt::Display for TotEventMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "$TOT field is {} but number of events that evenly fit into DATA is {}",
            self.tot, self.total_events,
        )
    }
}

impl fmt::Display for UnevenEventWidth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "Events are {} bytes wide, but this does not evenly \
             divide DATA segment which is {} bytes long \
             (remainder of {})",
            self.event_width, self.nbytes, self.remainder,
        )
    }
}

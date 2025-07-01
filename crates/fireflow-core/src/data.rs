//! Things pertaining to the DATA segment (mostly)
//!
//! Basic overview: DATA is arranged according to version-specific "layouts".
//! Each layout will enumerate all possible combinations for a given version,
//! which directly correspond to all valid combinations of $BYTEORD, $DATATYPE,
//! $PnB, $PnR, and $PnDATATYPE in the case of 3.2.
//!
//! Each layout may then be projected in a "reader" or "writer." Readers are
//! blank vectors waiting to accept data from disk. Writers are iterators that
//! read values from a dataframe and possibly convert them before writing.
//!
//! Now for the ugly bits.
//!
//! Layouts can first be classified by column width, where "fixed" layouts have
//! a single width per column and "delimited" layouts have a variable width. The
//! latter only corresponds to one layout: the case where $DATATYPE=A and all
//! $PnB=*. Values in such layouts will always be read as u64.
//!
//! Fixed layouts can further be classified by the type in each column:
//! 1) Single-type numeric layouts (aka "matrices")
//! 2) Fixed ASCII layouts
//! 3) Variable-width integer layouts
//! 4) Mixed layouts
//!
//! (1) is the simplest; each column is the same type which corresponds directly
//! with a native Rust type. This includes f32, f64, and uint ranging from 1 to
//! 8 bytes (including those that aren't powers of 2). Each type has a slightly
//! different reader/writer corresponding to distinct byte interpretations on
//! disk. (2) is similar in that the entire layout is one type; however, each
//! number is always read as u64 subject to the chars allowed by $PnB. (1)/(2)
//! are the only possibilities for FCS 2.0/3.0 since $BYTEORD restricts all $PnB
//! to the same width in the case of numeric $DATATYPE.
//!
//! (3) is a weird layout that almost nobody likely uses but is nonetheless
//! permitted starting with 3.1. Since $BYTEORD was changed to only mean
//! endian-ness, its relation to $PnB was severed. When DATATYPE=I, this means
//! $PnB may be changed freely, which allows different integer widths in each
//! column. In practice this makes the resulting data structure a dataframe (vs
//! a matrix).
//!
//! (4) was newly added to 3.2 by way of the PnDATATYPE keywords which now
//! allows the data layout to include any type. This obviously more complex but
//! is not computationally very different from (3).
//!
//! In addition to width, layouts may also be classified by whether $TOT is
//! known. In 2.0, $TOT is optional and may not be given. For delimited ASCII
//! layouts, not have $TOT means we need to parse until we reach the end of
//! DATA, hoping that all columns have the same length. For fixed layouts, we
//! can compute $TOT using $PnB and the length of DATA.

use crate::config::{ReaderConfig, StdTextReadConfig};
use crate::core::*;
use crate::error::*;
use crate::macros::{enum_from, enum_from_disp, match_many_to_one, newtype_disp, newtype_from};
use crate::nonempty::NonEmptyExt;
use crate::segment::*;
use crate::text::byteord::*;
use crate::text::float_or_int::{
    FloatOrInt, FloatRangeError, IntRangeError, NonNanF32, NonNanF64, NonNanFloat,
    ParseFloatOrIntError, ToFloatError,
};
use crate::text::index::{BoundaryIndexError, IndexError, IndexFromOne, MeasIndex};
use crate::text::keywords::*;
use crate::text::optional::{ClearOptional, ClearOptionalOr};
use crate::text::parser::*;
use crate::validated::ascii_range;
use crate::validated::ascii_range::{AsciiRange, Chars};
use crate::validated::bitmask::{
    Bitmask, Bitmask08, Bitmask16, Bitmask24, Bitmask32, Bitmask40, Bitmask48, Bitmask56,
    Bitmask64, BitmaskError,
};
use crate::validated::dataframe::*;
use crate::validated::standard::*;

use itertools::Itertools;
use nonempty::NonEmpty;
use num_traits::float::Float;
use serde::ser::SerializeStruct;
use serde::Serialize;
use std::convert::Infallible;
use std::fmt;
use std::io;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::num::ParseIntError;
use std::str;

/// All possible byte layouts for the DATA segment in 2.0.
///
/// This is identical to 3.0 in every way except that the $TOT keyword in 2.0
/// is optional, which requires a different interface.
#[derive(Clone, Serialize)]
pub struct DataLayout2_0(pub AnyOrderedLayout<MaybeTot>);

newtype_from!(DataLayout2_0, AnyOrderedLayout<MaybeTot>);

/// All possible byte layouts for the DATA segment in 2.0.
#[derive(Clone, Serialize)]
pub struct DataLayout3_0(pub AnyOrderedLayout<KnownTot>);

newtype_from!(DataLayout3_0, AnyOrderedLayout<KnownTot>);

/// All possible byte layouts for the DATA segment in 3.1.
///
/// Unlike 2.0 and 3.0, the integer layout allows the column widths to be
/// different. This is a consequence of making BYTEORD only mean "big or little
/// endian" and have nothing to do with number of bytes.
#[derive(Clone, Serialize)]
pub struct DataLayout3_1(pub NonMixedEndianLayout);

newtype_from!(DataLayout3_1, NonMixedEndianLayout);

enum_from!(
    /// All possible byte layouts for the DATA segment in 3.2.
    ///
    /// In addition to the loosened integer layouts in 3.1, 3.2 additionally allows
    /// each column to have a different type and size (hence "Mixed").
    #[derive(Clone, Serialize)]
    pub DataLayout3_2,
    [Mixed, EndianLayout<NullMixedType>],
    [NonMixed, NonMixedEndianLayout]
);

/// All possible byte layouts for the DATA segment in 2.0 and 3.0.
///
/// It is so named "Ordered" because the BYTEORD keyword represents any possible
/// byte ordering that may occur rather than simply little or big endian.
#[derive(Clone, Serialize)]
pub enum AnyOrderedLayout<T> {
    Ascii(AnyAsciiLayout<T>),
    Integer(AnyOrderedUintLayout<T>),
    F32(OrderedLayout<F32Range, T>),
    F64(OrderedLayout<F64Range, T>),
}

// TODO make an integer layout which has only one width, which will cover the
// vast majority of cases and make certain operations easier.
enum_from!(
    #[derive(Clone, Serialize)]
    pub NonMixedEndianLayout,
    [Ascii, AnyAsciiLayout<KnownTot>],
    [Integer, EndianLayout<AnyNullBitmask>],
    [F32, EndianLayout<F32Range>],
    [F64, EndianLayout<F64Range>]
);

type EndianLayout<C> = FixedLayout<C, Endian, KnownTot>;

/// Byte layouts for ASCII data.
///
/// This may either be fixed (ie columns have the same number of characters)
/// or variable (ie columns have have different number of characters and are
/// separated by delimiters).
#[derive(Clone, Serialize)]
pub enum AnyAsciiLayout<T> {
    Delimited(DelimAsciiLayout<T>),
    Fixed(FixedAsciiLayout<T>),
}

type FixedAsciiLayout<T> = FixedLayout<AsciiRange, (), T>;

/// Byte layout for delimited ASCII.
#[derive(Clone)]
pub struct DelimAsciiLayout<T> {
    pub ranges: NonEmpty<u64>,
    tot_action: PhantomData<T>,
}

/// Byte layout where each column has a fixed width.
#[derive(Clone)]
pub struct FixedLayout<C, L, T> {
    byte_layout: L,
    columns: NonEmpty<C>,
    tot_action: PhantomData<T>,
}

/// Byte layout for integers that may be in any byte order.
#[derive(Clone, Serialize)]
pub enum AnyOrderedUintLayout<T> {
    // TODO the first two don't need to be ordered
    Uint08(OrderedLayout<Bitmask08, T>),
    Uint16(OrderedLayout<Bitmask16, T>),
    Uint24(OrderedLayout<Bitmask24, T>),
    Uint32(OrderedLayout<Bitmask32, T>),
    Uint40(OrderedLayout<Bitmask40, T>),
    Uint48(OrderedLayout<Bitmask48, T>),
    Uint56(OrderedLayout<Bitmask56, T>),
    Uint64(OrderedLayout<Bitmask64, T>),
}

type OrderedLayout<C, T> = FixedLayout<C, <C as HasNativeWidth>::Order, T>;

macro_rules! into_any_ordered_layout {
    ($var:ident, $inner:ident) => {
        impl<T> From<OrderedLayout<$inner, T>> for AnyOrderedUintLayout<T> {
            fn from(value: OrderedLayout<$inner, T>) -> Self {
                Self::$var(value)
            }
        }
    };
}

into_any_ordered_layout!(Uint08, Bitmask08);
into_any_ordered_layout!(Uint16, Bitmask16);
into_any_ordered_layout!(Uint24, Bitmask24);
into_any_ordered_layout!(Uint32, Bitmask32);
into_any_ordered_layout!(Uint40, Bitmask40);
into_any_ordered_layout!(Uint48, Bitmask48);
into_any_ordered_layout!(Uint56, Bitmask56);
into_any_ordered_layout!(Uint64, Bitmask64);

/// The type of a non-delimited column in the DATA segment for 3.2
pub enum MixedType<F: ColumnFamily> {
    Ascii(F::ColumnWrapper<AsciiRange, u64, ()>),
    Uint(AnyBitmask<F>),
    F32(NativeWrapper<F, F32Range>),
    F64(NativeWrapper<F, F64Range>),
}

type NullMixedType = MixedType<ColumnNullFamily>;
type ReaderMixedType = MixedType<ColumnReaderFamily>;
type WriterMixedType<'a> = MixedType<ColumnWriterFamily<'a>>;

/// A big or little-endian integer column of some size (1-8 bytes)
pub enum AnyBitmask<F: ColumnFamily> {
    Uint08(NativeWrapper<F, Bitmask08>),
    Uint16(NativeWrapper<F, Bitmask16>),
    Uint24(NativeWrapper<F, Bitmask24>),
    Uint32(NativeWrapper<F, Bitmask32>),
    Uint40(NativeWrapper<F, Bitmask40>),
    Uint48(NativeWrapper<F, Bitmask48>),
    Uint56(NativeWrapper<F, Bitmask56>),
    Uint64(NativeWrapper<F, Bitmask64>),
}

type AnyNullBitmask = AnyBitmask<ColumnNullFamily>;
type AnyReaderBitmask = AnyBitmask<ColumnReaderFamily>;
type AnyWriterBitmask<'a> = AnyBitmask<ColumnWriterFamily<'a>>;

/// The type of any floating point column in all versions
#[derive(PartialEq, Clone, Copy, Serialize)]
pub struct FloatRange<T, const LEN: usize> {
    pub range: NonNanFloat<T>,
}

pub type F32Range = FloatRange<f32, 4>;
pub type F64Range = FloatRange<f64, 8>;

/// Instructions to read one column and store in a vector
struct ColumnReader<C, T, S> {
    column_type: C,
    data: Vec<T>,
    byte_layout: PhantomData<S>,
}

type UintColumnReader<C> = ColumnReader<C, <C as HasNativeType>::Native, Endian>;

/// Instructions to write one column using an iterator
struct ColumnWriter<'a, C, T, S> {
    column_type: C,
    data: AnySource<'a, T>,
    byte_layout: PhantomData<S>,
}

type UintColumnWriter<'a, C> = ColumnWriter<'a, C, <C as HasNativeType>::Native, Endian>;

/// Marker type for columns which are used in a layout (non-reader/writer)
pub struct ColumnNullFamily;

/// Marker type for columns which are in a layout and have data for reading
struct ColumnReaderFamily;

/// Marker type for columns which are in a layout and have data for writing
struct ColumnWriterFamily<'a>(std::marker::PhantomData<&'a ()>);

/// Marker type for layouts that might have $TOT
#[derive(Clone, Serialize)]
pub struct MaybeTot;

/// Marker type for layouts that always have $TOT
#[derive(Clone, Serialize)]
pub struct KnownTot;

// /// A struct whose fields map 1-1 with keyword values pertaining to data layout.
// pub struct LayoutValues<S, D> {
//     datatype: AlphaNumType,
//     byte_layout: S,
//     columns: Vec<ColumnLayoutValues<D>>,
// }

// type OrderedLayoutValues = LayoutValues<ByteOrd, ()>;
// type LayoutValues3_1 = LayoutValues<Endian, ()>;
// type LayoutValues3_2 = LayoutValues<Endian, Option<NumType>>;

/// A struct whose fields map 1-1 with keyword values in one data column
pub struct ColumnLayoutValues<D> {
    width: Width,
    range: Range,
    datatype: D,
}

type ColumnLayoutValues2_0 = ColumnLayoutValues<()>;
type ColumnLayoutValues3_2 = ColumnLayoutValues<Option<NumType>>;

// pub struct UintColumnValues {
//     width: Bytes,
//     range: Range,
// }

// pub enum MixedColumnValues {
//     Float(NonNanF32),
//     Double(NonNanF64),
//     Ascii(AsciiRange),
//     Uint(UintColumnValues),
// }

/// A type which represents a column which may have associated data.
///
/// Used to implement a higher-kinded type interface for columns that can be
/// by themselves or associated with reader or writer data.
pub trait ColumnFamily {
    type ColumnWrapper<C, T, S>;
}

type NativeWrapper<F, C> =
    <F as ColumnFamily>::ColumnWrapper<C, <C as HasNativeType>::Native, Endian>;

/// Methods for a type which may or may not have $TOT
trait TotDefinition {
    type Tot;

    fn with_tot<F, G, I, X>(input: I, tot: Self::Tot, tot_f: F, notot_f: G) -> X
    where
        F: FnOnce(I, Tot) -> X,
        G: FnOnce(I) -> X;

    fn check_tot(
        total_events: usize,
        tot: Self::Tot,
        allow_mismatch: bool,
    ) -> BiTentative<(), TotEventMismatch> {
        Self::with_tot(
            (),
            tot,
            |_, t| Self::check_tot_inner(total_events, t, allow_mismatch),
            |_| Tentative::new1(()),
        )
    }

    fn check_tot_inner(
        total_events: usize,
        tot: Tot,
        allow_mismatch: bool,
    ) -> BiTentative<(), TotEventMismatch> {
        if tot.0 != total_events {
            let i = TotEventMismatch { tot, total_events };
            Tentative::new_either((), vec![i], !allow_mismatch)
        } else {
            Tentative::new1(())
        }
    }
}

/// A version-specific data layout
pub trait VersionedDataLayout: Sized {
    type ByteLayout;
    type ColDatatype;
    type Tot;

    fn try_new(
        dt: AlphaNumType,
        size: Self::ByteLayout,
        cs: NonEmpty<ColumnLayoutValues<Self::ColDatatype>>,
        conf: &StdTextReadConfig,
    ) -> DeferredResult<Self, ColumnError<NewMixedTypeWarning>, NewDataLayoutError>;

    fn lookup(
        kws: &mut StdKeywords,
        conf: &StdTextReadConfig,
        par: Par,
    ) -> LookupLayoutResult<Option<Self>>;

    fn lookup_ro(kws: &StdKeywords, conf: &StdTextReadConfig) -> FromRawResult<Option<Self>>;

    // fn insert(
    //     &mut self,
    //     index: MeasIndex,
    //     range: Range,
    //     notrunc: bool,
    // ) -> DeferredResult<(), LayoutInsertColumnWarning, LayoutInsertColumnError>;

    fn insert_nocheck(
        &mut self,
        index: MeasIndex,
        range: FloatOrInt,
        notrunc: bool,
    ) -> BiTentative<(), LayoutInsertColumnWarning>;

    fn push(&mut self, range: FloatOrInt, notrunc: bool) -> BiTentative<(), LayoutPushColumnError>;

    fn remove(&mut self, index: MeasIndex) -> Result<(), ClearOptionalOr<IndexError>>;

    fn remove_nocheck(&mut self, index: MeasIndex) -> Result<(), ClearOptional>;

    fn ncols(&self) -> usize;

    fn nbytes(&self, df: &FCSDataFrame) -> u64;

    fn h_read_df<R: Read + Seek>(
        &self,
        h: &mut BufReader<R>,
        tot: Self::Tot,
        seg: AnyDataSegment,
        conf: &ReaderConfig,
    ) -> IODeferredResult<FCSDataFrame, ReadDataframeWarning, ReadDataframeError> {
        seg.inner.as_u64().try_coords().map_or(
            Ok(Tentative::new1(FCSDataFrame::default())),
            |(begin, _)| {
                h.seek(SeekFrom::Start(begin)).into_deferred()?;
                self.h_read_df_inner(h, tot, seg, conf)
            },
        )
    }

    fn h_read_df_inner<R: Read>(
        &self,
        h: &mut BufReader<R>,
        tot: Self::Tot,
        seg: AnyDataSegment,
        conf: &ReaderConfig,
    ) -> IODeferredResult<FCSDataFrame, ReadDataframeWarning, ReadDataframeError>;

    fn check_writer(&self, df: &FCSDataFrame) -> MultiResult<(), ColumnError<AnyLossError>>;

    fn h_write_df<W>(&self, h: &mut BufWriter<W>, df: &FCSDataFrame) -> io::Result<()>
    where
        W: Write,
    {
        // The dataframe should be encapsulated such that a) the column number
        // matches the number of measurements. If these are not true, the code
        // is wrong.
        let par = self.ncols();
        let ncols = df.ncols();
        if ncols != par {
            panic!("datafame columns ({ncols}) unequal to number of measurements ({par})");
        }
        self.h_write_df_inner(h, df)
    }

    fn h_write_df_inner<W: Write>(&self, h: &mut BufWriter<W>, df: &FCSDataFrame)
        -> io::Result<()>;

    // fn layout_values(&self) -> LayoutValues<Self::ByteLayout, Self::ColDatatype>;

    fn req_keywords(&self) -> [(String, String); 2];

    fn req_meas_keywords(&self) -> NonEmpty<(String, String, String)>;

    fn opt_meas_keywords(&self) -> Vec<(String, String, Option<String>)>;
}

pub trait HasNativeType: Sized {
    /// The native rust type
    type Native: Default + Copy;
}

/// A type which uses a defined number of bytes
pub trait HasNativeWidth: HasNativeType {
    /// The length of the type in an FCS file (may be less than native)
    const BYTES: Bytes;

    /// The length of the native Rust type
    const LEN: usize;

    /// The sized byte order to be used with this type
    type Order;
}

/// A column which has a $DATATYPE keyword
pub trait HasDatatype {
    const DATATYPE: AlphaNumType;
}

/// A type which has a width that may vary
pub trait IsFixed {
    fn nbytes(&self) -> u8;

    fn fixed_width(&self) -> BitsOrChars;

    fn range(&self) -> Range;

    fn req_meas_keywords(&self, i: MeasIndex) -> [(String, String, String); 2] {
        [
            Width::Fixed(self.fixed_width()).triple(i.into()),
            self.range().triple(i.into()),
        ]
    }
}

/// A column which may be transformed into a reader for a rust numeric type
trait ToNativeReader: HasNativeType {
    fn into_reader<S>(self, nrows: usize) -> ColumnReader<Self, Self::Native, S>
    where
        Self::Native: Default + Copy,
    {
        ColumnReader {
            column_type: self,
            data: vec![Self::Native::default(); nrows],
            byte_layout: PhantomData,
        }
    }
}

/// A column which may be transformed into a writer for a rust numeric type
trait ToNativeWriter
where
    Self: HasNativeType,
{
    type Error;

    fn into_writer<'a, S>(self, c: &'a AnyFCSColumn) -> ColumnWriter<'a, Self, Self::Native, S>
    where
        Self::Native: Default + Copy + AllFCSCast,
        AnySource<'a, Self::Native>: From<FCSColIter<'a, u8, Self::Native>>
            + From<FCSColIter<'a, u16, Self::Native>>
            + From<FCSColIter<'a, u32, Self::Native>>
            + From<FCSColIter<'a, u64, Self::Native>>
            + From<FCSColIter<'a, f32, Self::Native>>
            + From<FCSColIter<'a, f64, Self::Native>>,
    {
        ColumnWriter {
            column_type: self,
            data: AnySource::new::<Self::Native>(c),
            byte_layout: PhantomData,
        }
    }

    fn check_writer(&self, col: &AnyFCSColumn) -> Result<(), LossError<Self::Error>>
    where
        Self::Native: Default + Copy + AllFCSCast,
    {
        col.check_writer(|x| Self::check_other_loss(self, x))
    }

    fn check_other_loss(&self, x: Self::Native) -> Option<Self::Error>;
}

// TODO can't this just be with the native reader type?
trait NativeReadable<S, E>: HasNativeType {
    type Buf;

    fn h_read<R: Read>(
        &self,
        h: &mut BufReader<R>,
        byte_layout: S,
        buf: &mut Self::Buf,
    ) -> IOResult<Self::Native, E>;
}

trait Readable<S, E> {
    type Inner;
    type Buf;

    fn new(column_type: Self::Inner, nrows: usize) -> Self;

    fn into_column(self) -> AnyFCSColumn;

    fn h_read_row<R: Read>(
        &mut self,
        h: &mut BufReader<R>,
        row: usize,
        byte_layout: S,
        buf: &mut Self::Buf,
    ) -> IOResult<(), E>;
}

trait NativeWritable<S>: HasNativeType {
    fn h_write<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        x: CastResult<Self::Native>,
        byte_layout: S,
    ) -> io::Result<()>;
}

trait Writable<'a, S> {
    type Inner;

    fn new(column_type: Self::Inner, col: &'a AnyFCSColumn) -> Self;

    fn check_writer(column_type: Self::Inner, col: &'a AnyFCSColumn) -> Result<(), AnyLossError>;

    fn h_write<W: Write>(&mut self, h: &mut BufWriter<W>, byte_layout: S) -> io::Result<()>;
}

/// General methods for each numeric type.
///
/// This is mostly for converting to/from bytes with various endian-ness.
// TODO clean this up with https://github.com/rust-lang/rust/issues/76560 once
// it lands in a stable compiler, in theory there is no reason to put the length
// of the type as a parameter, but the current compiler is not smart enough
trait NumProps: Sized + Copy + Default {
    const LEN: usize;
    type BUF: AsRef<[u8]> + AsMut<[u8]> + Default;

    fn read_buf<R: Read>(h: &mut BufReader<R>) -> io::Result<Self::BUF>;

    fn from_big(buf: Self::BUF) -> Self;

    fn from_little(buf: Self::BUF) -> Self;

    fn from_endian(buf: Self::BUF, endian: Endian) -> Self {
        match endian {
            Endian::Big => Self::from_big(buf),
            Endian::Little => Self::from_little(buf),
        }
    }

    fn to_big(self) -> Self::BUF;

    fn to_little(self) -> Self::BUF;

    fn to_endian(self, endian: Endian) -> Self::BUF {
        match endian {
            Endian::Big => self.to_big(),
            Endian::Little => self.to_little(),
        }
    }
}

/// Methods for reading numbers which may be in arbitrary byte orders.
trait OrderedFromBytes<const OLEN: usize>: NumProps {
    fn h_read_from_ordered<R: Read>(h: &mut BufReader<R>, order: [u8; OLEN]) -> io::Result<Self> {
        let mut tmp = [0; OLEN];
        let mut buf = Self::BUF::default();
        h.read_exact(&mut tmp)?;
        for (i, j) in order.iter().enumerate() {
            buf.as_mut()[usize::from(*j)] = tmp[i];
        }
        Ok(Self::from_little(buf))
    }

    fn h_write_from_ordered<W: Write>(
        self,
        h: &mut BufWriter<W>,
        order: [u8; OLEN],
    ) -> io::Result<()> {
        let tmp = Self::to_little(self);
        let mut buf = [0; OLEN];
        for (i, j) in order.iter().enumerate() {
            buf[usize::from(*j)] = tmp.as_ref()[i];
        }
        h.write_all(tmp.as_ref())
    }
}

/// Methods for reading/writing integers (1-8 bytes) from FCS files.
trait IntFromBytes<const INTLEN: usize>: NumProps + OrderedFromBytes<INTLEN> {
    fn h_read_endian<R: Read>(h: &mut BufReader<R>, endian: Endian) -> io::Result<Self> {
        // This will read data that is not a power-of-two bytes long. Start by
        // reading n bytes into a vector, which can take a varying size. Then
        // copy this into the power of 2 buffer and reset all the unused cells
        // to 0. This copy has to go to one or the other end of the buffer
        // depending on endianness.
        let mut tmp = [0; INTLEN];
        let mut buf = Self::BUF::default();
        h.read_exact(&mut tmp)?;
        Ok(if endian == Endian::Big {
            let b = Self::LEN - INTLEN;
            buf.as_mut()[b..].copy_from_slice(&tmp[b..]);
            Self::from_big(buf)
        } else {
            buf.as_mut()[..INTLEN].copy_from_slice(&tmp[..INTLEN]);
            Self::from_little(buf)
        })
    }

    fn h_read_ordered<R: Read>(
        h: &mut BufReader<R>,
        byteord: SizedByteOrd<INTLEN>,
    ) -> io::Result<Self> {
        match byteord {
            SizedByteOrd::Endian(e) => Self::h_read_endian(h, e),
            SizedByteOrd::Order(order) => Self::h_read_from_ordered(h, order),
        }
    }

    fn h_write_endian<W: Write>(self, h: &mut BufWriter<W>, endian: Endian) -> io::Result<()> {
        let mut buf = [0; INTLEN];
        let (start, end, tmp) = if endian == Endian::Big {
            ((Self::LEN - INTLEN), Self::LEN, Self::to_big(self))
        } else {
            (0, INTLEN, Self::to_little(self))
        };
        buf[..].copy_from_slice(&tmp.as_ref()[start..end]);
        h.write_all(&buf)
    }

    fn h_write_ordered<W: Write>(
        self,
        h: &mut BufWriter<W>,
        byteord: SizedByteOrd<INTLEN>,
    ) -> io::Result<()> {
        match byteord {
            SizedByteOrd::Endian(e) => self.h_write_endian(h, e),
            SizedByteOrd::Order(o) => self.h_write_from_ordered(h, o),
        }
    }
}

/// Methods for reading/writing floats (32 and 64 bit) from FCS files.
trait FloatFromBytes<const LEN: usize>: NumProps + OrderedFromBytes<LEN> {
    fn h_read_endian<R: Read>(h: &mut BufReader<R>, endian: Endian) -> io::Result<Self> {
        let buf = Self::read_buf(h)?;
        Ok(Self::from_endian(buf, endian))
    }

    fn h_read_ordered<R: Read>(
        h: &mut BufReader<R>,
        byteord: SizedByteOrd<LEN>,
    ) -> io::Result<Self> {
        match byteord {
            SizedByteOrd::Endian(endian) => Self::h_read_endian(h, endian),
            SizedByteOrd::Order(order) => Self::h_read_from_ordered(h, order),
        }
    }

    fn h_write_endian<W: Write>(self, h: &mut BufWriter<W>, endian: Endian) -> io::Result<()> {
        let buf = Self::to_endian(self, endian);
        h.write_all(buf.as_ref())
    }

    fn h_write_ordered<W: Write>(
        self,
        h: &mut BufWriter<W>,
        byteord: SizedByteOrd<LEN>,
    ) -> io::Result<()> {
        match byteord {
            SizedByteOrd::Endian(endian) => self.h_write_endian(h, endian),
            SizedByteOrd::Order(order) => self.h_write_from_ordered(h, order),
        }
    }
}

/// Methods for converting between FCS TEXT keywords and column layouts.
pub(crate) trait VersionedColumnLayout: Sized {
    fn lookup_all(kws: &mut StdKeywords, par: Par) -> LookupResult<Vec<Self>> {
        (0..par.0)
            .map(|i| Self::lookup(kws, i.into()))
            .gather()
            .map(Tentative::mconcat)
            .map_err(DeferredFailure::mconcat)
    }

    fn lookup_ro_all(
        kws: &StdKeywords,
    ) -> DeferredResult<Vec<Self>, ParseKeyError<NumTypeError>, RawParsedError> {
        Par::get_metaroot_req(kws)
            .into_deferred()
            .def_and_maybe(|par| {
                (0..par.0)
                    .map(|i| Self::get(kws, i.into()))
                    .gather()
                    .map(Tentative::mconcat)
                    .map_err(DeferredFailure::mconcat)
            })
    }

    fn lookup(kws: &mut StdKeywords, i: MeasIndex) -> LookupResult<Self>;

    fn get(
        kws: &StdKeywords,
        i: MeasIndex,
    ) -> DeferredResult<Self, ParseKeyError<NumTypeError>, RawParsedError>;

    // fn req_keywords(&self, i: MeasIndex) -> impl Iterator<Item = (String, String, String)>;

    // fn opt_keywords(&self, i: MeasIndex) -> impl Iterator<Item = (String, String, Option<String>)>;
}

macro_rules! impl_null_layout {
    ($t:path, $($var:ident),*) => {
        impl Copy for $t {}

        impl Clone for $t {
            fn clone(&self) -> Self {
                *self
            }
        }

        impl Serialize for $t {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                match self {
                    $(
                        Self::$var(x) => x.serialize(serializer),
                    )*
                }
            }
        }
    };
}

impl_null_layout!(NullMixedType, Ascii, Uint, F32, F64);

impl_null_layout!(
    AnyNullBitmask,
    Uint08,
    Uint16,
    Uint24,
    Uint32,
    Uint40,
    Uint48,
    Uint56,
    Uint64
);

macro_rules! any_uint_from {
    ($var:ident, $inner:path) => {
        impl From<$inner> for AnyNullBitmask {
            fn from(value: $inner) -> Self {
                Self::$var(value)
            }
        }

        impl From<UintColumnReader<$inner>> for AnyReaderBitmask {
            fn from(value: UintColumnReader<$inner>) -> Self {
                Self::$var(value)
            }
        }

        impl<'a> From<UintColumnWriter<'a, $inner>> for AnyWriterBitmask<'a> {
            fn from(value: UintColumnWriter<'a, $inner>) -> Self {
                Self::$var(value)
            }
        }
    };
}

any_uint_from!(Uint08, Bitmask08);
any_uint_from!(Uint16, Bitmask16);
any_uint_from!(Uint24, Bitmask24);
any_uint_from!(Uint32, Bitmask32);
any_uint_from!(Uint40, Bitmask40);
any_uint_from!(Uint48, Bitmask48);
any_uint_from!(Uint56, Bitmask56);
any_uint_from!(Uint64, Bitmask64);

impl TotDefinition for MaybeTot {
    type Tot = Option<Tot>;

    fn with_tot<F, G, I, X>(input: I, tot: Self::Tot, tot_f: F, notot_f: G) -> X
    where
        F: FnOnce(I, Tot) -> X,
        G: FnOnce(I) -> X,
    {
        if let Some(t) = tot {
            tot_f(input, t)
        } else {
            notot_f(input)
        }
    }
}

impl TotDefinition for KnownTot {
    type Tot = Tot;

    fn with_tot<F, G, I, X>(input: I, tot: Self::Tot, tot_f: F, _: G) -> X
    where
        F: FnOnce(I, Tot) -> X,
        G: FnOnce(I) -> X,
    {
        tot_f(input, tot)
    }
}

impl ColumnFamily for ColumnNullFamily {
    type ColumnWrapper<C, T, S> = C;
}

impl ColumnFamily for ColumnReaderFamily {
    type ColumnWrapper<C, T, S> = ColumnReader<C, T, S>;
}

impl<'a> ColumnFamily for ColumnWriterFamily<'a> {
    type ColumnWrapper<C, T, S> = ColumnWriter<'a, C, T, S>;
}

macro_rules! any_uint_to_width {
    ($from:ident, $to:ident) => {
        impl TryFrom<AnyNullBitmask> for $to {
            type Error = UintToUintError;
            fn try_from(value: AnyNullBitmask) -> Result<Self, Self::Error> {
                let w = value.nbytes();
                if let AnyBitmask::$from(x) = value {
                    Ok(x)
                } else {
                    Err(UintToUintError {
                        from: w,
                        to: Self::BYTES.into(),
                    })
                }
            }
        }
    };
}

any_uint_to_width!(Uint08, Bitmask08);
any_uint_to_width!(Uint16, Bitmask16);
any_uint_to_width!(Uint24, Bitmask24);
any_uint_to_width!(Uint32, Bitmask32);
any_uint_to_width!(Uint40, Bitmask40);
any_uint_to_width!(Uint48, Bitmask48);
any_uint_to_width!(Uint56, Bitmask56);
any_uint_to_width!(Uint64, Bitmask64);

macro_rules! mixed_to_width {
    ($from:ident, $to:ident) => {
        impl TryFrom<NullMixedType> for $to {
            type Error = MixedToOrderedUintError;
            fn try_from(value: NullMixedType) -> Result<Self, Self::Error> {
                let w = value.nbytes();
                match value {
                    MixedType::Uint(x) => {
                        if let AnyBitmask::$from(y) = x {
                            Ok(y)
                        } else {
                            Err(UintToUintError {
                                from: w,
                                to: Self::BYTES.into(),
                            }
                            .into())
                        }
                    }
                    MixedType::Ascii(_) => Err(MixedIsAscii.into()),
                    MixedType::F32(_) => Err(MixedIsFloat.into()),
                    MixedType::F64(_) => Err(MixedIsDouble.into()),
                }
            }
        }
    };
}

mixed_to_width!(Uint08, Bitmask08);
mixed_to_width!(Uint16, Bitmask16);
mixed_to_width!(Uint24, Bitmask24);
mixed_to_width!(Uint32, Bitmask32);
mixed_to_width!(Uint40, Bitmask40);
mixed_to_width!(Uint48, Bitmask48);
mixed_to_width!(Uint56, Bitmask56);
mixed_to_width!(Uint64, Bitmask64);

impl TryFrom<NullMixedType> for AsciiRange {
    type Error = MixedToAsciiError;
    fn try_from(value: NullMixedType) -> Result<Self, Self::Error> {
        match value {
            MixedType::Ascii(x) => Ok(x),
            MixedType::Uint(x) => Err(MixedIsInteger { width: x.nbytes() }.into()),
            MixedType::F32(_) => Err(MixedIsFloat.into()),
            MixedType::F64(_) => Err(MixedIsDouble.into()),
        }
    }
}

impl TryFrom<NullMixedType> for AnyNullBitmask {
    type Error = MixedToEndianUintError;
    fn try_from(value: NullMixedType) -> Result<Self, Self::Error> {
        match value {
            MixedType::Ascii(_) => Err(MixedIsAscii.into()),
            MixedType::Uint(x) => Ok(x),
            MixedType::F32(_) => Err(MixedIsFloat.into()),
            MixedType::F64(_) => Err(MixedIsDouble.into()),
        }
    }
}

impl TryFrom<NullMixedType> for F32Range {
    type Error = MixedToFloatError;
    fn try_from(value: NullMixedType) -> Result<Self, Self::Error> {
        match value {
            MixedType::Ascii(_) => Err(MixedIsAscii.into()),
            MixedType::Uint(x) => Err(MixedIsInteger { width: x.nbytes() }.into()),
            MixedType::F32(x) => Ok(x),
            MixedType::F64(_) => Err(MixedIsDouble.into()),
        }
    }
}

impl TryFrom<NullMixedType> for F64Range {
    type Error = MixedToDoubleError;
    fn try_from(value: NullMixedType) -> Result<Self, Self::Error> {
        match value {
            MixedType::Ascii(_) => Err(MixedIsAscii.into()),
            MixedType::Uint(x) => Err(MixedIsInteger { width: x.nbytes() }.into()),
            MixedType::F32(_) => Err(MixedIsFloat.into()),
            MixedType::F64(x) => Ok(x),
        }
    }
}

enum_from!(
    pub MixedToOrderedUintError,
    [IsAscii, MixedIsAscii],
    [IsWrongInteger, UintToUintError],
    [IsFloat, MixedIsFloat],
    [IsDouble, MixedIsDouble]
);

impl fmt::Display for MixedToOrderedUintError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Self::IsWrongInteger(e) => write!(
                f,
                "could not convert mixed from {}- to {}-byte integer",
                e.from, e.to
            ),
            Self::IsAscii(e) => write!(f, "could not convert mixed from {e} to integer"),
            Self::IsFloat(e) => write!(f, "could not convert mixed from {e} to integer"),
            Self::IsDouble(e) => write!(f, "could not convert mixed from {e} to integer"),
        }
    }
}

enum_from!(
    pub MixedToEndianUintError,
    [IsAscii, MixedIsAscii],
    [IsFloat, MixedIsFloat],
    [IsDouble, MixedIsDouble]
);

impl fmt::Display for MixedToEndianUintError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Self::IsAscii(e) => write!(f, "could not convert mixed from {e} to integer"),
            Self::IsFloat(e) => write!(f, "could not convert mixed from {e} to integer"),
            Self::IsDouble(e) => write!(f, "could not convert mixed from {e} to integer"),
        }
    }
}

enum_from!(
    pub MixedToAsciiError,
    [IsInteger, MixedIsInteger],
    [IsFloat, MixedIsFloat],
    [IsDouble, MixedIsDouble]
);

impl fmt::Display for MixedToAsciiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let e = match self {
            Self::IsInteger(e) => e.to_string(),
            Self::IsFloat(e) => e.to_string(),
            Self::IsDouble(e) => e.to_string(),
        };
        write!(f, "could not convert mixed from {e} to ASCII")
    }
}

enum_from!(
    pub MixedToFloatError,
    [IsAscii, MixedIsAscii],
    [IsInteger, MixedIsInteger],
    [IsDouble, MixedIsDouble]
);

impl fmt::Display for MixedToFloatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let e = match self {
            Self::IsAscii(e) => e.to_string(),
            Self::IsInteger(e) => e.to_string(),
            Self::IsDouble(e) => e.to_string(),
        };
        write!(f, "could not convert mixed from {e} to float")
    }
}

enum_from!(
    pub MixedToDoubleError,
    [IsAscii, MixedIsAscii],
    [IsInteger, MixedIsInteger],
    [IsFloat, MixedIsFloat]
);

impl fmt::Display for MixedToDoubleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let e = match self {
            Self::IsAscii(e) => e.to_string(),
            Self::IsInteger(e) => e.to_string(),
            Self::IsFloat(e) => e.to_string(),
        };
        write!(f, "could not convert mixed from {e} to double")
    }
}

impl<T, const LEN: usize> ToNativeReader for Bitmask<T, LEN> where Self: HasNativeType<Native = T> {}

impl<T, const LEN: usize> ToNativeReader for FloatRange<T, LEN> where Self: HasNativeType<Native = T>
{}

impl ToNativeReader for AsciiRange {}

impl<T, const LEN: usize, E> NativeReadable<Endian, E> for Bitmask<T, LEN>
where
    Bitmask<T, LEN>: HasNativeType<Native = T>,
    T: Ord + Copy + IntFromBytes<LEN>,
{
    type Buf = ();

    fn h_read<R: Read>(
        &self,
        h: &mut BufReader<R>,
        byte_layout: Endian,
        _: &mut (),
    ) -> IOResult<T, E> {
        let x = T::h_read_endian(h, byte_layout)?;
        Ok(x)
    }
}

impl<T, const LEN: usize, E> NativeReadable<SizedByteOrd<LEN>, E> for Bitmask<T, LEN>
where
    Bitmask<T, LEN>: HasNativeType<Native = T>,
    T: Ord + Copy + IntFromBytes<LEN>,
{
    type Buf = ();

    fn h_read<R: Read>(
        &self,
        h: &mut BufReader<R>,
        byte_layout: SizedByteOrd<LEN>,
        _: &mut (),
    ) -> IOResult<T, E> {
        let x = T::h_read_ordered(h, byte_layout)?;
        Ok(x)
    }
}

impl<T, const LEN: usize, E> NativeReadable<Endian, E> for FloatRange<T, LEN>
where
    FloatRange<T, LEN>: HasNativeType<Native = T>,
    T: Copy + FloatFromBytes<LEN>,
{
    type Buf = ();

    fn h_read<R: Read>(
        &self,
        h: &mut BufReader<R>,
        byte_layout: Endian,
        _: &mut (),
    ) -> IOResult<T, E> {
        let x = T::h_read_endian(h, byte_layout)?;
        Ok(x)
    }
}

impl<T, const LEN: usize, E> NativeReadable<SizedByteOrd<LEN>, E> for FloatRange<T, LEN>
where
    FloatRange<T, LEN>: HasNativeType<Native = T>,
    T: Copy + FloatFromBytes<LEN>,
{
    type Buf = ();

    fn h_read<R: Read>(
        &self,
        h: &mut BufReader<R>,
        byte_layout: SizedByteOrd<LEN>,
        _: &mut (),
    ) -> IOResult<T, E> {
        let x = T::h_read_ordered(h, byte_layout)?;
        Ok(x)
    }
}

impl NativeReadable<(), AsciiToUintError> for AsciiRange {
    type Buf = Vec<u8>;

    fn h_read<R: Read>(
        &self,
        h: &mut BufReader<R>,
        _: (),
        buf: &mut Vec<u8>,
    ) -> IOResult<Self::Native, AsciiToUintError> {
        buf.clear();
        h.take(u8::from(self.chars()).into()).read_to_end(buf)?;
        ascii_to_uint(buf).map_err(ImpureError::Pure)
    }
}

impl<C, T, S, E> Readable<S, E> for ColumnReader<C, T, S>
where
    T: Copy + Default,
    C: NativeReadable<S, E> + HasNativeType<Native = T> + ToNativeReader,
    AnyFCSColumn: From<FCSColumn<T>>,
{
    type Inner = C;
    type Buf = <C as NativeReadable<S, E>>::Buf;

    fn new(column_type: Self::Inner, nrows: usize) -> Self {
        column_type.into_reader(nrows)
    }

    fn into_column(self) -> AnyFCSColumn {
        FCSColumn::from(self.data).into()
    }

    fn h_read_row<R: Read>(
        &mut self,
        h: &mut BufReader<R>,
        row: usize,
        byte_layout: S,
        buf: &mut Self::Buf,
    ) -> IOResult<(), E> {
        self.data[row] = self.column_type.h_read(h, byte_layout, buf)?;
        Ok(())
    }
}

impl Readable<Endian, AsciiToUintError> for ReaderMixedType {
    type Inner = NullMixedType;
    type Buf = Vec<u8>;

    fn new(column_type: Self::Inner, nrows: usize) -> Self {
        match column_type {
            MixedType::Ascii(c) => Self::Ascii(c.into_reader(nrows)),
            MixedType::Uint(c) => Self::Uint(Readable::<_, AsciiToUintError>::new(c, nrows)),
            MixedType::F32(c) => Self::F32(c.into_reader(nrows)),
            MixedType::F64(c) => Self::F64(c.into_reader(nrows)),
        }
    }

    fn into_column(self) -> AnyFCSColumn {
        match self {
            MixedType::Ascii(c) => c.into_column(),
            MixedType::Uint(c) => Readable::<_, AsciiToUintError>::into_column(c),
            MixedType::F32(c) => Readable::<_, AsciiToUintError>::into_column(c),
            MixedType::F64(c) => Readable::<_, AsciiToUintError>::into_column(c),
        }
    }

    fn h_read_row<R: Read>(
        &mut self,
        h: &mut BufReader<R>,
        row: usize,
        byte_layout: Endian,
        buf: &mut Self::Buf,
    ) -> IOResult<(), AsciiToUintError> {
        match self {
            MixedType::Ascii(c) => c.h_read_row(h, row, (), buf),
            MixedType::Uint(c) => c
                .h_read_row(h, row, byte_layout, &mut ())
                .map_err(|e| e.infallible()),
            MixedType::F32(c) => c
                .h_read_row(h, row, byte_layout, &mut ())
                .map_err(|e| e.infallible()),
            MixedType::F64(c) => c
                .h_read_row(h, row, byte_layout, &mut ())
                .map_err(|e| e.infallible()),
        }
    }
}

impl<E> Readable<Endian, E> for AnyReaderBitmask {
    type Inner = AnyNullBitmask;
    type Buf = ();

    fn new(column_type: Self::Inner, nrows: usize) -> Self {
        match_many_to_one!(
            column_type,
            AnyBitmask,
            [Uint08, Uint16, Uint24, Uint32, Uint40, Uint48, Uint56, Uint64],
            c,
            { c.into_reader(nrows).into() }
        )
    }

    fn into_column(self) -> AnyFCSColumn {
        match_many_to_one!(
            self,
            AnyBitmask,
            [Uint08, Uint16, Uint24, Uint32, Uint40, Uint48, Uint56, Uint64],
            c,
            { Readable::<_, E>::into_column(c) }
        )
    }

    fn h_read_row<R: Read>(
        &mut self,
        h: &mut BufReader<R>,
        row: usize,
        byte_layout: Endian,
        buf: &mut Self::Buf,
    ) -> IOResult<(), E> {
        match_many_to_one!(
            self,
            AnyBitmask,
            [Uint08, Uint16, Uint24, Uint32, Uint40, Uint48, Uint56, Uint64],
            c,
            { c.h_read_row(h, row, byte_layout, buf) }
        )
    }
}

impl<T, const LEN: usize> NativeWritable<Endian> for Bitmask<T, LEN>
where
    Bitmask<T, LEN>: HasNativeType<Native = T>,
    T: Ord + Copy + IntFromBytes<LEN>,
{
    fn h_write<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        x: CastResult<T>,
        byte_layout: Endian,
    ) -> io::Result<()> {
        self.apply(x.new).h_write_endian(h, byte_layout)
    }
}

impl<T, const LEN: usize> NativeWritable<SizedByteOrd<LEN>> for Bitmask<T, LEN>
where
    Bitmask<T, LEN>: HasNativeType<Native = T>,
    T: Ord + Copy + IntFromBytes<LEN>,
{
    fn h_write<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        x: CastResult<T>,
        byte_layout: SizedByteOrd<LEN>,
    ) -> io::Result<()> {
        self.apply(x.new).h_write_ordered(h, byte_layout)
    }
}

impl<T, const LEN: usize> NativeWritable<Endian> for FloatRange<T, LEN>
where
    FloatRange<T, LEN>: HasNativeType<Native = T>,
    T: Copy + FloatFromBytes<LEN>,
{
    fn h_write<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        x: CastResult<T>,
        byte_layout: Endian,
    ) -> io::Result<()> {
        x.new.h_write_endian(h, byte_layout)
    }
}

impl<T, const LEN: usize> NativeWritable<SizedByteOrd<LEN>> for FloatRange<T, LEN>
where
    FloatRange<T, LEN>: HasNativeType<Native = T>,
    T: Copy + FloatFromBytes<LEN>,
{
    fn h_write<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        x: CastResult<T>,
        byte_layout: SizedByteOrd<LEN>,
    ) -> io::Result<()> {
        x.new.h_write_ordered(h, byte_layout)
    }
}

impl NativeWritable<()> for AsciiRange {
    fn h_write<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        x: CastResult<Self::Native>,
        _: (),
    ) -> io::Result<()> {
        let s = x.new.to_string();
        let w: usize = u8::from(self.chars()).into();
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

impl<'a, C, T, S> Writable<'a, S> for ColumnWriter<'a, C, T, S>
where
    C: NativeWritable<S> + HasNativeType<Native = T> + ToNativeWriter,
    T: AllFCSCast + Copy + Default,
    AnyLossError: From<LossError<<C as ToNativeWriter>::Error>>,
    AnySource<'a, T>: From<FCSColIter<'a, u8, T>>
        + From<FCSColIter<'a, u16, T>>
        + From<FCSColIter<'a, u32, T>>
        + From<FCSColIter<'a, u64, T>>
        + From<FCSColIter<'a, f32, T>>
        + From<FCSColIter<'a, f64, T>>,
{
    type Inner = C;

    fn new(column_type: Self::Inner, col: &'a AnyFCSColumn) -> Self {
        column_type.into_writer(col)
    }

    fn check_writer(column_type: Self::Inner, col: &'a AnyFCSColumn) -> Result<(), AnyLossError> {
        column_type.check_writer(col).map_err(|e| e.into())
    }

    fn h_write<W: Write>(&mut self, h: &mut BufWriter<W>, byte_layout: S) -> io::Result<()> {
        let x = self.data.next().unwrap();
        self.column_type.h_write(h, x, byte_layout)
    }
}

impl<'a> Writable<'a, Endian> for WriterMixedType<'a> {
    type Inner = NullMixedType;

    fn new(column_type: Self::Inner, col: &'a AnyFCSColumn) -> Self {
        match column_type {
            MixedType::Ascii(c) => Self::Ascii(c.into_writer(col)),
            MixedType::Uint(c) => Self::Uint(AnyWriterBitmask::new(c, col)),
            MixedType::F32(c) => Self::F32(c.into_writer(col)),
            MixedType::F64(c) => Self::F64(c.into_writer(col)),
        }
    }

    fn check_writer(column_type: Self::Inner, col: &'a AnyFCSColumn) -> Result<(), AnyLossError> {
        match column_type {
            MixedType::Ascii(c) => c.check_writer(col).map_err(|e| e.into()),
            MixedType::Uint(c) => AnyWriterBitmask::check_writer(c, col),
            MixedType::F32(c) => c.check_writer(col).map_err(|e| e.into()),
            MixedType::F64(c) => c.check_writer(col).map_err(|e| e.into()),
        }
    }

    fn h_write<W: Write>(&mut self, h: &mut BufWriter<W>, byte_layout: Endian) -> io::Result<()> {
        match self {
            Self::Ascii(c) => {
                let x = c.data.next().unwrap();
                c.column_type.h_write(h, x, ())
            }
            Self::Uint(c) => c.h_write(h, byte_layout),
            Self::F32(c) => {
                let x = c.data.next().unwrap();
                c.column_type.h_write(h, x, byte_layout)
            }
            Self::F64(c) => {
                let x = c.data.next().unwrap();
                c.column_type.h_write(h, x, byte_layout)
            }
        }
    }
}

impl<'a> Writable<'a, Endian> for AnyWriterBitmask<'a> {
    type Inner = AnyNullBitmask;

    fn new(column_type: Self::Inner, col: &'a AnyFCSColumn) -> Self {
        match_many_to_one!(
            column_type,
            AnyBitmask,
            [Uint08, Uint16, Uint24, Uint32, Uint40, Uint48, Uint56, Uint64],
            c,
            { c.into_writer(col).into() }
        )
    }

    fn check_writer(column_type: Self::Inner, col: &'a AnyFCSColumn) -> Result<(), AnyLossError> {
        match_many_to_one!(
            column_type,
            AnyBitmask,
            [Uint08, Uint16, Uint24, Uint32, Uint40, Uint48, Uint56, Uint64],
            c,
            { c.check_writer(col).map_err(|e| e.into()) }
        )
    }

    fn h_write<W: Write>(&mut self, h: &mut BufWriter<W>, byte_layout: Endian) -> io::Result<()> {
        match_many_to_one!(
            self,
            Self,
            [Uint08, Uint16, Uint24, Uint32, Uint40, Uint48, Uint56, Uint64],
            c,
            {
                let x = c.data.next().unwrap();
                c.column_type.h_write(h, x, byte_layout)
            }
        )
    }
}

impl<T, const LEN: usize> ToNativeWriter for Bitmask<T, LEN>
where
    Self: HasNativeType<Native = T>,
    u64: From<T>,
    T: Ord + Copy,
{
    type Error = BitmaskLossError;

    fn check_other_loss(&self, x: T) -> Option<Self::Error> {
        if x > self.bitmask() {
            Some(BitmaskLossError(u64::from(self.bitmask())))
        } else {
            None
        }
    }
}

impl<T, const LEN: usize> ToNativeWriter for FloatRange<T, LEN>
where
    Self: HasNativeType<Native = T>,
{
    type Error = Infallible;

    fn check_other_loss(&self, _: T) -> Option<Self::Error> {
        None
    }
}

impl ToNativeWriter for AsciiRange {
    type Error = AsciiLossError;

    fn check_other_loss(&self, x: Self::Native) -> Option<Self::Error>
    where
        u64: From<Self::Native>,
    {
        if Chars::from_u64(x) > self.chars() {
            Some(AsciiLossError(self.chars()))
        } else {
            None
        }
    }
}

macro_rules! uint_to_mixed {
    ($uint:ident, $wrap:ident) => {
        impl From<$uint> for NullMixedType {
            fn from(value: $uint) -> Self {
                MixedType::Uint(AnyBitmask::$wrap(value))
            }
        }
    };
}

uint_to_mixed!(Bitmask08, Uint08);
uint_to_mixed!(Bitmask16, Uint16);
uint_to_mixed!(Bitmask24, Uint24);
uint_to_mixed!(Bitmask32, Uint32);
uint_to_mixed!(Bitmask40, Uint40);
uint_to_mixed!(Bitmask48, Uint48);
uint_to_mixed!(Bitmask56, Uint56);
uint_to_mixed!(Bitmask64, Uint64);

/// A wrapper for any of the 6 source types that can be written.
///
/// Each inner type is an iterator from a different source type which emit
/// the given target type.
enum AnySource<'a, TargetType> {
    FromU08(FCSColIter<'a, u8, TargetType>),
    FromU16(FCSColIter<'a, u16, TargetType>),
    FromU32(FCSColIter<'a, u32, TargetType>),
    FromU64(FCSColIter<'a, u64, TargetType>),
    FromF32(FCSColIter<'a, f32, TargetType>),
    FromF64(FCSColIter<'a, f64, TargetType>),
}

impl<'a, T> AnySource<'a, T> {
    fn new<TargetType>(c: &'a AnyFCSColumn) -> Self
    where
        TargetType: AllFCSCast,
        Self: From<FCSColIter<'a, u8, TargetType>>
            + From<FCSColIter<'a, u16, TargetType>>
            + From<FCSColIter<'a, u32, TargetType>>
            + From<FCSColIter<'a, u64, TargetType>>
            + From<FCSColIter<'a, f32, TargetType>>
            + From<FCSColIter<'a, f64, TargetType>>,
    {
        match_many_to_one!(c, AnyFCSColumn, [U08, U16, U32, U64, F32, F64], xs, {
            FCSDataType::as_col_iter(xs).into()
        })
    }

    fn next(&mut self) -> Option<CastResult<T>> {
        match_many_to_one!(
            self,
            Self,
            [FromU08, FromU16, FromU32, FromU64, FromF32, FromF64],
            c,
            { c.next() }
        )
    }
}

fn is_ascii_delim(x: u8) -> bool {
    // tab, newline, carriage return, space, or comma
    x == 9 || x == 10 || x == 13 || x == 32 || x == 44
}

impl<C: Serialize, L: Serialize, T> Serialize for FixedLayout<C, L, T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("FixedLayout", 2)?;
        state.serialize_field("columns", Vec::from(self.columns.as_ref()).as_slice())?;
        state.serialize_field("byte_layout", &self.byte_layout)?;
        state.end()
    }
}

impl<T> Serialize for DelimAsciiLayout<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("DelimitedLayout", 1)?;
        state.serialize_field("ranges", Vec::from(self.ranges.as_ref()).as_slice())?;
        state.end()
    }
}

impl<T> FixedAsciiLayout<T> {
    // fn ascii_layout_values<D: Copy, S: Default>(&self, datatype: D) -> LayoutValues<S, D> {
    //     LayoutValues {
    //         datatype: AlphaNumType::Ascii,
    //         // NOTE BYTEORD is meaningless for ASCII so use dummy
    //         byte_layout: S::default(),
    //         columns: self.column_layout_values(datatype).into(),
    //     }
    // }

    // fn column_layout_values<D: Copy>(&self, datatype: D) -> NonEmpty<ColumnLayoutValues<D>> {
    //     self.columns.as_ref().map(|c| ColumnLayoutValues {
    //         width: Width::Fixed(c.fixed_width()),
    //         range: Range(c.range().into()),
    //         datatype,
    //     })
    // }
}

impl EndianLayout<AnyNullBitmask> {
    pub(crate) fn endian_uint_try_new<D>(
        cs: NonEmpty<ColumnLayoutValues<D>>,
        e: Endian,
        notrunc: bool,
    ) -> DeferredResult<Self, ColumnError<BitmaskError>, ColumnError<NewUintTypeError>> {
        FixedLayout::try_new(cs, e, |c| {
            AnyBitmask::from_width_and_range(c.width, c.range, notrunc).def_errors_into()
        })
    }

    pub(crate) fn uint_try_into_ordered<T>(self) -> LayoutConvertResult<AnyOrderedUintLayout<T>> {
        let cs = self.columns;
        cs.head
            .try_into_one_size(cs.tail, self.byte_layout, 1)
            .mult_map_errors(|(index, error)| ConvertWidthError { index, error })
            .mult_errors_into()
    }
}

impl EndianLayout<NullMixedType> {
    // fn mixed_layout_values(&self) -> LayoutValues<Endian, Option<NumType>> {
    //     let cs: NonEmpty<_> = self.columns.as_ref().map(|c| ColumnLayoutValues {
    //         width: Width::Fixed(c.fixed_width()),
    //         range: c.range(),
    //         datatype: c.as_num_type(),
    //     });
    //     // If any numeric types are none, then that means at least one column is
    //     // ASCII, which means that $DATATYPE needs to be "A" since $PnDATATYPE
    //     // cannot be "A".
    //     let (datatype, columns) = if let Ok(mut ds) = cs.as_ref().try_map(|c| c.datatype.ok_or(()))
    //     {
    //         // Determine which type appears the most, use that for $DATATYPE
    //         ds.sort();
    //         // TODO this should be a general non-empty function
    //         let mut counts = NonEmpty::new((ds.head, 1));
    //         for d in ds.tail {
    //             if counts.last().0 == d {
    //                 counts.last_mut().1 += 1;
    //             } else {
    //                 counts.push((d, 1));
    //             }
    //         }
    //         let mode = counts.maximum_by_key(|x| x.1).0;
    //         // Set all columns which have same type as $DATATYPE to None
    //         let new_cs = cs.map(|mut c| {
    //             if c.datatype.is_some_and(|x| x == mode) {
    //                 c.datatype = None;
    //             }
    //             c
    //         });
    //         (mode.into(), new_cs)
    //     } else {
    //         (AlphaNumType::Ascii, cs)
    //     };
    //     LayoutValues3_2 {
    //         datatype,
    //         byte_layout: self.byte_layout,
    //         columns: columns.into(),
    //     }
    // }

    fn primary_datatype(&self) -> AlphaNumType {
        // If any numeric types are none, then that means at least one column is
        // ASCII, which means that $DATATYPE needs to be "A" since $PnDATATYPE
        // cannot be "A".
        if let Ok(mut ds) = self.columns.as_ref().try_map(|c| c.as_num_type().ok_or(())) {
            // Determine which type appears the most, use that for $DATATYPE
            ds.sort();
            // TODO this should be a general non-empty function
            let mut counts = NonEmpty::new((ds.head, 1));
            for d in ds.tail {
                if counts.last().0 == d {
                    counts.last_mut().1 += 1;
                } else {
                    counts.push((d, 1));
                }
            }
            let mode = counts.maximum_by_key(|x| x.1).0;
            mode.into()
        } else {
            AlphaNumType::Ascii
        }
    }

    pub(crate) fn try_into_ordered<T>(
        self,
    ) -> MultiResult<AnyOrderedLayout<T>, MixedToOrderedLayoutError> {
        let c0 = self.columns.head;
        let cs = self.columns.tail;
        let endian = self.byte_layout;
        match c0 {
            MixedType::Ascii(x) => cs
                .into_iter()
                .enumerate()
                .map(|(i, c)| {
                    c.try_into().map_err(|e| MixedColumnConvertError {
                        error: MixedToOrderedConvertError::Ascii(e),
                        index: (i + 1).into(),
                    })
                })
                .gather()
                .map(|xs| {
                    AnyOrderedLayout::Ascii(AnyAsciiLayout::Fixed(FixedLayout::new1(x, xs, ())))
                }),
            MixedType::Uint(x) => x
                .try_into_one_size(cs, endian, 1)
                .map(AnyOrderedLayout::Integer)
                .mult_map_errors(|(index, error)| MixedColumnConvertError {
                    index,
                    error: error.into(),
                }),
            MixedType::F32(x) => cs
                .into_iter()
                .enumerate()
                .map(|(i, c)| {
                    c.try_into().map_err(|e| MixedColumnConvertError {
                        error: MixedToOrderedConvertError::Float(e),
                        index: (i + 1).into(),
                    })
                })
                .gather()
                .map(|xs| AnyOrderedLayout::F32(FixedLayout::new1(x, xs, endian.into()))),
            MixedType::F64(x) => cs
                .into_iter()
                .enumerate()
                .map(|(i, c)| {
                    c.try_into().map_err(|e| MixedColumnConvertError {
                        error: MixedToOrderedConvertError::Double(e),
                        index: (i + 1).into(),
                    })
                })
                .gather()
                .map(|xs| AnyOrderedLayout::F64(FixedLayout::new1(x, xs, endian.into()))),
        }
    }

    pub(crate) fn try_into_non_mixed(
        self,
    ) -> MultiResult<NonMixedEndianLayout, MixedToNonMixedLayoutError> {
        let c0 = self.columns.head;
        let it = self.columns.tail.into_iter().enumerate();
        let byte_layout = self.byte_layout;
        match c0 {
            MixedType::Ascii(x) => it
                .map(|(i, c)| {
                    c.try_into()
                        .map_err(|e| (i, MixedToNonMixedConvertError::Ascii(e)))
                })
                .gather()
                .map(|xs| {
                    NonMixedEndianLayout::Ascii(AnyAsciiLayout::Fixed(FixedLayout::new1(x, xs, ())))
                }),
            MixedType::Uint(x) => it
                .map(|(i, c)| {
                    c.try_into()
                        .map_err(|e| (i, MixedToNonMixedConvertError::Integer(e)))
                })
                .gather()
                .map(|xs| NonMixedEndianLayout::Integer(FixedLayout::new1(x, xs, byte_layout))),
            MixedType::F32(x) => it
                .map(|(i, c)| {
                    c.try_into()
                        .map_err(|e| (i, MixedToNonMixedConvertError::Float(e)))
                })
                .gather()
                .map(|xs| NonMixedEndianLayout::F32(FixedLayout::new1(x, xs, byte_layout))),
            MixedType::F64(x) => it
                .map(|(i, c)| {
                    c.try_into()
                        .map_err(|e| (i, MixedToNonMixedConvertError::Double(e)))
                })
                .gather()
                .map(|xs| NonMixedEndianLayout::F64(FixedLayout::new1(x, xs, byte_layout))),
        }
        .mult_map_errors(|(i, error)| MixedColumnConvertError {
            index: (i + 1).into(),
            error,
        })
    }
}

macro_rules! impl_num_props {
    ($size:expr, $t:ty) => {
        impl NumProps for $t {
            const LEN: usize = $size;
            type BUF = [u8; $size];

            fn read_buf<R: Read>(h: &mut BufReader<R>) -> io::Result<[u8; $size]> {
                let mut buf = [0; $size];
                h.read_exact(&mut buf)?;
                Ok(buf)
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

impl_num_props!(1, u8);
impl_num_props!(2, u16);
impl_num_props!(4, u32);
impl_num_props!(8, u64);
impl_num_props!(4, f32);
impl_num_props!(8, f64);

impl OrderedFromBytes<1> for u8 {}
impl OrderedFromBytes<2> for u16 {}
impl OrderedFromBytes<3> for u32 {}
impl OrderedFromBytes<4> for u32 {}
impl OrderedFromBytes<5> for u64 {}
impl OrderedFromBytes<6> for u64 {}
impl OrderedFromBytes<7> for u64 {}
impl OrderedFromBytes<8> for u64 {}
impl OrderedFromBytes<4> for f32 {}
impl OrderedFromBytes<8> for f64 {}

impl FloatFromBytes<4> for f32 {}
impl FloatFromBytes<8> for f64 {}

impl IntFromBytes<1> for u8 {}
impl IntFromBytes<2> for u16 {}
impl IntFromBytes<3> for u32 {}
impl IntFromBytes<4> for u32 {}
impl IntFromBytes<5> for u64 {}
impl IntFromBytes<6> for u64 {}
impl IntFromBytes<7> for u64 {}
impl IntFromBytes<8> for u64 {}

// TODO move to source crate
impl<T, const LEN: usize> Bitmask<T, LEN> {
    fn try_from_many<E, X>(
        xs: Vec<X>,
        starting_index: usize,
    ) -> MultiResult<Vec<Self>, (MeasIndex, E)>
    where
        Self: TryFrom<X, Error = E>,
    {
        xs.into_iter()
            .enumerate()
            .map(|(i, c)| Self::try_from(c).map_err(|e| ((i + starting_index).into(), e)))
            .gather()
    }
}

impl<T, const LEN: usize> FloatRange<T, LEN> {
    /// Create a new float range from an integer or float.
    pub(crate) fn from_range(range: FloatOrInt, notrunc: bool) -> BiTentative<Self, FloatRangeError>
    where
        NonNanFloat<T>: TryFrom<FloatOrInt, Error = ToFloatError<T>>,
        T: num_traits::float::Float,
    {
        range.as_float(notrunc).map(|x| Self { range: x })
    }

    /// Make new float range from $PnB and $PnR values.
    ///
    /// Will return an error if $PnB is the incorrect size.
    pub(crate) fn from_width_and_range(
        width: Width,
        range: Range,
        notrunc: bool,
    ) -> DeferredResult<Self, FloatRangeError, FloatWidthError>
    where
        NonNanFloat<T>: TryFrom<FloatOrInt, Error = ToFloatError<T>>,
        T: num_traits::float::Float,
    {
        Bytes::try_from(width)
            .into_deferred()
            .def_and_maybe(|bytes| {
                if usize::from(u8::from(bytes)) == LEN {
                    Ok(Self::from_range(range.0, notrunc).errors_into())
                } else {
                    Err(DeferredFailure::new1(FloatWidthError::WrongWidth(
                        WrongFloatWidth {
                            expected: LEN,
                            width: bytes,
                        },
                    )))
                }
            })
    }
}

// TODO also check scale here?
impl NullMixedType {
    /// Make a new mixed range from $PnB and $PnR, and $PnDATATYPE values
    pub(crate) fn from_width_and_range(
        width: Width,
        range: Range,
        datatype: Option<NumType>,
        notrunc: bool,
    ) -> DeferredResult<Self, NewMixedTypeWarning, NewMixedTypeError> {
        if let Some(dt) = datatype {
            match dt {
                NumType::Integer => AnyBitmask::from_width_and_range(width, range, notrunc)
                    .def_map_value(Self::Uint)
                    .def_inner_into(),
                NumType::Single => F32Range::from_width_and_range(width, range, notrunc)
                    .def_map_value(Self::F32)
                    .def_inner_into(),
                NumType::Double => F64Range::from_width_and_range(width, range, notrunc)
                    .def_map_value(Self::F64)
                    .def_inner_into(),
            }
        } else {
            AsciiRange::from_width_and_range(width, range, notrunc)
                .def_map_value(Self::Ascii)
                .def_inner_into()
        }
    }

    /// Infer the mixed type from the type of the range.
    ///
    /// If int is supplied, return one of the uint types depending on size.
    /// If float is supplied, return f32 or f64 depending on size. This will
    /// never return ASCII.
    fn from_range(range: FloatOrInt) -> Self {
        match range {
            FloatOrInt::Int(x) => Self::Uint(AnyBitmask::from_u64(x)),
            FloatOrInt::Float(x) => x
                .try_as_f32()
                .map_or(Self::F64(FloatRange { range: x }), |y| {
                    Self::F32(FloatRange { range: y })
                }),
        }
    }

    fn as_num_type(&self) -> Option<NumType> {
        match self {
            Self::Ascii(_) => None,
            Self::Uint(_) => Some(NumType::Integer),
            Self::F32(_) => Some(NumType::Single),
            Self::F64(_) => Some(NumType::Double),
        }
    }
}

impl AnyNullBitmask {
    /// Make a new bitmask from $PnB and PnR values.
    ///
    /// Will return an error if $PnB (in bits) cannot be converted into a width
    /// in bytes.
    fn from_width_and_range(
        width: Width,
        range: Range,
        notrunc: bool,
    ) -> DeferredResult<Self, BitmaskError, NewUintTypeError> {
        width
            .try_into()
            .into_deferred()
            .def_and_tentatively(|bytes| Self::new1(bytes, range.0, notrunc).errors_into())
    }

    /// make a new bitmask from a float or integer.
    ///
    /// The size will be determined by the input and will be kept as small as
    /// possible.
    fn from_range(range: FloatOrInt, notrunc: bool) -> BiTentative<Self, IntRangeError> {
        range.as_uint(notrunc).map(Self::from_u64)
    }

    /// Make a new bitmask with a given width (in bytes) using a float/int.
    fn new1(width: Bytes, range: FloatOrInt, notrunc: bool) -> BiTentative<Self, BitmaskError> {
        match width {
            Bytes::B1 => Bitmask08::from_range(range, notrunc).map(|b| b.into()),
            Bytes::B2 => Bitmask16::from_range(range, notrunc).map(|b| b.into()),
            Bytes::B3 => Bitmask24::from_range(range, notrunc).map(|b| b.into()),
            Bytes::B4 => Bitmask32::from_range(range, notrunc).map(|b| b.into()),
            Bytes::B5 => Bitmask40::from_range(range, notrunc).map(|b| b.into()),
            Bytes::B6 => Bitmask48::from_range(range, notrunc).map(|b| b.into()),
            Bytes::B7 => Bitmask56::from_range(range, notrunc).map(|b| b.into()),
            Bytes::B8 => Bitmask64::from_range(range, notrunc).map(|b| b.into()),
        }
    }

    /// Make a new bitmask from a u64.
    ///
    /// The width is determined by the magnitude of the range; the smallest
    /// possible will be used.
    fn from_u64(range: u64) -> Self {
        // ASSUME these will never truncate because we check the width first
        match Bytes::from_u64(range) {
            Bytes::B1 => Self::Uint08(Bitmask::from_u64(range).0),
            Bytes::B2 => Self::Uint16(Bitmask::from_u64(range).0),
            Bytes::B3 => Self::Uint24(Bitmask::from_u64(range).0),
            Bytes::B4 => Self::Uint32(Bitmask::from_u64(range).0),
            Bytes::B5 => Self::Uint40(Bitmask::from_u64(range).0),
            Bytes::B6 => Self::Uint48(Bitmask::from_u64(range).0),
            Bytes::B7 => Self::Uint56(Bitmask::from_u64(range).0),
            Bytes::B8 => Self::Uint64(Bitmask::from_u64(range).0),
        }
    }

    pub(crate) fn try_into_one_size<X, E, T>(
        self,
        tail: Vec<X>,
        endian: Endian,
        starting_index: usize,
    ) -> MultiResult<AnyOrderedUintLayout<T>, (MeasIndex, E)>
    where
        Bitmask08: TryFrom<X, Error = E>,
        Bitmask16: TryFrom<X, Error = E>,
        Bitmask24: TryFrom<X, Error = E>,
        Bitmask32: TryFrom<X, Error = E>,
        Bitmask40: TryFrom<X, Error = E>,
        Bitmask48: TryFrom<X, Error = E>,
        Bitmask56: TryFrom<X, Error = E>,
        Bitmask64: TryFrom<X, Error = E>,
    {
        match_many_to_one!(
            self,
            Self,
            [Uint08, Uint16, Uint24, Uint32, Uint40, Uint48, Uint56, Uint64],
            x,
            {
                Bitmask::try_from_many(tail, starting_index)
                    .map(|xs| FixedLayout::new1(x, xs, endian.into()).into())
            }
        )
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

// impl<S, D> LayoutValues<S, D> {
//     pub(crate) fn req_keywords(&self) -> impl Iterator<Item = (String, String)>
//     where
//         S: ReqMetarootKey,
//     {
//         [self.datatype.pair(), self.byte_layout.pair()].into_iter()
//     }

//     pub(crate) fn req_meas_keywords(&self) -> impl Iterator<Item = (String, String, String)>
//     where
//         ColumnLayoutValues<D>: VersionedColumnLayout,
//     {
//         self.columns
//             .iter()
//             .enumerate()
//             .flat_map(|(i, c)| c.req_keywords(i.into()))
//     }

//     pub(crate) fn opt_meas_keywords(&self) -> impl Iterator<Item = (String, String, Option<String>)>
//     where
//         ColumnLayoutValues<D>: VersionedColumnLayout,
//     {
//         self.columns
//             .iter()
//             .enumerate()
//             .flat_map(|(i, c)| c.opt_keywords(i.into()))
//     }
// }

// impl<S: Default, D> Default for LayoutValues<S, D> {
//     fn default() -> Self {
//         Self {
//             datatype: AlphaNumType::Integer,
//             byte_layout: S::default(),
//             columns: vec![],
//         }
//     }
// }

impl VersionedColumnLayout for ColumnLayoutValues2_0 {
    fn lookup(kws: &mut StdKeywords, i: MeasIndex) -> LookupResult<Self> {
        let j = i.into();
        let w = Width::lookup_req(kws, j);
        let r = Range::lookup_req(kws, j);
        w.def_zip(r).def_map_value(|(width, range)| Self {
            width,
            range,
            datatype: (),
        })
    }

    fn get(
        kws: &StdKeywords,
        i: MeasIndex,
    ) -> DeferredResult<Self, ParseKeyError<NumTypeError>, RawParsedError> {
        let j = i.into();
        let w = Width::get_meas_req(kws, j).map_err(|e| e.into());
        let r = Range::get_meas_req(kws, j).map_err(|e| e.into());
        w.zip(r)
            .map(|(width, range)| Self {
                width,
                range,
                datatype: (),
            })
            .map(Tentative::new1)
            .map_err(DeferredFailure::new2)
    }

    // fn req_keywords(&self, i: MeasIndex) -> impl Iterator<Item = (String, String, String)> {
    //     let j = i.into();
    //     [self.range.triple(j), self.width.triple(j)].into_iter()
    // }

    // fn opt_keywords(&self, _: MeasIndex) -> impl Iterator<Item = (String, String, Option<String>)> {
    //     [].into_iter()
    // }
}

impl VersionedColumnLayout for ColumnLayoutValues3_2 {
    fn lookup(kws: &mut StdKeywords, i: MeasIndex) -> LookupResult<Self> {
        let j = i.into();
        let w = Width::lookup_req(kws, j);
        let r = Range::lookup_req(kws, j);
        w.def_zip(r).def_and_tentatively(|(width, range)| {
            NumType::lookup_opt(kws, j, false)
                .map(|x| x.0)
                .map(|datatype| Self {
                    width,
                    range,
                    datatype,
                })
        })
    }

    fn get(
        kws: &StdKeywords,
        i: MeasIndex,
    ) -> DeferredResult<Self, ParseKeyError<NumTypeError>, RawParsedError> {
        let j = i.into();
        let w = Width::get_meas_req(kws, j).map_err(|e| e.into());
        let r = Range::get_meas_req(kws, j).map_err(|e| e.into());
        w.zip(r)
            .map(Tentative::new1)
            .map_err(DeferredFailure::new2)
            .def_and_tentatively(|(width, range)| {
                NumType::get_meas_opt(kws, j)
                    .map(|x| x.0)
                    .map_or_else(|e| Tentative::new(None, vec![e], vec![]), Tentative::new1)
                    .map(|datatype| Self {
                        width,
                        range,
                        datatype,
                    })
            })
    }

    // fn req_keywords(&self, i: MeasIndex) -> impl Iterator<Item = (String, String, String)> {
    //     let j = i.into();
    //     [self.range.triple(j), self.width.triple(j)].into_iter()
    // }

    // fn opt_keywords(&self, i: MeasIndex) -> impl Iterator<Item = (String, String, Option<String>)> {
    //     [(
    //         NumType::std(i.into()).to_string(),
    //         NumType::std_blank(),
    //         self.datatype.map(|x| x.to_string()),
    //     )]
    //     .into_iter()
    // }
}

impl From<ColumnLayoutValues3_2> for ColumnLayoutValues2_0 {
    fn from(value: ColumnLayoutValues3_2) -> Self {
        Self {
            width: value.width,
            range: value.range,
            datatype: (),
        }
    }
}

impl<T> DelimAsciiLayout<T> {
    fn new(ranges: NonEmpty<u64>) -> Self {
        Self {
            ranges,
            tot_action: PhantomData,
        }
    }

    // fn layout_values<D: Copy, S: Default>(&self, datatype: D) -> LayoutValues<S, D> {
    //     LayoutValues {
    //         datatype: AlphaNumType::Ascii,
    //         // NOTE BYTEORD is meaningless for delimited ASCII so use a dummy
    //         byte_layout: S::default(),
    //         columns: self.column_layout_values(datatype).into(),
    //     }
    // }

    // fn column_layout_values<D: Copy>(&self, datatype: D) -> NonEmpty<ColumnLayoutValues<D>> {
    //     self.ranges.as_ref().map(|r| ColumnLayoutValues {
    //         width: Width::Variable,
    //         range: Range((*r).into()),
    //         datatype,
    //     })
    // }

    // fn insert(&mut self, index: MeasIndex, range: u64) -> Result<(), BoundaryIndexError> {
    //     IndexFromOne::from(index)
    //         .check_boundary_index(self.ranges.len())
    //         .map(|i| self.ranges.insert(i, range))
    // }

    fn insert_unchecked(&mut self, index: MeasIndex, range: u64) {
        self.ranges.insert(index.into(), range)
    }

    fn push(&mut self, range: u64) {
        self.ranges.push(range)
    }

    fn remove(&mut self, index: MeasIndex) -> Result<(), ClearOptionalOr<IndexError>> {
        self.ranges.remove(index.into())
    }

    fn remove_nocheck(&mut self, index: MeasIndex) -> Result<(), ClearOptional> {
        self.ranges.remove_nocheck(index.into())
    }

    fn h_read_df<R: Read>(
        &self,
        h: &mut BufReader<R>,
        tot: T::Tot,
        nbytes: usize,
    ) -> IOResult<FCSDataFrame, ReadDelimAsciiError>
    where
        T: TotDefinition,
    {
        let rs = &self.ranges;
        T::with_tot(
            h,
            tot,
            |_h, t| h_read_delim_with_rows(rs, _h, t, nbytes).map_err(|e| e.inner_into()),
            |_h| h_read_delim_without_rows(rs, _h, nbytes).map_err(|e| e.inner_into()),
        )
    }

    fn check_writer(&self, df: &FCSDataFrame) -> MultiResult<(), ColumnError<AnyLossError>> {
        df.iter_columns()
            .enumerate()
            .map(|(i, c)| {
                c.check_writer::<_, _, u64>(|_| None)
                    .map_err(|error| ColumnError {
                        error: AnyLossError::Int(error),
                        index: i.into(),
                    })
            })
            .gather()
            .void()
    }

    fn h_write_df<W: Write>(&self, h: &mut BufWriter<W>, df: &FCSDataFrame) -> io::Result<()> {
        let ncols = df.ncols();
        let nrows = df.nrows();
        // ASSUME dataframe has correct number of columns
        let mut column_srcs: Vec<_> = df.iter_columns().map(AnySource::new::<u64>).collect();
        for row in 0..nrows {
            for (col, xs) in column_srcs.iter_mut().enumerate() {
                let x = xs.next().unwrap();
                let s = x.new.to_string();
                let buf = s.as_bytes();
                h.write_all(buf)?;
                // write delimiter after all but last value
                if !(row == nrows - 1 && col == ncols - 1) {
                    h.write_all(&[32])?; // 32 = space in ASCII
                }
            }
        }
        Ok(())
    }

    fn req_keywords<X: Default + ReqMetarootKey>(&self) -> [(String, String); 2] {
        // NOTE BYTEORD is meaningless for delimited ASCII so use a dummy
        [AlphaNumType::Ascii.pair(), X::default().pair()]
    }

    fn req_meas_keywords(&self) -> NonEmpty<(String, String, String)> {
        let xs = self.ranges.as_ref().enumerate().map(|(i, r)| {
            let x = Width::Variable.triple(i.into());
            let y = Range((*r).into()).triple(i.into());
            NonEmpty::from((x, vec![y]))
        });
        NonEmpty::flatten(xs)
    }
}

fn h_read_delim_with_rows<R: Read>(
    ranges: &NonEmpty<u64>,
    h: &mut BufReader<R>,
    tot: Tot,
    nbytes: usize,
) -> IOResult<FCSDataFrame, ReadDelimWithRowsAsciiError> {
    let mut buf = Vec::new();
    let mut last_was_delim = false;
    let nrows = tot.0;
    let ncols = ranges.len();
    // Here we have $TOT so initialize vectors to required length
    let mut data = ranges.as_ref().map(|_| vec![0; nrows]);
    // let mut data = self.0.columns;
    // let nrows = data.head.len();
    // let ncols = data.len();
    let mut row = 0;
    let mut col = 0;
    // Delimiters are tab, newline, carriage return, space, or comma. Any
    // consecutive delimiter counts as one, and delimiters can be mixed.
    for b in h.bytes().take(nbytes) {
        let byte = b?;
        // exit if we encounter more rows than expected.
        if row == nrows {
            let e = ReadDelimWithRowsAsciiError::RowsExceeded(RowsExceededError(nrows));
            return Err(ImpureError::Pure(e));
        }
        if is_ascii_delim(byte) {
            if !last_was_delim {
                last_was_delim = true;
                data[col][row] = ascii_to_uint(&buf)
                    .map_err(ReadDelimWithRowsAsciiError::Parse)
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
        return Err(ImpureError::Pure(ReadDelimWithRowsAsciiError::Incomplete(
            e,
        )));
    }
    // The spec isn't clear if the last value should be a delim or
    // not, so flush the buffer if it has anything in it since we
    // only try to parse if we hit a delim above.
    if !buf.is_empty() {
        data[col][row] = ascii_to_uint(&buf)
            .map_err(ReadDelimWithRowsAsciiError::Parse)
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

fn h_read_delim_without_rows<R: Read>(
    ranges: &NonEmpty<u64>,
    h: &mut BufReader<R>,
    nbytes: usize,
) -> IOResult<FCSDataFrame, ReadDelimAsciiWithoutRowsError> {
    let mut buf = Vec::new();
    // Here we don't have $TOT so init to empty vectors
    let mut data = ranges.as_ref().map(|_| vec![]);
    let ncols = data.len();
    let mut col = 0;
    let mut last_was_delim = false;
    let go = |_data: &mut NonEmpty<Vec<u64>>, _col, _buf: &[u8]| {
        ascii_to_uint(_buf)
            .map_err(ReadDelimAsciiWithoutRowsError::Parse)
            .map_err(ImpureError::Pure)
            .map(|x| _data[_col].push(x))
    };
    // Delimiters are tab, newline, carriage return, space, or comma. Any
    // consecutive delimiter counts as one, and delimiters can be mixed.
    // If we don't know the number of rows, the only choice is to push onto
    // the column vectors one at a time. This leads to the possibility that
    // the vectors may not be the same length in the end, in which case,
    // scream loudly and bail.
    for b in h.bytes().take(nbytes) {
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
        return Err(ImpureError::Pure(ReadDelimAsciiWithoutRowsError::Unequal));
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

impl<C, S, T> FixedLayout<C, S, T> {
    fn new(columns: NonEmpty<C>, byte_layout: S) -> Self {
        Self {
            columns,
            byte_layout,
            tot_action: PhantomData,
        }
    }

    fn new1(head: C, tail: Vec<C>, byte_layout: S) -> Self {
        Self::new((head, tail).into(), byte_layout)
    }

    fn try_new<D, F, W, E, CW, CE>(
        cs: NonEmpty<ColumnLayoutValues<D>>,
        byte_layout: S,
        new_col_f: F,
    ) -> DeferredResult<Self, W, E>
    where
        W: From<ColumnError<CW>>,
        E: From<ColumnError<CE>>,
        F: Fn(ColumnLayoutValues<D>) -> DeferredResult<C, CW, CE>,
    {
        cs.enumerate()
            .map_results(|(i, c)| {
                new_col_f(c)
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
            .map_err(DeferredFailure::mconcat)
            .map(Tentative::mconcat_ne)
            .def_map_value(|columns| Self::new(columns, byte_layout))
    }

    // fn insert_inner(&mut self, index: MeasIndex, col: C) -> Result<(), BoundaryIndexError> {
    //     IndexFromOne::from(index)
    //         .check_boundary_index(self.columns.len())
    //         .map(|i| self.columns.insert(i, col))
    // }

    fn insert_inner_nocheck(&mut self, index: MeasIndex, col: C) {
        self.columns.insert(index.into(), col)
    }

    fn push_inner(&mut self, col: C) {
        self.columns.push(col)
    }

    fn remove_inner(&mut self, index: MeasIndex) -> Result<(), ClearOptionalOr<IndexError>> {
        self.columns.remove(index.into())
    }

    fn remove_nocheck_inner(&mut self, index: MeasIndex) -> Result<(), ClearOptional> {
        self.columns.remove_nocheck(index.into())
    }

    // fn layout_values<D: Copy, R>(&self, datatype: D) -> LayoutValues<R, D>
    // where
    //     R: From<S>,
    //     S: Copy,
    //     C: IsFixed + HasDatatype,
    // {
    //     LayoutValues {
    //         datatype: C::DATATYPE,
    //         byte_layout: self.byte_layout.into(),
    //         columns: self
    //             .columns
    //             .as_ref()
    //             .map(|c| ColumnLayoutValues {
    //                 width: Width::Fixed(c.fixed_width()),
    //                 range: c.range(),
    //                 datatype,
    //             })
    //             .into(),
    //     }
    // }

    fn h_read_df_numeric<R: Read, I, W, E>(
        &self,
        h: &mut BufReader<R>,
        tot: T::Tot,
        seg: AnyDataSegment,
        conf: &ReaderConfig,
    ) -> IODeferredResult<FCSDataFrame, W, E>
    where
        W: From<UnevenEventWidth> + From<TotEventMismatch>,
        E: From<UnevenEventWidth> + From<TotEventMismatch>,
        S: Copy,
        C: IsFixed + Copy,
        I: Readable<S, E, Inner = C, Buf = ()>,
        T: TotDefinition,
    {
        self.h_read_df::<_, I, _, _, E, E>(h, &mut (), tot, seg, conf)
    }

    fn h_read_df<R: Read, I, B, W, E, ReadErr>(
        &self,
        h: &mut BufReader<R>,
        buf: &mut B,
        tot: T::Tot,
        seg: AnyDataSegment,
        conf: &ReaderConfig,
    ) -> IODeferredResult<FCSDataFrame, W, E>
    where
        W: From<UnevenEventWidth> + From<TotEventMismatch>,
        E: From<ReadErr> + From<UnevenEventWidth> + From<TotEventMismatch>,
        S: Copy,
        C: IsFixed + Copy,
        I: Readable<S, ReadErr, Inner = C, Buf = B>,
        T: TotDefinition,
    {
        self.compute_nrows(seg, conf)
            .inner_into()
            .errors_liftio()
            .and_tentatively(|nrows| {
                T::check_tot(nrows, tot, conf.allow_tot_mismatch)
                    .map(|_| nrows)
                    .inner_into()
                    .errors_liftio()
            })
            .and_maybe(|nrows| {
                self.h_read_unchecked_df::<R, I, B, ReadErr>(h, nrows, buf)
                    .map_err(|e| e.inner_into())
                    .into_deferred()
            })
    }

    fn check_writer<'a, I>(
        &self,
        df: &'a FCSDataFrame,
    ) -> MultiResult<(), ColumnError<AnyLossError>>
    where
        C: Copy,
        I: Writable<'a, S, Inner = C>,
    {
        // ASSUME df has same number of columns as layout
        self.columns
            .iter()
            .zip(df.iter_columns())
            .enumerate()
            .map(|(i, (col_type, col_data))| {
                I::check_writer(*col_type, col_data).map_err(|error| ColumnError {
                    error,
                    index: i.into(),
                })
            })
            .gather()
            .void()
    }

    fn h_write_df<'a, W: Write, I>(
        &self,
        h: &mut BufWriter<W>,
        df: &'a FCSDataFrame,
    ) -> io::Result<()>
    where
        S: Copy,
        C: Copy,
        I: Writable<'a, S, Inner = C>,
    {
        let nrows = df.nrows();
        // ASSUME df has same number of columns as layout
        let mut cs: Vec<_> = self
            .columns
            .iter()
            .zip(df.iter_columns())
            .map(|(col_type, col_data)| I::new(*col_type, col_data))
            .collect();
        for _ in 0..nrows {
            for c in cs.iter_mut() {
                c.h_write(h, self.byte_layout)?;
            }
        }
        Ok(())
    }

    fn h_read_unchecked_df<R: Read, I, B, E>(
        &self,
        h: &mut BufReader<R>,
        nrows: usize,
        buf: &mut B,
    ) -> IOResult<FCSDataFrame, E>
    where
        S: Copy,
        C: IsFixed + Copy,
        I: Readable<S, E, Inner = C, Buf = B>,
    {
        let mut col_readers: Vec<_> = self.columns.iter().map(|c| I::new(*c, nrows)).collect();
        for row in 0..nrows {
            for c in col_readers.iter_mut() {
                c.h_read_row(h, row, self.byte_layout, buf)
                    .map_err(|e| e.inner_into())?;
            }
        }
        let data = col_readers.into_iter().map(|c| c.into_column()).collect();
        Ok(FCSDataFrame::try_new(data).unwrap())
    }

    fn columns_into<X>(self) -> FixedLayout<X, S, T>
    where
        X: From<C>,
    {
        FixedLayout::new(self.columns.map(|c| c.into()), self.byte_layout)
    }

    fn byte_layout_into<X>(self) -> FixedLayout<C, X, T>
    where
        X: From<S>,
    {
        FixedLayout::new(self.columns, self.byte_layout.into())
    }

    fn byte_layout_try_into<X>(self) -> Result<FixedLayout<C, X, T>, X::Error>
    where
        X: TryFrom<S>,
    {
        self.byte_layout
            .try_into()
            .map(|byte_layout| FixedLayout::new(self.columns, byte_layout))
    }

    fn tot_into<X>(self) -> FixedLayout<C, S, X> {
        FixedLayout::new(self.columns, self.byte_layout)
    }

    fn event_width(&self) -> usize
    where
        C: IsFixed,
    {
        self.columns.iter().map(|c| usize::from(c.nbytes())).sum()
    }

    fn ncols(&self) -> usize {
        self.columns.len()
    }

    fn nbytes(&self, df: &FCSDataFrame) -> u64
    where
        C: IsFixed,
    {
        (self.event_width() * df.nrows()) as u64
    }

    pub fn compute_nrows(
        &self,
        seg: AnyDataSegment,
        conf: &ReaderConfig,
    ) -> BiTentative<usize, UnevenEventWidth>
    where
        S: Clone,
        C: IsFixed,
    {
        let n = seg.inner.len() as usize;
        let w = self.event_width();
        let total_events = n / w;
        let remainder = n % w;
        if remainder > 0 {
            let i = UnevenEventWidth {
                event_width: w,
                nbytes: n,
                remainder,
            };
            Tentative::new_either(total_events, vec![i], !conf.allow_uneven_event_width)
        } else {
            Tentative::new1(total_events)
        }
    }

    fn req_keywords<X>(&self, byteord: X) -> [(String, String); 2]
    where
        S: Copy,
        X: ReqMetarootKey,
        C: HasDatatype,
    {
        [byteord.pair(), C::DATATYPE.pair()]
    }

    fn req_meas_keywords(&self) -> NonEmpty<(String, String, String)>
    where
        C: IsFixed,
    {
        let xs = self.columns.as_ref().enumerate().map(|(i, c)| {
            let [x, y] = c.req_meas_keywords(i.into());
            NonEmpty::from((x, vec![y]))
        });
        NonEmpty::flatten(xs)
    }
}

impl<C, const LEN: usize, S, T> FixedLayout<FloatRange<C, LEN>, S, T> {
    // pub(crate) fn new_floating(
    //     &self,
    //     ranges: NonEmpty<C>,
    //     byte_layout: S,
    // ) -> Result<Self, NanFloatError>
    // where
    //     NonNanFloat<C>: TryFrom<C, Error = NanFloatError>,
    // {
    //     let columns =
    //         ranges.try_map(|r| NonNanFloat::try_from(r).map(|range| FloatRange { range }))?;
    //     Ok(FixedLayout::new(columns, byte_layout))
    // }

    // fn insert_float(
    //     &mut self,
    //     index: MeasIndex,
    //     range: Range,
    //     notrunc: bool,
    // ) -> DeferredResult<(), FloatRangeError, FloatInsertColumnError>
    // where
    //     C: FloatFromBytes<LEN>,
    //     NonNanFloat<C>: TryFrom<FloatOrInt, Error = ToFloatError<C>>,
    // {
    //     C::column_type(range, notrunc)
    //         .errors_into()
    //         .and_maybe(|t| self.insert_inner(index, t).into_deferred())
    // }

    fn insert_float_nocheck(
        &mut self,
        index: MeasIndex,
        range: FloatOrInt,
        notrunc: bool,
    ) -> BiTentative<(), FloatRangeError>
    where
        C: Float,
        NonNanFloat<C>: TryFrom<FloatOrInt, Error = ToFloatError<C>>,
    {
        FloatRange::from_range(range, notrunc)
            .errors_into()
            .map(|t| self.insert_inner_nocheck(index, t))
    }

    fn push_float(&mut self, range: FloatOrInt, notrunc: bool) -> BiTentative<(), FloatRangeError>
    where
        C: Float,
        NonNanFloat<C>: TryFrom<FloatOrInt, Error = ToFloatError<C>>,
    {
        FloatRange::from_range(range, notrunc).map(|t| self.push_inner(t))
    }
}

// impl<C, const LEN: usize, T> FixedLayout<Bitmask<C, LEN>, SizedByteOrd<LEN>, T>
// where
//     C: IntFromBytes<LEN> + TryFrom<usize> + Ord,
// {
//     pub(crate) fn new_ordered_uint(
//         &self,
//         ranges: NonEmpty<C>,
//         byte_layout: SizedByteOrd<LEN>,
//         notrunc: bool,
//     ) -> BiTentative<Self, bitmask::BitmaskTruncationError>
//     where
//         C: num_traits::PrimInt,
//         u64: From<C>,
//     {
//         Tentative::mconcat_ne(ranges.map(|b| Bitmask::from_native_tnt(b, notrunc)))
//             .map(|columns| Self::new(columns, byte_layout))
//     }
// }

impl<T> FixedLayout<AnyNullBitmask, Endian, T> {
    // pub(crate) fn new_endian_uint(
    //     &self,
    //     ranges: NonEmpty<Range>,
    //     byte_layout: Endian,
    // ) -> BiTentative<Self, BitmaskError> {
    //     Tentative::mconcat_ne(widths.map(|w| AnyUintType::new1(w.width, w.range, true)))
    //         .map(|columns| Self::new(columns, byte_layout))
    // }

    // fn insert_uint(
    //     &mut self,
    //     index: MeasIndex,
    //     range: Range,
    //     notrunc: bool,
    // ) -> DeferredResult<(), IntRangeError, IntInsertColumnError> {
    //     AnyUintType::new_from_range(range, notrunc)
    //         .errors_into()
    //         .and_maybe(|t| self.insert_inner(index, t).into_deferred())
    // }

    fn insert_uint_nocheck(
        &mut self,
        index: MeasIndex,
        range: FloatOrInt,
        notrunc: bool,
    ) -> BiTentative<(), IntRangeError> {
        AnyBitmask::from_range(range, notrunc).map(|t| self.insert_inner_nocheck(index, t))
    }

    fn push_uint(&mut self, range: FloatOrInt, notrunc: bool) -> BiTentative<(), IntRangeError> {
        AnyBitmask::from_range(range, notrunc).map(|t| self.push_inner(t))
    }
}

// impl<T> FixedLayout<NullMixedType, Endian, T> {
//     pub(crate) fn new_mixed(
//         &self,
//         widths: NonEmpty<MixedColumnValues>,
//         byte_layout: Endian,
//     ) -> BiTentative<Self, BitmaskError> {
//         Tentative::mconcat_ne(widths.map(|w| AnyUintType::new1(w.width, w.range, true)))
//             .map(|columns| Self::new(columns, byte_layout))
//     }
// }

macro_rules! def_native_wrapper {
    ($name:path, $native:ty, $size:expr, $native_size:expr, $bytes:ident) => {
        impl HasNativeType for $name {
            type Native = $native;
        }

        impl HasNativeWidth for $name {
            const BYTES: Bytes = Bytes::$bytes;
            const LEN: usize = $native_size;
            type Order = SizedByteOrd<$size>;
        }
    };
}

def_native_wrapper!(Bitmask08, u8, 1, 1, B1);
def_native_wrapper!(Bitmask16, u16, 2, 2, B2);
def_native_wrapper!(Bitmask24, u32, 3, 4, B3);
def_native_wrapper!(Bitmask32, u32, 4, 4, B4);
def_native_wrapper!(Bitmask40, u64, 5, 8, B5);
def_native_wrapper!(Bitmask48, u64, 6, 8, B6);
def_native_wrapper!(Bitmask56, u64, 7, 8, B7);
def_native_wrapper!(Bitmask64, u64, 8, 8, B8);
def_native_wrapper!(F32Range, f32, 4, 4, B4);
def_native_wrapper!(F64Range, f64, 8, 8, B8);

impl HasNativeType for AsciiRange {
    type Native = u64;
}

impl HasDatatype for AsciiRange {
    const DATATYPE: AlphaNumType = AlphaNumType::Ascii;
}

impl<T, const LEN: usize> HasDatatype for Bitmask<T, LEN> {
    const DATATYPE: AlphaNumType = AlphaNumType::Integer;
}

impl HasDatatype for F32Range {
    const DATATYPE: AlphaNumType = AlphaNumType::Single;
}

impl HasDatatype for F64Range {
    const DATATYPE: AlphaNumType = AlphaNumType::Double;
}

impl HasDatatype for AnyNullBitmask {
    const DATATYPE: AlphaNumType = AlphaNumType::Integer;
}

impl<T, const LEN: usize> IsFixed for Bitmask<T, LEN>
where
    Self: HasNativeWidth,
    u64: From<T>,
    T: Copy,
{
    fn nbytes(&self) -> u8 {
        Self::BYTES.into()
    }

    fn fixed_width(&self) -> BitsOrChars {
        Self::BYTES.into()
    }

    fn range(&self) -> Range {
        let x = u64::from(self.value());
        // TODO fix u64 max
        Range(if x == u64::MAX { x } else { x + 1 }.into())
    }
}

impl IsFixed for AnyNullBitmask {
    fn nbytes(&self) -> u8 {
        match_many_to_one!(
            self,
            Self,
            [Uint08, Uint16, Uint24, Uint32, Uint40, Uint48, Uint56, Uint64],
            x,
            { x.nbytes() }
        )
    }

    fn fixed_width(&self) -> BitsOrChars {
        match_many_to_one!(
            self,
            Self,
            [Uint08, Uint16, Uint24, Uint32, Uint40, Uint48, Uint56, Uint64],
            x,
            { x.fixed_width() }
        )
    }

    fn range(&self) -> Range {
        match_many_to_one!(
            self,
            Self,
            [Uint08, Uint16, Uint24, Uint32, Uint40, Uint48, Uint56, Uint64],
            x,
            { x.range() }
        )
    }
}

impl<T, const LEN: usize> IsFixed for FloatRange<T, LEN>
where
    Self: HasNativeWidth,
    T: Copy,
    f64: From<T>,
{
    fn nbytes(&self) -> u8 {
        Self::BYTES.into()
    }

    fn fixed_width(&self) -> BitsOrChars {
        Self::BYTES.into()
    }

    fn range(&self) -> Range {
        Range(FloatOrInt::Float(self.range.inner_into()))
    }
}

impl IsFixed for AsciiRange {
    fn nbytes(&self) -> u8 {
        self.chars().into()
    }

    fn fixed_width(&self) -> BitsOrChars {
        self.chars().into()
    }

    fn range(&self) -> Range {
        Range(self.value().into())
    }
}

impl IsFixed for NullMixedType {
    fn nbytes(&self) -> u8 {
        match_many_to_one!(self, Self, [Ascii, Uint, F32, F64], x, { x.nbytes() })
    }

    fn fixed_width(&self) -> BitsOrChars {
        match_many_to_one!(self, Self, [Ascii, Uint, F32, F64], x, { x.fixed_width() })
    }

    fn range(&self) -> Range {
        match_many_to_one!(self, Self, [Ascii, Uint, F32, F64], x, { x.range() })
    }
}

macro_rules! source_from_iter {
    ($from:ident, $to:ident, $wrap:ident) => {
        impl<'a> From<FCSColIter<'a, $from, $to>> for AnySource<'a, $to> {
            fn from(value: FCSColIter<'a, $from, $to>) -> Self {
                Self::$wrap(value)
            }
        }
    };
}

source_from_iter!(u8, u8, FromU08);
source_from_iter!(u8, u16, FromU08);
source_from_iter!(u8, u32, FromU08);
source_from_iter!(u8, u64, FromU08);
source_from_iter!(u8, f32, FromU08);
source_from_iter!(u8, f64, FromU08);

source_from_iter!(u16, u8, FromU16);
source_from_iter!(u16, u16, FromU16);
source_from_iter!(u16, u32, FromU16);
source_from_iter!(u16, u64, FromU16);
source_from_iter!(u16, f32, FromU16);
source_from_iter!(u16, f64, FromU16);

source_from_iter!(u32, u8, FromU32);
source_from_iter!(u32, u16, FromU32);
source_from_iter!(u32, u32, FromU32);
source_from_iter!(u32, u64, FromU32);
source_from_iter!(u32, f32, FromU32);
source_from_iter!(u32, f64, FromU32);

source_from_iter!(u64, u8, FromU64);
source_from_iter!(u64, u16, FromU64);
source_from_iter!(u64, u32, FromU64);
source_from_iter!(u64, u64, FromU64);
source_from_iter!(u64, f32, FromU64);
source_from_iter!(u64, f64, FromU64);

source_from_iter!(f32, u8, FromF32);
source_from_iter!(f32, u16, FromF32);
source_from_iter!(f32, u32, FromF32);
source_from_iter!(f32, u64, FromF32);
source_from_iter!(f32, f32, FromF32);
source_from_iter!(f32, f64, FromF32);

source_from_iter!(f64, u8, FromF64);
source_from_iter!(f64, u16, FromF64);
source_from_iter!(f64, u32, FromF64);
source_from_iter!(f64, u64, FromF64);
source_from_iter!(f64, f32, FromF64);
source_from_iter!(f64, f64, FromF64);

macro_rules! match_any_uint {
    ($value:expr, $root:ident, $inner:ident, $action:block) => {
        match_many_to_one!(
            $value,
            $root,
            [Uint08, Uint16, Uint24, Uint32, Uint40, Uint48, Uint56, Uint64],
            $inner,
            $action
        )
    };
}

impl<T> AnyOrderedUintLayout<T> {
    // fn layout_values(&self) -> OrderedLayoutValues {
    //     match_any_uint!(self, Self, l, { l.layout_values(()) })
    // }

    fn tot_into<X>(self) -> AnyOrderedUintLayout<X> {
        match_any_uint!(self, Self, l, { l.tot_into().into() })
    }

    fn into_endian(self) -> Result<EndianLayout<AnyNullBitmask>, OrderedToEndianError> {
        match_any_uint!(self, Self, l, {
            l.tot_into()
                .byte_layout_try_into()
                .map(|x| x.columns_into())
        })
    }

    pub(crate) fn try_new<D>(
        cs: NonEmpty<ColumnLayoutValues<D>>,
        o: ByteOrd,
        notrunc: bool,
    ) -> DeferredResult<Self, ColumnError<BitmaskError>, NewFixedIntLayoutError> {
        let n = o.nbytes();
        // First, scan through the widths to make sure they are all fixed and
        // are all the same number of bytes as ByteOrd
        cs.iter()
            .map(|c| Bytes::try_from(c.width))
            .gather()
            .mult_map_errors(SingleFixedWidthError::Bytes)
            .and_then(|widths| {
                NonEmpty::collect(widths.into_iter().filter(|w| *w != n).unique())
                    .map_or(Ok(()), |ws| Err(NonEmpty::new(MultiWidthsError(ws).into())))
            })
            .mult_to_deferred()
            .def_and_maybe(|_| {
                // Second, make the layout, and force all columns to the correct
                // type based on ByteOrd. It is necessary to check the columns
                // first because the bitmask won't necessarily fail even if it
                // is larger than the target type.
                match_many_to_one!(o, ByteOrd, [O1, O2, O3, O4, O5, O6, O7, O8], o, {
                    FixedLayout::try_new(cs, o, |c| {
                        Ok(Bitmask::from_range(c.range.0, notrunc).errors_into())
                    })
                    .def_map_value(|x| x.into())
                })
            })
    }

    // fn insert(
    //     &mut self,
    //     index: MeasIndex,
    //     range: Range,
    //     notrunc: bool,
    // ) -> DeferredResult<(), IntRangeError, IntInsertColumnError> {
    //     match_any_uint!(self, Self, l, {
    //         IntFromBytes::column_type_from_range(range, notrunc)
    //             .errors_into()
    //             .and_maybe(|t| l.insert_inner(index, t).into_deferred())
    //             .def_map_value(|_| ())
    //     })
    // }

    fn insert_nocheck(
        &mut self,
        index: MeasIndex,
        range: FloatOrInt,
        notrunc: bool,
    ) -> BiTentative<(), BitmaskError> {
        match_any_uint!(self, Self, l, {
            Bitmask::from_range(range, notrunc).map(|t| l.insert_inner_nocheck(index, t))
        })
    }

    fn push(&mut self, range: FloatOrInt, notrunc: bool) -> BiTentative<(), BitmaskError> {
        match_any_uint!(self, Self, l, {
            Bitmask::from_range(range, notrunc)
                .map(|t| l.push_inner(t))
                .void()
        })
    }

    fn remove(&mut self, index: MeasIndex) -> Result<(), ClearOptionalOr<IndexError>> {
        match_any_uint!(self, Self, l, { l.remove_inner(index) })
    }

    fn remove_nocheck(&mut self, index: MeasIndex) -> Result<(), ClearOptional> {
        match_any_uint!(self, Self, l, { l.remove_nocheck_inner(index) })
    }

    fn ncols(&self) -> usize {
        match_any_uint!(self, Self, l, { l.columns.len() })
    }

    fn nbytes(&self, df: &FCSDataFrame) -> u64 {
        match_any_uint!(self, Self, l, { l.nbytes(df) })
    }

    fn h_read_df<R: Read, W, E>(
        &self,
        h: &mut BufReader<R>,
        tot: T::Tot,
        seg: AnyDataSegment,
        conf: &ReaderConfig,
    ) -> IODeferredResult<FCSDataFrame, W, E>
    where
        W: From<UnevenEventWidth> + From<TotEventMismatch>,
        E: From<UnevenEventWidth> + From<TotEventMismatch>,
        T: TotDefinition,
    {
        match_any_uint!(self, Self, l, {
            l.h_read_df_numeric::<_, ColumnReader<_, _, _>, _, E>(h, tot, seg, conf)
        })
    }

    fn check_writer(&self, df: &FCSDataFrame) -> MultiResult<(), ColumnError<AnyLossError>> {
        match_any_uint!(self, Self, l, {
            l.check_writer::<ColumnWriter<_, _, _>>(df)
        })
    }

    fn h_write_df<W: Write>(&self, h: &mut BufWriter<W>, df: &FCSDataFrame) -> io::Result<()> {
        match_any_uint!(self, Self, l, {
            l.h_write_df::<_, ColumnWriter<_, _, _>>(h, df)
        })
    }

    fn req_keywords(&self) -> [(String, String); 2] {
        match_any_uint!(self, Self, l, {
            l.req_keywords(ByteOrd::from(l.byte_layout))
        })
    }

    fn req_meas_keywords(&self) -> NonEmpty<(String, String, String)> {
        match_any_uint!(self, Self, l, { l.req_meas_keywords() })
    }
}

impl<T> AnyAsciiLayout<T> {
    // fn layout_values<D: Copy, S: Default>(&self, datatype: D) -> LayoutValues<S, D> {
    //     match self {
    //         Self::Delimited(x) => x.layout_values(datatype),
    //         Self::Fixed(x) => x.ascii_layout_values(datatype),
    //     }
    // }

    fn tot_into<X>(self) -> AnyAsciiLayout<X> {
        match self {
            Self::Delimited(x) => AnyAsciiLayout::Delimited(DelimAsciiLayout::new(x.ranges)),
            Self::Fixed(x) => AnyAsciiLayout::Fixed(x.tot_into()),
        }
    }

    pub(crate) fn try_new<D>(
        cs: NonEmpty<ColumnLayoutValues<D>>,
        notrunc: bool,
    ) -> DeferredResult<
        Self,
        ColumnError<IntRangeError>,
        ColumnError<ascii_range::NewAsciiRangeError>,
    > {
        let go = |error: IntRangeError, i: usize| ColumnError {
            error,
            index: i.into(),
        };
        if cs.iter().all(|c| c.width == Width::Variable) {
            let ts = cs.enumerate().map(|(i, c)| {
                c.range
                    .0
                    .as_uint(notrunc)
                    .map_errors(|e| go(e, i))
                    .map_warnings(|e| go(e, i))
            });
            let ret = Tentative::mconcat_ne(ts)
                .map(|ranges| AnyAsciiLayout::Delimited(DelimAsciiLayout::new(ranges)))
                .map_errors(|e| e.inner_into());
            Ok(ret)
        } else {
            FixedLayout::try_new(cs, (), |c| {
                AsciiRange::from_width_and_range(c.width, c.range, notrunc)
            })
            .def_map_value(Self::Fixed)
        }
    }

    // fn insert(
    //     &mut self,
    //     index: MeasIndex,
    //     range: Range,
    //     notrunc: bool,
    // ) -> DeferredResult<(), IntRangeError, IntInsertColumnError> {
    //     IntFromBytes::<8>::range_to_int(range, notrunc)
    //         .errors_into()
    //         .and_maybe(|x| match self {
    //             Self::Fixed(l) => l
    //                 .insert_inner(index, AsciiType::from_u64(x))
    //                 .into_deferred(),
    //             Self::Delimited(l) => l.insert(index, x).into_deferred(),
    //         })
    // }

    fn insert_nocheck(
        &mut self,
        index: MeasIndex,
        range: FloatOrInt,
        notrunc: bool,
    ) -> BiTentative<(), IntRangeError> {
        range.as_uint::<u64>(notrunc).map(|x| match self {
            Self::Fixed(l) => l.insert_inner_nocheck(index, x.into()),
            Self::Delimited(l) => l.insert_unchecked(index, x),
        })
    }

    fn push(&mut self, range: FloatOrInt, notrunc: bool) -> BiTentative<(), IntRangeError> {
        range.as_uint::<u64>(notrunc).map(|x| match self {
            Self::Fixed(l) => l.push_inner(x.into()),
            Self::Delimited(l) => l.push(x),
        })
    }

    fn remove(&mut self, index: MeasIndex) -> Result<(), ClearOptionalOr<IndexError>> {
        match self {
            Self::Fixed(l) => l.remove_inner(index),
            Self::Delimited(l) => l.remove(index),
        }
    }

    fn remove_nocheck(&mut self, index: MeasIndex) -> Result<(), ClearOptional> {
        match self {
            Self::Fixed(l) => l.remove_nocheck_inner(index),
            Self::Delimited(l) => l.remove_nocheck(index),
        }
    }

    fn new_fixed(columns: NonEmpty<AsciiRange>) -> Self {
        Self::Fixed(FixedLayout::new(columns, ()))
    }

    fn new_delim(ranges: NonEmpty<u64>) -> Self {
        Self::Delimited(DelimAsciiLayout::new(ranges))
    }

    fn ncols(&self) -> usize {
        match self {
            Self::Delimited(a) => a.ranges.len(),
            Self::Fixed(l) => l.columns.len(),
        }
    }

    fn nbytes(&self, df: &FCSDataFrame) -> u64 {
        match self {
            Self::Delimited(_) => df.ascii_nbytes(),
            Self::Fixed(l) => l.nbytes(df),
        }
    }

    fn h_read_checked_df<R: Read>(
        &self,
        h: &mut BufReader<R>,
        tot: T::Tot,
        seg: AnyDataSegment,
        conf: &ReaderConfig,
    ) -> IODeferredResult<FCSDataFrame, ReadDataframeWarning, ReadAsciiError>
    where
        T: TotDefinition,
    {
        match self {
            Self::Fixed(c) => {
                let mut buf = vec![];
                c.h_read_df::<_, ColumnReader<_, _, _>, _, _, ReadFixedAsciiError, _>(
                    h, &mut buf, tot, seg, conf,
                )
                .def_map_errors(|e| e.inner_into())
            }
            Self::Delimited(l) => l
                .h_read_df(h, tot, seg.inner.len() as usize)
                .map_err(|e| e.inner_into::<ReadDelimAsciiError>().inner_into())
                .into_deferred(),
        }
    }

    fn check_writer(&self, df: &FCSDataFrame) -> MultiResult<(), ColumnError<AnyLossError>> {
        match self {
            Self::Fixed(l) => l.check_writer::<ColumnWriter<_, _, _>>(df),
            Self::Delimited(l) => l.check_writer(df),
        }
    }

    fn h_write_df<W: Write>(&self, h: &mut BufWriter<W>, df: &FCSDataFrame) -> io::Result<()> {
        match self {
            Self::Fixed(l) => l.h_write_df::<_, ColumnWriter<_, _, _>>(h, df),
            Self::Delimited(l) => l.h_write_df(h, df),
        }
    }

    fn req_keywords<X: Default + ReqMetarootKey>(&self) -> [(String, String); 2] {
        match self {
            Self::Fixed(l) => l.req_keywords(X::default()),
            Self::Delimited(l) => l.req_keywords::<X>(),
        }
    }

    fn req_meas_keywords(&self) -> NonEmpty<(String, String, String)> {
        match self {
            Self::Fixed(l) => l.req_meas_keywords(),
            Self::Delimited(l) => l.req_meas_keywords(),
        }
    }
}

impl VersionedDataLayout for DataLayout2_0 {
    type ByteLayout = ByteOrd;
    type ColDatatype = ();
    type Tot = Option<Tot>;

    fn try_new(
        datatype: AlphaNumType,
        byteord: Self::ByteLayout,
        columns: NonEmpty<ColumnLayoutValues<Self::ColDatatype>>,
        conf: &StdTextReadConfig,
    ) -> DeferredResult<Self, ColumnError<NewMixedTypeWarning>, NewDataLayoutError> {
        AnyOrderedLayout::try_new(datatype, byteord, columns, conf)
            .def_map_value(|x| x.into())
            .def_map_warnings(|e| e.inner_into())
    }

    fn lookup(
        kws: &mut StdKeywords,
        conf: &StdTextReadConfig,
        par: Par,
    ) -> LookupLayoutResult<Option<Self>> {
        AnyOrderedLayout::lookup(kws, conf, par).def_map_value(|x| x.map(|y| y.into()))
    }

    fn lookup_ro(kws: &StdKeywords, conf: &StdTextReadConfig) -> FromRawResult<Option<Self>> {
        AnyOrderedLayout::lookup_ro(kws, conf).def_map_value(|x| x.map(|y| y.into()))
    }

    // fn insert(
    //     &mut self,
    //     index: MeasIndex,
    //     range: Range,
    //     notrunc: bool,
    // ) -> DeferredResult<(), LayoutInsertColumnWarning, LayoutInsertColumnError> {
    //     self.0.insert(index, range, notrunc)
    // }

    fn insert_nocheck(
        &mut self,
        index: MeasIndex,
        range: FloatOrInt,
        notrunc: bool,
    ) -> BiTentative<(), LayoutInsertColumnWarning> {
        self.0.insert_nocheck(index, range, notrunc)
    }

    fn push(&mut self, range: FloatOrInt, notrunc: bool) -> BiTentative<(), LayoutPushColumnError> {
        self.0.push(range, notrunc)
    }

    fn remove(&mut self, index: MeasIndex) -> Result<(), ClearOptionalOr<IndexError>> {
        self.0.remove(index)
    }

    fn remove_nocheck(&mut self, index: MeasIndex) -> Result<(), ClearOptional> {
        self.0.remove_nocheck(index)
    }

    fn ncols(&self) -> usize {
        self.0.ncols()
    }

    fn nbytes(&self, df: &FCSDataFrame) -> u64 {
        self.0.nbytes(df)
    }

    fn h_read_df_inner<R: Read>(
        &self,
        h: &mut BufReader<R>,
        tot: Self::Tot,
        seg: AnyDataSegment,
        conf: &ReaderConfig,
    ) -> IODeferredResult<FCSDataFrame, ReadDataframeWarning, ReadDataframeError> {
        self.0.h_read_checked_df(h, tot, seg, conf)
    }

    fn check_writer(&self, df: &FCSDataFrame) -> MultiResult<(), ColumnError<AnyLossError>> {
        self.0.check_writer(df)
    }

    fn h_write_df_inner<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        df: &FCSDataFrame,
    ) -> io::Result<()> {
        self.0.h_write_df(h, df)
    }

    // fn layout_values(&self) -> OrderedLayoutValues {
    //     self.0.layout_values()
    // }

    fn req_keywords(&self) -> [(String, String); 2] {
        self.0.req_keywords()
    }

    fn req_meas_keywords(&self) -> NonEmpty<(String, String, String)> {
        self.0.req_meas_keywords()
    }

    fn opt_meas_keywords(&self) -> Vec<(String, String, Option<String>)> {
        vec![]
    }
}

impl VersionedDataLayout for DataLayout3_0 {
    type ByteLayout = ByteOrd;
    type ColDatatype = ();
    type Tot = Tot;

    fn try_new(
        datatype: AlphaNumType,
        byteord: Self::ByteLayout,
        columns: NonEmpty<ColumnLayoutValues<Self::ColDatatype>>,
        conf: &StdTextReadConfig,
    ) -> DeferredResult<Self, ColumnError<NewMixedTypeWarning>, NewDataLayoutError> {
        AnyOrderedLayout::try_new(datatype, byteord, columns, conf)
            .def_map_value(|x| x.into())
            .def_map_warnings(|e| e.inner_into())
    }

    fn lookup(
        kws: &mut StdKeywords,
        conf: &StdTextReadConfig,
        par: Par,
    ) -> LookupLayoutResult<Option<Self>> {
        AnyOrderedLayout::lookup(kws, conf, par).def_map_value(|x| x.map(|y| y.into()))
    }

    fn lookup_ro(kws: &StdKeywords, conf: &StdTextReadConfig) -> FromRawResult<Option<Self>> {
        AnyOrderedLayout::lookup_ro(kws, conf).def_map_value(|x| x.map(|y| y.into()))
    }

    // fn insert(
    //     &mut self,
    //     index: MeasIndex,
    //     range: Range,
    //     notrunc: bool,
    // ) -> DeferredResult<(), LayoutInsertColumnWarning, LayoutInsertColumnError> {
    //     self.0.insert(index, range, notrunc)
    // }

    fn insert_nocheck(
        &mut self,
        index: MeasIndex,
        range: FloatOrInt,
        notrunc: bool,
    ) -> BiTentative<(), LayoutInsertColumnWarning> {
        self.0.insert_nocheck(index, range, notrunc)
    }

    fn push(&mut self, range: FloatOrInt, notrunc: bool) -> BiTentative<(), LayoutPushColumnError> {
        self.0.push(range, notrunc)
    }

    fn remove(&mut self, index: MeasIndex) -> Result<(), ClearOptionalOr<IndexError>> {
        self.0.remove(index)
    }

    fn remove_nocheck(&mut self, index: MeasIndex) -> Result<(), ClearOptional> {
        self.0.remove_nocheck(index)
    }

    fn ncols(&self) -> usize {
        self.0.ncols()
    }

    fn nbytes(&self, df: &FCSDataFrame) -> u64 {
        self.0.nbytes(df)
    }

    fn h_read_df_inner<R: Read>(
        &self,
        h: &mut BufReader<R>,
        tot: Self::Tot,
        seg: AnyDataSegment,
        conf: &ReaderConfig,
    ) -> IODeferredResult<FCSDataFrame, ReadDataframeWarning, ReadDataframeError> {
        self.0.h_read_checked_df(h, tot, seg, conf)
    }

    fn check_writer(&self, df: &FCSDataFrame) -> MultiResult<(), ColumnError<AnyLossError>> {
        self.0.check_writer(df)
    }

    fn h_write_df_inner<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        df: &FCSDataFrame,
    ) -> io::Result<()> {
        self.0.h_write_df(h, df)
    }

    // fn layout_values(&self) -> OrderedLayoutValues {
    //     self.0.layout_values()
    // }

    fn req_keywords(&self) -> [(String, String); 2] {
        self.0.req_keywords()
    }

    fn req_meas_keywords(&self) -> NonEmpty<(String, String, String)> {
        self.0.req_meas_keywords()
    }

    fn opt_meas_keywords(&self) -> Vec<(String, String, Option<String>)> {
        vec![]
    }
}

impl VersionedDataLayout for DataLayout3_1 {
    type ByteLayout = Endian;
    type ColDatatype = ();
    type Tot = Tot;

    fn try_new(
        datatype: AlphaNumType,
        endian: Self::ByteLayout,
        columns: NonEmpty<ColumnLayoutValues<Self::ColDatatype>>,
        conf: &StdTextReadConfig,
    ) -> DeferredResult<Self, ColumnError<NewMixedTypeWarning>, NewDataLayoutError> {
        NonMixedEndianLayout::try_new(datatype, endian, columns, conf)
            .def_map_value(|x| x.into())
            .def_map_warnings(|e| e.inner_into())
    }

    fn lookup(
        kws: &mut StdKeywords,
        conf: &StdTextReadConfig,
        par: Par,
    ) -> LookupLayoutResult<Option<Self>> {
        let cs = ColumnLayoutValues2_0::lookup_all(kws, par);
        let d = AlphaNumType::lookup_req_check_ascii(kws);
        let n = Endian::lookup_req(kws);
        d.def_zip3(n, cs)
            .def_inner_into()
            .def_and_maybe(|(datatype, byteord, columns)| {
                def_transpose(
                    NonEmpty::from_vec(columns)
                        .map(|xs| Self::try_new(datatype, byteord, xs, conf)),
                )
                .def_inner_into()
            })
    }

    fn lookup_ro(kws: &StdKeywords, conf: &StdTextReadConfig) -> FromRawResult<Option<Self>> {
        let cs = ColumnLayoutValues2_0::lookup_ro_all(kws);
        let d = AlphaNumType::get_metaroot_req(kws).into_deferred();
        let n = Endian::get_metaroot_req(kws).into_deferred();
        d.def_zip3(n, cs)
            .def_inner_into()
            .def_and_maybe(|(datatype, byteord, columns)| {
                def_transpose(
                    NonEmpty::from_vec(columns)
                        .map(|xs| Self::try_new(datatype, byteord, xs, conf)),
                )
                .def_inner_into()
            })
    }

    // fn insert(
    //     &mut self,
    //     index: MeasIndex,
    //     range: Range,
    //     notrunc: bool,
    // ) -> DeferredResult<(), LayoutInsertColumnWarning, LayoutInsertColumnError> {
    //     self.0.insert(index, range, notrunc)
    // }

    fn insert_nocheck(
        &mut self,
        index: MeasIndex,
        range: FloatOrInt,
        notrunc: bool,
    ) -> BiTentative<(), LayoutInsertColumnWarning> {
        self.0.insert_nocheck(index, range, notrunc)
    }

    fn push(&mut self, range: FloatOrInt, notrunc: bool) -> BiTentative<(), LayoutPushColumnError> {
        self.0.push(range, notrunc)
    }

    fn remove(&mut self, index: MeasIndex) -> Result<(), ClearOptionalOr<IndexError>> {
        self.0.remove(index)
    }

    fn remove_nocheck(&mut self, index: MeasIndex) -> Result<(), ClearOptional> {
        self.0.remove_nocheck(index)
    }

    fn ncols(&self) -> usize {
        self.0.ncols()
    }

    fn nbytes(&self, df: &FCSDataFrame) -> u64 {
        self.0.nbytes(df)
    }

    fn h_read_df_inner<R: Read>(
        &self,
        h: &mut BufReader<R>,
        tot: Self::Tot,
        seg: AnyDataSegment,
        conf: &ReaderConfig,
    ) -> IODeferredResult<FCSDataFrame, ReadDataframeWarning, ReadDataframeError> {
        self.0.h_read_df(h, tot, seg, conf)
    }

    fn check_writer(&self, df: &FCSDataFrame) -> MultiResult<(), ColumnError<AnyLossError>> {
        self.0.check_writer(df)
    }

    fn h_write_df_inner<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        df: &FCSDataFrame,
    ) -> io::Result<()> {
        self.0.h_write_df(h, df)
    }

    // fn layout_values(&self) -> LayoutValues3_1 {
    //     self.0.layout_values(())
    // }

    fn req_keywords(&self) -> [(String, String); 2] {
        self.0.req_keywords()
    }

    fn req_meas_keywords(&self) -> NonEmpty<(String, String, String)> {
        self.0.req_meas_keywords()
    }

    fn opt_meas_keywords(&self) -> Vec<(String, String, Option<String>)> {
        vec![]
    }
}

impl VersionedDataLayout for DataLayout3_2 {
    type ByteLayout = Endian;
    type ColDatatype = Option<NumType>;
    type Tot = Tot;

    fn try_new(
        datatype: AlphaNumType,
        endian: Self::ByteLayout,
        cs: NonEmpty<ColumnLayoutValues<Self::ColDatatype>>,
        conf: &StdTextReadConfig,
    ) -> DeferredResult<Self, ColumnError<NewMixedTypeWarning>, NewDataLayoutError> {
        let notrunc = conf.disallow_range_truncation;
        let unique_dt: Vec<_> = cs
            .iter()
            .map(|c| c.datatype.map(|x| x.into()).unwrap_or(datatype))
            .unique()
            .collect();
        match unique_dt[..] {
            [dt] => {
                let ds = cs.map(|c| ColumnLayoutValues {
                    width: c.width,
                    range: c.range,
                    datatype: (),
                });
                NonMixedEndianLayout::try_new(dt, endian, ds, conf)
                    .def_map_value(Self::NonMixed)
                    .def_map_warnings(|e| e.inner_into())
            }
            _ => FixedLayout::try_new(cs, endian, |c| {
                MixedType::from_width_and_range(c.width, c.range, c.datatype, notrunc)
            })
            .def_map_value(Self::Mixed),
        }
    }

    fn lookup(
        kws: &mut StdKeywords,
        conf: &StdTextReadConfig,
        par: Par,
    ) -> LookupLayoutResult<Option<Self>> {
        let d = AlphaNumType::lookup_req_check_ascii(kws);
        let e = Endian::lookup_req(kws);
        let cs = ColumnLayoutValues3_2::lookup_all(kws, par);
        d.def_zip3(e, cs)
            .def_inner_into()
            .def_and_maybe(|(datatype, endian, columns)| {
                def_transpose(
                    NonEmpty::from_vec(columns).map(|xs| Self::try_new(datatype, endian, xs, conf)),
                )
                .def_inner_into()
            })
    }

    fn lookup_ro(kws: &StdKeywords, conf: &StdTextReadConfig) -> FromRawResult<Option<Self>> {
        let d = AlphaNumType::get_metaroot_req(kws)
            .map_err(RawParsedError::from)
            .into_deferred();
        let e = Endian::get_metaroot_req(kws)
            .map_err(RawParsedError::from)
            .into_deferred();
        let cs = ColumnLayoutValues3_2::lookup_ro_all(kws).def_inner_into();
        d.def_zip3(e, cs)
            .def_and_maybe(|(datatype, endian, columns)| {
                def_transpose(
                    NonEmpty::from_vec(columns).map(|xs| Self::try_new(datatype, endian, xs, conf)),
                )
                .def_inner_into()
            })
    }

    // fn insert(
    //     &mut self,
    //     index: MeasIndex,
    //     range: Range,
    //     notrunc: bool,
    // ) -> DeferredResult<(), LayoutInsertColumnWarning, LayoutInsertColumnError> {
    //     match self {
    //         Self::NonMixed(x) => x.insert(index, range, notrunc),
    //         Self::Mixed(x) => x
    //             .insert_inner(index, MixedType::new_from_range(range.0))
    //             // TODO this is confusing
    //             .map_err(IntInsertColumnError::Index)
    //             .into_deferred(),
    //     }
    // }

    fn insert_nocheck(
        &mut self,
        index: MeasIndex,
        range: FloatOrInt,
        notrunc: bool,
    ) -> BiTentative<(), LayoutInsertColumnWarning> {
        match self {
            Self::NonMixed(x) => x.insert_nocheck(index, range, notrunc),
            Self::Mixed(x) => {
                x.insert_inner_nocheck(index, MixedType::from_range(range));
                Tentative::default()
            }
        }
    }

    fn push(&mut self, range: FloatOrInt, notrunc: bool) -> BiTentative<(), LayoutPushColumnError> {
        match self {
            Self::NonMixed(x) => x.push(range, notrunc),
            Self::Mixed(x) => {
                x.push_inner(MixedType::from_range(range));
                Tentative::default()
            }
        }
    }

    fn remove(&mut self, index: MeasIndex) -> Result<(), ClearOptionalOr<IndexError>> {
        match self {
            Self::NonMixed(x) => x.remove(index),
            Self::Mixed(x) => x.remove_inner(index),
        }
    }

    fn remove_nocheck(&mut self, index: MeasIndex) -> Result<(), ClearOptional> {
        match self {
            Self::NonMixed(x) => x.remove_nocheck(index),
            Self::Mixed(x) => x.remove_nocheck_inner(index),
        }
    }

    fn ncols(&self) -> usize {
        match self {
            Self::NonMixed(x) => x.ncols(),
            Self::Mixed(m) => m.ncols(),
        }
    }

    fn nbytes(&self, df: &FCSDataFrame) -> u64 {
        match self {
            Self::NonMixed(x) => x.nbytes(df),
            Self::Mixed(m) => m.nbytes(df),
        }
    }

    fn h_read_df_inner<R: Read>(
        &self,
        h: &mut BufReader<R>,
        tot: Self::Tot,
        seg: AnyDataSegment,
        conf: &ReaderConfig,
    ) -> IODeferredResult<FCSDataFrame, ReadDataframeWarning, ReadDataframeError> {
        match self {
            Self::NonMixed(x) => x.h_read_df(h, tot, seg, conf),
            Self::Mixed(m) => {
                let mut buf = vec![];
                m.h_read_df::<_, ReaderMixedType, _, _, _, _>(h, &mut buf, tot, seg, conf)
            }
        }
    }

    fn check_writer(&self, df: &FCSDataFrame) -> MultiResult<(), ColumnError<AnyLossError>> {
        match self {
            Self::NonMixed(x) => x.check_writer(df),
            Self::Mixed(m) => m.check_writer::<WriterMixedType>(df),
        }
    }

    fn h_write_df_inner<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        df: &FCSDataFrame,
    ) -> io::Result<()> {
        match self {
            Self::NonMixed(x) => x.h_write_df(h, df),
            Self::Mixed(m) => m.h_write_df::<_, WriterMixedType>(h, df),
        }
    }

    // fn layout_values(&self) -> LayoutValues3_2 {
    //     match self {
    //         Self::NonMixed(x) => x.layout_values(None),
    //         Self::Mixed(x) => x.mixed_layout_values(),
    //     }
    // }

    fn req_keywords(&self) -> [(String, String); 2] {
        match self {
            Self::NonMixed(x) => x.req_keywords(),
            Self::Mixed(x) => [x.primary_datatype().pair(), x.byte_layout.pair()],
        }
    }

    fn req_meas_keywords(&self) -> NonEmpty<(String, String, String)> {
        match self {
            Self::NonMixed(x) => x.req_meas_keywords(),
            Self::Mixed(x) => x.req_meas_keywords(),
        }
    }

    fn opt_meas_keywords(&self) -> Vec<(String, String, Option<String>)> {
        match self {
            Self::NonMixed(_) => vec![],
            Self::Mixed(x) => {
                let dt = x.primary_datatype();
                x.columns
                    .iter()
                    .map(|c| c.as_num_type())
                    .filter(|&pdt| pdt.is_some_and(|y| AlphaNumType::from(y) != dt))
                    .enumerate()
                    // TODO into thing is silly
                    .map(|(i, y)| NumType::triple(&y.into(), i.into()))
                    .collect()
            }
        }
    }
}

impl DataLayout3_1 {
    pub(crate) fn into_ordered<T>(self) -> LayoutConvertResult<AnyOrderedLayout<T>> {
        self.0.into_ordered()
    }
}

impl DataLayout3_2 {
    pub(crate) fn into_ordered<T>(self) -> LayoutConvertResult<AnyOrderedLayout<T>> {
        match self {
            Self::NonMixed(x) => x.into_ordered(),
            Self::Mixed(x) => x.try_into_ordered().mult_errors_into(),
        }
    }

    fn new_mixed(ranges: NonEmpty<NullMixedType>, endian: Endian) -> Self {
        // check if the mixed types are all the same, in which case we can use a
        // simpler layout
        if let Ok(rs) = ranges.as_ref().try_map(|x| AsciiRange::try_from(*x)) {
            Self::NonMixed(NonMixedEndianLayout::new_ascii_fixed(rs))
        } else if let Ok(rs) = ranges.as_ref().try_map(|x| AnyNullBitmask::try_from(*x)) {
            Self::NonMixed(NonMixedEndianLayout::new_uint(rs, endian))
        } else if let Ok(rs) = ranges.as_ref().try_map(|x| F32Range::try_from(*x)) {
            Self::NonMixed(NonMixedEndianLayout::new_f32(rs, endian))
        } else if let Ok(rs) = ranges.as_ref().try_map(|x| F64Range::try_from(*x)) {
            Self::NonMixed(NonMixedEndianLayout::new_f64(rs, endian))
        } else {
            Self::Mixed(FixedLayout::new(ranges, endian))
        }
    }

    // fn new_ascii_fixed(ranges: NonEmpty<AsciiRange>) -> Self {
    //     Self::Ascii(AnyAsciiLayout::new_fixed(ranges))
    // }

    // fn new_ascii_delim(ranges: NonEmpty<u64>) -> Self {
    //     Self::Ascii(AnyAsciiLayout::new_delim(ranges))
    // }

    // fn new_uint(columns: NonEmpty<AnyNullBitmask>, byte_layout: Endian) -> Self {
    //     Self::Integer(FixedLayout::new(columns, byte_layout))
    // }

    // fn new_f32(ranges: NonEmpty<NonNanF32>, byte_layout: Endian) -> Self {
    //     let rs = ranges.map(|range| FloatRange { range });
    //     Self::F32(FixedLayout::new(rs, byte_layout))
    // }

    // fn new_f64(ranges: NonEmpty<NonNanF64>, byte_layout: Endian) -> Self {
    //     let rs = ranges.map(|range| FloatRange { range });
    //     Self::F64(FixedLayout::new(rs, byte_layout))
    // }
}

impl<T> AnyOrderedLayout<T> {
    // fn layout_values(&self) -> OrderedLayoutValues {
    //     match self {
    //         Self::Ascii(x) => x.layout_values(()),
    //         Self::Integer(x) => x.layout_values(),
    //         Self::F32(x) => x.layout_values(()),
    //         Self::F64(x) => x.layout_values(()),
    //     }
    // }

    pub fn new_ascii_fixed(ranges: NonEmpty<AsciiRange>) -> Self {
        Self::Ascii(AnyAsciiLayout::new_fixed(ranges))
    }

    pub fn new_ascii_delim(ranges: NonEmpty<u64>) -> Self {
        Self::Ascii(AnyAsciiLayout::new_delim(ranges))
    }

    pub fn new_uint<U, const LEN: usize>(
        columns: NonEmpty<Bitmask<U, LEN>>,
        byte_layout: SizedByteOrd<LEN>,
    ) -> Self
    where
        AnyOrderedUintLayout<T>: From<FixedLayout<Bitmask<U, LEN>, SizedByteOrd<LEN>, T>>,
    {
        Self::Integer(FixedLayout::new(columns, byte_layout).into())
    }

    pub fn new_f32(ranges: NonEmpty<F32Range>, byte_layout: SizedByteOrd<4>) -> Self {
        Self::F32(FixedLayout::new(ranges, byte_layout))
    }

    pub fn new_f64(ranges: NonEmpty<F64Range>, byte_layout: SizedByteOrd<8>) -> Self {
        Self::F64(FixedLayout::new(ranges, byte_layout))
    }

    fn try_new(
        datatype: AlphaNumType,
        byteord: ByteOrd,
        columns: NonEmpty<ColumnLayoutValues2_0>,
        conf: &StdTextReadConfig,
    ) -> DeferredResult<Self, ColumnError<NewMixedTypeWarning>, NewDataLayoutError> {
        let notrunc = conf.disallow_range_truncation;
        match datatype {
            AlphaNumType::Ascii => AnyAsciiLayout::try_new(columns, notrunc)
                .def_map_value(Self::Ascii)
                .def_errors_into()
                .def_map_warnings(|w| w.inner_into()),
            AlphaNumType::Integer => AnyOrderedUintLayout::try_new(columns, byteord, notrunc)
                .def_map_value(Self::Integer)
                .def_map_warnings(|e| e.inner_into())
                .def_inner_into(),
            AlphaNumType::Single => byteord.try_into().into_deferred().def_and_maybe(|b| {
                FixedLayout::try_new(columns, b, |c| {
                    F32Range::from_width_and_range(c.width, c.range, notrunc).def_warnings_into()
                })
                .def_map_value(Self::F32)
            }),
            AlphaNumType::Double => byteord.try_into().into_deferred().def_and_maybe(|b| {
                FixedLayout::try_new(columns, b, |c| {
                    F64Range::from_width_and_range(c.width, c.range, notrunc).def_warnings_into()
                })
                .def_map_value(Self::F64)
            }),
        }
    }

    fn lookup(
        kws: &mut StdKeywords,
        conf: &StdTextReadConfig,
        par: Par,
    ) -> LookupLayoutResult<Option<Self>> {
        let cs = ColumnLayoutValues2_0::lookup_all(kws, par);
        let d = AlphaNumType::lookup_req(kws);
        let b = ByteOrd::lookup_req(kws);
        d.def_zip3(b, cs)
            .def_inner_into()
            .def_and_maybe(|(datatype, byteord, columns)| {
                def_transpose(
                    NonEmpty::from_vec(columns)
                        .map(|xs| Self::try_new(datatype, byteord, xs, conf)),
                )
                .def_inner_into()
            })
    }

    fn lookup_ro(kws: &StdKeywords, conf: &StdTextReadConfig) -> FromRawResult<Option<Self>> {
        let cs = ColumnLayoutValues2_0::lookup_ro_all(kws);
        let d = AlphaNumType::get_metaroot_req(kws).into_deferred();
        let b = ByteOrd::get_metaroot_req(kws).into_deferred();
        d.def_zip3(b, cs)
            .def_inner_into()
            .def_and_maybe(|(datatype, byteord, columns)| {
                def_transpose(
                    NonEmpty::from_vec(columns)
                        .map(|xs| Self::try_new(datatype, byteord, xs, conf)),
                )
                .def_inner_into()
            })
    }

    // fn insert(
    //     &mut self,
    //     index: MeasIndex,
    //     range: Range,
    //     notrunc: bool,
    // ) -> DeferredResult<(), LayoutInsertColumnWarning, LayoutInsertColumnError> {
    //     match self {
    //         Self::Ascii(l) => l.insert(index, range, notrunc).def_inner_into(),
    //         Self::Integer(l) => l.insert(index, range, notrunc).def_inner_into(),
    //         Self::F32(l) => l.insert_float(index, range, notrunc).def_inner_into(),
    //         Self::F64(l) => l.insert_float(index, range, notrunc).def_inner_into(),
    //     }
    // }

    fn insert_nocheck(
        &mut self,
        index: MeasIndex,
        range: FloatOrInt,
        notrunc: bool,
    ) -> BiTentative<(), LayoutInsertColumnWarning> {
        match self {
            Self::Ascii(l) => l.insert_nocheck(index, range, notrunc).inner_into(),
            Self::Integer(l) => l.insert_nocheck(index, range, notrunc).inner_into(),
            Self::F32(l) => l.insert_float_nocheck(index, range, notrunc).inner_into(),
            Self::F64(l) => l.insert_float_nocheck(index, range, notrunc).inner_into(),
        }
    }

    fn push(&mut self, range: FloatOrInt, notrunc: bool) -> BiTentative<(), LayoutPushColumnError> {
        match self {
            Self::Ascii(l) => l.push(range, notrunc).inner_into(),
            Self::Integer(l) => l.push(range, notrunc).inner_into(),
            Self::F32(l) => l.push_float(range, notrunc).inner_into(),
            Self::F64(l) => l.push_float(range, notrunc).inner_into(),
        }
    }

    fn remove(&mut self, index: MeasIndex) -> Result<(), ClearOptionalOr<IndexError>> {
        match self {
            Self::Ascii(l) => l.remove(index),
            Self::Integer(l) => l.remove(index),
            Self::F32(l) => l.remove_inner(index),
            Self::F64(l) => l.remove_inner(index),
        }
    }

    fn remove_nocheck(&mut self, index: MeasIndex) -> Result<(), ClearOptional> {
        match self {
            Self::Ascii(l) => l.remove_nocheck(index),
            Self::Integer(l) => l.remove_nocheck(index),
            Self::F32(l) => l.remove_nocheck_inner(index),
            Self::F64(l) => l.remove_nocheck_inner(index),
        }
    }

    fn ncols(&self) -> usize {
        match_many_to_one!(self, Self, [Ascii, Integer, F32, F64], x, { x.ncols() })
    }

    fn nbytes(&self, df: &FCSDataFrame) -> u64 {
        match_many_to_one!(self, Self, [Ascii, Integer, F32, F64], x, { x.nbytes(df) })
    }

    pub(crate) fn tot_into<X>(self) -> AnyOrderedLayout<X> {
        match self {
            Self::Ascii(a) => AnyOrderedLayout::Ascii(a.tot_into()),
            Self::Integer(i) => AnyOrderedLayout::Integer(i.tot_into()),
            Self::F32(f) => AnyOrderedLayout::F32(f.tot_into()),
            Self::F64(f) => AnyOrderedLayout::F64(f.tot_into()),
        }
    }

    fn h_read_checked_df<R: Read>(
        &self,
        h: &mut BufReader<R>,
        tot: T::Tot,
        seg: AnyDataSegment,
        conf: &ReaderConfig,
    ) -> IODeferredResult<FCSDataFrame, ReadDataframeWarning, ReadDataframeError>
    where
        T: TotDefinition,
    {
        match self {
            Self::Ascii(x) => x
                .h_read_checked_df(h, tot, seg, conf)
                .def_map_errors(|e| e.inner_into()),
            Self::Integer(x) => x.h_read_df(h, tot, seg, conf),
            Self::F32(x) => {
                x.h_read_df_numeric::<_, ColumnReader<_, _, _>, _, _>(h, tot, seg, conf)
            }
            Self::F64(x) => {
                x.h_read_df_numeric::<_, ColumnReader<_, _, _>, _, _>(h, tot, seg, conf)
            }
        }
    }

    fn check_writer(&self, df: &FCSDataFrame) -> MultiResult<(), ColumnError<AnyLossError>>
    where
        T: TotDefinition,
    {
        match self {
            Self::Ascii(x) => x.check_writer(df),
            Self::Integer(x) => x.check_writer(df),
            Self::F32(x) => x.check_writer::<ColumnWriter<_, _, _>>(df),
            Self::F64(x) => x.check_writer::<ColumnWriter<_, _, _>>(df),
        }
    }

    fn h_write_df<W: Write>(&self, h: &mut BufWriter<W>, df: &FCSDataFrame) -> io::Result<()>
    where
        T: TotDefinition,
    {
        match self {
            Self::Ascii(x) => x.h_write_df(h, df),
            Self::Integer(x) => x.h_write_df(h, df),
            Self::F32(x) => x.h_write_df::<_, ColumnWriter<_, _, _>>(h, df),
            Self::F64(x) => x.h_write_df::<_, ColumnWriter<_, _, _>>(h, df),
        }
    }

    pub fn into_unmixed(self) -> LayoutConvertResult<NonMixedEndianLayout> {
        match self {
            Self::Ascii(x) => Ok(NonMixedEndianLayout::Ascii(x.tot_into())),
            Self::Integer(x) => x.into_endian().map(NonMixedEndianLayout::Integer),
            Self::F32(x) => x
                .tot_into()
                .byte_layout_try_into()
                .map(NonMixedEndianLayout::F32),
            Self::F64(x) => x
                .tot_into()
                .byte_layout_try_into()
                .map(NonMixedEndianLayout::F64),
        }
        .into_mult()
    }

    fn req_keywords(&self) -> [(String, String); 2] {
        match self {
            Self::Ascii(x) => x.req_keywords::<ByteOrd>(),
            Self::Integer(x) => x.req_keywords(),
            Self::F32(x) => x.req_keywords(ByteOrd::from(x.byte_layout)),
            Self::F64(x) => x.req_keywords(ByteOrd::from(x.byte_layout)),
        }
    }

    fn req_meas_keywords(&self) -> NonEmpty<(String, String, String)> {
        match self {
            Self::Ascii(x) => x.req_meas_keywords(),
            Self::Integer(x) => x.req_meas_keywords(),
            Self::F32(x) => x.req_meas_keywords(),
            Self::F64(x) => x.req_meas_keywords(),
        }
    }

    pub(crate) fn into_3_1(self) -> LayoutConvertResult<DataLayout3_1> {
        self.into_unmixed().map(|x| x.into())
    }

    pub(crate) fn into_3_2(self) -> LayoutConvertResult<DataLayout3_2> {
        self.into_unmixed().map(DataLayout3_2::NonMixed)
    }
}

impl NonMixedEndianLayout {
    // fn layout_values<D: Copy>(&self, datatype: D) -> LayoutValues<Endian, D> {
    //     match self {
    //         Self::Ascii(x) => x.layout_values(datatype),
    //         Self::Integer(x) => x.layout_values(datatype),
    //         Self::F32(x) => x.layout_values(datatype),
    //         Self::F64(x) => x.layout_values(datatype),
    //     }
    // }

    fn new_ascii_fixed(ranges: NonEmpty<AsciiRange>) -> Self {
        Self::Ascii(AnyAsciiLayout::new_fixed(ranges))
    }

    fn new_ascii_delim(ranges: NonEmpty<u64>) -> Self {
        Self::Ascii(AnyAsciiLayout::new_delim(ranges))
    }

    fn new_uint(columns: NonEmpty<AnyNullBitmask>, endian: Endian) -> Self {
        Self::Integer(FixedLayout::new(columns, endian))
    }

    fn new_f32(ranges: NonEmpty<F32Range>, endian: Endian) -> Self {
        Self::F32(FixedLayout::new(ranges, endian))
    }

    fn new_f64(ranges: NonEmpty<F64Range>, endian: Endian) -> Self {
        Self::F64(FixedLayout::new(ranges, endian))
    }

    fn try_new(
        datatype: AlphaNumType,
        endian: Endian,
        columns: NonEmpty<ColumnLayoutValues<()>>,
        conf: &StdTextReadConfig,
    ) -> DeferredResult<Self, ColumnError<NewMixedTypeWarning>, NewDataLayoutError> {
        let notrunc = conf.disallow_range_truncation;
        match datatype {
            AlphaNumType::Ascii => AnyAsciiLayout::try_new(columns, notrunc)
                .def_map_value(Self::Ascii)
                .def_errors_into()
                .def_map_warnings(|w| w.inner_into()),
            AlphaNumType::Integer => FixedLayout::endian_uint_try_new(columns, endian, notrunc)
                .def_map_value(Self::Integer)
                .def_map_warnings(|e| e.inner_into())
                .def_inner_into(),
            AlphaNumType::Single => FixedLayout::try_new(columns, endian, |c| {
                F32Range::from_width_and_range(c.width, c.range, notrunc).def_warnings_into()
            })
            .def_map_value(Self::F32),
            AlphaNumType::Double => FixedLayout::try_new(columns, endian, |c| {
                F64Range::from_width_and_range(c.width, c.range, notrunc).def_warnings_into()
            })
            .def_map_value(Self::F64),
        }
    }

    fn h_read_df<R: Read>(
        &self,
        h: &mut BufReader<R>,
        tot: Tot,
        seg: AnyDataSegment,
        conf: &ReaderConfig,
    ) -> IODeferredResult<FCSDataFrame, ReadDataframeWarning, ReadDataframeError> {
        match self {
            Self::Ascii(x) => x
                .h_read_checked_df(h, tot, seg, conf)
                .def_map_errors(|e| e.inner_into()),
            Self::Integer(x) => x.h_read_df_numeric::<_, AnyReaderBitmask, _, _>(h, tot, seg, conf),
            Self::F32(x) => {
                x.h_read_df_numeric::<_, ColumnReader<_, _, _>, _, _>(h, tot, seg, conf)
            }
            Self::F64(x) => {
                x.h_read_df_numeric::<_, ColumnReader<_, _, _>, _, _>(h, tot, seg, conf)
            }
        }
    }

    fn check_writer(&self, df: &FCSDataFrame) -> MultiResult<(), ColumnError<AnyLossError>> {
        match self {
            Self::Ascii(x) => x.check_writer(df),
            Self::Integer(x) => x.check_writer::<AnyWriterBitmask>(df),
            Self::F32(x) => x.check_writer::<ColumnWriter<_, _, _>>(df),
            Self::F64(x) => x.check_writer::<ColumnWriter<_, _, _>>(df),
        }
    }

    fn h_write_df<W: Write>(&self, h: &mut BufWriter<W>, df: &FCSDataFrame) -> io::Result<()> {
        match self {
            Self::Ascii(x) => x.h_write_df(h, df),
            Self::Integer(x) => x.h_write_df::<_, AnyWriterBitmask>(h, df),
            Self::F32(x) => x.h_write_df::<_, ColumnWriter<_, _, _>>(h, df),
            Self::F64(x) => x.h_write_df::<_, ColumnWriter<_, _, _>>(h, df),
        }
    }

    // fn insert(
    //     &mut self,
    //     index: MeasIndex,
    //     range: Range,
    //     notrunc: bool,
    // ) -> DeferredResult<(), LayoutInsertColumnWarning, LayoutInsertColumnError> {
    //     match self {
    //         Self::Ascii(l) => l.insert(index, range, notrunc).def_inner_into(),
    //         Self::Integer(l) => l.insert_uint(index, range, notrunc).def_inner_into(),
    //         Self::F32(l) => l.insert_float(index, range, notrunc).def_inner_into(),
    //         Self::F64(l) => l.insert_float(index, range, notrunc).def_inner_into(),
    //     }
    // }

    fn insert_nocheck(
        &mut self,
        index: MeasIndex,
        range: FloatOrInt,
        notrunc: bool,
    ) -> BiTentative<(), LayoutInsertColumnWarning> {
        match self {
            Self::Ascii(l) => l.insert_nocheck(index, range, notrunc).inner_into(),
            Self::Integer(l) => l.insert_uint_nocheck(index, range, notrunc).inner_into(),
            Self::F32(l) => l.insert_float_nocheck(index, range, notrunc).inner_into(),
            Self::F64(l) => l.insert_float_nocheck(index, range, notrunc).inner_into(),
        }
    }

    fn push(&mut self, range: FloatOrInt, notrunc: bool) -> BiTentative<(), LayoutPushColumnError> {
        match self {
            Self::Ascii(l) => l.push(range, notrunc).inner_into(),
            Self::Integer(l) => l.push_uint(range, notrunc).inner_into(),
            Self::F32(l) => l.push_float(range, notrunc).inner_into(),
            Self::F64(l) => l.push_float(range, notrunc).inner_into(),
        }
    }

    fn remove(&mut self, index: MeasIndex) -> Result<(), ClearOptionalOr<IndexError>> {
        match self {
            Self::Ascii(l) => l.remove(index),
            Self::Integer(l) => l.remove_inner(index),
            Self::F32(l) => l.remove_inner(index),
            Self::F64(l) => l.remove_inner(index),
        }
    }

    fn remove_nocheck(&mut self, index: MeasIndex) -> Result<(), ClearOptional> {
        match self {
            Self::Ascii(l) => l.remove_nocheck(index),
            Self::Integer(l) => l.remove_nocheck_inner(index),
            Self::F32(l) => l.remove_nocheck_inner(index),
            Self::F64(l) => l.remove_nocheck_inner(index),
        }
    }

    fn ncols(&self) -> usize {
        match_many_to_one!(self, Self, [Ascii, Integer, F32, F64], x, { x.ncols() })
    }

    fn nbytes(&self, df: &FCSDataFrame) -> u64 {
        match_many_to_one!(self, Self, [Ascii, Integer, F32, F64], x, { x.nbytes(df) })
    }

    pub(crate) fn into_ordered<T>(self) -> LayoutConvertResult<AnyOrderedLayout<T>> {
        match self {
            Self::Ascii(x) => Ok(AnyOrderedLayout::Ascii(x.tot_into())),
            Self::Integer(x) => x.uint_try_into_ordered().map(AnyOrderedLayout::Integer),
            Self::F32(x) => Ok(AnyOrderedLayout::F32(x.tot_into().byte_layout_into())),
            Self::F64(x) => Ok(AnyOrderedLayout::F64(x.tot_into().byte_layout_into())),
        }
    }

    fn req_keywords(&self) -> [(String, String); 2] {
        match self {
            Self::Ascii(x) => x.req_keywords::<Endian>(),
            Self::Integer(x) => x.req_keywords(x.byte_layout),
            Self::F32(x) => x.req_keywords(x.byte_layout),
            Self::F64(x) => x.req_keywords(x.byte_layout),
        }
    }

    fn req_meas_keywords(&self) -> NonEmpty<(String, String, String)> {
        match self {
            Self::Ascii(x) => x.req_meas_keywords(),
            Self::Integer(x) => x.req_meas_keywords(),
            Self::F32(x) => x.req_meas_keywords(),
            Self::F64(x) => x.req_meas_keywords(),
        }
    }
}

enum_from_disp!(
    pub AsciiToUintError,
    [NotAscii, NotAsciiError],
    [Int, ParseIntError]
);

pub struct NotAsciiError(Vec<u8>);

enum_from_disp!(
    pub NewDataLayoutError,
    [Ascii,       ColumnError<ascii_range::NewAsciiRangeError>],
    [FixedInt,    NewFixedIntLayoutError],
    [Float,       ColumnError<FloatWidthError>],
    [VariableInt, ColumnError<NewUintTypeError>],
    [Mixed,       ColumnError<NewMixedTypeError>],
    [ByteOrd,     ByteOrdToSizedError]
);

enum_from_disp!(
    pub NewFixedIntLayoutError,
    [Width, SingleFixedWidthError],
    [Column, ColumnError<IntOrderedColumnError>]
);

enum_from_disp!(
    pub IntOrderedColumnError,
    [Order, ByteOrdToSizedError],
    // TODO sloppy nesting
    [Endian, ByteOrdToSizedEndianError],
    [Size,  BitmaskError]
);

enum_from_disp!(
    pub SingleFixedWidthError,
    [Bytes, WidthToBytesError],
    [Multi, MultiWidthsError]
);

pub struct MultiWidthsError(pub NonEmpty<Bytes>);

enum_from_disp!(
    pub NewMixedTypeError,
    [Ascii, ascii_range::NewAsciiRangeError],
    [Uint, NewUintTypeError],
    [Float, FloatWidthError]
);

enum_from_disp!(
    pub NewMixedTypeWarning,
    [Ascii, IntRangeError],
    [Uint, BitmaskError],
    [Float, FloatRangeError]
);

enum_from_disp!(
    pub NewUintTypeError,
    [Bitmask, BitmaskError],
    [Bytes, WidthToBytesError]
);

enum_from_disp!(
    pub NewOrderedUintLayoutError,
    [Column, ColumnError<OrderedFloatError>],
    [ByteOrd, ByteOrdToSizedError]
);

enum_from_disp!(
    pub OrderedFloatError,
    [Order,      ByteOrdToSizedError],
    [WrongWidth, FloatWidthError]
);

enum_from_disp!(
    pub FloatWidthError,
    [Bytes,      WidthToBytesError],
    [WrongWidth, WrongFloatWidth],
    [Range, FloatRangeError]
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
    [Layout, ColumnError<IntRangeError>],
    [Width, UnevenEventWidth],
    [Segment, ReqSegmentWithDefaultWarning<DataSegmentId>]
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

pub struct AsciiLossError(Chars);

impl fmt::Display for AsciiLossError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "ASCII data was too big and truncated into {} chars",
            self.0
        )
    }
}

pub struct BitmaskLossError(pub u64);

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
    index: IndexFromOne,
    error: E,
}

impl<E> ColumnError<E> {
    fn inner_into<X>(self) -> ColumnError<X>
    where
        X: From<E>,
    {
        ColumnError {
            index: self.index,
            error: self.error.into(),
        }
    }
}

type LookupLayoutResult<T> = DeferredResult<T, LookupLayoutWarning, LookupLayoutError>;

enum_from_disp!(
    pub LookupLayoutError,
    [New, NewDataLayoutError],
    [Raw, LookupKeysError]
);

enum_from_disp!(
    pub LookupLayoutWarning,
    [New, ColumnError<NewMixedTypeWarning>],
    [Raw, LookupKeysWarning]
);

type FromRawResult<T> = DeferredResult<T, RawToLayoutWarning, RawToLayoutError>;

enum_from_disp!(
    pub RawToLayoutError,
    [New, NewDataLayoutError],
    [Raw, RawParsedError]
);

enum_from_disp!(
    pub RawToLayoutWarning,
    [New, ColumnError<NewMixedTypeWarning>],
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
    pub ReadDataframeError,
    [Ascii, ReadAsciiError],
    [Uneven, UnevenEventWidth],
    [TotMismatch, TotEventMismatch],
    [Delim, ReadDelimWithRowsAsciiError],
    [DelimNoRows, ReadDelimAsciiWithoutRowsError],
    [AlphaNum, AsciiToUintError]
);

enum_from_disp!(
    pub ReadAsciiError,
    [Delim, ReadDelimAsciiError],
    [Fixed, ReadFixedAsciiError]
);

enum_from_disp!(
    pub ReadFixedAsciiError,
    [Uneven, UnevenEventWidth],
    [Tot, TotEventMismatch],
    [ToUint, AsciiToUintError]
);

enum_from_disp!(
    pub ReadDataframeWarning,
    [Uneven, UnevenEventWidth],
    [Tot, TotEventMismatch]
);

enum_from_disp!(
    pub ReadDelimAsciiError,
    [Rows, ReadDelimWithRowsAsciiError],
    [NoRows, ReadDelimAsciiWithoutRowsError]
);

enum_from_disp!(
    pub ReadDelimWithRowsAsciiError,
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

pub enum ReadDelimAsciiWithoutRowsError {
    Unequal,
    Parse(AsciiToUintError),
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

impl fmt::Display for ReadDelimAsciiWithoutRowsError {
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

pub struct ConvertWidthError {
    index: MeasIndex,
    error: UintToUintError,
}

impl fmt::Display for ConvertWidthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "integer conversion error in column {}: {}",
            self.index, self.error,
        )
    }
}

pub struct UintToUintError {
    from: u8,
    to: u8,
}

impl fmt::Display for UintToUintError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "could not convert integer from {} bytes to {} bytes",
            self.from, self.to,
        )
    }
}

pub struct MixedIsInteger {
    width: u8,
}

pub struct MixedIsAscii;

pub struct MixedIsFloat;

pub struct MixedIsDouble;

impl fmt::Display for MixedIsInteger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}-byte integer", self.width)
    }
}

impl fmt::Display for MixedIsAscii {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "fixed-width ASCII")
    }
}

impl fmt::Display for MixedIsFloat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "32-bit float")
    }
}

impl fmt::Display for MixedIsDouble {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "64-bit float")
    }
}

pub type MixedToOrderedLayoutError = MixedColumnConvertError<MixedToOrderedConvertError>;
pub type MixedToNonMixedLayoutError = MixedColumnConvertError<MixedToNonMixedConvertError>;

enum_from_disp!(
    pub MixedToOrderedConvertError,
    [Ascii, MixedToAsciiError],
    [Integer, MixedToOrderedUintError],
    [Float, MixedToFloatError],
    [Double, MixedToDoubleError]
);

enum_from_disp!(
    pub MixedToNonMixedConvertError,
    [Ascii, MixedToAsciiError],
    [Integer, MixedToEndianUintError],
    [Float, MixedToFloatError],
    [Double, MixedToDoubleError]
);

// enum_from_disp!(
//     pub LayoutInsertColumnError,
//     [Int, IntInsertColumnError],
//     [Float, FloatInsertColumnError]
// );

enum_from_disp!(
    pub LayoutPushColumnError,
    [Ascii, IntRangeError],
    [Int, BitmaskError],
    [Float, FloatRangeError]
);

enum_from_disp!(
    pub LayoutInsertColumnWarning,
    [Ascii, IntRangeError],
    [Int, BitmaskError],
    [Float, FloatRangeError]
);

// NOTE this also applies to ASCII since these use u64 (an "int")
enum_from_disp!(
    pub IntInsertColumnError,
    [Range, IntRangeError],
    [Index, BoundaryIndexError]
);

enum_from_disp!(
    pub FloatInsertColumnError,
    [Range, FloatRangeError],
    [Index, BoundaryIndexError]
);

pub struct MixedColumnConvertError<E> {
    index: MeasIndex,
    error: E,
}

impl<E: fmt::Display> fmt::Display for MixedColumnConvertError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "mixed conversion error in column {}: {}",
            self.index, self.error
        )
    }
}

#[derive(Debug)]
pub struct ColumnNumberError {
    this_len: usize,
    other_len: usize,
}

impl fmt::Display for ColumnNumberError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "number of columns is {}, input should match but got {}",
            self.this_len, self.other_len,
        )
    }
}

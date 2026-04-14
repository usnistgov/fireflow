//! Reading and writing the DATA segment
//!
//! # Basic overview
//!
//! DATA is arranged according to version-specific "layouts". Each layout will
//! enumerate all possible combinations for a given version, which directly
//! correspond to all valid combinations of $BYTEORD, $DATATYPE, $PnB, $PnR, and
//! $PnDATATYPE in the case of 3.2.
//!
//! Each layout may then be projected into a "reader" or "writer." Readers are
//! blank vectors waiting to accept data from disk. Writers are iterators that
//! read values from a dataframe and possibly convert them before writing.
//!
//! # Not-so-basic overview
//!
//! Layouts can first be classified by column width, where *fixed* layouts have
//! a single width per column and *delimited* layouts have a variable width. The
//! latter only corresponds to one layout: the case where $DATATYPE=A and all
//! $PnB=*. Values in such layouts will always be read as [`u64`].
//!
//! *Fixed* layouts can further be classified by the type in each column:
//!
//! 1) Single-type numeric layouts (aka "matrices")
//! 2) Fixed ASCII layouts
//! 3) Variable-width integer layouts
//! 4) Mixed layouts
//!
//! (1) is the simplest; each column is the same type which corresponds directly
//! with a native Rust type. This includes [`f32`], [`f64`], and uint ranging
//! from 1 to 8 bytes (including those that aren't powers of 2). Each type has a
//! slightly different reader/writer corresponding to distinct byte
//! interpretations on disk. (2) is similar in that the entire layout is one
//! type; however, each number is always read as [`u64`] subject to the chars
//! allowed by $PnB. (1)/(2) are the only possibilities for FCS 2.0/3.0 since
//! $BYTEORD restricts all $PnB to the same width in the case of numeric
//! $DATATYPE.
//!
//! (3) is a weird layout that only a few (but more than zero) vendors are known
//! to use. Since $BYTEORD was changed to only mean endian-ness, its relation to
//! $PnB was severed. When DATATYPE=I, this means $PnB may be changed freely,
//! which allows different integer widths in each column. In practice this makes
//! the resulting data structure a dataframe (vs a matrix).
//!
//! (4) was newly added to 3.2 by way of the $PnDATATYPE keywords which now
//! allows the data layout to include any type. This obviously more complex but
//! is not computationally very different from (3).
//!
//! In addition to width, layouts may also be classified by whether $TOT is
//! known. In 2.0, $TOT is optional and may not be given. For *delimited* ASCII
//! layouts, not have $TOT means we need to parse until we reach the end of
//! DATA, hoping that all columns have the same length. For *fixed* layouts, we
//! can compute $TOT using $PnB and the length of DATA.

use crate::config::{
    AllowTotMismatch, ConfigFlag as _, DisallowRangeTrunc, ReadDataKeywordsConfig, ReadEventsConfig,
};
use crate::core::{
    AsScaleOrTransform, Measurements, NamedTemporalsAndOpticals, ScaleTransform, VersionedMetaroot,
};
use crate::logging::{
    CommutativeResultIter as _, DeferredError, DeferredIter as _, DeferredSwitchableError,
    DeferredWarningAndError, ErrorGroup, ErrorsResult, GroupResult, IOErrorGroup, IOResult,
    ImpureError, LogResult, ResultExt as _, SwitchableErrorResult, SwitchableErrorsResult,
    WarningOrErrorResult, WarningsAndErrorResult, WarningsAndErrorsResult,
    WarningsAndIOGroupResult, WarningsAndIOResult,
};
use crate::macros::{def_summary, match_many_to_one};
use crate::segment::AnyDataSegment;
use crate::text::byteord::{
    ArrayByteOrd, BitsOrChars, ByteOrdToSizedError, Bytes, Endian, HasByteOrd, NoByteOrd,
    NoByteOrd3_1, OrderedToEndianError, PrivBytes, WidthToBytesError, WidthToFixedError,
};
use crate::text::float_decimal::{DecimalToFloatError, FloatDecimal, HasFloatBounds};
use crate::text::index::{IndexFromOne, MeasIndex};
use crate::text::keywords::{
    AlphaNumType, ByteOrd2_0, ByteOrd3_1, Gain, Keyword0FromValue as _, Keyword1FromValue as _,
    NumType, Par, Range, RangeToIntError, RangeToIntErrorKind, ReqMeasKeyword, ReqRootKeyword,
    Scale, SplitKeyword0, SplitKeyword1, Tot, Width,
};
use crate::text::lookup::{
    OptIndexedKey as _, OptIndexedKeyError, ReqIndexedKey as _, ReqIndexedKeyError, ReqKeyError,
    ReqMetarootKey as _,
};
use crate::text::named_vec::{NamedVec, NewNamedVecError};
use crate::text::optional::{Identity, MightHave, Nothing};
use crate::validated::ascii_range::{
    AsciiRangeFromKeywordsError, AsciiRangeValue, Chars, DelimAsciiRange, FixedAsciiRange,
};
use crate::validated::bitmask::{
    Bitmask, Bitmask08, Bitmask16, Bitmask24, Bitmask32, Bitmask40, Bitmask48, Bitmask56,
    Bitmask64, BitmaskTruncationError, BitmaskValue,
};
use crate::validated::dataframe::{
    AnyPrimitiveColumn, FFDataFrame, HasLen, HasWidth, InternalColumn, PrimitiveColumn,
    PrimitiveDataFrame, ambassador_impl_HasLen,
};
use crate::validated::keys::{IndexedKey as _, NonStdKeywords, StdKeywords};
use crate::validated::unaligned::{FCSRepr, U24, U40, U48, U56};

use fireflow_core_proc::impl_generic_enum_from;
use fireflow_types::config::{RowBufferSize, TruncateEventValues};
use fireflow_types::nonempty_string::DisplayableNE as _;
use type_families::{Functor, FunctorOnce, impl_functor, impl_functor_once, impl_kind1};

use ambassador::{Delegate, delegatable_trait};
use bigdecimal::BigDecimal;
use bytemuck::allocation::cast_vec;
use derive_more::{AsRef, Display, From, Into};
use derive_new::new;
use itertools::Itertools as _;
use nonempty_collections::{
    IntoIteratorExt as _, NEVec,
    iter::{NonEmptyIterator as _, once},
};
use num_traits::{Bounded, FromBytes, ToBytes};
use thiserror::Error;

use std::convert::Infallible;
use std::fmt;
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::iter;
use std::marker::PhantomData;
use std::mem;
use std::num::{NonZeroU8, ParseIntError};
use std::ops::Shr;
use std::str;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr},
    fireflow_types::python as py,
};

/// All possible byte layouts for the DATA segment in 2.0.
///
/// This is identical to 3.0 in every way except that the $TOT keyword in 2.0
/// is optional, which requires a different interface.
#[derive(Clone, From, Delegate, PartialEq)]
#[delegate(LayoutDims)]
#[delegate(LayoutRanges)]
#[delegate(LayoutDatatype)]
#[delegate(LayoutKeywords)]
#[delegate(Insertable<Range>)]
#[delegate(Removable<Range>)]
#[delegate(OptMeasLayoutKeywords)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct DataLayout2_0(pub AnyOrderedLayout<Option<Tot>>);

/// All possible DATA storage configurations for 2.0.
///
/// This is identical to 3.0 in every way except that the $TOT keyword in 2.0
/// is optional, which requires a different interface.
#[derive(Clone, From, Into, PartialEq)]
#[into(PrimitiveDataFrame)]
pub struct DataFrame2_0(pub AnyOrderedDataFrame<Option<Tot>>);

/// All possible byte layouts for the DATA segment in 3.0.
#[derive(Clone, From, Delegate, PartialEq)]
#[delegate(LayoutDims)]
#[delegate(LayoutRanges)]
#[delegate(LayoutDatatype)]
#[delegate(LayoutKeywords)]
#[delegate(Insertable<Range>)]
#[delegate(Removable<Range>)]
#[delegate(OptMeasLayoutKeywords)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct DataLayout3_0(pub AnyOrderedLayout<Identity<Tot>>);

/// All possible DATA storage configurations in 3.0.
#[derive(Clone, From, Into, PartialEq)]
#[into(PrimitiveDataFrame)]
pub struct DataFrame3_0(pub AnyOrderedDataFrame<Identity<Tot>>);

/// All possible byte layouts for the DATA segment in 3.1.
///
/// Unlike 2.0 and 3.0, the integer layout allows the column widths to be
/// different. This is a consequence of making BYTEORD only mean "big or little
/// endian" and have nothing to do with number of bytes.
#[derive(Clone, From, Delegate, PartialEq)]
#[delegate(LayoutDims)]
#[delegate(LayoutRanges)]
#[delegate(LayoutDatatype)]
#[delegate(LayoutKeywords)]
#[delegate(Insertable<Range>)]
#[delegate(Removable<Range>)]
#[delegate(OptMeasLayoutKeywords)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct DataLayout3_1(pub NonMixedEndianLayout<Nothing<NumType>>);

/// All possible storage configurations in 3.1.
///
/// Unlike 2.0 and 3.0, the integer layout allows the column widths to be
/// different. This is a consequence of making BYTEORD only mean "big or little
/// endian" and have nothing to do with number of bytes.
#[derive(Clone, From, PartialEq, Into)]
#[into(PrimitiveDataFrame)]
pub struct DataFrame3_1(pub NonMixedEndianDataFrame<Nothing<NumType>>);

/// All possible byte layouts for the DATA segment in 3.2.
///
/// In addition to the loosened integer layouts in 3.1, 3.2 additionally allows
/// each column to have a different type and size (hence "Mixed").
pub type DataLayout3_2 = Any3_2<MixedLayout, NonMixedEndianLayout<Option<NumType>>>;

impl_generic_enum_from! {
    DataLayout3_2,
    Mixed <- MixedLayout,
    NonMixed <- NonMixedEndianLayout<Option<NumType>>
}

/// All possible storage configurations in 3.2.
///
/// In addition to the loosened integer layouts in 3.1, 3.2 additionally allows
/// each column to have a different type and size (hence "Mixed").
pub type DataFrame3_2 = Any3_2<MixedDataFrame, NonMixedEndianDataFrame<Option<NumType>>>;

impl_generic_enum_from! {
    DataFrame3_2,
    Mixed <- MixedDataFrame,
    NonMixed <- NonMixedEndianDataFrame<Option<NumType>>
}

/// Generic container for 3.2 DATA configurations.
// TODO false positive lint
#[allow(clippy::duplicated_attributes)]
#[derive(Clone, Delegate, PartialEq)]
#[delegate(LayoutDims)]
#[delegate(LayoutRanges)]
#[delegate(LayoutDatatype, where = "M: LayoutDims, N: LayoutDims")]
#[delegate(LayoutKeywords, where = "M: LayoutDims, N: LayoutDims")]
#[delegate(Removable<Range>)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum Any3_2<M, N> {
    Mixed(M),
    NonMixed(N),
}

/// A DATA layout where every column may have a different type (3.2 only).
pub type MixedLayout = EndianLayout<MixedRange, Option<NumType>>;

/// A DATA segment where every column may be a different type (3.2 only).
pub type MixedDataFrame = DataFrame<MixedColumn, Endian, Identity<Tot>, Option<NumType>>;

/// All possible layouts for the DATA segment in 2.0 and 3.0.
///
/// It is so named "Ordered" because the $BYTEORD keyword represents any
/// possible byte ordering that may occur rather than simply little or big
/// endian.
pub type AnyOrderedLayout<T> = AnyDatatype<
    AnyAsciiLayout<T, Nothing<NumType>, true>,
    AnyOrderedUintLayout<T>,
    OrderedLayout<F32Range, T>,
    OrderedLayout<F64Range, T>,
>;

impl_generic_enum_from! {
    AnyOrderedLayout<T>,
    Ascii <- AnyAsciiLayout<T, Nothing<NumType>, true>,
    Uint <- AnyOrderedUintLayout<T>,
    F32 <- OrderedLayout<F32Range, T>,
    F64 <- OrderedLayout<F64Range, T>
}

/// All possible DATA storage configuration for 2.0 and 3.0.
///
/// It is so named "Ordered" because the $BYTEORD keyword represents any
/// possible byte ordering that may occur rather than simply little or big
/// endian.
pub type AnyOrderedDataFrame<T> = AnyDatatype<
    AnyAsciiDataFrame<T, Nothing<NumType>, true>,
    AnyOrderedUintDataFrame<T>,
    OrderedDataFrame<F32Range, T>,
    OrderedDataFrame<F64Range, T>,
>;

impl_generic_enum_from! {
    AnyOrderedDataFrame<T>,
    Ascii <- AnyAsciiDataFrame<T, Nothing<NumType>, true>,
    Uint <- AnyOrderedUintDataFrame<T>,
    F32 <- OrderedDataFrame<F32Range, T>,
    F64 <- OrderedDataFrame<F64Range, T>
}

/// All possible endian layouts with the same datatype (3.1)
pub type NonMixedEndianLayout<D> = AnyDatatype<
    AnyAsciiLayout<Identity<Tot>, D, false>,
    AnyEndianUintLayout<D>,
    EndianLayout<F32Range, D>,
    EndianLayout<F64Range, D>,
>;

impl_generic_enum_from! {
    NonMixedEndianLayout<D>,
    Ascii <- AnyAsciiLayout<Identity<Tot>, D, false>,
    Uint <- AnyEndianUintLayout<D>,
    F32 <- EndianLayout<F32Range, D>,
    F64 <- EndianLayout<F64Range, D>
}

pub type NonMixedEndianDataFrame<D> = AnyDatatype<
    AnyAsciiDataFrame<Identity<Tot>, D, false>,
    AnyEndianUintDataFrame<D>,
    EndianDataFrame<F32Range, D>,
    EndianDataFrame<F64Range, D>,
>;

impl_generic_enum_from! {
    NonMixedEndianDataFrame<D>,
    Ascii <- AnyAsciiDataFrame<Identity<Tot>, D, false>,
    Uint <- AnyEndianUintDataFrame<D>,
    F32 <- EndianDataFrame<F32Range, D>,
    F64 <- EndianDataFrame<F64Range, D>
}

pub type EndianLayout<C, D> = DataLayout<C, Endian, Identity<Tot>, D>;

pub type EndianDataFrame<C, D> = DataFrame<NativeColumn<C>, Endian, Identity<Tot>, D>;

pub type EndianUintLayout<D> = EndianLayout<AnyNullBitmask, D>;

pub type EndianUintDataFrame<D> = DataFrame<AnyBitmaskColumn, Endian, Identity<Tot>, D>;

/// DATA layouts for ASCII data.
///
/// This may either be fixed (ie columns have the same number of characters)
/// or variable (ie columns have have different number of characters and are
/// separated by delimiters).
pub type AnyAsciiLayout<T, D, const ORD: bool> =
    AnyAscii<DelimAsciiLayout<T, D, ORD>, FixedAsciiLayout<T, D, ORD>>;

pub type AnyAsciiDataFrame<T, D, const ORD: bool> =
    AnyAscii<DelimAsciiDataFrame<T, D, ORD>, FixedAsciiDataFrame<T, D, ORD>>;

// TODO false positive lint
#[allow(clippy::duplicated_attributes)]
#[derive(Clone, Delegate, PartialEq)]
#[delegate(LayoutDims)]
#[delegate(LayoutRanges)]
#[delegate(Insertable<Range>)]
#[delegate(LayoutDatatype, where = "D: LayoutDims, F: LayoutDims")]
#[delegate(LayoutKeywords, where = "D: LayoutDims, F: LayoutDims")]
#[delegate(Removable<Range>)]
#[delegate(OptMeasLayoutKeywords)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum AnyAscii<D, F> {
    Delimited(D),
    Fixed(F),
}

pub type FixedAsciiLayout<T, D, const ORD: bool> =
    DataLayout<FixedAsciiRange, NoByteOrd<ORD>, T, D>;

pub type FixedAsciiDataFrame<T, D, const ORD: bool> =
    DataFrame<NativeColumn<FixedAsciiRange>, NoByteOrd<ORD>, T, D>;

pub type DelimAsciiLayout<T, D, const ORD: bool> =
    DataLayout<DelimAsciiRange, NoByteOrd<ORD>, T, D>;

pub type DelimAsciiDataFrame<T, D, const ORD: bool> =
    ColumnGroup<FFDataFrame<AnnotatedColumn<DelimAsciiRange, u64, u64>>, NoByteOrd<ORD>, T, D>;

impl<C, L, T, D> AsRef<[C]> for DataLayout<C, L, T, D> {
    fn as_ref(&self) -> &[C] {
        self.inner.as_ref()
    }
}

pub type DataLayout<C, L, T, D> = ColumnGroup<Vec<C>, L, T, D>;

pub type DataFrame<C, L, T, D> = ColumnGroup<FFDataFrame<C>, L, T, D>;

pub type NativeColumn<C> = AnnotatedColumn<
    C,
    <<C as HasNativeType>::Native as FCSRepr>::Prim,
    <C as HasNativeType>::Native,
>;

/// DATA layout where each column has a fixed width.
#[derive(Clone, AsRef, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[new(visibility(""))]
pub struct ColumnGroup<Cols, Layout, TotType, Dtype> {
    inner: Cols,
    #[as_ref(Layout)]
    byte_layout: Layout,
    #[cfg_attr(feature = "serde", serde(skip))]
    _tot_def: PhantomData<TotType>,
    #[cfg_attr(feature = "serde", serde(skip))]
    _meas_data_def: PhantomData<Dtype>,
}

impl<C: Default, L: Default, T, D> Default for ColumnGroup<C, L, T, D> {
    fn default() -> Self {
        Self::new(C::default(), L::default())
    }
}

impl<C, L, T, D> DataLayout<C, L, T, D> {
    fn map_vec<Cf, F>(self, f: F) -> DataLayout<Cf, L, T, D>
    where
        F: Fn(C) -> Cf,
    {
        DataLayout::new(self.inner.fmap(f), self.byte_layout)
    }
}

impl<C, L, T, D> DataFrame<C, L, T, D> {
    fn map_cols<Cf, F>(self, f: F) -> DataFrame<Cf, L, T, D>
    where
        F: Fn(C) -> Cf,
    {
        DataFrame::new(self.inner.fmap(f), self.byte_layout)
    }
}

/// DATA layout for integers that may be in any byte order.
pub type AnyOrderedUintLayout<T> = AnyUint<
    OrderedLayout<Bitmask08, T>,
    OrderedLayout<Bitmask16, T>,
    OrderedLayout<Bitmask24, T>,
    OrderedLayout<Bitmask32, T>,
    OrderedLayout<Bitmask40, T>,
    OrderedLayout<Bitmask48, T>,
    OrderedLayout<Bitmask56, T>,
    OrderedLayout<Bitmask64, T>,
>;

pub type AnyOrderedUintDataFrame<T> = AnyUint<
    OrderedDataFrame<Bitmask08, T>,
    OrderedDataFrame<Bitmask16, T>,
    OrderedDataFrame<Bitmask24, T>,
    OrderedDataFrame<Bitmask32, T>,
    OrderedDataFrame<Bitmask40, T>,
    OrderedDataFrame<Bitmask48, T>,
    OrderedDataFrame<Bitmask56, T>,
    OrderedDataFrame<Bitmask64, T>,
>;

impl_generic_enum_from! {
    AnyOrderedUintLayout<T>,
    Uint08 <- OrderedLayout<Bitmask08, T>,
    Uint16 <- OrderedLayout<Bitmask16, T>,
    Uint24 <- OrderedLayout<Bitmask24, T>,
    Uint32 <- OrderedLayout<Bitmask32, T>,
    Uint40 <- OrderedLayout<Bitmask40, T>,
    Uint48 <- OrderedLayout<Bitmask48, T>,
    Uint56 <- OrderedLayout<Bitmask56, T>,
    Uint64 <- OrderedLayout<Bitmask64, T>
}

impl_generic_enum_from! {
    AnyOrderedUintDataFrame<T>,
    Uint08 <- OrderedDataFrame<Bitmask08, T>,
    Uint16 <- OrderedDataFrame<Bitmask16, T>,
    Uint24 <- OrderedDataFrame<Bitmask24, T>,
    Uint32 <- OrderedDataFrame<Bitmask32, T>,
    Uint40 <- OrderedDataFrame<Bitmask40, T>,
    Uint48 <- OrderedDataFrame<Bitmask48, T>,
    Uint56 <- OrderedDataFrame<Bitmask56, T>,
    Uint64 <- OrderedDataFrame<Bitmask64, T>
}

/// DATA layout for single-width integers that may only be big or little endian.
pub type AnySingleUintLayout<D> = AnyUint<
    EndianLayout<Bitmask08, D>,
    EndianLayout<Bitmask16, D>,
    EndianLayout<Bitmask24, D>,
    EndianLayout<Bitmask32, D>,
    EndianLayout<Bitmask40, D>,
    EndianLayout<Bitmask48, D>,
    EndianLayout<Bitmask56, D>,
    EndianLayout<Bitmask64, D>,
>;

pub type AnySingleUintDataFrame<D> = AnyUint<
    EndianDataFrame<Bitmask08, D>,
    EndianDataFrame<Bitmask16, D>,
    EndianDataFrame<Bitmask24, D>,
    EndianDataFrame<Bitmask32, D>,
    EndianDataFrame<Bitmask40, D>,
    EndianDataFrame<Bitmask48, D>,
    EndianDataFrame<Bitmask56, D>,
    EndianDataFrame<Bitmask64, D>,
>;

impl_generic_enum_from! {
    AnySingleUintLayout<D>,
    Uint08 <- EndianLayout<Bitmask08, D>,
    Uint16 <- EndianLayout<Bitmask16, D>,
    Uint24 <- EndianLayout<Bitmask24, D>,
    Uint32 <- EndianLayout<Bitmask32, D>,
    Uint40 <- EndianLayout<Bitmask40, D>,
    Uint48 <- EndianLayout<Bitmask48, D>,
    Uint56 <- EndianLayout<Bitmask56, D>,
    Uint64 <- EndianLayout<Bitmask64, D>
}

impl_generic_enum_from! {
    AnySingleUintDataFrame<D>,
    Uint08 <- EndianDataFrame<Bitmask08, D>,
    Uint16 <- EndianDataFrame<Bitmask16, D>,
    Uint24 <- EndianDataFrame<Bitmask24, D>,
    Uint32 <- EndianDataFrame<Bitmask32, D>,
    Uint40 <- EndianDataFrame<Bitmask40, D>,
    Uint48 <- EndianDataFrame<Bitmask48, D>,
    Uint56 <- EndianDataFrame<Bitmask56, D>,
    Uint64 <- EndianDataFrame<Bitmask64, D>
}

pub type AnyEndianUintLayout<D> = AnyEndianUint<AnySingleUintLayout<D>, EndianUintLayout<D>>;

impl_generic_enum_from! {
    AnyEndianUintLayout<D>,
    Single <- AnySingleUintLayout<D>,
    Multi <- EndianUintLayout<D>
}

pub type AnyEndianUintDataFrame<D> =
    AnyEndianUint<AnySingleUintDataFrame<D>, EndianUintDataFrame<D>>;

impl_generic_enum_from! {
    AnyEndianUintDataFrame<D>,
    Single <- AnySingleUintDataFrame<D>,
    Multi <- EndianUintDataFrame<D>
}

// TODO false positive lint
#[allow(clippy::duplicated_attributes)]
#[derive(Clone, Delegate, PartialEq)]
#[delegate(LayoutDims)]
#[delegate(LayoutRanges)]
#[delegate(LayoutDatatype, where = "W0: LayoutDims, W: LayoutDims")]
#[delegate(LayoutKeywords, where = "W0: LayoutDims, W: LayoutDims")]
#[delegate(Removable<Range>)]
#[delegate(OptMeasLayoutKeywords)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum AnyEndianUint<W0, W> {
    Single(W0),
    Multi(W),
}

macro_rules! match_any_uint {
    ($value:expr, $root:ident, $inner:ident, $action:block) => {
        match_many_to_one!(
            $value,
            $root,
            [
                Uint08, Uint16, Uint24, Uint32, Uint40, Uint48, Uint56, Uint64
            ],
            $inner,
            $action
        )
    };
}

pub type OrderedLayout<C, T> = DataLayout<
    C,
    ArrayByteOrd<<<C as HasNativeType>::Native as FCSRepr>::ByteOrd>,
    T,
    Nothing<NumType>,
>;

pub type OrderedDataFrame<C, T> = DataFrame<
    NativeColumn<C>,
    ArrayByteOrd<<<C as HasNativeType>::Native as FCSRepr>::ByteOrd>,
    T,
    Nothing<NumType>,
>;

#[derive(Clone, PartialEq, Into, new)]
#[new(visibility = "")]
pub struct AnnotatedColumn<M, T, R> {
    metadata: M,
    #[into(PrimitiveColumn<T>)]
    data: InternalColumn<T, R>,
}

impl<M, T, R> AnnotatedColumn<M, T, R> {
    fn empty(metadata: M) -> Self {
        Self::new(metadata, InternalColumn::default())
    }
}

impl<M, T, R> HasLen for AnnotatedColumn<M, T, R> {
    fn len(&self) -> usize {
        self.data.len()
    }
}

impl<T> From<RangedVec<T, T::Native>> for NativeColumn<T>
where
    T: HasNativeType,
    T::Native: FCSRepr,
    Vec<T::Native>:
        Into<InternalColumn<<<T as HasNativeType>::Native as FCSRepr>::Prim, T::Native>>,
{
    fn from(value: RangedVec<T, T::Native>) -> Self {
        Self::new(value.range, value.data.into())
    }
}

// impl<M, T, R> AnnotatedColumn<M, T, R> {
//     fn from_vec(m: M, xs: Vec<R>) -> Self
//     where
//         Vec<R>: Into<InternalColumn<T, R>>,
//     {
//         Self::new(m, xs.into())
//     }
// }

// trait ColumnToData: HasNativeType {
//     type Target;

//     fn add_data(self, xs: Vec<Self::Native>) -> Self::Target;
// }

// impl<T> ColumnToData for Bitmask<T>
// where
//     Self: HasNativeType<Native = T>,
//     T: FCSRepr,
//     Vec<T>: Into<InternalColumn<T::Prim, T>>,
// {
//     type Target = NativeColumn<Bitmask<T>>;

//     fn add_data(self, xs: Vec<Self::Native>) -> Self::Target {
//         AnnotatedColumn::from_vec(self, xs)
//     }
// }

// impl<T> ColumnToData for FloatRange<T>
// where
//     Self: HasNativeType<Native = T>,
//     T: FCSRepr,
//     Vec<T>: Into<InternalColumn<T::Prim, T>>,
// {
//     type Target = NativeColumn<FloatRange<T>>;

//     fn add_data(self, xs: Vec<Self::Native>) -> Self::Target {
//         AnnotatedColumn::from_vec(self, xs)
//     }
// }

/// Generic container for anything that can be categorized by $DATATYPE.
// TODO false positive lint
#[allow(clippy::duplicated_attributes)]
#[derive(Debug, PartialEq, Clone, Delegate)]
#[delegate(HasLen)]
#[delegate(LayoutDims)]
#[delegate(LayoutRanges)]
#[delegate(
    LayoutDatatype,
    where = "A: LayoutDims, \
             U: LayoutDims, \
             F: LayoutDims, \
             D: LayoutDims"
)]
#[delegate(
    LayoutKeywords,
    where = "A: LayoutDims, \
             U: LayoutDims, \
             F: LayoutDims, \
             D: LayoutDims"
)]
#[delegate(Removable<Range>)]
#[delegate(OptMeasLayoutKeywords)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum AnyDatatype<A, U, F, D> {
    Ascii(A),
    Uint(U),
    F32(F),
    F64(D),
}

pub type MixedRange = AnyDatatype<FixedAsciiRange, AnyNullBitmask, F32Range, F64Range>;

pub type MixedColumn = AnyDatatype<
    NativeColumn<FixedAsciiRange>,
    AnyBitmaskColumn,
    NativeColumn<F32Range>,
    NativeColumn<F64Range>,
>;

/// A big or little-endian integer column of some size (1-8 bytes)
// TODO false positive lint
#[allow(clippy::duplicated_attributes)]
#[derive(Debug, PartialEq, Clone, Copy, Delegate)]
#[delegate(HasLen)]
#[delegate(LayoutDims)]
#[delegate(LayoutRanges)]
#[delegate(Insertable<Range>)]
#[delegate(
    LayoutDatatype,
    where = "C08: LayoutDims, \
             C16: LayoutDims, \
             C24: LayoutDims, \
             C32: LayoutDims, \
             C40: LayoutDims, \
             C48: LayoutDims, \
             C56: LayoutDims, \
             C64: LayoutDims"
)]
#[delegate(
    LayoutKeywords,
    where = "C08: LayoutDims, \
             C16: LayoutDims, \
             C24: LayoutDims, \
             C32: LayoutDims, \
             C40: LayoutDims, \
             C48: LayoutDims, \
             C56: LayoutDims, \
             C64: LayoutDims"
)]
#[delegate(Removable<Range>)]
#[delegate(OptMeasLayoutKeywords)]
#[delegate(OrderedLayoutOps)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum AnyUint<C08, C16, C24, C32, C40, C48, C56, C64> {
    Uint08(C08),
    Uint16(C16),
    Uint24(C24),
    Uint32(C32),
    Uint40(C40),
    Uint48(C48),
    Uint56(C56),
    Uint64(C64),
}

#[derive(new)]
struct RangedVec<B, T> {
    range: B,
    data: Vec<T>,
}

type UintRangedVec<C> = RangedVec<C, <C as HasNativeType>::Native>;

pub type AnyNullBitmask =
    AnyUint<Bitmask08, Bitmask16, Bitmask24, Bitmask32, Bitmask40, Bitmask48, Bitmask56, Bitmask64>;

pub type AnyBitmaskColumn = AnyUint<
    NativeColumn<Bitmask08>,
    NativeColumn<Bitmask16>,
    NativeColumn<Bitmask24>,
    NativeColumn<Bitmask32>,
    NativeColumn<Bitmask40>,
    NativeColumn<Bitmask48>,
    NativeColumn<Bitmask56>,
    NativeColumn<Bitmask64>,
>;

type AnyUintVec = AnyUint<
    UintRangedVec<Bitmask08>,
    UintRangedVec<Bitmask16>,
    UintRangedVec<Bitmask24>,
    UintRangedVec<Bitmask32>,
    UintRangedVec<Bitmask40>,
    UintRangedVec<Bitmask48>,
    UintRangedVec<Bitmask56>,
    UintRangedVec<Bitmask64>,
>;

/// The type of any floating point column in all versions
#[derive(PartialEq, Clone, new, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct FloatRange<T> {
    range: FloatDecimal<T>,
}

pub type F32Range = FloatRange<f32>;
pub type F64Range = FloatRange<f64>;

/// A struct whose fields map 1-1 with keyword values in one data column
#[derive(new)]
pub struct ColumnLayoutValues<D> {
    width: Width,
    range: Range,
    datatype: D,
}

type ColumnLayoutValues2_0 = ColumnLayoutValues<Nothing<NumType>>;
type ColumnLayoutValues3_2 = ColumnLayoutValues<Option<NumType>>;

/// Diagnostic output when making new data layout from keywords
#[derive(new)]
pub struct NewLayout<T> {
    /// The layout itself
    pub layout: T,

    /// Original values of $PnR that were truncated.
    ///
    /// Length of vector will be equal to $PAR. If $PnR for a given column was
    /// truncated, it will be returned in its corresponding index. Otherwise the
    /// index will be [`Option::None`].
    pub truncated_columns: Vec<Option<Range>>,
}

impl_kind1!(pub NewLayoutFamily, NewLayout);

impl_functor_once!(
    NewLayout,
    self,
    mut f,
    NewLayout::new(f(self.layout), self.truncated_columns)
);

/// Diagnostic output from reading DATA segment
#[derive(Clone, PartialEq, Default, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct EventsDiagnostics {
    /// The width of one event in bytes (if not ASCII delimited).
    pub event_width: Option<u64>,

    /// The remainder after dividing length of DATA by event width.
    ///
    /// For well-formed files, this should be zero.
    ///
    /// Will be [`Option::None`] for delimited ASCII layouts.
    pub event_data_remainder: Option<u64>,

    /// `true` if $TOT does not match the number of events computed via event width.
    ///
    /// [`Option::None`] if $TOT is missing (FCS 2.0) or the layout is ASCII
    /// delimited and there is no event width.
    pub tot_event_mismatch: Option<bool>,

    /// Columns for which at least one event was over $PnR.
    ///
    /// Length of vector will be equal to $PAR. Elements correspond to column
    /// indices and will be `None` if not overrange. Otherwise, the first
    /// [`usize`] will be the row that has the first overrange value, and the
    /// second [`bool`] will be `true` if the value was truncated to fit and
    /// false otherwise.
    pub overrange_columns: Vec<OverrangeColumn>,
}

type OverrangeColumn = Option<(usize, bool)>;

#[derive(new)]
struct ComputedRowsResult {
    total_events: u64,
    event_width: u64,
    remainder: u64,
}

/// Output of converting $PnR to native rust type.
#[derive(new)]
pub struct ConvertedRange<T> {
    /// The native value
    pub(crate) native: T,

    /// Original range if it needed to be truncated to make the native value.
    pub(crate) non_truncated: Option<Range>,
}

impl_kind1!(pub ConvertedRangeFamily, ConvertedRange);

impl_functor_once!(
    ConvertedRange,
    self,
    mut f,
    ConvertedRange::new(f(self.native), self.non_truncated)
);

/// Result of possibly truncated value
enum TruncatedResult {
    None,
    Truncated(usize),
    Overrange(MeasIndex, usize, Range),
}

impl TruncatedResult {
    fn into_err(self) -> Option<EventOverRangeError> {
        if let Self::Overrange(i, j, r) = self {
            Some(EventOverRangeError::new(j, i, r))
        } else {
            None
        }
    }

    fn as_col(&self) -> OverrangeColumn {
        match self {
            Self::None => None,
            Self::Truncated(i) => Some((*i, true)),
            Self::Overrange(_, i, _) => Some((*i, false)),
        }
    }
}

/// A cache-friendly buffer for reading and writing DATA.
///
/// Since FCS data is row-major and we want to output it in column-major, we
/// effectively need to transpose the data on-the-fly as it is being read. We
/// can't just think about this like a matrix transposition because we have
/// different data types, and we need to read into separate vectors anyways
/// since this is what polars expects to see when is makes a series.
///
/// Therefore, the idea is the read several rows at a time into an intermediate
/// row buffer from which raw bytes will be copied, possibly rearranged (in the
/// case of mixed byteord), padded (in the case of non-power-of-two integers),
/// cast as their target datatype, and finally stored in their final column
/// vectors. Once we have this row buffer, each column will be filled serially
/// which means the source buffer will be strided and the destination buffer
/// will be indexed contiguously. The row buffer will be able to store a whole
/// number of rows from the DATA segment.
///
/// Since we are only dealing with one segment of one column at the same time,
/// this means that we can adjust this size of this buffer and the one column
/// segment by extension (it will have the same length as the number of rows in
/// the buffer) to fit in the CPU's cache (ideally L1d). In practice, final
/// speed will be determined by the balance between syscall overhead for reads
/// and writes vs cache misses.
struct RowBuffer {
    nrows: usize,
    row_width: usize,
    rows_per_buffer: usize,
    buf_size: u64,
    bytes: Vec<u8>,
}

macro_rules! match_map_uint {
    ($value:expr, $inner:ident, $action:expr) => {
        match $value {
            AnyUint::Uint08($inner) => AnyUint::Uint08($action),
            AnyUint::Uint16($inner) => AnyUint::Uint16($action),
            AnyUint::Uint24($inner) => AnyUint::Uint24($action),
            AnyUint::Uint32($inner) => AnyUint::Uint32($action),
            AnyUint::Uint40($inner) => AnyUint::Uint40($action),
            AnyUint::Uint48($inner) => AnyUint::Uint48($action),
            AnyUint::Uint56($inner) => AnyUint::Uint56($action),
            AnyUint::Uint64($inner) => AnyUint::Uint64($action),
        }
    };
}

impl IntoEmptyDataFrame for DataLayout2_0 {
    type DfTarget = DataFrame2_0;

    fn empty(&self) -> Self::DfTarget {
        self.0.empty().into()
    }
}

impl NormalizableLayout for DataLayout2_0 {
    fn normalize(&mut self) {}
}

impl ReadLayoutOps<Option<Tot>> for DataLayout2_0 {
    fn h_read_df_inner<R: Read>(
        &self,
        h: &mut BufReader<R>,
        tot: Option<Tot>,
        seg: &mut AnyDataSegment,
        conf: &ReadEventsConfig,
    ) -> WarningsAndIOGroupResult<
        DataFrameResult<Self::DfTarget>,
        ReadDataframeWarning,
        ReadDataframeError,
        (),
    > {
        self.0.h_read_into(h, tot, seg, conf)
    }
}

impl NormalizableLayout for DataLayout3_0 {
    fn normalize(&mut self) {}
}

impl IntoEmptyDataFrame for DataLayout3_0 {
    type DfTarget = DataFrame3_0;

    fn empty(&self) -> Self::DfTarget {
        self.0.empty().into()
    }
}

impl ReadLayoutOps<Identity<Tot>> for DataLayout3_0 {
    fn h_read_df_inner<R: Read>(
        &self,
        h: &mut BufReader<R>,
        tot: Identity<Tot>,
        seg: &mut AnyDataSegment,
        conf: &ReadEventsConfig,
    ) -> WarningsAndIOGroupResult<
        DataFrameResult<Self::DfTarget>,
        ReadDataframeWarning,
        ReadDataframeError,
        (),
    > {
        self.0.h_read_into(h, tot, seg, conf)
    }
}

impl IntoEmptyDataFrame for DataLayout3_1 {
    type DfTarget = DataFrame3_1;

    fn empty(&self) -> Self::DfTarget {
        self.0.empty().into()
    }
}

impl ReadLayoutOps<Identity<Tot>> for DataLayout3_1 {
    fn h_read_df_inner<R: Read>(
        &self,
        h: &mut BufReader<R>,
        tot: Identity<Tot>,
        seg: &mut AnyDataSegment,
        conf: &ReadEventsConfig,
    ) -> WarningsAndIOGroupResult<
        DataFrameResult<Self::DfTarget>,
        ReadDataframeWarning,
        ReadDataframeError,
        (),
    > {
        self.0.h_read_into(h, tot, seg, conf)
    }
}

impl Default for DataLayout3_2 {
    fn default() -> Self {
        Self::NonMixed(NonMixedEndianLayout::default())
    }
}

impl<W0: Default, W> Default for AnyEndianUint<W0, W> {
    fn default() -> Self {
        Self::Single(W0::default())
    }
}

impl<C08, C16, C24, C32: Default, C40, C48, C56, C64> Default
    for AnyUint<C08, C16, C24, C32, C40, C48, C56, C64>
{
    fn default() -> Self {
        Self::Uint32(C32::default())
    }
}

impl NormalizableLayout for DataLayout3_2 {
    fn normalize(&mut self) {
        *self = match mem::take(self) {
            Self::NonMixed(mut x) => {
                // this will simplify integer layouts as necessary
                x.normalize();
                Self::NonMixed(x)
            }
            Self::Mixed(x) => {
                if let Some((c0, cs)) = x.inner.split_first() {
                    let d0 = c0.col_datatype();
                    if cs.iter().all(|c| d0 == c.col_datatype()) {
                        let new = match d0 {
                            AlphaNumType::Ascii => {
                                let new = x.inner.fmap(|c| c.try_into().unwrap());
                                AnyDatatype::Ascii(AnyAscii::Fixed(ColumnGroup::new(
                                    new, NoByteOrd,
                                )))
                            }
                            AlphaNumType::Integer => {
                                let new = x.inner.fmap(|c| c.try_into().unwrap());
                                let mut l =
                                    AnyEndianUint::Multi(ColumnGroup::new(new, x.byte_layout));
                                l.normalize();
                                AnyDatatype::Uint(l)
                            }
                            AlphaNumType::Float => {
                                let new = x.inner.fmap(|c| c.try_into().unwrap());
                                AnyDatatype::F32(ColumnGroup::new(new, x.byte_layout))
                            }
                            AlphaNumType::Double => {
                                let new = x.inner.fmap(|c| c.try_into().unwrap());
                                AnyDatatype::F64(ColumnGroup::new(new, x.byte_layout))
                            }
                        };
                        Self::NonMixed(new)
                    } else {
                        Self::Mixed(x)
                    }
                } else {
                    // Return sane default if empty. This is useful since
                    // returning single will prevent tripping error routines
                    // when normalization is used as the first step in
                    // conversion from multi->single, where a multi result
                    // indicates failure.
                    Self::NonMixed(AnyDatatype::F32(ColumnGroup::new(vec![], x.byte_layout)))
                }
            }
        };
    }
}

impl<D> NormalizableLayout for AnyEndianUintLayout<D> {
    fn normalize(&mut self) {
        *self = match mem::take(self) {
            Self::Single(x) => Self::Single(x),
            Self::Multi(x) => {
                if let Some((c0, cs)) = x.inner.split_first() {
                    let n = c0.file_bytes();
                    if cs.iter().all(|c| n == c.file_bytes()) {
                        macro_rules! go {
                            ($var:ident) => {{
                                let new = x.inner.fmap(|c| c.try_into().unwrap());
                                AnyUint::$var(ColumnGroup::new(new, x.byte_layout))
                            }};
                        }
                        let new = match n {
                            PrivBytes::B1 => go!(Uint08),
                            PrivBytes::B2 => go!(Uint16),
                            PrivBytes::B3 => go!(Uint24),
                            PrivBytes::B4 => go!(Uint32),
                            PrivBytes::B5 => go!(Uint40),
                            PrivBytes::B6 => go!(Uint48),
                            PrivBytes::B7 => go!(Uint56),
                            PrivBytes::B8 => go!(Uint64),
                        };
                        Self::Single(new)
                    } else {
                        Self::Multi(x)
                    }
                } else {
                    // Return sane default if empty. This is useful since
                    // returning single will prevent tripping error routines
                    // when normalization is used as the first step in
                    // conversion from multi->single, where a multi result
                    // indicates failure.
                    Self::Single(AnyUint::Uint32(ColumnGroup::new(vec![], x.byte_layout)))
                }
            }
        };
    }
}

impl IntoEmptyDataFrame for DataLayout3_2 {
    type DfTarget = DataFrame3_2;

    fn empty(&self) -> Self::DfTarget {
        match_many_to_one!(self, Self, [Mixed, NonMixed], x, {
            Self::DfTarget::from(x.empty())
        })
    }
}

impl ReadLayoutOps<Identity<Tot>> for DataLayout3_2 {
    fn h_read_df_inner<R: Read>(
        &self,
        h: &mut BufReader<R>,
        tot: Identity<Tot>,
        seg: &mut AnyDataSegment,
        conf: &ReadEventsConfig,
    ) -> WarningsAndIOGroupResult<
        DataFrameResult<Self::DfTarget>,
        ReadDataframeWarning,
        ReadDataframeError,
        (),
    > {
        match self {
            Self::NonMixed(x) => x.h_read_into(h, tot, seg, conf),
            Self::Mixed(x) => x.h_read_into(h, tot, seg, conf),
        }
    }
}

impl From<DataFrame3_2> for PrimitiveDataFrame {
    fn from(value: DataFrame3_2) -> Self {
        match_many_to_one!(value, DataFrame3_2, [Mixed, NonMixed], x, { x.into() })
    }
}

impl From<MixedDataFrame> for PrimitiveDataFrame {
    fn from(value: MixedDataFrame) -> Self {
        value.inner.fmap(Into::into)
    }
}

impl IntoEmptyDataFrame for MixedLayout {
    type DfTarget = MixedDataFrame;

    fn empty(&self) -> Self::DfTarget {
        let cs = self.inner.iter().map(|c| c.clone().into());
        ColumnGroup::new(FFDataFrame::try_new(cs).unwrap(), self.byte_layout)
    }
}

macro_rules! match_any_mixed {
    ($value:expr, $inner:ident, $action:block) => {
        match_many_to_one!(
            $value,
            AnyDatatype,
            [Ascii, Uint, F32, F64],
            $inner,
            $action
        )
    };
}

impl<T> IntoEmptyDataFrame for AnyOrderedLayout<T> {
    type DfTarget = AnyOrderedDataFrame<T>;

    fn empty(&self) -> Self::DfTarget {
        match_any_mixed!(self, x, { AnyOrderedDataFrame::from(x.empty()) })
    }
}

impl<TotType> ReadLayoutOps<TotType> for AnyOrderedLayout<TotType> {
    fn h_read_df_inner<R: Read>(
        &self,
        h: &mut BufReader<R>,
        tot: TotType,
        seg: &mut AnyDataSegment,
        conf: &ReadEventsConfig,
    ) -> WarningsAndIOGroupResult<
        DataFrameResult<Self::DfTarget>,
        ReadDataframeWarning,
        ReadDataframeError,
        (),
    >
    where
        TotType: IsTot,
    {
        match_any_mixed!(self, x, { x.h_read_into(h, tot, seg, conf) })
    }
}

impl<T> From<AnyOrderedDataFrame<T>> for PrimitiveDataFrame {
    fn from(value: AnyOrderedDataFrame<T>) -> Self {
        match_any_mixed!(value, x, { x.into() })
    }
}

impl<D> Default for NonMixedEndianLayout<D> {
    fn default() -> Self {
        Self::Uint(AnyEndianUint::Multi(EndianLayout::default()))
    }
}

impl<A, I: NormalizableLayout, F32, F64> NormalizableLayout for AnyDatatype<A, I, F32, F64> {
    fn normalize(&mut self) {
        match self {
            Self::Ascii(_) => (),
            Self::Uint(x) => x.normalize(),
            Self::F32(_) => (),
            Self::F64(_) => (),
        }
    }
}

impl<D> IntoEmptyDataFrame for AnyEndianUintLayout<D> {
    type DfTarget = AnyEndianUintDataFrame<D>;

    fn empty(&self) -> Self::DfTarget {
        match_many_to_one!(self, AnyEndianUintLayout, [Single, Multi], x, {
            Self::DfTarget::from(x.empty())
        })
    }
}

impl<Dtype> ReadLayoutOps<Identity<Tot>> for AnyEndianUintLayout<Dtype>
where
    Dtype: IsNumType,
{
    fn h_read_df_inner<R: Read>(
        &self,
        h: &mut BufReader<R>,
        tot: Identity<Tot>,
        seg: &mut AnyDataSegment,
        conf: &ReadEventsConfig,
    ) -> WarningsAndIOGroupResult<
        DataFrameResult<Self::DfTarget>,
        ReadDataframeWarning,
        ReadDataframeError,
        (),
    > {
        match_many_to_one!(self, AnyEndianUintLayout, [Single, Multi], x, {
            x.h_read_into(h, tot, seg, conf)
        })
    }
}

impl<D> IntoEmptyDataFrame for NonMixedEndianLayout<D> {
    type DfTarget = NonMixedEndianDataFrame<D>;

    fn empty(&self) -> Self::DfTarget {
        match_many_to_one!(self, NonMixedEndianLayout, [Ascii, Uint, F32, F64], x, {
            Self::DfTarget::from(x.empty())
        })
    }
}

impl<Dtype> ReadLayoutOps<Identity<Tot>> for NonMixedEndianLayout<Dtype>
where
    Dtype: IsNumType,
{
    fn h_read_df_inner<R: Read>(
        &self,
        h: &mut BufReader<R>,
        tot: Identity<Tot>,
        seg: &mut AnyDataSegment,
        conf: &ReadEventsConfig,
    ) -> WarningsAndIOGroupResult<
        DataFrameResult<Self::DfTarget>,
        ReadDataframeWarning,
        ReadDataframeError,
        (),
    > {
        match_many_to_one!(self, NonMixedEndianLayout, [Ascii, Uint, F32, F64], x, {
            x.h_read_into(h, tot, seg, conf)
        })
    }
}

impl<D> IntoEmptyDataFrame for EndianLayout<AnyNullBitmask, D> {
    type DfTarget = EndianUintDataFrame<D>;

    fn empty(&self) -> Self::DfTarget {
        let cs = self.inner.iter().map(|&c| c.into());
        ColumnGroup::new(FFDataFrame::try_new(cs).unwrap(), self.byte_layout)
    }
}

impl<D> From<EndianUintDataFrame<D>> for PrimitiveDataFrame {
    fn from(value: EndianUintDataFrame<D>) -> Self {
        value.inner.fmap(Into::into)
    }
}

impl<D> From<AnyEndianUintDataFrame<D>> for PrimitiveDataFrame {
    fn from(value: AnyEndianUintDataFrame<D>) -> Self {
        match_many_to_one!(value, AnyEndianUintDataFrame, [Single, Multi], x, {
            x.into()
        })
    }
}

impl<D> From<NonMixedEndianDataFrame<D>> for PrimitiveDataFrame {
    fn from(value: NonMixedEndianDataFrame<D>) -> Self {
        match_many_to_one!(
            value,
            NonMixedEndianDataFrame,
            [Ascii, Uint, F32, F64],
            x,
            { x.into() }
        )
    }
}

impl<T, D, const ORD: bool> From<DelimAsciiLayout<T, D, ORD>> for AnyAsciiLayout<T, D, ORD> {
    fn from(value: DelimAsciiLayout<T, D, ORD>) -> Self {
        Self::Delimited(value)
    }
}

impl<T, D, const ORD: bool> From<FixedAsciiLayout<T, D, ORD>> for AnyAsciiLayout<T, D, ORD> {
    fn from(value: FixedAsciiLayout<T, D, ORD>) -> Self {
        Self::Fixed(value)
    }
}

impl<T, D, const ORD: bool> From<DelimAsciiDataFrame<T, D, ORD>> for AnyAsciiDataFrame<T, D, ORD> {
    fn from(value: DelimAsciiDataFrame<T, D, ORD>) -> Self {
        Self::Delimited(value)
    }
}

impl<T, D, const ORD: bool> From<FixedAsciiDataFrame<T, D, ORD>> for AnyAsciiDataFrame<T, D, ORD> {
    fn from(value: FixedAsciiDataFrame<T, D, ORD>) -> Self {
        Self::Fixed(value)
    }
}

impl<T, D, const ORD: bool> IntoEmptyDataFrame for AnyAsciiLayout<T, D, ORD> {
    type DfTarget = AnyAsciiDataFrame<T, D, ORD>;

    fn empty(&self) -> Self::DfTarget {
        match_many_to_one!(self, AnyAsciiLayout, [Delimited, Fixed], x, {
            AnyAsciiDataFrame::from(x.empty())
        })
    }
}

impl<TotType, Dtype, const ORD: bool> ReadLayoutOps<TotType> for AnyAsciiLayout<TotType, Dtype, ORD>
where
    Dtype: IsNumType,
{
    fn h_read_df_inner<R>(
        &self,
        h: &mut BufReader<R>,
        tot: TotType,
        seg: &mut AnyDataSegment,
        conf: &ReadEventsConfig,
    ) -> WarningsAndIOGroupResult<
        DataFrameResult<Self::DfTarget>,
        ReadDataframeWarning,
        ReadDataframeError,
        (),
    >
    where
        R: Read,
        TotType: IsTot,
    {
        match_many_to_one!(self, AnyAsciiLayout, [Delimited, Fixed], x, {
            x.h_read_into(h, tot, seg, conf)
        })
    }
}

impl<T, D, const ORD: bool> From<AnyAsciiDataFrame<T, D, ORD>> for PrimitiveDataFrame {
    fn from(value: AnyAsciiDataFrame<T, D, ORD>) -> Self {
        match_many_to_one!(value, AnyAsciiDataFrame, [Delimited, Fixed], x, {
            x.into()
        })
    }
}

// impl<T, D, const ORD: bool> From<DelimAsciiDataFrame<T, D, ORD>> for PrimitiveDataFrame {
//     fn from(value: DelimAsciiDataFrame<T, D, ORD>) -> Self {
//         value.columns.fmap(|c| PrimitiveColumn::from(c).into())
//     }
// }

// impl<T, D, const ORD: bool> IntoEmptyDataFrame for DelimAsciiLayout<T, D, ORD> {
//     type DfTarget = DelimAsciiDataFrame<T, D, ORD>;

//     fn empty(&self) -> Self::DfTarget {
//         let cs = self.columns.iter().map(|c| AnnotatedColumn::empty(*c));
//         Fixed::new(
//             FFDataFrame::try_new(cs).unwrap(),
//             NoByteOrd::<ORD>::default(),
//         )
//     }
// }

impl<C, L, T, D> IntoEmptyDataFrame for DataLayout<C, L, T, D>
where
    C: HasNativeType + Clone,
    L: Clone,
    C::Native: FCSRepr,
{
    type DfTarget = DataFrame<NativeColumn<C>, L, T, D>;

    fn empty(&self) -> Self::DfTarget {
        let cs = self.inner.iter().map(|c| AnnotatedColumn::empty(c.clone()));
        ColumnGroup::new(FFDataFrame::try_new(cs).unwrap(), self.byte_layout.clone())
    }
}

impl<C, L, T, D> From<DataFrame<NativeColumn<C>, L, T, D>> for PrimitiveDataFrame
where
    C: HasNativeType,
    NativeColumn<C>: Into<PrimitiveColumn<<C::Native as FCSRepr>::Prim>>,
    PrimitiveColumn<<C::Native as FCSRepr>::Prim>: Into<AnyPrimitiveColumn>,
    C::Native: FCSRepr,
{
    fn from(value: DataFrame<NativeColumn<C>, L, T, D>) -> Self {
        value
            .inner
            .fmap(|c| Into::<AnyPrimitiveColumn>::into(c.into()))
    }
}

impl<C08, C16, C24, C32, C40, C48, C56, C64> IntoEmptyDataFrame
    for AnyUint<C08, C16, C24, C32, C40, C48, C56, C64>
where
    C08: IntoEmptyDataFrame,
    C16: IntoEmptyDataFrame,
    C24: IntoEmptyDataFrame,
    C32: IntoEmptyDataFrame,
    C40: IntoEmptyDataFrame,
    C48: IntoEmptyDataFrame,
    C56: IntoEmptyDataFrame,
    C64: IntoEmptyDataFrame,
{
    type DfTarget = AnyUint<
        C08::DfTarget,
        C16::DfTarget,
        C24::DfTarget,
        C32::DfTarget,
        C40::DfTarget,
        C48::DfTarget,
        C56::DfTarget,
        C64::DfTarget,
    >;

    fn empty(&self) -> Self::DfTarget {
        match_map_uint!(self, x, x.empty())
    }
}

impl<TotType> ReadLayoutOps<TotType> for AnyOrderedUintLayout<TotType> {
    fn h_read_df_inner<R: Read>(
        &self,
        h: &mut BufReader<R>,
        tot: TotType,
        seg: &mut AnyDataSegment,
        conf: &ReadEventsConfig,
    ) -> WarningsAndIOGroupResult<
        DataFrameResult<Self::DfTarget>,
        ReadDataframeWarning,
        ReadDataframeError,
        (),
    >
    where
        TotType: IsTot,
    {
        match_any_uint!(self, Self, x, { x.h_read_into(h, tot, seg, conf) })
    }
}

impl<Dtype> ReadLayoutOps<Identity<Tot>> for AnySingleUintLayout<Dtype>
where
    Dtype: IsNumType,
{
    fn h_read_df_inner<R: Read>(
        &self,
        h: &mut BufReader<R>,
        tot: Identity<Tot>,
        seg: &mut AnyDataSegment,
        conf: &ReadEventsConfig,
    ) -> WarningsAndIOGroupResult<
        DataFrameResult<Self::DfTarget>,
        ReadDataframeWarning,
        ReadDataframeError,
        (),
    > {
        match_any_uint!(self, Self, x, { x.h_read_into(h, tot, seg, conf) })
    }
}

impl<C08, C16, C24, C32, C40, C48, C56, C64> AnyUint<C08, C16, C24, C32, C40, C48, C56, C64> {
    fn file_bytes(&self) -> PrivBytes
    where
        C08: HasNativeType,
        C16: HasNativeType,
        C24: HasNativeType,
        C32: HasNativeType,
        C40: HasNativeType,
        C48: HasNativeType,
        C56: HasNativeType,
        C64: HasNativeType,
        C08::Native: FCSRepr,
        C16::Native: FCSRepr,
        C24::Native: FCSRepr,
        C32::Native: FCSRepr,
        C40::Native: FCSRepr,
        C48::Native: FCSRepr,
        C56::Native: FCSRepr,
        C64::Native: FCSRepr,
    {
        let ret = match self {
            Self::Uint08(_) => C08::Native::FILE_BYTES,
            Self::Uint16(_) => C16::Native::FILE_BYTES,
            Self::Uint24(_) => C24::Native::FILE_BYTES,
            Self::Uint32(_) => C32::Native::FILE_BYTES,
            Self::Uint40(_) => C40::Native::FILE_BYTES,
            Self::Uint48(_) => C48::Native::FILE_BYTES,
            Self::Uint56(_) => C56::Native::FILE_BYTES,
            Self::Uint64(_) => C64::Native::FILE_BYTES,
        };
        ret.0
    }

    // fn fmap_into8<C08f, C16f, C24f, C32f, C40f, C48f, C56f, C64f>(
    //     self,
    // ) -> AnyUint<C08f, C16f, C24f, C32f, C40f, C48f, C56f, C64f>
    // where
    //     C08: Into<C08f>,
    //     C16: Into<C16f>,
    //     C24: Into<C24f>,
    //     C32: Into<C32f>,
    //     C40: Into<C40f>,
    //     C48: Into<C48f>,
    //     C56: Into<C56f>,
    //     C64: Into<C64f>,
    // {
    //     match_map_uint!(self, x, x.into())
    // }

    fn into8<X>(self) -> X
    where
        C08: Into<X>,
        C16: Into<X>,
        C24: Into<X>,
        C32: Into<X>,
        C40: Into<X>,
        C48: Into<X>,
        C56: Into<X>,
        C64: Into<X>,
    {
        match_any_uint!(self, AnyUint, x, { x.into() })
    }
}

impl<C08, C16, C24, C32, C40, C48, C56, C64> From<AnyUint<C08, C16, C24, C32, C40, C48, C56, C64>>
    for PrimitiveDataFrame
where
    C08: Into<Self>,
    C16: Into<Self>,
    C24: Into<Self>,
    C32: Into<Self>,
    C40: Into<Self>,
    C48: Into<Self>,
    C56: Into<Self>,
    C64: Into<Self>,
{
    fn from(value: AnyUint<C08, C16, C24, C32, C40, C48, C56, C64>) -> Self {
        value.into8()
    }
}

impl From<MixedRange> for MixedColumn {
    fn from(value: MixedRange) -> Self {
        match value {
            AnyDatatype::Ascii(x) => Self::Ascii(AnnotatedColumn::empty(x)),
            AnyDatatype::Uint(x) => Self::Uint(x.into()),
            AnyDatatype::F32(x) => Self::F32(AnnotatedColumn::empty(x)),
            AnyDatatype::F64(x) => Self::F64(AnnotatedColumn::empty(x)),
        }
    }
}

impl From<MixedColumn> for AnyPrimitiveColumn {
    fn from(value: MixedColumn) -> Self {
        match value {
            MixedColumn::Ascii(x) => Self::from(PrimitiveColumn::from(x)),
            MixedColumn::Uint(x) => x.into(),
            MixedColumn::F32(x) => Self::from(PrimitiveColumn::from(x)),
            MixedColumn::F64(x) => Self::from(PrimitiveColumn::from(x)),
        }
    }
}

impl From<MixedVec> for MixedColumn {
    fn from(value: MixedVec) -> Self {
        match value {
            MixedVec::Ascii(x) => Self::Ascii(NativeColumn::from(x)),
            MixedVec::Uint(x) => Self::Uint(x.into()),
            MixedVec::F32(x) => Self::F32(NativeColumn::from(x)),
            MixedVec::F64(x) => Self::F64(NativeColumn::from(x)),
        }
    }
}

type MixedVec = AnyDatatype<
    RangedVec<FixedAsciiRange, u64>,
    AnyUintVec,
    RangedVec<F32Range, f32>,
    RangedVec<F64Range, f64>,
>;

macro_rules! decl_mixed_read {
    ($name:ident, $int_fun:ident, $float_fun:ident) => {
        fn $name(
            &mut self,
            dst_index: usize,
            src: &[u8],
            src_index: usize,
        ) -> Result<(), AsciiToUintError> {
            match self {
                Self::Ascii(xs) => {
                    let src_width = usize::from(u8::from(xs.range.chars()));
                    xs.data[dst_index] = ascii_to_uint(&src[src_index..src_index + src_width])?;
                    return Ok(());
                }
                Self::Uint(xs) => xs.$int_fun(dst_index, src, src_index),
                Self::F32(xs) => xs.data[dst_index] = f32::$float_fun(src, src_index),
                Self::F64(xs) => xs.data[dst_index] = f64::$float_fun(src, src_index),
            }
            Ok(())
        }
    };
}

impl MixedVec {
    decl_mixed_read!(read_le, read_le, slice_be_bytes);
    decl_mixed_read!(read_be, read_be, slice_le_bytes);

    fn check_range(&mut self, i: MeasIndex, trunc: TruncateEventValues) -> TruncatedResult {
        match self {
            Self::Ascii(x) => x.data.check_range(&x.range, i, trunc),
            Self::Uint(x) => x.check_range(i, trunc),
            Self::F32(x) => x.data.check_range(&x.range, i, trunc),
            Self::F64(x) => x.data.check_range(&x.range, i, trunc),
        }
    }
}

impl<B, T> From<RangedVec<B, T>> for AnyPrimitiveColumn
where
    Self: From<PrimitiveColumn<T>>,
{
    fn from(value: RangedVec<B, T>) -> Self {
        Self::from(PrimitiveColumn::from(value.data))
    }
}

impl From<AnyNullBitmask> for AnyBitmaskColumn {
    fn from(value: AnyNullBitmask) -> Self {
        match_map_uint!(value, x, AnnotatedColumn::empty(x))
    }
}

impl From<AnyBitmaskColumn> for AnyPrimitiveColumn {
    fn from(value: AnyBitmaskColumn) -> Self {
        match_any_uint!(value, AnyBitmaskColumn, x, {
            PrimitiveColumn::from(x).into()
        })
    }
}

impl From<AnyUintVec> for AnyBitmaskColumn {
    fn from(value: AnyUintVec) -> Self {
        match_map_uint!(value, x, NativeColumn::from(x))
    }
}

macro_rules! decl_uint_read {
    ($name:ident, $fun:ident) => {
        fn $name(&mut self, dst_index: usize, src: &[u8], src_index: usize) {
            match self {
                Self::Uint08(xs) => {
                    xs.data[dst_index] = u8::$fun(src, src_index);
                }
                Self::Uint16(xs) => {
                    xs.data[dst_index] = u16::$fun(src, src_index);
                }
                Self::Uint24(xs) => {
                    xs.data[dst_index] = U24::$fun(src, src_index);
                }
                Self::Uint32(xs) => {
                    xs.data[dst_index] = u32::$fun(src, src_index);
                }
                Self::Uint40(xs) => {
                    xs.data[dst_index] = U40::$fun(src, src_index);
                }
                Self::Uint48(xs) => {
                    xs.data[dst_index] = U48::$fun(src, src_index);
                }
                Self::Uint56(xs) => {
                    xs.data[dst_index] = U56::$fun(src, src_index);
                }
                Self::Uint64(xs) => {
                    xs.data[dst_index] = u64::$fun(src, src_index);
                }
            }
        }
    };
}

impl AnyUintVec {
    decl_uint_read!(read_le, slice_le_bytes);
    decl_uint_read!(read_be, slice_be_bytes);

    fn check_range(&mut self, i: MeasIndex, trunc: TruncateEventValues) -> TruncatedResult {
        match_any_uint!(self, AnyUintVec, x, {
            x.data.check_range(&x.range, i, trunc)
        })
    }
}

// type AnyWriterBitmask<'a> = AnyBitmask<
//     UintColumnWriter<'a, Bitmask08>,
//     UintColumnWriter<'a, Bitmask16>,
//     UintColumnWriter<'a, Bitmask24>,
//     UintColumnWriter<'a, Bitmask32>,
//     UintColumnWriter<'a, Bitmask40>,
//     UintColumnWriter<'a, Bitmask48>,
//     UintColumnWriter<'a, Bitmask56>,
//     UintColumnWriter<'a, Bitmask64>,
// >;

// type UintColumnWriter<'a, C> = ColumnWriter<'a, C, <C as HasNativeType>::Native, Endian>;

// /// Instructions to write one column using an iterator
// #[derive(new)]
// struct ColumnWriter<'a, C, T, S> {
//     column_type: C,
//     data: AnySource<'a, T>,
//     loss: Option<AnyLossError>,
//     byte_layout: PhantomData<S>,
// }

// impl<C, T, S> ColumnWriter<'_, C, T, S> {
//     fn into_err(self, i: MeasIndex) -> Option<IndexedLossError> {
//         self.loss
//             .map(|error| IndexedError::new(i, error))
//             .map(IndexedLossError)
//     }
// }

impl RowBuffer {
    fn init(max_size: RowBufferSize, nrows: usize, row_width: usize) -> Self {
        // Max this to 1 here so that we always have at least one row we are
        // reading. If there are any machines that produce files with at least
        // 32KB rows (which would be ~1000 parameters at 32 bit column widths),
        // these will produce some lovely cache miss fireworks on most CPUs :/
        let rows_per_buffer = (max_size.0 / row_width).max(1);
        let buf_size = rows_per_buffer * row_width;
        Self {
            nrows,
            rows_per_buffer,
            buf_size: usize_to_u64(buf_size),
            row_width,
            bytes: Vec::with_capacity(buf_size),
        }
    }

    fn read_size<R: Read>(&mut self, h: &mut BufReader<R>, size: u64) -> io::Result<()> {
        self.bytes.clear();
        h.take(size).read_to_end(&mut self.bytes)?;
        Ok(())
    }

    fn read<R: Read>(&mut self, h: &mut BufReader<R>) -> io::Result<()> {
        self.read_size(h, self.buf_size)
    }

    fn whole_reads(&self) -> usize {
        self.nrows / self.rows_per_buffer
    }

    fn read_columns<C, E, R, Fr, Fw>(
        &mut self,
        h: &mut BufReader<R>,
        columns: &mut [C],
        mut fread: Fr,
        fwidth: Fw,
    ) -> IOResult<(), E>
    where
        R: Read,
        // TODO newtype these indices to be less confusing
        // dst bytes, dst index, src bytes, src index, column_index
        Fr: FnMut(&mut C, usize, &[u8], usize) -> Result<(), E>,
        Fw: Fn(usize) -> usize,
    {
        // Read groups of rows in outer loop
        let mut src_col_offset;
        let mut dst_row_offset = 0;
        for _ in 0..self.whole_reads() {
            self.read(h)?;
            src_col_offset = 0;
            // Once we have a buffer, iterate through each column and write data
            for (ci, c) in columns.iter_mut().enumerate() {
                // Within each column, write rows, striding the row buffer and
                // indexing consecutively in the current column
                let src_width = fwidth(ci);
                for row in 0..self.rows_per_buffer {
                    let src_idx = src_col_offset + self.row_width * row;
                    let dst_idx = dst_row_offset + row;
                    fread(c, dst_idx, &self.bytes, src_idx).map_err(ImpureError::Pure)?;
                }
                src_col_offset += src_width;
            }
            dst_row_offset += self.rows_per_buffer;
        }

        // Read remaining rows if they exist
        let remainder_rows = self.nrows % self.rows_per_buffer;
        let remainder_size = usize_to_u64(remainder_rows * self.row_width);
        self.read_size(h, remainder_size)?;
        src_col_offset = 0;
        for (ci, c) in columns.iter_mut().enumerate() {
            for row in 0..remainder_rows {
                let src_idx = src_col_offset + self.row_width * row;
                let dst_idx = dst_row_offset + row;
                fread(c, dst_idx, &self.bytes, src_idx).map_err(ImpureError::Pure)?;
            }
            src_col_offset += fwidth(ci);
        }

        Ok(())
    }

    /// Read stream of bytes using buffer where each value is the same type
    fn read_matrix<R, T, F>(
        &mut self,
        h: &mut BufReader<R>,
        columns: &mut [Vec<T>],
        from_buf: F,
    ) -> io::Result<()>
    where
        R: Read,
        F: Fn(&T::FileBuf) -> T,
        T: FCSRepr,
    {
        // This method has several nice optimizations:
        // 1. No errors on the inner two loops
        // 2. All values have the same byte layout, which means we don't need
        //    to dispatch different methods for different columns
        // 3. Using the assertions below and some unsafe code, we can remove
        //    all bounds checks on the inner loop.
        //
        // 1-3 above mean that that two inner loops have no jumps, which means
        // the compiler can unroll the loops and possibly autovectorize.
        let src_len = T::file_len();
        assert!(
            columns.iter().all(|c| c.len() == self.nrows),
            "all column lengths should be equal to given row number"
        );
        assert!(
            columns.len() * src_len == self.row_width,
            "incorrect column number size"
        );
        assert!(
            self.rows_per_buffer * self.whole_reads() <= self.nrows,
            "invalid whole reads number"
        );

        // Read groups of rows in outer loop
        for buf_idx in 0..self.whole_reads() {
            self.read(h)?;
            let start_row = buf_idx * self.rows_per_buffer;
            // Once we have a buffer, iterate through each column and write data
            for (ci, c) in columns.iter_mut().enumerate() {
                let src_col_offset = ci * src_len;
                // Within each column, write rows, striding the row buffer and
                // indexing consecutively in the current column
                let end_row = start_row + self.rows_per_buffer;
                let local_c = &mut c[start_row..end_row];
                for (row, value) in local_c.iter_mut().enumerate() {
                    let src_idx = src_col_offset + self.row_width * row;
                    debug_assert!(
                        src_idx + src_len < u64_to_usize(self.buf_size),
                        "out of bounds"
                    );
                    // SAFETY: src_idx given as row_width * R + C * LEN where R
                    // is row index (within the buffer) and C is column index.
                    // Both R and C must be less than the number of rows per
                    // buffer and the number of columns respectively since we
                    // are getting these via enumerate(). Therefore, the maximum
                    // that src_idx can ever be is row_width * (rows_per_buffer
                    // - 1) + (column_number - 1) * LEN. Adding LEN to the end
                    // of this exactly equals the size of the buffer itself in
                    // bytes, which means what follows can never overflow.
                    let buf = unsafe { T::array_from_slice(&self.bytes, src_idx) };
                    // let xs = unsafe { self.bytes.get_unchecked(src_idx..src_idx + src_len) };
                    // // SAFETY: this will not overflow because the slice and
                    // // array are both LEN u8 elements.
                    // let buf: T::FileBuf = unsafe { *(xs.as_ptr().cast()) };
                    *value = from_buf(&buf);
                }
            }
        }

        // Read remaining rows if they exist
        let remainder_rows = self.nrows % self.rows_per_buffer;
        let remainder_size = usize_to_u64(remainder_rows * self.row_width);
        self.read_size(h, remainder_size)?;
        for (ci, c) in columns.iter_mut().enumerate() {
            let src_col_offset = ci * src_len;
            let dst_row_offset = self.whole_reads() * self.rows_per_buffer;
            let local_c = &mut c[dst_row_offset..dst_row_offset + remainder_rows];
            for (row, value) in local_c.iter_mut().enumerate() {
                let src_idx = src_col_offset + self.row_width * row;
                debug_assert!(
                    src_idx + src_len < u64_to_usize(self.buf_size),
                    "out of bounds"
                );
                // SAFETY: see above
                let buf = unsafe { T::array_from_slice(&self.bytes, src_idx) };
                // let xs = unsafe { self.bytes.get_unchecked(src_idx..src_idx + src_len) };
                // SAFETY: see above
                // let buf: T::FileBuf = unsafe { *(xs.as_ptr().cast()) };
                *value = from_buf(&buf);
            }
        }

        Ok(())
    }

    /// Read a matrix where type is an aligned big or little endian value.
    fn read_endian_matrix<R, T>(
        &mut self,
        h: &mut BufReader<R>,
        cols: &mut [Vec<T>],
        endian: Endian,
    ) -> io::Result<()>
    where
        R: Read,
        T: FromBytes<Bytes = T::FileBuf> + FCSRepr,
    {
        match endian {
            Endian::Big => self.read_matrix(h, cols, T::from_be_bytes),
            Endian::Little => self.read_matrix(h, cols, T::from_le_bytes),
        }
    }

    /// Read a matrix where type is an aligned big, little, or mixed endian value.
    fn read_ordered_matrix<R, T>(
        &mut self,
        h: &mut BufReader<R>,
        cols: &mut [Vec<T>],
        s: ArrayByteOrd<T::ByteOrd>,
    ) -> io::Result<()>
    where
        R: Read,
        T: FromBytes<Bytes = T::FileBuf> + FCSRepr,
        T::FileBuf: AsRef<[u8]> + AsMut<[u8]> + Default,
        T::ByteOrd: AsRef<[u8]>,
    {
        match s {
            ArrayByteOrd::Endian(e) => self.read_endian_matrix(h, cols, e),
            ArrayByteOrd::Order(o) => self.read_matrix(h, cols, |bs| T::from_ordered_bytes(bs, &o)),
        }
    }

    // /// Read matrix or arbitrarily ordered uints whose size is not a power of 2.
    // fn read_unaligned_ordered_int_matrix<R, T, const SRC_LEN: usize, const DST_LEN: usize>(
    //     &mut self,
    //     h: &mut BufReader<R>,
    //     cols: &mut [Vec<T>],
    //     s: SizedByteOrd<SRC_LEN>,
    // ) -> io::Result<()>
    // where
    //     R: Read,
    //     [u8; DST_LEN]: Default,
    //     T: UnalignedIntFromBytes<SRC_LEN, DST_LEN>,
    // {
    //     match s {
    //         SizedByteOrd::Endian(e) => self.read_unaligned_endian_int_matrix(h, cols, e),
    //         SizedByteOrd::Order(o) => self.read_matrix::<_, _, _, SRC_LEN>(h, cols, |bs| {
    //             T::from_unaligned_ordered_bytes(bs, o)
    //         }),
    //     }
    // }

    // /// Read matrix or endian uints whose size is not a power of 2.
    // fn read_unaligned_endian_int_matrix<R, T, const SRC_LEN: usize, const DST_LEN: usize>(
    //     &mut self,
    //     h: &mut BufReader<R>,
    //     cols: &mut [Vec<T>],
    //     s: Endian,
    // ) -> io::Result<()>
    // where
    //     R: Read,
    //     [u8; DST_LEN]: Default,
    //     T: UnalignedIntFromBytes<SRC_LEN, DST_LEN>,
    // {
    //     match s {
    //         Endian::Big => {
    //             self.read_matrix::<_, _, _, SRC_LEN>(h, cols, T::from_unaligned_be_bytes)
    //         }
    //         Endian::Little => {
    //             self.read_matrix::<_, _, _, SRC_LEN>(h, cols, T::from_unaligned_le_bytes)
    //         }
    //     }
    // }

    /// Read a matrix where input bytes characters to be read as u64
    fn read_char_matrix<R: Read>(
        &mut self,
        h: &mut BufReader<R>,
        cols: &mut [RangedVec<FixedAsciiRange, u64>],
    ) -> IOResult<(), AsciiToUintError> {
        let ranges: Vec<_> = cols
            .iter()
            .map(|c| usize::from(u8::from(c.range.chars())))
            .collect();
        self.read_columns(
            h,
            cols,
            |dst, dst_index, src, src_index| {
                let src_width = usize::from(u8::from(dst.range.chars()));
                let x = ascii_to_uint(&src[src_index..src_index + src_width])?;
                dst.data[dst_index] = x;
                Ok(())
            },
            |i| ranges[i],
        )
    }

    /// Read a dataframe of unsigned integers with different widths
    fn read_any_uint_df<R: Read>(
        &mut self,
        h: &mut BufReader<R>,
        cols: &mut [AnyUintVec],
        endian: Endian,
    ) -> io::Result<()> {
        let src_widths: Vec<_> = cols
            .iter()
            .map(|c| usize::from(u8::from(PrivBytes::from(c))))
            .collect();
        let res = match endian {
            Endian::Big => self.read_columns(
                h,
                cols,
                |dst, dst_index, src, src_index| {
                    dst.read_be(dst_index, src, src_index);
                    Ok(())
                },
                |i| src_widths[i],
            ),
            Endian::Little => self.read_columns(
                h,
                cols,
                |dst, dst_index, src, src_index| {
                    dst.read_le(dst_index, src, src_index);
                    Ok(())
                },
                |i| src_widths[i],
            ),
        };
        res.map_err(|e: ImpureError<Infallible>| {
            let ImpureError::IO(i) = e;
            i
        })
    }

    /// Read a dataframe of any mix of column types
    fn read_mixed_df<R: Read>(
        &mut self,
        h: &mut BufReader<R>,
        cols: &mut [MixedVec],
        endian: Endian,
    ) -> IOResult<(), AsciiToUintError> {
        // TODO make a version of this that does not have ascii and therefore
        // cannot error, this should allow tighter loops since there will be
        // jmp op to check the error
        let src_widths: Vec<_> = cols
            .iter()
            .map(|c| match c {
                MixedVec::Ascii(x) => usize::from(u8::from(x.range.chars())),
                MixedVec::Uint(x) => usize::from(u8::from(PrivBytes::from(x))),
                MixedVec::F32(_) => 4,
                MixedVec::F64(_) => 8,
            })
            .collect();
        match endian {
            Endian::Big => self.read_columns(h, cols, AnyDatatype::read_be, |i| src_widths[i]),
            Endian::Little => self.read_columns(h, cols, AnyDatatype::read_le, |i| src_widths[i]),
        }
    }
}

/// A type which represents a column-specific datatype (or lack thereof)
pub trait IsNumType: Sized {
    fn lookup_datatype(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        i: MeasIndex,
        conf: &ReadDataKeywordsConfig,
    ) -> DeferredWarningAndError<Self, OptIndexedKeyError<NumType>, OptIndexedKeyError<NumType>>;

    fn lookup_datatype_ro(
        kws: &StdKeywords,
        i: MeasIndex,
        conf: &ReadDataKeywordsConfig,
    ) -> DeferredWarningAndError<Self, OptIndexedKeyError<NumType>, OptIndexedKeyError<NumType>>;

    fn lookup_all(
        std: &mut StdKeywords,
        meas_nonstd: &mut [NonStdKeywords],
        conf: &ReadDataKeywordsConfig,
    ) -> LookupMeasLayoutResult<Self> {
        meas_nonstd
            .iter_mut()
            .enumerate()
            .map(|(i, nkws)| Self::lookup_one(std, nkws, i.into(), conf))
            .sequence_commutative()
    }

    #[must_use]
    fn lookup_ro_all(
        kws: &StdKeywords,
        par: Par,
        conf: &ReadDataKeywordsConfig,
    ) -> LookupMeasLayoutResult<Self> {
        (0..par.0)
            .map(|i| Self::lookup_one_ro(kws, i.into(), conf))
            .sequence_commutative()
    }

    fn lookup_one(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        i: MeasIndex,
        conf: &ReadDataKeywordsConfig,
    ) -> LookupOneMeasLayoutResult<Self> {
        let width = Width::remove_meas_req(std, i);
        let range = Range::remove_meas_req(std, i);
        let datatype = Self::lookup_datatype(std, nonstd, i, conf);
        Self::make_meas(width, range, datatype)
    }

    #[must_use]
    fn lookup_one_ro(
        kws: &StdKeywords,
        i: MeasIndex,
        conf: &ReadDataKeywordsConfig,
    ) -> LookupOneMeasLayoutResult<Self> {
        let width = Width::get_meas_req(kws, i);
        let range = Range::get_meas_req(kws, i);
        let datatype = Self::lookup_datatype_ro(kws, i, conf);
        Self::make_meas(width, range, datatype)
    }

    fn make_meas(
        width: Result<Width, ReqIndexedKeyError<Width>>,
        range: Result<Range, ReqIndexedKeyError<Range>>,
        datatype: DeferredWarningAndError<
            Self,
            OptIndexedKeyError<NumType>,
            OptIndexedKeyError<NumType>,
        >,
    ) -> LookupOneMeasLayoutResult<Self> {
        let w = width.map_err(LookupMeasLayoutError::from).into_log();
        let r = range.map_err(LookupMeasLayoutError::from).into_log();
        let d = datatype
            .map_errors(LookupMeasLayoutError::from)
            .into_semigroup();
        w.zip3_commutative(r, d)
            .map_ok_value(|(w_, r_, d_)| ColumnLayoutValues::new(w_, r_, d_))
    }
}

/// Methods for a type which may or may not have $TOT
pub trait IsTot: Sized + MightHave<Tot> {
    fn with_tot<F, G, I, X>(input: I, tot: Self, tot_f: F, notot_f: G) -> X
    where
        F: FnOnce(I, Tot) -> X,
        G: FnOnce(I) -> X,
    {
        if let Some(t) = tot.to_opt() {
            tot_f(input, t)
        } else {
            notot_f(input)
        }
    }

    fn check_tot(
        total_events: u64,
        tot: Self,
        flag: AllowTotMismatch,
    ) -> SwitchableErrorResult<Option<bool>, (), AllowTotMismatch, TotEventMismatchError> {
        Self::with_tot(
            (),
            tot,
            |(), t| Self::check_tot_inner(total_events, t, flag).map_ok_value(Some),
            |()| LogResult::new_switchable_ok(None, flag),
        )
    }

    #[must_use]
    fn check_tot_inner(
        total_events: u64,
        tot: Tot,
        flag: AllowTotMismatch,
    ) -> SwitchableErrorResult<bool, (), AllowTotMismatch, TotEventMismatchError> {
        let count = usize::try_from(total_events)
            .expect("event count exceeded maximum platform pointer size");
        let i = TotEventMismatchError { tot, total_events };
        let tot_eq = tot.0 == count;
        LogResult::new_switchable_ok_if3(tot_eq, !tot_eq, (), i, flag)
    }
}

#[delegatable_trait]
pub trait LayoutDims: Sized {
    fn ncols(&self) -> usize;

    fn clear(&mut self);
}

#[delegatable_trait]
pub trait LayoutRanges: Sized {
    fn ranges(&self) -> Vec<Range>;
}

#[delegatable_trait]
pub trait LayoutDatatype: Sized {
    fn datatype(&self) -> AlphaNumType;

    fn datatypes(&self) -> Vec<AlphaNumType>;

    fn check_transforms<S, G>(&self, xforms: &[S]) -> GroupResult<(), S::Err, G>
    where
        S: CheckedScaleTransform,
        G: Default,
    {
        let ds = self.datatypes();
        debug_assert!(
            xforms.len() == ds.len(),
            "transforms length must be same as column number"
        );
        let es = ds
            .iter()
            .zip(xforms)
            .enumerate()
            // Only integers are allowed to have gain and log scaling, so
            // everything else should be a "noop" transform (ie a linear
            // transform with slope of 1.0). NOTE the standard itself is
            // vague about what should happen to ASCII values (presumably
            // since nobody cares) so here we just treat them like we treat
            // floating point types to keep the logic simple.
            .filter_map(|(i, (&datatype, s))| s.matches_datatype(datatype, i.into()).err());
        ErrorGroup::try_new(es)
    }

    fn check_transforms_and_len<S, G>(&self, xforms: &[S]) -> Result<(), MeasLayoutMismatchError>
    where
        Self: LayoutDims,
        G: Default,
        S: CheckedScaleTransform,
        ScaleDatatypeMismatchError: From<ErrorGroup<S::Err, G>>,
    {
        let meas_n = xforms.len();
        let layout_n = self.ncols();
        if meas_n != layout_n {
            let e = MeasLayoutLengthsError { meas_n, layout_n };
            return Err(e.into());
        }
        self.check_transforms(xforms)
            .map_err(ScaleDatatypeMismatchError::from)?;
        Ok(())
    }
}

/// A type which has optional measurement keywords.
///
/// This is only used to return $PnDATATYPE.
#[delegatable_trait]
pub trait OptMeasLayoutKeywords {
    /// Return vector of $PnDATATYPE.
    ///
    /// Vector length will equal DATA column number. `None` will be returned
    /// if $PnDATATYPE is not provided. For pre-3.2 layouts, all will be `None`.
    fn opt_meas_keywords(&self) -> Vec<Option<SplitKeyword1<NumType>>>;
}

/// A layout that can be simplified into another layout of the same type.
pub trait NormalizableLayout {
    fn normalize(&mut self);
}

#[delegatable_trait]
pub trait LayoutKeywords: Sized + LayoutDatatype {
    fn byteord_keyword(&self) -> ReqRootKeyword<'_>;

    fn req_keywords(&self) -> [ReqRootKeyword<'_>; 2] {
        let d = ReqRootKeyword::from_value(self.datatype());
        [d, self.byteord_keyword()]
    }

    fn req_meas_keywords(&self) -> Vec<[ReqMeasKeyword<'_>; 2]>;
}

#[delegatable_trait]
pub trait IntoEmptyDataFrame {
    type DfTarget;

    fn empty(&self) -> Self::DfTarget;
}

#[delegatable_trait]
pub trait ReadLayoutOps<T>: Sized + IntoEmptyDataFrame {
    fn h_read_into<R, X>(
        &self,
        h: &mut BufReader<R>,
        tot: T,
        seg: &mut AnyDataSegment,
        conf: &ReadEventsConfig,
    ) -> WarningsAndIOGroupResult<DataFrameResult<X>, ReadDataframeWarning, ReadDataframeError, ()>
    where
        R: Read,
        T: IsTot,
        Self::DfTarget: Into<X>,
    {
        self.h_read_df_inner(h, tot, seg, conf)
            .map_ok_value(Functor::fmap_into)
    }

    fn h_read_df_inner<R: Read>(
        &self,
        h: &mut BufReader<R>,
        tot: T,
        seg: &mut AnyDataSegment,
        conf: &ReadEventsConfig,
    ) -> WarningsAndIOGroupResult<
        DataFrameResult<Self::DfTarget>,
        ReadDataframeWarning,
        ReadDataframeError,
        (),
    >
    where
        T: IsTot;
}

#[derive(Default, new)]
pub struct DataFrameResult<D> {
    pub(crate) dataframe: D,
    pub(crate) diagnostics: EventsDiagnostics,
}

impl_kind1!(pub DataFrameResultFamily, DataFrameResult);

impl_functor_once!(
    DataFrameResult,
    self,
    mut f,
    DataFrameResult::new(f(self.dataframe), self.diagnostics)
);

/// A type which can accept a new column.
#[delegatable_trait]
pub trait Insertable<Column> {
    /// Error to emit if new column is not compatible with existing columns.
    type Error;

    /// Insert a new column at index.
    ///
    /// This will panic if index is out of bounds.
    fn insert_nocheck0(&mut self, index: MeasIndex, col: Column) -> Result<(), Self::Error>;

    /// Push new column to the right of the current column vector.
    fn push0(&mut self, col: Column) -> Result<(), Self::Error>;
}

/// A type which can have a column removed from it.
#[delegatable_trait]
pub trait Removable<Column>: Sized {
    /// Remove a column.
    ///
    /// Will panic if index is out of bounds.
    fn remove_nocheck(&mut self, index: MeasIndex) -> Column;
}

/// Standardized operations on ordered layouts
#[delegatable_trait]
pub trait OrderedLayoutOps: Sized {
    fn byte_order(&self) -> ByteOrd2_0;

    fn endianness(&self) -> Option<Endian> {
        self.byte_order().try_into().ok()
    }
}

/// A version-specific data layout
pub trait VersionedDataLayout
where
    for<'a> Self: Sized
        + ReadLayoutOps<Self::Tot>
        + LayoutDatatype
        + LayoutDims
        + Removable<Range>
        + OptMeasLayoutKeywords,
{
    type ByteLayout;
    type NumType: IsNumType;
    type Tot: IsTot;

    fn lookup(
        std: &mut StdKeywords,
        meas_nonstd: &mut [NonStdKeywords],
        conf: &ReadDataKeywordsConfig,
    ) -> LookupLayoutResult<NewLayout<Self>>;

    fn lookup_ro(
        kws: &StdKeywords,
        par: Par,
        conf: &ReadDataKeywordsConfig,
    ) -> LookupLayoutResult<NewLayout<Self>>;

    fn new_empty(datatype: AlphaNumType) -> Self;

    fn try_new(
        datatype: AlphaNumType,
        byteord: Self::ByteLayout,
        columns: Vec<ColumnLayoutValues<Self::NumType>>,
        conf: &ReadDataKeywordsConfig,
    ) -> WarningsAndErrorsResult<NewLayout<Self>, (), NewMixedTypeWarning, NewDataLayoutError>;

    fn h_read_df<R: Read + Seek>(
        &self,
        h: &mut BufReader<R>,
        tot: Self::Tot,
        seg: &mut AnyDataSegment,
        conf: &ReadEventsConfig,
    ) -> WarningsAndIOGroupResult<
        DataFrameResult<Self::DfTarget>,
        ReadDataframeWarning,
        ReadDataframeError,
        (),
    > {
        match seg.try_abs_coords() {
            // if we cannot get coords, it means the segment is empty, thus the
            // returned dataframe should be empty
            None => {
                let ret = DataFrameResult::new(self.empty(), EventsDiagnostics::default());
                LogResult::new_ok(ret)
            }
            Some((begin, _)) => h
                .seek(SeekFrom::Start(begin))
                .map_err(IOErrorGroup::from)
                .into_log()
                .nowarn_and_then(|_| self.h_read_df_inner(h, tot, seg, conf)),
        }
    }

    // fn h_write_df<W>(
    //     &self,
    //     h: &mut BufWriter<W>,
    //     df: &PrimitiveDataFrame,
    //     skip_conv_check: bool,
    // ) -> WarningsAndErrorResult<(), (), IndexedLossError, io::Error>
    // where
    //     W: Write,
    // {
    //     // The dataframe should be encapsulated such that a) the column number
    //     // matches the number of measurements. If these are not true, the code
    //     // is wrong.
    //     let par = self.ncols();
    //     let ncols = df.ncols();
    //     debug_assert!(
    //         ncols == par,
    //         "dataframe columns ({ncols}) unequal to number of measurements ({par})"
    //     );
    //     self.h_write_df_inner(h, df, skip_conv_check)
    // }

    fn check_measurement_vector_nolen<N, T, O: AsScaleOrTransform>(
        &self,
        meas: &Measurements<N, T, O>,
    ) -> Result<(), ScaleDatatypeMismatchError>
    where
        O::S: CheckedScaleTransform,
        <O::S as CheckedScaleTransform>::Summary: Default,
        ScaleDatatypeMismatchError: From<
            ErrorGroup<
                <O::S as CheckedScaleTransform>::Err,
                <O::S as CheckedScaleTransform>::Summary,
            >,
        >,
    {
        let xforms: Vec<_> = meas
            .iter_with(&|_, _| O::S::default(), &|_, m| {
                m.value.specific.as_scale_or_transform()
            })
            .collect();
        self.check_transforms(&xforms[..])?;
        Ok(())
    }

    fn check_measurement_vector<N, T, O: AsScaleOrTransform>(
        &self,
        meas: &Measurements<N, T, O>,
    ) -> Result<(), MeasLayoutMismatchError>
    where
        O::S: CheckedScaleTransform,
        <O::S as CheckedScaleTransform>::Summary: Default,
        ScaleDatatypeMismatchError: From<
            ErrorGroup<
                <O::S as CheckedScaleTransform>::Err,
                <O::S as CheckedScaleTransform>::Summary,
            >,
        >,
    {
        let xforms: Vec<_> = meas
            .iter_with(&|_, _| O::S::default(), &|_, m| {
                m.value.specific.as_scale_or_transform()
            })
            .collect();
        self.check_transforms_and_len(&xforms[..])
    }

    #[allow(clippy::type_complexity)]
    fn try_new_measurements<M: VersionedMetaroot>(
        &self,
        measurements: NamedTemporalsAndOpticals<M>,
    ) -> Result<Measurements<M::Name, M::Temporal, M::Optical>, MeasurementsWithLayoutError>
    where
        M::Optical: AsScaleOrTransform,
        <M::Optical as AsScaleOrTransform>::S: CheckedScaleTransform,
        <<M::Optical as AsScaleOrTransform>::S as CheckedScaleTransform>::Summary: Default,
        ScaleDatatypeMismatchError: From<
            ErrorGroup<
                <<M::Optical as AsScaleOrTransform>::S as CheckedScaleTransform>::Err,
                <<M::Optical as AsScaleOrTransform>::S as CheckedScaleTransform>::Summary,
            >,
        >,
    {
        let ms = NamedVec::try_new(measurements)?;
        self.check_measurement_vector(&ms)
            .map_err(MeasurementsWithLayoutError::from)?;
        Ok(ms)
    }
}

/// Convert layout to new FCS version
pub trait ConvertFromLayout<T>: Sized
where
    Self: VersionedDataLayout,
{
    fn convert_from_layout(value: T) -> LayoutConvertResult<Self>;
}

/// A scale transform which may be checked against a datatype to ensure compatibility
pub trait CheckedScaleTransform {
    type Err;
    type Summary;

    fn matches_datatype(&self, datatype: AlphaNumType, i: MeasIndex) -> Result<(), Self::Err>;
}

/// A column which has exactly one native Rust type
pub trait HasNativeType: Sized {
    /// The native rust type
    type Native: Default + Copy;
}

/// A column which has exactly one $DATATYPE value always always
trait HasOneDatatype: Sized {
    const DATATYPE: AlphaNumType;
}

/// A column which has a $DATATYPE keyword
trait HasDatatype: Sized {
    fn col_datatype(&self) -> AlphaNumType;

    fn datatype_from_columns(cs: &[Self]) -> AlphaNumType;
}

pub trait FromRange: Sized {
    type Error;

    fn from_range(range: Range) -> Result<ConvertedRange<Self>, Self::Error> {
        Self::from_range_inner(range)
            .set_err_value(())
            .resolve_nowarn()
    }

    fn from_range_switch(
        range: Range,
        flag: DisallowRangeTrunc,
    ) -> DeferredSwitchableError<ConvertedRange<Self>, DisallowRangeTrunc, Self::Error> {
        Self::from_range_inner(range).nowarn_into_switchable3(flag)
    }

    fn from_range_inner(range: Range) -> DeferredError<ConvertedRange<Self>, Self::Error>;
}

trait IntoRange: HasNativeType {
    fn as_range(&self) -> (Self::Native, Range);
}

/// A type which has a known width
pub trait IsFixed {
    fn nbytes(&self) -> NonZeroU8;

    fn fixed_width(&self) -> BitsOrChars;
}

// /// A column which may be transformed into a writer for a rust numeric type
// trait ToNativeWriter
// where
//     Self: HasNativeType,
// {
//     type Error;

//     fn into_native_writer<'a, S>(
//         self,
//         c: &'a AnyFCSColumn,
//     ) -> ColumnWriter<'a, Self, Self::Native, S>
//     where
//         Self::Native: Default + Copy + AllFCSCast,
//         AnySource<'a, Self::Native>: From<FCSColIter<'a, u8, Self::Native>>
//             + From<FCSColIter<'a, u16, Self::Native>>
//             + From<FCSColIter<'a, u32, Self::Native>>
//             + From<FCSColIter<'a, u64, Self::Native>>
//             + From<FCSColIter<'a, f32, Self::Native>>
//             + From<FCSColIter<'a, f64, Self::Native>>,
//     {
//         ColumnWriter::new(self, AnySource::new(c), None)
//     }

//     fn check_native_writer(&self, col: &AnyFCSColumn) -> Result<(), LossError<Self::Error>>
//     where
//         Self::Native: Default + Copy + AllFCSCast,
//     {
//         col.check_writer(|x| Self::check_other_loss(self, x))
//     }

//     fn check_other_loss(&self, x: Self::Native) -> Option<Self::Error>;
// }

// trait NativeWritable<S>: HasNativeType {
//     fn h_write<W: Write>(
//         &self,
//         h: &mut BufWriter<W>,
//         x: CastResult<Self::Native>,
//         byte_layout: S,
//     ) -> io::Result<Option<AnyLossError>>;
// }

// trait IntoWriter<'a, S> {
//     type Target: Writable<'a, S>;

//     fn into_writer(self, col: &'a AnyFCSColumn) -> Self::Target;

//     fn check_writer(&self, col: &'a AnyFCSColumn) -> Result<(), AnyLossError>;
// }

// trait Writable<'a, S> {
//     fn h_write<W: Write>(&mut self, h: &mut BufWriter<W>, byte_layout: S) -> io::Result<()>;

//     fn truncate(self, skip_conv_check: bool) -> (AnyFCSColumn, Option<AnyLossError>);

//     fn into_err(self, i: MeasIndex) -> Option<IndexedLossError>;
// }

// trait Castable: Sized + HasNativeType {
//     fn with_cast(&self, x: CastResult<Self::Native>) -> (Self::Native, Option<AnyLossError>);
// }

// /// General methods for each numeric type.
// ///
// /// This is mostly for converting to/from bytes with various endian-ness.
// // TODO clean this up with https://github.com/rust-lang/rust/issues/76560 once
// // it lands in a stable compiler, in theory there is no reason to put the length
// // of the type as a parameter, but the current compiler is not smart enough
// trait NumProps: Sized + Copy {
//     const LEN: usize;
//     type BUF: AsRef<[u8]> + AsMut<[u8]> + Default;

//     fn from_big(buf: &Self::BUF) -> Self;

//     fn from_little(buf: &Self::BUF) -> Self;

//     fn slice_from_little(bytes: &[u8], i: usize) -> Self {
//         let mut tmp = Self::BUF::default();
//         tmp.as_mut().copy_from_slice(&bytes[i..i + Self::LEN]);
//         Self::from_little(&tmp)
//     }

//     fn slice_from_big(bytes: &[u8], i: usize) -> Self {
//         let mut tmp = Self::BUF::default();
//         tmp.as_mut().copy_from_slice(&bytes[i..i + Self::LEN]);
//         Self::from_big(&tmp)
//     }

//     fn to_big(&self) -> Self::BUF;

//     fn to_little(&self) -> Self::BUF;

//     // fn to_endian(&self, endian: Endian) -> Self::BUF {
//     //     match endian {
//     //         Endian::Big => self.to_big(),
//     //         Endian::Little => self.to_little(),
//     //     }
//     // }
// }

// trait OrderedFromBytes0<const SRC_LEN: usize, const DST_LEN: usize>:
//     FromBytes<Bytes = [u8; DST_LEN]>
// where
//     [u8; DST_LEN]: Default,
// {
//     fn from_ordered_bytes(bytes: &[u8; SRC_LEN], order: [u8; DST_LEN]) -> Self {
//         let mut buf = Self::Bytes::default();
//         for (i, j) in order.iter().enumerate() {
//             buf.as_mut()[i] = bytes[usize::from(*j)];
//         }
//         Self::from_le_bytes(&buf)
//     }
// }

// trait OrderedFromBytes1: FromBytes<Bytes = Self::FileBuf> + FCSRepr
// where
//     Self::FileBuf: AsRef<[u8]> + AsMut<[u8]> + Default,
//     Self::ByteOrd: AsRef<[u8]>,
// {
//     fn from_ordered_bytes(bytes: &Self::FileBuf, order: Self::ByteOrd) -> Self {
//         let mut buf = Self::Bytes::default();
//         for (i, j) in order.as_ref().iter().enumerate() {
//             buf.as_mut()[i] = bytes.as_ref()[usize::from(*j)];
//         }
//         Self::from_le_bytes(&buf)
//     }
// }

// /// Methods for reading numbers which may be in arbitrary byte orders.
// trait OrderedFromBytes<const OLEN: usize>: NumProps {
//     fn h_write_from_ordered<W: Write>(
//         &self,
//         h: &mut BufWriter<W>,
//         order: [u8; OLEN],
//     ) -> io::Result<()> {
//         let tmp = Self::to_little(&self);
//         let mut buf = [0; OLEN];
//         for (i, j) in order.iter().enumerate() {
//             buf[usize::from(*j)] = tmp.as_ref()[i];
//         }
//         h.write_all(buf.as_ref())
//     }
// }

// /// Methods for reading/writing integers (1-8 bytes) from FCS files.
// trait UnalignedIntFromBytes<const SRC_LEN: usize, const DST_LEN: usize>:
//     FromBytes<Bytes = [u8; DST_LEN]>
// where
//     [u8; DST_LEN]: Default,
// {
//     fn from_unaligned_be_bytes(bytes: &[u8; SRC_LEN]) -> Self {
//         let mut buf = Self::Bytes::default();
//         let b = DST_LEN - SRC_LEN;
//         buf.as_mut()[b..].copy_from_slice(bytes);
//         Self::from_be_bytes(&buf)
//     }

//     fn from_unaligned_le_bytes(bytes: &[u8; SRC_LEN]) -> Self {
//         let mut buf = Self::Bytes::default();
//         buf.as_mut()[..SRC_LEN].copy_from_slice(bytes);
//         Self::from_le_bytes(&buf)
//     }

//     fn from_unaligned_ordered_bytes(bytes: &[u8; SRC_LEN], order: [u8; SRC_LEN]) -> Self {
//         let mut buf = Self::Bytes::default();
//         for (i, j) in order.iter().enumerate() {
//             buf.as_mut()[i] = bytes[usize::from(*j)];
//         }
//         Self::from_le_bytes(&buf)
//     }
// }

// /// Methods for reading/writing integers (1-8 bytes) from FCS files.
// trait IntFromBytes<const INTLEN: usize>: NumProps + OrderedFromBytes<INTLEN> {
//     // fn slice_unaligned_big(bytes: &[u8], index: usize) -> Self {
//     //     if Self::LEN == INTLEN {
//     //         Self::slice_from_big(bytes, index)
//     //     } else {
//     //         let tmp = &bytes[index..index + INTLEN];
//     //         let mut buf = Self::BUF::default();
//     //         let b = Self::LEN - INTLEN;
//     //         buf.as_mut()[b..].copy_from_slice(tmp);
//     //         Self::from_big(&buf)
//     //     }
//     // }

//     // fn slice_unaligned_little(bytes: &[u8], index: usize) -> Self {
//     //     if Self::LEN == INTLEN {
//     //         Self::slice_from_little(bytes, index)
//     //     } else {
//     //         let tmp = &bytes[index..index + INTLEN];
//     //         let mut buf = Self::BUF::default();
//     //         buf.as_mut()[..INTLEN].copy_from_slice(tmp);
//     //         Self::from_little(&buf)
//     //     }
//     // }

//     fn h_write_endian<W: Write>(&self, h: &mut BufWriter<W>, endian: Endian) -> io::Result<()> {
//         let mut buf = [0; INTLEN];
//         let (start, end, tmp) = if endian == Endian::Big {
//             ((Self::LEN - INTLEN), Self::LEN, Self::to_big(&self))
//         } else {
//             (0, INTLEN, Self::to_little(self))
//         };
//         buf[..].copy_from_slice(&tmp.as_ref()[start..end]);
//         h.write_all(&buf)
//     }

//     fn h_write_ordered<W: Write>(
//         self,
//         h: &mut BufWriter<W>,
//         byteord: ArrayByteOrd<[u8; INTLEN]>,
//     ) -> io::Result<()> {
//         match byteord {
//             ArrayByteOrd::Endian(e) => self.h_write_endian(h, e),
//             ArrayByteOrd::Order(o) => self.h_write_from_ordered(h, o),
//         }
//     }
// }

// /// Methods for reading/writing floats (32 and 64 bit) from FCS files.
// trait FloatFromBytes<const LEN: usize>: NumProps + OrderedFromBytes<LEN> {
//     fn h_write_endian<W: Write>(&self, h: &mut BufWriter<W>, endian: Endian) -> io::Result<()> {
//         let buf = Self::to_endian(&self, endian);
//         h.write_all(buf.as_ref())
//     }

//     fn h_write_ordered<W: Write>(
//         &self,
//         h: &mut BufWriter<W>,
//         byteord: ArrayByteOrd<[u8; LEN]>,
//     ) -> io::Result<()> {
//         match byteord {
//             ArrayByteOrd::Endian(endian) => self.h_write_endian(h, endian),
//             ArrayByteOrd::Order(order) => self.h_write_from_ordered(h, order),
//         }
//     }
// }

macro_rules! impl_any_uint {
    ($var:ident, $bitmask:path) => {
        impl From<$bitmask> for AnyNullBitmask {
            fn from(value: $bitmask) -> Self {
                Self::$var(value)
            }
        }

        // impl<'a> From<UintColumnWriter<'a, $bitmask>> for AnyWriterBitmask<'a> {
        //     fn from(value: UintColumnWriter<'a, $bitmask>) -> Self {
        //         Self::$var(value)
        //     }
        // }

        impl TryFrom<AnyNullBitmask> for $bitmask {
            type Error = UintToUintError;
            fn try_from(value: AnyNullBitmask) -> Result<Self, Self::Error> {
                let w = value.nbytes();
                if let AnyUint::$var(x) = value {
                    Ok(x)
                } else {
                    let b = <<Self as HasNativeType>::Native as FCSRepr>::FILE_BYTES;
                    Err(UintToUintError::new(w, b.into()))
                }
            }
        }

        impl TryFrom<MixedRange> for $bitmask {
            type Error = MixedToOrderedUintError;
            fn try_from(value: MixedRange) -> Result<Self, Self::Error> {
                let w = value.nbytes();
                if let AnyDatatype::Uint(x) = value {
                    if let AnyUint::$var(y) = x {
                        Ok(y)
                    } else {
                        let b = <<Self as HasNativeType>::Native as FCSRepr>::FILE_BYTES;
                        Err(UintToUintError::new(w, b.into()).into())
                    }
                } else {
                    let dest_type = value.as_alpha_num_type();
                    Err(MixedToNonMixedError::new(dest_type, value).into())
                }
            }
        }
    };
}

impl_any_uint!(Uint08, Bitmask08);
impl_any_uint!(Uint16, Bitmask16);
impl_any_uint!(Uint24, Bitmask24);
impl_any_uint!(Uint32, Bitmask32);
impl_any_uint!(Uint40, Bitmask40);
impl_any_uint!(Uint48, Bitmask48);
impl_any_uint!(Uint56, Bitmask56);
impl_any_uint!(Uint64, Bitmask64);

impl<C08, C16, C24, C32, C40, C48, C56, C64> From<&AnyUint<C08, C16, C24, C32, C40, C48, C56, C64>>
    for PrivBytes
{
    fn from(value: &AnyUint<C08, C16, C24, C32, C40, C48, C56, C64>) -> Self {
        match value {
            AnyUint::Uint08(_) => Self::B1,
            AnyUint::Uint16(_) => Self::B2,
            AnyUint::Uint24(_) => Self::B3,
            AnyUint::Uint32(_) => Self::B4,
            AnyUint::Uint40(_) => Self::B5,
            AnyUint::Uint48(_) => Self::B6,
            AnyUint::Uint56(_) => Self::B7,
            AnyUint::Uint64(_) => Self::B8,
        }
    }
}

impl From<FixedAsciiRange> for MixedRange {
    fn from(value: FixedAsciiRange) -> Self {
        Self::Ascii(value)
    }
}

impl From<DelimAsciiRange> for MixedRange {
    fn from(value: DelimAsciiRange) -> Self {
        // this will automatically make any delimited ASCII layout a fixed
        // layout if we go to mixed, which seems sane if not an exceedingly rare
        // use case.
        Self::Ascii(value.into())
    }
}

impl From<AnyNullBitmask> for MixedRange {
    fn from(value: AnyNullBitmask) -> Self {
        Self::Uint(value)
    }
}

impl<T> From<Bitmask<T>> for MixedRange
where
    AnyNullBitmask: From<Bitmask<T>>,
{
    fn from(value: Bitmask<T>) -> Self {
        Self::Uint(value.into())
    }
}

impl From<F32Range> for MixedRange {
    fn from(value: F32Range) -> Self {
        Self::F32(value)
    }
}

impl From<F64Range> for MixedRange {
    fn from(value: F64Range) -> Self {
        Self::F64(value)
    }
}

// impl<'a> From<ColumnWriter<'a, AsciiRange, u64, NoByteOrd<false>>> for WriterMixedType<'a> {
//     fn from(value: ColumnWriter<'a, AsciiRange, u64, NoByteOrd<false>>) -> Self {
//         Self::Ascii(value)
//     }
// }

// impl<'a> From<AnyWriterBitmask<'a>> for WriterMixedType<'a> {
//     fn from(value: AnyWriterBitmask<'a>) -> Self {
//         Self::Uint(value)
//     }
// }

// impl<'a> From<ColumnWriter<'a, F32Range, f32, Endian>> for WriterMixedType<'a> {
//     fn from(value: ColumnWriter<'a, F32Range, f32, Endian>) -> Self {
//         Self::F32(value)
//     }
// }

// impl<'a> From<ColumnWriter<'a, F64Range, f64, Endian>> for WriterMixedType<'a> {
//     fn from(value: ColumnWriter<'a, F64Range, f64, Endian>) -> Self {
//         Self::F64(value)
//     }
// }

impl IsNumType for Nothing<NumType> {
    fn lookup_datatype(
        _: &mut StdKeywords,
        _: &mut NonStdKeywords,
        _: MeasIndex,
        _: &ReadDataKeywordsConfig,
    ) -> DeferredWarningAndError<Self, OptIndexedKeyError<NumType>, OptIndexedKeyError<NumType>>
    {
        LogResult::new_ok(Self::default())
    }

    fn lookup_datatype_ro(
        _: &StdKeywords,
        _: MeasIndex,
        _: &ReadDataKeywordsConfig,
    ) -> DeferredWarningAndError<Self, OptIndexedKeyError<NumType>, OptIndexedKeyError<NumType>>
    {
        LogResult::new_ok(Self::default())
    }
}

impl IsNumType for Option<NumType> {
    fn lookup_datatype(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        i: MeasIndex,
        conf: &ReadDataKeywordsConfig,
    ) -> DeferredWarningAndError<Self, OptIndexedKeyError<NumType>, OptIndexedKeyError<NumType>>
    {
        NumType::remove_or_drop_meas_opt(std, nonstd, i, conf).switchable_into_commutative()
    }

    fn lookup_datatype_ro(
        kws: &StdKeywords,
        i: MeasIndex,
        conf: &ReadDataKeywordsConfig,
    ) -> DeferredWarningAndError<Self, OptIndexedKeyError<NumType>, OptIndexedKeyError<NumType>>
    {
        NumType::get_or_ignore_meas_opt(kws, i, conf).switchable_into_commutative()
    }
}

impl IsTot for Option<Tot> {}
impl IsTot for Identity<Tot> {}

impl From<&MixedRange> for Range {
    fn from(value: &MixedRange) -> Self {
        match_any_mixed!(value, x, { x.into() })
    }
}

impl From<&AnyNullBitmask> for Range {
    fn from(value: &AnyNullBitmask) -> Self {
        match_any_uint!(value, AnyNullBitmask, x, { x.into() })
    }
}

impl<T: Clone> From<&FloatRange<T>> for Range {
    fn from(value: &FloatRange<T>) -> Self {
        value.range.clone().into()
    }
}

macro_rules! mixed_to_inner {
    ($inner:ident, $var:ident) => {
        impl TryFrom<MixedRange> for $inner {
            type Error = MixedToNonMixedError;
            fn try_from(value: MixedRange) -> Result<Self, Self::Error> {
                let dest_type = value.as_alpha_num_type();
                if let AnyDatatype::$var(x) = value {
                    Ok(x)
                } else {
                    Err(MixedToNonMixedError::new(dest_type, value))
                }
            }
        }
    };
}

mixed_to_inner!(FixedAsciiRange, Ascii);
mixed_to_inner!(AnyNullBitmask, Uint);
mixed_to_inner!(F32Range, F32);
mixed_to_inner!(F64Range, F64);

// impl<T, const LEN: usize> Castable for Bitmask<T, LEN>
// where
//     Self: HasNativeType<Native = T>,
//     T: Copy + Ord + IsFCSDataType,
//     u64: From<T>,
// {
//     fn with_cast(&self, x: CastResult<T>) -> (T, Option<AnyLossError>) {
//         let (trunc, y) = self.apply(x.new);
//         let t = trunc
//             .map(LossError::Other)
//             .or(x.as_err().map(LossError::Cast))
//             .map(AnyLossError::Int);
//         (y, t)
//     }
// }

// impl<T, const LEN: usize> Castable for FloatRange<T, LEN>
// where
//     Self: HasNativeType<Native = T>,
//     T: Copy + IsFCSDataType,
// {
//     fn with_cast(&self, x: CastResult<T>) -> (T, Option<AnyLossError>) {
//         let t = x.as_err().map(LossError::Cast).map(AnyLossError::Float);
//         (x.new, t)
//     }
// }

// impl Castable for AsciiRange {
//     fn with_cast(&self, x: CastResult<Self::Native>) -> (Self::Native, Option<AnyLossError>) {
//         let t = x.as_err().map(LossError::Cast).map(AnyLossError::Ascii);
//         (x.new, t)
//     }
// }

// impl<T, const LEN: usize> NativeWritable<Endian> for Bitmask<T, LEN>
// where
//     Self: HasNativeType<Native = T>,
//     T: Ord + Copy + IntFromBytes<LEN> + IsFCSDataType,
//     u64: From<T>,
// {
//     fn h_write<W: Write>(
//         &self,
//         h: &mut BufWriter<W>,
//         x: CastResult<T>,
//         byte_layout: Endian,
//     ) -> io::Result<Option<AnyLossError>> {
//         let (y, trunc) = self.with_cast(x);
//         y.h_write_endian(h, byte_layout)?;
//         Ok(trunc)
//     }
// }

// impl<T, const LEN: usize> NativeWritable<ArrayByteOrd<[u8; LEN]>> for Bitmask<T, LEN>
// where
//     Self: HasNativeType<Native = T>,
//     T: Ord + Copy + IntFromBytes<LEN> + IsFCSDataType,
//     u64: From<T>,
// {
//     fn h_write<W: Write>(
//         &self,
//         h: &mut BufWriter<W>,
//         x: CastResult<T>,
//         byte_layout: ArrayByteOrd<[u8; LEN]>,
//     ) -> io::Result<Option<AnyLossError>> {
//         let (y, trunc) = self.with_cast(x);
//         y.h_write_ordered(h, byte_layout)?;
//         Ok(trunc)
//     }
// }

// impl<T, const LEN: usize> NativeWritable<Endian> for FloatRange<T, LEN>
// where
//     Self: HasNativeType<Native = T>,
//     T: Copy + FloatFromBytes<LEN> + IsFCSDataType,
// {
//     fn h_write<W: Write>(
//         &self,
//         h: &mut BufWriter<W>,
//         x: CastResult<T>,
//         byte_layout: Endian,
//     ) -> io::Result<Option<AnyLossError>> {
//         let (y, trunc) = self.with_cast(x);
//         y.h_write_endian(h, byte_layout)?;
//         Ok(trunc)
//     }
// }

// impl<T, const LEN: usize> NativeWritable<ArrayByteOrd<[u8; LEN]>> for FloatRange<T, LEN>
// where
//     Self: HasNativeType<Native = T>,
//     T: Copy + FloatFromBytes<LEN> + IsFCSDataType,
// {
//     fn h_write<W: Write>(
//         &self,
//         h: &mut BufWriter<W>,
//         x: CastResult<T>,
//         byte_layout: ArrayByteOrd<[u8; LEN]>,
//     ) -> io::Result<Option<AnyLossError>> {
//         let (y, trunc) = self.with_cast(x);
//         y.h_write_ordered(h, byte_layout)?;
//         Ok(trunc)
//     }
// }

// impl<const ORD: bool> NativeWritable<NoByteOrd<ORD>> for AsciiRange {
//     fn h_write<W: Write>(
//         &self,
//         h: &mut BufWriter<W>,
//         x: CastResult<Self::Native>,
//         _: NoByteOrd<ORD>,
//     ) -> io::Result<Option<AnyLossError>> {
//         let (value, trunc) = self.with_cast(x);
//         let str_value = value.to_string();
//         let width: usize = u8::from(self.chars()).into();
//         let err = if str_value.len() > width {
//             // if string is greater than allocated chars, only write a fraction
//             // starting from the left
//             let offset = str_value.len() - width;
//             h.write_all(&str_value.as_bytes()[offset..])?;
//             Some(LossError::Other(AsciiLossError(self.chars())))
//         } else {
//             // if string less than allocated chars, pad left side with zero before
//             // writing number
//             for _ in 0..(width - str_value.len()) {
//                 h.write_all(&[48])?;
//             }
//             h.write_all(str_value.as_bytes())?;
//             None
//         };
//         Ok(err.map(AnyLossError::Ascii).or(trunc))
//     }
// }

// impl<'a, C, S> IntoWriter<'a, S> for C
// where
//     C: ToNativeWriter,
//     ColumnWriter<'a, C, C::Native, S>: Writable<'a, S>,
//     C::Native: Default + Copy + AllFCSCast,
//     AnySource<'a, C::Native>: From<FCSColIter<'a, u8, C::Native>>
//         + From<FCSColIter<'a, u16, C::Native>>
//         + From<FCSColIter<'a, u32, C::Native>>
//         + From<FCSColIter<'a, u64, C::Native>>
//         + From<FCSColIter<'a, f32, C::Native>>
//         + From<FCSColIter<'a, f64, C::Native>>,
//     AnyLossError: From<LossError<C::Error>>,
// {
//     type Target = ColumnWriter<'a, C, C::Native, S>;

//     fn into_writer(self, col: &'a AnyFCSColumn) -> Self::Target {
//         self.into_native_writer(col)
//     }

//     fn check_writer(&self, col: &'a AnyFCSColumn) -> Result<(), AnyLossError> {
//         self.check_native_writer(col).map_err(Into::into)
//     }
// }

// impl<'a> IntoWriter<'a, Endian> for AnyNullBitmask {
//     type Target = AnyWriterBitmask<'a>;

//     fn into_writer(self, col: &'a AnyFCSColumn) -> Self::Target {
//         match_any_uint!(self, Self, c, { c.into_native_writer(col).into() })
//     }

//     fn check_writer(&self, col: &'a AnyFCSColumn) -> Result<(), AnyLossError> {
//         match_any_uint!(self, Self, c, {
//             c.check_native_writer(col).map_err(Into::into)
//         })
//     }
// }

// impl<'a> IntoWriter<'a, Endian> for MixedRange {
//     type Target = WriterMixedType<'a>;

//     fn into_writer(self, col: &'a AnyFCSColumn) -> Self::Target {
//         match_any_mixed!(self, c, { c.into_writer(col).into() })
//     }

//     fn check_writer(&self, col: &'a AnyFCSColumn) -> Result<(), AnyLossError> {
//         match self {
//             Self::Ascii(c) => IntoWriter::<NoByteOrd3_1>::check_writer(c, col),
//             Self::Uint(c) => c.check_writer(col),
//             Self::F32(c) => c.check_native_writer(col).map_err(Into::into),
//             Self::F64(c) => c.check_native_writer(col).map_err(Into::into),
//         }
//     }
// }

// impl<'a, C, T, S> Writable<'a, S> for ColumnWriter<'a, C, T, S>
// where
//     C: NativeWritable<S> + HasNativeType<Native = T> + ToNativeWriter + Castable,
//     AnyFCSColumn: From<FCSColumn<T>>,
// {
//     fn h_write<W: Write>(&mut self, h: &mut BufWriter<W>, byte_layout: S) -> io::Result<()> {
//         let x = self.data.next().unwrap();
//         // TODO this might not be optimal since this loss storage logic will
//         // (probably) fire for every written value even if we don't use it
//         let loss = self.column_type.h_write(h, x, byte_layout)?;
//         self.loss = mem::take(&mut self.loss).or(loss);
//         Ok(())
//     }

//     fn truncate(self, skip_conv_check: bool) -> (AnyFCSColumn, Option<AnyLossError>) {
//         let mut warn = None;
//         // TODO not optimal at all
//         let mut xs = vec![];
//         for x in self.data {
//             let (y, w) = self.column_type.with_cast(x);
//             if skip_conv_check {
//                 warn = mem::take(&mut warn).or(w);
//             }
//             xs.push(y);
//         }
//         (FCSColumn::from(xs).into(), warn)
//     }

//     fn into_err(self, i: MeasIndex) -> Option<IndexedLossError> {
//         self.into_err(i)
//     }
// }

// impl<'a> Writable<'a, Endian> for WriterMixedType<'a> {
//     fn h_write<W: Write>(&mut self, h: &mut BufWriter<W>, byte_layout: Endian) -> io::Result<()> {
//         match self {
//             Self::Ascii(c) => c.h_write(h, NoByteOrd),
//             Self::Uint(c) => c.h_write(h, byte_layout),
//             Self::F32(c) => c.h_write(h, byte_layout),
//             Self::F64(c) => c.h_write(h, byte_layout),
//         }
//     }

//     fn truncate(self, skip_conv_check: bool) -> (AnyFCSColumn, Option<AnyLossError>) {
//         match_any_mixed!(self, x, { x.truncate(skip_conv_check) })
//     }

//     fn into_err(self, i: MeasIndex) -> Option<IndexedLossError> {
//         match_any_mixed!(self, x, { x.into_err(i) })
//     }
// }

// impl<'a> Writable<'a, Endian> for AnyWriterBitmask<'a> {
//     fn h_write<W: Write>(&mut self, h: &mut BufWriter<W>, byte_layout: Endian) -> io::Result<()> {
//         match_any_uint!(self, Self, c, { c.h_write(h, byte_layout) })
//     }

//     fn truncate(self, skip_conv_check: bool) -> (AnyFCSColumn, Option<AnyLossError>) {
//         match_any_uint!(self, Self, x, { x.truncate(skip_conv_check) })
//     }

//     fn into_err(self, i: MeasIndex) -> Option<IndexedLossError> {
//         match_any_uint!(self, Self, x, { x.into_err(i) })
//     }
// }

// impl<T, const LEN: usize> ToNativeWriter for Bitmask<T, LEN>
// where
//     Self: HasNativeType<Native = T>,
//     u64: From<T>,
//     T: Ord + Copy,
// {
//     type Error = BitmaskLossError;

//     fn check_other_loss(&self, x: T) -> Option<Self::Error> {
//         (x > self.bitmask()).then(|| BitmaskLossError(u64::from(self.bitmask())))
//     }
// }

// impl<T, const LEN: usize> ToNativeWriter for FloatRange<T, LEN>
// where
//     Self: HasNativeType<Native = T>,
// {
//     type Error = Infallible;

//     fn check_other_loss(&self, _: T) -> Option<Self::Error> {
//         None
//     }
// }

// impl ToNativeWriter for AsciiRange {
//     type Error = AsciiLossError;

//     fn check_other_loss(&self, x: Self::Native) -> Option<Self::Error>
//     where
//         u64: From<Self::Native>,
//     {
//         (Chars::from_u64(x) > self.chars()).then(|| AsciiLossError(self.chars()))
//     }
// }

// /// A wrapper for any of the 6 source types that can be written.
// ///
// /// Each inner type is an iterator from a different source type which emit
// /// the given target type.
// enum AnySource<'a, TargetType> {
//     FromU08(FCSColIter<'a, u8, TargetType>),
//     FromU16(FCSColIter<'a, u16, TargetType>),
//     FromU32(FCSColIter<'a, u32, TargetType>),
//     FromU64(FCSColIter<'a, u64, TargetType>),
//     FromF32(FCSColIter<'a, f32, TargetType>),
//     FromF64(FCSColIter<'a, f64, TargetType>),
// }

// impl<'a, T> AnySource<'a, T> {
//     fn new(c: &'a AnyFCSColumn) -> Self
//     where
//         T: AllFCSCast,
//         Self: From<FCSColIter<'a, u8, T>>
//             + From<FCSColIter<'a, u16, T>>
//             + From<FCSColIter<'a, u32, T>>
//             + From<FCSColIter<'a, u64, T>>
//             + From<FCSColIter<'a, f32, T>>
//             + From<FCSColIter<'a, f64, T>>,
//     {
//         match_many_to_one!(c, AnyFCSColumn, [U08, U16, U32, U64, F32, F64], xs, {
//             IsFCSDataType::as_col_iter(xs).into()
//         })
//     }
// }

// impl<T> Iterator for AnySource<'_, T> {
//     type Item = CastResult<T>;

//     fn next(&mut self) -> Option<Self::Item> {
//         match_many_to_one!(
//             self,
//             Self,
//             [FromU08, FromU16, FromU32, FromU64, FromF32, FromF64],
//             c,
//             { c.next() }
//         )
//     }
// }

fn is_ascii_delim(x: u8) -> bool {
    // tab, newline, carriage return, space, or comma
    x == 9 || x == 10 || x == 13 || x == 32 || x == 44
}

// impl<D> EndianLayout<AnyNullBitmask, D> {
//     // pub(crate) fn endian_uint_try_new(
//     //     cs: Vec<ColumnLayoutValues<D>>,
//     //     e: Endian,
//     //     flag: DisallowRangeTrunc,
//     // ) -> WarningsAndErrorsResult<NewLayout<Self>, (), IndexedBitmaskError, NewUintTypeError>
//     // where
//     //     D: IsNumType,
//     // {
//     //     Self::try_new(cs, e, |i, c| {
//     //         AnyUint::from_width_and_range(c.width, c.range, i, flag).repack_errors()
//     //     })
//     // }

//     // pub(crate) fn uint_try_into_ordered<T>(
//     //     self,
//     // ) -> ErrorsResult<AnyOrderedUintLayout<T>, (), UintEndianToOrderedLayoutError> {
//     //     let mut it = self.columns.into_iter().peekable();
//     //     if let Some(head) = it.next() {
//     //         head.try_into_one_size(it, self.byte_layout, 1)
//     //             .map_errors(|(index, error)| IndexedError::new(index, error).into())
//     //     } else {
//     //         let b: ArrayByteOrd<[u8; 4]> = self.byte_layout.into();
//     //         LogResult::new_ok(FixedLayout::new(vec![], b).into())
//     //     }
//     // }
// }

impl<D> AnyEndianUintLayout<D> {
    pub(crate) fn try_new(
        cs: Vec<ColumnLayoutValues<D>>,
        e: Endian,
        flag: DisallowRangeTrunc,
    ) -> WarningsAndErrorsResult<NewLayout<Self>, (), IndexedBitmaskError, NewUintTypeError>
    where
        D: IsNumType,
    {
        DataLayout::try_new(cs, e, |i, c| {
            AnyUint::from_width_and_range(c.width, c.range, i, flag).repack_errors()
        })
        .map_ok_value(|res| {
            res.fmap_once(|c| {
                let mut wrapped = Self::Multi(c);
                wrapped.normalize();
                wrapped
            })
        })
    }

    #[must_use]
    pub fn phantom_into<D1>(self) -> AnyEndianUintLayout<D1> {
        match_many_to_one!(self, Self, [Single, Multi], x, { x.phantom_into().into() })
    }
}

impl<D> EndianLayout<MixedRange, D> {
    // pub(crate) fn try_into_ordered<T>(
    //     self,
    // ) -> ErrorsResult<AnyOrderedLayout<T>, (), MixedToOrderedLayoutError> {
    //     macro_rules! from_columns {
    //         ($i:expr) => {
    //             $i.enumerate()
    //                 .map(|(i, c)| {
    //                     c.try_into()
    //                         .map_err(|e| IndexedError::new(i + 1, e))
    //                         .map_err(MixedToNonMixedLayoutError)
    //                         .map_err(MixedToOrderedLayoutError::from)
    //                         .into_log()
    //                 })
    //                 .sequence_commutative()
    //         };
    //     }

    //     let mut it = self.columns.into_iter().peekable();
    //     if let Some(head) = it.next() {
    //         let endian = self.byte_layout;
    //         match head {
    //             AnyDatatype::Uint(x) => x
    //                 .try_into_one_size(it, endian, 1)
    //                 .map_ok_value(AnyOrderedLayout::Integer)
    //                 .map_errors(|(index, error)| error.into_col_error(index)),
    //             AnyDatatype::Ascii(x) => from_columns!(it)
    //                 .map_ok_value(|xs| FixedLayout::new1(x, xs, NoByteOrd))
    //                 .map_ok_value(AnyAsciiLayout::from)
    //                 .map_ok_value(AnyOrderedLayout::Ascii),
    //             AnyDatatype::F32(x) => from_columns!(it)
    //                 .map_ok_value(|xs| FixedLayout::new1(x, xs, endian.into()))
    //                 .map_ok_value(AnyOrderedLayout::F32),
    //             AnyDatatype::F64(x) => from_columns!(it)
    //                 .map_ok_value(|xs| FixedLayout::new1(x, xs, endian.into()))
    //                 .map_ok_value(AnyOrderedLayout::F64),
    //         }
    //     } else {
    //         let b: ArrayByteOrd<[u8; 4]> = self.byte_layout.into();
    //         LogResult::new_ok(AnyOrderedLayout::F32(FixedLayout::new(vec![], b)))
    //     }
    // }

    // pub(crate) fn try_into_non_mixed(
    //     self,
    // ) -> ErrorsResult<NonMixedEndianLayout<Nothing<NumType>>, (), MixedToNonMixedLayoutError> {
    //     let mut it = self.columns.into_iter().peekable().enumerate();
    //     if let Some((_, c0)) = it.next() {
    //         macro_rules! from_iter {
    //             ($iter:expr, $head:expr, $byte_layout:expr) => {
    //                 $iter
    //                     .map(|(i, c)| c.try_into().map_err(|e| (i, e)).into_log())
    //                     .sequence_commutative()
    //                     .map_ok_value(|xs| FixedLayout::new1($head, xs, $byte_layout))
    //                     .map_ok_value(NonMixedEndianLayout::from)
    //             };
    //         }

    //         let byte_layout = self.byte_layout;
    //         match c0 {
    //             AnyDatatype::Ascii(x) => it
    //                 .map(|(i, c)| c.try_into().map_err(|e| (i, e)).into_log::<_, _, Vec<_>>())
    //                 .sequence_commutative()
    //                 .map_ok_value(|xs| FixedLayout::new1(x, xs, NoByteOrd))
    //                 .map_ok_value(|l| AnyAsciiLayout::Fixed(l).into()),
    //             AnyDatatype::Uint(x) => from_iter!(it, x, byte_layout),
    //             AnyDatatype::F32(x) => from_iter!(it, x, byte_layout),
    //             AnyDatatype::F64(x) => from_iter!(it, x, byte_layout),
    //         }
    //         .map_errors(|(i, error)| IndexedError::new(i + 1, error).into())
    //     } else {
    //         let l = FixedLayout::new(vec![], self.byte_layout);
    //         LogResult::new_ok(NonMixedEndianLayout::Uint(l))
    //     }
    // }
}

// // NOTE num_traits has this but it doesn't have a nice way to init a default
// // buffer, this will probably be easier and cleaner anyways once we can use
// // const expressions
// macro_rules! impl_num_props {
//     ($size:expr, $t:ty) => {
//         impl NumProps for $t {
//             const LEN: usize = $size;
//             type BUF = [u8; $size];

//             fn to_big(&self) -> Self::BUF {
//                 self.to_be_bytes()
//             }

//             fn to_little(&self) -> Self::BUF {
//                 self.to_le_bytes()
//             }

//             fn from_big(buf: &Self::BUF) -> Self {
//                 <$t>::from_be_bytes(*buf)
//             }

//             fn from_little(buf: &Self::BUF) -> Self {
//                 <$t>::from_le_bytes(*buf)
//             }
//         }
//     };
// }

// // TODO silly
// macro_rules! impl_num_props0 {
//     ($size:expr, $t:ty) => {
//         impl NumProps for $t {
//             const LEN: usize = $size;
//             type BUF = [u8; $size];

//             fn to_big(&self) -> Self::BUF {
//                 self.to_be_bytes()
//             }

//             fn to_little(&self) -> Self::BUF {
//                 self.to_le_bytes()
//             }

//             fn from_big(buf: &Self::BUF) -> Self {
//                 <$t>::from_be_bytes(buf)
//             }

//             fn from_little(buf: &Self::BUF) -> Self {
//                 <$t>::from_le_bytes(buf)
//             }
//         }
//     };
// }

// impl_num_props!(1, u8);
// impl_num_props!(2, u16);
// impl_num_props0!(3, U24);
// impl_num_props!(4, u32);
// impl_num_props0!(5, U40);
// impl_num_props0!(6, U48);
// impl_num_props0!(7, U56);
// impl_num_props!(8, u64);
// impl_num_props!(4, f32);
// impl_num_props!(8, f64);

// impl OrderedFromBytes<1> for u8 {}
// impl OrderedFromBytes<2> for u16 {}
// impl OrderedFromBytes<3> for u32 {}
// impl OrderedFromBytes<4> for u32 {}
// impl OrderedFromBytes<5> for u64 {}
// impl OrderedFromBytes<6> for u64 {}
// impl OrderedFromBytes<7> for u64 {}
// impl OrderedFromBytes<8> for u64 {}
// impl OrderedFromBytes<4> for f32 {}
// impl OrderedFromBytes<8> for f64 {}

// impl OrderedFromBytes0<1, 1> for u8 {}
// impl OrderedFromBytes0<2, 2> for u16 {}
// impl OrderedFromBytes0<3, 4> for u32 {}
// impl OrderedFromBytes0<4, 4> for u32 {}
// impl OrderedFromBytes0<5, 8> for u64 {}
// impl OrderedFromBytes0<6, 8> for u64 {}
// impl OrderedFromBytes0<7, 8> for u64 {}
// impl OrderedFromBytes0<8, 8> for u64 {}
// impl OrderedFromBytes0<4, 4> for f32 {}
// impl OrderedFromBytes0<8, 8> for f64 {}

// impl FloatFromBytes<4> for f32 {}
// impl FloatFromBytes<8> for f64 {}

// impl IntFromBytes<1> for u8 {}
// impl IntFromBytes<2> for u16 {}
// impl IntFromBytes<3> for u32 {}
// impl IntFromBytes<4> for u32 {}
// impl IntFromBytes<5> for u64 {}
// impl IntFromBytes<6> for u64 {}
// impl IntFromBytes<7> for u64 {}
// impl IntFromBytes<8> for u64 {}

// impl IntFromBytes<3> for U24 {}
// impl IntFromBytes<5> for U40 {}
// impl IntFromBytes<6> for U48 {}
// impl IntFromBytes<7> for U56 {}

// impl OrderedFromBytes<3> for U24 {}
// impl OrderedFromBytes<5> for U40 {}
// impl OrderedFromBytes<6> for U48 {}
// impl OrderedFromBytes<7> for U56 {}

// impl UnalignedIntFromBytes<3, 4> for u32 {}
// impl UnalignedIntFromBytes<5, 8> for u64 {}
// impl UnalignedIntFromBytes<6, 8> for u64 {}
// impl UnalignedIntFromBytes<7, 8> for u64 {}

impl<T> FloatRange<T> {
    /// Make new float range from $PnB and $PnR values.
    ///
    /// Will return an error if $PnB is the incorrect size.
    pub(crate) fn from_width_and_range(
        width: Width,
        range: Range,
        i: MeasIndex,
        flag: DisallowRangeTrunc,
    ) -> WarningsAndErrorResult<ConvertedRange<Self>, (), IndexedFloatRangeError, FloatWidthError>
    where
        FloatDecimal<T>: TryFrom<BigDecimal, Error = DecimalToFloatError>,
        T: HasFloatBounds + FCSRepr,
    {
        PrivBytes::try_from(width)
            .map_err(|e| IndexedError::new(i, e))
            .map_err(IndexedWidthToBytesError)
            .into_log::<Vec<_>, Vec<_>, Nothing<_>>()
            .map_errors(FloatWidthError::from)
            .and_then_commutative(|bytes| {
                if usize::from(u8::from(bytes)) == T::file_len() {
                    Self::from_range_switch(range, flag)
                        .set_err_value(())
                        .map_switchable_errors(|e| IndexedError::new(i, e))
                        .map_switchable_errors(IndexedFloatRangeError)
                        .switchable_into_commutative()
                        .map_errors(FloatWidthError::from)
                        .repack_warnings()
                } else {
                    let e = FloatWidthError::from(WrongFloatWidth::new(bytes, T::file_len(), i));
                    LogResult::new_err(e)
                }
            })
    }
}

impl MixedRange {
    fn init_column(&self, nrows: usize) -> MixedVec {
        match self {
            Self::Ascii(b) => MixedVec::Ascii(RangedVec::new(*b, vec![0; nrows])),
            Self::Uint(b) => MixedVec::Uint(b.init_column(nrows)),
            Self::F32(b) => MixedVec::F32(RangedVec::new(b.clone(), vec![0.0; nrows])),
            Self::F64(b) => MixedVec::F64(RangedVec::new(b.clone(), vec![0.0; nrows])),
        }
    }

    /// Make a new mixed range from $PnB and $PnR, and $PnDATATYPE values
    pub(crate) fn from_width_and_range(
        width: Width,
        range: Range,
        datatype: Option<NumType>,
        global_datatype: AlphaNumType,
        i: MeasIndex,
        flag: DisallowRangeTrunc,
    ) -> WarningsAndErrorsResult<ConvertedRange<Self>, (), NewMixedTypeWarning, NewMixedTypeError>
    {
        macro_rules! from {
            ($t:ident, $width:expr, $range:expr, $i:expr, $flag:expr) => {
                $t::from_width_and_range($width, $range, $i, $flag)
                    .map_ok_value(|x| x.fmap_once(Self::from))
                    .map_commutative_warnings(NewMixedTypeWarning::from)
                    .map_errors(NewMixedTypeError::from)
                    .repack_errors()
            };
        }

        match datatype.map_or(global_datatype, AlphaNumType::from) {
            AlphaNumType::Ascii => from!(FixedAsciiRange, width, range, i, flag),
            AlphaNumType::Integer => from!(AnyUint, width, range, i, flag),
            AlphaNumType::Float => from!(F32Range, width, range, i, flag),
            AlphaNumType::Double => from!(F64Range, width, range, i, flag),
        }
    }

    fn as_alpha_num_type(&self) -> AlphaNumType {
        match self {
            Self::Ascii(_) => AlphaNumType::Ascii,
            Self::Uint(_) => AlphaNumType::Integer,
            Self::F32(_) => AlphaNumType::Float,
            Self::F64(_) => AlphaNumType::Double,
        }
    }
}

impl From<BitmaskValue<u64>> for AnyNullBitmask {
    /// Make a new bitmask from a u64.
    ///
    /// The width is determined by the magnitude of the range; the smallest
    /// possible will be used.
    fn from(value: BitmaskValue<u64>) -> Self {
        macro_rules! go {
            ($var:ident, $x:expr) => {{
                let (ret, truncated) = Bitmask::from_u64($x.0);
                debug_assert!(!truncated, "AnyNullBitmask input should never be truncated");
                Self::$var(ret)
            }};
        }
        match PrivBytes::from_u64(value.0) {
            PrivBytes::B1 => go!(Uint08, value),
            PrivBytes::B2 => go!(Uint16, value),
            PrivBytes::B3 => go!(Uint24, value),
            PrivBytes::B4 => go!(Uint32, value),
            PrivBytes::B5 => go!(Uint40, value),
            PrivBytes::B6 => go!(Uint48, value),
            PrivBytes::B7 => go!(Uint56, value),
            PrivBytes::B8 => go!(Uint64, value),
        }
    }
}

impl From<AnyNullBitmask> for BitmaskValue<u64> {
    /// Convert bitmask range (not bitmask itself) to u64.
    fn from(value: AnyNullBitmask) -> Self {
        match_any_uint!(value, AnyNullBitmask, x, { Self(u64::from(x)) })
    }
}

impl AnyNullBitmask {
    fn init_column(&self, nrows: usize) -> AnyUintVec {
        fn default_vec<T: Clone + Default>(n: usize) -> Vec<T> {
            vec![T::default(); n]
        }
        match self {
            Self::Uint08(b) => AnyUintVec::Uint08(RangedVec::new(*b, default_vec(nrows))),
            Self::Uint16(b) => AnyUintVec::Uint16(RangedVec::new(*b, default_vec(nrows))),
            Self::Uint24(b) => AnyUintVec::Uint24(RangedVec::new(*b, default_vec(nrows))),
            Self::Uint32(b) => AnyUintVec::Uint32(RangedVec::new(*b, default_vec(nrows))),
            Self::Uint40(b) => AnyUintVec::Uint40(RangedVec::new(*b, default_vec(nrows))),
            Self::Uint48(b) => AnyUintVec::Uint48(RangedVec::new(*b, default_vec(nrows))),
            Self::Uint56(b) => AnyUintVec::Uint56(RangedVec::new(*b, default_vec(nrows))),
            Self::Uint64(b) => AnyUintVec::Uint64(RangedVec::new(*b, default_vec(nrows))),
        }
    }

    /// Make a new bitmask from $PnB and PnR values.
    ///
    /// Will return an error if $PnB (in bits) cannot be converted into a width
    /// in bytes.
    fn from_width_and_range(
        width: Width,
        range: Range,
        i: MeasIndex,
        flag: DisallowRangeTrunc,
    ) -> WarningsAndErrorResult<ConvertedRange<Self>, (), IndexedBitmaskError, NewUintTypeError>
    {
        width
            .try_into()
            .map_err(|e| IndexedError::new(i, e))
            .map_err(IndexedWidthToBytesError)
            .map_err(NewUintTypeError::from)
            .into_log()
            .and_then_commutative(|bytes| {
                Self::try_new(bytes, range, i, flag)
                    .set_err_value(())
                    .switchable_into_commutative()
                    .map_errors(NewUintTypeError::from)
                    .repack_warnings()
            })
    }

    /// Make a new bitmask with a given width (in bytes) using a float/int.
    fn try_new(
        width: PrivBytes,
        range: Range,
        i: MeasIndex,
        flag: DisallowRangeTrunc,
    ) -> DeferredSwitchableError<ConvertedRange<Self>, DisallowRangeTrunc, IndexedBitmaskError>
    {
        macro_rules! go {
            ($t:ident) => {
                $t::from_range_switch(range, flag).map_deferred_value(FunctorOnce::fmap_into_once)
            };
        }
        let ret = match width {
            PrivBytes::B1 => go!(Bitmask08),
            PrivBytes::B2 => go!(Bitmask16),
            PrivBytes::B3 => go!(Bitmask24),
            PrivBytes::B4 => go!(Bitmask32),
            PrivBytes::B5 => go!(Bitmask40),
            PrivBytes::B6 => go!(Bitmask48),
            PrivBytes::B7 => go!(Bitmask56),
            PrivBytes::B8 => go!(Bitmask64),
        };
        ret.map_switchable_errors(|e| IndexedError::new(i, e))
            .map_switchable_errors(IndexedBitmaskError)
    }

    // pub(crate) fn try_into_one_size<X, E, T>(
    //     self,
    //     tail: impl IntoIterator<Item = X>,
    //     endian: Endian,
    //     starting_index: usize,
    // ) -> ErrorsResult<AnyOrderedUintLayout<T>, (), (MeasIndex, E)>
    // where
    //     Bitmask08: TryFrom<X, Error = E>,
    //     Bitmask16: TryFrom<X, Error = E>,
    //     Bitmask24: TryFrom<X, Error = E>,
    //     Bitmask32: TryFrom<X, Error = E>,
    //     Bitmask40: TryFrom<X, Error = E>,
    //     Bitmask48: TryFrom<X, Error = E>,
    //     Bitmask56: TryFrom<X, Error = E>,
    //     Bitmask64: TryFrom<X, Error = E>,
    // {
    //     match_any_uint!(self, Self, x, {
    //         Bitmask::try_from_many(tail, starting_index)
    //             .map_ok_value(|xs| FixedLayout::new1(x, xs, endian.into()).into())
    //     })
    // }
}

fn ascii_to_uint(buf: &[u8]) -> Result<u64, AsciiToUintError> {
    if buf.is_ascii() {
        // SAFETY: we just checked that all bytes are ASCII
        let s = unsafe { str::from_utf8_unchecked(buf) };
        s.parse().map_err(AsciiToUintError::from)
    } else {
        Err(NotAsciiError(buf.to_vec()).into())
    }
}

impl From<ColumnLayoutValues3_2> for ColumnLayoutValues2_0 {
    fn from(value: ColumnLayoutValues3_2) -> Self {
        Self::new(value.width, value.range, Nothing::default())
    }
}

impl<T, D, const ORD: bool> LayoutDatatype for DelimAsciiLayout<T, D, ORD> {
    fn datatype(&self) -> AlphaNumType {
        AlphaNumType::Ascii
    }

    fn datatypes(&self) -> Vec<AlphaNumType> {
        vec![self.datatype(); self.inner.len()]
    }
}

impl<T, D, const ORD: bool> LayoutKeywords for DelimAsciiLayout<T, D, ORD>
where
    NoByteOrd<ORD>: HasByteOrd,
    for<'a> ReqRootKeyword<'a>: From<SplitKeyword0<<NoByteOrd<ORD> as HasByteOrd>::ByteOrd>>,
{
    fn byteord_keyword(&self) -> ReqRootKeyword<'_> {
        // NOTE BYTEORD is meaningless for delimited ASCII so use a dummy
        let b = <NoByteOrd<ORD> as HasByteOrd>::ByteOrd::from(NoByteOrd);
        ReqRootKeyword::from_value(b)
    }

    fn req_meas_keywords(&self) -> Vec<[ReqMeasKeyword<'_>; 2]> {
        self.inner
            .iter()
            .enumerate()
            .map(|(i, r)| {
                let x = ReqMeasKeyword::from_value(Width::Variable, i);
                let y = ReqMeasKeyword::from_value(Range::from(r), i);
                [x, y]
            })
            .collect()
    }
}

impl<T, D, const ORD: bool> ReadLayoutOps<T> for DelimAsciiLayout<T, D, ORD> {
    fn h_read_df_inner<R: Read>(
        &self,
        h: &mut BufReader<R>,
        tot: T,
        seg: &mut AnyDataSegment,
        conf: &ReadEventsConfig,
    ) -> WarningsAndIOGroupResult<
        DataFrameResult<Self::DfTarget>,
        ReadDataframeWarning,
        ReadDataframeError,
        (),
    >
    where
        T: IsTot,
    {
        macro_rules! go {
            ($x:expr) => {
                $x.map_err(|e| {
                    e.fmap_once(ReadDelimAsciiError::from)
                        .fmap_once(ReadAsciiError::from)
                        .fmap_once(ReadDataframeError::from)
                })
            };
        }
        let rs = &self.inner;
        let nbytes = usize::try_from(seg.len()).expect("DATA length > usize");
        if rs.is_empty() && nbytes > 0 {
            let e = ReadAsciiError::from(ReadDelimAsciiError::from(ReadDelimNoColumnError));
            return LogResult::new_err(IOErrorGroup::new_pure_one(e.into()));
        }
        let res = T::with_tot(
            h,
            tot,
            |h_, t| go!(h_read_delim_with_rows(rs, h_, t, nbytes)),
            |h_| go!(h_read_delim_without_rows(rs, h_, nbytes)),
        );

        res.map_err(IOErrorGroup::from)
            .into_log()
            .and_then_commutative(|mut data| {
                debug_assert!(
                    data.iter().map(Vec::len).unique().count() < 2,
                    "columns must all be same length"
                );
                let mut es = vec![];
                let mut overrange = vec![None; rs.len()];
                let trunc = conf.truncate_event_values;
                let col_iter = data.iter_mut().zip(rs).enumerate();
                if AlphaNumType::Ascii.matches_truncation(trunc) {
                    // truncate values if we configured this behavior
                    for (i, (col, r)) in col_iter {
                        for (rowi, x) in col.iter_mut().enumerate() {
                            if *x > u64::from(*r) {
                                *x = u64::from(*r);
                                if overrange[i].is_none() {
                                    overrange[i] = Some((rowi, true));
                                }
                            }
                        }
                    }
                } else {
                    // otherwise warn/error if value is overrange
                    for (i, (col, r)) in col_iter {
                        for (rowi, x) in col.iter().enumerate() {
                            if *x > u64::from(*r) {
                                es.push(EventOverRangeError::new(rowi, i.into(), r.into()));
                                if overrange[i].is_none() {
                                    overrange[i] = Some((rowi, false));
                                }
                            }
                        }
                    }
                }
                let flag = conf.disallow_over_range;
                SwitchableErrorsResult::new_deferred_switchable_iter3((), es, flag)
                    .switchable_into_commutative()
                    .group()
                    .map_commutative_warnings(ReadDataframeWarning::from)
                    .map_error(ReadDataframeError::from)
                    .map_error(IOErrorGroup::new_pure_one)
                    .map_ok_value(|()| {
                        let cs = data
                            .into_iter()
                            .zip(&self.inner)
                            .map(|(vec, &range)| NativeColumn::from(RangedVec::new(range, vec)));
                        let out = EventsDiagnostics::new(None, None, None, overrange);
                        let df = FFDataFrame::try_new(cs).unwrap();
                        DataFrameResult::new(ColumnGroup::new_ascii(df), out)
                    })
            })
    }
}

fn h_read_delim_with_rows<R: Read>(
    ranges: &[DelimAsciiRange],
    h: &mut BufReader<R>,
    tot: Tot,
    nbytes: usize,
) -> Result<Vec<Vec<u64>>, ImpureError<ReadDelimWithRowsAsciiError>> {
    let mut buf = Vec::new();
    let mut last_was_delim = false;
    let nrows = tot.0;
    let ncols = ranges.len();
    debug_assert!(ncols > 0, "no columns given for ASCII layout");
    // Here we have $TOT so initialize vectors to required length
    let mut data = vec![vec![0; nrows]; ncols];
    let mut row = 0;
    let mut col = 0;
    // Delimiters are tab, newline, carriage return, space, or comma. Any
    // consecutive delimiter counts as one, and delimiters can be mixed.
    macro_rules! go {
        () => {
            data[col][row] = ascii_to_uint(&buf)
                .map_err(ReadDelimWithRowsAsciiError::Parse)
                .map_err(ImpureError::Pure)?;
            if col == ncols - 1 {
                col = 0;
                row += 1;
            } else {
                col += 1;
            }
        };
    }
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
                go!();
                buf.clear();
            }
        } else {
            buf.push(byte);
            last_was_delim = false;
        }
    }
    // The spec isn't clear if the last value should be a delim or
    // not, so flush the buffer if it has anything in it since we
    // only try to parse if we hit a delim above.
    if !buf.is_empty() {
        go!();
    }
    if !(col == 0 && row == nrows) {
        let e = DelimIncompleteError { col, row, nrows };
        let ee = ImpureError::Pure(ReadDelimWithRowsAsciiError::Incomplete(e));
        return Err(ee);
    }
    Ok(data)
}

fn h_read_delim_without_rows<R: Read>(
    ranges: &[DelimAsciiRange],
    h: &mut BufReader<R>,
    nbytes: usize,
) -> Result<Vec<Vec<u64>>, ImpureError<ReadDelimAsciiWithoutRowsError>> {
    let mut buf = Vec::new();
    // Here we don't have $TOT so init to empty vectors
    let mut data: Vec<_> = ranges.iter().map(|_| vec![]).collect();
    let ncols = data.len();
    debug_assert!(ncols > 0, "no columns given for ASCII layout");
    let mut col = 0;
    let mut last_was_delim = false;
    let go = |data_: &mut Vec<Vec<u64>>, col_: usize, buf_: &[u8]| {
        ascii_to_uint(buf_)
            .map_err(ReadDelimAsciiWithoutRowsError::Parse)
            .map_err(ImpureError::Pure)
            .map(|x| data_[col_].push(x))
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
    if data.iter().map(Vec::len).unique().count() > 1 {
        return Err(ImpureError::Pure(ReadDelimAsciiUnequalColumnsError.into()));
    }
    // The spec isn't clear if the last value should be a delim or
    // not, so flush the buffer if it has anything in it since we
    // only try to parse if we hit a delim above.
    if !buf.is_empty() {
        go(&mut data, col, &buf)?;
    }
    Ok(data)
}

impl<C: HasWidth, S, T, D> LayoutDims for ColumnGroup<C, S, T, D> {
    fn ncols(&self) -> usize {
        self.inner.width()
    }

    fn clear(&mut self) {
        self.inner.clear();
    }
}

impl<C, S, T, D> LayoutRanges for DataLayout<C, S, T, D>
where
    for<'c> Range: From<&'c C>,
{
    fn ranges(&self) -> Vec<Range> {
        self.inner.iter().map(Into::into).collect()
    }
}

impl<C, S, T, D> LayoutDatatype for DataLayout<C, S, T, D>
where
    C: HasDatatype,
{
    fn datatype(&self) -> AlphaNumType {
        C::datatype_from_columns(&self.inner)
    }

    fn datatypes(&self) -> Vec<AlphaNumType> {
        self.inner.iter().map(HasDatatype::col_datatype).collect()
    }
}

impl<C, S, T, D> LayoutKeywords for DataLayout<C, S, T, D>
where
    C: IsFixed + HasDatatype,
    S: Copy + HasByteOrd,
    for<'c> ReqRootKeyword<'c>: From<SplitKeyword0<S::ByteOrd>>,
    for<'c> Range: From<&'c C>,
{
    fn byteord_keyword(&self) -> ReqRootKeyword<'_> {
        ReqRootKeyword::from_value(S::ByteOrd::from(self.byte_layout))
    }

    fn req_meas_keywords(&self) -> Vec<[ReqMeasKeyword<'_>; 2]> {
        self.inner
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let w = ReqMeasKeyword::from_value(Width::Fixed(c.fixed_width()), i);
                let r = ReqMeasKeyword::from_value(Range::from(c), i);
                [w, r]
            })
            .collect()
    }
}

impl<C, S, T, D> Removable<Range> for DataLayout<C, S, T, D>
where
    for<'c> Range: From<&'c C>,
{
    fn remove_nocheck(&mut self, index: MeasIndex) -> Range {
        debug_assert!(
            usize::from(index) <= self.inner.len(),
            "Index should be less than/equal to column number"
        );
        Range::from(&self.inner.remove(index.into()))
    }
}

impl<Col, Layout, TotType, Dtype> ReadLayoutOps<TotType> for DataLayout<Col, Layout, TotType, Dtype>
where
    Self: FixedLayoutIO + IntoEmptyDataFrame<DfTarget = <Self as FixedLayoutIO>::DfTarget>,
    Dtype: IsNumType,
    Col: Clone + IsFixed,
    Layout: Copy,
{
    fn h_read_df_inner<R: Read>(
        &self,
        h: &mut BufReader<R>,
        tot: TotType,
        seg: &mut AnyDataSegment,
        conf: &ReadEventsConfig,
    ) -> WarningsAndIOGroupResult<
        DataFrameResult<Self::DfTarget>,
        ReadDataframeWarning,
        ReadDataframeError,
        (),
    >
    where
        TotType: IsTot,
    {
        self.compute_nrows(seg, conf)
            .map_non_commutative_warnings(ReadDataframeWarning::from)
            .non_commutative_into_commutative()
            .map_errors(ReadDataframeError::from)
            .into_semigroup()
            .group()
            .map_error(IOErrorGroup::Pure)
            .and_then_commutative(|nrow_out| {
                TotType::check_tot(nrow_out.total_events, tot, conf.allow_tot_mismatch)
                    .switchable_into_commutative()
                    .map_commutative_warnings(ReadDataframeWarning::from)
                    .map_errors(ReadDataframeError::from)
                    .into_semigroup()
                    .group()
                    .map_error(IOErrorGroup::Pure)
                    .map_ok_value(|tot_not_eq| (tot_not_eq, nrow_out))
            })
            .and_then_commutative(|(tot_not_eq, nrow_out)| {
                let n = usize::try_from(nrow_out.total_events).expect("nrows exceeds usize");
                self.h_read_unchecked_df(h, n, conf)
                    .map_error(IOErrorGroup::from)
                    .map_commutative_warnings(ReadDataframeWarning::from)
                    .repack_warnings()
                    .map_ok_value(|(df, trunc)| {
                        let out = EventsDiagnostics::new(
                            Some(nrow_out.event_width),
                            Some(nrow_out.remainder),
                            tot_not_eq,
                            trunc,
                        );
                        DataFrameResult::new(df, out)
                    })
            })
    }
}

impl<C: FromRange, S, T, D> Insertable<Range> for DataLayout<C, S, T, D> {
    type Error = C::Error;

    fn insert_nocheck0(&mut self, index: MeasIndex, col: Range) -> Result<(), Self::Error> {
        self.inner.insert(index.into(), C::from_range(col)?.native);
        Ok(())
    }

    fn push0(&mut self, col: Range) -> Result<(), Self::Error> {
        self.inner.push(C::from_range(col)?.native);
        Ok(())
    }
}

// This will try to convert any integer range to the smallest bitmask, so if the
// existing layout is single width and we pass a new range that requires less
// bits, this will become a mixed layout. This probably isn't a big deal since
// most people will hopefully never want to keep a single width layout with one
// tiny column that doesn't use the entire width. The only advantage would be
// faster reads and writes since we can assume one width for the read/write
// loops, but this is a performance consideration and probably affects very few
// users. Accommodating this level of control would require a more complex API.
impl<W, C08, C16, C24, C32, C40, C48, C56, C64> Insertable<Range>
    for AnyEndianUint<AnyUint<C08, C16, C24, C32, C40, C48, C56, C64>, W>
where
    W: Insertable<Range, Error = RangeToBitmaskError>,
    C08: Insertable<Range> + Default + Generalize<W>,
    C16: Insertable<Range> + Default + Generalize<W>,
    C24: Insertable<Range> + Default + Generalize<W>,
    C32: Insertable<Range> + Default + Generalize<W>,
    C40: Insertable<Range> + Default + Generalize<W>,
    C48: Insertable<Range> + Default + Generalize<W>,
    C56: Insertable<Range> + Default + Generalize<W>,
    C64: Insertable<Range> + Default + Generalize<W>,
{
    type Error = RangeToBitmaskError;

    fn insert_nocheck0(&mut self, index: MeasIndex, col: Range) -> Result<(), Self::Error> {
        match self {
            Self::Single(x) => match_any_uint!(x, AnyUint, y, {
                if y.insert_nocheck0(index, col.clone()).is_err() {
                    *self = Self::Multi(mem::take(y).into_general());
                    return self.insert_nocheck0(index, col);
                }
                Ok(())
            }),
            Self::Multi(x) => x.insert_nocheck0(index, col),
        }
    }

    fn push0(&mut self, col: Range) -> Result<(), Self::Error> {
        match self {
            Self::Single(x) => match_any_uint!(x, AnyUint, y, {
                if y.push0(col.clone()).is_err() {
                    *self = Self::Multi(mem::take(y).into_general());
                    return self.push0(col);
                }
                Ok(())
            }),
            Self::Multi(x) => x.push0(col),
        }
    }
}

impl<A, I, F32, F64> Insertable<Range> for AnyDatatype<A, I, F32, F64>
where
    A: Insertable<Range>,
    I: Insertable<Range>,
    F32: Insertable<Range>,
    F64: Insertable<Range>,
    InsertRangeError: From<A::Error> + From<I::Error> + From<F32::Error> + From<F64::Error>,
{
    type Error = InsertRangeError;

    fn insert_nocheck0(&mut self, index: MeasIndex, col: Range) -> Result<(), Self::Error> {
        match_any_mixed!(self, x, {
            x.insert_nocheck0(index, col).map_err(Self::Error::from)
        })
    }

    fn push0(&mut self, col: Range) -> Result<(), Self::Error> {
        match_any_mixed!(self, x, { x.push0(col).map_err(Self::Error::from) })
    }
}

impl<C, S, T, D> OptMeasLayoutKeywords for DataLayout<C, S, T, D> {
    fn opt_meas_keywords(&self) -> Vec<Option<SplitKeyword1<NumType>>> {
        vec![None; self.inner.len()]
    }
}

impl<C, S, T, D> OrderedLayoutOps for DataLayout<C, S, T, D>
where
    S: Copy,
    ByteOrd2_0: From<S>,
{
    fn byte_order(&self) -> ByteOrd2_0 {
        self.byte_layout.into()
    }
}

impl<C, L, T, D> ColumnGroup<C, L, T, D> {
    fn phantom_into<Tf, Df>(self) -> ColumnGroup<C, L, Tf, Df> {
        ColumnGroup::new(self.inner, self.byte_layout)
    }

    fn byte_layout_into<Lf>(self) -> ColumnGroup<C, Lf, T, D>
    where
        L: Into<Lf>,
    {
        ColumnGroup::new(self.inner, self.byte_layout.into())
    }

    fn byte_layout_try_into<Lf>(self) -> Result<ColumnGroup<C, Lf, T, D>, Lf::Error>
    where
        Lf: TryFrom<L>,
    {
        self.byte_layout
            .try_into()
            .map(|byte_layout| ColumnGroup::new(self.inner, byte_layout))
    }
}

impl<C, T, D, const ORD: bool> ColumnGroup<C, NoByteOrd<ORD>, T, D> {
    pub fn new_ascii(columns: C) -> Self {
        Self::new(columns, NoByteOrd::<ORD>)
    }
}

impl<C, S, T, D> DataLayout<C, S, T, D> {
    pub fn new_empty(byte_layout: S) -> Self {
        Self::new(vec![], byte_layout)
    }

    pub fn columns(&self) -> &[C] {
        self.as_ref()
    }

    pub fn widths(&self) -> Vec<BitsOrChars>
    where
        C: IsFixed,
    {
        self.inner.iter().map(IsFixed::fixed_width).collect()
    }

    // fn new1(head: C, tail: impl IntoIterator<Item = C>, byte_layout: S) -> Self {
    //     let mut cs = vec![head];
    //     cs.extend(tail);
    //     Self::new(cs, byte_layout)
    // }

    fn try_new<F, P, W, E>(
        cs: Vec<ColumnLayoutValues<D>>,
        byte_layout: S,
        new_col_f: F,
    ) -> WarningsAndErrorsResult<NewLayout<Self>, (), W, E>
    where
        D: IsNumType,
        F: Fn(
            MeasIndex,
            ColumnLayoutValues<D>,
        ) -> WarningsAndErrorsResult<ConvertedRange<C>, P, W, E>,
    {
        cs.into_iter()
            .enumerate()
            .map(|(i, c)| new_col_f(i.into(), c).repack_errors())
            .sequence_commutative()
            .map_ok_value(|xs| {
                let (new_columns, truncated): (Vec<_>, Vec<_>) = xs
                    .into_iter()
                    .map(|cr| (cr.native, cr.non_truncated))
                    .unzip();
                let new_layout = Self::new(new_columns, byte_layout);
                NewLayout::new(new_layout, truncated)
            })
    }

    // fn insert_column_nocheck(&mut self, index: MeasIndex, col: C) {
    //     debug_assert!(
    //         usize::from(index) <= self.inner.len(),
    //         "Index should be less than/equal to number of columns"
    //     );
    //     self.inner.insert(index.into(), col);
    // }

    // fn push_column(&mut self, col: C) {
    //     self.inner.push(col);
    // }

    fn columns_into<X>(self) -> DataLayout<X, S, T, D>
    where
        X: From<C>,
    {
        DataLayout::new(self.inner.fmap(Into::into), self.byte_layout)
    }

    fn event_width(&self) -> usize
    where
        C: IsFixed,
    {
        self.inner
            .iter()
            .map(|c| usize::from(u8::from(c.nbytes())))
            .sum()
    }

    #[allow(clippy::trivially_copy_pass_by_ref)]
    fn compute_nrows(
        &self,
        seg: &mut AnyDataSegment,
        conf: &ReadEventsConfig,
    ) -> WarningOrErrorResult<ComputedRowsResult, (), UnevenEventWidthError, EventWidthError>
    where
        S: Clone,
        C: IsFixed,
    {
        let n = seg.len();
        let w = usize_to_u64(self.event_width());
        if w == 0 {
            LogResult::new_err(EventWidthError::from(ZeroEventWidthError::new(n)))
        } else {
            let limit = conf.data_remainder_limit;
            let total_events = n / w;
            let remainder = n % w;
            let out = ComputedRowsResult::new(total_events, w, remainder);
            // If within remainder limit, truncate offset and return without
            // error
            if remainder <= limit.0 {
                seg.truncate(remainder);
                return LogResult::new_ok(out);
            }
            let is_ok = remainder == 0;
            let e = UnevenEventWidthError::new(w, n, remainder);
            let flag = conf.allow_uneven_event_width;
            SwitchableErrorResult::new_switchable_ok_if3(is_ok, out, (), e, flag)
                .switchable_into_non_commutative()
                .map_errors(EventWidthError::from)
        }
    }
}

trait FixedLayoutIO {
    type DfTarget;

    fn h_read_unchecked_df<R: Read>(
        &self,
        h: &mut BufReader<R>,
        nrows: usize,
        conf: &ReadEventsConfig,
    ) -> WarningsAndIOResult<
        (Self::DfTarget, Vec<OverrangeColumn>),
        EventOverRangeError,
        ReadDataframeError,
    > {
        let (df, rs) = match self.h_read_unchecked_df_inner(h, nrows, conf) {
            Ok(x) => x,
            Err(e) => return LogResult::new_err(e),
        };
        let overrange = rs.iter().map(TruncatedResult::as_col).collect();
        let es = rs.into_iter().filter_map(TruncatedResult::into_err);

        let flag = conf.disallow_over_range;
        SwitchableErrorsResult::new_deferred_switchable_iter3((), es, flag)
            .switchable_into_commutative()
            .group()
            .map_error(ReadDataframeError::from)
            .map_error(ImpureError::Pure)
            .map_ok_value(|()| (df, overrange))
    }

    fn h_read_unchecked_df_inner<R: Read>(
        &self,
        h: &mut BufReader<R>,
        nrows: usize,
        conf: &ReadEventsConfig,
    ) -> IOResult<(Self::DfTarget, Vec<TruncatedResult>), ReadDataframeError>;
}

trait ByteLayoutIO<C: HasNativeType> {
    fn read_matrix<R: Read>(
        &self,
        h: &mut BufReader<R>,
        buf: &mut RowBuffer,
        cols: &mut Vec<Vec<C::Native>>,
    ) -> io::Result<()>;
}

trait HasRange<C> {
    fn check_range(
        &mut self,
        range: &C,
        i: MeasIndex,
        trunc: TruncateEventValues,
    ) -> TruncatedResult;
}

impl<C> HasRange<C> for Vec<C::Native>
where
    C: HasNativeType + IntoRange + HasDatatype,
    C::Native: PartialOrd,
{
    fn check_range(
        &mut self,
        range: &C,
        i: MeasIndex,
        trunc: TruncateEventValues,
    ) -> TruncatedResult {
        let dt = range.col_datatype();
        let (upper_limit, rng) = range.as_range();
        if dt.matches_truncation(trunc) {
            // If we wish to truncate this column, silently truncate without
            // throwing any errors
            let mut j = None;
            for (rowi, x) in self.iter_mut().enumerate() {
                if *x > upper_limit {
                    if j.is_none() {
                        j = Some(rowi);
                    }
                    *x = upper_limit;
                }
            }
            j.map_or(TruncatedResult::None, TruncatedResult::Truncated)
        } else {
            // Otherwise, scan through the values and return error on first
            // encounter with overrange value
            for (rowi, x) in self.iter().enumerate() {
                if *x > upper_limit {
                    return TruncatedResult::Overrange(i, rowi, rng);
                }
            }
            TruncatedResult::None
        }
    }
}

macro_rules! impl_byte_layout_io {
    ($inner:path, $layout:path, $fun:ident) => {
        impl ByteLayoutIO<$inner> for $layout {
            fn read_matrix<R: Read>(
                &self,
                h: &mut BufReader<R>,
                buf: &mut RowBuffer,
                cols: &mut Vec<Vec<<$inner as HasNativeType>::Native>>,
            ) -> io::Result<()> {
                buf.$fun(h, cols, *self)
            }
        }
    };
}

macro_rules! impl_ordered_layout_io {
    ($t:ident) => {
        impl_byte_layout_io!(
            $t,
            ArrayByteOrd<<<$t as HasNativeType>::Native as FCSRepr>::ByteOrd>,
            read_ordered_matrix
        );
    };
}

// macro_rules! impl_unaligned_ord_int_layout_io {
//     ($t:ident, $len:expr) => {
//         impl_byte_layout_io!($t, SizedByteOrd<$len>, read_unaligned_ordered_int_matrix);
//     };
// }

macro_rules! impl_endian_layout_io {
    ($t:ident) => {
        impl_byte_layout_io!($t, Endian, read_endian_matrix);
    };
}

// macro_rules! impl_unaligned_endian_int_layout_io {
//     ($t:ident, $len:expr) => {
//         impl_byte_layout_io!(
//             $t,
//             Endian,
//             read_unaligned_endian_int_matrix::<_, _, $len, _>
//         );
//     };
// }

impl_ordered_layout_io!(Bitmask08);
impl_ordered_layout_io!(Bitmask16);
impl_ordered_layout_io!(Bitmask24);
impl_ordered_layout_io!(Bitmask32);
impl_ordered_layout_io!(Bitmask40);
impl_ordered_layout_io!(Bitmask48);
impl_ordered_layout_io!(Bitmask56);
impl_ordered_layout_io!(Bitmask64);
impl_ordered_layout_io!(F32Range);
impl_ordered_layout_io!(F64Range);

impl_endian_layout_io!(Bitmask08);
impl_endian_layout_io!(Bitmask16);
impl_endian_layout_io!(Bitmask24);
impl_endian_layout_io!(Bitmask32);
impl_endian_layout_io!(Bitmask40);
impl_endian_layout_io!(Bitmask48);
impl_endian_layout_io!(Bitmask56);
impl_endian_layout_io!(Bitmask64);
impl_endian_layout_io!(F32Range);
impl_endian_layout_io!(F64Range);

// impl_endian_layout_io!(Bitmask08);
// impl_endian_layout_io!(Bitmask16);
// impl_endian_layout_io!(Bitmask32);
// impl_endian_layout_io!(Bitmask64);
// impl_endian_layout_io!(F32Range);
// impl_endian_layout_io!(F64Range);

// impl_unaligned_ord_int_layout_io!(Bitmask24, 3);
// impl_unaligned_ord_int_layout_io!(Bitmask40, 5);
// impl_unaligned_ord_int_layout_io!(Bitmask48, 6);
// impl_unaligned_ord_int_layout_io!(Bitmask56, 7);

// impl_unaligned_endian_int_layout_io!(Bitmask24, 3);
// impl_unaligned_endian_int_layout_io!(Bitmask40, 5);
// impl_unaligned_endian_int_layout_io!(Bitmask48, 6);
// impl_unaligned_endian_int_layout_io!(Bitmask56, 7);

impl<T, D, const ORD: bool> FixedLayoutIO for FixedAsciiLayout<T, D, ORD> {
    type DfTarget = FixedAsciiDataFrame<T, D, ORD>;

    fn h_read_unchecked_df_inner<R: Read>(
        &self,
        h: &mut BufReader<R>,
        nrows: usize,
        conf: &ReadEventsConfig,
    ) -> IOResult<(Self::DfTarget, Vec<TruncatedResult>), ReadDataframeError> {
        let row_width = self.event_width();

        let mut row_buf = RowBuffer::init(conf.row_buffer_size, nrows, row_width);

        let mut columns: Vec<_> = self
            .inner
            .iter()
            .map(|r| RangedVec::new(*r, vec![0; nrows]))
            .collect();

        row_buf.read_char_matrix(h, &mut columns).map_err(|e| {
            e.fmap_once(ReadFixedAsciiError::from)
                .fmap_once(ReadAsciiError::from)
                .fmap_once(ReadDataframeError::from)
        })?;

        let trunc = conf.truncate_event_values;
        let rs = columns
            .iter_mut()
            .enumerate()
            .map(|(i, c)| c.data.check_range(&c.range, i.into(), trunc))
            .collect();

        let data = columns.into_iter().map(NativeColumn::from);
        let df = FFDataFrame::try_new(data).unwrap();
        Ok((ColumnGroup::new(df, self.byte_layout), rs))
    }
}

impl<C, S, Tot, Dt> FixedLayoutIO for DataLayout<C, S, Tot, Dt>
where
    S: ByteLayoutIO<C> + Copy,
    C: HasNativeType + IsFixed + Clone,
    C::Native: FCSRepr + PartialOrd,
    NativeColumn<C>: From<RangedVec<C, C::Native>>,
    Vec<C::Native>: HasRange<C>,
{
    type DfTarget = DataFrame<NativeColumn<C>, S, Tot, Dt>;

    fn h_read_unchecked_df_inner<R: Read>(
        &self,
        h: &mut BufReader<R>,
        nrows: usize,
        conf: &ReadEventsConfig,
    ) -> IOResult<(Self::DfTarget, Vec<TruncatedResult>), ReadDataframeError> {
        let mut row_buf = RowBuffer::init(conf.row_buffer_size, nrows, self.event_width());
        let ncols = self.columns().len();
        let mut columns = vec![vec![C::Native::default(); nrows]; ncols];

        self.byte_layout
            .read_matrix(h, &mut row_buf, &mut columns)?;

        let trunc = conf.truncate_event_values;
        let rs = columns
            .iter_mut()
            .enumerate()
            .zip(&self.inner[..])
            .map(|((i, d), c)| d.check_range(c, i.into(), trunc))
            .collect();

        let data = columns
            .into_iter()
            .zip(self.inner.iter().cloned())
            .map(|(data, range)| NativeColumn::from(RangedVec::new(range, data)));
        let df = FFDataFrame::try_new(data).unwrap();

        Ok((ColumnGroup::new(df, self.byte_layout), rs))
    }
}

impl<D> FixedLayoutIO for EndianLayout<AnyNullBitmask, D> {
    type DfTarget = EndianUintDataFrame<D>;

    fn h_read_unchecked_df_inner<R: Read>(
        &self,
        h: &mut BufReader<R>,
        nrows: usize,
        conf: &ReadEventsConfig,
    ) -> IOResult<(Self::DfTarget, Vec<TruncatedResult>), ReadDataframeError> {
        let mut row_buf = RowBuffer::init(conf.row_buffer_size, nrows, self.event_width());
        let mut columns: Vec<_> = self.inner.iter().map(|c| c.init_column(nrows)).collect();

        row_buf.read_any_uint_df(h, &mut columns, self.byte_layout)?;

        let trunc = conf.truncate_event_values;
        let rs = columns
            .iter_mut()
            .enumerate()
            .map(|(i, c)| c.check_range(i.into(), trunc))
            .collect();

        let data = columns.into_iter().map(AnyBitmaskColumn::from);
        let df = FFDataFrame::try_new(data).unwrap();

        Ok((ColumnGroup::new(df, self.byte_layout), rs))
    }
}

impl FixedLayoutIO for MixedLayout {
    type DfTarget = MixedDataFrame;

    fn h_read_unchecked_df_inner<R: Read>(
        &self,
        h: &mut BufReader<R>,
        nrows: usize,
        conf: &ReadEventsConfig,
    ) -> IOResult<(Self::DfTarget, Vec<TruncatedResult>), ReadDataframeError> {
        let mut buf = RowBuffer::init(conf.row_buffer_size, nrows, self.event_width());
        let en = self.byte_layout;
        let cs = &self.inner[..];

        let mut columns =
            if let Some(ret) = try_single::<_, _, F32Range>(h, cs, nrows, en, &mut buf)? {
                // If the types are all the same width (but not necessary the same
                // type), we can "cheat" and read the layout all as one type and
                // cast to other types after the fact. This will dramatically speed
                // up reading for massive files such as S8/A8.
                //
                // This is for 32-bit float+int
                ret
            } else if let Some(ret) = try_single::<_, _, F64Range>(h, cs, nrows, en, &mut buf)? {
                // ditto 64-bit
                ret
            } else {
                // Totally mixed layout, dispatch for each column. This will be
                // slower but is necessary to read each type correctly.
                let mut columns: Vec<_> = self.inner.iter().map(|c| c.init_column(nrows)).collect();
                buf.read_mixed_df(h, &mut columns, self.byte_layout)
                    .map_err(|e| {
                        e.fmap_once(ReadFixedAsciiError::from)
                            .fmap_once(ReadAsciiError::from)
                            .fmap_once(ReadDataframeError::from)
                    })?;
                columns
            };

        let trunc = conf.truncate_event_values;
        let rs = columns
            .iter_mut()
            .enumerate()
            .map(|(i, c)| c.check_range(i.into(), trunc))
            .collect();

        let data = columns.into_iter().map(MixedColumn::from);
        let df = FFDataFrame::try_new(data).unwrap();

        Ok((ColumnGroup::new(df, self.byte_layout), rs))
    }
}

enum Any4ByteType {
    F32(F32Range),
    Uint32(Bitmask32),
}

enum Any8ByteType {
    F64(F64Range),
    Uint64(Bitmask64),
}

macro_rules! impl_single_width {
    ($t:ident, $i:ident, $f:ident, $u:ident) => {
        impl TryFrom<MixedRange> for $t {
            type Error = ();
            fn try_from(value: MixedRange) -> Result<Self, Self::Error> {
                match value {
                    AnyDatatype::$f(r) => Ok(Self::$f(r)),
                    AnyDatatype::Uint(AnyUint::$u(r)) => Ok(Self::$u(r)),
                    _ => Err(()),
                }
            }
        }

        impl From<(Vec<$i>, $t)> for MixedVec {
            fn from(value: (Vec<$i>, $t)) -> Self {
                let (data, range) = value;
                match range {
                    $t::$f(r) => Self::$f(RangedVec::new(r, data)),
                    $t::$u(r) => {
                        let v = RangedVec::new(r, cast_vec(data));
                        Self::Uint(AnyUintVec::$u(v))
                    }
                }
            }
        }
    };
}

impl_single_width!(Any4ByteType, f32, F32, Uint32);
impl_single_width!(Any8ByteType, f64, F64, Uint64);

fn try_single<R, W, C>(
    h: &mut BufReader<R>,
    ranges: &[MixedRange],
    nrows: usize,
    endian: Endian,
    row_buf: &mut RowBuffer,
) -> io::Result<Option<Vec<MixedVec>>>
where
    R: Read,
    Endian: ByteLayoutIO<C>,
    W: TryFrom<MixedRange>,
    (Vec<C::Native>, W): Into<MixedVec>,
    C: HasNativeType,
    C::Native: Default + Clone,
{
    if let Ok(cs) = ranges
        .iter()
        .cloned()
        .map(W::try_from)
        .collect::<Result<Vec<_>, _>>()
    {
        let zero = <C as HasNativeType>::Native::default();
        let mut columns = vec![vec![zero; nrows]; cs.len()];
        ByteLayoutIO::<C>::read_matrix(&endian, h, row_buf, &mut columns)?;
        let ret = columns
            .into_iter()
            .zip(cs)
            .map(|(data, range)| (data, range).into())
            .collect();
        Ok(Some(ret))
    } else {
        Ok(None)
    }
}

macro_rules! def_native_wrapper {
    ($name:path, $native:ty) => {
        impl HasNativeType for $name {
            type Native = $native;
        }
    };
}

def_native_wrapper!(Bitmask08, u8);
def_native_wrapper!(Bitmask16, u16);
def_native_wrapper!(Bitmask24, U24);
def_native_wrapper!(Bitmask32, u32);
def_native_wrapper!(Bitmask40, U40);
def_native_wrapper!(Bitmask48, U48);
def_native_wrapper!(Bitmask56, U56);
def_native_wrapper!(Bitmask64, u64);
def_native_wrapper!(F32Range, f32);
def_native_wrapper!(F64Range, f64);
def_native_wrapper!(FixedAsciiRange, u64);
def_native_wrapper!(DelimAsciiRange, u64);

impl HasOneDatatype for FixedAsciiRange {
    const DATATYPE: AlphaNumType = AlphaNumType::Ascii;
}

impl<T> HasOneDatatype for Bitmask<T> {
    const DATATYPE: AlphaNumType = AlphaNumType::Integer;
}

impl HasOneDatatype for F32Range {
    const DATATYPE: AlphaNumType = AlphaNumType::Float;
}

impl HasOneDatatype for F64Range {
    const DATATYPE: AlphaNumType = AlphaNumType::Double;
}

impl HasOneDatatype for AnyNullBitmask {
    const DATATYPE: AlphaNumType = AlphaNumType::Integer;
}

impl<T: HasOneDatatype> HasDatatype for T {
    fn col_datatype(&self) -> AlphaNumType {
        T::DATATYPE
    }

    fn datatype_from_columns(_: &[Self]) -> AlphaNumType {
        T::DATATYPE
    }
}

impl<A, I, F32, F64> HasDatatype for AnyDatatype<A, I, F32, F64> {
    fn col_datatype(&self) -> AlphaNumType {
        match self {
            Self::Ascii(_) => AlphaNumType::Ascii,
            Self::Uint(_) => AlphaNumType::Integer,
            Self::F32(_) => AlphaNumType::Float,
            Self::F64(_) => AlphaNumType::Double,
        }
    }

    fn datatype_from_columns(cs: &[Self]) -> AlphaNumType {
        // If any numeric types are none, then that means at least one column is
        // ASCII, which means that $DATATYPE needs to be "A" since $PnDATATYPE
        // cannot be "A". Otherwise, find majority type.
        if let Some(xs) = cs.try_into_nonempty_iter() {
            if let Ok(mut ds) = xs
                .map(|c| NumType::try_from(c.col_datatype()))
                .collect::<Result<NEVec<_>, _>>()
            {
                ds.sort();
                let (dt, _) = ds
                    .nonempty_iter()
                    .group_by(|x| *x)
                    .map(|x| (*x.first(), x.len()))
                    .max_by_key(|(_, n)| *n);
                (*dt).into()
            } else {
                AlphaNumType::Ascii
            }
        } else {
            // NOTE this is a totally arbitrary default
            AlphaNumType::Integer
        }
    }
}

impl<T> IntoRange for Bitmask<T>
where
    Self: HasNativeType<Native = T>,
    T: Copy + Into<Range>,
{
    fn as_range(&self) -> (Self::Native, Range) {
        let b = self.bitmask();
        (b, b.into())
    }
}

impl<T> IntoRange for FloatRange<T>
where
    Self: HasNativeType<Native = T>,
    T: Copy,
    FloatDecimal<T>: Into<Range> + Into<T>,
{
    fn as_range(&self) -> (Self::Native, Range) {
        let r = &self.range;
        (r.clone().into(), r.clone().into())
    }
}

impl IntoRange for FixedAsciiRange {
    fn as_range(&self) -> (Self::Native, Range) {
        let r = self.value();
        (r.0, r.0.into())
    }
}

impl<T> FromRange for Bitmask<T>
where
    T: TryFrom<Range, Error = RangeToIntError<T>>
        + FCSRepr
        + Copy
        + Bounded
        + Shr<usize, Output = T>,
    u64: From<T>,
{
    type Error = RangeToBitmaskError;

    fn from_range_inner(range: Range) -> DeferredError<ConvertedRange<Self>, Self::Error> {
        range
            .clone()
            .into_uint()
            .map_error(RangeToBitmaskError::from)
            .and_then_replace(|x| {
                Self::try_from_native(x)
                    .map_error(RangeToBitmaskError::from)
                    .map_ok_value(|n| ConvertedRange::new(n, None))
                    .map_err_value(|n| ConvertedRange::new(n, Some(range)))
            })
    }
}

impl<T> FromRange for FloatRange<T>
where
    T: HasFloatBounds,
{
    type Error = DecimalToFloatError;

    fn from_range_inner(range: Range) -> DeferredError<ConvertedRange<Self>, Self::Error> {
        range
            .clone()
            .into_float()
            .map_deferred_value(Self::new)
            .map_ok_value(|n| ConvertedRange::new(n, None))
            .map_err_value(|n| ConvertedRange::new(n, Some(range)))
    }
}

impl FromRange for AsciiRangeValue {
    type Error = RangeToAsciiError;

    /// Make new AsciiRange from a float or integer.
    ///
    /// The number of chars will be automatically selected as the minimum
    /// required to express the range.
    fn from_range_inner(range: Range) -> DeferredError<ConvertedRange<Self>, Self::Error> {
        range
            .clone()
            .into_ascii_uint()
            .map_errors(RangeToAsciiError::from)
            .map_ok_value(|n| ConvertedRange::new(n, None))
            .map_err_value(|n| ConvertedRange::new(n, Some(range)))
    }
}

impl FromRange for FixedAsciiRange {
    type Error = RangeToAsciiError;

    /// Make new [`FixedAsciiRange`] from a float or integer.
    ///
    /// The number of chars will be automatically selected as the minimum
    /// required to express the range.
    fn from_range_inner(range: Range) -> DeferredError<ConvertedRange<Self>, Self::Error> {
        AsciiRangeValue::from_range_inner(range).map_deferred_value(Functor::fmap_into)
    }
}

impl FromRange for DelimAsciiRange {
    type Error = RangeToAsciiError;

    /// Make new [`DelimAsciiRange`] from a float or integer.
    fn from_range_inner(range: Range) -> DeferredError<ConvertedRange<Self>, Self::Error> {
        AsciiRangeValue::from_range_inner(range).map_deferred_value(Functor::fmap_into)
    }
}

// NOTE this is a bit weird since we are letting the type control the size.
// There are a few edge cases where a user may wish to control the size but
// these are all for performance and supporting them would make the API much
// more complex.
impl FromRange for AnyNullBitmask {
    type Error = RangeToBitmaskError;

    /// make a new bitmask from a float or integer.
    ///
    /// The size will be determined by the input and will be kept as small as
    /// possible.
    fn from_range_inner(range: Range) -> DeferredError<ConvertedRange<Self>, Self::Error> {
        range
            .clone()
            .into_uint()
            .map_errors(RangeToBitmaskError::from)
            .map_deferred_value(|x: BitmaskValue<u64>| Self::from(x))
            .map_ok_value(|n| ConvertedRange::new(n, None))
            .map_err_value(|n| ConvertedRange::new(n, Some(range)))
    }
}

impl<T> IsFixed for Bitmask<T>
where
    T: FCSRepr,
{
    fn nbytes(&self) -> NonZeroU8 {
        T::FILE_BYTES.into()
    }

    fn fixed_width(&self) -> BitsOrChars {
        BitsOrChars(T::FILE_BYTES.into())
    }
}

impl<T> IsFixed for FloatRange<T>
where
    T: FCSRepr,
{
    fn nbytes(&self) -> NonZeroU8 {
        T::FILE_BYTES.into()
    }

    fn fixed_width(&self) -> BitsOrChars {
        BitsOrChars(T::FILE_BYTES.into())
    }
}

impl IsFixed for FixedAsciiRange {
    fn nbytes(&self) -> NonZeroU8 {
        self.chars().into()
    }

    fn fixed_width(&self) -> BitsOrChars {
        BitsOrChars(self.chars().into())
    }
}

impl IsFixed for AnyNullBitmask {
    fn nbytes(&self) -> NonZeroU8 {
        match_any_uint!(self, Self, x, { x.nbytes() })
    }

    fn fixed_width(&self) -> BitsOrChars {
        match_any_uint!(self, Self, x, { x.fixed_width() })
    }
}

impl IsFixed for MixedRange {
    fn nbytes(&self) -> NonZeroU8 {
        match_any_mixed!(self, x, { x.nbytes() })
    }

    fn fixed_width(&self) -> BitsOrChars {
        match_any_mixed!(self, x, { x.fixed_width() })
    }
}

// macro_rules! source_from_iter {
//     ($from:ident, $to:ident, $wrap:ident) => {
//         impl<'a> From<FCSColIter<'a, $from, $to>> for AnySource<'a, $to> {
//             fn from(value: FCSColIter<'a, $from, $to>) -> Self {
//                 Self::$wrap(value)
//             }
//         }
//     };
// }

// source_from_iter!(u8, u8, FromU08);
// source_from_iter!(u8, u16, FromU08);
// source_from_iter!(u8, u32, FromU08);
// source_from_iter!(u8, u64, FromU08);
// source_from_iter!(u8, f32, FromU08);
// source_from_iter!(u8, f64, FromU08);

// source_from_iter!(u16, u8, FromU16);
// source_from_iter!(u16, u16, FromU16);
// source_from_iter!(u16, u32, FromU16);
// source_from_iter!(u16, u64, FromU16);
// source_from_iter!(u16, f32, FromU16);
// source_from_iter!(u16, f64, FromU16);

// source_from_iter!(u32, u8, FromU32);
// source_from_iter!(u32, u16, FromU32);
// source_from_iter!(u32, u32, FromU32);
// source_from_iter!(u32, u64, FromU32);
// source_from_iter!(u32, f32, FromU32);
// source_from_iter!(u32, f64, FromU32);

// source_from_iter!(u64, u8, FromU64);
// source_from_iter!(u64, u16, FromU64);
// source_from_iter!(u64, u32, FromU64);
// source_from_iter!(u64, u64, FromU64);
// source_from_iter!(u64, f32, FromU64);
// source_from_iter!(u64, f64, FromU64);

// source_from_iter!(f32, u8, FromF32);
// source_from_iter!(f32, u16, FromF32);
// source_from_iter!(f32, u32, FromF32);
// source_from_iter!(f32, u64, FromF32);
// source_from_iter!(f32, f32, FromF32);
// source_from_iter!(f32, f64, FromF32);

// source_from_iter!(f64, u8, FromF64);
// source_from_iter!(f64, u16, FromF64);
// source_from_iter!(f64, u32, FromF64);
// source_from_iter!(f64, u64, FromF64);
// source_from_iter!(f64, f32, FromF64);
// source_from_iter!(f64, f64, FromF64);

impl<D> AnySingleUintLayout<D> {
    // TODO why pub?
    #[must_use]
    pub fn phantom_into<X>(self) -> AnySingleUintLayout<X> {
        match_map_uint!(self, l, l.phantom_into())
    }
}

trait Generalize<T> {
    fn into_general(self) -> T;
}

impl<C, L, T, D> Generalize<EndianUintLayout<D>> for DataLayout<C, L, T, D>
where
    C: Into<AnyNullBitmask>,
    L: Into<Endian>,
{
    fn into_general(self) -> EndianUintLayout<D> {
        self.map_vec(Into::into).byte_layout_into().phantom_into()
    }
}

impl<C, L, T, D> Generalize<EndianUintDataFrame<D>> for DataFrame<C, L, T, D>
where
    C: Into<AnyBitmaskColumn>,
    L: Into<Endian>,
{
    fn into_general(self) -> EndianUintDataFrame<D> {
        self.map_cols(Into::into).byte_layout_into().phantom_into()
    }
}

impl<C, L, T, D> Generalize<MixedLayout> for DataLayout<C, L, T, D>
where
    C: Into<MixedRange>,
    L: Into<Endian>,
{
    fn into_general(self) -> MixedLayout {
        self.map_vec(Into::into).byte_layout_into().phantom_into()
    }
}

impl<T, D, const ORD: bool> Generalize<MixedLayout> for AnyAsciiLayout<T, D, ORD>
where
    NoByteOrd<ORD>: Into<Endian>,
{
    fn into_general(self) -> MixedLayout {
        match_many_to_one!(self, AnyAscii, [Delimited, Fixed], x, { x.into_general() })
    }
}

impl<D> Generalize<MixedLayout> for AnySingleUintLayout<D> {
    fn into_general(self) -> MixedLayout {
        match_any_uint!(self, Self, x, { x.map_vec(Into::into).phantom_into() })
    }
}

impl<D> Generalize<MixedLayout> for AnyEndianUintLayout<D> {
    fn into_general(self) -> MixedLayout {
        match_many_to_one!(self, AnyEndianUint, [Single, Multi], x, {
            x.into_general()
        })
    }
}

impl<D> Generalize<MixedLayout> for NonMixedEndianLayout<D> {
    fn into_general(self) -> MixedLayout {
        match_many_to_one!(self, AnyDatatype, [Ascii, Uint, F32, F64], x, {
            x.into_general()
        })
    }
}

impl<T> AnyOrderedUintLayout<T> {
    #[must_use]
    pub fn phantom_into<X>(self) -> AnyOrderedUintLayout<X> {
        match_map_uint!(self, l, l.phantom_into())
    }

    fn into_endian<D>(self) -> Result<AnySingleUintLayout<D>, OrderedToEndianError> {
        let ret = match_map_uint!(
            self,
            x,
            x.byte_layout_try_into()?.phantom_into().columns_into()
        );
        Ok(ret)
    }

    fn try_new(
        cs: Vec<ColumnLayoutValues<Nothing<NumType>>>,
        bo: ByteOrd2_0,
        conf: &ReadDataKeywordsConfig,
    ) -> WarningsAndErrorsResult<NewLayout<Self>, (), IndexedBitmaskError, NewFixedIntLayoutError>
    {
        let notrunc = conf.disallow_range_truncation;
        let real_bo = conf.integer_byteord_override.unwrap_or(bo);
        let n = real_bo.nbytes();

        // First, scan through the widths to make sure they are all fixed and
        // are all the same number of bytes as ByteOrd. Skip this step if we
        // are ignoring $PnB for width and simply using the length of $BYTEORD.
        let width_res = if conf.integer_widths_from_byteord.is_set() {
            LogResult::new_ok(())
        } else {
            cs.iter()
                .map(|c| c.width)
                .enumerate()
                .map(|(i, c)| {
                    PrivBytes::try_from(c)
                        .map_err(|e| IndexedError::new(i, e))
                        .map_err(IndexedWidthToBytesError)
                        .map_err(SingleFixedWidthError::from)
                })
                .map(Result::into_log::<_, _, Vec<_>>)
                .sequence_commutative()
                .and_then_commutative(|widths| {
                    let ws = widths.into_iter().filter(|&w| w != n).unique();
                    if let Some(mismatches) = ws.try_into_nonempty_iter() {
                        let e = WidthMismatchError::new(real_bo, mismatches.collect());
                        LogResult::new_err(SingleFixedWidthError::from(e))
                    } else {
                        LogResult::new_ok(())
                    }
                })
        };

        // Second, make the layout, and force all columns to the correct type
        // based on ByteOrd. It is necessary to check the columns first because
        // the bitmask won't necessarily fail even if it is larger than the
        // target type.
        //
        // NOTE this step is independent of $PnB, so downstream control flow is
        // dictated by warnings/errors
        let layout_res =
            match_many_to_one!(real_bo, ByteOrd2_0, [O1, O2, O3, O4, O5, O6, O7, O8], o, {
                DataLayout::try_new(cs, o, |i, c| {
                    Bitmask::from_range_switch(c.range, notrunc)
                        .map_switchable_errors(|e| IndexedError::new(i, e))
                        .map_switchable_errors(IndexedBitmaskError)
                        .switchable_into_commutative()
                        .into_semigroup()
                })
                .map_errors(NewFixedIntLayoutError::from)
                .set_err_value(())
                .map_ok_value(FunctorOnce::fmap_into_once)
            });

        width_res
            .nowarn_into_warn()
            .map_errors(NewFixedIntLayoutError::from)
            .zip_commutative(layout_res)
            .map_ok_value(|((), layout)| layout)
    }
}

impl<T, D, const ORD: bool> Default for AnyAsciiLayout<T, D, ORD> {
    fn default() -> Self {
        Self::Fixed(DataLayout::default())
    }
}

impl<T, D, const ORD: bool> AnyAsciiLayout<T, D, ORD> {
    #[must_use]
    pub fn phantom_into<T1, D1, const ORD_1: bool>(self) -> AnyAsciiLayout<T1, D1, ORD_1> {
        match self {
            Self::Delimited(x) => DelimAsciiLayout::new_ascii(x.inner).into(),
            Self::Fixed(x) => DataLayout::new_ascii(x.inner).into(),
        }
    }

    pub(crate) fn try_new(
        cs: Vec<ColumnLayoutValues<D>>,
        flag: DisallowRangeTrunc,
    ) -> WarningsAndErrorsResult<
        NewLayout<Self>,
        (),
        IndexedRangeToAsciiError,
        AsciiRangeFromKeywordsError,
    >
    where
        D: IsNumType,
    {
        if cs.iter().all(|c| c.width == Width::Variable) {
            cs.into_iter()
                .enumerate()
                .map(|(i, c)| FixedAsciiRange::from_range_indexed(c.range, i.into(), flag))
                .sequence_def()
                .map_ok_value(|rs| {
                    let ranges = rs.iter().map(|r| r.native.value().into()).collect();
                    let non_truncated = rs.into_iter().map(|r| r.non_truncated).collect();
                    let l = DelimAsciiLayout::new_ascii(ranges).into();
                    NewLayout::new(l, non_truncated)
                })
                .map_err_value(|_| ())
        } else {
            DataLayout::try_new(cs, NoByteOrd, |i, c| {
                FixedAsciiRange::from_width_and_range(c.width, c.range, i, flag)
            })
            .map_ok_value(FunctorOnce::fmap_into_once)
        }
    }

    fn new_fixed(columns: impl IntoIterator<Item = FixedAsciiRange>) -> Self {
        Self::Fixed(DataLayout::new_ascii(columns.into_iter().collect()))
    }

    fn new_delim(ranges: Vec<DelimAsciiRange>) -> Self {
        Self::Delimited(DelimAsciiLayout::new_ascii(ranges))
    }
}

impl<T, D, const ORD: bool> FixedAsciiLayout<T, D, ORD> {
    #[must_use]
    pub fn new_ascii_u64(ranges: Vec<AsciiRangeValue>) -> Self {
        Self::new_ascii(ranges.fmap_into())
    }
}

impl<T, TC> OrderedLayout<Bitmask<T>, TC>
where
    T: FCSRepr,
    Bitmask<T>: HasNativeType<Native = T>,
{
    #[must_use]
    pub fn new_endian_uint(ranges: Vec<Bitmask<T>>, endian: Endian) -> Self {
        Self::new(ranges, ArrayByteOrd::Endian(endian))
    }
}

impl<T, TC> OrderedLayout<FloatRange<T>, TC>
where
    T: FCSRepr,
    FloatRange<T>: HasNativeType<Native = T>,
{
    #[must_use]
    pub fn new_endian_float(ranges: Vec<FloatRange<T>>, endian: Endian) -> Self {
        Self::new(ranges, ArrayByteOrd::Endian(endian))
    }
}

impl VersionedDataLayout for DataLayout2_0 {
    type ByteLayout = ByteOrd2_0;
    type NumType = Nothing<NumType>;
    type Tot = Option<Tot>;

    fn lookup(
        std: &mut StdKeywords,
        meas_nonstd: &mut [NonStdKeywords],
        conf: &ReadDataKeywordsConfig,
    ) -> LookupLayoutResult<NewLayout<Self>> {
        AnyOrderedLayout::lookup(std, meas_nonstd, conf).map_ok_value(FunctorOnce::fmap_into_once)
    }

    fn lookup_ro(
        kws: &StdKeywords,
        par: Par,
        conf: &ReadDataKeywordsConfig,
    ) -> LookupLayoutResult<NewLayout<Self>> {
        AnyOrderedLayout::lookup_ro(kws, par, conf).map_ok_value(FunctorOnce::fmap_into_once)
    }

    fn new_empty(datatype: AlphaNumType) -> Self {
        AnyOrderedLayout::new_empty(datatype).into()
    }

    fn try_new(
        datatype: AlphaNumType,
        byteord: Self::ByteLayout,
        columns: Vec<ColumnLayoutValues<Self::NumType>>,
        conf: &ReadDataKeywordsConfig,
    ) -> WarningsAndErrorsResult<NewLayout<Self>, (), NewMixedTypeWarning, NewDataLayoutError> {
        AnyOrderedLayout::try_new(datatype, byteord, columns, conf)
            .map_ok_value(FunctorOnce::fmap_into_once)
    }
}

impl VersionedDataLayout for DataLayout3_0 {
    type ByteLayout = ByteOrd2_0;
    type NumType = Nothing<NumType>;
    type Tot = Identity<Tot>;

    fn lookup(
        std: &mut StdKeywords,
        meas_nonstd: &mut [NonStdKeywords],
        conf: &ReadDataKeywordsConfig,
    ) -> LookupLayoutResult<NewLayout<Self>> {
        AnyOrderedLayout::lookup(std, meas_nonstd, conf).map_ok_value(FunctorOnce::fmap_into_once)
    }

    fn lookup_ro(
        kws: &StdKeywords,
        par: Par,
        conf: &ReadDataKeywordsConfig,
    ) -> LookupLayoutResult<NewLayout<Self>> {
        AnyOrderedLayout::lookup_ro(kws, par, conf).map_ok_value(FunctorOnce::fmap_into_once)
    }

    fn new_empty(datatype: AlphaNumType) -> Self {
        AnyOrderedLayout::new_empty(datatype).into()
    }

    fn try_new(
        datatype: AlphaNumType,
        byteord: Self::ByteLayout,
        columns: Vec<ColumnLayoutValues<Nothing<NumType>>>,
        conf: &ReadDataKeywordsConfig,
    ) -> WarningsAndErrorsResult<NewLayout<Self>, (), NewMixedTypeWarning, NewDataLayoutError> {
        AnyOrderedLayout::try_new(datatype, byteord, columns, conf)
            .map_ok_value(FunctorOnce::fmap_into_once)
    }
}

impl VersionedDataLayout for DataLayout3_1 {
    type ByteLayout = Endian;
    type NumType = Nothing<NumType>;
    type Tot = Identity<Tot>;

    fn lookup(
        std: &mut StdKeywords,
        meas_nonstd: &mut [NonStdKeywords],
        conf: &ReadDataKeywordsConfig,
    ) -> LookupLayoutResult<NewLayout<Self>> {
        NonMixedEndianLayout::lookup(std, meas_nonstd, conf)
            .map_ok_value(FunctorOnce::fmap_into_once)
    }

    fn lookup_ro(
        kws: &StdKeywords,
        par: Par,
        conf: &ReadDataKeywordsConfig,
    ) -> LookupLayoutResult<NewLayout<Self>> {
        NonMixedEndianLayout::lookup_ro(kws, par, conf).map_ok_value(FunctorOnce::fmap_into_once)
    }

    fn new_empty(datatype: AlphaNumType) -> Self {
        NonMixedEndianLayout::new_empty(datatype).into()
    }

    fn try_new(
        datatype: AlphaNumType,
        byteord: Self::ByteLayout,
        columns: Vec<ColumnLayoutValues<Nothing<NumType>>>,
        conf: &ReadDataKeywordsConfig,
    ) -> WarningsAndErrorsResult<NewLayout<Self>, (), NewMixedTypeWarning, NewDataLayoutError> {
        NonMixedEndianLayout::try_new(datatype, byteord, columns, conf)
            .map_ok_value(FunctorOnce::fmap_into_once)
    }
}

impl VersionedDataLayout for DataLayout3_2 {
    type ByteLayout = ByteOrd3_1;
    type NumType = Option<NumType>;
    type Tot = Identity<Tot>;

    fn lookup(
        std: &mut StdKeywords,
        meas_nonstd: &mut [NonStdKeywords],
        conf: &ReadDataKeywordsConfig,
    ) -> LookupLayoutResult<NewLayout<Self>> {
        let datatype = AlphaNumType::remove_metaroot_req(std);
        let endian = ByteOrd3_1::remove_metaroot_req(std);
        let columns = Option::lookup_all(std, meas_nonstd, conf);
        Self::lookup_inner(datatype, endian, columns, conf)
    }

    fn lookup_ro(
        kws: &StdKeywords,
        par: Par,
        conf: &ReadDataKeywordsConfig,
    ) -> LookupLayoutResult<NewLayout<Self>> {
        let datatype = AlphaNumType::get_metaroot_req(kws);
        let endian = ByteOrd3_1::get_metaroot_req(kws);
        let columns = Option::<NumType>::lookup_ro_all(kws, par, conf);
        Self::lookup_inner(datatype, endian, columns, conf)
    }

    fn new_empty(datatype: AlphaNumType) -> Self {
        NonMixedEndianLayout::new_empty(datatype).into()
    }

    fn try_new(
        datatype: AlphaNumType,
        byteord: Self::ByteLayout,
        columns: Vec<ColumnLayoutValues<Option<NumType>>>,
        conf: &ReadDataKeywordsConfig,
    ) -> WarningsAndErrorsResult<NewLayout<Self>, (), NewMixedTypeWarning, NewDataLayoutError> {
        let notrunc = conf.disallow_range_truncation;
        let unique_dt: Vec<_> = columns
            .iter()
            .map(|c| c.datatype.map_or(datatype, Into::into))
            .unique()
            .collect();
        match unique_dt[..] {
            // no columns, therefore undetermined datatype, use whatever the
            // default layout is
            [] => {
                let l = NonMixedEndianLayout::new_empty1(datatype, byteord.0).into();
                LogResult::new_ok(NewLayout::new(l, vec![]))
            }
            // has columns with one datatype, use nonmixed layout
            [dt] => {
                let ds =
                    columns.fmap(|c| ColumnLayoutValues::new(c.width, c.range, Nothing::default()));
                NonMixedEndianLayout::try_new(dt, byteord.0, ds, conf).map_ok_value(
                    |x: NewLayout<_>| {
                        x.fmap_once(|y: NonMixedEndianLayout<_>| {
                            Self::NonMixed(y.phantom_into::<Option<NumType>>())
                        })
                    },
                )
            }
            // has columns with 1+ datatypes, use mixed layout
            _ => {
                let go = |i: MeasIndex, c: ColumnLayoutValues3_2| {
                    AnyDatatype::from_width_and_range(
                        c.width, c.range, c.datatype, datatype, i, notrunc,
                    )
                };
                DataLayout::try_new(columns, byteord.0, go)
                    .map_errors(NewDataLayoutError::from)
                    .map_ok_value(FunctorOnce::fmap_into_once)
            }
        }
    }
}

impl ConvertFromLayout<DataLayout3_0> for DataLayout2_0 {
    fn convert_from_layout(value: DataLayout3_0) -> LayoutConvertResult<Self> {
        LogResult::new_ok(Self(value.0.phantom_into()))
    }
}

impl ConvertFromLayout<DataLayout3_1> for DataLayout2_0 {
    fn convert_from_layout(value: DataLayout3_1) -> LayoutConvertResult<Self> {
        value.into_ordered().map_ok_value(Into::into)
    }
}

impl ConvertFromLayout<DataLayout3_2> for DataLayout2_0 {
    fn convert_from_layout(value: DataLayout3_2) -> LayoutConvertResult<Self> {
        value.into_ordered().map_ok_value(Into::into)
    }
}

impl ConvertFromLayout<DataLayout2_0> for DataLayout3_0 {
    fn convert_from_layout(value: DataLayout2_0) -> LayoutConvertResult<Self> {
        LogResult::new_ok(Self(value.0.phantom_into()))
    }
}

impl ConvertFromLayout<DataLayout3_1> for DataLayout3_0 {
    fn convert_from_layout(value: DataLayout3_1) -> LayoutConvertResult<Self> {
        value.into_ordered().map_ok_value(Into::into)
    }
}

impl ConvertFromLayout<DataLayout3_2> for DataLayout3_0 {
    fn convert_from_layout(value: DataLayout3_2) -> LayoutConvertResult<Self> {
        value.into_ordered().map_ok_value(Into::into)
    }
}

impl ConvertFromLayout<DataLayout2_0> for DataLayout3_1 {
    fn convert_from_layout(value: DataLayout2_0) -> LayoutConvertResult<Self> {
        value.0.into_3_1()
    }
}

impl ConvertFromLayout<DataLayout3_0> for DataLayout3_1 {
    fn convert_from_layout(value: DataLayout3_0) -> LayoutConvertResult<Self> {
        value.0.into_3_1()
    }
}

impl ConvertFromLayout<DataLayout3_2> for DataLayout3_1 {
    fn convert_from_layout(mut value: DataLayout3_2) -> LayoutConvertResult<Self> {
        value.normalize();
        match value {
            DataLayout3_2::NonMixed(x) => LogResult::new_ok(Self(x.phantom_into())),
            DataLayout3_2::Mixed(x) => x.conversion_fail(),
        }
    }
}

impl ConvertFromLayout<DataLayout2_0> for DataLayout3_2 {
    fn convert_from_layout(value: DataLayout2_0) -> LayoutConvertResult<Self> {
        value.0.into_3_2()
    }
}

impl ConvertFromLayout<DataLayout3_0> for DataLayout3_2 {
    fn convert_from_layout(value: DataLayout3_0) -> LayoutConvertResult<Self> {
        value.0.into_3_2()
    }
}

impl ConvertFromLayout<DataLayout3_1> for DataLayout3_2 {
    fn convert_from_layout(value: DataLayout3_1) -> LayoutConvertResult<Self> {
        LogResult::new_ok(Self::NonMixed(value.0.phantom_into()))
    }
}

impl CheckedScaleTransform for Scale {
    type Err = ScaleMismatchError;
    type Summary = ScaleMismatchSummary;

    fn matches_datatype(&self, datatype: AlphaNumType, i: MeasIndex) -> Result<(), Self::Err> {
        if datatype != AlphaNumType::Integer && matches!(self, Self::Log(_)) {
            return Err(ScaleMismatchError::new(i, datatype, *self));
        }
        Ok(())
    }
}

impl CheckedScaleTransform for ScaleTransform {
    type Err = ScaleTransformMismatchError;
    type Summary = ScaleTransformMismatchSummary;

    fn matches_datatype(&self, datatype: AlphaNumType, i: MeasIndex) -> Result<(), Self::Err> {
        if datatype != AlphaNumType::Integer && !self.is_noop() {
            return Err(ScaleTransformMismatchError::new(i, datatype, *self));
        }
        Ok(())
    }
}

impl<C, L, T, D> Insertable<C> for DataLayout<C, L, T, D> {
    type Error = Infallible;

    fn insert_nocheck0(&mut self, index: MeasIndex, col: C) -> Result<(), Infallible> {
        self.inner.insert(index.into(), col);
        Ok(())
    }

    fn push0(&mut self, col: C) -> Result<(), Infallible> {
        self.inner.push(col);
        Ok(())
    }
}

impl<C: HasLen, L, T, D> Insertable<C> for DataFrame<C, L, T, D> {
    type Error = Infallible;

    fn insert_nocheck0(&mut self, index: MeasIndex, col: C) -> Result<(), Infallible> {
        self.inner.insert_column_nocheck(index.into(), col);
        Ok(())
    }

    fn push0(&mut self, col: C) -> Result<(), Infallible> {
        self.inner.push_column_nocheck(col);
        Ok(())
    }
}

impl Insertable<MixedRange> for DataLayout3_2 {
    type Error = Infallible;

    fn insert_nocheck0(&mut self, index: MeasIndex, col: MixedRange) -> Result<(), Infallible> {
        macro_rules! go_mixed {
            ($from:expr) => {{
                *self = Self::Mixed(mem::take($from).into_general());
                self.insert_nocheck0(index, col);
            }};
        }
        macro_rules! go {
            ($var:ident, $from:expr) => {
                if let AnyDatatype::$var(r) = col {
                    $from.inner.insert(index.into(), r.into());
                } else {
                    go_mixed!($from);
                }
            };
        }

        match self {
            Self::Mixed(x) => {
                x.insert_nocheck0(index, col);
            }
            Self::NonMixed(x) => match x {
                AnyDatatype::Ascii(y) => match y {
                    AnyAscii::Delimited(z) => go!(Ascii, z),
                    AnyAscii::Fixed(z) => go!(Ascii, z),
                },
                AnyDatatype::Uint(y) => match y {
                    AnyEndianUint::Single(z) => {
                        match_any_uint!(z, AnyUint, s, {
                            if let Ok(r) = col.clone().try_into() {
                                s.inner.insert(index.into(), r);
                            } else {
                                go_mixed!(z);
                            }
                        });
                    }
                    AnyEndianUint::Multi(z) => go!(Uint, z),
                },
                AnyDatatype::F32(y) => go!(F32, y),
                AnyDatatype::F64(y) => go!(F64, y),
            },
        }
        Ok(())
    }

    fn push0(&mut self, col: MixedRange) -> Result<(), Infallible> {
        macro_rules! go_mixed {
            ($from:expr) => {{
                *self = Self::Mixed(mem::take($from).into_general());
                self.push0(col);
            }};
        }
        macro_rules! go {
            ($var:ident, $from:expr) => {
                if let AnyDatatype::$var(r) = col {
                    $from.inner.push(r.into());
                } else {
                    go_mixed!($from);
                }
            };
        }

        match self {
            Self::Mixed(x) => {
                x.push0(col);
            }
            Self::NonMixed(x) => match x {
                AnyDatatype::Ascii(y) => match y {
                    AnyAscii::Delimited(z) => go!(Ascii, z),
                    AnyAscii::Fixed(z) => go!(Ascii, z),
                },
                AnyDatatype::Uint(y) => match y {
                    AnyEndianUint::Single(z) => {
                        match_any_uint!(z, AnyUint, s, {
                            if let Ok(r) = col.clone().try_into() {
                                s.inner.push(r);
                            } else {
                                go_mixed!(z);
                            }
                        });
                    }
                    AnyEndianUint::Multi(z) => go!(Uint, z),
                },
                AnyDatatype::F32(y) => go!(F32, y),
                AnyDatatype::F64(y) => go!(F64, y),
            },
        }
        Ok(())
    }
}

impl OptMeasLayoutKeywords for DataLayout3_2 {
    fn opt_meas_keywords(&self) -> Vec<Option<SplitKeyword1<NumType>>> {
        let dt = self.datatype();
        match self {
            Self::NonMixed(x) => vec![None; x.ncols()],
            Self::Mixed(x) => x
                .inner
                .iter()
                .enumerate()
                .map(|(i, c)| {
                    NumType::try_from(c.col_datatype())
                        .ok()
                        .and_then(|y| (AlphaNumType::from(y) != dt).then_some(y))
                        .map(|v| SplitKeyword1::from_value1(v, i))
                })
                .collect(),
        }
    }
}

impl DataLayout3_1 {
    pub(crate) fn into_ordered<T>(self) -> LayoutConvertResult<AnyOrderedLayout<T>> {
        self.0.try_nonmixed_into_ordered()
    }
}

impl DataLayout3_2 {
    pub(crate) fn into_ordered<T>(mut self) -> LayoutConvertResult<AnyOrderedLayout<T>> {
        self.normalize();
        match self {
            Self::NonMixed(x) => x.try_nonmixed_into_ordered(),
            Self::Mixed(x) => x.conversion_fail(),
        }
    }

    #[must_use]
    pub fn new_mixed(rs: Vec<MixedRange>, endian: Endian) -> Self {
        // Check if the mixed types are all the same, in which case we can use a
        // simpler layout. This clone thing is not ideal but it will only be
        // cloning big-decimals for floats and will use Copy for everything else
        // (not a huge deal).
        macro_rules! go {
            ($t:ident) => {
                rs.iter()
                    .map(|x| $t::try_from(x.clone()))
                    .collect::<Result<Vec<_>, _>>()
            };
        }
        if let Ok(xs) = go!(FixedAsciiRange) {
            NonMixedEndianLayout::new_ascii_fixed(xs).into()
        } else if let Ok(xs) = go!(AnyNullBitmask) {
            NonMixedEndianLayout::new_uint(xs, endian).into()
        } else if let Ok(xs) = go!(F32Range) {
            NonMixedEndianLayout::new_f32(xs, endian).into()
        } else if let Ok(xs) = go!(F64Range) {
            NonMixedEndianLayout::new_f64(xs, endian).into()
        } else {
            DataLayout::new(rs, endian).into()
        }
    }

    fn lookup_inner(
        datatype: Result<AlphaNumType, ReqKeyError<AlphaNumType>>,
        endian: Result<ByteOrd3_1, ReqKeyError<ByteOrd3_1>>,
        columns: LookupMeasLayoutResult<Option<NumType>>,
        conf: &ReadDataKeywordsConfig,
    ) -> LookupLayoutResult<NewLayout<Self>> {
        let endian_ = endian.map_err(LookupLayoutError::from).into_log();
        let columns_ = columns
            .map_commutative_warnings(LookupLayoutWarning::from)
            .map_errors(LookupLayoutError::Meas);
        datatype
            .map_err(LookupLayoutError::from)
            .into_log()
            .zip3_commutative(endian_, columns_)
            .and_then_commutative(|(d, e, cs)| {
                Self::try_new(d, e, cs, conf)
                    .map_commutative_warnings(LookupLayoutWarning::from)
                    .map_errors(LookupLayoutError::from)
            })
    }
}

impl<D> EndianUintLayout<D> {
    fn conversion_fail<X>(&self) -> LayoutConvertResult<X> {
        debug_assert!(!self.inner.is_empty(), "columns must be non-empty");
        let ((_, w0), ws) = self
            .widths()
            .try_into_nonempty_iter()
            .unwrap()
            .enumerate()
            .next();
        let es = ws
            .filter(|(_, w)| &w0 != w)
            .map(|(i, w)| IndexedError::new(i, UintToUintError::new(w.into(), w0.into())))
            .map(UintEndianToOrderedLayoutError::from)
            .map(LayoutConvertError::from)
            .try_into_nonempty_iter()
            .expect("mixed layout should have at least one different type");
        LogResult::new_from_ne_err_iter(es, ())
    }
}

impl MixedLayout {
    fn conversion_fail<X>(&self) -> LayoutConvertResult<X> {
        debug_assert!(!self.inner.is_empty(), "columns must be non-empty");
        let d0 = self.datatype();
        let es = self
            .inner
            .iter()
            .filter_map(|c| {
                let d = c.as_alpha_num_type();
                (d0 != d).then(|| MixedToNonMixedError::new(d, c.clone()))
            })
            .enumerate()
            .map(|(i, e)| IndexedError::new(i, e))
            .map(MixedToNonMixedLayoutError::from)
            .map(LayoutConvertError::from)
            .try_into_nonempty_iter()
            .expect("mixed layout should have at least one different type");
        LogResult::new_from_ne_err_iter(es, ())
    }
}

impl<T> Default for AnyOrderedLayout<T> {
    fn default() -> Self {
        Self::Uint(AnyOrderedUintLayout::default())
    }
}

impl<T> AnyOrderedLayout<T> {
    fn lookup(
        std: &mut StdKeywords,
        meas_nonstd: &mut [NonStdKeywords],
        conf: &ReadDataKeywordsConfig,
    ) -> LookupLayoutResult<NewLayout<Self>> {
        let datatype = AlphaNumType::remove_metaroot_req(std);
        let byteord = ByteOrd2_0::remove_metaroot_req(std);
        let columns = Nothing::lookup_all(std, meas_nonstd, conf);
        Self::lookup_inner(datatype, byteord, columns, conf)
    }

    fn lookup_ro(
        kws: &StdKeywords,
        par: Par,
        conf: &ReadDataKeywordsConfig,
    ) -> LookupLayoutResult<NewLayout<Self>> {
        let datatype = AlphaNumType::get_metaroot_req(kws);
        let byteord = ByteOrd2_0::get_metaroot_req(kws);
        let columns = Nothing::<NumType>::lookup_ro_all(kws, par, conf);
        Self::lookup_inner(datatype, byteord, columns, conf)
    }

    fn lookup_inner(
        datatype: Result<AlphaNumType, ReqKeyError<AlphaNumType>>,
        byteord: Result<ByteOrd2_0, ReqKeyError<ByteOrd2_0>>,
        columns: LookupMeasLayoutResult<Nothing<NumType>>,
        conf: &ReadDataKeywordsConfig,
    ) -> LookupLayoutResult<NewLayout<Self>> {
        let byteord_ = byteord.map_err(LookupLayoutError::from).into_log();
        let columns_ = columns
            .map_commutative_warnings(LookupLayoutWarning::from)
            .map_errors(LookupLayoutError::Meas);
        datatype
            .map_err(LookupLayoutError::from)
            .into_log()
            .zip3_commutative(byteord_, columns_)
            .and_then_commutative(|(d, e, cs)| {
                Self::try_new(d, e, cs, conf)
                    .map_commutative_warnings(LookupLayoutWarning::from)
                    .map_errors(LookupLayoutError::from)
            })
    }

    #[must_use]
    pub fn new_ascii_fixed(ranges: Vec<FixedAsciiRange>) -> Self {
        AnyAsciiLayout::new_fixed(ranges).into()
    }

    #[must_use]
    pub fn new_ascii_delim(ranges: Vec<DelimAsciiRange>) -> Self {
        AnyAsciiLayout::new_delim(ranges).into()
    }

    #[must_use]
    pub fn new_uint<U>(columns: Vec<Bitmask<U>>, byte_layout: ArrayByteOrd<U::ByteOrd>) -> Self
    where
        U: FCSRepr,
        AnyOrderedUintLayout<T>:
            From<DataLayout<Bitmask<U>, ArrayByteOrd<U::ByteOrd>, T, Nothing<NumType>>>,
    {
        Self::Uint(DataLayout::new(columns, byte_layout).into())
    }

    #[must_use]
    pub fn new_f32(ranges: Vec<F32Range>, byte_layout: ArrayByteOrd<[u8; 4]>) -> Self {
        DataLayout::new(ranges, byte_layout).into()
    }

    #[must_use]
    pub fn new_f64(ranges: Vec<F64Range>, byte_layout: ArrayByteOrd<[u8; 8]>) -> Self {
        DataLayout::new(ranges, byte_layout).into()
    }

    fn new_empty(datatype: AlphaNumType) -> Self {
        match datatype {
            AlphaNumType::Ascii => AnyAsciiLayout::default().into(),
            AlphaNumType::Integer => AnyOrderedUintLayout::default().into(),
            AlphaNumType::Float => Self::F32(DataLayout::default()),
            AlphaNumType::Double => Self::F64(DataLayout::default()),
        }
    }

    fn try_new(
        datatype: AlphaNumType,
        byteord: ByteOrd2_0,
        columns: Vec<ColumnLayoutValues2_0>,
        conf: &ReadDataKeywordsConfig,
    ) -> WarningsAndErrorsResult<NewLayout<Self>, (), NewMixedTypeWarning, NewDataLayoutError> {
        macro_rules! from {
            ($i:expr) => {
                $i.map_errors(NewDataLayoutError::from)
                    .map_commutative_warnings(NewMixedTypeWarning::from)
                    .map_ok_value(FunctorOnce::fmap_into_once)
            };
        }

        macro_rules! go_float {
            ($t:ident, $notrunc:expr) => {
                byteord
                    .try_into()
                    .map_err(NewDataLayoutError::from)
                    .into_log()
                    .and_then_commutative(|b| {
                        from! {DataLayout::try_new(columns, b, |i, c| {
                            $t::from_width_and_range(c.width, c.range, i, $notrunc)
                                .repack_errors()
                        })}
                    })
            };
        }

        let notrunc = conf.disallow_range_truncation;

        match datatype {
            AlphaNumType::Ascii => from!(AnyAsciiLayout::try_new(columns, notrunc)),
            AlphaNumType::Integer => from!(AnyOrderedUintLayout::try_new(columns, byteord, conf)),
            AlphaNumType::Float => go_float!(F32Range, notrunc),
            AlphaNumType::Double => go_float!(F64Range, notrunc),
        }
    }

    #[must_use]
    pub fn phantom_into<X>(self) -> AnyOrderedLayout<X> {
        match_any_mixed!(self, x, { x.phantom_into().into() })
    }

    pub fn into_unmixed<D>(self) -> LayoutConvertResult<NonMixedEndianLayout<D>> {
        macro_rules! go_float {
            ($i:expr) => {
                $i.phantom_into()
                    .byte_layout_try_into()
                    .map(NonMixedEndianLayout::from)
                    .into_log::<_, _, Vec<_>>()
            };
        }
        let res = match self {
            Self::Ascii(x) => LogResult::new_ok(NonMixedEndianLayout::from(x.phantom_into())),
            Self::Uint(x) => x
                .into_endian()
                .map(AnyEndianUintLayout::from)
                .map(NonMixedEndianLayout::from)
                .into_log(),
            Self::F32(x) => go_float!(x),
            Self::F64(x) => go_float!(x),
        };
        res.map_errors(LayoutConvertError::from)
    }

    pub(crate) fn into_3_1(self) -> LayoutConvertResult<DataLayout3_1> {
        self.into_unmixed().map_ok_value(Into::into)
    }

    pub(crate) fn into_3_2(self) -> LayoutConvertResult<DataLayout3_2> {
        self.into_unmixed().map_ok_value(DataLayout3_2::NonMixed)
    }
}

impl NonMixedEndianLayout<Nothing<NumType>> {
    fn lookup(
        std: &mut StdKeywords,
        meas_nonstd: &mut [NonStdKeywords],
        conf: &ReadDataKeywordsConfig,
    ) -> LookupLayoutResult<NewLayout<Self>> {
        let datatype = AlphaNumType::remove_metaroot_req(std);
        let endian = ByteOrd3_1::remove_metaroot_req(std);
        let columns = Nothing::<NumType>::lookup_all(std, meas_nonstd, conf);
        Self::lookup_inner(datatype, endian, columns, conf)
    }

    fn lookup_ro(
        kws: &StdKeywords,
        par: Par,
        conf: &ReadDataKeywordsConfig,
    ) -> LookupLayoutResult<NewLayout<Self>> {
        let datatype = AlphaNumType::get_metaroot_req(kws);
        let endian = ByteOrd3_1::get_metaroot_req(kws);
        let columns = Nothing::<NumType>::lookup_ro_all(kws, par, conf);
        Self::lookup_inner(datatype, endian, columns, conf)
    }

    fn lookup_inner(
        datatype: Result<AlphaNumType, ReqKeyError<AlphaNumType>>,
        endian: Result<ByteOrd3_1, ReqKeyError<ByteOrd3_1>>,
        columns: LookupMeasLayoutResult<Nothing<NumType>>,
        conf: &ReadDataKeywordsConfig,
    ) -> LookupLayoutResult<NewLayout<Self>> {
        let endian_ = endian.map_err(LookupLayoutError::from).into_log();
        let columns_ = columns
            .map_commutative_warnings(LookupLayoutWarning::from)
            .map_errors(LookupLayoutError::Meas);
        datatype
            .map_err(LookupLayoutError::from)
            .into_log()
            .zip3_commutative(endian_, columns_)
            .and_then_commutative(|(d, e, cs)| {
                Self::try_new(d, e.0, cs, conf)
                    .map_commutative_warnings(LookupLayoutWarning::from)
                    .map_errors(LookupLayoutError::from)
            })
    }

    fn try_new(
        datatype: AlphaNumType,
        endian: Endian,
        columns: Vec<ColumnLayoutValues<Nothing<NumType>>>,
        conf: &ReadDataKeywordsConfig,
    ) -> WarningsAndErrorsResult<NewLayout<Self>, (), NewMixedTypeWarning, NewDataLayoutError> {
        let notrunc = conf.disallow_range_truncation;

        let go_f32 = |i: MeasIndex, c: ColumnLayoutValues<_>| {
            F32Range::from_width_and_range(c.width, c.range, i, notrunc).repack_errors()
        };

        let go_f64 = |i: MeasIndex, c: ColumnLayoutValues<_>| {
            F64Range::from_width_and_range(c.width, c.range, i, notrunc).repack_errors()
        };

        macro_rules! from {
            ($x:expr) => {
                $x.map_errors(NewDataLayoutError::from)
                    .map_commutative_warnings(NewMixedTypeWarning::from)
                    .map_ok_value(FunctorOnce::fmap_into_once)
            };
        }

        match datatype {
            AlphaNumType::Ascii => from!(AnyAsciiLayout::try_new(columns, notrunc)),
            AlphaNumType::Integer => {
                from!(AnyEndianUintLayout::try_new(columns, endian, notrunc))
            }
            AlphaNumType::Float => from!(DataLayout::try_new(columns, endian, go_f32)),
            AlphaNumType::Double => from!(DataLayout::try_new(columns, endian, go_f64)),
        }
    }
}

impl<D> NonMixedEndianLayout<D> {
    fn new_empty(datatype: AlphaNumType) -> Self {
        Self::new_empty1(datatype, Endian::default())
    }

    fn new_empty1(datatype: AlphaNumType, endian: Endian) -> Self {
        match datatype {
            AlphaNumType::Ascii => AnyAsciiLayout::default().into(),
            AlphaNumType::Integer => Self::Uint(AnyEndianUint::Single(AnyUint::Uint32(
                DataLayout::new_empty(endian),
            ))),
            AlphaNumType::Float => Self::F32(DataLayout::new_empty(endian)),
            AlphaNumType::Double => Self::F64(DataLayout::new_empty(endian)),
        }
    }

    #[must_use]
    pub fn new_ascii_fixed(ranges: impl IntoIterator<Item = FixedAsciiRange>) -> Self {
        AnyAsciiLayout::new_fixed(ranges).into()
    }

    #[must_use]
    pub fn new_ascii_delim(ranges: Vec<DelimAsciiRange>) -> Self {
        AnyAsciiLayout::new_delim(ranges).into()
    }

    // TODO make fixed width versions of this?

    #[must_use]
    pub fn new_uint(columns: Vec<AnyNullBitmask>, endian: Endian) -> Self {
        AnyEndianUint::Multi(DataLayout::new(columns, endian)).into()
    }

    #[must_use]
    pub fn new_f32(ranges: Vec<F32Range>, endian: Endian) -> Self {
        DataLayout::new(ranges, endian).into()
    }

    #[must_use]
    pub fn new_f64(ranges: Vec<F64Range>, endian: Endian) -> Self {
        DataLayout::new(ranges, endian).into()
    }

    pub(crate) fn try_nonmixed_into_ordered<T>(
        mut self,
    ) -> LayoutConvertResult<AnyOrderedLayout<T>> {
        self.normalize();
        match self {
            Self::Ascii(x) => LogResult::new_ok(AnyDatatype::Ascii(x.phantom_into())),
            Self::Uint(x) => match x {
                AnyEndianUint::Multi(y) => y.conversion_fail(),
                AnyEndianUint::Single(y) => match_any_uint!(y, AnyUint, z, {
                    LogResult::new_ok(AnyDatatype::Uint(
                        z.phantom_into().byte_layout_into().into(),
                    ))
                }),
            },
            Self::F32(x) => LogResult::new_ok(x.phantom_into().byte_layout_into().into()),
            Self::F64(x) => LogResult::new_ok(x.phantom_into().byte_layout_into().into()),
        }
    }

    #[must_use]
    pub fn phantom_into<D1>(self) -> NonMixedEndianLayout<D1> {
        match_many_to_one!(self, Self, [Ascii, Uint, F32, F64], x, {
            x.phantom_into().into()
        })
    }
}

/// Error when keywords cannot be used to make a new layout.
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum NewDataLayoutError {
    /// $PnB and $PnR could not be used to make ASCII column
    Ascii(AsciiRangeFromKeywordsError),
    /// $PnB and $PnR could not be used to make integer column (2.0/3.0)
    FixedInt(NewFixedIntLayoutError),
    /// $PnB and $PnR could not be used to make integer column (3.1/3.2)
    VariableInt(NewUintTypeError),
    /// $PnB and $PnR could not be used to make float column
    Float(FloatWidthError),
    /// $PnB and $PnR could not be used to make mixed column (3.2)
    Mixed(NewMixedTypeError),
    /// $BYTEORD does not match width allowed via $DATATYPE for float layout (2.0/3.0)
    ByteOrd(ByteOrdToSizedError),
}

/// Error when $PnB or $PnR cannot be used for an [`AnyOrderedUintLayout`] (2.0/3.0)
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum NewFixedIntLayoutError {
    Width(SingleFixedWidthError),
    Column(IndexedBitmaskError),
}

/// Error when $PnB cannot be used for an ordered integer layout (2.0/3.0 only)
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum SingleFixedWidthError {
    Bytes(IndexedWidthToBytesError),
    Width(WidthMismatchError),
}

/// Error when $PnB does not match width implied by $BYTEORD (2.0/3.0 only)
#[derive(Debug, Error, new)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct WidthMismatchError {
    byteord: ByteOrd2_0,
    found: NEVec<PrivBytes>,
}

impl fmt::Display for WidthMismatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let (head, tail) = self.found.nonempty_iter().next();
        let mut t = tail.peekable();
        if t.peek().is_none() {
            write!(
                f,
                "measurement width ({head}) does not match byte order ({})",
                self.byteord.as_displayable(),
            )
        } else {
            write!(
                f,
                "multiple measurement widths given ({}) for byte order [{}]",
                once(head).chain(t).into_iter().join(", "),
                self.byteord.as_displayable(),
            )
        }
    }
}

/// Error when using $PnB and $PnR to make a new [`MixedType`].
///
/// This only applies to FCS 3.2 and the value of $PnDATATYPE is implied by
/// the variant of this enum.
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum NewMixedTypeError {
    Ascii(AsciiRangeFromKeywordsError),
    Uint(NewUintTypeError),
    Float(FloatWidthError),
}

/// Warning when failing to truncate $PnR for use in a [`DataLayout3_2`].
#[derive(From, Display, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum NewMixedTypeWarning {
    Ascii(IndexedRangeToAsciiError),
    Uint(IndexedBitmaskError),
    Float(IndexedFloatRangeError),
}

/// Error when converting $PnR to float to be used in a float layout.
#[derive(From, Debug, Error)]
#[error(
    "could not use {k} in float layout because {e}",
    k = Range::std(_0.index),
    e = _0.error
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct IndexedFloatRangeError(IndexedError<DecimalToFloatError>);

/// Error when using $PnB or $PnR to make a new [`Bitmask`]
#[derive(From, Display, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum NewUintTypeError {
    Bitmask(IndexedBitmaskError),
    Bytes(IndexedWidthToBytesError),
}

/// Error when converting $PnB (in bits) to [`Bytes`]
#[derive(From, Debug, Error)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct IndexedWidthToBytesError(IndexedError<WidthToFixedError<WidthToBytesError>>);

impl fmt::Display for IndexedWidthToBytesError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let k = Width::std(self.0.index);
        match &self.0.error {
            WidthToFixedError::Fixed(e) => {
                write!(f, "could not convert {k} to bytes because {e}")
            }
            WidthToFixedError::Variable(_) => {
                write!(f, "{k} is variable ('*') when fixed is needed")
            }
        }
    }
}

/// Error when using $PnB or $PnR for float layout.
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum FloatWidthError {
    Bytes(IndexedWidthToBytesError),
    WrongWidth(WrongFloatWidth),
    Range(IndexedFloatRangeError),
}

/// Error when converting $PnR to [`Bitmask`] for integer layout based on $PnB.
#[derive(From, Debug, Error)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct IndexedBitmaskError(IndexedError<RangeToBitmaskError>);

impl fmt::Display for IndexedBitmaskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let i = self.0.index;
        let rng = Range::std(i);
        let width = Width::std(i);
        let e = match &self.0.error {
            RangeToBitmaskError::Over(v, b) => {
                format!("{v} cannot fit into {b} bytes set by {width}")
            }
            RangeToBitmaskError::Under(v) => {
                format!("{v} is less than zero")
            }
            RangeToBitmaskError::Float(v) => {
                format!("{v} would has decimal precision which would be lost")
            }
        };
        write!(f, "could not make bitmask from {rng} because {e}")
    }
}

/// Inner error for [`RangeToBitmaskError`] without the index
///
/// This is necessary to translate from the more general RangeToIntError to add
/// integer-layout-specific context. Furthermore, it subsumes
/// BitmaskTruncationError since this is a special case of $PnR not fitting into
/// a fixed number of bytes, where the bytes in this case happen to not align
/// with native datatypes (u8, u16, etc).
#[derive(Debug)]
pub enum RangeToBitmaskError {
    Over(BigDecimal, Bytes),
    Under(BigDecimal),
    Float(BigDecimal),
}

impl From<BitmaskTruncationError> for RangeToBitmaskError {
    fn from(value: BitmaskTruncationError) -> Self {
        Self::Over(BigDecimal::from(value.value), Bytes(value.bytes))
    }
}

impl<T> From<RangeToIntError<T>> for RangeToBitmaskError {
    fn from(value: RangeToIntError<T>) -> Self {
        let b = Bytes(value.dest_type);
        let v = value.src_value;
        match value.error_kind {
            RangeToIntErrorKind::Overrange => Self::Over(v, b),
            RangeToIntErrorKind::Underrange => Self::Under(v),
            RangeToIntErrorKind::PrecisionLoss(_) => Self::Float(v),
        }
    }
}

/// Error when converting $PnR to integer range for ASCII layout.
///
/// An error will occur if $PnR exceeds the upper limit of a 64-bit unsigned
/// integer. This is effectively a special case of $PnR to bitmask conversion.
///
/// Note, nothing bad will happen if $PnR exceeds the number of characters
/// set by $PnB.
#[derive(From, Debug, Error)]
#[error(
    "{k} could not be converted to integer ASCII upper bound because {e}",
    k = Range::std(_0.index),
    e = _0.error,
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct IndexedRangeToAsciiError(pub(crate) IndexedError<RangeToAsciiError>);

/// Inner error for [`IndexedRangeToAsciiError`] without the index
#[derive(Debug, Error)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::InvalidKeywordValueError))]
pub enum RangeToAsciiError {
    #[error("its value {0} cannot be represented with 8 bytes")]
    Over(BigDecimal),
    #[error("its value {0} is less than zero")]
    Under(BigDecimal),
    #[error("its value {0} has decimal precision which will be lost")]
    Float(BigDecimal),
}

impl<T> From<RangeToIntError<T>> for RangeToAsciiError {
    fn from(value: RangeToIntError<T>) -> Self {
        let v = value.src_value;
        match value.error_kind {
            RangeToIntErrorKind::Overrange => Self::Over(v),
            RangeToIntErrorKind::Underrange => Self::Under(v),
            RangeToIntErrorKind::PrecisionLoss(_) => Self::Float(v),
        }
    }
}

/// Error when checking $PnB for float layouts.
///
/// All $PnB should be 32 or 64 depending on $DATATYPE for these layouts.
#[derive(Debug, Display, new)]
#[display(
    "expected {k} to be {expected} but got {width} when determining float type",
    k = Range::std(self.index),
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct WrongFloatWidth {
    width: PrivBytes,
    expected: usize,
    index: MeasIndex,
}

/// Any error when computing event width for fixed-width layout
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum EventWidthError {
    Zero(ZeroEventWidthError),
    Uneven(UnevenEventWidthError),
}

/// Error when fixed-width layout does not evenly divide the length of DATA.
#[derive(Error, Debug, new)]
#[error(
    "Events are {event_width} bytes wide, but this does not evenly divide \
     DATA segment which is {nbytes} bytes long (remainder of {remainder})"
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct UnevenEventWidthError {
    event_width: u64,
    nbytes: u64,
    remainder: u64,
}

/// Error when fixed layout is empty which precludes computing event number.
#[derive(Error, Debug, new)]
#[error("DATA segment is {event_width} bytes but event width is zero")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct ZeroEventWidthError {
    event_width: u64,
}

/// Error when value is truncated when writing DATA with index
#[derive(From, Debug, Error)]
#[error("{e} in column {i}", e = _0.error, i = _0.index)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::DataLossError))]
pub struct IndexedLossError(IndexedError<AnyLossError>);

/// Error when value is truncated when writing DATA
#[derive(From, Display, Debug)]
pub(crate) enum AnyLossError {
    // Int(LossError<BitmaskLossError>),
    // Float(LossError<Infallible>),
    // Ascii(LossError<AsciiLossError>),
}

// /// Error when ASCII value is truncated to fewer chars when writing DATA
// #[derive(Clone, Copy, Debug, Error)]
// #[error("ASCII data truncated to {0} chars")]
// pub(crate) struct AsciiLossError(Chars);

type LookupLayoutResult<T> = WarningsAndErrorsResult<T, (), LookupLayoutWarning, LookupLayoutError>;

/// Error when looking up layout from key/value pairs
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupLayoutError {
    New(NewDataLayoutError),
    AlphaNumType(ReqKeyError<AlphaNumType>),
    ByteOrd2_0(ReqKeyError<ByteOrd2_0>),
    ByteOrd3_1(ReqKeyError<ByteOrd3_1>),
    Meas(LookupMeasLayoutError),
}

/// Warning when looking up layout from key/value pairs
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupLayoutWarning {
    New(NewMixedTypeWarning),
    Datatype(ReqKeyError<AlphaNumType>),
    Meas(OptIndexedKeyError<NumType>),
}

type LookupMeasLayoutResult<T> = WarningsAndErrorsResult<
    Vec<ColumnLayoutValues<T>>,
    (),
    OptIndexedKeyError<NumType>,
    LookupMeasLayoutError,
>;

type LookupOneMeasLayoutResult<T> = WarningsAndErrorsResult<
    ColumnLayoutValues<T>,
    (),
    OptIndexedKeyError<NumType>,
    LookupMeasLayoutError,
>;

/// Error when looking up measurement for layout from key/value pairs
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupMeasLayoutError {
    Width(ReqIndexedKeyError<Width>),
    Range(ReqIndexedKeyError<Range>),
    NumType(OptIndexedKeyError<NumType>),
}

/// Error when reading DATA segment
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ReadDataframeError {
    Ascii(ReadAsciiError),
    Width(EventWidthError),
    TotMismatch(TotEventMismatchError),
    Overrange(EventOverRangeErrors),
}

/// Warning when reading DATA segment
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ReadDataframeWarning {
    Uneven(UnevenEventWidthError),
    Tot(TotEventMismatchError),
    Overrange(EventOverRangeError),
}

/// Error when event value is above its $PnR
#[derive(Debug, Display, new)]
#[display(
    "event value in column {column} and row {row}, exceeds $PnR ({})",
    range.as_displayable()
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::EventDataError))]
pub struct EventOverRangeError {
    row: usize,
    column: MeasIndex,
    range: Range,
}

def_summary!(EventOverRangeSummary, "some events exceed $PnR");

pub type EventOverRangeErrors = ErrorGroup<EventOverRangeError, EventOverRangeSummary>;

/// Error when reading [`AnyAsciiLayout`]
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ReadAsciiError {
    Delim(ReadDelimAsciiError),
    Fixed(ReadFixedAsciiError),
}

/// Error when reading [`FixedAsciiLayout`]
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ReadFixedAsciiError {
    Uneven(UnevenEventWidthError),
    Tot(TotEventMismatchError),
    ToUint(AsciiToUintError),
}

/// Error when reading event value in ASCII layout
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::EventDataError))]
pub enum AsciiToUintError {
    NotAscii(NotAsciiError),
    Int(ParseIntError),
}

/// Error when encountering characters when parsing DATA as ASCII
#[derive(Debug, Display)]
#[display("bytestring is not valid ASCII: {_0:?}")]
pub struct NotAsciiError(Vec<u8>);

/// Error when $TOT mismatches with number of computed events for DATA.
///
/// This is only applicable to fixed width layouts because their width is used
/// to compute the number of events in DATA.
#[derive(Error, Debug)]
#[error(
    "$TOT field is {tot} but number of events that \
     evenly fit into DATA is {total_events}"
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct TotEventMismatchError {
    tot: Tot,
    total_events: u64,
}

/// Error when reading [`DelimAsciiLayout`] (with or without $TOT)
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ReadDelimAsciiError {
    Rows(ReadDelimWithRowsAsciiError),
    NoRows(ReadDelimAsciiWithoutRowsError),
    NoColumns(ReadDelimNoColumnError),
}

/// Error when ASCII layout has no columns but segment length is nonzero
#[derive(Debug, Error)]
#[error("No columns given for ASCII layout but DATA segment is non-empty")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct ReadDelimNoColumnError;

/// Error when reading [`DelimAsciiLayout`] with $TOT.
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ReadDelimWithRowsAsciiError {
    RowsExceeded(RowsExceededError),
    Incomplete(DelimIncompleteError),
    Parse(AsciiToUintError),
}

/// Error when reading [`DelimAsciiLayout`] where DATA is exhausted.
///
/// This happens if $TOT is greater than the true number of values in DATA.
#[derive(Debug, Error)]
#[error("Exceeded expected number of rows: {0}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct RowsExceededError(usize);

/// Error when reading [`DelimAsciiLayout`] where parsing ends unexpectedly.
///
/// This happens if $TOT is less than the true number of values in DATA.
#[derive(Debug, Error)]
#[error(
    "Parsing ended in column {c} and row {r}, \
     where expected number of rows is {nrows}",
    c = self.col + 1,
    r = self.row + 1

)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct DelimIncompleteError {
    col: usize,
    row: usize,
    nrows: usize,
}

/// Error when reading [`DelimAsciiLayout`] without $TOT
#[derive(From, Debug, Display, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ReadDelimAsciiWithoutRowsError {
    Parse(AsciiToUintError),
    Unequal(ReadDelimAsciiUnequalColumnsError),
}

/// Error when reading [`DelimAsciiLayout`] where columns are not equal length
#[derive(Debug, Error)]
#[error("parsing delimited ASCII without $TOT resulted in columns with unequal length")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct ReadDelimAsciiUnequalColumnsError;

pub(crate) type LayoutConvertResult<L> = ErrorsResult<L, (), LayoutConvertError>;

/// Error when converting between layout versions.
///
/// Some conversions are infallible:
/// * ASCII layouts are interchangeable between any version
/// * all non-mixed 3.2 layouts are interchangeable with 3.1 layouts
/// * all 2.0 layouts are interchangeable with 3.0 layouts
/// * 3.1/3.2 float layouts perfectly downgrade to 2.0/3.0 float layouts
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LayoutConvertError {
    /// Any 2.0/3.0 non-ASCII layout to 3.1/3.2
    OrderToEndian(OrderedToEndianError),
    /// 3.1/3.2 integer layout to 2.0/3.0 integer layout
    Width(UintEndianToOrderedLayoutError),
    /// 3.2 mixed layout to a 2.0/3.0 ordered uint layout
    MixedToOrdered(MixedToOrderedLayoutError),
    /// 3.2 mixed layout to a 3.1/3.2 non-mixed layout.
    MixedToNonMixed(MixedToNonMixedLayoutError),
}

/// Error when converting a [`NonMixedEndianLayout`] to [`AnyOrderedUintLayout`]
///
/// This arises due to 3.1+ layouts being allowed to support any width and
/// 2.0/3.0 layouts only supporting one width due to the $BYTEORD constraint.
#[derive(From, Debug, Error)]
#[error(
    "{b} and {r} encoding {from}-byte integers are incompatible with {to}-byte integer layout",
    from = _0.error.from,
    to = _0.error.to,
    b = Width::std(_0.index),
    r = Range::std(_0.index),
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConversionError))]
pub struct UintEndianToOrderedLayoutError(IndexedError<UintToUintError>);

/// Error when converting a [`DataLayout3_2`] to a [`NonMixedEndianLayout`]
///
/// This will fail due to type mismatches (A, I, F, or D), since the width for
/// integer layouts is allowed to vary.
#[derive(From, Debug, Error)]
#[error(
    "{b} and {r} when {p}='{from}' are incompatible in layout with $DATATYPE='{to}'",
    from = _0.error.src.as_alpha_num_type().as_displayable(),
    to = _0.error.dest_type.as_displayable(),
    p = NumType::std(_0.index),
    b = Width::std(_0.index),
    r = Range::std(_0.index),
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConversionError))]
pub struct MixedToNonMixedLayoutError(IndexedError<MixedToNonMixedError>);

/// Error when converting [`DataLayout3_2`] to [`AnyOrderedLayout`]
///
/// This can fail either because of a type mismatch (ie Float vs Integer) or
/// because the width is incorrect if the mixed layout has integer columns.
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum MixedToOrderedLayoutError {
    Integer(UintEndianToOrderedLayoutError),
    Other(MixedToNonMixedLayoutError),
}

/// [`MixedToOrderedLayoutError`] without the index.
///
/// Used for [`TryFrom`] impl's where the index is not known
#[derive(From)]
pub enum MixedToOrderedUintError {
    Integer(UintToUintError),
    Other(MixedToNonMixedError),
}

// impl MixedToOrderedUintError {
//     fn into_col_error(self, i: MeasIndex) -> MixedToOrderedLayoutError {
//         match self {
//             Self::Integer(e) => UintEndianToOrderedLayoutError(IndexedError::new(i, e)).into(),
//             Self::Other(e) => MixedToNonMixedLayoutError(IndexedError::new(i, e)).into(),
//         }
//     }
// }

/// Error when converting between [`Bitmask`]s with different byte-widths.
#[derive(Debug, new)]
pub struct UintToUintError {
    from: NonZeroU8,
    to: NonZeroU8,
}

/// Error when $PnDATATYPE of a column does not match $DATATYPE in a new layout.
#[derive(Debug, new)]
pub struct MixedToNonMixedError {
    dest_type: AlphaNumType,
    src: MixedRange,
}

/// Error when attempting to insert new [`Range`] into a layout.
#[derive(From, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum InsertRangeError {
    #[error("could not insert range into ASCII layout because {0}")]
    #[from(RangeToAsciiError)]
    Ascii(RangeToAsciiError),
    #[error("could not insert range into integer layout because {0}")]
    #[from(RangeToBitmaskError)]
    Int(RangeToNewBitmaskError),
    #[error("could not insert range into float layout because {0}")]
    #[from(DecimalToFloatError)]
    Float(DecimalToFloatError),
}

/// Inner error for converting [`Range`] to [`Bitmask`]
///
/// This is separate from RangeToBitmaskError since we need different error
/// messages here given that $PnR and $PnB do not apply to newly supplied ranges.
#[derive(From, Debug)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::InvalidKeywordValueError))]
pub struct RangeToNewBitmaskError(RangeToBitmaskError);

impl fmt::Display for RangeToNewBitmaskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match &self.0 {
            RangeToBitmaskError::Over(v, b) => {
                write!(f, "{v} cannot fit into {b} bytes as constrained by layout")
            }
            RangeToBitmaskError::Under(v) => {
                write!(f, "{v} is less than zero")
            }
            RangeToBitmaskError::Float(v) => {
                write!(f, "{v} has decimal precision which would be lost")
            }
        }
    }
}

/// Error when layout and measurement vector do not match.
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum MeasLayoutMismatchError {
    Lengths(MeasLayoutLengthsError),
    Scale(ScaleDatatypeMismatchError),
}

/// Error when scales do not match datatypes in layout.
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ScaleDatatypeMismatchError {
    Scale(ScaleMismatchErrors),
    ScaleTransform(ScaleTransformMismatchErrors),
}

/// Error when measurement vector and layout have different lengths.
#[derive(Debug, Error)]
#[error("measurement number ({meas_n}) does not match layout column number ({layout_n})")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct MeasLayoutLengthsError {
    meas_n: usize,
    layout_n: usize,
}

pub type ScaleErrorGroup<M> = ErrorGroup<
    <<<M as VersionedMetaroot>::Optical as AsScaleOrTransform>::S as CheckedScaleTransform>::Err,
    <<<M as VersionedMetaroot>::Optical as AsScaleOrTransform>::S as CheckedScaleTransform>::Summary,
>;

pub type ScaleMismatchErrors = ErrorGroup<ScaleMismatchError, ScaleMismatchSummary>;

def_summary!(
    ScaleMismatchSummary,
    "mismatch between scale and column datatypes"
);

pub type ScaleTransformMismatchErrors =
    ErrorGroup<ScaleTransformMismatchError, ScaleTransformMismatchSummary>;

def_summary!(
    ScaleTransformMismatchSummary,
    "mismatch between scale transforms and column datatypes"
);

/// Error when attempting to make a new measurement vector given a layout.
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum MeasurementsWithLayoutError {
    New(NewNamedVecError),
    Layout(MeasLayoutMismatchError),
}

/// Error when $PnE does not match the datatype in its corresponding column (2.0)
#[derive(Debug, Error, new)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct ScaleMismatchError {
    index: MeasIndex,
    datatype: AlphaNumType,
    scale: Scale,
}

impl fmt::Display for ScaleMismatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let i = self.index;
        let ekey = Scale::std(i);
        let dt = self.datatype.as_displayable();
        let eval = self.scale.as_displayable();
        write!(
            f,
            "only integer columns may have non-linear scale, \
             column is '{dt}' where {ekey} is '{eval}'"
        )
    }
}

/// Error when $PnE/$PnG do not match the datatype in the corresponding column (3.0+)
#[derive(Debug, Error, new)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct ScaleTransformMismatchError {
    index: MeasIndex,
    datatype: AlphaNumType,
    scale: ScaleTransform,
}

impl fmt::Display for ScaleTransformMismatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let i = self.index;
        let ekey = Scale::std(i);
        let gkey = Gain::std(i);
        let dt = self.datatype.as_displayable();
        let (eval, g): (Scale, Option<Gain>) = self.scale.into();
        let gval = g.map_or("not set".into(), |s| format!("'{}'", s.as_displayable()));
        write!(
            f,
            "only integer columns may have non-unitary scale transforms, \
             column is '{dt}' where {ekey} is '{}' and {gkey} is {gval}",
            eval.as_displayable(),
        )
    }
}

/// Inner helper type to add index data to an error message.
///
/// This does not implement any error-specific functions on its own because
/// the index will be used in a context-specific manner.
#[derive(new, Debug)]
pub(crate) struct IndexedError<E> {
    #[new(into)]
    pub(crate) index: IndexFromOne,
    pub(crate) error: E,
}

fn u64_to_usize(x: u64) -> usize {
    usize::try_from(x).expect("overflow")
}

fn usize_to_u64(x: usize) -> u64 {
    u64::try_from(x).expect("overflow")
}

#[cfg(feature = "python")]
mod python {
    use super::{AnyNullBitmask, FloatRange, MixedRange};

    use crate::text::float_decimal::{FloatDecimal, HasFloatBounds};
    use crate::text::keywords::AlphaNumType;
    use crate::validated::ascii_range::{AsciiRangeValue, FixedAsciiRange};
    use crate::validated::bitmask::BitmaskValue;

    use fireflow_types::python::InvalidKeywordValueError;

    use bigdecimal::BigDecimal;
    use pyo3::conversion::FromPyObjectBound;
    use pyo3::prelude::*;
    use pyo3::types::PyTuple;

    impl<'py, T> FromPyObject<'py> for FloatRange<T>
    where
        for<'a> T: FromPyObjectBound<'a, 'py> + HasFloatBounds,
        FloatDecimal<T>: TryFrom<BigDecimal>,
        PyErr: From<<FloatDecimal<T> as TryFrom<BigDecimal>>::Error>,
    {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let x = ob.extract::<BigDecimal>()?;
            Ok(FloatDecimal::try_from(x).map(Self::new)?)
        }
    }

    impl<'py, T> IntoPyObject<'py> for FloatRange<T> {
        type Target = PyAny;
        type Output = Bound<'py, PyAny>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            BigDecimal::from(self.range).into_pyobject(py)
        }
    }

    impl<'py> FromPyObject<'py> for MixedRange {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let (datatype, value): (AlphaNumType, Bound<'py, PyAny>) = ob.extract()?;
            match datatype {
                AlphaNumType::Float => {
                    let x = value.extract::<f32>()?;
                    let y = FloatDecimal::try_from(x)
                        .map_err(|e| InvalidKeywordValueError::new_err(e.to_string()))?;
                    Ok(FloatRange::new(y).into())
                }
                AlphaNumType::Double => {
                    let x = value.extract::<f64>()?;
                    let y = FloatDecimal::try_from(x)
                        .map_err(|e| InvalidKeywordValueError::new_err(e.to_string()))?;
                    Ok(FloatRange::new(y).into())
                }
                AlphaNumType::Integer => {
                    Ok(AnyNullBitmask::from(value.extract::<BitmaskValue<u64>>()?).into())
                }
                AlphaNumType::Ascii => {
                    Ok(FixedAsciiRange::from(value.extract::<AsciiRangeValue>()?).into())
                }
            }
        }
    }

    impl<'py> IntoPyObject<'py> for MixedRange {
        type Target = PyTuple;
        type Output = Bound<'py, PyTuple>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            match self {
                Self::Ascii(x) => ("A", x.value()).into_pyobject(py),
                Self::Uint(x) => ("I", BitmaskValue::<u64>::from(x)).into_pyobject(py),
                Self::F32(x) => ("F", BigDecimal::from(x.range)).into_pyobject(py),
                Self::F64(x) => ("D", BigDecimal::from(x.range)).into_pyobject(py),
            }
        }
    }
}

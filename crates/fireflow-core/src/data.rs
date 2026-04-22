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
    AllowTotMismatch, ConfigFlag as _, DisallowRangeTrunc, ReadDataKeywordsConfig,
    ReadEventsConfig, WriteDatasetInnerConfig,
};
use crate::core::{
    AsScaleOrTransform, Measurements, NamedTemporalsAndOpticals, ScaleTransform, VersionedMetaroot,
};
use crate::logging::{
    CommutativeResultIter as _, DeferredError, DeferredIter as _, DeferredSwitchableError,
    DeferredWarningAndError, ErrorGroup, ErrorsResult, GroupResult, IOErrorGroup, IOResult,
    ImpureError, LogResult, ResultExt as _, SwitchableErrorResult, SwitchableErrorsResult,
    WarningOrErrorResult, WarningsAndErrorResult, WarningsAndErrorsResult,
    WarningsAndIOGroupResult, io_to_log,
};
use crate::macros::{def_summary, match_many_to_one};
use crate::segment::AnyDataSegment;
use crate::text::byteord::{
    ArrayByteOrd, BitsOrChars, ByteOrdToSizedError, Bytes, Endian, HasByteOrd, NoByteOrd,
    OrderedToEndianError, PrivBytes, WidthToBytesError, WidthToFixedError,
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
    AsciiRangeFromKeywordsError, AsciiRangeValue, DelimAsciiRange, FixedAsciiRange,
};
use crate::validated::bitmask::{
    Bitmask, Bitmask08, Bitmask16, Bitmask24, Bitmask32, Bitmask40, Bitmask48, Bitmask56,
    Bitmask64, BitmaskTruncationError, BitmaskValue,
};
use crate::validated::dataframe::{
    AnyPrimitiveColumn, CastColError, FFDataFrame, FFDataFrameFamily, FromColumn, HasLen, HasWidth,
    InternalColumn, PrimitiveColumn, PrimitiveDataFrame, ambassador_impl_HasLen,
};
use crate::validated::keys::{IndexedKey as _, NonStdKeywords, StdKeywords};
use crate::validated::unaligned::{DstIndex, FCSRepr, SrcIndex, U24, U40, U48, U56};

use fireflow_core_proc::{IntoInner, impl_generic_enum_from};
use fireflow_types::config::{RowBufferSize, TruncateEventValues};
use fireflow_types::nonempty_string::DisplayableNE as _;
use type_families::{
    Functor, FunctorOnce, Kind1, Sibling1, VecFamily, impl_functor_once, impl_kind1,
};

use ambassador::{Delegate, delegatable_trait};
use bigdecimal::BigDecimal;
use bytemuck::{cast_slice, cast_vec};
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
pub type DataHeaders2_0 = AnyOrderedLayout2_0<VecFamily>;

/// All possible DATA storage configurations for 2.0.
///
/// This is identical to 3.0 in every way except that the $TOT keyword in 2.0
/// is optional, which requires a different interface.
pub type DataFrame2_0 = AnyOrderedLayout2_0<FFDataFrameFamily>;

/// All possible byte layouts for the DATA segment in 3.0.
pub type DataHeaders3_0 = AnyOrderedLayout3_0<VecFamily>;

/// All possible DATA storage configurations in 3.0.
pub type DataFrame3_0 = AnyOrderedLayout3_0<FFDataFrameFamily>;

/// All possible byte layouts for the DATA segment in 3.1.
///
/// Unlike 2.0 and 3.0, the integer layout allows the column widths to be
/// different. This is a consequence of making BYTEORD only mean "big or little
/// endian" and have nothing to do with number of bytes.
pub type DataHeaders3_1 = NonMixedEndianHeaders<Nothing<NumType>>;

/// All possible storage configurations in 3.1.
///
/// Unlike 2.0 and 3.0, the integer layout allows the column widths to be
/// different. This is a consequence of making BYTEORD only mean "big or little
/// endian" and have nothing to do with number of bytes.
pub type DataFrame3_1 = NonMixedEndianDataFrame<Nothing<NumType>>;

/// All possible byte layouts for the DATA segment in 3.2.
///
/// In addition to the loosened integer layouts in 3.1, 3.2 additionally allows
/// each column to have a different type and size (hence "Mixed").
pub type DataHeaders3_2 = Any3_2Layout<VecFamily>;

/// All possible storage configurations in 3.2.
///
/// In addition to the loosened integer layouts in 3.1, 3.2 additionally allows
/// each column to have a different type and size (hence "Mixed").
pub type DataFrame3_2 = Any3_2Layout<FFDataFrameFamily>;

/// Generic container for 3.2 DATA configurations.
// TODO false positive lint
#[allow(clippy::duplicated_attributes)]
#[derive(Clone, Delegate, PartialEq, IntoInner)]
#[into_inner(PrimitiveDataFrame)]
#[delegate(LayoutDims)]
#[delegate(LayoutRanges)]
#[delegate(LayoutDatatype, where = "M: LayoutDims, N: LayoutDims")]
#[delegate(LayoutKeywords, where = "M: LayoutDims, N: LayoutDims")]
#[delegate(Removable<R>, generics = "R")]
#[delegate(WriteLayoutOps)]
#[delegate(CheckRanges)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum Any3_2<M, N> {
    Mixed(M),
    NonMixed(N),
}

type Any3_2Layout<Fam> = Any3_2Group<Fam>;

type Any3_2Group<Fam> = Any3_2<MixedGroup<Fam>, AnyEndianDatatypeGroup<Fam, Option<NumType>>>;

type MixedGroup<Fam> = ColumnGroup_<Fam, MixedCol, false, ColumnMarkers3_2>;

pub type MixedHeaders = MixedGroup<VecFamily>;

type MixedDataFrame = MixedGroup<FFDataFrameFamily>;

type ColumnMarkers3_2 = ColumnMarkers<Identity<Tot>, Option<NumType>>;

pub type AnyOrderedGroup<Fam, T> =
    AnyOrderedDatatypeGroup<Fam, true, ColumnMarkers<T, Nothing<NumType>>>;

pub type AnyOrderedHeaders<T> = AnyOrderedGroup<VecFamily, T>;

type AnyOrderedDataFrame<T> = AnyOrderedGroup<FFDataFrameFamily, T>;

pub type AnyOrderedLayout2_0<Fam> = AnyOrderedGroup<Fam, Option<Tot>>;

pub type AnyOrderedLayout3_0<Fam> = AnyOrderedGroup<Fam, Identity<Tot>>;

type NonMixedEndianGroup<Fam, D> = AnyEndianDatatypeGroup<Fam, D>;

pub type NonMixedEndianHeaders<D> = NonMixedEndianGroup<VecFamily, D>;

type NonMixedEndianDataFrame<D> = NonMixedEndianGroup<FFDataFrameFamily, D>;

type VariableUintGroup<F, D> = ColumnGroup_<F, UvarCol, false, ColumnMarkers<Identity<Tot>, D>>;

type VariableUintHeaders<D> = VariableUintGroup<VecFamily, D>;

type VariableUintDataFrame<D> = VariableUintGroup<FFDataFrameFamily, D>;

pub type EndianUintHeaders<D> = EndianHeaders<UvarCol, D>;

// TODO false positive lint
#[allow(clippy::duplicated_attributes)]
#[derive(Clone, Delegate, PartialEq, IntoInner)]
#[into_inner(PrimitiveDataFrame)]
#[delegate(LayoutDims)]
#[delegate(LayoutRanges)]
#[delegate(LayoutDatatype, where = "Delim: LayoutDims, Fixed: LayoutDims")]
#[delegate(LayoutKeywords, where = "Delim: LayoutDims, Fixed: LayoutDims")]
#[delegate(Removable<R>, generics = "R")]
#[delegate(OptMeasLayoutKeywords)]
#[delegate(WriteLayoutOps)]
#[delegate(CheckRanges)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum AnyAscii<Delim, Fixed> {
    Delimited(Delim),
    Fixed(Fixed),
}

type AnyAsciiGroup<Fam, const ORD: bool, M> =
    AnyAscii<DelimAsciiGroup<Fam, ORD, M>, FixedAsciiGroup<Fam, ORD, M>>;

type AnyAsciiGroup2_0<F, T> = AnyAsciiGroup<F, true, ColumnMarkers<T, Nothing<NumType>>>;

type AnyAsciiGroup3_1<F, D> = AnyAsciiGroup<F, false, ColumnMarkers<Identity<Tot>, D>>;

pub type AnyAsciiHeaders<const ORD: bool, M> = AnyAsciiGroup<VecFamily, ORD, M>;

type AnyAsciiDataFrame<const ORD: bool, M> = AnyAsciiGroup<FFDataFrameFamily, ORD, M>;

type DelimAsciiGroup<Fam, const ORD: bool, M> = ColumnGroup_<Fam, DelimAsciiCol, ORD, M>;

pub type DelimAsciiHeaders<const ORD: bool, M> = DelimAsciiGroup<VecFamily, ORD, M>;

type FixedAsciiGroup<Fam, const ORD: bool, M> = ColumnGroup_<Fam, FixedAsciiCol, ORD, M>;

pub type FixedAsciiHeaders<const ORD: bool, M> = FixedAsciiGroup<VecFamily, ORD, M>;

type FixedAsciiDataFrame<const ORD: bool, M> = FixedAsciiGroup<FFDataFrameFamily, ORD, M>;

/// An 8-bit unsigned integer column.
#[derive(Clone, Copy, PartialEq)]
pub struct U08Col;

/// A 16-bit unsigned integer column.
#[derive(Clone, Copy, PartialEq)]
pub struct U16Col;

/// A 24-bit unsigned integer column.
#[derive(Clone, Copy, PartialEq)]
pub struct U24Col;

/// A 32-bit unsigned integer column.
#[derive(Clone, Copy, PartialEq)]
pub struct U32Col;

/// A 40-bit unsigned integer column.
#[derive(Clone, Copy, PartialEq)]
pub struct U40Col;

/// A 48-bit unsigned integer column.
#[derive(Clone, Copy, PartialEq)]
pub struct U48Col;

/// A 56-bit unsigned integer column.
#[derive(Clone, Copy, PartialEq)]
pub struct U56Col;

/// A 64-bit unsigned integer column.
#[derive(Clone, Copy, PartialEq)]
pub struct U64Col;

/// A 32-bit float column.
#[derive(Clone, Copy, PartialEq)]
pub struct F32Col;

/// A 64-bit float column.
#[derive(Clone, Copy, PartialEq)]
pub struct F64Col;

/// A fixed-width ascii column.
#[derive(Clone, Copy, PartialEq)]
pub struct FixedAsciiCol;

/// A delimited (variable width) ascii column.
#[derive(Clone, Copy, PartialEq)]
pub struct DelimAsciiCol;

/// An unsigned integer column which can be any width 8-64 (octets).
#[derive(Clone, Copy, PartialEq)]
pub struct UvarCol;

/// A column which may contain any type.
#[derive(Clone, Copy, PartialEq)]
pub struct MixedCol;

type ColumnGroup_<Fam, Col, const ORD: bool, M> = ColumnGroup<
    <Fam as Kind1>::Type<<Col as IsCol<Fam, ORD>>::Inner>,
    Fam,
    Col,
    <Col as IsCol<Fam, ORD>>::Layout,
    M,
    ORD,
>;

type ColumnHeaders<C, const ORD: bool, M> = ColumnGroup_<VecFamily, C, ORD, M>;

type ColumnDataFrame<C, const ORD: bool, M> = ColumnGroup_<VecFamily, C, ORD, M>;

/// DATA layout where each column has a fixed width.
#[derive(Clone, AsRef, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[new(visibility(""))]
pub struct ColumnGroup<Cols, CFam, Inner, Layout, Markers, const ORD: bool> {
    /// Thing holding the columns.
    container: Cols,
    // TODO this shouldn't be necessary anymore since ORD implies it
    /// The byte layout of a value in a column.
    #[as_ref(Layout)]
    byte_layout: Layout,
    /// The type family for the container.
    #[cfg_attr(feature = "serde", serde(skip))]
    _container_family: PhantomData<CFam>,
    /// The type within `container`.
    #[cfg_attr(feature = "serde", serde(skip))]
    _inner: PhantomData<Inner>,
    /// Marker types to further describe layout
    #[cfg_attr(feature = "serde", serde(skip))]
    _markers: PhantomData<Markers>,
}

/// Zero-sized marker types to describe data layouts.
///
/// These are used to distinguish certain layouts which have identical structure
/// but otherwise behave different depending on context.
#[derive(Clone, PartialEq, Default)]
pub struct ColumnMarkers<T, D> {
    /// Marker type to describe $TOT
    tot_def: PhantomData<T>,
    /// Marker type to describe $PnDatatype
    meas_data_def: PhantomData<D>,
}

// TODO false positive lint
#[allow(clippy::duplicated_attributes)]
#[derive(Clone, Delegate, PartialEq, IntoInner)]
#[into_inner(PrimitiveDataFrame)]
#[delegate(LayoutDims)]
#[delegate(LayoutRanges)]
#[delegate(LayoutDatatype, where = "Single: LayoutDims, Multi: LayoutDims")]
#[delegate(LayoutKeywords, where = "Single: LayoutDims, Multi: LayoutDims")]
#[delegate(Removable<R>, generics = "R")]
#[delegate(OptMeasLayoutKeywords)]
#[delegate(WriteLayoutOps)]
#[delegate(CheckRanges)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum AnyEndianUint<Single, Multi> {
    Single(Single),
    Multi(Multi),
}

type AnyEndianUintGroup<Fam, D> = AnyEndianUint<
    AnyUintGroup<Fam, false, ColumnMarkers<Identity<Tot>, D>>,
    VariableUintGroup<Fam, D>,
>;

type AnyFixedUintGroup<Fam, D> = AnyUintGroup<Fam, false, ColumnMarkers<Identity<Tot>, D>>;

type AnyFixedUintHeaders<D> = AnyFixedUintGroup<VecFamily, D>;

type AnyFixedUintDataFrame<D> = AnyFixedUintGroup<FFDataFrameFamily, D>;

pub type AnyEndianUintHeaders<D> = AnyEndianUintGroup<VecFamily, D>;

type AnyEndianUintDataFrame<D> = AnyEndianUintGroup<FFDataFrameFamily, D>;

/// Vector of data with a header describing it further.
///
/// This is used internally to represent the data in DATA with its associated
/// keywords.
#[derive(Clone, PartialEq, Into, new)]
#[new(visibility = "")]
pub struct AnnotatedColumn<M, T, R> {
    header: M,
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

/// An annotated column whose metadata maps to exactly one Rust type.
pub type NativeColumn<C> = AnnotatedColumn<
    C,
    <<C as HasNativeType>::Native as FCSRepr>::Prim,
    <C as HasNativeType>::Native,
>;

type NativeInternalColumn<C> =
    InternalColumn<<<C as HasNativeType>::Native as FCSRepr>::Prim, <C as HasNativeType>::Native>;

impl<T> AsRef<[T::Native]> for NativeColumn<T>
where
    T: HasNativeType,
    T::Native: FCSRepr,
    NativeInternalColumn<T>: AsRef<[T::Native]>,
{
    fn as_ref(&self) -> &[T::Native] {
        self.data.as_ref()
    }
}

impl<T> From<RangedVec<T, T::Native>> for NativeColumn<T>
where
    T: HasNativeType,
    T::Native: FCSRepr,
    Vec<T::Native>: Into<NativeInternalColumn<T>>,
{
    fn from(value: RangedVec<T, T::Native>) -> Self {
        Self::new(value.range, value.data.into())
    }
}

/// Generic container for anything that can be categorized by $DATATYPE.
// TODO false positive lint
#[allow(clippy::duplicated_attributes)]
#[derive(Debug, PartialEq, Clone, Delegate, IntoInner)]
#[into_inner(PrimitiveDataFrame)]
#[delegate(IsFixed)]
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
#[delegate(Removable<R>, generics = "R")]
#[delegate(OptMeasLayoutKeywords)]
#[delegate(WriteLayoutOps)]
#[delegate(CheckRanges)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum AnyDatatype<A, U, F, D> {
    Ascii(A),
    Uint(U),
    F32(F),
    F64(D),
}

type AnyDatatypeGroup_<Fam, const ORD: bool, M, I> = AnyDatatype<
    AnyAsciiGroup<Fam, ORD, M>,
    I,
    ColumnGroup_<Fam, F32Col, ORD, M>,
    ColumnGroup_<Fam, F64Col, ORD, M>,
>;

type AnyOrderedDatatypeGroup<Fam, const ORD: bool, M> =
    AnyDatatypeGroup_<Fam, ORD, M, AnyUintGroup<Fam, ORD, M>>;

pub type OrderedGroup<F, I, T> = ColumnGroup_<F, I, true, ColumnMarkers<T, Nothing<NumType>>>;

pub type EndianGroup<F, I, D> = ColumnGroup_<F, I, false, ColumnMarkers<Identity<Tot>, D>>;

pub type OrderedHeaders<I, T> = OrderedGroup<VecFamily, I, T>;

pub type EndianHeaders<I, D> = EndianGroup<VecFamily, I, D>;

type AnyEndianDatatypeGroup<Fam, D> =
    AnyDatatypeGroup_<Fam, false, ColumnMarkers<Identity<Tot>, D>, AnyEndianUintGroup<Fam, D>>;

pub type MixedRange = AnyDatatype<FixedAsciiRange, AnyBitmask, F32Range, F64Range>;

pub type MixedColumn = AnyDatatype<
    NativeColumn<FixedAsciiRange>,
    AnyBitmaskColumn,
    NativeColumn<F32Range>,
    NativeColumn<F64Range>,
>;

/// A big or little-endian integer column of some size (1-8 bytes)
// TODO false positive lint
#[allow(clippy::duplicated_attributes)]
#[derive(Debug, PartialEq, Clone, Copy, Delegate, IntoInner)]
#[into_inner(PrimitiveDataFrame)]
#[delegate(HasLen)]
#[delegate(IsBinary)]
#[delegate(LayoutDims)]
#[delegate(LayoutRanges)]
#[delegate(Insertable<R>, generics = "R")]
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
#[delegate(Removable<R>, generics = "R")]
#[delegate(OptMeasLayoutKeywords)]
#[delegate(OrderedLayoutOps)]
#[delegate(WriteLayoutOps)]
#[delegate(CheckRanges)]
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

type AnyUintGroup<Fam, const ORD: bool, M> = AnyUint<
    ColumnGroup_<Fam, U08Col, ORD, M>,
    ColumnGroup_<Fam, U16Col, ORD, M>,
    ColumnGroup_<Fam, U24Col, ORD, M>,
    ColumnGroup_<Fam, U32Col, ORD, M>,
    ColumnGroup_<Fam, U40Col, ORD, M>,
    ColumnGroup_<Fam, U48Col, ORD, M>,
    ColumnGroup_<Fam, U56Col, ORD, M>,
    ColumnGroup_<Fam, U64Col, ORD, M>,
>;

type AnyOrderedUintGroup<F, T> = AnyUintGroup<F, true, ColumnMarkers<T, Nothing<NumType>>>;

pub type AnyOrderedUintHeaders<T> = AnyOrderedUintGroup<VecFamily, T>;

type AnyOrderedUintDataFrame<T> = AnyOrderedUintGroup<FFDataFrameFamily, T>;

pub type AnyBitmask =
    AnyUint<Bitmask08, Bitmask16, Bitmask24, Bitmask32, Bitmask40, Bitmask48, Bitmask56, Bitmask64>;

/// Either a [`Range`] or something else, both of which encode $PnR.
///
/// This is meant for cases where createing a new column can be done with either
/// a general decimal value or a specific type which encodes further information
/// about the column to be written.
pub enum RangeOrType<T> {
    Range(Range),
    Specific(T),
}

pub type RangeOrBitmaskRange = RangeOrType<AnyBitmask>;

pub type RangeOrBitmaskColumn = RangeOrType<AnyBitmaskColumn>;

pub type RangeOrMixedRange = RangeOrType<MixedRange>;

pub type RangeOrMixedColumn = RangeOrType<MixedColumn>;

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
pub enum TruncatedResult {
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

/// Error when reading DATA segment and checking ranges
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ReadCheckedDataframeError {
    Read(ReadDataframeError),
    Overrange(EventOverRangeErrors),
}

/// Warning when reading DATA segment and checking ranges
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ReadCheckedDataframeWarning {
    Read(ReadDataframeWarning),
    Overrange(EventOverRangeError),
}

/// Error when reading DATA segment
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ReadDataframeError {
    Ascii(ReadAsciiError),
    Width(EventWidthError),
    TotMismatch(TotEventMismatchError),
}

/// Warning when reading DATA segment
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ReadDataframeWarning {
    Uneven(UnevenEventWidthError),
    Tot(TotEventMismatchError),
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
    from = _0.error.src.as_displayable(),
    to = _0.error.dest.as_displayable(),
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
    src: AlphaNumType,
    dest: AlphaNumType,
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
    #[error("could not insert range {0}")]
    #[from(MismatchTypeRangeError)]
    MismatchTypes(MismatchTypeRangeError),
}

/// Error when insert range with concrete type which mismatches layout.
#[derive(Debug, Error)]
// TODO make this say something useful
#[error("range type mistmatches")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::InvalidKeywordValueError))]
pub struct MismatchTypeRangeError;

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

#[derive(new)]
struct ComputedRowsResult {
    total_events: u64,
    event_width: u64,
    remainder: u64,
}

/// A vector with a range.
///
/// This is used internally when making new dataframes for layouts which have
/// variable types and widths between columns. The range is necessary in order
/// to assess the width and type of the column at runtime.
///
/// We cannot used AnnotatedColumn for these use cases since making a new
/// dataframe involves making a new buffer with the correct number of rows and
/// filling with 0's; the 0's are then mutated in place. This cannot happen in
/// an AnnotatedColumn since the underlying storage is a polars buffer which is
/// harder to mutate in place.
#[derive(new)]
struct RangedVec<B, T> {
    range: B,
    data: Vec<T>,
}

type NativeRangedVec<C> = RangedVec<C, <C as HasNativeType>::Native>;

type AnyUintVec = AnyUint<
    NativeRangedVec<Bitmask08>,
    NativeRangedVec<Bitmask16>,
    NativeRangedVec<Bitmask24>,
    NativeRangedVec<Bitmask32>,
    NativeRangedVec<Bitmask40>,
    NativeRangedVec<Bitmask48>,
    NativeRangedVec<Bitmask56>,
    NativeRangedVec<Bitmask64>,
>;

type MixedVec = AnyDatatype<
    RangedVec<FixedAsciiRange, u64>,
    AnyUintVec,
    RangedVec<F32Range, f32>,
    RangedVec<F64Range, f64>,
>;

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
struct RowBuffer<const IS_READ: bool> {
    nrows: usize,
    row_width: usize,
    rows_per_buffer: usize,
    buf_size: u64,
    bytes: Vec<u8>,
}

type ReadBuffer = RowBuffer<true>;

type WriteBuffer = RowBuffer<false>;

enum Any4ByteType<F32, I32> {
    F32(F32),
    Uint32(I32),
}

enum Any8ByteType<F64, I64> {
    F64(F64),
    Uint64(I64),
}

// Make some nice macros to dispatch and/or map our type-specific enums
//
// nomenclature:
// - match_any_* - apply syntax to each variant
// - match_map_* - apply syntax to each variant and enclose in the same variant

macro_rules! match_any_uint {
    ($value:expr, $inner:ident, $action:expr) => {
        match_many_to_one!(
            $value,
            AnyUint,
            [
                Uint08, Uint16, Uint24, Uint32, Uint40, Uint48, Uint56, Uint64
            ],
            $inner,
            $action
        )
    };
}

macro_rules! match_any_datatype {
    ($value:expr, $inner:ident, $action:expr ) => {
        match_many_to_one!(
            $value,
            AnyDatatype,
            [Ascii, Uint, F32, F64],
            $inner,
            $action
        )
    };
}

macro_rules! match_any_ascii {
    ($value:expr, $inner:ident, $action:expr) => {
        match_many_to_one!($value, AnyAscii, [Delimited, Fixed], $inner, $action)
    };
}

macro_rules! match_any_endian_uint {
    ($value:expr, $inner:ident, $action:expr) => {
        match_many_to_one!($value, AnyEndianUint, [Single, Multi], $inner, $action)
    };
}

macro_rules! match_any_3_2 {
    ($value:expr, $inner:ident, $action:expr) => {
        match_many_to_one!($value, Any3_2, [Mixed, NonMixed], $inner, $action)
    };
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

macro_rules! match_map_datatype {
    ($value:expr, $inner:ident, $action:expr) => {
        match $value {
            AnyDatatype::Ascii($inner) => AnyDatatype::Ascii($action),
            AnyDatatype::Uint($inner) => AnyDatatype::Uint($action),
            AnyDatatype::F32($inner) => AnyDatatype::F32($action),
            AnyDatatype::F64($inner) => AnyDatatype::F64($action),
        }
    };
}

macro_rules! match_map_ascii {
    ($value:expr, $inner:ident, $action:expr) => {
        match $value {
            AnyAscii::Delimited($inner) => AnyAscii::Delimited($action),
            AnyAscii::Fixed($inner) => AnyAscii::Fixed($action),
        }
    };
}

macro_rules! match_map_endian_uint {
    ($value:expr, $inner:ident, $action:expr) => {
        match $value {
            AnyEndianUint::Multi($inner) => AnyEndianUint::Multi($action),
            AnyEndianUint::Single($inner) => AnyEndianUint::Single($action),
        }
    };
}

// Implement version specific-operations for headers.
//
// This is the main trait that public-facing APIs will use.

/// A version-specific data layout with just headers (no DATA).
pub trait VersionedDataHeaders
where
    for<'a> Self: Sized
        + ReadLayoutOps<Self::Tot>
        + LayoutDatatype
        + LayoutDims
        + NormalizableLayout
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
        &mut self,
        h: &mut BufReader<R>,
        tot: Self::Tot,
        seg: &mut AnyDataSegment,
        conf: &ReadEventsConfig,
    ) -> WarningsAndIOGroupResult<
        DataFrameResult<Self::DfTarget>,
        ReadCheckedDataframeWarning,
        ReadCheckedDataframeError,
        (),
    >
    where
        Self::DfTarget: CheckRanges,
    {
        match seg.try_abs_coords() {
            // if we cannot get coords, it means the segment is empty, thus the
            // returned dataframe should be empty
            None => {
                let ret = DataFrameResult::new(self.empty(), EventsDiagnostics::default());
                LogResult::new_ok(ret)
            }
            Some((begin, _)) => {
                // seek to start
                io_to_log!(h.seek(SeekFrom::Start(begin)));
                // normalize layout (which is why self must be mut)
                self.normalize();
                // read dataframe
                self.h_read_df_inner(h, tot, seg, conf)
                    .map_pure_errors(ReadCheckedDataframeError::from)
                    .map_commutative_warnings(ReadCheckedDataframeWarning::from)
                    .and_then_commutative(|mut res| {
                        // check dataframe ranges (if configured)
                        let rs = res.dataframe.check_ranges(conf.truncate_event_values);
                        let overrange = rs.iter().map(TruncatedResult::as_col).collect();
                        let es = rs.into_iter().filter_map(TruncatedResult::into_err);
                        res.diagnostics.overrange_columns = overrange;
                        let flag = conf.disallow_over_range;
                        SwitchableErrorsResult::new_deferred_switchable_iter3((), es, flag)
                            .switchable_into_commutative()
                            .group()
                            .map_error(ReadCheckedDataframeError::from)
                            .map_error(IOErrorGroup::new_pure_one)
                            .map_commutative_warnings(ReadCheckedDataframeWarning::from)
                            .map_ok_value(|()| res)
                    })
            }
        }
    }

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

impl VersionedDataHeaders for DataHeaders2_0 {
    type ByteLayout = ByteOrd2_0;
    type NumType = Nothing<NumType>;
    type Tot = Option<Tot>;

    fn lookup(
        std: &mut StdKeywords,
        meas_nonstd: &mut [NonStdKeywords],
        conf: &ReadDataKeywordsConfig,
    ) -> LookupLayoutResult<NewLayout<Self>> {
        AnyOrderedHeaders::lookup(std, meas_nonstd, conf).map_ok_value(FunctorOnce::fmap_into_once)
    }

    fn lookup_ro(
        kws: &StdKeywords,
        par: Par,
        conf: &ReadDataKeywordsConfig,
    ) -> LookupLayoutResult<NewLayout<Self>> {
        AnyOrderedHeaders::lookup_ro(kws, par, conf).map_ok_value(FunctorOnce::fmap_into_once)
    }

    fn new_empty(datatype: AlphaNumType) -> Self {
        AnyOrderedHeaders::new_empty(datatype)
    }

    fn try_new(
        datatype: AlphaNumType,
        byteord: Self::ByteLayout,
        columns: Vec<ColumnLayoutValues<Self::NumType>>,
        conf: &ReadDataKeywordsConfig,
    ) -> WarningsAndErrorsResult<NewLayout<Self>, (), NewMixedTypeWarning, NewDataLayoutError> {
        AnyOrderedHeaders::try_new(datatype, byteord, columns, conf)
            .map_ok_value(FunctorOnce::fmap_into_once)
    }
}

impl VersionedDataHeaders for DataHeaders3_0 {
    type ByteLayout = ByteOrd2_0;
    type NumType = Nothing<NumType>;
    type Tot = Identity<Tot>;

    fn lookup(
        std: &mut StdKeywords,
        meas_nonstd: &mut [NonStdKeywords],
        conf: &ReadDataKeywordsConfig,
    ) -> LookupLayoutResult<NewLayout<Self>> {
        AnyOrderedHeaders::lookup(std, meas_nonstd, conf).map_ok_value(FunctorOnce::fmap_into_once)
    }

    fn lookup_ro(
        kws: &StdKeywords,
        par: Par,
        conf: &ReadDataKeywordsConfig,
    ) -> LookupLayoutResult<NewLayout<Self>> {
        AnyOrderedHeaders::lookup_ro(kws, par, conf).map_ok_value(FunctorOnce::fmap_into_once)
    }

    fn new_empty(datatype: AlphaNumType) -> Self {
        AnyOrderedHeaders::new_empty(datatype)
    }

    fn try_new(
        datatype: AlphaNumType,
        byteord: Self::ByteLayout,
        columns: Vec<ColumnLayoutValues<Nothing<NumType>>>,
        conf: &ReadDataKeywordsConfig,
    ) -> WarningsAndErrorsResult<NewLayout<Self>, (), NewMixedTypeWarning, NewDataLayoutError> {
        AnyOrderedHeaders::try_new(datatype, byteord, columns, conf)
            .map_ok_value(FunctorOnce::fmap_into_once)
    }
}

impl VersionedDataHeaders for DataHeaders3_1 {
    type ByteLayout = Endian;
    type NumType = Nothing<NumType>;
    type Tot = Identity<Tot>;

    fn lookup(
        std: &mut StdKeywords,
        meas_nonstd: &mut [NonStdKeywords],
        conf: &ReadDataKeywordsConfig,
    ) -> LookupLayoutResult<NewLayout<Self>> {
        NonMixedEndianHeaders::lookup(std, meas_nonstd, conf)
            .map_ok_value(FunctorOnce::fmap_into_once)
    }

    fn lookup_ro(
        kws: &StdKeywords,
        par: Par,
        conf: &ReadDataKeywordsConfig,
    ) -> LookupLayoutResult<NewLayout<Self>> {
        NonMixedEndianHeaders::lookup_ro(kws, par, conf).map_ok_value(FunctorOnce::fmap_into_once)
    }

    fn new_empty(datatype: AlphaNumType) -> Self {
        NonMixedEndianHeaders::new_empty(datatype)
    }

    fn try_new(
        datatype: AlphaNumType,
        byteord: Self::ByteLayout,
        columns: Vec<ColumnLayoutValues<Nothing<NumType>>>,
        conf: &ReadDataKeywordsConfig,
    ) -> WarningsAndErrorsResult<NewLayout<Self>, (), NewMixedTypeWarning, NewDataLayoutError> {
        NonMixedEndianHeaders::try_new(datatype, byteord, columns, conf)
            .map_ok_value(FunctorOnce::fmap_into_once)
    }
}

impl VersionedDataHeaders for DataHeaders3_2 {
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
        NonMixedEndianHeaders::new_empty(datatype).into()
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
                let l = NonMixedEndianHeaders::new_empty1(datatype, byteord.0).into();
                LogResult::new_ok(NewLayout::new(l, vec![]))
            }
            // has columns with one datatype, use nonmixed layout
            [dt] => {
                let ds =
                    columns.fmap(|c| ColumnLayoutValues::new(c.width, c.range, Nothing::default()));
                NonMixedEndianHeaders::try_new(dt, byteord.0, ds, conf).map_ok_value(
                    |x: NewLayout<_>| {
                        x.fmap_once(|y: NonMixedEndianHeaders<_>| Self::NonMixed(y.phantom_into()))
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
                ColumnGroup::try_new(columns, byteord.0, go)
                    .map_errors(NewDataLayoutError::from)
                    .map_ok_value(FunctorOnce::fmap_into_once)
            }
        }
    }
}

// Implement version specific ops for dataframes

/// A version-specific dataframe (headers + DATA)
pub trait VersionedDataFrame
where
    for<'a> Self: Sized
        + WriteLayoutOps
        + LayoutDatatype
        + LayoutDims
        + NormalizableLayout
        + Removable<RangeAndColumn>
        + OptMeasLayoutKeywords
        + CheckRanges,
{
    fn h_write_df<W>(
        &mut self,
        h: &mut BufWriter<W>,
        conf: &WriteDatasetInnerConfig,
    ) -> io::Result<()>
    where
        W: Write,
    {
        // normalize before writing (which is why self must be mut)
        self.normalize();
        self.h_write_df_inner(h, conf)
    }
}

impl VersionedDataFrame for DataFrame2_0 {}
impl VersionedDataFrame for DataFrame3_0 {}
impl VersionedDataFrame for DataFrame3_1 {}
impl VersionedDataFrame for DataFrame3_2 {}

// Implement base traits for layouts
//
// These traits are simple because they can be fractally delegated to inner
// types without any special tricks.

/// A layout which has a width (which also means the width can be cleared).
#[delegatable_trait]
pub trait LayoutDims: Sized {
    fn ncols(&self) -> usize;

    fn clear(&mut self);
}

impl<C: HasWidth, F, I, L, M, const ORD: bool> LayoutDims for ColumnGroup<C, F, I, L, M, ORD> {
    fn ncols(&self) -> usize {
        self.container.width()
    }

    fn clear(&mut self) {
        self.container.clear();
    }
}

/// A layout which has ranges.
#[delegatable_trait]
pub trait LayoutRanges: Sized {
    fn ranges(&self) -> Vec<Range>;
}

impl<C, F, I, L, M, const ORD: bool> LayoutRanges for ColumnGroup<C, F, I, L, M, ORD>
where
    I: IsCol<F, ORD>,
    C: AsRef<[I::Inner]>,
    for<'c> Range: From<&'c I::Inner>,
{
    fn ranges(&self) -> Vec<Range> {
        self.container.as_ref().iter().map(Into::into).collect()
    }
}

/// A layout which has one more datatypes.
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

impl<C, F, I, L, M, const ORD: bool> LayoutDatatype for ColumnGroup<C, F, I, L, M, ORD>
where
    I: IsCol<F, ORD>,
    C: AsRef<[I::Inner]>,
    I::Inner: HasDatatype,
{
    fn datatype(&self) -> AlphaNumType {
        I::Inner::datatype_from_columns(self.container.as_ref())
    }

    fn datatypes(&self) -> Vec<AlphaNumType> {
        self.container
            .as_ref()
            .iter()
            .map(HasDatatype::col_datatype)
            .collect()
    }
}

/// A layout which has FCS keywords.
#[delegatable_trait]
pub trait LayoutKeywords: Sized + LayoutDatatype {
    fn byteord_keyword(&self) -> ReqRootKeyword<'_>;

    fn req_keywords(&self) -> [ReqRootKeyword<'_>; 2] {
        let d = ReqRootKeyword::from_value(self.datatype());
        [d, self.byteord_keyword()]
    }

    fn req_meas_keywords(&self) -> Vec<[ReqMeasKeyword<'_>; 2]>;
}

impl<C, F, I, L, M, const ORD: bool> LayoutKeywords for ColumnGroup<C, F, I, L, M, ORD>
where
    I: IsCol<F, ORD, Layout = L>,
    C: AsRef<[I::Inner]>,
    I::Inner: HasDatatype + IntoWidth,
    L: Copy + HasByteOrd,
    for<'c> ReqRootKeyword<'c>: From<SplitKeyword0<L::ByteOrd>>,
    for<'c> Range: From<&'c I::Inner>,
{
    fn byteord_keyword(&self) -> ReqRootKeyword<'_> {
        ReqRootKeyword::from_value(self.byte_layout.into())
    }

    fn req_meas_keywords(&self) -> Vec<[ReqMeasKeyword<'_>; 2]> {
        self.container
            .as_ref()
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let w = ReqMeasKeyword::from_value(c.as_width(), i);
                let r = ReqMeasKeyword::from_value(Range::from(c), i);
                [w, r]
            })
            .collect()
    }
}

// Implement optional measured keywords
//
// This will return a vector of `None` in all cases except 3.2 where this will
// return the value of $PnDATATYPE if it exists. The base case can be delegated
// for pre-3.2.

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

impl<C, I, F, S, M, const ORD: bool> OptMeasLayoutKeywords for ColumnGroup<C, F, I, S, M, ORD>
where
    Self: LayoutDims,
{
    fn opt_meas_keywords(&self) -> Vec<Option<SplitKeyword1<NumType>>> {
        vec![None; self.ncols()]
    }
}

impl<C, F, I, M, N> OptMeasLayoutKeywords for Any3_2<ColumnGroup<C, F, I, Endian, M, false>, N>
where
    I: IsCol<F, false>,
    C: AsRef<[I::Inner]>,
    I::Inner: HasDatatype,
    Self: LayoutDatatype,
    N: LayoutDims,
{
    fn opt_meas_keywords(&self) -> Vec<Option<SplitKeyword1<NumType>>> {
        let dt = self.datatype();
        match self {
            Self::NonMixed(x) => vec![None; x.ncols()],
            Self::Mixed(x) => x
                .container
                .as_ref()
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

// Implement headers -> empty dataframe conversion
//
// This can be easily delegated but is more complex than what ambassador can do
// since it requires an associated type to describe the target.

/// A headers type that can be converted to an empty dataframe.
#[delegatable_trait]
pub trait IntoEmptyDataFrame {
    type DfTarget;

    fn empty(&self) -> Self::DfTarget;
}

impl IntoEmptyDataFrame for DataHeaders3_2 {
    type DfTarget = DataFrame3_2;

    fn empty(&self) -> Self::DfTarget {
        match_any_3_2!(self, x, Self::DfTarget::from(x.empty()))
    }
}

impl<A, I, F32, F64> IntoEmptyDataFrame for AnyDatatype<A, I, F32, F64>
where
    A: IntoEmptyDataFrame,
    I: IntoEmptyDataFrame,
    F32: IntoEmptyDataFrame,
    F64: IntoEmptyDataFrame,
{
    type DfTarget = AnyDatatype<A::DfTarget, I::DfTarget, F32::DfTarget, F64::DfTarget>;

    fn empty(&self) -> Self::DfTarget {
        match_map_datatype!(self, x, x.empty())
    }
}

impl<W0, W> IntoEmptyDataFrame for AnyEndianUint<W0, W>
where
    W0: IntoEmptyDataFrame,
    W: IntoEmptyDataFrame,
{
    type DfTarget = AnyEndianUint<W0::DfTarget, W::DfTarget>;

    fn empty(&self) -> Self::DfTarget {
        match_map_endian_uint!(self, x, x.empty())
    }
}

impl<D, F> IntoEmptyDataFrame for AnyAscii<D, F>
where
    D: IntoEmptyDataFrame,
    F: IntoEmptyDataFrame,
{
    type DfTarget = AnyAscii<D::DfTarget, F::DfTarget>;

    fn empty(&self) -> Self::DfTarget {
        match_map_ascii!(self, x, x.empty())
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

impl<C, const ORD: bool, M> IntoEmptyDataFrame for ColumnGroup_<VecFamily, C, ORD, M>
where
    C: IsCol<VecFamily, ORD>
        + IsCol<FFDataFrameFamily, ORD, Layout = <C as IsCol<VecFamily, ORD>>::Layout>,
    <C as IsCol<VecFamily, ORD>>::Inner:
        IntoEmptyColumn<Target = <C as IsCol<FFDataFrameFamily, ORD>>::Inner>,
    <C as IsCol<FFDataFrameFamily, ORD>>::Inner: HasLen,
    <C as IsCol<VecFamily, ORD>>::Layout: Clone,
{
    type DfTarget = ColumnGroup_<FFDataFrameFamily, C, ORD, M>;

    fn empty(&self) -> Self::DfTarget {
        let cs = self.container.iter().map(IntoEmptyColumn::empty);
        ColumnGroup::new(FFDataFrame::try_new(cs).unwrap(), self.byte_layout.clone())
    }
}

// Implement header -> dataframe read traits
//
// For the base column layout, there are only two types of impls: fixed and
// delimited. Only delimited ASCII layouts use the later.
//
// Aside from the base type, all enums and wrappers simply delegate downward to
// the base type and wrap the return type.

/// A headers type that can be converted to a dataframe by reading a bytestream.
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

impl ReadLayoutOps<Identity<Tot>> for DataHeaders3_2 {
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

impl<A, I, F32, F64, TotType> ReadLayoutOps<TotType> for AnyDatatype<A, I, F32, F64>
where
    A: ReadLayoutOps<TotType>,
    I: ReadLayoutOps<TotType>,
    F32: ReadLayoutOps<TotType>,
    F64: ReadLayoutOps<TotType>,
    A::DfTarget: Into<Self::DfTarget>,
    I::DfTarget: Into<Self::DfTarget>,
    F32::DfTarget: Into<Self::DfTarget>,
    F64::DfTarget: Into<Self::DfTarget>,
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
        match_any_datatype!(self, x, x.h_read_into(h, tot, seg, conf))
    }
}

impl<W0, W> ReadLayoutOps<Identity<Tot>> for AnyEndianUint<W0, W>
where
    W0: ReadLayoutOps<Identity<Tot>>,
    W: ReadLayoutOps<Identity<Tot>>,
    W0::DfTarget: Into<Self::DfTarget>,
    W::DfTarget: Into<Self::DfTarget>,
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
        match_any_endian_uint!(self, x, x.h_read_into(h, tot, seg, conf))
    }
}

impl<D, A, TotType> ReadLayoutOps<TotType> for AnyAscii<A, D>
where
    D: ReadLayoutOps<TotType>,
    A: ReadLayoutOps<TotType>,
    D::DfTarget: Into<Self::DfTarget>,
    A::DfTarget: Into<Self::DfTarget>,
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
        match_any_ascii!(self, x, x.h_read_into(h, tot, seg, conf))
    }
}

impl<C08, C16, C24, C32, C40, C48, C56, C64, TotType> ReadLayoutOps<TotType>
    for AnyUint<C08, C16, C24, C32, C40, C48, C56, C64>
where
    C08: ReadLayoutOps<TotType>,
    C16: ReadLayoutOps<TotType>,
    C24: ReadLayoutOps<TotType>,
    C32: ReadLayoutOps<TotType>,
    C40: ReadLayoutOps<TotType>,
    C48: ReadLayoutOps<TotType>,
    C56: ReadLayoutOps<TotType>,
    C64: ReadLayoutOps<TotType>,
    C08::DfTarget: Into<Self::DfTarget>,
    C16::DfTarget: Into<Self::DfTarget>,
    C24::DfTarget: Into<Self::DfTarget>,
    C32::DfTarget: Into<Self::DfTarget>,
    C40::DfTarget: Into<Self::DfTarget>,
    C48::DfTarget: Into<Self::DfTarget>,
    C56::DfTarget: Into<Self::DfTarget>,
    C64::DfTarget: Into<Self::DfTarget>,
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
        match_any_uint!(self, x, x.h_read_into(h, tot, seg, conf))
    }
}

impl<Col, I, Layout, const ORD: bool, TotType, Dtype> ReadLayoutOps<TotType>
    for ColumnGroup<Vec<Col>, VecFamily, I, Layout, ColumnMarkers<TotType, Dtype>, ORD>
where
    Self: FixedRead + IntoEmptyDataFrame<DfTarget = <Self as FixedRead>::DfTarget>,
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
                let n = u64_to_usize(nrow_out.total_events);
                self.h_read_fixed_df(h, n, conf)
                    .map_err(IOErrorGroup::from)
                    .map(|df| {
                        let out = EventsDiagnostics::new(
                            Some(nrow_out.event_width),
                            Some(nrow_out.remainder),
                            tot_not_eq,
                            // TODO this is awkward
                            vec![],
                        );
                        DataFrameResult::new(df, out)
                    })
                    .into_log()
            })
    }
}

impl<const ORD: bool, TotType, Dtype> ReadLayoutOps<TotType>
    for ColumnGroup<
        Vec<DelimAsciiRange>,
        VecFamily,
        DelimAsciiCol,
        NoByteOrd<ORD>,
        ColumnMarkers<TotType, Dtype>,
        ORD,
    >
where
    DelimAsciiCol: IsCol<VecFamily, ORD, Inner = DelimAsciiRange, Layout = NoByteOrd<ORD>>
        + IsCol<
            FFDataFrameFamily,
            ORD,
            Inner = NativeColumn<DelimAsciiRange>,
            Layout = NoByteOrd<ORD>,
        >,
{
    fn h_read_df_inner<R: Read>(
        &self,
        h: &mut BufReader<R>,
        tot: TotType,
        seg: &mut AnyDataSegment,
        _: &ReadEventsConfig,
    ) -> WarningsAndIOGroupResult<
        DataFrameResult<Self::DfTarget>,
        ReadDataframeWarning,
        ReadDataframeError,
        (),
    >
    where
        TotType: IsTot,
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
        let rs = &self.container[..];
        let nbytes = u64_to_usize(seg.len());
        if rs.is_empty() && nbytes > 0 {
            let e = ReadAsciiError::from(ReadDelimAsciiError::from(ReadDelimNoColumnError));
            return LogResult::new_err(IOErrorGroup::new_pure_one(e.into()));
        }
        let res = TotType::with_tot(
            h,
            tot,
            |h_, t| go!(h_read_delim_with_rows(rs, h_, t, nbytes)),
            |h_| go!(h_read_delim_without_rows(rs, h_, nbytes)),
        );

        res.map_err(IOErrorGroup::from)
            .map(|data| {
                debug_assert!(
                    data.iter().map(Vec::len).unique().count() < 2,
                    "columns must all be same length"
                );
                let cs = data
                    .into_iter()
                    .map(InternalColumn::from)
                    .zip(&self.container)
                    .map(|(vec, &range)| NativeColumn::new(range, vec));
                let df = FFDataFrame::try_new(cs).unwrap();
                let out = EventsDiagnostics::new(None, None, None, vec![]);
                DataFrameResult::new(ColumnGroup::new_ascii(df), out)
            })
            .into_log()
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

fn is_ascii_delim(x: u8) -> bool {
    // tab, newline, carriage return, space, or comma
    x == 9 || x == 10 || x == 13 || x == 32 || x == 44
}

// Implement write ops for dataframes

/// A headers type that can be converted to a dataframe by reading a bytestream.
#[delegatable_trait]
pub trait WriteLayoutOps: Sized {
    fn h_write_df_inner<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        conf: &WriteDatasetInnerConfig,
    ) -> io::Result<()>;
}

impl<Col, I, Layout, const ORD: bool, TotType, Dtype> WriteLayoutOps
    for ColumnGroup<
        FFDataFrame<Col>,
        FFDataFrameFamily,
        I,
        Layout,
        ColumnMarkers<TotType, Dtype>,
        ORD,
    >
where
    Self: FixedWrite,
{
    fn h_write_df_inner<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        conf: &WriteDatasetInnerConfig,
    ) -> io::Result<()> {
        self.h_write_fixed_df(h, conf)
    }
}

// delim ASCII needs to be handled differently, this is almost certainly not
// optimal but nobody will likely notice ;)
impl<const ORD: bool, TotType, Dtype> WriteLayoutOps
    for ColumnGroup<
        FFDataFrame<NativeColumn<DelimAsciiRange>>,
        FFDataFrameFamily,
        DelimAsciiCol,
        NoByteOrd<ORD>,
        ColumnMarkers<TotType, Dtype>,
        ORD,
    >
{
    fn h_write_df_inner<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        _: &WriteDatasetInnerConfig,
    ) -> io::Result<()> {
        let df = &self.container;
        let ncols = df.ncols();
        let nrows = df.nrows();

        for row_idx in 0..nrows {
            for (col_idx, col) in df.iter().enumerate() {
                let xs = col.as_ref();
                let s = xs[row_idx].to_string();
                h.write_all(s.as_bytes())?;
                // write delimiter after all but last value
                if !(row_idx == nrows - 1 && col_idx == ncols - 1) {
                    h.write_all(&[32])?; // 32 = space in ASCII
                }
            }
        }

        Ok(())
    }
}

// Implement low-level reading for header layouts with fixed width.
//
// All the "fast" code for reading DATA is here. Each impl is mostly a loop
// which reads a stream of bytes and copies them into vectors (one for each
// column in the dataframe to be built).
//
// The following performance optimizations are considered:
// 1. eliminating jmp ops from tight loops
// 2. reading bytes in a cache-coherent manner
//
// For (1), jmps themselves will not slow down the loop too much since most CPUs
// these days have branch prediction. However, the compiler can unroll or
// autovectorize (maybe) if a loop contains ZERO jmp ops which usually results
// in a massive performance gain. Most of the loops below were specially
// designed with this in mind.
//
// Eliminating jmp ops involves the following considerations:
//
// - We cannot have bounds checks, since panic requires a jmp in the final asm
//   code. This generally requires unsafe since these loops are complex enough
//   that the compiler can't confirm where the bounds are.
//
// - ASCII layouts need to check if the bytes are numeric characters, and must
//   fail otherwise. Failing requires a jmp op which means these loops will not
//   get the performance gain from eliminating all jmp ops. However, what we can
//   do is make a separate trait just for ASCII layouts and make all others
//   not consider a failure case. This way only ASCII users will suffer (which
//   is hopefully almost nobody).
//
// - Byte layout only needs to be read once before the loop since it will never
//   change throughout the entirety of DATA. This means each byte layout needs
//   its own loop (times the number of types that apply to that byte layout)
//   rather than using one loop which contains branches to change code path
//   depending on byte layout. This is alot of loops, but this is why generics
//   exist ;)
//
// - Complex layouts can often be simplified (ie "normalized"). For instance,
//   most 3.1+ integer layouts will only have one width. This might be
//   represented as a variable width layout. Variable width requires branching
//   within a loop in order to decide which width to use. Rather than always use
//   a variable width loop to read a layout which only has one width, we can
//   convert the variable width layout to one which is guaranteed to only has
//   one width, and use a specialized loop for that with no branching. The
//   same applies to mixed type layouts in 3.2 (many will only have one type).
//
// - If columns are all the same width but a different type, we can "cheat" by
//   reading all columns as one type (like a u32) and casting them after
//   reading to a different type (like an f32). This is nice for some machines
//   such as the FACSDiscover which generally produce quite beefy files with
//   different types in them; however, all types are 32-bits wide so this
//   cheat applies.
//
// For (2), cache coherence is attained in all loops via a buffer which will
// contain a fixed number of rows that fix nicely in CPU cache (ideally L1d).
// Without this, most files will result in constant cache misses since FCS files
// are row major and we wish to store data in column major. See the RowBuffer
// type for more details.
//
// In the loops below, this is achieved via 3 sub-loops (from outer to inner):
// - read N rows from DATA and store in buffer
// - loop through each column
// - stride over buffer and store N values into the given column
//
// I.e. the loop order is buffer -> columns -> values, which only requires the
// buffer and one column to be in the cache at one time.

/// Fixed header layout type to be converted to a dataframe type via a bytestream.
///
/// Does not apply to delimited ASCII layouts since these do not have
/// predictable widths in each column.
///
/// This trait is meant to be specialized for different layouts in order to
/// make it fast.
///
/// NOTE: layouts are assumed to be normalized prior to calling this trait.
trait FixedRead {
    type DfTarget;

    fn h_read_fixed_df<R: Read>(
        &self,
        h: &mut BufReader<R>,
        nrows: usize,
        conf: &ReadEventsConfig,
    ) -> IOResult<Self::DfTarget, ReadDataframeError>;
}

// basic read loop for most use cases
impl<C, L, I, M, const ORD: bool> FixedRead for ColumnGroup<Vec<C>, VecFamily, I, L, M, ORD>
where
    L: ByteLayoutIO<C> + Copy,
    C: HasNativeType + IsFixed + Clone,
    C::Native: FCSRepr,
    NativeInternalColumn<C>: From<Vec<C::Native>>,
{
    type DfTarget = ColumnGroup<FFDataFrame<NativeColumn<C>>, FFDataFrameFamily, I, L, M, ORD>;

    fn h_read_fixed_df<R: Read>(
        &self,
        h: &mut BufReader<R>,
        nrows: usize,
        conf: &ReadEventsConfig,
    ) -> IOResult<Self::DfTarget, ReadDataframeError> {
        let mut row_buf = RowBuffer::init(conf.row_buffer_size, nrows, self.event_width());
        let ncols = self.columns().len();
        let mut columns = vec![vec![C::Native::default(); nrows]; ncols];

        self.byte_layout
            .read_matrix(h, &mut row_buf, &mut columns)?;

        let data = columns
            .into_iter()
            .map(InternalColumn::from)
            .zip(self.container.iter().cloned())
            .map(|(data, range)| NativeColumn::new(range, data));
        let df = FFDataFrame::try_new(data).expect("column lengths are the same");

        Ok(ColumnGroup::new(df, self.byte_layout))
    }
}

// ASCII-specific loop which involves possibility of failure after reading every
// value (slower, requires branching)
impl<M, const ORD: bool> FixedRead
    for ColumnGroup<Vec<FixedAsciiRange>, VecFamily, FixedAsciiCol, NoByteOrd<ORD>, M, ORD>
{
    type DfTarget = ColumnGroup<
        FFDataFrame<NativeColumn<FixedAsciiRange>>,
        FFDataFrameFamily,
        FixedAsciiCol,
        NoByteOrd<ORD>,
        M,
        ORD,
    >;

    fn h_read_fixed_df<R: Read>(
        &self,
        h: &mut BufReader<R>,
        nrows: usize,
        conf: &ReadEventsConfig,
    ) -> IOResult<Self::DfTarget, ReadDataframeError> {
        let row_width = self.event_width();

        let mut row_buf = RowBuffer::init(conf.row_buffer_size, nrows, row_width);

        let mut columns: Vec<_> = self
            .container
            .iter()
            .map(|r| RangedVec::new(*r, vec![0; nrows]))
            .collect();

        row_buf.read_char_matrix(h, &mut columns).map_err(|e| {
            e.fmap_once(ReadFixedAsciiError::from)
                .fmap_once(ReadAsciiError::from)
                .fmap_once(ReadDataframeError::from)
        })?;

        let data = columns.into_iter().map(NativeColumn::from);
        let df = FFDataFrame::try_new(data).unwrap();
        Ok(ColumnGroup::new(df, self.byte_layout))
    }
}

// variable uint impl which is slower since it has branching to deal with
// multiple widths
impl<M> FixedRead for ColumnGroup<Vec<AnyBitmask>, VecFamily, UvarCol, Endian, M, false> {
    type DfTarget =
        ColumnGroup<FFDataFrame<AnyBitmaskColumn>, FFDataFrameFamily, UvarCol, Endian, M, false>;

    fn h_read_fixed_df<R: Read>(
        &self,
        h: &mut BufReader<R>,
        nrows: usize,
        conf: &ReadEventsConfig,
    ) -> IOResult<Self::DfTarget, ReadDataframeError> {
        let mut row_buf = RowBuffer::init(conf.row_buffer_size, nrows, self.event_width());
        let mut columns: Vec<_> = self
            .container
            .iter()
            .map(|c| c.init_column(nrows))
            .collect();

        row_buf.read_any_uint_df(h, &mut columns, self.byte_layout)?;

        let data = columns.into_iter().map(AnyBitmaskColumn::from);
        let df = FFDataFrame::try_new(data).unwrap();

        Ok(ColumnGroup::new(df, self.byte_layout))
    }
}

// Loop for mixed layouts which will first try to read as one type and cast to
// others if there are multiple types of the same width (very fast since this
// reads a matrix) and falling back on a slower loop with branching to deal with
// multiple types/widths.
impl<M> FixedRead for ColumnGroup<Vec<MixedRange>, VecFamily, MixedCol, Endian, M, false> {
    type DfTarget =
        ColumnGroup<FFDataFrame<MixedColumn>, FFDataFrameFamily, MixedCol, Endian, M, false>;

    fn h_read_fixed_df<R: Read>(
        &self,
        h: &mut BufReader<R>,
        nrows: usize,
        conf: &ReadEventsConfig,
    ) -> IOResult<Self::DfTarget, ReadDataframeError> {
        let mut buf = RowBuffer::init(conf.row_buffer_size, nrows, self.event_width());
        let en = self.byte_layout;
        let cs = &self.container[..];

        let columns = if let Some(ret) =
            try_read_single::<_, _, F32Range>(h, cs, nrows, en, &mut buf)?
        {
            // If the types are all the same width (but not necessary the same
            // type), we can "cheat" and read the layout all as one type and
            // cast to other types after the fact. This will dramatically speed
            // up reading for massive files such as S8/A8.
            //
            // This is for 32-bit float+int
            ret
        } else if let Some(ret) = try_read_single::<_, _, F64Range>(h, cs, nrows, en, &mut buf)? {
            // ditto 64-bit
            ret
        } else {
            // Totally mixed layout, dispatch for each column. This will be
            // slower but is necessary to read each type correctly.
            let mut columns: Vec<_> = self
                .container
                .iter()
                .map(|c| c.init_column(nrows))
                .collect();
            buf.read_mixed_df(h, &mut columns, self.byte_layout)
                .map_err(|e| {
                    e.fmap_once(ReadFixedAsciiError::from)
                        .fmap_once(ReadAsciiError::from)
                        .fmap_once(ReadDataframeError::from)
                })?;
            columns
        };

        let data = columns.into_iter().map(MixedColumn::from);
        let df = FFDataFrame::try_new(data).unwrap();

        Ok(ColumnGroup::new(df, self.byte_layout))
    }
}

type Any4ByteRange = Any4ByteType<F32Range, Bitmask32>;

type Any8ByteRange = Any8ByteType<F64Range, Bitmask64>;

macro_rules! impl_single_width_range {
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

impl_single_width_range!(Any4ByteRange, f32, F32, Uint32);
impl_single_width_range!(Any8ByteRange, f64, F64, Uint64);

/// Try to read DATA using multiple types with a single width.
fn try_read_single<R, W, C>(
    h: &mut BufReader<R>,
    ranges: &[MixedRange],
    nrows: usize,
    endian: Endian,
    row_buf: &mut ReadBuffer,
) -> io::Result<Option<Vec<MixedVec>>>
where
    R: Read,
    Endian: ByteLayoutIO<C>,
    W: TryFrom<MixedRange>,
    (Vec<C::Native>, W): Into<MixedVec>,
    C: HasNativeType,
    C::Native: Default + Clone + FCSRepr,
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

// Implemement low level fixed writing
//
// This has the same assumptions and optimizations as the read methods above,
// except that there is no error case to deal with in the case as ASCII since
// all u64 numbers are valid ASCII (not vice versa)

trait FixedWrite {
    fn h_write_fixed_df<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        conf: &WriteDatasetInnerConfig,
    ) -> io::Result<()>;
}

// basic write loop for most use cases
impl<C, L, I, M, const ORD: bool> FixedWrite
    for ColumnGroup<FFDataFrame<NativeColumn<C>>, FFDataFrameFamily, I, L, M, ORD>
where
    L: ByteLayoutIO<C> + Copy,
    C: HasNativeType + IsBinary + Clone,
    C::Native: FCSRepr + PartialOrd,
    NativeColumn<C>: AsRef<[C::Native]>,
{
    fn h_write_fixed_df<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        conf: &WriteDatasetInnerConfig,
    ) -> io::Result<()> {
        let nrows = self.container.nrows();
        let mut row_buf = RowBuffer::init(conf.row_buffer_size, nrows, self.event_width());
        let cols: Vec<_> = self.container.iter().map(AsRef::as_ref).collect();
        self.byte_layout.write_matrix(h, &mut row_buf, &cols[..])
    }
}

// ASCII-specific loop
impl<M, const ORD: bool> FixedWrite
    for ColumnGroup<
        FFDataFrame<NativeColumn<FixedAsciiRange>>,
        FFDataFrameFamily,
        FixedAsciiCol,
        NoByteOrd<ORD>,
        M,
        ORD,
    >
{
    fn h_write_fixed_df<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        conf: &WriteDatasetInnerConfig,
    ) -> io::Result<()> {
        let nrows = self.container.nrows();
        let mut row_buf = RowBuffer::init(conf.row_buffer_size, nrows, self.event_width());
        let cols = self.container.as_ref();
        row_buf.write_char_matrix(h, cols)
    }
}

// variable uint-layout
impl<M> FixedWrite
    for ColumnGroup<FFDataFrame<AnyBitmaskColumn>, FFDataFrameFamily, UvarCol, Endian, M, false>
{
    fn h_write_fixed_df<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        conf: &WriteDatasetInnerConfig,
    ) -> io::Result<()> {
        let nrows = self.container.nrows();
        let mut row_buf = RowBuffer::init(conf.row_buffer_size, nrows, self.event_width());
        let cols = self.container.as_ref();
        row_buf.write_any_uint_df(h, cols, self.byte_layout)
    }
}

// mixed type layout
impl<M> FixedWrite
    for ColumnGroup<FFDataFrame<MixedColumn>, FFDataFrameFamily, MixedCol, Endian, M, false>
{
    fn h_write_fixed_df<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        conf: &WriteDatasetInnerConfig,
    ) -> io::Result<()> {
        let nrows = self.container.nrows();
        let mut buf = RowBuffer::init(conf.row_buffer_size, nrows, self.event_width());
        let en = self.byte_layout;
        let cols = self.container.as_ref();
        if try_write_single::<_, Any4ByteColumn, F32Range>(h, cols, en, &mut buf)?
            || try_write_single::<_, Any8ByteColumn, F64Range>(h, cols, en, &mut buf)?
        {
            Ok(())
        } else {
            buf.write_mixed_df(h, cols, self.byte_layout)
        }
    }
}

type Any4ByteColumn = Any4ByteType<NativeColumn<F32Range>, NativeColumn<Bitmask32>>;

type Any8ByteColumn = Any8ByteType<NativeColumn<F64Range>, NativeColumn<Bitmask64>>;

macro_rules! impl_single_width_column {
    ($t:ident, $i:ident, $f:ident, $u:ident) => {
        impl TryFrom<MixedColumn> for $t {
            type Error = ();
            fn try_from(value: MixedColumn) -> Result<Self, Self::Error> {
                match value {
                    AnyDatatype::$f(r) => Ok(Self::$f(r)),
                    AnyDatatype::Uint(AnyUint::$u(r)) => Ok(Self::$u(r)),
                    _ => Err(()),
                }
            }
        }

        impl AsRef<[$i]> for $t {
            fn as_ref(&self) -> &[$i] {
                match self {
                    $t::$f(r) => r.as_ref(),
                    $t::$u(r) => cast_slice(r.as_ref()),
                }
            }
        }
    };
}

impl_single_width_column!(Any4ByteColumn, f32, F32, Uint32);
impl_single_width_column!(Any8ByteColumn, f64, F64, Uint64);

/// Try to write DATA using multiple types with a single width.
fn try_write_single<W, T, C>(
    h: &mut BufWriter<W>,
    cols: &[MixedColumn],
    endian: Endian,
    write_buf: &mut WriteBuffer,
) -> io::Result<bool>
where
    W: Write,
    Endian: ByteLayoutIO<C>,
    T: TryFrom<MixedColumn> + AsRef<[C::Native]>,
    C: HasNativeType,
    C::Native: FCSRepr,
{
    if let Ok(cs) = cols
        .iter()
        .cloned()
        .map(T::try_from)
        .collect::<Result<Vec<_>, _>>()
    {
        let columns: Vec<_> = cs.iter().map(AsRef::as_ref).collect();
        ByteLayoutIO::<C>::write_matrix(&endian, h, write_buf, &columns[..])?;
        Ok(true)
    } else {
        Ok(false)
    }
}

// Implement column-level byte width for applicable data layouts
//
// This should apply to all except ASCII layouts since this trait can only
// return byte widths up to 8.

/// A data layout which has binary types with known widths.
trait HasBinaryColumns {
    fn col_bytes(&self) -> Vec<PrivBytes>;
}

impl<C, F, I, L, M, const ORD: bool> HasBinaryColumns for ColumnGroup<C, F, I, L, M, ORD>
where
    I: IsCol<F, ORD>,
    C: AsRef<[I::Inner]>,
    I::Inner: IsBinary,
{
    fn col_bytes(&self) -> Vec<PrivBytes> {
        self.container
            .as_ref()
            .iter()
            .map(IsBinary::bytes)
            .collect()
    }
}

impl<C08, C16, C24, C32, C40, C48, C56, C64> HasBinaryColumns
    for AnyUint<C08, C16, C24, C32, C40, C48, C56, C64>
where
    C08: HasBinaryColumns,
    C16: HasBinaryColumns,
    C24: HasBinaryColumns,
    C32: HasBinaryColumns,
    C40: HasBinaryColumns,
    C48: HasBinaryColumns,
    C56: HasBinaryColumns,
    C64: HasBinaryColumns,
{
    fn col_bytes(&self) -> Vec<PrivBytes> {
        match_any_uint!(self, x, x.col_bytes())
    }
}

// Implement data layout conversions

/// Convert layout to new FCS version
pub trait ConvertFromLayout<T>: Sized {
    fn convert_from_layout(value: T) -> LayoutConvertResult<Self>;
}

// this covers all 2.0 <-> 3.0 conversions
impl<F, T0, T1> ConvertFromLayout<AnyOrderedGroup<F, T0>> for AnyOrderedGroup<F, T1>
where
    F: Kind1,
    U08Col: IsCol<F, true>,
    U16Col: IsCol<F, true>,
    U24Col: IsCol<F, true>,
    U32Col: IsCol<F, true>,
    U40Col: IsCol<F, true>,
    U48Col: IsCol<F, true>,
    U56Col: IsCol<F, true>,
    U64Col: IsCol<F, true>,
    F32Col: IsCol<F, true>,
    F64Col: IsCol<F, true>,
    FixedAsciiCol: IsCol<F, true>,
    DelimAsciiCol: IsCol<F, true>,
{
    fn convert_from_layout(value: AnyOrderedGroup<F, T0>) -> LayoutConvertResult<Self> {
        LogResult::new_ok(value.phantom_into())
    }
}

// this covers all 2.0/3.0 -> 3.1 conversions
impl<F, D, T> ConvertFromLayout<AnyOrderedGroup<F, T>> for NonMixedEndianGroup<F, D>
where
    F: Kind1,
    U08Col: IsCol<F, false> + IsCol<F, true>,
    U16Col: IsCol<F, false> + IsCol<F, true>,
    U24Col: IsCol<F, false> + IsCol<F, true>,
    U32Col: IsCol<F, false> + IsCol<F, true>,
    U40Col: IsCol<F, false> + IsCol<F, true>,
    U48Col: IsCol<F, false> + IsCol<F, true>,
    U56Col: IsCol<F, false> + IsCol<F, true>,
    U64Col: IsCol<F, false> + IsCol<F, true>,
    F32Col: IsCol<F, false> + IsCol<F, true>,
    F64Col: IsCol<F, false> + IsCol<F, true>,
    FixedAsciiCol: IsCol<F, false> + IsCol<F, true>,
    DelimAsciiCol: IsCol<F, false> + IsCol<F, true>,
    UvarCol: IsCol<F, false>,
    AnyAsciiGroup3_1<F, D>: ConvertFromLayout<AnyAsciiGroup2_0<F, T>>,
    EndianGroup<F, U08Col, D>: ConvertFromLayout<OrderedGroup<F, U08Col, T>>,
    EndianGroup<F, U16Col, D>: ConvertFromLayout<OrderedGroup<F, U16Col, T>>,
    EndianGroup<F, U24Col, D>: ConvertFromLayout<OrderedGroup<F, U24Col, T>>,
    EndianGroup<F, U32Col, D>: ConvertFromLayout<OrderedGroup<F, U32Col, T>>,
    EndianGroup<F, U40Col, D>: ConvertFromLayout<OrderedGroup<F, U40Col, T>>,
    EndianGroup<F, U48Col, D>: ConvertFromLayout<OrderedGroup<F, U48Col, T>>,
    EndianGroup<F, U56Col, D>: ConvertFromLayout<OrderedGroup<F, U56Col, T>>,
    EndianGroup<F, U64Col, D>: ConvertFromLayout<OrderedGroup<F, U64Col, T>>,
    EndianGroup<F, F32Col, D>: ConvertFromLayout<OrderedGroup<F, F32Col, T>>,
    EndianGroup<F, F64Col, D>: ConvertFromLayout<OrderedGroup<F, F64Col, T>>,
{
    fn convert_from_layout(value: AnyOrderedGroup<F, T>) -> LayoutConvertResult<Self> {
        match value {
            AnyDatatype::Ascii(x) => {
                AnyAsciiGroup::convert_from_layout(x).map_ok_value(Self::Ascii)
            }
            AnyDatatype::Uint(x) => {
                match_any_uint!(x, y, {
                    EndianGroup::convert_from_layout(y)
                        .map_ok_value(AnyUint::from)
                        .map_ok_value(AnyEndianUint::Single)
                        .map_ok_value(AnyDatatype::Uint)
                })
            }
            AnyDatatype::F32(x) => {
                EndianGroup::convert_from_layout(x).map_ok_value(AnyDatatype::F32)
            }
            AnyDatatype::F64(x) => {
                EndianGroup::convert_from_layout(x).map_ok_value(AnyDatatype::F64)
            }
        }
    }
}

// this covers all 3.1 -> 2.0/3.0 conversions
impl<F, D, T> ConvertFromLayout<NonMixedEndianGroup<F, D>> for AnyOrderedGroup<F, T>
where
    F: Kind1,
    U08Col: IsCol<F, false> + IsCol<F, true>,
    U16Col: IsCol<F, false> + IsCol<F, true>,
    U24Col: IsCol<F, false> + IsCol<F, true>,
    U32Col: IsCol<F, false> + IsCol<F, true>,
    U40Col: IsCol<F, false> + IsCol<F, true>,
    U48Col: IsCol<F, false> + IsCol<F, true>,
    U56Col: IsCol<F, false> + IsCol<F, true>,
    U64Col: IsCol<F, false> + IsCol<F, true>,
    F32Col: IsCol<F, false> + IsCol<F, true>,
    F64Col: IsCol<F, false> + IsCol<F, true>,
    FixedAsciiCol: IsCol<F, false> + IsCol<F, true>,
    DelimAsciiCol: IsCol<F, false> + IsCol<F, true>,
    UvarCol: IsCol<F, false>,
    AnyAsciiGroup2_0<F, T>: ConvertFromLayout<AnyAsciiGroup3_1<F, D>>,
    OrderedGroup<F, U08Col, T>: ConvertFromLayout<EndianGroup<F, U08Col, D>>,
    OrderedGroup<F, U16Col, T>: ConvertFromLayout<EndianGroup<F, U16Col, D>>,
    OrderedGroup<F, U24Col, T>: ConvertFromLayout<EndianGroup<F, U24Col, D>>,
    OrderedGroup<F, U32Col, T>: ConvertFromLayout<EndianGroup<F, U32Col, D>>,
    OrderedGroup<F, U40Col, T>: ConvertFromLayout<EndianGroup<F, U40Col, D>>,
    OrderedGroup<F, U48Col, T>: ConvertFromLayout<EndianGroup<F, U48Col, D>>,
    OrderedGroup<F, U56Col, T>: ConvertFromLayout<EndianGroup<F, U56Col, D>>,
    OrderedGroup<F, U64Col, T>: ConvertFromLayout<EndianGroup<F, U64Col, D>>,
    OrderedGroup<F, F32Col, T>: ConvertFromLayout<EndianGroup<F, F32Col, D>>,
    OrderedGroup<F, F64Col, T>: ConvertFromLayout<EndianGroup<F, F64Col, D>>,
    NonMixedEndianGroup<F, D>: NormalizableLayout,
    F::Type<<UvarCol as IsCol<F, false>>::Inner>: AsRef<[<UvarCol as IsCol<F, false>>::Inner]>,
    <UvarCol as IsCol<F, false>>::Inner: IsFixed,
{
    fn convert_from_layout(mut value: NonMixedEndianGroup<F, D>) -> LayoutConvertResult<Self> {
        value.normalize();
        match value {
            AnyDatatype::Ascii(x) => {
                AnyAsciiGroup::convert_from_layout(x).map_ok_value(Self::Ascii)
            }
            AnyDatatype::Uint(x) => match x {
                AnyEndianUint::Multi(y) => y.conversion_fail_by_width(),
                AnyEndianUint::Single(y) => {
                    match_any_uint!(y, z, {
                        OrderedGroup::convert_from_layout(z)
                            .map_ok_value(AnyUint::from)
                            .map_ok_value(Self::Uint)
                    })
                }
            },
            AnyDatatype::F32(x) => OrderedGroup::convert_from_layout(x).map_ok_value(Self::F32),
            AnyDatatype::F64(x) => OrderedGroup::convert_from_layout(x).map_ok_value(Self::F64),
        }
    }
}

// this covers all x.y -> 3.2 conversions
impl<F, A, I, F32, F64> ConvertFromLayout<AnyDatatype<A, I, F32, F64>> for Any3_2Layout<F>
where
    NonMixedEndianGroup<F, Option<NumType>>: ConvertFromLayout<AnyDatatype<A, I, F32, F64>>,
    F: Kind1,
    U08Col: IsCol<F, false>,
    U16Col: IsCol<F, false>,
    U24Col: IsCol<F, false>,
    U32Col: IsCol<F, false>,
    U40Col: IsCol<F, false>,
    U48Col: IsCol<F, false>,
    U56Col: IsCol<F, false>,
    U64Col: IsCol<F, false>,
    F32Col: IsCol<F, false>,
    F64Col: IsCol<F, false>,
    UvarCol: IsCol<F, false>,
    MixedCol: IsCol<F, false>,
    FixedAsciiCol: IsCol<F, false>,
    DelimAsciiCol: IsCol<F, false>,
{
    fn convert_from_layout(value: AnyDatatype<A, I, F32, F64>) -> LayoutConvertResult<Self> {
        NonMixedEndianGroup::convert_from_layout(value).map_ok_value(Self::NonMixed)
    }
}

// this covers all 3.2 -> x.y conversions
impl<F, A, I, F32, F64> ConvertFromLayout<Any3_2Group<F>> for AnyDatatype<A, I, F32, F64>
where
    Self: ConvertFromLayout<NonMixedEndianGroup<F, Option<NumType>>>,
    F: Kind1,
    U08Col: IsCol<F, false>,
    U16Col: IsCol<F, false>,
    U24Col: IsCol<F, false>,
    U32Col: IsCol<F, false>,
    U40Col: IsCol<F, false>,
    U48Col: IsCol<F, false>,
    U56Col: IsCol<F, false>,
    U64Col: IsCol<F, false>,
    F32Col: IsCol<F, false>,
    F64Col: IsCol<F, false>,
    FixedAsciiCol: IsCol<F, false>,
    DelimAsciiCol: IsCol<F, false>,
    UvarCol: IsCol<F, false>,
    MixedCol: IsCol<F, false>,
    Any3_2Group<F>: NormalizableLayout,
    MixedGroup<F>: LayoutDatatype,
{
    fn convert_from_layout(mut value: Any3_2Group<F>) -> LayoutConvertResult<Self> {
        value.normalize();
        match value {
            Any3_2::NonMixed(x) => Self::convert_from_layout(x),
            Any3_2::Mixed(x) => x.conversion_fail_by_datatype(),
        }
    }
}

// used for 3.1 <-> 3.2
impl<F, D0, D1> ConvertFromLayout<NonMixedEndianGroup<F, D0>> for NonMixedEndianGroup<F, D1>
where
    F: Kind1,
    U08Col: IsCol<F, false>,
    U16Col: IsCol<F, false>,
    U24Col: IsCol<F, false>,
    U32Col: IsCol<F, false>,
    U40Col: IsCol<F, false>,
    U48Col: IsCol<F, false>,
    U56Col: IsCol<F, false>,
    U64Col: IsCol<F, false>,
    F32Col: IsCol<F, false>,
    F64Col: IsCol<F, false>,
    FixedAsciiCol: IsCol<F, false>,
    DelimAsciiCol: IsCol<F, false>,
    UvarCol: IsCol<F, false>,
{
    fn convert_from_layout(value: NonMixedEndianGroup<F, D0>) -> LayoutConvertResult<Self> {
        LogResult::new_ok(value.phantom_into())
    }
}

// used for 2.0/3.0 -> 3.1/3.2
impl<F, I, D, T> ConvertFromLayout<OrderedGroup<F, I, T>> for EndianGroup<F, I, D>
where
    F: Kind1,
    I: IsCol<F, false> + IsCol<F, true, Inner = <I as IsCol<F, false>>::Inner>,
    <I as IsCol<F, true>>::Layout:
        TryInto<<I as IsCol<F, false>>::Layout, Error = OrderedToEndianError>,
{
    fn convert_from_layout(value: OrderedGroup<F, I, T>) -> LayoutConvertResult<Self> {
        match value.byte_layout_try_into() {
            Ok(x) => LogResult::new_ok(x.phantom_into()),
            Err(e) => LogResult::new_err(LayoutConvertError::OrderToEndian(e)),
        }
    }
}

// used for 3.1/3.2 -> 2.0/3.0
impl<F, I, D, T> ConvertFromLayout<EndianGroup<F, I, D>> for OrderedGroup<F, I, T>
where
    F: Kind1,
    I: IsCol<F, false> + IsCol<F, true, Inner = <I as IsCol<F, false>>::Inner>,
    <I as IsCol<F, false>>::Layout: Into<<I as IsCol<F, true>>::Layout>,
{
    fn convert_from_layout(value: EndianGroup<F, I, D>) -> LayoutConvertResult<Self> {
        LogResult::new_ok(value.byte_layout_into().phantom_into())
    }
}

// used for any 2.0 <-> 3.0 <-> 3.1 <-> 3.2
impl<F, const ORD1: bool, const ORD2: bool, M1, M2> ConvertFromLayout<AnyAsciiGroup<F, ORD1, M1>>
    for AnyAsciiGroup<F, ORD2, M2>
where
    F: Kind1,
    FixedAsciiCol:
        IsCol<F, ORD1> + IsCol<F, ORD2, Inner = <FixedAsciiCol as IsCol<F, ORD1>>::Inner>,
    DelimAsciiCol:
        IsCol<F, ORD1> + IsCol<F, ORD2, Inner = <DelimAsciiCol as IsCol<F, ORD1>>::Inner>,
    <FixedAsciiCol as IsCol<F, ORD1>>::Layout: Into<<FixedAsciiCol as IsCol<F, ORD2>>::Layout>,
    <DelimAsciiCol as IsCol<F, ORD1>>::Layout: Into<<DelimAsciiCol as IsCol<F, ORD2>>::Layout>,
{
    fn convert_from_layout(value: AnyAsciiGroup<F, ORD1, M1>) -> LayoutConvertResult<Self> {
        LogResult::new_ok(value.phantom_into().byte_layout_into())
    }
}

// Implement marker conversions
//
// This trait converts zero-sized marker types from one type to another. This is
// totally lossless. These types only exist to allow specialized impls to be
// created for layouts which otherwise have exactly the same data in Rust-land
// (example, layouts in 2.0 and 3.0+ might have the same data but will depend on
// $TOT differently because it is optional in 3.0+).

/// A type which has nested marker types that can be losslessly converted
pub trait PhantomInto {
    type Target<M>;
    fn phantom_into<Mf>(self) -> Self::Target<Mf>;
}

impl<D, F> PhantomInto for AnyAscii<D, F>
where
    D: PhantomInto,
    F: PhantomInto,
{
    type Target<Mf> = AnyAscii<D::Target<Mf>, F::Target<Mf>>;

    fn phantom_into<Mf>(self) -> Self::Target<Mf> {
        match_map_ascii!(self, x, x.phantom_into())
    }
}

impl<A, I, F32, F64> PhantomInto for AnyDatatype<A, I, F32, F64>
where
    A: PhantomInto,
    I: PhantomInto,
    F32: PhantomInto,
    F64: PhantomInto,
{
    type Target<Mf> = AnyDatatype<A::Target<Mf>, I::Target<Mf>, F32::Target<Mf>, F64::Target<Mf>>;

    fn phantom_into<Mf>(self) -> Self::Target<Mf> {
        match_map_datatype!(self, x, x.phantom_into())
    }
}

impl<C08, C16, C24, C32, C40, C48, C56, C64> PhantomInto
    for AnyUint<C08, C16, C24, C32, C40, C48, C56, C64>
where
    C08: PhantomInto,
    C16: PhantomInto,
    C24: PhantomInto,
    C32: PhantomInto,
    C40: PhantomInto,
    C48: PhantomInto,
    C56: PhantomInto,
    C64: PhantomInto,
{
    type Target<Mf> = AnyUint<
        C08::Target<Mf>,
        C16::Target<Mf>,
        C24::Target<Mf>,
        C32::Target<Mf>,
        C40::Target<Mf>,
        C48::Target<Mf>,
        C56::Target<Mf>,
        C64::Target<Mf>,
    >;

    fn phantom_into<Mf>(self) -> Self::Target<Mf> {
        match_map_uint!(self, x, x.phantom_into())
    }
}

impl<W0, W> PhantomInto for AnyEndianUint<W0, W>
where
    W0: PhantomInto,
    W: PhantomInto,
{
    type Target<Mf> = AnyEndianUint<W0::Target<Mf>, W::Target<Mf>>;

    fn phantom_into<Mf>(self) -> Self::Target<Mf> {
        match_map_endian_uint!(self, x, x.phantom_into())
    }
}

impl<C, F, I, T, M, const ORD: bool> PhantomInto for ColumnGroup<C, F, I, T, M, ORD> {
    type Target<Mf> = ColumnGroup<C, F, I, T, Mf, ORD>;

    fn phantom_into<Mf>(self) -> Self::Target<Mf> {
        ColumnGroup::new(self.container, self.byte_layout)
    }
}

// Implement insertion operations for layouts
//
// This is tricky for several reasons:
// 1. Inserting a mixed header can never fail but the rest can because of type
//    mismatches. Therefore we need a customizable error.
// 2. Inserting a mixed range/column must have an accompanying type with it
//    since otherwise it is unclear what the resulting type should be. For
//    all other layouts this is not a problem since the type is inherent to the
//    layout itself.
// 3. If we insert a column along with a range, we need to check the type
//    of the column as well.

/// A type which can accept a new column.
#[delegatable_trait]
pub trait Insertable<Column> {
    /// Error to emit if new column is not compatible with existing columns.
    type Error;

    /// Insert a new column at index.
    ///
    /// This will panic if index is out of bounds.
    fn insert_nocheck(&mut self, index: MeasIndex, col: Column) -> Result<(), Self::Error> {
        self.insert_or_push(Some(index), col)
    }

    /// Push new column to the right of the current column vector.
    fn push(&mut self, col: Column) -> Result<(), Self::Error> {
        self.insert_or_push(None, col)
    }

    fn insert_or_push(&mut self, index: Option<MeasIndex>, col: Column) -> Result<(), Self::Error>;
}

impl<D, F> Insertable<Range> for AnyAscii<D, F>
where
    D: Insertable<Range>,
    F: Insertable<Range>,
    InsertRangeError: From<D::Error> + From<F::Error>,
{
    type Error = InsertRangeError;

    fn insert_or_push(&mut self, index: Option<MeasIndex>, col: Range) -> Result<(), Self::Error> {
        match_any_ascii!(self, x, x.insert_or_push(index, col)?);
        Ok(())
    }
}

impl<D, F> Insertable<FixedAsciiRange> for AnyAscii<D, F>
where
    D: Insertable<DelimAsciiRange, Error = Infallible>,
    F: Insertable<FixedAsciiRange, Error = Infallible>,
{
    type Error = Infallible;

    fn insert_or_push(
        &mut self,
        index: Option<MeasIndex>,
        col: FixedAsciiRange,
    ) -> Result<(), Self::Error> {
        match self {
            Self::Delimited(x) => x.insert_or_push(index, col.into()),
            Self::Fixed(x) => x.insert_or_push(index, col),
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

    fn insert_or_push(&mut self, index: Option<MeasIndex>, col: Range) -> Result<(), Self::Error> {
        match_any_datatype!(self, x, {
            x.insert_or_push(index, col).map_err(Self::Error::from)
        })
    }
}

// Insert any width of int range into a variable layout. This cannot fail since
// any single-width layout can be made into a variable-width layout which can
// accept any width.
impl<D> Insertable<AnyBitmask> for AnyEndianUintHeaders<D> {
    type Error = Infallible;

    fn insert_or_push(
        &mut self,
        index: Option<MeasIndex>,
        col: AnyBitmask,
    ) -> Result<(), Self::Error> {
        match self {
            Self::Single(x) => match_any_uint!(x, y, {
                if let Ok(r) = col.try_into() {
                    if let Some(i) = index {
                        y.container.insert(i.into(), r);
                    } else {
                        y.container.push(r);
                    }
                    Ok(())
                } else {
                    let mut new = mem::take(y).map_inner(AnyBitmask::from);
                    new.insert_or_push(index, col)?;
                    *self = Self::Multi(new);
                    Ok(())
                }
            }),
            Self::Multi(x) => x.insert_or_push(index, col),
        }
    }
}

// // Insert general or specific range into variable int layout.
// impl<D> Insertable<RangeOrBitmaskRange> for AnyEndianUintHeaders<D> {
//     type Error = InsertRangeError;

//     fn insert_nocheck(
//         &mut self,
//         index: MeasIndex,
//         col: RangeOrBitmaskRange,
//     ) -> Result<(), Self::Error> {
//         let i = index.into();
//         match (self, col) {
//             (Self::Single(x), RangeOrType::Range(r)) => x.insert_nocheck(index, r),
//             (Self::Single(x), RangeOrType::Specific(r)) => {}
//             //     match_any_uint!(x, y, {
//             //     if let Ok(r) = col.try_into() {
//             //         y.container.insert(i, r);
//             //     } else {
//             //         let mut new = mem::take(y).map_inner(AnyBitmask::from);
//             //         new.container.insert(i, col);
//             //         *self = Self::Multi(new);
//             //     }
//             // }),
//             (Self::Multi(x), RangeOrType::Range(_)) => return Err(MismatchTypeRangeError.into()),
//             (Self::Multi(x), RangeOrType::Specific(r)) => x.container.insert(i, r),
//         }
//         Ok(())
//     }

//     fn push(&mut self, col: RangeOrBitmaskRange) -> Result<(), Self::Error> {
//         match self {
//             Self::Single(x) => match_any_uint!(x, y, {
//                 if let Ok(r) = col.try_into() {
//                     y.container.push(r);
//                 } else {
//                     let mut new = mem::take(y).map_inner(AnyBitmask::from);
//                     new.container.push(col);
//                     *self = Self::Multi(new);
//                 }
//             }),
//             Self::Multi(x) => x.container.push(col),
//         }
//         Ok(())
//     }
// }

// Insert any type of range into a nonmmixed layout, which may fail. This is
// better than just inserting a raw range since it gives the caller control over
// the final column type.
impl<A, I, F32, F64> Insertable<MixedRange> for AnyDatatype<A, I, F32, F64>
where
    A: Insertable<FixedAsciiRange, Error = Infallible>,
    I: Insertable<AnyBitmask, Error = Infallible>,
    F32: Insertable<F32Range, Error = Infallible>,
    F64: Insertable<F64Range, Error = Infallible>,
{
    type Error = InsertRangeError;

    fn insert_or_push(
        &mut self,
        index: Option<MeasIndex>,
        col: MixedRange,
    ) -> Result<(), Self::Error> {
        match (self, col) {
            (Self::Ascii(x), AnyDatatype::Ascii(r)) => x.insert_or_push(index, r),
            (Self::Uint(x), AnyDatatype::Uint(r)) => x.insert_or_push(index, r),
            (Self::F32(x), AnyDatatype::F32(r)) => x.insert_or_push(index, r),
            (Self::F64(x), AnyDatatype::F64(r)) => x.insert_or_push(index, r),
            _ => return Err(MismatchTypeRangeError.into()),
        };
        Ok(())
    }
}

// Insert any type of range into a mixed layout. This cannot fail since any
// single-type layout can be made into a mixed-type layout which can accept
// anything.
impl Insertable<MixedRange> for DataHeaders3_2 {
    type Error = Infallible;

    fn insert_or_push(
        &mut self,
        index: Option<MeasIndex>,
        col: MixedRange,
    ) -> Result<(), Infallible> {
        macro_rules! go_mixed {
            ($from:expr) => {{
                *self = Self::Mixed(
                    mem::take($from)
                        .map_inner(MixedRange::from)
                        .byte_layout_into(),
                );
                self.insert_or_push(index, col);
            }};
        }
        macro_rules! go {
            ($var:ident, $from:expr) => {
                if let AnyDatatype::$var(r) = col {
                    if let Some(i) = index {
                        $from.container.insert(i.into(), r.into());
                    } else {
                        $from.container.push(r.into());
                    }
                } else {
                    go_mixed!($from);
                }
            };
        }

        match self {
            Self::Mixed(x) => x.insert_or_push(index, col)?,
            Self::NonMixed(x) => match x {
                AnyDatatype::Ascii(y) => match y {
                    AnyAscii::Delimited(z) => go!(Ascii, z),
                    AnyAscii::Fixed(z) => go!(Ascii, z),
                },
                AnyDatatype::Uint(y) => match y {
                    AnyEndianUint::Single(z) => {
                        if let AnyDatatype::Uint(r) = col {
                            let Ok(()) = y.insert_or_push(index, r);
                        } else {
                            match_any_uint!(z, s, go_mixed!(s));
                        }
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

impl<C: FromRange, I, L, M, const ORD: bool> Insertable<Range>
    for ColumnGroup<Vec<C>, VecFamily, I, L, M, ORD>
{
    type Error = C::Error;

    fn insert_or_push(&mut self, index: Option<MeasIndex>, col: Range) -> Result<(), Self::Error> {
        if let Some(i) = index {
            self.container.insert(i.into(), C::from_range(col)?.native);
        } else {
            self.container.push(C::from_range(col)?.native);
        }
        Ok(())
    }
}

impl<C, I, L, M, const ORD: bool> Insertable<C> for ColumnGroup<Vec<C>, VecFamily, I, L, M, ORD> {
    type Error = Infallible;

    fn insert_or_push(&mut self, index: Option<MeasIndex>, col: C) -> Result<(), Self::Error> {
        if let Some(i) = index {
            self.container.insert(i.into(), col);
        } else {
            self.container.push(col);
        }
        Ok(())
    }
}

// impl<D> Insertable<RangeAndColumn> for AnyEndianUintDataFrame<D> {
//     type Error = ();

//     fn insert_nocheck(&mut self, index: MeasIndex, col: RangeAndColumn) -> Result<(), Self::Error> {
//         match self {
//             Self::Single(x) => match_any_uint!(x, y, {
//                 if y.insert_nocheck(index, col.clone()).is_err() {
//                     let new = mem::take(y).map_inner(AnyBitmask::from);
//                     *self = Self::Multi(new);
//                     return self.insert_nocheck(index, col);
//                 }
//                 Ok(())
//             }),
//             Self::Multi(x) => x.insert_nocheck(index, col),
//         }
//     }

//     fn push(&mut self, col: RangeAndColumn) -> Result<(), Self::Error> {
//         match self {
//             Self::Single(x) => match_any_uint!(x, y, {
//                 if y.push(col.clone()).is_err() {
//                     let new = mem::take(y).map_inner(AnyBitmask::from);
//                     *self = Self::Multi(new);
//                     return self.push(col);
//                 }
//                 Ok(())
//             }),
//             Self::Multi(x) => x.push(col),
//         }
//     }
// }

// impl<A, I, F32, F64> Insertable<RangeAndColumn> for AnyDatatype<A, I, F32, F64>
// where
//     A: Insertable<RangeAndColumn>,
//     I: Insertable<RangeAndColumn>,
//     F32: Insertable<RangeAndColumn>,
//     F64: Insertable<RangeAndColumn>,
//     InsertRangeError: From<A::Error> + From<I::Error> + From<F32::Error> + From<F64::Error>,
// {
//     type Error = InsertRangeError;

//     fn insert_nocheck(&mut self, index: MeasIndex, col: RangeAndColumn) -> Result<(), Self::Error> {
//         match_any_datatype!(self, x, {
//             x.insert_nocheck(index, col).map_err(Self::Error::from)
//         })
//     }

//     fn push(&mut self, col: RangeAndColumn) -> Result<(), Self::Error> {
//         match_any_datatype!(self, x, x.push(col).map_err(Self::Error::from))
//     }
// }

// // Insert range and column
// //
// // ASSUME length is correct for new column, caller must verify this
// impl<H, T, R, I, L, M, const ORD: bool> Insertable<RangeAndColumn>
//     for ColumnGroup<FFDataFrame<AnnotatedColumn<H, T, R>>, FFDataFrameFamily, I, L, M, ORD>
// where
//     RangeAndColumn: TryInto<AnnotatedColumn<H, T, R>>,
// {
//     type Error = <RangeAndColumn as TryInto<AnnotatedColumn<H, T, R>>>::Error;

//     fn insert_nocheck(&mut self, index: MeasIndex, col: RangeAndColumn) -> Result<(), Self::Error> {
//         self.container
//             .insert_column_nocheck(index.into(), col.try_into()?);
//         Ok(())
//     }

//     fn push(&mut self, col: RangeAndColumn) -> Result<(), Self::Error> {
//         self.container.push_column_nocheck(col.try_into()?);
//         Ok(())
//     }
// }

// impl<H, T, R> TryFrom<RangeAndColumn> for AnnotatedColumn<H, T, R>
// where
//     H: FromRange,
//     InternalColumn<T, R>: FromColumn<AnyPrimitiveColumn>,
// {
//     type Error = InsertRangeAndColumnError<H::Error>;

//     fn try_from(value: RangeAndColumn) -> Result<Self, Self::Error> {
//         let (r, c) = value;
//         let header = H::from_range(r)
//             .map_err(InsertRangeAndColumnError::Range)?
//             .native;
//         let data = InternalColumn::from_column(c)
//             .into_err()
//             .map_err(InsertRangeAndColumnError::Column)?;
//         Ok(Self::new(header, data))
//     }
// }

// pub enum InsertRangeAndColumnError<E> {
//     Range(E),
//     Column(CastColError),
// }

// Implement removable operations for layouts.
//
// Unlike insertions, this cannot fail which makes this trait simpler.
// Also (for now) these only return a range (ie $PnR value).

// TODO return type of mixed type which was removed?

/// A type which can have a column element removed from it.
#[delegatable_trait]
pub trait Removable<C>: Sized {
    /// Remove a column.
    ///
    /// Will panic if index is out of bounds.
    fn remove_nocheck(&mut self, index: MeasIndex) -> C;
}

impl<C, I, L, M, const ORD: bool> Removable<Range> for ColumnGroup<Vec<C>, VecFamily, I, L, M, ORD>
where
    for<'c> Range: From<&'c C>,
{
    fn remove_nocheck(&mut self, index: MeasIndex) -> Range {
        debug_assert!(
            usize::from(index) <= self.container.len(),
            "Index should be less than/equal to column number"
        );
        Range::from(&self.container.remove(index.into()))
    }
}

pub type RangeAndColumn = (Range, AnyPrimitiveColumn);

impl<C, I, L, M, const ORD: bool> Removable<RangeAndColumn>
    for ColumnGroup<FFDataFrame<C>, FFDataFrameFamily, I, L, M, ORD>
where
    for<'c> Range: From<&'c C>,
    for<'c> AnyPrimitiveColumn: From<&'c C>,
{
    fn remove_nocheck(&mut self, index: MeasIndex) -> RangeAndColumn {
        debug_assert!(
            usize::from(index) <= self.container.ncols(),
            "Index should be less than/equal to column number"
        );
        let c = &self.container.remove(index.into());
        (Range::from(c), AnyPrimitiveColumn::from(c))
    }
}

// Implement operations specific to 2.0/3.0 layouts.

/// Standardized operations on ordered layouts
#[delegatable_trait]
pub trait OrderedLayoutOps: Sized {
    fn byte_order(&self) -> ByteOrd2_0;

    fn endianness(&self) -> Option<Endian> {
        self.byte_order().try_into().ok()
    }
}

impl<C, F, I, L, M, const ORD: bool> OrderedLayoutOps for ColumnGroup<C, F, I, L, M, ORD>
where
    L: Copy,
    ByteOrd2_0: From<L>,
{
    fn byte_order(&self) -> ByteOrd2_0 {
        self.byte_layout.into()
    }
}

// Implement NormalizableLayout
//
// Most layouts will noop since they only have one possibility. The only two
// exceptions are Endian Integer layouts which can have one or many widths
// (normalization will try to convert to explicit single layout) and mixed-type
// layouts (normalization will try to reduce to a single type, with the nuance
// that these also may have mixed width integer layouts inside them).
//
// The concept of "normalization" exists to make conversion and performance
// optimization easier. By providing a type which represents the simpler case of
// a more general type, we can make specialized impls for these simpler types
// which makes calling code simpler.
//
// This trait only applies to layouts complicated enough to contain multiple
// types which can be equivalent to each other.

/// A layout that can be simplified into another layout of the same type.
pub trait NormalizableLayout {
    fn normalize(&mut self);
}

impl NormalizableLayout for DataHeaders2_0 {
    fn normalize(&mut self) {}
}

impl NormalizableLayout for DataFrame2_0 {
    fn normalize(&mut self) {}
}

impl NormalizableLayout for DataHeaders3_0 {
    fn normalize(&mut self) {}
}

impl NormalizableLayout for DataFrame3_0 {
    fn normalize(&mut self) {}
}

impl<A, I: NormalizableLayout, F32, F64> NormalizableLayout for AnyDatatype<A, I, F32, F64> {
    fn normalize(&mut self) {
        if let Self::Uint(x) = self {
            x.normalize();
        }
    }
}

impl<F, D> NormalizableLayout for AnyEndianUintGroup<F, D>
where
    Self: Default,
    F: Kind1,
    VariableUintGroup<F, D>: HasBinaryColumns,
    UvarCol: IsCol<F, false, Layout = Endian>,
    U08Col: IsCol<F, false, Layout = Endian>,
    U16Col: IsCol<F, false, Layout = Endian>,
    U24Col: IsCol<F, false, Layout = Endian>,
    U32Col: IsCol<F, false, Layout = Endian>,
    U40Col: IsCol<F, false, Layout = Endian>,
    U48Col: IsCol<F, false, Layout = Endian>,
    U56Col: IsCol<F, false, Layout = Endian>,
    U64Col: IsCol<F, false, Layout = Endian>,
    F::Type<<UvarCol as IsCol<F, false>>::Inner>: Functor<<UvarCol as IsCol<F, false>>::Inner>,
    F::Type<<U32Col as IsCol<F, false>>::Inner>: Default,
    <UvarCol as IsCol<F, false>>::Inner: ColInto<<U08Col as IsCol<F, false>>::Inner>
        + ColInto<<U16Col as IsCol<F, false>>::Inner>
        + ColInto<<U24Col as IsCol<F, false>>::Inner>
        + ColInto<<U32Col as IsCol<F, false>>::Inner>
        + ColInto<<U40Col as IsCol<F, false>>::Inner>
        + ColInto<<U48Col as IsCol<F, false>>::Inner>
        + ColInto<<U56Col as IsCol<F, false>>::Inner>
        + ColInto<<U64Col as IsCol<F, false>>::Inner>,
{
    fn normalize(&mut self) {
        *self = match mem::take(self) {
            Self::Single(x) => Self::Single(x),
            Self::Multi(x) => {
                if let Some((c0, cs)) = x.col_bytes().split_first() {
                    if cs.iter().all(|c| c0 == c) {
                        let new = match c0 {
                            PrivBytes::B1 => AnyUint::Uint08(x.map_inner(ColInto::col_into)),
                            PrivBytes::B2 => AnyUint::Uint16(x.map_inner(ColInto::col_into)),
                            PrivBytes::B3 => AnyUint::Uint24(x.map_inner(ColInto::col_into)),
                            PrivBytes::B4 => AnyUint::Uint32(x.map_inner(ColInto::col_into)),
                            PrivBytes::B5 => AnyUint::Uint40(x.map_inner(ColInto::col_into)),
                            PrivBytes::B6 => AnyUint::Uint48(x.map_inner(ColInto::col_into)),
                            PrivBytes::B7 => AnyUint::Uint56(x.map_inner(ColInto::col_into)),
                            PrivBytes::B8 => AnyUint::Uint64(x.map_inner(ColInto::col_into)),
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
                    Self::Single(AnyUint::Uint32(ColumnGroup::default()))
                }
            }
        };
    }
}

impl<F> NormalizableLayout for Any3_2Group<F>
where
    Self: Default,
    F: Kind1,
    UvarCol: IsCol<F, false, Layout = Endian>,
    U08Col: IsCol<F, false, Layout = Endian>,
    U16Col: IsCol<F, false, Layout = Endian>,
    U24Col: IsCol<F, false, Layout = Endian>,
    U32Col: IsCol<F, false, Layout = Endian>,
    U40Col: IsCol<F, false, Layout = Endian>,
    U48Col: IsCol<F, false, Layout = Endian>,
    U56Col: IsCol<F, false, Layout = Endian>,
    U64Col: IsCol<F, false, Layout = Endian>,
    F32Col: IsCol<F, false, Layout = Endian>,
    F64Col: IsCol<F, false, Layout = Endian>,
    DelimAsciiCol: IsCol<F, false>,
    FixedAsciiCol: IsCol<F, false, Layout = NoByteOrd<false>>,
    MixedCol: IsCol<F, false, Layout = Endian>,
    AnyEndianUintGroup<F, Option<NumType>>: NormalizableLayout,
    F::Type<<F32Col as IsCol<F, false>>::Inner>: Default,
    MixedGroup<F>: LayoutDatatype,
    F::Type<<MixedCol as IsCol<F, false>>::Inner>: Functor<<MixedCol as IsCol<F, false>>::Inner>,
    F::Type<<UvarCol as IsCol<F, false>>::Inner>: Functor<<UvarCol as IsCol<F, false>>::Inner>,
    <MixedCol as IsCol<F, false>>::Inner: ColInto<<F32Col as IsCol<F, false>>::Inner>
        + ColInto<<F64Col as IsCol<F, false>>::Inner>
        + ColInto<<UvarCol as IsCol<F, false>>::Inner>
        + ColInto<<FixedAsciiCol as IsCol<F, false>>::Inner>,
{
    fn normalize(&mut self) {
        *self = match mem::take(self) {
            Self::NonMixed(mut x) => {
                // this will simplify integer layouts as necessary
                x.normalize();
                Self::NonMixed(x)
            }
            Self::Mixed(x) => {
                if let Some((d0, ds)) = x.datatypes().split_first() {
                    if ds.iter().all(|d| d0 == d) {
                        let new = match d0 {
                            AlphaNumType::Ascii => {
                                let y = x.map_inner(ColInto::col_into).set_byte_layout(NoByteOrd);
                                AnyDatatype::Ascii(AnyAscii::Fixed(y))
                            }
                            AlphaNumType::Integer => {
                                let mut l = AnyEndianUint::Multi(x.map_inner(ColInto::col_into));
                                l.normalize();
                                AnyDatatype::Uint(l)
                            }
                            AlphaNumType::Float => AnyDatatype::F32(x.map_inner(ColInto::col_into)),
                            AlphaNumType::Double => {
                                AnyDatatype::F64(x.map_inner(ColInto::col_into))
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
                    Self::NonMixed(AnyDatatype::F32(ColumnGroup::default()))
                }
            }
        };
    }
}

// Implement range check for dataframe
//
// This might to be used after reading and before writing to ensure all data
// is within $PnR. This should be done by default for integers.
//
// This needs to be generic so it can work on all dataframes, including those
// within nested enums.

/// Check that all columns in dataframe are within range.
///
/// Return error or warning-like result depending on configuration parameter.
#[delegatable_trait]
pub trait CheckRanges {
    fn check_ranges(&mut self, trunc: TruncateEventValues) -> Vec<TruncatedResult>;
}

impl<C, I, L, M, const ORD: bool> CheckRanges
    for ColumnGroup<FFDataFrame<C>, FFDataFrameFamily, I, L, M, ORD>
where
    C: CheckRange,
{
    fn check_ranges(&mut self, trunc: TruncateEventValues) -> Vec<TruncatedResult> {
        self.container.check_ranges(trunc)
    }
}

// Implement internal dataframe -> primitive dataframe
//
// This can then be converted to a real Polars dataframe.
//
// Used for getting data out of a Core* type.

impl From<MixedDataFrame> for PrimitiveDataFrame {
    fn from(value: MixedDataFrame) -> Self {
        value.container.fmap(Into::into)
    }
}

impl<D> From<VariableUintDataFrame<D>> for PrimitiveDataFrame {
    fn from(value: VariableUintDataFrame<D>) -> Self {
        value.container.fmap(Into::into)
    }
}

impl<C, F, I, L, M, const ORD: bool>
    From<ColumnGroup<FFDataFrame<NativeColumn<C>>, F, I, L, M, ORD>> for PrimitiveDataFrame
where
    C: HasNativeType,
    NativeColumn<C>: Into<PrimitiveColumn<<C::Native as FCSRepr>::Prim>>,
    PrimitiveColumn<<C::Native as FCSRepr>::Prim>: Into<AnyPrimitiveColumn>,
    C::Native: FCSRepr,
{
    fn from(value: ColumnGroup<FFDataFrame<NativeColumn<C>>, F, I, L, M, ORD>) -> Self {
        value
            .container
            .fmap(|c| Into::<AnyPrimitiveColumn>::into(c.into()))
    }
}

// Implement column types and associated data
//
// Note that UvarCol and MixedCol only have ORD = false versions since these
// are only valid for 3.1+

/// A marker type to describe the contents of a layout.
///
/// This contains all information used to describe a column in either a headers
/// type or a dataframe type. This allows easily describing and mapping between
/// column data with just one marker type.
///
/// Parameters:
///
/// `F` - the type family (ie vector or dataframe) holding the data
/// `ORD` - whether the byte layout is ordered (2.0-3.0) or endian (3.1-3.2)
///
/// Any combination of these will uniquely describe any column.
pub trait IsCol<F, const ORD: bool> {
    /// The type contained with type represented by `F`.
    type Inner;
    /// The byte layout of this column.
    type Layout;
}

macro_rules! impl_numeric_column_type {
    ($c:ident, $inner:ident, $n:expr) => {
        impl IsCol<VecFamily, true> for $c {
            type Inner = $inner;
            type Layout = ArrayByteOrd<[u8; $n]>;
        }

        impl IsCol<FFDataFrameFamily, true> for $c {
            type Inner = NativeColumn<$inner>;
            type Layout = ArrayByteOrd<[u8; $n]>;
        }

        impl IsCol<VecFamily, false> for $c {
            type Inner = $inner;
            type Layout = Endian;
        }

        impl IsCol<FFDataFrameFamily, false> for $c {
            type Inner = NativeColumn<$inner>;
            type Layout = Endian;
        }
    };
}

impl_numeric_column_type!(U08Col, Bitmask08, 1);
impl_numeric_column_type!(U16Col, Bitmask16, 2);
impl_numeric_column_type!(U24Col, Bitmask24, 3);
impl_numeric_column_type!(U32Col, Bitmask32, 4);
impl_numeric_column_type!(U40Col, Bitmask40, 5);
impl_numeric_column_type!(U48Col, Bitmask48, 6);
impl_numeric_column_type!(U56Col, Bitmask56, 7);
impl_numeric_column_type!(U64Col, Bitmask64, 8);
impl_numeric_column_type!(F32Col, F32Range, 4);
impl_numeric_column_type!(F64Col, F64Range, 8);

macro_rules! impl_ascii_column_type {
    ($c:ident, $inner:ident) => {
        impl IsCol<VecFamily, true> for $c {
            type Inner = $inner;
            type Layout = NoByteOrd<true>;
        }

        impl IsCol<FFDataFrameFamily, true> for $c {
            type Inner = NativeColumn<$inner>;
            type Layout = NoByteOrd<true>;
        }

        impl IsCol<VecFamily, false> for $c {
            type Inner = $inner;
            type Layout = NoByteOrd<false>;
        }

        impl IsCol<FFDataFrameFamily, false> for $c {
            type Inner = NativeColumn<$inner>;
            type Layout = NoByteOrd<false>;
        }
    };
}

impl_ascii_column_type!(FixedAsciiCol, FixedAsciiRange);
impl_ascii_column_type!(DelimAsciiCol, DelimAsciiRange);

impl IsCol<VecFamily, false> for UvarCol {
    type Inner = AnyBitmask;
    type Layout = Endian;
}

impl IsCol<FFDataFrameFamily, false> for UvarCol {
    type Inner = AnyBitmaskColumn;
    type Layout = Endian;
}

impl IsCol<VecFamily, false> for MixedCol {
    type Inner = MixedRange;
    type Layout = Endian;
}

impl IsCol<FFDataFrameFamily, false> for MixedCol {
    type Inner = MixedColumn;
    type Layout = Endian;
}

// Implement header -> empty column
//
// This is easy for header types that map to exactly one rust type.
//
// Variable Uint and PolyType are exceptions since they themselves need to map
// to another wrapper type.

/// A header which can be converted to an empty column.
trait IntoEmptyColumn {
    type Target;

    fn empty(&self) -> Self::Target;
}

impl<T> IntoEmptyColumn for T
where
    T: HasNativeType + Clone,
    T::Native: FCSRepr,
{
    type Target = NativeColumn<T>;

    fn empty(&self) -> Self::Target {
        AnnotatedColumn::empty(self.clone())
    }
}

impl IntoEmptyColumn for AnyBitmask {
    type Target = AnyBitmaskColumn;

    fn empty(&self) -> Self::Target {
        match_map_uint!(self, x, AnnotatedColumn::empty(*x))
    }
}

impl IntoEmptyColumn for MixedRange {
    type Target = MixedColumn;

    fn empty(&self) -> Self::Target {
        match_map_datatype!(self, x, x.empty())
    }
}

// Implement byte width for column types which have a known width.
//
// This applies to all except ASCII types since it can only return up to 8 bytes

/// A column type which has a binary (ie not ASCII) representation.
#[delegatable_trait]
trait IsBinary: Sized {
    fn bytes(&self) -> PrivBytes;
}

impl<T> IsBinary for Bitmask<T>
where
    Self: HasNativeType<Native = T>,
    T: FCSRepr,
{
    fn bytes(&self) -> PrivBytes {
        T::FILE_BYTES.0
    }
}

impl<T> IsBinary for FloatRange<T>
where
    Self: HasNativeType<Native = T>,
    T: FCSRepr,
{
    fn bytes(&self) -> PrivBytes {
        T::FILE_BYTES.0
    }
}

impl<M: IsBinary, T, R> IsBinary for AnnotatedColumn<M, T, R> {
    fn bytes(&self) -> PrivBytes {
        self.header.bytes()
    }
}

impl<B: IsBinary, T> IsBinary for RangedVec<B, T> {
    fn bytes(&self) -> PrivBytes {
        self.range.bytes()
    }
}

// Implement fixed width for column types that have known width
//
// This applies to all except delim ASCII where $PnB is numeric and not '*'

/// A type which has a known width
#[delegatable_trait]
pub trait IsFixed {
    fn nbytes(&self) -> NonZeroU8;

    fn fixed_width(&self) -> BitsOrChars;
}

impl<T: IsBinary> IsFixed for T {
    fn nbytes(&self) -> NonZeroU8 {
        self.bytes().into()
    }

    fn fixed_width(&self) -> BitsOrChars {
        BitsOrChars(self.bytes().into())
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

impl IsFixed for NativeColumn<FixedAsciiRange> {
    fn nbytes(&self) -> NonZeroU8 {
        self.header.nbytes()
    }

    fn fixed_width(&self) -> BitsOrChars {
        self.header.fixed_width()
    }
}

// Implement native type for columns which map to exactly one Rust type
//
// Applies to all except compound types (ie mixed int width and mixed type)

/// A column which has exactly one native Rust type
pub trait HasNativeType: Sized {
    /// The native rust type
    type Native: Default + Copy;
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

impl<M: HasNativeType, T, R> HasNativeType for AnnotatedColumn<M, T, R> {
    type Native = M::Native;
}

// Implement datatype for column types which correspond 1-1 with $DATATYPE

/// A column which has exactly one $DATATYPE value always always
trait HasOneDatatype: Sized {
    const DATATYPE: AlphaNumType;
}

impl HasOneDatatype for FixedAsciiRange {
    const DATATYPE: AlphaNumType = AlphaNumType::Ascii;
}

impl HasOneDatatype for DelimAsciiRange {
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

impl<C08, C16, C24, C32, C40, C48, C56, C64> HasOneDatatype
    for AnyUint<C08, C16, C24, C32, C40, C48, C56, C64>
{
    const DATATYPE: AlphaNumType = AlphaNumType::Integer;
}

impl<M: HasOneDatatype, T, R> HasOneDatatype for AnnotatedColumn<M, T, R> {
    const DATATYPE: AlphaNumType = M::DATATYPE;
}

// Implement datatype for columns which might map to more than one datatype

/// A column which has a $DATATYPE keyword
trait HasDatatype: Sized {
    fn col_datatype(&self) -> AlphaNumType;

    fn datatype_from_columns(cs: &[Self]) -> AlphaNumType;
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

// Implement header type -> Range (ie $PnR)
//
// For now this is only used to get the range in terms of its native rust type
// and the $PnR (a BigDecimal) for documentation purposes when reading layouts
// and checking for out of range values.

/// A header type which can be converted to a $PnR range
trait IntoNativeRange: HasNativeType {
    fn as_range(&self) -> (Self::Native, Range);
}

impl<T> IntoNativeRange for Bitmask<T>
where
    Self: HasNativeType<Native = T>,
    T: Copy + Into<Range>,
{
    fn as_range(&self) -> (Self::Native, Range) {
        let b = self.bitmask();
        (b, b.into())
    }
}

impl<T> IntoNativeRange for FloatRange<T>
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

impl IntoNativeRange for FixedAsciiRange {
    fn as_range(&self) -> (Self::Native, Range) {
        let r = self.value();
        (r.0, r.0.into())
    }
}

impl IntoNativeRange for DelimAsciiRange {
    fn as_range(&self) -> (Self::Native, Range) {
        let r = self.0;
        (r.0, r.0.into())
    }
}

// Implement header type -> Range (ie $PnR) for types defined here.
//
// For bitmask and ascii types this is defined in separate modules.

impl From<&MixedRange> for Range {
    fn from(value: &MixedRange) -> Self {
        match_any_datatype!(value, x, x.into())
    }
}

impl From<&AnyBitmask> for Range {
    fn from(value: &AnyBitmask) -> Self {
        match_any_uint!(value, x, x.into())
    }
}

impl From<MixedRange> for Range {
    fn from(value: MixedRange) -> Self {
        match_any_datatype!(value, x, x.into())
    }
}

impl From<AnyBitmask> for Range {
    fn from(value: AnyBitmask) -> Self {
        match_any_uint!(value, x, x.into())
    }
}

impl<T: Clone> From<&FloatRange<T>> for Range {
    fn from(value: &FloatRange<T>) -> Self {
        value.clone().into()
    }
}

impl<T: Clone> From<FloatRange<T>> for Range {
    fn from(value: FloatRange<T>) -> Self {
        value.range.into()
    }
}

impl<C> From<&NativeColumn<C>> for Range
where
    C: HasNativeType + Clone + Into<Self>,
    C::Native: FCSRepr,
{
    fn from(value: &NativeColumn<C>) -> Self {
        value.header.clone().into()
    }
}

impl From<&AnyBitmaskColumn> for Range {
    fn from(value: &AnyBitmaskColumn) -> Self {
        match_any_uint!(value, x, x.into())
    }
}

impl From<&MixedColumn> for Range {
    fn from(value: &MixedColumn) -> Self {
        match_any_datatype!(value, x, x.into())
    }
}

// Implement Range (ie $PnR) -> header type
//
// This applies to all except mixed type headers since these need additional
// information to interpret the $PnR value as a given type.

/// A header type which can be converted from a $PnR range value.
pub trait FromRange: Sized {
    type Error;

    fn from_range(range: Range) -> Result<ConvertedRange<Self>, Self::Error> {
        Self::from_range_inner(range)
            .set_err_value(())
            .resolve_nowarn()
    }

    #[must_use]
    fn from_range_switch(
        range: Range,
        flag: DisallowRangeTrunc,
    ) -> DeferredSwitchableError<ConvertedRange<Self>, DisallowRangeTrunc, Self::Error> {
        Self::from_range_inner(range).nowarn_into_switchable3(flag)
    }

    fn from_range_inner(range: Range) -> DeferredError<ConvertedRange<Self>, Self::Error>;
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

// impl FromRange for AnyBitmask {
//     type Error = RangeToBitmaskError;

//     /// make a new bitmask from a float or integer.
//     ///
//     /// The size will be determined by the input and will be kept as small as
//     /// possible.
//     fn from_range_inner(range: Range) -> DeferredError<ConvertedRange<Self>, Self::Error> {
//         // NOTE this is a bit weird since we are letting the type control the
//         // size. There are a few edge cases where a user may wish to control the
//         // size but these are all for performance and supporting them would make
//         // the API much more complex.
//         range
//             .clone()
//             .into_uint()
//             .map_errors(RangeToBitmaskError::from)
//             .map_deferred_value(|x: BitmaskValue<u64>| Self::from(x))
//             .map_ok_value(|n| ConvertedRange::new(n, None))
//             .map_err_value(|n| ConvertedRange::new(n, Some(range)))
//     }
// }

// Implement Range (ie $PnR) and data -> column type
//
// This applies to all except mixed type headers since these need additional
// information to interpret the $PnR value as a given type.

// Implement header -> $PnB conversion
//
// This is simple for everything except delim ascii which returns '*' instead of
// a number.

/// Convert header type to $PnB value.
trait IntoWidth {
    fn as_width(&self) -> Width;
}

impl<T: IsFixed> IntoWidth for T {
    fn as_width(&self) -> Width {
        Width::Fixed(self.fixed_width())
    }
}

impl IntoWidth for DelimAsciiRange {
    fn as_width(&self) -> Width {
        Width::Variable
    }
}

// Implement header -> header conversions

/// Losslessly convert column type into another column type.
///
/// May panic. This is a convenience trait around TryInto to avoid putting trait
/// bounds in lots of annoying places.
trait ColInto<T> {
    fn col_into(self) -> T;
}

macro_rules! impl_col_into {
    ($from:path, $to:path) => {
        impl ColInto<$to> for $from {
            fn col_into(self) -> $to {
                self.try_into().unwrap()
            }
        }
    };
}

impl_col_into!(AnyBitmask, Bitmask08);
impl_col_into!(AnyBitmask, Bitmask16);
impl_col_into!(AnyBitmask, Bitmask24);
impl_col_into!(AnyBitmask, Bitmask32);
impl_col_into!(AnyBitmask, Bitmask40);
impl_col_into!(AnyBitmask, Bitmask48);
impl_col_into!(AnyBitmask, Bitmask56);
impl_col_into!(AnyBitmask, Bitmask64);

impl_col_into!(MixedRange, F32Range);
impl_col_into!(MixedRange, F64Range);
impl_col_into!(MixedRange, FixedAsciiRange);
impl_col_into!(MixedRange, AnyBitmask);

impl_col_into!(AnyBitmaskColumn, NativeColumn<Bitmask08>);
impl_col_into!(AnyBitmaskColumn, NativeColumn<Bitmask16>);
impl_col_into!(AnyBitmaskColumn, NativeColumn<Bitmask24>);
impl_col_into!(AnyBitmaskColumn, NativeColumn<Bitmask32>);
impl_col_into!(AnyBitmaskColumn, NativeColumn<Bitmask40>);
impl_col_into!(AnyBitmaskColumn, NativeColumn<Bitmask48>);
impl_col_into!(AnyBitmaskColumn, NativeColumn<Bitmask56>);
impl_col_into!(AnyBitmaskColumn, NativeColumn<Bitmask64>);

impl_col_into!(MixedColumn, NativeColumn<F32Range>);
impl_col_into!(MixedColumn, NativeColumn<F64Range>);
impl_col_into!(MixedColumn, NativeColumn<FixedAsciiRange>);
impl_col_into!(MixedColumn, AnyBitmaskColumn);

// Implement column -> primitive column

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

impl From<AnyBitmaskColumn> for AnyPrimitiveColumn {
    fn from(value: AnyBitmaskColumn) -> Self {
        match_any_uint!(value, x, PrimitiveColumn::from(x).into())
    }
}

impl<C> From<&NativeColumn<C>> for AnyPrimitiveColumn
where
    C: HasNativeType,
    C::Native: FCSRepr,
    NativeInternalColumn<C>: Clone + Into<PrimitiveColumn<<C::Native as FCSRepr>::Prim>>,
    PrimitiveColumn<<C::Native as FCSRepr>::Prim>: Into<Self>,
{
    fn from(value: &NativeColumn<C>) -> Self {
        let new: PrimitiveColumn<_> = value.data.clone().into();
        new.into()
    }
}

impl From<&MixedColumn> for AnyPrimitiveColumn {
    fn from(value: &MixedColumn) -> Self {
        value.clone().into()
    }
}

impl From<&AnyBitmaskColumn> for AnyPrimitiveColumn {
    fn from(value: &AnyBitmaskColumn) -> Self {
        value.clone().into()
    }
}

// Implement operations for $TOT marker type
//
// This is necessary since all versions except 2.0 require $TOT; in 2.0 this is
// optional. Therefore. these two different situations require their layouts to
// look up keywords differently (and fail accordingly) depending on this marker
// type.

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

impl IsTot for Option<Tot> {}
impl IsTot for Identity<Tot> {}

// Implement operations for $PnDATATYPE marker type
//
// This is controlled via a marker type in each layout. This is necessary since
// 3.2 layouts need to look for $PnDATATYPE keywords and the rest do not. This
// is encoded via the marker type, which is implemented here.

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

// Implement range check for columns
//
// This is the column-level trait for CheckRanges; see for details.

pub(crate) trait CheckRange {
    fn check_range(&mut self, i: MeasIndex, trunc: TruncateEventValues) -> TruncatedResult;
}

impl<C> CheckRange for NativeColumn<C>
where
    C: Clone + HasNativeType + HasDatatype + IntoNativeRange,
    <<C as HasNativeType>::Native as FCSRepr>::Prim: Copy + PartialOrd,
    C::Native: FCSRepr + Into<<<C as HasNativeType>::Native as FCSRepr>::Prim>,
{
    // TODO these errors could be cleaned up; we know that the highest range
    // that can be truncated is u64 or f64 so it isn't necessary to return a
    // rang object. Furthermore it shouldn't be necessary to pass the calling index.
    fn check_range(&mut self, i: MeasIndex, trunc: TruncateEventValues) -> TruncatedResult {
        let dt = self.header.col_datatype();
        let (u, rng) = self.header.as_range();
        let upper_limit = u.into();
        if dt.matches_truncation(trunc) {
            // If we wish to truncate this column, silently truncate without
            // throwing any errors
            let j = self.data.truncate(u);
            j.map_or(TruncatedResult::None, TruncatedResult::Truncated)
        } else {
            // Otherwise, scan through the values and return error on first
            // encounter with overrange value
            self.data
                .as_ref()
                .iter()
                .position(|x| *x > upper_limit)
                .map_or(TruncatedResult::None, |rowi| {
                    TruncatedResult::Overrange(i, rowi, rng)
                })
        }
    }
}

impl CheckRange for AnyBitmaskColumn {
    fn check_range(&mut self, i: MeasIndex, trunc: TruncateEventValues) -> TruncatedResult {
        match_any_uint!(self, x, x.check_range(i, trunc))
    }
}

impl CheckRange for MixedColumn {
    fn check_range(&mut self, i: MeasIndex, trunc: TruncateEventValues) -> TruncatedResult {
        match_any_datatype!(self, x, x.check_range(i, trunc))
    }
}

// Implement read dispatch for byte layouts.
//
// For simple cases where DATA is all the same type (or can be read as the same
// type and then cast to other types), each byte layout can be mapped to a
// specialized loop which reads all bytes as a matrix.

trait ByteLayoutIO<C: HasNativeType>
where
    C::Native: FCSRepr,
{
    fn read_matrix<R: Read>(
        &self,
        h: &mut BufReader<R>,
        buf: &mut ReadBuffer,
        cols: &mut Vec<Vec<C::Native>>,
    ) -> io::Result<()>;

    fn write_matrix<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        buf: &mut WriteBuffer,
        cols: &[&[<C as HasNativeType>::Native]],
    ) -> io::Result<()>;
}

macro_rules! impl_byte_layout_io {
    ($inner:path, $layout:path, $read_fun:ident, $write_fun:ident) => {
        impl ByteLayoutIO<$inner> for $layout {
            fn read_matrix<R: Read>(
                &self,
                h: &mut BufReader<R>,
                buf: &mut ReadBuffer,
                cols: &mut Vec<Vec<<$inner as HasNativeType>::Native>>,
            ) -> io::Result<()> {
                buf.$read_fun(h, cols, *self)
            }

            fn write_matrix<W: Write>(
                &self,
                h: &mut BufWriter<W>,
                buf: &mut WriteBuffer,
                cols: &[&[<$inner as HasNativeType>::Native]],
            ) -> io::Result<()> {
                buf.$write_fun(h, cols, *self)
            }
        }
    };
}

macro_rules! impl_ordered_layout_io {
    ($t:ident) => {
        impl_byte_layout_io!(
            $t,
            ArrayByteOrd<<<$t as HasNativeType>::Native as FCSRepr>::ByteOrd>,
            read_ordered_matrix,
            write_ordered_matrix
        );
    };
}

macro_rules! impl_endian_layout_io {
    ($t:ident) => {
        impl_byte_layout_io!($t, Endian, read_endian_matrix, write_endian_matrix);
    };
}

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

// Implement checks for scale transform against $DATATYPE
//
// Log scale transforms can only be used when $DATATYPE=I

/// A scale transform which may be checked against a datatype to ensure compatibility
pub trait CheckedScaleTransform {
    type Err;
    type Summary;

    fn matches_datatype(&self, datatype: AlphaNumType, i: MeasIndex) -> Result<(), Self::Err>;
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

// Implement default for layout types
//
// Note, this is different from the IntoEmptyDataFrame trait since this will
// make a totally empty layout (no columns) whereas IntoEmptyDataFrame will
// convert a headers layout to a dataframe layout with the same number of
// columns which often won't be zero.

impl<C: Default, I, F, L: Default, M, const ORD: bool> Default for ColumnGroup<C, F, I, L, M, ORD> {
    fn default() -> Self {
        Self::new(C::default(), L::default())
    }
}

impl<M, N: Default> Default for Any3_2<M, N> {
    fn default() -> Self {
        Self::NonMixed(N::default())
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

impl<A, I: Default, F32, F64> Default for AnyDatatype<A, I, F32, F64> {
    fn default() -> Self {
        Self::Uint(I::default())
    }
}

impl<D, F: Default> Default for AnyAscii<D, F> {
    fn default() -> Self {
        Self::Fixed(F::default())
    }
}

// Implement generalized nested layout conversions
//
// Note this is achieved via column marker types (U08Col, F32Col, etc) since
// these will be unique to each variant in a given enum and thus prevent
// conflicts.

impl_generic_enum_from! {
    AnyEndianUint<W0, W>,
    Single(W0)<C08, C16, C24, C32, C40, C48, C56, C64>
        ~ AnyUint<C08, C16, C24, C32, C40, C48, C56, C64>,
    Multi(W)<C, F, L, M, const ORD: bool>
        ~ ColumnGroup<C, F, UvarCol, L, M, ORD>
}

impl_generic_enum_from! {
    AnyUint<C08, C16, C24, C32, C40, C48, C56, C64>
        <C, F, L, M, const ORD: bool>,
    Uint08(C08) ~ ColumnGroup<C, F, U08Col, L, M, ORD>,
    Uint16(C16) ~ ColumnGroup<C, F, U16Col, L, M, ORD>,
    Uint24(C24) ~ ColumnGroup<C, F, U24Col, L, M, ORD>,
    Uint32(C32) ~ ColumnGroup<C, F, U32Col, L, M, ORD>,
    Uint40(C40) ~ ColumnGroup<C, F, U40Col, L, M, ORD>,
    Uint48(C48) ~ ColumnGroup<C, F, U48Col, L, M, ORD>,
    Uint56(C56) ~ ColumnGroup<C, F, U56Col, L, M, ORD>,
    Uint64(C64) ~ ColumnGroup<C, F, U64Col, L, M, ORD>
}

impl_generic_enum_from! {
    AnyDatatype<A, I, F32, F64>,
    Ascii(A)<Ad, Aa> ~ AnyAscii<Ad, Aa>,
    // impl From<AnyUint<...>> below since this can accept two layout types
    Uint(I)<W0, W> ~ AnyEndianUint<W0, W>,
    F32(F32)<C, F, L, M, const ORD: bool> ~ ColumnGroup<C, F, F32Col, L, M, ORD>,
    F64(F64)<C, F, L, M, const ORD: bool> ~ ColumnGroup<C, F, F64Col, L, M, ORD>
}

impl<F, const ORD: bool, M, A, F32, F64> From<AnyUintGroup<F, ORD, M>>
    for AnyDatatype<A, AnyUintGroup<F, ORD, M>, F32, F64>
where
    F: Kind1,
    U08Col: IsCol<F, ORD>,
    U16Col: IsCol<F, ORD>,
    U24Col: IsCol<F, ORD>,
    U32Col: IsCol<F, ORD>,
    U40Col: IsCol<F, ORD>,
    U48Col: IsCol<F, ORD>,
    U56Col: IsCol<F, ORD>,
    U64Col: IsCol<F, ORD>,
{
    fn from(value: AnyUintGroup<F, ORD, M>) -> Self {
        Self::Uint(value)
    }
}

impl_generic_enum_from! {
    AnyAscii<Delim, Fixed>,
    Delimited(Delim)<C, F, L, M, const ORD: bool> ~ ColumnGroup<C, F, DelimAsciiCol, L, M, ORD>,
    Fixed(Fixed)<C, F, L, M, const ORD: bool> ~ ColumnGroup<C, F, FixedAsciiCol, L, M, ORD>
}

impl<C, F, L, M, N, const ORD: bool> From<ColumnGroup<C, F, MixedCol, L, M, ORD>>
    for Any3_2<ColumnGroup<C, F, MixedCol, L, M, ORD>, N>
{
    fn from(value: ColumnGroup<C, F, MixedCol, L, M, ORD>) -> Self {
        Self::Mixed(value)
    }
}

impl<A, I, F32, F64, M> From<AnyDatatype<A, I, F32, F64>>
    for Any3_2<M, AnyDatatype<A, I, F32, F64>>
{
    fn from(value: AnyDatatype<A, I, F32, F64>) -> Self {
        Self::NonMixed(value)
    }
}

// Implement native range -> compound range conversions
//
// I.e. 8-bit bitmask -> Mixed type range

impl_generic_enum_from! {
    AnyBitmask,
    Uint08 ~ Bitmask08,
    Uint16 ~ Bitmask16,
    Uint24 ~ Bitmask24,
    Uint32 ~ Bitmask32,
    Uint40 ~ Bitmask40,
    Uint48 ~ Bitmask48,
    Uint56 ~ Bitmask56,
    Uint64 ~ Bitmask64
}

impl_generic_enum_from! {
    MixedRange,
    Ascii ~ FixedAsciiRange,
    Uint ~ AnyBitmask,
    F32 ~ F32Range,
    F64 ~ F64Range
}

// necessary for inserting $PnR into mixed layuot
impl From<DelimAsciiRange> for MixedRange {
    fn from(value: DelimAsciiRange) -> Self {
        // this will automatically make any delimited ASCII layout a fixed
        // layout if we go to mixed, which seems sane if not an exceedingly rare
        // use case.
        Self::Ascii(value.into())
    }
}

// necessary for inserting $PnR into mixed layuot
impl<T> From<Bitmask<T>> for MixedRange
where
    AnyBitmask: From<Bitmask<T>>,
{
    fn from(value: Bitmask<T>) -> Self {
        Self::Uint(value.into())
    }
}

// Implement nested ranges -> inner ranges (fallible)
//
// These are useful when converting layouts

macro_rules! impl_uint_try_from_var_uint {
    ($outer:path, $inner:path, $var:ident) => {
        impl TryFrom<$outer> for $inner {
            type Error = UintToUintError;
            fn try_from(value: $outer) -> Result<Self, Self::Error> {
                if let AnyUint::$var(x) = value {
                    Ok(x)
                } else {
                    let b = <<$inner as HasNativeType>::Native as FCSRepr>::FILE_BYTES;
                    Err(UintToUintError::new(value.bytes().into(), b.into()))
                }
            }
        }
    };
}

macro_rules! impl_nonmixed_try_from_mixed {
    ($outer:path, $inner:path, $var:ident) => {
        impl TryFrom<$outer> for $inner {
            type Error = MixedToNonMixedError;
            fn try_from(value: $outer) -> Result<Self, Self::Error> {
                if let AnyDatatype::$var(x) = value {
                    Ok(x)
                } else {
                    let src_type = value.col_datatype();
                    let dst_type = <$inner as HasOneDatatype>::DATATYPE;
                    Err(MixedToNonMixedError::new(src_type, dst_type))
                }
            }
        }
    };
}

macro_rules! impl_uint_try_from_mixed {
    ($var:ident, $bitmask:path) => {
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
                    let src_type = value.col_datatype();
                    Err(MixedToNonMixedError::new(src_type, AlphaNumType::Integer).into())
                }
            }
        }
    };
}

impl_uint_try_from_var_uint!(AnyBitmask, Bitmask08, Uint08);
impl_uint_try_from_var_uint!(AnyBitmask, Bitmask16, Uint16);
impl_uint_try_from_var_uint!(AnyBitmask, Bitmask24, Uint24);
impl_uint_try_from_var_uint!(AnyBitmask, Bitmask32, Uint32);
impl_uint_try_from_var_uint!(AnyBitmask, Bitmask40, Uint40);
impl_uint_try_from_var_uint!(AnyBitmask, Bitmask48, Uint48);
impl_uint_try_from_var_uint!(AnyBitmask, Bitmask56, Uint56);
impl_uint_try_from_var_uint!(AnyBitmask, Bitmask64, Uint64);

impl_uint_try_from_var_uint!(AnyBitmaskColumn, NativeColumn<Bitmask08>, Uint08);
impl_uint_try_from_var_uint!(AnyBitmaskColumn, NativeColumn<Bitmask16>, Uint16);
impl_uint_try_from_var_uint!(AnyBitmaskColumn, NativeColumn<Bitmask24>, Uint24);
impl_uint_try_from_var_uint!(AnyBitmaskColumn, NativeColumn<Bitmask32>, Uint32);
impl_uint_try_from_var_uint!(AnyBitmaskColumn, NativeColumn<Bitmask40>, Uint40);
impl_uint_try_from_var_uint!(AnyBitmaskColumn, NativeColumn<Bitmask48>, Uint48);
impl_uint_try_from_var_uint!(AnyBitmaskColumn, NativeColumn<Bitmask56>, Uint56);
impl_uint_try_from_var_uint!(AnyBitmaskColumn, NativeColumn<Bitmask64>, Uint64);

impl_nonmixed_try_from_mixed!(MixedRange, FixedAsciiRange, Ascii);
impl_nonmixed_try_from_mixed!(MixedRange, AnyBitmask, Uint);
impl_nonmixed_try_from_mixed!(MixedRange, F32Range, F32);
impl_nonmixed_try_from_mixed!(MixedRange, F64Range, F64);

impl_nonmixed_try_from_mixed!(MixedColumn, NativeColumn<FixedAsciiRange>, Ascii);
impl_nonmixed_try_from_mixed!(MixedColumn, AnyBitmaskColumn, Uint);
impl_nonmixed_try_from_mixed!(MixedColumn, NativeColumn<F32Range>, F32);
impl_nonmixed_try_from_mixed!(MixedColumn, NativeColumn<F64Range>, F64);

impl_uint_try_from_mixed!(Uint08, Bitmask08);
impl_uint_try_from_mixed!(Uint16, Bitmask16);
impl_uint_try_from_mixed!(Uint24, Bitmask24);
impl_uint_try_from_mixed!(Uint32, Bitmask32);
impl_uint_try_from_mixed!(Uint40, Bitmask40);
impl_uint_try_from_mixed!(Uint48, Bitmask48);
impl_uint_try_from_mixed!(Uint56, Bitmask56);
impl_uint_try_from_mixed!(Uint64, Bitmask64);

// Implement reference to container for ColumnGroup
//
// This is a slightly nicer way to "reference the thing inside a functor"
// without actually using the functor trait which is sometimes annoying.

impl<C, F, I, L, M, const ORD: bool> AsRef<[C]> for ColumnGroup<Vec<C>, F, I, L, M, ORD> {
    fn as_ref(&self) -> &[C] {
        self.container.as_ref()
    }
}

impl<C, F, I, L, M, const ORD: bool> AsRef<[C]> for ColumnGroup<FFDataFrame<C>, F, I, L, M, ORD> {
    fn as_ref(&self) -> &[C] {
        self.container.as_ref()
    }
}

// Implement methods on ColumnGroup

impl<C, F, I, M, const ORD: bool> ColumnGroup<C, F, I, NoByteOrd<ORD>, M, ORD> {
    pub fn new_ascii(columns: C) -> Self {
        Self::new(columns, NoByteOrd::<ORD>)
    }
}

impl<T, I, A, M, const ORD: bool>
    ColumnGroup<Vec<Bitmask<T>>, VecFamily, I, ArrayByteOrd<A>, M, ORD>
where
    T: FCSRepr,
    Bitmask<T>: HasNativeType<Native = T>,
{
    #[must_use]
    pub fn new_endian_uint(ranges: Vec<Bitmask<T>>, endian: Endian) -> Self {
        Self::new(ranges, ArrayByteOrd::Endian(endian))
    }
}

impl<T, I, A, M, const ORD: bool>
    ColumnGroup<Vec<FloatRange<T>>, VecFamily, I, ArrayByteOrd<A>, M, ORD>
where
    T: FCSRepr,
    FloatRange<T>: HasNativeType<Native = T>,
{
    #[must_use]
    pub fn new_endian_float(ranges: Vec<FloatRange<T>>, endian: Endian) -> Self {
        Self::new(ranges, ArrayByteOrd::Endian(endian))
    }
}

impl<C, I, L, T, D, const ORD: bool>
    ColumnGroup<Vec<C>, VecFamily, I, L, ColumnMarkers<T, D>, ORD>
{
    fn try_new<F, P, W, E>(
        cs: Vec<ColumnLayoutValues<D>>,
        byte_layout: L,
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
}

impl<C, F, I, L, M, const ORD: bool> ColumnGroup<C, F, I, L, M, ORD> {
    pub fn byte_layout_into<Lf, const ORD_F: bool>(self) -> ColumnGroup<C, F, I, Lf, M, ORD_F>
    where
        L: Into<Lf>,
    {
        ColumnGroup::new(self.container, self.byte_layout.into())
    }

    pub fn new_empty(byte_layout: L) -> Self
    where
        C: Default,
    {
        Self::new(C::default(), byte_layout)
    }

    pub fn columns<X>(&self) -> &[X]
    where
        C: AsRef<[X]>,
    {
        self.container.as_ref()
    }

    pub fn widths<X>(&self) -> Vec<BitsOrChars>
    where
        C: AsRef<[X]>,
        X: IsFixed,
    {
        self.columns().iter().map(IsFixed::fixed_width).collect()
    }

    /// Produce conversion error if columns are not all the same width.
    ///
    /// Useful when converting a mixed-width int layout into a single layout.
    fn conversion_fail_by_width<X, R>(&self) -> LayoutConvertResult<R>
    where
        C: AsRef<[X]>,
        X: IsFixed,
    {
        debug_assert!(!self.columns().is_empty(), "columns must be non-empty");
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

    /// Produce conversion error if columns are not all the same type.
    ///
    /// Useful when converting a mixed-type layout into a single layout.
    fn conversion_fail_by_datatype<X>(&self) -> LayoutConvertResult<X>
    where
        Self: LayoutDatatype,
    {
        let ds = self.datatypes();
        debug_assert!(!ds.is_empty(), "columns must be non-empty");
        let d0 = self.datatype();
        let es = ds
            .into_iter()
            .filter(|&d| d0 != d)
            .map(|d| MixedToNonMixedError::new(d, d0))
            .enumerate()
            .map(|(i, e)| IndexedError::new(i, e))
            .map(MixedToNonMixedLayoutError::from)
            .map(LayoutConvertError::from)
            .try_into_nonempty_iter()
            .expect("mixed layout should have at least one different type");
        LogResult::new_from_ne_err_iter(es, ())
    }

    fn event_width<X>(&self) -> usize
    where
        C: AsRef<[X]>,
        X: IsFixed,
    {
        self.container
            .as_ref()
            .iter()
            .map(|c| usize::from(u8::from(c.nbytes())))
            .sum()
    }

    #[allow(clippy::trivially_copy_pass_by_ref)]
    fn compute_nrows<X>(
        &self,
        seg: &mut AnyDataSegment,
        conf: &ReadEventsConfig,
    ) -> WarningOrErrorResult<ComputedRowsResult, (), UnevenEventWidthError, EventWidthError>
    where
        C: AsRef<[X]>,
        X: IsFixed,
        L: Clone,
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

    fn map_inner<Fun, If>(self, f: Fun) -> ColumnGroup<Sibling1<C, If::Inner>, F, If, L, M, ORD>
    where
        I: IsCol<F, ORD>,
        If: IsCol<F, ORD>,
        Fun: FnMut(I::Inner) -> If::Inner,
        C: Functor<I::Inner>,
    {
        ColumnGroup::new(self.container.fmap(f), self.byte_layout)
    }

    fn set_byte_layout<Lf, const ORD_F: bool>(
        self,
        byte_layout: Lf,
    ) -> ColumnGroup<C, F, I, Lf, M, ORD_F> {
        ColumnGroup::new(self.container, byte_layout)
    }

    fn byte_layout_try_into<Lf, const ORD_F: bool>(
        self,
    ) -> Result<ColumnGroup<C, F, I, Lf, M, ORD_F>, L::Error>
    where
        L: TryInto<Lf>,
    {
        let b = self.byte_layout.try_into()?;
        Ok(ColumnGroup::new(self.container, b))
    }
}

// Implement methods on specific aliases of column group

impl<T> AnyOrderedUintHeaders<T> {
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
                ColumnGroup::try_new(cs, o, |i, c| {
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

impl<T, D, const ORD: bool> AnyAsciiHeaders<ORD, ColumnMarkers<T, D>>
where
    FixedAsciiCol: IsCol<VecFamily, ORD, Inner = FixedAsciiRange, Layout = NoByteOrd<ORD>>,
    DelimAsciiCol: IsCol<VecFamily, ORD, Inner = DelimAsciiRange, Layout = NoByteOrd<ORD>>,
{
    fn try_new(
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
                    let l = ColumnGroup::new_ascii(ranges);
                    NewLayout::new(Self::Delimited(l), non_truncated)
                })
                .map_err_value(|_| ())
        } else {
            ColumnGroup::try_new(cs, NoByteOrd, |i, c| {
                FixedAsciiRange::from_width_and_range(c.width, c.range, i, flag)
            })
            .map_ok_value(FunctorOnce::fmap_into_once)
        }
    }

    fn new_fixed(columns: impl IntoIterator<Item = FixedAsciiRange>) -> Self {
        Self::Fixed(ColumnGroup::new_ascii(columns.into_iter().collect()))
    }

    fn new_delim(ranges: Vec<DelimAsciiRange>) -> Self {
        Self::Delimited(ColumnGroup::new_ascii(ranges))
    }
}

impl<const ORD: bool, M> FixedAsciiHeaders<ORD, M>
where
    FixedAsciiCol: IsCol<VecFamily, ORD, Inner = FixedAsciiRange, Layout = NoByteOrd<ORD>>,
{
    #[must_use]
    pub fn new_ascii_u64(ranges: Vec<AsciiRangeValue>) -> Self {
        Self::new_ascii(ranges.fmap_into())
    }
}

impl DataHeaders3_2 {
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
        let mut ret: Self = if let Ok(xs) = go!(FixedAsciiRange) {
            NonMixedEndianHeaders::new_ascii_fixed(xs).into()
        } else if let Ok(xs) = go!(AnyBitmask) {
            NonMixedEndianHeaders::new_uint(xs, endian).into()
        } else if let Ok(xs) = go!(F32Range) {
            NonMixedEndianHeaders::new_f32(xs, endian).into()
        } else if let Ok(xs) = go!(F64Range) {
            NonMixedEndianHeaders::new_f64(xs, endian).into()
        } else {
            ColumnGroup::new(rs, endian).into()
        };
        ret.normalize();
        ret
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

impl<T> AnyOrderedHeaders<T> {
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
        AnyAsciiHeaders::new_fixed(ranges).into()
    }

    #[must_use]
    pub fn new_ascii_delim(ranges: Vec<DelimAsciiRange>) -> Self {
        AnyAsciiHeaders::new_delim(ranges).into()
    }

    #[must_use]
    pub fn new_uint<I>(columns: Vec<I::Inner>, byte_layout: I::Layout) -> Self
    where
        I: IsCol<VecFamily, true>,
        I::Inner: HasNativeType,
        <I::Inner as HasNativeType>::Native: FCSRepr,
        AnyOrderedUintHeaders<T>: From<ColumnHeaders<I, true, ColumnMarkers<T, Nothing<NumType>>>>,
    {
        Self::Uint(ColumnGroup::new(columns, byte_layout).into())
    }

    #[must_use]
    pub fn new_f32(ranges: Vec<F32Range>, byte_layout: ArrayByteOrd<[u8; 4]>) -> Self {
        ColumnGroup::new(ranges, byte_layout).into()
    }

    #[must_use]
    pub fn new_f64(ranges: Vec<F64Range>, byte_layout: ArrayByteOrd<[u8; 8]>) -> Self {
        ColumnGroup::new(ranges, byte_layout).into()
    }

    fn new_empty(datatype: AlphaNumType) -> Self {
        match datatype {
            AlphaNumType::Ascii => AnyAsciiHeaders::default().into(),
            AlphaNumType::Integer => AnyOrderedUintHeaders::default().into(),
            AlphaNumType::Float => Self::F32(ColumnGroup::default()),
            AlphaNumType::Double => Self::F64(ColumnGroup::default()),
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
                        from! {ColumnGroup::try_new(columns, b, |i, c| {
                            $t::from_width_and_range(c.width, c.range, i, $notrunc)
                                .repack_errors()
                        })}
                    })
            };
        }

        let notrunc = conf.disallow_range_truncation;

        match datatype {
            AlphaNumType::Ascii => from!(AnyAsciiHeaders::try_new(columns, notrunc)),
            AlphaNumType::Integer => from!(AnyOrderedUintHeaders::try_new(columns, byteord, conf)),
            AlphaNumType::Float => go_float!(F32Range, notrunc),
            AlphaNumType::Double => go_float!(F64Range, notrunc),
        }
    }
}

impl NonMixedEndianHeaders<Nothing<NumType>> {
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
            AlphaNumType::Ascii => from!(AnyAsciiHeaders::try_new(columns, notrunc)),
            AlphaNumType::Integer => {
                from!(AnyEndianUintHeaders::try_new(columns, endian, notrunc))
            }
            AlphaNumType::Float => from!(ColumnGroup::try_new(columns, endian, go_f32)),
            AlphaNumType::Double => from!(ColumnGroup::try_new(columns, endian, go_f64)),
        }
    }
}

impl<D> NonMixedEndianHeaders<D> {
    fn new_empty(datatype: AlphaNumType) -> Self {
        Self::new_empty1(datatype, Endian::default())
    }

    fn new_empty1(datatype: AlphaNumType, endian: Endian) -> Self {
        match datatype {
            AlphaNumType::Ascii => AnyAsciiHeaders::default().into(),
            AlphaNumType::Integer => Self::Uint(AnyEndianUint::Single(AnyUint::Uint32(
                ColumnGroup::new_empty(endian),
            ))),
            AlphaNumType::Float => Self::F32(ColumnGroup::new_empty(endian)),
            AlphaNumType::Double => Self::F64(ColumnGroup::new_empty(endian)),
        }
    }

    #[must_use]
    pub fn new_ascii_fixed(ranges: impl IntoIterator<Item = FixedAsciiRange>) -> Self {
        AnyAsciiHeaders::new_fixed(ranges).into()
    }

    #[must_use]
    pub fn new_ascii_delim(ranges: Vec<DelimAsciiRange>) -> Self {
        AnyAsciiHeaders::new_delim(ranges).into()
    }

    // TODO make fixed width versions of this?

    #[must_use]
    pub fn new_uint(columns: Vec<AnyBitmask>, endian: Endian) -> Self {
        AnyEndianUint::Multi(ColumnGroup::new(columns, endian)).into()
    }

    #[must_use]
    pub fn new_f32(ranges: Vec<F32Range>, endian: Endian) -> Self {
        ColumnGroup::new(ranges, endian).into()
    }

    #[must_use]
    pub fn new_f64(ranges: Vec<F64Range>, endian: Endian) -> Self {
        ColumnGroup::new(ranges, endian).into()
    }
}

impl<Cd, Ca, Id, Ia, F, Ld, La, M, const ORD: bool>
    AnyAscii<ColumnGroup<Cd, F, Id, Ld, M, ORD>, ColumnGroup<Ca, F, Ia, La, M, ORD>>
{
    #[allow(clippy::type_complexity)]
    pub fn byte_layout_into<Ldf, Laf, const ORD_F: bool>(
        self,
    ) -> AnyAscii<ColumnGroup<Cd, F, Id, Ldf, M, ORD_F>, ColumnGroup<Ca, F, Ia, Laf, M, ORD_F>>
    where
        Ld: Into<Ldf>,
        La: Into<Laf>,
    {
        match_map_ascii!(self, x, x.byte_layout_into())
    }
}

impl<D> AnyEndianUintHeaders<D> {
    fn try_new(
        cs: Vec<ColumnLayoutValues<D>>,
        e: Endian,
        flag: DisallowRangeTrunc,
    ) -> WarningsAndErrorsResult<NewLayout<Self>, (), IndexedBitmaskError, NewUintTypeError>
    where
        D: IsNumType,
    {
        ColumnGroup::try_new(cs, e, |i, c| {
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
}

// Implement conversions for ranged vectors
//
// These are used for low level IO operations when making dataframes.

impl From<AnyUintVec> for AnyBitmaskColumn {
    fn from(value: AnyUintVec) -> Self {
        match_map_uint!(value, x, NativeColumn::from(x))
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

macro_rules! decl_mixed_read {
    ($name:ident, $int_fun:ident, $float_fun:ident) => {
        fn $name(
            &mut self,
            dst_index: DstIndex,
            src: &[u8],
            src_index: SrcIndex,
        ) -> Result<(), AsciiToUintError> {
            match self {
                Self::Ascii(xs) => {
                    let src_width = usize::from(u8::from(xs.range.chars()));
                    xs.data[dst_index.0] =
                        ascii_to_uint(&src[src_index.0..src_index.0 + src_width])?;
                    return Ok(());
                }
                Self::Uint(xs) => xs.$int_fun(dst_index, src, src_index),
                Self::F32(xs) => xs.data[dst_index.0] = f32::$float_fun(src, src_index),
                Self::F64(xs) => xs.data[dst_index.0] = f64::$float_fun(src, src_index),
            }
            Ok(())
        }
    };
}

impl MixedVec {
    decl_mixed_read!(read_le, read_le, from_be_slice);
    decl_mixed_read!(read_be, read_be, from_le_slice);
}

macro_rules! decl_mixed_write {
    ($name:ident, $int_fun:ident, $float_fun:ident) => {
        fn $name(&self, src_index: SrcIndex, dst: &mut [u8], dst_index: DstIndex) {
            match self {
                Self::Ascii(xs) => {
                    let v = xs.as_ref()[src_index.0];
                    xs.header.to_slice_unchecked(v, dst, dst_index);
                }
                Self::Uint(xs) => xs.$int_fun(src_index, dst, dst_index),
                Self::F32(xs) => xs.as_ref()[src_index.0].$float_fun(dst, dst_index),
                Self::F64(xs) => xs.as_ref()[src_index.0].$float_fun(dst, dst_index),
            }
        }
    };
}

impl MixedColumn {
    decl_mixed_write!(write_le, write_le, to_be_slice);
    decl_mixed_write!(write_be, write_be, to_le_slice);
}

macro_rules! decl_uint_read {
    ($name:ident, $fun:ident) => {
        fn $name(&mut self, dst_index: DstIndex, src: &[u8], src_index: SrcIndex) {
            match self {
                Self::Uint08(xs) => {
                    xs.data[dst_index.0] = u8::$fun(src, src_index);
                }
                Self::Uint16(xs) => {
                    xs.data[dst_index.0] = u16::$fun(src, src_index);
                }
                Self::Uint24(xs) => {
                    xs.data[dst_index.0] = U24::$fun(src, src_index);
                }
                Self::Uint32(xs) => {
                    xs.data[dst_index.0] = u32::$fun(src, src_index);
                }
                Self::Uint40(xs) => {
                    xs.data[dst_index.0] = U40::$fun(src, src_index);
                }
                Self::Uint48(xs) => {
                    xs.data[dst_index.0] = U48::$fun(src, src_index);
                }
                Self::Uint56(xs) => {
                    xs.data[dst_index.0] = U56::$fun(src, src_index);
                }
                Self::Uint64(xs) => {
                    xs.data[dst_index.0] = u64::$fun(src, src_index);
                }
            }
        }
    };
}

impl AnyUintVec {
    decl_uint_read!(read_le, from_le_slice);
    decl_uint_read!(read_be, from_be_slice);
}

macro_rules! decl_uint_write {
    ($name:ident, $fun:ident) => {
        fn $name(&self, src_index: SrcIndex, dst: &mut [u8], dst_index: DstIndex) {
            match self {
                Self::Uint08(xs) => xs.as_ref()[src_index.0].$fun(dst, dst_index),
                Self::Uint16(xs) => xs.as_ref()[src_index.0].$fun(dst, dst_index),
                Self::Uint24(xs) => {
                    let ys: &[U24] = xs.as_ref();
                    ys[src_index.0].$fun(dst, dst_index);
                }
                Self::Uint32(xs) => xs.as_ref()[src_index.0].$fun(dst, dst_index),
                Self::Uint40(xs) => {
                    let ys: &[U40] = xs.as_ref();
                    ys[src_index.0].$fun(dst, dst_index);
                }
                Self::Uint48(xs) => {
                    let ys: &[U48] = xs.as_ref();
                    ys[src_index.0].$fun(dst, dst_index);
                }
                Self::Uint56(xs) => {
                    let ys: &[U56] = xs.as_ref();
                    ys[src_index.0].$fun(dst, dst_index);
                }
                Self::Uint64(xs) => xs.as_ref()[src_index.0].$fun(dst, dst_index),
            }
        }
    };
}

impl AnyBitmaskColumn {
    decl_uint_write!(write_le, to_le_slice);
    decl_uint_write!(write_be, to_be_slice);
}

// Implement misc methods for header ranges

impl<T> FloatRange<T> {
    /// Make new float range from $PnB and $PnR values.
    ///
    /// Will return an error if $PnB is the incorrect size.
    fn from_width_and_range(
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
    fn from_width_and_range(
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
}

impl From<BitmaskValue<u64>> for AnyBitmask {
    /// Make a new bitmask from a u64.
    ///
    /// The width is determined by the magnitude of the range; the smallest
    /// possible will be used.
    fn from(value: BitmaskValue<u64>) -> Self {
        macro_rules! go {
            ($var:ident, $x:expr) => {{
                let (ret, truncated) = Bitmask::from_u64($x.0);
                debug_assert!(!truncated, "AnyBitmask input should never be truncated");
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

impl From<AnyBitmask> for BitmaskValue<u64> {
    /// Convert bitmask range (not bitmask itself) to u64.
    fn from(value: AnyBitmask) -> Self {
        match_any_uint!(value, x, Self(u64::from(x)))
    }
}

impl AnyBitmask {
    fn init_column(&self, nrows: usize) -> AnyUintVec {
        fn default_vec<T: Clone + Default>(n: usize) -> Vec<T> {
            vec![T::default(); n]
        }
        match_map_uint!(self, x, RangedVec::new(*x, default_vec(nrows)))
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
}

// Implement methods for RowBuffer
//
// This includes most of code used for reading and writing bytes from DATA

impl<const IS_READ: bool> RowBuffer<IS_READ> {
    fn init(max_size: RowBufferSize, nrows: usize, row_width: usize) -> Self {
        // Max this to 1 here so that we always have at least one row we are
        // reading. If there are any machines that produce files with at least
        // 32KB rows (which would be ~1000 parameters at 32 bit column widths),
        // these will produce some lovely cache miss fireworks on most CPUs :/
        let rows_per_buffer = (max_size.0 / row_width).max(1);
        let buf_size = rows_per_buffer * row_width;
        let mut new = Self {
            nrows,
            rows_per_buffer,
            buf_size: usize_to_u64(buf_size),
            row_width,
            bytes: Vec::with_capacity(buf_size),
        };
        if !IS_READ {
            // When writing, we need to fill the buffer with 0's up to capacity
            // and then copy data to it.
            new.bytes.fill(0);
        }
        new
    }

    fn whole_row_number(&self) -> usize {
        self.nrows / self.rows_per_buffer
    }

    fn remainder_row_number(&self) -> usize {
        self.nrows % self.rows_per_buffer
    }

    fn remainder_bytes(&self) -> usize {
        let remainder_rows = self.remainder_row_number();
        remainder_rows * self.row_width
    }
}

impl ReadBuffer {
    fn read_size<R: Read>(&mut self, h: &mut BufReader<R>, size: u64) -> io::Result<()> {
        self.bytes.clear();
        h.take(size).read_to_end(&mut self.bytes)?;
        Ok(())
    }

    fn read<R: Read>(&mut self, h: &mut BufReader<R>) -> io::Result<()> {
        self.read_size(h, self.buf_size)
    }

    fn read_remainder<R: Read>(&mut self, h: &mut BufReader<R>) -> io::Result<()> {
        let n = usize_to_u64(self.remainder_bytes());
        self.read_size(h, n)
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
        Fr: FnMut(&mut C, DstIndex, &[u8], SrcIndex) -> Result<(), E>,
        Fw: Fn(usize) -> usize,
    {
        // Read groups of rows in outer loop
        let mut src_col_offset;
        let mut dst_row_offset = 0;
        for _ in 0..self.whole_row_number() {
            self.read(h)?;
            src_col_offset = 0;
            // Once we have a buffer, iterate through each column and write data
            for (ci, c) in columns.iter_mut().enumerate() {
                // Within each column, write rows, striding the row buffer and
                // indexing consecutively in the current column
                let src_width = fwidth(ci);
                for row in 0..self.rows_per_buffer {
                    let src_idx = SrcIndex(src_col_offset + self.row_width * row);
                    let dst_idx = DstIndex(dst_row_offset + row);
                    fread(c, dst_idx, &self.bytes, src_idx).map_err(ImpureError::Pure)?;
                }
                src_col_offset += src_width;
            }
            dst_row_offset += self.rows_per_buffer;
        }

        // Read remaining rows if they exist
        self.read_remainder(h)?;
        src_col_offset = 0;
        for (ci, c) in columns.iter_mut().enumerate() {
            for row in 0..self.remainder_row_number() {
                let src_idx = SrcIndex(src_col_offset + self.row_width * row);
                let dst_idx = DstIndex(dst_row_offset + row);
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
            self.rows_per_buffer * self.whole_row_number() <= self.nrows,
            "invalid whole reads number"
        );

        // Read groups of rows in outer loop
        for buf_idx in 0..self.whole_row_number() {
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
                    let src_idx = SrcIndex(src_col_offset + self.row_width * row);
                    debug_assert!(
                        src_idx.0 + src_len < u64_to_usize(self.buf_size),
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
                    *value = from_buf(&buf);
                }
            }
        }

        // Read remaining rows if they exist
        self.read_remainder(h)?;
        let remainder_rows = self.remainder_row_number();
        let dst_row_offset = self.whole_row_number() * self.rows_per_buffer;
        for (ci, c) in columns.iter_mut().enumerate() {
            let src_col_offset = ci * src_len;
            let local_c = &mut c[dst_row_offset..dst_row_offset + remainder_rows];
            for (row, value) in local_c.iter_mut().enumerate() {
                let src_idx = SrcIndex(src_col_offset + self.row_width * row);
                debug_assert!(
                    src_idx.0 + src_len < u64_to_usize(self.buf_size),
                    "out of bounds"
                );
                // SAFETY: see above
                let buf = unsafe { T::array_from_slice(&self.bytes, src_idx) };
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

    /// Read a matrix where input bytes characters to be read as u64
    fn read_char_matrix<R: Read>(
        &mut self,
        h: &mut BufReader<R>,
        cols: &mut [RangedVec<FixedAsciiRange, u64>],
    ) -> IOResult<(), AsciiToUintError> {
        // TODO this smells like something that could be cleaned up later
        let ranges: Vec<_> = cols
            .iter()
            .map(|c| usize::from(u8::from(c.range.chars())))
            .collect();
        self.read_columns(
            h,
            cols,
            |dst, dst_index, src, src_index| {
                let src_width = usize::from(u8::from(dst.range.chars()));
                let x = ascii_to_uint(&src[src_index.0..src_index.0 + src_width])?;
                dst.data[dst_index.0] = x;
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
            .map(|c| usize::from(u8::from(c.bytes())))
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
        let src_widths: Vec<_> = cols
            .iter()
            .map(|c| match c {
                MixedVec::Ascii(x) => usize::from(u8::from(x.range.chars())),
                MixedVec::Uint(x) => usize::from(u8::from(x.bytes())),
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

impl WriteBuffer {
    fn write<W: Write>(&self, h: &mut BufWriter<W>) -> io::Result<()> {
        h.write_all(&self.bytes[..])
    }

    fn write_remainder<W: Write>(&self, h: &mut BufWriter<W>) -> io::Result<()> {
        let n = self.remainder_bytes();
        h.write_all(&self.bytes[..n])
    }

    fn write_columns<C, W, Fp, Fw>(
        &mut self,
        h: &mut BufWriter<W>,
        columns: &[C],
        mut fpush: Fp,
        fwidth: Fw,
    ) -> io::Result<()>
    where
        W: Write,
        Fp: FnMut(&C, SrcIndex, &mut [u8], DstIndex),
        Fw: Fn(usize) -> usize,
    {
        // Write groups of rows in outer loop
        let mut dst_col_offset;
        let mut src_row_offset = 0;
        for _ in 0..self.whole_row_number() {
            dst_col_offset = 0;
            // Once we have a buffer, iterate through each column and write data
            for (ci, c) in columns.iter().enumerate() {
                // Within each column, write rows, striding the row buffer and
                // indexing consecutively in the current column
                let src_width = fwidth(ci);
                for row in 0..self.rows_per_buffer {
                    let dst_idx = DstIndex(dst_col_offset + self.row_width * row);
                    let src_idx = SrcIndex(src_row_offset + row);
                    fpush(c, src_idx, &mut self.bytes, dst_idx);
                }
                dst_col_offset += src_width;
            }
            src_row_offset += self.rows_per_buffer;
            self.write(h)?;
        }

        // Read remaining rows if they exist
        let remainder_rows = self.remainder_row_number();
        dst_col_offset = 0;
        for (ci, c) in columns.iter().enumerate() {
            for row in 0..remainder_rows {
                let src_idx = SrcIndex(dst_col_offset + self.row_width * row);
                let dst_idx = DstIndex(src_row_offset + row);
                fpush(c, src_idx, &mut self.bytes, dst_idx);
            }
            dst_col_offset += fwidth(ci);
        }

        self.write_remainder(h)?;

        Ok(())
    }

    /// Read stream of bytes using buffer where each value is the same type
    fn write_matrix<W, T, F>(
        &mut self,
        h: &mut BufWriter<W>,
        columns: &[&[T]],
        to_buf: F,
    ) -> io::Result<()>
    where
        W: Write,
        F: Fn(&T) -> T::FileBuf,
        T: FCSRepr,
    {
        // This has similar analogous optimizations and assumptions as
        // ReadBuffer::read_matrix
        let dst_len = T::file_len();
        assert!(
            columns.iter().all(|c| c.len() == self.nrows),
            "all column lengths should be equal to given row number"
        );
        assert!(
            columns.len() * dst_len == self.row_width,
            "incorrect column number size"
        );
        assert!(
            self.rows_per_buffer * self.whole_row_number() <= self.nrows,
            "invalid whole reads number"
        );

        // Write groups of rows in outer loop
        for buf_idx in 0..self.whole_row_number() {
            let start_row = buf_idx * self.rows_per_buffer;
            // Once we have a buffer, iterate through each column and write data
            for (ci, c) in columns.iter().enumerate() {
                let dst_col_offset = ci * dst_len;
                // Within each column, write rows, striding the row buffer and
                // indexing consecutively in the current column
                let end_row = start_row + self.rows_per_buffer;
                let local_c = &c[start_row..end_row];
                for (row, value) in local_c.iter().enumerate() {
                    let dst_idx = DstIndex(dst_col_offset + self.row_width * row);
                    debug_assert!(
                        dst_idx.0 + dst_len < u64_to_usize(self.buf_size),
                        "out of bounds"
                    );
                    let buf = to_buf(value);
                    // SAFETY: src_idx given as row_width * R + C * LEN where R
                    // is row index (within the buffer) and C is column index.
                    // Both R and C must be less than the number of rows per
                    // buffer and the number of columns respectively since we
                    // are getting these via enumerate(). Therefore, the maximum
                    // that src_idx can ever be is row_width * (rows_per_buffer
                    // - 1) + (column_number - 1) * LEN. Adding LEN to the end
                    // of this exactly equals the size of the buffer itself in
                    // bytes, which means what follows can never overflow.
                    unsafe {
                        T::array_to_slice(&buf, &mut self.bytes, dst_idx);
                    };
                }
            }
            self.write(h)?;
        }

        // Write remaining rows if they exist
        let remainder_rows = self.remainder_row_number();
        let dst_row_offset = self.whole_row_number() * self.rows_per_buffer;
        for (ci, c) in columns.iter().enumerate() {
            let dst_col_offset = ci * dst_len;
            let local_c = &c[dst_row_offset..dst_row_offset + remainder_rows];
            for (row, value) in local_c.iter().enumerate() {
                let dst_idx = DstIndex(dst_col_offset + self.row_width * row);
                debug_assert!(
                    dst_idx.0 + dst_len < u64_to_usize(self.buf_size),
                    "out of bounds"
                );
                let buf = to_buf(value);
                // SAFETY: see above
                unsafe {
                    T::array_to_slice(&buf, &mut self.bytes, dst_idx);
                };
            }
        }

        self.write_remainder(h)?;

        Ok(())
    }

    /// Write a matrix where type is an aligned big or little endian value.
    fn write_endian_matrix<W, T>(
        &mut self,
        h: &mut BufWriter<W>,
        cols: &[&[T]],
        endian: Endian,
    ) -> io::Result<()>
    where
        W: Write,
        T: ToBytes<Bytes = T::FileBuf> + FCSRepr,
    {
        match endian {
            Endian::Big => self.write_matrix(h, cols, T::to_be_bytes),
            Endian::Little => self.write_matrix(h, cols, T::to_le_bytes),
        }
    }

    /// Write a matrix where type is an aligned big, little, or mixed endian value.
    fn write_ordered_matrix<W, T>(
        &mut self,
        h: &mut BufWriter<W>,
        cols: &[&[T]],
        s: ArrayByteOrd<T::ByteOrd>,
    ) -> io::Result<()>
    where
        W: Write,
        T: ToBytes<Bytes = T::FileBuf> + FCSRepr,
        T::FileBuf: AsRef<[u8]> + AsMut<[u8]> + Default,
        T::ByteOrd: AsRef<[u8]>,
    {
        match s {
            ArrayByteOrd::Endian(e) => self.write_endian_matrix(h, cols, e),
            ArrayByteOrd::Order(o) => self.write_matrix(h, cols, |bs| T::to_ordered_bytes(bs, &o)),
        }
    }

    /// Write a matrix where input bytes characters are to be read as u64
    fn write_char_matrix<W: Write>(
        &mut self,
        h: &mut BufWriter<W>,
        cols: &[NativeColumn<FixedAsciiRange>],
    ) -> io::Result<()> {
        let ranges: Vec<_> = cols
            .iter()
            .map(|c| usize::from(u8::from(c.header.chars())))
            .collect();
        self.write_columns(
            h,
            cols,
            |src, src_index, dst, dst_index| {
                let v = src.as_ref()[src_index.0];
                src.header.to_slice_unchecked(v, dst, dst_index);
            },
            |i| ranges[i],
        )
    }

    /// Write a dataframe of unsigned integers with different widths
    fn write_any_uint_df<W: Write>(
        &mut self,
        h: &mut BufWriter<W>,
        cols: &[AnyBitmaskColumn],
        endian: Endian,
    ) -> io::Result<()> {
        let src_widths: Vec<_> = cols
            .iter()
            .map(|c| usize::from(u8::from(c.bytes())))
            .collect();
        match endian {
            Endian::Big => self.write_columns(h, cols, AnyUint::write_be, |i| src_widths[i]),
            Endian::Little => self.write_columns(h, cols, AnyUint::write_le, |i| src_widths[i]),
        }
    }

    /// Write a dataframe of any mix of column types
    fn write_mixed_df<W: Write>(
        &mut self,
        h: &mut BufWriter<W>,
        cols: &[MixedColumn],
        endian: Endian,
    ) -> io::Result<()> {
        let src_widths: Vec<_> = cols
            .iter()
            .map(|c| match c {
                AnyDatatype::Ascii(x) => usize::from(u8::from(x.header.chars())),
                AnyDatatype::Uint(x) => usize::from(u8::from(x.bytes())),
                AnyDatatype::F32(_) => 4,
                AnyDatatype::F64(_) => 8,
            })
            .collect();
        match endian {
            Endian::Big => self.write_columns(h, cols, AnyDatatype::write_be, |i| src_widths[i]),
            Endian::Little => self.write_columns(h, cols, AnyDatatype::write_le, |i| src_widths[i]),
        }
    }
}

// Misc functions used throughout module

fn ascii_to_uint(buf: &[u8]) -> Result<u64, AsciiToUintError> {
    if buf.is_ascii() {
        // SAFETY: we just checked that all bytes are ASCII
        let s = unsafe { str::from_utf8_unchecked(buf) };
        s.parse().map_err(AsciiToUintError::from)
    } else {
        Err(NotAsciiError(buf.to_vec()).into())
    }
}

// TODO put these in a more general place
fn u64_to_usize(x: u64) -> usize {
    usize::try_from(x).expect("overflow")
}

fn usize_to_u64(x: usize) -> u64 {
    u64::try_from(x).expect("overflow")
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

// impl<'a> IntoWriter<'a, Endian> for AnyBitmask {
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

// impl<D> EndianLayout<AnyBitmask, D> {
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

// impl<D> EndianLayout<MixedRange, D> {
//     // pub(crate) fn try_into_ordered<T>(
//     //     self,
//     // ) -> ErrorsResult<AnyOrderedLayout<T>, (), MixedToOrderedLayoutError> {
//     //     macro_rules! from_columns {
//     //         ($i:expr) => {
//     //             $i.enumerate()
//     //                 .map(|(i, c)| {
//     //                     c.try_into()
//     //                         .map_err(|e| IndexedError::new(i + 1, e))
//     //                         .map_err(MixedToNonMixedLayoutError)
//     //                         .map_err(MixedToOrderedLayoutError::from)
//     //                         .into_log()
//     //                 })
//     //                 .sequence_commutative()
//     //         };
//     //     }

//     //     let mut it = self.columns.into_iter().peekable();
//     //     if let Some(head) = it.next() {
//     //         let endian = self.byte_layout;
//     //         match head {
//     //             AnyDatatype::Uint(x) => x
//     //                 .try_into_one_size(it, endian, 1)
//     //                 .map_ok_value(AnyOrderedLayout::Integer)
//     //                 .map_errors(|(index, error)| error.into_col_error(index)),
//     //             AnyDatatype::Ascii(x) => from_columns!(it)
//     //                 .map_ok_value(|xs| FixedLayout::new1(x, xs, NoByteOrd))
//     //                 .map_ok_value(AnyAsciiLayout::from)
//     //                 .map_ok_value(AnyOrderedLayout::Ascii),
//     //             AnyDatatype::F32(x) => from_columns!(it)
//     //                 .map_ok_value(|xs| FixedLayout::new1(x, xs, endian.into()))
//     //                 .map_ok_value(AnyOrderedLayout::F32),
//     //             AnyDatatype::F64(x) => from_columns!(it)
//     //                 .map_ok_value(|xs| FixedLayout::new1(x, xs, endian.into()))
//     //                 .map_ok_value(AnyOrderedLayout::F64),
//     //         }
//     //     } else {
//     //         let b: ArrayByteOrd<[u8; 4]> = self.byte_layout.into();
//     //         LogResult::new_ok(AnyOrderedLayout::F32(FixedLayout::new(vec![], b)))
//     //     }
//     // }

//     // pub(crate) fn try_into_non_mixed(
//     //     self,
//     // ) -> ErrorsResult<NonMixedEndianLayout<Nothing<NumType>>, (), MixedToNonMixedLayoutError> {
//     //     let mut it = self.columns.into_iter().peekable().enumerate();
//     //     if let Some((_, c0)) = it.next() {
//     //         macro_rules! from_iter {
//     //             ($iter:expr, $head:expr, $byte_layout:expr) => {
//     //                 $iter
//     //                     .map(|(i, c)| c.try_into().map_err(|e| (i, e)).into_log())
//     //                     .sequence_commutative()
//     //                     .map_ok_value(|xs| FixedLayout::new1($head, xs, $byte_layout))
//     //                     .map_ok_value(NonMixedEndianLayout::from)
//     //             };
//     //         }

//     //         let byte_layout = self.byte_layout;
//     //         match c0 {
//     //             AnyDatatype::Ascii(x) => it
//     //                 .map(|(i, c)| c.try_into().map_err(|e| (i, e)).into_log::<_, _, Vec<_>>())
//     //                 .sequence_commutative()
//     //                 .map_ok_value(|xs| FixedLayout::new1(x, xs, NoByteOrd))
//     //                 .map_ok_value(|l| AnyAsciiLayout::Fixed(l).into()),
//     //             AnyDatatype::Uint(x) => from_iter!(it, x, byte_layout),
//     //             AnyDatatype::F32(x) => from_iter!(it, x, byte_layout),
//     //             AnyDatatype::F64(x) => from_iter!(it, x, byte_layout),
//     //         }
//     //         .map_errors(|(i, error)| IndexedError::new(i + 1, error).into())
//     //     } else {
//     //         let l = FixedLayout::new(vec![], self.byte_layout);
//     //         LogResult::new_ok(NonMixedEndianLayout::Uint(l))
//     //     }
//     // }
// }

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

// impl<C, S, T, D> DataLayout<C, S, T, D> {
//     // fn new1(head: C, tail: impl IntoIterator<Item = C>, byte_layout: S) -> Self {
//     //     let mut cs = vec![head];
//     //     cs.extend(tail);
//     //     Self::new(cs, byte_layout)
//     // }

//     // fn insert_column_nocheck(&mut self, index: MeasIndex, col: C) {
//     //     debug_assert!(
//     //         usize::from(index) <= self.inner.len(),
//     //         "Index should be less than/equal to number of columns"
//     //     );
//     //     self.inner.insert(index.into(), col);
//     // }

//     // fn push_column(&mut self, col: C) {
//     //     self.inner.push(col);
//     // }
// }

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

#[cfg(feature = "python")]
mod python {
    use super::{AnyBitmask, FloatRange, MixedRange};

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
                    Ok(AnyBitmask::from(value.extract::<BitmaskValue<u64>>()?).into())
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

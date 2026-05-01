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
//! allows the data layout to include any type. This is more complex but is not
//! computationally very different from (3).
//!
//! In addition to width, layouts may also be classified by whether $TOT is
//! known. In 2.0, $TOT is optional and may not be given. For *delimited* ASCII
//! layouts, not have $TOT means we need to parse until we reach the end of
//! DATA, hoping that all columns have the same length. For *fixed* layouts, we
//! can compute $TOT using $PnB and the length of DATA.
//!
//! ### Conceptual model
//!
//! For a given FCS dataset, the $PnB, $PnR, $BYTEORD, $DATATYPE, and optionally
//! $PnDATATYPE keywords form the *data schema* which describes how data is
//! arranged in the DATA segment.
//!
//! This data schema can then be given a stream of bytes corresponding to DATA
//! that are used to fill in the *series* for each given column in the layout.
//! The resulting type is a "dataframe" which can be manipulated in memory and
//! written to disc. Each series inside the *dataframe* is internally validated
//! to fit based on the values of $PnR and $PnB.
//!
//! *Dataframe*s can be accessed either as the *data schema* used to create them or
//! via their underlying series. This separation is important because it is
//! easier to think about the contents of the DATA segment and the metadata
//! used to describe it separately.
//!
//! ### Performance
//!
//! This module includes several performance optimizations:
//!
//! * Cache-coherence is optimized via a buffer which holds a set number of
//!   rows. This should be small enough to fit in the CPU cache. This also
//!   allows us to deal with one column at a time in memory, which further
//!   optimizes cache performance and allows transposition between column-major
//!   in memory and row-major in FCS files.
//! * Any numeric layout (non-ASCII) that can be represented as a matrix has
//!   specialized read and write loops. These have no conditional branching
//!   inside them which allows the compiler to optimize them further (likely
//!   unrolling on many architectures).
//! * Layouts with mixed types are treated as "matrices" if their types have the
//!   same width. These are read/written using intermediate columns of all one
//!   type which are cast to the final type by reinterpreting bits (a zero-cost
//!   operation).
//! * Data itself is stored in a Polars buffers, which are effectively act like
//!   `Arc<Vec<T>>`. This means cloning is very fast and easy as is passing
//!   data between language boundaries.

// Terminology
//
// * column: The data schema or series corresponding to a given FCS measurement.
// * dataframe: The data schema and series for all FCS measurement in a DATA
//   segment.
// * datatype: The value of $DATATYPE or $PnDATATYPE (if 3.2) for a given
//   column.
// * fixed: Describes layouts whose columns are all a set width in bytes.
// * column schema: The collective value of $PnR and $PnB for a given measurement.
//   For all column types, this will be represented as one rust type.
// * data schema: All column schema with the values of $DATATYPE and $BYTEORD
//   which collectively describe the layout for DATA.
// * layout: Refers to either the schema or dataframe for all measurements.
// * range: The value of $PnR
// * series: The data for a measurement.
// * width: The value of $PnB

use crate::config::{
    AllowTotMismatch, ConfigFlag as _, DisallowOverRange, DisallowRangeTrunc,
    ReadDataKeywordsConfig, ReadEventsConfig, WriteDatasetInnerConfig,
};
use crate::core::{
    AsScaleOrTransform, Measurements, NamedTemporalsAndOpticals, ScaleTransform, TemporalOrOptical,
    VersionSet,
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
    AnyByteOrder, ArgBytes, ArrayByteOrd, BitsOrChars, ByteOrdToSizedError, Bytes, Endian,
    HasByteOrd, NoByteOrd, OrderedToEndianError, PrivBytes, VecToSizedError, WidthToBytesError,
    WidthToFixedError,
};
use crate::text::index::{IndexFromOne, MeasIndex};
use crate::text::keywords::{
    AlphaNumType, ByteOrd2_0, ByteOrd3_1, Gain, Keyword0FromValue as _, Keyword1FromValue as _,
    NumType, Par, RangeToIntError, RangeToIntErrorKind, ReqMeasKeyword, ReqRootKeyword, Scale,
    SplitKeyword0, SplitKeyword1, TextRange, Tot, Width,
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
    Bitmask64, BitmaskValue, NewBitmaskError,
};
use crate::validated::dataframe::{
    AnyPrimitiveSeries, CastSeriesError, DataFrame, DataFrameFamily, FromSeries, FromValue,
    HasFCSType, HasLen, HasWidth, InternalSeries, PrimitiveDataFrame, PrimitiveSeries,
    ambassador_impl_HasLen, ambassador_impl_HasWidth,
};
use crate::validated::finite_float::{
    DecimalToFloatError, FiniteF32, FiniteF64, FiniteF64toF32Error, FiniteFloat,
    U64ToFiniteFloatError,
};
use crate::validated::keys::{IndexedKey as _, NonStdKeywords, StdKeywords};
use crate::validated::row_buffer::{ReadBuffer, WriteBuffer};
use crate::validated::unaligned::{DstIndex, FCSRepr, SrcIndex, U24, U40, U48, U56};

use fireflow_core_proc::{IntoInner, impl_generic_enum_from};
use fireflow_types::config::CheckedRangeDatatypes;
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
use num_traits::{Bounded, ToPrimitive as _};
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
    fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr, FromInnerPyObject},
    fireflow_types::python as py,
    pyo3::prelude::*,
};

/// All possible byte layouts for the DATA segment in 2.0.
///
/// This is identical to 3.0 in every way except that the $TOT keyword in 2.0
/// is optional, which requires a different interface.
pub type DataSchema2_0 = AnyOrderedLayout2_0<VecFamily>;

/// All possible DATA storage configurations for 2.0.
///
/// This is identical to 3.0 in every way except that the $TOT keyword in 2.0
/// is optional, which requires a different interface.
pub type DataFrame2_0 = AnyOrderedLayout2_0<DataFrameFamily>;

/// All possible byte layouts for the DATA segment in 3.0.
pub type DataSchema3_0 = AnyOrderedLayout3_0<VecFamily>;

/// All possible DATA storage configurations in 3.0.
pub type DataFrame3_0 = AnyOrderedLayout3_0<DataFrameFamily>;

/// All possible byte layouts for the DATA segment in 3.1.
///
/// Unlike 2.0 and 3.0, the integer layout allows the column widths to be
/// different. This is a consequence of making BYTEORD only mean "big or little
/// endian" and have nothing to do with number of bytes.
pub type DataSchema3_1 = NonMixedDataSchema<Nothing<NumType>>;

/// All possible storage configurations in 3.1.
///
/// Unlike 2.0 and 3.0, the integer layout allows the column widths to be
/// different. This is a consequence of making BYTEORD only mean "big or little
/// endian" and have nothing to do with number of bytes.
pub type DataFrame3_1 = NonMixedDataFrame<Nothing<NumType>>;

/// All possible byte layouts for the DATA segment in 3.2.
///
/// In addition to the loosened integer layouts in 3.1, 3.2 additionally allows
/// each column to have a different type and size (hence "Mixed").
pub type DataSchema3_2 = Any3_2Layout<VecFamily>;

/// All possible storage configurations in 3.2.
///
/// In addition to the loosened integer layouts in 3.1, 3.2 additionally allows
/// each column to have a different type and size (hence "Mixed").
pub type DataFrame3_2 = Any3_2Layout<DataFrameFamily>;

/// Generic container for 3.2 DATA configurations.
// TODO false positive lint
#[allow(clippy::duplicated_attributes)]
#[derive(Clone, Delegate, PartialEq, IntoInner)]
#[into_inner(PrimitiveDataFrame)]
#[delegate(HasWidth)]
#[delegate(LayoutHeight)]
#[delegate(LayoutSize)]
#[delegate(LayoutDatatype, where = "M: HasWidth, N: HasWidth")]
#[delegate(LayoutKeywords, where = "M: HasWidth, N: HasWidth")]
#[delegate(LayoutRanges<R>, generics = "R")]
#[delegate(LayoutRemove<R>, generics = "R", where = "Self: LayoutNormalize")]
#[delegate(DataFrameWriteOps)]
#[delegate(DataFrameCheckRanges)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum Any3_2<M, N> {
    Mixed(M),
    NonMixed(N),
}

type Any3_2Layout<Fam> = Any3_2<MixedLayout<Fam>, AnyBigLittleDatatypeLayout<Fam, Option<NumType>>>;

type MixedLayout<Fam> = FamilyLayout<Fam, MixedCol, false, ColumnMarkers3_2>;

pub type MixedDataSchema = MixedLayout<VecFamily>;

type MixedDataFrame = MixedLayout<DataFrameFamily>;

type ColumnMarkers3_2 = ColumnMarkers<Identity<Tot>, Option<NumType>>;

pub type AnyOrderedLayout<Fam, T> =
    AnyOrderedDatatypeLayout<Fam, true, ColumnMarkers<T, Nothing<NumType>>>;

pub type AnyOrderedDataSchema<T> = AnyOrderedLayout<VecFamily, T>;

pub type AnyOrderedLayout2_0<Fam> = AnyOrderedLayout<Fam, Option<Tot>>;

pub type AnyOrderedLayout3_0<Fam> = AnyOrderedLayout<Fam, Identity<Tot>>;

type NonMixedLayout<Fam, D> = AnyBigLittleDatatypeLayout<Fam, D>;

pub type NonMixedDataSchema<D> = NonMixedLayout<VecFamily, D>;

type NonMixedDataFrame<D> = NonMixedLayout<DataFrameFamily, D>;

type VariableUintLayout<F, D> = FamilyLayout<F, UvarCol, false, ColumnMarkers<Identity<Tot>, D>>;

pub type VariableUintDataSchema<D> = VariableUintLayout<VecFamily, D>;

type VariableUintDataFrame<D> = VariableUintLayout<DataFrameFamily, D>;

// TODO false positive lint
#[allow(clippy::duplicated_attributes)]
#[derive(Clone, Delegate, PartialEq, IntoInner)]
#[into_inner(PrimitiveDataFrame)]
#[delegate(HasWidth)]
#[delegate(LayoutHeight)]
#[delegate(LayoutSize)]
#[delegate(LayoutDatatype, where = "Delim: HasWidth, Fixed: HasWidth")]
#[delegate(LayoutKeywords, where = "Delim: HasWidth, Fixed: HasWidth")]
#[delegate(LayoutRanges<R>, generics = "R")]
#[delegate(LayoutInsert<R>, generics = "R")]
#[delegate(LayoutRemove<R>, generics = "R", where = "Self: LayoutNormalize")]
#[delegate(LayoutOptMeasKeywords)]
#[delegate(DataFrameWriteOps)]
#[delegate(DataFrameCheckRanges)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum AnyAscii<Delim, Fixed> {
    Delimited(Delim),
    Fixed(Fixed),
}

type AnyAsciiLayout<Fam, const ORD: bool, M> =
    AnyAscii<DelimAsciiLayout<Fam, ORD, M>, FixedAsciiLayout<Fam, ORD, M>>;

type AnyAsciiLayout2_0<F, T> = AnyAsciiLayout<F, true, ColumnMarkers<T, Nothing<NumType>>>;

type AnyAsciiLayout3_1<F, D> = AnyAsciiLayout<F, false, ColumnMarkers<Identity<Tot>, D>>;

pub type AnyAsciiDataSchema<const ORD: bool, M> = AnyAsciiLayout<VecFamily, ORD, M>;

type DelimAsciiLayout<Fam, const ORD: bool, M> = FamilyLayout<Fam, DelimAsciiCol, ORD, M>;

pub type DelimAsciiDataSchema<const ORD: bool, M> = DelimAsciiLayout<VecFamily, ORD, M>;

type FixedAsciiLayout<Fam, const ORD: bool, M> = FamilyLayout<Fam, FixedAsciiCol, ORD, M>;

pub type FixedAsciiDataSchema<const ORD: bool, M> = FixedAsciiLayout<VecFamily, ORD, M>;

// type FixedAsciiDataFrame<const ORD: bool, M> = FixedAsciiLayout<DataFrameFamily, ORD, M>;

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

type FamilyLayout<Fam, Col, const ORD: bool, M> = Layout<
    <Fam as Kind1>::Type<<Col as IsCol<Fam, ORD>>::Inner>,
    Fam,
    Col,
    <Col as IsCol<Fam, ORD>>::Layout,
    M,
    ORD,
>;

type DataSchema_<C, const ORD: bool, M> = FamilyLayout<VecFamily, C, ORD, M>;

type DataFrame_<C, const ORD: bool, M> = FamilyLayout<DataFrameFamily, C, ORD, M>;

/// DATA layout where each column has a fixed width.
#[derive(Clone, AsRef, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[new(visibility(""))]
pub struct Layout<Cols, CFam, Inner, ByteOrder, Markers, const ORD: bool> {
    /// Thing holding the columns.
    container: Cols,
    /// The byte layout of a value in a column.
    #[as_ref(ByteOrder)]
    byteord: ByteOrder,
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
#[delegate(HasWidth)]
#[delegate(LayoutHeight)]
#[delegate(LayoutSize)]
#[delegate(LayoutDatatype, where = "Single: HasWidth, Multi: HasWidth")]
#[delegate(LayoutKeywords, where = "Single: HasWidth, Multi: HasWidth")]
#[delegate(LayoutRanges<R>, generics = "R")]
#[delegate(LayoutRemove<R>, generics = "R", where = "Self: LayoutNormalize")]
#[delegate(LayoutOptMeasKeywords)]
#[delegate(DataFrameWriteOps)]
#[delegate(DataFrameCheckRanges)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum AnyBigLittleUint<Single, Multi> {
    Single(Single),
    Multi(Multi),
}

type AnyBigLittleUintLayout<Fam, D> =
    AnyBigLittleUint<AnySingleUintLayout<Fam, D>, VariableUintLayout<Fam, D>>;

type AnySingleUintLayout<Fam, D> = AnyUintLayout<Fam, false, ColumnMarkers<Identity<Tot>, D>>;

pub type AnySingleUintDataSchema<D> = AnySingleUintLayout<VecFamily, D>;

pub type AnyBigLittleUintDataSchema<D> = AnyBigLittleUintLayout<VecFamily, D>;

type AnyBigLittleUintDataFrame<D> = AnyBigLittleUintLayout<DataFrameFamily, D>;

/// Vector of data with a column schema.
///
/// This is used internally to represent the data in DATA with its associated
/// keywords.
#[derive(Clone, PartialEq, Into, new)]
#[new(visibility = "")]
pub struct Series<M, T, R> {
    column_schema: M,
    #[into(PrimitiveSeries<T>)]
    series: InternalSeries<T, R>,
}

impl<M, T, R> Series<M, T, R> {
    pub fn from_prim(
        metadata: M,
        series: AnyPrimitiveSeries,
    ) -> Result<Self, <AnyPrimitiveSeries as TryInto<InternalSeries<T, R>>>::Error>
    where
        AnyPrimitiveSeries: TryInto<InternalSeries<T, R>>,
    {
        Ok(Self::new(metadata, series.try_into()?))
    }

    pub(crate) fn column_schema(&self) -> &M {
        &self.column_schema
    }

    fn empty(metadata: M) -> Self {
        Self::new(metadata, InternalSeries::default())
    }
}

impl<M, T, R> HasLen for Series<M, T, R> {
    fn len(&self) -> usize {
        self.series.len()
    }
}

/// A series whose metadata maps to exactly one Rust type.
pub type NativeSeries<C> = Series<
    C,
    <<C as ColumnHasNativeType>::Native as FCSRepr>::Prim,
    <C as ColumnHasNativeType>::Native,
>;

type NativeInternalSeries<C> = InternalSeries<
    <<C as ColumnHasNativeType>::Native as FCSRepr>::Prim,
    <C as ColumnHasNativeType>::Native,
>;

impl<T> AsRef<[T::Native]> for NativeSeries<T>
where
    T: ColumnHasNativeType,
    T::Native: FCSRepr,
    NativeInternalSeries<T>: AsRef<[T::Native]>,
{
    fn as_ref(&self) -> &[T::Native] {
        self.series.as_ref()
    }
}

impl<T> From<RangedVec<T, T::Native>> for NativeSeries<T>
where
    T: ColumnHasNativeType,
    T::Native: FCSRepr,
    Vec<T::Native>: Into<NativeInternalSeries<T>>,
{
    fn from(value: RangedVec<T, T::Native>) -> Self {
        Self::new(value.range, value.data.into())
    }
}

/// Generic container for anything that can be categorized by $DATATYPE.
// TODO false positive lint
#[allow(clippy::duplicated_attributes)]
#[derive(Debug, PartialEq, Clone, Copy, Delegate, IntoInner)]
#[into_inner(PrimitiveDataFrame)]
#[delegate(ColumnIsFixed)]
#[delegate(HasLen)]
#[delegate(HasWidth)]
#[delegate(LayoutHeight)]
#[delegate(LayoutSize)]
#[delegate(
    LayoutDatatype,
    where = "A: HasWidth, \
             U: HasWidth, \
             F: HasWidth, \
             D: HasWidth"
)]
#[delegate(
    LayoutKeywords,
    where = "A: HasWidth, \
             U: HasWidth, \
             F: HasWidth, \
             D: HasWidth"
)]
#[delegate(LayoutRanges<R>, generics = "R")]
#[delegate(LayoutRemove<R>, generics = "R")]
#[delegate(LayoutOptMeasKeywords)]
#[delegate(DataFrameWriteOps)]
#[delegate(DataFrameCheckRanges)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum AnyDatatype<A, U, F, D> {
    Ascii(A),
    Uint(U),
    F32(F),
    F64(D),
}

type AnyDatatypeLayout<Fam, const ORD: bool, M, I> = AnyDatatype<
    AnyAsciiLayout<Fam, ORD, M>,
    I,
    FamilyLayout<Fam, F32Col, ORD, M>,
    FamilyLayout<Fam, F64Col, ORD, M>,
>;

type AnyOrderedDatatypeLayout<Fam, const ORD: bool, M> =
    AnyDatatypeLayout<Fam, ORD, M, AnyUintLayout<Fam, ORD, M>>;

pub type OrderedLayout<F, I, T> = FamilyLayout<F, I, true, ColumnMarkers<T, Nothing<NumType>>>;

pub type BigLittleLayout<F, I, D> = FamilyLayout<F, I, false, ColumnMarkers<Identity<Tot>, D>>;

pub type OrderedDataSchema<I, T> = OrderedLayout<VecFamily, I, T>;

pub type BigLittleDataSchema<I, D> = BigLittleLayout<VecFamily, I, D>;

type AnyBigLittleDatatypeLayout<Fam, D> =
    AnyDatatypeLayout<Fam, false, ColumnMarkers<Identity<Tot>, D>, AnyBigLittleUintLayout<Fam, D>>;

pub type MixedRange = AnyDatatype<FixedAsciiRange, VariableBitmask, F32Range, F64Range>;

pub type MixedSeries = AnyDatatype<
    NativeSeries<FixedAsciiRange>,
    VariableUintSeries,
    NativeSeries<F32Range>,
    NativeSeries<F64Range>,
>;

/// A big or little-endian integer column of some size (1-8 bytes)
// TODO false positive lint
#[allow(clippy::duplicated_attributes)]
#[derive(Debug, PartialEq, Clone, Copy, Delegate, IntoInner)]
#[into_inner(PrimitiveDataFrame)]
#[delegate(HasLen)]
#[delegate(ColumnIsBinary)]
#[delegate(HasWidth)]
#[delegate(LayoutHeight)]
#[delegate(LayoutSize)]
#[delegate(
    LayoutDatatype,
    where = "C08: HasWidth, \
             C16: HasWidth, \
             C24: HasWidth, \
             C32: HasWidth, \
             C40: HasWidth, \
             C48: HasWidth, \
             C56: HasWidth, \
             C64: HasWidth"
)]
#[delegate(
    LayoutKeywords,
    where = "C08: HasWidth, \
             C16: HasWidth, \
             C24: HasWidth, \
             C32: HasWidth, \
             C40: HasWidth, \
             C48: HasWidth, \
             C56: HasWidth, \
             C64: HasWidth"
)]
#[delegate(LayoutRanges<R>, generics = "R")]
#[delegate(LayoutInsert<R>, generics = "R")]
#[delegate(LayoutRemove<R>, generics = "R")]
#[delegate(LayoutOptMeasKeywords)]
#[delegate(DataFrameWriteOps)]
#[delegate(DataFrameCheckRanges)]
#[delegate(LayoutNormalize)]
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

type AnyUintLayout<Fam, const ORD: bool, M> = AnyUint<
    FamilyLayout<Fam, U08Col, ORD, M>,
    FamilyLayout<Fam, U16Col, ORD, M>,
    FamilyLayout<Fam, U24Col, ORD, M>,
    FamilyLayout<Fam, U32Col, ORD, M>,
    FamilyLayout<Fam, U40Col, ORD, M>,
    FamilyLayout<Fam, U48Col, ORD, M>,
    FamilyLayout<Fam, U56Col, ORD, M>,
    FamilyLayout<Fam, U64Col, ORD, M>,
>;

type AnyOrderedUintLayout<F, T> = AnyUintLayout<F, true, ColumnMarkers<T, Nothing<NumType>>>;

pub type AnyOrderedUintDataSchema<T> = AnyOrderedUintLayout<VecFamily, T>;

// type AnyOrderedUintDataFrame<T> = AnyOrderedUintLayout<DataFrameFamily, T>;

pub type VariableBitmask =
    AnyUint<Bitmask08, Bitmask16, Bitmask24, Bitmask32, Bitmask40, Bitmask48, Bitmask56, Bitmask64>;

/// A range with or without additional typing information.
///
/// This is meant for cases where createing a new column can be done with either
/// a general decimal value or a specific type which encodes further information
/// about the column to be written.
#[cfg_attr(feature = "python", derive(IntoPyObject, FromPyObject))]
pub enum MaybeTypedRange<D, S> {
    Untyped(D),
    Typed(S),
}

#[derive(Clone, Copy)]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
#[repr(transparent)]
pub struct FullIntRange(pub u64);

#[derive(Clone, Copy)]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromPyObject))]
pub enum FullRange {
    Float(FiniteF64),
    Int(FullIntRange),
}

pub type RangeAndSeries<R> = (R, AnyPrimitiveSeries);

pub type DecimalRangeAndSeries = RangeAndSeries<FullRange>;

pub type MaybeTypedVariableBitmask = MaybeTypedRange<FullRange, VariableBitmask>;

pub type MaybeTypedVariableUintSeries = MaybeTypedRange<DecimalRangeAndSeries, VariableUintSeries>;

pub type MaybeTypedMixedRange = MaybeTypedRange<FullRange, MixedRange>;

pub type MaybeTypedMixedSeries = MaybeTypedRange<DecimalRangeAndSeries, MixedSeries>;

pub type VariableUintSeries = AnyUint<
    NativeSeries<Bitmask08>,
    NativeSeries<Bitmask16>,
    NativeSeries<Bitmask24>,
    NativeSeries<Bitmask32>,
    NativeSeries<Bitmask40>,
    NativeSeries<Bitmask48>,
    NativeSeries<Bitmask56>,
    NativeSeries<Bitmask64>,
>;

/// The type of any floating point column in all versions
#[derive(PartialEq, Clone, Copy, Debug, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
#[cfg_attr(feature = "python", derive(IntoPyObject), pyo3(transparent))]
pub struct FloatRange<T> {
    range: FiniteFloat<T>,
}

pub type F32Range = FloatRange<f32>;
pub type F64Range = FloatRange<f64>;

/// A struct whose fields map 1-1 with keyword values in one data column
#[derive(new)]
pub struct DataSchemaKeywordValues<D> {
    width: Width,
    range: TextRange,
    datatype: D,
}

type DataSchemaKeywordValues2_0 = DataSchemaKeywordValues<Nothing<NumType>>;
type DataSchemaKeywordValues3_2 = DataSchemaKeywordValues<Option<NumType>>;

/// Diagnostic output when making new data schema from keywords
#[derive(new)]
pub struct NewDataSchema<T> {
    /// The data schema itself.
    pub data_schema: T,

    /// Original values of $PnR that were truncated.
    ///
    /// Length of vector will be equal to $PAR. If $PnR for a given column was
    /// truncated, it will be returned in its corresponding index. Otherwise the
    /// index will be [`Option::None`].
    pub truncated_columns: Vec<Option<TextRange>>,
}

impl_kind1!(pub NewDataSchemaFamily, NewDataSchema);

impl_functor_once!(
    NewDataSchema,
    self,
    mut f,
    NewDataSchema::new(f(self.data_schema), self.truncated_columns)
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

pub type OverrangeColumn = Option<(usize, bool)>;

/// Output of converting $PnR to native rust type.
#[derive(new)]
pub struct ConvertedRange<T> {
    /// The native value
    pub(crate) native: T,

    /// Original range if it needed to be truncated to make the native value.
    pub(crate) non_truncated: Option<TextRange>,
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
    Overrange(MeasIndex, usize, TextRange),
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

/// Error when keywords cannot be used to make new column schema.
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum NewDataSchemaError {
    /// $PnB and $PnR could not be used to make ASCII column schema
    Ascii(AsciiRangeFromKeywordsError),
    /// $PnB and $PnR could not be used to make integer column schema (2.0/3.0)
    FixedInt(NewFixedIntLayoutError),
    /// $PnB and $PnR could not be used to make integer column schema (3.1/3.2)
    VariableInt(NewUintTypeError),
    /// $PnB and $PnR could not be used to make float column schema
    Float(FloatWidthError),
    /// $PnB and $PnR could not be used to make mixed column schema (3.2)
    Mixed(NewMixedRangeError),
    /// $BYTEORD does not match width allowed via $DATATYPE for float column schema (2.0/3.0)
    ByteOrd(ByteOrdToSizedError),
}

/// Error when $PnB or $PnR cannot be used for an [`AnyOrderedUintDataSchema`] (2.0/3.0)
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum NewFixedIntLayoutError {
    Width(SingleFixedWidthError),
    Column(IndexedBitmaskError),
}

/// Error when making a new 2.0/3.0 integer layout.
#[derive(From, Error, Debug, Display)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum NewOrderedUintLayoutError {
    Bitmask(NewBitmaskError),
    ByteOrd(VecToSizedError),
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

/// Error when using $PnB and $PnR to make a new [`MixedRange`].
///
/// This only applies to FCS 3.2 and the value of $PnDATATYPE is implied by
/// the variant of this enum.
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum NewMixedRangeError {
    Ascii(AsciiRangeFromKeywordsError),
    Uint(NewUintTypeError),
    Float(FloatWidthError),
}

/// Warning when failing to truncate $PnR for use in a [`DataSchema3_2`].
#[derive(From, Display, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum NewMixedRangeWarning {
    Ascii(IndexedRangeToAsciiError),
    Uint(IndexedBitmaskError),
    Float(IndexedFloatRangeError),
}

/// Error when converting $PnR to float to be used in a float layout.
#[derive(From, Debug, Error)]
#[error(
    "could not use {k} in float layout because {e}",
    k = TextRange::std(_0.index),
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
        let rng = TextRange::std(i);
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
    k = TextRange::std(_0.index),
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
    k = TextRange::std(self.index),
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

type LookupLayoutResult<T> =
    WarningsAndErrorsResult<T, (), LookupDataSchemaWarning, LookupDataSchemaError>;

/// Error when looking up layout from key/value pairs
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupDataSchemaError {
    New(NewDataSchemaError),
    AlphaNumType(ReqKeyError<AlphaNumType>),
    ByteOrd2_0(ReqKeyError<ByteOrd2_0>),
    ByteOrd3_1(ReqKeyError<ByteOrd3_1>),
    Meas(LookupMeasLayoutError),
}

/// Warning when looking up layout from key/value pairs
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupDataSchemaWarning {
    New(NewMixedRangeWarning),
    Datatype(ReqKeyError<AlphaNumType>),
    Meas(OptIndexedKeyError<NumType>),
}

type LookupMeasLayoutResult<T> = WarningsAndErrorsResult<
    Vec<DataSchemaKeywordValues<T>>,
    (),
    OptIndexedKeyError<NumType>,
    LookupMeasLayoutError,
>;

type LookupOneMeasLayoutResult<T> = WarningsAndErrorsResult<
    DataSchemaKeywordValues<T>,
    (),
    OptIndexedKeyError<NumType>,
    LookupMeasLayoutError,
>;

/// Error when looking up measurement for layout from key/value pairs
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupMeasLayoutError {
    Width(ReqIndexedKeyError<Width>),
    Range(ReqIndexedKeyError<TextRange>),
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
    range: TextRange,
}

def_summary!(pub EventOverRangeSummary, "some events exceed $PnR");

pub type EventOverRangeErrors = ErrorGroup<EventOverRangeError, EventOverRangeSummary>;

/// Error when reading [`AnyAsciiDataSchema`]
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ReadAsciiError {
    Delim(ReadDelimAsciiError),
    Fixed(ReadFixedAsciiError),
}

/// Error when reading [`FixedAsciiDataSchema`]
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

/// Error when reading [`DelimAsciiDataSchema`] (with or without $TOT)
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

/// Error when reading [`DelimAsciiDataSchema`] with $TOT.
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ReadDelimWithRowsAsciiError {
    RowsExceeded(RowsExceededError),
    Incomplete(DelimIncompleteError),
    Parse(AsciiToUintError),
}

/// Error when reading [`DelimAsciiDataSchema`] where DATA is exhausted.
///
/// This happens if $TOT is greater than the true number of values in DATA.
#[derive(Debug, Error)]
#[error("Exceeded expected number of rows: {0}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct RowsExceededError(usize);

/// Error when reading [`DelimAsciiDataSchema`] where parsing ends unexpectedly.
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

/// Error when reading [`DelimAsciiDataSchema`] without $TOT
#[derive(From, Debug, Display, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ReadDelimAsciiWithoutRowsError {
    Parse(AsciiToUintError),
    Unequal(ReadDelimAsciiUnequalColumnsError),
}

/// Error when reading [`DelimAsciiDataSchema`] where columns are not equal length
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

/// Error when converting a [`NonMixedDataSchema`] to [`AnyOrderedUintDataSchema`]
///
/// This arises due to 3.1+ layouts being allowed to support any width and
/// 2.0/3.0 layouts only supporting one width due to the $BYTEORD constraint.
#[derive(From, Debug, Error)]
#[error(
    "{b} and {r} encoding {from}-byte integers are incompatible with {to}-byte integer layout",
    from = _0.error.from,
    to = _0.error.to,
    b = Width::std(_0.index),
    r = TextRange::std(_0.index),
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConversionError))]
pub struct UintEndianToOrderedLayoutError(IndexedError<UintToUintError>);

/// Error when converting a [`DataSchema3_2`] to a [`NonMixedDataSchema`]
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
    r = TextRange::std(_0.index),
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConversionError))]
pub struct MixedToNonMixedLayoutError(IndexedError<MixedToNonMixedError>);

/// Error when converting [`DataSchema3_2`] to [`AnyOrderedDataSchema`]
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

/// Error when attempting to insert new [`FullRange`] into a layout.
#[derive(From, Debug, Error, Display)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum InsertFullRangeError {
    Ascii(AsciiRangeValueFromFullRangeError),
    IntToInt(NewBitmaskError),
    DecimalToInt(BitmaskFromFullRangeError),
    F32(F32RangeFromFullRangeError),
    F64(U64ToFiniteFloatError),
    MismatchTypes(MismatchTypeRangeError),
}

/// Error when making a new [`Bitmask`] from a [`FullRange`].
#[derive(Error, Debug, From)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConfigError))]
pub enum BitmaskFromFullRangeError {
    #[from(NewBitmaskError)]
    #[error("{0}")]
    New(NewBitmaskError),
    #[error("Float '{0}' is not an integer")]
    Float(FiniteF64),
}

/// Error when making [`AsciiRangeValue`] from [`FullRange`];
#[derive(From, Debug, Error)]
#[error("Could not make Ascii range from float value '{0}'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConfigError))]
pub struct AsciiRangeValueFromFullRangeError(FiniteF64);

/// Error when making [`F64Range`] from [`FullRange`];
#[derive(From, Display, Error, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum F32RangeFromFullRangeError {
    U64(U64ToFiniteFloatError),
    F64(FiniteF64toF32Error),
}

/// Error when attempting to insert new [`TextRange`] with a series into a layout.
#[derive(Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(PyErr: From<E>))]
pub enum InsertRangeAndSeriesError<E> {
    Range(E),
    Series(CastSeriesError),
}

impl_kind1!(pub InsertRangeAndSeriesErrorFamily, InsertRangeAndSeriesError);

impl_functor_once!(
    InsertRangeAndSeriesError,
    self,
    mut f,
    match self {
        Self::Range(x) => InsertRangeAndSeriesError::Range(f(x)),
        Self::Series(x) => InsertRangeAndSeriesError::Series(x),
    }
);

/// Error when insert range with concrete type which mismatches layout.
#[derive(Debug, Error)]
// TODO make this say something useful
#[error("range type mistmatches")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::InvalidKeywordValueError))]
pub struct MismatchTypeRangeError;

/// Inner error for converting [`TextRange`] to [`Bitmask`]
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

/// Error when measurement vector is not the same length as columns in DATA/dataframe
#[derive(Debug, Error)]
#[error("measurement number ({meas_n}) does not match dataframe column number ({data_n})")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct MeasDataMismatchError {
    meas_n: usize,
    data_n: usize,
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

pub type ScaleErrorGroup<V> = ErrorGroup<
    <<<V as VersionSet>::Optical as AsScaleOrTransform>::S as CheckedScaleTransform>::Err,
    <<<V as VersionSet>::Optical as AsScaleOrTransform>::S as CheckedScaleTransform>::Summary,
>;

pub type ScaleMismatchErrors = ErrorGroup<ScaleMismatchError, ScaleMismatchSummary>;

def_summary!(
    pub ScaleMismatchSummary,
    "mismatch between scale and column datatypes"
);

pub type ScaleTransformMismatchErrors =
    ErrorGroup<ScaleTransformMismatchError, ScaleTransformMismatchSummary>;

def_summary!(
    pub ScaleTransformMismatchSummary,
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

/// Error when converting a primitive dataframe into a versioned dataframe
#[derive(From, Error, Display, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum DataSchemaToDataFrameError {
    ColMismatch(MeasDataMismatchError),
    Cast(CastSeriesErrors),
}

// TODO which columns?
def_summary!(
    pub CastSeriesSummary,
    "one or more series could not be cast into correct type"
);

pub type CastSeriesErrors = ErrorGroup<CastSeriesError, CastSeriesSummary>;

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
/// We cannot use [`Series`] for these use cases since making a new
/// dataframe involves making a new buffer with the correct number of rows and
/// filling with 0's; the 0's are then mutated in place. This cannot happen in
/// an [`Series`] since the underlying storage is a polars buffer which
/// is harder to mutate in place.
#[derive(new)]
pub(crate) struct RangedVec<B, T> {
    pub(crate) range: B,
    pub(crate) data: Vec<T>,
}

type NativeRangedVec<C> = RangedVec<C, <C as ColumnHasNativeType>::Native>;

pub(crate) type AnyUintVec = AnyUint<
    NativeRangedVec<Bitmask08>,
    NativeRangedVec<Bitmask16>,
    NativeRangedVec<Bitmask24>,
    NativeRangedVec<Bitmask32>,
    NativeRangedVec<Bitmask40>,
    NativeRangedVec<Bitmask48>,
    NativeRangedVec<Bitmask56>,
    NativeRangedVec<Bitmask64>,
>;

pub(crate) type MixedVec = AnyDatatype<
    RangedVec<FixedAsciiRange, u64>,
    AnyUintVec,
    RangedVec<F32Range, f32>,
    RangedVec<F64Range, f64>,
>;

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

#[macro_export]
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

#[macro_export]
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

#[macro_export]
macro_rules! match_any_ascii {
    ($value:expr, $inner:ident, $action:expr) => {
        match_many_to_one!($value, AnyAscii, [Delimited, Fixed], $inner, $action)
    };
}

#[macro_export]
macro_rules! match_any_endian_uint {
    ($value:expr, $inner:ident, $action:expr) => {
        match_many_to_one!($value, AnyBigLittleUint, [Single, Multi], $inner, $action)
    };
}

#[macro_export]
macro_rules! match_any_3_2 {
    ($value:expr, $inner:ident, $action:expr) => {
        match_many_to_one!($value, Any3_2, [Mixed, NonMixed], $inner, $action)
    };
}

#[macro_export]
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

#[macro_export]
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

#[macro_export]
macro_rules! match_map_ascii {
    ($value:expr, $inner:ident, $action:expr) => {
        match $value {
            AnyAscii::Delimited($inner) => AnyAscii::Delimited($action),
            AnyAscii::Fixed($inner) => AnyAscii::Fixed($action),
        }
    };
}

#[macro_export]
macro_rules! match_map_endian_uint {
    ($value:expr, $inner:ident, $action:expr) => {
        match $value {
            AnyBigLittleUint::Multi($inner) => AnyBigLittleUint::Multi($action),
            AnyBigLittleUint::Single($inner) => AnyBigLittleUint::Single($action),
        }
    };
}

// Implement version specific-operations for data schema.
//
// This is the main trait that public-facing APIs will use.

// TODO add a method/trait to convert DataSchema -> DataFrame via a primitive
// dataframe, and also check that this won't fail

/// A version-specific data layout with just data schema (no DATA).
pub trait VersionedDataSchema
where
    for<'a> Self: Sized
        + DataSchemaReadOps<Self::Tot>
        + LayoutDatatype
        + HasWidth
        + LayoutNormalize
        + LayoutKeywords
        + LayoutOptMeasKeywords
        + WithPrimitiveDataFrame,
{
    type ByteOrder;
    type NumType: IsNumType;
    type Tot: IsTot;

    fn lookup(
        std: &mut StdKeywords,
        meas_nonstd: &mut [NonStdKeywords],
        conf: &ReadDataKeywordsConfig,
    ) -> LookupLayoutResult<NewDataSchema<Self>>;

    fn lookup_ro(
        kws: &StdKeywords,
        par: Par,
        conf: &ReadDataKeywordsConfig,
    ) -> LookupLayoutResult<NewDataSchema<Self>>;

    fn new_empty(datatype: AlphaNumType) -> Self;

    fn try_new(
        datatype: AlphaNumType,
        byteord: Self::ByteOrder,
        columns: Vec<DataSchemaKeywordValues<Self::NumType>>,
        conf: &ReadDataKeywordsConfig,
    ) -> WarningsAndErrorsResult<NewDataSchema<Self>, (), NewMixedRangeWarning, NewDataSchemaError>;

    fn h_read_df<R: Read + Seek>(
        &mut self,
        h: &mut BufReader<R>,
        tot: Self::Tot,
        seg: &mut AnyDataSegment,
        conf: &ReadEventsConfig,
    ) -> WarningsAndIOGroupResult<
        DataFrameResult<<Self as DataSchemaToEmptyDataFrame>::DfTarget>,
        ReadCheckedDataframeWarning,
        ReadCheckedDataframeError,
        (),
    >
    where
        <Self as DataSchemaToEmptyDataFrame>::DfTarget: DataFrameCheckRanges,
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
                        let trunc = conf.truncate_range_datatypes;
                        let flag = conf.disallow_over_range;
                        res.dataframe
                            .check_ranges_mut(trunc, flag)
                            .group()
                            .map_error(ReadCheckedDataframeError::from)
                            .map_error(IOErrorGroup::new_pure_one)
                            .map_commutative_warnings(ReadCheckedDataframeWarning::from)
                            .map_ok_value(|overrange| {
                                res.diagnostics.overrange_columns = overrange;
                                res
                            })
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
}

impl VersionedDataSchema for DataSchema2_0 {
    type ByteOrder = ByteOrd2_0;
    type NumType = Nothing<NumType>;
    type Tot = Option<Tot>;

    fn lookup(
        std: &mut StdKeywords,
        meas_nonstd: &mut [NonStdKeywords],
        conf: &ReadDataKeywordsConfig,
    ) -> LookupLayoutResult<NewDataSchema<Self>> {
        AnyOrderedDataSchema::lookup(std, meas_nonstd, conf)
            .map_ok_value(FunctorOnce::fmap_into_once)
    }

    fn lookup_ro(
        kws: &StdKeywords,
        par: Par,
        conf: &ReadDataKeywordsConfig,
    ) -> LookupLayoutResult<NewDataSchema<Self>> {
        AnyOrderedDataSchema::lookup_ro(kws, par, conf).map_ok_value(FunctorOnce::fmap_into_once)
    }

    fn new_empty(datatype: AlphaNumType) -> Self {
        AnyOrderedDataSchema::new_empty(datatype)
    }

    fn try_new(
        datatype: AlphaNumType,
        byteord: Self::ByteOrder,
        columns: Vec<DataSchemaKeywordValues<Self::NumType>>,
        conf: &ReadDataKeywordsConfig,
    ) -> WarningsAndErrorsResult<NewDataSchema<Self>, (), NewMixedRangeWarning, NewDataSchemaError>
    {
        AnyOrderedDataSchema::try_new(datatype, byteord, columns, conf)
            .map_ok_value(FunctorOnce::fmap_into_once)
    }
}

impl VersionedDataSchema for DataSchema3_0 {
    type ByteOrder = ByteOrd2_0;
    type NumType = Nothing<NumType>;
    type Tot = Identity<Tot>;

    fn lookup(
        std: &mut StdKeywords,
        meas_nonstd: &mut [NonStdKeywords],
        conf: &ReadDataKeywordsConfig,
    ) -> LookupLayoutResult<NewDataSchema<Self>> {
        AnyOrderedDataSchema::lookup(std, meas_nonstd, conf)
            .map_ok_value(FunctorOnce::fmap_into_once)
    }

    fn lookup_ro(
        kws: &StdKeywords,
        par: Par,
        conf: &ReadDataKeywordsConfig,
    ) -> LookupLayoutResult<NewDataSchema<Self>> {
        AnyOrderedDataSchema::lookup_ro(kws, par, conf).map_ok_value(FunctorOnce::fmap_into_once)
    }

    fn new_empty(datatype: AlphaNumType) -> Self {
        AnyOrderedDataSchema::new_empty(datatype)
    }

    fn try_new(
        datatype: AlphaNumType,
        byteord: Self::ByteOrder,
        columns: Vec<DataSchemaKeywordValues<Nothing<NumType>>>,
        conf: &ReadDataKeywordsConfig,
    ) -> WarningsAndErrorsResult<NewDataSchema<Self>, (), NewMixedRangeWarning, NewDataSchemaError>
    {
        AnyOrderedDataSchema::try_new(datatype, byteord, columns, conf)
            .map_ok_value(FunctorOnce::fmap_into_once)
    }
}

impl VersionedDataSchema for DataSchema3_1 {
    type ByteOrder = Endian;
    type NumType = Nothing<NumType>;
    type Tot = Identity<Tot>;

    fn lookup(
        std: &mut StdKeywords,
        meas_nonstd: &mut [NonStdKeywords],
        conf: &ReadDataKeywordsConfig,
    ) -> LookupLayoutResult<NewDataSchema<Self>> {
        NonMixedDataSchema::lookup(std, meas_nonstd, conf).map_ok_value(FunctorOnce::fmap_into_once)
    }

    fn lookup_ro(
        kws: &StdKeywords,
        par: Par,
        conf: &ReadDataKeywordsConfig,
    ) -> LookupLayoutResult<NewDataSchema<Self>> {
        NonMixedDataSchema::lookup_ro(kws, par, conf).map_ok_value(FunctorOnce::fmap_into_once)
    }

    fn new_empty(datatype: AlphaNumType) -> Self {
        NonMixedDataSchema::new_empty(datatype)
    }

    fn try_new(
        datatype: AlphaNumType,
        byteord: Self::ByteOrder,
        columns: Vec<DataSchemaKeywordValues<Nothing<NumType>>>,
        conf: &ReadDataKeywordsConfig,
    ) -> WarningsAndErrorsResult<NewDataSchema<Self>, (), NewMixedRangeWarning, NewDataSchemaError>
    {
        NonMixedDataSchema::try_new(datatype, byteord, columns, conf)
            .map_ok_value(FunctorOnce::fmap_into_once)
    }
}

impl VersionedDataSchema for DataSchema3_2 {
    type ByteOrder = ByteOrd3_1;
    type NumType = Option<NumType>;
    type Tot = Identity<Tot>;

    fn lookup(
        std: &mut StdKeywords,
        meas_nonstd: &mut [NonStdKeywords],
        conf: &ReadDataKeywordsConfig,
    ) -> LookupLayoutResult<NewDataSchema<Self>> {
        let datatype = AlphaNumType::remove_metaroot_req(std);
        let endian = ByteOrd3_1::remove_metaroot_req(std);
        let columns = Option::lookup_all(std, meas_nonstd, conf);
        Self::lookup_inner(datatype, endian, columns, conf)
    }

    fn lookup_ro(
        kws: &StdKeywords,
        par: Par,
        conf: &ReadDataKeywordsConfig,
    ) -> LookupLayoutResult<NewDataSchema<Self>> {
        let datatype = AlphaNumType::get_metaroot_req(kws);
        let endian = ByteOrd3_1::get_metaroot_req(kws);
        let columns = Option::<NumType>::lookup_ro_all(kws, par, conf);
        Self::lookup_inner(datatype, endian, columns, conf)
    }

    fn new_empty(datatype: AlphaNumType) -> Self {
        NonMixedDataSchema::new_empty(datatype).into()
    }

    fn try_new(
        datatype: AlphaNumType,
        byteord: Self::ByteOrder,
        columns: Vec<DataSchemaKeywordValues<Option<NumType>>>,
        conf: &ReadDataKeywordsConfig,
    ) -> WarningsAndErrorsResult<NewDataSchema<Self>, (), NewMixedRangeWarning, NewDataSchemaError>
    {
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
                let l = NonMixedDataSchema::new_empty1(datatype, byteord.0).into();
                LogResult::new_ok(NewDataSchema::new(l, vec![]))
            }
            // has columns with one datatype, use nonmixed layout
            [dt] => {
                let ds = columns
                    .fmap(|c| DataSchemaKeywordValues::new(c.width, c.range, Nothing::default()));
                NonMixedDataSchema::try_new(dt, byteord.0, ds, conf).map_ok_value(
                    |x: NewDataSchema<_>| {
                        x.fmap_once(|y: NonMixedDataSchema<_>| Self::NonMixed(y.phantom_into()))
                    },
                )
            }
            // has columns with 1+ datatypes, use mixed layout
            _ => {
                let go = |i: MeasIndex, c: DataSchemaKeywordValues3_2| {
                    AnyDatatype::from_width_and_range(
                        c.width, c.range, c.datatype, datatype, i, notrunc,
                    )
                };
                Layout::try_new(columns, byteord.0, go)
                    .map_errors(NewDataSchemaError::from)
                    .map_ok_value(FunctorOnce::fmap_into_once)
            }
        }
    }
}

// Implement version specific ops for dataframes

/// A version-specific dataframe (data schema + DATA)
pub trait VersionedDataFrame
where
    for<'a> Self: Sized
        + DataFrameWriteOps
        + LayoutDatatype
        + HasWidth
        + LayoutHeight
        + LayoutSize
        + LayoutKeywords
        + LayoutOptMeasKeywords
        + LayoutNormalize
        + DataFrameCheckRanges
        + DataFrameAsDataSchema
        + WithPrimitiveDataFrame,
{
    fn h_write_df<W>(&self, h: &mut BufWriter<W>, conf: &WriteDatasetInnerConfig) -> io::Result<()>
    where
        W: Write,
    {
        // Layout should be normalized after updating or creating. This is not
        // critical to get correct since the layout can still be written but
        // performance might be slower, so just emit a warning.
        #[cfg(debug_assertions)]
        {
            if !self.is_normalized() {
                eprintln!("[WARN] layout is not normalized");
            }
        }
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

impl<C: HasWidth, F, I, L, M, const ORD: bool> HasWidth for Layout<C, F, I, L, M, ORD> {
    fn width(&self) -> usize {
        self.container.width()
    }

    fn clear(&mut self) {
        self.container.clear();
    }
}

/// A layout which has ranges.
#[delegatable_trait]
pub trait LayoutRanges<R>: Sized {
    fn ranges(&self) -> Vec<R>;
}

impl<R, C, F, I, L, M, const ORD: bool> LayoutRanges<R> for Layout<C, F, I, L, M, ORD>
where
    I: IsCol<F, ORD>,
    C: AsRef<[I::Inner]>,
    I::Inner: Into<R> + Clone,
{
    fn ranges(&self) -> Vec<R> {
        self.container
            .as_ref()
            .iter()
            .cloned()
            .map(Into::into)
            .collect()
    }
}

/// A layout which has one more datatypes.
#[delegatable_trait]
pub trait LayoutDatatype: Sized {
    fn datatype(&self) -> AlphaNumType;

    fn datatypes(&self) -> Vec<AlphaNumType>;

    // fn datatypes_and_width(&self) -> Vec<(AlphaNumType, Width)>;

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
        Self: HasWidth,
        G: Default,
        S: CheckedScaleTransform,
        ScaleDatatypeMismatchError: From<ErrorGroup<S::Err, G>>,
    {
        let meas_n = xforms.len();
        let layout_n = self.width();
        if meas_n != layout_n {
            let e = MeasLayoutLengthsError { meas_n, layout_n };
            return Err(e.into());
        }
        self.check_transforms(xforms)
            .map_err(ScaleDatatypeMismatchError::from)?;
        Ok(())
    }

    fn check_meas_vec<V: VersionSet>(
        &self,
        meas: &[TemporalOrOptical<V>],
    ) -> Result<(), MeasLayoutMismatchError>
    where
        Self: HasWidth,
        V::Optical: AsScaleOrTransform,
        <V::Optical as AsScaleOrTransform>::S: CheckedScaleTransform,
        <<V::Optical as AsScaleOrTransform>::S as CheckedScaleTransform>::Summary: Default,
        ScaleDatatypeMismatchError: From<
            ErrorGroup<
                <<V::Optical as AsScaleOrTransform>::S as CheckedScaleTransform>::Err,
                <<V::Optical as AsScaleOrTransform>::S as CheckedScaleTransform>::Summary,
            >,
        >,
    {
        let xforms: Vec<_> = meas
            .iter()
            .map(|m| {
                m.as_ref().both(
                    |_| <V::Optical as AsScaleOrTransform>::S::default(),
                    |r| r.specific.as_scale_or_transform(),
                )
            })
            .collect();
        self.check_transforms_and_len(&xforms[..])
    }

    fn check_meas_named_vec<Name, Tmp, Opt: AsScaleOrTransform>(
        &self,
        meas: &Measurements<Name, Tmp, Opt>,
    ) -> Result<(), MeasLayoutMismatchError>
    where
        Self: HasWidth,
        Opt::S: CheckedScaleTransform,
        <Opt::S as CheckedScaleTransform>::Summary: Default,
        ScaleDatatypeMismatchError: From<
            ErrorGroup<
                <Opt::S as CheckedScaleTransform>::Err,
                <Opt::S as CheckedScaleTransform>::Summary,
            >,
        >,
    {
        let xforms: Vec<_> = meas
            .iter_with(&|_, _| Opt::S::default(), &|_, m| {
                m.value.specific.as_scale_or_transform()
            })
            .collect();
        self.check_transforms_and_len(&xforms[..])
    }

    #[allow(clippy::type_complexity)]
    fn try_new_measurements<V: VersionSet>(
        &self,
        measurements: NamedTemporalsAndOpticals<V>,
    ) -> Result<Measurements<V::Name, V::Temporal, V::Optical>, MeasurementsWithLayoutError>
    where
        Self: HasWidth,
        V::Optical: AsScaleOrTransform,
        <V::Optical as AsScaleOrTransform>::S: CheckedScaleTransform,
        <<V::Optical as AsScaleOrTransform>::S as CheckedScaleTransform>::Summary: Default,
        ScaleDatatypeMismatchError: From<
            ErrorGroup<
                <<V::Optical as AsScaleOrTransform>::S as CheckedScaleTransform>::Err,
                <<V::Optical as AsScaleOrTransform>::S as CheckedScaleTransform>::Summary,
            >,
        >,
    {
        let ms = NamedVec::try_new(measurements)?;
        self.check_meas_named_vec(&ms)
            .map_err(MeasurementsWithLayoutError::from)?;
        Ok(ms)
    }
}

impl<C, F, I, L, M, const ORD: bool> LayoutDatatype for Layout<C, F, I, L, M, ORD>
where
    I: IsCol<F, ORD>,
    C: AsRef<[I::Inner]>,
    I::Inner: ColumnHasDatatype,
{
    fn datatype(&self) -> AlphaNumType {
        I::Inner::datatype_from_columns(self.container.as_ref())
    }

    fn datatypes(&self) -> Vec<AlphaNumType> {
        self.container
            .as_ref()
            .iter()
            .map(ColumnHasDatatype::col_datatype)
            .collect()
    }

    // fn datatypes_and_width(&self) -> Vec<(AlphaNumType, Width)> {
    //     self.container
    //         .as_ref()
    //         .iter()
    //         .map(|c| {
    //             (
    //                 ColumnHasDatatype::col_datatype(c),
    //                 ColumnSchemaAsWidth::as_width(c),
    //             )
    //         })
    //         .collect()
    // }
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

impl<C, F, I, L, M, const ORD: bool> LayoutKeywords for Layout<C, F, I, L, M, ORD>
where
    I: IsCol<F, ORD, Layout = L>,
    C: AsRef<[I::Inner]>,
    I::Inner: ColumnHasDatatype + ColumnSchemaAsWidth + Clone,
    L: Copy + HasByteOrd,
    for<'c> ReqRootKeyword<'c>: From<SplitKeyword0<L::ByteOrd>>,
    TextRange: From<I::Inner>,
{
    fn byteord_keyword(&self) -> ReqRootKeyword<'_> {
        ReqRootKeyword::from_value(self.byteord.into())
    }

    fn req_meas_keywords(&self) -> Vec<[ReqMeasKeyword<'_>; 2]> {
        self.container
            .as_ref()
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let w = ReqMeasKeyword::from_value(c.as_width(), i);
                let r = ReqMeasKeyword::from_value(TextRange::from(c.clone()), i);
                [w, r]
            })
            .collect()
    }
}

/// A layout which has a height.
#[delegatable_trait]
pub trait LayoutHeight: Sized {
    fn nrows(&self) -> usize;
}

impl<C, I, L, M, const ORD: bool> LayoutHeight
    for Layout<DataFrame<C>, DataFrameFamily, I, L, M, ORD>
{
    fn nrows(&self) -> usize {
        self.container.nrows()
    }
}

// Implement dataframe size method, used to get length of DATA segment.
//
// For fixed layouts, this is event width * number of rows.
//
// For delimited ASCII layouts, this is the number of chars required to
// write each value with one delimiter in between.

/// A layout which has a size in bytes.
#[delegatable_trait]
pub trait LayoutSize: Sized {
    fn nbytes(&self) -> u64;
}

impl<C, I, L, M, const ORD: bool> LayoutSize for Layout<DataFrame<C>, DataFrameFamily, I, L, M, ORD>
where
    C: ColumnIsFixed,
{
    fn nbytes(&self) -> u64 {
        usize_to_u64(self.event_width() * self.nrows())
    }
}

impl<I, L, M, const ORD: bool> LayoutSize
    for Layout<DataFrame<NativeSeries<DelimAsciiRange>>, DataFrameFamily, I, L, M, ORD>
{
    fn nbytes(&self) -> u64 {
        let n = self.nrows() * self.width();
        if n == 0 {
            return 0;
        }
        let ndelim = n - 1;
        let go = |col: &NativeSeries<DelimAsciiRange>| -> u64 {
            col.as_ref()
                .iter()
                .map(|&x| u64::from(u8::from(Chars::from_u64(x))))
                .sum()
        };
        let ndigits: u64 = self.container.as_ref().iter().map(go).sum();
        ndigits + usize_to_u64(ndelim)
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
pub trait LayoutOptMeasKeywords {
    /// Return vector of $PnDATATYPE.
    ///
    /// Vector length will equal DATA column number. `None` will be returned
    /// if $PnDATATYPE is not provided. For pre-3.2 layouts, all will be `None`.
    fn opt_meas_keywords(&self) -> Vec<Option<SplitKeyword1<NumType>>>;
}

impl<C, I, F, S, M, const ORD: bool> LayoutOptMeasKeywords for Layout<C, F, I, S, M, ORD>
where
    Self: HasWidth,
{
    fn opt_meas_keywords(&self) -> Vec<Option<SplitKeyword1<NumType>>> {
        vec![None; self.width()]
    }
}

impl<C, F, I, M, N> LayoutOptMeasKeywords for Any3_2<Layout<C, F, I, Endian, M, false>, N>
where
    I: IsCol<F, false>,
    C: AsRef<[I::Inner]>,
    I::Inner: ColumnHasDatatype,
    Self: LayoutDatatype,
    N: HasWidth,
{
    fn opt_meas_keywords(&self) -> Vec<Option<SplitKeyword1<NumType>>> {
        let dt = self.datatype();
        match self {
            Self::NonMixed(x) => vec![None; x.width()],
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

// Implement data schema -> empty dataframe conversion
//
// This can be easily delegated but is more complex than what ambassador can do
// since it requires an associated type to describe the target.

/// A data schema type that can be converted to an empty dataframe.
#[delegatable_trait]
pub trait DataSchemaToEmptyDataFrame {
    type DfTarget;

    fn empty(&self) -> Self::DfTarget;
}

impl DataSchemaToEmptyDataFrame for DataSchema3_2 {
    type DfTarget = DataFrame3_2;

    fn empty(&self) -> Self::DfTarget {
        match_any_3_2!(self, x, Self::DfTarget::from(x.empty()))
    }
}

impl<A, I, F32, F64> DataSchemaToEmptyDataFrame for AnyDatatype<A, I, F32, F64>
where
    A: DataSchemaToEmptyDataFrame,
    I: DataSchemaToEmptyDataFrame,
    F32: DataSchemaToEmptyDataFrame,
    F64: DataSchemaToEmptyDataFrame,
{
    type DfTarget = AnyDatatype<A::DfTarget, I::DfTarget, F32::DfTarget, F64::DfTarget>;

    fn empty(&self) -> Self::DfTarget {
        match_map_datatype!(self, x, x.empty())
    }
}

impl<W0, W> DataSchemaToEmptyDataFrame for AnyBigLittleUint<W0, W>
where
    W0: DataSchemaToEmptyDataFrame,
    W: DataSchemaToEmptyDataFrame,
{
    type DfTarget = AnyBigLittleUint<W0::DfTarget, W::DfTarget>;

    fn empty(&self) -> Self::DfTarget {
        match_map_endian_uint!(self, x, x.empty())
    }
}

impl<D, F> DataSchemaToEmptyDataFrame for AnyAscii<D, F>
where
    D: DataSchemaToEmptyDataFrame,
    F: DataSchemaToEmptyDataFrame,
{
    type DfTarget = AnyAscii<D::DfTarget, F::DfTarget>;

    fn empty(&self) -> Self::DfTarget {
        match_map_ascii!(self, x, x.empty())
    }
}

impl<C08, C16, C24, C32, C40, C48, C56, C64> DataSchemaToEmptyDataFrame
    for AnyUint<C08, C16, C24, C32, C40, C48, C56, C64>
where
    C08: DataSchemaToEmptyDataFrame,
    C16: DataSchemaToEmptyDataFrame,
    C24: DataSchemaToEmptyDataFrame,
    C32: DataSchemaToEmptyDataFrame,
    C40: DataSchemaToEmptyDataFrame,
    C48: DataSchemaToEmptyDataFrame,
    C56: DataSchemaToEmptyDataFrame,
    C64: DataSchemaToEmptyDataFrame,
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

impl<C, const ORD: bool, M> DataSchemaToEmptyDataFrame for DataSchema_<C, ORD, M>
where
    C: IsCol<VecFamily, ORD>
        + IsCol<DataFrameFamily, ORD, Layout = <C as IsCol<VecFamily, ORD>>::Layout>,
    <C as IsCol<VecFamily, ORD>>::Inner:
        DataSchemaToEmptySeries<Target = <C as IsCol<DataFrameFamily, ORD>>::Inner>,
    <C as IsCol<DataFrameFamily, ORD>>::Inner: HasLen,
    <C as IsCol<VecFamily, ORD>>::Layout: Clone,
{
    type DfTarget = DataFrame_<C, ORD, M>;

    fn empty(&self) -> Self::DfTarget {
        let cs = self.container.iter().map(DataSchemaToEmptySeries::empty);
        Layout::new(DataFrame::try_new(cs).unwrap(), self.byteord.clone())
    }
}

// Implement dataframe -> data schema conversion
//
// This can be easily delegated but is more complex than what ambassador can do
// since it requires an associated type to describe the target.

/// A data schema type that can be converted to an empty dataframe.
#[delegatable_trait]
pub trait DataFrameAsDataSchema {
    type DataSchema;

    fn as_data_schema(&self) -> Self::DataSchema;
}

impl DataFrameAsDataSchema for DataFrame3_2 {
    type DataSchema = DataSchema3_2;

    fn as_data_schema(&self) -> Self::DataSchema {
        match_any_3_2!(self, x, Self::DataSchema::from(x.as_data_schema()))
    }
}

impl<A, I, F32, F64> DataFrameAsDataSchema for AnyDatatype<A, I, F32, F64>
where
    A: DataFrameAsDataSchema,
    I: DataFrameAsDataSchema,
    F32: DataFrameAsDataSchema,
    F64: DataFrameAsDataSchema,
{
    type DataSchema = AnyDatatype<A::DataSchema, I::DataSchema, F32::DataSchema, F64::DataSchema>;

    fn as_data_schema(&self) -> Self::DataSchema {
        match_map_datatype!(self, x, x.as_data_schema())
    }
}

impl<W0, W> DataFrameAsDataSchema for AnyBigLittleUint<W0, W>
where
    W0: DataFrameAsDataSchema,
    W: DataFrameAsDataSchema,
{
    type DataSchema = AnyBigLittleUint<W0::DataSchema, W::DataSchema>;

    fn as_data_schema(&self) -> Self::DataSchema {
        match_map_endian_uint!(self, x, x.as_data_schema())
    }
}

impl<D, F> DataFrameAsDataSchema for AnyAscii<D, F>
where
    D: DataFrameAsDataSchema,
    F: DataFrameAsDataSchema,
{
    type DataSchema = AnyAscii<D::DataSchema, F::DataSchema>;

    fn as_data_schema(&self) -> Self::DataSchema {
        match_map_ascii!(self, x, x.as_data_schema())
    }
}

impl<C08, C16, C24, C32, C40, C48, C56, C64> DataFrameAsDataSchema
    for AnyUint<C08, C16, C24, C32, C40, C48, C56, C64>
where
    C08: DataFrameAsDataSchema,
    C16: DataFrameAsDataSchema,
    C24: DataFrameAsDataSchema,
    C32: DataFrameAsDataSchema,
    C40: DataFrameAsDataSchema,
    C48: DataFrameAsDataSchema,
    C56: DataFrameAsDataSchema,
    C64: DataFrameAsDataSchema,
{
    type DataSchema = AnyUint<
        C08::DataSchema,
        C16::DataSchema,
        C24::DataSchema,
        C32::DataSchema,
        C40::DataSchema,
        C48::DataSchema,
        C56::DataSchema,
        C64::DataSchema,
    >;

    fn as_data_schema(&self) -> Self::DataSchema {
        match_map_uint!(self, x, x.as_data_schema())
    }
}

impl<C, const ORD: bool, M> DataFrameAsDataSchema for DataFrame_<C, ORD, M>
where
    C: IsCol<DataFrameFamily, ORD>
        + IsCol<VecFamily, ORD, Layout = <C as IsCol<DataFrameFamily, ORD>>::Layout>,
    <C as IsCol<DataFrameFamily, ORD>>::Inner:
        SeriesAsColumnSchema<Target = <C as IsCol<VecFamily, ORD>>::Inner>,
    <C as IsCol<DataFrameFamily, ORD>>::Layout: Clone,
{
    type DataSchema = DataSchema_<C, ORD, M>;

    fn as_data_schema(&self) -> Self::DataSchema {
        let cs = self
            .container
            .iter()
            .map(SeriesAsColumnSchema::as_column_schema)
            .collect();
        Layout::new(cs, self.byteord.clone())
    }
}

// Implement method to set data to a dataframe

pub trait WithPrimitiveDataFrame {
    type DfTarget;

    fn with_data(
        &self,
        df: PrimitiveDataFrame,
    ) -> Result<Self::DfTarget, DataSchemaToDataFrameError>;

    fn with_data_generic<T>(&self, df: T) -> Result<Self::DfTarget, DataSchemaToDataFrameError>
    where
        T: Into<PrimitiveDataFrame>,
    {
        self.with_data(df.into())
    }

    fn check_width<T>(&self, df: &T) -> Result<(), MeasDataMismatchError>
    where
        Self: HasWidth,
        T: HasWidth,
    {
        let df_width = df.width();
        let this_width = self.width();
        if df_width != this_width {
            return Err(MeasDataMismatchError {
                meas_n: this_width,
                data_n: df_width,
            });
        }
        Ok(())
    }

    fn check_data_loss(&self, df: &PrimitiveDataFrame) -> Result<(), CastSeriesErrors>;

    // Check that generic dataframe won't cause data loss errors with a new
    // layout. Cloning is the easy way to do this and it is cheap because the
    // data itself is behind a reference counter. The only caveat is that we
    // need to ensure this is dropped, since any conversions that happen later
    // might be in place, and this will trigger a copy if there are any dangling
    // references.

    fn check_data_loss_generic<T>(&self, df: &T) -> Result<(), CastSeriesErrors>
    where
        T: Clone + Into<PrimitiveDataFrame>,
    {
        self.check_data_loss(&df.clone().into())
    }
}

impl WithPrimitiveDataFrame for DataSchema3_2 {
    type DfTarget = DataFrame3_2;

    fn with_data(
        &self,
        df: PrimitiveDataFrame,
    ) -> Result<Self::DfTarget, DataSchemaToDataFrameError> {
        match_any_3_2!(self, x, Ok(Self::DfTarget::from(x.with_data(df)?)))
    }

    fn check_data_loss(&self, df: &PrimitiveDataFrame) -> Result<(), CastSeriesErrors> {
        match_any_3_2!(self, x, x.check_data_loss(df))
    }
}

impl WithPrimitiveDataFrame for DataFrame3_2 {
    type DfTarget = Self;

    fn with_data(
        &self,
        df: PrimitiveDataFrame,
    ) -> Result<Self::DfTarget, DataSchemaToDataFrameError> {
        match_any_3_2!(self, x, Ok(Self::DfTarget::from(x.with_data(df)?)))
    }

    fn check_data_loss(&self, df: &PrimitiveDataFrame) -> Result<(), CastSeriesErrors> {
        match_any_3_2!(self, x, x.check_data_loss(df))
    }
}

impl<A, I, F32, F64> WithPrimitiveDataFrame for AnyDatatype<A, I, F32, F64>
where
    A: WithPrimitiveDataFrame,
    I: WithPrimitiveDataFrame,
    F32: WithPrimitiveDataFrame,
    F64: WithPrimitiveDataFrame,
{
    type DfTarget = AnyDatatype<A::DfTarget, I::DfTarget, F32::DfTarget, F64::DfTarget>;

    fn with_data(
        &self,
        df: PrimitiveDataFrame,
    ) -> Result<Self::DfTarget, DataSchemaToDataFrameError> {
        Ok(match_map_datatype!(self, x, x.with_data(df)?))
    }

    fn check_data_loss(&self, df: &PrimitiveDataFrame) -> Result<(), CastSeriesErrors> {
        match_any_datatype!(self, x, x.check_data_loss(df))
    }
}

impl<W0, W> WithPrimitiveDataFrame for AnyBigLittleUint<W0, W>
where
    W0: WithPrimitiveDataFrame,
    W: WithPrimitiveDataFrame,
{
    type DfTarget = AnyBigLittleUint<W0::DfTarget, W::DfTarget>;

    fn with_data(
        &self,
        df: PrimitiveDataFrame,
    ) -> Result<Self::DfTarget, DataSchemaToDataFrameError> {
        Ok(match_map_endian_uint!(self, x, x.with_data(df)?))
    }

    fn check_data_loss(&self, df: &PrimitiveDataFrame) -> Result<(), CastSeriesErrors> {
        match_any_endian_uint!(self, x, x.check_data_loss(df))
    }
}

impl<D, F> WithPrimitiveDataFrame for AnyAscii<D, F>
where
    D: WithPrimitiveDataFrame,
    F: WithPrimitiveDataFrame,
{
    type DfTarget = AnyAscii<D::DfTarget, F::DfTarget>;

    fn with_data(
        &self,
        df: PrimitiveDataFrame,
    ) -> Result<Self::DfTarget, DataSchemaToDataFrameError> {
        Ok(match_map_ascii!(self, x, x.with_data(df)?))
    }

    fn check_data_loss(&self, df: &PrimitiveDataFrame) -> Result<(), CastSeriesErrors> {
        match_any_ascii!(self, x, x.check_data_loss(df))
    }
}

impl<C08, C16, C24, C32, C40, C48, C56, C64> WithPrimitiveDataFrame
    for AnyUint<C08, C16, C24, C32, C40, C48, C56, C64>
where
    C08: WithPrimitiveDataFrame,
    C16: WithPrimitiveDataFrame,
    C24: WithPrimitiveDataFrame,
    C32: WithPrimitiveDataFrame,
    C40: WithPrimitiveDataFrame,
    C48: WithPrimitiveDataFrame,
    C56: WithPrimitiveDataFrame,
    C64: WithPrimitiveDataFrame,
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

    fn with_data(
        &self,
        df: PrimitiveDataFrame,
    ) -> Result<Self::DfTarget, DataSchemaToDataFrameError> {
        Ok(match_map_uint!(self, x, x.with_data(df)?))
    }

    fn check_data_loss(&self, df: &PrimitiveDataFrame) -> Result<(), CastSeriesErrors> {
        match_any_uint!(self, x, x.check_data_loss(df))
    }
}

impl<C, const ORD: bool, M> WithPrimitiveDataFrame for DataSchema_<C, ORD, M>
where
    Self: DataSchemaToEmptyDataFrame<DfTarget = DataFrame_<C, ORD, M>>,
    DataFrame_<C, ORD, M>: WithPrimitiveDataFrame<DfTarget = DataFrame_<C, ORD, M>>,
    C: IsCol<VecFamily, ORD>
        + IsCol<DataFrameFamily, ORD, Layout = <C as IsCol<VecFamily, ORD>>::Layout>,
{
    type DfTarget = DataFrame_<C, ORD, M>;

    fn with_data(
        &self,
        df: PrimitiveDataFrame,
    ) -> Result<Self::DfTarget, DataSchemaToDataFrameError> {
        self.empty().with_data(df)
    }

    fn check_data_loss(&self, df: &PrimitiveDataFrame) -> Result<(), CastSeriesErrors> {
        self.empty().check_data_loss(df)
    }
}

impl<C, const ORD: bool, M> WithPrimitiveDataFrame for DataFrame_<C, ORD, M>
where
    C: IsCol<DataFrameFamily, ORD>,
    <C as IsCol<DataFrameFamily, ORD>>::Inner: ColumnWithSeries + HasLen,
    <C as IsCol<DataFrameFamily, ORD>>::Layout: Clone,
{
    type DfTarget = Self;

    fn with_data(
        &self,
        df: PrimitiveDataFrame,
    ) -> Result<Self::DfTarget, DataSchemaToDataFrameError> {
        self.check_width(&df)?;
        let rs = self
            .container
            .iter()
            .zip(Vec::from(df))
            .map(|(h, c)| h.with_series(c));
        let new_cols = Result::sequence_results(rs).map_err(ErrorGroup::deanonymize)?;
        let new_df = DataFrame::try_new(new_cols).expect("number of columns was checked already");
        Ok(Layout::new(new_df, self.byteord.clone()))
    }

    fn check_data_loss(&self, df: &PrimitiveDataFrame) -> Result<(), CastSeriesErrors> {
        let es = self
            .container
            .iter()
            .zip(df.iter())
            .filter_map(|(h, c)| h.is_lossless(c).err());
        ErrorGroup::try_new(es)?;
        Ok(())
    }
}

// Implement data schema -> dataframe read traits
//
// For the base column layout, there are only two types of impls: fixed and
// delimited. Only delimited ASCII layouts use the latter.
//
// Aside from the base type, all enums and wrappers simply delegate downward to
// the base type and wrap the return type.

/// A data schema that can be converted to a dataframe by reading a bytestream.
#[delegatable_trait]
pub trait DataSchemaReadOps<T>: Sized + DataSchemaToEmptyDataFrame {
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

impl DataSchemaReadOps<Identity<Tot>> for DataSchema3_2 {
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

impl<A, I, F32, F64, TotType> DataSchemaReadOps<TotType> for AnyDatatype<A, I, F32, F64>
where
    A: DataSchemaReadOps<TotType>,
    I: DataSchemaReadOps<TotType>,
    F32: DataSchemaReadOps<TotType>,
    F64: DataSchemaReadOps<TotType>,
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

impl<W0, W> DataSchemaReadOps<Identity<Tot>> for AnyBigLittleUint<W0, W>
where
    W0: DataSchemaReadOps<Identity<Tot>>,
    W: DataSchemaReadOps<Identity<Tot>>,
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

impl<D, A, TotType> DataSchemaReadOps<TotType> for AnyAscii<A, D>
where
    D: DataSchemaReadOps<TotType>,
    A: DataSchemaReadOps<TotType>,
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

impl<C08, C16, C24, C32, C40, C48, C56, C64, TotType> DataSchemaReadOps<TotType>
    for AnyUint<C08, C16, C24, C32, C40, C48, C56, C64>
where
    C08: DataSchemaReadOps<TotType>,
    C16: DataSchemaReadOps<TotType>,
    C24: DataSchemaReadOps<TotType>,
    C32: DataSchemaReadOps<TotType>,
    C40: DataSchemaReadOps<TotType>,
    C48: DataSchemaReadOps<TotType>,
    C56: DataSchemaReadOps<TotType>,
    C64: DataSchemaReadOps<TotType>,
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

impl<Col, I, ByteOrder, const ORD: bool, TotType, Dtype> DataSchemaReadOps<TotType>
    for Layout<Vec<Col>, VecFamily, I, ByteOrder, ColumnMarkers<TotType, Dtype>, ORD>
where
    Self: DataSchemaReadFixed
        + DataSchemaToEmptyDataFrame<DfTarget = <Self as DataSchemaReadFixed>::DfTarget>,
    Dtype: IsNumType,
    Col: Clone + ColumnIsFixed,
    ByteOrder: Copy,
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

impl<const ORD: bool, TotType, Dtype> DataSchemaReadOps<TotType>
    for Layout<
        Vec<DelimAsciiRange>,
        VecFamily,
        DelimAsciiCol,
        NoByteOrd<ORD>,
        ColumnMarkers<TotType, Dtype>,
        ORD,
    >
where
    DelimAsciiCol: IsCol<VecFamily, ORD, Inner = DelimAsciiRange, Layout = NoByteOrd<ORD>>
        + IsCol<DataFrameFamily, ORD, Inner = NativeSeries<DelimAsciiRange>, Layout = NoByteOrd<ORD>>,
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
                    .map(InternalSeries::from)
                    .zip(&self.container)
                    .map(|(vec, &range)| NativeSeries::new(range, vec));
                let df = DataFrame::try_new(cs).unwrap();
                let out = EventsDiagnostics::new(None, None, None, vec![]);
                DataFrameResult::new(Layout::new_ascii(df), out)
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

/// A data schema type that can be converted to a dataframe by reading a bytestream.
#[delegatable_trait]
pub trait DataFrameWriteOps: Sized {
    fn h_write_df_inner<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        conf: &WriteDatasetInnerConfig,
    ) -> io::Result<()>;
}

impl<Col, I, L, const ORD: bool, TotType, Dtype> DataFrameWriteOps
    for Layout<DataFrame<Col>, DataFrameFamily, I, L, ColumnMarkers<TotType, Dtype>, ORD>
where
    Self: DataFrameWriteFixed,
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
impl<const ORD: bool, TotType, Dtype> DataFrameWriteOps
    for Layout<
        DataFrame<NativeSeries<DelimAsciiRange>>,
        DataFrameFamily,
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

// Implement low-level reading for data schema with fixed width.
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

/// Fixed data schema to be converted to a dataframe type via a bytestream.
///
/// Does not apply to delimited ASCII layouts since these do not have
/// predictable widths in each column.
///
/// This trait is meant to be specialized for different layouts in order to
/// make it fast.
///
/// NOTE: layouts are assumed to be normalized prior to calling this trait.
trait DataSchemaReadFixed {
    type DfTarget;

    fn h_read_fixed_df<R: Read>(
        &self,
        h: &mut BufReader<R>,
        nrows: usize,
        conf: &ReadEventsConfig,
    ) -> IOResult<Self::DfTarget, ReadDataframeError>;
}

// basic read loop for most use cases
impl<C, L, I, M, const ORD: bool> DataSchemaReadFixed for Layout<Vec<C>, VecFamily, I, L, M, ORD>
where
    L: ByteOrderIO<C> + Copy,
    C: ColumnHasNativeType + ColumnIsFixed + Clone,
    C::Native: FCSRepr,
    NativeInternalSeries<C>: From<Vec<C::Native>>,
{
    type DfTarget = Layout<DataFrame<NativeSeries<C>>, DataFrameFamily, I, L, M, ORD>;

    fn h_read_fixed_df<R: Read>(
        &self,
        h: &mut BufReader<R>,
        nrows: usize,
        conf: &ReadEventsConfig,
    ) -> IOResult<Self::DfTarget, ReadDataframeError> {
        let df = if let Some(mut row_buf) =
            ReadBuffer::init(conf.row_buffer_size, nrows, self.event_width())
        {
            let ncols = self.columns().len();
            let mut columns = vec![vec![C::Native::default(); nrows]; ncols];

            self.byteord.read_matrix(h, &mut row_buf, &mut columns)?;

            let data = columns
                .into_iter()
                .map(InternalSeries::from)
                .zip(self.container.iter().cloned())
                .map(|(data, range)| NativeSeries::new(range, data));
            DataFrame::try_new(data).expect("column lengths are the same")
        } else {
            DataFrame::default()
        };

        Ok(Layout::new(df, self.byteord))
    }
}

// ASCII-specific loop which involves possibility of failure after reading every
// value (slower, requires branching)
impl<M, const ORD: bool> DataSchemaReadFixed
    for Layout<Vec<FixedAsciiRange>, VecFamily, FixedAsciiCol, NoByteOrd<ORD>, M, ORD>
{
    type DfTarget = Layout<
        DataFrame<NativeSeries<FixedAsciiRange>>,
        DataFrameFamily,
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

        let df = if let Some(mut row_buf) = ReadBuffer::init(conf.row_buffer_size, nrows, row_width)
        {
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

            let data = columns.into_iter().map(NativeSeries::from);
            DataFrame::try_new(data).unwrap()
        } else {
            DataFrame::default()
        };
        Ok(Layout::new(df, self.byteord))
    }
}

// variable uint impl which is slower since it has branching to deal with
// multiple widths
impl<M> DataSchemaReadFixed for Layout<Vec<VariableBitmask>, VecFamily, UvarCol, Endian, M, false> {
    type DfTarget =
        Layout<DataFrame<VariableUintSeries>, DataFrameFamily, UvarCol, Endian, M, false>;

    fn h_read_fixed_df<R: Read>(
        &self,
        h: &mut BufReader<R>,
        nrows: usize,
        conf: &ReadEventsConfig,
    ) -> IOResult<Self::DfTarget, ReadDataframeError> {
        let df = if let Some(mut row_buf) =
            ReadBuffer::init(conf.row_buffer_size, nrows, self.event_width())
        {
            let mut columns: Vec<_> = self
                .container
                .iter()
                .map(|c| c.init_column(nrows))
                .collect();

            row_buf.read_any_uint_df(h, &mut columns, self.byteord)?;

            let data = columns.into_iter().map(VariableUintSeries::from);
            DataFrame::try_new(data).unwrap()
        } else {
            DataFrame::default()
        };

        Ok(Layout::new(df, self.byteord))
    }
}

// Loop for mixed layouts which will first try to read as one type and cast to
// others if there are multiple types of the same width (very fast since this
// reads a matrix) and falling back on a slower loop with branching to deal with
// multiple types/widths.
impl<M> DataSchemaReadFixed for Layout<Vec<MixedRange>, VecFamily, MixedCol, Endian, M, false> {
    type DfTarget = Layout<DataFrame<MixedSeries>, DataFrameFamily, MixedCol, Endian, M, false>;

    fn h_read_fixed_df<R: Read>(
        &self,
        h: &mut BufReader<R>,
        nrows: usize,
        conf: &ReadEventsConfig,
    ) -> IOResult<Self::DfTarget, ReadDataframeError> {
        let Some(mut buf) = ReadBuffer::init(conf.row_buffer_size, nrows, self.event_width())
        else {
            return Ok(Layout::new(DataFrame::default(), self.byteord));
        };
        let en = self.byteord;
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
            buf.read_mixed_df(h, &mut columns, self.byteord)
                .map_err(|e| {
                    e.fmap_once(ReadFixedAsciiError::from)
                        .fmap_once(ReadAsciiError::from)
                        .fmap_once(ReadDataframeError::from)
                })?;
            columns
        };

        let data = columns.into_iter().map(MixedSeries::from);
        let df = DataFrame::try_new(data).unwrap();

        Ok(Layout::new(df, self.byteord))
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
    Endian: ByteOrderIO<C>,
    W: TryFrom<MixedRange>,
    (Vec<C::Native>, W): Into<MixedVec>,
    C: ColumnHasNativeType,
    C::Native: Default + Clone + FCSRepr,
{
    if let Ok(cs) = ranges
        .iter()
        .copied()
        .map(W::try_from)
        .collect::<Result<Vec<_>, _>>()
    {
        let zero = <C as ColumnHasNativeType>::Native::default();
        let mut columns = vec![vec![zero; nrows]; cs.len()];
        ByteOrderIO::<C>::read_matrix(&endian, h, row_buf, &mut columns)?;
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

trait DataFrameWriteFixed {
    fn h_write_fixed_df<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        conf: &WriteDatasetInnerConfig,
    ) -> io::Result<()>;
}

// basic write loop for most use cases
impl<C, L, I, M, const ORD: bool> DataFrameWriteFixed
    for Layout<DataFrame<NativeSeries<C>>, DataFrameFamily, I, L, M, ORD>
where
    L: ByteOrderIO<C> + Copy,
    C: ColumnHasNativeType + ColumnIsBinary + Clone,
    C::Native: FCSRepr + PartialOrd,
    NativeSeries<C>: AsRef<[C::Native]>,
{
    fn h_write_fixed_df<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        conf: &WriteDatasetInnerConfig,
    ) -> io::Result<()> {
        let nrows = self.container.nrows();
        if let Some(mut row_buf) =
            WriteBuffer::init(conf.row_buffer_size, nrows, self.event_width())
        {
            let cols: Vec<_> = self.container.iter().map(AsRef::as_ref).collect();
            self.byteord.write_matrix(h, &mut row_buf, &cols[..])?;
        }
        Ok(())
    }
}

// ASCII-specific loop
impl<M, const ORD: bool> DataFrameWriteFixed
    for Layout<
        DataFrame<NativeSeries<FixedAsciiRange>>,
        DataFrameFamily,
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
        if let Some(mut row_buf) =
            WriteBuffer::init(conf.row_buffer_size, nrows, self.event_width())
        {
            let cols = self.container.as_ref();
            row_buf.write_char_matrix(h, cols)?;
        }
        Ok(())
    }
}

// variable uint-layout
impl<M> DataFrameWriteFixed
    for Layout<DataFrame<VariableUintSeries>, DataFrameFamily, UvarCol, Endian, M, false>
{
    fn h_write_fixed_df<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        conf: &WriteDatasetInnerConfig,
    ) -> io::Result<()> {
        let nrows = self.container.nrows();
        if let Some(mut row_buf) =
            WriteBuffer::init(conf.row_buffer_size, nrows, self.event_width())
        {
            let cols = self.container.as_ref();
            row_buf.write_any_uint_df(h, cols, self.byteord)?;
        }
        Ok(())
    }
}

// mixed type layout
impl<M> DataFrameWriteFixed
    for Layout<DataFrame<MixedSeries>, DataFrameFamily, MixedCol, Endian, M, false>
{
    fn h_write_fixed_df<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        conf: &WriteDatasetInnerConfig,
    ) -> io::Result<()> {
        let nrows = self.container.nrows();
        if let Some(mut buf) = WriteBuffer::init(conf.row_buffer_size, nrows, self.event_width()) {
            let en = self.byteord;
            let cols = self.container.as_ref();
            if !(try_write_single::<_, Any4ByteColumn, F32Range>(h, cols, en, &mut buf)?
                || try_write_single::<_, Any8ByteColumn, F64Range>(h, cols, en, &mut buf)?)
            {
                buf.write_mixed_df(h, cols, self.byteord)?;
            }
        }
        Ok(())
    }
}

type Any4ByteColumn = Any4ByteType<NativeSeries<F32Range>, NativeSeries<Bitmask32>>;

type Any8ByteColumn = Any8ByteType<NativeSeries<F64Range>, NativeSeries<Bitmask64>>;

macro_rules! impl_single_width_column {
    ($t:ident, $i:ident, $f:ident, $u:ident) => {
        impl TryFrom<MixedSeries> for $t {
            type Error = ();
            fn try_from(value: MixedSeries) -> Result<Self, Self::Error> {
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
    cols: &[MixedSeries],
    endian: Endian,
    write_buf: &mut WriteBuffer,
) -> io::Result<bool>
where
    W: Write,
    Endian: ByteOrderIO<C>,
    T: TryFrom<MixedSeries> + AsRef<[C::Native]>,
    C: ColumnHasNativeType,
    C::Native: FCSRepr,
{
    if let Ok(cs) = cols
        .iter()
        .cloned()
        .map(T::try_from)
        .collect::<Result<Vec<_>, _>>()
    {
        let columns: Vec<_> = cs.iter().map(AsRef::as_ref).collect();
        ByteOrderIO::<C>::write_matrix(&endian, h, write_buf, &columns[..])?;
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

impl<C, F, I, L, M, const ORD: bool> HasBinaryColumns for Layout<C, F, I, L, M, ORD>
where
    I: IsCol<F, ORD>,
    C: AsRef<[I::Inner]>,
    I::Inner: ColumnIsBinary,
{
    fn col_bytes(&self) -> Vec<PrivBytes> {
        self.container
            .as_ref()
            .iter()
            .map(ColumnIsBinary::bytes)
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
impl<F, T0, T1> ConvertFromLayout<AnyOrderedLayout<F, T0>> for AnyOrderedLayout<F, T1>
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
    fn convert_from_layout(value: AnyOrderedLayout<F, T0>) -> LayoutConvertResult<Self> {
        LogResult::new_ok(value.phantom_into())
    }
}

// this covers all 2.0/3.0 -> 3.1 conversions
impl<F, D, T> ConvertFromLayout<AnyOrderedLayout<F, T>> for NonMixedLayout<F, D>
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
    AnyAsciiLayout3_1<F, D>: ConvertFromLayout<AnyAsciiLayout2_0<F, T>>,
    BigLittleLayout<F, U08Col, D>: ConvertFromLayout<OrderedLayout<F, U08Col, T>>,
    BigLittleLayout<F, U16Col, D>: ConvertFromLayout<OrderedLayout<F, U16Col, T>>,
    BigLittleLayout<F, U24Col, D>: ConvertFromLayout<OrderedLayout<F, U24Col, T>>,
    BigLittleLayout<F, U32Col, D>: ConvertFromLayout<OrderedLayout<F, U32Col, T>>,
    BigLittleLayout<F, U40Col, D>: ConvertFromLayout<OrderedLayout<F, U40Col, T>>,
    BigLittleLayout<F, U48Col, D>: ConvertFromLayout<OrderedLayout<F, U48Col, T>>,
    BigLittleLayout<F, U56Col, D>: ConvertFromLayout<OrderedLayout<F, U56Col, T>>,
    BigLittleLayout<F, U64Col, D>: ConvertFromLayout<OrderedLayout<F, U64Col, T>>,
    BigLittleLayout<F, F32Col, D>: ConvertFromLayout<OrderedLayout<F, F32Col, T>>,
    BigLittleLayout<F, F64Col, D>: ConvertFromLayout<OrderedLayout<F, F64Col, T>>,
{
    fn convert_from_layout(value: AnyOrderedLayout<F, T>) -> LayoutConvertResult<Self> {
        match value {
            AnyDatatype::Ascii(x) => {
                AnyAsciiLayout::convert_from_layout(x).map_ok_value(Self::Ascii)
            }
            AnyDatatype::Uint(x) => {
                match_any_uint!(x, y, {
                    BigLittleLayout::convert_from_layout(y)
                        .map_ok_value(AnyUint::from)
                        .map_ok_value(AnyBigLittleUint::Single)
                        .map_ok_value(AnyDatatype::Uint)
                })
            }
            AnyDatatype::F32(x) => {
                BigLittleLayout::convert_from_layout(x).map_ok_value(AnyDatatype::F32)
            }
            AnyDatatype::F64(x) => {
                BigLittleLayout::convert_from_layout(x).map_ok_value(AnyDatatype::F64)
            }
        }
    }
}

// this covers all 3.1 -> 2.0/3.0 conversions
impl<F, D, T> ConvertFromLayout<NonMixedLayout<F, D>> for AnyOrderedLayout<F, T>
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
    AnyAsciiLayout2_0<F, T>: ConvertFromLayout<AnyAsciiLayout3_1<F, D>>,
    OrderedLayout<F, U08Col, T>: ConvertFromLayout<BigLittleLayout<F, U08Col, D>>,
    OrderedLayout<F, U16Col, T>: ConvertFromLayout<BigLittleLayout<F, U16Col, D>>,
    OrderedLayout<F, U24Col, T>: ConvertFromLayout<BigLittleLayout<F, U24Col, D>>,
    OrderedLayout<F, U32Col, T>: ConvertFromLayout<BigLittleLayout<F, U32Col, D>>,
    OrderedLayout<F, U40Col, T>: ConvertFromLayout<BigLittleLayout<F, U40Col, D>>,
    OrderedLayout<F, U48Col, T>: ConvertFromLayout<BigLittleLayout<F, U48Col, D>>,
    OrderedLayout<F, U56Col, T>: ConvertFromLayout<BigLittleLayout<F, U56Col, D>>,
    OrderedLayout<F, U64Col, T>: ConvertFromLayout<BigLittleLayout<F, U64Col, D>>,
    OrderedLayout<F, F32Col, T>: ConvertFromLayout<BigLittleLayout<F, F32Col, D>>,
    OrderedLayout<F, F64Col, T>: ConvertFromLayout<BigLittleLayout<F, F64Col, D>>,
    NonMixedLayout<F, D>: LayoutNormalize,
    F::Type<<UvarCol as IsCol<F, false>>::Inner>: AsRef<[<UvarCol as IsCol<F, false>>::Inner]>,
    <UvarCol as IsCol<F, false>>::Inner: ColumnIsFixed,
{
    fn convert_from_layout(mut value: NonMixedLayout<F, D>) -> LayoutConvertResult<Self> {
        value.normalize();
        match value {
            AnyDatatype::Ascii(x) => {
                AnyAsciiLayout::convert_from_layout(x).map_ok_value(Self::Ascii)
            }
            AnyDatatype::Uint(x) => match x {
                AnyBigLittleUint::Multi(y) => y.conversion_fail_by_width(),
                AnyBigLittleUint::Single(y) => {
                    match_any_uint!(y, z, {
                        OrderedLayout::convert_from_layout(z)
                            .map_ok_value(AnyUint::from)
                            .map_ok_value(Self::Uint)
                    })
                }
            },
            AnyDatatype::F32(x) => OrderedLayout::convert_from_layout(x).map_ok_value(Self::F32),
            AnyDatatype::F64(x) => OrderedLayout::convert_from_layout(x).map_ok_value(Self::F64),
        }
    }
}

// this covers all x.y -> 3.2 conversions
impl<F, A, I, F32, F64> ConvertFromLayout<AnyDatatype<A, I, F32, F64>> for Any3_2Layout<F>
where
    NonMixedLayout<F, Option<NumType>>: ConvertFromLayout<AnyDatatype<A, I, F32, F64>>,
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
        NonMixedLayout::convert_from_layout(value).map_ok_value(Self::NonMixed)
    }
}

// this covers all 3.2 -> x.y conversions
impl<F, A, I, F32, F64> ConvertFromLayout<Any3_2Layout<F>> for AnyDatatype<A, I, F32, F64>
where
    Self: ConvertFromLayout<NonMixedLayout<F, Option<NumType>>>,
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
    Any3_2Layout<F>: LayoutNormalize,
    MixedLayout<F>: LayoutDatatype,
{
    fn convert_from_layout(mut value: Any3_2Layout<F>) -> LayoutConvertResult<Self> {
        value.normalize();
        match value {
            Any3_2::NonMixed(x) => Self::convert_from_layout(x),
            Any3_2::Mixed(x) => x.conversion_fail_by_datatype(),
        }
    }
}

// used for 3.1 <-> 3.2
impl<F, D0, D1> ConvertFromLayout<NonMixedLayout<F, D0>> for NonMixedLayout<F, D1>
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
    fn convert_from_layout(value: NonMixedLayout<F, D0>) -> LayoutConvertResult<Self> {
        LogResult::new_ok(value.phantom_into())
    }
}

// used for 2.0/3.0 -> 3.1/3.2
impl<F, I, D, T> ConvertFromLayout<OrderedLayout<F, I, T>> for BigLittleLayout<F, I, D>
where
    F: Kind1,
    I: IsCol<F, false> + IsCol<F, true, Inner = <I as IsCol<F, false>>::Inner>,
    <I as IsCol<F, true>>::Layout:
        TryInto<<I as IsCol<F, false>>::Layout, Error = OrderedToEndianError>,
{
    fn convert_from_layout(value: OrderedLayout<F, I, T>) -> LayoutConvertResult<Self> {
        match value.byte_layout_try_into() {
            Ok(x) => LogResult::new_ok(x.phantom_into()),
            Err(e) => LogResult::new_err(LayoutConvertError::OrderToEndian(e)),
        }
    }
}

// used for 3.1/3.2 -> 2.0/3.0
impl<F, I, D, T> ConvertFromLayout<BigLittleLayout<F, I, D>> for OrderedLayout<F, I, T>
where
    F: Kind1,
    I: IsCol<F, false> + IsCol<F, true, Inner = <I as IsCol<F, false>>::Inner>,
    <I as IsCol<F, false>>::Layout: Into<<I as IsCol<F, true>>::Layout>,
{
    fn convert_from_layout(value: BigLittleLayout<F, I, D>) -> LayoutConvertResult<Self> {
        LogResult::new_ok(value.byte_layout_into().phantom_into())
    }
}

// used for any 2.0 <-> 3.0 <-> 3.1 <-> 3.2
impl<F, const ORD1: bool, const ORD2: bool, M1, M2> ConvertFromLayout<AnyAsciiLayout<F, ORD1, M1>>
    for AnyAsciiLayout<F, ORD2, M2>
where
    F: Kind1,
    FixedAsciiCol:
        IsCol<F, ORD1> + IsCol<F, ORD2, Inner = <FixedAsciiCol as IsCol<F, ORD1>>::Inner>,
    DelimAsciiCol:
        IsCol<F, ORD1> + IsCol<F, ORD2, Inner = <DelimAsciiCol as IsCol<F, ORD1>>::Inner>,
    <FixedAsciiCol as IsCol<F, ORD1>>::Layout: Into<<FixedAsciiCol as IsCol<F, ORD2>>::Layout>,
    <DelimAsciiCol as IsCol<F, ORD1>>::Layout: Into<<DelimAsciiCol as IsCol<F, ORD2>>::Layout>,
{
    fn convert_from_layout(value: AnyAsciiLayout<F, ORD1, M1>) -> LayoutConvertResult<Self> {
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

impl<W0, W> PhantomInto for AnyBigLittleUint<W0, W>
where
    W0: PhantomInto,
    W: PhantomInto,
{
    type Target<Mf> = AnyBigLittleUint<W0::Target<Mf>, W::Target<Mf>>;

    fn phantom_into<Mf>(self) -> Self::Target<Mf> {
        match_map_endian_uint!(self, x, x.phantom_into())
    }
}

impl<C, F, I, T, M, const ORD: bool> PhantomInto for Layout<C, F, I, T, M, ORD> {
    type Target<Mf> = Layout<C, F, I, T, Mf, ORD>;

    fn phantom_into<Mf>(self) -> Self::Target<Mf> {
        Layout::new(self.container, self.byteord)
    }
}

// Implement insertion operations for layouts
//
// This is tricky for several reasons:
// 1. Inserting a mixed data schema can never fail but the rest can because of
//    type mismatches. Therefore we need a customizable error.
// 2. Inserting a mixed range/column must have an accompanying type with it
//    since otherwise it is unclear what the resulting type should be. For
//    all other layouts this is not a problem since the type is inherent to the
//    layout itself.
// 3. If we insert a column along with a range, we need to check the type
//    of the column as well.

/// A type which can accept a new column.
#[delegatable_trait]
pub trait LayoutInsert<Column>: LayoutNormalize {
    /// Error to emit if new column is not compatible with existing columns.
    type Error;

    /// Insert a new column at index.
    ///
    /// This will panic if index is out of bounds.
    fn insert_nocheck(&mut self, index: MeasIndex, col: Column) -> Result<(), Self::Error> {
        self.insert_or_push(Some(index), col)?;
        // Normalization is only needed here is we have an empty layout of a
        // mixed type; inserting one column by definition will have a single
        // type which is less complex than the initial mixed layout.
        self.normalize();
        Ok(())
    }

    /// Push new column to the right of the current column vector.
    fn push(&mut self, col: Column) -> Result<(), Self::Error> {
        self.insert_or_push(None, col)?;
        self.normalize();
        Ok(())
    }

    fn insert_or_push(&mut self, index: Option<MeasIndex>, col: Column) -> Result<(), Self::Error>;
}

impl<A, I, F32, F64> LayoutInsert<FullRange> for AnyDatatype<A, I, F32, F64>
where
    A: LayoutInsert<FullRange>,
    I: LayoutInsert<FullRange>,
    F32: LayoutInsert<FullRange>,
    F64: LayoutInsert<FullRange>,
    InsertFullRangeError: From<A::Error> + From<I::Error> + From<F32::Error> + From<F64::Error>,
{
    type Error = InsertFullRangeError;

    fn insert_or_push(
        &mut self,
        index: Option<MeasIndex>,
        col: FullRange,
    ) -> Result<(), Self::Error> {
        match_any_datatype!(self, x, {
            x.insert_or_push(index, col).map_err(Self::Error::from)
        })
    }
}

// Insert general range into potentially variable int layout. If single-width,
// coerce range to the width of the layout. If variable, fail instantly since
// width of the new range is ambiguous.
impl<D> LayoutInsert<FullRange> for AnyBigLittleUintDataSchema<D> {
    type Error = InsertFullRangeError;

    fn insert_or_push(
        &mut self,
        index: Option<MeasIndex>,
        col: FullRange,
    ) -> Result<(), Self::Error> {
        match self {
            Self::Single(x) => {
                x.insert_or_push(index, col)?;
                Ok(())
            }
            Self::Multi(_) => Err(MismatchTypeRangeError.into()),
        }
    }
}

// Insert specific bitmask into potentially variable int layout. If
// single-width, convert to multi before inserting.
impl<D> LayoutInsert<VariableBitmask> for AnyBigLittleUintDataSchema<D> {
    type Error = Infallible;

    fn insert_or_push(
        &mut self,
        index: Option<MeasIndex>,
        col: VariableBitmask,
    ) -> Result<(), Self::Error> {
        match self {
            Self::Single(x) => {
                match_any_uint!(x, y, {
                    if let Ok(r) = col.try_into() {
                        if let Some(i) = index {
                            y.container.insert(i.into(), r);
                        } else {
                            y.container.push(r);
                        }
                    } else {
                        let mut new = mem::take(y).map_inner(VariableBitmask::from);
                        let Ok(()) = new.insert_or_push(index, col);
                        *self = Self::Multi(new);
                    }
                    Ok(())
                })
            }
            Self::Multi(x) => x.insert_or_push(index, col),
        }
    }
}

// Insert general or specific range into variable int layout. The range can
// either be a general decimal or a specific bitmask type which implies the
// width of the new column.
impl<D> LayoutInsert<MaybeTypedVariableBitmask> for AnyBigLittleUintDataSchema<D> {
    type Error = InsertFullRangeError;

    fn insert_or_push(
        &mut self,
        index: Option<MeasIndex>,
        col: MaybeTypedVariableBitmask,
    ) -> Result<(), Self::Error> {
        match col {
            MaybeTypedRange::Untyped(r) => self.insert_or_push(index, r)?,
            MaybeTypedRange::Typed(r) => {
                let Ok(()) = self.insert_or_push(index, r);
            }
        }
        Ok(())
    }
}

impl<D> LayoutInsert<MaybeTypedVariableBitmask> for NonMixedDataSchema<D> {
    type Error = InsertFullRangeError;

    fn insert_or_push(
        &mut self,
        index: Option<MeasIndex>,
        col: MaybeTypedVariableBitmask,
    ) -> Result<(), Self::Error> {
        macro_rules! go {
            ($layout:expr) => {
                if let MaybeTypedRange::Untyped(r) = col {
                    $layout.insert_or_push(index, r)?;
                } else {
                    return Err(MismatchTypeRangeError.into());
                }
            };
        }
        match self {
            Self::Ascii(x) => go!(x),
            Self::Uint(x) => x.insert_or_push(index, col)?,
            Self::F32(x) => go!(x),
            Self::F64(x) => go!(x),
        }
        Ok(())
    }
}

impl LayoutInsert<MaybeTypedMixedRange> for DataSchema3_2 {
    type Error = InsertFullRangeError;

    fn insert_or_push(
        &mut self,
        index: Option<MeasIndex>,
        col: MaybeTypedMixedRange,
    ) -> Result<(), Self::Error> {
        macro_rules! go_mixed {
            ($col:expr, $from:expr) => {{
                let mut new = Self::Mixed(
                    mem::take($from)
                        .map_inner(MixedRange::from)
                        .byte_layout_into(),
                );
                new.insert_or_push(index, $col)?;
                *self = new;
            }};
        }
        macro_rules! go {
            ($col:expr, $var:ident, $from:expr) => {
                if let AnyDatatype::$var(r) = $col {
                    if let Some(i) = index {
                        $from.container.insert(i.into(), r.into());
                    } else {
                        $from.container.push(r.into());
                    }
                } else {
                    go_mixed!(MaybeTypedRange::Typed($col), $from);
                }
            };
        }

        match col {
            MaybeTypedRange::Untyped(r) => match self {
                Self::Mixed(_) => return Err(MismatchTypeRangeError.into()),
                Self::NonMixed(x) => x.insert_or_push(index, r)?,
            },

            MaybeTypedRange::Typed(r) => match self {
                Self::Mixed(x) => {
                    let Ok(()) = x.insert_or_push(index, r);
                }
                Self::NonMixed(x) => match x {
                    AnyDatatype::Ascii(y) => match y {
                        AnyAscii::Delimited(z) => go!(r, Ascii, z),
                        AnyAscii::Fixed(z) => go!(r, Ascii, z),
                    },
                    AnyDatatype::Uint(y) => match y {
                        AnyBigLittleUint::Single(z) => {
                            if let AnyDatatype::Uint(rr) = r {
                                let Ok(()) = y.insert_or_push(index, rr);
                            } else {
                                match_any_uint!(z, s, go_mixed!(MaybeTypedRange::Typed(r), s));
                            }
                        }
                        AnyBigLittleUint::Multi(z) => go!(r, Uint, z),
                    },
                    AnyDatatype::F32(y) => go!(r, F32, y),
                    AnyDatatype::F64(y) => go!(r, F64, y),
                },
            },
        }
        Ok(())
    }
}

impl<R: TryInto<C>, C, I, L, M, const ORD: bool> LayoutInsert<R>
    for Layout<Vec<C>, VecFamily, I, L, M, ORD>
{
    type Error = R::Error;

    fn insert_or_push(&mut self, index: Option<MeasIndex>, col: R) -> Result<(), Self::Error> {
        let r: C = col.try_into()?;
        if let Some(i) = index {
            self.container.insert(i.into(), r);
        } else {
            self.container.push(r);
        }
        Ok(())
    }
}

impl<A, I, F32, F64, Ae, Ie, F32e, F64e> LayoutInsert<DecimalRangeAndSeries>
    for AnyDatatype<A, I, F32, F64>
where
    A: LayoutInsert<DecimalRangeAndSeries, Error = InsertRangeAndSeriesError<Ae>>,
    I: LayoutInsert<DecimalRangeAndSeries, Error = InsertRangeAndSeriesError<Ie>>,
    F32: LayoutInsert<DecimalRangeAndSeries, Error = InsertRangeAndSeriesError<F32e>>,
    F64: LayoutInsert<DecimalRangeAndSeries, Error = InsertRangeAndSeriesError<F64e>>,
    InsertFullRangeError: From<Ae> + From<Ie> + From<F32e> + From<F64e>,
{
    type Error = InsertRangeAndSeriesError<InsertFullRangeError>;

    fn insert_or_push(
        &mut self,
        index: Option<MeasIndex>,
        col: DecimalRangeAndSeries,
    ) -> Result<(), Self::Error> {
        match_any_datatype!(self, x, {
            x.insert_or_push(index, col)
                .map_err(|e| e.fmap_once(InsertFullRangeError::from))
        })
    }
}

impl<D> LayoutInsert<DecimalRangeAndSeries> for AnyBigLittleUintDataFrame<D> {
    type Error = InsertRangeAndSeriesError<InsertFullRangeError>;

    fn insert_or_push(
        &mut self,
        index: Option<MeasIndex>,
        col: DecimalRangeAndSeries,
    ) -> Result<(), Self::Error> {
        match self {
            Self::Single(x) => {
                x.insert_or_push(index, col)
                    .map_err(FunctorOnce::fmap_into_once)?;
                Ok(())
            }
            Self::Multi(_) => Err(InsertRangeAndSeriesError::Range(
                MismatchTypeRangeError.into(),
            )),
        }
    }
}

impl<D> LayoutInsert<VariableUintSeries> for AnyBigLittleUintDataFrame<D> {
    type Error = Infallible;

    fn insert_or_push(
        &mut self,
        index: Option<MeasIndex>,
        col: VariableUintSeries,
    ) -> Result<(), Self::Error> {
        match self {
            Self::Single(x) => {
                match_any_uint!(x, y, {
                    if let Ok(r) = col.clone().try_into() {
                        if let Some(i) = index {
                            y.container.insert_series_nocheck(i.into(), r);
                        } else {
                            y.container.push_series_nocheck(r);
                        }
                    } else {
                        let mut new = mem::take(y).map_inner(VariableUintSeries::from);
                        let Ok(()) = new.insert_or_push(index, col);
                        *self = Self::Multi(new);
                    }
                    Ok(())
                })
            }
            Self::Multi(x) => x.insert_or_push(index, col),
        }
    }
}

impl<D> LayoutInsert<MaybeTypedVariableUintSeries> for AnyBigLittleUintDataFrame<D> {
    type Error = InsertRangeAndSeriesError<InsertFullRangeError>;

    fn insert_or_push(
        &mut self,
        index: Option<MeasIndex>,
        col: MaybeTypedVariableUintSeries,
    ) -> Result<(), Self::Error> {
        match col {
            MaybeTypedRange::Untyped(r) => self.insert_or_push(index, r)?,
            MaybeTypedRange::Typed(r) => {
                let Ok(()) = self.insert_or_push(index, r);
            }
        }
        Ok(())
    }
}

impl<D> LayoutInsert<MaybeTypedVariableUintSeries> for NonMixedDataFrame<D> {
    type Error = InsertRangeAndSeriesError<InsertFullRangeError>;

    fn insert_or_push(
        &mut self,
        index: Option<MeasIndex>,
        col: MaybeTypedVariableUintSeries,
    ) -> Result<(), Self::Error> {
        macro_rules! go {
            ($layout:expr) => {
                if let MaybeTypedRange::Untyped(r) = col {
                    $layout
                        .insert_or_push(index, r)
                        .map_err(InsertRangeAndSeriesError::fmap_into_once)?;
                } else {
                    return Err(InsertRangeAndSeriesError::Range(
                        MismatchTypeRangeError.into(),
                    ));
                }
            };
        }
        match self {
            Self::Ascii(x) => go!(x),
            Self::Uint(x) => x.insert_or_push(index, col)?,
            Self::F32(x) => go!(x),
            Self::F64(x) => go!(x),
        }
        Ok(())
    }
}

impl LayoutInsert<MaybeTypedMixedSeries> for DataFrame3_2 {
    type Error = InsertRangeAndSeriesError<InsertFullRangeError>;

    fn insert_or_push(
        &mut self,
        index: Option<MeasIndex>,
        col: MaybeTypedMixedSeries,
    ) -> Result<(), Self::Error> {
        macro_rules! go_mixed {
            ($col:expr, $from:expr) => {{
                let mut new = Self::Mixed(
                    mem::take($from)
                        .map_inner(MixedSeries::from)
                        .byte_layout_into(),
                );
                new.insert_or_push(index, $col)?;
                *self = new;
            }};
        }
        macro_rules! go {
            ($col:expr, $var:ident, $from:expr) => {
                if let AnyDatatype::$var(r) = $col {
                    if let Some(i) = index {
                        $from.container.insert_series_nocheck(i.into(), r.into());
                    } else {
                        $from.container.push_series_nocheck(r.into());
                    }
                } else {
                    go_mixed!(MaybeTypedRange::Typed($col), $from);
                }
            };
        }

        match col {
            MaybeTypedRange::Untyped(r) => match self {
                Self::Mixed(_) => {
                    return Err(InsertRangeAndSeriesError::Range(
                        MismatchTypeRangeError.into(),
                    ));
                }
                Self::NonMixed(x) => x.insert_or_push(index, r)?,
            },

            MaybeTypedRange::Typed(r) => match self {
                Self::Mixed(x) => {
                    let Ok(()) = x.insert_or_push(index, r);
                }
                Self::NonMixed(x) => match x {
                    AnyDatatype::Ascii(y) => match y {
                        AnyAscii::Delimited(z) => go!(r, Ascii, z),
                        AnyAscii::Fixed(z) => go!(r, Ascii, z),
                    },
                    AnyDatatype::Uint(y) => match y {
                        AnyBigLittleUint::Single(z) => {
                            if let AnyDatatype::Uint(rr) = r {
                                let Ok(()) = y.insert_or_push(index, rr);
                            } else {
                                match_any_uint!(z, s, go_mixed!(MaybeTypedRange::Typed(r), s));
                            }
                        }
                        AnyBigLittleUint::Multi(z) => go!(r, Uint, z),
                    },
                    AnyDatatype::F32(y) => go!(r, F32, y),
                    AnyDatatype::F64(y) => go!(r, F64, y),
                },
            },
        }
        Ok(())
    }
}

// Insert range and column
//
// ASSUME length is correct for new column, caller must verify this
impl<R, H, T, Raw, I, L, M, const ORD: bool> LayoutInsert<RangeAndSeries<R>>
    for Layout<DataFrame<Series<H, T, Raw>>, DataFrameFamily, I, L, M, ORD>
where
    RangeAndSeries<R>: TryInto<Series<H, T, Raw>>,
{
    type Error = <RangeAndSeries<R> as TryInto<Series<H, T, Raw>>>::Error;

    fn insert_or_push(
        &mut self,
        index: Option<MeasIndex>,
        col: RangeAndSeries<R>,
    ) -> Result<(), Self::Error> {
        let c = col.try_into()?;
        if let Some(i) = index {
            self.container.insert_series_nocheck(i.into(), c);
        } else {
            self.container.push_series_nocheck(c);
        }
        Ok(())
    }
}

// Insert range and column (no fail version)
//
// ASSUME length is correct for new column, caller must verify this
impl<C: HasLen, I, L, M, const ORD: bool> LayoutInsert<C>
    for Layout<DataFrame<C>, DataFrameFamily, I, L, M, ORD>
{
    type Error = Infallible;

    fn insert_or_push(&mut self, index: Option<MeasIndex>, col: C) -> Result<(), Self::Error> {
        if let Some(i) = index {
            self.container.insert_series_nocheck(i.into(), col);
        } else {
            self.container.push_series_nocheck(col);
        }
        Ok(())
    }
}

impl<R, H, T, Raw> TryFrom<RangeAndSeries<R>> for Series<H, T, Raw>
where
    R: TryInto<H>,
    InternalSeries<T, Raw>: FromSeries<AnyPrimitiveSeries>,
{
    type Error = InsertRangeAndSeriesError<R::Error>;

    fn try_from(value: RangeAndSeries<R>) -> Result<Self, Self::Error> {
        let (r, c) = value;
        let col_schema: H = r.try_into().map_err(InsertRangeAndSeriesError::Range)?;
        let data = InternalSeries::from_series(c)
            .into_result()
            .map_err(InsertRangeAndSeriesError::Series)?;
        Ok(Self::new(col_schema, data))
    }
}

// Implement removable operations for layouts.
//
// Unlike insertions, this cannot fail which makes this trait simpler.
// Also (for now) these only return a range (ie $PnR value).

// TODO return type of mixed type which was removed?

/// A type which can have a column removed from it.
#[delegatable_trait]
pub trait LayoutRemove<C>: Sized + LayoutNormalize {
    /// Remove a column.
    ///
    /// Will panic if index is out of bounds.
    fn remove_nocheck(&mut self, index: MeasIndex) -> C {
        let ret = self.remove_nocheck_inner(index);
        self.normalize();
        ret
    }

    fn remove_nocheck_inner(&mut self, index: MeasIndex) -> C;
}

impl<R, C, I, L, M, const ORD: bool> LayoutRemove<R> for Layout<Vec<C>, VecFamily, I, L, M, ORD>
where
    C: Into<R>,
{
    fn remove_nocheck_inner(&mut self, index: MeasIndex) -> R {
        debug_assert!(
            usize::from(index) <= self.container.len(),
            "Index should be less than/equal to column number"
        );
        self.container.remove(index.into()).into()
    }
}

impl<R, C, I, L, M, const ORD: bool> LayoutRemove<RangeAndSeries<R>>
    for Layout<DataFrame<C>, DataFrameFamily, I, L, M, ORD>
where
    C: Into<R> + Into<AnyPrimitiveSeries> + Clone,
{
    fn remove_nocheck_inner(&mut self, index: MeasIndex) -> RangeAndSeries<R> {
        debug_assert!(
            usize::from(index) <= self.container.ncols(),
            "Index should be less than/equal to column number"
        );
        let c = self.container.remove(index.into());
        // TODO clone shouldn't be necessary
        (c.clone().into(), c.into())
    }
}

// Implement byte layout access method
//
// Only used for non-ascii layouts, which also means these are not implemented
// for composite layouts that include ASCII.

// TODO these are only used for python interface

pub trait LayoutByteOrder {
    type ByteOrder;

    fn byte_order(&self) -> Self::ByteOrder;
}

impl<T> LayoutByteOrder for AnyOrderedUintDataSchema<T> {
    type ByteOrder = ByteOrd2_0;

    fn byte_order(&self) -> Self::ByteOrder {
        match self {
            AnyUint::Uint08(x) => ByteOrd2_0::O1(x.byte_order()),
            AnyUint::Uint16(x) => ByteOrd2_0::O2(x.byte_order()),
            AnyUint::Uint24(x) => ByteOrd2_0::O3(x.byte_order()),
            AnyUint::Uint32(x) => ByteOrd2_0::O4(x.byte_order()),
            AnyUint::Uint40(x) => ByteOrd2_0::O5(x.byte_order()),
            AnyUint::Uint48(x) => ByteOrd2_0::O6(x.byte_order()),
            AnyUint::Uint56(x) => ByteOrd2_0::O7(x.byte_order()),
            AnyUint::Uint64(x) => ByteOrd2_0::O8(x.byte_order()),
        }
    }
}

impl<D> LayoutByteOrder for AnySingleUintDataSchema<D> {
    type ByteOrder = Endian;

    fn byte_order(&self) -> Self::ByteOrder {
        match_any_uint!(self, x, x.byte_order())
    }
}

impl<C, F, I, L, M, const ORD: bool> LayoutByteOrder for Layout<C, F, I, L, M, ORD>
where
    L: Clone,
{
    type ByteOrder = L;

    fn byte_order(&self) -> L {
        self.byteord.clone()
    }
}

// Implement NormalizableLayout
//
// The concept of "normalization" exists to make conversion and performance
// optimization easier. By providing a type which represents the simpler case of
// a more general type, we can make specialized impls for these simpler types.
//
// Most layouts will noop since they only have one possibility. The only two
// exceptions are Endian Integer layouts which can have one or many widths
// (normalization will try to convert to explicit single layout) and mixed-type
// layouts (normalization will try to reduce to a single type, with the nuance
// that these also may have mixed width integer layouts inside them).
//
// This trait only applies to layouts complicated enough to contain multiple
// types which can be equivalent to each other.
//
// This is used in three places:
// 1. reading DATA
// 2. writing DATA
// 3. converting between layouts
//
// For 1. and 2. normalization increases performance. For 3. this makes certain
// conversions easier (ie if a complex layout cannot be normalized then it might
// not be downgradable). This is easy to apply for 1. and 3. since in both cases
// we will have an owned layout which means we can mutate it. For 2., we don't
// wish to mutably borrow the layout just so it can be normalized, so
// normalization is applied when modifying the layout in place and mutability is
// a given (ie remove, insert, push operations as well as setting a new layout
// from scratch which may not be normalized already). This is the only tricky
// place to apply normalization since each case must be normalized individually.

/// A layout that can be simplified into another layout of the same type.
#[delegatable_trait]
pub trait LayoutNormalize {
    fn is_normalized(&self) -> bool;

    fn normalize(&mut self);
}

// The base layout itself is trivial since it cannot be reduced to a simpler
// form and is thus always normalized.
impl<C, F, I, L, M, const ORD: bool> LayoutNormalize for Layout<C, F, I, L, M, ORD> {
    fn is_normalized(&self) -> bool {
        true
    }

    fn normalize(&mut self) {}
}

// Ditto ascii layouts
impl<D, F> LayoutNormalize for AnyAscii<D, F> {
    fn is_normalized(&self) -> bool {
        true
    }

    fn normalize(&mut self) {}
}

// Any layout that can hold multiple datatype defers to the integer type since
// this generic might be filled by a variable or single width layout which
// can be normalized. Ascii, F32, and F64 types are by definition irreducible
// and cannot be normalized, so noop if these are selected.
impl<A, I: LayoutNormalize, F32, F64> LayoutNormalize for AnyDatatype<A, I, F32, F64> {
    fn is_normalized(&self) -> bool {
        if let Self::Uint(x) = self {
            x.is_normalized()
        } else {
            true
        }
    }

    fn normalize(&mut self) {
        if let Self::Uint(x) = self {
            x.normalize();
        }
    }
}

// A variable width integer layout can be simplified to a single width layout
// if all columns have the same width.
impl<F, D> LayoutNormalize for AnyBigLittleUintLayout<F, D>
where
    Self: Default,
    F: Kind1,
    VariableUintLayout<F, D>: HasBinaryColumns,
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
    fn is_normalized(&self) -> bool {
        if let Self::Multi(x) = self {
            // if multi-width, layout is normalized if all columns are not
            // all the same size (which means it can't be simplified further)
            !x.col_bytes()
                .split_first()
                .is_some_and(|(c0, cs)| cs.iter().all(|c| c0 == c))
        } else {
            true
        }
    }

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
                    Self::Single(AnyUint::Uint32(Layout::default()))
                }
            }
        };
    }
}

// A mixed or non-mixed layout can be normalized if it is mixed and all types
// are the same. Note that this may also contain variable width integer layouts
// so the test for normalization needs to defer to this underlying layout when
// applicable.
impl<F> LayoutNormalize for Any3_2Layout<F>
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
    AnyBigLittleUintLayout<F, Option<NumType>>: LayoutNormalize,
    F::Type<<F32Col as IsCol<F, false>>::Inner>: Default,
    MixedLayout<F>: LayoutDatatype + AsRef<[<MixedCol as IsCol<F, false>>::Inner]>,
    F::Type<<MixedCol as IsCol<F, false>>::Inner>: Functor<<MixedCol as IsCol<F, false>>::Inner>,
    F::Type<<UvarCol as IsCol<F, false>>::Inner>: Functor<<UvarCol as IsCol<F, false>>::Inner>,
    <MixedCol as IsCol<F, false>>::Inner: ColInto<<F32Col as IsCol<F, false>>::Inner>
        + ColInto<<F64Col as IsCol<F, false>>::Inner>
        + ColInto<<UvarCol as IsCol<F, false>>::Inner>
        + ColInto<<FixedAsciiCol as IsCol<F, false>>::Inner>
        + ColumnSchemaAsWidth,
{
    fn is_normalized(&self) -> bool {
        match self {
            Self::NonMixed(x) => x.is_normalized(),
            Self::Mixed(x) => {
                if let Some((d0, ds)) = x.datatypes().split_first() {
                    // If Mixed, layout is normalized if all are not the same
                    // datatype, which means it cannot be simplified further.
                    // Note that we don't need to test the integer width, since
                    // it is possible to simplify a mixed layout even if the
                    // integer widths are different. This is further captured in
                    // the other match branch above.
                    !ds.iter().all(|d| d == d0)
                } else {
                    true
                }
            }
        }
    }

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
                                // make a multi-width layout first and then try
                                // to normalize further
                                let mut l = AnyBigLittleUint::Multi(x.map_inner(ColInto::col_into));
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
                    Self::NonMixed(AnyDatatype::F32(Layout::default()))
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
pub trait DataFrameCheckRanges {
    fn check_ranges_inner(&self, check: CheckedRangeDatatypes) -> Vec<TruncatedResult>;

    fn check_ranges_inner_mut(&mut self, trunc: CheckedRangeDatatypes) -> Vec<TruncatedResult>;

    fn check_ranges(
        &self,
        check: CheckedRangeDatatypes,
        disallow: DisallowOverRange,
    ) -> WarningsAndErrorsResult<Vec<OverrangeColumn>, (), EventOverRangeError, EventOverRangeError>
    {
        let rs = self.check_ranges_inner(check);
        let overrange = rs.iter().map(TruncatedResult::as_col).collect();
        let es = rs.into_iter().filter_map(TruncatedResult::into_err);
        SwitchableErrorsResult::new_deferred_switchable_iter3((), es, disallow)
            .switchable_into_commutative()
            .map_ok_value(|()| overrange)
    }

    fn check_ranges_mut(
        &mut self,
        trunc: CheckedRangeDatatypes,
        disallow: DisallowOverRange,
    ) -> WarningsAndErrorsResult<Vec<OverrangeColumn>, (), EventOverRangeError, EventOverRangeError>
    {
        let rs = self.check_ranges_inner_mut(trunc);
        let overrange = rs.iter().map(TruncatedResult::as_col).collect();
        let es = rs.into_iter().filter_map(TruncatedResult::into_err);
        SwitchableErrorsResult::new_deferred_switchable_iter3((), es, disallow)
            .switchable_into_commutative()
            .map_ok_value(|()| overrange)
    }
}

impl<C, I, L, M, const ORD: bool> DataFrameCheckRanges
    for Layout<DataFrame<C>, DataFrameFamily, I, L, M, ORD>
where
    C: CheckRange,
{
    fn check_ranges_inner(&self, check: CheckedRangeDatatypes) -> Vec<TruncatedResult> {
        self.container.check_ranges(check)
    }

    fn check_ranges_inner_mut(&mut self, trunc: CheckedRangeDatatypes) -> Vec<TruncatedResult> {
        self.container.check_ranges_mut(trunc)
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

impl<C, F, I, L, M, const ORD: bool> From<Layout<DataFrame<NativeSeries<C>>, F, I, L, M, ORD>>
    for PrimitiveDataFrame
where
    C: ColumnHasNativeType,
    NativeSeries<C>: Into<PrimitiveSeries<<C::Native as FCSRepr>::Prim>>,
    PrimitiveSeries<<C::Native as FCSRepr>::Prim>: Into<AnyPrimitiveSeries>,
    C::Native: FCSRepr,
{
    fn from(value: Layout<DataFrame<NativeSeries<C>>, F, I, L, M, ORD>) -> Self {
        value
            .container
            .fmap(|c| Into::<AnyPrimitiveSeries>::into(c.into()))
    }
}

// Implement column types and associated data
//
// Note that UvarCol and MixedCol only have ORD = false versions since these
// are only valid for 3.1+

/// A marker type to describe the contents of a layout.
///
/// This contains all information used to describe a column in either a data
/// schema or a dataframe. This allows easily describing and mapping between
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

        impl IsCol<DataFrameFamily, true> for $c {
            type Inner = NativeSeries<$inner>;
            type Layout = ArrayByteOrd<[u8; $n]>;
        }

        impl IsCol<VecFamily, false> for $c {
            type Inner = $inner;
            type Layout = Endian;
        }

        impl IsCol<DataFrameFamily, false> for $c {
            type Inner = NativeSeries<$inner>;
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

        impl IsCol<DataFrameFamily, true> for $c {
            type Inner = NativeSeries<$inner>;
            type Layout = NoByteOrd<true>;
        }

        impl IsCol<VecFamily, false> for $c {
            type Inner = $inner;
            type Layout = NoByteOrd<false>;
        }

        impl IsCol<DataFrameFamily, false> for $c {
            type Inner = NativeSeries<$inner>;
            type Layout = NoByteOrd<false>;
        }
    };
}

impl_ascii_column_type!(FixedAsciiCol, FixedAsciiRange);
impl_ascii_column_type!(DelimAsciiCol, DelimAsciiRange);

impl IsCol<VecFamily, false> for UvarCol {
    type Inner = VariableBitmask;
    type Layout = Endian;
}

impl IsCol<DataFrameFamily, false> for UvarCol {
    type Inner = VariableUintSeries;
    type Layout = Endian;
}

impl IsCol<VecFamily, false> for MixedCol {
    type Inner = MixedRange;
    type Layout = Endian;
}

impl IsCol<DataFrameFamily, false> for MixedCol {
    type Inner = MixedSeries;
    type Layout = Endian;
}

// Implement column schema -> empty series
//
// This is easy for column schema types that map to exactly one rust type.
//
// Variable Uint and PolyType are exceptions since they themselves need to map
// to another wrapper type.

/// A column schema which can be converted to an empty series.
trait DataSchemaToEmptySeries {
    type Target;

    fn empty(&self) -> Self::Target;
}

impl<T> DataSchemaToEmptySeries for T
where
    T: ColumnHasNativeType + Clone,
    T::Native: FCSRepr,
    AnyPrimitiveSeries: TryInto<NativeInternalSeries<T>, Error = CastSeriesError>,
{
    type Target = NativeSeries<T>;

    fn empty(&self) -> Self::Target {
        Series::empty(self.clone())
    }
}

impl DataSchemaToEmptySeries for VariableBitmask {
    type Target = VariableUintSeries;

    fn empty(&self) -> Self::Target {
        match_map_uint!(self, x, Series::empty(*x))
    }
}

impl DataSchemaToEmptySeries for MixedRange {
    type Target = MixedSeries;

    fn empty(&self) -> Self::Target {
        match_map_datatype!(self, x, x.empty())
    }
}

// Implement data column set method

trait ColumnWithSeries: Sized {
    fn with_series(&self, ser: AnyPrimitiveSeries) -> Result<Self, CastSeriesError>;

    fn is_lossless(&self, ser: &AnyPrimitiveSeries) -> Result<(), CastSeriesError>;
}

impl<T> ColumnWithSeries for NativeSeries<T>
where
    T: ColumnHasNativeType + Clone,
    T::Native: FCSRepr
        + FromValue<u8>
        + FromValue<u16>
        + FromValue<u32>
        + FromValue<u64>
        + FromValue<f32>
        + FromValue<f64>
        + HasFCSType,
    AnyPrimitiveSeries: TryInto<NativeInternalSeries<T>, Error = CastSeriesError>,
{
    fn with_series(&self, ser: AnyPrimitiveSeries) -> Result<Self, CastSeriesError> {
        Ok(Self::new(self.column_schema.clone(), ser.try_into()?))
    }

    fn is_lossless(&self, ser: &AnyPrimitiveSeries) -> Result<(), CastSeriesError> {
        ser.will_be_lossy::<T::Native>()
    }
}

impl ColumnWithSeries for VariableUintSeries {
    fn with_series(&self, ser: AnyPrimitiveSeries) -> Result<Self, CastSeriesError> {
        Ok(match_map_uint!(self, x, x.with_series(ser)?))
    }

    fn is_lossless(&self, ser: &AnyPrimitiveSeries) -> Result<(), CastSeriesError> {
        match_any_uint!(self, x, x.is_lossless(ser))
    }
}

impl ColumnWithSeries for MixedSeries {
    fn with_series(&self, ser: AnyPrimitiveSeries) -> Result<Self, CastSeriesError> {
        Ok(match_map_datatype!(self, x, x.with_series(ser)?))
    }

    fn is_lossless(&self, ser: &AnyPrimitiveSeries) -> Result<(), CastSeriesError> {
        match_any_datatype!(self, x, x.is_lossless(ser))
    }
}

// Implement series -> column schema
//
// This is easy for column schema types that map to exactly one rust type.
//
// Variable Uint and Mixed are exceptions since they themselves need to map
// to another wrapper type.

/// A series which has a data schema.
trait SeriesAsColumnSchema {
    type Target;

    fn as_column_schema(&self) -> Self::Target;
}

impl<T> SeriesAsColumnSchema for NativeSeries<T>
where
    T: ColumnHasNativeType + Clone,
    T::Native: FCSRepr,
{
    type Target = T;

    fn as_column_schema(&self) -> Self::Target {
        self.column_schema.clone()
    }
}

impl SeriesAsColumnSchema for VariableUintSeries {
    type Target = VariableBitmask;

    fn as_column_schema(&self) -> Self::Target {
        match_map_uint!(self, x, x.column_schema)
    }
}

impl SeriesAsColumnSchema for MixedSeries {
    type Target = MixedRange;

    fn as_column_schema(&self) -> Self::Target {
        match_map_datatype!(self, x, x.as_column_schema())
    }
}

// Implement byte width for column types which have a known width.
//
// This applies to all except ASCII types since it can only return up to 8 bytes

/// A column type which has a binary (ie not ASCII) representation.
#[delegatable_trait]
pub(crate) trait ColumnIsBinary: Sized {
    fn bytes(&self) -> PrivBytes;
}

impl<T> ColumnIsBinary for Bitmask<T>
where
    Self: ColumnHasNativeType<Native = T>,
    T: FCSRepr,
{
    fn bytes(&self) -> PrivBytes {
        T::FILE_BYTES.0
    }
}

impl<T> ColumnIsBinary for FloatRange<T>
where
    Self: ColumnHasNativeType<Native = T>,
    T: FCSRepr,
{
    fn bytes(&self) -> PrivBytes {
        T::FILE_BYTES.0
    }
}

impl<M: ColumnIsBinary, T, R> ColumnIsBinary for Series<M, T, R> {
    fn bytes(&self) -> PrivBytes {
        self.column_schema.bytes()
    }
}

impl<B: ColumnIsBinary, T> ColumnIsBinary for RangedVec<B, T> {
    fn bytes(&self) -> PrivBytes {
        self.range.bytes()
    }
}

// Implement fixed width for column types that have known width
//
// This applies to all except delim ASCII where $PnB is numeric and not '*'

/// A type which has a known width
#[delegatable_trait]
pub trait ColumnIsFixed {
    fn nbytes(&self) -> NonZeroU8;

    fn fixed_width(&self) -> BitsOrChars;
}

impl<T: ColumnIsBinary> ColumnIsFixed for T {
    fn nbytes(&self) -> NonZeroU8 {
        self.bytes().into()
    }

    fn fixed_width(&self) -> BitsOrChars {
        BitsOrChars(self.bytes().into())
    }
}

impl ColumnIsFixed for FixedAsciiRange {
    fn nbytes(&self) -> NonZeroU8 {
        self.chars().into()
    }

    fn fixed_width(&self) -> BitsOrChars {
        BitsOrChars(self.chars().into())
    }
}

impl ColumnIsFixed for NativeSeries<FixedAsciiRange> {
    fn nbytes(&self) -> NonZeroU8 {
        self.column_schema.nbytes()
    }

    fn fixed_width(&self) -> BitsOrChars {
        self.column_schema.fixed_width()
    }
}

// Implement native type for columns which map to exactly one Rust type
//
// Applies to all except compound types (ie mixed int width and mixed type)

/// A column which has exactly one native Rust type
pub trait ColumnHasNativeType: Sized {
    /// The native rust type
    type Native: Default + Copy;
}

macro_rules! def_native_wrapper {
    ($name:path, $native:ty) => {
        impl ColumnHasNativeType for $name {
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

impl<M: ColumnHasNativeType, T, R> ColumnHasNativeType for Series<M, T, R> {
    type Native = M::Native;
}

// Implement datatype for column types which correspond 1-1 with $DATATYPE

/// A column which has exactly one $DATATYPE value always always
trait ColumnHasOneDatatype: Sized {
    const DATATYPE: AlphaNumType;
}

impl ColumnHasOneDatatype for FixedAsciiRange {
    const DATATYPE: AlphaNumType = AlphaNumType::Ascii;
}

impl ColumnHasOneDatatype for DelimAsciiRange {
    const DATATYPE: AlphaNumType = AlphaNumType::Ascii;
}

impl<T> ColumnHasOneDatatype for Bitmask<T> {
    const DATATYPE: AlphaNumType = AlphaNumType::Integer;
}

impl ColumnHasOneDatatype for F32Range {
    const DATATYPE: AlphaNumType = AlphaNumType::Float;
}

impl ColumnHasOneDatatype for F64Range {
    const DATATYPE: AlphaNumType = AlphaNumType::Double;
}

impl<C08, C16, C24, C32, C40, C48, C56, C64> ColumnHasOneDatatype
    for AnyUint<C08, C16, C24, C32, C40, C48, C56, C64>
{
    const DATATYPE: AlphaNumType = AlphaNumType::Integer;
}

impl<M: ColumnHasOneDatatype, T, R> ColumnHasOneDatatype for Series<M, T, R> {
    const DATATYPE: AlphaNumType = M::DATATYPE;
}

// Implement datatype for columns which might map to more than one datatype

/// A column which has a $DATATYPE keyword
trait ColumnHasDatatype: Sized {
    fn col_datatype(&self) -> AlphaNumType;

    fn datatype_from_columns(cs: &[Self]) -> AlphaNumType;
}

impl<T: ColumnHasOneDatatype> ColumnHasDatatype for T {
    fn col_datatype(&self) -> AlphaNumType {
        T::DATATYPE
    }

    fn datatype_from_columns(_: &[Self]) -> AlphaNumType {
        T::DATATYPE
    }
}

impl<A, I, F32, F64> ColumnHasDatatype for AnyDatatype<A, I, F32, F64> {
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

// Implement real range (not $PnR) -> column

impl FullRange {
    fn as_u64(&self) -> Result<u64, FiniteF64> {
        match self {
            Self::Int(x) => Ok(x.0),
            Self::Float(x) => f64::from(*x).to_u64().ok_or(*x),
        }
    }
}

impl<T> TryFrom<FullIntRange> for Bitmask<T>
where
    u64: TryInto<Self>,
{
    type Error = <u64 as TryInto<Self>>::Error;

    fn try_from(value: FullIntRange) -> Result<Self, Self::Error> {
        value.0.try_into()
    }
}

impl<T> TryFrom<FullRange> for Bitmask<T>
where
    u64: TryInto<Self, Error = NewBitmaskError>,
{
    type Error = BitmaskFromFullRangeError;

    fn try_from(value: FullRange) -> Result<Self, Self::Error> {
        let x = match value.as_u64() {
            Ok(x) => x,
            Err(x) => return Err(BitmaskFromFullRangeError::Float(x)),
        };
        Ok(x.try_into()?)
    }
}

impl TryFrom<FullRange> for DelimAsciiRange {
    type Error = AsciiRangeValueFromFullRangeError;

    fn try_from(value: FullRange) -> Result<Self, Self::Error> {
        value
            .as_u64()
            .map_err(AsciiRangeValueFromFullRangeError)
            .map(AsciiRangeValue)
            .map(Self)
    }
}

impl TryFrom<FullRange> for FixedAsciiRange {
    type Error = AsciiRangeValueFromFullRangeError;

    fn try_from(value: FullRange) -> Result<Self, Self::Error> {
        value
            .as_u64()
            .map_err(AsciiRangeValueFromFullRangeError)
            .map(AsciiRangeValue)
            .map(Self::from)
    }
}

impl TryFrom<FullRange> for F32Range
where
    u64: TryInto<FiniteF64, Error = U64ToFiniteFloatError>,
    FiniteF64: TryInto<FiniteF32, Error = FiniteF64toF32Error>,
{
    type Error = F32RangeFromFullRangeError;

    fn try_from(value: FullRange) -> Result<Self, Self::Error> {
        let ret = match value {
            FullRange::Float(x) => x.try_into()?,
            FullRange::Int(x) => {
                let y: FiniteF64 = x.0.try_into()?;
                y.try_into()?
            }
        };
        Ok(Self::new(ret))
    }
}

impl TryFrom<FullRange> for F64Range {
    type Error = U64ToFiniteFloatError;

    fn try_from(value: FullRange) -> Result<Self, Self::Error> {
        match value {
            FullRange::Float(x) => Ok(Self::new(x)),
            FullRange::Int(x) => x.0.try_into().map(Self::new),
        }
    }
}

// Implement column -> real range (not $PnR)

impl<T> From<Bitmask<T>> for FullIntRange
where
    T: Into<u64>,
{
    fn from(value: Bitmask<T>) -> Self {
        Self(u64::from(value))
    }
}

impl From<VariableBitmask> for FullIntRange {
    fn from(value: VariableBitmask) -> Self {
        match_any_uint!(value, x, x.into())
    }
}

impl<T> From<Bitmask<T>> for FullRange
where
    T: Into<u64>,
{
    fn from(value: Bitmask<T>) -> Self {
        Self::Int(FullIntRange(u64::from(value)))
    }
}

impl From<F32Range> for FullRange {
    fn from(value: F32Range) -> Self {
        Self::Float(value.range.into())
    }
}

impl From<F64Range> for FullRange {
    fn from(value: F64Range) -> Self {
        Self::Float(value.range)
    }
}

impl From<FixedAsciiRange> for FullRange {
    fn from(value: FixedAsciiRange) -> Self {
        Self::Int(FullIntRange(value.value().0))
    }
}

impl From<DelimAsciiRange> for FullRange {
    fn from(value: DelimAsciiRange) -> Self {
        Self::Int(FullIntRange(value.0.0))
    }
}

impl From<VariableBitmask> for FullRange {
    fn from(value: VariableBitmask) -> Self {
        match_any_uint!(value, x, x.into())
    }
}

impl From<MixedRange> for FullRange {
    fn from(value: MixedRange) -> Self {
        match_any_datatype!(value, x, x.into())
    }
}

impl<C> From<NativeSeries<C>> for FullRange
where
    C: ColumnHasNativeType + Into<Self>,
    C::Native: FCSRepr,
{
    fn from(value: NativeSeries<C>) -> Self {
        value.column_schema.into()
    }
}

impl From<VariableUintSeries> for FullRange {
    fn from(value: VariableUintSeries) -> Self {
        match_any_uint!(value, x, x.into())
    }
}

impl From<MixedSeries> for FullRange {
    fn from(value: MixedSeries) -> Self {
        match_any_datatype!(value, x, x.into())
    }
}

// Implement column schema -> Range (ie $PnR) for types defined here.
//
// For bitmask and ascii types this is defined in separate modules.

impl From<MixedRange> for TextRange {
    fn from(value: MixedRange) -> Self {
        match_any_datatype!(value, x, x.into())
    }
}

impl From<VariableBitmask> for TextRange {
    fn from(value: VariableBitmask) -> Self {
        match_any_uint!(value, x, x.into())
    }
}

impl<T> From<FloatRange<T>> for TextRange
where
    FiniteFloat<T>: Into<BigDecimal>,
{
    fn from(value: FloatRange<T>) -> Self {
        Into::<BigDecimal>::into(value.range).into()
    }
}

impl<C> From<NativeSeries<C>> for TextRange
where
    C: ColumnHasNativeType + Into<Self>,
    C::Native: FCSRepr,
{
    fn from(value: NativeSeries<C>) -> Self {
        value.column_schema.into()
    }
}

impl From<VariableUintSeries> for TextRange {
    fn from(value: VariableUintSeries) -> Self {
        match_any_uint!(value, x, x.into())
    }
}

impl From<MixedSeries> for TextRange {
    fn from(value: MixedSeries) -> Self {
        match_any_datatype!(value, x, x.into())
    }
}

// Implement column schema -> $PnB conversion
//
// This is simple for everything except delim ascii which returns '*' instead of
// a number.

/// Convert column schema to $PnB value.
trait ColumnSchemaAsWidth {
    fn as_width(&self) -> Width;
}

impl<T: ColumnIsFixed> ColumnSchemaAsWidth for T {
    fn as_width(&self) -> Width {
        Width::Fixed(self.fixed_width())
    }
}

impl ColumnSchemaAsWidth for DelimAsciiRange {
    fn as_width(&self) -> Width {
        Width::Variable
    }
}

impl<T, R> ColumnSchemaAsWidth for Series<DelimAsciiRange, T, R> {
    fn as_width(&self) -> Width {
        self.column_schema.as_width()
    }
}

// Implement Range (ie $PnR) -> column schema
//
// This applies to all except mixed type columns since these need additional
// information to interpret the $PnR value as a given type.

/// A column schema type which can be converted from a $PnR range value.
pub trait ColumnSchemaFromTextRange: Sized {
    type Error;

    #[must_use]
    fn from_range(
        range: TextRange,
        flag: DisallowRangeTrunc,
    ) -> DeferredSwitchableError<ConvertedRange<Self>, DisallowRangeTrunc, Self::Error> {
        Self::from_range_inner(range).nowarn_into_switchable3(flag)
    }

    fn from_range_inner(range: TextRange) -> DeferredError<ConvertedRange<Self>, Self::Error>;
}

impl<T> ColumnSchemaFromTextRange for Bitmask<T>
where
    T: TryFrom<TextRange, Error = RangeToIntError<T>>
        + FCSRepr
        + Copy
        + Bounded
        + Shr<usize, Output = T>,
    u64: From<T>,
{
    type Error = RangeToBitmaskError;

    fn from_range_inner(range: TextRange) -> DeferredError<ConvertedRange<Self>, Self::Error> {
        range
            .clone()
            .into_uint()
            .map_error(RangeToBitmaskError::from)
            .map_deferred_value(Self::from_native)
            .map_ok_value(|n| ConvertedRange::new(n, None))
            .map_err_value(|n| ConvertedRange::new(n, Some(range)))
    }
}

impl<T> ColumnSchemaFromTextRange for FloatRange<T>
where
    BigDecimal: TryInto<FiniteFloat<T>, Error = DecimalToFloatError>,
    FiniteFloat<T>: Bounded,
{
    type Error = DecimalToFloatError;

    fn from_range_inner(range: TextRange) -> DeferredError<ConvertedRange<Self>, Self::Error> {
        range
            .clone()
            .into_float()
            .map_deferred_value(Self::new)
            .map_ok_value(|n| ConvertedRange::new(n, None))
            .map_err_value(|n| ConvertedRange::new(n, Some(range)))
    }
}

impl ColumnSchemaFromTextRange for AsciiRangeValue {
    type Error = RangeToAsciiError;

    /// Make new AsciiRange from a float or integer.
    ///
    /// The number of chars will be automatically selected as the minimum
    /// required to express the range.
    fn from_range_inner(range: TextRange) -> DeferredError<ConvertedRange<Self>, Self::Error> {
        range
            .clone()
            .into_ascii_uint()
            .map_errors(RangeToAsciiError::from)
            .map_ok_value(|n| ConvertedRange::new(n, None))
            .map_err_value(|n| ConvertedRange::new(n, Some(range)))
    }
}

impl ColumnSchemaFromTextRange for FixedAsciiRange {
    type Error = RangeToAsciiError;

    /// Make new [`FixedAsciiRange`] from a float or integer.
    ///
    /// The number of chars will be automatically selected as the minimum
    /// required to express the range.
    fn from_range_inner(range: TextRange) -> DeferredError<ConvertedRange<Self>, Self::Error> {
        AsciiRangeValue::from_range_inner(range).map_deferred_value(Functor::fmap_into)
    }
}

impl ColumnSchemaFromTextRange for DelimAsciiRange {
    type Error = RangeToAsciiError;

    /// Make new [`DelimAsciiRange`] from a float or integer.
    fn from_range_inner(range: TextRange) -> DeferredError<ConvertedRange<Self>, Self::Error> {
        AsciiRangeValue::from_range_inner(range).map_deferred_value(Functor::fmap_into)
    }
}

// Implement column -> column conversions

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

impl_col_into!(VariableBitmask, Bitmask08);
impl_col_into!(VariableBitmask, Bitmask16);
impl_col_into!(VariableBitmask, Bitmask24);
impl_col_into!(VariableBitmask, Bitmask32);
impl_col_into!(VariableBitmask, Bitmask40);
impl_col_into!(VariableBitmask, Bitmask48);
impl_col_into!(VariableBitmask, Bitmask56);
impl_col_into!(VariableBitmask, Bitmask64);

impl_col_into!(MixedRange, F32Range);
impl_col_into!(MixedRange, F64Range);
impl_col_into!(MixedRange, FixedAsciiRange);
impl_col_into!(MixedRange, VariableBitmask);

impl_col_into!(VariableUintSeries, NativeSeries<Bitmask08>);
impl_col_into!(VariableUintSeries, NativeSeries<Bitmask16>);
impl_col_into!(VariableUintSeries, NativeSeries<Bitmask24>);
impl_col_into!(VariableUintSeries, NativeSeries<Bitmask32>);
impl_col_into!(VariableUintSeries, NativeSeries<Bitmask40>);
impl_col_into!(VariableUintSeries, NativeSeries<Bitmask48>);
impl_col_into!(VariableUintSeries, NativeSeries<Bitmask56>);
impl_col_into!(VariableUintSeries, NativeSeries<Bitmask64>);

impl_col_into!(MixedSeries, NativeSeries<F32Range>);
impl_col_into!(MixedSeries, NativeSeries<F64Range>);
impl_col_into!(MixedSeries, NativeSeries<FixedAsciiRange>);
impl_col_into!(MixedSeries, VariableUintSeries);

// Implement column -> primitive column

impl From<MixedSeries> for AnyPrimitiveSeries {
    fn from(value: MixedSeries) -> Self {
        match value {
            MixedSeries::Ascii(x) => Self::from(PrimitiveSeries::from(x)),
            MixedSeries::Uint(x) => x.into(),
            MixedSeries::F32(x) => Self::from(PrimitiveSeries::from(x)),
            MixedSeries::F64(x) => Self::from(PrimitiveSeries::from(x)),
        }
    }
}

impl From<VariableUintSeries> for AnyPrimitiveSeries {
    fn from(value: VariableUintSeries) -> Self {
        match_any_uint!(value, x, PrimitiveSeries::from(x).into())
    }
}

impl<C> From<NativeSeries<C>> for AnyPrimitiveSeries
where
    C: ColumnHasNativeType,
    C::Native: FCSRepr,
    NativeInternalSeries<C>: Into<PrimitiveSeries<<C::Native as FCSRepr>::Prim>>,
    PrimitiveSeries<<C::Native as FCSRepr>::Prim>: Into<Self>,
{
    fn from(value: NativeSeries<C>) -> Self {
        let new: PrimitiveSeries<_> = value.series.into();
        new.into()
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
        let range = TextRange::remove_meas_req(std, i);
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
        let range = TextRange::get_meas_req(kws, i);
        let datatype = Self::lookup_datatype_ro(kws, i, conf);
        Self::make_meas(width, range, datatype)
    }

    fn make_meas(
        width: Result<Width, ReqIndexedKeyError<Width>>,
        range: Result<TextRange, ReqIndexedKeyError<TextRange>>,
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
            .map_ok_value(|(w_, r_, d_)| DataSchemaKeywordValues::new(w_, r_, d_))
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

// Implement column -> native range + $PnR
//
// This is used only internally by the range checker.

/// A column schema which has a $PnR and and native type for range.
trait ColumnSchemaAsRange: ColumnHasNativeType {
    fn as_range(&self) -> (Self::Native, TextRange);
}

impl<T> ColumnSchemaAsRange for Bitmask<T>
where
    Self: ColumnHasNativeType<Native = T> + Into<TextRange>,
    T: Copy,
{
    fn as_range(&self) -> (Self::Native, TextRange) {
        (self.bitmask(), (*self).into())
    }
}

impl<T> ColumnSchemaAsRange for FloatRange<T>
where
    Self: ColumnHasNativeType<Native = T> + Into<TextRange>,
    T: Copy,
    FiniteFloat<T>: Into<T>,
{
    fn as_range(&self) -> (Self::Native, TextRange) {
        (self.range.into(), (*self).into())
    }
}

impl ColumnSchemaAsRange for FixedAsciiRange {
    fn as_range(&self) -> (Self::Native, TextRange) {
        let r = self.value();
        (r.0, r.0.into())
    }
}

impl ColumnSchemaAsRange for DelimAsciiRange {
    fn as_range(&self) -> (Self::Native, TextRange) {
        let r = self.0;
        (r.0, r.0.into())
    }
}

// Implement range check for columns
//
// This is the column-level trait for CheckRanges; see for details.

pub(crate) trait CheckRange {
    fn check_range(&self, i: MeasIndex, check: CheckedRangeDatatypes) -> TruncatedResult;

    fn check_range_mut(&mut self, i: MeasIndex, trunc: CheckedRangeDatatypes) -> TruncatedResult;
}

impl<C> CheckRange for NativeSeries<C>
where
    C: Clone + ColumnHasNativeType + ColumnHasDatatype + ColumnSchemaAsRange,
    <<C as ColumnHasNativeType>::Native as FCSRepr>::Prim: Copy + PartialOrd,
    C::Native: FCSRepr + Into<<<C as ColumnHasNativeType>::Native as FCSRepr>::Prim>,
{
    // TODO not DRY
    fn check_range(&self, i: MeasIndex, check: CheckedRangeDatatypes) -> TruncatedResult {
        let dt = self.column_schema.col_datatype();
        let (u, rng) = self.column_schema.as_range();
        let upper_limit = u.into();
        if dt.matches_range_check(check) {
            self.series
                .as_ref()
                .iter()
                .position(|x| *x > upper_limit)
                .map_or(TruncatedResult::None, |rowi| {
                    TruncatedResult::Overrange(i, rowi, rng)
                })
        } else {
            TruncatedResult::None
        }
    }

    // TODO these errors could be cleaned up; we know that the highest range
    // that can be truncated is u64 or f64 so it isn't necessary to return a
    // rang object. Furthermore it shouldn't be necessary to pass the calling index.
    fn check_range_mut(&mut self, i: MeasIndex, trunc: CheckedRangeDatatypes) -> TruncatedResult {
        let dt = self.column_schema.col_datatype();
        let (u, rng) = self.column_schema.as_range();
        let upper_limit = u.into();
        if dt.matches_range_check(trunc) {
            // If we wish to truncate this column, silently truncate without
            // throwing any errors
            let j = self.series.truncate(u);
            j.map_or(TruncatedResult::None, TruncatedResult::Truncated)
        } else {
            // Otherwise, scan through the values and return error on first
            // encounter with overrange value
            self.series
                .as_ref()
                .iter()
                .position(|x| *x > upper_limit)
                .map_or(TruncatedResult::None, |rowi| {
                    TruncatedResult::Overrange(i, rowi, rng)
                })
        }
    }
}

impl CheckRange for VariableUintSeries {
    fn check_range(&self, i: MeasIndex, check: CheckedRangeDatatypes) -> TruncatedResult {
        match_any_uint!(self, x, x.check_range(i, check))
    }

    fn check_range_mut(&mut self, i: MeasIndex, trunc: CheckedRangeDatatypes) -> TruncatedResult {
        match_any_uint!(self, x, x.check_range_mut(i, trunc))
    }
}

impl CheckRange for MixedSeries {
    fn check_range(&self, i: MeasIndex, check: CheckedRangeDatatypes) -> TruncatedResult {
        match_any_datatype!(self, x, x.check_range(i, check))
    }

    fn check_range_mut(&mut self, i: MeasIndex, trunc: CheckedRangeDatatypes) -> TruncatedResult {
        match_any_datatype!(self, x, x.check_range_mut(i, trunc))
    }
}

// Implement read dispatch for byte layouts.
//
// For simple cases where DATA is all the same type (or can be read as the same
// type and then cast to other types), each byte layout can be mapped to a
// specialized loop which reads all bytes as a matrix.

trait ByteOrderIO<C: ColumnHasNativeType>
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
        cols: &[&[<C as ColumnHasNativeType>::Native]],
    ) -> io::Result<()>;
}

macro_rules! impl_byte_layout_io {
    ($inner:path, $layout:path, $read_fun:ident, $write_fun:ident) => {
        impl ByteOrderIO<$inner> for $layout {
            fn read_matrix<R: Read>(
                &self,
                h: &mut BufReader<R>,
                buf: &mut ReadBuffer,
                cols: &mut Vec<Vec<<$inner as ColumnHasNativeType>::Native>>,
            ) -> io::Result<()> {
                buf.$read_fun(h, cols, *self)
            }

            fn write_matrix<W: Write>(
                &self,
                h: &mut BufWriter<W>,
                buf: &mut WriteBuffer,
                cols: &[&[<$inner as ColumnHasNativeType>::Native]],
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
            ArrayByteOrd<<<$t as ColumnHasNativeType>::Native as FCSRepr>::ByteOrd>,
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
// convert a data schema to a dataframe layout with the same number of
// columns which often won't be zero.

impl<C: Default, I, F, L: Default, M, const ORD: bool> Default for Layout<C, F, I, L, M, ORD> {
    fn default() -> Self {
        Self::new(C::default(), L::default())
    }
}

impl<M, N: Default> Default for Any3_2<M, N> {
    fn default() -> Self {
        Self::NonMixed(N::default())
    }
}

impl<W0: Default, W> Default for AnyBigLittleUint<W0, W> {
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
    AnyBigLittleUint<W0, W>,
    Single(W0)<C08, C16, C24, C32, C40, C48, C56, C64>
        ~ AnyUint<C08, C16, C24, C32, C40, C48, C56, C64>,
    Multi(W)<C, F, L, M, const ORD: bool>
        ~ Layout<C, F, UvarCol, L, M, ORD>
}

impl_generic_enum_from! {
    AnyUint<C08, C16, C24, C32, C40, C48, C56, C64>
        <C, F, L, M, const ORD: bool>,
    Uint08(C08) ~ Layout<C, F, U08Col, L, M, ORD>,
    Uint16(C16) ~ Layout<C, F, U16Col, L, M, ORD>,
    Uint24(C24) ~ Layout<C, F, U24Col, L, M, ORD>,
    Uint32(C32) ~ Layout<C, F, U32Col, L, M, ORD>,
    Uint40(C40) ~ Layout<C, F, U40Col, L, M, ORD>,
    Uint48(C48) ~ Layout<C, F, U48Col, L, M, ORD>,
    Uint56(C56) ~ Layout<C, F, U56Col, L, M, ORD>,
    Uint64(C64) ~ Layout<C, F, U64Col, L, M, ORD>
}

impl_generic_enum_from! {
    AnyDatatype<A, I, F32, F64>,
    Ascii(A)<Ad, Aa> ~ AnyAscii<Ad, Aa>,
    // impl From<AnyUint<...>> below since this can accept two layout types
    Uint(I)<W0, W> ~ AnyBigLittleUint<W0, W>,
    F32(F32)<C, F, L, M, const ORD: bool> ~ Layout<C, F, F32Col, L, M, ORD>,
    F64(F64)<C, F, L, M, const ORD: bool> ~ Layout<C, F, F64Col, L, M, ORD>
}

impl<F, const ORD: bool, M, A, F32, F64> From<AnyUintLayout<F, ORD, M>>
    for AnyDatatype<A, AnyUintLayout<F, ORD, M>, F32, F64>
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
    fn from(value: AnyUintLayout<F, ORD, M>) -> Self {
        Self::Uint(value)
    }
}

impl_generic_enum_from! {
    AnyAscii<Delim, Fixed>,
    Delimited(Delim)<C, F, L, M, const ORD: bool> ~ Layout<C, F, DelimAsciiCol, L, M, ORD>,
    Fixed(Fixed)<C, F, L, M, const ORD: bool> ~ Layout<C, F, FixedAsciiCol, L, M, ORD>
}

impl<C, F, L, M, N, const ORD: bool> From<Layout<C, F, MixedCol, L, M, ORD>>
    for Any3_2<Layout<C, F, MixedCol, L, M, ORD>, N>
{
    fn from(value: Layout<C, F, MixedCol, L, M, ORD>) -> Self {
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
    VariableBitmask,
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
    VariableUintSeries,
    Uint08 ~ NativeSeries<Bitmask08>,
    Uint16 ~ NativeSeries<Bitmask16>,
    Uint24 ~ NativeSeries<Bitmask24>,
    Uint32 ~ NativeSeries<Bitmask32>,
    Uint40 ~ NativeSeries<Bitmask40>,
    Uint48 ~ NativeSeries<Bitmask48>,
    Uint56 ~ NativeSeries<Bitmask56>,
    Uint64 ~ NativeSeries<Bitmask64>
}

impl_generic_enum_from! {
    MixedRange,
    Ascii ~ FixedAsciiRange,
    Uint ~ VariableBitmask,
    F32 ~ F32Range,
    F64 ~ F64Range
}

impl_generic_enum_from! {
    MixedSeries,
    Ascii ~ NativeSeries<FixedAsciiRange>,
    Uint ~ VariableUintSeries,
    F32 ~ NativeSeries<F32Range>,
    F64 ~ NativeSeries<F64Range>
}

// necessary for inserting ascii range into mixed layuot
impl From<DelimAsciiRange> for MixedRange {
    fn from(value: DelimAsciiRange) -> Self {
        // this will automatically make any delimited ASCII layout a fixed
        // layout if we go to mixed, which seems sane if not an exceedingly rare
        // use case.
        Self::Ascii(value.into())
    }
}

// necessary for inserting ascii range into mixed layuot
impl From<NativeSeries<DelimAsciiRange>> for MixedSeries {
    fn from(value: NativeSeries<DelimAsciiRange>) -> Self {
        // this will automatically make any delimited ASCII layout a fixed
        // layout if we go to mixed, which seems sane if not an exceedingly rare
        // use case.
        Self::Ascii(value.into())
    }
}

// necessary for inserting ascii range into mixed layuot
impl From<NativeSeries<DelimAsciiRange>> for NativeSeries<FixedAsciiRange> {
    fn from(value: NativeSeries<DelimAsciiRange>) -> Self {
        NativeSeries::new(value.column_schema.into(), value.series)
    }
}

// necessary for inserting ascii range into mixed layuot
impl From<NativeSeries<FixedAsciiRange>> for NativeSeries<DelimAsciiRange> {
    fn from(value: NativeSeries<FixedAsciiRange>) -> Self {
        NativeSeries::new(value.column_schema.into(), value.series)
    }
}

// necessary for inserting bitmask into mixed layuot
impl<T> From<Bitmask<T>> for MixedRange
where
    VariableBitmask: From<Bitmask<T>>,
{
    fn from(value: Bitmask<T>) -> Self {
        Self::Uint(value.into())
    }
}

// necessary for inserting bitmask into mixed layuot
impl<T> From<NativeSeries<Bitmask<T>>> for MixedSeries
where
    VariableUintSeries: From<NativeSeries<Bitmask<T>>>,
    Bitmask<T>: ColumnHasNativeType,
    <Bitmask<T> as ColumnHasNativeType>::Native: FCSRepr,
{
    fn from(value: NativeSeries<Bitmask<T>>) -> Self {
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
                    let b = <<$inner as ColumnHasNativeType>::Native as FCSRepr>::FILE_BYTES;
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
                    let dst_type = <$inner as ColumnHasOneDatatype>::DATATYPE;
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
                        let b = <<Self as ColumnHasNativeType>::Native as FCSRepr>::FILE_BYTES;
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

impl_uint_try_from_var_uint!(VariableBitmask, Bitmask08, Uint08);
impl_uint_try_from_var_uint!(VariableBitmask, Bitmask16, Uint16);
impl_uint_try_from_var_uint!(VariableBitmask, Bitmask24, Uint24);
impl_uint_try_from_var_uint!(VariableBitmask, Bitmask32, Uint32);
impl_uint_try_from_var_uint!(VariableBitmask, Bitmask40, Uint40);
impl_uint_try_from_var_uint!(VariableBitmask, Bitmask48, Uint48);
impl_uint_try_from_var_uint!(VariableBitmask, Bitmask56, Uint56);
impl_uint_try_from_var_uint!(VariableBitmask, Bitmask64, Uint64);

impl_uint_try_from_var_uint!(VariableUintSeries, NativeSeries<Bitmask08>, Uint08);
impl_uint_try_from_var_uint!(VariableUintSeries, NativeSeries<Bitmask16>, Uint16);
impl_uint_try_from_var_uint!(VariableUintSeries, NativeSeries<Bitmask24>, Uint24);
impl_uint_try_from_var_uint!(VariableUintSeries, NativeSeries<Bitmask32>, Uint32);
impl_uint_try_from_var_uint!(VariableUintSeries, NativeSeries<Bitmask40>, Uint40);
impl_uint_try_from_var_uint!(VariableUintSeries, NativeSeries<Bitmask48>, Uint48);
impl_uint_try_from_var_uint!(VariableUintSeries, NativeSeries<Bitmask56>, Uint56);
impl_uint_try_from_var_uint!(VariableUintSeries, NativeSeries<Bitmask64>, Uint64);

impl_nonmixed_try_from_mixed!(MixedRange, FixedAsciiRange, Ascii);
impl_nonmixed_try_from_mixed!(MixedRange, VariableBitmask, Uint);
impl_nonmixed_try_from_mixed!(MixedRange, F32Range, F32);
impl_nonmixed_try_from_mixed!(MixedRange, F64Range, F64);

impl_nonmixed_try_from_mixed!(MixedSeries, NativeSeries<FixedAsciiRange>, Ascii);
impl_nonmixed_try_from_mixed!(MixedSeries, VariableUintSeries, Uint);
impl_nonmixed_try_from_mixed!(MixedSeries, NativeSeries<F32Range>, F32);
impl_nonmixed_try_from_mixed!(MixedSeries, NativeSeries<F64Range>, F64);

impl_uint_try_from_mixed!(Uint08, Bitmask08);
impl_uint_try_from_mixed!(Uint16, Bitmask16);
impl_uint_try_from_mixed!(Uint24, Bitmask24);
impl_uint_try_from_mixed!(Uint32, Bitmask32);
impl_uint_try_from_mixed!(Uint40, Bitmask40);
impl_uint_try_from_mixed!(Uint48, Bitmask48);
impl_uint_try_from_mixed!(Uint56, Bitmask56);
impl_uint_try_from_mixed!(Uint64, Bitmask64);

// Implement reference to container for Layout
//
// This is a slightly nicer way to "reference the thing inside a functor"
// without actually using the functor trait which is sometimes annoying.

impl<C, F, I, L, M, const ORD: bool> AsRef<[C]> for Layout<Vec<C>, F, I, L, M, ORD> {
    fn as_ref(&self) -> &[C] {
        self.container.as_ref()
    }
}

impl<C, F, I, L, M, const ORD: bool> AsRef<[C]> for Layout<DataFrame<C>, F, I, L, M, ORD> {
    fn as_ref(&self) -> &[C] {
        self.container.as_ref()
    }
}

// Implement methods on Layout

impl<C, F, I, M, const ORD: bool> Layout<C, F, I, NoByteOrd<ORD>, M, ORD> {
    pub fn new_ascii(columns: C) -> Self {
        Self::new(columns, NoByteOrd::<ORD>)
    }
}

impl<T, I, A, M, const ORD: bool> Layout<Vec<FloatRange<T>>, VecFamily, I, ArrayByteOrd<A>, M, ORD>
where
    T: FCSRepr,
    FloatRange<T>: ColumnHasNativeType<Native = T>,
{
    #[must_use]
    pub fn new_endian_float(ranges: Vec<FloatRange<T>>, endian: Endian) -> Self {
        Self::new(ranges, ArrayByteOrd::Endian(endian))
    }
}

impl<C, I, L, T, D, const ORD: bool> Layout<Vec<C>, VecFamily, I, L, ColumnMarkers<T, D>, ORD> {
    fn try_new<F, P, W, E>(
        cs: Vec<DataSchemaKeywordValues<D>>,
        byte_layout: L,
        new_col_f: F,
    ) -> WarningsAndErrorsResult<NewDataSchema<Self>, (), W, E>
    where
        D: IsNumType,
        F: Fn(
            MeasIndex,
            DataSchemaKeywordValues<D>,
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
                NewDataSchema::new(new_layout, truncated)
            })
    }
}

impl<C, F, I, L, M, const ORD: bool> Layout<C, F, I, L, M, ORD> {
    pub fn byte_layout_into<Lf, const ORD_F: bool>(self) -> Layout<C, F, I, Lf, M, ORD_F>
    where
        L: Into<Lf>,
    {
        Layout::new(self.container, self.byteord.into())
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
        X: ColumnIsFixed,
    {
        self.columns()
            .iter()
            .map(ColumnIsFixed::fixed_width)
            .collect()
    }

    /// Produce conversion error if columns are not all the same width.
    ///
    /// Useful when converting a mixed-width int layout into a single layout.
    fn conversion_fail_by_width<X, R>(&self) -> LayoutConvertResult<R>
    where
        C: AsRef<[X]>,
        X: ColumnIsFixed,
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
        X: ColumnIsFixed,
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
        X: ColumnIsFixed,
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

    fn map_inner<Fun, If>(self, f: Fun) -> Layout<Sibling1<C, If::Inner>, F, If, L, M, ORD>
    where
        I: IsCol<F, ORD>,
        If: IsCol<F, ORD>,
        Fun: FnMut(I::Inner) -> If::Inner,
        C: Functor<I::Inner>,
    {
        Layout::new(self.container.fmap(f), self.byteord)
    }

    fn set_byte_layout<Lf, const ORD_F: bool>(
        self,
        byte_layout: Lf,
    ) -> Layout<C, F, I, Lf, M, ORD_F> {
        Layout::new(self.container, byte_layout)
    }

    fn byte_layout_try_into<Lf, const ORD_F: bool>(
        self,
    ) -> Result<Layout<C, F, I, Lf, M, ORD_F>, L::Error>
    where
        L: TryInto<Lf>,
    {
        let b = self.byteord.try_into()?;
        Ok(Layout::new(self.container, b))
    }
}

// Implement methods on specific aliases of column group

// TODO these could be used internally when parsing from keywords

impl<D> AnySingleUintDataSchema<D> {
    /// Make a new big/little endian uint layout with a given width in bytes.
    ///
    /// Throw error if any of the provided ranges cannot fit within the allotted
    /// width.
    ///
    /// Only applicable to FCS 3.1/3.2.
    pub fn new_single_uint(
        ranges: Vec<FullIntRange>,
        byte_width: &ArgBytes,
        endian: Endian,
    ) -> Result<Self, NewBitmaskError> {
        macro_rules! go {
            ($var:ident) => {{
                let rs = ranges
                    .into_iter()
                    .map(Bitmask::try_from)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(AnyUint::$var(Layout::new(rs, endian)))
            }};
        }
        match byte_width.0 {
            PrivBytes::B1 => go!(Uint08),
            PrivBytes::B2 => go!(Uint16),
            PrivBytes::B3 => go!(Uint24),
            PrivBytes::B4 => go!(Uint32),
            PrivBytes::B5 => go!(Uint40),
            PrivBytes::B6 => go!(Uint48),
            PrivBytes::B7 => go!(Uint56),
            PrivBytes::B8 => go!(Uint64),
        }
    }

    #[must_use]
    pub fn byte_width(&self) -> ArgBytes {
        let b = match self {
            AnyUint::Uint08(_) => PrivBytes::B1,
            AnyUint::Uint16(_) => PrivBytes::B2,
            AnyUint::Uint24(_) => PrivBytes::B3,
            AnyUint::Uint32(_) => PrivBytes::B4,
            AnyUint::Uint40(_) => PrivBytes::B5,
            AnyUint::Uint48(_) => PrivBytes::B6,
            AnyUint::Uint56(_) => PrivBytes::B7,
            AnyUint::Uint64(_) => PrivBytes::B8,
        };
        ArgBytes(b)
    }
}

impl<T> AnyOrderedUintDataSchema<T> {
    /// Make a new uint layout with a given byte order and width in bytes
    ///
    /// Throw error if any of the provided ranges cannot fit within the allotted
    /// width.
    ///
    /// Only applicable to FCS 2.0/3.0.
    pub fn new_ordered_uint(
        ranges: Vec<FullIntRange>,
        byte_width: &ArgBytes,
        byte_order: AnyByteOrder,
    ) -> Result<Self, NewOrderedUintLayoutError> {
        macro_rules! go {
            ($var:ident) => {{
                let rs = ranges
                    .into_iter()
                    .map(Bitmask::try_from)
                    .collect::<Result<Vec<_>, _>>()?;
                let b = byte_order.try_into()?;
                Ok(AnyUint::$var(Layout::new(rs, b)))
            }};
        }
        match byte_width.0 {
            PrivBytes::B1 => go!(Uint08),
            PrivBytes::B2 => go!(Uint16),
            PrivBytes::B3 => go!(Uint24),
            PrivBytes::B4 => go!(Uint32),
            PrivBytes::B5 => go!(Uint40),
            PrivBytes::B6 => go!(Uint48),
            PrivBytes::B7 => go!(Uint56),
            PrivBytes::B8 => go!(Uint64),
        }
    }

    #[must_use]
    pub fn byte_width(&self) -> ArgBytes {
        let b = match self {
            AnyUint::Uint08(_) => PrivBytes::B1,
            AnyUint::Uint16(_) => PrivBytes::B2,
            AnyUint::Uint24(_) => PrivBytes::B3,
            AnyUint::Uint32(_) => PrivBytes::B4,
            AnyUint::Uint40(_) => PrivBytes::B5,
            AnyUint::Uint48(_) => PrivBytes::B6,
            AnyUint::Uint56(_) => PrivBytes::B7,
            AnyUint::Uint64(_) => PrivBytes::B8,
        };
        ArgBytes(b)
    }

    fn try_new(
        cs: Vec<DataSchemaKeywordValues<Nothing<NumType>>>,
        bo: ByteOrd2_0,
        conf: &ReadDataKeywordsConfig,
    ) -> WarningsAndErrorsResult<NewDataSchema<Self>, (), IndexedBitmaskError, NewFixedIntLayoutError>
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
                Layout::try_new(cs, o, |i, c| {
                    Bitmask::from_range(c.range, notrunc)
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

impl<T, D, const ORD: bool> AnyAsciiDataSchema<ORD, ColumnMarkers<T, D>>
where
    FixedAsciiCol: IsCol<VecFamily, ORD, Inner = FixedAsciiRange, Layout = NoByteOrd<ORD>>,
    DelimAsciiCol: IsCol<VecFamily, ORD, Inner = DelimAsciiRange, Layout = NoByteOrd<ORD>>,
{
    fn try_new(
        cs: Vec<DataSchemaKeywordValues<D>>,
        flag: DisallowRangeTrunc,
    ) -> WarningsAndErrorsResult<
        NewDataSchema<Self>,
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
                    let l = Layout::new_ascii(ranges);
                    NewDataSchema::new(Self::Delimited(l), non_truncated)
                })
                .map_err_value(|_| ())
        } else {
            Layout::try_new(cs, NoByteOrd, |i, c| {
                FixedAsciiRange::from_width_and_range(c.width, c.range, i, flag)
            })
            .map_ok_value(FunctorOnce::fmap_into_once)
        }
    }

    fn new_fixed(columns: impl IntoIterator<Item = FixedAsciiRange>) -> Self {
        Self::Fixed(Layout::new_ascii(columns.into_iter().collect()))
    }

    fn new_delim(ranges: Vec<DelimAsciiRange>) -> Self {
        Self::Delimited(Layout::new_ascii(ranges))
    }
}

impl<const ORD: bool, M> FixedAsciiDataSchema<ORD, M>
where
    FixedAsciiCol: IsCol<VecFamily, ORD, Inner = FixedAsciiRange, Layout = NoByteOrd<ORD>>,
{
    #[must_use]
    pub fn new_ascii_u64(ranges: Vec<AsciiRangeValue>) -> Self {
        Self::new_ascii(ranges.fmap_into())
    }
}

impl DataSchema3_2 {
    // #[must_use]
    // pub fn new_mixed(rs: Vec<MixedRange>, endian: Endian) -> Self {
    //     // Check if the mixed types are all the same, in which case we can use a
    //     // simpler layout. This clone thing is not ideal but it will only be
    //     // cloning big-decimals for floats and will use Copy for everything else
    //     // (not a huge deal).
    //     macro_rules! go {
    //         ($t:ident) => {
    //             rs.iter()
    //                 .map(|x| $t::try_from(x.clone()))
    //                 .collect::<Result<Vec<_>, _>>()
    //         };
    //     }
    //     let mut ret: Self = if let Ok(xs) = go!(FixedAsciiRange) {
    //         NonMixedEndianHeaders::new_ascii_fixed(xs).into()
    //     } else if let Ok(xs) = go!(VariableBitmask) {
    //         NonMixedEndianHeaders::new_uint(xs, endian).into()
    //     } else if let Ok(xs) = go!(F32Range) {
    //         NonMixedEndianHeaders::new_f32(xs, endian).into()
    //     } else if let Ok(xs) = go!(F64Range) {
    //         NonMixedEndianHeaders::new_f64(xs, endian).into()
    //     } else {
    //         Layout::new(rs, endian).into()
    //     };
    //     ret.normalize();
    //     ret
    // }

    fn lookup_inner(
        datatype: Result<AlphaNumType, ReqKeyError<AlphaNumType>>,
        endian: Result<ByteOrd3_1, ReqKeyError<ByteOrd3_1>>,
        columns: LookupMeasLayoutResult<Option<NumType>>,
        conf: &ReadDataKeywordsConfig,
    ) -> LookupLayoutResult<NewDataSchema<Self>> {
        let endian_ = endian.map_err(LookupDataSchemaError::from).into_log();
        let columns_ = columns
            .map_commutative_warnings(LookupDataSchemaWarning::from)
            .map_errors(LookupDataSchemaError::Meas);
        datatype
            .map_err(LookupDataSchemaError::from)
            .into_log()
            .zip3_commutative(endian_, columns_)
            .and_then_commutative(|(d, e, cs)| {
                Self::try_new(d, e, cs, conf)
                    .map_commutative_warnings(LookupDataSchemaWarning::from)
                    .map_errors(LookupDataSchemaError::from)
            })
    }
}

impl<T> AnyOrderedDataSchema<T> {
    fn lookup(
        std: &mut StdKeywords,
        meas_nonstd: &mut [NonStdKeywords],
        conf: &ReadDataKeywordsConfig,
    ) -> LookupLayoutResult<NewDataSchema<Self>> {
        let datatype = AlphaNumType::remove_metaroot_req(std);
        let byteord = ByteOrd2_0::remove_metaroot_req(std);
        let columns = Nothing::lookup_all(std, meas_nonstd, conf);
        Self::lookup_inner(datatype, byteord, columns, conf)
    }

    fn lookup_ro(
        kws: &StdKeywords,
        par: Par,
        conf: &ReadDataKeywordsConfig,
    ) -> LookupLayoutResult<NewDataSchema<Self>> {
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
    ) -> LookupLayoutResult<NewDataSchema<Self>> {
        let byteord_ = byteord.map_err(LookupDataSchemaError::from).into_log();
        let columns_ = columns
            .map_commutative_warnings(LookupDataSchemaWarning::from)
            .map_errors(LookupDataSchemaError::Meas);
        datatype
            .map_err(LookupDataSchemaError::from)
            .into_log()
            .zip3_commutative(byteord_, columns_)
            .and_then_commutative(|(d, e, cs)| {
                Self::try_new(d, e, cs, conf)
                    .map_commutative_warnings(LookupDataSchemaWarning::from)
                    .map_errors(LookupDataSchemaError::from)
            })
    }

    #[must_use]
    pub fn new_ascii_fixed(ranges: Vec<FixedAsciiRange>) -> Self {
        AnyAsciiDataSchema::new_fixed(ranges).into()
    }

    #[must_use]
    pub fn new_ascii_delim(ranges: Vec<DelimAsciiRange>) -> Self {
        AnyAsciiDataSchema::new_delim(ranges).into()
    }

    #[must_use]
    pub fn new_uint<I>(columns: Vec<I::Inner>, byte_layout: I::Layout) -> Self
    where
        I: IsCol<VecFamily, true>,
        I::Inner: ColumnHasNativeType,
        <I::Inner as ColumnHasNativeType>::Native: FCSRepr,
        AnyOrderedUintDataSchema<T>: From<DataSchema_<I, true, ColumnMarkers<T, Nothing<NumType>>>>,
    {
        Self::Uint(Layout::new(columns, byte_layout).into())
    }

    #[must_use]
    pub fn new_f32(ranges: Vec<F32Range>, byte_layout: ArrayByteOrd<[u8; 4]>) -> Self {
        Layout::new(ranges, byte_layout).into()
    }

    #[must_use]
    pub fn new_f64(ranges: Vec<F64Range>, byte_layout: ArrayByteOrd<[u8; 8]>) -> Self {
        Layout::new(ranges, byte_layout).into()
    }

    fn new_empty(datatype: AlphaNumType) -> Self {
        match datatype {
            AlphaNumType::Ascii => AnyAsciiDataSchema::default().into(),
            AlphaNumType::Integer => AnyOrderedUintDataSchema::default().into(),
            AlphaNumType::Float => Self::F32(Layout::default()),
            AlphaNumType::Double => Self::F64(Layout::default()),
        }
    }

    fn try_new(
        datatype: AlphaNumType,
        byteord: ByteOrd2_0,
        columns: Vec<DataSchemaKeywordValues2_0>,
        conf: &ReadDataKeywordsConfig,
    ) -> WarningsAndErrorsResult<NewDataSchema<Self>, (), NewMixedRangeWarning, NewDataSchemaError>
    {
        macro_rules! from {
            ($i:expr) => {
                $i.map_errors(NewDataSchemaError::from)
                    .map_commutative_warnings(NewMixedRangeWarning::from)
                    .map_ok_value(FunctorOnce::fmap_into_once)
            };
        }

        macro_rules! go_float {
            ($t:ident, $notrunc:expr) => {
                byteord
                    .try_into()
                    .map_err(NewDataSchemaError::from)
                    .into_log()
                    .and_then_commutative(|b| {
                        from! {Layout::try_new(columns, b, |i, c| {
                            $t::from_width_and_range(c.width, c.range, i, $notrunc)
                                .repack_errors()
                        })}
                    })
            };
        }

        let notrunc = conf.disallow_range_truncation;

        match datatype {
            AlphaNumType::Ascii => from!(AnyAsciiDataSchema::try_new(columns, notrunc)),
            AlphaNumType::Integer => {
                from!(AnyOrderedUintDataSchema::try_new(columns, byteord, conf))
            }
            AlphaNumType::Float => go_float!(F32Range, notrunc),
            AlphaNumType::Double => go_float!(F64Range, notrunc),
        }
    }
}

impl NonMixedDataSchema<Nothing<NumType>> {
    fn lookup(
        std: &mut StdKeywords,
        meas_nonstd: &mut [NonStdKeywords],
        conf: &ReadDataKeywordsConfig,
    ) -> LookupLayoutResult<NewDataSchema<Self>> {
        let datatype = AlphaNumType::remove_metaroot_req(std);
        let endian = ByteOrd3_1::remove_metaroot_req(std);
        let columns = Nothing::<NumType>::lookup_all(std, meas_nonstd, conf);
        Self::lookup_inner(datatype, endian, columns, conf)
    }

    fn lookup_ro(
        kws: &StdKeywords,
        par: Par,
        conf: &ReadDataKeywordsConfig,
    ) -> LookupLayoutResult<NewDataSchema<Self>> {
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
    ) -> LookupLayoutResult<NewDataSchema<Self>> {
        let endian_ = endian.map_err(LookupDataSchemaError::from).into_log();
        let columns_ = columns
            .map_commutative_warnings(LookupDataSchemaWarning::from)
            .map_errors(LookupDataSchemaError::Meas);
        datatype
            .map_err(LookupDataSchemaError::from)
            .into_log()
            .zip3_commutative(endian_, columns_)
            .and_then_commutative(|(d, e, cs)| {
                Self::try_new(d, e.0, cs, conf)
                    .map_commutative_warnings(LookupDataSchemaWarning::from)
                    .map_errors(LookupDataSchemaError::from)
            })
    }

    fn try_new(
        datatype: AlphaNumType,
        endian: Endian,
        columns: Vec<DataSchemaKeywordValues<Nothing<NumType>>>,
        conf: &ReadDataKeywordsConfig,
    ) -> WarningsAndErrorsResult<NewDataSchema<Self>, (), NewMixedRangeWarning, NewDataSchemaError>
    {
        let notrunc = conf.disallow_range_truncation;

        let go_f32 = |i: MeasIndex, c: DataSchemaKeywordValues<_>| {
            F32Range::from_width_and_range(c.width, c.range, i, notrunc).repack_errors()
        };

        let go_f64 = |i: MeasIndex, c: DataSchemaKeywordValues<_>| {
            F64Range::from_width_and_range(c.width, c.range, i, notrunc).repack_errors()
        };

        macro_rules! from {
            ($x:expr) => {
                $x.map_errors(NewDataSchemaError::from)
                    .map_commutative_warnings(NewMixedRangeWarning::from)
                    .map_ok_value(FunctorOnce::fmap_into_once)
            };
        }

        match datatype {
            AlphaNumType::Ascii => from!(AnyAsciiDataSchema::try_new(columns, notrunc)),
            AlphaNumType::Integer => {
                from!(AnyBigLittleUintDataSchema::try_new(
                    columns, endian, notrunc
                ))
            }
            AlphaNumType::Float => from!(Layout::try_new(columns, endian, go_f32)),
            AlphaNumType::Double => from!(Layout::try_new(columns, endian, go_f64)),
        }
    }
}

impl<D> NonMixedDataSchema<D> {
    fn new_empty(datatype: AlphaNumType) -> Self {
        Self::new_empty1(datatype, Endian::default())
    }

    fn new_empty1(datatype: AlphaNumType, endian: Endian) -> Self {
        match datatype {
            AlphaNumType::Ascii => AnyAsciiDataSchema::default().into(),
            AlphaNumType::Integer => Self::Uint(AnyBigLittleUint::Single(AnyUint::Uint32(
                Layout::new_empty(endian),
            ))),
            AlphaNumType::Float => Self::F32(Layout::new_empty(endian)),
            AlphaNumType::Double => Self::F64(Layout::new_empty(endian)),
        }
    }

    #[must_use]
    pub fn new_ascii_fixed(ranges: impl IntoIterator<Item = FixedAsciiRange>) -> Self {
        AnyAsciiDataSchema::new_fixed(ranges).into()
    }

    #[must_use]
    pub fn new_ascii_delim(ranges: Vec<DelimAsciiRange>) -> Self {
        AnyAsciiDataSchema::new_delim(ranges).into()
    }

    // TODO make fixed width versions of this?

    #[must_use]
    pub fn new_uint(columns: Vec<VariableBitmask>, endian: Endian) -> Self {
        AnyBigLittleUint::Multi(Layout::new(columns, endian)).into()
    }

    #[must_use]
    pub fn new_f32(ranges: Vec<F32Range>, endian: Endian) -> Self {
        Layout::new(ranges, endian).into()
    }

    #[must_use]
    pub fn new_f64(ranges: Vec<F64Range>, endian: Endian) -> Self {
        Layout::new(ranges, endian).into()
    }
}

impl<Cd, Ca, Id, Ia, F, Ld, La, M, const ORD: bool>
    AnyAscii<Layout<Cd, F, Id, Ld, M, ORD>, Layout<Ca, F, Ia, La, M, ORD>>
{
    #[allow(clippy::type_complexity)]
    pub fn byte_layout_into<Ldf, Laf, const ORD_F: bool>(
        self,
    ) -> AnyAscii<Layout<Cd, F, Id, Ldf, M, ORD_F>, Layout<Ca, F, Ia, Laf, M, ORD_F>>
    where
        Ld: Into<Ldf>,
        La: Into<Laf>,
    {
        match_map_ascii!(self, x, x.byte_layout_into())
    }
}

impl<D> AnyBigLittleUintDataSchema<D> {
    fn try_new(
        cs: Vec<DataSchemaKeywordValues<D>>,
        e: Endian,
        flag: DisallowRangeTrunc,
    ) -> WarningsAndErrorsResult<NewDataSchema<Self>, (), IndexedBitmaskError, NewUintTypeError>
    where
        D: IsNumType,
    {
        Layout::try_new(cs, e, |i, c| {
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

impl From<AnyUintVec> for VariableUintSeries {
    fn from(value: AnyUintVec) -> Self {
        match_map_uint!(value, x, NativeSeries::from(x))
    }
}

impl From<MixedVec> for MixedSeries {
    fn from(value: MixedVec) -> Self {
        match value {
            MixedVec::Ascii(x) => Self::Ascii(NativeSeries::from(x)),
            MixedVec::Uint(x) => Self::Uint(x.into()),
            MixedVec::F32(x) => Self::F32(NativeSeries::from(x)),
            MixedVec::F64(x) => Self::F64(NativeSeries::from(x)),
        }
    }
}

macro_rules! decl_mixed_read {
    ($name:ident, $int_fun:ident, $float_fun:ident) => {
        pub(crate) fn $name(
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
        pub(crate) fn $name(&self, src_index: SrcIndex, dst: &mut [u8], dst_index: DstIndex) {
            match self {
                Self::Ascii(xs) => {
                    let v = xs.as_ref()[src_index.0];
                    xs.column_schema.as_slice_unchecked(v, dst, &dst_index);
                }
                Self::Uint(xs) => xs.$int_fun(src_index, dst, dst_index),
                Self::F32(xs) => xs.as_ref()[src_index.0].$float_fun(dst, dst_index),
                Self::F64(xs) => xs.as_ref()[src_index.0].$float_fun(dst, dst_index),
            }
        }
    };
}

impl MixedSeries {
    decl_mixed_write!(write_le, write_le, to_be_slice);
    decl_mixed_write!(write_be, write_be, to_le_slice);
}

macro_rules! decl_uint_read {
    ($name:ident, $fun:ident) => {
        pub(crate) fn $name(&mut self, dst_index: DstIndex, src: &[u8], src_index: SrcIndex) {
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
        pub(crate) fn $name(&self, src_index: SrcIndex, dst: &mut [u8], dst_index: DstIndex) {
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

impl VariableUintSeries {
    decl_uint_write!(write_le, to_le_slice);
    decl_uint_write!(write_be, to_be_slice);
}

// Implement misc methods for data schema ranges

impl<T> FloatRange<T> {
    /// Make new float range from $PnB and $PnR values.
    ///
    /// Will return an error if $PnB is the incorrect size.
    fn from_width_and_range(
        width: Width,
        range: TextRange,
        i: MeasIndex,
        flag: DisallowRangeTrunc,
    ) -> WarningsAndErrorResult<ConvertedRange<Self>, (), IndexedFloatRangeError, FloatWidthError>
    where
        BigDecimal: TryInto<FiniteFloat<T>, Error = DecimalToFloatError>,
        FiniteFloat<T>: Bounded,
        T: FCSRepr,
    {
        PrivBytes::try_from(width)
            .map_err(|e| IndexedError::new(i, e))
            .map_err(IndexedWidthToBytesError)
            .into_log::<Vec<_>, Vec<_>, Nothing<_>>()
            .map_errors(FloatWidthError::from)
            .and_then_commutative(|bytes| {
                if usize::from(u8::from(bytes)) == T::file_len() {
                    Self::from_range(range, flag)
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
            Self::F32(b) => MixedVec::F32(RangedVec::new(*b, vec![0.0; nrows])),
            Self::F64(b) => MixedVec::F64(RangedVec::new(*b, vec![0.0; nrows])),
        }
    }

    /// Make a new mixed range from $PnB and $PnR, and $PnDATATYPE values
    fn from_width_and_range(
        width: Width,
        range: TextRange,
        datatype: Option<NumType>,
        global_datatype: AlphaNumType,
        i: MeasIndex,
        flag: DisallowRangeTrunc,
    ) -> WarningsAndErrorsResult<ConvertedRange<Self>, (), NewMixedRangeWarning, NewMixedRangeError>
    {
        macro_rules! from {
            ($t:ident, $width:expr, $range:expr, $i:expr, $flag:expr) => {
                $t::from_width_and_range($width, $range, $i, $flag)
                    .map_ok_value(|x| x.fmap_once(Self::from))
                    .map_commutative_warnings(NewMixedRangeWarning::from)
                    .map_errors(NewMixedRangeError::from)
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

impl From<BitmaskValue<u64>> for VariableBitmask {
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

impl From<VariableBitmask> for BitmaskValue<u64> {
    /// Convert bitmask range (not bitmask itself) to u64.
    fn from(value: VariableBitmask) -> Self {
        match_any_uint!(value, x, Self(u64::from(x)))
    }
}

impl VariableBitmask {
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
        range: TextRange,
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
        range: TextRange,
        i: MeasIndex,
        flag: DisallowRangeTrunc,
    ) -> DeferredSwitchableError<ConvertedRange<Self>, DisallowRangeTrunc, IndexedBitmaskError>
    {
        macro_rules! go {
            ($t:ident) => {
                $t::from_range(range, flag).map_deferred_value(FunctorOnce::fmap_into_once)
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

// Misc functions used throughout module

pub(crate) fn ascii_to_uint(buf: &[u8]) -> Result<u64, AsciiToUintError> {
    if buf.is_ascii() {
        // SAFETY: we just checked that all bytes are ASCII
        let s = unsafe { str::from_utf8_unchecked(buf) };
        s.parse().map_err(AsciiToUintError::from)
    } else {
        Err(NotAsciiError(buf.to_vec()).into())
    }
}

// TODO put these in a more general place
pub(crate) fn u64_to_usize(x: u64) -> usize {
    usize::try_from(x).expect("overflow")
}

pub(crate) fn usize_to_u64(x: usize) -> u64 {
    u64::try_from(x).expect("overflow")
}

#[cfg(feature = "python")]
mod python {
    use super::{AnyUint, FloatRange, MixedRange, VariableBitmask};

    use crate::validated::finite_float::FiniteFloat;

    use fireflow_types::python::{ColumnType, IntegerWidth};

    use pyo3::prelude::*;
    use pyo3::types::PyTuple;

    impl<'py, T> FromPyObject<'py> for FloatRange<T>
    where
        FiniteFloat<T>: FromPyObject<'py>,
    {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            Ok(Self::new(ob.extract::<FiniteFloat<T>>()?))
        }
    }

    impl<'py> FromPyObject<'py> for VariableBitmask {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let (width, value): (IntegerWidth, Bound<'py, PyAny>) = ob.extract()?;
            let ret = match width {
                IntegerWidth::I08 => Self::Uint08(value.extract()?),
                IntegerWidth::I16 => Self::Uint16(value.extract()?),
                IntegerWidth::I24 => Self::Uint24(value.extract()?),
                IntegerWidth::I32 => Self::Uint32(value.extract()?),
                IntegerWidth::I40 => Self::Uint40(value.extract()?),
                IntegerWidth::I48 => Self::Uint48(value.extract()?),
                IntegerWidth::I56 => Self::Uint56(value.extract()?),
                IntegerWidth::I64 => Self::Uint64(value.extract()?),
            };
            Ok(ret)
        }
    }

    impl<'py> IntoPyObject<'py> for VariableBitmask {
        type Target = PyTuple;
        type Output = Bound<'py, PyTuple>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            match self {
                Self::Uint08(x) => (IntegerWidth::I08, x).into_pyobject(py),
                Self::Uint16(x) => (IntegerWidth::I16, x).into_pyobject(py),
                Self::Uint24(x) => (IntegerWidth::I24, x).into_pyobject(py),
                Self::Uint32(x) => (IntegerWidth::I32, x).into_pyobject(py),
                Self::Uint40(x) => (IntegerWidth::I40, x).into_pyobject(py),
                Self::Uint48(x) => (IntegerWidth::I48, x).into_pyobject(py),
                Self::Uint56(x) => (IntegerWidth::I56, x).into_pyobject(py),
                Self::Uint64(x) => (IntegerWidth::I64, x).into_pyobject(py),
            }
        }
    }

    impl<'py> FromPyObject<'py> for MixedRange {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let (ctype, value): (ColumnType, Bound<'py, PyAny>) = ob.extract()?;
            let ret = match ctype {
                ColumnType::A => Self::Ascii(value.extract()?),
                ColumnType::F => Self::F32(value.extract()?),
                ColumnType::D => Self::F64(value.extract()?),
                ColumnType::I08 => Self::Uint(AnyUint::Uint08(value.extract()?)),
                ColumnType::I16 => Self::Uint(AnyUint::Uint16(value.extract()?)),
                ColumnType::I24 => Self::Uint(AnyUint::Uint24(value.extract()?)),
                ColumnType::I32 => Self::Uint(AnyUint::Uint32(value.extract()?)),
                ColumnType::I40 => Self::Uint(AnyUint::Uint40(value.extract()?)),
                ColumnType::I48 => Self::Uint(AnyUint::Uint48(value.extract()?)),
                ColumnType::I56 => Self::Uint(AnyUint::Uint56(value.extract()?)),
                ColumnType::I64 => Self::Uint(AnyUint::Uint64(value.extract()?)),
            };
            Ok(ret)
        }
    }

    impl<'py> IntoPyObject<'py> for MixedRange {
        type Target = PyTuple;
        type Output = Bound<'py, PyTuple>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            match self {
                Self::Ascii(x) => (ColumnType::A, x).into_pyobject(py),
                Self::F32(x) => (ColumnType::F, x).into_pyobject(py),
                Self::F64(x) => (ColumnType::D, x).into_pyobject(py),
                Self::Uint(AnyUint::Uint08(x)) => (ColumnType::I08, x).into_pyobject(py),
                Self::Uint(AnyUint::Uint16(x)) => (ColumnType::I16, x).into_pyobject(py),
                Self::Uint(AnyUint::Uint24(x)) => (ColumnType::I24, x).into_pyobject(py),
                Self::Uint(AnyUint::Uint32(x)) => (ColumnType::I32, x).into_pyobject(py),
                Self::Uint(AnyUint::Uint40(x)) => (ColumnType::I40, x).into_pyobject(py),
                Self::Uint(AnyUint::Uint48(x)) => (ColumnType::I48, x).into_pyobject(py),
                Self::Uint(AnyUint::Uint56(x)) => (ColumnType::I56, x).into_pyobject(py),
                Self::Uint(AnyUint::Uint64(x)) => (ColumnType::I64, x).into_pyobject(py),
            }
        }
    }
}

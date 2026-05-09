//! Dataframe and series for encoding FCS DATA segment.
//!
//! Here we only need to worry about 6 external types (u8-64, f32, f64) and 10
//! internal types, (f32, f64, all unsigned int widths 1-8 bytes).

use crate::data::{CheckRange, EventOverRangeError, TruncatedResult};
use crate::match_many_to_one;
use crate::validated::unaligned::{U24, U40, U48, U56};

use ambassador::{Delegate, delegatable_trait};
use fireflow_types::config::CheckedRangeDatatypes;
use type_families::{FunctorOnce as _, impl_functor, impl_functor_once, impl_kind1};

use bytemuck::cast_vec;
use derive_more::{AsRef, Display, From, Into};
use derive_new::new;
use num_traits::{AsPrimitive, Bounded, Float};
use polars_arrow::buffer::Buffer;
use thiserror::Error;

use std::marker::PhantomData;
use std::mem;
use std::slice::{Iter, from_raw_parts};

#[cfg(feature = "python")]
use {fireflow_core_proc::DisplayAsPyErr, fireflow_types::python as py};

/// Dataframe composed of allowed primitive types.
pub type PrimitiveDataFrame = DataFrame<AnyPrimitiveSeries>;

/// Column-major dataframe to represent events in DATA
///
/// This is a very light wrapper around a polars buffer which is ref-counted and
/// therefore allows us to return event to external interfaces without copying
/// memory. It is validated to contain no NULL values where all series have the
/// same length.
#[derive(Clone, PartialEq, AsRef, Into, new)]
#[new(visibility = "")]
pub struct DataFrame<C> {
    #[as_ref([C])]
    #[into]
    series: Vec<C>,
    nrows: usize,
}

impl_kind1!(
    #[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
    pub DataFrameFamily, DataFrame
);

impl_functor!(
    DataFrame,
    self,
    mut f,
    DataFrame::new(self.series.fmap(f), self.nrows)
);

impl<C> Default for DataFrame<C> {
    fn default() -> Self {
        Self::new(vec![], 0)
    }
}

/// Any valid series from [`DataFrame`]
#[derive(Clone, From, Delegate, PartialEq)]
#[delegate(HasLen)]
pub enum AnyPrimitiveSeries {
    U08(U08Series),
    U16(U16Series),
    U32(U32Series),
    U64(U64Series),
    F32(F32Series),
    F64(F64Series),
}

/// A generic series for [`DataFrame`]
#[derive(Clone, PartialEq, From, Into, AsRef)]
#[repr(transparent)]
#[as_ref([T])]
pub struct PrimitiveSeries<T>(pub Buffer<T>);

pub type U08Series = PrimitiveSeries<u8>;
pub type U16Series = PrimitiveSeries<u16>;
pub type U32Series = PrimitiveSeries<u32>;
pub type U64Series = PrimitiveSeries<u64>;
pub type F32Series = PrimitiveSeries<f32>;
pub type F64Series = PrimitiveSeries<f64>;

/// Internal series contain a native rust type and FCS type annotation.
///
/// The phantom type is to encode for types like u24 which store data as
/// u32 but are read/written to files in 24 bits.
#[derive(Clone, PartialEq, Into, AsRef, new)]
#[repr(transparent)]
#[new(visibility = "")]
pub struct InternalSeries<T, Raw> {
    #[into(PrimitiveSeries<T>)]
    #[into(Buffer<T>)]
    #[as_ref([T])]
    inner: Buffer<T>,
    _outer: PhantomData<Raw>,
}

impl<T, Raw> Default for InternalSeries<T, Raw> {
    fn default() -> Self {
        Self::new(Buffer::new())
    }
}

/// Error when building [`DataFrame`] from individual series
#[derive(Debug, Error)]
#[error("series lengths to not match")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct NewDataframeError;

/// Error when new series has number of rows which are not equal to that in [`DataFrame`]
#[derive(Debug, Error)]
#[error("series length ({col_len}) is different from number of rows in dataframe ({df_len})")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct SeriesLengthError {
    df_len: usize,
    col_len: usize,
}

/// Any internal series containing allowed internal types.
#[derive(Clone, From, Delegate)]
#[delegate(HasLen)]
pub(crate) enum AnyInternalSeries {
    U08(InternalU08Series),
    U16(InternalU16Series),
    U24(InternalU24Series),
    U32(InternalU32Series),
    U40(InternalU40Series),
    U48(InternalU48Series),
    U56(InternalU56Series),
    U64(InternalU64Series),
    F32(InternalF32Series),
    F64(InternalF64Series),
}

pub(crate) type InternalU08Series = InternalSeries<u8, u8>;
pub(crate) type InternalU16Series = InternalSeries<u16, u16>;
pub(crate) type InternalU24Series = InternalSeries<u32, U24>;
pub(crate) type InternalU32Series = InternalSeries<u32, u32>;
pub(crate) type InternalU40Series = InternalSeries<u64, U40>;
pub(crate) type InternalU48Series = InternalSeries<u64, U48>;
pub(crate) type InternalU56Series = InternalSeries<u64, U56>;
pub(crate) type InternalU64Series = InternalSeries<u64, u64>;
pub(crate) type InternalF32Series = InternalSeries<f32, f32>;
pub(crate) type InternalF64Series = InternalSeries<f64, f64>;

/// Error when casting one series type to another which results in loss.
#[derive(new, Debug, Error)]
#[error("could not cast series from {from_type} to {to_type}; failed at row {position}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::DataLossError))]
#[new(visibility(""))]
pub struct CastSeriesError {
    position: usize,
    from_type: FCSType,
    to_type: FCSType,
}

/// The result of a casting operation.
///
/// May have an error if the cast had data loss.
#[derive(new)]
pub(crate) struct CastSeriesResult<T> {
    inner: T,
    loss_error: Option<CastSeriesError>,
}

impl<T> CastSeriesResult<T> {
    pub(crate) fn into_result(self) -> Result<T, CastSeriesError> {
        self.loss_error.map_or(Ok(self.inner), Err)
    }
}

impl_kind1!(pub(crate) CastSeriesResultFamily, CastSeriesResult);

impl_functor_once!(
    CastSeriesResult,
    self,
    mut f,
    CastSeriesResult::new(f(self.inner), self.loss_error,)
);

/// The result of casting a value from one type to another.
///
/// Flag will be true if loss occured.
#[derive(new, Debug, PartialEq)]
pub(crate) struct CastValueResult<T> {
    inner: T,
    lossy: bool,
}

impl<T> CastValueResult<T> {
    fn new1(x: T) -> Self {
        Self::new(x, false)
    }

    pub(crate) fn lossless(self) -> Option<T> {
        (!self.lossy).then_some(self.inner)
    }
}

impl<F> CastValueResult<F> {
    fn int_to_float<I>(x: I) -> Self
    where
        I: PartialEq + AsPrimitive<F>,
        F: Float + AsPrimitive<I>,
    {
        let new_value: F = x.as_();
        let old_value: I = new_value.as_();
        Self::new(new_value, x != old_value)
    }
}

impl CastValueResult<f32> {
    fn f64_to_f32(x: f64) -> Self {
        let new_value: f32 = x.as_();
        let old_value = f64::from(new_value);
        Self::new(new_value, x.to_bits() != old_value.to_bits())
    }
}

impl<I> CastValueResult<I> {
    fn float_to_int<F>(x: F) -> Self
    where
        I: Bounded + AsPrimitive<F>,
        F: Float + AsPrimitive<I>,
    {
        Self::new(x.as_(), !float_is_uint::<F, I>(x))
    }
}

/// Any valid Rust numeric type which may be used in an [`DataFrame`]
#[derive(Clone, Copy, Debug, Display, PartialEq)]
pub(crate) enum FCSType {
    #[display("u8")]
    U08,
    #[display("u16")]
    U16,
    #[display("u24")]
    U24,
    #[display("u32")]
    U32,
    #[display("u40")]
    U40,
    #[display("u48")]
    U48,
    #[display("u56")]
    U56,
    #[display("u64")]
    U64,
    #[display("f32")]
    F32,
    #[display("f64")]
    F64,
}

// Implement as vec -> internal series
//
// Use cast_vec from bytemuck since the underlying data of the vector will
// always be a primitive type but may be wrapped as an FCS type of a smaller
// size. Cast should works since we don't need to touch the memory layout to
// convert.

macro_rules! impl_internal_from_vec {
    ($t:ident, $raw:ident) => {
        impl From<Vec<$raw>> for InternalSeries<$t, $raw> {
            fn from(value: Vec<$raw>) -> Self {
                Self::new(Buffer::from(cast_vec(value)))
            }
        }
    };
}

impl_internal_from_vec!(u8, u8);
impl_internal_from_vec!(u16, u16);
impl_internal_from_vec!(u32, U24);
impl_internal_from_vec!(u32, u32);
impl_internal_from_vec!(u64, U40);
impl_internal_from_vec!(u64, U48);
impl_internal_from_vec!(u64, U56);
impl_internal_from_vec!(u64, u64);
impl_internal_from_vec!(f32, f32);
impl_internal_from_vec!(f64, f64);

// Implement as ref for data stored in internal series
//
// AsRef for unaligned types needs an unsafe cast. This should be fine so long
// as the InternalSeries is sealed and maintains the coupling between the
// unaligned wrapper type and the primitive aligned type.
//
// Aligned types don't need this because of the AsRef impl on the InternalSeries
// type itself.

macro_rules! impl_internal_as_ref_unaligned {
    ($t:ident, $raw:ident) => {
        impl AsRef<[$raw]> for InternalSeries<$t, $raw> {
            fn as_ref(&self) -> &[$raw] {
                // SAFETY: the series is validated such that the primitive type
                // only contains values that are also valid as the target type
                unsafe { self.as_raw_slice() }
            }
        }
    };
}

impl_internal_as_ref_unaligned!(u32, U24);
impl_internal_as_ref_unaligned!(u64, U40);
impl_internal_as_ref_unaligned!(u64, U48);
impl_internal_as_ref_unaligned!(u64, U56);

// Implement value cast conversions

pub(crate) trait FromValue<From>: Sized {
    const LOSSLESS: bool;

    fn from_value(value: &From) -> CastValueResult<Self>;
}

macro_rules! impl_from_val_into {
    ($from:ident, $to:ident) => {
        impl FromValue<$from> for $to {
            const LOSSLESS: bool = true;
            fn from_value(value: &$from) -> CastValueResult<Self> {
                CastValueResult::new1((*value).into())
            }
        }
    };
}

macro_rules! impl_from_val_try_into {
    ($from:ident, $to:ident) => {
        impl FromValue<$from> for $to {
            const LOSSLESS: bool = false;
            fn from_value(value: &$from) -> CastValueResult<Self> {
                match (*value).try_into() {
                    Ok(x) => CastValueResult::new1(x),
                    Err(_) => CastValueResult::new(<$to as Bounded>::max_value(), true),
                }
            }
        }
    };
}

macro_rules! impl_from_val_int_to_float {
    ($from:ident, $inter:ident, $to:ident) => {
        impl FromValue<$from> for $to {
            const LOSSLESS: bool = false;
            fn from_value(value: &$from) -> CastValueResult<Self> {
                CastValueResult::int_to_float($inter::from(*value))
            }
        }
    };
}

macro_rules! impl_from_val_float_to_int {
    ($from:ident, $to:ident) => {
        impl FromValue<$from> for $to {
            const LOSSLESS: bool = false;
            fn from_value(value: &$from) -> CastValueResult<Self> {
                CastValueResult::float_to_int(*value)
            }
        }
    };
}

// U08; all targets are larger, so all conversions are lossless and don't
// require checks
impl_from_val_into!(u8, u8);
impl_from_val_into!(u8, u16);
impl_from_val_into!(u8, U24);
impl_from_val_into!(u8, u32);
impl_from_val_into!(u8, U40);
impl_from_val_into!(u8, U48);
impl_from_val_into!(u8, U56);
impl_from_val_into!(u8, u64);
impl_from_val_into!(u8, f32);
impl_from_val_into!(u8, f64);

// U16; all except u8 are larger, so no conversions require checks except for
// u16 -> u8
impl_from_val_try_into!(u16, u8);
impl_from_val_into!(u16, u16);
impl_from_val_into!(u16, U24);
impl_from_val_into!(u16, u32);
impl_from_val_into!(u16, U40);
impl_from_val_into!(u16, U48);
impl_from_val_into!(u16, U56);
impl_from_val_into!(u16, u64);
impl_from_val_into!(u16, f32);
impl_from_val_into!(u16, f64);

// U24; all except u8 and u16 are larger, so no conversion require checks except
// for u24 (really a u32) -> u8 or u16. u24 is actually a u32 internally and
// also a subset of u32, so u24 -> u32 is a noop. Also, u24 can perfectly fit
// within an f32 so this is also lossless and requires no checks.
impl_from_val_try_into!(U24, u8);
impl_from_val_try_into!(U24, u16);
impl_from_val_into!(U24, U24);
impl_from_val_into!(U24, u32); // target is larger
impl_from_val_into!(U24, U40);
impl_from_val_into!(U24, U48);
impl_from_val_into!(U24, U56);
impl_from_val_into!(U24, u64);
impl_from_val_into!(U24, f32);
impl_from_val_int_to_float!(U24, u32, f64);

// U32; requires the following special conversion logic:
//
// 1. -> u8/u16: these are smaller so check for loss
// 2. -> u24: this is the same type with a reduced range, so check for anything
//    over range and mutate in place without allocating new memory.
// 3. -> f32: anything larger than 2^24 will lose precision, so need to check
//
// u32 can perfectly fit in an f64 so this is lossless
impl_from_val_try_into!(u32, u8);
impl_from_val_try_into!(u32, u16);
impl_from_val_try_into!(u32, U24);
impl_from_val_into!(u32, u32);
impl_from_val_into!(u32, U40);
impl_from_val_into!(u32, U48);
impl_from_val_into!(u32, U56);
impl_from_val_into!(u32, u64);
impl_from_val_int_to_float!(u32, u32, f32);
impl_from_val_into!(u32, f64);

// U40; in general this is treated as a u64 when going to smaller types.
//
// Any of the larger integer types (48, 56, 64) are the same primitive type and
// have larger ranges, so these conversions are noops.
//
// f32 conversion requires similar checks to u32.
//
// f64 conversion is lossless since the upper integer limit of an f64 is 2^53.
impl_from_val_try_into!(U40, u8);
impl_from_val_try_into!(U40, u16);
impl_from_val_try_into!(U40, U24);
impl_from_val_try_into!(U40, u32);
impl_from_val_into!(U40, U40);
impl_from_val_into!(U40, U48); // target is larger
impl_from_val_into!(U40, U56); // target is larger
impl_from_val_into!(U40, u64); // target is larger
impl_from_val_int_to_float!(U40, u64, f32);
impl_from_val_into!(U40, f64);

// U48; This is the same as u40 except that u48 -> u40 is an in-place truncation
// and check since u40 is the same underlying type as u48 except with a smaller
// range.
impl_from_val_try_into!(U48, u8);
impl_from_val_try_into!(U48, u16);
impl_from_val_try_into!(U48, U24);
impl_from_val_try_into!(U48, u32);
impl_from_val_try_into!(U48, U40);
impl_from_val_into!(U48, U48);
impl_from_val_into!(U48, U56); // target is larger
impl_from_val_into!(U48, u64); // target is larger
impl_from_val_int_to_float!(U48, u64, f32);
impl_from_val_into!(U48, f64);

// U56; This is the same as u48 and u40, continuing the same pattern.
//
// The only other difference between this and u48 is that f64 conversion is no
// longer totally lossless, so this needs a precision check.
impl_from_val_try_into!(U56, u8);
impl_from_val_try_into!(U56, u16);
impl_from_val_try_into!(U56, U24);
impl_from_val_try_into!(U56, u32);
impl_from_val_try_into!(U56, U40);
impl_from_val_try_into!(U56, U48);
impl_from_val_into!(U56, U56);
impl_from_val_into!(U56, u64);
impl_from_val_int_to_float!(U56, u64, f32);
impl_from_val_int_to_float!(U56, u64, f64);

// U64; Generally the same as u56, continuing the same pattern
impl_from_val_try_into!(u64, u8);
impl_from_val_try_into!(u64, u16);
impl_from_val_try_into!(u64, U24);
impl_from_val_try_into!(u64, u32);
impl_from_val_try_into!(u64, U40);
impl_from_val_try_into!(u64, U48);
impl_from_val_try_into!(u64, U56);
impl_from_val_into!(u64, u64);
impl_from_val_int_to_float!(u64, u64, f32);
impl_from_val_int_to_float!(u64, u64, f64);

// F32; When converting to a primitive integer, this conversion requires a
// loss of precision check. When converting to an unaligned integer (u24, etc)
// this additionally requires a range check to ensure the integer value is not
// out of range.
//
// f32 -> f64 is lossless, see
// https://doc.rust-lang.org/reference/expressions/operator-expr.html#r-expr.as.numeric.float-widening
impl_from_val_float_to_int!(f32, u8);
impl_from_val_float_to_int!(f32, u16);
impl_from_val_float_to_int!(f32, U24);
impl_from_val_float_to_int!(f32, u32);
impl_from_val_float_to_int!(f32, U40);
impl_from_val_float_to_int!(f32, U48);
impl_from_val_float_to_int!(f32, U56);
impl_from_val_float_to_int!(f32, u64);
impl_from_val_into!(f32, f32);
impl_from_val_into!(f32, f64);

// F64; same as f32 except going from f64 to f32 requires a loss of precision
// check
impl_from_val_float_to_int!(f64, u8);
impl_from_val_float_to_int!(f64, u16);
impl_from_val_float_to_int!(f64, U24);
impl_from_val_float_to_int!(f64, u32);
impl_from_val_float_to_int!(f64, U40);
impl_from_val_float_to_int!(f64, U48);
impl_from_val_float_to_int!(f64, U56);
impl_from_val_float_to_int!(f64, u64);

impl FromValue<f64> for f32 {
    const LOSSLESS: bool = false;
    fn from_value(value: &f64) -> CastValueResult<Self> {
        CastValueResult::f64_to_f32(*value)
    }
}

impl_from_val_into!(f64, f64);

// Implement series cast conversions
//
// Use lots of macros for casting b/t series.
//
// This would be way easier with specialization. Alas, use total brute force to
// impl each conversion between each type. We have 6 types of primitive series
// and 10 types of internal series. The latter require bidirectional mapping in
// order to do in-place layout conversions. The former requires unidirectional
// mapping from primitive to internal which will be used for series insertion
// operations.
//
// All of these might be lossy since we might have a larger value being forced
// into a smaller size.
//
// Note that going from Internal to Primitive does not result in loss since we
// don't care what primitive type we end up with in this case and each internal
// series can map perfectly to one primitive colume.
//
// There are only three cases that need to be covered here since the logic of
// how to convert the values themselves is handled one layer down with another
// trait. For this layer, the main concern is performance. With that, the three
// cases are:
// 1. no-op: series' primitive types are the same and conversion is lossless
// 2. samesize mutation: primitive types are the same but conversion is lossy
// 3. different primitives: may/may not be lossy but target type is different
//
// 1. is zero-cost since it is a noop. 2. reuses the original vector since the
// primitives are the same. 3. requires a reallocation.

pub(crate) trait FromSeries<From>: Sized {
    fn from_series(col: From) -> CastSeriesResult<Self>;
}

/// Cast one series into another when underlying types are exactly the same.
macro_rules! impl_cast_col_noop {
    ($from:ident, $to:ident) => {
        impl FromSeries<$from> for $to {
            fn from_series(col: $from) -> CastSeriesResult<Self> {
                CastSeriesResult::new(Self::new(col.into()), None)
            }
        }
    };
}

/// Cast one series into another when first type converts losslessly to second.
macro_rules! impl_cast_col_into {
    ($from:ident, $to:ident) => {
        impl FromSeries<$from> for $to {
            fn from_series(col: $from) -> CastSeriesResult<Self> {
                cast_buffer(&col.into()).fmap_once(Self::new)
            }
        }
    };
}

/// Cast an integer series to a truncated series with the same internal type.
///
/// This is more optimal than a general cast because we don't need to reallocate
/// a new buffer; we only need to check the range of the current values and clip
/// if needed.
macro_rules! impl_truncate_from_samesize_int {
    ($from:ident, $to:ident) => {
        impl FromSeries<$from> for $to {
            fn from_series(col: $from) -> CastSeriesResult<Self> {
                Self::truncate_from_samesize_int(col.into())
            }
        }
    };
}

// U08

impl_cast_col_noop!(InternalU08Series, InternalU08Series);
impl_cast_col_into!(InternalU08Series, InternalU16Series);
impl_cast_col_into!(InternalU08Series, InternalU24Series);
impl_cast_col_into!(InternalU08Series, InternalU32Series);
impl_cast_col_into!(InternalU08Series, InternalU40Series);
impl_cast_col_into!(InternalU08Series, InternalU48Series);
impl_cast_col_into!(InternalU08Series, InternalU56Series);
impl_cast_col_into!(InternalU08Series, InternalU64Series);
impl_cast_col_into!(InternalU08Series, InternalF32Series);
impl_cast_col_into!(InternalU08Series, InternalF64Series);

impl_cast_col_noop!(U08Series, InternalU08Series);
impl_cast_col_into!(U08Series, InternalU16Series);
impl_cast_col_into!(U08Series, InternalU24Series);
impl_cast_col_into!(U08Series, InternalU32Series);
impl_cast_col_into!(U08Series, InternalU40Series);
impl_cast_col_into!(U08Series, InternalU48Series);
impl_cast_col_into!(U08Series, InternalU56Series);
impl_cast_col_into!(U08Series, InternalU64Series);
impl_cast_col_into!(U08Series, InternalF32Series);
impl_cast_col_into!(U08Series, InternalF64Series);

// U16

impl_cast_col_into!(InternalU16Series, InternalU08Series);
impl_cast_col_noop!(InternalU16Series, InternalU16Series);
impl_cast_col_into!(InternalU16Series, InternalU24Series);
impl_cast_col_into!(InternalU16Series, InternalU32Series);
impl_cast_col_into!(InternalU16Series, InternalU40Series);
impl_cast_col_into!(InternalU16Series, InternalU48Series);
impl_cast_col_into!(InternalU16Series, InternalU56Series);
impl_cast_col_into!(InternalU16Series, InternalU64Series);
impl_cast_col_into!(InternalU16Series, InternalF32Series);
impl_cast_col_into!(InternalU16Series, InternalF64Series);

impl_cast_col_into!(U16Series, InternalU08Series);
impl_cast_col_noop!(U16Series, InternalU16Series);
impl_cast_col_into!(U16Series, InternalU24Series);
impl_cast_col_into!(U16Series, InternalU32Series);
impl_cast_col_into!(U16Series, InternalU40Series);
impl_cast_col_into!(U16Series, InternalU48Series);
impl_cast_col_into!(U16Series, InternalU56Series);
impl_cast_col_into!(U16Series, InternalU64Series);
impl_cast_col_into!(U16Series, InternalF32Series);
impl_cast_col_into!(U16Series, InternalF64Series);

// U24

impl_cast_col_into!(InternalU24Series, InternalU08Series);
impl_cast_col_into!(InternalU24Series, InternalU16Series);
impl_cast_col_noop!(InternalU24Series, InternalU24Series);
impl_cast_col_noop!(InternalU24Series, InternalU32Series);
impl_cast_col_into!(InternalU24Series, InternalU40Series);
impl_cast_col_into!(InternalU24Series, InternalU48Series);
impl_cast_col_into!(InternalU24Series, InternalU56Series);
impl_cast_col_into!(InternalU24Series, InternalU64Series);
impl_cast_col_into!(InternalU24Series, InternalF32Series);
impl_cast_col_into!(InternalU24Series, InternalF64Series);

// U32

impl_cast_col_into!(InternalU32Series, InternalU08Series);
impl_cast_col_into!(InternalU32Series, InternalU16Series);
impl_truncate_from_samesize_int!(InternalU32Series, InternalU24Series);
impl_cast_col_noop!(InternalU32Series, InternalU32Series);
impl_cast_col_into!(InternalU32Series, InternalU40Series);
impl_cast_col_into!(InternalU32Series, InternalU48Series);
impl_cast_col_into!(InternalU32Series, InternalU56Series);
impl_cast_col_into!(InternalU32Series, InternalU64Series);
impl_cast_col_into!(InternalU32Series, InternalF32Series);
impl_cast_col_into!(InternalU32Series, InternalF64Series);

impl_cast_col_into!(U32Series, InternalU08Series);
impl_cast_col_into!(U32Series, InternalU16Series);
impl_truncate_from_samesize_int!(U32Series, InternalU24Series);
impl_cast_col_noop!(U32Series, InternalU32Series);
impl_cast_col_into!(U32Series, InternalU40Series);
impl_cast_col_into!(U32Series, InternalU48Series);
impl_cast_col_into!(U32Series, InternalU56Series);
impl_cast_col_into!(U32Series, InternalU64Series);
impl_cast_col_into!(U32Series, InternalF32Series);
impl_cast_col_into!(U32Series, InternalF64Series);

// U40

impl_cast_col_into!(InternalU40Series, InternalU08Series);
impl_cast_col_into!(InternalU40Series, InternalU16Series);
impl_cast_col_into!(InternalU40Series, InternalU24Series);
impl_cast_col_into!(InternalU40Series, InternalU32Series);
impl_cast_col_noop!(InternalU40Series, InternalU40Series);
impl_cast_col_noop!(InternalU40Series, InternalU48Series);
impl_cast_col_noop!(InternalU40Series, InternalU56Series);
impl_cast_col_noop!(InternalU40Series, InternalU64Series);
impl_cast_col_into!(InternalU40Series, InternalF32Series);
impl_cast_col_into!(InternalU40Series, InternalF64Series);

// U48

impl_cast_col_into!(InternalU48Series, InternalU08Series);
impl_cast_col_into!(InternalU48Series, InternalU16Series);
impl_cast_col_into!(InternalU48Series, InternalU24Series);
impl_cast_col_into!(InternalU48Series, InternalU32Series);
impl_truncate_from_samesize_int!(InternalU48Series, InternalU40Series);
impl_cast_col_noop!(InternalU48Series, InternalU48Series);
impl_cast_col_noop!(InternalU48Series, InternalU56Series);
impl_cast_col_noop!(InternalU48Series, InternalU64Series);
impl_cast_col_into!(InternalU48Series, InternalF32Series);
impl_cast_col_into!(InternalU48Series, InternalF64Series);

// U56

impl_cast_col_into!(InternalU56Series, InternalU08Series);
impl_cast_col_into!(InternalU56Series, InternalU16Series);
impl_cast_col_into!(InternalU56Series, InternalU24Series);
impl_cast_col_into!(InternalU56Series, InternalU32Series);
impl_truncate_from_samesize_int!(InternalU56Series, InternalU40Series);
impl_truncate_from_samesize_int!(InternalU56Series, InternalU48Series);
impl_cast_col_noop!(InternalU56Series, InternalU56Series);
impl_cast_col_noop!(InternalU56Series, InternalU64Series);
impl_cast_col_into!(InternalU56Series, InternalF32Series);
impl_cast_col_into!(InternalU56Series, InternalF64Series);

// U64

impl_cast_col_into!(InternalU64Series, InternalU08Series);
impl_cast_col_into!(InternalU64Series, InternalU16Series);
impl_cast_col_into!(InternalU64Series, InternalU24Series);
impl_cast_col_into!(InternalU64Series, InternalU32Series);
impl_truncate_from_samesize_int!(InternalU64Series, InternalU40Series);
impl_truncate_from_samesize_int!(InternalU64Series, InternalU48Series);
impl_truncate_from_samesize_int!(InternalU64Series, InternalU56Series);
impl_cast_col_noop!(InternalU64Series, InternalU64Series);
impl_cast_col_into!(InternalU64Series, InternalF32Series);
impl_cast_col_into!(InternalU64Series, InternalF64Series);

impl_cast_col_into!(U64Series, InternalU08Series);
impl_cast_col_into!(U64Series, InternalU16Series);
impl_cast_col_into!(U64Series, InternalU24Series);
impl_cast_col_into!(U64Series, InternalU32Series);
impl_truncate_from_samesize_int!(U64Series, InternalU40Series);
impl_truncate_from_samesize_int!(U64Series, InternalU48Series);
impl_truncate_from_samesize_int!(U64Series, InternalU56Series);
impl_cast_col_noop!(U64Series, InternalU64Series);
impl_cast_col_into!(U64Series, InternalF32Series);
impl_cast_col_into!(U64Series, InternalF64Series);

// F32

impl_cast_col_into!(InternalF32Series, InternalU08Series);
impl_cast_col_into!(InternalF32Series, InternalU16Series);
impl_cast_col_into!(InternalF32Series, InternalU24Series);
impl_cast_col_into!(InternalF32Series, InternalU32Series);
impl_cast_col_into!(InternalF32Series, InternalU40Series);
impl_cast_col_into!(InternalF32Series, InternalU48Series);
impl_cast_col_into!(InternalF32Series, InternalU56Series);
impl_cast_col_into!(InternalF32Series, InternalU64Series);
impl_cast_col_noop!(InternalF32Series, InternalF32Series);
impl_cast_col_into!(InternalF32Series, InternalF64Series);

impl_cast_col_into!(F32Series, InternalU08Series);
impl_cast_col_into!(F32Series, InternalU16Series);
impl_cast_col_into!(F32Series, InternalU24Series);
impl_cast_col_into!(F32Series, InternalU32Series);
impl_cast_col_into!(F32Series, InternalU40Series);
impl_cast_col_into!(F32Series, InternalU48Series);
impl_cast_col_into!(F32Series, InternalU56Series);
impl_cast_col_into!(F32Series, InternalU64Series);
impl_cast_col_noop!(F32Series, InternalF32Series);
impl_cast_col_into!(F32Series, InternalF64Series);

// F64

impl_cast_col_into!(InternalF64Series, InternalU08Series);
impl_cast_col_into!(InternalF64Series, InternalU16Series);
impl_cast_col_into!(InternalF64Series, InternalU24Series);
impl_cast_col_into!(InternalF64Series, InternalU32Series);
impl_cast_col_into!(InternalF64Series, InternalU40Series);
impl_cast_col_into!(InternalF64Series, InternalU48Series);
impl_cast_col_into!(InternalF64Series, InternalU56Series);
impl_cast_col_into!(InternalF64Series, InternalU64Series);
impl_cast_col_into!(InternalF64Series, InternalF32Series);
impl_cast_col_noop!(InternalF64Series, InternalF64Series);

impl_cast_col_into!(F64Series, InternalU08Series);
impl_cast_col_into!(F64Series, InternalU16Series);
impl_cast_col_into!(F64Series, InternalU24Series);
impl_cast_col_into!(F64Series, InternalU32Series);
impl_cast_col_into!(F64Series, InternalU40Series);
impl_cast_col_into!(F64Series, InternalU48Series);
impl_cast_col_into!(F64Series, InternalU56Series);
impl_cast_col_into!(F64Series, InternalU64Series);
impl_cast_col_into!(F64Series, InternalF32Series);
impl_cast_col_noop!(F64Series, InternalF64Series);

impl<T> FromSeries<AnyPrimitiveSeries> for T
where
    T: FromSeries<U08Series>
        + FromSeries<U16Series>
        + FromSeries<U32Series>
        + FromSeries<U64Series>
        + FromSeries<F32Series>
        + FromSeries<F64Series>,
{
    fn from_series(col: AnyPrimitiveSeries) -> CastSeriesResult<Self> {
        match_many_to_one!(
            col,
            AnyPrimitiveSeries,
            [U08, U16, U32, U64, F32, F64],
            x,
            FromSeries::from_series(x)
        )
    }
}

fn cast_buffer<X, Y>(buf: &Buffer<X>) -> CastSeriesResult<Buffer<Y>>
where
    Buffer<Y>: From<Vec<Y>>,
    Y: FromValue<X> + HasFCSType,
    X: HasFCSType,
{
    let mut err = None;
    let new = buf
        .iter()
        .enumerate()
        .map(|(i, x)| {
            let res = FromValue::from_value(x);
            if res.lossy {
                err = Some(CastSeriesError::new(i, X::TYPE, Y::TYPE));
            }
            res.inner
        })
        .collect();
    CastSeriesResult::new(Buffer::from(new), err)
}

fn cast_buffer_samesize<X, Y>(buf: Buffer<X>) -> CastSeriesResult<Buffer<X>>
where
    X: Copy + HasFCSType,
    Y: FromValue<X> + Into<X> + HasFCSType,
    Buffer<X>: From<Vec<X>>,
{
    let mut err = None;
    let mut inner = buf.make_mut();
    for (i, x) in inner.iter_mut().enumerate() {
        let res = Y::from_value(x);
        if res.lossy {
            err = Some(CastSeriesError::new(i, X::TYPE, Y::TYPE));
        }
        *x = res.inner.into();
    }
    CastSeriesResult::new(Buffer::from(inner), err)
}

// Implement width property for types used as columns in dataframe.

#[delegatable_trait]
pub trait HasWidth {
    #[allow(clippy::len_without_is_empty)]
    fn width(&self) -> usize;

    fn clear(&mut self);
}

impl<T> HasWidth for Vec<T> {
    fn width(&self) -> usize {
        self.len()
    }

    fn clear(&mut self) {
        self.clear();
    }
}

impl<T> HasWidth for DataFrame<T> {
    fn width(&self) -> usize {
        self.series.len()
    }

    fn clear(&mut self) {
        self.series.clear();
    }
}

// Implement length property for various useful things with length

#[delegatable_trait]
#[allow(clippy::len_without_is_empty)]
pub trait HasLen {
    // this will be used for vectors, len is always constant
    fn len(&self) -> usize;
}

impl<T> HasLen for PrimitiveSeries<T> {
    fn len(&self) -> usize {
        self.0.len()
    }
}

impl<T, R> HasLen for InternalSeries<T, R> {
    fn len(&self) -> usize {
        self.inner.len()
    }
}

impl<T> HasLen for Vec<T> {
    #[allow(clippy::use_self)]
    fn len(&self) -> usize {
        Vec::len(self)
    }
}

impl<T> HasLen for &[T] {
    fn len(&self) -> usize {
        self[..].len()
    }
}

// Implement FCS type annotation for underlying numeric types.
//
// This is used for displaying useful type info in errors

pub(crate) trait HasFCSType {
    const TYPE: FCSType;
}

macro_rules! impl_has_fcs_type {
    ($t:ident, $var:ident) => {
        impl HasFCSType for $t {
            const TYPE: FCSType = FCSType::$var;
        }
    };
}

impl_has_fcs_type!(u8, U08);
impl_has_fcs_type!(u16, U16);
impl_has_fcs_type!(U24, U24);
impl_has_fcs_type!(u32, U32);
impl_has_fcs_type!(U40, U40);
impl_has_fcs_type!(U48, U48);
impl_has_fcs_type!(U56, U56);
impl_has_fcs_type!(u64, U64);
impl_has_fcs_type!(f32, F32);
impl_has_fcs_type!(f64, F64);

// Implement prim<->internal conversions

impl From<AnyInternalSeries> for AnyPrimitiveSeries {
    fn from(value: AnyInternalSeries) -> Self {
        match value {
            AnyInternalSeries::U08(c) => Self::U08(PrimitiveSeries(c.inner)),
            AnyInternalSeries::U16(c) => Self::U16(PrimitiveSeries(c.inner)),
            AnyInternalSeries::U24(c) => Self::U32(PrimitiveSeries(c.inner)),
            AnyInternalSeries::U32(c) => Self::U32(PrimitiveSeries(c.inner)),
            AnyInternalSeries::U40(c) => Self::U64(PrimitiveSeries(c.inner)),
            AnyInternalSeries::U48(c) => Self::U64(PrimitiveSeries(c.inner)),
            AnyInternalSeries::U56(c) => Self::U64(PrimitiveSeries(c.inner)),
            AnyInternalSeries::U64(c) => Self::U64(PrimitiveSeries(c.inner)),
            AnyInternalSeries::F32(c) => Self::F32(PrimitiveSeries(c.inner)),
            AnyInternalSeries::F64(c) => Self::F64(PrimitiveSeries(c.inner)),
        }
    }
}

impl<T, R: HasFCSType> TryFrom<AnyPrimitiveSeries> for InternalSeries<T, R>
where
    Self: FromSeries<U08Series>
        + FromSeries<U16Series>
        + FromSeries<U32Series>
        + FromSeries<U64Series>
        + FromSeries<F32Series>
        + FromSeries<F64Series>,
{
    type Error = CastSeriesError;
    fn try_from(value: AnyPrimitiveSeries) -> Result<Self, Self::Error> {
        let ret = match value {
            AnyPrimitiveSeries::U08(c) => Self::from_series(c),
            AnyPrimitiveSeries::U16(c) => Self::from_series(c),
            AnyPrimitiveSeries::U32(c) => Self::from_series(c),
            AnyPrimitiveSeries::U64(c) => Self::from_series(c),
            AnyPrimitiveSeries::F32(c) => Self::from_series(c),
            AnyPrimitiveSeries::F64(c) => Self::from_series(c),
        };
        ret.into_result()
    }
}

// Implement misc methods

impl AnyPrimitiveSeries {
    #[must_use]
    pub fn len(&self) -> usize {
        match_many_to_one!(self, Self, [U08, U16, U32, U64, F32, F64], x, { x.0.len() })
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[cfg(feature = "serde")]
    #[must_use]
    pub fn as_u64(&self, i: usize) -> u64 {
        match self {
            Self::U08(x) => x.0[i].into(),
            Self::U16(x) => x.0[i].into(),
            Self::U32(x) => x.0[i].into(),
            Self::U64(x) => x.0[i],
            Self::F32(x) => x.0[i].to_bits().into(),
            Self::F64(x) => x.0[i].to_bits(),
        }
    }

    pub(crate) fn will_be_lossy<X>(&self) -> Result<(), CastSeriesError>
    where
        X: FromValue<u8>
            + FromValue<u16>
            + FromValue<u32>
            + FromValue<u64>
            + FromValue<f32>
            + FromValue<f64>
            + HasFCSType,
    {
        match self {
            Self::U08(x) => x.will_be_lossy::<X>(),
            Self::U16(x) => x.will_be_lossy::<X>(),
            Self::U32(x) => x.will_be_lossy::<X>(),
            Self::U64(x) => x.will_be_lossy::<X>(),
            Self::F32(x) => x.will_be_lossy::<X>(),
            Self::F64(x) => x.will_be_lossy::<X>(),
        }
    }
}

impl<T> PrimitiveSeries<T> {
    pub(crate) fn will_be_lossy<X>(&self) -> Result<(), CastSeriesError>
    where
        X: FromValue<T> + HasFCSType,
        T: HasFCSType,
    {
        if <X as FromValue<T>>::LOSSLESS {
            Ok(())
        } else {
            self.as_ref()
                .iter()
                .position(|x| X::from_value(x).lossy)
                .map_or(Ok(()), |i| Err(CastSeriesError::new(i, T::TYPE, X::TYPE)))
        }
    }
}

impl<C> DataFrame<C> {
    pub fn try_new(series: impl IntoIterator<Item = C>) -> Result<Self, NewDataframeError>
    where
        C: HasLen,
    {
        let mut it = series.into_iter();
        if let Some(c0) = it.by_ref().next() {
            let nrows = c0.len();
            let mut cs = vec![c0];
            for c in it {
                if c.len() != nrows {
                    return Err(NewDataframeError);
                }
                cs.push(c);
            }
            Ok(Self::new(cs, nrows))
        } else {
            Ok(Self::default())
        }
    }

    pub(crate) fn new_unchecked(series: impl IntoIterator<Item = C>) -> Self
    where
        C: HasLen,
    {
        Self::try_new(series).expect("caller should ensure columns are all same length")
    }

    #[must_use]
    pub fn new1(series: C) -> Self
    where
        C: HasLen,
    {
        Self {
            nrows: series.len(),
            series: vec![series],
        }
    }

    pub fn clear(&mut self) {
        self.series = Vec::default();
        self.nrows = 0;
    }

    #[allow(clippy::iter_without_into_iter)]
    pub fn iter(&self) -> Iter<'_, C> {
        self.series.iter()
    }

    pub(crate) fn check_ranges(&self, check: CheckedRangeDatatypes) -> Vec<EventOverRangeError>
    where
        C: CheckRange,
    {
        self.series
            .iter()
            .enumerate()
            .filter_map(|(i, c)| c.check_range(i.into(), check).err())
            .collect()
    }

    pub(crate) fn check_ranges_mut(
        &mut self,
        check: CheckedRangeDatatypes,
        trunc: bool,
    ) -> Vec<Option<TruncatedResult>>
    where
        C: CheckRange,
    {
        self.series
            .iter_mut()
            .enumerate()
            .map(|(i, c)| c.check_range_mut(i.into(), check, trunc))
            .collect()
    }

    #[must_use]
    pub fn nrows(&self) -> usize {
        self.nrows_nonempty().unwrap_or(0)
    }

    #[must_use]
    pub fn nrows_nonempty(&self) -> Option<usize> {
        if self.is_empty() {
            None
        } else {
            Some(self.nrows)
        }
    }

    #[must_use]
    pub fn ncols(&self) -> usize {
        self.series.len()
    }

    #[must_use]
    pub fn size(&self) -> u64 {
        u64::try_from(self.ncols() * self.nrows()).expect("cells in dataframe exceed 2^64")
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.ncols() == 0
    }

    pub(crate) fn remove(&mut self, i: usize) -> C {
        self.series.remove(i)
    }

    pub(crate) fn push_series_nocheck(&mut self, col: C)
    where
        C: HasLen,
    {
        debug_assert!(
            self.check_new_series(&col).is_ok(),
            "new series length differs from number of rows"
        );
        if self.is_empty() {
            *self = Self::new1(col);
        } else {
            self.series.push(col);
        }
    }

    // will panic if index is out of bounds
    pub(crate) fn insert_series_nocheck(&mut self, i: usize, col: C)
    where
        C: HasLen,
    {
        debug_assert!(
            self.check_new_series(&col).is_ok(),
            "new series length differs from number of rows"
        );
        if self.is_empty() {
            self.nrows = col.len();
        }
        // don't use Self::new1 here since we want to panic if i is out of
        // bounds
        self.series.insert(i, col);
    }

    pub(crate) fn check_new_series(&self, col: &C) -> Result<(), SeriesLengthError>
    where
        C: HasLen,
    {
        if let Some(df_len) = self.nrows_nonempty() {
            let col_len = col.len();
            if col_len != df_len {
                return Err(SeriesLengthError { df_len, col_len });
            }
        }
        Ok(())
    }
}

impl<T, Raw> InternalSeries<T, Raw> {
    /// Return reference to underlying data, cast in terms of raw type.
    ///
    /// # Safety
    ///
    /// The caller must ensure that all the values in the series are valid
    /// bit configurations when cast into the raw type.
    unsafe fn as_raw_slice(&self) -> &[Raw] {
        debug_assert!(size_of::<T>() == size_of::<Raw>(), "type sizes don't match");
        let xs = self.inner.as_ref();
        let p = xs.as_ptr().cast::<Raw>();
        let n = xs.len();
        // SAFETY: T and Raw are assumed to have the same layout and size
        unsafe { from_raw_parts(p, n) }
    }

    pub(crate) fn truncate<F>(&mut self, f: F) -> Option<usize>
    where
        T: Copy + PartialOrd,
        F: Fn(T) -> Option<T>,
    {
        let mut xs = mem::take(&mut self.inner).make_mut();
        let mut j = None;
        for (rowi, x) in xs.iter_mut().enumerate() {
            if let Some(u) = f(*x) {
                if j.is_none() {
                    j = Some(rowi);
                }
                *x = u;
            }
        }
        self.inner = Buffer::from(xs);
        j
    }

    fn truncate_from_samesize_int(buf: Buffer<T>) -> CastSeriesResult<Self>
    where
        Raw: FromValue<T> + Into<T> + HasFCSType,
        T: Copy + HasFCSType,
    {
        cast_buffer_samesize::<_, Raw>(buf).fmap_once(Self::new)
    }
}

fn float_is_uint<F: Float + 'static, I: Bounded + AsPrimitive<F>>(x: F) -> bool {
    let upper: F = I::max_value().as_();
    !x.is_nan() && !x.is_infinite() && !x.is_sign_negative() && x.fract().is_zero() && x <= upper
}

// TODO this seems like a good place for property testing
// (https://github.com/proptest-rs/proptest)
#[cfg(test)]
mod tests {
    use core::f32;

    use super::*;

    // only test lossy cases, assume the others will simply noop

    #[test]
    fn u16_to_u8() {
        assert!(!u8::from_value(&1_u16).lossy);
        assert_eq!(u8::from_value(&0x100_u16), CastValueResult::new(0xFF, true));
    }

    #[test]
    fn u32_to_u8() {
        assert!(!u8::from_value(&1_u32).lossy);
        assert_eq!(u8::from_value(&0x100_u32), CastValueResult::new(0xFF, true));
    }

    #[test]
    fn u64_to_u8() {
        assert!(!u8::from_value(&1_u64).lossy);
        assert_eq!(u8::from_value(&0x100_u64), CastValueResult::new(0xFF, true));
    }

    #[test]
    fn u32_to_u16() {
        assert!(!u16::from_value(&1_u32).lossy);
        assert_eq!(
            u16::from_value(&0x0001_0000_u32),
            CastValueResult::new(0xFFFF, true)
        );
    }

    #[test]
    fn u64_to_u16() {
        assert!(!u16::from_value(&1_u64).lossy);
        assert_eq!(
            u16::from_value(&0x0001_0000_u64),
            CastValueResult::new(0xFFFF, true)
        );
    }

    #[test]
    fn u64_to_u32() {
        assert!(!u32::from_value(&1_u64).lossy);
        assert_eq!(
            u32::from_value(&0x0001_0000_0000_u64),
            CastValueResult::new(0xFFFF_FFFF, true)
        );
    }

    // uint should map exactly to f32 if less than 2^24, above this it will
    // start rounding to nearest even number (and beyond as we get higher)

    #[test]
    fn u32_to_f32() {
        assert_eq!(f32::from_value(&1_u32), CastValueResult::new(1.0, false));
        assert_eq!(
            f32::from_value(&0x0100_0000_u32),
            CastValueResult::new(16_777_216.0, false)
        );
        assert_eq!(
            f32::from_value(&0x0100_0001_u32),
            CastValueResult::new(16_777_216.0, true)
        );
        assert_eq!(
            f32::from_value(&0x0100_0002_u32),
            CastValueResult::new(16_777_218.0, false)
        );
    }

    #[test]
    fn u64_to_f32() {
        assert_eq!(
            f32::from_value(&1_u64),
            CastValueResult::new(1.0_f32, false)
        );
        assert_eq!(
            f32::from_value(&0x0100_0000_u64),
            CastValueResult::new(16_777_216.0_f32, false)
        );
        assert_eq!(
            f32::from_value(&0x0100_0001_u64),
            CastValueResult::new(16_777_216.0_f32, true)
        );
        assert_eq!(
            f32::from_value(&0x0100_0002_u64),
            CastValueResult::new(16_777_218.0_f32, false)
        );
    }

    // uint should map exactly to f64 if less than 2^53, above this it will
    // start rounding to nearest even number (and beyond as we get higher)

    #[test]
    fn u64_to_f64() {
        assert_eq!(
            f64::from_value(&1_u64),
            CastValueResult::new(1.0_f64, false)
        );
        assert_eq!(
            f64::from_value(&0x0020_0000_0000_0000_u64),
            CastValueResult::new(9_007_199_254_740_992.0_f64, false)
        );
        assert_eq!(
            f64::from_value(&0x0020_0000_0000_0001_u64),
            CastValueResult::new(9_007_199_254_740_992.0_f64, true)
        );
        assert_eq!(
            f64::from_value(&0x0020_0000_0000_0002_u64),
            CastValueResult::new(9_007_199_254_740_994.0_f64, false)
        );
    }

    macro_rules! test_float_to_int {
        ($float:ident, $int:ident) => {
            let zero: $float = 0.0;
            let nonzero: $float = 1.5;
            let neg: $float = -1.0;

            assert_eq!($int::from_value(&zero), CastValueResult::new(0, false));
            #[allow(clippy::cast_possible_truncation)]
            #[allow(clippy::cast_precision_loss)]
            #[allow(clippy::cast_lossless)]
            #[allow(clippy::as_conversions)]
            let x = $int::from_value(&($int::MAX as $float));
            assert_eq!(x, CastValueResult::new($int::MAX, false));
            assert_eq!($int::from_value(&nonzero), CastValueResult::new(1, true));
            assert_eq!($int::from_value(&neg), CastValueResult::new(0, true));
            assert_eq!(
                $int::from_value(&$float::NAN),
                CastValueResult::new(0, true)
            );
            assert_eq!(
                $int::from_value(&$float::NEG_INFINITY),
                CastValueResult::new(0, true)
            );
            assert_eq!(
                $int::from_value(&$float::INFINITY),
                CastValueResult::new($int::MAX, true)
            );
        };
    }

    #[test]
    fn f32_to_u8() {
        test_float_to_int!(f32, u8);
    }

    #[test]
    fn f32_to_u16() {
        test_float_to_int!(f32, u16);
    }

    #[test]
    fn f32_to_u32() {
        test_float_to_int!(f32, u32);
    }

    #[test]
    fn f32_to_u64() {
        test_float_to_int!(f32, u64);
    }

    #[test]
    fn f64_to_u8() {
        test_float_to_int!(f64, u8);
    }

    #[test]
    fn f64_to_u16() {
        test_float_to_int!(f64, u16);
    }

    #[test]
    fn f64_to_u32() {
        test_float_to_int!(f64, u32);
    }

    #[test]
    fn f64_to_u64() {
        test_float_to_int!(f64, u64);
    }

    #[test]
    fn f64_to_f32() {
        // this should obviously pass
        assert_eq!(f32::from_value(&0.0_f64), CastValueResult::new(0.0, false));
        // this is the upper limit of ints that an f32 can represent exactly,
        // going above this will start to induce rounding errors that don't
        // happen in f64
        assert_eq!(
            f32::from_value(&16_777_216.0_f64),
            CastValueResult::new(16_777_216.0, false)
        );
        assert_eq!(
            f32::from_value(&16_777_217.0_f64),
            CastValueResult::new(16_777_216.0, true)
        );
        // this is a decimal that can be represented perfectly in both
        assert_eq!(f32::from_value(&0.5_f64), CastValueResult::new(0.5, false));
        // this is a repeating decimal which will have different representations
        // in f32 and f64, thus it will be lossy
        assert_eq!(f32::from_value(&0.2_f64), CastValueResult::new(0.2, true));
    }
}

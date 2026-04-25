use crate::data::{CheckRange, TruncatedResult};
use crate::macros::match_many_to_one;
use crate::validated::ascii_range::Chars;
use crate::validated::unaligned::{U24, U40, U48, U56};

use ambassador::{Delegate, delegatable_trait};
use fireflow_types::config::{CheckEventRanges, TruncateEventValues};
use type_families::{FunctorOnce as _, impl_functor, impl_functor_once, impl_kind1};

use bytemuck::{AnyBitPattern, NoUninit, cast_vec};
use derive_more::{AsRef, Display, From, Into};
use derive_new::new;
use num_traits::bounds::Bounded;
use num_traits::cast::AsPrimitive;
use num_traits::float::Float;
use num_traits::identities::Zero as _;
use polars_arrow::buffer::Buffer;
use thiserror::Error;

use std::iter;
use std::marker::PhantomData;
use std::mem;
use std::slice::{Iter, from_raw_parts};

#[cfg(feature = "python")]
use {fireflow_core_proc::DisplayAsPyErr, fireflow_types::python as py};

/// Column-major dataframe to represent events in DATA
///
/// This is a very light wrapper around a polars buffer which is ref-counted and
/// therefore allows us to return event to external interfaces without copying
/// memory. It is validated to contain no NULL values where all series have the
/// same length.
pub type PrimitiveDataFrame = DataFrame<AnyPrimitiveSeries>;

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

impl<T, R> TryFrom<AnyPrimitiveSeries> for InternalSeries<T, R>
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
            AnyPrimitiveSeries::U08(c) => InternalSeries::from_series(c),
            AnyPrimitiveSeries::U16(c) => InternalSeries::from_series(c),
            AnyPrimitiveSeries::U32(c) => InternalSeries::from_series(c),
            AnyPrimitiveSeries::U64(c) => InternalSeries::from_series(c),
            AnyPrimitiveSeries::F32(c) => InternalSeries::from_series(c),
            AnyPrimitiveSeries::F64(c) => InternalSeries::from_series(c),
        };
        ret.into_err()
    }
}

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

/// Any valid series from [`FCSDataFrame`]
#[derive(Clone, From, Delegate)]
#[delegate(HasLen)]
pub enum AnyPrimitiveSeries {
    U08(U08Series),
    U16(U16Series),
    U32(U32Series),
    U64(U64Series),
    F32(F32Series),
    F64(F64Series),
}

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

/// A generic series for [`FCSDataFrame`]
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

#[derive(Clone, PartialEq, Into, AsRef, new)]
#[repr(transparent)]
#[new(visibility = "")]
pub(crate) struct InternalSeries<T, Raw> {
    #[into(PrimitiveSeries<T>)]
    #[into(Buffer<T>)]
    #[as_ref([T])]
    inner: Buffer<T>,
    _outer: PhantomData<Raw>,
}

impl<T, Raw> Default for InternalSeries<T, Raw> {
    fn default() -> Self {
        InternalSeries::new(Buffer::new())
    }
}

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
                let xs = self.inner.as_ref();
                // SAFETY: primitive type in an internal series should be within
                // the range of the raw type
                unsafe { from_raw_parts(xs.as_ptr() as *const $raw, xs.len()) }
            }
        }
    };
}

impl_internal_as_ref_unaligned!(u32, U24);
impl_internal_as_ref_unaligned!(u64, U40);
impl_internal_as_ref_unaligned!(u64, U48);
impl_internal_as_ref_unaligned!(u64, U56);

impl<T, Raw> InternalSeries<T, Raw> {
    fn as_raw_slice(&self) -> &[Raw] {
        debug_assert!(size_of::<T>() == size_of::<Raw>(), "type sizes don't match");
        // SAFETY: T and Raw are assumed to have the same layout and size
        unsafe { &*self.inner.as_ref().as_ptr().cast::<&[Raw]>() }
    }

    pub(crate) fn truncate(&mut self, upper: Raw) -> Option<usize>
    where
        T: Copy + PartialOrd,
        Raw: Into<T>,
    {
        let mut xs = mem::take(&mut self.inner).make_mut();
        let mut j = None;
        let u: T = upper.into();
        for (rowi, x) in xs.iter_mut().enumerate() {
            if *x > u {
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
        T: Copy + PartialOrd + From<Raw>,
        Raw: Bounded,
    {
        let go = |x: T| {
            let upper = T::from(Raw::max_value());
            let has_err = x > upper;
            let new = if has_err { upper } else { x };
            (new, has_err)
        };
        map_buffer_iso(buf, go).fmap_once(Self::new)
    }

    fn truncate_from_int<I0>(buf: &Buffer<I0>) -> CastSeriesResult<Self>
    where
        I0: Clone,
        T: Copy + PartialOrd + From<Raw> + Bounded + TryFrom<I0>,
        Raw: Bounded,
    {
        let res0: CastSeriesResult<Buffer<T>> = buffer_int_to_int(buf);
        let res1 = Self::truncate_from_samesize_int(res0.inner);
        let err = res0
            .loss_position
            .zip(res1.loss_position)
            .map(|(x, y)| x.min(y));
        CastSeriesResult::new(res1.inner, err)
    }

    fn truncate_from_float<F>(buf: &Buffer<F>) -> CastSeriesResult<Self>
    where
        F: AsPrimitive<T> + Float,
        T: Copy + PartialOrd + From<Raw> + Bounded + AsPrimitive<F>,
        Raw: Bounded,
    {
        let res0: CastSeriesResult<Buffer<T>> = buffer_float_to_int(buf);
        let res1 = Self::truncate_from_samesize_int(res0.inner);
        // TODO this is an applicative
        let err = res0
            .loss_position
            .zip(res1.loss_position)
            .map(|(x, y)| x.min(y));
        CastSeriesResult::new(res1.inner, err)
    }
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
// TODO make this error actually useful
#[error("could not cast series to new type, value failed at row {position}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
// TODO there is probably a better error type for this
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
#[new(visibility(""))]
pub struct CastSeriesError {
    position: usize,
}

// TODO add to/from type names to this for error messages
#[derive(new)]
pub(crate) struct CastSeriesResult<T> {
    inner: T,
    loss_position: Option<usize>,
}

impl<T> CastSeriesResult<T> {
    pub(crate) fn into_err(self) -> Result<T, CastSeriesError> {
        match self.loss_position {
            Some(e) => Err(CastSeriesError::new(e)),
            None => Ok(self.inner),
        }
    }
}

impl_kind1!(pub(crate) CastSeriesResultFamily, CastSeriesResult);

impl_functor_once!(
    CastSeriesResult,
    self,
    mut f,
    CastSeriesResult::new(f(self.inner), self.loss_position,)
);

pub(crate) trait FromSeries<From>: Sized {
    fn from_series(col: From) -> CastSeriesResult<Self>;
}

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

// Lots of macros for casting b/t series.
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
                map_buffer(&col.into(), |x| (x.into(), false)).fmap_once(Self::new)
            }
        }
    };
}

/// Cast one int series into another where conversion might fail.
macro_rules! impl_cast_col_int_to_int {
    ($from:ident, $to:ident) => {
        impl FromSeries<$from> for $to {
            fn from_series(col: $from) -> CastSeriesResult<Self> {
                buffer_int_to_int(&col.into()).fmap_once(Self::new)
            }
        }
    };
}

/// Cast one int series into float series where conversion might fail.
macro_rules! impl_cast_col_int_to_float {
    ($from:ident, $to:ident) => {
        impl FromSeries<$from> for $to {
            fn from_series(col: $from) -> CastSeriesResult<Self> {
                buffer_int_to_float(&col.into()).fmap_once(Self::new)
            }
        }
    };
}

/// Cast one float series to into series where conversion might fail.
macro_rules! impl_cast_col_float_to_int {
    ($from:ident, $to:ident) => {
        impl FromSeries<$from> for $to {
            fn from_series(col: $from) -> CastSeriesResult<Self> {
                buffer_float_to_int(&col.into()).fmap_once(Self::new)
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

/// Cast an integer series to a truncated series with different internal type.
///
/// This might fail for two reasons. First, the starting value is bigger than
/// the target type can hold. Second, the value might be higher than the maximum
/// range of the target type, which is not the same as the maximum bitwise value
/// of the target type because it is truncated.
macro_rules! impl_truncate_from_int {
    ($from:ident, $to:ident) => {
        impl FromSeries<$from> for $to {
            fn from_series(col: $from) -> CastSeriesResult<Self> {
                Self::truncate_from_int(&col.into())
            }
        }
    };
}

/// Cast an float series to a truncated int series.
macro_rules! impl_truncate_from_float {
    ($from:ident, $to:ident) => {
        impl FromSeries<$from> for $to {
            fn from_series(col: $from) -> CastSeriesResult<Self> {
                Self::truncate_from_float(&col.into())
            }
        }
    };
}

/// Cast an int series to a float when int is validated to be within float range.
macro_rules! impl_int_to_float_lossless {
    ($from:ident, $to:ident) => {
        impl FromSeries<$from> for $to {
            fn from_series(col: $from) -> CastSeriesResult<Self> {
                map_buffer(&col.into(), |x| (x.as_(), false)).fmap_once(Self::new)
            }
        }
    };
}

/// Cast an f64 to f32 series.
macro_rules! impl_f64_to_f32 {
    ($from:ident, $to:ident) => {
        impl FromSeries<$from> for $to {
            fn from_series(col: $from) -> CastSeriesResult<Self> {
                let go = |x: f64| {
                    let new_value: f32 = x.as_();
                    let old_value = f64::from(new_value);
                    (new_value, x != old_value)
                };
                map_buffer(&col.into(), go).fmap_once(Self::new)
            }
        }
    };
}

// U08; all targets are larger, so all conversions are lossless and don't
// require checks

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

// U16; all except u8 are larger, so no conversions require checks except for
// u16 -> u8

impl_cast_col_int_to_int!(InternalU16Series, InternalU08Series);
impl_cast_col_noop!(InternalU16Series, InternalU16Series);
impl_cast_col_into!(InternalU16Series, InternalU24Series);
impl_cast_col_into!(InternalU16Series, InternalU32Series);
impl_cast_col_into!(InternalU16Series, InternalU40Series);
impl_cast_col_into!(InternalU16Series, InternalU48Series);
impl_cast_col_into!(InternalU16Series, InternalU56Series);
impl_cast_col_into!(InternalU16Series, InternalU64Series);
impl_cast_col_into!(InternalU16Series, InternalF32Series);
impl_cast_col_into!(InternalU16Series, InternalF64Series);

impl_cast_col_int_to_int!(U16Series, InternalU08Series);
impl_cast_col_noop!(U16Series, InternalU16Series);
impl_cast_col_into!(U16Series, InternalU24Series);
impl_cast_col_into!(U16Series, InternalU32Series);
impl_cast_col_into!(U16Series, InternalU40Series);
impl_cast_col_into!(U16Series, InternalU48Series);
impl_cast_col_into!(U16Series, InternalU56Series);
impl_cast_col_into!(U16Series, InternalU64Series);
impl_cast_col_into!(U16Series, InternalF32Series);
impl_cast_col_into!(U16Series, InternalF64Series);

// U24; all except u8 and u16 are larger, so no conversion require checks except
// for u24 (really a u32) -> u8 or u16. u24 is actually a u32 internally and
// also a subset of u32, so u24 -> u32 is a noop. Also, u24 can perfectly fit
// within an f32 so this is also lossless and requires no checks.

impl_cast_col_int_to_int!(InternalU24Series, InternalU08Series);
impl_cast_col_int_to_int!(InternalU24Series, InternalU16Series);
impl_cast_col_noop!(InternalU24Series, InternalU24Series);
impl_cast_col_noop!(InternalU24Series, InternalU32Series); // target is larger
impl_cast_col_into!(InternalU24Series, InternalU40Series);
impl_cast_col_into!(InternalU24Series, InternalU48Series);
impl_cast_col_into!(InternalU24Series, InternalU56Series);
impl_cast_col_into!(InternalU24Series, InternalU64Series);
impl_int_to_float_lossless!(InternalU24Series, InternalF32Series);
impl_cast_col_into!(InternalU24Series, InternalF64Series);

// U32; requires the following special conversion logic:
//
// 1. -> u8/u16: these are smaller so check for loss
// 2. -> u24: this is the same type with a reduced range, so check for anything
//    over range and mutate in place without allocating new memory.
// 3. -> f32: anything larger than 2^24 will lose precision, so need to check
//
// u32 can perfectly fit in an f64 so this is lossless

impl_cast_col_int_to_int!(InternalU32Series, InternalU08Series);
impl_cast_col_int_to_int!(InternalU32Series, InternalU16Series);
impl_truncate_from_samesize_int!(InternalU32Series, InternalU24Series);
impl_cast_col_noop!(InternalU32Series, InternalU32Series);
impl_cast_col_into!(InternalU32Series, InternalU40Series);
impl_cast_col_into!(InternalU32Series, InternalU48Series);
impl_cast_col_into!(InternalU32Series, InternalU56Series);
impl_cast_col_into!(InternalU32Series, InternalU64Series);
impl_cast_col_int_to_float!(InternalU32Series, InternalF32Series);
impl_cast_col_into!(InternalU32Series, InternalF64Series);

impl_cast_col_int_to_int!(U32Series, InternalU08Series);
impl_cast_col_int_to_int!(U32Series, InternalU16Series);
impl_truncate_from_samesize_int!(U32Series, InternalU24Series);
impl_cast_col_noop!(U32Series, InternalU32Series);
impl_cast_col_into!(U32Series, InternalU40Series);
impl_cast_col_into!(U32Series, InternalU48Series);
impl_cast_col_into!(U32Series, InternalU56Series);
impl_cast_col_into!(U32Series, InternalU64Series);
impl_cast_col_int_to_float!(U32Series, InternalF32Series);
impl_cast_col_into!(U32Series, InternalF64Series);

// U40; in general this is treated as a u64 when going to smaller types. The
// only special thing we need to add is an additional range check for u24
// conversion since simply converting to u32 using TryFrom won't be enough.
//
// Anything of the larger integer types (48, 56, 64) are the same underlying type
// and have larger ranges, so these conversions are noops.
//
// f32 conversion requires similar checks to u32.
//
// f64 conversion is lossless since the upper integer limit of an f64 is 2^53.

impl_cast_col_int_to_int!(InternalU40Series, InternalU08Series);
impl_cast_col_int_to_int!(InternalU40Series, InternalU16Series);
impl_truncate_from_int!(InternalU40Series, InternalU24Series);
impl_cast_col_int_to_int!(InternalU40Series, InternalU32Series);
impl_cast_col_noop!(InternalU40Series, InternalU40Series);
impl_cast_col_noop!(InternalU40Series, InternalU48Series); // target is larger
impl_cast_col_noop!(InternalU40Series, InternalU56Series); // target is larger
impl_cast_col_noop!(InternalU40Series, InternalU64Series); // target is larger
impl_cast_col_int_to_float!(InternalU40Series, InternalF32Series);
impl_int_to_float_lossless!(InternalU40Series, InternalF64Series);

// U48; This is the same as u40 except that u48 -> u40 is an in-place truncation
// and check since u40 is the same underlying type as u48 except with a smaller
// range.

impl_cast_col_int_to_int!(InternalU48Series, InternalU08Series);
impl_cast_col_int_to_int!(InternalU48Series, InternalU16Series);
impl_truncate_from_int!(InternalU48Series, InternalU24Series);
impl_cast_col_int_to_int!(InternalU48Series, InternalU32Series);
impl_truncate_from_samesize_int!(InternalU48Series, InternalU40Series);
impl_cast_col_noop!(InternalU48Series, InternalU48Series);
impl_cast_col_noop!(InternalU48Series, InternalU56Series); // target is larger
impl_cast_col_noop!(InternalU48Series, InternalU64Series); // target is larger
impl_cast_col_int_to_float!(InternalU48Series, InternalF32Series);
impl_int_to_float_lossless!(InternalU48Series, InternalF64Series);

// U56; This is the same as u48 and u40, continuing the same pattern.
//
// The only other difference between this and u48 is that f64 conversion is no
// longer totally lossless, so this needs a precision check.

impl_cast_col_int_to_int!(InternalU56Series, InternalU08Series);
impl_cast_col_int_to_int!(InternalU56Series, InternalU16Series);
impl_truncate_from_int!(InternalU56Series, InternalU24Series);
impl_cast_col_int_to_int!(InternalU56Series, InternalU32Series);
impl_truncate_from_samesize_int!(InternalU56Series, InternalU40Series);
impl_truncate_from_samesize_int!(InternalU56Series, InternalU48Series);
impl_cast_col_noop!(InternalU56Series, InternalU56Series); // target is larger
impl_cast_col_noop!(InternalU56Series, InternalU64Series);
impl_cast_col_int_to_float!(InternalU56Series, InternalF32Series);
impl_cast_col_int_to_float!(InternalU56Series, InternalF64Series);

// U64; Generally the same as u56, continuing the same pattern

impl_cast_col_int_to_int!(InternalU64Series, InternalU08Series);
impl_cast_col_int_to_int!(InternalU64Series, InternalU16Series);
impl_truncate_from_int!(InternalU64Series, InternalU24Series);
impl_cast_col_int_to_int!(InternalU64Series, InternalU32Series);
impl_truncate_from_samesize_int!(InternalU64Series, InternalU40Series);
impl_truncate_from_samesize_int!(InternalU64Series, InternalU48Series);
impl_truncate_from_samesize_int!(InternalU64Series, InternalU56Series);
impl_cast_col_noop!(InternalU64Series, InternalU64Series);
impl_cast_col_int_to_float!(InternalU64Series, InternalF32Series);
impl_cast_col_int_to_float!(InternalU64Series, InternalF64Series);

impl_cast_col_int_to_int!(U64Series, InternalU08Series);
impl_cast_col_int_to_int!(U64Series, InternalU16Series);
impl_truncate_from_int!(U64Series, InternalU24Series);
impl_cast_col_int_to_int!(U64Series, InternalU32Series);
impl_truncate_from_samesize_int!(U64Series, InternalU40Series);
impl_truncate_from_samesize_int!(U64Series, InternalU48Series);
impl_truncate_from_samesize_int!(U64Series, InternalU56Series);
impl_cast_col_noop!(U64Series, InternalU64Series);
impl_cast_col_int_to_float!(U64Series, InternalF32Series);
impl_cast_col_int_to_float!(U64Series, InternalF64Series);

// F32; When converting to a primitive integer, this conversion requires a
// loss of precision check. When converting to an unaligned integer (u24, etc)
// this additionally requires a range check to ensure the integer value is not
// out of range.
//
// f32 -> f64 is lossless, see
// https://doc.rust-lang.org/reference/expressions/operator-expr.html#r-expr.as.numeric.float-widening

impl_cast_col_float_to_int!(InternalF32Series, InternalU08Series);
impl_cast_col_float_to_int!(InternalF32Series, InternalU16Series);
impl_truncate_from_float!(InternalF32Series, InternalU24Series);
impl_cast_col_float_to_int!(InternalF32Series, InternalU32Series);
impl_truncate_from_float!(InternalF32Series, InternalU40Series);
impl_truncate_from_float!(InternalF32Series, InternalU48Series);
impl_truncate_from_float!(InternalF32Series, InternalU56Series);
impl_cast_col_float_to_int!(InternalF32Series, InternalU64Series);
impl_cast_col_noop!(InternalF32Series, InternalF32Series);
impl_cast_col_into!(InternalF32Series, InternalF64Series);

impl_cast_col_float_to_int!(F32Series, InternalU08Series);
impl_cast_col_float_to_int!(F32Series, InternalU16Series);
impl_truncate_from_float!(F32Series, InternalU24Series);
impl_cast_col_float_to_int!(F32Series, InternalU32Series);
impl_truncate_from_float!(F32Series, InternalU40Series);
impl_truncate_from_float!(F32Series, InternalU48Series);
impl_truncate_from_float!(F32Series, InternalU56Series);
impl_cast_col_float_to_int!(F32Series, InternalU64Series);
impl_cast_col_noop!(F32Series, InternalF32Series);
impl_cast_col_into!(F32Series, InternalF64Series);

// F64; same as f32 except going from f64 to f32 requires a loss of precision
// check

impl_cast_col_float_to_int!(InternalF64Series, InternalU08Series);
impl_cast_col_float_to_int!(InternalF64Series, InternalU16Series);
impl_truncate_from_float!(InternalF64Series, InternalU24Series);
impl_cast_col_float_to_int!(InternalF64Series, InternalU32Series);
impl_truncate_from_float!(InternalF64Series, InternalU40Series);
impl_truncate_from_float!(InternalF64Series, InternalU48Series);
impl_truncate_from_float!(InternalF64Series, InternalU56Series);
impl_cast_col_float_to_int!(InternalF64Series, InternalU64Series);
impl_f64_to_f32!(InternalF64Series, InternalF32Series);
impl_cast_col_noop!(InternalF64Series, InternalF64Series);

impl_cast_col_float_to_int!(F64Series, InternalU08Series);
impl_cast_col_float_to_int!(F64Series, InternalU16Series);
impl_truncate_from_float!(F64Series, InternalU24Series);
impl_cast_col_float_to_int!(F64Series, InternalU32Series);
impl_truncate_from_float!(F64Series, InternalU40Series);
impl_truncate_from_float!(F64Series, InternalU48Series);
impl_truncate_from_float!(F64Series, InternalU56Series);
impl_cast_col_float_to_int!(F64Series, InternalU64Series);
impl_f64_to_f32!(F64Series, InternalF32Series);
impl_cast_col_noop!(F64Series, InternalF64Series);

fn buffer_int_to_float<I, F>(buf: &Buffer<I>) -> CastSeriesResult<Buffer<F>>
where
    I: Bounded + PartialEq + AsPrimitive<F>,
    F: Float + AsPrimitive<I>,
    Buffer<F>: From<Vec<F>>,
{
    let go = |x: I| {
        let new_value: F = x.as_();
        let old_value: I = new_value.as_();
        (new_value, x != old_value)
    };
    map_buffer(buf, go)
}

fn buffer_int_to_int<I0, I1>(buf: &Buffer<I0>) -> CastSeriesResult<Buffer<I1>>
where
    I1: Bounded,
    I0: TryInto<I1> + Clone,
{
    let go = |x: I0| {
        if let Ok(y) = x.try_into() {
            (y, false)
        } else {
            (I1::max_value(), true)
        }
    };
    map_buffer(buf, go)
}

fn buffer_float_to_int<I, F>(buf: &Buffer<F>) -> CastSeriesResult<Buffer<I>>
where
    I: Bounded + AsPrimitive<F>,
    F: Float + AsPrimitive<I>,
    Buffer<I>: From<Vec<I>>,
{
    map_buffer(buf, |x: F| (x.as_(), !float_is_uint::<F, I>(x)))
}

fn map_buffer<F, X, Y>(buf: &Buffer<X>, mut f: F) -> CastSeriesResult<Buffer<Y>>
where
    X: Clone,
    Buffer<Y>: From<Vec<Y>>,
    F: FnMut(X) -> (Y, bool),
{
    let mut err = None;
    let new = buf
        .iter()
        .cloned()
        .enumerate()
        .map(|(i, x)| {
            let (y, has_loss) = f(x);
            if has_loss {
                err = Some(i);
            }
            y
        })
        .collect();
    CastSeriesResult::new(Buffer::from(new), err)
}

fn map_buffer_iso<F, X>(buf: Buffer<X>, mut f: F) -> CastSeriesResult<Buffer<X>>
where
    X: Copy,
    Buffer<X>: From<Vec<X>>,
    F: FnMut(X) -> (X, bool),
{
    let mut err = None;
    let mut inner = buf.make_mut();
    for (i, x) in inner.iter_mut().enumerate() {
        let (y, has_loss) = f(*x);
        if has_loss {
            err = Some(i);
        }
        *x = y;
    }
    CastSeriesResult::new(Buffer::from(inner), err)
}

fn float_is_uint<F: Float + 'static, I: Bounded + AsPrimitive<F>>(x: F) -> bool {
    let upper: F = I::max_value().as_();
    !x.is_nan() && !x.is_infinite() && !x.is_sign_negative() && x.fract().is_zero() && x <= upper
}

/// Any valid Rust numeric type which may be used in an [`FCSDataFrame`]
#[derive(Clone, Copy, Debug, Display, PartialEq)]
pub enum FCSDatatype {
    #[display("u8")]
    U08,
    #[display("u16")]
    U16,
    #[display("u32")]
    U32,
    #[display("u64")]
    U64,
    #[display("f32")]
    F32,
    #[display("f64")]
    F64,
}

impl PartialEq for AnyPrimitiveSeries {
    /// Test for numeric equality between two series.
    ///
    /// This will attempt to convert b/t datatypes when testing equality; for
    /// example, a `1` / `1.0` will be equal regardless of datatype because
    /// it can be losslessly converted between all possible types for a series
    /// (u8-64 and f32/f64).
    fn eq(&self, other: &Self) -> bool {
        fn go_try_into<XS, YS, X, Y>(xs: &XS, ys: &YS) -> bool
        where
            XS: AsRef<[X]>,
            YS: AsRef<[Y]>,
            X: PartialEq,
            Y: TryInto<X> + Copy,
        {
            xs.as_ref()
                .iter()
                .zip(ys.as_ref().iter())
                .all(|(x, y)| (*y).try_into().is_ok_and(|yx| &yx == x))
        }

        fn go_int_float<IS, FS, I, F>(xs: &IS, ys: &FS) -> bool
        where
            IS: AsRef<[I]>,
            FS: AsRef<[F]>,
            I: Bounded + AsPrimitive<F>,
            F: Float + 'static,
        {
            xs.as_ref()
                .iter()
                .zip(ys.as_ref().iter())
                .all(|(x, y)| float_is_uint::<F, I>(*y) && (*x).as_() == *y)
        }

        if self.len() != other.len() {
            return false;
        }

        match (self, other) {
            (Self::U08(xs), Self::U08(ys)) => go_try_into(xs, ys),
            (Self::U08(xs), Self::U16(ys)) => go_try_into(xs, ys),
            (Self::U08(xs), Self::U32(ys)) => go_try_into(xs, ys),
            (Self::U08(xs), Self::U64(ys)) => go_try_into(xs, ys),
            (Self::U08(xs), Self::F32(ys)) => go_int_float(xs, ys),
            (Self::U08(xs), Self::F64(ys)) => go_int_float(xs, ys),

            (Self::U16(xs), Self::U08(ys)) => go_try_into(xs, ys),
            (Self::U16(xs), Self::U16(ys)) => go_try_into(xs, ys),
            (Self::U16(xs), Self::U32(ys)) => go_try_into(xs, ys),
            (Self::U16(xs), Self::U64(ys)) => go_try_into(xs, ys),
            (Self::U16(xs), Self::F32(ys)) => go_int_float(xs, ys),
            (Self::U16(xs), Self::F64(ys)) => go_int_float(xs, ys),

            (Self::U32(xs), Self::U08(ys)) => go_try_into(xs, ys),
            (Self::U32(xs), Self::U16(ys)) => go_try_into(xs, ys),
            (Self::U32(xs), Self::U32(ys)) => go_try_into(xs, ys),
            (Self::U32(xs), Self::U64(ys)) => go_try_into(xs, ys),
            (Self::U32(xs), Self::F32(ys)) => go_int_float(xs, ys),
            (Self::U32(xs), Self::F64(ys)) => go_int_float(xs, ys),

            (Self::U64(xs), Self::U08(ys)) => go_try_into(xs, ys),
            (Self::U64(xs), Self::U16(ys)) => go_try_into(xs, ys),
            (Self::U64(xs), Self::U32(ys)) => go_try_into(xs, ys),
            (Self::U64(xs), Self::U64(ys)) => go_try_into(xs, ys),
            (Self::U64(xs), Self::F32(ys)) => go_int_float(xs, ys),
            (Self::U64(xs), Self::F64(ys)) => go_int_float(xs, ys),

            (Self::F32(xs), Self::U08(ys)) => go_int_float(ys, xs),
            (Self::F32(xs), Self::U16(ys)) => go_int_float(ys, xs),
            (Self::F32(xs), Self::U32(ys)) => go_int_float(ys, xs),
            (Self::F32(xs), Self::U64(ys)) => go_int_float(ys, xs),
            (Self::F32(xs), Self::F32(ys)) => go_try_into(xs, ys),
            (Self::F32(xs), Self::F64(ys)) => go_try_into(ys, xs),

            (Self::F64(xs), Self::U08(ys)) => go_int_float(ys, xs),
            (Self::F64(xs), Self::U16(ys)) => go_int_float(ys, xs),
            (Self::F64(xs), Self::U32(ys)) => go_int_float(ys, xs),
            (Self::F64(xs), Self::U64(ys)) => go_int_float(ys, xs),
            (Self::F64(xs), Self::F32(ys)) => go_try_into(xs, ys),
            (Self::F64(xs), Self::F64(ys)) => go_try_into(xs, ys),
        }
    }
}

impl<T> From<Vec<T>> for PrimitiveSeries<T> {
    fn from(value: Vec<T>) -> Self {
        Self(value.into())
    }
}

impl AnyPrimitiveSeries {
    #[must_use]
    pub fn len(&self) -> usize {
        match_many_to_one!(self, Self, [U08, U16, U32, U64, F32, F64], x, { x.0.len() })
    }

    // pub(crate) fn check_writer<E, F, ToType>(&self, f: F) -> Result<(), LossError<E>>
    // where
    //     F: Fn(ToType) -> Option<E>,
    //     ToType: AllFCSCast,
    // {
    //     match_many_to_one!(self, Self, [U08, U16, U32, U64, F32, F64], xs, {
    //         IsFCSDataType::check_writer(xs, f)
    //     })
    // }

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

    // /// The number of bytes occupied by the series if written as ASCII
    // #[must_use]
    // pub fn ascii_nbytes(&self) -> u32 {
    //     match self {
    //         Self::U08(xs) => u8::as_col_iter::<u64>(xs).map(|x| cast_nbytes(&x)).sum(),
    //         Self::U16(xs) => u16::as_col_iter::<u64>(xs).map(|x| cast_nbytes(&x)).sum(),
    //         Self::U32(xs) => u32::as_col_iter::<u64>(xs).map(|x| cast_nbytes(&x)).sum(),
    //         Self::U64(xs) => u64::as_col_iter::<u64>(xs).map(|x| cast_nbytes(&x)).sum(),
    //         Self::F32(xs) => f32::as_col_iter::<u64>(xs).map(|x| cast_nbytes(&x)).sum(),
    //         Self::F64(xs) => f64::as_col_iter::<u64>(xs).map(|x| cast_nbytes(&x)).sum(),
    //     }
    // }
}

/// Error when building [`FCSDataFrame`] from individual series
#[derive(Debug, Error)]
#[error("series lengths to not match")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct NewDataframeError;

/// Error when new series has number of rows which are not equal to that in [`FCSDataFrame`]
#[derive(Debug, Error)]
#[error("series length ({col_len}) is different from number of rows in dataframe ({df_len})")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct SeriesLengthError {
    df_len: usize,
    col_len: usize,
}

#[delegatable_trait]
pub trait HasLen {
    // this will be used for vectors, len is always constant
    #[allow(clippy::len_without_is_empty)]
    fn len(&self) -> usize;
}

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

    pub fn iter(&self) -> Iter<'_, C> {
        self.series.iter()
    }

    pub(crate) fn check_ranges(&self, check: CheckEventRanges) -> Vec<TruncatedResult>
    where
        C: CheckRange,
    {
        self.series
            .iter()
            .enumerate()
            .map(|(i, c)| c.check_range(i.into(), check))
            .collect()
    }

    pub(crate) fn check_ranges_mut(&mut self, trunc: TruncateEventValues) -> Vec<TruncatedResult>
    where
        C: CheckRange,
    {
        self.series
            .iter_mut()
            .enumerate()
            .map(|(i, c)| c.check_range_mut(i.into(), trunc))
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

    pub(crate) fn drop_in_place(&mut self, i: usize) -> Option<C>
    where
        C: HasLen,
    {
        if i > self.series.len() {
            None
        } else {
            Some(self.remove(i))
        }
    }

    // TODO why called nocheck?
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

// impl InternalColumn<u64, u64> {
//     /// Return number of bytes this will occupy if written as delimited ASCII
//     pub(crate) fn ascii_nbytes(&self) -> u64 {
//         let n = self.len();
//         if n == 0 {
//             return 0;
//         }
//         let ndelim = n - 1;
//         let ndigits: u64 = self
//             .as_ref()
//             .iter()
//             .map(|&x| u64::from(u8::from(Chars::from_u64(x))))
//             .sum();
//         ndigits + usize_to_u64(ndelim)
//     }
// }

// pub(crate) type FCSColIter<'a, FromType, ToType> =
//     iter::Map<iter::Copied<Iter<'a, FromType>>, fn(FromType) -> CastResult<ToType>>;

// pub(crate) trait IsFCSDataType
// where
//     Self: Sized + Copy,
//     [Self]: ToOwned,
// {
//     const NATIVE: FCSDatatype;

//     /// Return iterator for column, converting to native type on the fly.
//     fn as_col_iter<ToType>(c: &FCSColumn<Self>) -> FCSColIter<'_, Self, ToType>
//     where
//         ToType: NumCast<Self>,
//     {
//         Self::iter_native(c).map(ToType::from_truncated)
//     }

//     /// Try to convert column to native type, and return error on failure.
//     ///
//     /// This is separate from returning the iterator itself because if we can't
//     /// tolerate any loss, the only way to find with only the iterator it is
//     /// while we are using it to write a file, which opens the possibility of a
//     /// partially-written file (not good). Therefore we need to check this
//     /// before returning the iterator at all, which ironically can only be found
//     /// by iterating the entire vector once.
//     ///
//     /// This only applies to the case where we want to crash if any loss will
//     /// occur. If we only wish to warn the user and use lossy conversion
//     /// anyways, this only requires one iteration since the iterator itself will
//     /// return a [`CastResult`] which carries a flag if loss occurred.
//     fn check_writer<E, F, ToType>(c: &FCSColumn<Self>, f: F) -> Result<(), LossError<E>>
//     where
//         F: Fn(ToType) -> Option<E>,
//         ToType: NumCast<Self>,
//     {
//         for x in Self::as_col_iter::<ToType>(c) {
//             x.resolve()?;
//             if let Some(err) = f(x.new) {
//                 return Err(LossError::Other(err));
//             }
//         }
//         Ok(())
//     }

//     fn iter_native(c: &FCSColumn<Self>) -> iter::Copied<Iter<'_, Self>> {
//         c.0.iter().copied()
//     }
// }

// /// Error when value in [`FCSDataFrame`] loses information (type conversion or something else)
// #[derive(Clone, Copy, Display, Debug, Error)]
// pub enum LossError<E> {
//     Cast(#[from] CastError),
//     Other(E),
// }

// /// Error when value in [`FCSDataFrame`] loses information due to type conversion
// #[derive(Clone, Copy, Debug, Error, new)]
// #[error("data loss occurred when converting from {from} to {to}")]
// pub struct CastError {
//     from: FCSDatatype,
//     to: FCSDatatype,
// }

// impl IsFCSDataType for u8 {
//     const NATIVE: FCSDatatype = FCSDatatype::U08;
// }

// impl IsFCSDataType for u16 {
//     const NATIVE: FCSDatatype = FCSDatatype::U16;
// }

// impl IsFCSDataType for u32 {
//     const NATIVE: FCSDatatype = FCSDatatype::U32;
// }

// impl IsFCSDataType for u64 {
//     const NATIVE: FCSDatatype = FCSDatatype::U64;
// }

// impl IsFCSDataType for f32 {
//     const NATIVE: FCSDatatype = FCSDatatype::F32;
// }

// impl IsFCSDataType for f64 {
//     const NATIVE: FCSDatatype = FCSDatatype::F64;
// }

// #[cfg_attr(test, derive(Debug, PartialEq))]
// pub(crate) struct CastResult<T> {
//     pub(crate) new: T,
//     pub(crate) lossy: Option<FCSDatatype>,
// }

// impl<T> CastResult<T> {
//     fn new<FromT: IsFCSDataType>(new: T, has_loss: bool) -> Self {
//         let lossy = has_loss.then_some(FromT::NATIVE);
//         Self { new, lossy }
//     }

//     pub(crate) fn as_err(&self) -> Option<CastError>
//     where
//         T: IsFCSDataType,
//     {
//         self.lossy.map(|from| CastError::new(from, T::NATIVE))
//     }

//     pub(crate) fn resolve(&self) -> Result<(), CastError>
//     where
//         T: IsFCSDataType,
//     {
//         self.as_err().map_or(Ok(()), Err)
//     }
// }

// pub(crate) trait NumCast<T>: Sized + IsFCSDataType {
//     fn from_truncated(x: T) -> CastResult<Self>;
// }

// macro_rules! impl_cast_noloss {
//     ($from:ident, $to:ident) => {
//         impl NumCast<$from> for $to {
//             fn from_truncated(x: $from) -> CastResult<Self> {
//                 CastResult {
//                     new: x.into(),
//                     lossy: None,
//                 }
//             }
//         }
//     };
// }

// macro_rules! impl_cast_int_lossy {
//     ($from:ident, $to:ident) => {
//         impl NumCast<$from> for $to {
//             fn from_truncated(x: $from) -> CastResult<Self> {
//                 if let Ok(new) = $to::try_from(x) {
//                     CastResult::new::<$from>(new, false)
//                 } else {
//                     CastResult::new::<$from>($to::MAX, true)
//                 }
//             }
//         }
//     };
// }

// macro_rules! impl_cast_float_to_int_lossy {
//     ($from:ident, $to:ident) => {
//         impl NumCast<$from> for $to {
//             #[allow(clippy::cast_precision_loss)]
//             #[allow(clippy::cast_sign_loss)]
//             #[allow(clippy::cast_lossless)]
//             #[allow(clippy::cast_possible_truncation)]
//             #[allow(clippy::as_conversions)]
//             fn from_truncated(x: $from) -> CastResult<Self> {
//                 let has_loss = x.is_nan()
//                     || x.is_infinite()
//                     || x.is_sign_negative()
//                     || !x.fract().is_zero()
//                     || x > $to::MAX as $from;
//                 CastResult::new::<$from>(x as $to, has_loss)
//             }
//         }
//     };
// }

// macro_rules! impl_cast_int_to_float_lossy {
//     ($from:ident, $to:ident) => {
//         impl NumCast<$from> for $to {
//             #[allow(clippy::cast_precision_loss)]
//             #[allow(clippy::cast_sign_loss)]
//             #[allow(clippy::cast_possible_truncation)]
//             #[allow(clippy::as_conversions)]
//             fn from_truncated(x: $from) -> CastResult<Self> {
//                 let new = x as $to;
//                 let old = new as $from;
//                 CastResult::new::<$from>(new, old != x)
//             }
//         }
//     };
// }

// impl_cast_noloss!(u8, u8);
// impl_cast_noloss!(u8, u16);
// impl_cast_noloss!(u8, u32);
// impl_cast_noloss!(u8, u64);
// impl_cast_noloss!(u8, f32);
// impl_cast_noloss!(u8, f64);

// impl_cast_int_lossy!(u16, u8);
// impl_cast_noloss!(u16, u16);
// impl_cast_noloss!(u16, u32);
// impl_cast_noloss!(u16, u64);
// impl_cast_noloss!(u16, f32);
// impl_cast_noloss!(u16, f64);

// impl_cast_int_lossy!(u32, u8);
// impl_cast_int_lossy!(u32, u16);
// impl_cast_noloss!(u32, u32);
// impl_cast_noloss!(u32, u64);
// impl_cast_int_to_float_lossy!(u32, f32);
// impl_cast_noloss!(u32, f64);

// impl_cast_int_lossy!(u64, u8);
// impl_cast_int_lossy!(u64, u16);
// impl_cast_int_lossy!(u64, u32);
// impl_cast_noloss!(u64, u64);
// impl_cast_int_to_float_lossy!(u64, f32);
// impl_cast_int_to_float_lossy!(u64, f64);

// impl_cast_float_to_int_lossy!(f32, u8);
// impl_cast_float_to_int_lossy!(f32, u16);
// impl_cast_float_to_int_lossy!(f32, u32);
// impl_cast_float_to_int_lossy!(f32, u64);
// impl_cast_noloss!(f32, f32);
// // this will always be lossless, see
// // https://doc.rust-lang.org/reference/expressions/operator-expr.html#r-expr.as.numeric.float-widening
// impl_cast_noloss!(f32, f64);

// impl_cast_float_to_int_lossy!(f64, u8);
// impl_cast_float_to_int_lossy!(f64, u16);
// impl_cast_float_to_int_lossy!(f64, u32);
// impl_cast_float_to_int_lossy!(f64, u64);

// impl NumCast<f64> for f32 {
//     #[allow(clippy::cast_possible_truncation)]
//     #[allow(clippy::float_cmp)]
//     #[allow(clippy::as_conversions)]
//     fn from_truncated(x: f64) -> CastResult<Self> {
//         let new = x as Self;
//         let old = f64::from(new);
//         CastResult::new::<f64>(new, old != x)
//     }
// }

// impl_cast_noloss!(f64, f64);

// pub(crate) fn cast_nbytes(x: &CastResult<u64>) -> u32 {
//     u8::from(Chars::from_u64(x.new)).into()
// }

// pub(crate) trait AllFCSCast:
//     NumCast<u8> + NumCast<u16> + NumCast<u32> + NumCast<u64> + NumCast<f32> + NumCast<f64>
// {
// }

// impl<T> AllFCSCast for T where
//     T: NumCast<u8> + NumCast<u16> + NumCast<u32> + NumCast<u64> + NumCast<f32> + NumCast<f64>
// {
// }

// TODO this seems like a good place for property testing
// (https://github.com/proptest-rs/proptest)
#[cfg(test)]
mod tests {
    use core::f32;

    use super::*;

    // only test lossy cases, assume the others will simply noop

    #[test]
    fn u16_to_u8() {
        assert!(u8::from_truncated(1_u16).lossy.is_none());
        assert_eq!(
            u8::from_truncated(0x100_u16),
            CastResult::new::<u16>(0xFF, true)
        );
    }

    #[test]
    fn u32_to_u8() {
        assert!(u8::from_truncated(1_u32).lossy.is_none());
        assert_eq!(
            u8::from_truncated(0x100_u32),
            CastResult::new::<u32>(0xFF, true)
        );
    }

    #[test]
    fn u64_to_u8() {
        assert!(u8::from_truncated(1_u64).lossy.is_none());
        assert_eq!(
            u8::from_truncated(0x100_u64),
            CastResult::new::<u64>(0xFF, true)
        );
    }

    #[test]
    fn u32_to_u16() {
        assert!(u16::from_truncated(1_u32).lossy.is_none());
        assert_eq!(
            u16::from_truncated(0x0001_0000_u32),
            CastResult::new::<u32>(0xFFFF, true)
        );
    }

    #[test]
    fn u64_to_u16() {
        assert!(u16::from_truncated(1_u64).lossy.is_none());
        assert_eq!(
            u16::from_truncated(0x0001_0000_u64),
            CastResult::new::<u64>(0xFFFF, true)
        );
    }

    #[test]
    fn u64_to_u32() {
        assert!(u32::from_truncated(1_u64).lossy.is_none());
        assert_eq!(
            u32::from_truncated(0x0001_0000_0000_u64),
            CastResult::new::<u64>(0xFFFF_FFFF, true)
        );
    }

    // uint should map exactly to f32 if less than 2^24, above this it will
    // start rounding to nearest even number (and beyond as we get higher)

    #[test]
    fn u32_to_f32() {
        assert_eq!(
            f32::from_truncated(1_u32),
            CastResult::new::<u64>(1.0, false)
        );
        assert_eq!(
            f32::from_truncated(0x0100_0000_u32),
            CastResult::new::<u32>(16_777_216.0, false)
        );
        assert_eq!(
            f32::from_truncated(0x0100_0001_u32),
            CastResult::new::<u32>(16_777_216.0, true)
        );
        assert_eq!(
            f32::from_truncated(0x0100_0002_u32),
            CastResult::new::<u32>(16_777_218.0, false)
        );
    }

    #[test]
    fn u64_to_f32() {
        assert_eq!(
            f32::from_truncated(1_u64),
            CastResult::new::<u64>(1.0_f32, false)
        );
        assert_eq!(
            f32::from_truncated(0x0100_0000_u64),
            CastResult::new::<u64>(16_777_216.0_f32, false)
        );
        assert_eq!(
            f32::from_truncated(0x0100_0001_u64),
            CastResult::new::<u64>(16_777_216.0_f32, true)
        );
        assert_eq!(
            f32::from_truncated(0x0100_0002_u64),
            CastResult::new::<u64>(16_777_218.0_f32, false)
        );
    }

    // uint should map exactly to f64 if less than 2^53, above this it will
    // start rounding to nearest even number (and beyond as we get higher)

    #[test]
    fn u64_to_f64() {
        assert_eq!(
            f64::from_truncated(1_u64),
            CastResult::new::<u64>(1.0_f64, false)
        );
        assert_eq!(
            f64::from_truncated(0x0020_0000_0000_0000_u64),
            CastResult::new::<u64>(9_007_199_254_740_992.0_f64, false)
        );
        assert_eq!(
            f64::from_truncated(0x0020_0000_0000_0001_u64),
            CastResult::new::<u64>(9_007_199_254_740_992.0_f64, true)
        );
        assert_eq!(
            f64::from_truncated(0x0020_0000_0000_0002_u64),
            CastResult::new::<u64>(9_007_199_254_740_994.0_f64, false)
        );
    }

    macro_rules! test_float_to_int {
        ($float:ident, $int:ident) => {
            let zero: $float = 0.0;
            let nonzero: $float = 1.5;
            let neg: $float = -1.0;

            assert_eq!(
                $int::from_truncated(zero),
                CastResult::new::<$float>(0, false)
            );
            #[allow(clippy::cast_possible_truncation)]
            #[allow(clippy::cast_precision_loss)]
            #[allow(clippy::cast_lossless)]
            #[allow(clippy::as_conversions)]
            let x = $int::from_truncated($int::MAX as $float);
            assert_eq!(x, CastResult::new::<$float>($int::MAX, false));
            assert_eq!(
                $int::from_truncated(nonzero),
                CastResult::new::<$float>(1, true)
            );
            assert_eq!(
                $int::from_truncated(neg),
                CastResult::new::<$float>(0, true)
            );
            assert_eq!(
                $int::from_truncated($float::NAN),
                CastResult::new::<$float>(0, true)
            );
            assert_eq!(
                $int::from_truncated($float::NEG_INFINITY),
                CastResult::new::<$float>(0, true)
            );
            assert_eq!(
                $int::from_truncated($float::INFINITY),
                CastResult::new::<$float>($int::MAX, true)
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
        assert_eq!(
            f32::from_truncated(0.0_f64),
            CastResult::new::<f64>(0.0, false)
        );
        // this is the upper limit of ints that an f32 can represent exactly,
        // going above this will start to induce rounding errors that don't
        // happen in f64
        assert_eq!(
            f32::from_truncated(16_777_216.0_f64),
            CastResult::new::<f64>(16_777_216.0, false)
        );
        assert_eq!(
            f32::from_truncated(16_777_217.0_f64),
            CastResult::new::<f64>(16_777_216.0, true)
        );
        // this is a decimal that can be represented perfectly in both
        assert_eq!(
            f32::from_truncated(0.5_f64),
            CastResult::new::<f64>(0.5, false)
        );
        // this is a repeating decimal which will have different representations
        // in f32 and f64, thus it will be lossy
        assert_eq!(
            f32::from_truncated(0.2_f64),
            CastResult::new::<f64>(0.2, true)
        );
    }
}

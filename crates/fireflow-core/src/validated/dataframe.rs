use crate::macros::match_many_to_one;
use crate::validated::ascii_range::Chars;
use crate::validated::unaligned::{U24, U40, U48, U56};

use ambassador::{Delegate, delegatable_trait};
use type_families::{FunctorOnce as _, impl_functor_once, impl_kind1};

use bytemuck::{AnyBitPattern, NoUninit, cast_vec};
use derive_more::{AsRef, Display, From};
use derive_new::new;
use num_traits::bounds::Bounded;
use num_traits::cast::AsPrimitive;
use num_traits::float::Float;
use num_traits::identities::Zero as _;
use polars_arrow::buffer::Buffer;
use thiserror::Error;

use std::iter;
use std::marker::PhantomData;
use std::slice::Iter;

#[cfg(feature = "python")]
use {fireflow_core_proc::DisplayAsPyErr, fireflow_types::python as py};

/// Column-major dataframe to represent events in DATA
///
/// This is a very light wrapper around a polars buffer which is ref-counted and
/// therefore allows us to return event to external interfaces without copying
/// memory. It is validated to contain no NULL values where all columns have the
/// same length.
pub type PrimitiveDataFrame = FFDataFrame<AnyPrimitiveColumn>;

/// A dataframe with internally validated column ranges.
///
/// This validation allows us to integrate it seamlessly with data layouts
/// of varying byte widths, including those that aren't a power of 2.
pub(crate) type InternalDataFrame = FFDataFrame<AnyInternalColumn>;

// impl FCSDataFrame {
//     fn into_internal(self) -> (InternalDataFrame, Vec<Option<usize>>) {
//         let ncols = self.ncols();
//         let new_columns = Vec::with_capacity(ncols);
//         let error_positions = Vec::with_capacity(ncols);
//         for c in self.columns {
//             let res = c.cast_column();
//             new_columns.push(res.inner);
//             error_positions.push(res.loss_position);
//         }
//         let new = InternalDataFrame::new(new_columns, self.nrows);
//         (new, error_positions)
//     }
// }

// NOTE cloning a buffer is O(1) so this doesn't need to be impled for a reference
impl From<InternalDataFrame> for PrimitiveDataFrame {
    fn from(value: InternalDataFrame) -> Self {
        Self::new(
            value
                .columns
                .into_iter()
                .map(AnyPrimitiveColumn::from)
                .collect(),
            value.nrows,
        )
    }
}

// impl AnyFCSColumn {
//     fn into_internal(self) -> CastResult<AnyInternalColumn> {
//         match self {
//             AnyFCSColumn::U08(c) => c.cast_column(),
//         }
//     }
// }

impl From<AnyInternalColumn> for AnyPrimitiveColumn {
    fn from(value: AnyInternalColumn) -> Self {
        match value {
            AnyInternalColumn::U08(c) => Self::U08(PrimitiveColumn(c.inner)),
            AnyInternalColumn::U16(c) => Self::U16(PrimitiveColumn(c.inner)),
            AnyInternalColumn::U24(c) => Self::U32(PrimitiveColumn(c.inner)),
            AnyInternalColumn::U32(c) => Self::U32(PrimitiveColumn(c.inner)),
            AnyInternalColumn::U40(c) => Self::U64(PrimitiveColumn(c.inner)),
            AnyInternalColumn::U48(c) => Self::U64(PrimitiveColumn(c.inner)),
            AnyInternalColumn::U56(c) => Self::U64(PrimitiveColumn(c.inner)),
            AnyInternalColumn::U64(c) => Self::U64(PrimitiveColumn(c.inner)),
            AnyInternalColumn::F32(c) => Self::F32(PrimitiveColumn(c.inner)),
            AnyInternalColumn::F64(c) => Self::F64(PrimitiveColumn(c.inner)),
        }
    }
}

#[derive(Clone, PartialEq, new)]
#[new(visibility = "")]
pub struct FFDataFrame<C> {
    columns: Vec<C>,
    nrows: usize,
}

impl<C> Default for FFDataFrame<C> {
    fn default() -> Self {
        Self::new(vec![], 0)
    }
}

/// Any valid column from [`FCSDataFrame`]
#[derive(Clone, From, Delegate)]
#[delegate(HasLen)]
pub enum AnyPrimitiveColumn {
    U08(U08Column),
    U16(U16Column),
    U32(U32Column),
    U64(U64Column),
    F32(F32Column),
    F64(F64Column),
}

#[derive(Clone, From, Delegate)]
#[delegate(HasLen)]
pub(crate) enum AnyInternalColumn {
    U08(InternalU08Column),
    U16(InternalU16Column),
    U24(InternalU24Column),
    U32(InternalU32Column),
    U40(InternalU40Column),
    U48(InternalU48Column),
    U56(InternalU56Column),
    U64(InternalU64Column),
    F32(InternalF32Column),
    F64(InternalF64Column),
}

/// A generic column for [`FCSDataFrame`]
#[derive(Clone, PartialEq, AsRef)]
#[repr(transparent)]
#[as_ref([T])]
pub struct PrimitiveColumn<T>(pub Buffer<T>);

pub type U08Column = PrimitiveColumn<u8>;
pub type U16Column = PrimitiveColumn<u16>;
pub type U32Column = PrimitiveColumn<u32>;
pub type U64Column = PrimitiveColumn<u64>;
pub type F32Column = PrimitiveColumn<f32>;
pub type F64Column = PrimitiveColumn<f64>;

#[derive(Clone, new)]
#[repr(transparent)]
#[new(visibility = "")]
pub(crate) struct InternalColumn<T, Raw> {
    inner: Buffer<T>,
    _outer: PhantomData<Raw>,
}

macro_rules! impl_internal_from_vec {
    ($t:ident, $raw:ident) => {
        impl From<Vec<$t>> for InternalColumn<$t, $raw> {
            fn from(value: Vec<$t>) -> Self {
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

impl<T, Raw> InternalColumn<T, Raw> {
    fn as_raw_slice(&self) -> &[Raw] {
        debug_assert!(size_of::<T>() == size_of::<Raw>(), "type sizes don't match");
        // SAFETY: T and Raw are assumed to have the same layout and size
        unsafe { &*self.inner.as_ref().as_ptr().cast::<&[Raw]>() }
    }

    fn truncate_from_samesize_int(buf: Buffer<T>) -> CastColResult<Self>
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

    fn truncate_from_int<I0>(buf: &Buffer<I0>) -> CastColResult<Self>
    where
        I0: Clone,
        T: Copy + PartialOrd + From<Raw> + Bounded + TryFrom<I0>,
        Raw: Bounded,
    {
        let res0: CastColResult<Buffer<T>> = buffer_int_to_int(buf);
        let res1 = Self::truncate_from_samesize_int(res0.inner);
        let err = res0
            .loss_position
            .zip(res1.loss_position)
            .map(|(x, y)| x.min(y));
        CastColResult::new(res1.inner, err)
    }

    fn truncate_from_float<F>(buf: &Buffer<F>) -> CastColResult<Self>
    where
        F: AsPrimitive<T> + Float,
        T: Copy + PartialOrd + From<Raw> + Bounded + AsPrimitive<F>,
        Raw: Bounded,
    {
        let res0: CastColResult<Buffer<T>> = buffer_float_to_int(buf);
        let res1 = Self::truncate_from_samesize_int(res0.inner);
        // TODO this is an applicative
        let err = res0
            .loss_position
            .zip(res1.loss_position)
            .map(|(x, y)| x.min(y));
        CastColResult::new(res1.inner, err)
    }
}

pub(crate) type InternalU08Column = InternalColumn<u8, u8>;
pub(crate) type InternalU16Column = InternalColumn<u16, u16>;
pub(crate) type InternalU24Column = InternalColumn<u32, U24>;
pub(crate) type InternalU32Column = InternalColumn<u32, u32>;
pub(crate) type InternalU40Column = InternalColumn<u64, U40>;
pub(crate) type InternalU48Column = InternalColumn<u64, U48>;
pub(crate) type InternalU56Column = InternalColumn<u64, U56>;
pub(crate) type InternalU64Column = InternalColumn<u64, u64>;
pub(crate) type InternalF32Column = InternalColumn<f32, f32>;
pub(crate) type InternalF64Column = InternalColumn<f64, f64>;

#[derive(new)]
struct CastColResult<T> {
    inner: T,
    loss_position: Option<usize>,
}

impl_kind1!(CastColResultFamily, CastColResult);

impl_functor_once!(
    CastColResult,
    self,
    mut f,
    CastColResult::new(f(self.inner), self.loss_position,)
);

trait FromColumn<From>: Sized {
    fn from_column(col: From) -> CastColResult<Self>;
}

// Primitive->primitive column casts

macro_rules! impl_cast_col_noop {
    ($from:ident, $to:ident) => {
        impl FromColumn<$from> for $to {
            fn from_column(col: $from) -> CastColResult<Self> {
                CastColResult::new(Self::new(col.inner), None)
            }
        }
    };
}

macro_rules! impl_cast_col_into {
    ($from:ident, $to:ident) => {
        impl FromColumn<$from> for $to {
            fn from_column(col: $from) -> CastColResult<Self> {
                map_buffer(&col.inner, |x| (x.into(), false)).fmap_once(Self::new)
            }
        }
    };
}

macro_rules! impl_cast_col_int_to_int {
    ($from:ident, $to:ident) => {
        impl FromColumn<$from> for $to {
            fn from_column(col: $from) -> CastColResult<Self> {
                buffer_int_to_int(&col.inner).fmap_once(Self::new)
            }
        }
    };
}

macro_rules! impl_cast_col_int_to_float {
    ($from:ident, $to:ident) => {
        impl FromColumn<$from> for $to {
            fn from_column(col: $from) -> CastColResult<Self> {
                buffer_int_to_float(&col.inner).fmap_once(Self::new)
            }
        }
    };
}

macro_rules! impl_cast_col_float_to_int {
    ($from:ident, $to:ident) => {
        impl FromColumn<$from> for $to {
            fn from_column(col: $from) -> CastColResult<Self> {
                buffer_float_to_int(&col.inner).fmap_once(Self::new)
            }
        }
    };
}

/// Cast an integer column to a truncated column with the same internal type.
///
/// This is more optimal than a general cast because we don't need to reallocate
/// a new buffer; we only need to check the range of the current values and clip
/// if needed.
macro_rules! impl_truncate_from_samesize_int {
    ($from:ident, $to:ident) => {
        impl FromColumn<$from> for $to {
            fn from_column(col: $from) -> CastColResult<Self> {
                Self::truncate_from_samesize_int(col.inner)
            }
        }
    };
}

macro_rules! impl_truncate_from_int {
    ($from:ident, $to:ident) => {
        impl FromColumn<$from> for $to {
            fn from_column(col: $from) -> CastColResult<Self> {
                Self::truncate_from_int(&col.inner)
            }
        }
    };
}

macro_rules! impl_truncate_from_float {
    ($from:ident, $to:ident) => {
        impl FromColumn<$from> for $to {
            fn from_column(col: $from) -> CastColResult<Self> {
                Self::truncate_from_float(&col.inner)
            }
        }
    };
}

macro_rules! impl_int_to_float_lossless {
    ($from:ident, $to:ident) => {
        impl FromColumn<$from> for $to {
            fn from_column(col: $from) -> CastColResult<Self> {
                map_buffer(&col.inner, |x| (x.as_(), false)).fmap_once(Self::new)
            }
        }
    };
}

// U08; all targets are larger, so all conversions are lossless and don't
// require checks

impl_cast_col_noop!(InternalU08Column, InternalU08Column);
impl_cast_col_into!(InternalU08Column, InternalU16Column);
impl_cast_col_into!(InternalU08Column, InternalU24Column);
impl_cast_col_into!(InternalU08Column, InternalU32Column);
impl_cast_col_into!(InternalU08Column, InternalU40Column);
impl_cast_col_into!(InternalU08Column, InternalU48Column);
impl_cast_col_into!(InternalU08Column, InternalU56Column);
impl_cast_col_into!(InternalU08Column, InternalU64Column);
impl_cast_col_into!(InternalU08Column, InternalF32Column);
impl_cast_col_into!(InternalU08Column, InternalF64Column);

// U16; all except u8 are larger, so no conversions require checks except for
// u16 -> u8

impl_cast_col_int_to_int!(InternalU16Column, InternalU08Column);
impl_cast_col_noop!(InternalU16Column, InternalU16Column);
impl_cast_col_into!(InternalU16Column, InternalU24Column);
impl_cast_col_into!(InternalU16Column, InternalU32Column);
impl_cast_col_into!(InternalU16Column, InternalU40Column);
impl_cast_col_into!(InternalU16Column, InternalU48Column);
impl_cast_col_into!(InternalU16Column, InternalU56Column);
impl_cast_col_into!(InternalU16Column, InternalU64Column);
impl_cast_col_into!(InternalU16Column, InternalF32Column);
impl_cast_col_into!(InternalU16Column, InternalF64Column);

// U24; all except u8 and u16 are larger, so no conversion require checks except
// for u24 (really a u32) -> u8 or u16. u24 is actually a u32 internally and
// also a subset of u32, so u24 -> u32 is a noop. Also, u24 can perfectly fit
// within an f32 so this is also lossless and requires no checks.

impl_cast_col_int_to_int!(InternalU24Column, InternalU08Column);
impl_cast_col_int_to_int!(InternalU24Column, InternalU16Column);
impl_cast_col_noop!(InternalU24Column, InternalU24Column);
impl_cast_col_noop!(InternalU24Column, InternalU32Column); // target is larger
impl_cast_col_into!(InternalU24Column, InternalU40Column);
impl_cast_col_into!(InternalU24Column, InternalU48Column);
impl_cast_col_into!(InternalU24Column, InternalU56Column);
impl_cast_col_into!(InternalU24Column, InternalU64Column);
impl_int_to_float_lossless!(InternalU24Column, InternalF32Column);
impl_cast_col_into!(InternalU24Column, InternalF64Column);

// U32; requires the following special conversion logic:
//
// 1. -> u8/u16: these are smaller so check for loss
// 2. -> u24: this is the same type with a reduced range, so check for anything
//    over range and mutate in place without allocating new memory.
// 3. -> f32: anything larger than 2^24 will lose precision, so need to check
//
// u32 can perfectly fit in an f64 so this is lossless

impl_cast_col_int_to_int!(InternalU32Column, InternalU08Column);
impl_cast_col_int_to_int!(InternalU32Column, InternalU16Column);
impl_truncate_from_samesize_int!(InternalU32Column, InternalU24Column);
impl_cast_col_noop!(InternalU32Column, InternalU32Column);
impl_cast_col_into!(InternalU32Column, InternalU40Column);
impl_cast_col_into!(InternalU32Column, InternalU48Column);
impl_cast_col_into!(InternalU32Column, InternalU56Column);
impl_cast_col_into!(InternalU32Column, InternalU64Column);
impl_cast_col_int_to_float!(InternalU32Column, InternalF32Column);
impl_cast_col_into!(InternalU32Column, InternalF64Column);

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

impl_cast_col_int_to_int!(InternalU40Column, InternalU08Column);
impl_cast_col_int_to_int!(InternalU40Column, InternalU16Column);
impl_truncate_from_int!(InternalU40Column, InternalU24Column);
impl_cast_col_int_to_int!(InternalU40Column, InternalU32Column);
impl_cast_col_noop!(InternalU40Column, InternalU40Column);
impl_cast_col_noop!(InternalU40Column, InternalU48Column); // target is larger
impl_cast_col_noop!(InternalU40Column, InternalU56Column); // target is larger
impl_cast_col_noop!(InternalU40Column, InternalU64Column); // target is larger
impl_cast_col_int_to_float!(InternalU40Column, InternalF32Column);
impl_int_to_float_lossless!(InternalU40Column, InternalF64Column);

// U48; This is the same as u40 except that u48 -> u40 is an in-place truncation
// and check since u40 is the same underlying type as u48 except with a smaller
// range.

impl_cast_col_int_to_int!(InternalU48Column, InternalU08Column);
impl_cast_col_int_to_int!(InternalU48Column, InternalU16Column);
impl_truncate_from_int!(InternalU48Column, InternalU24Column);
impl_cast_col_int_to_int!(InternalU48Column, InternalU32Column);
impl_truncate_from_samesize_int!(InternalU48Column, InternalU40Column);
impl_cast_col_noop!(InternalU48Column, InternalU48Column);
impl_cast_col_noop!(InternalU48Column, InternalU56Column); // target is larger
impl_cast_col_noop!(InternalU48Column, InternalU64Column); // target is larger
impl_cast_col_int_to_float!(InternalU48Column, InternalF32Column);
impl_int_to_float_lossless!(InternalU48Column, InternalF64Column);

// U56; This is the same as u48 and u40, continuing the same pattern.
//
// The only other difference between this and u48 is that f64 conversion is no
// longer totally lossless, so this needs a precision check.

impl_cast_col_int_to_int!(InternalU56Column, InternalU08Column);
impl_cast_col_int_to_int!(InternalU56Column, InternalU16Column);
impl_truncate_from_int!(InternalU56Column, InternalU24Column);
impl_cast_col_int_to_int!(InternalU56Column, InternalU32Column);
impl_truncate_from_samesize_int!(InternalU56Column, InternalU40Column);
impl_truncate_from_samesize_int!(InternalU56Column, InternalU48Column);
impl_cast_col_noop!(InternalU56Column, InternalU56Column); // target is larger
impl_cast_col_noop!(InternalU56Column, InternalU64Column);
impl_cast_col_int_to_float!(InternalU56Column, InternalF32Column);
impl_cast_col_int_to_float!(InternalU56Column, InternalF64Column);

// U64; Generally the same as u56, continuing the same pattern

impl_cast_col_int_to_int!(InternalU64Column, InternalU08Column);
impl_cast_col_int_to_int!(InternalU64Column, InternalU16Column);
impl_truncate_from_int!(InternalU64Column, InternalU24Column);
impl_cast_col_int_to_int!(InternalU64Column, InternalU32Column);
impl_truncate_from_samesize_int!(InternalU64Column, InternalU40Column);
impl_truncate_from_samesize_int!(InternalU64Column, InternalU48Column);
impl_truncate_from_samesize_int!(InternalU64Column, InternalU56Column);
impl_cast_col_noop!(InternalU64Column, InternalU64Column);
impl_cast_col_int_to_float!(InternalU64Column, InternalF32Column);
impl_cast_col_int_to_float!(InternalU64Column, InternalF64Column);

// F32; When converting to a primitive integer, this conversion requires a
// loss of precision check. When converting to an unaligned integer (u24, etc)
// this additionally requires a range check to ensure the integer value is not
// out of range.
//
// f32 -> f64 is lossless, see
// https://doc.rust-lang.org/reference/expressions/operator-expr.html#r-expr.as.numeric.float-widening

impl_cast_col_float_to_int!(InternalF32Column, InternalU08Column);
impl_cast_col_float_to_int!(InternalF32Column, InternalU16Column);
impl_truncate_from_float!(InternalF32Column, InternalU24Column);
impl_cast_col_float_to_int!(InternalF32Column, InternalU32Column);
impl_truncate_from_float!(InternalF32Column, InternalU40Column);
impl_truncate_from_float!(InternalF32Column, InternalU48Column);
impl_truncate_from_float!(InternalF32Column, InternalU56Column);
impl_cast_col_float_to_int!(InternalF32Column, InternalU64Column);
impl_cast_col_noop!(InternalF32Column, InternalF32Column);
impl_cast_col_into!(InternalF32Column, InternalF64Column);

// F64; same as f32 except going from f64 to f32 requires a loss of precision
// check

impl_cast_col_float_to_int!(InternalF64Column, InternalU08Column);
impl_cast_col_float_to_int!(InternalF64Column, InternalU16Column);
impl_truncate_from_float!(InternalF64Column, InternalU24Column);
impl_cast_col_float_to_int!(InternalF64Column, InternalU32Column);
impl_truncate_from_float!(InternalF64Column, InternalU40Column);
impl_truncate_from_float!(InternalF64Column, InternalU48Column);
impl_truncate_from_float!(InternalF64Column, InternalU56Column);
impl_cast_col_float_to_int!(InternalF64Column, InternalU64Column);

impl FromColumn<InternalF64Column> for InternalF32Column {
    #[allow(clippy::float_cmp)]
    fn from_column(col: InternalF64Column) -> CastColResult<Self> {
        let go = |x: f64| {
            let new_value: f32 = x.as_();
            let old_value = f64::from(new_value);
            (new_value, x != old_value)
        };
        map_buffer(&col.inner, go).fmap_once(Self::new)
    }
}

impl_cast_col_noop!(InternalF64Column, InternalF64Column);

fn buffer_int_to_float<I, F>(buf: &Buffer<I>) -> CastColResult<Buffer<F>>
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

fn buffer_int_to_int<I0, I1>(buf: &Buffer<I0>) -> CastColResult<Buffer<I1>>
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

fn buffer_float_to_int<I, F>(buf: &Buffer<F>) -> CastColResult<Buffer<I>>
where
    I: Bounded + AsPrimitive<F>,
    F: Float + AsPrimitive<I>,
    Buffer<I>: From<Vec<I>>,
{
    map_buffer(buf, |x: F| (x.as_(), !float_is_uint::<F, I>(x)))
}

fn map_buffer<F, X, Y>(buf: &Buffer<X>, mut f: F) -> CastColResult<Buffer<Y>>
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
    CastColResult::new(Buffer::from(new), err)
}

fn map_buffer_iso<F, X>(buf: Buffer<X>, mut f: F) -> CastColResult<Buffer<X>>
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
    CastColResult::new(Buffer::from(inner), err)
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

impl PartialEq for AnyPrimitiveColumn {
    /// Test for numeric equality between two columns.
    ///
    /// This will attempt to convert b/t datatypes when testing equality; for
    /// example, a `1` / `1.0` will be equal regardless of datatype because
    /// it can be losslessly converted between all possible types for a column
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

impl<T> From<Vec<T>> for PrimitiveColumn<T> {
    fn from(value: Vec<T>) -> Self {
        Self(value.into())
    }
}

impl AnyPrimitiveColumn {
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

    // /// The number of bytes occupied by the column if written as ASCII
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

/// Error when building [`FCSDataFrame`] from individual columns
#[derive(Debug, Error)]
#[error("column lengths to not match")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct NewDataframeError;

/// Error when new column has number of rows which are not equal to that in [`FCSDataFrame`]
#[derive(Debug, Error)]
#[error("column length ({col_len}) is different from number of rows in dataframe ({df_len})")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct ColumnLengthError {
    df_len: usize,
    col_len: usize,
}

#[delegatable_trait]
pub trait HasLen {
    // this will be used for vectors, len is always constant
    #[allow(clippy::len_without_is_empty)]
    fn len(&self) -> usize;
}

impl<T> HasLen for PrimitiveColumn<T> {
    fn len(&self) -> usize {
        self.0.len()
    }
}

impl<T, R> HasLen for InternalColumn<T, R> {
    fn len(&self) -> usize {
        self.inner.len()
    }
}

impl<C> FFDataFrame<C> {
    pub fn try_new(columns: impl IntoIterator<Item = C>) -> Result<Self, NewDataframeError>
    where
        C: HasLen,
    {
        let mut it = columns.into_iter();
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
    pub fn new1(column: C) -> Self
    where
        C: HasLen,
    {
        Self {
            nrows: column.len(),
            columns: vec![column],
        }
    }

    pub fn clear(&mut self) {
        self.columns = Vec::default();
        self.nrows = 0;
    }

    pub fn iter_columns(&self) -> Iter<'_, C> {
        self.columns.iter()
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
        self.columns.len()
    }

    #[must_use]
    pub fn size(&self) -> u64 {
        u64::try_from(self.ncols() * self.nrows()).expect("cells in dataframe exceed 2^64")
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.ncols() == 0
    }

    pub(crate) fn drop_in_place(&mut self, i: usize) -> Option<C>
    where
        C: HasLen,
    {
        if i > self.columns.len() {
            None
        } else {
            Some(self.columns.remove(i))
        }
    }

    pub(crate) fn push_column_nocheck(&mut self, col: C)
    where
        C: HasLen,
    {
        debug_assert!(
            self.check_new_column(&col).is_ok(),
            "new column length differs from number of rows"
        );
        if self.is_empty() {
            *self = Self::new1(col);
        } else {
            self.columns.push(col);
        }
    }

    // will panic if index is out of bounds
    pub(crate) fn insert_column_nocheck(&mut self, i: usize, col: C)
    where
        C: HasLen,
    {
        debug_assert!(
            self.check_new_column(&col).is_ok(),
            "new column length differs from number of rows"
        );
        if self.is_empty() {
            self.nrows = col.len();
        }
        // don't use Self::new1 here since we want to panic if i is out of
        // bounds
        self.columns.insert(i, col);
    }

    pub(crate) fn check_new_column(&self, col: &C) -> Result<(), ColumnLengthError>
    where
        C: HasLen,
    {
        if let Some(df_len) = self.nrows_nonempty() {
            let col_len = col.len();
            if col_len != df_len {
                return Err(ColumnLengthError { df_len, col_len });
            }
        }
        Ok(())
    }
}

// impl PrimitiveDataFrame {
//     /// Return number of bytes this will occupy if written as delimited ASCII
//     pub(crate) fn ascii_nbytes(&self) -> u64 {
//         let n = self.size();
//         if n == 0 {
//             return 0;
//         }
//         let ndelim = n - 1;
//         let ndigits: u32 = self.iter_columns().map(AnyFCSColumn::ascii_nbytes).sum();
//         u64::from(ndigits) + ndelim
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

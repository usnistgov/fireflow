use crate::macros::match_many_to_one;
use crate::validated::ascii_range::Chars;

use type_families::{FunctorOnce as _, impl_functor_once, impl_kind1};

use derive_more::{AsRef, Display, From, Into};
use derive_new::new;
use num_traits::bounds::Bounded;
use num_traits::cast::AsPrimitive;
use num_traits::float::Float;
use num_traits::identities::Zero as _;
use num_traits::int::PrimInt;
use polars_arrow::buffer::Buffer;
use thiserror::Error;

use std::iter;
use std::slice::Iter;

#[cfg(feature = "python")]
use {fireflow_core_proc::DisplayAsPyErr, fireflow_types::python as py};

/// Column-major dataframe to represent events in DATA
///
/// This is a very light wrapper around a polars buffer which is ref-counted and
/// therefore allows us to return event to external interfaces without copying
/// memory. It is validated to contain no NULL values where all columns have the
/// same length.
#[derive(Clone, PartialEq, new)]
#[new(visibility = "")]
pub struct FCSDataFrame0<C> {
    columns: Vec<C>,
    nrows: usize,
}

impl<C> Default for FCSDataFrame0<C> {
    fn default() -> Self {
        Self::new(vec![], 0)
    }
}

pub type FCSDataFrame = FCSDataFrame0<AnyFCSColumn>;

/// Any valid column from [`FCSDataFrame`]
#[derive(Clone, From)]
pub enum AnyFCSColumn {
    U08(U08Column),
    U16(U16Column),
    U32(U32Column),
    U64(U64Column),
    F32(F32Column),
    F64(F64Column),
}

#[derive(Clone, From)]
pub enum AnyInternalColumn {
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
#[as_ref([T])]
pub struct FCSColumn<T>(pub Buffer<T>);

pub type U08Column = FCSColumn<u8>;
pub type U16Column = FCSColumn<u16>;
pub type U32Column = FCSColumn<u32>;
pub type U64Column = FCSColumn<u64>;
pub type F32Column = FCSColumn<f32>;
pub type F64Column = FCSColumn<f64>;

/// A generic column for [`FCSDataFrame`]
#[derive(Clone, Into)]
pub struct InternalColumn<T, const LEN: usize>(FCSColumn<T>);

macro_rules! decl_struct_col {
    ($t:ident, $inner:ident, $len:expr) => {
        pub type $t = InternalColumn<$inner, $len>;
    };
}

decl_struct_col!(InternalU08Column, u8, 1);
decl_struct_col!(InternalU16Column, u16, 2);
decl_struct_col!(InternalU24Column, u32, 3);
decl_struct_col!(InternalU32Column, u32, 4);
decl_struct_col!(InternalU40Column, u64, 5);
decl_struct_col!(InternalU48Column, u64, 6);
decl_struct_col!(InternalU56Column, u64, 7);
decl_struct_col!(InternalU64Column, u64, 8);
decl_struct_col!(InternalF32Column, f32, 4);
decl_struct_col!(InternalF64Column, f64, 8);

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
                CastColResult::new(Self(col.0), None)
            }
        }
    };
}

macro_rules! impl_cast_col_lossless {
    ($from:ident, $to:ident) => {
        impl FromColumn<$from> for $to {
            fn from_column(col: $from) -> CastColResult<Self> {
                map_buffer(&col.0, |x| (x.into(), false)).fmap_once(Self)
            }
        }
    };
}

macro_rules! impl_cast_col_int_to_int {
    ($from:ident, $to:ident) => {
        impl FromColumn<$from> for $to {
            fn from_column(col: $from) -> CastColResult<Self> {
                buffer_int_to_int(&col.0).fmap_once(Self)
            }
        }
    };
}

macro_rules! impl_cast_col_int_to_float {
    ($from:ident, $to:ident) => {
        impl FromColumn<$from> for $to {
            fn from_column(col: $from) -> CastColResult<Self> {
                buffer_int_to_float(&col.0).fmap_once(Self)
            }
        }
    };
}

macro_rules! impl_cast_col_float_to_int {
    ($from:ident, $to:ident) => {
        impl FromColumn<$from> for $to {
            fn from_column(col: $from) -> CastColResult<Self> {
                buffer_float_to_int(&col.0).fmap_once(Self)
            }
        }
    };
}

impl_cast_col_noop!(U08Column, U08Column);
impl_cast_col_lossless!(U08Column, U16Column);
impl_cast_col_lossless!(U08Column, U32Column);
impl_cast_col_lossless!(U08Column, U64Column);
impl_cast_col_lossless!(U08Column, F32Column);
impl_cast_col_lossless!(U08Column, F64Column);

impl_cast_col_int_to_int!(U16Column, U08Column);
impl_cast_col_noop!(U16Column, U16Column);
impl_cast_col_lossless!(U16Column, U32Column);
impl_cast_col_lossless!(U16Column, U64Column);
impl_cast_col_lossless!(U16Column, F32Column);
impl_cast_col_lossless!(U16Column, F64Column);

impl_cast_col_int_to_int!(U32Column, U08Column);
impl_cast_col_int_to_int!(U32Column, U16Column);
impl_cast_col_noop!(U32Column, U32Column);
impl_cast_col_lossless!(U32Column, U64Column);
impl_cast_col_int_to_float!(U32Column, F32Column);
impl_cast_col_lossless!(U32Column, F64Column);

impl_cast_col_int_to_int!(U64Column, U08Column);
impl_cast_col_int_to_int!(U64Column, U16Column);
impl_cast_col_int_to_int!(U64Column, U32Column);
impl_cast_col_noop!(U64Column, U64Column);
impl_cast_col_int_to_float!(U64Column, F32Column);
impl_cast_col_int_to_float!(U64Column, F64Column);

impl_cast_col_float_to_int!(F32Column, U08Column);
impl_cast_col_float_to_int!(F32Column, U16Column);
impl_cast_col_float_to_int!(F32Column, U32Column);
impl_cast_col_float_to_int!(F32Column, U64Column);
impl_cast_col_noop!(F32Column, F32Column);
// this will always be lossless, see
// https://doc.rust-lang.org/reference/expressions/operator-expr.html#r-expr.as.numeric.float-widening
impl_cast_col_lossless!(F32Column, F64Column);

impl_cast_col_float_to_int!(F64Column, U08Column);
impl_cast_col_float_to_int!(F64Column, U16Column);
impl_cast_col_float_to_int!(F64Column, U32Column);
impl_cast_col_float_to_int!(F64Column, U64Column);

impl FromColumn<F64Column> for F32Column {
    #[allow(clippy::float_cmp)]
    fn from_column(col: F64Column) -> CastColResult<Self> {
        let go = |x: f64| {
            let new_value: f32 = x.as_();
            let old_value = f64::from(new_value);
            (new_value, x != old_value)
        };
        map_buffer(&col.0, go).fmap_once(Self)
    }
}

impl_cast_col_noop!(F64Column, F64Column);

// Primitive->internal column casts
//
// This is the same as the primitive-level casts above with the added complexity
// of checking for integer ranges in the case where the internal target is an
// unaligned integer (24, 40, 48, and 56 bit).

/// Convert primitive column to internal column by wrapping the primitive type.
///
/// The assumes the primitive->primitive mapping will account for all loss.
/// For example, u8 -> f32 (no loss), or u32 -> u16 (loss of bytes). It will
/// not properly deal with u64 -> u24 since it won't check the truncation range.
macro_rules! impl_prim_to_internal_wrap {
    ($from:ident, $to:ident) => {
        impl FromColumn<$from> for $to {
            fn from_column(col: $from) -> CastColResult<Self> {
                FromColumn::from_column(col).fmap_once(Self)
            }
        }
    };
}

/// Cast an integer column to a truncated column with the same internal type.
///
/// This is more optimal than a general cast because we don't need to reallocate
/// a new buffer; we only need to check the range of the current values and clip
/// if needed.
macro_rules! impl_int_to_trunc_int {
    ($from:ident, $to:ident, $n:expr) => {
        impl FromColumn<$from> for $to {
            fn from_column(col: $from) -> CastColResult<Self> {
                buffer_int_to_trunc_int(col.0, (1 << $n) - 1)
                    .fmap_once(FCSColumn)
                    .fmap_once(Self)
            }
        }
    };
}

/// Cast a column to an unaligned integer type via its aligned intermediate
///
/// For example, f64->u32->u24 where the first conversion checks for losses
/// going from f32 to u32 and the second checks the range of the u32 to see if
/// it fits in a u24.
macro_rules! impl_col_to_trunc_int_via_primitive {
    ($from:ident, $to:ident) => {
        impl FromColumn<$from> for $to {
            fn from_column(col: $from) -> CastColResult<Self> {
                let res = FromColumn::from_column(col);
                if res.loss_position.is_some() {
                    res.fmap_once(Self)
                } else {
                    Self::from_column(res.inner)
                }
            }
        }
    };
}

// U08

impl_prim_to_internal_wrap!(U08Column, InternalU08Column);
impl_prim_to_internal_wrap!(U08Column, InternalU16Column);
impl_prim_to_internal_wrap!(U08Column, InternalU24Column);
impl_prim_to_internal_wrap!(U08Column, InternalU32Column);
impl_prim_to_internal_wrap!(U08Column, InternalU40Column);
impl_prim_to_internal_wrap!(U08Column, InternalU48Column);
impl_prim_to_internal_wrap!(U08Column, InternalU56Column);
impl_prim_to_internal_wrap!(U08Column, InternalU64Column);
impl_prim_to_internal_wrap!(U08Column, InternalF32Column);
impl_prim_to_internal_wrap!(U08Column, InternalF64Column);

// U16

impl_prim_to_internal_wrap!(U16Column, InternalU08Column);
impl_prim_to_internal_wrap!(U16Column, InternalU16Column);
impl_prim_to_internal_wrap!(U16Column, InternalU24Column);
impl_prim_to_internal_wrap!(U16Column, InternalU32Column);
impl_prim_to_internal_wrap!(U16Column, InternalU40Column);
impl_prim_to_internal_wrap!(U16Column, InternalU48Column);
impl_prim_to_internal_wrap!(U16Column, InternalU56Column);
impl_prim_to_internal_wrap!(U16Column, InternalU64Column);
impl_prim_to_internal_wrap!(U16Column, InternalF32Column);
impl_prim_to_internal_wrap!(U16Column, InternalF64Column);

// U32

impl_prim_to_internal_wrap!(U32Column, InternalU08Column);
impl_prim_to_internal_wrap!(U32Column, InternalU16Column);
impl_int_to_trunc_int!(U32Column, InternalU24Column, 24);
impl_prim_to_internal_wrap!(U32Column, InternalU32Column);
impl_prim_to_internal_wrap!(U32Column, InternalU40Column);
impl_prim_to_internal_wrap!(U32Column, InternalU48Column);
impl_prim_to_internal_wrap!(U32Column, InternalU56Column);
impl_prim_to_internal_wrap!(U32Column, InternalU64Column);
impl_prim_to_internal_wrap!(U32Column, InternalF32Column);
impl_prim_to_internal_wrap!(U32Column, InternalF64Column);

// U64

impl_prim_to_internal_wrap!(U64Column, InternalU08Column);
impl_prim_to_internal_wrap!(U64Column, InternalU16Column);
impl_col_to_trunc_int_via_primitive!(U64Column, InternalU24Column);
impl_prim_to_internal_wrap!(U64Column, InternalU32Column);
impl_int_to_trunc_int!(U64Column, InternalU40Column, 40);
impl_int_to_trunc_int!(U64Column, InternalU48Column, 48);
impl_int_to_trunc_int!(U64Column, InternalU56Column, 56);
impl_prim_to_internal_wrap!(U64Column, InternalU64Column);
impl_prim_to_internal_wrap!(U64Column, InternalF32Column);
impl_prim_to_internal_wrap!(U64Column, InternalF64Column);

// F32

impl_prim_to_internal_wrap!(F32Column, InternalU08Column);
impl_prim_to_internal_wrap!(F32Column, InternalU16Column);
impl_col_to_trunc_int_via_primitive!(F32Column, InternalU24Column);
impl_prim_to_internal_wrap!(F32Column, InternalU32Column);
impl_col_to_trunc_int_via_primitive!(F32Column, InternalU40Column);
impl_col_to_trunc_int_via_primitive!(F32Column, InternalU48Column);
impl_col_to_trunc_int_via_primitive!(F32Column, InternalU56Column);
impl_prim_to_internal_wrap!(F32Column, InternalU64Column);
impl_prim_to_internal_wrap!(F32Column, InternalF32Column);
impl_prim_to_internal_wrap!(F32Column, InternalF64Column);

// F64

impl_prim_to_internal_wrap!(F64Column, InternalU08Column);
impl_prim_to_internal_wrap!(F64Column, InternalU16Column);
impl_col_to_trunc_int_via_primitive!(F64Column, InternalU24Column);
impl_prim_to_internal_wrap!(F64Column, InternalU32Column);
impl_col_to_trunc_int_via_primitive!(F64Column, InternalU40Column);
impl_col_to_trunc_int_via_primitive!(F64Column, InternalU48Column);
impl_col_to_trunc_int_via_primitive!(F64Column, InternalU56Column);
impl_prim_to_internal_wrap!(F64Column, InternalU64Column);
impl_prim_to_internal_wrap!(F64Column, InternalF32Column);
impl_prim_to_internal_wrap!(F64Column, InternalF64Column);

// Internal->internal column casts
//
// This can be implemented using primitive->internal instances defined above.
//
// However, in a few cases it is more efficient to use a direct cast. These are
// u24->f32, u40->f64, and u48->f64. In these cases we know that the cast will
// never fail since all these integer ranges fit perfectly into their respective
// floats, which lets us bypass the double-cast check needed in the more general
// case to ensure the int is within a range where it can be exactly represented
// without loss of precision.

/// Cast internal column to another internal column via primitive->internal cast.
macro_rules! impl_internal_to_internal_delegate {
    ($from:ident, $to:ident) => {
        impl FromColumn<$from> for $to {
            fn from_column(col: $from) -> CastColResult<Self> {
                FromColumn::from_column(col.0)
            }
        }
    };
}

/// Convert int column to float column using as conversion.
///
/// This should only be used for integers that have been verified to fit
/// in the range of the target float. It will never return a lossy result.
macro_rules! impl_cast_col_float_to_int_as {
    ($from:ident, $to:ident) => {
        impl FromColumn<$from> for $to {
            fn from_column(col: $from) -> CastColResult<Self> {
                map_buffer(&col.0.0, |x| (x.as_(), false))
                    .fmap_once(FCSColumn)
                    .fmap_once(Self)
            }
        }
    };
}

// U08

impl_internal_to_internal_delegate!(InternalU08Column, InternalU08Column);
impl_internal_to_internal_delegate!(InternalU08Column, InternalU16Column);
impl_internal_to_internal_delegate!(InternalU08Column, InternalU24Column);
impl_internal_to_internal_delegate!(InternalU08Column, InternalU32Column);
impl_internal_to_internal_delegate!(InternalU08Column, InternalU40Column);
impl_internal_to_internal_delegate!(InternalU08Column, InternalU48Column);
impl_internal_to_internal_delegate!(InternalU08Column, InternalU56Column);
impl_internal_to_internal_delegate!(InternalU08Column, InternalU64Column);
impl_internal_to_internal_delegate!(InternalU08Column, InternalF32Column);
impl_internal_to_internal_delegate!(InternalU08Column, InternalF64Column);

// U16

impl_internal_to_internal_delegate!(InternalU16Column, InternalU08Column);
impl_internal_to_internal_delegate!(InternalU16Column, InternalU16Column);
impl_internal_to_internal_delegate!(InternalU16Column, InternalU24Column);
impl_internal_to_internal_delegate!(InternalU16Column, InternalU32Column);
impl_internal_to_internal_delegate!(InternalU16Column, InternalU40Column);
impl_internal_to_internal_delegate!(InternalU16Column, InternalU48Column);
impl_internal_to_internal_delegate!(InternalU16Column, InternalU56Column);
impl_internal_to_internal_delegate!(InternalU16Column, InternalU64Column);
impl_internal_to_internal_delegate!(InternalU16Column, InternalF32Column);
impl_internal_to_internal_delegate!(InternalU16Column, InternalF64Column);

// U24

impl_internal_to_internal_delegate!(InternalU24Column, InternalU08Column);
impl_internal_to_internal_delegate!(InternalU24Column, InternalU16Column);
impl_internal_to_internal_delegate!(InternalU24Column, InternalU24Column);
impl_internal_to_internal_delegate!(InternalU24Column, InternalU32Column);
impl_internal_to_internal_delegate!(InternalU24Column, InternalU40Column);
impl_internal_to_internal_delegate!(InternalU24Column, InternalU48Column);
impl_internal_to_internal_delegate!(InternalU24Column, InternalU56Column);
impl_internal_to_internal_delegate!(InternalU24Column, InternalU64Column);
// upper integer limit for f32 is exactly 2^24 so this is lossless
impl_cast_col_float_to_int_as!(InternalU24Column, InternalF32Column);
impl_internal_to_internal_delegate!(InternalU24Column, InternalF64Column);

// U32

impl_internal_to_internal_delegate!(InternalU32Column, InternalU08Column);
impl_internal_to_internal_delegate!(InternalU32Column, InternalU16Column);
impl_internal_to_internal_delegate!(InternalU32Column, InternalU24Column);
impl_internal_to_internal_delegate!(InternalU32Column, InternalU32Column);
impl_internal_to_internal_delegate!(InternalU32Column, InternalU40Column);
impl_internal_to_internal_delegate!(InternalU32Column, InternalU48Column);
impl_internal_to_internal_delegate!(InternalU32Column, InternalU56Column);
impl_internal_to_internal_delegate!(InternalU32Column, InternalU64Column);
impl_internal_to_internal_delegate!(InternalU32Column, InternalF32Column);
impl_internal_to_internal_delegate!(InternalU32Column, InternalF64Column);

// U40

impl_internal_to_internal_delegate!(InternalU40Column, InternalU08Column);
impl_internal_to_internal_delegate!(InternalU40Column, InternalU16Column);
impl_internal_to_internal_delegate!(InternalU40Column, InternalU24Column);
impl_internal_to_internal_delegate!(InternalU40Column, InternalU32Column);
impl_internal_to_internal_delegate!(InternalU40Column, InternalU40Column);
impl_internal_to_internal_delegate!(InternalU40Column, InternalU48Column);
impl_internal_to_internal_delegate!(InternalU40Column, InternalU56Column);
impl_internal_to_internal_delegate!(InternalU40Column, InternalU64Column);
impl_internal_to_internal_delegate!(InternalU40Column, InternalF32Column);
// // upper integer limit for f64 is exactly 2^53 so this is lossless
impl_cast_col_float_to_int_as!(InternalU40Column, InternalF64Column);

// U48

impl_internal_to_internal_delegate!(InternalU48Column, InternalU08Column);
impl_internal_to_internal_delegate!(InternalU48Column, InternalU16Column);
impl_internal_to_internal_delegate!(InternalU48Column, InternalU24Column);
impl_internal_to_internal_delegate!(InternalU48Column, InternalU32Column);
impl_internal_to_internal_delegate!(InternalU48Column, InternalU40Column);
impl_internal_to_internal_delegate!(InternalU48Column, InternalU48Column);
impl_internal_to_internal_delegate!(InternalU48Column, InternalU56Column);
impl_internal_to_internal_delegate!(InternalU48Column, InternalU64Column);
impl_internal_to_internal_delegate!(InternalU48Column, InternalF32Column);
// // upper integer limit for f64 is exactly 2^53 so this is lossless
impl_cast_col_float_to_int_as!(InternalU48Column, InternalF64Column);

// U56

impl_internal_to_internal_delegate!(InternalU56Column, InternalU08Column);
impl_internal_to_internal_delegate!(InternalU56Column, InternalU16Column);
impl_internal_to_internal_delegate!(InternalU56Column, InternalU24Column);
impl_internal_to_internal_delegate!(InternalU56Column, InternalU32Column);
impl_internal_to_internal_delegate!(InternalU56Column, InternalU40Column);
impl_internal_to_internal_delegate!(InternalU56Column, InternalU48Column);
impl_internal_to_internal_delegate!(InternalU56Column, InternalU56Column);
impl_internal_to_internal_delegate!(InternalU56Column, InternalU64Column);
impl_internal_to_internal_delegate!(InternalU56Column, InternalF32Column);
impl_internal_to_internal_delegate!(InternalU56Column, InternalF64Column);

// U64

impl_internal_to_internal_delegate!(InternalU64Column, InternalU08Column);
impl_internal_to_internal_delegate!(InternalU64Column, InternalU16Column);
impl_internal_to_internal_delegate!(InternalU64Column, InternalU24Column);
impl_internal_to_internal_delegate!(InternalU64Column, InternalU32Column);
impl_internal_to_internal_delegate!(InternalU64Column, InternalU40Column);
impl_internal_to_internal_delegate!(InternalU64Column, InternalU48Column);
impl_internal_to_internal_delegate!(InternalU64Column, InternalU56Column);
impl_internal_to_internal_delegate!(InternalU64Column, InternalU64Column);
impl_internal_to_internal_delegate!(InternalU64Column, InternalF32Column);
impl_internal_to_internal_delegate!(InternalU64Column, InternalF64Column);

// F32

impl_internal_to_internal_delegate!(InternalF32Column, InternalU08Column);
impl_internal_to_internal_delegate!(InternalF32Column, InternalU16Column);
impl_internal_to_internal_delegate!(InternalF32Column, InternalU24Column);
impl_internal_to_internal_delegate!(InternalF32Column, InternalU32Column);
impl_internal_to_internal_delegate!(InternalF32Column, InternalU40Column);
impl_internal_to_internal_delegate!(InternalF32Column, InternalU48Column);
impl_internal_to_internal_delegate!(InternalF32Column, InternalU56Column);
impl_internal_to_internal_delegate!(InternalF32Column, InternalU64Column);
impl_internal_to_internal_delegate!(InternalF32Column, InternalF32Column);
impl_internal_to_internal_delegate!(InternalF32Column, InternalF64Column);

// F64

impl_internal_to_internal_delegate!(InternalF64Column, InternalU08Column);
impl_internal_to_internal_delegate!(InternalF64Column, InternalU16Column);
impl_internal_to_internal_delegate!(InternalF64Column, InternalU24Column);
impl_internal_to_internal_delegate!(InternalF64Column, InternalU32Column);
impl_internal_to_internal_delegate!(InternalF64Column, InternalU40Column);
impl_internal_to_internal_delegate!(InternalF64Column, InternalU48Column);
impl_internal_to_internal_delegate!(InternalF64Column, InternalU56Column);
impl_internal_to_internal_delegate!(InternalF64Column, InternalU64Column);
impl_internal_to_internal_delegate!(InternalF64Column, InternalF32Column);
impl_internal_to_internal_delegate!(InternalF64Column, InternalF64Column);

fn buffer_int_to_trunc_int<I: PrimInt>(buf: Buffer<I>, limit: I) -> CastColResult<Buffer<I>> {
    map_buffer_iso(buf, |x| (x, x > limit))
}

fn buffer_int_to_float<I, F>(buf: &Buffer<I>) -> CastColResult<Buffer<F>>
where
    I: PrimInt + AsPrimitive<F>,
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
    I1: PrimInt,
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
    I: PrimInt + AsPrimitive<F>,
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

impl PartialEq for AnyFCSColumn {
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
            I: PrimInt + AsPrimitive<F>,
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

impl<T> From<Vec<T>> for FCSColumn<T> {
    fn from(value: Vec<T>) -> Self {
        Self(value.into())
    }
}

impl AnyFCSColumn {
    #[must_use]
    pub fn len(&self) -> usize {
        match_many_to_one!(self, Self, [U08, U16, U32, U64, F32, F64], x, { x.0.len() })
    }

    pub(crate) fn check_writer<E, F, ToType>(&self, f: F) -> Result<(), LossError<E>>
    where
        F: Fn(ToType) -> Option<E>,
        ToType: AllFCSCast,
    {
        match_many_to_one!(self, Self, [U08, U16, U32, U64, F32, F64], xs, {
            IsFCSDataType::check_writer(xs, f)
        })
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

    /// The number of bytes occupied by the column if written as ASCII
    #[must_use]
    pub fn ascii_nbytes(&self) -> u32 {
        match self {
            Self::U08(xs) => u8::as_col_iter::<u64>(xs).map(|x| cast_nbytes(&x)).sum(),
            Self::U16(xs) => u16::as_col_iter::<u64>(xs).map(|x| cast_nbytes(&x)).sum(),
            Self::U32(xs) => u32::as_col_iter::<u64>(xs).map(|x| cast_nbytes(&x)).sum(),
            Self::U64(xs) => u64::as_col_iter::<u64>(xs).map(|x| cast_nbytes(&x)).sum(),
            Self::F32(xs) => f32::as_col_iter::<u64>(xs).map(|x| cast_nbytes(&x)).sum(),
            Self::F64(xs) => f64::as_col_iter::<u64>(xs).map(|x| cast_nbytes(&x)).sum(),
        }
    }
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

impl FCSDataFrame {
    pub fn try_new(
        columns: impl IntoIterator<Item = AnyFCSColumn>,
    ) -> Result<Self, NewDataframeError> {
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
    pub fn new1(column: AnyFCSColumn) -> Self {
        Self {
            nrows: column.len(),
            columns: vec![column],
        }
    }

    pub fn clear(&mut self) {
        self.columns = Vec::default();
        self.nrows = 0;
    }

    pub fn iter_columns(&self) -> Iter<'_, AnyFCSColumn> {
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

    pub(crate) fn drop_in_place(&mut self, i: usize) -> Option<AnyFCSColumn> {
        if i > self.columns.len() {
            None
        } else {
            Some(self.columns.remove(i))
        }
    }

    pub(crate) fn push_column_nocheck(&mut self, col: AnyFCSColumn) {
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
    pub(crate) fn insert_column_nocheck(&mut self, i: usize, col: AnyFCSColumn) {
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

    pub(crate) fn check_new_column(&self, col: &AnyFCSColumn) -> Result<(), ColumnLengthError> {
        if let Some(df_len) = self.nrows_nonempty() {
            let col_len = col.len();
            if col_len != df_len {
                return Err(ColumnLengthError { df_len, col_len });
            }
        }
        Ok(())
    }

    /// Return number of bytes this will occupy if written as delimited ASCII
    pub(crate) fn ascii_nbytes(&self) -> u64 {
        let n = self.size();
        if n == 0 {
            return 0;
        }
        let ndelim = n - 1;
        let ndigits: u32 = self.iter_columns().map(AnyFCSColumn::ascii_nbytes).sum();
        u64::from(ndigits) + ndelim
    }
}

pub(crate) type FCSColIter<'a, FromType, ToType> =
    iter::Map<iter::Copied<Iter<'a, FromType>>, fn(FromType) -> CastResult<ToType>>;

pub(crate) trait IsFCSDataType
where
    Self: Sized + Copy,
    [Self]: ToOwned,
{
    const NATIVE: FCSDatatype;

    /// Return iterator for column, converting to native type on the fly.
    fn as_col_iter<ToType>(c: &FCSColumn<Self>) -> FCSColIter<'_, Self, ToType>
    where
        ToType: NumCast<Self>,
    {
        Self::iter_native(c).map(ToType::from_truncated)
    }

    /// Try to convert column to native type, and return error on failure.
    ///
    /// This is separate from returning the iterator itself because if we can't
    /// tolerate any loss, the only way to find with only the iterator it is
    /// while we are using it to write a file, which opens the possibility of a
    /// partially-written file (not good). Therefore we need to check this
    /// before returning the iterator at all, which ironically can only be found
    /// by iterating the entire vector once.
    ///
    /// This only applies to the case where we want to crash if any loss will
    /// occur. If we only wish to warn the user and use lossy conversion
    /// anyways, this only requires one iteration since the iterator itself will
    /// return a [`CastResult`] which carries a flag if loss occurred.
    fn check_writer<E, F, ToType>(c: &FCSColumn<Self>, f: F) -> Result<(), LossError<E>>
    where
        F: Fn(ToType) -> Option<E>,
        ToType: NumCast<Self>,
    {
        for x in Self::as_col_iter::<ToType>(c) {
            x.resolve()?;
            if let Some(err) = f(x.new) {
                return Err(LossError::Other(err));
            }
        }
        Ok(())
    }

    fn iter_native(c: &FCSColumn<Self>) -> iter::Copied<Iter<'_, Self>> {
        c.0.iter().copied()
    }
}

/// Error when value in [`FCSDataFrame`] loses information (type conversion or something else)
#[derive(Clone, Copy, Display, Debug, Error)]
pub enum LossError<E> {
    Cast(#[from] CastError),
    Other(E),
}

/// Error when value in [`FCSDataFrame`] loses information due to type conversion
#[derive(Clone, Copy, Debug, Error, new)]
#[error("data loss occurred when converting from {from} to {to}")]
pub struct CastError {
    from: FCSDatatype,
    to: FCSDatatype,
}

impl IsFCSDataType for u8 {
    const NATIVE: FCSDatatype = FCSDatatype::U08;
}

impl IsFCSDataType for u16 {
    const NATIVE: FCSDatatype = FCSDatatype::U16;
}

impl IsFCSDataType for u32 {
    const NATIVE: FCSDatatype = FCSDatatype::U32;
}

impl IsFCSDataType for u64 {
    const NATIVE: FCSDatatype = FCSDatatype::U64;
}

impl IsFCSDataType for f32 {
    const NATIVE: FCSDatatype = FCSDatatype::F32;
}

impl IsFCSDataType for f64 {
    const NATIVE: FCSDatatype = FCSDatatype::F64;
}

#[cfg_attr(test, derive(Debug, PartialEq))]
pub(crate) struct CastResult<T> {
    pub(crate) new: T,
    pub(crate) lossy: Option<FCSDatatype>,
}

impl<T> CastResult<T> {
    fn new<FromT: IsFCSDataType>(new: T, has_loss: bool) -> Self {
        let lossy = has_loss.then_some(FromT::NATIVE);
        Self { new, lossy }
    }

    pub(crate) fn as_err(&self) -> Option<CastError>
    where
        T: IsFCSDataType,
    {
        self.lossy.map(|from| CastError::new(from, T::NATIVE))
    }

    pub(crate) fn resolve(&self) -> Result<(), CastError>
    where
        T: IsFCSDataType,
    {
        self.as_err().map_or(Ok(()), Err)
    }
}

pub(crate) trait NumCast<T>: Sized + IsFCSDataType {
    fn from_truncated(x: T) -> CastResult<Self>;
}

macro_rules! impl_cast_noloss {
    ($from:ident, $to:ident) => {
        impl NumCast<$from> for $to {
            fn from_truncated(x: $from) -> CastResult<Self> {
                CastResult {
                    new: x.into(),
                    lossy: None,
                }
            }
        }
    };
}

macro_rules! impl_cast_int_lossy {
    ($from:ident, $to:ident) => {
        impl NumCast<$from> for $to {
            fn from_truncated(x: $from) -> CastResult<Self> {
                if let Ok(new) = $to::try_from(x) {
                    CastResult::new::<$from>(new, false)
                } else {
                    CastResult::new::<$from>($to::MAX, true)
                }
            }
        }
    };
}

macro_rules! impl_cast_float_to_int_lossy {
    ($from:ident, $to:ident) => {
        impl NumCast<$from> for $to {
            #[allow(clippy::cast_precision_loss)]
            #[allow(clippy::cast_sign_loss)]
            #[allow(clippy::cast_lossless)]
            #[allow(clippy::cast_possible_truncation)]
            #[allow(clippy::as_conversions)]
            fn from_truncated(x: $from) -> CastResult<Self> {
                let has_loss = x.is_nan()
                    || x.is_infinite()
                    || x.is_sign_negative()
                    || !x.fract().is_zero()
                    || x > $to::MAX as $from;
                CastResult::new::<$from>(x as $to, has_loss)
            }
        }
    };
}

macro_rules! impl_cast_int_to_float_lossy {
    ($from:ident, $to:ident) => {
        impl NumCast<$from> for $to {
            #[allow(clippy::cast_precision_loss)]
            #[allow(clippy::cast_sign_loss)]
            #[allow(clippy::cast_possible_truncation)]
            #[allow(clippy::as_conversions)]
            fn from_truncated(x: $from) -> CastResult<Self> {
                let new = x as $to;
                let old = new as $from;
                CastResult::new::<$from>(new, old != x)
            }
        }
    };
}

impl_cast_noloss!(u8, u8);
impl_cast_noloss!(u8, u16);
impl_cast_noloss!(u8, u32);
impl_cast_noloss!(u8, u64);
impl_cast_noloss!(u8, f32);
impl_cast_noloss!(u8, f64);

impl_cast_int_lossy!(u16, u8);
impl_cast_noloss!(u16, u16);
impl_cast_noloss!(u16, u32);
impl_cast_noloss!(u16, u64);
impl_cast_noloss!(u16, f32);
impl_cast_noloss!(u16, f64);

impl_cast_int_lossy!(u32, u8);
impl_cast_int_lossy!(u32, u16);
impl_cast_noloss!(u32, u32);
impl_cast_noloss!(u32, u64);
impl_cast_int_to_float_lossy!(u32, f32);
impl_cast_noloss!(u32, f64);

impl_cast_int_lossy!(u64, u8);
impl_cast_int_lossy!(u64, u16);
impl_cast_int_lossy!(u64, u32);
impl_cast_noloss!(u64, u64);
impl_cast_int_to_float_lossy!(u64, f32);
impl_cast_int_to_float_lossy!(u64, f64);

impl_cast_float_to_int_lossy!(f32, u8);
impl_cast_float_to_int_lossy!(f32, u16);
impl_cast_float_to_int_lossy!(f32, u32);
impl_cast_float_to_int_lossy!(f32, u64);
impl_cast_noloss!(f32, f32);
// this will always be lossless, see
// https://doc.rust-lang.org/reference/expressions/operator-expr.html#r-expr.as.numeric.float-widening
impl_cast_noloss!(f32, f64);

impl_cast_float_to_int_lossy!(f64, u8);
impl_cast_float_to_int_lossy!(f64, u16);
impl_cast_float_to_int_lossy!(f64, u32);
impl_cast_float_to_int_lossy!(f64, u64);

impl NumCast<f64> for f32 {
    #[allow(clippy::cast_possible_truncation)]
    #[allow(clippy::float_cmp)]
    #[allow(clippy::as_conversions)]
    fn from_truncated(x: f64) -> CastResult<Self> {
        let new = x as Self;
        let old = f64::from(new);
        CastResult::new::<f64>(new, old != x)
    }
}

impl_cast_noloss!(f64, f64);

pub(crate) fn cast_nbytes(x: &CastResult<u64>) -> u32 {
    u8::from(Chars::from_u64(x.new)).into()
}

pub(crate) trait AllFCSCast:
    NumCast<u8> + NumCast<u16> + NumCast<u32> + NumCast<u64> + NumCast<f32> + NumCast<f64>
{
}

impl<T> AllFCSCast for T where
    T: NumCast<u8> + NumCast<u16> + NumCast<u32> + NumCast<u64> + NumCast<f32> + NumCast<f64>
{
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

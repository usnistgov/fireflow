use bytemuck::NoUninit;
use derive_more::{From, Into};
use num_traits::Bounded;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Into, From, NoUninit)]
#[into(u32, u64)]
#[from(u8, u16)]
#[repr(transparent)]
pub(crate) struct U24(u32);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Into, From, NoUninit)]
#[into(u64)]
#[from(u8, u16, u32)]
#[repr(transparent)]
pub(crate) struct U40(u64);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Into, From, NoUninit)]
#[into(u64)]
#[from(u8, u16, u32)]
#[repr(transparent)]
pub(crate) struct U48(u64);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Into, From, NoUninit)]
#[into(u64)]
#[from(u8, u16, u32)]
#[repr(transparent)]
pub(crate) struct U56(u64);

pub(crate) struct TryFromUnalignedIntError;

// /// A Vec with an inner type which may be lossily converted to an unaligned type.
// pub(crate) trait CastableVec<Inner>: Sized {
//     fn cast_from_vec(xs: Vec<Inner>) -> (Vec<Self>, Option<usize>);
// }

macro_rules! impl_unaligned {
    ($from:ident, $to:ident, $n:expr) => {
        impl TryFrom<$from> for $to {
            type Error = TryFromUnalignedIntError;

            fn try_from(value: $from) -> Result<Self, Self::Error> {
                if value > $from::from($to::max_value()) {
                    Err(TryFromUnalignedIntError)
                } else {
                    Ok(Self(value))
                }
            }
        }

        impl Bounded for $to {
            fn min_value() -> Self {
                Self(0)
            }

            fn max_value() -> Self {
                Self((1 << $n) - 1)
            }
        }

        // impl CastableVec<$from> for $to {
        //     fn cast_from_vec(mut xs: Vec<$from>) -> (Vec<Self>, Option<usize>) {
        //         let mut err = None;
        //         for (i, x) in xs.iter_mut().enumerate() {
        //             let upper = $from::from(Self::max_value());
        //             if *x > upper {
        //                 *x = upper;
        //                 if err.is_none() {
        //                     err = Some(i);
        //                 }
        //             }
        //         }
        //         let n = xs.len();
        //         let cap = xs.capacity();
        //         let ptr = xs.as_mut_ptr().cast::<Self>();
        //         forget(xs);
        //         // SAFETY: we checked that each input is with range, and the
        //         // target type is a transparent wrapper around the input type so
        //         // the pointer cast is valid.
        //         let new = unsafe { Vec::from_raw_parts(ptr, n, cap) };
        //         (new, err)
        //     }
        // }
    };
}

impl_unaligned!(u32, U24, 24);
impl_unaligned!(u64, U40, 40);
impl_unaligned!(u64, U48, 48);
impl_unaligned!(u64, U56, 56);

// macro_rules! impl_castable_vec_unaligned {
//     ($from:ident, $inner:ident, $to:ident) => {
//         impl CastableVec<$from> for $to {
//             fn cast_from_vec(xs: Vec<$from>) -> (Vec<Self>, Option<usize>) {
//                 let prim: Vec<$inner> = cast_vec(xs);
//                 Self::cast_from_vec(prim)
//             }
//         }
//     };
// }

// impl_castable_vec_unaligned!(U48, u64, U40);
// impl_castable_vec_unaligned!(U56, u64, U40);
// impl_castable_vec_unaligned!(U56, u64, U48);

// macro_rules! impl_castable_small_to_big {
//     ($from:ident, $to:ident) => {
//         impl CastableVec<$from> for $to {
//             fn cast_from_vec(mut xs: Vec<$from>) -> (Vec<Self>, Option<usize>) {
//                 let n = xs.len();
//                 let cap = xs.capacity();
//                 let ptr = xs.as_mut_ptr().cast::<Self>();
//                 forget(xs);
//                 // SAFETY: input type should be a subset of target type, so cast
//                 // will never result in an invalid bit patter. Also they should
//                 // have the same layout since they should have the same inner
//                 // type. Both of these are assumed to be true and bad stuff
//                 // happens if they are not.
//                 let new = unsafe { Vec::from_raw_parts(ptr, n, cap) };
//                 (new, None)
//             }
//         }
//     };
// }

// impl_castable_small_to_big!(U40, U48);
// impl_castable_small_to_big!(U40, U56);
// impl_castable_small_to_big!(U48, U56);

// /// Make conversion from smaller number to bigger type (which will never fail).
// macro_rules! impl_small_to_big {
//     ($from:ident, $to:ident) => {
//         impl From<$from> for $to {
//             fn from(value: $from) -> Self {
//                 Self(value.0.into())
//             }
//         }
//     };
// }

// impl_small_to_big!(U24, U40);
// impl_small_to_big!(U24, U48);
// impl_small_to_big!(U24, U56);
// impl_small_to_big!(U40, U48);
// impl_small_to_big!(U40, U56);
// impl_small_to_big!(U48, U56);

// // special case since this is a primitive type that can be converted to a
// // smaller type which has a corresponding unaligned type
// impl TryFrom<u64> for U24 {
//     type Error = TryFromIntError;
//     fn try_from(value: u64) -> Result<Self, Self::Error> {
//         value.try_into()
//     }
// }

// /// Make fallible conversion from bigger type to smaller primitive type
// macro_rules! impl_big_to_small_prim {
//     ($from:ident, $to:ident) => {
//         impl TryFrom<$from> for $to {
//             type Error = TryFromIntError;
//             fn try_from(value: $from) -> Result<Self, Self::Error> {
//                 value.0.try_into()
//             }
//         }
//     };
// }

// impl_big_to_small_prim!(U24, u8);
// impl_big_to_small_prim!(U24, u16);
// impl_big_to_small_prim!(U40, u8);
// impl_big_to_small_prim!(U40, u16);
// impl_big_to_small_prim!(U40, u32);
// impl_big_to_small_prim!(U48, u8);
// impl_big_to_small_prim!(U48, u16);
// impl_big_to_small_prim!(U48, u32);
// impl_big_to_small_prim!(U56, u8);
// impl_big_to_small_prim!(U56, u16);
// impl_big_to_small_prim!(U56, u32);

// /// Make fallible conversion from bigger type to smaller unaligned type
// macro_rules! impl_big_to_small_unalign {
//     ($from:ident, $inner:ident, $to:ident) => {
//         impl TryFrom<$from> for $to {
//             type Error = TryFromUnalignedIntError;
//             fn try_from(value: $from) -> Result<Self, Self::Error> {
//                 let inner = $inner::try_from(value.0).map_err(|_| TryFromUnalignedIntError)?;
//                 inner.try_into()
//             }
//         }
//     };
// }

// impl_big_to_small_unalign!(U40, u32, U24);
// impl_big_to_small_unalign!(U48, u32, U24);
// impl_big_to_small_unalign!(U56, u32, U24);
// impl_big_to_small_unalign!(U48, u64, U40);
// impl_big_to_small_unalign!(U56, u64, U40);
// impl_big_to_small_unalign!(U56, u64, U48);

// // these are guaranteed to always work given the integer limits of f32/f64

// impl From<U24> for f32 {
//     fn from(value: U24) -> Self {
//         value.0.as_()
//     }
// }

// impl From<U24> for f64 {
//     fn from(value: U24) -> Self {
//         value.0.as_()
//     }
// }

// impl From<U40> for f64 {
//     fn from(value: U40) -> Self {
//         value.0.as_()
//     }
// }

// impl From<U48> for f64 {
//     fn from(value: U48) -> Self {
//         value.0.as_()
//     }
// }

// macro_rules! impl_unalign_as_float {
//     ($from:ident, $to:ident) => {
//         impl AsPrimitive<$to> for $from {
//             fn as_(self) -> $to {
//                 self.0.as_()
//             }
//         }
//     };
// }

// impl_unalign_as_float!(U24, f32);
// impl_unalign_as_float!(U40, f32);
// impl_unalign_as_float!(U48, f32);
// impl_unalign_as_float!(U56, f32);
// impl_unalign_as_float!(U24, f64);
// impl_unalign_as_float!(U40, f64);
// impl_unalign_as_float!(U48, f64);
// impl_unalign_as_float!(U56, f64);

// macro_rules! impl_float_as_unalign {
//     ($from:ident, $inner:ident, $to:ident) => {
//         impl AsPrimitive<$to> for $from {
//             fn as_(self) -> $to {
//                 let prim: $inner = self.as_();
//                 $to($inner::from($to::max_value()).min(prim))
//             }
//         }
//     };
// }

// impl_float_as_unalign!(f32, u32, U24);
// impl_float_as_unalign!(f32, u64, U40);
// impl_float_as_unalign!(f32, u64, U48);
// impl_float_as_unalign!(f32, u64, U56);
// impl_float_as_unalign!(f64, u32, U24);
// impl_float_as_unalign!(f64, u64, U40);
// impl_float_as_unalign!(f64, u64, U48);
// impl_float_as_unalign!(f64, u64, U56);

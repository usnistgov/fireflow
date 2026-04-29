use crate::text::byteord::{Bytes, PrivBytes};

use bigdecimal::BigDecimal;
use bytemuck::NoUninit;
use derive_more::{Display, From, Into, Shr};
use num_traits::{AsPrimitive, Bounded, FromBytes, ToBytes};
use std::ptr::copy_nonoverlapping;
use thiserror::Error;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{DisplayAsPyErr, TryFromPyObject},
    pyo3::prelude::*,
};

#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Into, From, NoUninit, Shr, Default, Debug, Display,
)]
#[into(u32, u64)]
#[from(u8, u16)]
#[repr(transparent)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(TryFromPyObject, IntoPyObject))]
pub struct U24(u32);

#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Into, From, NoUninit, Shr, Default, Debug, Display,
)]
#[into(u64)]
#[from(u8, u16, u32)]
#[repr(transparent)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(TryFromPyObject, IntoPyObject))]
pub struct U40(u64);

#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Into, From, NoUninit, Shr, Default, Debug, Display,
)]
#[into(u64)]
#[from(u8, u16, u32)]
#[repr(transparent)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(TryFromPyObject, IntoPyObject))]
pub struct U48(u64);

#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Into, From, NoUninit, Shr, Default, Debug, Display,
)]
#[into(u64)]
#[from(u8, u16, u32)]
#[repr(transparent)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(TryFromPyObject, IntoPyObject))]
pub struct U56(u64);

#[derive(Error, Debug)]
#[error("value out of range for unaligned, unsigned integer")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(PyOverflowError))]
pub struct TryFromUnalignedIntError;

/// Index in a slice from which bytes are to be copied.
pub struct SrcIndex(pub(crate) usize);

/// Index in a slice into which bytes are to be copied.
pub struct DstIndex(pub(crate) usize);

/// A type that can be converted from an FCS value to a memory value.
///
/// This is used for the various data types present in an FCS file.
// TODO this will be way less awkward once we get const traits (or simply the
// ability to use a const value as an array size, since for now I need to
// use fully-generic types for all arrays
pub trait FCSRepr {
    /// Length of type in bytes when in a file (as enum 1-8)
    const FILE_BYTES: Bytes;

    // TODO these are only going to be powers of 2
    /// Length of type in bytes when in memory (as enum 1-8)
    const MEM_BYTES: Bytes;

    /// Byte buffer for type within FCS file.
    type FileBuf;

    /// Byte buffer for type in memory (may not be same size as that in file).
    type MemBuf;

    /// The order the bytes appear in the file if not little/big endian.
    type ByteOrd;

    /// The primitive underlying this type (which may be the same).
    ///
    /// This type must be aligned (ie power of 2 size in bytes).
    type Prim;

    #[must_use]
    fn file_len() -> usize {
        usize::from(u8::from(Self::FILE_BYTES))
    }

    #[must_use]
    fn mem_len() -> usize {
        usize::from(u8::from(Self::MEM_BYTES))
    }

    #[must_use]
    fn from_be_slice(src: &[u8], index: SrcIndex) -> Self
    where
        Self: FromBytes<Bytes = Self::FileBuf>,
        Self::FileBuf: AsRef<[u8]> + AsMut<[u8]> + Default,
    {
        let n = Self::file_len();
        let mut buf = Self::FileBuf::default();
        let tmp = &src[index.0..index.0 + n];
        buf.as_mut().copy_from_slice(tmp);
        Self::from_be_bytes(&buf)
    }

    #[must_use]
    fn from_le_slice(src: &[u8], index: SrcIndex) -> Self
    where
        Self: FromBytes<Bytes = Self::FileBuf>,
        Self::FileBuf: AsRef<[u8]> + AsMut<[u8]> + Default,
    {
        let n = Self::file_len();
        let mut buf = Self::FileBuf::default();
        let tmp = &src[index.0..index.0 + n];
        buf.as_mut().copy_from_slice(tmp);
        Self::from_le_bytes(&buf)
    }

    fn to_be_slice(&self, dst: &mut [u8], index: DstIndex)
    where
        Self: ToBytes<Bytes = Self::FileBuf>,
        Self::FileBuf: AsRef<[u8]> + AsMut<[u8]> + Default,
    {
        let tmp = self.to_be_bytes();
        let n = Self::file_len();
        dst[index.0..index.0 + n].copy_from_slice(tmp.as_ref());
    }

    fn to_le_slice(&self, dst: &mut [u8], index: DstIndex)
    where
        Self: ToBytes<Bytes = Self::FileBuf>,
        Self::FileBuf: AsRef<[u8]> + AsMut<[u8]> + Default,
    {
        let tmp = self.to_le_bytes();
        let n = Self::file_len();
        dst[index.0..index.0 + n].copy_from_slice(tmp.as_ref());
    }

    fn from_ordered_bytes(bytes: &Self::FileBuf, order: &Self::ByteOrd) -> Self
    where
        Self: FromBytes<Bytes = Self::FileBuf>,
        Self::FileBuf: AsRef<[u8]> + AsMut<[u8]> + Default,
        Self::ByteOrd: AsRef<[u8]>,
    {
        let mut buf = Self::FileBuf::default();
        for (i, j) in order.as_ref().iter().enumerate() {
            buf.as_mut()[usize::from(*j)] = bytes.as_ref()[i];
        }
        Self::from_le_bytes(&buf)
    }

    fn to_ordered_bytes(&self, order: &Self::ByteOrd) -> Self::FileBuf
    where
        Self: ToBytes<Bytes = Self::FileBuf>,
        Self::FileBuf: AsRef<[u8]> + AsMut<[u8]> + Default,
        Self::ByteOrd: AsRef<[u8]>,
    {
        let bytes = self.to_le_bytes();
        let mut buf = Self::FileBuf::default();
        for (i, j) in order.as_ref().iter().enumerate() {
            buf.as_mut()[i] = bytes.as_ref()[usize::from(*j)];
        }
        buf
    }

    /// Read an FCS value from a byte stream.
    ///
    /// # SAFETY
    ///
    /// Caller must ensure that the bytes to be read, starting at the index and
    /// up to the last byte given by index + length of bytes to be read, are
    /// within the slice. This will not check bounds. This is meant to be used
    /// in very fast loops where performance is critical and adding a bounds
    /// check would insert a jump op which would in tern prevent nice compiler
    /// optimizations (unrolling, possibly vectorization, etc).
    unsafe fn array_from_slice(src: &[u8], i: &SrcIndex) -> Self::FileBuf;

    /// Write an FCS value to a byte stream.
    ///
    /// # SAFETY
    ///
    /// Caller must ensure that the bytes to be written, starting at the index
    /// and up to the last byte given by index + length of bytes to be written,
    /// are within the slice. This will not check bounds. This is meant to be
    /// used in very fast loops where performance is critical and adding a
    /// bounds check would insert a jump op which would in tern prevent nice
    /// compiler optimizations (unrolling, possibly vectorization, etc).
    unsafe fn array_to_slice(src: &Self::FileBuf, dst: &mut [u8], i: &DstIndex);
}

macro_rules! impl_file_bytes {
    ($t:ident, $prim:ident, $file_bytes:ident, $mem_bytes:ident, $file_len:expr, $mem_len:expr) => {
        impl FCSRepr for $t {
            const FILE_BYTES: Bytes = Bytes(PrivBytes::$file_bytes);
            const MEM_BYTES: Bytes = Bytes(PrivBytes::$mem_bytes);
            type FileBuf = [u8; $file_len];
            type MemBuf = [u8; $mem_len];
            type ByteOrd = Self::FileBuf;
            type Prim = $prim;

            unsafe fn array_from_slice(src: &[u8], i: &SrcIndex) -> Self::FileBuf {
                // SAFETY: caller must ensure this is not out of bounds
                unsafe { array_from_slice(src, i) }
            }

            unsafe fn array_to_slice(src: &Self::FileBuf, dst: &mut [u8], i: &DstIndex) {
                // SAFETY: caller must ensure this is not out of bounds
                unsafe { array_to_slice(src, dst, i) }
            }
        }
    };
}

unsafe fn array_from_slice<const LEN: usize>(src: &[u8], i: &SrcIndex) -> [u8; LEN] {
    // SAFETY: caller should ensure this is not out of bounds
    let xs = unsafe { src.get_unchecked(i.0..i.0 + LEN) };
    // SAFETY: length of slice should match returned array
    unsafe { *(xs.as_ptr().cast()) }
}

unsafe fn array_to_slice<const LEN: usize>(src: &[u8; LEN], dst: &mut [u8], i: &DstIndex) {
    // SAFETY: caller should ensure this is not out of bounds
    let p = unsafe { dst.as_mut_ptr().add(i.0) };
    // SAFETY: length of slice should match returned array
    unsafe { copy_nonoverlapping(src.as_ptr(), p, LEN) }
}

impl_file_bytes!(u8, u8, B1, B1, 1, 1);
impl_file_bytes!(u16, u16, B2, B2, 2, 2);
impl_file_bytes!(U24, u32, B3, B4, 3, 4);
impl_file_bytes!(u32, u32, B4, B4, 4, 4);
impl_file_bytes!(U40, u64, B5, B8, 5, 8);
impl_file_bytes!(U48, u64, B6, B8, 6, 8);
impl_file_bytes!(U56, u64, B7, B8, 7, 8);
impl_file_bytes!(u64, u64, B8, B8, 8, 8);
impl_file_bytes!(f32, f32, B4, B4, 4, 4);
impl_file_bytes!(f64, f64, B8, B8, 8, 8);

macro_rules! impl_unaligned {
    ($inner:ident, $outer:ident, $n:expr) => {
        impl TryFrom<$inner> for $outer {
            type Error = TryFromUnalignedIntError;

            fn try_from(value: $inner) -> Result<Self, Self::Error> {
                if value > $inner::from($outer::max_value()) {
                    Err(TryFromUnalignedIntError)
                } else {
                    Ok(Self(value))
                }
            }
        }

        impl Bounded for $outer {
            fn min_value() -> Self {
                Self(0)
            }

            fn max_value() -> Self {
                Self((1 << $n) - 1)
            }
        }

        impl FromBytes for $outer {
            type Bytes = <$outer as FCSRepr>::FileBuf;

            fn from_be_bytes(bytes: &Self::Bytes) -> Self {
                Self(from_unaligned_be_bytes(bytes))
            }

            fn from_le_bytes(bytes: &Self::Bytes) -> Self {
                Self(from_unaligned_le_bytes(bytes))
            }
        }

        impl ToBytes for $outer {
            type Bytes = <$outer as FCSRepr>::FileBuf;

            fn to_be_bytes(&self) -> Self::Bytes {
                to_unaligned_be_bytes(&self.0)
            }

            fn to_le_bytes(&self) -> Self::Bytes {
                to_unaligned_le_bytes(&self.0)
            }
        }

        impl From<$outer> for BigDecimal {
            fn from(value: $outer) -> Self {
                Self::from(value.0)
            }
        }
    };
}

impl_unaligned!(u32, U24, 24);
impl_unaligned!(u64, U40, 40);
impl_unaligned!(u64, U48, 48);
impl_unaligned!(u64, U56, 56);

fn from_unaligned_be_bytes<
    T: FromBytes<Bytes = [u8; OUTER_LEN]>,
    const INNER_LEN: usize,
    const OUTER_LEN: usize,
>(
    bytes: &[u8; INNER_LEN],
) -> T {
    let mut buf = [0; OUTER_LEN];
    let b = OUTER_LEN - INNER_LEN;
    buf[b..].copy_from_slice(bytes);
    T::from_be_bytes(&buf)
}

fn from_unaligned_le_bytes<
    T: FromBytes<Bytes = [u8; OUTER_LEN]>,
    const INNER_LEN: usize,
    const OUTER_LEN: usize,
>(
    bytes: &[u8; INNER_LEN],
) -> T {
    let mut buf = [0; OUTER_LEN];
    buf[..INNER_LEN].copy_from_slice(bytes);
    T::from_le_bytes(&buf)
}

fn to_unaligned_be_bytes<
    T: ToBytes<Bytes = [u8; INNER_LEN]>,
    const INNER_LEN: usize,
    const OUTER_LEN: usize,
>(
    x: &T,
) -> [u8; OUTER_LEN] {
    let mut buf = [0; OUTER_LEN];
    let b = OUTER_LEN - INNER_LEN;
    buf.copy_from_slice(&x.to_be_bytes()[b..]);
    buf
}

fn to_unaligned_le_bytes<
    T: ToBytes<Bytes = [u8; INNER_LEN]>,
    const INNER_LEN: usize,
    const OUTER_LEN: usize,
>(
    x: &T,
) -> [u8; OUTER_LEN] {
    let mut buf = [0; OUTER_LEN];
    buf.copy_from_slice(&x.to_le_bytes()[..INNER_LEN]);
    buf
}

/// Make conversion from smaller number to bigger type (which will never fail).
macro_rules! impl_small_to_big {
    ($from:ident, $to:ident) => {
        impl From<$from> for $to {
            fn from(value: $from) -> Self {
                Self(value.0.into())
            }
        }
    };
}

impl_small_to_big!(U24, U40);
impl_small_to_big!(U24, U48);
impl_small_to_big!(U24, U56);
impl_small_to_big!(U40, U48);
impl_small_to_big!(U40, U56);
impl_small_to_big!(U48, U56);

// special case since this is a primitive type that can be converted to a
// smaller type which has a corresponding unaligned type
impl TryFrom<u64> for U24 {
    type Error = TryFromUnalignedIntError;
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        u32::try_from(value)
            .map_err(|_| TryFromUnalignedIntError)?
            .try_into()
    }
}

/// Make fallible conversion from bigger type to smaller primitive type
macro_rules! impl_big_to_small_prim {
    ($from:ident, $to:ident) => {
        impl TryFrom<$from> for $to {
            type Error = TryFromUnalignedIntError;
            fn try_from(value: $from) -> Result<Self, Self::Error> {
                value.0.try_into().map_err(|_| TryFromUnalignedIntError)
            }
        }
    };
}

impl_big_to_small_prim!(U24, u8);
impl_big_to_small_prim!(U24, u16);
impl_big_to_small_prim!(U40, u8);
impl_big_to_small_prim!(U40, u16);
impl_big_to_small_prim!(U40, u32);
impl_big_to_small_prim!(U48, u8);
impl_big_to_small_prim!(U48, u16);
impl_big_to_small_prim!(U48, u32);
impl_big_to_small_prim!(U56, u8);
impl_big_to_small_prim!(U56, u16);
impl_big_to_small_prim!(U56, u32);

/// Make fallible conversion from bigger type to smaller unaligned type
macro_rules! impl_big_to_small_unalign {
    ($from:ident, $inner:ident, $to:ident) => {
        impl TryFrom<$from> for $to {
            type Error = TryFromUnalignedIntError;
            fn try_from(value: $from) -> Result<Self, Self::Error> {
                let inner = $inner::try_from(value.0).map_err(|_| TryFromUnalignedIntError)?;
                inner.try_into()
            }
        }
    };
}

impl_big_to_small_unalign!(U40, u32, U24);
impl_big_to_small_unalign!(U48, u32, U24);
impl_big_to_small_unalign!(U56, u32, U24);
impl_big_to_small_unalign!(U48, u64, U40);
impl_big_to_small_unalign!(U56, u64, U40);
impl_big_to_small_unalign!(U56, u64, U48);

/// Convert integer to float losslessly.
///
/// This only works if the int is totally with the range of the float which
/// perfectly expresses integers without any gaps.
macro_rules! impl_unaligned_to_float_lossless {
    ($i:ident, $f:ident) => {
        impl From<$i> for $f {
            fn from(value: $i) -> Self {
                value.0.as_()
            }
        }
    };
}

impl_unaligned_to_float_lossless!(U24, f32);
impl_unaligned_to_float_lossless!(U24, f64);
impl_unaligned_to_float_lossless!(U40, f64);
impl_unaligned_to_float_lossless!(U48, f64);

/// Convert int to float with possible loss.
macro_rules! impl_unalign_as_float {
    ($from:ident, $to:ident) => {
        impl AsPrimitive<$to> for $from {
            fn as_(self) -> $to {
                self.0.as_()
            }
        }
    };
}

impl_unalign_as_float!(U24, f32);
impl_unalign_as_float!(U40, f32);
impl_unalign_as_float!(U48, f32);
impl_unalign_as_float!(U56, f32);
impl_unalign_as_float!(U24, f64);
impl_unalign_as_float!(U40, f64);
impl_unalign_as_float!(U48, f64);
impl_unalign_as_float!(U56, f64);

/// Convert float to integer.
macro_rules! impl_float_as_unalign {
    ($from:ident, $inner:ident, $to:ident) => {
        impl AsPrimitive<$to> for $from {
            fn as_(self) -> $to {
                let prim: $inner = self.as_();
                $to($inner::from($to::max_value()).min(prim))
            }
        }
    };
}

impl_float_as_unalign!(f32, u32, U24);
impl_float_as_unalign!(f32, u64, U40);
impl_float_as_unalign!(f32, u64, U48);
impl_float_as_unalign!(f32, u64, U56);
impl_float_as_unalign!(f64, u32, U24);
impl_float_as_unalign!(f64, u64, U40);
impl_float_as_unalign!(f64, u64, U48);
impl_float_as_unalign!(f64, u64, U56);

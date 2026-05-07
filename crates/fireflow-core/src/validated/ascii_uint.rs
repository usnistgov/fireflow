//! Types used for constructing offsets in HEADER and TEXT

use crate::header::MAX_HEADER_OFFSET;

use derive_more::{Add, Display, From, FromStr, Into, Mul, Sub};
use fireflow_types::nonempty_string::{PaddedU64, ToDisplayNE};
use num_derive::{One, Zero};
use num_traits::ops::checked::CheckedSub;
use std::num::{NonZeroU64, ParseIntError, TryFromIntError};
use std::str;
use thiserror::Error;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{DisplayAsPyErr, FromInnerPyObject},
    pyo3::prelude::*,
};

/// An unsigned int which may only be 20 digits.
///
/// This will always be formatted as a right-aligned 0-padded integer 20 chars
/// wide. No validation will be performed as a u64 can only store 20 digits.
///
/// This is used for the offsets in TEXT which must be formatted in a fixed
/// width.
#[derive(
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    FromStr,
    Into,
    From,
    Add,
    Sub,
    Mul,
    Zero,
    One,
    Debug,
)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
#[into(u64, i128)]
#[mul(forward)]
#[from(u64, NonZeroU64)]
pub struct UintZeroPad20(pub u64);

impl ToDisplayNE<'_> for UintZeroPad20 {
    type NE = PaddedU64;
    fn to_ne(&'_ self) -> Self::NE {
        PaddedU64::new(20, '0', self.0)
    }
}

impl TryFrom<i128> for UintZeroPad20 {
    type Error = TryFromIntError;
    fn try_from(value: i128) -> Result<Self, Self::Error> {
        value.try_into().map(Self)
    }
}

impl CheckedSub for UintZeroPad20 {
    fn checked_sub(&self, v: &Self) -> Option<Self> {
        self.0.checked_sub(v.0).map(Self)
    }
}

/// An unsigned int which may only be 20 digits.
///
/// This will always be formatted as a right-aligned space-padded integer 20
/// chars wide. No validation will be performed as a u64 can only store 20
/// digits.
///
/// This is used for the OTHER offsets in HEADER which can be up to 20 chars
/// wide.
#[derive(
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    FromStr,
    Into,
    From,
    Add,
    Sub,
    Mul,
    Zero,
    One,
    Debug,
    Display,
)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromInnerPyObject))]
#[into(u64, i128)]
#[mul(forward)]
#[from(NonZeroU64, UintSpacePad8)]
#[display("{_0:>20}")]
pub struct UintSpacePad20(pub u64);

impl TryFrom<i128> for UintSpacePad20 {
    type Error = TryFromIntError;
    fn try_from(value: i128) -> Result<Self, Self::Error> {
        value.try_into().map(Self)
    }
}

impl CheckedSub for UintSpacePad20 {
    fn checked_sub(&self, v: &Self) -> Option<Self> {
        self.0.checked_sub(v.0).map(Self)
    }
}

impl UintSpacePad20 {
    /// Parse from a buffer that contains up to 20 bytes.
    ///
    /// Will panic if parsed digit is more than 20 digits long.
    pub(crate) fn from_bytes(bs: &[u8]) -> Result<i128, ParseFixedUintError> {
        debug_assert!(bs.len() <= 20, "cannot parse more than 20 bytes");
        Ok(ascii_str_from_bytes(bs)?.trim_start().parse::<i128>()?)
    }
}

// for symmetry with UintSpacePad8
impl TryFrom<u64> for UintSpacePad20 {
    type Error = Uint8DigitOverflowError;
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Ok(Self(value))
    }
}

impl HeaderString for UintSpacePad20 {
    const WIDTH: u8 = 20;
}

/// An unsigned int which must be <= 99,999,999.
///
/// Aside from this, it will behave just like a normal u32.
///
/// This is used as-is for HEADER offsets, and used in a wrapper for $NEXTDATA,
/// both of which have this constraint.
#[derive(
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Into,
    From,
    Add,
    Mul,
    Sub,
    Zero,
    One,
    Debug,
    Display,
)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromInnerPyObject))]
#[into(u32, u64, i128)]
#[from(u8, u16)] // ASSUME these will never fail
#[mul(forward)]
#[display("{_0:>8}")]
pub struct UintSpacePad8(u32);

impl CheckedSub for UintSpacePad8 {
    fn checked_sub(&self, v: &Self) -> Option<Self> {
        self.0.checked_sub(v.0).map(Self)
    }
}

impl UintSpacePad8 {
    /// Parse from a buffer that contains 8 bytes.
    pub(crate) fn from_bytes(bs: [u8; 8], allow_blank: bool) -> Result<i128, ParseFixedUintError> {
        if bs.iter().all(|&x| x == 32) && allow_blank {
            return Ok(0);
        }
        Ok(ascii_str_from_bytes(&bs[..])?.trim_start().parse()?)
    }
}

impl HeaderString for UintSpacePad8 {
    const WIDTH: u8 = 8;
}

pub(crate) trait HeaderString {
    const WIDTH: u8;
}

impl TryFrom<i128> for UintSpacePad8 {
    type Error = TryFromIntError;
    fn try_from(value: i128) -> Result<Self, Self::Error> {
        value.try_into().map(Self)
    }
}

impl TryFrom<u64> for UintSpacePad8 {
    type Error = Uint8DigitOverflowError;
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        value
            .try_into()
            .map_or(Err(Uint8DigitOverflowError(value)), |x: u32| {
                if x > MAX_HEADER_OFFSET {
                    Err(Uint8DigitOverflowError(x.into()))
                } else {
                    Ok(Self(x))
                }
            })
    }
}

/// Error when parsing fixed unsigned integer from ASCII
///
/// Used internally to create other errors
#[derive(Display, From, Debug)]
pub(crate) enum ParseFixedUintError {
    Int(ParseIntError),
    NotAscii(BytesNotAsciiError),
}

/// Error when unsigned integer exceeds 8 digits
#[derive(Debug, Error)]
#[error("must be {max} or less, got {0}", max = MAX_HEADER_OFFSET)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr), pyerr(PyOverflowError))]
pub struct Uint8DigitOverflowError(u64);

/// Error when parsing integer from ASCII with invalid ASCII characters
#[derive(Debug, Error)]
#[error("could not convert to ASCII string: {0:?}")]
pub struct BytesNotAsciiError(Vec<u8>);

/// Error when offsets in HEADER are negative (this happens for some reason)
#[derive(Debug, Error)]
#[error("HEADER offset is negative: {0}")]
pub struct NegativeOffsetError(pub i128);

fn ascii_str_from_bytes(xs: &[u8]) -> Result<&str, BytesNotAsciiError> {
    str::from_utf8(xs).map_err(|_| BytesNotAsciiError(xs.to_vec()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn u32_to_uint8digit() {
        assert!(UintSpacePad8::try_from(0_u64).is_ok());
        assert!(UintSpacePad8::try_from(1_u64).is_ok());
        assert!(UintSpacePad8::try_from(99_999_999_u64).is_ok());
        assert!(UintSpacePad8::try_from(100_000_000_u64).is_err());
    }
}

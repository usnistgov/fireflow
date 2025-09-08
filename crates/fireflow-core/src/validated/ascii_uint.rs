//! Types used for constructing offsets in HEADER and TEXT

use crate::header::MAX_HEADER_OFFSET;

use derive_more::{Add, Display, From, FromStr, Into, Mul, Sub};
use num_derive::{One, Zero};
use num_traits::identities::Zero;
use num_traits::ops::checked::CheckedSub;
use std::fmt;
use std::num::{NonZeroU64, ParseIntError, TryFromIntError};
use std::str;
use std::str::FromStr;

#[cfg(feature = "serde")]
use serde::Serialize;

/// An unsigned int which may only be 20 chars wide.
///
/// This will always be formatted as a right-aligned 0-padded integer 20 chars
/// wide. No validation will be performed as a u64 can only store 20 digits.
///
/// This is used for the offsets in TEXT which must be formatted in a fixed
/// width.
#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, FromStr, Into, From, Add, Sub, Mul, Zero, One,
)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[into(u64, i128)]
#[mul(forward)]
#[from(u64, NonZeroU64)]
pub struct UintZeroPad20(pub u64);

impl fmt::Display for UintZeroPad20 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{:0>20}", self.0)
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

/// An unsigned int which may only be 20 chars wide.
///
/// This will always be formatted as a right-aligned space-padded integer 20
/// chars wide. No validation will be performed as a u64 can only store 20
/// digits.
///
/// This is used for the OTHER offsets in HEADER which can be up to 20 chars
/// wide.
#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, FromStr, Into, From, Add, Sub, Mul, Zero, One,
)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[into(u64, i128)]
#[mul(forward)]
#[from(u64, NonZeroU64)]
pub struct UintSpacePad20(pub u64);

impl fmt::Display for UintSpacePad20 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{:>20}", self.0)
    }
}

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

/// An unsigned int which must be <= 99,999,999.
///
/// Aside from this, it will behave just like a normal u32.
///
/// This is used as-is for HEADER offsets, and used in a wrapper for $NEXTDATA,
/// both of which have this constraint.
#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Display, Into, From, Add, Mul, Sub, Zero, One,
)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[into(u32, u64, i128)]
#[from(u8, u16)] // ASSUME these will never fail
#[mul(forward)]
pub struct UintSpacePad8(u32);

impl CheckedSub for UintSpacePad8 {
    fn checked_sub(&self, v: &Self) -> Option<Self> {
        self.0.checked_sub(v.0).map(Self)
    }
}

impl UintSpacePad8 {
    /// Parse from a buffer that contains 8 bytes.
    pub(crate) fn from_bytes(
        bs: &[u8; 8],
        allow_blank: bool,
        allow_negative: bool,
    ) -> Result<Self, ParseFixedUintError> {
        let s = ascii_str_from_bytes(bs).map_err(ParseFixedUintError::NotAscii)?;
        let trimmed = s.trim_start();
        if allow_blank && trimmed.is_empty() {
            return Ok(UintSpacePad8::zero());
        }
        let x = trimmed.parse::<i32>().map_err(ParseFixedUintError::Int)?;
        if x < 0 {
            if allow_negative {
                Ok(Self::zero())
            } else {
                Err(ParseFixedUintError::Negative(NegativeOffsetError(x)))
            }
        } else {
            // ASSUME this will never wrap since the max digits we can read are
            // 8, which is only ~1e9 which is much less than 4e10 which is the
            // max of a u32.
            Ok(Self(x as u32))
        }
    }
}

impl TryFrom<i128> for UintSpacePad8 {
    type Error = TryFromIntError;
    fn try_from(value: i128) -> Result<Self, Self::Error> {
        value.try_into().map(Self)
    }
}

#[derive(Display, From)]
pub enum ParseFixedUintError {
    Int(ParseIntError),
    NotAscii(BytesNotAscii),
    Negative(NegativeOffsetError),
}

impl TryFrom<u64> for UintSpacePad8 {
    type Error = Uint8DigitOverflow;
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        value
            .try_into()
            .map_or(Err(Uint8DigitOverflow(value)), |x: u32| {
                if x > MAX_HEADER_OFFSET {
                    Err(Uint8DigitOverflow(x.into()))
                } else {
                    Ok(Self(x))
                }
            })
    }
}

impl FromStr for UintSpacePad8 {
    type Err = ParseUint8DigitError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<u64>()
            .map_err(ParseUint8DigitError::Int)
            .and_then(|x| x.try_into().map_err(ParseUint8DigitError::Overflow))
    }
}

#[derive(Display, From)]
pub enum ParseUint8DigitError {
    Overflow(Uint8DigitOverflow),
    Int(ParseIntError),
}

pub struct Uint8DigitOverflow(u64);

impl fmt::Display for Uint8DigitOverflow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "must be {} or less, got {}", MAX_HEADER_OFFSET, self.0)
    }
}

pub(crate) fn ascii_str_from_bytes(xs: &[u8]) -> Result<&str, BytesNotAscii> {
    if xs.is_ascii() {
        Ok(unsafe { str::from_utf8_unchecked(xs) })
    } else {
        Err(BytesNotAscii(xs.to_vec()))
    }
}

pub struct BytesNotAscii(Vec<u8>);

impl fmt::Display for BytesNotAscii {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "could not convert to ASCII string: {:?}", self.0)
    }
}

pub struct NegativeOffsetError(pub i32);

impl fmt::Display for NegativeOffsetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "HEADER offset is negative: {}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_u32_to_uint8digit() {
        assert!(UintSpacePad8::try_from(0_u64).is_ok());
        assert!(UintSpacePad8::try_from(1_u64).is_ok());
        assert!(UintSpacePad8::try_from(99_999_999_u64).is_ok());
        assert!(UintSpacePad8::try_from(100_000_000_u64).is_err());
    }

    #[test]
    fn test_str_to_uint8digit() {
        assert!("0".parse::<UintSpacePad8>().is_ok());
        assert!("99999999".parse::<UintSpacePad8>().is_ok());
        assert!("100000000".parse::<UintSpacePad8>().is_err());
    }
}

#[cfg(feature = "python")]
mod python {
    use super::{Uint8DigitOverflow, UintSpacePad20, UintSpacePad8, UintZeroPad20};
    use crate::python::macros::{impl_from_py_transparent, impl_try_from_py, impl_value_err};

    impl_from_py_transparent!(UintZeroPad20);
    impl_from_py_transparent!(UintSpacePad20);
    impl_try_from_py!(UintSpacePad8, u64);
    impl_value_err!(Uint8DigitOverflow);
}

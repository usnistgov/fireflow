//! Types to represent the $PnB and $PnR values for a uint column.

use crate::error::BiTentative;
use crate::text::keywords::{IntRangeError, Range};

use derive_more::{Display, From};
use num_traits::PrimInt;
use std::fmt;

#[cfg(feature = "serde")]
use serde::Serialize;

/// A number representing a value with bitmask up to LEN bits
#[derive(PartialEq, Clone, Copy, PartialOrd)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct Bitmask<T, const LEN: usize> {
    /// The value to be masked.
    ///
    /// This can be any integer up to LEN bits.
    value: T,

    /// The bitmask corresponding to ['range'].
    ///
    /// Will always be a power of 2 minus 1 (ie, some number of contiguous bits
    /// in binary). This will be able to hold ['range'] but will mask out any
    /// bits beyond those needed to express ['range'].
    bitmask: T,
}

pub type Bitmask08 = Bitmask<u8, 1>;
pub type Bitmask16 = Bitmask<u16, 2>;
pub type Bitmask24 = Bitmask<u32, 3>;
pub type Bitmask32 = Bitmask<u32, 4>;
pub type Bitmask40 = Bitmask<u64, 5>;
pub type Bitmask48 = Bitmask<u64, 6>;
pub type Bitmask56 = Bitmask<u64, 7>;
pub type Bitmask64 = Bitmask<u64, 8>;

impl<T, const LEN: usize> From<&Bitmask<T, LEN>> for Range
where
    T: Copy,
    u64: From<T>,
{
    fn from(value: &Bitmask<T, LEN>) -> Self {
        // NOTE add 1 since the spec treats int ranges as one less than they
        // appear in TEXT
        Range::from(u64::from(value.value))
    }
}

impl<T, const LEN: usize> Bitmask<T, LEN> {
    pub(crate) fn bitmask(&self) -> T
    where
        T: Copy,
    {
        self.bitmask
    }

    pub(crate) fn apply(&self, value: T) -> T
    where
        T: Ord + Copy,
    {
        self.bitmask.min(value)
    }

    pub(crate) fn from_native_tnt(
        value: T,
        notrunc: bool,
    ) -> BiTentative<Self, BitmaskTruncationError>
    where
        T: PrimInt,
        u64: From<T>,
    {
        let (bitmask, truncated) = Bitmask::from_native(value);
        let error = if truncated {
            Some(BitmaskTruncationError {
                bytes: Self::bits(),
                value: u64::from(value),
            })
        } else {
            None
        };
        BiTentative::new_either1(bitmask, error, notrunc)
    }

    // fn from_u64_tnt(value: u64, notrunc: bool) -> BiTentative<Self, BitmaskTruncationError>
    // where
    //     T: PrimInt + TryFrom<u64>,
    // {
    //     let (bitmask, truncated) = Bitmask::from_u64(value);
    //     let error = if truncated {
    //         Some(BitmaskTruncationError {
    //             bytes: Self::bits(),
    //             value,
    //         })
    //     } else {
    //         None
    //     };
    //     BiTentative::new_either1(bitmask, error, notrunc)
    // }

    pub fn from_native(value: T) -> (Self, bool)
    where
        T: PrimInt,
    {
        // ASSUME number of bits will never exceed 64 (or 255 for that matter)
        // and thus will fit in a u8
        let native_bits = (std::mem::size_of::<T>() * 8) as u8;
        let value_bits = native_bits - (value.leading_zeros() as u8);
        let truncated = value_bits > Self::bits();
        let bits = value_bits.min(Self::bits());
        let mask = if bits == 0 {
            T::zero()
        } else if bits == native_bits {
            T::max_value()
        } else {
            (T::one() << usize::from(bits)) - T::one()
        };
        (
            Self {
                bitmask: mask,
                value: value.min(mask),
            },
            truncated,
        )
    }

    pub(crate) fn from_u64(value: u64) -> (Self, bool)
    where
        T: PrimInt + TryFrom<u64>,
    {
        T::try_from(value)
            .map(Self::from_native)
            .unwrap_or((Self::max(), true))
    }

    fn max() -> Self
    where
        T: PrimInt,
    {
        Bitmask::from_native(T::max_value()).0
    }

    fn bytes() -> u8 {
        LEN as u8
    }

    fn bits() -> u8 {
        Self::bytes() * 8
    }
}

pub struct BitmaskTruncationError {
    bytes: u8,
    value: u64,
}

impl fmt::Display for BitmaskTruncationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "could not make bitmask for {} which exceeds {} bytes",
            self.value, self.bytes
        )
    }
}

#[derive(Display, From)]
pub enum BitmaskError {
    ToInt(IntRangeError<()>),
    Trunc(BitmaskTruncationError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_int_to_bitmask() {
        let x = 255;
        let (b, trunc) = Bitmask::<u16, 1>::from_native(x);
        assert_eq!((b.value, b.bitmask(), trunc), (255, 255, false));
    }

    #[test]
    fn test_int_to_bitmask_roundup() {
        let x = 254;
        let (b, trunc) = Bitmask::<u16, 1>::from_native(x);
        assert_eq!((b.value, b.bitmask(), trunc), (254, 255, false));
    }

    #[test]
    fn test_int_to_bitmask_trunc() {
        let x = 256;
        let (b, trunc) = Bitmask::<u16, 1>::from_native(x);
        assert_eq!((b.value, b.bitmask(), trunc), (255, 255, true));
    }

    #[test]
    fn test_int_to_bitmask_max_native() {
        let x = 65535;
        let (b, trunc) = Bitmask::<u16, 2>::from_native(x);
        assert_eq!((b.value, b.bitmask(), trunc), (65535, 65535, false));
    }

    #[test]
    fn test_int_to_bitmask_zero() {
        let x = 0;
        let (b, trunc) = Bitmask::<u16, 2>::from_native(x);
        assert_eq!((b.value, b.bitmask(), trunc), (0, 0, false));
    }

    #[test]
    fn test_max_1_byte() {
        let b = Bitmask::<u8, 1>::max();
        assert_eq!((b.value, b.bitmask()), (255, 255));
    }

    #[test]
    fn test_max_2_byte() {
        let b = Bitmask::<u16, 2>::max();
        assert_eq!((b.value, b.bitmask()), (65535, 65535));
    }

    #[test]
    fn test_max_3_byte() {
        let b = Bitmask::<u32, 3>::max();
        assert_eq!((b.value, b.bitmask()), (16777215, 16777215));
    }
}

#[cfg(feature = "python")]
mod python {
    use super::Bitmask;

    use pyo3::conversion::FromPyObjectBound;
    use pyo3::exceptions::PyOverflowError;
    use pyo3::prelude::*;

    impl<'py, T, const LEN: usize> FromPyObject<'py> for super::Bitmask<T, LEN>
    where
        for<'a> T: FromPyObjectBound<'a, 'py>,
        T: num_traits::PrimInt + std::fmt::Display,
    {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let x = ob.extract::<T>()?;
            let (ret, trunc) = Bitmask::from_native(x);
            if trunc {
                let e = format!("could not make {LEN}-byte bitmask from {x}");
                Err(PyOverflowError::new_err(e))
            } else {
                Ok(ret)
            }
        }
    }
}

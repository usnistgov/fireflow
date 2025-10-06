//! Types to represent the $PnB and $PnR values for a uint column.

use crate::error::BiTentative;
use crate::text::keywords::{IntRangeError, Range};

use bigdecimal::BigDecimal;
use derive_more::{Display, From};
use num_traits::identities::One;
use num_traits::PrimInt;
use std::mem::size_of;
use thiserror::Error;

#[cfg(feature = "serde")]
use serde::Serialize;

/// A number representing a value with bitmask up to LEN bits
#[derive(PartialEq, Clone, Copy, PartialOrd, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct Bitmask<T, const LEN: usize> {
    /// The value to be masked.
    ///
    /// This can be any integer up to LEN bits.
    value: T,

    /// The bitmask corresponding to `value`.
    ///
    /// Will always be a power of 2 minus 1 (ie, some number of contiguous bits
    /// in binary). This will be able to hold `value` but will mask out any
    /// bits beyond those needed to express `value`.
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
        Range::from(u64::from(value.value)) + Range::from(BigDecimal::one())
    }
}

impl<T, const LEN: usize> From<Bitmask<T, LEN>> for u64
where
    u64: From<T>,
{
    fn from(value: Bitmask<T, LEN>) -> Self {
        u64::from(value.value)
    }
}

impl<T, const LEN: usize> Bitmask<T, LEN> {
    pub(crate) fn bitmask(&self) -> T
    where
        T: Copy,
    {
        self.bitmask
    }

    pub(crate) fn apply(&self, value: T) -> (Option<BitmaskLossError>, T)
    where
        T: Ord + Copy,
        u64: From<T>,
    {
        let b = self.bitmask;
        let trunc = value > b;
        let e = trunc.then(|| BitmaskLossError(u64::from(b)));
        (e, b.min(value))
    }

    pub(crate) fn from_native_tnt(
        value: T,
        disallow_trunc: bool,
    ) -> BiTentative<Self, BitmaskTruncationError>
    where
        T: PrimInt,
        u64: From<T>,
    {
        let (bitmask, truncated) = Bitmask::from_native(value);
        let error = truncated.then(|| BitmaskTruncationError {
            bytes: Self::bits(),
            value: u64::from(value),
        });
        BiTentative::new_either1(bitmask, error, disallow_trunc)
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
        let native_bits = u8::try_from(size_of::<T>() * 8).unwrap();
        let value_bits = native_bits - u8::try_from(value.leading_zeros()).unwrap();
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
        // ASSUME 'LEN' will never exceed 8
        LEN.try_into().unwrap()
    }

    fn bits() -> u8 {
        Self::bytes() * 8
    }
}

#[derive(Debug, Error)]
#[error("could not make bitmask for {value} which exceeds {bytes} bytes")]
pub struct BitmaskTruncationError {
    bytes: u8,
    value: u64,
}

#[derive(Display, From, Debug, Error)]
pub enum BitmaskError {
    ToInt(IntRangeError<()>),
    Trunc(BitmaskTruncationError),
}

#[derive(Clone, Copy, Debug, Error)]
#[error("integer data was too big and truncated to bitmask {0}")]
pub struct BitmaskLossError(pub u64);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn int_to_bitmask() {
        let x = 0xFF;
        let (b, trunc) = Bitmask::<u16, 1>::from_native(x);
        assert_eq!((b.value, b.bitmask(), trunc), (0xFF, 0xFF, false));
    }

    #[test]
    fn int_to_bitmask_roundup() {
        let x = 0xFE;
        let (b, trunc) = Bitmask::<u16, 1>::from_native(x);
        assert_eq!((b.value, b.bitmask(), trunc), (0xFE, 0xFF, false));
    }

    #[test]
    fn int_to_bitmask_trunc() {
        let x = 0x100;
        let (b, trunc) = Bitmask::<u16, 1>::from_native(x);
        assert_eq!((b.value, b.bitmask(), trunc), (0xFF, 0xFF, true));
    }

    #[test]
    fn int_to_bitmask_max_native() {
        let x = 0xFFFF;
        let (b, trunc) = Bitmask::<u16, 2>::from_native(x);
        assert_eq!((b.value, b.bitmask(), trunc), (0xFFFF, 0xFFFF, false));
    }

    #[test]
    fn int_to_bitmask_zero() {
        let x = 0;
        let (b, trunc) = Bitmask::<u16, 2>::from_native(x);
        assert_eq!((b.value, b.bitmask(), trunc), (0, 0, false));
    }

    #[test]
    fn max_1_byte() {
        let b = Bitmask::<u8, 1>::max();
        assert_eq!((b.value, b.bitmask()), (0xFF, 0xFF));
    }

    #[test]
    fn max_2_byte() {
        let b = Bitmask::<u16, 2>::max();
        assert_eq!((b.value, b.bitmask()), (0xFFFF, 0xFFFF));
    }

    #[test]
    fn max_3_byte() {
        let b = Bitmask::<u32, 3>::max();
        assert_eq!((b.value, b.bitmask()), (0x00FF_FFFF, 0x00FF_FFFF));
    }
}

#[cfg(feature = "python")]
mod python {
    use super::Bitmask;

    use pyo3::conversion::FromPyObjectBound;
    use pyo3::exceptions::PyOverflowError;
    use pyo3::prelude::*;

    use std::fmt;

    impl<'py, T, const LEN: usize> FromPyObject<'py> for super::Bitmask<T, LEN>
    where
        for<'a> T: FromPyObjectBound<'a, 'py>,
        T: num_traits::PrimInt + fmt::Display,
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

    impl<'py, T, const LEN: usize> IntoPyObject<'py> for Bitmask<T, LEN>
    where
        T: IntoPyObject<'py>,
    {
        type Target = <T as IntoPyObject<'py>>::Target;
        type Output = <T as IntoPyObject<'py>>::Output;
        type Error = <T as IntoPyObject<'py>>::Error;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            self.value.into_pyobject(py)
        }
    }
}

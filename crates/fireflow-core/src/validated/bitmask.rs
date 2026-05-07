//! Types to represent the $PnB and $PnR values for a uint column.

use crate::convert::U32Ext as _;
use crate::text::byteord::PrivBytes;
use crate::text::keywords::TextRange;
use crate::validated::unaligned::FCSRepr;
use crate::validated::unaligned::{U24, U40, U48, U56};

use derive_new::new;
use num_traits::Bounded;
use thiserror::Error;

use std::ops::Shr;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{DisplayAsPyErr, FromInnerPyObject},
    fireflow_types::python as py,
    pyo3::prelude::*,
};

/// The type of an integer column for all versions.
#[derive(PartialEq, Clone, Copy, PartialOrd, Debug, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[new(visibility = "")]
pub struct Bitmask<T> {
    /// The value to be masked.
    ///
    /// This can be any integer up to the capacity of T.
    value: BitmaskValue<T>,

    /// The bitmask corresponding to [`Self::value`].
    ///
    /// Will always be a power of 2 minus 1 (ie, some number of contiguous bits
    /// in binary). This will be able to hold `value` but will mask out any
    /// bits beyond those needed to express `value`.
    bitmask: T,
}

/// Integer value for [`TextRange`] for a bitmask
#[derive(PartialEq, Clone, Copy, Debug, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromInnerPyObject, IntoPyObject))]
#[cfg_attr(feature = "python", bound(T: FromPyObject<'py>))]
pub struct BitmaskValue<T>(pub T);

pub type Bitmask08 = Bitmask<u8>;
pub type Bitmask16 = Bitmask<u16>;
pub type Bitmask24 = Bitmask<U24>;
pub type Bitmask32 = Bitmask<u32>;
pub type Bitmask40 = Bitmask<U40>;
pub type Bitmask48 = Bitmask<U48>;
pub type Bitmask56 = Bitmask<U56>;
pub type Bitmask64 = Bitmask<u64>;

impl<T> From<Bitmask<T>> for TextRange
where
    T: Copy + Into<u64>,
{
    fn from(value: Bitmask<T>) -> Self {
        // NOTE add 1 since the spec treats int ranges as one less than they
        // appear in TEXT
        Self::from(Into::<u64>::into(value.value.0) + 1)
    }
}

impl<T> From<&Bitmask<T>> for TextRange
where
    T: Copy + Into<u64>,
{
    fn from(value: &Bitmask<T>) -> Self {
        (*value).into()
    }
}

impl<T> From<Bitmask<T>> for u64
where
    T: Into<Self>,
{
    fn from(value: Bitmask<T>) -> Self {
        value.value.0.into()
    }
}

impl<T> TryFrom<u64> for Bitmask<T>
where
    T: Bounded + Shr<usize, Output = T> + Into<u64> + Copy + TryFrom<u64> + FCSRepr,
{
    type Error = NewBitmaskError;
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        let (new, trunc) = Self::from_u64(value);
        if trunc {
            let bytes = T::FILE_BYTES.0;
            let e = NewBitmaskError { bytes, value };
            return Err(e);
        }
        Ok(new)
    }
}

impl<T> Bitmask<T> {
    pub(crate) fn value(&self) -> T
    where
        T: Copy,
    {
        self.value.0
    }

    pub(crate) fn bitmask(&self) -> T
    where
        T: Copy,
    {
        self.bitmask
    }

    pub fn from_native(value: BitmaskValue<T>) -> Self
    where
        T: Bounded + Shr<usize, Output = T> + Into<u64> + Copy + FCSRepr,
    {
        // use min_value rather than zero to avoid constraints
        debug_assert!(T::min_value().into() == 0_u64, "min must be zero");
        let value64: u64 = value.0.into();
        let max_bits = u32::from(u8::from(T::FILE_BYTES)) * 8;
        let value_bits = u64::BITS - value64.leading_zeros();
        let mask = if value_bits == 0 {
            T::min_value()
        } else if value_bits == max_bits {
            T::max_value()
        } else {
            T::max_value() >> (max_bits - value_bits).u32_to_usize()
        };
        Self::new(value, mask)
    }

    pub(crate) fn from_u64(value: u64) -> (Self, bool)
    where
        T: Bounded + Shr<usize, Output = T> + Into<u64> + Copy + TryFrom<u64> + FCSRepr,
    {
        T::try_from(value)
            .map(|x| (Self::from_native(BitmaskValue(x)), false))
            .unwrap_or((Self::max(), true))
    }

    fn max() -> Self
    where
        T: Bounded,
    {
        Self::new(BitmaskValue(T::max_value()), T::max_value())
    }
}

/// Error when making a new [`Bitmask`] from a [`u64`].
#[derive(Error, Debug)]
#[error("Could not make a new {bytes}-byte bitmask from {value}, out of range")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct NewBitmaskError {
    bytes: PrivBytes,
    value: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn int_to_bitmask() {
        let x = BitmaskValue(0xFF);
        let b = Bitmask::<u8>::from_native(x);
        assert_eq!((b.value.0, b.bitmask()), (0xFF, 0xFF));
    }

    #[test]
    fn int_to_bitmask_roundup() {
        let x = BitmaskValue(0xFE);
        let b = Bitmask::<u8>::from_native(x);
        assert_eq!((b.value.0, b.bitmask()), (0xFE, 0xFF));
    }

    #[test]
    fn int_to_bitmask_max_native() {
        let x = BitmaskValue(0xFFFF);
        let b = Bitmask::<u16>::from_native(x);
        assert_eq!((b.value.0, b.bitmask()), (0xFFFF, 0xFFFF));
    }

    #[test]
    fn int_to_bitmask_zero() {
        let x = BitmaskValue(0);
        let b = Bitmask::<u16>::from_native(x);
        assert_eq!((b.value.0, b.bitmask()), (0, 0));
    }

    #[test]
    fn max_1_byte() {
        let b = Bitmask::<u8>::max();
        assert_eq!((b.value.0, b.bitmask()), (0xFF, 0xFF));
    }

    #[test]
    fn max_2_byte() {
        let b = Bitmask::<u16>::max();
        assert_eq!((b.value.0, b.bitmask()), (0xFFFF, 0xFFFF));
    }

    #[test]
    fn max_3_byte() {
        let b = Bitmask::<U24>::max();
        assert_eq!(
            (b.value.0, b.bitmask()),
            (
                0x00FF_FFFF_u32.try_into().unwrap(),
                0x00FF_FFFF_u32.try_into().unwrap()
            )
        );
    }
}

#[cfg(feature = "python")]
mod python {
    use crate::validated::unaligned::FCSRepr;

    use super::{Bitmask, BitmaskValue};

    use num_traits::Bounded;
    use pyo3::conversion::FromPyObjectBound;
    use pyo3::prelude::*;

    use std::fmt::Display;
    use std::ops::Shr;

    impl<'py, T> FromPyObject<'py> for super::Bitmask<T>
    where
        for<'a> T: FromPyObjectBound<'a, 'py>
            + Display
            + Into<u64>
            + FCSRepr
            + Bounded
            + Copy
            + Shr<usize, Output = T>,
    {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let x = ob.extract::<T>()?;
            Ok(Self::from_native(BitmaskValue(x)))
        }
    }

    impl<'py, T> IntoPyObject<'py> for Bitmask<T>
    where
        T: IntoPyObject<'py>,
    {
        type Target = <T as IntoPyObject<'py>>::Target;
        type Output = <T as IntoPyObject<'py>>::Output;
        type Error = <T as IntoPyObject<'py>>::Error;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            self.value.0.into_pyobject(py)
        }
    }
}

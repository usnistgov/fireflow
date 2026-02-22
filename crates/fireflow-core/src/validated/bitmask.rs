//! Types to represent the $PnB and $PnR values for a uint column.

use crate::logging::{
    CommutativeResultIter as _, DeferredError, ErrorsResult, LogResult, ResultExt as _,
};
use crate::text::byteord::PrivBytes;
use crate::text::index::MeasIndex;
use crate::text::keywords::Range;

use bigdecimal::BigDecimal;
use derive_more::Display;
use derive_new::new;
use num_traits::PrimInt;
use num_traits::identities::One as _;
use std::mem::size_of;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {fireflow_core_proc::FromInnerPyObject, pyo3::prelude::*};

/// The type of an integer column with `LEN` bytes in all versions.
#[derive(PartialEq, Clone, Copy, PartialOrd, Debug, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[new(visibility = "")]
pub struct Bitmask<T, const LEN: usize> {
    /// The value to be masked.
    ///
    /// This can be any integer up to LEN bits.
    value: BitmaskValue<T>,

    /// The bitmask corresponding to [`Self::value`].
    ///
    /// Will always be a power of 2 minus 1 (ie, some number of contiguous bits
    /// in binary). This will be able to hold `value` but will mask out any
    /// bits beyond those needed to express `value`.
    bitmask: T,
}

/// Integer value for [`Range`] for a bitmask
#[derive(PartialEq, Clone, Copy, Debug, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromInnerPyObject, IntoPyObject))]
#[cfg_attr(feature = "python", bound(T: FromPyObject<'py>))]
pub struct BitmaskValue<T>(pub T);

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
        Self::from(u64::from(value.value.0)) + Self::from(BigDecimal::one())
    }
}

impl<T, const LEN: usize> From<Bitmask<T, LEN>> for u64
where
    Self: From<T>,
{
    fn from(value: Bitmask<T, LEN>) -> Self {
        value.value.0.into()
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

    pub(crate) fn try_from_native(
        value: BitmaskValue<T>,
    ) -> DeferredError<Self, BitmaskTruncationError>
    where
        T: PrimInt,
        u64: From<T>,
    {
        let (bitmask, truncated) = Self::from_native(value);
        let error =
            truncated.then(|| BitmaskTruncationError::new(Self::bytes(), u64::from(value.0)));
        LogResult::new_deferred_maybe(bitmask, error)
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

    pub fn from_native(value: BitmaskValue<T>) -> (Self, bool)
    where
        T: PrimInt,
    {
        debug_assert!(size_of::<T>() * 8 <= 64, "type can only be 64-bit or less");
        let native_bits = u8::try_from(size_of::<T>() * 8).unwrap();
        let value_bits = native_bits - u8::try_from(value.0.leading_zeros()).unwrap();
        let truncated = value_bits > Self::bits();
        let bits = value_bits.min(Self::bits());
        let mask = if bits == 0 {
            T::zero()
        } else if bits == native_bits {
            T::max_value()
        } else {
            (T::one() << usize::from(bits)) - T::one()
        };
        let v = BitmaskValue(value.0.min(mask));
        (Self::new(v, mask), truncated)
    }

    pub(crate) fn from_u64(value: u64) -> (Self, bool)
    where
        T: PrimInt + TryFrom<u64>,
    {
        T::try_from(value)
            .map(BitmaskValue)
            .map(Self::from_native)
            .unwrap_or((Self::max(), true))
    }

    fn max() -> Self
    where
        T: PrimInt,
    {
        Self::from_native(BitmaskValue(T::max_value())).0
    }

    fn bytes() -> PrivBytes {
        u8::try_from(LEN)
            .unwrap()
            .try_into()
            .expect("Bytes greater than 8")
    }

    fn bits() -> u8 {
        u8::from(Self::bytes()) * 8
    }

    pub(crate) fn try_from_many<E, X>(
        xs: impl IntoIterator<Item = X>,
        starting_index: usize,
    ) -> ErrorsResult<Vec<Self>, (), (MeasIndex, E)>
    where
        Self: TryFrom<X, Error = E>,
    {
        xs.into_iter()
            .enumerate()
            .map(|(i, c)| {
                Self::try_from(c)
                    .map_err(|e| ((i + starting_index).into(), e))
                    .into_nowarn1()
                    .repack()
            })
            .sequence_commutative()
    }
}

/// Error when integer from $PnR must be truncated to fit into desired byte width.
///
/// This only occurs when attempting to bitmask a native type to a number of
/// bytes which is not a power of two (for instance, u32 to 3 bytes).  If $PnR
/// is bigger than the native type itself, this is different error.
///
/// This error is meant for internal use and converted to other errors which
/// add context.
#[derive(Debug, new)]
pub(crate) struct BitmaskTruncationError {
    pub(crate) bytes: PrivBytes,
    pub(crate) value: u64,
}

/// Error when integer is truncated using a bitmask which results in data loss
#[derive(Clone, Copy, Debug, Display)]
#[display("integer value truncated to {_0}")]
pub(crate) struct BitmaskLossError(pub u64);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn int_to_bitmask() {
        let x = BitmaskValue(0xFF);
        let (b, trunc) = Bitmask::<u16, 1>::from_native(x);
        assert_eq!((b.value.0, b.bitmask(), trunc), (0xFF, 0xFF, false));
    }

    #[test]
    fn int_to_bitmask_roundup() {
        let x = BitmaskValue(0xFE);
        let (b, trunc) = Bitmask::<u16, 1>::from_native(x);
        assert_eq!((b.value.0, b.bitmask(), trunc), (0xFE, 0xFF, false));
    }

    #[test]
    fn int_to_bitmask_trunc() {
        let x = BitmaskValue(0x100);
        let (b, trunc) = Bitmask::<u16, 1>::from_native(x);
        assert_eq!((b.value.0, b.bitmask(), trunc), (0xFF, 0xFF, true));
    }

    #[test]
    fn int_to_bitmask_max_native() {
        let x = BitmaskValue(0xFFFF);
        let (b, trunc) = Bitmask::<u16, 2>::from_native(x);
        assert_eq!((b.value.0, b.bitmask(), trunc), (0xFFFF, 0xFFFF, false));
    }

    #[test]
    fn int_to_bitmask_zero() {
        let x = BitmaskValue(0);
        let (b, trunc) = Bitmask::<u16, 2>::from_native(x);
        assert_eq!((b.value.0, b.bitmask(), trunc), (0, 0, false));
    }

    #[test]
    fn max_1_byte() {
        let b = Bitmask::<u8, 1>::max();
        assert_eq!((b.value.0, b.bitmask()), (0xFF, 0xFF));
    }

    #[test]
    fn max_2_byte() {
        let b = Bitmask::<u16, 2>::max();
        assert_eq!((b.value.0, b.bitmask()), (0xFFFF, 0xFFFF));
    }

    #[test]
    fn max_3_byte() {
        let b = Bitmask::<u32, 3>::max();
        assert_eq!((b.value.0, b.bitmask()), (0x00FF_FFFF, 0x00FF_FFFF));
    }
}

#[cfg(feature = "python")]
mod python {
    use super::{Bitmask, BitmaskValue};

    use pyo3::conversion::FromPyObjectBound;
    use pyo3::exceptions::PyOverflowError;
    use pyo3::prelude::*;

    use std::fmt;

    impl<'py, T, const LEN: usize> FromPyObject<'py> for super::Bitmask<T, LEN>
    where
        for<'a> T: FromPyObjectBound<'a, 'py> + num_traits::PrimInt + fmt::Display,
    {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let x = ob.extract::<T>()?;
            let (ret, trunc) = Self::from_native(BitmaskValue(x));
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
            self.value.0.into_pyobject(py)
        }
    }
}

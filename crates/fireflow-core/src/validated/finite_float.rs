use crate::text::keywords::TextRange;
use crate::validated::dataframe::FromValue;

use bigdecimal::BigDecimal;
use bigdecimal::num_bigint::Sign;
use derive_more::Display;
use derive_new::new;
use num_traits::{AsPrimitive as _, Bounded, ToPrimitive as _};
use thiserror::Error;

use std::any::type_name;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {fireflow_core_proc::DisplayAsPyErr, pyo3::prelude::*};

/// A float which has been validated to be a real number (not NaN or Inf).
#[derive(Clone, Copy, PartialEq, Debug, Display)]
#[repr(transparent)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
pub struct FiniteFloat<T>(T);

pub type FiniteF32 = FiniteFloat<f32>;

pub type FiniteF64 = FiniteFloat<f64>;

macro_rules! impl_float_decimal {
    ($t:ident, $to:ident) => {
        impl TryFrom<$t> for FiniteFloat<$t> {
            type Error = FloatToFiniteFloatError;

            fn try_from(value: $t) -> Result<Self, Self::Error> {
                if value.is_finite() {
                    Ok(Self(value))
                } else {
                    Err(FloatToFiniteFloatError)
                }
            }
        }

        impl From<FiniteFloat<$t>> for $t {
            fn from(value: FiniteFloat<$t>) -> Self {
                value.0
            }
        }

        // used when converting to TextRange
        impl From<FiniteFloat<$t>> for BigDecimal {
            fn from(value: FiniteFloat<$t>) -> Self {
                value.0.try_into().expect("float should not be NaN or Inf")
            }
        }

        // used for converting from TextRange
        impl TryFrom<BigDecimal> for FiniteFloat<$t> {
            type Error = DecimalToFloatError;
            fn try_from(value: BigDecimal) -> Result<Self, Self::Error> {
                match value.$to().and_then(|x| x.try_into().ok()) {
                    None => Err(DecimalToFloatError::new(value, type_name::<$t>())),
                    Some(x) => Ok(x),
                }
            }
        }
    };
}

impl_float_decimal!(f32, to_f32);
impl_float_decimal!(f64, to_f64);

impl<T> From<FiniteFloat<T>> for TextRange
where
    FiniteFloat<T>: Into<BigDecimal>,
{
    fn from(value: FiniteFloat<T>) -> Self {
        Self(value.into())
    }
}

impl<T: Bounded> Bounded for FiniteFloat<T> {
    fn min_value() -> Self {
        Self(T::min_value())
    }

    fn max_value() -> Self {
        Self(T::max_value())
    }
}

impl From<FiniteF32> for FiniteF64 {
    fn from(value: FiniteF32) -> Self {
        Self(value.0.as_())
    }
}

impl<T> TryFrom<u64> for FiniteFloat<T>
where
    T: FromValue<u64>,
{
    type Error = U64ToFiniteFloatError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        T::from_value(&value)
            .lossless()
            .map_or(Err(U64ToFiniteFloatError(value)), |x| Ok(Self(x)))
    }
}

impl TryFrom<FiniteF64> for FiniteF32 {
    type Error = FiniteF64toF32Error;

    fn try_from(value: FiniteF64) -> Result<Self, Self::Error> {
        f32::from_value(&value.0)
            .lossless()
            .map_or(Err(FiniteF64toF32Error(value)), |x| Ok(Self(x)))
    }
}

/// Error when converting [`u64`] to [`FiniteFloat`].
#[derive(Debug, Error)]
#[error("int '{0}' too large to be converted to float")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(
    feature = "python",
    pyerr(fireflow_types::python::InvalidKeywordValueError)
)]
pub struct U64ToFiniteFloatError(u64);

/// Error when converting [`FiniteF64`] to [`FiniteF32`].
#[derive(Debug, Error)]
#[error("64-bit float '{0}' too large to be converted to 32-bit finite float")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(
    feature = "python",
    pyerr(fireflow_types::python::InvalidKeywordValueError)
)]
pub struct FiniteF64toF32Error(FiniteF64);

/// Error when converting float to [`FiniteFloat`].
#[derive(Debug, Error)]
#[error("float could not be converted to decimal because it is not finite")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(
    feature = "python",
    pyerr(fireflow_types::python::InvalidKeywordValueError)
)]
pub struct FloatToFiniteFloatError;

/// Error when converting BigDecimal to f32 or f64
///
/// The only reason this may fail is due to being over or under the max/min
/// range of the target value.
#[derive(Debug, Error, new)]
#[new(visibility(""))]
#[error(
    "could not convert decimal '{src}' to {typename} since it is {x}",
    x = if self.over() {
        "over the maximum"
    } else {
        "under the minimum"
    }
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(
    feature = "python",
    pyerr(fireflow_types::python::InvalidKeywordValueError)
)]
pub struct DecimalToFloatError {
    src: BigDecimal,
    typename: &'static str,
}

impl DecimalToFloatError {
    pub(crate) fn over(&self) -> bool {
        let s = self.src.sign();
        debug_assert!(s != Sign::NoSign, "error when zero");
        s == Sign::Plus
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn str_to_float_dec_zero() {
        let d = "0".parse::<BigDecimal>().unwrap();
        assert!(FiniteFloat::<f32>::try_from(d.clone()).is_ok());
        assert!(FiniteFloat::<f64>::try_from(d).is_ok());
    }

    #[test]
    fn str_to_f32_submax() {
        let d = "34028236".parse::<BigDecimal>().unwrap();
        assert!(FiniteFloat::<f32>::try_from(d).is_ok());
    }

    #[test]
    fn str_to_f32_max() {
        let d = "340282350000000000000000000000000000000"
            .parse::<BigDecimal>()
            .unwrap();
        assert!(FiniteFloat::<f32>::try_from(d).is_ok());
    }

    #[test]
    fn str_to_f32_min() {
        let d = "-340282350000000000000000000000000000000"
            .parse::<BigDecimal>()
            .unwrap();
        assert!(FiniteFloat::<f32>::try_from(d).is_ok());
    }

    #[test]
    fn str_to_f32_hypermax() {
        let d = "340282358000000000000000000000000000000"
            .parse::<BigDecimal>()
            .unwrap();
        assert!(FiniteFloat::<f32>::try_from(d).is_err());
    }

    #[test]
    fn str_to_f32_hypermin() {
        let d = "-340282358000000000000000000000000000001"
            .parse::<BigDecimal>()
            .unwrap();
        assert!(FiniteFloat::<f32>::try_from(d).is_err());
    }

    #[test]
    fn str_to_f64_submax() {
        let d = "17976931348623158".parse::<BigDecimal>().unwrap();
        assert!(FiniteFloat::<f64>::try_from(d).is_ok());
    }

    #[test]
    fn str_to_f64_max() {
        let d = "179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
            .parse::<BigDecimal>()
            .unwrap();
        assert!(FiniteFloat::<f64>::try_from(d).is_ok());
    }

    #[test]
    fn str_to_f64_min() {
        let d = "-179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
            .parse::<BigDecimal>()
            .unwrap();
        assert!(FiniteFloat::<f64>::try_from(d).is_ok());
    }

    #[test]
    fn str_to_f64_hypermax() {
        let d = "179769313486231670000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
            .parse::<BigDecimal>()
            .unwrap();
        assert!(FiniteFloat::<f64>::try_from(d).is_err());
    }

    #[test]
    fn str_to_f64_hypermin() {
        let d = "-179769313486231670000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
            .parse::<BigDecimal>()
            .unwrap();
        assert!(FiniteFloat::<f64>::try_from(d).is_err());
    }
}

#[cfg(feature = "python")]
mod python {
    use super::{FiniteFloat, FloatToFiniteFloatError};

    use pyo3::prelude::*;

    impl<'py, T> FromPyObject<'py> for FiniteFloat<T>
    where
        T: FromPyObject<'py> + TryInto<Self, Error = FloatToFiniteFloatError>,
    {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let x = ob.extract::<T>()?;
            Ok(x.try_into()?)
        }
    }
}

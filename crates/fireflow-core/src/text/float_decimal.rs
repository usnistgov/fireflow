use crate::text::keywords::Range;

use bigdecimal::num_bigint::{BigUint, Sign};
use bigdecimal::{BigDecimal, ParseBigDecimalError};
use derive_more::Into;
use std::any::type_name;
use std::fmt;
use std::marker::PhantomData;

#[cfg(feature = "serde")]
use serde::Serialize;

/// A big decimal which has been validated to be within the range of a float.
#[derive(Clone, Into, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct FloatDecimal<T> {
    #[into]
    value: BigDecimal,
    _t: PhantomData<T>,
}

impl<T> FloatDecimal<T> {
    fn new(range: BigDecimal) -> Self {
        Self {
            value: range,
            _t: PhantomData,
        }
    }
}

impl TryFrom<f32> for FloatDecimal<f32> {
    type Error = ParseBigDecimalError;

    fn try_from(value: f32) -> Result<Self, Self::Error> {
        value.try_into().map(Self::new)
    }
}

impl TryFrom<f64> for FloatDecimal<f64> {
    type Error = ParseBigDecimalError;

    fn try_from(value: f64) -> Result<Self, Self::Error> {
        value.try_into().map(Self::new)
    }
}

impl<T> From<FloatDecimal<T>> for Range {
    fn from(value: FloatDecimal<T>) -> Self {
        Self(value.value)
    }
}

impl<T: HasFloatBounds> TryFrom<BigDecimal> for FloatDecimal<T> {
    type Error = DecimalToFloatError;

    fn try_from(value: BigDecimal) -> Result<Self, Self::Error> {
        let over = match value.sign() {
            Sign::NoSign => return Ok(FloatDecimal::new(value)),
            Sign::Minus => false,
            Sign::Plus => true,
        };
        let (bi, s) = value.normalized().into_bigint_and_scale();
        let (_, n) = bi.into_parts();
        println!("{:?}", value.normalized().sign());
        u64::try_from(n)
            .ok()
            .and_then(|x| {
                if x > T::DIGITS && s <= -i64::from(T::ZEROS) {
                    None
                } else {
                    Some(x)
                }
            })
            .map_or(
                Err(DecimalToFloatError {
                    src: value,
                    over,
                    typename: type_name::<T>(),
                }),
                |range| Ok(Self::new(BigDecimal::from_biguint(BigUint::from(range), s))),
            )
    }
}

pub trait HasFloatBounds: Sized {
    const DIGITS: u64;
    const ZEROS: u16;

    fn max_decimal() -> FloatDecimal<Self> {
        let u = BigUint::from(Self::DIGITS);
        let s = -i64::from(Self::ZEROS);
        FloatDecimal::new(BigDecimal::from_biguint(u, s))
    }

    fn min_decimal() -> FloatDecimal<Self> {
        FloatDecimal::new(-Self::max_decimal().value)
    }
}

impl HasFloatBounds for f32 {
    const DIGITS: u64 = 34028235;
    const ZEROS: u16 = 31;
}

impl HasFloatBounds for f64 {
    const DIGITS: u64 = 17976931348623157;
    const ZEROS: u16 = 292;
}

pub struct DecimalToFloatError {
    pub(crate) src: BigDecimal,
    pub(crate) over: bool,
    pub(crate) typename: &'static str,
}

impl fmt::Display for DecimalToFloatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let o = if self.over {
            "over the maximum"
        } else {
            "under the minimum"
        };
        write!(f, "{} is {o} range for {}", self.src, self.typename)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_str_to_float_dec_zero() {
        let d = "0".parse::<BigDecimal>().unwrap();
        assert_eq!(FloatDecimal::<f32>::try_from(d.clone()).is_ok(), true);
        assert_eq!(FloatDecimal::<f64>::try_from(d).is_ok(), true);
    }

    #[test]
    fn test_str_to_f32_submax() {
        let d = "34028236".parse::<BigDecimal>().unwrap();
        assert_eq!(FloatDecimal::<f32>::try_from(d).is_ok(), true);
    }

    #[test]
    fn test_str_to_f32_max() {
        let d = "340282350000000000000000000000000000000"
            .parse::<BigDecimal>()
            .unwrap();
        assert_eq!(FloatDecimal::<f32>::try_from(d).is_ok(), true);
    }

    #[test]
    fn test_str_to_f32_min() {
        let d = "-340282350000000000000000000000000000000"
            .parse::<BigDecimal>()
            .unwrap();
        assert_eq!(FloatDecimal::<f32>::try_from(d).is_ok(), true);
    }

    #[test]
    fn test_str_to_f32_hypermax() {
        let d = "340282350000000000000000000000000000001"
            .parse::<BigDecimal>()
            .unwrap();
        assert_eq!(FloatDecimal::<f32>::try_from(d).is_ok(), false);
    }

    #[test]
    fn test_str_to_f32_hypermin() {
        let d = "-340282350000000000000000000000000000001"
            .parse::<BigDecimal>()
            .unwrap();
        assert_eq!(FloatDecimal::<f32>::try_from(d).is_ok(), false);
    }

    #[test]
    fn test_str_to_f64_submax() {
        let d = "17976931348623158".parse::<BigDecimal>().unwrap();
        assert_eq!(FloatDecimal::<f64>::try_from(d).is_ok(), true);
    }

    #[test]
    fn test_str_to_f64_max() {
        let d = "179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
            .parse::<BigDecimal>()
            .unwrap();
        assert_eq!(FloatDecimal::<f64>::try_from(d).is_ok(), true);
    }

    #[test]
    fn test_str_to_f64_min() {
        let d = "-179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
            .parse::<BigDecimal>()
            .unwrap();
        assert_eq!(FloatDecimal::<f64>::try_from(d).is_ok(), true);
    }

    #[test]
    fn test_str_to_f64_hypermax() {
        let d = "179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001"
            .parse::<BigDecimal>()
            .unwrap();
        assert_eq!(FloatDecimal::<f64>::try_from(d).is_ok(), false);
    }

    #[test]
    fn test_str_to_f64_hypermin() {
        let d = "-179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001"
            .parse::<BigDecimal>()
            .unwrap();
        assert_eq!(FloatDecimal::<f64>::try_from(d).is_ok(), false);
    }
}

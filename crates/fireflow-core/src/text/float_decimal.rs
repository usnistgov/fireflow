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
    type Error = FloatToDecimalError;

    fn try_from(value: BigDecimal) -> Result<Self, Self::Error> {
        let over = match value.sign() {
            Sign::NoSign => return Ok(FloatDecimal::new(value)),
            Sign::Minus => false,
            Sign::Plus => true,
        };
        let (n, s) = value.normalized().into_bigint_and_scale();
        u64::try_from(n)
            .ok()
            .and_then(|x| {
                if x <= T::DIGITS && s >= -i64::from(T::ZEROS) {
                    Some(x)
                } else {
                    None
                }
            })
            .map_or(
                Err(FloatToDecimalError {
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

pub struct FloatToDecimalError {
    pub(crate) src: BigDecimal,
    pub(crate) over: bool,
    pub(crate) typename: &'static str,
}

impl fmt::Display for FloatToDecimalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let o = if self.over {
            "over the maximum"
        } else {
            "under the minimum"
        };
        write!(f, "{} is {o} range for {}", self.src, self.typename)
    }
}

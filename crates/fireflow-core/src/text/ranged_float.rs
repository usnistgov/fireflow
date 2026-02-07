use derive_more::{Add, Display, Into, Mul};
use num_derive::{One, Zero};
use std::fmt;
use std::num::ParseFloatError;
use std::str::FromStr;
use thiserror::Error;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use pyo3::prelude::*;

#[cfg(feature = "python")]
use fireflow_core_proc::{DisplayAsPyErr, TryFromPyObject};

/// A non-negative [`f32`]
#[derive(Clone, Copy, PartialEq, Display, Into, Add, Mul, One, Zero, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, TryFromPyObject))]
#[mul(forward)]
pub struct NonNegFloat(f32);

/// A positive [`f32`]
#[derive(Clone, Copy, PartialEq, Display, Into, Mul, One, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, TryFromPyObject))]
#[mul(forward)]
pub struct PositiveFloat(f32);

macro_rules! impl_ranged_float {
    ($type:ident, $op:tt, $zero:expr) => {
        impl FromStr for $type {
            type Err = RangedFloatError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                s.parse::<f32>()
                    .map_err(RangedFloatError::Parse)
                    .and_then(Self::try_from)
            }
        }

        impl TryFrom<f32> for $type {
            type Error = RangedFloatError;

            fn try_from(x: f32) -> Result<Self, RangedFloatError> {
                if 0.0 $op x {
                    Ok(Self(x))
                } else {
                    Err(RangedFloatError::Range {
                        x,
                        include_zero: $zero,
                    })
                }
            }
        }
    };
}

impl_ranged_float!(PositiveFloat, <, false);
impl_ranged_float!(NonNegFloat, <=, true);

/// Error when parsing either [`NonNegFloat`] or [`PositiveFloat`] from string
#[derive(Debug, Error)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(
    feature = "python",
    pyerr(fireflow_types::python::InvalidKeywordValueError)
)]
pub enum RangedFloatError {
    Parse(ParseFloatError),
    Range { x: f32, include_zero: bool },
}

impl fmt::Display for RangedFloatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Self::Parse(e) => e.fmt(f),
            Self::Range { x, include_zero } => {
                let gt = if *include_zero {
                    "greater than/equal to"
                } else {
                    "greater than"
                };
                write!(f, "float must be {gt} zero, got {x}")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn positive_float() {
        assert!(PositiveFloat::try_from(1.0_f32).is_ok());
        assert!(PositiveFloat::try_from(0.0_f32).is_err());
        assert!(PositiveFloat::try_from(-1.0_f32).is_err());
    }

    #[test]
    fn non_neg_float() {
        assert!(NonNegFloat::try_from(1.0_f32).is_ok());
        assert!(NonNegFloat::try_from(0.0_f32).is_ok());
        assert!(NonNegFloat::try_from(-1.0_f32).is_err());
    }
}

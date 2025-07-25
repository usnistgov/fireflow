use derive_more::{Add, Display, Into, Mul};
use num_derive::{One, Zero};
use std::fmt;
use std::num::ParseFloatError;
use std::str::FromStr;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use pyo3::prelude::*;

/// A non-negative float
#[derive(Clone, Copy, PartialEq, Display, Into, Add, Mul, One, Zero)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
#[mul(forward)]
pub struct NonNegFloat(f32);

/// A positive float
#[derive(Clone, Copy, PartialEq, Display, Into, Mul, One)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
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

pub enum RangedFloatError {
    Parse(ParseFloatError),
    Range { x: f32, include_zero: bool },
}

impl fmt::Display for RangedFloatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            RangedFloatError::Parse(e) => e.fmt(f),
            RangedFloatError::Range { x, include_zero } => {
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

#[cfg(feature = "python")]
mod python {
    use super::{NonNegFloat, PositiveFloat, RangedFloatError};
    use crate::python::macros::{impl_try_from_py, impl_value_err};

    impl_value_err!(RangedFloatError);
    impl_try_from_py!(PositiveFloat, f32);
    impl_try_from_py!(NonNegFloat, f32);
}

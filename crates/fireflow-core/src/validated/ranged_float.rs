use crate::macros::newtype_disp;

use serde::Serialize;
use std::fmt;
use std::num::ParseFloatError;
use std::str::FromStr;

/// A non-negative float
#[derive(Clone, Copy, Serialize, PartialEq)]
pub struct NonNegFloat(f32);

/// A positive float
#[derive(Clone, Copy, Serialize, PartialEq)]
pub struct PositiveFloat(f32);

newtype_disp!(NonNegFloat);
newtype_disp!(PositiveFloat);

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

        impl From<$type> for f32 {
            fn from(x: $type) -> Self {
                x.0
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

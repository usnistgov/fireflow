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

impl FromStr for NonNegFloat {
    type Err = RangedFloatError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<f32>()
            .map_err(RangedFloatError::Parse)
            .and_then(|x| {
                if x >= 0.0 {
                    Ok(NonNegFloat(x))
                } else {
                    Err(RangedFloatError::Range {
                        x,
                        include_zero: true,
                    })
                }
            })
    }
}

impl FromStr for PositiveFloat {
    type Err = RangedFloatError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<f32>()
            .map_err(RangedFloatError::Parse)
            .and_then(|x| {
                if x > 0.0 {
                    Ok(PositiveFloat(x))
                } else {
                    Err(RangedFloatError::Range {
                        x,
                        include_zero: false,
                    })
                }
            })
    }
}

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

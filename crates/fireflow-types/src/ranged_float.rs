use crate::nonempty_string::{ToDisplayNE, ambassador_impl_ToDisplayNE};

use ambassador::Delegate;
use derive_more::{Add, Into, Mul};
use num_derive::{One, Zero};
use thiserror::Error;

use std::fmt;
use std::num::ParseFloatError;
use std::str::FromStr;

#[cfg(feature = "testutil")]
use proptest::prelude::*;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{DisplayAsPyErr, TryFromPyObject},
    pyo3::prelude::*,
};

/// A non-negative [`f32`]
///
/// `NaN` and `-inf` are also forbidden.
#[derive(Clone, Copy, PartialEq, Into, Add, Mul, One, Zero, Debug, Delegate)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, TryFromPyObject))]
#[mul(forward)]
#[delegate(ToDisplayNE<'a>, generics = "'a")]
pub struct NonNegFloat(f32);

/// A positive [`f32`]
///
/// `NaN` and `-inf` are also forbidden.
#[derive(Clone, Copy, PartialEq, Into, Mul, One, Debug, Delegate)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, TryFromPyObject))]
#[mul(forward)]
#[delegate(ToDisplayNE<'a>, generics = "'a")]
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
#[derive(Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::InvalidKeywordValueError))]
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

#[cfg(feature = "testutil")]
impl Arbitrary for PositiveFloat {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with((): Self::Parameters) -> Self::Strategy {
        (0.0_f32..)
            .prop_filter("0.0 is not allowed", |&x| x > 0.0)
            .prop_map(|x| Self::try_from(x).unwrap())
            .boxed()
    }
}

#[cfg(feature = "testutil")]
impl Arbitrary for NonNegFloat {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with((): Self::Parameters) -> Self::Strategy {
        (0.0_f32..).prop_map(|x| Self::try_from(x).unwrap()).boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use proptest::num::f32;

    proptest! {
        #[test]
        fn positive_float(x in 0.0_f32..) {
            prop_assume! {
                x > 0.0
            }
            assert!(PositiveFloat::try_from(x).is_ok());
        }
    }

    #[test]
    fn positive_float_inf() {
        assert!(PositiveFloat::try_from(f32::INFINITY).is_ok());
    }

    #[test]
    fn positive_float_err() {
        assert!(PositiveFloat::try_from(f32::NAN).is_err());
        assert!(PositiveFloat::try_from(f32::NEG_INFINITY).is_err());
        assert!(PositiveFloat::try_from(0.0_f32).is_err());
        assert!(PositiveFloat::try_from(-1.0_f32).is_err());
    }

    proptest! {
        #[test]
        fn non_neg_float(x in 0.0_f32..) {
            assert!(NonNegFloat::try_from(x).is_ok());
        }
    }

    #[test]
    fn non_neg_float_inf() {
        assert!(NonNegFloat::try_from(f32::INFINITY).is_ok());
    }

    #[test]
    fn non_neg_float_err() {
        assert!(NonNegFloat::try_from(f32::NAN).is_err());
        assert!(NonNegFloat::try_from(f32::NEG_INFINITY).is_err());
        assert!(NonNegFloat::try_from(0.0_f32).is_ok());
        assert!(NonNegFloat::try_from(-1.0_f32).is_err());
    }
}

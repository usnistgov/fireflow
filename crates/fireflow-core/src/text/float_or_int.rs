use crate::macros::{newtype_disp, newtype_from_outer};

use super::byteord::Bytes;

use num_traits::bounds::Bounded;
use num_traits::AsPrimitive;
use serde::Serialize;
use std::fmt;
use std::num::{ParseFloatError, ParseIntError};
use std::str::FromStr;

/// The value of the $PnR key (all versions)
///
/// Technically this should only be an integer, but many versions also store
/// floats which makes sense for cases where $DATATYPE/$PnDATATYPE indicates
/// float or double.
#[derive(Clone, Copy, Serialize, PartialEq)]
pub enum FloatOrInt {
    Float(NonNanF64),
    Int(u64),
}

/// A float which is never NaN
#[derive(Clone, Copy, Serialize, PartialEq, Default)]
pub struct NonNanFloat<T>(T);

pub type NonNanF32 = NonNanFloat<f32>;
pub type NonNanF64 = NonNanFloat<f64>;

newtype_from_outer!(NonNanF64, f64);
newtype_disp!(NonNanF64);

pub(crate) trait FloatProps: Sized {
    fn maxval() -> NonNanFloat<Self>;
}

impl NonNanF64 {
    fn to_int<T>(&self) -> Result<T, FloatToIntError<T>>
    where
        T: AsPrimitive<f64> + Bounded,
        f64: AsPrimitive<T>,
    {
        let y = f64::from(*self);
        if y < 0.0 {
            Err(FloatToIntError::FloatUnderrange(y))
        } else if (T::max_value().as_()) < y {
            Err(FloatToIntError::FloatOverrange(y))
        } else if y.fract() != 0.0 {
            Err(FloatToIntError::FloatPrecisionLoss(y, y.as_()))
        } else {
            Ok(y.as_())
        }
    }
}

impl FloatProps for f32 {
    fn maxval() -> NonNanFloat<Self> {
        NonNanFloat(Self::MAX)
    }
}

impl FloatProps for f64 {
    fn maxval() -> NonNanFloat<Self> {
        NonNanFloat(Self::MAX)
    }
}

impl<T> NonNanFloat<T> {
    pub(crate) fn inner_into<X: From<T>>(self) -> NonNanFloat<X> {
        NonNanFloat(self.0.into())
    }
}

impl FromStr for FloatOrInt {
    type Err = ParseFloatOrIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Try to parse as u64 first. The only errors we should get are
        // underflow, overflow, or invalid digit (zero shouldn't happen for u64
        // and we can assume the incoming string is not empty since we don't
        // store empty keywords). In any of these cases, a float might work; if
        // it doesn't then this is truly an error. Also, if there is a very
        // large/small number, this should parse to -/+ Inf. NaN should never be
        // stored.
        match s.parse::<u64>() {
            Ok(x) => Ok(FloatOrInt::Int(x)),
            Err(_) => match s.parse::<f64>() {
                Ok(x) => NonNanF64::try_from(x)
                    .map(FloatOrInt::Float)
                    .map_err(ParseFloatOrIntError::Nan),
                Err(e) => Err(ParseFloatOrIntError::Float(e)),
            },
        }
    }
}

impl fmt::Display for FloatOrInt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Self::Float(x) => write!(f, "{x}"),
            Self::Int(x) => write!(f, "{x}"),
        }
    }
}

impl From<u64> for FloatOrInt {
    fn from(value: u64) -> Self {
        Self::Int(value)
    }
}

impl TryFrom<FloatOrInt> for Bytes {
    type Error = FloatToIntError<u64>;

    fn try_from(value: FloatOrInt) -> Result<Self, Self::Error> {
        match value {
            FloatOrInt::Int(x) => Ok(Bytes::from_u64(x)),
            FloatOrInt::Float(x) => x.to_int::<u64>().map(Bytes::from_u64),
        }
    }
}

macro_rules! impl_non_nan_float {
    ($inner:ident, $outer:ident) => {
        impl TryFrom<$inner> for $outer {
            type Error = NanFloatError;
            fn try_from(value: $inner) -> Result<Self, Self::Error> {
                if value.is_nan() {
                    return Err(NanFloatError);
                }
                Ok(Self(value))
            }
        }
    };
}

impl_non_nan_float!(f32, NonNanF32);
impl_non_nan_float!(f64, NonNanF64);

#[derive(Debug)]
pub struct NanFloatError;

impl fmt::Display for NanFloatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "float cannot be NaN when used for Range")
    }
}

pub enum ParseFloatOrIntError {
    Float(ParseFloatError),
    Int(ParseIntError),
    Nan(NanFloatError),
}

impl fmt::Display for ParseFloatOrIntError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            ParseFloatOrIntError::Float(e) => e.fmt(f),
            ParseFloatOrIntError::Int(e) => e.fmt(f),
            ParseFloatOrIntError::Nan(e) => e.fmt(f),
        }
    }
}

macro_rules! try_from_range_int {
    ($inttype:ident) => {
        impl TryFrom<FloatOrInt> for $inttype {
            type Error = ToIntError<$inttype>;

            fn try_from(value: FloatOrInt) -> Result<Self, Self::Error> {
                match value {
                    FloatOrInt::Int(x) => {
                        $inttype::try_from(x).or(Err(ToIntError::IntOverrange(x)))
                    }
                    FloatOrInt::Float(x) => x.to_int().map_err(ToIntError::Float),
                }
            }
        }
    };
}

try_from_range_int!(u8);
try_from_range_int!(u16);
try_from_range_int!(u32);
try_from_range_int!(u64);

impl TryFrom<FloatOrInt> for NonNanF32 {
    type Error = ToFloatError<f32>;
    fn try_from(value: FloatOrInt) -> Result<Self, Self::Error> {
        match value {
            FloatOrInt::Int(x) => {
                if x < 2_u64.pow(24) {
                    Ok(NonNanFloat(x as f32))
                } else {
                    let y = x as f32;
                    let z = y as u64;
                    if x != z {
                        Err(ToFloatError::IntPrecisionLoss(x, NonNanFloat(y)))
                    } else {
                        Ok(NonNanFloat(y))
                    }
                }
            }
            FloatOrInt::Float(x) => {
                let y = f64::from(x);
                if y <= (f32::MIN as f64) {
                    Err(ToFloatError::FloatUnderrange(y))
                } else if (f32::MAX as f64) <= y {
                    Err(ToFloatError::FloatOverrange(y))
                } else {
                    Ok(NonNanFloat(y as f32))
                }
            }
        }
    }
}

impl TryFrom<FloatOrInt> for NonNanF64 {
    type Error = ToFloatError<f64>;
    fn try_from(value: FloatOrInt) -> Result<Self, Self::Error> {
        match value {
            FloatOrInt::Int(x) => {
                if x < 2_u64.pow(53) {
                    Ok(NonNanFloat(x as f64))
                } else {
                    let y = x as f64;
                    let z = y as u64;
                    if x != z {
                        Err(ToFloatError::IntPrecisionLoss(x, NonNanFloat(y)))
                    } else {
                        Ok(NonNanFloat(y))
                    }
                }
            }
            FloatOrInt::Float(x) => Ok(x.into()),
        }
    }
}

pub enum ToIntError<X> {
    /// u64 is larger than target int can hold
    IntOverrange(u64),
    Float(FloatToIntError<X>),
}

pub enum FloatToIntError<X> {
    /// f64 is larger than target int can hold
    FloatOverrange(f64),
    /// f64 is smaller than target int can hold
    FloatUnderrange(f64),
    /// f64 would lose precision when converted to int
    FloatPrecisionLoss(f64, X),
}

impl<X: fmt::Display> fmt::Display for ToIntError<X> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            // TODO what is the target type?
            Self::IntOverrange(x) => {
                write!(
                    f,
                    "integer range {x} is larger than target unsigned integer can hold"
                )
            }
            Self::Float(e) => e.fmt(f),
        }
    }
}

impl<X: fmt::Display> fmt::Display for FloatToIntError<X> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Self::FloatOverrange(x) => {
                write!(
                    f,
                    "float range {x} is larger than target unsigned integer can hold"
                )
            }
            Self::FloatUnderrange(x) => {
                write!(
                    f,
                    "float range {x} is less than zero and \
                     could not be converted to unsigned integer"
                )
            }
            Self::FloatPrecisionLoss(x, y) => {
                write!(f, "float range {x} lost precision when converting to {y}")
            }
        }
    }
}

pub enum ToFloatError<X> {
    /// u64 would lose precision when converted to a float
    IntPrecisionLoss(u64, NonNanFloat<X>),
    /// f64 is larger than target float can hold
    FloatOverrange(f64),
    /// f64 is smaller than target float can hold
    FloatUnderrange(f64),
}

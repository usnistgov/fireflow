use crate::error::BiTentative;
use crate::macros::{newtype_disp, newtype_from_outer};
use crate::validated::ascii_range::Chars;

use num_traits::bounds::Bounded;
use num_traits::float::Float;
use num_traits::AsPrimitive;
use num_traits::PrimInt;
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

impl NonNanF64 {
    fn as_int<T>(&self) -> Result<T, FloatToIntError<T>>
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

impl<T> NonNanFloat<T> {
    pub(crate) fn inner_into<X: From<T>>(self) -> NonNanFloat<X> {
        NonNanFloat(self.0.into())
    }

    pub(crate) fn max_value() -> Self
    where
        T: Float,
    {
        Self(T::max_value())
    }

    pub(crate) fn zero() -> Self
    where
        T: Float,
    {
        Self(T::zero())
    }
}

impl NonNanF64 {
    pub(crate) fn try_as_f32(self) -> Option<NonNanF32> {
        // TODO, this actually isn't as intuitive as one would hope. For
        // example, "1.1" as input will fail this test, since the f64->f32-f64
        // conversion will result in "1.100000023841858" and thus return false.
        // Apparently the "as" cast will not try to force the result to be as
        // close numerically to the original as possible; in this case
        // it will merely add 0s to the end of the f32, which is different than
        // the starting value
        let x = f64::from(self);
        let y = x as f32;
        let z = y as f64;
        if x == z {
            Some(NonNanFloat(y))
        } else {
            None
        }
    }
}

impl FloatOrInt {
    pub(crate) fn as_uint<T>(&self, notrunc: bool) -> BiTentative<T, IntRangeError>
    where
        T: TryFrom<Self, Error = ToIntError<T>> + PrimInt,
    {
        let (b, e) = (*self).try_into().map_or_else(
            |e| match e {
                ToIntError::IntOverrange(x) => {
                    (T::max_value(), Some(IntRangeError::IntOverrange(x)))
                }
                ToIntError::Float(FloatToIntError::FloatOverrange(x)) => {
                    (T::max_value(), Some(IntRangeError::FloatOverrange(x)))
                }
                ToIntError::Float(FloatToIntError::FloatUnderrange(x)) => {
                    (T::zero(), Some(IntRangeError::FloatUnderrange(x)))
                }
                ToIntError::Float(FloatToIntError::FloatPrecisionLoss(x, y)) => {
                    (y, Some(IntRangeError::FloatPrecisionLoss(x)))
                }
            },
            |x| (x, None),
        );
        BiTentative::new_either1(b, e, notrunc)
    }

    pub(crate) fn as_float<T>(&self, notrunc: bool) -> BiTentative<NonNanFloat<T>, FloatRangeError>
    where
        NonNanFloat<T>: TryFrom<Self, Error = ToFloatError<T>>,
        T: Float,
    {
        let (x, e) = (*self).try_into().map_or_else(
            |e| match e {
                ToFloatError::IntPrecisionLoss(y, x) => {
                    (x, Some(FloatRangeError::IntPrecisionLoss(y)))
                }
                ToFloatError::FloatOverrange(y) => (
                    NonNanFloat::max_value(),
                    Some(FloatRangeError::FloatOverrange(y)),
                ),
                ToFloatError::FloatUnderrange(y) => (
                    NonNanFloat::zero(),
                    Some(FloatRangeError::FloatUnderrange(y)),
                ),
            },
            |x| (x, None),
        );
        BiTentative::new_either1(x, e, notrunc)
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

impl TryFrom<FloatOrInt> for Chars {
    type Error = FloatToIntError<u64>;

    fn try_from(value: FloatOrInt) -> Result<Self, Self::Error> {
        match value {
            FloatOrInt::Int(x) => Ok(Chars::from_u64(x)),
            FloatOrInt::Float(x) => x.as_int::<u64>().map(Chars::from_u64),
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
                    FloatOrInt::Float(x) => x.as_int().map_err(ToIntError::Float),
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
            FloatOrInt::Float(x) => Ok(x),
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

pub enum ToFloatError<X> {
    /// u64 would lose precision when converted to a float
    IntPrecisionLoss(u64, NonNanFloat<X>),
    /// f64 is larger than target float can hold
    FloatOverrange(f64),
    /// f64 is smaller than target float can hold
    FloatUnderrange(f64),
}

pub enum IntRangeError {
    IntOverrange(u64),
    FloatOverrange(f64),
    FloatUnderrange(f64),
    FloatPrecisionLoss(f64),
}

pub enum FloatRangeError {
    IntPrecisionLoss(u64),
    FloatOverrange(f64),
    FloatUnderrange(f64),
}

impl<X> From<ToIntError<X>> for IntRangeError {
    fn from(value: ToIntError<X>) -> Self {
        match value {
            ToIntError::IntOverrange(x) => Self::IntOverrange(x),
            ToIntError::Float(e) => match e {
                FloatToIntError::FloatOverrange(x) => Self::FloatOverrange(x),
                FloatToIntError::FloatUnderrange(x) => Self::FloatUnderrange(x),
                FloatToIntError::FloatPrecisionLoss(x, _) => Self::FloatPrecisionLoss(x),
            },
        }
    }
}

impl<X> From<ToFloatError<X>> for FloatRangeError {
    fn from(value: ToFloatError<X>) -> Self {
        match value {
            ToFloatError::FloatOverrange(x) => Self::FloatOverrange(x),
            ToFloatError::FloatUnderrange(x) => Self::FloatUnderrange(x),
            ToFloatError::IntPrecisionLoss(x, _) => Self::IntPrecisionLoss(x),
        }
    }
}

impl fmt::Display for IntRangeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            // TODO what is the target type?
            Self::IntOverrange(x) => {
                write!(
                    f,
                    "integer range {x} is larger than target unsigned integer can hold"
                )
            }
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
            Self::FloatPrecisionLoss(x) => {
                write!(
                    f,
                    "float range {x} lost precision when converting to unsigned integer"
                )
            }
        }
    }
}

impl fmt::Display for FloatRangeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Self::IntPrecisionLoss(x) => {
                write!(f, "int {x} is more precise than target float")
            }
            Self::FloatOverrange(x) => {
                write!(f, "{x} is larger than target float can hold")
            }
            Self::FloatUnderrange(x) => {
                write!(f, "{x} is smaller than target float can hold")
            }
        }
    }
}

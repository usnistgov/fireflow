use serde::Serialize;
use std::fmt;
use std::num::{ParseFloatError, ParseIntError};
use std::str::FromStr;

/// The value of the $PnR key (all versions)
///
/// Technically this should only be an integer, but many versions also store
/// floats which makes sense for cases where $DATATYPE/$PnDATATYPE indicates
/// float or double.
#[derive(Clone, Copy, Serialize)]
pub enum FloatOrInt {
    // this should never be NaN
    Float(f64),
    Int(u64),
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
                Ok(x) => Ok(FloatOrInt::Float(x)),
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

impl TryFrom<f64> for FloatOrInt {
    type Error = NanFloatOrInt;
    fn try_from(value: f64) -> Result<Self, Self::Error> {
        if value.is_nan() {
            return Err(NanFloatOrInt);
        }
        Ok(Self::Float(value))
    }
}

#[derive(Debug)]
pub struct NanFloatOrInt;

impl fmt::Display for NanFloatOrInt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "float cannot be NaN when used for Range")
    }
}

pub enum ParseFloatOrIntError {
    Float(ParseFloatError),
    Int(ParseIntError),
}

impl fmt::Display for ParseFloatOrIntError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            ParseFloatOrIntError::Float(e) => e.fmt(f),
            ParseFloatOrIntError::Int(e) => e.fmt(f),
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
                        if ($inttype::MAX as u64) < x {
                            Err(ToIntError::IntOverrange(x))
                        } else {
                            Ok(x as $inttype)
                        }
                    }
                    FloatOrInt::Float(x) => {
                        if x < 0.0 {
                            Err(ToIntError::FloatUnderrange(x))
                        } else if ($inttype::MAX as f64) < x {
                            Err(ToIntError::FloatOverrange(x))
                        } else if x.fract() != 0.0 {
                            Err(ToIntError::FloatPrecisionLoss(x, x as $inttype))
                        } else {
                            Ok(x as $inttype)
                        }
                    }
                }
            }
        }
    };
}

try_from_range_int!(u8);
try_from_range_int!(u16);
try_from_range_int!(u32);
try_from_range_int!(u64);

impl TryFrom<FloatOrInt> for f32 {
    type Error = ToFloatError<f32>;
    fn try_from(value: FloatOrInt) -> Result<Self, Self::Error> {
        match value {
            FloatOrInt::Int(x) => {
                if x < 2_u64.pow(24) {
                    Ok(x as f32)
                } else {
                    let y = x as f32;
                    let z = y as u64;
                    if x != z {
                        Err(ToFloatError::IntPrecisionLoss(x, y))
                    } else {
                        Ok(y)
                    }
                }
            }
            FloatOrInt::Float(x) => {
                if x <= (f32::MIN as f64) {
                    Err(ToFloatError::FloatUnderrange(x))
                } else if (f32::MAX as f64) <= x {
                    Err(ToFloatError::FloatOverrange(x))
                } else {
                    Ok(x as f32)
                }
            }
        }
    }
}

impl TryFrom<FloatOrInt> for f64 {
    type Error = ToFloatError<f64>;
    fn try_from(value: FloatOrInt) -> Result<Self, Self::Error> {
        match value {
            FloatOrInt::Int(x) => {
                if x < 2_u64.pow(53) {
                    Ok(x as f64)
                } else {
                    let y = x as f64;
                    let z = y as u64;
                    if x != z {
                        Err(ToFloatError::IntPrecisionLoss(x, y))
                    } else {
                        Ok(y)
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
    IntPrecisionLoss(u64, X),
    /// f64 is larger than target float can hold
    FloatOverrange(f64),
    /// f64 is smaller than target float can hold
    FloatUnderrange(f64),
}

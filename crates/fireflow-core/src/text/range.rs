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
pub enum Range {
    // this should never be NaN
    Float(f64),
    Int(u64),
}

impl FromStr for Range {
    type Err = ParseRangeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Try to parse as u64 first. The only errors we should get are
        // underflow, overflow, or invalid digit (zero shouldn't happen for u64
        // and we can assume the incoming string is not empty since we don't
        // store empty keywords). In any of these cases, a float might work; if
        // it doesn't then this is truly an error. Also, if there is a very
        // large/small number, this should parse to -/+ Inf. NaN should never be
        // stored.
        match s.parse::<u64>() {
            Ok(x) => Ok(Range::Int(x)),
            Err(_) => match s.parse::<f64>() {
                Ok(x) => Ok(Range::Float(x)),
                Err(e) => Err(ParseRangeError::Float(e)),
            },
        }
    }
}

impl fmt::Display for Range {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Range::Float(x) => write!(f, "{x}"),
            Range::Int(x) => write!(f, "{x}"),
        }
    }
}

impl From<u64> for Range {
    fn from(value: u64) -> Self {
        Range::Int(value)
    }
}

impl TryFrom<f64> for Range {
    type Error = NanRange;
    fn try_from(value: f64) -> Result<Self, Self::Error> {
        if value.is_nan() {
            return Err(NanRange);
        }
        Ok(Range::Float(value))
    }
}

#[derive(Debug)]
pub struct NanRange;

impl fmt::Display for NanRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "float cannot be NaN when used for Range")
    }
}

pub enum ParseRangeError {
    Float(ParseFloatError),
    Int(ParseIntError),
}

impl fmt::Display for ParseRangeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            ParseRangeError::Float(e) => e.fmt(f),
            ParseRangeError::Int(e) => e.fmt(f),
        }
    }
}

macro_rules! try_from_range_int {
    ($inttype:ident) => {
        impl TryFrom<Range> for $inttype {
            type Error = RangeToIntError<$inttype>;
            fn try_from(value: Range) -> Result<Self, Self::Error> {
                match value {
                    Range::Int(x) => {
                        if ($inttype::MAX as u64) < x {
                            Err(RangeToIntError::IntOverrange(x))
                        } else {
                            Ok(x as $inttype)
                        }
                    }
                    Range::Float(x) => {
                        if x < 0.0 {
                            Err(RangeToIntError::FloatUnderrange(x))
                        } else if ($inttype::MAX as f64) < x {
                            Err(RangeToIntError::FloatOverrange(x))
                        } else if x.fract() != 0.0 {
                            Err(RangeToIntError::FloatPrecisionLoss(x, x as $inttype))
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

impl TryFrom<Range> for f32 {
    type Error = RangeToFloatError<f32>;
    fn try_from(value: Range) -> Result<Self, Self::Error> {
        match value {
            Range::Int(x) => {
                if x < 2_u64.pow(24) {
                    Ok(x as f32)
                } else {
                    let y = x as f32;
                    let z = y as u64;
                    if x != z {
                        Err(RangeToFloatError::IntPrecisionLoss(x, y))
                    } else {
                        Ok(y)
                    }
                }
            }
            Range::Float(x) => {
                if x <= (f32::MIN as f64) {
                    Err(RangeToFloatError::FloatUnderrange(x))
                } else if (f32::MAX as f64) <= x {
                    Err(RangeToFloatError::FloatOverrange(x))
                } else {
                    Ok(x as f32)
                }
            }
        }
    }
}

impl TryFrom<Range> for f64 {
    type Error = RangeToFloatError<f64>;
    fn try_from(value: Range) -> Result<Self, Self::Error> {
        match value {
            Range::Int(x) => {
                if x < 2_u64.pow(53) {
                    Ok(x as f64)
                } else {
                    let y = x as f64;
                    let z = y as u64;
                    if x != z {
                        Err(RangeToFloatError::IntPrecisionLoss(x, y))
                    } else {
                        Ok(y)
                    }
                }
            }
            Range::Float(x) => Ok(x),
        }
    }
}

pub enum RangeToIntError<X> {
    /// u64 is larger than target int can hold
    IntOverrange(u64),
    /// f64 is larger than target int can hold
    FloatOverrange(f64),
    /// f64 is smaller than target int can hold
    FloatUnderrange(f64),
    /// f64 would lose precision when converted to int
    FloatPrecisionLoss(f64, X),
}

pub enum RangeToFloatError<X> {
    /// u64 would lose precision when converted to a float
    IntPrecisionLoss(u64, X),
    /// f64 is larger than target float can hold
    FloatOverrange(f64),
    /// f64 is smaller than target float can hold
    FloatUnderrange(f64),
}

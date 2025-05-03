use serde::Serialize;

use std::fmt;
use std::num::ParseFloatError;
use std::str::FromStr;

/// The value for the $PnE key (all versions).
///
/// Format is assumed to be 'f1,f2'
#[derive(Clone, Copy, PartialEq, Serialize)]
pub enum Scale {
    /// Linear scale, which maps to the value '0,0'
    Linear,

    /// Log scale, which maps to anything not '0,0' (although decades should be
    /// a positive number presumably)
    Log(LogScale),
}

#[derive(Clone, Copy, PartialEq, Serialize)]
pub struct LogScale {
    decades: f32,
    offset: f32,
}

impl Scale {
    pub fn try_new_log(decades: f32, offset: f32) -> Option<Self> {
        if decades > 0.0 && offset > 0.0 {
            Some(Scale::Log(LogScale { decades, offset }))
        } else {
            None
        }
    }
}

impl FromStr for Scale {
    type Err = ScaleError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.split(",").collect::<Vec<_>>()[..] {
            [ds, os] => {
                let f1 = ds.parse().map_err(ScaleError::FloatError)?;
                let f2 = os.parse().map_err(ScaleError::FloatError)?;
                match (f1, f2) {
                    (0.0, 0.0) => Ok(Scale::Linear),
                    (decades, offset) => Scale::try_new_log(decades, offset)
                        .ok_or(ScaleError::LogRange { decades, offset }),
                }
            }
            _ => Err(ScaleError::WrongFormat),
        }
    }
}

impl fmt::Display for Scale {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Scale::Log(LogScale { decades, offset }) => write!(f, "{decades},{offset}"),
            Scale::Linear => write!(f, "Lin"),
        }
    }
}

pub enum ScaleError {
    FloatError(ParseFloatError),
    LogRange { decades: f32, offset: f32 },
    WrongFormat,
}

impl fmt::Display for ScaleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            ScaleError::FloatError(x) => write!(f, "{}", x),
            ScaleError::WrongFormat => write!(f, "must be like 'f1,f2'"),
            ScaleError::LogRange { decades, offset } => write!(
                f,
                "decades/offset must both be positive, got '{decades},{offset}'"
            ),
        }
    }
}

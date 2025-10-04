use crate::config::StdTextReadConfig;
use crate::error::*;
use crate::text::parser::*;
use crate::text::ranged_float::*;

use num_traits::identities::One;
use std::fmt;
use std::num::ParseFloatError;
use std::str::FromStr;
use thiserror::Error;

#[cfg(feature = "serde")]
use serde::Serialize;

/// The value for the $PnE key (all versions).
///
/// Format is assumed to be 'f1,f2'
#[derive(Clone, Copy, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum Scale {
    /// Linear scale (ie '0,0')
    Linear,

    /// Log scale, where both numbers are positive
    Log(LogScale),
}

#[derive(Clone, Copy, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct LogScale {
    pub decades: PositiveFloat,
    pub offset: PositiveFloat,
}

impl fmt::Display for LogScale {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{},{}", self.decades, self.offset)
    }
}

impl Scale {
    pub fn try_new_log(decades: f32, offset: f32) -> Result<Self, LogRangeError> {
        (decades, offset).try_into().map(Self::Log)
    }
}

impl TryFrom<(f32, f32)> for LogScale {
    type Error = LogRangeError;

    fn try_from(value: (f32, f32)) -> Result<Self, Self::Error> {
        let (d0, o0) = value;
        PositiveFloat::try_from(d0)
            .zip(PositiveFloat::try_from(o0))
            .map(|(decades, offset)| Self { decades, offset })
            .map_err(|_| LogRangeError {
                decades: d0,
                offset: o0,
            })
    }
}

impl FromStrStateful for Scale {
    type Err = ScaleError;
    type Payload<'a> = ();

    fn from_str_st(s: &str, _: (), conf: &StdTextReadConfig) -> Result<Self, Self::Err> {
        let res = Self::from_str_delim(s, conf.trim_intra_value_whitespace);
        if conf.fix_log_scale_offsets {
            res.or_else(|e| {
                if let ScaleError::LogRange(le) = e {
                    le.try_fix_offset()
                        .map(Scale::Log)
                        .map_err(ScaleError::LogRange)
                } else {
                    Err(e)
                }
            })
        } else {
            res
        }
    }
}

impl FromStr for Scale {
    type Err = ScaleError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_str_delim(s, false)
    }
}

impl FromStrDelim for Scale {
    type Err = ScaleError;
    const DELIM: char = ',';

    fn from_iter<'a>(ss: impl Iterator<Item = &'a str>) -> Result<Self, Self::Err> {
        let xs: Vec<_> = ss.collect();
        match &xs[..] {
            [ds, os] => {
                let f1 = ds.parse().map_err(ScaleError::FloatError)?;
                let f2 = os.parse().map_err(ScaleError::FloatError)?;
                match (f1, f2) {
                    (0.0, 0.0) => Ok(Scale::Linear),
                    (decades, offset) => {
                        Scale::try_new_log(decades, offset).map_err(ScaleError::LogRange)
                    }
                }
            }
            _ => Err(ScaleError::WrongFormat),
        }
    }
}

impl fmt::Display for Scale {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Scale::Log(x) => x.fmt(f),
            Scale::Linear => write!(f, "0,0"),
        }
    }
}

#[derive(Debug, Error)]
pub enum ScaleError {
    #[error("{0}")]
    FloatError(ParseFloatError),
    #[error("{0}")]
    LogRange(LogRangeError),
    #[error("must be like 'f1,f2'")]
    WrongFormat,
}

#[derive(Debug, Error)]
#[error("decades/offset must both be positive, got '{decades},{offset}'")]
pub struct LogRangeError {
    decades: f32,
    offset: f32,
}

impl LogRangeError {
    /// Try to 'fix' log scales which are 'X,0' where X is positive.
    ///
    /// The 'recommended' way to fix these is to make the 0 and 1, which is
    /// what this does. This is a heuristic hack to get some files to work
    /// which didn't write $PnE correctly.
    pub(crate) fn try_fix_offset(self) -> Result<LogScale, Self> {
        if self.offset == 0.0
            && let Ok(decades) = PositiveFloat::try_from(self.decades)
        {
            return Ok(LogScale {
                decades,
                offset: PositiveFloat::one(),
            });
        }
        Err(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;

    #[test]
    fn test_scale() {
        assert_from_to_str::<Scale>("0,0");
        assert_from_to_str::<Scale>("4.5,0.01");
    }

    #[test]
    fn test_scale_invalid() {
        assert!("4.5,0".parse::<Scale>().is_err());
    }
}

#[cfg(feature = "python")]
mod python {
    use super::{LogRangeError, Scale};
    use crate::python::macros::impl_value_err;

    use pyo3::prelude::*;
    use pyo3::types::PyTuple;
    use pyo3::IntoPyObjectExt;

    // $PnE (2.0) as either () or (f32, f32) tuples in python
    impl<'py> FromPyObject<'py> for Scale {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            if ob.is_instance_of::<PyTuple>() && ob.len()? == 0 {
                Ok(Self::Linear)
            } else {
                let (decades, offset): (f32, f32) = ob.extract()?;
                let ret = Self::try_new_log(decades, offset)?;
                Ok(ret)
            }
        }
    }

    impl<'py> IntoPyObject<'py> for Scale {
        type Target = PyAny;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            match self {
                Self::Linear => Ok(PyTuple::empty(py).into_any()),
                Self::Log(l) => (f32::from(l.decades), f32::from(l.offset)).into_bound_py_any(py),
            }
        }
    }

    impl_value_err!(LogRangeError);
}

use derive_more::{Display, From, Into};
use thiserror::Error;

use std::num::{NonZeroU8, NonZeroUsize, ParseIntError};
use std::str::FromStr;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {crate::python as py, fireflow_core_proc::DisplayAsPyErr, pyo3::prelude::*};

/// Width to use when parsing OTHER segments.
///
/// Must be an integer between 8 and 20.
#[derive(Clone, Copy, Into, PartialEq, Debug, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
#[into(NonZeroU8, u8, NonZeroUsize)]
pub struct OtherWidth(NonZeroU8);

impl Default for OtherWidth {
    fn default() -> Self {
        const N: NonZeroU8 = NonZeroU8::new(8).unwrap();
        Self(N)
    }
}

impl TryFrom<u8> for OtherWidth {
    type Error = OtherWidthError;

    fn try_from(x: u8) -> Result<Self, Self::Error> {
        if let Some(n) = NonZeroU8::new(x)
            && (MIN_OTHER_WIDTH..=MAX_CHARS).contains(&n)
        {
            Ok(Self(n))
        } else {
            Err(OtherWidthError(x))
        }
    }
}

impl FromStr for OtherWidth {
    type Err = ParseOtherWidthError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let x = s.parse::<u8>()?;
        Ok(Self::try_from(x)?)
    }
}

/// Error when parsing [`OtherWidth`] from [`String`]
#[derive(From, Debug, Display, Error, PartialEq, Clone)]
pub enum ParseOtherWidthError {
    Int(ParseIntError),
    Width(OtherWidthError),
}

/// Error when creating [`OtherWidth`] for configuration struct
#[derive(Debug, Error, PartialEq, Clone)]
#[error("OTHER width should be integer b/t {MIN_OTHER_WIDTH} and {MAX_CHARS}, got {0}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConfigError))]
pub struct OtherWidthError(u8);

// TODO this is awkward to include here since it also applies to characters in
// general for PnB
pub const MAX_CHARS: NonZeroU8 = NonZeroU8::new(20).unwrap();

pub const MIN_OTHER_WIDTH: NonZeroU8 = NonZeroU8::new(8).unwrap();

#[cfg(feature = "python")]
mod python {
    use super::OtherWidth;

    use pyo3::prelude::*;

    impl<'py> FromPyObject<'_, 'py> for OtherWidth {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            let x: u8 = obj.extract()?;
            let y = x.try_into()?;
            Ok(y)
        }
    }
}

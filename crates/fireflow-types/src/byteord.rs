use derive_more::{AsRef, Display};
use nonempty_collections::NEVec;
use thiserror::Error;

use std::{
    num::{NonZeroU8, ParseIntError},
    str::FromStr,
};

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {crate::python as py, fireflow_core_proc::DisplayAsPyErr};

/// The order of bytes in a data type.
///
/// Corresponds to the value of $BYTEORD in FCS 2.0 and 3.0.
///
/// This is only needed for the user-facing configuration. This value has
/// other types elsewhere for different purposes internally.
#[derive(Clone, AsRef)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct ConfigByteOrd(NEVec<NonZeroU8>);

impl AsRef<[NonZeroU8]> for ConfigByteOrd {
    fn as_ref(&self) -> &[NonZeroU8] {
        self.0.as_ref()
    }
}

impl FromStr for ConfigByteOrd {
    type Err = ParseNewConfigByteOrdError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let xs = s
            .split(',')
            .map(str::parse::<NonZeroU8>)
            .collect::<Result<Vec<_>, _>>()
            .map_err(ParseNewConfigByteOrdError::Digit)?;
        Self::try_from_iter(xs).map_err(ParseNewConfigByteOrdError::New)
    }
}

impl ConfigByteOrd {
    pub fn try_from_iter(
        xs: impl IntoIterator<Item = NonZeroU8>,
    ) -> Result<Self, NewConfigByteOrdError> {
        if let Some(ne) = NEVec::try_from_vec(xs.into_iter().collect()) {
            let mut flags = [false; 8];
            let n = ne.len().get();
            if n > 8 {
                return Err(NewConfigByteOrdError::TooLong(n));
            }
            for i in &ne {
                flags[usize::from(i.get()) - 1] = true;
            }
            if !flags.iter().take(n).all(|x| *x) {
                return Err(NewConfigByteOrdError::NonUnique);
            }
            Ok(Self(ne))
        } else {
            Err(NewConfigByteOrdError::Empty)
        }
    }
}

/// Error when making new [`ConfigByteOrd`]
#[derive(Debug, Error, PartialEq, Eq, Clone, Copy)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConfigError))]
pub enum NewConfigByteOrdError {
    #[error("byte order cannot be empty")]
    Empty,
    #[error("byte order must be 1-8 integers long, got {0}")]
    TooLong(usize),
    #[error("byte order must have all integers 1-<len> exactly once")]
    NonUnique,
}

/// Error when parsing [`ByteOrd2_0`] from string
#[derive(Debug, Display, Error, PartialEq, Eq, Clone)]
pub enum ParseNewConfigByteOrdError {
    Digit(ParseIntError),
    New(NewConfigByteOrdError),
}

#[cfg(feature = "python")]
mod python {
    use super::ConfigByteOrd;

    use pyo3::prelude::*;

    use std::num::NonZeroU8;

    impl<'py> FromPyObject<'_, 'py> for ConfigByteOrd {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            let xs: Vec<NonZeroU8> = obj.extract()?;
            let ret = Self::try_from_iter(xs)?;
            Ok(ret)
        }
    }

    impl<'py> IntoPyObject<'py> for ConfigByteOrd {
        type Target = PyAny;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            Vec::from(self.0).into_pyobject(py)
        }
    }
}

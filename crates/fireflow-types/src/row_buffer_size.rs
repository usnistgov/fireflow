use derive_more::{Display, Into};
use thiserror::Error;

use std::str::FromStr;

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{DisplayAsPyErr, TryFromPyObject},
    pyo3::prelude::*,
};

#[cfg(feature = "serde")]
use serde::Serialize;

/// The size of the row buffer used to read DATA.
///
/// The minimum size is 4k.
#[derive(Clone, Copy, Into, Display)]
#[cfg_attr(feature = "python", derive(IntoPyObject, TryFromPyObject))]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct RowBufferSize(usize);

impl Default for RowBufferSize {
    fn default() -> Self {
        Self(DEFAULT_ROW_BUFFER_SIZE)
    }
}

/// Error when making new [`RowBufferSize`].
#[derive(Error, Debug)]
#[error("Row buffer size must be greater than {MIN_ROW_BUFFER_SIZE} bytes")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ConfigError))]
pub struct RowBufferSizeError;

impl FromStr for RowBufferSize {
    type Err = RowBufferSizeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_from(s.parse::<usize>().map_err(|_| RowBufferSizeError)?)
    }
}

impl TryFrom<usize> for RowBufferSize {
    type Error = RowBufferSizeError;
    fn try_from(value: usize) -> Result<Self, Self::Error> {
        if value < MIN_ROW_BUFFER_SIZE {
            Err(RowBufferSizeError)
        } else {
            Ok(Self(value))
        }
    }
}

impl RowBufferSize {
    #[must_use]
    pub const fn inner(&self) -> usize {
        self.0
    }
}

const MIN_ROW_BUFFER_SIZE: usize = 4096;

/// The default size of the row buffer.
///
/// Chosen as 90% of the most common L1D cache size (which is 32k).
pub const DEFAULT_ROW_BUFFER_SIZE: usize = 28000;

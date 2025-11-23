use crate::text::index::MeasIndex;

use derive_more::{AsRef, Display, Into};
use std::str::FromStr;
use thiserror::Error;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{DisplayAsPyErr, FromPyString},
    pyo3::prelude::*,
};

/// The value for the $PnN key (all versions).
///
/// This cannot contain commas or be empty.
#[derive(Clone, Eq, PartialEq, Hash, Debug, AsRef, Display, Into)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromPyString))]
#[as_ref(str)]
pub struct Shortname(String);

impl Shortname {
    pub fn new_unchecked<T: AsRef<str>>(s: T) -> Self {
        Self(s.as_ref().to_owned())
    }
}

impl FromStr for Shortname {
    type Err = ShortnameError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.contains(',') {
            Err(ShortnameError::Commas(s.into()))
        } else if s.is_empty() {
            Err(ShortnameError::Empty)
        } else {
            Ok(Self(s.into()))
        }
    }
}

impl From<MeasIndex> for Shortname {
    fn from(value: MeasIndex) -> Self {
        Self(format!("P{value}"))
    }
}

/// Error when parsing $PnN
#[derive(Debug, Error)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr), pyerr(PyValueError))]
pub enum ShortnameError {
    #[error("commas are not allowed in name '{0}'")]
    Commas(String),
    #[error("name cannot be empty")]
    Empty,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn str_to_shortname() {
        assert!("Thunderfist Chronicles".parse::<Shortname>().is_ok());
        assert!("Thunderfist,Chronicles".parse::<Shortname>().is_err());
    }
}

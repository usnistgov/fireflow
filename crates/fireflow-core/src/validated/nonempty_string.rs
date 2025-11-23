use derive_more::{Display, Into};
use std::str::FromStr;
use thiserror::Error;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{DisplayAsPyErr, FromPyString},
    pyo3::prelude::*,
};

/// A string which can never be empty
///
/// This is useful for required keywords which are strings. For optional
/// strings, empty string means the value is missing, so required keys simply
/// forbid empty strings.
#[derive(Clone, PartialEq, Eq, Default, Display, Into, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromPyString))]
pub struct NonEmptyString(String);

impl FromStr for NonEmptyString {
    type Err = NonEmptyStringError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            Err(NonEmptyStringError)
        } else {
            Ok(Self(s.to_owned()))
        }
    }
}

/// Error when string is empty which is not supposed to be empty
#[derive(Error, Debug)]
#[error("string cannot be empty")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ParseKeyError))]
pub struct NonEmptyStringError;

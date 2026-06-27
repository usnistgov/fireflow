use std::hash::{Hash, Hasher};

use derive_more::{AsRef, Display};
use regex::Regex;
use thiserror::Error;

use std::str::FromStr;

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{DisplayAsPyErr, FromPyString, IntoPyString},
    fireflow_types::python as py,
};

/// A regex which ignores case when matching
#[derive(Clone, AsRef, Display, Debug)]
#[cfg_attr(feature = "python", derive(IntoPyString, FromPyString))]
pub struct CaseInsRegex(Regex);

impl PartialEq<Self> for CaseInsRegex {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_str() == other.0.as_str()
    }
}

impl Eq for CaseInsRegex {}

impl Hash for CaseInsRegex {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.0.as_str().hash(state);
    }
}

/// Error when parsing [`CaseInsRegex`] from [`String`].
#[derive(Debug, Error, PartialEq, Clone)]
#[error("error when making case-insensitive regular expression: {0}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConfigError))]
pub struct CaseInsRegexError(regex::Error);

impl FromStr for CaseInsRegex {
    type Err = CaseInsRegexError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        regex::RegexBuilder::new(s)
            .case_insensitive(true)
            .build()
            .map(Self)
            .map_err(CaseInsRegexError)
    }
}

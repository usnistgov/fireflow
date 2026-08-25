use std::hash::{Hash, Hasher};

use derive_more::{AsRef, Display};
use regex::Regex;
use thiserror::Error;

use std::str::FromStr;

#[cfg(feature = "python")]
use {
    crate::python as py,
    fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr, FromPyString, IntoPyString},
    pyo3::prelude::*,
};

/// A regex which ignores case when matching
#[derive(Clone, AsRef, Display, Debug)]
#[cfg_attr(feature = "python", derive(IntoPyString, FromPyString))]
pub struct CaseInsRegex(Regex);

/// Either a literal string or regexp.
///
/// This exists for performance and ergononic reasons; if the goal is simply to
/// match lots of strings literally, it is faster and easier to use a hash
/// table, otherwise we need to search linearly through an array of patterns.
#[derive(Clone, PartialEq, Eq, Hash, Display, Debug)]
pub enum LiteralOrPattern<L> {
    #[display("{_0}")]
    Literal(L),
    #[display("{PATTERN_DELIMITER}{_0}{PATTERN_DELIMITER}")]
    Pattern(CaseInsRegex),
}

/// Error when parsing [`CaseInsRegex`] from [`String`].
#[derive(Debug, Error, PartialEq, Clone)]
#[error("error when making case-insensitive regular expression: {0}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConfigError))]
pub struct CaseInsRegexError(regex::Error);

/// Error when parsing literal or pattern string.
#[derive(Debug, Display, PartialEq, Error, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(E: Into<PyErr>))]
pub enum LiteralOrPatternError<E> {
    Regexp(CaseInsRegexError),
    Literal(E),
}

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

impl<L: FromStr> FromStr for LiteralOrPattern<L> {
    type Err = LiteralOrPatternError<<L as FromStr>::Err>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(inner) = s
            .strip_prefix(PATTERN_DELIMITER)
            .and_then(|x| x.strip_suffix(PATTERN_DELIMITER))
        {
            let ret = inner
                .parse::<CaseInsRegex>()
                .map_err(LiteralOrPatternError::Regexp)?;
            Ok(Self::Pattern(ret))
        } else {
            let ret = s.parse::<L>().map_err(LiteralOrPatternError::Literal)?;
            Ok(Self::Literal(ret))
        }
    }
}

pub const PATTERN_DELIMITER: char = '/';

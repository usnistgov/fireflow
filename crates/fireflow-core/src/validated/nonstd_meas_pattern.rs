use crate::text::index::IndexFromOne;
use crate::validated::case_ins_regex::CaseInsRegex;

use derive_more::{AsRef, Display};
use derive_new::new;
use regex::Regex;
use std::str::FromStr;
use thiserror::Error;

#[cfg(feature = "python")]
use fireflow_core_proc::{DisplayAsPyErr, FromPyString};

/// A [`String`] that matches part of a [`crate::validated::keys::NonStdKey`].
///
/// This will have exactly one `"%n"` and not start with a `"$"`. The `"%n"`
/// will be replaced by the measurement index which will be used to match
/// keywords.
#[derive(Clone, AsRef, Display)]
#[as_ref(str)]
#[cfg_attr(feature = "python", derive(FromPyString))]
pub struct NonStdMeasPattern(String);

impl Default for NonStdMeasPattern {
    fn default() -> Self {
        // ASSUME this wouldn't have caused an error if parsed directly from
        // a string
        Self("^P%n".into())
    }
}

/// A regular expression which matches a [`crate::validated::keys::NonStdKey`].
///
/// This must be derived from [`NonStdMeasPattern`].
#[derive(AsRef)]
#[as_ref(Regex)]
pub(crate) struct NonStdMeasRegex(CaseInsRegex);

impl FromStr for NonStdMeasPattern {
    type Err = NonStdMeasPatternError;

    fn from_str(s: &str) -> Result<Self, NonStdMeasPatternError> {
        if s.match_indices("%n").count() == 1 {
            Ok(Self(s.into()))
        } else {
            Err(NonStdMeasPatternError(s.into()))
        }
    }
}

impl NonStdMeasPattern {
    pub(crate) fn apply_index(
        &self,
        n: impl Into<IndexFromOne> + Clone,
    ) -> Result<NonStdMeasRegex, NonStdMeasRegexError> {
        self.0
            .replace("%n", n.clone().into().to_string().as_str())
            .as_str()
            .parse::<CaseInsRegex>()
            .map_err(|error| NonStdMeasRegexError::new(error, n))
            .map(NonStdMeasRegex)
    }
}

/// Error when parsing [`NonStdMeasPattern`] from string for configuration
#[derive(Error, Debug)]
#[error("non standard measurement pattern should have one '%n', found '{0}'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ConfigError))]
pub struct NonStdMeasPatternError(String);

/// Error when converting [`NonStdMeasPattern`] to regular expression
#[derive(Error, Debug, new)]
#[error("regexp error for measurement {index}: {error}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ConfigError))]
pub struct NonStdMeasRegexError {
    error: regex::Error,
    #[new(into)]
    index: IndexFromOne,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fromstr_nonstd_meas_pattern() {
        assert!("".parse::<NonStdMeasPattern>().is_err());
        assert!("n".parse::<NonStdMeasPattern>().is_err());
        assert!("%n".parse::<NonStdMeasPattern>().is_ok());
    }
}

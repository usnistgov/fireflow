use crate::text::index::IndexFromOne;

use fireflow_types::config::{NON_STD_MEAS_INDEX_PAT, NON_STD_MEAS_PAT_DEFAULT};

use derive_more::{AsRef, Display};
use derive_new::new;
use std::{convert::Infallible, str::FromStr};
use thiserror::Error;

use super::keys::{LiteralOrPattern, LiteralOrPatternError, NonStdKey};

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{DisplayAsPyErr, FromPyString, IntoPyString},
    fireflow_types::python as py,
};

/// A [`String`] that matches part of a [`crate::validated::keys::NonStdKey`].
///
/// If '/' is the first and last character, interpret as a regular expression,
/// otherwise a literal string which will be used as an exact prefix match.
///
/// This will have exactly one `"%n"`. The `"%n"` will be replaced by the
/// measurement index which will be used to match keywords.
#[derive(Clone, AsRef, Display)]
#[as_ref(str)]
#[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
pub struct NonStdMeasPattern(String);

impl Default for NonStdMeasPattern {
    fn default() -> Self {
        // ASSUME this wouldn't have caused an error if parsed directly from
        // a string
        Self(NON_STD_MEAS_PAT_DEFAULT.into())
    }
}

/// A regular expression which matches a [`crate::validated::keys::NonStdKey`].
///
/// This must be derived from [`NonStdMeasPattern`].
pub(crate) struct NonStdMeasRegex(LiteralOrPattern<String>);

impl NonStdMeasRegex {
    #[inline]
    pub(crate) fn is_match(&self, k: &NonStdKey) -> bool {
        match &self.0 {
            // ASSUME key is only ASCII and case-insensitive.
            LiteralOrPattern::Literal(prefix) => {
                let s: &str = k.as_ref();
                let bs = s.as_bytes();
                let kn = bs.len();
                if prefix.len() > kn {
                    return false;
                }
                bs[..kn].eq_ignore_ascii_case(prefix.as_bytes())
            }
            LiteralOrPattern::Pattern(pat) => pat.as_ref().is_match(k.as_ref()),
        }
    }
}

impl FromStr for NonStdMeasPattern {
    type Err = NonStdMeasPatternError;

    fn from_str(s: &str) -> Result<Self, NonStdMeasPatternError> {
        if s.match_indices(NON_STD_MEAS_INDEX_PAT).count() == 1 {
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
            .replace(
                NON_STD_MEAS_INDEX_PAT,
                n.clone().into().to_string().as_str(),
            )
            .as_str()
            .parse::<LiteralOrPattern<String>>()
            .map_err(|error| NonStdMeasRegexError::new(error, n))
            .map(NonStdMeasRegex)
    }
}

/// Error when parsing [`NonStdMeasPattern`] from string for configuration
#[derive(Error, Debug)]
#[error(
    "non standard measurement pattern should have one \
     '{NON_STD_MEAS_INDEX_PAT}', found '{0}'"
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConfigError))]
pub struct NonStdMeasPatternError(String);

/// Error when converting [`NonStdMeasPattern`] to regular expression
#[derive(Error, Debug, new)]
#[error("regexp error for measurement {index}: {error}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConfigError))]
pub struct NonStdMeasRegexError {
    error: LiteralOrPatternError<Infallible>,
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

use crate::text::index::IndexFromOne;
use crate::validated::case_ins_regex::CaseInsRegex;
use crate::validated::keys::NonStdKey;

use fireflow_types::config::{NON_STD_MEAS_INDEX_PAT, NON_STD_MEAS_PAT_DEFAULT, PATTERN_DELIMITER};

use derive_more::{AsRef, Display, From};
use nonempty_collections::{IntoIteratorExt as _, NonEmptyIterator as _};
use thiserror::Error;

use std::str::FromStr;

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

pub(crate) enum CompiledNonStdMeasPattern {
    Literal(LiteralNonStdMeasPattern),
    Regex(RegexNonStdMeasPattern),
}

/// Matches <prefix><number><suffix> case-insensitively.
///
/// Assume prefix and suffix are in lowercase ASCII and <number> starts at 1.
pub(crate) struct LiteralNonStdMeasPattern {
    prefix: Vec<u8>,
    suffix: Vec<u8>,
}

pub(crate) struct RegexNonStdMeasPattern(CaseInsRegex);

impl LiteralNonStdMeasPattern {
    pub(crate) fn get_index(&self, k: &NonStdKey) -> Option<IndexFromOne> {
        let s: &str = k.as_ref();
        debug_assert!(s.is_ascii(), "key is not ASCII");
        // ASSUME prefix and suffix were converted to ASCII lowercase
        let bs = s.as_bytes();
        let prefix_len = self.prefix.len();
        // Check if prefix matches
        if prefix_len > bs.len() {
            return None;
        }
        if bs[..prefix_len].to_ascii_lowercase() != self.prefix[..] {
            return None;
        }
        // Try to extract a number starting at 1.
        let is_digit = |x: &u8| (48..=57).contains(x);
        let is_nonzero = |x: &u8| (49..=57).contains(x);
        let ne = bs[prefix_len..].try_into_nonempty_iter()?;
        let (x0, xs) = ne.next();
        if !is_nonzero(x0) {
            return None;
        }
        let digit_begin = prefix_len;
        let suffix_begin = prefix_len + 1 + xs.take_while(|&x| is_digit(x)).count();
        #[allow(clippy::string_slice)]
        let i = s[digit_begin..suffix_begin].parse::<IndexFromOne>().ok()?;
        // Check if suffix matches; does not need to be complete since this
        // entire pattern is a prefix.
        let suffix_end = self.suffix.len() + suffix_begin;
        if suffix_begin > bs.len() {
            return None;
        }
        if bs[suffix_begin..suffix_end].to_ascii_lowercase() != self.suffix[..] {
            return None;
        }
        // Good job, you win
        Some(i)
    }
}

impl RegexNonStdMeasPattern {
    pub(crate) fn get_index(&self, k: &NonStdKey) -> Option<IndexFromOne> {
        let r = self.0.as_ref();
        let cap = r.captures(k.as_ref())?;
        let d = cap.get(1)?.as_str();
        let i = d
            .parse::<IndexFromOne>()
            .expect("match should only include digits");
        Some(i)
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
    pub(crate) fn compile(&self) -> Result<CompiledNonStdMeasPattern, NonStdMeasRegexError> {
        let s = self.0.as_str();
        if let Some(inner) = s
            .strip_prefix(PATTERN_DELIMITER)
            .and_then(|x| x.strip_suffix(PATTERN_DELIMITER))
        {
            let pat = inner.replace(NON_STD_MEAS_INDEX_PAT, "([1-9][0-9]*)");
            let ret = RegexNonStdMeasPattern(pat.parse::<CaseInsRegex>()?);
            Ok(CompiledNonStdMeasPattern::Regex(ret))
        } else {
            let mut it = s.split("%n");
            let mut go = || {
                it.by_ref()
                    .next()
                    .expect("literal should have one %n")
                    .as_bytes()
                    .to_ascii_lowercase()
            };
            let prefix = go();
            let suffix = go();
            debug_assert!(it.next().is_none(), "literal should have one %n");
            let ret = LiteralNonStdMeasPattern { prefix, suffix };
            Ok(CompiledNonStdMeasPattern::Literal(ret))
        }
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
#[derive(Error, Debug, From)]
#[error("error when making non-standard measurement pattern: {0}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConfigError))]
pub struct NonStdMeasRegexError(regex::Error);

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

use crate::text::index::IndexFromOne;
use crate::validated::case_ins_regex::CaseInsRegex;
use crate::validated::keys::NonStdKey;

use fireflow_types::config::{NON_STD_MEAS_INDEX_PAT, NON_STD_MEAS_PAT_DEFAULT, PATTERN_DELIMITER};

use derive_more::{AsRef, Display, From};
use thiserror::Error;

use std::str::FromStr;

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr, FromPyString, IntoPyString},
    fireflow_types::python as py,
};

/// A [`String`] that matches part of a [`crate::validated::keys::NonStdKey`].
///
/// If '/' is the first and last character, interpret as a regular expression,
/// otherwise a literal string which will be used as an exact prefix match.
///
/// This will have exactly one `"%n"`. The `"%n"` will be replaced by the
/// measurement index which will be used to match keywords.
#[derive(Clone, AsRef, Display, Debug, PartialEq)]
#[display("{}", self.original)]
#[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
pub struct NonStdMeasPattern {
    #[as_ref(str)]
    original: String,
    inner: CompiledNonStdMeasPattern,
}

impl Default for NonStdMeasPattern {
    fn default() -> Self {
        // ASSUME this wouldn't have caused an error if parsed directly from
        // a string
        NON_STD_MEAS_PAT_DEFAULT
            .parse()
            .expect("default should be tested")
    }
}

#[derive(Clone, Debug, PartialEq)]
enum CompiledNonStdMeasPattern {
    Literal(LiteralNonStdMeasPattern),
    Regex(RegexNonStdMeasPattern),
}

/// Matches <prefix><number><suffix> case-insensitively.
///
/// Assume prefix and suffix are in lowercase ASCII and <number> starts at 1.
#[derive(Clone, PartialEq, Debug)]
struct LiteralNonStdMeasPattern {
    prefix: Vec<u8>,
    suffix: Vec<u8>,
}

#[derive(Clone, PartialEq, Debug)]
struct RegexNonStdMeasPattern(CaseInsRegex);

impl NonStdMeasPattern {
    pub(crate) fn get_index(&self, k: &NonStdKey) -> Option<IndexFromOne> {
        match &self.inner {
            CompiledNonStdMeasPattern::Literal(p) => p.get_index(k),
            CompiledNonStdMeasPattern::Regex(p) => p.get_index(k),
        }
    }
}

impl LiteralNonStdMeasPattern {
    pub(crate) fn get_index(&self, k: &NonStdKey) -> Option<IndexFromOne> {
        let s: &str = k.as_ref();
        assert!(s.is_ascii(), "key is not ASCII");
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
        let digit_begin = prefix_len;
        let suffix_begin =
            prefix_len + bs[prefix_len..].iter().take_while(|&x| is_digit(x)).count();
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

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.match_indices(NON_STD_MEAS_INDEX_PAT).count() != 1 {
            return Err(NonStdMeasPatternTokenError(s.into()).into());
        }

        let inner = if let Some(inner) = s
            .strip_prefix(PATTERN_DELIMITER)
            .and_then(|x| x.strip_suffix(PATTERN_DELIMITER))
        {
            let pat = inner.replace(NON_STD_MEAS_INDEX_PAT, "(0*[1-9][0-9]*)");
            let ci_pat = pat.parse::<CaseInsRegex>().map_err(NonStdMeasRegexError)?;
            CompiledNonStdMeasPattern::Regex(RegexNonStdMeasPattern(ci_pat))
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
            assert!(it.next().is_none(), "literal should have one %n");
            let ret = LiteralNonStdMeasPattern { prefix, suffix };
            CompiledNonStdMeasPattern::Literal(ret)
        };
        let original = s.to_owned();
        Ok(Self { original, inner })
    }
}

/// Error when parsing [`NonStdMeasPattern`] from string for configuration
#[derive(Error, Debug, From, Display, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum NonStdMeasPatternError {
    Token(NonStdMeasPatternTokenError),
    Regex(NonStdMeasRegexError),
}

/// Error when parsing [`NonStdMeasPattern`] from string for configuration
#[derive(Error, Debug, PartialEq, Clone)]
#[error(
    "non standard measurement pattern should have one \
     '{NON_STD_MEAS_INDEX_PAT}', found '{0}'"
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConfigError))]
pub struct NonStdMeasPatternTokenError(String);

/// Error when converting [`NonStdMeasPattern`] to regular expression
#[derive(Error, Debug, From, PartialEq, Clone)]
#[error("error when making non-standard measurement pattern: {0}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConfigError))]
pub struct NonStdMeasRegexError(regex::Error);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::text::index::IndexFromOne;

    use assert_matches::assert_matches;
    use itertools::Itertools as _;
    use proptest::prelude::*;

    use std::iter::repeat_n;

    proptest! {
        #[test]
        fn fromstr_nonstd_meas_pattern_literal(s in "[^/][^%]*%n[^%]*[^/]") {
            assert!(matches!(
                s.parse::<NonStdMeasPattern>().map(|x| x.inner),
                Ok(CompiledNonStdMeasPattern::Literal(_))
            ));
        }
    }

    #[test]
    fn fromstr_nonstd_meas_pattern_pattern_minimal() {
        let s = "/%n/";
        assert!(matches!(
            s.parse::<NonStdMeasPattern>().map(|x| x.inner),
            Ok(CompiledNonStdMeasPattern::Regex(_))
        ));
    }

    #[test]
    fn fromstr_nonstd_meas_pattern_pattern_super_deluxe() {
        let s = "/(#|BD\\$|#NC)?P0*%n/";
        assert!(matches!(
            s.parse::<NonStdMeasPattern>().map(|x| x.inner),
            Ok(CompiledNonStdMeasPattern::Regex(_))
        ));
    }

    #[test]
    fn fromstr_nonstd_meas_pattern_invalid() {
        assert_matches!(
            "".parse::<NonStdMeasPattern>(),
            Err(NonStdMeasPatternError::Token(_))
        );
        assert_matches!(
            "n".parse::<NonStdMeasPattern>(),
            Err(NonStdMeasPatternError::Token(_))
        );
        assert_matches!(
            "/(%n/".parse::<NonStdMeasPattern>(),
            Err(NonStdMeasPatternError::Regex(_))
        );
    }

    proptest! {
        #[test]
        fn nonstd_meas_pattern_literal_match(
            (ns_key, index) in (0_usize..5, 0_usize..1000, "[[:alpha:][:punct:]]")
                .prop_map(|(n_zeros, index, rest)| {
                    let index1 = IndexFromOne::from(index);
                    let zeros = repeat_n("0", n_zeros).join("");
                    let s = format!("P{zeros}{index1}{rest}");
                    (s.parse::<NonStdKey>().unwrap(), index1)
                })
        ) {
            let s: NonStdMeasPattern = "P%n".parse().unwrap();
            assert_eq!(s.get_index(&ns_key), Some(index));
        }
    }

    #[test]
    fn nonstd_meas_pattern_literal_zero() {
        let s: NonStdMeasPattern = "P%nXXX".parse().unwrap();
        let k = "P0XXX".parse::<NonStdKey>().unwrap();
        assert_eq!(s.get_index(&k), None);
    }

    #[test]
    fn nonstd_meas_pattern_literal_blank() {
        let s: NonStdMeasPattern = "P%nXXX".parse().unwrap();
        let k = "P0".parse::<NonStdKey>().unwrap();
        assert_eq!(s.get_index(&k), None);
    }

    proptest! {
        #[test]
        fn nonstd_meas_pattern_regexp_match(
            (ns_key, index) in (0_usize..5, 0_usize..1000, "[[:alpha:][:punct:]]")
                .prop_map(|(n_zeros, index, rest)| {
                    let index1 = IndexFromOne::from(index);
                    let zeros = repeat_n("0", n_zeros).join("");
                    let s = format!("P{zeros}{index1}{rest}");
                    (s.parse::<NonStdKey>().unwrap(), index1)
                })
        ) {
            let s: NonStdMeasPattern = "/P%n/".parse().unwrap();
            assert_eq!(s.get_index(&ns_key), Some(index));
        }
    }

    #[test]
    fn nonstd_meas_pattern_regexp_zero() {
        let s: NonStdMeasPattern = "/P%nXXX/".parse().unwrap();
        let k = "P0XXX".parse::<NonStdKey>().unwrap();
        assert_eq!(s.get_index(&k), None);
    }

    #[test]
    fn nonstd_meas_pattern_regexp_blank() {
        let s: NonStdMeasPattern = "/P%nXXX/".parse().unwrap();
        let k = "P0".parse::<NonStdKey>().unwrap();
        assert_eq!(s.get_index(&k), None);
    }
}

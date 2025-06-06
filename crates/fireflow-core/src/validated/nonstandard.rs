use crate::macros::{newtype_asref, newtype_disp};
use crate::text::index::IndexFromOne;

use regex::Regex;
use serde::Serialize;
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

/// A String that matches a non-standard metadata keyword
///
/// This shall not start with '$'.
#[derive(Clone, PartialEq, Eq, Hash, Serialize)]
pub struct NonStdKey(String);

pub type NonStdPairs = Vec<(NonStdKey, String)>;
pub type NonStdKeywords = HashMap<NonStdKey, String>;

/// A String that matches part of a non-standard measurement key.
///
/// This will have exactly one '%n' and not start with a '$'. The
/// '%n' will be replaced by the measurement index which will be used
/// to match keywords.
#[derive(Clone)]
pub struct NonStdMeasPattern(String);

impl FromStr for NonStdKey {
    type Err = NonStdKeyError;

    fn from_str(s: &str) -> Result<Self, NonStdKeyError> {
        if s.starts_with("$") {
            Err(NonStdKeyError(s.to_string()))
        } else {
            Ok(NonStdKey(s.to_string()))
        }
    }
}

impl NonStdKey {
    pub fn from_unchecked(s: &str) -> Self {
        Self(s.into())
    }

    pub fn into_unchecked(s: String) -> Self {
        Self(s)
    }
}

pub struct NonStdMeasRegex(Regex);

impl NonStdMeasRegex {
    pub fn try_match(&self, s: &str) -> Option<NonStdKey> {
        if self.0.is_match(s) {
            Some(NonStdKey(s.to_string()))
        } else {
            None
        }
    }

    pub fn is_match(&self, s: &str) -> bool {
        self.0.is_match(s)
    }
}

impl FromStr for NonStdMeasPattern {
    type Err = NonStdMeasPatternError;

    fn from_str(s: &str) -> Result<Self, NonStdMeasPatternError> {
        if s.starts_with("$") || s.match_indices("%n").count() != 1 {
            Err(NonStdMeasPatternError(s.to_string()))
        } else {
            Ok(NonStdMeasPattern(s.to_string()))
        }
    }
}

impl NonStdMeasPattern {
    // pub fn with_par(&self, par: Par) -> MultiResult<Vec<NonStdMeasRegex, NonStdMeasRegexError>> {
    //     (0..par.0).map(|n| self.from_index(n.into())).gather()
    // }

    pub fn from_index(&self, n: IndexFromOne) -> Result<NonStdMeasRegex, NonStdMeasRegexError> {
        let pattern = self.0.replace("%n", n.to_string().as_str());
        Regex::new(pattern.as_str())
            .map_err(|_| NonStdMeasRegexError { pattern, index: n })
            .map(NonStdMeasRegex)
    }
}

pub struct NonStdKeyError(String);

impl fmt::Display for NonStdKeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "Non standard pattern must not start with '$', found '{}'",
            self.0
        )
    }
}

pub struct NonStdMeasKeyError(String);

impl fmt::Display for NonStdMeasKeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "Non standard measurement pattern must not \
             start with '$' and should have one '%n', found '{}'",
            self.0
        )
    }
}

#[derive(Debug)]
pub struct NonStdMeasPatternError(String);

impl fmt::Display for NonStdMeasPatternError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "Non standard measurement pattern must not \
             start with '$' and should have one '%n', found '{}'",
            self.0
        )
    }
}

pub struct NonStdMeasRegexError {
    pattern: String,
    index: IndexFromOne,
}

impl fmt::Display for NonStdMeasRegexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "Could not make regexp using pattern '{}' for measurement {}",
            self.pattern, self.index,
        )
    }
}

newtype_disp!(NonStdKey);
newtype_disp!(NonStdMeasPattern);

newtype_asref!(NonStdKey, str);
newtype_asref!(NonStdMeasPattern, str);

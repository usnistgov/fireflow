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

pub type NonStdKeywords = HashMap<NonStdKey, String>;

impl AsRef<str> for NonStdKey {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

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

/// A String that matches a non-standard measurement keyword.
///
/// This will have exactly one '%n' and not start with a '$'. The
/// '%n' will be replaced by the measurement index which will be used
/// to match keywords.
///
/// For example 'P%nFOO' for index 420 would be 'P420FOO'.
#[derive(Clone)]
pub struct NonStdMeasKey(String);

impl AsRef<str> for NonStdMeasKey {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

impl FromStr for NonStdMeasKey {
    type Err = NonStdMeasKeyError;

    fn from_str(s: &str) -> Result<Self, NonStdMeasKeyError> {
        if s.starts_with("$") || s.match_indices("%n").count() != 1 {
            Err(NonStdMeasKeyError(s.to_string()))
        } else {
            Ok(NonStdMeasKey(s.to_string()))
        }
    }
}

impl NonStdMeasKey {
    pub fn from_index(&self, n: usize) -> NonStdKey {
        NonStdKey(self.0.replace("%n", n.to_string().as_str()))
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

/// A String that matches part of a non-standard measurement key.
///
/// This will have exactly one '%n' and not start with a '$'. The
/// '%n' will be replaced by the measurement index which will be used
/// to match keywords.
#[derive(Clone)]
pub struct NonStdMeasPattern(String);

impl AsRef<str> for NonStdMeasPattern {
    fn as_ref(&self) -> &str {
        self.0.as_str()
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
    pub fn from_index(&self, n: usize) -> Result<NonStdMeasRegex, NonStdMeasRegexError> {
        let pattern = self.0.replace("%n", n.to_string().as_str());
        Regex::new(pattern.as_str())
            .map_err(|_| NonStdMeasRegexError { pattern, index: n })
            .map(NonStdMeasRegex)
    }
}

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
    index: usize,
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

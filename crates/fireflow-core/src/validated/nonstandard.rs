use crate::macros::{newtype_asref, newtype_disp};

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

/// A String that matches a non-standard measurement keyword.
///
/// This will have exactly one '%n' and not start with a '$'. The
/// '%n' will be replaced by the measurement index which will be used
/// to match keywords.
///
/// For example 'P%nFOO' for index 420 would be 'P420FOO'.
#[derive(Clone)]
pub struct NonStdMeasKey(String);

/// A String that matches part of a non-standard measurement key.
///
/// This will have exactly one '%n' and not start with a '$'. The
/// '%n' will be replaced by the measurement index which will be used
/// to match keywords.
#[derive(Clone)]
pub struct NonStdMeasPattern(String);

/// Values that may be be used for an optional keyword when converting versions
///
/// For example, when converting 2.0 -> 3.1, the "$VOL" keyword needs to be
/// filled with something. This will allow the user to specify a default for
/// $VOL. Alternatively, the user can supply a keyword that will be used to
/// lookup the value from the nonstandard keyword list (presumably in this case
/// the key would be something like "VOL"). Both of these are optional. If
/// neither is supplied, this keyword will be left unfilled. Obviously this only
/// applies if the keyword is optional.
#[derive(Clone)]
pub struct DefaultOptional<T, K> {
    /// Value to be used as a default
    pub default: Option<T>,

    /// Key to use when looking in the nonstandard keyword hash table.
    ///
    /// This is assumed not to start with "$".
    pub key: Option<K>,
}

pub type DefaultMetaOptional<T> = DefaultOptional<T, NonStdKey>;
pub type DefaultMeasOptional<T> = DefaultOptional<T, NonStdMeasKey>;

/// Comp/spillover matrix that may be converted or for which a default may exist
///
/// This is similar to `DefaultOptional` in that it has a default and a key for
/// which the default may be found. However, a conversion from a different key
/// may be attempted before these.
#[derive(Clone)]
pub struct DefaultMatrix<T> {
    /// If true, try to convert from a different key.
    ///
    /// For now this applies to $COMP<->$SPILLOVER conversions.
    pub try_convert: bool,
    pub default: DefaultMetaOptional<T>,
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

impl<T, K> Default for DefaultOptional<T, K> {
    fn default() -> DefaultOptional<T, K> {
        DefaultOptional {
            default: None,
            key: None,
        }
    }
}

impl<T> DefaultMetaOptional<T> {
    pub fn new(default: Option<T>, key: Option<String>) -> Result<Self, NonStdKeyError> {
        Ok(DefaultOptional {
            default,
            key: key.map_or(Ok(None), |s| s.parse().map(Some))?,
        })
    }
}

impl<T> DefaultMeasOptional<T> {
    pub fn new(default: Option<T>, key: Option<String>) -> Result<Self, NonStdMeasKeyError> {
        Ok(DefaultOptional {
            default,
            key: key.map_or(Ok(None), |s| s.parse().map(Some))?,
        })
    }
}

impl<T> DefaultMatrix<T> {
    pub fn new(
        default: Option<T>,
        key: Option<String>,
        try_convert: bool,
    ) -> Result<Self, NonStdKeyError> {
        Ok(DefaultMatrix {
            try_convert,
            default: DefaultMetaOptional::new(default, key)?,
        })
    }
}

impl<T> Default for DefaultMatrix<T> {
    fn default() -> DefaultMatrix<T> {
        DefaultMatrix {
            try_convert: false,
            default: DefaultMetaOptional::default(),
        }
    }
}

newtype_disp!(NonStdKey);
newtype_disp!(NonStdMeasKey);
newtype_disp!(NonStdMeasPattern);

newtype_asref!(NonStdKey, str);
newtype_asref!(NonStdMeasKey, str);
newtype_asref!(NonStdMeasPattern, str);

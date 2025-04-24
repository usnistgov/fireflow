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
pub enum KwSetter<T, K> {
    /// Value to be used as a default
    Default(T),

    /// Key to use when looking in the nonstandard keyword hash table.
    ///
    /// This is assumed not to start with "$".
    Key(Option<K>),
}

pub type MetaKwSetter<T> = KwSetter<T, NonStdKey>;
pub type MeasKwSetter<T> = KwSetter<T, NonStdMeasKey>;

/// Comp/spillover matrix that may be converted or for which a default may exist
///
/// This is similar to `DefaultOptional` in that it has a default and a key for
/// which the default may be found. However, a conversion from a different key
/// may be attempted before these.
#[derive(Clone)]
pub struct MatrixSetter<T> {
    /// If true, try to convert from a different key.
    ///
    /// For now this applies to $COMP<->$SPILLOVER conversions.
    pub try_convert: bool,
    pub default: MetaKwSetter<T>,
}

/// The index for a measurement
// TODO this should start from 1?
#[derive(Clone, Copy)]
pub struct MeasIdx(usize);

impl From<usize> for MeasIdx {
    fn from(value: usize) -> Self {
        MeasIdx(value + 1)
    }
}

impl From<MeasIdx> for usize {
    fn from(value: MeasIdx) -> Self {
        value.0 - 1
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

impl NonStdKey {
    pub fn from_unchecked(s: &str) -> Self {
        Self(s.into())
    }
}

impl NonStdMeasKey {
    pub fn from_index(&self, n: MeasIdx) -> NonStdKey {
        NonStdKey(self.0.replace("%n", n.to_string().as_str()))
    }

    pub fn from_unchecked(s: &str) -> Self {
        Self(s.into())
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
    pub fn from_index(&self, n: MeasIdx) -> Result<NonStdMeasRegex, NonStdMeasRegexError> {
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
    index: MeasIdx,
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

impl<T, K> Default for KwSetter<T, K> {
    fn default() -> KwSetter<T, K> {
        KwSetter::Key(None)
    }
}

// impl<T> MetaKwSetter<T> {
//     pub fn new(default: Option<T>, key: Option<String>) -> Result<Self, NonStdKeyError> {
//         Ok(KwSetter {
//             default,
//             key: key.map_or(Ok(None), |s| s.parse().map(Some))?,
//         })
//     }

//     pub fn init_unchecked(key: &'static str) -> Self {
//         KwSetter {
//             default: None,
//             key: Some(NonStdKey(key.into())),
//         }
//     }
// }

// impl<T> MeasKwSetter<T> {
//     pub fn new(default: Option<T>, key: Option<String>) -> Result<Self, NonStdMeasKeyError> {
//         Ok(KwSetter {
//             default,
//             key: key.map_or(Ok(None), |s| s.parse().map(Some))?,
//         })
//     }

//     pub fn init_unchecked(suffix: &'static str, n: usize) -> Self {
//         KwSetter {
//             default: None,
//             key: Some(NonStdMeasKey(format!("P{n}{suffix}"))),
//         }
//     }
// }

// impl<T> MatrixSetter<T> {
//     pub fn new(
//         default: Option<T>,
//         key: Option<String>,
//         try_convert: bool,
//     ) -> Result<Self, NonStdKeyError> {
//         Ok(MatrixSetter {
//             try_convert,
//             default: MetaKwSetter::new(default, key)?,
//         })
//     }

//     pub fn init_unchecked(key: &'static str) -> Self {
//         MatrixSetter {
//             try_convert: false,
//             default: MetaKwSetter::init_unchecked(key),
//         }
//     }
// }

impl<T> Default for MatrixSetter<T> {
    fn default() -> MatrixSetter<T> {
        MatrixSetter {
            try_convert: false,
            default: MetaKwSetter::default(),
        }
    }
}

newtype_disp!(NonStdKey);
newtype_disp!(NonStdMeasKey);
newtype_disp!(NonStdMeasPattern);
newtype_disp!(MeasIdx);

newtype_asref!(NonStdKey, str);
newtype_asref!(NonStdMeasKey, str);
newtype_asref!(NonStdMeasPattern, str);

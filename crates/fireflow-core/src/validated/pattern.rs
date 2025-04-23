use crate::macros::{newtype_from_outer, newtype_fromstr};

use regex::Regex;
use std::fmt;
use std::str::FromStr;

/// A pattern to match the $PnN for the time channel.
///
/// Defaults to matching "TIME" or "Time".
#[derive(Clone)]
pub struct TimePattern(pub CheckedPattern);

newtype_from_outer!(TimePattern, CheckedPattern);
newtype_fromstr!(TimePattern, PatternError);

impl Default for TimePattern {
    fn default() -> Self {
        Self(CheckedPattern(Regex::new("^(TIME|Time)$").unwrap()))
    }
}

#[derive(Clone)]
pub struct CheckedPattern(Regex);

impl CheckedPattern {
    pub fn as_inner(&self) -> &Regex {
        &self.0
    }
}

impl FromStr for CheckedPattern {
    type Err = PatternError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Regex::new(s)
            .map_err(|_| PatternError(s.to_string()))
            .map(CheckedPattern)
    }
}

pub struct PatternError(String);

impl fmt::Display for PatternError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "Could not make pattern from {}", self.0)
    }
}

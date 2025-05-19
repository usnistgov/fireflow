use crate::macros::{newtype_disp, newtype_from_outer, newtype_fromstr};

use regex::{Error, Regex};
use std::fmt;
use std::str::FromStr;

/// A pattern to match the $PnN for the time measurement.
///
/// Defaults to matching "TIME" or "Time".
#[derive(Clone)]
pub struct TimePattern(pub CheckedPattern);

newtype_from_outer!(TimePattern, CheckedPattern);
newtype_fromstr!(TimePattern, Error);
newtype_disp!(TimePattern);

impl Default for TimePattern {
    fn default() -> Self {
        Self(CheckedPattern(Regex::new("^(TIME|Time)$").unwrap()))
    }
}

#[derive(Clone)]
pub struct CheckedPattern(Regex);

newtype_disp!(CheckedPattern);

impl CheckedPattern {
    pub fn as_inner(&self) -> &Regex {
        &self.0
    }
}

impl FromStr for CheckedPattern {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Regex::new(s).map(CheckedPattern)
    }
}

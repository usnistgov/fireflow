use derive_more::{AsRef, Display, FromStr};
use regex::{Error, Regex};
use std::str::FromStr;

/// A pattern to match the $PnN for the time measurement.
///
/// Defaults to matching "TIME" or "Time".
#[derive(Clone, FromStr, Display)]
pub struct TimePattern(pub CheckedPattern);

impl Default for TimePattern {
    fn default() -> Self {
        Self(CheckedPattern(Regex::new("^(TIME|Time)$").unwrap()))
    }
}

#[derive(Clone, Display, AsRef)]
pub struct CheckedPattern(Regex);

impl FromStr for CheckedPattern {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Regex::new(s).map(CheckedPattern)
    }
}

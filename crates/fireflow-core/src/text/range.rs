use crate::macros::{newtype_asref, newtype_disp};

use regex::Regex;
use serde::Serialize;
use std::fmt;
use std::str::FromStr;

/// The value of the $PnR key (all versions)
///
/// Technically this should only be an integer, but many versions also store
/// floats which makes sense for cases where $DATATYPE/$PnDATATYPE indicates
/// float or double.
// TODO make sure this is really a number
#[derive(Clone, Serialize)]
pub struct Range(String);

newtype_asref!(Range, str);
newtype_disp!(Range);

const RANGEPAT: &str = "^(-|\\+)?([0-9]+|[0-9]+\\.[0-9]+)$";

impl FromStr for Range {
    type Err = ParseRangeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let r = Regex::new(RANGEPAT).unwrap();
        if r.is_match(s) {
            Ok(Range(s.to_string()))
        } else {
            Err(ParseRangeError)
        }
    }
}

impl From<u64> for Range {
    fn from(value: u64) -> Self {
        Range(value.to_string())
    }
}

impl From<f32> for Range {
    fn from(value: f32) -> Self {
        let x = if value.is_infinite() {
            if value.is_sign_positive() {
                f32::MAX
            } else {
                f32::MIN
            }
        } else if value.is_nan() {
            0.0
        } else {
            value
        };
        Range(x.to_string())
    }
}

impl From<f64> for Range {
    fn from(value: f64) -> Self {
        let x = if value.is_infinite() {
            if value.is_sign_positive() {
                f64::MAX
            } else {
                f64::MIN
            }
        } else if value.is_nan() {
            0.0
        } else {
            value
        };
        Range(x.to_string())
    }
}

pub struct ParseRangeError;

impl fmt::Display for ParseRangeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "must be an integer or decimal")
    }
}

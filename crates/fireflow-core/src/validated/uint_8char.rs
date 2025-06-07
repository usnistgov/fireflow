use crate::header::MAX_HEADER_OFFSET;
use crate::macros::{enum_from, enum_from_disp, match_many_to_one, newtype_from_outer};

use serde::Serialize;
use std::fmt;
use std::num::ParseIntError;
use std::str::FromStr;

/// An unsigned int which may only be 8 chars wide (ie less than 99,999,999)
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Default)]
pub struct Uint8Char(u32);

newtype_from_outer!(Uint8Char, u32);

impl TryFrom<u64> for Uint8Char {
    type Error = Uint8CharOverflow;
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        value
            .try_into()
            .map_or(Err(Uint8CharOverflow(value)), |x: u32| {
                if x > MAX_HEADER_OFFSET {
                    Err(Uint8CharOverflow(x.into()))
                } else {
                    Ok(Uint8Char(x))
                }
            })
    }
}

impl FromStr for Uint8Char {
    type Err = ParseUint8CharError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<u64>()
            .map_err(ParseUint8CharError::Int)
            .and_then(|x| x.try_into().map_err(ParseUint8CharError::Overflow))
    }
}

impl fmt::Display for Uint8Char {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{:0>8}", self.0)
    }
}

enum_from_disp!(
    pub ParseUint8CharError,
    [Overflow, Uint8CharOverflow],
    [Int, ParseIntError]
);

pub struct Uint8CharOverflow(u64);

impl fmt::Display for Uint8CharOverflow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "must be {} or less, got {}", MAX_HEADER_OFFSET, self.0)
    }
}

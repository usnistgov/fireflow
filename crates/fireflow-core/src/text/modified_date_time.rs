use crate::macros::{newtype_from, newtype_from_outer};

use chrono::{NaiveDateTime, Timelike};
use serde::Serialize;
use std::fmt;
use std::str::FromStr;

// TODO this doesn't need to be encapsulated

/// A datetime as used in the $LAST_MODIFIED key (3.1+ only)
///
/// Inner value is private to ensure it always gets parsed/printed using the
/// correct format
// TODO this should almost certainly be after $ENDDATETIME if given
#[derive(Clone, Copy, Serialize)]
pub struct ModifiedDateTime(NaiveDateTime);

newtype_from!(ModifiedDateTime, NaiveDateTime);
newtype_from_outer!(ModifiedDateTime, NaiveDateTime);

const DATETIME_FMT: &str = "%d-%b-%Y %H:%M:%S";

impl FromStr for ModifiedDateTime {
    type Err = ModifiedDateTimeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (dt, cc) =
            NaiveDateTime::parse_and_remainder(s, DATETIME_FMT).or(Err(ModifiedDateTimeError))?;
        if cc.is_empty() {
            Ok(ModifiedDateTime(dt))
        } else if cc.len() == 3 && cc.starts_with(".") {
            let tt: u32 = cc[1..3].parse().or(Err(ModifiedDateTimeError))?;
            dt.with_nanosecond(tt * 10000000)
                .map(ModifiedDateTime)
                .ok_or(ModifiedDateTimeError)
        } else {
            Err(ModifiedDateTimeError)
        }
    }
}

impl fmt::Display for ModifiedDateTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let dt = self.0.format(DATETIME_FMT);
        let cc = self.0.nanosecond() / 10000000;
        write!(f, "{dt}.{cc}")
    }
}

pub struct ModifiedDateTimeError;

impl fmt::Display for ModifiedDateTimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "must be like 'dd-mmm-yyyy hh:mm:ss[.cc]'")
    }
}

use crate::optionalkw::*;

use chrono::{DateTime, FixedOffset};
use serde::Serialize;
use std::fmt;

/// A convenient bundle for the $BEGINDATETIME and $ENDDATETIME keys (3.2+)
#[derive(Clone, Serialize, Default)]
pub struct Datetimes {
    /// Value for the $BEGINDATETIME key.
    begin: Option<BeginDateTime>,

    /// Value for the $ENDDATETIME key.
    end: Option<EndDateTime>,
}

#[derive(Clone, Copy, Serialize)]
pub struct BeginDateTime(pub FCSDateTime);

#[derive(Clone, Copy, Serialize)]
pub struct EndDateTime(pub FCSDateTime);

/// A datetime as used in the $(BEGIN|END)DATETIME keys (3.2+ only)
#[derive(Clone, Copy, Serialize)]
pub struct FCSDateTime(pub DateTime<FixedOffset>);

impl Datetimes {
    pub fn new(
        begin: OptionalKw<BeginDateTime>,
        end: OptionalKw<EndDateTime>,
    ) -> DatetimesResult<Self> {
        let ret = Self {
            begin: begin.0,
            end: end.0,
        };
        if ret.valid() {
            Ok(ret)
        } else {
            Err(InvalidDatetimes)
        }
    }

    pub fn begin(&self) -> OptionalKw<BeginDateTime> {
        OptionalKw(self.begin)
    }

    pub fn end(&self) -> OptionalKw<EndDateTime> {
        OptionalKw(self.end)
    }

    pub fn set_begin(&mut self, x: OptionalKw<BeginDateTime>) -> DatetimesResult<()> {
        let tmp = self.begin;
        self.begin = x.0;
        if self.valid() {
            Ok(())
        } else {
            self.begin = tmp;
            Err(InvalidDatetimes)
        }
    }

    pub fn set_end(&mut self, x: OptionalKw<EndDateTime>) -> DatetimesResult<()> {
        let tmp = self.end;
        self.end = x.0;
        if self.valid() {
            Ok(())
        } else {
            self.end = tmp;
            Err(InvalidDatetimes)
        }
    }

    pub fn valid(&self) -> bool {
        if let (Some(b), Some(e)) = (&self.begin, &self.end) {
            (b.0).0 < (e.0).0
        } else {
            true
        }
    }
}

pub struct InvalidDatetimes;

type DatetimesResult<T> = Result<T, InvalidDatetimes>;

impl fmt::Display for InvalidDatetimes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "$BEGINDATETIME is after $ENDDATETIME")
    }
}

use crate::macros::{newtype_from, newtype_from_outer};
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

newtype_from!(FCSDateTime, DateTime<FixedOffset>);
newtype_from_outer!(FCSDateTime, DateTime<FixedOffset>);

macro_rules! get_set {
    ($fn_get_naive:ident, $fn:ident, $fn_naive:ident, $in:path, $field:ident) => {
        pub fn $field(&self) -> OptionalKw<$in> {
            OptionalKw(self.$field)
        }

        pub fn $fn_get_naive(&self) -> Option<DateTime<FixedOffset>> {
            self.$field().0.map(|x| x.0.into())
        }

        pub fn $fn(&mut self, x: OptionalKw<$in>) -> DatetimesResult<()> {
            let tmp = self.$field;
            self.$field = x.0;
            if self.valid() {
                Ok(())
            } else {
                self.$field = tmp;
                Err(InvalidDatetimes)
            }
        }

        pub fn $fn_naive(&mut self, x: Option<DateTime<FixedOffset>>) -> DatetimesResult<()> {
            self.$fn(x.map(|y| FCSDateTime(y).into()).into())
        }
    };
}

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

    get_set!(
        begin_naive,
        set_begin,
        set_begin_naive,
        BeginDateTime,
        begin
    );

    get_set!(end_naive, set_end, set_end_naive, EndDateTime, end);

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

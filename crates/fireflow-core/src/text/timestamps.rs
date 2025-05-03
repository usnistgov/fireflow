use crate::macros::{newtype_from, newtype_from_outer};

use super::optionalkw::*;

use chrono::{NaiveDate, NaiveTime};
use serde::Serialize;
use std::fmt;

/// A convenient bundle holding data/time keyword values.
///
/// The generic type parameter is meant to account for the fact that the time
/// types for different versions are all slightly different in their treatment
/// of sub-second time.
#[derive(Clone, Serialize)]
pub struct Timestamps<X> {
    /// The value of the $BTIM key
    btim: Option<Btim<X>>,

    /// The value of the $ETIM key
    etim: Option<Etim<X>>,

    /// The value of the $DATE key
    date: Option<FCSDate>,
}

impl<X> Default for Timestamps<X> {
    fn default() -> Self {
        Self {
            btim: None,
            etim: None,
            date: None,
        }
    }
}

#[derive(Clone, Copy, Serialize)]
pub struct Btim<T>(pub T);

#[derive(Clone, Copy, Serialize)]
pub struct Etim<T>(pub T);

/// A date as used in the $DATE key
#[derive(Clone, Copy, Serialize)]
pub struct FCSDate(pub NaiveDate);

newtype_from!(FCSDate, NaiveDate);
newtype_from_outer!(FCSDate, NaiveDate);

macro_rules! get_set {
    ($fn_get_naive:ident, $fn:ident, $fn_naive:ident, $in:path, $in_naive:path, $field:ident) => {
        pub fn $field(&self) -> OptionalKw<$in> {
            OptionalKw(self.$field)
        }

        pub fn $fn_get_naive(&self) -> Option<$in_naive>
        where
            $in_naive: From<$in>,
        {
            self.$field().0.map(|x| x.into())
        }

        pub fn $fn(&mut self, x: OptionalKw<$in>) -> TimestampsResult<()> {
            let tmp = self.$field;
            self.$field = x.0;
            if self.valid() {
                Ok(())
            } else {
                self.$field = tmp;
                Err(InvalidTimestamps)
            }
        }

        pub fn $fn_naive(&mut self, x: Option<$in_naive>) -> TimestampsResult<()>
        where
            $in: From<$in_naive>,
        {
            self.$fn(x.map(|y| y.into()).into())
        }
    };
}

impl<X> Timestamps<X>
where
    X: PartialOrd,
    X: Copy,
{
    pub fn new(
        btim: OptionalKw<Btim<X>>,
        etim: OptionalKw<Etim<X>>,
        date: OptionalKw<FCSDate>,
    ) -> TimestampsResult<Self> {
        let ret = Self {
            btim: btim.0,
            etim: etim.0,
            date: date.0,
        };
        if ret.valid() {
            Ok(ret)
        } else {
            Err(InvalidTimestamps)
        }
    }

    get_set!(
        btim_naive,
        set_btim,
        set_btim_naive,
        Btim<X>,
        NaiveTime,
        btim
    );

    get_set!(
        etim_naive,
        set_etim,
        set_etim_naive,
        Etim<X>,
        NaiveTime,
        etim
    );

    get_set!(
        date_naive,
        set_date,
        set_date_naive,
        FCSDate,
        NaiveDate,
        date
    );

    pub fn map<F, Y>(self, f: F) -> Timestamps<Y>
    where
        F: Fn(X) -> Y,
    {
        Timestamps {
            btim: self.btim.map(|x| Btim(f(x.0))),
            etim: self.etim.map(|x| Etim(f(x.0))),
            date: self.date,
        }
    }

    pub fn valid(&self) -> bool {
        if self.date.is_some() {
            if let (Some(b), Some(e)) = (&self.btim, &self.etim) {
                b.0 < e.0
            } else {
                true
            }
        } else {
            true
        }
    }
}

pub struct InvalidTimestamps;

type TimestampsResult<T> = Result<T, InvalidTimestamps>;

impl fmt::Display for InvalidTimestamps {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "$ETIM is before $BTIM and $DATE is given")
    }
}

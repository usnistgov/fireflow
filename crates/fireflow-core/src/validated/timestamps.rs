use crate::optionalkw::*;

use chrono::NaiveDate;
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

    pub fn btim(&self) -> OptionalKw<Btim<X>> {
        OptionalKw(self.btim)
    }

    pub fn etim(&self) -> OptionalKw<Etim<X>> {
        OptionalKw(self.etim)
    }

    pub fn date(&self) -> OptionalKw<FCSDate> {
        OptionalKw(self.date)
    }

    pub fn set_btim(&mut self, x: OptionalKw<Btim<X>>) -> TimestampsResult<()> {
        let tmp = self.btim;
        self.btim = x.0;
        if self.valid() {
            Ok(())
        } else {
            self.btim = tmp;
            Err(InvalidTimestamps)
        }
    }

    pub fn set_etim(&mut self, x: OptionalKw<Etim<X>>) -> TimestampsResult<()> {
        let tmp = self.etim;
        self.etim = x.0;
        if self.valid() {
            Ok(())
        } else {
            self.etim = tmp;
            Err(InvalidTimestamps)
        }
    }

    pub fn set_date(&mut self, x: OptionalKw<FCSDate>) -> TimestampsResult<()> {
        let tmp = self.date;
        self.date = x.0;
        if self.valid() {
            Ok(())
        } else {
            self.date = tmp;
            Err(InvalidTimestamps)
        }
    }

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

use crate::core::{AnyMetarootKeyLossError, UnitaryKeyLossError};
use crate::error::*;
use crate::validated::keys::*;

use super::optional::*;
use super::parser::*;

use chrono::{DateTime, FixedOffset};
use derive_more::{AsRef, Display, From, FromStr, Into};
use serde::Serialize;
use std::fmt;
use std::mem;
use std::str::FromStr;

/// A convenient bundle for the $BEGINDATETIME and $ENDDATETIME keys (3.2+)
#[derive(Clone, Serialize, Default, AsRef)]
pub struct Datetimes {
    /// Value for the $BEGINDATETIME key.
    #[as_ref(Option<BeginDateTime>)]
    begin: Option<BeginDateTime>,

    /// Value for the $ENDDATETIME key.
    #[as_ref(Option<EndDateTime>)]
    end: Option<EndDateTime>,
}

#[derive(Clone, Copy, Serialize, From, Into, Display, FromStr)]
#[from(DateTime<FixedOffset>, FCSDateTime)]
#[into(DateTime<FixedOffset>, FCSDateTime)]
pub struct BeginDateTime(pub FCSDateTime);

#[derive(Clone, Copy, Serialize, From, Into, Display, FromStr)]
#[from(DateTime<FixedOffset>, FCSDateTime)]
#[into(DateTime<FixedOffset>, FCSDateTime)]
pub struct EndDateTime(pub FCSDateTime);

/// A datetime as used in the $(BEGIN|END)DATETIME keys (3.2+ only)
#[derive(Clone, Copy, Serialize, From, Into)]
pub struct FCSDateTime(pub DateTime<FixedOffset>);

impl Datetimes {
    pub fn try_new(
        begin: MaybeValue<BeginDateTime>,
        end: MaybeValue<EndDateTime>,
    ) -> DatetimesResult<Self> {
        let ret = Self {
            begin: begin.0,
            end: end.0,
        };
        if ret.valid() {
            Ok(ret)
        } else {
            Err(ReversedDatetimes)
        }
    }

    pub fn set_begin(&mut self, time: Option<BeginDateTime>) -> Result<(), ReversedDatetimes> {
        let tmp = mem::replace(&mut self.begin, time);
        if !self.valid() {
            self.begin = tmp;
            return Err(ReversedDatetimes);
        }
        Ok(())
    }

    pub fn set_end(&mut self, time: Option<EndDateTime>) -> Result<(), ReversedDatetimes> {
        let tmp = mem::replace(&mut self.end, time);
        if !self.valid() {
            self.end = tmp;
            return Err(ReversedDatetimes);
        }
        Ok(())
    }

    pub fn valid(&self) -> bool {
        if let (Some(b), Some(e)) = (&self.begin, &self.end) {
            (b.0).0 < (e.0).0
        } else {
            true
        }
    }

    pub(crate) fn lookup<E>(kws: &mut StdKeywords) -> LookupTentative<Self, E> {
        let b = BeginDateTime::lookup_opt(kws);
        let e = EndDateTime::lookup_opt(kws);
        b.zip(e).and_tentatively(|(begin, end)| {
            Datetimes::try_new(begin, end)
                .map(Tentative::new1)
                .unwrap_or_else(|w| {
                    let ow = LookupKeysWarning::Relation(w.into());
                    Tentative::new(Datetimes::default(), vec![ow], vec![])
                })
        })
    }

    pub(crate) fn opt_keywords(&self) -> impl Iterator<Item = (String, String)> {
        [
            OptMetarootKey::pair_opt(&MaybeValue(self.begin)),
            OptMetarootKey::pair_opt(&MaybeValue(self.end)),
        ]
        .into_iter()
        .flat_map(|(k, v)| v.map(|x| (k, x)))
    }

    pub(crate) fn check_loss(self, lossless: bool) -> BiTentative<(), AnyMetarootKeyLossError> {
        let mut tnt = Tentative::new1(());
        if self.begin.is_some() {
            tnt.push_error_or_warning(UnitaryKeyLossError::<BeginDateTime>::default(), lossless);
        }
        if self.end.is_some() {
            tnt.push_error_or_warning(UnitaryKeyLossError::<EndDateTime>::default(), lossless);
        }
        tnt
    }
}

pub struct ReversedDatetimes;

type DatetimesResult<T> = Result<T, ReversedDatetimes>;

impl fmt::Display for ReversedDatetimes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "$BEGINDATETIME is after $ENDDATETIME")
    }
}

impl FromStr for FCSDateTime {
    type Err = FCSDateTimeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let formats = [
            "%Y-%m-%dT%H:%M:%S%.f",
            "%Y-%m-%dT%H:%M:%S%.f%#z",
            "%Y-%m-%dT%H:%M:%S%.f%:z",
            "%Y-%m-%dT%H:%M:%S%.f%::z",
            "%Y-%m-%dT%H:%M:%S%.f%:::z",
        ];
        for f in formats {
            if let Ok(t) = DateTime::parse_from_str(s, f) {
                return Ok(FCSDateTime(t));
            }
        }
        Err(FCSDateTimeError)
    }
}

impl fmt::Display for FCSDateTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", self.0.format("%Y-%m-%dT%H:%M:%S%.f%:z"))
    }
}

pub struct FCSDateTimeError;

impl fmt::Display for FCSDateTimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "must be formatted like 'yyyy-mm-ddThh:mm:ss[TZD]'")
    }
}

use crate::error::*;
use crate::validated::keys::*;

use super::optional::*;
use super::parser::*;

use chrono::{NaiveDate, NaiveTime, Timelike};
use derive_more::{AsRef, Display, From, FromStr, Into};
use once_cell::sync::Lazy;
use regex::Regex;
use serde::Serialize;
use std::fmt;
use std::mem;
use std::str::FromStr;

/// A convenient bundle holding data/time keyword values.
///
/// The generic type parameter is meant to account for the fact that the time
/// types for different versions are all slightly different in their treatment
/// of sub-second time.
#[derive(Clone, Serialize, AsRef)]
pub struct Timestamps<X> {
    /// The value of the $BTIM key
    #[as_ref(Option<Btim<X>>)]
    btim: Option<Btim<X>>,

    /// The value of the $ETIM key
    #[as_ref(Option<Etim<X>>)]
    etim: Option<Etim<X>>,

    /// The value of the $DATE key
    #[as_ref(Option<FCSDate>)]
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

pub type Btim<T> = Xtim<false, T>;
pub type Etim<T> = Xtim<true, T>;

#[derive(Clone, Copy, Serialize, Display, FromStr, From)]
pub struct Xtim<const IS_ETIM: bool, T>(pub T);

/// A date as used in the $DATE key
#[derive(Clone, Copy, Serialize, From, Into, AsRef)]
pub struct FCSDate(pub NaiveDate);

impl<X> Timestamps<X> {
    pub fn new(
        btim: MaybeValue<Btim<X>>,
        etim: MaybeValue<Etim<X>>,
        date: MaybeValue<FCSDate>,
    ) -> TimestampsResult<Self>
    where
        X: PartialOrd,
    {
        let ret = Self {
            btim: btim.0,
            etim: etim.0,
            date: date.0,
        };
        if ret.valid() {
            Ok(ret)
        } else {
            Err(ReversedTimestamps)
        }
    }

    pub fn set_btim(&mut self, time: Option<Btim<X>>) -> TimestampsResult<()>
    where
        X: PartialOrd,
    {
        let tmp = mem::replace(&mut self.btim, time);
        if !self.valid() {
            self.btim = tmp;
            return Err(ReversedTimestamps);
        }
        Ok(())
    }

    pub fn set_etim(&mut self, time: Option<Etim<X>>) -> TimestampsResult<()>
    where
        X: PartialOrd,
    {
        let tmp = mem::replace(&mut self.etim, time);
        if !self.valid() {
            self.etim = tmp;
            return Err(ReversedTimestamps);
        }
        Ok(())
    }

    pub fn set_date(&mut self, date: Option<FCSDate>) -> TimestampsResult<()>
    where
        X: PartialOrd,
    {
        let tmp = mem::replace(&mut self.date, date);
        if !self.valid() {
            self.date = tmp;
            return Err(ReversedTimestamps);
        }
        Ok(())
    }

    pub fn map<F, Y>(self, f: F) -> Timestamps<Y>
    where
        F: Fn(X) -> Y,
    {
        Timestamps {
            btim: self.btim.map(|x| Xtim(f(x.0))),
            etim: self.etim.map(|x| Xtim(f(x.0))),
            date: self.date,
        }
    }

    pub fn valid(&self) -> bool
    where
        X: PartialOrd,
    {
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

    pub(crate) fn lookup<E>(kws: &mut StdKeywords) -> LookupTentative<Self, E>
    where
        Btim<X>: OptMetarootKey,
        Etim<X>: OptMetarootKey,
        ParseOptKeyWarning: From<<Btim<X> as FromStr>::Err> + From<<Etim<X> as FromStr>::Err>,
        X: PartialOrd,
    {
        let b = Btim::lookup_opt(kws);
        let e = Etim::lookup_opt(kws);
        let d = FCSDate::lookup_opt(kws);
        Self::process_lookup(b, e, d)
    }

    pub(crate) fn lookup_dep(
        kws: &mut StdKeywords,
        disallow_dep: bool,
    ) -> LookupTentative<Self, DeprecatedError>
    where
        Btim<X>: OptMetarootKey,
        Etim<X>: OptMetarootKey,
        ParseOptKeyWarning: From<<Btim<X> as FromStr>::Err> + From<<Etim<X> as FromStr>::Err>,
        X: PartialOrd,
    {
        let b = Btim::lookup_opt_dep(kws, disallow_dep);
        let e = Etim::lookup_opt_dep(kws, disallow_dep);
        let d = FCSDate::lookup_opt_dep(kws, disallow_dep);
        Self::process_lookup(b, e, d)
    }

    fn process_lookup<E>(
        b: LookupTentative<MaybeValue<Btim<X>>, E>,
        e: LookupTentative<MaybeValue<Etim<X>>, E>,
        d: LookupTentative<MaybeValue<FCSDate>, E>,
    ) -> LookupTentative<Self, E>
    where
        X: PartialOrd,
    {
        b.zip3(e, d).and_tentatively(|(btim, etim, date)| {
            Timestamps::new(btim, etim, date)
                .map(Tentative::new1)
                .unwrap_or_else(|w| {
                    let ow = LookupKeysWarning::Relation(w.into());
                    Tentative::new(Timestamps::default(), vec![ow], vec![])
                })
        })
    }

    pub(crate) fn opt_keywords(&self) -> impl Iterator<Item = (String, String)>
    where
        Btim<X>: OptMetarootKey,
        Etim<X>: OptMetarootKey,
        X: Copy,
    {
        [
            OptMetarootKey::pair_opt(&MaybeValue(self.btim)),
            OptMetarootKey::pair_opt(&MaybeValue(self.etim)),
            OptMetarootKey::pair_opt(&MaybeValue(self.date)),
        ]
        .into_iter()
        .flat_map(|(k, v)| v.map(|x| (k, x)))
    }
}

pub struct ReversedTimestamps;

type TimestampsResult<T> = Result<T, ReversedTimestamps>;

impl fmt::Display for ReversedTimestamps {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "$ETIM is before $BTIM and $DATE is given")
    }
}

// the "%b" format is case-insensitive so this should work for "Jan", "JAN",
// "jan", "jaN", etc
const FCS_DATE_FORMAT: &str = "%d-%b-%Y";

impl FromStr for FCSDate {
    type Err = FCSDateError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        NaiveDate::parse_from_str(s, FCS_DATE_FORMAT)
            .or(Err(FCSDateError))
            .map(FCSDate)
    }
}

impl fmt::Display for FCSDate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", self.0.format(FCS_DATE_FORMAT))
    }
}

pub struct FCSDateError;

impl fmt::Display for FCSDateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "must be like 'dd-mmm-yyyy'")
    }
}

/// A time as used in the $BTIM/ETIM keys without seconds (2.0 only)
#[derive(Clone, Copy, Serialize, PartialEq, Eq, PartialOrd, From, Into)]
pub struct FCSTime(pub NaiveTime);

impl FromStr for FCSTime {
    type Err = FCSTimeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        NaiveTime::parse_from_str(s, "%H:%M:%S")
            .map(FCSTime)
            .or(Err(FCSTimeError))
    }
}

impl fmt::Display for FCSTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", self.0.format("%H:%M:%S"))
    }
}

pub struct FCSTimeError;

impl fmt::Display for FCSTimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "must be like 'hh:mm:ss'")
    }
}

/// A time as used in the $BTIM/ETIM keys with 1/60 seconds (3.0 only)
#[derive(Clone, Copy, Serialize, PartialEq, Eq, PartialOrd, From, Into)]
pub struct FCSTime60(pub NaiveTime);

impl FromStr for FCSTime60 {
    type Err = FCSTime60Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        NaiveTime::parse_from_str(s, "%H:%M:%S")
            .or_else(|_| match s.split(":").collect::<Vec<_>>()[..] {
                [s1, s2, s3, s4] => {
                    let hh: u32 = s1.parse().or(Err(FCSTime60Error))?;
                    let mm: u32 = s2.parse().or(Err(FCSTime60Error))?;
                    let ss: u32 = s3.parse().or(Err(FCSTime60Error))?;
                    let tt: u32 = s4.parse().or(Err(FCSTime60Error))?;
                    let nn = tt * 1_000_000 / 60;
                    NaiveTime::from_hms_micro_opt(hh, mm, ss, nn).ok_or(FCSTime60Error)
                }
                _ => Err(FCSTime60Error),
            })
            .map(FCSTime60)
    }
}

impl fmt::Display for FCSTime60 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let base = self.0.format("%H:%M:%S");
        let cc = u64::from(self.0.nanosecond()) * 60 / 1_000_000_000;
        write!(f, "{}.{}", base, cc)
    }
}

pub struct FCSTime60Error;

impl fmt::Display for FCSTime60Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "must be like 'hh:mm:ss[:tt]' where 'tt' is in 1/60th seconds"
        )
    }
}

/// A time as used in the $BTIM/ETIM keys with centiseconds (3.1+ only)
#[derive(Clone, Copy, Serialize, PartialEq, Eq, PartialOrd, From, Into)]
pub struct FCSTime100(pub NaiveTime);

impl FromStr for FCSTime100 {
    type Err = FCSTime100Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        NaiveTime::parse_from_str(s, "%H:%M:%S")
            .or_else(|_| {
                static RE: Lazy<Regex> = Lazy::new(|| {
                    Regex::new(r"([0-9]){2}:([0-9]){2}:([0-9]){2}.([0-9]){2}").unwrap()
                });
                let cap = RE.captures(s).ok_or(FCSTime100Error)?;
                let [s1, s2, s3, s4] = cap.extract().1;
                // ASSUME these will never fail
                let hh: u32 = s1.parse().unwrap();
                let mm: u32 = s2.parse().unwrap();
                let ss: u32 = s3.parse().unwrap();
                let tt: u32 = s4.parse().unwrap();
                NaiveTime::from_hms_milli_opt(hh, mm, ss, tt * 10).ok_or(FCSTime100Error)
            })
            .map(FCSTime100)
    }
}

impl fmt::Display for FCSTime100 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let base = self.0.format("%H:%M:%S");
        let cc = self.0.nanosecond() / 10_000_000;
        write!(f, "{}.{}", base, cc)
    }
}

pub struct FCSTime100Error;

impl fmt::Display for FCSTime100Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "must be like 'hh:mm:ss[.cc]'")
    }
}

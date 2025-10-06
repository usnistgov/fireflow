use crate::config::StdTextReadConfig;
use crate::error::ResultExt as _;
use crate::validated::keys::StdKeywords;
use crate::validated::timepattern::ParseWithTimePatternError;

use super::optional::MaybeValue;
use super::parser::{
    FromStrStateful, LookupOptional, LookupTentative, OptMetarootKey, ParseOptKeyError,
};

use chrono::{NaiveDate, NaiveTime, Timelike as _};
use derive_more::{AsRef, Display, From, FromStr, Into};
use regex::Regex;
use std::fmt;
use std::mem;
use std::str::FromStr;
use std::sync::LazyLock;
use thiserror::Error;

#[cfg(feature = "serde")]
use serde::Serialize;

/// A convenient bundle holding data/time keyword values.
///
/// The generic type parameter is meant to account for the fact that the time
/// types for different versions are all slightly different in their treatment
/// of sub-second time.
#[derive(Clone, AsRef, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
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

#[derive(Clone, Copy, Display, FromStr, From, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct Xtim<const IS_ETIM: bool, T>(pub T);

impl<const IS_ETIM: bool, T> FromStrStateful for Xtim<IS_ETIM, T>
where
    T: FromStr + From<NaiveTime>,
{
    type Err = FCSFixedTimeError<<T as FromStr>::Err>;
    type Payload<'a> = ();

    fn from_str_st<'a>(s: &str, (): (), conf: &StdTextReadConfig) -> Result<Self, Self::Err> {
        let ret = if let Some(pat) = conf.time_pattern.as_ref() {
            pat.parse_str(s)?.into()
        } else {
            s.parse::<T>().map_err(FCSFixedTimeError::Native)?
        };
        Ok(Self(ret))
    }
}

/// A date as used in the $DATE key
#[derive(Clone, Copy, From, Into, AsRef, PartialEq, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[display("{}", _0.format(FCS_DATE_FORMAT))]
pub struct FCSDate(pub NaiveDate);

impl<X> Timestamps<X> {
    pub fn try_new(
        btim: Option<Btim<X>>,
        etim: Option<Etim<X>>,
        date: Option<FCSDate>,
    ) -> TimestampsResult<Self>
    where
        X: PartialOrd,
    {
        let ret = Self { btim, etim, date };
        if ret.valid() {
            Ok(ret)
        } else {
            Err(ReversedTimestampsError)
        }
    }

    pub fn set_btim(&mut self, time: Option<Btim<X>>) -> TimestampsResult<()>
    where
        X: PartialOrd,
    {
        let tmp = mem::replace(&mut self.btim, time);
        if !self.valid() {
            self.btim = tmp;
            return Err(ReversedTimestampsError);
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
            return Err(ReversedTimestampsError);
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
            return Err(ReversedTimestampsError);
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

    pub(crate) fn lookup(kws: &mut StdKeywords, conf: &StdTextReadConfig) -> LookupTentative<Self>
    where
        Btim<X>: OptMetarootKey,
        Etim<X>: OptMetarootKey,
        ParseOptKeyError:
            From<<Btim<X> as FromStrStateful>::Err> + From<<Etim<X> as FromStrStateful>::Err>,
        for<'a> X: PartialOrd + FromStr + From<NaiveTime>,
    {
        let b = Btim::lookup_opt_st(kws, (), conf);
        let e = Etim::lookup_opt_st(kws, (), conf);
        let d = FCSDate::lookup_opt_st(kws, (), conf);
        Self::process_lookup(b, e, d, conf)
    }

    pub(crate) fn lookup_dep(
        kws: &mut StdKeywords,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<Self>
    where
        Btim<X>: OptMetarootKey,
        Etim<X>: OptMetarootKey,
        ParseOptKeyError:
            From<<Btim<X> as FromStrStateful>::Err> + From<<Etim<X> as FromStrStateful>::Err>,
        for<'a> X: PartialOrd + FromStr + From<NaiveTime>,
    {
        let dd = conf.disallow_deprecated;
        let b = Btim::lookup_opt_st_dep(kws, dd, (), conf);
        let e = Etim::lookup_opt_st_dep(kws, dd, (), conf);
        let d = FCSDate::lookup_opt_st_dep(kws, dd, (), conf);
        Self::process_lookup(b, e, d, conf)
    }

    fn process_lookup(
        b: LookupOptional<Btim<X>>,
        e: LookupOptional<Etim<X>>,
        d: LookupOptional<FCSDate>,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<Self>
    where
        X: PartialOrd,
    {
        b.zip3(e, d).and_tentatively(|(btim, etim, date)| {
            Timestamps::try_new(btim.0, etim.0, date.0)
                .into_tentative_def(!conf.allow_optional_dropping)
                .inner_into()
        })
    }

    pub(crate) fn opt_keywords(&self) -> impl Iterator<Item = (String, String)>
    where
        Btim<X>: OptMetarootKey,
        Etim<X>: OptMetarootKey,
        X: Copy + fmt::Display,
    {
        [
            MaybeValue(self.btim).root_kw_pair(),
            MaybeValue(self.etim).root_kw_pair(),
            MaybeValue(self.date).root_kw_pair(),
        ]
        .into_iter()
        .filter_map(|(k, v)| v.map(|x| (k, x)))
    }
}

#[derive(Debug, Error)]
#[error("$ETIM is before $BTIM and $DATE is given")]
pub struct ReversedTimestampsError;

type TimestampsResult<T> = Result<T, ReversedTimestampsError>;

// the "%b" format is case-insensitive so this should work for "Jan", "JAN",
// "jan", "jaN", etc
const FCS_DATE_FORMAT: &str = "%d-%b-%Y";

impl FromStrStateful for FCSDate {
    type Err = FCSDateError;
    type Payload<'a> = ();

    fn from_str_st(s: &str, (): (), conf: &StdTextReadConfig) -> Result<Self, Self::Err> {
        if let Some(pattern) = &conf.date_pattern {
            Self::parse_with_pattern(s, pattern.as_ref())
        } else {
            s.parse::<FCSDate>()
        }
    }
}

impl FCSDate {
    fn parse_with_pattern(s: &str, pat: &str) -> Result<Self, FCSDateError> {
        NaiveDate::parse_from_str(s, pat)
            .or(Err(FCSDateError))
            .map(FCSDate)
    }
}

impl FromStr for FCSDate {
    type Err = FCSDateError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse_with_pattern(s, FCS_DATE_FORMAT)
    }
}

#[derive(Debug, Error)]
#[error("must be like 'dd-mmm-yyyy'")]
pub struct FCSDateError;

/// A time as used in the $BTIM/ETIM keys without seconds (2.0 only)
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, From, Into, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[display("{}", _0.format(FCS_TIME_FORMAT))]
pub struct FCSTime(pub NaiveTime);

const FCS_TIME_FORMAT: &str = "%H:%M:%S";

impl FromStr for FCSTime {
    type Err = FCSTimeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        NaiveTime::parse_from_str(s, FCS_TIME_FORMAT)
            .map(FCSTime)
            .or(Err(FCSTimeError))
    }
}

#[derive(Display, Debug, Error)]
pub enum FCSFixedTimeError<E> {
    Native(E),
    Patterned(#[from] ParseWithTimePatternError),
}

#[derive(Debug, Error)]
#[error(
    "must be like 'hh:mm:ss' where 'hh' is hours (0-23) and 'mm', \
     'ss', 'tt' are minutes, seconds respectively (0-59)."
)]
pub struct FCSTimeError;

/// A time as used in the $BTIM/ETIM keys with 1/60 seconds (3.0 only)
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, From, Into)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct FCSTime60(pub NaiveTime);

impl FromStr for FCSTime60 {
    type Err = FCSTime60Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        NaiveTime::parse_from_str(s, "%H:%M:%S")
            .or_else(|_| match s.split(':').collect::<Vec<_>>()[..] {
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
        write!(f, "{base}:{cc:02}")
    }
}

#[derive(Debug, Error)]
#[error(
    "must be like 'hh:mm:ss[:tt]' where 'hh' is hours (0-23) and 'mm', \
     'ss', 'tt' are minutes, seconds, and optional fractional seconds \
     respectively (0-59)."
)]
pub struct FCSTime60Error;

/// A time as used in the $BTIM/ETIM keys with centiseconds (3.1+ only)
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, From, Into)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct FCSTime100(pub NaiveTime);

impl FromStr for FCSTime100 {
    type Err = FCSTime100Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        NaiveTime::parse_from_str(s, "%H:%M:%S")
            .or_else(|_| {
                static RE: LazyLock<Regex> = LazyLock::new(|| {
                    Regex::new(r"^([0-9]{2}):([0-9]{2}):([0-9]{2})\.([0-9]{2})$").unwrap()
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
        write!(f, "{base}.{cc:02}")
    }
}

#[derive(Debug, Error)]
#[error(
    "must be like 'hh:mm:ss[.cc]' where 'hh' is hours (0-23) 'mm' and 'ss' \
     are minutes and seconds respectively (0-59), and 'cc' is optional \
     centiseconds (0-99)."
)]
pub struct FCSTime100Error;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;

    #[test]
    fn str_timestamps2_0() {
        assert_from_to_str::<FCSTime>("23:58:00");
    }

    #[test]
    fn str_timestamps3_0() {
        assert_from_to_str_almost::<FCSTime60>("23:58:00", "23:58:00:00");
        assert_from_to_str::<FCSTime60>("23:58:00:30");
        // TODO should probably avoid stuff like this
        assert_from_to_str_almost::<FCSTime60>("23:58:00:13", "23:58:00:12");
        // this is an overflow
        assert!("23:58:00:60".parse::<FCSTime60>().is_err());
    }

    #[test]
    fn str_timestamps3_1() {
        assert_from_to_str_almost::<FCSTime100>("23:58:00", "23:58:00.00");
        assert_from_to_str::<FCSTime100>("23:58:00.30");
        // this is an overflow
        assert!("23:58:00.100".parse::<FCSTime100>().is_err());
    }
}

#[cfg(feature = "python")]
mod python {
    use super::{FCSDate, FCSTime, FCSTime100, FCSTime60, ReversedTimestampsError, Xtim};
    use crate::python::macros::{impl_from_py_transparent, impl_pyreflow_err};

    use pyo3::prelude::*;

    impl_pyreflow_err!(ReversedTimestampsError);

    impl_from_py_transparent!(FCSDate);
    impl_from_py_transparent!(FCSTime);
    impl_from_py_transparent!(FCSTime60);
    impl_from_py_transparent!(FCSTime100);

    impl<'py, T, const IS_ETIM: bool> FromPyObject<'py> for Xtim<IS_ETIM, T>
    where
        T: FromPyObject<'py>,
    {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            Ok(Self(ob.extract::<T>()?))
        }
    }
}

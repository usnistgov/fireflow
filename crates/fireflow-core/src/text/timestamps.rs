use crate::config::{AllowOptionalDropping, ConfigFlag as _, ReadLayoutConfig, StdTextReadConfig};
use crate::logging::{DeferredError, DeferredSwitchableErrors, LogResult, ResultExt as _};
use crate::text::deprecated::DeprecatedTimestampsRef;
use crate::text::lookup::{FromStrWith, OptKeyStError, OptMetarootKey, Optional, ParseKeyError};
use crate::text::optional::KeywordPairMaybe;
use crate::validated::keys::{Key, NonStdKeywords, NonStdKeywordsExt as _, StdKeywords};
use crate::validated::timepattern::ParseWithTimePatternError;

use type_families::ApplyOnce as _;

use chrono::{NaiveDate, NaiveTime, Timelike as _};
use derive_more::{AsRef, Display, From, FromStr, Into};
use derive_new::new;
use regex::Regex;
use std::fmt;
use std::mem;
use std::str::FromStr;
use std::sync::LazyLock;
use thiserror::Error;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr, FromInnerPyObject};

/// The $DATE/$BTIM/$ETIM keywords
///
/// The generic type parameter is meant to account for the fact that the time
/// types for different versions are all slightly different in their treatment
/// of sub-second time.
///
/// When $DATE is present, $BTIM and $ETIM are validated to be in the correct
/// order.
#[derive(Clone, AsRef, PartialEq, new)]
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
        Self::new(None, None, None)
    }
}

/// Wrapper for $BTIM timestamp
pub type Btim<T> = Xtim<false, T>;

/// Wrapper for $ETIM timestamp
pub type Etim<T> = Xtim<true, T>;

/// A wrapper for timestamps which encodes if it is the start or end
#[derive(Clone, Copy, Display, FromStr, From, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct Xtim<const IS_ETIM: bool, T>(pub T);

impl<const IS_ETIM: bool, T> FromStrWith for Xtim<IS_ETIM, T>
where
    T: FromStr + From<NaiveTime>,
{
    type Err = FCSFixedTimeError<<T as FromStr>::Err>;
    type Payload<'a> = ();

    fn from_str_with<'a>(s: &str, (): (), conf: &StdTextReadConfig) -> Result<Self, Self::Err> {
        let ret = if let Some(pat) = conf.time_pattern.as_ref() {
            pat.parse_str(s)?.into()
        } else {
            s.parse::<T>().map_err(FCSFixedTimeError::Native)?
        };
        Ok(Self(ret))
    }
}

/// The value of the $DATE key
#[derive(Clone, Copy, From, Into, AsRef, PartialEq, Display, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromInnerPyObject))]
#[display("{}", _0.format(FCS_DATE_FORMAT))]
pub struct FCSDate(pub NaiveDate);

impl<X> Timestamps<X> {
    pub fn try_new(
        btim: Option<Btim<X>>,
        etim: Option<Etim<X>>,
        date: Option<FCSDate>,
    ) -> DeferredError<Self, ReversedTimestampsError>
    where
        X: PartialOrd,
    {
        let ret = Self::new(btim, etim, date);
        if ret.valid() {
            LogResult::new_ok(ret)
        } else {
            LogResult::new_err(ReversedTimestampsError).set_err_value(ret)
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
        Timestamps::new(
            self.btim.map(|x| Xtim(f(x.0))),
            self.etim.map(|x| Xtim(f(x.0))),
            self.date,
        )
    }

    pub fn valid(&self) -> bool
    where
        X: PartialOrd,
    {
        if let (Some(b), Some(e), Some(_)) = (&self.btim, &self.etim, &self.date) {
            return b.0 < e.0;
        }
        true
    }

    pub(crate) fn lookup<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        conf: &C,
    ) -> DeferredSwitchableErrors<Self, AllowOptionalDropping, LookupTimestampsError<X, X::Err>>
    where
        Btim<X>: OptMetarootKey + Optional<Outer = Option<Btim<X>>>,
        Etim<X>: OptMetarootKey + Optional<Outer = Option<Etim<X>>>,
        X: PartialOrd + FromStr + From<NaiveTime> + fmt::Display,
        C: AsRef<ReadLayoutConfig> + AsRef<StdTextReadConfig>,
    {
        macro_rules! go {
            ($x:expr) => {
                $x.map_err(LookupTimestampsError::from)
                    .into_deferred_nowarn()
            };
        }
        let b = Btim::remove_or_transfer_root_opt_with(std, nonstd, (), conf);
        let e = Etim::remove_or_transfer_root_opt_with(std, nonstd, (), conf);
        let d = FCSDate::remove_or_transfer_root_opt_with(std, nonstd, (), conf);
        let rconf: &ReadLayoutConfig = conf.as_ref();
        let flag = rconf.allow_optional_dropping;
        go!(b)
            .zip_f3_once(go!(e), go!(d))
            .and_then_deferred(|(btim, etim, date)| {
                Self::try_new(btim, etim, date)
                    .map_errors(LookupTimestampsError::Reversed)
                    .map_err_value(|ret| {
                        // If creating the new timestamp object failed,
                        // optionally transfer component keys to nonstandard
                        if rconf.transfer_dropped_optional.is_set() {
                            ret.date
                                .as_ref()
                                .inspect(|&x| nonstd.insert_demoted_metaroot(x));
                            ret.btim
                                .as_ref()
                                .inspect(|&x| nonstd.insert_demoted_metaroot(x));
                            ret.etim
                                .as_ref()
                                .inspect(|&x| nonstd.insert_demoted_metaroot(x));
                        }
                        ret
                    })
                    .into_semigroup()
            })
            .nowarn_into_switchable(flag)
    }

    pub(crate) fn opt_keywords(&self) -> impl Iterator<Item = (String, String)>
    where
        Btim<X>: Key,
        Etim<X>: Key,
        Option<Btim<X>>: KeywordPairMaybe<Inner = Btim<X>>,
        Option<Etim<X>>: KeywordPairMaybe<Inner = Etim<X>>,
        X: Copy + fmt::Display,
    {
        let a = self.btim.metaroot_opt_pair();
        let b = self.etim.metaroot_opt_pair();
        let c = self.date.metaroot_opt_pair();
        [a, b, c].into_iter().filter_map(|(k, v)| v.map(|x| (k, x)))
    }
}

impl Timestamps<FCSTime100> {
    pub(crate) fn deprecated(&mut self) -> impl Iterator<Item = DeprecatedTimestampsRef<'_>> {
        let a = DeprecatedTimestampsRef::from(&mut self.btim);
        let b = DeprecatedTimestampsRef::from(&mut self.etim);
        let c = DeprecatedTimestampsRef::from(&mut self.date);
        [a, b, c].into_iter()
    }
}

/// Error when $ETIM occurs before $BTIM.
///
/// This can only happen when $DATE is also given, because otherwise it cannot
/// be assumed that $BTIM and $ETIM occur on the same day.
#[derive(Debug, Error)]
#[error("$ETIM is before $BTIM and $DATE is given")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::RelationalError))]
pub struct ReversedTimestampsError;

type TimestampsResult<T> = Result<T, ReversedTimestampsError>;

// the "%b" format is case-insensitive so this should work for "Jan", "JAN",
// "jan", "jaN", etc
const FCS_DATE_FORMAT: &str = "%d-%b-%Y";

impl FromStrWith for FCSDate {
    type Err = FCSDateError;
    type Payload<'a> = ();

    fn from_str_with(s: &str, (): (), conf: &StdTextReadConfig) -> Result<Self, Self::Err> {
        if let Some(pattern) = &conf.date_pattern {
            Self::parse_with_pattern(s, pattern.as_ref())
        } else {
            s.parse::<Self>()
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

/// Error when parsing [`FCSDate`] from string
#[derive(Debug, Error)]
#[error("must be like 'dd-mmm-yyyy'")]
pub struct FCSDateError;

/// A time as used in the $BTIM/ETIM keys without seconds (2.0 only)
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, From, Into, Display, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromInnerPyObject))]
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

/// Error when parsing [`Xtim`] from string
#[derive(Display, Debug, Error)]
pub enum FCSFixedTimeError<E> {
    Native(E),
    Patterned(#[from] ParseWithTimePatternError),
}

/// Error when parsing [`FCSTime`] as string
#[derive(Debug, Error)]
#[error(
    "must be like 'hh:mm:ss' where 'hh' is hours (0-23) and 'mm', \
     'ss', 'tt' are minutes, seconds respectively (0-59)."
)]
pub struct FCSTimeError;

/// A time as used in the $BTIM/ETIM keys with 1/60 seconds (3.0 only)
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, From, Into, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromInnerPyObject))]
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

/// Error when parsing [`FCSTime60`] from string
#[derive(Debug, Error)]
#[error(
    "must be like 'hh:mm:ss[:tt]' where 'hh' is hours (0-23) and 'mm', \
     'ss', 'tt' are minutes, seconds, and optional fractional seconds \
     respectively (0-59)."
)]
pub struct FCSTime60Error;

/// A time as used in the $BTIM/ETIM keys with centiseconds (3.1+ only)
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, From, Into, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromInnerPyObject))]
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
                // ASSUME these will never fail because we matched only digits above
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

/// Error when parsing [`FCSTime100`] from string
#[derive(Debug, Error)]
#[error(
    "must be like 'hh:mm:ss[.cc]' where 'hh' is hours (0-23) 'mm' and 'ss' \
     are minutes and seconds respectively (0-59), and 'cc' is optional \
     centiseconds (0-99)."
)]
pub struct FCSTime100Error;

/// Error when looking up $BTIM/$ETIM/$DATE from key/value pairs
#[derive(Display, Debug, Error, From)]
#[cfg_attr(
    feature = "python",
    derive(AllIntoPyErr),
    bound(ParseKeyError<FCSFixedTimeError<E>, Btim<T>, ()>: Into<Self>),
    bound(ParseKeyError<FCSFixedTimeError<E>, Etim<T>, ()>: Into<Self>)
)]
pub enum LookupTimestampsError<T, E> {
    Date(OptKeyStError<FCSDate>),
    Btim(ParseKeyError<FCSFixedTimeError<E>, Btim<T>, ()>),
    Etim(ParseKeyError<FCSFixedTimeError<E>, Etim<T>, ()>),
    Reversed(ReversedTimestampsError),
}

impl From<FCSTime> for FCSTime60 {
    fn from(value: FCSTime) -> Self {
        Self(value.0)
    }
}

impl From<FCSTime> for FCSTime100 {
    fn from(value: FCSTime) -> Self {
        Self(value.0)
    }
}

impl From<FCSTime60> for FCSTime {
    fn from(value: FCSTime60) -> Self {
        // ASSUME this will never fail since it only returns error if
        // nanoseconds are > 2e9
        Self(value.0.with_nanosecond(0).unwrap())
    }
}

impl From<FCSTime100> for FCSTime {
    fn from(value: FCSTime100) -> Self {
        // ASSUME this will never fail since it only returns error if
        // nanoseconds are > 2e9
        Self(value.0.with_nanosecond(0).unwrap())
    }
}

impl From<FCSTime60> for FCSTime100 {
    fn from(value: FCSTime60) -> Self {
        Self(value.0)
    }
}

impl From<FCSTime100> for FCSTime60 {
    fn from(value: FCSTime100) -> Self {
        Self(value.0)
    }
}

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
    use super::Xtim;

    use pyo3::prelude::*;

    impl<'py, T, const IS_ETIM: bool> FromPyObject<'py> for Xtim<IS_ETIM, T>
    where
        T: FromPyObject<'py>,
    {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            Ok(Self(ob.extract::<T>()?))
        }
    }
}

use crate::config::{ReadDataKeywordsConfig, ReadStdKeywordsConfig};
use crate::logging::{ErrorResult, LogResult, WarningsAndErrorsResult};
use crate::text::keyword_enum::{
    AsStdKeywordPair as _, Keyword0FromValue as _, OptRootKeyword, SplitKeyword0,
};
use crate::text::lookup::{
    DiagnosedKeyword, FromStrWith, OptKeyStError, OptMetarootKey, Optional, ParseKeyError,
};
use crate::validated::keys::{NonStdKeywords, NonStdKeywordsExt as _, StdKeywords};
use crate::validated::timepattern::ParseWithTimePatternError;

use fireflow_types::config::{BASE_TIME_FORMAT, DEFAULT_DATE_FORMAT, DEFAULT_TIME_FORMAT_2_0};
use fireflow_types::nonempty_string::{NEStr, NEString, ToDisplayNE, ambassador_impl_ToDisplayNE};

use ambassador::Delegate;
use chrono::{NaiveDate, NaiveTime, Timelike as _};
use derive_more::{AsRef, Display, From, FromStr, Into};
use derive_new::new;
use num_traits::cast::ToPrimitive as _;
use regex::Regex;
use thiserror::Error;

use std::mem;
use std::str::FromStr;
use std::sync::LazyLock;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr, FromInnerPyObject},
    fireflow_types::python as py,
};

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
#[derive(Clone, Copy, FromStr, From, PartialEq, Debug, Delegate)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[delegate(ToDisplayNE<'a>, generics = "'a")]
pub struct Xtim<const IS_ETIM: bool, T>(pub T);

impl<const IS_ETIM: bool, T> FromStrWith for Xtim<IS_ETIM, T>
where
    T: FromStr + From<NaiveTime>,
{
    type Err = FCSFixedTimeError<<T as FromStr>::Err>;
    type Payload<'a> = ();
    type Diagnostic = ();
    type Config = ReadStdKeywordsConfig;

    fn from_str_with<'a>(
        s: &NEStr,
        (): (),
        conf: &Self::Config,
    ) -> Result<DiagnosedKeyword<Self, ()>, Self::Err> {
        let ret = if let Some(pat) = conf.time_pattern.as_ref() {
            pat.parse_str(s.as_str())?.into()
        } else {
            s.parse::<T>().map_err(FCSFixedTimeError::Native)?
        };
        Ok(DiagnosedKeyword::new1(Self(ret)))
    }
}

/// The value of the $DATE key
#[derive(Clone, Copy, From, Into, AsRef, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromInnerPyObject))]
pub struct FCSDate(pub NaiveDate);

impl<'a> ToDisplayNE<'a> for FCSDate {
    type NE = NEString;
    fn to_ne(&'a self) -> Self::NE {
        NEString::try_from(self.0.format(DEFAULT_DATE_FORMAT).to_string())
            .expect("format should be non-empty")
    }
}

type OldInputs<X> = (Option<Btim<X>>, Option<Etim<X>>, Option<FCSDate>);

impl<X> Timestamps<X> {
    pub fn try_new(
        btim: Option<Btim<X>>,
        etim: Option<Etim<X>>,
        date: Option<FCSDate>,
    ) -> ErrorResult<Self, OldInputs<X>, ReversedTimestampsError>
    where
        X: PartialOrd,
    {
        let ret = Self::new(btim, etim, date);
        if ret.valid() {
            LogResult::new_ok(ret)
        } else {
            let old = (ret.btim, ret.etim, ret.date);
            LogResult::new_err(ReversedTimestampsError).set_err_value(old)
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
            return b.0 <= e.0;
        }
        true
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn lookup<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        dropped: &mut StdKeywords,
        conf: &C,
    ) -> WarningsAndErrorsResult<
        Self,
        (),
        LookupTimestampsError<X, X::Err>,
        LookupTimestampsError<X, X::Err>,
    >
    where
        Btim<X>: OptMetarootKey + Optional<Outer = Option<Btim<X>>>,
        Etim<X>: OptMetarootKey + Optional<Outer = Option<Etim<X>>>,
        X: PartialOrd + FromStr + From<NaiveTime>,
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadStdKeywordsConfig>,
        for<'a> OptRootKeyword<'a>: From<SplitKeyword0<Btim<X>>> + From<SplitKeyword0<Etim<X>>>,
    {
        macro_rules! go {
            ($x:expr) => {
                $x.map_switchable_errors(LookupTimestampsError::from)
                    .switchable_into_commutative()
                    .set_err_value(())
                    .into_semigroup::<Vec<_>, _>()
            };
        }
        let b = Btim::remove_or_drop_root_opt_with(std, nonstd, dropped, (), conf);
        let e = Etim::remove_or_drop_root_opt_with(std, nonstd, dropped, (), conf);
        let d = FCSDate::remove_or_drop_root_opt_with(std, nonstd, dropped, (), conf);
        let rconf: &ReadDataKeywordsConfig = conf.as_ref();
        go!(b)
            .zip3_commutative(go!(e), go!(d))
            .and_then_commutative(|(btim, etim, date)| {
                Self::try_new(btim.into_native(), etim.into_native(), date.into_native())
                    .map_errors(LookupTimestampsError::Reversed)
                    .map_err_value(|(old_btim, old_etim, old_date)| {
                        // If creating the new timestamp object failed,
                        // optionally transfer component keys to nonstandard
                        let bk = old_btim.map(OptRootKeyword::from_value);
                        let ek = old_etim.map(OptRootKeyword::from_value);
                        let dk = old_date.map(OptRootKeyword::from_value);
                        let kws = [bk, ek, dk].into_iter().flatten();
                        match rconf.process_optional_failure.is_demote_or_drop() {
                            Some(true) => {
                                for kk in kws {
                                    nonstd.insert_demoted_keyword(kk.into());
                                }
                            }
                            Some(false) => {
                                for k in kws {
                                    k.insert_unique(dropped);
                                }
                            }
                            None => (),
                        }
                    })
                    .into_semigroup()
                    .nowarn_into_warn()
            })
    }

    pub(crate) fn opt_keywords<'a>(&self) -> impl Iterator<Item = OptRootKeyword<'a>>
    where
        X: Copy,
        OptRootKeyword<'a>: From<SplitKeyword0<Btim<X>>> + From<SplitKeyword0<Etim<X>>>,
    {
        let a = self.btim.map(OptRootKeyword::from_value);
        let b = self.etim.map(OptRootKeyword::from_value);
        let c = self.date.map(OptRootKeyword::from_value);
        [a, b, c].into_iter().flatten()
    }
}

/// Error when $ETIM occurs before $BTIM.
///
/// This can only happen when $DATE is also given, because otherwise it cannot
/// be assumed that $BTIM and $ETIM occur on the same day.
#[derive(Debug, Error)]
#[error("$ETIM is before $BTIM and $DATE is given")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct ReversedTimestampsError;

type TimestampsResult<T> = Result<T, ReversedTimestampsError>;

impl FromStrWith for FCSDate {
    type Err = FCSDateError;
    type Payload<'a> = ();
    type Diagnostic = ();
    type Config = ReadStdKeywordsConfig;

    fn from_str_with(
        s: &NEStr,
        (): (),
        conf: &Self::Config,
    ) -> Result<DiagnosedKeyword<Self, ()>, Self::Err> {
        let ret = if let Some(pattern) = &conf.date_pattern {
            Self::parse_with_pattern(s.as_str(), pattern.as_ref())
        } else {
            s.parse::<Self>()
        };
        ret.map(DiagnosedKeyword::new1)
    }
}

impl FCSDate {
    fn parse_with_pattern(s: &str, pat: &str) -> Result<Self, FCSDateError> {
        match NaiveDate::parse_from_str(s, pat) {
            Ok(v) => Ok(Self(v)),
            Err(_) => Err(FCSDateError::Config(ConfigFCSDateError(pat.to_owned()))),
        }
    }
}

impl FromStr for FCSDate {
    type Err = FCSDateError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        NaiveDate::parse_from_str(s, DEFAULT_DATE_FORMAT)
            .or(Err(FCSDateError::Std(StdFCSDateError)))
            .map(Self)
    }
}

/// Error when parsing [`FCSDate`] from string
#[derive(Debug, Display, Error)]
pub enum FCSDateError {
    Std(StdFCSDateError),
    Config(ConfigFCSDateError),
}

/// Error when parsing [`FCSDate`] from string using [`crate::validated::datepattern::DatePattern`]
#[derive(Debug, Error)]
#[error("value is not like given pattern '{0}'")]
pub struct ConfigFCSDateError(String);

/// Error when parsing [`FCSDate`] from string using default format.
#[derive(Debug, Error)]
#[error("must be like 'dd-mmm-yyyy'")]
pub struct StdFCSDateError;

/// A time as used in the $BTIM/ETIM keys without seconds (2.0 only)
#[derive(Clone, Copy, Eq, PartialOrd, From, Into, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromInnerPyObject))]
pub struct FCSTime(pub NaiveTime);

impl<'a> ToDisplayNE<'a> for FCSTime {
    type NE = NEString;
    fn to_ne(&'a self) -> Self::NE {
        NEString::try_from(self.0.format(DEFAULT_TIME_FORMAT_2_0).to_string())
            .expect("time format should never be empty")
    }
}

impl PartialEq for FCSTime {
    fn eq(&self, other: &Self) -> bool {
        // ignore sub-seconds since these timestamps do not have this level of
        // precision in FCS files
        self.0.second() == other.0.second()
            && self.0.minute() == other.0.minute()
            && self.0.hour() == other.0.hour()
    }
}

impl FromStr for FCSTime {
    type Err = FCSTimeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        NaiveTime::parse_from_str(s, DEFAULT_TIME_FORMAT_2_0)
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
#[derive(Clone, Copy, Eq, PartialOrd, From, Into, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromInnerPyObject))]
pub struct FCSTime60(pub NaiveTime);

impl PartialEq for FCSTime60 {
    fn eq(&self, other: &Self) -> bool {
        // compare both timestamps with the precision used on disk when writing
        // a file, which means we need to put in 1/60 seconds to truncate any
        // extra precision the timestamps may have
        let go = |s: &Self| u64::from(s.0.nanosecond()) * 60 / 1_000_000_000;
        let cc0 = go(self);
        let cc1 = go(other);
        cc0 == cc1
            && self.0.second() == other.0.second()
            && self.0.minute() == other.0.minute()
            && self.0.hour() == other.0.hour()
    }
}

impl FromStr for FCSTime60 {
    type Err = FCSTime60Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        NaiveTime::parse_from_str(s, BASE_TIME_FORMAT)
            .or_else(|_| {
                let xs = s.split(':').collect::<Vec<_>>();
                match &xs[..] {
                    [s1, s2, s3, s4] => {
                        let hh: u32 = s1.parse().or(Err(FCSTime60Error))?;
                        let mm: u32 = s2.parse().or(Err(FCSTime60Error))?;
                        let ss: u32 = s3.parse().or(Err(FCSTime60Error))?;
                        let tt: u32 = s4.parse().or(Err(FCSTime60Error))?;
                        if tt > 59 {
                            return Err(FCSTime60Error);
                        }
                        // Use ceiling to map 1/60 seconds to nanoseconds to exactly
                        // mirror what we do in the Display impl where we use floor
                        //
                        // ASSUME this will not fail because we only allow 0-59
                        // which will map exactly to f32. Also, multiplying by 1e6
                        // should not overflow since the max exact integer of f32 is
                        // 2^23 (~8e6)
                        let nn = tt.to_f32().unwrap() * 1_000_000.0 / 60.0;
                        let nn_ = nn.ceil().to_u32().unwrap();
                        NaiveTime::from_hms_micro_opt(hh, mm, ss, nn_).ok_or(FCSTime60Error)
                    }
                    _ => Err(FCSTime60Error),
                }
            })
            .map(FCSTime60)
    }
}

impl<'a> ToDisplayNE<'a> for FCSTime60 {
    type NE = NEString;
    fn to_ne(&'a self) -> Self::NE {
        let mut s = NEString::try_from(self.0.format(BASE_TIME_FORMAT).to_string())
            .expect("time format should never be empty");
        let cc = format!("{:02}", u64::from(self.0.nanosecond()) * 60 / 1_000_000_000);
        s.push(':');
        s.push_str(cc.as_str());
        s
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
#[derive(Clone, Copy, Eq, PartialOrd, From, Into, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromInnerPyObject))]
pub struct FCSTime100(pub NaiveTime);

impl PartialEq for FCSTime100 {
    fn eq(&self, other: &Self) -> bool {
        // compare both timestamps with the precision used on disk when writing
        // a file, which means we need to put in centiseconds to truncate any
        // extra precision the timestamps may have
        let go = |s: &Self| s.0.nanosecond() / 10_000_000;
        let cc0 = go(self);
        let cc1 = go(other);
        cc0 == cc1
            && self.0.second() == other.0.second()
            && self.0.minute() == other.0.minute()
            && self.0.hour() == other.0.hour()
    }
}

impl FromStr for FCSTime100 {
    type Err = FCSTime100Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        NaiveTime::parse_from_str(s, BASE_TIME_FORMAT)
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

impl<'a> ToDisplayNE<'a> for FCSTime100 {
    type NE = NEString;
    fn to_ne(&'a self) -> Self::NE {
        let mut s = NEString::try_from(self.0.format(BASE_TIME_FORMAT).to_string())
            .expect("time format should never be empty");
        let cc = format!("{:02}", self.0.nanosecond() / 10_000_000);
        s.push('.');
        s.push_str(cc.as_str());
        s
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
        assert_from_to_str::<FCSTime60>("23:58:00:13");
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

    impl<'a, 'py, T, const IS_ETIM: bool> FromPyObject<'a, 'py> for Xtim<IS_ETIM, T>
    where
        T: FromPyObject<'a, 'py>,
    {
        type Error = T::Error;
        fn extract(obj: Borrowed<'a, 'py, PyAny>) -> Result<Self, Self::Error> {
            obj.extract::<T>().map(Self)
        }
    }
}

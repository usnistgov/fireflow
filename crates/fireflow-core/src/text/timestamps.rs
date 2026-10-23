use crate::config::EvaledReadStdKeywordsConfig;
use crate::logging::{ErrorResult, LogResult};
use crate::std_index::index::StdLookupTx;
use crate::text::keyword_enum::{Keyword0FromValue as _, OptRootKeyword, SplitKeyword};
use crate::text::keywords::ValueToStdKey;
use crate::text::lookup::{Diagnosed, FromStrWith, OptStKeyError, OptValue};

use fireflow_types::config::{
    BASE_TIME_FORMAT, DEFAULT_DATE_FORMAT, DEFAULT_TIME_FORMAT_2_0, ReadDataKeywordsConfig,
};
use fireflow_types::datepattern::DatePattern;
use fireflow_types::timepattern::{ParseWithTimePatternError, TimePattern};
use nonempty::{NEStr, NEString, ToDisplayNE, ambassador_impl_ToDisplayNE};

use ambassador::Delegate;
use chrono::{NaiveDate, NaiveTime, Timelike as _};
use derive_more::{AsRef, Display, From, FromStr, Into};
use derive_new::new;
use num_traits::cast::ToPrimitive as _;
use regex::Regex;
use thiserror::Error;

use std::cmp::Ordering;
use std::mem;
use std::str::FromStr;
use std::sync::LazyLock;

#[cfg(feature = "serde")]
use serde::Serialize;

use super::lookup::ParseKeyError_;

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

#[derive(new, Default)]
pub(crate) struct TimestampsDiagnostics {
    pub(crate) btim: Option<TimePattern>,
    pub(crate) etim: Option<TimePattern>,
    pub(crate) date: Option<DatePattern>,
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
    type Diagnostic = Option<TimePattern>;
    type Config = EvaledReadStdKeywordsConfig;

    fn from_str_with<'a>(
        s: &NEStr,
        (): (),
        conf: &Self::Config,
    ) -> Result<Diagnosed<Self, Self::Diagnostic>, Self::Err> {
        if let Some(pat) = conf.time_pattern.as_ref() {
            pat.parse_str(s.as_str())
                .map_err(FCSFixedTimeError::Patterned)
                .map(|x| Diagnosed::new(Self(x.into()), Some(pat.clone())))
        } else {
            s.parse::<T>()
                .map_err(FCSFixedTimeError::Native)
                .map(|x| Diagnosed::new(Self(x), None))
        }
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
        match Self::try_new_inner(btim, etim, date) {
            Ok(x) => LogResult::new_ok(x),
            Err((old, e)) => LogResult::new_err(e).set_err_value(old),
        }
    }

    fn try_new_inner(
        btim: Option<Btim<X>>,
        etim: Option<Etim<X>>,
        date: Option<FCSDate>,
    ) -> Result<Self, (OldInputs<X>, ReversedTimestampsError)>
    where
        X: PartialOrd,
    {
        let ret = Self::new(btim, etim, date);
        if ret.valid() {
            Ok(ret)
        } else {
            Err(((ret.btim, ret.etim, ret.date), ReversedTimestampsError))
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
        kws: &mut StdLookupTx,
        conf: &C,
    ) -> (
        Diagnosed<Self, TimestampsDiagnostics>,
        LookupTimestampsErrorTable<X, X::Err>,
    )
    where
        Btim<X>: OptValue<Outer = Option<Btim<X>>> + ValueToStdKey<Index = ()>,
        Etim<X>: OptValue<Outer = Option<Etim<X>>> + ValueToStdKey<Index = ()>,
        X: PartialOrd + FromStr + From<NaiveTime>,
        C: AsRef<ReadDataKeywordsConfig> + AsRef<EvaledReadStdKeywordsConfig>,
        for<'a> OptRootKeyword<'a>: From<SplitKeyword<Btim<X>>> + From<SplitKeyword<Etim<X>>>,
    {
        let (btim, be) = Btim::remove_or_drop_root_opt_with(kws, (), conf);
        let (etim, ee) = Etim::remove_or_drop_root_opt_with(kws, (), conf);
        let (date, de) = FCSDate::remove_or_drop_root_opt_with(kws, (), conf);
        let rconf: &ReadDataKeywordsConfig = conf.as_ref();
        let diag = TimestampsDiagnostics::new(btim.diagnostic, etim.diagnostic, date.diagnostic);
        let (ts, rev_e) = match Self::try_new_inner(btim.inner, etim.inner, date.inner) {
            Ok(ts) => (ts, None),
            Err((old, rev_e)) => {
                // If creating the new timestamp object failed,
                // optionally transfer component keys to nonstandard
                let bk = old.0.map(|v| v.std0_());
                let ek = old.1.map(|v| v.std0_());
                let dk = old.2.map(|v| v.std0_());
                let flag = rconf.process_optional_failure;
                for k in [bk, ek, dk].into_iter().flatten() {
                    kws.set_failure_flag(&k, flag);
                }
                (Self::default(), Some(rev_e))
            }
        };
        let es = LookupTimestampsErrorTable::new(be, ee, de, rev_e);
        (Diagnosed::new(ts, diag), es)
    }

    pub(crate) fn opt_keywords<'a>(&self) -> impl Iterator<Item = OptRootKeyword<'a>>
    where
        X: Copy,
        Btim<X>: ValueToStdKey<Index = ()>,
        Etim<X>: ValueToStdKey<Index = ()>,
        OptRootKeyword<'a>: From<SplitKeyword<Btim<X>>> + From<SplitKeyword<Etim<X>>>,
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
#[derive(Debug, Error, PartialEq, Clone)]
#[error("$ETIM is before $BTIM and $DATE is given")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct ReversedTimestampsError;

type TimestampsResult<T> = Result<T, ReversedTimestampsError>;

impl FromStrWith for FCSDate {
    type Err = FCSDateError;
    type Payload<'a> = ();
    type Diagnostic = Option<DatePattern>;
    type Config = EvaledReadStdKeywordsConfig;

    fn from_str_with(
        s: &NEStr,
        (): (),
        conf: &Self::Config,
    ) -> Result<Diagnosed<Self, Self::Diagnostic>, Self::Err> {
        if let Some(pattern) = &conf.date_pattern {
            Self::parse_with_pattern(s.as_str(), pattern.as_ref())
                .map(|x| Diagnosed::new(x, Some(pattern.clone())))
        } else {
            s.parse::<Self>().map(|x| Diagnosed::new(x, None))
        }
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
#[derive(Debug, Display, Error, PartialEq, Eq, Clone)]
pub enum FCSDateError {
    Std(StdFCSDateError),
    Config(ConfigFCSDateError),
}

/// Error when parsing [`FCSDate`] from string using [`fireflow_types::datepattern::DatePattern`]
#[derive(Debug, Error, PartialEq, Eq, Clone)]
#[error("value is not like given pattern '{0}'")]
pub struct ConfigFCSDateError(String);

/// Error when parsing [`FCSDate`] from string using default format.
#[derive(Debug, Error, PartialEq, Eq, Clone)]
#[error("must be like 'dd-mmm-yyyy'")]
pub struct StdFCSDateError;

/// A time as used in the $BTIM/ETIM keys without seconds (2.0 only)
#[derive(Clone, Copy, Eq, From, Into, Debug)]
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

// For Eq/Ord, only compare h/m/s and not subseconds to guarantee that any
// non-zero subseconds. This must be done in both place to guarantee we won't
// have any contradictions such as a == b && a > b being true.

impl PartialEq for FCSTime {
    fn eq(&self, other: &Self) -> bool {
        self.tuple() == other.tuple()
    }
}

impl PartialOrd for FCSTime {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FCSTime {
    fn cmp(&self, other: &Self) -> Ordering {
        self.tuple().cmp(&other.tuple())
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

impl FCSTime {
    fn tuple(self) -> (u32, u32, u32) {
        (self.0.hour(), self.0.minute(), self.0.second())
    }
}

/// Error when parsing [`Xtim`] from string
#[derive(Display, Debug, Error, Clone, PartialEq)]
pub enum FCSFixedTimeError<E> {
    Native(E),
    Patterned(#[from] ParseWithTimePatternError),
}

/// Error when parsing [`FCSTime`] as string
#[derive(Debug, Error, PartialEq, Eq, Clone)]
#[error(
    "must be like 'hh:mm:ss' where 'hh' is hours (0-23) and 'mm', \
     'ss', 'tt' are minutes, seconds respectively (0-59)."
)]
pub struct FCSTimeError;

/// A time as used in the $BTIM/ETIM keys with 1/60 seconds (3.0 only)
#[derive(Clone, Copy, Eq, From, Into, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromInnerPyObject))]
pub struct FCSTime60(pub NaiveTime);

// For Eq/Ord, only compare h/m/s and not subseconds to guarantee that any
// non-zero subseconds. This must be done in both place to guarantee we won't
// have any contradictions such as a == b && a > b being true.

impl PartialEq for FCSTime60 {
    fn eq(&self, other: &Self) -> bool {
        self.tuple() == other.tuple()
    }
}

impl PartialOrd for FCSTime60 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FCSTime60 {
    fn cmp(&self, other: &Self) -> Ordering {
        self.tuple().cmp(&other.tuple())
    }
}

impl FCSTime60 {
    fn tuple(self) -> (u32, u32, u32, u64) {
        (
            self.0.hour(),
            self.0.minute(),
            self.0.second(),
            self.subseconds(),
        )
    }

    fn subseconds(self) -> u64 {
        // first mod is to filter out leap seconds
        u64::from(self.0.nanosecond()) % 1_000_000_000 * 60 / 1_000_000_000
    }
}

impl FromStr for FCSTime60 {
    type Err = FCSTime60Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.starts_with("24") || s.starts_with("23:60") | s.starts_with("23:59:60") {
            // Pre-filter out these edge case since chrono will actually parse
            // these and return as if they are valid (which they aren't or at
            // least they shouldn't be in this context)
            Err(FCSTime60Error)
        } else if let Ok(ts) = NaiveTime::parse_from_str(s, BASE_TIME_FORMAT) {
            Ok(Self(ts))
        } else {
            let xs = s.split(':').collect::<Vec<_>>();
            match &xs[..] {
                [s1, s2, s3, s4] => {
                    let hh: u32 = s1.parse().or(Err(FCSTime60Error))?;
                    let mm: u32 = s2.parse().or(Err(FCSTime60Error))?;
                    let ss: u32 = s3.parse().or(Err(FCSTime60Error))?;
                    let tt: u32 = s4.parse().or(Err(FCSTime60Error))?;
                    if tt > 59 {
                        Err(FCSTime60Error)
                    } else {
                        // Use ceiling to map 1/60 seconds to
                        // nanoseconds to exactly mirror what we do in
                        // the Display impl where we use floor
                        //
                        // ASSUME this will not fail because we only
                        // allow 0-59 which will map exactly to f32.
                        // Also, multiplying by 1e6 should not overflow
                        // since the max exact integer of f32 is 2^23
                        // (~8e6)
                        let nn = tt.to_f32().unwrap() * 1_000_000.0 / 60.0;
                        let nn_ = nn.ceil().to_u32().unwrap();
                        NaiveTime::from_hms_micro_opt(hh, mm, ss, nn_)
                            .ok_or(FCSTime60Error)
                            .map(Self)
                    }
                }
                _ => Err(FCSTime60Error),
            }
        }
    }
}

impl<'a> ToDisplayNE<'a> for FCSTime60 {
    type NE = NEString;
    fn to_ne(&'a self) -> Self::NE {
        let mut s = NEString::try_from(self.0.format(BASE_TIME_FORMAT).to_string())
            .expect("time format should never be empty");
        let cc = format!("{:02}", self.subseconds());
        s.push(':');
        s.push_str(cc.as_str());
        s
    }
}

/// Error when parsing [`FCSTime60`] from string
#[derive(Debug, Error, PartialEq, Eq, Clone)]
#[error(
    "must be like 'hh:mm:ss[:tt]' where 'hh' is hours (0-23) and 'mm', \
     'ss', 'tt' are minutes, seconds, and optional fractional seconds \
     respectively (0-59)."
)]
pub struct FCSTime60Error;

/// A time as used in the $BTIM/ETIM keys with centiseconds (3.1+ only)
#[derive(Clone, Copy, Eq, From, Into, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromInnerPyObject))]
pub struct FCSTime100(pub NaiveTime);

// For Eq/Ord, only compare h/m/s and not subseconds to guarantee that any
// non-zero subseconds. This must be done in both place to guarantee we won't
// have any contradictions such as a == b && a > b being true.

impl PartialEq for FCSTime100 {
    fn eq(&self, other: &Self) -> bool {
        self.tuple() == other.tuple()
    }
}

impl PartialOrd for FCSTime100 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FCSTime100 {
    fn cmp(&self, other: &Self) -> Ordering {
        self.tuple().cmp(&other.tuple())
    }
}

impl FCSTime100 {
    fn tuple(self) -> (u32, u32, u32, u64) {
        (
            self.0.hour(),
            self.0.minute(),
            self.0.second(),
            self.centiseconds(),
        )
    }

    fn centiseconds(self) -> u64 {
        // first mod is to filter out leap seconds
        u64::from(self.0.nanosecond()) % 1_000_000_000 / 10_000_000
    }
}

impl FromStr for FCSTime100 {
    type Err = FCSTime100Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.starts_with("24") || s.starts_with("23:60") | s.starts_with("23:59:60") {
            // Pre-filter out these edge case since chrono will actually parse
            // these and return as if they are valid (which they aren't or at
            // least they shouldn't be in this context)
            Err(FCSTime100Error)
        } else if let Ok(ts) = NaiveTime::parse_from_str(s, BASE_TIME_FORMAT) {
            Ok(Self(ts))
        } else {
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
            NaiveTime::from_hms_milli_opt(hh, mm, ss, tt * 10)
                .ok_or(FCSTime100Error)
                .map(Self)
        }
    }
}

impl<'a> ToDisplayNE<'a> for FCSTime100 {
    type NE = NEString;
    fn to_ne(&'a self) -> Self::NE {
        let mut s = NEString::try_from(self.0.format(BASE_TIME_FORMAT).to_string())
            .expect("time format should never be empty");
        let cc = format!("{:02}", self.centiseconds());
        s.push('.');
        s.push_str(cc.as_str());
        s
    }
}

/// Error when parsing [`FCSTime100`] from string
#[derive(Debug, Error, PartialEq, Eq, Clone)]
#[error(
    "must be like 'hh:mm:ss[.cc]' where 'hh' is hours (0-23) 'mm' and 'ss' \
     are minutes and seconds respectively (0-59), and 'cc' is optional \
     centiseconds (0-99)."
)]
pub struct FCSTime100Error;

/// Error when looking up $BTIM/$ETIM/$DATE from key/value pairs
#[derive(Display, Debug, Error, From, PartialEq, Clone)]
#[cfg_attr(
    feature = "python",
    derive(AllIntoPyErr),
    bound(ParseKeyError_<FCSFixedTimeError<E>, Btim<T>, ()>: Into<Self>),
    bound(ParseKeyError_<FCSFixedTimeError<E>, Etim<T>, ()>: Into<Self>)
)]
pub enum LookupTimestampsError<T, E> {
    Date(OptStKeyError<FCSDate>),
    Btim(ParseKeyError_<FCSFixedTimeError<E>, Btim<T>, ()>),
    Etim(ParseKeyError_<FCSFixedTimeError<E>, Etim<T>, ()>),
    Reversed(ReversedTimestampsError),
}

#[derive(new)]
pub(crate) struct LookupTimestampsErrorTable<T, E> {
    btim: Option<ParseKeyError_<FCSFixedTimeError<E>, Btim<T>, ()>>,
    etim: Option<ParseKeyError_<FCSFixedTimeError<E>, Etim<T>, ()>>,
    date: Option<OptStKeyError<FCSDate>>,
    // TODO this is mutually exclusive from the others
    reversed: Option<ReversedTimestampsError>,
}

impl<T, E> LookupTimestampsErrorTable<T, E> {
    pub(crate) fn into_warnings(self) -> impl Iterator<Item = LookupTimestampsError<T, E>> {
        let btim = self.btim.into_iter().map(LookupTimestampsError::Btim);
        let etim = self.etim.into_iter().map(LookupTimestampsError::Etim);
        let date = self.date.into_iter().map(LookupTimestampsError::from);
        let reversed = self.reversed.into_iter().map(LookupTimestampsError::from);
        btim.chain(etim).chain(date).chain(reversed)
    }
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

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn str_timestamps2_0(
            s in (0_usize..23, 0_usize..59, 0_usize..59)
                .prop_map(|(h, m, s)| format!("{h:02}:{m:02}:{s:02}"))
        )
        {
            assert_from_to_str::<FCSTime>(s.as_str());
        }
    }

    proptest! {
        #[test]
        fn str_timestamps3_0(
            s in (0_usize..23, 0_usize..59, 0_usize..59, 0_usize..59)
                .prop_map(|(h, m, s, f)| format!("{h:02}:{m:02}:{s:02}:{f:02}"))
        )
        {
            assert_from_to_str::<FCSTime60>(s.as_str());
        }
    }

    #[test]
    fn str_timestamps3_0_overflow_subseconds() {
        assert_eq!("23:58:00:60".parse::<FCSTime60>(), Err(FCSTime60Error));
    }

    #[test]
    fn str_timestamps3_0_overflow_seconds_long() {
        assert_eq!("23:59:60".parse::<FCSTime60>(), Err(FCSTime60Error));
        assert_eq!("23:59:60:00".parse::<FCSTime60>(), Err(FCSTime60Error));
    }

    #[test]
    fn str_timestamps3_0_overflow_minutes_long() {
        assert_eq!("23:60:00".parse::<FCSTime60>(), Err(FCSTime60Error));
        assert_eq!("23:60:00:00".parse::<FCSTime60>(), Err(FCSTime60Error));
    }

    #[test]
    fn str_timestamps3_0_overflow_hours() {
        assert_eq!("24:00:00".parse::<FCSTime60>(), Err(FCSTime60Error));
        assert_eq!("24:00:00:00".parse::<FCSTime60>(), Err(FCSTime60Error));
    }

    proptest! {
        #[test]
        fn str_timestamps3_1(
            s in (0_usize..23, 0_usize..59, 0_usize..59, 0_usize..99)
                .prop_map(|(h, m, s, f)| format!("{h:02}:{m:02}:{s:02}.{f:02}"))
        )
        {
            assert_from_to_str::<FCSTime100>(s.as_str());
        }
    }

    #[test]
    fn str_timestamps3_1_overflow() {
        assert_eq!("23:58:00.100".parse::<FCSTime100>(), Err(FCSTime100Error));
    }

    #[test]
    fn str_timestamps3_1_overflow_seconds_long() {
        assert_eq!("23:59:60".parse::<FCSTime100>(), Err(FCSTime100Error));
        assert_eq!("23:59:60.00".parse::<FCSTime100>(), Err(FCSTime100Error));
    }

    #[test]
    fn str_timestamps3_1_overflow_minutes_long() {
        assert_eq!("23:60:00".parse::<FCSTime100>(), Err(FCSTime100Error));
        assert_eq!("23:60:00.00".parse::<FCSTime100>(), Err(FCSTime100Error));
    }

    #[test]
    fn str_timestamps3_1_overflow_hours() {
        assert_eq!("24:00:00".parse::<FCSTime100>(), Err(FCSTime100Error));
        assert_eq!("24:00:00.00".parse::<FCSTime100>(), Err(FCSTime100Error));
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

use crate::config::{
    ConfigFlag as _, ProcessOptionalFailure, ReadDataKeywordsConfig, ReadStdKeywordsConfig,
};
use crate::core::UnitaryKeyLossError;
use crate::logging::{DeferredError, DeferredSwitchableErrors, LogResult, ResultExt as _};
use crate::text::lookup::{DiagnosedKeyword, FromStrWith, OptKeyStError, OptMetarootKey as _};
use crate::text::optional::KeywordPairMaybe as _;
use crate::validated::keys::{NonStdKeywords, NonStdKeywordsExt as _, StdKeywords};

use type_families::{ApplyOnce as _, BifunctorOnce as _};

use chrono::{DateTime, FixedOffset, Local, MappedLocalTime, NaiveDateTime, TimeZone as _};
use derive_more::{AsRef, Display, From, Into};
use std::mem;
use thiserror::Error;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr, FromInnerPyObject};

/// The $BEGINDATETIME and $ENDDATETIME keys (3.2+).
///
/// These are validated to be in order.
#[derive(Clone, Default, AsRef, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct Datetimes {
    #[as_ref(Option<BeginDateTime>)]
    begin: Option<BeginDateTime>,

    #[as_ref(Option<EndDateTime>)]
    end: Option<EndDateTime>,
}

/// The $BEGINDATETIME key.
#[derive(Clone, Copy, From, Into, Display, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromInnerPyObject))]
#[from(DateTime<FixedOffset>, FCSDateTime)]
#[into(DateTime<FixedOffset>, FCSDateTime)]
pub struct BeginDateTime(pub FCSDateTime);

/// The $ENDDATETIME key.
#[derive(Clone, Copy, From, Into, Display, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromInnerPyObject))]
#[from(DateTime<FixedOffset>, FCSDateTime)]
#[into(DateTime<FixedOffset>, FCSDateTime)]
pub struct EndDateTime(pub FCSDateTime);

/// A datetime as used in the $(BEGIN|END)DATETIME keys (3.2+ only)
#[derive(Clone, Copy, From, Into, PartialEq, Debug, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromInnerPyObject))]
#[display("{}", _0.format("%Y-%m-%dT%H:%M:%S%.f%:z"))]
pub struct FCSDateTime(pub DateTime<FixedOffset>);

impl Datetimes {
    #[must_use]
    pub fn try_new(
        begin: Option<BeginDateTime>,
        end: Option<EndDateTime>,
    ) -> DeferredError<Self, ReversedDatetimesError> {
        let ret = Self { begin, end };
        if ret.valid() {
            LogResult::new_ok(ret)
        } else {
            LogResult::new_err(ReversedDatetimesError).set_err_value(ret)
        }
    }

    pub fn set_begin(&mut self, time: Option<BeginDateTime>) -> Result<(), ReversedDatetimesError> {
        let tmp = mem::replace(&mut self.begin, time);
        if !self.valid() {
            self.begin = tmp;
            return Err(ReversedDatetimesError);
        }
        Ok(())
    }

    pub fn set_end(&mut self, time: Option<EndDateTime>) -> Result<(), ReversedDatetimesError> {
        let tmp = mem::replace(&mut self.end, time);
        if !self.valid() {
            self.end = tmp;
            return Err(ReversedDatetimesError);
        }
        Ok(())
    }

    #[must_use]
    pub fn valid(&self) -> bool {
        if let (Some(b), Some(e)) = (&self.begin, &self.end) {
            (b.0).0 <= (e.0).0
        } else {
            true
        }
    }

    pub(crate) fn lookup<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        conf: &C,
    ) -> DeferredSwitchableErrors<Self, ProcessOptionalFailure, LookupDatetimesError>
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadStdKeywordsConfig>,
    {
        let b = BeginDateTime::remove_or_transfer_root_opt_with(std, nonstd, (), conf)
            .map_err(LookupDatetimesError::from)
            .into_deferred_nowarn();
        let e = EndDateTime::remove_or_transfer_root_opt_with(std, nonstd, (), conf)
            .map_err(LookupDatetimesError::from)
            .into_deferred_nowarn();
        let rconf: &ReadDataKeywordsConfig = conf.as_ref();
        let flag = rconf.process_optional_failure;
        b.zip_f2_once(e)
            .and_then_deferred(|(begin, end)| {
                Self::try_new(begin.into_native(), end.into_native())
                    .map_errors(LookupDatetimesError::from)
                    .map_err_value(|ret| {
                        // If creating the new datetime object failed,
                        // optionally transfer component keys to nonstandard
                        if rconf.process_optional_failure.is_demote() {
                            ret.begin.inspect(|x| nonstd.insert_demoted_metaroot(x));
                            ret.end.inspect(|x| nonstd.insert_demoted_metaroot(x));
                        }
                        ret
                    })
                    .into_semigroup()
            })
            .nowarn_into_switchable(flag)
    }

    pub(crate) fn opt_keywords(&self) -> impl Iterator<Item = (String, String)> {
        [self.begin.metaroot_opt_pair(), self.end.metaroot_opt_pair()]
            .into_iter()
            .filter_map(|(k, v)| v.map(|x| (k, x)))
    }

    pub(crate) fn loss_errors(&self) -> impl Iterator<Item = DatetimeLossError> {
        let x0 = UnitaryKeyLossError::<BeginDateTime>::default();
        let y0 = self.begin.is_some().then_some(x0.into());
        let x1 = UnitaryKeyLossError::<EndDateTime>::default();
        let y1 = self.end.is_some().then_some(x1.into());
        [y0, y1].into_iter().flatten()
    }
}

macro_rules! impl_from_str_with {
    ($t:ident) => {
        impl FromStrWith for $t {
            type Err = FCSDateTimeError;
            type Payload<'a> = ();
            type Diagnostic = ();

            fn from_str_with(
                s: &str,
                (): (),
                conf: &ReadStdKeywordsConfig,
            ) -> Result<DiagnosedKeyword<Self, ()>, Self::Err> {
                FCSDateTime::from_str_with(s, (), conf).map(|x| x.first_once(Self))
            }
        }
    };
}

impl_from_str_with!(BeginDateTime);
impl_from_str_with!(EndDateTime);

impl FromStrWith for FCSDateTime {
    type Err = FCSDateTimeError;
    type Payload<'a> = ();
    type Diagnostic = ();

    fn from_str_with(
        s: &str,
        (): (),
        conf: &ReadStdKeywordsConfig,
    ) -> Result<DiagnosedKeyword<Self, ()>, Self::Err> {
        if let Some(pat) = conf.datetime_pattern.as_ref() {
            // first, try the given alternative format if it exists
            DateTime::parse_from_str(s, pat.as_str())
                .map(Self)
                .map(DiagnosedKeyword::new1)
                .map_err(|_| FCSDateTimeError::AltFormat(pat.to_owned()))
        } else if let Ok(naive) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f") {
            // next, try to parse without a timezone, defaulting to localtime and
            // converting to a fixed offset
            if conf.disallow_localtime.is_set() {
                Err(FCSDateTimeError::Localtime)
            } else {
                match Local::now().timezone().from_local_datetime(&naive) {
                    MappedLocalTime::Single(t) => {
                        Ok(DiagnosedKeyword::new1(Self(t.fixed_offset())))
                    }
                    MappedLocalTime::Ambiguous(_, _) => Err(FCSDateTimeError::Fold),
                    MappedLocalTime::None => Err(FCSDateTimeError::Gap),
                }
            }
        } else {
            // If zone information is present, try any number of formats which
            // are valid and mostly equivalent which contain the timezone
            let formats = [
                "%Y-%m-%dT%H:%M:%S%.f",
                "%Y-%m-%dT%H:%M:%S%.f%#z",
                "%Y-%m-%dT%H:%M:%S%.f%:z",
                "%Y-%m-%dT%H:%M:%S%.f%:::z",
            ];
            for f in formats {
                if let Ok(t) = DateTime::parse_from_str(s, f) {
                    return Ok(DiagnosedKeyword::new1(Self(t)));
                }
            }
            Err(FCSDateTimeError::Format)
        }
    }
}

/// Error when $ENDDATETIME occurs before $BEGINDATETIME
#[derive(Debug, Error)]
#[error("$BEGINDATETIME is after $ENDDATETIME")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::RelationalError))]
pub struct ReversedDatetimesError;

/// Error when parsing [`FCSDateTime`] from string
#[derive(Debug, Error)]
pub enum FCSDateTimeError {
    #[error("must be formatted like 'yyyy-mm-ddThh:mm:ss[TZD]'")]
    Format,
    #[error("could not parse with pattern '{0}'")]
    AltFormat(String),
    #[error(
        "timestamp parsed using localtime due to missing timezone, but this time \
         occurred when clock was turned backward which resulted in ambiguous UTC time"
    )]
    Fold,
    #[error(
        "timestamp parsed using localtime due to missing timezone, but this time \
         occurred when clock was turned forward and could not be mapped to UTC"
    )]
    Gap,
    #[error("using localtime because no timezone specified, which is ambiguous")]
    Localtime,
}

/// Error when parsing $BEGINDATETIME and $ENDDATETIME
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupDatetimesError {
    Begindatetime(OptKeyStError<BeginDateTime>),
    Enddatetime(OptKeyStError<EndDateTime>),
    Datetime(ReversedDatetimesError),
}

/// Error when $BEGINDATETIME or $ENDDATETIME are dropped due to version change
#[derive(From, Display, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum DatetimeLossError {
    Begin(UnitaryKeyLossError<BeginDateTime>),
    End(UnitaryKeyLossError<EndDateTime>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;
    use std::str::FromStr;

    impl FromStr for FCSDateTime {
        type Err = FCSDateTimeError;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            let conf = ReadStdKeywordsConfig::default();
            Self::from_str_with(s, (), &conf)
        }
    }

    #[test]
    fn str_to_datetime_local() {
        assert!("2112-01-01T00:00:00.0".parse::<FCSDateTime>().is_ok());
    }

    #[test]
    fn datetime_utc() {
        assert_from_to_str_almost::<FCSDateTime>(
            "2112-01-01T00:00:00.0Z",
            "2112-01-01T00:00:00+00:00",
        );
    }

    #[test]
    fn datetime_hh() {
        assert_from_to_str_almost::<FCSDateTime>(
            "2112-01-01T00:00:00.0+01",
            "2112-01-01T00:00:00+01:00",
        );
    }

    #[test]
    fn datetime_hh_mm() {
        assert_from_to_str_almost::<FCSDateTime>(
            "2112-01-01T00:00:00.0+00:01",
            "2112-01-01T00:00:00+00:01",
        );
    }

    #[test]
    fn datetime_hhmm() {
        assert_from_to_str_almost::<FCSDateTime>(
            "2112-01-01T00:00:00.0+0001",
            "2112-01-01T00:00:00+00:01",
        );
    }
}

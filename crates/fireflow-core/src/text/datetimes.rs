use crate::config::{ConfigFlag as _, EvaledReadDataKeywordsConfig, EvaledReadStdKeywordsConfig};
use crate::logging::{ErrorResult, LogResult, WarningsAndErrorsResult};
use crate::text::keyword_enum::{AsStdKeywordPair as _, Keyword0FromValue as _, OptRootKeyword};
use crate::text::lookup::{Diagnosed, FromStrWith, OptKeyStError, OptMetarootKey as _};
use crate::validated::keys::{NonStdKeywordsExt as _, StdKeywords, ValidKeywords};

use fireflow_types::keywords::{
    ISO_DATETIME_NO_TZ, ISO_DATETIME_TZ_HH, ISO_DATETIME_TZ_HH_MAYBE_MM, ISO_DATETIME_TZ_HH_MM,
};
use fireflow_types::nonempty_string::{NEStr, NEString, ToDisplayNE, ambassador_impl_ToDisplayNE};
use type_families::BifunctorOnce as _;

use ambassador::Delegate;
use chrono::{
    DateTime, FixedOffset, Local, MappedLocalTime, NaiveDateTime, ParseError, TimeZone as _,
};
use derive_more::{AsRef, Display, From, Into};
use derive_new::new;
use thiserror::Error;

use std::mem;

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

#[derive(new, Default)]
pub(crate) struct DatetimesDiagnostics {
    pub(crate) begin: DatetimeDiagnostic,
    pub(crate) end: DatetimeDiagnostic,
}

#[derive(new, Default)]
pub struct DatetimeDiagnostic {
    pub(crate) pattern: Option<String>,
    pub(crate) used_localtime: Option<bool>,
}

/// The $BEGINDATETIME key.
#[derive(Clone, Copy, From, Into, PartialEq, Debug, Delegate)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromInnerPyObject))]
#[from(DateTime<FixedOffset>, FCSDateTime)]
#[into(DateTime<FixedOffset>, FCSDateTime)]
#[delegate(ToDisplayNE<'a>, generics = "'a")]
pub struct BeginDateTime(pub FCSDateTime);

/// The $ENDDATETIME key.
#[derive(Clone, Copy, From, Into, PartialEq, Debug, Delegate)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromInnerPyObject))]
#[from(DateTime<FixedOffset>, FCSDateTime)]
#[into(DateTime<FixedOffset>, FCSDateTime)]
#[delegate(ToDisplayNE<'a>, generics = "'a")]
pub struct EndDateTime(pub FCSDateTime);

/// A datetime as used in the $(BEGIN|END)DATETIME keys (3.2+ only)
#[derive(Clone, Copy, From, Into, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromInnerPyObject))]
pub struct FCSDateTime(pub DateTime<FixedOffset>);

impl<'a> ToDisplayNE<'a> for FCSDateTime {
    type NE = NEString;
    fn to_ne(&'a self) -> Self::NE {
        NEString::try_from(self.0.format(ISO_DATETIME_TZ_HH_MM).to_string())
            .expect("format should be non-empty")
    }
}

impl Datetimes {
    #[must_use]
    pub fn try_new(
        begin: Option<BeginDateTime>,
        end: Option<EndDateTime>,
    ) -> ErrorResult<Self, (Option<BeginDateTime>, Option<EndDateTime>), ReversedDatetimesError>
    {
        let ret = Self { begin, end };
        if ret.valid() {
            LogResult::new_ok(ret)
        } else {
            LogResult::new_err(ReversedDatetimesError).set_err_value((ret.begin, ret.end))
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
        kws: &mut ValidKeywords,
        dropped: &mut StdKeywords,
        conf: &C,
    ) -> WarningsAndErrorsResult<
        Diagnosed<Self, DatetimesDiagnostics>,
        (),
        LookupDatetimesError,
        LookupDatetimesError,
    >
    where
        C: AsRef<EvaledReadDataKeywordsConfig> + AsRef<EvaledReadStdKeywordsConfig>,
    {
        macro_rules! go {
            ($x:expr) => {
                $x.map_switchable_errors(LookupDatetimesError::from)
                    .switchable_into_commutative()
                    .set_err_value(())
                    .into_semigroup::<Vec<_>, _>()
            };
        }
        let b = BeginDateTime::remove_or_drop_root_opt_with(kws, dropped, (), conf);
        let e = EndDateTime::remove_or_drop_root_opt_with(kws, dropped, (), conf);
        let rconf: &EvaledReadDataKeywordsConfig = conf.as_ref();
        go!(b)
            .zip_commutative(go!(e))
            .and_then_commutative(|(begin, end)| {
                let diag = DatetimesDiagnostics::new(begin.diagnostic, end.diagnostic);
                Self::try_new(begin.inner, end.inner)
                    .map_ok_value(|x| Diagnosed::new(x, diag))
                    .map_errors(LookupDatetimesError::from)
                    .map_err_value(|(old_begin, old_end)| {
                        // If creating the new datetime object failed,
                        // optionally transfer component keys to nonstandard
                        let bk = old_begin.map(OptRootKeyword::from_value);
                        let ek = old_end.map(OptRootKeyword::from_value);
                        let failed_kws = [bk, ek].into_iter().flatten();
                        match rconf.process_optional_failure.is_demote_or_drop() {
                            Some(true) => {
                                for k in failed_kws {
                                    kws.nonstd.insert_demoted_keyword(k.into());
                                }
                            }
                            Some(false) => {
                                for k in failed_kws {
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

    pub(crate) fn opt_keywords(&self) -> impl Iterator<Item = OptRootKeyword<'_>> {
        let x = self.begin.map(OptRootKeyword::from_value);
        let y = self.end.map(OptRootKeyword::from_value);
        [x, y].into_iter().flatten()
    }

    // pub(crate) fn loss_errors(&self) -> impl Iterator<Item = DatetimeLossError> {
    //     let x0 = Key0LossError::<BeginDateTime>::default();
    //     let y0 = self.begin.is_some().then_some(x0.into());
    //     let x1 = Key0LossError::<EndDateTime>::default();
    //     let y1 = self.end.is_some().then_some(x1.into());
    //     [y0, y1].into_iter().flatten()
    // }
}

macro_rules! impl_from_str_with {
    ($t:ident) => {
        impl FromStrWith for $t {
            type Err = FCSDateTimeError;
            type Payload<'a> = ();
            type Diagnostic = DatetimeDiagnostic;
            type Config = EvaledReadStdKeywordsConfig;

            fn from_str_with(
                s: &fireflow_types::nonempty_string::NEStr,
                (): (),
                conf: &Self::Config,
            ) -> Result<Diagnosed<Self, Self::Diagnostic>, Self::Err> {
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
    type Diagnostic = DatetimeDiagnostic;
    type Config = EvaledReadStdKeywordsConfig;

    fn from_str_with(
        s: &NEStr,
        (): (),
        conf: &Self::Config,
    ) -> Result<Diagnosed<Self, Self::Diagnostic>, Self::Err> {
        if let Some(pat) = conf.datetime_pattern.as_ref() {
            // first, try the given alternative format if it exists
            DateTime::parse_from_str(s.as_str(), pat.as_str())
                .map(Self)
                .map(|x| {
                    let diag = DatetimeDiagnostic::new(Some(pat.clone()), Some(false));
                    Diagnosed::new(x, diag)
                })
                .map_err(|e| FCSDateTimeError::AltFormat(e, pat.to_owned()))
        } else if let Ok(naive) = NaiveDateTime::parse_from_str(s.as_str(), ISO_DATETIME_NO_TZ) {
            // next, try to parse without a timezone, defaulting to localtime and
            // converting to a fixed offset
            if conf.disallow_localtime.is_set() {
                Err(FCSDateTimeError::Localtime)
            } else {
                match Local::now().timezone().from_local_datetime(&naive) {
                    MappedLocalTime::Single(t) => {
                        let diag = DatetimeDiagnostic::new(None, Some(true));
                        Ok(Diagnosed::new(Self(t.fixed_offset()), diag))
                    }
                    MappedLocalTime::Ambiguous(_, _) => Err(FCSDateTimeError::Fold),
                    MappedLocalTime::None => Err(FCSDateTimeError::Gap),
                }
            }
        } else {
            // If zone information is present, try any number of formats which
            // are valid and mostly equivalent which contain the timezone
            let formats = [
                ISO_DATETIME_NO_TZ,
                ISO_DATETIME_TZ_HH_MAYBE_MM,
                ISO_DATETIME_TZ_HH_MM,
                ISO_DATETIME_TZ_HH,
            ];
            for f in formats {
                if let Ok(t) = DateTime::parse_from_str(s.as_ref(), f) {
                    let diag = DatetimeDiagnostic::new(None, Some(false));
                    return Ok(Diagnosed::new(Self(t), diag));
                }
            }
            Err(FCSDateTimeError::Format)
        }
    }
}

/// Error when $ENDDATETIME occurs before $BEGINDATETIME
#[derive(Debug, Error, PartialEq, Clone)]
#[error("$BEGINDATETIME is after $ENDDATETIME")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(fireflow_types::python::RelationalError))]
pub struct ReversedDatetimesError;

/// Error when parsing [`FCSDateTime`] from string
#[derive(Debug, Error, PartialEq, Clone)]
pub enum FCSDateTimeError {
    #[error("must be formatted like 'yyyy-mm-ddThh:mm:ss[TZD]'")]
    Format,
    #[error("{0} with pattern '{1}'")]
    AltFormat(ParseError, String),
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
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupDatetimesError {
    Begindatetime(OptKeyStError<BeginDateTime>),
    Enddatetime(OptKeyStError<EndDateTime>),
    Datetime(ReversedDatetimesError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;
    use std::str::FromStr;

    impl FromStr for FCSDateTime {
        type Err = FCSDateTimeError;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            let conf = EvaledReadStdKeywordsConfig::default();
            Self::from_str_with(NEStr::try_new(s).unwrap(), (), &conf).map(|x| x.inner)
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

use crate::config::{AllowOptionalDropping, ConfigFlag as _, StdTextReadConfig};
use crate::core::{AnyMetarootKeyLossError, UnitaryKeyLossError};
use crate::logging::{DeferredError, DeferredSwitchableErrors, LogResult, ResultExt as _};
use crate::type_families::ApplyOnce as _;
use crate::validated::keys::{NonStdKeywords, NonStdKeywordsExt as _, StdKeywords};

use super::lookup::{OptKeyError, OptMetarootKey as _};
use super::optional::KeywordPairMaybe as _;

use chrono::{DateTime, FixedOffset, Local, NaiveDateTime, TimeZone as _};
use derive_more::{AsRef, Display, From, FromStr, Into};
use std::mem;
use std::str::FromStr;
use thiserror::Error;

#[cfg(feature = "serde")]
use serde::Serialize;

/// A convenient bundle for the $BEGINDATETIME and $ENDDATETIME keys (3.2+)
#[derive(Clone, Default, AsRef, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct Datetimes {
    /// Value for the $BEGINDATETIME key.
    #[as_ref(Option<BeginDateTime>)]
    begin: Option<BeginDateTime>,

    /// Value for the $ENDDATETIME key.
    #[as_ref(Option<EndDateTime>)]
    end: Option<EndDateTime>,
}

#[derive(Clone, Copy, From, Into, Display, FromStr, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[from(DateTime<FixedOffset>, FCSDateTime)]
#[into(DateTime<FixedOffset>, FCSDateTime)]
pub struct BeginDateTime(pub FCSDateTime);

#[derive(Clone, Copy, From, Into, Display, FromStr, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[from(DateTime<FixedOffset>, FCSDateTime)]
#[into(DateTime<FixedOffset>, FCSDateTime)]
pub struct EndDateTime(pub FCSDateTime);

/// A datetime as used in the $(BEGIN|END)DATETIME keys (3.2+ only)
#[derive(Clone, Copy, From, Into, PartialEq, Debug, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
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
            (b.0).0 < (e.0).0
        } else {
            true
        }
    }

    pub(crate) fn lookup(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        conf: &StdTextReadConfig,
    ) -> DeferredSwitchableErrors<Self, AllowOptionalDropping, LookupDatetimesError> {
        let b = BeginDateTime::transfer_metaroot_opt(std, nonstd, conf)
            .map_err(LookupDatetimesError::from)
            .into_deferred_nowarn();
        let e = EndDateTime::transfer_metaroot_opt(std, nonstd, conf)
            .map_err(LookupDatetimesError::from)
            .into_deferred_nowarn();
        let flag = conf.allow_optional_dropping;
        b.zip_f2_once(e)
            .and_then_def(|(begin, end)| {
                Self::try_new(begin, end)
                    .map_errors(LookupDatetimesError::from)
                    .map_err_value(|ret| {
                        // If creating the new datetime object failed,
                        // optionally transfer component keys to nonstandard
                        if conf.transfer_dropped_optional.is_set() {
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

    pub(crate) fn loss_errors(&self) -> impl Iterator<Item = AnyMetarootKeyLossError> {
        let x0 = UnitaryKeyLossError::<BeginDateTime>::new();
        let y0 = self.begin.is_some().then_some(x0.into());
        let x1 = UnitaryKeyLossError::<EndDateTime>::new();
        let y1 = self.end.is_some().then_some(x1.into());
        [y0, y1].into_iter().flatten()
    }
}

#[derive(Debug, Error)]
#[error("$BEGINDATETIME is after $ENDDATETIME")]
pub struct ReversedDatetimesError;

impl FromStr for FCSDateTime {
    type Err = FCSDateTimeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // first, try to parse without a timezone, defaulting to localtime and
        // converting to a fixed offset
        // TODO this should probably be a warning since it is ambiguous to
        // parse a timezone based solely on localtime
        if let Ok(naive) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f") {
            Local::now()
                .timezone()
                .from_local_datetime(&naive)
                .single()
                .map_or_else(
                    || Err(FCSDateTimeError::Unmapped(s.into())),
                    |t| Ok(Self(t.fixed_offset())),
                )
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
                    return Ok(Self(t));
                }
            }
            Err(FCSDateTimeError::Other)
        }
    }
}

#[derive(Debug, Error)]
#[error("must be formatted like 'yyyy-mm-ddThh:mm:ss[TZD]'")]
pub enum FCSDateTimeError {
    Unmapped(String),
    Other,
}

#[derive(From, Display, Debug, Error)]
pub enum LookupDatetimesError {
    Begindatetime(OptKeyError<BeginDateTime>),
    Enddatetime(OptKeyError<EndDateTime>),
    Datetime(ReversedDatetimesError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;

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

#[cfg(feature = "python")]
mod python {
    use super::{
        BeginDateTime, EndDateTime, FCSDateTime, LookupDatetimesError, ReversedDatetimesError,
    };
    use crate::python::macros::{impl_from_py_transparent, impl_from_pyerr, impl_pyreflow_err};

    impl_from_py_transparent!(FCSDateTime);
    impl_from_py_transparent!(BeginDateTime);
    impl_from_py_transparent!(EndDateTime);

    impl_pyreflow_err!(RelationalException, ReversedDatetimesError);

    impl_from_pyerr!(LookupDatetimesError, Begindatetime, Enddatetime, Datetime);
}

use crate::config::StdTextReadConfig;
use crate::core::{AnyMetarootKeyLossError, UnitaryKeyLossError};
use crate::error::{BiTentative, ResultExt as _, Tentative};
use crate::validated::keys::StdKeywords;

use super::optional::MaybeValue;
use super::parser::{LookupTentative, OptMetarootKey as _};

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
    pub fn try_new(
        begin: Option<BeginDateTime>,
        end: Option<EndDateTime>,
    ) -> DatetimesResult<Self> {
        let ret = Self { begin, end };
        if ret.valid() {
            Ok(ret)
        } else {
            Err(ReversedDatetimesError)
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

    pub(crate) fn lookup(kws: &mut StdKeywords, conf: &StdTextReadConfig) -> LookupTentative<Self> {
        let b = BeginDateTime::lookup_opt(kws, conf);
        let e = EndDateTime::lookup_opt(kws, conf);
        b.zip(e).and_tentatively(|(begin, end)| {
            Self::try_new(begin.0, end.0)
                .into_tentative_def(!conf.allow_optional_dropping)
                .inner_into()
        })
    }

    pub(crate) fn opt_keywords(&self) -> impl Iterator<Item = (String, String)> {
        [
            MaybeValue(self.begin).root_kw_pair(),
            MaybeValue(self.end).root_kw_pair(),
        ]
        .into_iter()
        .filter_map(|(k, v)| v.map(|x| (k, x)))
    }

    pub(crate) fn check_loss(self, allow_loss: bool) -> BiTentative<(), AnyMetarootKeyLossError> {
        let mut tnt = Tentative::new1(());
        if self.begin.is_some() {
            tnt.push_error_or_warning(UnitaryKeyLossError::<BeginDateTime>::new(), allow_loss);
        }
        if self.end.is_some() {
            tnt.push_error_or_warning(UnitaryKeyLossError::<EndDateTime>::new(), allow_loss);
        }
        tnt
    }
}

#[derive(Debug, Error)]
#[error("$BEGINDATETIME is after $ENDDATETIME")]
pub struct ReversedDatetimesError;

type DatetimesResult<T> = Result<T, ReversedDatetimesError>;

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
    use super::{BeginDateTime, EndDateTime, FCSDateTime, ReversedDatetimesError};
    use crate::python::macros::{impl_from_py_transparent, impl_pyreflow_err};

    impl_pyreflow_err!(ReversedDatetimesError);

    impl_from_py_transparent!(FCSDateTime);
    impl_from_py_transparent!(BeginDateTime);
    impl_from_py_transparent!(EndDateTime);
}

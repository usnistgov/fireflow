use crate::config::{StdTextReadConfig, TimeMeasNamePattern};
use crate::core::*;
use crate::error::*;
use crate::validated::keys::*;
use crate::validated::shortname::*;

use super::byteord::*;
use super::compensation::*;
use super::datetimes::*;
use super::gating;
use super::index::*;
use super::keywords::*;
use super::named_vec::*;
use super::optional::*;
use super::ranged_float::RangedFloatError;
use super::scale::*;
use super::spillover::*;
use super::timestamps::*;
use super::unstainedcenters::*;

use bigdecimal::ParseBigDecimalError;
use derive_more::{Display, From};
use derive_new::new;
use itertools::Itertools;
use nonempty::NonEmpty;
use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::fmt;
use std::num::{ParseFloatError, ParseIntError};
use std::str::FromStr;
use thiserror::Error;

#[cfg(feature = "python")]
use pyo3::prelude::*;

pub trait FromStrDelim: Sized {
    type Err;
    const DELIM: char;

    fn from_iter<'a>(ss: impl Iterator<Item = &'a str>) -> Result<Self, Self::Err>;

    fn from_str_delim(s: &str, trim_whitespace: bool) -> Result<Self, Self::Err> {
        let it = s.split(Self::DELIM);
        if trim_whitespace {
            Self::from_iter(it.map(|x| x.trim()))
        } else {
            Self::from_iter(it)
        }
    }
}

pub trait FromStrStateful: Sized {
    type Err;
    type Payload<'a>;

    fn from_str_st(_: &str, _: Self::Payload<'_>, _: &StdTextReadConfig)
        -> Result<Self, Self::Err>;
}

/// Any required key
pub(crate) trait Required {
    fn get_req(kws: &StdKeywords, k: StdKey) -> ReqResult<Self>
    where
        Self: FromStr,
    {
        get_req(kws, k)
    }

    fn remove_req(kws: &mut StdKeywords, k: StdKey) -> ReqResult<Self>
    where
        Self: FromStr,
    {
        match kws.remove(&k) {
            Some(v) => v
                .parse()
                .map_err(|error| ParseKeyError::new(error, k, v))
                .map_err(ReqKeyError::Parse),
            None => Err(ReqKeyError::Missing(k)),
        }
    }

    fn remove_req_st(
        kws: &mut StdKeywords,
        k: StdKey,
        data: Self::Payload<'_>,
        conf: &StdTextReadConfig,
    ) -> ReqStResult<Self>
    where
        Self: FromStrStateful,
    {
        match kws.remove(&k) {
            Some(v) => Self::from_str_st(v.as_str(), data, conf)
                .map_err(|error| ParseKeyError::new(error, k, v))
                .map_err(ReqKeyError::Parse),
            None => Err(ReqKeyError::Missing(k)),
        }
    }
}

/// Any optional key
pub(crate) trait Optional {
    fn get_opt(kws: &StdKeywords, k: StdKey) -> OptKwResult<Self>
    where
        Self: FromStr,
    {
        get_opt(kws, k).map(|x| x.into())
    }

    fn remove_opt(
        kws: &mut StdKeywords,
        k: StdKey,
    ) -> Result<MaybeValue<Self>, ParseKeyError<Self::Err>>
    where
        Self: FromStr,
    {
        kws.remove(&k)
            .map(|v| v.parse().map_err(|error| ParseKeyError::new(error, k, v)))
            .transpose()
            .map(|x| x.into())
    }

    fn remove_opt_st(
        kws: &mut StdKeywords,
        k: StdKey,
        data: Self::Payload<'_>,
        conf: &StdTextReadConfig,
    ) -> Result<MaybeValue<Self>, ParseKeyError<Self::Err>>
    where
        Self: FromStrStateful,
    {
        kws.remove(&k)
            .map(|v| {
                Self::from_str_st(v.as_str(), data, conf)
                    .map_err(|error| ParseKeyError::new(error, k, v))
            })
            .transpose()
            .map(|x| x.into())
    }
}

/// A required metaroot key
pub(crate) trait ReqMetarootKey: Sized + Required + Key {
    fn get_metaroot_req(kws: &StdKeywords) -> ReqResult<Self>
    where
        Self: FromStr,
    {
        Self::get_req(kws, Self::std())
    }

    fn remove_metaroot_req(kws: &mut StdKeywords) -> ReqResult<Self>
    where
        Self: FromStr,
    {
        Self::remove_req(kws, Self::std())
    }

    fn lookup_req(kws: &mut StdKeywords) -> LookupResult<Self>
    where
        Self: FromStr,
        ParseReqKeyError: From<<Self as FromStr>::Err>,
    {
        Self::remove_metaroot_req(kws)
            .map_err(|e| e.inner_into())
            .map_err(Box::new)
            .into_deferred()
    }

    // fn lookup_req_st(
    //     kws: &mut StdKeywords,
    //     data: Self::Payload<'_>,
    //     conf: &StdTextReadConfig,
    // ) -> LookupResult<Self>
    // where
    //     Self: FromStrStateful,
    //     ParseReqKeyError: From<<Self as FromStrStateful>::Err>,
    // {
    //     Self::remove_req_st(kws, Self::std(), data, conf)
    //         .map_err(|e| e.inner_into())
    //         .map_err(Box::new)
    //         .into_deferred()
    // }

    fn pair(&self) -> (String, String)
    where
        Self: fmt::Display,
    {
        (Self::std().to_string(), self.to_string())
    }
}

/// Any required key with one index
pub(crate) trait ReqIndexedKey: Sized + Required + IndexedKey {
    fn get_meas_req(kws: &StdKeywords, i: IndexFromOne) -> ReqResult<Self>
    where
        Self: FromStr,
    {
        Self::get_req(kws, Self::std(i))
    }

    fn remove_meas_req(kws: &mut StdKeywords, i: IndexFromOne) -> ReqResult<Self>
    where
        Self: FromStr,
    {
        Self::remove_req(kws, Self::std(i))
    }

    fn lookup_req(kws: &mut StdKeywords, i: IndexFromOne) -> LookupResult<Self>
    where
        Self: FromStr,
        ParseReqKeyError: From<<Self as FromStr>::Err>,
    {
        Self::remove_meas_req(kws, i)
            .map_err(|e| e.inner_into())
            .map_err(Box::new)
            .into_deferred()
    }

    fn lookup_req_st(
        kws: &mut StdKeywords,
        i: IndexFromOne,
        data: Self::Payload<'_>,
        conf: &StdTextReadConfig,
    ) -> LookupResult<Self>
    where
        Self: FromStrStateful,
        ParseReqKeyError: From<<Self as FromStrStateful>::Err>,
    {
        Self::remove_req_st(kws, Self::std(i), data, conf)
            .map_err(|e| e.inner_into())
            .map_err(Box::new)
            .into_deferred()
    }

    fn triple(&self, i: IndexFromOne) -> (MeasHeader, String, String)
    where
        Self: fmt::Display,
    {
        (
            Self::std_blank(),
            Self::std(i).to_string(),
            self.to_string(),
        )
    }

    fn pair(&self, i: IndexFromOne) -> (String, String)
    where
        Self: fmt::Display,
    {
        let (_, k, v) = self.triple(i);
        (k, v)
    }
}

/// An optional metaroot key
pub(crate) trait OptMetarootKey: Sized + Optional + Key {
    fn get_metaroot_opt(kws: &StdKeywords) -> OptKwResult<Self>
    where
        Self: FromStr,
    {
        Self::get_opt(kws, Self::std())
    }

    fn remove_metaroot_opt(kws: &mut StdKeywords) -> OptKwResult<Self>
    where
        Self: FromStr,
    {
        Self::remove_opt(kws, Self::std())
    }

    fn lookup_opt<E>(kws: &mut StdKeywords) -> LookupTentative<MaybeValue<Self>, E>
    where
        Self: FromStr,
        ParseOptKeyWarning: From<<Self as FromStr>::Err>,
    {
        process_opt(Self::remove_metaroot_opt(kws))
    }

    fn lookup_opt_st<E>(
        kws: &mut StdKeywords,
        data: Self::Payload<'_>,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<MaybeValue<Self>, E>
    where
        Self: FromStrStateful,
        ParseOptKeyWarning: From<<Self as FromStrStateful>::Err>,
    {
        process_opt(Self::remove_opt_st(kws, Self::std(), data, conf))
    }

    fn lookup_opt_dep(
        kws: &mut StdKeywords,
        disallow_dep: bool,
    ) -> LookupTentative<MaybeValue<Self>, DeprecatedError>
    where
        Self: FromStr,
        ParseOptKeyWarning: From<<Self as FromStr>::Err>,
    {
        let mut x = Self::lookup_opt(kws);
        eval_dep_maybe(&mut x, Self::std(), disallow_dep);
        x
    }

    fn lookup_opt_st_dep(
        kws: &mut StdKeywords,
        disallow_dep: bool,
        data: Self::Payload<'_>,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<MaybeValue<Self>, DeprecatedError>
    where
        Self: FromStrStateful,
        ParseOptKeyWarning: From<<Self as FromStrStateful>::Err>,
    {
        let mut x = Self::lookup_opt_st(kws, data, conf);
        eval_dep_maybe(&mut x, Self::std(), disallow_dep);
        x
    }

    fn pair_opt(opt: &MaybeValue<Self>) -> (String, Option<String>)
    where
        Self: fmt::Display,
    {
        (
            Self::std().to_string(),
            opt.0.as_ref().map(|s| s.to_string()),
        )
    }

    fn pair(&self) -> (String, String)
    where
        Self: fmt::Display,
    {
        (Self::std().to_string(), self.to_string())
    }
}

/// Any optional key with an index
pub(crate) trait OptIndexedKey: Sized + Optional + IndexedKey {
    fn get_meas_opt(kws: &StdKeywords, i: IndexFromOne) -> OptKwResult<Self>
    where
        Self: FromStr,
    {
        Self::get_opt(kws, Self::std(i))
    }

    fn remove_meas_opt(kws: &mut StdKeywords, i: IndexFromOne) -> OptKwResult<Self>
    where
        Self: FromStr,
    {
        Self::remove_opt(kws, Self::std(i))
    }

    fn remove_meas_opt_st(
        kws: &mut StdKeywords,
        i: IndexFromOne,
        data: Self::Payload<'_>,
        conf: &StdTextReadConfig,
    ) -> Result<MaybeValue<Self>, ParseKeyError<Self::Err>>
    where
        Self: FromStrStateful,
    {
        Self::remove_opt_st(kws, Self::std(i), data, conf)
    }

    fn lookup_opt<E>(kws: &mut StdKeywords, i: IndexFromOne) -> LookupTentative<MaybeValue<Self>, E>
    where
        Self: FromStr,
        ParseOptKeyWarning: From<<Self as FromStr>::Err>,
    {
        process_opt(Self::remove_meas_opt(kws, i))
    }

    fn lookup_opt_st<E>(
        kws: &mut StdKeywords,
        i: IndexFromOne,
        data: Self::Payload<'_>,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<MaybeValue<Self>, E>
    where
        Self: FromStrStateful,
        ParseOptKeyWarning: From<<Self as FromStrStateful>::Err>,
    {
        process_opt(Self::remove_opt_st(kws, Self::std(i), data, conf))
    }

    fn lookup_opt_dep(
        kws: &mut StdKeywords,
        i: IndexFromOne,
        disallow_dep: bool,
    ) -> LookupTentative<MaybeValue<Self>, DeprecatedError>
    where
        Self: FromStr,
        ParseOptKeyWarning: From<<Self as FromStr>::Err>,
    {
        let mut x = Self::lookup_opt(kws, i);
        eval_dep_maybe(&mut x, Self::std(i), disallow_dep);
        x
    }

    fn lookup_opt_st_dep(
        kws: &mut StdKeywords,
        i: IndexFromOne,
        disallow_dep: bool,
        data: Self::Payload<'_>,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<MaybeValue<Self>, DeprecatedError>
    where
        Self: FromStrStateful,
        ParseOptKeyWarning: From<<Self as FromStrStateful>::Err>,
    {
        let mut x = Self::lookup_opt_st(kws, i, data, conf);
        eval_dep_maybe(&mut x, Self::std(i), disallow_dep);
        x
    }

    fn triple(opt: &MaybeValue<Self>, i: IndexFromOne) -> (MeasHeader, String, Option<String>)
    where
        Self: fmt::Display,
    {
        (
            Self::std_blank(),
            Self::std(i).to_string(),
            opt.0.as_ref().map(|s| s.to_string()),
        )
    }

    fn pair_opt(opt: &MaybeValue<Self>, i: IndexFromOne) -> (String, Option<String>)
    where
        Self: fmt::Display,
    {
        let (_, k, v) = Self::triple(opt, i);
        (k, v)
    }

    fn pair(&self, i: IndexFromOne) -> (String, String)
    where
        Self: fmt::Display,
    {
        (Self::std(i).to_string(), self.to_string())
    }
}

/// Any key which references $PnN in its value
pub(crate) trait OptLinkedKey
where
    Self: Key + Optional + fmt::Display + FromStr + Sized,
{
    fn check_link(&self, names: &HashSet<&Shortname>) -> Result<(), LinkedNameError> {
        NonEmpty::collect(self.names().difference(names).copied().cloned())
            .map(|common_names| LinkedNameError {
                names: common_names,
                key: Self::std(),
            })
            .map(Err)
            .unwrap_or(Ok(()))
    }

    // fn lookup_opt_linked<E>(
    //     kws: &mut StdKeywords,
    //     names: &HashSet<&Shortname>,
    // ) -> LookupTentative<MaybeValue<Self>, E>
    // where
    //     ParseOptKeyWarning: From<<Self as FromStr>::Err>,
    // {
    //     process_opt(Self::remove_opt(kws, Self::std())).and_tentatively(|maybe| {
    //         if let Some(x) = maybe.0 {
    //             Self::check_link(&x, names).map_or_else(
    //                 |w| Tentative::new(None, vec![w.into()], vec![]),
    //                 |_| Tentative::new1(Some(x)),
    //             )
    //         } else {
    //             Tentative::new1(None)
    //         }
    //         .map(|x| x.into())
    //     })
    // }

    fn lookup_opt_linked_st<E, P>(
        kws: &mut StdKeywords,
        names: &HashSet<&Shortname>,
        payload: P,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<MaybeValue<Self>, E>
    where
        for<'a> Self: FromStrStateful<Payload<'a> = P>,
        ParseOptKeyWarning: From<<Self as FromStrStateful>::Err>,
    {
        // TODO not dry
        process_opt(Self::remove_opt_st(kws, Self::std(), payload, conf)).and_tentatively(|maybe| {
            if let Some(x) = maybe.0 {
                Self::check_link(&x, names).map_or_else(
                    |w| Tentative::new(None, vec![w.into()], vec![]),
                    |_| Tentative::new1(Some(x)),
                )
            } else {
                Tentative::new1(None)
            }
            .map(|x| x.into())
        })
    }

    // TODO not DRY
    fn pair_opt(opt: &MaybeValue<Self>) -> (String, Option<String>) {
        (
            Self::std().to_string(),
            opt.0.as_ref().map(|s| s.to_string()),
        )
    }

    fn reassign(&mut self, mapping: &NameMapping);

    fn names(&self) -> HashSet<&Shortname>;
}

/// Find a required standard key in a hash table
pub(crate) fn get_req<T>(kws: &StdKeywords, k: StdKey) -> ReqResult<T>
where
    T: FromStr,
{
    match kws.get(&k) {
        Some(v) => v
            .parse()
            .map_err(|error| ParseKeyError::new(error, k, v.clone()))
            .map_err(ReqKeyError::Parse),
        None => Err(ReqKeyError::Missing(k)),
    }
}

/// Find an optional standard key in a hash table
pub(crate) fn get_opt<T>(kws: &StdKeywords, k: StdKey) -> OptResult<T>
where
    T: FromStr,
{
    kws.get(&k)
        .map(|v| {
            v.parse()
                .map_err(|error| ParseKeyError::new(error, k, v.clone()))
        })
        .transpose()
}

#[derive(Debug, Error)]
#[error("{key} references non-existent $PnN: {bad}", bad = .names.iter().join(", "))]
pub struct LinkedNameError {
    pub key: StdKey,
    pub names: NonEmpty<Shortname>,
}

pub(crate) fn lookup_temporal_scale_3_0(
    kws: &mut StdKeywords,
    i: MeasIndex,
    nonstd: &mut NonStdKeywords,
    conf: &StdTextReadConfig,
) -> LookupResult<TemporalScale> {
    let j = i.into();
    if conf.force_time_linear {
        nonstd.transfer_demoted(kws, TemporalScale::std(j));
        Ok(Tentative::new1(TemporalScale))
    } else {
        TemporalScale::lookup_req(kws, j)
    }
}

pub(crate) fn lookup_temporal_gain_3_0(
    kws: &mut StdKeywords,
    i: MeasIndex,
    nonstd: &mut NonStdKeywords,
    conf: &StdTextReadConfig,
) -> LookupTentative<MaybeValue<Gain>, LookupKeysError> {
    let j = i.into();
    if conf.ignore_time_gain {
        nonstd.transfer_demoted(kws, Gain::std(j));
        Tentative::default()
    } else {
        let mut tnt_gain = Gain::lookup_opt(kws, j);
        tnt_gain.eval_error(|gain| {
            if gain.0.is_some_and(|g| f32::from(g.0) != 1.0) {
                Some(LookupKeysError::Misc(TemporalError::HasGain.into()))
            } else {
                None
            }
        });
        tnt_gain
    }
}

pub(crate) fn process_opt<V, E, W>(
    res: Result<MaybeValue<V>, ParseKeyError<W>>,
) -> Tentative<MaybeValue<V>, LookupKeysWarning, E>
where
    ParseOptKeyWarning: From<W>,
{
    res.into_tentative_warn_def()
        .map_warnings(|w| LookupKeysWarning::Parse(w.inner_into()))
}

pub(crate) type RawKeywords = HashMap<String, String>;

pub(crate) type ReqResult<T> = Result<T, ReqKeyError<<T as FromStr>::Err>>;
pub(crate) type ReqStResult<T> = Result<T, ReqKeyError<<T as FromStrStateful>::Err>>;
pub(crate) type OptResult<T> = Result<Option<T>, ParseKeyError<<T as FromStr>::Err>>;
pub(crate) type OptKwResult<T> = Result<MaybeValue<T>, ParseKeyError<<T as FromStr>::Err>>;

pub(crate) type LookupResult<V> = DeferredResult<V, LookupKeysWarning, LookupKeysError>;
pub(crate) type LookupTentative<V, E> = Tentative<V, LookupKeysWarning, E>;
pub(crate) type LookupOptional<V, E> = Tentative<MaybeValue<V>, LookupKeysWarning, E>;

// TODO this could be nested better
#[derive(From, Display, Debug, Error)]
pub enum LookupKeysError {
    Parse(Box<ReqKeyError<ParseReqKeyError>>),
    Dep(DeprecatedError),
    Misc(LookupMiscError),
}

#[derive(From, Display, Debug, Error)]
pub enum LookupKeysWarning {
    Parse(ParseKeyError<ParseOptKeyWarning>),
    Relation(LookupRelationalWarning),
    LinkedName(LinkedNameError),
    LinkedIndex(RegionIndexError),
    Dep(DeprecatedError),
}

#[derive(From, Display, Debug, Error)]
pub enum DeprecatedError {
    Key(DepKeyWarning),
    Value(DepValueWarning),
}

/// Error encountered when parsing a required key from a string
#[derive(From, Display, Debug, Error)]
pub enum ParseReqKeyError {
    Range(ParseBigDecimalError),
    AlphaNumType(AlphaNumTypeError),
    String(Infallible),
    Int(ParseIntError),
    Scale(ScaleError),
    TemporalScale(TemporalScaleError),
    RangedFloat(RangedFloatError),
    Mode(ModeError),
    ByteOrd(ParseByteOrdError),
    Endian(NewEndianError),
    Shortname(ShortnameError),
}

/// Error encountered when parsing an optional key from a string
#[derive(From, Display, Debug, Error)]
pub enum ParseOptKeyWarning {
    NumType(NumTypeError),
    Trigger(TriggerError),
    Scale(ScaleError),
    TemporalScale(TemporalScaleError),
    Float(ParseFloatError),
    RangedFloat(RangedFloatError),
    Feature(FeatureError),
    Wavelengths(WavelengthsError),
    Calibration3_1(CalibrationError<CalibrationFormat3_1>),
    Calibration3_2(CalibrationError<CalibrationFormat3_2>),
    Int(ParseIntError),
    String(Infallible),
    FCSDate(FCSDateError),
    FCSTime(FCSFixedTimeError<FCSTimeError>),
    FCSTime60(FCSFixedTimeError<FCSTime60Error>),
    FCSTime100(FCSFixedTimeError<FCSTime100Error>),
    FCSDateTime(FCSDateTimeError),
    ModifiedDateTime(LastModifiedError),
    Originality(OriginalityError),
    UnstainedCenter(ParseUnstainedCenterError),
    Mode3_2(Mode3_2Error),
    TemporalType(TemporalTypeError),
    OpticalType(OpticalTypeError),
    Shortname(ShortnameError),
    Display(DisplayError),
    Unicode(UnicodeError),
    Spillover(ParseSpilloverError),
    Compensation(ParseCompError),
    GateRange(ParseBigDecimalError),
    GateRegionIndex2_0(RegionGateIndexError<ParseIntError>),
    GateRegionIndex3_0(RegionGateIndexError<MeasOrGateIndexError>),
    GateRegionIndex3_2(RegionGateIndexError<PrefixedMeasIndexError>),
    GateRegionWindow(GatePairError),
    Gating(GatingError),
}

/// Misc errors encountered when looking up keywords for standardization
#[derive(From, Display, Debug, Error)]
pub enum LookupMiscError {
    // TODO this should be a configurable warning
    Temporal(TemporalError),
    NamedVec(NewNamedVecError),
    MissingTime(MissingTime),
    InvalidScale(ScaleTransformError),
}

/// Error triggered when time measurement is missing but required.
#[derive(Debug, Error)]
#[error("Could not find time measurement matching {0}")]
pub struct MissingTime(pub TimeMeasNamePattern);

/// Errors triggered when time measurement keyword value is invalid
// TODO add other optical keywords that shouldn't be set for time.
// TODO include meas idx here
#[derive(Debug, Error)]
pub enum TemporalError {
    #[error("$PnE must be '0,0' for temporal measurement")]
    NonLinear,
    #[error("$PnG must be 1.0 or not set for temporal measurement")]
    HasGain,
}

/// Error encountered when relation between two or more keys is invalid
#[derive(From, Display, Debug, Error)]
pub enum LookupRelationalWarning {
    Timestamp(ReversedTimestamps),
    Datetime(ReversedDatetimes),
    CompShape(NewCompError),
    CSVFlag(NewCSVFlagsError),
    GateRegion(gating::MismatchedIndexAndWindowError),
    GateMeasLink(gating::GateMeasurementLinkError),
    GatingScheme(gating::NewGatingSchemeError),
    Spillover(SpilloverIndexError),
}

/// Error/warning triggered when encountering a key which is deprecated
#[derive(Debug, Error)]
#[error("deprecated key: {0}")]
pub struct DepKeyWarning(pub StdKey);

/// Error/warning triggered when encountering a key value which is deprecated
#[derive(Debug, Error)]
pub enum DepValueWarning {
    #[error("$DATATYPE=A is deprecated")]
    DatatypeASCII,
    #[error("$MODE=C is deprecated")]
    ModeCorrelated,
    #[error("$MODE=U is deprecated")]
    ModeUncorrelated,
}

/// Error denoting that pseudostandard keyword was found.
#[derive(Debug, Error)]
#[error("pseudostandard keyword found: {0}")]
pub struct PseudostandardError(pub StdKey);

/// Error denoting that unused standard keyword was found.
#[derive(Debug, Error)]
#[error("unused standard keyword found: {0}")]
pub struct UnusedStandardError(pub StdKey);

#[derive(new, Debug, Error)]
#[error("{error} (key='{key}', value='{value}')")]
pub struct ParseKeyError<E> {
    pub error: E,
    pub key: StdKey,
    pub value: String,
}

impl<E> ParseKeyError<E> {
    pub fn inner_into<F>(self) -> ParseKeyError<F>
    where
        F: From<E>,
    {
        ParseKeyError {
            error: self.error.into(),
            key: self.key,
            value: self.value,
        }
    }

    pub fn with_error<F, X>(self, f: F) -> Result<X, Self>
    where
        F: FnOnce(E) -> Result<X, E>,
    {
        f(self.error).map_err(|error| Self {
            error,
            key: self.key,
            value: self.value,
        })
    }
}

#[derive(Debug, Error)]
pub enum ReqKeyError<E> {
    #[error("{0}")]
    Parse(ParseKeyError<E>),
    #[error("missing required key: {0}")]
    Missing(StdKey),
}

impl<E> ReqKeyError<E> {
    pub fn inner_into<F>(self) -> ReqKeyError<F>
    where
        F: From<E>,
    {
        match self {
            ReqKeyError::Parse(e) => ReqKeyError::Parse(e.inner_into()),
            ReqKeyError::Missing(e) => ReqKeyError::Missing(e),
        }
    }

    pub fn with_parse_error<F, X>(self, f: F) -> Result<X, Self>
    where
        F: FnOnce(E) -> Result<X, E>,
    {
        match self {
            ReqKeyError::Parse(p) => p.with_error(f).map_err(ReqKeyError::Parse),
            ReqKeyError::Missing(m) => Err(ReqKeyError::Missing(m)),
        }
    }
}

pub(crate) fn eval_dep_maybe<T>(
    x: &mut LookupTentative<MaybeValue<T>, DeprecatedError>,
    key: StdKey,
    disallow_dep: bool,
) {
    if disallow_dep {
        x.eval_error(|v| eval_dep(v, key));
    } else {
        x.eval_warning(|v| eval_dep(v, key).map(|e| e.into()));
    }
}

fn eval_dep<T>(v: &MaybeValue<T>, key: StdKey) -> Option<DeprecatedError> {
    if v.0.is_some() {
        Some(DeprecatedError::Key(DepKeyWarning(key)))
    } else {
        None
    }
}

#[derive(Clone, new, PartialEq)]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
pub struct ExtraStdKeywords {
    pub pseudostandard: StdKeywords,
    pub unused: StdKeywords,
}

impl ExtraStdKeywords {
    pub(crate) fn split_2_0(kws: StdKeywords) -> Self {
        Self::split_inner(kws, Self::matches_kw_2_0)
    }

    pub(crate) fn split_3_0(kws: StdKeywords) -> Self {
        Self::split_inner(kws, Self::matches_kw_3_0)
    }

    pub(crate) fn split_3_1(kws: StdKeywords) -> Self {
        Self::split_inner(kws, Self::matches_kw_3_1)
    }

    pub(crate) fn split_3_2(kws: StdKeywords) -> Self {
        Self::split_inner(kws, Self::matches_kw_3_2)
    }

    fn split_inner<F>(mut kws: StdKeywords, mut f: F) -> Self
    where
        F: FnMut(&StdKey) -> bool,
    {
        let unused: HashMap<_, _> = kws.extract_if(|k, _| f(k)).collect();
        Self {
            pseudostandard: kws,
            unused,
        }
    }

    fn matches_kw_2_0(k: &StdKey) -> bool {
        let s: &str = k.as_ref();
        s.eq_ignore_ascii_case(Tot::C) || Dfc::matches(k) || Self::matches_meas_kw_common(k)
    }

    fn matches_kw_3_0(k: &StdKey) -> bool {
        let s: &str = k.as_ref();
        Self::matches_offsets(k)
            || s.eq_ignore_ascii_case(Tot::C)
            || s.eq_ignore_ascii_case(Timestep::C)
            || Gain::matches(k)
            || Self::matches_meas_kw_common(k)
    }

    fn matches_kw_3_1(k: &StdKey) -> bool {
        let s: &str = k.as_ref();
        Self::matches_offsets(k)
            || s.eq_ignore_ascii_case(Tot::C)
            || s.eq_ignore_ascii_case(Timestep::C)
            || Gain::matches(k)
            || Display::matches(k)
            || Calibration3_1::matches(k)
            || Self::matches_meas_kw_common(k)
    }

    fn matches_kw_3_2(k: &StdKey) -> bool {
        let s: &str = k.as_ref();
        Self::matches_offsets(k)
            || s.eq_ignore_ascii_case(Tot::C)
            || s.eq_ignore_ascii_case(Timestep::C)
            || Gain::matches(k)
            || Display::matches(k)
            || Calibration3_2::matches(k)
            || NumType::matches(k)
            || DetectorName::matches(k)
            || Tag::matches(k)
            || Analyte::matches(k)
            || Feature::matches(k)
            || OpticalType::matches(k)
            || Self::matches_meas_kw_common(k)
    }

    fn matches_offsets(k: &StdKey) -> bool {
        let s: &str = k.as_ref();
        s.eq_ignore_ascii_case(Beginanalysis::C)
            || s.eq_ignore_ascii_case(Endanalysis::C)
            || s.eq_ignore_ascii_case(Begindata::C)
            || s.eq_ignore_ascii_case(Enddata::C)
    }

    fn matches_meas_kw_common(k: &StdKey) -> bool {
        Width::matches(k)
            || Range::matches(k)
            || Scale::matches(k)
            || Shortname::matches(k)
            || Longname::matches(k)
            || Power::matches(k)
            || DetectorType::matches(k)
            || PercentEmitted::matches(k)
            || DetectorVoltage::matches(k)
    }
}

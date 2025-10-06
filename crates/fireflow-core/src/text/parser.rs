use crate::config::{StdTextReadConfig, TimeMeasNamePattern};
use crate::core::{NewCSVFlagsError, ScaleTransformError};
use crate::error::{BiTentative, DeferredResult, ResultExt, Tentative};
use crate::validated::keys::{
    BiIndexedKey, IndexedKey, Key, MeasHeader, NonStdKeywords, NonStdKeywordsExt, StdKey,
    StdKeywords,
};
use crate::validated::shortname::{Shortname, ShortnameError};

use super::byteord::{NewEndianError, ParseByteOrdError, Width};
use super::compensation::{NewCompError, ParseCompError};
use super::datetimes::{FCSDateTimeError, ReversedDatetimesError};
use super::gating;
use super::index::{IndexFromOne, MeasIndex};
use super::keywords::{
    AlphaNumTypeError, Analyte, Beginanalysis, Begindata, Calibration3_1, Calibration3_2,
    CalibrationError, CalibrationFormat3_1, CalibrationFormat3_2, DetectorName, DetectorType,
    DetectorVoltage, Dfc, Display, DisplayError, Endanalysis, Enddata, Feature, FeatureError, Gain,
    GatePairError, GatingError, LastModifiedError, Longname, MeasOrGateIndexError, Mode3_2Error,
    ModeError, NumType, NumTypeError, OpticalType, OpticalTypeError, OriginalityError,
    PercentEmitted, Power, PrefixedMeasIndexError, Range, RegionGateIndexError, RegionIndexError,
    Tag, TemporalScale, TemporalScaleError, TemporalTypeError, Timestep, Tot, TriggerError,
    UnicodeError, WavelengthsError,
};
use super::named_vec::{NameMapping, NewNamedVecError};
use super::optional::MaybeValue;
use super::ranged_float::RangedFloatError;
use super::scale::{Scale, ScaleError};
use super::spillover::{ParseSpilloverError, SpilloverIndexError};
use super::timestamps::{
    FCSDateError, FCSFixedTimeError, FCSTime100Error, FCSTime60Error, FCSTimeError,
    ReversedTimestampsError,
};
use super::unstainedcenters::ParseUnstainedCenterError;

use bigdecimal::ParseBigDecimalError;
use derive_more::{Display, From};
use derive_new::new;
use itertools::Itertools;
use nonempty::NonEmpty;
use num_traits::identities::One;
use thiserror::Error;

use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::fmt;
use std::num::{ParseFloatError, ParseIntError};
use std::str::FromStr;

#[cfg(feature = "python")]
use pyo3::prelude::*;

pub trait FromStrDelim: Sized {
    type Err;
    const DELIM: char;

    fn from_iter<'a>(iter: impl Iterator<Item = &'a str>) -> Result<Self, Self::Err>;

    fn from_str_delim(s: &str, trim_whitespace: bool) -> Result<Self, Self::Err> {
        let it = s.split(Self::DELIM);
        if trim_whitespace {
            Self::from_iter(it.map(str::trim))
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
            Some(v) => v.parse().map_err(|e| ParseKeyError::new(e, k, v).into()),
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
                .map_err(|e| ParseKeyError::new(e, k, v).into()),
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
        get_opt(kws, k).map(Into::into)
    }

    fn remove_opt(
        kws: &mut StdKeywords,
        k: StdKey,
    ) -> Result<MaybeValue<Self>, OptKeyError<Self::Err>>
    where
        Self: FromStr,
    {
        kws.remove(&k)
            .map(|v| v.parse().map_err(|e| OptKeyError::new(e, k, v)))
            .transpose()
            .map(Into::into)
    }

    fn remove_opt_st(
        kws: &mut StdKeywords,
        k: StdKey,
        data: Self::Payload<'_>,
        conf: &StdTextReadConfig,
    ) -> Result<MaybeValue<Self>, OptKeyError<Self::Err>>
    where
        Self: FromStrStateful,
    {
        kws.remove(&k)
            .map(|v| {
                Self::from_str_st(v.as_str(), data, conf).map_err(|e| OptKeyError::new(e, k, v))
            })
            .transpose()
            .map(Into::into)
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
            .map_err(ReqKeyError::inner_into)
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
    fn get_meas_req(kws: &StdKeywords, i: impl Into<IndexFromOne>) -> ReqResult<Self>
    where
        Self: FromStr,
    {
        Self::get_req(kws, Self::std(i))
    }

    fn remove_meas_req(kws: &mut StdKeywords, i: impl Into<IndexFromOne>) -> ReqResult<Self>
    where
        Self: FromStr,
    {
        Self::remove_req(kws, Self::std(i))
    }

    fn lookup_req(kws: &mut StdKeywords, i: impl Into<IndexFromOne>) -> LookupResult<Self>
    where
        Self: FromStr,
        ParseReqKeyError: From<<Self as FromStr>::Err>,
    {
        Self::remove_meas_req(kws, i)
            .map_err(ReqKeyError::inner_into)
            .map_err(Box::new)
            .into_deferred()
    }

    fn lookup_req_st(
        kws: &mut StdKeywords,
        i: impl Into<IndexFromOne>,
        data: Self::Payload<'_>,
        conf: &StdTextReadConfig,
    ) -> LookupResult<Self>
    where
        Self: FromStrStateful,
        ParseReqKeyError: From<<Self as FromStrStateful>::Err>,
    {
        Self::remove_req_st(kws, Self::std(i), data, conf)
            .map_err(ReqKeyError::inner_into)
            .map_err(Box::new)
            .into_deferred()
    }

    fn triple(&self, i: impl Into<IndexFromOne>) -> (MeasHeader, String, String)
    where
        Self: fmt::Display,
    {
        (
            Self::std_blank(),
            Self::std(i).to_string(),
            self.to_string(),
        )
    }

    fn pair(&self, i: impl Into<IndexFromOne>) -> (String, String)
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

    fn lookup_opt(kws: &mut StdKeywords, conf: &StdTextReadConfig) -> LookupOptional<Self>
    where
        Self: FromStr,
        ParseOptKeyError: From<<Self as FromStr>::Err>,
    {
        process_opt(Self::remove_metaroot_opt(kws), conf)
    }

    fn lookup_opt_st(
        kws: &mut StdKeywords,
        data: Self::Payload<'_>,
        conf: &StdTextReadConfig,
    ) -> LookupOptional<Self>
    where
        Self: FromStrStateful,
        ParseOptKeyError: From<<Self as FromStrStateful>::Err>,
    {
        process_opt(Self::remove_opt_st(kws, Self::std(), data, conf), conf)
    }

    fn lookup_opt_dep(kws: &mut StdKeywords, conf: &StdTextReadConfig) -> LookupOptional<Self>
    where
        Self: FromStr,
        ParseOptKeyError: From<<Self as FromStr>::Err>,
    {
        let mut x = Self::lookup_opt(kws, conf);
        eval_dep_maybe(&mut x, Self::std(), conf.disallow_deprecated);
        x
    }

    fn lookup_opt_st_dep(
        kws: &mut StdKeywords,
        disallow_dep: bool,
        data: Self::Payload<'_>,
        conf: &StdTextReadConfig,
    ) -> LookupOptional<Self>
    where
        Self: FromStrStateful,
        ParseOptKeyError: From<<Self as FromStrStateful>::Err>,
    {
        let mut x = Self::lookup_opt_st(kws, data, conf);
        eval_dep_maybe(&mut x, Self::std(), disallow_dep);
        x
    }

    fn root_pair(&self) -> (String, String)
    where
        Self: fmt::Display,
    {
        (Self::std().to_string(), self.to_string())
    }
}

/// Any optional key with an index
pub(crate) trait OptIndexedKey: Sized + Optional + IndexedKey {
    fn get_meas_opt(kws: &StdKeywords, i: impl Into<IndexFromOne>) -> OptKwResult<Self>
    where
        Self: FromStr,
    {
        Self::get_opt(kws, Self::std(i))
    }

    fn remove_meas_opt(kws: &mut StdKeywords, i: impl Into<IndexFromOne>) -> OptKwResult<Self>
    where
        Self: FromStr,
    {
        Self::remove_opt(kws, Self::std(i))
    }

    fn remove_meas_opt_st(
        kws: &mut StdKeywords,
        i: impl Into<IndexFromOne>,
        data: Self::Payload<'_>,
        conf: &StdTextReadConfig,
    ) -> Result<MaybeValue<Self>, OptKeyError<Self::Err>>
    where
        Self: FromStrStateful,
    {
        Self::remove_opt_st(kws, Self::std(i), data, conf)
    }

    fn lookup_opt(
        kws: &mut StdKeywords,
        i: impl Into<IndexFromOne>,
        conf: &StdTextReadConfig,
    ) -> LookupOptional<Self>
    where
        Self: FromStr,
        ParseOptKeyError: From<<Self as FromStr>::Err>,
    {
        process_opt(Self::remove_meas_opt(kws, i), conf)
    }

    fn lookup_opt_st(
        kws: &mut StdKeywords,
        i: impl Into<IndexFromOne> + Copy,
        data: Self::Payload<'_>,
        conf: &StdTextReadConfig,
    ) -> LookupOptional<Self>
    where
        Self: FromStrStateful,
        ParseOptKeyError: From<<Self as FromStrStateful>::Err>,
    {
        process_opt(Self::remove_opt_st(kws, Self::std(i), data, conf), conf)
    }

    fn lookup_opt_dep(
        kws: &mut StdKeywords,
        i: impl Into<IndexFromOne> + Copy,
        conf: &StdTextReadConfig,
    ) -> LookupOptional<Self>
    where
        Self: FromStr,
        ParseOptKeyError: From<<Self as FromStr>::Err>,
    {
        let mut x = Self::lookup_opt(kws, i, conf);
        eval_dep_maybe(&mut x, Self::std(i), conf.disallow_deprecated);
        x
    }

    fn lookup_opt_st_dep(
        kws: &mut StdKeywords,
        i: impl Into<IndexFromOne> + Copy,
        data: Self::Payload<'_>,
        conf: &StdTextReadConfig,
    ) -> LookupOptional<Self>
    where
        Self: FromStrStateful,
        ParseOptKeyError: From<<Self as FromStrStateful>::Err>,
    {
        let mut x = Self::lookup_opt_st(kws, i, data, conf);
        eval_dep_maybe(&mut x, Self::std(i), conf.disallow_deprecated);
        x
    }

    fn meas_pair(&self, i: impl Into<IndexFromOne>) -> (String, String)
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
            .map(|common_names| LinkedNameError::new(Self::std(), common_names))
            .map_or(Ok(()), Err)
    }

    // fn lookup_opt_linked<E>(
    //     kws: &mut StdKeywords,
    //     names: &HashSet<&Shortname>,
    // ) -> LookupOptional<Self, E>
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
    //         .map(Into::into)
    //     })
    // }

    fn lookup_opt_linked_st<P>(
        kws: &mut StdKeywords,
        names: &HashSet<&Shortname>,
        payload: P,
        conf: &StdTextReadConfig,
    ) -> LookupOptional<Self>
    where
        for<'a> Self: FromStrStateful<Payload<'a> = P>,
        ParseOptKeyError: From<<Self as FromStrStateful>::Err>,
    {
        // TODO not dry
        process_opt(Self::remove_opt_st(kws, Self::std(), payload, conf), conf).and_tentatively(
            |maybe| {
                if let Some(x) = maybe.0 {
                    Self::check_link(&x, names)
                        .map(|()| x)
                        .into_tentative_opt(!conf.allow_optional_dropping)
                        .inner_into()
                } else {
                    Tentative::new1(None)
                }
                .value_into()
            },
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
                .map_err(|error| OptKeyError::new(error, k, v.clone()))
        })
        .transpose()
}

#[derive(Debug, Error, new)]
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
    if conf.force_time_linear {
        nonstd.transfer_demoted(kws, TemporalScale::std(i));
        Ok(Tentative::new1(TemporalScale))
    } else {
        TemporalScale::lookup_req(kws, i)
    }
}

pub(crate) fn lookup_temporal_gain_3_0(
    kws: &mut StdKeywords,
    i: MeasIndex,
    nonstd: &mut NonStdKeywords,
    conf: &StdTextReadConfig,
) -> LookupOptional<Gain> {
    if conf.ignore_time_gain {
        nonstd.transfer_demoted(kws, Gain::std(i));
        Tentative::default()
    } else {
        let go = |gain: &MaybeValue<Gain>| {
            gain.0
                .is_some_and(|g| !g.0.is_one())
                .then_some(TemporalGainError(i))
        };
        let mut tnt_gain = Gain::lookup_opt(kws, i, conf);
        if conf.allow_optional_dropping {
            tnt_gain.eval_warning(go);
        } else {
            tnt_gain.eval_error(go);
        }
        tnt_gain
    }
}

pub(crate) fn process_opt<V, W>(
    res: Result<MaybeValue<V>, OptKeyError<W>>,
    conf: &StdTextReadConfig,
) -> LookupOptional<V>
where
    ParseOptKeyError: From<W>,
{
    res.map_err(|x| LookupKeysWarning::Parse(x.inner_into()))
        .into_tentative_def(!conf.allow_optional_dropping)
        .errors_into()
}

pub(crate) type RawKeywords = HashMap<String, String>;

pub(crate) type ReqResult<T> = Result<T, ReqKeyError<<T as FromStr>::Err>>;
pub(crate) type ReqStResult<T> = Result<T, ReqKeyError<<T as FromStrStateful>::Err>>;
pub(crate) type OptResult<T> = Result<Option<T>, OptKeyError<<T as FromStr>::Err>>;
pub(crate) type OptKwResult<T> = Result<MaybeValue<T>, OptKeyError<<T as FromStr>::Err>>;

pub(crate) type LookupResult<V> = DeferredResult<V, LookupKeysWarning, LookupKeysError>;
pub(crate) type LookupTentative<V> = BiTentative<V, LookupKeysWarning>;
pub(crate) type LookupOptional<V> = LookupTentative<MaybeValue<V>>;

/// Errors when looking up any key.
///
/// This is to be used in the error slot of any result-like types.
///
/// Includes errors from a variety of sources (relational vs local, optional vs
/// required, etc). It also includes all errors which may also be warnings
/// if configuration permits.
#[derive(From, Display, Debug, Error)]
pub enum LookupKeysError {
    Parse(Box<ReqKeyError<ParseReqKeyError>>),
    NamedVec(NewNamedVecError),
    InvalidScale(ScaleTransformError),
    WarnAsError(LookupKeysWarning),
}

/// Warnings when looking up keys.
///
/// This is separate from `LookupKeysError` since the latter includes errors
/// which are always fatal and this includes errors which are sometimes
/// non-fatal (aka warnings).
///
/// Generally, these are non-fatal because they apply to keys which can be
/// dropped on failure and become fatal if dropping is forbidden.
#[derive(From, Display, Debug, Error)]
pub enum LookupKeysWarning {
    Parse(OptKeyError<ParseOptKeyError>),
    Timestamp(ReversedTimestampsError),
    Datetime(ReversedDatetimesError),
    Comp(NewCompError),
    CSVFlag(NewCSVFlagsError),
    GateRegion(gating::MismatchedIndexAndWindowError),
    GateMeasLink(gating::GateMeasurementLinkError),
    GatingScheme(gating::NewGatingSchemeError),
    Spillover(SpilloverIndexError),
    LinkedName(LinkedNameError),
    LinkedIndex(RegionIndexError),
    TemporalGain(TemporalGainError),
    MissingTime(MissingTime),
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
pub enum ParseOptKeyError {
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

/// Error triggered when time measurement is missing but required.
#[derive(Debug, Error)]
#[error("Could not find time measurement matching {0}")]
pub struct MissingTime(pub TimeMeasNamePattern);

/// Error triggered when time measurement has $PnG
#[derive(Debug, Error)]
#[error("$P{0}G must be 1.0 or not set for temporal measurement")]
pub struct TemporalGainError(MeasIndex);

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
pub struct ParseKeyError<E> {
    #[new(into)]
    pub error: E,
    pub key: StdKey,
    pub value: String,
}

impl<E: fmt::Display> fmt::Display for ParseKeyError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let value = truncate_string(self.value.as_str(), 30);
        write!(
            f,
            "key '{}' with value '{value}' could not be parsed: {}",
            self.key, self.error
        )
    }
}

impl<E> ParseKeyError<E> {
    pub fn inner_into<F>(self) -> ParseKeyError<F>
    where
        F: From<E>,
    {
        ParseKeyError::new(self.error, self.key, self.value)
    }
}

#[derive(From, Debug, Error)]
pub enum ReqKeyError<E> {
    #[error("{0}")]
    Parse(ParseKeyError<E>),
    #[error("missing required key: {0}")]
    Missing(StdKey),
}

pub type OptKeyError<E> = ParseKeyError<E>;

impl<E> ReqKeyError<E> {
    pub fn inner_into<F>(self) -> ReqKeyError<F>
    where
        F: From<E>,
    {
        match self {
            Self::Parse(e) => ReqKeyError::Parse(e.inner_into()),
            Self::Missing(e) => ReqKeyError::Missing(e),
        }
    }
}

pub(crate) fn eval_dep_maybe<T>(x: &mut LookupOptional<T>, key: StdKey, disallow_dep: bool) {
    if disallow_dep {
        x.eval_error(|v| eval_dep(v, key));
    } else {
        x.eval_warning(|v| eval_dep(v, key));
    }
}

fn eval_dep<T>(v: &MaybeValue<T>, key: StdKey) -> Option<DeprecatedError> {
    v.0.is_some().then_some(DepKeyWarning(key).into())
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
        Self::new(kws, unused)
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

pub(crate) fn truncate_string(s: &str, n: usize) -> String {
    // TODO this is the length in bytes, not chars (ie doesn't care about utf-8)
    if s.len() > n {
        format!("{}…(more)", s.chars().take(n).collect::<String>())
    } else {
        s.to_string()
    }
}

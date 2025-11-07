use crate::config::{StdTextReadConfig, TimeMeasNamePattern};
use crate::core::{NewCSVFlagsError, ScaleTransformError};
use crate::logging::{
    DeferredWarningsAndErrors, LogResult, ResultExt as _, WarningsAndErrorsResult,
};
use crate::validated::keys::{
    BiIndex, BiIndexedKey as _, IndexedKey, Key, MeasHeader, SpecificKey, StdKey, StdKeywords,
};
use crate::validated::nonempty_string::NonEmptyStringError;
use crate::validated::shortname::{Shortname, ShortnameError};

use super::byteord::{NewEndianError, ParseByteOrdError, Width};
use super::compensation::{NewCompError, ParseCompError};
use super::datetimes::{FCSDateTimeError, ReversedDatetimesError};
use super::gating;
use super::index::{GateIndex, IndexFromOne, MeasIndex};
use super::keywords::{
    AlphaNumTypeError, Analyte, Beginanalysis, Begindata, Calibration3_1, Calibration3_2,
    CalibrationError, CalibrationFormat3_1, CalibrationFormat3_2, DetectorName, DetectorType,
    DetectorVoltage, Dfc, Display, DisplayError, Endanalysis, Enddata, Feature, FeatureError, Gain,
    GatePairError, Gating, GatingError, LastModifiedError, Longname, MeasOrGateIndex,
    MeasOrGateIndexError, Mode3_2Error, ModeError, NumType, NumTypeError, OpticalType,
    OpticalTypeError, OriginalityError, ParseUnstainedCenterError, PercentEmitted, Power,
    PrefixedMeasIndex, PrefixedMeasIndexError, Range, RegionGateIndexError, RegionLinkError, Tag,
    TemporalGainError, TemporalScaleError, TemporalTypeError, Timestep, Tot, TriggerError,
    UnicodeError, WavelengthsError,
};
use super::ranged_float::RangedFloatError;
use super::scale::{Scale, ScaleError};
use super::spillover::{ParseSpilloverError, Spillover};
use super::timestamps::{
    FCSDateError, FCSFixedTimeError, FCSTime60Error, FCSTime100Error, FCSTimeError,
    ReversedTimestampsError,
};

use bigdecimal::ParseBigDecimalError;
use derive_more::{Display, From};
use derive_new::new;
use itertools::Itertools as _;
use nonempty::NonEmpty;
use thiserror::Error;

use std::collections::HashMap;
use std::convert::Infallible;
use std::fmt;
use std::marker::PhantomData;
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
pub(crate) trait Required: Sized {
    fn get_req(kws: &StdKeywords, k: StdKey) -> ReqResult<Self>
    where
        Self: FromStr,
    {
        get_req(kws, k)
    }

    fn remove_req<F, OutE, InE>(kws: &mut StdKeywords, k: StdKey, f: F) -> Result<Self, OutE>
    where
        F: FnOnce(StdKey, String) -> Result<Self, OutE>,
        OutE: From<ReqKeyError<InE>>,
    {
        match kws.remove(&k) {
            Some(v) => f(k, v),
            None => Err(ReqKeyError::Missing(k).into()),
        }
    }
}

/// Any optional key
pub(crate) trait Optional: Sized {
    type Outer: Default + From<Self>;

    fn get_opt(kws: &StdKeywords, k: StdKey) -> OptKwResult<Self>
    where
        Self: FromStr,
    {
        get_opt(kws, k)
    }

    fn remove_opt<F, E>(kws: &mut StdKeywords, k: StdKey, f: F) -> Result<Self::Outer, E>
    where
        F: FnOnce(StdKey, String) -> Result<Self, E>,
    {
        kws.remove(&k)
            .map(|v| f(k, v))
            .transpose()
            .map(|x| x.map(Into::into).unwrap_or_default())
    }

    fn remove_opt_tnt<F, W, E>(
        kws: &mut StdKeywords,
        k: StdKey,
        f: F,
    ) -> DeferredWarningsAndErrors<Self::Outer, W, E>
    where
        F: FnOnce(StdKey, String) -> DeferredWarningsAndErrors<Option<Self>, W, E>,
    {
        kws.remove(&k)
            .map_or(LogResult::new_ok(Self::Outer::default()), |v| {
                f(k, v).map_def_value(|x| x.map(Into::into).unwrap_or_default())
            })
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
        Self::remove_req(kws, Self::std(), |k, v| {
            v.parse().map_err(|e| ParseKeyError::new(e, k, v).into())
        })
    }

    fn lookup_req(kws: &mut StdKeywords) -> LookupResult<Self>
    where
        Self: FromStr,
        ParseReqKeyError: From<ReqKeyError<<Self as FromStr>::Err>>,
    {
        Self::remove_metaroot_req(kws)
            .map_err(ParseReqKeyError::from)
            .map_err(LookupKeysError::from)
            .into_log()
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
        Self::remove_req(kws, Self::std(i), |k, v| {
            v.parse().map_err(|e| ParseKeyError::new(e, k, v).into())
        })
    }

    fn lookup_req(kws: &mut StdKeywords, i: impl Into<IndexFromOne>) -> LookupResult<Self>
    where
        Self: FromStr,
        ParseReqKeyError: From<ReqKeyError<<Self as FromStr>::Err>>,
    {
        Self::remove_meas_req(kws, i)
            .map_err(ParseReqKeyError::from)
            .map_err(LookupKeysError::from)
            .into_log()
    }

    fn lookup_req_st(
        kws: &mut StdKeywords,
        i: impl Into<IndexFromOne>,
        data: Self::Payload<'_>,
        conf: &StdTextReadConfig,
    ) -> LookupResult<Self>
    where
        Self: FromStrStateful,
        ParseReqKeyError: From<ReqKeyError<<Self as FromStrStateful>::Err>>,
    {
        Self::remove_req(kws, Self::std(i), |k, v| {
            Self::from_str_st(v.as_str(), data, conf)
                .map_err(|e| ParseKeyError::new(e, k, v).into())
        })
        .map_err(ParseReqKeyError::from)
        .map_err(LookupKeysError::from)
        .into_log()
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

    fn meas_pair(&self, i: impl Into<IndexFromOne>) -> (String, String)
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

    fn remove_metaroot_opt(
        kws: &mut StdKeywords,
    ) -> Result<Self::Outer, ParseKeyError<<Self as FromStr>::Err>>
    where
        Self: FromStr,
    {
        Self::remove_opt(kws, Self::std(), parse_opt)
    }

    // TODO it might be easier to move the deprecation flag to the type itself
    // so that way we don't need to pass a bool down a zillion layers worth of
    // call stack
    fn lookup_metaroot_opt(
        kws: &mut StdKeywords,
        is_deprecated: bool,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<Self::Outer>
    where
        Self: FromStr,
        ParseOptKeyError: From<OptKeyError<<Self as FromStr>::Err>>,
    {
        Self::remove_opt_tnt(kws, Self::std(), |k, v| {
            parse_opt_tnt(k, v, is_deprecated, conf)
        })
    }

    fn lookup_metaroot_opt_nofail(
        kws: &mut StdKeywords,
        is_deprecated: bool,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<Self::Outer>
    where
        Self: FromStr<Err = Infallible>,
    {
        Self::remove_opt_tnt(kws, Self::std(), |k, v| {
            parse_opt_nofail(k, v, is_deprecated, conf)
        })
    }

    fn lookup_metatroot_opt_st(
        kws: &mut StdKeywords,
        is_deprecated: bool,
        data: Self::Payload<'_>,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<Self::Outer>
    where
        Self: FromStrStateful,
        ParseOptKeyError: From<OptKeyError<<Self as FromStrStateful>::Err>>,
    {
        Self::remove_opt_tnt(kws, Self::std(), |k, v| {
            parse_opt_tnt_st(k, v, is_deprecated, data, conf)
        })
    }

    fn metaroot_pair_std(&self) -> (StdKey, String)
    where
        Self: fmt::Display,
    {
        (Self::std(), self.to_string())
    }

    fn metaroot_pair(&self) -> (String, String)
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

    // fn remove_meas_opt(kws: &mut StdKeywords, i: impl Into<IndexFromOne>) -> OptKwResult<Self>
    // where
    //     Self: FromStr,
    // {
    //     Self::remove_opt(kws, Self::std(i), |k, v| {
    //         v.parse().map_err(|e| OptKeyError::new(e, k, v))
    //     })
    // }

    // fn remove_meas_opt_st(
    //     kws: &mut StdKeywords,
    //     i: impl Into<IndexFromOne>,
    //     data: Self::Payload<'_>,
    //     conf: &StdTextReadConfig,
    // ) -> Result<MaybeValue<Self>, OptKeyError<Self::Err>>
    // where
    //     Self: FromStrStateful,
    // {
    //     Self::remove_opt(kws, Self::std(i), |k, v| {
    //         Self::from_str_st(v.as_str(), data, conf).map_err(|e| OptKeyError::new(e, k, v))
    //     })
    // }

    fn lookup_meas_opt(
        kws: &mut StdKeywords,
        i: impl Into<IndexFromOne>,
        is_deprecated: bool,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<Self::Outer>
    where
        Self: FromStr,
        ParseOptKeyError: From<OptKeyError<<Self as FromStr>::Err>>,
    {
        Self::remove_opt_tnt(kws, Self::std(i), |k, v| {
            parse_opt_tnt(k, v, is_deprecated, conf)
        })
    }

    fn lookup_meas_opt_nofail(
        kws: &mut StdKeywords,
        i: impl Into<IndexFromOne>,
        is_deprecated: bool,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<Self::Outer>
    where
        Self: FromStr<Err = Infallible>,
    {
        Self::remove_opt_tnt(kws, Self::std(i), |k, v| {
            parse_opt_nofail(k, v, is_deprecated, conf)
        })
    }

    fn lookup_meas_opt_st(
        kws: &mut StdKeywords,
        i: impl Into<IndexFromOne> + Copy,
        is_deprecated: bool,
        data: Self::Payload<'_>,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<Self::Outer>
    where
        Self: FromStrStateful,
        ParseOptKeyError: From<OptKeyError<<Self as FromStrStateful>::Err>>,
    {
        Self::remove_opt_tnt(kws, Self::std(i), |k, v| {
            parse_opt_tnt_st(k, v, is_deprecated, data, conf)
        })
    }

    fn meas_pair_std(&self, i: impl Into<IndexFromOne>) -> (StdKey, String)
    where
        Self: fmt::Display,
    {
        (Self::std(i), self.to_string())
    }
}

pub(crate) fn parse_opt<T: FromStr>(k: StdKey, v: String) -> Result<T, OptKeyError<T::Err>> {
    v.parse().map_err(|e| OptKeyError::new(e, k, v))
}

pub(crate) fn parse_opt_nofail<T>(
    k: StdKey,
    v: String,
    is_deprecated: bool,
    conf: &StdTextReadConfig,
) -> LookupTentative<Option<T>>
where
    T: FromStr<Err = Infallible>,
{
    let Ok(res) = parse_opt(k.clone(), v);
    eval_drop_and_deprecated(Ok(res), k, is_deprecated, conf)
}

pub(crate) fn parse_opt_tnt<T: FromStr>(
    k: StdKey,
    v: String,
    is_deprecated: bool,
    conf: &StdTextReadConfig,
) -> LookupTentative<Option<T>>
where
    ParseOptKeyError: From<OptKeyError<T::Err>>,
{
    let res = parse_opt(k.clone(), v)
        .map_err(ParseOptKeyError::from)
        .map_err(LookupKeysWarning::Parse);
    eval_drop_and_deprecated(res, k, is_deprecated, conf)
}

pub(crate) fn parse_opt_st<T: FromStrStateful>(
    k: StdKey,
    v: String,
    data: T::Payload<'_>,
    conf: &StdTextReadConfig,
) -> Result<T, OptKeyError<T::Err>> {
    T::from_str_st(v.as_str(), data, conf).map_err(|e| OptKeyError::new(e, k, v))
}

pub(crate) fn parse_opt_tnt_st<T: FromStrStateful>(
    k: StdKey,
    v: String,
    is_deprecated: bool,
    data: T::Payload<'_>,
    conf: &StdTextReadConfig,
) -> LookupTentative<Option<T>>
where
    ParseOptKeyError: From<OptKeyError<T::Err>>,
{
    let res = parse_opt_st(k.clone(), v, data, conf)
        .map_err(ParseOptKeyError::from)
        .map_err(LookupKeysWarning::Parse);
    eval_drop_and_deprecated(res, k, is_deprecated, conf)
}

pub(crate) fn eval_drop_and_deprecated<T>(
    res: Result<T, LookupKeysWarning>,
    k: StdKey,
    is_deprecated: bool,
    conf: &StdTextReadConfig,
) -> LookupTentative<Option<T>> {
    res.into_deferred_fungible_opt::<_, Vec<_>>(conf.allow_optional_dropping)
        .fungible_into_commutative()
        .and_then_def(|value| {
            let is_ok = !(is_deprecated && value.is_some());
            let flag = conf.disallow_deprecated;
            let error = LookupKeysWarning::from(DeprecatedError::Key(DepKeyWarning(k)));
            LogResult::new_deferred_fungible_ok_if(is_ok, value, error, flag)
                .fungible_into_commutative()
        })
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

#[derive(Debug, Display, Error, new)]
#[display(
    "{key} references non-existent $PnN: {bad}",
    bad = self.names.iter().join(", ")
)]
pub struct NameLinkError<T, I> {
    names: NonEmpty<Shortname>,
    key: SpecificKey<T, I>,
}

#[derive(Debug, Display, Error, new)]
#[display(
    "{key} references non-existent measurement indices: {bad}",
    bad = self.indices.iter().join(", ")
)]
pub struct IndexLinkError<T, I> {
    indices: NonEmpty<MeasIndex>,
    key: SpecificKey<T, I>,
}

pub type KeyToNameLinkError<T> = NameLinkError<T, ()>;

pub type KeyToIndexLinkError<T> = IndexLinkError<T, ()>;
pub type IndexedKeyToIndexLinkError<T> = IndexLinkError<T, IndexFromOne>;
pub type BiIndexedKeyToIndexLinkError<T> = IndexLinkError<T, BiIndex>;

impl<T> NameLinkError<T, ()> {
    pub(crate) fn new_i0(js: NonEmpty<Shortname>) -> Self {
        Self::new(js, SpecificKey::default())
    }
}

impl<T> IndexLinkError<T, ()> {
    pub(crate) fn new_i0(js: NonEmpty<MeasIndex>) -> Self {
        Self::new(js, SpecificKey::default())
    }
}

#[derive(Debug, Display, Error, new)]
#[display(bound(T: Key))]
#[display(
    "{key} depends on other keys which do not exist: {bad}",
    key = T::std(),
    bad = self.deps.iter().join(", "),

)]
pub struct DependentKeyError<T> {
    deps: NonEmpty<StdKey>,
    _key: PhantomData<T>,
}

#[derive(Debug, Display, Error, new)]
#[display(bound(T: IndexedKey))]
#[display(
    "{key} depends on other keys which do not exist: {bad}",
    key = T::std(self.key_index),
    bad = self.deps.iter().join(", "),

)]
pub struct DependentIndexKeyError<T> {
    deps: NonEmpty<StdKey>,
    key_index: IndexFromOne,
    _key: PhantomData<T>,
}

pub(crate) type RawKeywords = HashMap<String, String>;

pub(crate) type ReqResult<T> = Result<T, ReqKeyError<<T as FromStr>::Err>>;
pub(crate) type OptResult<T> = Result<Option<T>, OptKeyError<<T as FromStr>::Err>>;
pub(crate) type OptKwResult<T> = Result<Option<T>, OptKeyError<<T as FromStr>::Err>>;

pub(crate) type LookupResult<V> =
    WarningsAndErrorsResult<V, (), LookupKeysWarning, LookupKeysError>;
pub(crate) type LookupTentative<V> =
    DeferredWarningsAndErrors<V, LookupKeysWarning, LookupKeysWarning>;
pub(crate) type LookupOptional<V> = LookupTentative<Option<V>>;

/// Errors when looking up any key.
///
/// This is to be used in the error slot of any result-like types.
///
/// Includes errors from a variety of sources (relational vs local, optional vs
/// required, etc). It also includes all errors which may also be warnings
/// if configuration permits.
#[derive(From, Display, Debug, Error)]
pub enum LookupKeysError {
    Parse(ParseReqKeyError),
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
    Parse(ParseOptKeyError),
    Timestamp(ReversedTimestampsError),
    Datetime(ReversedDatetimesError),
    Comp(NewCompError),
    CSVFlag(NewCSVFlagsError),
    GateRegion(gating::MismatchedIndexAndWindowError),
    GateMeasLink(gating::GateMeasurementLinkError),
    GatingScheme(DependentKeyError<Gating>),
    Spillover(KeyToIndexLinkError<Spillover>),
    RegionIndex2_0(RegionLinkError<GateIndex>),
    RegionIndex3_0(RegionLinkError<MeasOrGateIndex>),
    RegionIndex3_2(RegionLinkError<PrefixedMeasIndex>),
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
    Range(ReqKeyError<ParseBigDecimalError>),
    AlphaNumType(ReqKeyError<AlphaNumTypeError>),
    NonEmptyString(ReqKeyError<NonEmptyStringError>),
    Int(ReqKeyError<ParseIntError>),
    Scale(ReqKeyError<ScaleError>),
    TemporalScale(ReqKeyError<TemporalScaleError>),
    RangedFloat(ReqKeyError<RangedFloatError>),
    Mode(ReqKeyError<ModeError>),
    ByteOrd(ReqKeyError<ParseByteOrdError>),
    Endian(ReqKeyError<NewEndianError>),
    Shortname(ReqKeyError<ShortnameError>),
}

/// Error encountered when parsing an optional key from a string
#[derive(From, Display, Debug, Error)]
pub enum ParseOptKeyError {
    NumType(OptKeyError<NumTypeError>),
    Trigger(OptKeyError<TriggerError>),
    Scale(OptKeyError<ScaleError>),
    TemporalScale(OptKeyError<TemporalScaleError>),
    Float(OptKeyError<ParseFloatError>),
    RangedFloat(OptKeyError<RangedFloatError>),
    Feature(OptKeyError<FeatureError>),
    Wavelengths(OptKeyError<WavelengthsError>),
    Calibration3_1(OptKeyError<CalibrationError<CalibrationFormat3_1>>),
    Calibration3_2(OptKeyError<CalibrationError<CalibrationFormat3_2>>),
    Int(OptKeyError<ParseIntError>),
    FCSDate(OptKeyError<FCSDateError>),
    FCSTime(OptKeyError<FCSFixedTimeError<FCSTimeError>>),
    FCSTime60(OptKeyError<FCSFixedTimeError<FCSTime60Error>>),
    FCSTime100(OptKeyError<FCSFixedTimeError<FCSTime100Error>>),
    FCSDateTime(OptKeyError<FCSDateTimeError>),
    ModifiedDateTime(OptKeyError<LastModifiedError>),
    Originality(OptKeyError<OriginalityError>),
    UnstainedCenter(OptKeyError<ParseUnstainedCenterError>),
    Mode3_2(OptKeyError<Mode3_2Error>),
    TemporalType(OptKeyError<TemporalTypeError>),
    OpticalType(OptKeyError<OpticalTypeError>),
    Shortname(OptKeyError<ShortnameError>),
    Display(OptKeyError<DisplayError>),
    Unicode(OptKeyError<UnicodeError>),
    Spillover(OptKeyError<ParseSpilloverError>),
    Compensation(OptKeyError<ParseCompError>),
    GateRange(OptKeyError<ParseBigDecimalError>),
    GateRegionIndex2_0(OptKeyError<RegionGateIndexError<ParseIntError>>),
    GateRegionIndex3_0(OptKeyError<RegionGateIndexError<MeasOrGateIndexError>>),
    GateRegionIndex3_2(OptKeyError<RegionGateIndexError<PrefixedMeasIndexError>>),
    GateRegionWindow(OptKeyError<GatePairError>),
    Gating(OptKeyError<GatingError>),
}

/// Error triggered when time measurement is missing but required.
#[derive(Debug, Error)]
#[error("Could not find time measurement matching {0}")]
pub struct MissingTime(pub TimeMeasNamePattern);

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
    // TODO replace this with a generic type to prevent storing/cloning a string
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

#[derive(From, Debug, Error)]
pub enum ReqKeyError<E> {
    #[error("{0}")]
    Parse(ParseKeyError<E>),
    #[error("missing required key: {0}")]
    Missing(StdKey),
}

pub type OptKeyError<E> = ParseKeyError<E>;

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
    // NOTE this is the length in bytes, not chars (ie doesn't care about
    // utf-8), since this is just meant to make strings "shorter" it doesn't
    // matter that much
    if s.len() > n {
        format!("{}…(more)", s.chars().take(n).collect::<String>())
    } else {
        s.into()
    }
}

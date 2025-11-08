use crate::config::{StdTextReadConfig, TimeMeasNamePattern};
use crate::core::{NewCSVFlagsError, ScaleTransformError};
use crate::logging::{
    DeferredWarningsAndErrors, LogResult, ResultExt as _, WarningsAndErrorsResult,
};
use crate::validated::keys::{
    AnyKey, BiIndex, BiIndexedKey as _, IndexedKey, Key, MeasHeader, SpecificKey, StdKey,
    StdKeywords,
};
use crate::validated::shortname::Shortname;

use super::byteord::{ByteOrd2_0, ByteOrd3_1, Width};
use super::compensation::{Compensation3_0, NewCompError};
use super::datetimes::{BeginDateTime, EndDateTime, ReversedDatetimesError};
use super::gating;
use super::index::{GateIndex, IndexFromOne, MeasIndex};
use super::keywords::{
    Abrt, AlphaNumType, Analyte, Beginanalysis, Begindata, CSMode, CSTot, CSVBits, CSVFlag,
    Calibration3_1, Calibration3_2, Cyt3_2, DetectorName, DetectorType, DetectorVoltage, Dfc,
    Display, Endanalysis, Enddata, Feature, Gain, Gate, GateDetectorType, GateDetectorVoltage,
    GateFilter, GateLongname, GatePercentEmitted, GateRange, GateScale, GateShortname, Gating,
    LastModified, Longname, Lost, MeasOrGateIndex, Mode, Mode3_2, NumType, OpticalType,
    Originality, Par, PeakBin, PeakIndex, PercentEmitted, Plateid, Platename, Power,
    PrefixedMeasIndex, Range, RegionGateIndex, RegionLinkError, RegionWindow, Tag,
    TemporalGainError, TemporalScale2_0, TemporalScale3_0, TemporalType, Timestep, Tot, Trigger,
    Unicode, UnstainedCenters, Vol, Wavelength, Wavelengths, Wellid,
};
use super::scale::Scale;
use super::spillover::Spillover;
use super::timestamps::{
    Btim, Etim, FCSDate, FCSTime, FCSTime60, FCSTime100, ReversedTimestampsError,
};

use derive_more::{Display, From};
use derive_new::new;
use itertools::Itertools as _;
use nonempty::NonEmpty;
use thiserror::Error;

use std::collections::HashMap;
use std::convert::Infallible;
use std::fmt;
use std::num::ParseFloatError;
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

pub trait FromStrWith: Sized {
    type Err;
    type Payload<'a>;

    fn from_str_with(
        _: &str,
        _: Self::Payload<'_>,
        _: &StdTextReadConfig,
    ) -> Result<Self, Self::Err>;
}

/// Any required key
pub(crate) trait Required: Sized {
    fn get_req<I>(kws: &StdKeywords, k: SpecificKey<Self, I>) -> ReqResult<Self, I>
    where
        SpecificKey<Self, I>: AnyKey,
        Self: FromStr,
    {
        get_req(kws, k)
    }

    fn remove_req<F, OutE, InE, I>(
        kws: &mut StdKeywords,
        k: SpecificKey<Self, I>,
        f: F,
    ) -> Result<Self, OutE>
    where
        SpecificKey<Self, I>: AnyKey,
        F: FnOnce(SpecificKey<Self, I>, String) -> Result<Self, OutE>,
        OutE: From<ReqKeyError_<InE, Self, I>>,
    {
        match kws.remove(&k.as_std()) {
            Some(v) => f(k, v),
            None => Err(ReqKeyError_::Missing(k).into()),
        }
    }
}

/// Any optional key
pub(crate) trait Optional: Sized {
    type Outer: Default + From<Self>;

    fn get_opt<I>(kws: &StdKeywords, k: SpecificKey<Self, I>) -> OptKwResult<Self, I>
    where
        SpecificKey<Self, I>: AnyKey,
        Self: FromStr,
    {
        get_opt(kws, k)
    }

    fn remove_opt<F, E, I>(
        kws: &mut StdKeywords,
        k: SpecificKey<Self, I>,
        f: F,
    ) -> Result<Self::Outer, E>
    where
        SpecificKey<Self, I>: AnyKey,
        F: FnOnce(SpecificKey<Self, I>, String) -> Result<Self, E>,
    {
        kws.remove(&k.as_std())
            .map(|v| f(k, v))
            .transpose()
            .map(|x| x.map(Into::into).unwrap_or_default())
    }

    fn remove_opt_tnt<F, W, E, I>(
        kws: &mut StdKeywords,
        k: SpecificKey<Self, I>,
        f: F,
    ) -> DeferredWarningsAndErrors<Self::Outer, W, E>
    where
        SpecificKey<Self, I>: AnyKey,
        F: FnOnce(SpecificKey<Self, I>, String) -> DeferredWarningsAndErrors<Option<Self>, W, E>,
    {
        kws.remove(&k.as_std())
            .map_or(LogResult::new_ok(Self::Outer::default()), |v| {
                f(k, v).map_def_value(|x| x.map(Into::into).unwrap_or_default())
            })
    }
}

/// A required metaroot key
pub(crate) trait ReqMetarootKey: Sized + Required + Key {
    fn get_metaroot_req(kws: &StdKeywords) -> ReqResult<Self, ()>
    where
        Self: FromStr,
    {
        Self::get_req(kws, SpecificKey::default())
    }

    fn remove_metaroot_req(kws: &mut StdKeywords) -> ReqResult<Self, ()>
    where
        Self: FromStr,
    {
        Self::remove_req(kws, SpecificKey::default(), |k, v| {
            v.parse().map_err(|e| ParseKeyError::new(e, k, v).into())
        })
    }

    fn lookup_req(kws: &mut StdKeywords) -> LookupResult<Self>
    where
        Self: FromStr,
        ParseReqKeyError: From<ReqKeyError<Self>>,
    {
        Self::remove_metaroot_req(kws)
            .map_err(ParseReqKeyError::from)
            .map_err(LookupKeysError::from)
            .into_log()
    }

    fn pair(&self) -> (String, String)
    where
        Self: fmt::Display,
    {
        (Self::std().to_string(), self.to_string())
    }
}

/// Any required key with one index
pub(crate) trait ReqIndexedKey: Sized + Required + IndexedKey {
    fn get_meas_req(kws: &StdKeywords, i: impl Into<IndexFromOne>) -> ReqResult<Self, IndexFromOne>
    where
        Self: FromStr,
    {
        Self::get_req(kws, SpecificKey::new_i1(i.into()))
    }

    fn remove_meas_req(
        kws: &mut StdKeywords,
        i: impl Into<IndexFromOne>,
    ) -> ReqResult<Self, IndexFromOne>
    where
        Self: FromStr,
    {
        Self::remove_req(kws, SpecificKey::new_i1(i.into()), |k, v| {
            v.parse().map_err(|e| ParseKeyError::new(e, k, v).into())
        })
    }

    fn lookup_req(kws: &mut StdKeywords, i: impl Into<IndexFromOne>) -> LookupResult<Self>
    where
        Self: FromStr,
        ParseReqKeyError: From<ReqIndexedKeyError<Self>>,
    {
        Self::remove_meas_req(kws, i)
            .map_err(ParseReqKeyError::from)
            .map_err(LookupKeysError::from)
            .into_log()
    }

    fn lookup_req_with(
        kws: &mut StdKeywords,
        i: impl Into<IndexFromOne>,
        data: Self::Payload<'_>,
        conf: &StdTextReadConfig,
    ) -> LookupResult<Self>
    where
        Self: FromStrWith,
        ParseReqKeyError: From<ReqIndexedStKeyError<Self>>,
    {
        Self::remove_req(kws, SpecificKey::new_i1(i.into()), |k, v| {
            Self::from_str_with(v.as_str(), data, conf)
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
    fn get_metaroot_opt(kws: &StdKeywords) -> OptKwResult<Self, ()>
    where
        Self: FromStr,
    {
        Self::get_opt(kws, SpecificKey::default())
    }

    fn remove_metaroot_opt(kws: &mut StdKeywords) -> Result<Self::Outer, OptKeyError<Self>>
    where
        Self: FromStr,
    {
        Self::remove_opt(kws, SpecificKey::default(), parse_opt).map_err(OptKeyError_::from)
    }

    fn lookup_metaroot_opt_noerror(kws: &mut StdKeywords) -> Self::Outer
    where
        Self: FromStr<Err = Infallible>,
        SpecificKey<Self, ()>: Copy,
    {
        let Ok(res) = Self::remove_opt(kws, SpecificKey::default(), parse_opt);
        res
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
        ParseOptKeyError: From<OptKeyError<Self>>,
    {
        Self::remove_opt_tnt(kws, SpecificKey::default(), |k, v| {
            parse_opt_tnt(k, v, is_deprecated, conf)
                .map_errors(ParseOptKeyError::from)
                .map_commutative_warnings(ParseOptKeyError::from)
                .map_errors(LookupKeysWarning::from)
                .map_commutative_warnings(LookupKeysWarning::from)
        })
    }

    fn lookup_metatroot_opt_with(
        kws: &mut StdKeywords,
        is_deprecated: bool,
        data: Self::Payload<'_>,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<Self::Outer>
    where
        Self: FromStrWith,
        ParseOptKeyError: From<OptKeyStError<Self>>,
    {
        Self::remove_opt_tnt(kws, SpecificKey::default(), |k, v| {
            parse_opt_tnt_with(k, v, is_deprecated, data, conf)
                .map_errors(ParseOptKeyError::from)
                .map_commutative_warnings(ParseOptKeyError::from)
                .map_errors(LookupKeysWarning::from)
                .map_commutative_warnings(LookupKeysWarning::from)
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
    fn get_meas_opt(
        kws: &StdKeywords,
        i: impl Into<IndexFromOne>,
    ) -> OptKwResult<Self, IndexFromOne>
    where
        Self: FromStr,
    {
        Self::get_opt(kws, SpecificKey::new_i1(i.into()))
    }

    fn lookup_meas_opt_noerror(kws: &mut StdKeywords, i: impl Into<IndexFromOne>) -> Self::Outer
    where
        Self: FromStr<Err = Infallible>,
        SpecificKey<Self, ()>: Copy,
    {
        let Ok(res) = Self::remove_opt(kws, SpecificKey::new_i1(i.into()), parse_opt);
        res
    }

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
        ParseOptKeyError: From<OptIndexedKeyError<Self>>,
    {
        Self::remove_opt_tnt(kws, SpecificKey::new_i1(i.into()), |k, v| {
            parse_opt_tnt(k, v, is_deprecated, conf)
                .map_errors(ParseOptKeyError::from)
                .map_commutative_warnings(ParseOptKeyError::from)
                .map_errors(LookupKeysWarning::from)
                .map_commutative_warnings(LookupKeysWarning::from)
        })
    }

    fn lookup_meas_opt_with(
        kws: &mut StdKeywords,
        i: impl Into<IndexFromOne> + Copy,
        is_deprecated: bool,
        data: Self::Payload<'_>,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<Self::Outer>
    where
        Self: FromStrWith,
        ParseOptKeyError: From<OptIndexedKeyStError<Self>>,
    {
        Self::remove_opt_tnt(kws, SpecificKey::new_i1(i.into()), |k, v| {
            parse_opt_tnt_with(k, v, is_deprecated, data, conf)
                .map_errors(ParseOptKeyError::from)
                .map_commutative_warnings(ParseOptKeyError::from)
                .map_errors(LookupKeysWarning::from)
                .map_commutative_warnings(LookupKeysWarning::from)
        })
    }

    fn meas_pair_std(&self, i: impl Into<IndexFromOne>) -> (StdKey, String)
    where
        Self: fmt::Display,
    {
        (Self::std(i), self.to_string())
    }
}

pub(crate) fn parse_opt<T: FromStr, I>(
    k: SpecificKey<T, I>,
    v: String,
) -> Result<T, ParseKeyError<T::Err, T, I>> {
    v.parse().map_err(|e| ParseKeyError::new(e, k, v))
}

pub(crate) fn parse_opt_tnt<T: FromStr, I>(
    k: SpecificKey<T, I>,
    v: String,
    is_deprecated: bool,
    conf: &StdTextReadConfig,
) -> OptResult_<Option<T>, T::Err, T, I>
where
    SpecificKey<T, I>: Copy + AnyKey,
{
    let res = parse_opt(k, v).map_err(OptKeyError_::from);
    eval_drop_and_deprecated(res, k, is_deprecated, conf)
}

pub(crate) fn parse_opt_with<T: FromStrWith, I>(
    k: SpecificKey<T, I>,
    v: String,
    data: T::Payload<'_>,
    conf: &StdTextReadConfig,
) -> Result<T, OptKeyError_<T::Err, T, I>> {
    T::from_str_with(v.as_str(), data, conf)
        .map_err(|e| ParseKeyError::new(e, k, v))
        .map_err(OptKeyError_::from)
}

pub(crate) fn parse_opt_tnt_with<T: FromStrWith, I>(
    k: SpecificKey<T, I>,
    v: String,
    is_deprecated: bool,
    data: T::Payload<'_>,
    conf: &StdTextReadConfig,
) -> OptResult_<Option<T>, T::Err, T, I>
where
    SpecificKey<T, I>: Copy + AnyKey,
{
    let res = parse_opt_with(k, v, data, conf);
    eval_drop_and_deprecated(res, k, is_deprecated, conf)
}

pub(crate) fn eval_drop_and_deprecated<E, T, I>(
    res: Result<T, OptKeyError_<E, T, I>>,
    k: SpecificKey<T, I>,
    is_deprecated: bool,
    conf: &StdTextReadConfig,
) -> OptResult_<Option<T>, E, T, I>
where
    SpecificKey<T, I>: AnyKey,
{
    res.into_deferred_fungible_opt::<_, Vec<_>>(conf.allow_optional_dropping)
        .fungible_into_commutative()
        .and_then_def(|value| {
            let is_ok = !(is_deprecated && value.is_some());
            let flag = conf.disallow_deprecated;
            let error = OptKeyError_::Deprecated(k);
            LogResult::new_deferred_fungible_ok_if(is_ok, value, error, flag)
                .fungible_into_commutative()
        })
}

type OptResult_<R, E, T, I> =
    DeferredWarningsAndErrors<R, OptKeyError_<E, T, I>, OptKeyError_<E, T, I>>;

/// Find a required standard key in a hash table
pub(crate) fn get_req<T, I>(kws: &StdKeywords, k: SpecificKey<T, I>) -> ReqResult<T, I>
where
    SpecificKey<T, I>: AnyKey,
    T: FromStr,
{
    match kws.get(&k.as_std()) {
        Some(v) => v
            .parse()
            .map_err(|error| ParseKeyError::new(error, k, v.clone()))
            .map_err(ReqKeyError_::Parse),
        None => Err(ReqKeyError_::Missing(k)),
    }
}

/// Find an optional standard key in a hash table
pub(crate) fn get_opt<T, I>(kws: &StdKeywords, k: SpecificKey<T, I>) -> OptResult<T, I>
where
    SpecificKey<T, I>: AnyKey,
    T: FromStr,
{
    kws.get(&k.as_std())
        .map(|v| {
            v.parse()
                .map_err(|error| OptKeyError_::from(ParseKeyError::new(error, k, v.clone())))
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
#[display(
    "{key} depends on other keys which do not exist: {bad}",
    bad = self.deps.iter().join(", "),

)]
pub struct DependentKeyError_<T, I> {
    deps: NonEmpty<StdKey>,
    key: SpecificKey<T, I>,
}

pub type DependentKeyError<T> = DependentKeyError_<T, ()>;
pub type DependentIndexedKeyError<T> = DependentKeyError_<T, IndexFromOne>;

impl<T> DependentKeyError<T> {
    pub(crate) fn new1(deps: NonEmpty<StdKey>) -> Self {
        Self::new(deps, SpecificKey::default())
    }
}

impl<T> DependentIndexedKeyError<T> {
    pub(crate) fn new2(i: IndexFromOne, deps: NonEmpty<StdKey>) -> Self {
        Self::new(deps, SpecificKey::new_i1(i))
    }
}

pub(crate) type RawKeywords = HashMap<String, String>;

pub(crate) type ReqResult<T, I> = Result<T, ReqKeyError_<<T as FromStr>::Err, T, I>>;
pub(crate) type OptResult<T, I> = Result<Option<T>, OptKeyError_<<T as FromStr>::Err, T, I>>;
pub(crate) type OptKwResult<T, I> = Result<Option<T>, OptKeyError_<<T as FromStr>::Err, T, I>>;

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
    Dep(DepValueWarning),
}

// TODO break these up to be more context-specific (layout vs metaroot etc)
/// Error encountered when parsing a required key from a string
#[derive(From, Display, Debug, Error)]
pub enum ParseReqKeyError {
    AlphaNumType(ReqKeyError<AlphaNumType>),
    Scale(ReqIndexedKeyError<Scale>),
    TemporalScale(ReqIndexedKeyError<TemporalScale3_0>),
    Mode(ReqKeyError<Mode>),
    ByteOrd2_0(ReqKeyError<ByteOrd2_0>),
    ByteOrd3_1(ReqKeyError<ByteOrd3_1>),
    Shortname(ReqIndexedKeyError<Shortname>),
    Width(ReqIndexedKeyError<Width>),
    Range(ReqIndexedKeyError<Range>),
    Cyt3_2(ReqKeyError<Cyt3_2>),
    Par(ReqKeyError<Par>),
    Timestepe(ReqKeyError<Timestep>),
}

/// Error encountered when parsing an optional key from a string
#[derive(From, Display, Debug, Error)]
pub enum ParseOptKeyError {
    NumType(OptIndexedKeyError<NumType>),
    Trigger(OptKeyStError<Trigger>),
    Scale(OptIndexedKeyStError<Scale>),
    TemporalScale(OptIndexedKeyError<TemporalScale2_0>),
    Comp2_0(OptKeyError_<ParseFloatError, Dfc, BiIndex>),
    Comp3_0(OptKeyError<Compensation3_0>),
    Gain(OptIndexedKeyError<Gain>),
    Feature(OptIndexedKeyError<Feature>),
    Wavelengths(OptIndexedKeyStError<Wavelengths>),
    Calibration3_1(OptIndexedKeyError<Calibration3_1>),
    Calibration3_2(OptIndexedKeyError<Calibration3_2>),
    Date(OptKeyStError<FCSDate>),
    Btim2_0(OptKeyStError<Btim<FCSTime>>),
    Etim2_0(OptKeyStError<Etim<FCSTime>>),
    Btim3_0(OptKeyStError<Btim<FCSTime60>>),
    Etim3_0(OptKeyStError<Etim<FCSTime60>>),
    Btim3_1(OptKeyStError<Btim<FCSTime100>>),
    Etim3_1(OptKeyStError<Etim<FCSTime100>>),
    Begindatetime(OptKeyError<BeginDateTime>),
    Enddatetime(OptKeyError<EndDateTime>),
    ModifiedDateTime(OptKeyError<LastModified>),
    Originality(OptKeyError<Originality>),
    UnstainedCenter(OptKeyStError<UnstainedCenters>),
    Mode3_2(OptKeyError<Mode3_2>),
    TemporalType(OptIndexedKeyError<TemporalType>),
    OpticalType(OptIndexedKeyError<OpticalType>),
    Shortname(OptIndexedKeyError<Shortname>),
    Display(OptIndexedKeyError<Display>),
    Unicode(OptKeyStError<Unicode>),
    Spillover(OptKeyStError<Spillover>),
    GateRegionIndex2_0(OptIndexedKeyError<RegionGateIndex<GateIndex>>),
    GateRegionIndex3_0(OptIndexedKeyError<RegionGateIndex<MeasOrGateIndex>>),
    GateRegionIndex3_2(OptIndexedKeyError<RegionGateIndex<PrefixedMeasIndex>>),
    GateRegionWindow(OptIndexedKeyError<RegionWindow>),
    Gating(OptKeyError<Gating>),
    Gate(OptKeyError<Gate>),
    GateScale(OptIndexedKeyError<GateScale>),
    GateFilter(OptIndexedKeyError<GateFilter>),
    GateShortname(OptIndexedKeyError<GateShortname>),
    GatePercentEmitted(OptIndexedKeyError<GatePercentEmitted>),
    GateRange(OptIndexedKeyError<GateRange>),
    GateLongname(OptIndexedKeyError<GateLongname>),
    GateDetectorType(OptIndexedKeyError<GateDetectorType>),
    GateDetectorVoltage(OptIndexedKeyError<GateDetectorVoltage>),
    Vol(OptKeyError<Vol>),
    Power(OptIndexedKeyError<Power>),
    PercentEmitted(OptIndexedKeyError<PercentEmitted>),
    DetectorVoltage(OptIndexedKeyError<DetectorVoltage>),
    Abrt(OptKeyError<Abrt>),
    Lost(OptKeyError<Lost>),
    CSVBits(OptKeyError<CSVBits>),
    CSVFlag(OptIndexedKeyError<CSVFlag>),
    CSMode(OptKeyError<CSMode>),
    CSTot(OptKeyError<CSTot>),
    PeakBin(OptIndexedKeyError<PeakBin>),
    PeakIndex(OptIndexedKeyError<PeakIndex>),
    Wavelength(OptIndexedKeyError<Wavelength>),
    // TODO these actually can't fail, the only reason they are here is because
    // they can be deprecated
    Wellid(OptKeyError<Wellid>),
    Platename(OptKeyError<Platename>),
    Plateid(OptKeyError<Plateid>),
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
pub struct ParseKeyError<E, T, I> {
    #[new(into)]
    pub error: E,
    pub key: SpecificKey<T, I>,
    pub value: String,
}

impl<E, T, I> fmt::Display for ParseKeyError<E, T, I>
where
    E: fmt::Display,
    SpecificKey<T, I>: fmt::Display,
{
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
pub enum ReqKeyError_<E, T, I> {
    #[error("{0}")]
    Parse(ParseKeyError<E, T, I>),
    #[error("missing required key: {0}")]
    Missing(SpecificKey<T, I>),
}

#[derive(From, Debug, Error)]
pub enum OptKeyError_<E, T, I> {
    #[error("{0}")]
    Parse(ParseKeyError<E, T, I>),
    #[error("deprecated: {0}")]
    Deprecated(SpecificKey<T, I>),
}

pub type ReqKeyError<T> = ReqKeyError_<<T as FromStr>::Err, T, ()>;
pub type ReqIndexedKeyError<T> = ReqKeyError_<<T as FromStr>::Err, T, IndexFromOne>;

// pub type ReqKeyStError<T> = ReqKeyError_<<T as FromStrWith>::Err, T, ()>;
pub type ReqIndexedStKeyError<T> = ReqKeyError_<<T as FromStrWith>::Err, T, IndexFromOne>;

pub type OptKeyError<T> = OptKeyError_<<T as FromStr>::Err, T, ()>;
pub type OptIndexedKeyError<T> = OptKeyError_<<T as FromStr>::Err, T, IndexFromOne>;

pub type OptKeyStError<T> = OptKeyError_<<T as FromStrWith>::Err, T, ()>;
pub type OptIndexedKeyStError<T> = OptKeyError_<<T as FromStrWith>::Err, T, IndexFromOne>;

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

#[cfg(feature = "python")]
mod python {
    use crate::{
        data::RawParsedError,
        python::macros::{impl_from_pyerr, impl_pyreflow_err},
        text::keywords::{Nextdata, NumType, Par, Tot},
    };

    use super::{
        DepValueWarning, LookupKeysError, LookupKeysWarning, MissingTime, OptIndexedKeyError,
        OptKeyError, ParseOptKeyError, ParseReqKeyError, PseudostandardError, ReqKeyError,
        UnusedStandardError,
    };

    impl_pyreflow_err!(InvalidKeywordValueError, PseudostandardError);
    impl_pyreflow_err!(InvalidKeywordValueError, UnusedStandardError);
    impl_pyreflow_err!(InvalidKeywordValueError, ParseReqKeyError);
    impl_pyreflow_err!(InvalidKeywordValueError, ParseOptKeyError);

    // These are file layout errors despite being keywords since they contain
    // data pertaining to the byte layout of the file
    //
    //  TODO maybe...
    impl_pyreflow_err!(FileLayoutError, ReqKeyError<Tot>);
    impl_pyreflow_err!(FileLayoutError, OptKeyError<Tot>);
    impl_pyreflow_err!(FileLayoutError, ReqKeyError<Par>);
    impl_pyreflow_err!(FileLayoutError, OptKeyError<Nextdata>);
    impl_pyreflow_err!(FileLayoutError, OptIndexedKeyError<NumType>);
    impl_pyreflow_err!(FileLayoutError, RawParsedError);

    impl_pyreflow_err!(RelationalException, MissingTime);

    impl_pyreflow_err!(FCSDeprecatedError, DepValueWarning);

    impl_from_pyerr!(LookupKeysError, Parse, InvalidScale, WarnAsError);
    impl_from_pyerr!(
        LookupKeysWarning,
        Parse,
        Timestamp,
        Datetime,
        Comp,
        CSVFlag,
        GateRegion,
        GateMeasLink,
        GatingScheme,
        Spillover,
        RegionIndex2_0,
        RegionIndex3_0,
        RegionIndex3_2,
        TemporalGain,
        MissingTime,
        Dep
    );
}

use crate::config::{
    AllowOptionalDropping, ConfigFlag as _, StdTextReadConfig, TimeMeasNamePattern,
};
use crate::core::ScaleTransformError;
use crate::logging::{
    DeferredSwitchableError, ResultExt as _, WarningAndErrorResult, WarningsAndErrorsResult,
};
use crate::macros::match_many_to_one;
use crate::validated::keys::{
    AnyKey, BiIndex, BiIndexedKey as _, IndexedKey, Key, Key0, Key1, MeasHeader, NonStdKeywords,
    NonStdKeywordsExt as _, NonStdMeasRegexError, SpecificKey, StdKey, StdKeywords,
};
use crate::validated::shortname::Shortname;

use super::byteord::Width;
use super::compensation::{Compensation3_0, LookupComp2_0Error};
use super::datetimes::LookupDatetimesError;
use super::gating::{
    LookupAppliedGates2_0Error, LookupAppliedGates3_0Error, LookupAppliedGates3_2Error, Region,
};
use super::index::{IndexFromOne, MeasIndex, RegionIndex};
use super::keywords::{
    Abrt, Analyte, Beginanalysis, Begindata, CSMode, CSTot, CSVBits, CSVFlag, Calibration3_1,
    Calibration3_2, Cyt3_2, DetectorName, DetectorType, DetectorVoltage, Dfc, Display, Endanalysis,
    Enddata, Feature, Gain, GateDetectorType, GateDetectorVoltage, GateFilter, GateLongname,
    GatePercentEmitted, GateRange, GateScale, GateShortname, Gating, LastModified, Longname,
    LookupTemporalGain, Lost, Mode, Mode3_2, NumType, OpticalType, Originality, Par, PeakBin,
    PeakIndex, PercentEmitted, Plateid, Platename, Power, PrefixedMeasIndex, Range,
    RegionGateIndex, RegionWindow, Tag, TemporalScale2_0, TemporalScale3_0, TemporalType, Timestep,
    Tot, Trigger, Unicode, UnstainedCenters, Vol, Wavelength, Wavelengths, Wellid,
};
use super::optional::DisplayMaybe;
use super::scale::Scale;
use super::spillover::Spillover;
use super::timestamps::{
    Btim, Etim, FCSDate, FCSTime, FCSTime60, FCSTime60Error, FCSTime100, FCSTime100Error,
    FCSTimeError, LookupTimestampsError,
};

use derive_more::{Display, From};
use derive_new::new;
use itertools::Itertools as _;
use nonempty::NonEmpty;
use thiserror::Error;

use std::collections::HashMap;
use std::convert::Infallible;
use std::fmt;
use std::mem::take;
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
    fn get_req<I>(
        kws: &StdKeywords,
        k: SpecificKey<Self, I>,
    ) -> Result<Self, ReqKeyError_<Self::Err, Self, I>>
    where
        SpecificKey<Self, I>: AnyKey + Copy,
        Self: FromStr,
    {
        let v = Self::get_req_inner(kws, k).map_err(ReqKeyError_::from)?;
        v.parse()
            .map_err(|e| ParseKeyError::new(e, k, v.to_owned()))
            .map_err(ReqKeyError_::from)
    }

    fn remove_req<I>(
        kws: &mut StdKeywords,
        k: SpecificKey<Self, I>,
    ) -> Result<Self, ReqKeyError_<Self::Err, Self, I>>
    where
        SpecificKey<Self, I>: AnyKey + Copy,
        Self: FromStr,
    {
        let v = Self::remove_req_inner(kws, k).map_err(ReqKeyError_::from)?;
        v.parse()
            .map_err(|e| ParseKeyError::new(e, k, v))
            .map_err(ReqKeyError_::from)
    }

    fn remove_req_with<I>(
        kws: &mut StdKeywords,
        k: SpecificKey<Self, I>,
        data: Self::Payload<'_>,
        conf: &StdTextReadConfig,
    ) -> Result<Self, ReqKeyError_<Self::Err, Self, I>>
    where
        SpecificKey<Self, I>: AnyKey + Copy,
        Self: FromStrWith,
    {
        let v = Self::remove_req_inner(kws, k).map_err(ReqKeyError_::from)?;
        Self::from_str_with(&v, data, conf)
            .map_err(|e| ParseKeyError::new(e, k, v))
            .map_err(ReqKeyError_::from)
    }

    fn get_req_inner<I>(
        kws: &StdKeywords,
        k: SpecificKey<Self, I>,
    ) -> Result<&str, MissingKeyError<Self, I>>
    where
        SpecificKey<Self, I>: AnyKey,
    {
        match kws.get(&k.as_std()) {
            Some(v) => Ok(v),
            None => Err(MissingKeyError(k)),
        }
    }

    fn remove_req_inner<I>(
        kws: &mut StdKeywords,
        k: SpecificKey<Self, I>,
    ) -> Result<String, MissingKeyError<Self, I>>
    where
        SpecificKey<Self, I>: AnyKey,
    {
        match kws.remove(&k.as_std()) {
            Some(v) => Ok(v),
            None => Err(MissingKeyError(k)),
        }
    }
}

/// Any optional key
pub(crate) trait Optional: Sized {
    type Outer: Default + From<Self>;

    fn get_opt<I>(
        kws: &StdKeywords,
        k: SpecificKey<Self, I>,
    ) -> Result<Self::Outer, ParseKeyError<Self::Err, Self, I>>
    where
        SpecificKey<Self, I>: AnyKey,
        Self: FromStr,
    {
        Self::get_opt_inner(kws, k, |k_, v| {
            v.parse()
                .map_err(|e| ParseKeyError::new(e, k_, v.to_owned()))
        })
    }

    fn remove_opt<I>(
        kws: &mut StdKeywords,
        k: SpecificKey<Self, I>,
    ) -> Result<Self::Outer, ParseKeyError<Self::Err, Self, I>>
    where
        SpecificKey<Self, I>: AnyKey,
        Self: FromStr,
    {
        Self::remove_opt_inner(kws, k, |k_, v| {
            v.parse().map_err(|e| ParseKeyError::new(e, k_, v))
        })
    }

    fn remove_opt_with<I>(
        kws: &mut StdKeywords,
        k: SpecificKey<Self, I>,
        data: Self::Payload<'_>,
        conf: &StdTextReadConfig,
    ) -> Result<Self::Outer, ParseKeyError<Self::Err, Self, I>>
    where
        SpecificKey<Self, I>: AnyKey,
        Self: FromStrWith,
    {
        Self::remove_opt_inner(kws, k, |k_, v| {
            Self::from_str_with(v.as_str(), data, conf).map_err(|e| ParseKeyError::new(e, k_, v))
        })
    }

    fn remove_opt_nofail<I>(kws: &mut StdKeywords, k: SpecificKey<Self, I>) -> Self::Outer
    where
        SpecificKey<Self, I>: AnyKey,
        Self: FromStr<Err = Infallible>,
    {
        let Ok(res) = Self::remove_opt(kws, k);
        res
    }

    fn transfer_opt<I>(
        kws: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        k: SpecificKey<Self, I>,
        conf: &StdTextReadConfig,
    ) -> Result<Self::Outer, ParseKeyError<Self::Err, Self, I>>
    where
        SpecificKey<Self, I>: AnyKey + Copy,
        Self: FromStr,
    {
        Self::remove_opt(kws, k).inspect_err(|e| {
            if conf.transfer_dropped_optional.is_set() {
                nonstd.insert_demoted(k.as_std(), e.value.clone());
            }
        })
    }

    fn transfer_opt_with<I>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        k: SpecificKey<Self, I>,
        data: Self::Payload<'_>,
        conf: &StdTextReadConfig,
    ) -> Result<Self::Outer, ParseKeyError<Self::Err, Self, I>>
    where
        SpecificKey<Self, I>: AnyKey + Copy,
        Self: FromStrWith,
    {
        Self::remove_opt_with(std, k, data, conf).inspect_err(|e| {
            if conf.transfer_dropped_optional.is_set() {
                nonstd.insert_demoted(k.as_std(), e.value.clone());
            }
        })
    }

    fn drop_opt<I>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        k: SpecificKey<Self, I>,
        conf: &StdTextReadConfig,
    ) -> DeferredSwitchableError<
        Self::Outer,
        AllowOptionalDropping,
        ParseKeyError<Self::Err, Self, I>,
    >
    where
        SpecificKey<Self, I>: AnyKey + Copy,
        Self: FromStr,
    {
        Self::transfer_opt(std, nonstd, k, conf)
            .into_nowarn1()
            .set_err_value(Self::Outer::default())
            .nowarn_into_switchable(conf.allow_optional_dropping)
    }

    fn drop_opt_with<I>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        k: SpecificKey<Self, I>,
        data: Self::Payload<'_>,
        conf: &StdTextReadConfig,
    ) -> DeferredSwitchableError<
        Self::Outer,
        AllowOptionalDropping,
        ParseKeyError<Self::Err, Self, I>,
    >
    where
        SpecificKey<Self, I>: AnyKey + Copy,
        Self: FromStrWith,
    {
        Self::transfer_opt_with(std, nonstd, k, data, conf)
            .into_nowarn1()
            .set_err_value(Self::Outer::default())
            .nowarn_into_switchable(conf.allow_optional_dropping)
    }

    fn get_opt_inner<F, E, I>(
        kws: &StdKeywords,
        k: SpecificKey<Self, I>,
        f: F,
    ) -> Result<Self::Outer, E>
    where
        SpecificKey<Self, I>: AnyKey,
        F: FnOnce(SpecificKey<Self, I>, &str) -> Result<Self, E>,
    {
        kws.get(&k.as_std())
            .map(|v| f(k, v))
            .transpose()
            .map(|x| x.map(Self::Outer::from).unwrap_or_default())
    }

    fn remove_opt_inner<F, E, I>(
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
            .map(|x| x.map(Self::Outer::from).unwrap_or_default())
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
        Self::remove_req(kws, SpecificKey::default())
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
        Self::remove_req(kws, SpecificKey::new_i1(i.into()))
    }

    fn remove_meas_req_with(
        kws: &mut StdKeywords,
        i: impl Into<IndexFromOne>,
        data: Self::Payload<'_>,
        conf: &StdTextReadConfig,
    ) -> Result<Self, ReqIndexedStKeyError<Self>>
    where
        Self: FromStrWith,
    {
        Self::remove_req_with(kws, SpecificKey::new_i1(i.into()), data, conf)
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
    fn get_metaroot_opt(kws: &StdKeywords) -> Result<Self::Outer, OptKeyError<Self>>
    where
        Self: FromStr,
    {
        Self::get_opt(kws, SpecificKey::default())
    }

    fn remove_metaroot_opt(kws: &mut StdKeywords) -> Result<Self::Outer, OptKeyError<Self>>
    where
        Self: FromStr,
    {
        Self::remove_opt(kws, SpecificKey::default())
    }

    fn remove_metaroot_opt_nofail(kws: &mut StdKeywords) -> Self::Outer
    where
        Self: FromStr<Err = Infallible>,
    {
        Self::remove_opt_nofail(kws, SpecificKey::default())
    }

    fn transfer_metaroot_opt(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        conf: &StdTextReadConfig,
    ) -> Result<Self::Outer, OptKeyError<Self>>
    where
        Self: FromStr,
    {
        Self::transfer_opt(std, nonstd, SpecificKey::default(), conf)
    }

    fn transfer_metaroot_opt_with(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        data: Self::Payload<'_>,
        conf: &StdTextReadConfig,
    ) -> Result<Self::Outer, OptKeyStError<Self>>
    where
        Self: FromStrWith,
    {
        Self::transfer_opt_with(std, nonstd, SpecificKey::default(), data, conf)
    }

    fn drop_metaroot_opt(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        conf: &StdTextReadConfig,
    ) -> DeferredSwitchableError<Self::Outer, AllowOptionalDropping, OptKeyError<Self>>
    where
        Self: FromStr,
    {
        Self::drop_opt(std, nonstd, SpecificKey::default(), conf)
    }

    fn drop_metaroot_opt_with(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        data: Self::Payload<'_>,
        conf: &StdTextReadConfig,
    ) -> DeferredSwitchableError<Self::Outer, AllowOptionalDropping, OptKeyStError<Self>>
    where
        Self: FromStrWith,
    {
        Self::drop_opt_with(std, nonstd, SpecificKey::default(), data, conf)
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
    ) -> Result<Self::Outer, OptIndexedKeyError<Self>>
    where
        Self: FromStr,
    {
        Self::get_opt(kws, SpecificKey::new_i1(i.into()))
    }

    fn remove_meas_opt_nofail(kws: &mut StdKeywords, i: impl Into<IndexFromOne>) -> Self::Outer
    where
        Self: FromStr<Err = Infallible>,
    {
        Self::remove_opt_nofail(kws, SpecificKey::new_i1(i.into()))
    }

    fn transfer_meas_opt(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        i: impl Into<IndexFromOne>,
        conf: &StdTextReadConfig,
    ) -> Result<Self::Outer, OptIndexedKeyError<Self>>
    where
        Self: FromStr,
    {
        Self::transfer_opt(std, nonstd, SpecificKey::new_i1(i.into()), conf)
    }

    fn drop_meas_opt(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        i: impl Into<IndexFromOne>,
        conf: &StdTextReadConfig,
    ) -> DeferredSwitchableError<Self::Outer, AllowOptionalDropping, OptIndexedKeyError<Self>>
    where
        Self: FromStr,
    {
        Self::drop_opt(std, nonstd, SpecificKey::new_i1(i.into()), conf)
    }

    fn drop_meas_opt_with(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        i: impl Into<IndexFromOne> + Copy,
        data: Self::Payload<'_>,
        conf: &StdTextReadConfig,
    ) -> DeferredSwitchableError<Self::Outer, AllowOptionalDropping, OptIndexedKeyStError<Self>>
    where
        Self::Outer: PartialEq,
        Self: FromStrWith,
    {
        Self::drop_opt_with(std, nonstd, SpecificKey::new_i1(i.into()), data, conf)
    }

    fn meas_pair_std(&self, i: impl Into<IndexFromOne>) -> (StdKey, String)
    where
        Self: fmt::Display,
    {
        (Self::std(i), self.to_string())
    }
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

pub type LookupMetarootResult<V> =
    WarningsAndErrorsResult<V, (), LookupMetarootWarning, LookupMetarootError>;

#[derive(From, Display, Debug, Error)]
pub enum LookupMetarootError {
    Mode(ReqKeyError<Mode>),
    Cyt3_2(ReqKeyError<Cyt3_2>),
    Par(ReqKeyError<Par>),
    Warn(LookupMetarootWarning),
}

#[derive(From, Display, Debug, Error)]
pub enum LookupMetarootWarning {
    Trigger(OptKeyStError<Trigger>),
    Comp2_0(LookupComp2_0Error),
    Comp3_0(OptKeyError<Compensation3_0>),
    Timestamps2_0(LookupTimestampsError<FCSTime, FCSTimeError>),
    Timestamps3_0(LookupTimestampsError<FCSTime60, FCSTime60Error>),
    Timestamps3_1(LookupTimestampsError<FCSTime100, FCSTime100Error>),
    Datetimes(LookupDatetimesError),
    Modified(LookupModifiedDataError),
    UnstainedCenter(OptKeyStError<UnstainedCenters>),
    Mode3_2(OptKeyError<Mode3_2>),
    // NOTE this can never be an error even if we forbid deprecated keys
    // because there is no easy way to fix it (ie by dropping a key)
    Mode(DeprecatedModeWarning),
    Unicode(OptKeyStError<Unicode>),
    Spillover(OptKeyStError<Spillover>),
    Gate2_0(LookupAppliedGates2_0Error),
    Gate3_0(LookupAppliedGates3_0Error),
    Gate3_2(LookupAppliedGates3_2Error),
    Vol(OptKeyError<Vol>),
    Abrt(OptKeyError<Abrt>),
    Lost(OptKeyError<Lost>),
    Subset(LookupSubsetError),
}

#[derive(From, Display, Debug, Error)]
pub enum LookupSubsetError {
    Flags(LookupCSVFlagsError),
    Bits(OptKeyError<CSVBits>),
    Tot(OptKeyError<CSTot>),
}

pub type LookupMeasurementResult<V> =
    WarningsAndErrorsResult<V, (), LookupMeasurementWarning, LookupMeasurementError>;

#[derive(From, Display, Debug, Error)]
pub enum LookupMeasurementError {
    Temporal(LookupTemporalError),
    Optical(LookupOpticalError),
    Shortname(LookupShortnameError),
    Warn(LookupMeasurementWarning),
}

#[derive(From, Display, Debug, Error)]
pub enum LookupMeasurementWarning {
    Temporal(LookupTemporalWarning),
    Optical(LookupOpticalWarning),
    Shortname(OptIndexedKeyError<Shortname>),
    Pattern(NonStdMeasRegexError),
    MissingTime(MissingTime),
}

pub type LookupShortnameResult<V> =
    WarningAndErrorResult<V, (), OptIndexedKeyError<Shortname>, LookupShortnameError>;

#[derive(From, Display, Debug, Error)]
pub enum LookupShortnameError {
    Req(ReqIndexedKeyError<Shortname>),
    Opt(OptIndexedKeyError<Shortname>),
}

pub type LookupOpticalResult<V> =
    WarningsAndErrorsResult<V, (), LookupOpticalWarning, LookupOpticalError>;

#[derive(From, Display, Debug, Error)]
pub enum LookupOpticalError {
    Xform(ScaleTransformError),
    Scale(ReqIndexedKeyError<Scale>),
    Warn(LookupOpticalWarning),
}

#[derive(From, Display, Debug, Error)]
pub enum LookupOpticalWarning {
    Scale(OptIndexedKeyStError<Scale>),
    TemporalScale(OptIndexedKeyError<TemporalScale2_0>),
    Gain(OptIndexedKeyError<Gain>),
    TemporalGain(LookupTemporalGain),
    Feature(OptIndexedKeyError<Feature>),
    Wavelengths(OptIndexedKeyStError<Wavelengths>),
    Wavelength(OptIndexedKeyError<Wavelength>),
    Calibration3_1(OptIndexedKeyError<Calibration3_1>),
    Calibration3_2(OptIndexedKeyError<Calibration3_2>),
    TemporalType(OptIndexedKeyError<TemporalType>),
    OpticalType(OptIndexedKeyError<OpticalType>),
    Display(OptIndexedKeyError<Display>),
    Power(OptIndexedKeyError<Power>),
    PercentEmitted(OptIndexedKeyError<PercentEmitted>),
    DetectorVoltage(OptIndexedKeyError<DetectorVoltage>),
    Peak(LookupPeakError),
}

pub type LookupTemporalResult<V> =
    WarningsAndErrorsResult<V, (), LookupTemporalWarning, LookupTemporalError>;

#[derive(From, Display, Debug, Error)]
pub enum LookupTemporalError {
    TemporalScale(ReqIndexedKeyError<TemporalScale3_0>),
    Timestep(ReqKeyError<Timestep>),
    Warn(LookupTemporalWarning),
}

#[derive(From, Display, Debug, Error)]
pub enum LookupTemporalWarning {
    TemporalScale(OptIndexedKeyError<TemporalScale2_0>),
    TemporalGain(LookupTemporalGain),
    TemporalType(OptIndexedKeyError<TemporalType>),
    Display(OptIndexedKeyError<Display>),
    Peak(LookupPeakError),
}

#[derive(From)]
pub enum DeprecatedRef<'a> {
    Plate(DeprecatedPlateRef<'a>),
    Peak(DeprecatedPeakRef<'a>),
    Timestamps(DeprecatedTimestampsRef<'a>),
    PercentEmitted(IndexedDepRef<&'a mut Option<PercentEmitted>>),
    Mode(&'a mut Option<Mode3_2>),
    Gate(DepGatedMeasRef<'a>),
    Scheme(DeprecatedGatingSchemeRef<'a>),
}

#[derive(new)]
pub struct IndexedDepRef<T> {
    index: IndexFromOne,
    value: T,
}

#[derive(From)]
pub struct DeprecatedStrRef<'a, T>(pub(crate) &'a mut T);

#[derive(From)]
pub enum DeprecatedTimestampsRef<'a> {
    Btim(&'a mut Option<Btim<FCSTime100>>),
    Etim(&'a mut Option<Etim<FCSTime100>>),
    Date(&'a mut Option<FCSDate>),
}

#[derive(From)]
pub enum DeprecatedPeakRef<'a> {
    Index(IndexedDepRef<&'a mut Option<PeakIndex>>),
    Bin(IndexedDepRef<&'a mut Option<PeakBin>>),
}

#[derive(From)]
pub enum DeprecatedPlateRef<'a> {
    Plateid(DeprecatedStrRef<'a, Plateid>),
    Platename(DeprecatedStrRef<'a, Platename>),
    Wellid(DeprecatedStrRef<'a, Wellid>),
}

#[derive(From)]
pub enum DepGatedMeasRef<'a> {
    Scale(IndexedDepRef<&'a mut Option<GateScale>>),
    Filter(IndexedDepRef<DeprecatedStrRef<'a, GateFilter>>),
    Sname(IndexedDepRef<&'a mut Option<GateShortname>>),
    PEmit(IndexedDepRef<&'a mut Option<GatePercentEmitted>>),
    Range(IndexedDepRef<&'a mut Option<GateRange>>),
    Lname(IndexedDepRef<DeprecatedStrRef<'a, GateLongname>>),
    DetType(IndexedDepRef<DeprecatedStrRef<'a, GateDetectorType>>),
    DetVolt(IndexedDepRef<&'a mut Option<GateDetectorVoltage>>),
}

#[derive(From)]
pub enum DeprecatedGatingSchemeRef<'a> {
    Gating(&'a mut Option<Gating>),
    Region(&'a mut HashMap<RegionIndex, Region<PrefixedMeasIndex>>),
}

pub(crate) trait IsDeprecated {
    fn demote(&mut self, nonstd: &mut NonStdKeywords, keep: bool);

    fn errors(&self, es: &mut Vec<AnyDepKeyError>);
}

impl IsDeprecated for DeprecatedRef<'_> {
    fn demote(&mut self, nonstd: &mut NonStdKeywords, keep: bool) {
        match_many_to_one!(
            self,
            Self,
            [Plate, Peak, Timestamps, PercentEmitted, Mode, Gate, Scheme],
            x,
            {
                x.demote(nonstd, keep);
            }
        );
    }

    fn errors(&self, es: &mut Vec<AnyDepKeyError>) {
        match_many_to_one!(
            self,
            Self,
            [Plate, Peak, Timestamps, PercentEmitted, Mode, Gate, Scheme],
            x,
            {
                x.errors(es);
            }
        );
    }
}

impl IsDeprecated for DeprecatedTimestampsRef<'_> {
    fn demote(&mut self, nonstd: &mut NonStdKeywords, keep: bool) {
        match_many_to_one!(self, Self, [Btim, Etim, Date], x, {
            x.demote(nonstd, keep);
        });
    }

    fn errors(&self, es: &mut Vec<AnyDepKeyError>) {
        match_many_to_one!(self, Self, [Btim, Etim, Date], x, {
            x.errors(es);
        });
    }
}

impl IsDeprecated for DeprecatedPeakRef<'_> {
    fn demote(&mut self, nonstd: &mut NonStdKeywords, keep: bool) {
        match_many_to_one!(self, Self, [Index, Bin], x, {
            x.demote(nonstd, keep);
        });
    }

    fn errors(&self, es: &mut Vec<AnyDepKeyError>) {
        match_many_to_one!(self, Self, [Index, Bin], x, {
            x.errors(es);
        });
    }
}

impl IsDeprecated for DeprecatedPlateRef<'_> {
    fn demote(&mut self, nonstd: &mut NonStdKeywords, keep: bool) {
        match_many_to_one!(self, Self, [Plateid, Platename, Wellid], x, {
            x.demote(nonstd, keep);
        });
    }

    fn errors(&self, es: &mut Vec<AnyDepKeyError>) {
        match_many_to_one!(self, Self, [Plateid, Platename, Wellid], x, {
            x.errors(es);
        });
    }
}

impl IsDeprecated for DepGatedMeasRef<'_> {
    fn demote(&mut self, nonstd: &mut NonStdKeywords, keep: bool) {
        match_many_to_one!(
            self,
            Self,
            [Scale, Filter, Sname, PEmit, Range, Lname, DetType, DetVolt],
            x,
            {
                x.demote(nonstd, keep);
            }
        );
    }

    fn errors(&self, es: &mut Vec<AnyDepKeyError>) {
        match_many_to_one!(
            self,
            Self,
            [Scale, Filter, Sname, PEmit, Range, Lname, DetType, DetVolt],
            x,
            {
                x.errors(es);
            }
        );
    }
}

impl IsDeprecated for DeprecatedGatingSchemeRef<'_> {
    fn demote(&mut self, nonstd: &mut NonStdKeywords, keep: bool) {
        match self {
            Self::Gating(x) => x.demote(nonstd, keep),
            Self::Region(x) => {
                for (ri, r) in take(*x) {
                    for (k, v) in r.opt_keywords_std(ri) {
                        if keep {
                            nonstd.insert_demoted(k, v);
                        }
                    }
                }
            }
        }
    }

    fn errors(&self, es: &mut Vec<AnyDepKeyError>) {
        match self {
            Self::Gating(x) => x.errors(es),
            Self::Region(x) => {
                for (&r, _) in x.iter() {
                    let i = r.into();
                    es.push(AnyDepKeyError::RegionIndex(DepKeyWarning(Key1::new_i1(i))));
                    es.push(AnyDepKeyError::RegionWindow(DepKeyWarning(Key1::new_i1(i))));
                }
            }
        }
    }
}

impl<T> IsDeprecated for &mut Option<T>
where
    AnyDepKeyError: From<DepKey0<T>>,
    T: Key + fmt::Display,
{
    fn demote(&mut self, nonstd: &mut NonStdKeywords, keep: bool) {
        if let Some(y) = take(*self)
            && keep
        {
            nonstd.insert_demoted_metaroot_(&y);
        }
    }

    fn errors(&self, es: &mut Vec<AnyDepKeyError>) {
        if self.is_some() {
            es.push(AnyDepKeyError::from(DepKeyWarning(Key0::<T>::default())));
        }
    }
}

impl<T> IsDeprecated for DeprecatedStrRef<'_, T>
where
    AnyDepKeyError: From<DepKey0<T>>,
    T: Key + DisplayMaybe + Default,
{
    fn demote(&mut self, nonstd: &mut NonStdKeywords, keep: bool) {
        if let Some(y) = take(self.0).display_maybe()
            && keep
        {
            nonstd.insert_demoted_(Key0::<T>::default(), y);
        }
    }

    fn errors(&self, es: &mut Vec<AnyDepKeyError>) {
        if !self.0.is_default() {
            es.push(AnyDepKeyError::from(DepKeyWarning(Key0::<T>::default())));
        }
    }
}

impl<T> IsDeprecated for IndexedDepRef<&mut Option<T>>
where
    AnyDepKeyError: From<DepKey1<T>>,
    T: IndexedKey + fmt::Display,
{
    fn demote(&mut self, nonstd: &mut NonStdKeywords, keep: bool) {
        if let Some(y) = take(self.value)
            && keep
        {
            nonstd.insert_demoted_indexed_(self.index, &y);
        }
    }

    fn errors(&self, es: &mut Vec<AnyDepKeyError>) {
        if self.value.is_some() {
            es.push(AnyDepKeyError::from(DepKeyWarning(Key1::<T>::new_i1(
                self.index,
            ))));
        }
    }
}

impl<T> IsDeprecated for IndexedDepRef<DeprecatedStrRef<'_, T>>
where
    AnyDepKeyError: From<DepKey1<T>>,
    T: IndexedKey + DisplayMaybe + Default,
{
    fn demote(&mut self, nonstd: &mut NonStdKeywords, keep: bool) {
        if let Some(y) = take(self.value.0).display_maybe()
            && keep
        {
            nonstd.insert_demoted_(Key1::<T>::new_i1(self.index), y);
        }
    }

    fn errors(&self, es: &mut Vec<AnyDepKeyError>) {
        if !self.value.0.is_default() {
            es.push(AnyDepKeyError::from(DepKeyWarning(Key1::<T>::new_i1(
                self.index,
            ))));
        }
    }
}

#[derive(From, Display, Debug, Error)]
pub enum LookupPeakError {
    Bin(OptIndexedKeyError<PeakBin>),
    Index(OptIndexedKeyError<PeakIndex>),
}

#[derive(From, Display, Debug, Error)]
pub enum LookupCSVFlagsError {
    Mode(OptKeyError<CSMode>),
    Flag(OptIndexedKeyError<CSVFlag>),
}

#[derive(From, Display, Debug, Error)]
pub enum LookupModifiedDataError {
    LastModTime(OptKeyError<LastModified>),
    Originality(OptKeyError<Originality>),
}

/// Error triggered when time measurement is missing but required.
#[derive(Debug, Error)]
#[error("Could not find time measurement matching {0}")]
pub struct MissingTime(pub TimeMeasNamePattern);

// /// Error/warning triggered when encountering a key value which is deprecated
// #[derive(Debug, Error)]
// pub enum DepValueWarning {
//     #[error("$DATATYPE=A is deprecated")]
//     DatatypeASCII,
//     #[error("$MODE=C is deprecated")]
//     ModeCorrelated,
//     #[error("$MODE=U is deprecated")]
//     ModeUncorrelated,
// }

#[derive(Debug, Error)]
pub enum DeprecatedModeWarning {
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

#[derive(From, Display, Debug, Error)]
pub enum ReqKeyError_<E, T, I> {
    Parse(ParseKeyError<E, T, I>),
    Missing(MissingKeyError<T, I>),
}

pub type OptKeyError_<E, T, I> = ParseKeyError<E, T, I>;

// #[derive(From, Display, Debug, Error)]
// pub enum OptKeyError_<E, T, I> {
//     Parse(ParseKeyError<E, T, I>),
//     Deprecated(DepKeyWarning<T, I>),
// }

#[derive(Debug, Error)]
#[error("missing required key: {0}")]
pub struct MissingKeyError<T, I>(pub SpecificKey<T, I>);

/// Error/warning triggered when encountering a key which is deprecated
#[derive(Debug, Error)]
#[error("deprecated key: {0}")]
pub struct DepKeyWarning<T, I>(pub SpecificKey<T, I>);

pub type DepKey0<T> = DepKeyWarning<T, ()>;
pub type DepKey1<T> = DepKeyWarning<T, IndexFromOne>;

#[derive(From, Display, Debug, Error)]
pub enum AnyDepKeyError {
    Gating(DepKey0<Gating>),
    RegionIndex(DepKey1<RegionGateIndex<PrefixedMeasIndex>>),
    RegionWindow(DepKey1<RegionWindow>),
    GateScale(DepKey1<GateScale>),
    GateFilter(DepKey1<GateFilter>),
    GateShortname(DepKey1<GateShortname>),
    GatePercentEmitted(DepKey1<GatePercentEmitted>),
    GateRange(DepKey1<GateRange>),
    GateLongname(DepKey1<GateLongname>),
    GateDetectorType(DepKey1<GateDetectorType>),
    GateDetectorVoltage(DepKey1<GateDetectorVoltage>),
    Plateid(DepKey0<Plateid>),
    Platename(DepKey0<Platename>),
    Wellid(DepKey0<Wellid>),
    PeakIndex(DepKey1<PeakIndex>),
    PeakBin(DepKey1<PeakBin>),
    Btim(DepKey0<Btim<FCSTime100>>),
    Etim(DepKey0<Etim<FCSTime100>>),
    Date(DepKey0<FCSDate>),
    Mode(DepKey0<Mode3_2>),
    PcntEmit(DepKey1<PercentEmitted>),
}

pub type ReqKeyError<T> = ReqKeyError_<<T as FromStr>::Err, T, ()>;
pub type ReqIndexedKeyError<T> = ReqKeyError_<<T as FromStr>::Err, T, IndexFromOne>;

// pub type ReqKeyStError<T> = ReqKeyError_<<T as FromStrWith>::Err, T, ()>;
pub type ReqIndexedStKeyError<T> = ReqKeyError_<<T as FromStrWith>::Err, T, IndexFromOne>;

pub type OptKeyError<T> = ParseKeyError<<T as FromStr>::Err, T, ()>;
pub type OptIndexedKeyError<T> = ParseKeyError<<T as FromStr>::Err, T, IndexFromOne>;

pub type OptKeyStError<T> = ParseKeyError<<T as FromStrWith>::Err, T, ()>;
pub type OptIndexedKeyStError<T> = ParseKeyError<<T as FromStrWith>::Err, T, IndexFromOne>;

// pub type DepOptKeyError<T> = OptKeyError_<<T as FromStr>::Err, T, ()>;
// pub type DepOptIndexedKeyError<T> = OptKeyError_<<T as FromStr>::Err, T, IndexFromOne>;

// pub type DepOptKeyStError<T> = OptKeyError_<<T as FromStrWith>::Err, T, ()>;
// pub type DepOptIndexedKeyStError<T> = OptKeyError_<<T as FromStrWith>::Err, T, IndexFromOne>;

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
    use crate::data::RawParsedError;
    use crate::python::exceptions::FCSDeprecatedError;
    use crate::python::macros::{impl_from_pyerr, impl_pyreflow_err};

    use super::{
        AnyDepKeyError, DepKeyWarning, DeprecatedModeWarning, LookupCSVFlagsError,
        LookupMeasurementError, LookupMeasurementWarning, LookupMetarootError,
        LookupMetarootWarning, LookupModifiedDataError, LookupOpticalError, LookupOpticalWarning,
        LookupPeakError, LookupShortnameError, LookupSubsetError, LookupTemporalError,
        LookupTemporalWarning, MissingTime, ParseKeyError, PseudostandardError, ReqKeyError_,
        UnusedStandardError,
    };

    use pyo3::prelude::*;
    use std::fmt::Display;

    impl<T, I> From<DepKeyWarning<T, I>> for PyErr
    where
        DepKeyWarning<T, I>: Display,
    {
        fn from(value: DepKeyWarning<T, I>) -> Self {
            FCSDeprecatedError::new_err(value.to_string())
        }
    }

    impl<E, T, I> From<ReqKeyError_<E, T, I>> for PyErr
    where
        ReqKeyError_<E, T, I>: Display,
    {
        fn from(value: ReqKeyError_<E, T, I>) -> Self {
            FCSDeprecatedError::new_err(value.to_string())
        }
    }

    impl<E, T, I> From<ParseKeyError<E, T, I>> for PyErr
    where
        ParseKeyError<E, T, I>: Display,
    {
        fn from(value: ParseKeyError<E, T, I>) -> Self {
            FCSDeprecatedError::new_err(value.to_string())
        }
    }

    impl_pyreflow_err!(InvalidKeywordValueError, PseudostandardError);
    impl_pyreflow_err!(InvalidKeywordValueError, UnusedStandardError);

    // These are file layout errors despite being keywords since they contain
    // data pertaining to the byte layout of the file
    //
    //  TODO maybe...
    impl_pyreflow_err!(FileLayoutError, RawParsedError);

    impl_pyreflow_err!(RelationalException, MissingTime);

    impl_pyreflow_err!(FCSDeprecatedError, AnyDepKeyError);
    impl_pyreflow_err!(FCSDeprecatedError, DeprecatedModeWarning);

    impl_from_pyerr!(
        LookupMetarootWarning,
        Trigger,
        Comp2_0,
        Comp3_0,
        Timestamps2_0,
        Timestamps3_0,
        Timestamps3_1,
        Datetimes,
        Modified,
        UnstainedCenter,
        Mode3_2,
        Mode,
        Unicode,
        Spillover,
        Gate2_0,
        Gate3_0,
        Gate3_2,
        Vol,
        Abrt,
        Lost,
        Subset
    );

    impl_from_pyerr!(LookupMetarootError, Mode, Cyt3_2, Par, Warn);
    impl_from_pyerr!(LookupSubsetError, Flags, Bits, Tot);
    impl_from_pyerr!(LookupCSVFlagsError, Mode, Flag);
    impl_from_pyerr!(LookupModifiedDataError, LastModTime, Originality);

    impl_from_pyerr!(
        LookupMeasurementWarning,
        Temporal,
        Optical,
        Shortname,
        MissingTime,
        Pattern
    );
    impl_from_pyerr!(LookupMeasurementError, Temporal, Optical, Shortname, Warn);
    impl_from_pyerr!(LookupShortnameError, Req, Opt);
    impl_from_pyerr!(LookupTemporalError, TemporalScale, Timestep, Warn);
    impl_from_pyerr!(LookupOpticalError, Xform, Scale, Warn);
    impl_from_pyerr!(
        LookupTemporalWarning,
        TemporalScale,
        TemporalGain,
        TemporalType,
        Display,
        Peak
    );
    impl_from_pyerr!(
        LookupOpticalWarning,
        Scale,
        TemporalScale,
        Gain,
        TemporalGain,
        Feature,
        Wavelengths,
        Wavelength,
        Calibration3_1,
        Calibration3_2,
        TemporalType,
        OpticalType,
        Display,
        Power,
        PercentEmitted,
        DetectorVoltage,
        Peak
    );
    impl_from_pyerr!(LookupPeakError, Bin, Index);
}

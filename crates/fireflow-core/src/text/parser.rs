use crate::config::{AllowOptionalDropping, ConfigFlag as _, StdTextReadConfig};
use crate::logging::{DeferredSwitchableError, ResultExt as _};
use crate::macros::match_many_to_one;
use crate::validated::keys::{
    AnyKey, IndexedKey, Key, Key0, Key1, MeasHeader, NonStdKeywords, NonStdKeywordsExt as _,
    SpecificKey, StdKey, StdKeywords,
};

use super::gating::Region;
use super::index::{IndexFromOne, RegionIndex};
use super::keywords::{
    GateDetectorType, GateDetectorVoltage, GateFilter, GateLongname, GatePercentEmitted, GateRange,
    GateScale, GateShortname, Gating, Mode3_2, PeakBin, PeakIndex, PercentEmitted, Plateid,
    Platename, PrefixedMeasIndex, RegionGateIndex, RegionWindow, Wellid,
};
use super::optional::DisplayMaybe;
use super::timestamps::{Btim, Etim, FCSDate, FCSTime100};

use derive_more::{Display, From};
use derive_new::new;
use thiserror::Error;

use std::collections::HashMap;
use std::convert::Infallible;
use std::fmt;
use std::mem::take;
use std::str::FromStr;

/// An error caused when parsing a required non-indexed standard key
pub type ReqKeyError<T> = ReqKeyErrorInner<<T as FromStr>::Err, T, ()>;

/// An error caused when parsing a required indexed standard key
pub type ReqIndexedKeyError<T> = ReqKeyErrorInner<<T as FromStr>::Err, T, IndexFromOne>;

/// An error caused when parsing a required indexed standard key with external state
pub type ReqIndexedStKeyError<T> = ReqKeyErrorInner<<T as FromStrWith>::Err, T, IndexFromOne>;

/// An parse key error for an optional non-indexed key.
pub type OptKeyError<T> = ParseKeyError<<T as FromStr>::Err, T, ()>;

/// An parse key error for an optional indexed key.
pub type OptIndexedKeyError<T> = ParseKeyError<<T as FromStr>::Err, T, IndexFromOne>;

/// An parse key error for an optional non-indexed key when parsing with external state.
pub type OptKeyStError<T> = ParseKeyError<<T as FromStrWith>::Err, T, ()>;

/// An parse key error for an optional indexed key when parsing with external state.
pub type OptIndexedKeyStError<T> = ParseKeyError<<T as FromStrWith>::Err, T, IndexFromOne>;

/// An error caused by parsing a string incorrectly for a standard key value.
#[derive(new, Debug, Error)]
pub struct ParseKeyError<E, T, I> {
    pub error: E,
    pub key: SpecificKey<T, I>,
    pub value: String,
}

/// An error caused when parsing a required standard key
#[derive(From, Display, Debug, Error)]
pub enum ReqKeyErrorInner<E, T, I> {
    /// Error due to parsing
    Parse(ParseKeyError<E, T, I>),

    /// Error due to absence
    Missing(MissingKeyError<T, I>),
}

/// An error caused by a required standard key being missing
#[derive(Debug, Error)]
#[error("missing required key: {0}")]
pub struct MissingKeyError<T, I>(pub SpecificKey<T, I>);

type ReqResult<T, I> = Result<T, ReqKeyErrorInner<<T as FromStr>::Err, T, I>>;

// TODO move this dep stuff to own module
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

/// Parse a string that includes delimiters
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

/// Parse a string based on external data and config
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
    ) -> Result<Self, ReqKeyErrorInner<Self::Err, Self, I>>
    where
        SpecificKey<Self, I>: AnyKey + Copy,
        Self: FromStr,
    {
        let v = Self::get_req_inner(kws, k).map_err(ReqKeyErrorInner::from)?;
        v.parse()
            .map_err(|e| ParseKeyError::new(e, k, v.to_owned()))
            .map_err(ReqKeyErrorInner::from)
    }

    fn remove_req<I>(
        kws: &mut StdKeywords,
        k: SpecificKey<Self, I>,
    ) -> Result<Self, ReqKeyErrorInner<Self::Err, Self, I>>
    where
        SpecificKey<Self, I>: AnyKey + Copy,
        Self: FromStr,
    {
        let v = Self::remove_req_inner(kws, k).map_err(ReqKeyErrorInner::from)?;
        v.parse()
            .map_err(|e| ParseKeyError::new(e, k, v))
            .map_err(ReqKeyErrorInner::from)
    }

    fn remove_req_with<I>(
        kws: &mut StdKeywords,
        k: SpecificKey<Self, I>,
        data: Self::Payload<'_>,
        conf: &StdTextReadConfig,
    ) -> Result<Self, ReqKeyErrorInner<Self::Err, Self, I>>
    where
        SpecificKey<Self, I>: AnyKey + Copy,
        Self: FromStrWith,
    {
        let v = Self::remove_req_inner(kws, k).map_err(ReqKeyErrorInner::from)?;
        Self::from_str_with(&v, data, conf)
            .map_err(|e| ParseKeyError::new(e, k, v))
            .map_err(ReqKeyErrorInner::from)
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

/// Process mutable references to optional keys which are deprecated
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
    use crate::python::macros::impl_pyreflow_err;

    use super::{AnyDepKeyError, DepKeyWarning, ParseKeyError, ReqKeyErrorInner};

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

    impl<E, T, I> From<ReqKeyErrorInner<E, T, I>> for PyErr
    where
        ReqKeyErrorInner<E, T, I>: Display,
    {
        fn from(value: ReqKeyErrorInner<E, T, I>) -> Self {
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

    // These are file layout errors despite being keywords since they contain
    // data pertaining to the byte layout of the file
    //
    //  TODO maybe...
    impl_pyreflow_err!(FileLayoutError, RawParsedError);

    impl_pyreflow_err!(FCSDeprecatedError, AnyDepKeyError);
}

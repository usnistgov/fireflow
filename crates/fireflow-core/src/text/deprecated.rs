use crate::macros::match_many_to_one;
use crate::validated::keys::{
    IndexedKey, Key, Key0, Key1, NonStdKeywords, NonStdKeywordsExt as _, SpecificKey,
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
use std::fmt;
use std::mem::take;

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr},
    std::fmt::Display,
};

/// Error/warning triggered when encountering a key which is deprecated
#[derive(Debug, Error)]
#[error("deprecated key: {0}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::FCSDeprecatedError))]
#[cfg_attr(feature = "python", bound(DepKeyWarning<T, I>: Display))]
pub struct DepKeyWarning<T, I>(pub SpecificKey<T, I>);

pub type DepKey0<T> = DepKeyWarning<T, ()>;
pub type DepKey1<T> = DepKeyWarning<T, IndexFromOne>;

/// Error when a deprecated key is encountered for any FCS version
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
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

/// A mutable reference to a deprecated key.
///
/// Using mutable references allows one to gather all deprecated keys into one
/// batch for easy error reporting as well as dropping/transferring keywords if
/// desired. Note that all deprecated keys are optional by definition.
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

/// A mutable reference to a deprecated key with an index.
#[derive(new)]
pub struct IndexedDepRef<T> {
    index: IndexFromOne,
    value: T,
}

/// A mutable reference to a deprecated key whose value is a string.
#[derive(From)]
pub struct DeprecatedStrRef<'a, T>(pub(crate) &'a mut T);

/// A mutable reference to $BTIM/$ETIM/$DATE keys which are deprecated.
#[derive(From)]
pub enum DeprecatedTimestampsRef<'a> {
    Btim(&'a mut Option<Btim<FCSTime100>>),
    Etim(&'a mut Option<Etim<FCSTime100>>),
    Date(&'a mut Option<FCSDate>),
}

/// A mutable reference to $PKn and $PKNn keys which are deprecated.
#[derive(From)]
pub enum DeprecatedPeakRef<'a> {
    Index(IndexedDepRef<&'a mut Option<PeakIndex>>),
    Bin(IndexedDepRef<&'a mut Option<PeakBin>>),
}

/// A mutable reference to $PLATEID/$PLATENAME/$WELLID keys which are deprecated.
#[derive(From)]
pub enum DeprecatedPlateRef<'a> {
    Plateid(DeprecatedStrRef<'a, Plateid>),
    Platename(DeprecatedStrRef<'a, Platename>),
    Wellid(DeprecatedStrRef<'a, Wellid>),
}

/// A mutable reference to $Gn* keys which are deprecated.
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

/// A mutable reference to $GATING/$RnI/$RnW keys which are deprecated.
#[derive(From)]
pub enum DeprecatedGatingSchemeRef<'a> {
    Gating(&'a mut Option<Gating>),
    Region(&'a mut HashMap<RegionIndex, Region<PrefixedMeasIndex>>),
}

/// Process mutable references to keys which are deprecated
pub trait IsDeprecated {
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

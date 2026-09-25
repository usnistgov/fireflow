//! Enforce relational links between keywords.
//!
//! This amounts to two basic operations:
//!
//! 1) Checking if all links are valid: used when adding a new keyword which
//!    has links and ensuring this is valid.
//! 2) Checking that a keyword has any links (presumed valid) at all: This is
//!    useful when attempting to removing a keyword which may break a link.
//!
//! For (1), the basic idea is that some keywords ($SPILLOVER for example) refer
//! to measurements by $PnN. If any of these $PnN don't exist, the key should be
//! dropped since it is invalid and would produce a bad internal state.
//!
//! Specifically, there are two types of links to be enforced:
//! 1. key -> $PnN
//! 2. key -> index (which could be measurement, gating, etc)
//!
//! How this is actually done:
//! 1. Check each relevant data structure for invalid links
//! 2. If an invalid keyword is found, rip it out and store it in an enum
//! 3. When all invalid keywords are collected, loop through them an emit errors
//!    and/or demote them to nonstandard keywords (all are optional so this is
//!    a valid "fix" to preserve information).
//!
//! The reason these steps need to be broken apart like this is because we need
//! to run this process when creating a new Core* struct and also when we read
//! a file and parse keywords from a hash table. The former doesn't require
//! demoting optional keywords.

use crate::fixed_vec::OneOrTwo;
use crate::logging::ErrorGroup;
use crate::macros::def_summary;
use crate::std_index::index::StdLookupTx;
use crate::text::keywords::{
    Compensation3_0, Dfc, Gating, MeasOrGateIndex, PrefixedMeasIndex, RegionGateIndex,
    RegionWindow, Trigger, UnstainedCenters,
};
use crate::text::spillover::Spillover;
use crate::validated::keys::{DollarKey, DollarKey_, ValueToStdKey};
use crate::validated::shortname::Shortname;

use fireflow_types::config::ProcessOptionalFailure;
use fireflow_types::index::{MeasIndex, RegionIndex};
use fireflow_types::std_key::{
    DfcKey, DollarStdKey, DollarWrap0, IndexedKey, RegionKeyId, ToStd as _,
};
use nonempty::{IntoIteratorExt as _, IntoNonEmptyIterator as _, NEVec, NonEmptyIterator as _};

use derive_more::{AsRef, Display, From};
use derive_new::new;
use derive_where::derive_where;
use itertools::Itertools as _;
use thiserror::Error;

use std::collections::HashSet;
use std::fmt;
use std::marker::PhantomData;
use std::mem::take;

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr},
    fireflow_types::python as py,
    std::fmt::Display,
};

/// $PnN ([`Shortname`]s) from optical measurements that are pending removal
#[derive(AsRef, From)]
pub struct OpticalNamesToRemove<'a>(pub(crate) HashSet<&'a Shortname>);

/// Indices from all measurements which are pending removal
#[derive(AsRef, From)]
pub struct IndicesToRemove(pub(crate) HashSet<MeasIndex>);

//
// Existential relational errors (checking if existing links might be broken)
//

def_summary!(
    pub ExistingLinkFailure,
    "could not continue without breaking existing links"
);

pub type ExistingLinkErrors = ErrorGroup<ExistingLinkError, ExistingLinkFailure>;

/// Error when any keyword has references to it which would be broken if dropped
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ExistingLinkError {
    Named(AnyExistingNamedLinkError),
    Index(AnyExistingIndexLinkError),
}

/// Error when any keyword has named references to it which would be broken if dropped
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum AnyExistingNamedLinkError {
    Trigger(ExistingNamedLinkError<Trigger>),
    UnstainedCenters(ExistingNamedLinkError<UnstainedCenters>),
    Spillover(ExistingNamedLinkError<Spillover>),
}

/// Error when any keyword has indexed references to it which would be broken if dropped
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum AnyExistingIndexLinkError {
    Comp2_0(ExistingIndexedLinkError<Dfc, MeasIndex>),
    Comp3_0(ExistingIndexedLinkError<Compensation3_0, MeasIndex>),
    Region3_0(ExistingIndexedLinkError<RegionGateIndex<MeasOrGateIndex>, MeasIndex>),
    Region3_2(ExistingIndexedLinkError<RegionGateIndex<PrefixedMeasIndex>, MeasIndex>),
}

/// Error when a named reference would be broken if a measurement is dropped
#[derive(Error, new)]
#[derive_where(Clone, Debug, PartialEq; I)]
#[error(
    "{key} refers to existing $PnN which are about to be dropped: {xs}",
    xs = self.names.iter().join(", ")
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
#[cfg_attr(feature = "python", bound(DollarKey_<T, I>: Display))]
pub struct ExistingNamedLinkError_<T, I> {
    pub key: DollarKey_<T, I>,
    pub names: NEVec<Shortname>,
}

pub type ExistingNamedLinkError<T> = ExistingNamedLinkError_<T, <T as ValueToStdKey>::Index>;

/// Error when a keyword has indexed references to it which would be broken if dropped
#[derive(Display, Error, new)]
#[derive_where(Clone, Debug, PartialEq; I, J)]
#[display(
    "{key} refers to existing indices which are about to be dropped: {xs}",
    xs = self.indices.iter().join(", ")
)]
#[display(bound(J: fmt::Display))]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
#[cfg_attr(feature = "python", bound(DollarKey_<T, I>: Display))]
#[cfg_attr(feature = "python", bound(J: Display))]
pub struct ExistingIndexedLinkError_<T, I, J> {
    pub key: DollarKey_<T, I>,
    pub indices: NEVec<J>,
}

pub type ExistingIndexedLinkError<T, J> =
    ExistingIndexedLinkError_<T, <T as ValueToStdKey>::Index, J>;

//
// Broken relational errors (checking if new links are valid)
//

/// A relational keyword that has been removed due having a broken reference.
#[derive(From)]
pub enum RemovedLink {
    GatingRegion(RemovedGateLink),
    Gating(NEVec<RegionIndex>),
    Comp2_0(NEVec<RemovedComp2_0Cell>),
    Comp3_0(RemovedIndexLink<Compensation3_0>),
    Spillover(RemovedNamedLink<Spillover>),
    UnstainedCenters(RemovedNamedLink<UnstainedCenters>),
    Trigger(RemovedNamedLink<Trigger>),
}

/// An invalid $DFCmTOn keyword that was removed
#[derive(new)]
pub struct RemovedComp2_0Cell {
    key: DfcKey,
    missing: Comp2_0Missing,
}

/// Denotes which index from a removed $DFCmTOn keyword is invalid
pub(crate) enum Comp2_0Missing {
    Row,
    Col,
    Both,
}

/// A keyword which links to a non-existent $PnN which was removed.
#[derive(new)]
pub struct RemovedNamedLink<T> {
    names: LinkName,
    _key: PhantomData<T>,
}

pub(crate) enum LinkName {
    Both(NEVec<Shortname>, Option<Shortname>),
    Temporal(Shortname),
}

/// A keyword which links to a non-existent measurement index which was removed.
#[derive(new)]
pub struct RemovedIndexLink<T> {
    indices: NEVec<MeasIndex>,
    _key: PhantomData<T>,
}

/// A $RnI/$RnW pair which refers to a non-existent measurement index which was removed.
#[derive(new)]
pub struct RemovedGateLink {
    pub(crate) region_index: RegionIndex,
    pub(crate) meas_indices: OneOrTwo<MeasIndex>,
}

/// All possible relational errors
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum BrokenOrDependentLinkError {
    Indexed(BrokenIndexedLinkError),
    Named(BrokenNamedLinkError),
    Gating(DependentKeyError<Gating>),
    Window(DependentKeyError<RegionWindow>),
}

#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum BrokenIndexedLinkError {
    Comp2_0(KeyToIndexLinkError<Dfc>),
    Comp3_0(KeyToIndexLinkError<Compensation3_0>),
    Region(BrokenRegionLinkError),
}

#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum BrokenNamedLinkError {
    Spillover(KeyToNameLinkError<Spillover>),
    Trigger(KeyToNameLinkError<Trigger>),
    UnstainedCenters(KeyToNameLinkError<UnstainedCenters>),
}

// NOTE the index for RegionGateIndex<I> is arbitrary, it should not affect how
// the error is printed
pub(crate) type BrokenRegionLinkError = KeyToIndexLinkError<RegionGateIndex<PrefixedMeasIndex>>;

/// Error when key which references a non-existent optical $PnN or the temporal $PnN
#[derive(From, Display, Error)]
#[derive_where(Clone, Debug, PartialEq; I)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(DollarKey_<T, I>: Display))]
pub enum NamedLinkError_<T, I> {
    Optical(OpticalNamedLinkError_<T, I>),
    Temporal(TemporalNamedLinkError_<T, I>),
}

pub type KeyToNameLinkError<T> = NamedLinkError_<T, <T as ValueToStdKey>::Index>;

/// Error when key which references a non-existent measurement $PnN
#[derive(Display, Error, new)]
#[derive_where(Clone, Debug, PartialEq; I)]
#[display(
    "{key} references non-existent $PnN: {bad}",
    bad = self.names.iter().join(", ")
)]
#[cfg_attr(
    feature = "python",
    derive(DisplayAsPyErr),
    pyerr(py::RelationalError),
    bound(DollarKey_<T, I>: Display)
)]
pub struct OpticalNamedLinkError_<T, I> {
    key: DollarKey_<T, I>,
    names: NEVec<Shortname>,
}

pub type OpticalNamedLinkError<T> = OpticalNamedLinkError_<T, <T as ValueToStdKey>::Index>;

#[derive(Display, Error, new)]
#[derive_where(Clone, Debug, PartialEq; I)]
#[display("{key} cannot reference temporal $PnN: {name}")]
#[cfg_attr(
    feature = "python",
    derive(DisplayAsPyErr),
    pyerr(py::RelationalError),
    bound(DollarKey_<T, I>: Display)
)]
pub struct TemporalNamedLinkError_<T, I> {
    key: DollarKey_<T, I>,
    name: Shortname,
}

pub type TemporalNamedLinkError<T> = TemporalNamedLinkError_<T, <T as ValueToStdKey>::Index>;

/// Error when key which references a non-existent measurement index
#[derive(Display, Error, new)]
#[derive_where(Clone, Debug, PartialEq, Eq; I)]
#[display(
    "{key} references non-existent measurement indices: {bad}",
    bad = self.indices.iter().join(", ")
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
#[cfg_attr(feature = "python", bound(DollarKey_<T, I>: Display))]
pub struct KeyToIndexLinkError_<T, I> {
    indices: NEVec<MeasIndex>,
    key: DollarKey_<T, I>,
}

pub type KeyToIndexLinkError<T> = KeyToIndexLinkError_<T, <T as ValueToStdKey>::Index>;

/// Error when key which depends on another key which is invalid.
#[derive(Display, Error, new)]
#[derive_where(Clone, Debug, PartialEq; I)]
#[display(
    "{key} depends on other keys which do not exist: {bad}",
    bad = self.deps.iter().join(", "),
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
#[cfg_attr(feature = "python", bound(DollarKey_<T, I>: Display))]
pub struct DependentKeyError_<T, I> {
    deps: NEVec<DollarStdKey>,
    key: DollarKey_<T, I>,
}

pub type DependentKeyError<T> = DependentKeyError_<T, <T as ValueToStdKey>::Index>;

impl<T> OpticalNamedLinkError_<T, ()> {
    pub(crate) fn new_i0(js: NEVec<Shortname>) -> Self {
        Self::new(DollarKey_::default(), js)
    }
}

impl<T> TemporalNamedLinkError_<T, ()> {
    pub(crate) fn new_i0(name: Shortname) -> Self {
        Self::new(DollarKey_::default(), name)
    }
}

impl<T> KeyToIndexLinkError_<T, ()> {
    pub(crate) fn new_i0(js: NEVec<MeasIndex>) -> Self {
        Self::new(js, DollarKey_::default())
    }
}

impl<T> DependentKeyError_<T, ()> {
    pub(crate) fn new1(deps: NEVec<DollarStdKey>) -> Self {
        Self::new(deps, DollarKey_::default())
    }
}

impl<T, I> DependentKeyError_<T, I> {
    pub(crate) fn new2(i: I, deps: NEVec<DollarStdKey>) -> Self {
        Self::new(deps, DollarKey_::new(i))
    }
}

impl RemovedLink {
    pub(crate) fn insert_keyvals(&self, kws: &mut StdLookupTx, flag: ProcessOptionalFailure) {
        fn go<T>(kws: &mut StdLookupTx, flag: ProcessOptionalFailure)
        where
            T: ValueToStdKey<Index = ()>,
        {
            kws.set_failure_flag(&T::std0(), flag);
        }

        match self {
            Self::GatingRegion(x) => {
                kws.set_failure_flag(&RegionKeyId::I.to_std(&x.region_index), flag);
                kws.set_failure_flag(&RegionKeyId::W.to_std(&x.region_index), flag);
            }
            Self::Gating(_) => go::<Gating>(kws, flag),
            Self::Comp2_0(xs) => {
                for k in xs {
                    kws.set_failure_flag(&k.key.into(), flag);
                }
            }
            Self::Comp3_0(_) => go::<Compensation3_0>(kws, flag),
            Self::Spillover(_) => go::<Spillover>(kws, flag),
            Self::UnstainedCenters(_) => go::<UnstainedCenters>(kws, flag),
            Self::Trigger(_) => go::<Trigger>(kws, flag),
        }
    }

    pub(crate) fn push_errors(self, es: &mut Vec<BrokenOrDependentLinkError>) {
        macro_rules! go_named {
            ($es:expr, $x:expr) => {{
                $es.extend(
                    $x.into_errors()
                        .map(BrokenNamedLinkError::from)
                        .map(Into::into),
                )
            }};
        }
        match self {
            Self::GatingRegion(x) => {
                for e in x.into_errors() {
                    es.push(e);
                }
            }
            Self::Gating(indices) => {
                let ks = indices.into_nonempty_iter().flat_map(|ri| {
                    let k0 = RegionKeyId::I.to_std(&ri).into();
                    let k1 = RegionKeyId::W.to_std(&ri).into();
                    [k0, k1]
                });
                let e = DependentKeyError::<Gating>::new1(ks.collect());
                es.push(e.into());
            }
            Self::Comp2_0(xs) => {
                for x in xs {
                    es.push(BrokenIndexedLinkError::from(x.as_error()).into());
                }
            }
            Self::Comp3_0(x) => es.push(BrokenIndexedLinkError::from(x.into_error()).into()),
            Self::Spillover(x) => go_named!(es, x),
            Self::UnstainedCenters(x) => go_named!(es, x),
            Self::Trigger(x) => go_named!(es, x),
        }
    }
}

impl RemovedComp2_0Cell {
    fn as_error(&self) -> KeyToIndexLinkError<Dfc> {
        let i = self.key.index;
        let xs = match self.missing {
            Comp2_0Missing::Row => NEVec::new(i.i1),
            Comp2_0Missing::Col => NEVec::new(i.i0),
            Comp2_0Missing::Both => {
                let mut xs = NEVec::new(i.i0);
                xs.push(i.i1);
                xs
            }
        };
        KeyToIndexLinkError::new(xs, DollarKey_::new_i2(i.i0, i.i1))
    }
}

impl<T: ValueToStdKey> RemovedNamedLink<T> {
    fn into_errors(self) -> impl Iterator<Item = KeyToNameLinkError<T>>
    where
        T: ValueToStdKey<Index = ()>,
    {
        let ret = match self.names {
            LinkName::Both(os, t) => {
                let oe = Some(OpticalNamedLinkError::new_i0(os).into());
                let te = t.map(TemporalNamedLinkError::new_i0).map(Into::into);
                [oe, te]
            }
            LinkName::Temporal(t) => [None, Some(TemporalNamedLinkError::new_i0(t).into())],
        };
        ret.into_iter().flatten()
    }

    pub(crate) fn remove_invalid_link<F>(src: &mut Option<T>, f: F) -> Option<Self>
    where
        F: FnOnce(&T) -> Option<LinkName>,
    {
        let mut removed = None;
        *src = take(src).and_then(|s| {
            if let Some(ln) = f(&s) {
                removed = Some(Self::new(ln));
                None
            } else {
                Some(s)
            }
        });
        removed
    }
}

impl<T: ValueToStdKey> RemovedIndexLink<T> {
    fn into_error(self) -> KeyToIndexLinkError<T>
    where
        T: ValueToStdKey<Index = ()>,
    {
        KeyToIndexLinkError::new_i0(self.indices)
    }

    pub(crate) fn remove_invalid_link<F, I>(src: &mut Option<T>, f: F) -> Option<Self>
    where
        F: FnOnce(&T) -> I,
        I: IntoIterator<Item = MeasIndex>,
    {
        let mut removed = None;
        *src = take(src).and_then(|s| {
            if let Some(js) = f(&s).try_into_nonempty_iter() {
                removed = Some(Self::new(js.collect()));
                None
            } else {
                Some(s)
            }
        });
        removed
    }
}

impl RemovedGateLink {
    fn into_errors(self) -> impl Iterator<Item = BrokenOrDependentLinkError>
    where
        BrokenIndexedLinkError: From<BrokenRegionLinkError>,
    {
        let ri = self.region_index;
        let region_key = DollarWrap0(IndexedKey::new(ri, RegionKeyId::I).into());
        let k = DollarKey::new(ri);
        let e0 = KeyToIndexLinkError::new(self.meas_indices.into(), k);
        let e1 = DependentKeyError::<RegionWindow>::new2(ri, NEVec::new(region_key));
        [BrokenIndexedLinkError::from(e0).into(), e1.into()].into_iter()
    }
}

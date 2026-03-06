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
use crate::text::index::{IndexFromOne, MeasIndex};
use crate::text::keywords::{
    Compensation3_0, Dfc, Gating, Keyword0FromValue as _, MeasOrGateIndex, OptRootKeyword,
    PrefixedMeasIndex, RefKeyword0, RegionGateIndex, RegionKeyword, RegionWindow, SplitKeyword1,
    Trigger, UnstainedCenters,
};
use crate::validated::keys::{
    BiIndex, DollarKey, IndexedKey as _, Key, NonStdKeywords, NonStdKeywordsExt as _, SpecificKey,
    StdKey,
};
use crate::validated::shortname::Shortname;

use super::gating::Region;
use super::index::RegionIndex;
use super::keywords::{AsStdKeywordPair as _, SplitKeyword2};
use super::spillover::Spillover;

use derive_more::{AsRef, Display, From};
use derive_new::new;
use fireflow_types::nonempty_string::NEString;
use itertools::Itertools as _;
use nonempty_collections::{
    IntoIteratorExt as _, NEVec,
    iter::{IntoNonEmptyIterator as _, NonEmptyIterator as _},
};
use thiserror::Error;

use std::collections::HashSet;
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
    ExistingLinkFailure,
    "could not continue without breaking existing links"
);

pub type ExistingLinkErrors = ErrorGroup<ExistingLinkError, ExistingLinkFailure>;

/// Error when any keyword has references to it which would be broken if dropped
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ExistingLinkError {
    Named(AnyExistingNamedLinkError),
    Index(AnyExistingIndexLinkError),
}

/// Error when any keyword has named references to it which would be broken if dropped
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum AnyExistingNamedLinkError {
    Trigger(ExistingNamedLinkError<Trigger, ()>),
    UnstainedCenters(ExistingNamedLinkError<UnstainedCenters, ()>),
    Spillover(ExistingNamedLinkError<Spillover, ()>),
}

/// Error when any keyword has indexed references to it which would be broken if dropped
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum AnyExistingIndexLinkError {
    Comp2_0(ExistingIndexedLinkError<Dfc, BiIndex>),
    Comp3_0(ExistingIndexedLinkError<Compensation3_0, ()>),
    Region3_0(ExistingIndexedLinkError<RegionGateIndex<MeasOrGateIndex>, IndexFromOne>),
    Region3_2(ExistingIndexedLinkError<RegionGateIndex<PrefixedMeasIndex>, IndexFromOne>),
}

/// Error when a named reference would be broken if a measurement is dropped
#[derive(Debug, Error, new)]
#[error(
    "{key} refers to existing $PnN which are about to be dropped: {xs}",
    xs = self.names.iter().join(", ")
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
#[cfg_attr(feature = "python", bound(SpecificKey<T, I>: Display))]
pub struct ExistingNamedLinkError<T, I> {
    pub key: SpecificKey<T, I>,
    pub names: NEVec<Shortname>,
}

/// Error when a keyword has indexed references to it which would be broken if dropped
#[derive(Debug, Error, new)]
#[error(
    "{key} refers to existing indices which are about to be dropped: {xs}",
    xs = self.names.iter().join(", ")
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
#[cfg_attr(feature = "python", bound(SpecificKey<T, I>: Display))]
pub struct ExistingIndexedLinkError<T, I> {
    pub key: SpecificKey<T, I>,
    pub names: NEVec<IndexFromOne>,
}

//
// Broken relational errors (checking if new links are valid)
//

/// A relational keyword that has been removed due having a broken reference.
#[derive(From)]
pub enum RemovedLink {
    GatingRegion3_0(RemovedGateLink<MeasOrGateIndex>),
    GatingRegion3_2(RemovedGateLink<PrefixedMeasIndex>),
    Gating(RemovedGating),
    Comp2_0(NEVec<RemovedComp2_0Cell>),
    Comp3_0(RemovedIndexLink<Compensation3_0>),
    Spillover(RemovedNamedLink<Spillover>),
    UnstainedCenters(RemovedNamedLink<UnstainedCenters>),
    Trigger(RemovedNamedLink<Trigger>),
}

/// An invalid $DFCmTOn keyword that was removed
#[derive(new)]
pub struct RemovedComp2_0Cell {
    kw: SplitKeyword2<Dfc>,
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
    key: T,
    names: LinkName,
}

pub(crate) enum LinkName {
    Both(NEVec<Shortname>, Option<Shortname>),
    Temporal(Shortname),
}

/// A keyword which links to a non-existent measurement index which was removed.
#[derive(new)]
pub struct RemovedIndexLink<T> {
    key: T,
    indices: NEVec<MeasIndex>,
}

/// A $RnI/$RnW pair which refers to a non-existent measurement index which was removed.
#[derive(new)]
pub struct RemovedGateLink<I> {
    pub(crate) region_index: RegionIndex,
    pub(crate) region: Region<I>,
    pub(crate) meas_indices: OneOrTwo<MeasIndex>,
}

/// A $GATING keyword which references non-existent $RnI/$RnW keywords and was removed.
#[derive(new)]
pub struct RemovedGating {
    pub(crate) region_indices: NEVec<RegionIndex>,
    pub(crate) gating: Gating,
}

/// All possible relational errors
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum BrokenOrDependentLinkError {
    Indexed(BrokenIndexedLinkError),
    Named(BrokenNamedLinkError),
    Gating(DependentKeyError<Gating>),
    Window(DependentIndexedKeyError<RegionWindow>),
}

#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum BrokenIndexedLinkError {
    Comp2_0(BiIndexedKeyToIndexLinkError<Dfc>),
    Comp3_0(KeyToIndexLinkError<Compensation3_0>),
    Region3_0(BrokenRegionLinkError<MeasOrGateIndex>),
    Region3_2(BrokenRegionLinkError<PrefixedMeasIndex>),
}

#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum BrokenNamedLinkError {
    Spillover(KeyToNameLinkError<Spillover>),
    Trigger(KeyToNameLinkError<Trigger>),
    UnstainedCenters(KeyToNameLinkError<UnstainedCenters>),
}

pub(crate) type BrokenRegionLinkError<I> = IndexedKeyToIndexLinkError<RegionGateIndex<I>>;

/// Error when key which references a non-existent optical $PnN or the temporal $PnN
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(SpecificKey<T, I>: Display))]
pub enum NamedLinkError<T, I> {
    Optical(OpticalNamedLinkError<T, I>),
    Temporal(TemporalNamedLinkError<T, I>),
}

/// Error when key which references a non-existent measurement $PnN
#[derive(Debug, Display, Error, new)]
#[display(
    "{key} references non-existent $PnN: {bad}",
    bad = self.names.iter().join(", ")
)]
#[cfg_attr(
    feature = "python",
    derive(DisplayAsPyErr),
    pyerr(py::RelationalError),
    bound(SpecificKey<T, I>: Display)
)]
pub struct OpticalNamedLinkError<T, I> {
    key: SpecificKey<T, I>,
    names: NEVec<Shortname>,
}

#[derive(Debug, Display, Error, new)]
#[display("{key} cannot reference temporal $PnN: {name}")]
#[cfg_attr(
    feature = "python",
    derive(DisplayAsPyErr),
    pyerr(py::RelationalError),
    bound(SpecificKey<T, I>: Display)
)]
pub struct TemporalNamedLinkError<T, I> {
    key: SpecificKey<T, I>,
    name: Shortname,
}

/// Error when key which references a non-existent measurement index
#[derive(Debug, Display, Error, new)]
#[display(
    "{key} references non-existent measurement indices: {bad}",
    bad = self.indices.iter().join(", ")
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
#[cfg_attr(feature = "python", bound(DollarKey<T, I>: Display))]
pub struct IndexLinkError<T, I> {
    indices: NEVec<MeasIndex>,
    key: DollarKey<T, I>,
}

/// Error when key which depends on another key which is invalid.
#[derive(Debug, Display, Error, new)]
#[display(
    "{key} depends on other keys which do not exist: {bad}",
    bad = self.deps.iter().join(", "),
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
#[cfg_attr(feature = "python", bound(SpecificKey<T, I>: Display))]
pub struct DependentKeyErrorInner<T, I> {
    deps: NEVec<StdKey>,
    key: SpecificKey<T, I>,
}

pub type KeyToNameLinkError<T> = NamedLinkError<T, ()>;

pub type KeyToIndexLinkError<T> = IndexLinkError<T, ()>;
pub type IndexedKeyToIndexLinkError<T> = IndexLinkError<T, IndexFromOne>;
pub type BiIndexedKeyToIndexLinkError<T> = IndexLinkError<T, BiIndex>;

pub type DependentKeyError<T> = DependentKeyErrorInner<T, ()>;
pub type DependentIndexedKeyError<T> = DependentKeyErrorInner<T, IndexFromOne>;

impl<T> OpticalNamedLinkError<T, ()> {
    pub(crate) fn new_i0(js: NEVec<Shortname>) -> Self {
        Self::new(SpecificKey::default(), js)
    }
}

impl<T> TemporalNamedLinkError<T, ()> {
    pub(crate) fn new_i0(name: Shortname) -> Self {
        Self::new(SpecificKey::default(), name)
    }
}

impl<T> IndexLinkError<T, ()> {
    pub(crate) fn new_i0(js: NEVec<MeasIndex>) -> Self {
        Self::new(js, DollarKey::default())
    }
}

impl<T> DependentKeyError<T> {
    pub(crate) fn new1(deps: NEVec<StdKey>) -> Self {
        Self::new(deps, SpecificKey::default())
    }
}

impl<T> DependentIndexedKeyError<T> {
    pub(crate) fn new2(i: IndexFromOne, deps: NEVec<StdKey>) -> Self {
        Self::new(deps, SpecificKey::new_i1(i))
    }
}

impl RemovedLink {
    pub(crate) fn insert_keyvals(&self, kws: &mut NonStdKeywords) {
        fn go_ref<'a, T>(x: &'a T, kws: &mut NonStdKeywords)
        where
            OptRootKeyword<'a>: From<RefKeyword0<'a, T>>,
        {
            let kw = OptRootKeyword::from_ref(x);
            kws.insert_demoted_keyword(kw.into());
        }

        fn go_gate<'a, I>(r: &'a RemovedGateLink<I>, kws: &mut NonStdKeywords)
        where
            I: Copy,
            RegionKeyword<'a>: From<SplitKeyword1<RegionGateIndex<I>>>,
        {
            r.region.demote_keywords(r.region_index, kws);
        }

        match self {
            Self::GatingRegion3_0(x) => go_gate(x, kws),
            Self::GatingRegion3_2(x) => go_gate(x, kws),
            Self::Gating(x) => go_ref(&x.gating, kws),
            Self::Comp2_0(xs) => {
                for x in xs {
                    let (k, v) = x.as_keyval();
                    kws.insert_demoted(k, v.to_string());
                }
            }
            Self::Comp3_0(x) => go_ref(&x.key, kws),
            Self::Spillover(x) => go_ref(&x.key, kws),
            Self::UnstainedCenters(x) => {
                if let Some(kw) = OptRootKeyword::from_unstainedcenters(&x.key) {
                    kws.insert_demoted_keyword(kw.into());
                }
            }
            Self::Trigger(x) => go_ref(&x.key, kws),
        }
    }

    pub(crate) fn push_errors(self, es: &mut Vec<BrokenOrDependentLinkError>) {
        macro_rules! go_gate {
            ($es:expr, $x:expr) => {{
                for e in $x.into_errors() {
                    $es.push(e);
                }
            }};
        }
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
            Self::GatingRegion3_0(x) => go_gate!(es, x),
            Self::GatingRegion3_2(x) => go_gate!(es, x),
            Self::Gating(x) => {
                let ks = x.region_indices.into_nonempty_iter().flat_map(|ri| {
                    let k0 = RegionGateIndex::<()>::std(ri);
                    let k1 = RegionWindow::std(ri);
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
    fn as_keyval(&self) -> (StdKey, NEString) {
        self.kw.as_std_key_pair()
    }

    fn as_error(&self) -> BiIndexedKeyToIndexLinkError<Dfc> {
        let i = self.kw.key.index();
        let xs = match self.missing {
            Comp2_0Missing::Row => NEVec::new(i.i1.into()),
            Comp2_0Missing::Col => NEVec::new(i.i0.into()),
            Comp2_0Missing::Both => {
                let mut xs = NEVec::new(i.i0.into());
                xs.push(i.i1.into());
                xs
            }
        };
        BiIndexedKeyToIndexLinkError::new(xs, self.kw.key)
    }
}

impl<T: Key> RemovedNamedLink<T> {
    fn into_errors(self) -> impl Iterator<Item = KeyToNameLinkError<T>> {
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
                removed = Some(Self::new(s, ln));
                None
            } else {
                Some(s)
            }
        });
        removed
    }
}

impl<T: Key> RemovedIndexLink<T> {
    fn into_error(self) -> KeyToIndexLinkError<T> {
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
                removed = Some(Self::new(s, js.collect()));
                None
            } else {
                Some(s)
            }
        });
        removed
    }
}

impl<I> RemovedGateLink<I> {
    fn into_errors(self) -> impl Iterator<Item = BrokenOrDependentLinkError>
    where
        BrokenIndexedLinkError: From<BrokenRegionLinkError<I>>,
    {
        let ri = self.region_index;
        let region_key = RegionGateIndex::<()>::std(ri);
        let k = DollarKey::new_i1(ri);
        let e0 = IndexedKeyToIndexLinkError::new(self.meas_indices.into(), k);
        let e1 = DependentIndexedKeyError::new2(ri.into(), NEVec::new(region_key));
        [BrokenIndexedLinkError::from(e0).into(), e1.into()].into_iter()
    }
}

/// Enforce relational links between keywords.
///
/// This amounts to two basic operations:
///
/// 1) Checking if all links are valid: used when adding a new keyword which
///    has links and ensuring this is valid.
/// 2) Checking that a keyword has any links (presumed valid) at all: This is
///    useful when attempting to removing a keyword which may break a link.
///
/// For (1), the basic idea is that some keywords ($SPILLOVER for example) refer
/// to measurements by $PnN. If any of these $PnN don't exist, the key should be
/// dropped since it is invalid and would produce a bad internal state.
///
/// Specifically, there are two types of links to be enforced:
/// 1. key -> $PnN
/// 2. key -> index (which could be measurement, gating, etc)
///
/// How this is actually done:
/// 1. Check each relevant data structure for invalid links
/// 2. If an invalid keyword is found, rip it out and store it in an enum
/// 3. When all invalid keywords are collected, loop through them an emit errors
///    and/or demote them to nonstandard keywords (all are optional so this is
///    a valid "fix" to preserve information).
///
/// The reason these steps need to be broken apart like this is because we need
/// to run this process when creating a new Core* struct and also when we read
/// a file and parse keywords from a hash table. The former doesn't require
/// demoting optional keywords.
use crate::logging::ErrorGroup;
use crate::macros::def_summary;
use crate::text::index::{IndexFromOne, MeasIndex};
use crate::text::optional::DisplayMaybe as _;
use crate::validated::keys::{
    BiIndex, BiIndexedKey as _, IndexedKey as _, Key, NonStdKeywords, NonStdKeywordsExt as _,
    SpecificKey, StdKey,
};
use crate::validated::shortname::Shortname;

use super::gating::Region;
use super::index::RegionIndex;
use super::keywords::{
    Compensation3_0, Dfc, Gating, MeasOrGateIndex, PrefixedMeasIndex, RegionGateIndex,
    RegionWindow, Trigger, UnstainedCenters,
};
use super::spillover::Spillover;

use derive_more::{AsRef, Display, From};
use derive_new::new;
use itertools::Itertools as _;
use nonempty::NonEmpty;
use std::collections::HashSet;
use thiserror::Error;

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr},
    std::fmt::Display,
};

// // TODO this actually should include everything
// /// Indices from all measurements except time if present
// #[derive(AsRef, From)]
// pub struct MeasIndicesNoTime(pub(crate) HashSet<MeasIndex>);

// /// $PnN ([`Shortname`]s) from all measurements except time if present
// #[derive(AsRef, From)]
// pub struct MeasNamesNoTime<'a>(pub(crate) HashSet<&'a Shortname>);

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
#[cfg_attr(feature = "python", pyerr(crate::python::RelationalError))]
#[cfg_attr(feature = "python", bound(SpecificKey<T, I>: Display))]
pub struct ExistingNamedLinkError<T, I> {
    pub key: SpecificKey<T, I>,
    pub names: NonEmpty<Shortname>,
}

/// Error when a keyword has indexed references to it which would be broken if dropped
#[derive(Debug, Error, new)]
#[error(
    "{key} refers to existing indices which are about to be dropped: {xs}",
    xs = self.names.iter().join(", ")
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::RelationalError))]
#[cfg_attr(feature = "python", bound(SpecificKey<T, I>: Display))]
pub struct ExistingIndexedLinkError<T, I> {
    pub key: SpecificKey<T, I>,
    pub names: NonEmpty<IndexFromOne>,
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
    Comp2_0(NonEmpty<RemovedComp2_0Cell>),
    Comp3_0(RemovedIndexLink<Compensation3_0>),
    Spillover(RemovedNamedLink<Spillover>),
    UnstainedCenters(RemovedNamedLink<UnstainedCenters>),
    Trigger(RemovedNamedLink<Trigger>),
}

/// An invalid $DFCmTOn keyword that was removed
#[derive(new)]
pub struct RemovedComp2_0Cell {
    row: MeasIndex,
    col: MeasIndex,
    value: f32,
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
    names: NonEmpty<Shortname>,
}

/// A keyword which links to a non-existent measurement index which was removed.
#[derive(new)]
pub struct RemovedIndexLink<T> {
    key: T,
    indices: NonEmpty<MeasIndex>,
}

/// A $RnI/$RnW pair which refers to a non-existent measurement index which was removed.
#[derive(new)]
pub struct RemovedGateLink<I> {
    pub(crate) region_index: RegionIndex,
    pub(crate) region: Region<I>,
    // TODO this will always either be 1 or 2
    pub(crate) meas_indices: NonEmpty<MeasIndex>,
}

/// A $GATING keyword which references non-existent $RnI/$RnW keywords and was removed.
#[derive(new)]
pub struct RemovedGating {
    pub(crate) region_indices: NonEmpty<RegionIndex>,
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

/// Error when key which references a non-existent measurement $PnN
#[derive(Debug, Display, Error, new)]
#[display(
    "{key} references non-existent $PnN: {bad}",
    bad = self.names.iter().join(", ")
)]
#[cfg_attr(
    feature = "python",
    derive(DisplayAsPyErr),
    pyerr(crate::python::RelationalError),
    bound(SpecificKey<T, I>: Display)
)]
pub struct NamedLinkError<T, I> {
    key: SpecificKey<T, I>,
    names: NonEmpty<Shortname>,
}

/// Error when key which references a non-existent measurement index
#[derive(Debug, Display, Error, new)]
#[display(
    "{key} references non-existent measurement indices: {bad}",
    bad = self.indices.iter().join(", ")
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::RelationalError))]
#[cfg_attr(feature = "python", bound(SpecificKey<T, I>: Display))]
pub struct IndexLinkError<T, I> {
    indices: NonEmpty<MeasIndex>,
    key: SpecificKey<T, I>,
}

/// Error when key which depends on another key which is invalid.
#[derive(Debug, Display, Error, new)]
#[display(
    "{key} depends on other keys which do not exist: {bad}",
    bad = self.deps.iter().join(", "),
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::RelationalError))]
#[cfg_attr(feature = "python", bound(SpecificKey<T, I>: Display))]
pub struct DependentKeyErrorInner<T, I> {
    deps: NonEmpty<StdKey>,
    key: SpecificKey<T, I>,
}

pub type KeyToNameLinkError<T> = NamedLinkError<T, ()>;

pub type KeyToIndexLinkError<T> = IndexLinkError<T, ()>;
pub type IndexedKeyToIndexLinkError<T> = IndexLinkError<T, IndexFromOne>;
pub type BiIndexedKeyToIndexLinkError<T> = IndexLinkError<T, BiIndex>;

pub type DependentKeyError<T> = DependentKeyErrorInner<T, ()>;
pub type DependentIndexedKeyError<T> = DependentKeyErrorInner<T, IndexFromOne>;

impl<T> NamedLinkError<T, ()> {
    pub(crate) fn new_i0(js: NonEmpty<Shortname>) -> Self {
        Self::new(SpecificKey::default(), js)
    }
}

impl<T> IndexLinkError<T, ()> {
    pub(crate) fn new_i0(js: NonEmpty<MeasIndex>) -> Self {
        Self::new(js, SpecificKey::default())
    }
}

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

impl RemovedLink {
    pub(crate) fn insert_keyvals(&self, kws: &mut NonStdKeywords) {
        macro_rules! go_gate {
            ($kws:expr, $x:expr) => {{
                for (k, v) in $x.region.opt_keywords_std($x.region_index) {
                    $kws.insert_demoted(k, v);
                }
            }};
        }
        match self {
            Self::GatingRegion3_0(x) => go_gate!(kws, x),
            Self::GatingRegion3_2(x) => go_gate!(kws, x),
            Self::Gating(x) => kws.insert_demoted_metaroot(&x.gating),
            Self::Comp2_0(xs) => {
                for x in xs {
                    let (k, v) = x.as_keyval();
                    kws.insert_demoted(k, v);
                }
            }
            Self::Comp3_0(x) => kws.insert_demoted_metaroot(&x.key),
            Self::Spillover(x) => kws.insert_demoted_metaroot(&x.key),
            Self::UnstainedCenters(x) => {
                if let Some(v) = x.key.display_maybe() {
                    kws.insert_demoted_as::<UnstainedCenters>(v);
                }
            }
            Self::Trigger(x) => kws.insert_demoted_metaroot(&x.key),
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
        match self {
            Self::GatingRegion3_0(x) => go_gate!(es, x),
            Self::GatingRegion3_2(x) => go_gate!(es, x),
            Self::Gating(x) => {
                let ks = x
                    .region_indices
                    .map(|ri| {
                        let k0 = RegionGateIndex::<()>::std(ri);
                        let k1 = RegionWindow::std(ri);
                        (k0, vec![k1])
                    })
                    .map(NonEmpty::from);
                let e = DependentKeyError::<Gating>::new1(NonEmpty::flatten(ks));
                es.push(e.into());
            }
            Self::Comp2_0(xs) => {
                for x in xs {
                    es.push(BrokenIndexedLinkError::from(x.as_error()).into());
                }
            }
            Self::Comp3_0(x) => es.push(BrokenIndexedLinkError::from(x.into_error()).into()),
            Self::Spillover(x) => es.push(BrokenNamedLinkError::from(x.into_error()).into()),
            Self::UnstainedCenters(x) => es.push(BrokenNamedLinkError::from(x.into_error()).into()),
            Self::Trigger(x) => es.push(BrokenNamedLinkError::from(x.into_error()).into()),
        }
    }
}

impl RemovedComp2_0Cell {
    fn as_keyval(&self) -> (StdKey, String) {
        // NOTE col is first
        let k = Dfc::std(self.col, self.row);
        (k, self.value.to_string())
    }

    fn as_error(&self) -> BiIndexedKeyToIndexLinkError<Dfc> {
        let xs = match self.missing {
            Comp2_0Missing::Row => NonEmpty::new(self.row),
            Comp2_0Missing::Col => NonEmpty::new(self.col),
            Comp2_0Missing::Both => {
                let mut xs = NonEmpty::new(self.col);
                xs.push(self.row);
                xs
            }
        };
        let k = SpecificKey::new_i2(self.col.into(), self.row.into());
        BiIndexedKeyToIndexLinkError::new(xs, k)
    }
}

impl<T: Key> RemovedNamedLink<T> {
    fn into_error(self) -> KeyToNameLinkError<T> {
        KeyToNameLinkError::new_i0(self.names)
    }
}

impl<T: Key> RemovedIndexLink<T> {
    fn into_error(self) -> KeyToIndexLinkError<T> {
        KeyToIndexLinkError::new_i0(self.indices)
    }
}

impl<I> RemovedGateLink<I> {
    fn into_errors(self) -> impl Iterator<Item = BrokenOrDependentLinkError>
    where
        BrokenIndexedLinkError: From<BrokenRegionLinkError<I>>,
    {
        let ri = self.region_index;
        let region_key = RegionGateIndex::<()>::std(ri);
        let k = SpecificKey::new_i1(ri.into());
        let e0 = IndexedKeyToIndexLinkError::new(self.meas_indices, k);
        let e1 = DependentIndexedKeyError::new2(ri.into(), NonEmpty::new(region_key));
        [BrokenIndexedLinkError::from(e0).into(), e1.into()].into_iter()
    }
}

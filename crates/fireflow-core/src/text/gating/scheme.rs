use crate::config::EvaledReadStdKeywordsConfig;
use crate::core::TrimmedKeywords;
use crate::data::IndexedError;
use crate::logging::{
    DeferredIter as _, DeferredSwitchableErrors, DeferredWarningsAndErrors, ResultExt as _,
    SwitchableErrorsResult,
};
use crate::std_index::index::StdLookupTx;
use crate::text::gating::{
    AnyIndexForRegionError, BiIndexForRegionError, LinkedMeasIndex, LookupRegionError, Region,
    UniIndexForRegionError,
};
use crate::text::keyword_enum::{
    DollarKey, Keyword0FromValue as _, OptRootKeyword, RegionKeyword, SplitKeyword,
};
use crate::text::keywords::{Gating, Par, RegionGateIndex, ValueToStdKey};
use crate::text::lookup::{Diagnosed, OptKeyError, OptValue};
use crate::text::relational::{
    BrokenRegionLinkError, DependentKeyError, ExistingIndexedLinkError, HasLink,
    KeyToIndexLinkError, RemovedGateLink, RemovedLink,
};

use fireflow_types::config::{AllowLoss, ProcessOptionalFailure, ReadDataKeywordsConfig};
use fireflow_types::index::{GateIndex, MeasIndex, RegionIndex};
use fireflow_types::keys::nonstd::DollarWrap;
use fireflow_types::keys::raw_std::{IndexedKey, RawStdKey, RegionKey, RegionKeyId};
use nonempty::{IntoIteratorExt as _, NEVec, NonEmptyIterator as _};
use type_families::{BifunctorOnce as _, impl_functor, impl_kind1};

use derive_more::{AsRef, Display, From};
use derive_new::new;
use itertools::Itertools as _;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::mem::take;
use std::str::FromStr;
use thiserror::Error;

#[cfg(feature = "serde")]
use serde::Serialize;

use super::LookupRegionIndexError;

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr},
    fireflow_types::python as py,
};

/// The $GATING/$RnI/$RnW keywords in a unified bundle.
///
/// All regions in $GATING are validated to have corresponding $RnI/$RnW
/// keywords, and each $RnI/$RnW pair is validated to be consistent (ie both are
/// univariate or bivariate)
#[derive(Clone, PartialEq, AsRef, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct GatingScheme<I> {
    #[as_ref(Option<Gating>)]
    gating: Option<Gating>,
    #[as_ref(HashMap<RegionIndex, Region<I>>)]
    regions: HashMap<RegionIndex, Region<I>>,
}

/// Error when parsing $GATING/$RnI/$RnW keywords
#[derive(Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(E: Into<Self>))]
pub enum LookupGatingSchemeError<E> {
    Link(DependentKeyError<Gating>),
    Gating(OptKeyError<Gating>),
    Region(LookupRegionError<E>),
}

/// Error when converting $GATING/$RnI/$RnW keywords to new version.
///
/// $RnI can fail because it may contain indices that refer to something
/// that is unsupported in the target version.
///
/// $GATING can fail because it may refer to $RnI/$RnW keywords which are
/// no longer valid as described above.
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(ConvertIndexForRegionError<I>: fmt::Display))]
pub enum ConvertSchemeError<I> {
    Region(ConvertIndexForRegionError<I>),
    Scheme(DependentKeyError<Gating>),
}

/// Error when converting 3.0./3.1 $RnI keywords to either 2.0 or 3.2
///
/// In 3.0/3.1, these can point to either gates or measurements. In either
/// target version they can only point to one, in which case any with the other
/// should lead to this error.
#[derive(Debug, Display, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConversionError))]
#[cfg_attr(feature = "python", bound(ConvertIndexForRegionError<I>: fmt::Display))]
pub struct ConvertIndexForRegionError<I>(IndexedError<RegionIndex, AnyIndexForRegionError<I>>);

impl<I> Default for GatingScheme<I> {
    fn default() -> Self {
        Self::new(None, HashMap::new())
    }
}

impl_kind1!(pub GatingSchemeFamily, GatingScheme);

impl_functor!(
    GatingScheme,
    self,
    mut f,
    GatingScheme::new(self.gating, self.regions.fmap(|ri| ri.fmap(&mut f)))
);

impl<I> GatingScheme<I> {
    pub fn try_new(
        gating: Option<Gating>,
        regions: HashMap<RegionIndex, Region<I>>,
    ) -> Result<Self, DependentKeyError<Gating>> {
        if let Some(ris) = gating.as_ref().and_then(|g| {
            g.region_indices()
                .into_iter()
                .filter(|ri| !regions.contains_key(ri))
                .map(|ri| RegionKey::new(ri, RegionKeyId::I))
                .map(RawStdKey::from)
                .map(DollarWrap)
                .try_into_nonempty_iter()
        }) {
            Err(DependentKeyError::new1(ris.collect()))
        } else {
            Ok(Self { gating, regions })
        }
    }

    #[must_use]
    pub fn split(self) -> (HashMap<RegionIndex, Region<I>>, Option<Gating>) {
        (self.regions, self.gating)
    }

    pub(crate) fn regions(&self) -> &HashMap<RegionIndex, Region<I>> {
        &self.regions
    }

    /// Shift indices when a new measurement is inserted.
    ///
    /// New measurement is assumed to be inserted at `i`. All regions with
    /// measurement indices greater than i will be incremented by one.
    pub(crate) fn shift_meas_indices_after_insert(&mut self, i: MeasIndex)
    where
        I: LinkedMeasIndex,
    {
        for r in self.regions.values_mut() {
            r.shift_after_insert(i);
        }
    }

    /// Shift indices when a new measurement is removed.
    ///
    /// Measurement at `i` is assumed to be removed and should not have any
    /// regions pointing to it. All regions with measurement indices greater
    /// than i will be decremented by one.
    pub(crate) fn shift_meas_indices_after_remove(&mut self, i: MeasIndex)
    where
        I: LinkedMeasIndex,
    {
        for r in self.regions.values_mut() {
            r.shift_after_remove(i);
        }
    }

    pub(crate) fn existing_link_errors<C>(
        &self,
        indices: &C,
    ) -> impl Iterator<Item = ExistingIndexedLinkError<RegionGateIndex<I>, MeasIndex>>
    where
        RegionGateIndex<I>: ValueToStdKey<Index = RegionIndex>,
        I: LinkedMeasIndex,
        C: HasLink<Inner = MeasIndex>,
    {
        self.meas_indices()
            .filter(|(_, mi)| indices.has_link(mi))
            .map(|(ri, mi)| {
                let js = NEVec::new(mi);
                ExistingIndexedLinkError::new(DollarKey::new(ri), js)
            })
    }

    pub(crate) fn invalid_link_errors(
        &self,
        par: &Par,
    ) -> impl Iterator<Item = BrokenRegionLinkError>
    where
        I: LinkedMeasIndex,
    {
        self.meas_indices()
            .filter(|(_, mi)| usize::from(*mi) >= par.0)
            .map(|(ri, mi)| {
                let js = NEVec::new(mi);
                KeyToIndexLinkError::new(js, DollarKey::new(ri))
            })
    }

    pub(crate) fn remove_invalid_links(&mut self, par: Par) -> Vec<RemovedLink>
    where
        I: LinkedMeasIndex,
        RemovedLink: From<RemovedGateLink>,
    {
        let mut bad_indices = HashSet::new();
        let mut removed_links = vec![];
        // Then remove any $RnI/$RnW keywords which reference measurements that
        // don't exist.
        self.regions = take(&mut self.regions)
            .into_iter()
            .filter_map(|(ri, region)| {
                if let Some(xs) = region.indices().filter_map(|x| {
                    let y = x.meas_index()?;
                    (usize::from(y) >= par.0).then_some(y)
                }) {
                    let e = RemovedLink::from(RemovedGateLink::new(ri, xs));
                    removed_links.push(e);
                    bad_indices.insert(ri);
                    None
                } else {
                    Some((ri, region))
                }
            })
            .collect();
        // Check the $GATING keyword to see if it has any links to $RnI/$RnW
        // which in turn reference measurement indices that don't exist. If it
        // has any, rip it out and return it.
        self.gating = take(&mut self.gating).and_then(|g| {
            let xs = g.region_indices();
            let ys = xs.iter().copied().filter(|rni| bad_indices.contains(rni));
            if let Some(zs) = ys.try_into_nonempty_iter() {
                let e = RemovedLink::Gating(zs.collect());
                removed_links.push(e);
                None
            } else {
                Some(g)
            }
        });
        removed_links
    }

    fn meas_indices(&self) -> impl Iterator<Item = (RegionIndex, MeasIndex)>
    where
        I: LinkedMeasIndex,
    {
        self.regions
            .iter()
            .flat_map(|(ri, v)| v.meas_indices().map(|mi| (*ri, mi)))
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn lookup<C>(
        kws: &mut StdLookupTx,
        conf: &C,
    ) -> DeferredWarningsAndErrors<
        Diagnosed<Self, TrimmedKeywords>,
        LookupGatingSchemeError<LookupRegionIndexError<I>>,
        LookupGatingSchemeError<LookupRegionIndexError<I>>,
    >
    where
        I: FromStr + LinkedMeasIndex + PartialEq + Copy,
        C: AsRef<ReadDataKeywordsConfig> + AsRef<EvaledReadStdKeywordsConfig>,
        for<'a> RegionKeyword<'a>: From<SplitKeyword<RegionGateIndex<I>>>,
        RegionGateIndex<I>:
            ValueToStdKey<Index = RegionIndex> + OptValue<Outer = Option<RegionGateIndex<I>>>,
    {
        let rconf: &ReadDataKeywordsConfig = conf.as_ref();
        let flag = rconf.process_optional_failure;
        let (gating, ge) = Gating::remove_or_drop_root_opt(kws, conf.as_ref());
        // Looking up each region keyword is tricky. Unlike $Pn* and $Gn*
        // keywords, there is no $PAR or $GATE equivalent to bound the search.
        // We could use $GATING but the standard does not explicitly forbid
        // having $Rn* keywords that are not in $GATING (the opposite is true
        // however).
        //
        // Luckily, we already partitioned each keyword by types, so
        // exhaustively searching each region keyword and parsing should not be
        // too challenging and should be effectively a no-op for files without
        // $Rn* keywords (ie, 99.999999999% of them). First get a list of all
        // keys, then find which indices are paired (ie have both $RnI and
        // $RnW). These are the ones we care about. The rest won't be touched
        // and will wash out later when we process extra keywords that weren't
        // parsed (which makes sense since there is no obvious reason to have a
        // non-paired $Rn* keyword).
        let (ris, rws): (HashSet<_>, HashSet<_>) = kws
            .iter_region_keys()
            .map(|rk| match rk.id {
                RegionKeyId::I => Ok(rk.index),
                RegionKeyId::W => Err(rk.index),
            })
            .partition_result();
        ris.union(&rws)
            .map(|&ri| {
                Region::lookup(kws, ri, conf)
                    .map_deferred_value(|out| out.first_once(|r| r.map(|x| (ri, x))))
                    .map_errors(LookupGatingSchemeError::Region)
                    .map_commutative_warnings(LookupGatingSchemeError::Region)
            })
            .sequence_def()
            .and_then_deferred_switchable_result3(flag, |rs| {
                let mut regions = HashMap::new();
                let mut trimmed = vec![];
                for r in rs {
                    let _ = r.inner.map(|(k, v)| regions.insert(k, v));
                    trimmed.extend(r.diagnostic);
                }
                Self::try_new(gating, regions)
                    .map_err(LookupGatingSchemeError::Link)
                    .map(|x| Diagnosed::new(x, trimmed))
            })
            .extend_warnings_or_errors3(
                ge,
                |v| v,
                LookupGatingSchemeError::Gating,
                LookupGatingSchemeError::Gating,
                flag,
            )
    }

    pub(crate) fn set_failure_flag(self, kws: &mut StdLookupTx, flag: ProcessOptionalFailure)
    where
        I: Copy,
        RegionGateIndex<I>: ValueToStdKey<Index = RegionIndex>,
        for<'a> RegionKeyword<'a>: From<SplitKeyword<RegionGateIndex<I>>>,
    {
        for (ri, r) in self.regions {
            r.set_failure_flag(ri, kws, flag);
        }
        if let Some(g) = self.gating.as_ref().map(ValueToStdKey::std0_) {
            kws.set_failure_flag(&g, flag);
        }
    }

    pub(crate) fn opt_keywords<'a>(&'a self) -> impl Iterator<Item = OptRootKeyword<'a>>
    where
        I: Copy,
        RegionGateIndex<I>: ValueToStdKey<Index = RegionIndex>,
        RegionKeyword<'a>: From<SplitKeyword<RegionGateIndex<I>>>,
    {
        let gating = self.gating.as_ref().map(OptRootKeyword::from_ref);
        self.regions
            .iter()
            .flat_map(|(ri, r)| r.opt_keywords(*ri))
            .map(OptRootKeyword::from)
            .chain(gating)
    }

    pub(crate) fn convert_indices<J0, J1>(
        self,
        flag: AllowLoss,
    ) -> DeferredSwitchableErrors<GatingScheme<J0>, AllowLoss, ConvertSchemeError<J1>>
    where
        I: Copy,
        J0: TryFrom<I, Error = UniIndexForRegionError<J1>>,
        AnyIndexForRegionError<J1>: From<J0::Error> + From<BiIndexForRegionError<J1>>,
    {
        // ASSUME region indices will still be unique in new hash table
        let mut regions = HashMap::new();
        let es = self
            .regions
            .into_iter()
            .filter_map(|(ri, r)| match r.try_index_into() {
                Ok(r_) => {
                    regions.insert(ri, r_);
                    None
                }
                Err(e) => Some(IndexedError::new(ri, e)),
            })
            .map(ConvertIndexForRegionError)
            .map(ConvertSchemeError::from);
        SwitchableErrorsResult::new_switchable_iter3((), (), es, flag).and_then_switchable(|()| {
            GatingScheme::try_new(self.gating, regions)
                .map_err(ConvertSchemeError::from)
                .into_nowarn()
                .set_err_value(GatingScheme::default())
        })
    }
}

impl fmt::Display for ConvertIndexForRegionError<GateIndex> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        self.fmt(true, f)
    }
}

impl fmt::Display for ConvertIndexForRegionError<MeasIndex> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        self.fmt(false, f)
    }
}

impl<I> ConvertIndexForRegionError<I> {
    fn fmt(&self, index_is_gate: bool, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error>
    where
        I: fmt::Display,
    {
        let region_key = RawStdKey::from(IndexedKey::new(self.0.index, RegionKeyId::I));
        let keys = |i: &I, is_plural: bool, is_gate: bool| {
            let prefix = if is_gate { "G" } else { "P" };
            let key = format!("{prefix}{i}*");
            if is_plural {
                format!("{key} keywords")
            } else {
                format!("a {key} keyword")
            }
        };
        let target_keys = |i: &I, is_plural: bool| {
            let meas = keys(i, is_plural, false);
            let gate = keys(i, is_plural, true);
            if index_is_gate {
                (gate, meas)
            } else {
                (meas, gate)
            }
        };
        let (which, (from, to)) = match &self.0.error {
            AnyIndexForRegionError::Univariate(UniIndexForRegionError(i)) => {
                ("index", target_keys(i, false))
            }
            AnyIndexForRegionError::Bivariate(b) => match b {
                BiIndexForRegionError::LeftBivariate(i) => ("left index", target_keys(i, true)),
                BiIndexForRegionError::RightBivariate(i) => ("right index", target_keys(i, true)),
                BiIndexForRegionError::Bivariate(i0, i1) => {
                    let (from0, to0) = target_keys(i0, true);
                    let (from1, to1) = target_keys(i1, true);
                    let from = format!("{from0} and {from1}");
                    let to = format!("{to0} and {to1}");
                    ("indices", (from, to))
                }
            },
        };
        write!(
            f,
            "cannot convert {which} in {region_key} to refer \
             to {to} because they currently refer to {from}"
        )
    }
}

use crate::config::{AllowLoss, ReadDataKeywordsConfig, ReadStdKeywordsConfig};
use crate::core::TrimmedKeywords;
use crate::data::IndexedError;
use crate::fixed_vec::OneOrTwo;
use crate::logging::{
    DeferredIter as _, DeferredSwitchableErrors, DeferredWarningsAndErrors, LogResult,
    ResultExt as _, SwitchableErrorsResult, WarningsAndErrorsResult,
};
use crate::nonempty::FcsNEVec;
use crate::text::index::{GateIndex, IndexFromOne, MeasIndex, RegionIndex};
use crate::text::keyword_enum::{
    GateMeasKeyword, Keyword0FromValue as _, Keyword1FromValue as _, OptRootKeyword, RegionKeyword,
    SplitKeyword, SplitKeyword1,
};
use crate::text::keywords::{
    Gate, GateDetectorType, GateDetectorVoltage, GateFilter, GateLongname, GatePercentEmitted,
    GateRange, GateScale, GateShortname, Gating, IndexPair, MeasOrGateIndex, Par,
    PrefixedMeasIndex, RegionGateIndex, RegionWindow, RegionWindowRef, ScaleFix, UniGate, Vertex,
};
use crate::text::lookup::{
    OptIndexedKey, OptIndexedKeyError, OptIndexedKeyStError, OptKeyError, OptMetarootKey as _,
    Optional,
};
use crate::text::relational::{
    BrokenRegionLinkError, DependentKeyError, ExistingIndexedLinkError, IndexedKeyToIndexLinkError,
    IndicesToRemove, RemovedGateLink, RemovedGating, RemovedLink,
};
use crate::validated::keys::{
    AsStdKey as _, DKey1, IndexedKey as _, NonStdKeywords, NonStdKeywordsExt as _, StdKeywords,
};
use fireflow_types::nonempty_string::{DisplayNE as _, ToNE};
use type_families::{
    ApplyOnce as _, Functor as _, FunctorOnce as _, impl_functor, impl_functor_once, impl_kind1,
};

use derive_more::{AsRef, Display, From};
use derive_new::new;
use itertools::Itertools as _;
use nonempty_collections::{IntoIteratorExt as _, NEVec, iter::NonEmptyIterator as _};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::mem::take;
use std::str::FromStr;
use thiserror::Error;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr},
    fireflow_types::python as py,
};

/// The $GATING/$RnI/$RnW/$Gn* keywords in a unified bundle (2.0)
pub type AppliedGates2_0 = AppliedGatesPre3_2<GateIndex>;

/// The $GATING/$RnI/$RnW/$Gn* keywords in a unified bundle (3.0-3.1)
pub type AppliedGates3_0 = AppliedGatesPre3_2<MeasOrGateIndex>;

/// The $GATING/$RnI/$RnW/$Gn* keywords in a unified bundle (2.0-3.1)
#[derive(Clone, PartialEq, AsRef)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct AppliedGatesPre3_2<I> {
    #[as_ref(GatedMeasurements)]
    #[as_ref([GatedMeasurement])]
    gated_measurements: GatedMeasurements,
    #[as_ref(Option<Gating>)]
    #[as_ref(HashMap<RegionIndex, Region<I>>)]
    scheme: GatingScheme<I>,
}

impl<I> Default for AppliedGatesPre3_2<I> {
    fn default() -> Self {
        Self {
            gated_measurements: GatedMeasurements::default(),
            scheme: GatingScheme::default(),
        }
    }
}

/// The $GATING/$RnI/$RnW keywords in a unified bundle (3.2)
#[derive(Clone, PartialEq, Default, AsRef)]
#[as_ref(Option<Gating>)]
#[as_ref(HashMap<RegionIndex, Region3_2>)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct AppliedGates3_2(pub GatingScheme<PrefixedMeasIndex>);

/// The $GATING/$RnI/$RnW keywords in a unified bundle.
///
/// All regions in $GATING are assumed to have corresponding $RnI/$RnW keywords,
/// and each $RnI/$RnW pair is assumed to be consistent (ie both are univariate
/// or bivariate)
#[derive(Clone, PartialEq, AsRef, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct GatingScheme<I> {
    #[as_ref(Option<Gating>)]
    gating: Option<Gating>,
    #[as_ref(HashMap<RegionIndex, Region<I>>)]
    regions: HashMap<RegionIndex, Region<I>>,
}

/// A list of $Gn* keywords for indices 1-n.
///
/// $GATE is equal to length of this.
#[derive(Clone, PartialEq, Default, From, AsRef)]
#[as_ref([GatedMeasurement])]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct GatedMeasurements(pub Vec<GatedMeasurement>);

/// A uni/bivariate region corresponding to an $RnI/$RnW keyword pair
#[derive(Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum Region<I> {
    Univariate(UnivariateRegion<I>),
    Bivariate(BivariateRegion<I>),
}

impl_kind1!(pub RegionFamily, Region);
impl_kind1!(pub UnivariateRegionFamily, UnivariateRegion);
impl_kind1!(pub BivariateRegionFamily, BivariateRegion);

pub type Region2_0 = Region<GateIndex>;
pub type Region3_0 = Region<MeasOrGateIndex>;
pub type Region3_2 = Region<PrefixedMeasIndex>;

/// A univariate region corresponding to an $RnI/$RnW keyword pair
#[derive(Clone, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct UnivariateRegion<I> {
    pub gate: UniGate,
    pub index: I,
}

/// A bivariate region corresponding to an $RnI/$RnW keyword pair
#[derive(Clone, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct BivariateRegion<I> {
    pub vertices: FcsNEVec<Vertex>,
    pub index: IndexPair<I>,
}

/// The values for $Gm* keywords (2.0-3.1)
#[allow(clippy::too_many_arguments)]
#[derive(Clone, Default, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct GatedMeasurement {
    /// Value for $GmE
    #[new(into)]
    pub scale: Option<GateScale>,

    /// Value for $GmF
    pub filter: GateFilter,

    /// Value for $GmN
    #[new(into)]
    pub shortname: Option<GateShortname>,

    /// Value for $GmP
    #[new(into)]
    pub percent_emitted: Option<GatePercentEmitted>,

    /// Value for $GmR
    #[new(into)]
    pub range: Option<GateRange>,

    /// Value for $GmS
    pub longname: GateLongname,

    /// Value for $GmT
    pub detector_type: GateDetectorType,

    /// Value for $GmV
    #[new(into)]
    pub detector_voltage: Option<GateDetectorVoltage>,
}

pub(crate) trait LinkedMeasIndex: Sized {
    fn meas_index(&self) -> Option<MeasIndex>;

    fn meas_index_mut(&mut self) -> Option<&mut MeasIndex>;
}

impl LinkedMeasIndex for GateIndex {
    fn meas_index(&self) -> Option<MeasIndex> {
        None
    }

    fn meas_index_mut(&mut self) -> Option<&mut MeasIndex> {
        None
    }
}

impl LinkedMeasIndex for MeasOrGateIndex {
    fn meas_index(&self) -> Option<MeasIndex> {
        match self {
            Self::Gate(_) => None,
            Self::Meas(x) => Some(*x),
        }
    }

    fn meas_index_mut(&mut self) -> Option<&mut MeasIndex> {
        match self {
            Self::Gate(_) => None,
            Self::Meas(x) => Some(x),
        }
    }
}

impl LinkedMeasIndex for PrefixedMeasIndex {
    fn meas_index(&self) -> Option<MeasIndex> {
        Some((*self).into())
    }

    fn meas_index_mut(&mut self) -> Option<&mut MeasIndex> {
        Some(self.as_mut())
    }
}

impl_functor_once!(
    UnivariateRegion,
    self,
    mut f,
    UnivariateRegion::new(self.gate, f(self.index))
);

impl_functor!(
    BivariateRegion,
    self,
    f,
    BivariateRegion::new(self.vertices, self.index.fmap(f))
);

impl_functor!(
    Region,
    self,
    f,
    match self {
        Self::Univariate(x) => Region::Univariate(x.fmap_once(f)),
        Self::Bivariate(x) => Region::Bivariate(x.fmap(f)),
    }
);

impl<I> UnivariateRegion<I> {
    fn try_map<F, J, E>(self, f: F) -> Result<UnivariateRegion<J>, E>
    where
        F: FnOnce(I) -> Result<J, E>,
    {
        Ok(UnivariateRegion {
            gate: self.gate,
            index: f(self.index)?,
        })
    }
}

impl<I> BivariateRegion<I> {
    fn try_map<F, J, E>(self, f: F) -> Result<BivariateRegion<J>, E>
    where
        F: FnMut(I, I) -> Result<(J, J), E>,
    {
        Ok(BivariateRegion::new(self.vertices, self.index.try_map(f)?))
    }
}

impl<I> AppliedGatesPre3_2<I> {
    pub fn try_new(
        gated_measurements: Vec<GatedMeasurement>,
        scheme: GatingScheme<I>,
    ) -> Result<Self, GateMeasurementLinkError>
    where
        I: Copy,
        GateIndex: TryFrom<I>,
    {
        let n = gated_measurements.len();
        if let Some(xs) = scheme
            .regions
            .iter()
            .flat_map(|(_, r)| r.indices())
            .copied()
            .flat_map(GateIndex::try_from)
            .filter(|&i| usize::from(i) >= n)
            .try_into_nonempty_iter()
        {
            Err(GateMeasurementLinkError(xs.collect()))
        } else {
            Ok(Self {
                gated_measurements: gated_measurements.into(),
                scheme,
            })
        }
    }

    pub fn try_new1(
        gated_measurements: Vec<GatedMeasurement>,
        regions: HashMap<RegionIndex, Region<I>>,
        gating: Option<Gating>,
    ) -> Result<Self, NewAppliedGatesWithSchemeError>
    where
        I: Copy,
        GateIndex: TryFrom<I>,
    {
        let scheme = GatingScheme::try_new(gating, regions)?;
        Ok(Self::try_new(gated_measurements, scheme)?)
    }

    #[must_use]
    pub fn split(
        self,
    ) -> (
        Vec<GatedMeasurement>,
        HashMap<RegionIndex, Region<I>>,
        Option<Gating>,
    ) {
        (
            self.gated_measurements.0,
            self.scheme.regions,
            self.scheme.gating,
        )
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn lookup<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        conf: &C,
    ) -> WarningsAndErrorsResult<
        (Self, TrimmedKeywords, Vec<ScaleFix>),
        (),
        LookupAppliedGatesPre3_2Error<I>,
        LookupAppliedGatesPre3_2Error<I>,
    >
    where
        GateIndex: TryFrom<I>,
        I: FromStr + LinkedMeasIndex + PartialEq + Copy,
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadStdKeywordsConfig>,
        for<'a> RegionKeyword<'a>: From<SplitKeyword1<RegionGateIndex<I>>>,
        RegionGateIndex<I>: OptIndexedKey + Optional<Outer = Option<RegionGateIndex<I>>>,
    {
        let ag = GatingScheme::lookup(std, nonstd, conf)
            .map_errors(LookupAppliedGatesError::Scheme)
            .map_commutative_warnings(LookupAppliedGatesError::Scheme);
        let gm = GatedMeasurements::lookup(std, nonstd, conf)
            .map_errors(LookupAppliedGatesError::GatedMeas)
            .map_commutative_warnings(LookupAppliedGatesError::GatedMeas);
        let rconf: &ReadDataKeywordsConfig = conf.as_ref();
        let flag = rconf.process_optional_failure;
        ag.zip_f2_once(gm)
            .and_then_deferred_switchable_result(flag, |(scheme_out, gated_ms_out)| {
                let (scheme, scheme_diag) = scheme_out;
                let (gated_ms, gated_ms_diag) = gated_ms_out;
                Self::try_new(gated_ms.0, scheme)
                    .map_err(LookupAppliedGatesError::Link)
                    .map(|x| (x, scheme_diag, gated_ms_diag))
            })
            .map_err_value(|(ret, _, _)| {
                if rconf.process_optional_failure.is_demote() {
                    ret.scheme.demote_keywords(nonstd);
                    ret.gated_measurements.demote_keywords(nonstd);
                }
            })
    }

    pub(crate) fn opt_keywords<'a>(&'a self) -> impl Iterator<Item = OptRootKeyword<'a>>
    where
        I: Copy,
        RegionKeyword<'a>: From<SplitKeyword1<RegionGateIndex<I>>>,
    {
        let gate = self
            .gated_measurements
            .gate()
            .map(OptRootKeyword::from_value);
        self.gated_measurements
            .0
            .iter()
            .enumerate()
            .flat_map(|(i, m)| m.opt_keywords(i.into()))
            .map(OptRootKeyword::from)
            .chain(gate)
            .chain(self.scheme.opt_keywords())
    }
}

impl AppliedGates3_0 {
    /// Shift indices when a new measurement is inserted.
    ///
    /// New measurement is assumed to be inserted at `i`. All regions with
    /// measurement indices greater than i will be incremented by one.
    pub(crate) fn shift_meas_indices_after_insert(&mut self, i: MeasIndex) {
        self.scheme.shift_meas_indices_after_insert(i);
    }

    /// Shift indices when a new measurement is removed.
    ///
    /// Measurement at `i` is assumed to be removed and should not have any
    /// regions pointing to it. All regions with measurement indices greater
    /// than i will be decremented by one.
    pub(crate) fn shift_meas_indices_after_remove(&mut self, i: MeasIndex) {
        self.scheme.shift_meas_indices_after_remove(i);
    }

    pub(crate) fn remove_invalid_links(&mut self, par: Par) -> Vec<RemovedLink> {
        self.scheme.remove_invalid_links(par)
    }

    pub(crate) fn existing_link_errors(
        &self,
        indices: &IndicesToRemove,
    ) -> impl Iterator<Item = ExistingIndexedLinkError<RegionGateIndex<MeasOrGateIndex>, IndexFromOne>>
    {
        self.scheme.existing_link_errors(indices)
    }

    pub(crate) fn invalid_link_errors(
        &self,
        par: &Par,
    ) -> impl Iterator<Item = BrokenRegionLinkError<MeasOrGateIndex>> {
        self.scheme.invalid_link_errors(par)
    }

    pub(crate) fn try_into_2_0(
        self,
        flag: AllowLoss,
    ) -> DeferredSwitchableErrors<AppliedGates2_0, AllowLoss, AppliedGates3_0To2_0Error> {
        self.scheme
            .convert_indices(flag)
            .map_switchable_errors(AppliedGates3_0To2_0Error::from)
            .and_then_switchable(|scheme| {
                AppliedGates2_0::try_new(self.gated_measurements.0, scheme)
                    .map_err(AppliedGates3_0To2_0Error::from)
                    .into_nowarn()
                    .set_err_value(AppliedGates2_0::default())
            })
    }

    pub(crate) fn try_into_3_2(
        self,
        flag: AllowLoss,
    ) -> DeferredSwitchableErrors<AppliedGates3_2, AllowLoss, AppliedGates3_0To3_2Error> {
        self.scheme
            .convert_indices(flag)
            .map_deferred_value(AppliedGates3_2)
    }
}

impl AppliedGates3_2 {
    pub fn try_new(
        gating: Option<Gating>,
        regions: HashMap<RegionIndex, Region<PrefixedMeasIndex>>,
    ) -> Result<Self, DependentKeyError<Gating>> {
        GatingScheme::try_new(gating, regions).map(Self)
    }

    #[must_use]
    pub fn split(self) -> (HashMap<RegionIndex, Region3_2>, Option<Gating>) {
        (self.0.regions, self.0.gating)
    }

    /// Shift indices when a new measurement is inserted.
    ///
    /// New measurement is assumed to be inserted at `i`. All regions with
    /// measurement indices greater than i will be incremented by one.
    pub(crate) fn shift_meas_indices_after_insert(&mut self, i: MeasIndex) {
        self.0.shift_meas_indices_after_insert(i);
    }

    /// Shift indices when a new measurement is removed.
    ///
    /// Measurement at `i` is assumed to be removed and should not have any
    /// regions pointing to it. All regions with measurement indices greater
    /// than i will be decremented by one.
    pub(crate) fn shift_meas_indices_after_remove(&mut self, i: MeasIndex) {
        self.0.shift_meas_indices_after_remove(i);
    }

    pub(crate) fn existing_link_errors(
        &self,
        indices: &IndicesToRemove,
    ) -> impl Iterator<Item = ExistingIndexedLinkError<RegionGateIndex<PrefixedMeasIndex>, IndexFromOne>>
    {
        self.0.existing_link_errors(indices)
    }

    pub(crate) fn invalid_link_errors(
        &self,
        par: &Par,
    ) -> impl Iterator<Item = BrokenRegionLinkError<PrefixedMeasIndex>> {
        self.0.invalid_link_errors(par)
    }

    pub(crate) fn lookup<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        conf: &C,
    ) -> WarningsAndErrorsResult<
        (Self, TrimmedKeywords),
        (),
        LookupAppliedGates3_2Error,
        LookupAppliedGates3_2Error,
    >
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadStdKeywordsConfig>,
    {
        let rconf: &ReadDataKeywordsConfig = conf.as_ref();
        GatingScheme::lookup(std, nonstd, conf)
            .map_ok_value(|(x, y)| (Self(x), y))
            .map_err_value(|(ret, _)| {
                if rconf.process_optional_failure.is_demote() {
                    ret.demote_keywords(nonstd);
                }
            })
    }

    pub(crate) fn opt_keywords(&self) -> impl Iterator<Item = OptRootKeyword<'_>> {
        self.0.opt_keywords()
    }
}

impl GatedMeasurement {
    fn lookup<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        i: GateIndex,
        conf: &C,
    ) -> DeferredWarningsAndErrors<(Self, ScaleFix), LookupGatedMeasError, LookupGatedMeasError>
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadStdKeywordsConfig>,
    {
        macro_rules! go {
            ($x:expr) => {
                $x.map_switchable_errors(LookupGatedMeasError::from)
                    .switchable_into_commutative()
                    .into_semigroup()
            };
        }
        let scale = GateScale::remove_or_drop_meas_opt_with(std, nonstd, i, (), conf);
        let filter = GateFilter::remove_meas_opt_nofail(std, i);
        let sname = GateShortname::remove_or_drop_meas_opt(std, nonstd, i, conf.as_ref());
        let pemit = GatePercentEmitted::remove_or_drop_meas_opt(std, nonstd, i, conf.as_ref());
        let range = GateRange::remove_or_drop_meas_opt(std, nonstd, i, conf.as_ref());
        let lname = GateLongname::remove_meas_opt_nofail(std, i);
        let dtype = GateDetectorType::remove_meas_opt_nofail(std, i);
        let dvolt = GateDetectorVoltage::remove_or_drop_meas_opt(std, nonstd, i, conf.as_ref());
        go!(scale).lift_f5_once(
            go!(sname),
            go!(pemit),
            go!(range),
            go!(dvolt),
            |s, n, p, r, v| {
                (
                    Self::new(s.native, filter, n, p, r, lname, dtype, v),
                    s.diagnostic,
                )
            },
        )
    }

    fn opt_keywords(&self, i: GateIndex) -> impl IntoIterator<Item = GateMeasKeyword<'_>> {
        let x0 = GateMeasKeyword::from_str(&self.filter, i);
        let x1 = GateMeasKeyword::from_str(&self.longname, i);
        let x2 = GateMeasKeyword::from_str(&self.detector_type, i);
        let x3 = self.scale.map(|v| GateMeasKeyword::from_value(v, i));
        let x4 = self
            .shortname
            .as_ref()
            .map(|v| GateMeasKeyword::from_ref(v, i));
        let x5 = self
            .percent_emitted
            .map(|v| GateMeasKeyword::from_value(v, i));
        let x6 = self.range.as_ref().map(|v| GateMeasKeyword::from_ref(v, i));
        let x7 = self
            .detector_voltage
            .map(|v| GateMeasKeyword::from_value(v, i));
        [x0, x1, x2, x3, x4, x5, x6, x7].into_iter().flatten()
    }

    fn demote_keywords(self, i: GateIndex, nonstd: &mut NonStdKeywords) {
        for k in self.opt_keywords(i) {
            nonstd.insert_demoted_keyword(OptRootKeyword::from(k).into());
        }
    }
}

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
                .map(RegionGateIndex::<()>::std)
                .try_into_nonempty_iter()
        }) {
            Err(DependentKeyError::new1(ris.collect()))
        } else {
            Ok(Self { gating, regions })
        }
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

    pub(crate) fn existing_link_errors(
        &self,
        indices: &IndicesToRemove,
    ) -> impl Iterator<Item = ExistingIndexedLinkError<RegionGateIndex<I>, IndexFromOne>>
    where
        I: LinkedMeasIndex,
    {
        self.meas_indices()
            .filter(|(_, mi)| indices.as_ref().contains(mi))
            .map(|(ri, mi)| {
                let js = NEVec::new(mi.into());
                ExistingIndexedLinkError::new(DKey1::new_i1(ri), js)
            })
    }

    pub(crate) fn invalid_link_errors(
        &self,
        par: &Par,
    ) -> impl Iterator<Item = BrokenRegionLinkError<I>>
    where
        I: LinkedMeasIndex,
    {
        self.meas_indices()
            .filter(|(_, mi)| usize::from(*mi) >= par.0)
            .map(|(ri, mi)| {
                let js = NEVec::new(mi);
                IndexedKeyToIndexLinkError::new(js, DKey1::new_i1(ri))
            })
    }

    pub(crate) fn remove_invalid_links(&mut self, par: Par) -> Vec<RemovedLink>
    where
        I: LinkedMeasIndex,
        RemovedLink: From<RemovedGateLink<I>>,
    {
        let mut bad_indices = HashSet::new();
        let mut removed_links = vec![];
        // Then remove any $RnI/$RnW keywords which reference measurements that
        // don't exist.
        self.regions = take(&mut self.regions)
            .into_iter()
            .filter_map(|(rni, rnw)| {
                if let Some(xs) = rnw.indices().filter_map(|x| {
                    let y = x.meas_index()?;
                    (usize::from(y) >= par.0).then_some(y)
                }) {
                    let e = RemovedLink::from(RemovedGateLink::new(rni, rnw, xs));
                    removed_links.push(e);
                    bad_indices.insert(rni);
                    None
                } else {
                    Some((rni, rnw))
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
                let e = RemovedLink::Gating(RemovedGating::new(zs.collect(), g));
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
    fn lookup<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        conf: &C,
    ) -> DeferredWarningsAndErrors<
        (Self, TrimmedKeywords),
        LookupGatingSchemeError<LookupRegionIndexError<I>>,
        LookupGatingSchemeError<LookupRegionIndexError<I>>,
    >
    where
        I: FromStr + LinkedMeasIndex + PartialEq + Copy,
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadStdKeywordsConfig>,
        for<'a> RegionKeyword<'a>: From<SplitKeyword1<RegionGateIndex<I>>>,
        RegionGateIndex<I>: OptIndexedKey + Optional<Outer = Option<RegionGateIndex<I>>>,
    {
        let rconf: &ReadDataKeywordsConfig = conf.as_ref();
        let flag = rconf.process_optional_failure;
        Gating::remove_or_drop_root_opt(std, nonstd, conf.as_ref())
            .map_switchable_errors(LookupGatingSchemeError::Gating)
            .switchable_into_commutative()
            .into_semigroup()
            .and_then_deferred(|gating| {
                gating
                    .as_ref()
                    .map_or(LogResult::new_ok_default(), |g| {
                        g.region_indices()
                            .into_iter()
                            .map(|ri| {
                                Region::lookup(std, nonstd, ri, conf)
                                    .map_deferred_value(|(r, r_diag)| (r.map(|x| (ri, x)), r_diag))
                                    .map_errors(LookupGatingSchemeError::Region)
                                    .map_commutative_warnings(LookupGatingSchemeError::Region)
                            })
                            .sequence_def()
                    })
                    .and_then_deferred_switchable_result(flag, |rs| {
                        let mut regions = HashMap::new();
                        let mut trimmed = vec![];
                        for (r, t) in rs {
                            let _ = r.map(|(k, v)| regions.insert(k, v));
                            trimmed.extend(t);
                        }
                        Self::try_new(gating, regions)
                            .map_err(LookupGatingSchemeError::Link)
                            .map(|x| (x, trimmed))
                    })
            })
    }

    fn demote_keywords(self, nonstd: &mut NonStdKeywords)
    where
        I: Copy,
        for<'a> RegionKeyword<'a>: From<SplitKeyword1<RegionGateIndex<I>>>,
    {
        for (ri, r) in self.regions {
            r.demote_keywords(ri, nonstd);
        }
        let g = self
            .gating
            .as_ref()
            .map(OptRootKeyword::from_ref)
            .map(Into::into);
        nonstd.insert_demoted_keyword_opt(g);
    }

    pub(crate) fn opt_keywords<'a>(&'a self) -> impl Iterator<Item = OptRootKeyword<'a>>
    where
        I: Copy,
        RegionKeyword<'a>: From<SplitKeyword1<RegionGateIndex<I>>>,
    {
        let gating = self.gating.as_ref().map(OptRootKeyword::from_ref);
        self.regions
            .iter()
            .flat_map(|(ri, r)| r.opt_keywords(*ri))
            .map(OptRootKeyword::from)
            .chain(gating)
    }

    fn convert_indices<J0, J1, const GATE_IS_INDEX: bool>(
        self,
        flag: AllowLoss,
    ) -> DeferredSwitchableErrors<GatingScheme<J0>, AllowLoss, ConvertSchemeError<J1, GATE_IS_INDEX>>
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

impl<I> Region<I> {
    pub(crate) fn try_new(
        r_index: RegionGateIndex<I>,
        window: RegionWindow,
    ) -> Result<Self, (RegionGateIndex<I>, RegionWindow)> {
        match (r_index, window) {
            (RegionGateIndex::Univariate(index), RegionWindow::Univariate(gate)) => {
                Ok(Self::Univariate(UnivariateRegion { gate, index }))
            }
            (RegionGateIndex::Bivariate(index), RegionWindow::Bivariate(vs)) => {
                Ok(Self::Bivariate(BivariateRegion {
                    index,
                    vertices: vs.into(),
                }))
            }
            (r, w) => Err((r, w)),
        }
    }

    #[allow(clippy::type_complexity)]
    fn lookup<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        ri: RegionIndex,
        conf: &C,
    ) -> DeferredWarningsAndErrors<
        (Option<Self>, TrimmedKeywords),
        LookupRegionError<LookupRegionIndexError<I>>,
        LookupRegionError<LookupRegionIndexError<I>>,
    >
    where
        I: FromStr + LinkedMeasIndex + PartialEq,
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadStdKeywordsConfig>,
        for<'a> RegionKeyword<'a>: From<SplitKeyword1<RegionGateIndex<I>>>,
        RegionGateIndex<I>: OptIndexedKey + Optional<Outer = Option<RegionGateIndex<I>>>,
    {
        let index_res = RegionGateIndex::remove_or_drop_meas_opt_with(std, nonstd, ri, (), conf)
            .map_switchable_errors(LookupRegionError::Region)
            .switchable_into_commutative()
            .into_semigroup();
        let window_res = RegionWindow::remove_or_drop_meas_opt_with(std, nonstd, ri, (), conf)
            .map_switchable_errors(LookupRegionError::Window)
            .switchable_into_commutative()
            .into_semigroup();
        let rconf: &ReadDataKeywordsConfig = conf.as_ref();
        let flag = rconf.process_optional_failure;
        let demote_index = |gi, ns: &mut NonStdKeywords| {
            let k = OptRootKeyword::from(RegionKeyword::from_value(gi, ri)).into();
            ns.insert_demoted_keyword(k);
        };
        let demote_window = |w: RegionWindow, ns: &mut NonStdKeywords| {
            let k = DKey1::<RegionWindow>::new_i1(ri).as_std_key();
            let v = ToNE(w).to_ne_string();
            ns.insert_demoted(k, v);
        };
        index_res
            .zip_f2_once(window_res)
            .and_then_deferred_switchable_result(flag, |(gi_out, w_out)| {
                // Try to combine the gateindex and window together to make a
                // region. This will only work if both are present and
                // they are both the same type (uni/bi-variate). If anything
                // fails, return none, log an error (or warning if we allow
                // dropping), and demote the keywords if applicable.
                let (gi_val, gi_trimmed) = gi_out.into_opt_indexed_pair(ri.into());
                let (w_val, w_trimmed) = w_out.into_opt_indexed_pair(ri.into());
                let trimmed = gi_trimmed.into_iter().chain(w_trimmed).collect();
                let res = match (gi_val, w_val) {
                    (Some(gi), Some(w)) => match Self::try_new(gi, w) {
                        Ok(x) => Ok(Some(x.fmap_into())),
                        Err((old_gi, old_w)) => {
                            if flag.is_demote() {
                                demote_index(old_gi, nonstd);
                                demote_window(old_w, nonstd);
                            }
                            Err(IndexWindowMismatchError::Both(ri))
                        }
                    },
                    (Some(old_gi), None) => {
                        if flag.is_demote() {
                            demote_index(old_gi, nonstd);
                        }
                        Err(IndexWindowMismatchError::NoWindow(ri))
                    }
                    (None, Some(old_w)) => {
                        if flag.is_demote() {
                            demote_window(old_w, nonstd);
                        }
                        Err(IndexWindowMismatchError::NoIndex(ri))
                    }
                    (None, None) => Ok(None),
                };
                res.map_err(LookupRegionError::Mismatch)
                    .map(|x| (x, trimmed))
            })
    }

    pub(crate) fn demote_keywords<'a>(&'a self, i: RegionIndex, nonstd: &mut NonStdKeywords)
    where
        I: Copy,
        RegionKeyword<'a>: From<SplitKeyword1<RegionGateIndex<I>>>,
    {
        for r in self.opt_keywords(i) {
            let kw = OptRootKeyword::from(r).into();
            nonstd.insert_demoted_keyword(kw);
        }
    }

    pub(crate) fn opt_keywords<'a>(&'a self, i: RegionIndex) -> [RegionKeyword<'a>; 2]
    where
        I: Copy,
        RegionKeyword<'a>: From<SplitKeyword1<RegionGateIndex<I>>>,
    {
        let ri = match self {
            Self::Univariate(r) => RegionGateIndex::Univariate(r.index),
            Self::Bivariate(r) => RegionGateIndex::Bivariate(r.index),
        };
        let rw = match self {
            Self::Univariate(r) => RegionWindowRef::Univariate(&r.gate),
            Self::Bivariate(r) => RegionWindowRef::Bivariate(r.vertices.0.as_nonempty_slice()),
        };
        let x0 = RegionKeyword::from_value(ri, i);
        let rk = DKey1::new_i1(i);
        let x1 = RegionKeyword::Window(SplitKeyword::new(rk, rw));
        [x0, x1]
    }

    fn try_index_into<J0, J1>(self) -> Result<Region<J0>, AnyIndexForRegionError<J1>>
    where
        J0: TryFrom<I, Error = UniIndexForRegionError<J1>>,
        AnyIndexForRegionError<J1>: From<J0::Error> + From<BiIndexForRegionError<J1>>,
        I: Copy,
    {
        match self {
            Self::Univariate(x) => Ok(Region::Univariate(x.try_map(TryFrom::try_from)?)),
            Self::Bivariate(x) => Ok(Region::Bivariate(
                x.try_map(BiIndexForRegionError::try_from2)?,
            )),
        }
    }

    pub(crate) fn indices(&self) -> OneOrTwo<&I> {
        match self {
            Self::Univariate(r) => OneOrTwo::One(&r.index),
            Self::Bivariate(r) => OneOrTwo::Two(&r.index.x, &r.index.x),
        }
    }

    fn meas_indices(&self) -> impl Iterator<Item = MeasIndex>
    where
        I: LinkedMeasIndex,
    {
        self.indices()
            .fmap(LinkedMeasIndex::meas_index)
            .into_iter()
            .flatten()
    }

    fn shift_after_insert(&mut self, i: MeasIndex)
    where
        I: LinkedMeasIndex,
    {
        let ix = usize::from(i);
        let go = |j: &mut MeasIndex| {
            let jx = usize::from(*j);
            *j = if jx >= ix { jx + 1 } else { jx }.into();
        };
        match self {
            Self::Univariate(r) => r.index.meas_index_mut().map(go),
            Self::Bivariate(r) => {
                r.index.x.meas_index_mut().map(go);
                r.index.y.meas_index_mut().map(go)
            }
        };
    }

    fn shift_after_remove(&mut self, i: MeasIndex)
    where
        I: LinkedMeasIndex,
    {
        let ix = usize::from(i);
        let go = |j: &mut MeasIndex| {
            let jx = usize::from(*j);
            assert!(
                jx != ix,
                "removed index should not have any regions pointing to it"
            );
            *j = if jx > ix { jx - 1 } else { jx }.into();
        };
        match self {
            Self::Univariate(r) => r.index.meas_index_mut().map(go),
            Self::Bivariate(r) => {
                r.index.x.meas_index_mut().map(go);
                r.index.y.meas_index_mut().map(go)
            }
        };
    }
}

impl TryFrom<MeasOrGateIndex> for PrefixedMeasIndex {
    type Error = UniIndexForRegionError<GateIndex>;
    fn try_from(value: MeasOrGateIndex) -> Result<Self, Self::Error> {
        match value {
            MeasOrGateIndex::Meas(i) => Ok(i.into()),
            MeasOrGateIndex::Gate(i) => Err(UniIndexForRegionError(i)),
        }
    }
}

impl From<PrefixedMeasIndex> for MeasOrGateIndex {
    fn from(value: PrefixedMeasIndex) -> Self {
        Self::Meas(value.0)
    }
}

impl TryFrom<MeasOrGateIndex> for GateIndex {
    type Error = UniIndexForRegionError<MeasIndex>;
    fn try_from(value: MeasOrGateIndex) -> Result<Self, Self::Error> {
        match value {
            MeasOrGateIndex::Gate(i) => Ok(i),
            MeasOrGateIndex::Meas(i) => Err(UniIndexForRegionError(i)),
        }
    }
}

impl GatedMeasurements {
    pub(crate) fn gate(&self) -> Option<Gate> {
        if self.0.is_empty() {
            None
        } else {
            Some(Gate(self.0.len()))
        }
    }

    fn demote_keywords(self, nonstd: &mut NonStdKeywords) {
        let gate = self.gate().map(OptRootKeyword::from_value).map(Into::into);
        nonstd.insert_demoted_keyword_opt(gate);
        for (i, g) in self.0.into_iter().enumerate() {
            g.demote_keywords(i.into(), nonstd);
        }
    }

    fn lookup<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        conf: &C,
    ) -> DeferredWarningsAndErrors<
        (Self, Vec<ScaleFix>),
        LookupGatedMeasurementsError,
        LookupGatedMeasurementsError,
    >
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadStdKeywordsConfig>,
    {
        Gate::remove_or_drop_root_opt(std, nonstd, conf.as_ref())
            .map_switchable_errors(LookupGatedMeasurementsError::Gate)
            .switchable_into_commutative()
            .into_semigroup()
            .and_then_deferred(|maybe| {
                if let Some(n) = maybe {
                    (0..n.0)
                        .map(|i| {
                            GatedMeasurement::lookup(std, nonstd, i.into(), conf)
                                .map_commutative_warnings(LookupGatedMeasurementsError::Meas)
                                .map_errors(LookupGatedMeasurementsError::Meas)
                        })
                        .sequence_def()
                        .map_deferred_value(|xs| {
                            let (gs, ds) = xs.into_iter().unzip();
                            (Self(gs), ds)
                        })
                } else {
                    LogResult::new_ok_default()
                }
            })
    }
}

impl From<AppliedGates2_0> for AppliedGates3_0 {
    fn from(value: AppliedGates2_0) -> Self {
        Self {
            gated_measurements: value.gated_measurements,
            scheme: value.scheme.fmap_into(),
        }
    }
}

impl From<AppliedGates3_2> for AppliedGates3_0 {
    fn from(value: AppliedGates3_2) -> Self {
        Self {
            gated_measurements: vec![].into(),
            scheme: value.0.fmap_into(),
        }
    }
}

/// Error when building new applied gates object with both scheme and gated measurements.
///
/// This only applies to 2.0/3.0/3.1
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum NewAppliedGatesWithSchemeError {
    Link(GateMeasurementLinkError),
    Scheme(DependentKeyError<Gating>),
}

/// Error when converting gating keywords from 3.0/3.1 to 2.0
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum AppliedGates3_0To2_0Error {
    Scheme(ConvertSchemeError<MeasIndex, false>),
    Link(GateMeasurementLinkError),
}

/// Error when converting gating keywords from 3.0/3.1 to 3.2
pub type AppliedGates3_0To3_2Error = ConvertSchemeError<GateIndex, true>;

/// Error when converting $GATING/$RnI/$RnW keywords to new version.
///
/// $RnI can fail because it may contain indices that refer to something
/// that is unsupported in the target version.
///
/// $GATING can fail because it may refer to $RnI/$RnW keywords which are
/// no longer valid as described above.
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(I: Into<IndexFromOne> + Copy))]
pub enum ConvertSchemeError<I, const INDEX_IS_GATE: bool> {
    Region(ConvertIndexForRegionError<I, INDEX_IS_GATE>),
    Scheme(DependentKeyError<Gating>),
}

/// Error when converting 3.0./3.1 $RnI keywords to either 2.0 or 3.2
///
/// In 3.0/3.1, these can point to either gates or measurements. In either
/// target version they can only point to one, in which case any with the other
/// should lead to this error.
#[derive(Debug, Display, Error)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConversionError))]
#[cfg_attr(feature = "python", bound(I: Into<IndexFromOne> + Copy))]
pub struct ConvertIndexForRegionError<I, const INDEX_IS_GATE: bool>(
    IndexedError<AnyIndexForRegionError<I>>,
);

impl<I: Into<IndexFromOne> + Copy, const INDEX_IS_GATE: bool> fmt::Display
    for ConvertIndexForRegionError<I, INDEX_IS_GATE>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let region_key = RegionGateIndex::<()>::std(self.0.index);
        let keys = |i: I, is_plural: bool, is_gate: bool| {
            let prefix = if is_gate { "G" } else { "P" };
            let key = format!("{prefix}{}*", i.into());
            if is_plural {
                format!("{key} keywords")
            } else {
                format!("a {key} keyword")
            }
        };
        let target_keys = |i: I, is_plural: bool| {
            let meas = keys(i, is_plural, false);
            let gate = keys(i, is_plural, true);
            if INDEX_IS_GATE {
                (gate, meas)
            } else {
                (meas, gate)
            }
        };
        let (which, (from, to)) = match &self.0.error {
            AnyIndexForRegionError::Univariate(UniIndexForRegionError(i)) => {
                ("index", target_keys(*i, false))
            }
            AnyIndexForRegionError::Bivariate(b) => match b {
                BiIndexForRegionError::LeftBivariate(i) => ("left index", target_keys(*i, true)),
                BiIndexForRegionError::RightBivariate(i) => ("right index", target_keys(*i, true)),
                BiIndexForRegionError::Bivariate(i0, i1) => {
                    let (from0, to0) = target_keys(*i0, true);
                    let (from1, to1) = target_keys(*i1, true);
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

/// Error when converting between region index types
#[derive(From, Debug)]
enum AnyIndexForRegionError<I> {
    Univariate(UniIndexForRegionError<I>),
    Bivariate(BiIndexForRegionError<I>),
}

/// Error when converting between region index types (bivariate)
#[derive(Debug)]
enum BiIndexForRegionError<I> {
    LeftBivariate(I),
    RightBivariate(I),
    Bivariate(I, I),
}

/// Error when converting between region index types (univariate)
#[derive(Debug, Display)]
pub struct UniIndexForRegionError<I>(I);

impl<J1> BiIndexForRegionError<J1> {
    fn try_from2<I, J0>(x0: I, x1: I) -> Result<(J0, J0), Self>
    where
        I: Copy,
        J0: TryFrom<I, Error = UniIndexForRegionError<J1>>,
    {
        match (J0::try_from(x0), J0::try_from(x1)) {
            (Ok(y0), Ok(y1)) => Ok((y0, y1)),
            (Err(y0), Ok(_)) => Err(Self::LeftBivariate(y0.0)),
            (Ok(_), Err(y1)) => Err(Self::RightBivariate(y1.0)),
            (Err(y0), Err(y1)) => Err(Self::Bivariate(y0.0, y1.0)),
        }
    }
}

/// Error when parsing $GATING/$RnI/$RnW/$Gn*/$GATE keywords for 2.0-3.2
pub type LookupAppliedGatesPre3_2Error<I> = LookupAppliedGatesError<LookupRegionIndexError<I>>;

/// Error when parsing $GATING/$RnI/$RnW/$Gn*/$GATE keywords for 2.0
pub type LookupAppliedGates2_0Error = LookupAppliedGatesPre3_2Error<GateIndex>;

/// Error when parsing $GATING/$RnI/$RnW/$Gn*/$GATE keywords for 3.0 and 3.1
pub type LookupAppliedGates3_0Error = LookupAppliedGatesPre3_2Error<MeasOrGateIndex>;

/// Error when parsing $GATING/$RnI/$RnW keywords for 3.2
pub type LookupAppliedGates3_2Error =
    LookupGatingSchemeError<LookupRegionIndexError<PrefixedMeasIndex>>;

/// Error when parsing $RnI keyword (generic)
pub type LookupRegionIndexError<I> = OptIndexedKeyStError<RegionGateIndex<I>>;

/// Error when parsing $GATING/$RnI/$RnW/$Gn*/$GATE keywords
#[derive(Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr), bound(E: Into<Self>))]
pub enum LookupAppliedGatesError<E> {
    Scheme(LookupGatingSchemeError<E>),
    GatedMeas(LookupGatedMeasurementsError),
    Link(GateMeasurementLinkError),
}

/// Error when $RnI keywords reference nonexistent $Gn* keywords
#[derive(Debug, Error)]
#[error("$RnI keywords reference nonexistent $Gn* indices: {}", .0.iter().join(","))]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct GateMeasurementLinkError(NEVec<GateIndex>);

/// Error when parsing $GATING/$RnI/$RnW keywords
#[derive(Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(E: Into<Self>))]
pub enum LookupGatingSchemeError<E> {
    Link(DependentKeyError<Gating>),
    Gating(OptKeyError<Gating>),
    Region(LookupRegionError<E>),
}

/// Error when parsing $RnI/$RnW keywords
#[derive(Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(E: Into<Self>))]
pub enum LookupRegionError<E> {
    Mismatch(IndexWindowMismatchError),
    Region(E),
    Window(OptIndexedKeyStError<RegionWindow>),
}

/// Error when $RnI and $RnW keywords mismatch
#[derive(Debug, Error)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub enum IndexWindowMismatchError {
    #[error("values for $R{0}I and $R{0}W must both be univariate or bivariate")]
    Both(RegionIndex),
    #[error("$R{0}I not found when $R{0}W was given")]
    NoIndex(RegionIndex),
    #[error("$R{0}W not found when $R{0}I was given")]
    NoWindow(RegionIndex),
}

/// Error when parsing $Gn* and $GATE keywords
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupGatedMeasurementsError {
    Gate(OptKeyError<Gate>),
    Meas(LookupGatedMeasError),
}

/// Error when parsing $Gn* keywords
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupGatedMeasError {
    Scale(OptIndexedKeyStError<GateScale>),
    Shortname(OptIndexedKeyError<GateShortname>),
    PercentEmitted(OptIndexedKeyError<GatePercentEmitted>),
    Range(OptIndexedKeyError<GateRange>),
    DetectorVoltage(OptIndexedKeyError<GateDetectorVoltage>),
}

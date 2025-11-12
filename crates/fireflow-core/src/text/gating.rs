use crate::config::{AllowLoss, AllowOptionalDropping, ConfigFlag as _, StdTextReadConfig};
use crate::core::{RemovedGateLink, RemovedGating, RemovedLink};
use crate::logging::{
    DeferredIter as _, DeferredWarningsAndErrors, FungibleErrorsResult, LogResult, ResultExt as _,
};
use crate::nonempty::FCSNonEmpty;
use crate::type_families::ApplyOnce as _;
use crate::validated::keys::{
    IndexedKey as _, NonStdKeywords, NonStdKeywordsExt as _, StdKey, StdKeywords,
};

use super::optional::KeywordPairMaybe as _;
use super::parser::{
    DepGatedMeasRef, DependentKeyError, DeprecatedGatingSchemeRef, DeprecatedStrRef, IndexedDepRef,
    LookupAppliedGates2_0Error, LookupAppliedGates3_0Error, LookupAppliedGates3_2Error,
    LookupAppliedGatesError, LookupGatedMeasError, LookupGatedMeasurementsError,
    LookupGatingSchemeError, LookupRegionError, OptIndexedKey as _, OptIndexedKeyError,
    OptMetarootKey,
};

use super::index::{GateIndex, MeasIndex, RegionIndex};
use super::keywords::{
    Gate, GateDetectorType, GateDetectorVoltage, GateFilter, GateLongname, GatePercentEmitted,
    GateRange, GateScale, GateShortname, Gating, IndexPair, MeasOrGateIndex, PrefixedMeasIndex,
    RegionGateIndex, RegionWindow, UniGate, Vertex,
};

use derive_more::{AsRef, Display, From};
use derive_new::new;
use itertools::Itertools as _;
use nonempty::NonEmpty;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::mem::take;
use std::str::FromStr;
use thiserror::Error;

#[cfg(feature = "serde")]
use serde::Serialize;

/// The $GATING/$RnI/$RnW/$Gn* keywords in a unified bundle (2.0)
///
/// Each region is assumed to point to a member of `gated_measurements`.
#[derive(Clone, PartialEq, Default, AsRef)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct AppliedGates2_0 {
    #[as_ref([GatedMeasurement])]
    gated_measurements: GatedMeasurements,
    #[as_ref(Option<Gating>)]
    #[as_ref(HashMap<RegionIndex, Region2_0>)]
    scheme: GatingScheme<GateIndex>,
}

/// The $GATING/$RnI/$RnW/$Gn* keywords in a unified bundle (3.0-3.1)
///
/// Each region is assumed to point to a member of `gated_measurements` or
/// a measurement in the [`Core`] struct
#[derive(Clone, PartialEq, Default, AsRef)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct AppliedGates3_0 {
    #[as_ref([GatedMeasurement])]
    gated_measurements: GatedMeasurements,
    #[as_ref(Option<Gating>)]
    #[as_ref(HashMap<RegionIndex, Region3_0>)]
    scheme: GatingScheme<MeasOrGateIndex>,
}

/// The $GATING/$RnI/$RnW keywords in a unified bundle (3.2)
///
/// Each region is assumed to point to a measurement in the [`Core`] struct
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
#[derive(Clone, PartialEq, AsRef)]
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

pub type Region2_0 = Region<GateIndex>;
pub type Region3_0 = Region<MeasOrGateIndex>;
pub type Region3_2 = Region<PrefixedMeasIndex>;

/// A univariate region corresponding to an $RnI/$RnW keyword pair
#[derive(Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct UnivariateRegion<I> {
    pub gate: UniGate,
    pub index: I,
}

/// A bivariate region corresponding to an $RnI/$RnW keyword pair
#[derive(Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct BivariateRegion<I> {
    pub vertices: FCSNonEmpty<Vertex>,
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
    ///
    /// Unlike $PnN, this is not validated to be without commas
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

impl<I> UnivariateRegion<I> {
    fn map<F, J>(self, f: F) -> UnivariateRegion<J>
    where
        F: FnOnce(I) -> J,
    {
        UnivariateRegion {
            gate: self.gate,
            index: f(self.index),
        }
    }

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
    fn map<F, J>(self, f: F) -> BivariateRegion<J>
    where
        F: FnMut(I) -> J,
    {
        BivariateRegion {
            vertices: self.vertices,
            index: self.index.map(f),
        }
    }

    fn try_map<F, J, E>(self, f: F) -> Result<BivariateRegion<J>, E>
    where
        F: FnMut(I) -> Result<J, E>,
    {
        Ok(BivariateRegion {
            vertices: self.vertices,
            index: self.index.try_map(f)?,
        })
    }
}

impl AppliedGates2_0 {
    pub fn try_new(
        gated_measurements: Vec<GatedMeasurement>,
        scheme: GatingScheme<GateIndex>,
    ) -> Result<Self, GateMeasurementLinkError> {
        let n = gated_measurements.len();
        if let Some(xs) = NonEmpty::collect(
            scheme
                .regions
                .iter()
                .flat_map(|(_, r)| r.indices())
                .filter(|i| usize::from(*i) >= n),
        ) {
            Err(GateMeasurementLinkError(xs))
        } else {
            Ok(Self {
                gated_measurements: gated_measurements.into(),
                scheme,
            })
        }
    }

    pub fn try_new1(
        gated_measurements: Vec<GatedMeasurement>,
        regions: HashMap<RegionIndex, Region2_0>,
        gating: Option<Gating>,
    ) -> Result<Self, NewAppliedGatesWithSchemeError> {
        let scheme = GatingScheme::try_new(gating, regions)?;
        Ok(Self::try_new(gated_measurements, scheme)?)
    }

    #[must_use]
    pub fn split(
        self,
    ) -> (
        Vec<GatedMeasurement>,
        HashMap<RegionIndex, Region2_0>,
        Option<Gating>,
    ) {
        (
            self.gated_measurements.0,
            self.scheme.regions,
            self.scheme.gating,
        )
    }

    pub(crate) fn is_empty(&self) -> bool {
        // ASSUME if this is empty then the gating regions will also be empty
        // since they will have nothing to refer
        self.gated_measurements.0.is_empty()
    }

    pub(crate) fn lookup(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        conf: &StdTextReadConfig,
    ) -> DeferredWarningsAndErrors<Self, LookupAppliedGates2_0Error, LookupAppliedGates2_0Error>
    {
        let ag = GatingScheme::lookup(std, nonstd, conf)
            .map_errors(LookupAppliedGatesError::Scheme)
            .map_commutative_warnings(LookupAppliedGatesError::Scheme);
        let gm = GatedMeasurements::lookup(std, nonstd, conf)
            .map_errors(LookupAppliedGatesError::GatedMeas)
            .map_commutative_warnings(LookupAppliedGatesError::GatedMeas);
        let flag = conf.allow_optional_dropping;
        ag.zip_f2_once(gm)
            .and_then_def_result(flag, |(scheme, gated_measurements)| {
                Self::try_new(gated_measurements.0, scheme).map_err(LookupAppliedGatesError::Link)
            })
    }

    pub(crate) fn opt_keywords(&self) -> impl Iterator<Item = (String, String)> {
        let gate = Gate(self.gated_measurements.0.len());
        self.gated_measurements
            .0
            .iter()
            .enumerate()
            .flat_map(|(i, m)| m.opt_keywords(i.into()))
            .chain([gate.metaroot_pair()])
            .chain(self.scheme.opt_keywords())
    }
}

impl AppliedGates3_0 {
    pub fn try_new(
        gated_measurements: Vec<GatedMeasurement>,
        scheme: GatingScheme<MeasOrGateIndex>,
    ) -> Result<Self, GateMeasurementLinkError> {
        let n = gated_measurements.len();
        if let Some(xs) = NonEmpty::collect(
            scheme
                .regions
                .iter()
                .flat_map(|(_, r)| r.indices())
                .flat_map(GateIndex::try_from)
                .filter(|&i| usize::from(i) >= n),
        ) {
            Err(GateMeasurementLinkError(xs))
        } else {
            Ok(Self {
                gated_measurements: gated_measurements.into(),
                scheme,
            })
        }
    }

    pub fn try_new1(
        gated_measurements: Vec<GatedMeasurement>,
        regions: HashMap<RegionIndex, Region3_0>,
        gating: Option<Gating>,
    ) -> Result<Self, NewAppliedGatesWithSchemeError> {
        let scheme = GatingScheme::try_new(gating, regions)?;
        Ok(Self::try_new(gated_measurements, scheme)?)
    }

    #[must_use]
    pub fn split(
        self,
    ) -> (
        Vec<GatedMeasurement>,
        HashMap<RegionIndex, Region3_0>,
        Option<Gating>,
    ) {
        (
            self.gated_measurements.0,
            self.scheme.regions,
            self.scheme.gating,
        )
    }

    /// Shift indices when a new measurement is inserted.
    ///
    /// New measurement is assumed to be inserted at `i`. All regions with
    /// measurement indices greater than i will be incremented by one.
    pub(crate) fn shift_meas_indices_after_insert(&mut self, i: MeasIndex) {
        self.scheme.shift_meas_indices_after_insert(i);
    }

    pub(crate) fn indices_difference(
        &self,
        indices: &HashSet<MeasIndex>,
    ) -> impl Iterator<Item = MeasIndex> {
        self.scheme.indices_difference(indices)
    }

    pub(crate) fn remove_invalid_links(
        &mut self,
        indices: &HashSet<MeasIndex>,
    ) -> impl Iterator<Item = RemovedLink> {
        self.scheme.remove_invalid_links(indices)
    }

    pub(crate) fn lookup(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        conf: &StdTextReadConfig,
    ) -> DeferredWarningsAndErrors<Self, LookupAppliedGates3_0Error, LookupAppliedGates3_0Error>
    {
        let s = GatingScheme::lookup(std, nonstd, conf)
            .map_errors(LookupAppliedGatesError::Scheme)
            .map_commutative_warnings(LookupAppliedGatesError::Scheme);
        let ms = GatedMeasurements::lookup(std, nonstd, conf)
            .map_errors(LookupAppliedGatesError::GatedMeas)
            .map_commutative_warnings(LookupAppliedGatesError::GatedMeas);
        s.zip_f2_once(ms)
            .and_then_def(|(scheme, gated_measurements)| {
                Self::try_new(gated_measurements.0, scheme)
                    .map_err(LookupAppliedGatesError::Link)
                    .into_succ()
            })
    }

    pub(crate) fn opt_keywords(&self) -> impl Iterator<Item = (String, String)> {
        let g = self.gated_measurements.0.len();
        let gate = if g == 0 { None } else { Some(Gate(g)) };
        self.gated_measurements
            .0
            .iter()
            .enumerate()
            .flat_map(|(i, m)| m.opt_keywords(i.into()))
            .chain(self.scheme.opt_keywords())
            .chain(gate.map(|x| OptMetarootKey::metaroot_pair(&x)))
    }

    // TODO use flag to control optional dropping
    pub(crate) fn try_into_2_0(
        self,
        flag: AllowLoss,
    ) -> DeferredWarningsAndErrors<
        AppliedGates2_0,
        AppliedGates3_0To2_0Error,
        AppliedGates3_0To2_0Error,
    > {
        let drop_flag = AllowOptionalDropping(true);
        // ASSUME region indices will still be unique in new hash table
        let (regions, es): (HashMap<_, _>, Vec<_>) = self
            .scheme
            .regions
            .into_iter()
            .map(|(ri, r)| r.try_map(TryInto::try_into).map(|x| (ri, x)))
            .partition_result();
        let index_res = FungibleErrorsResult::new_fungible_ok((), flag)
            .extend_deferred_fungible_errors(es.into_iter().map(AppliedGates3_0To2_0Error::Index))
            .map_fungible_errors(AppliedGates3_0To2_0Error::from)
            .fungible_into_commutative();
        let scheme_res = GatingScheme::try_new(self.scheme.gating, regions)
            .into_deferred_fungible::<_, Vec<_>>(drop_flag)
            .map_fungible_errors(AppliedGates3_0To2_0Error::from)
            .fungible_into_commutative();
        index_res
            .lift_f2_once(scheme_res, |(), scheme| scheme)
            .and_then_def_result(drop_flag, |scheme| {
                AppliedGates2_0::try_new(self.gated_measurements.0, scheme)
                    .map_err(AppliedGates3_0To2_0Error::from)
            })
    }

    pub(crate) fn try_into_3_2(
        self,
        flag: AllowLoss,
    ) -> DeferredWarningsAndErrors<
        AppliedGates3_2,
        AppliedGates3_0To3_2Error,
        AppliedGates3_0To3_2Error,
    > {
        let drop_flag = AllowOptionalDropping(true);
        // ASSUME region indices will still be unique in new hash table
        let (regions, es): (HashMap<_, _>, Vec<_>) = self
            .scheme
            .regions
            .into_iter()
            .map(|(ri, r)| r.try_map(TryInto::try_into).map(|x| (ri, x)))
            .partition_result();
        FungibleErrorsResult::new_fungible_ok((), flag)
            .extend_deferred_fungible_errors(es.into_iter().map(AppliedGates3_0To3_2Error::Index))
            .eval_deferred_fungible_error(|()| {
                let n_gates = self.gated_measurements.0.len();
                (n_gates > 0).then_some(AppliedGates3_0To3_2Error::HasGates(n_gates))
            })
            .fungible_into_commutative()
            .and_then_def_result(drop_flag, |()| {
                AppliedGates3_2::try_new(self.scheme.gating, regions)
                    .map_err(AppliedGates3_0To3_2Error::from)
            })
    }

    pub(crate) fn deprecated(&mut self) -> impl Iterator<Item = DepGatedMeasRef<'_>> {
        self.gated_measurements
            .0
            .iter_mut()
            .enumerate()
            .flat_map(|(i, g)| g.deprecated(i.into()))
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

    pub(crate) fn indices_difference(
        &self,
        indices: &HashSet<MeasIndex>,
    ) -> impl Iterator<Item = MeasIndex> {
        self.0.indices_difference(indices)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn lookup(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        conf: &StdTextReadConfig,
    ) -> DeferredWarningsAndErrors<Self, LookupAppliedGates3_2Error, LookupAppliedGates3_2Error>
    {
        GatingScheme::lookup(std, nonstd, conf).map_def_value(Self)
    }

    pub(crate) fn opt_keywords(&self) -> impl Iterator<Item = (String, String)> {
        self.0.opt_keywords()
    }
}

impl GatedMeasurement {
    fn lookup(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        i: GateIndex,
        conf: &StdTextReadConfig,
    ) -> DeferredWarningsAndErrors<Self, LookupGatedMeasError, LookupGatedMeasError> {
        macro_rules! go {
            ($x:expr) => {
                $x.map_fungible_errors(LookupGatedMeasError::from)
                    .fungible_into_commutative()
                    .into_semigroup()
            };
        }
        let scale = GateScale::drop_meas_opt_with(std, nonstd, i, (), conf);
        let filter = GateFilter::remove_meas_opt_nofail(std, i);
        let sname = GateShortname::drop_meas_opt(std, nonstd, i, conf);
        let pemit = GatePercentEmitted::drop_meas_opt(std, nonstd, i, conf);
        let range = GateRange::drop_meas_opt(std, nonstd, i, conf);
        let lname = GateLongname::remove_meas_opt_nofail(std, i);
        let dtype = GateDetectorType::remove_meas_opt_nofail(std, i);
        let dvolt = GateDetectorVoltage::drop_meas_opt(std, nonstd, i, conf);
        go!(scale).lift_f5_once(
            go!(sname),
            go!(pemit),
            go!(range),
            go!(dvolt),
            |s, n, p, r, v| Self::new(s, filter, n, p, r, lname, dtype, v),
        )
    }

    fn deprecated(&mut self, i: GateIndex) -> impl Iterator<Item = DepGatedMeasRef<'_>> {
        let j = i.into();
        macro_rules! go {
            ($j:expr, $x:expr) => {
                DepGatedMeasRef::from(IndexedDepRef::new($j, $x))
            };
        }
        let x0 = go!(j, &mut self.scale);
        let x1 = go!(j, DeprecatedStrRef(&mut self.filter));
        let x2 = go!(j, &mut self.shortname);
        let x3 = go!(j, &mut self.percent_emitted);
        let x4 = go!(j, &mut self.range);
        let x5 = go!(j, DeprecatedStrRef(&mut self.longname));
        let x6 = go!(j, DeprecatedStrRef(&mut self.detector_type));
        let x7 = go!(j, &mut self.detector_voltage);
        [x0, x1, x2, x3, x4, x5, x6, x7].into_iter()
    }

    pub(crate) fn opt_keywords(&self, i: GateIndex) -> impl Iterator<Item = (String, String)> {
        [
            self.scale.meas_opt_pair(i),
            self.filter.meas_opt_pair(i),
            self.shortname.meas_opt_pair(i),
            self.percent_emitted.meas_opt_pair(i),
            self.range.meas_opt_pair(i),
            self.longname.meas_opt_pair(i),
            self.detector_type.meas_opt_pair(i),
            self.detector_voltage.meas_opt_pair(i),
        ]
        .into_iter()
        .filter_map(|(k, v)| v.map(|x| (k, x)))
    }
}

impl<I> Default for GatingScheme<I> {
    fn default() -> Self {
        Self {
            gating: None,
            regions: HashMap::new(),
        }
    }
}

impl<I> GatingScheme<I> {
    pub fn try_new(
        gating: Option<Gating>,
        regions: HashMap<RegionIndex, Region<I>>,
    ) -> Result<Self, DependentKeyError<Gating>> {
        // NOTE generic parameter in RegionGateIndex is a dummy, all should
        // format to the same string
        if let Some(ris) = gating.as_ref().and_then(|g| {
            NonEmpty::collect(
                g.region_indices()
                    .into_iter()
                    .filter(|ri| !regions.contains_key(ri))
                    .map(RegionGateIndex::<GateIndex>::std),
            )
        }) {
            Err(DependentKeyError::new1(ris))
        } else {
            Ok(Self { gating, regions })
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        // ASSUME gating will also be empty since it will have nothing to
        // refer to if this is also empty
        self.regions.is_empty()
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

    fn indices_difference(&self, indices: &HashSet<MeasIndex>) -> impl Iterator<Item = MeasIndex>
    where
        I: LinkedMeasIndex,
    {
        self.meas_indices().filter(|i| !indices.contains(i))
    }

    pub(crate) fn remove_invalid_links(
        &mut self,
        indices: &HashSet<MeasIndex>,
    ) -> impl Iterator<Item = RemovedLink>
    where
        I: LinkedMeasIndex,
        RemovedLink: From<RemovedGateLink<I>>,
    {
        // Check the $GATING keyword to see if it has any links to $RnI/$RnW
        // which in turn reference measurement indices that don't exist. If it
        // has any, rip it out and return it.
        let gating = if let Some(g) = self.gating.as_ref() {
            let xs = g.region_indices();
            let ys = xs.iter().copied().filter(|&rni| {
                self.regions
                    .get(&rni)
                    .into_iter()
                    .any(|rnw| rnw.meas_indices().any(|x| !indices.contains(&x)))
            });
            NonEmpty::collect(ys).map(|zs| {
                // ASSUME this won't fail because we are inside an if let Some
                // block
                let ret = take(&mut self.gating).unwrap();
                RemovedGating::new(zs, ret)
            })
        } else {
            None
        };
        // Then remove any $RnI/$RnW keywords which reference measurements that
        // don't exist.
        self.regions
            .extract_if(|_, rnw| rnw.meas_indices().any(|x| !indices.contains(&x)))
            .map(|(rni, rnw)| {
                let bad_indices = rnw.meas_indices().filter(|x| !indices.contains(x));
                // ASSUME this won't fail because we pre-filtered above
                let js = NonEmpty::collect(bad_indices).unwrap();
                RemovedLink::from(RemovedGateLink::new(rni, rnw, js))
            })
            .chain(gating.map(RemovedLink::Gating))
    }

    fn meas_indices(&self) -> impl Iterator<Item = MeasIndex>
    where
        I: LinkedMeasIndex,
    {
        self.regions.iter().flat_map(|(_, v)| v.meas_indices())
    }

    #[allow(clippy::type_complexity)]
    fn lookup(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        conf: &StdTextReadConfig,
    ) -> DeferredWarningsAndErrors<
        Self,
        LookupGatingSchemeError<
            OptIndexedKeyError<RegionGateIndex<I>>,
            OptIndexedKeyError<RegionWindow>,
        >,
        LookupGatingSchemeError<
            OptIndexedKeyError<RegionGateIndex<I>>,
            OptIndexedKeyError<RegionWindow>,
        >,
    >
    where
        I: FromStr + fmt::Display + LinkedMeasIndex + PartialEq,
    {
        let flag = conf.allow_optional_dropping;
        // TODO demote as necessary
        Gating::drop_metaroot_opt(std, nonstd, conf)
            .map_fungible_errors(LookupGatingSchemeError::Gating)
            .fungible_into_commutative()
            .into_semigroup()
            .and_then_def(|gating| {
                gating
                    .as_ref()
                    .map_or(LogResult::new_ok_default(), |g| {
                        g.region_indices()
                            .into_iter()
                            .map(|ri| {
                                Region::lookup(std, nonstd, ri, conf)
                                    .map_def_value(|x| x.map(|y| (ri, y)))
                                    .map_errors(LookupGatingSchemeError::Region)
                                    .map_commutative_warnings(LookupGatingSchemeError::Region)
                            })
                            .mappend_def()
                    })
                    .and_then_def_result(flag, |rs| {
                        // TODO impl iterator for try_new
                        let regions = rs.into_iter().flatten().collect();
                        Self::try_new(gating, regions).map_err(LookupGatingSchemeError::Link)
                    })
            })
    }

    pub(crate) fn opt_keywords(&self) -> impl Iterator<Item = (String, String)>
    where
        I: fmt::Display + FromStr + Copy,
    {
        self.regions
            .iter()
            .flat_map(|(ri, r)| r.opt_keywords(*ri))
            .chain(self.gating.as_ref().map(OptMetarootKey::metaroot_pair))
    }

    fn inner_into<J: From<I>>(self) -> GatingScheme<J> {
        GatingScheme {
            gating: self.gating,
            regions: self
                .regions
                .into_iter()
                .map(|(ri, r)| (ri, r.inner_into()))
                .collect(),
        }
    }
}

impl GatingScheme<PrefixedMeasIndex> {
    pub(crate) fn deprecated(&mut self) -> impl Iterator<Item = DeprecatedGatingSchemeRef<'_>> {
        let g = DeprecatedGatingSchemeRef::from(&mut self.gating);
        let r = DeprecatedGatingSchemeRef::from(&mut self.regions);
        [g, r].into_iter()
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
    fn lookup(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        ri: RegionIndex,
        conf: &StdTextReadConfig,
    ) -> DeferredWarningsAndErrors<
        Option<Self>,
        LookupRegionError<OptIndexedKeyError<RegionGateIndex<I>>, OptIndexedKeyError<RegionWindow>>,
        LookupRegionError<OptIndexedKeyError<RegionGateIndex<I>>, OptIndexedKeyError<RegionWindow>>,
    >
    where
        I: FromStr + fmt::Display + LinkedMeasIndex + PartialEq,
    {
        let index_res = RegionGateIndex::drop_meas_opt(std, nonstd, ri, conf)
            .map_fungible_errors(LookupRegionError::Region)
            .fungible_into_commutative()
            .into_semigroup();
        let window_res = RegionWindow::drop_meas_opt_with(std, nonstd, ri, (), conf)
            .map_fungible_errors(LookupRegionError::Window)
            .fungible_into_commutative()
            .into_semigroup();
        let flag = conf.allow_optional_dropping;
        index_res
            .zip_f2_once(window_res)
            .and_then_def_result(flag, |(gi_opt, w_opt)| {
                // Try to combine the gateindex and window together to make a
                // region. This will only work if both are present and
                // they are both the same type (uni/bi-variate). If anything
                // fails, return none, log an error (or warning if we allow
                // dropping), and demote the keywords if applicable.
                let res = match (gi_opt, w_opt) {
                    (Some(gi), Some(w)) => match Self::try_new(gi, w) {
                        Ok(x) => Ok(Some(x.inner_into())),
                        Err((gi_, w_)) => {
                            if flag.is_set() {
                                nonstd.insert_demoted_meas(ri.into(), &gi_);
                                nonstd.insert_demoted_meas(ri.into(), &w_);
                            }
                            Err(IndexWindowMismatchError::Both(ri))
                        }
                    },
                    (Some(gi), None) => {
                        if flag.is_set() {
                            nonstd.insert_demoted_meas(ri.into(), &gi);
                        }
                        Err(IndexWindowMismatchError::NoWindow(ri))
                    }
                    (None, Some(w)) => {
                        if flag.is_set() {
                            nonstd.insert_demoted_meas(ri.into(), &w);
                        }
                        Err(IndexWindowMismatchError::NoIndex(ri))
                    }
                    (None, None) => Ok(None),
                };
                res.map_err(LookupRegionError::Mismatch)
            })
    }

    pub(crate) fn opt_keywords_std(&self, i: RegionIndex) -> impl Iterator<Item = (StdKey, String)>
    where
        I: Copy + FromStr + fmt::Display,
    {
        let (ri, rw) = self.split();
        [ri.meas_pair_std(i), rw.meas_pair_std(i)].into_iter()
    }

    pub(crate) fn opt_keywords(&self, i: RegionIndex) -> impl Iterator<Item = (String, String)>
    where
        I: Copy + FromStr + fmt::Display,
    {
        self.opt_keywords_std(i).map(|(k, v)| (k.to_string(), v))
    }

    pub(crate) fn split(&self) -> (RegionGateIndex<I>, RegionWindow)
    where
        I: Copy,
    {
        match self {
            Self::Univariate(r) => (
                RegionGateIndex::Univariate(r.index),
                RegionWindow::Univariate(r.gate.clone()),
            ),
            Self::Bivariate(r) => (
                RegionGateIndex::Bivariate(r.index),
                RegionWindow::Bivariate(r.vertices.clone().into()),
            ),
        }
    }

    pub(crate) fn map<F, J>(self, f: F) -> Region<J>
    where
        F: FnMut(I) -> J,
    {
        match self {
            Self::Univariate(x) => Region::Univariate(x.map(f)),
            Self::Bivariate(x) => Region::Bivariate(x.map(f)),
        }
    }

    pub(crate) fn try_map<F, J, E>(self, f: F) -> Result<Region<J>, E>
    where
        F: FnMut(I) -> Result<J, E>,
    {
        match self {
            Self::Univariate(x) => Ok(Region::Univariate(x.try_map(f)?)),
            Self::Bivariate(x) => Ok(Region::Bivariate(x.try_map(f)?)),
        }
    }

    pub(crate) fn inner_into<J: From<I>>(self) -> Region<J> {
        self.map(Into::into)
    }

    pub(crate) fn indices(&self) -> NonEmpty<I>
    where
        I: Copy,
    {
        match self {
            Self::Univariate(r) => NonEmpty::new(r.index),
            Self::Bivariate(r) => (r.index.x, vec![r.index.x]).into(),
        }
    }

    fn meas_indices(&self) -> impl Iterator<Item = MeasIndex>
    where
        I: LinkedMeasIndex,
    {
        match self {
            Self::Univariate(r) => r.index.meas_index().into_iter().chain(None),
            Self::Bivariate(r) => {
                let i = &r.index;
                i.x.meas_index().into_iter().chain(i.y.meas_index())
            }
        }
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
}

impl TryFrom<MeasOrGateIndex> for PrefixedMeasIndex {
    type Error = RegionToMeasIndexError;
    fn try_from(value: MeasOrGateIndex) -> Result<Self, Self::Error> {
        match value {
            MeasOrGateIndex::Meas(i) => Ok(i.into()),
            MeasOrGateIndex::Gate(i) => Err(RegionToMeasIndexError(i)),
        }
    }
}

impl From<PrefixedMeasIndex> for MeasOrGateIndex {
    fn from(value: PrefixedMeasIndex) -> Self {
        Self::Meas(value.0)
    }
}

impl TryFrom<MeasOrGateIndex> for GateIndex {
    type Error = RegionToGateIndexError;
    fn try_from(value: MeasOrGateIndex) -> Result<Self, Self::Error> {
        match value {
            MeasOrGateIndex::Gate(i) => Ok(i),
            MeasOrGateIndex::Meas(i) => Err(RegionToGateIndexError(i)),
        }
    }
}

impl TryFrom<GateIndex> for PrefixedMeasIndex {
    type Error = GateToMeasIndexError;
    fn try_from(value: GateIndex) -> Result<Self, Self::Error> {
        Err(GateToMeasIndexError(value))
    }
}

impl TryFrom<PrefixedMeasIndex> for GateIndex {
    type Error = MeasToGateIndexError;
    fn try_from(value: PrefixedMeasIndex) -> Result<Self, Self::Error> {
        Err(MeasToGateIndexError(value))
    }
}

impl GatedMeasurements {
    fn lookup(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        conf: &StdTextReadConfig,
    ) -> DeferredWarningsAndErrors<Self, LookupGatedMeasurementsError, LookupGatedMeasurementsError>
    {
        Gate::drop_metaroot_opt(std, nonstd, conf)
            .map_fungible_errors(LookupGatedMeasurementsError::Gate)
            .fungible_into_commutative()
            .into_semigroup()
            .and_then_def(|maybe| {
                if let Some(n) = maybe {
                    (0..n.0)
                        .map(|i| {
                            GatedMeasurement::lookup(std, nonstd, i.into(), conf)
                                .map_commutative_warnings(LookupGatedMeasurementsError::Meas)
                                .map_errors(LookupGatedMeasurementsError::Meas)
                        })
                        .mappend_def()
                        .map_def_value(Self)
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
            scheme: value.scheme.inner_into(),
        }
    }
}

impl From<AppliedGates3_2> for AppliedGates3_0 {
    fn from(value: AppliedGates3_2) -> Self {
        Self {
            gated_measurements: vec![].into(),
            scheme: value.0.inner_into(),
        }
    }
}

#[derive(Debug, Error)]
#[error(
    "cannot convert region index ({0}) to measurement \
     index since it refers to a gate"
)]
pub struct RegionToMeasIndexError(GateIndex);

#[derive(Debug, Error)]
#[error(
    "cannot convert region index ({0}) to gating index since \
     it refers to a measurement"
)]
pub struct RegionToGateIndexError(MeasIndex);

#[derive(Debug, Error)]
#[error("cannot convert gate index ({0}) to measurement index")]
pub struct GateToMeasIndexError(GateIndex);

#[derive(Debug, Error)]
#[error("cannot convert measurement index ({0}) to gate index")]
pub struct MeasToGateIndexError(PrefixedMeasIndex);

#[derive(Debug, Error)]
#[error("$RnI regions reference nonexistent gates: {}", .0.iter().join(","))]
pub struct GateMeasurementLinkError(NonEmpty<GateIndex>);

#[derive(From, Display, Debug, Error)]
pub enum NewAppliedGatesWithSchemeError {
    Link(GateMeasurementLinkError),
    Scheme(DependentKeyError<Gating>),
}

#[derive(From, Display, Debug, Error)]
pub enum AppliedGates3_0To2_0Error {
    Index(RegionToGateIndexError),
    Scheme(DependentKeyError<Gating>),
    Link(GateMeasurementLinkError),
}

#[derive(Debug, Error)]
pub enum AppliedGates3_0To3_2Error {
    #[error("{0}")]
    Index(#[from] RegionToMeasIndexError),
    #[error("$GATING references {0} $Gn* keywords")]
    HasGates(usize),
    #[error("{0}")]
    Scheme(#[from] DependentKeyError<Gating>),
}

#[derive(Debug, Error)]
#[error("cannot convert 2.0 $GATING/$Gn*/$RnI/$RnW keywords to 3.2")]
pub struct AppliedGates2_0To3_2Error;

#[derive(Debug, Error)]
#[error("cannot convert 3.2 $GATING/$RnI/$RnW keywords to 2.0")]
pub struct AppliedGates3_2To2_0Error;

#[derive(Debug, Error)]
pub enum IndexWindowMismatchError {
    #[error("values for $R{0}I and $R{0}W must both be univariate or bivariate")]
    Both(RegionIndex),
    #[error("$R{0}I not found when $R{0}W was given")]
    NoIndex(RegionIndex),
    #[error("$R{0}W not found when $R{0}I was given")]
    NoWindow(RegionIndex),
}

#[cfg(feature = "python")]
mod python {
    use crate::python::macros::{
        impl_from_py_via_fromstr, impl_from_pyerr, impl_pyreflow_err, impl_to_py_via_display,
        impl_value_err,
    };
    use crate::text::keywords::{Gating, GatingError, MeasOrGateIndex, MeasOrGateIndexError};
    use crate::text::parser::{
        LookupAppliedGatesError, LookupGatedMeasError, LookupGatedMeasurementsError,
        LookupGatingSchemeError, LookupRegionError,
    };

    use super::{
        GateMeasurementLinkError, IndexWindowMismatchError, NewAppliedGatesWithSchemeError,
    };

    use pyo3::prelude::*;

    impl_from_py_via_fromstr!(Gating);
    impl_to_py_via_display!(Gating);

    impl_from_py_via_fromstr!(MeasOrGateIndex);
    impl_to_py_via_display!(MeasOrGateIndex);

    impl_value_err!(GatingError);
    impl_value_err!(MeasOrGateIndexError);

    impl_pyreflow_err!(RelationalException, GateMeasurementLinkError);
    impl_pyreflow_err!(RelationalException, IndexWindowMismatchError);

    impl_from_pyerr!(NewAppliedGatesWithSchemeError, Link, Scheme);

    impl<E0, E1> From<LookupGatingSchemeError<E0, E1>> for PyErr
    where
        LookupRegionError<E0, E1>: Into<Self>,
    {
        fn from(value: LookupGatingSchemeError<E0, E1>) -> Self {
            match value {
                LookupGatingSchemeError::Link(x) => x.into(),
                LookupGatingSchemeError::Gating(x) => x.into(),
                LookupGatingSchemeError::Region(x) => x.into(),
            }
        }
    }

    impl<E0, E1> From<LookupRegionError<E0, E1>> for PyErr
    where
        E0: Into<Self>,
        E1: Into<Self>,
    {
        fn from(value: LookupRegionError<E0, E1>) -> Self {
            match value {
                LookupRegionError::Mismatch(x) => x.into(),
                LookupRegionError::Region(x) => x.into(),
                LookupRegionError::Window(x) => x.into(),
            }
        }
    }

    impl<E0> From<LookupAppliedGatesError<E0>> for PyErr
    where
        E0: Into<Self>,
    {
        fn from(value: LookupAppliedGatesError<E0>) -> Self {
            match value {
                LookupAppliedGatesError::Scheme(x) => x.into(),
                LookupAppliedGatesError::GatedMeas(x) => x.into(),
                LookupAppliedGatesError::Link(x) => x.into(),
            }
        }
    }

    impl_from_pyerr!(LookupGatedMeasurementsError, Gate, Meas);
    impl_from_pyerr!(
        LookupGatedMeasError,
        Scale,
        Shortname,
        PercentEmitted,
        Range,
        DetectorVoltage
    );
}

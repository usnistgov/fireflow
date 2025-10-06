use crate::config::StdTextReadConfig;
use crate::error::{
    BiDeferredResult, DeferredExt as _, PassthruExt as _, ResultExt as _, Tentative,
};
use crate::nonempty::FCSNonEmpty;
use crate::text::index::{GateIndex, IndexFromOne, MeasIndex, RegionIndex};
use crate::text::keywords::{
    Gate, GateDetectorType, GateDetectorVoltage, GateFilter, GateLongname, GatePercentEmitted,
    GateRange, GateScale, GateShortname, Gating, IndexPair, MeasOrGateIndex, Par,
    PrefixedMeasIndex, RegionGateIndex, RegionWindow, UniGate, Vertex,
};
use crate::text::optional::MaybeValue;
use crate::text::parser::{
    LookupOptional, LookupTentative, OptIndexedKey as _, OptMetarootKey, ParseOptKeyError,
};
use crate::validated::keys::StdKeywords;

use derive_more::{AsRef, Display, From};
use derive_new::new;
use itertools::Itertools as _;
use nonempty::NonEmpty;
use std::collections::{HashMap, HashSet};
use std::fmt;
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
    pub scale: MaybeValue<GateScale>,

    /// Value for $GmF
    #[new(into)]
    pub filter: MaybeValue<GateFilter>,

    /// Value for $GmN
    ///
    /// Unlike $PnN, this is not validated to be without commas
    #[new(into)]
    pub shortname: MaybeValue<GateShortname>,

    /// Value for $GmP
    #[new(into)]
    pub percent_emitted: MaybeValue<GatePercentEmitted>,

    /// Value for $GmR
    #[new(into)]
    pub range: MaybeValue<GateRange>,

    /// Value for $GmS
    #[new(into)]
    pub longname: MaybeValue<GateLongname>,

    /// Value for $GmT
    #[new(into)]
    pub detector_type: MaybeValue<GateDetectorType>,

    /// Value for $GmV
    #[new(into)]
    pub detector_voltage: MaybeValue<GateDetectorVoltage>,
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
        kws: &mut StdKeywords,
        par: Par,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<Self> {
        let ag = GatingScheme::lookup(
            kws,
            |k| Gating::lookup_opt(k, conf),
            |k, j| Region::lookup(k, j, par, conf),
            conf,
        );
        let gm = GatedMeasurements::lookup(kws, conf);
        ag.zip(gm).and_tentatively(|(scheme, gated_measurements)| {
            Self::try_new(gated_measurements.0, scheme)
                .into_tentative_def(!conf.allow_optional_dropping)
                .inner_into()
        })
    }

    pub(crate) fn opt_keywords(&self) -> impl Iterator<Item = (String, String)> {
        let gate = Gate(self.gated_measurements.0.len());
        self.gated_measurements
            .0
            .iter()
            .enumerate()
            .flat_map(|(i, m)| m.opt_keywords(i.into()))
            .chain([gate.root_pair()])
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

    pub(crate) fn lookup(
        kws: &mut StdKeywords,
        par: Par,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<Self> {
        Self::lookup_inner(
            kws,
            |k| {
                GatingScheme::lookup(
                    k,
                    |kws_| Gating::lookup_opt(kws_, conf),
                    |kk, j| Region::lookup(kk, j, par, conf),
                    conf,
                )
            },
            |k| GatedMeasurements::lookup(k, conf),
        )
    }

    pub(crate) fn lookup_dep(
        kws: &mut StdKeywords,
        par: Par,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<Self> {
        Self::lookup_inner(
            kws,
            |k| {
                GatingScheme::lookup(
                    k,
                    |kk| Gating::lookup_opt_dep(kk, conf),
                    |kk, i| Region::lookup_dep(kk, i, par, conf),
                    conf,
                )
            },
            |k| GatedMeasurements::lookup_dep(k, conf),
        )
    }

    fn lookup_inner<F0, F1>(
        kws: &mut StdKeywords,
        lookup_scheme: F0,
        lookup_meas: F1,
    ) -> LookupTentative<Self>
    where
        F0: FnOnce(&mut StdKeywords) -> LookupTentative<GatingScheme<MeasOrGateIndex>>,
        F1: FnOnce(&mut StdKeywords) -> LookupTentative<GatedMeasurements>,
    {
        let s = lookup_scheme(kws);
        let ms = lookup_meas(kws);
        s.zip(ms).and_tentatively(|(scheme, gated_measurements)| {
            Self::try_new(gated_measurements.0, scheme)
                .into_tentative_warn_def()
                .warnings_into()
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
            .chain(gate.map(|x| OptMetarootKey::root_pair(&x)))
    }

    pub(crate) fn try_into_2_0(
        self,
        allow_loss: bool,
    ) -> BiDeferredResult<AppliedGates2_0, AppliedGates3_0To2_0Error> {
        // ASSUME region indices will still be unique in new hash table
        let (regions, es): (HashMap<_, _>, Vec<_>) = self
            .scheme
            .regions
            .into_iter()
            .map(|(ri, r)| r.try_map(TryInto::try_into).map(|x| (ri, x)))
            .partition_result();
        let mut res = GatingScheme::try_new(self.scheme.gating, regions)
            .into_deferred()
            .def_and_maybe(|scheme| {
                AppliedGates2_0::try_new(self.gated_measurements.0, scheme).into_deferred()
            });
        for e in es {
            res.def_push_error_or_warning(AppliedGates3_0To2_0Error::Index(e), !allow_loss);
        }
        res
    }

    pub(crate) fn try_into_3_2(
        self,
        allow_loss: bool,
    ) -> BiDeferredResult<AppliedGates3_2, AppliedGates3_0To3_2Error> {
        // ASSUME region indices will still be unique in new hash table
        let (regions, es): (HashMap<_, _>, Vec<_>) = self
            .scheme
            .regions
            .into_iter()
            .map(|(ri, r)| r.try_map(TryInto::try_into).map(|x| (ri, x)))
            .partition_result();
        let mut res = AppliedGates3_2::try_new(self.scheme.gating, regions).into_deferred();
        for e in es {
            res.def_push_error_or_warning(AppliedGates3_0To3_2Error::Index(e), !allow_loss);
        }
        let n_gates = self.gated_measurements.0.len();
        if n_gates > 0 {
            res.def_push_error_or_warning(
                AppliedGates3_0To3_2Error::HasGates(n_gates),
                !allow_loss,
            );
        }
        res
    }
}

impl AppliedGates3_2 {
    pub fn try_new(
        gating: Option<Gating>,
        regions: HashMap<RegionIndex, Region<PrefixedMeasIndex>>,
    ) -> Result<Self, NewGatingSchemeError> {
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
        kws: &mut StdKeywords,
        par: Par,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<Self> {
        GatingScheme::lookup(
            kws,
            |k| Gating::lookup_opt_dep(k, conf),
            |k, i| Region::lookup_dep(k, i, par, conf),
            conf,
        )
        .map(Self)
    }

    pub(crate) fn opt_keywords(&self) -> impl Iterator<Item = (String, String)> {
        self.0.opt_keywords()
    }
}

impl GatedMeasurement {
    fn lookup(
        kws: &mut StdKeywords,
        i: GateIndex,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<Self> {
        Self::lookup_inner(
            kws,
            i,
            |k, j| GateScale::lookup_opt_st(k, j, (), conf),
            |k, j| GateFilter::lookup_opt(k, j, conf),
            |k, j| GateShortname::lookup_opt(k, j, conf),
            |k, j| GatePercentEmitted::lookup_opt(k, j, conf),
            |k, j| GateRange::lookup_opt(k, j, conf),
            |k, j| GateLongname::lookup_opt(k, j, conf),
            |k, j| GateDetectorType::lookup_opt(k, j, conf),
            |k, j| GateDetectorVoltage::lookup_opt(k, j, conf),
        )
    }

    fn lookup_dep(
        kws: &mut StdKeywords,
        i: GateIndex,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<Self> {
        Self::lookup_inner(
            kws,
            i,
            |k, j| GateScale::lookup_opt_st_dep(k, j, (), conf),
            |k, j| GateFilter::lookup_opt_dep(k, j, conf),
            |k, j| GateShortname::lookup_opt_dep(k, j, conf),
            |k, j| GatePercentEmitted::lookup_opt_dep(k, j, conf),
            |k, j| GateRange::lookup_opt_dep(k, j, conf),
            |k, j| GateLongname::lookup_opt_dep(k, j, conf),
            |k, j| GateDetectorType::lookup_opt_dep(k, j, conf),
            |k, j| GateDetectorVoltage::lookup_opt_dep(k, j, conf),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn lookup_inner<F0, F1, F2, F3, F4, F5, F6, F7>(
        kws: &mut StdKeywords,
        i: GateIndex,
        lookup_scale: F0,
        lookup_filter: F1,
        lookup_shortname: F2,
        lookup_pe: F3,
        lookup_range: F4,
        lookup_longname: F5,
        lookup_det_type: F6,
        lookup_det_volt: F7,
    ) -> LookupTentative<Self>
    where
        F0: FnOnce(&mut StdKeywords, GateIndex) -> LookupOptional<GateScale>,
        F1: FnOnce(&mut StdKeywords, GateIndex) -> LookupOptional<GateFilter>,
        F2: FnOnce(&mut StdKeywords, GateIndex) -> LookupOptional<GateShortname>,
        F3: FnOnce(&mut StdKeywords, GateIndex) -> LookupOptional<GatePercentEmitted>,
        F4: FnOnce(&mut StdKeywords, GateIndex) -> LookupOptional<GateRange>,
        F5: FnOnce(&mut StdKeywords, GateIndex) -> LookupOptional<GateLongname>,
        F6: FnOnce(&mut StdKeywords, GateIndex) -> LookupOptional<GateDetectorType>,
        F7: FnOnce(&mut StdKeywords, GateIndex) -> LookupOptional<GateDetectorVoltage>,
    {
        let scale = lookup_scale(kws, i);
        let filter = lookup_filter(kws, i);
        let shortname = lookup_shortname(kws, i);
        let perc_emit = lookup_pe(kws, i);
        let rng = lookup_range(kws, i);
        let longname = lookup_longname(kws, i);
        let det_type = lookup_det_type(kws, i);
        let det_volt = lookup_det_volt(kws, i);
        scale
            .zip4(filter, shortname, perc_emit)
            .zip5(rng, longname, det_type, det_volt)
            .map(|((e, f, n, p), r, s, t, v)| Self::new(e, f, n, p, r, s, t, v))
    }

    pub(crate) fn opt_keywords(&self, i: GateIndex) -> impl Iterator<Item = (String, String)> {
        [
            self.scale.meas_kw_pair(i),
            self.filter.meas_kw_pair(i),
            self.shortname.meas_kw_pair(i),
            self.percent_emitted.meas_kw_pair(i),
            self.range.meas_kw_pair(i),
            self.longname.meas_kw_pair(i),
            self.detector_type.meas_kw_pair(i),
            self.detector_voltage.meas_kw_pair(i),
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
    ) -> Result<Self, NewGatingSchemeError> {
        if let Some(ris) = gating.as_ref().and_then(|g| {
            NonEmpty::collect(
                g.region_indices()
                    .into_iter()
                    .filter(|ri| !regions.contains_key(ri)),
            )
        }) {
            Err(NewGatingSchemeError(ris))
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

    fn meas_indices(&self) -> impl Iterator<Item = MeasIndex>
    where
        I: LinkedMeasIndex,
    {
        self.regions.iter().flat_map(|(_, v)| v.meas_indices())
    }

    fn lookup<F0, F1>(
        kws: &mut StdKeywords,
        lookup_gating: F0,
        lookup_region: F1,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<Self>
    where
        F0: Fn(&mut StdKeywords) -> LookupOptional<Gating>,
        F1: Fn(&mut StdKeywords, RegionIndex) -> LookupOptional<Region<I>>,
    {
        lookup_gating(kws).map(|g| g.0).and_tentatively(|gating| {
            gating
                .as_ref()
                .map(|g| {
                    Tentative::mconcat(
                        g.region_indices()
                            .into_iter()
                            .map(|ri| lookup_region(kws, ri).map(|x| x.0.map(|y| (ri, y)))),
                    )
                })
                .unwrap_or_default()
                .and_tentatively(|rs| {
                    let regions = rs.into_iter().flatten().collect();
                    Self::try_new(gating, regions)
                        .into_tentative_def(!conf.allow_optional_dropping)
                        .inner_into()
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
            .chain(self.gating.as_ref().map(OptMetarootKey::root_pair))
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

impl<I> Region<I> {
    pub(crate) fn try_new(r_index: RegionGateIndex<I>, window: RegionWindow) -> Option<Self> {
        match (r_index, window) {
            (RegionGateIndex::Univariate(index), RegionWindow::Univariate(gate)) => {
                Some(Region::Univariate(UnivariateRegion { gate, index }))
            }
            (RegionGateIndex::Bivariate(index), RegionWindow::Bivariate(vs)) => {
                Some(Region::Bivariate(BivariateRegion {
                    index,
                    vertices: vs.into(),
                }))
            }
            _ => None,
        }
    }

    fn lookup(
        kws: &mut StdKeywords,
        i: RegionIndex,
        par: Par,
        conf: &StdTextReadConfig,
    ) -> LookupOptional<Self>
    where
        I: FromStr + fmt::Display + LinkedMeasIndex,
        ParseOptKeyError: From<<RegionGateIndex<I> as FromStr>::Err>,
    {
        Self::lookup_inner(
            kws,
            i,
            |k, j| RegionGateIndex::lookup_region_opt(k, j, par, conf),
            |k, j| RegionWindow::lookup_opt_st(k, j, (), conf),
            conf,
        )
    }

    fn lookup_dep(
        kws: &mut StdKeywords,
        i: RegionIndex,
        par: Par,
        conf: &StdTextReadConfig,
    ) -> LookupOptional<Self>
    where
        I: FromStr + fmt::Display + LinkedMeasIndex,
        ParseOptKeyError: From<<RegionGateIndex<I> as FromStr>::Err>,
    {
        Self::lookup_inner(
            kws,
            i,
            |k, j| RegionGateIndex::lookup_region_opt_dep(k, j, par, conf),
            |k, j| RegionWindow::lookup_opt_st_dep(k, j, (), conf),
            conf,
        )
    }

    fn lookup_inner<F0, F1>(
        kws: &mut StdKeywords,
        i: RegionIndex,
        lookup_index: F0,
        lookup_window: F1,
        conf: &StdTextReadConfig,
    ) -> LookupOptional<Self>
    where
        F0: FnOnce(&mut StdKeywords, RegionIndex) -> LookupOptional<RegionGateIndex<I>>,
        F1: FnOnce(&mut StdKeywords, IndexFromOne) -> LookupOptional<RegionWindow>,
        I: FromStr + fmt::Display,
        ParseOptKeyError: From<<RegionGateIndex<I> as FromStr>::Err>,
    {
        let n = lookup_index(kws, i);
        let w = lookup_window(kws, i.into());
        n.zip(w)
            .and_tentatively(|(n_, y_)| {
                n_.0.zip(y_.0)
                    .and_then(|(gi, win)| Self::try_new(gi, win).map(Region::inner_into))
                    .ok_or(MismatchedIndexAndWindowError)
                    .into_tentative_opt(!conf.allow_optional_dropping)
                    .inner_into()
            })
            .value_into()
    }

    pub(crate) fn opt_keywords(&self, i: RegionIndex) -> impl Iterator<Item = (String, String)>
    where
        I: Copy + FromStr + fmt::Display,
    {
        let (ri, rw) = self.split();
        [ri.meas_pair(i), rw.meas_pair(i)].into_iter()
    }

    pub(crate) fn split(&self) -> (RegionGateIndex<I>, RegionWindow)
    where
        I: Copy,
    {
        match self {
            Self::Univariate(r) => (
                RegionGateIndex::Univariate(r.index),
                // TODO clone
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
    fn lookup(kws: &mut StdKeywords, conf: &StdTextReadConfig) -> LookupTentative<Self> {
        Self::lookup_inner(
            kws,
            |k| Gate::lookup_opt(k, conf),
            GatedMeasurement::lookup,
            conf,
        )
    }

    fn lookup_dep(kws: &mut StdKeywords, conf: &StdTextReadConfig) -> LookupTentative<Self> {
        Self::lookup_inner(
            kws,
            |k| Gate::lookup_opt_dep(k, conf),
            GatedMeasurement::lookup_dep,
            conf,
        )
    }

    fn lookup_inner<F0, F1>(
        kws: &mut StdKeywords,
        lookup_gate: F0,
        lookup_meas: F1,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<Self>
    where
        F0: FnOnce(&mut StdKeywords) -> LookupOptional<Gate>,
        F1: Fn(
            &mut StdKeywords,
            GateIndex,
            &StdTextReadConfig,
        ) -> LookupTentative<GatedMeasurement>,
    {
        lookup_gate(kws).and_tentatively(|maybe| {
            if let Some(n) = maybe.0 {
                let xs = (0..n.0).map(|i| lookup_meas(kws, i.into(), conf));
                return Tentative::mconcat(xs).map(Self);
            }
            Tentative::default()
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
#[error("$GATING regions reference nonexistent gates: {}", .0.iter().join(","))]
pub struct GateMeasurementLinkError(NonEmpty<GateIndex>);

#[derive(From, Display, Debug, Error)]
pub enum NewAppliedGatesWithSchemeError {
    Link(GateMeasurementLinkError),
    Scheme(NewGatingSchemeError),
}

#[derive(From, Display, Debug, Error)]
pub enum AppliedGates3_0To2_0Error {
    Index(RegionToGateIndexError),
    Scheme(NewGatingSchemeError),
    Link(GateMeasurementLinkError),
}

#[derive(Debug, Error)]
pub enum AppliedGates3_0To3_2Error {
    #[error("{0}")]
    Index(#[from] RegionToMeasIndexError),
    #[error("$GATING references {0} $Gn* keywords")]
    HasGates(usize),
    #[error("{0}")]
    Scheme(#[from] NewGatingSchemeError),
}

#[derive(Debug, Error)]
#[error("cannot convert 2.0 $GATING/$Gn*/$RnI/$RnW keywords to 3.2")]
pub struct AppliedGates2_0To3_2Error;

#[derive(Debug, Error)]
#[error("cannot convert 3.2 $GATING/$RnI/$RnW keywords to 2.0")]
pub struct AppliedGates3_2To2_0Error;

#[derive(Debug, Error)]
#[error("values for $RnI and $RnW must both be univariate or bivariate")]
pub struct MismatchedIndexAndWindowError;

#[derive(Debug, Error)]
#[error(
    "cannot remove measurements since it is referenced by a gating region: {}",
    .0.iter().join(",")
)]
pub struct RemoveGateMeasIndexError(NonEmpty<MeasIndex>);

#[derive(Debug, Error)]
#[error("could not make gating scheme, regions not found: {}", .0.iter().join(","))]
pub struct NewGatingSchemeError(NonEmpty<RegionIndex>);

#[cfg(feature = "python")]
mod python {
    use crate::python::macros::{
        impl_from_py_via_fromstr, impl_pyreflow_err, impl_to_py_via_display, impl_value_err,
    };
    use crate::text::keywords::{Gating, GatingError, MeasOrGateIndex, MeasOrGateIndexError};

    use super::{GateMeasurementLinkError, NewAppliedGatesWithSchemeError, NewGatingSchemeError};

    impl_from_py_via_fromstr!(Gating);
    impl_to_py_via_display!(Gating);

    impl_from_py_via_fromstr!(MeasOrGateIndex);
    impl_to_py_via_display!(MeasOrGateIndex);

    impl_value_err!(GatingError);
    impl_value_err!(MeasOrGateIndexError);
    impl_pyreflow_err!(GateMeasurementLinkError);
    impl_pyreflow_err!(NewGatingSchemeError);
    impl_pyreflow_err!(NewAppliedGatesWithSchemeError);
}

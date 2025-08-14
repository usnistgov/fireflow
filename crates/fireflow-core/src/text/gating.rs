use crate::config::*;
use crate::error::*;
use crate::nonempty::FCSNonEmpty;
use crate::text::index::{GateIndex, IndexFromOne, MeasIndex, RegionIndex};
use crate::text::keywords::*;
use crate::text::optional::*;
use crate::text::parser::*;
use crate::validated::keys::*;

use derive_more::{AsRef, Display, From};
use derive_new::new;
use itertools::Itertools;
use nonempty::NonEmpty;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::str::FromStr;

#[cfg(feature = "serde")]
use serde::Serialize;

/// The $GATING/$RnI/$RnW/$Gn* keywords in a unified bundle (2.0)
///
/// Each region is assumed to point to a member of ['gated_measurements'].
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
/// Each region is assumed to point to a member of ['gated_measurements'] or
/// a measurement in the ['Core'] struct
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
/// Each region is assumed to point to a measurement in the ['Core'] struct
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
        gated_measurements: GatedMeasurements,
        scheme: GatingScheme<GateIndex>,
    ) -> Result<Self, GateMeasurementLinkError> {
        let n = gated_measurements.0.len();
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
                gated_measurements,
                scheme,
            })
        }
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
    ) -> LookupTentative<Self, DeprecatedError> {
        let ag = GatingScheme::lookup(kws, Gating::lookup_opt, |k, j| Region::lookup(k, j, par));
        let gm = GatedMeasurements::lookup(kws, conf);
        ag.zip(gm).and_tentatively(|(scheme, gated_measurements)| {
            Self::try_new(gated_measurements, scheme)
                .into_tentative_warn_def()
                .map_warnings(|e| LookupKeysWarning::Relation(e.into()))
        })
    }

    pub(crate) fn opt_keywords(&self) -> impl Iterator<Item = (String, String)> {
        let gate = Gate(self.gated_measurements.0.len());
        self.gated_measurements
            .0
            .iter()
            .enumerate()
            .flat_map(|(i, m)| m.opt_keywords(i.into()))
            .chain([OptMetarootKey::pair(&gate)])
            .chain(self.scheme.opt_keywords())
    }
}

impl AppliedGates3_0 {
    pub fn try_new(
        gated_measurements: GatedMeasurements,
        scheme: GatingScheme<MeasOrGateIndex>,
    ) -> Result<Self, GateMeasurementLinkError> {
        let n = gated_measurements.0.len();
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
                gated_measurements,
                scheme,
            })
        }
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

    pub(crate) fn lookup<E>(
        kws: &mut StdKeywords,
        par: Par,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<Self, E> {
        Self::lookup_inner(
            kws,
            |k| GatingScheme::lookup(k, Gating::lookup_opt, |kk, j| Region::lookup(kk, j, par)),
            |k| GatedMeasurements::lookup(k, conf),
        )
    }

    pub(crate) fn lookup_dep(
        kws: &mut StdKeywords,
        par: Par,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<Self, DeprecatedError> {
        let dd = conf.disallow_deprecated;
        Self::lookup_inner(
            kws,
            |k| {
                GatingScheme::lookup(
                    k,
                    |kk| Gating::lookup_opt_dep(kk, dd),
                    |kk, i| Region::lookup_dep(kk, i, par, dd),
                )
            },
            |k| GatedMeasurements::lookup_dep(k, conf),
        )
    }

    fn lookup_inner<E, F0, F1>(
        kws: &mut StdKeywords,
        lookup_scheme: F0,
        lookup_meas: F1,
    ) -> LookupTentative<Self, E>
    where
        F0: FnOnce(&mut StdKeywords) -> LookupTentative<GatingScheme<MeasOrGateIndex>, E>,
        F1: FnOnce(&mut StdKeywords) -> LookupTentative<GatedMeasurements, E>,
    {
        let s = lookup_scheme(kws);
        let ms = lookup_meas(kws);
        s.zip(ms).and_tentatively(|(scheme, gated_measurements)| {
            Self::try_new(gated_measurements, scheme)
                .into_tentative_warn_def()
                .map_warnings(|e| LookupKeysWarning::Relation(e.into()))
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
            .chain(gate.map(|x| OptMetarootKey::pair(&x)))
    }

    pub(crate) fn try_into_2_0(
        self,
        lossless: bool,
    ) -> BiDeferredResult<AppliedGates2_0, AppliedGates3_0To2_0Error> {
        // ASSUME region indices will still be unique in new hash table
        let (regions, es): (HashMap<_, _>, Vec<_>) = self
            .scheme
            .regions
            .into_iter()
            .map(|(ri, r)| r.try_map(|i| i.try_into()).map(|x| (ri, x)))
            .partition_result();
        let mut res = GatingScheme::try_new(self.scheme.gating, regions)
            .into_deferred()
            .def_and_maybe(|scheme| {
                AppliedGates2_0::try_new(self.gated_measurements, scheme).into_deferred()
            });
        for e in es {
            res.def_push_error_or_warning(AppliedGates3_0To2_0Error::Index(e), lossless);
        }
        res
    }

    pub(crate) fn try_into_3_2(
        self,
        lossless: bool,
    ) -> BiDeferredResult<AppliedGates3_2, AppliedGates3_0To3_2Error> {
        // ASSUME region indices will still be unique in new hash table
        let (regions, es): (HashMap<_, _>, Vec<_>) = self
            .scheme
            .regions
            .into_iter()
            .map(|(ri, r)| r.try_map(|i| i.try_into()).map(|x| (ri, x)))
            .partition_result();
        let mut res = AppliedGates3_2::try_new(self.scheme.gating, regions).into_deferred();
        for e in es {
            res.def_push_error_or_warning(AppliedGates3_0To3_2Error::Index(e), lossless);
        }
        let n_gates = self.gated_measurements.0.len();
        if n_gates > 0 {
            res.def_push_error_or_warning(AppliedGates3_0To3_2Error::HasGates(n_gates), lossless);
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
        disallow_deprecated: bool,
    ) -> LookupTentative<Self, DeprecatedError> {
        GatingScheme::lookup(
            kws,
            |k| Gating::lookup_opt_dep(k, disallow_deprecated),
            |k, i| Region::lookup_dep(k, i, par, disallow_deprecated),
        )
        .map(Self)
    }

    pub(crate) fn opt_keywords(&self) -> impl Iterator<Item = (String, String)> {
        self.0.opt_keywords()
    }
}

impl GatedMeasurement {
    fn lookup<E>(
        kws: &mut StdKeywords,
        i: GateIndex,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<Self, E> {
        Self::lookup_inner(
            kws,
            i,
            |k, j| GateScale::lookup_fixed_opt(k, j, conf),
            GateFilter::lookup_opt,
            GateShortname::lookup_opt,
            GatePercentEmitted::lookup_opt,
            GateRange::lookup_opt,
            GateLongname::lookup_opt,
            GateDetectorType::lookup_opt,
            GateDetectorVoltage::lookup_opt,
        )
    }

    fn lookup_dep(
        kws: &mut StdKeywords,
        i: GateIndex,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<Self, DeprecatedError> {
        let dd = conf.disallow_deprecated;
        Self::lookup_inner(
            kws,
            i,
            |k, j| GateScale::lookup_fixed_opt_dep(k, j, conf),
            |k, j| GateFilter::lookup_opt_dep(k, j, dd),
            |k, j| GateShortname::lookup_opt_dep(k, j, dd),
            |k, j| GatePercentEmitted::lookup_opt_dep(k, j, dd),
            |k, j| GateRange::lookup_opt_dep(k, j, dd),
            |k, j| GateLongname::lookup_opt_dep(k, j, dd),
            |k, j| GateDetectorType::lookup_opt_dep(k, j, dd),
            |k, j| GateDetectorVoltage::lookup_opt_dep(k, j, dd),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn lookup_inner<E, F0, F1, F2, F3, F4, F5, F6, F7>(
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
    ) -> LookupTentative<Self, E>
    where
        F0: FnOnce(&mut StdKeywords, GateIndex) -> LookupOptional<GateScale, E>,
        F1: FnOnce(&mut StdKeywords, IndexFromOne) -> LookupOptional<GateFilter, E>,
        F2: FnOnce(&mut StdKeywords, IndexFromOne) -> LookupOptional<GateShortname, E>,
        F3: FnOnce(&mut StdKeywords, IndexFromOne) -> LookupOptional<GatePercentEmitted, E>,
        F4: FnOnce(&mut StdKeywords, IndexFromOne) -> LookupOptional<GateRange, E>,
        F5: FnOnce(&mut StdKeywords, IndexFromOne) -> LookupOptional<GateLongname, E>,
        F6: FnOnce(&mut StdKeywords, IndexFromOne) -> LookupOptional<GateDetectorType, E>,
        F7: FnOnce(&mut StdKeywords, IndexFromOne) -> LookupOptional<GateDetectorVoltage, E>,
    {
        let j = i.into();
        let e = lookup_scale(kws, i);
        let f = lookup_filter(kws, j);
        let n = lookup_shortname(kws, j);
        let p = lookup_pe(kws, j);
        let r = lookup_range(kws, j);
        let s = lookup_longname(kws, j);
        let t = lookup_det_type(kws, j);
        let v = lookup_det_volt(kws, j);
        e.zip4(f, n, p).zip5(r, s, t, v).map(
            |(
                (scale, filter, shortname, percent_emitted),
                range,
                longname,
                detector_type,
                detector_voltage,
            )| {
                Self {
                    scale,
                    filter,
                    shortname,
                    percent_emitted,
                    range,
                    longname,
                    detector_type,
                    detector_voltage,
                }
            },
        )
    }

    pub(crate) fn opt_keywords(&self, i: GateIndex) -> impl Iterator<Item = (String, String)> {
        let j = i.into();
        [
            OptIndexedKey::pair_opt(&self.scale, j),
            OptIndexedKey::pair_opt(&self.filter, j),
            OptIndexedKey::pair_opt(&self.shortname, j),
            OptIndexedKey::pair_opt(&self.percent_emitted, j),
            OptIndexedKey::pair_opt(&self.range, j),
            OptIndexedKey::pair_opt(&self.longname, j),
            OptIndexedKey::pair_opt(&self.detector_type, j),
            OptIndexedKey::pair_opt(&self.detector_voltage, j),
        ]
        .into_iter()
        .flat_map(|(k, v)| v.map(|x| (k, x)))
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
        for (_, r) in self.regions.iter_mut() {
            r.shift_after_insert(i)
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

    fn lookup<F0, F1, E>(
        kws: &mut StdKeywords,
        lookup_gating: F0,
        lookup_region: F1,
    ) -> LookupTentative<Self, E>
    where
        F0: Fn(&mut StdKeywords) -> LookupOptional<Gating, E>,
        F1: Fn(&mut StdKeywords, RegionIndex) -> LookupOptional<Region<I>, E>,
    {
        lookup_gating(kws).map(|g| g.0).and_tentatively(|gating| {
            gating
                .as_ref()
                .map(|g| {
                    Tentative::mconcat(
                        g.region_indices()
                            .into_iter()
                            .map(|ri| lookup_region(kws, ri).map(|x| x.0.map(|y| (ri, y))))
                            .collect(),
                    )
                })
                .unwrap_or_default()
                .and_tentatively(|rs| {
                    let regions = rs.into_iter().flatten().collect();
                    Self::try_new(gating, regions)
                        .into_tentative_warn_def()
                        .map_warnings(|e| LookupKeysWarning::Relation(e.into()))
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
            .chain(self.gating.as_ref().map(OptMetarootKey::pair))
    }

    fn inner_into<J>(self) -> GatingScheme<J>
    where
        J: From<I>,
    {
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
                Some(Region::Univariate(UnivariateRegion { index, gate }))
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

    fn lookup<E>(
        kws: &mut StdKeywords,
        i: RegionIndex,
        par: Par,
    ) -> LookupTentative<MaybeValue<Self>, E>
    where
        I: FromStr + fmt::Display + LinkedMeasIndex,
        ParseOptKeyWarning: From<<RegionGateIndex<I> as FromStr>::Err>,
    {
        Self::lookup_inner(
            kws,
            i,
            |k, j| RegionGateIndex::lookup_region_opt(k, j, par),
            RegionWindow::lookup_opt,
        )
    }

    fn lookup_dep(
        kws: &mut StdKeywords,
        i: RegionIndex,
        par: Par,
        disallow_dep: bool,
    ) -> LookupTentative<MaybeValue<Self>, DeprecatedError>
    where
        I: FromStr + fmt::Display + LinkedMeasIndex,
        ParseOptKeyWarning: From<<RegionGateIndex<I> as FromStr>::Err>,
    {
        Self::lookup_inner(
            kws,
            i,
            |k, j| RegionGateIndex::lookup_region_opt_dep(k, j, par, disallow_dep),
            |k, j| RegionWindow::lookup_opt_dep(k, j, disallow_dep),
        )
    }

    fn lookup_inner<F0, F1, E>(
        kws: &mut StdKeywords,
        i: RegionIndex,
        lookup_index: F0,
        lookup_window: F1,
    ) -> LookupTentative<MaybeValue<Self>, E>
    where
        F0: FnOnce(
            &mut StdKeywords,
            RegionIndex,
        ) -> LookupTentative<MaybeValue<RegionGateIndex<I>>, E>,
        F1: FnOnce(&mut StdKeywords, IndexFromOne) -> LookupTentative<MaybeValue<RegionWindow>, E>,
        I: FromStr + fmt::Display,
        ParseOptKeyWarning: From<<RegionGateIndex<I> as FromStr>::Err>,
    {
        let n = lookup_index(kws, i);
        let w = lookup_window(kws, i.into());
        n.zip(w)
            .and_tentatively(|(_n, _y)| {
                _n.0.zip(_y.0)
                    .and_then(|(gi, win)| Self::try_new(gi, win).map(|x| x.inner_into()))
                    .map_or_else(
                        || {
                            let warn =
                                LookupRelationalWarning::GateRegion(MismatchedIndexAndWindowError)
                                    .into();
                            Tentative::new(None, vec![warn], vec![])
                        },
                        |x| Tentative::new1(Some(x)),
                    )
            })
            .map(|x| x.into())
    }

    pub(crate) fn opt_keywords(&self, i: RegionIndex) -> impl Iterator<Item = (String, String)>
    where
        I: Copy + FromStr + fmt::Display,
    {
        let (ri, rw) = self.split();
        [
            OptIndexedKey::pair(&ri, i.into()),
            OptIndexedKey::pair(&rw, i.into()),
        ]
        .into_iter()
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

    pub(crate) fn inner_into<J>(self) -> Region<J>
    where
        J: From<I>,
    {
        self.map(|i| i.into())
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
    fn lookup<E>(kws: &mut StdKeywords, conf: &StdTextReadConfig) -> LookupTentative<Self, E> {
        Self::lookup_inner(kws, Gate::lookup_opt, GatedMeasurement::lookup, conf)
    }

    fn lookup_dep(
        kws: &mut StdKeywords,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<Self, DeprecatedError> {
        let dd = conf.disallow_deprecated;
        Self::lookup_inner(
            kws,
            |k| Gate::lookup_opt_dep(k, dd),
            GatedMeasurement::lookup_dep,
            conf,
        )
    }

    fn lookup_inner<E, F0, F1>(
        kws: &mut StdKeywords,
        lookup_gate: F0,
        lookup_meas: F1,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<Self, E>
    where
        F0: FnOnce(&mut StdKeywords) -> LookupOptional<Gate, E>,
        F1: Fn(
            &mut StdKeywords,
            GateIndex,
            &StdTextReadConfig,
        ) -> LookupTentative<GatedMeasurement, E>,
    {
        lookup_gate(kws).and_tentatively(|maybe| {
            if let Some(n) = maybe.0 {
                let xs = (0..n.0).map(|i| lookup_meas(kws, i.into(), conf)).collect();
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

pub struct RegionToMeasIndexError(GateIndex);

impl fmt::Display for RegionToMeasIndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "cannot convert region index ({}) to measurement index since \
                   it refers to a gate",
            self.0
        )
    }
}

pub struct RegionToGateIndexError(MeasIndex);

impl fmt::Display for RegionToGateIndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "cannot convert region index ({}) to gating index since \
                   it refers to a measurement",
            self.0
        )
    }
}

pub struct GateToMeasIndexError(GateIndex);

impl fmt::Display for GateToMeasIndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "cannot convert gate index ({}) to measurement index",
            self.0
        )
    }
}

pub struct MeasToGateIndexError(PrefixedMeasIndex);

impl fmt::Display for MeasToGateIndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "cannot convert measurement index ({}) to gate index",
            self.0
        )
    }
}

pub struct GateMeasurementLinkError(NonEmpty<GateIndex>);

impl fmt::Display for GateMeasurementLinkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "$GATING regions reference nonexistent gates: {}",
            self.0.iter().join(",")
        )
    }
}

#[derive(From, Display)]
pub enum AppliedGates3_0To2_0Error {
    Index(RegionToGateIndexError),
    Scheme(NewGatingSchemeError),
    Link(GateMeasurementLinkError),
}

#[derive(From)]
pub enum AppliedGates3_0To3_2Error {
    #[from]
    Index(RegionToMeasIndexError),
    HasGates(usize),
    #[from]
    Scheme(NewGatingSchemeError),
}

impl fmt::Display for AppliedGates3_0To3_2Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Self::Index(x) => x.fmt(f),
            Self::Scheme(x) => x.fmt(f),
            Self::HasGates(n) => write!(f, "$GATING references {n} $Gn* keywords"),
        }
    }
}

pub struct AppliedGates2_0To3_2Error;

impl fmt::Display for AppliedGates2_0To3_2Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "cannot convert 2.0 $GATING/$Gn*/$RnI/$RnW keywords to 3.2"
        )
    }
}

pub struct AppliedGates3_2To2_0Error;

impl fmt::Display for AppliedGates3_2To2_0Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "cannot convert 3.2 $GATING/$RnI/$RnW keywords to 2.0")
    }
}

pub struct MismatchedIndexAndWindowError;

impl fmt::Display for MismatchedIndexAndWindowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "values for $RnI and $RnW must both be univariate or bivariate"
        )
    }
}

pub struct RemoveGateMeasIndexError(NonEmpty<MeasIndex>);

impl fmt::Display for RemoveGateMeasIndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "cannot remove measurements since it is referenced by a gating region: {}",
            self.0.iter().join(",")
        )
    }
}

pub struct NewGatingSchemeError(NonEmpty<RegionIndex>);

impl fmt::Display for NewGatingSchemeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "could not make gating scheme, regions not found: {}",
            self.0.iter().join(",")
        )
    }
}

#[cfg(feature = "python")]
mod python {
    use crate::python::macros::{
        impl_from_py_via_fromstr, impl_pyreflow_err, impl_to_py_via_display, impl_value_err,
    };
    use crate::text::keywords::{Gating, GatingError, MeasOrGateIndex};

    use super::{GateMeasurementLinkError, MeasOrGateIndexError, NewGatingSchemeError};

    impl_from_py_via_fromstr!(Gating);
    impl_to_py_via_display!(Gating);

    impl_from_py_via_fromstr!(MeasOrGateIndex);
    impl_to_py_via_display!(MeasOrGateIndex);

    impl_value_err!(GatingError);
    impl_value_err!(MeasOrGateIndexError);
    impl_pyreflow_err!(GateMeasurementLinkError);
    impl_pyreflow_err!(NewGatingSchemeError);
}

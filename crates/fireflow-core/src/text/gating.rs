use crate::config::{AllowLoss, ConfigFlag as _, ReadLayoutConfig, ReadStdKeywordsConfig};
use crate::core::{IndexedKeyLossError, UnitaryKeyLossError};
use crate::data::IndexedError;
use crate::logging::{
    DeferredIter as _, DeferredSwitchableErrors, DeferredWarningsAndErrors, LogResult,
    ResultExt as _, SwitchableErrorsResult,
};
use crate::nonempty::FCSNonEmpty;
use crate::text::deprecated::{DepGatedMeasRef, DeprecatedGatingSchemeRef};
use crate::text::deprecated::{DeprecatedStrRef, IndexedDepRef};
use crate::text::index::{GateIndex, IndexFromOne, MeasIndex, RegionIndex};
use crate::text::keywords::{
    Gate, GateDetectorType, GateDetectorVoltage, GateFilter, GateLongname, GatePercentEmitted,
    GateRange, GateScale, GateShortname, Gating, IndexPair, MeasOrGateIndex, Par,
    PrefixedMeasIndex, RegionGateIndex, RegionWindow, UniGate, Vertex,
};
use crate::text::lookup::{
    OptIndexedKey as _, OptIndexedKeyError, OptIndexedKeyStError, OptKeyError, OptMetarootKey,
};
use crate::text::optional::{CheckMaybe as _, KeywordPairMaybe as _};
use crate::text::relational::{
    DependentKeyError, ExistingIndexedLinkError, IndexedKeyToIndexLinkError, IndicesToRemove,
    RemovedGateLink, RemovedGating, RemovedLink,
};
use crate::validated::keys::{
    IndexedKey as _, Key1, NonStdKeywords, NonStdKeywordsExt as _, StdKey, StdKeywords,
};
use type_families::{
    ApplyOnce as _, Functor as _, FunctorOnce as _, impl_functor, impl_functor_once, impl_kind1,
};

use derive_more::{AsRef, Display, From};
use derive_new::new;
use itertools::Itertools as _;
use nonempty::NonEmpty;
use std::collections::HashMap;
use std::fmt;
use std::iter::repeat;
use std::mem::take;
use std::str::FromStr;
use thiserror::Error;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr};

/// The $GATING/$RnI/$RnW/$Gn* keywords in a unified bundle (2.0)
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

impl_kind1!(GatingSchemeFamily, GatingScheme);

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

impl_kind1!(RegionFamily, Region);
impl_kind1!(UnivariateRegionFamily, UnivariateRegion);
impl_kind1!(BivariateRegionFamily, BivariateRegion);

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

    pub(crate) fn lookup<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        conf: &C,
    ) -> DeferredWarningsAndErrors<Self, LookupAppliedGates2_0Error, LookupAppliedGates2_0Error>
    where
        C: AsRef<ReadLayoutConfig> + AsRef<ReadStdKeywordsConfig>,
    {
        let ag = GatingScheme::lookup(std, nonstd, conf)
            .map_errors(LookupAppliedGatesError::Scheme)
            .map_commutative_warnings(LookupAppliedGatesError::Scheme);
        let gm = GatedMeasurements::lookup(std, nonstd, conf)
            .map_errors(LookupAppliedGatesError::GatedMeas)
            .map_commutative_warnings(LookupAppliedGatesError::GatedMeas);
        let rconf: &ReadLayoutConfig = conf.as_ref();
        let flag = rconf.allow_optional_dropping;
        ag.zip_f2_once(gm)
            .and_then_deferred_switchable_result(flag, |(scheme, gated_measurements)| {
                Self::try_new(gated_measurements.0, scheme).map_err(LookupAppliedGatesError::Link)
            })
            .map_err_value(|ret| {
                if rconf.transfer_dropped_optional.is_set() {
                    ret.opt_keywords_std()
                        .for_each(|(k, v)| nonstd.insert_demoted(k, v));
                }
                ret
            })
    }

    pub(crate) fn opt_keywords_std(&self) -> impl Iterator<Item = (StdKey, String)> {
        let gate = Gate(self.gated_measurements.0.len());
        self.gated_measurements
            .0
            .iter()
            .enumerate()
            .flat_map(|(i, m)| m.opt_keywords_std(i.into()))
            .chain([gate.root_pair_std()])
            .chain(self.scheme.opt_keywords_std())
    }

    pub(crate) fn opt_keywords(&self) -> impl Iterator<Item = (String, String)> {
        self.opt_keywords_std().map(|(k, v)| (k.to_string(), v))
    }

    pub(crate) fn loss_errors(&self) -> impl Iterator<Item = AppliedGates2_0To3_2LossError> {
        let gs = self
            .gated_measurements
            .loss_errors()
            .map(AppliedGates2_0To3_2LossError::from);
        let ss = self
            .scheme
            .loss_errors()
            .map(AppliedGates2_0To3_2LossError::from);
        gs.chain(ss)
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

    // pub(crate) fn indices_difference(
    //     &self,
    //     indices: &MeasIndicesNoTime,
    // ) -> impl Iterator<Item = (RegionIndex, MeasIndex)> {
    //     self.scheme.indices_difference(indices)
    // }

    pub(crate) fn remove_invalid_links(&mut self, par: &Par) -> impl Iterator<Item = RemovedLink> {
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
    ) -> impl Iterator<Item = IndexedKeyToIndexLinkError<RegionGateIndex<MeasOrGateIndex>>> {
        self.scheme.invalid_link_errors(par)
    }

    pub(crate) fn lookup<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        conf: &C,
    ) -> DeferredWarningsAndErrors<Self, LookupAppliedGates3_0Error, LookupAppliedGates3_0Error>
    where
        C: AsRef<ReadLayoutConfig> + AsRef<ReadStdKeywordsConfig>,
    {
        let s = GatingScheme::lookup(std, nonstd, conf)
            .map_errors(LookupAppliedGatesError::Scheme)
            .map_commutative_warnings(LookupAppliedGatesError::Scheme);
        let ms = GatedMeasurements::lookup(std, nonstd, conf)
            .map_errors(LookupAppliedGatesError::GatedMeas)
            .map_commutative_warnings(LookupAppliedGatesError::GatedMeas);
        let rconf: &ReadLayoutConfig = conf.as_ref();
        s.zip_f2_once(ms)
            .and_then_deferred(|(scheme, gated_measurements)| {
                Self::try_new(gated_measurements.0, scheme)
                    .map_err(LookupAppliedGatesError::Link)
                    .into_succ()
            })
            .map_err_value(|ret| {
                if rconf.transfer_dropped_optional.is_set() {
                    ret.opt_keywords_std()
                        .for_each(|(k, v)| nonstd.insert_demoted(k, v));
                }
                ret
            })
    }

    pub(crate) fn opt_keywords_std(&self) -> impl Iterator<Item = (StdKey, String)> {
        let g = self.gated_measurements.0.len();
        let gate = if g == 0 { None } else { Some(Gate(g)) };
        self.gated_measurements
            .0
            .iter()
            .enumerate()
            .flat_map(|(i, m)| m.opt_keywords_std(i.into()))
            .chain(self.scheme.opt_keywords_std())
            .chain(gate.map(|x| OptMetarootKey::root_pair_std(&x)))
    }

    pub(crate) fn opt_keywords(&self) -> impl Iterator<Item = (String, String)> {
        self.opt_keywords_std().map(|(k, v)| (k.to_string(), v))
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
        let gs = self
            .gated_measurements
            .loss_errors()
            .map(AppliedGates3_0To3_2Error::from);
        self.scheme
            .convert_indices(flag)
            .map_switchable_errors(AppliedGates3_0To3_2Error::from)
            .extend_deferred_switchable_errors(gs)
            .map_deferred_value(AppliedGates3_2)
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

    // pub(crate) fn indices_difference(
    //     &self,
    //     indices: &MeasIndicesNoTime,
    // ) -> impl Iterator<Item = (RegionIndex, MeasIndex)> {
    //     self.0.indices_difference(indices)
    // }

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
    ) -> impl Iterator<Item = IndexedKeyToIndexLinkError<RegionGateIndex<PrefixedMeasIndex>>> {
        self.0.invalid_link_errors(par)
    }

    pub(crate) fn lookup<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        conf: &C,
    ) -> DeferredWarningsAndErrors<Self, LookupAppliedGates3_2Error, LookupAppliedGates3_2Error>
    where
        C: AsRef<ReadLayoutConfig> + AsRef<ReadStdKeywordsConfig>,
    {
        let rconf: &ReadLayoutConfig = conf.as_ref();
        GatingScheme::lookup(std, nonstd, conf)
            .map_deferred_value(Self)
            .map_err_value(|ret| {
                if rconf.transfer_dropped_optional.is_set() {
                    ret.0
                        .opt_keywords_std()
                        .for_each(|(k, v)| nonstd.insert_demoted(k, v));
                }
                ret
            })
    }

    pub(crate) fn opt_keywords(&self) -> impl Iterator<Item = (String, String)> {
        self.0.opt_keywords_std().map(|(k, v)| (k.to_string(), v))
    }

    pub(crate) fn loss_errors(&self) -> impl Iterator<Item = GatingSchemeLossError> {
        self.0.loss_errors()
    }
}

impl GatedMeasurement {
    fn lookup<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        i: GateIndex,
        conf: &C,
    ) -> DeferredWarningsAndErrors<Self, LookupGatedMeasError, LookupGatedMeasError>
    where
        C: AsRef<ReadLayoutConfig> + AsRef<ReadStdKeywordsConfig>,
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

    fn opt_keywords_std(&self, i: GateIndex) -> impl Iterator<Item = (StdKey, String)> {
        let x0 = self.scale.meas_opt_pair_std(i);
        let x1 = self.filter.meas_opt_pair_std(i);
        let x2 = self.shortname.meas_opt_pair_std(i);
        let x3 = self.percent_emitted.meas_opt_pair_std(i);
        let x4 = self.range.meas_opt_pair_std(i);
        let x5 = self.longname.meas_opt_pair_std(i);
        let x6 = self.detector_type.meas_opt_pair_std(i);
        let x7 = self.detector_voltage.meas_opt_pair_std(i);
        [x0, x1, x2, x3, x4, x5, x6, x7]
            .into_iter()
            .filter_map(|(k, v)| v.map(|x| (k, x)))
    }

    fn loss_errors(&self, i: GateIndex) -> impl Iterator<Item = GatedMeasurementLossError> {
        let x0 = self.scale.indexed_key_loss_error(i);
        let x1 = self.filter.indexed_key_loss_error(i);
        let x2 = self.shortname.indexed_key_loss_error(i);
        let x3 = self.percent_emitted.indexed_key_loss_error(i);
        let x4 = self.range.indexed_key_loss_error(i);
        let x5 = self.longname.indexed_key_loss_error(i);
        let x6 = self.detector_type.indexed_key_loss_error(i);
        let x7 = self.detector_voltage.indexed_key_loss_error(i);
        [x0, x1, x2, x3, x4, x5, x6, x7].into_iter().flatten()
    }
}

impl<I> Default for GatingScheme<I> {
    fn default() -> Self {
        Self::new(None, HashMap::new())
    }
}

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

    // fn indices_difference(
    //     &self,
    //     indices: &MeasIndicesNoTime,
    // ) -> impl Iterator<Item = (RegionIndex, MeasIndex)>
    // where
    //     I: LinkedMeasIndex,
    // {
    //     self.meas_indices()
    //         .filter(|(_, mi)| !indices.as_ref().contains(mi))
    // }

    pub(crate) fn existing_link_errors(
        &self,
        indices: &IndicesToRemove,
    ) -> impl Iterator<Item = ExistingIndexedLinkError<RegionGateIndex<I>, IndexFromOne>>
    where
        I: LinkedMeasIndex,
    {
        // TODO this will print one error for every measurement index even in
        // cases where one RnI keyword has a pair of indices. This isn't a huge
        // deal but it means we could have twice as many error messages as
        // otherwise.
        self.meas_indices()
            .filter(|(_, mi)| indices.as_ref().contains(mi))
            .map(|(ri, mi)| {
                let js = NonEmpty::new(mi.into());
                ExistingIndexedLinkError::new(Key1::new_i1(ri.into()), js)
            })
    }

    pub(crate) fn invalid_link_errors(
        &self,
        par: &Par,
    ) -> impl Iterator<Item = IndexedKeyToIndexLinkError<RegionGateIndex<I>>>
    where
        I: LinkedMeasIndex,
    {
        self.meas_indices()
            .filter(|(_, mi)| usize::from(*mi) < usize::from(par.0))
            .map(|(ri, mi)| {
                let js = NonEmpty::new(mi.into());
                IndexedKeyToIndexLinkError::new(js, Key1::new_i1(ri.into()))
            })
    }

    pub(crate) fn remove_invalid_links(&mut self, par: &Par) -> impl Iterator<Item = RemovedLink>
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
                self.regions.get(&rni).into_iter().any(|rnw| {
                    rnw.meas_indices()
                        .any(|x| usize::from(x) >= usize::from(*par))
                })
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
            .extract_if(|_, rnw| {
                rnw.meas_indices()
                    .any(|x| usize::from(x) >= usize::from(*par))
            })
            .map(|(rni, rnw)| {
                let bad_indices = rnw
                    .meas_indices()
                    .filter(|x| usize::from(*x) >= usize::from(*par));
                // ASSUME this won't fail because we pre-filtered above
                let js = NonEmpty::collect(bad_indices).unwrap();
                RemovedLink::from(RemovedGateLink::new(rni, rnw, js))
            })
            .chain(gating.map(RemovedLink::Gating))
    }

    fn meas_indices(&self) -> impl Iterator<Item = (RegionIndex, MeasIndex)>
    where
        I: LinkedMeasIndex,
    {
        self.regions
            .iter()
            .flat_map(|(ri, v)| v.meas_indices().map(|mi| (*ri, mi)))
    }

    fn lookup<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        conf: &C,
    ) -> DeferredWarningsAndErrors<
        Self,
        LookupGatingSchemeError<LookupRegionIndexError<I>>,
        LookupGatingSchemeError<LookupRegionIndexError<I>>,
    >
    where
        I: FromStr + fmt::Display + LinkedMeasIndex + PartialEq + Copy,
        C: AsRef<ReadLayoutConfig> + AsRef<ReadStdKeywordsConfig>,
    {
        let rconf: &ReadLayoutConfig = conf.as_ref();
        let flag = rconf.allow_optional_dropping;
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
                                    .map_deferred_value(|x| x.map(|y| (ri, y)))
                                    .map_errors(LookupGatingSchemeError::Region)
                                    .map_commutative_warnings(LookupGatingSchemeError::Region)
                            })
                            .mappend_def()
                    })
                    .and_then_deferred_switchable_result(flag, |rs| {
                        let regions = rs.into_iter().flatten().collect();
                        Self::try_new(gating, regions).map_err(LookupGatingSchemeError::Link)
                    })
            })
    }

    pub(crate) fn opt_keywords_std(&self) -> impl Iterator<Item = (StdKey, String)>
    where
        I: fmt::Display + FromStr + Copy,
    {
        self.regions
            .iter()
            .flat_map(|(ri, r)| r.opt_keywords_std(*ri))
            .chain(self.gating.as_ref().map(OptMetarootKey::root_pair_std))
    }

    // pub(crate) fn opt_keywords(&self) -> impl Iterator<Item = (String, String)>
    // where
    //     I: fmt::Display + FromStr + Copy,
    // {
    //     self.regions
    //         .iter()
    //         .flat_map(|(ri, r)| r.opt_keywords(*ri))
    //         .chain(self.gating.as_ref().map(OptMetarootKey::root_pair))
    // }

    pub(crate) fn loss_errors(&self) -> impl Iterator<Item = GatingSchemeLossError>
    where
        I: Copy,
    {
        let gating = self
            .gating
            .root_key_loss_error()
            .map(GatingSchemeLossError::Gating);
        self.regions
            .keys()
            .flat_map(|ri| Region::<I>::loss_errors(*ri))
            .map(GatingSchemeLossError::from)
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
        SwitchableErrorsResult::new_switchable_iter((), (), es, flag).and_then_switchable(|()| {
            GatingScheme::try_new(self.gating, regions)
                .map_err(ConvertSchemeError::from)
                .into_nowarn()
                .set_err_value(GatingScheme::default())
        })
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

    fn lookup<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        ri: RegionIndex,
        conf: &C,
    ) -> DeferredWarningsAndErrors<
        Option<Self>,
        LookupRegionError<LookupRegionIndexError<I>>,
        LookupRegionError<LookupRegionIndexError<I>>,
    >
    where
        I: FromStr + fmt::Display + LinkedMeasIndex + PartialEq,
        C: AsRef<ReadLayoutConfig> + AsRef<ReadStdKeywordsConfig>,
    {
        let index_res = RegionGateIndex::remove_or_drop_meas_opt_with(std, nonstd, ri, (), conf)
            .map_switchable_errors(LookupRegionError::Region)
            .switchable_into_commutative()
            .into_semigroup();
        let window_res = RegionWindow::remove_or_drop_meas_opt_with(std, nonstd, ri, (), conf)
            .map_switchable_errors(LookupRegionError::Window)
            .switchable_into_commutative()
            .into_semigroup();
        let rconf: &ReadLayoutConfig = conf.as_ref();
        let flag = rconf.allow_optional_dropping;
        index_res
            .zip_f2_once(window_res)
            .and_then_deferred_switchable_result(flag, |(gi_opt, w_opt)| {
                // Try to combine the gateindex and window together to make a
                // region. This will only work if both are present and
                // they are both the same type (uni/bi-variate). If anything
                // fails, return none, log an error (or warning if we allow
                // dropping), and demote the keywords if applicable.
                let res = match (gi_opt, w_opt) {
                    (Some(gi), Some(w)) => match Self::try_new(gi, w) {
                        Ok(x) => Ok(Some(x.fmap_into())),
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

    fn loss_errors(i: RegionIndex) -> impl Iterator<Item = GateRegionLossError>
    where
        I: Copy,
    {
        let ri = IndexedKeyLossError(Key1::new_i1(i.into()));
        let rw = IndexedKeyLossError(Key1::new_i1(i.into()));
        [
            GateRegionLossError::Index(ri),
            GateRegionLossError::Window(rw),
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
                RegionWindow::Univariate(r.gate.clone()),
            ),
            Self::Bivariate(r) => (
                RegionGateIndex::Bivariate(r.index),
                RegionWindow::Bivariate(r.vertices.clone().into()),
            ),
        }
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
    fn lookup<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        conf: &C,
    ) -> DeferredWarningsAndErrors<Self, LookupGatedMeasurementsError, LookupGatedMeasurementsError>
    where
        C: AsRef<ReadLayoutConfig> + AsRef<ReadStdKeywordsConfig>,
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
                        .mappend_def()
                        .map_deferred_value(Self)
                } else {
                    LogResult::new_ok_default()
                }
            })
    }

    fn loss_errors(&self) -> impl Iterator<Item = GatedMeasurementsLossError> {
        let xs = &self.0;
        let g = (!xs.is_empty()).then_some(GatedMeasurementsLossError::Gate(
            UnitaryKeyLossError::default(),
        ));
        xs.iter()
            .enumerate()
            .flat_map(|(i, m)| m.loss_errors(i.into()))
            .map(GatedMeasurementsLossError::from)
            .chain(g)
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
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum AppliedGates3_0To3_2Error {
    Scheme(ConvertSchemeError<GateIndex, true>),
    GatedMeas(GatedMeasurementsLossError),
}

/// Error when converting gating keywords from 2.0 to 3.2
///
/// This conversion is actually impossible, so all this will signify is the
/// keywords that are to be dropped.
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum AppliedGates2_0To3_2LossError {
    GatedMeas(GatedMeasurementsLossError),
    Scheme(GatingSchemeLossError),
}

/// Error when converting $GATING/$RnI/$RnW keywords to new version.
///
/// $RnI can fail because it may contain indices that refer to something
/// that is unsupported in the target version.
///
/// $GATING can fail because it may refer to $RnI/$RnW keywords which are
/// no longer valid as described above.
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(I: fmt::Display + Copy))]
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
#[cfg_attr(feature = "python", pyerr(crate::python::ConversionError))]
#[cfg_attr(feature = "python", bound(I: fmt::Display + Copy))]
pub struct ConvertIndexForRegionError<I, const INDEX_IS_GATE: bool>(
    IndexedError<AnyIndexForRegionError<I>>,
);

impl<I: fmt::Display + Copy, const INDEX_IS_GATE: bool> fmt::Display
    for ConvertIndexForRegionError<I, INDEX_IS_GATE>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let region_key = RegionGateIndex::<()>::std(self.0.index);
        let keys = |i: I, is_plural: bool, is_gate: bool| {
            let prefix = if is_gate { "G" } else { "P" };
            let key = format!("{prefix}{i}*");
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

/// Error when $GATING/$RnI/$RnW keywords need to be dropped when converting versions
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum GatingSchemeLossError {
    Region(GateRegionLossError),
    Gating(UnitaryKeyLossError<Gating>),
}

/// Error when $RnI/$RnW keywords need to be dropped when converting versions
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum GateRegionLossError {
    Index(IndexedKeyLossError<RegionGateIndex<()>>),
    Window(IndexedKeyLossError<RegionWindow>),
}

/// Error when $Gn* or $GATE keywords need to be dropped when converting versions
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum GatedMeasurementsLossError {
    Gate(UnitaryKeyLossError<Gate>),
    GatedMeas(GatedMeasurementLossError),
}

/// Error when $Gn* keywords need to be dropped when converting versions
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum GatedMeasurementLossError {
    Scale(IndexedKeyLossError<GateScale>),
    Filter(IndexedKeyLossError<GateFilter>),
    Shortname(IndexedKeyLossError<GateShortname>),
    PEmit(IndexedKeyLossError<GatePercentEmitted>),
    Range(IndexedKeyLossError<GateRange>),
    Longname(IndexedKeyLossError<GateLongname>),
    DetType(IndexedKeyLossError<GateDetectorType>),
    DetVolt(IndexedKeyLossError<GateDetectorVoltage>),
}

/// Error when parsing $GATING/$RnI/$RnW/$Gn*/$GATE keywords for 2.0
pub type LookupAppliedGates2_0Error = LookupAppliedGatesError<LookupRegionIndex2_0Error>;

/// Error when parsing $GATING/$RnI/$RnW/$Gn*/$GATE keywords for 3.0 and 3.1
pub type LookupAppliedGates3_0Error = LookupAppliedGatesError<LookupRegionIndex3_0Error>;

/// Error when parsing $GATING/$RnI/$RnW keywords for 3.2
pub type LookupAppliedGates3_2Error = LookupGatingSchemeError<LookupRegionIndex3_2Error>;

/// Error when parsing $RnI keyword for 2.0
pub type LookupRegionIndex2_0Error = LookupRegionIndexError<GateIndex>;

/// Error when parsing $RnI keyword for 3.0/3.1
pub type LookupRegionIndex3_0Error = LookupRegionIndexError<MeasOrGateIndex>;

/// Error when parsing $RnI keyword for 3.2
pub type LookupRegionIndex3_2Error = LookupRegionIndexError<PrefixedMeasIndex>;

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
// TODO this seems like it should be a general link error
#[derive(Debug, Error)]
#[error("$RnI keywords reference nonexistent $Gn* indices: {}", .0.iter().join(","))]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::RelationalError))]
pub struct GateMeasurementLinkError(NonEmpty<GateIndex>);

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
#[cfg_attr(feature = "python", pyerr(crate::python::RelationalError))]
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

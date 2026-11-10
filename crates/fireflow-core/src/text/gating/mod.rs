pub mod scheme;

use crate::config::EvaledReadStdKeywordsConfig;
use crate::core::TrimmedKeywords;
use crate::fixed_vec::OneOrTwo;
use crate::logging::{
    DeferredIter as _, DeferredSwitchableErrors, DeferredWarningsAndErrors, LogResult,
    ResultExt as _, WarningsAndErrorsResult,
};
use crate::std_index::index::StdLookupTx;
use crate::text::gating::scheme::{ConvertSchemeError, GatingScheme, LookupGatingSchemeError};
use crate::text::keyword_enum::{
    AsStdKeywordPair as _, DollarKey, GateMeasKeyword, Keyword0FromValue as _,
    Keyword1FromValue as _, OptRootKeyword, RegionKeyword, SplitKeyword, SplitKeyword_,
};
use crate::text::keywords::{
    Gate, GateDetectorType, GateDetectorVoltage, GateFilter, GateLongname, GatePercentEmitted,
    GateRange, GateScale, GateShortname, Gating, IndexPair, MeasOrGateIndex, Par,
    PrefixedMeasIndex, RegionGateIndex, RegionWindow, RegionWindowRef, ScaleFix, UniGate,
    ValueToStdKey, Vertex,
};
use crate::text::lookup::{Diagnosed, OptKeyError, OptStKeyError, OptValue};
use crate::text::relational::{
    BrokenRegionLinkError, DependentKeyError, ExistingIndexedLinkError, HasLink, RemovedLink,
};

use fireflow_types::config::{AllowLoss, ProcessOptionalFailure, ReadDataKeywordsConfig};
use fireflow_types::index::{GateIndex, MeasIndex, RegionIndex};
use fireflow_types::keys::nonstd::DollarWrap;
use nonempty::{IntoIteratorExt as _, NEVec, NonEmptyIterator as _};
use type_families::{
    ApplyOnce as _, BifunctorOnce as _, Functor as _, FunctorOnce as _, impl_functor,
    impl_functor_once, impl_kind1,
};

use derive_more::{AsRef, Display, From};
use derive_new::new;
use itertools::Itertools as _;
use std::collections::HashMap;
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

#[cfg(feature = "python")]
impl UnivariateRegion<MeasOrGateIndex> {
    // Arg order matters here since this is wrapped in a macro in the python
    // API. The is_gate arg is optional which is why it needs to be last.
    #[must_use]
    pub fn new3_0(index: usize, gate: UniGate, is_gate: bool) -> Self {
        Self::new(gate, MeasOrGateIndex::new(index, is_gate))
    }

    #[must_use]
    pub fn index(&self) -> usize {
        self.index.index()
    }

    #[must_use]
    pub fn is_gate(&self) -> bool {
        self.index.is_gate()
    }
}

/// A bivariate region corresponding to an $RnI/$RnW keyword pair
#[derive(Clone, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct BivariateRegion<I> {
    pub vertices: NEVec<Vertex>,
    pub index: IndexPair<I>,
}

#[cfg(feature = "python")]
impl BivariateRegion<MeasOrGateIndex> {
    // Arg order matters here since this is wrapped in a macro in the python
    // API. The *_is_gate args are optional which is why they need to be last.
    #[must_use]
    pub fn new3_0(
        x_index: usize,
        y_index: usize,
        vertices: NEVec<Vertex>,
        x_is_gate: bool,
        y_is_gate: bool,
    ) -> Self {
        let x = MeasOrGateIndex::new(x_index, x_is_gate);
        let y = MeasOrGateIndex::new(y_index, y_is_gate);
        Self::new(vertices, IndexPair::new(x, y))
    }

    #[must_use]
    pub fn x_index(&self) -> usize {
        self.index.x.index()
    }

    #[must_use]
    pub fn y_index(&self) -> usize {
        self.index.y.index()
    }

    #[must_use]
    pub fn x_is_gate(&self) -> bool {
        self.index.x.is_gate()
    }

    #[must_use]
    pub fn y_is_gate(&self) -> bool {
        self.index.y.is_gate()
    }
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

#[derive(new, Default)]
pub(crate) struct AppliedGatesDiagnostics {
    pub(crate) trimmed: TrimmedKeywords,
    pub(crate) fixed_scales: Vec<ScaleFix>,
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
            .regions()
            .values()
            .flat_map(|r| r.indices())
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

    // pub fn try_new1(
    //     gated_measurements: Vec<GatedMeasurement>,
    //     regions: HashMap<RegionIndex, Region<I>>,
    //     gating: Option<Gating>,
    // ) -> Result<Self, NewAppliedGatesWithSchemeError>
    // where
    //     I: Copy,
    //     GateIndex: TryFrom<I>,
    // {
    //     let scheme = GatingScheme::try_new(gating, regions)?;
    //     Ok(Self::try_new(gated_measurements, scheme)?)
    // }

    #[must_use]
    pub fn split(
        self,
    ) -> (
        Vec<GatedMeasurement>,
        HashMap<RegionIndex, Region<I>>,
        Option<Gating>,
    ) {
        let (r, g) = self.scheme.split();
        (self.gated_measurements.0, r, g)
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn lookup<C>(
        kws: &mut StdLookupTx,
        conf: &C,
    ) -> WarningsAndErrorsResult<
        Diagnosed<Self, AppliedGatesDiagnostics>,
        (),
        LookupAppliedGatesPre3_2Error<I>,
        LookupAppliedGatesPre3_2Error<I>,
    >
    where
        GateIndex: TryFrom<I>,
        I: FromStr + LinkedMeasIndex + PartialEq + Copy,
        C: AsRef<ReadDataKeywordsConfig> + AsRef<EvaledReadStdKeywordsConfig>,
        for<'a> RegionKeyword<'a>: From<SplitKeyword<RegionGateIndex<I>>>,
        RegionGateIndex<I>:
            ValueToStdKey<Index = RegionIndex> + OptValue<Outer = Option<RegionGateIndex<I>>>,
    {
        let ag = GatingScheme::lookup(kws, conf)
            .map_errors(LookupAppliedGatesError::Scheme)
            .map_commutative_warnings(LookupAppliedGatesError::Scheme);
        let gm = GatedMeasurements::lookup(kws, conf)
            .map_errors(LookupAppliedGatesError::GatedMeas)
            .map_commutative_warnings(LookupAppliedGatesError::GatedMeas);
        let rconf: &ReadDataKeywordsConfig = conf.as_ref();
        let flag = rconf.process_optional_failure;
        ag.zip_f2_once(gm)
            .and_then_deferred_switchable_result3(flag, |(scheme, gated_ms)| {
                let diag = AppliedGatesDiagnostics::new(scheme.diagnostic, gated_ms.diagnostic);
                Self::try_new(gated_ms.inner.0, scheme.inner)
                    .map_err(LookupAppliedGatesError::Link)
                    .map(|x| Diagnosed::new(x, diag))
            })
            .map_err_value(|ret| {
                let opt_flag = rconf.process_optional_failure;
                ret.inner.scheme.set_failure_flag(kws, opt_flag);
                ret.inner.gated_measurements.set_failure_flag(kws, opt_flag);
            })
    }

    pub(crate) fn opt_keywords<'a>(&'a self) -> impl Iterator<Item = OptRootKeyword<'a>>
    where
        I: Copy,
        RegionGateIndex<I>: ValueToStdKey<Index = RegionIndex>,
        RegionKeyword<'a>: From<SplitKeyword<RegionGateIndex<I>>>,
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

    pub(crate) fn existing_link_errors<C>(
        &self,
        indices: &C,
    ) -> impl Iterator<Item = ExistingIndexedLinkError<RegionGateIndex<MeasOrGateIndex>, MeasIndex>>
    where
        C: HasLink<Inner = MeasIndex>,
    {
        self.scheme.existing_link_errors(indices)
    }

    pub(crate) fn invalid_link_errors(
        &self,
        par: &Par,
    ) -> impl Iterator<Item = BrokenRegionLinkError> {
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
        self.0.split()
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

    pub(crate) fn existing_link_errors<C>(
        &self,
        indices: &C,
    ) -> impl Iterator<Item = ExistingIndexedLinkError<RegionGateIndex<PrefixedMeasIndex>, MeasIndex>>
    where
        C: HasLink<Inner = MeasIndex>,
    {
        self.0.existing_link_errors(indices)
    }

    pub(crate) fn invalid_link_errors(
        &self,
        par: &Par,
    ) -> impl Iterator<Item = BrokenRegionLinkError> {
        self.0.invalid_link_errors(par)
    }

    pub(crate) fn lookup<C>(
        kws: &mut StdLookupTx,
        conf: &C,
    ) -> WarningsAndErrorsResult<
        Diagnosed<Self, AppliedGatesDiagnostics>,
        (),
        LookupAppliedGates3_2Error,
        LookupAppliedGates3_2Error,
    >
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<EvaledReadStdKeywordsConfig>,
    {
        let rconf: &ReadDataKeywordsConfig = conf.as_ref();
        GatingScheme::lookup(kws, conf)
            .map_ok_value(|out| out.bimap_once(Self, |d| AppliedGatesDiagnostics::new(d, vec![])))
            .map_err_value(|ret| {
                let flag = rconf.process_optional_failure;
                ret.inner.set_failure_flag(kws, flag);
            })
    }

    pub(crate) fn opt_keywords(&self) -> impl Iterator<Item = OptRootKeyword<'_>> {
        self.0.opt_keywords()
    }
}

impl GatedMeasurement {
    fn lookup<C>(
        kws: &mut StdLookupTx,
        i: GateIndex,
        conf: &C,
    ) -> DeferredWarningsAndErrors<
        Diagnosed<Self, ScaleFix>,
        LookupGatedMeasError,
        LookupGatedMeasError,
    >
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<EvaledReadStdKeywordsConfig>,
    {
        let (scale, se) = GateScale::remove_or_drop_meas_opt_with(kws, i, (), conf);
        let filter = GateFilter::remove_meas_opt_nofail(kws, i);
        let (sname, ne) = GateShortname::remove_or_drop_meas_opt(kws, i, conf.as_ref());
        let (pemit, pe) = GatePercentEmitted::remove_or_drop_meas_opt(kws, i, conf.as_ref());
        let (rng, re) = GateRange::remove_or_drop_meas_opt(kws, i, conf.as_ref());
        let lname = GateLongname::remove_meas_opt_nofail(kws, i);
        let dtype = GateDetectorType::remove_meas_opt_nofail(kws, i);
        let (dvolt, de) = GateDetectorVoltage::remove_or_drop_meas_opt(kws, i, conf.as_ref());
        let es = [
            se.map(LookupGatedMeasError::from),
            pe.map(LookupGatedMeasError::from),
            ne.map(LookupGatedMeasError::from),
            re.map(LookupGatedMeasError::from),
            de.map(LookupGatedMeasError::from),
        ]
        .into_iter()
        .flatten();
        let ret = Self::new(scale.inner, filter, sname, pemit, rng, lname, dtype, dvolt);
        let diag = Diagnosed::new(ret, scale.diagnostic);
        LogResult::new_deferred_from_err_iter(es, diag)
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

    fn set_failure_flag(self, i: GateIndex, kws: &mut StdLookupTx, flag: ProcessOptionalFailure) {
        for x in self.opt_keywords(i) {
            let k = x.as_std_key();
            kws.set_failure_flag(&DollarWrap(k), flag);
        }
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
            (RegionGateIndex::Bivariate(index), RegionWindow::Bivariate(vertices)) => {
                Ok(Self::Bivariate(BivariateRegion { vertices, index }))
            }
            (r, w) => Err((r, w)),
        }
    }

    #[allow(clippy::type_complexity)]
    fn lookup<C>(
        kws: &mut StdLookupTx,
        ri: RegionIndex,
        conf: &C,
    ) -> DeferredWarningsAndErrors<
        Diagnosed<Option<Self>, TrimmedKeywords>,
        LookupRegionError<LookupRegionIndexError<I>>,
        LookupRegionError<LookupRegionIndexError<I>>,
    >
    where
        I: FromStr + LinkedMeasIndex + PartialEq,
        C: AsRef<ReadDataKeywordsConfig> + AsRef<EvaledReadStdKeywordsConfig>,
        for<'a> RegionKeyword<'a>: From<SplitKeyword<RegionGateIndex<I>>>,
        RegionGateIndex<I>:
            ValueToStdKey<Index = RegionIndex> + OptValue<Outer = Option<RegionGateIndex<I>>>,
    {
        let (gi_out, ie) = RegionGateIndex::remove_or_drop_meas_opt_with(kws, ri, (), conf);
        let (w_out, we) = RegionWindow::remove_or_drop_meas_opt_with(kws, ri, (), conf);
        let rconf: &ReadDataKeywordsConfig = conf.as_ref();
        let flag = rconf.process_optional_failure;
        // Try to combine the gateindex and window together to make a
        // region. This will only work if both are present and
        // they are both the same type (uni/bi-variate). If anything
        // fails, return none, log an error (or warning if we allow
        // dropping), and demote the keywords if applicable.
        let (gi_val, gi_trimmed) = gi_out.into_opt_indexed_pair(&ri);
        let (w_val, w_trimmed) = w_out.into_opt_indexed_pair(&ri);
        let trimmed = gi_trimmed.into_iter().chain(w_trimmed).collect();
        let res = match (gi_val, w_val) {
            (Some(gi), Some(w)) => match Self::try_new(gi, w) {
                Ok(x) => Ok(Some(x.fmap_into())),
                Err((old_gi, old_w)) => {
                    kws.set_failure_flag(&old_gi.std_(&ri), flag);
                    kws.set_failure_flag(&old_w.std_(&ri), flag);
                    Err(IndexWindowMismatchError::Both(ri))
                }
            },
            (Some(old_gi), None) => {
                kws.set_failure_flag(&old_gi.std_(&ri), flag);
                Err(IndexWindowMismatchError::NoWindow(ri))
            }
            (None, Some(old_w)) => {
                kws.set_failure_flag(&old_w.std_(&ri), flag);
                Err(IndexWindowMismatchError::NoIndex(ri))
            }
            (None, None) => Ok(None),
        };
        let es = [
            ie.map(LookupRegionError::Region),
            we.map(LookupRegionError::Window),
        ]
        .into_iter()
        .flatten();
        res.map_err(LookupRegionError::Mismatch)
            .map(|x| Diagnosed::new(x, trimmed))
            .into_deferred_switchable3(flag)
            .extend_deferred_switchable_errors3(es)
            .switchable_into_commutative()
    }

    pub(crate) fn set_failure_flag<'a>(
        &'a self,
        i: RegionIndex,
        kws: &mut StdLookupTx,
        flag: ProcessOptionalFailure,
    ) where
        I: Copy,
        RegionGateIndex<I>: ValueToStdKey<Index = RegionIndex>,
        RegionKeyword<'a>: From<SplitKeyword<RegionGateIndex<I>>>,
    {
        for r in self.opt_keywords(i) {
            let k = DollarWrap(r.as_std_key());
            kws.set_failure_flag(&k, flag);
        }
    }

    // pub(crate) fn set_lookup_action_seen<'a>(
    //     &'a self,
    //     i: RegionIndex,
    //     kws: &mut StdLookupTx,
    //     a: LookupAction,
    // ) where
    //     I: Copy,
    //     RegionGateIndex<I>: ValueToStdKey<Index = RegionIndex>,
    //     RegionKeyword<'a>: From<SplitKeyword<RegionGateIndex<I>>>,
    // {
    //     for r in self.opt_keywords(i) {
    //         let k = r.as_std_key();
    //         kws.set_lookup_action_seen(&k, a);
    //     }
    // }

    pub(crate) fn opt_keywords<'a>(&'a self, i: RegionIndex) -> [RegionKeyword<'a>; 2]
    where
        I: Copy,
        RegionGateIndex<I>: ValueToStdKey<Index = RegionIndex>,
        RegionKeyword<'a>: From<SplitKeyword<RegionGateIndex<I>>>,
    {
        let ri = match self {
            Self::Univariate(r) => RegionGateIndex::Univariate(r.index),
            Self::Bivariate(r) => RegionGateIndex::Bivariate(r.index),
        };
        let rw = match self {
            Self::Univariate(r) => RegionWindowRef::Univariate(&r.gate),
            Self::Bivariate(r) => RegionWindowRef::Bivariate(r.vertices.as_nonempty_slice()),
        };
        let x0 = RegionKeyword::from_value(ri, i);
        let rk = DollarKey::new(i);
        let x1 = RegionKeyword::Window(SplitKeyword_::new(rk, rw));
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
            Self::Bivariate(r) => OneOrTwo::Two(&r.index.x, &r.index.y),
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

    fn set_failure_flag(self, kws: &mut StdLookupTx, flag: ProcessOptionalFailure) {
        if let Some(k) = self.gate().map(|v| v.std0_()) {
            kws.set_failure_flag(&k, flag);
        }
        for (i, g) in self.0.into_iter().enumerate() {
            g.set_failure_flag(i.into(), kws, flag);
        }
    }

    fn lookup<C>(
        kws: &mut StdLookupTx,
        conf: &C,
    ) -> DeferredWarningsAndErrors<
        Diagnosed<Self, Vec<ScaleFix>>,
        LookupGatedMeasurementsError,
        LookupGatedMeasurementsError,
    >
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<EvaledReadStdKeywordsConfig>,
    {
        let rconf: &ReadDataKeywordsConfig = conf.as_ref();
        let flag = rconf.process_optional_failure;
        let (maybe_gate, ge) = Gate::remove_or_drop_root_opt(kws, conf.as_ref());
        if let Some(n) = maybe_gate {
            // TODO large $GATE could crash process
            (0..n.0)
                .map(|i| {
                    GatedMeasurement::lookup(kws, i.into(), conf)
                        .map_commutative_warnings(LookupGatedMeasurementsError::Meas)
                        .map_errors(LookupGatedMeasurementsError::Meas)
                })
                .sequence_def()
                .map_deferred_value(|xs| {
                    let (gs, ds) = xs.into_iter().map(|x| (x.inner, x.diagnostic)).unzip();
                    Diagnosed::new(Self(gs), ds)
                })
                .extend_warnings_or_errors3(
                    ge,
                    |v| v,
                    LookupGatedMeasurementsError::Gate,
                    LookupGatedMeasurementsError::Gate,
                    flag,
                )
        } else {
            LogResult::new_ok_default()
        }
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
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum NewAppliedGatesWithSchemeError {
    Link(GateMeasurementLinkError),
    Scheme(DependentKeyError<Gating>),
}

/// Error when converting gating keywords from 3.0/3.1 to 2.0
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum AppliedGates3_0To2_0Error {
    Scheme(ConvertSchemeError<MeasIndex>),
    Link(GateMeasurementLinkError),
}

/// Error when converting gating keywords from 3.0/3.1 to 3.2
pub type AppliedGates3_0To3_2Error = ConvertSchemeError<GateIndex>;

/// Error when converting between region index types
#[derive(From, Debug, PartialEq, Clone)]
pub(crate) enum AnyIndexForRegionError<I> {
    Univariate(UniIndexForRegionError<I>),
    Bivariate(BiIndexForRegionError<I>),
}

/// Error when converting between region index types (bivariate)
#[derive(Debug, PartialEq, Clone)]
pub(crate) enum BiIndexForRegionError<I> {
    LeftBivariate(I),
    RightBivariate(I),
    Bivariate(I, I),
}

/// Error when converting between region index types (univariate)
#[derive(Debug, Display, PartialEq, Clone)]
pub struct UniIndexForRegionError<I>(pub(crate) I);

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
pub type LookupRegionIndexError<I> = OptStKeyError<RegionGateIndex<I>>;

/// Error when parsing $GATING/$RnI/$RnW/$Gn*/$GATE keywords
#[derive(Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr), bound(E: Into<Self>))]
pub enum LookupAppliedGatesError<E> {
    Scheme(LookupGatingSchemeError<E>),
    GatedMeas(LookupGatedMeasurementsError),
    Link(GateMeasurementLinkError),
}

/// Error when $RnI keywords reference nonexistent $Gn* keywords
#[derive(Debug, Error, PartialEq, Clone)]
#[error("$RnI keywords reference nonexistent $Gn* indices: {}", .0.iter().join(","))]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct GateMeasurementLinkError(NEVec<GateIndex>);

/// Error when parsing $RnI/$RnW keywords
#[derive(Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(E: Into<Self>))]
pub enum LookupRegionError<E> {
    Mismatch(IndexWindowMismatchError),
    Region(E),
    Window(OptStKeyError<RegionWindow>),
}

/// Error when $RnI and $RnW keywords mismatch
#[derive(Debug, Error, PartialEq, Clone)]
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
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupGatedMeasurementsError {
    Gate(OptKeyError<Gate>),
    Meas(LookupGatedMeasError),
}

/// Error when parsing $Gn* keywords
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupGatedMeasError {
    Scale(OptStKeyError<GateScale>),
    Shortname(OptKeyError<GateShortname>),
    PercentEmitted(OptKeyError<GatePercentEmitted>),
    Range(OptKeyError<GateRange>),
    DetectorVoltage(OptKeyError<GateDetectorVoltage>),
}

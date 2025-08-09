use crate::config::*;
use crate::error::*;
use crate::text::index::*;
use crate::text::keywords::*;
use crate::text::optional::*;
use crate::text::parser::*;
use crate::validated::keys::*;

use itertools::Itertools;
use nonempty::NonEmpty;
use std::fmt;
use std::str::FromStr;

#[cfg(feature = "serde")]
use serde::Serialize;

/// The $GATING/$RnI/$RnW/$Gn* keywords in a unified bundle (2.0)
///
/// Each region is assumed to point to a member of ['gated_measurements'].
// TODO updates to these are currently not validated
#[derive(Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct AppliedGates2_0 {
    gated_measurements: GatedMeasurements,
    regions: GatingRegions<GateIndex>,
}

/// The $GATING/$RnI/$RnW/$Gn* keywords in a unified bundle (3.0-3.1)
///
/// Each region is assumed to point to a member of ['gated_measurements'] or
/// a measurement in the ['Core'] struct
// TODO updates to these are currently not validated
#[derive(Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct AppliedGates3_0 {
    gated_measurements: Vec<GatedMeasurement>,
    regions: GatingRegions<MeasOrGateIndex>,
}

/// The $GATING/$RnI/$RnW keywords in a unified bundle (3.2)
///
/// Each region is assumed to point to a measurement in the ['Core'] struct
// TODO updates to these are currently not validated
#[derive(Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct AppliedGates3_2 {
    pub regions: GatingRegions<PrefixedMeasIndex>,
}

/// The $GATING/$RnI/$RnW keywords in a unified bundle.
///
/// All regions in $GATING are assumed to have corresponding $RnI/$RnW keywords,
/// and each $RnI/$RnW pair is assumed to be consistent (ie both are univariate
/// or bivariate)
#[derive(Clone, PartialEq)]
pub struct GatingRegions<I> {
    pub gating: Gating,
    pub regions: NonEmpty<(RegionIndex, Region<I>)>,
}

/// A list of $Gn* keywords for indices 1-n.
///
/// The maximum value of 'n' implies the $GATE keyword.
#[derive(Clone, PartialEq)]
pub struct GatedMeasurements(pub NonEmpty<GatedMeasurement>);

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
pub struct BivariateRegion<I> {
    pub vertices: NonEmpty<Vertex>,
    pub x_index: I,
    pub y_index: I,
}

/// The values for $Gm* keywords (2.0-3.1)
#[derive(Clone, Default, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct GatedMeasurement {
    /// Value for $GmE
    pub scale: MaybeValue<GateScale>,

    /// Value for $GmF
    pub filter: MaybeValue<GateFilter>,

    /// Value for $GmN
    ///
    /// Unlike $PnN, this is not validated to be without commas
    pub shortname: MaybeValue<GateShortname>,

    /// Value for $GmP
    pub percent_emitted: MaybeValue<GatePercentEmitted>,

    /// Value for $GmR
    pub range: MaybeValue<GateRange>,

    /// Value for $GmS
    pub longname: MaybeValue<GateLongname>,

    /// Value for $GmT
    pub detector_type: MaybeValue<GateDetectorType>,

    /// Value for $GmV
    pub detector_voltage: MaybeValue<GateDetectorVoltage>,
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
    fn map<F, J>(self, mut f: F) -> BivariateRegion<J>
    where
        F: FnMut(I) -> J,
    {
        BivariateRegion {
            vertices: self.vertices,
            x_index: f(self.x_index),
            y_index: f(self.y_index),
        }
    }

    fn try_map<F, J, E>(self, mut f: F) -> Result<BivariateRegion<J>, E>
    where
        F: FnMut(I) -> Result<J, E>,
    {
        Ok(BivariateRegion {
            vertices: self.vertices,
            x_index: f(self.x_index)?,
            y_index: f(self.y_index)?,
        })
    }
}

impl AppliedGates2_0 {
    pub(crate) fn lookup(
        kws: &mut StdKeywords,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<MaybeValue<Self>, DeprecatedError> {
        let ag = GatingRegions::lookup(kws, Gating::lookup_opt, Region::lookup);
        let gm = GatedMeasurements::lookup(kws, conf);
        ag.zip(gm).and_tentatively(|(x, y)| {
            if let Some((applied, gated_measurements)) = x.0.zip(y.0) {
                let ret = Self {
                    gated_measurements,
                    regions: applied,
                };
                match ret.check_gates() {
                    Ok(_) => Tentative::new1(Some(ret).into()),
                    Err(e) => {
                        let w = LookupKeysWarning::Relation(e.into());
                        Tentative::new(None.into(), vec![w], vec![])
                    }
                }
            } else {
                Tentative::new1(None.into())
            }
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
            .chain(self.regions.opt_keywords())
    }

    pub fn check_gates(&self) -> Result<(), GateMeasurementLinkError> {
        let n = self.gated_measurements.0.len();
        let it = self
            .regions
            .regions
            .as_ref()
            .flat_map(|(_, r)| r.clone().flatten())
            .into_iter()
            .filter(|i| usize::from(*i) > n);
        NonEmpty::collect(it).map_or(Ok(()), |xs| Err(GateMeasurementLinkError(xs)))
    }
}

impl AppliedGates3_0 {
    pub(crate) fn lookup<E>(
        kws: &mut StdKeywords,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<MaybeValue<Self>, E> {
        Self::lookup_inner(
            kws,
            |k| GatingRegions::lookup(k, Gating::lookup_opt, Region::lookup),
            |k| GatedMeasurements::lookup(k, conf),
        )
    }

    pub(crate) fn lookup_dep(
        kws: &mut StdKeywords,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<MaybeValue<Self>, DeprecatedError> {
        let dd = conf.disallow_deprecated;
        Self::lookup_inner(
            kws,
            |k| {
                GatingRegions::lookup(
                    k,
                    |kk| Gating::lookup_opt_dep(kk, dd),
                    |kk, i| Region::lookup_dep(kk, i, dd),
                )
            },
            |k| GatedMeasurements::lookup_dep(k, conf),
        )
    }

    fn lookup_inner<E, F0, F1>(
        kws: &mut StdKeywords,
        lookup_regions: F0,
        lookup_meas: F1,
    ) -> LookupTentative<MaybeValue<Self>, E>
    where
        F0: FnOnce(&mut StdKeywords) -> LookupOptional<GatingRegions<MeasOrGateIndex>, E>,
        F1: FnOnce(&mut StdKeywords) -> LookupOptional<GatedMeasurements, E>,
    {
        let ag = lookup_regions(kws);
        let gm = lookup_meas(kws);
        ag.zip(gm).and_tentatively(|(x, y)| {
            if let Some(applied) = x.0 {
                let ret = Self {
                    gated_measurements: y.0.map(|z| z.0.into()).unwrap_or_default(),
                    regions: applied,
                };
                match ret.check_gates() {
                    Ok(_) => Tentative::new1(Some(ret).into()),
                    Err(e) => {
                        let w = LookupKeysWarning::Relation(e.into());
                        Tentative::new(None.into(), vec![w], vec![])
                    }
                }
            } else {
                Tentative::new1(None.into())
            }
        })
    }

    pub(crate) fn opt_keywords(&self) -> impl Iterator<Item = (String, String)> {
        let g = self.gated_measurements.len();
        let gate = if g == 0 { None } else { Some(Gate(g)) };
        self.gated_measurements
            .iter()
            .enumerate()
            .flat_map(|(i, m)| m.opt_keywords(i.into()))
            .chain(self.regions.opt_keywords())
            .chain(gate.map(|x| OptMetarootKey::pair(&x)))
    }

    pub fn check_gates(&self) -> Result<(), GateMeasurementLinkError> {
        let n = self.gated_measurements.len();
        let it = self
            .regions
            .regions
            .as_ref()
            .flat_map(|(_, r)| r.clone().flatten())
            .into_iter()
            .flat_map(|i| GateIndex::try_from(i).ok())
            .filter(|i| usize::from(*i) > n);
        NonEmpty::collect(it).map_or(Ok(()), |xs| Err(GateMeasurementLinkError(xs)))
    }

    pub(crate) fn try_into_2_0(
        self,
        lossless: bool,
    ) -> BiDeferredResult<AppliedGates2_0, AppliedGates3_0To2_0Error> {
        let (rs, es): (Vec<_>, Vec<_>) = self
            .regions
            .regions
            .into_iter()
            .map(|(ri, r)| r.try_map(|i| i.try_into()).map(|x| (ri, x)))
            .partition_result();
        let gr_res = NonEmpty::from_vec(rs)
            .map(|regions| GatingRegions {
                regions,
                gating: self.regions.gating,
            })
            .ok_or(AppliedGates3_0To2_0Error::NoRegions);
        let gms_res =
            NonEmpty::from_vec(self.gated_measurements).ok_or(AppliedGates3_0To2_0Error::NoGates);
        let mut res = gr_res
            .zip(gms_res)
            .mult_to_deferred()
            .def_map_value(|(regions, gs)| AppliedGates2_0 {
                gated_measurements: GatedMeasurements(gs),
                regions,
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
        let (rs, es): (Vec<_>, Vec<_>) = self
            .regions
            .regions
            .into_iter()
            .map(|(ri, r)| r.try_map(|i| i.try_into()).map(|x| (ri, x)))
            .partition_result();
        let mut res = NonEmpty::from_vec(rs)
            .map(|regions| GatingRegions {
                regions,
                gating: self.regions.gating,
            })
            .ok_or(AppliedGates3_0To3_2Error::NoRegions)
            .map(|regions| AppliedGates3_2 { regions })
            .into_deferred();
        for e in es {
            res.def_push_error_or_warning(AppliedGates3_0To3_2Error::Index(e), lossless);
        }
        let n_gates = self.gated_measurements.len();
        if n_gates > 0 {
            res.def_push_error_or_warning(AppliedGates3_0To3_2Error::HasGates(n_gates), lossless);
        }
        res
    }
}

impl AppliedGates3_2 {
    pub(crate) fn lookup(
        kws: &mut StdKeywords,
        disallow_deprecated: bool,
    ) -> LookupTentative<MaybeValue<Self>, DeprecatedError> {
        GatingRegions::lookup(
            kws,
            |k| Gating::lookup_opt_dep(k, disallow_deprecated),
            |k, i| Region::lookup_dep(k, i, disallow_deprecated),
        )
        .map(|x| x.map(|regions| Self { regions }))
    }

    pub(crate) fn opt_keywords(&self) -> impl Iterator<Item = (String, String)> {
        self.regions.opt_keywords()
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

impl<I> GatingRegions<I> {
    fn lookup<F0, F1, E>(
        kws: &mut StdKeywords,
        lookup_gating: F0,
        lookup_region: F1,
    ) -> LookupTentative<MaybeValue<Self>, E>
    where
        F0: Fn(&mut StdKeywords) -> LookupTentative<MaybeValue<Gating>, E>,
        F1: Fn(&mut StdKeywords, RegionIndex) -> LookupTentative<MaybeValue<Region<I>>, E>,
    {
        lookup_gating(kws)
            .and_tentatively(|maybe| {
                if let Some(gating) = maybe.0 {
                    let res = gating.flatten().try_map(|ri| {
                        lookup_region(kws, ri)
                            .map(|x| x.0.map(|y| (ri, y)))
                            .transpose()
                            .ok_or(GateRegionLinkError.into())
                    });
                    match res {
                        Ok(xs) => {
                            Tentative::mconcat_ne(xs).map(|regions| Some(Self { regions, gating }))
                        }
                        Err(w) => {
                            Tentative::new(None, vec![LookupKeysWarning::Relation(w)], vec![])
                        }
                    }
                } else {
                    Tentative::new1(None)
                }
            })
            .map(|x| x.into())
    }

    pub(crate) fn opt_keywords(&self) -> impl Iterator<Item = (String, String)>
    where
        I: fmt::Display,
        I: FromStr,
        I: Copy,
    {
        self.regions
            .iter()
            .flat_map(|(ri, r)| r.opt_keywords(*ri))
            .chain([OptMetarootKey::pair(&self.gating)])
    }

    fn inner_into<J>(self) -> GatingRegions<J>
    where
        J: From<I>,
    {
        GatingRegions {
            gating: self.gating,
            regions: self.regions.map(|(ri, r)| (ri, r.inner_into())),
        }
    }
}

impl<I> Region<I> {
    pub(crate) fn try_new(r_index: RegionGateIndex<I>, window: RegionWindow) -> Option<Self> {
        match (r_index, window) {
            (RegionGateIndex::Univariate(index), RegionWindow::Univariate(gate)) => {
                Some(Region::Univariate(UnivariateRegion { index, gate }))
            }
            (RegionGateIndex::Bivariate(x_index, y_index), RegionWindow::Bivariate(vertices)) => {
                Some(Region::Bivariate(BivariateRegion {
                    x_index,
                    y_index,
                    vertices,
                }))
            }
            _ => None,
        }
    }

    fn lookup<E>(kws: &mut StdKeywords, i: RegionIndex) -> LookupTentative<MaybeValue<Self>, E>
    where
        I: FromStr,
        I: fmt::Display,
        ParseOptKeyWarning: From<<RegionGateIndex<I> as FromStr>::Err>,
    {
        Self::lookup_inner(
            kws,
            i,
            RegionGateIndex::lookup_opt,
            RegionWindow::lookup_opt,
        )
    }

    fn lookup_dep(
        kws: &mut StdKeywords,
        i: RegionIndex,
        disallow_dep: bool,
    ) -> LookupTentative<MaybeValue<Self>, DeprecatedError>
    where
        I: FromStr,
        I: fmt::Display,
        ParseOptKeyWarning: From<<RegionGateIndex<I> as FromStr>::Err>,
    {
        Self::lookup_inner(
            kws,
            i,
            |k, j| RegionGateIndex::lookup_opt_dep(k, j, disallow_dep),
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
            IndexFromOne,
        ) -> LookupTentative<MaybeValue<RegionGateIndex<I>>, E>,
        F1: FnOnce(&mut StdKeywords, IndexFromOne) -> LookupTentative<MaybeValue<RegionWindow>, E>,
        I: FromStr,
        I: fmt::Display,
        ParseOptKeyWarning: From<<RegionGateIndex<I> as FromStr>::Err>,
    {
        let n = lookup_index(kws, i.into());
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
        I: Copy,
        I: FromStr,
        I: fmt::Display,
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
            Self::Univariate(x) => (
                RegionGateIndex::Univariate(x.index),
                // TODO clone
                RegionWindow::Univariate(x.gate.clone()),
            ),
            Self::Bivariate(x) => (
                RegionGateIndex::Bivariate(x.x_index, x.y_index),
                RegionWindow::Bivariate(x.vertices.clone()),
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

    pub(crate) fn flatten(self) -> NonEmpty<I> {
        match self {
            Self::Univariate(r) => NonEmpty::new(r.index),
            Self::Bivariate(r) => (r.x_index, vec![r.y_index]).into(),
        }
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
    fn lookup<E>(
        kws: &mut StdKeywords,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<MaybeValue<Self>, E> {
        Self::lookup_inner(kws, Gate::lookup_opt, GatedMeasurement::lookup, conf)
    }

    fn lookup_dep(
        kws: &mut StdKeywords,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<MaybeValue<Self>, DeprecatedError> {
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
    ) -> LookupTentative<MaybeValue<Self>, E>
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
                // TODO this will be nicer with NonZeroUsize
                if n.0 > 0 {
                    let xs = NonEmpty::collect((0..n.0).map(|i| lookup_meas(kws, i.into(), conf)))
                        .unwrap();
                    return Tentative::mconcat_ne(xs).map(|x| Some(Self(x)).into());
                }
            }
            Tentative::new1(None.into())
        })
    }
}

impl From<AppliedGates2_0> for AppliedGates3_0 {
    fn from(value: AppliedGates2_0) -> Self {
        Self {
            gated_measurements: value.gated_measurements.0.into(),
            regions: value.regions.inner_into(),
        }
    }
}

impl From<AppliedGates3_2> for AppliedGates3_0 {
    fn from(value: AppliedGates3_2) -> Self {
        Self {
            gated_measurements: vec![],
            regions: value.regions.inner_into(),
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

impl fmt::Display for GateRegionLinkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "regions in $GATING which do not have $RnI/$RnW")
    }
}

pub struct GateRegionLinkError;

impl fmt::Display for GateMeasurementLinkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "$GATING regions reference nonexistent gates: {}",
            self.0.iter().join(",")
        )
    }
}

pub enum AppliedGates3_0To2_0Error {
    Index(RegionToGateIndexError),
    NoGates,
    NoRegions,
}

impl fmt::Display for AppliedGates3_0To2_0Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Self::Index(x) => x.fmt(f),
            Self::NoGates => write!(f, "no $Gn* keywords present"),
            Self::NoRegions => write!(f, "no valid $Rn* keywords present"),
        }
    }
}

pub enum AppliedGates3_0To3_2Error {
    Index(RegionToMeasIndexError),
    HasGates(usize),
    NoRegions,
}

impl fmt::Display for AppliedGates3_0To3_2Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Self::Index(x) => x.fmt(f),
            Self::HasGates(n) => write!(f, "$GATING references {n} $Gn* keywords"),
            Self::NoRegions => write!(f, "no valid $Rn* keywords present"),
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

#[cfg(feature = "serde")]
mod serialize {
    use super::{BivariateRegion, GatedMeasurements, GatingRegions};
    use serde::{ser::SerializeStruct, Serialize};

    impl Serialize for GatedMeasurements {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            self.0.iter().collect::<Vec<_>>().serialize(serializer)
        }
    }

    impl<I> Serialize for GatingRegions<I>
    where
        I: Serialize,
    {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            let mut state = serializer.serialize_struct("AppliedGates", 2)?;
            state.serialize_field("gating", &self.gating)?;
            state.serialize_field("regions", &self.regions.iter().collect::<Vec<_>>())?;
            state.end()
        }
    }

    impl<I: Serialize> Serialize for BivariateRegion<I> {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            let mut state = serializer.serialize_struct("BivariateRegion", 2)?;
            state.serialize_field("vertices", &self.vertices.iter().collect::<Vec<_>>())?;
            state.serialize_field("x_index", &self.x_index)?;
            state.serialize_field("y_index", &self.y_index)?;
            state.end()
        }
    }
}

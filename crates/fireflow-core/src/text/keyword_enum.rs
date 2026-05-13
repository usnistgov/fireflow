//! Wrapper types for keyword values.
//!
//! Used to iterate over keywords without converting to strings first, allowing
//! fast access and easy filtering if required.

use crate::meas::GainLossError;
use crate::text::datetimes::{BeginDateTime, EndDateTime};
use crate::text::index::{IndexFromOne, MeasIndex, RegionIndex};
use crate::text::keywords as kws;
use crate::text::spillover::Spillover;
use crate::text::timestamps::FCSDate;
use crate::validated::keys::{AnyKey, DKey0, DKey1, DKey2, DollarKey, NonStdKey, VersionedKey};
use crate::validated::keys::{AsStdKey, StdKey};
use crate::validated::shortname::Shortname;
use crate::validated::textdelim::{
    DelimCollisionError, HasDelim, TEXTDelim, ambassador_impl_HasDelim,
};

use fireflow_types::keywords::{Version, VersionMembership};
use fireflow_types::nonempty_string::{
    DisplayNE as _, DisplayableNE as _, NEStr, NEString, ToDisplayNE, ToNE,
};

use ambassador::{Delegate, delegatable_trait};
use derive_more::{Display, From};
use derive_new::new;
use num_traits::One as _;
use thiserror::Error;

use std::fmt::{self, Write as _};
use std::num::NonZeroU32;

#[cfg(feature = "serde")]
use crate::validated::keys::IndexedKey;

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr},
    fireflow_types::python as py,
};

/// Any offset keyword type
#[derive(Clone, From, Delegate)]
#[delegate(DisplayEscaped)]
pub(crate) enum OffsetKeyword {
    Nextdata(SplitKeyword0<kws::Nextdata>),
    Begindata(SplitKeyword0<kws::Begindata>),
    Enddata(SplitKeyword0<kws::Enddata>),
    Beginanalysis(SplitKeyword0<kws::Beginanalysis>),
    Endanalysis(SplitKeyword0<kws::Endanalysis>),
    Beginstext(SplitKeyword0<kws::Beginstext>),
    Endstext(SplitKeyword0<kws::Endstext>),
}

/// Any (non-offset) keyword type
#[derive(Clone, From, Delegate)]
#[delegate(HasDelim)]
#[delegate(DisplayEscaped)]
pub(crate) enum AnyKeyword<'a> {
    Req(ReqKeyword<'a>),
    Opt(OptKeyword<'a>),
}

/// Any required keyword type
#[derive(Clone, From, Delegate)]
#[delegate(HasDelim)]
#[delegate(AsStdKeywordPair)]
#[delegate(DisplayEscaped)]
pub(crate) enum ReqKeyword<'a> {
    Root(ReqRootKeyword<'a>),
    Meas(ReqMeasKeyword<'a>),
}

/// Any optional keyword type
#[derive(Clone, From, Delegate)]
#[delegate(HasDelim)]
#[delegate(AsKeywordPair)]
#[delegate(DisplayEscaped)]
pub(crate) enum OptKeyword<'a> {
    Root(StdOrNonStdOptRootKeyword<'a>),
    Meas(StdOrNonStdOptMeasKeyword<'a>),
}

/// Any non-measurement keyword type
#[derive(Clone, From, Delegate)]
#[delegate(HasDelim)]
#[delegate(AsKeywordPair)]
#[delegate(DisplayEscaped)]
pub(crate) enum StdOrNonStdOptRootKeyword<'a> {
    Std(OptRootKeyword<'a>),
    NonStd(NonStdKeyword<'a>),
}

/// Any measurement keyword type
#[derive(Clone, From, Delegate)]
#[delegate(HasDelim)]
#[delegate(AsKeywordPair)]
#[delegate(DisplayEscaped)]
pub(crate) enum StdOrNonStdOptMeasKeyword<'a> {
    Std(OptMeasKeyword<'a>),
    NonStd(NonStdKeyword<'a>),
}

/// Any required root keyword type
// TODO this shouldn't need to be pub
#[derive(Clone, From, Delegate)]
#[delegate(AsStdKeywordPair)]
#[delegate(DisplayEscaped)]
pub enum ReqRootKeyword<'a> {
    ByteOrd2_0(SplitKeyword0<kws::ByteOrd2_0>),
    ByteOrd3_1(SplitKeyword0<kws::ByteOrd3_1>),
    Par(SplitKeyword0<kws::Par>),
    Tot(SplitKeyword0<kws::Tot>),
    Datatype(SplitKeyword0<kws::AlphaNumType>),
    Mode(SplitKeyword0<kws::Mode>),
    Cyt(RefKeyword0<'a, kws::Cyt3_2>),
}

/// Any optional root keyword type
#[derive(Clone, From, Delegate)]
#[delegate(AsStdKeywordPair)]
#[delegate(DisplayEscaped)]
#[delegate(HasMembership)]
pub enum OptRootKeyword<'a> {
    GateMeas(GateMeasKeyword<'a>),
    GateRegion(RegionKeyword<'a>),
    Dfc(SplitKeyword2<kws::Dfc>),
    UnstainedCenters(SplitKeyword<DKey0<kws::UnstainedCenters>, kws::NEUnstainedCenters>),
    CSMode(SplitKeyword0<kws::CSMode>),
    CSVFlag(SplitKeyword1<kws::CSVFlag>),
    CSVBits(NonZeroU32Keyword0<kws::CSVBits>),
    CSTot(NonZeroU32Keyword0<kws::CSTot>),
    Btim2_0(SplitKeyword0<kws::Btim2_0>),
    Btim3_0(SplitKeyword0<kws::Btim3_0>),
    Btim3_1(SplitKeyword0<kws::Btim3_1>),
    Etim2_0(SplitKeyword0<kws::Etim2_0>),
    Etim3_0(SplitKeyword0<kws::Etim3_0>),
    Etim3_1(SplitKeyword0<kws::Etim3_1>),
    Date(SplitKeyword0<FCSDate>),
    Begindatetime(SplitKeyword0<BeginDateTime>),
    Enddatetime(SplitKeyword0<EndDateTime>),
    Gate(SplitKeyword0<kws::Gate>),
    Gating(RefKeyword0<'a, kws::Gating>),
    Comp(RefKeyword0<'a, kws::Compensation3_0>),
    Unicode(RefKeyword0<'a, kws::Unicode>),
    Abrt(SplitKeyword0<kws::Abrt>),
    Lost(SplitKeyword0<kws::Lost>),
    Tr(RefKeyword0<'a, kws::Trigger>),
    Vol(SplitKeyword0<kws::Vol>),
    LastModified(SplitKeyword0<kws::LastModified>),
    Originality(SplitKeyword0<kws::Originality>),
    Mode3_2(SplitKeyword0<kws::Mode3_2>),
    Spillover(RefKeyword0<'a, Spillover>),
    Cyt(NEStringKeyword0<'a, kws::Cyt>),
    Cytsn(NEStringKeyword0<'a, kws::Cytsn>),
    Com(NEStringKeyword0<'a, kws::Com>),
    Cells(NEStringKeyword0<'a, kws::Cells>),
    Exp(NEStringKeyword0<'a, kws::Exp>),
    Fil(NEStringKeyword0<'a, kws::Fil>),
    Inst(NEStringKeyword0<'a, kws::Inst>),
    Op(NEStringKeyword0<'a, kws::Op>),
    Proj(NEStringKeyword0<'a, kws::Proj>),
    Smno(NEStringKeyword0<'a, kws::Smno>),
    Src(NEStringKeyword0<'a, kws::Src>),
    Sys(NEStringKeyword0<'a, kws::Sys>),
    Flowrate(NEStringKeyword0<'a, kws::Flowrate>),
    LastModifier(NEStringKeyword0<'a, kws::LastModifier>),
    UnstainedInfo(NEStringKeyword0<'a, kws::UnstainedInfo>),
    Carrierid(NEStringKeyword0<'a, kws::Carrierid>),
    Carriertype(NEStringKeyword0<'a, kws::Carriertype>),
    Locationid(NEStringKeyword0<'a, kws::Locationid>),
    Plateid(NEStringKeyword0<'a, kws::Plateid>),
    Platename(NEStringKeyword0<'a, kws::Platename>),
    Wellid(NEStringKeyword0<'a, kws::Wellid>),
}

/// Any required measurement keyword type
#[derive(Clone, From, Delegate)]
#[delegate(AsStdKeywordPair)]
#[delegate(DisplayEscaped)]
#[cfg_attr(feature = "serde", delegate(AsHeader))]
pub enum ReqMeasKeyword<'a> {
    Shortname(RefKeyword1<'a, Shortname>),
    Scale(SplitKeyword1<kws::Scale>),
    TemporalScale3_0(SplitKeyword1<kws::TemporalScale3_0>),
    Width(SplitKeyword1<kws::Width>),
    Range(SplitKeyword1<kws::TextRange>),
}

/// Any optional measurement keyword type
#[derive(Clone, From, Delegate)]
#[delegate(AsStdKeywordPair)]
#[delegate(DisplayEscaped)]
pub enum OptMeasKeyword<'a> {
    Shortname(RefKeyword1<'a, Shortname>),
    NumType(SplitKeyword1<kws::NumType>),
    Optical(OptOpticalKeyword<'a>),
    Temporal(OptTemporalKeyword<'a>),
}

/// Any optional optical keyword type
#[derive(Clone, From, Delegate)]
#[delegate(AsStdKeywordPair)]
#[delegate(DisplayEscaped)]
#[delegate(HasMembership)]
#[cfg_attr(feature = "serde", delegate(AsHeader))]
pub enum OptOpticalKeyword<'a> {
    Longname(NEStringKeyword1<'a, kws::Longname>),
    Filter(NEStringKeyword1<'a, kws::Filter>),
    DetectorType(NEStringKeyword1<'a, kws::DetectorType>),
    DetectorName(NEStringKeyword1<'a, kws::DetectorName>),
    Tag(NEStringKeyword1<'a, kws::Tag>),
    Analyte(NEStringKeyword1<'a, kws::Analyte>),
    OpticalType(NEStringKeyword1<'a, kws::OpticalType>),
    Wavelengths(SplitKeyword<DKey1<kws::Wavelengths>, kws::NEWavelengths<'a>>),
    Scale(SplitKeyword1<kws::Scale>),
    Power(SplitKeyword1<kws::Power>),
    PercentEmitted(SplitKeyword1<kws::PercentEmitted>),
    DetectorVoltage(SplitKeyword1<kws::DetectorVoltage>),
    Gain(SplitKeyword1<kws::Gain>),
    Wavelength(SplitKeyword1<kws::Wavelength>),
    Display(SplitKeyword1<kws::Display>),
    Feature(RefKeyword1<'a, kws::Feature>),
    Calibration3_1(RefKeyword1<'a, kws::Calibration3_1>),
    Calibration3_2(RefKeyword1<'a, kws::Calibration3_2>),
    Peak(OptPeakKeyword),
}

/// Any optional temporal keyword type
#[derive(Clone, From, Delegate)]
#[delegate(AsStdKeywordPair)]
#[delegate(DisplayEscaped)]
#[delegate(HasMembership)]
pub enum OptTemporalKeyword<'a> {
    Longname(NEStringKeyword1<'a, kws::Longname>),
    TemporalType(OptZSTKeyword1<kws::TemporalType, kws::TemporalTypeInner>),
    TemporalScale2_0(OptZSTKeyword1<kws::TemporalScale2_0, kws::TemporalScaleInner>),
    Display(SplitKeyword1<kws::Display>),
    Timestep(SplitKeyword0<kws::Timestep>),
    Peak(OptPeakKeyword),
}

/// Any $PK*n keyword type
#[derive(Clone, From, Delegate)]
#[delegate(AsStdKeywordPair)]
#[delegate(DisplayEscaped)]
#[delegate(HasMembership)]
#[cfg_attr(feature = "serde", delegate(AsHeader))]
pub enum OptPeakKeyword {
    PeakBin(SplitKeyword1<kws::PeakBin>),
    PeakIndex(SplitKeyword1<kws::PeakIndex>),
}

/// Any $Gn* keyword type
#[derive(Clone, From, Delegate)]
#[delegate(AsStdKeywordPair)]
#[delegate(DisplayEscaped)]
#[delegate(HasMembership)]
pub enum GateMeasKeyword<'a> {
    Scale(SplitKeyword1<kws::GateScale>),
    Shortname(RefKeyword1<'a, kws::GateShortname>),
    PercentEmitted(SplitKeyword1<kws::GatePercentEmitted>),
    Range(RefKeyword1<'a, kws::GateRange>),
    DetectorVoltage(SplitKeyword1<kws::GateDetectorVoltage>),
    Filter(NEStringKeyword1<'a, kws::GateFilter>),
    Longname(NEStringKeyword1<'a, kws::GateLongname>),
    DetectorType(NEStringKeyword1<'a, kws::GateDetectorType>),
}

/// Any $Rn* keyword type
#[derive(Clone, From, Delegate)]
#[delegate(AsStdKeywordPair)]
#[delegate(DisplayEscaped)]
#[delegate(HasMembership)]
pub enum RegionKeyword<'a> {
    GateIndex2_0(SplitKeyword1<kws::RegionGateIndex2_0>),
    GateIndex3_0(SplitKeyword1<kws::RegionGateIndex3_0>),
    GateIndex3_2(SplitKeyword1<kws::RegionGateIndex3_2>),
    Window(RegionWindowSplitKeyword<'a>),
}

/// A non-standard keyword.
pub(crate) type NonStdKeyword<'a> = SplitKeyword<&'a NonStdKey, &'a NEStr>;

/// A keyword-value pair as two individual types.
#[derive(Clone, new)]
pub struct SplitKeyword<K, V> {
    pub(crate) key: K,
    pub(crate) value: V,
}

pub type SplitKeyword0<T> = SplitKeyword<DKey0<T>, T>;
pub type SplitKeyword1<T> = SplitKeyword<DKey1<T>, T>;
pub type SplitKeyword2<T> = SplitKeyword<DKey2<T>, T>;

pub type RefKeyword0<'a, T> = SplitKeyword<DKey0<T>, &'a T>;
pub type RefKeyword1<'a, T> = SplitKeyword<DKey1<T>, &'a T>;

pub type OptZSTKeyword1<K, T> = SplitKeyword<DKey1<K>, T>;

pub type NEStringKeyword0<'a, T> = NEStringKeyword<'a, DKey0<T>>;
pub type NEStringKeyword1<'a, T> = NEStringKeyword<'a, DKey1<T>>;

pub type NonZeroU32Keyword0<T> = NonZeroU32Keyword<DKey0<T>>;

pub type NEStringKeyword<'a, K> = SplitKeyword<K, &'a NEStr>;
pub type NonZeroU32Keyword<K> = SplitKeyword<K, NonZeroU32>;

pub type RegionWindowSplitKeyword<'a> =
    SplitKeyword<DKey1<kws::RegionWindow>, kws::RegionWindowRef<'a>>;

/// Error when a metaroot keyword will be lost when converting versions
#[derive(From, Display, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum AnyMetarootKeyLossError {
    Cytsn(Key0LossError<kws::Cytsn>),
    Unicode(Key0LossError<kws::Unicode>),
    Vol(Key0LossError<kws::Vol>),
    Flowrate(Key0LossError<kws::Flowrate>),
    Comp2_0(Key2LossError<kws::Dfc>),
    Comp3_0(Key0LossError<kws::Compensation3_0>),
    Spillover(Key0LossError<Spillover>),
    Begin(Key0LossError<BeginDateTime>),
    End(Key0LossError<EndDateTime>),
    Bits(Key0LossError<kws::CSVBits>),
    Tot(Key0LossError<kws::CSTot>),
    CSMode(Key0LossError<kws::CSMode>),
    CSVFlag(Key1LossError<kws::CSVFlag>),
    Carrierid(Key0LossError<kws::Carrierid>),
    Locationid(Key0LossError<kws::Locationid>),
    Carriertype(Key0LossError<kws::Carriertype>),
    Platename(Key0LossError<kws::Platename>),
    Plateid(Key0LossError<kws::Plateid>),
    Wellid(Key0LossError<kws::Wellid>),
    LastModifier(Key0LossError<kws::LastModifier>),
    LastModified(Key0LossError<kws::LastModified>),
    Originality(Key0LossError<kws::Originality>),
    UnstainedCenters(Key0LossError<kws::UnstainedCenters>),
    UnstainedInfo(Key0LossError<kws::UnstainedInfo>),
    Gate(Key0LossError<kws::Gate>),
    GateScale(Key1LossError<kws::GateScale>),
    GateFilter(Key1LossError<kws::GateFilter>),
    GateShortname(Key1LossError<kws::GateShortname>),
    GatePEmit(Key1LossError<kws::GatePercentEmitted>),
    GateRange(Key1LossError<kws::GateRange>),
    GateLongname(Key1LossError<kws::GateLongname>),
    GateDetType(Key1LossError<kws::GateDetectorType>),
    GateDetVolt(Key1LossError<kws::GateDetectorVoltage>),
    Region(RegionLossError),
    Gating(GatingLossError),
}

/// Error when $RnW/$RnI keyword must be dropped due to reference incompatibility.
///
/// This will only happen when converting between 2.0 and 3.2.
#[derive(Debug, Error, new)]
#[error(
    "$R{index}{region_type} keyword must be dropped as it refers to ${kw_type}n* \
     keywords which is incompatible with version {ver}",
    region_type = if self.is_index { "I" } else { "W" },
    kw_type = if self.current_is_2_0 { "G" } else { "P" },
    ver = if self.current_is_2_0 { Version::FCS3_2 } else { Version::FCS2_0 },
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConversionError))]
pub struct RegionLossError {
    current_is_2_0: bool,
    is_index: bool,
    index: RegionIndex,
}

/// Error when the $GATING keyword must be dropped due to reference incompatibility.
///
/// This will only happen when converting between 2.0 and 3.2.
#[derive(Debug, Error, new)]
#[error(
    "$GATING keyword must be dropped since it refers to $RnI/$RnW keywords which \
     must also be dropped since they refer to ${kw_type}n* keywords which is \
     incompatible with version {ver}",
    kw_type = if self.current_is_2_0 { "G" } else { "P" },
    ver = if self.current_is_2_0 { Version::FCS3_2 } else { Version::FCS2_0 },
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConversionError))]
pub struct GatingLossError {
    current_is_2_0: bool,
}

/// Error when an optical keyword will be lost when converting versions
#[derive(From, Display, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum AnyOpticalKeyLossError {
    MeasType(Key1LossError<kws::OpticalType>),
    Analyte(Key1LossError<kws::Analyte>),
    Tag(Key1LossError<kws::Tag>),
    Gain(GainLossError),
    Display(Key1LossError<kws::Display>),
    DetectorName(Key1LossError<kws::DetectorName>),
    Feature(Key1LossError<kws::Feature>),
    Calibration3_1(Key1LossError<kws::Calibration3_1>),
    Calibration3_2(Key1LossError<kws::Calibration3_2>),
    Peak(PeakLossError),
}

/// Error when a temporal keyword will be lost when converting versions
#[derive(From, Display, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum AnyTemporalKeyLossError {
    TempType(Key1LossError<kws::TemporalType>),
    Display(Key1LossError<kws::Display>),
    Timestamp(TimestepLossError),
    Peak(PeakLossError),
}

/// Error when the $PnG does not exist in target version and is not 1.0.
#[derive(Debug, Error, new)]
#[error(
    "$TIMESTEP does not exist in target version and is currently not 1.0 \
     which means data will be lost on dropping"
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConversionError))]
pub struct TimestepLossError;

/// Error when an optical keyword will be lost when converting to temporal
#[derive(From, Display, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum AnyOpticalToTemporalKeyLossError {
    Filter(Key1LossError<kws::Filter>),
    Power(Key1LossError<kws::Power>),
    DetectorType(Key1LossError<kws::DetectorType>),
    PercentEmitted(Key1LossError<kws::PercentEmitted>),
    DetectorVoltage(Key1LossError<kws::DetectorVoltage>),
    Wavelength(Key1LossError<kws::Wavelength>),
    Wavelengths(Key1LossError<kws::Wavelengths>),
    MeasType(Key1LossError<kws::OpticalType>),
    Analyte(Key1LossError<kws::Analyte>),
    Tag(Key1LossError<kws::Tag>),
    Scale(NonLinearScaleError),
    Gain(NonUnitGainError),
    DetectorName(Key1LossError<kws::DetectorName>),
    Feature(Key1LossError<kws::Feature>),
    Calibration3_1(Key1LossError<kws::Calibration3_1>),
    Calibration3_2(Key1LossError<kws::Calibration3_2>),
}

/// Error when the $PnG is not 1.0 for temporal measurement conversion.
#[derive(Debug, Error)]
#[error("$P{0}E must be linear to allow conversion to temporal measurement")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConversionError))]
pub struct NonLinearScaleError(pub(crate) MeasIndex);

/// Error when the $PnG is not 1.0 for temporal measurement conversion.
#[derive(Debug, Error)]
#[error("$P{0}G must be 1.0 to allow conversion to temporal measurement")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConversionError))]
pub struct NonUnitGainError(pub(crate) MeasIndex);

/// Error when a temporal keyword will be lost when converting to optical
#[derive(From, Display, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum AnyTemporalToOpticalKeyLossError {
    TempType(Key1LossError<kws::TemporalType>),
}

/// Error when $PKn and $PKNn keywords would be lost due to version change
#[derive(From, Display, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum PeakLossError {
    Bin(Key1LossError<kws::PeakBin>),
    Number(Key1LossError<kws::PeakIndex>),
}

/// Error when key would be lost upon conversion
#[derive(Debug, Error, Display)]
#[display(bound(K: fmt::Display))]
#[display("{_0} must be dropped to convert")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConversionError))]
#[cfg_attr(feature = "python", bound(K: fmt::Display))]
pub struct KeyLossError<K>(pub K);

pub type Key0LossError<T> = KeyLossError<DKey0<T>>;
pub type Key1LossError<T> = KeyLossError<DKey1<T>>;
pub type Key2LossError<T> = KeyLossError<DKey2<T>>;

pub(crate) trait Keyword0FromValue<'a> {
    fn from_value<T>(x: T) -> Self
    where
        Self: From<SplitKeyword0<T>>,
    {
        Self::from(SplitKeyword0::from_value0(x))
    }

    fn from_ref<T>(x: &'a T) -> Self
    where
        Self: From<RefKeyword0<'a, T>>,
    {
        Self::from(RefKeyword0::from_ref0(x))
    }

    fn from_str<T>(x: &'a T) -> Option<Self>
    where
        T: AsRef<str>,
        Self: From<NEStringKeyword0<'a, T>>,
    {
        NEStringKeyword0::try_new_ne_str0(x).map(Self::from)
    }
}

pub(crate) trait Keyword1FromValue<'a> {
    fn from_value<T>(x: T, i: impl Into<IndexFromOne>) -> Self
    where
        Self: From<SplitKeyword1<T>>,
    {
        Self::from(SplitKeyword1::from_value1(x, i))
    }

    fn from_ref<T>(x: &'a T, i: impl Into<IndexFromOne>) -> Self
    where
        Self: From<RefKeyword1<'a, T>>,
    {
        Self::from(RefKeyword1::from_ref1(x, i))
    }

    fn from_str<T>(x: &'a T, i: impl Into<IndexFromOne>) -> Option<Self>
    where
        T: AsRef<str>,
        Self: From<NEStringKeyword1<'a, T>>,
    {
        NEStringKeyword1::try_new_ne_str1(x, i).map(Self::from)
    }

    fn from_opt_zst<T, Z>(x: T, i: MeasIndex) -> Option<Self>
    where
        Z: Copy,
        T: AsRef<Option<Z>>,
        Self: From<OptZSTKeyword1<T, Z>>,
    {
        let y: &Option<Z> = x.as_ref();
        let z = y.as_ref().copied()?;
        let ret = SplitKeyword::new(DKey1::<T>::new_i1(i), z);
        Some(Self::from(ret))
    }
}

#[delegatable_trait]
pub(crate) trait HasMembership {
    fn membership(&self) -> VersionMembership;

    fn contains_version(&self, version: Version) -> bool {
        self.membership().contains_version(version)
    }
}

#[cfg(feature = "serde")]
#[delegatable_trait]
pub(crate) trait AsHeader {
    fn std_blank(&self) -> String;
}

#[delegatable_trait]
pub(crate) trait AsStdKeywordPair {
    fn as_std_key_pair(&self) -> (StdKey, NEString);
}

#[delegatable_trait]
pub(crate) trait AsKeywordPair {
    fn as_key_pair(&self) -> (AnyKey, NEString);

    fn as_str_pair(&self) -> (NEString, NEString) {
        let (k, v) = self.as_key_pair();
        (ToNE(k).to_ne_string(), v)
    }
}

#[delegatable_trait]
trait DisplayEscaped {
    fn fmt_escaped(&self, delim: TEXTDelim, f: &mut fmt::Formatter<'_>) -> fmt::Result;
}

impl<T> SplitKeyword0<T> {
    pub(crate) fn from_value0(value: T) -> Self {
        Self::new(DKey0::<T>::default(), value)
    }
}

impl<T> SplitKeyword1<T> {
    pub(crate) fn from_value1(value: T, i: impl Into<IndexFromOne>) -> Self {
        Self::new(DKey1::<T>::new_i1(i.into()), value)
    }
}

impl<'a, T> RefKeyword0<'a, T> {
    pub(crate) fn from_ref0(value: &'a T) -> Self {
        Self::new(DKey0::<T>::default(), value)
    }
}

impl<'a, T> RefKeyword1<'a, T> {
    pub(crate) fn from_ref1(value: &'a T, i: impl Into<IndexFromOne>) -> Self {
        Self::new(DKey1::<T>::new_i1(i.into()), value)
    }
}

impl<'a, T> NEStringKeyword0<'a, T> {
    pub(crate) fn try_new_ne_str0(kw: &'a T) -> Option<Self>
    where
        T: AsRef<str>,
    {
        let value = NEStr::try_new(kw.as_ref())?;
        Some(Self::new(DKey0::<T>::default(), value))
    }
}

impl<'a, T> NEStringKeyword1<'a, T> {
    pub(crate) fn try_new_ne_str1(kw: &'a T, i: impl Into<IndexFromOne>) -> Option<Self>
    where
        T: AsRef<str>,
    {
        let value = NEStr::try_new(kw.as_ref())?;
        Some(Self::new(DKey1::<T>::new_i1(i.into()), value))
    }
}

impl<T> NonZeroU32Keyword0<T> {
    pub(crate) fn try_new_nz_u32(kw: &T) -> Option<Self>
    where
        T: AsRef<u32>,
    {
        let value = NonZeroU32::new(*kw.as_ref())?;
        Some(Self::new(DKey0::<T>::default(), value))
    }
}

impl<'a> OptRootKeyword<'a> {
    pub(crate) fn from_u32<T>(x: &T) -> Option<Self>
    where
        T: AsRef<u32>,
        Self: From<NonZeroU32Keyword0<T>>,
    {
        NonZeroU32Keyword0::try_new_nz_u32(x).map(Self::from)
    }

    pub(crate) fn from_unstainedcenters(x: &'a kws::UnstainedCenters) -> Option<Self> {
        Some(Self::from(SplitKeyword::new(DKey0::default(), x.try_ne()?)))
    }
}

impl<'a> OptOpticalKeyword<'a> {
    pub(crate) fn from_wavelengths(x: &'a kws::Wavelengths, i: MeasIndex) -> Option<Self> {
        let ret = SplitKeyword::new(DKey1::new_i1(i), x.try_ne()?);
        Some(Self::from(ret))
    }
}

impl OptTemporalKeyword<'_> {
    pub(crate) fn from_timestep(x: kws::Timestep) -> Self {
        let ret = SplitKeyword::new(DKey0::default(), x);
        Self::from(ret)
    }
}

impl Keyword0FromValue<'_> for OffsetKeyword {}
impl<'a> Keyword0FromValue<'a> for ReqRootKeyword<'a> {}
impl<'a> Keyword0FromValue<'a> for OptRootKeyword<'a> {}

impl<'a> Keyword1FromValue<'a> for ReqMeasKeyword<'a> {}
impl<'a> Keyword1FromValue<'a> for OptMeasKeyword<'a> {}
impl<'a> Keyword1FromValue<'a> for OptOpticalKeyword<'a> {}
impl<'a> Keyword1FromValue<'a> for OptTemporalKeyword<'a> {}
impl Keyword1FromValue<'_> for OptPeakKeyword {}
impl<'a> Keyword1FromValue<'a> for GateMeasKeyword<'a> {}
impl Keyword1FromValue<'_> for RegionKeyword<'_> {}

impl<K, V> AsStdKeywordPair for SplitKeyword<K, V>
where
    K: AsStdKey,
    for<'a> V: ToDisplayNE<'a>,
{
    fn as_std_key_pair(&self) -> (StdKey, NEString) {
        (self.key.as_std_key(), ToNE(&self.value).to_ne_string())
    }
}

impl<T: AsStdKeywordPair> AsKeywordPair for T {
    fn as_key_pair(&self) -> (AnyKey, NEString) {
        let (k, v) = self.as_std_key_pair();
        (k.into(), v)
    }
}

impl AsKeywordPair for NonStdKeyword<'_> {
    fn as_key_pair(&self) -> (AnyKey, NEString) {
        (self.key.clone().into(), ToNE(&self.value).to_ne_string())
    }
}

impl<I, V: VersionedKey, X> HasMembership for SplitKeyword<DollarKey<V, I>, X> {
    fn membership(&self) -> VersionMembership {
        V::VERS
    }
}

#[cfg(feature = "serde")]
impl<I, V: IndexedKey, X> AsHeader for SplitKeyword<DollarKey<V, I>, X> {
    fn std_blank(&self) -> String {
        V::std_blank()
    }
}

impl<I, V: HasDelim> HasDelim for SplitKeyword<DollarKey<V, I>, V> {
    fn has_delim(&self, d: TEXTDelim) -> Option<DelimCollisionError> {
        self.value.has_delim(d)
    }
}

impl<I, V: HasDelim> HasDelim for SplitKeyword<DollarKey<V, I>, &V> {
    fn has_delim(&self, d: TEXTDelim) -> Option<DelimCollisionError> {
        self.value.has_delim(d)
    }
}

impl HasDelim for ReqRootKeyword<'_> {
    fn has_delim(&self, d: TEXTDelim) -> Option<DelimCollisionError> {
        if let Self::Cyt(x) = self {
            x.has_delim(d)
        } else {
            None
        }
    }
}

impl HasDelim for ReqMeasKeyword<'_> {
    fn has_delim(&self, d: TEXTDelim) -> Option<DelimCollisionError> {
        if let Self::Shortname(x) = self {
            x.has_delim(d)
        } else {
            None
        }
    }
}

impl HasDelim for NonStdKeyword<'_> {
    fn has_delim(&self, d: TEXTDelim) -> Option<DelimCollisionError> {
        self.key.has_delim(d).or(self.value.has_delim(d))
    }
}

impl HasDelim for OptRootKeyword<'_> {
    fn has_delim(&self, d: TEXTDelim) -> Option<DelimCollisionError> {
        match self {
            Self::GateMeas(x) => x.has_delim(d),
            Self::Unicode(x) => x.has_delim(d),
            Self::Tr(x) => x.has_delim(d),
            Self::Spillover(x) => x.has_delim(d),
            Self::Cyt(x) => x.value.has_delim(d),
            Self::Cytsn(x) => x.value.has_delim(d),
            Self::Com(x) => x.value.has_delim(d),
            Self::Cells(x) => x.value.has_delim(d),
            Self::Exp(x) => x.value.has_delim(d),
            Self::Fil(x) => x.value.has_delim(d),
            Self::Inst(x) => x.value.has_delim(d),
            Self::Op(x) => x.value.has_delim(d),
            Self::Proj(x) => x.value.has_delim(d),
            Self::Smno(x) => x.value.has_delim(d),
            Self::Src(x) => x.value.has_delim(d),
            Self::Sys(x) => x.value.has_delim(d),
            Self::Flowrate(x) => x.value.has_delim(d),
            Self::LastModifier(x) => x.value.has_delim(d),
            Self::UnstainedInfo(x) => x.value.has_delim(d),
            Self::Carrierid(x) => x.value.has_delim(d),
            Self::Carriertype(x) => x.value.has_delim(d),
            Self::Locationid(x) => x.value.has_delim(d),
            Self::Plateid(x) => x.value.has_delim(d),
            Self::Platename(x) => x.value.has_delim(d),
            Self::Wellid(x) => x.value.has_delim(d),
            _ => None,
        }
    }
}

impl HasDelim for OptMeasKeyword<'_> {
    fn has_delim(&self, d: TEXTDelim) -> Option<DelimCollisionError> {
        match self {
            Self::Shortname(x) => x.value.has_delim(d),
            Self::Optical(x) => x.has_delim(d),
            Self::Temporal(x) => x.has_delim(d),
            Self::NumType(_) => None,
        }
    }
}

impl HasDelim for OptOpticalKeyword<'_> {
    fn has_delim(&self, d: TEXTDelim) -> Option<DelimCollisionError> {
        match self {
            Self::Feature(x) => x.has_delim(d),
            Self::Calibration3_1(x) => x.has_delim(d),
            Self::Calibration3_2(x) => x.has_delim(d),
            Self::Longname(x) => x.value.has_delim(d),
            Self::Filter(x) => x.value.has_delim(d),
            Self::DetectorType(x) => x.value.has_delim(d),
            Self::DetectorName(x) => x.value.has_delim(d),
            Self::Tag(x) => x.value.has_delim(d),
            Self::Analyte(x) => x.value.has_delim(d),
            Self::OpticalType(x) => x.value.has_delim(d),
            _ => None,
        }
    }
}

impl HasDelim for OptTemporalKeyword<'_> {
    fn has_delim(&self, d: TEXTDelim) -> Option<DelimCollisionError> {
        if let Self::Longname(x) = self {
            x.value.has_delim(d)
        } else {
            None
        }
    }
}

impl HasDelim for GateMeasKeyword<'_> {
    fn has_delim(&self, d: TEXTDelim) -> Option<DelimCollisionError> {
        match self {
            Self::Filter(x) => x.value.has_delim(d),
            Self::Longname(x) => x.value.has_delim(d),
            Self::DetectorType(x) => x.value.has_delim(d),
            Self::Shortname(x) => x.value.has_delim(d),
            _ => None,
        }
    }
}

impl OptRootKeyword<'_> {
    pub(crate) fn as_loss_error(
        &self,
        current_version: Version,
        target_version: Version,
    ) -> Option<AnyMetarootKeyLossError> {
        let go_region = |kw: &RegionKeyword<'_>| {
            let (i, is_index) = match kw {
                // If $RnI in refers to $Gn* and $Pn* in 2.0 and 3.2
                // respectively, which means they are totally incompatible and
                // can be dropped outright
                RegionKeyword::GateIndex2_0(k) => (k.key.index(), true),
                RegionKeyword::GateIndex3_2(k) => (k.key.index(), true),
                // $RnI in 3.0/3.1 is different since they can refer to either
                // $Gn* or $Pn*; It is easier to simply deal with these when
                // trying to transform the version of the entire gating object
                // since the regions need to be rewritten anyways to keep
                // regions which still have valid links.
                RegionKeyword::GateIndex3_0(_) => return None,
                // $RnW follows the same pattern as above since it is always
                // paired with an $RnI, and pairs with matching 'n' must be
                // dropped together.
                RegionKeyword::Window(k) => match current_version {
                    Version::FCS2_0 | Version::FCS3_2 => (k.key.index(), false),
                    _ => return None,
                },
            };
            let is_2_0 = current_version == Version::FCS2_0;
            let match_target = if is_2_0 {
                Version::FCS3_2
            } else {
                Version::FCS2_0
            };
            (target_version == match_target)
                .then_some(RegionLossError::new(is_2_0, is_index, i.into()).into())
        };
        let ret = match self {
            Self::GateMeas(kw) => match kw {
                GateMeasKeyword::Scale(x) => KeyLossError(x.key).into(),
                GateMeasKeyword::Shortname(x) => KeyLossError(x.key).into(),
                GateMeasKeyword::PercentEmitted(x) => KeyLossError(x.key).into(),
                GateMeasKeyword::Range(x) => KeyLossError(x.key).into(),
                GateMeasKeyword::DetectorVoltage(x) => KeyLossError(x.key).into(),
                GateMeasKeyword::Filter(x) => KeyLossError(x.key).into(),
                GateMeasKeyword::Longname(x) => KeyLossError(x.key).into(),
                GateMeasKeyword::DetectorType(x) => KeyLossError(x.key).into(),
            },
            Self::Gate(kw) => KeyLossError(kw.key).into(),
            Self::Dfc(kw) => KeyLossError(kw.key).into(),
            Self::UnstainedCenters(kw) => KeyLossError(kw.key).into(),
            Self::UnstainedInfo(kw) => KeyLossError(kw.key).into(),
            Self::CSMode(kw) => KeyLossError(kw.key).into(),
            Self::CSVFlag(kw) => KeyLossError(kw.key).into(),
            Self::CSVBits(kw) => KeyLossError(kw.key).into(),
            Self::CSTot(kw) => KeyLossError(kw.key).into(),
            Self::Begindatetime(kw) => KeyLossError(kw.key).into(),
            Self::Enddatetime(kw) => KeyLossError(kw.key).into(),
            Self::Comp(kw) => KeyLossError(kw.key).into(),
            Self::Unicode(kw) => KeyLossError(kw.key).into(),
            Self::Vol(kw) => KeyLossError(kw.key).into(),
            Self::LastModified(kw) => KeyLossError(kw.key).into(),
            Self::Originality(kw) => KeyLossError(kw.key).into(),
            Self::LastModifier(kw) => KeyLossError(kw.key).into(),
            Self::Spillover(kw) => KeyLossError(kw.key).into(),
            Self::Flowrate(kw) => KeyLossError(kw.key).into(),
            Self::Carrierid(kw) => KeyLossError(kw.key).into(),
            Self::Carriertype(kw) => KeyLossError(kw.key).into(),
            Self::Locationid(kw) => KeyLossError(kw.key).into(),
            Self::Plateid(kw) => KeyLossError(kw.key).into(),
            Self::Platename(kw) => KeyLossError(kw.key).into(),
            Self::Wellid(kw) => KeyLossError(kw.key).into(),
            Self::Cytsn(kw) => KeyLossError(kw.key).into(),
            Self::GateRegion(kw) => return go_region(kw),
            // $GATING follows the same pattern as $RnI/$RnW above
            Self::Gating(_) => match (current_version, target_version) {
                (Version::FCS2_0, Version::FCS3_2) | (Version::FCS3_2, Version::FCS2_0) => {
                    let is_2_0 = current_version == Version::FCS2_0;
                    GatingLossError::new(is_2_0).into()
                }
                _ => return None,
            },
            // All of these are shared b/t versions and therefore cannot cause
            // loss when converting. Note $MODE is valid in all versions but its
            // value is constrained in 3.2; this is dealt with elsewhere
            Self::Mode3_2(_)
            | Self::Btim2_0(_)
            | Self::Btim3_0(_)
            | Self::Btim3_1(_)
            | Self::Etim2_0(_)
            | Self::Etim3_0(_)
            | Self::Etim3_1(_)
            | Self::Date(_)
            | Self::Abrt(_)
            | Self::Lost(_)
            | Self::Tr(_)
            | Self::Cyt(_)
            | Self::Com(_)
            | Self::Cells(_)
            | Self::Exp(_)
            | Self::Fil(_)
            | Self::Inst(_)
            | Self::Op(_)
            | Self::Proj(_)
            | Self::Smno(_)
            | Self::Src(_)
            | Self::Sys(_) => return None,
        };
        Some(ret)
    }
}

impl OptOpticalKeyword<'_> {
    pub(crate) fn as_loss_error(&self) -> Option<AnyOpticalKeyLossError> {
        let ret = match self {
            Self::DetectorName(kw) => KeyLossError(kw.key).into(),
            Self::Tag(kw) => KeyLossError(kw.key).into(),
            Self::Analyte(kw) => KeyLossError(kw.key).into(),
            Self::OpticalType(kw) => KeyLossError(kw.key).into(),
            Self::Display(kw) => KeyLossError(kw.key).into(),
            Self::Feature(kw) => KeyLossError(kw.key).into(),
            Self::Calibration3_1(kw) => KeyLossError(kw.key).into(),
            Self::Calibration3_2(kw) => KeyLossError(kw.key).into(),
            Self::Peak(kw) => {
                let ret = match kw {
                    OptPeakKeyword::PeakBin(k) => PeakLossError::from(KeyLossError(k.key)),
                    OptPeakKeyword::PeakIndex(k) => PeakLossError::from(KeyLossError(k.key)),
                };
                ret.into()
            }
            // These are shared b/t all versions so cannot result in loss when
            // converting, or they are dealt with elsewhere as follows:
            // * Wavelengths: error emitted when converting from vector
            //   (3.1/3.2) to scaler (2.0/3.0), done when converting
            //   Wavelengths -> Wavelength
            // * Gain: error emitted when going to 2.0 and the value is not 1.0,
            //   done when mapping ScaleTransform -> Scale.
            Self::Wavelength(_)
            | Self::Wavelengths(_)
            | Self::Filter(_)
            | Self::DetectorType(_)
            | Self::Power(_)
            | Self::PercentEmitted(_)
            | Self::DetectorVoltage(_)
            | Self::Longname(_)
            | Self::Gain(_)
            | Self::Scale(_) => return None,
        };
        Some(ret)
    }

    pub(crate) fn as_temporal_loss_error(&self) -> Option<AnyOpticalToTemporalKeyLossError> {
        let ret = match self {
            Self::DetectorName(kw) => KeyLossError(kw.key).into(),
            Self::Tag(kw) => KeyLossError(kw.key).into(),
            Self::Analyte(kw) => KeyLossError(kw.key).into(),
            Self::OpticalType(kw) => KeyLossError(kw.key).into(),
            Self::Feature(kw) => KeyLossError(kw.key).into(),
            Self::Calibration3_1(kw) => KeyLossError(kw.key).into(),
            Self::Calibration3_2(kw) => KeyLossError(kw.key).into(),
            Self::Wavelength(kw) => KeyLossError(kw.key).into(),
            Self::Wavelengths(kw) => KeyLossError(kw.key).into(),
            Self::Filter(kw) => KeyLossError(kw.key).into(),
            Self::DetectorType(kw) => KeyLossError(kw.key).into(),
            Self::Power(kw) => KeyLossError(kw.key).into(),
            Self::PercentEmitted(kw) => KeyLossError(kw.key).into(),
            Self::DetectorVoltage(kw) => KeyLossError(kw.key).into(),
            // $PnE and $PnG are dealt with here because the temporal structs
            // don't actually hold anything for scale and gain since these values
            // are always the same.
            //
            // $PnE must always be linear for temporal measurement
            Self::Scale(kw) => {
                let i = kw.key.index().into();
                return (!matches!(kw.value, kws::Scale::Linear))
                    .then_some(NonLinearScaleError(i).into());
            }
            // $PnG must be 1.0 if it exists since temporal measurement does not
            // have gain
            Self::Gain(kw) => {
                let i = kw.key.index().into();
                return (!kw.value.0.is_one()).then_some(NonUnitGainError(i).into());
            }
            // These are shared b/t temporal and optical so cannot result in
            // loss.
            Self::Peak(_) | Self::Display(_) | Self::Longname(_) => return None,
        };
        Some(ret)
    }
}

impl OptTemporalKeyword<'_> {
    pub(crate) fn as_loss_error(&self) -> Option<AnyTemporalKeyLossError> {
        let ret = match self {
            Self::TemporalType(kw) => KeyLossError(kw.key).into(),
            Self::Display(kw) => KeyLossError(kw.key).into(),
            Self::Peak(kw) => {
                let ret = match kw {
                    OptPeakKeyword::PeakBin(k) => PeakLossError::from(KeyLossError(k.key)),
                    OptPeakKeyword::PeakIndex(k) => PeakLossError::from(KeyLossError(k.key)),
                };
                ret.into()
            }
            // $TIMESTEP is only lossy if not one since it is implied to be
            // one if not in target version
            Self::Timestep(kw) => {
                return (!kw.value.0.is_one()).then_some(TimestepLossError.into());
            }
            // These are shared b/t all versions so cannot result in loss when
            // converting.
            Self::TemporalScale2_0(_) | Self::Longname(_) => return None,
        };
        Some(ret)
    }

    pub(crate) fn as_optical_loss_error(&self) -> Option<AnyTemporalToOpticalKeyLossError> {
        let ret = match self {
            Self::TemporalType(kw) => KeyLossError(kw.key).into(),
            // These are shared with optical so cannot result in loss. $TIMESTEP
            // is dealt with separately since it is usually either moved to a
            // new measurement or returned and thus is not lossy.
            Self::Display(_)
            | Self::Peak(_)
            | Self::Timestep(_)
            | Self::TemporalScale2_0(_)
            | Self::Longname(_) => return None,
        };
        Some(ret)
    }
}

/// A type which may be escaped when written as a string.
#[derive(new)]
pub(crate) struct Escaped<T> {
    delim: TEXTDelim,
    inner: T,
}

impl<T> Escaped<T> {
    pub(crate) fn write_str(&self, buf: &mut NEString)
    where
        Self: fmt::Display,
    {
        write!(buf, "{self}").expect("str write should be infallible");
    }
}

impl<T: DisplayEscaped + ?Sized> fmt::Display for Escaped<&T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt_escaped(self.delim, f)
    }
}

struct EscapedFormatter<'a, 'b> {
    delim: TEXTDelim,
    inner: &'a mut fmt::Formatter<'b>,
}

impl EscapedFormatter<'_, '_> {
    fn write_with_delim<V>(&mut self, v: &V, escape: bool) -> fmt::Result
    where
        V: ?Sized + for<'a> ToDisplayNE<'a>,
    {
        let delim = self.delim;
        let w = v.as_displayable();
        if escape {
            write!(self, "{w}")?;
            write!(self.inner, "{delim}")
        } else {
            write!(self.inner, "{w}{delim}")
        }
    }
}

impl fmt::Write for EscapedFormatter<'_, '_> {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        let d = self.delim;
        // Check if delim is in str before trying to escape it. This is
        // a massive optimization since encoding and decoding to chars
        // on the fly is extremely expensive as opposed to checking if
        // any single byte in the string is equal to some value.
        if s.contains(char::from(d)) {
            for c in s.bytes() {
                if c == u8::from(d) {
                    // if delimiter found, write it twice
                    write!(self.inner, "{x}{x}", x = self.delim)?;
                } else {
                    // otherwise write non-delim once
                    self.inner.write_char(char::from(c))?;
                }
            }
        } else {
            self.inner.write_str(s)?;
        }
        Ok(())
    }
}

impl<K, I, V> DisplayEscaped for SplitKeyword<DollarKey<K, I>, V>
where
    for<'a> DollarKey<K, I>: ToDisplayNE<'a>,
    for<'a> V: ToDisplayNE<'a>,
{
    fn fmt_escaped(&self, delim: TEXTDelim, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut xf = EscapedFormatter { delim, inner: f };
        // ASSUME standard keys don't need to be escaped because the delim
        // character is 0-31 which never appears in the standard keys
        xf.write_with_delim(&self.key, false)?;
        xf.write_with_delim(&self.value, true)?;
        Ok(())
    }
}

impl DisplayEscaped for NonStdKeyword<'_> {
    fn fmt_escaped(&self, delim: TEXTDelim, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut xf = EscapedFormatter { delim, inner: f };
        xf.write_with_delim(self.key, true)?;
        xf.write_with_delim(&self.value, true)?;
        Ok(())
    }
}

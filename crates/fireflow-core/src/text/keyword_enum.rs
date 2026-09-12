//! Wrapper types for keyword values.
//!
//! Used to iterate over keywords without converting to strings first, allowing
//! fast access and easy filtering if required.

use crate::meas::GainLossError;
use crate::std_index::tx::{KeywordAction, StdIndexTx};
use crate::text::datetimes::{BeginDateTime, EndDateTime};
use crate::text::keywords as kws;
use crate::text::spillover::Spillover;
use crate::text::timestamps::FCSDate;
use crate::validated::keys::{DollarKey, DollarKey_, NonStdKey, SpecificKey_, WritableKey};
use crate::validated::shortname::Shortname;

#[cfg(feature = "serde")]
use fireflow_types::std_key::BlankKeyword;
use fireflow_types::{
    index::{MeasIndex, RegionIndex},
    keywords::{Version, VersionMembership},
    nonempty::{DisplayNE as _, DisplayableNE as _, NEStr, NEString, ToDisplayNE, ToNE},
    std_key::StdKey,
    textdelim::{DelimCollisionError, HasDelim, TEXTDelim, ambassador_impl_HasDelim},
};

use ambassador::{Delegate, delegatable_trait};
use derive_more::{Display, From};
use derive_new::new;
use num_traits::One as _;
use thiserror::Error;

use std::fmt::{self, Write as _};
use std::num::NonZeroU32;

#[cfg(feature = "serde")]
use crate::validated::keys::ValueToStdKey;

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr},
    fireflow_types::python as py,
};

/// Any offset keyword type
#[derive(Clone, From, Delegate)]
#[delegate(DisplayEscaped)]
pub(crate) enum OffsetKeyword {
    Nextdata(SplitKeyword<kws::Nextdata>),
    Begindata(SplitKeyword<kws::Begindata>),
    Enddata(SplitKeyword<kws::Enddata>),
    Beginanalysis(SplitKeyword<kws::Beginanalysis>),
    Endanalysis(SplitKeyword<kws::Endanalysis>),
    Beginstext(SplitKeyword<kws::Beginstext>),
    Endstext(SplitKeyword<kws::Endstext>),
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
    Meas(OptMeasKeyword<'a>),
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

/// Any required root keyword type
// TODO this shouldn't need to be pub
#[derive(Clone, From, Delegate)]
#[delegate(AsStdKeywordPair)]
#[delegate(DisplayEscaped)]
pub enum ReqRootKeyword<'a> {
    ByteOrd2_0(SplitKeyword<kws::ByteOrd2_0>),
    ByteOrd3_1(SplitKeyword<kws::ByteOrd3_1>),
    Par(SplitKeyword<kws::Par>),
    Tot(SplitKeyword<kws::Tot>),
    Datatype(SplitKeyword<kws::AlphaNumType>),
    Mode(SplitKeyword<kws::Mode>),
    Cyt(RefKeyword<'a, kws::Cyt3_2>),
}

/// Any optional root keyword type
#[derive(Clone, From, Delegate)]
#[delegate(AsStdKeywordPair)]
#[delegate(DisplayEscaped)]
#[delegate(HasMembership)]
pub enum OptRootKeyword<'a> {
    GateMeas(GateMeasKeyword<'a>),
    GateRegion(RegionKeyword<'a>),
    Dfc(SplitKeyword<kws::Dfc>),
    UnstainedCenters(SplitKeyword_<DollarKey<kws::UnstainedCenters>, kws::NEUnstainedCenters>),
    CSMode(SplitKeyword<kws::CSMode>),
    CSVFlag(SplitKeyword<kws::CSVFlag>),
    CSVBits(NonZeroU32Keyword<kws::CSVBits>),
    CSTot(NonZeroU32Keyword<kws::CSTot>),
    Btim2_0(SplitKeyword<kws::Btim2_0>),
    Btim3_0(SplitKeyword<kws::Btim3_0>),
    Btim3_1(SplitKeyword<kws::Btim3_1>),
    Etim2_0(SplitKeyword<kws::Etim2_0>),
    Etim3_0(SplitKeyword<kws::Etim3_0>),
    Etim3_1(SplitKeyword<kws::Etim3_1>),
    Date(SplitKeyword<FCSDate>),
    Begindatetime(SplitKeyword<BeginDateTime>),
    Enddatetime(SplitKeyword<EndDateTime>),
    Gate(SplitKeyword<kws::Gate>),
    Gating(RefKeyword<'a, kws::Gating>),
    Comp(RefKeyword<'a, kws::Compensation3_0>),
    Unicode(RefKeyword<'a, kws::Unicode>),
    Abrt(SplitKeyword<kws::Abrt>),
    Lost(SplitKeyword<kws::Lost>),
    Tr(RefKeyword<'a, kws::Trigger>),
    Vol(SplitKeyword<kws::Vol>),
    LastModified(SplitKeyword<kws::LastModified>),
    Originality(SplitKeyword<kws::Originality>),
    Mode3_2(SplitKeyword<kws::Mode3_2>),
    Spillover(RefKeyword<'a, Spillover>),
    Cyt(NEStringKeyword<'a, kws::Cyt>),
    Cytsn(NEStringKeyword<'a, kws::Cytsn>),
    Com(NEStringKeyword<'a, kws::Com>),
    Cells(NEStringKeyword<'a, kws::Cells>),
    Exp(NEStringKeyword<'a, kws::Exp>),
    Fil(NEStringKeyword<'a, kws::Fil>),
    Inst(NEStringKeyword<'a, kws::Inst>),
    Op(NEStringKeyword<'a, kws::Op>),
    Proj(NEStringKeyword<'a, kws::Proj>),
    Smno(NEStringKeyword<'a, kws::Smno>),
    Src(NEStringKeyword<'a, kws::Src>),
    Sys(NEStringKeyword<'a, kws::Sys>),
    Flowrate(NEStringKeyword<'a, kws::Flowrate>),
    LastModifier(NEStringKeyword<'a, kws::LastModifier>),
    UnstainedInfo(NEStringKeyword<'a, kws::UnstainedInfo>),
    Carrierid(NEStringKeyword<'a, kws::Carrierid>),
    Carriertype(NEStringKeyword<'a, kws::Carriertype>),
    Locationid(NEStringKeyword<'a, kws::Locationid>),
    Plateid(NEStringKeyword<'a, kws::Plateid>),
    Platename(NEStringKeyword<'a, kws::Platename>),
    Wellid(NEStringKeyword<'a, kws::Wellid>),
}

/// Any required measurement keyword type
#[derive(Clone, From, Delegate)]
#[delegate(AsStdKeywordPair)]
#[delegate(DisplayEscaped)]
#[cfg_attr(feature = "serde", delegate(AsHeader))]
pub enum ReqMeasKeyword<'a> {
    Shortname(RefKeyword<'a, Shortname>),
    Scale(SplitKeyword<kws::Scale>),
    TemporalScale3_0(SplitKeyword<kws::TemporalScale3_0>),
    Width(SplitKeyword<kws::Width>),
    Range(SplitKeyword<kws::TextRange>),
}

/// Any optional measurement keyword type
#[derive(Clone, From, Delegate)]
#[delegate(AsStdKeywordPair)]
#[delegate(DisplayEscaped)]
pub enum OptMeasKeyword<'a> {
    Shortname(RefKeyword<'a, Shortname>),
    NumType(SplitKeyword<kws::NumType>),
    Optical(OptScaledOpticalKeyword<'a>),
    Temporal(OptTemporalKeyword<'a>),
}

/// Any optional optical keyword type
#[derive(Clone, From, Delegate)]
#[delegate(AsStdKeywordPair)]
#[delegate(DisplayEscaped)]
#[delegate(HasMembership)]
#[cfg_attr(feature = "serde", delegate(AsHeader))]
pub enum OptScaledOpticalKeyword<'a> {
    Scale(OptScaleKeyword),
    Optical(OptOpticalKeyword<'a>),
}

/// Any optional scale keyword type
#[derive(Clone, From, Delegate)]
#[delegate(AsStdKeywordPair)]
#[delegate(DisplayEscaped)]
#[delegate(HasMembership)]
#[cfg_attr(feature = "serde", delegate(AsHeader))]
pub enum OptScaleKeyword {
    Scale(SplitKeyword<kws::Scale>),
    Gain(SplitKeyword<kws::Gain>),
}

/// Any optional optical keyword type
#[derive(Clone, From, Delegate)]
#[delegate(AsStdKeywordPair)]
#[delegate(DisplayEscaped)]
#[delegate(HasMembership)]
#[cfg_attr(feature = "serde", delegate(AsHeader))]
pub enum OptOpticalKeyword<'a> {
    Longname(NEStringKeyword<'a, kws::Longname>),
    Filter(NEStringKeyword<'a, kws::Filter>),
    DetectorType(NEStringKeyword<'a, kws::DetectorType>),
    DetectorName(NEStringKeyword<'a, kws::DetectorName>),
    Tag(NEStringKeyword<'a, kws::Tag>),
    Analyte(NEStringKeyword<'a, kws::Analyte>),
    OpticalType(NEStringKeyword<'a, kws::OpticalType>),
    Wavelengths(SplitKeyword_<DollarKey<kws::Wavelengths>, kws::NEWavelengths<'a>>),
    Power(SplitKeyword<kws::Power>),
    PercentEmitted(SplitKeyword<kws::PercentEmitted>),
    DetectorVoltage(SplitKeyword<kws::DetectorVoltage>),
    Wavelength(SplitKeyword<kws::Wavelength>),
    Display(SplitKeyword<kws::Display>),
    Feature(RefKeyword<'a, kws::Feature>),
    Calibration3_1(RefKeyword<'a, kws::Calibration3_1>),
    Calibration3_2(RefKeyword<'a, kws::Calibration3_2>),
    Peak(OptPeakKeyword),
}

/// Any optional temporal keyword type
#[derive(Clone, From, Delegate)]
#[delegate(AsStdKeywordPair)]
#[delegate(DisplayEscaped)]
#[delegate(HasMembership)]
pub enum OptTemporalKeyword<'a> {
    Timestep(SplitKeyword<kws::Timestep>),
    Meas(OptMeasTemporalKeyword<'a>),
}

/// Any optional temporal keyword type (sans TIMESTEP)
#[derive(Clone, From, Delegate)]
#[delegate(AsStdKeywordPair)]
#[delegate(DisplayEscaped)]
#[delegate(HasMembership)]
#[cfg_attr(feature = "serde", delegate(AsHeader))]
pub enum OptMeasTemporalKeyword<'a> {
    Longname(NEStringKeyword<'a, kws::Longname>),
    TemporalType(OptZSTKeyword<kws::TemporalType, kws::TemporalTypeInner>),
    TemporalScale2_0(OptZSTKeyword<kws::TemporalScale2_0, kws::TemporalScaleInner>),
    Display(SplitKeyword<kws::Display>),
    Peak(OptPeakKeyword),
}

/// Any $PK*n keyword type
#[derive(Clone, From, Delegate)]
#[delegate(AsStdKeywordPair)]
#[delegate(DisplayEscaped)]
#[delegate(HasMembership)]
#[cfg_attr(feature = "serde", delegate(AsHeader))]
pub enum OptPeakKeyword {
    PeakBin(SplitKeyword<kws::PeakBin>),
    PeakIndex(SplitKeyword<kws::PeakIndex>),
}

/// Any $Gn* keyword type
#[derive(Clone, From, Delegate)]
#[delegate(AsStdKeywordPair)]
#[delegate(DisplayEscaped)]
#[delegate(HasMembership)]
pub enum GateMeasKeyword<'a> {
    Scale(SplitKeyword<kws::GateScale>),
    Shortname(RefKeyword<'a, kws::GateShortname>),
    PercentEmitted(SplitKeyword<kws::GatePercentEmitted>),
    Range(RefKeyword<'a, kws::GateRange>),
    DetectorVoltage(SplitKeyword<kws::GateDetectorVoltage>),
    Filter(NEStringKeyword<'a, kws::GateFilter>),
    Longname(NEStringKeyword<'a, kws::GateLongname>),
    DetectorType(NEStringKeyword<'a, kws::GateDetectorType>),
}

/// Any $Rn* keyword type
#[derive(Clone, From, Delegate)]
#[delegate(AsStdKeywordPair)]
#[delegate(DisplayEscaped)]
#[delegate(HasMembership)]
pub enum RegionKeyword<'a> {
    GateIndex2_0(SplitKeyword<kws::RegionGateIndex2_0>),
    GateIndex3_0(SplitKeyword<kws::RegionGateIndex3_0>),
    GateIndex3_2(SplitKeyword<kws::RegionGateIndex3_2>),
    Window(RegionWindowSplitKeyword<'a>),
}

/// A non-standard keyword.
pub(crate) type NonStdKeyword<'a> = SplitKeyword_<&'a NonStdKey, &'a NEStr>;

/// A keyword-value pair as two individual types.
#[derive(Clone, new)]
pub struct SplitKeyword_<K, V> {
    pub(crate) key: K,
    pub(crate) value: V,
}

pub type SplitKeyword<T> = SplitKeyword_<DollarKey<T>, T>;

// pub type SplitKeyword0<T> = SplitKeyword<DKey0<T>, T>;
// pub type SplitKeywordMeas<T> = SplitKeyword<SpecificMeasKey<T>, T>;
// pub type SplitKeyword2<T> = SplitKeyword<DKey2<T>, T>;

pub type RefKeyword<'a, T> = SplitKeyword_<DollarKey<T>, &'a T>;

// pub type RefKeyword0<'a, T> = SplitKeyword_<DKey0<T>, &'a T>;
// pub type RefKeyword1<'a, T> = SplitKeyword_<SpecificMeasKey<T>, &'a T>;

pub type OptZSTKeyword<K, T> = SplitKeyword_<DollarKey<K>, T>;
// pub type OptZSTKeyword1<K, T> = SplitKeyword_<SpecificMeasKey<K>, T>;

pub type NEStringKeyword<'a, T> = NEStringKeyword_<'a, DollarKey<T>>;

// pub type NEStringKeyword0<'a, T> = NEStringKeyword<'a, DKey0<T>>;
// pub type NEStringKeyword1<'a, T> = NEStringKeyword<'a, SpecificMeasKey<T>>;

pub type NonZeroU32Keyword<T> = NonZeroU32Keyword_<DollarKey<T>>;

// pub type NonZeroU32Keyword0<T> = NonZeroU32Keyword<DKey0<T>>;

pub type NEStringKeyword_<'a, K> = SplitKeyword_<K, &'a NEStr>;
pub type NonZeroU32Keyword_<K> = SplitKeyword_<K, NonZeroU32>;

pub type RegionWindowSplitKeyword<'a> =
    SplitKeyword_<DollarKey<kws::RegionWindow>, kws::RegionWindowRef<'a>>;

/// Error when a metaroot keyword will be lost when converting versions
#[derive(From, Display, Debug, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum AnyMetarootKeyLossError {
    Cytsn(KeyLossError<kws::Cytsn>),
    Unicode(KeyLossError<kws::Unicode>),
    Vol(KeyLossError<kws::Vol>),
    Flowrate(KeyLossError<kws::Flowrate>),
    Comp2_0(KeyLossError<kws::Dfc>),
    Comp3_0(KeyLossError<kws::Compensation3_0>),
    Spillover(KeyLossError<Spillover>),
    Begin(KeyLossError<BeginDateTime>),
    End(KeyLossError<EndDateTime>),
    Bits(KeyLossError<kws::CSVBits>),
    Tot(KeyLossError<kws::CSTot>),
    CSMode(KeyLossError<kws::CSMode>),
    CSVFlag(KeyLossError<kws::CSVFlag>),
    Carrierid(KeyLossError<kws::Carrierid>),
    Locationid(KeyLossError<kws::Locationid>),
    Carriertype(KeyLossError<kws::Carriertype>),
    Platename(KeyLossError<kws::Platename>),
    Plateid(KeyLossError<kws::Plateid>),
    Wellid(KeyLossError<kws::Wellid>),
    LastModifier(KeyLossError<kws::LastModifier>),
    LastModified(KeyLossError<kws::LastModified>),
    Originality(KeyLossError<kws::Originality>),
    UnstainedCenters(KeyLossError<kws::UnstainedCenters>),
    UnstainedInfo(KeyLossError<kws::UnstainedInfo>),
    Gate(KeyLossError<kws::Gate>),
    GateScale(KeyLossError<kws::GateScale>),
    GateFilter(KeyLossError<kws::GateFilter>),
    GateShortname(KeyLossError<kws::GateShortname>),
    GatePEmit(KeyLossError<kws::GatePercentEmitted>),
    GateRange(KeyLossError<kws::GateRange>),
    GateLongname(KeyLossError<kws::GateLongname>),
    GateDetType(KeyLossError<kws::GateDetectorType>),
    GateDetVolt(KeyLossError<kws::GateDetectorVoltage>),
    Region(RegionLossError),
    Gating(GatingLossError),
}

/// Error when $RnW/$RnI keyword must be dropped due to reference incompatibility.
///
/// This will only happen when converting between 2.0 and 3.2.
#[derive(Debug, Error, new, PartialEq, Clone)]
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
#[derive(Debug, Error, new, PartialEq, Clone)]
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
#[derive(From, Display, Debug, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum AnyOpticalKeyLossError {
    MeasType(KeyLossError<kws::OpticalType>),
    Analyte(KeyLossError<kws::Analyte>),
    Tag(KeyLossError<kws::Tag>),
    Gain(GainLossError),
    Display(KeyLossError<kws::Display>),
    DetectorName(KeyLossError<kws::DetectorName>),
    Feature(KeyLossError<kws::Feature>),
    Calibration3_1(KeyLossError<kws::Calibration3_1>),
    Calibration3_2(KeyLossError<kws::Calibration3_2>),
    Peak(PeakLossError),
}

/// Error when a temporal keyword will be lost when converting versions
#[derive(From, Display, Debug, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum AnyTemporalKeyLossError {
    TempType(KeyLossError<kws::TemporalType>),
    Display(KeyLossError<kws::Display>),
    Timestamp(TimestepLossError),
    Peak(PeakLossError),
}

/// Error when the $PnG does not exist in target version and is not 1.0.
#[derive(Debug, Error, new, PartialEq, Clone)]
#[error(
    "$TIMESTEP does not exist in target version and is currently not 1.0 \
     which means data will be lost on dropping"
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConversionError))]
pub struct TimestepLossError;

/// Error when an optical keyword will be lost when converting to temporal
#[derive(From, Display, Debug, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum AnyOpticalToTemporalKeyLossError {
    Filter(KeyLossError<kws::Filter>),
    Power(KeyLossError<kws::Power>),
    DetectorType(KeyLossError<kws::DetectorType>),
    PercentEmitted(KeyLossError<kws::PercentEmitted>),
    DetectorVoltage(KeyLossError<kws::DetectorVoltage>),
    Wavelength(KeyLossError<kws::Wavelength>),
    Wavelengths(KeyLossError<kws::Wavelengths>),
    MeasType(KeyLossError<kws::OpticalType>),
    Analyte(KeyLossError<kws::Analyte>),
    Tag(KeyLossError<kws::Tag>),
    Scale(NonLinearScaleError),
    Gain(NonUnitGainError),
    DetectorName(KeyLossError<kws::DetectorName>),
    Feature(KeyLossError<kws::Feature>),
    Calibration3_1(KeyLossError<kws::Calibration3_1>),
    Calibration3_2(KeyLossError<kws::Calibration3_2>),
}

/// Error when the $PnG is not 1.0 for temporal measurement conversion.
#[derive(Debug, Error, PartialEq, Clone)]
#[error("$P{0}E must be linear to allow conversion to temporal measurement")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConversionError))]
pub struct NonLinearScaleError(pub(crate) MeasIndex);

/// Error when the $PnG is not 1.0 for temporal measurement conversion.
#[derive(Debug, Error, PartialEq, Clone)]
#[error("$P{0}G must be 1.0 to allow conversion to temporal measurement")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConversionError))]
pub struct NonUnitGainError(pub(crate) MeasIndex);

/// Error when a temporal keyword will be lost when converting to optical
#[derive(From, Display, Debug, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum AnyTemporalToOpticalKeyLossError {
    TempType(KeyLossError<kws::TemporalType>),
}

/// Error when $PKn and $PKNn keywords would be lost due to version change
#[derive(From, Display, Debug, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum PeakLossError {
    Bin(KeyLossError<kws::PeakBin>),
    Number(KeyLossError<kws::PeakIndex>),
}

/// Error when key would be lost upon conversion
#[derive(Debug, Error, Display, PartialEq, Clone)]
#[display(bound(K: fmt::Display))]
#[display("{_0} must be dropped to convert")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConversionError))]
#[cfg_attr(feature = "python", bound(K: fmt::Display))]
pub struct KeyLossError_<K>(pub K);

pub type KeyLossError<T> = KeyLossError_<DollarKey<T>>;
// pub type KeyLossError<T> = KeyLossError<SpecificMeasKey<T>>;
// pub type Key2LossError<T> = KeyLossError<DKey2<T>>;

pub(crate) trait Keyword0FromValue<'a> {
    fn from_value<T>(x: T) -> Self
    where
        T: ValueToStdKey<Index = ()>,
        Self: From<SplitKeyword<T>>,
    {
        Self::from(SplitKeyword::from_value0(x))
    }

    fn from_ref<T>(x: &'a T) -> Self
    where
        T: ValueToStdKey<Index = ()>,
        Self: From<RefKeyword<'a, T>>,
    {
        Self::from(RefKeyword::from_ref0(x))
    }

    fn from_str<T>(x: &'a T) -> Option<Self>
    where
        T: ValueToStdKey<Index = ()> + AsRef<str>,
        Self: From<NEStringKeyword<'a, T>>,
    {
        NEStringKeyword::try_new_ne_str0(x).map(Self::from)
    }
}

pub(crate) trait Keyword1FromValue<'a> {
    fn from_value<T>(x: T, i: T::Index) -> Self
    where
        T: ValueToStdKey,
        Self: From<SplitKeyword<T>>,
    {
        Self::from(SplitKeyword::from_value1(x, i))
    }

    fn from_ref<T>(x: &'a T, i: T::Index) -> Self
    where
        T: ValueToStdKey,
        Self: From<RefKeyword<'a, T>>,
    {
        Self::from(RefKeyword::from_ref1(x, i))
    }

    fn from_str<T>(x: &'a T, i: T::Index) -> Option<Self>
    where
        T: ValueToStdKey + AsRef<str>,
        Self: From<NEStringKeyword<'a, T>>,
    {
        NEStringKeyword::try_new_ne_str1(x, i).map(Self::from)
    }

    fn from_opt_zst<T, Z>(x: T, i: T::Index) -> Option<Self>
    where
        Z: Copy,
        T: ValueToStdKey + AsRef<Option<Z>>,
        Self: From<OptZSTKeyword<T, Z>>,
    {
        let y: &Option<Z> = x.as_ref();
        let z = y.as_ref().copied()?;
        let ret = SplitKeyword_::new(DollarKey::new(i), z);
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
    fn std_blank(&self) -> &'static NEStr;
}

#[delegatable_trait]
pub(crate) trait AsStdKeywordPair: Sized {
    fn as_std_key_pair(&self) -> (StdKey, NEString);

    fn as_std_key(&self) -> StdKey {
        self.as_std_key_pair().0
    }
}

#[delegatable_trait]
pub(crate) trait AsKeywordPair {
    fn as_key_pair(&self) -> (WritableKey, NEString);

    fn as_str_pair(&self) -> (NEString, NEString) {
        let (k, v) = self.as_key_pair();
        (ToNE(k).to_ne_string(), v)
    }
}

#[delegatable_trait]
trait DisplayEscaped {
    fn fmt_escaped(&self, delim: TEXTDelim, f: &mut fmt::Formatter<'_>) -> fmt::Result;
}

impl<T: ValueToStdKey> SplitKeyword<T> {
    pub(crate) fn from_value0(value: T) -> Self
    where
        T: ValueToStdKey<Index = ()>,
    {
        Self::new(DollarKey::<T>::default(), value)
    }

    pub(crate) fn from_value1(value: T, i: T::Index) -> Self {
        Self::new(DollarKey::new(i), value)
    }
}

impl<'a, T: ValueToStdKey> RefKeyword<'a, T> {
    pub(crate) fn from_ref0(value: &'a T) -> Self
    where
        T: ValueToStdKey<Index = ()>,
    {
        Self::new(DollarKey::<T>::default(), value)
    }

    pub(crate) fn from_ref1(value: &'a T, i: T::Index) -> Self {
        Self::new(DollarKey::<T>::new(i), value)
    }
}

impl<'a, T: ValueToStdKey> NEStringKeyword<'a, T> {
    pub(crate) fn try_new_ne_str0(kw: &'a T) -> Option<Self>
    where
        T: ValueToStdKey<Index = ()> + AsRef<str>,
    {
        let value = NEStr::try_new(kw.as_ref())?;
        Some(Self::new(DollarKey::<T>::default(), value))
    }

    pub(crate) fn try_new_ne_str1(kw: &'a T, i: T::Index) -> Option<Self>
    where
        T: AsRef<str>,
    {
        let value = NEStr::try_new(kw.as_ref())?;
        Some(Self::new(DollarKey::<T>::new(i), value))
    }
}

impl<T: ValueToStdKey> NonZeroU32Keyword<T> {
    pub(crate) fn try_new_nz_u32(kw: &T) -> Option<Self>
    where
        T: ValueToStdKey<Index = ()> + AsRef<u32>,
    {
        let value = NonZeroU32::new(*kw.as_ref())?;
        Some(Self::new(DollarKey::<T>::default(), value))
    }
}

impl<'a> OptRootKeyword<'a> {
    pub(crate) fn from_u32<T>(x: &T) -> Option<Self>
    where
        T: ValueToStdKey<Index = ()> + AsRef<u32>,
        Self: From<NonZeroU32Keyword<T>>,
    {
        NonZeroU32Keyword::try_new_nz_u32(x).map(Self::from)
    }

    pub(crate) fn from_unstainedcenters(x: &'a kws::UnstainedCenters) -> Option<Self> {
        Some(Self::from(SplitKeyword_::new(
            DollarKey::default(),
            x.try_ne()?,
        )))
    }
}

impl<'a> OptOpticalKeyword<'a> {
    pub(crate) fn from_wavelengths(x: &'a kws::Wavelengths, i: MeasIndex) -> Option<Self> {
        let ret = SplitKeyword_::new(DollarKey::new(i), x.try_ne()?);
        Some(Self::from(ret))
    }
}

impl Keyword0FromValue<'_> for OffsetKeyword {}
impl<'a> Keyword0FromValue<'a> for ReqRootKeyword<'a> {}
impl<'a> Keyword0FromValue<'a> for OptRootKeyword<'a> {}

impl<'a> Keyword1FromValue<'a> for ReqMeasKeyword<'a> {}
impl<'a> Keyword1FromValue<'a> for OptMeasKeyword<'a> {}
impl<'a> Keyword1FromValue<'a> for OptOpticalKeyword<'a> {}
impl Keyword1FromValue<'_> for OptScaleKeyword {}
impl<'a> Keyword1FromValue<'a> for OptMeasTemporalKeyword<'a> {}
impl Keyword1FromValue<'_> for OptPeakKeyword {}
impl<'a> Keyword1FromValue<'a> for GateMeasKeyword<'a> {}
impl Keyword1FromValue<'_> for RegionKeyword<'_> {}

impl<T: ValueToStdKey, V> AsStdKeywordPair for SplitKeyword_<DollarKey<T>, V>
where
    for<'a> V: ToDisplayNE<'a>,
{
    fn as_std_key_pair(&self) -> (StdKey, NEString) {
        (StdKey::from(&self.key.0), ToNE(&self.value).to_ne_string())
    }
}

impl<T: AsStdKeywordPair> AsKeywordPair for T {
    fn as_key_pair(&self) -> (WritableKey, NEString) {
        let (k, v) = self.as_std_key_pair();
        (k.into(), v)
    }
}

impl AsKeywordPair for NonStdKeyword<'_> {
    fn as_key_pair(&self) -> (WritableKey, NEString) {
        (self.key.clone().into(), ToNE(&self.value).to_ne_string())
    }
}

impl<I, V, X> HasMembership for SplitKeyword_<DollarKey_<V, I>, X>
where
    SpecificKey_<V, I>: Into<StdKey> + Copy,
{
    fn membership(&self) -> VersionMembership {
        self.key.0.into().membership()
    }
}

#[cfg(feature = "serde")]
impl<V: ValueToStdKey, X> AsHeader for SplitKeyword_<DollarKey<V>, X>
where
    V::Id: BlankKeyword,
{
    fn std_blank(&self) -> &'static NEStr {
        V::STD.blank()
    }
}

impl<I, V: HasDelim> HasDelim for SplitKeyword_<DollarKey_<V, I>, V> {
    fn has_delim(&self, d: TEXTDelim) -> Option<DelimCollisionError> {
        self.value.has_delim(d)
    }
}

impl<I, V: HasDelim> HasDelim for SplitKeyword_<DollarKey_<V, I>, &V> {
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

impl HasDelim for OptScaledOpticalKeyword<'_> {
    fn has_delim(&self, d: TEXTDelim) -> Option<DelimCollisionError> {
        match self {
            Self::Optical(x) => x.has_delim(d),
            Self::Scale(_) => None,
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
        if let Self::Meas(x) = self {
            x.has_delim(d)
        } else {
            None
        }
    }
}

impl HasDelim for OptMeasTemporalKeyword<'_> {
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
                GateMeasKeyword::Scale(x) => KeyLossError_(x.key).into(),
                GateMeasKeyword::Shortname(x) => KeyLossError_(x.key).into(),
                GateMeasKeyword::PercentEmitted(x) => KeyLossError_(x.key).into(),
                GateMeasKeyword::Range(x) => KeyLossError_(x.key).into(),
                GateMeasKeyword::DetectorVoltage(x) => KeyLossError_(x.key).into(),
                GateMeasKeyword::Filter(x) => KeyLossError_(x.key).into(),
                GateMeasKeyword::Longname(x) => KeyLossError_(x.key).into(),
                GateMeasKeyword::DetectorType(x) => KeyLossError_(x.key).into(),
            },
            Self::Gate(kw) => KeyLossError_(kw.key).into(),
            Self::Dfc(kw) => KeyLossError_(kw.key).into(),
            Self::UnstainedCenters(kw) => KeyLossError_(kw.key).into(),
            Self::UnstainedInfo(kw) => KeyLossError_(kw.key).into(),
            Self::CSMode(kw) => KeyLossError_(kw.key).into(),
            Self::CSVFlag(kw) => KeyLossError_(kw.key).into(),
            Self::CSVBits(kw) => KeyLossError_(kw.key).into(),
            Self::CSTot(kw) => KeyLossError_(kw.key).into(),
            Self::Begindatetime(kw) => KeyLossError_(kw.key).into(),
            Self::Enddatetime(kw) => KeyLossError_(kw.key).into(),
            Self::Comp(kw) => KeyLossError_(kw.key).into(),
            Self::Unicode(kw) => KeyLossError_(kw.key).into(),
            Self::Vol(kw) => KeyLossError_(kw.key).into(),
            Self::LastModified(kw) => KeyLossError_(kw.key).into(),
            Self::Originality(kw) => KeyLossError_(kw.key).into(),
            Self::LastModifier(kw) => KeyLossError_(kw.key).into(),
            Self::Spillover(kw) => KeyLossError_(kw.key).into(),
            Self::Flowrate(kw) => KeyLossError_(kw.key).into(),
            Self::Carrierid(kw) => KeyLossError_(kw.key).into(),
            Self::Carriertype(kw) => KeyLossError_(kw.key).into(),
            Self::Locationid(kw) => KeyLossError_(kw.key).into(),
            Self::Plateid(kw) => KeyLossError_(kw.key).into(),
            Self::Platename(kw) => KeyLossError_(kw.key).into(),
            Self::Wellid(kw) => KeyLossError_(kw.key).into(),
            Self::Cytsn(kw) => KeyLossError_(kw.key).into(),
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
            Self::DetectorName(kw) => KeyLossError_(kw.key).into(),
            Self::Tag(kw) => KeyLossError_(kw.key).into(),
            Self::Analyte(kw) => KeyLossError_(kw.key).into(),
            Self::OpticalType(kw) => KeyLossError_(kw.key).into(),
            Self::Display(kw) => KeyLossError_(kw.key).into(),
            Self::Feature(kw) => KeyLossError_(kw.key).into(),
            Self::Calibration3_1(kw) => KeyLossError_(kw.key).into(),
            Self::Calibration3_2(kw) => KeyLossError_(kw.key).into(),
            Self::Peak(kw) => {
                let ret = match kw {
                    OptPeakKeyword::PeakBin(k) => PeakLossError::from(KeyLossError_(k.key)),
                    OptPeakKeyword::PeakIndex(k) => PeakLossError::from(KeyLossError_(k.key)),
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
            | Self::Longname(_) => return None,
        };
        Some(ret)
    }

    pub(crate) fn as_temporal_loss_error(&self) -> Option<AnyOpticalToTemporalKeyLossError> {
        let ret = match self {
            Self::DetectorName(kw) => KeyLossError_(kw.key).into(),
            Self::Tag(kw) => KeyLossError_(kw.key).into(),
            Self::Analyte(kw) => KeyLossError_(kw.key).into(),
            Self::OpticalType(kw) => KeyLossError_(kw.key).into(),
            Self::Feature(kw) => KeyLossError_(kw.key).into(),
            Self::Calibration3_1(kw) => KeyLossError_(kw.key).into(),
            Self::Calibration3_2(kw) => KeyLossError_(kw.key).into(),
            Self::Wavelength(kw) => KeyLossError_(kw.key).into(),
            Self::Wavelengths(kw) => KeyLossError_(kw.key).into(),
            Self::Filter(kw) => KeyLossError_(kw.key).into(),
            Self::DetectorType(kw) => KeyLossError_(kw.key).into(),
            Self::Power(kw) => KeyLossError_(kw.key).into(),
            Self::PercentEmitted(kw) => KeyLossError_(kw.key).into(),
            Self::DetectorVoltage(kw) => KeyLossError_(kw.key).into(),
            // These are shared b/t temporal and optical so cannot result in
            // loss.
            Self::Peak(_) | Self::Display(_) | Self::Longname(_) => return None,
        };
        Some(ret)
    }
}

impl OptScaledOpticalKeyword<'_> {
    pub(crate) fn as_temporal_loss_error(&self) -> Option<AnyOpticalToTemporalKeyLossError> {
        match self {
            Self::Optical(x) => x.as_temporal_loss_error(),
            Self::Scale(x) => match x {
                // $PnE and $PnG are dealt with here because the temporal
                // structs don't actually hold anything for scale and gain since
                // these values are always the same.
                //
                // $PnE must always be linear for temporal measurement
                OptScaleKeyword::Scale(kw) => {
                    let i = kw.key.index().into();
                    (!matches!(kw.value, kws::Scale::Linear))
                        .then_some(NonLinearScaleError(i).into())
                }
                // $PnG must be 1.0 if it exists since temporal measurement does
                // not have gain
                OptScaleKeyword::Gain(kw) => {
                    let i = kw.key.index().into();
                    (!kw.value.0.is_one()).then_some(NonUnitGainError(i).into())
                }
            },
        }
    }
}

impl OptTemporalKeyword<'_> {
    pub(crate) fn from_timestep(x: kws::Timestep) -> Self {
        let ret = SplitKeyword_::new(DollarKey::default(), x);
        Self::from(ret)
    }

    pub(crate) fn as_loss_error(&self) -> Option<AnyTemporalKeyLossError> {
        match self {
            Self::Meas(kw) => kw.as_loss_error(),
            // $TIMESTEP is only lossy if not one since it is implied to be
            // one if not in target version
            Self::Timestep(kw) => (!kw.value.0.is_one()).then_some(TimestepLossError.into()),
        }
    }

    pub(crate) fn as_optical_loss_error(&self) -> Option<AnyTemporalToOpticalKeyLossError> {
        match self {
            Self::Meas(kw) => kw.as_optical_loss_error(),
            // $TIMESTEP is dealt with separately since it is usually either
            // moved to a new measurement or returned and thus is not lossy.
            Self::Timestep(_) => None,
        }
    }
}

impl OptMeasTemporalKeyword<'_> {
    pub(crate) fn as_loss_error(&self) -> Option<AnyTemporalKeyLossError> {
        let ret = match self {
            Self::TemporalType(kw) => KeyLossError_(kw.key).into(),
            Self::Display(kw) => KeyLossError_(kw.key).into(),
            Self::Peak(kw) => {
                let ret = match kw {
                    OptPeakKeyword::PeakBin(k) => PeakLossError::from(KeyLossError_(k.key)),
                    OptPeakKeyword::PeakIndex(k) => PeakLossError::from(KeyLossError_(k.key)),
                };
                ret.into()
            }
            // These are shared b/t all versions so cannot result in loss when
            // converting.
            Self::TemporalScale2_0(_) | Self::Longname(_) => return None,
        };
        Some(ret)
    }

    pub(crate) fn as_optical_loss_error(&self) -> Option<AnyTemporalToOpticalKeyLossError> {
        let ret = match self {
            Self::TemporalType(kw) => KeyLossError_(kw.key).into(),
            // These are shared with optical so cannot result in loss. $TIMESTEP
            // is dealt with separately since it is usually either moved to a
            // new measurement or returned and thus is not lossy.
            Self::Display(_) | Self::Peak(_) | Self::TemporalScale2_0(_) | Self::Longname(_) => {
                return None;
            }
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

impl<K: ValueToStdKey, V> DisplayEscaped for SplitKeyword_<DollarKey<K>, V>
where
    for<'a> DollarKey<K>: ToDisplayNE<'a>,
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

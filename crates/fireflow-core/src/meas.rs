//! The DATA segment and metadata for measurements.

use crate::config::{
    AllowLoss, ReadDataKeywordsConfig, ReadEventsConfig, ReadStdKeywordsConfig,
    TemporalHasOpticalKeyError,
};
use crate::core::{TrimmedKeywords, Versioned};
use crate::data::{
    self, CastSeriesErrors, ConvertFromLayout, DataFrameAsDataSchema, DataFrameCheckRanges as _,
    DataSchemaToDataFrameError, DataSchemaToEmptyDataFrame, EventOverRangeError,
    LayoutConvertError, LayoutDatatype, LayoutInsert, LayoutInsertScaleCheck, LayoutNormalize,
    LayoutRemove, LayoutWidth, MeasLayoutMismatchError, MeasurementsWithLayoutError,
    OverrangeColumn, ReadCheckedDataframeError, ReadCheckedDataframeWarning, ReadDataFrameResult,
    ScaleColumnDatatypeMismatchError, ScaleDatatypeMismatchErrors, VersionedDataFrame,
    VersionedDataSchema, WithPrimitiveDataFrame,
};
use crate::logging::{
    DeferredError, DeferredSwitchableErrors, DeferredWarningsAndErrors, ErrorGroup, ErrorResult,
    ErrorsResult, LogResult, OptionExt as _, ResultExt as _, SwitchableErrorResult,
    WarningAndErrorResult, WarningOrErrorResult, WarningsAndErrorsResult, WarningsAndIOGroupResult,
};
use crate::macros::{assert_eq_len, def_summary};
use crate::segment::AnyDataSegment;
use crate::text::index::MeasIndex;
use crate::text::keyword_enum::{
    AnyOpticalKeyLossError, AnyOpticalToTemporalKeyLossError, AnyTemporalKeyLossError,
    AnyTemporalToOpticalKeyLossError, HasMembership as _, Keyword1FromValue as _, NonStdKeyword,
    OptMeasKeyword, OptOpticalKeyword, OptPeakKeyword, OptScaleKeyword, OptScaledOpticalKeyword,
    OptTemporalKeyword, ReqMeasKeyword, StdOrNonStdOptMeasKeyword,
};
use crate::text::keywords::{
    AlphaNumType, Analyte, Calibration3_1, Calibration3_2, CalibrationLossError, DetectorName,
    DetectorType, DetectorVoltage, Display, Feature, Filter, Gain, LogScale, Longname,
    LookupTemporalGainError, OpticalScaleFix, OpticalType, PeakBin, PeakIndex, PercentEmitted,
    Power, Scale, Tag, TemporalScale2_0, TemporalScale3_0, TemporalScaleFix, TemporalType,
    Timestep, TimestepAdded, Wavelength, Wavelengths, WavelengthsLossError,
};
use crate::text::lookup::{
    DiagnosedKeyword, OptIndexedKey as _, OptIndexedKeyError, OptIndexedKeyStError,
    ReqIndexedKey as _, ReqIndexedKeyError, ReqIndexedStKeyError, ReqKeyError,
};
use crate::text::named_vec::{
    Either, Eithers, Element, ElementIndexError, IndexedElement, InputLengthError,
    InsertCenterError, InsertError, NameMapping, NameNotFoundError, NamePresentError, NamedVec,
    NewNamedVecError, Pair, PushCenterError, RenameError, SetCenterError, SetElementsError,
    SetKeysError, SetNamesError, SetValuesError,
};
use crate::text::optional::{Identity, MightHave, Nothing};
use crate::text::ranged_float::PositiveFloat;
use crate::validated::dataframe::PrimitiveDataFrame;
use crate::validated::keys::{IndexedKey as _, Key1, NonStdKeywords, StdKey, StdKeywords};
use crate::validated::shortname::Shortname;

use fireflow_types::config::{CheckedRangeDatatypes, OverRangeAction, TemporalOpticalKey};
use fireflow_types::keywords::{
    HasVersion, OpticalFeature, Version2_0, Version3_0, Version3_1, Version3_2,
};
use fireflow_types::nonempty_string::{DisplayableNE as _, NEString};
use type_families::{ApplyOnce as _, BifunctorOnce as _};

use derive_more::{AsMut, AsRef, Display, From};
use derive_new::new;
use num_traits::One as _;
use regex::Regex;
use thiserror::Error;

use std::borrow::Cow;
use std::convert::Infallible;
use std::fmt;
use std::io::{BufReader, Read, Seek};
use std::iter::{empty, once};
use std::marker::PhantomData;
use std::mem;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr, FromInnerPyObject},
    fireflow_types::python as py,
    pyo3::prelude::*,
};

/// Metadata and event data for all measurements.
///
/// This consists of two entities:
///
/// 1. `meta`: non-DATA $PnN keywords and $TIMESTEP
/// 2. `data`: DATA $PnN keywords + $BYTEORD + $DATATYPE, possibly with
///    DATA itself
///
/// $PnN keywords are split across (1) and (2) since only a few are necessary
/// for reading DATA itself, and this allows us to skip reading most of the
/// keywords if desired.
///
/// $PnE and $PnG belong to (1) but need to match (2) since their values depend
/// on the datatype used for the columns (ie $DATATYPE and possibly
/// $PnDATATYPE).
///
/// This struct is sealed in order to keep these data structures consistent with
/// each other. Namely, the following must always hold:
///
/// 1. The column number of meta and `data` must match
/// 2. The $PnE/$PnG configuration in meta must match the datatypes in `data`
///    for each corresponding column.
///
/// Additionally, the data structures in meta and `data` have their own internal
/// consistency guarantees.
///
/// NOTE: this struct has methods that allow internal mutation for metadata that
/// is accessible via [`AsMut`]. This trait is implemented for all keyword types
/// except for $PnE/$PnG, which ensures that constraint (2) is always true.
#[derive(Clone, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[new(visibility(""))]
pub struct CoreMeasurements<L, T, O, X, N, V> {
    /// All non-DATA measurement TEXT keywords.
    meta: NamedVec<N, Temporal<T>, ScaledOptical<X, O>>,

    /// The DATA segment + associated TEXT keywords.
    ///
    /// This is derived from $BYTEORD, $DATATYPE, $PnB, $PnR and maybe
    /// $PnDATATYPE for version 3.2.
    ///
    /// DATA may or may not be included depending on the exact type.
    data: L,

    /// Marker for FCS version. Used to lock the types for other fields.
    _version: PhantomData<V>,
}

pub type Optical2_0 = Optical<InnerOptical2_0>;
pub type Optical3_0 = Optical<InnerOptical3_0>;
pub type Optical3_1 = Optical<InnerOptical3_1>;
pub type Optical3_2 = Optical<InnerOptical3_2>;

pub type Temporal2_0 = Temporal<InnerTemporal2_0>;
pub type Temporal3_0 = Temporal<InnerTemporal3_0>;
pub type Temporal3_1 = Temporal<InnerTemporal3_1>;
pub type Temporal3_2 = Temporal<InnerTemporal3_2>;

pub type MeasMeta2_0 =
    MeasMeta<Option<Shortname>, InnerTemporal2_0, InnerOptical2_0, OpticalScale2_0>;

pub type MeasMeta3_0 =
    MeasMeta<Option<Shortname>, InnerTemporal3_0, InnerOptical3_0, OpticalScale3_0>;

pub type MeasMeta3_1 =
    MeasMeta<Identity<Shortname>, InnerTemporal3_1, InnerOptical3_1, OpticalScale3_0>;

pub type MeasMeta3_2 =
    MeasMeta<Identity<Shortname>, InnerTemporal3_2, InnerOptical3_2, OpticalScale3_0>;

pub(crate) type MeasMeta<N, T, O, X> = NamedVec<N, Temporal<T>, ScaledOptical<X, O>>;

pub(crate) type VMeasMeta<V> =
    NamedVec<<V as VersionMeasSet>::Name, VTemporal<V>, VScaledOptical<V>>;

#[derive(Clone, Default, AsRef, AsMut, PartialEq, new)]
#[new(visibility(""))]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct CommonMeasurement {
    /// Value for $PnS
    #[as_ref(Longname)]
    #[as_mut(Longname)]
    #[new(into)]
    longname: Longname,

    /// Non standard keywords that belong to this measurement.
    ///
    /// These are found using a configurable pattern to filter matching keys.
    #[as_ref(NonStdKeywords)]
    #[as_mut(NonStdKeywords)]
    nonstandard_keywords: NonStdKeywords,
}

/// Structured data for time keywords.
///
/// Explicit fields are common to all versions. The generic type parameter
/// allows for version-specific information to be encoded.
#[derive(Clone, AsRef, AsMut, PartialEq, new)]
#[new(visibility(""))]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct Temporal<T> {
    /// Fields shared with optical measurements
    #[as_ref(forward)]
    #[as_mut(forward)]
    common: CommonMeasurement,

    /// Version specific data
    specific: T,
}

/// Optical keywords including $PnE and $PnG (if 3.0+)
#[derive(Clone, PartialEq, AsRef, AsMut, new)]
#[new(visibility(""))]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct ScaledOptical<X, O> {
    #[as_ref(forward)]
    #[as_mut(forward)]
    inner: Optical<O>,
    scale: X,
}

/// Structured data for optical keywords.
///
/// Explicit fields are common to all versions. The generic type parameter
/// allows for version-specific information to be encoded.
#[derive(Clone, AsRef, AsMut, PartialEq, new)]
#[new(visibility(""))]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct Optical<O> {
    /// Fields shared with optical measurements
    #[as_ref(forward)]
    #[as_mut(forward)]
    common: CommonMeasurement,

    /// Value for $PnF
    #[as_ref(Filter)]
    #[as_mut(Filter)]
    #[new(into)]
    filter: Filter,

    /// Value for $PnO
    #[as_ref(Option<Power>)]
    #[as_mut(Option<Power>)]
    #[new(into)]
    power: Option<Power>,

    /// Value for $PnD
    #[as_ref(DetectorType)]
    #[as_mut(DetectorType)]
    #[new(into)]
    detector_type: DetectorType,

    /// Value for $PnP
    #[as_ref(Option<PercentEmitted>)]
    #[as_mut(Option<PercentEmitted>)]
    #[new(into)]
    percent_emitted: Option<PercentEmitted>,

    /// Value for $PnV
    #[as_ref(Option<DetectorVoltage>)]
    #[as_mut(Option<DetectorVoltage>)]
    #[new(into)]
    detector_voltage: Option<DetectorVoltage>,

    /// Version specific data
    specific: O,
}

/// Temporal measurement fields specific to version 2.0
#[derive(Clone, Default, AsRef, AsMut, PartialEq, new)]
#[new(visibility(""))]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct InnerTemporal2_0 {
    /// Values of $Pkn/$PKNn
    #[as_ref(Option<PeakBin>)]
    #[as_ref(Option<PeakIndex>)]
    #[as_mut(Option<PeakBin>)]
    #[as_mut(Option<PeakIndex>)]
    peak: PeakData,
}

/// Temporal measurement fields specific to version 3.0
///
/// $PnE is implied as linear but not included since it only has one value
#[derive(Clone, AsRef, AsMut, PartialEq, new)]
#[new(visibility(""))]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct InnerTemporal3_0 {
    /// Value for $TIMESTEP
    #[as_ref(Timestep)]
    #[as_mut(Timestep)]
    timestep: Timestep,

    /// Values of $Pkn/$PKNn
    #[as_ref(Option<PeakBin>)]
    #[as_ref(Option<PeakIndex>)]
    #[as_mut(Option<PeakBin>)]
    #[as_mut(Option<PeakIndex>)]
    peak: PeakData,
}

/// Temporal measurement fields specific to version 3.1
///
/// $PnE is implied as linear but not included since it only has one value
#[derive(Clone, AsRef, AsMut, PartialEq, new)]
#[new(visibility(""))]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct InnerTemporal3_1 {
    /// Value for $TIMESTEP
    #[as_ref(Timestep)]
    #[as_mut(Timestep)]
    timestep: Timestep,

    /// Value for $PnD
    #[as_ref(Option<Display>)]
    #[as_mut(Option<Display>)]
    #[new(into)]
    display: Option<Display>,

    /// Values of $Pkn/$PKNn
    #[as_ref(Option<PeakBin>)]
    #[as_ref(Option<PeakIndex>)]
    #[as_mut(Option<PeakBin>)]
    #[as_mut(Option<PeakIndex>)]
    peak: PeakData,
}

/// Temporal measurement fields specific to version 3.2
///
/// $PnE is implied as linear but not included since it only has one value
#[derive(Clone, AsRef, AsMut, PartialEq, new)]
#[new(visibility(""))]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct InnerTemporal3_2 {
    /// Value for $TIMESTEP
    #[as_ref(Timestep)]
    #[as_mut(Timestep)]
    timestep: Timestep,

    /// Value for $PnD
    #[as_ref(Option<Display>)]
    #[as_mut(Option<Display>)]
    #[new(into)]
    display: Option<Display>,

    /// Value for $PnTYPE
    #[as_ref(TemporalType)]
    #[as_mut(TemporalType)]
    #[new(into)]
    measurement_type: TemporalType,
}

/// Optical measurement fields specific to version 2.0
#[derive(Clone, Default, AsRef, AsMut, PartialEq, new)]
#[new(visibility(""))]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct InnerOptical2_0 {
    /// Value for $PnL
    #[as_ref(Option<Wavelength>)]
    #[as_mut(Option<Wavelength>)]
    #[new(into)]
    wavelength: Option<Wavelength>,

    /// Values of $Pkn/$PKNn
    #[as_ref(Option<PeakBin>)]
    #[as_ref(Option<PeakIndex>)]
    #[as_mut(Option<PeakBin>)]
    #[as_mut(Option<PeakIndex>)]
    peak: PeakData,
}

/// Optical measurement fields specific to version 3.0
#[derive(Clone, AsRef, AsMut, PartialEq, new)]
#[new(visibility(""))]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct InnerOptical3_0 {
    /// Value for $PnL
    #[as_ref(Option<Wavelength>)]
    #[as_mut(Option<Wavelength>)]
    #[new(into)]
    wavelength: Option<Wavelength>,

    /// Values of $Pkn/$PKNn
    #[as_ref(Option<PeakBin>)]
    #[as_ref(Option<PeakIndex>)]
    #[as_mut(Option<PeakBin>)]
    #[as_mut(Option<PeakIndex>)]
    peak: PeakData,
}

/// Optical measurement fields specific to version 3.1
#[derive(Clone, AsRef, AsMut, PartialEq, new)]
#[new(visibility(""))]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct InnerOptical3_1 {
    /// Value for $PnL
    #[as_ref(Wavelengths)]
    #[as_mut(Wavelengths)]
    #[new(into)]
    wavelengths: Wavelengths,

    /// Value for $PnCALIBRATION
    #[as_ref(Option<Calibration3_1>)]
    #[as_mut(Option<Calibration3_1>)]
    #[new(into)]
    calibration: Option<Calibration3_1>,

    /// Value for $PnD
    #[as_ref(Option<Display>)]
    #[as_mut(Option<Display>)]
    #[new(into)]
    display: Option<Display>,

    /// Values of $Pkn/$PKNn
    #[as_ref(Option<PeakBin>)]
    #[as_ref(Option<PeakIndex>)]
    #[as_mut(Option<PeakBin>)]
    #[as_mut(Option<PeakIndex>)]
    peak: PeakData,
}

/// Optical measurement fields specific to version 3.2
#[allow(clippy::too_many_arguments)]
#[derive(Clone, AsRef, AsMut, PartialEq, new)]
#[new(visibility(""))]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct InnerOptical3_2 {
    /// Value for $PnL
    #[as_ref(Wavelengths)]
    #[as_mut(Wavelengths)]
    #[new(into)]
    wavelengths: Wavelengths,

    /// Value for $PnCALIBRATION
    #[as_ref(Option<Calibration3_2>)]
    #[as_mut(Option<Calibration3_2>)]
    #[new(into)]
    calibration: Option<Calibration3_2>,

    /// Value for $PnD
    #[as_ref(Option<Display>)]
    #[as_mut(Option<Display>)]
    #[new(into)]
    display: Option<Display>,

    /// Value for $PnANALYTE
    #[as_ref(Analyte)]
    #[as_mut(Analyte)]
    #[new(into)]
    analyte: Analyte,

    /// Value for $PnFEATURE
    #[as_ref(Option<Feature>)]
    #[as_mut(Option<Feature>)]
    #[new(into)]
    feature: Option<Feature>,

    /// Value for $PnTYPE
    #[as_ref(OpticalType)]
    #[as_mut(OpticalType)]
    #[new(into)]
    measurement_type: OpticalType,

    /// Value for $PnTAG
    #[as_ref(Tag)]
    #[as_mut(Tag)]
    #[new(into)]
    tag: Tag,

    /// Value for $PnDET
    #[as_ref(DetectorName)]
    #[as_mut(DetectorName)]
    #[new(into)]
    detector_name: DetectorName,
}

/// A scale transform derived from $PnE (2.0+).
#[derive(Clone, Copy, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
pub struct OpticalScale2_0(pub Option<Scale>);

impl Default for OpticalScale2_0 {
    fn default() -> Self {
        Self(Some(Scale::default()))
    }
}

impl OpticalScale2_0 {
    #[must_use]
    pub fn none() -> Self {
        Self(None)
    }
}

/// A scale transform derived from $PnE/$PnG.
#[derive(Clone, Copy, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum OpticalScale3_0 {
    /// A linear transform ($PnE=0,0 and $PnG=1.0 or is null)
    Lin(PositiveFloat),
    /// A log transform ($PnE!=0,0 and $PnG!=1.0 or is null)
    Log(LogScale),
}

impl Default for OpticalScale3_0 {
    fn default() -> Self {
        Self::Lin(PositiveFloat::one())
    }
}

/// A bundle for $PKn and $PKNn (2.0-3.1)
///
/// It makes little sense to have only one of these since they both collectively
/// describe a histogram peak. This currently is not enforced since these keys
/// are likely not used much and it is easy for users to check these themselves.
#[derive(Clone, Default, AsRef, AsMut, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct PeakData {
    /// Value of $Pkn
    #[as_ref(Option<PeakBin>)]
    #[as_mut(Option<PeakBin>)]
    #[new(into)]
    bin: Option<PeakBin>,

    /// Value of $PkNn
    #[as_ref(Option<PeakIndex>)]
    #[as_mut(Option<PeakIndex>)]
    #[new(into)]
    size: Option<PeakIndex>,
}

#[derive(new)]
pub struct DiagnosedScaledOptical<M> {
    pub(crate) this: M,
    pub(crate) scale: OpticalScaleFix,
    pub(crate) trimmed: TrimmedKeywords,
}

#[derive(new)]
pub struct DiagnosedOptical<M> {
    pub(crate) this: M,
    pub(crate) trimmed: TrimmedKeywords,
}

#[derive(new)]
pub struct DiagnosedTemporal<M> {
    pub(crate) this: M,
    pub(crate) scale: TemporalScaleFix,
    pub(crate) trimmed: TrimmedKeywords,
    pub(crate) tmp_opt_pairs: Vec<(StdKey, NEString)>,
    pub(crate) timestep_added: TimestepAdded,
}

/// Error when looking up [`CoreMeasurements`] from keywords
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum NewMeasError {
    /// Measurement vector has more than one time element
    Meas(NewNamedVecError),
    /// Measurement and layout are incompatible
    Layout(MeasLayoutMismatchError),
}

/// Error when looking up [`CoreMeasurements`] from keywords
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupMeasError {
    /// Measurement vector has more than one time element
    Meas(NewNamedVecError),
    /// Measurement and layout are incompatible
    Layout(ScaleDatatypeMismatchErrors),
    /// Time channel is missing entirely
    Time(MissingTimeError),
}

/// Error triggered when time measurement is missing but required.
#[derive(Debug, Error)]
#[error("Could not find time measurement matching '{0}'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct MissingTimeError(pub Regex);

/// Error when parsing $PnN
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupShortnameError {
    Req(ReqIndexedKeyError<Shortname>),
    Opt(OptIndexedKeyError<Shortname>),
}

/// Error when parsing any optical or scale keyword
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupScaledOpticalError {
    Optical(LookupOpticalError),
    Scale(LookupScaleError),
}

/// Warning when parsing any optical or scale keyword
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupScaledOpticalWarning {
    Optical(LookupOpticalWarning),
    Scale(LookupScaleWarning),
}

/// Error when parsing any optical measurement keyword
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupOpticalError {
    New(NewOpticalScaleError),
    Lookup(ReqIndexedStKeyError<Scale>),
    Warn(LookupOpticalWarning),
}

/// Warning when parsing any optical measurement keyword
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupOpticalWarning {
    Feature(OptIndexedKeyStError<Feature>),
    Wavelengths(OptIndexedKeyStError<Wavelengths>),
    Wavelength(OptIndexedKeyError<Wavelength>),
    Calibration3_1(OptIndexedKeyStError<Calibration3_1>),
    Calibration3_2(OptIndexedKeyStError<Calibration3_2>),
    OpticalType(OptIndexedKeyError<OpticalType>),
    Display(OptIndexedKeyStError<Display>),
    Power(OptIndexedKeyError<Power>),
    PercentEmitted(OptIndexedKeyError<PercentEmitted>),
    DetectorVoltage(OptIndexedKeyError<DetectorVoltage>),
    Peak(LookupPeakError),
}

/// Error when parsing any optical scale keyword
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupScaleError {
    New(NewOpticalScaleError),
    Lookup(ReqIndexedStKeyError<Scale>),
    Warn(LookupScaleWarning),
}

/// Warning when parsing any optical scale keyword
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupScaleWarning {
    Scale(OptIndexedKeyStError<Scale>),
    Gain(OptIndexedKeyError<Gain>),
}

/// Error when $PnE is log and $PnG is not 1.0 or None
#[derive(Debug, Error)]
#[error(
    "could not make scale transform with log scale \
     '{}' and non-unit gain '{}'",
    scale.as_displayable(),
    gain.as_displayable(),
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct NewOpticalScaleError {
    scale: Scale,
    gain: Gain,
}

/// Error when parsing any temporal measurement keyword
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupTemporalError {
    TemporalScale(ReqIndexedStKeyError<TemporalScale3_0>),
    Timestep(ReqKeyError<Timestep>),
    Warn(LookupTemporalWarning),
}

/// Warning when parsing any temporal measurement keyword
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupTemporalWarning {
    TemporalScale(OptIndexedKeyStError<TemporalScale2_0>),
    TemporalGain(LookupTemporalGainError),
    TemporalType(OptIndexedKeyError<TemporalType>),
    Display(OptIndexedKeyStError<Display>),
    Peak(LookupPeakError),
    Optical(TemporalHasOpticalKeyError),
}

/// Error when parsing $PKn or $PKNn
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupPeakError {
    Bin(OptIndexedKeyError<PeakBin>),
    Index(OptIndexedKeyError<PeakIndex>),
}

/// Error when converting [`CoreMeasurements`] to new FCS version
#[derive(Debug, Display, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum MeasConvertError {
    Rewrap(NameConversionError),
    Optical(ScaledOpticalConvertError),
    Temporal(AnyTemporalKeyLossError),
    Layout(LayoutConvertError),
}

/// Warning when converting [`CoreMeasurements`] to new FCS version
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum MeasConvertWarning {
    Optical(ScaledOpticalConvertWarning),
    Temporal(AnyTemporalKeyLossError),
}

/// Error when converting scaled optical measurement to new FCS version
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ScaledOpticalConvertError {
    Scale(ScaleConvertError),
    Optical(OpticalConvertError),
}

/// Warning when converting scaled optical measurement to new FCS version
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ScaledOpticalConvertWarning {
    Scale(GainLossError),
    Optical(OpticalConvertWarning),
}

/// Error when converting optical measurement to new FCS version
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum OpticalConvertError {
    NoScale(NoScaleError),
    Warning(OpticalConvertWarning),
}

/// Warning when converting optical measurement to new FCS version
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum OpticalConvertWarning {
    Wavelengths(WavelengthsLossError),
    Calibration(CalibrationLossError),
    Xfer(AnyOpticalKeyLossError),
}

/// Error when converting scale to new FCS version
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ScaleConvertError {
    NoScale(NoScaleError),
    Gain(GainLossError),
}

/// Error when $PnN is optional and missing in current version and required in target
#[derive(Debug, Error)]
#[error("{0} is required in target version but missing in current version")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConversionError))]
pub struct NameConversionError(Key1<Shortname>);

/// Error when replacing temporal measurement by index
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ReplaceTemporalErrorByIndex {
    ToOptical(AnyTemporalToOpticalKeyLossError),
    Set(SetCenterError),
}

/// Error when replacing temporal measurement by name
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ReplaceTemporalErrorByName {
    ToOptical(AnyTemporalToOpticalKeyLossError),
    Name(NameNotFoundError),
}

/// Error when setting a new temporal measurement by name ($PnN)
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum SetTemporalByNameError {
    Inner(SetTemporalError),
    Name(NameNotFoundError),
}

/// Error when setting a new temporal measurement by index
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum SetTemporalByIndexError {
    Inner(SetTemporalError),
    Set(SetCenterError),
}

/// Error when setting a new temporal measurement
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum SetTemporalError {
    /// Temporal already exists, in which case old one needs to be converted to optical
    Swap(SwapOpticalTemporalErrors),
    /// Temporal does not exist, in which case one optical measurement must be converted
    ToOptical(OpticalToTemporalErrors),
}

/// Error when $PnE/$PnG do not match the datatype for a given column
#[derive(Debug, Error, new)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct ScaleDatatypeMismatchError {
    index: MeasIndex,
    datatype: AlphaNumType,
    scale: LogScale,
    has_gain: bool,
}

impl fmt::Display for ScaleDatatypeMismatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let i = self.index;
        let ekey = Scale::std(i);
        let dt = self.datatype.as_displayable();
        let s = self.scale.as_displayable();
        let g = if self.has_gain {
            let gkey = Gain::std(i);
            format!(" and {gkey} 1.0 or not set")
        } else {
            String::new()
        };
        write!(
            f,
            "only integer columns may have non-linear scale, \
             column is '{dt}' where {ekey} is '{s}'{g}"
        )
    }
}

type SwapOpticalTemporalErrors = ErrorGroup<SwapOpticalTemporalError, SwapOpticalTemporalSummary>;

#[derive(Display, Debug, new)]
#[display("could not swap temporal index {tmp_index} with optical index {opt_index}")]
pub struct SwapOpticalTemporalSummary {
    opt_index: MeasIndex,
    tmp_index: MeasIndex,
}

/// Error when swapping optical and temporal measurement
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum SwapOpticalTemporalError {
    TemporalToOptical(AnyTemporalToOpticalKeyLossError),
    OpticalToTemporal(AnyOpticalToTemporalKeyLossError),
}

type OpticalToTemporalErrors = ErrorGroup<OpticalToTemporalError, OpticalToTemporalSummary>;

#[derive(Display, Debug, new)]
#[display("could not convert optical index at {opt_index} to temporal")]
pub struct OpticalToTemporalSummary {
    opt_index: MeasIndex,
}

/// Error when converting optical to temporal measurement
pub type OpticalToTemporalError = AnyOpticalToTemporalKeyLossError;

/// Error when $PnE is not set on optical measurement and target version requires it
#[derive(Debug, Error)]
#[error("{} must be set before converting measurement", Scale::std(self.0))]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConversionError))]
pub struct NoScaleError(MeasIndex);

/// Error when the $PnG does not exist in target version and is not 1.0.
#[derive(Debug, Error)]
#[error(
    "$P{0}G does not exist in target version and is currently not 1.0 \
     which means data will be lost on dropping"
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConversionError))]
pub struct GainLossError(MeasIndex);

/// Error when pushing a temporal measurement into [`CoreMeasurements`]
#[derive(Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(PyErr: From<E>))]
pub enum PushTemporalError<E> {
    Center(PushCenterError),
    Layout(E),
}

/// Error when inserting a temporal measurement into [`CoreMeasurements`]
#[derive(Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(PyErr: From<E>))]
pub enum InsertTemporalError<E> {
    Center(InsertCenterError),
    Layout(E),
}

/// Error when pushing an optical measurement into [`CoreMeasurements`]
#[derive(Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(PyErr: From<E>))]
pub enum PushOpticalError<E> {
    Unique(NamePresentError),
    Scale(ScaleColumnDatatypeMismatchError),
    Layout(E),
}

/// Error when inserting an optical measurement into [`CoreMeasurements`]
#[derive(Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(PyErr: From<E>))]
pub enum InsertOpticalError<E> {
    Insert(InsertError),
    Scale(ScaleColumnDatatypeMismatchError),
    Layout(E),
}

/// Error when setting data schema for a dataset.
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum DatasetSetDataSchemaError {
    DataSchema(MeasLayoutMismatchError),
    Cast(CastSeriesErrors),
}

/// Error when setting measurements vector without names
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum SetUnnamedMeasurementsError {
    New(ScaleDatatypeMismatchErrors),
    Set(SetValuesError),
}

/// Error when setting measurements vector without names
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum SetUnnamedMeasurementsAndDataSchemaError {
    New(MeasLayoutMismatchError),
    Set(SetValuesError),
}

/// Error when setting named measurements and data schema for a dataset.
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum DatasetSetUnnamedMeasAndDataSchemaError {
    Cast(CastSeriesErrors),
    Meas(SetUnnamedMeasurementsAndDataSchemaError),
}

/// Error when setting measurements without $PnN and DATA/dataframe simultaneously
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum SetUnnamdMeasurementsAndDataError {
    Meas(SetUnnamedMeasurementsError),
    Mismatch(DataSchemaToDataFrameError),
}

pub(crate) type VersionedCoreLayout<L, V> = CoreMeasurements<
    L,
    <V as VersionMeasSet>::Temporal,
    <V as VersionMeasSet>::Optical,
    <V as VersionMeasSet>::OpticalScale,
    <V as VersionMeasSet>::Name,
    V,
>;

def_summary!(
    pub SetScalesSummary,
    "could not set scales for optical measurements"
);

/// Error when setting $PnE for all measurements (3.0+)
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum SetScalesError {
    Layout(MeasLayoutMismatchError),
    Temporal(NonIdentityTemporalScaleError),
}

/// Error when attempting to set temporal scale to something other than identity
#[derive(Debug, Error)]
#[error("tried to set temporal scale to non-identity")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct NonIdentityTemporalScaleError;

type VersionedMeasurements<V> =
    NamedVec<<V as VersionMeasSet>::Name, VTemporal<V>, VScaledOptical<V>>;

// type VersionedElement<V> = Element<VTemporal<V>, VOptical<V>>;

type VElementWithScale<V> =
    Element<VTemporal<V>, (VOptical<V>, <V as VersionMeasSet>::OpticalScale)>;

type VTemporal<V> = Temporal<<V as VersionMeasSet>::Temporal>;
type VOptical<V> = Optical<<V as VersionMeasSet>::Optical>;
type VScaledOptical<V> =
    ScaledOptical<<V as VersionMeasSet>::OpticalScale, <V as VersionMeasSet>::Optical>;

pub(crate) type TemporalOrOptical<T, O> = Element<Temporal<T>, Optical<O>>;

pub(crate) type TemporalOrOpticalWithScale<T, O, S> = Element<Temporal<T>, (Optical<O>, S)>;

type TemporalsAndOpticals<T, O> = Vec<TemporalOrOptical<T, O>>;

type TemporalOrScaledOptical<T, S, O> = Element<Temporal<T>, ScaledOptical<S, O>>;

type NamedTemporalOrOpticalWithScale<K, T, S, O> =
    Element<(Shortname, Temporal<T>), (K, Optical<O>, S)>;

pub type NamedTemporalsAndOpticalsWithScale<K, T, S, O> =
    Vec<NamedTemporalOrOpticalWithScale<K, T, S, O>>;

type TemporalsAndScaledOpticals<T, S, O> = Vec<TemporalOrScaledOptical<T, S, O>>;

pub(crate) type VTemporalOrOptical<V> = Element<VTemporal<V>, VOptical<V>>;

pub(crate) type VTemporalOrOpticalWithScale<V> =
    Element<VTemporal<V>, (VOptical<V>, <V as VersionMeasSet>::OpticalScale)>;

pub(crate) type VNamedTemporalOrOpticalWithScale<V> = Element<
    Pair<Shortname, VTemporal<V>>,
    Pair<<V as VersionMeasSet>::Name, (VOptical<V>, <V as VersionMeasSet>::OpticalScale)>,
>;

// pub(crate) type VNamedTemporalOrOptical<V> =
//     EitherPair<<V as VersionLayoutSet>::Name, VTemporal<V>, VOptical<V>>;

pub(crate) type VTemporalsAndOpticals<V> = Vec<VTemporalOrOptical<V>>;

// TODO dry me off
pub(crate) type VNamedTemporalsAndOpticalsWithScale<V> = Vec<
    Element<
        (Shortname, VTemporal<V>),
        (
            <V as VersionMeasSet>::Name,
            VOptical<V>,
            <V as VersionMeasSet>::OpticalScale,
        ),
    >,
>;

pub(crate) type VNamedTemporalsAndScaledOpticals<V> =
    Eithers<<V as VersionMeasSet>::Name, VTemporal<V>, VScaledOptical<V>>;

pub(crate) type TemporalsAndOpticals2_0 = VNamedTemporalsAndOpticalsWithScale<Version2_0>;
pub(crate) type TemporalsAndOpticals3_0 = VNamedTemporalsAndOpticalsWithScale<Version3_0>;
pub(crate) type TemporalsAndOpticals3_1 = VNamedTemporalsAndOpticalsWithScale<Version3_1>;
pub(crate) type TemporalsAndOpticals3_2 = VNamedTemporalsAndOpticalsWithScale<Version3_2>;

// Implement version mapping for types that belong together

pub trait VersionMeasSet: HasVersion {
    type Optical: OpticalKeywords;
    type Temporal: TemporalKeywords + TemporalMaybeToOptical;
    type Name: MightHave<Shortname>;
    type OpticalScale: Default
        + Copy
        + CheckedScaleTransform
        + LookupOpticalScale
        + OpticalScaleKeywords;
    type DataSchema: VersionedDataSchema;
    type DataFrame: VersionedDataFrame;
}

macro_rules! impl_version_set {
    ($v:ident, $opt:path, $t:path, $n:path, $x:path, $l:path, $d:path) => {
        impl VersionMeasSet for $v {
            type Optical = $opt;
            type Temporal = $t;
            type Name = $n;
            type OpticalScale = $x;
            type DataSchema = $l;
            type DataFrame = $d;
        }
    };
}

impl_version_set!(
    Version2_0,
    InnerOptical2_0,
    InnerTemporal2_0,
    Option<Shortname>,
    OpticalScale2_0,
    data::DataSchema2_0,
    data::DataFrame2_0
);

impl_version_set!(
    Version3_0,
    InnerOptical3_0,
    InnerTemporal3_0,
    Option<Shortname>,
    OpticalScale3_0,
    data::DataSchema3_0,
    data::DataFrame3_0
);

impl_version_set!(
    Version3_1,
    InnerOptical3_1,
    InnerTemporal3_1,
    Identity<Shortname>,
    OpticalScale3_0,
    data::DataSchema3_1,
    data::DataFrame3_1
);

impl_version_set!(
    Version3_2,
    InnerOptical3_2,
    InnerTemporal3_2,
    Identity<Shortname>,
    OpticalScale3_0,
    data::DataSchema3_2,
    data::DataFrame3_2
);

// Implement references to inner types.
//
// This will be the primary way for the API to access keywords values since
// the AsRef trait provides a clean an elegant way to access internals without
// rewriting a method for every keyword.
//
// Note that mutable references are never used for types that must be internally
// validated for consistency with other values.

macro_rules! impl_ref_specific_ro {
    ($outer:ident, $inner:ident, $($ref:path),*) => {
        $(
            impl AsRef<$ref> for $outer<$inner> {
                fn as_ref(&self) -> &$ref {
                    self.specific.as_ref()
                }
            }
        )*
    };
}

pub(crate) use impl_ref_specific_ro;

macro_rules! impl_ref_specific_rw {
    ($outer:ident, $inner:ident, $($ref:path),*) => {
        $(
            impl AsMut<$ref> for $outer<$inner> {
                fn as_mut(&mut self) -> &mut $ref {
                    self.specific.as_mut()
                }
            }

            impl_ref_specific_ro!($outer, $inner, $ref);
        )*
    };
}

pub(crate) use impl_ref_specific_rw;

impl_ref_specific_rw!(
    Optical,
    InnerOptical2_0,
    Option<Wavelength>,
    Option<PeakBin>,
    Option<PeakIndex>
);

impl_ref_specific_rw!(
    Optical,
    InnerOptical3_0,
    Option<Wavelength>,
    Option<PeakBin>,
    Option<PeakIndex>
);

impl_ref_specific_rw!(
    Optical,
    InnerOptical3_1,
    Wavelengths,
    Option<PeakBin>,
    Option<PeakIndex>,
    Option<Calibration3_1>,
    Option<Display>
);

impl_ref_specific_rw!(
    Optical,
    InnerOptical3_2,
    Wavelengths,
    Option<Calibration3_2>,
    Option<Display>,
    Analyte,
    Option<Feature>,
    OpticalType,
    Tag,
    DetectorName
);

impl_ref_specific_rw!(
    Temporal,
    InnerTemporal2_0,
    Option<PeakBin>,
    Option<PeakIndex>
);

impl_ref_specific_rw!(
    Temporal,
    InnerTemporal3_0,
    Timestep,
    Option<PeakBin>,
    Option<PeakIndex>
);

impl_ref_specific_rw!(
    Temporal,
    InnerTemporal3_1,
    Timestep,
    Option<Display>,
    Option<PeakBin>,
    Option<PeakIndex>
);

impl_ref_specific_rw!(
    Temporal,
    InnerTemporal3_2,
    Timestep,
    Option<Display>,
    TemporalType
);

impl<X, O> AsMut<CommonMeasurement> for ScaledOptical<X, O> {
    fn as_mut(&mut self) -> &mut CommonMeasurement {
        &mut self.inner.common
    }
}

impl<X> AsMut<CommonMeasurement> for Temporal<X> {
    fn as_mut(&mut self) -> &mut CommonMeasurement {
        &mut self.common
    }
}

impl<X, O> AsRef<CommonMeasurement> for ScaledOptical<X, O> {
    fn as_ref(&self) -> &CommonMeasurement {
        &self.inner.common
    }
}

impl<X> AsRef<CommonMeasurement> for Temporal<X> {
    fn as_ref(&self) -> &CommonMeasurement {
        &self.common
    }
}

// Implement Scale -> $Pn* keywords methods

pub trait OpticalScaleKeywords: Sized {
    fn req_keywords(&self, i: MeasIndex) -> impl Iterator<Item = ReqMeasKeyword<'_>>;

    fn opt_keywords(&self, i: MeasIndex) -> impl Iterator<Item = OptScaleKeyword>;
}

impl OpticalScaleKeywords for OpticalScale2_0 {
    fn req_keywords(&self, _: MeasIndex) -> impl Iterator<Item = ReqMeasKeyword<'_>> {
        empty()
    }

    fn opt_keywords(&self, i: MeasIndex) -> impl Iterator<Item = OptScaleKeyword> {
        self.0
            .map(|s| OptScaleKeyword::from_value(s, i))
            .into_iter()
    }
}

impl OpticalScaleKeywords for OpticalScale3_0 {
    fn req_keywords(&self, i: MeasIndex) -> impl Iterator<Item = ReqMeasKeyword<'_>> {
        once(self.req_keyword(i))
    }

    fn opt_keywords(&self, i: MeasIndex) -> impl Iterator<Item = OptScaleKeyword> {
        self.opt_keyword(i).into_iter()
    }
}

// Implement Optical -> $Pn* keywords methods

pub trait OpticalKeywords: Sized + Versioned {
    fn opt_keywords_inner(&self, i: MeasIndex) -> impl Iterator<Item = OptOpticalKeyword<'_>>;
}

impl OpticalKeywords for InnerOptical2_0 {
    fn opt_keywords_inner(&self, i: MeasIndex) -> impl Iterator<Item = OptOpticalKeyword<'_>> {
        let x1 = self.wavelength.map(|v| OptOpticalKeyword::from_value(v, i));
        let ps = self.peak.opt_keywords(i).map(OptOpticalKeyword::from);
        once(x1).flatten().chain(ps)
    }
}

impl OpticalKeywords for InnerOptical3_0 {
    fn opt_keywords_inner(&self, i: MeasIndex) -> impl Iterator<Item = OptOpticalKeyword<'_>> {
        let ps = self.peak.opt_keywords(i).map(OptOpticalKeyword::from);
        let w = self.wavelength.map(|v| OptOpticalKeyword::from_value(v, i));
        w.into_iter().chain(ps)
    }
}

impl OpticalKeywords for InnerOptical3_1 {
    fn opt_keywords_inner(&self, i: MeasIndex) -> impl Iterator<Item = OptOpticalKeyword<'_>> {
        let x0 = OptOpticalKeyword::from_wavelengths(&self.wavelengths, i);
        let x1 = self
            .calibration
            .as_ref()
            .map(|v| OptOpticalKeyword::from_ref(v, i));
        let x2 = self.display.map(|v| OptOpticalKeyword::from_value(v, i));
        let ps = self.peak.opt_keywords(i).map(OptOpticalKeyword::from);
        [x0, x1, x2].into_iter().flatten().chain(ps)
    }
}

impl OpticalKeywords for InnerOptical3_2 {
    fn opt_keywords_inner(&self, i: MeasIndex) -> impl Iterator<Item = OptOpticalKeyword<'_>> {
        let x0 = OptOpticalKeyword::from_str(&self.detector_name, i);
        let x1 = OptOpticalKeyword::from_str(&self.tag, i);
        let x2 = OptOpticalKeyword::from_str(&self.measurement_type, i);
        let x3 = OptOpticalKeyword::from_str(&self.analyte, i);
        let x4 = OptOpticalKeyword::from_wavelengths(&self.wavelengths, i);
        let x5 = self
            .calibration
            .as_ref()
            .map(|v| OptOpticalKeyword::from_ref(v, i));
        let x6 = self.display.map(|x| OptOpticalKeyword::from_value(x, i));
        let x7 = self
            .feature
            .as_ref()
            .map(|x| OptOpticalKeyword::from_ref(x, i));
        [x0, x1, x2, x3, x4, x5, x6, x7].into_iter().flatten()
    }
}

// Implement common methods to manipulate temporal keywords

pub trait TemporalKeywords: Sized + Versioned {
    fn req_meas_keywords_inner(&self, i: MeasIndex) -> Option<ReqMeasKeyword<'_>>;

    fn opt_meas_keywords_inner(&self, i: MeasIndex)
    -> impl Iterator<Item = OptTemporalKeyword<'_>>;
}

impl TemporalKeywords for InnerTemporal2_0 {
    fn req_meas_keywords_inner(&self, _: MeasIndex) -> Option<ReqMeasKeyword<'_>> {
        None
    }

    fn opt_meas_keywords_inner(
        &self,
        i: MeasIndex,
    ) -> impl Iterator<Item = OptTemporalKeyword<'_>> {
        // TODO awkward
        let s = OptTemporalKeyword::from_opt_zst(TemporalScale2_0::from(true), i);
        let ps = self.peak.opt_keywords(i).map(OptTemporalKeyword::from);
        ps.chain(s)
    }
}

impl TemporalKeywords for InnerTemporal3_0 {
    fn req_meas_keywords_inner(&self, i: MeasIndex) -> Option<ReqMeasKeyword<'_>> {
        Some(ReqMeasKeyword::from_value(TemporalScale3_0::default(), i))
    }

    fn opt_meas_keywords_inner(
        &self,
        i: MeasIndex,
    ) -> impl Iterator<Item = OptTemporalKeyword<'_>> {
        let ps = self.peak.opt_keywords(i).map(OptTemporalKeyword::from);
        ps.chain(once(OptTemporalKeyword::from_timestep(self.timestep)))
    }
}

impl TemporalKeywords for InnerTemporal3_1 {
    fn req_meas_keywords_inner(&self, i: MeasIndex) -> Option<ReqMeasKeyword<'_>> {
        Some(ReqMeasKeyword::from_value(TemporalScale3_0::default(), i))
    }

    fn opt_meas_keywords_inner(
        &self,
        i: MeasIndex,
    ) -> impl Iterator<Item = OptTemporalKeyword<'_>> {
        let ps = self.peak.opt_keywords(i).map(OptTemporalKeyword::from);
        let d = self.display.map(|v| OptTemporalKeyword::from_value(v, i));
        let t = OptTemporalKeyword::from_timestep(self.timestep);
        ps.chain(d).chain(once(t))
    }
}

impl TemporalKeywords for InnerTemporal3_2 {
    fn req_meas_keywords_inner(&self, i: MeasIndex) -> Option<ReqMeasKeyword<'_>> {
        Some(ReqMeasKeyword::from_value(TemporalScale3_0::default(), i))
    }

    fn opt_meas_keywords_inner(
        &self,
        i: MeasIndex,
    ) -> impl Iterator<Item = OptTemporalKeyword<'_>> {
        let d = self.display.map(|v| OptTemporalKeyword::from_value(v, i));
        let t = OptTemporalKeyword::from_timestep(self.timestep);
        d.into_iter().chain(once(t))
    }
}

// Implement trait to test if temporal can be converted to optical

pub trait TemporalMaybeToOptical: Sized + Versioned {
    type Warning;
    type Error;

    fn can_convert_to_optical(&self, i: MeasIndex) -> Result<(), Self::Error>;
}

impl TemporalMaybeToOptical for InnerTemporal2_0 {
    type Warning = Nothing<()>;
    type Error = Infallible;

    fn can_convert_to_optical(&self, _: MeasIndex) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl TemporalMaybeToOptical for InnerTemporal3_0 {
    type Warning = Nothing<()>;
    type Error = Infallible;

    fn can_convert_to_optical(&self, _: MeasIndex) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl TemporalMaybeToOptical for InnerTemporal3_1 {
    type Warning = Nothing<()>;
    type Error = Infallible;

    fn can_convert_to_optical(&self, _: MeasIndex) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl TemporalMaybeToOptical for InnerTemporal3_2 {
    type Warning = Option<AnyTemporalToOpticalKeyLossError>;
    type Error = AnyTemporalToOpticalKeyLossError;

    fn can_convert_to_optical(&self, i: MeasIndex) -> Result<(), Self::Error> {
        OptTemporalKeyword::from_opt_zst(self.measurement_type, i)
            .and_then(|x| x.as_optical_loss_error())
            .map_or(Ok(()), Err)
    }
}

// Implement common method to swap optical and temporal measurement

pub trait SwapOpticalWithTemporal<T: TemporalKeywords>: Sized + OpticalKeywords {
    /// Swap convert a temporal and optical channel into the other.
    ///
    /// This is necessary to have in one function since we may want to recover
    /// a bad conversion. Thus we need to first check if the two types can be
    /// converted into the other, and if so, actually do the conversion, and if
    /// not, return the originals with error(s).
    ///
    /// It may seem tempting to use two TryFroms to so this, but this won't work
    /// in the case where one conversion succeeds and the other fails. Rust's
    /// ownership model dictates that the successful conversion consume the
    /// original value, in which case we are stuck halfway with no path to
    /// recover the original state.
    #[allow(clippy::type_complexity)]
    fn swap_optical_temporal(
        old: (MeasIndex, Temporal<T>),
        new: (MeasIndex, Optical<Self>),
        flag: AllowLoss,
    ) -> SwitchableErrorResult<
        (Optical<Self>, Temporal<T>),
        (Temporal<T>, Optical<Self>),
        AllowLoss,
        SwapOpticalTemporalErrors,
    > {
        let go = |old_t: Temporal<T>, old_o: Optical<Self>| {
            let (so, st) = Self::swap_optical_temporal_inner(old_t.specific, old_o.specific);
            let f = Filter::default();
            let d = DetectorType::default();
            let new_o = Optical::new(old_t.common, f, None, d, None, None, so);
            let new_t = Temporal::new(old_o.common, st);
            (new_o, new_t)
        };

        let (tmp_index, tmp) = old;
        let (opt_index, opt) = new;

        let t_to_o_err = tmp
            .opt_meas_keywords(tmp_index)
            .filter_map(|x| x.as_optical_loss_error())
            .map(SwapOpticalTemporalError::from);
        let o_to_t_errs = opt
            .opt_keywords(opt_index)
            .filter_map(|x| x.as_temporal_loss_error());

        let es = o_to_t_errs
            .map(SwapOpticalTemporalError::from)
            .chain(t_to_o_err);

        let s = SwapOpticalTemporalSummary::new(opt_index, tmp_index);

        ErrorGroup::try_new_with(s, es)
            .into_deferred_switchable3(flag)
            .set_deferred_value((tmp, opt))
            .map_ok_value(|(t, o)| go(t, o))
    }

    fn swap_optical_temporal_inner(t: T, o: Self) -> (Self, T);
}

impl SwapOpticalWithTemporal<InnerTemporal2_0> for InnerOptical2_0 {
    fn swap_optical_temporal_inner(t: InnerTemporal2_0, o: Self) -> (Self, InnerTemporal2_0) {
        let new_t = InnerTemporal2_0::new(o.peak);
        let new_o = Self::new(None, t.peak);
        (new_o, new_t)
    }
}

impl SwapOpticalWithTemporal<InnerTemporal3_0> for InnerOptical3_0 {
    fn swap_optical_temporal_inner(t: InnerTemporal3_0, o: Self) -> (Self, InnerTemporal3_0) {
        let new_t = InnerTemporal3_0::new(t.timestep, o.peak);
        let new_o = Self::new(None, t.peak);
        (new_o, new_t)
    }
}

impl SwapOpticalWithTemporal<InnerTemporal3_1> for InnerOptical3_1 {
    fn swap_optical_temporal_inner(t: InnerTemporal3_1, o: Self) -> (Self, InnerTemporal3_1) {
        let new_t = InnerTemporal3_1::new(t.timestep, o.display, o.peak);
        let new_o = Self::new(Wavelengths::default(), None, t.display, t.peak);
        (new_o, new_t)
    }
}

impl SwapOpticalWithTemporal<InnerTemporal3_2> for InnerOptical3_2 {
    fn swap_optical_temporal_inner(t: InnerTemporal3_2, o: Self) -> (Self, InnerTemporal3_2) {
        let new_t = InnerTemporal3_2::new(t.timestep, o.display, TemporalType::default());
        let new_o = Self::new(
            Wavelengths::default(),
            None,
            t.display,
            Analyte::default(),
            None,
            OpticalType::default(),
            Tag::default(),
            DetectorName::default(),
        );
        (new_o, new_t)
    }
}

// Implement method to convert optical -> temporal conversion

pub trait TemporalFromOptical<O: OpticalKeywords>: Sized {
    type TData;

    fn from_optical<X>(
        opt: ScaledOptical<X, O>,
        i: MeasIndex,
        data: Self::TData,
        allow_loss: AllowLoss,
    ) -> SwitchableErrorResult<
        Temporal<Self>,
        ScaledOptical<X, O>,
        AllowLoss,
        OpticalToTemporalErrors,
    >
    where
        X: OpticalScaleKeywords,
    {
        let es = opt
            .opt_keywords(i)
            .filter_map(|x| x.as_temporal_loss_error());

        let s = OpticalToTemporalSummary::new(i);
        ErrorGroup::try_new_with(s, es)
            .into_deferred_switchable3::<_, Nothing<_>>(allow_loss)
            .set_deferred_value((opt, data))
            .map_ok_value(|(o, d)| Self::from_optical_unchecked(o.inner, d))
            .map_err_value(|(o, _)| o)
    }

    fn from_optical_unchecked(o: Optical<O>, d: Self::TData) -> Temporal<Self> {
        Temporal::new(o.common, Self::from_optical_inner(o.specific, d))
    }

    fn from_optical_inner(o: O, d: Self::TData) -> Self;
}

impl TemporalFromOptical<InnerOptical2_0> for InnerTemporal2_0 {
    type TData = ();

    fn from_optical_inner(o: InnerOptical2_0, (): Self::TData) -> Self {
        Self::new(o.peak)
    }
}

impl TemporalFromOptical<InnerOptical3_0> for InnerTemporal3_0 {
    type TData = Timestep;

    fn from_optical_inner(o: InnerOptical3_0, d: Self::TData) -> Self {
        Self::new(d, o.peak)
    }
}

impl TemporalFromOptical<InnerOptical3_1> for InnerTemporal3_1 {
    type TData = Timestep;

    fn from_optical_inner(o: InnerOptical3_1, d: Self::TData) -> Self {
        Self::new(d, o.display, o.peak)
    }
}

impl TemporalFromOptical<InnerOptical3_2> for InnerTemporal3_2 {
    type TData = Timestep;

    fn from_optical_inner(o: InnerOptical3_2, d: Self::TData) -> Self {
        Self::new(d, o.display, TemporalType::default())
    }
}

// Implement method to convert temporal -> optical conversion

pub trait OpticalFromTemporal<T: TemporalMaybeToOptical>: Sized {
    type TData;
    type LossFlag;

    #[allow(clippy::type_complexity)]
    fn from_temporal(
        tmp: Temporal<T>,
        i: MeasIndex,
        flag: Self::LossFlag,
    ) -> LogResult<
        (Optical<Self>, Self::TData),
        Temporal<T>,
        T::Warning,
        Nothing<()>,
        Self::LossFlag,
        T::Error,
        Nothing<T::Error>,
    >;

    fn from_temporal_unchecked(t: Temporal<T>) -> (Optical<Self>, Self::TData) {
        let (specific, td) = Self::from_temporal_inner(t.specific);
        let new = Optical::new(
            t.common,
            Filter::default(),
            None,
            DetectorType::default(),
            None,
            None,
            specific,
        );
        (new, td)
    }

    fn from_temporal_inner(t: T) -> (Self, Self::TData);
}

impl OpticalFromTemporal<InnerTemporal2_0> for InnerOptical2_0 {
    type TData = ();
    type LossFlag = ();

    #[allow(clippy::type_complexity)]
    fn from_temporal(
        tmp: Temporal<InnerTemporal2_0>,
        i: MeasIndex,
        (): Self::LossFlag,
    ) -> ErrorResult<(Optical<Self>, Self::TData), Temporal<InnerTemporal2_0>, Infallible> {
        let () = tmp.specific.can_convert_to_optical(i).unwrap_infallible();
        LogResult::new_ok(Self::from_temporal_unchecked(tmp))
    }

    fn from_temporal_inner(t: InnerTemporal2_0) -> (Self, Self::TData) {
        let new = Self::new(None, t.peak);
        (new, ())
    }
}

impl OpticalFromTemporal<InnerTemporal3_0> for InnerOptical3_0 {
    type TData = Timestep;
    type LossFlag = ();

    #[allow(clippy::type_complexity)]
    fn from_temporal(
        tmp: Temporal<InnerTemporal3_0>,
        i: MeasIndex,
        (): Self::LossFlag,
    ) -> ErrorResult<(Optical<Self>, Self::TData), Temporal<InnerTemporal3_0>, Infallible> {
        let () = tmp.specific.can_convert_to_optical(i).unwrap_infallible();
        LogResult::new_ok(Self::from_temporal_unchecked(tmp))
    }

    fn from_temporal_inner(t: InnerTemporal3_0) -> (Self, Self::TData) {
        let new = Self::new(None, t.peak);
        (new, t.timestep)
    }
}

impl OpticalFromTemporal<InnerTemporal3_1> for InnerOptical3_1 {
    type TData = Timestep;
    type LossFlag = ();

    #[allow(clippy::type_complexity)]
    fn from_temporal(
        tmp: Temporal<InnerTemporal3_1>,
        i: MeasIndex,
        (): Self::LossFlag,
    ) -> ErrorResult<(Optical<Self>, Self::TData), Temporal<InnerTemporal3_1>, Infallible> {
        let () = tmp.specific.can_convert_to_optical(i).unwrap_infallible();
        LogResult::new_ok(Self::from_temporal_unchecked(tmp))
    }

    fn from_temporal_inner(t: InnerTemporal3_1) -> (Self, Self::TData) {
        let new = Self::new(Wavelengths::default(), None, t.display, t.peak);
        (new, t.timestep)
    }
}

impl OpticalFromTemporal<InnerTemporal3_2> for InnerOptical3_2 {
    type TData = Timestep;
    type LossFlag = AllowLoss;

    #[allow(clippy::type_complexity)]
    fn from_temporal(
        tmp: Temporal<InnerTemporal3_2>,
        i: MeasIndex,
        flag: AllowLoss,
    ) -> SwitchableErrorResult<
        (Optical<Self>, Self::TData),
        Temporal<InnerTemporal3_2>,
        AllowLoss,
        AnyTemporalToOpticalKeyLossError,
    > {
        tmp.specific
            .can_convert_to_optical(i)
            .into_deferred_switchable3::<_, Nothing<_>>(flag)
            .set_deferred_value(tmp)
            .map_ok_value(Self::from_temporal_unchecked)
    }

    fn from_temporal_inner(t: InnerTemporal3_2) -> (Self, Self::TData) {
        let new = Self::new(
            Wavelengths::default(),
            None,
            t.display,
            Analyte::default(),
            None,
            OpticalType::default(),
            Tag::default(),
            DetectorName::default(),
        );
        (new, t.timestep)
    }
}

// Implement method to look up $PnN from a hash table

type LookupShortnameResult<V> =
    WarningAndErrorResult<V, (), OptIndexedKeyError<Shortname>, LookupShortnameError>;

pub trait LookupShortname: Sized {
    fn lookup_shortname(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        i: MeasIndex,
        conf: &ReadDataKeywordsConfig,
    ) -> LookupShortnameResult<Self>;
}

impl LookupShortname for Option<Shortname> {
    fn lookup_shortname(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        i: MeasIndex,
        conf: &ReadDataKeywordsConfig,
    ) -> LookupShortnameResult<Self> {
        Shortname::remove_or_drop_meas_opt(std, nonstd, i, conf)
            .set_err_value(())
            .switchable_into_commutative()
            .map_errors(LookupShortnameError::from)
    }
}

impl LookupShortname for Identity<Shortname> {
    fn lookup_shortname(
        std: &mut StdKeywords,
        _: &mut NonStdKeywords,
        i: MeasIndex,
        _: &ReadDataKeywordsConfig,
    ) -> LookupShortnameResult<Self> {
        Shortname::remove_meas_req(std, i)
            .map(Identity)
            .map_err(LookupShortnameError::from)
            .into_log()
    }
}

// Implement method to look up $PnE/$PnG from a hash table

type LookupOpticalScaleResult<S> =
    WarningsAndErrorsResult<S, (), LookupScaleWarning, LookupScaleError>;

pub trait LookupOpticalScale: Sized {
    fn lookup_optical_scale<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        i: MeasIndex,
        dt: AlphaNumType,
        conf: &C,
    ) -> LookupOpticalScaleResult<DiagnosedKeyword<Self, OpticalScaleFix>>
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadStdKeywordsConfig>;
}

impl LookupOpticalScale for OpticalScale2_0 {
    fn lookup_optical_scale<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        i: MeasIndex,
        dt: AlphaNumType,
        conf: &C,
    ) -> LookupOpticalScaleResult<DiagnosedKeyword<Self, OpticalScaleFix>>
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadStdKeywordsConfig>,
    {
        Scale::remove_or_drop_meas_opt_with(std, nonstd, i, dt, conf)
            .map_switchable_errors(LookupScaleWarning::from)
            .switchable_into_commutative()
            .map_errors(LookupScaleError::from)
            .map_ok_value(|diag| diag.first_once(|x: Option<Scale>| Self(x)))
            .set_err_value(())
            .into_semigroup()
    }
}

impl LookupOpticalScale for OpticalScale3_0 {
    fn lookup_optical_scale<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        i: MeasIndex,
        dt: AlphaNumType,
        conf: &C,
    ) -> LookupOpticalScaleResult<DiagnosedKeyword<Self, OpticalScaleFix>>
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadStdKeywordsConfig>,
    {
        let gain = Gain::remove_or_drop_meas_opt(std, nonstd, i, conf.as_ref())
            .map_switchable_errors(LookupScaleWarning::from)
            .switchable_into_commutative()
            .map_errors(LookupScaleError::from)
            .into_semigroup();
        let scale = Scale::remove_meas_req_with(std, i, dt, conf.as_ref())
            .map_err(LookupScaleError::from)
            .into_log();
        gain.zip_commutative(scale).and_then_commutative(|(g, s)| {
            Self::try_from((s.native, g))
                .map(|x| DiagnosedKeyword::new(x, s.diagnostic))
                .map_err(LookupScaleError::from)
                .into_log()
        })
    }
}

// Implement method to look up optical keywords from a hash table

type LookupOpticalResult<V> =
    WarningsAndErrorsResult<V, (), LookupOpticalWarning, LookupOpticalError>;

pub trait LookupOptical: Sized + OpticalKeywords {
    fn lookup_specific<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        i: MeasIndex,
        conf: &C,
    ) -> LookupOpticalResult<DiagnosedOptical<Self>>
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadStdKeywordsConfig>;
}

impl LookupOptical for InnerOptical2_0 {
    fn lookup_specific<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        i: MeasIndex,
        conf: &C,
    ) -> LookupOpticalResult<DiagnosedOptical<Self>>
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadStdKeywordsConfig>,
    {
        let wave = Wavelength::remove_or_drop_meas_opt(std, nonstd, i, conf.as_ref())
            .map_switchable_errors(LookupOpticalWarning::from)
            .switchable_into_commutative()
            .into_semigroup();
        let peak = PeakData::lookup(std, nonstd, i, conf.as_ref())
            .map_warnings_and_errors(LookupOpticalWarning::from);
        wave.zip_commutative(peak)
            .map_errors(LookupOpticalError::from)
            .map_ok_value(|(wi, pi)| {
                let ret = Self::new(wi, pi);
                DiagnosedOptical::new(ret, vec![])
            })
    }
}

impl LookupOptical for InnerOptical3_0 {
    fn lookup_specific<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        i: MeasIndex,
        conf: &C,
    ) -> LookupOpticalResult<DiagnosedOptical<Self>>
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadStdKeywordsConfig>,
    {
        let wave = Wavelength::remove_or_drop_meas_opt(std, nonstd, i, conf.as_ref())
            .map_switchable_errors(LookupOpticalWarning::from)
            .switchable_into_commutative()
            .into_semigroup();
        let peak = PeakData::lookup(std, nonstd, i, conf.as_ref())
            .map_warnings_and_errors(LookupOpticalWarning::from);
        // let scale = OpticalTransform3_0::lookup(std, nonstd, i, dt, conf);
        wave.zip_commutative(peak)
            .map_errors(LookupOpticalError::from)
            // .zip_commutative(scale)
            .map_ok_value(|(w, p)| {
                let ret = Self::new(w, p);
                DiagnosedOptical::new(ret, vec![])
            })
    }
}

impl LookupOptical for InnerOptical3_1 {
    fn lookup_specific<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        i: MeasIndex,
        conf: &C,
    ) -> LookupOpticalResult<DiagnosedOptical<Self>>
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadStdKeywordsConfig>,
    {
        macro_rules! go {
            ($x:expr) => {
                $x.map_switchable_errors(LookupOpticalWarning::from)
                    .switchable_into_commutative()
                    .into_semigroup()
            };
        }
        let wave = Wavelengths::remove_or_drop_meas_opt_with(std, nonstd, i, (), conf);
        let cal = Calibration3_1::remove_or_drop_meas_opt_with(std, nonstd, i, (), conf);
        let dpy = Display::remove_or_drop_meas_opt_with(std, nonstd, i, (), conf);
        let peak = PeakData::lookup(std, nonstd, i, conf.as_ref())
            .map_warnings_and_errors(LookupOpticalWarning::from);
        // let scale = OpticalTransform3_0::lookup(std, nonstd, i, dt, conf);
        go!(wave)
            .zip4_commutative(go!(cal), go!(dpy), peak)
            .map_errors(LookupOpticalError::from)
            .map_ok_value(|(w_out, c_out, d_out, p)| {
                let (w, w_trimmed) = w_out.into_indexed_pair(i);
                let (c, c_trimmed) = c_out.into_opt_indexed_pair(i.into());
                let (d, d_trimmed) = d_out.into_opt_indexed_pair(i.into());
                let ret = Self::new(w, c, d, p);
                let trimmed = w_trimmed
                    .into_iter()
                    .chain(c_trimmed)
                    .chain(d_trimmed)
                    .collect();
                DiagnosedOptical::new(ret, trimmed)
            })
    }
}

impl LookupOptical for InnerOptical3_2 {
    fn lookup_specific<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        i: MeasIndex,
        conf: &C,
    ) -> LookupOpticalResult<DiagnosedOptical<Self>>
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadStdKeywordsConfig>,
    {
        macro_rules! go {
            ($x:expr) => {
                $x.map_switchable_errors(LookupOpticalWarning::from)
                    .switchable_into_commutative()
                    .map_errors(LookupOpticalError::from)
                    .into_semigroup()
            };
        }

        let wave = Wavelengths::remove_or_drop_meas_opt_with(std, nonstd, i, (), conf);
        let cal = Calibration3_2::remove_or_drop_meas_opt_with(std, nonstd, i, (), conf);
        let dpy = Display::remove_or_drop_meas_opt_with(std, nonstd, i, (), conf);
        let meas = OpticalType::remove_or_drop_meas_opt(std, nonstd, i, conf.as_ref());
        let feat = Feature::remove_or_drop_meas_opt_with(std, nonstd, i, (), conf);

        let det_name = DetectorName::remove_meas_opt_nofail(std, i);
        let tag = Tag::remove_meas_opt_nofail(std, i);
        let anal = Analyte::remove_meas_opt_nofail(std, i);

        // let scale = OpticalTransform3_0::lookup(std, nonstd, i, dt, conf);

        go!(wave)
            .zip5_commutative(go!(cal), go!(dpy), go!(meas), go!(feat))
            .map_ok_value(|(w_out, c_out, d_out, m, f)| {
                let (w, w_trimmed) = w_out.into_indexed_pair(i);
                let (c, c_trimmed) = c_out.into_opt_indexed_pair(i.into());
                let (d, d_trimmed) = d_out.into_opt_indexed_pair(i.into());
                let ret = Self::new(w, c, d, anal, f.native, m, tag, det_name);
                let trimmed = c_trimmed
                    .into_iter()
                    .chain(w_trimmed)
                    .chain(d_trimmed)
                    .collect();
                DiagnosedOptical::new(ret, trimmed)
            })
    }
}

// Implement method to look up temporal keywords from a hash table

type LookupTemporalResult<V> =
    WarningsAndErrorsResult<V, (), LookupTemporalWarning, LookupTemporalError>;

pub trait LookupTemporal: TemporalKeywords {
    fn lookup_specific<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        i: MeasIndex,
        conf: &C,
    ) -> LookupTemporalResult<DiagnosedTemporal<Self>>
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadStdKeywordsConfig>;
}

impl LookupTemporal for InnerTemporal2_0 {
    fn lookup_specific<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        i: MeasIndex,
        conf: &C,
    ) -> LookupTemporalResult<DiagnosedTemporal<Self>>
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadStdKeywordsConfig>,
    {
        let sconf: &ReadStdKeywordsConfig = conf.as_ref();
        let flag = sconf.process_time_optical_keys;
        let ignore = &sconf.ignore_time_optical_keys;
        let scale = TemporalScale2_0::remove_or_drop_meas_opt_with(std, nonstd, i, (), conf)
            .map_switchable_errors(LookupTemporalWarning::from)
            .switchable_into_commutative()
            .into_semigroup();
        let peak = PeakData::lookup(std, nonstd, i, conf.as_ref())
            .map_warnings_and_errors(LookupTemporalWarning::from);
        let tgts = TemporalOpticalKey::TARGETS_2_0;
        let tmp_opt_res = ignore
            .remove(&tgts, std, nonstd, i, flag)
            .map_warnings_and_errors(LookupTemporalWarning::from);
        scale
            .zip3_commutative(peak, tmp_opt_res)
            .map_errors(LookupTemporalError::from)
            .map_ok_value(|(s, p, tmp_opt_pairs)| {
                let this = Self::new(p);
                DiagnosedTemporal::new(this, s.diagnostic, vec![], tmp_opt_pairs, false)
            })
    }
}

impl LookupTemporal for InnerTemporal3_0 {
    fn lookup_specific<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        i: MeasIndex,
        conf: &C,
    ) -> LookupTemporalResult<DiagnosedTemporal<Self>>
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadStdKeywordsConfig>,
    {
        let sconf: &ReadStdKeywordsConfig = conf.as_ref();
        let flag = sconf.process_time_optical_keys;
        let ignore = &sconf.ignore_time_optical_keys;
        let gain = Gain::lookup_temporal_3_0(std, nonstd, i, conf)
            .map_switchable_errors(LookupTemporalWarning::from)
            .switchable_into_commutative();
        let peak = PeakData::lookup(std, nonstd, i, conf.as_ref())
            .map_warnings_and_errors(LookupTemporalWarning::from);
        let tgts = TemporalOpticalKey::TARGETS_3_0;
        let tmp_opt = ignore
            .remove(&tgts, std, nonstd, i, flag)
            .map_warnings_and_errors(LookupTemporalWarning::from);
        let scale = TemporalScale3_0::remove_meas_req_with(std, i, (), conf.as_ref())
            .map_err(LookupTemporalError::from);
        let timestep = Timestep::lookup(std, conf.as_ref()).map_err(LookupTemporalError::from);
        let req_res = scale.zip(timestep);
        gain.zip3_commutative(peak, tmp_opt)
            .map_errors(LookupTemporalError::from)
            .zip_commutative(req_res)
            .map_ok_value(|((_, p, tmp_opt_pairs), (s, t))| {
                let this = Self::new(t.native, p);
                DiagnosedTemporal::new(this, s.diagnostic, vec![], tmp_opt_pairs, t.diagnostic)
            })
    }
}

impl LookupTemporal for InnerTemporal3_1 {
    fn lookup_specific<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        i: MeasIndex,
        conf: &C,
    ) -> LookupTemporalResult<DiagnosedTemporal<Self>>
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadStdKeywordsConfig>,
    {
        let sconf: &ReadStdKeywordsConfig = conf.as_ref();
        let flag = sconf.process_time_optical_keys;
        let ignore = &sconf.ignore_time_optical_keys;
        let gain = Gain::lookup_temporal_3_0(std, nonstd, i, conf)
            .map_switchable_errors(LookupTemporalWarning::from)
            .switchable_into_commutative();
        let dpy = Display::remove_or_drop_meas_opt_with(std, nonstd, i, (), conf)
            .map_switchable_errors(LookupTemporalWarning::from)
            .switchable_into_commutative()
            .into_semigroup();
        let peak = PeakData::lookup(std, nonstd, i, conf.as_ref())
            .map_warnings_and_errors(LookupTemporalWarning::from);
        let tgts = TemporalOpticalKey::TARGETS_3_1;
        let tmp_opt = ignore
            .remove(&tgts, std, nonstd, i, flag)
            .map_warnings_and_errors(LookupTemporalWarning::from);
        let scale = TemporalScale3_0::remove_meas_req_with(std, i, (), conf.as_ref())
            .map_err(LookupTemporalError::from);
        let timestep = Timestep::lookup(std, conf.as_ref()).map_err(LookupTemporalError::from);
        let req_res = scale.zip(timestep);
        gain.zip4_commutative(dpy, peak, tmp_opt)
            .map_errors(LookupTemporalError::from)
            .zip_commutative(req_res)
            .map_ok_value(|((_, d_out, p, tmp_opt_pairs), (s, t))| {
                let (d, d_trimmed) = d_out.into_opt_indexed_pair(i.into());
                let trimmed = d_trimmed.into_iter().collect();
                let ret = Self::new(t.native, d, p);
                DiagnosedTemporal::new(ret, s.diagnostic, trimmed, tmp_opt_pairs, t.diagnostic)
            })
    }
}

impl LookupTemporal for InnerTemporal3_2 {
    fn lookup_specific<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        i: MeasIndex,
        conf: &C,
    ) -> LookupTemporalResult<DiagnosedTemporal<Self>>
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadStdKeywordsConfig>,
    {
        let sconf: &ReadStdKeywordsConfig = conf.as_ref();
        let flag = sconf.process_time_optical_keys;
        let ignore = &sconf.ignore_time_optical_keys;
        let gain = Gain::lookup_temporal_3_0(std, nonstd, i, conf)
            .map_switchable_errors(LookupTemporalWarning::from)
            .switchable_into_commutative();
        let dpy = Display::remove_or_drop_meas_opt_with(std, nonstd, i, (), conf)
            .map_switchable_errors(LookupTemporalWarning::from)
            .switchable_into_commutative()
            .into_semigroup();
        let meas = TemporalType::remove_or_drop_meas_opt(std, nonstd, i, conf.as_ref())
            .map_switchable_errors(LookupTemporalWarning::from)
            .switchable_into_commutative()
            .into_semigroup();
        let tgts = TemporalOpticalKey::TARGETS_3_2;
        let tmp_opt = ignore
            .remove(&tgts, std, nonstd, i, flag)
            .map_warnings_and_errors(LookupTemporalWarning::from);
        let scale = TemporalScale3_0::remove_meas_req_with(std, i, (), conf.as_ref())
            .map_err(LookupTemporalError::from);
        let timestep = Timestep::lookup(std, conf.as_ref()).map_err(LookupTemporalError::from);
        let req_res = scale.zip(timestep);
        gain.zip4_commutative(dpy, meas, tmp_opt)
            .map_errors(LookupTemporalError::from)
            .zip_commutative(req_res)
            .map_ok_value(|((_, d_out, m, tmp_opt_pairs), (s, t))| {
                let (d, d_trimmed) = d_out.into_opt_indexed_pair(i.into());
                let trimmed = d_trimmed.into_iter().collect();
                let ret = Self::new(t.native, d, m);
                DiagnosedTemporal::new(ret, s.diagnostic, trimmed, tmp_opt_pairs, t.diagnostic)
            })
    }
}

// Implement method to convert $PnN values between versions
//
// In this case, there are only two types for $PnN so this only requires three
// impls (one for identity)

pub trait ConvertFromShortname<T>: Sized + MightHave<Shortname> {
    fn convert_from_shortname(value: T, i: MeasIndex) -> Result<Self, NameConversionError>;
}

impl<T: MightHave<Shortname>> ConvertFromShortname<T> for T {
    fn convert_from_shortname(value: T, _: MeasIndex) -> Result<Self, NameConversionError> {
        Ok(value)
    }
}

impl ConvertFromShortname<Option<Shortname>> for Identity<Shortname> {
    fn convert_from_shortname(
        value: Option<Shortname>,
        i: MeasIndex,
    ) -> Result<Self, NameConversionError> {
        value
            .ok_or_else(|| NameConversionError(Key1::new_i1(i)))
            .map(Identity)
    }
}

impl ConvertFromShortname<Identity<Shortname>> for Option<Shortname> {
    fn convert_from_shortname(
        value: Identity<Shortname>,
        _: MeasIndex,
    ) -> Result<Self, NameConversionError> {
        Ok(Some(value.0))
    }
}

// Implement method to convert optical keyword values between versions

type OpticalConvertResult<M> =
    WarningsAndErrorsResult<M, (), OpticalConvertWarning, OpticalConvertError>;

pub trait ConvertFromOptical<O: OpticalKeywords>: Sized + OpticalKeywords {
    fn convert_from_optical_inner(
        value: O,
        i: MeasIndex,
        flag: AllowLoss,
    ) -> OpticalConvertResult<Self>;

    fn convert_from_optical(value: O, i: MeasIndex, flag: AllowLoss) -> OpticalConvertResult<Self> {
        let target_version = Self::Ver::as_version();
        let es: Vec<_> = value
            .opt_keywords_inner(i)
            .filter(|x| !x.contains_version(target_version))
            .filter_map(|k| k.as_loss_error())
            .map(OpticalConvertWarning::from)
            .collect();
        let res = Self::convert_from_optical_inner(value, i, flag);
        res.extend_warnings_or_errors3(es, |_| (), |w| w, OpticalConvertError::from, flag)
    }
}

impl ConvertFromOptical<InnerOptical3_0> for InnerOptical2_0 {
    fn convert_from_optical_inner(
        value: InnerOptical3_0,
        _: MeasIndex,
        _: AllowLoss,
    ) -> OpticalConvertResult<Self> {
        LogResult::new_ok(Self::new(value.wavelength, value.peak))
    }
}

impl ConvertFromOptical<InnerOptical3_1> for InnerOptical2_0 {
    fn convert_from_optical_inner(
        value: InnerOptical3_1,
        i: MeasIndex,
        flag: AllowLoss,
    ) -> OpticalConvertResult<Self> {
        value
            .wavelengths
            .into_wavelength(i)
            .map_errors(OpticalConvertWarning::from)
            .into_semigroup()
            .map_deferred_value(|w| Self::new(w, value.peak))
            .nowarn_into_switchable3(flag)
            .switchable_into_commutative()
            .map_errors(OpticalConvertError::from)
            .set_err_value(())
    }
}

impl ConvertFromOptical<InnerOptical3_2> for InnerOptical2_0 {
    fn convert_from_optical_inner(
        value: InnerOptical3_2,
        i: MeasIndex,
        flag: AllowLoss,
    ) -> OpticalConvertResult<Self> {
        value
            .wavelengths
            .into_wavelength(i)
            .map_errors(OpticalConvertWarning::from)
            .into_semigroup()
            .map_deferred_value(|w| Self::new(w, PeakData::default()))
            .nowarn_into_switchable3(flag)
            .switchable_into_commutative()
            .map_errors(OpticalConvertError::from)
            .set_err_value(())
    }
}

impl ConvertFromOptical<InnerOptical2_0> for InnerOptical3_0 {
    fn convert_from_optical_inner(
        value: InnerOptical2_0,
        _: MeasIndex,
        _: AllowLoss,
    ) -> OpticalConvertResult<Self> {
        LogResult::new_ok(Self::new(value.wavelength, value.peak))
    }
}

impl ConvertFromOptical<InnerOptical3_1> for InnerOptical3_0 {
    fn convert_from_optical_inner(
        value: InnerOptical3_1,
        i: MeasIndex,
        flag: AllowLoss,
    ) -> OpticalConvertResult<Self> {
        value
            .wavelengths
            .into_wavelength(i)
            .map_errors(OpticalConvertWarning::from)
            .repack_errors::<Vec<_>>()
            .nowarn_into_switchable3(flag)
            .switchable_into_commutative()
            .map_errors(OpticalConvertError::from)
            .set_err_value(())
            .map_ok_value(|w| Self::new(w, value.peak))
    }
}

impl ConvertFromOptical<InnerOptical3_2> for InnerOptical3_0 {
    fn convert_from_optical_inner(
        value: InnerOptical3_2,
        i: MeasIndex,
        flag: AllowLoss,
    ) -> OpticalConvertResult<Self> {
        value
            .wavelengths
            .into_wavelength(i)
            .map_errors(OpticalConvertWarning::from)
            .repack_errors::<Vec<_>>()
            .nowarn_into_switchable3(flag)
            .switchable_into_commutative()
            .map_errors(OpticalConvertError::from)
            .set_err_value(())
            .map_ok_value(|w| Self::new(w, PeakData::default()))
    }
}

impl ConvertFromOptical<InnerOptical2_0> for InnerOptical3_1 {
    fn convert_from_optical_inner(
        value: InnerOptical2_0,
        _: MeasIndex,
        _: AllowLoss,
    ) -> OpticalConvertResult<Self> {
        let wave = value.wavelength.map(Wavelengths::from).unwrap_or_default();
        LogResult::new_ok(Self::new(wave, None, None, value.peak))
    }
}

impl ConvertFromOptical<InnerOptical3_0> for InnerOptical3_1 {
    fn convert_from_optical_inner(
        value: InnerOptical3_0,
        _: MeasIndex,
        _: AllowLoss,
    ) -> OpticalConvertResult<Self> {
        let wave = value.wavelength.map(Wavelengths::from).unwrap_or_default();
        LogResult::new_ok(Self::new(wave, None, None, value.peak))
    }
}

impl ConvertFromOptical<InnerOptical3_2> for InnerOptical3_1 {
    fn convert_from_optical_inner(
        value: InnerOptical3_2,
        i: MeasIndex,
        flag: AllowLoss,
    ) -> OpticalConvertResult<Self> {
        let cal_res = value
            .calibration
            .map(|c| {
                c.into_3_1(i)
                    .nowarn_into_switchable3(flag)
                    .map_switchable_errors(OpticalConvertWarning::from)
                    .switchable_into_commutative()
                    .into_semigroup()
            })
            .transpose_log_result();

        cal_res
            .map_errors(OpticalConvertError::from)
            .set_err_value(())
            .map_ok_value(|cal| {
                Self::new(value.wavelengths, cal, value.display, PeakData::default())
            })
    }
}

impl ConvertFromOptical<InnerOptical2_0> for InnerOptical3_2 {
    fn convert_from_optical_inner(
        value: InnerOptical2_0,
        _: MeasIndex,
        _: AllowLoss,
    ) -> OpticalConvertResult<Self> {
        let wave = value.wavelength.map(Wavelengths::from).unwrap_or_default();
        LogResult::new_ok(Self::new(
            wave,
            None,
            None,
            Analyte::default(),
            None,
            OpticalType::default(),
            Tag::default(),
            DetectorName::default(),
        ))
    }
}

impl ConvertFromOptical<InnerOptical3_0> for InnerOptical3_2 {
    fn convert_from_optical_inner(
        value: InnerOptical3_0,
        _: MeasIndex,
        _: AllowLoss,
    ) -> OpticalConvertResult<Self> {
        let wave = value.wavelength.map(Wavelengths::from).unwrap_or_default();
        LogResult::new_ok(Self::new(
            wave,
            None,
            None,
            Analyte::default(),
            None,
            OpticalType::default(),
            Tag::default(),
            DetectorName::default(),
        ))
    }
}

impl ConvertFromOptical<InnerOptical3_1> for InnerOptical3_2 {
    fn convert_from_optical_inner(
        value: InnerOptical3_1,
        _: MeasIndex,
        _: AllowLoss,
    ) -> OpticalConvertResult<Self> {
        LogResult::new_ok(Self::new(
            value.wavelengths,
            value.calibration.map(Into::into),
            value.display,
            Analyte::default(),
            None,
            OpticalType::default(),
            Tag::default(),
            DetectorName::default(),
        ))
    }
}

// Implement method to convert temporal keyword values between versions

type TemporalConvertResult<M> = DeferredSwitchableErrors<M, AllowLoss, AnyTemporalKeyLossError>;

pub trait ConvertFromTemporal<T: TemporalKeywords>: Sized + TemporalKeywords {
    fn convert_from_temporal_inner(value: T) -> Self;

    fn convert_from_temporal(
        value: T,
        i: MeasIndex,
        flag: AllowLoss,
    ) -> TemporalConvertResult<Self> {
        let target_version = Self::Ver::as_version();
        let es: Vec<_> = value
            .opt_meas_keywords_inner(i)
            .filter(|x| !x.contains_version(target_version))
            .filter_map(|k| k.as_loss_error())
            .collect();
        let res = Self::convert_from_temporal_inner(value);
        LogResult::new_deferred_switchable_iter3(res, es, flag)
    }
}

impl ConvertFromTemporal<InnerTemporal3_0> for InnerTemporal2_0 {
    fn convert_from_temporal_inner(value: InnerTemporal3_0) -> Self {
        Self::new(value.peak)
    }
}

impl ConvertFromTemporal<InnerTemporal3_1> for InnerTemporal2_0 {
    fn convert_from_temporal_inner(value: InnerTemporal3_1) -> Self {
        Self::new(value.peak)
    }
}

impl ConvertFromTemporal<InnerTemporal3_2> for InnerTemporal2_0 {
    fn convert_from_temporal_inner(_: InnerTemporal3_2) -> Self {
        Self::new(PeakData::default())
    }
}

impl ConvertFromTemporal<InnerTemporal2_0> for InnerTemporal3_0 {
    fn convert_from_temporal_inner(value: InnerTemporal2_0) -> Self {
        Self::new(Timestep::default(), value.peak)
    }
}

impl ConvertFromTemporal<InnerTemporal3_1> for InnerTemporal3_0 {
    fn convert_from_temporal_inner(value: InnerTemporal3_1) -> Self {
        Self::new(value.timestep, value.peak)
    }
}

impl ConvertFromTemporal<InnerTemporal3_2> for InnerTemporal3_0 {
    fn convert_from_temporal_inner(value: InnerTemporal3_2) -> Self {
        Self::new(value.timestep, PeakData::default())
    }
}

impl ConvertFromTemporal<InnerTemporal2_0> for InnerTemporal3_1 {
    fn convert_from_temporal_inner(value: InnerTemporal2_0) -> Self {
        Self::new(Timestep::default(), None, value.peak)
    }
}

impl ConvertFromTemporal<InnerTemporal3_0> for InnerTemporal3_1 {
    fn convert_from_temporal_inner(value: InnerTemporal3_0) -> Self {
        Self::new(value.timestep, None, value.peak)
    }
}

impl ConvertFromTemporal<InnerTemporal3_2> for InnerTemporal3_1 {
    fn convert_from_temporal_inner(value: InnerTemporal3_2) -> Self {
        Self::new(value.timestep, value.display, PeakData::default())
    }
}

impl ConvertFromTemporal<InnerTemporal2_0> for InnerTemporal3_2 {
    fn convert_from_temporal_inner(_: InnerTemporal2_0) -> Self {
        Self::new(Timestep::default(), None, TemporalType::default())
    }
}

impl ConvertFromTemporal<InnerTemporal3_0> for InnerTemporal3_2 {
    fn convert_from_temporal_inner(value: InnerTemporal3_0) -> Self {
        Self::new(value.timestep, None, TemporalType::default())
    }
}

impl ConvertFromTemporal<InnerTemporal3_1> for InnerTemporal3_2 {
    fn convert_from_temporal_inner(value: InnerTemporal3_1) -> Self {
        Self::new(value.timestep, value.display, TemporalType::default())
    }
}

// Implement method to convert between different scale types

type ScaleConvertResult<S> = WarningAndErrorResult<S, (), GainLossError, ScaleConvertError>;

pub trait ConvertFromScale<S>: Sized {
    fn convert_from_scale(value: S, i: MeasIndex, flag: AllowLoss) -> ScaleConvertResult<Self>;
}

impl<T> ConvertFromScale<T> for T {
    fn convert_from_scale(value: T, _: MeasIndex, _: AllowLoss) -> ScaleConvertResult<Self> {
        LogResult::new_ok(value)
    }
}

impl ConvertFromScale<OpticalScale2_0> for OpticalScale3_0 {
    fn convert_from_scale(
        value: OpticalScale2_0,
        i: MeasIndex,
        _: AllowLoss,
    ) -> ScaleConvertResult<Self> {
        if let Some(s) = value.0 {
            LogResult::new_ok(s.into())
        } else {
            LogResult::new_err(NoScaleError(i).into())
        }
    }
}

impl ConvertFromScale<OpticalScale3_0> for OpticalScale2_0 {
    fn convert_from_scale(
        value: OpticalScale3_0,
        i: MeasIndex,
        flag: AllowLoss,
    ) -> ScaleConvertResult<Self> {
        value
            .try_convert_to_scale(i)
            .nowarn_into_switchable3(flag)
            .switchable_into_commutative()
            .map_errors(ScaleConvertError::from)
            .map_ok_value(|s| Self(Some(s)))
            .set_err_value(())
    }
}

// Implement checks for scale transform against $DATATYPE
//
// Log scale transforms can only be used when $DATATYPE=I

/// A scale transform which may be checked against a datatype to ensure compatibility
pub trait CheckedScaleTransform {
    const HAS_GAIN: bool;

    fn matches_datatype(
        &self,
        datatype: &AlphaNumType,
        i: MeasIndex,
    ) -> Result<(), ScaleDatatypeMismatchError> {
        self.matches_datatype_log(datatype)
            .map_err(|s| ScaleDatatypeMismatchError::new(i, *datatype, s, Self::HAS_GAIN))
    }

    fn matches_datatype_log(&self, datatype: &AlphaNumType) -> Result<(), LogScale> {
        // Only integers are allowed to have gain and log scaling, so everything
        // else should be a "noop" transform (ie a linear transform with slope
        // of 1.0). NOTE the standard itself is vague about what should happen
        // to ASCII values (presumably since nobody cares) so here we just treat
        // them like we treat floating point types to keep the logic simple.
        if let Some(s) = self.as_log()
            && datatype != &AlphaNumType::Integer
        {
            Err(s)
        } else {
            Ok(())
        }
    }

    fn as_log(&self) -> Option<LogScale>;

    fn is_identity(&self) -> bool;
}

impl CheckedScaleTransform for OpticalScale2_0 {
    const HAS_GAIN: bool = false;

    fn as_log(&self) -> Option<LogScale> {
        if let Some(Scale::Log(s)) = self.0 {
            Some(s)
        } else {
            None
        }
    }

    fn is_identity(&self) -> bool {
        // TODO we assume blank == linear, is this right?
        self.0.is_none_or(|s| s == Scale::Linear)
    }
}

impl CheckedScaleTransform for OpticalScale3_0 {
    const HAS_GAIN: bool = true;

    fn as_log(&self) -> Option<LogScale> {
        if let Self::Log(s) = self {
            Some(*s)
        } else {
            None
        }
    }

    fn is_identity(&self) -> bool {
        *self == Self::default()
    }
}

// Implement methods for scaled optical wrapper type

type ScaledOpticalConvertResult<X, O> = WarningsAndErrorsResult<
    ScaledOptical<X, O>,
    (),
    ScaledOpticalConvertWarning,
    ScaledOpticalConvertError,
>;

impl<X, O> ScaledOptical<X, O> {
    pub fn inner(&self) -> &Optical<O> {
        &self.inner
    }

    pub fn scale(&self) -> &X {
        &self.scale
    }

    pub(crate) fn new_identity(inner: Optical<O>) -> Self
    where
        X: Default,
    {
        Self::new(inner, X::default())
    }

    pub(crate) fn lookup_scaled_optical<C>(
        std: &mut StdKeywords,
        mut nonstd: NonStdKeywords,
        i: MeasIndex,
        dt: AlphaNumType,
        conf: &C,
    ) -> WarningsAndErrorsResult<
        DiagnosedScaledOptical<Self>,
        (),
        LookupScaledOpticalWarning,
        LookupScaledOpticalError,
    >
    where
        X: LookupOpticalScale,
        O: LookupOptical,
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadStdKeywordsConfig>,
    {
        let s_res = X::lookup_optical_scale(std, &mut nonstd, i, dt, conf)
            .map_errors(LookupScaledOpticalError::from)
            .map_commutative_warnings(LookupScaledOpticalWarning::from);
        let o_res = Optical::lookup_optical(std, nonstd, i, conf)
            .map_errors(LookupScaledOpticalError::from)
            .map_commutative_warnings(LookupScaledOpticalWarning::from);
        s_res.zip_commutative(o_res).map_ok_value(|(s, o)| {
            DiagnosedScaledOptical::new(Self::new(o.this, s.native), s.diagnostic, o.trimmed)
        })
    }

    pub(crate) fn req_keywords(&self, i: MeasIndex) -> impl Iterator<Item = ReqMeasKeyword<'_>>
    where
        O: OpticalKeywords,
        X: OpticalScaleKeywords,
    {
        self.scale.req_keywords(i)
    }

    pub(crate) fn opt_keywords(
        &self,
        i: MeasIndex,
    ) -> impl Iterator<Item = OptScaledOpticalKeyword<'_>>
    where
        O: OpticalKeywords,
        X: OpticalScaleKeywords,
    {
        let os = self
            .inner
            .opt_keywords(i)
            .map(OptScaledOpticalKeyword::from);
        let ss = self
            .scale
            .opt_keywords(i)
            .map(OptScaledOpticalKeyword::from);
        os.chain(ss)
    }

    pub(crate) fn opt_and_nonstd_keywords(
        &self,
        i: MeasIndex,
    ) -> impl Iterator<Item = StdOrNonStdOptMeasKeyword<'_>>
    where
        O: OpticalKeywords,
        X: OpticalScaleKeywords,
    {
        let cs = self
            .inner
            .common
            .nonstandard_keywords
            .iter()
            .map(|(k, v)| NonStdKeyword::new(k, v.as_ne_str()))
            .map(StdOrNonStdOptMeasKeyword::from);
        self.opt_keywords(i)
            .map(OptMeasKeyword::from)
            .map(StdOrNonStdOptMeasKeyword::from)
            .chain(cs)
    }

    fn try_convert<Xf, Of>(
        self,
        i: MeasIndex,
        flag: AllowLoss,
    ) -> ScaledOpticalConvertResult<Xf, Of>
    where
        O: OpticalKeywords,
        Of: ConvertFromOptical<O>,
        Xf: ConvertFromScale<X>,
    {
        let new_opt = self
            .inner
            .try_convert(i, flag)
            .map_commutative_warnings(ScaledOpticalConvertWarning::from)
            .map_errors(ScaledOpticalConvertError::from);
        let new_scale = Xf::convert_from_scale(self.scale, i, flag)
            .map_commutative_warnings(ScaledOpticalConvertWarning::from)
            .map_errors(ScaledOpticalConvertError::from)
            .into_semigroup();
        new_opt
            .zip_commutative(new_scale)
            .map_ok_value(|(o, s)| ScaledOptical::new(o, s))
    }

    #[allow(clippy::type_complexity)]
    fn swap_optical_temporal<T>(
        old: (MeasIndex, Temporal<T>),
        new: (MeasIndex, Self),
        allow_loss: AllowLoss,
    ) -> SwitchableErrorResult<
        (Self, Temporal<T>),
        (Temporal<T>, Self),
        AllowLoss,
        SwapOpticalTemporalErrors,
    >
    where
        T: TemporalKeywords,
        O: SwapOpticalWithTemporal<T>,
    {
        let (new_i, new_o) = new;
        O::swap_optical_temporal(old, (new_i, new_o.inner), allow_loss)
            .inject_value(new_o.scale)
            .map_ok_value(|((o, t), s)| (Self::new(o, s), t))
            .map_err_value(|((t, o), s)| (t, Self::new(o, s)))
    }
}

// Implement methods for optical keyword type

impl Optical2_0 {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new_2_0(
        wavelength: Option<Wavelength>,
        bin: Option<PeakBin>,
        size: Option<PeakIndex>,
        filter: Filter,
        power: Option<Power>,
        detector_type: DetectorType,
        percent_emitted: Option<PercentEmitted>,
        detector_voltage: Option<DetectorVoltage>,
        longname: Longname,
        nonstandard_keywords: NonStdKeywords,
    ) -> Self {
        let common = CommonMeasurement::new(longname, nonstandard_keywords);
        let specific = InnerOptical2_0::new(wavelength, PeakData::new(bin, size));
        Self::new(
            common,
            filter,
            power,
            detector_type,
            percent_emitted,
            detector_voltage,
            specific,
        )
    }
}

impl Optical3_0 {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new_3_0(
        wavelength: Option<Wavelength>,
        bin: Option<PeakBin>,
        size: Option<PeakIndex>,
        filter: Filter,
        power: Option<Power>,
        detector_type: DetectorType,
        percent_emitted: Option<PercentEmitted>,
        detector_voltage: Option<DetectorVoltage>,
        longname: Longname,
        nonstandard_keywords: NonStdKeywords,
    ) -> Self {
        let common = CommonMeasurement::new(longname, nonstandard_keywords);
        let specific = InnerOptical3_0::new(wavelength, PeakData::new(bin, size));
        Self::new(
            common,
            filter,
            power,
            detector_type,
            percent_emitted,
            detector_voltage,
            specific,
        )
    }
}

impl Optical3_1 {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new_3_1(
        wavelengths: Wavelengths,
        calibration: Option<Calibration3_1>,
        display: Option<Display>,
        bin: Option<PeakBin>,
        size: Option<PeakIndex>,
        filter: Filter,
        power: Option<Power>,
        detector_type: DetectorType,
        percent_emitted: Option<PercentEmitted>,
        detector_voltage: Option<DetectorVoltage>,
        longname: Longname,
        nonstandard_keywords: NonStdKeywords,
    ) -> Self {
        let common = CommonMeasurement::new(longname, nonstandard_keywords);
        let specific =
            InnerOptical3_1::new(wavelengths, calibration, display, PeakData::new(bin, size));
        Self::new(
            common,
            filter,
            power,
            detector_type,
            percent_emitted,
            detector_voltage,
            specific,
        )
    }
}

impl Optical3_2 {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new_3_2(
        wavelengths: Wavelengths,
        calibration: Option<Calibration3_2>,
        display: Option<Display>,
        analyte: Analyte,
        feature: Option<Feature>,
        tag: Tag,
        measurement_type: OpticalType,
        detector_name: DetectorName,
        filter: Filter,
        power: Option<Power>,
        detector_type: DetectorType,
        percent_emitted: Option<PercentEmitted>,
        detector_voltage: Option<DetectorVoltage>,
        longname: Longname,
        nonstandard_keywords: NonStdKeywords,
    ) -> Self {
        let common = CommonMeasurement::new(longname, nonstandard_keywords);
        let specific = InnerOptical3_2::new(
            wavelengths,
            calibration,
            display,
            analyte,
            feature,
            measurement_type,
            tag,
            detector_name,
        );
        Self::new(
            common,
            filter,
            power,
            detector_type,
            percent_emitted,
            detector_voltage,
            specific,
        )
    }
}

impl<O> Optical<O> {
    pub fn awh_feature(&self) -> Option<OpticalFeature>
    where
        Self: AsRef<Option<Feature>>,
    {
        let x: &Option<Feature> = self.as_ref();
        if let Feature::Optical(i) = x.as_ref()? {
            Some(*i)
        } else {
            None
        }
    }

    pub fn set_awh_feature(&mut self, v: Option<OpticalFeature>)
    where
        Self: AsMut<Option<Feature>>,
    {
        *self.as_mut() = v.map(Feature::Optical);
    }

    pub(crate) fn lookup_optical<C>(
        std: &mut StdKeywords,
        mut nonstd: NonStdKeywords,
        i: MeasIndex,
        conf: &C,
    ) -> LookupOpticalResult<DiagnosedOptical<Self>>
    where
        O: LookupOptical,
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadStdKeywordsConfig>,
    {
        macro_rules! go {
            ($x:expr) => {
                $x.map_switchable_errors(LookupOpticalWarning::from)
                    .switchable_into_commutative()
                    .map_errors(LookupOpticalError::from)
                    .into_semigroup()
            };
        }
        let filter = Filter::remove_meas_opt_nofail(std, i);
        let power = Power::remove_or_drop_meas_opt(std, &mut nonstd, i, conf.as_ref());
        let det_type = DetectorType::remove_meas_opt_nofail(std, i);
        let perc_emit = PercentEmitted::remove_or_drop_meas_opt(std, &mut nonstd, i, conf.as_ref());
        let det_volt = DetectorVoltage::remove_or_drop_meas_opt(std, &mut nonstd, i, conf.as_ref());
        let specific = O::lookup_specific(std, &mut nonstd, i, conf);
        let common = CommonMeasurement::lookup(std, nonstd, i);
        go!(power)
            .zip4_commutative(go!(perc_emit), go!(det_volt), specific)
            .map_ok_value(|(p, e, v, s_out)| {
                let ret = Self::new(common, filter, p, det_type, e, v, s_out.this);
                DiagnosedOptical::new(ret, s_out.trimmed)
            })
    }

    pub(crate) fn opt_keywords(&self, i: MeasIndex) -> impl Iterator<Item = OptOpticalKeyword<'_>>
    where
        O: OpticalKeywords,
    {
        let x0 = OptOpticalKeyword::from_str(&self.common.longname, i);
        let x1 = OptOpticalKeyword::from_str(&self.filter, i);
        let x2 = OptOpticalKeyword::from_str(&self.detector_type, i);
        let x3 = self.power.map(|v| OptOpticalKeyword::from_value(v, i));
        let x4 = self
            .percent_emitted
            .map(|v| OptOpticalKeyword::from_value(v, i));
        let x5 = self
            .detector_voltage
            .map(|v| OptOpticalKeyword::from_value(v, i));
        [x0, x1, x2, x3, x4, x5]
            .into_iter()
            .flatten()
            .chain(self.specific.opt_keywords_inner(i))
    }

    fn try_convert<Of: ConvertFromOptical<O>>(
        self,
        i: MeasIndex,
        flag: AllowLoss,
    ) -> OpticalConvertResult<Optical<Of>>
    where
        O: OpticalKeywords,
    {
        Of::convert_from_optical(self.specific, i, flag).map_ok_value(|specific| {
            Optical::new(
                self.common,
                self.filter,
                self.power,
                self.detector_type,
                self.percent_emitted,
                self.detector_voltage,
                specific,
            )
        })
    }
}

// Implement methods for temporal keyword type

impl Temporal2_0 {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new_2_0(
        bin: Option<PeakBin>,
        size: Option<PeakIndex>,
        longname: Longname,
        nonstandard_keywords: NonStdKeywords,
    ) -> Self {
        let common = CommonMeasurement::new(longname, nonstandard_keywords);
        let specific = InnerTemporal2_0::new(PeakData::new(bin, size));
        Self::new(common, specific)
    }
}

impl Temporal3_0 {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new_3_0(
        timestep: Timestep,
        bin: Option<PeakBin>,
        size: Option<PeakIndex>,
        longname: Longname,
        nonstandard_keywords: NonStdKeywords,
    ) -> Self {
        let common = CommonMeasurement::new(longname, nonstandard_keywords);
        let specific = InnerTemporal3_0::new(timestep, PeakData::new(bin, size));
        Self::new(common, specific)
    }
}

impl Temporal3_1 {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new_3_1(
        timestep: Timestep,
        display: Option<Display>,
        bin: Option<PeakBin>,
        size: Option<PeakIndex>,
        longname: Longname,
        nonstandard_keywords: NonStdKeywords,
    ) -> Self {
        let common = CommonMeasurement::new(longname, nonstandard_keywords);
        let specific = InnerTemporal3_1::new(timestep, display, PeakData::new(bin, size));
        Self::new(common, specific)
    }
}

impl Temporal3_2 {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new_3_2(
        timestep: Timestep,
        display: Option<Display>,
        has_type: bool,
        longname: Longname,
        nonstandard_keywords: NonStdKeywords,
    ) -> Self {
        let common = CommonMeasurement::new(longname, nonstandard_keywords);
        let specific = InnerTemporal3_2::new(timestep, display, has_type);
        Self::new(common, specific)
    }
}

impl<T> Temporal<T> {
    pub(crate) fn lookup_temporal<C>(
        std: &mut StdKeywords,
        mut nonstd: NonStdKeywords,
        i: MeasIndex,
        conf: &C,
    ) -> LookupTemporalResult<DiagnosedTemporal<Self>>
    where
        T: LookupTemporal,
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadStdKeywordsConfig>,
    {
        T::lookup_specific(std, &mut nonstd, i, conf).map_ok_value(|specific| {
            let common = CommonMeasurement::lookup(std, nonstd, i);
            DiagnosedTemporal::new(
                Self::new(common, specific.this),
                specific.scale,
                specific.trimmed,
                specific.tmp_opt_pairs,
                specific.timestep_added,
            )
        })
    }

    pub(crate) fn req_meas_keywords(&self, i: MeasIndex) -> Option<ReqMeasKeyword<'_>>
    where
        T: TemporalKeywords,
    {
        self.specific.req_meas_keywords_inner(i)
    }

    pub(crate) fn opt_and_nonstd_keywords(
        &self,
        i: MeasIndex,
    ) -> impl Iterator<Item = StdOrNonStdOptMeasKeyword<'_>>
    where
        T: TemporalKeywords,
    {
        let cs = self
            .common
            .nonstandard_keywords
            .iter()
            .map(|(k, v)| NonStdKeyword::new(k, v.as_ne_str()))
            .map(StdOrNonStdOptMeasKeyword::from);
        self.opt_meas_keywords(i)
            .map(OptMeasKeyword::from)
            .map(StdOrNonStdOptMeasKeyword::from)
            .chain(cs)
    }

    fn opt_meas_keywords(&self, i: MeasIndex) -> impl Iterator<Item = OptTemporalKeyword<'_>>
    where
        T: TemporalKeywords,
    {
        OptTemporalKeyword::from_str(&self.common.longname, i)
            .into_iter()
            .chain(self.specific.opt_meas_keywords_inner(i))
    }

    fn try_convert<ToT>(self, i: MeasIndex, flag: AllowLoss) -> TemporalConvertResult<Temporal<ToT>>
    where
        ToT: ConvertFromTemporal<T>,
        T: TemporalKeywords,
    {
        ToT::convert_from_temporal(self.specific, i, flag)
            .map_deferred_value(|specific| Temporal::new(self.common, specific))
    }
}

// Implement methods for core*

impl<L, T, O, X, N, V> CoreMeasurements<L, T, O, X, N, V> {
    /// Get read-only reference to measurements
    pub(crate) fn measurements(&self) -> &NamedVec<N, Temporal<T>, ScaledOptical<X, O>> {
        &self.meta
    }

    /// Get read-only reference to layout
    pub(crate) fn layout(&self) -> &L {
        &self.data
    }

    pub(crate) fn scales(&self) -> impl Iterator<Item = X>
    where
        X: Default + Copy,
    {
        // TODO not DRY, used in lots of scale checks for the named vec
        self.meta
            .iter()
            .map(|m| m.both(|_| X::default(), |o| o.value.scale))
    }

    // TODO lots of allocations, there should be a better way to do this
    fn add_scales(
        &self,
        measurements: TemporalsAndOpticals<T, O>,
    ) -> TemporalsAndScaledOpticals<T, X, O>
    where
        X: Default + Copy,
    {
        measurements
            .into_iter()
            // TODO this will truncate the measurements to the length of
            // scales, which will result in now throwing an error in the caller
            // if the length is later checked
            .zip(self.scales())
            .map(|(m, s)| m.bimap_once(|t| t, |o| ScaledOptical::new(o, s)))
            .collect()
    }

    pub fn set_scales(&mut self, scales: Vec<X>) -> ErrorsResult<(), (), SetScalesError>
    where
        X: Copy + CheckedScaleTransform,
        L: LayoutDatatype,
    {
        let center_scale_not_linear = || {
            self.meta
                .center_index()
                .map(|i| {
                    assert_eq_len!(scales.len(), self.meta.len(), "scales", "measurements");
                    scales[usize::from(i)]
                })
                .is_some_and(|s| !s.is_identity())
                .then_some(NonIdentityTemporalScaleError.into())
        };

        let l = &self.data;
        l.check_transforms_and_len(scales.iter().copied())
            .map_err(SetScalesError::from)
            .into_nowarn()
            .eval_deferred_error(|()| center_scale_not_linear())
            .when_ok(|| {
                assert_eq_len!(scales.len(), self.meta.len(), "scales", "measurements");
                self.meta
                    .alter_values_zip(scales, |_, _| (), |m, x| m.value.scale = x)
                    .unwrap();
            })
    }
}

impl<L, V> VersionedCoreLayout<L, V>
where
    V: VersionMeasSet,
{
    /// Set shortnames regardless of wrapper key type.
    pub(crate) fn set_all_shortnames(
        &mut self,
        ns: Vec<Shortname>,
    ) -> Result<NameMapping, SetNamesError> {
        self.meta.set_names(ns)
    }

    /// Set shortnames when wrapper key type is Option
    pub(crate) fn set_measurement_shortnames_maybe(
        &mut self,
        ns: Vec<Option<Shortname>>,
    ) -> Result<NameMapping, SetKeysError>
    where
        V: VersionMeasSet<Name = Option<Shortname>>,
    {
        self.meta.set_keys(ns)
    }

    #[allow(clippy::type_complexity)]
    pub fn try_convert<Vf, Lf>(
        self,
        allow_loss: AllowLoss,
    ) -> WarningsAndErrorsResult<
        VersionedCoreLayout<Lf, Vf>,
        (),
        MeasConvertWarning,
        MeasConvertError,
    >
    where
        Vf: VersionMeasSet,
        Vf::Optical: ConvertFromOptical<V::Optical>,
        Vf::Temporal: ConvertFromTemporal<V::Temporal>,
        Vf::OpticalScale: ConvertFromScale<V::OpticalScale>,
        Vf::Name: MightHave<Shortname> + Clone + ConvertFromShortname<V::Name>,
        // TODO technically normalize shouldn't be needed here but it won't hurt anything
        Lf: ConvertFromLayout<L> + LayoutNormalize,
    {
        let meas_res = self
            .meta
            .map_center_value(|v| {
                v.value
                    .try_convert(v.index, allow_loss)
                    .switchable_into_commutative()
            })
            .set_err_value(())
            .map_errors(MeasConvertError::Temporal)
            .map_commutative_warnings(MeasConvertWarning::from)
            .and_then_commutative(|meas| {
                meas.map_non_center_values(|i, v| v.try_convert(i, allow_loss))
                    .map_errors(MeasConvertError::Optical)
                    .map_commutative_warnings(MeasConvertWarning::from)
            })
            .and_then_commutative(|meas| {
                meas.try_rewrapped(|i, n| Vf::Name::convert_from_shortname(n, i))
                    .map_errors(MeasConvertError::Rewrap)
                    .nowarn_into_warn()
            });
        let layout_res = ConvertFromLayout::convert_from_layout(self.data)
            .map_errors(MeasConvertError::Layout)
            .nowarn_into_warn();
        meas_res
            .zip_commutative(layout_res)
            .map_ok_value(|(measurements, layout)| CoreMeasurements::new(measurements, layout))
    }

    pub(crate) fn set_temporal(
        &mut self,
        n: &Shortname,
        timestep: <V::Temporal as TemporalFromOptical<V::Optical>>::TData,
        allow_loss: AllowLoss,
    ) -> WarningOrErrorResult<bool, (), SetTemporalError, SetTemporalByNameError>
    where
        V::Temporal: TemporalFromOptical<V::Optical>,
        V::Optical: SwapOpticalWithTemporal<V::Temporal>,
    {
        self.meta.set_center_by_name(
            n,
            |old, new| {
                ScaledOptical::swap_optical_temporal(old, new, allow_loss)
                    .map_switchable_errors(SetTemporalError::from)
                    .switchable_into_non_commutative()
                    .map_errors(SetTemporalByNameError::from)
            },
            |i, old_o| {
                V::Temporal::from_optical(old_o, i, timestep, allow_loss)
                    .map_switchable_errors(SetTemporalError::from)
                    .switchable_into_non_commutative()
                    .map_errors(SetTemporalByNameError::from)
            },
        )
    }

    pub(crate) fn set_temporal_at(
        &mut self,
        index: MeasIndex,
        timestep: <V::Temporal as TemporalFromOptical<V::Optical>>::TData,
        allow_loss: AllowLoss,
    ) -> WarningOrErrorResult<bool, (), SetTemporalError, SetTemporalByIndexError>
    where
        V::Temporal: TemporalFromOptical<V::Optical>,
        V::Optical: SwapOpticalWithTemporal<V::Temporal>,
    {
        self.meta.set_center_by_index(
            index,
            |old, new| {
                ScaledOptical::swap_optical_temporal(old, new, allow_loss)
                    .map_switchable_errors(SetTemporalError::from)
                    .switchable_into_non_commutative()
                    .map_errors(SetTemporalByIndexError::from)
            },
            |i, old_o| {
                V::Temporal::from_optical(old_o, i, timestep, allow_loss)
                    .map_switchable_errors(SetTemporalError::from)
                    .switchable_into_non_commutative()
                    .map_errors(SetTemporalByIndexError::from)
            },
        )
    }

    /// Unset temporal measurement.
    pub(crate) fn unset_temporal<F, X, LWC, RWC, E, EC>(
        &mut self,
        to_opt: F,
    ) -> LogResult<Option<X>, (), LWC, RWC, (), E, EC>
    where
        F: FnOnce(
            MeasIndex,
            Temporal<V::Temporal>,
        ) -> LogResult<
            (Optical<V::Optical>, X),
            Temporal<V::Temporal>,
            LWC,
            RWC,
            (),
            E,
            EC,
        >,
        LWC: Default,
    {
        let go = |i, t| {
            to_opt(i, t)
                .map_ok_value(|(o, s)| (ScaledOptical::new(o, V::OpticalScale::default()), s))
        };
        self.meta.unset_center(go)
    }

    pub(crate) fn rename(
        &mut self,
        index: MeasIndex,
        key: V::Name,
    ) -> Result<(Shortname, Shortname), RenameError> {
        self.meta.rename(index, key)
    }

    pub(crate) fn rename_temporal(&mut self, name: Shortname) -> Option<Shortname> {
        self.meta.rename_center(name)
    }

    pub fn as_temporal_mut(&mut self) -> Option<IndexedElement<&mut Shortname, &mut VTemporal<V>>> {
        self.meta.as_center_mut()
    }

    pub(crate) fn alter_values<F, G, R>(&mut self, with_tmp: F, with_opt: G) -> Vec<R>
    where
        F: Fn(IndexedElement<&Shortname, &mut VTemporal<V>>) -> R,
        G: Fn(IndexedElement<&V::Name, &mut VOptical<V>>) -> R,
    {
        let with_scaled_opt = |e: IndexedElement<&_, &mut ScaledOptical<_, _>>| {
            with_opt(IndexedElement::new(e.index, e.key, &mut e.value.inner))
        };
        self.meta.alter_values(with_tmp, with_scaled_opt)
    }

    pub(crate) fn alter_values_zip<G, F, X, R>(
        &mut self,
        xs: Vec<X>,
        with_tmp: F,
        with_opt: G,
    ) -> Result<Vec<R>, InputLengthError>
    where
        F: Fn(IndexedElement<&Shortname, &mut VTemporal<V>>, X) -> R,
        G: Fn(IndexedElement<&V::Name, &mut VOptical<V>>, X) -> R,
    {
        let with_scaled_opt = |e: IndexedElement<&_, &mut ScaledOptical<_, _>>, x| {
            with_opt(IndexedElement::new(e.index, e.key, &mut e.value.inner), x)
        };
        self.meta.alter_values_zip(xs, with_tmp, with_scaled_opt)
    }

    pub(crate) fn alter_elements_zip<Fo, Ft, Fe, X, Y, R, E, G>(
        &mut self,
        xs: Vec<Element<X, Y>>,
        g: G,
        with_opt: Fo,
        with_tmp: Ft,
        with_err: Fe,
    ) -> Result<Vec<R>, SetElementsError<ErrorGroup<E, G>>>
    where
        Ft: Fn(IndexedElement<&Shortname, &mut VTemporal<V>>, X) -> R,
        Fo: Fn(IndexedElement<&V::Name, &mut VOptical<V>>, Y) -> R,
        Fe: Fn(MeasIndex, bool) -> E,
    {
        let with_scaled_opt = |e: IndexedElement<&_, &mut ScaledOptical<_, _>>, x| {
            with_opt(IndexedElement::new(e.index, e.key, &mut e.value.inner), x)
        };
        self.meta
            .alter_elements_zip(xs, g, with_scaled_opt, with_tmp, with_err)
    }

    pub(crate) fn alter_common_values_zip<F, X, R, T>(
        &mut self,
        xs: impl IntoIterator<Item = X>,
        f: F,
    ) -> Result<Vec<R>, InputLengthError>
    where
        F: Fn(MeasIndex, &mut T, X) -> R,
        VTemporal<V>: AsMut<T>,
        VScaledOptical<V>: AsMut<T>,
    {
        self.meta.alter_common_values_zip(xs, f)
    }

    pub(crate) fn replace_at(
        &mut self,
        index: MeasIndex,
        value: Optical<V::Optical>,
    ) -> Result<VElementWithScale<V>, ElementIndexError> {
        let ret = self
            .meta
            .replace_at(index, ScaledOptical::new_identity(value))?;
        Ok(ret.bimap_once(|t| t, |o| (o.inner, o.scale)))
    }

    pub(crate) fn replace_named(
        &mut self,
        name: &Shortname,
        value: VOptical<V>,
    ) -> Result<VElementWithScale<V>, NameNotFoundError> {
        let ret = self
            .meta
            .replace_named(name, ScaledOptical::new_identity(value))?;
        Ok(ret.bimap_once(|t| t, |o| (o.inner, o.scale)))
    }

    pub(crate) fn replace_temporal_at_nofail<F>(
        &mut self,
        index: MeasIndex,
        value: VTemporal<V>,
        to_opt: F,
    ) -> Result<VElementWithScale<V>, SetCenterError>
    where
        F: FnOnce(MeasIndex, VTemporal<V>) -> VOptical<V>,
    {
        let to_scaled_opt = |i, t| ScaledOptical::new_identity(to_opt(i, t));
        let ret = self
            .meta
            .replace_center_at_nofail(index, value, to_scaled_opt)?;
        Ok(ret.bimap_once(|t| t, |o| (o.inner, o.scale)))
    }

    pub(crate) fn replace_temporal_at<F, LWC, RWC, E, EC>(
        &mut self,
        index: MeasIndex,
        value: VTemporal<V>,
        to_opt: F,
    ) -> LogResult<VElementWithScale<V>, (), LWC, RWC, (), E, EC>
    where
        F: FnOnce(
            MeasIndex,
            VTemporal<V>,
        ) -> LogResult<VOptical<V>, VTemporal<V>, LWC, RWC, (), E, EC>,
        E: From<SetCenterError>,
        LWC: Default,
        RWC: Default,
        EC: Default,
    {
        let to_scaled_opt = |i, t| to_opt(i, t).map_ok_value(ScaledOptical::new_identity);
        self.meta
            .replace_center_at(index, value, to_scaled_opt)
            .map_ok_value(|ret| ret.bimap_once(|t| t, |o| (o.inner, o.scale)))
    }

    pub(crate) fn replace_temporal_by_name_nofail<F>(
        &mut self,
        n: &Shortname,
        value: VTemporal<V>,
        to_opt: F,
    ) -> Result<VElementWithScale<V>, NameNotFoundError>
    where
        F: FnOnce(MeasIndex, VTemporal<V>) -> VOptical<V>,
    {
        let to_scaled_opt = |i, t| ScaledOptical::new_identity(to_opt(i, t));
        let ret = self
            .meta
            .replace_center_by_name_nofail(n, value, to_scaled_opt)?;
        Ok(ret.bimap_once(|t| t, |o| (o.inner, o.scale)))
    }

    pub(crate) fn replace_temporal_by_name<F, LWC, RWC, E, EC>(
        &mut self,
        n: &Shortname,
        value: VTemporal<V>,
        to_opt: F,
    ) -> LogResult<VElementWithScale<V>, (), LWC, RWC, (), E, EC>
    where
        F: FnOnce(
            MeasIndex,
            VTemporal<V>,
        ) -> LogResult<VOptical<V>, VTemporal<V>, LWC, RWC, (), E, EC>,
        E: From<NameNotFoundError>,
        EC: Default,
        LWC: Default,
        RWC: Default,
    {
        let to_scaled_opt = |i, t| to_opt(i, t).map_ok_value(ScaledOptical::new_identity);
        self.meta
            .replace_center_by_name(n, value, to_scaled_opt)
            .map_ok_value(|ret| ret.bimap_once(|t| t, |o| (o.inner, o.scale)))
    }

    pub(crate) fn push_temporal_inner<C>(
        &mut self,
        name: Shortname,
        temporal: Temporal<V::Temporal>,
        data_column: C,
    ) -> ErrorsResult<(), (), PushTemporalError<L::Error>>
    where
        L: LayoutInsert<C>,
    {
        self.meta
            .check_push_center(&name)
            .map_errors(PushTemporalError::Center)
            .nowarn_and_then(|()| {
                self.data
                    .push(data_column)
                    .map_err(PushTemporalError::Layout)
                    .into_log()
            })
            .when_ok(|| self.meta.push_center_nocheck(name, temporal))
    }

    pub(crate) fn insert_temporal_inner<C>(
        &mut self,
        i: MeasIndex,
        name: Shortname,
        temporal: Temporal<V::Temporal>,
        data_column: C,
    ) -> ErrorsResult<(), (), InsertTemporalError<L::Error>>
    where
        L: LayoutInsert<C>,
    {
        self.meta
            .check_insert_center(i, &name)
            .map_errors(InsertTemporalError::Center)
            .nowarn_and_then(|()| {
                self.data
                    .insert_nocheck(i, data_column)
                    .map_err(InsertTemporalError::Layout)
                    .into_log()
            })
            .when_ok(|| self.meta.insert_center_nocheck(i, name, temporal))
    }

    pub(crate) fn push_optical_inner<C>(
        &mut self,
        name: V::Name,
        optical: Optical<V::Optical>,
        scale: V::OpticalScale,
        data_column: C,
    ) -> ErrorsResult<Shortname, (), PushOpticalError<L::Error>>
    where
        L: LayoutInsert<C> + LayoutInsertScaleCheck<C>,
    {
        let scale_res = self
            .data
            .matches_scale(&data_column, &scale)
            .map_err(PushOpticalError::Scale);
        let push_res = self
            .meta
            .check_push(&name)
            .map(Cow::into_owned)
            .map_err(PushOpticalError::Unique);
        scale_res
            .zip(push_res)
            .nowarn_and_then(|((), ret)| {
                self.data
                    .push(data_column)
                    .map_err(PushOpticalError::Layout)
                    .into_log()
                    .set_ok_value(ret)
            })
            .map_ok_value(|ret| {
                let s = ScaledOptical::new(optical, scale);
                self.meta.push_nocheck(name, s);
                ret
            })
    }

    pub(crate) fn insert_optical_inner<C>(
        &mut self,
        i: MeasIndex,
        name: V::Name,
        optical: Optical<V::Optical>,
        scale: V::OpticalScale,
        data_column: C,
    ) -> ErrorsResult<Shortname, (), InsertOpticalError<L::Error>>
    where
        L: LayoutInsert<C> + LayoutInsertScaleCheck<C>,
    {
        let scale_res = self
            .data
            .matches_scale(&data_column, &scale)
            .map_err(InsertOpticalError::Scale)
            .into_nowarn();
        let insert_res = self
            .meta
            .check_insert(i, &name)
            .map_ok_value(Cow::into_owned)
            .map_errors(InsertOpticalError::Insert);
        scale_res
            .zip_commutative(insert_res)
            .nowarn_and_then(|((), ret)| {
                self.data
                    .insert_nocheck(i, data_column)
                    .map_err(InsertOpticalError::Layout)
                    .into_log()
                    .set_ok_value(ret)
            })
            .map_ok_value(|ret| {
                let s = ScaledOptical::new(optical, scale);
                self.meta.insert_nocheck(i, name, s);
                ret
            })
    }

    pub(crate) fn remove_measurement_by_name<C>(
        &mut self,
        name: &Shortname,
    ) -> Result<(MeasIndex, VTemporalOrOpticalWithScale<V>, C), NameNotFoundError>
    where
        L: LayoutRemove<C>,
    {
        let (i, e) = self.meta.remove_name(name)?;
        let r = self.data.remove_nocheck(i);
        let m = e.bimap_once(|t| t, |o| (o.inner, o.scale));
        Ok((i, m, r))
    }

    pub(crate) fn remove_measurement_by_index<C>(
        &mut self,
        index: MeasIndex,
    ) -> Result<(VNamedTemporalOrOpticalWithScale<V>, C), ElementIndexError>
    where
        L: LayoutRemove<C>,
    {
        let p = self.meta.remove_index(index)?;
        let r = self.data.remove_nocheck(index);
        let m = p.bimap_once(|t| t, |o| o.second_once(|v| (v.inner, v.scale)));
        Ok((m, r))
    }

    pub(crate) fn set_unnamed_measurements(
        &mut self,
        measurements: VTemporalsAndOpticals<V>,
    ) -> Result<(), SetUnnamedMeasurementsError>
    where
        L: LayoutWidth + LayoutDatatype,
    {
        // This will ensure the new measurements have the same length as the old
        // and that the temporal/optical types are in the same spot
        self.meta.set_values(self.add_scales(measurements))?;
        self.validate("measurements should match length and scale/datatype of data");
        Ok(())
    }

    pub(crate) fn set_named_measurements_with<F, E, Ei>(
        &mut self,
        measurements: VNamedTemporalsAndOpticalsWithScale<V>,
        f: F,
    ) -> Result<(), E>
    where
        L: LayoutDatatype + LayoutWidth,
        F: FnOnce(&VersionedMeasurements<V>, &VersionedMeasurements<V>) -> Result<(), Ei>,
        E: From<Ei> + From<MeasurementsWithLayoutError>,
    {
        // Ensure new measurement length and scales match existing schema.
        let meas = self.data.try_new_measmeta::<V>(measurements)?;
        // Check other stuff before committing (links, etc)
        f(&self.meta, &meas)?;
        self.meta = meas;
        self.validate("measurements should match length and scale/datatype of data");
        Ok(())
    }

    pub(crate) fn set_named_measurements_and_layout_with<F, E, Ei>(
        &mut self,
        measurements: VNamedTemporalsAndOpticalsWithScale<V>,
        layout: L,
        f: F,
    ) -> Result<(), E>
    where
        L: LayoutDatatype + LayoutWidth + LayoutNormalize,
        F: FnOnce(&VersionedMeasurements<V>, &VersionedMeasurements<V>) -> Result<(), Ei>,
        E: From<Ei> + From<MeasurementsWithLayoutError>,
    {
        // Ensure new layout and measurements have matching length and scales.
        // No need to check these with existing data since these two inputs will
        // totally override everything.
        let meas = layout.try_new_measmeta::<V>(measurements)?;
        // Check other stuff before committing (links, etc)
        f(&self.meta, &meas)?;
        self.meta = meas;
        self.set_layout_inner(layout);
        self.validate("inputs should have matching length and scale/datatype");
        Ok(())
    }

    pub(crate) fn set_unnamed_measurements_and_layout(
        &mut self,
        measurements: VTemporalsAndOpticals<V>,
        layout: L,
    ) -> Result<(), SetUnnamedMeasurementsAndDataSchemaError>
    where
        L: LayoutWidth + LayoutDatatype + LayoutNormalize,
    {
        // TODO check length match b/t meas and layout
        // // ensure new layout and measurements have matching length and scales
        // layout.check_unmamed_meas_xforms_and_len::<V>(&measurements[..])?;
        // this will check that new measurements have same length as old
        self.meta.set_values(self.add_scales(measurements))?;
        self.set_layout_inner(layout);
        self.validate("inputs should have matching length and scale/datatype");
        Ok(())
    }

    pub(crate) fn clear(&mut self)
    where
        L: LayoutWidth,
    {
        self.meta = NamedVec::default();
        self.data.clear();
    }

    fn set_layout_inner(&mut self, mut layout: L)
    where
        L: LayoutNormalize,
    {
        layout.normalize();
        self.data = layout;
    }

    fn validate(&self, msg: &'static str)
    where
        L: LayoutDatatype,
    {
        assert!(
            self.data.check_measmeta_scales_and_len(&self.meta).is_ok(),
            "{msg}",
        );
    }
}

impl<V> VersionedCoreLayout<<V as VersionMeasSet>::DataSchema, V>
where
    V: VersionMeasSet,
{
    // only meant to be called during lookup when keywords are being read from
    // a hashtable
    pub(crate) fn try_new(
        measurements: VNamedTemporalsAndScaledOpticals<V>,
        data_schema: V::DataSchema,
        conf: &ReadStdKeywordsConfig,
    ) -> WarningsAndErrorsResult<Self, (), MissingTimeError, LookupMeasError>
    where
        V::DataSchema: LayoutWidth,
    {
        // this should be true since both depend on $PAR
        assert_eq_len!(
            measurements.len(),
            data_schema.width(),
            "measurements",
            "data schema"
        );
        let go = |ms: &NamedVec<_, _, _>| {
            if let Some(pat) = conf.time_meas_pattern.0.as_ref()
                && ms.as_center().is_none()
                && !ms.is_empty()
            {
                return Some(MissingTimeError(pat.clone()));
            }
            None
        };
        let missing_flag = conf.allow_missing_time;
        MeasMeta::try_new(measurements)
            .map_err(LookupMeasError::from)
            .into_log()
            .eval_warning_or_error3(missing_flag, |_| (), |()| (), go)
            .and_then_commutative(|meas| {
                // Check that new metadata and schema have compatible scales and
                // datatypes. Length is assumed to be fine (see assert above)
                data_schema
                    .check_measmeta_scales(&meas)
                    .map_err(LookupMeasError::from)
                    .into_log()
                    .map_ok_value(|()| {
                        let ret = Self::new(meas, data_schema);
                        ret.validate("inputs should have matching length and scale/datatype");
                        ret
                    })
            })
    }

    pub(crate) fn try_new_nodrop(
        measurements: VNamedTemporalsAndOpticalsWithScale<V>,
        data_schema: V::DataSchema,
    ) -> ErrorsResult<Self, (), NewMeasError>
    where
        V::DataSchema: LayoutWidth,
    {
        MeasMeta::try_new(wrap_scaled_opticals::<V>(measurements))
            .map_err(NewMeasError::from)
            .into_nowarn()
            .and_then_commutative(|meas| {
                // Check that schema and measurements have compatible length
                // and scale/datatypes.
                data_schema
                    .check_measmeta_scales_and_len(&meas)
                    .map_err(NewMeasError::from)
                    .into_log()
                    .map_ok_value(|()| {
                        let ret = Self::new(meas, data_schema);
                        ret.validate("inputs should have matching length and scale/datatype");
                        ret
                    })
            })
    }

    pub(crate) fn set_data_schema(
        &mut self,
        data_schema: V::DataSchema,
    ) -> Result<(), MeasLayoutMismatchError> {
        // Ensure that new schema has same length and compatible datatypes
        // compared to existing measurements.
        data_schema.check_measmeta_scales_and_len(&self.meta)?;
        self.set_layout_inner(data_schema);
        self.validate("inputs should have matching length and scale/datatype");
        Ok(())
    }

    pub(crate) fn with_data(
        self,
        df: PrimitiveDataFrame,
    ) -> Result<VersionedCoreLayout<<V as VersionMeasSet>::DataFrame, V>, DataSchemaToDataFrameError>
    where
        V::DataSchema: WithPrimitiveDataFrame<DfTarget = V::DataFrame> + LayoutWidth,
    {
        // Check that width of new dataframe matches current schema. Do not
        // check datatypes/scale since this is metadata-independent.
        let typed_df = self.data.with_data(df)?;
        assert!(
            typed_df.width() == self.meta.len(),
            "new df columns should match meas length"
        );
        Ok(CoreMeasurements::new(self.meta, typed_df))
    }

    pub(crate) fn h_read_df<R>(
        mut self,
        h: &mut BufReader<R>,
        tot: <V::DataSchema as VersionedDataSchema>::Tot,
        seg: &mut AnyDataSegment,
        conf: &ReadEventsConfig,
    ) -> WarningsAndIOGroupResult<
        ReadDataFrameResult<VersionedCoreLayout<<V as VersionMeasSet>::DataFrame, V>>,
        ReadCheckedDataframeWarning,
        ReadCheckedDataframeError,
        (),
    >
    where
        R: Read + Seek,
        V::DataSchema: VersionedDataSchema + DataSchemaToEmptyDataFrame<DfTarget = V::DataFrame>,
    {
        self.data
            .h_read_df(h, tot, seg, conf)
            .map_ok_value(|df_out| {
                // New dataframe should have same metadata and column number
                // compared to old schema, so calling new with no checks should
                // be valid.
                let new = CoreMeasurements::new(self.meta, df_out.inner);
                ReadDataFrameResult::new(new, df_out.diagnostics)
            })
    }
}

impl<V> VersionedCoreLayout<<V as VersionMeasSet>::DataFrame, V>
where
    V: VersionMeasSet,
{
    pub(crate) fn without_data(self) -> VersionedCoreLayout<<V as VersionMeasSet>::DataSchema, V>
    where
        V::DataFrame: DataFrameAsDataSchema<DataSchema = V::DataSchema>,
    {
        // This simply removes the data, metadata should still be in sync
        CoreMeasurements::new(self.meta, self.data.as_data_schema())
    }

    pub(crate) fn check_ranges(
        &mut self,
        check_range_datatypes: CheckedRangeDatatypes,
        over_range_action: OverRangeAction,
    ) -> WarningsAndErrorsResult<Vec<OverrangeColumn>, (), EventOverRangeError, EventOverRangeError>
    {
        // This mutates range values which do not have to be kept in sync, so
        // no consistency checks are required.
        self.data
            .check_ranges_mut(check_range_datatypes, over_range_action)
    }

    pub(crate) fn set_dataframe_schema(
        &mut self,
        data_schema: &V::DataSchema,
    ) -> Result<(), DatasetSetDataSchemaError>
    where
        V::DataFrame: Clone + Into<PrimitiveDataFrame> + Default,
        V::DataSchema: WithPrimitiveDataFrame<DfTarget = V::DataFrame>,
    {
        // Ensure new data schema has matching length and datatypes compared
        // to existing measurements.
        data_schema.check_measmeta_scales_and_len(&self.meta)?;
        // Check for any data loss that may happen within the dataframe itself.
        data_schema.check_data_loss_generic(&self.data)?;
        self.set_data_schema_unchecked(data_schema);
        self.validate("inputs should have matching length and scale/datatype");
        Ok(())
    }

    pub(crate) fn set_unnamed_measurements_dataframe_schema(
        &mut self,
        measurements: VTemporalsAndOpticals<V>,
        data_schema: &V::DataSchema,
    ) -> Result<(), DatasetSetUnnamedMeasAndDataSchemaError>
    where
        V::DataFrame: Clone + Into<PrimitiveDataFrame> + Default,
        V::DataSchema: WithPrimitiveDataFrame<DfTarget = V::DataFrame>,
    {
        // TODO check length b/t meas and schema
        // // Check that new measurements and new data schema are in sync.
        // data_schema
        //     .check_unmamed_meas_xforms_and_len::<V>(&measurements[..])
        //     .map_err(SetUnnamedMeasurementsAndDataSchemaError::from)?;
        // This checks for data loss due to type conversions in the dataframe.
        data_schema.check_data_loss_generic(&self.data)?;
        // This additionally checks that the new measurements are the same
        // length as existing, so that the new meas+schema will remain
        // consistent with the dataframe.
        self.meta
            .set_values(self.add_scales(measurements))
            .map_err(SetUnnamedMeasurementsAndDataSchemaError::from)?;
        self.set_data_schema_unchecked(data_schema);
        self.validate("inputs should have matching length and scale/datatype");
        Ok(())
    }

    pub(crate) fn set_named_measurements_and_dataframe_schema_with<F, E, Ei>(
        &mut self,
        measurements: VNamedTemporalsAndOpticalsWithScale<V>,
        data_schema: &V::DataSchema,
        f: F,
    ) -> Result<(), E>
    where
        V::DataFrame: Clone + Into<PrimitiveDataFrame> + Default,
        V::DataSchema: WithPrimitiveDataFrame<DfTarget = V::DataFrame>,
        F: FnOnce(&VersionedMeasurements<V>, &VersionedMeasurements<V>) -> Result<(), Ei>,
        E: From<Ei>
            + From<MeasurementsWithLayoutError>
            + From<DatasetSetDataSchemaError>
            + From<DataSchemaToDataFrameError>,
    {
        // Length b/t new meas and new schema is checked here
        let meas = data_schema.try_new_measmeta::<V>(measurements)?;
        // Length b/t new schema and existing schema is checked here
        data_schema.check_data_loss_and_width_generic(&self.data)?;
        // Anything else (links, etc) are checked here
        f(&self.meta, &meas)?;
        self.set_data_schema_unchecked(data_schema);
        self.meta = meas;
        self.validate("inputs should have matching length and scale/datatype");
        Ok(())
    }

    pub(crate) fn set_named_measurements_and_data_with<F, E, Ei>(
        &mut self,
        measurements: VNamedTemporalsAndOpticalsWithScale<V>,
        df: PrimitiveDataFrame,
        f: F,
    ) -> Result<(), E>
    where
        V::DataFrame: WithPrimitiveDataFrame<DfTarget = V::DataFrame>,
        F: FnOnce(&VersionedMeasurements<V>, &VersionedMeasurements<V>) -> Result<(), Ei>,
        E: From<Ei> + From<MeasurementsWithLayoutError> + From<DataSchemaToDataFrameError>,
    {
        // This checks that the length of the new dataframe is same as the old
        let new_df = self.data.with_data(df)?;
        // This checks that the new measurements are the same length as existing
        // and that the scales match
        self.set_named_measurements_with::<_, E, Ei>(measurements, f)?;
        self.data = new_df;
        self.validate("inputs should have matching length and scale/datatype");
        Ok(())
    }

    pub(crate) fn set_data(
        &mut self,
        df: PrimitiveDataFrame,
    ) -> Result<(), DataSchemaToDataFrameError>
    where
        V::DataFrame: WithPrimitiveDataFrame<DfTarget = V::DataFrame>,
    {
        // This checks that old and new column number are the same
        self.data = self.data.with_data(df)?;
        assert!(
            self.meta.len() == self.data.width(),
            "metadata and data should have same number of colunms"
        );
        Ok(())
    }

    pub(crate) fn set_unnamed_measurements_and_data(
        &mut self,
        measurements: VTemporalsAndOpticals<V>,
        df: PrimitiveDataFrame,
    ) -> Result<(), SetUnnamdMeasurementsAndDataError>
    where
        V::DataFrame: WithPrimitiveDataFrame<DfTarget = V::DataFrame>,
    {
        // This ensures new dataframe has same width as existing schema.
        let new_df = self.data.with_data(df)?;
        // This ensures new measurements have matching length and scale datatype
        // compared to existing schema.
        self.set_unnamed_measurements(measurements)?;
        self.data = new_df;
        Ok(())
    }

    fn set_data_schema_unchecked(&mut self, data_schema: &V::DataSchema)
    where
        V::DataFrame: Into<PrimitiveDataFrame> + Default,
        V::DataSchema: WithPrimitiveDataFrame<DfTarget = V::DataFrame>,
    {
        let new_data_schema = data_schema
            .with_data_generic(mem::take(&mut self.data))
            .expect("data loss and dimensions were checked by caller");
        self.set_layout_inner(new_data_schema);
    }
}

// Implement conversions between scale and scale transforms

impl From<Scale> for OpticalScale3_0 {
    fn from(value: Scale) -> Self {
        match value {
            Scale::Linear => Self::Lin(PositiveFloat::one()),
            Scale::Log(x) => Self::Log(x),
        }
    }
}

impl From<OpticalScale3_0> for (Scale, Option<Gain>) {
    fn from(value: OpticalScale3_0) -> Self {
        match value {
            OpticalScale3_0::Lin(g) => (Scale::Linear, Some(Gain(g))),
            OpticalScale3_0::Log(l) => (Scale::Log(l), None),
        }
    }
}

impl TryFrom<(Scale, Option<Gain>)> for OpticalScale3_0 {
    type Error = NewOpticalScaleError;

    /// Convert values for $PnE and $PnG to a scale transform (3.0+)
    ///
    /// If scale is linear, return a linear transform with slope equal to $PnG
    /// or 1.0 if $PnG not given.
    ///
    /// If scale is log, return a log transform with the parameters in $PnE.
    /// Return error if $PnG is given and not 1.0.
    fn try_from(value: (Scale, Option<Gain>)) -> Result<Self, Self::Error> {
        let (scale, gain) = value;
        match scale {
            Scale::Linear => Ok(Self::Lin(gain.map_or(PositiveFloat::one(), |g| g.0))),
            Scale::Log(l) => {
                if let Some(g) = gain
                    && !g.0.is_one()
                {
                    return Err(NewOpticalScaleError { scale, gain: g });
                }
                Ok(Self::Log(l))
            }
        }
    }
}

// Implement methods on misc data structures

impl PeakData {
    fn lookup(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        i: MeasIndex,
        conf: &ReadDataKeywordsConfig,
    ) -> DeferredWarningsAndErrors<Self, LookupPeakError, LookupPeakError> {
        let b = PeakBin::remove_or_drop_meas_opt(std, nonstd, i, conf)
            .map_switchable_errors(LookupPeakError::from)
            .switchable_into_commutative()
            .into_semigroup();
        let s = PeakIndex::remove_or_drop_meas_opt(std, nonstd, i, conf)
            .map_switchable_errors(LookupPeakError::from)
            .switchable_into_commutative()
            .into_semigroup();
        b.lift_f2_once(s, Self::new)
    }

    pub(crate) fn opt_keywords(&self, i: MeasIndex) -> impl Iterator<Item = OptPeakKeyword> {
        let x = self.bin.map(|v| OptPeakKeyword::from_value(v, i));
        let y = self.size.map(|v| OptPeakKeyword::from_value(v, i));
        [x, y].into_iter().flatten()
    }
}

impl OpticalScale3_0 {
    /// Convert to a simple scale value (just $PnE, no $PnG).
    ///
    /// This may be lossy because the $PnG value cannot be represented with
    /// just a `Scale` object, and thus needs to be dropped if present and
    /// not equal to 1.0.
    fn try_convert_to_scale(self, i: MeasIndex) -> DeferredError<Scale, GainLossError> {
        match self {
            Self::Lin(x) => {
                let v = Scale::Linear;
                LogResult::new_log_if(x.is_one(), v, v, GainLossError(i))
            }
            Self::Log(x) => LogResult::new_ok(Scale::Log(x)),
        }
    }

    fn req_keyword(&self, i: MeasIndex) -> ReqMeasKeyword<'_> {
        let (scale, _): (Scale, _) = (*self).into();
        ReqMeasKeyword::from_value(scale, i)
    }

    fn opt_keyword(&self, i: MeasIndex) -> Option<OptScaleKeyword> {
        if let (_, Some(gain)) = (*self).into()
            && !gain.0.is_one()
        {
            Some(OptScaleKeyword::from_value(gain, i))
        } else {
            None
        }
    }
}

impl CommonMeasurement {
    fn lookup(std: &mut StdKeywords, nonstd: NonStdKeywords, i: MeasIndex) -> Self {
        let longname = Longname::remove_meas_opt_nofail(std, i);
        Self::new(longname, nonstd)
    }
}

// Misc functions

pub(crate) fn wrap_scaled_opticals<V: VersionMeasSet>(
    measurements: VNamedTemporalsAndOpticalsWithScale<V>,
) -> impl Iterator<Item = Either<V::Name, VTemporal<V>, VScaledOptical<V>>> {
    measurements
        .into_iter()
        .map(|e| e.bimap_once(|t| t, |(n, o, s)| (n, ScaledOptical::new(o, s))))
}

#[cfg(feature = "python")]
mod python {
    use super::OpticalScale3_0;

    use crate::text::ranged_float::PositiveFloat;

    use fireflow_types::python::InvalidKeywordValueError;

    use pyo3::IntoPyObjectExt as _;
    use pyo3::prelude::*;

    // $PnE/$PnG (3.0+) as a tuple like (f32) or (f32, f32) in python
    impl<'py> FromPyObject<'py> for OpticalScale3_0 {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            if let Ok(gain) = ob.extract::<PositiveFloat>() {
                Ok(Self::Lin(gain))
            } else if let Ok(log) = ob.extract::<(f32, f32)>() {
                Ok(Self::Log(log.try_into()?))
            } else {
                Err(InvalidKeywordValueError::new_err(
                    "scale transform must be a positive \
                     float or a 2-tuple of positive floats",
                ))
            }
        }
    }

    impl<'py> IntoPyObject<'py> for OpticalScale3_0 {
        type Target = PyAny;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            match self {
                Self::Lin(gain) => f32::from(gain).into_bound_py_any(py),
                Self::Log(l) => (f32::from(l.decades), f32::from(l.offset)).into_bound_py_any(py),
            }
        }
    }
}

//! Data structures representing standardized TEXT segment

use crate::config::{
    AllowLoss, AppendFlag, AppendableFlag, ConfigFlag as _, DatasetOffset, DatasetOffsetError,
    DisallowDeprecated, DisallowRangeTrunc, DummyTriFlag, ProcessKeywordFailure,
    ProcessOptionalFailure, ReadDataKeywordsConfig, ReadEventsConfig, ReadHeaderAndTEXTConfig,
    ReadOffsetConfig, ReadSharedConfig, ReadState, ReadStdKeywordsConfig,
    TemporalHasOpticalKeyError, TemporalOpticalKey, TimeMeasNamePattern, TriFlag,
    WriteDatasetInnerConfig, WriteMultiConfig, WriteMultiDatasetConfig, WriteMultiTEXTConfig,
    WriteTEXTInnerConfig,
};
use crate::data::{
    ConvertFromLayout, DataLayout2_0, DataLayout3_0, DataLayout3_1, DataLayout3_2,
    EventsDiagnostics, IndexedLossError, InsertRangeError, InterLayoutOps as _, IsTot,
    LayoutConvertError, LayoutOps as _, LookupLayoutError, LookupLayoutWarning,
    MeasLayoutMismatchError, MeasurementsWithLayoutError, NewDataLayoutError, ReadDataframeError,
    ReadDataframeWarning, ScaleDatatypeMismatchError, VersionedDataLayout,
};
use crate::header::{
    GuessVersionError, HeaderKeywordsToWrite, KeywordVersionScores, ParsedOtherSegments,
    RelativeOtherSegments, Version, Version2_0, Version3_0, Version3_1, Version3_2,
};
use crate::logging::{
    CommutativeResultIter as _, DeferredError, DeferredIter as _, DeferredSwitchableError,
    DeferredSwitchableErrors, DeferredWarningsAndErrors, ErrorGroup, ErrorResult, ErrorsResult,
    GroupResult, IOErrorGroup, ImpureError, LogResult, OptionExt as _, ResultExt as _, Success,
    SwitchableErrorResult, SwitchableErrorsResult, WarningAndErrorResult, WarningAndErrorsResult,
    WarningAndGroupResult, WarningOrErrorResult, WarningsAndErrorsResult, WarningsAndGroupResult,
    WarningsAndIOGroupResult, WarningsResult, io_to_log, split_log,
};
use crate::macros::{def_summary, match_many_to_one};
use crate::segment::{
    AnalysisSegmentId, AnyAnalysisSegment, AnyDataSegment, DataSegmentId, HeaderAnalysisSegment,
    HeaderDataSegment, KeyedOptSegmentWithDefault as _, KeyedReqSegmentWithDefault as _,
    NonDataSegments, OptSegmentWithDefaultWarning, OtherSegment20, OtherSegmentId, RelativeSegment,
    RelativeToAbsSegmentError, ReqSegmentWithDefaultError, ReqSegmentWithDefaultWarning,
    SegmentMismatchWarning, UncorrectedSegment,
};
use crate::text::compensation::{Compensation, Compensation2_0, LookupComp2_0Error};
use crate::text::datetimes::{
    BeginDateTime, DatetimeLossError, Datetimes, EndDateTime, LookupDatetimesError,
    ReversedDatetimesError,
};
use crate::text::deprecated::{
    AnyDepKeyError, DeprecatedPeakRef, DeprecatedPlateRef, DeprecatedRef, DeprecatedStrRef,
    IndexedDepRef, IsDeprecated as _,
};
use crate::text::gating::{
    AppliedGates2_0, AppliedGates2_0To3_2LossError, AppliedGates3_0, AppliedGates3_0To2_0Error,
    AppliedGates3_0To3_2Error, AppliedGates3_2, GatingSchemeLossError, LookupAppliedGates2_0Error,
    LookupAppliedGates3_0Error, LookupAppliedGates3_2Error,
};
use crate::text::index::{IndexFromOne, MeasIndex};
use crate::text::keywords::{
    Abrt, Analyte, AnyScaleDiagnostic, CSMode, CSTot, CSVBits, CSVFlag, Calibration3_1,
    Calibration3_2, CalibrationLossError, Carrierid, Carriertype, Cells, Com, Compensation3_0, Cyt,
    Cyt3_2, Cytsn, DeprecatedModeWarning, DetectorName, DetectorType, DetectorVoltage, Dfc,
    Display, Exp, ExtraStdKeywords, Feature, Fil, Filter, Flowrate, Gain, HyperGateError,
    HyperParError, Inst, KeywordOtherVersionError, LastModified, LastModifier, Locationid,
    LogScale, Longname, LookupTemporalGainError, Lost, MeasOrGateIndex, Mode, Mode3_2,
    ModeUpgradeError, Nextdata, NoCytError, Op, OpticalFeature, OpticalType, Originality, Par,
    PeakBin, PeakIndex, PercentEmitted, Plateid, Platename, Power, PrefixedMeasIndex, Proj,
    PseudostandardError, Range, Scale, ScaleDiagnostic, Smno, Src, Sys, Tag, TemporalScale2_0,
    TemporalScale3_0, TemporalScaleDiagnostic, TemporalType, Timestep, TimestepFoundError, Tot,
    Trigger, Unicode, UnstainedCenters, UnstainedInfo, Vol, Wavelength, Wavelengths,
    WavelengthsLossError, Wellid,
};
use crate::text::lookup::{
    OptIndexedKey as _, OptIndexedKeyError, OptIndexedKeyStError, OptKeyError, OptKeyStError,
    OptMetarootKey as _, ReqIndexedKey as _, ReqIndexedKeyError, ReqIndexedStKeyError, ReqKeyError,
    ReqMetarootKey as _,
};
use crate::text::named_vec::{
    EitherPair, Eithers, Element, ElementIndexError, IndexedElement, InputLengthError,
    InsertCenterError, InsertError, NameMapping, NameNotFoundError, NamePresentError, NamedSet,
    NamedVec, NewNamedVecError, NonCenterElement, PushCenterError, RenameError, SetCenterError,
    SetElementsError, SetKeysError, SetNamesError, SetValuesError, uniquify_names,
};
use crate::text::optional::{CheckMaybe as _, Identity, KeywordPairMaybe as _, MightHave, Nothing};
use crate::text::ranged_float::PositiveFloat;
use crate::text::relational::{
    AnyExistingIndexLinkError, AnyExistingNamedLinkError, BrokenIndexedLinkError,
    BrokenNamedLinkError, BrokenOrDependentLinkError, BrokenRegionLinkError,
    ExistingIndexedLinkError, ExistingLinkError, ExistingLinkErrors, IndicesToRemove,
    KeyToNameLinkError, OpticalNamesToRemove, RemovedLink,
};
use crate::text::spillover::Spillover;
use crate::text::timestamps::{
    Btim, Etim, FCSDate, FCSTime, FCSTime60, FCSTime60Error, FCSTime100, FCSTime100Error,
    FCSTimeError, LookupTimestampsError, ReversedTimestampsError, Timestamps, Xtim,
};
use crate::validated::ascii_uint::{
    HeaderString, Uint8DigitOverflowError, UintSpacePad8, UintSpacePad20,
};
use crate::validated::dataframe as df;
use crate::validated::dataframe::{AnyFCSColumn, FCSDataFrame};
use crate::validated::keys::{
    BiIndexedKey, IndexedKey, Key, Key0, Key1, Key2, MeasHeader, NonStdKey, NonStdKeywords,
    NonStdKeywordsExt as _, StdKey, StdKeywords, ValidKeywords,
};
use crate::validated::nonstd_meas_pattern::NonStdMeasRegexError;
use crate::validated::shortname::Shortname;
use crate::validated::textdelim::TEXTDelim;

use type_families::{ApplyOnce as _, BifunctorOnce as _, Functor as _, FunctorOnce as _, Pointed};

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveTime};
use derive_more::{AsMut, AsRef, Display, From};
use derive_new::new;
use itertools::Itertools as _;
use nalgebra::DMatrix;
use nonempty::NonEmpty;
use num_traits::identities::{One as _, Zero};
use regex::Regex;
use thiserror::Error;

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::convert::{AsRef, Infallible};
use std::fmt;
use std::io::{self, BufReader, BufWriter, Read, Seek, Write};
use std::iter::{empty, once};
use std::mem;
use std::path::PathBuf;

#[cfg(feature = "serde")]
use {crate::data::req_meas_headers, serde::Serialize, std::string::ToString as _};

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr, FromInnerPyObject},
    pyo3::prelude::*,
};

/// A standardized representation of one FCS dataset.
///
/// This is the main type that handles all "standard mode" operations.
///
/// This is highly generic to allow different FCS versions to share code, and
/// also to encode presence/absence of the "data" in a dataset
/// (ie DATA+ANALYSIS+OTHER). At minimum, this contains the TEXT segment and
/// all keywords are decomposed and stored as native Rust types in a natural
/// hierarchy.
///
/// # Concrete type overview
///
/// The following concrete types (along with their FCS versions) are defined:
///
/// | version | TEXT only       | entire dataset     |
/// |---------|-----------------|--------------------|
/// |     2.0 | [`CoreTEXT2_0`] | [`CoreDataset2_0`] |
/// |     3.0 | [`CoreTEXT3_0`] | [`CoreDataset3_0`] |
/// |     3.1 | [`CoreTEXT3_1`] | [`CoreDataset3_1`] |
/// |     3.2 | [`CoreTEXT3_2`] | [`CoreDataset3_2`] |
///
/// # Generic parameters for version-specific behavior
///
/// * `M`: version-specific type for keywords which don't belong to a measurement (metaroot)
/// * `T`: version-specific type for temporal measurement keywords
/// * `P`: version-specific type for optical measurement keywords
/// * `N`: version-specific type for $PnN
/// * `L`: version-specific type for layout keywords
///
/// The types for these parameters and their specific FCS versions are
/// summarized as follows:
///
/// | version | `M`                  | `T`                  | `P`                 | `N`                     | `L`               |
/// |---------|----------------------|----------------------|---------------------|-------------------------|-------------------|
/// |     2.0 | [`InnerMetaroot2_0`] | [`InnerTemporal2_0`] | [`InnerOptical2_0`] | [`Option<Shortname>`]   | [`DataLayout2_0`] |
/// |     3.0 | [`InnerMetaroot3_0`] | [`InnerTemporal3_0`] | [`InnerOptical2_0`] | [`Option<Shortname>`]   | [`DataLayout3_0`] |
/// |     3.1 | [`InnerMetaroot3_1`] | [`InnerTemporal3_1`] | [`InnerOptical2_0`] | [`Identity<Shortname>`] | [`DataLayout3_1`] |
/// |     3.2 | [`InnerMetaroot3_2`] | [`InnerTemporal3_2`] | [`InnerOptical2_0`] | [`Identity<Shortname>`] | [`DataLayout3_2`] |
///
/// # Generic parameters for data
///
/// * `A`: the ANALYSIS segment ([`Analysis`])
/// * `D`: the DATA segment ([`FCSDataFrame`])
/// * `O`: the OTHER segments ([`Others`])
///
/// Each of these are either their indicated Rust type above or `()` if not
/// included. The former corresponds to [`CoreDataset`] and the latter
/// corresponds to [`CoreTEXT`].
///
/// # Caveats
///
/// Importantly this does NOT include the following:
/// - $TOT (inferred from summed bit width and length of DATA)
/// - $PAR (inferred from length of measurement vector)
/// - $NEXTDATA (handled elsewhere)
/// - $(BEGIN|END)(DATA|ANALYSIS|STEXT) (handled elsewhere)
///
/// These are not included because this struct will also be used to encode the
/// TEXT data when writing a new FCS file, and the keywords that are not
/// included can be computed on the fly when writing.
#[derive(Clone, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[new(visibility = "")]
// NOTE fields are private since metaroot, measurements, and layout are all
// related to each other and must be kept in sync
pub struct Core<A, D, O, M, T, P, N, L> {
    /// Metaroot TEXT keywords.
    ///
    /// This includes all keywords that are not part of measurements or the data
    /// layout (ie the "root" of the metadata if thought of as a hierarchy)
    metaroot: Metaroot<M>,

    /// All measurement TEXT keywords.
    ///
    /// Specifically these are denoted by "$Pn*" keywords where "n" is the index
    /// of the measurement which also corresponds to its column in the DATA
    /// segment. The index of each measurement in this vector is n - 1.
    measurements: NamedVec<N, Temporal<T>, Optical<P>>,

    /// The byte layout of the DATA segment
    ///
    /// This is derived from $BYTEORD, $DATATYPE, $PnB, $PnR and maybe
    /// $PnDATATYPE for version 3.2.
    layout: L,

    /// DATA segment (if applicable)
    data: D,

    /// ANALYSIS segment (if applicable)
    analysis: A,

    /// Other segments (if applicable)
    others: O,
}

/// The ANALYSIS segment, which is just a string of bytes
#[derive(Clone, From, PartialEq, Default)]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
pub struct Analysis(pub Vec<u8>);

/// An OTHER segment, which is just a string of bytes
#[derive(Clone, From, PartialEq)]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
pub struct Other(pub Vec<u8>);

/// All OTHER segments
#[derive(Clone, Default, From, PartialEq)]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
pub struct Others(pub Vec<Other>);

/// Root of the metadata hierarchy.
///
/// Explicit fields are common to all FCS versions.
///
/// The generic type parameter allows version-specific data to be encoded.
#[allow(clippy::too_many_arguments)]
#[derive(Clone, AsRef, AsMut, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct Metaroot<X> {
    /// Value of $ABRT
    #[as_ref(Option<Abrt>)]
    #[as_mut(Option<Abrt>)]
    #[new(into)]
    pub abrt: Option<Abrt>,

    /// Value of $COM
    #[as_ref(Com)]
    #[as_mut(Com)]
    #[new(into)]
    pub com: Com,

    /// Value of $CELLS
    #[as_ref(Cells)]
    #[as_mut(Cells)]
    #[new(into)]
    pub cells: Cells,

    /// Value of $EXP
    #[as_ref(Exp)]
    #[as_mut(Exp)]
    #[new(into)]
    pub exp: Exp,

    /// Value of $FIL
    #[as_ref(Fil)]
    #[as_mut(Fil)]
    #[new(into)]
    pub fil: Fil,

    /// Value of $INST
    #[as_ref(Inst)]
    #[as_mut(Inst)]
    #[new(into)]
    pub inst: Inst,

    /// Value of $LOST
    #[as_ref(Option<Lost>)]
    #[as_mut(Option<Lost>)]
    #[new(into)]
    pub lost: Option<Lost>,

    /// Value of $OP
    #[as_ref(Op)]
    #[as_mut(Op)]
    #[new(into)]
    pub op: Op,

    /// Value of $PROJ
    #[as_ref(Proj)]
    #[as_mut(Proj)]
    #[new(into)]
    pub proj: Proj,

    /// Value of $SMNO
    #[as_ref(Smno)]
    #[as_mut(Smno)]
    #[new(into)]
    pub smno: Smno,

    /// Value of $SRC
    #[as_ref(Src)]
    #[as_mut(Src)]
    #[new(into)]
    pub src: Src,

    /// Value of $SYS
    #[as_ref(Sys)]
    #[as_mut(Sys)]
    #[new(into)]
    pub sys: Sys,

    /// Value of $TR
    #[as_ref(Option<Trigger>)]
    #[new(into)]
    // NOTE not mutable to prevent mutation when part of Core
    pub tr: Option<Trigger>,

    /// Version-specific data
    pub specific: X,

    /// Non-standard keywords.
    ///
    /// This will include all the keywords that do not start with '$'.
    ///
    /// Keywords which do start with '$' but are not part of the standard are
    /// considered 'pseudostandard' and stored elsewhere since this structure
    /// will also be used to write FCS-compliant files (which do not allow
    /// nonstandard keywords starting with '$')
    pub nonstandard_keywords: NonStdKeywords,
}

#[derive(Clone, Default, AsRef, AsMut, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct CommonMeasurement {
    /// Value for $PnS
    #[as_ref(Longname)]
    #[as_mut(Longname)]
    #[new(into)]
    pub longname: Longname,

    /// Non standard keywords that belong to this measurement.
    ///
    /// These are found using a configurable pattern to filter matching keys.
    #[as_ref(NonStdKeywords)]
    #[as_mut(NonStdKeywords)]
    pub nonstandard_keywords: NonStdKeywords,
}

/// Structured data for time keywords.
///
/// Explicit fields are common to all versions. The generic type parameter
/// allows for version-specific information to be encoded.
#[derive(Clone, AsRef, AsMut, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct Temporal<X> {
    /// Fields shared with optical measurements
    #[as_ref(forward)]
    #[as_mut(forward)]
    pub common: CommonMeasurement,

    /// Version specific data
    pub specific: X,
}

/// Structured data for optical keywords.
///
/// Explicit fields are common to all versions. The generic type parameter
/// allows for version-specific information to be encoded.
#[derive(Clone, AsRef, AsMut, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct Optical<X> {
    /// Fields shared with optical measurements
    #[as_ref(forward)]
    #[as_mut(forward)]
    pub common: CommonMeasurement,

    /// Value for $PnF
    #[as_ref(Filter)]
    #[as_mut(Filter)]
    #[new(into)]
    pub filter: Filter,

    /// Value for $PnO
    #[as_ref(Option<Power>)]
    #[as_mut(Option<Power>)]
    #[new(into)]
    pub power: Option<Power>,

    /// Value for $PnD
    #[as_ref(DetectorType)]
    #[as_mut(DetectorType)]
    #[new(into)]
    pub detector_type: DetectorType,

    /// Value for $PnP
    #[as_ref(Option<PercentEmitted>)]
    #[as_mut(Option<PercentEmitted>)]
    #[new(into)]
    pub percent_emitted: Option<PercentEmitted>,

    /// Value for $PnV
    #[as_ref(Option<DetectorVoltage>)]
    #[as_mut(Option<DetectorVoltage>)]
    #[new(into)]
    pub detector_voltage: Option<DetectorVoltage>,

    /// Version specific data
    pub specific: X,
}

/// Standardized FCS dataset for any version
#[derive(Clone, From)]
pub enum AnyCore<A, D, O> {
    #[from(Core2_0<A, D, O>)]
    FCS2_0(Box<Core2_0<A, D, O>>),
    #[from(Core3_0<A, D, O>)]
    FCS3_0(Box<Core3_0<A, D, O>>),
    #[from(Core3_1<A, D, O>)]
    FCS3_1(Box<Core3_1<A, D, O>>),
    #[from(Core3_2<A, D, O>)]
    FCS3_2(Box<Core3_2<A, D, O>>),
}

pub type AnyCoreTEXT = AnyCore<(), (), ()>;
pub type AnyCoreDataset = AnyCore<Analysis, FCSDataFrame, Others>;

macro_rules! match_anycore {
    ($self:expr, $bind:ident, $stuff:block) => {
        match_many_to_one!($self, Self, [FCS2_0, FCS3_0, FCS3_1, FCS3_2], $bind, $stuff)
    };
}

impl<A, D, O> AnyCore<A, D, O> {
    #[must_use]
    pub fn version(&self) -> Version {
        match_many_to_one!(self, Self, [FCS2_0, FCS3_0, FCS3_1, FCS3_2], x, {
            (*x).fcs_version()
        })
    }

    #[must_use]
    pub fn shortnames(&self) -> Vec<Shortname> {
        match_anycore!(self, x, { x.all_shortnames() })
    }

    #[cfg(feature = "serde")]
    pub fn print_meas_table(&self, delim: &str) {
        match_anycore!(self, x, { x.print_meas_table(delim) });
    }

    pub fn print_comp_or_spillover_table(&self, delim: &str) {
        if let Some((names, matrix)) = self.spillover_or_comp_table() {
            let header = once("[-]")
                .chain(names.iter().map(AsRef::as_ref))
                .join(delim);
            println!("{header}");
            for (r, n) in matrix.row_iter().zip(&names[..]) {
                println!("{n}{delim}{}", r.iter().join(delim));
            }
        } else {
            println!("[]");
        }
    }

    fn spillover_or_comp_table(&self) -> Option<(Vec<Shortname>, DMatrix<f32>)> {
        match self {
            Self::FCS2_0(x) => x.named_compensation(),
            Self::FCS3_0(x) => x.named_compensation(),
            Self::FCS3_1(x) => x.named_spillover(),
            Self::FCS3_2(x) => x.named_spillover(),
        }
    }
}

impl AnyCoreTEXT {
    #[allow(clippy::type_complexity)]
    pub(crate) fn parse_flat<C>(
        version: Version,
        kws: ValidKeywords,
        segs: &NonDataSegments,
        st: &ReadState<C>,
    ) -> WarningsAndErrorsResult<
        (
            Self,
            StdTEXTDiagnostics,
            TEXTOffsets<Option<Tot>>,
            Option<KeywordVersionScores>,
        ),
        (),
        StdTEXTFromFlatTEXTWarning,
        StdTEXTFromFlatTEXTError,
    >
    where
        C: AsRef<ReadHeaderAndTEXTConfig>
            + AsRef<ReadOffsetConfig>
            + AsRef<ReadStdKeywordsConfig>
            + AsRef<ReadDataKeywordsConfig>,
    {
        macro_rules! go {
            ($t:ident, $s:expr) => {
                $t::new_from_keywords_with_offsets(kws, segs, st)
                    .map_ok_value(|(x, y, z)| (x.into(), y, z.into_common(), $s))
                    .map_errors(StdTEXTFromFlatTEXTError::from)
            };
        }

        let sconf: &ReadHeaderAndTEXTConfig = st.conf.as_ref();

        match version.autodetect(&kws.std, sconf.version_override.as_ref()) {
            Ok((ver, scores)) => match ver {
                Version::FCS2_0 => go!(CoreTEXT2_0, scores),
                Version::FCS3_0 => go!(CoreTEXT3_0, scores),
                Version::FCS3_1 => go!(CoreTEXT3_1, scores),
                Version::FCS3_2 => go!(CoreTEXT3_2, scores),
            },
            Err(e) => LogResult::new_err(StdTEXTFromFlatTEXTError::from(e)),
        }
    }
}

impl AnyCoreDataset {
    #[must_use]
    pub fn as_data(&self) -> &FCSDataFrame {
        match_anycore!(self, x, { &x.data })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_from_keywords<C, R>(
        h: &mut BufReader<R>,
        version: Version,
        kws: ValidKeywords,
        data_seg: HeaderDataSegment,
        analysis_seg: HeaderAnalysisSegment,
        other_segs: ParsedOtherSegments,
        st: &ReadState<C>,
    ) -> WarningsAndIOGroupResult<
        (Self, StdDatasetWithKwsOutput, Option<KeywordVersionScores>),
        StdDatasetFromFlatTEXTWarning,
        StdDatasetFromFlatTextError,
        (),
    >
    where
        R: Read + Seek,
        C: AsRef<ReadHeaderAndTEXTConfig>
            + AsRef<ReadOffsetConfig>
            + AsRef<ReadStdKeywordsConfig>
            + AsRef<ReadDataKeywordsConfig>
            + AsRef<ReadEventsConfig>,
    {
        let segs = NonDataSegments::new_no_text(data_seg, analysis_seg, other_segs);

        macro_rules! go {
            ($t:ident, $s:expr) => {
                $t::new_from_keywords_inner(h, kws, &segs, st)
                    .map_ok_value(|(x, y)| (x.into(), y, $s))
                    .map_pure_errors(StdDatasetFromFlatTextError::from)
            };
        }

        let sconf: &ReadHeaderAndTEXTConfig = st.conf.as_ref();

        match version.autodetect(&kws.std, sconf.version_override.as_ref()) {
            Ok((ver, scores)) => match ver {
                Version::FCS2_0 => go!(CoreDataset2_0, scores),
                Version::FCS3_0 => go!(CoreDataset3_0, scores),
                Version::FCS3_1 => go!(CoreDataset3_1, scores),
                Version::FCS3_2 => go!(CoreDataset3_2, scores),
            },
            Err(e) => LogResult::new_err(IOErrorGroup::new_pure_one(e.into())),
        }
    }
}

/// Metaroot fields specific to version 2.0
#[derive(Clone, AsRef, AsMut, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct InnerMetaroot2_0 {
    /// Value of $MODE
    #[as_ref(Mode)]
    #[as_mut(Mode)]
    pub mode: Mode,

    /// Value of $CYT
    #[as_ref(Cyt)]
    #[as_mut(Cyt)]
    #[new(into)]
    pub cyt: Cyt,

    /// Compensation matrix derived from 'DFCnTOm' key/value pairs
    #[as_ref(Option<Compensation2_0>)]
    #[new(into)]
    // NOTE not mutable to prevent mutation when part of Core
    pub comp: Option<Compensation2_0>,

    /// Values of $BTIM/ETIM/$DATE
    #[as_ref(Timestamps2_0, Option<FCSDate>)]
    #[as_mut(Timestamps2_0)]
    pub timestamps: Timestamps2_0,

    /// Values of $Gm*/$RnI/$RnW/$GATING/$GATE
    #[as_ref(AppliedGates2_0)]
    #[as_mut(AppliedGates2_0)]
    // NOTE not mutable to prevent mutation when part of Core
    pub applied_gates: AppliedGates2_0,
}

/// Metaroot fields specific to version 3.0
#[allow(clippy::too_many_arguments)]
#[derive(Clone, AsRef, AsMut, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct InnerMetaroot3_0 {
    /// Value of $MODE
    #[as_ref(Mode)]
    #[as_mut(Mode)]
    pub mode: Mode,

    /// Value of $CYT
    #[as_ref(Cyt)]
    #[as_mut(Cyt)]
    #[new(into)]
    pub cyt: Cyt,

    /// Value of $COMP
    #[as_ref(Option<Compensation3_0>)]
    #[new(into)]
    // NOTE not mutable to prevent mutation when part of Core
    pub comp: Option<Compensation3_0>,

    /// Values of $BTIM/ETIM/$DATE
    #[as_ref(Timestamps3_0, Option<FCSDate>)]
    #[as_mut(Timestamps3_0)]
    pub timestamps: Timestamps3_0,

    /// Value of $CYTSN
    #[as_ref(Cytsn)]
    #[as_mut(Cytsn)]
    #[new(into)]
    pub cytsn: Cytsn,

    /// Value of $UNICODE
    #[as_ref(Option<Unicode>)]
    #[as_mut(Option<Unicode>)]
    #[new(into)]
    pub unicode: Option<Unicode>,

    /// Aggregated values for $CS* keywords
    #[as_ref(CSVBits)]
    #[as_mut(CSVBits)]
    #[as_ref(CSTot)]
    #[as_mut(CSTot)]
    #[as_ref(CSVFlags)]
    #[as_mut(CSVFlags)]
    pub subset: SubsetData,

    /// Values of $Gm*/$RnI/$RnW/$GATING/$GATE
    #[as_ref(AppliedGates3_0)]
    #[new(into)]
    // NOTE not mutable to prevent mutation when part of Core
    pub applied_gates: AppliedGates3_0,
}

/// Metaroot fields specific to version 3.1
#[allow(clippy::too_many_arguments)]
#[derive(Clone, AsRef, AsMut, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct InnerMetaroot3_1 {
    /// Value of $MODE
    #[as_ref(Mode)]
    #[as_mut(Mode)]
    pub mode: Mode,

    /// Value of $CYT
    #[as_ref(Cyt)]
    #[as_mut(Cyt)]
    #[new(into)]
    pub cyt: Cyt,

    /// Values of $BTIM/ETIM/$DATE
    #[as_ref(Timestamps3_1, Option<FCSDate>)]
    #[as_mut(Timestamps3_1)]
    pub timestamps: Timestamps3_1,

    /// Value of $CYTSN
    #[as_ref(Cytsn)]
    #[as_mut(Cytsn)]
    #[new(into)]
    pub cytsn: Cytsn,

    /// Value of $SPILLOVER
    #[as_ref(Option<Spillover>)]
    #[new(into)]
    // NOTE not mutable to prevent mutation when part of Core
    pub spillover: Option<Spillover>,

    /// Values of $LAST_MODIFIED/$LAST_MODIFIER/$ORIGINALITY
    #[as_ref(LastModifier, Option<LastModified>, Option<Originality>)]
    #[as_mut(LastModifier, Option<LastModified>, Option<Originality>)]
    pub modification: ModificationData,

    /// Values of $PLATEID/$PLATENAME/$WELLID
    #[as_ref(Plateid, Wellid, Platename)]
    #[as_mut(Plateid, Wellid, Platename)]
    pub plate: PlateData,

    /// Value of $VOL
    #[as_ref(Option<Vol>)]
    #[as_mut(Option<Vol>)]
    #[new(into)]
    pub vol: Option<Vol>,

    /// Aggregated values for $CS* keywords
    #[as_ref(CSVBits)]
    #[as_mut(CSVBits)]
    #[as_ref(CSTot)]
    #[as_mut(CSTot)]
    #[as_ref(CSVFlags)]
    #[as_mut(CSVFlags)]
    pub subset: SubsetData,

    /// Values of $Gm*/$RnI/$RnW/$GATING/$GATE
    #[as_ref(AppliedGates3_0)]
    #[new(into)]
    // NOTE not mutable to prevent mutation when part of Core
    pub applied_gates: AppliedGates3_0,
}

/// Metaroot fields specific to version 3.2
#[allow(clippy::too_many_arguments)]
#[derive(Clone, AsRef, AsMut, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct InnerMetaroot3_2 {
    /// Value of $MODE
    #[as_ref(Option<Mode3_2>)]
    #[as_mut(Option<Mode3_2>)]
    #[new(into)]
    pub mode: Option<Mode3_2>,

    /// Values of $BTIM/ETIM/$DATE
    #[as_ref(Timestamps3_1, Option<FCSDate>)]
    #[as_mut(Timestamps3_1)]
    pub timestamps: Timestamps3_1,

    /// Values of $BEGINDATETIME/$ENDDATETIME
    #[as_ref(Option<BeginDateTime>, Option<EndDateTime>, Datetimes)]
    #[as_mut(Datetimes)]
    pub datetimes: Datetimes,

    /// Value of $CYT
    #[as_ref(Cyt3_2)]
    #[as_mut(Cyt3_2)]
    pub cyt: Cyt3_2,

    /// Value of $SPILLOVER
    #[as_ref(Option<Spillover>)]
    #[new(into)]
    // NOTE not mutable to prevent mutation when part of Core
    pub spillover: Option<Spillover>,

    /// Value of $CYTSN
    #[as_ref(Cytsn)]
    #[as_mut(Cytsn)]
    #[new(into)]
    pub cytsn: Cytsn,

    /// Values of $LAST_MODIFIED/$LAST_MODIFIER/$ORIGINALITY
    #[as_ref(LastModifier, Option<LastModified>, Option<Originality>)]
    #[as_mut(LastModifier, Option<LastModified>, Option<Originality>)]
    pub modification: ModificationData,

    /// Values of $PLATEID/$PLATENAME/$WELLID
    #[as_ref(Plateid, Wellid, Platename)]
    #[as_mut(Plateid, Wellid, Platename)]
    pub plate: PlateData,

    /// Value of $VOL
    #[as_ref(Option<Vol>)]
    #[as_mut(Option<Vol>)]
    #[new(into)]
    pub vol: Option<Vol>,

    /// Values of $CARRIERID/$CARRIERTYPE/$LOCATIONID
    #[as_ref(Carrierid, Carriertype, Locationid)]
    #[as_mut(Carrierid, Carriertype, Locationid)]
    pub carrier: CarrierData,

    /// Values of $UNSTAINEDINFO/$UNSTAINEDCENTERS
    #[as_ref(UnstainedCenters, UnstainedInfo)]
    #[as_mut(UnstainedInfo)]
    pub unstained: UnstainedData,

    /// Value of $FLOWRATE
    #[as_ref(Flowrate)]
    #[as_mut(Flowrate)]
    #[new(into)]
    pub flowrate: Flowrate,

    /// Values of $RnI/$RnW/$GATING
    #[as_ref(AppliedGates3_2)]
    // NOTE not mutable to prevent mutation when part of Core
    pub applied_gates: AppliedGates3_2,
}

/// Temporal measurement fields specific to version 2.0
#[derive(Clone, Default, AsRef, AsMut, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct InnerTemporal2_0 {
    /// Value of $PnE
    ///
    /// Unlike subsequent versions, included here because it is optional rather
    /// than required and constant.
    #[as_ref(TemporalScale2_0)]
    #[as_mut(TemporalScale2_0)]
    #[new(into)]
    pub scale: TemporalScale2_0,

    /// Values of $Pkn/$PKNn
    #[as_ref(Option<PeakBin>)]
    #[as_ref(Option<PeakIndex>)]
    #[as_mut(Option<PeakBin>)]
    #[as_mut(Option<PeakIndex>)]
    pub peak: PeakData,
}

/// Temporal measurement fields specific to version 3.0
///
/// $PnE is implied as linear but not included since it only has one value
#[derive(Clone, AsRef, AsMut, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct InnerTemporal3_0 {
    /// Value for $TIMESTEP
    #[as_ref(Timestep)]
    #[as_mut(Timestep)]
    pub timestep: Timestep,

    /// Values of $Pkn/$PKNn
    #[as_ref(Option<PeakBin>)]
    #[as_ref(Option<PeakIndex>)]
    #[as_mut(Option<PeakBin>)]
    #[as_mut(Option<PeakIndex>)]
    pub peak: PeakData,
}

/// Temporal measurement fields specific to version 3.1
///
/// $PnE is implied as linear but not included since it only has one value
#[derive(Clone, AsRef, AsMut, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct InnerTemporal3_1 {
    /// Value for $TIMESTEP
    #[as_ref(Timestep)]
    #[as_mut(Timestep)]
    pub timestep: Timestep,

    /// Value for $PnD
    #[as_ref(Option<Display>)]
    #[as_mut(Option<Display>)]
    #[new(into)]
    pub display: Option<Display>,

    /// Values of $Pkn/$PKNn
    #[as_ref(Option<PeakBin>)]
    #[as_ref(Option<PeakIndex>)]
    #[as_mut(Option<PeakBin>)]
    #[as_mut(Option<PeakIndex>)]
    pub peak: PeakData,
}

/// Temporal measurement fields specific to version 3.2
///
/// $PnE is implied as linear but not included since it only has one value
#[derive(Clone, AsRef, AsMut, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct InnerTemporal3_2 {
    /// Value for $TIMESTEP
    #[as_ref(Timestep)]
    #[as_mut(Timestep)]
    pub timestep: Timestep,

    /// Value for $PnD
    #[as_ref(Option<Display>)]
    #[as_mut(Option<Display>)]
    #[new(into)]
    pub display: Option<Display>,

    /// Value for $PnTYPE
    #[as_ref(TemporalType)]
    #[as_mut(TemporalType)]
    #[new(into)]
    pub measurement_type: TemporalType,
}

/// Optical measurement fields specific to version 2.0
#[derive(Clone, Default, AsRef, AsMut, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct InnerOptical2_0 {
    /// Value for $PnE
    ///
    /// This does not accessible via [`AsMut`] since this would expose this
    /// value to modification via [`Core::set_optical`] which we do not want
    /// since [`ScaleTransform`] needs to be synced with [`Core::layout`]. Consequently,
    /// the measurement array in `Core` is also private.
    ///
    /// There is no harm in modifying `scale` when this struct is on its own,
    /// however, so it is still public.
    #[as_ref(Option<Scale>)]
    #[new(into)]
    pub scale: Option<Scale>,

    /// Value for $PnL
    #[as_ref(Option<Wavelength>)]
    #[as_mut(Option<Wavelength>)]
    #[new(into)]
    pub wavelength: Option<Wavelength>,

    /// Values of $Pkn/$PKNn
    #[as_ref(Option<PeakBin>)]
    #[as_ref(Option<PeakIndex>)]
    #[as_mut(Option<PeakBin>)]
    #[as_mut(Option<PeakIndex>)]
    pub peak: PeakData,
}

/// Optical measurement fields specific to version 3.0
#[derive(Clone, AsRef, AsMut, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct InnerOptical3_0 {
    /// Value for $PnE/$PnG
    ///
    /// This does not accessible via [`AsMut`] since this would expose this
    /// value to modification via [`Core::set_optical`] which we do not want
    /// since [`ScaleTransform`] needs to be synced with [`Core::layout`]. Consequently,
    /// the measurement array in `Core` is also private.
    ///
    /// There is no harm in modifying `scale` when this struct is on its own,
    /// however, so it is still public.
    #[as_ref(ScaleTransform)]
    #[new(into)]
    pub scale: ScaleTransform,

    /// Value for $PnL
    #[as_ref(Option<Wavelength>)]
    #[as_mut(Option<Wavelength>)]
    #[new(into)]
    pub wavelength: Option<Wavelength>,

    /// Values of $Pkn/$PKNn
    #[as_ref(Option<PeakBin>)]
    #[as_ref(Option<PeakIndex>)]
    #[as_mut(Option<PeakBin>)]
    #[as_mut(Option<PeakIndex>)]
    pub peak: PeakData,
}

/// Optical measurement fields specific to version 3.1
#[derive(Clone, AsRef, AsMut, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct InnerOptical3_1 {
    /// Value for $PnE/$PnG
    ///
    /// This does not accessible via [`AsMut`] since this would expose this
    /// value to modification via [`Core::set_optical`] which we do not want
    /// since [`ScaleTransform`] needs to be synced with [`Core::layout`]. Consequently,
    /// the measurement array in `Core` is also private.
    ///
    /// There is no harm in modifying `scale` when this struct is on its own,
    /// however, so it is still public.
    #[as_ref(ScaleTransform)]
    #[new(into)]
    pub scale: ScaleTransform,

    /// Value for $PnL
    #[as_ref(Wavelengths)]
    #[as_mut(Wavelengths)]
    #[new(into)]
    pub wavelengths: Wavelengths,

    /// Value for $PnCALIBRATION
    #[as_ref(Option<Calibration3_1>)]
    #[as_mut(Option<Calibration3_1>)]
    #[new(into)]
    pub calibration: Option<Calibration3_1>,

    /// Value for $PnD
    #[as_ref(Option<Display>)]
    #[as_mut(Option<Display>)]
    #[new(into)]
    pub display: Option<Display>,

    /// Values of $Pkn/$PKNn
    #[as_ref(Option<PeakBin>)]
    #[as_ref(Option<PeakIndex>)]
    #[as_mut(Option<PeakBin>)]
    #[as_mut(Option<PeakIndex>)]
    pub peak: PeakData,
}

/// Optical measurement fields specific to version 3.2
#[allow(clippy::too_many_arguments)]
#[derive(Clone, AsRef, AsMut, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct InnerOptical3_2 {
    /// Value for $PnE/$PnG
    ///
    /// This does not accessible via [`AsMut`] since this would expose this
    /// value to modification via [`Core::set_optical`] which we do not want
    /// since [`ScaleTransform`] needs to be synced with [`Core::layout`]. Consequently,
    /// the measurement array in `Core` is also private.
    ///
    /// There is no harm in modifying `scale` when this struct is on its own,
    /// however, so it is still public.
    #[as_ref(ScaleTransform)]
    #[new(into)]
    pub scale: ScaleTransform,

    /// Value for $PnL
    #[as_ref(Wavelengths)]
    #[as_mut(Wavelengths)]
    #[new(into)]
    pub wavelengths: Wavelengths,

    /// Value for $PnCALIBRATION
    #[as_ref(Option<Calibration3_2>)]
    #[as_mut(Option<Calibration3_2>)]
    #[new(into)]
    pub calibration: Option<Calibration3_2>,

    /// Value for $PnD
    #[as_ref(Option<Display>)]
    #[as_mut(Option<Display>)]
    #[new(into)]
    pub display: Option<Display>,

    /// Value for $PnANALYTE
    #[as_ref(Analyte)]
    #[as_mut(Analyte)]
    #[new(into)]
    pub analyte: Analyte,

    /// Value for $PnFEATURE
    #[as_ref(Option<Feature>)]
    #[as_mut(Option<Feature>)]
    #[new(into)]
    pub feature: Option<Feature>,

    /// Value for $PnTYPE
    #[as_ref(OpticalType)]
    #[as_mut(OpticalType)]
    #[new(into)]
    pub measurement_type: OpticalType,

    /// Value for $PnTAG
    #[as_ref(Tag)]
    #[as_mut(Tag)]
    #[new(into)]
    pub tag: Tag,

    /// Value for $PnDET
    #[as_ref(DetectorName)]
    #[as_mut(DetectorName)]
    #[new(into)]
    pub detector_name: DetectorName,
}

/// A scale transform derived from $PnE/$PnG.
#[derive(Clone, Copy, PartialEq, Debug, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum ScaleTransform {
    /// A linear transform ($PnE=0,0 and $PnG=1.0 or is null)
    #[display("Lin({_0})")]
    Lin(PositiveFloat),
    /// A log transform ($PnE!=0,0 and $PnG!=1.0 or is null)
    #[display("Log({_0})")]
    Log(LogScale),
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
    pub bin: Option<PeakBin>,

    /// Value of $PkNn
    #[as_ref(Option<PeakIndex>)]
    #[as_mut(Option<PeakIndex>)]
    #[new(into)]
    pub size: Option<PeakIndex>,
}

/// A bundle for $CSMODE, $CSVBITS, and $CSVnFLAG (3.0, 3.1)
///
/// These describe what is sometimes present in the ANALYSIS segment for 3.0 and
/// 3.1. In these versions, it was similar to TEXT which had key/value pairs. In
/// 3.2, these keywords were removed and the ANALYSIS segment became a free-form
/// bytestring. This library currently makes no attempt to interpret the
/// ANALYSIS segment given the CS* keywords, but may add this in the future if
/// the need arises.
#[derive(Clone, PartialEq, Default, AsRef, AsMut, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct SubsetData {
    /// Value of $CSBITS if given
    #[as_ref(CSVBits)]
    #[as_mut(CSVBits)]
    #[new(into)]
    pub bits: CSVBits,

    /// Value of $CSTOT if given
    #[as_ref(CSTot)]
    #[as_mut(CSTot)]
    #[new(into)]
    pub tot: CSTot,

    #[as_ref(CSVFlags)]
    #[as_mut(CSVFlags)]
    #[new(into)]
    pub flags: CSVFlags,
}

/// Values of $CSVnFLAG if given, with length equal to $CSMODE
#[derive(Clone, PartialEq, From, Default, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
pub struct CSVFlags(pub Vec<Option<CSVFlag>>);

/// A bundle for $ORIGINALITY, $LAST_MODIFIER, and $LAST_MODIFIED (3.1+)
#[derive(Clone, Default, AsRef, AsMut, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct ModificationData {
    #[as_ref(LastModifier)]
    #[as_mut(LastModifier)]
    #[new(into)]
    pub last_modifier: LastModifier,

    #[as_ref(Option<LastModified>)]
    #[as_mut(Option<LastModified>)]
    #[new(into)]
    pub last_modified: Option<LastModified>,

    #[as_ref(Option<Originality>)]
    #[as_mut(Option<Originality>)]
    #[new(into)]
    pub originality: Option<Originality>,
}

/// A bundle for $PLATEID, $PLATENAME, and $WELLID (3.1+)
#[derive(Clone, Default, AsRef, AsMut, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct PlateData {
    #[as_ref(Plateid)]
    #[as_mut(Plateid)]
    #[new(into)]
    pub plateid: Plateid,

    #[as_ref(Platename)]
    #[as_mut(Platename)]
    #[new(into)]
    pub platename: Platename,

    #[as_ref(Wellid)]
    #[as_mut(Wellid)]
    #[new(into)]
    pub wellid: Wellid,
}

/// A bundle for $UNSTAINEDCENTERS and $UNSTAINEDINFO (3.2+)
#[derive(Clone, Default, AsRef, AsMut, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct UnstainedData {
    #[as_ref(UnstainedCenters)]
    #[new(into)]
    // NOTE not mutable to prevent mutation when part of Core
    pub unstainedcenters: UnstainedCenters,

    #[as_ref(UnstainedInfo)]
    #[as_mut(UnstainedInfo)]
    #[new(into)]
    pub unstainedinfo: UnstainedInfo,
}

/// A bundle for $CARRIERID, $CARRIERTYPE, $LOCATIONID (3.2+)
#[derive(Clone, Default, AsRef, AsMut, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct CarrierData {
    #[as_ref(Carrierid)]
    #[as_mut(Carrierid)]
    #[new(into)]
    pub carrierid: Carrierid,

    #[as_ref(Carriertype)]
    #[as_mut(Carriertype)]
    #[new(into)]
    pub carriertype: Carriertype,

    #[as_ref(Locationid)]
    #[as_mut(Locationid)]
    #[new(into)]
    pub locationid: Locationid,
}

pub type Temporal2_0 = Temporal<InnerTemporal2_0>;
pub type Temporal3_0 = Temporal<InnerTemporal3_0>;
pub type Temporal3_1 = Temporal<InnerTemporal3_1>;
pub type Temporal3_2 = Temporal<InnerTemporal3_2>;

pub type Optical2_0 = Optical<InnerOptical2_0>;
pub type Optical3_0 = Optical<InnerOptical3_0>;
pub type Optical3_1 = Optical<InnerOptical3_1>;
pub type Optical3_2 = Optical<InnerOptical3_2>;

pub type Measurements2_0 = Measurements<Option<Shortname>, InnerTemporal2_0, InnerOptical2_0>;
pub type Measurements3_0 = Measurements<Option<Shortname>, InnerTemporal3_0, InnerOptical3_0>;
pub type Measurements3_1 = Measurements<Identity<Shortname>, InnerTemporal3_1, InnerOptical3_1>;
pub type Measurements3_2 = Measurements<Identity<Shortname>, InnerTemporal3_2, InnerOptical3_2>;

pub type Metaroot2_0 = Metaroot<InnerMetaroot2_0>;
pub type Metaroot3_0 = Metaroot<InnerMetaroot3_0>;
pub type Metaroot3_1 = Metaroot<InnerMetaroot3_1>;
pub type Metaroot3_2 = Metaroot<InnerMetaroot3_2>;

/// A standardized TEXT segment
pub type CoreTEXT<M, T, P, N, L> = Core<(), (), (), M, T, P, N, L>;

/// A standardized FCS dataset (TEXT+DATA+ANALYSIS+OTHER)
pub type CoreDataset<M, T, P, N, L> = Core<Analysis, FCSDataFrame, Others, M, T, P, N, L>;

pub type Core2_0<A, D, O> = Core<
    A,
    D,
    O,
    InnerMetaroot2_0,
    InnerTemporal2_0,
    InnerOptical2_0,
    Option<Shortname>,
    DataLayout2_0,
>;
pub type Core3_0<A, D, O> = Core<
    A,
    D,
    O,
    InnerMetaroot3_0,
    InnerTemporal3_0,
    InnerOptical3_0,
    Option<Shortname>,
    DataLayout3_0,
>;
pub type Core3_1<A, D, O> = Core<
    A,
    D,
    O,
    InnerMetaroot3_1,
    InnerTemporal3_1,
    InnerOptical3_1,
    Identity<Shortname>,
    DataLayout3_1,
>;
pub type Core3_2<A, D, O> = Core<
    A,
    D,
    O,
    InnerMetaroot3_2,
    InnerTemporal3_2,
    InnerOptical3_2,
    Identity<Shortname>,
    DataLayout3_2,
>;

pub type CoreTEXT2_0 = Core2_0<(), (), ()>;
pub type CoreTEXT3_0 = Core3_0<(), (), ()>;
pub type CoreTEXT3_1 = Core3_1<(), (), ()>;
pub type CoreTEXT3_2 = Core3_2<(), (), ()>;

pub type CoreDataset2_0 = Core2_0<Analysis, FCSDataFrame, Others>;
pub type CoreDataset3_0 = Core3_0<Analysis, FCSDataFrame, Others>;
pub type CoreDataset3_1 = Core3_1<Analysis, FCSDataFrame, Others>;
pub type CoreDataset3_2 = Core3_2<Analysis, FCSDataFrame, Others>;

/// Reader for ANALYSIS segment
#[derive(new)]
pub struct AnalysisReader {
    pub seg: AnyAnalysisSegment,
}

/// Reader for OTHER segments
#[derive(new)]
pub struct OthersReader<'a> {
    pub segs: &'a [OtherSegment20],
}

/// Output of using keywords to read standardized TEXT+DATA
#[derive(Clone, new, PartialEq)]
pub struct StdDatasetWithKwsOutput {
    /// DATA+ANALYSIS
    pub dataset_segments: DatasetSegments,

    /// Keywords that start with '$' that are not part of the standard
    pub std_diagnostics: StdTEXTDiagnostics,

    /// Diagnostic output from parsing DATA segment
    pub events_diagnostics: EventsDiagnostics,
}

/// Standardized TEXT+DATA+ANALYSIS with DATA+ANALYSIS offsets
#[derive(Clone, Copy, new, PartialEq)]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct DatasetSegments {
    /// offsets used to parse DATA
    pub data: AnyDataSegment,

    /// offsets used to parse ANALYSIS
    pub analysis: AnyAnalysisSegment,

    /// Uncorrected offsets for DATA if from TEXT
    pub data_uncorrected: Option<UncorrectedSegment>,

    /// Uncorrected offsets for ANALYSIS if from TEXT
    pub analysis_uncorrected: Option<UncorrectedSegment>,
}

/// Internal configuration options used when writing HEADER+TEXT
struct WriteHeaderAndTextConfig<'a> {
    delim: TEXTDelim,
    tot: Tot,
    data_len: u64,
    analysis_len: u64,
    other_segs: &'a [Other],
    has_nextdata: AppendableFlag,
}

/// Used for [`Core::standard_keywords`] to control if req/opt keys should be returned
pub enum IncludeReqOrOpt {
    Req_,
    Opt_,
    Both,
}

/// Used for [`Core::standard_keywords`] to control if root/meas keys should be returned
pub enum IncludeRootOrMeas {
    Root,
    Meas,
    Both,
}

/// Diagnostic output from standardizing TEXT
#[derive(Clone, PartialEq, new)]
#[allow(clippy::too_many_arguments)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct StdTEXTDiagnostics {
    /// Keys which start with `"$"` but are not part of the standard.
    pub pseudostandard: StdKeywords,
    /// Standard $Pn* keys where `n` is higher than $PAR
    pub hyper_par: StdKeywords,
    /// Standard $Gn* keys where `n` is higher than $GATE
    pub hyper_gate: StdKeywords,
    /// Keys which do not belong in this version but are valid in another.
    pub other_version: StdKeywords,
    /// $TIMESTEP if it is given but not used.
    pub timestep: Option<String>,
    /// Original $PnN if they are renamed.
    pub original_names: Vec<Option<Shortname>>,
    /// Diagnostic outcomes from fixing $PnE keys.
    pub scale: Vec<AnyScaleDiagnostic>,
    /// Original keyword values that were trimmed for whitespace between commas.
    pub trimmed: Vec<(StdKey, String)>,
    /// Optical keys that were found in the temporal measurement.
    pub temporal_optical_pairs: Vec<(StdKey, String)>,
}

impl StdTEXTDiagnostics {
    fn from_extra(
        extra: ExtraStdKeywords,
        original_names: Vec<Option<Shortname>>,
        scale: Vec<AnyScaleDiagnostic>,
        trimmed: Vec<(StdKey, String)>,
        temporal_optical_pairs: Vec<(StdKey, String)>,
    ) -> Self {
        Self {
            pseudostandard: extra.pseudostandard,
            hyper_par: extra.hyper_par,
            hyper_gate: extra.hyper_gate,
            other_version: extra.other_version,
            timestep: extra.timestep,
            original_names,
            scale,
            trimmed,
            temporal_optical_pairs,
        }
    }
}

#[derive(new)]
pub struct DiagnosedMetaroot<M> {
    this: M,
    trimmed: Vec<(StdKey, String)>,
}

#[derive(new)]
pub struct DiagnosedOptical<M> {
    this: M,
    scale: ScaleDiagnostic,
    trimmed: Vec<(StdKey, String)>,
}

#[derive(new)]
pub struct DiagnosedTemporal<M> {
    this: M,
    scale: TemporalScaleDiagnostic,
    trimmed: Vec<(StdKey, String)>,
    tmp_opt_pairs: Vec<(StdKey, String)>,
}

#[derive(new)]
struct DiagnosedUnstainedData {
    this: UnstainedData,
    trimmed: Option<(StdKey, String)>,
}

impl WriteHeaderAndTextConfig<'_> {
    fn new_nodata(delim: TEXTDelim, has_nextdata: AppendableFlag) -> Self {
        Self {
            delim,
            tot: Tot(0),
            data_len: 0,
            analysis_len: 0,
            other_segs: &[],
            has_nextdata,
        }
    }

    fn other_lens(&self) -> Vec<u64> {
        self.other_segs
            .iter()
            .map(|s| u64::try_from(s.0.len()).expect("OTHER segment length exceeds 2^64"))
            .collect()
    }
}

mod private {
    pub struct NoTouchy;
}

pub trait HasCompensation: AsRef<Option<Self::Comp>> {
    type Comp: From<Compensation> + AsRef<Compensation>;

    // set wrapped inner type with common outer type (Compensation)
    fn set_comp(&mut self, comp: Option<Compensation>, _: private::NoTouchy) {
        *self.comp_mut(private::NoTouchy) = comp.map(Into::into);
    }

    // almost like as_ref, except the reference needs to go on the inside since
    // the newtype wrapper needs to be removed
    fn comp(&self, _: private::NoTouchy) -> Option<&Compensation> {
        self.as_ref().as_ref().map(AsRef::as_ref)
    }

    // private as_mut
    fn comp_mut(&mut self, _: private::NoTouchy) -> &mut Option<Self::Comp>;
}

pub trait HasSpillover {
    // private as_mut
    fn spill_mut(&mut self, _: private::NoTouchy) -> &mut Option<Spillover>;
}

pub trait HasScale {
    // private as_mut
    fn scale_mut(&mut self, _: private::NoTouchy) -> &mut Option<Scale>;
}

pub trait HasScaleTransform {
    fn transform_mut(&mut self, _: private::NoTouchy) -> &mut ScaleTransform;
}

pub trait HasUnstainedCenters {
    // private as_mut
    fn unstainedcenters_mut(&mut self, _: private::NoTouchy) -> &mut UnstainedCenters;
}

pub trait HasAppliedGates3_0 {
    // private as_mut
    fn applied_gates3_0_mut(&mut self, _: private::NoTouchy) -> &mut AppliedGates3_0;
}

pub trait HasAppliedGates3_2 {
    // private as_mut
    fn applied_gates3_2_mut(&mut self, _: private::NoTouchy) -> &mut AppliedGates3_2;
}

pub trait AsScaleTransform {
    fn as_transform(&self) -> ScaleTransform;
}

pub trait Versioned {
    type Layout: VersionedDataLayout;
    type Offsets: VersionedTEXTOffsets<TotDef = <Self::Layout as VersionedDataLayout>::Tot>;

    fn fcs_version() -> Version;
}

pub(crate) trait PrivVersioned: Versioned {
    fn h_lookup_and_read<C, R>(
        h: &mut BufReader<R>,
        kws: &StdKeywords,
        segs: &NonDataSegments,
        st: &ReadState<C>,
    ) -> WarningsAndIOGroupResult<
        (FCSDataFrame, Analysis, DatasetSegments, EventsDiagnostics),
        LookupAndReadDataAnalysisWarning,
        LookupAndReadDataAnalysisError,
        (),
    >
    where
        R: Read + Seek,
        Self::Offsets: AsRef<DatasetSegments>,
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadEventsConfig> + AsRef<ReadOffsetConfig>,
    {
        let layout_res = Par::get_metaroot_req(kws)
            .map_err(LookupAndReadDataAnalysisError::from)
            .into_log()
            .and_then_commutative(|par| {
                Self::Layout::lookup_ro(kws, par, st.conf.as_ref())
                    .map_commutative_warnings(LookupAndReadDataAnalysisWarning::from)
                    .map_errors(LookupAndReadDataAnalysisError::from)
            });
        let offset_res = Self::Offsets::lookup_ro(kws, segs, st)
            .map_commutative_warnings(LookupAndReadDataAnalysisWarning::from)
            .map_errors(LookupAndReadDataAnalysisError::from);
        layout_res
            .zip_commutative(offset_res)
            .group()
            .map_error(IOErrorGroup::Pure)
            .and_then_commutative(|(layout_out, offsets)| {
                let dataset_segs = offsets.as_ref();
                let ar = AnalysisReader::new(dataset_segs.analysis);
                let read_conf: &ReadEventsConfig = st.conf.as_ref();
                layout_out
                    .layout
                    .h_read_df(h, offsets.tot(), dataset_segs.data, read_conf)
                    .map_commutative_warnings(LookupAndReadDataAnalysisWarning::from)
                    .map_pure_errors(LookupAndReadDataAnalysisError::from)
                    .and_then_commutative(|(df, event_out)| {
                        ar.h_read(h)
                            .map(|a| (df, a, *dataset_segs, event_out))
                            .map_err(IOErrorGroup::from)
                            .into_log()
                    })
            })
    }
}

pub trait LookupMetaroot: Sized + VersionedMetaroot {
    fn lookup_shortname(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        i: MeasIndex,
        conf: &ReadDataKeywordsConfig,
    ) -> LookupShortnameResult<Self::Name>;

    fn lookup_specific<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        ms: &[Self::Name],
        conf: &C,
    ) -> LookupMetarootResult<(Self, Vec<(StdKey, String)>)>
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadStdKeywordsConfig>;
}

pub trait LookupOptical: Sized + VersionedOptical {
    fn lookup_specific<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        i: MeasIndex,
        conf: &C,
    ) -> LookupOpticalResult<DiagnosedOptical<Self>>
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadStdKeywordsConfig>;
}

pub trait LookupTemporal: VersionedTemporal {
    fn lookup_specific<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        i: MeasIndex,
        conf: &C,
    ) -> LookupTemporalResult<DiagnosedTemporal<Self>>
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadStdKeywordsConfig>;
}

pub trait ConvertFromShortname<T>: Sized + MightHave<Shortname> {
    fn convert_from_shortname(value: T, i: MeasIndex) -> Result<Self, NameConversionError>;
}

pub trait ConvertFromMetaroot<M>: Sized + VersionedMetaroot {
    fn convert_from_metaroot(value: M, flag: AllowLoss) -> MetarootConvertResult<Self>;
}

pub trait ConvertFromOptical<O>: Sized + VersionedOptical {
    fn convert_from_optical(value: O, i: MeasIndex, flag: AllowLoss) -> OpticalConvertResult<Self>;
}

pub trait ConvertFromTemporal<T>: Sized + VersionedTemporal {
    fn convert_from_temporal(
        value: T,
        i: MeasIndex,
        flag: AllowLoss,
    ) -> TemporalConvertResult<Self>;
}

pub trait VersionedMetaroot: Sized {
    type Ver: Versioned;
    type Optical: VersionedOptical<Ver = Self::Ver>;
    type Temporal: VersionedTemporal<Ver = Self::Ver>;
    type Name: MightHave<Shortname>;

    /// Return error if any named links are broken
    fn meas_invalid_named_links_inner(
        &self,
        names: &NamedSet<'_>,
    ) -> impl Iterator<Item = BrokenNamedLinkError>;

    /// Return error if any indexed links are broken
    fn meas_invalid_indexed_links_inner(
        &self,
        par: &Par,
    ) -> impl Iterator<Item = BrokenIndexedLinkError>;

    /// Check that all links point to a valid name or index.
    ///
    /// If this is not the case, either drop invalid keywords or return error.
    fn remove_invalid_links(
        &mut self,
        par: &Par,
        names: &NamedSet<'_>,
    ) -> impl Iterator<Item = RemovedLink>;

    fn deprecated(&mut self, _: private::NoTouchy) -> impl Iterator<Item = DeprecatedRef<'_>>;

    /// Return error if any data in this struct links to given list of names.
    fn meas_has_existing_named_links_with_inner(
        &self,
        names: &OpticalNamesToRemove<'_>,
    ) -> impl Iterator<Item = AnyExistingNamedLinkError>;

    /// Return error if any data in struct has index links.
    fn meas_has_existing_index_links_with_inner(
        &self,
        par: Par,
        indices: &IndicesToRemove,
    ) -> impl Iterator<Item = AnyExistingIndexLinkError>;

    /// Rename any measurement references in keywords.
    fn rename_meas_links_inner(&mut self, mapping: &NameMapping);

    /// Update linked indices in keywords after inserting a new measurement.
    ///
    /// Everything after `index` must be incremented by 1.
    fn insert_meas_index_inner(&mut self, i: MeasIndex);

    fn keywords_req_inner(&self) -> impl Iterator<Item = (String, String)>;

    fn keywords_opt_inner(&self) -> impl Iterator<Item = (String, String)>;

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
        old: (MeasIndex, Temporal<Self::Temporal>),
        new: (MeasIndex, Optical<Self::Optical>),
        flag: AllowLoss,
    ) -> SwitchableErrorResult<
        (Optical<Self::Optical>, Temporal<Self::Temporal>),
        (Temporal<Self::Temporal>, Optical<Self::Optical>),
        AllowLoss,
        SwapOpticalTemporalErrors,
    > {
        let go = |old_t: Temporal<Self::Temporal>, old_o: Optical<Self::Optical>| {
            let (so, st) = Self::swap_optical_temporal_inner(old_t.specific, old_o.specific);
            let f = Filter::default();
            let d = DetectorType::default();
            let new_o = Optical::new(old_t.common, f, None, d, None, None, so);
            let new_t = Temporal::new(old_o.common, st);
            (new_o, new_t)
        };

        let (tmp_index, tmp) = old;
        let (opt_index, opt) = new;

        let scale_err = opt
            .specific
            .nonlinear_scale_error(opt_index)
            .map(SwapOpticalTemporalError::from);
        let t_to_o_err = tmp
            .specific
            .temporal_to_optical_error(tmp_index)
            .map(SwapOpticalTemporalError::from);
        let o_to_t_specific_errs = opt.specific.optical_to_temporal_loss_errors(opt_index);
        let o_to_t_common_errs = opt.loss_errors(opt_index);

        let es = o_to_t_specific_errs
            .chain(o_to_t_common_errs)
            .map(SwapOpticalTemporalError::from)
            .chain(scale_err)
            .chain(t_to_o_err);

        let s = SwapOpticalTemporalSummary::new(opt_index, tmp_index);

        ErrorGroup::try_new_with(s, es)
            .into_deferred_switchable3(flag)
            .set_deferred_value((tmp, opt))
            .map_ok_value(|(t, o)| go(t, o))
    }

    fn swap_optical_temporal_inner(
        t: Self::Temporal,
        o: Self::Optical,
    ) -> (Self::Optical, Self::Temporal);
}

pub trait DeprecatedMetaroot: Sized {}

pub trait VersionedOptical: Sized {
    type Ver: Versioned;

    fn req_suffixes_inner(
        &self,
        i: MeasIndex,
    ) -> impl Iterator<Item = (MeasHeader, String, String)>;

    fn opt_suffixes_inner(
        &self,
        i: MeasIndex,
    ) -> impl Iterator<Item = (MeasHeader, String, Option<String>)>;

    fn nonlinear_scale_error(&self, i: MeasIndex) -> Option<OpticalNonLinearError>;

    fn optical_to_temporal_loss_errors(
        &self,
        i: MeasIndex,
    ) -> impl Iterator<Item = AnyOpticalToTemporalKeyLossError>;

    fn deprecated(&mut self, i: MeasIndex) -> impl Iterator<Item = DeprecatedRef<'_>>;
}

pub trait VersionedTemporal: Sized {
    type Ver: Versioned;
    type Warning;
    type Error;

    fn req_meta_keywords_inner(&self) -> impl Iterator<Item = (String, String)>;

    fn req_meas_keywords_inner(&self, i: MeasIndex) -> impl Iterator<Item = (String, String)>;

    fn opt_meas_keywords_inner(&self, i: MeasIndex) -> impl Iterator<Item = (String, String)>;

    fn can_convert_to_optical(&self, i: MeasIndex) -> Result<(), Self::Error>;

    fn temporal_to_optical_error(&self, i: MeasIndex) -> Option<AnyTemporalToOpticalKeyLossError>;

    fn deprecated(&mut self, i: MeasIndex) -> impl Iterator<Item = DeprecatedRef<'_>>;
}

pub trait TemporalFromOptical<O: VersionedOptical>: Sized {
    type TData;

    fn from_optical(
        opt: Optical<O>,
        i: MeasIndex,
        data: Self::TData,
        allow_loss: AllowLoss,
    ) -> SwitchableErrorResult<Temporal<Self>, Optical<O>, AllowLoss, OpticalToTemporalErrors> {
        let opt_common_errs = opt.loss_errors(i);
        let opt_specific_errs = opt.specific.optical_to_temporal_loss_errors(i);
        let scale_err = opt
            .specific
            .nonlinear_scale_error(i)
            .map(OpticalToTemporalError::from);
        let es = opt_common_errs
            .chain(opt_specific_errs)
            .map(OpticalToTemporalError::from)
            .chain(scale_err);

        let s = OpticalToTemporalSummary::new(i);
        ErrorGroup::try_new_with(s, es)
            .into_deferred_switchable3::<_, Nothing<_>>(allow_loss)
            .set_deferred_value((opt, data))
            .map_ok_value(|(o, d)| Self::from_optical_unchecked(o, d))
            .map_err_value(|(o, _)| o)
    }

    fn from_optical_unchecked(o: Optical<O>, d: Self::TData) -> Temporal<Self> {
        Temporal::new(o.common, Self::from_optical_inner(o.specific, d))
    }

    fn from_optical_inner(o: O, d: Self::TData) -> Self;
}

pub trait OpticalFromTemporal<T: VersionedTemporal>: Sized {
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

pub trait VersionedTEXTOffsets: Sized {
    type TotDef: IsTot;

    fn lookup<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        segs: &NonDataSegments,
        st: &ReadState<C>,
    ) -> LookupTEXTOffsetsResult<Self>
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadOffsetConfig>;

    fn lookup_ro<C>(
        std: &StdKeywords,
        segs: &NonDataSegments,
        st: &ReadState<C>,
    ) -> LookupTEXTOffsetsResult<Self>
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadOffsetConfig>;

    fn tot(&self) -> Self::TotDef;

    fn into_common(self) -> TEXTOffsets<Option<Tot>>;
}

/// Segment offsets and $TOT as read from TEXT segment
///
/// This is used later to parse DATA and ANALYSIS.
#[derive(AsRef, new)]
pub struct TEXTOffsets<T> {
    #[as_ref]
    pub segs: DatasetSegments,
    pub tot: T,
}

#[derive(From, AsRef)]
#[as_ref(DatasetSegments)]
pub struct TEXTOffsets2_0(pub TEXTOffsets<Option<Tot>>);

#[derive(From, AsRef)]
#[as_ref(DatasetSegments)]
pub struct TEXTOffsets3_0(pub TEXTOffsets<Tot>);

#[derive(From, AsRef)]
#[as_ref(DatasetSegments)]
pub struct TEXTOffsets3_2(pub TEXTOffsets<Tot>);

impl CommonMeasurement {
    fn lookup(std: &mut StdKeywords, nonstd: NonStdKeywords, i: MeasIndex) -> Self {
        let longname = Longname::remove_meas_opt_nofail(std, i);
        Self::new(longname, nonstd)
    }
}

impl<T> Temporal<T> {
    fn lookup_temporal<C>(
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
            )
        })
    }

    fn convert<ToT>(self, i: MeasIndex, flag: AllowLoss) -> TemporalConvertResult<Temporal<ToT>>
    where
        ToT: ConvertFromTemporal<T>,
    {
        ToT::convert_from_temporal(self.specific, i, flag)
            .map_deferred_value(|specific| Temporal::new(self.common, specific))
    }

    fn req_meas_keywords(&self, i: MeasIndex) -> impl Iterator<Item = (String, String)>
    where
        T: VersionedTemporal,
    {
        self.specific.req_meas_keywords_inner(i)
    }

    fn req_meta_keywords(&self) -> impl Iterator<Item = (String, String)>
    where
        T: VersionedTemporal,
    {
        self.specific.req_meta_keywords_inner()
    }

    fn opt_meas_keywords(&self, i: MeasIndex) -> impl Iterator<Item = (String, String)>
    where
        T: VersionedTemporal,
    {
        once(self.common.longname.meas_opt_pair(i))
            .filter_map(|(k, v)| v.map(|x| (k, x)))
            .chain(self.specific.opt_meas_keywords_inner(i))
    }

    fn all_opt_keywords(&self, n: MeasIndex) -> impl Iterator<Item = (String, String)>
    where
        T: VersionedTemporal,
    {
        self.opt_meas_keywords(n).chain(
            self.common
                .nonstandard_keywords
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone())),
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

    fn try_convert<Of: ConvertFromOptical<O>>(
        self,
        i: MeasIndex,
        flag: AllowLoss,
    ) -> OpticalConvertResult<Optical<Of>> {
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

    fn lookup_optical<C>(
        std: &mut StdKeywords,
        i: MeasIndex,
        mut nonstd: NonStdKeywords,
        conf: &C,
    ) -> LookupOpticalResult<DiagnosedOptical<Self>>
    where
        O: LookupOptical,
        Version: From<O::Ver>,
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
                DiagnosedOptical::new(ret, s_out.scale, s_out.trimmed)
            })
    }

    fn req_keywords(&self, i: MeasIndex) -> impl Iterator<Item = (MeasHeader, String, String)>
    where
        O: VersionedOptical,
    {
        self.specific.req_suffixes_inner(i)
    }

    fn opt_keywords(
        &self,
        i: MeasIndex,
    ) -> impl Iterator<Item = (MeasHeader, String, Option<String>)>
    where
        O: VersionedOptical,
    {
        [
            self.common.longname.meas_opt_triple(i),
            self.filter.meas_opt_triple(i),
            self.power.meas_opt_triple(i),
            self.detector_type.meas_opt_triple(i),
            self.percent_emitted.meas_opt_triple(i),
            self.detector_voltage.meas_opt_triple(i),
        ]
        .into_iter()
        .chain(self.specific.opt_suffixes_inner(i))
    }

    #[cfg(feature = "serde")]
    fn table_pairs(&self) -> impl Iterator<Item = (MeasHeader, Option<String>)>
    where
        O: VersionedOptical,
    {
        // zero is a dummy and not meaningful here
        let n = 0.into();
        self.req_keywords(n)
            .map(|(t, _, v)| (t, Some(v)))
            .chain(self.opt_keywords(n).map(|(k, _, v)| (k, v)))
    }

    #[cfg(feature = "serde")]
    fn table_header(&self, opt_layout: Vec<MeasHeader>) -> Vec<String>
    where
        O: VersionedOptical,
    {
        let req_layout = req_meas_headers();
        [MeasHeader("index".into()), Shortname::std_blank()]
            .into_iter()
            .chain(req_layout)
            .chain(self.table_pairs().map(|(k, _)| k))
            .chain(opt_layout)
            .map(|x| x.0)
            .collect()
    }

    #[cfg(feature = "serde")]
    fn table_row(
        &self,
        i: MeasIndex,
        n: Option<&Shortname>,
        req_layout: [String; 2],
        opt_layout: Vec<Option<String>>,
    ) -> Vec<String>
    where
        O: VersionedOptical,
    {
        let na = || "NA".into();
        [i.to_string(), n.map_or(na(), ToString::to_string)]
            .into_iter()
            .chain(req_layout)
            .chain(
                self.table_pairs()
                    .map(|(_, v)| v)
                    .map(|v| v.unwrap_or(na())),
            )
            .chain(opt_layout.into_iter().map(|x| x.unwrap_or(na())))
            .collect()
    }

    fn all_req_keywords(&self, n: MeasIndex) -> impl Iterator<Item = (String, String)>
    where
        O: VersionedOptical,
    {
        self.req_keywords(n).map(|(_, k, v)| (k, v))
    }

    fn all_opt_keywords(&self, n: MeasIndex) -> impl Iterator<Item = (String, String)>
    where
        O: VersionedOptical,
    {
        self.opt_keywords(n)
            .filter_map(|(_, k, v)| v.map(|x| (k, x)))
            .chain(
                self.common
                    .nonstandard_keywords
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.clone())),
            )
    }

    pub(crate) fn as_transform(&self) -> ScaleTransform
    where
        O: AsScaleTransform,
    {
        self.specific.as_transform()
    }

    fn loss_errors(&self, i: MeasIndex) -> impl Iterator<Item = AnyOpticalToTemporalKeyLossError> {
        let filter = self.filter.indexed_key_loss_error(i);
        let power = self.power.indexed_key_loss_error(i);
        let det_type = self.detector_type.indexed_key_loss_error(i);
        let per_emit = self.percent_emitted.indexed_key_loss_error(i);
        let det_volt = self.detector_voltage.indexed_key_loss_error(i);
        [filter, power, det_type, per_emit, det_volt]
            .into_iter()
            .flatten()
    }

    fn deprecated(
        &mut self,
        i: MeasIndex,
        es: &mut Vec<AnyDepKeyError>,
        keep: bool,
        do_demote: bool,
    ) where
        O: VersionedOptical,
        Version: From<O::Ver>,
    {
        let v = O::Ver::fcs_version();
        let p = (v >= Version::FCS3_2).then(|| {
            DeprecatedRef::PercentEmitted(IndexedDepRef::new(i.into(), &mut self.percent_emitted))
        });
        for mut d in self.specific.deprecated(i).chain(p) {
            if do_demote {
                d.demote(&mut self.common.nonstandard_keywords, keep);
            }
            d.errors(es);
        }
    }
}

impl<M, V: Versioned> Metaroot<M>
where
    M: VersionedMetaroot<Ver = V>,
{
    fn try_convert<ToM: ConvertFromMetaroot<M>>(
        self,
        flag: AllowLoss,
    ) -> MetarootConvertResult<Metaroot<ToM>> {
        ToM::convert_from_metaroot(self.specific, flag).map_ok_value(|specific| {
            Metaroot::new(
                self.abrt,
                self.com,
                self.cells,
                self.exp,
                self.fil,
                self.inst,
                self.lost,
                self.op,
                self.proj,
                self.smno,
                self.src,
                self.sys,
                self.tr,
                specific,
                self.nonstandard_keywords,
            )
        })
    }

    fn lookup_metaroot<C>(
        std: &mut StdKeywords,
        ms: &[M::Name],
        mut nonstd: NonStdKeywords,
        conf: &C,
    ) -> LookupMetarootResult<DiagnosedMetaroot<Self>>
    where
        M: LookupMetaroot,
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadStdKeywordsConfig>,
    {
        macro_rules! go {
            ($x:expr) => {
                $x.map_switchable_errors(LookupMetarootWarning::from)
                    .switchable_into_commutative()
                    .map_errors(LookupMetarootError::from)
                    .into_semigroup()
            };
        }
        let com = Com::remove_root_opt_nofail(std);
        let cells = Cells::remove_root_opt_nofail(std);
        let exp = Exp::remove_root_opt_nofail(std);
        let fil = Fil::remove_root_opt_nofail(std);
        let inst = Inst::remove_root_opt_nofail(std);
        let op = Op::remove_root_opt_nofail(std);
        let proj = Proj::remove_root_opt_nofail(std);
        let smno = Smno::remove_root_opt_nofail(std);
        let src = Src::remove_root_opt_nofail(std);
        let sys = Sys::remove_root_opt_nofail(std);

        let abrt_res = Abrt::remove_or_drop_root_opt(std, &mut nonstd, conf.as_ref());
        let lost_res = Lost::remove_or_drop_root_opt(std, &mut nonstd, conf.as_ref());
        let tr_res = Trigger::remove_or_drop_root_opt_with(std, &mut nonstd, (), conf);

        let spec_res = M::lookup_specific(std, &mut nonstd, ms, conf);

        go!(abrt_res)
            .zip4_commutative(go!(lost_res), go!(tr_res), spec_res)
            .map_ok_value(|(abrt, lost, tr_out, (specific, trimmed))| {
                let (tr, tr_trimmed) = tr_out.into_opt_root_pair();
                let ret = Self::new(
                    abrt, com, cells, exp, fil, inst, lost, op, proj, smno, src, sys, tr, specific,
                    nonstd,
                );
                let trimmed2 = trimmed.into_iter().chain(tr_trimmed).collect();
                DiagnosedMetaroot::new(ret, trimmed2)
            })
    }

    fn all_req_keywords(&self, par: Par) -> impl Iterator<Item = (String, String)> {
        once(par.pair()).chain(self.specific.keywords_req_inner())
    }

    fn all_opt_keywords(&self) -> impl Iterator<Item = (String, String)> {
        [
            self.abrt.metaroot_opt_pair(),
            self.com.metaroot_opt_pair(),
            self.cells.metaroot_opt_pair(),
            self.exp.metaroot_opt_pair(),
            self.fil.metaroot_opt_pair(),
            self.inst.metaroot_opt_pair(),
            self.lost.metaroot_opt_pair(),
            self.op.metaroot_opt_pair(),
            self.proj.metaroot_opt_pair(),
            self.smno.metaroot_opt_pair(),
            self.src.metaroot_opt_pair(),
            self.sys.metaroot_opt_pair(),
            self.tr.metaroot_opt_pair(),
        ]
        .into_iter()
        .filter_map(|(k, v)| v.map(|x| (k, x)))
        .chain(self.specific.keywords_opt_inner())
        .chain(
            self.nonstandard_keywords
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone())),
        )
    }

    fn rename_trigger_meas_link(&mut self, mapping: &NameMapping) {
        if let Some(tr) = self.tr.as_mut() {
            tr.reassign(mapping);
        }
    }

    fn rename_meas_links(&mut self, mapping: &NameMapping) {
        self.rename_trigger_meas_link(mapping);
        self.specific.rename_meas_links_inner(mapping);
    }

    fn meas_has_existing_named_links_with(
        &self,
        names: &OpticalNamesToRemove<'_>,
    ) -> impl Iterator<Item = AnyExistingNamedLinkError> {
        let tr = self
            .tr
            .as_ref()
            .and_then(|tr| tr.existing_link_error(names))
            .map(AnyExistingNamedLinkError::Trigger);
        self.specific
            .meas_has_existing_named_links_with_inner(names)
            .chain(tr)
    }

    fn meas_has_existing_links_with(
        &self,
        par: Par,
        names: &OpticalNamesToRemove<'_>,
        indices: &IndicesToRemove,
    ) -> impl Iterator<Item = ExistingLinkError> {
        let es = self
            .meas_has_existing_named_links_with(names)
            .map(ExistingLinkError::from);
        self.specific
            .meas_has_existing_index_links_with_inner(par, indices)
            .map(ExistingLinkError::from)
            .chain(es)
    }

    fn invalid_named_links(
        &self,
        names: &NamedSet<'_>,
    ) -> impl Iterator<Item = BrokenNamedLinkError> {
        let tr = self
            .tr
            .as_ref()
            .and_then(|tr| tr.invalid_link_error(names))
            .map(BrokenNamedLinkError::from);
        self.specific
            .meas_invalid_named_links_inner(names)
            .chain(tr)
    }

    // Return a vector of errors here to let the caller decide how to package
    // them. This allows the caller to hardcode the drop flag which allows for
    // a simpler result type.
    fn remove_invalid_links(
        &mut self,
        par: Par,
        names: &NamedSet<'_>,
        allow_dropping: bool,
    ) -> Vec<BrokenOrDependentLinkError> {
        let tr = Trigger::remove_invalid_links(&mut self.tr, names);
        let mut es = vec![];
        for x in self
            .specific
            .remove_invalid_links(&par, names)
            .chain(tr.map(RemovedLink::from))
        {
            if allow_dropping {
                x.insert_keyvals(&mut self.nonstandard_keywords);
            }
            x.push_errors(&mut es);
        }
        es
    }
}

impl<M, T> From<Optical<M>> for Temporal<T>
where
    T: From<M>,
{
    fn from(value: Optical<M>) -> Self {
        Self::new(value.common, value.specific.into())
    }
}

impl<M, T> From<Temporal<T>> for Optical<M>
where
    M: From<T>,
{
    fn from(value: Temporal<T>) -> Self {
        Self::new(
            value.common,
            Filter::default(),
            None,
            DetectorType::default(),
            None,
            None,
            value.specific.into(),
        )
    }
}

pub(crate) type TemporalOrOptical<M> = Element<
    Temporal<<M as VersionedMetaroot>::Temporal>,
    Optical<<M as VersionedMetaroot>::Optical>,
>;

pub(crate) type TemporalsAndOpticals<M> = Vec<TemporalOrOptical<M>>;

pub(crate) type NamedTemporalOrOptical<M> = EitherPair<
    <M as VersionedMetaroot>::Name,
    Temporal<<M as VersionedMetaroot>::Temporal>,
    Optical<<M as VersionedMetaroot>::Optical>,
>;

pub(crate) type NamedTemporalsAndOpticals<M> = Eithers<
    <M as VersionedMetaroot>::Name,
    Temporal<<M as VersionedMetaroot>::Temporal>,
    Optical<<M as VersionedMetaroot>::Optical>,
>;

pub(crate) type TemporalsAndOpticals2_0 = NamedTemporalsAndOpticals<InnerMetaroot2_0>;
pub(crate) type TemporalsAndOpticals3_0 = NamedTemporalsAndOpticals<InnerMetaroot3_0>;
pub(crate) type TemporalsAndOpticals3_1 = NamedTemporalsAndOpticals<InnerMetaroot3_1>;
pub(crate) type TemporalsAndOpticals3_2 = NamedTemporalsAndOpticals<InnerMetaroot3_2>;

pub(crate) type Measurements<N, T, O> = NamedVec<N, Temporal<T>, Optical<O>>;

pub(crate) type VersionedCore<A, D, O, M> = Core<
    A,
    D,
    O,
    M,
    <M as VersionedMetaroot>::Temporal,
    <M as VersionedMetaroot>::Optical,
    <M as VersionedMetaroot>::Name,
    <<M as VersionedMetaroot>::Ver as Versioned>::Layout,
>;

pub(crate) type VersionedCoreTEXT<M> = VersionedCore<(), (), (), M>;

pub(crate) type VersionedCoreDataset<M> = VersionedCore<Analysis, FCSDataFrame, Others, M>;

impl<A, D, O, M, T, P, N, L> Core<A, D, O, M, T, P, N, L> {
    /// Return $PAR, which is simply the number of measurements in this struct
    pub fn par(&self) -> Par {
        Par(self.measurements.len())
    }
}

impl<M, A, D, O> VersionedCore<A, D, O, M>
where
    M: VersionedMetaroot,
{
    /// Show FCS version.
    pub fn fcs_version(&self) -> Version
    where
        Version: From<M::Ver>,
    {
        M::Ver::fcs_version()
    }

    pub fn write_texts(
        path: &PathBuf,
        cores: &[Self],
        conf: &WriteTEXTInnerConfig,
    ) -> Result<Option<Nextdata>, ImpureError<Uint8DigitOverflowError>>
    where
        Version: From<M::Ver>,
    {
        let n = cores.len();
        let mut nd = None;
        for (i, c) in cores.iter().enumerate() {
            let appendable = AppendableFlag::from(i + 1 < n);
            let append = AppendFlag(i > 0);
            let multi = WriteMultiConfig::new(appendable, append);
            let sconf = WriteMultiTEXTConfig::new(*conf, multi);
            nd = Some(c.write_text(path, &sconf)?);
        }
        Ok(nd)
    }

    /// Write this core structure (HEADER+TEXT) to path
    pub fn write_text(
        &self,
        path: &PathBuf,
        conf: &WriteMultiTEXTConfig,
    ) -> Result<Nextdata, ImpureError<Uint8DigitOverflowError>>
    where
        Version: From<M::Ver>,
    {
        let opts = conf.multi.append.file_options();
        let f = opts.open(path)?;
        let mut h = BufWriter::new(f);
        self.h_write_text(&mut h, &conf.inner, conf.multi.appendable)
    }

    /// Write this core structure (HEADER+TEXT) to a handle
    pub fn h_write_text<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        conf: &WriteTEXTInnerConfig,
        has_nextdata: AppendableFlag,
    ) -> Result<Nextdata, ImpureError<Uint8DigitOverflowError>>
    where
        Version: From<M::Ver>,
    {
        if conf.big_other.is_set() {
            self.h_write_text_inner1::<_, UintSpacePad20>(h, conf.delim, has_nextdata)
        } else {
            self.h_write_text_inner1::<_, UintSpacePad8>(h, conf.delim, has_nextdata)
        }
    }

    fn h_write_text_inner1<W: Write, T>(
        &self,
        h: &mut BufWriter<W>,
        delim: TEXTDelim,
        has_nextdata: AppendableFlag,
    ) -> Result<Nextdata, ImpureError<Uint8DigitOverflowError>>
    where
        Version: From<M::Ver>,
        T: Zero + TryFrom<u64, Error = Uint8DigitOverflowError> + HeaderString,
    {
        let conf = WriteHeaderAndTextConfig::new_nodata(delim, has_nextdata);
        self.h_write_text_inner::<_, T>(h, &conf)
    }

    fn h_write_text_inner<W: Write, T>(
        &self,
        h: &mut BufWriter<W>,
        conf: &WriteHeaderAndTextConfig<'_>,
    ) -> Result<Nextdata, ImpureError<Uint8DigitOverflowError>>
    where
        Version: From<M::Ver>,
        T: Zero + TryFrom<u64, Error = Uint8DigitOverflowError> + HeaderString,
    {
        let hdr_kws: HeaderKeywordsToWrite<T> = self
            .header_and_flat_keywords(conf)
            .map_err(ImpureError::Pure)?;
        hdr_kws.h_write(h, M::Ver::fcs_version(), conf.delim, conf.other_segs)?;
        Ok(hdr_kws.nextdata)
    }

    /// Return all keywords as an ordered list of pairs
    ///
    /// This will not include $TOT, $NEXTDATA, or any offset keywords since
    /// these only matter when the dataset is written.
    pub fn standard_keywords(
        &self,
        req_or_opt: IncludeReqOrOpt,
        root_or_meas: IncludeRootOrMeas,
    ) -> HashMap<String, String> {
        fn go(
            xs: impl Iterator<Item = (String, String)>,
            include: bool,
        ) -> impl Iterator<Item = (String, String)> {
            include.then_some(xs).into_iter().flatten()
        }

        let (include_req_root, include_opt_root, include_req_meas, include_opt_meas) =
            match (req_or_opt, root_or_meas) {
                (IncludeReqOrOpt::Both, IncludeRootOrMeas::Both) => (true, true, true, true),
                (IncludeReqOrOpt::Both, IncludeRootOrMeas::Root) => (true, true, false, false),
                (IncludeReqOrOpt::Both, IncludeRootOrMeas::Meas) => (false, false, true, true),
                (IncludeReqOrOpt::Req_, IncludeRootOrMeas::Both) => (true, false, true, false),
                (IncludeReqOrOpt::Opt_, IncludeRootOrMeas::Both) => (false, true, false, true),
                (IncludeReqOrOpt::Req_, IncludeRootOrMeas::Root) => (true, false, false, false),
                (IncludeReqOrOpt::Opt_, IncludeRootOrMeas::Root) => (false, true, false, false),
                (IncludeReqOrOpt::Req_, IncludeRootOrMeas::Meas) => (false, false, true, false),
                (IncludeReqOrOpt::Opt_, IncludeRootOrMeas::Meas) => (false, false, false, true),
            };

        go(self.req_root_keywords(), include_req_root)
            .chain(go(self.opt_root_keywords(), include_opt_root))
            .chain(go(self.req_meas_keywords(), include_req_meas))
            .chain(go(self.opt_meas_keywords(), include_opt_meas))
            .collect()
    }

    /// Set the $TR keyword.
    ///
    /// Return error if supplied name is not a measurement name (a $PnN) or
    /// if name references temporal measurement.
    pub fn set_trigger(&mut self, tr: Option<Trigger>) -> Result<(), KeyToNameLinkError<Trigger>> {
        let ns = self.measurements.named_set();
        tr.as_ref()
            .and_then(|t| t.invalid_link_error(&ns))
            .map_or(Ok(()), Err)?;
        self.metaroot.tr = tr;
        Ok(())
    }

    /// Set threshold for $TR keyword
    ///
    /// Return true if trigger exists, false otherwise.
    pub fn set_trigger_threshold(&mut self, x: u32) -> bool {
        if let Some(tr) = self.metaroot.tr.as_mut() {
            tr.threshold = x;
            true
        } else {
            false
        }
    }

    /// Return a list of measurement names as stored in $PnN.
    pub fn shortnames_maybe(&self) -> Vec<Option<&Shortname>> {
        self.measurements
            .iter()
            .map(|x| x.both(|t| Some(&t.key), |m| M::Name::as_opt(&m.key)))
            .collect()
    }

    /// Return a list of measurement names as stored in $PnN
    ///
    /// For cases where $PnN is optional and its value is not given, this will
    /// return "Pn" where "n" is the parameter index starting at 1.
    pub fn all_shortnames(&self) -> Vec<Shortname> {
        self.measurements.iter_all_names().collect()
    }

    /// Set all $PnN keywords to list of names.
    ///
    /// The length of the names must match the number of measurements. Any
    /// keywords refering to the old names will be updated to reflect the new
    /// names. For 2.0 and 3.0 which have optional $PnN, all $PnN will end up
    /// being set.
    pub fn set_all_shortnames(&mut self, ns: Vec<Shortname>) -> Result<NameMapping, SetNamesError> {
        let mapping = self.measurements.set_names(ns)?;
        self.metaroot.rename_meas_links(&mapping);
        Ok(mapping)
    }

    /// Set the measurement matching given name to be the time measurement.
    pub fn set_temporal(
        &mut self,
        n: &Shortname,
        timestep: <M::Temporal as TemporalFromOptical<M::Optical>>::TData,
        allow_loss: AllowLoss,
    ) -> WarningOrErrorResult<bool, (), SetTemporalError, SetTemporalByNameError>
    where
        M::Temporal: TemporalFromOptical<M::Optical>,
    {
        self.measurements.set_center_by_name(
            n,
            |old, new| {
                M::swap_optical_temporal(old, new, allow_loss)
                    .map_switchable_errors(SetTemporalError::from)
                    .switchable_into_non_commutative()
                    .map_errors(SetTemporalByNameError::from)
            },
            |i, old_o| {
                M::Temporal::from_optical(old_o, i, timestep, allow_loss)
                    .map_switchable_errors(SetTemporalError::from)
                    .switchable_into_non_commutative()
                    .map_errors(SetTemporalByNameError::from)
            },
        )
    }

    /// Set the measurement at given index to the time measurement.
    pub fn set_temporal_at(
        &mut self,
        index: MeasIndex,
        timestep: <M::Temporal as TemporalFromOptical<M::Optical>>::TData,
        allow_loss: AllowLoss,
    ) -> WarningOrErrorResult<bool, (), SetTemporalError, SetTemporalByIndexError>
    where
        M::Temporal: TemporalFromOptical<M::Optical>,
    {
        self.measurements.set_center_by_index(
            index,
            |old, new| {
                M::swap_optical_temporal(old, new, allow_loss)
                    .map_switchable_errors(SetTemporalError::from)
                    .switchable_into_non_commutative()
                    .map_errors(SetTemporalByIndexError::from)
            },
            |i, old_o| {
                M::Temporal::from_optical(old_o, i, timestep, allow_loss)
                    .map_switchable_errors(SetTemporalError::from)
                    .switchable_into_non_commutative()
                    .map_errors(SetTemporalByIndexError::from)
            },
        )
    }

    /// Convert time measurement to optical measurement.
    ///
    /// Return true if a time measurement existed and was converted, false
    /// otherwise.
    pub fn unset_temporal(
        &mut self,
    ) -> Option<<M::Optical as OpticalFromTemporal<M::Temporal>>::TData>
    where
        M::Optical: OpticalFromTemporal<M::Temporal, LossFlag = ()>,
        M::Temporal: VersionedTemporal<Warning = Nothing<()>, Error = Infallible>,
    {
        self.measurements
            .unset_center(|i, old_t| M::Optical::from_temporal(old_t, i, ()))
            .infallible_nowarn_into()
    }

    /// Convert time measurement to optical measurement.
    ///
    /// Return true if a time measurement existed and was converted, false
    /// otherwise.
    #[allow(clippy::type_complexity)]
    pub fn unset_temporal_lossy(
        &mut self,
        allow_loss: AllowLoss,
    ) -> WarningOrErrorResult<
        Option<<M::Optical as OpticalFromTemporal<M::Temporal>>::TData>,
        (),
        AnyTemporalToOpticalKeyLossError,
        AnyTemporalToOpticalKeyLossError,
    >
    where
        M::Optical: OpticalFromTemporal<M::Temporal, LossFlag = AllowLoss>,
        M::Temporal: VersionedTemporal<
                Warning = Option<AnyTemporalToOpticalKeyLossError>,
                Error = AnyTemporalToOpticalKeyLossError,
            >,
    {
        self.measurements.unset_center(|i, old_t| {
            M::Optical::from_temporal(old_t, i, allow_loss).switchable_into_non_commutative()
        })
    }

    /// Read nonstandard key/value pairs for each measurement.
    ///
    /// This includes the time measurement if present.
    pub fn get_meas_nonstandard(&self) -> Vec<&HashMap<NonStdKey, String>> {
        self.measurements.iter_common_values().collect()
    }

    /// Set nonstandard key/value pairs for each measurement.
    ///
    /// This includes the time measurement if present.
    pub fn set_meas_nonstandard(
        &mut self,
        xs: impl IntoIterator<Item = HashMap<NonStdKey, String>>,
    ) -> Result<(), InputLengthError> {
        self.measurements
            .alter_common_values_zip(xs, |_, y: &mut HashMap<_, _>, x| *y = x)
            .map(|_| ())
    }

    /// Replace optical measurement at index.
    ///
    /// If index points to a temporal measurement, replace it with the given
    /// optical measurement. In both cases the name is kept. Return the
    /// measurement that was replaced if the index was in bounds.
    pub fn replace_optical_at(
        &mut self,
        index: MeasIndex,
        m: Optical<M::Optical>,
    ) -> Result<TemporalOrOptical<M>, ElementIndexError> {
        self.measurements.replace_at(index, m)
    }

    /// Replace optical measurement with name.
    ///
    /// If name refers to a temporal measurement, replace it with the given
    /// optical measurement. Return the measurement that was replaced if the
    /// index was in bounds.
    pub fn replace_optical_named(
        &mut self,
        name: &Shortname,
        m: Optical<M::Optical>,
    ) -> Result<TemporalOrOptical<M>, NameNotFoundError> {
        self.measurements.replace_named(name, m)
    }

    /// Replace temporal measurement at index.
    pub fn replace_temporal_at(
        &mut self,
        index: MeasIndex,
        m: Temporal<M::Temporal>,
    ) -> Result<TemporalOrOptical<M>, SetCenterError>
    where
        M::Optical: OpticalFromTemporal<M::Temporal, LossFlag = ()>,
        M::Temporal: VersionedTemporal<Warning = Nothing<()>, Error = Infallible>,
    {
        self.measurements
            .replace_center_at_nofail(index, m, |i, old_t| {
                M::Optical::from_temporal(old_t, i, ())
                    .set_err_value(())
                    .infallible_nowarn_into()
                    .0
            })
    }

    /// Replace temporal measurement at index.
    pub fn replace_temporal_at_lossy(
        &mut self,
        index: MeasIndex,
        m: Temporal<M::Temporal>,
        allow_loss: AllowLoss,
    ) -> WarningOrErrorResult<
        TemporalOrOptical<M>,
        (),
        AnyTemporalToOpticalKeyLossError,
        ReplaceTemporalErrorByIndex,
    >
    where
        M::Optical: OpticalFromTemporal<M::Temporal, LossFlag = AllowLoss>,
        M::Temporal: VersionedTemporal<
                Warning = Option<AnyTemporalToOpticalKeyLossError>,
                Error = AnyTemporalToOpticalKeyLossError,
            >,
    {
        self.measurements.replace_center_at(index, m, |i, old_t| {
            M::Optical::from_temporal(old_t, i, allow_loss)
                .switchable_into_non_commutative()
                .map_ok_value(|(x, _)| x)
                .map_errors(ReplaceTemporalErrorByIndex::from)
        })
    }

    /// Replace temporal measurement with name.
    pub fn replace_temporal_named(
        &mut self,
        name: &Shortname,
        m: Temporal<M::Temporal>,
    ) -> Result<TemporalOrOptical<M>, NameNotFoundError>
    where
        M::Optical: OpticalFromTemporal<M::Temporal, LossFlag = ()>,
        M::Temporal: VersionedTemporal<Warning = Nothing<()>, Error = Infallible>,
    {
        self.measurements
            .replace_center_by_name_nofail(name, m, |i, old_t| {
                M::Optical::from_temporal(old_t, i, ())
                    .set_err_value(())
                    .infallible_nowarn_into()
                    .0
            })
    }

    /// Replace temporal measurement with name.
    pub fn replace_temporal_named_lossy(
        &mut self,
        name: &Shortname,
        m: Temporal<M::Temporal>,
        allow_loss: AllowLoss,
    ) -> WarningOrErrorResult<
        TemporalOrOptical<M>,
        (),
        AnyTemporalToOpticalKeyLossError,
        ReplaceTemporalErrorByName,
    >
    where
        M::Optical: OpticalFromTemporal<M::Temporal, LossFlag = AllowLoss>,
        M::Temporal: VersionedTemporal<
                Warning = Option<AnyTemporalToOpticalKeyLossError>,
                Error = AnyTemporalToOpticalKeyLossError,
            >,
    {
        self.measurements
            .replace_center_by_name(name, m, |i, old_t| {
                M::Optical::from_temporal(old_t, i, allow_loss)
                    .switchable_into_non_commutative()
                    .map_ok_value(|(x, _)| x)
                    .map_errors(ReplaceTemporalErrorByName::from)
            })
    }

    /// Rename a measurement
    ///
    /// If index points to the center element and the wrapped name contains
    /// nothing, the default name will be assigned. Return error if index is
    /// out of bounds or name is not unique. Return pair of old and new name
    /// on success.
    pub fn rename_measurement(
        &mut self,
        index: MeasIndex,
        key: M::Name,
    ) -> Result<(Shortname, Shortname), RenameError> {
        self.measurements.rename(index, key).map(|(old, new)| {
            let mapping = once((old.clone(), new.clone())).collect();
            self.metaroot.rename_meas_links(&mapping);
            (old, new)
        })
    }

    /// Rename time measurement if it exists
    pub fn rename_temporal(&mut self, name: Shortname) -> Option<Shortname> {
        self.measurements.rename_center(name)
    }

    /// Apply functions to measurement values
    pub fn alter_measurements<F, G, R>(&mut self, f: F, g: G) -> Vec<R>
    where
        F: Fn(IndexedElement<&Shortname, &mut Temporal<M::Temporal>>) -> R,
        G: Fn(IndexedElement<&M::Name, &mut Optical<M::Optical>>) -> R,
    {
        self.measurements.alter_values(f, g)
    }

    /// Apply functions to measurement values with payload
    pub fn alter_measurements_zip<F, G, X, R>(
        &mut self,
        xs: Vec<X>,
        f: F,
        g: G,
    ) -> Result<Vec<R>, InputLengthError>
    where
        F: Fn(IndexedElement<&Shortname, &mut Temporal<M::Temporal>>, X) -> R,
        G: Fn(IndexedElement<&M::Name, &mut Optical<M::Optical>>, X) -> R,
    {
        self.measurements.alter_values_zip(xs, f, g)
    }

    /// Return reference to time measurement as a name/value pair.
    pub fn temporal(&self) -> Option<IndexedElement<&Shortname, &Temporal<M::Temporal>>> {
        self.measurements.as_center()
    }

    /// Return mutable reference to time measurement as a name/value pair.
    pub fn temporal_mut(
        &mut self,
    ) -> Option<IndexedElement<&mut Shortname, &mut Temporal<M::Temporal>>> {
        self.measurements.as_center_mut()
    }

    /// Return a reference to a field in metaroot
    pub fn metaroot<X>(&self) -> &X
    where
        Metaroot<M>: AsRef<X>,
    {
        self.metaroot.as_ref()
    }

    /// Return a reference to an optional field in metaroot
    pub fn metaroot_opt<X>(&self) -> Option<&X>
    where
        Metaroot<M>: AsRef<Option<X>>,
    {
        self.metaroot().as_ref()
    }

    /// Set a field in metaroot
    pub fn set_metaroot<X>(&mut self, x: X)
    where
        Metaroot<M>: AsMut<X>,
    {
        *self.metaroot.as_mut() = x;
    }

    /// Get a field from all measurements as an interator
    pub fn meas<'a, X: 'a>(&'a self) -> impl Iterator<Item = &'a X>
    where
        Temporal<M::Temporal>: AsRef<X>,
        Optical<M::Optical>: AsRef<X>,
    {
        self.measurements
            .iter()
            .map(|x| x.both(|t| t.value.as_ref(), |m| m.value.as_ref()))
    }

    /// Get an optional field from all measurements as an interator
    pub fn meas_opt<'a, X: 'a>(&'a self) -> impl Iterator<Item = Option<&'a X>>
    where
        Temporal<M::Temporal>: AsRef<Option<X>>,
        Optical<M::Optical>: AsRef<Option<X>>,
    {
        self.meas::<Option<X>>().map(|x| x.as_ref())
    }

    /// Set the field on all measurements to values in a vector
    pub fn set_meas<X>(&mut self, xs: Vec<X>) -> Result<(), InputLengthError>
    where
        Temporal<M::Temporal>: AsMut<X>,
        Optical<M::Optical>: AsMut<X>,
    {
        self.measurements
            .alter_values_zip(
                xs,
                |m, x| *m.value.as_mut() = x,
                |m, x| *m.value.as_mut() = x,
            )
            .map(|_| ())
    }

    /// Return field from all optical measurements as an iterator
    pub fn optical<'a, X: 'a>(&'a self) -> impl Iterator<Item = NonCenterElement<&'a X>>
    where
        Optical<M::Optical>: AsRef<X>,
    {
        self.measurements
            .iter()
            .map(|e| e.bimap_once(|_| (), |v| v.value.as_ref()).into())
    }

    /// Return optional field from all optical measurements as an iterator
    pub fn optical_opt<'a, X: 'a>(&'a self) -> impl Iterator<Item = NonCenterElement<Option<&'a X>>>
    where
        Optical<M::Optical>: AsRef<Option<X>>,
    {
        self.optical()
            .map(|e| e.0.second_once(|x| x.as_ref()).into())
    }

    // /// Return optional field from all optical measurements as an iterator
    // pub fn optical_temporal_opt<'a, X: 'a, Y: 'a>(
    //     &'a self,
    // ) -> impl Iterator<Item = Element<Option<&'a X>, Option<&'a Y>>>
    // where
    //     Optical<M::Optical>: AsRef<Option<X>>,
    //     Temporal<M::Temporal>: AsRef<Option<Y>>,
    // {
    //     self.optical()
    //         .map(|e| e.0.bimap(|x| x.as_ref(), |y| y.as_ref()))
    // }

    /// Set fields on all optical measurements to values in a vector
    pub fn set_optical<X>(&mut self, xs: Vec<NonCenterElement<X>>) -> Result<(), SetOpticalError>
    where
        Optical<M::Optical>: AsMut<X>,
    {
        let ys = xs.fmap(|x| x.0);
        self.measurements.alter_elements_zip(
            ys,
            SetOpticalSummary,
            |m, x| *m.value.as_mut() = x,
            |_, ()| (),
            |i, is_opt| MeasMismatchError::new(is_opt, i),
        )?;
        Ok(())
    }

    /// Get field which is on both optical and temporal measurement types
    pub fn get_temporal_optical<'a, X: 'a, Y: 'a>(
        &'a self,
    ) -> impl Iterator<Item = Element<&'a X, &'a Y>>
    where
        Temporal<M::Temporal>: AsRef<X>,
        Optical<M::Optical>: AsRef<Y>,
    {
        self.measurements
            .iter()
            .map(|x| x.bimap_once(|m| m.value.as_ref(), |m| m.value.as_ref()))
    }

    /// Set field which is on both optical and temporal measurement types
    pub fn set_temporal_optical<T>(&mut self, xs: Vec<T>) -> Result<(), InputLengthError>
    where
        Optical<M::Optical>: AsMut<T>,
        Temporal<M::Temporal>: AsMut<T>,
    {
        self.measurements
            .alter_values_zip(
                xs,
                |m, x| *m.value.as_mut() = x,
                |m, x| *m.value.as_mut() = x,
            )
            .map(|_| ())
    }

    /// Set field which is on both optical and temporal measurement types
    pub fn set_temporal_optical2<X, Y>(
        &mut self,
        xs: Vec<Element<X, Y>>,
    ) -> Result<(), SetAllMeasError>
    where
        Temporal<M::Temporal>: AsMut<X>,
        Optical<M::Optical>: AsMut<Y>,
    {
        self.measurements.alter_elements_zip(
            xs,
            SetAllMeasSummary,
            |m, x| *m.value.as_mut() = x,
            |m, y| *m.value.as_mut() = y,
            |i, is_opt| MeasMismatchError::new(is_opt, i),
        )?;
        Ok(())
    }

    /// Get value for $BTIM as a [`NaiveTime`]
    pub fn btim_naive<X>(&self) -> Option<NaiveTime>
    where
        X: Copy,
        NaiveTime: From<X>,
        Metaroot<M>: AsRef<Option<Btim<X>>>,
    {
        self.time_naive()
    }

    /// Get value for $ETIM as a [`NaiveTime`]
    pub fn etim_naive<X>(&self) -> Option<NaiveTime>
    where
        X: Copy,
        NaiveTime: From<X>,
        Metaroot<M>: AsRef<Option<Etim<X>>>,
    {
        self.time_naive()
    }

    /// Set value for $BTIM as a [`NaiveTime`]
    ///
    /// Return error if resulting $BTIM starts after $ETIM and $DATE is
    /// specified.
    pub fn set_btim_naive<X>(
        &mut self,
        time: Option<NaiveTime>,
    ) -> Result<(), ReversedTimestampsError>
    where
        X: PartialOrd + From<NaiveTime>,
        Metaroot<M>: AsMut<Timestamps<X>>,
    {
        let t = self.metaroot.as_mut();
        t.set_btim(time.map(|x| Xtim(x.into())))
    }

    /// Set value for $ETIM as a [`NaiveTime`]
    ///
    /// Return error if resulting $BTIM starts after $ETIM and $DATE is
    /// specified.
    pub fn set_etim_naive<X>(
        &mut self,
        time: Option<NaiveTime>,
    ) -> Result<(), ReversedTimestampsError>
    where
        X: PartialOrd + From<NaiveTime>,
        Metaroot<M>: AsMut<Timestamps<X>>,
    {
        let t = self.metaroot.as_mut();
        t.set_etim(time.map(|x| Xtim(x.into())))
    }

    /// Get $DATE as a [`NaiveDate`]
    pub fn date_naive(&self) -> Option<NaiveDate>
    where
        Metaroot<M>: AsRef<Option<FCSDate>>,
    {
        self.metaroot.as_ref().as_ref().map(|&x| x.into())
    }

    /// Set $DATE as a [`NaiveDate`]
    ///
    /// Return error if resulting $BTIM starts after $ETIM and $DATE is
    /// specified.
    pub fn set_date_naive<X>(
        &mut self,
        date: Option<NaiveDate>,
    ) -> Result<(), ReversedTimestampsError>
    where
        X: PartialOrd,
        Metaroot<M>: AsMut<Timestamps<X>>,
    {
        self.metaroot.as_mut().set_date(date.map(Into::into))
    }

    /// Get $BEGINDATETIME as a [`DateTime<FixedOffset>`]
    pub fn begindatetime(&self) -> Option<DateTime<FixedOffset>>
    where
        Metaroot<M>: AsRef<Option<BeginDateTime>>,
    {
        self.metaroot.as_ref().as_ref().copied().map(Into::into)
    }

    /// Get $ENDDATETIME as a [`DateTime<FixedOffset>`]
    pub fn enddatetime(&self) -> Option<DateTime<FixedOffset>>
    where
        Metaroot<M>: AsRef<Option<EndDateTime>>,
    {
        self.metaroot.as_ref().as_ref().copied().map(Into::into)
    }

    /// Set $BEGINDATETIME as a [`DateTime<FixedOffset>`]
    ///
    /// Return error if resulting $BEGINDATETIME is after $ENDDATETIME.
    pub fn set_begindatetime(
        &mut self,
        date: Option<DateTime<FixedOffset>>,
    ) -> Result<(), ReversedDatetimesError>
    where
        Metaroot<M>: AsMut<Datetimes>,
    {
        self.metaroot.as_mut().set_begin(date.map(Into::into))
    }

    /// Set $ENDDATETIME as a [`DateTime<FixedOffset>`]
    ///
    /// Return error if resulting $BEGINDATETIME is after $ENDDATETIME.
    pub fn set_enddatetime(
        &mut self,
        date: Option<DateTime<FixedOffset>>,
    ) -> Result<(), ReversedDatetimesError>
    where
        Metaroot<M>: AsMut<Datetimes>,
    {
        self.metaroot.as_mut().set_end(date.map(Into::into))
    }

    /// Get $TIMESTEP value if the time measurement exists.
    pub fn timestep(&self) -> Option<&Timestep>
    where
        Temporal<M::Temporal>: AsRef<Timestep>,
    {
        self.measurements.as_center().map(|x| x.value.as_ref())
    }

    /// Set $TIMESTEP value if the time measurement exists.
    ///
    /// Return `true` if the time measurement exist (which means its $TIMESTEP
    /// was updated) and `false` otherwise.
    pub fn set_timestep(&mut self, timestep: Timestep) -> Option<Timestep>
    where
        Temporal<M::Temporal>: AsMut<Timestep>,
    {
        self.measurements.as_center_mut().map(|x| {
            let ts = x.value.as_mut();
            let old = *ts;
            *ts = timestep;
            old
        })
    }

    /// Show $COMP.
    pub fn compensation(&self) -> Option<&Compensation>
    where
        M: HasCompensation,
    {
        self.metaroot.specific.comp(private::NoTouchy)
    }

    /// Set matrix for $COMP.
    ///
    /// Return true if successfully set. Return false if matrix is either not
    /// square or rows/columns are not the same length as $PAR.
    pub fn set_compensation(
        &mut self,
        matrix: Option<Compensation>,
    ) -> Result<(), CompParMismatchError>
    where
        M: HasCompensation,
    {
        if let Some(m) = matrix.as_ref() {
            let comp = m.as_ref().ncols();
            let par = self.measurements.len();
            if comp != par {
                return Err(CompParMismatchError { par, comp });
            }
        }
        self.metaroot.specific.set_comp(matrix, private::NoTouchy);
        Ok(())
    }

    /// Show $SPILLOVER
    pub fn spillover(&self) -> Option<&Spillover>
    where
        M: AsRef<Option<Spillover>>,
    {
        self.metaroot.specific.as_ref().as_ref()
    }

    /// Set $SPILLOVER
    ///
    /// Return error if any measurements reference temporal measurement or
    /// if supplied matrix is invalid.
    pub fn set_spillover(&mut self, spillover: Option<Spillover>) -> Result<(), SetSpilloverErrors>
    where
        M: HasSpillover,
    {
        if let Some(s) = spillover.as_ref() {
            let ns = self.measurements.named_set();
            SetSpilloverErrors::try_new(s.invalid_link_errors(&ns))?;
        }
        *self.metaroot.specific.spill_mut(private::NoTouchy) = spillover;
        Ok(())
    }

    /// Set $UNSTAINEDCENTERS
    ///
    /// Will return error for each name that is not in $PnN or if any name
    /// references the temporal channel.
    pub fn set_unstained_centers(
        &mut self,
        us: UnstainedCenters,
    ) -> Result<(), SetUnstainedCentersErrors>
    where
        M: HasUnstainedCenters,
    {
        let ns = self.measurements.named_set();
        SetUnstainedCentersErrors::try_new(us.invalid_link_error(&ns))?;
        *self
            .metaroot
            .specific
            .unstainedcenters_mut(private::NoTouchy) = us;
        Ok(())
    }

    /// Return $PnE (2.0)
    pub fn scales(&self) -> impl Iterator<Item = Option<Scale>>
    where
        Optical<M::Optical>: AsRef<Option<Scale>>,
    {
        self.measurements.iter().map(|x| {
            x.both(
                |_| Some(Scale::Linear),
                |m| m.value.as_ref().as_ref().copied(),
            )
        })
    }

    /// Return $PnE/$PnG (3.0+)
    pub fn transforms(&self) -> impl Iterator<Item = ScaleTransform>
    where
        Optical<M::Optical>: AsRef<ScaleTransform>,
    {
        self.measurements
            .iter()
            .map(|x| x.both(|_| ScaleTransform::default(), |m| *m.value.as_ref()))
    }

    /// Return $PnFEATURE if it is area/width/height (3.2+)
    ///
    /// Values which are not area, width, or height will be returned as `None`.
    pub fn awh_features(&self) -> impl Iterator<Item = NonCenterElement<Option<OpticalFeature>>>
    where
        Optical<M::Optical>: AsRef<Option<Feature>>,
    {
        self.optical_opt().map(|x| {
            x.fmap_once(|y| {
                let f = y?;
                if let Feature::Optical(i) = f {
                    Some(*i)
                } else {
                    None
                }
            })
        })
    }

    /// Return $PnFEATURE if it is not area/width/height (3.2+)
    pub fn other_features(&self) -> impl Iterator<Item = NonCenterElement<Option<&str>>>
    where
        Optical<M::Optical>: AsRef<Option<Feature>>,
    {
        self.optical_opt().map(|x| {
            x.fmap_once(|y| {
                let f = y?;
                if let Feature::Other(i) = f {
                    Some(i.as_ref())
                } else {
                    None
                }
            })
        })
    }

    /// Return $PnFEATURE if it is area/width/height (3.2+)
    ///
    /// This should be used only if the required features are area, width, and
    /// height. Any `None` values in the vector will unset the value of
    /// $PnFEATURE for that measurement.
    pub fn set_awh_features(
        &mut self,
        xs: Vec<NonCenterElement<Option<OpticalFeature>>>,
    ) -> Result<(), SetOpticalError>
    where
        Optical<M::Optical>: AsMut<Option<Feature>>,
    {
        let ys = xs.fmap(|y| y.fmap_once(|z| z.fmap_once(Feature::Optical)));
        self.set_optical(ys)
    }

    /// Set $PnE (2.0)
    pub fn set_scales(
        &mut self,
        scales: Vec<Option<Scale>>,
    ) -> GroupResult<(), SetScalesError, SetScalesSummary>
    where
        M::Optical: HasScale,
    {
        let center_scale_not_linear = || {
            self.measurements
                .center_index()
                .map(usize::from)
                .and_then(|i| scales.get(i).map(Option::as_ref))
                .flatten()
                .is_some_and(|&s| s != Scale::Linear)
                .then_some(NonLinearTemporalScaleError.into())
        };

        let l = &self.layout;
        let xforms: Vec<_> = scales
            .iter()
            .copied()
            .map(|s| s.map(ScaleTransform::from).unwrap_or_default())
            .collect();
        l.check_transforms_and_len(&xforms[..])
            .map_err(SetScalesError::from)
            .into_nowarn()
            .eval_deferred_error(|()| center_scale_not_linear())
            .when_ok(|| {
                debug_assert!(
                    self.measurements.len() == scales.len(),
                    "Input scales vector should be same length as existing measurements"
                );
                self.measurements
                    .alter_values_zip(
                        scales,
                        |_, _| (),
                        |m, x| *m.value.specific.scale_mut(private::NoTouchy) = x,
                    )
                    .unwrap();
            })
            .group()
            .resolve_nowarn()
    }

    /// Set $PnE/$PnG (3.0+)
    pub fn set_transforms(
        &mut self,
        xforms: Vec<ScaleTransform>,
    ) -> GroupResult<(), SetTransformsError, SetTransformsSummary>
    where
        M::Optical: HasScaleTransform,
    {
        let center_xform_not_noop = || {
            self.measurements
                .center_index()
                .map(usize::from)
                .and_then(|i| xforms.get(i))
                .is_some_and(ScaleTransform::is_noop)
                .then_some(NonLinearTemporalTransformError.into())
        };

        let l = &self.layout;
        l.check_transforms_and_len(&xforms[..])
            .map_err(SetTransformsError::from)
            .into_nowarn()
            .eval_deferred_error(|()| center_xform_not_noop())
            .when_ok(|| {
                debug_assert!(
                    self.measurements.len() == xforms.len(),
                    "Input transforms vector should be same length as existing measurements"
                );
                self.measurements
                    .alter_values_zip(
                        xforms,
                        |_, _| (),
                        |m, x| *m.value.specific.transform_mut(private::NoTouchy) = x,
                    )
                    .unwrap();
            })
            .group()
            .resolve_nowarn()
    }

    /// Set gating keywords (3.0/3.1)
    pub fn set_applied_gates_3_0(
        &mut self,
        ag: AppliedGates3_0,
    ) -> GroupResult<(), BrokenRegionLinkError<MeasOrGateIndex>, SetAppliedGatesSummary>
    where
        M: HasAppliedGates3_0,
    {
        let p = self.par();
        let es = ag.invalid_link_errors(&p);
        ErrorGroup::try_new(es)?;
        *self
            .metaroot
            .specific
            .applied_gates3_0_mut(private::NoTouchy) = ag;
        Ok(())
    }

    /// Set gating keywords (3.2)
    pub fn set_applied_gates_3_2(
        &mut self,
        ag: AppliedGates3_2,
    ) -> GroupResult<(), BrokenRegionLinkError<PrefixedMeasIndex>, SetAppliedGatesSummary>
    where
        M: HasAppliedGates3_2,
    {
        let p = self.par();
        let es = ag.invalid_link_errors(&p);
        ErrorGroup::try_new(es)?;
        *self
            .metaroot
            .specific
            .applied_gates3_2_mut(private::NoTouchy) = ag;
        Ok(())
    }

    /// Get reference to non-standard keywords.
    pub fn nonstandard_keywords(&self) -> &NonStdKeywords {
        &self.metaroot.nonstandard_keywords
    }

    /// Set non-standard keywords to new hash map.
    pub fn set_nonstandard_keywords(&mut self, kws: NonStdKeywords) {
        self.metaroot.nonstandard_keywords = kws;
    }

    /// Convert to another FCS version.
    ///
    /// Conversion may fail if some required keywords in the target version
    /// are not present in current version.
    #[allow(clippy::type_complexity)]
    pub fn try_convert<ToM>(
        self,
        allow_loss: AllowLoss,
    ) -> WarningsAndGroupResult<
        VersionedCore<A, D, O, ToM>,
        MetarootConvertWarning,
        ConvertError,
        ConvertSummary,
    >
    where
        Version: From<M::Ver> + From<ToM::Ver>,
        ToM: VersionedMetaroot + ConvertFromMetaroot<M>,
        ToM::Optical: VersionedOptical + ConvertFromOptical<M::Optical>,
        ToM::Temporal: VersionedTemporal + ConvertFromTemporal<M::Temporal>,
        ToM::Name: MightHave<Shortname> + Clone + ConvertFromShortname<M::Name>,
        <ToM::Ver as Versioned>::Layout: ConvertFromLayout<<M::Ver as Versioned>::Layout>,
    {
        let root_res = self
            .metaroot
            .try_convert(allow_loss)
            .map_errors(ConvertError::Meta);
        let meas_res = self
            .measurements
            .map_center_value(|v| {
                v.value
                    .convert(v.index, allow_loss)
                    .switchable_into_commutative()
            })
            .set_err_value(())
            .map_errors(ConvertError::Temporal)
            .map_commutative_warnings(MetarootConvertWarning::from)
            .and_then_commutative(|meas| {
                meas.map_non_center_values(|i, v| v.try_convert(i, allow_loss))
                    .map_errors(ConvertError::Optical)
                    .map_commutative_warnings(MetarootConvertWarning::from)
            })
            .and_then_commutative(|meas| {
                meas.try_rewrapped(|i, n| ToM::Name::convert_from_shortname(n, i))
                    .map_errors(ConvertError::Rewrap)
                    .nowarn_into_warn()
            });
        let layout_res = ConvertFromLayout::convert_from_layout(self.layout)
            .map_errors(ConvertError::Layout)
            .nowarn_into_warn();
        let v0 = M::Ver::fcs_version();
        let v1 = ToM::Ver::fcs_version();
        let summary = ConvertSummary::new(v0, v1);
        root_res
            .zip3_commutative(meas_res, layout_res)
            .map_ok_value(|(metaroot, measurements, layout)| {
                Core::new(
                    metaroot,
                    measurements,
                    layout,
                    self.data,
                    self.analysis,
                    self.others,
                )
            })
            .group_with(summary)
    }

    fn named_compensation(&self) -> Option<(Vec<Shortname>, DMatrix<f32>)>
    where
        M: HasCompensation,
    {
        self.compensation().as_ref().map(|c| {
            let m: &DMatrix<f32> = c.as_ref();
            (self.all_shortnames(), m.clone())
        })
    }

    fn named_spillover(&self) -> Option<(Vec<Shortname>, DMatrix<f32>)>
    where
        M: AsRef<Option<Spillover>>,
    {
        self.spillover().as_ref().map(|c| {
            let ns: &[Shortname] = c.as_ref();
            let m: &DMatrix<f32> = c.as_ref();
            (ns.to_vec(), m.clone())
        })
    }

    fn time_naive<const IS_ETIM: bool, X>(&self) -> Option<NaiveTime>
    where
        X: Copy,
        NaiveTime: From<X>,
        Metaroot<M>: AsRef<Option<Xtim<IS_ETIM, X>>>,
    {
        let t: &Option<Xtim<IS_ETIM, X>> = self.metaroot.as_ref();
        t.as_ref().map(|&x| x.0.into())
    }

    fn remove_measurement_by_name_inner(
        &mut self,
        name: &Shortname,
    ) -> Result<(MeasIndex, TemporalOrOptical<M>, Range), RemoveMeasByNameError> {
        if let Some(&index) = self.measurement_named_indices().get(name) {
            // NOTE if the meas to be removed is temporal, this name shouldn't
            // trigger a link error because $SPILLOVER, $UNSTAINEDCENTERS, and
            // $TR should never link to a temporal measurement
            let ns = HashSet::from([name]).into();
            let js = HashSet::from([index]).into();
            let es = self
                .metaroot
                .meas_has_existing_links_with(self.par(), &ns, &js);
            ExistingLinkErrors::try_new(es)?;
        }
        let (i, e) = self.measurements.remove_name(name)?;
        let r = self.layout.remove_nocheck(i);
        Ok((i, e, r))
    }

    fn remove_measurement_by_index_inner(
        &mut self,
        index: MeasIndex,
    ) -> Result<(NamedTemporalOrOptical<M>, Range), RemoveMeasByIndexError> {
        if let Some(&name) = self.measurement_indexed_names().get(&index) {
            // NOTE (ditto previous function)
            let ns = HashSet::from([name]).into();
            let js = HashSet::from([index]).into();
            let es = self
                .metaroot
                .meas_has_existing_links_with(self.par(), &ns, &js);
            ExistingLinkErrors::try_new(es)?;
        }
        let p = self.measurements.remove_index(index)?;
        let r = self.layout.remove_nocheck(index);
        Ok((p, r))
    }

    // each of these push/insert functions follow the same pattern:
    // 1. check if addition can occur
    // 2. try to insert range and add to errors from 1 if applicable
    // 3. if both of these succeed, add new measurement and update indices

    fn push_temporal_inner(
        &mut self,
        n: Shortname,
        m: Temporal<M::Temporal>,
        r: Range,
        flag: DisallowRangeTrunc,
    ) -> WarningAndErrorsResult<(), (), InsertRangeError, PushTemporalError> {
        self.measurements
            .check_push_center(&n)
            .map_errors(PushTemporalError::from)
            .nowarn_and_then(|()| {
                self.layout
                    .push(r, flag)
                    .switchable_into_commutative()
                    .map_errors(PushTemporalError::from)
                    .repack_errors()
            })
            .when_ok(|| {
                self.measurements.push_center_nocheck(n, m);
                let i = self.par().0.into();
                self.metaroot.specific.insert_meas_index_inner(i);
            })
    }

    fn insert_temporal_inner(
        &mut self,
        i: MeasIndex,
        n: Shortname,
        m: Temporal<M::Temporal>,
        r: Range,
        flag: DisallowRangeTrunc,
    ) -> WarningAndErrorsResult<(), (), InsertRangeError, InsertTemporalError> {
        self.measurements
            .check_insert_center(i, &n)
            .map_errors(InsertTemporalError::from)
            .nowarn_and_then(|()| {
                self.layout
                    .insert_nocheck(i, r, flag)
                    .switchable_into_commutative()
                    .map_errors(InsertTemporalError::from)
                    .repack_errors()
            })
            .when_ok(|| {
                self.measurements.insert_center_nocheck(i, n, m);
                self.metaroot.specific.insert_meas_index_inner(i);
            })
    }

    fn push_optical_inner(
        &mut self,
        n: M::Name,
        m: Optical<M::Optical>,
        r: Range,
        flag: DisallowRangeTrunc,
    ) -> WarningAndErrorsResult<Shortname, (), InsertRangeError, PushOpticalError> {
        self.measurements
            .check_push(&n)
            .map(Cow::into_owned)
            .map_err(PushOpticalError::from)
            .into_nowarn()
            .nowarn_and_then(|ret| {
                self.layout
                    .push(r, flag)
                    .switchable_into_commutative()
                    .map_errors(PushOpticalError::from)
                    .repack_errors()
                    .set_ok_value(ret)
            })
            .map_ok_value(|ret| {
                self.measurements.push_nocheck(n, m);
                let i = self.par().0.into();
                self.metaroot.specific.insert_meas_index_inner(i);
                ret
            })
    }

    fn insert_optical_inner(
        &mut self,
        i: MeasIndex,
        n: M::Name,
        m: Optical<M::Optical>,
        r: Range,
        flag: DisallowRangeTrunc,
    ) -> WarningAndErrorsResult<Shortname, (), InsertRangeError, InsertOpticalError> {
        self.measurements
            .check_insert(i, &n)
            .map_ok_value(Cow::into_owned)
            .map_errors(InsertOpticalError::from)
            .nowarn_and_then(|ret| {
                self.layout
                    .insert_nocheck(i, r, flag)
                    .switchable_into_commutative()
                    .map_errors(InsertOpticalError::from)
                    .repack_errors()
                    .set_ok_value(ret)
            })
            .map_ok_value(|ret| {
                self.measurements.insert_nocheck(i, n, m);
                self.metaroot.specific.insert_meas_index_inner(i);
                ret
            })
    }

    /// Get reference to measurement vector.
    pub fn measurements(&self) -> &Measurements<M::Name, M::Temporal, M::Optical> {
        &self.measurements
    }

    /// Set measurements.
    ///
    /// Return error if names are not unique, if there is more than one
    /// time measurement, or if the measurement length doesn't match the
    /// layout length.
    pub fn set_named_measurements(
        &mut self,
        xs: NamedTemporalsAndOpticals<M>,
        allow_shared_names: bool,
        skip_index_check: bool,
    ) -> Result<(), SetMeasurementsError>
    where
        M::Optical: AsScaleTransform,
    {
        self.set_named_measurements_inner(xs, allow_shared_names, skip_index_check)
    }

    /// Set measurements without $PnN.
    pub fn set_measurements(
        &mut self,
        measurements: TemporalsAndOpticals<M>,
    ) -> Result<(), SetUnnamedMeasurementsError>
    where
        M::Optical: AsScaleTransform,
    {
        self.set_measurements_inner(measurements)
    }

    /// Get reference to data layout
    pub fn layout(&self) -> &<M::Ver as Versioned>::Layout {
        &self.layout
    }

    /// Set data layout
    ///
    /// Will return error if layout does not have same number of columns as
    /// measurements.
    pub fn set_layout(
        &mut self,
        layout: <M::Ver as Versioned>::Layout,
    ) -> Result<(), MeasLayoutMismatchError>
    where
        M::Optical: AsScaleTransform,
    {
        layout.check_measurement_vector(&self.measurements)?;
        self.layout = layout;
        Ok(())
    }

    /// Set measurements and layout
    ///
    /// Return error if measurement names are not unique, there is more
    /// than one time measurement, or the layout and measurements have
    /// different lengths.
    pub fn set_named_measurements_and_layout(
        &mut self,
        measurements: NamedTemporalsAndOpticals<M>,
        layout: <M::Ver as Versioned>::Layout,
        allow_shared_names: bool,
        skip_index_check: bool,
    ) -> Result<(), SetMeasurementsError>
    where
        M::Optical: AsScaleTransform,
    {
        let meas = layout.try_new_measurements::<M>(measurements)?;
        self.new_meas_link_errors(&meas, allow_shared_names, skip_index_check)?;
        self.measurements = meas;
        self.layout = layout;
        Ok(())
    }

    /// Set measurements without $PnN and layout
    pub fn set_measurements_and_layout(
        &mut self,
        measurements: TemporalsAndOpticals<M>,
        layout: <M::Ver as Versioned>::Layout,
    ) -> Result<(), SetUnnamedMeasurementsError>
    where
        M::Optical: AsScaleTransform,
    {
        self.set_measurements_and_layout_inner(measurements, layout)
    }

    fn set_named_measurements_inner(
        &mut self,
        measurements: NamedTemporalsAndOpticals<M>,
        allow_shared_names: bool,
        skip_index_check: bool,
    ) -> Result<(), SetMeasurementsError>
    where
        M::Optical: AsScaleTransform,
    {
        let meas = self.layout.try_new_measurements::<M>(measurements)?;
        self.new_meas_link_errors(&meas, allow_shared_names, skip_index_check)?;
        self.measurements = meas;
        Ok(())
    }

    fn set_measurements_inner(
        &mut self,
        measurements: TemporalsAndOpticals<M>,
    ) -> Result<(), SetUnnamedMeasurementsError>
    where
        M::Optical: AsScaleTransform,
    {
        let xforms: Vec<_> = measurements
            .iter()
            .map(|m| {
                m.as_ref()
                    .both(|_| ScaleTransform::default(), Optical::as_transform)
            })
            .collect();
        self.layout.check_transforms_and_len(&xforms[..])?;
        self.measurements.set_values(measurements)?;
        Ok(())
    }

    fn set_measurements_and_layout_inner(
        &mut self,
        measurements: TemporalsAndOpticals<M>,
        layout: <M::Ver as Versioned>::Layout,
    ) -> Result<(), SetUnnamedMeasurementsError>
    where
        M::Optical: AsScaleTransform,
    {
        let xforms: Vec<_> = measurements
            .iter()
            .map(|m| {
                m.as_ref()
                    .both(|_| ScaleTransform::default(), Optical::as_transform)
            })
            .collect();
        layout.check_transforms_and_len(&xforms[..])?;
        self.measurements.set_values(measurements)?;
        self.layout = layout;
        Ok(())
    }

    fn unset_measurements_inner(&mut self) -> Result<(), ExistingLinkErrors> {
        let p = self.par();
        let (js, ns) = self.all_indices_and_names_to_remove();
        let es = self.metaroot.meas_has_existing_links_with(p, &ns, &js);
        ExistingLinkErrors::try_new(es)?;
        self.measurements = NamedVec::default();
        self.layout.clear();
        Ok(())
    }

    fn header_and_flat_keywords<T>(
        &self,
        conf: &WriteHeaderAndTextConfig<'_>,
    ) -> Result<HeaderKeywordsToWrite<T>, Uint8DigitOverflowError>
    where
        Version: From<M::Ver>,
        T: TryFrom<u64, Error = Uint8DigitOverflowError> + HeaderString,
    {
        let req: Vec<_> = self
            .req_root_keywords()
            .chain([conf.tot.pair()])
            .chain(self.req_meas_keywords())
            .collect();
        let opt: Vec<_> = self
            .opt_root_keywords()
            .chain(self.opt_meas_keywords())
            .collect();
        let other_lens = &conf.other_lens()[..];
        if M::Ver::fcs_version() == Version::FCS2_0 {
            HeaderKeywordsToWrite::new_2_0(
                req,
                opt,
                conf.data_len,
                conf.analysis_len,
                other_lens,
                conf.has_nextdata,
            )
        } else {
            HeaderKeywordsToWrite::new_3_0(
                req,
                opt,
                conf.data_len,
                conf.analysis_len,
                other_lens,
                conf.has_nextdata,
            )
        }
    }

    fn opt_meas_keywords(&self) -> impl Iterator<Item = (String, String)> {
        let ns = (!M::Name::INFALLABLE).then(|| self.shortname_keywords());
        let lv = self.layout.opt_meas_keywords();
        self.measurements
            .iter_with(
                &|i, x| Temporal::all_opt_keywords(&x.value, i).collect::<Vec<_>>(),
                &|i, x| Optical::all_opt_keywords(&x.value, i).collect(),
            )
            .flatten()
            .chain(ns.into_iter().flatten())
            .chain(
                lv.into_iter()
                    .flatten()
                    .filter_map(|(k, v)| v.map(|x| (k, x))),
            )
    }

    fn req_meas_keywords(&self) -> impl Iterator<Item = (String, String)> {
        let ns = M::Name::INFALLABLE.then(|| self.shortname_keywords());
        let lv = self.layout.req_meas_keywords();
        self.measurements
            .iter_with(
                &|i, x| Temporal::req_meas_keywords(&x.value, i).collect::<Vec<_>>(),
                &|i, x| Optical::all_req_keywords(&x.value, i).collect(),
            )
            .flatten()
            .chain(ns.into_iter().flatten())
            .chain(lv.into_iter().flatten())
    }

    fn req_root_keywords(&self) -> impl Iterator<Item = (String, String)> {
        let time_meta = self
            .measurements
            .as_center()
            .map(|tc| Temporal::req_meta_keywords(tc.value));
        let lv = self.layout.req_keywords();
        Metaroot::all_req_keywords(&self.metaroot, self.par())
            .chain(time_meta.into_iter().flatten())
            .chain(lv)
    }

    fn opt_root_keywords(&self) -> impl Iterator<Item = (String, String)> {
        Metaroot::all_opt_keywords(&self.metaroot)
    }

    fn shortname_keywords(&self) -> impl Iterator<Item = (String, String)> {
        self.measurements
            .indexed_names()
            .map(|(i, n)| (Shortname::std(i).to_string(), n.to_string()))
    }

    #[cfg(feature = "serde")]
    fn meas_table(&self, delim: &str) -> Vec<String>
    where
        M::Temporal: Clone,
        M::Optical: OpticalFromTemporal<M::Temporal> + Clone,
    {
        let ms = &self.measurements;
        if let Ok(m0) = ms.get(0.into()) {
            let lt = &self.layout;
            let req_layout: Vec<_> = lt.req_meas_keywords().fmap(|[x, y]| [x.1, y.1]);
            let opt_layout: Vec<_> = lt
                .opt_meas_keywords()
                .fmap(|xs| xs.into_iter().map(|(_, v)| v).collect::<Vec<_>>());
            let header = m0
                .both(
                    |t| M::Optical::from_temporal_unchecked(t.1.clone()).0,
                    |o| o.1.clone(),
                )
                .table_header(lt.opt_meas_headers());
            let rows = self
                .measurements
                .iter()
                // NOTE this will force-convert all fields in the time
                // measurement, which for this is actually want we want
                .map(|r| {
                    r.both(
                        |t| {
                            let v = M::Optical::from_temporal_unchecked(t.value.clone());
                            (v.0, Some(&t.key))
                        },
                        |o| (o.value.clone(), M::Name::as_opt(&o.key)),
                    )
                })
                .zip(req_layout)
                .zip(opt_layout)
                .enumerate()
                .map(|(i, (((v, n), lr), lo))| v.table_row(i.into(), n, lr, lo));
            once(header).chain(rows).map(|r| r.join(delim)).collect()
        } else {
            vec![]
        }
    }

    #[cfg(feature = "serde")]
    pub(crate) fn print_meas_table(&self, delim: &str)
    where
        M::Temporal: Clone,
        M::Optical: OpticalFromTemporal<M::Temporal> + Clone,
    {
        for e in self.meas_table(delim) {
            println!("{e}");
        }
    }

    fn split_nonstandard(
        par: Par,
        nonstd: &mut NonStdKeywords,
        conf: &ReadStdKeywordsConfig,
    ) -> Success<Vec<NonStdKeywords>, (), Option<NonStdMeasRegexError>> {
        // Use nonstandard measurement pattern to assign keyvals to their
        // measurement if they match. Only capture one warning because if the
        // pattern is wrong for one measurement it is probably wrong for all of
        // them.
        let blank_meas_nonstd = || vec![HashMap::new(); par.0];
        conf.nonstandard_measurement_pattern
            .as_ref()
            .map_or(Ok(blank_meas_nonstd()), |pat| {
                // match largest indices first to avoid matching incomplete
                // prefix (ie "P1" will match "P11")
                (0..par.0)
                    .rev()
                    .map(|n| {
                        pat.apply_index(n).map(|p| {
                            let r: &Regex = p.as_ref();
                            nonstd.extract_if(|k, _| r.is_match(k.as_ref())).collect()
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map(|mut ns| {
                        // reverse again to put in order
                        ns.reverse();
                        ns
                    })
            })
            .into_succ_or(blank_meas_nonstd())
    }

    #[allow(clippy::type_complexity)]
    fn lookup_names<C>(
        std: &mut StdKeywords,
        nonstd: &mut [NonStdKeywords],
        conf: &C,
    ) -> WarningsAndErrorsResult<
        (Vec<M::Name>, Vec<Option<Shortname>>),
        (),
        OptIndexedKeyError<Shortname>,
        LookupShortnameError,
    >
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadStdKeywordsConfig>,
        M: LookupMetaroot,
        M::Name: Pointed<Shortname>,
        Version: From<M::Ver>,
    {
        nonstd
            .iter_mut()
            .enumerate()
            .map(|(n, meas_nonstd)| {
                let i = n.into();
                M::lookup_shortname(std, meas_nonstd, i, conf.as_ref()).into_semigroup()
            })
            .sequence_commutative()
            .map_ok_value(|mut names| {
                let sconf: &ReadStdKeywordsConfig = conf.as_ref();
                if sconf.dedup_measurement_names.is_set() {
                    let original = uniquify_names(&mut names[..]);
                    (names, original)
                } else {
                    let mut original = vec![];
                    original.resize_with(names.len(), || None);
                    (names, original)
                }
            })
    }

    #[allow(clippy::type_complexity)]
    fn lookup_measurements<C>(
        std: &mut StdKeywords,
        names: Vec<M::Name>,
        nonstd: Vec<NonStdKeywords>,
        conf: &C,
    ) -> LookupMeasurementResult<(
        NamedTemporalsAndOpticals<M>,
        Vec<AnyScaleDiagnostic>,
        Vec<(StdKey, String)>,
        Vec<(StdKey, String)>,
    )>
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadStdKeywordsConfig>,
        M: LookupMetaroot,
        M::Temporal: LookupTemporal,
        M::Optical: LookupOptical,
        M::Name: Pointed<Shortname>,
        Version: From<M::Ver>,
    {
        let sconf: &ReadStdKeywordsConfig = conf.as_ref();
        let mut found_time = false;

        let mut match_time_pattern = |i, wrapped| {
            let res = match M::Name::unwrap(wrapped) {
                Ok(name) => {
                    if let Some(tp) = sconf.time_meas_pattern.as_ref()
                        && tp.0.is_match(name.as_ref())
                    {
                        if found_time {
                            let e = DuplicateTimeNameError(i, name);
                            Err(LookupMeasurementError::from(e))
                        } else {
                            found_time = true;
                            Ok(Element::Center(name))
                        }
                    } else {
                        Ok(Element::NonCenter(M::Name::wrap(name)))
                    }
                }
                Err(key) => Ok(Element::NonCenter(key)),
            };
            res.into_log()
        };

        names
            .into_iter()
            .zip(nonstd)
            .enumerate()
            .map(|(i, (wrapped, meas_nonstd))| {
                let j = i.into();
                // If $PnN is found, check that it matches the time pattern (if
                // given). Also check that only zero or one $PnN match the time
                // pattern, and throw error otherwise.
                match_time_pattern(j, wrapped)
                    // Once we checked $PnN, pull all the rest of the
                    // standardized keywords from the hashtable and collect
                    // errors. In general, required keywords will trigger an
                    // error if they are missing and optional keywords will
                    // trigger a warning. Either can generate an error/warning
                    // if they fail to be parsed to their type
                    .and_then_commutative(|key| match key {
                        Element::Center(name) => {
                            Temporal::lookup_temporal(std, meas_nonstd, j, conf)
                                .map_errors(LookupMeasurementError::from)
                                .map_commutative_warnings(LookupMeasurementWarning::from)
                                .map_ok_value(|x| {
                                    let ret = Element::Center((name, x.this));
                                    (ret, x.scale.into(), x.trimmed, x.tmp_opt_pairs)
                                })
                        }
                        Element::NonCenter(k) => Optical::lookup_optical(std, j, meas_nonstd, conf)
                            .map_errors(LookupMeasurementError::from)
                            .map_commutative_warnings(LookupMeasurementWarning::from)
                            .map_ok_value(|x| {
                                let ret = Element::NonCenter((k, x.this));
                                (ret, x.scale.into(), x.trimmed, vec![])
                            }),
                    })
            })
            .sequence_commutative()
            .map_ok_value(|xs| {
                let mut ms = vec![];
                let mut ds = vec![];
                let mut trimmed = vec![];
                let mut tops = vec![];
                for (m, d, ps, ts) in xs {
                    ms.push(m);
                    ds.push(d);
                    trimmed.extend(ps);
                    tops.extend(ts);
                }
                (ms, ds, trimmed, tops)
            })
    }

    fn measurement_indexed_names(&self) -> HashMap<MeasIndex, &Shortname> {
        self.measurements.indexed_names().collect()
    }

    fn measurement_named_indices(&self) -> HashMap<&Shortname, MeasIndex> {
        self.measurements
            .indexed_names()
            .map(|(i, m)| (m, i))
            .collect()
    }

    fn all_indices_and_names_to_remove(&self) -> (IndicesToRemove, OpticalNamesToRemove<'_>) {
        let (js, ns): (HashSet<_>, HashSet<_>) =
            self.measurements.indexed_non_center_names().unzip();
        (js.into(), ns.into())
    }

    /// Check that links will not be broken when setting new measurement names.
    ///
    /// This is useful when setting the measurements in bulk and the names may
    /// change all at once.
    ///
    /// For named links, assume by default that new measurements are
    /// incompatible with old measurements (despite possibly sharing names) and
    /// thus any existing links are considered broken. If `allow_shared_names`
    /// is true, check that named links are within the new measurement names and
    /// return error if not. Do not include time when doing this since this
    /// cannot be linked.
    ///
    /// For indexed links, assume by default that new measurement order does not
    /// correspond to new measurement order, in which case any existing links
    /// will be broken. If `skip_index_check` is true, bypass this assumption
    /// and only check that the indices in the final result are valid. This
    /// should only be true when the user knows that measurements that have
    /// links are in the same order b/t new and old.
    ///
    /// The number of measurements is assumed to be correct; this should be
    /// checked elsewhere.
    fn new_meas_link_errors<X, Y>(
        &self,
        measurements: &Measurements<M::Name, X, Y>,
        allow_shared_names: bool,
        skip_index_check: bool,
    ) -> Result<(), SetMeasurementLinkErrors> {
        debug_assert!(
            self.measurements.len() == measurements.len(),
            "measurement vector are not same length"
        );
        let (js, ns) = self.all_indices_and_names_to_remove();
        let m = &self.metaroot;
        let s = &m.specific;
        let named_errs: Vec<_> = if allow_shared_names {
            // If name sharing is allowed, treat this as if keywords that have
            // references ($SPILLOVER, etc) are being added to the new
            // measurement, in which case we only need to ensure that links in
            // the final configuration match
            let nset = measurements.named_set();
            m.invalid_named_links(&nset)
                .map(SetMeasurementLinkError::from)
                .collect()
        } else {
            // If name sharing is not allowed, act as if all measurement will be
            // unset and replaced in two discrete steps, and any existing links
            // will be broken after the unset step. This effectively means we
            // can't have any links.
            m.meas_has_existing_named_links_with(&ns)
                .map(SetMeasurementLinkError::from)
                .collect()
        };
        let par = self.par();
        let index_errs: Vec<_> = if skip_index_check {
            s.meas_invalid_indexed_links_inner(&par)
                .map(SetMeasurementLinkError::from)
                .collect()
        } else {
            s.meas_has_existing_index_links_with_inner(par, &js)
                .map(SetMeasurementLinkError::from)
                .collect()
        };
        SetMeasurementLinkErrors::try_new(named_errs.into_iter().chain(index_errs))
    }
}

impl<M: VersionedMetaroot> VersionedCoreTEXT<M> {
    #[allow(clippy::type_complexity)]
    pub(crate) fn new_from_keywords_with_offsets<C>(
        mut kws: ValidKeywords,
        segs: &NonDataSegments,
        st: &ReadState<C>,
    ) -> WarningsAndErrorsResult<
        (Self, StdTEXTDiagnostics, <M::Ver as Versioned>::Offsets),
        (),
        StdTEXTFromFlatTEXTWarning,
        StdTEXTFromFlatTEXTErrorInner,
    >
    where
        M: LookupMetaroot,
        M::Temporal: LookupTemporal,
        M::Optical: LookupOptical + AsScaleTransform,
        Version: From<M::Ver>,
        <M::Ver as Versioned>::Layout: VersionedDataLayout,
        C: AsRef<ReadStdKeywordsConfig> + AsRef<ReadDataKeywordsConfig> + AsRef<ReadOffsetConfig>,
    {
        // Lookup DATA/ANALYSIS offsets and $TOT; these are not stored in the
        // Core struct but they will be needed later for parsing DATA and
        // ANALYSIS, and processing these keywords now will make it easier to
        // determine if TEXT is totally standardized or not.
        let offsets_res =
            <M::Ver as Versioned>::Offsets::lookup(&mut kws.std, &mut kws.nonstd, segs, st)
                .map_commutative_warnings(StdTEXTFromFlatTEXTWarning::from)
                .map_errors(StdTEXTFromFlatTEXTErrorInner::from);

        Self::lookup_inner(kws, &st.conf)
            .zip_commutative(offsets_res)
            .map_ok_value(|((x, y), z)| (x, y, z))
    }

    /// Make a new CoreTEXT from flat keywords.
    ///
    /// Return any errors encountered, including missing required keywords,
    /// parse errors, and/or deprecation warnings.
    ///
    /// This will not process $TOT or $(BEGIN|END)(TEXT|DATA). If present these
    /// will trigger pseudostandard warnings.
    pub fn new_from_keywords<C>(
        kws: ValidKeywords,
        conf: &C,
    ) -> WarningsAndGroupResult<
        (Self, StdTEXTDiagnostics),
        StdTEXTFromFlatTEXTWarning,
        StdTEXTFromKeywordsError,
        CoreTEXTFromKeywordsSummary,
    >
    where
        M: LookupMetaroot,
        M::Temporal: LookupTemporal,
        M::Optical: LookupOptical + AsScaleTransform,
        Version: From<M::Ver>,
        <M::Ver as Versioned>::Layout: VersionedDataLayout,
        C: AsRef<ReadStdKeywordsConfig> + AsRef<ReadDataKeywordsConfig> + AsRef<ReadSharedConfig>,
    {
        Self::lookup_inner(kws, conf)
            .map_errors(StdTEXTFromKeywordsError::from)
            .group()
    }

    #[allow(clippy::too_many_lines)]
    fn lookup_inner<C>(
        mut kws: ValidKeywords,
        conf: &C,
    ) -> WarningsAndErrorsResult<
        (Self, StdTEXTDiagnostics),
        (),
        StdTEXTFromFlatTEXTWarning,
        StdTEXTFromFlatTEXTErrorInner,
    >
    where
        M: LookupMetaroot,
        M::Temporal: LookupTemporal,
        M::Optical: LookupOptical + AsScaleTransform,
        Version: From<M::Ver>,
        <M::Ver as Versioned>::Layout: VersionedDataLayout,
        C: AsRef<ReadStdKeywordsConfig> + AsRef<ReadDataKeywordsConfig>,
    {
        // Lookup $PAR first since we need this to get the measurements
        let par_res = Par::remove_metaroot_req(&mut kws.std)
            .map_err(LookupMetarootError::from)
            .map_err(StdTEXTFromFlatTEXTErrorInner::from)
            .into_log();

        let version = M::Ver::fcs_version();
        let sconf: &ReadStdKeywordsConfig = conf.as_ref();

        par_res.and_then_commutative(|par| {
            let std = &mut kws.std;
            let nonstd = &mut kws.nonstd;
            // Split nonstandard measurements using pattern (if given); this
            // implicitly will encode $PAR downstream via length
            let nonstd_succ = Self::split_nonstandard(par, nonstd, conf.as_ref());
            let mut core_res = WarningsAndErrorsResult::Succ(nonstd_succ.repack())
                .map_commutative_warnings(StdTEXTFromFlatTEXTWarning::from)
                // Lookup $PnN and layout (which are independent of each other)
                .and_then_commutative(|mut meas_nonstd| {
                    Self::lookup_names(std, &mut meas_nonstd[..], conf)
                        .map_commutative_warnings(StdTEXTFromFlatTEXTWarning::from)
                        .map_errors(StdTEXTFromFlatTEXTErrorInner::from)
                        .map_ok_value(|n| (n, meas_nonstd))
                })
                // Lookup root and measurement keywords (which depend on $PnN)
                // and layout
                .and_then_commutative(|((dedup_names, original_names), mut meas_nonstd)| {
                    let mnsks = &mut meas_nonstd[..];
                    let layout_res =
                        <M::Ver as Versioned>::Layout::lookup(std, mnsks, conf.as_ref())
                            .map_commutative_warnings(StdTEXTFromFlatTEXTWarning::from)
                            .map_errors(StdTEXTFromFlatTEXTErrorInner::from);

                    let root_res =
                        Metaroot::lookup_metaroot(std, &dedup_names[..], kws.nonstd, conf)
                            .map_commutative_warnings(StdTEXTFromFlatTEXTWarning::from)
                            .map_errors(StdTEXTFromFlatTEXTErrorInner::from);

                    let meas_res = Self::lookup_measurements(std, dedup_names, meas_nonstd, conf)
                        .map_commutative_warnings(StdTEXTFromFlatTEXTWarning::from)
                        .map_errors(StdTEXTFromFlatTEXTErrorInner::from);

                    root_res
                        .zip3_commutative(meas_res, layout_res)
                        .map_ok_value(|x| (x, original_names))
                })
                .and_then_commutative(
                    |((mut metaroot_out, meas_out, layout_out), original_names)| {
                        let (meas, meas_scale, meas_trimmed, tmp_opt_pairs) = meas_out;
                        metaroot_out.trimmed.extend(meas_trimmed);
                        Self::try_new(metaroot_out.this, meas, layout_out.layout, conf)
                            .map_commutative_warnings(StdTEXTFromFlatTEXTWarning::from)
                            .map_errors(StdTEXTFromFlatTEXTErrorInner::from)
                            .map_ok_value(|ret| {
                                (
                                    ret,
                                    original_names,
                                    meas_scale,
                                    metaroot_out.trimmed,
                                    tmp_opt_pairs,
                                )
                            })
                    },
                );

            // Push pseudostandard/unused warnings/errors
            // TODO fix gate arg
            let (mut extra, errors) =
                ExtraStdKeywords::split_keywords(kws.std, version, par, 10000.into());

            if let Some(t) = mem::take(&mut extra.timestep) {
                let flag = sconf.process_extra_timestep.0;
                core_res = core_res
                    .map_ok_value(|mut core| {
                        if flag.is_demote() {
                            core.0
                                .metaroot
                                .nonstandard_keywords
                                .insert_demoted(Timestep::std(), t);
                        }
                        core
                    })
                    .extend_warnings_or_errors3(
                        Some(TimestepFoundError),
                        |_v| (),
                        |_p| (),
                        StdTEXTFromFlatTEXTWarning::from,
                        StdTEXTFromFlatTEXTErrorInner::from,
                        flag.as_triflag(),
                    );
            }

            macro_rules! go_extra {
                ($proc:ident, $keyvals:ident, $errors:ident) => {
                    let flag: ProcessKeywordFailure = sconf.$proc.into();
                    core_res = core_res
                        .map_ok_value(|mut core| {
                            if flag.is_demote() {
                                for (k, v) in mem::take(&mut extra.$keyvals) {
                                    core.0.metaroot.nonstandard_keywords.insert_demoted(k, v);
                                }
                            }
                            core
                        })
                        .extend_warnings_or_errors3(
                            errors.$errors,
                            |_v| (),
                            |_p| (),
                            StdTEXTFromFlatTEXTWarning::from,
                            StdTEXTFromFlatTEXTErrorInner::from,
                            flag.as_triflag(),
                        );
                };
            }

            go_extra!(process_pseudostandard, pseudostandard, pseudo);
            go_extra!(process_hyper_par, hyper_par, hyper_par);
            go_extra!(process_hyper_par, hyper_gate, hyper_gate);
            go_extra!(process_other_version, other_version, other_version);

            core_res.map_ok_value(|(ret, original_names, scale, trimmed, tmp_opt_pairs)| {
                let d = StdTEXTDiagnostics::from_extra(
                    extra,
                    original_names,
                    scale,
                    trimmed,
                    tmp_opt_pairs,
                );
                (ret, d)
            })
        })
    }

    /// Remove a measurement matching the given name.
    ///
    /// Return removed measurement and its index if found.
    pub fn remove_measurement_by_name(
        &mut self,
        n: &Shortname,
    ) -> Result<(MeasIndex, TemporalOrOptical<M>, Range), RemoveMeasByNameError> {
        self.remove_measurement_by_name_inner(n)
    }

    /// Remove a measurement at a given position
    ///
    /// Return removed measurement and its name if found.
    pub fn remove_measurement_by_index(
        &mut self,
        index: MeasIndex,
    ) -> Result<(NamedTemporalOrOptical<M>, Range), RemoveMeasByIndexError> {
        self.remove_measurement_by_index_inner(index)
    }

    /// Add time measurement to the end of the measurement vector.
    ///
    /// Return error if time measurement already exists or name is non-unique.
    pub fn push_temporal(
        &mut self,
        n: Shortname,
        m: Temporal<M::Temporal>,
        r: Range,
        disallow_trunc: DisallowRangeTrunc,
    ) -> WarningAndGroupResult<(), InsertRangeError, PushTemporalError, PushTemporalSummary> {
        self.push_temporal_inner(n, m, r, disallow_trunc).group()
    }

    /// Add time measurement at the given position
    ///
    /// Return error if time measurement already exists, name is non-unique, or
    /// index is out of bounds.
    pub fn insert_temporal(
        &mut self,
        i: MeasIndex,
        n: Shortname,
        m: Temporal<M::Temporal>,
        r: Range,
        disallow_trunc: DisallowRangeTrunc,
    ) -> WarningAndGroupResult<(), InsertRangeError, InsertTemporalError, InsertTemporalSummary>
    {
        self.insert_temporal_inner(i, n, m, r, disallow_trunc)
            .group()
    }

    /// Add optical measurement to the end of the measurement vector
    ///
    /// Return error if name is non-unique.
    pub fn push_optical(
        &mut self,
        n: M::Name,
        m: Optical<M::Optical>,
        r: Range,
        disallow_trunc: DisallowRangeTrunc,
    ) -> WarningAndGroupResult<Shortname, InsertRangeError, PushOpticalError, PushOpticalSummary>
    {
        self.push_optical_inner(n, m, r, disallow_trunc).group()
    }

    /// Add optical measurement at a given position
    ///
    /// Return error if name is non-unique, or index is out of bounds.
    pub fn insert_optical(
        &mut self,
        i: MeasIndex,
        n: M::Name,
        m: Optical<M::Optical>,
        r: Range,
        disallow_trunc: DisallowRangeTrunc,
    ) -> WarningAndGroupResult<Shortname, InsertRangeError, InsertOpticalError, InsertOpticalSummary>
    {
        self.insert_optical_inner(i, n, m, r, disallow_trunc)
            .group()
    }

    /// Remove measurements
    pub fn unset_measurements(&mut self) -> Result<(), ExistingLinkErrors> {
        self.unset_measurements_inner()
    }

    /// Make new CoreDataset from CoreTEXT with supplied DATA and ANALYSIS
    ///
    /// Number of columns must match number of measurements and must all be the
    /// same length.
    pub fn into_coredataset(
        self,
        df: FCSDataFrame,
        analysis: Analysis,
        others: Others,
    ) -> Result<VersionedCoreDataset<M>, MeasDataMismatchError> {
        let data_n = df.ncols();
        let meas_n = self.par().0;
        if data_n != meas_n {
            return Err(MeasDataMismatchError { meas_n, data_n });
        }
        Ok(self.into_coredataset_unchecked(df, analysis, others))
    }

    pub(crate) fn into_coredataset_unchecked(
        self,
        data: FCSDataFrame,
        analysis: Analysis,
        others: Others,
    ) -> VersionedCoreDataset<M> {
        CoreDataset::new(
            self.metaroot,
            self.measurements,
            self.layout,
            data,
            analysis,
            others,
        )
    }

    fn deprecated(
        &mut self,
        dep_flag: DisallowDeprecated,
        xfer_flag: ProcessOptionalFailure,
    ) -> SwitchableErrorsResult<(), (), DisallowDeprecated, AnyDepKeyError>
    where
        Version: From<M::Ver>,
    {
        let mut es = vec![];
        // Demote deprecated keywords to nonstandard if a) we consider it an
        // error if a deprecated key is present and b) if when we drop and
        // optional flag we are to transfer it to the nonstandard dict. If (a)
        // is not true, we don't care (only a warning), if (b) is not true, the
        // transfer shouldn't happen
        //
        // NOTE the drop_optional flag should not be used here because the
        // disallow_deprecated flag effectively takes its place. If this flag is
        // set, the we consider it an error to be deprecated, thus dropping a
        // keyval is not relevant (error = crash).
        let keep = xfer_flag.is_demote();
        let disallow_dep = matches!(dep_flag.0, TriFlag::True | TriFlag::Silent);
        let do_demote = disallow_dep && xfer_flag.is_demote();
        for mut d in self.metaroot.specific.deprecated(private::NoTouchy) {
            if do_demote {
                d.demote(&mut self.metaroot.nonstandard_keywords, keep);
            }
            d.errors(&mut es);
        }
        for (i, e) in self.measurements.iter_mut().enumerate() {
            match e {
                Element::Center(t) => {
                    for mut d in t.specific.deprecated(i.into()) {
                        if do_demote {
                            d.demote(&mut t.common.nonstandard_keywords, keep);
                        }
                        d.errors(&mut es);
                    }
                }
                Element::NonCenter(o) => o.deprecated(i.into(), &mut es, keep, do_demote),
            }
        }
        LogResult::new_switchable_iter3((), (), es, dep_flag)
    }

    // only meant to be called during lookup when keywords are being read from
    // a hashtable
    pub(crate) fn try_new<C>(
        mut metaroot: Metaroot<M>,
        measurements: NamedTemporalsAndOpticals<M>,
        layout: <M::Ver as Versioned>::Layout,
        conf: &C,
    ) -> WarningsAndErrorsResult<Self, (), NewCoreWarning, LookupCoreError>
    where
        M::Optical: AsScaleTransform,
        Version: From<M::Ver>,
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadStdKeywordsConfig>,
    {
        // this should be true since the length of both is derived from $PAR
        debug_assert!(
            measurements.len() == layout.ncols(),
            "measurements and layout should be same length"
        );
        let rconf: &ReadDataKeywordsConfig = conf.as_ref();
        let sconf: &ReadStdKeywordsConfig = conf.as_ref();

        let go = |ms: &NamedVec<_, _, _>| {
            if let Some(pat) = sconf.time_meas_pattern.as_ref()
                && ms.as_center().is_none()
                && !ms.is_empty()
            {
                return Some(NewCoreWarning::from(MissingTime(pat.clone())));
            }
            None
        };

        let drop_flag = rconf.process_optional_failure;
        let missing_flag = sconf.allow_missing_time;
        Measurements::try_new(measurements)
            .map_err(LookupCoreError::from)
            .into_log()
            .eval_warning_or_error3(missing_flag, |_| (), |()| (), go)
            .and_then_commutative(|ms| {
                layout
                    .check_measurement_vector_nolen(&ms)
                    .map_err(LookupCoreError::from)
                    .into_log()
                    .set_ok_value(ms)
            })
            .and_then_commutative(|ms| {
                Self::check_relationships(&mut metaroot, &ms, drop_flag.is_demote())
                    .map_errors(NewCoreWarning::from)
                    .nowarn_into_switchable(drop_flag)
                    .switchable_into_commutative()
                    .map_errors(LookupCoreError::from)
                    .map_commutative_warnings(NewCoreWarning::from)
                    .map_ok_value(|()| Self::new(metaroot, ms, layout, (), (), ()))
                    .and_then_commutative(|mut ret| {
                        let xfer_flag = rconf.process_optional_failure;
                        let dep_flag = sconf.disallow_deprecated;
                        ret.deprecated(dep_flag, xfer_flag)
                            .map_switchable_errors(NewCoreWarning::from)
                            .switchable_into_commutative()
                            .map_errors(LookupCoreError::from)
                            .set_ok_value(ret)
                    })
            })
    }

    pub(crate) fn try_new_nodrop(
        mut metaroot: Metaroot<M>,
        measurements: NamedTemporalsAndOpticals<M>,
        layout: <M::Ver as Versioned>::Layout,
    ) -> ErrorsResult<Self, (), NewCoreError>
    where
        M::Optical: AsScaleTransform,
    {
        Measurements::try_new(measurements)
            .map_err(NewCoreError::from)
            .into_nowarn()
            .and_then_commutative(|ms| {
                layout
                    .check_measurement_vector(&ms)
                    .map_err(NewCoreError::from)
                    .into_nowarn()
                    .set_ok_value(ms)
            })
            .and_then_commutative(|ms| {
                Self::check_relationships(&mut metaroot, &ms, false)
                    .map_errors(NewCoreWarning::from)
                    .map_errors(NewCoreError::from)
                    .map_ok_value(|()| Self::new(metaroot, ms, layout, (), (), ()))
            })
    }

    /// Check for invalid keyword relationships.
    ///
    /// For example, $SPILLOVER in the metaroot must refer to valid
    /// measurements.
    ///
    /// If allow_dropping is true, remove keywords with invalid relationships.
    fn check_relationships(
        metaroot: &mut Metaroot<M>,
        measurements: &Measurements<M::Name, M::Temporal, M::Optical>,
        allow_drop: bool,
    ) -> ErrorsResult<(), (), BrokenOrDependentLinkError>
    where
        M::Optical: AsScaleTransform,
    {
        let ns = measurements.named_set();
        let par = Par(measurements.len());
        let link_errs = metaroot.remove_invalid_links(par, &ns, allow_drop);
        LogResult::new_err_from_iter(link_errs, ())
    }

    fn new_unchecked(
        metaroot: Metaroot<M>,
        measurements: Measurements<M::Name, M::Temporal, M::Optical>,
        layout: <M::Ver as Versioned>::Layout,
    ) -> Self {
        Self::new(metaroot, measurements, layout, (), (), ())
    }
}

impl<M> VersionedCoreDataset<M>
where
    M: VersionedMetaroot,
    <M::Ver as Versioned>::Layout: VersionedDataLayout,
{
    pub fn new_from_keywords<C>(
        p: &PathBuf,
        kws: ValidKeywords,
        data_seg: RelativeSegment<DataSegmentId>,
        analysis_seg: RelativeSegment<AnalysisSegmentId>,
        other_segs: RelativeOtherSegments,
        dataset_offset: DatasetOffset,
        conf: &C,
    ) -> WarningsAndIOGroupResult<
        (Self, StdDatasetWithKwsOutput),
        StdDatasetFromFlatTEXTWarning,
        StdDatasetFromFlatTextError,
        StdDatasetWithKwsSummary,
    >
    where
        M: LookupMetaroot,
        M::Temporal: LookupTemporal,
        M::Optical: LookupOptical + AsScaleTransform,
        Version: From<M::Ver>,
        <M::Ver as Versioned>::Offsets: AsRef<DatasetSegments>,
        C: AsRef<ReadStdKeywordsConfig>
            + AsRef<ReadOffsetConfig>
            + AsRef<ReadDataKeywordsConfig>
            + AsRef<ReadEventsConfig>
            + AsRef<ReadSharedConfig>,
    {
        ReadState::open(p, dataset_offset, conf)
            .map_err(|e| e.fmap_once(StdDatasetFromFlatTextErrorInner::from))
            .map_err(IOErrorGroup::from)
            .into_log()
            .and_then_commutative(|(st, file)| {
                let data_res = data_seg
                    .relative_to_abs(dataset_offset, st.file_len)
                    .into_nowarn();
                let anal_res = analysis_seg
                    .relative_to_abs(dataset_offset, st.file_len)
                    .into_nowarn();
                let oss_res = if let Some((segs, width)) = other_segs {
                    segs.into_iter()
                        .map(|s| s.relative_to_abs(dataset_offset, st.file_len).into_log())
                        .sequence_commutative()
                        .map_ok_value(|xs| Some((NonEmpty::from_vec(xs).unwrap(), width)))
                } else {
                    LogResult::new_ok(None)
                };
                data_res
                    .zip3_commutative(anal_res, oss_res)
                    .map_errors(StdDatasetFromFlatTextErrorInner::from)
                    .map_ok_value(|(d, a, o)| (d, a, o, st, file))
                    .nowarn_into_warn()
                    .group()
                    .map_error(IOErrorGroup::Pure)
            })
            .and_then_commutative(|(d, a, os, st, file)| {
                let segs = NonDataSegments::new_no_text(d, a, os);
                let mut h = BufReader::new(file);
                Self::new_from_keywords_inner(&mut h, kws, &segs, &st)
            })
            .warnings_to_pure_errors(*conf.as_ref(), StdDatasetFromFlatTextErrorInner::from)
            .map_pure_errors(StdDatasetFromFlatTextError::from)
            .deanonymize()
    }

    pub(crate) fn new_from_keywords_inner<C, R>(
        h: &mut BufReader<R>,
        kws: ValidKeywords,
        segs: &NonDataSegments,
        st: &ReadState<C>,
    ) -> WarningsAndIOGroupResult<
        (Self, StdDatasetWithKwsOutput),
        StdDatasetFromFlatTEXTWarning,
        StdDatasetFromFlatTextErrorInner,
        (),
    >
    where
        R: Read + Seek,
        M: LookupMetaroot,
        M::Temporal: LookupTemporal,
        M::Optical: LookupOptical + AsScaleTransform,
        Version: From<M::Ver>,
        <M::Ver as Versioned>::Offsets: AsRef<DatasetSegments>,
        C: AsRef<ReadStdKeywordsConfig>
            + AsRef<ReadOffsetConfig>
            + AsRef<ReadDataKeywordsConfig>
            + AsRef<ReadEventsConfig>,
    {
        VersionedCoreTEXT::<M>::new_from_keywords_with_offsets(kws, segs, st)
            .map_commutative_warnings(StdDatasetFromFlatTEXTWarning::from)
            .map_errors(StdDatasetFromFlatTextErrorInner::from)
            .group()
            .map_error(IOErrorGroup::Pure)
            .and_then_commutative(|(text, extra, offsets)| {
                let dataset_segs = offsets.as_ref();
                let os: Vec<_> = segs.header.as_others().copied().collect();
                let or = OthersReader::new(&os[..]);
                let ar = AnalysisReader::new(dataset_segs.analysis);
                let read_conf: &ReadEventsConfig = st.conf.as_ref();
                text.layout
                    .h_read_df(h, offsets.tot(), dataset_segs.data, read_conf)
                    .map_commutative_warnings(StdDatasetFromFlatTEXTWarning::from)
                    .map_pure_errors(StdDatasetFromFlatTextErrorInner::from)
                    .and_then_commutative(|(data, event_out)| {
                        ar.h_read(h)
                            .and_then(|analysis| {
                                let others = or.h_read(h)?;
                                let c = text.into_coredataset_unchecked(data, analysis, others);
                                let out =
                                    StdDatasetWithKwsOutput::new(*dataset_segs, extra, event_out);
                                Ok((c, out))
                            })
                            .map_err(IOErrorGroup::from)
                            .into_log()
                    })
            })
    }

    pub fn write_datasets(
        path: &PathBuf,
        cores: &[Self],
        conf: &WriteDatasetInnerConfig,
    ) -> WarningsAndIOGroupResult<
        Option<Nextdata>,
        IndexedLossError,
        StdWriterError,
        WriteDatasetSummary,
    >
    where
        Version: From<M::Ver>,
    {
        let n = cores.len();
        let mut results = vec![];
        for (i, c) in cores.iter().enumerate() {
            let appendable = AppendableFlag::from(i + 1 < n);
            let append = AppendFlag(i > 0);
            let multi = WriteMultiConfig::new(appendable, append);
            let sconf = WriteMultiDatasetConfig::new(*conf, multi);
            let succ = split_log!(c.write_dataset(path, &sconf));
            results.push(succ);
        }
        let mut it = results.into_iter();
        if let Some(r0) = it.by_ref().next() {
            let ret = it.fold(r0, |acc, r| acc.lift_f2_once(r, |_, nd| nd));
            LogResult::Succ(ret.fmap_once(Some))
        } else {
            LogResult::new_ok_default()
        }
    }

    /// Write this core structure (HEADER+TEXT) to a file path
    pub fn write_dataset(
        &self,
        path: &PathBuf,
        conf: &WriteMultiDatasetConfig,
    ) -> WarningsAndIOGroupResult<Nextdata, IndexedLossError, StdWriterError, WriteDatasetSummary>
    where
        Version: From<M::Ver>,
    {
        let opts = conf.multi.append.file_options();
        let f = io_to_log!(opts.open(path));
        let mut h = BufWriter::new(f);
        self.h_write_dataset(&mut h, &conf.inner, conf.multi.appendable)
    }

    /// Write this dataset (HEADER+TEXT+DATA+ANALYSIS+OTHER) to a handle
    pub fn h_write_dataset<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        conf: &WriteDatasetInnerConfig,
        has_nextdata: AppendableFlag,
    ) -> WarningsAndIOGroupResult<Nextdata, IndexedLossError, StdWriterError, WriteDatasetSummary>
    where
        Version: From<M::Ver>,
    {
        let df = &self.data;
        let layout = &self.layout;
        let delim = conf.text.delim;
        let tot = Tot(df.nrows());
        let analysis_len =
            u64::try_from(self.analysis.0.len()).expect("ANALYSIS segment length exceeds 2^64");
        let others = &self.others.0[..];

        let check_res = if conf.skip_conversion_check.is_set() {
            LogResult::new_ok(())
        } else {
            layout.check_writer(df)
        };

        check_res
            .map_errors(StdWriterError::from)
            .nowarn_into_warn()
            .group()
            .map_error(IOErrorGroup::Pure)
            // write HEADER+TEXT+OTHER(s) first
            .and_then_commutative(|()| {
                let data_len = layout.nbytes(df);
                let ht_conf = WriteHeaderAndTextConfig {
                    delim,
                    tot,
                    data_len,
                    analysis_len,
                    other_segs: others,
                    has_nextdata,
                };
                let res = if conf.text.big_other.is_set() {
                    self.h_write_text_inner::<_, UintSpacePad20>(h, &ht_conf)
                } else {
                    self.h_write_text_inner::<_, UintSpacePad8>(h, &ht_conf)
                };
                res.map_err(|e| e.fmap_once(StdWriterError::from))
                    .map_err(IOErrorGroup::from)
                    .into_log()
            })
            // write DATA; conversion check flag is flipped from above since
            // we want to emit warnings as we are writing if we did not run
            // through the data once at the beginning and check for
            // conversion loss.
            .and_commutative(|| {
                let flag = !conf.skip_conversion_check.is_set();
                layout.h_write_df(h, df, flag).map_error(IOErrorGroup::from)
            })
            // write ANALYSIS
            .and_commutative(|| {
                io_to_log!(h.write_all(&self.analysis.0));
                LogResult::new_ok(())
            })
            .deanonymize()
    }

    /// Return reference to DATA segment as dataframe.
    pub fn data(&self) -> &FCSDataFrame {
        &self.data
    }

    /// Return reference to ANALYSIS segment as byte string.
    pub fn analysis(&self) -> &Analysis {
        &self.analysis
    }

    /// Return mutable reference to ANALYSIS segment as byte string.
    pub fn analysis_mut(&mut self) -> &mut Analysis {
        &mut self.analysis
    }

    /// Return reference to OTHER segments as byte strings.
    pub fn others(&self) -> &Others {
        &self.others
    }

    /// Return mutable reference to OTHER segments as byte strings.
    pub fn others_mut(&mut self) -> &mut Others {
        &mut self.others
    }

    /// Add columns to this dataset.
    ///
    /// Return error if columns are not all the same length or number of columns
    /// doesn't match the number of measurement.
    pub fn set_data(&mut self, df: FCSDataFrame) -> Result<(), ColumnsToDataframeError> {
        let data_n = df.ncols();
        let meas_n = self.par().0;
        if data_n != meas_n {
            return Err(MeasDataMismatchError { meas_n, data_n }.into());
        }
        self.data = df;
        Ok(())
    }

    /// Remove all measurements and data
    pub fn unset_data(&mut self) -> Result<(), ExistingLinkErrors> {
        self.unset_measurements_inner()?;
        self.data.clear();
        Ok(())
    }

    /// Coerce all values in DATA to fit within types specified in layout.
    ///
    /// If `skip_conv_check` is `false`, also return warnings for truncation;
    /// otherwise truncation is performed silently.
    ///
    /// This will copy the entire dataframe regardless of whether or not the
    /// data needs to be truncated. This will hopefully be fixed in the future.
    pub fn truncate_data(&mut self, skip_conv_check: bool) -> WarningsResult<(), IndexedLossError> {
        // TODO this function is hilariously not-optimized; each column will be
        // cast into a totally new vector even if they are they exact same
        // type with no possible truncation. This also means that the new
        // dataframe will be totally separate from the old one. Unfortunately,
        // the best fix for this requires specialization, since we need a way
        // to tell rust to do nothing when the input and output types match and
        // otherwise do something else.
        self.layout
            .truncate_df(&self.data, skip_conv_check)
            .fmap_once(|data| self.data = data)
    }

    /// Remove a measurement matching the given name.
    ///
    /// Return removed measurement and its index if found.
    pub fn remove_measurement_by_name(
        &mut self,
        n: &Shortname,
    ) -> Result<(MeasIndex, TemporalOrOptical<M>, AnyFCSColumn, Range), RemoveMeasByNameError> {
        let (index, meas, rng) = self.remove_measurement_by_name_inner(n)?;
        let col = self.data.drop_in_place(index.into()).unwrap();
        Ok((index, meas, col, rng))
    }

    /// Remove a measurement at a given position
    ///
    /// Return removed measurement and its name if found.
    pub fn remove_measurement_by_index(
        &mut self,
        index: MeasIndex,
    ) -> Result<(NamedTemporalOrOptical<M>, AnyFCSColumn, Range), RemoveMeasByIndexError> {
        let (meas, rng) = self.remove_measurement_by_index_inner(index)?;
        let col = self.data.drop_in_place(index.into()).unwrap();
        Ok((meas, col, rng))
    }

    /// Add time measurement to the end of the measurement vector.
    ///
    /// Return error if time measurement already exists or name is non-unique.
    pub fn push_temporal(
        &mut self,
        n: Shortname,
        m: Temporal<M::Temporal>,
        col: AnyFCSColumn,
        r: Range,
        disallow_trunc: DisallowRangeTrunc,
    ) -> WarningAndGroupResult<(), InsertRangeError, PushTemporalToDatasetError, PushTemporalSummary>
    {
        self.data
            .check_new_column(&col)
            .map_err(PushTemporalToDatasetError::from)
            .into_nowarn()
            .nowarn_and_then(|()| {
                self.push_temporal_inner(n, m, r, disallow_trunc)
                    .map_errors(PushTemporalToDatasetError::from)
            })
            .when_ok(|| self.data.push_column_nocheck(col))
            .group()
    }

    /// Add time measurement at the given position
    ///
    /// Return error if time measurement already exists, name is non-unique, or
    /// index is out of bounds.
    pub fn insert_temporal(
        &mut self,
        i: MeasIndex,
        n: Shortname,
        m: Temporal<M::Temporal>,
        col: AnyFCSColumn,
        r: Range,
        disallow_trunc: DisallowRangeTrunc,
    ) -> WarningAndGroupResult<
        (),
        InsertRangeError,
        InsertTemporalToDatasetError,
        InsertTemporalSummary,
    > {
        self.data
            .check_new_column(&col)
            .map_err(InsertTemporalToDatasetError::from)
            .into_nowarn()
            .nowarn_and_then(|()| {
                self.insert_temporal_inner(i, n, m, r, disallow_trunc)
                    .map_errors(InsertTemporalToDatasetError::from)
            })
            .when_ok(|| {
                self.data.insert_column_nocheck(i.into(), col);
            })
            .group()
    }

    /// Add measurement to the end of the measurement vector
    ///
    /// Return error if name is non-unique.
    pub fn push_optical(
        &mut self,
        n: M::Name,
        m: Optical<M::Optical>,
        col: AnyFCSColumn,
        r: Range,
        disallow_trunc: DisallowRangeTrunc,
    ) -> WarningAndGroupResult<
        Shortname,
        InsertRangeError,
        PushOpticalToDatasetError,
        PushOpticalSummary,
    > {
        self.data
            .check_new_column(&col)
            .map_err(PushOpticalToDatasetError::from)
            .into_nowarn()
            .nowarn_and_then(|()| {
                self.push_optical_inner(n, m, r, disallow_trunc)
                    .map_errors(PushOpticalToDatasetError::from)
            })
            .when_ok(|| self.data.push_column_nocheck(col))
            .group()
    }

    /// Add measurement at a given position
    ///
    /// Return error if name is non-unique, or index is out of bounds.
    pub fn insert_optical(
        &mut self,
        i: MeasIndex,
        n: M::Name,
        m: Optical<M::Optical>,
        col: AnyFCSColumn,
        r: Range,
        disallow_trunc: DisallowRangeTrunc,
    ) -> WarningAndGroupResult<
        Shortname,
        InsertRangeError,
        InsertOpticalInDatasetError,
        InsertOpticalSummary,
    > {
        self.data
            .check_new_column(&col)
            .map_err(InsertOpticalInDatasetError::from)
            .into_nowarn()
            .nowarn_and_then(|()| {
                self.insert_optical_inner(i, n, m, r, disallow_trunc)
                    .map_errors(InsertOpticalInDatasetError::from)
            })
            .when_ok(|| self.data.insert_column_nocheck(i.into(), col))
            .group()
    }

    /// Convert this struct into [`CoreTEXT`].
    ///
    /// This simply entails taking ownership and dropping the ANALYSIS and DATA
    /// fields.
    pub fn into_coretext(self) -> VersionedCoreTEXT<M> {
        CoreTEXT::new_unchecked(self.metaroot, self.measurements, self.layout)
    }

    /// Set measurements and dataframe together
    ///
    /// Length of measurements must match the width of the input dataframe.
    pub fn set_named_measurements_and_data(
        &mut self,
        xs: NamedTemporalsAndOpticals<M>,
        df: FCSDataFrame,
        allow_shared_names: bool,
        skip_index_check: bool,
    ) -> Result<(), SetMeasurementsAndDataError>
    where
        M::Optical: AsScaleTransform,
    {
        let meas_n = xs.len();
        let data_n = df.ncols();
        if meas_n != data_n {
            return Err(MeasDataMismatchError { meas_n, data_n }.into());
        }
        self.set_named_measurements_inner(xs, allow_shared_names, skip_index_check)?;
        self.data = df;
        Ok(())
    }

    /// Set measurements without $PnN and dataframe together
    ///
    /// Length of measurements must match the width of the input dataframe.
    pub fn set_measurements_and_data(
        &mut self,
        measurements: TemporalsAndOpticals<M>,
        df: FCSDataFrame,
    ) -> Result<(), SetUnnamdMeasurementsAndDataError>
    where
        M::Optical: AsScaleTransform,
    {
        let meas_n = measurements.len();
        let data_n = df.ncols();
        if meas_n != data_n {
            return Err(MeasDataMismatchError { meas_n, data_n }.into());
        }
        self.set_measurements_inner(measurements)?;
        self.data = df;
        Ok(())
    }

    /// Set measurements without $PnN, layout, and dataframe together
    ///
    /// Length of measurements must match the width of the input dataframe.
    pub fn set_measurements_layout_and_data(
        &mut self,
        measurements: TemporalsAndOpticals<M>,
        layout: <M::Ver as Versioned>::Layout,
        df: FCSDataFrame,
    ) -> Result<(), SetUnnamdMeasurementsAndDataError>
    where
        M::Optical: AsScaleTransform,
    {
        let meas_n = measurements.len();
        let data_n = df.ncols();
        if meas_n != data_n {
            return Err(MeasDataMismatchError { meas_n, data_n }.into());
        }
        self.set_measurements_and_layout_inner(measurements, layout)?;
        self.data = df;
        Ok(())
    }
}

impl HasCompensation for InnerMetaroot2_0 {
    type Comp = Compensation2_0;

    fn comp_mut(&mut self, _: private::NoTouchy) -> &mut Option<Self::Comp> {
        &mut self.comp
    }
}

impl HasCompensation for InnerMetaroot3_0 {
    type Comp = Compensation3_0;

    fn comp_mut(&mut self, _: private::NoTouchy) -> &mut Option<Self::Comp> {
        &mut self.comp
    }
}

impl HasSpillover for InnerMetaroot3_1 {
    fn spill_mut(&mut self, _: private::NoTouchy) -> &mut Option<Spillover> {
        &mut self.spillover
    }
}

impl HasSpillover for InnerMetaroot3_2 {
    fn spill_mut(&mut self, _: private::NoTouchy) -> &mut Option<Spillover> {
        &mut self.spillover
    }
}

impl HasUnstainedCenters for InnerMetaroot3_2 {
    fn unstainedcenters_mut(&mut self, _: private::NoTouchy) -> &mut UnstainedCenters {
        &mut self.unstained.unstainedcenters
    }
}

impl HasScale for InnerOptical2_0 {
    fn scale_mut(&mut self, _: private::NoTouchy) -> &mut Option<Scale> {
        &mut self.scale
    }
}

impl HasScaleTransform for InnerOptical3_0 {
    fn transform_mut(&mut self, _: private::NoTouchy) -> &mut ScaleTransform {
        &mut self.scale
    }
}

impl HasScaleTransform for InnerOptical3_1 {
    fn transform_mut(&mut self, _: private::NoTouchy) -> &mut ScaleTransform {
        &mut self.scale
    }
}

impl HasScaleTransform for InnerOptical3_2 {
    fn transform_mut(&mut self, _: private::NoTouchy) -> &mut ScaleTransform {
        &mut self.scale
    }
}

impl HasAppliedGates3_0 for InnerMetaroot3_0 {
    fn applied_gates3_0_mut(&mut self, _: private::NoTouchy) -> &mut AppliedGates3_0 {
        &mut self.applied_gates
    }
}

impl HasAppliedGates3_0 for InnerMetaroot3_1 {
    fn applied_gates3_0_mut(&mut self, _: private::NoTouchy) -> &mut AppliedGates3_0 {
        &mut self.applied_gates
    }
}

impl HasAppliedGates3_2 for InnerMetaroot3_2 {
    fn applied_gates3_2_mut(&mut self, _: private::NoTouchy) -> &mut AppliedGates3_2 {
        &mut self.applied_gates
    }
}

impl<M, A, D, O> VersionedCore<A, D, O, M>
where
    M: VersionedMetaroot<Name = Option<Shortname>>,
{
    /// Set all $PnN keywords to list of names.
    pub fn set_measurement_shortnames_maybe(
        &mut self,
        ns: Vec<Option<Shortname>>,
    ) -> Result<NameMapping, SetKeysError> {
        let mapping = self.measurements.set_keys(ns)?;
        self.metaroot.rename_meas_links(&mapping);
        Ok(mapping)
    }
}

impl CoreTEXT2_0 {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn try_new_2_0(
        measurements: TemporalsAndOpticals2_0,
        layout: DataLayout2_0,
        mode: Mode,
        cyt: Cyt,
        comp: Option<Compensation>,
        btim: Option<Btim<FCSTime>>,
        etim: Option<Etim<FCSTime>>,
        date: Option<FCSDate>,
        abrt: Option<Abrt>,
        com: Com,
        cells: Cells,
        exp: Exp,
        fil: Fil,
        inst: Inst,
        lost: Option<Lost>,
        op: Op,
        proj: Proj,
        smno: Smno,
        src: Src,
        sys: Sys,
        tr: Option<Trigger>,
        applied_gates: AppliedGates2_0,
        nonstandard_keywords: NonStdKeywords,
    ) -> ErrorsResult<Self, (), NewCoreTEXTError> {
        Timestamps::try_new(btim, etim, date)
            .map_errors(NewCoreTEXTError::from)
            .set_err_value(())
            .into_semigroup()
            .and_then_commutative(|ts| {
                let specific =
                    InnerMetaroot2_0::new(mode, cyt, comp.map(Into::into), ts, applied_gates);
                let metaroot = Metaroot::new(
                    abrt,
                    com,
                    cells,
                    exp,
                    fil,
                    inst,
                    lost,
                    op,
                    proj,
                    smno,
                    src,
                    sys,
                    tr,
                    specific,
                    nonstandard_keywords,
                );
                Self::try_new_nodrop(metaroot, measurements, layout)
                    .map_errors(NewCoreTEXTError::from)
            })
    }
}

impl CoreTEXT3_0 {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn try_new_3_0(
        measurements: TemporalsAndOpticals3_0,
        layout: DataLayout3_0,
        mode: Mode,
        cyt: Cyt,
        comp: Option<Compensation>,
        btim: Option<Btim<FCSTime60>>,
        etim: Option<Etim<FCSTime60>>,
        date: Option<FCSDate>,
        cytsn: Cytsn,
        unicode: Option<Unicode>,
        csvbits: CSVBits,
        cstot: CSTot,
        csvflags: CSVFlags,
        abrt: Option<Abrt>,
        com: Com,
        cells: Cells,
        exp: Exp,
        fil: Fil,
        inst: Inst,
        lost: Option<Lost>,
        op: Op,
        proj: Proj,
        smno: Smno,
        src: Src,
        sys: Sys,
        tr: Option<Trigger>,
        applied_gates: AppliedGates3_0,
        nonstandard_keywords: NonStdKeywords,
    ) -> ErrorsResult<Self, (), NewCoreTEXTError> {
        let subset = SubsetData::new(csvbits, cstot, csvflags);
        Timestamps::try_new(btim, etim, date)
            .map_errors(NewCoreTEXTError::from)
            .set_err_value(())
            .into_semigroup()
            .and_then_commutative(|ts| {
                let specific = InnerMetaroot3_0::new(
                    mode,
                    cyt,
                    comp.map(Into::into),
                    ts,
                    cytsn,
                    unicode,
                    subset,
                    applied_gates,
                );
                let metaroot = Metaroot::new(
                    abrt,
                    com,
                    cells,
                    exp,
                    fil,
                    inst,
                    lost,
                    op,
                    proj,
                    smno,
                    src,
                    sys,
                    tr,
                    specific,
                    nonstandard_keywords,
                );
                Self::try_new_nodrop(metaroot, measurements, layout)
                    .map_errors(NewCoreTEXTError::from)
            })
    }
}

impl CoreTEXT3_1 {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn try_new_3_1(
        measurements: TemporalsAndOpticals3_1,
        layout: DataLayout3_1,
        mode: Mode,
        cyt: Cyt,
        btim: Option<Btim<FCSTime100>>,
        etim: Option<Etim<FCSTime100>>,
        date: Option<FCSDate>,
        cytsn: Cytsn,
        spillover: Option<Spillover>,
        last_modifier: LastModifier,
        last_mod_date: Option<LastModified>,
        originality: Option<Originality>,
        plateid: Plateid,
        platename: Platename,
        wellid: Wellid,
        vol: Option<Vol>,
        csvbits: CSVBits,
        cstot: CSTot,
        csvflags: CSVFlags,
        abrt: Option<Abrt>,
        com: Com,
        cells: Cells,
        exp: Exp,
        fil: Fil,
        inst: Inst,
        lost: Option<Lost>,
        op: Op,
        proj: Proj,
        smno: Smno,
        src: Src,
        sys: Sys,
        tr: Option<Trigger>,
        applied_gates: AppliedGates3_0,
        nonstandard_keywords: NonStdKeywords,
    ) -> ErrorsResult<Self, (), NewCoreTEXTError> {
        let subset = SubsetData::new(csvbits, cstot, csvflags);
        Timestamps::try_new(btim, etim, date)
            .map_errors(NewCoreTEXTError::from)
            .set_err_value(())
            .into_semigroup()
            .and_then_commutative(|ts| {
                let specific = InnerMetaroot3_1::new(
                    mode,
                    cyt,
                    ts,
                    cytsn,
                    spillover,
                    ModificationData::new(last_modifier, last_mod_date, originality),
                    PlateData::new(plateid, platename, wellid),
                    vol,
                    subset,
                    applied_gates,
                );
                let metaroot = Metaroot::new(
                    abrt,
                    com,
                    cells,
                    exp,
                    fil,
                    inst,
                    lost,
                    op,
                    proj,
                    smno,
                    src,
                    sys,
                    tr,
                    specific,
                    nonstandard_keywords,
                );
                Self::try_new_nodrop(metaroot, measurements, layout)
                    .map_errors(NewCoreTEXTError::from)
            })
    }
}

impl CoreTEXT3_2 {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn try_new_3_2(
        measurements: TemporalsAndOpticals3_2,
        layout: DataLayout3_2,
        cyt: Cyt3_2,
        mode: Option<Mode3_2>,
        btim: Option<Btim<FCSTime100>>,
        etim: Option<Etim<FCSTime100>>,
        date: Option<FCSDate>,
        begindatetime: Option<BeginDateTime>,
        enddatetime: Option<EndDateTime>,
        cytsn: Cytsn,
        spillover: Option<Spillover>,
        last_modifier: LastModifier,
        last_mod_date: Option<LastModified>,
        originality: Option<Originality>,
        plateid: Plateid,
        platename: Platename,
        wellid: Wellid,
        vol: Option<Vol>,
        carrierid: Carrierid,
        carriertype: Carriertype,
        locationid: Locationid,
        unstainedinfo: UnstainedInfo,
        unstainedcenters: UnstainedCenters,
        flowrate: Flowrate,
        abrt: Option<Abrt>,
        com: Com,
        cells: Cells,
        exp: Exp,
        fil: Fil,
        inst: Inst,
        lost: Option<Lost>,
        op: Op,
        proj: Proj,
        smno: Smno,
        src: Src,
        sys: Sys,
        tr: Option<Trigger>,
        applied_gates: AppliedGates3_2,
        nonstandard_keywords: NonStdKeywords,
    ) -> ErrorsResult<Self, (), NewCoreTEXTError> {
        let ts_res = Timestamps::try_new(btim, etim, date)
            .map_errors(NewCoreTEXTError::from)
            .into_semigroup();
        let dt_res = Datetimes::try_new(begindatetime, enddatetime)
            .map_errors(NewCoreTEXTError::from)
            .into_semigroup();
        ts_res
            .zip_commutative(dt_res)
            .and_then_commutative(|(ts, dt)| {
                let specific = InnerMetaroot3_2::new(
                    mode,
                    ts,
                    dt,
                    cyt,
                    spillover,
                    cytsn,
                    ModificationData::new(last_modifier, last_mod_date, originality),
                    PlateData::new(plateid, platename, wellid),
                    vol,
                    CarrierData::new(carrierid, carriertype, locationid),
                    UnstainedData::new(unstainedcenters, unstainedinfo),
                    flowrate,
                    applied_gates,
                );
                let metaroot = Metaroot::new(
                    abrt,
                    com,
                    cells,
                    exp,
                    fil,
                    inst,
                    lost,
                    op,
                    proj,
                    smno,
                    src,
                    sys,
                    tr,
                    specific,
                    nonstandard_keywords,
                );
                Self::try_new_nodrop(metaroot, measurements, layout)
                    .map_errors(NewCoreTEXTError::from)
            })
    }
}

impl UnstainedData {
    fn lookup<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        conf: &C,
    ) -> DeferredSwitchableError<
        DiagnosedUnstainedData,
        DummyTriFlag,
        OptKeyStError<UnstainedCenters>,
    >
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadStdKeywordsConfig>,
    {
        let i = UnstainedInfo::remove_root_opt_nofail(std);
        UnstainedCenters::remove_or_drop_root_opt_with(std, nonstd, (), conf).map_deferred_value(
            |out| {
                let (c, t) = out.into_root_pair();
                DiagnosedUnstainedData::new(Self::new(c, i), t)
            },
        )
    }

    fn opt_keywords(&self) -> impl Iterator<Item = (String, String)> {
        [
            self.unstainedcenters.metaroot_opt_pair(),
            self.unstainedinfo.metaroot_opt_pair(),
        ]
        .into_iter()
        .filter_map(|(k, v)| v.map(|x| (k, x)))
    }

    fn loss_errors(&self) -> impl Iterator<Item = UnstainedLossError> {
        let a = self.unstainedcenters.root_key_loss_error();
        let b = self.unstainedinfo.root_key_loss_error();
        [a, b].into_iter().flatten()
    }
}

impl SubsetData {
    fn lookup(
        kws: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        conf: &ReadDataKeywordsConfig,
    ) -> DeferredWarningsAndErrors<Self, LookupSubsetError, LookupSubsetError> {
        let f =
            CSVFlags::lookup(kws, nonstd, conf).map_warnings_and_errors(LookupSubsetError::from);
        let b = CSVBits::remove_or_drop_root_opt(kws, nonstd, conf)
            .map_switchable_errors(LookupSubsetError::from)
            .switchable_into_commutative()
            .into_semigroup();
        let t = CSTot::remove_or_drop_root_opt(kws, nonstd, conf)
            .map_switchable_errors(LookupSubsetError::from)
            .switchable_into_commutative()
            .into_semigroup();
        f.lift_f3_once(b, t, |flags, bits, tot| Self::new(bits, tot, flags))
    }

    fn opt_keywords(&self) -> impl Iterator<Item = (String, String)> {
        [self.bits.metaroot_opt_pair(), self.tot.metaroot_opt_pair()]
            .into_iter()
            .filter_map(|(k, v)| v.map(|x| (k, x)))
            .chain(self.flags.opt_keywords())
    }

    fn loss_errors(&self) -> impl Iterator<Item = SubsetLossError> {
        let es = self.bits.root_key_loss_error();
        self.flags
            .loss_errors()
            .map(SubsetLossError::from)
            .chain(es)
    }
}

impl CSVFlags {
    fn lookup(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        conf: &ReadDataKeywordsConfig,
    ) -> DeferredWarningsAndErrors<Self, LookupCSVFlagsError, LookupCSVFlagsError> {
        CSMode::remove_or_drop_root_opt(std, nonstd, conf)
            .map_switchable_errors(LookupCSVFlagsError::from)
            .switchable_into_commutative()
            .into_semigroup()
            .and_then_deferred(|m| {
                // NOTE the standard seems to say that these flags are only
                // required if the user wishes to encode a subset value using
                // 0 as the identifier. This is in contrast to the paper it
                // references (Redelman and Coder 1994) which seems to say
                // they are required. Either way, I'm still not sure how these
                // were ever supposed to be used. Good luck ;)
                let n = m.map(|x| x.0).unwrap_or_default();
                (0..n)
                    .map(|i| {
                        CSVFlag::remove_or_drop_meas_opt(std, nonstd, i, conf)
                            .map_switchable_errors(LookupCSVFlagsError::from)
                            .switchable_into_commutative()
                            .into_semigroup()
                    })
                    .sequence_def()
            })
            .map_deferred_value(Self)
    }

    fn opt_keywords(&self) -> impl Iterator<Item = (String, String)> {
        let m = (!self.0.is_empty()).then(|| CSMode(self.0.len()).root_pair());
        self.0
            .iter()
            .enumerate()
            .map(|(i, f)| f.meas_opt_pair(i))
            .filter_map(|(k, v)| v.map(|x| (k, x)))
            .chain(m)
    }

    fn loss_errors(&self) -> impl Iterator<Item = CSVFlagsLossError> {
        let e = (!self.0.is_empty()).then_some(UnitaryKeyLossError::<CSMode>::default().into());
        let go = |(i, f): (usize, &Option<_>)| f.indexed_key_loss_error(i);
        self.0.iter().enumerate().filter_map(go).chain(e)
    }
}

impl ModificationData {
    fn lookup<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        conf: &C,
    ) -> DeferredWarningsAndErrors<Self, LookupModifiedDataError, LookupModifiedDataError>
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadStdKeywordsConfig>,
    {
        let last_mod = LastModifier::remove_root_opt_nofail(std);
        let last_mod_date = LastModified::remove_or_drop_root_opt_with(std, nonstd, (), conf)
            .map_switchable_errors(LookupModifiedDataError::from)
            .switchable_into_commutative()
            .into_semigroup();
        let ori = Originality::remove_or_drop_root_opt(std, nonstd, conf.as_ref())
            .map_switchable_errors(LookupModifiedDataError::from)
            .switchable_into_commutative()
            .into_semigroup();
        last_mod_date.lift_f2_once(ori, |d, o| Self::new(last_mod, d.native, o))
    }

    fn opt_keywords(&self) -> impl Iterator<Item = (String, String)> {
        [
            self.last_modifier.metaroot_opt_pair(),
            self.last_modified.metaroot_opt_pair(),
            self.originality.metaroot_opt_pair(),
        ]
        .into_iter()
        .filter_map(|(k, v)| v.map(|x| (k, x)))
    }

    fn loss_errors(&self) -> impl Iterator<Item = ModificationLossError> {
        let a = self.last_modified.root_key_loss_error();
        let b = self.last_modifier.root_key_loss_error();
        let c = self.originality.root_key_loss_error();
        [a, b, c].into_iter().flatten()
    }
}

impl CarrierData {
    fn lookup(kws: &mut StdKeywords) -> Self {
        let l = Locationid::remove_root_opt_nofail(kws);
        let i = Carrierid::remove_root_opt_nofail(kws);
        let t = Carriertype::remove_root_opt_nofail(kws);
        Self::new(i, t, l)
    }

    fn opt_keywords(&self) -> impl Iterator<Item = (String, String)> {
        let a = self.carrierid.metaroot_opt_pair();
        let b = self.carriertype.metaroot_opt_pair();
        let c = self.locationid.metaroot_opt_pair();
        [a, b, c].into_iter().filter_map(|(k, v)| v.map(|x| (k, x)))
    }

    fn loss_errors(&self) -> impl Iterator<Item = CarrierLossError> {
        let a = self.carrierid.root_key_loss_error();
        let b = self.carriertype.root_key_loss_error();
        let c = self.locationid.root_key_loss_error();
        [a, b, c].into_iter().flatten()
    }
}

impl PlateData {
    fn lookup(kws: &mut StdKeywords) -> Self {
        let w = Wellid::remove_root_opt_nofail(kws);
        let n = Platename::remove_root_opt_nofail(kws);
        let i = Plateid::remove_root_opt_nofail(kws);
        Self::new(i, n, w)
    }

    fn deprecated(&mut self) -> impl Iterator<Item = DeprecatedPlateRef<'_>> {
        let a = DeprecatedPlateRef::from(DeprecatedStrRef(&mut self.platename));
        let b = DeprecatedPlateRef::from(DeprecatedStrRef(&mut self.plateid));
        let c = DeprecatedPlateRef::from(DeprecatedStrRef(&mut self.wellid));
        [a, b, c].into_iter()
    }

    fn opt_keywords(&self) -> impl Iterator<Item = (String, String)> {
        [
            self.wellid.metaroot_opt_pair(),
            self.platename.metaroot_opt_pair(),
            self.plateid.metaroot_opt_pair(),
        ]
        .into_iter()
        .filter_map(|(k, v)| v.map(|x| (k, x)))
    }

    fn loss_errors(self) -> impl Iterator<Item = PlateLossError> {
        let a = self.platename.root_key_loss_error();
        let b = self.plateid.root_key_loss_error();
        let c = self.wellid.root_key_loss_error();
        [a, b, c].into_iter().flatten()
    }
}

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

    fn deprecated(&mut self, i: MeasIndex) -> impl Iterator<Item = DeprecatedPeakRef<'_>> {
        let j = i.into();
        let a = DeprecatedPeakRef::from(IndexedDepRef::new(j, &mut self.size));
        let b = DeprecatedPeakRef::from(IndexedDepRef::new(j, &mut self.bin));
        [a, b].into_iter()
    }

    pub(crate) fn opt_keywords(
        &self,
        i: MeasIndex,
    ) -> impl Iterator<Item = (MeasHeader, String, Option<String>)> {
        [self.bin.meas_opt_triple(i), self.size.meas_opt_triple(i)].into_iter()
    }

    fn loss_errors(&self, i: MeasIndex) -> impl Iterator<Item = PeakLossError> {
        let a = self.bin.indexed_key_loss_error(i);
        let b = self.size.indexed_key_loss_error(i);
        [a, b].into_iter().flatten()
    }
}

impl ConvertFromOptical<InnerOptical3_0> for InnerOptical2_0 {
    fn convert_from_optical(
        value: InnerOptical3_0,
        i: MeasIndex,
        flag: AllowLoss,
    ) -> OpticalConvertResult<Self> {
        ScaleTransform::try_convert_to_scale(value.scale, i)
            .map_errors(AnyOpticalKeyLossError::from)
            .map_error(OpticalConvertWarning::from)
            .nowarn_into_switchable3(flag)
            .switchable_into_commutative()
            .map_error(OpticalConvertError::from)
            .map_ok_value(|scale| Self::new(Some(scale), value.wavelength, value.peak))
            .set_err_value(())
            .repack()
    }
}

impl ConvertFromOptical<InnerOptical3_1> for InnerOptical2_0 {
    fn convert_from_optical(
        value: InnerOptical3_1,
        i: MeasIndex,
        flag: AllowLoss,
    ) -> OpticalConvertResult<Self> {
        let cal = value.calibration.indexed_key_loss_error(i);
        let dpy = value.display.indexed_key_loss_error(i);
        let check_errs = [cal, dpy].into_iter().flatten();

        let wave = value
            .wavelengths
            .into_wavelength(i)
            .map_errors(OpticalConvertWarning::from)
            .repack_errors::<Vec<_>>();
        let xform = ScaleTransform::try_convert_to_scale(value.scale, i)
            .map_errors(AnyOpticalKeyLossError::from)
            .repack_errors::<Vec<_>>()
            .extend_deferred_errors(check_errs)
            .map_errors(OpticalConvertWarning::from)
            .repack_errors::<Vec<_>>();

        xform
            .lift_f2_once(wave, |s, w| Self::new(Some(s), w, value.peak))
            .nowarn_into_switchable3(flag)
            .switchable_into_commutative()
            .map_errors(OpticalConvertError::from)
            .set_err_value(())
    }
}

impl ConvertFromOptical<InnerOptical3_2> for InnerOptical2_0 {
    fn convert_from_optical(
        value: InnerOptical3_2,
        i: MeasIndex,
        flag: AllowLoss,
    ) -> OpticalConvertResult<Self> {
        let cal = value.calibration.indexed_key_loss_error(i);
        let dpy = value.display.indexed_key_loss_error(i);
        let anal = value.analyte.indexed_key_loss_error(i);
        let feat = value.feature.indexed_key_loss_error(i);
        let meas = value.measurement_type.indexed_key_loss_error(i);
        let tag = value.tag.indexed_key_loss_error(i);
        let det_name = value.detector_name.indexed_key_loss_error(i);
        let check_errs = [cal, dpy, anal, feat, meas, tag, det_name]
            .into_iter()
            .flatten();

        let xform = ScaleTransform::try_convert_to_scale(value.scale, i)
            .map_errors(AnyOpticalKeyLossError::from)
            .repack_errors::<Vec<_>>()
            .extend_deferred_errors(check_errs)
            .map_errors(OpticalConvertWarning::from)
            .repack_errors::<Vec<_>>();
        let wave = value
            .wavelengths
            .into_wavelength(i)
            .map_errors(OpticalConvertWarning::from)
            .repack_errors::<Vec<_>>();

        xform
            .lift_f2_once(wave, |s, w| Self::new(Some(s), w, PeakData::default()))
            .nowarn_into_switchable3(flag)
            .switchable_into_commutative()
            .map_errors(OpticalConvertError::from)
            .set_err_value(())
    }
}

impl ConvertFromOptical<InnerOptical2_0> for InnerOptical3_0 {
    fn convert_from_optical(
        value: InnerOptical2_0,
        i: MeasIndex,
        _: AllowLoss,
    ) -> OpticalConvertResult<Self> {
        value
            .scale
            .ok_or(NoScaleError(i).into())
            .map(|s| Self::new(s, value.wavelength, value.peak))
            .into_log()
    }
}

impl ConvertFromOptical<InnerOptical3_1> for InnerOptical3_0 {
    fn convert_from_optical(
        value: InnerOptical3_1,
        i: MeasIndex,
        flag: AllowLoss,
    ) -> OpticalConvertResult<Self> {
        let cal = value.calibration.indexed_key_loss_error(i);
        let dpy = value.display.indexed_key_loss_error(i);
        let check_errs = [cal, dpy]
            .into_iter()
            .flatten()
            .map(OpticalConvertWarning::Xfer);

        value
            .wavelengths
            .into_wavelength(i)
            .map_errors(OpticalConvertWarning::from)
            .repack_errors::<Vec<_>>()
            .extend_deferred_errors(check_errs)
            .nowarn_into_switchable3(flag)
            .switchable_into_commutative()
            .map_errors(OpticalConvertError::from)
            .set_err_value(())
            .map_ok_value(|w| Self::new(value.scale, w, value.peak))
    }
}

impl ConvertFromOptical<InnerOptical3_2> for InnerOptical3_0 {
    fn convert_from_optical(
        value: InnerOptical3_2,
        i: MeasIndex,
        flag: AllowLoss,
    ) -> OpticalConvertResult<Self> {
        let cal = value.calibration.indexed_key_loss_error(i);
        let dpy = value.display.indexed_key_loss_error(i);
        let anal = value.analyte.indexed_key_loss_error(i);
        let feat = value.feature.indexed_key_loss_error(i);
        let meas = value.measurement_type.indexed_key_loss_error(i);
        let tag = value.tag.indexed_key_loss_error(i);
        let det_name = value.detector_name.indexed_key_loss_error(i);

        let check_errs = [cal, dpy, anal, feat, meas, tag, det_name]
            .into_iter()
            .flatten()
            .map(OpticalConvertWarning::Xfer);

        value
            .wavelengths
            .into_wavelength(i)
            .map_errors(OpticalConvertWarning::from)
            .repack_errors::<Vec<_>>()
            .extend_deferred_errors(check_errs)
            .nowarn_into_switchable3(flag)
            .switchable_into_commutative()
            .map_errors(OpticalConvertError::from)
            .set_err_value(())
            .map_ok_value(|w| Self::new(value.scale, w, PeakData::default()))
    }
}

impl ConvertFromOptical<InnerOptical2_0> for InnerOptical3_1 {
    fn convert_from_optical(
        value: InnerOptical2_0,
        i: MeasIndex,
        _: AllowLoss,
    ) -> OpticalConvertResult<Self> {
        let wave = value.wavelength.map(Wavelengths::from).unwrap_or_default();
        value
            .scale
            .ok_or(NoScaleError(i).into())
            .map(|s| Self::new(s, wave, None, None, value.peak))
            .into_log()
    }
}

impl ConvertFromOptical<InnerOptical3_0> for InnerOptical3_1 {
    fn convert_from_optical(
        value: InnerOptical3_0,
        _: MeasIndex,
        _: AllowLoss,
    ) -> OpticalConvertResult<Self> {
        let wave = value.wavelength.map(Wavelengths::from).unwrap_or_default();
        LogResult::new_ok(Self::new(value.scale, wave, None, None, value.peak))
    }
}

impl ConvertFromOptical<InnerOptical3_2> for InnerOptical3_1 {
    fn convert_from_optical(
        value: InnerOptical3_2,
        i: MeasIndex,
        flag: AllowLoss,
    ) -> OpticalConvertResult<Self> {
        let anal = value.analyte.indexed_key_loss_error(i);
        let feat = value.feature.indexed_key_loss_error(i);
        let meas = value.measurement_type.indexed_key_loss_error(i);
        let tag = value.tag.indexed_key_loss_error(i);
        let det_name = value.detector_name.indexed_key_loss_error(i);

        let check_errs = [anal, feat, meas, tag, det_name]
            .into_iter()
            .flatten()
            .map(OpticalConvertWarning::Xfer);

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

        SwitchableErrorsResult::new_deferred_switchable_iter3((), check_errs, flag)
            .switchable_into_commutative()
            .zip_commutative(cal_res)
            .map_errors(OpticalConvertError::from)
            .map_ok_value(|((), cal)| {
                Self::new(
                    value.scale,
                    value.wavelengths,
                    cal,
                    value.display,
                    PeakData::default(),
                )
            })
    }
}

impl ConvertFromOptical<InnerOptical2_0> for InnerOptical3_2 {
    fn convert_from_optical(
        value: InnerOptical2_0,
        i: MeasIndex,
        flag: AllowLoss,
    ) -> OpticalConvertResult<Self> {
        let wave = value.wavelength.map(Wavelengths::from).unwrap_or_default();
        let scale_res = value.scale.ok_or(NoScaleError(i).into()).into_log();
        let es = value
            .peak
            .loss_errors(i)
            .map(AnyOpticalKeyLossError::from)
            .map(OpticalConvertWarning::from);
        SwitchableErrorsResult::new_deferred_switchable_iter3((), es, flag)
            .switchable_into_commutative()
            .map_errors(OpticalConvertError::from)
            .zip_commutative(scale_res)
            .map_ok_value(|((), s)| {
                Self::new(
                    s,
                    wave,
                    None,
                    None,
                    Analyte::default(),
                    None,
                    OpticalType::default(),
                    Tag::default(),
                    DetectorName::default(),
                )
            })
    }
}

impl ConvertFromOptical<InnerOptical3_0> for InnerOptical3_2 {
    fn convert_from_optical(
        value: InnerOptical3_0,
        i: MeasIndex,
        flag: AllowLoss,
    ) -> OpticalConvertResult<Self> {
        let wave = value.wavelength.map(Wavelengths::from).unwrap_or_default();
        let es = value
            .peak
            .loss_errors(i)
            .map(AnyOpticalKeyLossError::from)
            .map(OpticalConvertWarning::from);
        SwitchableErrorsResult::new_deferred_switchable_iter3((), es, flag)
            .switchable_into_commutative()
            .map_errors(OpticalConvertError::from)
            .map_ok_value(|()| {
                Self::new(
                    value.scale,
                    wave,
                    None,
                    None,
                    Analyte::default(),
                    None,
                    OpticalType::default(),
                    Tag::default(),
                    DetectorName::default(),
                )
            })
    }
}

impl ConvertFromOptical<InnerOptical3_1> for InnerOptical3_2 {
    fn convert_from_optical(
        value: InnerOptical3_1,
        i: MeasIndex,
        flag: AllowLoss,
    ) -> OpticalConvertResult<Self> {
        let es = value
            .peak
            .loss_errors(i)
            .map(AnyOpticalKeyLossError::from)
            .map(OpticalConvertWarning::from);
        SwitchableErrorsResult::new_deferred_switchable_iter3((), es, flag)
            .switchable_into_commutative()
            .map_errors(OpticalConvertError::from)
            .map_ok_value(|()| {
                Self::new(
                    value.scale,
                    value.wavelengths,
                    value.calibration.map(Into::into),
                    value.display,
                    Analyte::default(),
                    None,
                    OpticalType::default(),
                    Tag::default(),
                    DetectorName::default(),
                )
            })
    }
}

macro_rules! impl_ref {
    ($outer:ident, $inner:ident) => {
        impl AsRef<$inner> for $outer<$inner> {
            fn as_ref(&self) -> &$inner {
                &self.specific
            }
        }

        impl AsMut<$inner> for $outer<$inner> {
            fn as_mut(&mut self) -> &mut $inner {
                &mut self.specific
            }
        }
    };
}

impl_ref!(Metaroot, InnerMetaroot2_0);
impl_ref!(Metaroot, InnerMetaroot3_0);
impl_ref!(Metaroot, InnerMetaroot3_1);
impl_ref!(Metaroot, InnerMetaroot3_2);

impl_ref!(Optical, InnerOptical2_0);
impl_ref!(Optical, InnerOptical3_0);
impl_ref!(Optical, InnerOptical3_1);
impl_ref!(Optical, InnerOptical3_2);

impl_ref!(Temporal, InnerTemporal2_0);
impl_ref!(Temporal, InnerTemporal3_0);
impl_ref!(Temporal, InnerTemporal3_1);
impl_ref!(Temporal, InnerTemporal3_2);

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

impl_ref_specific_rw!(
    Metaroot,
    InnerMetaroot2_0,
    Mode,
    Cyt,
    Timestamps2_0,
    AppliedGates2_0
);

impl_ref_specific_rw!(
    Metaroot,
    InnerMetaroot3_0,
    Mode,
    Cyt,
    Cytsn,
    Option<Unicode>,
    CSVBits,
    CSTot,
    CSVFlags,
    Timestamps3_0
);

impl_ref_specific_rw!(
    Metaroot,
    InnerMetaroot3_1,
    Mode,
    Cyt,
    Cytsn,
    LastModifier,
    Option<LastModified>,
    Option<Originality>,
    Plateid,
    Wellid,
    Platename,
    Option<Vol>,
    CSVBits,
    CSTot,
    CSVFlags,
    Timestamps3_1
);

impl_ref_specific_rw!(
    Metaroot,
    InnerMetaroot3_2,
    Cyt3_2,
    Datetimes,
    Option<Mode3_2>,
    Cytsn,
    LastModifier,
    Option<LastModified>,
    Option<Originality>,
    Plateid,
    Wellid,
    Platename,
    Carrierid,
    Carriertype,
    Locationid,
    Option<Vol>,
    Flowrate,
    UnstainedInfo,
    Timestamps3_1
);

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
    TemporalScale2_0,
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

impl_ref_specific_ro!(
    Metaroot,
    InnerMetaroot2_0,
    Option<FCSDate>,
    Option<Compensation2_0>
);

impl_ref_specific_ro!(
    Metaroot,
    InnerMetaroot3_0,
    Option<FCSDate>,
    Option<Compensation3_0>,
    AppliedGates3_0
);

impl_ref_specific_ro!(Metaroot, InnerMetaroot3_1, Option<FCSDate>, AppliedGates3_0);

impl_ref_specific_ro!(
    Metaroot,
    InnerMetaroot3_2,
    Option<FCSDate>,
    Option<BeginDateTime>,
    Option<EndDateTime>,
    UnstainedCenters,
    AppliedGates3_2
);

impl_ref_specific_ro!(Optical, InnerOptical2_0, Option<Scale>);

impl_ref_specific_ro!(Optical, InnerOptical3_0, ScaleTransform);

impl_ref_specific_ro!(Optical, InnerOptical3_1, ScaleTransform);

impl_ref_specific_ro!(Optical, InnerOptical3_2, ScaleTransform);

impl<X, M, const IS_ETIM: bool> AsRef<Option<Xtim<IS_ETIM, X>>> for Metaroot<M>
where
    Self: AsRef<Timestamps<X>>,
    Timestamps<X>: AsRef<Option<Xtim<IS_ETIM, X>>>,
{
    fn as_ref(&self) -> &Option<Xtim<IS_ETIM, X>> {
        self.as_ref().as_ref()
    }
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
            .ok_or_else(|| NameConversionError(Key1::new_i1(i.into())))
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

impl ConvertFromMetaroot<InnerMetaroot3_0> for InnerMetaroot2_0 {
    fn convert_from_metaroot(
        value: InnerMetaroot3_0,
        flag: AllowLoss,
    ) -> MetarootConvertResult<Self> {
        let c = value.cytsn.root_key_loss_error();
        let u = value.unicode.root_key_loss_error();
        let s = value
            .subset
            .loss_errors()
            .map(AnyMetarootKeyLossError::from);
        let es = [c, u].into_iter().flatten().chain(s);
        let check_res = SwitchableErrorsResult::new_deferred_switchable_iter3((), es, flag)
            .switchable_into_commutative()
            .map_commutative_warnings(MetarootConvertWarning::from)
            .map_errors(MetarootConvertError::from);
        let ag_res = value
            .applied_gates
            .try_into_2_0(flag)
            .switchable_into_commutative()
            .map_commutative_warnings(MetarootConvertWarning::from)
            .map_errors(MetarootConvertError::from);
        check_res.zip_commutative(ag_res).map_ok_value(|((), ag)| {
            Self::new(
                value.mode,
                value.cyt,
                value.comp.map(|x| x.0.into()),
                value.timestamps.map(Into::into),
                ag,
            )
        })
    }
}

impl ConvertFromMetaroot<InnerMetaroot3_1> for InnerMetaroot2_0 {
    fn convert_from_metaroot(
        value: InnerMetaroot3_1,
        flag: AllowLoss,
    ) -> MetarootConvertResult<Self> {
        macro_rules! loss_errors {
            ($x:expr) => {
                $x.loss_errors().map(AnyMetarootKeyLossError::from)
            };
        }
        let cytsn = value.cytsn.root_key_loss_error();
        let vol = value.vol.root_key_loss_error();
        let spill = value.spillover.root_key_loss_error();
        let plate = loss_errors!(value.plate);
        let subset = loss_errors!(value.subset);
        let modi = loss_errors!(value.modification);
        let es = [cytsn, vol, spill]
            .into_iter()
            .flatten()
            .chain(plate)
            .chain(subset)
            .chain(modi);
        let check_res = SwitchableErrorsResult::new_deferred_switchable_iter3((), es, flag)
            .switchable_into_commutative()
            .map_commutative_warnings(MetarootConvertWarning::from)
            .map_errors(MetarootConvertError::from);
        let ag_res = value
            .applied_gates
            .try_into_2_0(flag)
            .switchable_into_commutative()
            .map_commutative_warnings(MetarootConvertWarning::from)
            .map_errors(MetarootConvertError::from);
        let ts = value.timestamps.map(Into::into);
        check_res
            .zip_commutative(ag_res)
            .map_ok_value(|((), ag)| Self::new(value.mode, value.cyt, None, ts, ag))
    }
}

impl ConvertFromMetaroot<InnerMetaroot3_2> for InnerMetaroot2_0 {
    fn convert_from_metaroot(
        value: InnerMetaroot3_2,
        flag: AllowLoss,
    ) -> MetarootConvertResult<Self> {
        macro_rules! loss_errors {
            ($x:expr) => {
                $x.loss_errors().map(AnyMetarootKeyLossError::from)
            };
        }
        let cytsn = value.cytsn.root_key_loss_error();
        let vol = value.vol.root_key_loss_error();
        let spill = value.spillover.root_key_loss_error();
        let flow = value.flowrate.root_key_loss_error();
        let modi = loss_errors!(value.modification);
        let plate = loss_errors!(value.plate);
        let dt = loss_errors!(value.datetimes);
        let carrier = loss_errors!(value.carrier);
        let us = loss_errors!(value.unstained);
        let ag = loss_errors!(value.applied_gates);

        let es = [cytsn, vol, spill, flow]
            .into_iter()
            .flatten()
            .chain(modi)
            .chain(plate)
            .chain(dt)
            .chain(carrier)
            .chain(us)
            .chain(ag);
        SwitchableErrorsResult::new_deferred_switchable_iter3((), es, flag)
            .switchable_into_commutative()
            .map_commutative_warnings(MetarootConvertWarning::from)
            .map_errors(MetarootConvertError::from)
            .map_ok_value(|()| {
                Self::new(
                    Mode::List,
                    value.cyt,
                    None,
                    value.timestamps.map(Into::into),
                    AppliedGates2_0::default(),
                )
            })
    }
}

impl ConvertFromMetaroot<InnerMetaroot2_0> for InnerMetaroot3_0 {
    fn convert_from_metaroot(value: InnerMetaroot2_0, _: AllowLoss) -> MetarootConvertResult<Self> {
        LogResult::new_ok(Self::new(
            value.mode,
            value.cyt,
            value.comp.map(|x| x.0.into()),
            value.timestamps.map(Into::into),
            Cytsn::default(),
            None,
            SubsetData::default(),
            value.applied_gates,
        ))
    }
}

impl ConvertFromMetaroot<InnerMetaroot3_1> for InnerMetaroot3_0 {
    fn convert_from_metaroot(
        value: InnerMetaroot3_1,
        flag: AllowLoss,
    ) -> MetarootConvertResult<Self> {
        macro_rules! loss_errors {
            ($x:expr) => {
                $x.loss_errors().map(AnyMetarootKeyLossError::from)
            };
        }
        let plate = loss_errors!(value.plate);
        let modi = loss_errors!(value.modification);
        let vol = value.vol.root_key_loss_error();
        let es = vol.into_iter().chain(plate).chain(modi);
        SwitchableErrorsResult::new_deferred_switchable_iter3((), es, flag)
            .switchable_into_commutative()
            .map_commutative_warnings(MetarootConvertWarning::from)
            .map_errors(MetarootConvertError::from)
            .map_ok_value(|()| {
                Self::new(
                    value.mode,
                    value.cyt,
                    None,
                    value.timestamps.map(Into::into),
                    value.cytsn,
                    None,
                    SubsetData::default(),
                    value.applied_gates,
                )
            })
    }
}

impl ConvertFromMetaroot<InnerMetaroot3_2> for InnerMetaroot3_0 {
    fn convert_from_metaroot(
        value: InnerMetaroot3_2,
        flag: AllowLoss,
    ) -> MetarootConvertResult<Self> {
        macro_rules! loss_errors {
            ($x:expr) => {
                $x.loss_errors().map(AnyMetarootKeyLossError::from)
            };
        }
        let vol = value.vol.root_key_loss_error();
        let flow = value.flowrate.root_key_loss_error();
        let modi = loss_errors!(value.modification);
        let plate = loss_errors!(value.plate);
        let dt = loss_errors!(value.datetimes);
        let carrier = loss_errors!(value.carrier);
        let us = loss_errors!(value.unstained);
        let es = [vol, flow]
            .into_iter()
            .flatten()
            .chain(modi)
            .chain(plate)
            .chain(dt)
            .chain(carrier)
            .chain(us);
        SwitchableErrorsResult::new_deferred_switchable_iter3((), es, flag)
            .switchable_into_commutative()
            .map_commutative_warnings(MetarootConvertWarning::from)
            .map_errors(MetarootConvertError::from)
            .map_ok_value(|()| {
                Self::new(
                    Mode::List,
                    value.cyt,
                    None,
                    value.timestamps.map(Into::into),
                    value.cytsn,
                    None,
                    SubsetData::default(),
                    value.applied_gates,
                )
            })
    }
}

impl ConvertFromMetaroot<InnerMetaroot2_0> for InnerMetaroot3_1 {
    fn convert_from_metaroot(
        value: InnerMetaroot2_0,
        flag: AllowLoss,
    ) -> MetarootConvertResult<Self> {
        let es = value
            .comp
            .as_ref()
            .into_iter()
            .flat_map(Compensation2_0::loss_errors)
            .map(AnyMetarootKeyLossError::from);
        SwitchableErrorsResult::new_deferred_switchable_iter3((), es, flag)
            .switchable_into_commutative()
            .map_commutative_warnings(MetarootConvertWarning::from)
            .map_errors(MetarootConvertError::from)
            .map_ok_value(|()| {
                Self::new(
                    value.mode,
                    value.cyt,
                    value.timestamps.map(Into::into),
                    Cytsn::default(),
                    None,
                    ModificationData::default(),
                    PlateData::default(),
                    None,
                    SubsetData::default(),
                    value.applied_gates,
                )
            })
    }
}

impl ConvertFromMetaroot<InnerMetaroot3_0> for InnerMetaroot3_1 {
    fn convert_from_metaroot(
        value: InnerMetaroot3_0,
        flag: AllowLoss,
    ) -> MetarootConvertResult<Self> {
        let comp = value.comp.root_key_loss_error();
        let us = value.unicode.root_key_loss_error();
        let es = [comp, us].into_iter().flatten();
        SwitchableErrorsResult::new_deferred_switchable_iter3((), es, flag)
            .switchable_into_commutative()
            .map_commutative_warnings(MetarootConvertWarning::Loss)
            .map_errors(MetarootConvertError::from)
            .map_ok_value(|()| {
                Self::new(
                    value.mode,
                    value.cyt,
                    value.timestamps.map(Into::into),
                    value.cytsn,
                    None,
                    ModificationData::default(),
                    PlateData::default(),
                    None,
                    value.subset,
                    value.applied_gates,
                )
            })
    }
}

impl ConvertFromMetaroot<InnerMetaroot3_2> for InnerMetaroot3_1 {
    fn convert_from_metaroot(
        value: InnerMetaroot3_2,
        flag: AllowLoss,
    ) -> MetarootConvertResult<Self> {
        let dt = value
            .datetimes
            .loss_errors()
            .map(AnyMetarootKeyLossError::from);
        let carrier = value
            .carrier
            .loss_errors()
            .map(AnyMetarootKeyLossError::from);
        let us = value
            .unstained
            .loss_errors()
            .map(AnyMetarootKeyLossError::from);
        let flow = value.flowrate.root_key_loss_error();
        let es = flow.into_iter().chain(dt).chain(carrier).chain(us);
        SwitchableErrorsResult::new_deferred_switchable_iter3((), es, flag)
            .switchable_into_commutative()
            .map_commutative_warnings(MetarootConvertWarning::Loss)
            .map_errors(MetarootConvertError::from)
            .map_ok_value(|()| {
                Self::new(
                    Mode::List,
                    value.cyt,
                    value.timestamps,
                    value.cytsn,
                    value.spillover,
                    value.modification,
                    value.plate,
                    value.vol,
                    SubsetData::default(),
                    value.applied_gates,
                )
            })
    }
}

impl ConvertFromMetaroot<InnerMetaroot2_0> for InnerMetaroot3_2 {
    fn convert_from_metaroot(
        value: InnerMetaroot2_0,
        flag: AllowLoss,
    ) -> MetarootConvertResult<Self> {
        let comp = value
            .comp
            .as_ref()
            .into_iter()
            .flat_map(Compensation2_0::loss_errors)
            .map(AnyMetarootKeyLossError::from);
        let ag = value
            .applied_gates
            .loss_errors()
            .map(AnyMetarootKeyLossError::from);
        let es = comp.chain(ag);
        let check_res = SwitchableErrorsResult::new_deferred_switchable_iter3((), es, flag)
            .switchable_into_commutative()
            .map_commutative_warnings(MetarootConvertWarning::from)
            .map_errors(MetarootConvertError::from);

        let mode_res = Mode3_2::try_from(value.mode)
            .into_deferred_switchable_opt3::<_, Vec<_>>(flag)
            .switchable_into_commutative()
            .map_commutative_warnings(MetarootConvertWarning::from)
            .map_errors(MetarootConvertError::from);

        let cyt_res = value
            .cyt
            .try_into()
            .map_err(MetarootConvertError::from)
            .into_log();

        check_res
            .zip3_commutative(mode_res, cyt_res)
            .map_ok_value(|((), mode, cyt)| {
                Self::new(
                    mode,
                    value.timestamps.map(Into::into),
                    Datetimes::default(),
                    cyt,
                    None,
                    Cytsn::default(),
                    ModificationData::default(),
                    PlateData::default(),
                    None,
                    CarrierData::default(),
                    UnstainedData::default(),
                    Flowrate::default(),
                    AppliedGates3_2::default(),
                )
            })
    }
}

impl ConvertFromMetaroot<InnerMetaroot3_0> for InnerMetaroot3_2 {
    fn convert_from_metaroot(
        value: InnerMetaroot3_0,
        flag: AllowLoss,
    ) -> MetarootConvertResult<Self> {
        let uni = value.unicode.root_key_loss_error();
        let comp = value.comp.root_key_loss_error();
        let subset = value
            .subset
            .loss_errors()
            .map(AnyMetarootKeyLossError::from);
        let es = [uni, comp].into_iter().flatten().chain(subset);
        let check_res = SwitchableErrorsResult::new_deferred_switchable_iter3((), es, flag)
            .switchable_into_commutative()
            .map_commutative_warnings(MetarootConvertWarning::from)
            .map_errors(MetarootConvertError::from);

        let ag_res = value
            .applied_gates
            .try_into_3_2(flag)
            .switchable_into_commutative()
            .map_commutative_warnings(MetarootConvertWarning::from)
            .map_errors(MetarootConvertError::from);
        let mode_res = Mode3_2::try_from(value.mode)
            .into_deferred_switchable_opt3::<_, Vec<_>>(flag)
            .switchable_into_commutative()
            .map_commutative_warnings(MetarootConvertWarning::from)
            .map_errors(MetarootConvertError::from);
        let cyt_res = value
            .cyt
            .try_into()
            .map_err(MetarootConvertError::from)
            .into_log();

        check_res
            .zip4_commutative(mode_res, ag_res, cyt_res)
            .map_ok_value(|((), mode, applied_gates, cyt)| {
                Self::new(
                    mode,
                    value.timestamps.map(Into::into),
                    Datetimes::default(),
                    cyt,
                    None,
                    value.cytsn,
                    ModificationData::default(),
                    PlateData::default(),
                    None,
                    CarrierData::default(),
                    UnstainedData::default(),
                    Flowrate::default(),
                    applied_gates,
                )
            })
    }
}

impl ConvertFromMetaroot<InnerMetaroot3_1> for InnerMetaroot3_2 {
    fn convert_from_metaroot(
        value: InnerMetaroot3_1,
        flag: AllowLoss,
    ) -> MetarootConvertResult<Self> {
        let es = value
            .subset
            .loss_errors()
            .map(AnyMetarootKeyLossError::from);
        let check_res = SwitchableErrorsResult::new_deferred_switchable_iter3((), es, flag)
            .switchable_into_commutative()
            .map_commutative_warnings(MetarootConvertWarning::from)
            .map_errors(MetarootConvertError::from);

        let ag_res = value
            .applied_gates
            .try_into_3_2(flag)
            .switchable_into_commutative()
            .map_commutative_warnings(MetarootConvertWarning::from)
            .map_errors(MetarootConvertError::from);
        let mode_rs = Mode3_2::try_from(value.mode)
            .into_deferred_switchable_opt3::<_, Vec<_>>(flag)
            .switchable_into_commutative()
            .map_commutative_warnings(MetarootConvertWarning::from)
            .map_errors(MetarootConvertError::from);
        let cyt_res = value
            .cyt
            .try_into()
            .map_err(MetarootConvertError::from)
            .into_log();

        check_res
            .zip4_commutative(ag_res, mode_rs, cyt_res)
            .map_ok_value(|((), applied_gates, mode, cyt)| {
                Self::new(
                    mode,
                    value.timestamps,
                    Datetimes::default(),
                    cyt,
                    value.spillover,
                    value.cytsn,
                    value.modification,
                    value.plate,
                    value.vol,
                    CarrierData::default(),
                    UnstainedData::default(),
                    Flowrate::default(),
                    applied_gates,
                )
            })
    }
}

impl ScaleTransform {
    /// Convert to a simple scale value (just $PnE, no $PnG).
    ///
    /// This may be lossy because the $PnG value cannot be represented with
    /// just a `Scale` object, and thus needs to be dropped if present and
    /// not equal to 1.0.
    fn try_convert_to_scale(self, i: MeasIndex) -> DeferredError<Scale, IndexedKeyLossError<Gain>> {
        match self {
            Self::Lin(x) => {
                let e = IndexedKeyLossError::<Gain>(Key1::new_i1(i.into()));
                let v = Scale::Linear;
                LogResult::new_log_if(x.is_one(), v, v, e)
            }
            Self::Log(x) => LogResult::new_ok(Scale::Log(x)),
        }
    }

    fn lookup<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        i: MeasIndex,
        conf: &C,
    ) -> LookupOpticalResult<(Self, ScaleDiagnostic)>
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadStdKeywordsConfig>,
    {
        let gain = Gain::remove_or_drop_meas_opt(std, nonstd, i, conf.as_ref())
            .map_switchable_errors(LookupOpticalWarning::from)
            .switchable_into_commutative()
            .map_errors(LookupOpticalError::from)
            .into_semigroup();
        let scale = Scale::remove_meas_req_with(std, i, (), conf.as_ref())
            .map_err(LookupOpticalError::from)
            .into_log();
        gain.zip_commutative(scale).and_then_commutative(|(g, s)| {
            Self::try_from((s.native, g))
                .map(|x| (x, s.diagnostic))
                .map_err(LookupOpticalError::from)
                .into_log()
        })
    }

    fn req_suffixes(&self, i: MeasIndex) -> impl Iterator<Item = (MeasHeader, String, String)> {
        let (scale, _): (Scale, _) = (*self).into();
        [scale.triple(i)].into_iter()
    }

    fn opt_suffixes(
        &self,
        i: MeasIndex,
    ) -> impl Iterator<Item = (MeasHeader, String, Option<String>)> {
        let (_, gain): (_, Option<Gain>) = (*self).into();
        (!gain.is_some_and(|g| g.0.is_one()))
            .then_some(gain)
            .map(|g| g.meas_opt_triple(i))
            .into_iter()
    }

    pub(crate) fn is_noop(&self) -> bool {
        *self == Self::default()
    }
}

impl From<Scale> for ScaleTransform {
    fn from(value: Scale) -> Self {
        match value {
            Scale::Linear => Self::Lin(PositiveFloat::one()),
            Scale::Log(x) => Self::Log(x),
        }
    }
}

impl From<ScaleTransform> for (Scale, Option<Gain>) {
    fn from(value: ScaleTransform) -> Self {
        match value {
            ScaleTransform::Lin(g) => (Scale::Linear, Some(Gain(g))),
            ScaleTransform::Log(l) => (Scale::Log(l), None),
        }
    }
}

impl TryFrom<(Scale, Option<Gain>)> for ScaleTransform {
    type Error = ScaleTransformError;

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
                    return Err(ScaleTransformError { scale, gain: g });
                }
                Ok(Self::Log(l))
            }
        }
    }
}

impl Default for ScaleTransform {
    fn default() -> Self {
        Self::Lin(PositiveFloat::one())
    }
}

impl ConvertFromTemporal<InnerTemporal3_0> for InnerTemporal2_0 {
    fn convert_from_temporal(
        value: InnerTemporal3_0,
        _: MeasIndex,
        flag: AllowLoss,
    ) -> TemporalConvertResult<Self> {
        let e = value
            .timestep
            .loss_error()
            .map(AnyTemporalKeyLossError::from);
        let v = Self::new(true, value.peak);
        SwitchableErrorsResult::new_deferred_switchable_maybe3(v, e, flag)
    }
}

impl ConvertFromTemporal<InnerTemporal3_1> for InnerTemporal2_0 {
    fn convert_from_temporal(
        value: InnerTemporal3_1,
        i: MeasIndex,
        flag: AllowLoss,
    ) -> TemporalConvertResult<Self> {
        let t = value
            .timestep
            .loss_error()
            .map(AnyTemporalKeyLossError::from);
        let d = value.display.indexed_key_loss_error(i);
        let es = [t, d].into_iter().flatten();
        let v = Self::new(true, value.peak);
        LogResult::new_deferred_switchable_iter3(v, es, flag)
    }
}

impl ConvertFromTemporal<InnerTemporal3_2> for InnerTemporal2_0 {
    fn convert_from_temporal(
        value: InnerTemporal3_2,
        i: MeasIndex,
        flag: AllowLoss,
    ) -> TemporalConvertResult<Self> {
        let di = value.display.indexed_key_loss_error(i);
        let m = value.measurement_type.indexed_key_loss_error(i);
        let t = value
            .timestep
            .loss_error()
            .map(AnyTemporalKeyLossError::from);
        let es = [di, m, t].into_iter().flatten();
        let v = Self::new(true, PeakData::default());
        LogResult::new_deferred_switchable_iter3(v, es, flag)
    }
}

impl ConvertFromTemporal<InnerTemporal2_0> for InnerTemporal3_0 {
    fn convert_from_temporal(
        value: InnerTemporal2_0,
        _: MeasIndex,
        flag: AllowLoss,
    ) -> TemporalConvertResult<Self> {
        LogResult::new_switchable_ok(Self::new(Timestep::default(), value.peak), flag)
    }
}

impl ConvertFromTemporal<InnerTemporal3_1> for InnerTemporal3_0 {
    fn convert_from_temporal(
        value: InnerTemporal3_1,
        i: MeasIndex,
        flag: AllowLoss,
    ) -> TemporalConvertResult<Self> {
        let e = value.display.indexed_key_loss_error(i);
        let v = Self::new(value.timestep, value.peak);
        LogResult::new_deferred_switchable_maybe3(v, e, flag)
    }
}

impl ConvertFromTemporal<InnerTemporal3_2> for InnerTemporal3_0 {
    fn convert_from_temporal(
        value: InnerTemporal3_2,
        i: MeasIndex,
        flag: AllowLoss,
    ) -> TemporalConvertResult<Self> {
        let di = value.display.indexed_key_loss_error(i);
        let m = value.measurement_type.indexed_key_loss_error(i);
        let es = [di, m].into_iter().flatten();
        let v = Self::new(value.timestep, PeakData::default());
        LogResult::new_deferred_switchable_iter3(v, es, flag)
    }
}

impl ConvertFromTemporal<InnerTemporal2_0> for InnerTemporal3_1 {
    fn convert_from_temporal(
        value: InnerTemporal2_0,
        _: MeasIndex,
        flag: AllowLoss,
    ) -> TemporalConvertResult<Self> {
        LogResult::new_switchable_ok(Self::new(Timestep::default(), None, value.peak), flag)
    }
}

impl ConvertFromTemporal<InnerTemporal3_0> for InnerTemporal3_1 {
    fn convert_from_temporal(
        value: InnerTemporal3_0,
        _: MeasIndex,
        flag: AllowLoss,
    ) -> TemporalConvertResult<Self> {
        LogResult::new_switchable_ok(Self::new(value.timestep, None, value.peak), flag)
    }
}

impl ConvertFromTemporal<InnerTemporal3_2> for InnerTemporal3_1 {
    fn convert_from_temporal(
        value: InnerTemporal3_2,
        i: MeasIndex,
        flag: AllowLoss,
    ) -> TemporalConvertResult<Self> {
        let e = value.measurement_type.indexed_key_loss_error(i);
        let v = Self::new(value.timestep, value.display, PeakData::default());
        LogResult::new_deferred_switchable_maybe3(v, e, flag)
    }
}

impl ConvertFromTemporal<InnerTemporal2_0> for InnerTemporal3_2 {
    fn convert_from_temporal(
        value: InnerTemporal2_0,
        i: MeasIndex,
        flag: AllowLoss,
    ) -> TemporalConvertResult<Self> {
        let es = value.peak.loss_errors(i).map(AnyTemporalKeyLossError::from);
        let v = Self::new(Timestep::default(), None, TemporalType::default());
        LogResult::new_deferred_switchable_iter3(v, es, flag)
    }
}

impl ConvertFromTemporal<InnerTemporal3_0> for InnerTemporal3_2 {
    fn convert_from_temporal(
        value: InnerTemporal3_0,
        i: MeasIndex,
        flag: AllowLoss,
    ) -> TemporalConvertResult<Self> {
        let es = value.peak.loss_errors(i).map(AnyTemporalKeyLossError::from);
        let v = Self::new(value.timestep, None, TemporalType::default());
        LogResult::new_deferred_switchable_iter3(v, es, flag)
    }
}

impl ConvertFromTemporal<InnerTemporal3_1> for InnerTemporal3_2 {
    fn convert_from_temporal(
        value: InnerTemporal3_1,
        i: MeasIndex,
        flag: AllowLoss,
    ) -> TemporalConvertResult<Self> {
        let es = value.peak.loss_errors(i).map(AnyTemporalKeyLossError::from);
        let v = Self::new(value.timestep, value.display, TemporalType::default());
        LogResult::new_deferred_switchable_iter3(v, es, flag)
    }
}

impl Versioned for Version2_0 {
    type Layout = DataLayout2_0;
    type Offsets = TEXTOffsets2_0;

    fn fcs_version() -> Version {
        Self.into()
    }
}

impl Versioned for Version3_0 {
    type Layout = DataLayout3_0;
    type Offsets = TEXTOffsets3_0;

    fn fcs_version() -> Version {
        Self.into()
    }
}

impl Versioned for Version3_1 {
    type Layout = DataLayout3_1;
    type Offsets = TEXTOffsets3_0;

    fn fcs_version() -> Version {
        Self.into()
    }
}

impl Versioned for Version3_2 {
    type Layout = DataLayout3_2;
    type Offsets = TEXTOffsets3_2;

    fn fcs_version() -> Version {
        Self.into()
    }
}

impl PrivVersioned for Version2_0 {}
impl PrivVersioned for Version3_0 {}
impl PrivVersioned for Version3_1 {}
impl PrivVersioned for Version3_2 {}

impl AsScaleTransform for InnerOptical2_0 {
    fn as_transform(&self) -> ScaleTransform {
        self.scale.map(Into::into).unwrap_or_default()
    }
}

impl AsScaleTransform for InnerOptical3_0 {
    fn as_transform(&self) -> ScaleTransform {
        self.scale
    }
}

impl AsScaleTransform for InnerOptical3_1 {
    fn as_transform(&self) -> ScaleTransform {
        self.scale
    }
}

impl AsScaleTransform for InnerOptical3_2 {
    fn as_transform(&self) -> ScaleTransform {
        self.scale
    }
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
        let scale = Scale::remove_or_drop_meas_opt_with(std, nonstd, i, (), conf)
            .map_switchable_errors(LookupOpticalWarning::from)
            .switchable_into_commutative()
            .into_semigroup();
        let wave = Wavelength::remove_or_drop_meas_opt(std, nonstd, i, conf.as_ref())
            .map_switchable_errors(LookupOpticalWarning::from)
            .switchable_into_commutative()
            .into_semigroup();
        let peak = PeakData::lookup(std, nonstd, i, conf.as_ref())
            .map_warnings_and_errors(LookupOpticalWarning::from);
        scale
            .zip3_commutative(wave, peak)
            .map_errors(LookupOpticalError::from)
            .map_ok_value(|(si_out, wi, pi)| {
                let ret = Self::new(si_out.native, wi, pi);
                DiagnosedOptical::new(ret, si_out.diagnostic, vec![])
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
        let scale = ScaleTransform::lookup(std, nonstd, i, conf);
        wave.zip_commutative(peak)
            .map_errors(LookupOpticalError::from)
            .zip_commutative(scale)
            .map_ok_value(|((w, p), (s, sd))| {
                let ret = Self::new(s, w, p);
                DiagnosedOptical::new(ret, sd, vec![])
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
        let scale = ScaleTransform::lookup(std, nonstd, i, conf);
        go!(wave)
            .zip4_commutative(go!(cal), go!(dpy), peak)
            .map_errors(LookupOpticalError::from)
            .zip_commutative(scale)
            .map_ok_value(|((w_out, c_out, d_out, p), (s, sd))| {
                let (w, w_trimmed) = w_out.into_indexed_pair(i);
                let (c, c_trimmed) = c_out.into_opt_indexed_pair(i);
                let (d, d_trimmed) = d_out.into_opt_indexed_pair(i);
                let ret = Self::new(s, w, c, d, p);
                let trimmed = w_trimmed
                    .into_iter()
                    .chain(c_trimmed)
                    .chain(d_trimmed)
                    .collect();
                DiagnosedOptical::new(ret, sd, trimmed)
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

        let scale = ScaleTransform::lookup(std, nonstd, i, conf);

        go!(wave)
            .zip6_commutative(go!(cal), go!(dpy), go!(meas), go!(feat), scale)
            .map_ok_value(|(w_out, c_out, d_out, m, f, (s, sd))| {
                let (w, w_trimmed) = w_out.into_indexed_pair(i);
                let (c, c_trimmed) = c_out.into_opt_indexed_pair(i);
                let (d, d_trimmed) = d_out.into_opt_indexed_pair(i);
                // TODO return something for feature
                let ret = Self::new(s, w, c, d, anal, f.native, m, tag, det_name);
                let trimmed = c_trimmed
                    .into_iter()
                    .chain(w_trimmed)
                    .chain(d_trimmed)
                    .collect();
                DiagnosedOptical::new(ret, sd, trimmed)
            })
    }
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
        let tmp_opt_res = TemporalOpticalKey::remove_keys_2_0(ignore, std, nonstd, i, flag)
            .map_warnings_and_errors(LookupTemporalWarning::from);
        scale
            .zip3_commutative(peak, tmp_opt_res)
            .map_errors(LookupTemporalError::from)
            .map_ok_value(|(s, p, tmp_opt_pairs)| {
                let this = Self::new(s.native, p);
                DiagnosedTemporal::new(this, s.diagnostic, vec![], tmp_opt_pairs)
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
        let tmp_opt = TemporalOpticalKey::remove_keys_3_0(ignore, std, nonstd, i, flag)
            .map_warnings_and_errors(LookupTemporalWarning::from);
        let scale = TemporalScale3_0::remove_meas_req_with(std, i, (), conf.as_ref())
            .map_err(LookupTemporalError::from);
        let timestep = Timestep::remove_metaroot_req(std).map_err(LookupTemporalError::from);
        let req_res = scale.zip(timestep);
        gain.zip3_commutative(peak, tmp_opt)
            .map_errors(LookupTemporalError::from)
            .zip_commutative(req_res)
            .map_ok_value(|((_, p, tmp_opt_pairs), (s, t))| {
                let this = Self::new(t, p);
                DiagnosedTemporal::new(this, s.diagnostic, vec![], tmp_opt_pairs)
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
        let tmp_opt = TemporalOpticalKey::remove_keys_3_1(ignore, std, nonstd, i, flag)
            .map_warnings_and_errors(LookupTemporalWarning::from);
        let scale = TemporalScale3_0::remove_meas_req_with(std, i, (), conf.as_ref())
            .map_err(LookupTemporalError::from);
        let timestep = Timestep::remove_metaroot_req(std).map_err(LookupTemporalError::from);
        let req_res = scale.zip(timestep);
        gain.zip4_commutative(dpy, peak, tmp_opt)
            .map_errors(LookupTemporalError::from)
            .zip_commutative(req_res)
            .map_ok_value(|((_, d_out, p, tmp_opt_pairs), (s, t))| {
                let (d, d_trimmed) = d_out.into_opt_indexed_pair(i);
                let trimmed = d_trimmed.into_iter().collect();
                let ret = Self::new(t, d, p);
                DiagnosedTemporal::new(ret, s.diagnostic, trimmed, tmp_opt_pairs)
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
        let tmp_opt = TemporalOpticalKey::remove_keys_3_2(ignore, std, nonstd, i, flag)
            .map_warnings_and_errors(LookupTemporalWarning::from);
        let scale = TemporalScale3_0::remove_meas_req_with(std, i, (), conf.as_ref())
            .map_err(LookupTemporalError::from);
        let timestep = Timestep::remove_metaroot_req(std).map_err(LookupTemporalError::from);
        let req_res = scale.zip(timestep);
        gain.zip4_commutative(dpy, meas, tmp_opt)
            .map_errors(LookupTemporalError::from)
            .zip_commutative(req_res)
            .map_ok_value(|((_, d_out, m, tmp_opt_pairs), (s, t))| {
                let (d, d_trimmed) = d_out.into_opt_indexed_pair(i);
                let trimmed = d_trimmed.into_iter().collect();
                let ret = Self::new(t, d, m);
                DiagnosedTemporal::new(ret, s.diagnostic, trimmed, tmp_opt_pairs)
            })
    }
}

impl VersionedOptical for InnerOptical2_0 {
    type Ver = Version2_0;
    fn req_suffixes_inner(
        &self,
        _: MeasIndex,
    ) -> impl Iterator<Item = (MeasHeader, String, String)> {
        empty()
    }

    fn opt_suffixes_inner(
        &self,
        i: MeasIndex,
    ) -> impl Iterator<Item = (MeasHeader, String, Option<String>)> {
        [
            self.scale.meas_opt_triple(i),
            self.wavelength.meas_opt_triple(i),
        ]
        .into_iter()
        .chain(self.peak.opt_keywords(i))
    }

    fn nonlinear_scale_error(&self, i: MeasIndex) -> Option<OpticalNonLinearError> {
        let v = Self::Ver::fcs_version();
        self.scale
            .as_ref()
            .is_some_and(|s| *s == Scale::Linear)
            .then_some(OpticalNonLinearError::new(i, v))
    }

    fn optical_to_temporal_loss_errors(
        &self,
        i: MeasIndex,
    ) -> impl Iterator<Item = AnyOpticalToTemporalKeyLossError> {
        self.wavelength.indexed_key_loss_error(i).into_iter()
    }

    fn deprecated(&mut self, _: MeasIndex) -> impl Iterator<Item = DeprecatedRef<'_>> {
        empty()
    }
}

impl VersionedOptical for InnerOptical3_0 {
    type Ver = Version3_0;
    fn req_suffixes_inner(
        &self,
        i: MeasIndex,
    ) -> impl Iterator<Item = (MeasHeader, String, String)> {
        self.scale.req_suffixes(i)
    }

    fn opt_suffixes_inner(
        &self,
        i: MeasIndex,
    ) -> impl Iterator<Item = (MeasHeader, String, Option<String>)> {
        once(self.wavelength.meas_opt_triple(i))
            .chain(self.peak.opt_keywords(i))
            .chain(self.scale.opt_suffixes(i))
    }

    fn nonlinear_scale_error(&self, i: MeasIndex) -> Option<OpticalNonLinearError> {
        let v = Self::Ver::fcs_version();
        (!self.scale.is_noop()).then_some(OpticalNonLinearError::new(i, v))
    }

    fn optical_to_temporal_loss_errors(
        &self,
        i: MeasIndex,
    ) -> impl Iterator<Item = AnyOpticalToTemporalKeyLossError> {
        self.wavelength.indexed_key_loss_error(i).into_iter()
    }

    fn deprecated(&mut self, _: MeasIndex) -> impl Iterator<Item = DeprecatedRef<'_>> {
        empty()
    }
}

impl VersionedOptical for InnerOptical3_1 {
    type Ver = Version3_1;
    fn req_suffixes_inner(
        &self,
        i: MeasIndex,
    ) -> impl Iterator<Item = (MeasHeader, String, String)> {
        self.scale.req_suffixes(i)
    }

    fn opt_suffixes_inner(
        &self,
        i: MeasIndex,
    ) -> impl Iterator<Item = (MeasHeader, String, Option<String>)> {
        [
            self.wavelengths.meas_opt_triple(i),
            self.calibration.meas_opt_triple(i),
            self.display.meas_opt_triple(i),
        ]
        .into_iter()
        .chain(self.peak.opt_keywords(i))
        .chain(self.scale.opt_suffixes(i))
    }

    fn nonlinear_scale_error(&self, i: MeasIndex) -> Option<OpticalNonLinearError> {
        let v = Self::Ver::fcs_version();
        (!self.scale.is_noop()).then_some(OpticalNonLinearError::new(i, v))
    }

    fn optical_to_temporal_loss_errors(
        &self,
        i: MeasIndex,
    ) -> impl Iterator<Item = AnyOpticalToTemporalKeyLossError> {
        let a = self.calibration.indexed_key_loss_error(i);
        let b = self.wavelengths.indexed_key_loss_error(i);
        [a, b].into_iter().flatten()
    }

    fn deprecated(&mut self, i: MeasIndex) -> impl Iterator<Item = DeprecatedRef<'_>> {
        self.peak.deprecated(i).map(DeprecatedRef::from)
    }
}

impl VersionedOptical for InnerOptical3_2 {
    type Ver = Version3_2;
    fn req_suffixes_inner(
        &self,
        i: MeasIndex,
    ) -> impl Iterator<Item = (MeasHeader, String, String)> {
        self.scale.req_suffixes(i)
    }

    fn opt_suffixes_inner(
        &self,
        i: MeasIndex,
    ) -> impl Iterator<Item = (MeasHeader, String, Option<String>)> {
        [
            self.wavelengths.meas_opt_triple(i),
            self.calibration.meas_opt_triple(i),
            self.display.meas_opt_triple(i),
            self.detector_name.meas_opt_triple(i),
            self.tag.meas_opt_triple(i),
            self.measurement_type.meas_opt_triple(i),
            self.feature.meas_opt_triple(i),
            self.analyte.meas_opt_triple(i),
        ]
        .into_iter()
        .chain(self.scale.opt_suffixes(i))
    }

    fn nonlinear_scale_error(&self, i: MeasIndex) -> Option<OpticalNonLinearError> {
        let v = Self::Ver::fcs_version();
        (!self.scale.is_noop()).then_some(OpticalNonLinearError::new(i, v))
    }

    fn optical_to_temporal_loss_errors(
        &self,
        i: MeasIndex,
    ) -> impl Iterator<Item = AnyOpticalToTemporalKeyLossError> {
        let cal = self.calibration.indexed_key_loss_error(i);
        let wave = self.wavelengths.indexed_key_loss_error(i);
        let meas = self.measurement_type.indexed_key_loss_error(i);
        let anal = self.analyte.indexed_key_loss_error(i);
        let tag = self.tag.indexed_key_loss_error(i);
        let det_name = self.detector_name.indexed_key_loss_error(i);
        let feat = self.feature.indexed_key_loss_error(i);
        [cal, wave, meas, anal, tag, det_name, feat]
            .into_iter()
            .flatten()
    }

    fn deprecated(&mut self, _: MeasIndex) -> impl Iterator<Item = DeprecatedRef<'_>> {
        empty()
    }
}

impl VersionedTemporal for InnerTemporal2_0 {
    type Ver = Version2_0;
    type Warning = Nothing<()>;
    type Error = Infallible;

    fn req_meta_keywords_inner(&self) -> impl Iterator<Item = (String, String)> {
        empty()
    }

    fn req_meas_keywords_inner(&self, _: MeasIndex) -> impl Iterator<Item = (String, String)> {
        empty()
    }

    fn opt_meas_keywords_inner(&self, i: MeasIndex) -> impl Iterator<Item = (String, String)> {
        self.peak
            .opt_keywords(i)
            .map(|(_, k, v)| (k, v))
            .chain([self.scale.meas_opt_pair(i)])
            .filter_map(|(k, v)| v.map(|x| (k, x)))
    }

    fn can_convert_to_optical(&self, _: MeasIndex) -> Result<(), Self::Error> {
        Ok(())
    }

    fn temporal_to_optical_error(&self, i: MeasIndex) -> Option<AnyTemporalToOpticalKeyLossError> {
        self.can_convert_to_optical(i).infallible_err_into()
    }

    fn deprecated(&mut self, _: MeasIndex) -> impl Iterator<Item = DeprecatedRef<'_>> {
        empty()
    }
}

impl VersionedTemporal for InnerTemporal3_0 {
    type Ver = Version3_0;
    type Warning = Nothing<()>;
    type Error = Infallible;

    fn req_meta_keywords_inner(&self) -> impl Iterator<Item = (String, String)> {
        [self.timestep.pair()].into_iter()
    }

    fn req_meas_keywords_inner(&self, i: MeasIndex) -> impl Iterator<Item = (String, String)> {
        [TemporalScale3_0::default().meas_pair(i)].into_iter()
    }

    fn opt_meas_keywords_inner(&self, i: MeasIndex) -> impl Iterator<Item = (String, String)> {
        self.peak
            .opt_keywords(i)
            .filter_map(|(_, k, v)| v.map(|x| (k, x)))
    }

    fn can_convert_to_optical(&self, _: MeasIndex) -> Result<(), Self::Error> {
        Ok(())
    }

    fn temporal_to_optical_error(&self, i: MeasIndex) -> Option<AnyTemporalToOpticalKeyLossError> {
        self.can_convert_to_optical(i).infallible_err_into()
    }

    fn deprecated(&mut self, _: MeasIndex) -> impl Iterator<Item = DeprecatedRef<'_>> {
        empty()
    }
}

impl VersionedTemporal for InnerTemporal3_1 {
    type Ver = Version3_1;
    type Warning = Nothing<()>;
    type Error = Infallible;

    fn req_meta_keywords_inner(&self) -> impl Iterator<Item = (String, String)> {
        [self.timestep.pair()].into_iter()
    }

    fn req_meas_keywords_inner(&self, i: MeasIndex) -> impl Iterator<Item = (String, String)> {
        [TemporalScale3_0::default().meas_pair(i)].into_iter()
    }

    fn opt_meas_keywords_inner(&self, i: MeasIndex) -> impl Iterator<Item = (String, String)> {
        self.peak
            .opt_keywords(i)
            .map(|(_, k, v)| (k, v))
            .chain([self.display.meas_opt_pair(i)])
            .filter_map(|(k, v)| v.map(|x| (k, x)))
    }

    fn can_convert_to_optical(&self, _: MeasIndex) -> Result<(), Self::Error> {
        Ok(())
    }

    fn temporal_to_optical_error(&self, i: MeasIndex) -> Option<AnyTemporalToOpticalKeyLossError> {
        self.can_convert_to_optical(i).infallible_err_into()
    }

    fn deprecated(&mut self, i: MeasIndex) -> impl Iterator<Item = DeprecatedRef<'_>> {
        self.peak.deprecated(i).map(DeprecatedRef::from)
    }
}

impl VersionedTemporal for InnerTemporal3_2 {
    type Ver = Version3_2;
    type Warning = Option<AnyTemporalToOpticalKeyLossError>;
    type Error = AnyTemporalToOpticalKeyLossError;

    fn req_meta_keywords_inner(&self) -> impl Iterator<Item = (String, String)> {
        [self.timestep.pair()].into_iter()
    }

    fn req_meas_keywords_inner(&self, i: MeasIndex) -> impl Iterator<Item = (String, String)> {
        [TemporalScale3_0::default().meas_pair(i)].into_iter()
    }

    fn opt_meas_keywords_inner(&self, i: MeasIndex) -> impl Iterator<Item = (String, String)> {
        once(self.display.meas_opt_pair(i)).filter_map(|(k, v)| v.map(|x| (k, x)))
    }

    fn can_convert_to_optical(&self, i: MeasIndex) -> Result<(), Self::Error> {
        self.measurement_type
            .indexed_key_loss_error(i)
            .map_or(Ok(()), Err)
    }

    fn temporal_to_optical_error(&self, i: MeasIndex) -> Option<AnyTemporalToOpticalKeyLossError> {
        self.can_convert_to_optical(i).err()
    }

    fn deprecated(&mut self, _: MeasIndex) -> impl Iterator<Item = DeprecatedRef<'_>> {
        empty()
    }
}

impl VersionedTEXTOffsets for TEXTOffsets2_0 {
    type TotDef = Option<Tot>;

    fn lookup<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        segs: &NonDataSegments,
        st: &ReadState<C>,
    ) -> LookupTEXTOffsetsResult<Self>
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadOffsetConfig>,
    {
        Tot::remove_or_drop_root_opt(std, nonstd, st.conf.as_ref())
            .map_ok_value(|tot| {
                let s = DatasetSegments::new(
                    segs.header.data.into_any(),
                    segs.header.analysis.into_any(),
                    None,
                    None,
                );
                TEXTOffsets::new(s, tot).into()
            })
            .set_err_value(())
            .switchable_into_commutative()
            .map_commutative_warnings(LookupTEXTOffsetsWarning::from)
            .map_errors(LookupTEXTOffsetsError::from)
            .into_semigroup()
    }

    fn lookup_ro<C>(
        std: &StdKeywords,
        segs: &NonDataSegments,
        _: &ReadState<C>,
    ) -> LookupTEXTOffsetsResult<Self>
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadOffsetConfig>,
    {
        let succ = Tot::get_root_opt(std)
            .map_err(LookupTEXTOffsetsWarning::from)
            .into_succ()
            .fmap_once(|tot| {
                let s = DatasetSegments::new(
                    segs.header.data.into_any(),
                    segs.header.analysis.into_any(),
                    None,
                    None,
                );
                TEXTOffsets::new(s, tot).into()
            });
        LogResult::Succ(succ)
    }

    fn tot(&self) -> Self::TotDef {
        self.0.tot
    }

    fn into_common(self) -> TEXTOffsets<Option<Tot>> {
        let x = self.0;
        TEXTOffsets::new(x.segs, x.tot)
    }
}

macro_rules! lookup_offsets_3_0 {
    ($std:expr, $segs:expr, $st:expr, $tot:ident, $offsets:ident) => {{
        let tot_res = Tot::$tot($std)
            .map_err(LookupTEXTOffsetsError::from)
            .into_log();
        let dconf: &ReadDataKeywordsConfig = $st.conf.as_ref();
        let data_ignore = dconf.ignore_text_data_offsets;
        let data_corr = dconf.text_data_correction;
        let data_res = DataSegmentId::$offsets($std, $segs, data_ignore, data_corr, $st)
            .map_commutative_warnings(LookupTEXTOffsetsWarning::from)
            .map_errors(LookupTEXTOffsetsError::from);
        let anal_ignore = dconf.ignore_text_analysis_offsets;
        let anal_corr = dconf.text_analysis_correction;
        let anal_res = AnalysisSegmentId::$offsets($std, $segs, anal_ignore, anal_corr, $st)
            .map_commutative_warnings(LookupTEXTOffsetsWarning::from)
            .map_errors(LookupTEXTOffsetsError::from);
        tot_res
            .zip3_commutative(data_res, anal_res)
            .map_ok_value(|(tot, (d, dr), (a, ar))| {
                TEXTOffsets::new(DatasetSegments::new(d, a, dr, ar), tot).into()
            })
    }};
}

impl VersionedTEXTOffsets for TEXTOffsets3_0 {
    type TotDef = Identity<Tot>;

    fn lookup<C>(
        std: &mut StdKeywords,
        _: &mut NonStdKeywords,
        segs: &NonDataSegments,
        st: &ReadState<C>,
    ) -> LookupTEXTOffsetsResult<Self>
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadOffsetConfig>,
    {
        lookup_offsets_3_0!(std, segs, st, remove_metaroot_req, remove_req_or)
    }

    fn lookup_ro<C>(
        std: &StdKeywords,
        segs: &NonDataSegments,
        st: &ReadState<C>,
    ) -> LookupTEXTOffsetsResult<Self>
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadOffsetConfig>,
    {
        lookup_offsets_3_0!(std, segs, st, get_metaroot_req, get_req_or)
    }

    fn tot(&self) -> Self::TotDef {
        Identity(self.0.tot)
    }

    fn into_common(self) -> TEXTOffsets<Option<Tot>> {
        let x = self.0;
        TEXTOffsets::new(x.segs, Some(x.tot))
    }
}

macro_rules! lookup_offsets_3_2 {
    ($std:expr, $segs:expr, $st:expr, $tot:ident, $offset_req:ident, $offset_opt:ident) => {{
        let tot_res = Tot::$tot($std)
            .map_err(LookupTEXTOffsetsError::from)
            .into_log();
        let dconf: &ReadDataKeywordsConfig = $st.conf.as_ref();
        let data_corr = dconf.text_data_correction;
        let data_ignore = dconf.ignore_text_data_offsets;
        let data_res = DataSegmentId::$offset_req($std, $segs, data_ignore, data_corr, $st)
            .map_commutative_warnings(LookupTEXTOffsetsWarning::from)
            .map_errors(LookupTEXTOffsetsError::from);
        let anal_corr = dconf.text_analysis_correction;
        let anal_ignore = dconf.ignore_text_analysis_offsets;
        let anal_res = AnalysisSegmentId::$offset_opt($std, $segs, anal_ignore, anal_corr, $st)
            .map_commutative_warnings(LookupTEXTOffsetsWarning::from)
            .map_errors(LookupTEXTOffsetsError::from);
        tot_res
            .zip3_commutative(data_res, anal_res)
            .map_ok_value(|(tot, (d, dr), (a, ar))| {
                TEXTOffsets::new(DatasetSegments::new(d, a, dr, ar), tot).into()
            })
    }};
}

impl VersionedTEXTOffsets for TEXTOffsets3_2 {
    type TotDef = Identity<Tot>;

    fn lookup<C>(
        std: &mut StdKeywords,
        _: &mut NonStdKeywords,
        segs: &NonDataSegments,
        st: &ReadState<C>,
    ) -> LookupTEXTOffsetsResult<Self>
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadOffsetConfig>,
    {
        lookup_offsets_3_2!(
            std,
            segs,
            st,
            remove_metaroot_req,
            remove_req_or,
            remove_opt_or
        )
    }

    fn lookup_ro<C>(
        std: &StdKeywords,
        segs: &NonDataSegments,
        st: &ReadState<C>,
    ) -> LookupTEXTOffsetsResult<Self>
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadOffsetConfig>,
    {
        lookup_offsets_3_2!(std, segs, st, get_metaroot_req, get_req_or, get_opt_or)
    }

    fn tot(&self) -> Self::TotDef {
        Identity(self.0.tot)
    }

    fn into_common(self) -> TEXTOffsets<Option<Tot>> {
        let x = self.0;
        TEXTOffsets::new(x.segs, Some(x.tot))
    }
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
        let new = Self::new(Some(Scale::Linear), None, t.peak);
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
        let new = Self::new(ScaleTransform::default(), None, t.peak);
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
        let new = Self::new(
            ScaleTransform::default(),
            Wavelengths::default(),
            None,
            t.display,
            t.peak,
        );
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
            ScaleTransform::default(),
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

impl TemporalFromOptical<InnerOptical2_0> for InnerTemporal2_0 {
    type TData = ();

    fn from_optical_inner(o: InnerOptical2_0, (): Self::TData) -> Self {
        Self::new(o.scale.is_some(), o.peak)
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

type Timestamps2_0 = Timestamps<FCSTime>;
type Timestamps3_0 = Timestamps<FCSTime60>;
type Timestamps3_1 = Timestamps<FCSTime100>;

impl LookupMetaroot for InnerMetaroot2_0 {
    fn lookup_shortname(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        i: MeasIndex,
        conf: &ReadDataKeywordsConfig,
    ) -> LookupShortnameResult<Self::Name> {
        Shortname::remove_or_drop_meas_opt(std, nonstd, i, conf)
            .set_err_value(())
            .switchable_into_commutative()
            .map_errors(LookupShortnameError::from)
    }

    fn lookup_specific<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        ms: &[Self::Name],
        conf: &C,
    ) -> LookupMetarootResult<(Self, Vec<(StdKey, String)>)>
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadStdKeywordsConfig>,
    {
        let par = Par(ms.len());
        let comp = Compensation2_0::lookup(std, par, conf.as_ref())
            .map_switchable_errors(LookupMetarootWarning::from)
            .switchable_into_commutative();
        let cyt = Cyt::remove_root_opt_nofail(std);
        let ts = Timestamps::lookup(std, nonstd, conf)
            .map_warnings_and_errors(LookupMetarootWarning::from);
        let ag = AppliedGates2_0::lookup(std, nonstd, conf)
            .map_warnings_and_errors(LookupMetarootWarning::from);
        let mode = Mode::remove_metaroot_req(std)
            .map_err(LookupMetarootError::from)
            .into_log();
        comp.zip3_commutative(ts, ag)
            .map_errors(LookupMetarootError::from)
            .zip_commutative(mode)
            .map_ok_value(|((c, t, g), m)| (Self::new(m, cyt, c, t, g), vec![]))
    }
}

impl LookupMetaroot for InnerMetaroot3_0 {
    fn lookup_shortname(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        i: MeasIndex,
        conf: &ReadDataKeywordsConfig,
    ) -> LookupShortnameResult<Self::Name> {
        Shortname::remove_or_drop_meas_opt(std, nonstd, i, conf)
            .set_err_value(())
            .switchable_into_commutative()
            .map_errors(LookupShortnameError::from)
    }

    fn lookup_specific<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        _: &[Self::Name],
        conf: &C,
    ) -> LookupMetarootResult<(Self, Vec<(StdKey, String)>)>
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadStdKeywordsConfig>,
    {
        macro_rules! go {
            ($x:expr) => {
                $x.map_switchable_errors(LookupMetarootWarning::from)
                    .switchable_into_commutative()
                    .into_semigroup()
            };
        }

        let cyt = Cyt::remove_root_opt_nofail(std);
        let cytsn = Cytsn::remove_root_opt_nofail(std);

        let comp = Compensation3_0::remove_or_drop_root_opt_with(std, nonstd, (), conf);
        let uni = Unicode::remove_or_drop_root_opt_with(std, nonstd, (), conf);

        let ts = Timestamps::lookup(std, nonstd, conf)
            .map_warnings_and_errors(LookupMetarootWarning::from);
        let subset = SubsetData::lookup(std, nonstd, conf.as_ref())
            .map_warnings_and_errors(LookupMetarootWarning::from);
        let ag = AppliedGates3_0::lookup(std, nonstd, conf)
            .map_warnings_and_errors(LookupMetarootWarning::from);

        let mode = Mode::remove_metaroot_req(std)
            .map_err(LookupMetarootError::from)
            .into_log();

        go!(comp)
            .zip5_commutative(subset, ts, go!(uni), ag)
            .map_errors(LookupMetarootError::from)
            .zip_commutative(mode)
            .map_ok_value(|((co_out, su, t, u_out, g), m)| {
                let (co, c_trimmed) = co_out.into_opt_root_pair();
                let (u, u_trimmed) = u_out.into_opt_root_pair();
                let ret = Self::new(m, cyt, co, t, cytsn, u, su, g);
                let trimmed = c_trimmed.into_iter().chain(u_trimmed).collect();
                (ret, trimmed)
            })
    }
}

impl LookupMetaroot for InnerMetaroot3_1 {
    fn lookup_shortname(
        std: &mut StdKeywords,
        _: &mut NonStdKeywords,
        i: MeasIndex,
        _: &ReadDataKeywordsConfig,
    ) -> LookupShortnameResult<Self::Name> {
        Shortname::remove_meas_req(std, i)
            .map(Identity)
            .map_err(LookupShortnameError::from)
            .into_log()
    }

    fn lookup_specific<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        ms: &[Self::Name],
        conf: &C,
    ) -> LookupMetarootResult<(Self, Vec<(StdKey, String)>)>
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadStdKeywordsConfig>,
    {
        let process_mode = |mode| {
            let err = match &mode {
                Mode::Correlated => Some(DeprecatedModeWarning::ModeCorrelated),
                Mode::Uncorrelated => Some(DeprecatedModeWarning::ModeUncorrelated),
                Mode::List => None,
            };
            let sconf: &ReadStdKeywordsConfig = conf.as_ref();
            let flag = sconf.disallow_deprecated;
            SwitchableErrorsResult::new_switchable_iter3(mode, (), err, flag)
                .map_switchable_errors(LookupMetarootWarning::from)
                .switchable_into_commutative()
                .map_errors(LookupMetarootError::from)
        };

        let ordered_names: Vec<_> = ms.iter().map(|n| &n.0).collect();

        let cyt = Cyt::remove_root_opt_nofail(std);
        let cytsn = Cytsn::remove_root_opt_nofail(std);
        let plate = PlateData::lookup(std);

        let vol = Vol::remove_or_drop_root_opt(std, nonstd, conf.as_ref())
            .map_switchable_errors(LookupMetarootWarning::from)
            .switchable_into_commutative()
            .into_semigroup();

        let spill = Spillover::remove_or_drop_root_opt_with(std, nonstd, &ordered_names[..], conf)
            .map_switchable_errors(LookupMetarootWarning::from)
            .switchable_into_commutative()
            .into_semigroup();

        let subset = SubsetData::lookup(std, nonstd, conf.as_ref())
            .map_warnings_and_errors(LookupMetarootWarning::from);
        let ag = AppliedGates3_0::lookup(std, nonstd, conf)
            .map_warnings_and_errors(LookupMetarootWarning::from);
        let modif = ModificationData::lookup(std, nonstd, conf)
            .map_warnings_and_errors(LookupMetarootWarning::from);
        let ts = Timestamps::lookup(std, nonstd, conf)
            .map_warnings_and_errors(LookupMetarootWarning::from);

        let mode = Mode::remove_metaroot_req(std)
            .map_err(LookupMetarootError::from)
            .into_log()
            .and_then_commutative(process_mode);

        spill
            .zip6_commutative(subset, modif, ts, vol, ag)
            .map_errors(LookupMetarootError::from)
            .zip_commutative(mode)
            .map_ok_value(|((sp_out, su, md, t, v, g), m)| {
                let (sp, sp_trimmed) = sp_out.into_opt_root_pair();
                let ret = Self::new(m, cyt, t, cytsn, sp, md, plate, v, su, g);
                let trimmed = sp_trimmed.into_iter().collect();
                (ret, trimmed)
            })
    }
}

impl LookupMetaroot for InnerMetaroot3_2 {
    fn lookup_shortname(
        std: &mut StdKeywords,
        _: &mut NonStdKeywords,
        i: MeasIndex,
        _: &ReadDataKeywordsConfig,
    ) -> LookupShortnameResult<Self::Name> {
        Shortname::remove_meas_req(std, i)
            .map(Identity)
            .map_err(LookupShortnameError::from)
            .into_log()
    }

    fn lookup_specific<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        ms: &[Self::Name],
        conf: &C,
    ) -> LookupMetarootResult<(Self, Vec<(StdKey, String)>)>
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadStdKeywordsConfig>,
    {
        macro_rules! go {
            ($x:expr) => {
                $x.map_switchable_errors(LookupMetarootWarning::from)
                    .switchable_into_commutative()
                    .into_semigroup()
            };
        }

        let ordered_names: Vec<_> = ms.iter().map(|n| &n.0).collect();

        let flow = Flowrate::remove_root_opt_nofail(std);
        let cytsn = Cytsn::remove_root_opt_nofail(std);
        let plate = PlateData::lookup(std);
        let carrier = CarrierData::lookup(std);

        let mode = go!(Mode3_2::remove_or_drop_root_opt(std, nonstd, conf.as_ref()));
        let us = go!(UnstainedData::lookup(std, nonstd, conf));
        let vol = go!(Vol::remove_or_drop_root_opt(std, nonstd, conf.as_ref()));
        let spill = go!(Spillover::remove_or_drop_root_opt_with(
            std,
            nonstd,
            &ordered_names[..],
            conf
        ));

        let modif = ModificationData::lookup(std, nonstd, conf)
            .map_warnings_and_errors(LookupMetarootWarning::from);
        let ts = Timestamps::lookup(std, nonstd, conf)
            .map_warnings_and_errors(LookupMetarootWarning::from);
        let dt = Datetimes::lookup(std, nonstd, conf)
            .map_warnings_and_errors(LookupMetarootWarning::from);
        let agates = AppliedGates3_2::lookup(std, nonstd, conf)
            .map_warnings_and_errors(LookupMetarootWarning::from);

        let cyt = Cyt3_2::remove_metaroot_req(std)
            .map_err(LookupMetarootError::from)
            .into_log();

        dt.zip4_commutative(modif, mode, spill)
            .zip5_commutative(ts, us, vol, agates)
            .map_errors(LookupMetarootError::from)
            .zip_commutative(cyt)
            .map_ok_value(|(((d, md, mo, sp_out), t, u_out, v, ag), c)| {
                let (sp, sp_trimmed) = sp_out.into_opt_root_pair();
                let ret = Self::new(
                    mo, t, d, c, sp, cytsn, md, plate, v, carrier, u_out.this, flow, ag,
                );
                let trimmed = sp_trimmed.into_iter().chain(u_out.trimmed).collect();
                (ret, trimmed)
            })
    }
}

impl VersionedMetaroot for InnerMetaroot2_0 {
    type Ver = Version2_0;
    type Optical = InnerOptical2_0;
    type Temporal = InnerTemporal2_0;
    type Name = Option<Shortname>;

    fn meas_invalid_named_links_inner(
        &self,
        _: &NamedSet<'_>,
    ) -> impl Iterator<Item = BrokenNamedLinkError> {
        empty()
    }

    fn meas_invalid_indexed_links_inner(
        &self,
        par: &Par,
    ) -> impl Iterator<Item = BrokenIndexedLinkError> {
        self.comp
            .as_ref()
            .into_iter()
            .flat_map(|comp| comp.invalid_link_errors(par))
            .map(BrokenIndexedLinkError::from)
    }

    fn remove_invalid_links(
        &mut self,
        par: &Par,
        _: &NamedSet<'_>,
    ) -> impl Iterator<Item = RemovedLink> {
        Compensation2_0::remove_invalid_link(&mut self.comp, *par).into_iter()
    }

    fn deprecated(&mut self, _: private::NoTouchy) -> impl Iterator<Item = DeprecatedRef<'_>> {
        empty()
    }

    fn meas_has_existing_named_links_with_inner(
        &self,
        _: &OpticalNamesToRemove<'_>,
    ) -> impl Iterator<Item = AnyExistingNamedLinkError> {
        empty()
    }

    fn meas_has_existing_index_links_with_inner(
        &self,
        _: Par,
        _: &IndicesToRemove,
    ) -> impl Iterator<Item = AnyExistingIndexLinkError> {
        // don't check specific indices for $COMP since this keyword links
        // all indices
        self.comp.as_ref().into_iter().flat_map(|comp| {
            comp.existing_links()
                .map(AnyExistingIndexLinkError::Comp2_0)
        })
    }

    fn rename_meas_links_inner(&mut self, _: &NameMapping) {}

    fn insert_meas_index_inner(&mut self, i: MeasIndex) {
        if let Some(x) = self.comp.as_mut() {
            x.0.insert_identity_by_index_unchecked(i);
        }
    }

    fn keywords_req_inner(&self) -> impl Iterator<Item = (String, String)> {
        [self.mode.pair()].into_iter()
    }

    fn keywords_opt_inner(&self) -> impl Iterator<Item = (String, String)> {
        once(self.cyt.metaroot_opt_pair())
            .filter_map(|(k, v)| v.map(|x| (k, x)))
            .chain(self.applied_gates.opt_keywords())
            .chain(self.timestamps.opt_keywords())
            .chain(
                self.comp
                    .as_ref()
                    .map(Compensation2_0::opt_keywords)
                    .into_iter()
                    .flatten(),
            )
    }

    fn swap_optical_temporal_inner(
        t: Self::Temporal,
        o: Self::Optical,
    ) -> (Self::Optical, Self::Temporal) {
        let new_t = Self::Temporal::new(o.scale.is_some(), o.peak);
        let new_o = Self::Optical::new(bool::from(t.scale).then_some(Scale::Linear), None, t.peak);
        (new_o, new_t)
    }
}

impl VersionedMetaroot for InnerMetaroot3_0 {
    type Ver = Version3_0;
    type Optical = InnerOptical3_0;
    type Temporal = InnerTemporal3_0;
    type Name = Option<Shortname>;

    fn meas_invalid_named_links_inner(
        &self,
        _: &NamedSet<'_>,
    ) -> impl Iterator<Item = BrokenNamedLinkError> {
        empty()
    }

    fn meas_invalid_indexed_links_inner(
        &self,
        par: &Par,
    ) -> impl Iterator<Item = BrokenIndexedLinkError> {
        let comp = self
            .comp
            .as_ref()
            .and_then(|comp| comp.invalid_link_errors(*par))
            .map(BrokenIndexedLinkError::from);
        self.applied_gates
            .invalid_link_errors(par)
            .map(BrokenIndexedLinkError::from)
            .chain(comp)
    }

    fn remove_invalid_links(
        &mut self,
        par: &Par,
        _: &NamedSet<'_>,
    ) -> impl Iterator<Item = RemovedLink> {
        let comp =
            Compensation3_0::remove_invalid_link(&mut self.comp, *par).map(RemovedLink::from);
        let ag = self.applied_gates.remove_invalid_links(par);
        comp.into_iter().chain(ag)
    }

    fn deprecated(&mut self, _: private::NoTouchy) -> impl Iterator<Item = DeprecatedRef<'_>> {
        empty()
    }

    fn meas_has_existing_named_links_with_inner(
        &self,
        _: &OpticalNamesToRemove<'_>,
    ) -> impl Iterator<Item = AnyExistingNamedLinkError> {
        empty()
    }

    fn meas_has_existing_index_links_with_inner(
        &self,
        par: Par,
        indices: &IndicesToRemove,
    ) -> impl Iterator<Item = AnyExistingIndexLinkError> {
        // don't check specific indices for $COMP since this keyword links
        // all indices
        let comp = self.comp.as_ref().and_then(|_| {
            NonEmpty::collect((0..par.0).map(IndexFromOne::from))
                .map(|js| ExistingIndexedLinkError::new(Key0::default(), js))
                .map(AnyExistingIndexLinkError::from)
        });
        let ag = self
            .applied_gates
            .existing_link_errors(indices)
            .map(AnyExistingIndexLinkError::from);
        ag.chain(comp)
    }

    fn rename_meas_links_inner(&mut self, _: &NameMapping) {}

    fn insert_meas_index_inner(&mut self, i: MeasIndex) {
        if let Some(x) = self.comp.as_mut() {
            x.0.insert_identity_by_index_unchecked(i);
        }
        self.applied_gates.shift_meas_indices_after_insert(i);
    }

    fn keywords_req_inner(&self) -> impl Iterator<Item = (String, String)> {
        [self.mode.pair()].into_iter()
    }

    fn keywords_opt_inner(&self) -> impl Iterator<Item = (String, String)> {
        [
            self.cyt.metaroot_opt_pair(),
            self.comp.metaroot_opt_pair(),
            self.cytsn.metaroot_opt_pair(),
            self.unicode.metaroot_opt_pair(),
        ]
        .into_iter()
        .filter_map(|(k, v)| v.map(|x| (k, x)))
        .chain(self.applied_gates.opt_keywords())
        .chain(self.subset.opt_keywords())
        .chain(self.timestamps.opt_keywords())
    }

    fn swap_optical_temporal_inner(
        t: Self::Temporal,
        o: Self::Optical,
    ) -> (Self::Optical, Self::Temporal) {
        let new_t = Self::Temporal::new(t.timestep, o.peak);
        let new_o = Self::Optical::new(ScaleTransform::default(), None, t.peak);
        (new_o, new_t)
    }
}

impl VersionedMetaroot for InnerMetaroot3_1 {
    type Ver = Version3_1;
    type Optical = InnerOptical3_1;
    type Temporal = InnerTemporal3_1;
    type Name = Identity<Shortname>;

    fn meas_invalid_named_links_inner(
        &self,
        names: &NamedSet<'_>,
    ) -> impl Iterator<Item = BrokenNamedLinkError> {
        self.spillover
            .as_ref()
            .into_iter()
            .flat_map(|sp| sp.invalid_link_errors(names))
            .map(BrokenNamedLinkError::from)
    }

    fn meas_invalid_indexed_links_inner(
        &self,
        par: &Par,
    ) -> impl Iterator<Item = BrokenIndexedLinkError> {
        self.applied_gates
            .invalid_link_errors(par)
            .map(BrokenIndexedLinkError::from)
    }

    fn remove_invalid_links(
        &mut self,
        par: &Par,
        names: &NamedSet<'_>,
    ) -> impl Iterator<Item = RemovedLink> {
        let spill = Spillover::remove_invalid_link(&mut self.spillover, names);
        self.applied_gates
            .remove_invalid_links(par)
            .chain(spill.map(RemovedLink::from))
    }

    fn deprecated(&mut self, _: private::NoTouchy) -> impl Iterator<Item = DeprecatedRef<'_>> {
        self.applied_gates.deprecated().map(DeprecatedRef::from)
    }

    fn meas_has_existing_named_links_with_inner(
        &self,
        names: &OpticalNamesToRemove<'_>,
    ) -> impl Iterator<Item = AnyExistingNamedLinkError> {
        self.spillover
            .as_ref()
            .and_then(|s| s.existing_link_error(names))
            .map(AnyExistingNamedLinkError::from)
            .into_iter()
    }

    fn meas_has_existing_index_links_with_inner(
        &self,
        _: Par,
        indices: &IndicesToRemove,
    ) -> impl Iterator<Item = AnyExistingIndexLinkError> {
        self.applied_gates
            .existing_link_errors(indices)
            .map(AnyExistingIndexLinkError::from)
    }

    fn rename_meas_links_inner(&mut self, mapping: &NameMapping) {
        if let Some(s) = self.spillover.as_mut() {
            s.reassign(mapping);
        }
    }

    fn insert_meas_index_inner(&mut self, i: MeasIndex) {
        self.applied_gates.shift_meas_indices_after_insert(i);
    }

    fn keywords_req_inner(&self) -> impl Iterator<Item = (String, String)> {
        [self.mode.pair()].into_iter()
    }

    fn keywords_opt_inner(&self) -> impl Iterator<Item = (String, String)> {
        [
            self.cyt.metaroot_opt_pair(),
            self.spillover.metaroot_opt_pair(),
            self.cytsn.metaroot_opt_pair(),
            self.vol.metaroot_opt_pair(),
        ]
        .into_iter()
        .filter_map(|(k, v)| v.map(|x| (k, x)))
        .chain(self.applied_gates.opt_keywords())
        .chain(self.subset.opt_keywords())
        .chain(self.modification.opt_keywords())
        .chain(self.plate.opt_keywords())
        .chain(self.timestamps.opt_keywords())
    }

    fn swap_optical_temporal_inner(
        t: Self::Temporal,
        o: Self::Optical,
    ) -> (Self::Optical, Self::Temporal) {
        let new_t = Self::Temporal::new(t.timestep, o.display, o.peak);
        let new_o = Self::Optical::new(
            ScaleTransform::default(),
            Wavelengths::default(),
            None,
            t.display,
            t.peak,
        );
        (new_o, new_t)
    }
}

impl VersionedMetaroot for InnerMetaroot3_2 {
    type Ver = Version3_2;
    type Optical = InnerOptical3_2;
    type Temporal = InnerTemporal3_2;
    type Name = Identity<Shortname>;

    fn meas_invalid_named_links_inner(
        &self,
        names: &NamedSet<'_>,
    ) -> impl Iterator<Item = BrokenNamedLinkError> {
        let sp = self
            .spillover
            .as_ref()
            .into_iter()
            .flat_map(|sp| sp.invalid_link_errors(names))
            .map(BrokenNamedLinkError::from);
        self.unstained
            .unstainedcenters
            .invalid_link_error(names)
            .map(BrokenNamedLinkError::from)
            .chain(sp)
    }

    fn meas_invalid_indexed_links_inner(
        &self,
        par: &Par,
    ) -> impl Iterator<Item = BrokenIndexedLinkError> {
        self.applied_gates
            .invalid_link_errors(par)
            .map(BrokenIndexedLinkError::from)
    }

    fn remove_invalid_links(
        &mut self,
        par: &Par,
        names: &NamedSet<'_>,
    ) -> impl Iterator<Item = RemovedLink> {
        let uc = self.unstained.unstainedcenters.remove_invalid_links(names);
        let spill = Spillover::remove_invalid_link(&mut self.spillover, names);
        self.applied_gates
            .0
            .remove_invalid_links(par)
            .chain(spill.map(RemovedLink::from))
            .chain(uc.map(RemovedLink::from))
    }

    fn deprecated(&mut self, _: private::NoTouchy) -> impl Iterator<Item = DeprecatedRef<'_>> {
        let a = self.timestamps.deprecated().map(DeprecatedRef::from);
        let b = DeprecatedRef::from(&mut self.mode);
        let c = self.applied_gates.0.deprecated().map(DeprecatedRef::from);
        self.plate
            .deprecated()
            .map(DeprecatedRef::from)
            .chain(a)
            .chain(once(b))
            .chain(c)
    }

    fn meas_has_existing_named_links_with_inner(
        &self,
        names: &OpticalNamesToRemove<'_>,
    ) -> impl Iterator<Item = AnyExistingNamedLinkError> {
        let spill = self
            .spillover
            .as_ref()
            .and_then(|s| s.existing_link_error(names))
            .map(AnyExistingNamedLinkError::from);
        let us = self
            .unstained
            .unstainedcenters
            .existing_link_error(names)
            .map(AnyExistingNamedLinkError::from);
        [spill, us].into_iter().flatten()
    }

    fn meas_has_existing_index_links_with_inner(
        &self,
        _: Par,
        indices: &IndicesToRemove,
    ) -> impl Iterator<Item = AnyExistingIndexLinkError> {
        self.applied_gates
            .existing_link_errors(indices)
            .map(AnyExistingIndexLinkError::from)
    }

    fn rename_meas_links_inner(&mut self, mapping: &NameMapping) {
        if let Some(x) = self.spillover.as_mut() {
            x.reassign(mapping);
        }
        self.unstained.unstainedcenters.reassign(mapping);
    }

    fn insert_meas_index_inner(&mut self, i: MeasIndex) {
        self.applied_gates.shift_meas_indices_after_insert(i);
    }

    fn keywords_req_inner(&self) -> impl Iterator<Item = (String, String)> {
        [self.cyt.pair()].into_iter()
    }

    fn keywords_opt_inner(&self) -> impl Iterator<Item = (String, String)> {
        [
            self.mode.metaroot_opt_pair(),
            self.spillover.metaroot_opt_pair(),
            self.cytsn.metaroot_opt_pair(),
            self.vol.metaroot_opt_pair(),
            self.flowrate.metaroot_opt_pair(),
        ]
        .into_iter()
        .filter_map(|(k, v)| v.map(|x| (k, x)))
        .chain(self.applied_gates.opt_keywords())
        .chain(self.unstained.opt_keywords())
        .chain(self.modification.opt_keywords())
        .chain(self.carrier.opt_keywords())
        .chain(self.plate.opt_keywords())
        .chain(self.timestamps.opt_keywords())
        .chain(self.datetimes.opt_keywords())
    }

    fn swap_optical_temporal_inner(
        t: Self::Temporal,
        o: Self::Optical,
    ) -> (Self::Optical, Self::Temporal) {
        let new_t = Self::Temporal::new(t.timestep, o.display, TemporalType::default());
        let new_o = Self::Optical::new(
            ScaleTransform::default(),
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

impl Temporal2_0 {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new_2_0(
        has_scale: bool,
        bin: Option<PeakBin>,
        size: Option<PeakIndex>,
        longname: Longname,
        nonstandard_keywords: NonStdKeywords,
    ) -> Self {
        let common = CommonMeasurement::new(longname, nonstandard_keywords);
        let specific = InnerTemporal2_0::new(has_scale, PeakData::new(bin, size));
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

impl Optical2_0 {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new_2_0(
        scale: Option<Scale>,
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
        let specific = InnerOptical2_0::new(scale, wavelength, PeakData::new(bin, size));
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
        transform: ScaleTransform,
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
        let specific = InnerOptical3_0::new(transform, wavelength, PeakData::new(bin, size));
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
        transform: ScaleTransform,
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
        let specific = InnerOptical3_1::new(
            transform,
            wavelengths,
            calibration,
            display,
            PeakData::new(bin, size),
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

impl Optical3_2 {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new_3_2(
        transform: ScaleTransform,
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
            transform,
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

    // pub fn new_def(scale: Scale) -> Self {
    //     let specific = InnerOptical3_2::new_def(scale);
    //     Self::new_common(specific)
    // }
}

impl<X> AsMut<CommonMeasurement> for Optical<X> {
    fn as_mut(&mut self) -> &mut CommonMeasurement {
        &mut self.common
    }
}

impl<X> AsMut<CommonMeasurement> for Temporal<X> {
    fn as_mut(&mut self) -> &mut CommonMeasurement {
        &mut self.common
    }
}

impl<X> AsRef<CommonMeasurement> for Optical<X> {
    fn as_ref(&self) -> &CommonMeasurement {
        &self.common
    }
}

impl<X> AsRef<CommonMeasurement> for Temporal<X> {
    fn as_ref(&self) -> &CommonMeasurement {
        &self.common
    }
}

impl AnalysisReader {
    pub(crate) fn h_read<R: Read + Seek>(&self, h: &mut BufReader<R>) -> io::Result<Analysis> {
        let mut buf = vec![];
        self.seg.h_read_contents(h, &mut buf)?;
        Ok(buf.into())
    }
}

impl OthersReader<'_> {
    pub(crate) fn h_read<R: Read + Seek>(&self, h: &mut BufReader<R>) -> io::Result<Others> {
        let mut buf = vec![];
        let mut others = vec![];
        for s in self.segs {
            s.h_read_contents(h, &mut buf)?;
            others.push(Other(buf.clone()));
            buf.clear();
        }
        Ok(Others(others))
    }
}

/// Error when converting [`Core`] to new FCS version
#[derive(Debug, Display, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ConvertError {
    Rewrap(NameConversionError),
    Meta(MetarootConvertError),
    Optical(OpticalConvertError),
    Temporal(AnyTemporalKeyLossError),
    Layout(LayoutConvertError),
}

type MetarootConvertResult<M> =
    WarningsAndErrorsResult<M, (), MetarootConvertWarning, MetarootConvertError>;

type OpticalConvertResult<M> =
    WarningsAndErrorsResult<M, (), OpticalConvertWarning, OpticalConvertError>;

type TemporalConvertResult<M> = DeferredSwitchableErrors<M, AllowLoss, AnyTemporalKeyLossError>;

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

/// Error when $PnN is optional and missing in current version and required in target
#[derive(Debug, Error)]
#[error("{0} is required in target version but missing in current version")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ConversionError))]
pub struct NameConversionError(Key1<Shortname>);

/// Error when writing [`CoreDataset`] to file
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum StdWriterError {
    Layout(NewDataLayoutError),
    Check(IndexedLossError),
    Overflow(Uint8DigitOverflowError),
}

/// Error when setting measurements vector
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum SetMeasurementsError {
    New(MeasurementsWithLayoutError),
    Link(SetMeasurementLinkErrors),
}

/// Link error when setting new measurements
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum SetMeasurementLinkError {
    NamedBroken(BrokenNamedLinkError),
    IndexedBroken(BrokenIndexedLinkError),
    NamedExisting(AnyExistingNamedLinkError),
    IndexedExisting(AnyExistingIndexLinkError),
}

pub type SetMeasurementLinkErrors = ErrorGroup<SetMeasurementLinkError, SetMeasurementLinkSummary>;

def_summary!(
    SetMeasurementLinkSummary,
    "link errors when setting measurements"
);

/// Error when setting measurements vector without names
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum SetUnnamedMeasurementsError {
    New(MeasLayoutMismatchError),
    Set(SetValuesError),
}

/// Error when setting $PnE for all measurements (3.0+)
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum SetScalesError {
    Layout(MeasLayoutMismatchError),
    Temporal(NonLinearTemporalScaleError),
}

/// Error when setting $PnE/PnG for all measurements (3.0+)
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum SetTransformsError {
    Layout(MeasLayoutMismatchError),
    Temporal(NonLinearTemporalTransformError),
}

/// Error when setting measurements and DATA/dataframe simultaneously
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum SetMeasurementsAndDataError {
    Meas(SetMeasurementsError),
    Mismatch(MeasDataMismatchError),
}

/// Error when setting measurements without $PnN and DATA/dataframe simultaneously
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum SetUnnamdMeasurementsAndDataError {
    Meas(SetUnnamedMeasurementsError),
    Mismatch(MeasDataMismatchError),
}

/// Error when setting dataframe/DATA segment in [`CoreDataset`]
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ColumnsToDataframeError {
    New(df::NewDataframeError),
    Mismatch(MeasDataMismatchError),
}

/// Error when removing measurement by name ($PnN)
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum RemoveMeasByNameError {
    Link(ExistingLinkErrors),
    Name(NameNotFoundError),
}

/// Error when removing measurement by index
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum RemoveMeasByIndexError {
    Link(ExistingLinkErrors),
    Index(ElementIndexError),
}

/// Error when pushing a temporal measurement into [`CoreTEXT`]
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum PushTemporalError {
    Center(PushCenterError),
    Layout(InsertRangeError),
}

/// Error when inserting a temporal measurement into [`CoreTEXT`]
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum InsertTemporalError {
    Center(InsertCenterError),
    Layout(InsertRangeError),
}

/// Error when pushing an optical measurement into [`CoreTEXT`]
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum PushOpticalError {
    Unique(NamePresentError),
    Layout(InsertRangeError),
}

/// Error when inserting an optical measurement into [`CoreTEXT`]
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum InsertOpticalError {
    Insert(InsertError),
    Layout(InsertRangeError),
}

/// Error when pushing a temporal measurement into [`CoreDataset`]
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum PushTemporalToDatasetError {
    Measurement(PushTemporalError),
    Column(df::ColumnLengthError),
}

/// Error when inserting a temporal measurement into [`CoreDataset`]
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum InsertTemporalToDatasetError {
    Measurement(InsertTemporalError),
    Column(df::ColumnLengthError),
}

/// Error when pushing an optical measurement into [`CoreDataset`]
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum PushOpticalToDatasetError {
    Measurement(PushOpticalError),
    Column(df::ColumnLengthError),
}

/// Error when inserting an optical measurement into [`CoreDataset`]
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum InsertOpticalInDatasetError {
    Measurement(InsertOpticalError),
    Column(df::ColumnLengthError),
}

/// Error when measurement vector is not the same length as columns in DATA/dataframe
#[derive(Debug, Error)]
#[error("measurement number ({meas_n}) does not match dataframe column number ({data_n})")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::RelationalError))]
pub struct MeasDataMismatchError {
    meas_n: usize,
    data_n: usize,
}

/// Error when attempting to set temporal $PnE to log (2.0)
#[derive(Debug, Error)]
#[error("tried to set temporal $PnE to nonlinear scale")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::RelationalError))]
pub struct NonLinearTemporalScaleError;

/// Error when attempting to set temporal $PnE/$PnG to non-unitary transform (3.0+)
#[derive(Debug, Error)]
#[error("tried to set temporal $PnE/$PnG to nonlinear transform")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::RelationalError))]
pub struct NonLinearTemporalTransformError;

/// Error when reading standardized TEXT from keyword pairs
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum StdTEXTFromKeywordsError {
    Error(StdTEXTFromFlatTEXTErrorInner),
    Warn(StdTEXTFromFlatTEXTWarning),
}

/// Error when reading standardized TEXT from keyword pairs
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum StdTEXTFromFlatTEXTError {
    Inner(StdTEXTFromFlatTEXTErrorInner),
    Version(GuessVersionError),
}

/// Error (inner) when reading standardized TEXT from keyword pairs
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum StdTEXTFromFlatTEXTErrorInner {
    New(LookupCoreError),
    Metaroot(LookupMetarootError),
    Meas(LookupMeasurementError),
    Shortname(LookupShortnameError),
    Layout(LookupLayoutError),
    Offsets(LookupTEXTOffsetsError),
    Timestep(TimestepFoundError),
    Pseudo(PseudostandardError),
    HyperPar(HyperParError),
    HyperGate(HyperGateError),
    OtherVersion(KeywordOtherVersionError),
}

/// Warning when reading standardized TEXT from keyword pairs
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum StdTEXTFromFlatTEXTWarning {
    New(NewCoreWarning),
    Pattern(NonStdMeasRegexError),
    Metaroot(LookupMetarootWarning),
    Meas(LookupMeasurementWarning),
    Shortname(OptIndexedKeyError<Shortname>),
    Layout(LookupLayoutWarning),
    Offsets(LookupTEXTOffsetsWarning),
    Timestep(TimestepFoundError),
    Pseudo(PseudostandardError),
    HyperPar(HyperParError),
    HyperGate(HyperGateError),
    OtherVersion(KeywordOtherVersionError),
}

/// Error when reading standardized DATA from keyword pairs
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum StdDatasetFromFlatTextError {
    Inner(StdDatasetFromFlatTextErrorInner),
    Version(GuessVersionError),
}

/// Error (inner) when reading standardized DATA from keyword pairs
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum StdDatasetFromFlatTextErrorInner {
    DatasetOffset(DatasetOffsetError),
    TEXT(StdTEXTFromFlatTEXTErrorInner),
    Dataframe(ReadDataframeError),
    Offsets(LookupTEXTOffsetsError),
    Segment(RelativeToAbsSegmentError),
    Warn(StdDatasetFromFlatTEXTWarning),
}

/// Warning when reading standardized DATA from keyword pairs
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum StdDatasetFromFlatTEXTWarning {
    TEXT(StdTEXTFromFlatTEXTWarning),
    Offsets(LookupTEXTOffsetsWarning),
    Layout(ReadDataframeWarning),
}

/// Error when $PnE is not set on optical measurement and target version requires it
#[derive(Debug, Error)]
#[error("{} must be set before converting measurement", Scale::std(self.0))]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ConversionError))]
pub struct NoScaleError(MeasIndex);

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
    NonLinear(OpticalNonLinearError),
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
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum OpticalToTemporalError {
    NonLinear(OpticalNonLinearError),
    OpticalToTemporal(AnyOpticalToTemporalKeyLossError),
}

/// Error when optical measurement is non-unitary but is to be converted to temporal.
#[derive(Debug, Error, new)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::RelationalError))]
pub struct OpticalNonLinearError {
    index: MeasIndex,
    version: Version,
}

impl fmt::Display for OpticalNonLinearError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let i = self.index;
        let e = Scale::std(i);
        if self.version < Version::FCS3_0 {
            write!(f, "{e} must be '0,0'")
        } else {
            let g = Gain::std(i);
            write!(f, "{e} must be '0,0' and {g} must be null or unity")
        }
    }
}

/// Error when metaroot is changed to new FCS version
///
/// Most of these only apply to very specific version combinations.
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum MetarootConvertError {
    NoCyt(NoCytError),
    Mode(ModeUpgradeError),
    Gates3_0To2_0(AppliedGates3_0To2_0Error),
    Gates3_0To3_2(AppliedGates3_0To3_2Error),
    Loss(AnyMetarootKeyLossError),
}

/// Warning when metaroot is changed to new FCS version
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum MetarootConvertWarning {
    Mode(ModeUpgradeError),
    Gates3_0To2_0(AppliedGates3_0To2_0Error),
    Gates3_0To3_2(AppliedGates3_0To3_2Error),
    Loss(AnyMetarootKeyLossError),
    Optical(OpticalConvertWarning),
    Temporal(AnyTemporalKeyLossError),
}

/// Error when a metaroot keyword will be lost when converting versions
#[derive(From, Display, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum AnyMetarootKeyLossError {
    Cytsn(UnitaryKeyLossError<Cytsn>),
    Unicode(UnitaryKeyLossError<Unicode>),
    Vol(UnitaryKeyLossError<Vol>),
    Flowrate(UnitaryKeyLossError<Flowrate>),
    Comp2_0(BiIndexedKeyLossError<Dfc>),
    Comp3_0(UnitaryKeyLossError<Compensation3_0>),
    Spillover(UnitaryKeyLossError<Spillover>),
    Unstained(UnstainedLossError),
    Datetime(DatetimeLossError),
    Carrier(CarrierLossError),
    Plate(PlateLossError),
    Modification(ModificationLossError),
    Subset(SubsetLossError),
    // TODO maybe explain why these are dropped. The scheme keywords are present
    // in 3.2 but are incompabible with 2.0 since they reference measurements
    // rather than Gn* keywords.
    AppliedGates2_0To3_2(AppliedGates2_0To3_2LossError),
    // TODO ditto (mostly)
    AppliedGates3_2To2_0(GatingSchemeLossError),
}

/// Error when an optical keyword will be lost when converting versions
#[derive(From, Display, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum AnyOpticalKeyLossError {
    Filter(IndexedKeyLossError<Filter>),
    Power(IndexedKeyLossError<Power>),
    DetectorType(IndexedKeyLossError<DetectorType>),
    PercentEmitted(IndexedKeyLossError<PercentEmitted>),
    DetectorVoltage(IndexedKeyLossError<DetectorVoltage>),
    Wavelength(IndexedKeyLossError<Wavelength>),
    Wavelengths(IndexedKeyLossError<Wavelengths>),
    MeasType(IndexedKeyLossError<OpticalType>),
    TempType(IndexedKeyLossError<TemporalType>),
    Analyte(IndexedKeyLossError<Analyte>),
    Tag(IndexedKeyLossError<Tag>),
    Gain(IndexedKeyLossError<Gain>),
    Display(IndexedKeyLossError<Display>),
    DetectorName(IndexedKeyLossError<DetectorName>),
    Feature(IndexedKeyLossError<Feature>),
    Calibration3_1(IndexedKeyLossError<Calibration3_1>),
    Calibration3_2(IndexedKeyLossError<Calibration3_2>),
    Peak(PeakLossError),
}

/// Error when a temporal keyword will be lost when converting versions
#[derive(From, Display, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum AnyTemporalKeyLossError {
    TempType(IndexedKeyLossError<TemporalType>),
    Display(IndexedKeyLossError<Display>),
    Timestamp(UnitaryKeyLossError<Timestep>),
    Peak(PeakLossError),
}

/// Error when an optical keyword will be lost when converting to temporal
#[derive(From, Display, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum AnyOpticalToTemporalKeyLossError {
    Filter(IndexedKeyLossError<Filter>),
    Power(IndexedKeyLossError<Power>),
    DetectorType(IndexedKeyLossError<DetectorType>),
    PercentEmitted(IndexedKeyLossError<PercentEmitted>),
    DetectorVoltage(IndexedKeyLossError<DetectorVoltage>),
    Wavelength(IndexedKeyLossError<Wavelength>),
    Wavelengths(IndexedKeyLossError<Wavelengths>),
    MeasType(IndexedKeyLossError<OpticalType>),
    Analyte(IndexedKeyLossError<Analyte>),
    Tag(IndexedKeyLossError<Tag>),
    Gain(IndexedKeyLossError<Gain>),
    DetectorName(IndexedKeyLossError<DetectorName>),
    Feature(IndexedKeyLossError<Feature>),
    Calibration3_1(IndexedKeyLossError<Calibration3_1>),
    Calibration3_2(IndexedKeyLossError<Calibration3_2>),
}

/// Error when a temporal keyword will be lost when converting to optical
#[derive(From, Display, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum AnyTemporalToOpticalKeyLossError {
    TempType(IndexedKeyLossError<TemporalType>),
}

/// Error when $PKn and $PKNn keywords would be lost due to version change
#[derive(From, Display, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum PeakLossError {
    Bin(IndexedKeyLossError<PeakBin>),
    Number(IndexedKeyLossError<PeakIndex>),
}

/// Error when "plate" keywords would be lost due to version change
#[derive(From, Display, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum PlateLossError {
    Platename(UnitaryKeyLossError<Platename>),
    Plateid(UnitaryKeyLossError<Plateid>),
    Wellid(UnitaryKeyLossError<Wellid>),
}

/// Error when "subset" keywords would be lost due to version change
#[derive(From, Display, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum SubsetLossError {
    Bits(UnitaryKeyLossError<CSVBits>),
    Flag(CSVFlagsLossError),
}

/// Error when $CSVnFLAG and $CSMode keywords would be lost due to version change
#[derive(From, Display, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum CSVFlagsLossError {
    CSMode(UnitaryKeyLossError<CSMode>),
    CSVFlag(IndexedKeyLossError<CSVFlag>),
}

/// Error when "modification" keywords would be lost due to version change
#[derive(From, Display, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ModificationLossError {
    LastModifier(UnitaryKeyLossError<LastModifier>),
    LastModified(UnitaryKeyLossError<LastModified>),
    Originality(UnitaryKeyLossError<Originality>),
}

/// Error when "carrier" keywords would be lost due to version change
#[derive(From, Display, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum CarrierLossError {
    Carrierid(UnitaryKeyLossError<Carrierid>),
    Locationid(UnitaryKeyLossError<Locationid>),
    Carriertype(UnitaryKeyLossError<Carriertype>),
}

/// Error when $UNSTAINED* keywords would be lost due to version change
#[derive(From, Display, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum UnstainedLossError {
    UnstainedCenters(UnitaryKeyLossError<UnstainedCenters>),
    UnstainedInfo(UnitaryKeyLossError<UnstainedInfo>),
}

/// Error when reading DATA segment from already-parsed keywords
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupAndReadDataAnalysisError {
    DatasetOffset(DatasetOffsetError),
    Par(ReqKeyError<Par>),
    Offsets(LookupTEXTOffsetsError),
    Layout(LookupLayoutError),
    Dataframe(ReadDataframeError),
    Segment(RelativeToAbsSegmentError),
    Warn(LookupAndReadDataAnalysisWarning),
}

/// Warning when reading DATA segment from already-parsed keywords
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupAndReadDataAnalysisWarning {
    Offsets(LookupTEXTOffsetsWarning),
    Layout(LookupLayoutWarning),
    Data(ReadDataframeWarning),
}

/// Error when looking up offsets for parsing DATA
///
/// Note that not every error applies to every version.
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupTEXTOffsetsError {
    /// $TOT is missing (2.0+)
    Tot2(OptKeyError<Tot>),
    /// $TOT is missing (3.0+)
    Tot3(ReqKeyError<Tot>),
    /// required DATA keywords are missing (3.0/3.1)
    ReqData(ReqSegmentWithDefaultError<DataSegmentId>),
    /// required ANALYSIS keywords are missing (3.0/3.1)
    ReqAnalysis(ReqSegmentWithDefaultError<AnalysisSegmentId>),
    /// TEXT DATA segment does not match HEADER (3.0+)
    MismatchData(SegmentMismatchWarning<DataSegmentId>),
    /// required TEXT ANALYSIS segment does not match HEADER (3.0/3.1)
    MismatchAnalysis(SegmentMismatchWarning<AnalysisSegmentId>),
    /// optional TEXT ANALYSIS segment does not match HEADER (3.2)
    MismatchAnalysisOpt(OptSegmentWithDefaultWarning<AnalysisSegmentId>),
}

/// Warning when looking up offsets for parsing DATA
///
/// Note that not every warning applies to every version.
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupTEXTOffsetsWarning {
    /// $TOT is optional in FCS 2.0 (for some reason)
    Tot(OptKeyError<Tot>),
    /// TEXT DATA segment can be optionally be overridden by HEADER (3.0+)
    ReqData(ReqSegmentWithDefaultWarning<DataSegmentId>),
    /// TEXT ANALYSIS segment can be optionally be overridden by HEADER (3.0+)
    ReqAnalysis(ReqSegmentWithDefaultWarning<AnalysisSegmentId>),
    /// TEXT ANALYSIS segment does not match HEADER and is dropped (3.0+)
    MismatchAnalysis(OptSegmentWithDefaultWarning<AnalysisSegmentId>),
}

/// Error when building new [`CoreTEXT`]
///
/// The timestep/datetime errors are technically "relational" but are here and
/// not in NewCoreRelationalerror because each time/date object is created
/// prior to calling the function that would produce that error, and these
/// are validated for correct order.
///
/// Note that not every error applies to each version.
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum NewCoreTEXTError {
    /// Any new Core* error
    Core(NewCoreError),
    /// datetimes are flipped; all versions
    Timestamps(ReversedTimestampsError),
    /// datetimes are flipped; 3.2 only
    Datetimes(ReversedDatetimesError),
}

/// Error when making new [`CoreTEXT`] or [`CoreDataset`]
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum NewCoreError {
    /// Measurement vector has more than one time element
    Meas(NewNamedVecError),
    /// Measurement and layout are incompatible
    Layout(MeasLayoutMismatchError),
    /// Any other warning which is configured to be a fatal error
    Warn(NewCoreWarning),
}

/// Error when looking up [`CoreTEXT`] or [`CoreDataset`] from keywords
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupCoreError {
    /// Measurement vector has more than one time element
    Meas(NewNamedVecError),
    /// Measurement and layout are incompatible
    Layout(ScaleDatatypeMismatchError),
    /// Any other warning which is configured to be a fatal error
    Warn(NewCoreWarning),
}

/// Warning when building new [`CoreTEXT`]
///
/// Each of these are also errors but can be configured to only be warnings
/// if the user desires.
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum NewCoreWarning {
    /// Time channel is missing entirely
    Time(MissingTime),
    /// A keyword has invalid links (and is dropped in the case of a warning)
    Link(BrokenOrDependentLinkError),
    /// A keyword is deprecated
    Deprecated(AnyDepKeyError),
}

type LookupMetarootResult<V> =
    WarningsAndErrorsResult<V, (), LookupMetarootWarning, LookupMetarootError>;

/// Error when parsing any metaroot keyword
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupMetarootError {
    Mode(ReqKeyError<Mode>),
    Cyt3_2(ReqKeyError<Cyt3_2>),
    Par(ReqKeyError<Par>),
    Warn(LookupMetarootWarning),
}

/// Warning when parsing any metaroot keyword
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupMetarootWarning {
    Trigger(OptKeyStError<Trigger>),
    Comp2_0(LookupComp2_0Error),
    Comp3_0(OptKeyStError<Compensation3_0>),
    Timestamps2_0(LookupTimestampsError<FCSTime, FCSTimeError>),
    Timestamps3_0(LookupTimestampsError<FCSTime60, FCSTime60Error>),
    Timestamps3_1(LookupTimestampsError<FCSTime100, FCSTime100Error>),
    Datetimes(LookupDatetimesError),
    Modified(LookupModifiedDataError),
    UnstainedCenter(OptKeyStError<UnstainedCenters>),
    Mode3_2(OptKeyError<Mode3_2>),
    // NOTE this can never be an error even if we forbid deprecated keys
    // because there is no easy way to fix it (ie by dropping a key)
    Mode(DeprecatedModeWarning),
    Unicode(OptKeyStError<Unicode>),
    Spillover(OptKeyStError<Spillover>),
    Gate2_0(LookupAppliedGates2_0Error),
    Gate3_0(LookupAppliedGates3_0Error),
    Gate3_2(LookupAppliedGates3_2Error),
    Vol(OptKeyError<Vol>),
    Abrt(OptKeyError<Abrt>),
    Lost(OptKeyError<Lost>),
    Subset(LookupSubsetError),
}

type LookupMeasurementResult<V> =
    WarningsAndErrorsResult<V, (), LookupMeasurementWarning, LookupMeasurementError>;

/// Error when parsing any measurement keyword
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupMeasurementError {
    Temporal(LookupTemporalError),
    Optical(LookupOpticalError),
    TimeName(DuplicateTimeNameError),
    Warn(LookupMeasurementWarning),
}

/// Error when more than one $PnN matches the given time pattern
#[derive(Debug, Error)]
#[error(
    "Time pattern matched {k} with name {1} but a previous measurement already \
     matched; adjust time pattern so it only matches one $PnN",
    k = Shortname::std(self.0),
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ConfigError))]
pub struct DuplicateTimeNameError(MeasIndex, Shortname);

/// Warning when parsing any measurement keyword.
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupMeasurementWarning {
    Temporal(LookupTemporalWarning),
    Optical(LookupOpticalWarning),
    MissingTime(MissingTime),
}

type LookupShortnameResult<V> =
    WarningAndErrorResult<V, (), OptIndexedKeyError<Shortname>, LookupShortnameError>;

/// Error when parsing $PnN
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupShortnameError {
    Req(ReqIndexedKeyError<Shortname>),
    Opt(OptIndexedKeyError<Shortname>),
}

type LookupOpticalResult<V> =
    WarningsAndErrorsResult<V, (), LookupOpticalWarning, LookupOpticalError>;

/// Error when parsing any optical measurement keyword
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupOpticalError {
    Xform(ScaleTransformError),
    Scale(ReqIndexedStKeyError<Scale>),
    Warn(LookupOpticalWarning),
}

/// Warning when parsing any optical measurement keyword
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupOpticalWarning {
    Scale(OptIndexedKeyStError<Scale>),
    Gain(OptIndexedKeyError<Gain>),
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

type LookupTemporalResult<V> =
    WarningsAndErrorsResult<V, (), LookupTemporalWarning, LookupTemporalError>;

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

/// Error when parsing $CS* keywords.
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupSubsetError {
    Flags(LookupCSVFlagsError),
    Bits(OptKeyError<CSVBits>),
    Tot(OptKeyError<CSTot>),
}

/// Error when parsing $CSMODE or $CSVnFlag
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupCSVFlagsError {
    Mode(OptKeyError<CSMode>),
    Flag(OptIndexedKeyError<CSVFlag>),
}

/// Error when parsing keywords for $LAST_MODIFIED or $ORIGINALITY
///
/// Note that $LAST_MODIFIER is infallible.
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupModifiedDataError {
    LastModTime(OptKeyStError<LastModified>),
    Originality(OptKeyError<Originality>),
}

/// Error triggered when time measurement is missing but required.
#[derive(Debug, Error)]
#[error("Could not find time measurement matching '{0}'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::RelationalError))]
pub struct MissingTime(pub TimeMeasNamePattern);

type LookupTEXTOffsetsResult<T> =
    WarningsAndErrorsResult<T, (), LookupTEXTOffsetsWarning, LookupTEXTOffsetsError>;

/// Error when non-indexed key would be lost upon conversion
#[derive(Debug, Error, Display)]
#[display(bound(T: Key))]
#[display("{_0} must be dropped to convert")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ConversionError))]
#[cfg_attr(feature = "python", bound(T: Key))]
pub struct UnitaryKeyLossError<T>(pub Key0<T>);

impl<T> Default for UnitaryKeyLossError<T> {
    fn default() -> Self {
        Self(Key0::default())
    }
}

/// Error when indexed key would be lost upon conversion
#[derive(Debug, Error, Display)]
#[display(bound(T: IndexedKey))]
#[display("{_0} must be dropped to convert")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ConversionError))]
#[cfg_attr(feature = "python", bound(T: IndexedKey))]
pub struct IndexedKeyLossError<T>(pub Key1<T>);

/// Error when bi-indexed key would be lost upon conversion
#[derive(Debug, Error, Display)]
#[display(bound(T: BiIndexedKey))]
#[display("{_0} must be dropped to convert")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ConversionError))]
#[cfg_attr(feature = "python", bound(T: BiIndexedKey))]
pub struct BiIndexedKeyLossError<T>(pub Key2<T>);

/// Error when $PnE is log and $PnG is not 1.0 or None
#[derive(Debug, Error)]
#[error(
    "could not make scale transform with log scale \
     '{scale}' and non-unit gain '{gain}'"
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::RelationalError))]
pub struct ScaleTransformError {
    scale: Scale,
    gain: Gain,
}

/// Error when $COMP does not have the same number of rows/columns as $PAR
#[derive(Debug, Error)]
#[error("$COMP must have same row/column number as $PAR ({par}), got {comp}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::RelationalError))]
pub struct CompParMismatchError {
    par: usize,
    comp: usize,
}

type SetSpilloverErrors = ErrorGroup<KeyToNameLinkError<Spillover>, SetSpilloverSummary>;

def_summary!(SetSpilloverSummary, "error when setting $SPILLOVER");

type SetUnstainedCentersErrors =
    ErrorGroup<KeyToNameLinkError<UnstainedCenters>, SetUnstainedCentersSummary>;

def_summary!(
    SetUnstainedCentersSummary,
    "error when setting $UNSTAINEDCENTERS"
);

type SetOpticalError = SetElementsError<ErrorGroup<MeasMismatchError, SetOpticalSummary>>;

def_summary!(
    SetOpticalSummary,
    "attempted to assign incompatible optical measurement values"
);

type SetAllMeasError = SetElementsError<ErrorGroup<MeasMismatchError, SetAllMeasSummary>>;

def_summary!(
    SetAllMeasSummary,
    "attempted to assign incompatible optical and temporal measurement values"
);

/// Error when temporal type is assigned to optical measurement and vice versa.
#[derive(Debug, Error, new)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::RelationalError))]
pub struct MeasMismatchError {
    key_is_optical: bool,
    index: MeasIndex,
}

impl fmt::Display for MeasMismatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let k = self.index;
        if self.key_is_optical {
            write!(f, "optical index {k} must not be assigned empty-tuple")
        } else {
            write!(f, "temporal index {k} must be assigned empty tuple")
        }
    }
}

#[cfg(feature = "python")]
def_summary!(NewCoreTEXTSummary, "could not make new CoreTEXT");

#[cfg(feature = "python")]
def_summary!(NewCoreDatasetSummary, "could not make new CoreDataset");

#[derive(Display, new)]
#[display("could not convert version from {from} to {to}")]
pub struct ConvertSummary {
    from: Version,
    to: Version,
}

def_summary!(
    SetScalesSummary,
    "could not set scales for optical measurements"
);

def_summary!(
    SetTransformsSummary,
    "could not set scale transforms for optical measurements"
);

def_summary!(PushTemporalSummary, "could not push temporal measurement");

def_summary!(
    InsertTemporalSummary,
    "could not insert temporal measurement"
);

def_summary!(PushOpticalSummary, "could not push optical measurement");

def_summary!(InsertOpticalSummary, "could not insert optical measurement");

def_summary!(SetAppliedGatesSummary, "could not set gating keywords");

def_summary!(
    SetMeasurementsAndLayoutSummary,
    "could not set measurements and layout"
);

def_summary!(WriteDatasetSummary, "could not write FCS file");

def_summary!(
    CoreTEXTFromKeywordsSummary,
    "could not create new CoreTEXT from keywords"
);

def_summary!(
    StdDatasetWithKwsSummary,
    "could not read standardized dataset from keywords"
);

#[cfg(feature = "serde")]
mod serialize {
    use crate::core::AnyCore;
    use serde::{Serialize, ser::SerializeStruct as _};

    impl<A, D, O> Serialize for AnyCore<A, D, O>
    where
        A: Serialize,
        D: Serialize,
        O: Serialize,
    {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            let mut state = serializer.serialize_struct("AnyCore", 2)?;
            match self {
                Self::FCS2_0(x) => {
                    state.serialize_field("version", &x.fcs_version())?;
                    state.serialize_field("data", &x)?;
                }
                Self::FCS3_0(x) => {
                    state.serialize_field("version", &x.fcs_version())?;
                    state.serialize_field("data", &x)?;
                }
                Self::FCS3_1(x) => {
                    state.serialize_field("version", &x.fcs_version())?;
                    state.serialize_field("data", &x)?;
                }
                Self::FCS3_2(x) => {
                    state.serialize_field("version", &x.fcs_version())?;
                    state.serialize_field("data", &x)?;
                }
            }
            state.end()
        }
    }
}

#[cfg(feature = "python")]
mod python {
    use crate::python::InvalidKeywordValueError;
    use crate::text::ranged_float::PositiveFloat;

    use super::IncludeReqOrOpt;
    use super::IncludeRootOrMeas;
    use super::ScaleTransform;

    use pyo3::IntoPyObjectExt as _;
    use pyo3::exceptions::PyValueError;
    use pyo3::prelude::*;

    // $PnE/$PnG (3.0+) as a tuple like (f32) or (f32, f32) in python
    impl<'py> FromPyObject<'py> for ScaleTransform {
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

    impl<'py> IntoPyObject<'py> for ScaleTransform {
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

    impl<'py> FromPyObject<'py> for IncludeReqOrOpt {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let s = ob.extract::<String>()?;
            match s.as_str() {
                "req_only" => Ok(Self::Req_),
                "opt_only" => Ok(Self::Opt_),
                "both" => Ok(Self::Both),
                _ => Err(PyValueError::new_err(
                    "must be one of 'req_only', 'opt_only', or 'both'",
                )),
            }
        }
    }

    impl<'py> FromPyObject<'py> for IncludeRootOrMeas {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let s = ob.extract::<String>()?;
            match s.as_str() {
                "root_only" => Ok(Self::Root),
                "meas_only" => Ok(Self::Meas),
                "both" => Ok(Self::Both),
                _ => Err(PyValueError::new_err(
                    "must be one of 'root_only', 'meas_only', or 'both'",
                )),
            }
        }
    }
}

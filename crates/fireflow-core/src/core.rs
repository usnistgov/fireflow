//! Data structures representing standardized TEXT segment

use crate::api::HeaderAndSuppOffsets;
use crate::config::{
    AllowLoss, AppendFlag, AppendableFlag, ConfigFlag as _, DatasetOffset, DatasetOffsetError,
    DummyTriFlag, OverlapCorrectionLimit, ReadDataKeywordsConfig, ReadEventsConfig,
    ReadHeaderAndTEXTConfig, ReadOffsetConfig, ReadSharedConfig, ReadState, ReadStdKeywordsConfig,
    TemporalHasOpticalKeyError, WriteDatasetInnerConfig, WriteMultiConfig, WriteMultiDatasetConfig,
    WriteMultiTEXTConfig, WriteTEXTInnerConfig,
};
use crate::convert::UsizeExt as _;
use crate::data::{
    CastSeriesErrors, CheckedScaleTransform, ConvertFromLayout, DataFrame2_0, DataFrame3_0,
    DataFrame3_1, DataFrame3_2, DataFrameAsDataSchema, DataFrameCheckRanges, DataSchema2_0,
    DataSchema3_0, DataSchema3_1, DataSchema3_2, DataSchemaToDataFrameError,
    DataSchemaToEmptyDataFrame, EventOverRangeError, EventOverRangeSummary, EventsDiagnostics,
    IsTot, LayoutConvertError, LayoutDatatype, LayoutHeight as _, LayoutInsert, LayoutKeywords,
    LayoutNormalize, LayoutOptMeasKeywords, LayoutRemove, LayoutSize as _, LookupDataSchemaError,
    LookupDataSchemaWarning, MeasLayoutMismatchError, MeasurementsWithLayoutError,
    NewDataSchemaError, RangeAndSeries, ReadCheckedDataframeError, ReadCheckedDataframeWarning,
    ScaleDatatypeMismatchError, ScaleErrorGroup, VersionedDataFrame, VersionedDataSchema,
    WithPrimitiveDataFrame,
};
use crate::header::{
    GuessVersionError, HeaderKeywordsToWrite, KeywordVersionScores, WriteTEXTHeaderError,
    autodetect_version,
};
use crate::logging::{
    CommutativeResultIter as _, DeferredError, DeferredIter as _, DeferredSwitchableError,
    DeferredSwitchableErrors, DeferredWarningsAndErrors, ErrorGroup, ErrorResult, ErrorsResult,
    GroupResult, IOErrorGroup, ImpureError, LogResult, OptionExt as _, ResultExt as _, Success,
    SwitchableErrorResult, WarningAndErrorResult, WarningOrErrorResult, WarningsAndErrorsResult,
    WarningsAndGroupResult, WarningsAndIOGroupResult, io_to_log,
};
use crate::macros::def_summary;
use crate::match_many_to_one;
use crate::segment::{
    AnalysisSegmentId, AnyAnalysisSegment, AnyDataSegment, DataSegmentId, HeaderOrTextSegment,
    KeyedOptSegmentWithDefault as _, KeyedReqSegmentWithDefault as _, OptSegmentWithDefaultWarning,
    OtherSegment20, ReqSegmentWithDefaultError, ReqSegmentWithDefaultWarning, SegmentMismatchError,
    SegmentOverlapError, UncorrectedSegment,
};
use crate::text::compensation::{Compensation, Compensation2_0, LookupComp2_0Error};
use crate::text::datetimes::{
    BeginDateTime, Datetimes, EndDateTime, LookupDatetimesError, ReversedDatetimesError,
};
use crate::text::gating::{
    AppliedGates2_0, AppliedGates3_0, AppliedGates3_0To2_0Error, AppliedGates3_0To3_2Error,
    AppliedGates3_2, GatedMeasurements, LookupAppliedGates2_0Error, LookupAppliedGates3_0Error,
    LookupAppliedGates3_2Error,
};
use crate::text::index::{IndexFromOne, MeasIndex, RegionIndex};
use crate::text::keyword_enum::{
    AnyKeyword, AsKeywordPair as _, HasMembership as _, Keyword0FromValue as _,
    Keyword1FromValue as _, NonStdKeyword, OptKeyword, OptMeasKeyword, OptOpticalKeyword,
    OptPeakKeyword, OptRootKeyword, OptTemporalKeyword, ReqKeyword, ReqMeasKeyword, ReqRootKeyword,
    SplitKeyword, SplitKeyword1, StdOrNonStdOptMeasKeyword, StdOrNonStdOptRootKeyword,
};
use crate::text::keywords::{
    Abrt, AlphaNumType, Analyte, AnyMeasScaleFix, CSMode, CSTot, CSVBits, CSVFlag, Calibration3_1,
    Calibration3_2, CalibrationLossError, Carrierid, Carriertype, Cells, Com, Compensation3_0, Cyt,
    Cyt3_2, Cytsn, DetectorName, DetectorType, DetectorVoltage, Dfc, Display, Exp,
    ExtraStdKeywords, Feature, Fil, Filter, Flowrate, Gain, Gate, GateDetectorType,
    GateDetectorVoltage, GateFilter, GateLongname, GatePercentEmitted, GateRange, GateScale,
    GateShortname, HyperGateError, HyperParError, Inst, KeywordOtherVersionError, LastModified,
    LastModifier, Locationid, LogScale, Longname, LookupTemporalGainError, Lost, MeasOrGateIndex,
    Mode, Mode3_2, ModeUpgradeError, Nextdata, NoCytError, Op, OpticalScaleFix, OpticalType,
    Originality, Par, PeakBin, PeakIndex, PercentEmitted, Plateid, Platename, Power,
    PrefixedMeasIndex, Proj, PseudostandardError, Scale, ScaleFix, Smno, Src, Sys, Tag,
    TemporalScale2_0, TemporalScale3_0, TemporalScaleFix, TemporalType, Timestep, TimestepAdded,
    TimestepFoundError, Tot, Trigger, Unicode, UnstainedCenters, UnstainedInfo, Vol, Wavelength,
    Wavelengths, WavelengthsLossError, Wellid,
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
use crate::text::optional::{Identity, MightHave, Nothing};
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
use crate::validated::dataframe::{AnyPrimitiveSeries, HasWidth, PrimitiveDataFrame};
use crate::validated::header_segments::ParsedHeaderSegments;
use crate::validated::keys::{
    DKey0, DKey1, DKey2, IndexedKey as _, Key as _, Key1, NonStdKey, NonStdKeywords,
    NonStdKeywordsExt as _, StdKey, StdKeywords, ValidKeywords,
};
use crate::validated::nonstd_meas_pattern::NonStdMeasRegexError;
use crate::validated::shortname::Shortname;
use crate::validated::textdelim::TEXTDelim;

use fireflow_types::config::{
    CheckedRangeDatatypes, IncludeReqOrOpt, IncludeRootOrMeas, OverRangeAction, TemporalOpticalKey,
};
use fireflow_types::keywords::{
    HasVersion, OpticalFeature, Version, Version2_0, Version3_0, Version3_1, Version3_2,
};
use fireflow_types::nonempty_string::{DisplayableNE as _, NEString};
use type_families::{ApplyOnce as _, BifunctorOnce as _, Functor as _, FunctorOnce as _, Pointed};

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveTime};
use derive_more::{AsMut, AsRef, Display, From};
use derive_new::new;
use hashbrown::HashMap;
use itertools::Itertools as _;
use nonempty_collections::{IntoIteratorExt as _, iter::NonEmptyIterator as _};
use num_traits::identities::{One as _, Zero};
use regex::Regex;
use thiserror::Error;

use std::borrow::Cow;
use std::collections::HashSet;
use std::convert::{AsRef, Infallible};
use std::fmt;
use std::io::{self, BufReader, BufWriter, Read, Seek, Write};
use std::iter::{empty, once};
use std::marker::PhantomData;
use std::mem;
use std::path::PathBuf;

#[cfg(feature = "serde")]
use {
    crate::text::keyword_enum::{AsHeader as _, RefKeyword1},
    crate::text::keywords::{NumType, TextRange, Width},
    nalgebra::DMatrix,
    serde::Serialize,
    std::string::ToString as _,
};

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr, FromInnerPyObject},
    fireflow_types::python as py,
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
/// * `L`: version-specific type for data schema keywords (which may include DATA)
///
/// The types for these parameters and their specific FCS versions are
/// summarized as follows:
///
/// | version | `M`                  | `T`                  | `P`                 | `N`                     | `L` (no DATA)     | `L` (with DATA)      |
/// |---------|----------------------|----------------------|---------------------|-------------------------|-------------------|----------------------|
/// |     2.0 | [`InnerMetaroot2_0`] | [`InnerTemporal2_0`] | [`InnerOptical2_0`] | [`Option<Shortname>`]   | [`DataSchema2_0`] | [`DataFrame2_0`] |
/// |     3.0 | [`InnerMetaroot3_0`] | [`InnerTemporal3_0`] | [`InnerOptical2_0`] | [`Option<Shortname>`]   | [`DataSchema3_0`] | [`DataFrame3_0`] |
/// |     3.1 | [`InnerMetaroot3_1`] | [`InnerTemporal3_1`] | [`InnerOptical2_0`] | [`Identity<Shortname>`] | [`DataSchema3_1`] | [`DataFrame3_1`] |
/// |     3.2 | [`InnerMetaroot3_2`] | [`InnerTemporal3_2`] | [`InnerOptical2_0`] | [`Identity<Shortname>`] | [`DataSchema3_2`] | [`DataFrame3_2`] |
///
/// # Generic parameters for data
///
/// * `A`: the ANALYSIS segment ([`Analysis`])
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
#[derive(Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
// NOTE fields are private since metaroot, measurements, and data schema are all
// related to each other and must be kept in sync
pub struct Core<A, L, O, M, T, P, N, V> {
    /// Metaroot TEXT keywords.
    ///
    /// This includes all keywords that are not part of measurements or the data
    /// schema (ie the "root" of the metadata if thought of as a hierarchy)
    metaroot: Metaroot<M>,

    /// All measurement TEXT keywords.
    ///
    /// Specifically these are denoted by "$Pn*" keywords where "n" is the index
    /// of the measurement which also corresponds to its column in the DATA
    /// segment. The index of each measurement in this vector is n - 1.
    measurements: NamedVec<N, Temporal<T>, Optical<P>>,

    /// The DATA segment
    ///
    /// This is derived from $BYTEORD, $DATATYPE, $PnB, $PnR and maybe
    /// $PnDATATYPE for version 3.2.
    layout: L,

    /// ANALYSIS segment (if applicable)
    analysis: A,

    /// Other segments (if applicable)
    others: O,

    /// Marker for FCS version. Used to lock the types for other fields.
    #[cfg_attr(feature = "serde", serde(skip))]
    _version: PhantomData<V>,
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
pub enum AnyCore<A, L2_0, L3_0, L3_1, L3_2, O> {
    #[from(Core2_0<A, L2_0, O>)]
    FCS2_0(Box<Core2_0<A, L2_0, O>>),
    #[from(Core3_0<A, L3_0, O>)]
    FCS3_0(Box<Core3_0<A, L3_0, O>>),
    #[from(Core3_1<A, L3_1, O>)]
    FCS3_1(Box<Core3_1<A, L3_1, O>>),
    #[from(Core3_2<A, L3_2, O>)]
    FCS3_2(Box<Core3_2<A, L3_2, O>>),
}

pub type AnyCoreTEXT = AnyCore<(), DataSchema2_0, DataSchema3_0, DataSchema3_1, DataSchema3_2, ()>;
pub type AnyCoreDataset =
    AnyCore<Analysis, DataFrame2_0, DataFrame3_0, DataFrame3_1, DataFrame3_2, Others>;

macro_rules! match_anycore {
    ($self:expr, $bind:ident, $stuff:block) => {
        match_many_to_one!($self, Self, [FCS2_0, FCS3_0, FCS3_1, FCS3_2], $bind, $stuff)
    };
}

impl<A, L2_0, L3_0, L3_1, L3_2, O> AnyCore<A, L2_0, L3_0, L3_1, L3_2, O> {
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
    pub fn print_meas_table<W: io::Write>(&self, w: &mut W, delim: u8) -> io::Result<()>
    where
        L2_0: LayoutKeywords + LayoutOptMeasKeywords,
        L3_0: LayoutKeywords + LayoutOptMeasKeywords,
        L3_1: LayoutKeywords + LayoutOptMeasKeywords,
        L3_2: LayoutKeywords + LayoutOptMeasKeywords,
    {
        match_anycore!(self, x, { x.print_meas_table(w, delim) })
    }

    #[cfg(feature = "serde")]
    pub fn print_comp_or_spillover_table<W: io::Write>(
        &self,
        w: &mut W,
        delim: u8,
    ) -> io::Result<()> {
        if let Some((names, matrix)) = self.spillover_or_comp_table() {
            let mut first = true;
            for s in once("[-]").chain(names.iter().map(AsRef::as_ref)) {
                if !first {
                    w.write_all(&[delim])?;
                }
                first = false;
                write!(w, "{s}")?;
            }
            writeln!(w)?;

            for (row, n) in matrix.row_iter().zip(&names[..]) {
                write!(w, "{n}")?;
                for x in row {
                    w.write_all(&[delim])?;
                    write!(w, "{x}")?;
                }
                writeln!(w)?;
            }
        } else {
            writeln!(w, "[]")?;
        }
        Ok(())
    }

    #[cfg(feature = "serde")]
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
        segs: &mut HeaderAndSuppOffsets,
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

        match autodetect_version(version, &kws.std, sconf.version_override.as_ref()) {
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
    pub fn as_data(&self) -> PrimitiveDataFrame {
        match_anycore!(self, x, { x.layout.clone().into() })
    }

    #[must_use]
    pub fn datatypes(&self) -> Vec<AlphaNumType> {
        match_anycore!(self, x, { x.layout.datatypes() })
    }

    #[must_use]
    pub fn write_dataset(
        &self,
        path: &PathBuf,
        conf: &WriteMultiDatasetConfig,
    ) -> WarningsAndIOGroupResult<Nextdata, EventOverRangeError, StdWriterError, WriteDatasetSummary>
    {
        match_many_to_one!(self, Self, [FCS2_0, FCS3_0, FCS3_1, FCS3_2], x, {
            x.write_dataset(path, conf)
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_from_keywords<C, R>(
        h: &mut BufReader<R>,
        hns: &mut HeaderAndSuppOffsets,
        kws: ValidKeywords,
        st: &ReadState<C>,
    ) -> WarningsAndIOGroupResult<
        (Self, StdDatasetFromKwsOutput, Option<KeywordVersionScores>),
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
        let version = hns.header.version;
        macro_rules! go {
            ($t:ident, $s:expr) => {
                $t::new_from_keywords_inner(h, kws, hns, st)
                    .map_ok_value(|(x, y)| (x.into(), y, $s))
                    .map_pure_errors(StdDatasetFromFlatTextError::from)
            };
        }

        let sconf: &ReadHeaderAndTEXTConfig = st.conf.as_ref();

        match autodetect_version(version, &kws.std, sconf.version_override.as_ref()) {
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
    /// since [`ScaleTransform`] needs to be synced with [`Core::data_schema`].
    /// Consequently, the measurement array in `Core` is also private.
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
    /// since [`ScaleTransform`] needs to be synced with [`Core::data_schema`].
    /// Consequently, the measurement array in `Core` is also private.
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
    /// since [`ScaleTransform`] needs to be synced with [`Core::data_schema`].
    /// Consequently, the measurement array in `Core` is also private.
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
    /// since [`ScaleTransform`] needs to be synced with [`Core::data_schema`].
    /// Consequently, the measurement array in `Core` is also private.
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

/// Segment offsets and $TOT as read from TEXT segment
///
/// This is used later to parse DATA and ANALYSIS.
#[derive(new)]
pub struct TEXTOffsets<T> {
    pub(crate) segs: DatasetSegments,
    pub(crate) tot: T,
}

impl<T> TEXTOffsets<T> {
    fn into_common(self) -> TEXTOffsets<Option<Tot>>
    where
        T: MightHave<Tot>,
    {
        TEXTOffsets::new(self.segs, self.tot.to_opt())
    }
}

/// Marker type encoding offset keywords for 2.0
pub struct TEXTOffsets2_0;

/// Marker type encoding offset keywords for 3.0/3.1
pub struct TEXTOffsets3_0;

/// Marker type encoding offset keywords for 3.2
pub struct TEXTOffsets3_2;

pub type MetarootTEXTOffsets<V> =
    TEXTOffsets<<<V as VersionSet>::Offsets as LookupTEXTOffsets>::TotDef>;

/// A scale transform derived from $PnE/$PnG.
#[derive(Clone, Copy, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum ScaleTransform {
    /// A linear transform ($PnE=0,0 and $PnG=1.0 or is null)
    Lin(PositiveFloat),
    /// A log transform ($PnE!=0,0 and $PnG!=1.0 or is null)
    Log(LogScale),
}

impl Default for ScaleTransform {
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

pub type Optical2_0 = Optical<InnerOptical2_0>;
pub type Optical3_0 = Optical<InnerOptical3_0>;
pub type Optical3_1 = Optical<InnerOptical3_1>;
pub type Optical3_2 = Optical<InnerOptical3_2>;

pub type Temporal2_0 = Temporal<InnerTemporal2_0>;
pub type Temporal3_0 = Temporal<InnerTemporal3_0>;
pub type Temporal3_1 = Temporal<InnerTemporal3_1>;
pub type Temporal3_2 = Temporal<InnerTemporal3_2>;

pub type Measurements2_0 = Measurements<Option<Shortname>, InnerTemporal2_0, InnerOptical2_0>;
pub type Measurements3_0 = Measurements<Option<Shortname>, InnerTemporal3_0, InnerOptical3_0>;
pub type Measurements3_1 = Measurements<Identity<Shortname>, InnerTemporal3_1, InnerOptical3_1>;
pub type Measurements3_2 = Measurements<Identity<Shortname>, InnerTemporal3_2, InnerOptical3_2>;

pub type Metaroot2_0 = Metaroot<InnerMetaroot2_0>;
pub type Metaroot3_0 = Metaroot<InnerMetaroot3_0>;
pub type Metaroot3_1 = Metaroot<InnerMetaroot3_1>;
pub type Metaroot3_2 = Metaroot<InnerMetaroot3_2>;

type Timestamps2_0 = Timestamps<FCSTime>;
type Timestamps3_0 = Timestamps<FCSTime60>;
type Timestamps3_1 = Timestamps<FCSTime100>;

/// A standardized TEXT segment
pub type CoreTEXT<M, L, T, P, N, V> = Core<(), L, (), M, T, P, N, V>;

/// A standardized FCS dataset (TEXT+DATA+ANALYSIS+OTHER)
pub type CoreDataset<M, L, T, P, N, V> = Core<Analysis, L, Others, M, T, P, N, V>;

pub type Core2_0<A, L, O> = VersionedCore<A, L, O, Version2_0>;
pub type Core3_0<A, L, O> = VersionedCore<A, L, O, Version3_0>;
pub type Core3_1<A, L, O> = VersionedCore<A, L, O, Version3_1>;
pub type Core3_2<A, L, O> = VersionedCore<A, L, O, Version3_2>;

pub type CoreTEXT2_0 = VersionedCoreTEXT<Version2_0>;
pub type CoreTEXT3_0 = VersionedCoreTEXT<Version3_0>;
pub type CoreTEXT3_1 = VersionedCoreTEXT<Version3_1>;
pub type CoreTEXT3_2 = VersionedCoreTEXT<Version3_2>;

pub type CoreDataset2_0 = VersionedCoreDataset<Version2_0>;
pub type CoreDataset3_0 = VersionedCoreDataset<Version3_0>;
pub type CoreDataset3_1 = VersionedCoreDataset<Version3_1>;
pub type CoreDataset3_2 = VersionedCoreDataset<Version3_2>;

pub(crate) type TemporalOrOptical<V> =
    Element<Temporal<<V as VersionSet>::Temporal>, Optical<<V as VersionSet>::Optical>>;

pub(crate) type TemporalsAndOpticals<V> = Vec<TemporalOrOptical<V>>;

pub(crate) type NamedTemporalOrOptical<V> = EitherPair<
    <V as VersionSet>::Name,
    Temporal<<V as VersionSet>::Temporal>,
    Optical<<V as VersionSet>::Optical>,
>;

pub(crate) type NamedTemporalsAndOpticals<M> = Eithers<
    <M as VersionSet>::Name,
    Temporal<<M as VersionSet>::Temporal>,
    Optical<<M as VersionSet>::Optical>,
>;

pub(crate) type TemporalsAndOpticals2_0 = NamedTemporalsAndOpticals<Version2_0>;
pub(crate) type TemporalsAndOpticals3_0 = NamedTemporalsAndOpticals<Version3_0>;
pub(crate) type TemporalsAndOpticals3_1 = NamedTemporalsAndOpticals<Version3_1>;
pub(crate) type TemporalsAndOpticals3_2 = NamedTemporalsAndOpticals<Version3_2>;

pub(crate) type Measurements<N, T, O> = NamedVec<N, Temporal<T>, Optical<O>>;

pub(crate) type VersionedCore<A, L, O, V> = Core<
    A,
    L,
    O,
    <V as VersionSet>::Metaroot,
    <V as VersionSet>::Temporal,
    <V as VersionSet>::Optical,
    <V as VersionSet>::Name,
    V,
>;

pub(crate) type VersionedCoreTEXT<V> = VersionedCore<(), <V as VersionSet>::DataSchema, (), V>;

pub(crate) type VersionedCoreDataset<V> =
    VersionedCore<Analysis, <V as VersionSet>::DataFrame, Others, V>;

/// Reader for ANALYSIS segment
#[derive(new)]
pub struct AnalysisReader {
    pub seg: AnyAnalysisSegment,
}

impl AnalysisReader {
    pub(crate) fn h_read<R: Read + Seek>(&self, h: &mut BufReader<R>) -> io::Result<Analysis> {
        let mut buf = vec![];
        self.seg.h_read_contents(h, &mut buf)?;
        Ok(buf.into())
    }
}

/// Reader for OTHER segments
#[derive(new)]
pub struct OthersReader {
    pub segs: Vec<OtherSegment20>,
}

impl OthersReader {
    pub(crate) fn h_read<R: Read + Seek>(&self, h: &mut BufReader<R>) -> io::Result<Others> {
        let mut buf = vec![];
        let mut others = vec![];
        for s in &self.segs {
            s.h_read_contents(h, &mut buf)?;
            others.push(Other(buf.clone()));
            buf.clear();
        }
        Ok(Others(others))
    }
}

/// Output of using keywords to crate new standardized TEXT+DATA
#[derive(Clone, new, PartialEq)]
pub struct NewStdDatasetFromKwsOutput {
    /// Standardized data from one FCS dataset
    pub dataset: StdDatasetFromKwsOutput,

    /// (Possibly modified) offsets used to parse HEADER.
    pub header: ParsedHeaderSegments,
}

/// Output when making standardized TEXT+DATA
#[derive(Clone, new, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct StdDatasetFromKwsOutput {
    /// DATA+ANALYSIS
    pub dataset_segments: DatasetSegments,

    /// Keywords that start with '$' that are not part of the standard
    pub std_diagnostics: StdTEXTDiagnostics,

    /// Diagnostic output from parsing DATA segment
    pub events_diagnostics: EventsDiagnostics,
}

/// Standardized TEXT+DATA+ANALYSIS with DATA+ANALYSIS offsets
#[derive(Clone, Copy, PartialEq, new)]
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
pub(crate) struct WriteHeaderAndTextConfig<'a> {
    pub(crate) delim: TEXTDelim,
    pub(crate) tot: Tot,
    pub(crate) data_len: u64,
    pub(crate) analysis_len: u64,
    pub(crate) other_segs: &'a [Other],
    pub(crate) has_nextdata: AppendableFlag,
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

    pub(crate) fn other_lens(&self) -> Vec<u64> {
        self.other_segs
            .iter()
            .map(|s| s.0.len().usize_to_u64())
            .collect()
    }
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
    pub timestep: Option<NEString>,
    /// Original $PnN if they are renamed.
    pub original_names: Vec<Option<Shortname>>,
    /// Diagnostic outcomes from fixing $PnE keys.
    pub scale: Vec<AnyMeasScaleFix>,
    /// Diagnostic outcomes from fixing $GnE keys.
    pub gate_scale: Vec<ScaleFix>,
    /// Original keyword values that were trimmed for whitespace between commas.
    pub trimmed: TrimmedKeywords,
    /// Optical keys that were found in the temporal measurement.
    pub temporal_optical_pairs: Vec<(StdKey, NEString)>,
    /// $TIMESTEP was missing and was added via config
    pub timestep_added: TimestepAdded,
}

pub(crate) type TrimmedKeywords = Vec<(StdKey, NEString)>;

impl StdTEXTDiagnostics {
    fn from_extra(
        extra: ExtraStdKeywords,
        original_names: Vec<Option<Shortname>>,
        gate_scale: Vec<ScaleFix>,
        meas: MeasurementDiagnostics,
    ) -> Self {
        Self {
            pseudostandard: extra.pseudostandard,
            hyper_par: extra.hyper_par,
            hyper_gate: extra.hyper_gate,
            other_version: extra.other_version,
            timestep: extra.timestep,
            original_names,
            scale: meas.scale,
            gate_scale,
            trimmed: meas.trimmed,
            temporal_optical_pairs: meas.tmp_opt_pairs,
            timestep_added: meas.timestep_added,
        }
    }
}

#[derive(new)]
pub struct MeasurementDiagnostics {
    scale: Vec<AnyMeasScaleFix>,
    trimmed: TrimmedKeywords,
    tmp_opt_pairs: Vec<(StdKey, NEString)>,
    timestep_added: TimestepAdded,
}

#[derive(new)]
pub struct DiagnosedMetaroot<M> {
    this: M,
    trimmed: TrimmedKeywords,
    fixed_gate_scales: Vec<ScaleFix>,
}

#[derive(new)]
pub struct DiagnosedOptical<M> {
    this: M,
    scale: OpticalScaleFix,
    trimmed: TrimmedKeywords,
}

#[derive(new)]
pub struct DiagnosedTemporal<M> {
    this: M,
    scale: TemporalScaleFix,
    trimmed: TrimmedKeywords,
    tmp_opt_pairs: Vec<(StdKey, NEString)>,
    timestep_added: TimestepAdded,
}

#[derive(new)]
struct DiagnosedUnstainedData {
    this: UnstainedData,
    trimmed: Option<(StdKey, NEString)>,
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
#[cfg_attr(feature = "python", pyerr(py::ConversionError))]
pub struct NameConversionError(Key1<Shortname>);

/// Error when writing [`CoreDataset`] to file
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum StdWriterError {
    Layout(NewDataSchemaError),
    Check(EventOverRangeError),
    HeaderText(WriteTEXTHeaderError),
}

/// Error when setting measurements vector
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum SetNamedMeasurementsError {
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
    pub SetMeasurementLinkSummary,
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
    Meas(SetNamedMeasurementsError),
    Mismatch(DataSchemaToDataFrameError),
}

/// Error when setting measurements without $PnN and DATA/dataframe simultaneously
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum SetUnnamdMeasurementsAndDataError {
    Meas(SetUnnamedMeasurementsError),
    Mismatch(DataSchemaToDataFrameError),
}

/// Error when setting data schema for a dataset.
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum DatasetSetDataSchemaError {
    DataSchema(MeasLayoutMismatchError),
    Cast(CastSeriesErrors),
}

/// Error when setting named measurements and data schema for a dataset.
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum DatasetSetUnnamedMeasAndDataSchemaError {
    Cast(CastSeriesErrors),
    Meas(SetUnnamedMeasurementsError),
}

/// Error when setting named measurements and data schema for a dataset.
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum DatasetSetNamedMeasAndDataSchemaError {
    DataSchema(DatasetSetDataSchemaError),
    Meas(SetNamedMeasurementsError),
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
#[derive(Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(PyErr: From<E>))]
pub enum PushTemporalError<E> {
    Center(PushCenterError),
    Layout(E),
}

/// Error when inserting a temporal measurement into [`CoreTEXT`]
#[derive(Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(PyErr: From<E>))]
pub enum InsertTemporalError<E> {
    Center(InsertCenterError),
    Layout(E),
}

/// Error when pushing an optical measurement into [`CoreTEXT`]
#[derive(Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(PyErr: From<E>))]
pub enum PushOpticalError<E> {
    Unique(NamePresentError),
    Layout(E),
}

/// Error when inserting an optical measurement into [`CoreTEXT`]
#[derive(Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(PyErr: From<E>))]
pub enum InsertOpticalError<E> {
    Insert(InsertError),
    Layout(E),
}

/// Error when attempting to set temporal $PnE to log (2.0)
#[derive(Debug, Error)]
#[error("tried to set temporal $PnE to nonlinear scale")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct NonLinearTemporalScaleError;

/// Error when attempting to set temporal $PnE/$PnG to non-unitary transform (3.0+)
#[derive(Debug, Error)]
#[error("tried to set temporal $PnE/$PnG to nonlinear transform")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
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
    DataSchema(LookupDataSchemaError),
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
    DataSchema(LookupDataSchemaWarning),
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
    Dataframe(ReadCheckedDataframeError),
    Offsets(LookupTEXTOffsetsError),
    Warn(StdDatasetFromFlatTEXTWarning),
}

/// Warning when reading standardized DATA from keyword pairs
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum StdDatasetFromFlatTEXTWarning {
    TEXT(StdTEXTFromFlatTEXTWarning),
    Offsets(LookupTEXTOffsetsWarning),
    Layout(ReadCheckedDataframeWarning),
}

/// Error when $PnE is not set on optical measurement and target version requires it
#[derive(Debug, Error)]
#[error("{} must be set before converting measurement", Scale::std(self.0))]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConversionError))]
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
    Cytsn(Key0LossError<Cytsn>),
    Unicode(Key0LossError<Unicode>),
    Vol(Key0LossError<Vol>),
    Flowrate(Key0LossError<Flowrate>),
    Comp2_0(Key2LossError<Dfc>),
    Comp3_0(Key0LossError<Compensation3_0>),
    Spillover(Key0LossError<Spillover>),
    Begin(Key0LossError<BeginDateTime>),
    End(Key0LossError<EndDateTime>),
    Bits(Key0LossError<CSVBits>),
    Tot(Key0LossError<CSTot>),
    CSMode(Key0LossError<CSMode>),
    CSVFlag(Key1LossError<CSVFlag>),
    Carrierid(Key0LossError<Carrierid>),
    Locationid(Key0LossError<Locationid>),
    Carriertype(Key0LossError<Carriertype>),
    Platename(Key0LossError<Platename>),
    Plateid(Key0LossError<Plateid>),
    Wellid(Key0LossError<Wellid>),
    LastModifier(Key0LossError<LastModifier>),
    LastModified(Key0LossError<LastModified>),
    Originality(Key0LossError<Originality>),
    UnstainedCenters(Key0LossError<UnstainedCenters>),
    UnstainedInfo(Key0LossError<UnstainedInfo>),
    Gate(Key0LossError<Gate>),
    GateScale(Key1LossError<GateScale>),
    GateFilter(Key1LossError<GateFilter>),
    GateShortname(Key1LossError<GateShortname>),
    GatePEmit(Key1LossError<GatePercentEmitted>),
    GateRange(Key1LossError<GateRange>),
    GateLongname(Key1LossError<GateLongname>),
    GateDetType(Key1LossError<GateDetectorType>),
    GateDetVolt(Key1LossError<GateDetectorVoltage>),
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
    MeasType(Key1LossError<OpticalType>),
    Analyte(Key1LossError<Analyte>),
    Tag(Key1LossError<Tag>),
    Gain(GainLossError),
    Display(Key1LossError<Display>),
    DetectorName(Key1LossError<DetectorName>),
    Feature(Key1LossError<Feature>),
    Calibration3_1(Key1LossError<Calibration3_1>),
    Calibration3_2(Key1LossError<Calibration3_2>),
    Peak(PeakLossError),
}

/// Error when the $PnG does not exist in target version and is not 1.0.
#[derive(Debug, Error)]
#[error(
    "$P{0}G does not exist in target version and is currently not 1.0 \
     which means data will be lost on dropping"
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConversionError))]
pub struct GainLossError(MeasIndex);

/// Error when a temporal keyword will be lost when converting versions
#[derive(From, Display, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum AnyTemporalKeyLossError {
    TempType(Key1LossError<TemporalType>),
    Display(Key1LossError<Display>),
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
    Filter(Key1LossError<Filter>),
    Power(Key1LossError<Power>),
    DetectorType(Key1LossError<DetectorType>),
    PercentEmitted(Key1LossError<PercentEmitted>),
    DetectorVoltage(Key1LossError<DetectorVoltage>),
    Wavelength(Key1LossError<Wavelength>),
    Wavelengths(Key1LossError<Wavelengths>),
    MeasType(Key1LossError<OpticalType>),
    Analyte(Key1LossError<Analyte>),
    Tag(Key1LossError<Tag>),
    Scale(NonLinearScaleError),
    Gain(NonUnitGainError),
    DetectorName(Key1LossError<DetectorName>),
    Feature(Key1LossError<Feature>),
    Calibration3_1(Key1LossError<Calibration3_1>),
    Calibration3_2(Key1LossError<Calibration3_2>),
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
    TempType(Key1LossError<TemporalType>),
}

/// Error when $PKn and $PKNn keywords would be lost due to version change
#[derive(From, Display, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum PeakLossError {
    Bin(Key1LossError<PeakBin>),
    Number(Key1LossError<PeakIndex>),
}

/// Error when reading DATA segment from already-parsed keywords
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupAndReadDataAnalysisError {
    DatasetOffset(DatasetOffsetError),
    Par(ReqKeyError<Par>),
    Offsets(LookupTEXTOffsetsError),
    DataSchema(LookupDataSchemaError),
    Dataframe(ReadCheckedDataframeError),
    Warn(LookupAndReadDataAnalysisWarning),
}

/// Warning when reading DATA segment from already-parsed keywords
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupAndReadDataAnalysisWarning {
    Offsets(LookupTEXTOffsetsWarning),
    DataSchema(LookupDataSchemaWarning),
    Data(ReadCheckedDataframeWarning),
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
    MismatchData(SegmentMismatchError<DataSegmentId>),
    /// required TEXT ANALYSIS segment does not match HEADER (3.0/3.1)
    MismatchAnalysis(SegmentMismatchError<AnalysisSegmentId>),
    /// optional TEXT ANALYSIS segment does not match HEADER (3.2)
    MismatchAnalysisOpt(OptSegmentWithDefaultWarning<AnalysisSegmentId>),
    /// DATA and ANALYSIS offsets are both non-empty and overlap each other
    DataAnalysisOverlap(SegmentOverlapError),
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
#[cfg_attr(feature = "python", pyerr(py::ConfigError))]
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
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct MissingTime(pub Regex);

type LookupTEXTOffsetsResult<T> =
    WarningsAndErrorsResult<T, (), LookupTEXTOffsetsWarning, LookupTEXTOffsetsError>;

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
pub struct ScaleTransformError {
    scale: Scale,
    gain: Gain,
}

/// Error when $COMP does not have the same number of rows/columns as $PAR
#[derive(Debug, Error)]
#[error("$COMP must have same row/column number as $PAR ({par}), got {comp}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct CompParMismatchError {
    par: usize,
    comp: usize,
}

type SetSpilloverErrors = ErrorGroup<KeyToNameLinkError<Spillover>, SetSpilloverSummary>;

def_summary!(pub SetSpilloverSummary, "error when setting $SPILLOVER");

type SetUnstainedCentersErrors =
    ErrorGroup<KeyToNameLinkError<UnstainedCenters>, SetUnstainedCentersSummary>;

def_summary!(
    pub SetUnstainedCentersSummary,
    "error when setting $UNSTAINEDCENTERS"
);

type SetOpticalError = SetElementsError<ErrorGroup<MeasMismatchError, SetOpticalSummary>>;

def_summary!(
    pub SetOpticalSummary,
    "attempted to assign incompatible optical measurement values"
);

type SetAllMeasError = SetElementsError<ErrorGroup<MeasMismatchError, SetAllMeasSummary>>;

def_summary!(
    pub SetAllMeasSummary,
    "attempted to assign incompatible optical and temporal measurement values"
);

/// Error when temporal type is assigned to optical measurement and vice versa.
#[derive(Debug, Error, new)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct MeasMismatchError {
    key_is_optical: bool,
    index: MeasIndex,
}

// TODO this error is confusing for any temporal type which is not unit
impl fmt::Display for MeasMismatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let k = self.index;
        if self.key_is_optical {
            write!(f, "optical index {k} must not be assigned temporal type")
        } else {
            write!(f, "temporal index {k} must be assigned empty tuple")
        }
    }
}

#[cfg(feature = "python")]
def_summary!(pub NewCoreTEXTSummary, "could not make new CoreTEXT");

#[cfg(feature = "python")]
def_summary!(pub NewCoreDatasetSummary, "could not make new CoreDataset");

#[derive(Display, new)]
#[display("could not convert version from {from} to {to}")]
pub struct ConvertSummary {
    from: Version,
    to: Version,
}

def_summary!(
    pub SetScalesSummary,
    "could not set scales for optical measurements"
);

def_summary!(
    pub SetTransformsSummary,
    "could not set scale transforms for optical measurements"
);

def_summary!(pub PushTemporalSummary, "could not push temporal measurement");

def_summary!(
    pub InsertTemporalSummary,
    "could not insert temporal measurement"
);

def_summary!(pub PushOpticalSummary, "could not push optical measurement");

def_summary!(pub InsertOpticalSummary, "could not insert optical measurement");

def_summary!(pub SetAppliedGatesSummary, "could not set gating keywords");

def_summary!(pub WriteDatasetSummary, "could not write FCS file");

def_summary!(
    pub CoreTEXTFromKeywordsSummary,
    "could not create new CoreTEXT from keywords"
);

def_summary!(
    pub StdDatasetWithKwsSummary,
    "could not read standardized dataset from keywords"
);

// Implement references to inner types.
//
// This will be the primary way for the API to access keywords values since
// the AsRef trait provides a clean an elegant way to access internals without
// rewriting a method for every keyword.
//
// Note that mutable references are never used for types that must be internally
// validated for consistency with other values.

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

// Implement private mutable access for compensation matrix

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

// Implement private mutable access for spillover matrix

pub trait HasSpillover {
    // private as_mut
    fn spill_mut(&mut self, _: private::NoTouchy) -> &mut Option<Spillover>;
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

// Implement private mutable access for $PnE (2.0)

pub trait HasScale {
    // private as_mut
    fn scale_mut(&mut self, _: private::NoTouchy) -> &mut Option<Scale>;
}

impl HasScale for InnerOptical2_0 {
    fn scale_mut(&mut self, _: private::NoTouchy) -> &mut Option<Scale> {
        &mut self.scale
    }
}

// Implement private mutable access for $PnE/$PnG (3.0+)

pub trait HasScaleTransform {
    fn transform_mut(&mut self, _: private::NoTouchy) -> &mut ScaleTransform;
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

// Implement private mutable access for $UNSTAINEDCENTERS (3.2)

pub trait HasUnstainedCenters {
    // private as_mut
    fn unstainedcenters_mut(&mut self, _: private::NoTouchy) -> &mut UnstainedCenters;
}

impl HasUnstainedCenters for InnerMetaroot3_2 {
    fn unstainedcenters_mut(&mut self, _: private::NoTouchy) -> &mut UnstainedCenters {
        &mut self.unstained.unstainedcenters
    }
}

// Implement private mutable access for gating keywords (3.0/3.1)

pub trait HasAppliedGates {
    type Gates;
    // private as_mut
    fn applied_gates_mut(&mut self, _: private::NoTouchy) -> &mut Self::Gates;
}

impl HasAppliedGates for InnerMetaroot3_0 {
    type Gates = AppliedGates3_0;
    fn applied_gates_mut(&mut self, _: private::NoTouchy) -> &mut Self::Gates {
        &mut self.applied_gates
    }
}

impl HasAppliedGates for InnerMetaroot3_1 {
    type Gates = AppliedGates3_0;
    fn applied_gates_mut(&mut self, _: private::NoTouchy) -> &mut Self::Gates {
        &mut self.applied_gates
    }
}

impl HasAppliedGates for InnerMetaroot3_2 {
    type Gates = AppliedGates3_2;
    fn applied_gates_mut(&mut self, _: private::NoTouchy) -> &mut Self::Gates {
        &mut self.applied_gates
    }
}

// Implement mapping from metadata type to generalized scale transform
//
// Used for checking transforms against the datatype for a measurement.

pub trait AsScaleOrTransform {
    type S: Default;
    fn as_scale_or_transform(&self) -> Self::S;
}

impl AsScaleOrTransform for InnerOptical2_0 {
    type S = Scale;
    fn as_scale_or_transform(&self) -> Self::S {
        self.scale.unwrap_or(Scale::Linear)
    }
}

impl AsScaleOrTransform for InnerOptical3_0 {
    type S = ScaleTransform;
    fn as_scale_or_transform(&self) -> Self::S {
        self.scale
    }
}

impl AsScaleOrTransform for InnerOptical3_1 {
    type S = ScaleTransform;
    fn as_scale_or_transform(&self) -> Self::S {
        self.scale
    }
}

impl AsScaleOrTransform for InnerOptical3_2 {
    type S = ScaleTransform;
    fn as_scale_or_transform(&self) -> Self::S {
        self.scale
    }
}

// Implement version mapping for metadata types

pub trait Versioned {
    type Ver: HasVersion;
}

macro_rules! impl_versioned {
    ($t:path, $v:ident) => {
        impl Versioned for $t {
            type Ver = $v;
        }
    };
}

impl_versioned!(InnerMetaroot2_0, Version2_0);
impl_versioned!(InnerMetaroot3_0, Version3_0);
impl_versioned!(InnerMetaroot3_1, Version3_1);
impl_versioned!(InnerMetaroot3_2, Version3_2);
impl_versioned!(InnerOptical2_0, Version2_0);
impl_versioned!(InnerOptical3_0, Version3_0);
impl_versioned!(InnerOptical3_1, Version3_1);
impl_versioned!(InnerOptical3_2, Version3_2);
impl_versioned!(InnerTemporal2_0, Version2_0);
impl_versioned!(InnerTemporal3_0, Version3_0);
impl_versioned!(InnerTemporal3_1, Version3_1);
impl_versioned!(InnerTemporal3_2, Version3_2);

// Implement mapping between FCS version and all metadata types

pub trait VersionSet: HasVersion {
    type Metaroot: VersionedMetaroot;
    type Optical: VersionedOptical;
    type Temporal: VersionedTemporal;
    type Name: MightHave<Shortname>;
    type DataSchema: VersionedDataSchema;
    type DataFrame: VersionedDataFrame;
    type Offsets: LookupTEXTOffsets<TotDef = <Self::DataSchema as VersionedDataSchema>::Tot>;
}

macro_rules! impl_version_set {
    ($v:ident, $m:path, $opt:path, $t:path, $n:path, $l:path, $d:path, $ofs:path) => {
        impl VersionSet for $v {
            type Metaroot = $m;
            type Optical = $opt;
            type Temporal = $t;
            type Name = $n;
            type DataSchema = $l;
            type DataFrame = $d;
            type Offsets = $ofs;
        }
    };
}

impl_version_set!(
    Version2_0,
    InnerMetaroot2_0,
    InnerOptical2_0,
    InnerTemporal2_0,
    Option<Shortname>,
    DataSchema2_0,
    DataFrame2_0,
    TEXTOffsets2_0
);

impl_version_set!(
    Version3_0,
    InnerMetaroot3_0,
    InnerOptical3_0,
    InnerTemporal3_0,
    Option<Shortname>,
    DataSchema3_0,
    DataFrame3_0,
    TEXTOffsets3_0
);

impl_version_set!(
    Version3_1,
    InnerMetaroot3_1,
    InnerOptical3_1,
    InnerTemporal3_1,
    Identity<Shortname>,
    DataSchema3_1,
    DataFrame3_1,
    TEXTOffsets3_0
);

impl_version_set!(
    Version3_2,
    InnerMetaroot3_2,
    InnerOptical3_2,
    InnerTemporal3_2,
    Identity<Shortname>,
    DataSchema3_2,
    DataFrame3_2,
    TEXTOffsets3_2
);

// Implement misc methods for a given version
//
// Used to keep messy functions out of public API

pub(crate) trait PrivVersionSet: VersionSet {
    fn h_lookup_and_read<C, R>(
        h: &mut BufReader<R>,
        kws: &StdKeywords,
        hns: &mut HeaderAndSuppOffsets,
        st: &ReadState<C>,
    ) -> WarningsAndIOGroupResult<
        (
            PrimitiveDataFrame,
            Analysis,
            DatasetSegments,
            EventsDiagnostics,
        ),
        LookupAndReadDataAnalysisWarning,
        LookupAndReadDataAnalysisError,
        (),
    >
    where
        <Self::DataSchema as DataSchemaToEmptyDataFrame>::DfTarget:
            Into<PrimitiveDataFrame> + DataFrameCheckRanges,
        R: Read + Seek,
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadEventsConfig> + AsRef<ReadOffsetConfig>,
    {
        let layout_res = Par::get_metaroot_req(kws)
            .map_err(LookupAndReadDataAnalysisError::from)
            .into_log()
            .and_then_commutative(|par| {
                Self::DataSchema::lookup_ro(kws, par, st.conf.as_ref())
                    .map_commutative_warnings(LookupAndReadDataAnalysisWarning::from)
                    .map_errors(LookupAndReadDataAnalysisError::from)
            });
        let offset_res = Self::Offsets::lookup_ro(kws, hns, st)
            .map_commutative_warnings(LookupAndReadDataAnalysisWarning::from)
            .map_errors(LookupAndReadDataAnalysisError::from);
        layout_res
            .zip_commutative(offset_res)
            .group()
            .map_error(IOErrorGroup::Pure)
            .and_then_commutative(|(mut layout_out, mut offsets)| {
                let ar = AnalysisReader::new(offsets.segs.analysis);
                layout_out
                    .data_schema
                    .h_read_df(h, offsets.tot, &mut offsets.segs.data, st.conf.as_ref())
                    .map_commutative_warnings(LookupAndReadDataAnalysisWarning::from)
                    .map_pure_errors(LookupAndReadDataAnalysisError::from)
                    .and_then_commutative(|df_out| {
                        ar.h_read(h)
                            .map(|a| (df_out.dataframe.into(), a, offsets.segs, df_out.diagnostics))
                            .map_err(IOErrorGroup::from)
                            .into_log()
                    })
            })
    }
}

impl PrivVersionSet for Version2_0 {}
impl PrivVersionSet for Version3_0 {}
impl PrivVersionSet for Version3_1 {}
impl PrivVersionSet for Version3_2 {}

// Implement method to look up $PnN from a hash table

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

// Implement method to look up root keywords from a hash table

pub trait LookupMetaroot<N>: Sized {
    fn lookup_specific<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        ms: &[N],
        conf: &C,
    ) -> LookupMetarootResult<DiagnosedMetaroot<Self>>
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadStdKeywordsConfig>;
}

impl LookupMetaroot<Option<Shortname>> for InnerMetaroot2_0 {
    fn lookup_specific<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        ms: &[Option<Shortname>],
        conf: &C,
    ) -> LookupMetarootResult<DiagnosedMetaroot<Self>>
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
            .map_ok_value(|((c, t, g), m)| {
                DiagnosedMetaroot::new(Self::new(m, cyt, c, t, g.0), g.1, g.2)
            })
    }
}

impl LookupMetaroot<Option<Shortname>> for InnerMetaroot3_0 {
    fn lookup_specific<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        _: &[Option<Shortname>],
        conf: &C,
    ) -> LookupMetarootResult<DiagnosedMetaroot<Self>>
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
                let ret = Self::new(m, cyt, co, t, cytsn, u, su, g.0);
                let trimmed = c_trimmed.into_iter().chain(u_trimmed).chain(g.1).collect();
                DiagnosedMetaroot::new(ret, trimmed, g.2)
            })
    }
}

impl LookupMetaroot<Identity<Shortname>> for InnerMetaroot3_1 {
    fn lookup_specific<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        ms: &[Identity<Shortname>],
        conf: &C,
    ) -> LookupMetarootResult<DiagnosedMetaroot<Self>>
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadStdKeywordsConfig>,
    {
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
            .into_log();

        spill
            .zip6_commutative(subset, modif, ts, vol, ag)
            .map_errors(LookupMetarootError::from)
            .zip_commutative(mode)
            .map_ok_value(|((sp_out, su, md, t, v, g), m)| {
                let (sp, sp_trimmed) = sp_out.into_opt_root_pair();
                let ret = Self::new(m, cyt, t, cytsn, sp, md, plate, v, su, g.0);
                let trimmed = sp_trimmed.into_iter().chain(g.1).collect();
                DiagnosedMetaroot::new(ret, trimmed, g.2)
            })
    }
}

impl LookupMetaroot<Identity<Shortname>> for InnerMetaroot3_2 {
    fn lookup_specific<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        ms: &[Identity<Shortname>],
        conf: &C,
    ) -> LookupMetarootResult<DiagnosedMetaroot<Self>>
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
                    mo, t, d, c, sp, cytsn, md, plate, v, carrier, u_out.this, flow, ag.0,
                );
                let trimmed = sp_trimmed
                    .into_iter()
                    .chain(u_out.trimmed)
                    .chain(ag.1)
                    .collect();
                DiagnosedMetaroot::new(ret, trimmed, vec![])
            })
    }
}

// Implement method to look up optical keywords from a hash table

pub trait LookupOptical: Sized + VersionedOptical {
    fn lookup_specific<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        i: MeasIndex,
        dt: AlphaNumType,
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
        dt: AlphaNumType,
        conf: &C,
    ) -> LookupOpticalResult<DiagnosedOptical<Self>>
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadStdKeywordsConfig>,
    {
        let scale = Scale::remove_or_drop_meas_opt_with(std, nonstd, i, dt, conf)
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
        dt: AlphaNumType,
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
        let scale = ScaleTransform::lookup(std, nonstd, i, dt, conf);
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
        dt: AlphaNumType,
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
        let scale = ScaleTransform::lookup(std, nonstd, i, dt, conf);
        go!(wave)
            .zip4_commutative(go!(cal), go!(dpy), peak)
            .map_errors(LookupOpticalError::from)
            .zip_commutative(scale)
            .map_ok_value(|((w_out, c_out, d_out, p), (s, sd))| {
                let (w, w_trimmed) = w_out.into_indexed_pair(i);
                let (c, c_trimmed) = c_out.into_opt_indexed_pair(i.into());
                let (d, d_trimmed) = d_out.into_opt_indexed_pair(i.into());
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
        dt: AlphaNumType,
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

        let scale = ScaleTransform::lookup(std, nonstd, i, dt, conf);

        go!(wave)
            .zip6_commutative(go!(cal), go!(dpy), go!(meas), go!(feat), scale)
            .map_ok_value(|(w_out, c_out, d_out, m, f, (s, sd))| {
                let (w, w_trimmed) = w_out.into_indexed_pair(i);
                let (c, c_trimmed) = c_out.into_opt_indexed_pair(i.into());
                let (d, d_trimmed) = d_out.into_opt_indexed_pair(i.into());
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

// Implement method to look up temporal keywords from a hash table

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
                let this = Self::new(s.native, p);
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

// Implement common methods to lookup offset keywords from hash table

pub trait LookupTEXTOffsets: Sized {
    type TotDef: IsTot;

    fn lookup<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        segs: &mut HeaderAndSuppOffsets,
        st: &ReadState<C>,
    ) -> LookupTEXTOffsetsResult<TEXTOffsets<Self::TotDef>>
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadOffsetConfig>;

    fn lookup_ro<C>(
        std: &StdKeywords,
        segs: &mut HeaderAndSuppOffsets,
        st: &ReadState<C>,
    ) -> LookupTEXTOffsetsResult<TEXTOffsets<Self::TotDef>>
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadOffsetConfig>;
}

impl LookupTEXTOffsets for TEXTOffsets2_0 {
    type TotDef = Option<Tot>;

    fn lookup<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        segs: &mut HeaderAndSuppOffsets,
        st: &ReadState<C>,
    ) -> LookupTEXTOffsetsResult<TEXTOffsets<Self::TotDef>>
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadOffsetConfig>,
    {
        Tot::remove_or_drop_root_opt(std, nonstd, st.conf.as_ref())
            .map_ok_value(|tot| {
                let s = segs.header.segments.as_dataset_segments(None, None);
                TEXTOffsets::new(s, tot)
            })
            .set_err_value(())
            .switchable_into_commutative()
            .map_commutative_warnings(LookupTEXTOffsetsWarning::from)
            .map_errors(LookupTEXTOffsetsError::from)
            .into_semigroup()
    }

    fn lookup_ro<C>(
        std: &StdKeywords,
        segs: &mut HeaderAndSuppOffsets,
        _: &ReadState<C>,
    ) -> LookupTEXTOffsetsResult<TEXTOffsets<Self::TotDef>>
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadOffsetConfig>,
    {
        let succ = Tot::get_root_opt(std)
            .map_err(LookupTEXTOffsetsWarning::from)
            .into_succ()
            .fmap_once(|tot| {
                let s = segs.header.segments.as_dataset_segments(None, None);
                TEXTOffsets::new(s, tot)
            });
        LogResult::Succ(succ)
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
            .and_then_commutative(|(tot, (d, dr), (a, ar))| {
                let oconf: &ReadOffsetConfig = $st.conf.as_ref();
                let limit = oconf.overlap_correction_limit;
                DatasetSegments::try_new(d, a, dr, ar, limit)
                    .map(|dos| TEXTOffsets::new(dos, Identity(tot)))
                    .map_err(LookupTEXTOffsetsError::from)
                    .into_log()
            })
    }};
}

impl LookupTEXTOffsets for TEXTOffsets3_0 {
    type TotDef = Identity<Tot>;

    fn lookup<C>(
        std: &mut StdKeywords,
        _: &mut NonStdKeywords,
        segs: &mut HeaderAndSuppOffsets,
        st: &ReadState<C>,
    ) -> LookupTEXTOffsetsResult<TEXTOffsets<Self::TotDef>>
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadOffsetConfig>,
    {
        lookup_offsets_3_0!(std, segs, st, remove_metaroot_req, remove_req_or)
    }

    fn lookup_ro<C>(
        std: &StdKeywords,
        segs: &mut HeaderAndSuppOffsets,
        st: &ReadState<C>,
    ) -> LookupTEXTOffsetsResult<TEXTOffsets<Self::TotDef>>
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadOffsetConfig>,
    {
        lookup_offsets_3_0!(std, segs, st, get_metaroot_req, get_req_or)
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
            .and_then_commutative(|(tot, (d, dr), (a, ar))| {
                let oconf: &ReadOffsetConfig = $st.conf.as_ref();
                let limit = oconf.overlap_correction_limit;
                DatasetSegments::try_new(d, a, dr, ar, limit)
                    .map(|dos| TEXTOffsets::new(dos, Identity(tot)))
                    .map_err(LookupTEXTOffsetsError::from)
                    .into_log()
            })
    }};
}

impl LookupTEXTOffsets for TEXTOffsets3_2 {
    type TotDef = Identity<Tot>;

    fn lookup<C>(
        std: &mut StdKeywords,
        _: &mut NonStdKeywords,
        segs: &mut HeaderAndSuppOffsets,
        st: &ReadState<C>,
    ) -> LookupTEXTOffsetsResult<TEXTOffsets<Self::TotDef>>
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
        segs: &mut HeaderAndSuppOffsets,
        st: &ReadState<C>,
    ) -> LookupTEXTOffsetsResult<TEXTOffsets<Self::TotDef>>
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadOffsetConfig>,
    {
        lookup_offsets_3_2!(std, segs, st, get_metaroot_req, get_req_or, get_opt_or)
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

// Implement method to convert root keyword values between versions

pub trait ConvertFromMetaroot<M: VersionedMetaroot>: Sized + VersionedMetaroot {
    fn convert_from_metaroot_inner(value: M, flag: AllowLoss) -> MetarootConvertResult<Self>;

    fn convert_from_metaroot(value: M, flag: AllowLoss) -> MetarootConvertResult<Self> {
        let current_version = M::Ver::as_version();
        let target_version = Self::Ver::as_version();
        let es: Vec<_> = value
            .keywords_opt_inner()
            .filter(|x| !x.contains_version(target_version))
            .filter_map(|k| k.as_loss_error(current_version, target_version))
            .collect();
        let res = Self::convert_from_metaroot_inner(value, flag);
        res.extend_warnings_or_errors3(
            es,
            |_| (),
            MetarootConvertWarning::from,
            MetarootConvertError::from,
            flag,
        )
    }
}

impl ConvertFromMetaroot<InnerMetaroot3_0> for InnerMetaroot2_0 {
    fn convert_from_metaroot_inner(
        value: InnerMetaroot3_0,
        flag: AllowLoss,
    ) -> MetarootConvertResult<Self> {
        value
            .applied_gates
            .try_into_2_0(flag)
            .switchable_into_commutative()
            .map_commutative_warnings(MetarootConvertWarning::from)
            .map_errors(MetarootConvertError::from)
            .set_err_value(())
            .map_ok_value(|ag| {
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
    fn convert_from_metaroot_inner(
        value: InnerMetaroot3_1,
        flag: AllowLoss,
    ) -> MetarootConvertResult<Self> {
        let ts = value.timestamps.map(Into::into);
        value
            .applied_gates
            .try_into_2_0(flag)
            .switchable_into_commutative()
            .map_commutative_warnings(MetarootConvertWarning::from)
            .map_errors(MetarootConvertError::from)
            .set_err_value(())
            .map_ok_value(|ag| Self::new(value.mode, value.cyt, None, ts, ag))
    }
}

impl ConvertFromMetaroot<InnerMetaroot3_2> for InnerMetaroot2_0 {
    fn convert_from_metaroot_inner(
        value: InnerMetaroot3_2,
        _: AllowLoss,
    ) -> MetarootConvertResult<Self> {
        LogResult::new_ok(Self::new(
            Mode::List,
            value.cyt,
            None,
            value.timestamps.map(Into::into),
            AppliedGates2_0::default(),
        ))
    }
}

impl ConvertFromMetaroot<InnerMetaroot2_0> for InnerMetaroot3_0 {
    fn convert_from_metaroot_inner(
        value: InnerMetaroot2_0,
        _: AllowLoss,
    ) -> MetarootConvertResult<Self> {
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
    fn convert_from_metaroot_inner(
        value: InnerMetaroot3_1,
        _: AllowLoss,
    ) -> MetarootConvertResult<Self> {
        LogResult::new_ok(Self::new(
            value.mode,
            value.cyt,
            None,
            value.timestamps.map(Into::into),
            value.cytsn,
            None,
            SubsetData::default(),
            value.applied_gates,
        ))
    }
}

impl ConvertFromMetaroot<InnerMetaroot3_2> for InnerMetaroot3_0 {
    fn convert_from_metaroot_inner(
        value: InnerMetaroot3_2,
        _: AllowLoss,
    ) -> MetarootConvertResult<Self> {
        LogResult::new_ok(Self::new(
            Mode::List,
            value.cyt,
            None,
            value.timestamps.map(Into::into),
            value.cytsn,
            None,
            SubsetData::default(),
            value.applied_gates,
        ))
    }
}

impl ConvertFromMetaroot<InnerMetaroot2_0> for InnerMetaroot3_1 {
    fn convert_from_metaroot_inner(
        value: InnerMetaroot2_0,
        _: AllowLoss,
    ) -> MetarootConvertResult<Self> {
        LogResult::new_ok(Self::new(
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
        ))
    }
}

impl ConvertFromMetaroot<InnerMetaroot3_0> for InnerMetaroot3_1 {
    fn convert_from_metaroot_inner(
        value: InnerMetaroot3_0,
        _: AllowLoss,
    ) -> MetarootConvertResult<Self> {
        LogResult::new_ok(Self::new(
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
        ))
    }
}

impl ConvertFromMetaroot<InnerMetaroot3_2> for InnerMetaroot3_1 {
    fn convert_from_metaroot_inner(
        value: InnerMetaroot3_2,
        _: AllowLoss,
    ) -> MetarootConvertResult<Self> {
        LogResult::new_ok(Self::new(
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
        ))
    }
}

impl ConvertFromMetaroot<InnerMetaroot2_0> for InnerMetaroot3_2 {
    fn convert_from_metaroot_inner(
        value: InnerMetaroot2_0,
        flag: AllowLoss,
    ) -> MetarootConvertResult<Self> {
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

        mode_res
            .zip_commutative(cyt_res)
            .map_ok_value(|(mode, cyt)| {
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
    fn convert_from_metaroot_inner(
        value: InnerMetaroot3_0,
        flag: AllowLoss,
    ) -> MetarootConvertResult<Self> {
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

        mode_res
            .zip3_commutative(ag_res, cyt_res)
            .map_ok_value(|(mode, applied_gates, cyt)| {
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
    fn convert_from_metaroot_inner(
        value: InnerMetaroot3_1,
        flag: AllowLoss,
    ) -> MetarootConvertResult<Self> {
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

        ag_res
            .zip3_commutative(mode_rs, cyt_res)
            .map_ok_value(|(applied_gates, mode, cyt)| {
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

// Implement method to convert optical keyword values between versions

pub trait ConvertFromOptical<O: VersionedOptical>: Sized + VersionedOptical {
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
    fn convert_from_optical_inner(
        value: InnerOptical3_1,
        i: MeasIndex,
        flag: AllowLoss,
    ) -> OpticalConvertResult<Self> {
        let scale = ScaleTransform::try_convert_to_scale(value.scale, i)
            .map_errors(AnyOpticalKeyLossError::from)
            .map_errors(OpticalConvertWarning::from)
            .into_semigroup();
        let wave = value
            .wavelengths
            .into_wavelength(i)
            .map_errors(OpticalConvertWarning::from)
            .into_semigroup();
        scale
            .lift_f2_once(wave, |s, w| Self::new(s, w, value.peak))
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
        let scale = ScaleTransform::try_convert_to_scale(value.scale, i)
            .map_errors(AnyOpticalKeyLossError::from)
            .map_errors(OpticalConvertWarning::from)
            .into_semigroup();
        let wave = value
            .wavelengths
            .into_wavelength(i)
            .map_errors(OpticalConvertWarning::from)
            .into_semigroup();
        scale
            .lift_f2_once(wave, |s, w| Self::new(s, w, PeakData::default()))
            .nowarn_into_switchable3(flag)
            .switchable_into_commutative()
            .map_errors(OpticalConvertError::from)
            .set_err_value(())
    }
}

impl ConvertFromOptical<InnerOptical2_0> for InnerOptical3_0 {
    fn convert_from_optical_inner(
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
            .map_ok_value(|w| Self::new(value.scale, w, value.peak))
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
            .map_ok_value(|w| Self::new(value.scale, w, PeakData::default()))
    }
}

impl ConvertFromOptical<InnerOptical2_0> for InnerOptical3_1 {
    fn convert_from_optical_inner(
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
    fn convert_from_optical_inner(
        value: InnerOptical3_0,
        _: MeasIndex,
        _: AllowLoss,
    ) -> OpticalConvertResult<Self> {
        let wave = value.wavelength.map(Wavelengths::from).unwrap_or_default();
        LogResult::new_ok(Self::new(value.scale, wave, None, None, value.peak))
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
    fn convert_from_optical_inner(
        value: InnerOptical2_0,
        i: MeasIndex,
        _: AllowLoss,
    ) -> OpticalConvertResult<Self> {
        let wave = value.wavelength.map(Wavelengths::from).unwrap_or_default();
        value
            .scale
            .ok_or(NoScaleError(i).into())
            .into_log()
            .map_ok_value(|s| {
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
    fn convert_from_optical_inner(
        value: InnerOptical3_0,
        _: MeasIndex,
        _: AllowLoss,
    ) -> OpticalConvertResult<Self> {
        let wave = value.wavelength.map(Wavelengths::from).unwrap_or_default();
        LogResult::new_ok(Self::new(
            value.scale,
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
            value.scale,
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

pub trait ConvertFromTemporal<T: VersionedTemporal>: Sized + VersionedTemporal {
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
        Self::new(true, value.peak)
    }
}

impl ConvertFromTemporal<InnerTemporal3_1> for InnerTemporal2_0 {
    fn convert_from_temporal_inner(value: InnerTemporal3_1) -> Self {
        Self::new(true, value.peak)
    }
}

impl ConvertFromTemporal<InnerTemporal3_2> for InnerTemporal2_0 {
    fn convert_from_temporal_inner(_: InnerTemporal3_2) -> Self {
        Self::new(true, PeakData::default())
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

// Implement common methods to manipulate root keywords

pub trait VersionedMetaroot: Sized + Versioned {
    /// Return value of $GATE if it exists.
    fn gate(&self) -> Option<Gate>;

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
        par: Par,
        names: &NamedSet<'_>,
    ) -> impl Iterator<Item = RemovedLink>;

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

    fn keywords_req_inner(&self) -> impl Iterator<Item = ReqRootKeyword<'_>>;

    fn keywords_opt_inner(&self) -> impl Iterator<Item = OptRootKeyword<'_>>;
}

impl VersionedMetaroot for InnerMetaroot2_0 {
    fn gate(&self) -> Option<Gate> {
        let g: &GatedMeasurements = self.applied_gates.as_ref();
        g.gate()
    }

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
        par: Par,
        _: &NamedSet<'_>,
    ) -> impl Iterator<Item = RemovedLink> {
        Compensation2_0::remove_invalid_link(&mut self.comp, par).into_iter()
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

    fn keywords_req_inner(&self) -> impl Iterator<Item = ReqRootKeyword<'_>> {
        once(ReqRootKeyword::from_value(self.mode))
    }

    fn keywords_opt_inner(&self) -> impl Iterator<Item = OptRootKeyword<'_>> {
        let cyt = OptRootKeyword::from_str(&self.cyt);
        self.comp
            .as_ref()
            .map(Compensation2_0::non_zero_indices)
            .into_iter()
            .flatten()
            .map(|x| SplitKeyword::new(DKey2::new_i2(x.col, x.row), x.value))
            .map(OptRootKeyword::from)
            .chain(cyt)
            .chain(self.applied_gates.opt_keywords())
            .chain(self.timestamps.opt_keywords())
    }
}

impl VersionedMetaroot for InnerMetaroot3_0 {
    fn gate(&self) -> Option<Gate> {
        let g: &GatedMeasurements = self.applied_gates.as_ref();
        g.gate()
    }

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
        par: Par,
        _: &NamedSet<'_>,
    ) -> impl Iterator<Item = RemovedLink> {
        let comp = Compensation3_0::remove_invalid_link(&mut self.comp, par).map(RemovedLink::from);
        let ag = self.applied_gates.remove_invalid_links(par);
        comp.into_iter().chain(ag)
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
            (0..par.0)
                .map(IndexFromOne::from)
                .try_into_nonempty_iter()
                .map(|js| ExistingIndexedLinkError::new(DKey0::default(), js.collect()))
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

    fn keywords_req_inner(&self) -> impl Iterator<Item = ReqRootKeyword<'_>> {
        once(ReqRootKeyword::from_value(self.mode))
    }

    fn keywords_opt_inner(&self) -> impl Iterator<Item = OptRootKeyword<'_>> {
        let x0 = OptRootKeyword::from_str(&self.cyt);
        let x1 = OptRootKeyword::from_str(&self.cytsn);
        let x2 = self.comp.as_ref().map(OptRootKeyword::from_ref);
        let x3 = self.unicode.as_ref().map(OptRootKeyword::from_ref);
        [x0, x1, x2, x3]
            .into_iter()
            .flatten()
            .chain(self.applied_gates.opt_keywords())
            .chain(self.subset.opt_keywords())
            .chain(self.timestamps.opt_keywords())
    }
}

impl VersionedMetaroot for InnerMetaroot3_1 {
    fn gate(&self) -> Option<Gate> {
        let g: &GatedMeasurements = self.applied_gates.as_ref();
        g.gate()
    }

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
        par: Par,
        names: &NamedSet<'_>,
    ) -> impl Iterator<Item = RemovedLink> {
        let spill = Spillover::remove_invalid_link(&mut self.spillover, names);
        self.applied_gates
            .remove_invalid_links(par)
            .into_iter()
            .chain(spill.map(RemovedLink::from))
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

    fn keywords_req_inner(&self) -> impl Iterator<Item = ReqRootKeyword<'_>> {
        once(ReqRootKeyword::from_value(self.mode))
    }

    fn keywords_opt_inner(&self) -> impl Iterator<Item = OptRootKeyword<'_>> {
        let x0 = OptRootKeyword::from_str(&self.cyt);
        let x1 = OptRootKeyword::from_str(&self.cytsn);
        let x2 = self.spillover.as_ref().map(OptRootKeyword::from_ref);
        let x3 = self.vol.map(OptRootKeyword::from_value);
        [x0, x1, x2, x3]
            .into_iter()
            .flatten()
            .chain(self.applied_gates.opt_keywords())
            .chain(self.subset.opt_keywords())
            .chain(self.modification.opt_keywords())
            .chain(self.plate.opt_keywords())
            .chain(self.timestamps.opt_keywords())
    }
}

impl VersionedMetaroot for InnerMetaroot3_2 {
    fn gate(&self) -> Option<Gate> {
        None
    }

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
        par: Par,
        names: &NamedSet<'_>,
    ) -> impl Iterator<Item = RemovedLink> {
        let uc = self.unstained.unstainedcenters.remove_invalid_links(names);
        let spill = Spillover::remove_invalid_link(&mut self.spillover, names);
        self.applied_gates
            .0
            .remove_invalid_links(par)
            .into_iter()
            .chain(spill.map(RemovedLink::from))
            .chain(uc.map(RemovedLink::from))
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

    fn keywords_req_inner(&self) -> impl Iterator<Item = ReqRootKeyword<'_>> {
        once(ReqRootKeyword::from_ref(&self.cyt))
    }

    fn keywords_opt_inner(&self) -> impl Iterator<Item = OptRootKeyword<'_>> {
        let x0 = OptRootKeyword::from_str(&self.cytsn);
        let x1 = OptRootKeyword::from_str(&self.flowrate);
        let x2 = self.mode.map(OptRootKeyword::from_value);
        let x3 = self.spillover.as_ref().map(OptRootKeyword::from_ref);
        let x4 = self.vol.map(OptRootKeyword::from_value);
        [x0, x1, x2, x3, x4]
            .into_iter()
            .flatten()
            .chain(self.applied_gates.opt_keywords())
            .chain(self.unstained.opt_keywords())
            .chain(self.modification.opt_keywords())
            .chain(self.carrier.opt_keywords())
            .chain(self.plate.opt_keywords())
            .chain(self.timestamps.opt_keywords())
            .chain(self.datetimes.opt_keywords())
    }
}

// Implement common methods to manipulate optical keywords

pub trait VersionedOptical: Sized + Versioned {
    fn req_keywords_inner(&self, i: MeasIndex) -> impl Iterator<Item = ReqMeasKeyword<'_>>;

    fn opt_keywords_inner(&self, i: MeasIndex) -> impl Iterator<Item = OptOpticalKeyword<'_>>;
}

impl VersionedOptical for InnerOptical2_0 {
    fn req_keywords_inner(&self, _: MeasIndex) -> impl Iterator<Item = ReqMeasKeyword<'_>> {
        empty()
    }

    fn opt_keywords_inner(&self, i: MeasIndex) -> impl Iterator<Item = OptOpticalKeyword<'_>> {
        let x0 = self.scale.map(|v| OptOpticalKeyword::from_value(v, i));
        let x1 = self.wavelength.map(|v| OptOpticalKeyword::from_value(v, i));
        let ps = self.peak.opt_keywords(i).map(OptOpticalKeyword::from);
        [x0, x1].into_iter().flatten().chain(ps)
    }
}

impl VersionedOptical for InnerOptical3_0 {
    fn req_keywords_inner(&self, i: MeasIndex) -> impl Iterator<Item = ReqMeasKeyword<'_>> {
        self.scale.req_suffixes(i)
    }

    fn opt_keywords_inner(&self, i: MeasIndex) -> impl Iterator<Item = OptOpticalKeyword<'_>> {
        let ps = self.peak.opt_keywords(i).map(OptOpticalKeyword::from);
        let sc = self.scale.opt_keyword(i);
        let w = self.wavelength.map(|v| OptOpticalKeyword::from_value(v, i));
        w.into_iter().chain(ps).chain(sc)
    }
}

impl VersionedOptical for InnerOptical3_1 {
    fn req_keywords_inner(&self, i: MeasIndex) -> impl Iterator<Item = ReqMeasKeyword<'_>> {
        self.scale.req_suffixes(i)
    }

    fn opt_keywords_inner(&self, i: MeasIndex) -> impl Iterator<Item = OptOpticalKeyword<'_>> {
        let x0 = OptOpticalKeyword::from_wavelengths(&self.wavelengths, i);
        let x1 = self
            .calibration
            .as_ref()
            .map(|v| OptOpticalKeyword::from_ref(v, i));
        let x2 = self.display.map(|v| OptOpticalKeyword::from_value(v, i));
        let ps = self.peak.opt_keywords(i).map(OptOpticalKeyword::from);
        let sc = self.scale.opt_keyword(i);
        [x0, x1, x2].into_iter().flatten().chain(ps).chain(sc)
    }
}

impl VersionedOptical for InnerOptical3_2 {
    fn req_keywords_inner(&self, i: MeasIndex) -> impl Iterator<Item = ReqMeasKeyword<'_>> {
        self.scale.req_suffixes(i)
    }

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
        [x0, x1, x2, x3, x4, x5, x6, x7]
            .into_iter()
            .flatten()
            .chain(self.scale.opt_keyword(i))
    }
}

// Implement common methods to manipulate temporal keywords

pub trait VersionedTemporal: Sized + Versioned {
    type Warning;
    type Error;

    fn req_meas_keywords_inner(&self, i: MeasIndex) -> Option<ReqMeasKeyword<'_>>;

    fn opt_meas_keywords_inner(&self, i: MeasIndex)
    -> impl Iterator<Item = OptTemporalKeyword<'_>>;

    fn can_convert_to_optical(&self, i: MeasIndex) -> Result<(), Self::Error>;
}

impl VersionedTemporal for InnerTemporal2_0 {
    type Warning = Nothing<()>;
    type Error = Infallible;

    fn req_meas_keywords_inner(&self, _: MeasIndex) -> Option<ReqMeasKeyword<'_>> {
        None
    }

    fn opt_meas_keywords_inner(
        &self,
        i: MeasIndex,
    ) -> impl Iterator<Item = OptTemporalKeyword<'_>> {
        let s = OptTemporalKeyword::from_opt_zst(self.scale, i);
        let ps = self.peak.opt_keywords(i).map(OptTemporalKeyword::from);
        ps.chain(s)
    }

    fn can_convert_to_optical(&self, _: MeasIndex) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl VersionedTemporal for InnerTemporal3_0 {
    type Warning = Nothing<()>;
    type Error = Infallible;

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

    fn can_convert_to_optical(&self, _: MeasIndex) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl VersionedTemporal for InnerTemporal3_1 {
    type Warning = Nothing<()>;
    type Error = Infallible;

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

    fn can_convert_to_optical(&self, _: MeasIndex) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl VersionedTemporal for InnerTemporal3_2 {
    type Warning = Option<AnyTemporalToOpticalKeyLossError>;
    type Error = AnyTemporalToOpticalKeyLossError;

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

    fn can_convert_to_optical(&self, i: MeasIndex) -> Result<(), Self::Error> {
        OptTemporalKeyword::from_opt_zst(self.measurement_type, i)
            .and_then(|x| x.as_optical_loss_error())
            .map_or(Ok(()), Err)
    }
}

// Implement common method to swap optical and temporal measurement

pub trait SwapOpticalWithTemporal<T: VersionedTemporal>: Sized + VersionedOptical {
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
        let new_t = InnerTemporal2_0::new(o.scale.is_some(), o.peak);
        let new_o = Self::new(bool::from(t.scale).then_some(Scale::Linear), None, t.peak);
        (new_o, new_t)
    }
}

impl SwapOpticalWithTemporal<InnerTemporal3_0> for InnerOptical3_0 {
    fn swap_optical_temporal_inner(t: InnerTemporal3_0, o: Self) -> (Self, InnerTemporal3_0) {
        let new_t = InnerTemporal3_0::new(t.timestep, o.peak);
        let new_o = Self::new(ScaleTransform::default(), None, t.peak);
        (new_o, new_t)
    }
}

impl SwapOpticalWithTemporal<InnerTemporal3_1> for InnerOptical3_1 {
    fn swap_optical_temporal_inner(t: InnerTemporal3_1, o: Self) -> (Self, InnerTemporal3_1) {
        let new_t = InnerTemporal3_1::new(t.timestep, o.display, o.peak);
        let new_o = Self::new(
            ScaleTransform::default(),
            Wavelengths::default(),
            None,
            t.display,
            t.peak,
        );
        (new_o, new_t)
    }
}

impl SwapOpticalWithTemporal<InnerTemporal3_2> for InnerOptical3_2 {
    fn swap_optical_temporal_inner(t: InnerTemporal3_2, o: Self) -> (Self, InnerTemporal3_2) {
        let new_t = InnerTemporal3_2::new(t.timestep, o.display, TemporalType::default());
        let new_o = Self::new(
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

// Implement method to convert optical -> temporal conversion

pub trait TemporalFromOptical<O: VersionedOptical>: Sized {
    type TData;

    fn from_optical(
        opt: Optical<O>,
        i: MeasIndex,
        data: Self::TData,
        allow_loss: AllowLoss,
    ) -> SwitchableErrorResult<Temporal<Self>, Optical<O>, AllowLoss, OpticalToTemporalErrors> {
        let es = opt
            .opt_keywords(i)
            .filter_map(|x| x.as_temporal_loss_error());

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

// Implement method to convert temporal -> optical conversion

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

// Implement conversions between scale and scale transforms

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

impl CommonMeasurement {
    fn lookup(std: &mut StdKeywords, nonstd: NonStdKeywords, i: MeasIndex) -> Self {
        let longname = Longname::remove_meas_opt_nofail(std, i);
        Self::new(longname, nonstd)
    }
}

// Implement methods for root keyword type

impl<M: VersionedMetaroot> Metaroot<M> {
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

    fn lookup_metaroot<C, N>(
        std: &mut StdKeywords,
        ms: &[N],
        mut nonstd: NonStdKeywords,
        conf: &C,
    ) -> LookupMetarootResult<DiagnosedMetaroot<Self>>
    where
        M: LookupMetaroot<N>,
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
            .map_ok_value(|(abrt, lost, tr_out, meta)| {
                let (tr, tr_trimmed) = tr_out.into_opt_root_pair();
                let ret = Self::new(
                    abrt, com, cells, exp, fil, inst, lost, op, proj, smno, src, sys, tr,
                    meta.this, nonstd,
                );
                let trimmed2 = meta.trimmed.into_iter().chain(tr_trimmed).collect();
                DiagnosedMetaroot::new(ret, trimmed2, meta.fixed_gate_scales)
            })
    }

    fn req_keywords(&self, par: Par) -> impl Iterator<Item = ReqRootKeyword<'_>> {
        once(ReqRootKeyword::from_value(par)).chain(self.specific.keywords_req_inner())
    }

    fn opt_and_nonstd_keywords(&self) -> impl Iterator<Item = StdOrNonStdOptRootKeyword<'_>> {
        let x0 = OptRootKeyword::from_str(&self.com);
        let x1 = OptRootKeyword::from_str(&self.cells);
        let x2 = OptRootKeyword::from_str(&self.exp);
        let x3 = OptRootKeyword::from_str(&self.fil);
        let x4 = OptRootKeyword::from_str(&self.inst);
        let x5 = OptRootKeyword::from_str(&self.op);
        let x6 = OptRootKeyword::from_str(&self.proj);
        let x7 = OptRootKeyword::from_str(&self.smno);
        let x8 = OptRootKeyword::from_str(&self.src);
        let x9 = OptRootKeyword::from_str(&self.sys);
        let x10 = self.abrt.map(OptRootKeyword::from_value);
        let x11 = self.lost.map(OptRootKeyword::from_value);
        let x12 = self.tr.as_ref().map(OptRootKeyword::from_ref);
        let ns = self
            .nonstandard_keywords
            .iter()
            .map(|(k, v)| NonStdKeyword::new(k, v.as_ne_str()))
            .map(StdOrNonStdOptRootKeyword::from);
        [x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12]
            .into_iter()
            .flatten()
            .chain(self.specific.keywords_opt_inner())
            .map(StdOrNonStdOptRootKeyword::from)
            .chain(ns)
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
        demote: bool,
    ) -> Vec<BrokenOrDependentLinkError> {
        let tr = Trigger::remove_invalid_links(&mut self.tr, names);
        let mut es = vec![];
        for x in self
            .specific
            .remove_invalid_links(par, names)
            .chain(tr.map(RemovedLink::from))
        {
            if demote {
                x.insert_keyvals(&mut self.nonstandard_keywords);
            }
            x.push_errors(&mut es);
        }
        es
    }
}

// Implement methods for optical keyword type

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
    ) -> OpticalConvertResult<Optical<Of>>
    where
        O: VersionedOptical,
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

    fn lookup_optical<C>(
        std: &mut StdKeywords,
        i: MeasIndex,
        mut nonstd: NonStdKeywords,
        dt: AlphaNumType,
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
        let specific = O::lookup_specific(std, &mut nonstd, i, dt, conf);
        let common = CommonMeasurement::lookup(std, nonstd, i);
        go!(power)
            .zip4_commutative(go!(perc_emit), go!(det_volt), specific)
            .map_ok_value(|(p, e, v, s_out)| {
                let ret = Self::new(common, filter, p, det_type, e, v, s_out.this);
                DiagnosedOptical::new(ret, s_out.scale, s_out.trimmed)
            })
    }

    fn req_keywords(&self, i: MeasIndex) -> impl Iterator<Item = ReqMeasKeyword<'_>>
    where
        O: VersionedOptical,
    {
        self.specific.req_keywords_inner(i)
    }

    fn opt_keywords(&self, i: MeasIndex) -> impl Iterator<Item = OptOpticalKeyword<'_>>
    where
        O: VersionedOptical,
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

    fn opt_and_nonstd_keywords(
        &self,
        i: MeasIndex,
    ) -> impl Iterator<Item = StdOrNonStdOptMeasKeyword<'_>>
    where
        O: VersionedOptical,
    {
        let cs = self
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
}

// Implement methods for temporal keyword type

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
                specific.timestep_added,
            )
        })
    }

    fn convert<ToT>(self, i: MeasIndex, flag: AllowLoss) -> TemporalConvertResult<Temporal<ToT>>
    where
        ToT: ConvertFromTemporal<T>,
        T: VersionedTemporal,
    {
        ToT::convert_from_temporal(self.specific, i, flag)
            .map_deferred_value(|specific| Temporal::new(self.common, specific))
    }

    fn req_meas_keywords(&self, i: MeasIndex) -> Option<ReqMeasKeyword<'_>>
    where
        T: VersionedTemporal,
    {
        self.specific.req_meas_keywords_inner(i)
    }

    fn opt_meas_keywords(&self, i: MeasIndex) -> impl Iterator<Item = OptTemporalKeyword<'_>>
    where
        T: VersionedTemporal,
    {
        OptTemporalKeyword::from_str(&self.common.longname, i)
            .into_iter()
            .chain(self.specific.opt_meas_keywords_inner(i))
    }

    fn all_opt_keywords(&self, i: MeasIndex) -> impl Iterator<Item = StdOrNonStdOptMeasKeyword<'_>>
    where
        T: VersionedTemporal,
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
}

// Implement methods for Core*

impl CoreTEXT2_0 {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn try_new_2_0(
        measurements: TemporalsAndOpticals2_0,
        data_schema: DataSchema2_0,
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
                Self::try_new_nodrop(metaroot, measurements, data_schema)
                    .map_errors(NewCoreTEXTError::from)
            })
    }
}

impl CoreTEXT3_0 {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn try_new_3_0(
        measurements: TemporalsAndOpticals3_0,
        data_schema: DataSchema3_0,
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
                Self::try_new_nodrop(metaroot, measurements, data_schema)
                    .map_errors(NewCoreTEXTError::from)
            })
    }
}

impl CoreTEXT3_1 {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn try_new_3_1(
        measurements: TemporalsAndOpticals3_1,
        data_schema: DataSchema3_1,
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
                Self::try_new_nodrop(metaroot, measurements, data_schema)
                    .map_errors(NewCoreTEXTError::from)
            })
    }
}

impl CoreTEXT3_2 {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn try_new_3_2(
        measurements: TemporalsAndOpticals3_2,
        data_schema: DataSchema3_2,
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
                Self::try_new_nodrop(metaroot, measurements, data_schema)
                    .map_errors(NewCoreTEXTError::from)
            })
    }
}

impl<A, L, O, M, T, P, N, V> Core<A, L, O, M, T, P, N, V> {
    /// Return $PAR, which is simply the number of measurements in this struct
    pub fn par(&self) -> Par {
        Par(self.measurements.len())
    }

    fn new(
        metaroot: Metaroot<M>,
        measurements: NamedVec<N, Temporal<T>, Optical<P>>,
        mut layout: L,
        analysis: A,
        others: O,
    ) -> Self
    where
        L: LayoutNormalize,
    {
        layout.normalize();
        Self {
            metaroot,
            measurements,
            layout,
            analysis,
            others,
            _version: PhantomData,
        }
    }
}

impl<V, A, L, O> VersionedCore<A, L, O, V>
where
    V: VersionSet,
{
    /// Show FCS version.
    pub fn fcs_version(&self) -> Version {
        V::as_version()
    }

    pub fn write_texts(
        path: &PathBuf,
        cores: &[Self],
        conf: &WriteTEXTInnerConfig,
    ) -> Result<Option<Nextdata>, ImpureError<WriteTEXTHeaderError>>
    where
        L: LayoutKeywords + LayoutOptMeasKeywords,
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
    ) -> Result<Nextdata, ImpureError<WriteTEXTHeaderError>>
    where
        L: LayoutKeywords + LayoutOptMeasKeywords,
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
    ) -> Result<Nextdata, ImpureError<WriteTEXTHeaderError>>
    where
        L: LayoutKeywords + LayoutOptMeasKeywords,
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
    ) -> Result<Nextdata, ImpureError<WriteTEXTHeaderError>>
    where
        L: LayoutKeywords + LayoutOptMeasKeywords,
        T: TryFrom<u64, Error = Uint8DigitOverflowError>
            + Copy
            + Zero
            + fmt::Display
            + HeaderString
            + Into<u64>,
    {
        let conf = WriteHeaderAndTextConfig::new_nodata(delim, has_nextdata);
        self.h_write_text_inner::<_, T>(h, &conf)
    }

    fn h_write_text_inner<W: Write, T>(
        &self,
        h: &mut BufWriter<W>,
        conf: &WriteHeaderAndTextConfig<'_>,
    ) -> Result<Nextdata, ImpureError<WriteTEXTHeaderError>>
    where
        L: LayoutKeywords + LayoutOptMeasKeywords,
        T: TryFrom<u64, Error = Uint8DigitOverflowError>
            + Copy
            + Zero
            + fmt::Display
            + HeaderString
            + Into<u64>,
    {
        let hdr_kws: HeaderKeywordsToWrite<T> = self
            .header_and_flat_keywords(conf)
            .map_err(ImpureError::Pure)?;
        hdr_kws.h_write(h, V::as_version(), conf.other_segs)?;
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
    ) -> HashMap<NEString, NEString>
    where
        L: LayoutKeywords + LayoutOptMeasKeywords,
    {
        fn go(
            xs: impl Iterator<Item = (NEString, NEString)>,
            include: bool,
        ) -> impl Iterator<Item = (NEString, NEString)> {
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

        let req_root = self.req_root_keywords().map(|x| x.as_str_pair());
        let opt_root = self.opt_root_keywords().map(|x| x.as_str_pair());
        let req_meas = self.req_meas_keywords().map(|x| x.as_str_pair());
        let opt_meas = self.opt_meas_keywords().map(|x| x.as_str_pair());
        go(req_root, include_req_root)
            .chain(go(opt_root, include_opt_root))
            .chain(go(req_meas, include_req_meas))
            .chain(go(opt_meas, include_opt_meas))
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
            .map(|x| x.both(|t| Some(&t.key), |m| V::Name::as_opt(&m.key)))
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

    /// Set all $PnN keywords to list of names.
    pub fn set_measurement_shortnames_maybe(
        &mut self,
        ns: Vec<Option<Shortname>>,
    ) -> Result<NameMapping, SetKeysError>
    where
        V: VersionSet<Name = Option<Shortname>>,
    {
        let mapping = self.measurements.set_keys(ns)?;
        self.metaroot.rename_meas_links(&mapping);
        Ok(mapping)
    }

    /// Set the measurement matching given name to be the time measurement.
    pub fn set_temporal(
        &mut self,
        n: &Shortname,
        timestep: <V::Temporal as TemporalFromOptical<V::Optical>>::TData,
        allow_loss: AllowLoss,
    ) -> WarningOrErrorResult<bool, (), SetTemporalError, SetTemporalByNameError>
    where
        V::Temporal: TemporalFromOptical<V::Optical>,
        V::Optical: SwapOpticalWithTemporal<V::Temporal>,
    {
        self.measurements.set_center_by_name(
            n,
            |old, new| {
                V::Optical::swap_optical_temporal(old, new, allow_loss)
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

    /// Set the measurement at given index to the time measurement.
    pub fn set_temporal_at(
        &mut self,
        index: MeasIndex,
        timestep: <V::Temporal as TemporalFromOptical<V::Optical>>::TData,
        allow_loss: AllowLoss,
    ) -> WarningOrErrorResult<bool, (), SetTemporalError, SetTemporalByIndexError>
    where
        V::Temporal: TemporalFromOptical<V::Optical>,
        V::Optical: SwapOpticalWithTemporal<V::Temporal>,
    {
        self.measurements.set_center_by_index(
            index,
            |old, new| {
                V::Optical::swap_optical_temporal(old, new, allow_loss)
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

    /// Convert time measurement to optical measurement.
    ///
    /// Return true if a time measurement existed and was converted, false
    /// otherwise.
    pub fn unset_temporal(
        &mut self,
    ) -> Option<<V::Optical as OpticalFromTemporal<V::Temporal>>::TData>
    where
        V::Optical: OpticalFromTemporal<V::Temporal, LossFlag = ()>,
        V::Temporal: VersionedTemporal<Warning = Nothing<()>, Error = Infallible>,
    {
        self.measurements
            .unset_center(|i, old_t| V::Optical::from_temporal(old_t, i, ()))
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
        Option<<V::Optical as OpticalFromTemporal<V::Temporal>>::TData>,
        (),
        AnyTemporalToOpticalKeyLossError,
        AnyTemporalToOpticalKeyLossError,
    >
    where
        V::Optical: OpticalFromTemporal<V::Temporal, LossFlag = AllowLoss>,
        V::Temporal: VersionedTemporal<
                Warning = Option<AnyTemporalToOpticalKeyLossError>,
                Error = AnyTemporalToOpticalKeyLossError,
            >,
    {
        self.measurements.unset_center(|i, old_t| {
            V::Optical::from_temporal(old_t, i, allow_loss).switchable_into_non_commutative()
        })
    }

    /// Add time measurement to the end of the measurement vector.
    ///
    /// Return error if time measurement already exists or name is non-unique.
    pub fn push_temporal<C>(
        &mut self,
        n: Shortname,
        m: Temporal<V::Temporal>,
        r: C,
    ) -> GroupResult<(), PushTemporalError<<L as LayoutInsert<C>>::Error>, PushTemporalSummary>
    where
        L: LayoutInsert<C>,
    {
        self.push_temporal_inner(n, m, r).group().resolve_nowarn()
    }

    /// Add time measurement at the given position
    ///
    /// Return error if time measurement already exists, range is incompatible,
    /// name is non-unique, or index is out of bounds.
    pub fn insert_temporal<C>(
        &mut self,
        i: MeasIndex,
        n: Shortname,
        m: Temporal<V::Temporal>,
        r: C,
    ) -> GroupResult<(), InsertTemporalError<<L as LayoutInsert<C>>::Error>, InsertTemporalSummary>
    where
        L: LayoutInsert<C>,
    {
        self.insert_temporal_inner(i, n, m, r)
            .group()
            .resolve_nowarn()
    }

    /// Add optical measurement to the end of the measurement vector
    ///
    /// Return error if name is non-unique or range is incompatible.
    pub fn push_optical<C>(
        &mut self,
        n: V::Name,
        m: Optical<V::Optical>,
        r: C,
    ) -> GroupResult<Shortname, PushOpticalError<<L as LayoutInsert<C>>::Error>, PushOpticalSummary>
    where
        L: LayoutInsert<C>,
    {
        self.push_optical_inner(n, m, r).group().resolve_nowarn()
    }

    /// Add optical measurement at a given position
    ///
    /// Return error if name is non-unique, range is incompatible, or index is
    /// out of bounds.
    pub fn insert_optical<C>(
        &mut self,
        i: MeasIndex,
        n: V::Name,
        m: Optical<V::Optical>,
        r: C,
    ) -> GroupResult<
        Shortname,
        InsertOpticalError<<L as LayoutInsert<C>>::Error>,
        InsertOpticalSummary,
    >
    where
        L: LayoutInsert<C>,
    {
        self.insert_optical_inner(i, n, m, r)
            .group()
            .resolve_nowarn()
    }

    /// Read nonstandard key/value pairs for each measurement.
    ///
    /// This includes the time measurement if present.
    pub fn get_meas_nonstandard(&self) -> Vec<&HashMap<NonStdKey, NEString>> {
        self.measurements.iter_common_values().collect()
    }

    /// Set nonstandard key/value pairs for each measurement.
    ///
    /// This includes the time measurement if present.
    pub fn set_meas_nonstandard(
        &mut self,
        xs: impl IntoIterator<Item = HashMap<NonStdKey, NEString>>,
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
        m: Optical<V::Optical>,
    ) -> Result<TemporalOrOptical<V>, ElementIndexError> {
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
        m: Optical<V::Optical>,
    ) -> Result<TemporalOrOptical<V>, NameNotFoundError> {
        self.measurements.replace_named(name, m)
    }

    /// Replace temporal measurement at index.
    pub fn replace_temporal_at(
        &mut self,
        index: MeasIndex,
        m: Temporal<V::Temporal>,
    ) -> Result<TemporalOrOptical<V>, SetCenterError>
    where
        V::Optical: OpticalFromTemporal<V::Temporal, LossFlag = ()>,
        V::Temporal: VersionedTemporal<Warning = Nothing<()>, Error = Infallible>,
    {
        self.measurements
            .replace_center_at_nofail(index, m, |i, old_t| {
                V::Optical::from_temporal(old_t, i, ())
                    .set_err_value(())
                    .infallible_nowarn_into()
                    .0
            })
    }

    /// Replace temporal measurement at index.
    pub fn replace_temporal_at_lossy(
        &mut self,
        index: MeasIndex,
        m: Temporal<V::Temporal>,
        allow_loss: AllowLoss,
    ) -> WarningOrErrorResult<
        TemporalOrOptical<V>,
        (),
        AnyTemporalToOpticalKeyLossError,
        ReplaceTemporalErrorByIndex,
    >
    where
        V::Optical: OpticalFromTemporal<V::Temporal, LossFlag = AllowLoss>,
        V::Temporal: VersionedTemporal<
                Warning = Option<AnyTemporalToOpticalKeyLossError>,
                Error = AnyTemporalToOpticalKeyLossError,
            >,
    {
        self.measurements.replace_center_at(index, m, |i, old_t| {
            V::Optical::from_temporal(old_t, i, allow_loss)
                .switchable_into_non_commutative()
                .map_ok_value(|(x, _)| x)
                .map_errors(ReplaceTemporalErrorByIndex::from)
        })
    }

    /// Replace temporal measurement with name.
    pub fn replace_temporal_named(
        &mut self,
        name: &Shortname,
        m: Temporal<V::Temporal>,
    ) -> Result<TemporalOrOptical<V>, NameNotFoundError>
    where
        V::Optical: OpticalFromTemporal<V::Temporal, LossFlag = ()>,
        V::Temporal: VersionedTemporal<Warning = Nothing<()>, Error = Infallible>,
    {
        self.measurements
            .replace_center_by_name_nofail(name, m, |i, old_t| {
                V::Optical::from_temporal(old_t, i, ())
                    .set_err_value(())
                    .infallible_nowarn_into()
                    .0
            })
    }

    /// Replace temporal measurement with name.
    pub fn replace_temporal_named_lossy(
        &mut self,
        name: &Shortname,
        m: Temporal<V::Temporal>,
        allow_loss: AllowLoss,
    ) -> WarningOrErrorResult<
        TemporalOrOptical<V>,
        (),
        AnyTemporalToOpticalKeyLossError,
        ReplaceTemporalErrorByName,
    >
    where
        V::Optical: OpticalFromTemporal<V::Temporal, LossFlag = AllowLoss>,
        V::Temporal: VersionedTemporal<
                Warning = Option<AnyTemporalToOpticalKeyLossError>,
                Error = AnyTemporalToOpticalKeyLossError,
            >,
    {
        self.measurements
            .replace_center_by_name(name, m, |i, old_t| {
                V::Optical::from_temporal(old_t, i, allow_loss)
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
        key: V::Name,
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
        F: Fn(IndexedElement<&Shortname, &mut Temporal<V::Temporal>>) -> R,
        G: Fn(IndexedElement<&V::Name, &mut Optical<V::Optical>>) -> R,
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
        F: Fn(IndexedElement<&Shortname, &mut Temporal<V::Temporal>>, X) -> R,
        G: Fn(IndexedElement<&V::Name, &mut Optical<V::Optical>>, X) -> R,
    {
        self.measurements.alter_values_zip(xs, f, g)
    }

    /// Return reference to time measurement as a name/value pair.
    pub fn temporal(&self) -> Option<IndexedElement<&Shortname, &Temporal<V::Temporal>>> {
        self.measurements.as_center()
    }

    /// Return mutable reference to time measurement as a name/value pair.
    pub fn temporal_mut(
        &mut self,
    ) -> Option<IndexedElement<&mut Shortname, &mut Temporal<V::Temporal>>> {
        self.measurements.as_center_mut()
    }

    /// Return a reference to a field in metaroot
    pub fn metaroot<X>(&self) -> &X
    where
        Metaroot<V::Metaroot>: AsRef<X>,
    {
        self.metaroot.as_ref()
    }

    /// Return a reference to an optional field in metaroot
    pub fn metaroot_opt<X>(&self) -> Option<&X>
    where
        Metaroot<V::Metaroot>: AsRef<Option<X>>,
    {
        self.metaroot().as_ref()
    }

    /// Set a field in metaroot
    pub fn set_metaroot<X>(&mut self, x: X)
    where
        Metaroot<V::Metaroot>: AsMut<X>,
    {
        *self.metaroot.as_mut() = x;
    }

    /// Get a field from all measurements as an interator
    pub fn meas<'a, X: 'a>(&'a self) -> impl Iterator<Item = &'a X>
    where
        Temporal<V::Temporal>: AsRef<X>,
        Optical<V::Optical>: AsRef<X>,
    {
        self.measurements
            .iter()
            .map(|x| x.both(|t| t.value.as_ref(), |m| m.value.as_ref()))
    }

    /// Get an optional field from all measurements as an interator
    pub fn meas_opt<'a, X: 'a>(&'a self) -> impl Iterator<Item = Option<&'a X>>
    where
        Temporal<V::Temporal>: AsRef<Option<X>>,
        Optical<V::Optical>: AsRef<Option<X>>,
    {
        self.meas::<Option<X>>().map(|x| x.as_ref())
    }

    /// Set the field on all measurements to values in a vector
    pub fn set_meas<X>(&mut self, xs: Vec<X>) -> Result<(), InputLengthError>
    where
        Temporal<V::Temporal>: AsMut<X>,
        Optical<V::Optical>: AsMut<X>,
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
        Optical<V::Optical>: AsRef<X>,
    {
        self.measurements
            .iter()
            .map(|e| e.bimap_once(|_| (), |v| v.value.as_ref()).into())
    }

    /// Return optional field from all optical measurements as an iterator
    pub fn optical_opt<'a, X: 'a>(&'a self) -> impl Iterator<Item = NonCenterElement<Option<&'a X>>>
    where
        Optical<V::Optical>: AsRef<Option<X>>,
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
        Optical<V::Optical>: AsMut<X>,
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
        Temporal<V::Temporal>: AsRef<X>,
        Optical<V::Optical>: AsRef<Y>,
    {
        self.measurements
            .iter()
            .map(|x| x.bimap_once(|m| m.value.as_ref(), |m| m.value.as_ref()))
    }

    /// Set field which is on both optical and temporal measurement types
    pub fn set_temporal_optical<T>(&mut self, xs: Vec<T>) -> Result<(), InputLengthError>
    where
        Optical<V::Optical>: AsMut<T>,
        Temporal<V::Temporal>: AsMut<T>,
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
        Temporal<V::Temporal>: AsMut<X>,
        Optical<V::Optical>: AsMut<Y>,
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
        Metaroot<V::Metaroot>: AsRef<Option<Btim<X>>>,
    {
        self.time_naive()
    }

    /// Get value for $ETIM as a [`NaiveTime`]
    pub fn etim_naive<X>(&self) -> Option<NaiveTime>
    where
        X: Copy,
        NaiveTime: From<X>,
        Metaroot<V::Metaroot>: AsRef<Option<Etim<X>>>,
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
        Metaroot<V::Metaroot>: AsMut<Timestamps<X>>,
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
        Metaroot<V::Metaroot>: AsMut<Timestamps<X>>,
    {
        let t = self.metaroot.as_mut();
        t.set_etim(time.map(|x| Xtim(x.into())))
    }

    /// Get $DATE as a [`NaiveDate`]
    pub fn date_naive(&self) -> Option<NaiveDate>
    where
        Metaroot<V::Metaroot>: AsRef<Option<FCSDate>>,
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
        Metaroot<V::Metaroot>: AsMut<Timestamps<X>>,
    {
        self.metaroot.as_mut().set_date(date.map(Into::into))
    }

    /// Get $BEGINDATETIME as a [`DateTime<FixedOffset>`]
    pub fn begindatetime(&self) -> Option<DateTime<FixedOffset>>
    where
        Metaroot<V::Metaroot>: AsRef<Option<BeginDateTime>>,
    {
        self.metaroot.as_ref().as_ref().copied().map(Into::into)
    }

    /// Get $ENDDATETIME as a [`DateTime<FixedOffset>`]
    pub fn enddatetime(&self) -> Option<DateTime<FixedOffset>>
    where
        Metaroot<V::Metaroot>: AsRef<Option<EndDateTime>>,
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
        Metaroot<V::Metaroot>: AsMut<Datetimes>,
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
        Metaroot<V::Metaroot>: AsMut<Datetimes>,
    {
        self.metaroot.as_mut().set_end(date.map(Into::into))
    }

    /// Get $TIMESTEP value if the time measurement exists.
    pub fn timestep(&self) -> Option<&Timestep>
    where
        Temporal<V::Temporal>: AsRef<Timestep>,
    {
        self.measurements.as_center().map(|x| x.value.as_ref())
    }

    /// Set $TIMESTEP value if the time measurement exists.
    ///
    /// Return `true` if the time measurement exist (which means its $TIMESTEP
    /// was updated) and `false` otherwise.
    pub fn set_timestep(&mut self, timestep: Timestep) -> Option<Timestep>
    where
        Temporal<V::Temporal>: AsMut<Timestep>,
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
        V::Metaroot: HasCompensation,
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
        V::Metaroot: HasCompensation,
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
        V::Metaroot: AsRef<Option<Spillover>>,
    {
        self.metaroot.specific.as_ref().as_ref()
    }

    /// Set $SPILLOVER
    ///
    /// Return error if any measurements reference temporal measurement or
    /// if supplied matrix is invalid.
    pub fn set_spillover(&mut self, spillover: Option<Spillover>) -> Result<(), SetSpilloverErrors>
    where
        V::Metaroot: HasSpillover,
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
        V::Metaroot: HasUnstainedCenters,
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
        Optical<V::Optical>: AsRef<Option<Scale>>,
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
        Optical<V::Optical>: AsRef<ScaleTransform>,
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
        Optical<V::Optical>: AsRef<Option<Feature>>,
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
        Optical<V::Optical>: AsRef<Option<Feature>>,
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
        Optical<V::Optical>: AsMut<Option<Feature>>,
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
        V::Optical: HasScale,
        L: LayoutDatatype + HasWidth,
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
        V::Optical: HasScaleTransform,
        L: LayoutDatatype + HasWidth,
    {
        let center_xform_not_noop = || {
            self.measurements
                .center_index()
                .map(usize::from)
                .and_then(|i| xforms.get(i))
                .is_some_and(|s| !ScaleTransform::is_noop(s))
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
        V::Metaroot: HasAppliedGates<Gates = AppliedGates3_0>,
    {
        let p = self.par();
        let es = ag.invalid_link_errors(&p);
        ErrorGroup::try_new(es)?;
        *self.metaroot.specific.applied_gates_mut(private::NoTouchy) = ag;
        Ok(())
    }

    /// Set gating keywords (3.2)
    pub fn set_applied_gates_3_2(
        &mut self,
        ag: AppliedGates3_2,
    ) -> GroupResult<(), BrokenRegionLinkError<PrefixedMeasIndex>, SetAppliedGatesSummary>
    where
        V::Metaroot: HasAppliedGates<Gates = AppliedGates3_2>,
    {
        let p = self.par();
        let es = ag.invalid_link_errors(&p);
        ErrorGroup::try_new(es)?;
        *self.metaroot.specific.applied_gates_mut(private::NoTouchy) = ag;
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
    pub fn try_convert<Vf, Lf>(
        self,
        allow_loss: AllowLoss,
    ) -> WarningsAndGroupResult<
        VersionedCore<A, Lf, O, Vf>,
        MetarootConvertWarning,
        ConvertError,
        ConvertSummary,
    >
    where
        Vf: VersionSet,
        Vf::Metaroot: ConvertFromMetaroot<V::Metaroot>,
        Vf::Optical: ConvertFromOptical<V::Optical>,
        Vf::Temporal: ConvertFromTemporal<V::Temporal>,
        Vf::Name: MightHave<Shortname> + Clone + ConvertFromShortname<V::Name>,
        // TODO technically normalize shouldn't be needed here but it won't hurt anything
        Lf: ConvertFromLayout<L> + LayoutNormalize,
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
                meas.try_rewrapped(|i, n| Vf::Name::convert_from_shortname(n, i))
                    .map_errors(ConvertError::Rewrap)
                    .nowarn_into_warn()
            });
        let layout_res = ConvertFromLayout::convert_from_layout(self.layout)
            .map_errors(ConvertError::Layout)
            .nowarn_into_warn();
        let v0 = V::as_version();
        let v1 = Vf::as_version();
        let summary = ConvertSummary::new(v0, v1);
        root_res
            .zip3_commutative(meas_res, layout_res)
            .map_ok_value(|(metaroot, measurements, layout)| {
                Core::new(metaroot, measurements, layout, self.analysis, self.others)
            })
            .group_with(summary)
    }

    /// Get reference to measurement vector.
    pub fn measurements(&self) -> &Measurements<V::Name, V::Temporal, V::Optical> {
        &self.measurements
    }

    /// Set measurements.
    ///
    /// Return error if names are not unique, if there is more than one
    /// time measurement, or if the measurement length doesn't match the
    /// data schema length.
    pub fn set_named_measurements(
        &mut self,
        xs: NamedTemporalsAndOpticals<V>,
        allow_shared_names: bool,
        skip_index_check: bool,
    ) -> Result<(), SetNamedMeasurementsError>
    where
        V::Optical: AsScaleOrTransform,
        <V::Optical as AsScaleOrTransform>::S: CheckedScaleTransform,
        <<V::Optical as AsScaleOrTransform>::S as CheckedScaleTransform>::Summary: Default,
        ScaleDatatypeMismatchError: From<ScaleErrorGroup<V>>,
        L: LayoutDatatype + HasWidth,
    {
        self.set_named_measurements_inner(xs, allow_shared_names, skip_index_check)
    }

    /// Set measurements without $PnN.
    pub fn set_measurements(
        &mut self,
        measurements: TemporalsAndOpticals<V>,
    ) -> Result<(), SetUnnamedMeasurementsError>
    where
        V::Optical: AsScaleOrTransform,
        <V::Optical as AsScaleOrTransform>::S: CheckedScaleTransform,
        <<V::Optical as AsScaleOrTransform>::S as CheckedScaleTransform>::Summary: Default,
        ScaleDatatypeMismatchError: From<ScaleErrorGroup<V>>,
        L: HasWidth + LayoutDatatype,
    {
        self.set_measurements_inner(measurements)
    }

    #[cfg(feature = "serde")]
    fn named_compensation(&self) -> Option<(Vec<Shortname>, DMatrix<f32>)>
    where
        V::Metaroot: HasCompensation,
    {
        self.compensation().as_ref().map(|c| {
            let m: &DMatrix<f32> = c.as_ref();
            (self.all_shortnames(), m.clone())
        })
    }

    #[cfg(feature = "serde")]
    fn named_spillover(&self) -> Option<(Vec<Shortname>, DMatrix<f32>)>
    where
        V::Metaroot: AsRef<Option<Spillover>>,
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
        Metaroot<V::Metaroot>: AsRef<Option<Xtim<IS_ETIM, X>>>,
    {
        let t: &Option<Xtim<IS_ETIM, X>> = self.metaroot.as_ref();
        t.as_ref().map(|&x| x.0.into())
    }

    fn remove_measurement_by_name_inner<C>(
        &mut self,
        name: &Shortname,
    ) -> Result<(MeasIndex, TemporalOrOptical<V>, C), RemoveMeasByNameError>
    where
        L: LayoutRemove<C>,
    {
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

    fn remove_measurement_by_index_inner<C>(
        &mut self,
        index: MeasIndex,
    ) -> Result<(NamedTemporalOrOptical<V>, C), RemoveMeasByIndexError>
    where
        L: LayoutRemove<C>,
    {
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

    fn push_temporal_inner<C>(
        &mut self,
        n: Shortname,
        m: Temporal<V::Temporal>,
        r: C,
    ) -> ErrorsResult<(), (), PushTemporalError<L::Error>>
    where
        L: LayoutInsert<C>,
    {
        self.measurements
            .check_push_center(&n)
            .map_errors(PushTemporalError::Center)
            .nowarn_and_then(|()| {
                self.layout
                    .push(r)
                    .map_err(PushTemporalError::Layout)
                    .into_log()
            })
            .when_ok(|| {
                self.measurements.push_center_nocheck(n, m);
                let i = self.par().0.into();
                self.metaroot.specific.insert_meas_index_inner(i);
            })
    }

    fn insert_temporal_inner<C>(
        &mut self,
        i: MeasIndex,
        n: Shortname,
        m: Temporal<V::Temporal>,
        r: C,
    ) -> ErrorsResult<(), (), InsertTemporalError<L::Error>>
    where
        L: LayoutInsert<C>,
    {
        self.measurements
            .check_insert_center(i, &n)
            .map_errors(InsertTemporalError::Center)
            .nowarn_and_then(|()| {
                self.layout
                    .insert_nocheck(i, r)
                    .map_err(InsertTemporalError::Layout)
                    .into_log()
            })
            .when_ok(|| {
                self.measurements.insert_center_nocheck(i, n, m);
                self.metaroot.specific.insert_meas_index_inner(i);
            })
    }

    fn push_optical_inner<C>(
        &mut self,
        n: V::Name,
        m: Optical<V::Optical>,
        r: C,
    ) -> ErrorsResult<Shortname, (), PushOpticalError<L::Error>>
    where
        L: LayoutInsert<C>,
    {
        self.measurements
            .check_push(&n)
            .map(Cow::into_owned)
            .map_err(PushOpticalError::Unique)
            .into_nowarn()
            .nowarn_and_then(|ret| {
                self.layout
                    .push(r)
                    .map_err(PushOpticalError::Layout)
                    .into_log()
                    .set_ok_value(ret)
            })
            .map_ok_value(|ret| {
                self.measurements.push_nocheck(n, m);
                let i = self.par().0.into();
                self.metaroot.specific.insert_meas_index_inner(i);
                ret
            })
    }

    fn insert_optical_inner<C>(
        &mut self,
        i: MeasIndex,
        n: V::Name,
        m: Optical<V::Optical>,
        r: C,
    ) -> ErrorsResult<Shortname, (), InsertOpticalError<L::Error>>
    where
        L: LayoutInsert<C>,
    {
        self.measurements
            .check_insert(i, &n)
            .map_ok_value(Cow::into_owned)
            .map_errors(InsertOpticalError::Insert)
            .nowarn_and_then(|ret| {
                self.layout
                    .insert_nocheck(i, r)
                    .map_err(InsertOpticalError::Layout)
                    .into_log()
                    .set_ok_value(ret)
            })
            .map_ok_value(|ret| {
                self.measurements.insert_nocheck(i, n, m);
                self.metaroot.specific.insert_meas_index_inner(i);
                ret
            })
    }

    fn set_named_measurements_inner(
        &mut self,
        measurements: NamedTemporalsAndOpticals<V>,
        allow_shared_names: bool,
        skip_index_check: bool,
    ) -> Result<(), SetNamedMeasurementsError>
    where
        V::Optical: AsScaleOrTransform,
        <V::Optical as AsScaleOrTransform>::S: CheckedScaleTransform,
        <<V::Optical as AsScaleOrTransform>::S as CheckedScaleTransform>::Summary: Default,
        ScaleDatatypeMismatchError: From<ScaleErrorGroup<V>>,
        L: LayoutDatatype + HasWidth,
    {
        let meas = self.layout.try_new_measurements::<V>(measurements)?;
        self.new_meas_link_errors(&meas, allow_shared_names, skip_index_check)?;
        self.measurements = meas;
        Ok(())
    }

    fn set_measurements_inner(
        &mut self,
        measurements: TemporalsAndOpticals<V>,
    ) -> Result<(), SetUnnamedMeasurementsError>
    where
        V::Optical: AsScaleOrTransform,
        <V::Optical as AsScaleOrTransform>::S: CheckedScaleTransform,
        <<V::Optical as AsScaleOrTransform>::S as CheckedScaleTransform>::Summary: Default,
        ScaleDatatypeMismatchError: From<ScaleErrorGroup<V>>,
        L: HasWidth + LayoutDatatype,
    {
        self.layout.check_meas_vec::<V>(&measurements[..])?;
        self.measurements.set_values(measurements)?;
        Ok(())
    }

    fn set_measurements_and_layout_inner(
        &mut self,
        measurements: TemporalsAndOpticals<V>,
        layout: L,
    ) -> Result<(), SetUnnamedMeasurementsError>
    where
        V::Optical: AsScaleOrTransform,
        <V::Optical as AsScaleOrTransform>::S: CheckedScaleTransform,
        <<V::Optical as AsScaleOrTransform>::S as CheckedScaleTransform>::Summary: Default,
        ScaleDatatypeMismatchError: From<ScaleErrorGroup<V>>,
        L: HasWidth + LayoutDatatype + LayoutNormalize,
    {
        layout.check_meas_vec::<V>(&measurements[..])?;
        self.measurements.set_values(measurements)?;
        self.set_layout_inner(layout);
        Ok(())
    }

    fn unset_measurements_inner(&mut self) -> Result<(), ExistingLinkErrors>
    where
        L: HasWidth,
    {
        let p = self.par();
        let (js, ns) = self.all_indices_and_names_to_remove();
        let es = self.metaroot.meas_has_existing_links_with(p, &ns, &js);
        ExistingLinkErrors::try_new(es)?;
        self.measurements = NamedVec::default();
        self.layout.clear();
        Ok(())
    }

    fn set_layout_inner(&mut self, mut layout: L)
    where
        L: LayoutNormalize,
    {
        layout.normalize();
        self.layout = layout;
    }

    fn header_and_flat_keywords<T>(
        &self,
        conf: &WriteHeaderAndTextConfig<'_>,
    ) -> Result<HeaderKeywordsToWrite<T>, WriteTEXTHeaderError>
    where
        L: LayoutKeywords + LayoutOptMeasKeywords,
        T: TryFrom<u64, Error = Uint8DigitOverflowError> + Copy + Zero + HeaderString + Into<u64>,
    {
        let req = self
            .req_root_keywords()
            .chain(once(ReqRootKeyword::from_value(conf.tot)))
            .map(ReqKeyword::from)
            .chain(self.req_meas_keywords().map(ReqKeyword::from));
        let opt = self
            .opt_root_keywords()
            .map(OptKeyword::from)
            .chain(self.opt_meas_keywords().map(OptKeyword::from));
        if V::as_version() == Version::FCS2_0 {
            let ks = req.map(AnyKeyword::from).chain(opt.map(AnyKeyword::from));
            HeaderKeywordsToWrite::new_2_0(ks, conf)
        } else {
            HeaderKeywordsToWrite::new_3_0(req, opt, conf)
        }
    }

    fn opt_meas_keywords(&self) -> impl Iterator<Item = StdOrNonStdOptMeasKeyword<'_>>
    where
        L: LayoutOptMeasKeywords,
    {
        let ns = (!V::Name::INFALLABLE)
            .then(|| {
                self.measurements
                    .indexed_opt_names()
                    .flatten()
                    .enumerate()
                    .map(|(i, v)| OptMeasKeyword::from_ref(v, i))
                    .map(StdOrNonStdOptMeasKeyword::from)
            })
            .into_iter()
            .flatten();
        let lv = self
            .layout
            .opt_meas_keywords()
            .into_iter()
            .flatten()
            .map(OptMeasKeyword::from)
            .map(StdOrNonStdOptMeasKeyword::from);
        self.measurements
            .iter_with(
                &|i, x| Temporal::all_opt_keywords(&x.value, i).collect::<Vec<_>>(),
                &|i, x| Optical::opt_and_nonstd_keywords(&x.value, i).collect(),
            )
            .flatten()
            .chain(ns)
            .chain(lv)
    }

    fn req_meas_keywords(&self) -> impl Iterator<Item = ReqMeasKeyword<'_>>
    where
        L: LayoutKeywords,
    {
        let ns = (V::Name::INFALLABLE)
            .then(|| {
                self.measurements
                    .indexed_opt_names()
                    .flatten()
                    .enumerate()
                    .map(|(i, v)| ReqMeasKeyword::from_ref(v, i))
            })
            .into_iter()
            .flatten();
        let lv = self.layout.req_meas_keywords().into_iter().flatten();
        self.measurements
            .iter_with(
                &|i, x| x.value.req_meas_keywords(i).into_iter().collect(),
                &|i, x| x.value.req_keywords(i).collect::<Vec<_>>(),
            )
            .flatten()
            .chain(ns)
            .chain(lv)
    }

    fn req_root_keywords(&self) -> impl Iterator<Item = ReqRootKeyword<'_>>
    where
        L: LayoutKeywords,
    {
        let lv = self.layout.req_keywords();
        Metaroot::req_keywords(&self.metaroot, self.par()).chain(lv)
    }

    fn opt_root_keywords(&self) -> impl Iterator<Item = StdOrNonStdOptRootKeyword<'_>> {
        self.metaroot.opt_and_nonstd_keywords()
    }

    #[cfg(feature = "serde")]
    #[allow(clippy::too_many_lines)]
    fn print_meas_table<'a, W: Write>(&'a self, w: &mut W, delim: u8) -> io::Result<()>
    where
        V::Temporal: Clone,
        V::Optical: OpticalFromTemporal<V::Temporal> + Clone,
        L: LayoutKeywords + LayoutOptMeasKeywords,
    {
        const INDEX: &str = "index";

        #[derive(From, Clone)]
        enum MeasKeyword<'a> {
            Index(MeasIndex),
            Req(ReqMeasKeyword<'a>),
            Optical(OptOpticalKeyword<'a>),
            NumType(SplitKeyword1<NumType>),
        }

        impl<'a> MeasKeyword<'a> {
            fn key(&'a self) -> String {
                match self {
                    MeasKeyword::Index(_) => INDEX.into(),
                    MeasKeyword::Req(x) => x.std_blank(),
                    MeasKeyword::Optical(x) => x.std_blank(),
                    MeasKeyword::NumType(x) => x.std_blank(),
                }
            }

            fn value(&'a self) -> String {
                match self {
                    MeasKeyword::Index(x) => x.to_string(),
                    MeasKeyword::Req(x) => x.as_str_pair().1.into(),
                    MeasKeyword::Optical(x) => x.as_str_pair().1.into(),
                    MeasKeyword::NumType(x) => x.as_str_pair().1.into(),
                }
            }

            fn assign(self, header: &[String], row: &mut [Option<String>]) {
                let key = self.key();
                if let Some(i) = header.iter().position(|x| x == &key) {
                    row[i] = Some(self.value());
                }
            }
        }

        let mut header = vec![];

        let version = V::as_version();

        let common = [
            INDEX.into(),
            Shortname::std_blank(),
            Width::std_blank(),
            TextRange::std_blank(),
            Scale::std_blank(),
            Filter::std_blank(),
            // NOTE same for Wavelengths
            Wavelength::std_blank(),
            Power::std_blank(),
            DetectorType::std_blank(),
            PercentEmitted::std_blank(),
            DetectorVoltage::std_blank(),
        ];

        let peak = [PeakBin::std_blank(), PeakIndex::std_blank()];

        header.extend(common);

        match version {
            Version::FCS2_0 => {
                header.extend(peak);
            }
            Version::FCS3_0 => {
                header.push(Gain::std_blank());
                header.extend(peak);
            }
            Version::FCS3_1 => {
                header.push(Gain::std_blank());
                header.push(Calibration3_1::std_blank());
                header.push(Display::std_blank());
                header.extend(peak);
            }
            Version::FCS3_2 => {
                header.push(Gain::std_blank());
                header.push(Calibration3_2::std_blank());
                header.push(Display::std_blank());
                header.push(DetectorName::std_blank());
                header.push(Tag::std_blank());
                header.push(OpticalType::std_blank());
                header.push(Feature::std_blank());
                header.push(Analyte::std_blank());
                header.push(NumType::std_blank());
            }
        }

        let shortname = |n: Option<&'a Shortname>, index: MeasIndex| {
            n.map(|v| RefKeyword1::from_ref1(v, index))
                .map(ReqMeasKeyword::from)
                .map(MeasKeyword::from)
        };

        // Convert all measurements to optical, which should be fine since
        // optical keywords is a superset of temporal keywords (sans $TIMESTEP
        // which won't be shown here
        let ms: Vec<_> = self
            .measurements
            .iter()
            .map(|m| {
                m.both(
                    |t| {
                        let (o, _) = V::Optical::from_temporal_unchecked(t.value.clone());
                        (Some(&t.key), o)
                    },
                    |o| (V::Name::as_opt(&o.key), o.value.clone()),
                )
            })
            .collect();

        let lt = &self.layout;
        let req_layout = lt.req_meas_keywords();
        let opt_layout = lt.opt_meas_keywords();

        debug_assert!(
            req_layout.len() == opt_layout.len(),
            "layout lengths not the same"
        );

        debug_assert!(
            ms.len() == req_layout.len(),
            "measurement length not equal to layout length"
        );

        let ls = req_layout.into_iter().zip(opt_layout);

        if let Some(ne) = ms.iter().zip(ls).enumerate().try_into_nonempty_iter() {
            let mut first = true;

            for s in &header {
                if !first {
                    w.write_all(&[delim])?;
                }
                first = false;
                write!(w, "{s}")?;
            }
            writeln!(w)?;

            for (i, ((n, m), (req_l, opt_l))) in ne {
                let mut row = vec![None; header.len()];
                let j = MeasIndex::from(i);
                let xs = once(MeasKeyword::from(j))
                    .chain(shortname(*n, j))
                    .chain(req_l.map(MeasKeyword::from))
                    .chain(opt_l.fmap(MeasKeyword::from))
                    .chain(m.req_keywords(j).map(MeasKeyword::from))
                    .chain(m.opt_keywords(j).map(MeasKeyword::from));
                for x in xs {
                    x.assign(&header[..], &mut row);
                }
                first = true;
                for r in &row {
                    if !first {
                        w.write_all(&[delim])?;
                    }
                    if let Some(x) = r {
                        write!(w, "{x}")?;
                    } else {
                        write!(w, "NA")?;
                    }
                    first = false;
                }
                writeln!(w)?;
            }
        }
        Ok(())
    }

    fn split_nonstandard(
        par: Par,
        nonstd: &mut NonStdKeywords,
        conf: &ReadStdKeywordsConfig,
    ) -> Success<Vec<NonStdKeywords>, (), Option<NonStdMeasRegexError>> {
        let mut meas_targets = vec![HashMap::new(); par.0];
        let compiled = if let Some(ns_pat) = conf.nonstandard_measurement_pattern.0.as_ref() {
            match ns_pat.compile() {
                Ok(x) => x,
                Err(e) => {
                    let ret = Success::new_non_switchable(meas_targets);
                    return ret.set_warnings(Some(e));
                }
            }
        } else {
            return Success::new_non_switchable(meas_targets);
        };

        let sorted = nonstd
            .drain()
            .map(|(k, v)| {
                let i = compiled
                    .get_index(&k)
                    .map(usize::from)
                    .and_then(|i| (i < par.0).then_some(i));
                (k, v, i)
            })
            .sorted_by_key(|x| x.2);
        for (k, v, i) in sorted {
            if let Some(j) = i {
                meas_targets[j].insert(k, v);
            } else {
                nonstd.insert(k, v);
            }
        }
        Success::new_non_switchable(meas_targets)
    }

    #[allow(clippy::type_complexity)]
    fn lookup_names<C>(
        std: &mut StdKeywords,
        nonstd: &mut [NonStdKeywords],
        conf: &C,
    ) -> WarningsAndErrorsResult<
        (Vec<V::Name>, Vec<Option<Shortname>>),
        (),
        OptIndexedKeyError<Shortname>,
        LookupShortnameError,
    >
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadStdKeywordsConfig>,
        V::Metaroot: LookupMetaroot<V::Name>,
        V::Name: LookupShortname,
    {
        nonstd
            .iter_mut()
            .enumerate()
            .map(|(n, meas_nonstd)| {
                let i = n.into();
                V::Name::lookup_shortname(std, meas_nonstd, i, conf.as_ref()).into_semigroup()
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

    fn lookup_measurements<C>(
        std: &mut StdKeywords,
        names: Vec<V::Name>,
        nonstd: Vec<NonStdKeywords>,
        dts: &[AlphaNumType],
        conf: &C,
    ) -> LookupMeasurementResult<(NamedTemporalsAndOpticals<V>, MeasurementDiagnostics)>
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadStdKeywordsConfig>,
        V::Metaroot: LookupMetaroot<V::Name>,
        V::Temporal: LookupTemporal,
        V::Optical: LookupOptical,
        V::Name: Pointed<Shortname>,
    {
        let sconf: &ReadStdKeywordsConfig = conf.as_ref();
        let mut found_time = false;

        let mut match_time_pattern = |i, wrapped| {
            let res = match V::Name::unwrap(wrapped) {
                Ok(name) => {
                    if let Some(tp) = sconf.time_meas_pattern.0.as_ref()
                        && tp.is_match(name.as_ref())
                    {
                        if found_time {
                            let e = DuplicateTimeNameError(i, name);
                            Err(LookupMeasurementError::from(e))
                        } else {
                            found_time = true;
                            Ok(Element::Center(name))
                        }
                    } else {
                        Ok(Element::NonCenter(V::Name::wrap(name)))
                    }
                }
                Err(key) => Ok(Element::NonCenter(key)),
            };
            res.into_log()
        };

        debug_assert!(
            names.len() == dts.len(),
            "datatypes and names must be equal length"
        );

        names
            .into_iter()
            .zip(nonstd)
            .zip(dts)
            .enumerate()
            .map(|(i, ((wrapped, meas_nonstd), dt))| {
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
                                .map_ok_value(|x| Element::Center((name, x)))
                        }
                        Element::NonCenter(k) => {
                            Optical::lookup_optical(std, j, meas_nonstd, *dt, conf)
                                .map_errors(LookupMeasurementError::from)
                                .map_commutative_warnings(LookupMeasurementWarning::from)
                                .map_ok_value(|x| Element::NonCenter((k, x)))
                        }
                    })
            })
            .sequence_commutative()
            .map_ok_value(|xs| {
                let mut ms = vec![];
                let mut ds = vec![];
                let mut trimmed = vec![];
                let mut tops = vec![];
                let mut timestep_added = false;
                for x in xs {
                    match x {
                        Element::Center((name, y)) => {
                            ms.push(Element::Center((name, y.this)));
                            ds.push(y.scale.into());
                            trimmed.extend(y.trimmed);
                            tops.extend(y.tmp_opt_pairs);
                            timestep_added = timestep_added || y.timestep_added;
                        }
                        Element::NonCenter((name, y)) => {
                            ms.push(Element::NonCenter((name, y.this)));
                            ds.push(y.scale.into());
                            trimmed.extend(y.trimmed);
                        }
                    }
                }
                let d = MeasurementDiagnostics::new(ds, trimmed, tops, timestep_added);
                (ms, d)
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
        measurements: &Measurements<V::Name, X, Y>,
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

impl<V: VersionSet> VersionedCoreTEXT<V> {
    #[allow(clippy::type_complexity)]
    pub(crate) fn new_from_keywords_with_offsets<C>(
        mut kws: ValidKeywords,
        segs: &mut HeaderAndSuppOffsets,
        st: &ReadState<C>,
    ) -> WarningsAndErrorsResult<
        (Self, StdTEXTDiagnostics, MetarootTEXTOffsets<V>),
        (),
        StdTEXTFromFlatTEXTWarning,
        StdTEXTFromFlatTEXTErrorInner,
    >
    where
        V::Metaroot: LookupMetaroot<V::Name>,
        V::Temporal: LookupTemporal,
        V::Optical: LookupOptical + AsScaleOrTransform,
        V::Name: LookupShortname,
        V::DataSchema: VersionedDataSchema,
        C: AsRef<ReadStdKeywordsConfig> + AsRef<ReadDataKeywordsConfig> + AsRef<ReadOffsetConfig>,
        <V::Optical as AsScaleOrTransform>::S: CheckedScaleTransform,
        <<V::Optical as AsScaleOrTransform>::S as CheckedScaleTransform>::Summary: Default,
        ScaleDatatypeMismatchError: From<ScaleErrorGroup<V>>,
    {
        // Lookup DATA/ANALYSIS offsets and $TOT; these are not stored in the
        // Core struct but they will be needed later for parsing DATA and
        // ANALYSIS, and processing these keywords now will make it easier to
        // determine if TEXT is totally standardized or not.
        let offsets_res = V::Offsets::lookup(&mut kws.std, &mut kws.nonstd, segs, st)
            .map_commutative_warnings(StdTEXTFromFlatTEXTWarning::from)
            .map_errors(StdTEXTFromFlatTEXTErrorInner::from);

        Self::lookup_inner(kws, &st.conf)
            .zip_commutative(offsets_res)
            .map_ok_value(|((x, y), z)| (x, y, z))
    }

    /// Make a new CoreTEXT from flat keywords.
    ///
    /// Return any errors encountered, including missing required keywords and
    /// parse errors.
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
        V::Metaroot: LookupMetaroot<V::Name>,
        V::Temporal: LookupTemporal,
        V::Optical: LookupOptical + AsScaleOrTransform,
        V::Name: LookupShortname,
        V::DataSchema: VersionedDataSchema,
        C: AsRef<ReadStdKeywordsConfig> + AsRef<ReadDataKeywordsConfig> + AsRef<ReadSharedConfig>,
        <V::Optical as AsScaleOrTransform>::S: CheckedScaleTransform,
        <<V::Optical as AsScaleOrTransform>::S as CheckedScaleTransform>::Summary: Default,
        ScaleDatatypeMismatchError: From<ScaleErrorGroup<V>>,
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
        V::Metaroot: LookupMetaroot<V::Name>,
        V::Temporal: LookupTemporal,
        V::Optical: LookupOptical + AsScaleOrTransform,
        V::Name: LookupShortname,
        V::DataSchema: VersionedDataSchema,
        C: AsRef<ReadStdKeywordsConfig> + AsRef<ReadDataKeywordsConfig>,
        <V::Optical as AsScaleOrTransform>::S: CheckedScaleTransform,
        <<V::Optical as AsScaleOrTransform>::S as CheckedScaleTransform>::Summary: Default,
        ScaleDatatypeMismatchError: From<ScaleErrorGroup<V>>,
    {
        // Lookup $PAR first since we need this to get the measurements
        let par_res = Par::remove_metaroot_req(&mut kws.std)
            .map_err(LookupMetarootError::from)
            .map_err(StdTEXTFromFlatTEXTErrorInner::from)
            .into_log();

        let version = V::as_version();
        let sconf: &ReadStdKeywordsConfig = conf.as_ref();

        macro_rules! go_err {
            ($x:expr) => {
                $x.map_commutative_warnings(StdTEXTFromFlatTEXTWarning::from)
                    .map_errors(StdTEXTFromFlatTEXTErrorInner::from)
            };
        }

        par_res.and_then_commutative(|par| {
            let std = &mut kws.std;
            let nonstd = &mut kws.nonstd;
            // Split nonstandard measurements using pattern (if given); this
            // implicitly will encode $PAR downstream via length
            let nonstd_succ = Self::split_nonstandard(par, nonstd, conf.as_ref());
            let mut core_res = WarningsAndErrorsResult::Succ(nonstd_succ.repack())
                .map_commutative_warnings(StdTEXTFromFlatTEXTWarning::from)
                // Lookup $PnN and data schema (which are independent of each other)
                .and_then_commutative(|mut meas_nonstd| {
                    let ret = Self::lookup_names(std, &mut meas_nonstd[..], conf)
                        .map_ok_value(|n| (n, meas_nonstd));
                    go_err!(ret)
                })
                // Lookup root (which depends on $PnN) and data schema
                .and_then_commutative(|((dedup_names, original_names), mut meas_nonstd)| {
                    let mnsks = &mut meas_nonstd[..];
                    let layout_res = V::DataSchema::lookup(std, mnsks, conf.as_ref());

                    let root_res =
                        Metaroot::lookup_metaroot(std, &dedup_names[..], kws.nonstd, conf);

                    go_err!(root_res)
                        .zip_commutative(go_err!(layout_res))
                        .map_ok_value(|x| (x, meas_nonstd, dedup_names, original_names))
                })
                // Lookup measure which depends on global datatype
                .and_then_commutative(
                    |((metaroot_out, layout_out), meas_nonstd, dedup_names, original_names)| {
                        let dts = &layout_out.data_schema.datatypes()[..];
                        let ret =
                            Self::lookup_measurements(std, dedup_names, meas_nonstd, dts, conf);
                        go_err!(ret).map_ok_value(|x| (metaroot_out, layout_out, x, original_names))
                    },
                )
                .and_then_commutative(
                    |(metaroot_out, layout_out, (meas, mut meas_diag), original_names)| {
                        meas_diag.trimmed.extend(metaroot_out.trimmed);
                        let fixes = metaroot_out.fixed_gate_scales;
                        let ret =
                            Self::try_new(metaroot_out.this, meas, layout_out.data_schema, conf)
                                .map_ok_value(|ret| (ret, original_names, fixes, meas_diag));
                        go_err!(ret)
                    },
                );

            let gate = core_res
                .as_ref()
                .and_then(|(core, _, _, _)| core.metaroot.specific.gate())
                .unwrap_or(Gate::from(0));

            // Push pseudostandard/unused warnings/errors
            let (mut extra, errors) = ExtraStdKeywords::split_keywords(kws.std, version, par, gate);

            let flag = sconf.process_extra_timestep;
            core_res = core_res
                .extend_warnings_or_errors3(
                    // Check this first because we might take the timestamp out
                    // of this slot below to demote it
                    extra.timestep.is_some().then_some(TimestepFoundError),
                    |_v| (),
                    StdTEXTFromFlatTEXTWarning::from,
                    StdTEXTFromFlatTEXTErrorInner::from,
                    flag.as_triflag(),
                )
                .map_ok_value(|mut core| {
                    if flag.is_demote()
                        && let Some(t) = mem::take(&mut extra.timestep)
                    {
                        core.0
                            .metaroot
                            .nonstandard_keywords
                            .insert_demoted(Timestep::std(), t);
                    }
                    core
                });

            macro_rules! go_extra {
                ($proc:ident, $keyvals:ident, $errors:ident) => {
                    let flag = sconf.$proc;
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

            core_res.map_ok_value(|(ret, original_names, fixes, diag)| {
                let d = StdTEXTDiagnostics::from_extra(extra, original_names, fixes, diag);
                (ret, d)
            })
        })
    }

    /// Get reference to data schema
    pub fn data_schema(&self) -> &V::DataSchema {
        &self.layout
    }

    /// Set data schema.
    ///
    /// Will return error if data schema does not have same number of columns as
    /// measurements.
    pub fn set_data_schema(
        &mut self,
        data_schema: V::DataSchema,
    ) -> Result<(), MeasLayoutMismatchError>
    where
        V::Optical: AsScaleOrTransform,
        <V::Optical as AsScaleOrTransform>::S: CheckedScaleTransform,
        <<V::Optical as AsScaleOrTransform>::S as CheckedScaleTransform>::Summary: Default,
        ScaleDatatypeMismatchError: From<ScaleErrorGroup<V>>,
    {
        data_schema.check_meas_named_vec(&self.measurements)?;
        self.set_layout_inner(data_schema);
        Ok(())
    }

    /// Set measurements without $PnN and data schema
    pub fn set_measurements_and_data_schema(
        &mut self,
        measurements: TemporalsAndOpticals<V>,
        data_schema: V::DataSchema,
    ) -> Result<(), SetUnnamedMeasurementsError>
    where
        V::Optical: AsScaleOrTransform,
        <V::Optical as AsScaleOrTransform>::S: CheckedScaleTransform,
        <<V::Optical as AsScaleOrTransform>::S as CheckedScaleTransform>::Summary: Default,
        ScaleDatatypeMismatchError: From<ScaleErrorGroup<V>>,
        V::DataSchema: HasWidth + LayoutDatatype + LayoutNormalize,
    {
        self.set_measurements_and_layout_inner(measurements, data_schema)
    }

    /// Set measurements and data schema
    ///
    /// Return error if measurement names are not unique, there is more than one
    /// time measurement, or the data schema and measurements have different
    /// lengths.
    pub fn set_named_measurements_and_data_schema(
        &mut self,
        measurements: NamedTemporalsAndOpticals<V>,
        data_schema: V::DataSchema,
        allow_shared_names: bool,
        skip_index_check: bool,
    ) -> Result<(), SetNamedMeasurementsError>
    where
        V::Optical: AsScaleOrTransform,
        <V::Optical as AsScaleOrTransform>::S: CheckedScaleTransform,
        <<V::Optical as AsScaleOrTransform>::S as CheckedScaleTransform>::Summary: Default,
        ScaleDatatypeMismatchError: From<ScaleErrorGroup<V>>,
    {
        let meas = data_schema.try_new_measurements::<V>(measurements)?;
        self.new_meas_link_errors(&meas, allow_shared_names, skip_index_check)?;
        self.measurements = meas;
        self.set_layout_inner(data_schema);
        Ok(())
    }

    /// Remove a measurement matching the given name.
    ///
    /// Return removed measurement and its index if found.
    pub fn remove_measurement_by_name<R>(
        &mut self,
        n: &Shortname,
    ) -> Result<(MeasIndex, TemporalOrOptical<V>, R), RemoveMeasByNameError>
    where
        V::DataSchema: LayoutRemove<R>,
    {
        self.remove_measurement_by_name_inner(n)
    }

    /// Remove a measurement at a given position
    ///
    /// Return removed measurement and its name if found.
    pub fn remove_measurement_by_index<R>(
        &mut self,
        index: MeasIndex,
    ) -> Result<(NamedTemporalOrOptical<V>, R), RemoveMeasByIndexError>
    where
        V::DataSchema: LayoutRemove<R>,
    {
        self.remove_measurement_by_index_inner(index)
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
        df: PrimitiveDataFrame,
        analysis: Analysis,
        others: Others,
    ) -> Result<VersionedCoreDataset<V>, DataSchemaToDataFrameError>
    where
        V::DataSchema: WithPrimitiveDataFrame<DfTarget = V::DataFrame>,
    {
        let typed_df = self.layout.with_data(df)?;
        let new = Core::new(self.metaroot, self.measurements, typed_df, analysis, others);
        Ok(new)
    }

    // only meant to be called during lookup when keywords are being read from
    // a hashtable
    pub(crate) fn try_new<C>(
        mut metaroot: Metaroot<V::Metaroot>,
        measurements: NamedTemporalsAndOpticals<V>,
        data_schema: V::DataSchema,
        conf: &C,
    ) -> WarningsAndErrorsResult<Self, (), NewCoreWarning, LookupCoreError>
    where
        V::DataSchema: HasWidth,
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadStdKeywordsConfig>,
        V::Optical: AsScaleOrTransform,
        <V::Optical as AsScaleOrTransform>::S: CheckedScaleTransform,
        <<V::Optical as AsScaleOrTransform>::S as CheckedScaleTransform>::Summary: Default,
        ScaleDatatypeMismatchError: From<ScaleErrorGroup<V>>,
    {
        // this should be true since the length of both is derived from $PAR
        debug_assert!(
            measurements.len() == data_schema.width(),
            "measurements and data schema should be same length"
        );
        let rconf: &ReadDataKeywordsConfig = conf.as_ref();
        let sconf: &ReadStdKeywordsConfig = conf.as_ref();

        let go = |ms: &NamedVec<_, _, _>| {
            if let Some(pat) = sconf.time_meas_pattern.0.as_ref()
                && ms.as_center().is_none()
                && !ms.is_empty()
            {
                return Some(NewCoreWarning::from(MissingTime(pat.clone())));
            }
            None
        };

        let opt_flag = rconf.process_optional_failure;
        let missing_flag = sconf.allow_missing_time;
        Measurements::try_new(measurements)
            .map_err(LookupCoreError::from)
            .into_log()
            .eval_warning_or_error3(missing_flag, |_| (), |()| (), go)
            .and_then_commutative(|ms| {
                data_schema
                    .check_measurement_vector_nolen(&ms)
                    .map_err(LookupCoreError::from)
                    .into_log()
                    .set_ok_value(ms)
            })
            .and_then_commutative(|ms| {
                Self::check_relationships(&mut metaroot, &ms, opt_flag.is_demote())
                    .map_errors(NewCoreWarning::from)
                    .nowarn_into_switchable(opt_flag)
                    .switchable_into_commutative()
                    .map_errors(LookupCoreError::from)
                    .map_commutative_warnings(NewCoreWarning::from)
                    .map_ok_value(|()| Self::new(metaroot, ms, data_schema, (), ()))
            })
    }

    pub(crate) fn try_new_nodrop(
        mut metaroot: Metaroot<V::Metaroot>,
        measurements: NamedTemporalsAndOpticals<V>,
        data_schema: V::DataSchema,
    ) -> ErrorsResult<Self, (), NewCoreError>
    where
        V::Optical: AsScaleOrTransform,
        <V::Optical as AsScaleOrTransform>::S: CheckedScaleTransform,
        <<V::Optical as AsScaleOrTransform>::S as CheckedScaleTransform>::Summary: Default,
        ScaleDatatypeMismatchError: From<ScaleErrorGroup<V>>,
    {
        Measurements::try_new(measurements)
            .map_err(NewCoreError::from)
            .into_nowarn()
            .and_then_commutative(|ms| {
                data_schema
                    .check_meas_named_vec(&ms)
                    .map_err(NewCoreError::from)
                    .into_nowarn()
                    .set_ok_value(ms)
            })
            .and_then_commutative(|ms| {
                Self::check_relationships(&mut metaroot, &ms, false)
                    .map_errors(NewCoreWarning::from)
                    .map_errors(NewCoreError::from)
                    .map_ok_value(|()| Self::new(metaroot, ms, data_schema, (), ()))
            })
    }

    /// Check for invalid keyword relationships.
    ///
    /// For example, $SPILLOVER in the metaroot must refer to valid
    /// measurements.
    ///
    /// If allow_dropping is true, remove keywords with invalid relationships.
    fn check_relationships(
        metaroot: &mut Metaroot<V::Metaroot>,
        measurements: &Measurements<V::Name, V::Temporal, V::Optical>,
        demote: bool,
    ) -> ErrorsResult<(), (), BrokenOrDependentLinkError>
    where
        V::Optical: AsScaleOrTransform,
    {
        let ns = measurements.named_set();
        let par = Par(measurements.len());
        let link_errs = metaroot.remove_invalid_links(par, &ns, demote);
        LogResult::new_from_err_iter(link_errs, (), ())
    }

    fn new_unchecked(
        metaroot: Metaroot<V::Metaroot>,
        measurements: Measurements<V::Name, V::Temporal, V::Optical>,
        data_schema: V::DataSchema,
    ) -> Self {
        Self::new(metaroot, measurements, data_schema, (), ())
    }
}

impl<V: VersionSet> VersionedCoreDataset<V> {
    pub fn new_from_keywords<C>(
        p: &PathBuf,
        mut hns: HeaderAndSuppOffsets,
        kws: ValidKeywords,
        dataset_offset: DatasetOffset,
        conf: &C,
    ) -> WarningsAndIOGroupResult<
        (Self, NewStdDatasetFromKwsOutput),
        StdDatasetFromFlatTEXTWarning,
        StdDatasetFromFlatTextError,
        StdDatasetWithKwsSummary,
    >
    where
        V::Metaroot: LookupMetaroot<V::Name>,
        V::Temporal: LookupTemporal,
        V::Optical: LookupOptical + AsScaleOrTransform,
        V::Name: LookupShortname,
        V::DataSchema: DataSchemaToEmptyDataFrame<DfTarget = V::DataFrame>,
        C: AsRef<ReadStdKeywordsConfig>
            + AsRef<ReadOffsetConfig>
            + AsRef<ReadDataKeywordsConfig>
            + AsRef<ReadEventsConfig>
            + AsRef<ReadSharedConfig>,
        <V::Optical as AsScaleOrTransform>::S: CheckedScaleTransform,
        <<V::Optical as AsScaleOrTransform>::S as CheckedScaleTransform>::Summary: Default,
        ScaleDatatypeMismatchError: From<ScaleErrorGroup<V>>,
    {
        ReadState::open(p, dataset_offset, conf)
            .map_err(|e| e.fmap_once(StdDatasetFromFlatTextErrorInner::from))
            .map_err(IOErrorGroup::from)
            .into_log()
            .and_then_commutative(|(st, file)| {
                let mut h = BufReader::new(file);
                Self::new_from_keywords_inner(&mut h, kws, &mut hns, &st)
            })
            .map_ok_value(|(ret, dataset)| {
                let out = NewStdDatasetFromKwsOutput::new(dataset, hns.header.segments);
                (ret, out)
            })
            .warnings_to_pure_errors(*conf.as_ref(), StdDatasetFromFlatTextErrorInner::from)
            .map_pure_errors(StdDatasetFromFlatTextError::from)
            .deanonymize()
    }

    pub(crate) fn new_from_keywords_inner<C, R>(
        h: &mut BufReader<R>,
        kws: ValidKeywords,
        hns: &mut HeaderAndSuppOffsets,
        st: &ReadState<C>,
    ) -> WarningsAndIOGroupResult<
        (Self, StdDatasetFromKwsOutput),
        StdDatasetFromFlatTEXTWarning,
        StdDatasetFromFlatTextErrorInner,
        (),
    >
    where
        R: Read + Seek,
        V::Metaroot: LookupMetaroot<V::Name>,
        V::Temporal: LookupTemporal,
        V::Optical: LookupOptical + AsScaleOrTransform,
        V::Name: LookupShortname,
        V::DataSchema: DataSchemaToEmptyDataFrame<DfTarget = V::DataFrame>,
        C: AsRef<ReadStdKeywordsConfig>
            + AsRef<ReadOffsetConfig>
            + AsRef<ReadDataKeywordsConfig>
            + AsRef<ReadEventsConfig>,
        <V::Optical as AsScaleOrTransform>::S: CheckedScaleTransform,
        <<V::Optical as AsScaleOrTransform>::S as CheckedScaleTransform>::Summary: Default,
        ScaleDatatypeMismatchError: From<ScaleErrorGroup<V>>,
    {
        VersionedCoreTEXT::<V>::new_from_keywords_with_offsets(kws, hns, st)
            .map_commutative_warnings(StdDatasetFromFlatTEXTWarning::from)
            .map_errors(StdDatasetFromFlatTextErrorInner::from)
            .group()
            .map_error(IOErrorGroup::Pure)
            .and_then_commutative(|(mut text, extra, mut offsets)| {
                let or = hns.header.segments.others_reader();
                let ar = AnalysisReader::new(offsets.segs.analysis);
                text.layout
                    .h_read_df(h, offsets.tot, &mut offsets.segs.data, st.conf.as_ref())
                    .map_commutative_warnings(StdDatasetFromFlatTEXTWarning::from)
                    .map_pure_errors(StdDatasetFromFlatTextErrorInner::from)
                    .and_then_commutative(|df_out| {
                        ar.h_read(h)
                            .and_then(|analysis| {
                                let others = or.h_read(h)?;
                                let new = Self::new(
                                    text.metaroot,
                                    text.measurements,
                                    df_out.dataframe,
                                    analysis,
                                    others,
                                );
                                let out = StdDatasetFromKwsOutput::new(
                                    offsets.segs,
                                    extra,
                                    df_out.diagnostics,
                                );
                                Ok((new, out))
                            })
                            .map_err(IOErrorGroup::from)
                            .into_log()
                    })
            })
    }

    /// Write this core structure (HEADER+TEXT) to a file path
    pub fn write_dataset(
        &self,
        path: &PathBuf,
        conf: &WriteMultiDatasetConfig,
    ) -> WarningsAndIOGroupResult<Nextdata, EventOverRangeError, StdWriterError, WriteDatasetSummary>
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
    ) -> WarningsAndIOGroupResult<Nextdata, EventOverRangeError, StdWriterError, WriteDatasetSummary>
    {
        let delim = conf.text.delim;
        let tot = Tot(self.layout.nrows());
        let analysis_len = self.analysis.0.len().usize_to_u64();
        let others = &self.others.0[..];

        self.layout
            .check_ranges(conf.checked_range_datatypes, conf.disallow_over_range)
            .map_errors(StdWriterError::from)
            .group()
            .map_error(IOErrorGroup::Pure)
            // write HEADER+TEXT+OTHER(s) first
            .and_then_commutative(|()| {
                let data_len = self.layout.nbytes();
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
            // write DATA and ANALYSIS
            .and_commutative(|| {
                io_to_log!(self.layout.h_write_df(h, conf));
                io_to_log!(h.write_all(&self.analysis.0));
                LogResult::new_ok(())
            })
            .deanonymize()
    }

    /// Return reference to DATA segment as dataframe.
    pub fn data(&self) -> PrimitiveDataFrame
    where
        V::DataFrame: Clone + Into<PrimitiveDataFrame>,
    {
        self.layout.clone().into()
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
    pub fn set_data(&mut self, df: PrimitiveDataFrame) -> Result<(), DataSchemaToDataFrameError>
    where
        V::DataFrame: WithPrimitiveDataFrame<DfTarget = V::DataFrame>,
    {
        self.layout = self.layout.with_data(df)?;
        Ok(())
    }

    /// Remove all measurements and data
    pub fn unset_data(&mut self) -> Result<(), ExistingLinkErrors> {
        self.unset_measurements_inner()?;
        self.layout.clear();
        Ok(())
    }

    /// Check that all events are within $PnR.
    ///
    /// `check_event_ranges` can be used to control which datatypes are checked.
    /// By default, only integers are checked.
    ///
    /// If `truncate` is `true`, truncate events in place if they exceed $PnR.
    pub fn check_ranges(
        &mut self,
        check_range_datatypes: CheckedRangeDatatypes,
        over_range_action: OverRangeAction,
    ) -> WarningsAndGroupResult<
        Vec<Option<usize>>,
        EventOverRangeError,
        EventOverRangeError,
        EventOverRangeSummary,
    > {
        self.layout
            .check_ranges_mut(check_range_datatypes, over_range_action)
            .group()
            .map_ok_value(|rs| rs.fmap(|x| x.map(|(i, _)| i)))
    }

    /// Get data schema.
    pub fn data_schema(&self) -> V::DataSchema
    where
        V::DataFrame: DataFrameAsDataSchema<DataSchema = V::DataSchema>,
    {
        self.layout.as_data_schema()
    }

    /// Set data schema.
    ///
    /// Will return error if data schema does not have same number of columns as
    /// measurements.
    // pass by value here to keep api consistent b/t coretext and coredataset
    #[allow(clippy::needless_pass_by_value)]
    pub fn set_data_schema(
        &mut self,
        data_schema: V::DataSchema,
    ) -> Result<(), DatasetSetDataSchemaError>
    where
        V::Optical: AsScaleOrTransform,
        <V::Optical as AsScaleOrTransform>::S: CheckedScaleTransform,
        <<V::Optical as AsScaleOrTransform>::S as CheckedScaleTransform>::Summary: Default,
        ScaleDatatypeMismatchError: From<ScaleErrorGroup<V>>,
        V::DataFrame: Clone + Into<PrimitiveDataFrame> + Default,
        V::DataSchema: WithPrimitiveDataFrame<DfTarget = V::DataFrame>,
    {
        data_schema.check_meas_named_vec(&self.measurements)?;
        data_schema.check_data_loss_generic(&self.layout)?;
        let new_data_schema = data_schema
            .with_data_generic(mem::take(&mut self.layout))
            .expect("data loss and dimensions were checked above");
        self.set_layout_inner(new_data_schema);
        Ok(())
    }

    /// Set measurements without $PnN and data_schema
    #[allow(clippy::needless_pass_by_value)]
    // pass by value here to keep api consistent b/t coretext and coredataset
    pub fn set_measurements_and_data_schema(
        &mut self,
        measurements: TemporalsAndOpticals<V>,
        data_schema: V::DataSchema,
    ) -> Result<(), DatasetSetUnnamedMeasAndDataSchemaError>
    where
        V::Optical: AsScaleOrTransform,
        <V::Optical as AsScaleOrTransform>::S: CheckedScaleTransform,
        <<V::Optical as AsScaleOrTransform>::S as CheckedScaleTransform>::Summary: Default,
        ScaleDatatypeMismatchError: From<ScaleErrorGroup<V>>,
        V::DataFrame: Clone + Into<PrimitiveDataFrame> + Default,
        V::DataSchema: WithPrimitiveDataFrame<DfTarget = V::DataFrame>,
    {
        // ensures length of new data schema == length of new measurements
        data_schema
            .check_meas_vec::<V>(&measurements[..])
            .map_err(SetUnnamedMeasurementsError::from)?;
        // length is not checked here, just data loss
        data_schema.check_data_loss_generic(&self.layout)?;
        // ensures length of new measurements == length of old measurements
        self.measurements
            .set_values(measurements)
            .map_err(SetUnnamedMeasurementsError::from)?;
        let new_data_schema = data_schema
            .with_data(mem::take(&mut self.layout).into())
            .expect("data loss and dimensions were checked above");
        self.set_layout_inner(new_data_schema);
        Ok(())
    }

    /// Set measurements and data schema
    ///
    /// Return error if measurement names are not unique, there is more than one
    /// time measurement, or the data schema and measurements have different
    /// lengths.
    // pass by value here to keep api consistent b/t coretext and coredataset
    #[allow(clippy::needless_pass_by_value)]
    pub fn set_named_measurements_and_data_schema(
        &mut self,
        measurements: NamedTemporalsAndOpticals<V>,
        data_schema: V::DataSchema,
        allow_shared_names: bool,
        skip_index_check: bool,
    ) -> Result<(), DatasetSetNamedMeasAndDataSchemaError>
    where
        V::Optical: AsScaleOrTransform,
        <V::Optical as AsScaleOrTransform>::S: CheckedScaleTransform,
        <<V::Optical as AsScaleOrTransform>::S as CheckedScaleTransform>::Summary: Default,
        ScaleDatatypeMismatchError: From<ScaleErrorGroup<V>>,
        V::DataFrame: Clone + Into<PrimitiveDataFrame> + Default,
        V::DataSchema: WithPrimitiveDataFrame<DfTarget = V::DataFrame>,
    {
        let meas = data_schema
            .try_new_measurements::<V>(measurements)
            .map_err(SetNamedMeasurementsError::from)?;
        self.new_meas_link_errors(&meas, allow_shared_names, skip_index_check)
            .map_err(SetNamedMeasurementsError::from)?;
        self.set_data_schema(data_schema)?;
        self.measurements = meas;
        Ok(())
    }

    /// Remove a measurement matching the given name.
    ///
    /// Return removed measurement and its index if found.
    pub fn remove_measurement_by_name<R>(
        &mut self,
        n: &Shortname,
    ) -> Result<(MeasIndex, TemporalOrOptical<V>, AnyPrimitiveSeries, R), RemoveMeasByNameError>
    where
        V::DataFrame: LayoutRemove<RangeAndSeries<R>>,
    {
        let (index, meas, (rng, col)) = self.remove_measurement_by_name_inner(n)?;
        Ok((index, meas, col, rng))
    }

    /// Remove a measurement at a given position
    ///
    /// Return removed measurement and its name if found.
    pub fn remove_measurement_by_index<R>(
        &mut self,
        index: MeasIndex,
    ) -> Result<(NamedTemporalOrOptical<V>, AnyPrimitiveSeries, R), RemoveMeasByIndexError>
    where
        V::DataFrame: LayoutRemove<RangeAndSeries<R>>,
    {
        let (meas, (rng, col)) = self.remove_measurement_by_index_inner(index)?;
        Ok((meas, col, rng))
    }

    /// Convert this struct into [`CoreTEXT`].
    ///
    /// This simply entails taking ownership and dropping the ANALYSIS and DATA
    /// fields.
    pub fn into_coretext(self) -> VersionedCoreTEXT<V>
    where
        V::DataFrame: DataFrameAsDataSchema<DataSchema = V::DataSchema>,
    {
        CoreTEXT::new_unchecked(
            self.metaroot,
            self.measurements,
            self.layout.as_data_schema(),
        )
    }

    /// Set measurements and dataframe together
    ///
    /// Length of measurements must match the width of the input dataframe.
    pub fn set_named_measurements_and_data(
        &mut self,
        xs: NamedTemporalsAndOpticals<V>,
        df: PrimitiveDataFrame,
        allow_shared_names: bool,
        skip_index_check: bool,
    ) -> Result<(), SetMeasurementsAndDataError>
    where
        V::Optical: AsScaleOrTransform,
        <V::Optical as AsScaleOrTransform>::S: CheckedScaleTransform,
        <<V::Optical as AsScaleOrTransform>::S as CheckedScaleTransform>::Summary: Default,
        ScaleDatatypeMismatchError: From<ScaleErrorGroup<V>>,
        V::DataFrame: WithPrimitiveDataFrame<DfTarget = V::DataFrame>,
    {
        let new_df = self.layout.with_data(df)?;
        self.set_named_measurements_inner(xs, allow_shared_names, skip_index_check)?;
        self.layout = new_df;
        Ok(())
    }

    /// Set measurements without $PnN and dataframe together
    ///
    /// Length of measurements must match the width of the input dataframe.
    pub fn set_measurements_and_data(
        &mut self,
        measurements: TemporalsAndOpticals<V>,
        df: PrimitiveDataFrame,
    ) -> Result<(), SetUnnamdMeasurementsAndDataError>
    where
        V::Optical: AsScaleOrTransform,
        <V::Optical as AsScaleOrTransform>::S: CheckedScaleTransform,
        <<V::Optical as AsScaleOrTransform>::S as CheckedScaleTransform>::Summary: Default,
        ScaleDatatypeMismatchError: From<ScaleErrorGroup<V>>,
        V::DataFrame: WithPrimitiveDataFrame<DfTarget = V::DataFrame>,
    {
        let new_df = self.layout.with_data(df)?;
        self.set_measurements_inner(measurements)?;
        self.layout = new_df;
        Ok(())
    }

    /// Set measurements without $PnN, data schema, and data itself together
    ///
    /// Each input must represent the same number of columns.
    #[allow(clippy::needless_pass_by_value)]
    pub fn set_measurements_data_schema_and_data(
        &mut self,
        measurements: TemporalsAndOpticals<V>,
        data_schema: V::DataSchema,
        df: PrimitiveDataFrame,
    ) -> Result<(), SetUnnamdMeasurementsAndDataError>
    where
        V::Optical: AsScaleOrTransform,
        <V::Optical as AsScaleOrTransform>::S: CheckedScaleTransform,
        <<V::Optical as AsScaleOrTransform>::S as CheckedScaleTransform>::Summary: Default,
        ScaleDatatypeMismatchError: From<ScaleErrorGroup<V>>,
        V::DataSchema: WithPrimitiveDataFrame<DfTarget = V::DataFrame>,
    {
        let new_df = data_schema.with_data(df)?;
        self.set_measurements_and_layout_inner(measurements, new_df)?;
        Ok(())
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

    fn opt_keywords(&self) -> impl Iterator<Item = OptRootKeyword<'_>> {
        let x0 = OptRootKeyword::from_unstainedcenters(&self.unstainedcenters);
        let x1 = OptRootKeyword::from_str(&self.unstainedinfo);
        [x0, x1].into_iter().flatten()
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

    fn opt_keywords(&self) -> impl Iterator<Item = OptRootKeyword<'_>> {
        let x = OptRootKeyword::from_u32(&self.bits);
        let y = OptRootKeyword::from_u32(&self.tot);
        [x, y]
            .into_iter()
            .flatten()
            .chain(self.flags.opt_keywords())
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

    fn opt_keywords(&self) -> impl Iterator<Item = OptRootKeyword<'_>> {
        let xs = &self.0;
        let mode = (!xs.is_empty()).then_some(OptRootKeyword::from_value(CSMode(xs.len())));
        xs.iter()
            .flatten()
            .enumerate()
            .map(|(i, k)| SplitKeyword1::from_value1(*k, i))
            .map(OptRootKeyword::from)
            .chain(mode)
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

    fn opt_keywords(&self) -> impl Iterator<Item = OptRootKeyword<'_>> {
        let x0 = OptRootKeyword::from_str(&self.last_modifier);
        let x1 = self.last_modified.map(OptRootKeyword::from_value);
        let x2 = self.originality.map(OptRootKeyword::from_value);
        [x0, x1, x2].into_iter().flatten()
    }
}

impl CarrierData {
    fn lookup(kws: &mut StdKeywords) -> Self {
        let l = Locationid::remove_root_opt_nofail(kws);
        let i = Carrierid::remove_root_opt_nofail(kws);
        let t = Carriertype::remove_root_opt_nofail(kws);
        Self::new(i, t, l)
    }

    fn opt_keywords(&self) -> impl IntoIterator<Item = OptRootKeyword<'_>> {
        let a = OptRootKeyword::from_str(&self.carrierid);
        let b = OptRootKeyword::from_str(&self.carriertype);
        let c = OptRootKeyword::from_str(&self.locationid);
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

    fn opt_keywords(&self) -> impl Iterator<Item = OptRootKeyword<'_>> {
        let x0 = OptRootKeyword::from_str(&self.wellid);
        let x1 = OptRootKeyword::from_str(&self.platename);
        let x2 = OptRootKeyword::from_str(&self.plateid);
        [x0, x1, x2].into_iter().flatten()
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

    pub(crate) fn opt_keywords(&self, i: MeasIndex) -> impl Iterator<Item = OptPeakKeyword> {
        let x = self.bin.map(|v| OptPeakKeyword::from_value(v, i));
        let y = self.size.map(|v| OptPeakKeyword::from_value(v, i));
        [x, y].into_iter().flatten()
    }
}

impl ScaleTransform {
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

    fn lookup<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        i: MeasIndex,
        dt: AlphaNumType,
        conf: &C,
    ) -> LookupOpticalResult<(Self, OpticalScaleFix)>
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadStdKeywordsConfig>,
    {
        let gain = Gain::remove_or_drop_meas_opt(std, nonstd, i, conf.as_ref())
            .map_switchable_errors(LookupOpticalWarning::from)
            .switchable_into_commutative()
            .map_errors(LookupOpticalError::from)
            .into_semigroup();
        let scale = Scale::remove_meas_req_with(std, i, dt, conf.as_ref())
            .map_err(LookupOpticalError::from)
            .into_log();
        gain.zip_commutative(scale).and_then_commutative(|(g, s)| {
            Self::try_from((s.native, g))
                .map(|x| (x, s.diagnostic))
                .map_err(LookupOpticalError::from)
                .into_log()
        })
    }

    fn req_suffixes(&self, i: MeasIndex) -> impl Iterator<Item = ReqMeasKeyword<'_>> {
        let (scale, _): (Scale, _) = (*self).into();
        once(ReqMeasKeyword::from_value(scale, i))
    }

    fn opt_keyword(&self, i: MeasIndex) -> Option<OptOpticalKeyword<'_>> {
        if let (_, Some(gain)) = (*self).into()
            && !gain.0.is_one()
        {
            Some(OptOpticalKeyword::from_value(gain, i))
        } else {
            None
        }
    }

    pub(crate) fn is_noop(&self) -> bool {
        *self == Self::default()
    }
}

impl DatasetSegments {
    fn try_new(
        data: HeaderOrTextSegment<DataSegmentId>,
        analysis: HeaderOrTextSegment<AnalysisSegmentId>,
        data_uncorr: Option<UncorrectedSegment>,
        analysis_uncorr: Option<UncorrectedSegment>,
        limit: OverlapCorrectionLimit,
    ) -> Result<Self, SegmentOverlapError> {
        // Check for overlaps if we have two non-empty segments that are both
        // from TEXT. We can assume that if they are both from HEADER that
        // this has already been checked.
        if let (HeaderOrTextSegment::Text(mut dt), HeaderOrTextSegment::Text(mut at)) =
            (data, analysis)
            && let (Some(dq), Some(aq)) = (dt.try_as_generic(), at.try_as_generic())
        {
            if dq.begin < aq.begin {
                let overlap = dq.get_tail_overlap(&aq);
                if overlap <= limit.0 {
                    dt.truncate(overlap);
                } else {
                    return Err(SegmentOverlapError::new(dq, aq));
                }
            } else {
                let overlap = aq.get_tail_overlap(&dq);
                if overlap <= limit.0 {
                    at.truncate(overlap);
                } else {
                    return Err(SegmentOverlapError::new(aq, dq));
                }
            }
        }
        Ok(Self::new(
            data.into_any(),
            analysis.into_any(),
            data_uncorr,
            analysis_uncorr,
        ))
    }
}

mod private {
    pub struct NoTouchy;
}

#[cfg(feature = "serde")]
mod serialize {
    use crate::core::AnyCore;
    use serde::{Serialize, ser::SerializeStruct as _};

    impl<A, L2_0, L3_0, L3_1, L3_2, O> Serialize for AnyCore<A, L2_0, L3_0, L3_1, L3_2, O>
    where
        A: Serialize,
        L2_0: Serialize,
        L3_0: Serialize,
        L3_1: Serialize,
        L3_2: Serialize,
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
    use super::ScaleTransform;

    use crate::text::ranged_float::PositiveFloat;

    use fireflow_types::python::InvalidKeywordValueError;

    use pyo3::IntoPyObjectExt as _;
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
}

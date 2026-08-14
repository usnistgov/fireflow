//! Data structures representing standardized TEXT segment

use crate::api::{FCSFileReader, HeaderAndSuppOffsets, next_dataset_boundary};
use crate::config::{
    AllowLoss, AppendFlag, AppendRepairFlagError, AppendableFlag, ComputeWriteCRC, ConfigFlag as _,
    DummyTriFlag, EvaledReadDataKeywordsConfig, EvaledReadStdKeywordsConfig,
    OverlapCorrectionLimit, ReadDataKeywordsConfig, ReadDatasetConfig, ReadHeaderAndTEXTConfig,
    ReadOffsetConfig, ReadSharedConfig, ReadStdKeywordsConfig, WriteDatasetInnerConfig,
    WriteMultiConfig, WriteMultiDatasetConfig, WriteMultiTEXTConfig, WriteTEXTInnerConfig,
};
use crate::convert::UsizeExt as _;
use crate::data::{
    ConvertFromLayout, DataFrame2_0, DataFrame3_0, DataFrame3_1, DataFrame3_2,
    DataFrameAsDataSchema, DataFrameCheckRanges, DataSchema2_0, DataSchema3_0, DataSchema3_1,
    DataSchema3_2, DataSchemaDiagnostics, DataSchemaToDataFrameError, DataSchemaToEmptyDataFrame,
    EventOverRangeError, EventOverRangeSummary, EventsDiagnostics, IsTot, LayoutDatatype,
    LayoutHeight as _, LayoutInsert, LayoutInsertScaleCheck, LayoutKeywords, LayoutNormalize,
    LayoutOptMeasKeywords, LayoutRemove, LayoutSize as _, LayoutWidth, LookupDataSchemaError,
    LookupDataSchemaWarning, MeasLayoutMismatchError, MeasurementsWithLayoutError,
    NewDataSchemaError, OverrangeColumn, RangeAndSeries, ReadCheckedDataframeError,
    ReadCheckedDataframeWarning, VersionedDataFrame as _, VersionedDataSchema,
    WithPrimitiveDataFrame,
};
use crate::header::{
    GuessVersionError, HeaderKeywordsToWrite, KeywordVersionScores, WriteTEXTHeaderError,
    autodetect_version,
};
use crate::logging::{
    CommutativeResultIter as _, DeferredIter as _, DeferredSwitchableError,
    DeferredWarningsAndErrors, ErrorGroup, ErrorsResult, GroupResult, IOErrorGroup, ImpureError,
    LogResult, ResultExt as _, WarningAndGroupResult, WarningAndIOGroupResult,
    WarningOrErrorResult, WarningsAndErrorsResult, WarningsAndGroupResult,
    WarningsAndIOGroupResult, io_to_log,
};
use crate::macros::{assert_eq_msg, def_summary};
use crate::match_many_to_one;
use crate::meas::{
    ConvertFromOptical, ConvertFromScale, ConvertFromShortname, ConvertFromTemporal,
    CoreMeasurements, DatasetSetDataSchemaError, DatasetSetUnnamedMeasAndDataSchemaError,
    InnerOptical2_0, InnerOptical3_0, InnerOptical3_1, InnerOptical3_2, InnerTemporal2_0,
    InnerTemporal3_0, InnerTemporal3_1, InnerTemporal3_2, InsertOpticalError, InsertTemporalError,
    LookupMeasError, LookupOptical, LookupScaledOpticalError, LookupScaledOpticalWarning,
    LookupShortname, LookupShortnameError, LookupTemporal, LookupTemporalError,
    LookupTemporalWarning, MeasConvertError, MeasConvertWarning, MeasMeta, MissingTimeError,
    NewMeasError, Optical, OpticalFromTemporal, PushOpticalError, PushTemporalError,
    ReplaceTemporalByIndexError, ReplaceTemporalByNameError, ScaledOptical, SetScalesError,
    SetScalesSummary, SetTemporalByIndexError, SetTemporalByNameError, SetTemporalError,
    SetUnnamdMeasurementsAndDataError, SetUnnamedMeasurementsAndDataSchemaError,
    SetUnnamedMeasurementsError, SwapOpticalWithTemporal, Temporal, TemporalFromOptical,
    TemporalMaybeToOptical, TemporalsAndOpticalsWithScale2_0, TemporalsAndOpticalsWithScale3_0,
    TemporalsAndOpticalsWithScale3_1, TemporalsAndOpticalsWithScale3_2,
    VNamedTemporalsAndOpticalsWithScale, VNamedTemporalsAndScaledOpticals,
    VPairedTemporalOrOpticalWithScale, VTemporalOrOpticalWithScale, VTemporalsAndOpticals,
    VersionMeasSet, impl_ref_specific_ro, impl_ref_specific_rw,
};
use crate::segment::read::{PrimaryTextOffsets, SupplementalTextOffsets};
use crate::segment::{
    AnalysisSegmentId, DataSegmentId,
    read::{
        AnyAnalysisOffsets, AnyDataOffsets, HeaderOrTextOffsets, IndexedOtherOffsets,
        IsOffsetPair as _, KeyedOptSegmentWithDefault as _, KeyedReqSegmentWithDefault as _,
        OffsetPairsOverlapError, OffsetsMismatchError, OptOffsetsWithDefaultWarning,
        OriginalOffsets, ReqOffsetsWithDefaultError, ReqOffsetsWithDefaultWarning, TextOffsetsName,
        TextOffsetsOverflow, TextToHeaderOrSuppOffsetsOverlap,
    },
};
use crate::text::datetimes::{
    BeginDateTime, Datetimes, DatetimesDiagnostics, EndDateTime, LookupDatetimesError,
    ReversedDatetimesError,
};
use crate::text::gating::{
    AppliedGates2_0, AppliedGates3_0, AppliedGates3_0To2_0Error, AppliedGates3_0To3_2Error,
    AppliedGates3_2, AppliedGatesDiagnostics, GatedMeasurements, LookupAppliedGates2_0Error,
    LookupAppliedGates3_0Error, LookupAppliedGates3_2Error,
};
use crate::text::index::{IndexFromOne, MeasIndex};
use crate::text::keyword_enum::{
    AnyKeyword, AnyMetarootKeyLossError, AnyTemporalToOpticalKeyLossError, AsKeywordPair as _,
    HasMembership as _, Keyword0FromValue as _, Keyword1FromValue as _, NonStdKeyword, OptKeyword,
    OptMeasKeyword, OptRootKeyword, ReqKeyword, ReqMeasKeyword, ReqRootKeyword, SplitKeyword,
    SplitKeyword1, StdOrNonStdOptRootKeyword,
};
use crate::text::keywords::{
    Abrt, AlphaNumType, AnyMeasScaleFix, CSMode, CSTot, CSVBits, CSVFlag, Carrierid, Carriertype,
    Cells, Com, Compensation2_0, Compensation3_0, Cyt, Cyt3_2, Cytsn, Exp, ExtraStdKeywords,
    Feature, Fil, Flowrate, Gate, HyperGateError, HyperParError, Inst, KeywordOtherVersionError,
    LastModified, LastModifier, Locationid, LookupComp2_0Error, Lost, MeasOrGateIndex, Mode,
    Mode3_2, ModeUpgradeError, Nextdata, NoCytError, Op, Originality, Par, Plateid, Platename,
    PrefixedMeasIndex, Proj, PseudostandardError, ScaleFix, Smno, Src, Sys, Timestep,
    TimestepAdded, TimestepFoundError, Tot, Trigger, Unicode, UnstainedCenters, UnstainedInfo, Vol,
    Wellid,
};
use crate::text::lookup::{
    Diagnosed, OptIndexedKey as _, OptIndexedKeyError, OptKeyError, OptKeyStError,
    OptMetarootKey as _, ReqKeyError, ReqMetarootKey as _,
};
use crate::text::named_vec::{
    Element, ElementIndexError, IndexedElement, InputLengthError, KeyIsOptical, NameMapping,
    NameNotFoundError, NamePresentError, NamedSet, NonCenterElement, RenameError, SetCenterError,
    SetElementsError, SetKeysError, SetNamesError, all_unique_names,
};
use crate::text::optional::{Identity, MightHave, Nothing};
use crate::text::relational::{
    AnyExistingIndexLinkError, AnyExistingNamedLinkError, BrokenIndexedLinkError,
    BrokenNamedLinkError, BrokenOrDependentLinkError, BrokenRegionLinkError,
    ExistingIndexedLinkError, ExistingLinkError, ExistingLinkErrors, IndicesToRemove,
    KeyToNameLinkError, OpticalNamesToRemove, RemovedLink,
};
use crate::text::spillover::{Spillover, SpilloverDiagnostics};
use crate::text::timestamps::{
    Btim, Etim, FCSDate, FCSTime, FCSTime60, FCSTime60Error, FCSTime100, FCSTime100Error,
    FCSTimeError, LookupTimestampsError, ReversedTimestampsError, Timestamps,
    TimestampsDiagnostics, Xtim,
};
use crate::validated::ascii_uint::{
    HeaderString, Uint8DigitOverflowError, UintSpacePad8, UintSpacePad20,
};
use crate::validated::compensation::Compensation;
use crate::validated::dataframe::{AnyPrimitiveSeries, PrimitiveDataFrame};
use crate::validated::datepattern::DatePattern;
use crate::validated::header_offsets::FinalHeaderOffsets;
use crate::validated::keys::{
    DKey0, DKey2, IndexedKey as _, Key as _, NonStdKeywords, NonStdKeywordsExt as _,
    RepairCollisionError, RepairDiagnostics, StdKey, StdKeywords, StringOrBytes, ValidKeywords,
};
use crate::validated::read_state::{
    CRC_LEN, CRCError, DatasetLen, DatasetLenEOFError, DatasetOffset, DatasetOffsetError,
    TEXTReadState, WriteFCSDigest,
};
use crate::validated::shortname::Shortname;
use crate::validated::textdelim::TEXTDelim;
use crate::validated::timepattern::TimePattern;

use fireflow_types::config::{
    IncludeReqOrOpt, IncludeRootOrMeas, OverBitmaskAction, OverRangeAction,
};
use fireflow_types::keywords::{
    HasVersion, OpticalFeature, Version, Version2_0, Version3_0, Version3_1, Version3_2,
};
use fireflow_types::nonempty_string::{NESliceExt as _, NEStr, NEString};
use nonempty_collections::NESlice;
use type_families::{ApplyOnce as _, BifunctorOnce as _, Functor as _, FunctorOnce as _, Pointed};

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveTime};
use derive_more::{AsMut, AsRef, Display, From};
use derive_new::new;
use hashbrown::{HashMap, hash_map::Entry};
use itertools::Itertools as _;
use nonempty_collections::{IntoIteratorExt as _, NEVec, iter::NonEmptyIterator as _};
use num_traits::identities::Zero;
use thiserror::Error;

use std::collections::HashSet;
use std::convert::{AsRef, Infallible};
use std::fmt;
use std::io::{self, BufReader, BufWriter, Read, Seek, Write};
use std::iter::{empty, once};
use std::mem;
use std::num::NonZeroU64;
use std::path::PathBuf;

#[cfg(feature = "serde")]
use {
    crate::text::keyword_enum::{
        AsHeader as _, OptMeasTemporalKeyword, OptScaledOpticalKeyword, OptTemporalKeyword,
        RefKeyword1,
    },
    crate::text::keywords as kws,
    ndarray::Array2,
    serde::Serialize,
    std::string::ToString as _,
};

#[cfg(feature = "python")]
use {
    crate::data::FullRange,
    crate::meas::VTemporalOrOptical,
    crate::text::named_vec::EitherPair,
    fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr, FromInnerPyObject},
    fireflow_types::python as py,
    pyo3::exceptions::PyValueError,
    pyo3::prelude::*,
    python::{PyRangeType, PySplitScale},
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
/// |     2.0 | [`InnerRootMeta2_0`] | [`InnerTemporal2_0`] | [`InnerOptical2_0`] | [`Option<Shortname>`]   | [`DataSchema2_0`] | [`DataFrame2_0`] |
/// |     3.0 | [`InnerRootMeta3_0`] | [`InnerTemporal3_0`] | [`InnerOptical2_0`] | [`Option<Shortname>`]   | [`DataSchema3_0`] | [`DataFrame3_0`] |
/// |     3.1 | [`InnerRootMeta3_1`] | [`InnerTemporal3_1`] | [`InnerOptical2_0`] | [`Identity<Shortname>`] | [`DataSchema3_1`] | [`DataFrame3_1`] |
/// |     3.2 | [`InnerRootMeta3_2`] | [`InnerTemporal3_2`] | [`InnerOptical2_0`] | [`Identity<Shortname>`] | [`DataSchema3_2`] | [`DataFrame3_2`] |
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
#[derive(Clone, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[new(visibility(""))]
// NOTE fields are private since metaroot, measurements, and data schema are all
// related to each other and must be kept in sync
pub struct Core<Analysis, Layout, Other, Root, Temporal, Optical, Scale, Name, Version> {
    /// Metaroot TEXT keywords.
    ///
    /// This includes all keywords that are not part of measurements or the data
    /// schema (ie the "root" of the metadata if thought of as a hierarchy)
    rootmeta: RootMeta<Root>,

    /// Measurement TEXT keywords and DATA if applicable.
    meas: CoreMeasurements<Layout, Temporal, Optical, Scale, Name, Version>,

    /// Non-standard keywords.
    ///
    /// This will include all the keywords that do not start with '$'.
    ///
    /// Keywords which do start with '$' but are not part of the standard are
    /// considered 'pseudostandard' and stored elsewhere since this structure
    /// will also be used to write FCS-compliant files (which do not allow
    /// nonstandard keywords starting with '$')
    nonstandard_keywords: NonStdKeywords,

    /// ANALYSIS segment (if applicable)
    analysis: Analysis,

    /// Other segments (if applicable)
    others: Other,
}

/// The ANALYSIS segment, which is just a string of bytes
#[derive(Clone, PartialEq, Default)]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
pub struct Analysis(pub StringOrBytes);

/// An OTHER segment, which is just a string of bytes
#[derive(Clone, PartialEq)]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
pub struct Other(pub StringOrBytes);

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
pub struct RootMeta<X> {
    /// Value of $ABRT
    #[as_ref(Option<Abrt>)]
    #[as_mut(Option<Abrt>)]
    #[new(into)]
    abrt: Option<Abrt>,

    /// Value of $COM
    #[as_ref(Com)]
    #[as_mut(Com)]
    #[new(into)]
    com: Com,

    /// Value of $CELLS
    #[as_ref(Cells)]
    #[as_mut(Cells)]
    #[new(into)]
    cells: Cells,

    /// Value of $EXP
    #[as_ref(Exp)]
    #[as_mut(Exp)]
    #[new(into)]
    exp: Exp,

    /// Value of $FIL
    #[as_ref(Fil)]
    #[as_mut(Fil)]
    #[new(into)]
    fil: Fil,

    /// Value of $INST
    #[as_ref(Inst)]
    #[as_mut(Inst)]
    #[new(into)]
    inst: Inst,

    /// Value of $LOST
    #[as_ref(Option<Lost>)]
    #[as_mut(Option<Lost>)]
    #[new(into)]
    lost: Option<Lost>,

    /// Value of $OP
    #[as_ref(Op)]
    #[as_mut(Op)]
    #[new(into)]
    op: Op,

    /// Value of $PROJ
    #[as_ref(Proj)]
    #[as_mut(Proj)]
    #[new(into)]
    proj: Proj,

    /// Value of $SMNO
    #[as_ref(Smno)]
    #[as_mut(Smno)]
    #[new(into)]
    smno: Smno,

    /// Value of $SRC
    #[as_ref(Src)]
    #[as_mut(Src)]
    #[new(into)]
    src: Src,

    /// Value of $SYS
    #[as_ref(Sys)]
    #[as_mut(Sys)]
    #[new(into)]
    sys: Sys,

    /// Value of $TR
    #[as_ref(Option<Trigger>)]
    #[new(into)]
    // NOTE not mutable to prevent mutation when part of Core
    tr: Option<Trigger>,

    /// Version-specific data
    specific: X,
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

/// Metaroot fields specific to version 2.0
#[derive(Clone, AsRef, AsMut, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct InnerRootMeta2_0 {
    /// Value of $MODE
    #[as_ref(Mode)]
    #[as_mut(Mode)]
    mode: Mode,

    /// Value of $CYT
    #[as_ref(Cyt)]
    #[as_mut(Cyt)]
    #[new(into)]
    cyt: Cyt,

    /// Compensation matrix derived from 'DFCnTOm' key/value pairs
    #[as_ref(Option<Compensation2_0>)]
    #[new(into)]
    // NOTE not mutable to prevent mutation when part of Core
    comp: Option<Compensation2_0>,

    /// Values of $BTIM/ETIM/$DATE
    #[as_ref(Timestamps2_0, Option<FCSDate>)]
    #[as_mut(Timestamps2_0)]
    timestamps: Timestamps2_0,

    /// Values of $Gm*/$RnI/$RnW/$GATING/$GATE
    #[as_ref(AppliedGates2_0)]
    #[as_mut(AppliedGates2_0)]
    // NOTE not mutable to prevent mutation when part of Core
    applied_gates: AppliedGates2_0,
}

/// Metaroot fields specific to version 3.0
#[allow(clippy::too_many_arguments)]
#[derive(Clone, AsRef, AsMut, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct InnerRootMeta3_0 {
    /// Value of $MODE
    #[as_ref(Mode)]
    #[as_mut(Mode)]
    mode: Mode,

    /// Value of $CYT
    #[as_ref(Cyt)]
    #[as_mut(Cyt)]
    #[new(into)]
    cyt: Cyt,

    /// Value of $COMP
    #[as_ref(Option<Compensation3_0>)]
    #[new(into)]
    // NOTE not mutable to prevent mutation when part of Core
    comp: Option<Compensation3_0>,

    /// Values of $BTIM/ETIM/$DATE
    #[as_ref(Timestamps3_0, Option<FCSDate>)]
    #[as_mut(Timestamps3_0)]
    timestamps: Timestamps3_0,

    /// Value of $CYTSN
    #[as_ref(Cytsn)]
    #[as_mut(Cytsn)]
    #[new(into)]
    cytsn: Cytsn,

    /// Value of $UNICODE
    #[as_ref(Option<Unicode>)]
    #[as_mut(Option<Unicode>)]
    #[new(into)]
    unicode: Option<Unicode>,

    /// Aggregated values for $CS* keywords
    #[as_ref(CSVBits)]
    #[as_mut(CSVBits)]
    #[as_ref(CSTot)]
    #[as_mut(CSTot)]
    #[as_ref(CSVFlags)]
    #[as_mut(CSVFlags)]
    subset: SubsetData,

    /// Values of $Gm*/$RnI/$RnW/$GATING/$GATE
    #[as_ref(AppliedGates3_0)]
    #[new(into)]
    // NOTE not mutable to prevent mutation when part of Core
    applied_gates: AppliedGates3_0,
}

/// Metaroot fields specific to version 3.1
#[allow(clippy::too_many_arguments)]
#[derive(Clone, AsRef, AsMut, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct InnerRootMeta3_1 {
    /// Value of $MODE
    #[as_ref(Mode)]
    #[as_mut(Mode)]
    mode: Mode,

    /// Value of $CYT
    #[as_ref(Cyt)]
    #[as_mut(Cyt)]
    #[new(into)]
    cyt: Cyt,

    /// Values of $BTIM/ETIM/$DATE
    #[as_ref(Timestamps3_1, Option<FCSDate>)]
    #[as_mut(Timestamps3_1)]
    timestamps: Timestamps3_1,

    /// Value of $CYTSN
    #[as_ref(Cytsn)]
    #[as_mut(Cytsn)]
    #[new(into)]
    cytsn: Cytsn,

    /// Value of $SPILLOVER
    #[as_ref(Option<Spillover>)]
    #[new(into)]
    // NOTE not mutable to prevent mutation when part of Core
    spillover: Option<Spillover>,

    /// Values of $LAST_MODIFIED/$LAST_MODIFIER/$ORIGINALITY
    #[as_ref(LastModifier, Option<LastModified>, Option<Originality>)]
    #[as_mut(LastModifier, Option<LastModified>, Option<Originality>)]
    modification: ModificationData,

    /// Values of $PLATEID/$PLATENAME/$WELLID
    #[as_ref(Plateid, Wellid, Platename)]
    #[as_mut(Plateid, Wellid, Platename)]
    plate: PlateData,

    /// Value of $VOL
    #[as_ref(Option<Vol>)]
    #[as_mut(Option<Vol>)]
    #[new(into)]
    vol: Option<Vol>,

    /// Aggregated values for $CS* keywords
    #[as_ref(CSVBits)]
    #[as_mut(CSVBits)]
    #[as_ref(CSTot)]
    #[as_mut(CSTot)]
    #[as_ref(CSVFlags)]
    #[as_mut(CSVFlags)]
    subset: SubsetData,

    /// Values of $Gm*/$RnI/$RnW/$GATING/$GATE
    #[as_ref(AppliedGates3_0)]
    #[new(into)]
    // NOTE not mutable to prevent mutation when part of Core
    applied_gates: AppliedGates3_0,
}

/// Metaroot fields specific to version 3.2
#[allow(clippy::too_many_arguments)]
#[derive(Clone, AsRef, AsMut, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct InnerRootMeta3_2 {
    /// Value of $MODE
    #[as_ref(Option<Mode3_2>)]
    #[as_mut(Option<Mode3_2>)]
    #[new(into)]
    mode: Option<Mode3_2>,

    /// Values of $BTIM/ETIM/$DATE
    #[as_ref(Timestamps3_1, Option<FCSDate>)]
    #[as_mut(Timestamps3_1)]
    timestamps: Timestamps3_1,

    /// Values of $BEGINDATETIME/$ENDDATETIME
    #[as_ref(Option<BeginDateTime>, Option<EndDateTime>, Datetimes)]
    #[as_mut(Datetimes)]
    datetimes: Datetimes,

    /// Value of $CYT
    #[as_ref(Cyt3_2)]
    #[as_mut(Cyt3_2)]
    cyt: Cyt3_2,

    /// Value of $SPILLOVER
    #[as_ref(Option<Spillover>)]
    #[new(into)]
    // NOTE not mutable to prevent mutation when part of Core
    spillover: Option<Spillover>,

    /// Value of $CYTSN
    #[as_ref(Cytsn)]
    #[as_mut(Cytsn)]
    #[new(into)]
    cytsn: Cytsn,

    /// Values of $LAST_MODIFIED/$LAST_MODIFIER/$ORIGINALITY
    #[as_ref(LastModifier, Option<LastModified>, Option<Originality>)]
    #[as_mut(LastModifier, Option<LastModified>, Option<Originality>)]
    modification: ModificationData,

    /// Values of $PLATEID/$PLATENAME/$WELLID
    #[as_ref(Plateid, Wellid, Platename)]
    #[as_mut(Plateid, Wellid, Platename)]
    plate: PlateData,

    /// Value of $VOL
    #[as_ref(Option<Vol>)]
    #[as_mut(Option<Vol>)]
    #[new(into)]
    vol: Option<Vol>,

    /// Values of $CARRIERID/$CARRIERTYPE/$LOCATIONID
    #[as_ref(Carrierid, Carriertype, Locationid)]
    #[as_mut(Carrierid, Carriertype, Locationid)]
    carrier: CarrierData,

    /// Values of $UNSTAINEDINFO/$UNSTAINEDCENTERS
    #[as_ref(UnstainedCenters, UnstainedInfo)]
    #[as_mut(UnstainedInfo)]
    unstained: UnstainedData,

    /// Value of $FLOWRATE
    #[as_ref(Flowrate)]
    #[as_mut(Flowrate)]
    #[new(into)]
    flowrate: Flowrate,

    /// Values of $RnI/$RnW/$GATING
    #[as_ref(AppliedGates3_2)]
    // NOTE not mutable to prevent mutation when part of Core
    applied_gates: AppliedGates3_2,
}

/// Segment offsets and $TOT as read from TEXT segment
///
/// This is used later to parse DATA and ANALYSIS.
#[derive(new)]
pub struct TEXTOffsets<T> {
    pub(crate) offsets: DatasetOffsets,
    pub(crate) tot: T,
}

impl<T> TEXTOffsets<T> {
    fn into_common(self) -> TEXTOffsets<Option<Tot>>
    where
        T: MightHave<Tot>,
    {
        TEXTOffsets::new(self.offsets, self.tot.to_opt())
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

pub type Metaroot2_0 = RootMeta<InnerRootMeta2_0>;
pub type Metaroot3_0 = RootMeta<InnerRootMeta3_0>;
pub type Metaroot3_1 = RootMeta<InnerRootMeta3_1>;
pub type Metaroot3_2 = RootMeta<InnerRootMeta3_2>;

type Timestamps2_0 = Timestamps<FCSTime>;
type Timestamps3_0 = Timestamps<FCSTime60>;
type Timestamps3_1 = Timestamps<FCSTime100>;

/// A standardized TEXT segment
pub type CoreTEXT<M, L, T, O, X, N, V> = Core<(), L, (), M, T, O, X, N, V>;

/// A standardized FCS dataset (TEXT+DATA+ANALYSIS+OTHER)
pub type CoreDataset<M, L, T, O, X, N, V> = Core<Analysis, L, Others, M, T, O, X, N, V>;

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

pub(crate) type VersionedCore<A, L, O, V> = Core<
    A,
    L,
    O,
    <V as VersionSet>::RootMeta,
    <V as VersionMeasSet>::Temporal,
    <V as VersionMeasSet>::Optical,
    <V as VersionMeasSet>::OpticalScale,
    <V as VersionMeasSet>::Name,
    V,
>;

pub(crate) type VersionedCoreTEXT<V> = VersionedCore<(), <V as VersionMeasSet>::DataSchema, (), V>;

pub(crate) type VersionedCoreDataset<V> =
    VersionedCore<Analysis, <V as VersionMeasSet>::DataFrame, Others, V>;

/// Reader for ANALYSIS segment
#[derive(new)]
pub struct AnalysisReader {
    pub seg: AnyAnalysisOffsets,
}

impl AnalysisReader {
    pub(crate) fn h_read<R: Read + Seek>(&self, h: &mut BufReader<R>) -> io::Result<Analysis> {
        let mut buf = vec![];
        self.seg.h_read_contents(h, &mut buf)?;
        Ok(Analysis(StringOrBytes::from(buf)))
    }
}

/// Reader for OTHER segments
#[derive(new)]
pub struct OthersReader {
    pub offsets: Vec<IndexedOtherOffsets>,
}

impl OthersReader {
    pub(crate) fn h_read<R: Read + Seek>(&self, h: &mut BufReader<R>) -> io::Result<Others> {
        let mut buf = vec![];
        let mut others = vec![];
        for s in &self.offsets {
            s.offsets.h_read_contents(h, &mut buf)?;
            others.push(Other(StringOrBytes::from(buf.clone())));
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
    pub header: FinalHeaderOffsets,
}

/// Output when making standardized TEXT+DATA
#[derive(Clone, new, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct StdDatasetFromKwsOutput {
    /// DATA+ANALYSIS
    pub dataset_offsets: DatasetOffsets,

    /// Diagnostic output from repairing the keyword list
    pub repair_diagnostics: RepairDiagnostics,

    /// Keywords that start with '$' that are not part of the standard
    pub std_diagnostics: StdTEXTDiagnostics,

    /// Diagnostic output from parsing entire dataset.
    pub dataset_diagnostics: DatasetDiagnostics,
}

/// Diagnostic output from reading entire dataset.
#[derive(Clone, PartialEq, Default, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[allow(clippy::too_many_arguments)]
pub struct DatasetDiagnostics {
    /// The width of one event in bytes (if not ASCII delimited).
    pub event_width: Option<u64>,

    /// The remainder after dividing length of DATA by event width.
    ///
    /// For well-formed files, this should be zero.
    ///
    /// Will be [`Option::None`] for delimited ASCII layouts.
    pub event_data_remainder: Option<u64>,

    /// `true` if $TOT does not match the number of events computed via event width.
    ///
    /// [`Option::None`] if $TOT is missing (FCS 2.0) or the layout is ASCII
    /// delimited and there is no event width.
    pub tot_event_mismatch: Option<bool>,

    /// Columns for which at least one event was over $PnR.
    ///
    /// Length of vector will be equal to $PAR. Elements correspond to column
    /// indices and will be `None` if not overrange. Otherwise, the first
    /// [`usize`] will be the row that has the first overrange value, and the
    /// second [`bool`] will be `true` if the value was truncated to fit and
    /// false otherwise.
    pub overrange_columns: Vec<OverrangeColumn>,

    /// Unparsed bytes between segments.
    pub intra_segment_dark_bytes: Vec<IntraSegmentDarkBytes>,

    /// Unparsed bytes between the end of this dataset and the beginning of the next.
    pub post_dataset_dark_bytes: Option<DarkBytes>,

    /// Value of the cyclic redundancy check (CRC) as read from the file.
    ///
    /// Will always be `None` for 2.0.
    pub file_crc: Option<CRCOutput>,

    /// Value of the computed cyclic redundancy check (CRC) of the dataset.
    ///
    /// Will always be `None` for 2.0.
    pub computed_crc: Option<u16>,

    /// The total length of the dataset in bytes.
    ///
    /// Will count from the first byte of HEADER to the last segment or the CRC.
    pub dataset_len: u64,

    /// The offset of the next dataset if it exists.
    ///
    /// This can be obtained either from $NEXTDATA or by manually scanning the
    /// file for the next dataset.
    ///
    /// Will be `None` if this is the last dataset in the FCS file.
    pub next_dataset_offset: Option<DatasetOffset>,

    /// `true` if the value of [`Self::next_dataset_offset`] was found by
    /// manually scanning the file.
    pub next_dataset_manually_scanned: bool,
}

/// The output of parsing the CRC at the end of the last dataset.
#[derive(Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum CRCOutput {
    /// CRC was a valid 16 bit decimal number.
    Valid(u16),
    /// CRC bytes were found but did not parse to a 16-bit number.
    Invalid(StringOrBytes),
}

// TODO split this into sub structs for analysis and data since they are both
// repeated twice in each field here
/// Standardized TEXT+DATA+ANALYSIS with DATA+ANALYSIS offsets
#[derive(Clone, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct DatasetOffsets {
    /// Offsets used to parse DATA
    pub final_data: AnyDataOffsets,

    /// Offsets used to parse ANALYSIS
    pub final_analysis: AnyAnalysisOffsets,

    /// Encodes origin of DATA offsets.
    pub data_origin: TEXTOffsetsOrigin,

    /// Encodes origin of ANALYSIS offsets.
    pub analysis_origin: TEXTOffsetsOrigin,

    /// The amount of overlap between TEXT DATA and ANALYSIS.
    ///
    /// Will only be `Some` if both TEXT offsets exist, are different from
    /// HEADER (either they mismatch or HEADER is empty), and overlap each
    /// other.
    pub data_analysis_overlap: Option<NonZeroU64>,
}

#[derive(Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum TEXTOffsetsOrigin {
    /// TEXT offsets were empty.
    ///
    /// This is the only possible level for 2.0.
    EmptyTEXT,
    /// TEXT offsets were present but ignored.
    Ignored(Option<OriginalOffsets>),
    /// TEXT offsets are required but could not be parsed.
    // TODO this could either mean the offsets were entirely missing or that
    // they were present but could not be parsed into numbers.
    Unparsed,
    /// TEXT offsets are required but were numerically malformed.
    Malformed(OriginalOffsets),
    /// TEXT offsets present and match HEADER exactly.
    Match,
    /// TEXT offsets present and mismatch HEADER, latter were chosen
    MismatchHeader(OriginalOffsets),
    /// TEXT offsets present and mismatch HEADER, former were chosen
    MismatchTEXT(MismatchedTEXTOffsetOrigin),
}

#[derive(Clone, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct MismatchedTEXTOffsetOrigin {
    header_is_empty: bool,
    uncorr: OriginalOffsets,
    overlaps: Vec<TextToHeaderOrSuppOffsetsOverlap>,
    overflow: Option<TextOffsetsOverflow>,
}

/// Unparsed bytes which are between two segments in an FCS file.
#[derive(Clone, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct IntraSegmentDarkBytes {
    /// The name of the segment immediately prior.
    pub prev: FlankingSegmentName,
    /// The name of the segment immediately after.
    pub next: FlankingSegmentName,
    /// The starting offset of this region.
    pub start: u64,
    /// The final offset of this region (one greater than offset of the last byte).
    pub end: u64,
    /// The byte contents of this region.
    pub bytes: DarkBytes,
}

/// Bytes which are not part of any segment.
///
/// Many cases of these will be "padding," which is just one character like a
/// space or \0 char repeated many times. In these cases, it is more efficient
/// and more ergonomic to encode this special case.
#[derive(Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum DarkBytes {
    Padding { character: u8, n: usize },
    Bytes(NEVec<u8>),
    Utf8(NEString),
}

/// The name of a segment which immediately before/after a region of dark bytes.
#[derive(Clone, Copy, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum FlankingSegmentName {
    PrimaryText,
    SupplementalText,
    Other(usize),
    Data,
    Analysis,
}

/// Internal configuration options used when writing HEADER+TEXT
pub(crate) struct WriteHeaderAndTextConfig<'a> {
    pub(crate) delim: TEXTDelim,
    pub(crate) tot: Tot,
    pub(crate) data_len: u64,
    pub(crate) analysis_len: u64,
    pub(crate) other_segs: &'a [Other],
    pub(crate) has_nextdata: AppendableFlag,
    pub(crate) fil: Option<NEString>,
}

impl WriteHeaderAndTextConfig<'_> {
    fn new_nodata(delim: TEXTDelim, has_nextdata: AppendableFlag, fil: Option<NEString>) -> Self {
        Self {
            delim,
            tot: Tot(0),
            data_len: 0,
            analysis_len: 0,
            other_segs: &[],
            has_nextdata,
            fil,
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
    /// Optional keys which could not be parsed
    pub optional: StdKeywords,

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

    /// Original $PnN if they are renamed to remove duplicates.
    pub dedup_names: Vec<Option<Shortname>>,

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

    /// `Some(true)` if $SPILLOVER used indices rather than names.
    pub spillover_was_indexed: Option<bool>,

    /// Alternative pattern used to parse $BTIM.
    pub btim_pattern: Option<TimePattern>,

    /// Alternative pattern used to parse $ETIM.
    pub etim_pattern: Option<TimePattern>,

    /// Alternative pattern used to parse $DATE.
    pub date_pattern: Option<DatePattern>,

    /// Alternative pattern used to parse $BEGINDATETIME.
    pub begindatetime_pattern: Option<String>,

    /// Alternative pattern used to parse $BEGINDATETIME.
    pub enddatetime_pattern: Option<String>,

    /// `Some(true)` if $BEGINDATETIME was parsed with local time zone.
    pub begindatetime_used_localtime: Option<bool>,

    /// `Some(true)` if $ENDDATETIME was parsed with local time zone.
    pub enddatetime_used_localtime: Option<bool>,

    /// Alternative pattern used to parse $LAST_MODIFIED.
    pub last_modified_pattern: Option<String>,

    /// Diagnostic output from parsing the data schema.
    pub schema_diagnostics: DataSchemaDiagnostics,
}

pub(crate) type TrimmedKeyword = (StdKey, NEString);
pub(crate) type TrimmedKeywords = Vec<TrimmedKeyword>;

impl StdTEXTDiagnostics {
    fn from_extra(
        extra: ExtraStdKeywords,
        optional: StdKeywords,
        original_names: Vec<Option<Shortname>>,
        metaroot: MetarootDiagnostics,
        meas: MeasurementDiagnostics,
        schema: DataSchemaDiagnostics,
    ) -> Self {
        let mut trimmed = metaroot.trimmed;
        trimmed.extend(meas.trimmed);
        trimmed.extend(metaroot.spillover.trimmed);
        trimmed.extend(metaroot.applied_gates.trimmed);
        Self {
            optional,
            pseudostandard: extra.pseudostandard,
            hyper_par: extra.hyper_par,
            hyper_gate: extra.hyper_gate,
            other_version: extra.other_version,
            timestep: extra.timestep,
            dedup_names: original_names,
            scale: meas.scale,
            gate_scale: metaroot.applied_gates.fixed_scales,
            trimmed,
            temporal_optical_pairs: meas.tmp_opt_pairs,
            timestep_added: meas.timestep_added,
            spillover_was_indexed: metaroot.spillover.indexed,
            btim_pattern: metaroot.timestamps.btim,
            etim_pattern: metaroot.timestamps.etim,
            date_pattern: metaroot.timestamps.date,
            begindatetime_pattern: metaroot.datetime.begin.pattern,
            enddatetime_pattern: metaroot.datetime.end.pattern,
            begindatetime_used_localtime: metaroot.datetime.begin.used_localtime,
            enddatetime_used_localtime: metaroot.datetime.end.used_localtime,
            last_modified_pattern: metaroot.last_modified_pattern,
            schema_diagnostics: schema,
        }
    }
}

pub type DiagnosedMetaroot<M> = Diagnosed<M, MetarootDiagnostics>;

type DiagnosedUnstainedData<U> = Diagnosed<U, Option<TrimmedKeyword>>;

#[derive(new)]
pub struct MeasurementDiagnostics {
    scale: Vec<AnyMeasScaleFix>,
    trimmed: TrimmedKeywords,
    tmp_opt_pairs: Vec<(StdKey, NEString)>,
    timestep_added: TimestepAdded,
}

#[derive(new)]
pub struct MetarootDiagnostics {
    trimmed: TrimmedKeywords,
    applied_gates: AppliedGatesDiagnostics,
    spillover: SpilloverDiagnostics,
    timestamps: TimestampsDiagnostics,
    datetime: DatetimesDiagnostics,
    last_modified_pattern: Option<String>,
}

/// Error when converting [`Core`] to new FCS version
#[derive(Debug, Display, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ConvertError {
    Meta(MetarootConvertError),
    Meas(MeasConvertError),
}

/// Error when converting [`Core`] to new FCS version
#[derive(Debug, Display, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ConvertWarning {
    Meta(MetarootConvertWarning),
    Meas(MeasConvertWarning),
}

type MetarootConvertResult<M> =
    WarningsAndErrorsResult<M, (), MetarootConvertWarning, MetarootConvertError>;

/// Error when writing [`CoreDataset`] to file
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum StdWriterError {
    Layout(NewDataSchemaError),
    Check(EventOverRangeError),
    HeaderText(WriteTEXTHeaderError),
}

/// Link error when setting new measurements
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
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

/// Error when setting measurements and DATA/dataframe simultaneously
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum SetNamedMeasurementsAndDataError {
    Meas(SetNamedMeasurementsError),
    Layout(MeasurementsWithLayoutError),
    Mismatch(DataSchemaToDataFrameError),
    Link(SetMeasurementLinkErrors),
}

/// Error when setting measurements and DATA/dataframe simultaneously
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum SetUnnamdMeasurementsAndDataSchemaAndDataFrameError {
    Data(DataSchemaToDataFrameError),
    Meas(SetUnnamedMeasurementsAndDataSchemaError),
}

/// Error when setting measurements vector
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum SetNamedMeasurementsError {
    New(MeasurementsWithLayoutError),
    Link(SetMeasurementLinkErrors),
}

/// Error when setting named measurements and data schema for a dataset.
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum DatasetSetNamedMeasAndDataSchemaError {
    Layout(MeasurementsWithLayoutError),
    DataSchema(DatasetSetDataSchemaError),
    DataFrame(DataSchemaToDataFrameError),
    Link(SetMeasurementLinkErrors),
}

/// Error when removing measurement by name ($PnN)
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum RemoveMeasByNameError {
    Link(ExistingLinkErrors),
    Name(NameNotFoundError),
}

/// Error when removing measurement by index
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum RemoveMeasByIndexError {
    Link(ExistingLinkErrors),
    Index(ElementIndexError),
}

/// Error when reading standardized TEXT from keyword pairs
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum StdTEXTFromKeywordsError {
    Error(StdTEXTFromFlatTEXTErrorInner),
    Warn(StdTEXTFromFlatTEXTWarning),
    Repair(RepairCollisionError),
    RepairAppend(AppendRepairFlagError),
}

/// Error when reading standardized TEXT from keyword pairs
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum StdTEXTFromKeywordsWarning {
    Error(StdTEXTFromFlatTEXTWarning),
    Repair(RepairCollisionError),
}

/// Error when reading standardized TEXT from keyword pairs
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum StdTEXTFromFlatTEXTError {
    Inner(StdTEXTFromFlatTEXTErrorInner),
    Version(GuessVersionError),
    Repair(RepairCollisionError),
}

/// Error (inner) when reading standardized TEXT from keyword pairs
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
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
    Repair(RepairCollisionError),
    AppendRepair(AppendRepairFlagError),
}

/// Warning when reading standardized TEXT from keyword pairs
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum StdTEXTFromFlatTEXTWarning {
    New(NewCoreWarning),
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
    Repair(RepairCollisionError),
}

/// Error when reading any version of standardized DATA from keyword pairs.
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum AnyStdDatasetFromFlatTextError {
    Inner(StdDatasetFromFlatTextErrorInner),
    Version(GuessVersionError),
}

/// Error when reading specific version of standardized DATA from keyword pairs
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum StdDatasetFromKeywordsError {
    Inner(StdDatasetFromFlatTextErrorInner),
    DatatsetLen(DatasetLenEOFError),
    Warn(StdDatasetFromFlatTEXTWarning),
}

/// Error (inner) when reading standardized DATA from keyword pairs
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum StdDatasetFromFlatTextErrorInner {
    DatasetOffset(DatasetOffsetError),
    TEXT(StdTEXTFromFlatTEXTErrorInner),
    Dataframe(ReadCheckedDataframeError),
    Offsets(LookupTEXTOffsetsError),
    CRC(CRCError),
}

/// Warning when reading standardized DATA from keyword pairs
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum StdDatasetFromFlatTEXTWarning {
    TEXT(StdTEXTFromFlatTEXTWarning),
    Offsets(LookupTEXTOffsetsWarning),
    Layout(ReadCheckedDataframeWarning),
    CRC(CRCError),
}

/// Error when metaroot is changed to new FCS version
///
/// Most of these only apply to very specific version combinations.
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum MetarootConvertError {
    NoCyt(NoCytError),
    Mode(ModeUpgradeError),
    Gates3_0To2_0(AppliedGates3_0To2_0Error),
    Gates3_0To3_2(AppliedGates3_0To3_2Error),
    Loss(AnyMetarootKeyLossError),
}

/// Warning when metaroot is changed to new FCS version
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum MetarootConvertWarning {
    Mode(ModeUpgradeError),
    Gates3_0To2_0(AppliedGates3_0To2_0Error),
    Gates3_0To3_2(AppliedGates3_0To3_2Error),
    Loss(AnyMetarootKeyLossError),
}

/// Error when reading DATA offsets from already-parsed keywords
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupAndReadDataAnalysisError {
    DatasetOffset(DatasetOffsetError),
    DatasetLen(DatasetLenEOFError),
    Par(ReqKeyError<Par>),
    Offsets(LookupTEXTOffsetsError),
    DataSchema(LookupDataSchemaError),
    Dataframe(ReadCheckedDataframeError),
    Warn(LookupAndReadDataAnalysisWarning),
    CRC(CRCError),
    Repair(RepairCollisionError),
    RepairAppend(AppendRepairFlagError),
}

/// Warning when reading DATA offsets from already-parsed keywords
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupAndReadDataAnalysisWarning {
    Offsets(LookupTEXTOffsetsWarning),
    DataSchema(LookupDataSchemaWarning),
    Data(ReadCheckedDataframeWarning),
    CRC(CRCError),
    Repair(RepairCollisionError),
}

/// Error when looking up offsets for parsing DATA
///
/// Note that not every error applies to every version.
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupTEXTOffsetsError {
    /// $TOT is missing (2.0+)
    Tot2(OptKeyError<Tot>),
    /// $TOT is missing (3.0+)
    Tot3(ReqKeyError<Tot>),
    /// required DATA keywords are missing (3.0/3.1)
    ReqData(ReqOffsetsWithDefaultError<DataSegmentId>),
    /// required ANALYSIS keywords are missing (3.0/3.1)
    ReqAnalysis(ReqOffsetsWithDefaultError<AnalysisSegmentId>),
    /// TEXT DATA offsets do not match HEADER (3.0+)
    MismatchData(OffsetsMismatchError<DataSegmentId>),
    /// required TEXT ANALYSIS offsets do not match HEADER (3.0/3.1)
    MismatchAnalysis(OffsetsMismatchError<AnalysisSegmentId>),
    /// optional TEXT ANALYSIS offsets do not match HEADER (3.2)
    MismatchAnalysisOpt(OptOffsetsWithDefaultWarning<AnalysisSegmentId>),
    /// DATA and ANALYSIS offsets are both non-empty and overlap each other
    DataAnalysisOverlap(OffsetPairsOverlapError<TextOffsetsName, TextOffsetsName>),
}

/// Warning when looking up offsets for parsing DATA
///
/// Note that not every warning applies to every version.
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupTEXTOffsetsWarning {
    /// $TOT is optional in FCS 2.0 (for some reason)
    Tot(OptKeyError<Tot>),
    /// TEXT DATA offsets can be optionally be overridden by HEADER (3.0+)
    ReqData(ReqOffsetsWithDefaultWarning<DataSegmentId>),
    /// TEXT ANALYSIS offsets can be optionally be overridden by HEADER (3.0+)
    ReqAnalysis(ReqOffsetsWithDefaultWarning<AnalysisSegmentId>),
    /// TEXT ANALYSIS offsets do not match HEADER and are dropped (3.0+)
    MismatchAnalysis(OptOffsetsWithDefaultWarning<AnalysisSegmentId>),
}

/// Error when building new [`CoreTEXT`]
///
/// The timestep/datetime errors are technically "relational" but are here and
/// not in NewCoreRelationalerror because each time/date object is created
/// prior to calling the function that would produce that error, and these
/// are validated for correct order.
///
/// Note that not every error applies to each version.
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
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
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum NewCoreError {
    /// Measurement vector has more than one time element
    Meas(NewMeasError),
    /// A keyword has invalid links (and is dropped in the case of a warning)
    Link(BrokenOrDependentLinkError),
}

/// Error when looking up [`CoreTEXT`] or [`CoreDataset`] from keywords
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupCoreError {
    /// Error when looking up measurement keywords
    Meas(LookupMeasError),
    /// Any other warning which is configured to be a fatal error
    Warn(NewCoreWarning),
}

/// Warning when building new [`CoreTEXT`]
///
/// Each of these are also errors but can be configured to only be warnings
/// if the user desires.
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum NewCoreWarning {
    /// Time channel is missing entirely
    Time(MissingTimeError),
    /// A keyword has invalid links (and is dropped in the case of a warning)
    Link(BrokenOrDependentLinkError),
}

type LookupMetarootResult<V> =
    WarningsAndErrorsResult<V, (), LookupMetarootWarning, LookupMetarootError>;

/// Error when parsing any metaroot keyword
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupMetarootError {
    Mode(ReqKeyError<Mode>),
    Cyt3_2(ReqKeyError<Cyt3_2>),
    Par(ReqKeyError<Par>),
    Warn(LookupMetarootWarning),
}

/// Warning when parsing any metaroot keyword
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
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
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupMeasurementError {
    Temporal(LookupTemporalError),
    Optical(LookupScaledOpticalError),
    TimeName(DuplicateTimeNameError),
    Warn(LookupMeasurementWarning),
}

/// Error when more than one $PnN matches the given time pattern
#[derive(Debug, Error, PartialEq, Clone)]
#[error(
    "Time pattern matched {k} with name {1} but a previous measurement already \
     matched; adjust time pattern so it only matches one $PnN",
    k = Shortname::std(self.0),
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConfigError))]
pub struct DuplicateTimeNameError(MeasIndex, Shortname);

/// Warning when parsing any measurement keyword.
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupMeasurementWarning {
    Temporal(LookupTemporalWarning),
    Optical(LookupScaledOpticalWarning),
    MissingTime(MissingTimeError),
}

/// Error when parsing $CS* keywords.
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupSubsetError {
    Flags(LookupCSVFlagsError),
    Bits(OptKeyError<CSVBits>),
    Tot(OptKeyError<CSTot>),
}

/// Error when parsing $CSMODE or $CSVnFlag
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupCSVFlagsError {
    Mode(OptKeyError<CSMode>),
    Flag(OptIndexedKeyError<CSVFlag>),
}

/// Error when parsing keywords for $LAST_MODIFIED or $ORIGINALITY
///
/// Note that $LAST_MODIFIER is infallible.
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupModifiedDataError {
    LastModTime(OptKeyStError<LastModified>),
    Originality(OptKeyError<Originality>),
}

type LookupTEXTOffsetsResult<T> =
    WarningsAndErrorsResult<T, (), LookupTEXTOffsetsWarning, LookupTEXTOffsetsError>;

/// Error when $COMP does not have the same number of rows/columns as $PAR
#[derive(Debug, Error, PartialEq, Clone)]
#[error("$COMP must have same row/column number as $PAR ({par}), got {comp}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct CompParMismatchError {
    par: usize,
    comp: usize,
}

/// Error when setting a new temporal measurement by name ($PnN)
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum SetLinkedTemporalByNameError {
    Inner(SetTemporalByNameError),
    Link(ExistingLinkErrors),
}

/// Error when setting a new temporal measurement by index ($PnN)
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum SetLinkedTemporalByIndexError {
    Inner(SetTemporalByIndexError),
    Link(ExistingLinkErrors),
}

/// Error when replacing temporal measurement by index
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ReplaceTemporalByIndexNoLossError {
    Set(SetCenterError),
    Link(ExistingLinkErrors),
}

/// Error when replacing temporal measurement by name
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ReplaceTemporalByNameNoLossError {
    Set(NameNotFoundError),
    Link(ExistingLinkErrors),
}

/// Error when replacing temporal measurement by index
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ReplaceLinkedTemporalByIndexError {
    Set(ReplaceTemporalByIndexError),
    Link(ExistingLinkErrors),
}

/// Error when replacing temporal measurement by index
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ReplaceLinkedTemporalByNameError {
    Set(ReplaceTemporalByNameError),
    Link(ExistingLinkErrors),
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

def_summary!(
    pub SetTemporalByNameSummary,
    "could not assign temporal measurement at name"
);

def_summary!(
    pub SetTemporalByIndexSummary,
    "could not assign temporal measurement at index"
);

/// Error when temporal type is assigned to optical measurement and vice versa.
#[derive(Debug, Error, new, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct MeasMismatchError {
    key_is_optical: KeyIsOptical,
    index: MeasIndex,
}

// TODO this error is confusing for any temporal type which is not unit
impl fmt::Display for MeasMismatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let k = self.index;
        if self.key_is_optical.0 {
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

impl_ref_specific_rw!(
    RootMeta,
    InnerRootMeta2_0,
    Mode,
    Cyt,
    Timestamps2_0,
    AppliedGates2_0
);

impl_ref_specific_rw!(
    RootMeta,
    InnerRootMeta3_0,
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
    RootMeta,
    InnerRootMeta3_1,
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
    RootMeta,
    InnerRootMeta3_2,
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

impl_ref_specific_ro!(
    RootMeta,
    InnerRootMeta2_0,
    Option<FCSDate>,
    Option<Compensation2_0>
);

impl_ref_specific_ro!(
    RootMeta,
    InnerRootMeta3_0,
    Option<FCSDate>,
    Option<Compensation3_0>,
    AppliedGates3_0
);

impl_ref_specific_ro!(RootMeta, InnerRootMeta3_1, Option<FCSDate>, AppliedGates3_0);

impl_ref_specific_ro!(
    RootMeta,
    InnerRootMeta3_2,
    Option<FCSDate>,
    Option<BeginDateTime>,
    Option<EndDateTime>,
    UnstainedCenters,
    AppliedGates3_2
);

impl<X, M, const IS_ETIM: bool> AsRef<Option<Xtim<IS_ETIM, X>>> for RootMeta<M>
where
    Self: AsRef<Timestamps<X>>,
    Timestamps<X>: AsRef<Option<Xtim<IS_ETIM, X>>>,
{
    fn as_ref(&self) -> &Option<Xtim<IS_ETIM, X>> {
        self.as_ref().as_ref()
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

impl HasCompensation for InnerRootMeta2_0 {
    type Comp = Compensation2_0;

    fn comp_mut(&mut self, _: private::NoTouchy) -> &mut Option<Self::Comp> {
        &mut self.comp
    }
}

impl HasCompensation for InnerRootMeta3_0 {
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

impl HasSpillover for InnerRootMeta3_1 {
    fn spill_mut(&mut self, _: private::NoTouchy) -> &mut Option<Spillover> {
        &mut self.spillover
    }
}

impl HasSpillover for InnerRootMeta3_2 {
    fn spill_mut(&mut self, _: private::NoTouchy) -> &mut Option<Spillover> {
        &mut self.spillover
    }
}

// Implement private mutable access for $UNSTAINEDCENTERS (3.2)

pub trait HasUnstainedCenters {
    // private as_mut
    fn unstainedcenters_mut(&mut self, _: private::NoTouchy) -> &mut UnstainedCenters;
}

impl HasUnstainedCenters for InnerRootMeta3_2 {
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

impl HasAppliedGates for InnerRootMeta3_0 {
    type Gates = AppliedGates3_0;
    fn applied_gates_mut(&mut self, _: private::NoTouchy) -> &mut Self::Gates {
        &mut self.applied_gates
    }
}

impl HasAppliedGates for InnerRootMeta3_1 {
    type Gates = AppliedGates3_0;
    fn applied_gates_mut(&mut self, _: private::NoTouchy) -> &mut Self::Gates {
        &mut self.applied_gates
    }
}

impl HasAppliedGates for InnerRootMeta3_2 {
    type Gates = AppliedGates3_2;
    fn applied_gates_mut(&mut self, _: private::NoTouchy) -> &mut Self::Gates {
        &mut self.applied_gates
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

impl_versioned!(InnerRootMeta2_0, Version2_0);
impl_versioned!(InnerRootMeta3_0, Version3_0);
impl_versioned!(InnerRootMeta3_1, Version3_1);
impl_versioned!(InnerRootMeta3_2, Version3_2);
impl_versioned!(InnerOptical2_0, Version2_0);
impl_versioned!(InnerOptical3_0, Version3_0);
impl_versioned!(InnerOptical3_1, Version3_1);
impl_versioned!(InnerOptical3_2, Version3_2);
impl_versioned!(InnerTemporal2_0, Version2_0);
impl_versioned!(InnerTemporal3_0, Version3_0);
impl_versioned!(InnerTemporal3_1, Version3_1);
impl_versioned!(InnerTemporal3_2, Version3_2);

// Implement mapping between FCS version and all metadata types

pub trait VersionSet: VersionMeasSet {
    type RootMeta: VersionedRootMeta;
    type Offsets: LookupTEXTOffsets<TotDef = <Self::DataSchema as VersionedDataSchema>::Tot>;
}

macro_rules! impl_version_set {
    ($v:ident, $m:path,  $ofs:path) => {
        impl VersionSet for $v {
            type RootMeta = $m;
            type Offsets = $ofs;
        }
    };
}

impl_version_set!(Version2_0, InnerRootMeta2_0, TEXTOffsets2_0);
impl_version_set!(Version3_0, InnerRootMeta3_0, TEXTOffsets3_0);
impl_version_set!(Version3_1, InnerRootMeta3_1, TEXTOffsets3_0);
impl_version_set!(Version3_2, InnerRootMeta3_2, TEXTOffsets3_2);

// Implement misc methods for a given version
//
// Used to keep messy functions out of public API

#[derive(new)]
pub(crate) struct LookupFlatDatasetOutput {
    pub(crate) df: PrimitiveDataFrame,
    pub(crate) analysis: Analysis,
    pub(crate) ds_offsets: DatasetOffsets,
    pub(crate) event_diag: EventsDiagnostics,
    pub(crate) schema_diag: DataSchemaDiagnostics,
    pub(crate) repair_diag: RepairDiagnostics,
}

pub(crate) trait PrivVersionSet: VersionSet {
    fn h_lookup_and_read<C, R>(
        h: &mut BufReader<R>,
        kws: &mut ValidKeywords,
        hns: &mut HeaderAndSuppOffsets,
        st: &TEXTReadState<C>,
    ) -> WarningsAndIOGroupResult<
        LookupFlatDatasetOutput,
        LookupAndReadDataAnalysisWarning,
        LookupAndReadDataAnalysisError,
        (),
    >
    where
        <Self::DataSchema as DataSchemaToEmptyDataFrame>::DfTarget:
            Into<PrimitiveDataFrame> + DataFrameCheckRanges,
        R: Read + Seek,
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadDatasetConfig> + AsRef<ReadOffsetConfig>,
    {
        #[derive(AsRef)]
        struct LookupConfig {
            #[as_ref(EvaledReadDataKeywordsConfig)]
            data_kws: EvaledReadDataKeywordsConfig,
            #[as_ref(ReadDatasetConfig)]
            dataset: ReadDatasetConfig,
            #[as_ref(ReadOffsetConfig)]
            offsets: ReadOffsetConfig,
        }

        AsRef::<ReadDataKeywordsConfig>::as_ref(st.conf())
            .eval(kws)
            .map_ok_value(|data_kws| {
                st.as_ref().first_once(|conf| LookupConfig {
                    data_kws,
                    dataset: *AsRef::<ReadDatasetConfig>::as_ref(&conf),
                    offsets: *AsRef::<ReadOffsetConfig>::as_ref(&conf),
                })
            })
            .map_errors(LookupAndReadDataAnalysisError::from)
            .nowarn_into_warn()
            .group()
            .map_error(IOErrorGroup::Pure)
            .and_then_commutative(|lst| {
                // Repair the keyword list before doing anything.
                let repair_res = kws
                    .repair(&lst.conf().data_kws)
                    .map_commutative_warnings(LookupAndReadDataAnalysisWarning::from)
                    .map_errors(LookupAndReadDataAnalysisError::from)
                    .into_semigroup();

                let layout_res = Par::get_metaroot_req(&kws.std)
                    .map_err(LookupAndReadDataAnalysisError::from)
                    .into_log()
                    .and_then_commutative(|par| {
                        Self::DataSchema::lookup_ro(&kws.std, par, lst.conf().as_ref())
                            .map_commutative_warnings(LookupAndReadDataAnalysisWarning::from)
                            .map_errors(LookupAndReadDataAnalysisError::from)
                    });

                let offset_res = Self::Offsets::lookup_ro(&kws.std, hns, &lst)
                    .map_commutative_warnings(LookupAndReadDataAnalysisWarning::from)
                    .map_errors(LookupAndReadDataAnalysisError::from);

                layout_res
                    .zip3_commutative(offset_res, repair_res)
                    .group()
                    .map_error(IOErrorGroup::Pure)
                    .and_then_commutative(|(mut layout_out, mut offsets, repair_diag)| {
                        let ar = AnalysisReader::new(offsets.offsets.final_analysis);
                        layout_out
                            .data_schema
                            .h_read_df(
                                h,
                                offsets.tot,
                                &mut offsets.offsets.final_data,
                                lst.conf().as_ref(),
                            )
                            .map_commutative_warnings(LookupAndReadDataAnalysisWarning::from)
                            .map_pure_errors(LookupAndReadDataAnalysisError::from)
                            .and_then_commutative(|df_out| {
                                ar.h_read(h)
                                    .map(|a| {
                                        LookupFlatDatasetOutput::new(
                                            df_out.inner.into(),
                                            a,
                                            offsets.offsets,
                                            df_out.diagnostics,
                                            layout_out.diagnostics,
                                            repair_diag,
                                        )
                                    })
                                    .map_err(IOErrorGroup::from)
                                    .into_log()
                            })
                    })
            })
    }
}

impl PrivVersionSet for Version2_0 {}
impl PrivVersionSet for Version3_0 {}
impl PrivVersionSet for Version3_1 {}
impl PrivVersionSet for Version3_2 {}

// Implement method to look up root keywords from a hash table

pub trait LookupMetaroot<N>: Sized {
    fn lookup_specific<C>(
        kws: &mut ValidKeywords,
        dropped: &mut StdKeywords,
        ms: &[N],
        conf: &C,
    ) -> LookupMetarootResult<DiagnosedMetaroot<Self>>
    where
        C: AsRef<EvaledReadDataKeywordsConfig> + AsRef<EvaledReadStdKeywordsConfig>;
}

impl LookupMetaroot<Option<Shortname>> for InnerRootMeta2_0 {
    fn lookup_specific<C>(
        kws: &mut ValidKeywords,
        dropped: &mut StdKeywords,
        ms: &[Option<Shortname>],
        conf: &C,
    ) -> LookupMetarootResult<DiagnosedMetaroot<Self>>
    where
        C: AsRef<EvaledReadDataKeywordsConfig> + AsRef<EvaledReadStdKeywordsConfig>,
    {
        let par = Par(ms.len());
        let comp = Compensation2_0::lookup(kws, dropped, par, conf.as_ref())
            .map_switchable_errors(LookupMetarootWarning::from)
            .switchable_into_commutative();
        let cyt = Cyt::remove_root_opt_nofail(&mut kws.std);
        let ts = Timestamps::lookup(kws, dropped, conf)
            .map_warnings_and_errors(LookupMetarootWarning::from);
        let ag = AppliedGates2_0::lookup(kws, dropped, conf)
            .map_warnings_and_errors(LookupMetarootWarning::from);
        let mode = Mode::remove_metaroot_req(&mut kws.std)
            .map_err(LookupMetarootError::from)
            .into_log();
        comp.zip3_commutative(ts, ag)
            .map_errors(LookupMetarootError::from)
            .zip_commutative(mode)
            .map_ok_value(|((c, t, g), m)| {
                let diag = MetarootDiagnostics::new(
                    vec![],
                    g.diagnostic,
                    SpilloverDiagnostics::default(),
                    t.diagnostic,
                    DatetimesDiagnostics::default(),
                    None,
                );
                DiagnosedMetaroot::new(Self::new(m, cyt, c, t.inner, g.inner), diag)
            })
    }
}

impl LookupMetaroot<Option<Shortname>> for InnerRootMeta3_0 {
    fn lookup_specific<C>(
        kws: &mut ValidKeywords,
        dropped: &mut StdKeywords,
        _: &[Option<Shortname>],
        conf: &C,
    ) -> LookupMetarootResult<DiagnosedMetaroot<Self>>
    where
        C: AsRef<EvaledReadDataKeywordsConfig> + AsRef<EvaledReadStdKeywordsConfig>,
    {
        macro_rules! go {
            ($x:expr) => {
                $x.map_switchable_errors(LookupMetarootWarning::from)
                    .switchable_into_commutative()
                    .into_semigroup()
            };
        }

        let cyt = Cyt::remove_root_opt_nofail(&mut kws.std);
        let cytsn = Cytsn::remove_root_opt_nofail(&mut kws.std);

        let comp = Compensation3_0::remove_or_drop_root_opt_with(kws, dropped, (), conf);
        let uni = Unicode::remove_or_drop_root_opt_with(kws, dropped, (), conf);

        let ts = Timestamps::lookup(kws, dropped, conf)
            .map_warnings_and_errors(LookupMetarootWarning::from);
        let subset = SubsetData::lookup(kws, dropped, conf.as_ref())
            .map_warnings_and_errors(LookupMetarootWarning::from);
        let ag = AppliedGates3_0::lookup(kws, dropped, conf)
            .map_warnings_and_errors(LookupMetarootWarning::from);

        let mode = Mode::remove_metaroot_req(&mut kws.std)
            .map_err(LookupMetarootError::from)
            .into_log();

        go!(comp)
            .zip5_commutative(subset, ts, go!(uni), ag)
            .map_errors(LookupMetarootError::from)
            .zip_commutative(mode)
            .map_ok_value(|((co_out, su, t, u_out, g), m)| {
                let (co, c_trimmed) = co_out.into_opt_root_pair();
                let (u, u_trimmed) = u_out.into_opt_root_pair();
                let ret = Self::new(m, cyt, co, t.inner, cytsn, u, su, g.inner);
                let trimmed = c_trimmed.into_iter().chain(u_trimmed).collect();
                let diag = MetarootDiagnostics::new(
                    trimmed,
                    g.diagnostic,
                    SpilloverDiagnostics::default(),
                    t.diagnostic,
                    DatetimesDiagnostics::default(),
                    None,
                );
                DiagnosedMetaroot::new(ret, diag)
            })
    }
}

impl LookupMetaroot<Identity<Shortname>> for InnerRootMeta3_1 {
    fn lookup_specific<C>(
        kws: &mut ValidKeywords,
        dropped: &mut StdKeywords,
        ms: &[Identity<Shortname>],
        conf: &C,
    ) -> LookupMetarootResult<DiagnosedMetaroot<Self>>
    where
        C: AsRef<EvaledReadDataKeywordsConfig> + AsRef<EvaledReadStdKeywordsConfig>,
    {
        let ordered_names: Vec<_> = ms.iter().map(|n| &n.0).collect();

        let cyt = Cyt::remove_root_opt_nofail(&mut kws.std);
        let cytsn = Cytsn::remove_root_opt_nofail(&mut kws.std);
        let plate = PlateData::lookup(&mut kws.std);

        let vol = Vol::remove_or_drop_root_opt(kws, dropped, conf.as_ref())
            .map_switchable_errors(LookupMetarootWarning::from)
            .switchable_into_commutative()
            .into_semigroup();

        let spill = Spillover::remove_or_drop_root_opt_with(kws, dropped, &ordered_names[..], conf)
            .map_switchable_errors(LookupMetarootWarning::from)
            .switchable_into_commutative()
            .into_semigroup();

        let subset = SubsetData::lookup(kws, dropped, conf.as_ref())
            .map_warnings_and_errors(LookupMetarootWarning::from);
        let ag = AppliedGates3_0::lookup(kws, dropped, conf)
            .map_warnings_and_errors(LookupMetarootWarning::from);
        let modif = ModificationData::lookup(kws, dropped, conf)
            .map_warnings_and_errors(LookupMetarootWarning::from);
        let ts = Timestamps::lookup(kws, dropped, conf)
            .map_warnings_and_errors(LookupMetarootWarning::from);

        let mode = Mode::remove_metaroot_req(&mut kws.std)
            .map_err(LookupMetarootError::from)
            .into_log();

        spill
            .zip6_commutative(subset, modif, ts, vol, ag)
            .map_errors(LookupMetarootError::from)
            .zip_commutative(mode)
            .map_ok_value(|((sp, su, md, t, v, g), m)| {
                let ret = Self::new(
                    m, cyt, t.inner, cytsn, sp.inner, md.inner, plate, v, su, g.inner,
                );
                let diag = MetarootDiagnostics::new(
                    vec![],
                    g.diagnostic,
                    sp.diagnostic,
                    t.diagnostic,
                    DatetimesDiagnostics::default(),
                    md.diagnostic,
                );
                DiagnosedMetaroot::new(ret, diag)
            })
    }
}

impl LookupMetaroot<Identity<Shortname>> for InnerRootMeta3_2 {
    fn lookup_specific<C>(
        kws: &mut ValidKeywords,
        dropped: &mut StdKeywords,
        ms: &[Identity<Shortname>],
        conf: &C,
    ) -> LookupMetarootResult<DiagnosedMetaroot<Self>>
    where
        C: AsRef<EvaledReadDataKeywordsConfig> + AsRef<EvaledReadStdKeywordsConfig>,
    {
        macro_rules! go {
            ($x:expr) => {
                $x.map_switchable_errors(LookupMetarootWarning::from)
                    .switchable_into_commutative()
                    .into_semigroup()
            };
        }

        let ordered_names: Vec<_> = ms.iter().map(|n| &n.0).collect();

        let flow = Flowrate::remove_root_opt_nofail(&mut kws.std);
        let cytsn = Cytsn::remove_root_opt_nofail(&mut kws.std);
        let plate = PlateData::lookup(&mut kws.std);
        let carrier = CarrierData::lookup(&mut kws.std);

        let mode = go!(Mode3_2::remove_or_drop_root_opt(
            kws,
            dropped,
            conf.as_ref()
        ));
        let us = go!(UnstainedData::lookup(kws, dropped, conf));
        let vol = go!(Vol::remove_or_drop_root_opt(kws, dropped, conf.as_ref()));
        let spill = go!(Spillover::remove_or_drop_root_opt_with(
            kws,
            dropped,
            &ordered_names[..],
            conf
        ));

        let modif = ModificationData::lookup(kws, dropped, conf)
            .map_warnings_and_errors(LookupMetarootWarning::from);
        let ts = Timestamps::lookup(kws, dropped, conf)
            .map_warnings_and_errors(LookupMetarootWarning::from);
        let dt = Datetimes::lookup(kws, dropped, conf)
            .map_warnings_and_errors(LookupMetarootWarning::from);
        let agates = AppliedGates3_2::lookup(kws, dropped, conf)
            .map_warnings_and_errors(LookupMetarootWarning::from);

        let cyt = Cyt3_2::remove_metaroot_req(&mut kws.std)
            .map_err(LookupMetarootError::from)
            .into_log();

        dt.zip4_commutative(modif, mode, spill)
            .zip5_commutative(ts, us, vol, agates)
            .map_errors(LookupMetarootError::from)
            .zip_commutative(cyt)
            .map_ok_value(|(((d, md, mo, sp), t, u_out, v, ag), c)| {
                let ret = Self::new(
                    mo,
                    t.inner,
                    d.inner,
                    c,
                    sp.inner,
                    cytsn,
                    md.inner,
                    plate,
                    v,
                    carrier,
                    u_out.inner,
                    flow,
                    ag.inner,
                );
                let trimmed = u_out.diagnostic.into_iter().collect();
                let diag = MetarootDiagnostics::new(
                    trimmed,
                    ag.diagnostic,
                    sp.diagnostic,
                    t.diagnostic,
                    d.diagnostic,
                    md.diagnostic,
                );
                DiagnosedMetaroot::new(ret, diag)
            })
    }
}

// Implement common methods to lookup offset keywords from hash table

pub trait LookupTEXTOffsets: Sized {
    type TotDef: IsTot;

    fn lookup<C>(
        kws: &mut ValidKeywords,
        dropped: &mut StdKeywords,
        offsets: &mut HeaderAndSuppOffsets,
        st: &TEXTReadState<C>,
    ) -> LookupTEXTOffsetsResult<TEXTOffsets<Self::TotDef>>
    where
        C: AsRef<EvaledReadDataKeywordsConfig> + AsRef<ReadOffsetConfig>;

    fn lookup_ro<C>(
        std: &StdKeywords,
        offsets: &mut HeaderAndSuppOffsets,
        st: &TEXTReadState<C>,
    ) -> LookupTEXTOffsetsResult<TEXTOffsets<Self::TotDef>>
    where
        C: AsRef<EvaledReadDataKeywordsConfig> + AsRef<ReadOffsetConfig>;
}

impl LookupTEXTOffsets for TEXTOffsets2_0 {
    type TotDef = Option<Tot>;

    fn lookup<C>(
        kws: &mut ValidKeywords,
        dropped: &mut StdKeywords,
        offsets: &mut HeaderAndSuppOffsets,
        st: &TEXTReadState<C>,
    ) -> LookupTEXTOffsetsResult<TEXTOffsets<Self::TotDef>>
    where
        C: AsRef<EvaledReadDataKeywordsConfig> + AsRef<ReadOffsetConfig>,
    {
        Tot::remove_or_drop_root_opt(kws, dropped, st.conf().as_ref())
            .map_ok_value(|tot| {
                let s = offsets.header.final_offsets.as_dataset_offsets_2_0();
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
        offsets: &mut HeaderAndSuppOffsets,
        _: &TEXTReadState<C>,
    ) -> LookupTEXTOffsetsResult<TEXTOffsets<Self::TotDef>>
    where
        C: AsRef<EvaledReadDataKeywordsConfig> + AsRef<ReadOffsetConfig>,
    {
        let succ = Tot::get_root_opt(std)
            .map_err(LookupTEXTOffsetsWarning::from)
            .into_succ()
            .fmap_once(|tot| {
                let s = offsets.header.final_offsets.as_dataset_offsets_2_0();
                TEXTOffsets::new(s, tot)
            });
        LogResult::Succ(succ)
    }
}

macro_rules! lookup_offsets_3_0 {
    ($std:expr, $offsets:expr, $st:expr, $tot:ident, $lookup:ident) => {{
        let tot_res = Tot::$tot($std)
            .map_err(LookupTEXTOffsetsError::from)
            .into_log();
        let dconf: &EvaledReadDataKeywordsConfig = $st.conf().as_ref();
        let data_ignore = dconf.ignore_text_data_offsets;
        let data_corr = dconf.text_data_correction;
        let data_res = DataSegmentId::$lookup($std, $offsets, data_ignore, data_corr, $st)
            .map_commutative_warnings(LookupTEXTOffsetsWarning::from)
            .map_errors(LookupTEXTOffsetsError::from);
        let anal_ignore = dconf.ignore_text_analysis_offsets;
        let anal_corr = dconf.text_analysis_correction;
        let anal_res = AnalysisSegmentId::$lookup($std, $offsets, anal_ignore, anal_corr, $st)
            .map_commutative_warnings(LookupTEXTOffsetsWarning::from)
            .map_errors(LookupTEXTOffsetsError::from);
        tot_res
            .zip3_commutative(data_res, anal_res)
            .and_then_commutative(|(tot, d, a)| {
                let oconf: &ReadOffsetConfig = $st.conf().as_ref();
                let limit = oconf.overlap_correction_limit;
                DatasetOffsets::try_new(d, a, limit)
                    .map(|dos| TEXTOffsets::new(dos, Identity(tot)))
                    .map_err(LookupTEXTOffsetsError::from)
                    .into_log()
            })
    }};
}

impl LookupTEXTOffsets for TEXTOffsets3_0 {
    type TotDef = Identity<Tot>;

    fn lookup<C>(
        kws: &mut ValidKeywords,
        _: &mut StdKeywords,
        offsets: &mut HeaderAndSuppOffsets,
        st: &TEXTReadState<C>,
    ) -> LookupTEXTOffsetsResult<TEXTOffsets<Self::TotDef>>
    where
        C: AsRef<EvaledReadDataKeywordsConfig> + AsRef<ReadOffsetConfig>,
    {
        lookup_offsets_3_0!(
            &mut kws.std,
            offsets,
            st,
            remove_metaroot_req,
            remove_req_or
        )
    }

    fn lookup_ro<C>(
        std: &StdKeywords,
        offsets: &mut HeaderAndSuppOffsets,
        st: &TEXTReadState<C>,
    ) -> LookupTEXTOffsetsResult<TEXTOffsets<Self::TotDef>>
    where
        C: AsRef<EvaledReadDataKeywordsConfig> + AsRef<ReadOffsetConfig>,
    {
        lookup_offsets_3_0!(std, offsets, st, get_metaroot_req, get_req_or)
    }
}

macro_rules! lookup_offsets_3_2 {
    ($std:expr, $offsets:expr, $st:expr, $tot:ident, $lookup_req:ident, $lookup_opt:ident) => {{
        let tot_res = Tot::$tot($std)
            .map_err(LookupTEXTOffsetsError::from)
            .into_log();
        let dconf: &EvaledReadDataKeywordsConfig = $st.conf().as_ref();
        let data_corr = dconf.text_data_correction;
        let data_ignore = dconf.ignore_text_data_offsets;
        let data_res = DataSegmentId::$lookup_req($std, $offsets, data_ignore, data_corr, $st)
            .map_commutative_warnings(LookupTEXTOffsetsWarning::from)
            .map_errors(LookupTEXTOffsetsError::from);
        let anal_corr = dconf.text_analysis_correction;
        let anal_ignore = dconf.ignore_text_analysis_offsets;
        let anal_res = AnalysisSegmentId::$lookup_opt($std, $offsets, anal_ignore, anal_corr, $st)
            .map_commutative_warnings(LookupTEXTOffsetsWarning::from)
            .map_errors(LookupTEXTOffsetsError::from);
        tot_res
            .zip3_commutative(data_res, anal_res)
            .and_then_commutative(|(tot, d, a)| {
                let oconf: &ReadOffsetConfig = $st.conf().as_ref();
                let limit = oconf.overlap_correction_limit;
                DatasetOffsets::try_new(d, a, limit)
                    .map(|dos| TEXTOffsets::new(dos, Identity(tot)))
                    .map_err(LookupTEXTOffsetsError::from)
                    .into_log()
            })
    }};
}

impl LookupTEXTOffsets for TEXTOffsets3_2 {
    type TotDef = Identity<Tot>;

    fn lookup<C>(
        kws: &mut ValidKeywords,
        _: &mut StdKeywords,
        offsets: &mut HeaderAndSuppOffsets,
        st: &TEXTReadState<C>,
    ) -> LookupTEXTOffsetsResult<TEXTOffsets<Self::TotDef>>
    where
        C: AsRef<EvaledReadDataKeywordsConfig> + AsRef<ReadOffsetConfig>,
    {
        lookup_offsets_3_2!(
            &mut kws.std,
            offsets,
            st,
            remove_metaroot_req,
            remove_req_or,
            remove_opt_or
        )
    }

    fn lookup_ro<C>(
        std: &StdKeywords,
        offsets: &mut HeaderAndSuppOffsets,
        st: &TEXTReadState<C>,
    ) -> LookupTEXTOffsetsResult<TEXTOffsets<Self::TotDef>>
    where
        C: AsRef<EvaledReadDataKeywordsConfig> + AsRef<ReadOffsetConfig>,
    {
        lookup_offsets_3_2!(std, offsets, st, get_metaroot_req, get_req_or, get_opt_or)
    }
}

// Implement method to convert root keyword values between versions

pub trait ConvertFromMetaroot<M: VersionedRootMeta>: Sized + VersionedRootMeta {
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

impl ConvertFromMetaroot<InnerRootMeta3_0> for InnerRootMeta2_0 {
    fn convert_from_metaroot_inner(
        value: InnerRootMeta3_0,
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

impl ConvertFromMetaroot<InnerRootMeta3_1> for InnerRootMeta2_0 {
    fn convert_from_metaroot_inner(
        value: InnerRootMeta3_1,
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

impl ConvertFromMetaroot<InnerRootMeta3_2> for InnerRootMeta2_0 {
    fn convert_from_metaroot_inner(
        value: InnerRootMeta3_2,
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

impl ConvertFromMetaroot<InnerRootMeta2_0> for InnerRootMeta3_0 {
    fn convert_from_metaroot_inner(
        value: InnerRootMeta2_0,
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

impl ConvertFromMetaroot<InnerRootMeta3_1> for InnerRootMeta3_0 {
    fn convert_from_metaroot_inner(
        value: InnerRootMeta3_1,
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

impl ConvertFromMetaroot<InnerRootMeta3_2> for InnerRootMeta3_0 {
    fn convert_from_metaroot_inner(
        value: InnerRootMeta3_2,
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

impl ConvertFromMetaroot<InnerRootMeta2_0> for InnerRootMeta3_1 {
    fn convert_from_metaroot_inner(
        value: InnerRootMeta2_0,
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

impl ConvertFromMetaroot<InnerRootMeta3_0> for InnerRootMeta3_1 {
    fn convert_from_metaroot_inner(
        value: InnerRootMeta3_0,
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

impl ConvertFromMetaroot<InnerRootMeta3_2> for InnerRootMeta3_1 {
    fn convert_from_metaroot_inner(
        value: InnerRootMeta3_2,
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

impl ConvertFromMetaroot<InnerRootMeta2_0> for InnerRootMeta3_2 {
    fn convert_from_metaroot_inner(
        value: InnerRootMeta2_0,
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

impl ConvertFromMetaroot<InnerRootMeta3_0> for InnerRootMeta3_2 {
    fn convert_from_metaroot_inner(
        value: InnerRootMeta3_0,
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

impl ConvertFromMetaroot<InnerRootMeta3_1> for InnerRootMeta3_2 {
    fn convert_from_metaroot_inner(
        value: InnerRootMeta3_1,
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

// Implement common methods to manipulate root keywords

pub trait VersionedRootMeta: Sized + Versioned {
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

    /// Update linked indices in keywords after inserting a new measurement.
    ///
    /// Everything after `index` must be decremented by 1.
    ///
    /// Caller is assumed to have checked that nothing points to `i`.
    fn remove_meas_index_inner(&mut self, i: MeasIndex);

    fn keywords_req_inner(&self) -> impl Iterator<Item = ReqRootKeyword<'_>>;

    fn keywords_opt_inner(&self) -> impl Iterator<Item = OptRootKeyword<'_>>;
}

impl VersionedRootMeta for InnerRootMeta2_0 {
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

    fn remove_meas_index_inner(&mut self, _: MeasIndex) {
        assert!(
            self.comp.is_none(),
            "tried to remove indices while $COMP present"
        );
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

impl VersionedRootMeta for InnerRootMeta3_0 {
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

    fn remove_meas_index_inner(&mut self, i: MeasIndex) {
        assert!(
            self.comp.is_none(),
            "tried to remove indices while $COMP present"
        );
        self.applied_gates.shift_meas_indices_after_remove(i);
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

impl VersionedRootMeta for InnerRootMeta3_1 {
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

    fn remove_meas_index_inner(&mut self, i: MeasIndex) {
        self.applied_gates.shift_meas_indices_after_remove(i);
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

impl VersionedRootMeta for InnerRootMeta3_2 {
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

    fn remove_meas_index_inner(&mut self, i: MeasIndex) {
        self.applied_gates.shift_meas_indices_after_remove(i);
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

// Implement methods for root keyword type

impl<M: VersionedRootMeta> RootMeta<M> {
    fn try_convert<ToM: ConvertFromMetaroot<M>>(
        self,
        flag: AllowLoss,
    ) -> MetarootConvertResult<RootMeta<ToM>> {
        ToM::convert_from_metaroot(self.specific, flag).map_ok_value(|specific| {
            RootMeta::new(
                self.abrt, self.com, self.cells, self.exp, self.fil, self.inst, self.lost, self.op,
                self.proj, self.smno, self.src, self.sys, self.tr, specific,
            )
        })
    }

    fn lookup_metaroot<C, N>(
        kws: &mut ValidKeywords,
        dropped: &mut StdKeywords,
        ms: &[N],
        conf: &C,
    ) -> LookupMetarootResult<DiagnosedMetaroot<Self>>
    where
        M: LookupMetaroot<N>,
        C: AsRef<EvaledReadDataKeywordsConfig> + AsRef<EvaledReadStdKeywordsConfig>,
    {
        macro_rules! go {
            ($x:expr) => {
                $x.map_switchable_errors(LookupMetarootWarning::from)
                    .switchable_into_commutative()
                    .map_errors(LookupMetarootError::from)
                    .into_semigroup()
            };
        }
        let com = Com::remove_root_opt_nofail(&mut kws.std);
        let cells = Cells::remove_root_opt_nofail(&mut kws.std);
        let exp = Exp::remove_root_opt_nofail(&mut kws.std);
        let fil = Fil::remove_root_opt_nofail(&mut kws.std);
        let inst = Inst::remove_root_opt_nofail(&mut kws.std);
        let op = Op::remove_root_opt_nofail(&mut kws.std);
        let proj = Proj::remove_root_opt_nofail(&mut kws.std);
        let smno = Smno::remove_root_opt_nofail(&mut kws.std);
        let src = Src::remove_root_opt_nofail(&mut kws.std);
        let sys = Sys::remove_root_opt_nofail(&mut kws.std);

        let abrt_res = Abrt::remove_or_drop_root_opt(kws, dropped, conf.as_ref());
        let lost_res = Lost::remove_or_drop_root_opt(kws, dropped, conf.as_ref());
        let tr_res = Trigger::remove_or_drop_root_opt_with(kws, dropped, (), conf);

        let spec_res = M::lookup_specific(kws, dropped, ms, conf);

        go!(abrt_res)
            .zip4_commutative(go!(lost_res), go!(tr_res), spec_res)
            .map_ok_value(|(abrt, lost, tr_out, mut meta)| {
                let (tr, tr_trimmed) = tr_out.into_opt_root_pair();
                meta.diagnostic.trimmed.extend(tr_trimmed);
                meta.first_once(|native| {
                    Self::new(
                        abrt, com, cells, exp, fil, inst, lost, op, proj, smno, src, sys, tr,
                        native,
                    )
                })
            })
    }

    fn req_keywords(&self, par: Par) -> impl Iterator<Item = ReqRootKeyword<'_>> {
        once(ReqRootKeyword::from_value(par)).chain(self.specific.keywords_req_inner())
    }

    fn opt_root_keywords(&self) -> impl Iterator<Item = OptRootKeyword<'_>> {
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
        [x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12]
            .into_iter()
            .flatten()
            .chain(self.specific.keywords_opt_inner())
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
        nonstandard_keywords: &mut NonStdKeywords,
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
                x.insert_keyvals(nonstandard_keywords);
            }
            x.push_errors(&mut es);
        }
        es
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
    fn new_meas_link_errors<N, X0, X1, X2>(
        &self,
        cur_meas: &MeasMeta<N, X0, X1, X2>,
        new_meas: &MeasMeta<N, X0, X1, X2>,
        allow_shared_names: bool,
        skip_index_check: bool,
    ) -> Result<(), SetMeasurementLinkErrors>
    where
        N: MightHave<Shortname>,
    {
        let n = cur_meas.len();
        assert_eq_msg!(n, new_meas.len(), "current measurement", "new measurements");
        let (js, ns) = cur_meas.all_indices_and_names_to_remove();
        let s = &self.specific;
        let named_errs: Vec<_> = if allow_shared_names {
            // If name sharing is allowed, treat this as if keywords that have
            // references ($SPILLOVER, etc) are being added to the new
            // measurement, in which case we only need to ensure that links in
            // the final configuration match
            let nset = new_meas.named_set();
            self.invalid_named_links(&nset)
                .map(SetMeasurementLinkError::from)
                .collect()
        } else {
            // If name sharing is not allowed, act as if all measurement will be
            // unset and replaced in two discrete steps, and any existing links
            // will be broken after the unset step. This effectively means we
            // can't have any links.
            self.meas_has_existing_named_links_with(&ns)
                .map(SetMeasurementLinkError::from)
                .collect()
        };
        let par = n.into();
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

// Implement methods for Core*

impl CoreTEXT2_0 {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn try_new_2_0(
        measurements: TemporalsAndOpticalsWithScale2_0,
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
                    InnerRootMeta2_0::new(mode, cyt, comp.map(Into::into), ts, applied_gates);
                let metaroot = RootMeta::new(
                    abrt, com, cells, exp, fil, inst, lost, op, proj, smno, src, sys, tr, specific,
                );
                Self::try_new_nodrop(metaroot, measurements, data_schema, nonstandard_keywords)
                    .map_errors(NewCoreTEXTError::from)
            })
    }
}

impl CoreTEXT3_0 {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn try_new_3_0(
        measurements: TemporalsAndOpticalsWithScale3_0,
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
                let specific = InnerRootMeta3_0::new(
                    mode,
                    cyt,
                    comp.map(Into::into),
                    ts,
                    cytsn,
                    unicode,
                    subset,
                    applied_gates,
                );
                let metaroot = RootMeta::new(
                    abrt, com, cells, exp, fil, inst, lost, op, proj, smno, src, sys, tr, specific,
                );
                Self::try_new_nodrop(metaroot, measurements, data_schema, nonstandard_keywords)
                    .map_errors(NewCoreTEXTError::from)
            })
    }
}

impl CoreTEXT3_1 {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn try_new_3_1(
        measurements: TemporalsAndOpticalsWithScale3_1,
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
                let specific = InnerRootMeta3_1::new(
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
                let metaroot = RootMeta::new(
                    abrt, com, cells, exp, fil, inst, lost, op, proj, smno, src, sys, tr, specific,
                );
                Self::try_new_nodrop(metaroot, measurements, data_schema, nonstandard_keywords)
                    .map_errors(NewCoreTEXTError::from)
            })
    }
}

impl CoreTEXT3_2 {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn try_new_3_2(
        measurements: TemporalsAndOpticalsWithScale3_2,
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
                let specific = InnerRootMeta3_2::new(
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
                let metaroot = RootMeta::new(
                    abrt, com, cells, exp, fil, inst, lost, op, proj, smno, src, sys, tr, specific,
                );
                Self::try_new_nodrop(metaroot, measurements, data_schema, nonstandard_keywords)
                    .map_errors(NewCoreTEXTError::from)
            })
    }
}

impl<Anal, Layout, Other, Root, Tmp, Opt, Scale, Name, Ver>
    Core<Anal, Layout, Other, Root, Tmp, Opt, Scale, Name, Ver>
{
    /// Return $PAR, which is simply the number of measurements in this struct
    pub fn par(&self) -> Par {
        Par(self.meas.meta().len())
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
        let fil = conf
            .inner
            .override_fil
            .is_set()
            .then(|| path_to_ne_string(path))
            .flatten();
        self.h_write_text(&mut h, &conf.inner, conf.multi.appendable, fil)
    }

    /// Write this core structure (HEADER+TEXT) to a handle
    pub fn h_write_text<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        conf: &WriteTEXTInnerConfig,
        has_nextdata: AppendableFlag,
        fil: Option<NEString>,
    ) -> Result<Nextdata, ImpureError<WriteTEXTHeaderError>>
    where
        L: LayoutKeywords + LayoutOptMeasKeywords,
    {
        let d = conf.delim;
        let c = conf.compute_crc;
        if conf.big_other.is_set() {
            self.h_write_text_inner1::<_, UintSpacePad20>(h, d, has_nextdata, c, fil)
        } else {
            self.h_write_text_inner1::<_, UintSpacePad8>(h, d, has_nextdata, c, fil)
        }
    }

    fn h_write_text_inner1<W: Write, T>(
        &self,
        h: &mut BufWriter<W>,
        delim: TEXTDelim,
        has_nextdata: AppendableFlag,
        compute_crc: ComputeWriteCRC,
        fil: Option<NEString>,
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
        let conf = WriteHeaderAndTextConfig::new_nodata(delim, has_nextdata, fil);
        let mut digest = WriteFCSDigest::new(compute_crc, V::as_version());
        let nextdata = self.h_write_text_inner::<_, T>(h, &conf, &mut digest)?;
        digest.write_final(h)?;
        Ok(nextdata)
    }

    fn h_write_text_inner<W: Write, T>(
        &self,
        h: &mut BufWriter<W>,
        conf: &WriteHeaderAndTextConfig<'_>,
        digest: &mut WriteFCSDigest,
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
        hdr_kws.h_write(h, V::as_version(), conf.other_segs, digest)?;
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
        let opt_root = self.opt_std_and_nonstd_keywords().map(|x| x.as_str_pair());
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
        let ns = self.meas.meta().named_set();
        tr.as_ref()
            .and_then(|t| t.invalid_link_error(&ns))
            .map_or(Ok(()), Err)?;
        self.rootmeta.tr = tr;
        Ok(())
    }

    /// Set threshold for $TR keyword
    ///
    /// Return true if trigger exists, false otherwise.
    pub fn set_trigger_threshold(&mut self, x: u32) -> bool {
        if let Some(tr) = self.rootmeta.tr.as_mut() {
            tr.threshold = x;
            true
        } else {
            false
        }
    }

    /// Return a list of measurement names as stored in $PnN.
    pub fn shortnames_maybe(&self) -> Vec<Option<&Shortname>> {
        self.meas
            .meta()
            .iter()
            .map(|x| x.both(|t| Some(&t.key), |m| V::Name::as_opt(&m.key)))
            .collect()
    }

    /// Return a list of measurement names as stored in $PnN
    ///
    /// For cases where $PnN is optional and its value is not given, this will
    /// return "Pn" where "n" is the parameter index starting at 1.
    pub fn all_shortnames(&self) -> Vec<Shortname> {
        self.meas.meta().iter_all_names().collect()
    }

    /// Set all $PnN keywords to list of names.
    ///
    /// The length of the names must match the number of measurements. Any
    /// keywords refering to the old names will be updated to reflect the new
    /// names. For 2.0 and 3.0 which have optional $PnN, all $PnN will end up
    /// being set.
    pub fn set_all_shortnames(&mut self, ns: Vec<Shortname>) -> Result<NameMapping, SetNamesError> {
        let mapping = self.meas.set_all_shortnames(ns)?;
        self.rootmeta.rename_meas_links(&mapping);
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
        let mapping = self.meas.set_measurement_shortnames_maybe(ns)?;
        self.rootmeta.rename_meas_links(&mapping);
        Ok(mapping)
    }

    /// Set the measurement matching given name to be the time measurement.
    pub fn set_temporal(
        &mut self,
        name: &Shortname,
        timestep: <V::Temporal as TemporalFromOptical<V::Optical>>::TData,
        allow_loss: AllowLoss,
    ) -> WarningAndGroupResult<
        bool,
        SetTemporalError,
        SetLinkedTemporalByNameError,
        SetTemporalByNameSummary,
    >
    where
        V::Temporal: TemporalFromOptical<V::Optical>,
        V::Optical: SwapOpticalWithTemporal<V::Temporal>,
    {
        self.name_has_existing_links(name)
            .map_err(SetLinkedTemporalByNameError::from)
            .into_nowarn()
            .nowarn_and_then(|()| {
                self.meas
                    .set_temporal(name, timestep, allow_loss)
                    .map_errors(SetLinkedTemporalByNameError::from)
            })
            .group()
    }

    /// Set the measurement at given index to the time measurement.
    pub fn set_temporal_at(
        &mut self,
        index: MeasIndex,
        timestep: <V::Temporal as TemporalFromOptical<V::Optical>>::TData,
        allow_loss: AllowLoss,
    ) -> WarningAndGroupResult<
        bool,
        SetTemporalError,
        SetLinkedTemporalByIndexError,
        SetTemporalByIndexSummary,
    >
    where
        V::Temporal: TemporalFromOptical<V::Optical>,
        V::Optical: SwapOpticalWithTemporal<V::Temporal>,
    {
        self.index_has_existing_links(index)
            .map_err(SetLinkedTemporalByIndexError::from)
            .into_nowarn()
            .nowarn_and_then(|()| {
                self.meas
                    .set_temporal_at(index, timestep, allow_loss)
                    .map_errors(SetLinkedTemporalByIndexError::from)
            })
            .group()
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
        V::Temporal: TemporalMaybeToOptical<Warning = Nothing<()>, Error = Infallible>,
    {
        self.meas
            .unset_temporal(|i, old_t| V::Optical::from_temporal(old_t, i, ()))
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
        V::Temporal: TemporalMaybeToOptical<
                Warning = Option<AnyTemporalToOpticalKeyLossError>,
                Error = AnyTemporalToOpticalKeyLossError,
            >,
    {
        self.meas.unset_temporal(|i, old_t| {
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
        name: V::Name,
        opt: Optical<V::Optical>,
        scale: V::OpticalScale,
        data_column: C,
    ) -> GroupResult<Shortname, PushOpticalError<<L as LayoutInsert<C>>::Error>, PushOpticalSummary>
    where
        L: LayoutInsert<C> + LayoutInsertScaleCheck<C>,
    {
        self.push_optical_inner(name, opt, scale, data_column)
            .group()
            .resolve_nowarn()
    }

    /// Add optical measurement at a given position
    ///
    /// Return error if name is non-unique, range is incompatible, or index is
    /// out of bounds.
    pub fn insert_optical<C>(
        &mut self,
        i: MeasIndex,
        name: V::Name,
        opt: Optical<V::Optical>,
        scale: V::OpticalScale,
        data_column: C,
    ) -> GroupResult<
        Shortname,
        InsertOpticalError<<L as LayoutInsert<C>>::Error>,
        InsertOpticalSummary,
    >
    where
        L: LayoutInsert<C> + LayoutInsertScaleCheck<C>,
    {
        self.insert_optical_inner(i, name, opt, scale, data_column)
            .group()
            .resolve_nowarn()
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
    ) -> Result<VTemporalOrOpticalWithScale<V>, ElementIndexError> {
        self.meas.replace_optical_at(index, m)
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
    ) -> Result<VTemporalOrOpticalWithScale<V>, NameNotFoundError> {
        self.meas.replace_optical_named(name, m)
    }

    /// Replace position at index with a temporal value.
    pub fn replace_temporal_at(
        &mut self,
        index: MeasIndex,
        m: Temporal<V::Temporal>,
    ) -> Result<VTemporalOrOpticalWithScale<V>, ReplaceTemporalByIndexNoLossError>
    where
        V::Optical: OpticalFromTemporal<V::Temporal, LossFlag = ()>,
        V::Temporal: TemporalMaybeToOptical<Warning = Nothing<()>, Error = Infallible>,
    {
        self.index_has_existing_links(index)?;
        let ret = self.meas.replace_temporal_at_nofail(index, m, |i, old_t| {
            V::Optical::from_temporal(old_t, i, ())
                .set_err_value(())
                .infallible_nowarn_into()
                .0
        })?;
        Ok(ret)
    }

    /// Replace position at index with a temporal value where conversion cannot fail.
    pub fn replace_temporal_at_lossy(
        &mut self,
        index: MeasIndex,
        m: Temporal<V::Temporal>,
        allow_loss: AllowLoss,
    ) -> WarningOrErrorResult<
        VTemporalOrOpticalWithScale<V>,
        (),
        AnyTemporalToOpticalKeyLossError,
        ReplaceLinkedTemporalByIndexError,
    >
    where
        V::Optical: OpticalFromTemporal<V::Temporal, LossFlag = AllowLoss>,
        V::Temporal: TemporalMaybeToOptical<
                Warning = Option<AnyTemporalToOpticalKeyLossError>,
                Error = AnyTemporalToOpticalKeyLossError,
            >,
    {
        self.index_has_existing_links(index)
            .map_err(ReplaceLinkedTemporalByIndexError::from)
            .into_nowarn1()
            .nowarn_and_then(|()| {
                self.meas
                    .replace_temporal_at(index, m, |i, old_t| {
                        V::Optical::from_temporal(old_t, i, allow_loss)
                            .switchable_into_non_commutative()
                            .map_ok_value(|(x, _)| x)
                            .map_errors(ReplaceTemporalByIndexError::from)
                    })
                    .map_errors(ReplaceLinkedTemporalByIndexError::from)
            })
    }

    /// Replace position with name with a temporal value.
    pub fn replace_temporal_named(
        &mut self,
        name: &Shortname,
        m: Temporal<V::Temporal>,
    ) -> Result<VTemporalOrOpticalWithScale<V>, ReplaceTemporalByNameNoLossError>
    where
        V::Optical: OpticalFromTemporal<V::Temporal, LossFlag = ()>,
        V::Temporal: TemporalMaybeToOptical<Warning = Nothing<()>, Error = Infallible>,
    {
        self.name_has_existing_links(name)?;
        let ret = self
            .meas
            .replace_temporal_by_name_nofail(name, m, |i, old_t| {
                V::Optical::from_temporal(old_t, i, ())
                    .set_err_value(())
                    .infallible_nowarn_into()
                    .0
            })?;
        Ok(ret)
    }

    /// Replace position with name with a temporal value where conversion cannot fail.
    pub fn replace_temporal_named_lossy(
        &mut self,
        name: &Shortname,
        m: Temporal<V::Temporal>,
        allow_loss: AllowLoss,
    ) -> WarningOrErrorResult<
        VTemporalOrOpticalWithScale<V>,
        (),
        AnyTemporalToOpticalKeyLossError,
        ReplaceLinkedTemporalByNameError,
    >
    where
        V::Optical: OpticalFromTemporal<V::Temporal, LossFlag = AllowLoss>,
        V::Temporal: TemporalMaybeToOptical<
                Warning = Option<AnyTemporalToOpticalKeyLossError>,
                Error = AnyTemporalToOpticalKeyLossError,
            >,
    {
        self.name_has_existing_links(name)
            .map_err(ReplaceLinkedTemporalByNameError::from)
            .into_nowarn1()
            .nowarn_and_then(|()| {
                self.meas
                    .replace_temporal_by_name(name, m, |i, old_t| {
                        V::Optical::from_temporal(old_t, i, allow_loss)
                            .switchable_into_non_commutative()
                            .map_ok_value(|(x, _)| x)
                            .map_errors(ReplaceTemporalByNameError::from)
                    })
                    .map_errors(ReplaceLinkedTemporalByNameError::from)
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
        self.meas.rename(index, key).map(|(old, new)| {
            let mapping = once((old.clone(), new.clone())).collect();
            self.rootmeta.rename_meas_links(&mapping);
            (old, new)
        })
    }

    /// Rename time measurement if it exists
    pub fn rename_temporal(
        &mut self,
        name: Shortname,
    ) -> Result<Option<Shortname>, NamePresentError> {
        self.meas.rename_temporal(name)
    }

    /// Apply functions to measurement values
    pub fn alter_measurements<F, G, R>(&mut self, f: F, g: G) -> Vec<R>
    where
        F: Fn(IndexedElement<&Shortname, &mut Temporal<V::Temporal>>) -> R,
        G: Fn(IndexedElement<&V::Name, &mut Optical<V::Optical>>) -> R,
    {
        self.meas.alter_values(f, g)
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
        self.meas.alter_values_zip(xs, f, g)
    }

    /// Return reference to time measurement as a name/value pair.
    pub fn temporal(&self) -> Option<IndexedElement<&Shortname, &Temporal<V::Temporal>>> {
        self.meas.meta().as_center()
    }

    /// Return mutable reference to time measurement as a name/value pair.
    pub fn temporal_mut(
        &mut self,
    ) -> Option<IndexedElement<&Shortname, &mut Temporal<V::Temporal>>> {
        self.meas.as_temporal_mut()
    }

    /// Return a reference to a field in metaroot
    pub fn metaroot<X>(&self) -> &X
    where
        RootMeta<V::RootMeta>: AsRef<X>,
    {
        self.rootmeta.as_ref()
    }

    /// Return a reference to an optional field in metaroot
    pub fn metaroot_opt<X>(&self) -> Option<&X>
    where
        RootMeta<V::RootMeta>: AsRef<Option<X>>,
    {
        self.metaroot().as_ref()
    }

    /// Set a field in metaroot
    pub fn set_metaroot<X>(&mut self, x: X)
    where
        RootMeta<V::RootMeta>: AsMut<X>,
    {
        *self.rootmeta.as_mut() = x;
    }

    /// Get a field from all measurements as an interator
    pub fn meas<'a, X: 'a>(&'a self) -> impl Iterator<Item = &'a X>
    where
        Temporal<V::Temporal>: AsRef<X>,
        Optical<V::Optical>: AsRef<X>,
    {
        self.meas
            .meta()
            .iter()
            .map(|x| x.both(|t| t.value.as_ref(), |m| m.value.inner().as_ref()))
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
        self.meas
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
        self.meas
            .meta()
            .iter()
            .map(|e| e.bimap_once(|_| (), |v| v.value.inner().as_ref()).into())
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
        self.meas.alter_elements_zip(
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
        self.meas
            .meta()
            .iter()
            .map(|x| x.bimap_once(|m| m.value.as_ref(), |m| m.value.inner().as_ref()))
    }

    /// Set field which is on both optical and temporal measurement types
    pub fn set_temporal_optical<T>(&mut self, xs: Vec<T>) -> Result<(), InputLengthError>
    where
        Optical<V::Optical>: AsMut<T>,
        Temporal<V::Temporal>: AsMut<T>,
    {
        self.meas
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
        self.meas.alter_elements_zip(
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
        RootMeta<V::RootMeta>: AsRef<Option<Btim<X>>>,
    {
        self.time_naive()
    }

    /// Get value for $ETIM as a [`NaiveTime`]
    pub fn etim_naive<X>(&self) -> Option<NaiveTime>
    where
        X: Copy,
        NaiveTime: From<X>,
        RootMeta<V::RootMeta>: AsRef<Option<Etim<X>>>,
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
        RootMeta<V::RootMeta>: AsMut<Timestamps<X>>,
    {
        let t = self.rootmeta.as_mut();
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
        RootMeta<V::RootMeta>: AsMut<Timestamps<X>>,
    {
        let t = self.rootmeta.as_mut();
        t.set_etim(time.map(|x| Xtim(x.into())))
    }

    /// Get $DATE as a [`NaiveDate`]
    pub fn date_naive(&self) -> Option<NaiveDate>
    where
        RootMeta<V::RootMeta>: AsRef<Option<FCSDate>>,
    {
        self.rootmeta.as_ref().as_ref().map(|&x| x.into())
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
        RootMeta<V::RootMeta>: AsMut<Timestamps<X>>,
    {
        self.rootmeta.as_mut().set_date(date.map(Into::into))
    }

    /// Get $BEGINDATETIME as a [`DateTime<FixedOffset>`]
    pub fn begindatetime(&self) -> Option<DateTime<FixedOffset>>
    where
        RootMeta<V::RootMeta>: AsRef<Option<BeginDateTime>>,
    {
        self.rootmeta.as_ref().as_ref().copied().map(Into::into)
    }

    /// Get $ENDDATETIME as a [`DateTime<FixedOffset>`]
    pub fn enddatetime(&self) -> Option<DateTime<FixedOffset>>
    where
        RootMeta<V::RootMeta>: AsRef<Option<EndDateTime>>,
    {
        self.rootmeta.as_ref().as_ref().copied().map(Into::into)
    }

    /// Set $BEGINDATETIME as a [`DateTime<FixedOffset>`]
    ///
    /// Return error if resulting $BEGINDATETIME is after $ENDDATETIME.
    pub fn set_begindatetime(
        &mut self,
        date: Option<DateTime<FixedOffset>>,
    ) -> Result<(), ReversedDatetimesError>
    where
        RootMeta<V::RootMeta>: AsMut<Datetimes>,
    {
        self.rootmeta.as_mut().set_begin(date.map(Into::into))
    }

    /// Set $ENDDATETIME as a [`DateTime<FixedOffset>`]
    ///
    /// Return error if resulting $BEGINDATETIME is after $ENDDATETIME.
    pub fn set_enddatetime(
        &mut self,
        date: Option<DateTime<FixedOffset>>,
    ) -> Result<(), ReversedDatetimesError>
    where
        RootMeta<V::RootMeta>: AsMut<Datetimes>,
    {
        self.rootmeta.as_mut().set_end(date.map(Into::into))
    }

    /// Get $TIMESTEP value if the time measurement exists.
    pub fn timestep(&self) -> Option<&Timestep>
    where
        Temporal<V::Temporal>: AsRef<Timestep>,
    {
        self.meas.meta().as_center().map(|x| x.value.as_ref())
    }

    /// Set $TIMESTEP value if the time measurement exists.
    ///
    /// Return `true` if the time measurement exist (which means its $TIMESTEP
    /// was updated) and `false` otherwise.
    pub fn set_timestep(&mut self, timestep: Timestep) -> Option<Timestep>
    where
        Temporal<V::Temporal>: AsMut<Timestep>,
    {
        self.meas.as_temporal_mut().map(|x| {
            let ts = x.value.as_mut();
            let old = *ts;
            *ts = timestep;
            old
        })
    }

    /// Show $COMP.
    pub fn compensation(&self) -> Option<&Compensation>
    where
        V::RootMeta: HasCompensation,
    {
        self.rootmeta.specific.comp(private::NoTouchy)
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
        V::RootMeta: HasCompensation,
    {
        if let Some(m) = matrix.as_ref() {
            let comp = m.matrix().ncols();
            let par = self.meas.meta().len();
            if comp != par {
                return Err(CompParMismatchError { par, comp });
            }
        }
        self.rootmeta.specific.set_comp(matrix, private::NoTouchy);
        Ok(())
    }

    /// Show $SPILLOVER
    pub fn spillover(&self) -> Option<&Spillover>
    where
        V::RootMeta: AsRef<Option<Spillover>>,
    {
        self.rootmeta.specific.as_ref().as_ref()
    }

    /// Set $SPILLOVER
    ///
    /// Return error if any measurements reference temporal measurement or
    /// if supplied matrix is invalid.
    pub fn set_spillover(&mut self, spillover: Option<Spillover>) -> Result<(), SetSpilloverErrors>
    where
        V::RootMeta: HasSpillover,
    {
        if let Some(s) = spillover.as_ref() {
            let ns = self.meas.meta().named_set();
            SetSpilloverErrors::try_new(s.invalid_link_errors(&ns))?;
        }
        *self.rootmeta.specific.spill_mut(private::NoTouchy) = spillover;
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
        V::RootMeta: HasUnstainedCenters,
    {
        let ns = self.meas.meta().named_set();
        SetUnstainedCentersErrors::try_new(us.invalid_link_error(&ns))?;
        *self
            .rootmeta
            .specific
            .unstainedcenters_mut(private::NoTouchy) = us;
        Ok(())
    }

    /// Return scale keywords
    pub fn scales(&self) -> impl Iterator<Item = V::OpticalScale> {
        self.meas.scales()
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

    /// Set scale keywords
    pub fn set_scales(
        &mut self,
        scales: Vec<V::OpticalScale>,
    ) -> GroupResult<(), SetScalesError, SetScalesSummary>
    where
        L: LayoutDatatype + LayoutWidth,
    {
        self.meas.set_scales(scales).group().resolve_nowarn()
    }

    /// Set gating keywords (3.0/3.1)
    pub fn set_applied_gates_3_0(
        &mut self,
        ag: AppliedGates3_0,
    ) -> GroupResult<(), BrokenRegionLinkError<MeasOrGateIndex>, SetAppliedGatesSummary>
    where
        V::RootMeta: HasAppliedGates<Gates = AppliedGates3_0>,
    {
        let p = self.par();
        let es = ag.invalid_link_errors(&p);
        ErrorGroup::try_new(es)?;
        *self.rootmeta.specific.applied_gates_mut(private::NoTouchy) = ag;
        Ok(())
    }

    /// Set gating keywords (3.2)
    pub fn set_applied_gates_3_2(
        &mut self,
        ag: AppliedGates3_2,
    ) -> GroupResult<(), BrokenRegionLinkError<PrefixedMeasIndex>, SetAppliedGatesSummary>
    where
        V::RootMeta: HasAppliedGates<Gates = AppliedGates3_2>,
    {
        let p = self.par();
        let es = ag.invalid_link_errors(&p);
        ErrorGroup::try_new(es)?;
        *self.rootmeta.specific.applied_gates_mut(private::NoTouchy) = ag;
        Ok(())
    }

    /// Get reference to non-standard keywords.
    pub fn nonstandard_keywords(&self) -> &NonStdKeywords {
        &self.nonstandard_keywords
    }

    /// Set non-standard keywords to new hash map.
    pub fn set_nonstandard_keywords(&mut self, kws: NonStdKeywords) {
        self.nonstandard_keywords = kws;
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
        ConvertWarning,
        ConvertError,
        ConvertSummary,
    >
    where
        Vf: VersionSet,
        Vf::RootMeta: ConvertFromMetaroot<V::RootMeta>,
        Vf::Optical: ConvertFromOptical<V::Optical>,
        Vf::Temporal: ConvertFromTemporal<V::Temporal>,
        Vf::OpticalScale: ConvertFromScale<V::OpticalScale>,
        Vf::Name: MightHave<Shortname> + Clone + ConvertFromShortname<V::Name>,
        // TODO technically normalize shouldn't be needed here but it won't hurt anything
        Lf: ConvertFromLayout<L> + LayoutNormalize,
    {
        let root_res = self
            .rootmeta
            .try_convert(allow_loss)
            .map_errors(ConvertError::Meta)
            .map_commutative_warnings(ConvertWarning::Meta);
        let meas_res = self
            .meas
            .try_convert(allow_loss)
            .map_errors(ConvertError::Meas)
            .map_commutative_warnings(ConvertWarning::Meas);
        let v0 = V::as_version();
        let v1 = Vf::as_version();
        let summary = ConvertSummary::new(v0, v1);
        root_res
            .zip_commutative(meas_res)
            .map_ok_value(|(metaroot, meas_layout)| {
                Core::new(
                    metaroot,
                    meas_layout,
                    self.nonstandard_keywords,
                    self.analysis,
                    self.others,
                )
            })
            .group_with(summary)
    }

    /// Get reference to measurement vector.
    pub fn measurements(&self) -> &MeasMeta<V::Name, V::Temporal, V::Optical, V::OpticalScale> {
        self.meas.meta()
    }

    /// Set measurements.
    ///
    /// Return error if names are not unique, if there is more than one
    /// time measurement, or if the measurement length doesn't match the
    /// data schema length.
    pub fn set_named_measurements(
        &mut self,
        measurements: VNamedTemporalsAndOpticalsWithScale<V>,
        allow_shared_names: bool,
        skip_index_check: bool,
    ) -> Result<(), SetNamedMeasurementsError>
    where
        L: LayoutDatatype + LayoutWidth,
    {
        let go = |cur_meas: &_, new_meas: &_| {
            self.rootmeta.new_meas_link_errors(
                cur_meas,
                new_meas,
                allow_shared_names,
                skip_index_check,
            )
        };
        self.meas.set_named_measurements_with(measurements, go)
    }

    /// Set measurements without $PnN.
    pub fn set_measurements(
        &mut self,
        measurements: VTemporalsAndOpticals<V>,
    ) -> Result<(), SetUnnamedMeasurementsError>
    where
        L: LayoutWidth + LayoutDatatype,
    {
        self.meas.set_unnamed_measurements(measurements)
    }

    #[cfg(feature = "serde")]
    fn named_compensation(&self) -> Option<(Vec<Shortname>, Array2<f32>)>
    where
        V::RootMeta: HasCompensation,
    {
        self.compensation().as_ref().map(|c| {
            let m: &Array2<f32> = c.as_ref();
            (self.all_shortnames(), m.clone())
        })
    }

    #[cfg(feature = "serde")]
    fn named_spillover(&self) -> Option<(Vec<Shortname>, Array2<f32>)>
    where
        V::RootMeta: AsRef<Option<Spillover>>,
    {
        self.spillover().as_ref().map(|c| {
            let ns: &[Shortname] = c.as_ref();
            let m: &Array2<f32> = c.as_ref();
            (ns.to_vec(), m.clone())
        })
    }

    fn time_naive<const IS_ETIM: bool, X>(&self) -> Option<NaiveTime>
    where
        X: Copy,
        NaiveTime: From<X>,
        RootMeta<V::RootMeta>: AsRef<Option<Xtim<IS_ETIM, X>>>,
    {
        let t: &Option<Xtim<IS_ETIM, X>> = self.rootmeta.as_ref();
        t.as_ref().map(|&x| x.0.into())
    }

    fn remove_measurement_by_name_inner<C>(
        &mut self,
        name: &Shortname,
    ) -> Result<(MeasIndex, VTemporalOrOpticalWithScale<V>, C), RemoveMeasByNameError>
    where
        L: LayoutRemove<C>,
    {
        self.name_has_existing_links(name)?;
        let ret = self.meas.remove_measurement_by_name(name)?;
        self.rootmeta.specific.remove_meas_index_inner(ret.0);
        Ok(ret)
    }

    fn remove_measurement_by_index_inner<C>(
        &mut self,
        index: MeasIndex,
    ) -> Result<(VPairedTemporalOrOpticalWithScale<V>, C), RemoveMeasByIndexError>
    where
        L: LayoutRemove<C>,
    {
        self.index_has_existing_links(index)?;
        let ret = self.meas.remove_measurement_by_index(index)?;
        self.rootmeta.specific.remove_meas_index_inner(index);
        Ok(ret)
    }

    fn name_has_existing_links(&self, name: &Shortname) -> Result<(), ExistingLinkErrors> {
        if let Some(&index) = self.meas.meta().named_indices().get(name) {
            // NOTE if the meas to be removed is temporal, this name shouldn't
            // trigger a link error because $SPILLOVER, $UNSTAINEDCENTERS, and
            // $TR should never link to a temporal measurement
            let ns = HashSet::from([name]).into();
            let js = HashSet::from([index]).into();
            let es = self
                .rootmeta
                .meas_has_existing_links_with(self.par(), &ns, &js);
            ExistingLinkErrors::try_new(es)
        } else {
            Ok(())
        }
    }

    fn index_has_existing_links(&self, index: MeasIndex) -> Result<(), ExistingLinkErrors> {
        if let Some(&name) = self.meas.meta().indexed_name_map().get(&index) {
            // NOTE if the meas to be removed is temporal, this name shouldn't
            // trigger a link error because $SPILLOVER, $UNSTAINEDCENTERS, and
            // $TR should never link to a temporal measurement
            let ns = HashSet::from([name]).into();
            let js = HashSet::from([index]).into();
            let es = self
                .rootmeta
                .meas_has_existing_links_with(self.par(), &ns, &js);
            ExistingLinkErrors::try_new(es)
        } else {
            Ok(())
        }
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
        self.meas.push_temporal_inner(n, m, r).when_ok(|| {
            let i = self.par().0.into();
            self.rootmeta.specific.insert_meas_index_inner(i);
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
        self.meas
            .insert_temporal_inner(i, n, m, r)
            .when_ok(|| self.rootmeta.specific.insert_meas_index_inner(i))
    }

    fn push_optical_inner<C>(
        &mut self,
        name: V::Name,
        opt: Optical<V::Optical>,
        scale: V::OpticalScale,
        data_column: C,
    ) -> ErrorsResult<Shortname, (), PushOpticalError<L::Error>>
    where
        L: LayoutInsert<C> + LayoutInsertScaleCheck<C>,
    {
        self.meas
            .push_optical_inner(name, opt, scale, data_column)
            .map_ok_value(|ret| {
                let i = self.par().0.into();
                self.rootmeta.specific.insert_meas_index_inner(i);
                ret
            })
    }

    fn insert_optical_inner<C>(
        &mut self,
        i: MeasIndex,
        name: V::Name,
        opt: Optical<V::Optical>,
        scale: V::OpticalScale,
        data_column: C,
    ) -> ErrorsResult<Shortname, (), InsertOpticalError<L::Error>>
    where
        L: LayoutInsert<C> + LayoutInsertScaleCheck<C>,
    {
        self.meas
            .insert_optical_inner(i, name, opt, scale, data_column)
            .map_ok_value(|ret| {
                self.rootmeta.specific.insert_meas_index_inner(i);
                ret
            })
    }

    fn set_measurements_and_layout_inner(
        &mut self,
        measurements: VTemporalsAndOpticals<V>,
        layout: L,
    ) -> Result<(), SetUnnamedMeasurementsAndDataSchemaError>
    where
        L: LayoutWidth + LayoutDatatype + LayoutNormalize,
    {
        self.meas
            .set_unnamed_measurements_and_layout(measurements, layout)
    }

    fn unset_measurements_inner(&mut self) -> Result<(), ExistingLinkErrors>
    where
        L: LayoutWidth,
    {
        let p = self.par();
        let (js, ns) = self.meas.meta().all_indices_and_names_to_remove();
        let es = self.rootmeta.meas_has_existing_links_with(p, &ns, &js);
        ExistingLinkErrors::try_new(es)?;
        self.meas.clear();
        Ok(())
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
            .opt_std_and_nonstd_keywords()
            .map(OptKeyword::from)
            .chain(self.opt_meas_keywords().map(OptKeyword::from));
        if V::as_version() == Version::FCS2_0 {
            let ks = req.map(AnyKeyword::from).chain(opt.map(AnyKeyword::from));
            HeaderKeywordsToWrite::new_2_0(ks, conf)
        } else {
            HeaderKeywordsToWrite::new_3_0(req, opt, conf)
        }
    }

    fn opt_meas_keywords(&self) -> impl Iterator<Item = OptMeasKeyword<'_>>
    where
        L: LayoutOptMeasKeywords,
    {
        let ns = (!V::Name::INFALLABLE)
            .then(|| {
                self.meas
                    .meta()
                    .opt_names()
                    .flatten()
                    .enumerate()
                    .map(|(i, v)| OptMeasKeyword::from_ref(v, i))
            })
            .into_iter()
            .flatten();
        let lv = self
            .meas
            .data()
            .opt_meas_keywords()
            .into_iter()
            .flatten()
            .map(OptMeasKeyword::from);
        self.meas
            .meta()
            .iter_with(
                &|i, x| {
                    Temporal::opt_meas_keywords(&x.value, i)
                        .map(OptMeasKeyword::from)
                        .collect::<Vec<_>>()
                },
                &|i, x| {
                    ScaledOptical::opt_keywords(&x.value, i)
                        .map(OptMeasKeyword::from)
                        .collect()
                },
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
                self.meas
                    .meta()
                    .opt_names()
                    .flatten()
                    .enumerate()
                    .map(|(i, v)| ReqMeasKeyword::from_ref(v, i))
            })
            .into_iter()
            .flatten();
        let lv = self.meas.data().req_meas_keywords().into_iter().flatten();
        self.meas
            .meta()
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
        let lv = self.meas.data().req_keywords();
        RootMeta::req_keywords(&self.rootmeta, self.par()).chain(lv)
    }

    fn opt_std_and_nonstd_keywords(&self) -> impl Iterator<Item = StdOrNonStdOptRootKeyword<'_>> {
        let ns = self
            .nonstandard_keywords
            .iter()
            .map(|(k, v)| NonStdKeyword::new(k, v.as_ne_str()))
            .map(StdOrNonStdOptRootKeyword::from);
        self.rootmeta
            .opt_root_keywords()
            .map(StdOrNonStdOptRootKeyword::from)
            .chain(ns)
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
            Optical(OptScaledOpticalKeyword<'a>),
            Temporal(OptMeasTemporalKeyword<'a>),
            NumType(SplitKeyword1<kws::NumType>),
        }

        impl<'a> MeasKeyword<'a> {
            fn key(&'a self) -> String {
                match self {
                    MeasKeyword::Index(_) => INDEX.into(),
                    MeasKeyword::Req(x) => x.std_blank(),
                    MeasKeyword::Optical(x) => x.std_blank(),
                    MeasKeyword::Temporal(x) => x.std_blank(),
                    MeasKeyword::NumType(x) => x.std_blank(),
                }
            }

            fn value(&'a self) -> String {
                match self {
                    MeasKeyword::Index(x) => x.to_string(),
                    MeasKeyword::Req(x) => x.as_str_pair().1.into(),
                    MeasKeyword::Optical(x) => x.as_str_pair().1.into(),
                    MeasKeyword::Temporal(x) => x.as_str_pair().1.into(),
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
            kws::Width::std_blank(),
            kws::TextRange::std_blank(),
            kws::Scale::std_blank(),
            kws::Filter::std_blank(),
            // NOTE same for Wavelengths
            kws::Wavelength::std_blank(),
            kws::Power::std_blank(),
            kws::DetectorType::std_blank(),
            kws::PercentEmitted::std_blank(),
            kws::DetectorVoltage::std_blank(),
        ];

        let peak = [kws::PeakBin::std_blank(), kws::PeakIndex::std_blank()];

        header.extend(common);

        match version {
            Version::FCS2_0 => {
                header.extend(peak);
            }
            Version::FCS3_0 => {
                header.push(kws::Gain::std_blank());
                header.extend(peak);
            }
            Version::FCS3_1 => {
                header.push(kws::Gain::std_blank());
                header.push(kws::Calibration3_1::std_blank());
                header.push(kws::Display::std_blank());
                header.extend(peak);
            }
            Version::FCS3_2 => {
                header.push(kws::Gain::std_blank());
                header.push(kws::Calibration3_2::std_blank());
                header.push(kws::Display::std_blank());
                header.push(kws::DetectorName::std_blank());
                header.push(kws::Tag::std_blank());
                header.push(kws::OpticalType::std_blank());
                header.push(kws::Feature::std_blank());
                header.push(kws::Analyte::std_blank());
                header.push(kws::NumType::std_blank());
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
            .meas
            .meta()
            .iter()
            .map(|m| {
                m.both(
                    |t| (Some(&t.key), Element::Center(t.value.clone())),
                    |o| (V::Name::as_opt(&o.key), Element::NonCenter(o.value.clone())),
                )
            })
            .collect();

        let lt = &self.meas.data();
        let req_layout = lt.req_meas_keywords();
        let opt_layout = lt.opt_meas_keywords();

        assert_eq_msg!(
            req_layout.len(),
            opt_layout.len(),
            "required schema columns",
            "optional schema columns"
        );

        assert_eq_msg!(
            ms.len(),
            req_layout.len(),
            "measurement length",
            "schema length"
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

            // For temporal measurements, keep all keywords except $TIMESTEP
            // since this won't fit anywhere in the table
            let remove_timestep = |k| {
                if let OptTemporalKeyword::Meas(x) = k {
                    Some(x)
                } else {
                    None
                }
            };

            for (i, ((n, m), (req_l, opt_l))) in ne {
                let mut row = vec![None; header.len()];
                let j = MeasIndex::from(i);
                let req: Vec<_> = match m {
                    Element::Center(t) => t
                        .req_meas_keywords(j)
                        .into_iter()
                        .map(MeasKeyword::from)
                        .collect(),
                    Element::NonCenter(o) => o.req_keywords(j).map(MeasKeyword::from).collect(),
                };
                let opt: Vec<_> = match m {
                    Element::Center(t) => t
                        .opt_meas_keywords(j)
                        .filter_map(remove_timestep)
                        .map(MeasKeyword::from)
                        .collect(),
                    Element::NonCenter(o) => o.opt_keywords(j).map(MeasKeyword::from).collect(),
                };
                let xs = once(MeasKeyword::from(j))
                    .chain(shortname(*n, j))
                    .chain(req_l.map(MeasKeyword::from))
                    .chain(opt_l.fmap(MeasKeyword::from))
                    .chain(req)
                    .chain(opt);
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

    #[allow(clippy::type_complexity)]
    fn lookup_names<C>(
        kws: &mut ValidKeywords,
        par: Par,
        dropped: &mut StdKeywords,
        conf: &C,
    ) -> WarningsAndErrorsResult<
        (Vec<V::Name>, Vec<Option<Shortname>>),
        (),
        OptIndexedKeyError<Shortname>,
        LookupShortnameError,
    >
    where
        C: AsRef<EvaledReadDataKeywordsConfig> + AsRef<EvaledReadStdKeywordsConfig>,
        V::RootMeta: LookupMetaroot<V::Name>,
        V::Name: LookupShortname,
    {
        (0..par.0)
            .map(|n| {
                let i = n.into();
                V::Name::lookup_shortname(kws, dropped, i, conf.as_ref()).into_semigroup()
            })
            .sequence_commutative()
            .map_ok_value(|mut names| {
                let sconf: &EvaledReadStdKeywordsConfig = conf.as_ref();
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
        kws: &mut ValidKeywords,
        dropped: &mut StdKeywords,
        names: Vec<V::Name>,
        dts: &[AlphaNumType],
        conf: &C,
    ) -> LookupMeasurementResult<(VNamedTemporalsAndScaledOpticals<V>, MeasurementDiagnostics)>
    where
        C: AsRef<EvaledReadDataKeywordsConfig> + AsRef<EvaledReadStdKeywordsConfig>,
        V::RootMeta: LookupMetaroot<V::Name>,
        V::Temporal: LookupTemporal,
        V::Optical: LookupOptical,
        V::Name: Pointed<Shortname>,
    {
        let sconf: &EvaledReadStdKeywordsConfig = conf.as_ref();
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

        assert_eq_msg!(names.len(), dts.len(), "datatypes", "names");

        names
            .into_iter()
            .zip(dts)
            .enumerate()
            .map(|(i, (wrapped, dt))| {
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
                        Element::Center(name) => Temporal::lookup_temporal(kws, dropped, j, conf)
                            .map_errors(LookupMeasurementError::from)
                            .map_commutative_warnings(LookupMeasurementWarning::from)
                            .map_ok_value(|x| Element::Center((name, x))),
                        Element::NonCenter(k) => {
                            ScaledOptical::lookup_scaled_optical(kws, dropped, j, *dt, conf)
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
}

impl<V: VersionSet> VersionedCoreTEXT<V> {
    #[allow(clippy::type_complexity)]
    pub(crate) fn new_from_keywords_with_offsets<C>(
        mut kws: ValidKeywords,
        offsets: &mut HeaderAndSuppOffsets,
        st: &TEXTReadState<C>,
    ) -> WarningsAndErrorsResult<
        (
            Self,
            StdTEXTDiagnostics,
            MetarootTEXTOffsets<V>,
            RepairDiagnostics,
        ),
        (),
        StdTEXTFromFlatTEXTWarning,
        StdTEXTFromFlatTEXTErrorInner,
    >
    where
        V::RootMeta: LookupMetaroot<V::Name>,
        V::Temporal: LookupTemporal,
        V::Optical: LookupOptical,
        V::Name: LookupShortname,
        V::DataSchema: VersionedDataSchema,
        C: AsRef<EvaledReadStdKeywordsConfig>
            + AsRef<EvaledReadDataKeywordsConfig>
            + AsRef<ReadOffsetConfig>,
    {
        let mut dropped = HashMap::new();

        // Repair the keyword list before doing anything.
        let repair_res = kws
            .repair(st.conf().as_ref())
            .map_commutative_warnings(StdTEXTFromFlatTEXTWarning::from)
            .map_errors(StdTEXTFromFlatTEXTErrorInner::from)
            .into_semigroup();

        // Lookup DATA/ANALYSIS offsets and $TOT; these are not stored in the
        // Core struct but they will be needed later for parsing DATA and
        // ANALYSIS, and processing these keywords now will make it easier to
        // determine if TEXT is totally standardized or not.
        let offsets_res = V::Offsets::lookup(&mut kws, &mut dropped, offsets, st)
            .map_commutative_warnings(StdTEXTFromFlatTEXTWarning::from)
            .map_errors(StdTEXTFromFlatTEXTErrorInner::from);

        Self::lookup_inner(kws, dropped, st.conf())
            .zip3_commutative(offsets_res, repair_res)
            .map_ok_value(|((a, b), c, d)| (a, b, c, d))
    }

    /// Make a new CoreTEXT from flat keywords.
    ///
    /// Return any errors encountered, including missing required keywords and
    /// parse errors.
    ///
    /// This will not process $TOT or $(BEGIN|END)(TEXT|DATA). If present these
    /// will trigger pseudostandard warnings.
    pub fn new_from_keywords<C>(
        mut kws: ValidKeywords,
        conf: &C,
    ) -> WarningsAndGroupResult<
        (Self, StdTEXTDiagnostics, RepairDiagnostics),
        StdTEXTFromKeywordsWarning,
        StdTEXTFromKeywordsError,
        CoreTEXTFromKeywordsSummary,
    >
    where
        V::RootMeta: LookupMetaroot<V::Name>,
        V::Temporal: LookupTemporal,
        V::Optical: LookupOptical,
        V::Name: LookupShortname,
        V::DataSchema: VersionedDataSchema,
        C: AsRef<ReadStdKeywordsConfig> + AsRef<ReadDataKeywordsConfig>,
    {
        #[derive(AsRef)]
        struct LookupConf {
            #[as_ref(EvaledReadStdKeywordsConfig)]
            text: EvaledReadStdKeywordsConfig,
            #[as_ref(EvaledReadDataKeywordsConfig)]
            data: EvaledReadDataKeywordsConfig,
        }

        let sconf: &ReadStdKeywordsConfig = conf.as_ref();
        let dconf: &ReadDataKeywordsConfig = conf.as_ref();

        dconf
            .eval(&kws)
            .map_ok_value(|data| LookupConf {
                text: sconf.eval(&kws),
                data,
            })
            .map_errors(StdTEXTFromKeywordsError::from)
            .nowarn_into_warn()
            .group()
            .and_then_commutative(|lconf| {
                let repair_res = kws
                    .repair(&lconf.data)
                    .map_errors(StdTEXTFromKeywordsError::from)
                    .map_commutative_warnings(StdTEXTFromKeywordsWarning::from)
                    .into_semigroup();

                Self::lookup_inner(kws, HashMap::new(), &lconf)
                    .map_errors(StdTEXTFromKeywordsError::from)
                    .map_commutative_warnings(StdTEXTFromKeywordsWarning::from)
                    .zip_commutative(repair_res)
                    .map_ok_value(|((a, b), c)| (a, b, c))
                    .group()
            })
    }

    #[allow(clippy::too_many_lines)]
    fn lookup_inner<C>(
        mut kws: ValidKeywords,
        mut dropped: StdKeywords,
        conf: &C,
    ) -> WarningsAndErrorsResult<
        (Self, StdTEXTDiagnostics),
        (),
        StdTEXTFromFlatTEXTWarning,
        StdTEXTFromFlatTEXTErrorInner,
    >
    where
        V::RootMeta: LookupMetaroot<V::Name>,
        V::Temporal: LookupTemporal,
        V::Optical: LookupOptical,
        V::Name: LookupShortname,
        V::DataSchema: VersionedDataSchema,
        C: AsRef<EvaledReadStdKeywordsConfig> + AsRef<EvaledReadDataKeywordsConfig>,
    {
        // Lookup $PAR first since we need this to get the measurements
        let par_res = Par::remove_metaroot_req(&mut kws.std)
            .map_err(LookupMetarootError::from)
            .map_err(StdTEXTFromFlatTEXTErrorInner::from)
            .into_log();

        let version = V::as_version();
        let sconf: &EvaledReadStdKeywordsConfig = conf.as_ref();

        macro_rules! go_err {
            ($x:expr) => {
                $x.map_commutative_warnings(StdTEXTFromFlatTEXTWarning::from)
                    .map_errors(StdTEXTFromFlatTEXTErrorInner::from)
            };
        }

        par_res.and_then_commutative(|par| {
            // Lookup $PnN first (everything else depends on these)
            let names_res = Self::lookup_names(&mut kws, par, &mut dropped, conf);
            let mut core_res = go_err!(names_res)
                // Lookup root (which depends on $PnN) and data schema
                .and_then_commutative(|(dedup_names, original_names)| {
                    let layout_res =
                        V::DataSchema::lookup(&mut kws, par, &mut dropped, conf.as_ref());

                    let root_res =
                        RootMeta::lookup_metaroot(&mut kws, &mut dropped, &dedup_names[..], conf);

                    go_err!(root_res)
                        .zip_commutative(go_err!(layout_res))
                        .map_ok_value(|x| (x, dedup_names, original_names))
                })
                // Lookup measure which depends on global datatype
                .and_then_commutative(
                    |((metaroot_out, layout_out), dedup_names, original_names)| {
                        let dts = &layout_out.data_schema.datatypes()[..];
                        let ret = Self::lookup_measurements(
                            &mut kws,
                            &mut dropped,
                            dedup_names,
                            dts,
                            conf,
                        );
                        go_err!(ret).map_ok_value(|x| (metaroot_out, layout_out, x, original_names))
                    },
                )
                .and_then_commutative(
                    |(metaroot_out, layout_out, (meas, meas_diag), original_names)| {
                        let meta_diag = metaroot_out.diagnostic;
                        let ret = Self::try_new(
                            metaroot_out.inner,
                            meas,
                            layout_out.data_schema,
                            kws.nonstd,
                            conf,
                        )
                        .map_ok_value(|ret| {
                            (
                                ret,
                                original_names,
                                meta_diag,
                                meas_diag,
                                layout_out.diagnostics,
                            )
                        });
                        go_err!(ret)
                    },
                );

            let gate = core_res
                .as_ref()
                .and_then(|(core, _, _, _, _)| core.rootmeta.specific.gate())
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
                                    core.0.nonstandard_keywords.insert_demoted(k, v);
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

            core_res.map_ok_value(|(ret, original_names, meta_diag, meas_diag, schema_diag)| {
                let d = StdTEXTDiagnostics::from_extra(
                    extra,
                    dropped,
                    original_names,
                    meta_diag,
                    meas_diag,
                    schema_diag,
                );
                (ret, d)
            })
        })
    }

    /// Get reference to data schema
    pub fn data_schema(&self) -> &V::DataSchema {
        self.meas.data()
    }

    /// Set data schema.
    ///
    /// Will return error if data schema does not have same number of columns as
    /// measurements.
    pub fn set_data_schema(
        &mut self,
        data_schema: V::DataSchema,
    ) -> Result<(), MeasLayoutMismatchError> {
        self.meas.set_data_schema(data_schema)
    }

    /// Set measurements without $PnN and data schema
    pub fn set_measurements_and_data_schema(
        &mut self,
        measurements: VTemporalsAndOpticals<V>,
        data_schema: V::DataSchema,
    ) -> Result<(), SetUnnamedMeasurementsAndDataSchemaError>
    where
        V::DataSchema: LayoutDatatype + LayoutNormalize,
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
        measurements: VNamedTemporalsAndOpticalsWithScale<V>,
        data_schema: V::DataSchema,
        allow_shared_names: bool,
        skip_index_check: bool,
    ) -> Result<(), SetNamedMeasurementsError> {
        let go = |cur_meas: &_, new_meas: &_| {
            self.rootmeta.new_meas_link_errors(
                cur_meas,
                new_meas,
                allow_shared_names,
                skip_index_check,
            )
        };
        self.meas
            .set_named_measurements_and_layout_with(measurements, data_schema, go)
    }

    /// Remove a measurement matching the given name.
    ///
    /// Return removed measurement and its index if found.
    pub fn remove_measurement_by_name<R>(
        &mut self,
        n: &Shortname,
    ) -> Result<(MeasIndex, VTemporalOrOpticalWithScale<V>, R), RemoveMeasByNameError>
    where
        V::DataSchema: LayoutRemove<R>,
    {
        self.remove_measurement_by_name_inner(n)
    }

    #[allow(clippy::type_complexity)]
    #[cfg(feature = "python")]
    pub fn py_remove_measurement_by_name<R>(
        &mut self,
        n: &Shortname,
    ) -> Result<
        (
            MeasIndex,
            VTemporalOrOptical<V>,
            R,
            <V::OpticalScale as PySplitScale>::MaybeScale,
        ),
        RemoveMeasByNameError,
    >
    where
        V::DataSchema: LayoutRemove<R>,
        V::OpticalScale: PySplitScale,
    {
        let go = |(i, m, r)| {
            let (mm, s) = V::OpticalScale::split_scale(m);
            (i, mm, r, s)
        };
        self.remove_measurement_by_name(n).map(go)
    }

    #[allow(clippy::type_complexity)]
    #[cfg(feature = "python")]
    pub fn py_remove_measurement_by_name_typed<R>(
        &mut self,
        n: &Shortname,
    ) -> Result<
        (
            MeasIndex,
            VTemporalOrOptical<V>,
            FullRange,
            <V::OpticalScale as PySplitScale>::MaybeScale,
            Option<R>,
        ),
        RemoveMeasByNameError,
    >
    where
        R: PyRangeType,
        V::DataSchema: LayoutRemove<R::Range>,
        V::OpticalScale: PySplitScale,
    {
        let go = |(i, m, r)| {
            let (mm, s) = V::OpticalScale::split_scale(m);
            let (rr, t) = R::split_range(r);
            (i, mm, rr, s, t)
        };
        self.remove_measurement_by_name(n).map(go)
    }

    /// Remove a measurement at a given position
    ///
    /// Return removed measurement and its name if found.
    pub fn remove_measurement_by_index<R>(
        &mut self,
        index: MeasIndex,
    ) -> Result<(VPairedTemporalOrOpticalWithScale<V>, R), RemoveMeasByIndexError>
    where
        V::DataSchema: LayoutRemove<R>,
    {
        self.remove_measurement_by_index_inner(index)
    }

    #[allow(clippy::type_complexity)]
    #[cfg(feature = "python")]
    pub fn py_remove_measurement_by_index<R>(
        &mut self,
        index: MeasIndex,
    ) -> Result<
        (
            V::Name,
            VTemporalOrOptical<V>,
            R,
            <V::OpticalScale as PySplitScale>::MaybeScale,
        ),
        RemoveMeasByIndexError,
    >
    where
        V::Name: MightHave<Shortname>,
        V::DataSchema: LayoutRemove<R>,
        V::OpticalScale: PySplitScale,
    {
        let go = |(p, r): (EitherPair<_, _, _>, _)| {
            let (n, m) = p.unzip();
            let (mm, s) = V::OpticalScale::split_scale(m);
            (n, mm, r, s)
        };
        self.remove_measurement_by_index(index).map(go)
    }

    #[allow(clippy::type_complexity)]
    #[cfg(feature = "python")]
    pub fn py_remove_measurement_by_index_typed<R>(
        &mut self,
        index: MeasIndex,
    ) -> Result<
        (
            V::Name,
            VTemporalOrOptical<V>,
            FullRange,
            <V::OpticalScale as PySplitScale>::MaybeScale,
            Option<R>,
        ),
        RemoveMeasByIndexError,
    >
    where
        V::Name: MightHave<Shortname>,
        R: PyRangeType,
        V::DataSchema: LayoutRemove<R::Range>,
        V::OpticalScale: PySplitScale,
    {
        let go = |(p, r): (EitherPair<_, _, _>, _)| {
            let (n, m) = p.unzip();
            let (mm, s) = V::OpticalScale::split_scale(m);
            let (rr, t) = R::split_range(r);
            (n, mm, rr, s, t)
        };
        self.remove_measurement_by_index(index).map(go)
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
        let layout = self.meas.with_data(df)?;
        Ok(Core::new(
            self.rootmeta,
            layout,
            self.nonstandard_keywords,
            analysis,
            others,
        ))
    }

    // only meant to be called during lookup when keywords are being read from
    // a hashtable
    pub(crate) fn try_new<C>(
        mut metaroot: RootMeta<V::RootMeta>,
        measurements: VNamedTemporalsAndScaledOpticals<V>,
        data_schema: V::DataSchema,
        mut nonstd: NonStdKeywords,
        conf: &C,
    ) -> WarningsAndErrorsResult<Self, (), NewCoreWarning, LookupCoreError>
    where
        V::DataSchema: LayoutWidth,
        C: AsRef<EvaledReadDataKeywordsConfig> + AsRef<EvaledReadStdKeywordsConfig>,
    {
        let rconf: &EvaledReadDataKeywordsConfig = conf.as_ref();
        let opt_flag = rconf.process_optional_failure;
        CoreMeasurements::try_new(measurements, data_schema, conf.as_ref())
            .map_errors(LookupCoreError::from)
            .map_commutative_warnings(NewCoreWarning::from)
            .and_then_commutative(|ml| {
                Self::check_relationships(
                    &mut metaroot,
                    ml.meta(),
                    &mut nonstd,
                    opt_flag.is_demote(),
                )
                .map_errors(NewCoreWarning::from)
                .nowarn_into_switchable(opt_flag)
                .switchable_into_commutative()
                .map_errors(LookupCoreError::from)
                .map_commutative_warnings(NewCoreWarning::from)
                .map_ok_value(|()| Self::new(metaroot, ml, nonstd, (), ()))
            })
    }

    pub(crate) fn try_new_nodrop(
        mut metaroot: RootMeta<V::RootMeta>,
        measurements: VNamedTemporalsAndOpticalsWithScale<V>,
        data_schema: V::DataSchema,
        mut nonstd: NonStdKeywords,
    ) -> ErrorsResult<Self, (), NewCoreError> {
        CoreMeasurements::try_new_nodrop(measurements, data_schema)
            .map_errors(NewCoreError::from)
            .and_then_commutative(|ml| {
                Self::check_relationships(&mut metaroot, ml.meta(), &mut nonstd, false)
                    .map_errors(NewCoreError::from)
                    .map_ok_value(|()| Self::new(metaroot, ml, nonstd, (), ()))
            })
    }

    /// Check for invalid keyword relationships.
    ///
    /// For example, $SPILLOVER in the metaroot must refer to valid
    /// measurements.
    ///
    /// If allow_dropping is true, remove keywords with invalid relationships.
    fn check_relationships(
        metaroot: &mut RootMeta<V::RootMeta>,
        measurements: &MeasMeta<V::Name, V::Temporal, V::Optical, V::OpticalScale>,
        nonstd: &mut NonStdKeywords,
        demote: bool,
    ) -> ErrorsResult<(), (), BrokenOrDependentLinkError> {
        let ns = measurements.named_set();
        let par = Par(measurements.len());
        let link_errs = metaroot.remove_invalid_links(par, &ns, nonstd, demote);
        LogResult::new_from_err_iter(link_errs, (), ())
    }
}

impl<V: VersionSet> VersionedCoreDataset<V> {
    pub fn new_from_keywords<C>(
        p: &PathBuf,
        mut hns: HeaderAndSuppOffsets,
        kws: ValidKeywords,
        dataset_offset: DatasetOffset,
        dataset_len: Option<DatasetLen>,
        conf: &C,
    ) -> WarningsAndIOGroupResult<
        (Self, NewStdDatasetFromKwsOutput),
        StdDatasetFromFlatTEXTWarning,
        StdDatasetFromKeywordsError,
        StdDatasetWithKwsSummary,
    >
    where
        V::RootMeta: LookupMetaroot<V::Name>,
        V::Temporal: LookupTemporal,
        V::Optical: LookupOptical,
        V::Name: LookupShortname,
        V::DataSchema: DataSchemaToEmptyDataFrame<DfTarget = V::DataFrame>,
        C: AsRef<ReadStdKeywordsConfig>
            + AsRef<ReadOffsetConfig>
            + AsRef<ReadDataKeywordsConfig>
            + AsRef<ReadDatasetConfig>
            + AsRef<ReadSharedConfig>,
    {
        #[derive(AsRef)]
        struct LookupConfig {
            #[as_ref(EvaledReadStdKeywordsConfig)]
            std: EvaledReadStdKeywordsConfig,
            #[as_ref(EvaledReadDataKeywordsConfig)]
            data: EvaledReadDataKeywordsConfig,
            #[as_ref(ReadOffsetConfig)]
            offsets: ReadOffsetConfig,
            #[as_ref(ReadDatasetConfig)]
            dataset: ReadDatasetConfig,
        }

        #[allow(
            clippy::result_large_err,
            reason = "top level function, shouldn't be used often, large call stack won't matter much"
        )]
        FCSFileReader::open_with_state(p, dataset_offset, conf)
            .map_err(|e| e.fmap_once(StdDatasetFromFlatTextErrorInner::from))
            .map_err(|e| e.fmap_once(StdDatasetFromKeywordsError::from))
            .and_then(|(fr, st)| {
                st.maybe_with_dataset_length(dataset_len)
                    .map(|txt_st| (fr, txt_st))
                    .map_err(StdDatasetFromKeywordsError::from)
                    .map_err(ImpureError::Pure)
            })
            .map_err(IOErrorGroup::from)
            .into_log()
            .and_then_commutative(|(mut fr, txt_st)| {
                AsRef::<ReadDataKeywordsConfig>::as_ref(txt_st.conf())
                    .eval(&kws)
                    .map_ok_value(|data| {
                        txt_st.first_once(|iconf| LookupConfig {
                            std: AsRef::<ReadStdKeywordsConfig>::as_ref(&iconf).eval(&kws),
                            data,
                            offsets: *AsRef::<ReadOffsetConfig>::as_ref(&iconf),
                            dataset: *AsRef::<ReadDatasetConfig>::as_ref(&iconf),
                        })
                    })
                    .map_errors(StdTEXTFromFlatTEXTErrorInner::from)
                    .map_errors(StdDatasetFromFlatTextErrorInner::from)
                    .map_errors(StdDatasetFromKeywordsError::from)
                    .nowarn_into_warn()
                    .group()
                    .map_error(IOErrorGroup::Pure)
                    .and_then_commutative(|lst| {
                        Self::new_from_keywords_inner(&mut fr.buf_read, kws, &mut hns, false, &lst)
                            .map_pure_errors(StdDatasetFromKeywordsError::from)
                    })
            })
            .map_ok_value(|(ret, dataset)| {
                let out = NewStdDatasetFromKwsOutput::new(dataset, hns.header.final_offsets);
                (ret, out)
            })
            .warnings_to_pure_errors(*conf.as_ref(), StdDatasetFromKeywordsError::from)
            .deanonymize()
    }

    pub(crate) fn new_from_keywords_inner<C, R>(
        h: &mut BufReader<R>,
        kws: ValidKeywords,
        hns: &mut HeaderAndSuppOffsets,
        scan_next_dataset: bool,
        st: &TEXTReadState<C>,
    ) -> WarningsAndIOGroupResult<
        (Self, StdDatasetFromKwsOutput),
        StdDatasetFromFlatTEXTWarning,
        StdDatasetFromFlatTextErrorInner,
        (),
    >
    where
        R: Read + Seek,
        V::RootMeta: LookupMetaroot<V::Name>,
        V::Temporal: LookupTemporal,
        V::Optical: LookupOptical,
        V::Name: LookupShortname,
        V::DataSchema: DataSchemaToEmptyDataFrame<DfTarget = V::DataFrame>,
        C: AsRef<EvaledReadStdKeywordsConfig>
            + AsRef<ReadOffsetConfig>
            + AsRef<EvaledReadDataKeywordsConfig>
            + AsRef<ReadDatasetConfig>,
    {
        VersionedCoreTEXT::<V>::new_from_keywords_with_offsets(kws, hns, st)
            .map_commutative_warnings(StdDatasetFromFlatTEXTWarning::from)
            .map_errors(StdDatasetFromFlatTextErrorInner::from)
            .group()
            .map_error(IOErrorGroup::Pure)
            .and_then_commutative(|(core, std_diag, mut offsets, repair_diag)| {
                let or = hns.header.final_offsets.others_reader();
                let ar = AnalysisReader::new(offsets.offsets.final_analysis);
                let other = io_to_log!(or.h_read(h));
                let analysis = io_to_log!(ar.h_read(h));
                let version = core.fcs_version();
                let final_data = &mut offsets.offsets.final_data;
                core.meas
                    .h_read_df(h, offsets.tot, final_data, st.conf().as_ref())
                    .map_commutative_warnings(StdDatasetFromFlatTEXTWarning::from)
                    .map_pure_errors(StdDatasetFromFlatTextErrorInner::from)
                    .and_then_commutative(|df_out| {
                        let ed = df_out.diagnostics;
                        let d = &offsets.offsets;
                        let v = version;
                        let s = scan_next_dataset;
                        let ns = core.nonstandard_keywords;
                        let new = Self::new(core.rootmeta, df_out.inner, ns, analysis, other);
                        DatasetDiagnostics::from_parts(h, v, ed, hns, d, s, st)
                            .map_commutative_warnings(StdDatasetFromFlatTEXTWarning::from)
                            .map_pure_errors(StdDatasetFromFlatTextErrorInner::from)
                            .repack_warnings()
                            .map_ok_value(|ds_diag| {
                                let diag = StdDatasetFromKwsOutput::new(
                                    offsets.offsets,
                                    repair_diag,
                                    std_diag,
                                    ds_diag,
                                );
                                (new, diag)
                            })
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
        let fil = conf
            .inner
            .text
            .override_fil
            .is_set()
            .then(|| path_to_ne_string(path))
            .flatten();
        self.h_write_dataset(&mut h, &conf.inner, conf.multi.appendable, fil)
    }

    /// Write this dataset (HEADER+TEXT+DATA+ANALYSIS+OTHER) to a handle
    pub fn h_write_dataset<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        conf: &WriteDatasetInnerConfig,
        has_nextdata: AppendableFlag,
        fil: Option<NEString>,
    ) -> WarningsAndIOGroupResult<Nextdata, EventOverRangeError, StdWriterError, WriteDatasetSummary>
    {
        let df = self.meas.data();
        let delim = conf.text.delim;
        let tot = Tot(df.nrows());
        let analysis_len = self.analysis.0.len().usize_to_u64();
        let other_segs = &self.others.0[..];
        let mut digest = WriteFCSDigest::new(conf.text.compute_crc, V::as_version());

        df.check_ranges(conf.allow_over_bitmask, conf.disallow_over_range)
            .map_errors(StdWriterError::from)
            .group()
            .map_error(IOErrorGroup::Pure)
            // write HEADER+TEXT+OTHER(s) first
            .and_then_commutative(|()| {
                let data_len = df.nbytes();
                let ht_conf = WriteHeaderAndTextConfig {
                    delim,
                    tot,
                    data_len,
                    analysis_len,
                    other_segs,
                    has_nextdata,
                    fil,
                };
                let res = if conf.text.big_other.is_set() {
                    self.h_write_text_inner::<_, UintSpacePad20>(h, &ht_conf, &mut digest)
                } else {
                    self.h_write_text_inner::<_, UintSpacePad8>(h, &ht_conf, &mut digest)
                };
                res.map_err(|e| e.fmap_once(StdWriterError::from))
                    .map_err(IOErrorGroup::from)
                    .into_log()
            })
            // write DATA+ANALYSIS+CRC
            .and_commutative(|| {
                io_to_log!(df.h_write_df(h, &mut digest, conf));
                io_to_log!(digest.update_and_write(h, self.analysis.0.as_bytes()));
                io_to_log!(digest.write_final(h));
                LogResult::new_ok(())
            })
            .deanonymize()
    }

    /// Return reference to DATA segment as dataframe.
    pub fn data(&self) -> PrimitiveDataFrame
    where
        V::DataFrame: Clone + Into<PrimitiveDataFrame>,
    {
        self.meas.data().clone().into()
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
        self.meas.set_data(df)
    }

    /// Remove all measurements and data
    pub fn unset_data(&mut self) -> Result<(), ExistingLinkErrors> {
        self.unset_measurements_inner()?;
        self.meas.clear();
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
        disallow_bitmask_trunc: OverBitmaskAction,
        over_range_action: OverRangeAction,
    ) -> WarningsAndGroupResult<
        Vec<Option<usize>>,
        EventOverRangeError,
        EventOverRangeError,
        EventOverRangeSummary,
    > {
        self.meas
            .check_ranges(disallow_bitmask_trunc, over_range_action)
            .group()
            .map_ok_value(|rs| rs.fmap(|x| x.map(|(i, _)| i)))
    }

    /// Get data schema.
    pub fn data_schema(&self) -> V::DataSchema
    where
        V::DataFrame: DataFrameAsDataSchema<DataSchema = V::DataSchema>,
    {
        self.meas.data().as_data_schema()
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
        V::DataFrame: Clone + Into<PrimitiveDataFrame> + Default,
        V::DataSchema: WithPrimitiveDataFrame<DfTarget = V::DataFrame>,
    {
        self.meas.set_dataframe_schema(&data_schema)
    }

    /// Set measurements without $PnN and data_schema
    #[allow(clippy::needless_pass_by_value)]
    // pass by value here to keep api consistent b/t coretext and coredataset
    pub fn set_measurements_and_data_schema(
        &mut self,
        measurements: VTemporalsAndOpticals<V>,
        data_schema: V::DataSchema,
    ) -> Result<(), DatasetSetUnnamedMeasAndDataSchemaError>
    where
        V::DataFrame: Clone + Into<PrimitiveDataFrame> + Default,
        V::DataSchema: WithPrimitiveDataFrame<DfTarget = V::DataFrame>,
    {
        // NOTE no check for broken links since this doesn't touch names
        self.meas
            .set_unnamed_measurements_dataframe_schema(measurements, &data_schema)
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
        measurements: VNamedTemporalsAndOpticalsWithScale<V>,
        data_schema: V::DataSchema,
        allow_shared_names: bool,
        skip_index_check: bool,
    ) -> Result<(), DatasetSetNamedMeasAndDataSchemaError>
    where
        V::DataFrame: Clone + Into<PrimitiveDataFrame> + Default,
        V::DataSchema: WithPrimitiveDataFrame<DfTarget = V::DataFrame>,
    {
        let go = |cur_meas: &_, new_meas: &_| {
            self.rootmeta.new_meas_link_errors(
                cur_meas,
                new_meas,
                allow_shared_names,
                skip_index_check,
            )
        };
        self.meas
            .set_named_measurements_and_dataframe_schema_with(measurements, &data_schema, go)
    }

    /// Remove a measurement matching the given name.
    ///
    /// Return removed measurement and its index if found.
    pub fn remove_measurement_by_name<R>(
        &mut self,
        n: &Shortname,
    ) -> Result<
        (
            MeasIndex,
            VTemporalOrOpticalWithScale<V>,
            AnyPrimitiveSeries,
            R,
        ),
        RemoveMeasByNameError,
    >
    where
        V::DataFrame: LayoutRemove<RangeAndSeries<R>>,
    {
        let (index, meas, (rng, col)) = self.remove_measurement_by_name_inner(n)?;
        Ok((index, meas, col, rng))
    }

    #[allow(clippy::type_complexity)]
    #[cfg(feature = "python")]
    pub fn py_remove_measurement_by_name<R>(
        &mut self,
        n: &Shortname,
    ) -> Result<
        (
            MeasIndex,
            VTemporalOrOptical<V>,
            AnyPrimitiveSeries,
            R,
            <V::OpticalScale as PySplitScale>::MaybeScale,
        ),
        RemoveMeasByNameError,
    >
    where
        V::DataFrame: LayoutRemove<RangeAndSeries<R>>,
        V::OpticalScale: PySplitScale,
    {
        let go = |(i, m, c, r)| {
            let (mm, s) = V::OpticalScale::split_scale(m);
            (i, mm, c, r, s)
        };
        self.remove_measurement_by_name(n).map(go)
    }

    #[allow(clippy::type_complexity)]
    #[cfg(feature = "python")]
    pub fn py_remove_measurement_by_name_typed<R>(
        &mut self,
        n: &Shortname,
    ) -> Result<
        (
            MeasIndex,
            VTemporalOrOptical<V>,
            AnyPrimitiveSeries,
            FullRange,
            <V::OpticalScale as PySplitScale>::MaybeScale,
            Option<R>,
        ),
        RemoveMeasByNameError,
    >
    where
        R: PyRangeType,
        V::DataFrame: LayoutRemove<RangeAndSeries<R::Range>>,
        V::OpticalScale: PySplitScale,
    {
        let go = |(i, m, c, r)| {
            let (mm, s) = V::OpticalScale::split_scale(m);
            let (rr, t) = R::split_range(r);
            (i, mm, c, rr, s, t)
        };
        self.remove_measurement_by_name(n).map(go)
    }

    /// Remove a measurement at a given position
    ///
    /// Return removed measurement and its name if found.
    pub fn remove_measurement_by_index<R>(
        &mut self,
        index: MeasIndex,
    ) -> Result<(VPairedTemporalOrOpticalWithScale<V>, AnyPrimitiveSeries, R), RemoveMeasByIndexError>
    where
        V::DataFrame: LayoutRemove<RangeAndSeries<R>>,
    {
        let (meas, (rng, col)) = self.remove_measurement_by_index_inner(index)?;
        Ok((meas, col, rng))
    }

    #[allow(clippy::type_complexity)]
    #[cfg(feature = "python")]
    pub fn py_remove_measurement_by_index<R>(
        &mut self,
        index: MeasIndex,
    ) -> Result<
        (
            V::Name,
            VTemporalOrOptical<V>,
            AnyPrimitiveSeries,
            R,
            <V::OpticalScale as PySplitScale>::MaybeScale,
        ),
        RemoveMeasByIndexError,
    >
    where
        V::Name: MightHave<Shortname>,
        V::DataFrame: LayoutRemove<RangeAndSeries<R>>,
        V::OpticalScale: PySplitScale,
    {
        let go = |(p, c, r): (EitherPair<_, _, _>, _, _)| {
            let (n, m) = p.unzip();
            let (mm, s) = V::OpticalScale::split_scale(m);
            (n, mm, c, r, s)
        };
        self.remove_measurement_by_index(index).map(go)
    }

    #[allow(clippy::type_complexity)]
    #[cfg(feature = "python")]
    pub fn py_remove_measurement_by_index_typed<R>(
        &mut self,
        index: MeasIndex,
    ) -> Result<
        (
            V::Name,
            VTemporalOrOptical<V>,
            AnyPrimitiveSeries,
            FullRange,
            <V::OpticalScale as PySplitScale>::MaybeScale,
            Option<R>,
        ),
        RemoveMeasByIndexError,
    >
    where
        V::Name: MightHave<Shortname>,
        R: PyRangeType,
        V::DataFrame: LayoutRemove<RangeAndSeries<R::Range>>,
        V::OpticalScale: PySplitScale,
    {
        let go = |(p, c, r): (EitherPair<_, _, _>, _, _)| {
            let (n, m) = p.unzip();
            let (mm, s) = V::OpticalScale::split_scale(m);
            let (rr, t) = R::split_range(r);
            (n, mm, c, rr, s, t)
        };
        self.remove_measurement_by_index(index).map(go)
    }

    /// Convert this struct into [`CoreTEXT`].
    ///
    /// This simply entails taking ownership and dropping the ANALYSIS and DATA
    /// fields.
    pub fn into_coretext(self) -> VersionedCoreTEXT<V>
    where
        V::DataFrame: DataFrameAsDataSchema<DataSchema = V::DataSchema>,
    {
        CoreTEXT::new(
            self.rootmeta,
            self.meas.without_data(),
            self.nonstandard_keywords,
            (),
            (),
        )
    }

    /// Set measurements and dataframe together
    ///
    /// Length of measurements must match the width of the input dataframe.
    pub fn set_named_measurements_and_data(
        &mut self,
        measurements: VNamedTemporalsAndOpticalsWithScale<V>,
        df: PrimitiveDataFrame,
        allow_shared_names: bool,
        skip_index_check: bool,
    ) -> Result<(), SetNamedMeasurementsAndDataError>
    where
        V::DataFrame: WithPrimitiveDataFrame<DfTarget = V::DataFrame>,
    {
        let go = |cur_meas: &_, new_meas: &_| {
            self.rootmeta.new_meas_link_errors(
                cur_meas,
                new_meas,
                allow_shared_names,
                skip_index_check,
            )
        };
        self.meas
            .set_named_measurements_and_data_with(measurements, df, go)
    }

    /// Set measurements without $PnN and dataframe together
    ///
    /// Length of measurements must match the width of the input dataframe.
    pub fn set_measurements_and_data(
        &mut self,
        measurements: VTemporalsAndOpticals<V>,
        df: PrimitiveDataFrame,
    ) -> Result<(), SetUnnamdMeasurementsAndDataError>
    where
        V::DataFrame: WithPrimitiveDataFrame<DfTarget = V::DataFrame>,
    {
        self.meas
            .set_unnamed_measurements_and_data(measurements, df)
    }

    /// Set measurements without $PnN, data schema, and data itself together
    ///
    /// Each input must represent the same number of columns.
    #[allow(clippy::needless_pass_by_value)]
    pub fn set_measurements_data_schema_and_data(
        &mut self,
        measurements: VTemporalsAndOpticals<V>,
        data_schema: V::DataSchema,
        df: PrimitiveDataFrame,
    ) -> Result<(), SetUnnamdMeasurementsAndDataSchemaAndDataFrameError>
    where
        V::DataSchema: WithPrimitiveDataFrame<DfTarget = V::DataFrame>,
    {
        let new_df = data_schema.with_data(df)?;
        self.set_measurements_and_layout_inner(measurements, new_df)?;
        Ok(())
    }
}

// Implement methods for anycore*

#[derive(new)]
pub(crate) struct AnyCoreOutput<T> {
    pub(crate) inner: T,
    pub(crate) std_diag: StdTEXTDiagnostics,
    pub(crate) offsets: TEXTOffsets<Option<Tot>>,
    pub(crate) repair_diag: RepairDiagnostics,
    pub(crate) scores: Option<KeywordVersionScores>,
}

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

            // NDArrays are row-major so this should print row-by-row
            for (row, n) in matrix.outer_iter().zip(&names[..]) {
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
    fn spillover_or_comp_table(&self) -> Option<(Vec<Shortname>, Array2<f32>)> {
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
        offsets: &mut HeaderAndSuppOffsets,
        st: &TEXTReadState<C>,
    ) -> WarningsAndErrorsResult<
        AnyCoreOutput<Self>,
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
        #[derive(AsRef)]
        struct LookupConfig {
            #[as_ref(EvaledReadStdKeywordsConfig)]
            std: EvaledReadStdKeywordsConfig,
            #[as_ref(EvaledReadDataKeywordsConfig)]
            data: EvaledReadDataKeywordsConfig,
            #[as_ref(ReadOffsetConfig)]
            offsets: ReadOffsetConfig,
        }

        macro_rules! go {
            ($t:ident, $s:expr, $st:expr) => {
                $t::new_from_keywords_with_offsets(kws, offsets, $st)
                    .map_ok_value(|(a, b, c, d)| {
                        AnyCoreOutput::new(a.into(), b, c.into_common(), d, $s)
                    })
                    .map_errors(StdTEXTFromFlatTEXTError::from)
            };
        }

        let sconf: &ReadHeaderAndTEXTConfig = st.conf().as_ref();

        AsRef::<ReadDataKeywordsConfig>::as_ref(st.conf())
            .eval(&kws)
            .map_ok_value(|data| {
                st.as_ref().first_once(|conf| LookupConfig {
                    std: AsRef::<ReadStdKeywordsConfig>::as_ref(&conf).eval(&kws),
                    data,
                    offsets: *AsRef::<ReadOffsetConfig>::as_ref(&conf),
                })
            })
            .map_errors(StdTEXTFromFlatTEXTErrorInner::from)
            .map_errors(StdTEXTFromFlatTEXTError::from)
            .nowarn_into_warn()
            .and_then_commutative(|lst| {
                match autodetect_version(version, &kws.std, sconf.version_override.as_ref()) {
                    Ok((ver, scores)) => match ver {
                        Version::FCS2_0 => go!(CoreTEXT2_0, scores, &lst),
                        Version::FCS3_0 => go!(CoreTEXT3_0, scores, &lst),
                        Version::FCS3_1 => go!(CoreTEXT3_1, scores, &lst),
                        Version::FCS3_2 => go!(CoreTEXT3_2, scores, &lst),
                    },
                    Err(e) => LogResult::new_err(StdTEXTFromFlatTEXTError::from(e)),
                }
            })
    }
}

impl AnyCoreDataset {
    #[must_use]
    pub fn as_data(&self) -> PrimitiveDataFrame {
        match_anycore!(self, x, { x.meas.data().clone().into() })
    }

    #[must_use]
    pub fn datatypes(&self) -> Vec<AlphaNumType> {
        match_anycore!(self, x, { x.meas.data().datatypes() })
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
        scan_next_dataset: bool,
        st: &TEXTReadState<C>,
    ) -> WarningsAndIOGroupResult<
        (Self, StdDatasetFromKwsOutput, Option<KeywordVersionScores>),
        StdDatasetFromFlatTEXTWarning,
        AnyStdDatasetFromFlatTextError,
        (),
    >
    where
        R: Read + Seek,
        C: AsRef<ReadHeaderAndTEXTConfig>
            + AsRef<ReadOffsetConfig>
            + AsRef<ReadStdKeywordsConfig>
            + AsRef<ReadDataKeywordsConfig>
            + AsRef<ReadDatasetConfig>,
    {
        #[derive(AsRef)]
        struct LookupConfig {
            #[as_ref(EvaledReadStdKeywordsConfig)]
            std: EvaledReadStdKeywordsConfig,
            #[as_ref(EvaledReadDataKeywordsConfig)]
            data: EvaledReadDataKeywordsConfig,
            #[as_ref(ReadOffsetConfig)]
            offsets: ReadOffsetConfig,
            #[as_ref(ReadDatasetConfig)]
            dataset: ReadDatasetConfig,
        }

        let version = hns.header.version;
        macro_rules! go {
            ($t:ident, $s:expr, $st:expr) => {
                $t::new_from_keywords_inner(h, kws, hns, scan_next_dataset, $st)
                    .map_ok_value(|(x, y)| (x.into(), y, $s))
                    .map_pure_errors(AnyStdDatasetFromFlatTextError::from)
            };
        }

        let sconf: &ReadHeaderAndTEXTConfig = st.conf().as_ref();

        AsRef::<ReadDataKeywordsConfig>::as_ref(st.conf())
            .eval(&kws)
            .map_ok_value(|data| {
                st.as_ref().first_once(|conf| LookupConfig {
                    std: AsRef::<ReadStdKeywordsConfig>::as_ref(&conf).eval(&kws),
                    data,
                    offsets: *AsRef::<ReadOffsetConfig>::as_ref(&conf),
                    dataset: *AsRef::<ReadDatasetConfig>::as_ref(&conf),
                })
            })
            .map_errors(StdTEXTFromFlatTEXTErrorInner::from)
            .map_errors(StdDatasetFromFlatTextErrorInner::from)
            .map_errors(AnyStdDatasetFromFlatTextError::from)
            .nowarn_into_warn()
            .group()
            .map_error(IOErrorGroup::Pure)
            .and_then_commutative(|lst| {
                match autodetect_version(version, &kws.std, sconf.version_override.as_ref()) {
                    Ok((ver, scores)) => match ver {
                        Version::FCS2_0 => go!(CoreDataset2_0, scores, &lst),
                        Version::FCS3_0 => go!(CoreDataset3_0, scores, &lst),
                        Version::FCS3_1 => go!(CoreDataset3_1, scores, &lst),
                        Version::FCS3_2 => go!(CoreDataset3_2, scores, &lst),
                    },
                    Err(e) => LogResult::new_err(IOErrorGroup::new_pure_one(e.into())),
                }
            })
    }
}

// Implement methods for misc types

impl UnstainedData {
    fn lookup<C>(
        kws: &mut ValidKeywords,
        dropped: &mut StdKeywords,
        conf: &C,
    ) -> DeferredSwitchableError<
        DiagnosedUnstainedData<Self>,
        DummyTriFlag,
        OptKeyStError<UnstainedCenters>,
    >
    where
        C: AsRef<EvaledReadDataKeywordsConfig> + AsRef<EvaledReadStdKeywordsConfig>,
    {
        let i = UnstainedInfo::remove_root_opt_nofail(&mut kws.std);
        UnstainedCenters::remove_or_drop_root_opt_with(kws, dropped, (), conf).map_deferred_value(
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
        kws: &mut ValidKeywords,
        dropped: &mut StdKeywords,
        conf: &EvaledReadDataKeywordsConfig,
    ) -> DeferredWarningsAndErrors<Self, LookupSubsetError, LookupSubsetError> {
        let f =
            CSVFlags::lookup(kws, dropped, conf).map_warnings_and_errors(LookupSubsetError::from);
        let b = CSVBits::remove_or_drop_root_opt(kws, dropped, conf)
            .map_switchable_errors(LookupSubsetError::from)
            .switchable_into_commutative()
            .into_semigroup();
        let t = CSTot::remove_or_drop_root_opt(kws, dropped, conf)
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
        kws: &mut ValidKeywords,
        dropped: &mut StdKeywords,
        conf: &EvaledReadDataKeywordsConfig,
    ) -> DeferredWarningsAndErrors<Self, LookupCSVFlagsError, LookupCSVFlagsError> {
        CSMode::remove_or_drop_root_opt(kws, dropped, conf)
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
                        CSVFlag::remove_or_drop_meas_opt(kws, dropped, i, conf)
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
        kws: &mut ValidKeywords,
        dropped: &mut StdKeywords,
        conf: &C,
    ) -> DeferredWarningsAndErrors<
        Diagnosed<Self, Option<String>>,
        LookupModifiedDataError,
        LookupModifiedDataError,
    >
    where
        C: AsRef<EvaledReadDataKeywordsConfig> + AsRef<EvaledReadStdKeywordsConfig>,
    {
        let last_mod = LastModifier::remove_root_opt_nofail(&mut kws.std);
        let last_mod_date = LastModified::remove_or_drop_root_opt_with(kws, dropped, (), conf)
            .map_switchable_errors(LookupModifiedDataError::from)
            .switchable_into_commutative()
            .into_semigroup();
        let ori = Originality::remove_or_drop_root_opt(kws, dropped, conf.as_ref())
            .map_switchable_errors(LookupModifiedDataError::from)
            .switchable_into_commutative()
            .into_semigroup();
        last_mod_date.lift_f2_once(ori, |d, o| {
            let ret = Self::new(last_mod, d.inner, o);
            Diagnosed::new(ret, d.diagnostic)
        })
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

impl DatasetOffsets {
    fn try_new(
        mut data: HeaderOrTextOffsets<DataSegmentId>,
        mut analysis: HeaderOrTextOffsets<AnalysisSegmentId>,
        limit: OverlapCorrectionLimit,
    ) -> Result<Self, OffsetPairsOverlapError<TextOffsetsName, TextOffsetsName>> {
        // Check for overlaps if we have two non-empty segments that are both
        // from TEXT. We can assume that if they are both from HEADER that
        // this has already been checked.
        let da_overlap = if let (
            HeaderOrTextOffsets::Text { seg: dt, .. },
            HeaderOrTextOffsets::Text { seg: at, .. },
        ) = (&mut data, &mut analysis)
            && let (Some(d_ne), Some(a_ne)) = (dt.as_nonempty_mut(), at.as_nonempty_mut())
        {
            let res = if d_ne.begin() < a_ne.begin() {
                d_ne.tail_overlap_pair_and_truncate(&a_ne, limit.0, ())
            } else {
                a_ne.tail_overlap_pair_and_truncate(&d_ne, limit.0, ())
            };
            if let Some(r) = res {
                if r.truncated {
                    // TODO add more detail
                    Some(r.overlap.overlap)
                } else {
                    return Err(OffsetPairsOverlapError(r.overlap));
                }
            } else {
                None
            }
        } else {
            None
        };
        let (dseg, dorig) = data.into_any();
        let (aseg, aorig) = analysis.into_any();
        Ok(Self::new(dseg, aseg, dorig, aorig, da_overlap))
    }

    pub(crate) fn max_end_offset(&self) -> Option<u64> {
        let d = self.final_data.as_nonempty().map(|o| o.end());
        let a = self.final_analysis.as_nonempty().map(|o| o.end());
        d.max(a)
    }
}

impl TEXTOffsetsOrigin {
    #[cfg(feature = "python")]
    pub fn py_try_new(
        level: py::TEXTOffsetOriginType,
        uncorr: Option<OriginalOffsets>,
        overlaps: Vec<TextToHeaderOrSuppOffsetsOverlap>,
        overflow: Option<TextOffsetsOverflow>,
    ) -> PyResult<Self> {
        let ret = match (level, uncorr, &overlaps[..], overflow) {
            (py::TEXTOffsetOriginType::EmptyTEXT, None, [], None) => Self::EmptyTEXT,
            (py::TEXTOffsetOriginType::Ignored, u, [], None) => Self::Ignored(u),
            (py::TEXTOffsetOriginType::Unparsed, None, [], None) => Self::Unparsed,
            (py::TEXTOffsetOriginType::Malformed, Some(u), [], None) => Self::Malformed(u),
            (py::TEXTOffsetOriginType::Match, None, [], None) => Self::Match,
            (py::TEXTOffsetOriginType::MismatchHeader, Some(u), [], None) => {
                Self::MismatchHeader(u)
            }
            (py::TEXTOffsetOriginType::MismatchTEXT, Some(u), _, _) => Self::MismatchTEXT(
                MismatchedTEXTOffsetOrigin::new(false, u, overlaps, overflow),
            ),
            (py::TEXTOffsetOriginType::EmptyHeader, Some(u), _, _) => {
                Self::MismatchTEXT(MismatchedTEXTOffsetOrigin::new(true, u, overlaps, overflow))
            }
            _ => {
                return Err(PyValueError::new_err(
                    "invalid combination of level and values, see class-level docstring",
                ));
            }
        };
        Ok(ret)
    }

    #[cfg(feature = "python")]
    #[must_use]
    pub fn py_origin_type(&self) -> py::TEXTOffsetOriginType {
        match self {
            Self::EmptyTEXT => py::TEXTOffsetOriginType::EmptyTEXT,
            Self::Ignored(_) => py::TEXTOffsetOriginType::Ignored,
            Self::Unparsed => py::TEXTOffsetOriginType::Unparsed,
            Self::Malformed(_) => py::TEXTOffsetOriginType::Malformed,
            Self::Match => py::TEXTOffsetOriginType::Match,
            Self::MismatchHeader(_) => py::TEXTOffsetOriginType::MismatchHeader,
            Self::MismatchTEXT(x) => {
                if x.header_is_empty {
                    py::TEXTOffsetOriginType::EmptyHeader
                } else {
                    py::TEXTOffsetOriginType::MismatchTEXT
                }
            }
        }
    }

    #[cfg(feature = "python")]
    #[must_use]
    pub fn py_original_offsets(&self) -> Option<OriginalOffsets> {
        match self {
            Self::EmptyTEXT | Self::Unparsed | Self::Match => None,
            Self::MismatchHeader(u) | Self::Malformed(u) => Some(*u),
            Self::Ignored(u) => *u,
            Self::MismatchTEXT(x) => Some(x.uncorr),
        }
    }

    #[cfg(feature = "python")]
    #[must_use]
    pub fn py_overlaps(&self) -> &[TextToHeaderOrSuppOffsetsOverlap] {
        if let Self::MismatchTEXT(x) = self {
            &x.overlaps[..]
        } else {
            &[]
        }
    }

    #[cfg(feature = "python")]
    #[must_use]
    pub fn py_overflow(&self) -> Option<TextOffsetsOverflow> {
        if let Self::MismatchTEXT(x) = self {
            x.overflow
        } else {
            None
        }
    }
}

impl IntraSegmentDarkBytes {
    pub(crate) fn read_all<R: io::Read + Seek>(
        h: &mut BufReader<R>,
        ptext: PrimaryTextOffsets,
        stext: Option<SupplementalTextOffsets>,
        data: AnyDataOffsets,
        analysis: AnyAnalysisOffsets,
        other: &[IndexedOtherOffsets],
    ) -> io::Result<Vec<Self>> {
        macro_rules! go {
            ($pair:expr, $name:expr) => {
                $pair
                    .as_nonempty()
                    .map(|ne| (ne.abs_begin(), ne.abs_end(), $name))
            };
        }
        let mut ret = vec![];
        let mut buf = vec![];
        let pairs = [
            go!(ptext, FlankingSegmentName::PrimaryText),
            go!(data, FlankingSegmentName::Data),
            go!(analysis, FlankingSegmentName::Analysis),
        ]
        .into_iter()
        .flatten()
        .chain(stext.and_then(|s| go!(s, FlankingSegmentName::SupplementalText)))
        .chain(
            other
                .iter()
                .filter_map(|o| go!(o.offsets, FlankingSegmentName::Other(o.index))),
        )
        .sorted_by_key(|(b, _, _)| *b)
        .tuple_windows();
        for ((_, end0, n0), (start1, _, n1)) in pairs {
            let nbytes = start1
                .checked_sub(end0)
                .expect("offsets should not overlap");
            if nbytes == 0 {
                continue;
            }
            h.seek(io::SeekFrom::Start(end0))?;
            h.take(nbytes).read_to_end(&mut buf)?;
            let bytes = DarkBytes::try_from_slice(&buf[..]).unwrap();
            let dark = Self {
                prev: n0,
                next: n1,
                start: end0,
                end: start1,
                bytes,
            };
            ret.push(dark);
            buf.clear();
        }
        Ok(ret)
    }
}

impl DarkBytes {
    pub(crate) fn try_from_vec(bytes: Vec<u8>) -> Option<Self> {
        let ne = NEVec::try_from_vec(bytes)?;
        let (x, x0) = ne.split_first();
        let ret = if x0.iter().all(|y| y == x) {
            Self::Padding {
                character: *x,
                n: usize::from(ne.len()),
            }
        } else {
            match NEString::from_utf8(ne) {
                Ok(s) => Self::Utf8(s),
                Err(e) => Self::Bytes(e.into_bytes()),
            }
        };
        Some(ret)
    }

    pub(crate) fn try_from_slice(bytes: &[u8]) -> Option<Self> {
        let ne = NESlice::try_from_slice(bytes)?;
        let (x, x0) = ne.split_first();
        let ret = if x0.iter().all(|y| y == x) {
            Self::Padding {
                character: *x,
                n: usize::from(ne.len()),
            }
        } else if let Ok(s) = NEStr::from_utf8(&ne) {
            Self::Utf8(s.to_owned())
        } else {
            Self::Bytes(ne.to_ne_vec())
        };
        Some(ret)
    }

    #[cfg(feature = "python")]
    fn try_from_string(s: String) -> Option<Self> {
        let ne = NEString::try_from(s).ok()?;
        Some(Self::Utf8(ne))
    }
}

impl DatasetDiagnostics {
    pub(crate) fn from_parts<R, C>(
        h: &mut BufReader<R>,
        version: Version,
        events: EventsDiagnostics,
        header_supp_offsets: &HeaderAndSuppOffsets,
        dataset_offsets: &DatasetOffsets,
        scan_next_dataset: bool,
        st: &TEXTReadState<C>,
    ) -> WarningAndIOGroupResult<Self, CRCError, CRCError, ()>
    where
        R: Read + Seek,
        C: AsRef<ReadDatasetConfig>,
    {
        let dconf: &ReadDatasetConfig = st.conf().as_ref();
        // First, find next byte after the last segment in the dataset. Find the
        // max of the TEXT+STEXT+OTHER first followed by DATA+ANALYSIS. The
        // latter are separate since these could come from either TEXT or
        // HEADER, and this data is in a separate struct. At minimum, this
        // should always be at least the byte after TEXT since TEXT should be
        // valid and non-empty if we got this far.
        let hns_max = header_supp_offsets.text_other_max_end_offset();
        let da_max = dataset_offsets.max_end_offset();
        let max_end_offset = da_max.map_or(hns_max, |x| x.max(hns_max));

        // If desired, read any "dark bytes" between segments that have been
        // previously read. This involves many scattered reads across the file
        // so it is turned off by default.
        let intra_seg_dark = if dconf.read_intra_segment_dark_bytes.is_set() {
            io_to_log!(IntraSegmentDarkBytes::read_all(
                h,
                header_supp_offsets.header.final_offsets.text(),
                header_supp_offsets.supp_text.final_offsets(),
                dataset_offsets.final_data,
                dataset_offsets.final_analysis,
                header_supp_offsets.header.final_offsets.other_ref(),
            ))
        } else {
            vec![]
        };

        // If desired, manually determine the "real" boundary of this dataset by
        // scanning the file for a string like "FCSX.Y ". Start this seek after
        // the last known segment as was read previously. This is also expensive
        // since it involves another random seek and a multi-byte rolling
        // comparison across potentially the entire rest of file. This is meant
        // for cases where $NEXTDATA cannot be trusted (which is surprisingly
        // often). In the ideal case, $NEXTDATA is correct, in which case this
        // scan will advance 8 bytes if the CRC exists, and 0 bytes if the CRC
        // does not exist. This step must be done before finding the CRC since
        // we do not know a priori if the CRC exists or not, and we should not
        // bother trying to parse it if there are no bytes left in the dataset.
        let next_dataset_abs_start = if scan_next_dataset {
            io_to_log!(h.seek(io::SeekFrom::Start(st.dataset_offset().0 + max_end_offset)));
            io_to_log!(next_dataset_boundary(h))
        } else {
            st.dataset_bounds()
                .from_nextdata
                .then_some(st.dataset_offset().0 + st.dataset_bounds().len.0)
                .map(DatasetOffset)
        };
        let dataset_abs_end = next_dataset_abs_start.map_or(st.file_len().0, |x| x.0);

        // Read the CRC value, and optionally test the CRC against the CRC
        // computed from the dataset contents. The latter is relatively
        // expensive since we must read the entire dataset again to compute its
        // CRC; therefore it is turned off by default.
        st.test_crc(h, max_end_offset, dataset_abs_end, version, dconf)
            .and_then_nowarn_commutative(|(file_crc, computed_crc)| {
                // Compute the final dataset length by adding the CRC length
                // to the value of the byte offset after the last segment found
                // above.
                let crc_len = if file_crc
                    .as_ref()
                    .is_some_and(|c| matches!(c, CRCOutput::Valid(_)))
                {
                    u64::from(CRC_LEN)
                } else {
                    0
                };
                let dataset_len = crc_len + max_end_offset;

                // If desired, read the "dark bytes" after the CRC and before
                // the next dataset. This is turned off by default since it
                // involves another random read to the file. It also could be
                // potentially large. If $NEXTDATA is 0 and manual scanning was
                // either turned off or didn't find another dataset, this will
                // read until EOF and return every byte as-is. Some files
                // (CyTOF) are known to misuse $NEXTDATA and/or save large data
                // dumps at the end of an FCS file. This is meant for such cases
                // where one wants/knows that this data exists, but most users
                // won't care and it is alot of extra overhead.
                let post_dataset_abs_start = st.dataset_offset().0 + dataset_len;
                let post_dark = if let Some(post_dataset_nbytes) =
                    dataset_abs_end.checked_sub(post_dataset_abs_start)
                    && dconf.read_post_dataset_dark_bytes.is_set()
                {
                    let mut buf = vec![];
                    io_to_log!(h.seek(io::SeekFrom::Start(post_dataset_abs_start)));
                    io_to_log!(h.take(post_dataset_nbytes).read_to_end(&mut buf));
                    buf
                } else {
                    vec![]
                };

                let ret = Self::new(
                    events.pre.event_width,
                    events.pre.event_data_remainder,
                    events.pre.tot_event_mismatch,
                    events.overrange_columns,
                    intra_seg_dark,
                    DarkBytes::try_from_vec(post_dark),
                    file_crc,
                    computed_crc,
                    dataset_len,
                    next_dataset_abs_start,
                    scan_next_dataset,
                );
                LogResult::new_ok(ret)
            })
    }
}

// Misc functions

/// Make all keys unique if they are not already.
///
/// Do this by appending "~X" to keys which are not unique and incrementing "X"
/// starting at 0.
///
/// Return vector of original names if they were changed.
pub(crate) fn uniquify_names<K>(xs: &mut [K]) -> Vec<Option<Shortname>>
where
    K: MightHave<Shortname>,
{
    // First get list of all duplicates by collecting all names and pairing with
    // their indices. Any key with more than one index is duplicated and should
    // be processed later.
    let mut counts: HashMap<&Shortname, NEVec<usize>> = HashMap::new();
    let mut original = vec![];
    original.resize_with(xs.len(), || None); // Avoid using Clone for Option<K>
    for (i, k) in xs.iter().enumerate() {
        if let Some(n) = k.as_opt() {
            match counts.entry(n) {
                Entry::Occupied(mut z) => {
                    z.get_mut().push(i);
                }
                Entry::Vacant(z) => {
                    z.insert_entry(NEVec::new(i));
                }
            }
        }
    }

    // Next make a list of replacement names corresponding to each index. For
    // each duplicated name, init a counter at 0 and increment this counter
    // until it results in a unique name. Once it is unique, save this with
    // its index and repeat for remaining indices under the duplicated name.
    // Finally, repeat this process for all duplicated names.
    //
    // ASSUME: we don't need to check "ghost names" (ie names that will be made
    // in place of missing names) because they will have a different prefix.
    // Ghost names will be like "P1", "P2", etc and deduped names (here) will be
    // like "P~1", "P~2", etc.
    let mut replacements: Vec<(usize, Shortname)> = vec![];
    for (key, indices) in counts.iter().filter(|(_, v)| usize::from(v.len()) > 1) {
        let mut n = 0;
        for i in indices {
            let mut new = key.increment(n);
            while counts.contains_key(&new) {
                n += 1;
                new = key.increment(n);
            }
            replacements.push((*i, new));
            n += 1;
        }
    }

    drop(counts);

    // Finally, replace the names themselves
    for (i, r) in replacements {
        // ASSUME this will never fail because these indices were obtained from
        // .enumerate and we are not changing the length of the slice
        original[i] = mem::replace(&mut xs[i], K::wrap(r)).to_opt();
    }

    assert!(
        all_unique_names(xs.iter().map(|k| k.as_opt())),
        "names are still not unique"
    );

    original
}

#[allow(clippy::ptr_arg)]
fn path_to_ne_string(p: &PathBuf) -> Option<NEString> {
    let n = p.as_path().file_name()?;
    let s = n.to_str()?;
    let ne = NEStr::try_new(s)?;
    Some(ne.to_owned())
}

mod private {
    pub struct NoTouchy;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uniquify_empty() {
        let mut xs: Vec<Option<Shortname>> = vec![];
        uniquify_names(&mut xs[..]);
        assert_eq!(xs, vec![]);
    }

    #[test]
    fn uniquify_good() {
        let mut xs = vec![
            Some("a".parse::<Shortname>().unwrap()),
            None,
            Some("b".parse::<Shortname>().unwrap()),
        ];
        uniquify_names(&mut xs[..]);
        assert_eq!(
            xs,
            vec![
                Some("a".parse::<Shortname>().unwrap()),
                None,
                Some("b".parse::<Shortname>().unwrap()),
            ]
        );
    }

    #[test]
    fn uniquify_bad() {
        let mut xs = vec![
            Some("a".parse::<Shortname>().unwrap()),
            None,
            Some("a".parse::<Shortname>().unwrap()),
        ];
        uniquify_names(&mut xs[..]);
        assert_eq!(
            xs,
            vec![
                Some("a~0".parse::<Shortname>().unwrap()),
                None,
                Some("a~1".parse::<Shortname>().unwrap()),
            ]
        );
    }
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
    use super::{CRCOutput, DarkBytes, FlankingSegmentName};

    use crate::data::{
        AnyDatatype, AnyUint, FullRange, MaybeTypedMixedRange, MaybeTypedRange,
        MaybeTypedVariableBitmask,
    };
    use crate::meas::{
        OpticalScale2_0, OpticalScale3_0, TemporalOrOptical, TemporalOrOpticalWithScale,
    };
    use crate::text::byteord::ArgBytes;
    use crate::text::named_vec::Element;
    use crate::validated::keys::StringOrBytes;

    use fireflow_types::python::{self as py, ColumnType, ConfigError};

    use pyo3::{IntoPyObjectExt as _, prelude::*};

    pub trait PySplitScale: Sized {
        type MaybeScale;

        fn split_scale<T, O>(
            e: TemporalOrOpticalWithScale<T, O, Self>,
        ) -> (TemporalOrOptical<T, O>, Self::MaybeScale);
    }

    impl PySplitScale for OpticalScale2_0 {
        type MaybeScale = Self;

        fn split_scale<T, O>(
            e: TemporalOrOpticalWithScale<T, O, Self>,
        ) -> (TemporalOrOptical<T, O>, Self::MaybeScale) {
            e.both(
                |t| (Element::Center(t), Self::none()),
                |(o, s)| (Element::NonCenter(o), s),
            )
        }
    }

    impl PySplitScale for OpticalScale3_0 {
        type MaybeScale = Option<Self>;

        fn split_scale<T, O>(
            e: TemporalOrOpticalWithScale<T, O, Self>,
        ) -> (TemporalOrOptical<T, O>, Self::MaybeScale) {
            e.both(
                |t| (Element::Center(t), None),
                |(o, s)| (Element::NonCenter(o), Some(s)),
            )
        }
    }

    pub trait PyRangeType: Sized {
        type Range;

        fn split_range(range: Self::Range) -> (FullRange, Option<Self>);
    }

    impl PyRangeType for ArgBytes {
        type Range = MaybeTypedVariableBitmask;

        fn split_range(range: Self::Range) -> (FullRange, Option<Self>) {
            match range {
                MaybeTypedRange::Untyped(x) => (x, None),
                MaybeTypedRange::Typed(x) => (x.into(), Some(Self(x.as_bytes()))),
            }
        }
    }

    impl PyRangeType for ColumnType {
        type Range = MaybeTypedMixedRange;

        fn split_range(range: Self::Range) -> (FullRange, Option<Self>) {
            match range {
                MaybeTypedRange::Untyped(x) => (x, None),
                MaybeTypedRange::Typed(x) => {
                    let w = match x {
                        AnyDatatype::Ascii(_) => Self::A,
                        AnyDatatype::Uint(y) => match y {
                            AnyUint::Uint08(_) => Self::U08,
                            AnyUint::Uint16(_) => Self::U16,
                            AnyUint::Uint24(_) => Self::U24,
                            AnyUint::Uint32(_) => Self::U32,
                            AnyUint::Uint40(_) => Self::U40,
                            AnyUint::Uint48(_) => Self::U48,
                            AnyUint::Uint56(_) => Self::U56,
                            AnyUint::Uint64(_) => Self::U64,
                        },
                        AnyDatatype::F32(_) => Self::F32,
                        AnyDatatype::F64(_) => Self::F64,
                    };
                    (x.into(), Some(w))
                }
            }
        }
    }

    impl<'py> FromPyObject<'_, 'py> for FlankingSegmentName {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            if let Ok(s) = obj.extract::<String>() {
                let ss = s.as_str();
                if ss == py::SEGMENT_NAME_TEXT.as_str() {
                    return Ok(Self::PrimaryText);
                } else if ss == py::SEGMENT_NAME_STEXT.as_str() {
                    return Ok(Self::SupplementalText);
                } else if ss == py::SEGMENT_NAME_DATA.as_str() {
                    return Ok(Self::Data);
                } else if ss == py::SEGMENT_NAME_ANALYSIS.as_str() {
                    return Ok(Self::Analysis);
                }
            } else if let Ok(i) = obj.extract::<usize>() {
                return Ok(Self::Other(i));
            }
            Err(ConfigError::new_err(format!(
                "must be one of {}, {}, {}, {} or a number \
                 which is the index of an OTHER segment.",
                py::SEGMENT_NAME_TEXT,
                py::SEGMENT_NAME_STEXT,
                py::SEGMENT_NAME_DATA,
                py::SEGMENT_NAME_ANALYSIS,
            )))
        }
    }

    impl<'py> IntoPyObject<'py> for FlankingSegmentName {
        type Target = PyAny;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            match self {
                Self::PrimaryText => py::SEGMENT_NAME_TEXT.as_str().into_bound_py_any(py),
                Self::SupplementalText => py::SEGMENT_NAME_STEXT.as_str().into_bound_py_any(py),
                Self::Data => py::SEGMENT_NAME_DATA.as_str().into_bound_py_any(py),
                Self::Analysis => py::SEGMENT_NAME_ANALYSIS.as_str().into_bound_py_any(py),
                Self::Other(i) => i.into_bound_py_any(py),
            }
        }
    }

    impl<'py> FromPyObject<'_, 'py> for CRCOutput {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            if let Ok(b) = obj.extract::<Vec<u8>>() {
                return Ok(Self::Invalid(StringOrBytes::from(b)));
            } else if let Ok(crc) = obj.extract::<u16>() {
                return Ok(Self::Valid(crc));
            }
            Err(ConfigError::new_err(
                "must be an 8-character byte string or a 16-bit integer",
            ))
        }
    }

    impl<'py> IntoPyObject<'py> for CRCOutput {
        type Target = PyAny;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            match self {
                Self::Valid(crc) => crc.into_bound_py_any(py),
                Self::Invalid(v) => v.into_bound_py_any(py),
            }
        }
    }

    impl<'py> FromPyObject<'_, 'py> for DarkBytes {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            if let Ok(b) = obj.extract::<Vec<u8>>()
                && let Some(ret) = Self::try_from_vec(b)
            {
                return Ok(ret);
            } else if let Ok(s) = obj.extract::<String>()
                && let Some(ret) = Self::try_from_string(s)
            {
                return Ok(ret);
            } else if let Ok((character, n)) = obj.extract::<(u8, usize)>() {
                return Ok(Self::Padding { character, n });
            }
            Err(ConfigError::new_err(
                "must be a non-empty string, byte sequence of tuple where first \
                 element is a byte character and the second is a number \
                 representing the number of repeats of this character.",
            ))
        }
    }

    impl<'py> IntoPyObject<'py> for DarkBytes {
        type Target = PyAny;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            match self {
                Self::Padding { character, n } => (character, n).into_bound_py_any(py),
                Self::Bytes(b) => Vec::from(b).into_bound_py_any(py),
                Self::Utf8(b) => b.as_str().into_bound_py_any(py),
            }
        }
    }
}

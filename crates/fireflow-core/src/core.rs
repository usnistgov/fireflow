use crate::config::{
    ReadLayoutConfig, ReadState, ReadTEXTOffsetsConfig, ReaderConfig, SharedConfig,
    StdTextReadConfig, TemporalOpticalKey, WriteConfig,
};
use crate::data::{
    AnyLossError, AnyRangeError, ColumnError, ConvertWidthError, DataLayout2_0, DataLayout3_0,
    DataLayout3_1, DataLayout3_2, InterLayoutOps as _, KnownTot, LayoutOps as _, LookupLayoutError,
    LookupLayoutWarning, MaybeTot, MeasLayoutMismatchError, MixedToNonMixedLayoutError,
    MixedToOrderedLayoutError, NewDataLayoutError, NewDataReaderError, RawToLayoutError,
    RawToLayoutWarning, ReadDataframeError, ReadDataframeWarning, TotDefinition,
    VersionedDataLayout,
};
use crate::error::{
    BiTentative, DeferredExt as _, DeferredFailure, DeferredResult, ErrorIter as _,
    IODeferredExt as _, IODeferredResult, IOResult, IOTerminalResult, ImpureError,
    InfalliblePassthruExt as _, MultiResult, MultiResultExt as _, PassthruExt as _, PassthruResult,
    ResultExt as _, Tentative, Terminal, TerminalResult,
};
use crate::header::{
    HeaderKeywordsToWrite, Version, Version2_0, Version3_0, Version3_1, Version3_2,
};
use crate::macros::{def_failure, match_many_to_one};
use crate::nonempty::FCSNonEmpty;
use crate::segment::{
    AnalysisSegmentId, AnyAnalysisSegment, AnyDataSegment, DataSegmentId, HeaderAnalysisSegment,
    HeaderDataSegment, KeyedOptSegment, KeyedReqSegment, NewSegmentConfig,
    OptSegmentWithDefaultWarning, OtherSegment20, ReqSegmentWithDefaultError,
    ReqSegmentWithDefaultWarning, SegmentMismatchWarning,
};

use crate::text::{
    byteord::OrderedToEndianError,
    compensation::{Compensation, Compensation2_0, Compensation3_0},
    datetimes::{BeginDateTime, Datetimes, EndDateTime, ReversedDatetimesError},
    gating::{self, AppliedGates2_0, AppliedGates3_0, AppliedGates3_2},
    index::{IndexFromOne, MeasIndex},
    keywords::{
        Abrt, Analyte, Beginstext, CSMode, CSTot, CSVBits, CSVFlag, Calibration3_1, Calibration3_2,
        Carrierid, Carriertype, Cells, Com, Cyt, Cyt3_2, Cytsn, DetectorName, DetectorType,
        DetectorVoltage, Display, Endstext, Exp, Feature, Fil, Filter, Flowrate, Gain, Inst,
        IntRangeError, LastModified, LastModifier, Locationid, Longname, Lost, Mode, Mode3_2,
        ModeUpgradeError, Nextdata, NoCytError, Op, OpticalType, Originality, Par, PeakBin,
        PeakNumber, PercentEmitted, Plateid, Platename, Power, Proj, Range, Smno, Src, Sys, Tag,
        TemporalScale, TemporalType, Timestep, TimestepLossError, Tot, Trigger, Unicode,
        UnstainedInfo, Vol, Wavelength, Wavelengths, WavelengthsLossError, Wellid,
    },
    named_vec::{
        EitherPair, Eithers, Element, ElementIndexError, IndexedElement, IndexedElementError,
        InsertCenterError, InsertError, KeyLengthError, KeyNotFoundError, NameMapping, NamedVec,
        NewNamedVecError, NonCenterElement, NonUniqueKeyError, RenameError, SetCenterError,
        SetElementsError, SetKeysError, SetNamesError,
    },
    optional::{AlwaysFamily, AlwaysValue, MaybeFamily, MaybeValue, MightHave},
    parser::{
        lookup_temporal_gain_3_0, lookup_temporal_scale_3_0, DepValueWarning, DeprecatedError,
        ExtraStdKeywords, LookupKeysError, LookupKeysWarning, LookupOptional, LookupResult,
        LookupTentative, MissingTime, OptIndexedKey as _, OptKeyError, OptLinkedKey as _,
        OptMetarootKey as _, PseudostandardError, RawKeywords, ReqIndexedKey as _, ReqKeyError,
        ReqMetarootKey as _, UnusedStandardError,
    },
    ranged_float::PositiveFloat,
    scale::{LogScale, Scale},
    spillover::{NewSpilloverError, Spillover},
    timestamps::{
        Btim, Etim, FCSDate, FCSTime, FCSTime100, FCSTime60, ReversedTimestampsError, Timestamps,
        Xtim,
    },
    unstainedcenters::UnstainedCenters,
};

use crate::validated::{
    ascii_uint::{HeaderString, Uint8DigitOverflow, UintSpacePad20, UintSpacePad8},
    dataframe as df,
    dataframe::{AnyFCSColumn, FCSDataFrame},
    keys::{
        IndexedKey, Key, MeasHeader, NonStdKey, NonStdKeywords, NonStdKeywordsExt as _,
        NonStdMeasRegexError, StdKeywords, ValidKeywords,
    },
    shortname::Shortname,
    textdelim::TEXTDelim,
};

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveTime, Timelike as _};
use derive_more::{AsMut, AsRef, Display, From};
use derive_new::new;
use itertools::Itertools as _;
use nalgebra::DMatrix;
use nonempty::NonEmpty;
use num_traits::identities::{One as _, Zero};
use regex::Regex;
use thiserror::Error;

use std::collections::{HashMap, HashSet};
use std::convert::{AsRef, Infallible};
use std::fmt;
use std::fs::File;
use std::io;
use std::io::{BufReader, BufWriter, Read, Seek, Write};
use std::iter::once;
use std::marker::PhantomData;
use std::num::ParseIntError;
use std::path::PathBuf;

#[cfg(feature = "serde")]
use {crate::data::req_meas_headers, serde::Serialize, std::string::ToString as _};

#[cfg(feature = "python")]
use pyo3::prelude::*;

/// Represents the minimal data required to write an FCS file.
///
/// At minimum, this contains the TEXT keywords in a version-specific structure
/// with a few exceptions (see next). It may also contain the DATA and ANALYSIS
/// segments depending on how much of the FCS file is read. These fields are
/// left generic to allow this flexibility.
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
pub struct Core<A, D, O, M, T, P, N, W, L> {
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
    measurements: NamedVec<N, W, Temporal<T>, Optical<P>>,

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
#[cfg_attr(feature = "python", derive(IntoPyObject))]
pub struct Analysis(pub Vec<u8>);

/// An OTHER segment, which is just a string of bytes
#[derive(Clone, From, PartialEq)]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
pub struct Other(pub Vec<u8>);

/// All OTHER segments
#[derive(Clone, Default, From, PartialEq)]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
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
    pub abrt: MaybeValue<Abrt>,

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
    pub lost: MaybeValue<Lost>,

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
    pub tr: MaybeValue<Trigger>,

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
    #[as_ref(Option<Filter>)]
    #[as_mut(Option<Filter>)]
    #[new(into)]
    pub filter: MaybeValue<Filter>,

    /// Value for $PnO
    #[as_ref(Option<Power>)]
    #[as_mut(Option<Power>)]
    #[new(into)]
    pub power: MaybeValue<Power>,

    /// Value for $PnD
    #[as_ref(Option<DetectorType>)]
    #[as_mut(Option<DetectorType>)]
    #[new(into)]
    pub detector_type: MaybeValue<DetectorType>,

    /// Value for $PnP
    #[as_ref(Option<PercentEmitted>)]
    #[as_mut(Option<PercentEmitted>)]
    #[new(into)]
    pub percent_emitted: MaybeValue<PercentEmitted>,

    /// Value for $PnV
    #[as_ref(Option<DetectorVoltage>)]
    #[as_mut(Option<DetectorVoltage>)]
    #[new(into)]
    pub detector_voltage: MaybeValue<DetectorVoltage>,

    /// Version specific data
    pub specific: X,
}

/// Minimal TEXT data for any supported FCS version
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
    pub(crate) fn parse_raw<C>(
        version: Version,
        kws: ValidKeywords,
        data: HeaderDataSegment,
        analysis: HeaderAnalysisSegment,
        st: &ReadState<C>,
    ) -> DeferredResult<
        (Self, ExtraStdKeywords, TEXTOffsets<Option<Tot>>),
        StdTEXTFromRawWarning,
        StdTEXTFromRawError,
    >
    where
        C: AsRef<StdTextReadConfig> + AsRef<ReadLayoutConfig> + AsRef<ReadTEXTOffsetsConfig>,
    {
        match version {
            Version::FCS2_0 => CoreTEXT2_0::new_from_keywords_with_offsets(kws, data, analysis, st)
                .def_map_value(|(x, y, z)| (x.into(), y, z.into_common())),
            Version::FCS3_0 => CoreTEXT3_0::new_from_keywords_with_offsets(kws, data, analysis, st)
                .def_map_value(|(x, y, z)| (x.into(), y, z.into_common())),
            Version::FCS3_1 => CoreTEXT3_1::new_from_keywords_with_offsets(kws, data, analysis, st)
                .def_map_value(|(x, y, z)| (x.into(), y, z.into_common())),
            Version::FCS3_2 => CoreTEXT3_2::new_from_keywords_with_offsets(kws, data, analysis, st)
                .def_map_value(|(x, y, z)| (x.into(), y, z.into_common())),
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
        other_segs: &[OtherSegment20],
        conf: &ReadState<C>,
    ) -> IODeferredResult<
        (Self, StdDatasetWithKwsOutput),
        StdDatasetFromRawWarning,
        StdDatasetFromRawError,
    >
    where
        R: Read + Seek,
        C: AsRef<StdTextReadConfig>
            + AsRef<ReadLayoutConfig>
            + AsRef<ReaderConfig>
            + AsRef<ReadTEXTOffsetsConfig>,
    {
        match version {
            Version::FCS2_0 => CoreDataset2_0::new_from_keywords_inner(
                h,
                kws,
                data_seg,
                analysis_seg,
                other_segs,
                conf,
            )
            .def_map_value(|(x, y)| (x.into(), y)),
            Version::FCS3_0 => CoreDataset3_0::new_from_keywords_inner(
                h,
                kws,
                data_seg,
                analysis_seg,
                other_segs,
                conf,
            )
            .def_map_value(|(x, y)| (x.into(), y)),
            Version::FCS3_1 => CoreDataset3_1::new_from_keywords_inner(
                h,
                kws,
                data_seg,
                analysis_seg,
                other_segs,
                conf,
            )
            .def_map_value(|(x, y)| (x.into(), y)),
            Version::FCS3_2 => CoreDataset3_2::new_from_keywords_inner(
                h,
                kws,
                data_seg,
                analysis_seg,
                other_segs,
                conf,
            )
            .def_map_value(|(x, y)| (x.into(), y)),
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
    pub comp: MaybeValue<Compensation2_0>,

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
    pub comp: MaybeValue<Compensation3_0>,

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
    pub unicode: MaybeValue<Unicode>,

    /// Aggregated values for $CS* keywords
    #[as_ref(Option<CSVBits>)]
    #[as_mut(Option<CSVBits>)]
    #[as_ref(Option<CSTot>)]
    #[as_mut(Option<CSTot>)]
    #[as_ref(Option<CSVFlags>)]
    #[as_mut(Option<CSVFlags>)]
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
    pub spillover: MaybeValue<Spillover>,

    /// Values of $LAST_MODIFIED/$LAST_MODIFIER/$ORIGINALITY
    #[as_ref(LastModifier, Option<LastModified>, Option<Originality>)]
    #[as_mut(LastModifier, Option<LastModified>, Option<Originality>)]
    pub modification: ModificationData,

    /// Values of $PLATEID/$PLATENAME/$WELLID
    #[as_ref(Option<Plateid>, Option<Wellid>, Option<Platename>)]
    #[as_mut(Option<Plateid>, Option<Wellid>, Option<Platename>)]
    pub plate: PlateData,

    /// Value of $VOL
    #[as_ref(Option<Vol>)]
    #[as_mut(Option<Vol>)]
    #[new(into)]
    pub vol: MaybeValue<Vol>,

    /// Aggregated values for $CS* keywords
    #[as_ref(Option<CSVBits>)]
    #[as_mut(Option<CSVBits>)]
    #[as_ref(Option<CSTot>)]
    #[as_mut(Option<CSTot>)]
    #[as_ref(Option<CSVFlags>)]
    #[as_mut(Option<CSVFlags>)]
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
    pub mode: MaybeValue<Mode3_2>,

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
    pub spillover: MaybeValue<Spillover>,

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
    #[as_ref(Option<Plateid>, Option<Wellid>, Option<Platename>)]
    #[as_mut(Option<Plateid>, Option<Wellid>, Option<Platename>)]
    pub plate: PlateData,

    /// Value of $VOL
    #[as_ref(Option<Vol>)]
    #[as_mut(Option<Vol>)]
    #[new(into)]
    pub vol: MaybeValue<Vol>,

    /// Values of $CARRIERID/$CARRIERTYPE/$LOCATIONID
    #[as_ref(Option<Carrierid>, Option<Carriertype>, Option<Locationid>)]
    #[as_mut(Option<Carrierid>, Option<Carriertype>, Option<Locationid>)]
    pub carrier: CarrierData,

    /// Values of $UNSTAINEDINFO/$UNSTAINEDCENTERS
    #[as_ref(Option<UnstainedCenters>, Option<UnstainedInfo>)]
    #[as_mut(Option<UnstainedInfo>)]
    pub unstained: UnstainedData,

    /// Value of $FLOWRATE
    #[as_ref(Option<Flowrate>)]
    #[as_mut(Option<Flowrate>)]
    #[new(into)]
    pub flowrate: MaybeValue<Flowrate>,

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
    #[new(into)]
    pub scale: MaybeValue<TemporalScale>,

    /// Values of $Pkn/$PKNn
    #[as_ref(Option<PeakBin>)]
    #[as_ref(Option<PeakNumber>)]
    #[as_mut(Option<PeakBin>)]
    #[as_mut(Option<PeakNumber>)]
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
    #[as_ref(Option<PeakNumber>)]
    #[as_mut(Option<PeakBin>)]
    #[as_mut(Option<PeakNumber>)]
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

    /// Value for $PnDISPLAY
    #[as_ref(Option<Display>)]
    #[as_mut(Option<Display>)]
    #[new(into)]
    pub display: MaybeValue<Display>,

    /// Values of $Pkn/$PKNn
    #[as_ref(Option<PeakBin>)]
    #[as_ref(Option<PeakNumber>)]
    #[as_mut(Option<PeakBin>)]
    #[as_mut(Option<PeakNumber>)]
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

    /// Value for $PnDISPLAY
    #[as_ref(Option<Display>)]
    #[as_mut(Option<Display>)]
    #[new(into)]
    pub display: MaybeValue<Display>,

    /// Value for $PnTYPE
    #[as_ref(Option<TemporalType>)]
    #[as_mut(Option<TemporalType>)]
    #[new(into)]
    pub measurement_type: MaybeValue<TemporalType>,
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
    pub scale: MaybeValue<Scale>,

    /// Value for $PnL
    #[as_ref(Option<Wavelength>)]
    #[as_mut(Option<Wavelength>)]
    #[new(into)]
    pub wavelength: MaybeValue<Wavelength>,

    /// Values of $Pkn/$PKNn
    #[as_ref(Option<PeakBin>)]
    #[as_ref(Option<PeakNumber>)]
    #[as_mut(Option<PeakBin>)]
    #[as_mut(Option<PeakNumber>)]
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
    pub wavelength: MaybeValue<Wavelength>,

    /// Values of $Pkn/$PKNn
    #[as_ref(Option<PeakBin>)]
    #[as_ref(Option<PeakNumber>)]
    #[as_mut(Option<PeakBin>)]
    #[as_mut(Option<PeakNumber>)]
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
    #[as_ref(Option<Wavelengths>)]
    #[as_mut(Option<Wavelengths>)]
    #[new(into)]
    pub wavelengths: MaybeValue<Wavelengths>,

    /// Value for $PnCALIBRATION
    #[as_ref(Option<Calibration3_1>)]
    #[as_mut(Option<Calibration3_1>)]
    #[new(into)]
    pub calibration: MaybeValue<Calibration3_1>,

    /// Value for $PnDISPLAY
    #[as_ref(Option<Display>)]
    #[as_mut(Option<Display>)]
    #[new(into)]
    pub display: MaybeValue<Display>,

    /// Values of $Pkn/$PKNn
    #[as_ref(Option<PeakBin>)]
    #[as_ref(Option<PeakNumber>)]
    #[as_mut(Option<PeakBin>)]
    #[as_mut(Option<PeakNumber>)]
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
    #[as_ref(Option<Wavelengths>)]
    #[as_mut(Option<Wavelengths>)]
    #[new(into)]
    pub wavelengths: MaybeValue<Wavelengths>,

    /// Value for $PnCALIBRATION
    #[as_ref(Option<Calibration3_2>)]
    #[as_mut(Option<Calibration3_2>)]
    #[new(into)]
    pub calibration: MaybeValue<Calibration3_2>,

    /// Value for $PnDISPLAY
    #[as_ref(Option<Display>)]
    #[as_mut(Option<Display>)]
    #[new(into)]
    pub display: MaybeValue<Display>,

    /// Value for $PnANALYTE
    #[as_ref(Analyte)]
    #[as_mut(Analyte)]
    #[new(into)]
    pub analyte: Analyte,

    /// Value for $PnFEATURE
    #[as_ref(Option<Feature>)]
    #[as_mut(Option<Feature>)]
    #[new(into)]
    pub feature: MaybeValue<Feature>,

    /// Value for $PnTYPE
    #[as_ref(Option<OpticalType>)]
    #[as_mut(Option<OpticalType>)]
    #[new(into)]
    pub measurement_type: MaybeValue<OpticalType>,

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
    pub bin: MaybeValue<PeakBin>,

    /// Value of $PkNn
    #[as_ref(Option<PeakNumber>)]
    #[as_mut(Option<PeakNumber>)]
    #[new(into)]
    pub size: MaybeValue<PeakNumber>,
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
    #[as_ref(Option<CSVBits>)]
    #[as_mut(Option<CSVBits>)]
    #[new(into)]
    pub bits: MaybeValue<CSVBits>,

    /// Value of $CSTOT if given
    #[as_ref(Option<CSTot>)]
    #[as_mut(Option<CSTot>)]
    #[new(into)]
    pub tot: MaybeValue<CSTot>,

    #[as_ref(Option<CSVFlags>)]
    #[as_mut(Option<CSVFlags>)]
    #[new(into)]
    pub flags: MaybeValue<CSVFlags>,
}

/// Values of $CSVnFLAG if given, with length equal to $CSMODE
#[derive(Clone, PartialEq, From)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
pub struct CSVFlags(pub FCSNonEmpty<MaybeValue<CSVFlag>>);

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
    pub last_modified: MaybeValue<LastModified>,

    #[as_ref(Option<Originality>)]
    #[as_mut(Option<Originality>)]
    #[new(into)]
    pub originality: MaybeValue<Originality>,
}

/// A bundle for $PLATEID, $PLATENAME, and $WELLID (3.1+)
#[derive(Clone, Default, AsRef, AsMut, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct PlateData {
    #[as_ref(Option<Plateid>)]
    #[as_mut(Option<Plateid>)]
    #[new(into)]
    pub plateid: MaybeValue<Plateid>,

    #[as_ref(Option<Platename>)]
    #[as_mut(Option<Platename>)]
    #[new(into)]
    pub platename: MaybeValue<Platename>,

    #[as_ref(Option<Wellid>)]
    #[as_mut(Option<Wellid>)]
    #[new(into)]
    pub wellid: MaybeValue<Wellid>,
}

/// A bundle for $UNSTAINEDCENTERS and $UNSTAINEDINFO (3.2+)
#[derive(Clone, Default, AsRef, AsMut, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct UnstainedData {
    #[as_ref(Option<UnstainedCenters>)]
    #[new(into)]
    // NOTE not mutable to prevent mutation when part of Core
    pub unstainedcenters: MaybeValue<UnstainedCenters>,

    #[as_ref(Option<UnstainedInfo>)]
    #[as_mut(Option<UnstainedInfo>)]
    #[new(into)]
    pub unstainedinfo: MaybeValue<UnstainedInfo>,
}

/// A bundle for $CARRIERID, $CARRIERTYPE, $LOCATIONID (3.2+)
#[derive(Clone, Default, AsRef, AsMut, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct CarrierData {
    #[as_ref(Option<Carrierid>)]
    #[as_mut(Option<Carrierid>)]
    #[new(into)]
    pub carrierid: MaybeValue<Carrierid>,

    #[as_ref(Option<Carriertype>)]
    #[as_mut(Option<Carriertype>)]
    #[new(into)]
    pub carriertype: MaybeValue<Carriertype>,

    #[as_ref(Option<Locationid>)]
    #[as_mut(Option<Locationid>)]
    #[new(into)]
    pub locationid: MaybeValue<Locationid>,
}

pub type Temporal2_0 = Temporal<InnerTemporal2_0>;
pub type Temporal3_0 = Temporal<InnerTemporal3_0>;
pub type Temporal3_1 = Temporal<InnerTemporal3_1>;
pub type Temporal3_2 = Temporal<InnerTemporal3_2>;

pub type Optical2_0 = Optical<InnerOptical2_0>;
pub type Optical3_0 = Optical<InnerOptical3_0>;
pub type Optical3_1 = Optical<InnerOptical3_1>;
pub type Optical3_2 = Optical<InnerOptical3_2>;

pub type Measurements2_0 = Measurements<MaybeFamily, InnerTemporal2_0, InnerOptical2_0>;
pub type Measurements3_0 = Measurements<MaybeFamily, InnerTemporal3_0, InnerOptical3_0>;
pub type Measurements3_1 = Measurements<AlwaysFamily, InnerTemporal3_1, InnerOptical3_1>;
pub type Measurements3_2 = Measurements<AlwaysFamily, InnerTemporal3_2, InnerOptical3_2>;

pub type Metaroot2_0 = Metaroot<InnerMetaroot2_0>;
pub type Metaroot3_0 = Metaroot<InnerMetaroot3_0>;
pub type Metaroot3_1 = Metaroot<InnerMetaroot3_1>;
pub type Metaroot3_2 = Metaroot<InnerMetaroot3_2>;

/// A minimal representation of the TEXT segment
pub type CoreTEXT<M, T, P, N, W, L> = Core<(), (), (), M, T, P, N, W, L>;

/// A minimal representation of the TEXT+DATA+ANALYSIS segments
pub type CoreDataset<M, T, P, N, W, L> = Core<Analysis, FCSDataFrame, Others, M, T, P, N, W, L>;

pub type Core2_0<A, D, O> = Core<
    A,
    D,
    O,
    InnerMetaroot2_0,
    InnerTemporal2_0,
    InnerOptical2_0,
    MaybeFamily,
    MaybeValue<Shortname>,
    DataLayout2_0,
>;
pub type Core3_0<A, D, O> = Core<
    A,
    D,
    O,
    InnerMetaroot3_0,
    InnerTemporal3_0,
    InnerOptical3_0,
    MaybeFamily,
    MaybeValue<Shortname>,
    DataLayout3_0,
>;
pub type Core3_1<A, D, O> = Core<
    A,
    D,
    O,
    InnerMetaroot3_1,
    InnerTemporal3_1,
    InnerOptical3_1,
    AlwaysFamily,
    AlwaysValue<Shortname>,
    DataLayout3_1,
>;
pub type Core3_2<A, D, O> = Core<
    A,
    D,
    O,
    InnerMetaroot3_2,
    InnerTemporal3_2,
    InnerOptical3_2,
    AlwaysFamily,
    AlwaysValue<Shortname>,
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
#[cfg_attr(feature = "python", derive(IntoPyObject))]
pub struct StdDatasetWithKwsOutput {
    /// DATA+ANALYSIS
    pub dataset_segments: DatasetSegments,

    /// Keywords that start with '$' that are not part of the standard
    pub extra: ExtraStdKeywords,
}

/// Standardized TEXT+DATA+ANALYSIS with DATA+ANALYSIS offsets
#[derive(Clone, Copy, new, PartialEq)]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
pub struct DatasetSegments {
    /// offsets used to parse DATA
    pub data: AnyDataSegment,

    /// offsets used to parse ANALYSIS
    pub analysis: AnyAnalysisSegment,
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
    fn unstainedcenters_mut(&mut self, _: private::NoTouchy) -> &mut Option<UnstainedCenters>;
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
    type Offsets: VersionedTEXTOffsets<TotDef = <Self::Layout as VersionedDataLayout>::TotDef>;

    fn fcs_version() -> Self;

    fn h_lookup_and_read<C, R>(
        h: &mut BufReader<R>,
        kws: &StdKeywords,
        data: HeaderDataSegment,
        analysis: HeaderAnalysisSegment,
        st: &ReadState<C>,
    ) -> IODeferredResult<
        (FCSDataFrame, Analysis, DatasetSegments),
        LookupAndReadDataAnalysisWarning,
        LookupAndReadDataAnalysisError,
    >
    where
        R: Read + Seek,
        Self::Offsets: AsRef<DatasetSegments>,
        C: AsRef<ReadLayoutConfig> + AsRef<ReaderConfig> + AsRef<ReadTEXTOffsetsConfig>,
    {
        let layout_res = Self::Layout::lookup_ro(kws, st.conf.as_ref()).def_inner_into();
        let offset_res = Self::Offsets::lookup_ro(kws, data, analysis, st).def_inner_into();
        layout_res
            .def_zip(offset_res)
            .def_errors_liftio()
            .def_and_maybe(|(layout, offsets)| {
                let dataset_segs = offsets.as_ref();
                let ar = AnalysisReader::new(dataset_segs.analysis);
                let read_conf: &ReaderConfig = st.conf.as_ref();
                let data_res = layout
                    .h_read_df(h, offsets.tot(), dataset_segs.data, read_conf)
                    .def_io_into();
                let analysis_res = ar.h_read(h).into_deferred();
                data_res
                    .def_zip(analysis_res)
                    .def_map_value(|(d, a)| (d, a, *dataset_segs))
            })
    }
}

pub trait LookupMetaroot: Sized + VersionedMetaroot {
    fn lookup_shortname(
        kws: &mut StdKeywords,
        i: MeasIndex,
        conf: &StdTextReadConfig,
    ) -> LookupResult<<Self::Name as MightHave>::Wrapper<Shortname>>;

    fn lookup_specific(
        kws: &mut StdKeywords,
        par: Par,
        names: &HashSet<&Shortname>,
        ordered_names: &[&Shortname],
        conf: &StdTextReadConfig,
    ) -> LookupResult<Self>;
}

pub trait ConvertFromMetaroot<M>: Sized
where
    Self: VersionedMetaroot,
    M: VersionedMetaroot,
{
    fn convert_from_metaroot(value: M, allow_loss: bool) -> MetarootConvertResult<Self>;
}

pub trait ConvertFromOptical<O>: Sized
where
    Self: VersionedOptical,
{
    fn convert_from_optical(value: O, i: MeasIndex, allow_loss: bool)
        -> OpticalConvertResult<Self>;
}

pub trait ConvertFromTemporal<T>: Sized
where
    Self: VersionedTemporal,
{
    fn convert_from_temporal(
        value: T,
        i: MeasIndex,
        allow_loss: bool,
    ) -> TemporalConvertTentative<Self>;
}

pub trait ConvertFromLayout<T>: Sized
where
    Self: VersionedDataLayout,
{
    fn convert_from_layout(value: T) -> LayoutConvertResult<Self>;
}

pub trait VersionedMetaroot: Sized {
    type Ver: Versioned;
    type Optical: VersionedOptical<Ver = Self::Ver>;
    type Temporal: VersionedTemporal<Ver = Self::Ver>;
    type Name: MightHave;

    /// Return error if any data in this struct links to given list of names.
    fn check_meas_named_links_inner(
        &self,
        names: &HashSet<&Shortname>,
    ) -> Result<(), ExistingNamedLinkError>;

    /// Return error if any data in struct has index links.
    fn check_meas_index_links_inner(
        &self,
        indices: &HashSet<MeasIndex>,
    ) -> Result<(), ExistingIndexLinkError>;

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
        t: Temporal<Self::Temporal>,
        o: Optical<Self::Optical>,
        i: MeasIndex,
        allow_loss: bool,
    ) -> PassthruResult<
        (Optical<Self::Optical>, Temporal<Self::Temporal>),
        Box<(Temporal<Self::Temporal>, Optical<Self::Optical>)>,
        SwapOpticalTemporalError,
        SwapOpticalTemporalError,
    > {
        let go = |old_t: Temporal<Self::Temporal>, old_o: Optical<Self::Optical>| {
            let (so, st) = Self::swap_optical_temporal_inner(old_t.specific, old_o.specific);
            let new_o = Optical::new(old_t.common, None, None, None, None, None, so);
            let new_t = Temporal::new(old_o.common, st);
            (new_o, new_t)
        };
        let t_res = o.specific.can_convert_to_temporal(i).mult_errors_into();
        let o_specific_res = t.specific.can_convert_to_optical_swap(i);
        let o_common_res = o
            .check_keys_transfer(i)
            .mult_errors_into::<OpticalToTemporalError>()
            .mult_errors_into();
        match t_res.mult_zip3(o_specific_res, o_common_res) {
            Ok(_) => Ok(Tentative::new1(go(t, o))),
            Err(es) => {
                if allow_loss {
                    Ok(Tentative::new(go(t, o), es, []))
                } else {
                    Err(DeferredFailure::new([], es, Box::new((t, o))))
                }
            }
        }
    }

    fn swap_optical_temporal_inner(
        t: Self::Temporal,
        o: Self::Optical,
    ) -> (Self::Optical, Self::Temporal);
}

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

    fn can_convert_to_temporal(&self, i: MeasIndex) -> MultiResult<(), OpticalToTemporalError>;
}

pub trait LookupOptical: Sized + VersionedOptical {
    fn lookup_specific(
        kws: &mut StdKeywords,
        i: MeasIndex,
        conf: &StdTextReadConfig,
    ) -> LookupResult<Self>;
}

pub trait VersionedTemporal: Sized {
    type Ver: Versioned;
    type Err;

    fn req_meta_keywords_inner(&self) -> impl Iterator<Item = (String, String)>;

    fn req_meas_keywords_inner(&self, i: MeasIndex) -> impl Iterator<Item = (String, String)>;

    fn opt_meas_keywords_inner(&self, i: MeasIndex) -> impl Iterator<Item = (String, String)>;

    fn can_convert_to_optical(&self, i: MeasIndex) -> MultiResult<(), Self::Err>;

    fn can_convert_to_optical_swap(
        &self,
        i: MeasIndex,
    ) -> MultiResult<(), SwapOpticalTemporalError>;
}

pub trait LookupTemporal: VersionedTemporal {
    fn lookup_specific(
        std: &mut StdKeywords,
        i: MeasIndex,
        nonstd: &mut NonStdKeywords,
        conf: &StdTextReadConfig,
    ) -> LookupResult<Self>;
}

pub trait TemporalFromOptical<O: VersionedOptical>: Sized {
    type TData;

    fn from_optical(
        o: Optical<O>,
        i: MeasIndex,
        d: Self::TData,
        allow_loss: bool,
    ) -> PassthruResult<
        Temporal<Self>,
        Box<Optical<O>>,
        OpticalToTemporalError,
        OpticalToTemporalError,
    > {
        let opt_common_res = o
            .check_keys_transfer(i)
            .mult_errors_into::<OpticalToTemporalError>();
        let opt_specific_res = o.specific.can_convert_to_temporal(i).mult_errors_into();
        match opt_common_res.mult_zip(opt_specific_res) {
            Ok(_) => Ok(Tentative::new1(Self::from_optical_unchecked(o, d))),
            Err(es) => {
                if allow_loss {
                    Ok(Tentative::new(Self::from_optical_unchecked(o, d), es, []))
                } else {
                    Err(DeferredFailure::new([], es, Box::new(o)))
                }
            }
        }
    }

    fn from_optical_unchecked(o: Optical<O>, d: Self::TData) -> Temporal<Self> {
        Temporal::new(o.common, Self::from_optical_inner(o.specific, d))
    }

    fn from_optical_inner(o: O, d: Self::TData) -> Self;
}

pub trait OpticalFromTemporal<T: VersionedTemporal>: Sized {
    type TData;
    type Loss;

    #[allow(clippy::type_complexity)]
    fn from_temporal(
        t: Temporal<T>,
        i: MeasIndex,
        allow_loss: Self::Loss,
    ) -> PassthruResult<(Optical<Self>, Self::TData), Box<Temporal<T>>, T::Err, T::Err>;

    fn from_temporal_unchecked(t: Temporal<T>) -> (Optical<Self>, Self::TData) {
        let (specific, td) = Self::from_temporal_inner(t.specific);
        let new = Optical::new(t.common, None, None, None, None, None, specific);
        (new, td)
    }

    fn from_temporal_inner(t: T) -> (Self, Self::TData);
}

pub trait VersionedTEXTOffsets: Sized {
    type TotDef: TotDefinition;

    fn lookup<C>(
        kws: &mut StdKeywords,
        data: HeaderDataSegment,
        analysis: HeaderAnalysisSegment,
        st: &ReadState<C>,
    ) -> LookupTEXTOffsetsResult<Self>
    where
        C: AsRef<ReadTEXTOffsetsConfig>;

    fn lookup_ro<C>(
        kws: &StdKeywords,
        data: HeaderDataSegment,
        analysis: HeaderAnalysisSegment,
        st: &ReadState<C>,
    ) -> LookupTEXTOffsetsResult<Self>
    where
        C: AsRef<ReadTEXTOffsetsConfig>;

    fn tot(&self) -> <Self::TotDef as TotDefinition>::Tot;

    fn into_common(self) -> TEXTOffsets<Option<Tot>>;
}

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
    fn lookup(
        std: &mut StdKeywords,
        i: MeasIndex,
        nonstd: NonStdKeywords,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<Self> {
        Longname::lookup_meas_opt(std, i, false, conf).map(|longname| Self::new(longname, nonstd))
    }
}

impl<T> Temporal<T> {
    fn lookup_temporal(
        std: &mut StdKeywords,
        i: MeasIndex,
        mut nonstd: NonStdKeywords,
        conf: &StdTextReadConfig,
    ) -> LookupResult<Self>
    where
        T: LookupTemporal,
    {
        T::lookup_specific(std, i, &mut nonstd, conf).def_and_tentatively(|specific| {
            CommonMeasurement::lookup(std, i, nonstd, conf)
                .map(|common| Self::new(common, specific))
                .errors_into()
        })
    }

    fn convert<ToT>(self, i: MeasIndex, allow_loss: bool) -> TemporalConvertTentative<Temporal<ToT>>
    where
        ToT: ConvertFromTemporal<T>,
    {
        ToT::convert_from_temporal(self.specific, i, allow_loss)
            .map(|specific| Temporal::new(self.common, specific))
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
}

impl<O> Optical<O> {
    fn try_convert<ToP: ConvertFromOptical<O>>(
        self,
        i: MeasIndex,
        allow_loss: bool,
    ) -> OpticalConvertResult<Optical<ToP>> {
        ToP::convert_from_optical(self.specific, i, allow_loss).def_map_value(|specific| {
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

    fn lookup_optical(
        std: &mut StdKeywords,
        i: MeasIndex,
        nonstd: NonStdKeywords,
        conf: &StdTextReadConfig,
    ) -> LookupResult<Self>
    where
        O: LookupOptical,
        Version: From<O::Ver>,
    {
        let version = O::Ver::fcs_version();
        let filter = Filter::lookup_meas_opt(std, i, false, conf);
        let power = Power::lookup_meas_opt(std, i, false, conf);
        let det_type = DetectorType::lookup_meas_opt(std, i, false, conf);
        let pe_dep = Version::from(version) == Version::FCS3_2;
        let perc_emit = PercentEmitted::lookup_meas_opt(std, i, pe_dep, conf);
        let det_volt = DetectorVoltage::lookup_meas_opt(std, i, false, conf);
        filter
            .zip5(power, det_type, perc_emit, det_volt)
            .errors_into()
            .and_maybe(|(f, p, d, e, v)| {
                CommonMeasurement::lookup(std, i, nonstd, conf)
                    .errors_into()
                    .and_maybe(|c| {
                        O::lookup_specific(std, i, conf)
                            .def_map_value(|s| Self::new(c, f, p, d, e, v, s))
                    })
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
            self.filter.meas_kw_triple(i),
            self.power.meas_kw_triple(i),
            self.detector_type.meas_kw_triple(i),
            self.percent_emitted.meas_kw_triple(i),
            self.detector_voltage.meas_kw_triple(i),
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

    fn check_keys_transfer(
        &self,
        i: MeasIndex,
    ) -> MultiResult<(), AnyOpticalToTemporalKeyLossError> {
        let filter = self.filter.check_indexed_key_transfer(i);
        let power = self.power.check_indexed_key_transfer(i);
        let det_type = self.detector_type.check_indexed_key_transfer(i);
        let per_emit = self.percent_emitted.check_indexed_key_transfer(i);
        let det_volt = self.detector_voltage.check_indexed_key_transfer(i);
        filter
            .zip3(power, det_type)
            .mult_zip(per_emit.zip(det_volt))
            .void()
    }
}

impl<M> Metaroot<M>
where
    M: VersionedMetaroot,
{
    fn try_convert<ToM: ConvertFromMetaroot<M>>(
        self,
        allow_loss: bool,
    ) -> MetarootConvertResult<Metaroot<ToM>> {
        ToM::convert_from_metaroot(self.specific, allow_loss).def_map_value(|specific| {
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

    fn lookup_metaroot(
        std: &mut StdKeywords,
        ms: &Measurements<M::Name, M::Temporal, M::Optical>,
        nonstd: NonStdKeywords,
        conf: &StdTextReadConfig,
    ) -> LookupResult<Self>
    where
        M: LookupMetaroot,
    {
        let par = Par(ms.len());
        let names: HashSet<_> = ms.indexed_names().map(|(_, n)| n).collect();
        let ordered_names: Vec<_> = ms.indexed_names().map(|(_, n)| n).collect();
        let abrt = Abrt::lookup_metaroot_opt(std, false, conf);
        let com = Com::lookup_metaroot_opt(std, false, conf);
        let cells = Cells::lookup_metaroot_opt(std, false, conf);
        let exp = Exp::lookup_metaroot_opt(std, false, conf);
        let fil = Fil::lookup_metaroot_opt(std, false, conf);
        let inst = Inst::lookup_metaroot_opt(std, false, conf);
        let lost = Lost::lookup_metaroot_opt(std, false, conf);
        let op = Op::lookup_metaroot_opt(std, false, conf);
        let proj = Proj::lookup_metaroot_opt(std, false, conf);
        let smno = Smno::lookup_metaroot_opt(std, false, conf);
        let src = Src::lookup_metaroot_opt(std, false, conf);
        let sys = Sys::lookup_metaroot_opt(std, false, conf);
        let tr = Trigger::lookup_opt_linked_st(std, &names, (), conf);
        abrt.zip5(com, cells, exp, fil)
            .zip5(inst, lost, op, proj)
            .zip5(smno, src, sys, tr)
            .errors_into()
            .and_maybe(
                |(
                    ((abrt_, com_, cells_, exp_, fil_), inst_, lost_, op_, proj_),
                    smno_,
                    src_,
                    sys_,
                    tr_,
                )| {
                    M::lookup_specific(std, par, &names, &ordered_names, conf).def_map_value(
                        |specific| {
                            Self::new(
                                abrt_, com_, cells_, exp_, fil_, inst_, lost_, op_, proj_, smno_,
                                src_, sys_, tr_, specific, nonstd,
                            )
                        },
                    )
                },
            )
    }

    fn all_req_keywords(&self, par: Par) -> impl Iterator<Item = (String, String)> {
        once(par.pair()).chain(self.specific.keywords_req_inner())
    }

    fn all_opt_keywords(&self) -> impl Iterator<Item = (String, String)> {
        [
            self.abrt.root_kw_pair(),
            self.com.metaroot_opt_pair(),
            self.cells.metaroot_opt_pair(),
            self.exp.metaroot_opt_pair(),
            self.fil.metaroot_opt_pair(),
            self.inst.metaroot_opt_pair(),
            self.lost.root_kw_pair(),
            self.op.metaroot_opt_pair(),
            self.proj.metaroot_opt_pair(),
            self.smno.metaroot_opt_pair(),
            self.src.metaroot_opt_pair(),
            self.sys.metaroot_opt_pair(),
            self.tr.root_kw_pair(),
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
        if let Some(tr) = self.tr.0.as_mut() {
            tr.reassign(mapping);
        }
    }

    fn rename_meas_links(&mut self, mapping: &NameMapping) {
        self.rename_trigger_meas_link(mapping);
        self.specific.rename_meas_links_inner(mapping);
    }

    fn check_meas_named_links(
        &self,
        names: &HashSet<&Shortname>,
    ) -> Result<(), ExistingNamedLinkError> {
        if self
            .tr
            .0
            .as_ref()
            .is_some_and(|tr| names.contains(&tr.measurement))
        {
            return Err(ExistingNamedLinkError::Trigger);
        }
        self.specific.check_meas_named_links_inner(names)
    }

    fn check_meas_links(
        &self,
        indexed_names: &[(MeasIndex, &Shortname)],
    ) -> Result<(), ExistingLinkError> {
        let ns = indexed_names.iter().map(|(_, n)| *n).collect();
        let js = indexed_names.iter().map(|(i, _)| i).copied().collect();
        self.check_meas_named_links(&ns)?;
        self.specific.check_meas_index_links_inner(&js)?;
        Ok(())
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
            None,
            None,
            None,
            None,
            None,
            value.specific.into(),
        )
    }
}

pub(crate) type Measurements<N, T, O> =
    NamedVec<N, <N as MightHave>::Wrapper<Shortname>, Temporal<T>, Optical<O>>;

pub(crate) type VersionedCore<A, D, O, M> = Core<
    A,
    D,
    O,
    M,
    <M as VersionedMetaroot>::Temporal,
    <M as VersionedMetaroot>::Optical,
    <M as VersionedMetaroot>::Name,
    <<M as VersionedMetaroot>::Name as MightHave>::Wrapper<Shortname>,
    <<M as VersionedMetaroot>::Ver as Versioned>::Layout,
>;

pub(crate) type VersionedCoreTEXT<M> = VersionedCore<(), (), (), M>;

pub(crate) type VersionedCoreDataset<M> = VersionedCore<Analysis, FCSDataFrame, Others, M>;

pub(crate) type VersionedConvertError<N, ToN> = ConvertError<
    <<ToN as MightHave>::Wrapper<Shortname> as TryFrom<
        <N as MightHave>::Wrapper<Shortname>,
    >>::Error,
>;

impl<M, A, D, O> VersionedCore<A, D, O, M>
where
    M: VersionedMetaroot,
    M::Name: Clone,
{
    /// Show FCS version.
    pub fn fcs_version(&self) -> Version
    where
        Version: From<M::Ver>,
    {
        M::Ver::fcs_version().into()
    }

    /// Write this core structure (HEADER+TEXT) to a handle
    pub fn h_write_text<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        delim: TEXTDelim,
        big_other: bool,
    ) -> IOTerminalResult<(), Infallible, Uint8DigitOverflow, WriteTEXTFailure>
    where
        Version: From<M::Ver>,
    {
        if big_other {
            self.h_write_text_inner1::<_, UintSpacePad20>(h, delim)
        } else {
            self.h_write_text_inner1::<_, UintSpacePad8>(h, delim)
        }
    }

    fn h_write_text_inner1<W: Write, T>(
        &self,
        h: &mut BufWriter<W>,
        delim: TEXTDelim,
    ) -> IOTerminalResult<(), Infallible, Uint8DigitOverflow, WriteTEXTFailure>
    where
        Version: From<M::Ver>,
        T: Zero + TryFrom<u64, Error = Uint8DigitOverflow> + HeaderString,
    {
        self.h_write_text_inner::<_, T>(h, delim, Tot(0), 0, 0, &[])
            .terminate(WriteTEXTFailure)
    }

    fn h_write_text_inner<W: Write, T>(
        &self,
        h: &mut BufWriter<W>,
        delim: TEXTDelim,
        tot: Tot,
        data_len: u64,
        analysis_len: u64,
        other_segs: &[Other],
    ) -> IOResult<(), Uint8DigitOverflow>
    where
        Version: From<M::Ver>,
        T: Zero + TryFrom<u64, Error = Uint8DigitOverflow> + HeaderString,
    {
        // TODO do something useful with $NEXTDATA
        let other_lens: Vec<_> = other_segs
            .iter()
            .map(|s| u64::try_from(s.0.len()).expect("OTHER segment length exceeds 2^64"))
            .collect();
        let hdr_kws: HeaderKeywordsToWrite<T> = self
            .header_and_raw_keywords(tot, data_len, analysis_len, &other_lens[..], false)
            .map_err(ImpureError::Pure)?;
        Ok(hdr_kws.h_write(h, M::Ver::fcs_version().into(), delim, other_segs)?)
    }

    /// Return all keywords as an ordered list of pairs
    ///
    /// Thiw will only include keywords that can be directly derived from
    /// [`CoreTEXT`]. This means it will not include $TOT, since this depends on
    /// the DATA segment.
    // TODO fix clippy issue here (it has a good point)
    #[allow(clippy::fn_params_excessive_bools)]
    pub fn standard_keywords(
        &self,
        exclude_req_root: bool,
        exclude_opt_root: bool,
        exclude_req_meas: bool,
        exclude_opt_meas: bool,
    ) -> RawKeywords {
        fn go(
            xs: impl Iterator<Item = (String, String)>,
            exclude: bool,
        ) -> impl Iterator<Item = (String, String)> {
            (!exclude).then_some(xs).into_iter().flatten()
        }

        go(self.req_root_keywords(), exclude_req_root)
            .chain(go(self.opt_root_keywords(), exclude_opt_root))
            .chain(go(self.req_meas_keywords(), exclude_req_meas))
            .chain(go(self.opt_meas_keywords(), exclude_opt_meas))
            .collect()
    }

    /// Set the $TR keyword.
    ///
    /// Return error if supplied name is not a measurement name (a $PnN).
    pub fn set_trigger(&mut self, tr: Option<Trigger>) -> Result<(), TriggerLinkError> {
        let ns = self.measurement_names();
        if tr.as_ref().is_some_and(|t| !ns.contains(&t.measurement)) {
            return Err(TriggerLinkError);
        }
        self.metaroot.tr = tr.into();
        Ok(())
    }

    /// Set threshold for $TR keyword
    ///
    /// Return true if trigger exists, false otherwise.
    pub fn set_trigger_threshold(&mut self, x: u32) -> bool {
        if let Some(tr) = self.metaroot.tr.0.as_mut() {
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
        allow_loss: bool,
    ) -> TerminalResult<bool, SetTemporalError, SetTemporalError, SetTemporalFailure>
    where
        M::Temporal: TemporalFromOptical<M::Optical>,
    {
        self.measurements
            .set_center_by_name(
                n,
                |i, old_o, old_t| {
                    M::swap_optical_temporal(old_o, old_t, i, allow_loss).def_inner_into()
                },
                |i, old_o| {
                    <M::Temporal as TemporalFromOptical<M::Optical>>::from_optical(
                        old_o, i, timestep, allow_loss,
                    )
                    .def_inner_into::<SwapOpticalTemporalError, SwapOpticalTemporalError>()
                    .def_inner_into()
                },
            )
            .def_terminate(SetTemporalFailure)
    }

    /// Set the measurement at given index to the time measurement.
    pub fn set_temporal_at(
        &mut self,
        index: MeasIndex,
        timestep: <M::Temporal as TemporalFromOptical<M::Optical>>::TData,
        allow_loss: bool,
    ) -> TerminalResult<bool, SetTemporalError, SetTemporalError, SetTemporalFailure>
    where
        M::Temporal: TemporalFromOptical<M::Optical>,
    {
        self.measurements
            .set_center_by_index(
                index,
                |i, old_o, old_t| {
                    M::swap_optical_temporal(old_o, old_t, i, allow_loss).def_inner_into()
                },
                |i, old_o| {
                    <M::Temporal as TemporalFromOptical<M::Optical>>::from_optical(
                        old_o, i, timestep, allow_loss,
                    )
                    .def_inner_into::<SwapOpticalTemporalError, SwapOpticalTemporalError>()
                    .def_inner_into()
                },
            )
            .def_terminate(SetTemporalFailure)
    }

    /// Convert time measurement to optical measurement.
    ///
    /// Return true if a time measurement existed and was converted, false
    /// otherwise.
    pub fn unset_temporal(
        &mut self,
    ) -> Option<<M::Optical as OpticalFromTemporal<M::Temporal>>::TData>
    where
        M::Optical: OpticalFromTemporal<M::Temporal, Loss = ()>,
        M::Temporal: VersionedTemporal<Err = Infallible>,
    {
        self.measurements
            .unset_center(|i, old_t| {
                <M::Optical as OpticalFromTemporal<M::Temporal>>::from_temporal(old_t, i, ())
            })
            .unwrap_infallible()
    }

    /// Convert time measurement to optical measurement.
    ///
    /// Return true if a time measurement existed and was converted, false
    /// otherwise.
    #[allow(clippy::type_complexity)]
    pub fn unset_temporal_lossy(
        &mut self,
        allow_loss: bool,
    ) -> TerminalResult<
        Option<<M::Optical as OpticalFromTemporal<M::Temporal>>::TData>,
        TemporalToOpticalError,
        TemporalToOpticalError,
        UnsetTemporalFailure,
    >
    where
        M::Optical: OpticalFromTemporal<M::Temporal, Loss = bool>,
        M::Temporal: VersionedTemporal<Err = TemporalToOpticalError>,
    {
        self.measurements
            .unset_center(|i, old_t| {
                <M::Optical as OpticalFromTemporal<M::Temporal>>::from_temporal(
                    old_t, i, allow_loss,
                )
            })
            .terminate(UnsetTemporalFailure)
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
        xs: Vec<HashMap<NonStdKey, String>>,
    ) -> Result<(), KeyLengthError> {
        self.measurements
            .alter_common_values_zip(xs, |_, y: &mut HashMap<_, _>, x| *y = x)
            .void()
    }

    /// Replace optical measurement at index.
    ///
    /// If index points to a temporal measurement, replace it with the given
    /// optical measurement. In both cases the name is kept. Return the
    /// measurement that was replaced if the index was in bounds.
    #[allow(clippy::type_complexity)]
    pub fn replace_optical_at(
        &mut self,
        index: MeasIndex,
        m: Optical<M::Optical>,
    ) -> Result<Element<Temporal<M::Temporal>, Optical<M::Optical>>, ElementIndexError> {
        self.measurements.replace_at(index, m)
    }

    /// Replace optical measurement with name.
    ///
    /// If name refers to a temporal measurement, replace it with the given
    /// optical measurement. Return the measurement that was replaced if the
    /// index was in bounds.
    #[allow(clippy::type_complexity)]
    pub fn replace_optical_named(
        &mut self,
        name: &Shortname,
        m: Optical<M::Optical>,
    ) -> Option<Element<Temporal<M::Temporal>, Optical<M::Optical>>> {
        self.measurements.replace_named(name, m)
    }

    /// Replace temporal measurement at index.
    #[allow(clippy::type_complexity)]
    pub fn replace_temporal_at(
        &mut self,
        index: MeasIndex,
        m: Temporal<M::Temporal>,
    ) -> Result<Element<Temporal<M::Temporal>, Optical<M::Optical>>, SetCenterError>
    where
        M::Optical: OpticalFromTemporal<M::Temporal, Loss = ()>,
        M::Temporal: VersionedTemporal<Err = Infallible>,
    {
        self.measurements
            .replace_center_at_nofail(index, m, |i, old_t| {
                <M::Optical as OpticalFromTemporal<M::Temporal>>::from_temporal(old_t, i, ())
                    .def_void_passthru()
                    .def_unwrap_infallible()
                    .0
            })
    }

    /// Replace temporal measurement at index.
    #[allow(clippy::type_complexity)]
    pub fn replace_temporal_at_lossy(
        &mut self,
        index: MeasIndex,
        m: Temporal<M::Temporal>,
        allow_loss: bool,
    ) -> TerminalResult<
        Element<Temporal<M::Temporal>, Optical<M::Optical>>,
        ReplaceTemporalError,
        ReplaceTemporalError,
        ReplaceTemporalFailure,
    >
    where
        M::Optical: OpticalFromTemporal<M::Temporal, Loss = bool>,
        M::Temporal: VersionedTemporal<Err = TemporalToOpticalError>,
    {
        self.measurements
            .replace_center_at(index, m, |i, old_t| {
                <M::Optical as OpticalFromTemporal<M::Temporal>>::from_temporal(
                    old_t, i, allow_loss,
                )
                .def_inner_into()
                .def_map_value(|(x, _)| x)
            })
            .def_terminate(ReplaceTemporalFailure)
    }

    /// Replace temporal measurement at index.
    #[allow(clippy::type_complexity)]
    pub fn replace_temporal_named(
        &mut self,
        name: &Shortname,
        m: Temporal<M::Temporal>,
    ) -> Option<Element<Temporal<M::Temporal>, Optical<M::Optical>>>
    where
        M::Optical: OpticalFromTemporal<M::Temporal, Loss = ()>,
        M::Temporal: VersionedTemporal<Err = Infallible>,
    {
        self.measurements
            .replace_center_by_name_nofail(name, m, |i, old_t| {
                <M::Optical as OpticalFromTemporal<M::Temporal>>::from_temporal(old_t, i, ())
                    .def_void_passthru()
                    .def_unwrap_infallible()
                    .0
            })
    }

    /// Replace temporal measurement at index.
    #[allow(clippy::type_complexity)]
    pub fn replace_temporal_named_lossy(
        &mut self,
        name: &Shortname,
        m: Temporal<M::Temporal>,
        allow_loss: bool,
    ) -> TerminalResult<
        Option<Element<Temporal<M::Temporal>, Optical<M::Optical>>>,
        ReplaceTemporalError,
        ReplaceTemporalError,
        ReplaceTemporalFailure,
    >
    where
        M::Optical: OpticalFromTemporal<M::Temporal, Loss = bool>,
        M::Temporal: VersionedTemporal<Err = TemporalToOpticalError>,
    {
        self.measurements
            .replace_center_by_name(name, m, |i, old_t| {
                <M::Optical as OpticalFromTemporal<M::Temporal>>::from_temporal(
                    old_t, i, allow_loss,
                )
                .def_inner_into()
                .def_map_value(|(x, _)| x)
            })
            .def_terminate(ReplaceTemporalFailure)
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
        key: <M::Name as MightHave>::Wrapper<Shortname>,
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
        F: Fn(
            IndexedElement<&<M::Name as MightHave>::Wrapper<Shortname>, &mut Optical<M::Optical>>,
        ) -> R,
        G: Fn(IndexedElement<&Shortname, &mut Temporal<M::Temporal>>) -> R,
    {
        self.measurements.alter_values(f, g)
    }

    /// Apply functions to measurement values with payload
    pub fn alter_measurements_zip<F, G, X, R>(
        &mut self,
        xs: Vec<X>,
        f: F,
        g: G,
    ) -> Result<Vec<R>, KeyLengthError>
    where
        F: Fn(
            IndexedElement<&<M::Name as MightHave>::Wrapper<Shortname>, &mut Optical<M::Optical>>,
            X,
        ) -> R,
        G: Fn(IndexedElement<&Shortname, &mut Temporal<M::Temporal>>, X) -> R,
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
    pub fn set_meas<X>(&mut self, xs: Vec<X>) -> Result<(), KeyLengthError>
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
            .void()
    }

    /// Return field from all optical measurements as an iterator
    pub fn optical<'a, X: 'a>(&'a self) -> impl Iterator<Item = NonCenterElement<&'a X>>
    where
        Optical<M::Optical>: AsRef<X>,
    {
        self.measurements
            .iter()
            .map(|e| e.bimap(|_| (), |v| v.value.as_ref()).into())
    }

    /// Return optional field from all optical measurements as an iterator
    pub fn optical_opt<'a, X: 'a>(&'a self) -> impl Iterator<Item = NonCenterElement<Option<&'a X>>>
    where
        Optical<M::Optical>: AsRef<Option<X>>,
    {
        self.optical()
            .map(|e| e.0.map_non_center(|x| x.as_ref()).into())
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
    pub fn set_optical<X>(
        &mut self,
        xs: Vec<NonCenterElement<X>>,
    ) -> TerminalResult<(), Infallible, SetElementsError, SetOpticalFailure>
    where
        Optical<M::Optical>: AsMut<X>,
    {
        let ys = xs.into_iter().map(|x| x.0).collect();
        self.measurements
            .alter_elements_zip(ys, |m, x| *m.value.as_mut() = x, |_, ()| ())
            .void()
            .mult_terminate(SetOpticalFailure)
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
            .map(|x| x.bimap(|m| m.value.as_ref(), |m| m.value.as_ref()))
    }

    /// Set field which is on both optical and temporal measurement types
    pub fn set_temporal_optical<T>(&mut self, xs: Vec<T>) -> Result<(), KeyLengthError>
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
            .void()
    }

    /// Set field which is on both optical and temporal measurement types
    pub fn set_temporal_optical2<X, Y>(
        &mut self,
        xs: Vec<Element<X, Y>>,
    ) -> TerminalResult<(), Infallible, SetElementsError, SetOpticalFailure>
    where
        Temporal<M::Temporal>: AsMut<X>,
        Optical<M::Optical>: AsMut<Y>,
    {
        self.measurements
            .alter_elements_zip(
                xs,
                |m, x| *m.value.as_mut() = x,
                |m, y| *m.value.as_mut() = y,
            )
            .void()
            .mult_terminate(SetOpticalFailure)
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
    pub fn set_spillover(&mut self, spillover: Option<Spillover>) -> Result<(), SpilloverLinkError>
    where
        M: HasSpillover,
    {
        if let Some(s) = spillover.as_ref() {
            let current = self.all_shortnames();
            let ms: &[Shortname] = s.as_ref();
            let ns: HashSet<_> = ms.iter().collect();
            if !ns.is_subset(&current.iter().collect()) {
                return Err(SpilloverLinkError);
            }
        }
        *self.metaroot.specific.spill_mut(private::NoTouchy) = spillover;
        Ok(())
    }

    /// Set $UNSTAINEDCENTERS
    ///
    /// Will return error for each name that is not in $PnN.
    pub fn set_unstained_centers(
        &mut self,
        us: Option<UnstainedCenters>,
    ) -> TerminalResult<(), Infallible, MissingMeasurementNameError, SetUnstainedFailure>
    where
        M: HasUnstainedCenters,
    {
        let ms = self.measurement_names();
        if let Some(es) = us.as_ref().map(|xs| xs.names()).and_then(|ns| {
            NonEmpty::collect(
                ns.difference(&ms)
                    .map(|&n| MissingMeasurementNameError(n.clone())),
            )
        }) {
            return Err(es).mult_terminate(SetUnstainedFailure);
        }
        *self
            .metaroot
            .specific
            .unstainedcenters_mut(private::NoTouchy) = us;
        Ok(Terminal::default())
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

    /// Set $PnE (2.0)
    pub fn set_scales(
        &mut self,
        scales: Vec<Option<Scale>>,
    ) -> TerminalResult<(), Infallible, SetScalesError, SetScalesFailure>
    where
        M::Optical: HasScale,
    {
        let go = || {
            let l = &self.layout;
            let xforms: Vec<_> = scales
                .iter()
                .copied()
                .map(|s| s.map(ScaleTransform::from).unwrap_or_default())
                .collect();
            l.check_transforms_and_len(&xforms[..]).mult_errors_into()?;
            // ASSUME this won't panic because we checked length above
            if let Some(i) = self.measurements.center_index().map(usize::from)
                && scales[i] != Some(Scale::Linear)
            {
                return Err(NonEmpty::new(NonLinearTemporalScaleError.into()));
            }
            // ASSUME this won't fail because we checked the length and time
            // index first
            self.measurements
                .alter_values_zip(
                    scales,
                    |m, x| *m.value.specific.scale_mut(private::NoTouchy) = x,
                    |_, _| (),
                )
                .void()
                .unwrap();
            Ok(())
        };
        go().mult_terminate(SetScalesFailure)
    }

    /// Set $PnE/$PnG (3.0+)
    pub fn set_transforms(
        &mut self,
        xforms: Vec<ScaleTransform>,
    ) -> TerminalResult<(), Infallible, SetTransformsError, SetTransformsFailure>
    where
        M::Optical: HasScaleTransform,
    {
        let go = || {
            let l = &self.layout;
            l.check_transforms_and_len(&xforms[..]).mult_errors_into()?;
            // ASSUME this won't panic because we checked length first
            if let Some(i) = self.measurements.center_index().map(usize::from)
                && !xforms[i].is_noop()
            {
                return Err(NonEmpty::new(NonLinearTemporalTransformError.into()));
            }
            // ASSUME this won't fail because we checked the length first
            self.measurements
                .alter_values_zip(
                    xforms,
                    |m, x| *m.value.specific.transform_mut(private::NoTouchy) = x,
                    |_, _| (),
                )
                .void()
                .unwrap();
            Ok(())
        };
        go().mult_terminate(SetTransformsFailure)
    }

    /// Return $PAR, which is simply the number of measurements in this struct
    pub fn par(&self) -> Par {
        Par(self.measurements.len())
    }

    /// Set gating keywords (3.0/3.1)
    pub fn set_applied_gates_3_0(&mut self, ag: AppliedGates3_0) -> Result<(), GatingMeasLinkError>
    where
        M: HasAppliedGates3_0,
    {
        let js = (0..self.par().0).map(MeasIndex::from).collect();
        if let Some(ne) = NonEmpty::collect(ag.indices_difference(&js)) {
            return Err(GatingMeasLinkError(ne));
        }
        *self
            .metaroot
            .specific
            .applied_gates3_0_mut(private::NoTouchy) = ag;
        Ok(())
    }

    /// Set gating keywords (3.2)
    pub fn set_applied_gates_3_2(&mut self, ag: AppliedGates3_2) -> Result<(), GatingMeasLinkError>
    where
        M: HasAppliedGates3_2,
    {
        let js = (0..self.par().0).map(MeasIndex::from).collect();
        if let Some(ne) = NonEmpty::collect(ag.indices_difference(&js)) {
            return Err(GatingMeasLinkError(ne));
        }
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
        allow_loss: bool,
    ) -> TerminalResult<
        VersionedCore<A, D, O, ToM>,
        MetarootConvertWarning,
        VersionedConvertError<M::Name, ToM::Name>,
        ConvertFailure,
    >
    where
        Version: From<M::Ver> + From<ToM::Ver>,
        M::Name: Clone,
        ToM: VersionedMetaroot + ConvertFromMetaroot<M>,
        ToM::Optical: VersionedOptical + ConvertFromOptical<M::Optical>,
        ToM::Temporal: VersionedTemporal + ConvertFromTemporal<M::Temporal>,
        ToM::Name: MightHave + Clone,
        <ToM::Ver as Versioned>::Layout: ConvertFromLayout<<M::Ver as Versioned>::Layout>,
        <ToM::Name as MightHave>::Wrapper<Shortname>:
            TryFrom<<M::Name as MightHave>::Wrapper<Shortname>>,
    {
        let m = self
            .metaroot
            .try_convert(allow_loss)
            .def_map_errors(ConvertErrorInner::Meta);
        let ps = self
            .measurements
            .map_center_value(|y| Ok(y.value.convert(y.index, allow_loss)))
            .def_map_errors(ConvertErrorInner::Temporal)
            .def_warnings_into()
            .def_and_maybe(|xs| {
                xs.map_non_center_values(|i, v| v.try_convert(i, allow_loss))
                    .def_map_errors(ConvertErrorInner::Optical)
                    .def_warnings_into()
            })
            .def_and_maybe(|x| {
                x.try_rewrapped()
                    .mult_map_errors(ConvertErrorInner::Rewrap)
                    .mult_to_deferred()
            });
        let lres = ConvertFromLayout::convert_from_layout(self.layout)
            .mult_map_errors(ConvertErrorInner::Layout)
            .mult_to_deferred();
        m.def_zip3(ps, lres)
            .def_map_value(|(metaroot, measurements, layout)| {
                Core::new(
                    metaroot,
                    measurements,
                    layout,
                    self.data,
                    self.analysis,
                    self.others,
                )
            })
            .def_map_errors(|error| {
                ConvertError::new(M::Ver::fcs_version(), ToM::Ver::fcs_version(), error)
            })
            .def_terminate(ConvertFailure)
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

    // TODO also return the removed layout
    #[allow(clippy::type_complexity)]
    fn remove_measurement_by_name_inner(
        &mut self,
        name: &Shortname,
    ) -> Result<
        (
            MeasIndex,
            Element<Temporal<M::Temporal>, Optical<M::Optical>>,
        ),
        RemoveMeasByNameError,
    > {
        let xs = self.measurement_named_indices();
        if let Some(index) = xs.get(name) {
            self.metaroot.check_meas_links(&[(*index, name)])?;
        }
        let ret = self.measurements.remove_name(name)?;
        self.layout.remove_nocheck(ret.0);
        Ok(ret)
    }

    #[allow(clippy::type_complexity)]
    fn remove_measurement_by_index_inner(
        &mut self,
        index: MeasIndex,
    ) -> Result<
        EitherPair<M::Name, Temporal<M::Temporal>, Optical<M::Optical>>,
        RemoveMeasByIndexError,
    > {
        let xs = self.measurement_indexed_names();
        if let Some(name) = xs.get(&index) {
            self.metaroot.check_meas_links(&[(index, name)])?;
        }
        let ret = self.measurements.remove_index(index)?;
        self.layout.remove_nocheck(index);
        Ok(ret)
    }

    fn push_temporal_inner(
        &mut self,
        n: Shortname,
        m: Temporal<M::Temporal>,
        r: Range,
        disallow_trunc: bool,
    ) -> DeferredResult<(), AnyRangeError, InsertTemporalError> {
        self.metaroot
            .specific
            .insert_meas_index_inner(self.par().0.into());
        self.measurements
            .push_center(n, m)
            .into_deferred()
            .def_and_tentatively(|()| self.layout.push(r, disallow_trunc).inner_into())
    }

    fn insert_temporal_inner(
        &mut self,
        i: MeasIndex,
        n: Shortname,
        m: Temporal<M::Temporal>,
        r: Range,
        disallow_trunc: bool,
    ) -> DeferredResult<(), AnyRangeError, InsertTemporalError> {
        self.metaroot.specific.insert_meas_index_inner(i);
        self.measurements
            .insert_center(i, n, m)
            .into_deferred()
            .def_and_tentatively(|()| {
                self.layout
                    .insert_nocheck(i, r, disallow_trunc)
                    .inner_into()
            })
    }

    fn push_optical_inner(
        &mut self,
        n: <M::Name as MightHave>::Wrapper<Shortname>,
        m: Optical<M::Optical>,
        r: Range,
        disallow_trunc: bool,
    ) -> DeferredResult<Shortname, AnyRangeError, PushOpticalError> {
        self.metaroot
            .specific
            .insert_meas_index_inner(self.par().0.into());
        self.measurements
            .push(n, m)
            .into_deferred()
            .def_and_tentatively(|ret| {
                self.layout
                    .push(r, disallow_trunc)
                    .errors_into()
                    .map(|()| ret)
            })
    }

    fn insert_optical_inner(
        &mut self,
        i: MeasIndex,
        n: <M::Name as MightHave>::Wrapper<Shortname>,
        m: Optical<M::Optical>,
        r: Range,
        disallow_trunc: bool,
    ) -> DeferredResult<Shortname, AnyRangeError, InsertOpticalError> {
        self.metaroot.specific.insert_meas_index_inner(i);
        self.measurements
            .insert(i, n, m)
            .into_deferred()
            .def_and_tentatively(|ret| {
                self.layout
                    .insert_nocheck(i, r, disallow_trunc)
                    .map(|()| ret)
                    .errors_into()
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
    pub fn set_measurements(
        &mut self,
        xs: Eithers<M::Name, Temporal<M::Temporal>, Optical<M::Optical>>,
        allow_shared_names: bool,
        skip_index_check: bool,
    ) -> TerminalResult<(), Infallible, SetMeasurementsError, SetMeasurementsFailure>
    where
        M::Optical: AsScaleTransform,
    {
        self.set_measurements_inner(xs, allow_shared_names, skip_index_check)
            .mult_terminate(SetMeasurementsFailure)
    }

    // TODO add replace measurements function which doesn't touch PnN but
    // requires time meas to be in the same location

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
    ) -> TerminalResult<(), Infallible, MeasLayoutMismatchError, SetLayoutFailure>
    where
        M::Optical: AsScaleTransform,
    {
        layout
            .check_measurement_vector(&self.measurements)
            .mult_terminate(SetLayoutFailure)?;
        self.layout = layout;
        Ok(Terminal::default())
    }

    /// Set measurements and layout
    ///
    /// Return error if measurement names are not unique, there is more
    /// than one time measurement, or the layout and measurements have
    /// different lengths.
    pub fn set_measurements_and_layout(
        &mut self,
        measurements: Eithers<M::Name, Temporal<M::Temporal>, Optical<M::Optical>>,
        layout: <M::Ver as Versioned>::Layout,
        allow_shared_names: bool,
        skip_index_check: bool,
    ) -> TerminalResult<(), Infallible, SetMeasurementsError, SetMeasurementsAndLayoutFailure>
    where
        M::Optical: AsScaleTransform,
    {
        let go = || {
            self.check_new_meas_links(&measurements, allow_shared_names, skip_index_check)
                .into_mult()?;
            let ms = NamedVec::try_new(measurements).into_mult()?;
            layout.check_measurement_vector(&ms).mult_errors_into()?;
            self.measurements = ms;
            self.layout = layout;
            Ok(())
        };
        go().mult_terminate(SetMeasurementsAndLayoutFailure)
    }

    pub fn set_measurements_inner(
        &mut self,
        measurements: Eithers<M::Name, Temporal<M::Temporal>, Optical<M::Optical>>,
        allow_shared_names: bool,
        skip_index_check: bool,
    ) -> MultiResult<(), SetMeasurementsError>
    where
        M::Optical: AsScaleTransform,
    {
        self.check_new_meas_links(&measurements, allow_shared_names, skip_index_check)
            .into_mult()?;
        let ms = NamedVec::try_new(measurements).into_mult()?;
        self.layout
            .check_measurement_vector(&ms)
            .mult_errors_into()?;
        self.measurements = ms;
        Ok(())
    }

    fn unset_measurements_inner(&mut self) -> Result<(), ExistingLinkError> {
        let xs: Vec<_> = self.measurement_indexed_names().into_iter().collect();
        self.metaroot.check_meas_links(&xs)?;
        self.measurements = NamedVec::default();
        self.layout.clear();
        Ok(())
    }

    fn header_and_raw_keywords<T>(
        &self,
        tot: Tot,
        data_len: u64,
        analysis_len: u64,
        other_lens: &[u64],
        has_nextdata: bool,
    ) -> Result<HeaderKeywordsToWrite<T>, Uint8DigitOverflow>
    where
        Version: From<M::Ver>,
        T: TryFrom<u64, Error = Uint8DigitOverflow> + HeaderString,
    {
        let req: Vec<_> = self
            .req_root_keywords()
            .chain([tot.pair()])
            .chain(self.req_meas_keywords())
            .collect();
        let opt: Vec<_> = self
            .opt_root_keywords()
            .chain(self.opt_meas_keywords())
            .collect();
        if Version::from(M::Ver::fcs_version()) == Version::FCS2_0 {
            HeaderKeywordsToWrite::new_2_0(
                req,
                opt,
                data_len,
                analysis_len,
                other_lens,
                has_nextdata,
            )
        } else {
            HeaderKeywordsToWrite::new_3_0(
                req,
                opt,
                data_len,
                analysis_len,
                other_lens,
                has_nextdata,
            )
        }
    }

    fn opt_meas_keywords(&self) -> impl Iterator<Item = (String, String)> {
        let ns = (!M::Name::INFALLABLE).then(|| self.shortname_keywords());
        let lv = self.layout.opt_meas_keywords();
        self.measurements
            .iter_with(
                &|i, x| Temporal::opt_meas_keywords(&x.value, i).collect::<Vec<_>>(),
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
        if let Some(m0) = ms.get(0.into()).ok().and_then(Element::non_center) {
            let lt = &self.layout;
            let req_layout: Vec<_> = lt
                .req_meas_keywords()
                .into_iter()
                .map(|[x, y]| [x.1, y.1])
                .collect();
            let opt_layout: Vec<_> = lt
                .opt_meas_keywords()
                .into_iter()
                .map(|xs| xs.into_iter().map(|(_, v)| v).collect::<Vec<_>>())
                .collect();
            let header = m0.1.table_header(lt.opt_meas_headers());
            let rows = self
                .measurements
                .iter()
                .map(|r| {
                    // NOTE this will force-convert all fields in the time
                    // measurement, which for this is actually want we want
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

    #[allow(clippy::type_complexity)]
    fn lookup_measurements(
        std: &mut StdKeywords,
        par: Par,
        nonstd: &mut NonStdKeywords,
        conf: &StdTextReadConfig,
    ) -> DeferredResult<
        Measurements<M::Name, M::Temporal, M::Optical>,
        LookupMeasWarning,
        LookupKeysError,
    >
    where
        M: LookupMetaroot,
        M::Temporal: LookupTemporal,
        M::Optical: LookupOptical,
        Version: From<M::Ver>,
    {
        // Use nonstandard measurement pattern to assign keyvals to their
        // measurement if they match. Only capture one warning because if the
        // pattern is wrong for one measurement it is probably wrong for all of
        // them.
        let blank_meas_nonstd = || vec![HashMap::new(); par.0];
        let tnt = conf.nonstandard_measurement_pattern.as_ref().map_or(
            Tentative::new1(blank_meas_nonstd()),
            |pat| {
                (0..par.0)
                    .map(|n| {
                        pat.apply_index(n).map(|p| {
                            let r: &Regex = p.as_ref();
                            nonstd.extract_if(|k, _| r.is_match(k.as_ref())).collect()
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .into_tentative_warn(blank_meas_nonstd())
            },
        );

        // then iterate over each measurement and look for standardized keys
        tnt.warnings_into().and_maybe(|meas_nonstds| {
            meas_nonstds
                .into_iter()
                .enumerate()
                .map(|(n, meas_nonstd)| {
                    let i = n.into();
                    // Try to find $PnN first, for later versions this will
                    // totally fail if not found since this is required. If it
                    // does exist, also check if it matches the time pattern and
                    // use it as the time measurement if it does.
                    M::lookup_shortname(std, i, conf).def_and_maybe(|wrapped| {
                        // TODO if more than one name matches the time pattern
                        // this will give a cryptic "cannot find $TIMESTEP" for
                        // each subsequent match, which is not helpful. Probably
                        // the best way around this is to add measurement index
                        // and possibly key to the error, so at least the user
                        // will know it is trying to find $TIMESTEP in a
                        // nonsense measurement.
                        let key = M::Name::unwrap(wrapped).and_then(|name| {
                            if let Some(tp) = conf.time_meas_pattern.as_ref()
                                && tp.0.is_match(name.as_ref())
                            {
                                return Ok(name);
                            }
                            Err(M::Name::wrap(name))
                        });
                        // Once we checked $PnN, pull all the rest of the
                        // standardized keywords from the hashtable and collect
                        // errors. In general, required keywords will trigger an
                        // error if they are missing and optional keywords will
                        // trigger a warning. Either can generate an
                        // error/warning if they fail to be parsed to their type
                        match key {
                            // TODO add switch to "downgrade" failed time
                            // channel to optical channel, which is more general
                            Ok(name) => Temporal::lookup_temporal(std, i, meas_nonstd, conf)
                                .def_map_value(|t| Element::Center((name, t))),
                            Err(k) => Optical::lookup_optical(std, i, meas_nonstd, conf)
                                .def_map_value(|m| Element::NonCenter((k, m))),
                        }
                    })
                })
                .gather()
                .map_err(DeferredFailure::mconcat)
                .map(Tentative::mconcat)
                .def_and_maybe(|xs| {
                    // Finally, attempt to put our proto-measurement binary soup
                    // into a named vector, which will have a special element
                    // for the time measurement if it exists, and will scream if
                    // we have more than one time measurement.
                    NamedVec::try_new(xs.into()).into_deferred()
                })
                .def_warnings_into()
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

    fn measurement_names(&self) -> HashSet<&Shortname> {
        self.measurements.indexed_names().map(|(_, x)| x).collect()
    }

    fn check_meas_any_named_links(&self) -> Result<(), ExistingNamedLinkError> {
        self.metaroot
            .check_meas_named_links(&self.measurement_names())
    }

    fn check_meas_any_index_links(&self) -> Result<(), ExistingIndexLinkError> {
        let indices: HashSet<_> = (0..self.par().0).map(MeasIndex::from).collect();
        self.metaroot
            .specific
            .check_meas_index_links_inner(&indices)
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
    /// will be broken. If `skip_index_check` is true, bypass this assumption.
    /// This should only be true when the user knows that measurements that have
    /// links are in the same order b/t new and old.
    ///
    /// The number of measurements is assumed to be correct; this should be
    /// checked elsewhere.
    fn check_new_meas_links<X, Y>(
        &self,
        measurements: &Eithers<M::Name, X, Y>,
        allow_shared_names: bool,
        skip_index_check: bool,
    ) -> Result<(), ExistingLinkError> {
        if allow_shared_names {
            let ns = measurements.non_center_names().collect();
            self.metaroot.check_meas_named_links(&ns)?;
        } else {
            self.check_meas_any_named_links()?;
        }
        if !skip_index_check {
            self.check_meas_any_index_links()?;
        }
        Ok(())
    }
}

impl<M> VersionedCoreTEXT<M>
where
    M: VersionedMetaroot,
    M::Name: Clone,
{
    pub(crate) fn new_from_keywords_with_offsets<C>(
        mut kws: ValidKeywords,
        data: HeaderDataSegment,
        analysis: HeaderAnalysisSegment,
        st: &ReadState<C>,
    ) -> DeferredResult<
        (Self, ExtraStdKeywords, <M::Ver as Versioned>::Offsets),
        StdTEXTFromRawWarning,
        StdTEXTFromRawError,
    >
    where
        M: LookupMetaroot,
        M::Temporal: LookupTemporal,
        M::Optical: LookupOptical,
        Version: From<M::Ver>,
        <M::Ver as Versioned>::Layout: VersionedDataLayout,
        C: AsRef<StdTextReadConfig> + AsRef<ReadLayoutConfig> + AsRef<ReadTEXTOffsetsConfig>,
    {
        // Lookup DATA/ANALYSIS offsets and $TOT; these are not stored in the
        // Core struct but they will be needed later for parsing DATA and
        // ANALYSIS, and processing these keywords now will make it easier to
        // determine if TEXT is totally standardized or not.
        let offsets_res = <M::Ver as Versioned>::Offsets::lookup(&mut kws.std, data, analysis, st)
            .def_inner_into();

        Self::lookup_inner(kws, &st.conf)
            .def_zip(offsets_res)
            .def_map_value(|((x, y), z)| (x, y, z))
    }

    /// Make a new CoreTEXT from raw keywords.
    ///
    /// Return any errors encountered, including missing required keywords,
    /// parse errors, and/or deprecation warnings.
    ///
    /// This will not process $TOT or $(BEGIN|END)(TEXT|DATA). If present these
    /// will trigger pseudostandard warnings.
    pub fn new_from_keywords<C>(
        kws: ValidKeywords,
        conf: &C,
    ) -> TerminalResult<
        (Self, ExtraStdKeywords),
        StdTEXTFromRawWarning,
        StdTEXTFromKeywordsError,
        CoreTEXTFromKeywordsFailure,
    >
    where
        M: LookupMetaroot,
        M::Temporal: LookupTemporal,
        M::Optical: LookupOptical,
        Version: From<M::Ver>,
        <M::Ver as Versioned>::Layout: VersionedDataLayout,
        C: AsRef<StdTextReadConfig> + AsRef<ReadLayoutConfig> + AsRef<SharedConfig>,
    {
        Self::lookup_inner(kws, conf)
            .def_errors_into()
            .def_terminate_maybe_warn(CoreTEXTFromKeywordsFailure, conf.as_ref(), Into::into)
    }

    fn lookup_inner<C>(
        mut kws: ValidKeywords,
        conf: &C,
    ) -> DeferredResult<(Self, ExtraStdKeywords), StdTEXTFromRawWarning, StdTEXTFromRawError>
    where
        M: LookupMetaroot,
        M::Temporal: LookupTemporal,
        M::Optical: LookupOptical,
        Version: From<M::Ver>,
        <M::Ver as Versioned>::Layout: VersionedDataLayout,
        C: AsRef<StdTextReadConfig> + AsRef<ReadLayoutConfig>,
    {
        // $NEXTDATA/$BEGINSTEXT/$ENDSTEXT should have already been
        // processed when we read the TEXT; remove them so they don't
        // trigger false positives later when we test for pseudostandard keys
        let _ = kws.std.remove(&Nextdata::std());
        let _ = kws.std.remove(&Beginstext::std());
        let _ = kws.std.remove(&Endstext::std());

        // Lookup $PAR first since we need this to get the measurements
        let par_res = Par::lookup_req(&mut kws.std).def_inner_into();

        let version = Version::from(M::Ver::fcs_version());
        let std_conf = conf.as_ref();

        par_res.def_and_maybe(|par| {
            // Lookup measurements/layout/metaroot with $PAR
            let meas_res = Self::lookup_measurements(&mut kws.std, par, &mut kws.nonstd, std_conf)
                .def_inner_into();
            let layout_res = <M::Ver as Versioned>::Layout::lookup(&mut kws.std, conf, par)
                .def_map_errors(Box::new)
                .def_inner_into();
            meas_res
                .def_zip(layout_res)
                .def_and_maybe(|(ms, layout)| {
                    Metaroot::lookup_metaroot(&mut kws.std, &ms, kws.nonstd, std_conf)
                        .def_map_value(|metaroot| Self::new_unchecked(metaroot, ms, layout))
                        .def_inner_into()
                })
                .map(|mut tnt_core| {
                    // Check that the time measurement is present if we want
                    // it and the measurement vector is non-empty
                    let go = |core: &Self| {
                        if let Some(pat) = std_conf.time_meas_pattern.as_ref()
                            && core.measurements.as_center().is_none()
                            && !core.measurements.is_empty()
                        {
                            return Some(LookupKeysWarning::from(MissingTime(pat.clone())));
                        }
                        None
                    };
                    if std_conf.allow_missing_time {
                        tnt_core.eval_warning(go);
                    } else {
                        tnt_core.eval_error(|c| go(c).map(LookupKeysError::WarnAsError));
                    }
                    let esks = match version {
                        Version::FCS2_0 => ExtraStdKeywords::split_2_0(kws.std),
                        Version::FCS3_0 => ExtraStdKeywords::split_3_0(kws.std),
                        Version::FCS3_1 => ExtraStdKeywords::split_3_1(kws.std),
                        Version::FCS3_2 => ExtraStdKeywords::split_3_2(kws.std),
                    };

                    let ps = esks.pseudostandard.keys().cloned().map(PseudostandardError);
                    tnt_core.extend_errors_or_warnings(ps, !std_conf.allow_pseudostandard);

                    let us = esks.unused.keys().cloned().map(UnusedStandardError);
                    tnt_core.extend_errors_or_warnings(us, !std_conf.allow_unused_standard);

                    tnt_core.map(|x| (x, esks))
                })
        })
    }

    /// Remove a measurement matching the given name.
    ///
    /// Return removed measurement and its index if found.
    #[allow(clippy::type_complexity)]
    pub fn remove_measurement_by_name(
        &mut self,
        n: &Shortname,
    ) -> Result<
        (
            MeasIndex,
            Element<Temporal<M::Temporal>, Optical<M::Optical>>,
        ),
        RemoveMeasByNameError,
    > {
        self.remove_measurement_by_name_inner(n)
    }

    /// Remove a measurement at a given position
    ///
    /// Return removed measurement and its name if found.
    #[allow(clippy::type_complexity)]
    pub fn remove_measurement_by_index(
        &mut self,
        index: MeasIndex,
    ) -> Result<
        EitherPair<M::Name, Temporal<M::Temporal>, Optical<M::Optical>>,
        RemoveMeasByIndexError,
    > {
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
        disallow_trunc: bool,
    ) -> TerminalResult<(), AnyRangeError, InsertTemporalError, PushTemporalFailure> {
        self.push_temporal_inner(n, m, r, disallow_trunc)
            .def_terminate(PushTemporalFailure)
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
        disallow_trunc: bool,
    ) -> TerminalResult<(), AnyRangeError, InsertTemporalError, InsertTemporalFailure> {
        self.insert_temporal_inner(i, n, m, r, disallow_trunc)
            .def_terminate(InsertTemporalFailure)
    }

    /// Add optical measurement to the end of the measurement vector
    ///
    /// Return error if name is non-unique.
    pub fn push_optical(
        &mut self,
        n: <M::Name as MightHave>::Wrapper<Shortname>,
        m: Optical<M::Optical>,
        r: Range,
        disallow_trunc: bool,
    ) -> TerminalResult<Shortname, AnyRangeError, PushOpticalError, PushOpticalFailure> {
        self.push_optical_inner(n, m, r, disallow_trunc)
            .def_terminate(PushOpticalFailure)
    }

    /// Add optical measurement at a given position
    ///
    /// Return error if name is non-unique, or index is out of bounds.
    pub fn insert_optical(
        &mut self,
        i: MeasIndex,
        n: <M::Name as MightHave>::Wrapper<Shortname>,
        m: Optical<M::Optical>,
        r: Range,
        disallow_trunc: bool,
    ) -> TerminalResult<Shortname, AnyRangeError, InsertOpticalError, InsertOpticalFailure> {
        self.insert_optical_inner(i, n, m, r, disallow_trunc)
            .def_terminate(InsertOpticalFailure)
    }

    /// Remove measurements
    pub fn unset_measurements(&mut self) -> Result<(), ExistingLinkError> {
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
}

impl<M> VersionedCoreDataset<M>
where
    M: VersionedMetaroot,
    M::Name: Clone,
    <M::Ver as Versioned>::Layout: VersionedDataLayout,
{
    pub fn new_from_keywords<C>(
        p: PathBuf,
        kws: ValidKeywords,
        data_seg: HeaderDataSegment,
        analysis_seg: HeaderAnalysisSegment,
        other_segs: &[OtherSegment20],
        conf: &C,
    ) -> IOTerminalResult<
        (Self, StdDatasetWithKwsOutput),
        StdDatasetFromRawWarning,
        StdDatasetFromRawError,
        StdDatasetWithKwsFailure,
    >
    where
        M: LookupMetaroot,
        M::Temporal: LookupTemporal,
        M::Optical: LookupOptical,
        Version: From<M::Ver>,
        <M::Ver as Versioned>::Offsets: AsRef<DatasetSegments>,
        C: AsRef<StdTextReadConfig>
            + AsRef<ReadLayoutConfig>
            + AsRef<ReaderConfig>
            + AsRef<ReadTEXTOffsetsConfig>
            + AsRef<SharedConfig>,
    {
        File::options()
            .read(true)
            .open(p)
            .and_then(|file| ReadState::init(&file, conf).map(|st| (st, file)))
            .into_deferred()
            .def_and_maybe(|(st, file)| {
                let mut h = BufReader::new(file);
                Self::new_from_keywords_inner(&mut h, kws, data_seg, analysis_seg, other_segs, &st)
            })
            .def_terminate_maybe_warn(StdDatasetWithKwsFailure, conf.as_ref(), |w| {
                ImpureError::Pure(StdDatasetFromRawError::from(w))
            })
    }

    pub(crate) fn new_from_keywords_inner<C, R>(
        h: &mut BufReader<R>,
        kws: ValidKeywords,
        data_seg: HeaderDataSegment,
        analysis_seg: HeaderAnalysisSegment,
        other_segs: &[OtherSegment20],
        st: &ReadState<C>,
    ) -> IODeferredResult<
        (Self, StdDatasetWithKwsOutput),
        StdDatasetFromRawWarning,
        StdDatasetFromRawError,
    >
    where
        R: Read + Seek,
        M: LookupMetaroot,
        M::Temporal: LookupTemporal,
        M::Optical: LookupOptical,
        Version: From<M::Ver>,
        <M::Ver as Versioned>::Offsets: AsRef<DatasetSegments>,
        C: AsRef<StdTextReadConfig>
            + AsRef<ReadLayoutConfig>
            + AsRef<ReaderConfig>
            + AsRef<ReadTEXTOffsetsConfig>,
    {
        VersionedCoreTEXT::<M>::new_from_keywords_with_offsets(kws, data_seg, analysis_seg, st)
            .def_map_errors(Box::new)
            .def_inner_into()
            .def_errors_liftio()
            .def_and_maybe(|(text, extra, offsets)| {
                let dataset_segs = offsets.as_ref();
                let or = OthersReader::new(other_segs);
                let ar = AnalysisReader::new(dataset_segs.analysis);
                let read_conf: &ReaderConfig = st.conf.as_ref();
                let data_res = text
                    .layout
                    .h_read_df(h, offsets.tot(), dataset_segs.data, read_conf)
                    .def_warnings_into()
                    .def_map_errors(ImpureError::inner_into);
                let analysis_res = ar.h_read(h).into_deferred();
                let others_res = or.h_read(h).into_deferred();
                let out = StdDatasetWithKwsOutput::new(*dataset_segs, extra);
                data_res.def_zip3(analysis_res, others_res).def_map_value(
                    |(data, analysis, others)| {
                        let c = text.into_coredataset_unchecked(data, analysis, others);
                        (c, out)
                    },
                )
            })
    }

    /// Write this dataset (HEADER+TEXT+DATA+ANALYSIS+OTHER) to a handle
    pub fn h_write_dataset<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        conf: &WriteConfig,
    ) -> IOTerminalResult<(), StdWriterWarning, StdWriterError, WriteDatasetFailure>
    where
        Version: From<M::Ver>,
    {
        let df = &self.data;
        let layout = &self.layout;
        let delim = conf.delim;
        let tot = Tot(df.nrows());
        let analysis_len =
            u64::try_from(self.analysis.0.len()).expect("ANALYSIS segment length exceeds 2^64");
        let others = &self.others.0[..];

        let check_res = if conf.skip_conversion_check {
            Ok(Tentative::default())
        } else {
            layout
                .check_writer(df)
                .map_err(DeferredFailure::new2)
                .map(|()| Tentative::default())
                .def_errors_into()
                .def_errors_liftio()
        };

        check_res
            .def_and_maybe(|()| {
                let data_len = layout.nbytes(df);
                if conf.big_other {
                    self.h_write_text_inner::<_, UintSpacePad20>(
                        h,
                        delim,
                        tot,
                        data_len,
                        analysis_len,
                        others,
                    )
                } else {
                    self.h_write_text_inner::<_, UintSpacePad8>(
                        h,
                        delim,
                        tot,
                        data_len,
                        analysis_len,
                        others,
                    )
                }
                .map_err(ImpureError::inner_into)
                .map_err(DeferredFailure::new1)?;

                // write DATA; conversion check flag is flipped from above since
                // we want to emit warnings as we are writing if we did not run
                // through the data once at the beginning and check for
                // conversion loss.
                layout
                    .h_write_df(h, df, !conf.skip_conversion_check)
                    .def_warnings_into()?;

                // write ANALYSIS
                h.write_all(&self.analysis.0).into_deferred()
            })
            .def_terminate(WriteDatasetFailure)
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
    pub fn unset_data(&mut self) -> Result<(), ExistingLinkError> {
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
    pub fn truncate_data(
        &mut self,
        skip_conv_check: bool,
    ) -> Terminal<(), ColumnError<AnyLossError>> {
        // TODO this function is hilariously not-optimized; each column will be
        // cast into a totally new vector even if they are they exact same
        // type with no possible truncation. This also means that the new
        // dataframe will be totally separate from the old one. Unfortunately,
        // the best fix for this requires specialization, since we need a way
        // to tell rust to do nothing when the input and output types match and
        // otherwise do something else.
        self.layout
            .truncate_df(&self.data, skip_conv_check)
            .map(|data| {
                self.data = data;
            })
            .into_terminal()
    }

    // TODO add function to append event(s)?

    /// Remove a measurement matching the given name.
    ///
    /// Return removed measurement and its index if found.
    #[allow(clippy::type_complexity)]
    pub fn remove_measurement_by_name(
        &mut self,
        n: &Shortname,
    ) -> Result<
        (
            MeasIndex,
            Element<Temporal<M::Temporal>, Optical<M::Optical>>,
        ),
        RemoveMeasByNameError,
    > {
        self.remove_measurement_by_name_inner(n).map(|(i, x)| {
            self.data.drop_in_place(i.into()).unwrap();
            (i, x)
        })
    }

    /// Remove a measurement at a given position
    ///
    /// Return removed measurement and its name if found.
    #[allow(clippy::type_complexity)]
    pub fn remove_measurement_by_index(
        &mut self,
        index: MeasIndex,
    ) -> Result<
        EitherPair<M::Name, Temporal<M::Temporal>, Optical<M::Optical>>,
        RemoveMeasByIndexError,
    > {
        let res = self.remove_measurement_by_index_inner(index)?;
        self.data.drop_in_place(index.into()).unwrap();
        Ok(res)
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
        disallow_trunc: bool,
    ) -> TerminalResult<(), AnyRangeError, PushTemporalToDatasetError, PushTemporalFailure> {
        self.push_temporal_inner(n, m, r, disallow_trunc)
            .def_errors_into()
            .def_and_maybe(|()| self.data.push_column(col).into_deferred())
            .def_terminate(PushTemporalFailure)
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
        disallow_trunc: bool,
    ) -> TerminalResult<(), AnyRangeError, InsertTemporalToDatasetError, InsertTemporalFailure>
    {
        self.insert_temporal_inner(i, n, m, r, disallow_trunc)
            .def_errors_into()
            .def_and_maybe(|()| {
                // ASSUME index is within bounds here since it was checked above
                self.data
                    .insert_column_nocheck(i.into(), col)
                    .into_deferred()
            })
            .def_terminate(InsertTemporalFailure)
    }

    /// Add measurement to the end of the measurement vector
    ///
    /// Return error if name is non-unique.
    pub fn push_optical(
        &mut self,
        n: <M::Name as MightHave>::Wrapper<Shortname>,
        m: Optical<M::Optical>,
        col: AnyFCSColumn,
        r: Range,
        disallow_trunc: bool,
    ) -> TerminalResult<Shortname, AnyRangeError, PushOpticalToDatasetError, PushOpticalFailure>
    {
        self.push_optical_inner(n, m, r, disallow_trunc)
            .def_errors_into()
            .def_and_maybe(|k| {
                self.data
                    .push_column(col)
                    .into_deferred()
                    .def_map_value(|()| k)
            })
            .def_terminate(PushOpticalFailure)
    }

    /// Add measurement at a given position
    ///
    /// Return error if name is non-unique, or index is out of bounds.
    pub fn insert_optical(
        &mut self,
        i: MeasIndex,
        n: <M::Name as MightHave>::Wrapper<Shortname>,
        m: Optical<M::Optical>,
        col: AnyFCSColumn,
        r: Range,
        disallow_trunc: bool,
    ) -> TerminalResult<Shortname, AnyRangeError, InsertOpticalInDatasetError, InsertOpticalFailure>
    {
        self.insert_optical_inner(i, n, m, r, disallow_trunc)
            .def_errors_into()
            .def_and_maybe(|k| {
                // ASSUME index is within bounds here since it was checked above
                self.data
                    .insert_column_nocheck(i.into(), col)
                    .into_deferred()
                    .def_map_value(|()| k)
            })
            .def_terminate(InsertOpticalFailure)
    }

    /// Convert this struct into a CoreTEXT.
    ///
    /// This simply entails taking ownership and dropping the ANALYSIS and DATA
    /// fields.
    pub fn into_coretext(self) -> VersionedCoreTEXT<M> {
        CoreTEXT::new_unchecked(self.metaroot, self.measurements, self.layout)
    }

    /// Set measurements and dataframe together
    ///
    /// Length of measurements must match the width of the input dataframe.
    pub fn set_measurements_and_data(
        &mut self,
        xs: Eithers<M::Name, Temporal<M::Temporal>, Optical<M::Optical>>,
        df: FCSDataFrame,
        allow_shared_names: bool,
        skip_index_check: bool,
    ) -> TerminalResult<(), Infallible, SetMeasurementsAndDataError, SetMeasurementsAndDataFailure>
    where
        M::Optical: AsScaleTransform,
    {
        let go = || {
            let meas_n = xs.0.len();
            let data_n = df.ncols();
            if meas_n != data_n {
                return Err(MeasDataMismatchError { meas_n, data_n }).into_mult();
            }
            self.set_measurements_inner(xs, allow_shared_names, skip_index_check)
                .mult_errors_into()?;
            self.data = df;
            Ok(())
        };
        go().mult_terminate(SetMeasurementsAndDataFailure)
    }
}

impl<M: VersionedMetaroot> VersionedCoreTEXT<M> {
    pub(crate) fn try_new(
        metaroot: Metaroot<M>,
        measurements: Eithers<M::Name, Temporal<M::Temporal>, Optical<M::Optical>>,
        layout: <M::Ver as Versioned>::Layout,
    ) -> MultiResult<Self, NewCoreError>
    where
        M::Optical: AsScaleTransform,
    {
        let ms = Measurements::try_new(measurements).into_mult()?;
        let ns: Vec<_> = ms.indexed_names().collect();
        metaroot.check_meas_links(&ns[..]).into_mult()?;
        layout.check_measurement_vector(&ms).mult_errors_into()?;
        Ok(Self::new(metaroot, ms, layout, (), (), ()))
    }

    fn new_unchecked(
        metaroot: Metaroot<M>,
        measurements: Measurements<M::Name, M::Temporal, M::Optical>,
        layout: <M::Ver as Versioned>::Layout,
    ) -> Self {
        Self::new(metaroot, measurements, layout, (), (), ())
    }
}

impl HasCompensation for InnerMetaroot2_0 {
    type Comp = Compensation2_0;

    fn comp_mut(&mut self, _: private::NoTouchy) -> &mut Option<Self::Comp> {
        &mut self.comp.0
    }
}

impl HasCompensation for InnerMetaroot3_0 {
    type Comp = Compensation3_0;

    fn comp_mut(&mut self, _: private::NoTouchy) -> &mut Option<Self::Comp> {
        &mut self.comp.0
    }
}

impl HasSpillover for InnerMetaroot3_1 {
    fn spill_mut(&mut self, _: private::NoTouchy) -> &mut Option<Spillover> {
        &mut self.spillover.0
    }
}

impl HasSpillover for InnerMetaroot3_2 {
    fn spill_mut(&mut self, _: private::NoTouchy) -> &mut Option<Spillover> {
        &mut self.spillover.0
    }
}

impl HasUnstainedCenters for InnerMetaroot3_2 {
    fn unstainedcenters_mut(&mut self, _: private::NoTouchy) -> &mut Option<UnstainedCenters> {
        &mut self.unstained.unstainedcenters.0
    }
}

impl HasScale for InnerOptical2_0 {
    fn scale_mut(&mut self, _: private::NoTouchy) -> &mut Option<Scale> {
        &mut self.scale.0
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
    M: VersionedMetaroot<Name = MaybeFamily>,
{
    /// Set all $PnN keywords to list of names.
    pub fn set_measurement_shortnames_maybe(
        &mut self,
        ns: Vec<Option<Shortname>>,
    ) -> Result<NameMapping, SetKeysError> {
        let ks = ns.into_iter().map(Into::into).collect();
        let mapping = self.measurements.set_keys(ks)?;
        self.metaroot.rename_meas_links(&mapping);
        Ok(mapping)
    }
}

impl CoreTEXT2_0 {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new_2_0(
        measurements: Eithers<MaybeFamily, Temporal<InnerTemporal2_0>, Optical<InnerOptical2_0>>,
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
    ) -> MultiResult<Self, NewCoreTEXTError> {
        let timestamps = Timestamps::try_new(btim, etim, date).into_mult()?;
        let specific =
            InnerMetaroot2_0::new(mode, cyt, comp.map(Into::into), timestamps, applied_gates);
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
        Self::try_new(metaroot, measurements, layout).mult_errors_into()
    }
}

impl CoreTEXT3_0 {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new_3_0(
        measurements: Eithers<MaybeFamily, Temporal<InnerTemporal3_0>, Optical<InnerOptical3_0>>,
        layout: DataLayout3_0,
        mode: Mode,
        cyt: Cyt,
        comp: Option<Compensation>,
        btim: Option<Btim<FCSTime60>>,
        etim: Option<Etim<FCSTime60>>,
        date: Option<FCSDate>,
        cytsn: Cytsn,
        unicode: Option<Unicode>,
        csvbits: Option<CSVBits>,
        cstot: Option<CSTot>,
        csvflags: Option<CSVFlags>,
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
    ) -> MultiResult<Self, NewCoreTEXTError> {
        let timestamps = Timestamps::try_new(btim, etim, date).into_mult()?;
        let subset = SubsetData::new(csvbits, cstot, csvflags);
        let specific = InnerMetaroot3_0::new(
            mode,
            cyt,
            comp.map(Into::into),
            timestamps,
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
        Self::try_new(metaroot, measurements, layout).mult_errors_into()
    }
}

impl CoreTEXT3_1 {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new_3_1(
        measurements: Eithers<AlwaysFamily, Temporal<InnerTemporal3_1>, Optical<InnerOptical3_1>>,
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
        plateid: Option<Plateid>,
        platename: Option<Platename>,
        wellid: Option<Wellid>,
        vol: Option<Vol>,
        csvbits: Option<CSVBits>,
        cstot: Option<CSTot>,
        csvflags: Option<CSVFlags>,
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
    ) -> MultiResult<Self, NewCoreTEXTError> {
        let timestamps = Timestamps::try_new(btim, etim, date).into_mult()?;
        let subset = SubsetData::new(csvbits, cstot, csvflags);
        let specific = InnerMetaroot3_1::new(
            mode,
            cyt,
            timestamps,
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
        Self::try_new(metaroot, measurements, layout).mult_errors_into()
    }
}

impl CoreTEXT3_2 {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new_3_2(
        measurements: Eithers<AlwaysFamily, Temporal<InnerTemporal3_2>, Optical<InnerOptical3_2>>,
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
        plateid: Option<Plateid>,
        platename: Option<Platename>,
        wellid: Option<Wellid>,
        vol: Option<Vol>,
        carrierid: Option<Carrierid>,
        carriertype: Option<Carriertype>,
        locationid: Option<Locationid>,
        unstainedinfo: Option<UnstainedInfo>,
        unstainedcenters: Option<UnstainedCenters>,
        flowrate: Option<Flowrate>,
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
    ) -> MultiResult<Self, NewCoreTEXTError> {
        let timestamps = Timestamps::try_new(btim, etim, date).into_mult()?;
        let datetimes = Datetimes::try_new(begindatetime, enddatetime).into_mult()?;
        let specific = InnerMetaroot3_2::new(
            mode,
            timestamps,
            datetimes,
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
        Self::try_new(metaroot, measurements, layout).mult_errors_into()
    }
}

impl UnstainedData {
    fn lookup(
        kws: &mut StdKeywords,
        names: &HashSet<&Shortname>,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<Self> {
        let c = UnstainedCenters::lookup_opt_linked_st(kws, names, (), conf);
        let i = UnstainedInfo::lookup_metaroot_opt(kws, false, conf);
        c.zip(i)
            .map(|(unstainedcenters, unstainedinfo)| Self::new(unstainedcenters, unstainedinfo))
    }

    fn opt_keywords(&self) -> impl Iterator<Item = (String, String)> {
        [
            self.unstainedcenters.root_kw_pair(),
            self.unstainedinfo.root_kw_pair(),
        ]
        .into_iter()
        .filter_map(|(k, v)| v.map(|x| (k, x)))
    }

    fn check_loss(self, allow_loss: bool) -> BiTentative<(), AnyMetarootKeyLossError> {
        let c = self.unstainedcenters.check_key_transfer(allow_loss);
        let i = self.unstainedinfo.check_key_transfer(allow_loss);
        c.zip(i).void()
    }
}

impl SubsetData {
    fn lookup(kws: &mut StdKeywords, conf: &StdTextReadConfig) -> LookupTentative<Self> {
        let f = CSVFlags::lookup(kws, conf);
        let b = CSVBits::lookup_metaroot_opt(kws, false, conf);
        let t = CSTot::lookup_metaroot_opt(kws, false, conf);
        f.zip3(b, t)
            .map(|(flags, bits, tot)| Self::new(bits, tot, flags))
    }

    fn opt_keywords(&self) -> impl Iterator<Item = (String, String)> {
        [self.bits.root_kw_pair(), self.tot.root_kw_pair()]
            .into_iter()
            .filter_map(|(k, v)| v.map(|x| (k, x)))
            .chain(
                self.flags
                    .0
                    .as_ref()
                    .into_iter()
                    .flat_map(CSVFlags::opt_keywords),
            )
    }

    fn check_loss(self, allow_loss: bool) -> BiTentative<(), AnyMetarootKeyLossError> {
        self.bits
            .check_key_transfer(allow_loss)
            .and_tentatively(|()| {
                self.flags
                    .0
                    .map_or(Tentative::default(), |flags| flags.check_loss(allow_loss))
            })
    }
}

impl CSVFlags {
    fn try_from_iter(
        value: impl IntoIterator<Item = Option<u32>>,
    ) -> Result<Self, NewCSVFlagsError> {
        NonEmpty::collect(value.into_iter().map(|x| x.map(CSVFlag).into()))
            .ok_or(NewCSVFlagsError)
            .map(|xs| Self(xs.into()))
    }

    // TODO technically these should be marked deprecated because they were
    // taken out in 3.2, but the standards don't say so
    fn lookup(kws: &mut StdKeywords, conf: &StdTextReadConfig) -> LookupOptional<Self> {
        CSMode::lookup_metaroot_opt(kws, false, conf)
            .and_tentatively(|m| {
                if let Some(n) = m.0 {
                    let fs = (0..n.0).map(|i| CSVFlag::lookup_meas_opt(kws, i, false, conf));
                    Tentative::mconcat(fs).and_tentatively(|flags| {
                        let xs = flags.into_iter().map(|x| x.0.map(|y| y.0));
                        Self::try_from_iter(xs)
                            .into_tentative_opt(!conf.allow_optional_dropping)
                            .inner_into()
                    })
                } else {
                    Tentative::default()
                }
            })
            .value_into()
    }

    fn opt_keywords(&self) -> impl Iterator<Item = (String, String)> {
        let m = CSMode((self.0).0.len());
        (self.0)
            .0
            .iter()
            .enumerate()
            .map(|(i, f)| f.meas_kw_pair(i))
            .filter_map(|(k, v)| v.map(|x| (k, x)))
            .chain([m.metaroot_pair()])
    }

    fn check_loss(self, allow_loss: bool) -> BiTentative<(), AnyMetarootKeyLossError> {
        let xs = (self.0)
            .0
            .into_iter()
            .enumerate()
            .map(|(i, f)| f.check_indexed_key_transfer_own(i, allow_loss));
        let mut tnt = Tentative::mconcat(xs).void();
        tnt.push_error_or_warning(UnitaryKeyLossError::<CSMode>::new(), allow_loss);
        tnt
    }
}

impl ModificationData {
    fn lookup(kws: &mut StdKeywords, conf: &StdTextReadConfig) -> LookupTentative<Self> {
        let last_mod = LastModifier::lookup_metaroot_opt(kws, false, conf);
        let last_mod_date = LastModified::lookup_metaroot_opt(kws, false, conf);
        let ori = Originality::lookup_metaroot_opt(kws, false, conf);
        last_mod
            .zip3(last_mod_date, ori)
            .map(|(last_modifier, last_modified, originality)| {
                Self::new(last_modifier, last_modified, originality)
            })
    }

    fn opt_keywords(&self) -> impl Iterator<Item = (String, String)> {
        [
            self.last_modifier.metaroot_opt_pair(),
            self.last_modified.root_kw_pair(),
            self.originality.root_kw_pair(),
        ]
        .into_iter()
        .filter_map(|(k, v)| v.map(|x| (k, x)))
    }

    fn check_loss(self, allow_loss: bool) -> BiTentative<(), AnyMetarootKeyLossError> {
        let d = self.last_modified.check_key_transfer(allow_loss);
        let r = self.last_modifier.check_key_transfer(allow_loss);
        let o = self.originality.check_key_transfer(allow_loss);
        d.zip3(r, o).void()
    }
}

impl CarrierData {
    fn lookup(kws: &mut StdKeywords, conf: &StdTextReadConfig) -> LookupTentative<Self> {
        let l = Locationid::lookup_metaroot_opt(kws, false, conf);
        let i = Carrierid::lookup_metaroot_opt(kws, false, conf);
        let t = Carriertype::lookup_metaroot_opt(kws, false, conf);
        l.zip3(i, t).map(|(locationid, carrierid, carriertype)| {
            Self::new(carrierid, carriertype, locationid)
        })
    }

    fn opt_keywords(&self) -> impl Iterator<Item = (String, String)> {
        [
            self.carrierid.root_kw_pair(),
            self.carriertype.root_kw_pair(),
            self.locationid.root_kw_pair(),
        ]
        .into_iter()
        .filter_map(|(k, v)| v.map(|x| (k, x)))
    }

    fn check_loss(self, allow_loss: bool) -> BiTentative<(), AnyMetarootKeyLossError> {
        let i = self.carrierid.check_key_transfer(allow_loss);
        let t = self.carriertype.check_key_transfer(allow_loss);
        let l = self.locationid.check_key_transfer(allow_loss);
        i.zip3(t, l).void()
    }
}

impl PlateData {
    fn lookup(
        kws: &mut StdKeywords,
        is_deprecated: bool,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<Self> {
        let w = Wellid::lookup_metaroot_opt(kws, is_deprecated, conf);
        let n = Platename::lookup_metaroot_opt(kws, is_deprecated, conf);
        let i = Plateid::lookup_metaroot_opt(kws, is_deprecated, conf);
        w.zip3(n, i)
            .map(|(wellid, platename, plateid)| Self::new(plateid, platename, wellid))
    }

    fn opt_keywords(&self) -> impl Iterator<Item = (String, String)> {
        [
            self.wellid.root_kw_pair(),
            self.platename.root_kw_pair(),
            self.plateid.root_kw_pair(),
        ]
        .into_iter()
        .filter_map(|(k, v)| v.map(|x| (k, x)))
    }

    fn check_loss(self, allow_loss: bool) -> BiTentative<(), AnyMetarootKeyLossError> {
        let n = self.platename.check_key_transfer(allow_loss);
        let i = self.plateid.check_key_transfer(allow_loss);
        let w = self.wellid.check_key_transfer(allow_loss);
        n.zip3(i, w).void()
    }
}

impl PeakData {
    fn lookup(
        kws: &mut StdKeywords,
        i: MeasIndex,
        is_deprecated: bool,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<Self> {
        let b = PeakBin::lookup_meas_opt(kws, i, is_deprecated, conf);
        let s = PeakNumber::lookup_meas_opt(kws, i, is_deprecated, conf);
        b.zip(s).map(|(bin, size)| Self::new(bin, size))
    }

    pub(crate) fn opt_keywords(
        &self,
        i: MeasIndex,
    ) -> impl Iterator<Item = (MeasHeader, String, Option<String>)> {
        [self.bin.meas_kw_triple(i), self.size.meas_kw_triple(i)].into_iter()
    }

    fn check_loss(self, i: MeasIndex, allow_loss: bool) -> BiTentative<(), AnyMeasKeyLossError> {
        let b = self.bin.check_indexed_key_transfer_own(i, allow_loss);
        let s = self.size.check_indexed_key_transfer_own(i, allow_loss);
        b.zip(s).void()
    }
}

impl From<FCSTime> for FCSTime60 {
    fn from(value: FCSTime) -> Self {
        Self(value.0)
    }
}

impl From<FCSTime> for FCSTime100 {
    fn from(value: FCSTime) -> Self {
        Self(value.0)
    }
}

impl From<FCSTime60> for FCSTime {
    fn from(value: FCSTime60) -> Self {
        // ASSUME this will never fail, we are just removing nanoseconds
        Self(value.0.with_nanosecond(0).unwrap())
    }
}

impl From<FCSTime100> for FCSTime {
    fn from(value: FCSTime100) -> Self {
        // ASSUME this will never fail, we are just removing nanoseconds
        Self(value.0.with_nanosecond(0).unwrap())
    }
}

impl From<FCSTime60> for FCSTime100 {
    fn from(value: FCSTime60) -> Self {
        Self(value.0)
    }
}

impl From<FCSTime100> for FCSTime60 {
    fn from(value: FCSTime100) -> Self {
        Self(value.0)
    }
}

impl From<Wavelength> for Wavelengths {
    fn from(value: Wavelength) -> Self {
        Self(FCSNonEmpty::new(value.0))
    }
}

impl From<Calibration3_1> for Calibration3_2 {
    fn from(value: Calibration3_1) -> Self {
        Self {
            unit: value.unit,
            offset: 0.0,
            slope: value.slope,
        }
    }
}

impl From<Calibration3_2> for Calibration3_1 {
    fn from(value: Calibration3_2) -> Self {
        Self {
            unit: value.unit,
            slope: value.slope,
        }
    }
}

// TODO this is awkward
fn convert_wavelengths(
    w: MaybeValue<Wavelengths>,
    allow_loss: bool,
) -> BiTentative<MaybeValue<Wavelength>, WavelengthsLossError> {
    w.0.map(|x| x.into_wavelength(allow_loss))
        .map_or(Tentative::new1(None), |tnt| tnt.map(Some))
        .value_into()
}

impl ConvertFromOptical<InnerOptical3_0> for InnerOptical2_0 {
    fn convert_from_optical(
        value: InnerOptical3_0,
        i: MeasIndex,
        allow_loss: bool,
    ) -> OpticalConvertResult<Self> {
        let out = ScaleTransform::try_convert_to_scale(value.scale, i, allow_loss)
            .inner_into()
            .map(|scale| Self::new(Some(scale), value.wavelength, value.peak));
        Ok(out)
    }
}

impl ConvertFromOptical<InnerOptical3_1> for InnerOptical2_0 {
    fn convert_from_optical(
        value: InnerOptical3_1,
        i: MeasIndex,
        allow_loss: bool,
    ) -> OpticalConvertResult<Self> {
        let xform = ScaleTransform::try_convert_to_scale(value.scale, i, allow_loss);
        let cal = value
            .calibration
            .check_indexed_key_transfer_own(i, allow_loss);
        let dpy = value.display.check_indexed_key_transfer_own(i, allow_loss);
        let wave = convert_wavelengths(value.wavelengths, allow_loss).inner_into();
        let out = xform
            .zip3(cal, dpy)
            .inner_into()
            .zip(wave)
            .map(|((scale, (), ()), wavelength)| Self::new(Some(scale), wavelength, value.peak));
        Ok(out)
    }
}

impl ConvertFromOptical<InnerOptical3_2> for InnerOptical2_0 {
    fn convert_from_optical(
        value: InnerOptical3_2,
        i: MeasIndex,
        allow_loss: bool,
    ) -> OpticalConvertResult<Self> {
        let xform = ScaleTransform::try_convert_to_scale(value.scale, i, allow_loss);
        let cal = value
            .calibration
            .check_indexed_key_transfer_own(i, allow_loss);
        let dpy = value.display.check_indexed_key_transfer_own(i, allow_loss);
        let anal = value.analyte.check_indexed_key_transfer_own(i, allow_loss);
        let feat = value.feature.check_indexed_key_transfer_own(i, allow_loss);
        let meas = value
            .measurement_type
            .check_indexed_key_transfer_own(i, allow_loss);
        let tag = value.tag.check_indexed_key_transfer_own(i, allow_loss);
        let det_name = value
            .detector_name
            .check_indexed_key_transfer_own(i, allow_loss);
        let w = convert_wavelengths(value.wavelengths, allow_loss).inner_into();
        let out = det_name
            .zip6(cal, dpy, anal, feat, meas)
            .zip3(tag, xform)
            .inner_into()
            .zip(w)
            .map(|((_, (), scale), wavelength)| {
                Self::new(Some(scale), wavelength, PeakData::default())
            });
        Ok(out)
    }
}

impl ConvertFromOptical<InnerOptical2_0> for InnerOptical3_0 {
    fn convert_from_optical(
        value: InnerOptical2_0,
        i: MeasIndex,
        _: bool,
    ) -> OpticalConvertResult<Self> {
        value
            .scale
            .0
            .ok_or(NoScaleError(i))
            .map(|s| Self::new(s, value.wavelength, value.peak))
            .into_deferred()
    }
}

impl ConvertFromOptical<InnerOptical3_1> for InnerOptical3_0 {
    fn convert_from_optical(
        value: InnerOptical3_1,
        i: MeasIndex,
        allow_loss: bool,
    ) -> OpticalConvertResult<Self> {
        let cal = value
            .calibration
            .check_indexed_key_transfer_own::<AnyMeasKeyLossError>(i, allow_loss);
        let dpy = value.display.check_indexed_key_transfer_own(i, allow_loss);
        let wave = convert_wavelengths(value.wavelengths, allow_loss).inner_into();
        let out = cal
            .zip(dpy)
            .inner_into()
            .zip(wave)
            .map(|(_, wavelength)| Self::new(value.scale, wavelength, value.peak));
        Ok(out)
    }
}

impl ConvertFromOptical<InnerOptical3_2> for InnerOptical3_0 {
    fn convert_from_optical(
        value: InnerOptical3_2,
        i: MeasIndex,
        allow_loss: bool,
    ) -> OpticalConvertResult<Self> {
        let cal = value
            .calibration
            .check_indexed_key_transfer_own::<AnyMeasKeyLossError>(i, allow_loss);
        let dpy = value.display.check_indexed_key_transfer_own(i, allow_loss);
        let anal = value.analyte.check_indexed_key_transfer_own(i, allow_loss);
        let feat = value.feature.check_indexed_key_transfer_own(i, allow_loss);
        let meas = value
            .measurement_type
            .check_indexed_key_transfer_own(i, allow_loss);
        let tag = value.tag.check_indexed_key_transfer_own(i, allow_loss);
        let det_name = value
            .detector_name
            .check_indexed_key_transfer_own(i, allow_loss);
        let wave = convert_wavelengths(value.wavelengths, allow_loss).inner_into();
        let out = cal
            .zip5(dpy, anal, feat, meas)
            .zip3(tag, det_name)
            .inner_into()
            .zip(wave)
            .map(|(_, wavelength)| Self::new(value.scale, wavelength, PeakData::default()));
        Ok(out)
    }
}

impl ConvertFromOptical<InnerOptical2_0> for InnerOptical3_1 {
    fn convert_from_optical(
        value: InnerOptical2_0,
        i: MeasIndex,
        _: bool,
    ) -> OpticalConvertResult<Self> {
        value
            .scale
            .0
            .ok_or(NoScaleError(i))
            .map(|s| Self::new(s, value.wavelength.map(Into::into), None, None, value.peak))
            .into_deferred()
    }
}

impl ConvertFromOptical<InnerOptical3_0> for InnerOptical3_1 {
    fn convert_from_optical(
        value: InnerOptical3_0,
        _: MeasIndex,
        _: bool,
    ) -> OpticalConvertResult<Self> {
        Ok(Tentative::new1(Self::new(
            value.scale,
            value.wavelength.map(Into::into),
            None,
            None,
            value.peak,
        )))
    }
}

impl ConvertFromOptical<InnerOptical3_2> for InnerOptical3_1 {
    fn convert_from_optical(
        value: InnerOptical3_2,
        i: MeasIndex,
        allow_loss: bool,
    ) -> OpticalConvertResult<Self> {
        let anal = value
            .analyte
            .check_indexed_key_transfer_own::<AnyMeasKeyLossError>(i, allow_loss);
        let feat = value.feature.check_indexed_key_transfer_own(i, allow_loss);
        let meas = value
            .measurement_type
            .check_indexed_key_transfer_own(i, allow_loss);
        let tag = value.tag.check_indexed_key_transfer_own(i, allow_loss);
        let det_name = value
            .detector_name
            .check_indexed_key_transfer_own(i, allow_loss);
        let out = anal
            .zip3(feat, meas)
            .zip3(tag, det_name)
            .inner_into()
            .map(|_| {
                Self::new(
                    value.scale,
                    value.wavelengths,
                    // TODO warn offset might be lost here
                    value.calibration.map(Into::into),
                    value.display,
                    PeakData::default(),
                )
            });
        Ok(out)
    }
}

impl ConvertFromOptical<InnerOptical2_0> for InnerOptical3_2 {
    fn convert_from_optical(
        value: InnerOptical2_0,
        i: MeasIndex,
        allow_loss: bool,
    ) -> OpticalConvertResult<Self> {
        value
            .peak
            .check_loss(i, allow_loss)
            .inner_into()
            .and_maybe(|()| {
                value
                    .scale
                    .0
                    .ok_or(NoScaleError(i))
                    .map(|s| {
                        Self::new(
                            s,
                            value.wavelength.map(Into::into),
                            None,
                            None,
                            Analyte::default(),
                            None,
                            None,
                            Tag::default(),
                            DetectorName::default(),
                        )
                    })
                    .into_deferred()
            })
    }
}

impl ConvertFromOptical<InnerOptical3_0> for InnerOptical3_2 {
    fn convert_from_optical(
        value: InnerOptical3_0,
        i: MeasIndex,
        allow_loss: bool,
    ) -> OpticalConvertResult<Self> {
        let out = value.peak.check_loss(i, allow_loss).inner_into().map(|()| {
            Self::new(
                value.scale,
                value.wavelength.map(Into::into),
                None,
                None,
                Analyte::default(),
                None,
                None,
                Tag::default(),
                DetectorName::default(),
            )
        });
        Ok(out)
    }
}

impl ConvertFromOptical<InnerOptical3_1> for InnerOptical3_2 {
    fn convert_from_optical(
        value: InnerOptical3_1,
        i: MeasIndex,
        allow_loss: bool,
    ) -> OpticalConvertResult<Self> {
        let out = value.peak.check_loss(i, allow_loss).inner_into().map(|()| {
            Self::new(
                value.scale,
                value.wavelengths,
                value.calibration.map(Into::into),
                value.display,
                Analyte::default(),
                None,
                None,
                Tag::default(),
                DetectorName::default(),
            )
        });
        Ok(out)
    }
}

type MetarootConvertResult<M> = DeferredResult<M, MetarootConvertWarning, MetarootConvertError>;

type OpticalConvertResult<M> = DeferredResult<M, OpticalConvertWarning, OpticalConvertError>;

type TemporalConvertTentative<M> = BiTentative<M, TemporalConvertError>;

pub(crate) type LayoutConvertResult<L> = MultiResult<L, LayoutConvertError>;

#[derive(From, Display, Debug, Error)]
pub enum OpticalConvertError {
    NoScale(NoScaleError),
    Wavelengths(WavelengthsLossError),
    Xfer(AnyMeasKeyLossError),
}

#[derive(From, Display, Debug, Error)]
pub enum OpticalConvertWarning {
    Wavelengths(WavelengthsLossError),
    Xfer(AnyMeasKeyLossError),
}

#[derive(From, Display, Debug, Error)]
pub enum TemporalConvertError {
    Timestep(TimestepLossError),
    Xfer(AnyMeasKeyLossError),
}

#[derive(From, Display, Debug, Error)]
pub enum LayoutConvertError {
    OrderToEndian(OrderedToEndianError),
    Width(ConvertWidthError),
    MixedToOrdered(MixedToOrderedLayoutError),
    MixedToNonMixed(MixedToNonMixedLayoutError),
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
    Option<CSVBits>,
    Option<CSTot>,
    Option<CSVFlags>,
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
    Option<Plateid>,
    Option<Wellid>,
    Option<Platename>,
    Option<Vol>,
    Option<CSVBits>,
    Option<CSTot>,
    Option<CSVFlags>,
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
    Option<Plateid>,
    Option<Wellid>,
    Option<Platename>,
    Option<Carrierid>,
    Option<Carriertype>,
    Option<Locationid>,
    Option<Vol>,
    Option<Flowrate>,
    Option<UnstainedInfo>,
    Timestamps3_1
);

impl_ref_specific_rw!(
    Optical,
    InnerOptical2_0,
    Option<Wavelength>,
    Option<PeakBin>,
    Option<PeakNumber>
);

impl_ref_specific_rw!(
    Optical,
    InnerOptical3_0,
    Option<Wavelength>,
    Option<PeakBin>,
    Option<PeakNumber>
);

impl_ref_specific_rw!(
    Optical,
    InnerOptical3_1,
    Option<Wavelengths>,
    Option<PeakBin>,
    Option<PeakNumber>,
    Option<Calibration3_1>,
    Option<Display>
);

impl_ref_specific_rw!(
    Optical,
    InnerOptical3_2,
    Option<Wavelengths>,
    Option<Calibration3_2>,
    Option<Display>,
    Analyte,
    Option<Feature>,
    Option<OpticalType>,
    Tag,
    DetectorName
);

impl_ref_specific_rw!(
    Temporal,
    InnerTemporal2_0,
    Option<PeakBin>,
    Option<PeakNumber>
);

impl_ref_specific_rw!(
    Temporal,
    InnerTemporal3_0,
    Timestep,
    Option<PeakBin>,
    Option<PeakNumber>
);

impl_ref_specific_rw!(
    Temporal,
    InnerTemporal3_1,
    Timestep,
    Option<Display>,
    Option<PeakBin>,
    Option<PeakNumber>
);

impl_ref_specific_rw!(
    Temporal,
    InnerTemporal3_2,
    Timestep,
    Option<Display>,
    Option<TemporalType>
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
    Option<UnstainedCenters>,
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

impl ConvertFromMetaroot<InnerMetaroot3_0> for InnerMetaroot2_0 {
    fn convert_from_metaroot(
        value: InnerMetaroot3_0,
        allow_loss: bool,
    ) -> MetarootConvertResult<Self> {
        let c = value.cytsn.check_key_transfer(allow_loss);
        let u = value.unicode.check_key_transfer(allow_loss);
        let s = value.subset.check_loss(allow_loss);
        let ret = c.zip3(u, s).inner_into().and_tentatively(|_| {
            value
                .applied_gates
                .try_into_2_0(allow_loss)
                .def_unfail_default()
                .inner_into()
                .map(|ag| {
                    Self::new(
                        value.mode,
                        value.cyt,
                        value.comp.map(|x| x.0.into()),
                        value.timestamps.map(Into::into),
                        ag,
                    )
                })
        });
        Ok(ret)
    }
}

impl ConvertFromMetaroot<InnerMetaroot3_1> for InnerMetaroot2_0 {
    fn convert_from_metaroot(
        value: InnerMetaroot3_1,
        allow_loss: bool,
    ) -> MetarootConvertResult<Self> {
        let cytsn = value.cytsn.check_key_transfer(allow_loss);
        let vol = value.vol.check_key_transfer(allow_loss);
        let spill = value.spillover.check_key_transfer(allow_loss);
        let modi = value.modification.check_loss(allow_loss);
        let plate = value.plate.check_loss(allow_loss);
        let subset = value.subset.check_loss(allow_loss);
        let out = cytsn
            .zip6(vol, spill, modi, plate, subset)
            .inner_into()
            .and_tentatively(|_| {
                value
                    .applied_gates
                    .try_into_2_0(allow_loss)
                    .def_unfail_default()
                    .inner_into()
                    .map(|applied_gates| {
                        Self::new(
                            value.mode,
                            value.cyt,
                            None,
                            value.timestamps.map(Into::into),
                            applied_gates,
                        )
                    })
            });
        Ok(out)
    }
}

impl ConvertFromMetaroot<InnerMetaroot3_2> for InnerMetaroot2_0 {
    fn convert_from_metaroot(
        value: InnerMetaroot3_2,
        allow_loss: bool,
    ) -> MetarootConvertResult<Self> {
        let cytsn = value.cytsn.check_key_transfer(allow_loss);
        let vol = value.vol.check_key_transfer(allow_loss);
        let spill = value.spillover.check_key_transfer(allow_loss);
        let flow = value.flowrate.check_key_transfer(allow_loss);
        let modi = value.modification.check_loss(allow_loss);
        let plate = value.plate.check_loss(allow_loss);
        let dt = value.datetimes.check_loss(allow_loss);
        let carrier = value.carrier.check_loss(allow_loss);
        let us = value.unstained.check_loss(allow_loss);
        let mut ret = cytsn
            .zip6(vol, spill, flow, modi, plate)
            .zip4(dt, carrier, us)
            .inner_into()
            .map(|_| {
                Self::new(
                    Mode::List,
                    value.cyt,
                    None,
                    value.timestamps.map(Into::into),
                    AppliedGates2_0::default(),
                )
            });
        if !value.applied_gates.is_empty() {
            ret.push_error_or_warning(gating::AppliedGates3_2To2_0Error, allow_loss);
        }
        Ok(ret)
    }
}

impl ConvertFromMetaroot<InnerMetaroot2_0> for InnerMetaroot3_0 {
    fn convert_from_metaroot(value: InnerMetaroot2_0, _: bool) -> MetarootConvertResult<Self> {
        Ok(Tentative::new1(Self::new(
            value.mode,
            value.cyt,
            value.comp.map(|x| x.0.into()),
            value.timestamps.map(Into::into),
            Cytsn::default(),
            None,
            SubsetData::default(),
            value.applied_gates,
        )))
    }
}

impl ConvertFromMetaroot<InnerMetaroot3_1> for InnerMetaroot3_0 {
    fn convert_from_metaroot(
        value: InnerMetaroot3_1,
        allow_loss: bool,
    ) -> MetarootConvertResult<Self> {
        let plate = value.plate.check_loss(allow_loss);
        let modi = value.modification.check_loss(allow_loss);
        let vol = value.vol.check_key_transfer(allow_loss);
        let out = plate.zip3(modi, vol).inner_into().map(|_| {
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
        });
        Ok(out)
    }
}

impl ConvertFromMetaroot<InnerMetaroot3_2> for InnerMetaroot3_0 {
    fn convert_from_metaroot(
        value: InnerMetaroot3_2,
        allow_loss: bool,
    ) -> MetarootConvertResult<Self> {
        let vol = value.vol.check_key_transfer(allow_loss);
        let flow = value.flowrate.check_key_transfer(allow_loss);
        let modi = value.modification.check_loss(allow_loss);
        let plate = value.plate.check_loss(allow_loss);
        let dt = value.datetimes.check_loss(allow_loss);
        let carrier = value.carrier.check_loss(allow_loss);
        let us = value.unstained.check_loss(allow_loss);
        let out = vol
            .zip6(flow, modi, plate, dt, carrier)
            .zip(us)
            .inner_into()
            .map(|_| {
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
            });
        Ok(out)
    }
}

impl ConvertFromMetaroot<InnerMetaroot2_0> for InnerMetaroot3_1 {
    fn convert_from_metaroot(
        value: InnerMetaroot2_0,
        allow_loss: bool,
    ) -> MetarootConvertResult<Self> {
        let mut out = Tentative::new1(Self::new(
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
        ));
        if value.comp.0.is_some() {
            out.push_error_or_warning(Comp2_0TransferError, allow_loss);
        }
        Ok(out)
    }
}

impl ConvertFromMetaroot<InnerMetaroot3_0> for InnerMetaroot3_1 {
    fn convert_from_metaroot(
        value: InnerMetaroot3_0,
        allow_loss: bool,
    ) -> MetarootConvertResult<Self> {
        let comp = value.comp.check_key_transfer(allow_loss);
        let us = value.unicode.check_key_transfer(allow_loss);
        let out = comp.zip(us).inner_into().map(|_| {
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
        });
        Ok(out)
    }
}

impl ConvertFromMetaroot<InnerMetaroot3_2> for InnerMetaroot3_1 {
    fn convert_from_metaroot(
        value: InnerMetaroot3_2,
        allow_loss: bool,
    ) -> MetarootConvertResult<Self> {
        let dt = value.datetimes.check_loss(allow_loss);
        let carrier = value.carrier.check_loss(allow_loss);
        let us = value.unstained.check_loss(allow_loss);
        let flow = value.flowrate.check_key_transfer(allow_loss);
        let ret = dt.zip4(carrier, us, flow).inner_into().map(|_| {
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
        });
        Ok(ret)
    }
}

impl ConvertFromMetaroot<InnerMetaroot2_0> for InnerMetaroot3_2 {
    fn convert_from_metaroot(
        value: InnerMetaroot2_0,
        allow_loss: bool,
    ) -> MetarootConvertResult<Self> {
        let mut res = value
            .cyt
            .try_into()
            .into_deferred()
            .def_and_tentatively(|cyt| {
                Mode3_2::try_from(value.mode)
                    .into_tentative_opt(allow_loss)
                    .inner_into()
                    .map(|mode| {
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
                            None,
                            AppliedGates3_2::default(),
                        )
                    })
            });
        if !value.applied_gates.is_empty() {
            res.def_push_error_or_warning(gating::AppliedGates2_0To3_2Error, allow_loss);
        }
        if value.comp.0.is_some() {
            res.def_push_error_or_warning(Comp2_0TransferError, allow_loss);
        }
        res
    }
}

impl ConvertFromMetaroot<InnerMetaroot3_0> for InnerMetaroot3_2 {
    fn convert_from_metaroot(
        value: InnerMetaroot3_0,
        allow_loss: bool,
    ) -> MetarootConvertResult<Self> {
        let uni = value.unicode.check_key_transfer(allow_loss);
        let comp = value.comp.check_key_transfer(allow_loss);
        let subset = value.subset.check_loss(allow_loss);
        uni.zip3(comp, subset).inner_into().and_maybe(|_| {
            value
                .applied_gates
                .try_into_3_2(allow_loss)
                .def_unfail_default()
                .inner_into()
                .and_maybe(|applied_gates| {
                    value
                        .cyt
                        .try_into()
                        .into_deferred()
                        .def_and_tentatively(|cyt| {
                            Mode3_2::try_from(value.mode)
                                .into_tentative_opt(allow_loss)
                                .inner_into()
                                .map(|mode| {
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
                                        None,
                                        applied_gates,
                                    )
                                })
                        })
                })
        })
    }
}

impl ConvertFromMetaroot<InnerMetaroot3_1> for InnerMetaroot3_2 {
    fn convert_from_metaroot(
        value: InnerMetaroot3_1,
        allow_loss: bool,
    ) -> MetarootConvertResult<Self> {
        let ss = value.subset.check_loss(allow_loss).inner_into();
        let a = value
            .applied_gates
            .try_into_3_2(allow_loss)
            .def_unfail_default()
            .inner_into();
        ss.zip(a).and_maybe(|((), applied_gates)| {
            value
                .cyt
                .try_into()
                .into_deferred()
                .def_and_tentatively(|cyt| {
                    Mode3_2::try_from(value.mode)
                        .into_tentative_opt(allow_loss)
                        .inner_into()
                        .map(|mode| {
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
                                None,
                                applied_gates,
                            )
                        })
                })
        })
    }
}

impl ScaleTransform {
    /// Convert to a simple scale value (just $PnE, no $PnG).
    ///
    /// This may be lossy because the $PnG value cannot be represented with
    /// just a `Scale` object, and thus is needs to be dropped if present and
    /// not equal to 1.0.
    fn try_convert_to_scale(
        self,
        i: MeasIndex,
        allow_loss: bool,
    ) -> BiTentative<Scale, AnyMeasKeyLossError> {
        match self {
            Self::Lin(x) => {
                let mut ret = Tentative::new1(Scale::Linear);
                if !x.is_one() {
                    ret.push_error_or_warning(IndexedKeyLossError::<Gain>::new(i), !allow_loss);
                }
                ret
            }
            Self::Log(x) => Tentative::new1(Scale::Log(x)),
        }
    }

    fn lookup(kws: &mut StdKeywords, i: MeasIndex, conf: &StdTextReadConfig) -> LookupResult<Self> {
        Gain::lookup_meas_opt(kws, i, false, conf)
            .errors_into()
            .and_maybe(|g| {
                Scale::lookup_req_st(kws, i, (), conf)
                    .def_and_maybe(|s| Self::try_from((s, g)).into_deferred())
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
        let (_, gain): (_, MaybeValue<Gain>) = (*self).into();
        [gain.meas_kw_triple(i)].into_iter()
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

impl From<ScaleTransform> for (Scale, MaybeValue<Gain>) {
    fn from(value: ScaleTransform) -> Self {
        match value {
            ScaleTransform::Lin(g) => (Scale::Linear, Some(Gain(g)).into()),
            ScaleTransform::Log(l) => (Scale::Log(l), None.into()),
        }
    }
}

impl TryFrom<(Scale, MaybeValue<Gain>)> for ScaleTransform {
    type Error = ScaleTransformError;

    /// Convert values for $PnE and $PnG to a scale transform (3.0+)
    ///
    /// If scale is linear, return a linear transform with slope equal to $PnG
    /// or 1.0 if $PnG not given.
    ///
    /// If scale is log, return a log transform with the parameters in $PnE.
    /// Return error if $PnG is given and not 1.0.
    fn try_from(value: (Scale, MaybeValue<Gain>)) -> Result<Self, Self::Error> {
        let scale = value.0;
        let gain = (value.1).0;
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
        allow_loss: bool,
    ) -> TemporalConvertTentative<Self> {
        value
            .timestep
            .check_conversion(allow_loss)
            .map(|()| Self::new(Some(TemporalScale), value.peak))
            .inner_into()
    }
}

impl ConvertFromTemporal<InnerTemporal3_1> for InnerTemporal2_0 {
    fn convert_from_temporal(
        value: InnerTemporal3_1,
        i: MeasIndex,
        allow_loss: bool,
    ) -> TemporalConvertTentative<Self> {
        let t = value.timestep.check_conversion(allow_loss).inner_into();
        let d = value
            .display
            .check_indexed_key_transfer_own::<AnyMeasKeyLossError>(i, allow_loss)
            .inner_into();
        t.zip(d).map(|_| Self::new(Some(TemporalScale), value.peak))
    }
}

impl ConvertFromTemporal<InnerTemporal3_2> for InnerTemporal2_0 {
    fn convert_from_temporal(
        value: InnerTemporal3_2,
        i: MeasIndex,
        allow_loss: bool,
    ) -> TemporalConvertTentative<Self> {
        let di = value
            .display
            .check_indexed_key_transfer_own::<AnyMeasKeyLossError>(i, allow_loss);
        let m = value
            .measurement_type
            .check_indexed_key_transfer_own(i, allow_loss);
        let t = value.timestep.check_conversion(allow_loss).inner_into();
        di.zip(m)
            .inner_into()
            .zip(t)
            .map(|_| Self::new(Some(TemporalScale), PeakData::default()))
    }
}

impl ConvertFromTemporal<InnerTemporal2_0> for InnerTemporal3_0 {
    fn convert_from_temporal(
        value: InnerTemporal2_0,
        _: MeasIndex,
        _: bool,
    ) -> TemporalConvertTentative<Self> {
        Tentative::new1(Self::new(Timestep::default(), value.peak))
    }
}

impl ConvertFromTemporal<InnerTemporal3_1> for InnerTemporal3_0 {
    fn convert_from_temporal(
        value: InnerTemporal3_1,
        i: MeasIndex,
        allow_loss: bool,
    ) -> TemporalConvertTentative<Self> {
        value
            .display
            .check_indexed_key_transfer_own::<AnyMeasKeyLossError>(i, allow_loss)
            .inner_into()
            .map(|()| Self::new(value.timestep, value.peak))
    }
}

impl ConvertFromTemporal<InnerTemporal3_2> for InnerTemporal3_0 {
    fn convert_from_temporal(
        value: InnerTemporal3_2,
        i: MeasIndex,
        allow_loss: bool,
    ) -> TemporalConvertTentative<Self> {
        let di = value
            .display
            .check_indexed_key_transfer_own::<AnyMeasKeyLossError>(i, allow_loss);
        let m = value
            .measurement_type
            .check_indexed_key_transfer_own(i, allow_loss);
        di.zip(m)
            .inner_into()
            .map(|_| Self::new(value.timestep, PeakData::default()))
    }
}

impl ConvertFromTemporal<InnerTemporal2_0> for InnerTemporal3_1 {
    fn convert_from_temporal(
        value: InnerTemporal2_0,
        _: MeasIndex,
        _: bool,
    ) -> TemporalConvertTentative<Self> {
        Tentative::new1(Self::new(Timestep::default(), None, value.peak))
    }
}

impl ConvertFromTemporal<InnerTemporal3_0> for InnerTemporal3_1 {
    fn convert_from_temporal(
        value: InnerTemporal3_0,
        _: MeasIndex,
        _: bool,
    ) -> TemporalConvertTentative<Self> {
        Tentative::new1(Self::new(value.timestep, None, value.peak))
    }
}

impl ConvertFromTemporal<InnerTemporal3_2> for InnerTemporal3_1 {
    fn convert_from_temporal(
        value: InnerTemporal3_2,
        i: MeasIndex,
        allow_loss: bool,
    ) -> TemporalConvertTentative<Self> {
        value
            .measurement_type
            .check_indexed_key_transfer_own::<AnyMeasKeyLossError>(i, allow_loss)
            .inner_into()
            .map(|()| Self::new(value.timestep, value.display, PeakData::default()))
    }
}

impl ConvertFromTemporal<InnerTemporal2_0> for InnerTemporal3_2 {
    fn convert_from_temporal(
        value: InnerTemporal2_0,
        i: MeasIndex,
        allow_loss: bool,
    ) -> TemporalConvertTentative<Self> {
        value
            .peak
            .check_loss(i, allow_loss)
            .inner_into()
            .map(|()| Self::new(Timestep::default(), None, None))
    }
}

impl ConvertFromTemporal<InnerTemporal3_0> for InnerTemporal3_2 {
    fn convert_from_temporal(
        value: InnerTemporal3_0,
        i: MeasIndex,
        allow_loss: bool,
    ) -> TemporalConvertTentative<Self> {
        value
            .peak
            .check_loss(i, allow_loss)
            .inner_into()
            .map(|()| Self::new(value.timestep, None, None))
    }
}

impl ConvertFromTemporal<InnerTemporal3_1> for InnerTemporal3_2 {
    fn convert_from_temporal(
        value: InnerTemporal3_1,
        i: MeasIndex,
        allow_loss: bool,
    ) -> TemporalConvertTentative<Self> {
        value
            .peak
            .check_loss(i, allow_loss)
            .inner_into()
            .map(|()| Self::new(value.timestep, value.display, None))
    }
}

impl ConvertFromLayout<DataLayout3_0> for DataLayout2_0 {
    fn convert_from_layout(value: DataLayout3_0) -> LayoutConvertResult<Self> {
        Ok(Self(value.0.phantom_into()))
    }
}

impl ConvertFromLayout<DataLayout3_1> for DataLayout2_0 {
    fn convert_from_layout(value: DataLayout3_1) -> LayoutConvertResult<Self> {
        value.into_ordered().map(Into::into)
    }
}

impl ConvertFromLayout<DataLayout3_2> for DataLayout2_0 {
    fn convert_from_layout(value: DataLayout3_2) -> LayoutConvertResult<Self> {
        value.into_ordered().map(Into::into)
    }
}

impl ConvertFromLayout<DataLayout2_0> for DataLayout3_0 {
    fn convert_from_layout(value: DataLayout2_0) -> LayoutConvertResult<Self> {
        Ok(Self(value.0.phantom_into()))
    }
}

impl ConvertFromLayout<DataLayout3_1> for DataLayout3_0 {
    fn convert_from_layout(value: DataLayout3_1) -> LayoutConvertResult<Self> {
        value.into_ordered().map(Into::into)
    }
}

impl ConvertFromLayout<DataLayout3_2> for DataLayout3_0 {
    fn convert_from_layout(value: DataLayout3_2) -> LayoutConvertResult<Self> {
        value.into_ordered().map(Into::into)
    }
}

impl ConvertFromLayout<DataLayout2_0> for DataLayout3_1 {
    fn convert_from_layout(value: DataLayout2_0) -> LayoutConvertResult<Self> {
        value.0.into_3_1()
    }
}

impl ConvertFromLayout<DataLayout3_0> for DataLayout3_1 {
    fn convert_from_layout(value: DataLayout3_0) -> LayoutConvertResult<Self> {
        value.0.into_3_1()
    }
}

impl ConvertFromLayout<DataLayout3_2> for DataLayout3_1 {
    fn convert_from_layout(value: DataLayout3_2) -> LayoutConvertResult<Self> {
        match value {
            DataLayout3_2::NonMixed(x) => Ok(Self(x.phantom_into())),
            DataLayout3_2::Mixed(x) => x.try_into_non_mixed().map(Self).mult_errors_into(),
        }
    }
}

impl ConvertFromLayout<DataLayout2_0> for DataLayout3_2 {
    fn convert_from_layout(value: DataLayout2_0) -> LayoutConvertResult<Self> {
        value.0.into_3_2()
    }
}

impl ConvertFromLayout<DataLayout3_0> for DataLayout3_2 {
    fn convert_from_layout(value: DataLayout3_0) -> LayoutConvertResult<Self> {
        value.0.into_3_2()
    }
}

impl ConvertFromLayout<DataLayout3_1> for DataLayout3_2 {
    fn convert_from_layout(value: DataLayout3_1) -> LayoutConvertResult<Self> {
        Ok(Self::NonMixed(value.0.phantom_into()))
    }
}

impl Versioned for Version2_0 {
    type Layout = DataLayout2_0;
    type Offsets = TEXTOffsets2_0;

    fn fcs_version() -> Self {
        Self
    }
}

impl Versioned for Version3_0 {
    type Layout = DataLayout3_0;
    type Offsets = TEXTOffsets3_0;

    fn fcs_version() -> Self {
        Self
    }
}

impl Versioned for Version3_1 {
    type Layout = DataLayout3_1;
    type Offsets = TEXTOffsets3_0;

    fn fcs_version() -> Self {
        Self
    }
}

impl Versioned for Version3_2 {
    type Layout = DataLayout3_2;
    type Offsets = TEXTOffsets3_2;

    fn fcs_version() -> Self {
        Self
    }
}

impl AsScaleTransform for InnerOptical2_0 {
    fn as_transform(&self) -> ScaleTransform {
        self.scale.0.map(Into::into).unwrap_or_default()
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
    fn lookup_specific(
        kws: &mut StdKeywords,
        i: MeasIndex,
        conf: &StdTextReadConfig,
    ) -> LookupResult<Self> {
        let scale = Scale::lookup_meas_opt_st(kws, i, false, (), conf);
        let wave = Wavelength::lookup_meas_opt(kws, i, false, conf);
        let peak = PeakData::lookup(kws, i, false, conf);
        Ok(scale
            .zip3(wave, peak)
            .errors_into()
            .map(|(si, wi, pi)| Self::new(si, wi, pi)))
    }
}

impl LookupOptical for InnerOptical3_0 {
    fn lookup_specific(
        kws: &mut StdKeywords,
        i: MeasIndex,
        conf: &StdTextReadConfig,
    ) -> LookupResult<Self> {
        let wave = Wavelength::lookup_meas_opt(kws, i, false, conf);
        let peak = PeakData::lookup(kws, i, false, conf);
        wave.zip(peak).errors_into().and_maybe(|(wi, pi)| {
            ScaleTransform::lookup(kws, i, conf).def_map_value(|s| Self::new(s, wi, pi))
        })
    }
}

impl LookupOptical for InnerOptical3_1 {
    fn lookup_specific(
        kws: &mut StdKeywords,
        i: MeasIndex,
        conf: &StdTextReadConfig,
    ) -> LookupResult<Self> {
        let wave = Wavelengths::lookup_meas_opt_st(kws, i, false, (), conf);
        let cal = Calibration3_1::lookup_meas_opt(kws, i, false, conf);
        let dpy = Display::lookup_meas_opt(kws, i, false, conf);
        let peak = PeakData::lookup(kws, i, true, conf);
        wave.zip4(cal, dpy, peak)
            .errors_into()
            .and_maybe(|(wi, ci, di, pi)| {
                ScaleTransform::lookup(kws, i, conf)
                    .def_map_value(|scale| Self::new(scale, wi, ci, di, pi))
            })
    }
}

impl LookupOptical for InnerOptical3_2 {
    fn lookup_specific(
        kws: &mut StdKeywords,
        i: MeasIndex,
        conf: &StdTextReadConfig,
    ) -> LookupResult<Self> {
        let wave = Wavelengths::lookup_meas_opt_st(kws, i, false, (), conf);
        let cal = Calibration3_2::lookup_meas_opt(kws, i, false, conf);
        let dpy = Display::lookup_meas_opt(kws, i, false, conf);
        let det_name = DetectorName::lookup_meas_opt(kws, i, false, conf);
        let tag = Tag::lookup_meas_opt(kws, i, false, conf);
        let meas = OpticalType::lookup_meas_opt(kws, i, false, conf);
        let feat = Feature::lookup_meas_opt(kws, i, false, conf);
        let anal = Analyte::lookup_meas_opt(kws, i, false, conf);
        wave.zip4(cal, dpy, det_name)
            .zip5(tag, meas, feat, anal)
            .errors_into()
            .and_maybe(|((w, c, d, n), t, m, f, a)| {
                ScaleTransform::lookup(kws, i, conf)
                    .def_map_value(|s| Self::new(s, w, c, d, a, f, m, t, n))
            })
    }
}

impl LookupTemporal for InnerTemporal2_0 {
    fn lookup_specific(
        std: &mut StdKeywords,
        i: MeasIndex,
        nonstd: &mut NonStdKeywords,
        conf: &StdTextReadConfig,
    ) -> LookupResult<Self> {
        let scale = if conf.force_time_linear {
            nonstd.transfer_demoted(std, TemporalScale::std(i));
            Tentative::new1(Some(TemporalScale).into())
        } else {
            TemporalScale::lookup_meas_opt(std, i, false, conf)
        };
        let peak = PeakData::lookup(std, i, false, conf);
        TemporalOpticalKey::remove_keys(&conf.ignore_time_optical_keys, std, nonstd, i);
        Ok(scale.zip(peak).errors_into().map(|(s, p)| Self::new(s, p)))
    }
}

impl LookupTemporal for InnerTemporal3_0 {
    fn lookup_specific(
        std: &mut StdKeywords,
        i: MeasIndex,
        nonstd: &mut NonStdKeywords,
        conf: &StdTextReadConfig,
    ) -> LookupResult<Self> {
        let gain = lookup_temporal_gain_3_0(std, i, nonstd, conf);
        let peak = PeakData::lookup(std, i, false, conf);
        TemporalOpticalKey::remove_keys(&conf.ignore_time_optical_keys, std, nonstd, i);
        gain.zip(peak).errors_into().and_maybe(|(_, p)| {
            let scale = lookup_temporal_scale_3_0(std, i, nonstd, conf);
            let timestep = Timestep::lookup_req(std);
            scale
                .def_zip(timestep)
                .def_map_value(|(_, t)| Self::new(t, p))
        })
    }
}

impl LookupTemporal for InnerTemporal3_1 {
    fn lookup_specific(
        std: &mut StdKeywords,
        i: MeasIndex,
        nonstd: &mut NonStdKeywords,
        conf: &StdTextReadConfig,
    ) -> LookupResult<Self> {
        let gain = lookup_temporal_gain_3_0(std, i, nonstd, conf);
        let dpy = Display::lookup_meas_opt(std, i, false, conf);
        let peak = PeakData::lookup(std, i, true, conf).errors_into();
        TemporalOpticalKey::remove_keys(&conf.ignore_time_optical_keys, std, nonstd, i);
        gain.zip3(dpy, peak).errors_into().and_maybe(|(_, d, p)| {
            let scale = lookup_temporal_scale_3_0(std, i, nonstd, conf);
            let timestep = Timestep::lookup_req(std);
            scale
                .def_zip(timestep)
                .def_map_value(|(_, t)| Self::new(t, d, p))
        })
    }
}

impl LookupTemporal for InnerTemporal3_2 {
    fn lookup_specific(
        std: &mut StdKeywords,
        i: MeasIndex,
        nonstd: &mut NonStdKeywords,
        conf: &StdTextReadConfig,
    ) -> LookupResult<Self> {
        let gain = lookup_temporal_gain_3_0(std, i, nonstd, conf);
        let dpy = Display::lookup_meas_opt(std, i, false, conf);
        let meas = TemporalType::lookup_meas_opt(std, i, false, conf);
        TemporalOpticalKey::remove_keys(&conf.ignore_time_optical_keys, std, nonstd, i);
        gain.zip3(dpy, meas).errors_into().and_maybe(|(_, d, m)| {
            let scale = lookup_temporal_scale_3_0(std, i, nonstd, conf);
            let timestep = Timestep::lookup_req(std);
            scale
                .def_zip(timestep)
                .def_map_value(|(_, t)| Self::new(t, d, m))
        })
    }
}

impl VersionedOptical for InnerOptical2_0 {
    type Ver = Version2_0;
    fn req_suffixes_inner(
        &self,
        _: MeasIndex,
    ) -> impl Iterator<Item = (MeasHeader, String, String)> {
        [].into_iter()
    }

    fn opt_suffixes_inner(
        &self,
        i: MeasIndex,
    ) -> impl Iterator<Item = (MeasHeader, String, Option<String>)> {
        [
            self.scale.meas_kw_triple(i),
            self.wavelength.meas_kw_triple(i),
        ]
        .into_iter()
        .chain(self.peak.opt_keywords(i))
    }

    fn can_convert_to_temporal(&self, i: MeasIndex) -> MultiResult<(), OpticalToTemporalError> {
        let mut res = self
            .wavelength
            .check_indexed_key_transfer(i)
            .map_err(OpticalToTemporalError::Loss)
            .into_mult();
        if let Err(err) = res.as_mut()
            && !self.scale.as_ref_opt().is_some_and(|s| *s == Scale::Linear)
        {
            err.push(OpticalNonLinearError.into());
        }
        res
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
        once(self.wavelength.meas_kw_triple(i))
            .chain(self.peak.opt_keywords(i))
            .chain(self.scale.opt_suffixes(i))
    }

    fn can_convert_to_temporal(&self, i: MeasIndex) -> MultiResult<(), OpticalToTemporalError> {
        let w = self
            .wavelength
            .check_indexed_key_transfer(i)
            .map_err(OpticalToTemporalError::Loss);
        let s = if self.scale.is_noop() {
            Ok(())
        } else {
            Err(OpticalNonLinearError.into())
        };
        w.zip(s).void()
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
            self.wavelengths.meas_kw_triple(i),
            self.calibration.meas_kw_triple(i),
            self.display.meas_kw_triple(i),
        ]
        .into_iter()
        .chain(self.peak.opt_keywords(i))
        .chain(self.scale.opt_suffixes(i))
    }

    fn can_convert_to_temporal(&self, i: MeasIndex) -> MultiResult<(), OpticalToTemporalError> {
        let c = self
            .calibration
            .check_indexed_key_transfer(i)
            .map_err(OpticalToTemporalError::Loss);
        let w = self
            .wavelengths
            .check_indexed_key_transfer(i)
            .map_err(OpticalToTemporalError::Loss);
        let s = if self.scale.is_noop() {
            Ok(())
        } else {
            Err(OpticalNonLinearError.into())
        };
        c.zip3(w, s).void()
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
            self.wavelengths.meas_kw_triple(i),
            self.calibration.meas_kw_triple(i),
            self.display.meas_kw_triple(i),
            self.detector_name.meas_opt_triple(i),
            self.tag.meas_opt_triple(i),
            self.measurement_type.meas_kw_triple(i),
            self.feature.meas_kw_triple(i),
            self.analyte.meas_opt_triple(i),
        ]
        .into_iter()
        .chain(self.scale.opt_suffixes(i))
    }

    fn can_convert_to_temporal(&self, i: MeasIndex) -> MultiResult<(), OpticalToTemporalError> {
        let cal = self
            .calibration
            .check_indexed_key_transfer::<AnyOpticalToTemporalKeyLossError>(i);
        let wave = self.wavelengths.check_indexed_key_transfer(i);
        let meas = self.measurement_type.check_indexed_key_transfer(i);
        let anal = self.analyte.check_indexed_key_transfer(i);
        let tag = self.tag.check_indexed_key_transfer(i);
        let det_name = self.detector_name.check_indexed_key_transfer(i);
        let feat = self.feature.check_indexed_key_transfer(i);
        let res = cal
            .zip3(wave, meas)
            .mult_zip3(anal.zip(tag), det_name.zip(feat))
            .mult_errors_into();
        let s = if self.scale.is_noop() {
            Ok(())
        } else {
            Err(OpticalNonLinearError)
        };
        res.mult_zip(s.into_mult()).void()
    }
}

impl VersionedTemporal for InnerTemporal2_0 {
    type Ver = Version2_0;
    type Err = Infallible;

    fn req_meta_keywords_inner(&self) -> impl Iterator<Item = (String, String)> {
        [].into_iter()
    }

    fn req_meas_keywords_inner(&self, _: MeasIndex) -> impl Iterator<Item = (String, String)> {
        [].into_iter()
    }

    fn opt_meas_keywords_inner(&self, i: MeasIndex) -> impl Iterator<Item = (String, String)> {
        self.peak
            .opt_keywords(i)
            .map(|(_, k, v)| (k, v))
            .chain([self.scale.meas_kw_pair(i)])
            .filter_map(|(k, v)| v.map(|x| (k, x)))
    }

    fn can_convert_to_optical(&self, _: MeasIndex) -> MultiResult<(), Self::Err> {
        Ok(())
    }

    fn can_convert_to_optical_swap(
        &self,
        i: MeasIndex,
    ) -> MultiResult<(), SwapOpticalTemporalError> {
        let Ok(ret) = self.can_convert_to_optical(i);
        Ok(ret)
    }
}

impl VersionedTemporal for InnerTemporal3_0 {
    type Ver = Version3_0;
    type Err = Infallible;

    fn req_meta_keywords_inner(&self) -> impl Iterator<Item = (String, String)> {
        [self.timestep.pair()].into_iter()
    }

    fn req_meas_keywords_inner(&self, i: MeasIndex) -> impl Iterator<Item = (String, String)> {
        [TemporalScale.meas_pair(i)].into_iter()
    }

    fn opt_meas_keywords_inner(&self, i: MeasIndex) -> impl Iterator<Item = (String, String)> {
        self.peak
            .opt_keywords(i)
            .filter_map(|(_, k, v)| v.map(|x| (k, x)))
    }

    fn can_convert_to_optical(&self, _: MeasIndex) -> MultiResult<(), Self::Err> {
        Ok(())
    }

    fn can_convert_to_optical_swap(
        &self,
        i: MeasIndex,
    ) -> MultiResult<(), SwapOpticalTemporalError> {
        let Ok(ret) = self.can_convert_to_optical(i);
        Ok(ret)
    }
}

impl VersionedTemporal for InnerTemporal3_1 {
    type Ver = Version3_1;
    type Err = Infallible;

    fn req_meta_keywords_inner(&self) -> impl Iterator<Item = (String, String)> {
        [self.timestep.pair()].into_iter()
    }

    fn req_meas_keywords_inner(&self, i: MeasIndex) -> impl Iterator<Item = (String, String)> {
        [TemporalScale.meas_pair(i)].into_iter()
    }

    fn opt_meas_keywords_inner(&self, i: MeasIndex) -> impl Iterator<Item = (String, String)> {
        self.peak
            .opt_keywords(i)
            .map(|(_, k, v)| (k, v))
            .chain([self.display.meas_kw_pair(i)])
            .filter_map(|(k, v)| v.map(|x| (k, x)))
    }

    fn can_convert_to_optical(&self, _: MeasIndex) -> MultiResult<(), Self::Err> {
        Ok(())
    }

    fn can_convert_to_optical_swap(
        &self,
        i: MeasIndex,
    ) -> MultiResult<(), SwapOpticalTemporalError> {
        let Ok(ret) = self.can_convert_to_optical(i);
        Ok(ret)
    }
}

impl VersionedTemporal for InnerTemporal3_2 {
    type Ver = Version3_2;
    type Err = TemporalToOpticalError;

    fn req_meta_keywords_inner(&self) -> impl Iterator<Item = (String, String)> {
        [self.timestep.pair()].into_iter()
    }

    fn req_meas_keywords_inner(&self, i: MeasIndex) -> impl Iterator<Item = (String, String)> {
        [TemporalScale.meas_pair(i)].into_iter()
    }

    fn opt_meas_keywords_inner(&self, i: MeasIndex) -> impl Iterator<Item = (String, String)> {
        once(self.display.meas_kw_pair(i)).filter_map(|(k, v)| v.map(|x| (k, x)))
    }

    fn can_convert_to_optical(&self, i: MeasIndex) -> MultiResult<(), Self::Err> {
        self.measurement_type
            .check_indexed_key_transfer(i)
            .map_err(TemporalToOpticalError::Loss)
            .map_err(NonEmpty::new)
    }

    fn can_convert_to_optical_swap(
        &self,
        i: MeasIndex,
    ) -> MultiResult<(), SwapOpticalTemporalError> {
        self.can_convert_to_optical(i).mult_errors_into()
    }
}

impl VersionedTEXTOffsets for TEXTOffsets2_0 {
    type TotDef = MaybeTot;

    fn lookup<C>(
        kws: &mut StdKeywords,
        data: HeaderDataSegment,
        analysis: HeaderAnalysisSegment,
        _: &ReadState<C>,
    ) -> LookupTEXTOffsetsResult<Self>
    where
        C: AsRef<ReadTEXTOffsetsConfig>,
    {
        Ok(Tot::remove_metaroot_opt(kws)
            .map(|x| x.0)
            .into_tentative_warn_def()
            .warnings_into()
            .map(|tot| {
                TEXTOffsets::new(
                    DatasetSegments::new(data.into_any(), analysis.into_any()),
                    tot,
                )
                .into()
            }))
    }

    fn lookup_ro<C>(
        kws: &StdKeywords,
        data: HeaderDataSegment,
        analysis: HeaderAnalysisSegment,
        _: &ReadState<C>,
    ) -> LookupTEXTOffsetsResult<Self>
    where
        C: AsRef<ReadTEXTOffsetsConfig>,
    {
        Ok(Tot::get_metaroot_opt(kws)
            .map(|x| x.0)
            .into_tentative_warn_def()
            .warnings_into()
            .map(|tot| {
                TEXTOffsets::new(
                    DatasetSegments::new(data.into_any(), analysis.into_any()),
                    tot,
                )
                .into()
            }))
    }

    fn tot(&self) -> <Self::TotDef as TotDefinition>::Tot {
        self.0.tot
    }

    fn into_common(self) -> TEXTOffsets<Option<Tot>> {
        let x = self.0;
        TEXTOffsets::new(x.segs, x.tot)
    }
}

impl VersionedTEXTOffsets for TEXTOffsets3_0 {
    type TotDef = KnownTot;

    fn lookup<C>(
        kws: &mut StdKeywords,
        data: HeaderDataSegment,
        analysis: HeaderAnalysisSegment,
        st: &ReadState<C>,
    ) -> LookupTEXTOffsetsResult<Self>
    where
        C: AsRef<ReadTEXTOffsetsConfig>,
    {
        let tot_res = Tot::remove_metaroot_req(kws).into_deferred();
        let file_len = Some(st.file_len.into());
        let conf = st.conf.as_ref();
        let data_res = KeyedReqSegment::remove_or(
            kws,
            data,
            conf.ignore_text_data_offsets,
            conf.allow_header_text_offset_mismatch,
            conf.allow_missing_required_offsets,
            &NewSegmentConfig::new(
                conf.text_data_correction,
                file_len,
                conf.truncate_text_offsets,
            ),
        )
        .def_inner_into();
        let analysis_res = KeyedReqSegment::remove_or(
            kws,
            analysis,
            conf.ignore_text_analysis_offsets,
            conf.allow_header_text_offset_mismatch,
            conf.allow_missing_required_offsets,
            &NewSegmentConfig::new(
                conf.text_analysis_correction,
                file_len,
                conf.truncate_text_offsets,
            ),
        )
        .def_inner_into();
        tot_res
            .def_zip3(data_res, analysis_res)
            .def_map_value(|(tot, d, a)| TEXTOffsets::new(DatasetSegments::new(d, a), tot).into())
    }

    fn lookup_ro<C>(
        kws: &StdKeywords,
        data: HeaderDataSegment,
        analysis: HeaderAnalysisSegment,
        st: &ReadState<C>,
    ) -> LookupTEXTOffsetsResult<Self>
    where
        C: AsRef<ReadTEXTOffsetsConfig>,
    {
        let tot_res = Tot::get_metaroot_req(kws).into_deferred();
        let file_len = Some(st.file_len.into());
        let conf = st.conf.as_ref();
        let data_res = KeyedReqSegment::get_or(
            kws,
            data,
            conf.ignore_text_data_offsets,
            conf.allow_header_text_offset_mismatch,
            conf.allow_missing_required_offsets,
            &NewSegmentConfig::new(
                conf.text_data_correction,
                file_len,
                conf.truncate_text_offsets,
            ),
        )
        .def_inner_into();
        let analysis_res = KeyedReqSegment::get_or(
            kws,
            analysis,
            conf.ignore_text_analysis_offsets,
            conf.allow_header_text_offset_mismatch,
            conf.allow_missing_required_offsets,
            &NewSegmentConfig::new(
                conf.text_analysis_correction,
                file_len,
                conf.truncate_text_offsets,
            ),
        )
        .def_inner_into();
        tot_res
            .def_zip3(data_res, analysis_res)
            .def_map_value(|(tot, d, a)| TEXTOffsets::new(DatasetSegments::new(d, a), tot).into())
    }

    fn tot(&self) -> <Self::TotDef as TotDefinition>::Tot {
        self.0.tot
    }

    fn into_common(self) -> TEXTOffsets<Option<Tot>> {
        let x = self.0;
        TEXTOffsets::new(x.segs, Some(x.tot))
    }
}

impl VersionedTEXTOffsets for TEXTOffsets3_2 {
    type TotDef = KnownTot;

    fn lookup<C>(
        kws: &mut StdKeywords,
        data: HeaderDataSegment,
        analysis: HeaderAnalysisSegment,
        st: &ReadState<C>,
    ) -> LookupTEXTOffsetsResult<Self>
    where
        C: AsRef<ReadTEXTOffsetsConfig>,
    {
        let tot_res = Tot::remove_metaroot_req(kws).into_deferred();
        let file_len = Some(st.file_len.into());
        let conf = st.conf.as_ref();
        let data_res = KeyedReqSegment::remove_or(
            kws,
            data,
            conf.ignore_text_data_offsets,
            conf.allow_header_text_offset_mismatch,
            conf.allow_missing_required_offsets,
            &NewSegmentConfig::new(
                conf.text_data_correction,
                file_len,
                conf.truncate_text_offsets,
            ),
        )
        .def_inner_into();
        tot_res.def_zip(data_res).def_and_tentatively(|(tot, d)| {
            {
                KeyedOptSegment::remove_or(
                    kws,
                    analysis,
                    conf.ignore_text_analysis_offsets,
                    conf.allow_header_text_offset_mismatch,
                    &NewSegmentConfig::new(
                        conf.text_analysis_correction,
                        file_len,
                        conf.truncate_text_offsets,
                    ),
                )
                .inner_into()
                .map(|a| TEXTOffsets::new(DatasetSegments::new(d, a), tot).into())
            }
        })
    }

    fn lookup_ro<C>(
        kws: &StdKeywords,
        data: HeaderDataSegment,
        analysis: HeaderAnalysisSegment,
        st: &ReadState<C>,
    ) -> LookupTEXTOffsetsResult<Self>
    where
        C: AsRef<ReadTEXTOffsetsConfig>,
    {
        let conf = &st.conf.as_ref();
        let tot_res = Tot::get_metaroot_req(kws).into_deferred();
        let file_len = Some(st.file_len.into());
        let data_res = KeyedReqSegment::get_or(
            kws,
            data,
            conf.ignore_text_data_offsets,
            conf.allow_header_text_offset_mismatch,
            conf.allow_missing_required_offsets,
            &NewSegmentConfig::new(
                conf.text_data_correction,
                file_len,
                conf.truncate_text_offsets,
            ),
        )
        .def_inner_into();
        tot_res.def_zip(data_res).def_and_tentatively(|(tot, d)| {
            KeyedOptSegment::get_or(
                kws,
                analysis,
                conf.ignore_text_analysis_offsets,
                conf.allow_header_text_offset_mismatch,
                &NewSegmentConfig::new(
                    conf.text_analysis_correction,
                    file_len,
                    conf.truncate_text_offsets,
                ),
            )
            .inner_into()
            .map(|a| TEXTOffsets::new(DatasetSegments::new(d, a), tot).into())
        })
    }

    fn tot(&self) -> <Self::TotDef as TotDefinition>::Tot {
        self.0.tot
    }

    fn into_common(self) -> TEXTOffsets<Option<Tot>> {
        let x = self.0;
        TEXTOffsets::new(x.segs, Some(x.tot))
    }
}

impl OpticalFromTemporal<InnerTemporal2_0> for InnerOptical2_0 {
    type TData = ();
    type Loss = ();

    #[allow(clippy::type_complexity)]
    fn from_temporal(
        t: Temporal<InnerTemporal2_0>,
        i: MeasIndex,
        (): Self::Loss,
    ) -> PassthruResult<
        (Optical<Self>, Self::TData),
        Box<Temporal<InnerTemporal2_0>>,
        Infallible,
        Infallible,
    > {
        t.specific
            .can_convert_to_optical(i)
            .unwrap_or_else(|e| match e {});
        Ok(Tentative::new1(Self::from_temporal_unchecked(t)))
    }

    fn from_temporal_inner(t: InnerTemporal2_0) -> (Self, Self::TData) {
        let new = Self::new(Some(Scale::Linear), None, t.peak);
        (new, ())
    }
}

impl OpticalFromTemporal<InnerTemporal3_0> for InnerOptical3_0 {
    type TData = Timestep;
    type Loss = ();

    #[allow(clippy::type_complexity)]
    fn from_temporal(
        t: Temporal<InnerTemporal3_0>,
        i: MeasIndex,
        (): Self::Loss,
    ) -> PassthruResult<
        (Optical<Self>, Self::TData),
        Box<Temporal<InnerTemporal3_0>>,
        Infallible,
        Infallible,
    > {
        t.specific
            .can_convert_to_optical(i)
            .unwrap_or_else(|e| match e {});
        Ok(Tentative::new1(Self::from_temporal_unchecked(t)))
    }

    fn from_temporal_inner(t: InnerTemporal3_0) -> (Self, Self::TData) {
        let new = Self::new(ScaleTransform::default(), None, t.peak);
        (new, t.timestep)
    }
}

impl OpticalFromTemporal<InnerTemporal3_1> for InnerOptical3_1 {
    type TData = Timestep;
    type Loss = ();

    #[allow(clippy::type_complexity)]
    fn from_temporal(
        t: Temporal<InnerTemporal3_1>,
        i: MeasIndex,
        (): Self::Loss,
    ) -> PassthruResult<
        (Optical<Self>, Self::TData),
        Box<Temporal<InnerTemporal3_1>>,
        Infallible,
        Infallible,
    > {
        t.specific
            .can_convert_to_optical(i)
            .unwrap_or_else(|e| match e {});
        Ok(Tentative::new1(Self::from_temporal_unchecked(t)))
    }

    fn from_temporal_inner(t: InnerTemporal3_1) -> (Self, Self::TData) {
        let new = Self::new(ScaleTransform::default(), None, None, t.display, t.peak);
        (new, t.timestep)
    }
}

impl OpticalFromTemporal<InnerTemporal3_2> for InnerOptical3_2 {
    type TData = Timestep;
    type Loss = bool;

    #[allow(clippy::type_complexity)]
    fn from_temporal(
        t: Temporal<InnerTemporal3_2>,
        i: MeasIndex,
        allow_loss: Self::Loss,
    ) -> PassthruResult<
        (Optical<Self>, Self::TData),
        Box<Temporal<InnerTemporal3_2>>,
        TemporalToOpticalError,
        TemporalToOpticalError,
    > {
        match t.specific.can_convert_to_optical(i) {
            Ok(()) => Ok(Tentative::new1(Self::from_temporal_unchecked(t))),
            Err(es) => {
                if allow_loss {
                    Ok(Tentative::new(Self::from_temporal_unchecked(t), es, []))
                } else {
                    Err(DeferredFailure::new([], es, Box::new(t)))
                }
            }
        }
    }

    fn from_temporal_inner(t: InnerTemporal3_2) -> (Self, Self::TData) {
        let new = Self::new(
            ScaleTransform::default(),
            None,
            None,
            t.display,
            Analyte::default(),
            None,
            None,
            Tag::default(),
            DetectorName::default(),
        );
        (new, t.timestep)
    }
}

impl TemporalFromOptical<InnerOptical2_0> for InnerTemporal2_0 {
    type TData = ();

    fn from_optical_inner(o: InnerOptical2_0, (): Self::TData) -> Self {
        Self::new(o.scale.map(|_| TemporalScale), o.peak)
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
        Self::new(d, o.display, None)
    }
}

type Timestamps2_0 = Timestamps<FCSTime>;
type Timestamps3_0 = Timestamps<FCSTime60>;
type Timestamps3_1 = Timestamps<FCSTime100>;

impl LookupMetaroot for InnerMetaroot2_0 {
    fn lookup_shortname(
        kws: &mut StdKeywords,
        i: MeasIndex,
        conf: &StdTextReadConfig,
    ) -> LookupResult<<Self::Name as MightHave>::Wrapper<Shortname>> {
        Ok(Shortname::lookup_meas_opt(kws, i, false, conf).errors_into())
    }

    fn lookup_specific(
        kws: &mut StdKeywords,
        par: Par,
        _: &HashSet<&Shortname>,
        _: &[&Shortname],
        conf: &StdTextReadConfig,
    ) -> LookupResult<Self> {
        let comp = Compensation2_0::lookup(kws, par, conf);
        let cytn = Cyt::lookup_metaroot_opt(kws, false, conf);
        let ts = Timestamps::lookup(kws, false, conf);
        let ag = AppliedGates2_0::lookup(kws, par, conf);
        comp.zip4(cytn, ts, ag)
            .errors_into()
            .and_maybe(|(co, cy, t, g)| {
                Mode::lookup_req(kws).def_map_value(|mo| Self::new(mo, cy, co, t, g))
            })
    }
}

impl LookupMetaroot for InnerMetaroot3_0 {
    fn lookup_shortname(
        kws: &mut StdKeywords,
        i: MeasIndex,
        conf: &StdTextReadConfig,
    ) -> LookupResult<<Self::Name as MightHave>::Wrapper<Shortname>> {
        Ok(Shortname::lookup_meas_opt(kws, i, false, conf).errors_into())
    }

    fn lookup_specific(
        kws: &mut StdKeywords,
        par: Par,
        _: &HashSet<&Shortname>,
        _: &[&Shortname],
        conf: &StdTextReadConfig,
    ) -> LookupResult<Self> {
        let comp = Compensation3_0::lookup_metaroot_opt(kws, false, conf);
        let cyt = Cyt::lookup_metaroot_opt(kws, false, conf);
        let cytsn = Cytsn::lookup_metaroot_opt(kws, false, conf);
        let subset = SubsetData::lookup(kws, conf);
        let ts = Timestamps::lookup(kws, false, conf);
        let uni = Unicode::lookup_metatroot_opt_st(kws, false, (), conf);
        let ag = AppliedGates3_0::lookup(kws, par, false, conf);
        comp.zip4(cyt, cytsn, subset)
            .zip4(ts, uni, ag)
            .errors_into()
            .and_maybe(|((co, cy, sn, su), t, u, g)| {
                Mode::lookup_req(kws).def_map_value(|mo| Self::new(mo, cy, co, t, sn, u, su, g))
            })
    }
}

impl LookupMetaroot for InnerMetaroot3_1 {
    fn lookup_shortname(
        kws: &mut StdKeywords,
        i: MeasIndex,
        _: &StdTextReadConfig,
    ) -> LookupResult<<Self::Name as MightHave>::Wrapper<Shortname>> {
        Shortname::lookup_req(kws, i).map(|x| x.map(AlwaysValue))
    }

    fn lookup_specific(
        kws: &mut StdKeywords,
        par: Par,
        names: &HashSet<&Shortname>,
        ordered_names: &[&Shortname],
        conf: &StdTextReadConfig,
    ) -> LookupResult<Self> {
        let cyt = Cyt::lookup_metaroot_opt(kws, false, conf);
        let spill = Spillover::lookup_metatroot_opt_st(kws, false, (names, ordered_names), conf);
        let cytsn = Cytsn::lookup_metaroot_opt(kws, false, conf);
        let subset = SubsetData::lookup(kws, conf);
        let modif = ModificationData::lookup(kws, conf);
        let plate = PlateData::lookup(kws, false, conf);
        let ts = Timestamps::lookup(kws, false, conf);
        let vol = Vol::lookup_metaroot_opt(kws, false, conf);
        let ag = AppliedGates3_0::lookup(kws, par, true, conf).errors_into();
        let mode_dep = |m: &Mode| match m {
            Mode::Correlated => Some(DeprecatedError::Value(DepValueWarning::ModeCorrelated)),
            Mode::Uncorrelated => Some(DeprecatedError::Value(DepValueWarning::ModeUncorrelated)),
            Mode::List => None,
        };
        cyt.zip5(spill, cytsn, subset, modif)
            .zip5(plate, ts, vol, ag)
            .errors_into()
            .and_maybe(|((c, sp, sn, su, md), p, t, v, g)| {
                let mut mode = Mode::lookup_req(kws);
                mode.def_eval_warning(mode_dep);
                mode.def_map_value(|mo| Self::new(mo, c, t, sn, sp, md, p, v, su, g))
            })
    }
}

impl LookupMetaroot for InnerMetaroot3_2 {
    fn lookup_shortname(
        kws: &mut StdKeywords,
        i: MeasIndex,
        _: &StdTextReadConfig,
    ) -> LookupResult<<Self::Name as MightHave>::Wrapper<Shortname>> {
        Shortname::lookup_req(kws, i).map(|x| x.map(AlwaysValue))
    }

    fn lookup_specific(
        kws: &mut StdKeywords,
        par: Par,
        names: &HashSet<&Shortname>,
        ordered_names: &[&Shortname],
        conf: &StdTextReadConfig,
    ) -> LookupResult<Self> {
        let carrier = CarrierData::lookup(kws, conf);
        let dt = Datetimes::lookup(kws, conf);
        let flow = Flowrate::lookup_metaroot_opt(kws, false, conf);
        let modif = ModificationData::lookup(kws, conf);
        let mode = Mode3_2::lookup_metaroot_opt(kws, true, conf);
        let spill = Spillover::lookup_metatroot_opt_st(kws, false, (names, ordered_names), conf);
        let cytsn = Cytsn::lookup_metaroot_opt(kws, false, conf);
        let plate = PlateData::lookup(kws, true, conf);
        let ts = Timestamps::lookup(kws, true, conf);
        let us = UnstainedData::lookup(kws, names, conf);
        let vol = Vol::lookup_metaroot_opt(kws, false, conf);
        let ag = AppliedGates3_2::lookup(kws, par, conf);
        carrier
            .zip6(dt, flow, modif, mode, spill)
            .zip6(cytsn, plate, ts, us, vol)
            .zip(ag)
            .errors_into()
            .and_maybe(
                |(((ca_, d_, f_, md_, mo_, sp_), sn_, p_, t_, u_, v_), ag_)| {
                    Cyt3_2::lookup_req(kws).def_map_value(|c_| {
                        Self::new(mo_, t_, d_, c_, sp_, sn_, md_, p_, v_, ca_, u_, f_, ag_)
                    })
                },
            )
    }
}

impl VersionedMetaroot for InnerMetaroot2_0 {
    type Ver = Version2_0;
    type Optical = InnerOptical2_0;
    type Temporal = InnerTemporal2_0;
    type Name = MaybeFamily;

    fn check_meas_named_links_inner(
        &self,
        _: &HashSet<&Shortname>,
    ) -> Result<(), ExistingNamedLinkError> {
        Ok(())
    }

    fn check_meas_index_links_inner(
        &self,
        _: &HashSet<MeasIndex>,
    ) -> Result<(), ExistingIndexLinkError> {
        // don't check specific indices for $COMP since this keyword links
        // all indices
        if self.comp.0.is_some() {
            Err(ExistingIndexLinkError::Comp)
        } else {
            Ok(())
        }
    }

    fn rename_meas_links_inner(&mut self, _: &NameMapping) {}

    fn insert_meas_index_inner(&mut self, i: MeasIndex) {
        if let Some(x) = self.comp.0.as_mut() {
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
                    .as_ref_opt()
                    .map_or(vec![], Compensation2_0::opt_keywords),
            )
    }

    fn swap_optical_temporal_inner(
        t: Self::Temporal,
        o: Self::Optical,
    ) -> (Self::Optical, Self::Temporal) {
        let new_t = Self::Temporal::new(o.scale.map(|_| TemporalScale), o.peak);
        let new_o = Self::Optical::new(t.scale.map(|_| Scale::Linear), None, t.peak);
        (new_o, new_t)
    }
}

impl VersionedMetaroot for InnerMetaroot3_0 {
    type Ver = Version3_0;
    type Optical = InnerOptical3_0;
    type Temporal = InnerTemporal3_0;
    type Name = MaybeFamily;

    fn check_meas_named_links_inner(
        &self,
        _: &HashSet<&Shortname>,
    ) -> Result<(), ExistingNamedLinkError> {
        Ok(())
    }

    fn check_meas_index_links_inner(
        &self,
        indices: &HashSet<MeasIndex>,
    ) -> Result<(), ExistingIndexLinkError> {
        // don't check specific indices for $COMP since this keyword links
        // all indices
        if self.comp.0.is_some() {
            Err(ExistingIndexLinkError::Comp)
        } else if self.applied_gates.indices_difference(indices).count() > 0 {
            Err(ExistingIndexLinkError::GateRegion)
        } else {
            Ok(())
        }
    }

    fn rename_meas_links_inner(&mut self, _: &NameMapping) {}

    fn insert_meas_index_inner(&mut self, i: MeasIndex) {
        if let Some(x) = self.comp.0.as_mut() {
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
            self.comp.root_kw_pair(),
            self.cytsn.metaroot_opt_pair(),
            self.unicode.root_kw_pair(),
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
    type Name = AlwaysFamily;

    fn check_meas_named_links_inner(
        &self,
        names: &HashSet<&Shortname>,
    ) -> Result<(), ExistingNamedLinkError> {
        if self
            .spillover
            .0
            .as_ref()
            .is_some_and(|s| s.names_difference(names).count() > 0)
        {
            Err(ExistingNamedLinkError::Spillover)
        } else {
            Ok(())
        }
    }

    fn check_meas_index_links_inner(
        &self,
        indices: &HashSet<MeasIndex>,
    ) -> Result<(), ExistingIndexLinkError> {
        if self.applied_gates.indices_difference(indices).count() > 0 {
            Err(ExistingIndexLinkError::GateRegion)
        } else {
            Ok(())
        }
    }

    fn rename_meas_links_inner(&mut self, mapping: &NameMapping) {
        if let Some(s) = self.spillover.0.as_mut() {
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
            self.spillover.root_kw_pair(),
            self.cytsn.metaroot_opt_pair(),
            self.vol.root_kw_pair(),
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
        let new_o = Self::Optical::new(ScaleTransform::default(), None, None, t.display, t.peak);
        (new_o, new_t)
    }
}

impl VersionedMetaroot for InnerMetaroot3_2 {
    type Ver = Version3_2;
    type Optical = InnerOptical3_2;
    type Temporal = InnerTemporal3_2;
    type Name = AlwaysFamily;

    fn check_meas_named_links_inner(
        &self,
        names: &HashSet<&Shortname>,
    ) -> Result<(), ExistingNamedLinkError> {
        if self
            .spillover
            .0
            .as_ref()
            .is_some_and(|s| s.names_difference(names).count() > 0)
        {
            Err(ExistingNamedLinkError::Spillover)
        } else if self
            .unstained
            .unstainedcenters
            .0
            .as_ref()
            .is_some_and(|u| u.names_difference(names).count() > 0)
        {
            Err(ExistingNamedLinkError::UnstainedCenters)
        } else {
            Ok(())
        }
    }

    fn check_meas_index_links_inner(
        &self,
        indices: &HashSet<MeasIndex>,
    ) -> Result<(), ExistingIndexLinkError> {
        if self.applied_gates.indices_difference(indices).count() > 0 {
            Err(ExistingIndexLinkError::GateRegion)
        } else {
            Ok(())
        }
    }

    fn rename_meas_links_inner(&mut self, mapping: &NameMapping) {
        if let Some(x) = self.spillover.0.as_mut() {
            x.reassign(mapping);
        }
        if let Some(x) = self.unstained.unstainedcenters.0.as_mut() {
            x.reassign(mapping);
        }
    }

    fn insert_meas_index_inner(&mut self, i: MeasIndex) {
        self.applied_gates.shift_meas_indices_after_insert(i);
    }

    fn keywords_req_inner(&self) -> impl Iterator<Item = (String, String)> {
        [self.cyt.pair()].into_iter()
    }

    fn keywords_opt_inner(&self) -> impl Iterator<Item = (String, String)> {
        [
            self.spillover.root_kw_pair(),
            self.cytsn.metaroot_opt_pair(),
            self.vol.root_kw_pair(),
            self.flowrate.root_kw_pair(),
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
        let new_t = Self::Temporal::new(t.timestep, o.display, None);
        let new_o = Self::Optical::new(
            ScaleTransform::default(),
            None,
            None,
            t.display,
            Analyte::default(),
            None,
            None,
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
        size: Option<PeakNumber>,
        longname: Longname,
        nonstandard_keywords: NonStdKeywords,
    ) -> Self {
        let common = CommonMeasurement::new(longname, nonstandard_keywords);
        let scale = has_scale.then_some(TemporalScale);
        let specific = InnerTemporal2_0::new(scale, PeakData::new(bin, size));
        Self::new(common, specific)
    }
}

impl Temporal3_0 {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new_3_0(
        timestep: Timestep,
        bin: Option<PeakBin>,
        size: Option<PeakNumber>,
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
        size: Option<PeakNumber>,
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
        let meas_type = has_type.then_some(TemporalType);
        let specific = InnerTemporal3_2::new(timestep, display, meas_type);
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
        size: Option<PeakNumber>,
        filter: Option<Filter>,
        power: Option<Power>,
        detector_type: Option<DetectorType>,
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
        size: Option<PeakNumber>,
        filter: Option<Filter>,
        power: Option<Power>,
        detector_type: Option<DetectorType>,
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
        wavelengths: Option<Wavelengths>,
        calibration: Option<Calibration3_1>,
        display: Option<Display>,
        bin: Option<PeakBin>,
        size: Option<PeakNumber>,
        filter: Option<Filter>,
        power: Option<Power>,
        detector_type: Option<DetectorType>,
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
        wavelengths: Option<Wavelengths>,
        calibration: Option<Calibration3_2>,
        display: Option<Display>,
        analyte: Analyte,
        feature: Option<Feature>,
        tag: Tag,
        measurement_type: Option<OpticalType>,
        detector_name: DetectorName,
        filter: Option<Filter>,
        power: Option<Power>,
        detector_type: Option<DetectorType>,
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

#[derive(Debug, Error, new)]
#[error("could not convert from {from} to {to}: {inner}")]
pub struct ConvertError<E> {
    #[new(into)]
    from: Version,
    #[new(into)]
    to: Version,
    inner: ConvertErrorInner<E>,
}

#[derive(Debug, Display, Error)]
pub enum ConvertErrorInner<E> {
    Rewrap(IndexedElementError<E>),
    Meta(MetarootConvertError),
    Optical(IndexedElementError<OpticalConvertError>),
    Temporal(IndexedElementError<TemporalConvertError>),
    Layout(LayoutConvertError),
}

#[derive(Debug, Error)]
#[error("Some $PnN are blank and could not be converted")]
pub struct BlankShortnames;

#[derive(From, Display, Debug, Error)]
pub enum StdReaderError {
    Layout(NewDataLayoutError),
    Reader(NewDataReaderError),
}

#[derive(From, Display, Debug, Error)]
pub enum StdWriterError {
    Layout(NewDataLayoutError),
    Check(ColumnError<AnyLossError>),
    Overflow(Uint8DigitOverflow),
}

#[derive(From, Display, Debug, Error)]
pub enum StdWriterWarning {
    Column(ColumnError<IntRangeError<()>>),
    Check(ColumnError<AnyLossError>),
}

#[derive(From, Display, Debug, Error)]
pub enum ExistingLinkError {
    Named(ExistingNamedLinkError),
    Index(ExistingIndexLinkError),
}

#[derive(Debug, Error)]
pub enum ExistingNamedLinkError {
    #[error("$TR depends on existing $PnN")]
    Trigger,
    #[error("$UNSTAINEDCENTERS depends on existing $PnN")]
    UnstainedCenters,
    #[error("$SPILLOVER depends on existing $PnN")]
    Spillover,
}

#[derive(Debug, Error)]
pub enum ExistingIndexLinkError {
    #[error("$COMP refers to existing measurement by index")]
    Comp,
    // TODO which ones?
    #[error("$RnI refers to existing measurement by index")]
    GateRegion,
}

#[derive(From, Display, Debug, Error)]
pub enum SetSpilloverError {
    New(NewSpilloverError),
    Link(SpilloverLinkError),
}

#[derive(Debug, Error)]
#[error("all $SPILLOVER names must match a $PnN")]
pub struct SpilloverLinkError;

#[derive(Debug, Error)]
#[error("$TR measurement must match a $PnN")]
pub struct TriggerLinkError;

#[derive(From, Display, Debug, Error)]
pub enum SetMeasurementsError {
    New(NewNamedVecError),
    Link(ExistingLinkError),
    Layout(MeasLayoutMismatchError),
}

#[derive(From, Display, Debug, Error)]
pub enum SetScalesError {
    Layout(MeasLayoutMismatchError),
    Temporal(NonLinearTemporalScaleError),
}

#[derive(From, Display, Debug, Error)]
pub enum SetTransformsError {
    Layout(MeasLayoutMismatchError),
    Temporal(NonLinearTemporalTransformError),
}

#[derive(From, Display, Debug, Error)]
pub enum SetMeasurementsAndDataError {
    Meas(SetMeasurementsError),
    Mismatch(MeasDataMismatchError),
}

#[derive(From, Display, Debug, Error)]
pub enum ColumnsToDataframeError {
    New(df::NewDataframeError),
    Mismatch(MeasDataMismatchError),
}

#[derive(From, Display, Debug, Error)]
pub enum SetMeasurementsOnlyError {
    Meas(SetMeasurementsError),
    Mismatch(MeasDataMismatchError),
}

#[derive(From, Display, Debug, Error)]
pub enum RemoveMeasByNameError {
    Link(ExistingLinkError),
    Name(KeyNotFoundError),
}

#[derive(From, Display, Debug, Error)]
pub enum RemoveMeasByIndexError {
    Link(ExistingLinkError),
    Index(ElementIndexError),
}

#[derive(From, Display, Debug, Error)]
pub enum InsertTemporalError {
    Center(InsertCenterError),
    Layout(AnyRangeError),
}

#[derive(From, Display, Debug, Error)]
pub enum PushOpticalError {
    Unique(NonUniqueKeyError),
    Layout(AnyRangeError),
}

#[derive(From, Display, Debug, Error)]
pub enum InsertOpticalError {
    Insert(InsertError),
    Layout(AnyRangeError),
}

#[derive(From, Display, Debug, Error)]
pub enum PushTemporalToDatasetError {
    Measurement(InsertTemporalError),
    Column(df::ColumnLengthError),
}

#[derive(From, Display, Debug, Error)]
pub enum InsertTemporalToDatasetError {
    Measurement(InsertTemporalError),
    Column(df::ColumnLengthError),
}

#[derive(From, Display, Debug, Error)]
pub enum PushOpticalToDatasetError {
    Measurement(PushOpticalError),
    Column(df::ColumnLengthError),
}

#[derive(From, Display, Debug, Error)]
pub enum InsertOpticalInDatasetError {
    Measurement(InsertOpticalError),
    Column(df::ColumnLengthError),
}

#[derive(Debug, Error)]
#[error("measurement number ({meas_n}) does not match dataframe column number ({data_n})")]
pub struct MeasDataMismatchError {
    meas_n: usize,
    data_n: usize,
}

#[derive(Debug, Error)]
#[error("name {0} does not exist in measurements")]
pub struct MissingMeasurementNameError(Shortname);

#[derive(Debug, Error)]
#[error("tried to set temporal $PnE to nonlinear scale")]
pub struct NonLinearTemporalScaleError;

#[derive(Debug, Error)]
#[error("tried to set temporal $PnE/$PnG to nonlinear transform")]
pub struct NonLinearTemporalTransformError;

#[derive(From, Display, Debug, Error)]
pub enum StdTEXTFromKeywordsError {
    Error(StdTEXTFromRawError),
    Warn(StdTEXTFromRawWarning),
}

#[derive(From, Display, Debug, Error)]
pub enum StdTEXTFromRawError {
    Metaroot(LookupKeysError),
    Layout(Box<LookupLayoutError>),
    Offsets(LookupTEXTOffsetsError),
    Pseudostandard(PseudostandardError),
    Unused(UnusedStandardError),
}

#[derive(From, Display, Debug, Error)]
pub enum StdTEXTFromRawWarning {
    Metaroot(LookupKeysWarning),
    Meas(LookupMeasWarning),
    Layout(LookupLayoutWarning),
    Offsets(LookupTEXTOffsetsWarning),
    Pseudostandard(PseudostandardError),
    Unused(UnusedStandardError),
}

#[derive(From, Display, Debug, Error)]
pub enum StdDatasetFromRawError {
    TEXT(Box<StdTEXTFromRawError>),
    Dataframe(ReadDataframeError),
    Offsets(LookupTEXTOffsetsError),
    Warn(StdDatasetFromRawWarning),
    // Mismatch(DataSegmentMismatchError),
}

#[derive(From, Display, Debug, Error)]
pub enum StdDatasetFromRawWarning {
    TEXT(StdTEXTFromRawWarning),
    Offsets(LookupTEXTOffsetsWarning),
    Layout(ReadDataframeWarning),
    // Mismatch(DataSegmentMismatchError),
}

#[derive(From, Display, Debug, Error)]
pub enum LookupMeasWarning {
    Parse(LookupKeysWarning),
    Pattern(NonStdMeasRegexError),
}

// for now this just means $PnE isn't set and should be to convert
#[derive(Debug, Error)]
#[error("{} must be set before converting measurement", Scale::std(self.0))]
pub struct NoScaleError(MeasIndex);

#[derive(From, Display, Debug, Error)]
pub enum ReplaceTemporalError {
    ToOptical(TemporalToOpticalError),
    Set(SetCenterError),
}

#[derive(From, Display, Debug, Error)]
pub enum SetTemporalError {
    Swap(SwapOpticalTemporalError),
    Set(SetCenterError),
}

#[derive(From, Display, Debug, Error)]
pub enum SwapOpticalTemporalError {
    ToTemporal(OpticalToTemporalError),
    ToOptical(TemporalToOpticalError),
}

#[derive(From, Display, Debug, Error)]
pub enum OpticalToTemporalError {
    NonLinear(OpticalNonLinearError),
    Loss(AnyOpticalToTemporalKeyLossError),
}

#[derive(From, Display, Debug, Error)]
pub enum TemporalToOpticalError {
    Loss(AnyTemporalToOpticalKeyLossError),
}

#[derive(From, Display, Debug, Error)]
pub enum SetTemporalIndexError {
    Convert(OpticalToTemporalError),
    Index(SetCenterError),
}

#[derive(Debug, Error)]
#[error("$PnE must be '0,0' and $PnG must be null or unity to convert to temporal")]
pub struct OpticalNonLinearError;

#[derive(From, Display, Debug, Error)]
pub enum MetarootConvertError {
    NoCyt(NoCytError),
    Mode(ModeUpgradeError),
    GateLink(gating::RegionToGateIndexError),
    MeasLink(gating::RegionToMeasIndexError),
    GateToMeas(gating::GateToMeasIndexError),
    MeasToGate(gating::MeasToGateIndexError),
    Gates3_0To2_0(gating::AppliedGates3_0To2_0Error),
    Gates3_0To3_2(gating::AppliedGates3_0To3_2Error),
    Gates3_2To2_0(gating::AppliedGates3_2To2_0Error),
    Gates2_0To3_2(gating::AppliedGates2_0To3_2Error),
    Loss(AnyMetarootKeyLossError),
    Comp2_0(Comp2_0TransferError),
}

#[derive(From, Display, Debug, Error)]
pub enum MetarootConvertWarning {
    Mode(ModeUpgradeError),
    Gates3_0To2_0(gating::AppliedGates3_0To2_0Error),
    Gates3_0To3_2(gating::AppliedGates3_0To3_2Error),
    Gates3_2To2_0(gating::AppliedGates3_2To2_0Error),
    Gates2_0To3_2(gating::AppliedGates2_0To3_2Error),
    Loss(AnyMetarootKeyLossError),
    Optical(OpticalConvertWarning),
    Temporal(TemporalConvertError),
    Comp2_0(Comp2_0TransferError),
}

/// Error when a metaroot keyword will be lost when converting versions
#[derive(From, Display, Debug, Error)]
pub enum AnyMetarootKeyLossError {
    Cytsn(UnitaryKeyLossError<Cytsn>),
    Unicode(UnitaryKeyLossError<Unicode>),
    Vol(UnitaryKeyLossError<Vol>),
    Flowrate(UnitaryKeyLossError<Flowrate>),
    Comp(UnitaryKeyLossError<Compensation3_0>),
    Platename(UnitaryKeyLossError<Platename>),
    Plateid(UnitaryKeyLossError<Plateid>),
    Wellid(UnitaryKeyLossError<Wellid>),
    Carrierid(UnitaryKeyLossError<Carrierid>),
    Locationid(UnitaryKeyLossError<Locationid>),
    Carriertype(UnitaryKeyLossError<Carriertype>),
    LastModifier(UnitaryKeyLossError<LastModifier>),
    LastModified(UnitaryKeyLossError<LastModified>),
    Originality(UnitaryKeyLossError<Originality>),
    UnstainedCenters(UnitaryKeyLossError<UnstainedCenters>),
    UnstainedInfo(UnitaryKeyLossError<UnstainedInfo>),
    Begindatetime(UnitaryKeyLossError<BeginDateTime>),
    Enddatetime(UnitaryKeyLossError<EndDateTime>),
    Spillover(UnitaryKeyLossError<Spillover>),
    CSMode(UnitaryKeyLossError<CSMode>),
    CSVBits(UnitaryKeyLossError<CSVBits>),
    CSVFlag(IndexedKeyLossError<CSVFlag>),
}

/// Error when an optical keyword will be lost when converting versions
#[derive(From, Display, Debug, Error)]
pub enum AnyMeasKeyLossError {
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
    PeakBin(IndexedKeyLossError<PeakBin>),
    PeakNumber(IndexedKeyLossError<PeakNumber>),
    Calibration3_1(IndexedKeyLossError<Calibration3_1>),
    Calibration3_2(IndexedKeyLossError<Calibration3_2>),
}

/// Error when an optical keyword will be lost when converting to temporal
#[derive(From, Display, Debug, Error)]
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
#[derive(From, Display, Debug, Error)]
pub enum AnyTemporalToOpticalKeyLossError {
    TempType(IndexedKeyLossError<TemporalType>),
}

#[derive(From, Display, Debug, Error)]
pub enum LookupAndReadDataAnalysisError {
    Offsets(LookupTEXTOffsetsError),
    Layout(RawToLayoutError),
    Dataframe(ReadDataframeError),
    Warn(LookupAndReadDataAnalysisWarning),
    // Mismatch(DataSegmentMismatchError),
}

#[derive(From, Display, Debug, Error)]
pub enum LookupAndReadDataAnalysisWarning {
    Offsets(LookupTEXTOffsetsWarning),
    Layout(RawToLayoutWarning),
    Data(ReadDataframeWarning),
    // Mismatch(DataSegmentMismatchError),
}

#[derive(From, Display, Debug, Error)]
pub enum LookupTEXTOffsetsWarning {
    Tot(OptKeyError<ParseIntError>),
    ReqData(ReqSegmentWithDefaultWarning<DataSegmentId>),
    ReqAnalysis(ReqSegmentWithDefaultWarning<AnalysisSegmentId>),
    MismatchAnalysis(OptSegmentWithDefaultWarning<AnalysisSegmentId>),
}

#[derive(From, Display, Debug, Error)]
pub enum LookupTEXTOffsetsError {
    Tot(ReqKeyError<ParseIntError>),
    ReqData(ReqSegmentWithDefaultError<DataSegmentId>),
    ReqAnalysis(ReqSegmentWithDefaultError<AnalysisSegmentId>),
    MismatchData(SegmentMismatchWarning<DataSegmentId>),
    MismatchAnalysis(SegmentMismatchWarning<AnalysisSegmentId>),
}

#[derive(From, Display, Debug, Error)]
pub enum NewCoreError {
    Meas(NewNamedVecError),
    Link(ExistingLinkError),
    Layout(MeasLayoutMismatchError),
}

#[derive(From, Display, Debug, Error)]
pub enum NewCoreTEXTError {
    Core(NewCoreError),
    Timestamps(ReversedTimestampsError),
    Datetimes(ReversedDatetimesError),
}

type LookupTEXTOffsetsResult<T> =
    DeferredResult<T, LookupTEXTOffsetsWarning, LookupTEXTOffsetsError>;

#[derive(Debug, Error)]
#[error("$DFCiTOj keywords are set and not applicable to the target version")]
pub struct Comp2_0TransferError;

#[derive(Debug, Error, Display, new)]
#[display(bound(T: Key))]
#[display("{} is set but is not applicable to target version", T::std())]
pub struct UnitaryKeyLossError<T>(PhantomData<T>);

#[derive(Debug, Error, Display, new)]
#[display(bound(T: IndexedKey))]
#[display("{} is set but is not applicable to target version", T::std(*_1))]
pub struct IndexedKeyLossError<T>(PhantomData<T>, #[new(into)] IndexFromOne);

#[derive(Debug, Error)]
#[error("number of columns is {this_len}, input should match but got {other_len}")]
pub struct ColumnNumberError {
    this_len: usize,
    other_len: usize,
}

#[derive(Debug, Error)]
#[error(
    "could not make scale transform with log scale \
     '{scale}' and non-unit gain '{gain}'"
)]
pub struct ScaleTransformError {
    scale: Scale,
    gain: Gain,
}

#[derive(Debug, Error)]
#[error("$COMP must have same row/column number as $PAR ({par}), got {comp}")]
pub struct CompParMismatchError {
    par: usize,
    comp: usize,
}

#[derive(Debug, Error)]
#[error("$RnI references non-existed measurements by index: {}", .0.iter().join(","))]
pub struct GatingMeasLinkError(NonEmpty<MeasIndex>);

#[derive(Debug, Error)]
#[error("$CSVnFLAGS must not be empty")]
pub struct NewCSVFlagsError;

def_failure!(ConvertFailure, "could not change FCS version");

def_failure!(SetLayoutFailure, "could not set data layout");

def_failure!(PushTemporalFailure, "could not push temporal measurement");

def_failure!(
    SetScalesFailure,
    "could not set scales for optical measurements"
);

def_failure!(
    SetTransformsFailure,
    "could not set scale transforms for optical measurements"
);

def_failure!(SetTemporalFailure, "could not set temporal measurement");

def_failure!(
    ReplaceTemporalFailure,
    "could not replace temporal measurement"
);

def_failure!(UnsetTemporalFailure, "could not unset temporal measurement");

def_failure!(InsertTemporalFailure, "could not push temporal measurement");

def_failure!(PushOpticalFailure, "could not push optical measurement");

def_failure!(InsertOpticalFailure, "could not push optical measurement");

def_failure!(
    SetOpticalFailure,
    "could not set values for optical measurements"
);

def_failure!(SetMeasurementsFailure, "could not set measurements");

def_failure!(
    SetMeasurementsAndLayoutFailure,
    "could not set measurements and layout"
);

def_failure!(
    SetMeasurementsAndDataFailure,
    "could not set measurements and data"
);

def_failure!(SetUnstainedFailure, "could not set $UNSTAINEDCENTERS");

def_failure!(WriteTEXTFailure, "could not write HEADER and TEXT segments");

def_failure!(WriteDatasetFailure, "could not write FCS file");

def_failure!(
    CoreTEXTFromKeywordsFailure,
    "could not create new CoreTEXT from keywords"
);

def_failure!(
    StdDatasetWithKwsFailure,
    "could not read standardized dataset from keywords"
);

#[cfg(feature = "serde")]
mod serialize {
    use crate::core::AnyCore;
    use serde::{ser::SerializeStruct as _, Serialize};

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
    use crate::python::exceptions::PyreflowException;
    use crate::python::macros::{impl_from_py_transparent, impl_pyreflow_err};
    use crate::text::ranged_float::PositiveFloat;
    use crate::validated::dataframe::python::SeriesToColumnError;

    use super::{
        Analysis, CSVFlags, ColumnsToDataframeError, CompParMismatchError, ExistingLinkError,
        GatingMeasLinkError, MeasDataMismatchError, MissingMeasurementNameError, NewCoreTEXTError,
        Other, Others, RemoveMeasByIndexError, RemoveMeasByNameError, ScaleTransform,
        SetMeasurementsError, SpilloverLinkError, TriggerLinkError,
    };

    use derive_more::{Display, From};
    use pyo3::exceptions::{PyIndexError, PyValueError};
    use pyo3::prelude::*;
    use pyo3::IntoPyObjectExt as _;

    impl_from_py_transparent!(Analysis);
    impl_from_py_transparent!(Other);
    impl_from_py_transparent!(Others);
    impl_from_py_transparent!(CSVFlags);

    // $PnE/$PnG (3.0+) as a tuple like (f32) or (f32, f32) in python
    impl<'py> FromPyObject<'py> for ScaleTransform {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            if let Ok(gain) = ob.extract::<PositiveFloat>() {
                Ok(Self::Lin(gain))
            } else if let Ok(log) = ob.extract::<(f32, f32)>() {
                Ok(Self::Log(log.try_into()?))
            } else {
                // TODO make this into a general "argument value error"
                Err(PyValueError::new_err(
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

    #[derive(From, Display)]
    pub enum SetMeasurementsAndDataframeError {
        Meas(SetMeasurementsError),
        DataFrame(SeriesToColumnError),
        Mismatch(MeasDataMismatchError),
    }

    impl_pyreflow_err!(MeasDataMismatchError);
    impl_pyreflow_err!(SetMeasurementsAndDataframeError);
    impl_pyreflow_err!(ColumnsToDataframeError);
    impl_pyreflow_err!(MissingMeasurementNameError);
    impl_pyreflow_err!(ExistingLinkError);
    impl_pyreflow_err!(SpilloverLinkError);
    impl_pyreflow_err!(CompParMismatchError);
    impl_pyreflow_err!(TriggerLinkError);
    impl_pyreflow_err!(GatingMeasLinkError);
    impl_pyreflow_err!(NewCoreTEXTError);

    impl From<RemoveMeasByIndexError> for PyErr {
        fn from(value: RemoveMeasByIndexError) -> Self {
            match value {
                RemoveMeasByIndexError::Link(x) => PyreflowException::new_err(x.to_string()),
                RemoveMeasByIndexError::Index(x) => PyIndexError::new_err(x.to_string()),
            }
        }
    }

    impl From<RemoveMeasByNameError> for PyErr {
        fn from(value: RemoveMeasByNameError) -> Self {
            match value {
                RemoveMeasByNameError::Link(x) => PyreflowException::new_err(x.to_string()),
                RemoveMeasByNameError::Name(x) => PyIndexError::new_err(x.to_string()),
            }
        }
    }
}

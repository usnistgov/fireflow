//! Top-level functions for parsing FCS files
use crate::config::{
    AppendRepairFlagError, EvaledReadRepairKeywordsConfig, ReadFlatDatasetConfig,
    ReadFlatDatasetFromKeywordsConfig, ReadFlatTEXTConfig, ReadHeaderConfig,
    ReadRepairKeywordsConfig, ReadStdDatasetConfig, ReadStdKeywordsConfig, ReadStdTEXTConfig,
    WriteMultiDatasetConfig, eval_repair_conf,
};
use crate::convert::{InstantExt as _, UsizeExt as _};
use crate::core::{
    Analysis, AnyCoreDataset, AnyCoreTEXT, AnyStdDatasetFromKeywordsError,
    AnyStdTEXTFromKeywordsError, CRCOutput, DatasetDiagnostics, DatasetOffsets,
    LookupAndReadDataAnalysisError, LookupAndReadDataAnalysisWarning, Others, PrivVersionSet as _,
    StdDatasetFromKeywordsWarningInner, StdDatasetFromKwsOutput, StdTEXTDiagnostics,
    StdTEXTFromKeywordsWithOffsetsWarning, StdWriterError, WriteDatasetSummary,
};
use crate::data::{DataSchemaDiagnostics, EventOverRangeError};
use crate::fixed_vec::OneOrTwo;
use crate::header::{
    GuessVersionError, Header, HeaderError, KeywordVersionScores, autodetect_version,
};
use crate::logging::{
    DeferredErrors, DeferredWarningsAndErrors, IOAnonErrorGroup, IOErrorGroup, IOResult,
    ImpureError, LogResult, ResultExt as _, SuccessResultIter as _, SwitchableErrorResult,
    SwitchableErrorsResult, WarningAndErrorResult, WarningsAndErrorResult, WarningsAndErrorsResult,
    WarningsAndIOGroupResult, io_to_log, split_log,
};
use crate::macros::def_summary;
use crate::segment::read::{
    AnyRegion, AreNamedOffsets, DatasetOverflowError, GuessOtherWidthError, HasOneName as _,
    HasRegion, HeaderOffsetsName, HeaderOffsetsOverflow, IsDataOrAnalysis, IsOffsetPair as _,
    KeyedOptSegment as _, KeyedReqSegment as _, NonEmptyOffsets, OffsetPairsOverlapError,
    OffsetsOverlap, OptOffsetsError, OptSegmentKeyError, OriginalOffsets, PairResult,
    PrimaryTextOffsets, ReqOffsetsError, ReqSegmentKeyError, SuppOffsetsOverflow,
    SuppTextOffsetsName, SuppToHeaderOffsetsOverlap, SupplementalTextOffsets, TEXTOffsets,
    TextOffsetsName, TextToHeaderOrSuppOffsetsOverlap,
};
use crate::std_index::index::{RepairDiagnostics, RepairError, StdKeywords, StdLookupTx};
use crate::text::keywords::{
    AlphaNumType, Beginstext, Endstext, LookupNextdataError, Nextdata, ReadNextdataError, Tot,
};
use crate::text::lookup::{MissingKeyError, ParseKeyError, ReqKeyErrorInner_};
use crate::validated::dataframe::PrimitiveDataFrame;
use crate::validated::header_offsets::{
    FinalHeaderOffsets, OffsetsValidationError, PrimaryTEXTOverflowError,
    SuppToHeaderOffsetsValidationError, TextToHeaderOrSuppOffsetsValidationError,
};
use crate::validated::keys::{
    AnyKey, DollarKeyOrBytes, NEDelimBytes, NEStringOrBytes, NonStdKey, ParsedKeyword,
    ParsedKeywordCounts, ParsedKeywordsDiagnostic, ParsedNonStdKeywords, PseudoStdKeywords,
    StringOrBytes, TruncatedNEBytes, TruncatedNEString, ValidKeywords, ValueToStdKey,
};
use crate::validated::read_state::{
    CRCError, DatasetLen, DatasetLenEOFError, DatasetOffset, DatasetOffsetError, FileLen,
    HeaderReadState, TEXTReadState,
};

use fireflow_types::config::{
    AppendFlag, AppendableFlag, ConfigFlag as _, DelimEscapeMode, Encoding, OverlapCorrectionLimit,
    ReadDataKeywordsConfig, ReadDatasetConfig, ReadHeaderAndTEXTConfig, ReadHeaderInnerConfig,
    ReadOffsetConfig, ReadSharedConfig, TriErrorFlag as _, VersionOverride,
    WriteDatasetInnerConfig, WriteMultiConfig,
};
use fireflow_types::keywords::{Version, Version2_0, Version3_0, Version3_1, Version3_2};
use fireflow_types::nonempty::{
    IntoIteratorExt as _, NESlice, NEStr, NEVec, NonEmptyIterator as _,
};
use fireflow_types::segment::{OffsetsFromTEXT, SupplementalTextSegmentId};
use fireflow_types::std_key::{DollarPseudoStdKey, DollarStdKey, RootKey, ToStd as _};

use type_families::{ApplyOnce as _, BifunctorOnce, Functor as _, FunctorOnce as _};

use derive_more::{AsRef, Display, From};
use derive_new::new;
use itertools::Itertools as _;
use thiserror::Error;

use std::fmt;
use std::fs::{self, File};
use std::io::{self, BufReader, Read, Seek};
use std::iter;
use std::mem;
use std::num::{NonZeroUsize, ParseIntError};
use std::path::PathBuf;
use std::time::Instant;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr},
    fireflow_types::python as py,
    pyo3::exceptions::PyValueError,
    pyo3::prelude::*,
};

/// Dump version and build information
#[must_use]
pub fn build_info() -> BuildInfo {
    BuildInfo {
        version: built::PKG_VERSION,
        commit_hash: built::GIT_COMMIT_HASH,
        build_date: built::BUILT_TIME_UTC,
        rustc_version: built::RUSTC_VERSION,
        target: built::TARGET,
        is_debug: cfg!(debug_assertions),
        opt_level: built::OPT_LEVEL,
        target_features: env!("FIREFLOW_TARGET_FEATURES"),
        // features: built::FEATURES_STR,
    }
}

/// Read HEADER from an FCS file.
pub fn fcs_read_header(
    path: &PathBuf,
    dataset_offset: DatasetOffset,
    conf: &ReadHeaderConfig,
) -> WarningsAndIOGroupResult<Header, GuessOtherWidthError, ReadHeaderError, HeaderSummary> {
    let start_time = Instant::now();
    let mut fr = io_to_log!(FCSFileReader::open(path));
    fr.as_read_dataset_state(dataset_offset, start_time, conf)
        .map_err(ReadHeaderError::from)
        .map_err(IOAnonErrorGroup::new_pure_one)
        .into_log()
        .and_then_commutative(|mut st| {
            Header::h_read(&mut fr.buf_read, &mut st)
                .map_error(|e| e.fmap(ReadHeaderError::from))
                .map_ok_value(|out| out.header)
            // .warnings_to_pure_errors(&conf.shared, ReadHeaderError::from)
        })
        .deanonymize()
}

/// Read HEADER and key/value pairs from TEXT in an FCS file at a given position
#[must_use]
pub fn fcs_read_flat_text(
    path: &PathBuf,
    dataset_offset: DatasetOffset,
    conf: &ReadFlatTEXTConfig,
) -> WarningsAndIOGroupResult<
    FlatTEXTOutput,
    HeaderOrFlatTEXTWarning,
    HeaderOrFlatTextError,
    FlatTEXTSummary,
> {
    let start_time = Instant::now();
    let mut fr = io_to_log!(FCSFileReader::open(path));
    fr.read_flat_text(dataset_offset, start_time, conf)
}

/// Read HEADER and standardized TEXT at a given position from an FCS file.
#[must_use]
pub fn fcs_read_std_text(
    path: &PathBuf,
    dataset_offset: DatasetOffset,
    conf: &ReadStdTEXTConfig,
) -> WarningsAndIOGroupResult<
    (AnyCoreTEXT, StdTEXTOutput),
    StdTEXTWarning,
    StdTEXTError,
    StdTEXTSummary,
> {
    let start_time = Instant::now();
    let mut fr = io_to_log!(FCSFileReader::open(path));
    fr.read_std_text(dataset_offset, start_time, conf)
}

/// Read dataset from FCS at given position file using flat TEXT.
#[must_use]
pub fn fcs_read_flat_dataset(
    path: &PathBuf,
    dataset_offset: DatasetOffset,
    scan_next_dataset: bool,
    conf: &ReadFlatDatasetConfig,
) -> WarningsAndIOGroupResult<
    FlatDatasetOutput,
    ReadFlatDatasetWarning,
    ReadFlatDatasetError,
    FlatDatasetSummary,
> {
    let start_time = Instant::now();
    let mut fr = io_to_log!(FCSFileReader::open(path));
    fr.read_flat_dataset(dataset_offset, scan_next_dataset, start_time, conf)
}

/// Read dataset from FCS file at given position using standardized TEXT.
#[must_use]
pub fn fcs_read_std_dataset(
    path: &PathBuf,
    dataset_offset: DatasetOffset,
    scan_next_dataset: bool,
    conf: &ReadStdDatasetConfig,
) -> WarningsAndIOGroupResult<
    (AnyCoreDataset, StdDatasetOutput),
    StdDatasetWarning,
    StdDatasetError,
    StdDatasetSummary,
> {
    let start_time = Instant::now();
    let mut fr = io_to_log!(FCSFileReader::open(path));
    fr.read_std_dataset(dataset_offset, scan_next_dataset, start_time, conf)
}

/// Read DATA/ANALYSIS in FCS file using provided keywords.
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn fcs_read_flat_dataset_with_keywords(
    path: &PathBuf,
    mut hns: HeaderAndSuppOffsets,
    std: &StdKeywords,
    dataset_offset: DatasetOffset,
    dataset_len: Option<DatasetLen>,
    conf: &ReadFlatDatasetFromKeywordsConfig,
) -> WarningsAndIOGroupResult<
    NewFlatDatasetFromKwsOutput,
    ReadFlatDatasetFromKwsOutputWarning,
    ReadFlatDatasetWithKwsError,
    FlatDatasetWithKwsSummary,
> {
    let start_time = Instant::now();
    FCSFileReader::open_with_state(path, dataset_offset, start_time, conf)
        .map_err(|e| e.fmap_once(ReadFlatDatasetWithKwsError::from))
        .and_then(|(fr, st)| {
            st.maybe_with_dataset_length(dataset_len)
                .map(|txt_st| (txt_st, fr))
                .map_err(ReadFlatDatasetWithKwsError::from)
                .map_err(ImpureError::Pure)
        })
        .map_err(IOErrorGroup::from)
        .into_log()
        .and_then_commutative(|(txt_st, mut fr)| {
            let br = &mut fr.buf_read;
            let v = hns.header.version;
            let tx = std.as_transaction();
            let st = txt_st.start_time();
            FlatDatasetFromKwsOutput::h_read(br, v, &tx, &mut hns, false, st, &txt_st)
                .map_pure_errors(ReadFlatDatasetWithKwsError::from)
        })
        .map_ok_value(|dataset| NewFlatDatasetFromKwsOutput::new(dataset, hns.header.final_offsets))
        .warnings_to_pure_errors(conf.shared, ReadFlatDatasetWithKwsError::from)
        .deanonymize()
}

/// Read HEADER and TEXT from multiple datasets in flat mode.
#[must_use]
pub fn fcs_read_flat_texts(
    path: &PathBuf,
    skip: Option<usize>,
    limit: Option<usize>,
    scan: bool,
    conf: &ReadFlatTEXTConfig,
) -> WarningsAndIOGroupResult<
    Vec<FlatTEXTOutput>,
    HeaderOrFlatTEXTWarning,
    HeaderOrFlatTextError,
    FlatTEXTSummary,
> {
    let start_time = Instant::now();
    let mut fr = io_to_log!(FCSFileReader::open(path));
    fr.read_flat_texts(skip, limit, scan, start_time, conf)
}

/// Read HEADER and TEXT from multiple datasets in standardized mode.
#[must_use]
pub fn fcs_read_std_texts(
    path: &PathBuf,
    skip: Option<usize>,
    limit: Option<usize>,
    scan: bool,
    conf: &ReadStdTEXTConfig,
) -> WarningsAndIOGroupResult<
    Vec<(AnyCoreTEXT, StdTEXTOutput)>,
    MultiStdTEXTWarning,
    MultiStdTEXTError,
    StdTEXTSummary,
> {
    let start_time = Instant::now();
    let mut fr = io_to_log!(FCSFileReader::open(path));
    fr.read_std_texts(skip, limit, scan, start_time, conf)
}

/// Read multiple datasets from FCS file in flat mode.
#[must_use]
pub fn fcs_read_flat_datasets(
    path: &PathBuf,
    skip: Option<usize>,
    limit: Option<usize>,
    scan: bool,
    conf: &ReadFlatDatasetConfig,
) -> WarningsAndIOGroupResult<
    Vec<FlatDatasetOutput>,
    MultiFlatDatasetWarning,
    MultiFlatDatasetError,
    FlatDatasetSummary,
> {
    let start_time = Instant::now();
    let mut fr = io_to_log!(FCSFileReader::open(path));
    fr.read_flat_datasets(skip, limit, scan, start_time, conf)
}

/// Read multiple datasets from FCS file
#[must_use]
pub fn fcs_read_std_datasets(
    path: &PathBuf,
    skip: Option<usize>,
    limit: Option<usize>,
    scan: bool,
    conf: &ReadStdDatasetConfig,
) -> WarningsAndIOGroupResult<
    Vec<(AnyCoreDataset, StdDatasetOutput)>,
    MultiStdDatasetWarning,
    MultiStdDatasetError,
    StdDatasetSummary,
> {
    let start_time = Instant::now();
    let mut fr = io_to_log!(FCSFileReader::open(path));
    fr.read_std_datasets(skip, limit, scan, start_time, conf)
}

/// Summarize the contents of an FCS file
#[must_use]
pub fn fcs_summarize(
    path: &PathBuf,
    skip: Option<usize>,
    limit: Option<usize>,
    scan: bool,
    conf: &ReadFlatDatasetConfig,
) -> WarningsAndIOGroupResult<
    Vec<DatasetSummary>,
    MultiFlatDatasetWarning,
    MultiFlatDatasetError,
    FlatDatasetSummary,
> {
    fcs_read_flat_datasets(path, skip, limit, scan, conf)
        .map_ok_value(|x| x.fmap(FlatDatasetOutput::summarize))
}

/// Scan through an FCS file and look for the starting offset of a dataset.
///
/// This is useful for situations where the $NEXTDATA keyword cannot be trusted
/// and one suspects that there may be multiple datasets in a file.
///
/// Specifically, this will look for the pattern "FCS2.0|FCS3.0|FCS3.1|FCS3.2"
/// followed by 4 spaces.
pub fn fcs_scan_dataset_boundaries(path: &PathBuf) -> io::Result<Vec<(Version, DatasetOffset)>> {
    // General strategy:
    // 1. take overlapping buffers of some size
    // 2. iterate over all overlapping windows in this buffer
    // 3. test each window against the version pattern for a match
    const OVERLAP_SIZE: usize = BOUNDARY_MATCH_SIZE - 1;
    const BUF_SIZE: u64 = 32000;

    let mut file = fs::File::options().read(true).open(path)?;
    let mut bounds = vec![];
    let mut buf = vec![];
    let mut file_pos = 0;
    file.by_ref().take(BUF_SIZE).read_to_end(&mut buf)?;

    while buf.len() >= BOUNDARY_MATCH_SIZE {
        for w in buf[..].array_windows() {
            if let Some(v) = match_bytes_version(w) {
                bounds.push((v, DatasetOffset(file_pos)));
            }
            file_pos += 1;
        }
        // Shift the last WINDOW_SIZE - 1 bytes from the end to the front of
        // the buffer. We don't want the full window size because that would
        // double-count the last window in the buffer.
        let mut tmp = [0_u8; OVERLAP_SIZE];
        tmp.copy_from_slice(&buf[buf.len() - OVERLAP_SIZE..]);
        buf.clear();
        buf.extend(tmp);
        file.by_ref()
            .take(BUF_SIZE - OVERLAP_SIZE.usize_to_u64())
            .read_to_end(&mut buf)?;
    }

    Ok(bounds)
}

const BOUNDARY_MATCH_SIZE: usize = 10;

fn match_bytes_version(xs: &[u8; BOUNDARY_MATCH_SIZE]) -> Option<Version> {
    match xs {
        b"FCS2.0    " => Some(Version::FCS2_0),
        b"FCS3.0    " => Some(Version::FCS3_0),
        b"FCS3.1    " => Some(Version::FCS3_1),
        b"FCS3.2    " => Some(Version::FCS3_2),
        _ => None,
    }
}

pub(crate) fn next_dataset_boundary<R: Read + Seek>(
    h: &mut BufReader<R>,
) -> io::Result<Option<DatasetOffset>> {
    const OVERLAP_SIZE: usize = BOUNDARY_MATCH_SIZE - 1;
    const BUF_SIZE: u64 = 32000;

    let mut buf = vec![];
    let mut file_offset = h.stream_position()?;
    h.by_ref().take(BUF_SIZE).read_to_end(&mut buf)?;

    while buf.len() >= BOUNDARY_MATCH_SIZE {
        for w in buf[..].array_windows() {
            if match_bytes_version(w).is_some() {
                return Ok(Some(DatasetOffset(file_offset)));
            }
            file_offset += 1;
        }
        let mut tmp = [0_u8; OVERLAP_SIZE];
        tmp.copy_from_slice(&buf[buf.len() - OVERLAP_SIZE..]);
        buf.clear();
        buf.extend(tmp);
        h.by_ref()
            .take(BUF_SIZE - OVERLAP_SIZE.usize_to_u64())
            .read_to_end(&mut buf)?;
    }

    Ok(None)
}

/// Write multiple FCS datasets (of any version) to a file.
#[must_use]
pub fn fcs_write_datasets(
    path: &PathBuf,
    cores: &[AnyCoreDataset],
    conf: &WriteDatasetInnerConfig,
) -> WarningsAndIOGroupResult<
    Option<Nextdata>,
    EventOverRangeError,
    StdWriterError,
    WriteDatasetSummary,
> {
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

/// Version and build information for this libary
#[derive(Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct BuildInfo {
    pub version: &'static str,
    pub commit_hash: Option<&'static str>,
    pub build_date: &'static str,
    pub rustc_version: &'static str,
    pub target: &'static str,
    pub is_debug: bool,
    pub opt_level: &'static str,
    pub target_features: &'static str,
    // this will be good to include if/once we actually add features that make
    // sense to toggle
    //
    // pub features: &'static str,
}

/// Output from parsing the TEXT segment.
#[derive(Clone, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct FlatTEXTOutput {
    /// Keywords from TEXT
    pub keywords: ValidKeywords,

    /// Miscellaneous data from parsing TEXT
    pub flat_diagnostics: FlatTEXTDiagnostics,
}

/// Output of parsing the TEXT segment and standardizing keywords.
#[derive(Clone, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct StdTEXTOutput {
    /// TEXT value for $TOT
    ///
    /// This should always be Some for 3.0+ and might be None for 2.0.
    pub tot: Option<Tot>,

    /// Offsets for DATA and ANALYSIS
    pub dataset_offsets: DatasetOffsets,

    /// Diagnostic output from TEXT standardization
    pub std_diagnostics: StdTEXTDiagnostics,

    /// Diagnostic output from flat TEXT parsing
    pub flat_diagnostics: FlatTEXTDiagnostics,

    /// Diagnostic output from repairing the keyword list
    pub repair_diagnostics: RepairDiagnostics,

    /// Scores generated if version was guessed.
    pub version_scores: Option<KeywordVersionScores>,

    /// Keywords which start with a '$' but are not part of any standard.
    pub pseudostandard: PseudoStdKeywords,
}

/// Output of parsing one flat dataset (TEXT+DATA) from an FCS file.
#[derive(Clone, new, PartialEq)]
pub struct FlatDatasetOutput {
    /// Standard and nonstandard keywords.
    pub keywords: ValidKeywords,

    /// Output from parsing HEADER+TEXT
    pub flat_diagnostics: FlatTEXTDiagnostics,

    /// Output from parsing DATA+ANALYSIS
    pub dataset: FlatDatasetFromKwsOutput,

    /// Scores generated if version was guessed.
    pub version_scores: Option<KeywordVersionScores>,

    /// Diagnostic output from repairing the keyword list
    pub repair_diagnostics: RepairDiagnostics,
}

/// Output of parsing one standardized dataset (TEXT+DATA) from an FCS file.
#[derive(Clone, new, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct StdDatasetOutput {
    /// Standardized data from one FCS dataset
    pub dataset: StdDatasetFromKwsOutput,

    /// Miscellaneous data from parsing TEXT
    pub flat_diagnostics: FlatTEXTDiagnostics,

    /// Scores generated if version was guessed.
    pub version_scores: Option<KeywordVersionScores>,

    /// Diagnostic output from repairing the keyword list
    pub repair_diagnostics: RepairDiagnostics,

    /// Keywords which start with a '$' but are not part of any standard.
    pub pseudostandard: PseudoStdKeywords,
}

/// Output of using keywords to crate new flat TEXT+DATA
#[derive(Clone, new, PartialEq)]
pub struct NewFlatDatasetFromKwsOutput {
    /// Standardized data from one FCS dataset
    pub dataset: FlatDatasetFromKwsOutput,

    /// (Possibly modified) offsets used to parse HEADER.
    pub header: FinalHeaderOffsets,
}

/// Output when making flat TEXT+DATA
#[derive(Clone, PartialEq, new)]
#[allow(clippy::too_many_arguments)]
pub struct FlatDatasetFromKwsOutput {
    /// DATA output
    pub data: PrimitiveDataFrame,

    /// ANALYSIS output
    pub analysis: Analysis,

    /// OTHER output(s)
    pub others: Others,

    /// Offsets used to parse DATA and ANALYSIS
    pub dataset_offsets: DatasetOffsets,

    /// Diagnostic output from parsing the data schema.
    pub schema_diagnostics: DataSchemaDiagnostics,

    /// Diagnostic output from parsing entire dataset.
    pub dataset_diagnostics: DatasetDiagnostics,
}

// TODO should all these std/nonstd keys just be keystrings since the $ is implied?
/// Data pertaining to parsing the TEXT segment.
#[allow(clippy::too_many_arguments)]
#[derive(new, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct FlatTEXTDiagnostics {
    /// HEADER data and supplemental TEXT offsets
    pub header_supp: HeaderAndSuppOffsets,

    // TODO add original $NEXTDATA (so we can see if it was corrected).
    /// Amount by which which primary TEXT exceeded EOF.
    pub primary_text_overflow: u64,

    /// Amounts by which non-primary-TEXT HEADER offsets exceeded the dataset length.
    pub header_overflows: Vec<HeaderOffsetsOverflow>,

    /// The total time in nanoseconds it took to read TEXT.
    pub read_text_ns: u128,

    /// Output from splitting primary TEXT
    pub primary_split: SplitTEXTDiagnostics,

    /// Output from splitting supplemental TEXT
    pub supp_split: Option<SplitTEXTDiagnostics>,
}

/// HEADER data and supplemental offsets.
///
/// These are together because reading DATA and ANALYSIS from TEXT needs to be
/// validated against everything here. Offsets here may even be modified.
/// Keeping this together makes this easier.
#[derive(new, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct HeaderAndSuppOffsets {
    /// HEADER as parsed from dataset in file.
    pub header: Header,

    /// Supplemental TEXT offsets and their reason for exclusion if not present.
    pub supp_text: SuppTEXTOffsetsOutput,

    /// NEXTDATA offset
    ///
    /// This will be copied as represented in TEXT. If it is 0, there is no next
    /// dataset, otherwise it points to the next dataset in the file.
    pub nextdata: Option<Nextdata>,
}

/// The supplemental TEXT offsets from a file after parsing.
#[derive(Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum SuppTEXTOffsetsOutput {
    /// No offsets.
    ///
    /// This will always be returned for 2.0 files. 3.2 files may return this
    /// if the offsets are missing since they are optional.
    Empty,
    /// Offsets required but could not be parsed.
    Unparsed,
    /// Offsets required but were numerically malformed.
    Malformed(OriginalOffsets),
    /// Offsets present but perfectly duplicated primary TEXT and thus were ignored.
    DuplicatesPrimaryTEXT,
    /// Offsets present but perfectly duplicated ANALYSIS and thus were ignored.
    DuplicatesAnalysis,
    /// Offsets present but ignored by user configuration.
    Ignored(Option<OriginalOffsets>),
    /// Offsets present and valid.
    Valid(ValidSuppTEXTOffsets),
}

#[derive(Clone, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct ValidSuppTEXTOffsets {
    /// The final offsets used to read supplemental TEXT.
    final_: SupplementalTextOffsets,
    /// The original offsets as written in the file.
    original: OriginalOffsets,
    /// The index of the OTHER offsets that exactly replicates this if applicable.
    duplicated_other: Option<usize>,
    /// Overlaps between supp TEXT and other offsets in HEADER.
    overlaps: Vec<SuppToHeaderOffsetsOverlap>,
    /// Amount the offset exceeds $NEXTDATA or EOF if applicable
    overflow: Option<SuppOffsetsOverflow>,
}

/// Data pertaining to parsing the TEXT segment.
#[derive(new, Clone, PartialEq)]
#[allow(clippy::too_many_arguments)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct SplitTEXTDiagnostics {
    /// Delimiter used to parse TEXT.
    ///
    /// Included here for informational purposes.
    pub delimiter: u8,

    /// `true` if TEXT delimiters were escaped
    pub escaped: bool,

    /// Valid keys with non-UTF8 values.
    pub keys_with_non_utf8_values: Vec<(AnyKey, TruncatedNEBytes)>,

    /// Valid values with non-ASCII keys.
    pub values_with_non_ascii_keys: Vec<(TruncatedNEBytes, TruncatedNEString)>,

    /// Keywords that could not be parsed.
    ///
    /// These have either a non-ASCII key or a non-UTF8 value (or both).
    /// Included here for debugging
    pub byte_pairs: Vec<(TruncatedNEBytes, TruncatedNEBytes)>,

    /// Standard keys which appear more than once with their values.
    pub non_unique_std_keywords: Vec<(DollarStdKey, TruncatedNEString)>,

    /// Standard keys which appear more than once with their values.
    pub non_unique_pstd_keywords: Vec<(DollarPseudoStdKey, TruncatedNEString)>,

    /// Nonstandard keys which appear more than once with their values.
    pub non_unique_nonstd_keywords: Vec<(NonStdKey, TruncatedNEString)>,

    /// Keys with empty values as a result of trimming whitespace.
    pub keys_with_empty_trimmed_values: Vec<(DollarKeyOrBytes, TruncatedNEString)>,

    /// Keys with values that are not empty after whitespace was trimmed off.
    ///
    /// Values included here are the original values before trimming.
    pub keys_with_trimmed_values: Vec<(DollarKeyOrBytes, TruncatedNEString)>,

    /// Keys that have blank values.
    ///
    /// Only relevant in escaped delimiter mode.
    pub keys_with_blank_values: Vec<NEStringOrBytes>,

    /// Values with blank keys.
    pub values_with_blank_keys: Vec<NEStringOrBytes>,

    // TODO rename to something like "empty_pairs"
    /// Number of key/value pairs that were skipped because both were blank.
    pub skipped_pairs: usize,

    /// Tokens with delimiters at their boundaries (without the delimiters).
    ///
    /// Only relevant in escaped delimiter mode.
    pub tokens_with_boundary_delims: Vec<NEStringOrBytes>,

    /// Last token if the number of tokens was odd.
    pub last_odd_token: StringOrBytes,

    /// `true` if the number of delimiters was even.
    ///
    /// This means there was either one too many or one too few delimiters.
    /// If [`Self::last_odd_token`] is non-empty, it was the former, otherwise
    /// the latter.
    pub has_even_delims: bool,

    /// The number of delimiters (excluding the first) at the front of TEXT.
    ///
    /// This will only be non-zero for escaped mode.
    pub extra_leading_delims: usize,

    /// `true` if TEXT was encoded with UTF-8, `false` for Latin-1.
    pub multibyte_encoded: bool,
}

/// Summary of an FCS dataset
#[derive(Clone, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[allow(clippy::too_many_arguments)]
pub struct DatasetSummary {
    /// FCS version
    pub version: Version,

    /// Length of TEXT (in bytes)
    pub text_len: u64,

    /// Length of DATA (in bytes)
    pub data_len: u64,

    /// Length of ANALYSIS (in bytes)
    pub analysis_len: u64,

    /// Number of events ($TOT)
    pub n_events: usize,

    /// Number of measurements ($PAR)
    pub n_measurements: usize,

    /// Number of OTHER segments
    pub n_other: usize,

    /// Total length of OTHER segments (in bytes)
    pub others_len: usize,

    /// The value of $DATATYPE
    pub datatype: Option<AlphaNumType>,

    /// The offset in the FCS file where this HEADER appears.
    pub dataset_offset: DatasetOffset,

    /// Value of the cyclic redundancy check (CRC) as read from the file.
    ///
    /// Will always be `None` for 2.0.
    pub file_crc: Option<CRCOutput>,

    /// Value of the computed cyclic redundancy check (CRC) of the dataset.
    ///
    /// Will always be `None` for 2.0.
    pub computed_crc: Option<u16>,

    /// Number of nanoseconds spent reading HEADER
    pub read_header_ns: u128,

    /// Number of nanoseconds spent reading TEXT
    pub read_text_ns: u128,

    /// Number of nanoseconds spent reading the data schema
    pub read_schema_ns: u128,

    /// Number of nanoseconds spent reading DATA
    pub read_data_ns: u128,

    /// Number of nanoseconds spent checking DATA against PNR
    pub check_range_ns: u128,

    /// The number of nanoseconds spent reading OTHER and/or ANALYSIS.
    pub read_other_analysis_ns: u128,

    /// Number of nanoseconds spent computing the CRC
    pub read_crc_ns: u128,

    /// Number of nanoseconds spent read dark bytes
    pub read_dark_bytes_ns: u128,

    /// Number of nanoseconds spent scanning for next dataset
    pub scan_next_ns: u128,
}

/// Warning when parsing [`Header`]
#[derive(From, Display, Error, Debug, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ReadHeaderError {
    Header(HeaderError),
    DatasetOffset(DatasetOffsetError),
}

/// Warning when parsing TEXT in standard mode
#[derive(From, Display, Error, Debug, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum StdTEXTWarning {
    Flat(HeaderOrFlatTEXTWarning),
    Std(StdTEXTFromKeywordsWithOffsetsWarning),
}

/// Error when parsing TEXT in standard mode
#[derive(From, Display, Error, Debug, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum StdTEXTError {
    Flat(HeaderOrFlatTextError),
    Std(AnyStdTEXTFromKeywordsError),
    Warn(StdTEXTWarning),
}

/// Warning when parsing TEXT+DATA in standard mode
#[derive(From, Display, Error, Debug, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum StdDatasetWarning {
    Flat(HeaderOrFlatTEXTWarning),
    Std(StdDatasetFromKeywordsWarningInner),
}

/// Error when parsing TEXT+DATA in standard mode
#[derive(From, Display, Error, Debug, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum StdDatasetError {
    Flat(HeaderOrFlatTextError),
    Std(AnyStdDatasetFromKeywordsError),
    Warn(StdDatasetWarning),
}

/// Warning when parsing TEXT+DATA in flat mode
#[derive(From, Display, Error, Debug, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ReadFlatDatasetWarning {
    Flat(HeaderOrFlatTEXTWarning),
    Read(ReadFlatDatasetFromKwsOutputWarning),
    Repair(RepairError),
}

/// Warning when parsing TEXT+DATA in flat mode
#[derive(From, Display, Error, Debug, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ReadFlatDatasetError {
    Flat(HeaderOrFlatTextError),
    Read(ReadFlatDatasetFromKwsOutputError),
    Warn(ReadFlatDatasetWarning),
    Version(GuessVersionError),
    Repair(RepairError),
    RepairAppend(AppendRepairFlagError),
}

/// Error when parsing HEADER or TEXT segments in flat mode
#[derive(From, Display, Error, Debug, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum HeaderOrFlatTextError {
    DatasetOffset(DatasetOffsetError),
    Header(HeaderError),
    FlatTEXT(ParseFlatTEXTError),
    Warn(HeaderOrFlatTEXTWarning),
}

/// Error when reading DATA offsets from already-parsed keywords
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ReadFlatDatasetWithKwsError {
    Dataset(ReadFlatDatasetFromKwsOutputError),
    DatasetLen(DatasetLenEOFError),
    DatasetOffset(DatasetOffsetError),
    Warn(ReadFlatDatasetFromKwsOutputWarning),
}

/// Error when reading DATA offsets from already-parsed keywords
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ReadFlatDatasetFromKwsOutputError {
    Tx(LookupAndReadDataAnalysisError),
    CRC(CRCError),
}

/// Warning when reading DATA offsets from already-parsed keywords
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ReadFlatDatasetFromKwsOutputWarning {
    Tx(LookupAndReadDataAnalysisWarning),
    CRC(CRCError),
}

/// Error when looking up and parsing supplemental TEXT offsets from primary TEXT.
#[derive(From, Display, Error, Debug, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum STextOffsetsError {
    ReqOffsets(ReqOffsetsError<Beginstext, Endstext>),
    Overlap(SuppToHeaderOffsetsValidationError),
    Duplicated(DuplicateSTextError),
    Nextdata(DatasetOverflowError<SuppTextOffsetsName>),
}

/// Warning when looking up and parsing supplemental TEXT offsets from primary TEXT.
#[derive(From, Display, Error, Debug, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum STextOffsetsWarning {
    OptOffsets(OptOffsetsError<Beginstext, Endstext>),
    Error(STextOffsetsError),
}

/// Error when supplement and primary TEXT offsets are identity
#[derive(Error, Debug, new, PartialEq, Clone)]
#[error(
    "{location} and supplemental TEXT have identical offsets, keeping {}: {offsets}",
    if self.keep_supp { "latter" } else { "former" }
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct DuplicateSTextError {
    offsets: OriginalOffsets,
    location: AnyRegion,
    keep_supp: bool,
}

/// Warning when parsing multiple [`FlatDatasetOutput`]s
#[derive(From, Display, Error, Debug, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum MultiFlatDatasetWarning {
    Text(HeaderOrFlatTEXTWarning), // for reading skipped datasets to get $NEXTDATA
    Data(ReadFlatDatasetWarning),
}

/// Error when parsing multiple [`FlatDatasetOutput`]s
#[derive(From, Display, Error, Debug, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum MultiFlatDatasetError {
    Text(HeaderOrFlatTextError), // for reading skipped datasets to get $NEXTDATA
    Data(ReadFlatDatasetError),
}

/// Error when parsing multiple TEXT segments in std mode
#[derive(From, Display, Error, Debug, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum MultiStdTEXTError {
    FLat(HeaderOrFlatTextError), // for reading skipped datasets to get $NEXTDATA
    Single(StdTEXTError),
}

/// Warning when parsing multiple TEXT segments in std mode
#[derive(From, Display, Error, Debug, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum MultiStdTEXTWarning {
    Flat(HeaderOrFlatTEXTWarning), // for reading skipped datasets to get $NEXTDATA
    Std(StdTEXTWarning),
}

/// Error when parsing multiple datasets in std mode
#[derive(From, Display, Error, Debug, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum MultiStdDatasetError {
    Text(HeaderOrFlatTextError), // for reading skipped datasets to get $NEXTDATA
    Data(StdDatasetError),
}

/// Warning when parsing multiple TEXT segment in std mode
#[derive(From, Display, Error, Debug, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum MultiStdDatasetWarning {
    Flat(HeaderOrFlatTEXTWarning), // for reading skipped datasets to get $NEXTDATA
    Std(StdDatasetWarning),
}

/// Warning when parsing HEADER + TEXT segment in flat mode
#[derive(From, Display, Error, Debug, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum HeaderOrFlatTEXTWarning {
    Header(GuessOtherWidthError),
    Text(ParseFlatTEXTWarning),
}

/// Warning when parsing TEXT segment in flat mode
#[derive(From, Display, Error, Debug, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ParseFlatTEXTWarning {
    Char(DelimCharError),
    Primary(ParseKeywordsIssue),
    Supplemental(ParseSupplementalTEXTError),
    SuppOffsets(STextOffsetsWarning),
    Nextdata(ReadNextdataError),
}

/// Error when parsing TEXT segment in flat mode
#[derive(From, Display, Error, Debug, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ParseFlatTEXTError {
    Empty(EmptyTEXTError),
    PrimaryTEXTOverflow(PrimaryTEXTOverflowError),
    Delim(DelimCharError),
    Primary(ParseKeywordsIssue),
    Supplemental(ParseSupplementalTEXTError),
    SuppOffsets(STextOffsetsError),
    Nextdata(LookupNextdataError),
    NextdataOffset(DatasetOverflowError<HeaderOffsetsName>),
}

/// Error when parsing supplemental TEXT
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ParseSupplementalTEXTError {
    Keywords(ParseKeywordsIssue),
    Mismatch(DelimMismatch),
}

/// Error when extracting keywords from TEXT segment (primary or supplemental)
#[derive(Display, From, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ParseKeywordsIssue {
    BlankPair(BlankPairError),
    BlankKey(BlankKeyError),
    BlankValue(BlankValueError),
    TrimmedValue(TrimmedBlankValueError),
    Uneven(UnevenTokensError),
    EvenFinal(EvenDelimiterError),
    Bound(DelimBoundError),
    Leading(LeadingDelimError),
    NonUniqueStd(StdPresent),
    NonUniquePstd(PseudoStdPresent),
    NonUniqueNonStd(NonStdPresent),
    Key(NonAsciiKeyError),
    Value(NonUtf8ValueError),
    Both(NonAsciiOrUtf8KeywordError),
}

/// Error when key has blank value
#[derive(Debug, PartialEq, Error, Clone)]
#[error("skipping key {0} with blank value")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeyError))]
pub struct BlankValueError(pub DollarKeyOrBytes);

/// Error when key has blank value
#[derive(new, Debug, PartialEq, Error, Clone)]
#[error("key '{key}' with original value '{value}' was trimmed to blank in {kind} TEXT")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeyError))]
pub struct TrimmedBlankValueError {
    kind: TEXTKind,
    key: DollarKeyOrBytes,
    value: TruncatedNEString,
}

/// Error when blank key is encountered in TEXT
#[derive(Debug, Error, new, PartialEq, Clone)]
#[error("skipping blank key in {kind} TEXT with value of '{value}'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct BlankKeyError {
    kind: TEXTKind,
    value: NEStringOrBytes,
}

/// Error when blank key is encountered in TEXT
#[derive(Debug, Error, new, PartialEq, Clone)]
#[error("there were {n} blank key/value pairs in {kind} TEXT")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct BlankPairError {
    kind: TEXTKind,
    n: NonZeroUsize,
}

/// Error when number of tokens in TEXT is not even
#[derive(Debug, Error, new, PartialEq, Clone)]
#[error("{kind} TEXT segment has uneven number of tokens, last odd token is '{token}'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct UnevenTokensError {
    kind: TEXTKind,
    token: NEStringOrBytes,
}

/// Error when TEXT contains an even number of delimiters.
///
/// TEXT can only contain an odd number of delimiters in a standards compliant
/// file.
#[derive(Debug, Error, PartialEq, Clone)]
#[error("{0} TEXT contains an uneven number of delimiters")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct EvenDelimiterError(TEXTKind);

/// Error when delimiter(s) is/arg found after a token at a boundary.
///
/// This can only happen in escaped TEXT.
#[derive(Debug, Error, new, PartialEq, Clone)]
#[error(
    "escaped delimiter(s) encountered before unescaped delimiter \
     at the end of '{token}' in {kind} TEXT"
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct DelimBoundError {
    kind: TEXTKind,
    token: NEStringOrBytes,
}

/// Error when text starts with more than one delimiter in escaped mode.
///
/// This can only happen in escaped TEXT.
#[derive(Debug, Error, new, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct LeadingDelimError {
    kind: TEXTKind,
    extra: NonZeroUsize,
}

impl fmt::Display for LeadingDelimError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let k = self.kind;
        let extra = self.extra.get();
        let total = extra + 1;
        if total & 1 == 1 {
            write!(
                f,
                "{k} TEXT starts with {total} delimiters, \
                 {extra} of which are escaped",
            )
        } else {
            write!(
                f,
                "{k} TEXT starts with {total} delimiters which are all escaped"
            )
        }
    }
}

/// Error when key is already present in hash table.
#[derive(Debug, PartialEq, Error, new, Clone)]
#[error("key '{key}' already present, has value '{value}' in {kind} TEXT")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeyError))]
#[cfg_attr(feature = "python", bound(T: fmt::Display))]
pub struct KeyPresent<T> {
    kind: TEXTKind,
    key: T,
    value: TruncatedNEString,
}

pub type StdPresent = KeyPresent<DollarStdKey>;
pub type PseudoStdPresent = KeyPresent<DollarPseudoStdKey>;
pub type NonStdPresent = KeyPresent<NonStdKey>;

/// Error when key or value with invalid UTF-8 characters is encountered
#[derive(new, Debug, Error, PartialEq, Clone)]
#[error("non ASCII key {key} and non UTF-8 value {value} encountered in {kind} TEXT")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeyError))]
pub struct NonAsciiOrUtf8KeywordError {
    kind: TEXTKind,
    key: TruncatedNEBytes,
    value: TruncatedNEBytes,
}

/// Error when key is not ASCII
#[derive(new, Debug, Error, PartialEq, Clone)]
#[error("non ASCII key encountered with bytes {key} and value '{value}' in {kind} TEXT")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeyError))]
pub struct NonAsciiKeyError {
    kind: TEXTKind,
    key: TruncatedNEBytes,
    value: TruncatedNEString,
}

/// Error when value is not Utf8
#[derive(new, Debug, Error, PartialEq, Clone)]
#[error("non UTF-8 key encountered with bytes {value} and key '{key}' in {kind} TEXT")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeyError))]
pub struct NonUtf8ValueError {
    kind: TEXTKind,
    key: AnyKey,
    value: TruncatedNEBytes,
}

/// Error when TEXT delimiter is not ASCII
#[derive(Debug, Error, PartialEq, Clone)]
#[error("delimiter must be ASCII character 1-126 inclusive, got {0}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct DelimCharError(u8);

/// Error when primary TEXT segment is empty
#[derive(Debug, Error, PartialEq, Clone)]
#[error("Primary TEXT segment is empty")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct EmptyTEXTError;

/// Error when delimiter of supplemental TEXT does not match primary TEXT
#[derive(Debug, Clone, Error, new, PartialEq)]
#[error(
    "first byte of supplemental TEXT ({supp}) does not match \
     delimiter of primary TEXT ({delim})"
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct DelimMismatch {
    supp: u8,
    delim: u8,
}

/// Differentiate TEXT being primary or supplemental
#[derive(Clone, Copy, Debug, Display, PartialEq)]
enum TEXTKind {
    #[display("Primary")]
    Primary,
    #[display("Supplemental")]
    Supplemental,
}

/// Result of guessing the escape more for TEXT.
#[derive(Debug, PartialEq, Clone, Copy)]
enum GuessedEscapeMode {
    Escaped,
    Unescaped,
    Ambiguous,
}

pub(crate) struct FCSFileReader {
    pub(crate) file_len: FileLen,
    pub(crate) buf_read: BufReader<File>,
}

#[derive(new)]
struct FlatTEXTOutputInner<T, C> {
    this: T,
    read_end: Instant,
    state: TEXTReadState<C>,
}

def_summary!(pub HeaderSummary, "could not parse HEADER");

def_summary!(pub FlatTEXTSummary, "could not parse TEXT segment");

def_summary!(pub StdTEXTSummary, "could not standardize TEXT segment");

def_summary!(
    pub StdDatasetSummary,
    "could not read DATA with standardized TEXT"
);

def_summary!(pub FlatDatasetSummary, "could not read DATA with flat TEXT");

def_summary!(
    pub FlatDatasetWithKwsSummary,
    "could not read flat dataset from keywords"
);

impl FCSFileReader {
    pub(crate) fn open(p: &PathBuf) -> io::Result<Self> {
        let file = File::options().read(true).open(p)?;
        let m = file.metadata()?;
        let file_len = m.len().into();
        let handle = BufReader::new(file);
        Ok(Self {
            file_len,
            buf_read: handle,
        })
    }

    pub(crate) fn open_with_state<C>(
        p: &PathBuf,
        dataset_offset: DatasetOffset,
        start_time: Instant,
        conf: C,
    ) -> IOResult<(Self, HeaderReadState<C>), DatasetOffsetError> {
        let fr = Self::open(p)?;
        let st = fr
            .as_read_dataset_state(dataset_offset, start_time, conf)
            .map_err(ImpureError::Pure)?;
        Ok((fr, st))
    }

    pub(crate) fn as_read_dataset_state<C>(
        &self,
        dataset_offset: DatasetOffset,
        start_time: Instant,
        conf: C,
    ) -> Result<HeaderReadState<C>, DatasetOffsetError> {
        HeaderReadState::init(self.file_len, dataset_offset, start_time, conf)
    }

    fn read_flat_text(
        &mut self,
        dataset_offset: DatasetOffset,
        start_time: Instant,
        conf: &ReadFlatTEXTConfig,
    ) -> WarningsAndIOGroupResult<
        FlatTEXTOutput,
        HeaderOrFlatTEXTWarning,
        HeaderOrFlatTextError,
        FlatTEXTSummary,
    > {
        self.read_flat_text_inner(dataset_offset, start_time, conf)
            .map_ok_value(|out| out.this)
            .warnings_to_pure_errors(conf.shared, HeaderOrFlatTextError::from)
            .deanonymize()
    }

    fn read_std_text(
        &mut self,
        dataset_offset: DatasetOffset,
        start_time: Instant,
        conf: &ReadStdTEXTConfig,
    ) -> WarningsAndIOGroupResult<
        (AnyCoreTEXT, StdTEXTOutput),
        StdTEXTWarning,
        StdTEXTError,
        StdTEXTSummary,
    > {
        self.read_flat_text_inner(dataset_offset, start_time, conf)
            .map_pure_errors(StdTEXTError::from)
            .map_commutative_warnings(StdTEXTWarning::from)
            .and_then_commutative(|out| {
                out.this
                    .into_std_text(out.read_end, &out.state)
                    .map_commutative_warnings(StdTEXTWarning::from)
                    .map_errors(StdTEXTError::from)
                    .group()
                    .map_errors(IOErrorGroup::Pure)
            })
            .warnings_to_pure_errors(conf.shared, StdTEXTError::from)
            .deanonymize()
    }

    fn read_flat_dataset(
        &mut self,
        dataset_offset: DatasetOffset,
        scan_next_dataset: bool,
        start_time: Instant,
        conf: &ReadFlatDatasetConfig,
    ) -> WarningsAndIOGroupResult<
        FlatDatasetOutput,
        ReadFlatDatasetWarning,
        ReadFlatDatasetError,
        FlatDatasetSummary,
    > {
        #[derive(AsRef)]
        struct LookupConfig {
            #[as_ref(EvaledReadRepairKeywordsConfig)]
            repair: EvaledReadRepairKeywordsConfig,
            #[as_ref(ReadDataKeywordsConfig)]
            data_kws: ReadDataKeywordsConfig,
            #[as_ref(ReadDatasetConfig)]
            dataset: ReadDatasetConfig,
            #[as_ref(ReadOffsetConfig)]
            offsets: ReadOffsetConfig,
        }

        self.read_flat_text_inner(dataset_offset, start_time, conf)
            .map_commutative_warnings(ReadFlatDatasetWarning::from)
            .map_pure_errors(ReadFlatDatasetError::from)
            .and_then_commutative(|out| {
                let version = out.this.flat_diagnostics.header_supp.header.version;
                let oride = conf.flat.version_override.as_ref();
                autodetect_version(version, &out.this.keywords.std, oride)
                    .map_err(ReadFlatDatasetError::from)
                    .map_err(IOErrorGroup::new_pure_one)
                    .map(|(new_version, scores)| (new_version, out, scores))
                    .into_log()
            })
            .and_then_commutative(|(new_ver, out, scores)| {
                let st = &out.state;
                eval_repair_conf(st.conf().as_ref(), &out.this.keywords)
                    .map_ok_value(|repair| {
                        st.as_ref().first_once(|conf_| LookupConfig {
                            repair,
                            // TODO useless clone
                            data_kws: AsRef::<ReadDataKeywordsConfig>::as_ref(&conf_).clone(),
                            dataset: *AsRef::<ReadDatasetConfig>::as_ref(&conf_),
                            offsets: *AsRef::<ReadOffsetConfig>::as_ref(&conf_),
                        })
                    })
                    .map_errors(ReadFlatDatasetError::from)
                    .nowarn_into_warn()
                    .group()
                    .map_error(IOErrorGroup::Pure)
                    .and_then_commutative(|lst| {
                        let mut flat = out.this;
                        let mut rtx = flat.keywords.std.as_transaction();
                        let repair_res = rtx
                            .repair(
                                &mut flat.keywords.pstd,
                                &mut flat.keywords.nonstd,
                                &lst.conf().repair,
                            )
                            .map_commutative_warnings(ReadFlatDatasetWarning::from)
                            .map_errors(ReadFlatDatasetError::from);
                        let ltx = rtx.into_lookup_transaction();
                        let hns = &mut flat.flat_diagnostics.header_supp;
                        FlatDatasetFromKwsOutput::h_read(
                            &mut self.buf_read,
                            new_ver,
                            &ltx,
                            hns,
                            scan_next_dataset,
                            out.read_end,
                            &out.state,
                        )
                        .map_commutative_warnings(ReadFlatDatasetWarning::from)
                        .map_pure_errors(ReadFlatDatasetError::from)
                        .zip_io_group_commutative(repair_res)
                        .map_ok_value(|(dataset, repair_diag)| {
                            FlatDatasetOutput::new(
                                flat.keywords,
                                flat.flat_diagnostics,
                                dataset,
                                scores,
                                repair_diag,
                            )
                        })
                    })
            })
            .warnings_to_pure_errors(conf.shared, ReadFlatDatasetError::from)
            .deanonymize()
    }

    fn read_std_dataset(
        &mut self,
        dataset_offset: DatasetOffset,
        scan_next_dataset: bool,
        start_time: Instant,
        conf: &ReadStdDatasetConfig,
    ) -> WarningsAndIOGroupResult<
        (AnyCoreDataset, StdDatasetOutput),
        StdDatasetWarning,
        StdDatasetError,
        StdDatasetSummary,
    > {
        self.read_flat_text_inner(dataset_offset, start_time, conf)
            .map_pure_errors(StdDatasetError::from)
            .map_commutative_warnings(StdDatasetWarning::from)
            .and_then_commutative(|out| {
                out.this
                    .into_std_dataset(
                        &mut self.buf_read,
                        scan_next_dataset,
                        out.read_end,
                        &out.state,
                    )
                    .map_commutative_warnings(StdDatasetWarning::from)
                    .map_pure_errors(StdDatasetError::from)
            })
            .warnings_to_pure_errors(conf.shared, StdDatasetError::from)
            .deanonymize()
    }

    fn read_flat_texts(
        &mut self,
        skip: Option<usize>,
        limit: Option<usize>,
        scan_next_dataset: bool,
        mut start_time: Instant,
        conf: &ReadFlatTEXTConfig,
    ) -> WarningsAndIOGroupResult<
        Vec<FlatTEXTOutput>,
        HeaderOrFlatTEXTWarning,
        HeaderOrFlatTextError,
        FlatTEXTSummary,
    > {
        let mut dataset_offset = Some(DatasetOffset::default());
        let mut count = 0_usize;
        let mut results = vec![];
        while let Some(dso) = dataset_offset
            && limit.is_none_or(|x| count <= x)
        {
            let res = self.read_flat_text(dso, start_time, conf);
            let succ = split_log!(res);
            let scanned_dataset_offset = if scan_next_dataset {
                io_to_log!(next_dataset_boundary(&mut self.buf_read))
            } else {
                None
            };
            let nextdata_res = succ.fmap_once(|ret| {
                let hns = &ret.flat_diagnostics.header_supp;
                let nd = hns.nextdata.map(u64::from);
                dataset_offset = scanned_dataset_offset.or_else(|| {
                    let n = nd?;
                    (n > 0).then_some(DatasetOffset(dso.0 + n))
                });
                ret
            });
            results.push(nextdata_res);
            count += 1;
            // Subsequent start times will be a bit shorter because the file is
            // already open.
            start_time = Instant::now();
        }
        results
            .into_iter()
            .sequence_success()
            .fmap_once(|xs| xs.into_iter().skip(skip.unwrap_or_default()).collect())
            .into_log()
    }

    fn read_std_texts(
        &mut self,
        skip: Option<usize>,
        limit: Option<usize>,
        scan_next_dataset: bool,
        mut start_time: Instant,
        conf: &ReadStdTEXTConfig,
    ) -> WarningsAndIOGroupResult<
        Vec<(AnyCoreTEXT, StdTEXTOutput)>,
        MultiStdTEXTWarning,
        MultiStdTEXTError,
        StdTEXTSummary,
    > {
        let mut dataset_offset = Some(DatasetOffset::default());
        let mut count = 0_usize;
        let mut results = vec![];
        let rconf = ReadFlatTEXTConfig {
            header: AsRef::<ReadHeaderInnerConfig>::as_ref(conf).clone(),
            flat: AsRef::<ReadHeaderAndTEXTConfig>::as_ref(conf).clone(),
            offset: *conf.as_ref(),
            shared: *conf.as_ref(),
        };
        macro_rules! get_scanned {
            () => {
                if scan_next_dataset {
                    io_to_log!(next_dataset_boundary(&mut self.buf_read))
                } else {
                    None
                }
            };
        }
        while let Some(dso) = dataset_offset
            && limit.is_none_or(|x| count < x)
        {
            let nextdata_res = if skip.is_some_and(|s| count < s) {
                let res = self
                    .read_flat_text(dso, start_time, &rconf)
                    .map_commutative_warnings(MultiStdTEXTWarning::from)
                    .map_pure_errors(MultiStdTEXTError::from)
                    .map_error(|e| e.set_group(StdTEXTSummary));
                let succ = split_log!(res);
                let scanned_dataset_offset = get_scanned!();
                succ.fmap_once(|ret| {
                    let hns = ret.flat_diagnostics.header_supp;
                    let nd = hns.nextdata.map(u64::from);
                    dataset_offset = scanned_dataset_offset.or_else(|| {
                        let n = nd?;
                        (n > 0).then_some(DatasetOffset(dso.0 + n))
                    });
                    None
                })
            } else {
                let res = Self::read_std_text(self, dso, start_time, conf)
                    .map_commutative_warnings(MultiStdTEXTWarning::from)
                    .map_pure_errors(MultiStdTEXTError::from);
                let succ = split_log!(res);
                let scanned_dataset_offset = get_scanned!();
                succ.fmap_once(|ret| {
                    dataset_offset = scanned_dataset_offset.or_else(|| {
                        let nd = u64::from(ret.1.flat_diagnostics.header_supp.nextdata?);
                        (nd > 0).then_some(DatasetOffset(dso.0 + nd))
                    });
                    Some(ret)
                })
            };
            results.push(nextdata_res);
            count += 1;
            // Subsequent start times will be a bit shorter because the file is
            // already open.
            start_time = Instant::now();
        }
        results
            .into_iter()
            .sequence_success()
            .fmap_once(|xs| xs.into_iter().flatten().collect())
            .into_log()
    }

    fn read_flat_datasets(
        &mut self,
        skip: Option<usize>,
        limit: Option<usize>,
        scan_next_dataset: bool,
        start_time: Instant,
        conf: &ReadFlatDatasetConfig,
    ) -> WarningsAndIOGroupResult<
        Vec<FlatDatasetOutput>,
        MultiFlatDatasetWarning,
        MultiFlatDatasetError,
        FlatDatasetSummary,
    > {
        self.read_nextdata_loop(
            skip,
            limit,
            scan_next_dataset,
            start_time,
            conf,
            FlatDatasetSummary,
            Self::read_flat_dataset,
            |ret| ret.dataset.dataset_diagnostics.next_dataset_offset,
        )
    }

    fn read_std_datasets(
        &mut self,
        skip: Option<usize>,
        limit: Option<usize>,
        scan_next_dataset: bool,
        start_time: Instant,
        conf: &ReadStdDatasetConfig,
    ) -> WarningsAndIOGroupResult<
        Vec<(AnyCoreDataset, StdDatasetOutput)>,
        MultiStdDatasetWarning,
        MultiStdDatasetError,
        StdDatasetSummary,
    > {
        self.read_nextdata_loop(
            skip,
            limit,
            scan_next_dataset,
            start_time,
            conf,
            StdDatasetSummary,
            Self::read_std_dataset,
            |ret| ret.1.dataset.dataset_diagnostics.next_dataset_offset,
        )
    }

    fn read_flat_text_inner<C>(
        &mut self,
        dataset_offset: DatasetOffset,
        start_time: Instant,
        conf: C,
    ) -> WarningsAndIOGroupResult<
        FlatTEXTOutputInner<FlatTEXTOutput, C>,
        HeaderOrFlatTEXTWarning,
        HeaderOrFlatTextError,
        (),
    >
    where
        C: AsRef<ReadHeaderAndTEXTConfig> + AsRef<ReadHeaderInnerConfig> + AsRef<ReadOffsetConfig>,
    {
        self.as_read_dataset_state(dataset_offset, start_time, conf)
            .map_err(HeaderOrFlatTextError::from)
            .map_err(IOAnonErrorGroup::new_pure_one)
            .into_log()
            .and_then_commutative(|st| FlatTEXTOutput::h_read(&mut self.buf_read, st))
    }

    #[allow(clippy::too_many_arguments)]
    fn read_nextdata_loop<X, W, E, Wi, Ei, G, C, Fsucc, Fnext>(
        &mut self,
        skip: Option<usize>,
        limit: Option<usize>,
        scan_next_dataset: bool,
        mut start_time: Instant,
        conf: &C,
        g: G,
        mut f0: Fsucc,
        mut fnext: Fnext,
    ) -> WarningsAndIOGroupResult<Vec<X>, W, E, G>
    where
        Fsucc: FnMut(
            &mut Self,
            DatasetOffset,
            bool,
            Instant,
            &C,
        ) -> WarningsAndIOGroupResult<X, Wi, Ei, G>,
        Fnext: FnMut(&X) -> Option<DatasetOffset>,
        E: From<HeaderOrFlatTextError> + From<Ei>,
        W: From<HeaderOrFlatTEXTWarning> + From<Wi>,
        C: AsRef<ReadHeaderInnerConfig>
            + AsRef<ReadHeaderAndTEXTConfig>
            + AsRef<ReadOffsetConfig>
            + AsRef<ReadSharedConfig>,
        G: Copy,
    {
        let mut dataset_offset = Some(DatasetOffset::default());
        let mut count = 0_usize;
        let mut results = vec![];
        let rconf = ReadFlatTEXTConfig {
            header: AsRef::<ReadHeaderInnerConfig>::as_ref(conf).clone(),
            flat: AsRef::<ReadHeaderAndTEXTConfig>::as_ref(conf).clone(),
            offset: *conf.as_ref(),
            shared: *conf.as_ref(),
        };
        while let Some(dso) = dataset_offset
            && limit.is_none_or(|x| count < x)
        {
            let nextdata_res = if skip.is_some_and(|s| count < s) {
                let res = self
                    .read_flat_text(dso, start_time, &rconf)
                    .map_commutative_warnings(W::from)
                    .map_pure_errors(E::from)
                    .map_error(|e| e.set_group(g));
                let succ = split_log!(res);
                let scanned_dataset_offset = if scan_next_dataset {
                    io_to_log!(next_dataset_boundary(&mut self.buf_read))
                } else {
                    None
                };
                succ.fmap_once(|ret| {
                    let hns = ret.flat_diagnostics.header_supp;
                    let nd = hns.nextdata.map(u64::from);
                    dataset_offset = scanned_dataset_offset.or_else(|| {
                        let n = nd?;
                        (n > 0).then_some(DatasetOffset(dso.0 + n))
                    });
                    None
                })
            } else {
                let res = f0(self, dso, scan_next_dataset, start_time, conf)
                    .map_commutative_warnings(W::from)
                    .map_pure_errors(E::from);
                let succ = split_log!(res);
                succ.fmap_once(|ret| {
                    dataset_offset =
                        fnext(&ret).and_then(|next_dso| (next_dso.0 > 0).then_some(next_dso));
                    Some(ret)
                })
            };
            results.push(nextdata_res);
            count += 1;
            // Subsequent start times will be a bit shorter because the file is
            // already open.
            start_time = Instant::now();
        }
        results
            .into_iter()
            .sequence_success()
            .fmap_once(|xs| xs.into_iter().flatten().collect())
            .into_log()
    }
}

impl HeaderAndSuppOffsets {
    /// Ensure this offset pair does not overlap with another offset pair.
    ///
    /// Specifically check that no other offset pairs (except its analogue in
    /// HEADER if non-empty) overlaps with this one. Also ensure that that these
    /// offsets don't overlap with HEADER itself.
    pub(crate) fn validate_text_offsets<I>(
        &mut self,
        offsets: &mut TEXTOffsets<I>,
        limit: OverlapCorrectionLimit,
    ) -> DeferredErrors<
        Vec<TextToHeaderOrSuppOffsetsOverlap>,
        TextToHeaderOrSuppOffsetsValidationError,
    >
    where
        I: HasRegion + AreNamedOffsets<TextOffsetsName, Params = ()> + IsDataOrAnalysis,
    {
        if let Some(this_ne) = offsets.as_nonempty_mut() {
            // Check for overlap with STEXT offsets. This offset pair should not
            // be modified since it has already been read. Therefore, only
            // change the offsets of the new pair if its ending offset is within
            // STEXT.
            let mut supp_overlap = None;
            let stxt_error = self.supp_text.as_offset_pair().and_then(|mut supp_pair| {
                let supp_ne = supp_pair.as_nonempty_mut()?;
                if this_ne.slice_pair() < supp_ne.slice_pair() {
                    let res = this_ne.tail_overlap_pair_and_truncate(&supp_ne, limit.0, ())?;
                    let o = res.overlap.second_into_once();
                    if res.truncated {
                        supp_overlap = Some(o);
                        None
                    } else {
                        Some(OffsetPairsOverlapError(o))
                    }
                } else {
                    supp_ne.tail_overlap_pair(&this_ne).map(|truncated_len| {
                        // TODO these offsets should be flipped
                        let o = OffsetsOverlap::new(
                            this_ne.as_named1(),
                            supp_ne.as_named1().fmap_into_once(),
                            truncated_len,
                        );
                        OffsetPairsOverlapError(o)
                    })
                }
            });
            // Check for any errors between this offset pair and HEADER offset
            // pair, modifying as necessary and as overlap limit permits.
            self.header
                .final_offsets
                .validate_text_data_or_analysis(offsets, limit)
                .map_errors(OffsetsValidationError::into2)
                .extend_errors(stxt_error.map(OffsetsValidationError::from), |v| v)
                .map_deferred_value(|hdr_overlaps| {
                    hdr_overlaps
                        .into_iter()
                        .map(BifunctorOnce::second_into_once)
                        .chain(supp_overlap)
                        .collect()
                })
        } else {
            LogResult::new_ok(vec![])
        }
    }

    pub(crate) fn text_other_max_end_offset(&self) -> u64 {
        let hdr_max = self.header.final_offsets.ptext_other_max_end_offset();
        self.supp_text
            .final_offsets()
            .and_then(|o| o.as_nonempty())
            .map_or(hdr_max, |o| o.end().max(hdr_max))
    }
}

impl FlatDatasetOutput {
    fn summarize(self) -> DatasetSummary {
        let fd = self.flat_diagnostics;
        let hdr = fd.header_supp.header;
        let ds = self.dataset;
        let txt = AsRef::<PrimaryTextOffsets>::as_ref(&hdr.final_offsets);
        let datatype = self
            .keywords
            .std
            .get(&RootKey::Datatype.to_std0())
            .parse()
            .ok();
        DatasetSummary {
            version: hdr.version,
            text_len: txt.nbytes(),
            data_len: ds.dataset_offsets.final_data.nbytes(),
            analysis_len: ds.dataset_offsets.final_analysis.nbytes(),
            n_events: ds.data.nrows(),
            n_measurements: ds.data.ncols(),
            n_other: ds.others.0.len(),
            others_len: ds.others.0.iter().map(|x| x.0.len()).sum(),
            datatype,
            dataset_offset: hdr.dataset_offset,
            file_crc: ds.dataset_diagnostics.file_crc,
            computed_crc: ds.dataset_diagnostics.computed_crc,
            read_header_ns: hdr.read_header_ns,
            read_text_ns: fd.read_text_ns,
            read_schema_ns: ds.schema_diagnostics.read_schema_ns,
            read_data_ns: ds.dataset_diagnostics.read_data_ns,
            check_range_ns: ds.dataset_diagnostics.check_range_ns,
            read_other_analysis_ns: ds.dataset_diagnostics.read_other_analysis_ns,
            read_crc_ns: ds.dataset_diagnostics.read_crc_ns,
            read_dark_bytes_ns: ds.dataset_diagnostics.read_dark_bytes_ns,
            scan_next_ns: ds.dataset_diagnostics.scan_next_ns,
        }
    }
}

impl FlatDatasetFromKwsOutput {
    /// Read from handle with offsets/version from HEADER and parsed TEXT keywords.
    fn h_read<C, R>(
        h: &mut BufReader<R>,
        new_version: Version,
        tx: &StdLookupTx,
        hns: &mut HeaderAndSuppOffsets,
        scan_next_dataset: bool,
        start_time: Instant,
        st: &TEXTReadState<C>,
    ) -> WarningsAndIOGroupResult<
        Self,
        ReadFlatDatasetFromKwsOutputWarning,
        ReadFlatDatasetFromKwsOutputError,
        (),
    >
    where
        R: Read + Seek,
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadOffsetConfig> + AsRef<ReadDatasetConfig>,
    {
        let lookup_res = match new_version {
            Version::FCS2_0 => Version2_0::h_lookup_and_read(h, tx, hns, start_time, st),
            Version::FCS3_0 => Version3_0::h_lookup_and_read(h, tx, hns, start_time, st),
            Version::FCS3_1 => Version3_1::h_lookup_and_read(h, tx, hns, start_time, st),
            Version::FCS3_2 => Version3_2::h_lookup_and_read(h, tx, hns, start_time, st),
        };

        lookup_res
            .map_pure_errors(ReadFlatDatasetFromKwsOutputError::from)
            .map_commutative_warnings(ReadFlatDatasetFromKwsOutputWarning::from)
            .and_then_commutative(|out| {
                let snd = scan_next_dataset;
                let v = new_version;
                let d = &out.ds_offsets;
                let ed = out.event_diag;
                let t = &out.timings;
                DatasetDiagnostics::from_parts(h, v, ed, hns, d, snd, t, st)
                    .map_commutative_warnings(ReadFlatDatasetFromKwsOutputWarning::from)
                    .map_pure_errors(ReadFlatDatasetFromKwsOutputError::from)
                    .repack_warnings()
                    .map_ok_value(|ds_diag| {
                        Self::new(
                            out.df,
                            out.analysis,
                            out.others,
                            out.ds_offsets,
                            out.schema_diag,
                            ds_diag,
                        )
                    })
            })
    }
}

impl FlatTEXTOutput {
    /// Read flat TEXT from file handle.
    fn h_read<C, R>(
        h: &mut BufReader<R>,
        mut st: HeaderReadState<C>,
    ) -> WarningsAndErrorResult<
        FlatTEXTOutputInner<Self, C>,
        (),
        HeaderOrFlatTEXTWarning,
        IOErrorGroup<HeaderOrFlatTextError, ()>,
    >
    where
        R: Read + Seek,
        C: AsRef<ReadHeaderAndTEXTConfig> + AsRef<ReadHeaderInnerConfig> + AsRef<ReadOffsetConfig>,
    {
        Header::h_read(h, &mut st)
            .map_commutative_warnings(HeaderOrFlatTEXTWarning::from)
            .map_pure_errors(HeaderOrFlatTextError::from)
            .and_then_commutative(|out| {
                Self::h_read_from_header(h, out.header, out.read_end, st)
                    .map_commutative_warnings(HeaderOrFlatTEXTWarning::from)
                    .map_pure_errors(HeaderOrFlatTextError::from)
            })
    }

    /// Read flat TEXT from file handle with offsets from HEADER.
    fn h_read_from_header<C, R>(
        h: &mut BufReader<R>,
        mut header: Header,
        start_time: Instant,
        st: HeaderReadState<C>,
    ) -> WarningsAndIOGroupResult<
        FlatTEXTOutputInner<Self, C>,
        ParseFlatTEXTWarning,
        ParseFlatTEXTError,
        (),
    >
    where
        R: Read + Seek,
        C: AsRef<ReadHeaderAndTEXTConfig> + AsRef<ReadOffsetConfig>,
    {
        let conf: &ReadHeaderAndTEXTConfig = st.conf().as_ref();
        // Clip the primary TEXT offsets if they exceed EOF.
        let ptext_overflow = match header.final_offsets.try_truncate_primary_text(&st) {
            Ok(overflow) => overflow,
            Err(e) => {
                let pure = IOErrorGroup::new_pure_one(ParseFlatTEXTError::from(e));
                return LogResult::new_err(pure);
            }
        };

        let ptext_offsets: &PrimaryTextOffsets = header.final_offsets.as_ref();

        let Some(ne_ptext_offsets) = ptext_offsets.as_nonempty() else {
            let e = IOErrorGroup::new_pure_one(EmptyTEXTError.into());
            return LogResult::new_err(e);
        };

        let ptext_bytes = io_to_log!(ne_ptext_offsets.h_read_contents(h));
        let penc = conf.use_encoding.choose(ptext_bytes.as_ref());

        let ptext_ne_slice = ptext_bytes.as_nonempty_slice();
        let delim_res = split_first_delim(ptext_ne_slice, conf)
            .map_errors(ParseFlatTEXTError::from)
            .map_commutative_warnings(ParseFlatTEXTWarning::from)
            .into_semigroup();

        // TODO note in standards compliance document that the only two keywords
        // that are absolutely mandatory to be in the primary text are the two
        // stext offsets (for FCS3.0+) and $NEXTDATA since I make no distinction
        // if a keyword (required or not) comes from primary or supp unless it
        // is necessary for parsing supp itself. The standards say that all
        // required keywords need to be in primary.
        delim_res
            .group()
            .map_error(IOErrorGroup::Pure)
            .and_then_commutative(|(delim, bytes)| {
                let c = st.conf().as_ref();
                SplitTEXTDiagnostics::primary_from_bytes(delim, bytes, penc, c)
                    .map_commutative_warnings(ParseFlatTEXTWarning::from)
                    .map_errors(ParseFlatTEXTError::from)
                    .group()
                    .map_error(IOErrorGroup::Pure)
                    .and_then_commutative(|(index, nonstd, diag)| {
                        Nextdata::lookup_ro(&index, ptext_offsets, st)
                            .map_commutative_warnings(ParseFlatTEXTWarning::from)
                            .map_errors(ParseFlatTEXTError::from)
                            .into_semigroup()
                            .map_ok_value(|(nextdata, txt_st)| {
                                (delim, index, nonstd, diag, nextdata, txt_st)
                            })
                            .group()
                            .map_error(IOErrorGroup::Pure)
                    })
            })
            .and_then_commutative(
                |(delim, prim_index, mut nonstd, prim_diag, nextdata, txt_st)| {
                    SuppTEXTOffsetsOutput::lookup(&prim_index, &mut header, &txt_st)
                        .map_commutative_warnings(ParseFlatTEXTWarning::from)
                        .map_errors(ParseFlatTEXTError::from)
                        .group()
                        .map_error(IOErrorGroup::Pure)
                        .and_then_commutative(|supp_out| {
                            let ne_offsets =
                                supp_out.as_offset_pair().and_then(|p| p.as_nonempty());
                            if let Some(ne) = ne_offsets {
                                let c = txt_st.conf().as_ref();
                                SplitTEXTDiagnostics::h_read_supp(h, delim, &ne, &mut nonstd, c)
                                    .map_commutative_warnings(ParseFlatTEXTWarning::from)
                                    .map_pure_errors(ParseFlatTEXTError::from)
                                    .map_ok_value(|(supp_index, mut supp_diag)| {
                                        let (index, non_unique_std) = prim_index.concat(supp_index);
                                        supp_diag.non_unique_std_keywords.extend(non_unique_std);
                                        (index, supp_out, Some(supp_diag))
                                    })
                            } else {
                                LogResult::new_ok((prim_index, supp_out, None))
                            }
                        })
                        .map_ok_value(|(index, supp_out, supp_diag)| {
                            (
                                index, nonstd, nextdata, supp_out, prim_diag, supp_diag, txt_st,
                            )
                        })
                },
            )
            .and_then_commutative(
                |(index, nonstd, nextdata, supp_text_offsets, prim_out, supp_out, txt_st)| {
                    // Check if any HEADER offsets exceed $NEXTDATA
                    let hdr_trunc_res = header
                        .final_offsets
                        .try_truncate_non_primary_text(&txt_st)
                        .nowarn_into_warn()
                        .map_errors(ParseFlatTEXTError::from)
                        .group()
                        .map_error(IOErrorGroup::Pure);

                    let vkws = ValidKeywords::new(index, nonstd.pstd, nonstd.nonstd);
                    let header_supp =
                        HeaderAndSuppOffsets::new(header, supp_text_offsets, nextdata);

                    hdr_trunc_res.map_ok_value(|header_overflows| {
                        let text_read_end = Instant::now();
                        let read_text_ns = text_read_end.duration_since1(start_time).as_nanos();
                        let diag = FlatTEXTDiagnostics {
                            header_supp,
                            primary_text_overflow: ptext_overflow,
                            header_overflows,
                            read_text_ns,
                            primary_split: prim_out,
                            supp_split: supp_out,
                        };
                        FlatTEXTOutputInner::new(Self::new(vkws, diag), text_read_end, txt_st)
                    })
                },
            )
    }

    /// Convert flat TEXT into standardized TEXT.
    fn into_std_text<C>(
        mut self,
        read_text_end: Instant,
        st: &TEXTReadState<C>,
    ) -> WarningsAndErrorsResult<
        (AnyCoreTEXT, StdTEXTOutput),
        (),
        StdTEXTFromKeywordsWithOffsetsWarning,
        AnyStdTEXTFromKeywordsError,
    >
    where
        C: AsRef<ReadHeaderAndTEXTConfig>
            + AsRef<ReadRepairKeywordsConfig>
            + AsRef<ReadOffsetConfig>
            + AsRef<ReadStdKeywordsConfig>
            + AsRef<ReadDataKeywordsConfig>,
    {
        let hns = &mut self.flat_diagnostics.header_supp;
        let version = hns.header.version;
        AnyCoreTEXT::parse_flat(version, self.keywords, hns, read_text_end, st).map_ok_value(
            |out| {
                let std_out = StdTEXTOutput::new(
                    out.offsets.tot,
                    out.offsets.offsets,
                    out.std_diag,
                    self.flat_diagnostics,
                    out.repair_diag,
                    out.scores,
                    out.pseudostandard,
                );
                (out.inner, std_out)
            },
        )
    }

    /// Convert into standardized dataset, reading data as necessary.
    fn into_std_dataset<C, R>(
        mut self,
        h: &mut BufReader<R>,
        scan_next_dataset: bool,
        read_text_end: Instant,
        st: &TEXTReadState<C>,
    ) -> WarningsAndIOGroupResult<
        (AnyCoreDataset, StdDatasetOutput),
        StdDatasetFromKeywordsWarningInner,
        AnyStdDatasetFromKeywordsError,
        (),
    >
    where
        R: Read + Seek,
        C: AsRef<ReadHeaderAndTEXTConfig>
            + AsRef<ReadRepairKeywordsConfig>
            + AsRef<ReadOffsetConfig>
            + AsRef<ReadStdKeywordsConfig>
            + AsRef<ReadDataKeywordsConfig>
            + AsRef<ReadDatasetConfig>,
    {
        let hdr = &mut self.flat_diagnostics.header_supp;
        AnyCoreDataset::new_from_keywords(
            h,
            hdr,
            self.keywords,
            scan_next_dataset,
            read_text_end,
            st,
        )
        .map_ok_value(|out| {
            let dx = StdDatasetOutput::new(
                out.data,
                self.flat_diagnostics,
                out.scores,
                out.repair,
                out.pseudo,
            );
            (out.inner, dx)
        })
    }
}

impl SplitTEXTDiagnostics {
    fn build(inner: SplitTEXTDiagnosticsInner, parsed: ParsedKeywordsDiagnostic) -> Self {
        Self {
            delimiter: inner.delimiter,
            escaped: inner.escaped,
            keys_with_non_utf8_values: parsed.keys_with_non_utf8_values,
            values_with_non_ascii_keys: parsed.values_with_non_ascii_keys,
            byte_pairs: parsed.byte_pairs,
            non_unique_std_keywords: parsed.non_unique_std_keywords,
            non_unique_pstd_keywords: parsed.non_unique_pstd_keywords,
            non_unique_nonstd_keywords: parsed.non_unique_nonstd_keywords,
            keys_with_empty_trimmed_values: parsed.keys_with_empty_trimmed_values,
            keys_with_trimmed_values: parsed.keys_with_trimmed_values,
            keys_with_blank_values: inner.keys_with_blank_values,
            values_with_blank_keys: inner.values_with_blank_keys,
            skipped_pairs: inner.skipped_pairs,
            tokens_with_boundary_delims: inner.tokens_with_boundary_delims,
            last_odd_token: inner.last_odd_token,
            has_even_delims: inner.has_even_delims,
            extra_leading_delims: inner.extra_leading_delims,
            multibyte_encoded: inner.multibyte_encoded,
        }
    }

    /// Read supp TEXT from file handle and store keywords in hash table.
    fn h_read_supp<R: Read + Seek>(
        h: &mut BufReader<R>,
        delim: u8,
        offsets: &NonEmptyOffsets<SupplementalTextSegmentId, OffsetsFromTEXT>,
        nonstd: &mut ParsedNonStdKeywords,
        conf: &ReadHeaderAndTEXTConfig,
    ) -> WarningsAndIOGroupResult<
        (StdKeywords, Self),
        ParseSupplementalTEXTError,
        ParseSupplementalTEXTError,
        (),
    > {
        let bytes = io_to_log!(offsets.h_read_contents(h));
        let enc = conf.use_encoding.choose(bytes.as_ref());
        let ne = bytes.as_nonempty_slice();
        Self::supp_from_bytes(nonstd, delim, ne, enc, conf)
            .group()
            .map_error(IOErrorGroup::Pure)
    }

    /// Read primary TEXT from bytes and store keywords in hash table.
    fn primary_from_bytes(
        delim: u8,
        bytes: &[u8],
        enc: Encoding,
        conf: &ReadHeaderAndTEXTConfig,
    ) -> WarningsAndErrorsResult<
        (StdKeywords, ParsedNonStdKeywords, Self),
        (),
        ParseKeywordsIssue,
        ParseKeywordsIssue,
    > {
        let raw_tokens = Self::split_bytes(delim, bytes);
        let raw_slice = raw_tokens.as_nonempty_slice();
        let mut nonstd = ParsedNonStdKeywords::default();
        let tk = TEXTKind::Primary;
        Self::from_bytes_inner(&mut nonstd, tk, delim, raw_slice, enc, conf)
            .map_ok_value(|(index, diag)| (index, nonstd, diag))
    }

    /// Read supp TEXT from bytes and store keywords in hash table.
    fn supp_from_bytes(
        kws: &mut ParsedNonStdKeywords,
        delim: u8,
        bytes: &NESlice<u8>,
        enc: Encoding,
        conf: &ReadHeaderAndTEXTConfig,
    ) -> WarningsAndErrorsResult<
        (StdKeywords, Self),
        (),
        ParseSupplementalTEXTError,
        ParseSupplementalTEXTError,
    > {
        let (b, bs) = bytes.split_first();
        let raw_tokens = Self::split_bytes(*b, bs);
        let raw_slice = raw_tokens.as_nonempty_slice();
        let flag = conf.allow_supp_text_own_delim;
        Self::from_bytes_inner(kws, TEXTKind::Supplemental, *b, raw_slice, enc, conf)
            .map_warnings_and_errors(ParseSupplementalTEXTError::from)
            .eval_warning_or_error3(
                flag,
                |_| (),
                |()| (),
                |_| (*b != delim).then_some(DelimMismatch::new(delim, *b)),
            )
    }

    fn split_bytes(delim: u8, xs: &[u8]) -> NEVec<&[u8]> {
        xs.split(|&x| x == delim)
            .try_into_nonempty_iter()
            .expect("split should always give at least one element")
            .collect()
    }

    /// Read TEXT segment (primary or supp) from bytes.
    fn from_bytes_inner(
        nonstd: &mut ParsedNonStdKeywords,
        tk: TEXTKind,
        delim: u8,
        raw_tokens: &NESlice<&'_ [u8]>,
        enc: Encoding,
        conf: &ReadHeaderAndTEXTConfig,
    ) -> WarningsAndErrorsResult<(StdKeywords, Self), (), ParseKeywordsIssue, ParseKeywordsIssue>
    {
        let escaped = GuessedEscapeMode::is_escaped(raw_tokens, conf.delim_escape_mode);
        let trim = conf.trim_value_whitespace.is_trim();
        let (index, diag) = if escaped {
            Self::parse_escaped(nonstd, delim, raw_tokens, trim, enc)
        } else {
            Self::parse_unescaped(nonstd, delim, raw_tokens, trim, enc)
        };
        diag.finalize(tk, conf).map_ok_value(|d| (index, d))
    }

    fn finalize(
        self,
        tk: TEXTKind,
        conf: &ReadHeaderAndTEXTConfig,
    ) -> WarningsAndErrorsResult<Self, (), ParseKeywordsIssue, ParseKeywordsIssue> {
        let mut n_errors = 0;
        let mut n_warnings = 0;

        let mut count_if = |flag, val| match flag {
            Some(true) => n_errors += val,
            Some(false) => n_warnings += val,
            None => (),
        };

        let empty_key_flag = conf.allow_empty_keys.is_error();
        let delim_bound_flag = conf.allow_delim_at_boundary.is_error();
        let non_unique_flag = conf.allow_nonunique.is_error();
        let bad_key_flag = conf.allow_non_ascii_keys.is_error();
        let bad_val_flag = conf.allow_non_utf8_values.is_error();
        let bad_key_or_val_flag = bad_key_flag.zip(bad_val_flag).map(|(a, b)| a || b);
        let trimmed_flag = conf.trim_value_whitespace.is_error();
        let last_odd_flag = conf.allow_odd_tokens.is_error();
        let even_delim_flag = conf.allow_even_delims.is_error();
        let extra_delim_flag = delim_bound_flag; // TODO is this right?

        let n_non_unique = self.non_unique_std_keywords.len()
            + self.non_unique_pstd_keywords.len()
            + self.non_unique_nonstd_keywords.len();

        let blank_pairs_error =
            NonZeroUsize::new(self.skipped_pairs).map(|n| BlankPairError::new(tk, n));
        let last_odd_error = self
            .last_odd_token
            .clone()
            .into_ne()
            .map(|t| UnevenTokensError::new(tk, t));
        let even_delim_error = self.has_even_delims.then_some(EvenDelimiterError(tk));
        let extra_delim_error =
            NonZeroUsize::new(self.extra_leading_delims).map(|n| LeadingDelimError::new(tk, n));

        count_if(empty_key_flag, self.values_with_blank_keys.len());
        count_if(empty_key_flag, usize::from(blank_pairs_error.is_some()));
        count_if(delim_bound_flag, self.tokens_with_boundary_delims.len());
        count_if(non_unique_flag, n_non_unique);
        count_if(bad_key_flag, self.values_with_non_ascii_keys.len());
        count_if(bad_val_flag, self.keys_with_non_utf8_values.len());
        count_if(bad_key_or_val_flag, self.byte_pairs.len());
        count_if(trimmed_flag, self.keys_with_empty_trimmed_values.len());
        count_if(last_odd_flag, usize::from(last_odd_error.is_some()));
        count_if(even_delim_flag, usize::from(even_delim_error.is_some()));
        count_if(extra_delim_flag, usize::from(extra_delim_error.is_some()));

        let mut errors = Vec::with_capacity(n_errors);
        let mut warnings = Vec::with_capacity(n_warnings);

        let empty_keys_errors = self
            .values_with_blank_keys
            .iter()
            .map(|k| BlankKeyError::new(tk, k.to_owned()));
        let delim_bound_errors = self
            .tokens_with_boundary_delims
            .iter()
            .map(|k| DelimBoundError::new(tk, k.to_owned()));
        let non_unique_std_errors = self
            .non_unique_std_keywords
            .iter()
            .map(|(k, v)| KeyPresent::new(tk, *k, v.clone()));
        let non_unique_pseudo_errors = self
            .non_unique_pstd_keywords
            .iter()
            .map(|(k, v)| KeyPresent::new(tk, k.clone(), v.clone()));
        let non_unique_nonstd_error = self
            .non_unique_nonstd_keywords
            .iter()
            .map(|(k, v)| KeyPresent::new(tk, k.clone(), v.clone()));
        let bad_key_errors = self
            .values_with_non_ascii_keys
            .iter()
            .map(|(k, v)| NonAsciiKeyError::new(tk, k.clone(), v.clone()));
        let bad_val_errors = self
            .keys_with_non_utf8_values
            .iter()
            .map(|(k, v)| NonUtf8ValueError::new(tk, k.clone(), v.clone()));
        let bad_key_or_val_errors = self
            .byte_pairs
            .iter()
            .map(|(k, v)| NonAsciiOrUtf8KeywordError::new(tk, k.clone(), v.clone()));
        let trimmed_errors = self
            .keys_with_empty_trimmed_values
            .iter()
            .map(|(k, v)| TrimmedBlankValueError::new(tk, k.clone(), v.clone()));

        macro_rules! extend_if {
            ($flag:expr, $vals:expr) => {
                let it = $vals.into_iter().map(ParseKeywordsIssue::from);
                match $flag {
                    Some(true) => errors.extend(it),
                    Some(false) => warnings.extend(it),
                    None => (),
                }
            };
        }

        extend_if!(empty_key_flag, empty_keys_errors);
        extend_if!(empty_key_flag, blank_pairs_error);
        extend_if!(delim_bound_flag, delim_bound_errors);
        extend_if!(non_unique_flag, non_unique_std_errors);
        extend_if!(non_unique_flag, non_unique_pseudo_errors);
        extend_if!(non_unique_flag, non_unique_nonstd_error);
        extend_if!(bad_key_flag, bad_key_errors);
        extend_if!(bad_val_flag, bad_val_errors);
        extend_if!(bad_key_or_val_flag, bad_key_or_val_errors);
        extend_if!(trimmed_flag, trimmed_errors);
        extend_if!(last_odd_flag, last_odd_error);
        extend_if!(even_delim_flag, even_delim_error);
        extend_if!(extra_delim_flag, extra_delim_error);

        if let Some(ne) = NEVec::try_from_vec(errors) {
            LogResult::new_from_ne_err_iter(ne, ()).set_commutative_warnings(warnings)
        } else {
            LogResult::new_ok(self).set_commutative_warnings(warnings)
        }
    }

    fn parse_escaped(
        nonstd: &mut ParsedNonStdKeywords,
        delim: u8,
        segs: &NESlice<&[u8]>,
        trim: bool,
        enc: Encoding,
    ) -> (StdKeywords, Self) {
        let mut diag = ParsedKeywordsDiagnostic::default();
        let mut extra_leading_delims = 0;
        let mut tokens_with_boundary_delims = vec![];

        let go =
            |delim_bound_tokens, last_odd_token, has_even_delims, extra_leading_delims_, diag_| {
                let inner = SplitTEXTDiagnosticsInner::new_escaped(
                    delim,
                    delim_bound_tokens,
                    last_odd_token,
                    has_even_delims,
                    extra_leading_delims_,
                    enc.is_multi(),
                );
                Self::build(inner, diag_)
            };

        // Estimate necessary capacity for destination vector based on length of
        // input. If any delimiters are escaped, this will lead to fewer
        // keywords and this estimate will overshoot.
        let mut parsed = Vec::with_capacity(segs.len().get() / 2);

        // The number of blanks which are found in a row
        let mut consec_blanks = 0_usize;

        // Dynamic buffers to hold tokens with escaped delimiters. This is
        // necessary because we cannot just copy escaped text as-is; we need to
        // remove every other delimiter to make it literal, which implies we
        // need to allocate a new string.
        let mut keybuf: NEVec<u8>;
        let mut valbuf: Option<NEDelimBytes> = None;

        let mut it = segs.iter();

        // Prime the loop with the first token which belongs to a key. This
        // will fail if TEXT is entirely delimiters, in which case there is
        // nothing more to do.
        keybuf = if let Some(token0) = it.by_ref().find_map(|token| {
            let ne = NESlice::try_from_slice(token);
            if ne.is_none() {
                extra_leading_delims += 1;
            }
            ne
        }) {
            token0.to_ne_vec()
        } else {
            // No tokens found, which means TEXT is entirely delimiters (which
            // includes TEXT being just one delim and otherwise empty).
            let text_diag = go(
                tokens_with_boundary_delims,
                StringOrBytes::default(),
                false,
                extra_leading_delims,
                diag,
            );
            return (StdKeywords::default(), text_diag);
        };

        // Determine if the number of delimiters is even or odd, throw an error
        // for the former. Remove leading delimiters since we 'pretend' that
        // TEXT is missing one delimiter if this number is odd (which means the
        // actual number of leading delims is even since we already counted
        // the first before running this function).
        let has_even_delims = (segs.len().get() - extra_leading_delims) & 1 == 0;

        for token in it {
            if let Some(ne_token) = NESlice::try_from_slice(token) {
                if consec_blanks & 1 == 0 {
                    // Previous consecutive delimiter sequence was odd (which
                    // means the number of blanks is even). This is a token
                    // boundary, and the last sequence of token can be processed
                    // as needed.
                    if consec_blanks > 0 {
                        // If we have more than one delimiter (more than zero
                        // blanks) then there are multiple delimiters on the end
                        // which is not allowed. Scream at user, they will be
                        // happy and enlightened.
                        let seg = NEStringOrBytes::from(ne_token.to_ne_vec());
                        tokens_with_boundary_delims.push(seg);
                    }
                    if let Some(ne_val) = mem::take(&mut valbuf) {
                        let kb = keybuf.as_nonempty_slice();
                        let p = ParsedKeyword::from_pair(kb, ne_val, trim, enc);
                        parsed.push(p);
                        keybuf = ne_token.to_ne_vec();
                    } else {
                        valbuf = Some(NEDelimBytes::init(ne_token, delim));
                    }
                } else if let Some(b) = NonZeroUsize::new(consec_blanks) {
                    // Previous consecutive delimiter sequence was even and
                    // non-zero. Push this number / 2 followed by the current
                    // token fragment to the active buffer.
                    let n_delim = b.div_ceil(NonZeroUsize::new(2).unwrap());
                    let ds = iter::repeat_n(delim, n_delim.get());
                    if let Some(v) = valbuf.as_mut() {
                        v.append(ne_token, n_delim);
                    } else {
                        keybuf.extend(ds.chain(ne_token.iter().copied()));
                    }
                }
                consec_blanks = 0;
            } else {
                consec_blanks += 1;
            }
        }

        // If the number of consecutive blanks was odd and greater than zero,
        // the last token ended with a string of escaped delimiters which was
        // not captured at the end of the loop.
        let has_escaped_delim_end = consec_blanks > 1 && consec_blanks & 1 == 1;

        // Unprime the loop since we can only add a key/val pair after
        // encountering the delimiter boundary after the value token. If there
        // was an even number of tokens, we will have both a key and value that
        // can be pushed. If we only have a key, keep this as last odd token.
        let last_odd_token = if let Some(ne_val) = mem::take(&mut valbuf) {
            if has_escaped_delim_end {
                let seg = ne_val.as_owned();
                tokens_with_boundary_delims.push(NEStringOrBytes::from(seg));
            }
            // Both key and value are present, this is the last pair in TEXT so
            // push to the end of keywords
            let kb = keybuf.as_nonempty_slice();
            let p = ParsedKeyword::from_pair(kb, ne_val, trim, enc);
            parsed.push(p);
            StringOrBytes::default()
        } else {
            if has_escaped_delim_end {
                let seg = keybuf.as_nonempty_slice();
                tokens_with_boundary_delims.push(NEStringOrBytes::from(seg));
            }
            // Only key is present which means we have an odd number of tokens.
            Vec::from(keybuf).into()
        };

        let mut counts = ParsedKeywordCounts::default();

        for p in &parsed {
            p.count(&mut counts);
        }

        diag.reserve(&counts);

        nonstd.reserve(&counts);

        let (index, non_unique_std) = if counts.std_owned_kws == 0 {
            let mut std = Vec::with_capacity(counts.std_slice_kws);
            for p in parsed {
                p.dispatch_slice_only(&mut std, nonstd, &mut diag);
            }
            StdKeywords::from_vec(std)
        } else {
            let mut std = Vec::with_capacity(counts.std_slice_kws + counts.std_owned_kws);
            for p in parsed {
                p.dispatch_slice_or_owned(&mut std, nonstd, &mut diag);
            }
            StdKeywords::from_vec(std)
        };

        diag.non_unique_std_keywords = non_unique_std;

        let text_diag = go(
            tokens_with_boundary_delims,
            last_odd_token,
            has_even_delims,
            extra_leading_delims,
            diag,
        );

        (index, text_diag)
    }

    fn parse_unescaped(
        nonstd: &mut ParsedNonStdKeywords,
        delim: u8,
        segs: &NESlice<&[u8]>,
        trim: bool,
        enc: Encoding,
    ) -> (StdKeywords, Self) {
        enum Unescaped<'a> {
            Keyword(ParsedKeyword<'a>),
            EmptyKey(NEVec<u8>),
            EmptyValue(NEVec<u8>),
            EmptyPair,
        }

        let (pairs, extra_token, has_even_tokens) = Self::trim_tokens_end(segs);

        let has_even_delims = !has_even_tokens;

        let last_odd_token = extra_token
            .as_ref()
            .map(|s| s.as_ref().to_vec().into())
            .unwrap_or_default();

        let parsed: Vec<_> = pairs
            .iter()
            .tuples()
            .map(|(key, value)| {
                let k = NESlice::try_from_slice(key);
                let v = NESlice::try_from_slice(value);
                match (k, v) {
                    (Some(kk), Some(vv)) => {
                        Unescaped::Keyword(ParsedKeyword::from_pair(kk, vv, trim, enc))
                    }
                    (Some(kk), None) => Unescaped::EmptyValue(kk.to_ne_vec()),
                    (None, Some(vv)) => Unescaped::EmptyKey(vv.to_ne_vec()),
                    (None, None) => Unescaped::EmptyPair,
                }
            })
            .collect();

        let mut counts = ParsedKeywordCounts::default();
        let mut n_empty_keys = 0;
        let mut n_empty_values = 0;
        let mut n_empty_pairs = 0;

        for p in &parsed {
            match p {
                Unescaped::Keyword(k) => k.count(&mut counts),
                Unescaped::EmptyKey(_) => n_empty_keys += 1,
                Unescaped::EmptyValue(_) => n_empty_values += 1,
                Unescaped::EmptyPair => n_empty_pairs += 1,
            }
        }

        let mut diag = ParsedKeywordsDiagnostic::default();
        diag.reserve(&counts);

        nonstd.reserve(&counts);

        let mut values_with_blank_keys = Vec::with_capacity(n_empty_keys);
        let mut keys_with_blank_values = Vec::with_capacity(n_empty_values);

        let (index, non_unique_std) = if counts.std_owned_kws == 0 {
            let mut std = Vec::with_capacity(counts.std_slice_kws);
            for p in parsed {
                match p {
                    Unescaped::Keyword(k) => k.dispatch_slice_only(&mut std, nonstd, &mut diag),
                    Unescaped::EmptyKey(k) => values_with_blank_keys.push(k.into()),
                    Unescaped::EmptyValue(v) => keys_with_blank_values.push(v.into()),
                    Unescaped::EmptyPair => (),
                }
            }
            StdKeywords::from_vec(std)
        } else {
            let mut std = Vec::with_capacity(counts.std_slice_kws + counts.std_owned_kws);
            for p in parsed {
                match p {
                    Unescaped::Keyword(k) => k.dispatch_slice_or_owned(&mut std, nonstd, &mut diag),
                    Unescaped::EmptyKey(k) => values_with_blank_keys.push(k.into()),
                    Unescaped::EmptyValue(v) => keys_with_blank_values.push(v.into()),
                    Unescaped::EmptyPair => (),
                }
            }
            StdKeywords::from_vec(std)
        };

        diag.non_unique_std_keywords = non_unique_std;

        let inner = SplitTEXTDiagnosticsInner::new_unescaped(
            delim,
            n_empty_pairs,
            keys_with_blank_values,
            values_with_blank_keys,
            last_odd_token,
            has_even_delims,
            enc.is_multi(),
        );

        (index, Self::build(inner, diag))
    }

    /// Maybe trim end off slice of tokens so that the length is even.
    ///
    /// Return final slice, the last odd non-empty slice if it was taken off,
    /// and a boolean that will be `true` if the number of tokens started as
    /// even. The 'perfect' case (ie standards compliant FCS file) is `None` and
    /// `true` for the odd slice and boolean. All combinations are possible.
    fn trim_tokens_end<'a, 'b>(
        raw_tokens: &'b NESlice<&'a [u8]>,
    ) -> (&'b [&'a [u8]], Option<&'a NESlice<u8>>, bool) {
        let has_even_tokens = raw_tokens.len().get() & 1 == 1;
        let (&last, rest) = raw_tokens.split_last();
        let mut extra_token = None;
        let even_tokens = match (has_even_tokens, NESlice::try_from_slice(last)) {
            // Delimiter number is odd and last token is empty. This should
            // happen in a perfect situation since the final token should be
            // empty if TEXT ends with a delimiter, and the total number of
            // delimiters should be odd (which means the number of tokens is
            // even). This second part is true regardless of escaping.
            //
            // Return all but last empty token as it is a blank.
            (true, None) => rest,
            // Delimiter number is odd but last token is not empty. This means
            // there is an extra token at the end without a delimiter. Usually
            // this 'token' is whitespace padding.
            (true, extra) => {
                extra_token = extra;
                rest
            }
            // Delimiter number is even but last token is empty. This means
            // TEXT ended with a delimiter but the number of tokens is odd.
            // The last odd token may be blank, in which case TEXT ended with
            // two delimiters and the real one is 2nd from the end. This will
            // remove both since neither are necessary.
            (false, None) => {
                let (penultimate_token, segs) = rest.split_last().expect(
                    "this should never fail because input is non empty and \
                     and we branch here if length is even",
                );
                extra_token = NESlice::try_from_slice(penultimate_token);
                segs
            }
            // Delimiter number is even and last token is not empty. This
            // means TEXT did not end with a delimiter and the number of tokens
            // is even.
            (false, Some(_)) => raw_tokens.as_ref(),
        };
        assert!(
            even_tokens.len() & 1 == 0,
            "number of tokens should be even"
        );
        (even_tokens, extra_token, has_even_tokens)
    }
}

struct SplitTEXTDiagnosticsInner {
    delimiter: u8,
    escaped: bool,
    skipped_pairs: usize,
    keys_with_blank_values: Vec<NEStringOrBytes>,
    values_with_blank_keys: Vec<NEStringOrBytes>,
    tokens_with_boundary_delims: Vec<NEStringOrBytes>,
    last_odd_token: StringOrBytes,
    has_even_delims: bool,
    extra_leading_delims: usize,
    multibyte_encoded: bool,
}

impl SplitTEXTDiagnosticsInner {
    fn new_escaped(
        delimiter: u8,
        tokens_with_boundary_delims: Vec<NEStringOrBytes>,
        last_odd_token: StringOrBytes,
        has_even_delims: bool,
        extra_leading_delims: usize,
        multibyte_encoded: bool,
    ) -> Self {
        Self {
            delimiter,
            escaped: true,
            // these are only possible if blanks are allowed; they aren't in
            // escaped mode
            skipped_pairs: 0,
            keys_with_blank_values: vec![],
            values_with_blank_keys: vec![],
            tokens_with_boundary_delims,
            last_odd_token,
            has_even_delims,
            extra_leading_delims,
            multibyte_encoded,
        }
    }

    fn new_unescaped(
        delimiter: u8,
        skipped_pairs: usize,
        keys_with_blank_values: Vec<NEStringOrBytes>,
        values_with_blank_keys: Vec<NEStringOrBytes>,
        last_odd_token: StringOrBytes,
        has_even_delims: bool,
        multibyte_encoded: bool,
    ) -> Self {
        Self {
            delimiter,
            escaped: false,
            skipped_pairs,
            keys_with_blank_values,
            values_with_blank_keys,
            // this is only possible in unescaped since consecutive delims will
            // be interpreted as blanks
            tokens_with_boundary_delims: vec![],
            last_odd_token,
            has_even_delims,
            // ditto for leading delimiters, these will be read as blanks
            extra_leading_delims: 0,
            multibyte_encoded,
        }
    }
}

impl GuessedEscapeMode {
    fn is_escaped(segs: &NESlice<&[u8]>, mode: DelimEscapeMode) -> bool {
        let go = |default| match Self::test_both_modes(segs) {
            Self::Escaped => true,
            Self::Unescaped => false,
            Self::Ambiguous => default,
        };
        match mode {
            DelimEscapeMode::Unescaped => false,
            // Only choose escaped if there is at least one blank token,
            // otherwise it doesn't matter which mode we use and it is faster to
            // use unescaped
            DelimEscapeMode::Escaped => Self::has_any_empty(segs),
            DelimEscapeMode::GuessEscaped => go(true),
            DelimEscapeMode::GuessUnescaped => go(false),
        }
    }

    fn has_any_empty(raw_tokens: &NESlice<&[u8]>) -> bool {
        // Only consider the first even number of tokens since both modes should
        // deal with extra crap at the end in the same way
        let (segs, _, _) = SplitTEXTDiagnostics::trim_tokens_end(raw_tokens);
        segs.iter().any(|s| s.is_empty())
    }

    fn test_both_modes(raw_tokens: &NESlice<&[u8]>) -> Self {
        // Only consider the first even number of tokens since both modes
        // should deal with extra crap at the end in the same way
        let (segs, _, _) = SplitTEXTDiagnostics::trim_tokens_end(raw_tokens);

        let mut any_empty_tokens = false;
        let mut any_unescaped_blank_keys = false;
        let mut any_escaped_delims_in_keys = false;
        let mut prev_escaped_was_key = false;

        // Loop through tokens as if in either escaped or unescaped mode
        // and test if we have any blank keys (unescaped) or keys with escaped
        // delims (escaped). Also track if we have any empty tokens at all,
        // because if we have none then the choice of mode doesn't matter and
        // we can choose whatever is fastest to maximize performance.
        for (i, s) in segs.iter().enumerate() {
            // In unescaped mode, even tokens are keys; test if any are blank
            if i & 1 == 0 && s.is_empty() {
                any_unescaped_blank_keys = true;
            }
            // In escaped mode, record if we encounter two consecutive
            // delimiters (ie a blank token) while in a key.
            if s.is_empty() {
                any_empty_tokens = true;
                if prev_escaped_was_key {
                    any_escaped_delims_in_keys = true;
                }
            } else {
                prev_escaped_was_key = !prev_escaped_was_key;
            }
            if any_unescaped_blank_keys && any_escaped_delims_in_keys {
                break;
            }
        }

        // If there were no empty tokens, it doesn't matter which mode is active
        // so pick unescaped since it is faster
        if !any_empty_tokens {
            return Self::Unescaped;
        }

        match (any_unescaped_blank_keys, any_escaped_delims_in_keys) {
            (true, true) => Self::Ambiguous,
            (true, false) => Self::Escaped,
            _ => Self::Unescaped,
        }
    }
}

impl SuppTEXTOffsetsOutput {
    fn as_offset_pair(&self) -> Option<SupplementalTextOffsets> {
        if let Self::Valid(valid) = self {
            Some(valid.final_)
        } else {
            None
        }
    }

    #[allow(clippy::too_many_lines)]
    fn lookup<C>(
        index: &StdKeywords,
        header: &mut Header,
        st: &TEXTReadState<C>,
    ) -> WarningsAndErrorsResult<Self, (), STextOffsetsWarning, STextOffsetsError>
    where
        C: AsRef<ReadHeaderAndTEXTConfig> + AsRef<ReadOffsetConfig>,
    {
        enum OffsetResult {
            Empty,
            Missing,
            Malformed(OriginalOffsets),
            Valid(SupplementalTextOffsets, OriginalOffsets),
        }

        fn get_req<T>(index: &StdKeywords) -> Result<i128, ReqKeyErrorInner_<ParseIntError, T, ()>>
        where
            T: ValueToStdKey<Index = ()>,
        {
            match NEStr::try_new(index.get(&T::std0())) {
                Some(v) => v
                    .as_str()
                    .parse::<i128>()
                    .map_err(|e| ParseKeyError::new1(e, (), v.to_owned()))
                    .map_err(ReqKeyErrorInner_::from),
                None => Err(ReqKeyErrorInner_::from(MissingKeyError::new1(()))),
            }
        }

        fn get_opt<T>(index: &StdKeywords) -> Result<Option<i128>, ParseKeyError<ParseIntError, T>>
        where
            T: ValueToStdKey<Index = ()>,
        {
            NEStr::try_new(index.get(&T::std0()))
                .map(|v| {
                    v.parse::<i128>()
                        .map_err(|e| ParseKeyError::new1(e, (), v.to_owned()))
                })
                .transpose()
        }

        let hconf: &ReadHeaderAndTEXTConfig = st.conf().as_ref();
        let oconf: &ReadOffsetConfig = st.conf().as_ref();
        let config_corr = hconf.supp_text_correction;

        let validate_offsets =
            |hdr: &mut Header, mut final_supp: SupplementalTextOffsets, orig_supp, other_index| {
                let overlap_limit = oconf.overlap_correction_limit;
                let overflow_res = if let Some(ne) = final_supp.as_nonempty_mut() {
                    ne.truncate_dataset_len((), st)
                        .map_err(STextOffsetsError::from)
                        .into_log()
                } else {
                    LogResult::new_ok(None)
                };
                let overlap_res = hdr
                    .final_offsets
                    .validate_supp_text(&mut final_supp, overlap_limit)
                    .map_errors(STextOffsetsError::from)
                    .set_err_value(());
                overflow_res
                    .zip_commutative(overlap_res)
                    .map_ok_value(|(overflow, overlaps)| {
                        let valid = ValidSuppTEXTOffsets::new(
                            final_supp,
                            orig_supp,
                            other_index,
                            overlaps,
                            overflow,
                        );
                        Self::Valid(valid)
                    })
                    .nowarn_into_warn()
            };

        // At this point, we have not yet overridden the version since we have
        // not read STEXT and therefore might not have all keywords. This puts
        // us in a bit of an awkward spot in the case we wish to autodetect the
        // version. Primary TEXT by definition must have all required keywords,
        // so we can use $BEGIN/ENDDATA to test if the version is 3.0 or higher.
        // Additionally, we can use lack of $CYT to test if the version is less
        // then 3.2, although in practice this keyword is usually present
        // despite it being optional pre-3.2. This all likely doesn't matter
        // much anyways since STEXT is seldom used.
        let ver = match hconf.version_override {
            None => header.version,
            Some(VersionOverride::Force(v)) => v,
            Some(VersionOverride::AutoDetect { .. }) => {
                if index.contains_key(&RootKey::Begindata.to_std0())
                    || index.contains_key(&RootKey::Enddata.to_std0())
                {
                    if index.contains_key(&RootKey::Cyt.to_std0()) {
                        Version::FCS3_2
                    } else {
                        Version::FCS3_1
                    }
                } else {
                    Version::FCS2_0
                }
            }
        };

        let res = match ver {
            Version::FCS2_0 => LogResult::new_ok(OffsetResult::Empty),
            Version::FCS3_0 | Version::FCS3_1 => {
                let x0 = get_req::<Beginstext>(index).map_err(ReqSegmentKeyError::Begin);
                let x1 = get_req::<Endstext>(index).map_err(ReqSegmentKeyError::End);
                let pair = OneOrTwo::from_results(x0, x1);
                let res = match SupplementalTextSegmentId::with_req_pair(pair, config_corr, st) {
                    PairResult::Valid(final_, orig) => Ok(OffsetResult::Valid(final_, orig)),
                    PairResult::Malformed(orig, e) => {
                        let r = OffsetResult::Malformed(orig);
                        Err((r, OneOrTwo::One(ReqOffsetsError::Segment(e))))
                    }
                    PairResult::Unparsed(es) => {
                        Err((OffsetResult::Missing, es.fmap(ReqOffsetsError::Key)))
                    }
                };
                match res {
                    Ok(x) => LogResult::new_ok(x),
                    Err((x, es)) => {
                        if hconf.ignore_supp_text.is_set() {
                            LogResult::new_ok(x)
                        } else {
                            let flag = hconf.allow_missing_supp_text;
                            SwitchableErrorsResult::new_deferred_switchable_iter3(x, es, flag)
                                .map_switchable_errors(STextOffsetsError::from)
                                .switchable_into_commutative()
                                .map_commutative_warnings(STextOffsetsWarning::from)
                        }
                    }
                }
            }
            Version::FCS3_2 => {
                let x0 = get_opt::<Beginstext>(index).map_err(OptSegmentKeyError::Begin);
                let x1 = get_opt::<Endstext>(index).map_err(OptSegmentKeyError::End);
                let pair = OneOrTwo::from_results(x0, x1).map(|(x, y)| x.zip(y));
                let res = match SupplementalTextSegmentId::with_opt_pair(pair, config_corr, st) {
                    None => Ok(OffsetResult::Empty),
                    Some(PairResult::Valid(final_, orig)) => Ok(OffsetResult::Valid(final_, orig)),
                    Some(PairResult::Malformed(orig, e)) => {
                        let r = OffsetResult::Malformed(orig);
                        Err((r, OneOrTwo::One(OptOffsetsError::Segment(e))))
                    }
                    Some(PairResult::Unparsed(es)) => {
                        Err((OffsetResult::Missing, es.fmap(OptOffsetsError::Key)))
                    }
                };
                match res {
                    Ok(x) => LogResult::new_ok(x),
                    Err((x, es)) => {
                        if hconf.ignore_supp_text.is_set() {
                            LogResult::new_ok(x)
                        } else {
                            let mut out = DeferredWarningsAndErrors::new_ok(x);
                            out.extend_commutative_warnings(es);
                            out.map_commutative_warnings(STextOffsetsWarning::from)
                        }
                    }
                }
            }
        };

        res.set_err_value(()).and_then_commutative(|offset_res| {
            match offset_res {
                OffsetResult::Empty => LogResult::new_ok(Self::Empty),
                OffsetResult::Malformed(uncorr) => {
                    let out = if hconf.ignore_supp_text.is_set() {
                        Self::Ignored(Some(uncorr))
                    } else {
                        Self::Malformed(uncorr)
                    };
                    LogResult::new_ok(out)
                }
                OffsetResult::Missing => {
                    let out = if hconf.ignore_supp_text.is_set() {
                        Self::Ignored(None)
                    } else {
                        Self::Unparsed
                    };
                    LogResult::new_ok(out)
                }
                OffsetResult::Valid(final_supp, orig_supp) => {
                    // Return original without any processing if ignored
                    if hconf.ignore_supp_text.is_set() {
                        return LogResult::new_ok(Self::Ignored(Some(orig_supp)));
                    }

                    // Offsets found, check for validity
                    let uncorr_ptxt = header.original_offsets.text;
                    let uncorr_anal = header.original_offsets.analysis;
                    let uncorr_others = &mut header.original_offsets.other[..];

                    let go = |loc, ret| {
                        // Supp TEXT is identical to another offset pair. Keep
                        // the other pair.
                        //
                        // TODO it may be necessary to configure which pair to
                        // keep in the future.
                        let flag = hconf.allow_duplicated_supp_text;
                        let e = DuplicateSTextError::new(orig_supp, loc, false);
                        SwitchableErrorsResult::new_switchable3(ret, (), e, flag)
                            .map_switchable_errors(STextOffsetsError::from)
                            .switchable_into_commutative()
                            .map_commutative_warnings(STextOffsetsWarning::from)
                    };

                    if final_supp.is_empty() {
                        // supp TEXT is empty, return as-is
                        let valid =
                            ValidSuppTEXTOffsets::new(final_supp, orig_supp, None, vec![], None);
                        LogResult::new_ok(Self::Valid(valid))
                    } else if uncorr_ptxt == orig_supp {
                        // Primary and supp are identical, keep primary
                        go(AnyRegion::Text, Self::DuplicatesPrimaryTEXT)
                    } else if uncorr_ptxt == uncorr_anal {
                        // Supp and ANALYSIS are the same, keep latter
                        go(AnyRegion::Analysis, Self::DuplicatesAnalysis)
                    } else if let Some(i) = uncorr_others.iter().position(|s| s == &orig_supp) {
                        // Supp and one OTHER offset are the same, keep Supp and
                        // remove matching OTHER with the assumption that Supp
                        // is actually a real supp text and not some binary
                        // blob.
                        //
                        // TODO this assumption can be checked by reading the
                        // segment but this would make this function way more
                        // complex.
                        //
                        // See FR-FCM-ZZZ4/MVa2011-06-30_fcs31.fcs for an
                        // example of this configuration
                        header.final_offsets.remove_other(i);
                        let flag = hconf.allow_duplicated_supp_text;
                        let e = DuplicateSTextError::new(orig_supp, AnyRegion::Other, true);
                        SwitchableErrorsResult::new_switchable3((), (), e, flag)
                            .map_switchable_errors(STextOffsetsError::from)
                            .switchable_into_commutative()
                            .map_commutative_warnings(STextOffsetsWarning::from)
                            .and_then_commutative(|()| {
                                validate_offsets(header, final_supp, orig_supp, Some(i))
                            })
                    } else {
                        // Supp not identical to anything else, check for
                        // overlaps and keep if there are none. ASSUME the
                        // HEADER offsets have already been validated and
                        // adjusted such that they do not overlap.
                        validate_offsets(header, final_supp, orig_supp, None)
                    }
                }
            }
        })
    }

    // This enum would be very complex to impl in python as a union type.
    // Instead, make a wrapper class with methods that project various
    // components of the enum to the user. For instance, the level of the enum
    // will be projected as a string literal, the uncorrected offsets will be
    // projected as (int, int) | None, etc. The __new__ method for this will
    // then take all these projections in reverse and validated the
    // presence/absence of them. It would be nice if we could just use the
    // type-safe nature of the enum in python, but python's type system is not
    // good enough for that.

    /// Create a new enum.
    ///
    /// This is intended to be called by __new__ on the python side.
    #[cfg(feature = "python")]
    pub fn py_try_new(
        level: py::SuppTEXTOffsetOriginType,
        seg: Option<SupplementalTextOffsets>,
        uncorr: Option<OriginalOffsets>,
        other_index: Option<usize>,
        overlaps: Vec<SuppToHeaderOffsetsOverlap>,
        overflow: Option<SuppOffsetsOverflow>,
    ) -> PyResult<Self> {
        match (level, seg, uncorr, other_index, &overlaps[..], overflow) {
            (py::SuppTEXTOffsetOriginType::Empty, None, None, None, [], None) => Ok(Self::Empty),
            (py::SuppTEXTOffsetOriginType::Unparsed, None, None, None, [], None) => {
                Ok(Self::Unparsed)
            }
            (py::SuppTEXTOffsetOriginType::Malformed, None, Some(u), None, [], None) => {
                Ok(Self::Malformed(u))
            }
            (py::SuppTEXTOffsetOriginType::DuplicatesPrimaryTEXT, None, None, None, [], None) => {
                Ok(Self::DuplicatesPrimaryTEXT)
            }
            (py::SuppTEXTOffsetOriginType::DuplicatesAnalysis, None, None, None, [], None) => {
                Ok(Self::DuplicatesAnalysis)
            }
            (py::SuppTEXTOffsetOriginType::Ignored, None, u, None, [], None) => {
                Ok(Self::Ignored(u))
            }
            (py::SuppTEXTOffsetOriginType::DuplicatesOther, Some(s), Some(u), Some(i), _, _) => Ok(
                Self::Valid(ValidSuppTEXTOffsets::new(s, u, Some(i), overlaps, overflow)),
            ),
            (py::SuppTEXTOffsetOriginType::Valid, Some(s), Some(u), None, _, _) => Ok(Self::Valid(
                ValidSuppTEXTOffsets::new(s, u, None, overlaps, overflow),
            )),
            _ => Err(PyValueError::new_err(
                "invalid combination of level and values, see class-level docstring",
            )),
        }
    }

    /// Project the origin type as a string
    #[cfg(feature = "python")]
    #[must_use]
    pub fn py_origin_type(&self) -> py::SuppTEXTOffsetOriginType {
        match self {
            Self::Empty => py::SuppTEXTOffsetOriginType::Empty,
            Self::Unparsed => py::SuppTEXTOffsetOriginType::Unparsed,
            Self::Malformed(_) => py::SuppTEXTOffsetOriginType::Malformed,
            Self::DuplicatesPrimaryTEXT => py::SuppTEXTOffsetOriginType::DuplicatesPrimaryTEXT,
            Self::DuplicatesAnalysis => py::SuppTEXTOffsetOriginType::DuplicatesAnalysis,
            Self::Ignored(_) => py::SuppTEXTOffsetOriginType::Ignored,
            Self::Valid(x) => {
                if x.duplicated_other.is_some() {
                    py::SuppTEXTOffsetOriginType::DuplicatesOther
                } else {
                    py::SuppTEXTOffsetOriginType::Valid
                }
            }
        }
    }

    /// Project the original offsets if they exist
    #[cfg(feature = "python")]
    #[must_use]
    pub fn py_original_offsets(&self) -> Option<OriginalOffsets> {
        match self {
            Self::Empty
            | Self::Unparsed
            | Self::DuplicatesPrimaryTEXT
            | Self::DuplicatesAnalysis => None,
            Self::Malformed(x) => Some(*x),
            Self::Ignored(x) => *x,
            Self::Valid(x) => Some(x.original),
        }
    }

    /// The final offsets if they exist.
    pub(crate) fn final_offsets(&self) -> Option<SupplementalTextOffsets> {
        if let Self::Valid(x) = self {
            Some(x.final_)
        } else {
            None
        }
    }

    /// The final offsets if they exist.
    #[cfg(feature = "python")]
    #[must_use]
    pub fn py_final_offsets(&self) -> Option<SupplementalTextOffsets> {
        self.final_offsets()
    }

    /// The OTHER index that duplicates these offsets if applicable.
    #[cfg(feature = "python")]
    #[must_use]
    pub fn py_other_index(&self) -> Option<usize> {
        if let Self::Valid(x) = self {
            x.duplicated_other
        } else {
            None
        }
    }

    /// Offset pairs which overlap supplemental TEXT
    #[cfg(feature = "python")]
    #[must_use]
    pub fn py_overlaps(&self) -> &[SuppToHeaderOffsetsOverlap] {
        if let Self::Valid(x) = self {
            &x.overlaps[..]
        } else {
            &[]
        }
    }

    /// The amount by which this offset exceeds $NEXTDATA or EOF if applicable.
    #[cfg(feature = "python")]
    #[must_use]
    pub fn py_overflow(&self) -> Option<SuppOffsetsOverflow> {
        if let Self::Valid(x) = self {
            x.overflow
        } else {
            None
        }
    }
}

fn split_first_delim<'a>(
    bytes: &'a NESlice<u8>,
    conf: &ReadHeaderAndTEXTConfig,
) -> WarningAndErrorResult<(u8, &'a [u8]), (), DelimCharError, DelimCharError> {
    let (delim, rest) = bytes.split_first();
    let is_ok = is_valid_delim(*delim);
    let e = DelimCharError(*delim);
    let flag = conf.allow_non_ascii_delim;
    SwitchableErrorResult::new_switchable_ok_if3(is_ok, (*delim, rest), (), e, flag)
        .switchable_into_commutative()
}

const fn is_valid_delim(b: u8) -> bool {
    1 <= b && b <= 126
}

mod built {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

#[cfg(test)]
mod tests {
    use super::*;
    use fireflow_types::{ne_str, nonempty::string::DisplayableNE as _};

    #[allow(clippy::needless_pass_by_value)]
    fn assert_guessed_mode(s: &str, comp: GuessedEscapeMode) {
        let segs: NEVec<_> = s
            .as_bytes()
            .split(|&x| x == b'/')
            .try_into_nonempty_iter()
            .unwrap()
            .collect();
        let slice = segs.as_nonempty_slice();
        assert_eq!(GuessedEscapeMode::test_both_modes(&slice), comp);
    }

    #[test]
    fn split_text_escape() {
        let mut kws = ParsedKeywords::default();
        let conf = ReadHeaderAndTEXTConfig::default();
        // NOTE should not start with delim
        let bytes = b"$P4F/700//75 BP/";
        let delim = b'/';
        let raw_tokens: NEVec<_> = bytes
            .split(|&x| x == delim)
            .try_into_nonempty_iter()
            .unwrap()
            .collect();
        let raw_slice = raw_tokens.as_nonempty_slice();
        let out = SplitTEXTDiagnostics::insert_escaped(
            &mut kws,
            delim,
            &raw_slice,
            TEXTKind::Primary,
            Encoding::Utf8,
            &conf,
        );
        let (_, ws, es) = out.deconstruct();
        let v = kws
            .std
            .iter()
            .map(|(k, v)| (k.as_ne_string(), v.as_ref()))
            .next()
            .unwrap();
        assert_eq!((ne_str!("$P4F").to_owned(), "700/75 BP"), v);
        assert!(es.is_empty(), "errors: {es:?}");
        assert!(ws.is_empty(), "warnings: {ws:?}");
    }

    #[test]
    fn guess_no_escaped() {
        assert_guessed_mode("aaa/bbb/ccc/ddd/", GuessedEscapeMode::Unescaped);
    }

    #[test]
    fn guess_escaped() {
        assert_guessed_mode("aaa/bbb//bbb/ccc/ddd/", GuessedEscapeMode::Escaped);
    }

    #[test]
    fn guess_unescaped() {
        assert_guessed_mode("aaa//bbb/bbb/ccc/ddd/", GuessedEscapeMode::Unescaped);
    }

    #[test]
    fn guess_blank_key_and_key_delim() {
        assert_guessed_mode("aaa//bbb/bbb//ccc/ddd/eee/", GuessedEscapeMode::Ambiguous);
    }

    // This is a rare case where TEXT starts with more than one delimiter. The
    // only choice is to use escaped mode since the leading delimiters cannot be
    // considered as part of the key, and the key itself cannot be blank
    // according to the guessing criteria for unescaped mode.
    #[test]
    fn guess_leading_delim() {
        assert_guessed_mode("/aaa/bbb/bbb/ccc/", GuessedEscapeMode::Escaped);
    }

    // Same as above but with an escape in a key, which precludes escaped mode
    // and thus produces and ambiguous result
    #[test]
    fn guess_leading_delim_key_escaped() {
        assert_guessed_mode("/aaa/bbb/bb//b/ccc/", GuessedEscapeMode::Ambiguous);
    }
}

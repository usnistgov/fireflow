//! Top-level functions for parsing FCS files
use crate::config::{
    AppendFlag, AppendableFlag, CRCConfig, ConfigFlag as _, OverlapCorrectionLimit,
    ReadDataKeywordsConfig, ReadEventsConfig, ReadFlatDatasetConfig,
    ReadFlatDatasetFromKeywordsConfig, ReadFlatTEXTConfig, ReadHeaderAndTEXTConfig,
    ReadHeaderConfig, ReadHeaderInnerConfig, ReadOffsetConfig, ReadSharedConfig,
    ReadStdDatasetConfig, ReadStdKeywordsConfig, ReadStdTEXTConfig, VersionOverride,
    WriteDatasetInnerConfig, WriteMultiConfig, WriteMultiDatasetConfig,
};
use crate::convert::UsizeExt as _;
use crate::core::{
    Analysis, AnyCoreDataset, AnyCoreTEXT, AnyStdDatasetFromFlatTextError, DatasetOffsets,
    LookupAndReadDataAnalysisError, LookupAndReadDataAnalysisWarning, Others, PrivVersionSet as _,
    StdDatasetFromFlatTEXTWarning, StdDatasetFromKwsOutput, StdTEXTDiagnostics,
    StdTEXTFromFlatTEXTError, StdTEXTFromFlatTEXTWarning, StdWriterError, WriteDatasetSummary,
};
use crate::data::{EventOverRangeError, EventsDiagnostics};
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
use crate::segment::{
    OffsetsFromTEXT, SupplementalTextSegmentId,
    read::{
        AnyRegion, AreNamedOffsets, DatasetOverflowError, GuessOtherWidthError, HasOneName as _,
        HasRegion, HeaderOffsetsName, HeaderOffsetsOverflow, IsDataOrAnalysis, IsOffsetPair as _,
        KeyedOptSegment as _, KeyedReqSegment as _, NonEmptyOffsets, OffsetPairsOverlapError,
        OffsetsOverlap, OptOffsetsError, OriginalOffsets, PairResult, PrimaryTextOffsets,
        ReqOffsetsError, SuppOffsetsOverflow, SuppTextOffsetsName, SuppToHeaderOffsetsOverlap,
        SupplementalTextOffsets, TEXTOffsets, TextOffsetsName, TextToHeaderOrSuppOffsetsOverlap,
    },
};
use crate::text::keywords::{
    AlphaNumType, Begindata, Beginstext, Cyt, Enddata, Endstext, LookupNextdataError, Nextdata,
    ReadNextdataError, Tot,
};
use crate::text::lookup::ReqMetarootKey as _;
use crate::validated::dataframe::PrimitiveDataFrame;
use crate::validated::header_offsets::{
    FinalHeaderOffsets, OffsetsValidationError, PrimaryTEXTOverflowError,
    SuppToHeaderOffsetsValidationError, TextToHeaderOrSuppOffsetsValidationError,
};
use crate::validated::keys::{
    InvalidKeywordCharsError, Key as _, KeyOrBytes, KeywordInsertError, NEStringOrBytes, NonStdKey,
    ParsedKeywords, ParsedKeywordsDiagnostic, StdKey, StdKeywords, StdPresent, StringOrBytes,
    TruncatedNEString, ValidKeywords,
};
use crate::validated::read_state::{
    DatasetLen, DatasetOffset, DatasetOffsetError, FileLen, HeaderReadState, TEXTReadState,
};

use fireflow_types::config::{DelimEscapeMode, Encoding};
use fireflow_types::keywords::{Version, Version2_0, Version3_0, Version3_1, Version3_2};
use fireflow_types::nonempty_string::NESliceExt as _;
use hashbrown::HashMap;
use type_families::{ApplyOnce as _, BifunctorOnce, Functor as _, FunctorOnce as _};

use derive_more::{Display, From};
use derive_new::new;
use itertools::Itertools as _;
use nonempty_collections::{IntoIteratorExt as _, NESlice, NEVec, NonEmptyIterator as _};
use thiserror::Error;

use core::fmt;
use std::fs;
use std::fs::File;
use std::io::{self, BufReader, Read, Seek};
use std::iter;
use std::num::NonZeroUsize;
use std::path::PathBuf;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr},
    fireflow_types::python as py,
    pyo3::exceptions::PyValueError,
    pyo3::prelude::*,
};

/// Read HEADER from an FCS file.
pub fn fcs_read_header(
    path: &PathBuf,
    dataset_offset: DatasetOffset,
    conf: &ReadHeaderConfig,
) -> WarningsAndIOGroupResult<Header, GuessOtherWidthError, ReadHeaderError, HeaderSummary> {
    let mut fr = io_to_log!(FCSFileReader::open(path));
    fr.as_read_dataset_state(dataset_offset, conf)
        .map_err(ReadHeaderError::from)
        .map_err(IOAnonErrorGroup::new_pure_one)
        .into_log()
        .and_then_commutative(|mut st| {
            Header::h_read(&mut fr.buf_read, &mut st).map_error(|e| e.fmap(ReadHeaderError::from))
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
    let mut fr = io_to_log!(FCSFileReader::open(path));
    fr.read_flat_text(dataset_offset, conf)
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
    let mut fr = io_to_log!(FCSFileReader::open(path));
    fr.read_std_text(dataset_offset, conf)
}

/// Read dataset from FCS at given position file using flat TEXT.
#[must_use]
pub fn fcs_read_flat_dataset(
    path: &PathBuf,
    dataset_offset: DatasetOffset,
    conf: &ReadFlatDatasetConfig,
) -> WarningsAndIOGroupResult<
    FlatDatasetOutput,
    FlatDatasetWarning,
    FlatDatasetError,
    FlatDatasetSummary,
> {
    let mut fr = io_to_log!(FCSFileReader::open(path));
    fr.read_flat_dataset(dataset_offset, conf)
}

/// Read dataset from FCS file at given position using standardized TEXT.
#[must_use]
pub fn fcs_read_std_dataset(
    path: &PathBuf,
    dataset_offset: DatasetOffset,
    conf: &ReadStdDatasetConfig,
) -> WarningsAndIOGroupResult<
    (AnyCoreDataset, StdDatasetOutput),
    StdDatasetWarning,
    StdDatasetError,
    StdDatasetSummary,
> {
    let mut fr = io_to_log!(FCSFileReader::open(path));
    fr.read_std_dataset(dataset_offset, conf)
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
    LookupAndReadDataAnalysisWarning,
    LookupAndReadDataAnalysisError,
    FlatDatasetWithKwsSummary,
> {
    FCSFileReader::open_with_state(path, dataset_offset, conf)
        .map_err(|e| e.fmap_once(LookupAndReadDataAnalysisError::from))
        .and_then(|(fr, st)| {
            st.maybe_with_dataset_length(dataset_len)
                .map(|txt_st| (txt_st, fr))
                .map_err(LookupAndReadDataAnalysisError::from)
                .map_err(ImpureError::Pure)
        })
        .map_err(IOErrorGroup::from)
        .into_log()
        .and_then_commutative(|(txt_st, mut fr)| {
            let v = hns.header.version;
            FlatDatasetFromKwsOutput::h_read_with_header_and_text(
                &mut fr.buf_read,
                v,
                std,
                &mut hns,
                &txt_st,
            )
        })
        .map_ok_value(|dataset| NewFlatDatasetFromKwsOutput::new(dataset, hns.header.final_offsets))
        .warnings_to_pure_errors(conf.shared, LookupAndReadDataAnalysisError::from)
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
    let mut fr = io_to_log!(FCSFileReader::open(path));
    fr.read_flat_texts(skip, limit, scan, conf)
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
    let mut fr = io_to_log!(FCSFileReader::open(path));
    fr.read_std_texts(skip, limit, scan, conf)
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
    let mut fr = io_to_log!(FCSFileReader::open(path));
    fr.read_flat_datasets(skip, limit, scan, conf)
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
    let mut fr = io_to_log!(FCSFileReader::open(path));
    fr.read_std_datasets(skip, limit, scan, conf)
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

fn next_dataset_boundary<R: Read + Seek>(
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

    /// Scores generated if version was guessed.
    pub version_scores: Option<KeywordVersionScores>,
}

/// Output of parsing one flat dataset (TEXT+DATA) from an FCS file.
#[derive(Clone, new, PartialEq)]
pub struct FlatDatasetOutput {
    /// Output from parsing HEADER+TEXT
    pub text: FlatTEXTOutput,

    /// Output from parsing DATA+ANALYSIS
    pub dataset: FlatDatasetFromKwsOutput,

    /// Scores generated if version was guessed.
    pub version_scores: Option<KeywordVersionScores>,
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
pub struct FlatDatasetFromKwsOutput {
    /// DATA output
    pub data: PrimitiveDataFrame,

    /// ANALYSIS output
    pub analysis: Analysis,

    /// OTHER output(s)
    pub others: Others,

    /// Offsets used to parse DATA and ANALYSIS
    pub dataset_offsets: DatasetOffsets,

    /// Diagnostic output from parsing DATA segment
    pub events_diagnostics: EventsDiagnostics,

    /// Value of the cyclic redundancy check (CRC) as read from the file.
    ///
    /// Will always be `None` for 2.0.
    pub file_crc: Option<CRCOutput>,

    /// Value of the computed cyclic redundancy check (CRC) of the dataset.
    ///
    /// Will always be `None` for 2.0.
    pub computed_crc: Option<u16>,
}

/// The output of parsing the CRC at the end of the last dataset.
#[derive(Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum CRCOutput {
    /// CRC was a valid 16 bit decimal number.
    Valid(u16),
    /// CRC bytes were found but did not parse to a 16-bit number.
    Invalid(Vec<u8>),
}

// TODO should all these std/nonstd keys just be keystrings since the $ is implied?
/// Data pertaining to parsing the TEXT segment.
#[allow(clippy::too_many_arguments)]
#[derive(new, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct FlatTEXTDiagnostics {
    /// HEADER data and supplemental TEXT offsets
    pub header_supp: HeaderAndSuppOffsets,

    /// Amount by which which primary TEXT exceeded EOF.
    pub primary_text_overflow: u64,

    /// Amounts by which non-primary-TEXT HEADER offsets exceeded the dataset length.
    pub header_overflows: Vec<HeaderOffsetsOverflow>,

    /// Keywords that could not be parsed.
    ///
    /// These have either a non-ASCII key or a non-UTF8 value (or both).
    /// Included here for debugging
    pub byte_pairs: Vec<(KeyOrBytes, NEStringOrBytes)>,

    /// Standard keys which appear more than once with their values.
    pub non_unique_std_keywords: Vec<(StdKey, TruncatedNEString)>,

    /// Nonstandard keys which appear more than once with their values.
    pub non_unique_nonstd_keywords: Vec<(NonStdKey, TruncatedNEString)>,

    /// Ignored standard keys with their values
    pub ignored_standard_keywords: Vec<(StdKey, NEStringOrBytes)>,

    /// Keys with empty values as a result of trimming whitespace.
    pub keys_with_empty_trimmed_values: Vec<KeyOrBytes>,

    /// Keys with values that are not empty after whitespace was trimmed off.
    ///
    /// Values included here are the original values before trimming.
    pub keys_with_trimmed_values: Vec<(KeyOrBytes, TruncatedNEString)>,

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

    /// Keys that have blank values.
    ///
    /// Only relevant in escaped delimiter mode.
    pub keys_with_blank_values: Vec<NEStringOrBytes>,

    /// Values with blank keys.
    pub values_with_blank_keys: Vec<NEStringOrBytes>,

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

    /// Value of the cyclic redundancy check (CRC) as read from the file.
    ///
    /// Will always be `None` for 2.0.
    pub file_crc: Option<CRCOutput>,

    /// Value of the computed cyclic redundancy check (CRC) of the dataset.
    ///
    /// Will always be `None` for 2.0.
    pub computed_crc: Option<u16>,
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
    Std(StdTEXTFromFlatTEXTWarning),
}

/// Error when parsing TEXT in standard mode
#[derive(From, Display, Error, Debug, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum StdTEXTError {
    Flat(HeaderOrFlatTextError),
    Std(StdTEXTFromFlatTEXTError),
    Warn(StdTEXTWarning),
}

/// Warning when parsing TEXT+DATA in standard mode
#[derive(From, Display, Error, Debug, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum StdDatasetWarning {
    Flat(HeaderOrFlatTEXTWarning),
    Std(StdDatasetFromFlatTEXTWarning),
}

/// Error when parsing TEXT+DATA in standard mode
#[derive(From, Display, Error, Debug, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum StdDatasetError {
    Flat(HeaderOrFlatTextError),
    Std(AnyStdDatasetFromFlatTextError),
    Warn(StdDatasetWarning),
}

/// Warning when parsing TEXT+DATA in flat mode
#[derive(From, Display, Error, Debug, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum FlatDatasetWarning {
    Flat(HeaderOrFlatTEXTWarning),
    Read(LookupAndReadDataAnalysisWarning),
}

/// Warning when parsing TEXT+DATA in flat mode
#[derive(From, Display, Error, Debug, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum FlatDatasetError {
    Flat(HeaderOrFlatTextError),
    Read(LookupAndReadDataAnalysisError),
    Warn(FlatDatasetWarning),
    Version(GuessVersionError),
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
    Data(FlatDatasetWarning),
}

/// Error when parsing multiple [`FlatDatasetOutput`]s
#[derive(From, Display, Error, Debug, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum MultiFlatDatasetError {
    Text(HeaderOrFlatTextError), // for reading skipped datasets to get $NEXTDATA
    Data(FlatDatasetError),
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
    InvalidChars(InvalidKeywordCharsError),
    AppendSupp(StdPresent),
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
    InvalidKeyword(InvalidKeywordCharsError),
    InvalidChars(StdPresent),
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
    Uneven(UnevenTokensError),
    EvenFinal(EvenDelimiterError),
    Insert(KeywordInsertError),
    Bound(DelimBoundError),
    Leading(LeadingDelimError),
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
        conf: C,
    ) -> IOResult<(Self, HeaderReadState<C>), DatasetOffsetError> {
        let fr = Self::open(p)?;
        let st = fr
            .as_read_dataset_state(dataset_offset, conf)
            .map_err(ImpureError::Pure)?;
        Ok((fr, st))
    }

    pub(crate) fn as_read_dataset_state<C>(
        &self,
        dataset_offset: DatasetOffset,
        conf: C,
    ) -> Result<HeaderReadState<C>, DatasetOffsetError> {
        HeaderReadState::init(self.file_len, dataset_offset, conf)
    }

    fn read_flat_text(
        &mut self,
        dataset_offset: DatasetOffset,
        conf: &ReadFlatTEXTConfig,
    ) -> WarningsAndIOGroupResult<
        FlatTEXTOutput,
        HeaderOrFlatTEXTWarning,
        HeaderOrFlatTextError,
        FlatTEXTSummary,
    > {
        self.read_flat_text_inner(dataset_offset, conf)
            .map_ok_value(|(x, _)| x)
            .warnings_to_pure_errors(conf.shared, HeaderOrFlatTextError::from)
            .deanonymize()
    }

    fn read_std_text(
        &mut self,
        dataset_offset: DatasetOffset,
        conf: &ReadStdTEXTConfig,
    ) -> WarningsAndIOGroupResult<
        (AnyCoreTEXT, StdTEXTOutput),
        StdTEXTWarning,
        StdTEXTError,
        StdTEXTSummary,
    > {
        self.read_flat_text_inner(dataset_offset, conf)
            .map_pure_errors(StdTEXTError::from)
            .map_commutative_warnings(StdTEXTWarning::from)
            .and_then_commutative(|(flat, txt_st)| {
                flat.into_std_text(&txt_st)
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
        conf: &ReadFlatDatasetConfig,
    ) -> WarningsAndIOGroupResult<
        FlatDatasetOutput,
        FlatDatasetWarning,
        FlatDatasetError,
        FlatDatasetSummary,
    > {
        self.read_flat_text_inner(dataset_offset, conf)
            .map_commutative_warnings(FlatDatasetWarning::from)
            .map_pure_errors(FlatDatasetError::from)
            .and_then_commutative(|(flat, txt_st)| {
                let version = flat.flat_diagnostics.header_supp.header.version;
                let oride = conf.flat.version_override.as_ref();
                autodetect_version(version, &flat.keywords.std, oride)
                    .map_err(FlatDatasetError::from)
                    .map_err(IOErrorGroup::new_pure_one)
                    .map(|(new_version, scores)| (new_version, flat, txt_st, scores))
                    .into_log()
            })
            .and_then_commutative(|(new_ver, mut flat, st, scores)| {
                let hns = &mut flat.flat_diagnostics.header_supp;
                let std = &flat.keywords.std;
                FlatDatasetFromKwsOutput::h_read_with_header_and_text(
                    &mut self.buf_read,
                    new_ver,
                    std,
                    hns,
                    &st,
                )
                .map_ok_value(|dataset| FlatDatasetOutput::new(flat, dataset, scores))
                .map_commutative_warnings(FlatDatasetWarning::from)
                .map_pure_errors(FlatDatasetError::from)
            })
            .warnings_to_pure_errors(conf.shared, FlatDatasetError::from)
            .deanonymize()
    }

    fn read_std_dataset(
        &mut self,
        dataset_offset: DatasetOffset,
        conf: &ReadStdDatasetConfig,
    ) -> WarningsAndIOGroupResult<
        (AnyCoreDataset, StdDatasetOutput),
        StdDatasetWarning,
        StdDatasetError,
        StdDatasetSummary,
    > {
        self.read_flat_text_inner(dataset_offset, conf)
            .map_pure_errors(StdDatasetError::from)
            .map_commutative_warnings(StdDatasetWarning::from)
            .and_then_commutative(|(flat, txt_st)| {
                flat.into_std_dataset(&mut self.buf_read, &txt_st)
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
        scan: bool,
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
            let res = self.read_flat_text(dso, conf);
            let succ = split_log!(res);
            let scanned_dataset_offset = if scan {
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
        }
        results
            .into_iter()
            .sequence_success()
            .fmap_once(|xs| xs.into_iter().skip(skip.unwrap_or_default()).collect())
            .into_log()
    }

    fn read_std_datasets(
        &mut self,
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
        self.read_nextdata_loop(
            skip,
            limit,
            scan,
            conf,
            StdDatasetSummary,
            Self::read_std_dataset,
            |ret| ret.1.flat_diagnostics.header_supp.nextdata,
        )
    }

    fn read_std_texts(
        &mut self,
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
        self.read_nextdata_loop(
            skip,
            limit,
            scan,
            conf,
            StdTEXTSummary,
            Self::read_std_text,
            |ret| ret.1.flat_diagnostics.header_supp.nextdata,
        )
    }

    fn read_flat_datasets(
        &mut self,
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
        self.read_nextdata_loop(
            skip,
            limit,
            scan,
            conf,
            FlatDatasetSummary,
            Self::read_flat_dataset,
            |ret| ret.text.flat_diagnostics.header_supp.nextdata,
        )
    }

    fn read_flat_text_inner<C>(
        &mut self,
        dataset_offset: DatasetOffset,
        conf: C,
    ) -> WarningsAndIOGroupResult<
        (FlatTEXTOutput, TEXTReadState<C>),
        HeaderOrFlatTEXTWarning,
        HeaderOrFlatTextError,
        (),
    >
    where
        C: AsRef<ReadHeaderAndTEXTConfig> + AsRef<ReadHeaderInnerConfig> + AsRef<ReadOffsetConfig>,
    {
        self.as_read_dataset_state(dataset_offset, conf)
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
        scan: bool,
        conf: &C,
        g: G,
        mut f0: Fsucc,
        mut fnext: Fnext,
    ) -> WarningsAndIOGroupResult<Vec<X>, W, E, G>
    where
        Fsucc: FnMut(&mut Self, DatasetOffset, &C) -> WarningsAndIOGroupResult<X, Wi, Ei, G>,
        Fnext: FnMut(&X) -> Option<Nextdata>,
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
        macro_rules! get_scanned {
            () => {
                if scan {
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
                    .read_flat_text(dso, &rconf)
                    .map_commutative_warnings(W::from)
                    .map_pure_errors(E::from)
                    .map_error(|e| e.set_group(g));
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
                let res = f0(self, dso, conf)
                    .map_commutative_warnings(W::from)
                    .map_pure_errors(E::from);
                let succ = split_log!(res);
                let scanned_dataset_offset = get_scanned!();
                succ.fmap_once(|ret| {
                    dataset_offset = scanned_dataset_offset.or_else(|| {
                        let nd = u64::from(fnext(&ret)?);
                        (nd > 0).then_some(DatasetOffset(dso.0 + nd))
                    });
                    Some(ret)
                })
            };
            results.push(nextdata_res);
            count += 1;
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

    pub(crate) fn max_end_offset(&self) -> Option<u64> {
        let hdr_max = self.header.final_offsets.max_end_offset();
        let supp_max = self
            .supp_text
            .final_offsets()
            .and_then(|o| o.as_nonempty())
            .map(|o| o.end());
        hdr_max.max(supp_max)
    }
}

impl FlatDatasetOutput {
    fn summarize(self) -> DatasetSummary {
        let fd = self.text.flat_diagnostics;
        let hdr = fd.header_supp.header;
        let ds = self.dataset;
        let txt = AsRef::<PrimaryTextOffsets>::as_ref(&hdr.final_offsets);
        DatasetSummary {
            version: hdr.version,
            text_len: txt.nbytes(),
            data_len: ds.dataset_offsets.final_data.nbytes(),
            analysis_len: ds.dataset_offsets.final_analysis.nbytes(),
            n_events: ds.data.nrows(),
            n_measurements: ds.data.ncols(),
            n_other: ds.others.0.len(),
            others_len: ds.others.0.iter().map(|x| x.0.len()).sum(),
            datatype: AlphaNumType::get_metaroot_req(&self.text.keywords.std).ok(),
            file_crc: ds.file_crc,
            computed_crc: ds.computed_crc,
        }
    }
}

impl FlatDatasetFromKwsOutput {
    /// Read from handle with offsets/version from HEADER and parsed TEXT keywords.
    fn h_read_with_header_and_text<C, R>(
        h: &mut BufReader<R>,
        new_version: Version,
        kws: &StdKeywords,
        hns: &mut HeaderAndSuppOffsets,
        st: &TEXTReadState<C>,
    ) -> WarningsAndIOGroupResult<
        Self,
        LookupAndReadDataAnalysisWarning,
        LookupAndReadDataAnalysisError,
        (),
    >
    where
        R: Read + Seek,
        C: AsRef<ReadDataKeywordsConfig>
            + AsRef<ReadOffsetConfig>
            + AsRef<ReadEventsConfig>
            + AsRef<CRCConfig>,
    {
        kws_to_df_analysis(new_version, h, kws, hns, st)
            .map_pure_errors(LookupAndReadDataAnalysisError::from)
            .and_then_commutative(|(data, analysis, dataset_offsets, event_out)| {
                let hns_max = hns.max_end_offset();
                let da_max = dataset_offsets.max_end_offset();
                let crc_res = if let Some(crc_start) = hns_max.max(da_max) {
                    st.test_crc(h, crc_start, new_version, *st.conf().as_ref())
                } else {
                    LogResult::new_ok((None, None))
                };
                let or = hns.header.final_offsets.others_reader();
                crc_res
                    .map_commutative_warnings(LookupAndReadDataAnalysisWarning::from)
                    .map_pure_errors(LookupAndReadDataAnalysisError::from)
                    .repack_warnings()
                    .and_then_commutative(|(file_crc, computed_crc)| {
                        let go = |others| {
                            Self::new(
                                data,
                                analysis,
                                others,
                                dataset_offsets,
                                event_out,
                                file_crc,
                                computed_crc,
                            )
                        };
                        or.h_read(h).map(go).map_err(IOErrorGroup::from).into_log()
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
        (Self, TEXTReadState<C>),
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
            .and_then_commutative(|header| {
                Self::h_read_from_header(h, header, st)
                    .map_commutative_warnings(HeaderOrFlatTEXTWarning::from)
                    .map_pure_errors(HeaderOrFlatTextError::from)
            })
    }

    /// Read flat TEXT from file handle with offsets from HEADER.
    fn h_read_from_header<C, R>(
        h: &mut BufReader<R>,
        mut header: Header,
        st: HeaderReadState<C>,
    ) -> WarningsAndIOGroupResult<
        (Self, TEXTReadState<C>),
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
        let enc = conf.use_encoding.choose(ptext_bytes.as_ref());

        let ptext_ne_slice = ptext_bytes.as_nonempty_slice();
        let delim_res = split_first_delim(&ptext_ne_slice, conf)
            .map_errors(ParseFlatTEXTError::from)
            .map_commutative_warnings(ParseFlatTEXTWarning::from)
            .into_semigroup();

        delim_res
            .group()
            .map_error(IOErrorGroup::Pure)
            // Parse primary TEXT and get $NEXTDATA if it exists
            .and_then_commutative(|(delim, bytes)| {
                SplitTEXTDiagnostics::primary_from_bytes(delim, bytes, enc, st.conf().as_ref())
                    .map_commutative_warnings(ParseFlatTEXTWarning::from)
                    .map_errors(ParseFlatTEXTError::from)
                    .and_then_commutative(|(kws, prim_diag)| {
                        Nextdata::lookup_ro(&kws.std, ptext_offsets, st)
                            .map_commutative_warnings(ParseFlatTEXTWarning::from)
                            .map_errors(ParseFlatTEXTError::from)
                            .into_semigroup()
                            .map_ok_value(|(nextdata, txt_st)| {
                                (kws, delim, prim_diag, nextdata, txt_st)
                            })
                    })
                    .group()
                    .map_error(IOErrorGroup::Pure)
            })
            // Parse supplemental TEXT if applicable
            .and_then_commutative(|(mut kws, delim, prim_diag, nextdata, txt_st)| {
                SuppTEXTOffsetsOutput::lookup(&kws.std, &mut header, &txt_st)
                    .map_commutative_warnings(ParseFlatTEXTWarning::from)
                    .map_errors(ParseFlatTEXTError::from)
                    .group()
                    .map_error(IOErrorGroup::Pure)
                    .and_then_commutative(|supp_out| {
                        if let Some(ne) = supp_out.as_offset_pair().and_then(|p| p.as_nonempty()) {
                            let s = txt_st.conf().as_ref();
                            SplitTEXTDiagnostics::h_read_supp(h, &ne, &mut kws, delim, enc, s)
                                .map_commutative_warnings(ParseFlatTEXTWarning::from)
                                .map_pure_errors(ParseFlatTEXTError::from)
                                .map_ok_value(|supp_diag| (supp_out, supp_diag))
                        } else {
                            LogResult::new_ok((supp_out, None))
                        }
                    })
                    .map_ok_value(|(supp_out, supp_diag)| {
                        (kws, nextdata, supp_out, prim_diag, supp_diag, txt_st)
                    })
            })
            .and_then_commutative(
                |(mut kws, nextdata, supp_text_offsets, prim_out, supp_out, txt_st)| {
                    // Check if any HEADER offsets exceed $NEXTDATA
                    let hdr_trunc_res = header
                        .final_offsets
                        .try_truncate_non_primary_text(&txt_st)
                        .nowarn_into_warn()
                        .map_errors(ParseFlatTEXTError::from);

                    let hconf: &ReadHeaderAndTEXTConfig = txt_st.conf().as_ref();

                    // Combine primary and supp TEXT keywords; check for uniqueness
                    let append_res = kws
                        .append_std(&hconf.append_standard_keywords, hconf.allow_nonunique)
                        .switchable_into_commutative()
                        .map_commutative_warnings(ParseFlatTEXTWarning::from)
                        .map_errors(ParseFlatTEXTError::from);

                    let vkws = ValidKeywords::new(kws.std, kws.nonstd);

                    hdr_trunc_res
                        .zip_commutative(append_res)
                        .and_then_commutative(|(nd_overlaps, ())| {
                            // Build diagnostics output, throw errors for any badly
                            // formatted tokens collected during parsing
                            let header_supp =
                                HeaderAndSuppOffsets::new(header, supp_text_offsets, nextdata);
                            // TODO technically this does not depend on the previous
                            // results so this can be folded out and run in parallel
                            // with nextdata and append checks
                            kws.diag
                                .into_flat_diag(
                                    header_supp,
                                    ptext_overflow,
                                    nd_overlaps,
                                    prim_out,
                                    supp_out,
                                    txt_st.conf().as_ref(),
                                )
                                .map_commutative_warnings(ParseFlatTEXTWarning::from)
                                .map_errors(ParseFlatTEXTError::from)
                                .set_err_value(())
                        })
                        .map_ok_value(|diag| (Self::new(vkws, diag), txt_st))
                        .group()
                        .map_error(IOErrorGroup::Pure)
                },
            )
    }

    /// Convert flat TEXT into standardized TEXT.
    fn into_std_text<C>(
        mut self,
        st: &TEXTReadState<C>,
    ) -> WarningsAndErrorsResult<
        (AnyCoreTEXT, StdTEXTOutput),
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
        let hns = &mut self.flat_diagnostics.header_supp;
        let version = hns.header.version;
        AnyCoreTEXT::parse_flat(version, self.keywords, hns, st).map_ok_value(
            |(standardized, extra, offsets, scores)| {
                let out = StdTEXTOutput::new(
                    offsets.tot,
                    offsets.offsets,
                    extra,
                    self.flat_diagnostics,
                    scores,
                );
                (standardized, out)
            },
        )
    }

    /// Convert into standardized dataset, reading data as necessary.
    fn into_std_dataset<C, R>(
        mut self,
        h: &mut BufReader<R>,
        st: &TEXTReadState<C>,
    ) -> WarningsAndIOGroupResult<
        (AnyCoreDataset, StdDatasetOutput),
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
            + AsRef<ReadEventsConfig>
            + AsRef<CRCConfig>,
    {
        let hdr = &mut self.flat_diagnostics.header_supp;
        AnyCoreDataset::new_from_keywords(h, hdr, self.keywords, st).map_ok_value(
            |(core, out, scores)| {
                let dx = StdDatasetOutput::new(out, self.flat_diagnostics, scores);
                (core, dx)
            },
        )
    }
}

impl SplitTEXTDiagnostics {
    /// Read supp TEXT from file handle and store keywords in hash table.
    fn h_read_supp<R: Read + Seek>(
        h: &mut BufReader<R>,
        offsets: &NonEmptyOffsets<SupplementalTextSegmentId, OffsetsFromTEXT>,
        kws: &mut ParsedKeywords,
        delim: u8,
        enc: Encoding,
        conf: &ReadHeaderAndTEXTConfig,
    ) -> WarningsAndIOGroupResult<
        Option<Self>,
        ParseSupplementalTEXTError,
        ParseSupplementalTEXTError,
        (),
    > {
        let bytes = io_to_log!(offsets.h_read_contents(h));
        let ne = bytes.as_nonempty_slice();
        Self::supp_from_bytes(kws, delim, &ne, enc, conf)
            .group()
            .map_error(IOErrorGroup::Pure)
    }

    /// Read primary TEXT from bytes and store keywords in hash table.
    fn primary_from_bytes(
        delim: u8,
        bytes: &[u8],
        enc: Encoding,
        conf: &ReadHeaderAndTEXTConfig,
    ) -> WarningsAndErrorsResult<(ParsedKeywords, Self), (), ParseKeywordsIssue, ParseKeywordsIssue>
    {
        let raw_tokens = Self::split_bytes(delim, bytes);
        let raw_slice = raw_tokens.as_nonempty_slice();
        // We are about to insert a massive amount of data into two hash tables,
        // so make a guess as to how big they need to be to avoid reallocation.
        //
        // Assume that the number of inserts to each hash table will be roughly
        // half the the number of tokens since they come in pairs. In
        // practice, this will almost always be true, and may be a bit less if
        // some escapes are present.
        //
        // Also assume that the number of non-standard and standard keywords is
        // roughly equal. This probably varies quite a bit but it is hard to
        // know without scanning each token first which is also costly.
        //
        // Finally, assume the STEXT is almost never present and therefore not
        // worth considering. This makes the estimation much simpler since we
        // can't read STEXT without TEXT first.
        let cap = raw_tokens.len().get() / 2;
        let mut kws = ParsedKeywords {
            std: HashMap::with_capacity(cap / 2),
            nonstd: HashMap::with_capacity(cap / 2),
            diag: ParsedKeywordsDiagnostic::default(),
        };
        Self::from_bytes_inner(&mut kws, delim, &raw_slice, TEXTKind::Primary, enc, conf)
            .map_ok_value(|ret| (kws, ret))
    }

    /// Read supp TEXT from bytes and store keywords in hash table.
    fn supp_from_bytes(
        kws: &mut ParsedKeywords,
        delim: u8,
        bytes: &NESlice<'_, u8>,
        enc: Encoding,
        conf: &ReadHeaderAndTEXTConfig,
    ) -> WarningsAndErrorsResult<
        Option<Self>,
        (),
        ParseSupplementalTEXTError,
        ParseSupplementalTEXTError,
    > {
        let (b0, bs) = bytes.split_first();
        let flag = conf.allow_supp_text_own_delim;
        let raw_tokens = Self::split_bytes(*b0, bs);
        let raw_slice = raw_tokens.as_nonempty_slice();
        Self::from_bytes_inner(kws, *b0, &raw_slice, TEXTKind::Supplemental, enc, conf)
            .map_warnings_and_errors(ParseSupplementalTEXTError::from)
            .eval_warning_or_error3(
                flag,
                |_| (),
                |()| (),
                |_| (*b0 != delim).then_some(DelimMismatch::new(delim, *b0)),
            )
            .map_ok_value(Some)
    }

    fn split_bytes(delim: u8, xs: &[u8]) -> NEVec<&[u8]> {
        xs.split(|&x| x == delim)
            .try_into_nonempty_iter()
            .expect("split should always give at least one element")
            .collect()
    }

    /// Maybe trim end off slice of tokens so that the length is even.
    ///
    /// Return final slice, the last odd non-empty slice if it was taken off,
    /// and a boolean that will be `true` if the number of tokens started as
    /// even. The 'perfect' case (ie standards compliant FCS file) is `None` and
    /// `true` for the odd slice and boolean. All combinations are possible.
    fn trim_tokens_end<'a, 'b>(
        raw_tokens: &'b NESlice<'_, &'a [u8]>,
    ) -> (&'b [&'a [u8]], Option<NESlice<'a, u8>>, bool) {
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

    /// Read TEXT segment (primary or supp) from bytes.
    fn from_bytes_inner(
        kws: &mut ParsedKeywords,
        delim: u8,
        raw_tokens: &NESlice<'_, &'_ [u8]>,
        tk: TEXTKind,
        enc: Encoding,
        conf: &ReadHeaderAndTEXTConfig,
    ) -> WarningsAndErrorsResult<Self, (), ParseKeywordsIssue, ParseKeywordsIssue> {
        let escaped = GuessedEscapeMode::is_escaped(raw_tokens, conf.delim_escape_mode);
        if escaped {
            Self::insert_escaped(kws, delim, raw_tokens, tk, enc, conf)
        } else {
            Self::insert_unescaped(kws, delim, raw_tokens, tk, enc, conf)
        }
    }

    /// Split bytes without delimiter escaping and store keys in hash table.
    fn insert_unescaped(
        kws: &mut ParsedKeywords,
        delim: u8,
        segs: &NESlice<'_, &[u8]>,
        tk: TEXTKind,
        enc: Encoding,
        conf: &ReadHeaderAndTEXTConfig,
    ) -> WarningsAndErrorsResult<Self, (), ParseKeywordsIssue, ParseKeywordsIssue> {
        let mut out = Self {
            escaped: false,
            delimiter: delim,
            keys_with_blank_values: vec![],
            values_with_blank_keys: vec![],
            tokens_with_boundary_delims: vec![],
            skipped_pairs: 0,
            last_odd_token: StringOrBytes::default(),
            has_even_delims: false,
            extra_leading_delims: 0,
        };
        let mut insert_errs = vec![];
        let mut any_insert_err = false;

        let (pairs, extra_token, has_even_tokens) = Self::trim_tokens_end(segs);

        out.has_even_delims = !has_even_tokens;

        let matchers = conf.as_matchers();

        for (key, value) in pairs.iter().tuples() {
            let k = NESlice::try_from_slice(key);
            let v = NESlice::try_from_slice(value);
            match (k, v) {
                (Some(kk), Some(vv)) => {
                    if let Some((e, is_err)) = kws.insert(&kk, &vv, &matchers, enc, conf) {
                        any_insert_err = any_insert_err || is_err;
                        insert_errs.push(ParseKeywordsIssue::from(e));
                    }
                }
                (Some(kk), None) => out.keys_with_blank_values.push(kk.to_ne_vec().into()),
                (None, Some(vv)) => out.values_with_blank_keys.push(vv.to_ne_vec().into()),
                (None, None) => out.skipped_pairs += 1,
            }
        }

        let blank_key_errors = out
            .values_with_blank_keys
            .iter()
            .cloned()
            .map(|k| BlankKeyError::new(tk, k))
            .map(ParseKeywordsIssue::from);

        let blank_pair_error = NonZeroUsize::new(out.skipped_pairs)
            .map(|n| BlankPairError::new(tk, n))
            .map(ParseKeywordsIssue::from);

        out.last_odd_token = extra_token
            .as_ref()
            .map(|s| s.as_ref().to_vec().into())
            .unwrap_or_default();

        let last_odd_err = extra_token
            .map(|t| UnevenTokensError::new(tk, t.to_ne_vec().into()))
            .map(ParseKeywordsIssue::from);

        let even_delim_err = (!has_even_tokens).then_some(EvenDelimiterError(tk).into());

        let res = if any_insert_err {
            LogResult::new_from_err_iter(insert_errs, (), ())
        } else {
            LogResult::new_ok(()).set_commutative_warnings(insert_errs)
        };

        // NOTE blank pair error shares the same flag, which technically is a
        // bit confusing but this error is so rare it probably won't matter from
        // ux perspective
        res.extend_deferred_warnings_or_errors3(
            blank_key_errors.chain(blank_pair_error),
            conf.allow_empty_keys,
        )
        .extend_deferred_warnings_or_errors3(even_delim_err, conf.allow_even_delims)
        .extend_deferred_warnings_or_errors3(last_odd_err, conf.allow_odd_tokens)
        .set_ok_value(out)
    }

    /// Split bytes with delimiter escaping and store keys in hash table.
    #[allow(clippy::too_many_lines)]
    fn insert_escaped(
        kws: &mut ParsedKeywords,
        delim: u8,
        segs: &NESlice<'_, &[u8]>,
        tk: TEXTKind,
        enc: Encoding,
        conf: &ReadHeaderAndTEXTConfig,
    ) -> WarningsAndErrorsResult<Self, (), ParseKeywordsIssue, ParseKeywordsIssue> {
        let mut out = Self {
            escaped: true,
            delimiter: delim,
            keys_with_blank_values: vec![],
            values_with_blank_keys: vec![],
            skipped_pairs: 0,
            tokens_with_boundary_delims: vec![],
            last_odd_token: StringOrBytes::default(),
            has_even_delims: false,
            extra_leading_delims: 0,
        };
        let mut insert_results = vec![];
        let mut any_insert_err = false;

        let matchers = conf.as_matchers();

        let mut push_pair = |ks: &mut ParsedKeywords, kb: &NESlice<u8>, vb: &NESlice<u8>| {
            let _ = ks.insert(kb, vb, &matchers, enc, conf).map(|(e, is_err)| {
                any_insert_err = any_insert_err || is_err;
                insert_results.push(ParseKeywordsIssue::from(e));
            });
        };

        // The number of blanks which are found in a row
        let mut consec_blanks = 0_usize;

        // Dynamic buffers to hold tokens with escaped delimiters. This is
        // necessary because we cannot just copy escaped text as-is; we need to
        // remove every other delimiter to make it literal, which implies we
        // need to allocate a new string.
        let mut keybuf: NEVec<u8>;
        let mut valbuf: Vec<u8> = vec![];

        let mut it = segs.iter();

        // Prime the loop with the first token which belongs to a key. This
        // will fail if TEXT is entirely delimiters, in which case there is
        // nothing more to do.
        keybuf = if let Some(token0) = it.by_ref().find_map(|token| {
            let ne = NESlice::try_from_slice(token);
            if ne.is_none() {
                out.extra_leading_delims += 1;
            }
            ne
        }) {
            token0.to_ne_vec()
        } else {
            // No tokens found, which means TEXT is entirely delimiters
            // (which includes TEXT being just one delim and otherwise empty).
            out.extra_leading_delims = segs.len().get();
            return LogResult::new_ok(out);
        };

        // Determine if the number of delimiters is even or odd, throw an error
        // for the former. Remove leading delimiters since we 'pretend' that
        // TEXT is missing one delimiter if this number is odd (which means the
        // actual number of leading delims is even since we already counted
        // the first before running this function).
        out.has_even_delims = (segs.len().get() - out.extra_leading_delims) & 1 == 0;
        let even_delim_err = out.has_even_delims.then_some(EvenDelimiterError(tk).into());

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
                        out.tokens_with_boundary_delims.push(seg);
                    }
                    if let Some(ne_val) = NESlice::try_from_slice(&valbuf[..]) {
                        push_pair(kws, &keybuf.as_nonempty_slice(), &ne_val);
                        valbuf.clear();
                        keybuf = ne_token.to_ne_vec();
                    } else {
                        valbuf.extend_from_slice(ne_token.as_ref());
                    }
                } else {
                    // Previous consecutive delimiter sequence was even. Push
                    // this number / 2 followed by the current token fragment
                    // to the active buffer.
                    let ds = iter::repeat_n(delim, consec_blanks.div_ceil(2));
                    if valbuf.is_empty() {
                        keybuf.extend(ds.chain(ne_token.iter().copied()));
                    } else {
                        valbuf.extend(ds.chain(ne_token.iter().copied()));
                    }
                }
                consec_blanks = 0;
            } else {
                consec_blanks += 1;
            }
        }

        // Unprime the loop since we can only add a key/val pair after
        // encountering the delimiter boundary after the value token. If there
        // was an even number of tokens, we will have both a key and value that
        // can be pushed. If we only have a key, keep this as the last odd token
        // and throw error.
        let last_odd_err = if let Some(ne_val) = NESlice::try_from_slice(&valbuf[..]) {
            // both key and value are present, this is the last pair in
            // TEXT so push to the end of keywords
            push_pair(kws, &keybuf.as_nonempty_slice(), &ne_val);
            None
        } else {
            // Only key is present which means we have an odd number of
            // tokens. Scream at user so they will be enlightened.
            let last = NEStringOrBytes::from(keybuf.as_nonempty_slice());
            let e = UnevenTokensError::new(tk, last.clone()).into();
            out.last_odd_token = last.into();
            Some(e)
        };

        // If the number of consecutive blanks was odd and greater than zero,
        // the last token ended with a string of escaped delimiters which was
        // not captured at the end of the loop.
        if consec_blanks > 1 && consec_blanks & 1 == 1 {
            let seg = NESlice::try_from_slice(&valbuf[..]).unwrap_or(keybuf.as_nonempty_slice());
            out.tokens_with_boundary_delims
                .push(NEStringOrBytes::from(seg));
        }

        let leading_delim_err = NonZeroUsize::try_from(out.extra_leading_delims)
            .ok()
            .map(|n| LeadingDelimError::new(tk, n).into());

        let bound_iter = out
            .tokens_with_boundary_delims
            .iter()
            .map(|token| DelimBoundError::new(tk, token.clone()).into())
            .chain(leading_delim_err);

        let res = if any_insert_err {
            LogResult::new_from_err_iter(insert_results, (), ())
        } else {
            LogResult::new_ok(()).set_commutative_warnings(insert_results)
        };

        res.extend_deferred_warnings_or_errors3(bound_iter, conf.allow_delim_at_boundary)
            .extend_deferred_warnings_or_errors3(even_delim_err, conf.allow_even_delims)
            .extend_deferred_warnings_or_errors3(last_odd_err, conf.allow_odd_tokens)
            .set_ok_value(out)
    }
}

impl GuessedEscapeMode {
    fn is_escaped(segs: &NESlice<'_, &[u8]>, mode: DelimEscapeMode) -> bool {
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

    fn has_any_empty(raw_tokens: &NESlice<'_, &[u8]>) -> bool {
        // Only consider the first even number of tokens since both modes should
        // deal with extra crap at the end in the same way
        let (segs, _, _) = SplitTEXTDiagnostics::trim_tokens_end(raw_tokens);
        segs.iter().any(|s| s.is_empty())
    }

    fn test_both_modes(raw_tokens: &NESlice<'_, &[u8]>) -> Self {
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
        kws: &StdKeywords,
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
            Some(VersionOverride::AutoDetect(_)) => {
                if kws.contains_key(&Begindata::std()) || kws.contains_key(&Enddata::std()) {
                    if kws.contains_key(&Cyt::std()) {
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
                let pair = SupplementalTextSegmentId::get_req_pair(kws);
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
                let pair = SupplementalTextSegmentId::get_opt_pair(kws);
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

fn kws_to_df_analysis<C, R>(
    new_version: Version,
    h: &mut BufReader<R>,
    kws: &StdKeywords,
    hns: &mut HeaderAndSuppOffsets,
    st: &TEXTReadState<C>,
) -> WarningsAndIOGroupResult<
    (
        PrimitiveDataFrame,
        Analysis,
        DatasetOffsets,
        EventsDiagnostics,
    ),
    LookupAndReadDataAnalysisWarning,
    LookupAndReadDataAnalysisError,
    (),
>
where
    R: Read + Seek,
    C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadOffsetConfig> + AsRef<ReadEventsConfig>,
{
    match new_version {
        Version::FCS2_0 => Version2_0::h_lookup_and_read(h, kws, hns, st),
        Version::FCS3_0 => Version3_0::h_lookup_and_read(h, kws, hns, st),
        Version::FCS3_1 => Version3_1::h_lookup_and_read(h, kws, hns, st),
        Version::FCS3_2 => Version3_2::h_lookup_and_read(h, kws, hns, st),
    }
}

fn split_first_delim<'a>(
    bytes: &'a NESlice<'_, u8>,
    conf: &ReadHeaderAndTEXTConfig,
) -> WarningAndErrorResult<(u8, &'a [u8]), (), DelimCharError, DelimCharError> {
    let (delim, rest) = bytes.split_first();
    let is_ok = (1..=126).contains(delim);
    let e = DelimCharError(*delim);
    let flag = conf.allow_non_ascii_delim;
    SwitchableErrorResult::new_switchable_ok_if3(is_ok, (*delim, rest), (), e, flag)
        .switchable_into_commutative()
}

#[cfg(test)]
mod tests {
    use super::*;
    use fireflow_types::{ne_str, nonempty_string::DisplayableNE as _};

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

#[cfg(feature = "python")]
mod python {
    use super::CRCOutput;

    use fireflow_types::python::ConfigError;
    use pyo3::{IntoPyObjectExt as _, prelude::*};

    impl<'py> FromPyObject<'_, 'py> for CRCOutput {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            if let Ok(b) = obj.extract::<Vec<u8>>() {
                return Ok(Self::Invalid(b));
            } else if let Ok(b) = obj.extract::<u16>() {
                return Ok(Self::Valid(b));
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
                Self::Valid(v) => v.into_bound_py_any(py),
                Self::Invalid(v) => v.into_bound_py_any(py),
            }
        }
    }
}

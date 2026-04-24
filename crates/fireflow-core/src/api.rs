//! Top-level functions for parsing FCS files
use crate::config::{
    AppendFlag, AppendableFlag, ConfigFlag as _, DatasetOffset, DatasetOffsetError,
    OverlapCorrectionLimit, ReadDataKeywordsConfig, ReadEventsConfig, ReadFlatDatasetConfig,
    ReadFlatDatasetFromKeywordsConfig, ReadFlatTEXTConfig, ReadHeaderAndTEXTConfig,
    ReadHeaderConfig, ReadHeaderInnerConfig, ReadOffsetConfig, ReadSharedConfig, ReadState,
    ReadStdDatasetConfig, ReadStdKeywordsConfig, ReadStdTEXTConfig, VersionOverride,
    WriteDatasetInnerConfig, WriteMultiConfig, WriteMultiDatasetConfig,
};
use crate::core::{
    Analysis, AnyCoreDataset, AnyCoreTEXT, DatasetSegments, LookupAndReadDataAnalysisError,
    LookupAndReadDataAnalysisWarning, Others, PrivVersionSet as _, StdDatasetFromFlatTEXTWarning,
    StdDatasetFromFlatTextError, StdDatasetFromKwsOutput, StdTEXTDiagnostics,
    StdTEXTFromFlatTEXTError, StdTEXTFromFlatTEXTWarning, StdWriterError, WriteDatasetSummary,
};
use crate::data::{EventsDiagnostics, IndexedLossError};
use crate::header::{
    GuessVersionError, Header, HeaderError, KeywordVersionScores, autodetect_version,
};
use crate::logging::{
    DeferredWarningsAndErrors, ErrorsResult, IOAnonErrorGroup, IOErrorGroup, LogResult,
    ResultExt as _, SuccessResultIter as _, SwitchableErrorResult, SwitchableErrorsResult,
    WarningAndErrorResult, WarningsAndErrorResult, WarningsAndErrorsResult,
    WarningsAndIOGroupResult, io_to_log, split_log,
};
use crate::macros::def_summary;
use crate::segment::{
    AnyRegion, GuessOtherWidthError, HasRegion, IsDataOrAnalysis, KeyedOptSegment as _,
    KeyedReqSegment as _, OptSegmentError, PrimaryTextSegment, ReqSegmentError,
    SegmentOverlapError, SupplementalTextSegment, SupplementalTextSegmentId, TEXTSegment,
    UncorrectedSegment,
};
use crate::text::keywords::{
    AlphaNumType, Begindata, Beginstext, Cyt, Enddata, Endstext, Nextdata, ReadNextdataError, Tot,
};
use crate::text::lookup::ReqMetarootKey as _;
use crate::validated::dataframe::PrimitiveDataFrame;
use crate::validated::header_segments::{
    NextdataOffsetsError, ParsedHeaderSegments, SegmentValidationError,
};
use crate::validated::keys::{
    InvalidKeywordCharsError, Key as _, KeyOrBytes, KeywordInsertError, NEStringOrBytes, NonStdKey,
    ParsedKeywords, ParsedKeywordsDiagnostic, StdKey, StdKeywords, StdPresent, StringOrBytes,
    TruncatedNEString, ValidKeywords,
};

use fireflow_types::config::DelimEscapeMode;
use fireflow_types::keywords::{Version, Version2_0, Version3_0, Version3_1, Version3_2};
use fireflow_types::nonempty_string::NESliceExt as _;
use hashbrown::HashMap;
use type_families::{ApplyOnce as _, Functor as _, FunctorOnce as _};

use derive_more::{Display, From};
use derive_new::new;
use itertools::Itertools as _;
use nonempty_collections::{IntoIteratorExt as _, NESlice, NEVec, NonEmptyIterator as _};
use thiserror::Error;

use core::fmt;
use std::fs;
use std::io::{BufReader, Read, Seek};
use std::iter;
use std::num::NonZeroUsize;
use std::path::PathBuf;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr},
    fireflow_types::python as py,
};

/// Read HEADER from an FCS file.
pub fn fcs_read_header(
    path: &PathBuf,
    dataset_offset: DatasetOffset,
    conf: &ReadHeaderConfig,
) -> WarningsAndIOGroupResult<Header, GuessOtherWidthError, ReadHeaderError, HeaderSummary> {
    let file_res = ReadState::open(path, dataset_offset, conf)
        .map_err(|e| e.fmap_once(ReadHeaderError::from))
        .map_err(IOAnonErrorGroup::from)
        .map_err(IOAnonErrorGroup::deanonymize);
    let (st, file) = io_to_log!(file_res);
    let mut reader = BufReader::new(file);
    Header::h_read(&mut reader, &st)
        .map_error(|e| e.fmap(ReadHeaderError::from))
        // .warnings_to_pure_errors(&conf.shared, ReadHeaderError::from)
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
    FlatTEXTOutput::read(path, dataset_offset, conf)
        .map_ok_value(|(x, _, _)| x)
        .warnings_to_pure_errors(conf.shared, HeaderOrFlatTextError::from)
        .deanonymize()
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
    FlatTEXTOutput::read(path, dataset_offset, conf)
        .map_ok_value(|(x, _, st)| (x, st))
        .map_commutative_warnings(StdTEXTWarning::from)
        .map_pure_errors(StdTEXTError::from)
        .and_then_commutative(|(flat, st)| {
            flat.into_std_text(&st)
                .map_commutative_warnings(StdTEXTWarning::from)
                .map_errors(StdTEXTError::from)
                .group()
                .map_errors(IOErrorGroup::Pure)
        })
        .warnings_to_pure_errors(conf.shared, StdTEXTError::from)
        .deanonymize()
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
    FlatTEXTOutput::read(path, dataset_offset, conf)
        .map_pure_errors(FlatDatasetError::from)
        .map_commutative_warnings(FlatDatasetWarning::from)
        .and_then_commutative(|(flat, h, st)| {
            let version = flat.flat_diagnostics.header_supp.header.version;
            let oride = conf.flat.version_override.as_ref();
            autodetect_version(version, &flat.keywords.std, oride)
                .map_err(FlatDatasetError::from)
                .map_err(IOErrorGroup::new_pure_one)
                .map(|(new_version, scores)| (new_version, flat, h, st, scores))
                .into_log()
        })
        .and_then_commutative(|(new_ver, mut flat, mut h, st, scores)| {
            let hns = &mut flat.flat_diagnostics.header_supp;
            let std = &flat.keywords.std;
            FlatDatasetFromKwsOutput::h_read_with_header_and_text(&mut h, new_ver, std, hns, &st)
                .map_ok_value(|dataset| FlatDatasetOutput::new(flat, dataset, scores))
                .map_commutative_warnings(FlatDatasetWarning::from)
                .map_pure_errors(FlatDatasetError::from)
        })
        .warnings_to_pure_errors(conf.shared, FlatDatasetError::from)
        .deanonymize()
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
    FlatTEXTOutput::read(path, dataset_offset, conf)
        .map_commutative_warnings(StdDatasetWarning::from)
        .map_pure_errors(StdDatasetError::from)
        .and_then_commutative(|(flat, mut h, st)| {
            flat.into_std_dataset(&mut h, &st)
                .map_commutative_warnings(StdDatasetWarning::from)
                .map_pure_errors(StdDatasetError::from)
        })
        .warnings_to_pure_errors(conf.shared, StdDatasetError::from)
        .deanonymize()
}

/// Read DATA/ANALYSIS in FCS file using provided keywords.
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn fcs_read_flat_dataset_with_keywords(
    path: &PathBuf,
    mut hns: HeaderAndSuppOffsets,
    std: &StdKeywords,
    dataset_offset: DatasetOffset,
    conf: &ReadFlatDatasetFromKeywordsConfig,
) -> WarningsAndIOGroupResult<
    NewFlatDatasetFromKwsOutput,
    LookupAndReadDataAnalysisWarning,
    LookupAndReadDataAnalysisError,
    FlatDatasetWithKwsSummary,
> {
    ReadState::open(path, dataset_offset, conf)
        .map_err(|e| e.fmap_once(LookupAndReadDataAnalysisError::from))
        .map_err(IOErrorGroup::from)
        .into_log()
        .and_then_commutative(|(st, file)| {
            let v = hns.header.version;
            let mut h = BufReader::new(file);
            FlatDatasetFromKwsOutput::h_read_with_header_and_text(&mut h, v, std, &mut hns, &st)
        })
        .map_ok_value(|dataset| NewFlatDatasetFromKwsOutput::new(dataset, hns.header.segments))
        .warnings_to_pure_errors(conf.shared, LookupAndReadDataAnalysisError::from)
        .deanonymize()
}

/// Read HEADER and TEXT from multiple datasets in flat mode.
#[must_use]
pub fn fcs_read_flat_texts(
    path: &PathBuf,
    skip: Option<usize>,
    limit: Option<usize>,
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
        let res = fcs_read_flat_text(path, dso, conf);
        let succ = split_log!(res);
        let nextdata_res = succ.fmap_once(|ret| {
            let hns = &ret.flat_diagnostics.header_supp;
            let nd = hns.nextdata.map(u64::from);
            dataset_offset = nd.and_then(|n| (n > 0).then_some(DatasetOffset(dso.0 + n)));
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

/// Read HEADER and TEXT from multiple datasets in standardized mode.
#[must_use]
pub fn fcs_read_std_texts(
    path: &PathBuf,
    skip: Option<usize>,
    limit: Option<usize>,
    conf: &ReadStdTEXTConfig,
) -> WarningsAndIOGroupResult<
    Vec<(AnyCoreTEXT, StdTEXTOutput)>,
    MultiStdTEXTWarning,
    MultiStdTEXTError,
    StdTEXTSummary,
> {
    read_nextdata_loop(
        path,
        skip,
        limit,
        conf,
        StdTEXTSummary,
        fcs_read_std_text,
        |ret| ret.1.flat_diagnostics.header_supp.nextdata,
    )
}

/// Read multiple datasets from FCS file in flat mode.
#[must_use]
pub fn fcs_read_flat_datasets(
    path: &PathBuf,
    skip: Option<usize>,
    limit: Option<usize>,
    conf: &ReadFlatDatasetConfig,
) -> WarningsAndIOGroupResult<
    Vec<FlatDatasetOutput>,
    MultiFlatDatasetWarning,
    MultiFlatDatasetError,
    FlatDatasetSummary,
> {
    read_nextdata_loop(
        path,
        skip,
        limit,
        conf,
        FlatDatasetSummary,
        fcs_read_flat_dataset,
        |ret| ret.text.flat_diagnostics.header_supp.nextdata,
    )
}

/// Read multiple datasets from FCS file
#[must_use]
pub fn fcs_read_std_datasets(
    path: &PathBuf,
    skip: Option<usize>,
    limit: Option<usize>,
    conf: &ReadStdDatasetConfig,
) -> WarningsAndIOGroupResult<
    Vec<(AnyCoreDataset, StdDatasetOutput)>,
    MultiStdDatasetWarning,
    MultiStdDatasetError,
    StdDatasetSummary,
> {
    read_nextdata_loop(
        path,
        skip,
        limit,
        conf,
        StdDatasetSummary,
        fcs_read_std_dataset,
        |ret| ret.1.flat_diagnostics.header_supp.nextdata,
    )
}

/// Summarize the contents of an FCS file
#[must_use]
pub fn fcs_summarize(
    path: &PathBuf,
    skip: Option<usize>,
    limit: Option<usize>,
    conf: &ReadFlatDatasetConfig,
) -> WarningsAndIOGroupResult<
    Vec<DatasetSummary>,
    MultiFlatDatasetWarning,
    MultiFlatDatasetError,
    FlatDatasetSummary,
> {
    fcs_read_flat_datasets(path, skip, limit, conf)
        .map_ok_value(|x| x.fmap(FlatDatasetOutput::summarize))
}

/// Write multiple FCS datasets (of any version) to a file.
#[must_use]
pub fn fcs_write_datasets(
    path: &PathBuf,
    cores: &[AnyCoreDataset],
    conf: &WriteDatasetInnerConfig,
) -> WarningsAndIOGroupResult<Option<Nextdata>, IndexedLossError, StdWriterError, WriteDatasetSummary>
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

    /// Segments for DATA and ANALYSIS
    pub dataset_segments: DatasetSegments,

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
    pub header: ParsedHeaderSegments,
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
    pub dataset_segments: DatasetSegments,

    /// Diagnostic output from parsing DATA segment
    pub events_diagnostics: EventsDiagnostics,
}

// TODO should all these std/nonstd keys just be keystrings since the $ is implied?
/// Data pertaining to parsing the TEXT segment.
#[allow(clippy::too_many_arguments)]
#[derive(new, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct FlatTEXTDiagnostics {
    /// HEADER data and supplemental TEXT offsets
    pub header_supp: HeaderAndSuppOffsets,

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
    pub keys_with_trimmed_values: Vec<(KeyOrBytes, NEStringOrBytes)>,

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

    /// Supplemental TEXT offsets (corrected and uncorrected)
    ///
    /// This is not needed downstream and included here for informational
    /// purposes. It will always be None for 2.0 which does not include this.
    pub supp_text: Option<(Option<SupplementalTextSegment>, UncorrectedSegment)>,

    /// NEXTDATA offset
    ///
    /// This will be copied as represented in TEXT. If it is 0, there is no next
    /// dataset, otherwise it points to the next dataset in the file.
    pub nextdata: Option<Nextdata>,
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
}

/// Warning when parsing [`Header`]
#[derive(From, Display, Error, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ReadHeaderError {
    Header(HeaderError),
    DatasetOffset(DatasetOffsetError),
}

/// Warning when parsing TEXT in standard mode
#[derive(From, Display, Error, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum StdTEXTWarning {
    Flat(HeaderOrFlatTEXTWarning),
    Std(StdTEXTFromFlatTEXTWarning),
}

/// Error when parsing TEXT in standard mode
#[derive(From, Display, Error, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum StdTEXTError {
    Flat(HeaderOrFlatTextError),
    Std(StdTEXTFromFlatTEXTError),
    Warn(StdTEXTWarning),
}

/// Warning when parsing TEXT+DATA in standard mode
#[derive(From, Display, Error, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum StdDatasetWarning {
    Flat(HeaderOrFlatTEXTWarning),
    Std(StdDatasetFromFlatTEXTWarning),
}

/// Error when parsing TEXT+DATA in standard mode
#[derive(From, Display, Error, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum StdDatasetError {
    Flat(HeaderOrFlatTextError),
    Std(StdDatasetFromFlatTextError),
    Warn(StdDatasetWarning),
}

/// Warning when parsing TEXT+DATA in flat mode
#[derive(From, Display, Error, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum FlatDatasetWarning {
    Flat(HeaderOrFlatTEXTWarning),
    Read(LookupAndReadDataAnalysisWarning),
}

/// Warning when parsing TEXT+DATA in flat mode
#[derive(From, Display, Error, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum FlatDatasetError {
    Flat(HeaderOrFlatTextError),
    Read(LookupAndReadDataAnalysisError),
    Warn(FlatDatasetWarning),
    Version(GuessVersionError),
}

/// Error when parsing HEADER or TEXT segments in flat mode
#[derive(From, Display, Error, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum HeaderOrFlatTextError {
    DatasetOffset(DatasetOffsetError),
    Header(HeaderError),
    FlatTEXT(ParseFlatTEXTError),
    Warn(HeaderOrFlatTEXTWarning),
}

/// Error when looking up and parsing supplemental TEXT offsets from primary TEXT.
#[derive(From, Display, Error, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum STextSegmentError {
    ReqSegment(ReqSegmentError<Beginstext, Endstext>),
    Overlap(SegmentValidationError),
    Duplicated(DuplicateSTextError),
}

/// Warning when looking up and parsing supplemental TEXT offsets from primary TEXT.
#[derive(From, Display, Error, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum STextSegmentWarning {
    OptSegment(OptSegmentError<Beginstext, Endstext>),
    Error(STextSegmentError),
}

/// Error when supplement and primary TEXT offsets are identity
#[derive(Error, Debug, new)]
#[error(
    "{location} and supplemental TEXT have identical offsets, keeping {}: {offsets}",
    if self.keep_supp { "latter" } else { "former" }
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct DuplicateSTextError {
    offsets: UncorrectedSegment,
    location: AnyRegion,
    keep_supp: bool,
}

/// Warning when parsing multiple [`FlatDatasetOutput`]s
#[derive(From, Display, Error, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum MultiFlatDatasetWarning {
    Text(HeaderOrFlatTEXTWarning), // for reading skipped datasets to get $NEXTDATA
    Data(FlatDatasetWarning),
}

/// Error when parsing multiple [`FlatDatasetOutput`]s
#[derive(From, Display, Error, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum MultiFlatDatasetError {
    Text(HeaderOrFlatTextError), // for reading skipped datasets to get $NEXTDATA
    Data(FlatDatasetError),
}

/// Error when parsing multiple TEXT segments in std mode
#[derive(From, Display, Error, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum MultiStdTEXTError {
    FLat(HeaderOrFlatTextError), // for reading skipped datasets to get $NEXTDATA
    Single(StdTEXTError),
}

/// Warning when parsing multiple TEXT segments in std mode
#[derive(From, Display, Error, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum MultiStdTEXTWarning {
    Flat(HeaderOrFlatTEXTWarning), // for reading skipped datasets to get $NEXTDATA
    Std(StdTEXTWarning),
}

/// Error when parsing multiple datasets in std mode
#[derive(From, Display, Error, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum MultiStdDatasetError {
    Text(HeaderOrFlatTextError), // for reading skipped datasets to get $NEXTDATA
    Data(StdDatasetError),
}

/// Warning when parsing multiple TEXT segment in std mode
#[derive(From, Display, Error, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum MultiStdDatasetWarning {
    Flat(HeaderOrFlatTEXTWarning), // for reading skipped datasets to get $NEXTDATA
    Std(StdDatasetWarning),
}

/// Warning when parsing HEADER + TEXT segment in flat mode
#[derive(From, Display, Error, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum HeaderOrFlatTEXTWarning {
    Header(GuessOtherWidthError),
    Text(ParseFlatTEXTWarning),
}

/// Warning when parsing TEXT segment in flat mode
#[derive(From, Display, Error, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ParseFlatTEXTWarning {
    Char(DelimCharError),
    Primary(ParseKeywordsIssue),
    Supplemental(ParseSupplementalTEXTError),
    SuppOffsets(STextSegmentWarning),
    Nextdata(ReadNextdataError),
    InvalidChars(InvalidKeywordCharsError),
    AppendSupp(StdPresent),
}

/// Error when parsing TEXT segment in flat mode
#[derive(From, Display, Error, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ParseFlatTEXTError {
    Delim(DelimVerifyError),
    Primary(ParsePrimaryTEXTError),
    Supplemental(ParseSupplementalTEXTError),
    SuppOffsets(STextSegmentError),
    Nextdata(ReadNextdataError),
    InvalidKeyword(InvalidKeywordCharsError),
    InvalidChars(StdPresent),
    NextdataOffset(NextdataOffsetsError),
}

/// Error when parsing primary TEXT
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ParsePrimaryTEXTError {
    Keywords(ParseKeywordsIssue),
    Empty(NoTEXTWordsError),
}

/// Error when parsing supplemental TEXT
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ParseSupplementalTEXTError {
    Keywords(ParseKeywordsIssue),
    Mismatch(DelimMismatch),
}

/// Error when extracting keywords from TEXT segment (primary or supplemental)
#[derive(Display, From, Debug, Error)]
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

/// Error when verifying TEXT delimiter
#[derive(From, Display, Error, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum DelimVerifyError {
    Empty(EmptyTEXTError),
    Char(DelimCharError),
}

/// Error when TEXT delimiter is not ASCII
#[derive(Debug, Error)]
#[error("delimiter must be ASCII character 1-126 inclusive, got {0}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct DelimCharError(u8);

// TODO also for supp text
/// Error when primary TEXT segment is empty
#[derive(Debug, Error)]
#[error("Primary TEXT segment is empty")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct EmptyTEXTError;

// TODO also for supp text
/// Error when primary TEXT segment only has a delimiter
#[derive(Debug, Error)]
#[error("Primary TEXT has a delimiter and no tokens")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct NoTEXTWordsError;

/// Error when blank key is encountered in TEXT
#[derive(Debug, Error, new)]
#[error("skipping blank key in {kind} TEXT with value of '{value}'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct BlankKeyError {
    kind: TEXTKind,
    value: NEStringOrBytes,
}

/// Error when blank key is encountered in TEXT
#[derive(Debug, Error, new)]
#[error("there were {n} blank key/value pairs in {kind} TEXT")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct BlankPairError {
    kind: TEXTKind,
    n: NonZeroUsize,
}

/// Error when number of tokens in TEXT is not even
#[derive(Debug, Error, new)]
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
#[derive(Debug, Error)]
#[error("{0} TEXT contains an uneven number of delimiters")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct EvenDelimiterError(TEXTKind);

/// Error when delimiter(s) is/arg found after a token at a boundary.
///
/// This can only happen in escaped TEXT.
#[derive(Debug, Error, new)]
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
#[derive(Debug, Error, new)]
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
#[derive(Debug, Clone, Error, new)]
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
#[derive(Clone, Copy, Debug, Display)]
enum TEXTKind {
    #[display("Primary")]
    Primary,
    #[display("Supplemental")]
    Supplemental,
}

/// Result of guessing the escape more for TEXT.
#[derive(Debug, PartialEq)]
enum GuessedEscapeMode {
    Escaped,
    Unescaped,
    Ambiguous,
}

/// Indicates what was found for supplemental TEXT.
#[derive(Clone, Copy)]
enum SuppTEXTResult {
    Present(SupplementalTextSegment, UncorrectedSegment),
    Ignored(UncorrectedSegment),
    NotFound,
}

impl From<SuppTEXTResult> for Option<(Option<SupplementalTextSegment>, UncorrectedSegment)> {
    fn from(value: SuppTEXTResult) -> Self {
        match value {
            SuppTEXTResult::Present(x, y) => Some((Some(x), y)),
            SuppTEXTResult::Ignored(y) => Some((None, y)),
            SuppTEXTResult::NotFound => None,
        }
    }
}

impl HeaderAndSuppOffsets {
    /// Ensure this segment does not overlap with other segments.
    ///
    /// Specifically check that no other segment (except its analogue in HEADER
    /// if non-empty) overlaps with this one. Also ensure that that these
    /// segments don't overlap with HEADER itself.
    pub(crate) fn validate<I>(
        &mut self,
        s: &mut TEXTSegment<I>,
        limit: OverlapCorrectionLimit,
    ) -> Vec<SegmentValidationError>
    where
        I: HasRegion + IsDataOrAnalysis,
    {
        if let Some(this_seg) = s.try_as_generic() {
            // Check for overlap with STEXT segment. This segment should not be
            // modified since it has already been read. Therefore, only change
            // the offsets of the new segment if its ending offset is within
            // STEXT.
            let stxt_error = self
                .supp_text
                .as_ref()
                .and_then(|(x, _)| x.as_ref())
                .and_then(|supp| {
                    let stxt_seg = supp.try_as_generic()?;
                    if this_seg.as_pair() < stxt_seg.as_pair() {
                        let overlap = this_seg.get_tail_overlap(&stxt_seg);
                        if overlap <= limit.0 {
                            s.truncate(overlap);
                            None
                        } else {
                            let e = SegmentOverlapError::new(this_seg, stxt_seg);
                            Some(SegmentValidationError::from(e))
                        }
                    } else {
                        let overlap = stxt_seg.get_tail_overlap(&this_seg);
                        (overlap > 0).then(|| {
                            let e = SegmentOverlapError::new(this_seg, stxt_seg);
                            SegmentValidationError::from(e)
                        })
                    }
                });
            // Check for any errors between this segment and HEADER segments,
            // modifying as necessary and as overlap limit permits.
            self.header
                .segments
                .validate_text_data_or_analysis(s, limit)
                .chain(stxt_error)
                .collect()
        } else {
            vec![]
        }
    }
}

impl FlatDatasetOutput {
    fn summarize(self) -> DatasetSummary {
        let fd = self.text.flat_diagnostics;
        let hdr = fd.header_supp.header;
        let ds = self.dataset;
        let txt = AsRef::<PrimaryTextSegment>::as_ref(&hdr.segments);
        DatasetSummary {
            version: hdr.version,
            text_len: txt.len(),
            data_len: ds.dataset_segments.data.len(),
            analysis_len: ds.dataset_segments.analysis.len(),
            n_events: ds.data.nrows(),
            n_measurements: ds.data.ncols(),
            n_other: ds.others.0.len(),
            others_len: ds.others.0.iter().map(|x| x.0.len()).sum(),
            datatype: AlphaNumType::get_metaroot_req(&self.text.keywords.std).ok(),
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
        st: &ReadState<C>,
    ) -> WarningsAndIOGroupResult<
        Self,
        LookupAndReadDataAnalysisWarning,
        LookupAndReadDataAnalysisError,
        (),
    >
    where
        R: Read + Seek,
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadOffsetConfig> + AsRef<ReadEventsConfig>,
    {
        kws_to_df_analysis(new_version, h, kws, hns, st)
            .map_pure_errors(LookupAndReadDataAnalysisError::from)
            .and_then_commutative(|(data, analysis, dataset_segments, event_out)| {
                let or = hns.header.segments.others_reader();
                let go = |others| Self::new(data, analysis, others, dataset_segments, event_out);
                or.h_read(h).map(go).map_err(IOErrorGroup::from).into_log()
            })
    }
}

impl FlatTEXTOutput {
    /// Read flat TEXT from file path.
    #[allow(clippy::type_complexity)]
    fn read<C>(
        p: &PathBuf,
        dataset_offset: DatasetOffset,
        conf: C,
    ) -> WarningsAndIOGroupResult<
        (Self, BufReader<fs::File>, ReadState<C>),
        HeaderOrFlatTEXTWarning,
        HeaderOrFlatTextError,
        (),
    >
    where
        C: AsRef<ReadHeaderAndTEXTConfig> + AsRef<ReadHeaderInnerConfig> + AsRef<ReadOffsetConfig>,
    {
        ReadState::open(p, dataset_offset, conf)
            .map_err(|e| e.fmap_once(HeaderOrFlatTextError::from))
            .map_err(IOErrorGroup::from)
            .into_log()
            .and_then_commutative(|(st, file)| {
                let mut h = BufReader::new(file);
                Self::h_read(&mut h, &st).map_ok_value(|x| (x, h, st))
            })
    }

    /// Read flat TEXT from file handle.
    fn h_read<C, R>(
        h: &mut BufReader<R>,
        st: &ReadState<C>,
    ) -> WarningsAndErrorResult<
        Self,
        (),
        HeaderOrFlatTEXTWarning,
        IOErrorGroup<HeaderOrFlatTextError, ()>,
    >
    where
        R: Read + Seek,
        C: AsRef<ReadHeaderAndTEXTConfig> + AsRef<ReadHeaderInnerConfig> + AsRef<ReadOffsetConfig>,
    {
        Header::h_read(h, st)
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
        st: &ReadState<C>,
    ) -> WarningsAndIOGroupResult<Self, ParseFlatTEXTWarning, ParseFlatTEXTError, ()>
    where
        R: Read + Seek,
        C: AsRef<ReadHeaderAndTEXTConfig> + AsRef<ReadOffsetConfig>,
    {
        let conf = st.conf.as_ref();
        let mut buf = vec![];
        let ptext_seg: &PrimaryTextSegment = header.segments.as_ref();

        io_to_log!(ptext_seg.h_read_contents(h, &mut buf));
        let delim_res = split_first_delim(&buf, conf)
            .map_errors(ParseFlatTEXTError::from)
            .map_commutative_warnings(ParseFlatTEXTWarning::from)
            .into_semigroup();

        delim_res
            .group()
            .map_error(IOErrorGroup::Pure)
            .and_then_commutative(|(delim, bytes)| {
                // let mut kws = ParsedKeywords::default();
                SplitTEXTDiagnostics::primary_from_bytes(delim, bytes, conf)
                    .map_commutative_warnings(ParseFlatTEXTWarning::from)
                    .map_errors(ParseFlatTEXTError::from)
                    .group()
                    .map_error(IOErrorGroup::Pure)
                    .map_ok_value(|(kws, escaped)| (kws, delim, escaped))
            })
            .and_then_commutative(|(mut kws, delim, prim_out)| {
                lookup_supp_text_offsets(&kws.std, &mut header, st)
                    .map_commutative_warnings(ParseFlatTEXTWarning::from)
                    .map_errors(ParseFlatTEXTError::from)
                    .group()
                    .map_error(IOErrorGroup::Pure)
                    .set_err_value(())
                    .and_then_commutative(|seg| {
                        if let SuppTEXTResult::Present(corr_seg, _) = seg {
                            buf.clear();
                            SplitTEXTDiagnostics::h_read_supp(
                                h, &corr_seg, &mut kws, &mut buf, delim, conf,
                            )
                            .map_commutative_warnings(ParseFlatTEXTWarning::from)
                            .map_pure_errors(ParseFlatTEXTError::from)
                            .map_ok_value(|supp_out| (kws, seg, prim_out, supp_out))
                        } else {
                            LogResult::new_ok((kws, seg, prim_out, None))
                        }
                    })
            })
            .and_then_commutative(|(mut kws, supp_text_seg, prim_out, supp_out)| {
                let nextdata_res = Nextdata::lookup_ro(&kws.std, conf)
                    .map_commutative_warnings(ParseFlatTEXTWarning::from)
                    .map_errors(ParseFlatTEXTError::from)
                    .into_semigroup();

                let repair_res = kws
                    .append_std(&conf.append_standard_keywords, conf.allow_nonunique)
                    .switchable_into_commutative()
                    .map_commutative_warnings(ParseFlatTEXTWarning::from)
                    .map_errors(ParseFlatTEXTError::from);

                let vkws = ValidKeywords::new(kws.std, kws.nonstd);

                nextdata_res
                    .zip_f2_once(repair_res)
                    .set_err_value(())
                    .group()
                    .map_error(IOErrorGroup::Pure)
                    .and_then_commutative(|(nextdata, ())| {
                        // Check segments against $NEXTDATA
                        let es = if let Some(n) = nextdata {
                            let oconf: &ReadOffsetConfig = st.conf.as_ref();
                            let limit = oconf.overlap_correction_limit;
                            header.segments.validate_nextdata(n, limit)
                        } else {
                            vec![]
                        };
                        let nd_res = WarningsAndErrorsResult::new_from_err_iter(es, (), ())
                            .map_errors(ParseFlatTEXTError::from);

                        // Build diagnostics output, throw errors for bad keywords
                        let header_supp =
                            HeaderAndSuppOffsets::new(header, supp_text_seg.into(), nextdata);
                        let diag_res = kws
                            .diag
                            .into_flat_diag(header_supp, prim_out, supp_out, conf)
                            .map_commutative_warnings(ParseFlatTEXTWarning::from)
                            .map_errors(ParseFlatTEXTError::from)
                            .set_err_value(());
                        nd_res
                            .zip_commutative(diag_res)
                            .group()
                            .map_error(IOErrorGroup::Pure)
                    })
                    .map_ok_value(|((), diag)| Self::new(vkws, diag))
            })
    }

    /// Convert flat TEXT into standardized TEXT.
    fn into_std_text<C>(
        mut self,
        st: &ReadState<C>,
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
                    offsets.segs,
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
        st: &ReadState<C>,
    ) -> WarningsAndIOGroupResult<
        (AnyCoreDataset, StdDatasetOutput),
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
        seg: &SupplementalTextSegment,
        kws: &mut ParsedKeywords,
        buf: &mut Vec<u8>,
        delim: u8,
        conf: &ReadHeaderAndTEXTConfig,
    ) -> WarningsAndIOGroupResult<
        Option<Self>,
        ParseSupplementalTEXTError,
        ParseSupplementalTEXTError,
        (),
    > {
        io_to_log!(seg.h_read_contents(h, buf));
        Self::supp_from_bytes(kws, delim, buf, conf)
            .group()
            .map_error(IOErrorGroup::Pure)
    }

    /// Read primary TEXT from bytes and store keywords in hash table.
    fn primary_from_bytes(
        delim: u8,
        bytes: &[u8],
        conf: &ReadHeaderAndTEXTConfig,
    ) -> WarningsAndErrorsResult<
        (ParsedKeywords, Self),
        (),
        ParseKeywordsIssue,
        ParsePrimaryTEXTError,
    > {
        if bytes.is_empty() {
            LogResult::new_err(NoTEXTWordsError.into())
        } else {
            let raw_segs = Self::split_bytes(delim, bytes);
            let raw_slice = raw_segs.as_nonempty_slice();
            // We are about to insert a massive amount of data into two hash
            // tables, so make a guess as to how big they need to be to avoid
            // reallocation.
            //
            // Assume that the number of inserts to each hash table will be
            // roughly half the the number of segments since they come in pairs.
            // In practice, this will almost always be true, and may be a bit
            // less if some escapes are present.
            //
            // Also assume that the number of non-standard and standard keywords
            // is roughly equal. This probably varies quite a bit but it is hard
            // to know without scanning each token first which is also costly.
            //
            // Finally, assume the STEXT is almost never present and therefore
            // not worth considering. This makes the estimation much simpler
            // since we can't read STEXT without TEXT first.
            let cap = raw_segs.len().get() / 2;
            let mut kws = ParsedKeywords {
                std: HashMap::with_capacity(cap / 2),
                nonstd: HashMap::with_capacity(cap / 2),
                diag: ParsedKeywordsDiagnostic::default(),
            };
            Self::from_bytes_inner(&mut kws, delim, &raw_slice, TEXTKind::Primary, conf)
                .map_errors(ParsePrimaryTEXTError::from)
                .map_ok_value(|ret| (kws, ret))
        }
    }

    /// Read supp TEXT from bytes and store keywords in hash table.
    fn supp_from_bytes(
        kws: &mut ParsedKeywords,
        delim: u8,
        bytes: &[u8],
        conf: &ReadHeaderAndTEXTConfig,
    ) -> WarningsAndErrorsResult<
        Option<Self>,
        (),
        ParseSupplementalTEXTError,
        ParseSupplementalTEXTError,
    > {
        if let Some((byte0, rest)) = bytes.split_first() {
            let flag = conf.allow_supp_text_own_delim;
            let raw_segs = Self::split_bytes(*byte0, rest);
            let raw_slice = raw_segs.as_nonempty_slice();
            Self::from_bytes_inner(kws, *byte0, &raw_slice, TEXTKind::Supplemental, conf)
                .map_warnings_and_errors(ParseSupplementalTEXTError::from)
                .eval_warning_or_error3(
                    flag,
                    |_| (),
                    |()| (),
                    |_| (*byte0 != delim).then_some(DelimMismatch::new(delim, *byte0)),
                )
                .map_ok_value(Some)
        } else {
            // if empty do nothing, this is expected for most files
            LogResult::new_ok(None)
        }
    }

    fn split_bytes(delim: u8, xs: &[u8]) -> NEVec<&[u8]> {
        xs.split(|&x| x == delim)
            .try_into_nonempty_iter()
            .expect("split should always give at least one element")
            .collect()
    }

    /// Maybe trim end off slice of byte segments so that the length is even.
    ///
    /// Return final slice, the last odd non-empty slice if it was taken off,
    /// and a boolean that will be `true` if the number of tokens started as
    /// even. The 'perfect' case (ie standards compliant FCS file) is `None` and
    /// `true` for the odd slice and boolean. All combinations are possible.
    fn trim_segment_end<'a, 'b>(
        raw_segs: &'b NESlice<'_, &'a [u8]>,
    ) -> (&'b [&'a [u8]], Option<NESlice<'a, u8>>, bool) {
        let has_even_tokens = raw_segs.len().get() & 1 == 1;
        let (&last, rest) = raw_segs.split_last();
        let mut extra_seg = None;
        let even_segs = match (has_even_tokens, NESlice::try_from_slice(last)) {
            // Delimiter number is odd and last segment is empty. This should
            // happen in a perfect situation since the final segment should be
            // empty if TEXT ends with a delimiter, and the total number of
            // delimiters should be odd (which means the number of tokens is
            // even). This second part is true regardless of escaping.
            //
            // Return all but last empty segment as it is a blank.
            (true, None) => rest,
            // Delimiter number is odd but last segment is not empty. This means
            // there is an extra token at the end without a delimiter. Usually
            // this 'token' is whitespace padding.
            (true, extra) => {
                extra_seg = extra;
                rest
            }
            // Delimiter number is even but last segment is empty. This means
            // TEXT ended with a delimiter but the number of tokens is odd.
            // The last odd token may be blank, in which case TEXT ended with
            // two delimiters and the real one is 2nd from the end. This will
            // remove both since neither are necessary.
            (false, None) => {
                let (penultimate_seg, segs) = rest.split_last().expect(
                    "this should never fail because input is non empty and \
                     and we branch here if length is even",
                );
                extra_seg = NESlice::try_from_slice(penultimate_seg);
                segs
            }
            // Delimiter number is even and last segment is not empty. This
            // means TEXT did not end with a delimiter and the number of tokens
            // is even.
            (false, Some(_)) => raw_segs.as_ref(),
        };
        debug_assert!(
            even_segs.len() & 1 == 0,
            "number of segments should be even"
        );
        (even_segs, extra_seg, has_even_tokens)
    }

    /// Read TEXT segment (primary or supp) from bytes.
    fn from_bytes_inner(
        kws: &mut ParsedKeywords,
        delim: u8,
        raw_segs: &NESlice<'_, &'_ [u8]>,
        tk: TEXTKind,
        conf: &ReadHeaderAndTEXTConfig,
    ) -> WarningsAndErrorsResult<Self, (), ParseKeywordsIssue, ParseKeywordsIssue> {
        let escaped = GuessedEscapeMode::is_escaped(raw_segs, conf.delim_escape_mode);
        if escaped {
            Self::insert_escaped(kws, delim, raw_segs, tk, conf)
        } else {
            Self::insert_unescaped(kws, delim, raw_segs, tk, conf)
        }
    }

    /// Split bytes without delimiter escaping and store keys in hash table.
    fn insert_unescaped(
        kws: &mut ParsedKeywords,
        delim: u8,
        segs: &NESlice<'_, &[u8]>,
        tk: TEXTKind,
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

        let (pairs, extra_seg, has_even_tokens) = Self::trim_segment_end(segs);

        out.has_even_delims = !has_even_tokens;

        let matchers = conf.as_matchers();

        for (key, value) in pairs.iter().tuples() {
            let k = NESlice::try_from_slice(key);
            let v = NESlice::try_from_slice(value);
            match (k, v) {
                (Some(kk), Some(vv)) => {
                    if let Some((e, is_err)) = kws.insert(&kk, &vv, &matchers, conf) {
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

        out.last_odd_token = extra_seg
            .as_ref()
            .map(|s| s.as_ref().to_vec().into())
            .unwrap_or_default();

        let last_odd_err = extra_seg
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
            let _ = ks.insert(kb, vb, &matchers, conf).map(|(e, is_err)| {
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

        // Prime the loop with the first segment which belongs to a key. This
        // will fail if TEXT is entirely delimiters, in which case there is
        // nothing more to do.
        keybuf = if let Some(segment0) = it.by_ref().find_map(|segment| {
            let ne = NESlice::try_from_slice(segment);
            if ne.is_none() {
                out.extra_leading_delims += 1;
            }
            ne
        }) {
            segment0.to_ne_vec()
        } else {
            // No segments found, which means TEXT is entirely delimiters.
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

        for segment in it {
            if let Some(ne_segment) = NESlice::try_from_slice(segment) {
                if consec_blanks & 1 == 0 {
                    // Previous consecutive delimiter sequence was odd (which
                    // means the number of blanks is even). This is a token
                    // boundary, and the last sequence of segments can be
                    // processed as needed.
                    if consec_blanks > 0 {
                        // If we have more than one delimiter (more than zero
                        // blanks) then there are multiple delimiters on the end
                        // which is not allowed. Scream at user, they will be
                        // happy and enlightened.
                        let seg = NEStringOrBytes::from(ne_segment.to_ne_vec());
                        out.tokens_with_boundary_delims.push(seg);
                    }
                    if let Some(ne_val) = NESlice::try_from_slice(&valbuf[..]) {
                        push_pair(kws, &keybuf.as_nonempty_slice(), &ne_val);
                        valbuf.clear();
                        keybuf = ne_segment.to_ne_vec();
                    } else {
                        valbuf.extend_from_slice(ne_segment.as_ref());
                    }
                } else {
                    // Previous consecutive delimiter sequence was even. Push
                    // this number / 2 followed by the current token fragment
                    // to the active buffer.
                    let ds = iter::repeat_n(delim, consec_blanks.div_ceil(2));
                    if valbuf.is_empty() {
                        keybuf.extend(ds.chain(ne_segment.iter().copied()));
                    } else {
                        valbuf.extend(ds.chain(ne_segment.iter().copied()));
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

    fn has_any_empty(raw_segs: &NESlice<'_, &[u8]>) -> bool {
        // Only consider the first even number of segments since both modes
        // should deal with extra crap at the end in the same way
        let (segs, _, _) = SplitTEXTDiagnostics::trim_segment_end(raw_segs);
        segs.iter().any(|s| s.is_empty())
    }

    fn test_both_modes(raw_segs: &NESlice<'_, &[u8]>) -> Self {
        // Only consider the first even number of segments since both modes
        // should deal with extra crap at the end in the same way
        let (segs, _, _) = SplitTEXTDiagnostics::trim_segment_end(raw_segs);

        let mut any_empty_tokens = false;
        let mut any_unescaped_blank_keys = false;
        let mut any_escaped_delims_in_keys = false;
        let mut prev_escaped_was_key = false;

        // Loop through segments as if in either escaped or unescaped mode
        // and test if we have any blank keys (unescaped) or keys with escaped
        // delims (escaped). Also track if we have any empty segments at all,
        // because if we have none then the choice of mode doesn't matter and
        // we can choose whatever is fastest to maximize performance.
        for (i, s) in segs.iter().enumerate() {
            // In unescaped mode, even tokens are keys; test if any are blank
            if i & 1 == 0 && s.is_empty() {
                any_unescaped_blank_keys = true;
            }
            // In escaped mode, record if we encounter two consecutive
            // delimiters (ie a blank segment) while in a key.
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

fn read_nextdata_loop<X, W, E, Wi, Ei, G, C, Fsucc, Fnext>(
    p: &PathBuf,
    skip: Option<usize>,
    limit: Option<usize>,
    conf: &C,
    g: G,
    mut f0: Fsucc,
    mut fnext: Fnext,
) -> WarningsAndIOGroupResult<Vec<X>, W, E, G>
where
    Fsucc: FnMut(&PathBuf, DatasetOffset, &C) -> WarningsAndIOGroupResult<X, Wi, Ei, G>,
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
    while let Some(dso) = dataset_offset
        && limit.is_none_or(|x| count <= x)
    {
        let nextdata_res = if skip.is_some_and(|s| count < s) {
            let res = fcs_read_flat_text(p, dso, &rconf)
                .map_commutative_warnings(W::from)
                .map_pure_errors(E::from)
                .map_error(|e| e.set_group(g));
            let succ = split_log!(res);
            succ.fmap_once(|ret| {
                let hns = ret.flat_diagnostics.header_supp;
                let nd = hns.nextdata.map(u64::from);
                dataset_offset = nd.and_then(|n| (n > 0).then_some(DatasetOffset(dso.0 + n)));
                None
            })
        } else {
            let res = f0(p, dso, conf)
                .map_commutative_warnings(W::from)
                .map_pure_errors(E::from);
            let succ = split_log!(res);
            succ.fmap_once(|ret| {
                dataset_offset = fnext(&ret)
                    .map(u64::from)
                    .and_then(|nd| (nd > 0).then_some(DatasetOffset(dso.0 + nd)));
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

fn kws_to_df_analysis<C, R>(
    new_version: Version,
    h: &mut BufReader<R>,
    kws: &StdKeywords,
    hns: &mut HeaderAndSuppOffsets,
    st: &ReadState<C>,
) -> WarningsAndIOGroupResult<
    (PrimitiveDataFrame, Analysis, DatasetSegments, EventsDiagnostics),
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
    bytes: &'a [u8],
    conf: &ReadHeaderAndTEXTConfig,
) -> WarningAndErrorResult<(u8, &'a [u8]), (), DelimCharError, DelimVerifyError> {
    if let Some((delim, rest)) = bytes.split_first() {
        let is_ok = (1..=126).contains(delim);
        let e = DelimCharError(*delim);
        let flag = conf.allow_non_ascii_delim;
        SwitchableErrorResult::new_switchable_ok_if3(is_ok, (*delim, rest), (), e, flag)
            .switchable_into_commutative()
            .map_errors(DelimVerifyError::from)
    } else {
        LogResult::new_err(EmptyTEXTError.into())
    }
}

fn lookup_supp_text_offsets<C>(
    kws: &StdKeywords,
    header: &mut Header,
    st: &ReadState<C>,
) -> DeferredWarningsAndErrors<SuppTEXTResult, STextSegmentWarning, STextSegmentError>
where
    C: AsRef<ReadHeaderAndTEXTConfig> + AsRef<ReadOffsetConfig>,
{
    let hconf: &ReadHeaderAndTEXTConfig = st.conf.as_ref();
    let oconf: &ReadOffsetConfig = st.conf.as_ref();
    // At this point, we have not yet overridden the version since we have not
    // read STEXT and therefore might not have all keywords. This puts us in a
    // bit of an awkward spot in the case we wish to autodetect the version.
    // Primary TEXT by definition must have all required keywords, so we can use
    // $BEGIN/ENDDATA to test if the version is 3.0 or higher. Additionally, we
    // can use lack of $CYT to test if the version is less then 3.2, although in
    // practice this keyword is usually present despite it being optional
    // pre-3.2. This all likely doesn't matter much anyways since STEXT is
    // seldom used.
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
    let corr = hconf.supp_text_correction;
    let res = match ver {
        Version::FCS2_0 => LogResult::new_ok(None),
        Version::FCS3_0 | Version::FCS3_1 => {
            let pair = SupplementalTextSegmentId::get_req_pair(kws);
            match SupplementalTextSegmentId::with_req_pair(pair, corr, st) {
                Ok(seg) => LogResult::new_ok(Some(seg)),
                Err(es) => {
                    let (e0, e1) = es.split();
                    let flag = hconf.allow_missing_supp_text;
                    SwitchableErrorsResult::new_deferred_switchable3(None, e0, flag)
                        .extend_deferred_switchable_errors3(e1)
                        .map_switchable_errors(STextSegmentError::from)
                        .switchable_into_commutative()
                        .map_commutative_warnings(STextSegmentWarning::from)
                }
            }
        }
        Version::FCS3_2 => {
            let pair = SupplementalTextSegmentId::get_opt_pair(kws);
            match SupplementalTextSegmentId::with_opt_pair(pair, corr, st) {
                Ok(seg) => LogResult::new_ok(seg),
                Err(es) => {
                    let mut res = DeferredWarningsAndErrors::new_ok(None);
                    res.extend_commutative_warnings(es);
                    res.map_commutative_warnings(STextSegmentWarning::from)
                }
            }
        }
    };
    res.and_then_deferred(|maybe| {
        if let Some((mut seg_stxt, uncorr_stxt)) = maybe {
            // Return uncorrected segments without any processing if ignored
            let present = SuppTEXTResult::Present(seg_stxt, uncorr_stxt);
            let ignored = SuppTEXTResult::Ignored(uncorr_stxt);
            if hconf.ignore_supp_text.is_set() {
                return LogResult::new_ok(ignored);
            }

            // Offsets found, check for validity
            let uncorr_ptxt = header.uncorrected_segments.text;
            let uncorr_anal = header.uncorrected_segments.analysis;
            let uncorr_others = &mut header.uncorrected_segments.other[..];

            let go = |loc| {
                // Supp TEXT is identical to another segment. Keep the other
                // segment and return None for supp TEXT
                //
                // TODO it may be necessary to configure which segment to keep
                // in the future.
                let flag = hconf.allow_duplicated_supp_text;
                let e = DuplicateSTextError::new(uncorr_stxt, loc, false);
                SwitchableErrorsResult::new_switchable3(ignored, ignored, e, flag)
                    .map_switchable_errors(STextSegmentError::from)
                    .switchable_into_commutative()
                    .map_commutative_warnings(STextSegmentWarning::from)
            };

            if seg_stxt.is_empty() {
                // supp TEXT is empty, return as-is
                LogResult::new_ok(present)
            } else if uncorr_ptxt == uncorr_stxt {
                // Primary and supp are identical, keep primary
                go(AnyRegion::Text)
            } else if uncorr_ptxt == uncorr_anal {
                // Supp and ANALYSIS are the same, keep latter
                go(AnyRegion::Analysis)
            } else if let Some(i) = uncorr_others.iter().position(|s| s == &uncorr_stxt) {
                // Supp and one OTHER offset are the same, keep Supp and remove
                // matching OTHER with the assumption that Supp is actually
                // a real supp text and not some binary blob.
                //
                // TODO this assumption can be checked by reading the segment
                // but this would make this function way more complex.
                //
                // See FR-FCM-ZZZ4/MVa2011-06-30_fcs31.fcs for an example of
                // this
                // LogResult::new_ok(SuppTEXTResult::Present(seg_stxt, uncorr_stxt))
                header.segments.remove_other(i);
                let flag = hconf.allow_duplicated_supp_text;
                let e = DuplicateSTextError::new(uncorr_stxt, AnyRegion::Other, true);
                SwitchableErrorsResult::new_switchable3(present, present, e, flag)
                    .map_switchable_errors(STextSegmentError::from)
                    .switchable_into_commutative()
                    .map_commutative_warnings(STextSegmentWarning::from)
            } else {
                // Supp not identical to anything else, check for overlaps and
                // keep if there are none. ASSUME the HEADER segments have
                // already been validated and adjusted such that they do not
                // overlap.
                let limit = oconf.overlap_correction_limit;
                let es = header
                    .segments
                    .validate_supp_text(&mut seg_stxt, limit)
                    .map(STextSegmentError::from);
                // TODO throw warnings sometimes for these?
                ErrorsResult::new_from_err_iter(es, present, ignored).nowarn_into_warn()
            }
        } else {
            // No offsets found
            LogResult::new_ok(SuppTEXTResult::NotFound)
        }
    })
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
        let raw_segs: NEVec<_> = bytes
            .split(|&x| x == delim)
            .try_into_nonempty_iter()
            .unwrap()
            .collect();
        let raw_slice = raw_segs.as_nonempty_slice();
        let out = SplitTEXTDiagnostics::insert_escaped(
            &mut kws,
            delim,
            &raw_slice,
            TEXTKind::Primary,
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

//! Top-level functions for parsing FCS files
use crate::config::{
    ConfigFlag as _, DatasetOffset, DatasetOffsetError, OverlapCorrectionLimit,
    ReadDataKeywordsConfig, ReadEventsConfig, ReadFlatDatasetConfig,
    ReadFlatDatasetFromKeywordsConfig, ReadFlatTEXTConfig, ReadHeaderAndTEXTConfig,
    ReadHeaderConfig, ReadHeaderInnerConfig, ReadOffsetConfig, ReadSharedConfig, ReadState,
    ReadStdDatasetConfig, ReadStdKeywordsConfig, ReadStdTEXTConfig, VersionOverride,
};
use crate::core::{
    Analysis, AnyCoreDataset, AnyCoreTEXT, DatasetSegments, LookupAndReadDataAnalysisError,
    LookupAndReadDataAnalysisWarning, Others, PrivVersioned as _, StdDatasetFromFlatTEXTWarning,
    StdDatasetFromFlatTextError, StdDatasetFromKwsOutput, StdTEXTDiagnostics,
    StdTEXTFromFlatTEXTError, StdTEXTFromFlatTEXTWarning,
};
use crate::data::EventsDiagnostics;
use crate::header::{
    GuessVersionError, Header, HeaderError, KeywordVersionScores, Version, Version2_0, Version3_0,
    Version3_1, Version3_2,
};
use crate::logging::{
    DeferredIter as _, DeferredWarningsAndErrors, ErrorsResult, IOAnonErrorGroup, IOErrorGroup,
    LogResult, ResultExt as _, SuccessResultIter as _, SwitchableErrorResult,
    SwitchableErrorsResult, WarningAndErrorResult, WarningsAndErrorResult, WarningsAndErrorsResult,
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
use crate::validated::dataframe::FCSDataFrame;
use crate::validated::header_segments::{
    NextdataOffsetsError, ParsedHeaderSegments, SegmentValidationError,
};
use crate::validated::keys::{
    InvalidKeywordCharsError, Key as _, KeyOrBytes, KeywordInsertError, NonStdKey, ParsedKeywords,
    StdKey, StdKeywords, StdPresent, StringOrBytes, TruncatedBytes, TruncatedString, ValidKeywords,
};

use fireflow_types::config::DelimEscapeMode;
use type_families::{ApplyOnce as _, Functor as _, FunctorOnce as _};

use derive_more::{Display, From};
use derive_new::new;
use itertools::Itertools as _;
use nonempty_collections::{NESlice, NEVec};
use thiserror::Error;

use std::fs;
use std::io::{BufReader, Read, Seek};
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
            version
                .autodetect(&flat.keywords.std, conf.flat.version_override.as_ref())
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
    pub data: FCSDataFrame,

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
    pub byte_pairs: Vec<(KeyOrBytes, StringOrBytes)>,

    /// Standard keys which appear more than once with their values.
    pub non_unique_std_keywords: Vec<(StdKey, TruncatedString)>,

    /// Nonstandard keys which appear more than once with their values.
    pub non_unique_nonstd_keywords: Vec<(NonStdKey, TruncatedString)>,

    /// Ignored standard keys with their values
    pub ignored_standard_keywords: Vec<(StdKey, StringOrBytes)>,

    /// Keys with empty values as a result of trimming whitespace.
    pub keys_with_empty_trimmed_values: Vec<KeyOrBytes>,

    /// Keys with values that are not empty after whitespace was trimmed off.
    ///
    /// Values included here are the original values before trimming.
    pub keys_with_trimmed_values: Vec<(KeyOrBytes, StringOrBytes)>,

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
    pub keys_with_blank_values: Vec<StringOrBytes>,

    /// Values with blank keys.
    pub values_with_blank_keys: Vec<StringOrBytes>,

    /// Tokens with delimiters at their boundaries (without the delimiters).
    ///
    /// Only relevant in escaped delimiter mode.
    pub tokens_with_boundary_delims: Vec<StringOrBytes>,

    /// Last token if the number of tokens was odd.
    pub last_odd_token: StringOrBytes,

    /// `true` if final delimiter was missing
    pub missing_final_delim: bool,

    /// `true` if there was an extra delimiter after TEXT which was ignored
    pub has_extra_delim: bool,

    /// Training bytes after TEXT
    pub trailing_bytes: Vec<u8>,
}

struct SplitTEXTOutputInner {
    keys_with_blank_values: Vec<StringOrBytes>,
    values_with_blank_keys: Vec<StringOrBytes>,
    tokens_with_boundary_delims: Vec<StringOrBytes>,
    last_odd_token: StringOrBytes,
    missing_final_delim: bool,
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
    BlankKey(BlankKeyError),
    Uneven(UnevenTokensError),
    Final(FinalDelimError),
    EvenFinal(EvenFinalDelimError),
    Insert(KeywordInsertError),
    Bound(DelimBoundError),
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

/// Error when primary TEXT segment is empty
#[derive(Debug, Error)]
#[error("Primary TEXT segment is empty")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct EmptyTEXTError;

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
    value: StringOrBytes,
}

/// Error when number of tokens in TEXT is not even
#[derive(Debug, Error)]
#[error("{0} TEXT segment has uneven number of tokens")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct UnevenTokensError(TEXTKind);

/// Error when final character in TEXT is not a delimiter
#[derive(Debug, Error, new)]
#[error(
    "{kind} TEXT does not end with delim; instead ends with {n}-byte sequence: {bytes}",
    n = self.bytes.0.len()
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct FinalDelimError {
    kind: TEXTKind,
    bytes: TruncatedBytes,
}

/// Error when TEXT ends with even number of delimiters
///
/// This can only happen in escaped TEXT
#[derive(Debug, Error)]
#[error("Primary TEXT ends with an even number of delimiters and thus are all escaped")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct EvenFinalDelimError;

/// Error when delimiter is found at token boundary.
///
/// This can only happen in escaped TEXT
#[derive(Debug, Error, new)]
#[error(
    "escaped delimiter encountered before unescaped delimiter \
     and after '{value}' in {kind} TEXT"
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct DelimBoundError {
    kind: TEXTKind,
    value: StringOrBytes,
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

/// Data used to fix the final offset of TEXT.
///
/// To fix the final offset, first assume it is never too small. This means
/// everything in TEXT is available to be read in the byte segment, possibly
/// with more non-TEXT at the end. The objective is to figure out how much
/// to trim off the end.
///
/// TEXT may be too long for several reasons:
///
/// 1) Some files pad TEXT with extra non-delimiter chars at the end (spaces
///    usually, but sometimes null). The real offset is likely much smaller than
///    the one in HEADER. Sometimes non-delimiter chars may be from other
///    segments. In these cases, the offset is likely only one greater than it
///    should be and thus there will only be one non-delimiter character.
///
/// 2) Some files add one extra delimiter to the end of TEXT (for unknown
///    reasons) which results in an offset that is one greater than correct.
///    These extra delimiters can be detected by counting all delimiters in TEXT
///    (they should be odd for both escape modes).
///
/// These can occur in combination (and are properly dealt with here).
#[derive(Default)]
struct TrimTEXTData {
    /// Number of consecutive final delimiters.
    ///
    /// If this is >1 and the number of total delimiters is even, assume that
    /// the last delimiter was erroneously added and remove it.
    final_delim: usize,
    /// Number of delims that are not consecutive final delimiters.
    ///
    /// These are necessary to figure out if the total number of delimiters is
    /// odd or even.
    other_delim: usize,
    /// Number of non-delim chars after last delim.
    ///
    /// These will be trimmed off.
    trailing: usize,
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
                let mut kws = ParsedKeywords::default();
                SplitTEXTDiagnostics::primary_from_bytes(&mut kws, delim, bytes, conf)
                    .map_commutative_warnings(ParseFlatTEXTWarning::from)
                    .map_errors(ParseFlatTEXTError::from)
                    .group()
                    .map_error(IOErrorGroup::Pure)
                    .map_ok_value(|escaped| (kws, delim, escaped))
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
        kws: &mut ParsedKeywords,
        delim: u8,
        bytes: &[u8],
        conf: &ReadHeaderAndTEXTConfig,
    ) -> WarningsAndErrorsResult<Self, (), ParseKeywordsIssue, ParsePrimaryTEXTError> {
        if bytes.is_empty() {
            LogResult::new_err(NoTEXTWordsError.into())
        } else {
            Self::from_bytes_inner(kws, delim, bytes, TEXTKind::Primary, conf)
                .map_errors(ParsePrimaryTEXTError::from)
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
            Self::from_bytes_inner(kws, *byte0, rest, TEXTKind::Supplemental, conf)
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

    /// Read TEXT segment (primary or supp) from bytes.
    fn from_bytes_inner(
        kws: &mut ParsedKeywords,
        delim: u8,
        bytes: &[u8],
        tk: TEXTKind,
        conf: &ReadHeaderAndTEXTConfig,
    ) -> WarningsAndErrorsResult<Self, (), ParseKeywordsIssue, ParseKeywordsIssue> {
        let (trimmed_bytes, has_final, remainder): (&[u8], bool, &[u8]) =
            if conf.trim_text_end.is_set() {
                TrimTEXTData::split_trimmed(delim, bytes)
            } else {
                (bytes, false, &[])
            };
        let escaped = GuessedEscapeMode::is_escaped(delim, trimmed_bytes, conf.delim_escape_mode);
        let res = if escaped {
            SplitTEXTOutputInner::split_escaped(kws, delim, trimmed_bytes, tk, conf)
        } else {
            SplitTEXTOutputInner::split_unescaped(kws, delim, trimmed_bytes, tk, conf)
        };
        res.map_ok_value(|inner| Self {
            delimiter: delim,
            keys_with_blank_values: inner.keys_with_blank_values,
            values_with_blank_keys: inner.values_with_blank_keys,
            tokens_with_boundary_delims: inner.tokens_with_boundary_delims,
            last_odd_token: inner.last_odd_token,
            missing_final_delim: inner.missing_final_delim,
            has_extra_delim: has_final,
            trailing_bytes: remainder.to_vec(),
            escaped,
        })
    }
}

impl SplitTEXTOutputInner {
    /// Split bytes without delimiter escaping and store keys in hash table.
    fn split_unescaped(
        kws: &mut ParsedKeywords,
        delim: u8,
        bytes: &[u8],
        tk: TEXTKind,
        conf: &ReadHeaderAndTEXTConfig,
    ) -> WarningsAndErrorsResult<Self, (), ParseKeywordsIssue, ParseKeywordsIssue> {
        let mut keys_with_blank_values = vec![];
        let mut values_with_blank_keys = vec![];
        let mut insert_results = vec![];

        let mut it = bytes.split(|x| *x == delim).peekable();
        let mut prev_was_key = false;
        let mut prev_token: &[u8] = &[];

        while let Some(key) = it.next() {
            prev_was_key = true;
            prev_token = key;
            if let Some(ne_key) = NESlice::try_from_slice(key) {
                if let Some(value) = it.next() {
                    prev_was_key = false;
                    prev_token = value;
                    if let Some(ne_value) = NESlice::try_from_slice(value) {
                        let e = kws
                            .insert(&ne_key, &ne_value, conf)
                            .non_commutative_into_commutative()
                            .map_commutative_warnings(ParseKeywordsIssue::from)
                            .map_errors(ParseKeywordsIssue::from);
                        insert_results.push(e);
                    } else {
                        // If there is nothing after a blank value this actually means
                        // that TEXT has an odd number of tokens and ends with a
                        // delimiter, and the "value" is the blank after the last
                        // delimiter
                        if it.peek().is_some() {
                            keys_with_blank_values.push(StringOrBytes::from(key.to_vec()));
                        }
                    }
                } else {
                    // exiting here means we found a key without a value and also didn't
                    // end with a delim
                    break;
                }
            } else if let Some(value) = it.next() {
                prev_was_key = false;
                prev_token = value;
                values_with_blank_keys.push(StringOrBytes::from(value.to_vec()));
            } else {
                // if everything is correct, we should exit here since the
                // last token will be the blank slice after the final delim
                break;
            }
        }

        // We should end on a blank, which corresponds to a (not valid) key. If this
        // is not the case, the number of tokens was not even.
        let uneven_ok = prev_was_key;
        let uneven_err = UnevenTokensError(tk).into();
        let uneven_res =
            LogResult::new_switchable_ok_if3(uneven_ok, (), (), uneven_err, conf.allow_odd)
                .switchable_into_commutative();
        let last_odd_token = if uneven_ok {
            StringOrBytes::default()
        } else {
            prev_token.to_vec().into()
        };

        // If the last token was not a blank, we did not end on a delimiter.
        let delim_flag = conf.allow_missing_final_delim;
        let final_delim_err = NEVec::try_from_slice(prev_token)
            .map(|bs| FinalDelimError::new(tk, TruncatedBytes(Vec::from(bs))))
            .map(ParseKeywordsIssue::from);
        let missing_final_delim = final_delim_err.is_some();
        let final_delim_res = LogResult::new_switchable_maybe3((), (), final_delim_err, delim_flag)
            .switchable_into_commutative();

        let blank_key_errors = values_with_blank_keys
            .iter()
            .map(|k| BlankKeyError::new(tk, k.clone()));

        let blank_key_res = SwitchableErrorsResult::new_switchable_iter3(
            (),
            (),
            blank_key_errors,
            conf.allow_empty_keys,
        )
        .map_switchable_errors(ParseKeywordsIssue::from)
        .switchable_into_commutative();

        let ret = Self {
            keys_with_blank_values,
            values_with_blank_keys,
            tokens_with_boundary_delims: vec![],
            last_odd_token,
            missing_final_delim,
        };

        // TODO this is one instance where it could be inefficient to chain together
        // lots of options, which are stack allocated but need to be converted to
        // singleton vectors (heap allocated) to turn each of the results into
        // a semigroup that can be concated. Two options a) tune the iterator so
        // it can consume options or b) use stack-vectors for warnings
        insert_results
            .into_iter()
            .map(LogResult::into_semigroup)
            .chain([uneven_res, blank_key_res, final_delim_res])
            .sequence_def_void()
            .set_ok_value(ret)
    }

    /// Split bytes with delimiter escaping and store keys in hash table.
    #[allow(clippy::too_many_lines)]
    fn split_escaped(
        kws: &mut ParsedKeywords,
        delim: u8,
        bytes: &[u8],
        tk: TEXTKind,
        conf: &ReadHeaderAndTEXTConfig,
    ) -> WarningsAndErrorsResult<Self, (), ParseKeywordsIssue, ParseKeywordsIssue> {
        let mut insert_results = vec![];
        let mut tokens_with_boundary_delims = vec![];

        let mut push_pair = |kb: &NESlice<u8>, vb: &NESlice<u8>| {
            let e = kws
                .insert(kb, vb, conf)
                .non_commutative_into_commutative()
                .map_commutative_warnings(ParseKeywordsIssue::from)
                .map_errors(ParseKeywordsIssue::from);
            insert_results.push(e);
        };

        let push_delim = |kb: &mut Vec<_>, vb: &mut Vec<_>, k: usize| {
            let n = k.div_ceil(2);
            let buf = if vb.is_empty() { kb } else { vb };
            for _ in 0..n {
                buf.push(delim);
            }
        };

        let mut consec_blanks = 0;
        let mut lastbuf: &[u8] = &[];
        let mut keybuf: Vec<u8> = vec![];
        let mut valuebuf: Vec<u8> = vec![];

        for segment in bytes.split(|x| *x == delim) {
            if segment.is_empty() {
                consec_blanks += 1;
            } else {
                if consec_blanks & 1 == 0 {
                    if consec_blanks > 0 {
                        let seg = StringOrBytes::from(segment.to_vec());
                        tokens_with_boundary_delims.push(seg);
                    }
                    // Previous number of delimiters is odd, treat this as a token
                    // boundary
                    if let Some(ne_val) = NESlice::try_from_slice(&valuebuf[..]) {
                        let ne_key = NESlice::try_from_slice(&keybuf[..])
                            .expect("key buffer should not be empty");
                        push_pair(&ne_key, &ne_val);
                        keybuf.clear();
                        valuebuf.clear();
                        keybuf.extend_from_slice(segment);
                    } else if !keybuf.is_empty() {
                        valuebuf.extend_from_slice(segment);
                    } else {
                        // this should only be reached on first iteration
                        keybuf.extend_from_slice(segment);
                    }
                } else {
                    // Previous consecutive delimiter sequence was even. Push n / 2
                    // delimiters to whatever the current token is. Then push to
                    // key or value
                    push_delim(&mut keybuf, &mut valuebuf, consec_blanks);
                    if valuebuf.is_empty() {
                        keybuf.extend_from_slice(segment);
                    } else {
                        valuebuf.extend_from_slice(segment);
                    }
                }
                consec_blanks = 0;
            }
            lastbuf = segment;
        }

        // If all went perfectly, we should have one consecutive blank at this point
        // since the space between the last delim and the end will show up as a
        // blank. The value of the last buffer should also be an empty slice.
        //
        // If we have 0 consecutive blanks, then there was no delim at the end,
        // which is an error. In this case the last buffer should be a non-empty
        // slice.
        //
        // If number of blanks is even and not 0, then the last token ended with one
        // or more escaped delimiters, but the TEXT didn't (2 errors, delim at
        // boundary and no delim ending TEXT). Note that here, blanks = number of
        // literal delimiters, whereas in the loop, this corresponded to blanks + 1
        // delimiters.
        //
        // If number of blanks is odd but not 1, the last token ended with one or
        // more escaped delimiters (error: on a boundary) and the TEXT ended with a
        // delimiter (not an error).

        let mut even_delim_err = None;

        if consec_blanks > 1 {
            let seg = if valuebuf.is_empty() {
                keybuf.clone()
            } else {
                valuebuf.clone()
            };

            if consec_blanks & 1 == 0 {
                even_delim_err = Some(EvenFinalDelimError.into());
            } else {
                tokens_with_boundary_delims.push(StringOrBytes::from(seg));
            }
        }

        let (uneven_err, last_odd_token) =
            if let Some(ne_val) = NESlice::try_from_slice(&valuebuf[..]) {
                let ne_key =
                    NESlice::try_from_slice(&keybuf[..]).expect("key buffer should not be empty");
                push_pair(&ne_key, &ne_val);
                (None, None)
            } else {
                (
                    Some(UnevenTokensError(tk).into()),
                    Some(keybuf.clone().into()),
                )
            };

        let uneven_res = LogResult::new_switchable_maybe3((), (), uneven_err, conf.allow_odd)
            .switchable_into_commutative();

        // NOTE this is the same flag used for when the delimiter is missing
        // entirely since this is the net result of escaping an even number of
        // delimiters
        let delim_flag = conf.allow_missing_final_delim;
        let even_delim_res = LogResult::new_switchable_maybe3((), (), even_delim_err, delim_flag)
            .switchable_into_commutative();
        let final_delim_err = NEVec::try_from_slice(lastbuf)
            .map(|bs| FinalDelimError::new(tk, TruncatedBytes(Vec::from(bs))))
            .map(ParseKeywordsIssue::from);
        let missing_final_delim = final_delim_err.is_some();
        let final_delim_res = LogResult::new_switchable_maybe3((), (), final_delim_err, delim_flag)
            .switchable_into_commutative();

        let bound_iter = tokens_with_boundary_delims
            .iter()
            .map(|token| DelimBoundError::new(tk, token.clone()).into());
        let boundary_res =
            LogResult::new_switchable_iter3((), (), bound_iter, conf.allow_delim_at_boundary)
                .switchable_into_commutative();

        let ret = Self {
            keys_with_blank_values: vec![],
            values_with_blank_keys: vec![],
            tokens_with_boundary_delims,
            last_odd_token: last_odd_token.unwrap_or_default(),
            missing_final_delim,
        };

        insert_results
            .into_iter()
            .map(LogResult::into_semigroup)
            .chain([uneven_res, even_delim_res, boundary_res, final_delim_res])
            .sequence_def_void()
            .set_ok_value(ret)
    }
}

// impl FlatTEXTDiagnostics {
//     /// Extract HEADER offset data for use in reading offsets from TEXT
//     fn non_data_segments(&self) -> NonDataSegments {
//         let hs = self.header_supp.header.segments.clone();
//         let supp = self.header_supp.supp_text.as_ref().copied().map(|(c, _)| c);
//         let ud = self.header_supp.header.uncorrected_segments.data;
//         let ua = self.header_supp.header.uncorrected_segments.analysis;
//         NonDataSegments::new(hs, supp, ud, ua)
//     }
// }

impl GuessedEscapeMode {
    fn is_escaped(delim: u8, bytes: &[u8], mode: DelimEscapeMode) -> bool {
        let res = match mode {
            DelimEscapeMode::Unescaped => Ok(false),
            DelimEscapeMode::Escaped => Ok(true),
            DelimEscapeMode::GuessEscaped => Err(true),
            DelimEscapeMode::GuessUnescaped => Err(false),
        };
        res.unwrap_or_else(|default| match Self::from_bytes(delim, bytes) {
            Self::Escaped => true,
            Self::Unescaped => false,
            Self::Ambiguous => default,
        })
    }

    fn from_bytes(delim: u8, bytes: &[u8]) -> Self {
        let mut n_unescaped_tokens = 0_usize;
        let mut n_escaped_tokens = 0_usize;

        // Init to 1 to include leading delim
        let mut n_consecutive_delims = 1_usize;

        // If any consecutive delims found. If this is false then it doesn't
        // matter what mode we pick since the result will be the same in any
        // case.
        let mut any_consec_delims = false;

        // True if we have seen a non-delim character. Necessary since we only
        // know if we have a new escaped token after we see a non-delim, but we
        // can only increase the token count if we know that the characters
        // before the delim sequence were non-delims. This is the case for all
        // delim sequences except the very first one
        let mut non_delim_seen = false;

        // Init these to true since the first token is a key by definition
        let mut in_unescaped_key = true;
        let mut in_escaped_key = true;

        // Count of keys with escaped delimiters in them. These are not allowed
        // so if this is non-zero then we cannot use escaped mode.
        let mut n_keys_with_escaped_delims = 0_usize;

        // Count of keys which are blank. This can only happen in unescaped mode
        // by definition, and we can assume that blank keys are invalid. If this
        // is non-zero then we cannot use unescaped mode.
        let mut n_unescaped_blank_keys = 0_usize;

        // Number of consecutive non-delim sequences in the current token in
        // escaped mode. This is used to determine if a key has escaped delims
        // in it.
        let mut n_escaped_token_fragments = 0_usize;

        for b in bytes {
            if *b == delim {
                if n_consecutive_delims > 0 {
                    any_consec_delims = true;
                    if in_unescaped_key {
                        n_unescaped_blank_keys += 1;
                    }
                }
                in_unescaped_key = !in_unescaped_key;
                n_unescaped_tokens += 1;
                n_consecutive_delims += 1;
            } else {
                if non_delim_seen {
                    if n_consecutive_delims & 1 == 1 {
                        // if previous number of delims is odd, treat this as a
                        // token boundary and thus everything prior is a new
                        // token
                        if n_escaped_token_fragments > 0 && in_escaped_key {
                            n_keys_with_escaped_delims += 1;
                        }
                        n_escaped_tokens += 1;
                        in_escaped_key = !in_escaped_key;
                        n_escaped_token_fragments = 0;
                    } else if n_consecutive_delims > 0 {
                        // if previous number of delims is even, treat this as
                        // an escaped delim sequence and increase fragment count
                        n_escaped_token_fragments += 1;
                    }
                }
                non_delim_seen = true;
                n_consecutive_delims = 0;
            }
        }

        // Unprime the loop since we can only check for escaped tokens after we
        // encounter a non-delim character, which won't happen for the final
        // token if TEXT ends with a delim (which it should if perfectly valid)
        if non_delim_seen && n_consecutive_delims & 1 == 1 {
            // if previous number of delims is odd, treat this as a token
            // boundary and thus everything prior is a new token
            if n_escaped_token_fragments > 0 && in_escaped_key {
                n_keys_with_escaped_delims += 1;
            }
            n_escaped_tokens += 1;
        }

        // If there are no consecutive delims, it doesn't matter what mode we
        // choose (from a parsing perspective) since they are both equivalent.
        // Choose unescaped since this should be slightly faster.
        if !any_consec_delims {
            return Self::Unescaped;
        }

        let unescaped_even_tokens = n_unescaped_tokens & 1 == 0;
        let unescaped_nw_blank = n_unescaped_blank_keys == 0;
        let escaped_even_tokens = n_escaped_tokens & 1 == 0;
        let escaped_no_delim = n_keys_with_escaped_delims == 0;

        let m = (
            unescaped_even_tokens,
            unescaped_nw_blank,
            escaped_even_tokens,
            escaped_no_delim,
        );

        match m {
            // Unescaped mode results in even token with no blank keys
            (true, true, false, _) | (true, true, _, false) => Self::Unescaped,
            // Escaped mode results in even token with no delims in keys
            (false, _, true, true) | (_, false, true, true) => Self::Escaped,
            // All other cases we can't make a determination based on any number
            // of issues (outlined for each combination below). Rather than
            // error here return control to the default escape mode parser and
            // let that throw errors when encountered. This has the advantage of
            // letting the user control how fallback happens, and if they want
            // to accept dangling tokens, missing final delims, etc after
            // failing to guess.
            //
            // Both modes result in even token count with no other issues, not
            // sure which to pick
            (true, true, true, true)
            // Both modes result in odd token count
                | (false, _, false, _)
            // Escaped has even token count but delims in keys
                | (false, _, true, false)
            // Enescaped has even token count but blank keys
                | (true, false, false, _)
            // Both have even tokens but keys have issues
                | (true, false, true, false) => Self::Ambiguous,
        }
    }
}

impl TrimTEXTData {
    /// Split bytes into untrimmed and trimmed slices.
    fn split_trimmed(delim: u8, bytes: &[u8]) -> (&[u8], bool, &[u8]) {
        Self::from_bytes(delim, bytes).split_bytes(bytes)
    }

    fn from_bytes(delim: u8, bytes: &[u8]) -> Self {
        let mut it = bytes.iter().rev();
        // Count number of non-delims (ie garbage/space) at the end of TEXT
        let trailing = it.by_ref().peeking_take_while(|&&x| x != delim).count();
        // Count number of delimiters at the end of TEXT (after trimming garbage)
        let final_delim = it.by_ref().peeking_take_while(|&&x| x == delim).count();
        // Count the rest of the delimiters
        let other_delim = it.filter(|&&x| x == delim).count();
        Self {
            final_delim,
            other_delim,
            trailing,
        }
    }

    fn total_delim(&self) -> usize {
        self.final_delim + self.other_delim + 1
    }

    fn has_final_double_delim(&self) -> bool {
        // Compute the final position at which to trim. If the number of
        // delimiters is even and TEXT ends with at least 2 delimiters, assume
        // one of the delimiters is "extra" and remove it by adding one to the
        // number of non-delimiter chars after TEXT we wish to trim off.
        self.total_delim() & 1 == 0 && self.final_delim > 1
    }

    fn split_bytes<'a>(&self, bytes: &'a [u8]) -> (&'a [u8], bool, &'a [u8]) {
        let has_final = self.has_final_double_delim();
        let n_trim = self.trailing + usize::from(has_final);
        debug_assert!(n_trim <= bytes.len(), "trying to trim more than length");
        let split_index = bytes.len() - n_trim;
        let (trimmed, rem) = bytes.split_at(split_index);
        (trimmed, has_final, &rem[usize::from(has_final)..])
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
    // segs: &mut NonDataSegments,
    st: &ReadState<C>,
) -> WarningsAndIOGroupResult<
    (FCSDataFrame, Analysis, DatasetSegments, EventsDiagnostics),
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

def_summary!(HeaderSummary, "could not parse HEADER");

def_summary!(FlatTEXTSummary, "could not parse TEXT segment");

def_summary!(StdTEXTSummary, "could not standardize TEXT segment");

def_summary!(
    StdDatasetSummary,
    "could not read DATA with standardized TEXT"
);

def_summary!(FlatDatasetSummary, "could not read DATA with flat TEXT");

def_summary!(
    FlatDatasetWithKwsSummary,
    "could not read flat dataset from keywords"
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_text_escape() {
        let mut kws = ParsedKeywords::default();
        let conf = ReadHeaderAndTEXTConfig::default();
        // NOTE should not start with delim
        let bytes = b"$P4F/700//75 BP/";
        let delim = 47;
        let out =
            SplitTEXTOutputInner::split_escaped(&mut kws, delim, bytes, TEXTKind::Primary, &conf);
        let (_, ws, es) = out.deconstruct();
        let v = kws
            .std
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .next()
            .unwrap();
        assert_eq!(("$P4F".into(), "700/75 BP".into()), v);
        assert!(es.is_empty(), "errors: {es:?}");
        assert!(ws.is_empty(), "warnings: {ws:?}");
    }

    #[test]
    fn guess_no_escaped() {
        let txt = "aaa/bbb/ccc/ddd/";
        assert_eq!(
            GuessedEscapeMode::from_bytes(b'/', txt.as_bytes()),
            GuessedEscapeMode::Unescaped
        );
    }

    #[test]
    fn guess_odd_tokens() {
        let txt = "aaa//bbb//ccc//ddd/";
        assert_eq!(
            GuessedEscapeMode::from_bytes(b'/', txt.as_bytes()),
            GuessedEscapeMode::Ambiguous
        );
    }

    #[test]
    fn guess_escaped() {
        let txt = "aaa/bbb//bbb/ccc/ddd/";
        assert_eq!(
            GuessedEscapeMode::from_bytes(b'/', txt.as_bytes()),
            GuessedEscapeMode::Escaped
        );
    }

    #[test]
    fn guess_unescaped() {
        let txt = "aaa//bbb/bbb/ccc/ddd/";
        assert_eq!(
            GuessedEscapeMode::from_bytes(b'/', txt.as_bytes()),
            GuessedEscapeMode::Unescaped
        );
    }

    #[test]
    fn guess_blank_key_and_key_delim() {
        let txt = "aaa//bbb/bbb//ccc/ddd/eee/";
        assert_eq!(
            GuessedEscapeMode::from_bytes(b'/', txt.as_bytes()),
            GuessedEscapeMode::Ambiguous
        );
    }
}

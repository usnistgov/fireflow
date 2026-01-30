//! Top-level functions for parsing FCS files
use crate::config::{
    ConfigFlag as _, DatasetOffset, DatasetOffsetError, DelimEscapeMode, ReadDataKeywordsConfig,
    ReadEventsConfig, ReadFlatDatasetConfig, ReadFlatDatasetFromKeywordsConfig, ReadFlatTEXTConfig,
    ReadHeaderAndTEXTConfig, ReadHeaderConfig, ReadHeaderInnerConfig, ReadOffsetConfig,
    ReadSharedConfig, ReadState, ReadStdDatasetConfig, ReadStdKeywordsConfig, ReadStdTEXTConfig,
    TriFlag, VersionOverride,
};
use crate::core::{
    Analysis, AnyCoreDataset, AnyCoreTEXT, DatasetSegments, LookupAndReadDataAnalysisError,
    LookupAndReadDataAnalysisWarning, Others, OthersReader, PrivVersioned as _,
    StdDatasetFromFlatTEXTWarning, StdDatasetFromFlatTextError, StdDatasetWithKwsOutput,
    StdTEXTDiagnostics, StdTEXTFromFlatTEXTError, StdTEXTFromFlatTEXTWarning,
};
use crate::data::EventsDiagnostics;
use crate::header::{
    GuessVersionError, Header, HeaderError, HeaderSegments, HeaderValidationError,
    KeywordVersionScores, UncorrectedHeaderSegments, Version, Version2_0, Version3_0, Version3_1,
    Version3_2,
};
use crate::logging::{
    CommutativeResultIter as _, DeferredIter as _, DeferredWarningAndError,
    DeferredWarningsAndErrors, IOAnonErrorGroup, IOErrorGroup, LogResult, ResultExt as _, Success,
    SuccessResultIter as _, SwitchableErrorResult, SwitchableErrorsResult, WarningAndErrorResult,
    WarningsAndErrorResult, WarningsAndErrorsResult, WarningsAndIOGroupResult, io_to_log,
    split_log,
};
use crate::macros::def_summary;
use crate::segment::{
    AnalysisSegmentId, DataSegmentId, GuessOtherWidthError, KeyedOptSegment as _,
    KeyedReqSegment as _, NonDataSegments, OptSegmentError, OtherSegmentId, RelativeSegment,
    ReqSegmentError, SupplementalTextSegment, SupplementalTextSegmentId, UncorrectedSegment,
};
use crate::text::keywords::{
    AlphaNumType, Begindata, Beginstext, Cyt, Enddata, Endstext, Nextdata, Tot,
};
use crate::text::lookup::{
    OptKeyError, OptMetarootKey as _, ReqKeyError, ReqMetarootKey as _, truncate_string,
};
use crate::validated::ascii_uint::UintSpacePad20;
use crate::validated::dataframe::FCSDataFrame;
use crate::validated::keys::{
    BlankValueError, BytesPairs, Key as _, KeywordInsertError, NonStdKey, ParsedKeywords, StdKey,
    StdKeywords, StdPresent, StringOrBytes, ValidKeywords,
};

use type_families::{ApplyOnce as _, Functor as _, FunctorOnce as _};

use derive_more::{Display, From};
use derive_new::new;
use itertools::Itertools as _;
use nonempty::NonEmpty;
use thiserror::Error;

use std::fmt;
use std::fs;
use std::io::{BufReader, Read, Seek};
use std::iter::once;
use std::num::NonZeroUsize;
use std::path::PathBuf;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {
    crate::python as py,
    fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr},
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
    read_flat_text_inner(path, dataset_offset, conf)
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
    read_flat_text_inner(path, dataset_offset, conf)
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
    read_flat_text_inner(path, dataset_offset, conf)
        .map_pure_errors(FlatDatasetError::from)
        .map_commutative_warnings(FlatDatasetWarning::from)
        .and_then_commutative(|(mut flat, h, st)| {
            flat.version
                .autodetect(&flat.keywords.std, conf.flat.version_override.as_ref())
                .map_err(FlatDatasetError::from)
                .map_err(IOErrorGroup::new_pure_one)
                .map(|(v, scores)| {
                    flat.version = v;
                    (flat, h, st, scores)
                })
                .into_log()
        })
        .and_then_commutative(|(flat, mut h, st, scores)| {
            let segs = flat.flat_diagnostics.non_data_segments();
            h_read_dataset_from_kws(&mut h, flat.version, &flat.keywords.std, &segs, &st)
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
    read_flat_text_inner(path, dataset_offset, conf)
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
    version: Version,
    std: &StdKeywords,
    data_seg: RelativeSegment<DataSegmentId>,
    analysis_seg: RelativeSegment<AnalysisSegmentId>,
    other_segs: Vec<RelativeSegment<OtherSegmentId>>,
    dataset_offset: DatasetOffset,
    conf: &ReadFlatDatasetFromKeywordsConfig,
) -> WarningsAndIOGroupResult<
    FlatDatasetWithKwsOutput,
    LookupAndReadDataAnalysisWarning,
    LookupAndReadDataAnalysisError,
    FlatDatasetWithKwsSummary,
> {
    ReadState::open(path, dataset_offset, conf)
        .map_err(|e| e.fmap_once(LookupAndReadDataAnalysisError::from))
        .map_err(IOErrorGroup::from)
        .into_log()
        .and_then_commutative(|(st, file)| {
            let data_res = data_seg
                .relative_to_abs(dataset_offset, st.file_len)
                .into_nowarn();
            let anal_res = analysis_seg
                .relative_to_abs(dataset_offset, st.file_len)
                .into_nowarn();
            let oss_res = other_segs
                .into_iter()
                .map(|s| s.relative_to_abs(dataset_offset, st.file_len).into_log())
                .sequence_commutative();
            data_res
                .zip3_commutative(anal_res, oss_res)
                .map_errors(LookupAndReadDataAnalysisError::from)
                .map_ok_value(|(d, a, o)| (d, a, o, st, file))
                .nowarn_into_warn()
                .group()
                .map_error(IOErrorGroup::Pure)
        })
        .and_then_commutative(|(d, a, os, st, file)| {
            let segs = NonDataSegments::new_no_text(d, a, os);
            let mut h = BufReader::new(file);
            h_read_dataset_from_kws(&mut h, version, std, &segs, &st)
        })
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
            dataset_offset = ret
                .flat_diagnostics
                .nextdata
                .and_then(|nd| (nd > 0).then_some(DatasetOffset(dso.0 + nd)));
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
        |ret| ret.1.flat_diagnostics.nextdata,
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
        |ret| ret.text.flat_diagnostics.nextdata,
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
        |ret| ret.1.flat_diagnostics.nextdata,
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
    Fnext: FnMut(&X) -> Option<u64>,
    E: From<HeaderOrFlatTextError> + From<Ei>,
    W: From<HeaderOrFlatTEXTWarning> + From<Wi>,
    C: AsRef<ReadHeaderAndTEXTConfig> + AsRef<ReadOffsetConfig> + AsRef<ReadSharedConfig>,
    G: Copy,
{
    let mut dataset_offset = Some(DatasetOffset::default());
    let mut count = 0_usize;
    let mut results = vec![];
    let rconf = ReadFlatTEXTConfig {
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
                dataset_offset = ret
                    .flat_diagnostics
                    .nextdata
                    .and_then(|nd| (nd > 0).then_some(DatasetOffset(dso.0 + nd)));
                None
            })
        } else {
            let res = f0(p, dso, conf)
                .map_commutative_warnings(W::from)
                .map_pure_errors(E::from);
            let succ = split_log!(res);
            succ.fmap_once(|ret| {
                dataset_offset =
                    fnext(&ret).and_then(|nd| (nd > 0).then_some(DatasetOffset(dso.0 + nd)));
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

/// Output from parsing the TEXT segment.
#[derive(Clone, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct FlatTEXTOutput {
    /// FCS version
    pub version: Version,

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
    pub dataset: FlatDatasetWithKwsOutput,

    /// Scores generated if version was guessed.
    pub version_scores: Option<KeywordVersionScores>,
}

/// Output of parsing one standardized dataset (TEXT+DATA) from an FCS file.
#[derive(Clone, new, PartialEq)]
pub struct StdDatasetOutput {
    /// Standardized data from one FCS dataset
    pub dataset: StdDatasetWithKwsOutput,

    /// Miscellaneous data from parsing TEXT
    pub flat_diagnostics: FlatTEXTDiagnostics,

    /// Scores generated if version was guessed.
    pub version_scores: Option<KeywordVersionScores>,
}

/// Output of using keywords to read flat TEXT+DATA
#[derive(Clone, PartialEq, new)]
pub struct FlatDatasetWithKwsOutput {
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

/// Data pertaining to parsing the TEXT segment.
#[allow(clippy::too_many_arguments)]
#[derive(new, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct FlatTEXTDiagnostics {
    /// Corrected offsets read from HEADER
    pub header_segments: HeaderSegments<UintSpacePad20>,

    /// Uncorrected offsets read from HEADER
    pub uncorrected_header_segments: UncorrectedHeaderSegments,

    /// Supplemental TEXT offsets (corrected and uncorrected)
    ///
    /// This is not needed downstream and included here for informational
    /// purposes. It will always be None for 2.0 which does not include this.
    pub supp_text: Option<(SupplementalTextSegment, UncorrectedSegment)>,

    /// NEXTDATA offset
    ///
    /// This will be copied as represented in TEXT. If it is 0, there is no next
    /// dataset, otherwise it points to the next dataset in the file.
    pub nextdata: Option<u64>,

    /// Delimiter used to parse TEXT.
    ///
    /// Included here for informational purposes.
    pub delimiter: u8,

    /// Keywords that could not be parsed.
    ///
    /// These have either a non-ASCII key or a non-UTF8 value (or both).
    /// Included here for debugging
    pub byte_pairs: BytesPairs,

    /// Standard keys which appear more than once with their values.
    pub non_unique_std_keywords: Vec<(StdKey, String)>,

    /// Nonstandard keys which appear more than once with their values.
    pub non_unique_nonstd_keywords: Vec<(NonStdKey, String)>,

    /// Ignored standard keys with their values
    pub ignored_standard_keywords: Vec<(StdKey, StringOrBytes)>,

    /// Keys with empty values as a result of trimming whitespace.
    pub keys_with_empty_trimmed_values: Vec<StringOrBytes>,

    /// Keys with values that are not empty after whitespace was trimmed off.
    ///
    /// Values included here are the original values before trimming.
    pub keys_with_trimmed_values: Vec<(StringOrBytes, StringOrBytes)>,

    /// Output from splitting primary TEXT
    pub primary_split: SplitTEXTDiagnostics,

    /// Output from splitting supplemental TEXT
    pub supp_split: Option<SplitTEXTDiagnostics>,
}

/// Data pertaining to parsing the TEXT segment.
#[derive(new, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct SplitTEXTDiagnostics {
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

    /// Length of trailing whitespace after TEXT in bytes
    pub trailing_whitespace_length: usize,
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

impl FlatDatasetOutput {
    fn summarize(self) -> DatasetSummary {
        DatasetSummary {
            version: self.text.version,
            text_len: self.text.flat_diagnostics.header_segments.text.len(),
            data_len: self.dataset.dataset_segments.data.len(),
            analysis_len: self.dataset.dataset_segments.analysis.len(),
            n_events: self.dataset.data.nrows(),
            n_measurements: self.dataset.data.ncols(),
            n_other: self.dataset.others.0.len(),
            others_len: self.dataset.others.0.iter().map(|x| x.0.len()).sum(),
            datatype: AlphaNumType::get_metaroot_req(&self.text.keywords.std).ok(),
        }
    }
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
    Overlap(HeaderValidationError),
}

/// Warning when looking up and parsing supplemental TEXT offsets from primary TEXT.
#[derive(From, Display, Error, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum STextSegmentWarning {
    OptSegment(OptSegmentError<Beginstext, Endstext>),
    Error(STextSegmentError),
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
    Nextdata(OptKeyError<Nextdata>),
    NonUtf8(NonUtf8KeywordError),
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
    Nextdata(ReqKeyError<Nextdata>),
    NonUtf8(NonUtf8KeywordError),
    AppendSupp(StdPresent),
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
    // TODO not necessary?
    BlankValue(BlankValueError),
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
#[error(
    "skipping blank key in {kind} TEXT with value of '{}' in Latin-1",
    truncate_string(self.bytes.as_latin1(21).as_str(), 20),
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct BlankKeyError {
    kind: TEXTKind,
    bytes: StringOrBytes,
}

/// Error when number of tokens in TEXT is not even
#[derive(Debug, Error)]
#[error("{0} TEXT segment has uneven number of tokens")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct UnevenTokensError(TEXTKind);

/// Error when final character in TEXT is not a delimiter
#[derive(Debug, Error, new)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct FinalDelimError {
    kind: TEXTKind,
    bytes: NonEmpty<u8>,
}

#[derive(Clone, Copy, Debug, Display)]
enum TEXTKind {
    #[display("Primary")]
    Primary,
    #[display("Supplemental")]
    Supplemental,
}

impl fmt::Display for FinalDelimError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        const MAX_FINAL_BYTES: usize = 20;
        let n = self.bytes.len();
        let xs: Vec<_> = self.bytes.iter().copied().take(MAX_FINAL_BYTES).collect();
        let (what, s) = if let Ok(s) = str::from_utf8(&xs[..]) {
            let ss = format!("'{s}'");
            ("string", replace_whitespace_chars(ss.as_str()))
        } else {
            ("bytestring", xs.iter().join(","))
        };
        let cont = if let Some(diff) = n
            .checked_sub(MAX_FINAL_BYTES)
            .and_then(|x| NonZeroUsize::try_from(x).ok())
        {
            format!(" ({diff} more)")
        } else {
            String::new()
        };
        write!(
            f,
            "{} TEXT does not end with delim; ends with {what} of length {n}: \
             {s}{cont}",
            self.kind
        )
    }
}

fn replace_whitespace_chars(s: &str) -> String {
    s.replace('\n', "\\n")
        .replace('\t', "\\t")
        .replace('\r', "\\r")
        .replace('\0', "\\0")
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
    "escaped delimiter encountered before unescaped delimiter and after {} in {} TEXT",
    truncate_string(self.bytes.as_latin1(21).as_str(), 20),
    self.kind
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct DelimBoundError {
    bytes: StringOrBytes,
    kind: TEXTKind,
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

/// Error when key or value with invalid UTF-8 characters is encountered
#[derive(Debug, Error)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct NonUtf8KeywordError {
    key: StringOrBytes,
    value: StringOrBytes,
}

impl fmt::Display for NonUtf8KeywordError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let n = 20;
        let go = |xs: &StringOrBytes| {
            let s = &xs.as_latin1(n + 1);
            truncate_string(s.as_str(), n)
        };
        write!(
            f,
            "non UTF-8 key/value pair encountered and dropped, \
             first {n} chars of both as Latin-1 are '{}' and '{}'",
            go(&self.key),
            go(&self.value),
        )
    }
}

#[allow(clippy::type_complexity)]
fn read_flat_text_inner<C>(
    p: &PathBuf,
    dataset_offset: DatasetOffset,
    conf: C,
) -> WarningsAndIOGroupResult<
    (FlatTEXTOutput, BufReader<fs::File>, ReadState<C>),
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
            FlatTEXTOutput::h_read(&mut h, &st).map_ok_value(|x| (x, h, st))
        })
}

fn h_read_dataset_from_kws<C, R>(
    h: &mut BufReader<R>,
    version: Version,
    kws: &StdKeywords,
    segs: &NonDataSegments,
    st: &ReadState<C>,
) -> WarningsAndIOGroupResult<
    FlatDatasetWithKwsOutput,
    LookupAndReadDataAnalysisWarning,
    LookupAndReadDataAnalysisError,
    (),
>
where
    R: Read + Seek,
    C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadOffsetConfig> + AsRef<ReadEventsConfig>,
{
    kws_to_df_analysis(version, h, kws, segs, st)
        .map_pure_errors(LookupAndReadDataAnalysisError::from)
        .and_then_commutative(|(data, analysis, dataset_segments, event_out)| {
            OthersReader::new(&segs.header.other[..])
                .h_read(h)
                .map(|others| {
                    FlatDatasetWithKwsOutput::new(
                        data,
                        analysis,
                        others,
                        dataset_segments,
                        event_out,
                    )
                })
                .map_err(IOErrorGroup::from)
                .into_log()
        })
}

impl FlatTEXTOutput {
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
                h_read_flat_text_from_header(h, header, st)
                    .map_commutative_warnings(HeaderOrFlatTEXTWarning::from)
                    .map_pure_errors(HeaderOrFlatTextError::from)
            })
    }

    fn into_std_text<C>(
        self,
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
        let segs = self.flat_diagnostics.non_data_segments();
        AnyCoreTEXT::parse_flat(self.version, self.keywords, &segs, st).map_ok_value(
            |(standardized, extra, offsets, scores)| {
                let out = StdTEXTOutput::new(
                    offsets.tot,
                    *offsets.as_ref(),
                    extra,
                    self.flat_diagnostics,
                    scores,
                );
                (standardized, out)
            },
        )
    }

    fn into_std_dataset<C, R>(
        self,
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
        let hs = self.flat_diagnostics.header_segments.clone();
        let d = hs.data;
        let a = hs.analysis;
        let o = hs.other;
        AnyCoreDataset::new_from_keywords(h, self.version, self.keywords, d, a, o, st).map_ok_value(
            |(core, out, scores)| {
                let dx = StdDatasetOutput::new(out, self.flat_diagnostics, scores);
                (core, dx)
            },
        )
    }
}

fn kws_to_df_analysis<C, R>(
    version: Version,
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
    C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadOffsetConfig> + AsRef<ReadEventsConfig>,
{
    match version {
        Version::FCS2_0 => Version2_0::h_lookup_and_read(h, kws, segs, st),
        Version::FCS3_0 => Version3_0::h_lookup_and_read(h, kws, segs, st),
        Version::FCS3_1 => Version3_1::h_lookup_and_read(h, kws, segs, st),
        Version::FCS3_2 => Version3_2::h_lookup_and_read(h, kws, segs, st),
    }
}

fn h_read_flat_text_from_header<C, R>(
    h: &mut BufReader<R>,
    header: Header,
    st: &ReadState<C>,
) -> WarningsAndIOGroupResult<FlatTEXTOutput, ParseFlatTEXTWarning, ParseFlatTEXTError, ()>
where
    R: Read + Seek,
    C: AsRef<ReadHeaderAndTEXTConfig> + AsRef<ReadOffsetConfig>,
{
    let conf = st.conf.as_ref();
    let mut buf = vec![];
    let ptext_seg = header.segments.text;

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
            split_flat_primary_text(&mut kws, delim, bytes, conf)
                .map_commutative_warnings(ParseFlatTEXTWarning::from)
                .map_errors(ParseFlatTEXTError::from)
                .group()
                .map_error(IOErrorGroup::Pure)
                .map_ok_value(|escaped| (kws, delim, escaped))
        })
        .and_then_commutative(|(mut kws, delim, prim_out)| {
            if conf.ignore_supp_text.is_set() {
                LogResult::new_ok((delim, kws, None, prim_out, None))
            } else {
                lookup_stext_offsets(&kws.std, &header, st)
                    .map_commutative_warnings(ParseFlatTEXTWarning::from)
                    .map_errors(ParseFlatTEXTError::from)
                    .group()
                    .map_error(IOErrorGroup::Pure)
                    .set_err_value(())
                    .and_then_commutative(|seg| {
                        buf.clear();
                        let corr_seg = seg.as_ref().map(|(c, _)| c);
                        h_read_flat_supp_text(h, corr_seg, &mut kws, &mut buf, delim, conf)
                            .map_commutative_warnings(ParseFlatTEXTWarning::from)
                            .map_pure_errors(ParseFlatTEXTError::from)
                            .map_ok_value(|supp_out| (delim, kws, seg, prim_out, supp_out))
                    })
            }
        })
        .and_then_commutative(|(delim, mut kws, supp_text_seg, prim_out, supp_out)| {
            let nextdata_res = lookup_nextdata(&kws.std, conf)
                .map_commutative_warnings(ParseFlatTEXTWarning::from)
                .map_errors(ParseFlatTEXTError::from)
                .into_semigroup();

            let repair_res = kws
                .append_std(&conf.append_standard_keywords, conf.allow_nonunique)
                .switchable_into_commutative()
                .map_commutative_warnings(ParseFlatTEXTWarning::from)
                .map_errors(ParseFlatTEXTError::from);

            let vkws = ValidKeywords::new(kws.std, kws.nonstd);

            let be_res = byte_errors(&kws.byte_pairs, conf)
                .map_commutative_warnings(ParseFlatTEXTWarning::from)
                .map_errors(ParseFlatTEXTError::from);

            nextdata_res
                .zip_f3_once(repair_res, be_res)
                .set_err_value(())
                .group()
                .map_error(IOErrorGroup::Pure)
                .map_ok_value(|(nextdata, (), ())| {
                    let parse = FlatTEXTDiagnostics {
                        header_segments: header.segments,
                        uncorrected_header_segments: header.uncorrected_segments,
                        supp_text: supp_text_seg,
                        nextdata,
                        delimiter: delim,
                        byte_pairs: kws.byte_pairs,
                        non_unique_std_keywords: kws.non_unique_std_keywords,
                        non_unique_nonstd_keywords: kws.non_unique_nonstd_keywords,
                        ignored_standard_keywords: kws.ignored_std_keywords,
                        keys_with_empty_trimmed_values: kws.keys_with_empty_trimmed_values,
                        keys_with_trimmed_values: kws.keys_with_trimmed_values,
                        primary_split: prim_out,
                        supp_split: supp_out,
                    };
                    FlatTEXTOutput::new(header.version, vkws, parse)
                })
        })
}

fn h_read_flat_supp_text<R: Read + Seek>(
    h: &mut BufReader<R>,
    maybe_seg: Option<&SupplementalTextSegment>,
    kws: &mut ParsedKeywords,
    buf: &mut Vec<u8>,
    delim: u8,
    conf: &ReadHeaderAndTEXTConfig,
) -> WarningsAndIOGroupResult<
    Option<SplitTEXTDiagnostics>,
    ParseSupplementalTEXTError,
    ParseSupplementalTEXTError,
    (),
> {
    if let Some(seg) = maybe_seg {
        io_to_log!(seg.h_read_contents(h, buf));
        split_flat_supp_text(kws, delim, buf, conf)
            .group()
            .map_error(IOErrorGroup::Pure)
    } else {
        LogResult::new_ok(None)
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

fn split_flat_primary_text(
    kws: &mut ParsedKeywords,
    delim: u8,
    bytes: &[u8],
    conf: &ReadHeaderAndTEXTConfig,
) -> WarningsAndErrorsResult<SplitTEXTDiagnostics, (), ParseKeywordsIssue, ParsePrimaryTEXTError> {
    if bytes.is_empty() {
        LogResult::new_err(NoTEXTWordsError.into())
    } else {
        split_flat_text_inner(kws, delim, bytes, TEXTKind::Primary, conf)
            .map_errors(ParsePrimaryTEXTError::from)
    }
}

fn split_flat_supp_text(
    kws: &mut ParsedKeywords,
    delim: u8,
    bytes: &[u8],
    conf: &ReadHeaderAndTEXTConfig,
) -> WarningsAndErrorsResult<
    Option<SplitTEXTDiagnostics>,
    (),
    ParseSupplementalTEXTError,
    ParseSupplementalTEXTError,
> {
    if let Some((byte0, rest)) = bytes.split_first() {
        let flag = conf.allow_supp_text_own_delim;
        split_flat_text_inner(kws, *byte0, rest, TEXTKind::Supplemental, conf)
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

fn split_flat_text_inner(
    kws: &mut ParsedKeywords,
    delim: u8,
    bytes: &[u8],
    tk: TEXTKind,
    conf: &ReadHeaderAndTEXTConfig,
) -> WarningsAndErrorsResult<SplitTEXTDiagnostics, (), ParseKeywordsIssue, ParseKeywordsIssue> {
    let (trimmed_bytes, remainder): (&[u8], &[u8]) = if conf.trim_text_end.is_set() {
        TrimTEXTData::with_bytes(delim, bytes)
    } else {
        (bytes, &[])
    };
    let escaped = GuessedEscapeMode::is_escaped(delim, trimmed_bytes, conf.delim_escape_mode);
    let res = if escaped {
        split_flat_text_escaped_delim(kws, delim, trimmed_bytes, tk, conf)
    } else {
        split_flat_text_unescaped_delim(kws, delim, trimmed_bytes, tk, conf)
    };
    res.map_ok_value(|inner| SplitTEXTDiagnostics {
        keys_with_blank_values: inner.keys_with_blank_values,
        values_with_blank_keys: inner.values_with_blank_keys,
        tokens_with_boundary_delims: inner.tokens_with_boundary_delims,
        last_odd_token: inner.last_odd_token,
        missing_final_delim: inner.missing_final_delim,
        trailing_whitespace_length: remainder.len(),
        escaped,
    })
}

#[derive(Debug, PartialEq)]
enum GuessedEscapeMode {
    Escaped,
    Unescaped,
    Ambiguous,
}

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

impl TrimTEXTData {
    fn with_bytes(delim: u8, bytes: &[u8]) -> (&[u8], &[u8]) {
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

    fn split_bytes<'a>(&self, bytes: &'a [u8]) -> (&'a [u8], &'a [u8]) {
        let n_trim = if self.has_final_double_delim() {
            self.trailing + 1
        } else {
            self.trailing
        };
        // Split the raw byte segment (or not if it is empty)
        if let Some(split_index) = bytes.len().checked_sub(n_trim) {
            bytes.split_at(split_index)
        } else {
            (bytes, &[])
        }
    }
}

fn split_flat_text_unescaped_delim(
    kws: &mut ParsedKeywords,
    delim: u8,
    bytes: &[u8],
    tk: TEXTKind,
    conf: &ReadHeaderAndTEXTConfig,
) -> WarningsAndErrorsResult<SplitTEXTOutputInner, (), ParseKeywordsIssue, ParseKeywordsIssue> {
    let mut keys_with_blank_values = vec![];
    let mut values_with_blank_keys = vec![];
    let mut insert_results = vec![];

    let mut it = bytes.split(|x| *x == delim).peekable();
    let mut prev_was_key = false;
    let mut prev_token: &[u8] = &[];

    while let Some(key) = it.next() {
        prev_was_key = true;
        prev_token = key;
        if key.is_empty() {
            if let Some(value) = it.next() {
                prev_was_key = false;
                prev_token = value;
                values_with_blank_keys.push(StringOrBytes::from(value.to_vec()));
            } else {
                // if everything is correct, we should exit here since the
                // last token will be the blank slice after the final delim
                break;
            }
        } else if let Some(value) = it.next() {
            prev_was_key = false;
            prev_token = value;
            if value.is_empty() {
                // If there is nothing after a blank value this actually means
                // that TEXT has an odd number of tokens and ends with a
                // delimiter, and the "value" is the blank after the last
                // delimiter
                if it.peek().is_some() {
                    keys_with_blank_values.push(StringOrBytes::from(key.to_vec()));
                }
            } else {
                let e = kws
                    .insert(key, value, conf)
                    .map_commutative_warnings(ParseKeywordsIssue::from)
                    .map_errors(ParseKeywordsIssue::from);
                insert_results.push(e);
            }
        } else {
            // exiting here means we found a key without a value and also didn't
            // end with a delim
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
    let final_delim_err = NonEmpty::from_slice(prev_token)
        .map(|bs| FinalDelimError::new(tk, bs))
        .map(ParseKeywordsIssue::from);
    let missing_final_delim = final_delim_err.is_some();
    let final_delim_res = LogResult::new_switchable_maybe3((), (), final_delim_err, delim_flag)
        .switchable_into_commutative();

    // TODO these should be ignore regardless
    let blank_key_errors = values_with_blank_keys
        .iter()
        .map(|k| BlankKeyError::new(tk, k.clone()));

    // TODO its a bit weird that only this error doesn't mention the TEXT from
    // which is came
    let blank_value_errors = keys_with_blank_values
        .iter()
        .map(|k| BlankValueError(k.clone()));

    let blank_key_res = SwitchableErrorsResult::new_switchable_iter3(
        (),
        (),
        blank_key_errors,
        conf.allow_empty_keys,
    )
    .map_switchable_errors(ParseKeywordsIssue::from)
    .switchable_into_commutative();

    let blank_val_succ = Success::new_non_switchable(())
        .set_warnings(blank_value_errors.collect::<Vec<_>>())
        .map_warnings(ParseKeywordsIssue::from);
    let blank_val_res = LogResult::Succ(blank_val_succ);

    let ret = SplitTEXTOutputInner {
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
        .chain([uneven_res, blank_key_res, blank_val_res, final_delim_res])
        .sequence_def_void()
        .set_ok_value(ret)
}

#[allow(clippy::too_many_lines)]
fn split_flat_text_escaped_delim(
    kws: &mut ParsedKeywords,
    delim: u8,
    bytes: &[u8],
    tk: TEXTKind,
    conf: &ReadHeaderAndTEXTConfig,
) -> WarningsAndErrorsResult<SplitTEXTOutputInner, (), ParseKeywordsIssue, ParseKeywordsIssue> {
    let mut insert_results = vec![];
    let mut tokens_with_boundary_delims = vec![];

    let mut push_pair = |kb: &Vec<_>, vb: &Vec<_>| {
        let e = kws
            .insert(kb, vb, conf)
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
                if !valuebuf.is_empty() {
                    push_pair(&keybuf, &valuebuf);
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
        push_delim(&mut keybuf, &mut valuebuf, consec_blanks);

        if consec_blanks & 1 == 0 {
            even_delim_err = Some(EvenFinalDelimError.into());
        } else {
            tokens_with_boundary_delims.push(StringOrBytes::from(seg));
        }
    }

    let (uneven_err, last_odd_token) = if valuebuf.is_empty() {
        (
            Some(UnevenTokensError(tk).into()),
            Some(keybuf.clone().into()),
        )
    } else {
        push_pair(&keybuf, &valuebuf);
        (None, None)
    };

    let uneven_res = LogResult::new_switchable_maybe3((), (), uneven_err, conf.allow_odd)
        .switchable_into_commutative();

    // NOTE this is the same flag used for when the delimiter is missing
    // entirely since this is the net result of escaping an even number of
    // delimiters
    let delim_flag = conf.allow_missing_final_delim;
    let even_delim_res = LogResult::new_switchable_maybe3((), (), even_delim_err, delim_flag)
        .switchable_into_commutative();
    let final_delim_err = NonEmpty::from_slice(lastbuf)
        .map(|bs| FinalDelimError::new(tk, bs))
        .map(ParseKeywordsIssue::from);
    let missing_final_delim = final_delim_err.is_some();
    let final_delim_res = LogResult::new_switchable_maybe3((), (), final_delim_err, delim_flag)
        .switchable_into_commutative();

    let bound_iter = tokens_with_boundary_delims
        .iter()
        .map(|token| DelimBoundError::new(token.clone(), tk).into());
    let boundary_res =
        LogResult::new_switchable_iter3((), (), bound_iter, conf.allow_delim_at_boundary)
            .switchable_into_commutative();

    let ret = SplitTEXTOutputInner {
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

fn lookup_stext_offsets<C>(
    kws: &StdKeywords,
    header: &Header,
    st: &ReadState<C>,
) -> DeferredWarningsAndErrors<
    Option<(SupplementalTextSegment, UncorrectedSegment)>,
    STextSegmentWarning,
    STextSegmentError,
>
where
    C: AsRef<ReadHeaderAndTEXTConfig> + AsRef<ReadOffsetConfig>,
{
    let conf: &ReadHeaderAndTEXTConfig = st.conf.as_ref();
    debug_assert!(
        !conf.ignore_supp_text.is_set(),
        "tried to get supp TEXT offsets when supp TEXT is ignored"
    );
    // At this point, we have not yet overridden the version since we have not
    // read STEXT and therefore might not have all keywords. This puts us in a
    // bit of an awkward spot in the case we wish to autodetect the version.
    // Primary TEXT by definition must have all required keywords, so we can use
    // $BEGIN/ENDDATA to test if the version is 3.0 or higher. Additionally, we
    // can use lack of $CYT to test if the version is less then 3.2, although in
    // practice this keyword is usually present despite it being optional
    // pre-3.2. This all likely doesn't matter much anyways since STEXT is
    // seldom used.
    let ver = match conf.version_override {
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
    let corr = conf.supp_text_correction;
    let res = match ver {
        Version::FCS2_0 => LogResult::new_ok(None),
        Version::FCS3_0 | Version::FCS3_1 => {
            let pair = SupplementalTextSegmentId::get_req_pair(kws);
            match SupplementalTextSegmentId::with_req_pair(pair, corr, st) {
                Ok(seg) => LogResult::new_ok(Some(seg)),
                Err((e0, e1)) => {
                    let flag = conf.allow_missing_supp_text;
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
                Err((e0, e1)) => {
                    let mut res = DeferredWarningsAndErrors::new_ok(None);
                    res.extend_commutative_warnings(once(e0).chain(e1));
                    res.map_commutative_warnings(STextSegmentWarning::from)
                }
            }
        }
    };
    res.and_then_deferred(|x| {
        x.map_or(LogResult::new_ok(None), |(seg, raw)| {
            let flag = conf.allow_overlapping_supp_text;
            header
                .segments
                .validate_text(&seg, conf.header.other_width)
                .set_ok_value(Some((seg, raw)))
                .set_err_value(None)
                .nowarn_into_switchable3(flag)
                .map_switchable_errors(STextSegmentError::from)
                .switchable_into_commutative()
                .map_commutative_warnings(STextSegmentWarning::from)
        })
    })
}

fn lookup_nextdata(
    kws: &StdKeywords,
    conf: &ReadHeaderAndTEXTConfig,
) -> DeferredWarningAndError<Option<u64>, OptKeyError<Nextdata>, ReqKeyError<Nextdata>> {
    let ret = match conf.allow_missing_nextdata.0 {
        TriFlag::True => LogResult::Succ(Nextdata::get_root_opt(kws).into_succ()),
        TriFlag::False => Nextdata::get_metaroot_req(kws)
            .map(Some)
            .into_log()
            .set_err_value(None),
        TriFlag::Silent => LogResult::new_ok(
            kws.get(&Nextdata::std())
                .and_then(|s| s.parse::<Nextdata>().ok()),
        ),
    };
    ret.map_deferred_value(|x| {
        x.map(|y| {
            let c = i128::from(conf.nextdata_correction);
            let z = i128::from(y.0).saturating_add(c);
            if z < 0 {
                0_u64
            } else {
                u64::try_from(z).unwrap_or(u64::MAX)
            }
        })
    })
}

fn byte_errors(
    byte_pairs: &BytesPairs,
    conf: &ReadHeaderAndTEXTConfig,
) -> DeferredWarningsAndErrors<(), NonUtf8KeywordError, NonUtf8KeywordError> {
    let es = byte_pairs
        .iter()
        .cloned()
        .map(|(key, value)| NonUtf8KeywordError { key, value });
    LogResult::new_switchable_iter3((), (), es, conf.allow_non_utf8).switchable_into_commutative()
}

impl FlatTEXTDiagnostics {
    fn non_data_segments(&self) -> NonDataSegments {
        let hs = self.header_segments.clone();
        NonDataSegments::new(hs, self.supp_text.as_ref().copied().map(|(c, _)| c))
    }
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
        let out = split_flat_text_escaped_delim(&mut kws, delim, bytes, TEXTKind::Primary, &conf);
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

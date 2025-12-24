//! Top-level functions for parsing FCS files
use crate::config::{
    AllowMissingFinalDelim, AllowMissingNextdata, ConfigFlag as _, DatasetOffset,
    DatasetOffsetError, ReadDataKeywordsConfig, ReadEventsConfig, ReadFlatDatasetConfig,
    ReadFlatDatasetFromKeywordsConfig, ReadFlatTEXTConfig, ReadHeaderAndTEXTConfig,
    ReadHeaderConfig, ReadHeaderInnerConfig, ReadSharedConfig, ReadState, ReadStdDatasetConfig,
    ReadStdKeywordsConfig, ReadStdTEXTConfig, TruncateOffsets,
};
use crate::core::{
    Analysis, AnyCoreDataset, AnyCoreTEXT, DatasetSegments, LookupAndReadDataAnalysisError,
    LookupAndReadDataAnalysisWarning, Others, OthersReader, PrivVersioned as _,
    StdDatasetFromFlatTEXTWarning, StdDatasetFromFlatTextError, StdDatasetWithKwsOutput,
    StdTEXTFromFlatTEXTError, StdTEXTFromFlatTEXTWarning,
};
use crate::header::{
    Header, HeaderError, HeaderSegments, HeaderValidationError, Version, Version2_0, Version3_0,
    Version3_1, Version3_2,
};
use crate::logging::{
    CommutativeResultIter as _, DeferredErrors, DeferredIter as _, DeferredWarningAndError,
    DeferredWarningsAndErrors, IOAnonErrorGroup, IOErrorGroup, IOGroupResult, LogResult,
    ResultExt as _, SuccessResultIter as _, SwitchableErrorResult, SwitchableErrorsResult,
    WarningAndErrorResult, WarningsAndErrorResult, WarningsAndErrorsResult,
    WarningsAndIOGroupResult, io_to_log, split_log,
};
use crate::macros::def_summary;
use crate::segment::{
    HeaderAnalysisSegment, HeaderDataSegment, KeyedOptSegment as _, KeyedReqSegment as _,
    NonDataSegments, OptSegmentError, OtherSegment20, PrimaryTextSegment, ReqSegmentError,
    SupplementalTextSegment, SupplementalTextSegmentId, TEXTCorrection,
};
use crate::text::keywords::{Beginstext, Endstext, ExtraStdKeywords, Nextdata, Tot};
use crate::text::lookup::{
    OptKeyError, OptMetarootKey as _, ReqKeyError, ReqMetarootKey as _, truncate_string,
};
use crate::validated::ascii_uint::UintSpacePad20;
use crate::validated::dataframe::FCSDataFrame;
use crate::validated::keys::{
    BlankValueError, BytesPairs, Key as _, KeywordInsertError, NonAsciiPairs, ParsedKeywords,
    StdKeywords, StdPresent, ValidKeywords,
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
) -> IOGroupResult<Header, ReadHeaderError, HeaderSummary> {
    let (st, file) = ReadState::open(path, dataset_offset, conf)
        .map_err(|e| e.fmap_once(ReadHeaderError::from))
        .map_err(IOAnonErrorGroup::from)
        .map_err(IOAnonErrorGroup::deanonymize)?;
    let mut reader = BufReader::new(file);
    Header::h_read(&mut reader, &st)
        .map_err(|e| e.fmap(ReadHeaderError::from))
        .map_err(IOErrorGroup::deanonymize)
}

/// Read HEADER and key/value pairs from TEXT in an FCS file at a given position
#[must_use]
pub fn fcs_read_flat_text(
    path: &PathBuf,
    dataset_offset: DatasetOffset,
    conf: &ReadFlatTEXTConfig,
) -> WarningsAndIOGroupResult<
    FlatTEXTOutput,
    ParseFlatTEXTWarning,
    HeaderOrFlatTextError,
    FlatTEXTSummary,
> {
    read_fcs_flat_text_inner(path, dataset_offset, conf)
        .map_ok_value(|(x, _, _)| x)
        .warnings_to_pure_errors(&conf.shared, HeaderOrFlatTextError::from)
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
    read_fcs_flat_text_inner(path, dataset_offset, conf)
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
        .warnings_to_pure_errors(&conf.shared, StdTEXTError::from)
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
    read_fcs_flat_text_inner(path, dataset_offset, conf)
        .map_pure_errors(FlatDatasetError::from)
        .map_commutative_warnings(FlatDatasetWarning::from)
        .and_then_commutative(|(flat, mut h, st)| {
            let segs = flat.parse.non_data_segments();
            h_read_dataset_from_kws(&mut h, flat.version, &flat.keywords.std, &segs, &st)
                .map_ok_value(|dataset| FlatDatasetOutput::new(flat, dataset))
                .map_commutative_warnings(FlatDatasetWarning::from)
                .map_pure_errors(FlatDatasetError::from)
        })
        .warnings_to_pure_errors(&conf.shared, FlatDatasetError::from)
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
    read_fcs_flat_text_inner(path, dataset_offset, conf)
        .map_commutative_warnings(StdDatasetWarning::from)
        .map_pure_errors(StdDatasetError::from)
        .and_then_commutative(|(flat, mut h, st)| {
            flat.into_std_dataset(&mut h, &st)
                .map_commutative_warnings(StdDatasetWarning::from)
                .map_pure_errors(StdDatasetError::from)
        })
        .warnings_to_pure_errors(&conf.shared, StdDatasetError::from)
        .deanonymize()
}

/// Read DATA/ANALYSIS in FCS file using provided keywords.
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn fcs_read_flat_dataset_with_keywords(
    path: &PathBuf,
    version: Version,
    std: &StdKeywords,
    data_seg: HeaderDataSegment,
    analysis_seg: HeaderAnalysisSegment,
    other_segs: &[OtherSegment20],
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
            let segs = NonDataSegments::new(
                PrimaryTextSegment::default(),
                data_seg,
                analysis_seg,
                other_segs,
                None,
            );
            let mut h = BufReader::new(file);
            h_read_dataset_from_kws(&mut h, version, std, &segs, &st)
        })
        .warnings_to_pure_errors(&conf.shared, LookupAndReadDataAnalysisError::from)
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
    ParseFlatTEXTWarning,
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
                .parse
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
        |ret| ret.1.parse.nextdata,
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
        |ret| ret.text.parse.nextdata,
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
        |ret| ret.1.parse.nextdata,
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
    W: From<ParseFlatTEXTWarning> + From<Wi>,
    C: AsRef<ReadHeaderAndTEXTConfig> + AsRef<ReadSharedConfig>,
    G: Copy,
{
    let mut dataset_offset = Some(DatasetOffset::default());
    let mut count = 0_usize;
    let mut results = vec![];
    let rconf = ReadFlatTEXTConfig {
        flat: AsRef::<ReadHeaderAndTEXTConfig>::as_ref(conf).clone(),
        shared: AsRef::<ReadSharedConfig>::as_ref(conf).clone(),
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
                    .parse
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
    pub parse: FlatTEXTParseData,
}

/// Output of parsing the TEXT segment and standardizing keywords.
#[derive(Clone, PartialEq, new)]
pub struct StdTEXTOutput {
    /// TEXT value for $TOT
    ///
    /// This should always be Some for 3.0+ and might be None for 2.0.
    pub tot: Option<Tot>,

    /// Segments for DATA and ANALYSIS
    pub dataset_segments: DatasetSegments,

    /// Keywords that start with '$' that are not part of the standard
    pub extra: ExtraStdKeywords,

    /// Miscellaneous data from parsing TEXT
    pub parse: FlatTEXTParseData,
}

/// Output of parsing one flat dataset (TEXT+DATA) from an FCS file.
#[derive(Clone, new, PartialEq)]
pub struct FlatDatasetOutput {
    /// Output from parsing HEADER+TEXT
    pub text: FlatTEXTOutput,

    /// Output from parsing DATA+ANALYSIS
    pub dataset: FlatDatasetWithKwsOutput,
}

/// Output of parsing one standardized dataset (TEXT+DATA) from an FCS file.
#[derive(Clone, new, PartialEq)]
pub struct StdDatasetOutput {
    /// Standardized data from one FCS dataset
    pub dataset: StdDatasetWithKwsOutput,

    /// Miscellaneous data from parsing TEXT
    pub parse: FlatTEXTParseData,
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
}

/// Data pertaining to parsing the TEXT segment.
#[derive(new, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct FlatTEXTParseData {
    /// Offsets read from HEADER
    pub header_segments: HeaderSegments<UintSpacePad20>,

    /// Supplemental TEXT offsets
    ///
    /// This is not needed downstream and included here for informational
    /// purposes. It will always be None for 2.0 which does not include this.
    pub supp_text: Option<SupplementalTextSegment>,

    /// NEXTDATA offset
    ///
    /// This will be copied as represented in TEXT. If it is 0, there is no next
    /// dataset, otherwise it points to the next dataset in the file.
    pub nextdata: Option<u64>,

    /// Delimiter used to parse TEXT.
    ///
    /// Included here for informational purposes.
    pub delimiter: u8,

    /// Keywords with a non-ASCII but still valid UTF-8 key.
    ///
    /// Non-ASCII keys are non-conforment but are included here in case the user
    /// wants to fix them or know they are present
    pub non_ascii: NonAsciiPairs,

    /// Keywords that could not be parsed.
    ///
    /// These have either a key or value or both that is not a UTF-8 string.
    /// Included here for debugging
    pub byte_pairs: BytesPairs,
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
    // TODO add data layout information
}

impl FlatDatasetOutput {
    fn summarize(self) -> DatasetSummary {
        DatasetSummary {
            version: self.text.version,
            text_len: self.text.parse.header_segments.text.len(),
            data_len: self.dataset.dataset_segments.data.len(),
            analysis_len: self.dataset.dataset_segments.analysis.len(),
            n_events: self.dataset.data.nrows(),
            n_measurements: self.dataset.data.ncols(),
            n_other: self.dataset.others.0.len(),
            others_len: self.dataset.others.0.iter().map(|x| x.0.len()).sum(),
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
    Flat(ParseFlatTEXTWarning),
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
    Flat(ParseFlatTEXTWarning),
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
    Flat(ParseFlatTEXTWarning),
    Read(LookupAndReadDataAnalysisWarning),
}

/// Warning when parsing TEXT+DATA in flat mode
#[derive(From, Display, Error, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum FlatDatasetError {
    Flat(HeaderOrFlatTextError),
    Read(LookupAndReadDataAnalysisError),
    Warn(FlatDatasetWarning),
}

/// Error when parsing HEADER or TEXT segments in flat mode
#[derive(From, Display, Error, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum HeaderOrFlatTextError {
    DatasetOffset(DatasetOffsetError),
    Header(HeaderError),
    FlatTEXT(ParseFlatTEXTError),
    Warn(ParseFlatTEXTWarning),
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
    Text(ParseFlatTEXTWarning), // for reading skipped datasets to get $NEXTDATA
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
    Flat(ParseFlatTEXTWarning), // for reading skipped datasets to get $NEXTDATA
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
    Flat(ParseFlatTEXTWarning), // for reading skipped datasets to get $NEXTDATA
    Std(StdDatasetWarning),
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
    NonAscii(NonAsciiKeyError),
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
    BlankValue(BlankValueError),
    Uneven(UnevenWordsError),
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
#[error("Primary TEXT has a delimiter and no words")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct NoTEXTWordsError;

/// Error when blank key is encountered in TEXT
#[derive(Debug, Error)]
#[error("encountered blank key in {0} TEXT, skipping key and its value")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct BlankKeyError(TEXTKind);

/// Error when number of words in TEXT is not even
#[derive(Debug, Error)]
#[error("{0} TEXT segment has uneven number of words")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct UnevenWordsError(TEXTKind);

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
            ("string", format!("'{s}'"))
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

/// Error when TEXT ends with even number of delimiters
///
/// This can only happen in escaped TEXT
#[derive(Debug, Error)]
#[error("Primary TEXT ends with an even number of delimiters and thus are all escaped")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct EvenFinalDelimError;

/// Error when delimiter is found at word boundary.
///
/// This can only happen in escaped TEXT
#[derive(Debug, Error)]
#[error("delimiter encountered at word boundary in Primary TEXT")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct DelimBoundError;

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

/// Error when non-ASCII key is encounter when parsing TEXT
#[derive(Debug, Clone, Error)]
#[error("non-ASCII key encountered and dropped: {0}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeyError))]
pub struct NonAsciiKeyError(String);

/// Error when key or value with invalid UTF-8 characters is encountered
#[derive(Debug, Error)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct NonUtf8KeywordError {
    key: Vec<u8>,
    value: Vec<u8>,
}

impl fmt::Display for NonUtf8KeywordError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let n = 20;
        let go = |xs: &Vec<u8>| {
            let s = xs
                .iter()
                .take(n + 1)
                .copied()
                .map(char::from)
                .collect::<String>();
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
fn read_fcs_flat_text_inner<C>(
    p: &PathBuf,
    dataset_offset: DatasetOffset,
    conf: C,
) -> WarningsAndIOGroupResult<
    (FlatTEXTOutput, BufReader<fs::File>, ReadState<C>),
    ParseFlatTEXTWarning,
    HeaderOrFlatTextError,
    (),
>
where
    C: AsRef<ReadHeaderAndTEXTConfig>
        + AsRef<ReadHeaderInnerConfig>
        + AsRef<TruncateOffsets>
        + AsRef<TEXTCorrection<SupplementalTextSegmentId>>,
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
    C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadEventsConfig>,
{
    kws_to_df_analysis(version, h, kws, segs, st)
        .map_pure_errors(LookupAndReadDataAnalysisError::from)
        .and_then_commutative(|(data, analysis, dataset_segments)| {
            OthersReader::new(segs.other)
                .h_read(h)
                .map(|others| {
                    FlatDatasetWithKwsOutput::new(data, analysis, others, dataset_segments)
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
        ParseFlatTEXTWarning,
        IOErrorGroup<HeaderOrFlatTextError, ()>,
    >
    where
        R: Read + Seek,
        C: AsRef<ReadHeaderAndTEXTConfig>
            + AsRef<ReadHeaderInnerConfig>
            + AsRef<TruncateOffsets>
            + AsRef<TEXTCorrection<SupplementalTextSegmentId>>,
    {
        Header::h_read(h, st)
            .into_log()
            .map_pure_errors(HeaderOrFlatTextError::from)
            .and_then_commutative(|mut header| {
                let conf: &ReadHeaderAndTEXTConfig = st.conf.as_ref();
                if let Some(v) = conf.version_override {
                    header.version = v;
                }
                h_read_flat_text_from_header(h, header, st)
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
        C: AsRef<ReadStdKeywordsConfig> + AsRef<ReadDataKeywordsConfig>,
    {
        let segs = self.parse.non_data_segments();
        AnyCoreTEXT::parse_flat(self.version, self.keywords, &segs, st).map_ok_value(
            |(standardized, extra, offsets)| {
                let out = StdTEXTOutput::new(offsets.tot, *offsets.as_ref(), extra, self.parse);
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
        C: AsRef<ReadStdKeywordsConfig> + AsRef<ReadDataKeywordsConfig> + AsRef<ReadEventsConfig>,
    {
        let hs = &self.parse.header_segments;
        let d = hs.data;
        let a = hs.analysis;
        let o = &hs.other[..];
        AnyCoreDataset::new_from_keywords(h, self.version, self.keywords, d, a, o, st)
            .map_ok_value(|(core, out)| (core, StdDatasetOutput::new(out, self.parse)))
    }
}

fn kws_to_df_analysis<C, R>(
    version: Version,
    h: &mut BufReader<R>,
    kws: &StdKeywords,
    segs: &NonDataSegments,
    st: &ReadState<C>,
) -> WarningsAndIOGroupResult<
    (FCSDataFrame, Analysis, DatasetSegments),
    LookupAndReadDataAnalysisWarning,
    LookupAndReadDataAnalysisError,
    (),
>
where
    R: Read + Seek,
    C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadEventsConfig>,
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
    C: AsRef<ReadHeaderAndTEXTConfig>
        + AsRef<TEXTCorrection<SupplementalTextSegmentId>>
        + AsRef<TruncateOffsets>,
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
                .map_ok_value(|()| (kws, delim))
        })
        .and_then_commutative(|(mut kws, delim)| {
            if conf.ignore_supp_text.is_set() {
                // NOTE rip out the STEXT keywords so they don't trigger a false
                // positive pseudostandard keyword error later
                let _ = kws.std.remove(&Beginstext::std());
                let _ = kws.std.remove(&Endstext::std());
                LogResult::new_ok((delim, kws, None))
            } else {
                lookup_stext_offsets(&kws.std, &header, st)
                    .map_commutative_warnings(ParseFlatTEXTWarning::from)
                    .map_errors(ParseFlatTEXTError::from)
                    .group()
                    .map_error(IOErrorGroup::Pure)
                    .set_err_value(())
                    .and_then_commutative(|seg| {
                        buf.clear();
                        h_read_flat_supp_text(h, seg.as_ref(), &mut kws, &mut buf, delim, conf)
                            .map_commutative_warnings(ParseFlatTEXTWarning::from)
                            .map_pure_errors(ParseFlatTEXTError::from)
                            .map_ok_value(|()| (delim, kws, seg))
                    })
            }
        })
        .and_then_commutative(|(delim, mut kws, supp_text_seg)| {
            let nextdata_res = lookup_nextdata(&kws.std, conf.allow_missing_nextdata)
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
                .map_ok_value(|(nextdata, ())| {
                    let parse = FlatTEXTParseData::new(
                        header.segments,
                        supp_text_seg,
                        nextdata,
                        delim,
                        kws.non_ascii,
                        kws.byte_pairs,
                    );
                    FlatTEXTOutput::new(header.version, vkws, parse)
                })
        })
        .and_then_commutative(|flat| {
            // TODO these can be done earlier
            let p = &flat.parse;
            let na = p
                .as_non_ascii_errors(conf)
                .map_errors(ParseFlatTEXTError::from);
            let be = p.as_byte_errors(conf).map_errors(ParseFlatTEXTError::from);
            [na, be]
                .into_iter()
                .mappend_commutative()
                .group()
                .map_errors(IOErrorGroup::Pure)
                .nowarn_into_warn()
                .map_ok_value(|_| flat)
        })
}

fn h_read_flat_supp_text<R: Read + Seek>(
    h: &mut BufReader<R>,
    maybe_seg: Option<&SupplementalTextSegment>,
    kws: &mut ParsedKeywords,
    buf: &mut Vec<u8>,
    delim: u8,
    conf: &ReadHeaderAndTEXTConfig,
) -> WarningsAndIOGroupResult<(), ParseSupplementalTEXTError, ParseSupplementalTEXTError, ()> {
    if let Some(seg) = maybe_seg {
        io_to_log!(seg.h_read_contents(h, buf));
        split_flat_supp_text(kws, delim, buf, conf)
            .group()
            .map_error(IOErrorGroup::Pure)
    } else {
        LogResult::new_ok(())
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
        SwitchableErrorResult::new_switchable_ok_if(is_ok, (*delim, rest), (), e, flag)
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
) -> DeferredWarningsAndErrors<(), ParseKeywordsIssue, ParsePrimaryTEXTError> {
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
) -> DeferredWarningsAndErrors<(), ParseSupplementalTEXTError, ParseSupplementalTEXTError> {
    if let Some((byte0, rest)) = bytes.split_first() {
        let flag = conf.allow_supp_text_own_delim;
        split_flat_text_inner(kws, *byte0, rest, TEXTKind::Supplemental, conf)
            .map_warnings_and_errors(ParseSupplementalTEXTError::from)
            .eval_deferred_warning_or_error(flag, |()| {
                (*byte0 != delim).then_some(DelimMismatch::new(delim, *byte0))
            })
    } else {
        // if empty do nothing, this is expected for most files
        LogResult::new_ok(())
    }
}

fn split_flat_text_inner(
    kws: &mut ParsedKeywords,
    delim: u8,
    bytes: &[u8],
    tk: TEXTKind,
    conf: &ReadHeaderAndTEXTConfig,
) -> WarningsAndErrorsResult<(), (), ParseKeywordsIssue, ParseKeywordsIssue> {
    if conf.use_literal_delims.is_set() {
        split_flat_text_literal_delim(kws, delim, bytes, tk, conf)
    } else {
        split_flat_text_escaped_delim(kws, delim, bytes, tk, conf)
    }
}

fn split_flat_text_literal_delim(
    kws: &mut ParsedKeywords,
    delim: u8,
    bytes: &[u8],
    tk: TEXTKind,
    conf: &ReadHeaderAndTEXTConfig,
) -> WarningsAndErrorsResult<(), (), ParseKeywordsIssue, ParseKeywordsIssue> {
    let mut blank_errors = vec![];
    let mut insert_results = vec![];

    let mut it = bytes.split(|x| *x == delim);
    let mut prev_was_key = false;
    let mut prev_word: &[u8] = &[];

    while let Some(key) = it.next() {
        prev_was_key = true;
        prev_word = key;
        if key.is_empty() {
            if let Some(value) = it.next() {
                prev_was_key = false;
                prev_word = value;
                blank_errors.push(BlankKeyError(tk).into());
            } else {
                // if everything is correct, we should exit here since the
                // last word will be the blank slice after the final delim
                break;
            }
        } else if let Some(value) = it.next() {
            prev_was_key = false;
            prev_word = value;
            if value.is_empty() {
                blank_errors.push(BlankValueError(key.to_vec()).into());
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

    // If last word is all spaces and we want to "trim" them, this will allow us
    // to bypass some errors below since these can be ignored
    let trim_trailing = prev_word
        .iter()
        .all(|c| char::is_ascii_whitespace(&char::from(*c)))
        && conf.trim_trailing_whitespace.is_set();

    // We should end on a blank, which corresponds to a (not valid) key. If this
    // is not the case, the number of words was not even.
    let uneven_ok = prev_was_key || trim_trailing;
    // Don't emit this error if we are trimming whitespace off the end, because
    // the "odd word" in that case is entirely whitespace and therefore can be
    // ignored
    let uneven_err = UnevenWordsError(tk).into();
    let uneven_res = LogResult::new_switchable_ok_if(uneven_ok, (), (), uneven_err, conf.allow_odd)
        .switchable_into_commutative();

    // If the last word was not a blank, we did not end on a delimiter.

    let delim_flag = conf.allow_missing_final_delim;
    // Don't emit this error if we are trimming whitespace off the end because
    // the thing immediately before the whitespace is a delimiter in this case
    let final_delim_res = (!trim_trailing)
        .then(|| check_final_delimiter(prev_word, tk, delim_flag).switchable_into_commutative());

    // TODO this includes blanks keys and blank values (which are different failure types)
    let blank_res = LogResult::new_switchable_iter((), (), blank_errors, conf.allow_empty)
        .switchable_into_commutative();

    // TODO this is one instance where it could be inefficient to chain together
    // lots of options, which are stack allocated but need to be converted to
    // singleton vectors (heap allocated) to turn each of the results into
    // a semigroup that can be concated. Two options a) tune the iterator so
    // it can consume options or b) use stack-vectors for warnings
    insert_results
        .into_iter()
        .map(LogResult::into_semigroup)
        .chain([uneven_res, blank_res])
        .chain(final_delim_res)
        .mappend_def_void()
}

fn split_flat_text_escaped_delim(
    kws: &mut ParsedKeywords,
    delim: u8,
    bytes: &[u8],
    tk: TEXTKind,
    conf: &ReadHeaderAndTEXTConfig,
) -> WarningsAndErrorsResult<(), (), ParseKeywordsIssue, ParseKeywordsIssue> {
    let mut insert_results = vec![];
    let mut boundary_errors = vec![];

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
                // Previous number of delimiters is odd, treat this as a word
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
                if consec_blanks > 0 {
                    // TODO should probably say which boundary
                    boundary_errors.push(DelimBoundError.into());
                }
            } else {
                // Previous consecutive delimiter sequence was even. Push n / 2
                // delimiters to whatever the current word is. Then push to
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

    // If last word is all spaces and we want to "trim" them, this will allow us
    // to bypass some errors below since these can be ignored
    let trim_trailing = lastbuf
        .iter()
        .all(|c| char::is_ascii_whitespace(&char::from(*c)))
        && conf.trim_trailing_whitespace.is_set();

    // If all went perfectly, we should have one consecutive blank at this point
    // since the space between the last delim and the end will show up as a
    // blank. The value of the last buffer should also be an empty slice.
    //
    // If we have 0 consecutive blanks, then there was no delim at the end,
    // which is an error. In this case the last buffer should be a non-empty
    // slice.
    //
    // If number of blanks is even and not 0, then the last word ended with one
    // or more escaped delimiters, but the TEXT didn't (2 errors, delim at
    // boundary and no delim ending TEXT). Note that here, blanks = number of
    // literal delimiters, whereas in the loop, this corresponded to blanks + 1
    // delimiters.
    //
    // If number of blanks is odd but not 1, the last word ended with one or
    // more escaped delimiters (error: on a boundary) and the TEXT ended with a
    // delimiter (not an error).

    let mut even_delim_err = None;

    if consec_blanks > 1 {
        boundary_errors.push(DelimBoundError.into());
        push_delim(&mut keybuf, &mut valuebuf, consec_blanks);

        if consec_blanks & 1 == 1 {
            even_delim_err = Some(EvenFinalDelimError.into());
        }
    }

    let uneven_err = if valuebuf.is_empty() {
        // Don't emit this error if we are trimming whitespace off the end,
        // because the "odd word" in that case is entirely whitespace and
        // therefore can be ignored
        (!trim_trailing).then_some(UnevenWordsError(tk).into())
    } else {
        push_pair(&keybuf, &valuebuf);
        None
    };

    let uneven_res = LogResult::new_switchable_maybe((), (), uneven_err, conf.allow_odd)
        .switchable_into_commutative();

    // NOTE this is the same flag used for when the delimiter is missing
    // entirely since this is the net result of escaping an even number of
    // delimiters
    let delim_flag = conf.allow_missing_final_delim;
    let even_delim_res = LogResult::new_switchable_maybe((), (), even_delim_err, delim_flag)
        .switchable_into_commutative();
    // Don't emit this error if we are trimming whitespace off the end because
    // the thing immediately before the whitespace is a delimiter in this case
    let final_delim_res = (!trim_trailing)
        .then(|| check_final_delimiter(lastbuf, tk, delim_flag).switchable_into_commutative());

    let boundary_res =
        LogResult::new_switchable_iter((), (), boundary_errors, conf.allow_delim_at_boundary)
            .switchable_into_commutative();

    insert_results
        .into_iter()
        .map(LogResult::into_semigroup)
        .chain([uneven_res, even_delim_res, boundary_res])
        .chain(final_delim_res)
        .mappend_def_void()
}

fn check_final_delimiter(
    buf: &[u8],
    tk: TEXTKind,
    flag: AllowMissingFinalDelim,
) -> SwitchableErrorsResult<(), (), AllowMissingFinalDelim, ParseKeywordsIssue> {
    let e = NonEmpty::from_slice(buf)
        .map(|bs| FinalDelimError::new(tk, bs))
        .map(ParseKeywordsIssue::from);
    LogResult::new_switchable_maybe((), (), e, flag)
}

fn lookup_stext_offsets<C>(
    kws: &StdKeywords,
    header: &Header,
    st: &ReadState<C>,
) -> DeferredWarningsAndErrors<
    Option<SupplementalTextSegment>,
    STextSegmentWarning,
    STextSegmentError,
>
where
    C: AsRef<TruncateOffsets>
        + AsRef<TEXTCorrection<SupplementalTextSegmentId>>
        + AsRef<ReadHeaderAndTEXTConfig>,
{
    let conf: &ReadHeaderAndTEXTConfig = st.conf.as_ref();
    // If this flag is set, pretend that supp TEXT does not exist at all. No
    // parsing, no errors, no testing for overlaps. Note that these keywords
    // will be removed during standardization so we don't need to worry about
    // triggering false positive pseudostandard errors later
    if conf.ignore_supp_text.is_set() {
        // TODO this check is redundant, use debug_assert to ensure the flag
        // is not set instead since this should be controlled at the caller level
        return LogResult::new_ok(None);
    }
    let res = match header.version {
        Version::FCS2_0 => LogResult::new_ok(None),
        Version::FCS3_0 | Version::FCS3_1 => {
            let pair = SupplementalTextSegmentId::get_req_pair(kws);
            match SupplementalTextSegmentId::with_req_pair(pair, st) {
                Ok(seg) => LogResult::new_ok(Some(seg)),
                Err((e0, e1)) => {
                    let flag = conf.allow_missing_supp_text;
                    SwitchableErrorsResult::new_deferred_switchable(None, e0, flag)
                        .extend_deferred_switchable_errors(e1)
                        .map_switchable_errors(STextSegmentError::from)
                        .switchable_into_commutative()
                        .map_commutative_warnings(STextSegmentWarning::from)
                }
            }
        }
        Version::FCS3_2 => {
            let pair = SupplementalTextSegmentId::get_opt_pair(kws);
            match SupplementalTextSegmentId::with_opt_pair(pair, st) {
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
        x.map_or(LogResult::new_ok(None), |seg| {
            let flag = conf.allow_overlapping_supp_text;
            header
                .segments
                .validate_text(&seg, conf.header.other_width)
                .nowarn_into_switchable(flag)
                .map_switchable_errors(STextSegmentError::from)
                .switchable_into_commutative()
                .map_commutative_warnings(STextSegmentWarning::from)
                .set_ok_value(Some(seg))
                .set_err_value(None)
        })
    })
}

fn lookup_nextdata(
    kws: &StdKeywords,
    flag: AllowMissingNextdata,
) -> DeferredWarningAndError<Option<u64>, OptKeyError<Nextdata>, ReqKeyError<Nextdata>> {
    let ret = if flag.is_set() {
        Nextdata::get_metaroot_req(kws)
            .map(Some)
            .into_log()
            .set_err_value(None)
    } else {
        LogResult::Succ(Nextdata::get_root_opt(kws).into_succ())
    };
    ret.map_deferred_value(|x| x.map(|y| u64::from(y.0)))
}

impl FlatTEXTParseData {
    fn as_non_ascii_errors(
        &self,
        conf: &ReadHeaderAndTEXTConfig,
    ) -> DeferredErrors<(), NonAsciiKeyError> {
        if conf.allow_non_ascii_keywords.is_set() {
            LogResult::new_ok(())
        } else {
            let es = self
                .non_ascii
                .iter()
                .map(|(k, _)| NonAsciiKeyError(k.clone()));
            LogResult::new_err_from_iter(es, ())
        }
    }

    fn as_byte_errors(
        &self,
        conf: &ReadHeaderAndTEXTConfig,
    ) -> DeferredErrors<(), NonUtf8KeywordError> {
        if conf.allow_non_utf8.is_set() {
            LogResult::new_ok(())
        } else {
            let es = self
                .byte_pairs
                .iter()
                .cloned()
                .map(|(key, value)| NonUtf8KeywordError { key, value });
            LogResult::new_err_from_iter(es, ())
        }
    }

    fn non_data_segments(&self) -> NonDataSegments<'_> {
        let hs = &self.header_segments;
        NonDataSegments::new(
            hs.text,
            hs.data,
            hs.analysis,
            &hs.other[..],
            self.supp_text.as_ref().copied(),
        )
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
}

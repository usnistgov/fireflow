use crate::config::{
    AllowMissingFinalDelim, ConfigFlag as _, HeaderConfigInner, ReadHeaderAndTEXTConfig,
    ReadHeaderConfig, ReadLayoutConfig, ReadRawDatasetConfig, ReadRawDatasetFromKeywordsConfig,
    ReadRawTEXTConfig, ReadState, ReadStdDatasetConfig, ReadStdDatasetFromKeywordsConfig,
    ReadStdTEXTConfig, ReadTEXTOffsetsConfig, ReaderConfig, StdTextReadConfig, TruncateOffsets,
};
use crate::core::{
    Analysis, AnyCoreDataset, AnyCoreTEXT, DatasetSegments, LookupAndReadDataAnalysisError,
    LookupAndReadDataAnalysisWarning, Others, OthersReader, StdDatasetFromRawError,
    StdDatasetFromRawWarning, StdDatasetWithKwsFailure, StdDatasetWithKwsOutput,
    StdTEXTFromRawError, StdTEXTFromRawWarning, Versioned as _,
};
use crate::data::{NewDataReaderError, NewDataReaderWarning, RawToLayoutError, RawToLayoutWarning};
use crate::header::{
    Header, HeaderError, HeaderSegments, HeaderValidationError, Version, Version2_0, Version3_0,
    Version3_1, Version3_2,
};
use crate::logging::{
    CmtResultIter as _, DeferredErrors, DeferredIter as _, DeferredWarningAndError,
    DeferredWarningsAndErrors, FungibleErrorResult, FungibleErrorsResult, IOSummaryResult,
    ImpureError, LogResult, ResultExt as _, WarningAndErrorResult, WarningsAndErrorsResult,
    WarningsAndIOSummaryResult,
};
use crate::macros::def_failure;
use crate::segment::{
    HeaderAnalysisSegment, HeaderDataSegment, KeyedOptSegment as _, KeyedReqSegment as _,
    OptSegmentError, OtherSegment20, PrimaryTextSegment, ReqSegmentError, SupplementalTextSegment,
    SupplementalTextSegmentId, TEXTCorrection,
};
use crate::text::keywords::{Beginstext, Endstext, Nextdata, Tot};
use crate::text::parser::{
    ExtraStdKeywords, OptKeyError, ReqKeyError, get_opt, get_req, truncate_string,
};
use crate::type_families::ApplyOnce as _;
use crate::validated::ascii_uint::UintSpacePad20;
use crate::validated::dataframe::FCSDataFrame;
use crate::validated::keys::{
    BlankValueError, BytesPairs, Key as _, KeywordInsertError, NonAsciiPairs, ParsedKeywords,
    StdKeywords, ValidKeywords,
};

use derive_more::{Display, From};
use derive_new::new;
use itertools::Itertools as _;
use nonempty::NonEmpty;
use thiserror::Error;

use std::fmt;
use std::fs;
use std::io::{BufReader, Read, Seek};
use std::iter::once;
use std::num::{NonZeroUsize, ParseIntError};
use std::path::PathBuf;

#[cfg(feature = "serde")]
use serde::Serialize;

/// Read HEADER from an FCS file.
pub fn fcs_read_header(
    p: &PathBuf,
    conf: &ReadHeaderConfig,
) -> IOSummaryResult<Header, HeaderError, HeaderFailure> {
    ReadState::open(p, conf)
        .map_err(ImpureError::IO)
        .into_log()
        .and_then_cmt(|(st, file)| {
            let mut reader = BufReader::new(file);
            Header::h_read(&mut reader, &st)
        })
        .summarize_errors()
        .resolve_nowarn()
}

/// Read HEADER and key/value pairs from TEXT in an FCS file.
#[must_use]
pub fn fcs_read_raw_text(
    p: &PathBuf,
    conf: &ReadRawTEXTConfig,
) -> WarningsAndIOSummaryResult<RawTEXTOutput, ParseRawTEXTWarning, HeaderOrRawError, RawTEXTFailure>
{
    read_fcs_raw_text_inner(p, conf)
        .map_ok_value(|(x, _, _)| x)
        .cmt_warnings_to_errors(&conf.shared, |w| ImpureError::Pure(w.into()))
        .summarize_errors()
}

/// Read HEADER and standardized TEXT from an FCS file.
#[must_use]
pub fn fcs_read_std_text(
    p: &PathBuf,
    conf: &ReadStdTEXTConfig,
) -> WarningsAndIOSummaryResult<
    (AnyCoreTEXT, StdTEXTOutput),
    StdTEXTWarning,
    StdTEXTError,
    StdTEXTFailure,
> {
    read_fcs_raw_text_inner(p, conf)
        .map_ok_value(|(x, _, st)| (x, st))
        .cmt_warnings_into()
        .map_errors(ImpureError::inner_into)
        .and_then_cmt(|(raw, st)| {
            raw.into_std_text(&st)
                .cmt_warnings_into()
                .map_errors(|e| ImpureError::Pure(e.into()))
        })
        .cmt_warnings_to_errors(&conf.shared, |w| ImpureError::Pure(StdTEXTError::from(w)))
        .summarize_errors()
}

/// Read dataset from FCS file using standardized TEXT.
#[must_use]
pub fn fcs_read_raw_dataset(
    p: &PathBuf,
    conf: &ReadRawDatasetConfig,
) -> WarningsAndIOSummaryResult<
    RawDatasetOutput,
    RawDatasetWarning,
    RawDatasetError,
    RawDatasetFailure,
> {
    read_fcs_raw_text_inner(p, conf)
        .cmt_warnings_into()
        .map_errors(ImpureError::inner_into)
        .and_then_cmt(|(raw, mut h, st)| {
            h_read_dataset_from_kws(
                &mut h,
                raw.version,
                &raw.keywords.std,
                raw.parse.header_segments.data,
                raw.parse.header_segments.analysis,
                &raw.parse.header_segments.other[..],
                &st,
            )
            .map_ok_value(|dataset| RawDatasetOutput::new(raw, dataset))
            .cmt_warnings_into()
            .map_errors(ImpureError::inner_into)
        })
        .cmt_warnings_to_errors(&conf.shared, |w| {
            ImpureError::Pure(RawDatasetError::from(w))
        })
        .summarize_errors()
}

/// Read dataset from FCS file using raw key/value pairs from TEXT.
#[must_use]
pub fn fcs_read_std_dataset(
    p: &PathBuf,
    conf: &ReadStdDatasetConfig,
) -> WarningsAndIOSummaryResult<
    (AnyCoreDataset, StdDatasetOutput),
    StdDatasetWarning,
    StdDatasetError,
    StdDatasetFailure,
> {
    read_fcs_raw_text_inner(p, conf)
        .cmt_warnings_into()
        .map_errors(ImpureError::inner_into)
        .and_then_cmt(|(raw, mut h, st)| {
            raw.into_std_dataset(&mut h, &st)
                .cmt_warnings_into()
                .map_errors(ImpureError::inner_into)
        })
        .cmt_warnings_to_errors(&conf.shared, |w| {
            ImpureError::Pure(StdDatasetError::from(w))
        })
        .summarize_errors()
}

/// Read DATA/ANALYSIS in FCS file using provided keywords.
#[must_use]
pub fn fcs_read_raw_dataset_with_keywords(
    p: &PathBuf,
    version: Version,
    std: &StdKeywords,
    data_seg: HeaderDataSegment,
    analysis_seg: HeaderAnalysisSegment,
    other_segs: &[OtherSegment20],
    conf: &ReadRawDatasetFromKeywordsConfig,
) -> WarningsAndIOSummaryResult<
    RawDatasetWithKwsOutput,
    LookupAndReadDataAnalysisWarning,
    LookupAndReadDataAnalysisError,
    RawDatasetWithKwsFailure,
> {
    ReadState::open(p, conf)
        .map_err(ImpureError::IO)
        .into_log()
        .and_then_cmt(|(st, file)| {
            let mut h = BufReader::new(file);
            h_read_dataset_from_kws(
                &mut h,
                version,
                std,
                data_seg,
                analysis_seg,
                other_segs,
                &st,
            )
        })
        .cmt_warnings_to_errors(&conf.shared, |w| {
            ImpureError::Pure(LookupAndReadDataAnalysisError::from(w))
        })
        .summarize_errors()
}

/// Read DATA/ANALYSIS in FCS file using provided keywords to be standardized.
#[must_use]
pub fn fcs_read_std_dataset_with_keywords(
    p: &PathBuf,
    version: Version,
    kws: ValidKeywords,
    data_seg: HeaderDataSegment,
    analysis_seg: HeaderAnalysisSegment,
    other_segs: &[OtherSegment20],
    conf: &ReadStdDatasetFromKeywordsConfig,
) -> WarningsAndIOSummaryResult<
    (AnyCoreDataset, StdDatasetWithKwsOutput),
    StdDatasetFromRawWarning,
    StdDatasetFromRawError,
    StdDatasetWithKwsFailure,
> {
    ReadState::open(p, conf)
        .map_err(ImpureError::IO)
        .into_log()
        .and_then_cmt(|(st, file)| {
            let mut h = BufReader::new(file);
            AnyCoreDataset::new_from_keywords(
                &mut h,
                version,
                kws,
                data_seg,
                analysis_seg,
                other_segs,
                &st,
            )
        })
        .cmt_warnings_to_errors(&conf.shared, |w| {
            ImpureError::Pure(StdDatasetFromRawError::from(w))
        })
        .summarize_errors()
}

/// Output from parsing the TEXT segment.
#[derive(Clone, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct RawTEXTOutput {
    /// FCS version
    pub version: Version,

    /// Keywords from TEXT
    pub keywords: ValidKeywords,

    /// Miscellaneous data from parsing TEXT
    pub parse: RawTEXTParseData,
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
    pub parse: RawTEXTParseData,
}

/// Output of parsing one raw dataset (TEXT+DATA) from an FCS file.
#[derive(Clone, new, PartialEq)]
pub struct RawDatasetOutput {
    /// Output from parsing HEADER+TEXT
    pub text: RawTEXTOutput,

    /// Output from parsing DATA+ANALYSIS
    pub dataset: RawDatasetWithKwsOutput,
}

/// Output of parsing one standardized dataset (TEXT+DATA) from an FCS file.
#[derive(Clone, new, PartialEq)]
pub struct StdDatasetOutput {
    /// Standardized data from one FCS dataset
    pub dataset: StdDatasetWithKwsOutput,

    /// Miscellaneous data from parsing TEXT
    pub parse: RawTEXTParseData,
}

/// Output of using keywords to read raw TEXT+DATA
#[derive(Clone, PartialEq, new)]
pub struct RawDatasetWithKwsOutput {
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
pub struct RawTEXTParseData {
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
    pub nextdata: Option<u32>,

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

#[derive(From, Display)]
pub enum StdTEXTWarning {
    Raw(ParseRawTEXTWarning),
    Std(StdTEXTFromRawWarning),
}

#[derive(From, Display)]
pub enum StdTEXTError {
    Raw(HeaderOrRawError),
    Std(StdTEXTFromRawError),
    Warn(StdTEXTWarning),
}

#[derive(From, Display)]
pub enum StdDatasetWarning {
    Raw(ParseRawTEXTWarning),
    Std(StdDatasetFromRawWarning),
}

#[derive(From, Display)]
pub enum StdDatasetError {
    Raw(HeaderOrRawError),
    Std(StdDatasetFromRawError),
    Warn(StdDatasetWarning),
}

#[derive(From, Display)]
pub enum RawDatasetWarning {
    Raw(ParseRawTEXTWarning),
    Read(LookupAndReadDataAnalysisWarning),
}

#[derive(From, Display)]
pub enum RawDatasetError {
    Raw(HeaderOrRawError),
    Read(LookupAndReadDataAnalysisError),
    Warn(RawDatasetWarning),
}

#[derive(From, Display)]
pub enum ParseRawTEXTWarning {
    Char(DelimCharError),
    Keywords(ParseKeywordsIssue),
    SuppOffsets(STextSegmentWarning),
    Nextdata(OptKeyError<ParseIntError>),
    Nonstandard(NonstandardError),
}

#[derive(From, Display)]
pub enum HeaderOrRawError {
    Header(HeaderError),
    RawTEXT(ParseRawTEXTError),
    Warn(ParseRawTEXTWarning),
}

#[derive(From, Display)]
pub enum RawToReaderError {
    Layout(RawToLayoutError),
    Reader(NewDataReaderError),
}

#[derive(From, Display)]
pub enum RawToReaderWarning {
    Layout(RawToLayoutWarning),
    Reader(NewDataReaderWarning),
}

#[derive(From, Display)]
pub enum STextSegmentError {
    ReqSegment(ReqSegmentError),
    Dup(DuplicatedSuppTEXT),
}

#[derive(From, Display)]
pub enum STextSegmentWarning {
    ReqSegment(ReqSegmentError),
    OptSegment(OptSegmentError),
    Dup(DuplicatedSuppTEXT),
}

#[derive(Debug, Error)]
#[error("primary and supplemental TEXT are duplicated")]
pub struct DuplicatedSuppTEXT;

#[derive(From, Display)]
pub enum ParseRawTEXTError {
    Delim(DelimVerifyError),
    Primary(ParsePrimaryTEXTError),
    Supplemental(ParseSupplementalTEXTError),
    SuppOffsets(STextSegmentError),
    Nextdata(ReqKeyError<ParseIntError>),
    NonAscii(NonAsciiKeyError),
    NonUtf8(NonUtf8KeywordError),
    Nonstandard(NonstandardError),
    Header(Box<HeaderValidationError>),
}

#[derive(From, Display)]
pub enum DelimVerifyError {
    Empty(EmptyTEXTError),
    Char(DelimCharError),
}

#[derive(Debug, Error)]
#[error("delimiter must be ASCII character 1-126 inclusive, got {0}")]
pub struct DelimCharError(u8);

#[derive(Debug, Error)]
#[error("Primary TEXT segment is empty")]
pub struct EmptyTEXTError;

#[derive(Debug, Error)]
#[error("Primary TEXT has a delimiter and no words")]
pub struct NoTEXTWordsError;

#[derive(Debug, Error)]
#[error("encountered blank key in {0} TEXT, skipping key and its value")]
pub struct BlankKeyError(TEXTKind);

#[derive(Debug, Error)]
#[error("{0} TEXT segment has uneven number of words")]
pub struct UnevenWordsError(TEXTKind);

#[derive(Debug, new)]
pub struct FinalDelimError {
    kind: TEXTKind,
    bytes: NonEmpty<u8>,
}

// this can only happen in escaped TEXT
#[derive(Debug, Error)]
#[error("Primary TEXT ends with an even number of delimiters and thus are all escaped")]
pub struct EvenFinalDelimError;

// this can only happen in escaped TEXT
#[derive(Debug, Error)]
#[error("delimiter encountered at word boundary in Primary TEXT")]
pub struct DelimBoundError;

#[derive(Clone, Copy, Debug, Display)]
pub enum TEXTKind {
    #[display("Primary")]
    Primary,
    #[display("Supplemental")]
    Supplemental,
}

#[derive(From, Display, Debug, Error)]
pub enum ParsePrimaryTEXTError {
    Keywords(ParseKeywordsIssue),
    Empty(NoTEXTWordsError),
}

#[derive(Display, From, Debug, Error)]
pub enum ParseKeywordsIssue {
    BlankKey(BlankKeyError),
    BlankValue(BlankValueError),
    Uneven(UnevenWordsError),
    Final(FinalDelimError),
    EvenFinal(EvenFinalDelimError),
    Insert(KeywordInsertError),
    Bound(DelimBoundError),
    // this is only for supp TEXT but seems less wasteful/convoluted to put here
    Mismatch(DelimMismatch),
}

#[derive(From, Display, Debug, Error)]
pub enum ParseSupplementalTEXTError {
    Keywords(ParseKeywordsIssue),
    Mismatch(DelimMismatch),
}

#[derive(Debug, Clone, Error, new)]
#[error(
    "first byte of supplemental TEXT ({supp}) does not match \
     delimiter of primary TEXT ({delim})"
)]
pub struct DelimMismatch {
    supp: u8,
    delim: u8,
}

#[derive(Debug, Clone, Error)]
#[error("non-ASCII key encountered and dropped: {0}")]
pub struct NonAsciiKeyError(String);

pub struct NonUtf8KeywordError {
    key: Vec<u8>,
    value: Vec<u8>,
}

#[derive(Debug, Clone, Error)]
#[error("nonstandard keywords detected")]
pub struct NonstandardError;

#[allow(clippy::type_complexity)]
fn read_fcs_raw_text_inner<C>(
    p: &PathBuf,
    conf: C,
) -> WarningsAndErrorsResult<
    (RawTEXTOutput, BufReader<fs::File>, ReadState<C>),
    (),
    ParseRawTEXTWarning,
    ImpureError<HeaderOrRawError>,
>
where
    C: AsRef<ReadHeaderAndTEXTConfig>
        + AsRef<HeaderConfigInner>
        + AsRef<TruncateOffsets>
        + AsRef<TEXTCorrection<SupplementalTextSegmentId>>,
{
    ReadState::open(p, conf)
        .map_err(ImpureError::IO)
        .into_log()
        .and_then_cmt(|(st, file)| {
            let mut h = BufReader::new(file);
            RawTEXTOutput::h_read(&mut h, &st).map_ok_value(|x| (x, h, st))
        })
}

fn h_read_dataset_from_kws<C, R>(
    h: &mut BufReader<R>,
    version: Version,
    kws: &StdKeywords,
    data_seg: HeaderDataSegment,
    analysis_seg: HeaderAnalysisSegment,
    other_segs: &[OtherSegment20],
    st: &ReadState<C>,
) -> WarningsAndErrorsResult<
    RawDatasetWithKwsOutput,
    (),
    LookupAndReadDataAnalysisWarning,
    ImpureError<LookupAndReadDataAnalysisError>,
>
where
    R: Read + Seek,
    C: AsRef<ReadLayoutConfig> + AsRef<ReaderConfig> + AsRef<ReadTEXTOffsetsConfig>,
{
    kws_to_df_analysis(version, h, kws, data_seg, analysis_seg, st)
        .map_errors(ImpureError::inner_into)
        .and_then_cmt(|(data, analysis, dataset_segments)| {
            OthersReader::new(other_segs)
                .h_read(h)
                .map_err(ImpureError::IO)
                .into_log()
                .map_ok_value(|others| {
                    RawDatasetWithKwsOutput::new(data, analysis, others, dataset_segments)
                })
        })
}

impl RawTEXTOutput {
    fn h_read<C, R>(
        h: &mut BufReader<R>,
        st: &ReadState<C>,
    ) -> WarningsAndErrorsResult<Self, (), ParseRawTEXTWarning, ImpureError<HeaderOrRawError>>
    where
        R: Read + Seek,
        C: AsRef<ReadHeaderAndTEXTConfig>
            + AsRef<HeaderConfigInner>
            + AsRef<TruncateOffsets>
            + AsRef<TEXTCorrection<SupplementalTextSegmentId>>,
    {
        Header::h_read(h, st)
            .nowarn_into_warn()
            .map_errors(ImpureError::inner_into)
            .and_then_cmt(|mut header| {
                let conf: &ReadHeaderAndTEXTConfig = st.conf.as_ref();
                if let Some(v) = conf.version_override {
                    header.version = v;
                }
                h_read_raw_text_from_header(h, header, st).map_errors(ImpureError::inner_into)
            })
    }

    fn into_std_text<C>(
        self,
        st: &ReadState<C>,
    ) -> WarningsAndErrorsResult<
        (AnyCoreTEXT, StdTEXTOutput),
        (),
        StdTEXTFromRawWarning,
        StdTEXTFromRawError,
    >
    where
        C: AsRef<StdTextReadConfig> + AsRef<ReadLayoutConfig> + AsRef<ReadTEXTOffsetsConfig>,
    {
        let header = &self.parse.header_segments;
        AnyCoreTEXT::parse_raw(
            self.version,
            self.keywords,
            header.data,
            header.analysis,
            st,
        )
        .map_ok_value(|(standardized, extra, offsets)| {
            let out = StdTEXTOutput::new(offsets.tot, *offsets.as_ref(), extra, self.parse);
            (standardized, out)
        })
    }

    fn into_std_dataset<C, R>(
        self,
        h: &mut BufReader<R>,
        st: &ReadState<C>,
    ) -> WarningsAndErrorsResult<
        (AnyCoreDataset, StdDatasetOutput),
        (),
        StdDatasetFromRawWarning,
        ImpureError<StdDatasetFromRawError>,
    >
    where
        R: Read + Seek,
        C: AsRef<StdTextReadConfig>
            + AsRef<ReadLayoutConfig>
            + AsRef<ReaderConfig>
            + AsRef<ReadTEXTOffsetsConfig>,
    {
        AnyCoreDataset::new_from_keywords(
            h,
            self.version,
            self.keywords,
            self.parse.header_segments.data,
            self.parse.header_segments.analysis,
            &self.parse.header_segments.other[..],
            st,
        )
        .map_ok_value(|(core, out)| (core, StdDatasetOutput::new(out, self.parse)))
    }
}

fn kws_to_df_analysis<C, R>(
    version: Version,
    h: &mut BufReader<R>,
    kws: &StdKeywords,
    data: HeaderDataSegment,
    analysis: HeaderAnalysisSegment,
    st: &ReadState<C>,
) -> WarningsAndErrorsResult<
    (FCSDataFrame, Analysis, DatasetSegments),
    (),
    LookupAndReadDataAnalysisWarning,
    ImpureError<LookupAndReadDataAnalysisError>,
>
where
    R: Read + Seek,
    C: AsRef<ReadLayoutConfig> + AsRef<ReaderConfig> + AsRef<ReadTEXTOffsetsConfig>,
{
    match version {
        Version::FCS2_0 => Version2_0::h_lookup_and_read(h, kws, data, analysis, st),
        Version::FCS3_0 => Version3_0::h_lookup_and_read(h, kws, data, analysis, st),
        Version::FCS3_1 => Version3_1::h_lookup_and_read(h, kws, data, analysis, st),
        Version::FCS3_2 => Version3_2::h_lookup_and_read(h, kws, data, analysis, st),
    }
}

fn h_read_raw_text_from_header<C, R>(
    h: &mut BufReader<R>,
    header: Header,
    st: &ReadState<C>,
) -> WarningsAndErrorsResult<RawTEXTOutput, (), ParseRawTEXTWarning, ImpureError<ParseRawTEXTError>>
where
    R: Read + Seek,
    C: AsRef<ReadHeaderAndTEXTConfig>
        + AsRef<TEXTCorrection<SupplementalTextSegmentId>>
        + AsRef<TruncateOffsets>,
{
    let conf = st.conf.as_ref();
    let mut buf = vec![];
    let ptext_seg = header.segments.text;

    let delim_res = ptext_seg
        .h_read_contents(h, &mut buf)
        .into_io_log()
        .and_then_cmt(|()| {
            // buffer is filled above by side effect, and this won't run if the
            // read step has an error
            split_first_delim(&buf, conf)
                .map_errors(|e| ImpureError::Pure(e.into()))
                .cmt_warnings_into()
                .repack()
        });

    delim_res
        .and_then_cmt(|(delim, bytes)| {
            let mut kws = ParsedKeywords::default();
            split_raw_primary_text(&mut kws, delim, bytes, conf)
                .map_commutative_warnings(ParseRawTEXTWarning::from)
                .map_errors(ParseRawTEXTError::from)
                .map_errors(ImpureError::Pure)
                .map_ok_value(|()| (kws, delim))
        })
        .and_then_cmt(|(mut kws, delim)| {
            if conf.ignore_supp_text.is_set() {
                // NOTE rip out the STEXT keywords so they don't trigger a false
                // positive pseudostandard keyword error later
                let _ = kws.std.remove(&Beginstext::std());
                let _ = kws.std.remove(&Endstext::std());
                LogResult::new_ok((delim, kws, None))
            } else {
                lookup_stext_offsets(&kws.std, header.version, ptext_seg, st)
                    .map_commutative_warnings(ParseRawTEXTWarning::from)
                    .map_errors(ParseRawTEXTError::from)
                    .map_errors(ImpureError::Pure)
                    .and_then_def(|seg| {
                        buf.clear();
                        h_read_raw_supp_text(h, seg.as_ref(), &mut kws, &mut buf, delim, conf)
                            .map_commutative_warnings(ParseRawTEXTWarning::from)
                            .map_errors(ImpureError::inner_into)
                            .map_ok_value(|()| (delim, kws, seg))
                    })
            }
        })
        .and_then_cmt(|(delim, mut kws, supp_text_seg)| {
            let nextdata_res = lookup_nextdata(&kws.std, conf.allow_missing_nextdata)
                .map_commutative_warnings(ParseRawTEXTWarning::from)
                .map_errors(ParseRawTEXTError::from)
                .map_errors(ImpureError::Pure)
                .repack();

            let repair_res = kws
                .append_std(&conf.append_standard_keywords, conf.allow_nonunique)
                .map_fungible_errors(KeywordInsertError::from)
                .map_fungible_errors(ParseKeywordsIssue::from)
                .fungible_into_commutative()
                .map_commutative_warnings(ParseRawTEXTWarning::from)
                .map_errors(ParsePrimaryTEXTError::from)
                .map_errors(ParseRawTEXTError::from)
                .map_errors(ImpureError::Pure);

            let vkws = ValidKeywords::new(kws.std, kws.nonstd);

            nextdata_res
                .zip_f2_once(repair_res)
                .set_err_value(())
                .map_ok_value(|(nextdata, ())| {
                    let parse = RawTEXTParseData::new(
                        header.segments,
                        supp_text_seg,
                        nextdata,
                        delim,
                        kws.non_ascii,
                        kws.byte_pairs,
                    );
                    RawTEXTOutput::new(header.version, vkws, parse)
                })
        })
        .and_then_cmt(|raw| {
            let p = &raw.parse;
            let na = p.as_non_ascii_errors(conf).errors_into();
            let be = p.as_byte_errors(conf).errors_into();
            let os = p.as_overlapping_segment_error().errors_into();
            [na, be, os]
                .into_iter()
                .mappend_cmt()
                .map_errors(ImpureError::Pure)
                .nowarn_into_warn()
                .map_ok_value(|_| raw)
        })
}

fn h_read_raw_supp_text<R: Read + Seek>(
    h: &mut BufReader<R>,
    maybe_seg: Option<&SupplementalTextSegment>,
    kws: &mut ParsedKeywords,
    buf: &mut Vec<u8>,
    delim: u8,
    conf: &ReadHeaderAndTEXTConfig,
) -> DeferredWarningsAndErrors<(), ParseKeywordsIssue, ImpureError<ParseSupplementalTEXTError>> {
    if let Some(seg) = maybe_seg {
        seg.h_read_contents(h, buf)
            .into_io_log()
            .and_then_cmt(|()| {
                // buffer is read above by side effect
                split_raw_supp_text(kws, delim, buf, conf)
                    .map_errors(ImpureError::Pure)
                    .cmt_warnings_into()
                    .map_errors(ImpureError::inner_into)
            })
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
        FungibleErrorResult::new_fungible_ok_if(is_ok, (*delim, rest), (), e, flag)
            .fungible_into_commutative()
            .map_errors(DelimVerifyError::from)
    } else {
        LogResult::new_err1(EmptyTEXTError.into())
    }
}

fn split_raw_primary_text(
    kws: &mut ParsedKeywords,
    delim: u8,
    bytes: &[u8],
    conf: &ReadHeaderAndTEXTConfig,
) -> DeferredWarningsAndErrors<(), ParseKeywordsIssue, ParsePrimaryTEXTError> {
    if bytes.is_empty() {
        LogResult::new_err1(NoTEXTWordsError.into())
    } else {
        split_raw_text_inner(kws, delim, bytes, TEXTKind::Primary, conf).errors_into()
    }
}

fn split_raw_supp_text(
    kws: &mut ParsedKeywords,
    delim: u8,
    bytes: &[u8],
    conf: &ReadHeaderAndTEXTConfig,
) -> DeferredWarningsAndErrors<(), ParseKeywordsIssue, ParseSupplementalTEXTError> {
    if let Some((byte0, rest)) = bytes.split_first() {
        let flag = conf.allow_supp_text_own_delim;
        split_raw_text_inner(kws, *byte0, rest, TEXTKind::Supplemental, conf)
            .eval_warning_or_error(flag, |()| {
                (*byte0 != delim).then_some(DelimMismatch::new(delim, *byte0))
            })
            .map_errors(ParseSupplementalTEXTError::from)
    } else {
        // if empty do nothing, this is expected for most files
        LogResult::new_ok(())
    }
}

// TODO this will fail early
fn split_raw_text_inner(
    kws: &mut ParsedKeywords,
    delim: u8,
    bytes: &[u8],
    tk: TEXTKind,
    conf: &ReadHeaderAndTEXTConfig,
) -> WarningsAndErrorsResult<(), (), ParseKeywordsIssue, ParseKeywordsIssue> {
    if conf.use_literal_delims {
        split_raw_text_literal_delim(kws, delim, bytes, tk, conf)
    } else {
        split_raw_text_escaped_delim(kws, delim, bytes, tk, conf)
    }
}

// TODO this will fail early
fn split_raw_text_literal_delim(
    kws: &mut ParsedKeywords,
    delim: u8,
    bytes: &[u8],
    tk: TEXTKind,
    conf: &ReadHeaderAndTEXTConfig,
) -> WarningsAndErrorsResult<(), (), ParseKeywordsIssue, ParseKeywordsIssue> {
    let mut blank_errors = vec![];
    let mut insert_results = vec![];

    // ASSUME input slice does not start with delim
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
                    .map_non_cmt_warnings(ParseKeywordsIssue::from)
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
    // is not the case, the number of words was not even.
    let uneven_err = UnevenWordsError(tk).into();
    let uneven_res =
        LogResult::new_fungible_ok_if(prev_was_key, (), (), uneven_err, conf.allow_odd)
            .fungible_into_commutative();

    // If the last word was not a blank, we did not end on a delimiter.

    let delim_flag = conf.allow_missing_final_delim;
    let final_delim_res =
        check_final_delimiter(prev_word, tk, delim_flag).fungible_into_commutative();

    let blank_res = LogResult::new_fungible_iter((), (), blank_errors, conf.allow_empty)
        .fungible_into_commutative();

    // TODO this is one instance where it could be inefficient to chain together
    // lots of options, which are stack allocated but need to be converted to
    // singleton vectors (heap allocated) to turn each of the results into
    // a semigroup that can be concated. Two options a) tune the iterator so
    // it can consume options or b) use stack-vectors for warnings
    insert_results
        .into_iter()
        .map(LogResult::non_cmt_into_cmt)
        .map(LogResult::into_semigroup)
        .chain([uneven_res, final_delim_res, blank_res])
        .mappend_def_void()
}

fn split_raw_text_escaped_delim(
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
            .map_non_cmt_warnings(ParseKeywordsIssue::from)
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

    // ASSUME input slice does not start with delim
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
        Some(UnevenWordsError(tk).into())
    } else {
        push_pair(&keybuf, &valuebuf);
        None
    };

    let uneven_res = LogResult::new_fungible_maybe((), (), uneven_err, conf.allow_odd)
        .fungible_into_commutative();

    // NOTE this is the same flag used for when the delimiter is missing
    // entirely since this is the net result of escaping an even number of
    // delimiters
    let delim_flag = conf.allow_missing_final_delim;
    let even_delim_res = LogResult::new_fungible_maybe((), (), even_delim_err, delim_flag)
        .fungible_into_commutative();
    let final_delim_res =
        check_final_delimiter(lastbuf, tk, delim_flag).fungible_into_commutative();

    let boundary_res =
        LogResult::new_fungible_iter((), (), boundary_errors, conf.allow_delim_at_boundary)
            .fungible_into_commutative();

    insert_results
        .into_iter()
        .map(LogResult::non_cmt_into_cmt)
        .map(LogResult::into_semigroup)
        .chain([uneven_res, final_delim_res, even_delim_res, boundary_res])
        .mappend_def_void()
}

fn check_final_delimiter(
    buf: &[u8],
    tk: TEXTKind,
    flag: AllowMissingFinalDelim,
) -> FungibleErrorsResult<(), (), AllowMissingFinalDelim, ParseKeywordsIssue> {
    let e = NonEmpty::from_slice(buf)
        .map(|bs| FinalDelimError::new(tk, bs))
        .map(ParseKeywordsIssue::from);
    LogResult::new_fungible_maybe((), (), e, flag)
}

fn lookup_stext_offsets<C>(
    kws: &StdKeywords,
    version: Version,
    text_segment: PrimaryTextSegment,
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
    let res = match version {
        Version::FCS2_0 => LogResult::new_ok(None),
        Version::FCS3_0 | Version::FCS3_1 => {
            let pair = SupplementalTextSegmentId::get_req_pair(kws);
            match SupplementalTextSegmentId::with_req_pair(pair, st) {
                Ok(seg) => LogResult::new_ok(Some(seg)),
                Err((e0, e1)) => {
                    let flag = conf.allow_missing_supp_text;
                    FungibleErrorsResult::new_deferred_fungible(None, e0, flag)
                        .extend_deferred_fungible_errors(e1.map(|x| *x))
                        .fungible_into_commutative()
                        .map_errors(STextSegmentError::from)
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
                    res.extend_commutative_warnings(once(e0).chain(e1.map(|x| *x)));
                    res.map_commutative_warnings(STextSegmentWarning::from)
                }
            }
        }
    };
    res.and_then_def(|x| {
        x.map_or(LogResult::new_ok(None), |seg| {
            // shouldn't this detect any overlap?
            if seg.same_coords(&text_segment) {
                let flag = conf.allow_duplicated_supp_text;
                // TODO why return None?
                FungibleErrorsResult::new_deferred_fungible(None, DuplicatedSuppTEXT, flag)
                    .fungible_into_commutative()
                    .map_errors(STextSegmentError::from)
                    .map_commutative_warnings(STextSegmentWarning::from)
            } else {
                LogResult::new_ok(Some(seg))
            }
        })
    })
}

// TODO the reason we use get instead of remove here is because we don't want to
// mess up the keyword list for raw mode, but in standardized mode we are
// consuming the hash table as a way to test for pseudostandard keywords (ie
// those that are left over). In order to reconcile these, we either need to
// make two raw text reader functions which either take immutable or mutable kws
// or use a more clever hash table that marks keys when we see them.
fn lookup_nextdata(
    kws: &StdKeywords,
    enforce: bool,
) -> DeferredWarningAndError<Option<u32>, OptKeyError<ParseIntError>, ReqKeyError<ParseIntError>> {
    let k = Nextdata::std();
    if enforce {
        get_req(kws, k)
            .into_log()
            .map_ok_value(Some)
            .map_err_value(|()| None)
    } else {
        get_opt(kws, k).into_succ()
    }
}

impl RawTEXTParseData {
    fn as_non_ascii_errors(
        &self,
        conf: &ReadHeaderAndTEXTConfig,
    ) -> DeferredErrors<(), NonAsciiKeyError> {
        if conf.allow_non_ascii_keywords {
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
        if conf.allow_non_utf8 {
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

    fn as_overlapping_segment_error(&self) -> DeferredErrors<(), ParseRawTEXTError> {
        if let Some(s) = self.supp_text {
            let x = self
                .header_segments
                .contains_text_segment(&s)
                .map_err(Into::into)
                .into_log();
            let y = self.header_segments.overlaps_with(&s).errors_into();
            x.lift_f2_once(y, |(), ()| ())
                .map_errors(|e| ParseRawTEXTError::from(Box::new(e)))
        } else {
            LogResult::new_ok(())
        }
    }
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

def_failure!(HeaderFailure, "could not parse HEADER");

def_failure!(RawTEXTFailure, "could not parse TEXT segment");

def_failure!(StdTEXTFailure, "could not standardize TEXT segment");

def_failure!(
    StdDatasetFailure,
    "could not read DATA with standardized TEXT"
);

def_failure!(RawDatasetFailure, "could not read DATA with raw TEXT");

def_failure!(
    RawDatasetWithKwsFailure,
    "could not read raw dataset from keywords"
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
        let out = split_raw_text_escaped_delim(&mut kws, delim, bytes, TEXTKind::Primary, &conf);
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

#[cfg(feature = "python")]
mod python {
    use crate::{
        core::{LookupAndReadDataAnalysisError, StdDatasetFromRawError},
        python::macros::impl_pyreflow_err,
    };

    use super::{HeaderOrRawError, RawDatasetError, StdDatasetError, StdTEXTError};

    impl_pyreflow_err!(HeaderOrRawError);
    impl_pyreflow_err!(StdTEXTError);
    impl_pyreflow_err!(RawDatasetError);
    impl_pyreflow_err!(StdDatasetError);
    impl_pyreflow_err!(LookupAndReadDataAnalysisError);
    impl_pyreflow_err!(StdDatasetFromRawError);
}

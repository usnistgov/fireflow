use crate::config::{
    HeaderConfigInner, ReadHeaderAndTEXTConfig, ReadHeaderConfig, ReadLayoutConfig,
    ReadRawDatasetConfig, ReadRawDatasetFromKeywordsConfig, ReadRawTEXTConfig, ReadState,
    ReadStdDatasetConfig, ReadStdDatasetFromKeywordsConfig, ReadStdTEXTConfig,
    ReadTEXTOffsetsConfig, ReaderConfig, StdTextReadConfig,
};
use crate::core::{
    Analysis, AnyCoreDataset, AnyCoreTEXT, DatasetSegments, LookupAndReadDataAnalysisError,
    LookupAndReadDataAnalysisWarning, Others, OthersReader, StdDatasetFromRawError,
    StdDatasetFromRawWarning, StdDatasetWithKwsFailure, StdDatasetWithKwsOutput,
    StdTEXTFromRawError, StdTEXTFromRawWarning, Versioned as _,
};
use crate::data::{NewDataReaderError, NewDataReaderWarning, RawToLayoutError, RawToLayoutWarning};
use crate::error::{
    DeferredExt as _, DeferredFailure, DeferredResult, IODeferredExt as _, IODeferredResult,
    IOTerminalResult, ImpureError, Leveled, MultiResultExt as _, NonEmptyFamily, PassthruExt as _,
    ResultExt as _, Tentative, TentativeInner, TerminalExt as _, VecFamily,
};
use crate::header::{
    Header, HeaderError, HeaderSegments, HeaderValidationError, Version, Version2_0, Version3_0,
    Version3_1, Version3_2,
};
use crate::macros::def_failure;
use crate::segment::{
    HeaderAnalysisSegment, HeaderDataSegment, KeyedOptSegment, KeyedReqSegment, NewSegmentConfig,
    OptSegmentError, OtherSegment20, PrimaryTextSegment, ReqSegmentError, SupplementalTextSegment,
};
use crate::text::keywords::{Beginstext, Endstext, Nextdata, Tot};
use crate::text::parser::{
    get_opt, get_req, truncate_string, ExtraStdKeywords, OptKeyError, ReqKeyError,
};
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

use std::convert::Infallible;
use std::fmt;
use std::fs;
use std::io::{BufReader, Read, Seek};
use std::num::{NonZeroUsize, ParseIntError};
use std::path;

#[cfg(feature = "serde")]
use serde::Serialize;

/// Read HEADER from an FCS file.
pub fn fcs_read_header(
    p: &path::PathBuf,
    conf: &ReadHeaderConfig,
) -> IOTerminalResult<Header, Infallible, HeaderError, HeaderFailure> {
    ReadState::open(p, conf)
        .into_deferred()
        .def_and_maybe(|(st, file)| {
            let mut reader = BufReader::new(file);
            Header::h_read(&mut reader, &st).mult_to_deferred()
        })
        .def_terminate_def()
}

/// Read HEADER and key/value pairs from TEXT in an FCS file.
pub fn fcs_read_raw_text(
    p: &path::PathBuf,
    conf: &ReadRawTEXTConfig,
) -> IOTerminalResult<RawTEXTOutput, ParseRawTEXTWarning, HeaderOrRawError, RawTEXTFailure> {
    read_fcs_raw_text_inner(p, conf)
        .def_map_value(|(x, _, _)| x)
        .def_terminate_maybe_warn(RawTEXTFailure, &conf.shared, |w| {
            ImpureError::Pure(w.into())
        })
}

/// Read HEADER and standardized TEXT from an FCS file.
pub fn fcs_read_std_text(
    p: &path::PathBuf,
    conf: &ReadStdTEXTConfig,
) -> IOTerminalResult<(AnyCoreTEXT, StdTEXTOutput), StdTEXTWarning, StdTEXTError, StdTEXTFailure> {
    read_fcs_raw_text_inner(p, conf)
        .def_map_value(|(x, _, st)| (x, st))
        .def_warnings_into()
        .def_io_into()
        .def_and_maybe(|(raw, st)| raw.into_std_text(&st).def_inner_into().def_errors_liftio())
        .def_terminate_maybe_warn_def(&conf.shared, |w| ImpureError::Pure(StdTEXTError::from(w)))
}

/// Read dataset from FCS file using standardized TEXT.
pub fn fcs_read_raw_dataset(
    p: &path::PathBuf,
    conf: &ReadRawDatasetConfig,
) -> IOTerminalResult<RawDatasetOutput, RawDatasetWarning, RawDatasetError, RawDatasetFailure> {
    read_fcs_raw_text_inner(p, conf)
        .def_warnings_into()
        .def_io_into()
        .def_and_maybe(|(raw, mut h, st)| {
            h_read_dataset_from_kws(
                &mut h,
                raw.version,
                &raw.keywords.std,
                raw.parse.header_segments.data,
                raw.parse.header_segments.analysis,
                &raw.parse.header_segments.other[..],
                &st,
            )
            .def_map_value(|dataset| RawDatasetOutput { text: raw, dataset })
            .def_warnings_into()
            .def_io_into()
        })
        .def_terminate_maybe_warn_def(&conf.shared, |w| {
            ImpureError::Pure(RawDatasetError::from(w))
        })
}

/// Read dataset from FCS file using raw key/value pairs from TEXT.
pub fn fcs_read_std_dataset(
    p: &path::PathBuf,
    conf: &ReadStdDatasetConfig,
) -> IOTerminalResult<
    (AnyCoreDataset, StdDatasetOutput),
    StdDatasetWarning,
    StdDatasetError,
    StdDatasetFailure,
> {
    read_fcs_raw_text_inner(p, conf)
        .def_warnings_into()
        .def_io_into()
        .def_and_maybe(|(raw, mut h, st)| {
            raw.into_std_dataset(&mut h, &st)
                .def_warnings_into()
                .def_io_into()
        })
        .def_terminate_maybe_warn_def(&conf.shared, |w| {
            ImpureError::Pure(StdDatasetError::from(w))
        })
}

/// Read DATA/ANALYSIS in FCS file using provided keywords.
pub fn fcs_read_raw_dataset_with_keywords(
    p: &path::PathBuf,
    version: Version,
    std: &StdKeywords,
    data_seg: HeaderDataSegment,
    analysis_seg: HeaderAnalysisSegment,
    other_segs: &[OtherSegment20],
    conf: &ReadRawDatasetFromKeywordsConfig,
) -> IOTerminalResult<
    RawDatasetWithKwsOutput,
    LookupAndReadDataAnalysisWarning,
    LookupAndReadDataAnalysisError,
    RawDatasetWithKwsFailure,
> {
    ReadState::open(p, conf)
        .into_deferred()
        .def_and_maybe(|(st, file)| {
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
        .def_terminate_maybe_warn_def(&conf.shared, |w| {
            ImpureError::Pure(LookupAndReadDataAnalysisError::from(w))
        })
}

/// Read DATA/ANALYSIS in FCS file using provided keywords to be standardized.
pub fn fcs_read_std_dataset_with_keywords(
    p: &path::PathBuf,
    version: Version,
    kws: ValidKeywords,
    data_seg: HeaderDataSegment,
    analysis_seg: HeaderAnalysisSegment,
    other_segs: &[OtherSegment20],
    conf: &ReadStdDatasetFromKeywordsConfig,
) -> IOTerminalResult<
    (AnyCoreDataset, StdDatasetWithKwsOutput),
    StdDatasetFromRawWarning,
    StdDatasetFromRawError,
    StdDatasetWithKwsFailure,
> {
    ReadState::open(p, conf)
        .into_deferred()
        .def_and_maybe(|(st, file)| {
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
        .def_terminate_maybe_warn_def(&conf.shared, |w| {
            ImpureError::Pure(StdDatasetFromRawError::from(w))
        })
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
#[derive(Clone, new, PartialEq)]
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
#[derive(Clone, new, PartialEq)]
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

#[derive(Debug)]
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

#[derive(Debug, Clone, Error)]
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
    p: &path::PathBuf,
    conf: C,
) -> DeferredResult<
    (RawTEXTOutput, BufReader<fs::File>, ReadState<C>),
    ParseRawTEXTWarning,
    ImpureError<HeaderOrRawError>,
>
where
    C: AsRef<ReadHeaderAndTEXTConfig> + AsRef<HeaderConfigInner>,
{
    ReadState::open(p, conf)
        .into_deferred()
        .def_and_maybe(|(st, file)| {
            let mut h = BufReader::new(file);
            RawTEXTOutput::h_read(&mut h, &st).def_map_value(|x| (x, h, st))
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
) -> IODeferredResult<
    RawDatasetWithKwsOutput,
    LookupAndReadDataAnalysisWarning,
    LookupAndReadDataAnalysisError,
>
where
    R: Read + Seek,
    C: AsRef<ReadLayoutConfig> + AsRef<ReaderConfig> + AsRef<ReadTEXTOffsetsConfig>,
{
    kws_to_df_analysis(version, h, kws, data_seg, analysis_seg, st)
        .def_inner_into()
        .def_and_maybe(|(data, analysis, dataset_segments)| {
            let or = OthersReader { segs: other_segs };
            or.h_read(h)
                .into_deferred()
                .def_map_value(|others| RawDatasetWithKwsOutput {
                    data,
                    analysis,
                    others,
                    dataset_segments,
                })
        })
}

impl RawTEXTOutput {
    fn h_read<C, R>(
        h: &mut BufReader<R>,
        st: &ReadState<C>,
    ) -> DeferredResult<Self, ParseRawTEXTWarning, ImpureError<HeaderOrRawError>>
    where
        R: Read + Seek,
        C: AsRef<ReadHeaderAndTEXTConfig> + AsRef<HeaderConfigInner>,
    {
        Header::h_read(h, st)
            .mult_to_deferred()
            .def_map_errors(|e: ImpureError<HeaderError>| e.inner_into())
            .def_and_maybe(|mut header| {
                let conf: &ReadHeaderAndTEXTConfig = st.conf.as_ref();
                if let Some(v) = conf.version_override {
                    header.version = v;
                }
                h_read_raw_text_from_header(h, header, st).def_map_errors(ImpureError::inner_into)
            })
    }

    fn into_std_text<C>(
        self,
        st: &ReadState<C>,
    ) -> DeferredResult<(AnyCoreTEXT, StdTEXTOutput), StdTEXTFromRawWarning, StdTEXTFromRawError>
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
        .def_map_value(|(standardized, extra, offsets)| {
            (
                standardized,
                StdTEXTOutput {
                    parse: self.parse,
                    tot: offsets.tot,
                    dataset_segments: *offsets.as_ref(),
                    extra,
                },
            )
        })
    }

    fn into_std_dataset<C, R>(
        self,
        h: &mut BufReader<R>,
        st: &ReadState<C>,
    ) -> DeferredResult<
        (AnyCoreDataset, StdDatasetOutput),
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
        .def_map_value(|(core, out)| (core, StdDatasetOutput::new(out, self.parse)))
    }
}

fn kws_to_df_analysis<C, R>(
    version: Version,
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
) -> IODeferredResult<RawTEXTOutput, ParseRawTEXTWarning, ParseRawTEXTError>
where
    R: Read + Seek,
    C: AsRef<ReadHeaderAndTEXTConfig>,
{
    let conf = st.conf.as_ref();
    let mut buf = vec![];
    let ptext_seg = header.segments.text;
    ptext_seg.h_read_contents(h, &mut buf).into_deferred()?;

    let tnt_delim = split_first_delim(&buf, conf)
        .def_inner_into()
        .def_errors_liftio()?;

    let kws_res = tnt_delim
        .and_maybe(|(delim, bytes)| {
            let kws = ParsedKeywords::default();
            split_raw_primary_text(kws, delim, bytes, conf)
                .def_inner_into()
                .def_errors_liftio()
                .def_map_value(|kws_| (delim, kws_))
        })
        .def_and_maybe(|(delim, mut kws)| {
            if conf.ignore_supp_text {
                // NOTE rip out the STEXT keywords so they don't trigger a false
                // positive pseudostandard keyword error later
                let _ = kws.std.remove(&Beginstext::std());
                let _ = kws.std.remove(&Endstext::std());
                Ok(Tentative::new1((delim, kws, None)))
            } else {
                lookup_stext_offsets(&kws.std, header.version, ptext_seg, st)
                    .inner_into()
                    .errors_liftio()
                    .and_maybe::<_, _, _, _, _, _, _, VecFamily, NonEmptyFamily, _, _>(
                        |maybe_supp_seg| {
                            let tnt_supp_kws = if let Some(seg) = maybe_supp_seg {
                                buf.clear();
                                seg.h_read_contents(h, &mut buf)?;
                                split_raw_supp_text(kws, delim, &buf, conf)
                                    .inner_into()
                                    .errors_liftio()
                            } else {
                                TentativeInner::new1(kws)
                            };
                            Ok(tnt_supp_kws.map(|k| (delim, k, maybe_supp_seg)))
                        },
                    )
            }
        });

    let repair_res = kws_res
        .def_and_tentatively::<_, _, VecFamily, VecFamily, VecFamily, VecFamily>(
            |(delim, mut kws, supp_text_seg)| {
                kws.append_std(&conf.append_standard_keywords, conf.allow_nonunique)
                    .map_or_else(
                        |es| {
                            Leveled::many_to_tentative(es.into())
                                .map_errors(KeywordInsertError::from)
                                .map_errors(ParseKeywordsIssue::from)
                                .map_errors(ParsePrimaryTEXTError::from)
                                .map_warnings(KeywordInsertError::from)
                                .map_warnings(ParseKeywordsIssue::from)
                                .inner_into()
                                .errors_liftio()
                        },
                        |()| Tentative::default(),
                    )
                    .map(|()| (delim, kws, supp_text_seg))
            },
        );

    repair_res.def_and_tentatively::<_, _, VecFamily, VecFamily, VecFamily, VecFamily>(
        |(delimiter, kws, supp_text_seg)| {
            let mut tnt_parse = lookup_nextdata(&kws.std, conf.allow_missing_nextdata)
                .errors_into::<ParseRawTEXTError>()
                .map(|nextdata| {
                    RawTEXTParseData::new(
                        header.segments,
                        supp_text_seg,
                        nextdata,
                        delimiter,
                        kws.non_ascii,
                        kws.byte_pairs,
                    )
                });

            tnt_parse.eval_errors(|v| v.as_non_ascii_errors(conf));
            tnt_parse.eval_errors(|v| v.as_byte_errors(conf));
            tnt_parse.eval_errors(RawTEXTParseData::as_overlapping_segment_error);

            let vkws = ValidKeywords::new(kws.std, kws.nonstd);

            tnt_parse
                .inner_into()
                .map(|parse| RawTEXTOutput::new(header.version, vkws, parse))
                .errors_liftio()
        },
    )
}

fn split_first_delim<'a>(
    bytes: &'a [u8],
    conf: &ReadHeaderAndTEXTConfig,
) -> DeferredResult<(u8, &'a [u8]), DelimCharError, DelimVerifyError> {
    if let Some((delim, rest)) = bytes.split_first() {
        let mut tnt = Tentative::new1((*delim, rest));
        if !(1..=126).contains(delim) {
            tnt.push_error_or_warning(DelimCharError(*delim), !conf.allow_non_ascii_delim);
        }
        Ok(tnt)
    } else {
        Err(DeferredFailure::new1(EmptyTEXTError))
    }
}

fn split_raw_primary_text(
    kws: ParsedKeywords,
    delim: u8,
    bytes: &[u8],
    conf: &ReadHeaderAndTEXTConfig,
) -> DeferredResult<ParsedKeywords, ParseKeywordsIssue, ParsePrimaryTEXTError> {
    if bytes.is_empty() {
        Err(DeferredFailure::new1(NoTEXTWordsError))
    } else {
        Ok(split_raw_text_inner(kws, delim, bytes, TEXTKind::Primary, conf).errors_into())
    }
}

fn split_raw_supp_text(
    kws: ParsedKeywords,
    delim: u8,
    bytes: &[u8],
    conf: &ReadHeaderAndTEXTConfig,
) -> Tentative<ParsedKeywords, ParseKeywordsIssue, ParseSupplementalTEXTError> {
    if let Some((byte0, rest)) = bytes.split_first() {
        let mut tnt =
            split_raw_text_inner(kws, *byte0, rest, TEXTKind::Supplemental, conf).errors_into();
        if *byte0 != delim {
            let x = DelimMismatch {
                delim,
                supp: *byte0,
            };
            if conf.allow_supp_text_own_delim {
                tnt.push_error(x);
            } else {
                tnt.push_warning(x);
            }
        }
        tnt
    } else {
        // if empty do nothing, this is expected for most files
        Tentative::new1(kws)
    }
}

fn split_raw_text_inner(
    kws: ParsedKeywords,
    delim: u8,
    bytes: &[u8],
    tk: TEXTKind,
    conf: &ReadHeaderAndTEXTConfig,
) -> Tentative<ParsedKeywords, ParseKeywordsIssue, ParseKeywordsIssue> {
    if conf.use_literal_delims {
        split_raw_text_literal_delim(kws, delim, bytes, tk, conf)
    } else {
        split_raw_text_escaped_delim(kws, delim, bytes, tk, conf)
    }
}

fn split_raw_text_literal_delim(
    mut kws: ParsedKeywords,
    delim: u8,
    bytes: &[u8],
    tk: TEXTKind,
    conf: &ReadHeaderAndTEXTConfig,
) -> Tentative<ParsedKeywords, ParseKeywordsIssue, ParseKeywordsIssue> {
    let mut errors = vec![];
    let mut warnings = vec![];

    let mut push_issue = |is_warning, error: ParseKeywordsIssue| {
        if is_warning {
            warnings.push(error);
        } else {
            errors.push(error);
        }
    };

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
                push_issue(conf.allow_empty, BlankKeyError(tk).into());
            } else {
                // if everything is correct, we should exit here since the
                // last word will be the blank slice after the final delim
                break;
            }
        } else if let Some(value) = it.next() {
            prev_was_key = false;
            prev_word = value;
            if value.is_empty() {
                push_issue(conf.allow_empty, BlankValueError(key.to_vec()).into());
            } else if let Err(lvl) = kws.insert(key, value, conf) {
                match lvl.inner_into() {
                    Leveled::Error(e) => push_issue(false, e),
                    Leveled::Warning(w) => push_issue(true, w),
                }
            }
        } else {
            // exiting here means we found a key without a value and also didn't
            // end with a delim
            break;
        }
    }

    if !prev_was_key {
        push_issue(conf.allow_odd, UnevenWordsError(tk).into());
    }

    if let Some(bs) = NonEmpty::from_slice(prev_word) {
        push_issue(
            conf.allow_missing_final_delim,
            FinalDelimError {
                kind: tk,
                bytes: bs,
            }
            .into(),
        );
    }

    Tentative::new_vec(kws, warnings, errors)
}

fn split_raw_text_escaped_delim(
    mut kws: ParsedKeywords,
    delim: u8,
    bytes: &[u8],
    tk: TEXTKind,
    conf: &ReadHeaderAndTEXTConfig,
) -> Tentative<ParsedKeywords, ParseKeywordsIssue, ParseKeywordsIssue> {
    let mut ews = (vec![], vec![]);

    let push_issue = |ews_: &mut (Vec<_>, Vec<_>), is_warning, error: ParseKeywordsIssue| {
        let warnings = &mut ews_.0;
        let errors = &mut ews_.1;
        if is_warning {
            warnings.push(error);
        } else {
            errors.push(error);
        }
    };

    let mut push_pair = |ews_: &mut (Vec<_>, Vec<_>), kb: &Vec<_>, vb: &Vec<_>| {
        if let Err(lvl) = kws.insert(kb, vb, conf) {
            match lvl.inner_into() {
                Leveled::Error(e) => push_issue(ews_, false, e),
                Leveled::Warning(w) => push_issue(ews_, true, w),
            }
        }
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
                    push_pair(&mut ews, &keybuf, &valuebuf);
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
                    push_issue(
                        &mut ews,
                        conf.allow_delim_at_boundary,
                        DelimBoundError.into(),
                    );
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

    if let Some(bs) = NonEmpty::from_slice(lastbuf) {
        push_issue(
            &mut ews,
            conf.allow_missing_final_delim,
            FinalDelimError {
                kind: tk,
                bytes: bs,
            }
            .into(),
        );
    }

    if consec_blanks > 1 {
        push_issue(
            &mut ews,
            conf.allow_delim_at_boundary,
            DelimBoundError.into(),
        );
        push_delim(&mut keybuf, &mut valuebuf, consec_blanks);

        if consec_blanks & 1 == 1 {
            push_issue(
                &mut ews,
                conf.allow_missing_final_delim,
                EvenFinalDelimError.into(),
            );
        }
    }

    if valuebuf.is_empty() {
        push_issue(&mut ews, conf.allow_odd, UnevenWordsError(tk).into());
    } else {
        push_pair(&mut ews, &keybuf, &valuebuf);
    }

    Tentative::new_vec(kws, ews.0, ews.1)
}

fn lookup_stext_offsets<C>(
    kws: &StdKeywords,
    version: Version,
    text_segment: PrimaryTextSegment,
    st: &ReadState<C>,
) -> Tentative<Option<SupplementalTextSegment>, STextSegmentWarning, STextSegmentError>
where
    C: AsRef<ReadHeaderAndTEXTConfig>,
{
    let conf = st.conf.as_ref();
    let seg_conf = NewSegmentConfig {
        corr: conf.supp_text_correction,
        file_len: Some(st.file_len.into()),
        truncate_offsets: conf.header.truncate_offsets,
    };
    match version {
        Version::FCS2_0 => Tentative::new1(None),
        Version::FCS3_0 | Version::FCS3_1 => KeyedReqSegment::get_mult(kws, &seg_conf).map_or_else(
            |es| Tentative::new_vec_either(None, es, !conf.allow_missing_supp_text),
            |t| Tentative::new1(Some(t)),
        ),
        Version::FCS3_2 => KeyedOptSegment::get(kws, &seg_conf).warnings_into(),
    }
    .and_tentatively(|x| {
        x.map_or(Tentative::default(), |seg| {
            if seg.same_coords(&text_segment) {
                Tentative::new_vec_either(
                    None,
                    vec![DuplicatedSuppTEXT],
                    !conf.allow_duplicated_supp_text,
                )
            } else {
                Tentative::new1(Some(seg))
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
) -> Tentative<Option<u32>, OptKeyError<ParseIntError>, ReqKeyError<ParseIntError>> {
    let k = Nextdata::std();
    if enforce {
        get_req(kws, k).into_tentative_err_opt()
    } else {
        get_opt(kws, k).into_tentative_warn_def()
    }
}

impl RawTEXTParseData {
    fn as_non_ascii_errors(&self, conf: &ReadHeaderAndTEXTConfig) -> Vec<NonAsciiKeyError> {
        if conf.allow_non_ascii_keywords {
            vec![]
        } else {
            self.non_ascii
                .iter()
                .map(|(k, _)| NonAsciiKeyError(k.clone()))
                .collect()
        }
    }

    fn as_byte_errors(&self, conf: &ReadHeaderAndTEXTConfig) -> Vec<NonUtf8KeywordError> {
        if conf.allow_non_utf8 {
            vec![]
        } else {
            self.byte_pairs
                .iter()
                .cloned()
                .map(|(key, value)| NonUtf8KeywordError { key, value })
                .collect()
        }
    }

    fn as_overlapping_segment_error(&self) -> Vec<ParseRawTEXTError> {
        if let Some(s) = self.supp_text {
            let x = self
                .header_segments
                .contains_text_segment(&s)
                .into_mult::<HeaderValidationError>();
            let y = self.header_segments.overlaps_with(&s).mult_errors_into();
            x.mult_zip(y)
                .mult_map_errors(|e| ParseRawTEXTError::from(Box::new(e)))
                .err()
                .map(Vec::from)
                .unwrap_or_default()
        } else {
            vec![]
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
        let kws = ParsedKeywords::default();
        let conf = ReadHeaderAndTEXTConfig::default();
        // NOTE should not start with delim
        let bytes = b"$P4F/700//75 BP/";
        let delim = 47;
        let out = split_raw_text_escaped_delim(kws, delim, bytes, TEXTKind::Primary, &conf);
        let v = out
            .value()
            .std
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .next()
            .unwrap();
        let es = out.errors();
        let ws = out.warnings();
        assert_eq!(("$P4F".into(), "700/75 BP".into()), v);
        assert!(es.is_empty(), "errors: {es:?}");
        assert!(ws.is_empty(), "warnings: {ws:?}");
    }
}

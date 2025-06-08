use crate::config::*;
use crate::core::*;
use crate::data::*;
use crate::error::*;
use crate::header::*;
use crate::macros::{enum_from, enum_from_disp, match_many_to_one};
use crate::segment::*;
use crate::text::keywords::*;
use crate::text::parser::*;
use crate::text::timestamps::*;
use crate::validated::dataframe::FCSDataFrame;
use crate::validated::standard::*;

use chrono::NaiveDate;
use itertools::Itertools;
use serde::Serialize;
use std::fmt;
use std::fs;
use std::io::{BufReader, Read, Seek};
use std::num::ParseIntError;
use std::path;

/// Read HEADER from an FCS file.
pub fn fcs_read_header(
    p: &path::PathBuf,
    conf: &HeaderConfig,
) -> IOTerminalResult<Header, (), HeaderError, HeaderFailure> {
    fs::File::options()
        .read(true)
        .open(p)
        .into_deferred()
        .def_and_maybe(|file| {
            let mut reader = BufReader::new(file);
            Header::h_read(&mut reader, conf).mult_to_deferred()
        })
        .def_terminate(HeaderFailure)
}

/// Read HEADER and key/value pairs from TEXT in an FCS file.
pub fn fcs_read_raw_text(
    p: &path::PathBuf,
    conf: &RawTextReadConfig,
) -> IOTerminalResult<RawTEXTOutput, ParseRawTEXTWarning, HeaderOrRawError, RawTEXTFailure> {
    read_fcs_raw_text_inner(p, conf)
        .def_map_value(|(x, _)| x)
        .def_terminate(RawTEXTFailure)
}

/// Read HEADER and standardized TEXT from an FCS file.
pub fn fcs_read_std_text(
    p: &path::PathBuf,
    conf: &StdTextReadConfig,
) -> IOTerminalResult<StdTEXTOutput, StdTEXTWarning, StdTEXTError, StdTEXTFailure> {
    read_fcs_raw_text_inner(p, &conf.raw)
        .def_map_value(|(x, _)| x)
        .def_io_into()
        .def_and_maybe(|raw| raw.into_std_text(conf).def_inner_into().def_errors_liftio())
        .def_terminate(StdTEXTFailure)
}

/// Read dataset from FCS file using standardized TEXT.
pub fn fcs_read_raw_dataset(
    p: &path::PathBuf,
    conf: &DataReadConfig,
) -> IOTerminalResult<RawDatasetOutput, RawDatasetWarning, RawDatasetError, RawDatasetFailure> {
    read_fcs_raw_text_inner(p, &conf.standard.raw)
        .def_io_into()
        .def_and_maybe(|(raw, mut h)| {
            h_read_dataset_from_kws(
                &mut h,
                raw.version,
                &raw.keywords.std,
                raw.parse.header_segments.data,
                raw.parse.header_segments.analysis,
                &raw.parse.header_segments.other[..],
                conf,
            )
            .def_map_value(|dataset| RawDatasetOutput { text: raw, dataset })
            .def_io_into()
        })
        .def_terminate(RawDatasetFailure)
}

/// Read dataset from FCS file using raw key/value pairs from TEXT.
pub fn fcs_read_std_dataset(
    p: &path::PathBuf,
    conf: &DataReadConfig,
) -> IOTerminalResult<StdDatasetOutput, StdDatasetWarning, StdDatasetError, StdDatasetFailure> {
    read_fcs_raw_text_inner(p, &conf.standard.raw)
        .def_io_into()
        .def_and_maybe(|(raw, mut h)| raw.into_std_dataset(&mut h, conf).def_io_into())
        .def_terminate(StdDatasetFailure)
}

/// Read DATA/ANALYSIS in FCS file using provided keywords.
pub fn fcs_read_raw_dataset_with_keywords(
    p: path::PathBuf,
    version: Version,
    std: &StdKeywords,
    data_seg: HeaderDataSegment,
    analysis_seg: HeaderAnalysisSegment,
    other_segs: Vec<OtherSegment>,
    conf: &DataReadConfig,
) -> IOTerminalResult<
    RawDatasetWithKwsOutput,
    ReadRawDatasetWarning,
    DatasetWithKwsError,
    RawDatasetWithKwsFailure,
> {
    fs::File::options()
        .read(true)
        .open(p)
        .into_deferred()
        .def_and_maybe(|file| {
            let mut h = BufReader::new(file);
            h_read_dataset_from_kws(
                &mut h,
                version,
                std,
                data_seg,
                analysis_seg,
                &other_segs[..],
                conf,
            )
        })
        .def_terminate(RawDatasetWithKwsFailure)
}

/// Read DATA/ANALYSIS in FCS file using provided keywords to be standardized.
pub fn fcs_read_std_dataset_with_keywords(
    p: &path::PathBuf,
    version: Version,
    mut kws: ValidKeywords,
    data_seg: HeaderDataSegment,
    analysis_seg: HeaderAnalysisSegment,
    other_segs: Vec<OtherSegment>,
    conf: &DataReadConfig,
) -> IOTerminalResult<
    StdDatasetWithKwsOutput,
    StdDatasetFromRawWarning,
    StdDatasetFromRawError,
    StdDatasetWithKwsFailure,
> {
    fs::File::options()
        .read(true)
        .open(p)
        .into_deferred()
        .def_and_maybe(|file| {
            let mut h = BufReader::new(file);
            AnyCoreDataset::parse_raw(
                &mut h,
                version,
                &mut kws.std,
                kws.nonstd,
                data_seg,
                analysis_seg,
                &other_segs[..],
                conf,
            )
            .def_map_value(|(core, d_seg, a_seg)| StdDatasetWithKwsOutput {
                standardized: DatasetWithSegments {
                    core,
                    data_seg: d_seg,
                    analysis_seg: a_seg,
                },
                deviant: kws.std,
            })
        })
        .def_terminate(StdDatasetWithKwsFailure)
}

/// Output from parsing the TEXT segment.
#[derive(Serialize)]
pub struct RawTEXTOutput {
    /// FCS version
    pub version: Version,

    /// Keywords from TEXT
    pub keywords: ValidKeywords,

    /// Miscellaneous data from parsing TEXT
    pub parse: RawTEXTParseData,
}

/// Output of parsing the TEXT segment and standardizing keywords.
pub struct StdTEXTOutput {
    /// Standardized data from TEXT
    pub standardized: AnyCoreTEXT,

    /// TEXT value for $TOT
    pub tot: Option<String>,

    /// TEXT value for $TIMESTEP if a time channel was not found (3.0+)
    pub timestep: Option<String>,

    /// TEXT values for $BEGIN/ENDDATA
    pub data: SegmentKeywords,

    /// TEXT values for $BEGIN/ENDANALYSIS
    pub analysis: SegmentKeywords,

    /// Keywords that start with '$' that are not part of the standard
    pub deviant: StdKeywords,

    /// Miscellaneous data from parsing TEXT
    pub parse: RawTEXTParseData,
}

/// Output of parsing one raw dataset (TEXT+DATA) from an FCS file.
pub struct RawDatasetOutput {
    /// Output from parsing HEADER+TEXT
    pub text: RawTEXTOutput,

    /// Output from parsing DATA+ANALYSIS
    pub dataset: RawDatasetWithKwsOutput,
}

/// Output of parsing one standardized dataset (TEXT+DATA) from an FCS file.
pub struct StdDatasetOutput {
    /// Standardized data from one FCS dataset
    pub dataset: StdDatasetWithKwsOutput,

    /// Miscellaneous data from parsing TEXT
    pub parse: RawTEXTParseData,
}

/// Output of using keywords to read standardized TEXT+DATA
pub struct StdDatasetWithKwsOutput {
    /// DATA+ANALYSIS
    pub standardized: DatasetWithSegments,

    /// Keywords that start with '$' that are not part of the standard
    pub deviant: StdKeywords,
}

/// Output of using keywords to read raw TEXT+DATA
pub struct RawDatasetWithKwsOutput {
    /// DATA output
    pub data: FCSDataFrame,

    /// ANALYSIS output
    pub analysis: Analysis,

    /// OTHER output(s)
    pub others: Others,

    /// offsets used to parse DATA
    pub data_seg: AnyDataSegment,

    /// offsets used to parse ANALYSIS
    pub analysis_seg: AnyAnalysisSegment,
}

/// Data pertaining to parsing the TEXT segment.
#[derive(Clone, Serialize)]
pub struct RawTEXTParseData {
    /// Offsets read from HEADER
    pub header_segments: HeaderSegments,

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

/// Raw TEXT values for $BEGIN/END* keywords
pub struct SegmentKeywords {
    pub begin: Option<String>,
    pub end: Option<String>,
}

/// Standardized TEXT+DATA+ANALYSIS with DATA+ANALYSIS offsets
pub struct DatasetWithSegments {
    /// Standardized dataset
    pub core: AnyCoreDataset,

    /// offsets used to parse DATA
    pub data_seg: AnyDataSegment,

    /// offsets used to parse ANALYSIS
    pub analysis_seg: AnyAnalysisSegment,
}

pub struct HeaderFailure;

pub struct RawTEXTFailure;

pub struct RawDatasetFailure;

pub struct RawDatasetWithKwsFailure;

pub struct StdTEXTFailure;

pub struct StdDatasetFailure;

pub struct StdDatasetWithKwsFailure;

enum_from_disp!(
    pub StdTEXTWarning,
    [Raw, ParseRawTEXTWarning],
    [Std, LookupMeasWarning]
);

enum_from_disp!(
    pub StdTEXTError,
    [Raw, HeaderOrRawError],
    [Std, LookupKeysError]
);

enum_from_disp!(
    pub StdDatasetWarning,
    [Raw, ParseRawTEXTWarning],
    [Std, StdDatasetFromRawWarning]
);

enum_from_disp!(
    pub StdDatasetError,
    [Raw, HeaderOrRawError],
    [Std, StdDatasetFromRawError]
);

enum_from_disp!(
    pub RawDatasetWarning,
    [Raw, ParseRawTEXTWarning],
    [Std, LookupMeasWarning],
    [Read, ReadRawDatasetWarning]
);

enum_from_disp!(
    pub RawDatasetError,
    [Raw, HeaderOrRawError],
    [Std, LookupKeysError],
    [Read, DatasetWithKwsError]
);

enum_from_disp!(
    pub ParseRawTEXTWarning,
    [Char, DelimCharError],
    [Keywords, ParseKeywordsIssue],
    [SuppOffsets, STextSegmentWarning],
    [Nextdata, ParseKeyError<ParseIntError>],
    [Nonstandard, NonstandardError]

);

enum_from_disp!(
    pub HeaderOrRawError,
    [Header, HeaderError],
    [RawTEXT, ParseRawTEXTError]
);

enum_from_disp!(
    pub DatasetWithKwsError,
    [DataReader, RawToReaderError],
    [AnalysisReader, NewAnalysisReaderError],
    [Read, ReadDataError]
);

enum_from_disp!(
    pub ReadRawDatasetWarning,
    [DataReader, RawToReaderWarning],
    [AnalysisReader, NewAnalysisReaderWarning]
);

enum_from_disp!(
    pub RawToReaderError,
    [Layout, RawToLayoutError],
    [Reader, NewDataReaderError]
);

enum_from_disp!(
    pub RawToReaderWarning,
    [Layout, RawToLayoutWarning],
    [Reader, NewDataReaderWarning]
);

enum_from_disp!(
    pub STextSegmentWarning,
    [ReqSegment, ReqSegmentError],
    [OptSegment, OptSegmentError]
);

enum_from_disp!(
    pub ParseRawTEXTError,
    [Delim, DelimVerifyError],
    [Primary, ParsePrimaryTEXTError],
    [Supplemental, ParseSupplementalTEXTError],
    [SuppOffsets, ReqSegmentError],
    [Nextdata, ReqKeyError<ParseIntError>],
    [NonAscii, NonAsciiKeyError],
    [NonUtf8, NonUtf8KeywordError],
    [Nonstandard, NonstandardError],
    [Header, Box<HeaderValidationError>]
);

enum_from_disp!(
    pub DelimVerifyError,
    [Empty, EmptyTEXTError],
    [Char, DelimCharError]
);

pub struct DelimCharError(u8);

pub struct EmptyTEXTError;

pub struct BlankKeyError;

pub struct BlankValueError(Vec<u8>);

pub struct UnevenWordsError;

pub struct FinalDelimError;

pub struct DelimBoundError;

enum_from_disp!(
    pub ParsePrimaryTEXTError,
    [Keywords, ParseKeywordsIssue],
    [Empty, NoTEXTWordsError]
);

pub struct NoTEXTWordsError;

enum_from_disp!(
    pub ParseKeywordsIssue,
    [BlankKey, BlankKeyError],
    [BlankValue, BlankValueError],
    [Uneven, UnevenWordsError],
    [Final, FinalDelimError],
    [Unique, KeywordInsertError],
    [Bound, DelimBoundError],
    // this is only for supp TEXT but seems less wasteful/convoluted to put here
    [Mismatch, DelimMismatch]

);

enum_from_disp!(
    pub ParseSupplementalTEXTError,
    [Keywords, ParseKeywordsIssue],
    [Mismatch, DelimMismatch]
);

pub struct DelimMismatch {
    supp: u8,
    delim: u8,
}

pub struct NonAsciiKeyError(String);

pub struct NonUtf8KeywordError {
    key: Vec<u8>,
    value: Vec<u8>,
}

pub struct NonstandardError;

fn read_fcs_raw_text_inner(
    p: &path::PathBuf,
    conf: &RawTextReadConfig,
) -> DeferredResult<
    (RawTEXTOutput, BufReader<fs::File>),
    ParseRawTEXTWarning,
    ImpureError<HeaderOrRawError>,
> {
    fs::File::options()
        .read(true)
        .open(p)
        .into_deferred()
        .def_and_maybe(|file| {
            let mut h = BufReader::new(file);
            RawTEXTOutput::h_read(&mut h, conf).def_map_value(|x| (x, h))
        })
}

fn h_read_dataset_from_kws<R: Read + Seek>(
    h: &mut BufReader<R>,
    version: Version,
    kws: &StdKeywords,
    data_seg: HeaderDataSegment,
    analysis_seg: HeaderAnalysisSegment,
    other_segs: &[OtherSegment],
    conf: &DataReadConfig,
) -> IODeferredResult<RawDatasetWithKwsOutput, ReadRawDatasetWarning, DatasetWithKwsError> {
    let data_res = kws_to_data_reader(version, kws, data_seg, conf)
        .def_inner_into()
        .def_errors_liftio();
    let analysis_res = kws_to_analysis_reader(version, kws, analysis_seg, &conf.reader)
        .def_inner_into()
        .def_errors_liftio();
    data_res.def_zip(analysis_res).def_and_maybe(|(dr, ar)| {
        let or = OthersReader { segs: other_segs };
        h_read_data_and_analysis(h, dr, ar, or)
            .map(
                |(data, analysis, others, d_seg, a_seg)| RawDatasetWithKwsOutput {
                    data,
                    analysis,
                    others,
                    data_seg: d_seg,
                    analysis_seg: a_seg,
                },
            )
            .into_deferred()
            .def_map_errors(|e: ImpureError<ReadDataError>| e.inner_into())
    })
}

impl RawTEXTOutput {
    fn h_read<R: Read + Seek>(
        h: &mut BufReader<R>,
        conf: &RawTextReadConfig,
    ) -> DeferredResult<Self, ParseRawTEXTWarning, ImpureError<HeaderOrRawError>> {
        Header::h_read(h, &conf.header)
            .mult_to_deferred()
            .def_map_errors(|e: ImpureError<HeaderError>| e.inner_into())
            .def_and_maybe(|header| {
                h_read_raw_text_from_header(h, header, conf).def_map_errors(|e| e.inner_into())
            })
    }

    fn into_std_text(
        self,
        conf: &StdTextReadConfig,
    ) -> DeferredResult<StdTEXTOutput, LookupMeasWarning, LookupKeysError> {
        let mut kws = self.keywords;
        AnyCoreTEXT::parse_raw(self.version, &mut kws.std, kws.nonstd, conf).def_map_value(
            |standardized| {
                let std = &mut kws.std;
                let tot = std.remove(&Tot::std());
                let timestep = std.remove(&Timestep::std());
                let data = SegmentKeywords {
                    begin: std.remove(&Begindata::std()),
                    end: std.remove(&Enddata::std()),
                };
                let analysis = SegmentKeywords {
                    begin: std.remove(&Beginanalysis::std()),
                    end: std.remove(&Endanalysis::std()),
                };
                StdTEXTOutput {
                    parse: self.parse,
                    standardized,
                    tot,
                    timestep,
                    data,
                    analysis,
                    deviant: kws.std,
                }
            },
        )
    }

    fn into_std_dataset<R: Read + Seek>(
        self,
        h: &mut BufReader<R>,
        conf: &DataReadConfig,
    ) -> DeferredResult<
        StdDatasetOutput,
        StdDatasetFromRawWarning,
        ImpureError<StdDatasetFromRawError>,
    > {
        let mut kws = self.keywords;
        AnyCoreDataset::parse_raw(
            h,
            self.version,
            &mut kws.std,
            kws.nonstd,
            self.parse.header_segments.data,
            self.parse.header_segments.analysis,
            &self.parse.header_segments.other[..],
            conf,
        )
        .def_map_value(|(core, data_seg, analysis_seg)| StdDatasetOutput {
            dataset: StdDatasetWithKwsOutput {
                standardized: DatasetWithSegments {
                    core,
                    data_seg,
                    analysis_seg,
                },
                deviant: kws.std,
            },
            parse: self.parse,
        })
    }
}

fn kws_to_data_reader(
    version: Version,
    kws: &StdKeywords,
    seg: HeaderDataSegment,
    conf: &DataReadConfig,
) -> DeferredResult<DataReader, RawToReaderWarning, RawToReaderError> {
    let cs = &conf.shared;
    let cr = &conf.reader;
    match version {
        Version::FCS2_0 => DataLayout2_0::try_new_from_raw(kws, cs)
            .def_inner_into()
            .def_and_maybe(|dl| dl.into_data_reader_raw(kws, seg, cr).def_inner_into()),
        Version::FCS3_0 => DataLayout3_0::try_new_from_raw(kws, cs)
            .def_inner_into()
            .def_and_maybe(|dl| dl.into_data_reader_raw(kws, seg, cr).def_inner_into()),
        Version::FCS3_1 => DataLayout3_1::try_new_from_raw(kws, cs)
            .def_inner_into()
            .def_and_maybe(|dl| dl.into_data_reader_raw(kws, seg, cr).def_inner_into()),
        Version::FCS3_2 => DataLayout3_2::try_new_from_raw(kws, cs)
            .def_inner_into()
            .def_and_maybe(|dl| dl.into_data_reader_raw(kws, seg, cr).def_inner_into()),
    }
}

fn kws_to_analysis_reader(
    version: Version,
    kws: &StdKeywords,
    seg: HeaderAnalysisSegment,
    conf: &ReaderConfig,
) -> AnalysisReaderResult<AnalysisReader> {
    match version {
        Version::FCS2_0 => DataLayout2_0::as_analysis_reader_raw(kws, seg, conf).def_inner_into(),
        Version::FCS3_0 => DataLayout3_0::as_analysis_reader_raw(kws, seg, conf).def_inner_into(),
        Version::FCS3_1 => DataLayout3_1::as_analysis_reader_raw(kws, seg, conf).def_inner_into(),
        Version::FCS3_2 => DataLayout3_2::as_analysis_reader_raw(kws, seg, conf).def_inner_into(),
    }
}

fn h_read_raw_text_from_header<R: Read + Seek>(
    h: &mut BufReader<R>,
    header: Header,
    conf: &RawTextReadConfig,
) -> DeferredResult<RawTEXTOutput, ParseRawTEXTWarning, ImpureError<ParseRawTEXTError>> {
    let mut buf = vec![];
    header
        .segments
        .text
        .inner
        .h_read_contents(h, &mut buf)
        .into_deferred()?;

    let tnt_delim = split_first_delim(&buf, conf)
        .def_inner_into()
        .def_errors_liftio()?;

    let tnt_primary = tnt_delim.and_maybe(|(delim, bytes)| {
        let kws = ParsedKeywords::default();
        split_raw_primary_text(kws, delim, bytes, conf)
            .def_inner_into()
            .def_errors_liftio()
            .def_map_value(|_kws| repair_offsets(_kws, conf))
            .def_map_value(|_kws| (delim, _kws))
    })?;

    let tnt_all_kws = tnt_primary.and_maybe(|(delim, mut kws)| {
        lookup_stext_offsets(&mut kws.std, header.version, conf)
            .errors_into()
            .errors_liftio()
            .warnings_into()
            .map(|s| (s, kws))
            .and_maybe(|(maybe_supp_seg, _kws)| {
                let tnt_supp_kws = if let Some(seg) = maybe_supp_seg {
                    buf.clear();
                    seg.inner
                        .h_read_contents(h, &mut buf)
                        .map_err(|e| DeferredFailure::new1(e.into()))?;
                    split_raw_supp_text(_kws, delim, &buf, conf)
                        .inner_into()
                        .errors_liftio()
                } else {
                    Tentative::new1(_kws)
                };
                Ok(tnt_supp_kws.map(|k| (delim, k, maybe_supp_seg)))
            })
    })?;

    let out = tnt_all_kws.and_tentatively(|(delimiter, mut kws, supp_text_seg)| {
        repair_keywords(&mut kws.std, conf);
        let mut tnt_parse = lookup_nextdata(&kws.std, conf.enforce_nextdata)
            .errors_into()
            .map(|nextdata| RawTEXTParseData {
                header_segments: header.segments,
                supp_text: supp_text_seg,
                nextdata,
                delimiter,
                non_ascii: kws.non_ascii,
                byte_pairs: kws.byte_pairs,
            });

        // throw errors if we found any non-ascii keywords and we want to know
        tnt_parse.eval_errors(|pd| {
            if conf.enforce_keyword_ascii {
                pd.non_ascii
                    .iter()
                    .map(|(k, _)| ParseRawTEXTError::NonAscii(NonAsciiKeyError(k.clone())))
                    .collect()
            } else {
                vec![]
            }
        });

        // throw errors if we found any non-utf8 keywords and we want to know
        tnt_parse.eval_errors(|pd| {
            if conf.enforce_utf8 {
                pd.byte_pairs
                    .iter()
                    .map(|(k, v)| {
                        ParseRawTEXTError::NonUtf8(NonUtf8KeywordError {
                            key: k.clone(),
                            value: v.clone(),
                        })
                    })
                    .collect()
            } else {
                vec![]
            }
        });

        // throw errors if the supp text segment overlaps with HEADER or
        // anything else
        tnt_parse.eval_errors(|pd| {
            if let Some(s) = pd.supp_text {
                let x = pd.header_segments.contains_text_segment(s).into_mult();
                let y = pd.header_segments.overlaps_with(s).mult_errors_into();
                x.mult_zip(y)
                    .mult_map_errors(Box::new)
                    .mult_map_errors(ParseRawTEXTError::Header)
                    .err()
                    .map(|n| n.into())
                    .unwrap_or_default()
            } else {
                vec![]
            }
        });

        // throw error if we have nonstandard keywords and we forbid them
        tnt_parse.eval_error(|_| {
            if kws.nonstd.is_empty() && conf.disallow_nonstandard {
                Some(NonstandardError.into())
            } else {
                None
            }
        });

        tnt_parse
            .inner_into()
            .map(|parse| RawTEXTOutput {
                version: header.version,
                parse,
                keywords: ValidKeywords {
                    std: kws.std,
                    nonstd: kws.nonstd,
                },
            })
            .errors_liftio()
    });

    Ok(out)
}

fn split_first_delim<'a>(
    bytes: &'a [u8],
    conf: &RawTextReadConfig,
) -> DeferredResult<(u8, &'a [u8]), DelimCharError, DelimVerifyError> {
    if let Some((delim, rest)) = bytes.split_first() {
        let mut tnt = Tentative::new1((*delim, rest));
        if (1..=126).contains(delim) {
            Ok(tnt)
        } else {
            let e = DelimCharError(*delim);
            if conf.force_ascii_delim {
                Err(DeferredFailure::new1(e.into()))
            } else {
                tnt.push_warning(e);
                Ok(tnt)
            }
        }
    } else {
        Err(DeferredFailure::new1(EmptyTEXTError.into()))
    }
}

fn split_raw_primary_text(
    kws: ParsedKeywords,
    delim: u8,
    bytes: &[u8],
    conf: &RawTextReadConfig,
) -> DeferredResult<ParsedKeywords, ParseKeywordsIssue, ParsePrimaryTEXTError> {
    if bytes.is_empty() {
        Err(DeferredFailure::new1(NoTEXTWordsError.into()))
    } else {
        Ok(split_raw_text_inner(kws, delim, bytes, conf).errors_into())
    }
}

fn split_raw_supp_text(
    kws: ParsedKeywords,
    delim: u8,
    bytes: &[u8],
    conf: &RawTextReadConfig,
) -> Tentative<ParsedKeywords, ParseKeywordsIssue, ParseSupplementalTEXTError> {
    if let Some((byte0, rest)) = bytes.split_first() {
        let mut tnt = split_raw_text_inner(kws, *byte0, rest, conf).errors_into();
        if *byte0 != delim {
            let x = DelimMismatch {
                delim,
                supp: *byte0,
            };
            if conf.enforce_stext_delim {
                tnt.push_error(x.into());
            } else {
                tnt.push_warning(x.into());
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
    conf: &RawTextReadConfig,
) -> Tentative<ParsedKeywords, ParseKeywordsIssue, ParseKeywordsIssue> {
    if conf.allow_double_delim {
        split_raw_text_noescape(kws, delim, bytes, conf)
    } else {
        split_raw_text_escape(kws, delim, bytes, conf)
    }
}

fn split_raw_text_noescape(
    mut kws: ParsedKeywords,
    delim: u8,
    bytes: &[u8],
    conf: &RawTextReadConfig,
) -> Tentative<ParsedKeywords, ParseKeywordsIssue, ParseKeywordsIssue> {
    let mut errors = vec![];
    let mut warnings = vec![];

    let mut push_issue = |test, error| {
        if test {
            errors.push(error);
        } else {
            warnings.push(error);
        }
    };

    // ASSUME input slice does not start with delim
    let mut it = bytes.split(|x| *x == delim);
    let mut prev_was_blank = false;
    let mut prev_was_key = false;

    while let Some(key) = it.next() {
        prev_was_key = true;
        prev_was_blank = key.is_empty();
        if key.is_empty() {
            if let Some(value) = it.next() {
                prev_was_key = false;
                prev_was_blank = value.is_empty();
                push_issue(conf.enforce_nonempty, BlankKeyError.into());
            } else {
                // if everything is correct, we should exit here since the
                // last word will be the blank slice after the final delim
                break;
            }
        } else if let Some(value) = it.next() {
            prev_was_key = false;
            prev_was_blank = value.is_empty();
            if value.is_empty() {
                push_issue(conf.enforce_nonempty, BlankValueError(key.to_vec()).into());
            } else if let Err(e) = kws.insert(key, value) {
                push_issue(conf.enforce_unique, e.into());
            }
        } else {
            // exiting here means we found a key without a value and also didn't
            // end with a delim
            break;
        }
    }

    if !prev_was_key {
        push_issue(conf.enforce_even, UnevenWordsError.into());
    }

    if !prev_was_blank {
        push_issue(conf.enforce_final_delim, FinalDelimError.into());
    }

    Tentative::new(kws, warnings, errors)
}

fn split_raw_text_escape(
    mut kws: ParsedKeywords,
    delim: u8,
    bytes: &[u8],
    conf: &RawTextReadConfig,
) -> Tentative<ParsedKeywords, ParseKeywordsIssue, ParseKeywordsIssue> {
    let mut ews = (vec![], vec![]);

    let push_issue = |_ews: &mut (Vec<_>, Vec<_>), test, error| {
        let warnings = &mut _ews.0;
        let errors = &mut _ews.1;
        if test {
            errors.push(error);
        } else {
            warnings.push(error);
        }
    };

    let mut push_pair = |_ews: &mut (Vec<_>, Vec<_>), kb: &Vec<_>, vb: &Vec<_>| {
        if let Err(e) = kws.insert(kb, vb) {
            push_issue(_ews, conf.enforce_unique, e.into())
        }
    };

    let push_delim = |kb: &mut Vec<_>, vb: &mut Vec<_>, k: usize| {
        let n = (k + 1) / 2;
        let buf = if vb.is_empty() { kb } else { vb };
        for _ in 0..n {
            buf.push(delim);
        }
    };

    // ASSUME input slice does not start with delim
    let mut consec_blanks = 0;
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
                    push_issue(&mut ews, conf.enforce_delim_nobound, DelimBoundError.into());
                }
            } else {
                // Previous consecutive delimiter sequence was even. Push n / 2
                // delimiters to whatever the current word is.
                push_delim(&mut keybuf, &mut valuebuf, consec_blanks);
            }
            consec_blanks = 0;
        }
    }

    if consec_blanks == 0 {
        push_issue(&mut ews, conf.enforce_final_delim, FinalDelimError.into());
    } else if consec_blanks & 1 == 1 {
        // technically this ends with a delim but it is part of a word so
        // doesn't count
        push_issue(&mut ews, conf.enforce_final_delim, FinalDelimError.into());
        push_issue(&mut ews, conf.enforce_delim_nobound, DelimBoundError.into());
        push_delim(&mut keybuf, &mut valuebuf, consec_blanks);
    } else if consec_blanks > 1 {
        push_issue(&mut ews, conf.enforce_delim_nobound, DelimBoundError.into());
    }

    if valuebuf.is_empty() {
        push_issue(&mut ews, conf.enforce_even, UnevenWordsError.into());
    } else {
        push_pair(&mut ews, &keybuf, &valuebuf);
    }

    Tentative::new(kws, ews.0, ews.1)
}

fn repair_keywords(kws: &mut StdKeywords, conf: &RawTextReadConfig) {
    for (key, v) in kws.iter_mut() {
        // TODO generalized this and possibly put in a trait
        if key == &FCSDate::std() {
            if let Some(pattern) = &conf.date_pattern {
                if let Ok(d) = NaiveDate::parse_from_str(v, pattern.as_ref()) {
                    *v = FCSDate(d).to_string();
                }
            }
        }
    }
}

fn repair_offsets(mut kws: ParsedKeywords, conf: &RawTextReadConfig) -> ParsedKeywords {
    if conf.repair_offset_spaces {
        for (key, v) in kws.std.iter_mut() {
            if key == &Begindata::std()
                || key == &Enddata::std()
                || key == &Beginstext::std()
                || key == &Endstext::std()
                || key == &Beginanalysis::std()
                || key == &Endanalysis::std()
                || key == &Nextdata::std()
                || key == &Tot::std()
            {
                *v = v.as_str().trim().to_string();
            }
        }
    }
    kws
}

fn lookup_stext_offsets(
    kws: &mut StdKeywords,
    version: Version,
    conf: &RawTextReadConfig,
) -> Tentative<Option<SupplementalTextSegment>, STextSegmentWarning, ReqSegmentError> {
    match version {
        Version::FCS2_0 => Tentative::new1(None),
        Version::FCS3_0 | Version::FCS3_1 => KeyedReqSegment::get_mult(kws, conf.stext)
            .map_or_else(
                |es| Tentative::new_either(None, es.into(), conf.enforce_stext),
                |t| Tentative::new1(Some(t)),
            ),
        Version::FCS3_2 => KeyedOptSegment::get(kws, conf.stext).warnings_into(),
    }
}

// TODO the reason we use get instead of remove here is because we don't want to
// mess up the keyword list for raw mode, but in standardized mode we are
// consuming the hash table as a way to test for deviant keywords (ie those that
// are left over). In order to reconcile these, we either need to make two raw
// text reader functions which either take immutable or mutable kws or use a
// more clever hash table that marks keys when we see them.
fn lookup_nextdata(
    kws: &StdKeywords,
    enforce: bool,
) -> Tentative<Option<u32>, ParseKeyError<ParseIntError>, ReqKeyError<ParseIntError>> {
    let k = Nextdata::std();
    if enforce {
        get_req(kws, k).map_or_else(
            |e| Tentative::new(None, vec![], vec![e]),
            |t| Tentative::new1(Some(t)),
        )
    } else {
        get_opt(kws, k).map_or_else(|w| Tentative::new(None, vec![w], vec![]), Tentative::new1)
    }
}

impl fmt::Display for DelimCharError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "delimiter must be ASCII character 1-126 inclusive, got {}",
            self.0
        )
    }
}

impl fmt::Display for EmptyTEXTError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "TEXT segment is empty")
    }
}

impl fmt::Display for BlankKeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "encountered blank key, skipping key and its value")
    }
}

impl fmt::Display for BlankValueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "skipping key with blank value, keys bytes were {}",
            self.0.iter().join(",")
        )
    }
}

impl fmt::Display for UnevenWordsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "TEXT segment has uneven number of words",)
    }
}

impl fmt::Display for FinalDelimError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "TEXT does not end with delim",)
    }
}

impl fmt::Display for DelimBoundError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "delimiter encountered at word boundary",)
    }
}

impl fmt::Display for NoTEXTWordsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "TEXT has a delimiter and no words",)
    }
}

impl fmt::Display for DelimMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "first byte of supplemental TEXT ({}) does not match delimiter of primary TEXT ({})",
            self.supp, self.delim
        )
    }
}

impl fmt::Display for NonAsciiKeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "non-ASCII key encountered and dropped: {}", self.0)
    }
}

impl fmt::Display for NonUtf8KeywordError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let n = 10;
        write!(
            f,
            "non UTF-8 key/value pair encountered and dropped, \
             first 10 bytes of both are ({})/({})",
            self.key.iter().take(n).join(","),
            self.value.iter().take(n).join(",")
        )
    }
}

impl fmt::Display for NonstandardError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "nonstandard keywords detected")
    }
}

impl fmt::Display for HeaderFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "could not parse HEADER")
    }
}

impl fmt::Display for RawTEXTFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "could not parse TEXT segment")
    }
}

impl fmt::Display for StdTEXTFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "could not standardize TEXT segment")
    }
}

impl fmt::Display for StdDatasetFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "could not read DATA with standardized TEXT")
    }
}

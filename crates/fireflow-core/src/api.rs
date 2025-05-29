use crate::config::*;
pub use crate::core::*;
pub use crate::data::*;
use crate::error::*;
pub use crate::header::*;
pub use crate::header_text::*;
use crate::macros::{enum_from, enum_from_disp, match_many_to_one};
pub use crate::segment::*;
pub use crate::text::keywords::*;
use crate::text::parser::*;
use crate::text::timestamps::*;
use crate::validated::dataframe::FCSDataFrame;
use crate::validated::nonstandard::NonStdKeywords;
use crate::validated::standard::*;

use chrono::NaiveDate;
use itertools::Itertools;
use serde::Serialize;
use std::fmt;
use std::fs;
use std::io;
use std::io::{BufReader, Read, Seek};
use std::num::ParseIntError;
use std::path;
use std::str;

// TODO gating parameters not added (yet)

/// Output from parsing the TEXT segment.
#[derive(Serialize)]
pub struct RawTEXTOutput {
    pub version: Version,
    pub keywords: ValidKeywords,
    pub parse: RawTEXTSupplementalData,
}

/// Output of parsing the TEXT segment and standardizing keywords.
pub struct StandardizedTEXTOutput {
    pub standardized: AnyCoreTEXT,
    pub tot: Option<String>,
    pub data: SegmentKeywords,
    pub analysis: SegmentKeywords,
    pub deviant: StdKeywords,
    pub parse: RawTEXTSupplementalData,
}

/// Intermediate store for TEXT keywords that might be parsed as a segment
pub struct SegmentKeywords {
    pub begin: Option<String>,
    pub end: Option<String>,
}

/// Output of parsing one raw dataset (TEXT+DATA) from an FCS file.
pub struct RawDatasetOutput {
    pub version: Version,
    pub keywords: ValidKeywords,
    pub dataset: KwsToRawDatasetOutput,
    pub parse: RawTEXTSupplementalData,
}

/// Output of parsing one standardized dataset (TEXT+DATA) from an FCS file.
pub struct StandardizedDatasetOutput {
    pub dataset: ParsedDataset,
    pub deviant: StdKeywords,
    pub supp: RawTEXTSupplementalData,
}

/// Output of parsing one standardized dataset (TEXT+DATA) from an FCS file.
pub struct KwsToStdDatasetOutput {
    pub dataset: ParsedDataset,
    pub deviant: StdKeywords,
}

// pub struct TEXTOffsets {
//     pub data_seg: AnyDataSegment,
//     pub analysis_seg: AnyAnalysisSegment,
// }

pub struct StdDatasetFromRaw {
    pub parsed: ParsedDataset,
    pub deviant: StdKeywords,
}

pub struct ParsedDataset {
    pub core: AnyCoreDataset,
    pub data_seg: AnyDataSegment,
    pub analysis_seg: AnyAnalysisSegment,
}

pub struct KwsToRawDatasetOutput {
    pub data: FCSDataFrame,
    pub analysis: Analysis,
    pub data_seg: AnyDataSegment,
    pub analysis_seg: AnyAnalysisSegment,
}

/// Data pertaining to parsing the TEXT segment.
#[derive(Clone, Serialize)]
pub struct RawTEXTSupplementalData {
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

/// Return header in an FCS file.
///
/// The header contains the version and offsets for the TEXT, DATA, and ANALYSIS
/// segments, all of which are present in fixed byte offset segments. This
/// function will fail and return an error if the file does not follow this
/// structure. Will also check that the begin and end segments are not reversed.
///
/// Depending on the version, all of these except the TEXT offsets might be 0
/// which indicates they are actually stored in TEXT due to size limitations.
pub fn read_fcs_header(
    p: &path::PathBuf,
    conf: &HeaderConfig,
) -> TerminalResult<Header, (), ImpureError<HeaderError>, HeaderFailure> {
    fs::File::options()
        .read(true)
        .open(p)
        .into_deferred0()
        .and_maybe(|file| {
            let mut reader = BufReader::new(file);
            h_read_header(&mut reader, conf).into_deferred1()
        })
        .terminate(HeaderFailure)
}

/// Return header and raw key/value metadata pairs in an FCS file.
///
/// First will parse the header according to [`read_fcs_header`]. If this fails
/// an error will be returned.
///
/// Next will use the offset information in the header to parse the TEXT segment
/// for key/value pairs. On success will return these pairs as-is using Strings
/// in a HashMap. No other processing will be performed.
pub fn read_fcs_raw_text(
    p: &path::PathBuf,
    conf: &RawTextReadConfig,
) -> TerminalResult<
    RawTEXTOutput,
    ParseRawTEXTWarning,
    ImpureError<HeaderOrRawError>,
    ParseRawTEXTFailure,
> {
    read_fcs_raw_text_inner(p, conf)
        .map_value(|(x, _)| x)
        .terminate(ParseRawTEXTFailure)
}

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
        .into_deferred0()
        .and_maybe(|file| {
            let mut h = BufReader::new(file);
            RawTEXTOutput::h_read(&mut h, conf).map_value(|x| (x, h))
        })
}

/// Return header and standardized metadata in an FCS file.
///
/// Begins by parsing header and raw keywords according to [`read_fcs_raw_text`]
/// and will return error if this function fails.
///
/// Next, all keywords in the TEXT segment will be validated to conform to the
/// FCS standard indicated in the header and returned in a struct storing each
/// key/value pair in a standardized manner. This will halt and return any
/// errors encountered during this process.
pub fn read_fcs_std_text(
    p: &path::PathBuf,
    conf: &StdTextReadConfig,
) -> TerminalResult<
    StandardizedTEXTOutput,
    StdTEXTWarning,
    ImpureError<StdTEXTError>,
    CoreTEXTFailure,
> {
    read_fcs_raw_text_inner(p, &conf.raw)
        .map_value(|(x, _)| x)
        .errors_map(|e| e.inner_into())
        .warning_into()
        .and_maybe(|raw| raw.into_std_text(conf).inner_into().error_impure())
        .terminate(CoreTEXTFailure)
}

/// Return header, raw metadata, and data in an FCS file.
///
/// In contrast to [`read_fcs_std_file`], this will return the keywords as a
/// flat list of key/value pairs. Only the bare minimum of these will be read in
/// order to determine how to parse the DATA segment (including $DATATYPE,
/// $BYTEORD, etc). No other checks will be performed to ensure the metadata
/// conforms to the FCS standard version indicated in the header.
///
/// This might be useful for applications where one does not necessarily need
/// the strict structure of the standardized metadata, or if one does not care
/// too much about the degree to which the metadata conforms to standard.
///
/// Other than this, behavior is identical to [`read_fcs_std_file`],
pub fn read_fcs_raw_file(
    p: &path::PathBuf,
    conf: &DataReadConfig,
) -> TerminalResult<
    RawDatasetOutput,
    RawDatasetWarning,
    ImpureError<RawDatasetError>,
    ReadRawDatasetFailure,
> {
    read_fcs_raw_text_inner(p, &conf.standard.raw)
        .errors_map(|e| e.inner_into())
        .warning_into()
        .and_maybe(|(raw, mut h)| {
            h_read_dataset_from_kws(
                &mut h,
                raw.version,
                &raw.keywords.std,
                raw.parse.header_segments.data,
                raw.parse.header_segments.analysis,
                conf,
            )
            .map_value(|dataset| RawDatasetOutput {
                version: raw.version,
                dataset,
                parse: raw.parse,
                keywords: raw.keywords,
            })
            .warning_into()
            .errors_map(|e| e.inner_into())
        })
        .terminate(ReadRawDatasetFailure)
}

/// Return header, structured metadata, and data in an FCS file.
///
/// Begins by parsing header and raw keywords according to [`read_fcs_text`]
/// and will return error if this function fails.
///
/// Next, the DATA segment will be parsed according to the metadata present
/// in TEXT.
///
/// On success will return all three of the above segments along with any
/// non-critical warnings.
///
/// The [`conf`] argument can be used to control the behavior of each reading
/// step, including the repair of non-conforming files.
pub fn read_fcs_std_file(
    p: &path::PathBuf,
    conf: &DataReadConfig,
) -> TerminalResult<
    StandardizedDatasetOutput,
    StdDatasetWarning,
    ImpureError<StdDatasetError>,
    CoreDatasetFailure,
> {
    read_fcs_raw_text_inner(p, &conf.standard.raw)
        .errors_map(|e| e.inner_into())
        .warning_into()
        .and_maybe(|(raw, mut h)| {
            raw.into_std_dataset(&mut h, conf)
                .warning_into()
                .errors_map(|e| e.inner_into())
        })
        .terminate(CoreDatasetFailure)
}

pub fn read_fcs_raw_file_from_raw(
    p: path::PathBuf,
    version: Version,
    std: &StdKeywords,
    conf: &DataReadConfig,
    data_seg: HeaderDataSegment,
    analysis_seg: HeaderAnalysisSegment,
) -> TerminalResult<
    KwsToRawDatasetOutput,
    ReadRawDatasetWarning,
    ImpureError<KwsToDatasetError>,
    KwsToRawDatasetFailure,
> {
    fs::File::options()
        .read(true)
        .open(p)
        .into_deferred0()
        .and_maybe(|file| {
            let mut h = BufReader::new(file);
            h_read_dataset_from_kws(&mut h, version, std, data_seg, analysis_seg, conf)
        })
        .terminate(KwsToRawDatasetFailure)
}

pub fn read_fcs_std_file_from_raw(
    p: &path::PathBuf,
    version: Version,
    mut std: StdKeywords,
    nonstd: NonStdKeywords,
    conf: &DataReadConfig,
    data_seg: HeaderDataSegment,
    analysis_seg: HeaderAnalysisSegment,
) -> TerminalResult<
    KwsToStdDatasetOutput,
    CoreDatasetFromRawWarning,
    ImpureError<CoreDatasetFromRawError>,
    KwsToStdDatasetFailure,
> {
    fs::File::options()
        .read(true)
        .open(p)
        .into_deferred0()
        .and_maybe(|file| {
            let mut h = BufReader::new(file);
            AnyCoreDataset::parse_raw(
                &mut h,
                version,
                &mut std,
                nonstd,
                data_seg,
                analysis_seg,
                conf,
            )
            .map_value(|(core, d_seg, a_seg)| KwsToStdDatasetOutput {
                dataset: ParsedDataset {
                    core,
                    data_seg: d_seg,
                    analysis_seg: a_seg,
                },
                deviant: std,
            })
        })
        .terminate(KwsToStdDatasetFailure)
}

// fn h_read_raw_dataset<R: Read + Seek>(
//     h: &mut BufReader<R>,
//     raw: RawTEXTOutput,
//     conf: &DataReadConfig,
// ) -> TerminalResult<
//     RawDatasetOutput,
//     ReadRawDatasetWarning,
//     ReadRawDatasetError,
//     ReadRawDatasetFailure,
// > {
//     let version = raw.version;

//     h_read_raw_dataset_from_raw(
//         h,
//         version,
//         &raw.keywords.std,
//         raw.parse.header_segments.data,
//         raw.parse.header_segments.analysis,
//         conf,
//     )
//     .term_map_value(|dataset| RawDatasetOutput {
//         version,
//         keywords: raw.keywords,
//         dataset,
//         parse: raw.parse,
//     })
// }

// fn h_read_std_dataset<R: Read + Seek>(
//     h: &mut BufReader<R>,
//     std: StandardizedTEXTOutput,
//     conf: &DataReadConfig,
// ) -> TerminalResult<
//     StandardizedDatasetOutput,
//     ReadStdDatasetWarning,
//     ReadStdDatasetError,
//     ReadStdDatasetFailure,
// > {
//     h_read_std_dataset_from_core(
//         h,
//         std.standardized,
//         TotValue(std.tot.as_deref()),
//         std.data,
//         std.analysis,
//         std.parse.header_segments.data,
//         std.parse.header_segments.analysis,
//         conf,
//     )
//     .term_map_value(|dataset| StandardizedDatasetOutput {
//         deviant: std.deviant,
//         dataset,
//         supp: std.parse,
//     })
// }

fn h_read_dataset_from_kws<R: Read + Seek>(
    h: &mut BufReader<R>,
    version: Version,
    kws: &StdKeywords,
    data_seg: HeaderDataSegment,
    analysis_seg: HeaderAnalysisSegment,
    conf: &DataReadConfig,
) -> DeferredResult<KwsToRawDatasetOutput, ReadRawDatasetWarning, ImpureError<KwsToDatasetError>> {
    let data_res = kws_to_data_reader(version, kws, data_seg, conf)
        .inner_into()
        .error_impure();
    let analysis_res = kws_to_analysis_reader(version, kws, analysis_seg, conf)
        .inner_into()
        .error_impure();
    data_res.zip_def(analysis_res).and_maybe(|(dr, ar)| {
        h_read_data_and_analysis(h, dr, ar)
            .map(|(data, analysis, d_seg, a_seg)| KwsToRawDatasetOutput {
                data,
                analysis,
                data_seg: d_seg,
                analysis_seg: a_seg,
            })
            .into_deferred0()
            .errors_map(|e: ImpureError<ReadDataError>| e.inner_into())
    })
}

fn h_read_data_and_analysis<R: Read + Seek>(
    h: &mut BufReader<R>,
    data_reader: DataReader,
    analysis_reader: AnalysisReader,
) -> Result<(FCSDataFrame, Analysis, AnyDataSegment, AnyAnalysisSegment), ImpureError<ReadDataError>>
{
    let dseg = data_reader.seg;
    let data = data_reader.h_read(h)?;
    let analysis = analysis_reader.h_read(h)?;
    Ok((data, analysis, dseg, analysis_reader.seg))
}

// #[allow(clippy::too_many_arguments)]
// fn h_read_std_dataset_from_core<R: Read + Seek>(
//     h: &mut BufReader<R>,
//     core: AnyCoreTEXT,
//     tot: TotValue,
//     data_seg: SegmentKeywords<DataSegmentId>,
//     anal_seg: SegmentKeywords<AnalysisSegmentId>,
//     def_data_seg: HeaderDataSegment,
//     def_anal_seg: HeaderAnalysisSegment,
//     conf: &DataReadConfig,
// ) -> TerminalResult<ParsedDataset, ReadStdDatasetWarning, ReadStdDatasetError, ReadStdDatasetFailure>
// {
//     let version = core.version();

//     let tnt_anal = lookup_analysis_offsets(anal_seg, conf, version, def_anal_seg).inner_into();

//     let reader_res = lookup_data_offsets(data_seg, conf, version, def_data_seg)
//         .inner_into()
//         .and_maybe(|_data_seg| {
//             core.as_data_reader(&tot, conf, _data_seg)
//                 .inner_into()
//                 .map_value(|reader| (reader, _data_seg))
//         });

//     reader_res
//         .and_tentatively(|x| tnt_anal.map(|y| (x, y)))
//         .and_maybe(|((reader, _data_seg), analysis_seg)| {
//             let columns = reader
//                 .h_read(h)
//                 .map_err(|e| DeferredFailure::new1(e.into()))?;
//             let analysis =
//                 h_read_analysis(h, analysis_seg).map_err(|e| DeferredFailure::new1(e.into()))?;
//             let pd = ParsedDataset {
//                 core: core.into_coredataset_unchecked(columns, analysis),
//                 data_seg: _data_seg,
//                 analysis_seg,
//             };
//             Ok(Tentative::new1(pd))
//         })
//         .terminate(ReadStdDatasetFailure)
// }

impl RawTEXTOutput {
    fn h_read<R: Read + Seek>(
        h: &mut BufReader<R>,
        conf: &RawTextReadConfig,
    ) -> DeferredResult<Self, ParseRawTEXTWarning, ImpureError<HeaderOrRawError>> {
        h_read_header(h, &conf.header)
            .into_deferred1()
            .errors_map(|e: ImpureError<HeaderError>| e.inner_into())
            .and_maybe(|header| {
                h_read_raw_text_from_header(h, header, conf).errors_map(|e| e.inner_into())
            })
    }

    fn into_std_text(
        self,
        conf: &StdTextReadConfig,
    ) -> DeferredResult<StandardizedTEXTOutput, LookupMeasWarning, ParseKeysError> {
        let mut kws = self.keywords;
        AnyCoreTEXT::parse_raw(self.version, &mut kws.std, kws.nonstd, conf).map_value(
            |standardized| {
                let std = &mut kws.std;
                let tot = std.remove(&Tot::std());
                let data = SegmentKeywords {
                    begin: std.remove(&Begindata::std()),
                    end: std.remove(&Enddata::std()),
                };
                let analysis = SegmentKeywords {
                    begin: std.remove(&Beginanalysis::std()),
                    end: std.remove(&Endanalysis::std()),
                };
                StandardizedTEXTOutput {
                    parse: self.parse,
                    standardized,
                    tot,
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
        StandardizedDatasetOutput,
        CoreDatasetFromRawWarning,
        ImpureError<CoreDatasetFromRawError>,
    > {
        let mut kws = self.keywords;
        AnyCoreDataset::parse_raw(
            h,
            self.version,
            &mut kws.std,
            kws.nonstd,
            self.parse.header_segments.data,
            self.parse.header_segments.analysis,
            conf,
        )
        .map_value(|(core, data_seg, analysis_seg)| StandardizedDatasetOutput {
            dataset: ParsedDataset {
                core,
                data_seg,
                analysis_seg,
            },
            deviant: kws.std,
            supp: self.parse,
        })
    }
}

fn kws_to_data_reader(
    version: Version,
    kws: &StdKeywords,
    seg: HeaderDataSegment,
    conf: &DataReadConfig,
) -> DeferredResult<DataReader, RawToReaderWarning, RawToReaderError> {
    let sc = &conf.shared;
    match version {
        Version::FCS2_0 => DataLayout2_0::try_new_from_raw(kws, sc)
            .inner_into()
            .and_maybe(|dl| dl.into_data_reader_raw(kws, seg, conf).inner_into()),
        Version::FCS3_0 => DataLayout3_0::try_new_from_raw(kws, sc)
            .inner_into()
            .and_maybe(|dl| dl.into_data_reader_raw(kws, seg, conf).inner_into()),
        Version::FCS3_1 => DataLayout3_1::try_new_from_raw(kws, sc)
            .inner_into()
            .and_maybe(|dl| dl.into_data_reader_raw(kws, seg, conf).inner_into()),
        Version::FCS3_2 => DataLayout3_2::try_new_from_raw(kws, sc)
            .inner_into()
            .and_maybe(|dl| dl.into_data_reader_raw(kws, seg, conf).inner_into()),
    }
}

fn kws_to_analysis_reader(
    version: Version,
    kws: &StdKeywords,
    seg: HeaderAnalysisSegment,
    conf: &DataReadConfig,
) -> AnalysisReaderResult {
    match version {
        Version::FCS2_0 => DataLayout2_0::as_analysis_reader_raw(kws, seg, conf).inner_into(),
        Version::FCS3_0 => DataLayout3_0::as_analysis_reader_raw(kws, seg, conf).inner_into(),
        Version::FCS3_1 => DataLayout3_1::as_analysis_reader_raw(kws, seg, conf).inner_into(),
        Version::FCS3_2 => DataLayout3_2::as_analysis_reader_raw(kws, seg, conf).inner_into(),
    }
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

fn split_raw_text_nodouble(
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

fn split_raw_text_double(
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

fn split_raw_primary_text(
    kws: ParsedKeywords,
    delim: u8,
    bytes: &[u8],
    conf: &RawTextReadConfig,
) -> DeferredResult<ParsedKeywords, ParseKeywordsIssue, ParsePrimaryTEXTError> {
    if bytes.is_empty() {
        Err(DeferredFailure::new1(NoTEXTWordsError.into()))
    } else if conf.allow_double_delim {
        Ok(split_raw_text_double(kws, delim, bytes, conf).errors_into())
    } else {
        Ok(split_raw_text_nodouble(kws, delim, bytes, conf).errors_into())
    }
}

fn split_raw_supp_text(
    kws: ParsedKeywords,
    delim: u8,
    bytes: &[u8],
    conf: &RawTextReadConfig,
) -> Tentative<ParsedKeywords, ParseKeywordsIssue, ParseSupplementalTEXTError> {
    if let Some((byte0, rest)) = bytes.split_first() {
        let mut tnt = if conf.allow_double_delim {
            split_raw_text_double(kws, *byte0, rest, conf).errors_into()
        } else {
            split_raw_text_nodouble(kws, *byte0, rest, conf).errors_into()
        };
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

fn pad_zeros(s: &str) -> String {
    let len = s.len();
    let trimmed = s.trim_start();
    let newlen = trimmed.len();
    ("0").repeat(len - newlen) + trimmed
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
            {
                *v = pad_zeros(v.as_str())
            }
        }
    }
    kws
}

// fn lookup_data_offsets(
//     kws: SegmentKeywords<DataSegmentId>,
//     conf: &DataReadConfig,
//     version: Version,
//     default: HeaderDataSegment,
// ) -> Tentative<AnyDataSegment, DataSegmentWarning, DataSegmentError> {
//     let d = SegmentDefaultWarning::default();
//     match version {
//         Version::FCS2_0 => Tentative::new1(default.into_any()),
//         _ => SegmentFromTEXT::req(kws, conf.data).map_or_else(
//             |es| {
//                 if conf.standard.raw.enforce_required_offsets {
//                     let ws = es.map(|e| e.into()).into_iter().chain([d.into()]).collect();
//                     Tentative::new(default.into_any(), vec![], ws)
//                 } else {
//                     let ws = es.map(|e| e.into()).into_iter().chain([d.into()]).collect();
//                     Tentative::new(default.into_any(), ws, vec![])
//                 }
//             },
//             |t| {
//                 let w = if t.inner != default.inner && !default.inner.is_empty() {
//                     Some(SegmentMismatchWarning {
//                         header: default,
//                         text: t,
//                     })
//                 } else {
//                     None
//                 };
//                 let xs = w.into_iter().collect();
//                 Tentative::new_either(t.into_any(), xs, conf.standard.raw.enforce_offset_match)
//             },
//         ),
//     }
// }

// fn lookup_analysis_offsets(
//     kws: SegmentKeywords<AnalysisSegmentId>,
//     conf: &DataReadConfig,
//     version: Version,
//     default: HeaderAnalysisSegment,
// ) -> Tentative<AnyAnalysisSegment, AnalysisSegmentWarning, AnalysisSegmentError> {
//     let d = SegmentDefaultWarning::default();
//     let def_any = default.into_any();
//     match version {
//         Version::FCS2_0 => Tentative::new1(def_any),

//         Version::FCS3_0 | Version::FCS3_1 => SegmentFromTEXT::req(kws, conf.analysis).map_or_else(
//             |es| {
//                 // TODO clean this up
//                 if conf.standard.raw.enforce_required_offsets {
//                     let ws = es.map(|e| e.into()).into_iter().chain([d.into()]).collect();
//                     Tentative::new(def_any, vec![], ws)
//                 } else {
//                     let ws = es.map(|e| e.into()).into_iter().chain([d.into()]).collect();
//                     Tentative::new(def_any, ws, vec![])
//                 }
//             },
//             |t| Tentative::new1(t.into_any()),
//         ),

//         Version::FCS3_2 => SegmentFromTEXT::opt(kws, conf.analysis).map_or_else(
//             |es| {
//                 // unlike the above, this can never error because the keywords
//                 // are optional
//                 let ws = es.map(|e| e.into()).into_iter().chain([d.into()]).collect();
//                 Tentative::new(def_any, ws, vec![])
//             },
//             |t| {
//                 if let Some(this_seg) = t {
//                     let w = if this_seg.inner != default.inner && !default.inner.is_empty() {
//                         Some(SegmentMismatchWarning {
//                             header: default,
//                             text: this_seg,
//                         })
//                     } else {
//                         None
//                     };
//                     let this_any = this_seg.into_any();
//                     let xs: Vec<_> = w.into_iter().collect();
//                     Tentative::new_either(this_any, xs, conf.standard.raw.enforce_offset_match)
//                 } else {
//                     Tentative::new1(t.map(|x| x.into_any()).unwrap_or(def_any))
//                 }
//             },
//         ),
//     }
// }

fn lookup_stext_offsets(
    kws: &mut StdKeywords,
    version: Version,
    conf: &RawTextReadConfig,
) -> Tentative<Option<SupplementalTextSegment>, STextSegmentWarning, ReqSegmentError> {
    match version {
        Version::FCS2_0 => Tentative::new1(None),
        Version::FCS3_0 | Version::FCS3_1 => LookupSegment::remove_req_mult(kws, conf.stext)
            .map_or_else(
                |es| Tentative::new_either(None, es.into(), conf.enforce_stext),
                |t| Tentative::new1(Some(t)),
            ),
        Version::FCS3_2 => LookupSegment::remove_opt(kws, conf.stext).warnings_into(),
    }
}

fn lookup_nextdata(
    kws: &StdKeywords,
    enforce: bool,
) -> Tentative<Option<u32>, ParseKeyError<ParseIntError>, ReqKeyError<ParseIntError>> {
    let k = &Nextdata::std();
    if enforce {
        get_req(kws, k).map_or_else(
            |e| Tentative::new(None, vec![], vec![e]),
            |t| Tentative::new1(Some(t)),
        )
    } else {
        get_opt(kws, k).map_or_else(|w| Tentative::new(None, vec![w], vec![]), Tentative::new1)
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
        .h_read(h, &mut buf)
        .into_deferred0()?;

    let tnt_delim = split_first_delim(&buf, conf).inner_into().error_impure()?;

    let tnt_primary = tnt_delim.and_maybe(|(delim, bytes)| {
        let kws = ParsedKeywords::default();
        split_raw_primary_text(kws, delim, bytes, conf)
            .inner_into()
            .error_impure()
            .map_value(|_kws| repair_offsets(_kws, conf))
            .map_value(|_kws| (delim, _kws))
    })?;

    let tnt_all_kws = tnt_primary.and_maybe(|(delim, mut kws)| {
        lookup_stext_offsets(&mut kws.std, header.version, conf)
            .errors_into()
            .errors_map(ImpureError::Pure)
            .warnings_into()
            .map(|s| (s, kws))
            .and_maybe(|(maybe_supp_seg, _kws)| {
                let tnt_supp_kws = if let Some(seg) = maybe_supp_seg {
                    buf.clear();
                    seg.inner
                        .h_read(h, &mut buf)
                        .map_err(|e| DeferredFailure::new1(e.into()))?;
                    split_raw_supp_text(_kws, delim, &buf, conf)
                        .inner_into()
                        .error_impure()
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
            .map(|nextdata| RawTEXTSupplementalData {
                header_segments: header.segments,
                supp_text: supp_text_seg,
                nextdata,
                delimiter,
                non_ascii: kws.non_ascii,
                byte_pairs: kws.byte_pairs,
            });
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

        tnt_parse
            .map(|parse| RawTEXTOutput {
                version: header.version,
                parse,
                keywords: ValidKeywords {
                    std: kws.std,
                    nonstd: kws.nonstd,
                },
            })
            .errors_into()
            .errors_map(ImpureError::Pure)
            .warnings_into()
    });

    Ok(out)
}

enum_from_disp!(
    pub DataSegmentError,
    [Req, ReqSegmentError],
    [Mismatch, SegmentMismatchWarning<DataSegmentId>],
    [Default, SegmentDefaultWarning<DataSegmentId>]
);

enum_from_disp!(
    pub DataSegmentWarning,
    [Segment, ReqSegmentError],
    [Default, SegmentDefaultWarning<DataSegmentId>],
    [Mismatch, SegmentMismatchWarning<DataSegmentId>]
);

enum_from_disp!(
    pub AnalysisSegmentWarning,
    [ReqSegment, ReqSegmentError],
    [OptSegment, OptSegmentError],
    [Default, SegmentDefaultWarning<AnalysisSegmentId>],
    [Mismatch, SegmentMismatchWarning<AnalysisSegmentId>]
);

enum_from_disp!(
    pub AnalysisSegmentError,
    [Req, ReqSegmentError],
    [Mismatch, SegmentMismatchWarning<AnalysisSegmentId>],
    [Default, SegmentDefaultWarning<AnalysisSegmentId>]
);

enum_from_disp!(
    pub STextSegmentWarning,
    [ReqSegment, ReqSegmentError],
    [OptSegment, OptSegmentError]
);

pub struct DelimCharError(u8);

impl fmt::Display for DelimCharError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "delimiter must be ASCII character 1-126 inclusive, got {}",
            self.0
        )
    }
}

pub struct EmptyTEXTError;

impl fmt::Display for EmptyTEXTError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "TEXT segment is empty")
    }
}

enum_from_disp!(
    pub DelimVerifyError,
    [Empty, EmptyTEXTError],
    [Char, DelimCharError]
);

pub struct BlankKeyError;

impl fmt::Display for BlankKeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "encountered blank key, skipping key and its value")
    }
}

pub struct BlankValueError(Vec<u8>);

impl fmt::Display for BlankValueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "skipping key with blank value, keys bytes were {}",
            self.0.iter().join(",")
        )
    }
}

pub struct UnevenWordsError;

impl fmt::Display for UnevenWordsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "TEXT segment has uneven number of words",)
    }
}

pub struct FinalDelimError;

impl fmt::Display for FinalDelimError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "TEXT does not end with delim",)
    }
}

pub struct DelimBoundError;

impl fmt::Display for DelimBoundError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "delimiter encountered at word boundary",)
    }
}

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

pub struct NoTEXTWordsError;

impl fmt::Display for NoTEXTWordsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "TEXT has a delimiter and no words",)
    }
}

enum_from_disp!(
    pub ParsePrimaryTEXTError,
    [Keywords, ParseKeywordsIssue],
    [Empty, NoTEXTWordsError]
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

impl fmt::Display for DelimMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "first byte of supplemental TEXT ({}) does not match delimiter of primary TEXT ({})",
            self.supp, self.delim
        )
    }
}

enum_from_disp!(
    pub ParseRawTEXTError,
    [Delim, DelimVerifyError],
    [Primary, ParsePrimaryTEXTError],
    [Supplemental, ParseSupplementalTEXTError],
    [SuppOffsets, ReqSegmentError],
    [Nextdata, ReqKeyError<ParseIntError>],
    [NonAscii, NonAsciiKeyError],
    [NonUtf8, NonUtf8KeywordError]
);

pub struct NonAsciiKeyError(String);

impl fmt::Display for NonAsciiKeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "non-ASCII key encountered and dropped: {}", self.0)
    }
}

pub struct NonUtf8KeywordError {
    key: Vec<u8>,
    value: Vec<u8>,
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

enum_from_disp!(
    pub ParseRawTEXTWarning,
    [Char, DelimCharError],
    [Keywords, ParseKeywordsIssue],
    [SuppOffsets, STextSegmentWarning],
    [Nextdata, ParseKeyError<ParseIntError>]
);

enum_from!(
    pub AnyParseHeaderFailure,
    [File, io::Error],
    [Header, TerminalFailure<(), ImpureError<HeaderError>, HeaderFailure>]
);

enum_from!(
    pub AnyRawTEXTFailure,
    [File, io::Error],
    [Parse, ParseRawTEXTFailure]
);

enum_from_disp!(
    pub HeaderOrRawError,
    [Header, HeaderError],
    [RawTEXT, ParseRawTEXTError]
);

enum_from!(
    pub AnyStdTEXTFailure,
    [Raw, AnyRawTEXTFailure],
    [Std, TerminalFailure<StdTEXTWarning, ParseKeysError, CoreTEXTFailure>]
);

enum_from_disp!(
    pub StdTEXTWarning,
    [Raw, ParseRawTEXTWarning],
    [Std, LookupMeasWarning]
);

enum_from_disp!(
    pub StdTEXTError,
    [Raw, HeaderOrRawError],
    [Std, ParseKeysError]
);

enum_from_disp!(
    pub StdDatasetWarning,
    [Raw, ParseRawTEXTWarning],
    [Std, CoreDatasetFromRawWarning]
);

enum_from_disp!(
    pub StdDatasetError,
    [Raw, HeaderOrRawError],
    [Std, CoreDatasetFromRawError]
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
    [Std, ParseKeysError],
    [Read, KwsToDatasetError]
);

enum_from!(
    pub AnyRawDatasetWarning,
    [Read, ReadRawDatasetWarning]
);

enum_from!(
    pub AnyKwsToRawDatasetFailure,
    [File, io::Error],
    [Read, TerminalFailure<ReadRawDatasetWarning, KwsToDatasetError, ReadRawDatasetFailure>]
);

enum_from_disp!(
    pub KwsToDatasetError,
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
    pub ReadStdDatasetError,
    [DataSegment, DataSegmentError],
    [AnalysisSegment, AnalysisSegmentError],
    [ToReader, StdReaderError],
    [ReadData, ImpureError<ReadDataError>],
    [ReadAnalysis, io::Error]
);

enum_from_disp!(
    pub ReadStdDatasetWarning,
    [DataSeg, DataSegmentWarning],
    [AnalysisSeg, AnalysisSegmentWarning],
    [ToReader, NewDataReaderWarning]
);

pub struct ReadRawDatasetFailure;

pub struct ReadStdDatasetFailure;

enum_from!(
    pub AnyStdDatasetFromRawFailure,
    [File, io::Error],
    [Std, TerminalFailure<LookupMeasWarning, ParseKeysError, CoreTEXTFailure>],
    [Read, TerminalFailure<AnyStdDatasetFromRawWarning, ReadStdDatasetError, ReadStdDatasetFailure>]
);

enum_from_disp!(
    pub ReadRawOrStdWarning,
    [Raw, ParseRawTEXTWarning],
    [Std, LookupMeasWarning]
);

enum_from_disp!(
    pub AnyStdDatasetWarning,
    [Raw, ParseRawTEXTWarning],
    [Std, CoreDatasetFromRawWarning]
);

enum_from_disp!(
    pub AnyStdDatasetFromRawWarning,
    [Text, LookupMeasWarning],
    [Dataset, ReadStdDatasetWarning]
);

pub struct ParseRawTEXTFailure;

pub struct CoreTEXTFailure;

pub struct CoreDatasetFailure;

pub struct HeaderFailure;

pub struct KwsToRawDatasetFailure;

pub struct KwsToStdDatasetFailure;

impl fmt::Display for HeaderFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "could not parse HEADER")
    }
}

impl fmt::Display for ParseRawTEXTFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "could not parse TEXT segment")
    }
}

impl fmt::Display for CoreTEXTFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "could not standardize TEXT segment")
    }
}

impl fmt::Display for CoreDatasetFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "could not read DATA with standardized TEXT")
    }
}

impl fmt::Display for ReadStdDatasetFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "could not read DATA segment")
    }
}

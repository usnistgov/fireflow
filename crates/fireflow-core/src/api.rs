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
///
/// This is derived from the HEADER which should be parsed in order to obtain
/// this.
///
/// The purpose of this is to obtain the TEXT keywords (primary and
/// supplemental) using the least amount of processing, which should increase
/// performance and minimize potential errors thrown if this is what the user
/// desires.
///
/// This will also be used as input downstream to 'standardize' the TEXT segment
/// according to version, and also to parse DATA if either is desired.
#[derive(Serialize)]
pub struct RawTEXT {
    /// FCS Version from HEADER
    pub version: Version,

    /// Keyword pairs from TEXT
    pub keywords: ValidKeywords,

    /// Data used for parsing TEXT which might be used later to parse remainder.
    ///
    /// This will include primary TEXT, DATA, and ANALYSIS offsets as seen in
    /// HEADER. It will also include $BEGIN/ENDSTEXT as found in TEXT (if found)
    /// which will be used to parse the supplemental TEXT segment if it exists.
    ///
    /// $NEXTDATA will also be included if found.
    ///
    /// The delimiter used to parse the keywords will also be included.
    pub parse: ParseData,
}

/// Output of parsing the TEXT segment and standardizing keywords.
///
/// This is derived from ['RawTEXT'].
///
/// The process of "standardization" involves gathering version specific
/// keywords in the TEXT segment and parsing their values such that they
/// conform to the types specified in the standard.
///
/// Version is not included since this is implied by the standardized structs
/// used.
pub struct StandardizedTEXT {
    /// Structured data derived from TEXT specific to the indicated FCS version.
    ///
    /// All keywords that were included in the ['RawTEXT'] used to create this
    /// will be included here. Anything standardized will be put into a field
    /// that can be readily accessed directly and returned with the proper type.
    /// Anything nonstandard will be kept in a hash table whose values will
    /// be strings.
    pub standardized: AnyCoreTEXT,

    /// Raw standard keywords remaining after the standardization process
    ///
    /// This only should include $TOT, $BEGINDATA, $ENDDATA, $BEGINANALISYS, and
    /// $ENDANALYSIS. These are only needed to process the data segment and are
    /// not necessary to create the CoreTEXT, and thus are not included.
    pub remainder: StdKeywords,

    /// Raw keywords that are not standard but start with '$'
    pub deviant: StdKeywords,

    /// Data used for parsing TEXT which might be used later to parse remainder.
    ///
    /// The is analogous to that of [`RawTEXT`] and is copied as-is when
    /// creating this.
    pub parse: ParseData,
}

/// Output of parsing one raw dataset (TEXT+DATA) from an FCS file.
///
/// Computationally this will be created by skipping (most of) the
/// standardization step and instead parsing the minimal-required keywords
/// to parse DATA (BYTEORD, DATATYPE, etc).
pub struct RawDataset {
    /// FCS Version from HEADER
    pub version: Version,

    /// Keyword pairs from TEXT
    pub keywords: ValidKeywords,

    pub dataset: RawParsedDataset,

    /// Data used for parsing the FCS file.
    ///
    /// This will include all offsets, $NEXTDATA (if found) and the TEXT
    /// delimiter. The DATA and ANALYSIS offsets will reflect those actually
    /// used to parse these segments, which may or may not reflect the HEADER.
    pub parse: ParseData,
}

/// Output of parsing one standardized dataset (TEXT+DATA) from an FCS file.
pub struct StandardizedDataset {
    /// Structured data derived from TEXT specific to the indicated FCS version.
    pub dataset: ParsedDataset,

    /// Raw standard keywords remaining after processing.
    ///
    /// This should be empty if everything worked. Here for debugging.
    pub remainder: StdKeywords,

    /// Non-standard keywords that start with '$'.
    pub deviant: StdKeywords,

    /// Data used for parsing the FCS file.
    ///
    /// This will include all offsets, $NEXTDATA (if found) and the TEXT
    /// delimiter. The DATA and ANALYSIS offsets will reflect those actually
    /// used to parse these segments, which may or may not reflect the HEADER.
    pub parse: ParseData,
}

/// Data pertaining to parsing the TEXT segment.
///
/// Includes offsets, TEXT delimiter, $NEXTDATA (if present) and any
/// non-conforming keywords.
#[derive(Clone, Serialize)]
pub struct ParseData {
    /// Primary TEXT offsets
    ///
    /// The offsets that were used to parse the TEXT segment. Included here for
    /// informational purposes.
    pub prim_text: PrimaryTextSegment,

    /// Supplemental TEXT offsets
    ///
    /// This is not needed downstream and included here for informational
    /// purposes. It will always be None for 2.0 which does not include this.
    pub supp_text: Option<SupplementalTextSegment>,

    /// DATA offsets
    ///
    /// The offsets pointing to the DATA segment. When this struct is present
    /// in [RawTEXT] or [StandardizedTEXT], this will reflect what is in the
    /// HEADER. In [StandardizedDataset], this will reflect the values from
    /// $BEGIN/ENDDATA if applicable.
    ///
    /// This will be 0,0 if DATA has no data or if there was an error acquiring
    /// the offsets.
    pub data: HeaderDataSegment,

    /// ANALYSIS offsets.
    ///
    /// The meaning of this is analogous to [data] above.
    pub analysis: HeaderAnalysisSegment,

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
) -> Result<Header, AnyParseHeaderFailure> {
    let file = fs::File::options().read(true).open(p)?;
    let mut reader = BufReader::new(file);
    let header = h_read_header(&mut reader, conf)?;
    Ok(header)
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
) -> Result<Terminal<RawTEXT, ParseRawTEXTWarning>, AnyRawTEXTFailure> {
    let file = fs::File::options().read(true).open(p)?;
    let mut h = BufReader::new(file);
    let raw = RawTEXT::h_read(&mut h, conf)?;
    Ok(raw)
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
) -> Result<Terminal<StandardizedTEXT, AnyStdTEXTWarning>, AnyStdTEXTFailure> {
    let term_raw = read_fcs_raw_text(p, &conf.raw)?;
    term_raw
        .warnings_into()
        .and_finally(|raw| raw.into_std(conf).term_warnings_into())
        .map_err(|e| e.into())
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
    p: path::PathBuf,
    conf: &DataReadConfig,
) -> Result<Terminal<RawDataset, AnyRawDatasetWarning>, AnyRawDatasetFailure> {
    let file = fs::File::options().read(true).open(p)?;
    let mut h = BufReader::new(file);
    let term_raw = RawTEXT::h_read(&mut h, &conf.standard.raw)?;
    term_raw
        .warnings_into()
        .and_finally(|raw| h_read_raw_dataset(&mut h, raw, conf).term_warnings_into())
        .map_err(|e| e.into())
}

pub fn read_fcs_raw_file_from_raw(
    p: path::PathBuf,
    version: Version,
    std: &StdKeywords,
    conf: &DataReadConfig,
    data_seg: HeaderDataSegment,
    analysis_seg: HeaderAnalysisSegment,
) -> Result<Terminal<RawParsedDataset, AnyRawDatasetWarning>, AnyRawDatasetFailure> {
    let file = fs::File::options().read(true).open(p)?;
    let mut h = BufReader::new(file);
    h_read_raw_dataset_from_raw(&mut h, version, std, data_seg, analysis_seg, conf)
        .term_warnings_into()
        .map_err(|e| e.into())
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
) -> Result<Terminal<StandardizedDataset, AnyStdDatasetWarning>, AnyStdDatasetFailure> {
    let file = fs::File::options().read(true).open(p)?;
    let mut h = BufReader::new(file);
    let term_raw = RawTEXT::h_read(&mut h, &conf.standard.raw)?;
    let term_std = term_raw
        .warnings_into()
        .and_finally(|raw| raw.into_std(&conf.standard).term_warnings_into())?;
    term_std
        .warnings_into()
        .and_finally(|std| h_read_std_dataset(&mut h, std, conf).term_warnings_into())
        .map_err(|e| e.into())
}

pub fn read_fcs_std_file_from_raw(
    p: &path::PathBuf,
    version: Version,
    mut std: StdKeywords,
    nonstd: NonStdKeywords,
    conf: &DataReadConfig,
    data_seg: HeaderDataSegment,
    analysis_seg: HeaderAnalysisSegment,
) -> Result<Terminal<StdDatasetFromRaw, AnyStdDatasetFromRawWarning>, AnyStdDatasetFromRawFailure> {
    let file = fs::File::options().read(true).open(p)?;
    let mut h = BufReader::new(file);
    let term_core =
        AnyCoreTEXT::parse_raw(version, &mut std, nonstd, &conf.standard).term_warnings_into()?;
    term_core
        .warnings_into()
        .and_finally(|core| {
            h_read_std_dataset_from_core(&mut h, core, &std, data_seg, analysis_seg, conf)
                .term_warnings_into()
        })
        .term_map_value(|parsed| {
            let (remainder, deviant) = split_remainder(std);
            StdDatasetFromRaw {
                parsed,
                deviant,
                remainder,
            }
        })
        .map_err(|e| e.into())
}

pub struct StdDatasetFromRaw {
    pub parsed: ParsedDataset,
    pub deviant: StdKeywords,
    // TODO this is confusing
    pub remainder: StdKeywords,
}

pub struct ParsedDataset {
    pub core: AnyCoreDataset,
    pub data_seg: AnyDataSegment,
    pub analysis_seg: AnyAnalysisSegment,
}

pub struct RawParsedDataset {
    pub data: FCSDataFrame,
    pub analysis: Analysis,
    pub data_seg: AnyDataSegment,
    pub analysis_seg: AnyAnalysisSegment,
}

// pub struct DatasetFromRaw {
//     remainder: StdKeywords,
//     deviant: StdKeywords,
//     dataset: AnyCoreDataset,
// }

fn h_read_raw_dataset<R: Read + Seek>(
    h: &mut BufReader<R>,
    raw: RawTEXT,
    conf: &DataReadConfig,
) -> TerminalResult<RawDataset, ReadRawDatasetWarning, ReadRawDatasetError, ReadRawDatasetFailure> {
    let version = raw.version;

    h_read_raw_dataset_from_raw(
        h,
        version,
        &raw.keywords.std,
        raw.parse.data,
        raw.parse.analysis,
        conf,
    )
    .term_map_value(|dataset| RawDataset {
        version,
        keywords: raw.keywords,
        dataset,
        parse: raw.parse,
    })
}

fn h_read_std_dataset<R: Read + Seek>(
    h: &mut BufReader<R>,
    std: StandardizedTEXT,
    conf: &DataReadConfig,
) -> TerminalResult<
    StandardizedDataset,
    ReadStdDatasetWarning,
    ReadStdDatasetError,
    ReadStdDatasetFailure,
> {
    h_read_std_dataset_from_core(
        h,
        std.standardized,
        &std.remainder,
        std.parse.data,
        std.parse.analysis,
        conf,
    )
    .term_map_value(|dataset| StandardizedDataset {
        remainder: std.remainder,
        deviant: std.deviant,
        dataset,
        parse: std.parse,
    })
}

fn h_read_raw_dataset_from_raw<R: Read + Seek>(
    h: &mut BufReader<R>,
    version: Version,
    std: &StdKeywords,
    def_data_seg: HeaderDataSegment,
    def_analysis_seg: HeaderAnalysisSegment,
    conf: &DataReadConfig,
) -> TerminalResult<
    RawParsedDataset,
    ReadRawDatasetWarning,
    ReadRawDatasetError,
    ReadRawDatasetFailure,
> {
    let tnt_anal = lookup_analysis_offsets(std, conf, version, def_analysis_seg).inner_into();

    let reader_res = lookup_data_offsets(std, conf, version, def_data_seg)
        .inner_into()
        .and_maybe(|data_seg| {
            kws_to_reader(version, std, data_seg, conf)
                .inner_into()
                .map_value(|reader| (reader, data_seg))
        });

    reader_res
        .and_tentatively(|x| tnt_anal.map(|y| (x, y)))
        .and_maybe(|((reader, data_seg), analysis_seg)| {
            let data = reader
                .h_read(h)
                .map_err(|e| DeferredFailure::new1(e.into()))?;
            let analysis =
                h_read_analysis(h, analysis_seg).map_err(|e| DeferredFailure::new1(e.into()))?;
            let ret = RawParsedDataset {
                data,
                analysis,
                data_seg,
                analysis_seg,
            };
            Ok(Tentative::new1(ret))
        })
        .terminate(ReadRawDatasetFailure)
}

fn h_read_std_dataset_from_core<R: Read + Seek>(
    h: &mut BufReader<R>,
    core: AnyCoreTEXT,
    kws: &StdKeywords,
    def_data_seg: HeaderDataSegment,
    def_anal_seg: HeaderAnalysisSegment,
    conf: &DataReadConfig,
) -> TerminalResult<ParsedDataset, ReadStdDatasetWarning, ReadStdDatasetError, ReadStdDatasetFailure>
{
    let version = core.version();

    let tnt_anal = lookup_analysis_offsets(kws, conf, version, def_anal_seg).inner_into();

    let reader_res = lookup_data_offsets(kws, conf, version, def_data_seg)
        .inner_into()
        .and_maybe(|data_seg| {
            core.as_data_reader(kws, conf, data_seg)
                .inner_into()
                .map_value(|reader| (reader, data_seg))
        });

    reader_res
        .and_tentatively(|x| tnt_anal.map(|y| (x, y)))
        .and_maybe(|((reader, data_seg), analysis_seg)| {
            let columns = reader
                .h_read(h)
                .map_err(|e| DeferredFailure::new1(e.into()))?;
            let analysis =
                h_read_analysis(h, analysis_seg).map_err(|e| DeferredFailure::new1(e.into()))?;
            let pd = ParsedDataset {
                core: core.into_coredataset_unchecked(columns, analysis),
                data_seg,
                analysis_seg,
            };
            Ok(Tentative::new1(pd))
        })
        .terminate(ReadStdDatasetFailure)
}

impl RawTEXT {
    fn h_read<R: Read + Seek>(
        h: &mut BufReader<R>,
        conf: &RawTextReadConfig,
    ) -> Result<Terminal<Self, ParseRawTEXTWarning>, HeaderOrRawFailure> {
        let header = h_read_header(h, &conf.header)?;
        let raw = h_read_raw_text_from_header(h, &header, conf)?;
        Ok(raw)
    }

    fn into_std(
        self,
        conf: &StdTextReadConfig,
    ) -> TerminalResult<StandardizedTEXT, LookupMeasWarning, ParseKeysError, CoreTEXTFailure> {
        let mut kws = self.keywords;
        AnyCoreTEXT::parse_raw(self.version, &mut kws.std, kws.nonstd, conf).term_map_value(
            |standardized| {
                let (remainder, deviant) = split_remainder(kws.std);
                StandardizedTEXT {
                    parse: self.parse,
                    standardized,
                    remainder,
                    deviant,
                }
            },
        )
    }
}

fn kws_to_reader(
    version: Version,
    kws: &StdKeywords,
    data_seg: AnyDataSegment,
    conf: &DataReadConfig,
) -> DeferredResult<DataReader, RawToReaderWarning, RawToReaderError> {
    let sc = &conf.shared;
    match version {
        Version::FCS2_0 => DataLayout2_0::try_new_from_raw(kws, sc)
            .inner_into()
            .and_maybe(|dl| dl.into_reader(kws, data_seg, conf).inner_into()),
        Version::FCS3_0 => DataLayout3_0::try_new_from_raw(kws, sc)
            .inner_into()
            .and_maybe(|dl| dl.into_reader(kws, data_seg, conf).inner_into()),
        Version::FCS3_1 => DataLayout3_1::try_new_from_raw(kws, sc)
            .inner_into()
            .and_maybe(|dl| dl.into_reader(kws, data_seg, conf).inner_into()),
        Version::FCS3_2 => DataLayout3_2::try_new_from_raw(kws, sc)
            .inner_into()
            .and_maybe(|dl| dl.into_reader(kws, data_seg, conf).inner_into()),
    }
    .map(|x| {
        x.map(|column_reader| DataReader {
            column_reader,
            begin: data_seg.inner.begin().into(),
        })
    })
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

fn lookup_req_segment<I: Into<SegmentId> + Clone + Copy>(
    kws: &StdKeywords,
    bk: &StdKey,
    ek: &StdKey,
    corr: OffsetCorrection,
    id: I,
) -> MultiResult<SpecificSegment<I, SegmentFromTEXT>, ReqSegmentError> {
    let x0 = get_req(kws, bk).map_err(|e| e.into());
    let x1 = get_req(kws, ek).map_err(|e| e.into());
    x0.zip(x1).and_then(|(begin, end)| {
        SpecificSegment::try_new(begin, end, corr, id, SegmentFromTEXT).into_mult()
    })
}

fn lookup_opt_segment<I: Into<SegmentId> + Clone + Copy>(
    kws: &StdKeywords,
    bk: &StdKey,
    ek: &StdKey,
    corr: OffsetCorrection,
    id: I,
) -> MultiResult<Option<SpecificSegment<I, SegmentFromTEXT>>, OptSegmentError> {
    let x0 = get_opt(kws, bk).map_err(|e| e.into());
    let x1 = get_opt(kws, ek).map_err(|e| e.into());
    x0.zip(x1).and_then(|(b, e)| {
        b.zip(e)
            .map(|(begin, end)| {
                SpecificSegment::try_new(begin, end, corr, id, SegmentFromTEXT).into_mult()
            })
            .transpose()
    })
}

// TODO unclear if these next two functions should throw errors or warnings
// on failure
fn lookup_data_offsets(
    kws: &StdKeywords,
    conf: &DataReadConfig,
    version: Version,
    default: HeaderDataSegment,
) -> Tentative<AnyDataSegment, DataSegmentWarning, DataSegmentError> {
    let d = SegmentDefaultWarning(DataSegmentId);
    match version {
        Version::FCS2_0 => Tentative::new1(default.into_any()),
        _ => lookup_req_segment(
            kws,
            &Begindata::std(),
            &Enddata::std(),
            conf.data,
            DataSegmentId,
        )
        .map_or_else(
            |es| {
                if conf.standard.raw.enforce_required_offsets {
                    let ws = es.map(|e| e.into()).into_iter().chain([d.into()]).collect();
                    Tentative::new(default.into_any(), vec![], ws)
                } else {
                    let ws = es.map(|e| e.into()).into_iter().chain([d.into()]).collect();
                    Tentative::new(default.into_any(), ws, vec![])
                }
            },
            |t| {
                let w = if t.inner != default.inner && !default.inner.is_empty() {
                    Some(SegmentMismatchWarning {
                        header: default,
                        text: t,
                    })
                } else {
                    None
                };
                if conf.standard.raw.enforce_offset_match {
                    let xs = w.map(|x| x.into()).into_iter().collect();
                    Tentative::new(t.into_any(), vec![], xs)
                } else {
                    let xs = w.map(|x| x.into()).into_iter().collect();
                    Tentative::new(t.into_any(), xs, vec![])
                }
            },
        ),
    }
}

fn lookup_analysis_offsets(
    kws: &StdKeywords,
    conf: &DataReadConfig,
    version: Version,
    default: HeaderAnalysisSegment,
) -> Tentative<AnyAnalysisSegment, AnalysisSegmentWarning, AnalysisSegmentError> {
    let d = SegmentDefaultWarning(AnalysisSegmentId);
    let def_any = default.into_any();
    match version {
        Version::FCS2_0 => Tentative::new1(def_any),

        Version::FCS3_0 | Version::FCS3_1 => lookup_req_segment(
            kws,
            &Beginanalysis::std(),
            &Endanalysis::std(),
            conf.analysis,
            AnalysisSegmentId,
        )
        .map_or_else(
            |es| {
                if conf.standard.raw.enforce_required_offsets {
                    let ws = es.map(|e| e.into()).into_iter().chain([d.into()]).collect();
                    Tentative::new(def_any, vec![], ws)
                } else {
                    let ws = es.map(|e| e.into()).into_iter().chain([d.into()]).collect();
                    Tentative::new(def_any, ws, vec![])
                }
            },
            |t| Tentative::new1(t.into_any()),
        ),

        Version::FCS3_2 => lookup_opt_segment(
            kws,
            &Beginanalysis::std(),
            &Endanalysis::std(),
            conf.analysis,
            AnalysisSegmentId,
        )
        .map_or_else(
            |es| {
                // unlike the above, this can never error because the keywords
                // are optional
                let ws = es
                    .map(|e| e.into())
                    .into_iter()
                    .chain([SegmentDefaultWarning(AnalysisSegmentId).into()])
                    .collect();
                Tentative::new(def_any, ws, vec![])
            },
            |t| {
                if let Some(this_seg) = t {
                    let w = if this_seg.inner != default.inner && !default.inner.is_empty() {
                        Some(SegmentMismatchWarning {
                            header: default,
                            text: this_seg,
                        })
                    } else {
                        None
                    };
                    let this_any = this_seg.into_any();
                    if conf.standard.raw.enforce_offset_match {
                        let xs = w.map(|x| x.into()).into_iter().collect();
                        Tentative::new(this_any, vec![], xs)
                    } else {
                        let xs = w.map(|x| x.into()).into_iter().collect();
                        Tentative::new(this_any, xs, vec![])
                    }
                } else {
                    Tentative::new1(t.map(|x| x.into_any()).unwrap_or(def_any))
                }
            },
        ),
    }
}

fn lookup_stext_offsets(
    kws: &StdKeywords,
    version: Version,
    conf: &RawTextReadConfig,
) -> Tentative<Option<SupplementalTextSegment>, STextSegmentWarning, ReqSegmentError> {
    match version {
        Version::FCS2_0 => Tentative::new1(None),
        Version::FCS3_0 | Version::FCS3_1 => lookup_req_segment(
            kws,
            &Beginstext::std(),
            &Endstext::std(),
            conf.stext,
            SupplementalTextSegmentId,
        )
        .map_or_else(
            |es| {
                if conf.enforce_stext {
                    Tentative::new(None, vec![], es.into())
                } else {
                    Tentative::new(None, es.map(|e| e.into()).into(), vec![])
                }
            },
            |t| Tentative::new1(Some(t)),
        ),
        Version::FCS3_2 => lookup_opt_segment(
            kws,
            &Beginstext::std(),
            &Endstext::std(),
            conf.stext,
            SupplementalTextSegmentId,
        )
        .map_or_else(
            |es| Tentative::new(None, es.map(|e| e.into()).into(), vec![]),
            Tentative::new1,
        ),
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
    header: &Header,
    conf: &RawTextReadConfig,
) -> TerminalResult<RawTEXT, ParseRawTEXTWarning, ImpureError<ParseRawTEXTError>, ParseRawTEXTFailure>
{
    let mut buf = vec![];
    header
        .text
        .inner
        .h_read(h, &mut buf)
        .map_err(|e| DeferredFailure::new1(e.into()).terminate(ParseRawTEXTFailure))?;

    let term_delim = split_first_delim(&buf, conf)
        .inner_into()
        .error_impure()
        .terminate(ParseRawTEXTFailure)?;

    let term_primary = term_delim.and_maybe(ParseRawTEXTFailure, |(delim, bytes)| {
        let kws = ParsedKeywords::default();
        split_raw_primary_text(kws, delim, bytes, conf)
            .inner_into()
            .error_impure()
            .map_value(|_kws| repair_offsets(_kws, conf))
            .map_value(|_kws| (delim, _kws))
    })?;

    let term_all_kws = term_primary.and_finally(|(delim, kws)| {
        lookup_stext_offsets(&kws.std, header.version, conf)
            .errors_into()
            .errors_map(ImpureError::Pure)
            .warnings_into()
            .map(|s| (s, kws))
            .and_finally(|(maybe_supp_seg, _kws)| {
                let term_supp_kws = if let Some(seg) = maybe_supp_seg {
                    buf.clear();
                    seg.inner.h_read(h, &mut buf).map_err(|e| {
                        DeferredFailure::new1(e.into()).terminate(ParseRawTEXTFailure)
                    })?;
                    split_raw_supp_text(_kws, delim, &buf, conf)
                        .inner_into()
                        .error_impure()
                        .terminate(ParseRawTEXTFailure)?
                } else {
                    Terminal::new(_kws)
                };
                Ok(term_supp_kws.map(|k| (delim, k, maybe_supp_seg)))
            })
    })?;

    term_all_kws.and_tentatively(
        ParseRawTEXTFailure,
        |(delimiter, mut kws, supp_text_seg)| {
            repair_keywords(&mut kws.std, conf);
            let mut tnt_parse = lookup_nextdata(&kws.std, conf.enforce_nextdata)
                .errors_into()
                .map(|nextdata| ParseData {
                    prim_text: header.text,
                    supp_text: supp_text_seg,
                    data: header.data,
                    analysis: header.analysis,
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
                .map(|parse| RawTEXT {
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
        },
    )
}

fn split_remainder(xs: StdKeywords) -> (StdKeywords, StdKeywords) {
    xs.into_iter()
        .map(|(k, v)| {
            if k == Tot::std()
                || k == Begindata::std()
                || k == Enddata::std()
                || k == Beginanalysis::std()
                || k == Endanalysis::std()
            {
                Ok((k, v))
            } else {
                Err((k, v))
            }
        })
        .partition_result()
}

enum_from_disp!(
    pub ReqSegmentError,
    [Key, ReqKeyError<ParseIntError>],
    [Segment, SegmentError]
);

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
    pub OptSegmentError,
    [Key, ParseKeyError<ParseIntError>],
    [Segment, SegmentError]
);

pub struct SegmentMismatchWarning<S> {
    header: SpecificSegment<S, SegmentFromHeader>,
    text: SpecificSegment<S, SegmentFromTEXT>,
}

impl<S> fmt::Display for SegmentMismatchWarning<S>
where
    S: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "segments differ in HEADER ({}) and TEXT ({}) for {}, using TEXT",
            self.header.inner.fmt_pair(),
            self.text.inner.fmt_pair(),
            self.header.id(),
        )
    }
}

pub struct SegmentDefaultWarning<S>(S);

impl<S> fmt::Display for SegmentDefaultWarning<S>
where
    S: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "could not obtain {} segment offset from TEXT, \
             using offsets from HEADER",
            self.0
        )
    }
}

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

pub struct ParseRawTEXTFailure;

enum_from!(
    pub AnyParseHeaderFailure,
    [File, io::Error],
    [Header, TerminalFailure<(), ImpureError<HeaderError>, HeaderFailure>]
);

enum_from!(
    pub AnyRawTEXTFailure,
    [File, io::Error],
    [Parse, HeaderOrRawFailure]
);

// TODO could nest the header bits better here
enum_from!(
    pub HeaderOrRawFailure,
    [Header, TerminalFailure<(), ImpureError<HeaderError>, HeaderFailure>],
    [RawTEXT, TerminalFailure<ParseRawTEXTWarning, ImpureError<ParseRawTEXTError>, ParseRawTEXTFailure>]
);

impl fmt::Display for ParseRawTEXTFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "could not parse TEXT segment")
    }
}

enum_from!(
    pub AnyStdTEXTFailure,
    [Raw, AnyRawTEXTFailure],
    [Std, TerminalFailure<AnyStdTEXTWarning, ParseKeysError, CoreTEXTFailure>]
);

impl fmt::Display for CoreTEXTFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "could not standardize TEXT segment")
    }
}

enum_from_disp!(
    pub AnyStdTEXTWarning,
    [Raw, ParseRawTEXTWarning],
    [Std, LookupMeasWarning]
);

enum_from!(
    pub AnyRawDatasetFailure,
    [File, io::Error],
    [Raw, HeaderOrRawFailure],
    [Read, TerminalFailure<AnyRawDatasetWarning, ReadRawDatasetError, ReadRawDatasetFailure>]
);

enum_from!(
    pub AnyRawDatasetWarning,
    [Raw, ParseRawTEXTWarning],
    [Read, ReadRawDatasetWarning]
);

enum_from_disp!(
    pub ReadRawDatasetError,
    [DataSegment, DataSegmentError],
    [AnalysisSegment, AnalysisSegmentError],
    [ToReader, RawToReaderError],
    [ReadData, ImpureError<ReadDataError>],
    [ReadAnalysis, io::Error]
);

enum_from_disp!(
    pub ReadRawDatasetWarning,
    [DataSeg, DataSegmentWarning],
    [AnalysisSeg, AnalysisSegmentWarning],
    [ToReader, RawToReaderWarning]
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
    [ToReader, NewReaderWarning]
);

pub struct ReadRawDatasetFailure;

pub struct ReadStdDatasetFailure;

enum_from!(
    pub AnyStdDatasetFailure,
    [File, io::Error],
    [Raw, HeaderOrRawFailure],
    [Std, TerminalFailure<ReadRawOrStdWarning, ParseKeysError, CoreTEXTFailure>],
    [Read, TerminalFailure<AnyStdDatasetWarning, ReadStdDatasetError, ReadStdDatasetFailure>]
);

enum_from!(
    pub AnyStdDatasetFromRawFailure,
    [File, io::Error],
    [Std, TerminalFailure<LookupMeasWarning, ParseKeysError, CoreTEXTFailure>],
    [Read, TerminalFailure<AnyStdDatasetFromRawWarning, ReadStdDatasetError, ReadStdDatasetFailure>]
);

impl fmt::Display for ReadStdDatasetFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "could not read DATA segment")
    }
}

enum_from_disp!(
    pub ReadRawOrStdWarning,
    [Raw, ParseRawTEXTWarning],
    [Std, LookupMeasWarning]
);

enum_from_disp!(
    pub AnyStdDatasetWarning,
    [Raw, ReadRawOrStdWarning],
    [Std, ReadStdDatasetWarning]
);

enum_from_disp!(
    pub AnyStdDatasetFromRawWarning,
    [Text, LookupMeasWarning],
    [Dataset, ReadStdDatasetWarning]
);

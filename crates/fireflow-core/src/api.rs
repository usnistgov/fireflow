use crate::config::*;
pub use crate::core::*;
pub use crate::data::*;
use crate::error::*;
pub use crate::header::*;
pub use crate::header_text::*;
pub use crate::segment::*;
pub use crate::text::keywords::*;
use crate::text::timestamps::*;
use crate::validated::dataframe::FCSDataFrame;

use chrono::NaiveDate;
use itertools::Itertools;
use serde::Serialize;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fs;
use std::io::{BufReader, Read, Seek};
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
#[derive(Clone, Serialize)]
pub struct RawTEXT {
    /// FCS Version from HEADER
    pub version: Version,

    /// Keyword pairs from TEXT
    pub keywords: AllKeywords,

    /// Data used for parsing TEXT which might be used later to parse remainder.
    ///
    /// This will include primary TEXT, DATA, and ANALYSIS offsets as seen in
    /// HEADER. It will also include $BEGIN/ENDSTEXT as found in TEXT (if found)
    /// which will be used to parse the supplemental TEXT segment if it exists.
    ///
    /// $NEXTDATA will also be included if found.
    ///
    /// The delimiter used to parse the keywords will also be included.
    pub parse: ParseParameters,
    // TODO also include the junk that couldn't be parsed in here somewhere
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
#[derive(Clone)]
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
    pub parse: ParseParameters,
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
    pub keywords: AllKeywords,

    /// DATA segment as a polars DataFrame
    ///
    /// The type of each column is such that each measurement is encoded with
    /// zero loss. This will/should never contain NULL values despite the
    /// underlying arrow framework allowing NULLs to exist.
    pub data: FCSDataFrame,

    /// ANALYSIS segment
    ///
    /// This will be empty if ANALYSIS either doesn't exist or the computation
    /// fails. This has not standard structure, so the best we can capture is a
    /// byte sequence.
    pub analysis: Analysis,

    /// Data used for parsing the FCS file.
    ///
    /// This will include all offsets, $NEXTDATA (if found) and the TEXT
    /// delimiter. The DATA and ANALYSIS offsets will reflect those actually
    /// used to parse these segments, which may or may not reflect the HEADER.
    pub parse: ParseParameters,
}

/// Output of parsing one standardized dataset (TEXT+DATA) from an FCS file.
#[derive(Clone)]
pub struct StandardizedDataset {
    /// Structured data derived from TEXT specific to the indicated FCS version.
    pub dataset: AnyCoreDataset,

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
    pub parse: ParseParameters,
}

/// Parameters used to parse the FCS file.
///
/// Includes offsets, TEXT delimiter, and $NEXTDATA (if present).
#[derive(Clone, Serialize)]
pub struct ParseParameters {
    /// Primary TEXT offsets
    ///
    /// The offsets that were used to parse the TEXT segment. Included here for
    /// informational purposes.
    pub prim_text: Segment,

    /// Supplemental TEXT offsets
    ///
    /// This is not needed downstream and included here for informational
    /// purposes. It will always be None for 2.0 which does not include this.
    pub supp_text: Option<Segment>,

    /// DATA offsets
    ///
    /// The offsets pointing to the DATA segment. When this struct is present
    /// in [RawTEXT] or [StandardizedTEXT], this will reflect what is in the
    /// HEADER. In [StandardizedDataset], this will reflect the values from
    /// $BEGIN/ENDDATA if applicable.
    ///
    /// This will be 0,0 if DATA has no data or if there was an error acquiring
    /// the offsets.
    pub data: Segment,

    /// ANALYSIS offsets.
    ///
    /// The meaning of this is analogous to [data] above.
    pub analysis: Segment,

    /// NEXTDATA offset
    ///
    /// This will be copied as represented in TEXT. If it is 0, there is no next
    /// dataset, otherwise it points to the next dataset in the file.
    pub nextdata: Option<u32>,

    /// Delimiter used to parse TEXT.
    ///
    /// Included here for informational purposes.
    pub delimiter: u8,
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
pub fn read_fcs_header(p: &path::PathBuf, conf: &HeaderConfig) -> ImpureResult<Header> {
    let file = fs::File::options().read(true).open(p)?;
    let mut reader = BufReader::new(file);
    h_read_header(&mut reader, conf)
}

/// Return header and raw key/value metadata pairs in an FCS file.
///
/// First will parse the header according to [`read_fcs_header`]. If this fails
/// an error will be returned.
///
/// Next will use the offset information in the header to parse the TEXT segment
/// for key/value pairs. On success will return these pairs as-is using Strings
/// in a HashMap. No other processing will be performed.
pub fn read_fcs_raw_text(p: &path::PathBuf, conf: &RawTextReadConfig) -> ImpureResult<RawTEXT> {
    let file = fs::File::options().read(true).open(p)?;
    let mut h = BufReader::new(file);
    RawTEXT::h_read(&mut h, conf)
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
) -> ImpureResult<StandardizedTEXT> {
    let raw_succ = read_fcs_raw_text(p, &conf.raw)?;
    let out = raw_succ.try_map(|raw| raw.into_std(conf))?;
    Ok(out)
}

/// Return header, raw metadata, and data in an FCS file.
///
/// In contrast to [`read_fcs_file`], this will return the keywords as a flat
/// list of key/value pairs. Only the bare minimum of these will be read in
/// order to determine how to parse the DATA segment (including $DATATYPE,
/// $BYTEORD, etc). No other checks will be performed to ensure the metadata
/// conforms to the FCS standard version indicated in the header.
///
/// This might be useful for applications where one does not necessarily need
/// the strict structure of the standardized metadata, or if one does not care
/// too much about the degree to which the metadata conforms to standard.
///
/// Other than this, behavior is identical to [`read_fcs_file`],
pub fn read_fcs_raw_file(p: path::PathBuf, conf: &DataReadConfig) -> ImpureResult<RawDataset> {
    let file = fs::File::options().read(true).open(p)?;
    let mut h = BufReader::new(file);
    RawTEXT::h_read(&mut h, &conf.standard.raw)?
        .try_map(|raw| h_read_raw_dataset(&mut h, raw, conf))
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
pub fn read_fcs_file(
    p: &path::PathBuf,
    conf: &DataReadConfig,
) -> ImpureResult<StandardizedDataset> {
    let file = fs::File::options().read(true).open(p)?;
    let mut h = BufReader::new(file);
    RawTEXT::h_read(&mut h, &conf.standard.raw)?
        .try_map(|raw| raw.into_std(&conf.standard))?
        .try_map(|std| h_read_std_dataset(&mut h, std, conf))
}

fn h_read_raw_dataset<R: Read + Seek>(
    h: &mut BufReader<R>,
    raw: RawTEXT,
    conf: &DataReadConfig,
) -> ImpureResult<RawDataset> {
    let anal_succ =
        lookup_analysis_offsets(&raw.keywords.std, conf, raw.version, &raw.parse.analysis);
    lookup_data_offsets(&raw.keywords.std, conf, raw.version, &raw.parse.data)
        .and_then(|data_seg| {
            raw.as_reader(data_seg)
                .combine(anal_succ, |reader, analysis_seg| {
                    (reader, data_seg, analysis_seg)
                })
        })
        .try_map(|(reader_maybe, data_seg, analysis_seg)| {
            let dmsg = "could not create data parser".to_string();
            let data_parser = reader_maybe.ok_or(Failure::new(dmsg))?;
            let data = data_parser.h_read(h)?;
            let analysis = h_read_analysis(h, &analysis_seg)?;
            Ok(PureSuccess::from(RawDataset {
                version: raw.version,
                keywords: raw.keywords,
                data,
                analysis,
                parse: ParseParameters {
                    data: data_seg,
                    analysis: analysis_seg,
                    ..raw.parse
                },
            }))
        })
}

fn h_read_std_dataset<R: Read + Seek>(
    h: &mut BufReader<R>,
    std: StandardizedTEXT,
    conf: &DataReadConfig,
) -> ImpureResult<StandardizedDataset> {
    let mut kws = std.remainder;
    let version = std.standardized.version();
    let anal_succ = lookup_analysis_offsets(&kws, conf, version, &std.parse.analysis);
    lookup_data_offsets(&kws, conf, version, &std.parse.data)
        .and_then(|data_seg| {
            std.standardized
                .as_data_reader(&mut kws, conf, data_seg)
                .combine(anal_succ, |data_parser, analysis_seg| {
                    (data_parser, data_seg, analysis_seg)
                })
        })
        .try_map(|(reader_maybe, data_seg, analysis_seg)| {
            let dmsg = "could not create data reader".to_string();
            let reader = reader_maybe.ok_or(Failure::new(dmsg))?;
            let columns = reader.h_read(h)?;
            let analysis = h_read_analysis(h, &analysis_seg)?;
            let dataset =
                AnyCoreDataset::from_coretext_unchecked(std.standardized, columns, analysis);
            Ok(PureSuccess::from(StandardizedDataset {
                parse: ParseParameters {
                    data: data_seg,
                    analysis: analysis_seg,
                    ..std.parse
                },
                remainder: kws,
                // ASSUME we have checked that the dataframe has the same number
                // of columns as number of measurements; therefore, this should
                // not fail.
                dataset,
                deviant: std.deviant,
            }))
        })
}

impl RawTEXT {
    fn h_read<R: Read + Seek>(
        h: &mut BufReader<R>,
        conf: &RawTextReadConfig,
    ) -> ImpureResult<Self> {
        h_read_header(h, &conf.header)?
            .try_map(|header| h_read_raw_text_from_header(h, &header, conf))
    }

    fn into_std(self, conf: &StdTextReadConfig) -> PureResult<StandardizedTEXT> {
        let mut kws = self.keywords;
        AnyCoreTEXT::parse_raw(self.version, &mut kws.std, kws.nonstd, conf).map(|std_succ| {
            std_succ.map({
                |standardized| {
                    let (remainder, deviant) = split_remainder(kws.std);
                    StandardizedTEXT {
                        parse: self.parse,
                        standardized,
                        remainder,
                        deviant,
                    }
                }
            })
        })
    }

    fn as_reader(&self, data_seg: Segment) -> PureMaybe<DataReader> {
        let kws = &self.keywords.std;
        match self.version {
            // TODO clean this up
            Version::FCS2_0 => {
                let res = DataLayout2_0::try_new_from_raw(kws);
                PureMaybe::from_result_errors(res).and_then_opt(|dl| dl.into_reader(kws, data_seg))
            }
            Version::FCS3_0 => {
                let res = DataLayout3_0::try_new_from_raw(kws);
                PureMaybe::from_result_errors(res).and_then_opt(|dl| dl.into_reader(kws, data_seg))
            }
            Version::FCS3_1 => {
                let res = DataLayout3_1::try_new_from_raw(kws);
                PureMaybe::from_result_errors(res).and_then_opt(|dl| dl.into_reader(kws, data_seg))
            }
            Version::FCS3_2 => {
                let res = DataLayout3_2::try_new_from_raw(kws);
                PureMaybe::from_result_errors(res).and_then_opt(|dl| dl.into_reader(kws, data_seg))
            }
        }
        .map(|x| {
            x.map(|column_reader| DataReader {
                column_reader,
                // TODO fix cast
                begin: data_seg.begin() as u64,
            })
        })
    }
}

fn verify_delim(xs: &[u8], conf: &RawTextReadConfig) -> PureSuccess<u8> {
    // First character is the delimiter
    let delimiter: u8 = xs[0];

    // Check that it is a valid UTF8 character
    //
    // TODO we technically don't need this to be true in the case of double
    // delimiters, but this is non-standard anyways and probably rare
    let mut res = PureSuccess::from(delimiter);
    if String::from_utf8(vec![delimiter]).is_err() {
        res.push_error(format!(
            "Delimiter {delimiter} is not a valid utf8 character"
        ));
    }

    // Check that the delim is valid; this is technically only written in the
    // spec for 3.1+ but for older versions this should still be true since
    // these were ASCII-everywhere
    if !(1..=126).contains(&delimiter) {
        let msg = format!("Delimiter {delimiter} is not an ASCII character b/t 1-126");
        res.push_msg_leveled(msg, conf.force_ascii_delim);
    }
    res
}

fn split_raw_text_nodouble(
    mut kws: AllKeywords,
    xs: &[u8],
    delim: u8,
    conf: &RawTextReadConfig,
) -> PureSuccess<AllKeywords> {
    let mut deferred = PureErrorBuf::default();

    // ASSUME input slice does not start or end with delim
    let mut it = xs.split(|x| *x == delim);
    let mut prev_was_blank = false;
    let mut prev_was_key = false;

    while let Some(key) = it.next() {
        prev_was_key = true;
        prev_was_blank = key.is_empty();
        if key.is_empty() {
            if let Some(value) = it.next() {
                prev_was_key = false;
                prev_was_blank = value.is_empty();
                let msg = "blank key in TEXT, skipping next value".to_string();
                deferred.push_msg_leveled(msg, conf.enforce_nonempty);
            } else {
                // if everything is correct, we should exit here since the
                // last word will be the blank slice after the final delim
                break;
            }
        } else if let Some(value) = it.next() {
            prev_was_key = false;
            prev_was_blank = value.is_empty();
            if value.is_empty() {
                let msg = "blank value in TEXT".to_string();
                deferred.push_msg_leveled(msg, conf.enforce_nonempty)
            } else if let Err(e) = kws.insert(key, value) {
                deferred.push_msg_leveled(e.to_string(), conf.enforce_unique);
            }
        } else {
            // exiting here means we found a key without a value and also didn't
            // end with a delim
            break;
        }
    }

    if !prev_was_key {
        let msg = "TEXT has uneven number of words".to_string();
        deferred.push_msg_leveled(msg, conf.enforce_even);
    }

    if !prev_was_blank {
        let msg = "TEXT does not end with delim".to_string();
        deferred.push_msg_leveled(msg, conf.enforce_final_delim);
    }

    PureSuccess {
        data: kws,
        deferred,
    }
}

fn split_raw_text_double(
    mut kws: AllKeywords,
    xs: &[u8],
    delim: u8,
    conf: &RawTextReadConfig,
) -> PureSuccess<AllKeywords> {
    let mut deferred = PureErrorBuf::default();

    // ASSUME input slice does not start with delim
    let mut consec_blanks = 0;
    let mut keybuf: Vec<u8> = vec![];
    let mut valuebuf: Vec<u8> = vec![];

    let mut push_pair = |kb: &Vec<_>, vb: &Vec<_>, def: &mut PureErrorBuf| {
        if let Err(e) = kws.insert(kb, vb) {
            def.push_msg_leveled(e.to_string(), conf.enforce_unique);
        }
    };

    let push_delim = |kb: &mut Vec<_>, vb: &mut Vec<_>, k: usize| {
        let n = (k + 1) / 2;
        let buf = if vb.is_empty() { kb } else { vb };
        for _ in 0..n {
            buf.push(delim);
        }
    };

    for segment in xs.split(|x| *x == delim) {
        if segment.is_empty() {
            consec_blanks += 1;
        } else {
            if consec_blanks & 1 == 0 {
                // Previous number of delimiters is odd, treat this as a word
                // boundary
                if !valuebuf.is_empty() {
                    push_pair(&keybuf, &valuebuf, &mut deferred);
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
                    deferred.push_warning("delim at word boundary".to_string());
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
        let msg = "TEXT does not end with delim".to_string();
        deferred.push_msg_leveled(msg, conf.enforce_final_delim)
    } else if consec_blanks & 1 == 1 {
        // technically this ends with a delim but it is part of a word so
        // doesn't count
        let msg = "TEXT does not end with delim".to_string();
        deferred.push_msg_leveled(msg, conf.enforce_final_delim);
        push_delim(&mut keybuf, &mut valuebuf, consec_blanks);
    } else if consec_blanks > 1 {
        // TODO toggleme
        deferred.push_warning("delim at word boundary".to_string());
    }

    if valuebuf.is_empty() {
        let msg = "TEXT has uneven number of words".to_string();
        deferred.push_msg_leveled(msg, conf.enforce_even)
    } else {
        push_pair(&keybuf, &valuebuf, &mut deferred);
    }

    PureSuccess {
        data: kws,
        deferred,
    }
}

fn split_raw_text(
    mut kws: AllKeywords,
    xs: &[u8],
    delim: u8,
    conf: &RawTextReadConfig,
) -> PureSuccess<AllKeywords> {
    if let Some(start) = xs.iter().position(|x| *x == delim) {
        let ys = &xs[(start + 1)..];
        if ys.is_empty() {
            let mut succ = PureSuccess::from(kws);
            succ.push_error("TEXT is empty".into());
            succ
        } else if conf.allow_double_delim {
            split_raw_text_double(kws, ys, delim, conf)
        } else {
            split_raw_text_nodouble(kws, ys, delim, conf)
        }
    } else {
        let mut succ = PureSuccess::from(kws);
        succ.push_error("delimiter not found, cannot parse TEXT".into());
        succ
    }
}

fn repair_keywords(kws: &mut StdKeywords, conf: &RawTextReadConfig) {
    for (key, v) in kws.iter_mut() {
        // TODO generalized this and possibly put in a trait
        if key == &FCSDate::std() {
            if let Some(pattern) = &conf.date_pattern {
                if let Ok(d) = NaiveDate::parse_from_str(v, pattern.as_ref()) {
                    *v = format!("{}", FCSDate(d))
                }
            }
        }
    }
}

fn hash_raw_pairs(pairs: RawPairs, conf: &RawTextReadConfig) -> PureSuccess<RawKeywords> {
    let standard: HashMap<_, _> = HashMap::new();
    let mut res = PureSuccess::from(standard);
    // TODO filter keywords based on pattern somewhere here
    for (key, value) in pairs.into_iter() {
        match res.data.entry(key) {
            Entry::Occupied(e) => {
                let msg = format!("Skipping already-inserted key: {}", e.key());
                res.push_msg_leveled(msg, conf.enforce_unique);
            }
            Entry::Vacant(e) => {
                e.insert(value);
            }
        }
    }
    res
}

fn pad_zeros(s: &str) -> String {
    let len = s.len();
    let trimmed = s.trim_start();
    let newlen = trimmed.len();
    ("0").repeat(len - newlen) + trimmed
}

fn repair_offsets(mut kws: AllKeywords, conf: &RawTextReadConfig) -> AllKeywords {
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

// TODO use non-empty here (and everywhere else we return multiple errors)
fn lookup_req_segment(
    kws: &StdKeywords,
    bk: &StdKey,
    ek: &StdKey,
    corr: OffsetCorrection,
    id: SegmentId,
) -> Result<Segment, Vec<String>> {
    let x0 = get_req(kws, bk);
    let x1 = get_req(kws, ek);
    match (x0, x1) {
        (Ok(begin), Ok(end)) => Segment::try_new(begin, end, corr, id).map_err(|x| vec![x]),
        (a, b) => Err([a.err(), b.err()].into_iter().flatten().collect()),
    }
}

fn lookup_opt_segment(
    kws: &StdKeywords,
    bk: &StdKey,
    ek: &StdKey,
    corr: OffsetCorrection,
    id: SegmentId,
) -> Result<Option<Segment>, Vec<String>> {
    let x0 = get_opt(kws, bk);
    let x1 = get_opt(kws, ek);
    match (x0, x1) {
        (Ok(mb), Ok(me)) => {
            if let (Some(begin), Some(end)) = (mb, me) {
                Segment::try_new(begin, end, corr, id)
                    .map_err(|x| vec![x])
                    .map(Some)
            } else {
                Ok(None)
            }
        }
        (a, b) => Err([a.err(), b.err()].into_iter().flatten().collect()),
    }
}

// TODO unclear if these next two functions should throw errors or warnings
// on failure
fn lookup_data_offsets(
    kws: &StdKeywords,
    conf: &DataReadConfig,
    version: Version,
    default: &Segment,
) -> PureSuccess<Segment> {
    match version {
        Version::FCS2_0 => Ok(*default),
        _ => lookup_req_segment(
            kws,
            &Begindata::std(),
            &Enddata::std(),
            conf.data,
            SegmentId::Data,
        ),
    }
    .map_or_else(
        |es| {
            // TODO toggle this?
            let mut def = PureErrorBuf::from_many(es, PureErrorLevel::Warning);
            let msg =
                "could not use DATA offsets in TEXT, defaulting to HEADER offsets".to_string();
            def.push_warning(msg);
            PureSuccess {
                data: *default,
                deferred: def,
            }
        },
        PureSuccess::from,
    )
}

fn lookup_analysis_offsets(
    kws: &StdKeywords,
    conf: &DataReadConfig,
    version: Version,
    default: &Segment,
) -> PureSuccess<Segment> {
    let default_succ = |msgs| {
        // TODO toggle this?
        let mut def = PureErrorBuf::from_many(msgs, PureErrorLevel::Warning);
        let msg =
            "could not use ANALYSIS offsets in TEXT, defaulting to HEADER offsets".to_string();
        def.push_warning(msg);
        PureSuccess {
            data: *default,
            deferred: def,
        }
    };
    match version {
        Version::FCS2_0 => Ok(Some(*default)),
        Version::FCS3_0 | Version::FCS3_1 => lookup_req_segment(
            kws,
            &Beginanalysis::std(),
            &Endanalysis::std(),
            conf.analysis,
            SegmentId::Analysis,
        )
        .map(Some),
        Version::FCS3_2 => lookup_opt_segment(
            kws,
            &Beginanalysis::std(),
            &Endanalysis::std(),
            conf.analysis,
            SegmentId::Analysis,
        ),
    }
    .map_or_else(default_succ, |mab_seg| {
        mab_seg.map_or_else(|| default_succ(vec![]), PureSuccess::from)
    })
}

fn lookup_stext_offsets(
    kws: &StdKeywords,
    version: Version,
    conf: &RawTextReadConfig,
) -> PureMaybe<Segment> {
    // TODO add another msg explaining that the supp text won't be read if
    // offsets not found
    match version {
        Version::FCS2_0 => PureSuccess::from(None),
        Version::FCS3_0 | Version::FCS3_1 => {
            let res = lookup_req_segment(
                kws,
                &Beginstext::std(),
                &Endstext::std(),
                conf.stext,
                SegmentId::SupplementalText,
            );
            let level = if conf.enforce_stext {
                PureErrorLevel::Error
            } else {
                PureErrorLevel::Warning
            };
            PureMaybe::from_result_strs(res, level)
        }
        Version::FCS3_2 => lookup_opt_segment(
            kws,
            &Beginstext::std(),
            &Endstext::std(),
            conf.stext,
            SegmentId::SupplementalText,
        )
        .map_or_else(
            |es| PureSuccess {
                data: None,
                deferred: PureErrorBuf::from_many(es, PureErrorLevel::Warning),
            },
            PureSuccess::from,
        ),
    }
}

fn add_keywords(
    kws: &mut RawKeywords,
    pairs: RawPairs,
    conf: &RawTextReadConfig,
) -> PureSuccess<()> {
    let mut succ = PureSuccess::from(());
    for (k, v) in pairs.into_iter() {
        match kws.entry(k) {
            Entry::Occupied(e) => {
                let msg = format!(
                    "Skipping already-inserted key from supplemental TEXT: {}",
                    e.key()
                );
                succ.push_msg_leveled(msg, conf.enforce_unique);
            }
            Entry::Vacant(e) => {
                e.insert(v);
            }
        }
    }
    succ
}

fn lookup_nextdata(kws: &StdKeywords, enforce: bool) -> PureMaybe<u32> {
    let k = &Nextdata::std();
    if enforce {
        PureMaybe::from_result_1(get_req(kws, k), PureErrorLevel::Error)
    } else {
        PureMaybe::from_result_1(get_opt(kws, k), PureErrorLevel::Warning).map(|x| x.flatten())
    }
}

fn h_read_raw_text_from_header<R: Read + Seek>(
    h: &mut BufReader<R>,
    header: &Header,
    conf: &RawTextReadConfig,
) -> ImpureResult<RawTEXT> {
    let mut buf = vec![];
    header.text.read(h, &mut buf)?;

    verify_delim(&buf, conf).try_map(|delimiter| {
        let split_succ = split_raw_text(AllKeywords::default(), &buf, delimiter, conf)
            .map(|kws| repair_offsets(kws, conf));
        let stext_succ = split_succ.try_map(|mut kws| {
            lookup_stext_offsets(&mut kws.std, header.version, conf).try_map(|s| {
                let succ = if let Some(seg) = s {
                    if seg.is_unset() {
                        PureSuccess::from(kws)
                    } else {
                        buf.clear();
                        seg.read(h, &mut buf)?;
                        split_raw_text(kws, &buf, delimiter, conf)
                    }
                } else {
                    PureSuccess::from(kws)
                };
                Ok(succ.map(|k| (k, s)))
            })
        })?;
        Ok(stext_succ.and_then(|(mut kws, supp_text_seg)| {
            repair_keywords(&mut kws.std, conf);
            // TODO this will throw an error if not present, but we may not care
            // so toggle b/t error and warning
            let enforce_nextdata = true;
            lookup_nextdata(&kws.std, enforce_nextdata).map(|nextdata| RawTEXT {
                version: header.version,
                parse: ParseParameters {
                    prim_text: header.text,
                    supp_text: supp_text_seg,
                    data: header.data,
                    analysis: header.analysis,
                    nextdata,
                    delimiter,
                },
                keywords: kws,
            })
        }))
    })
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

// fn comp_to_spillover(comp: Compensation, ns: &[Shortname]) -> Option<Spillover> {
//     // Matrix should be square, so if inverse fails that means that somehow it
//     // isn't full rank
//     comp.matrix.try_inverse().map(|matrix| Spillover {
//         measurements: ns.to_vec(),
//         matrix,
//     })
// }

// // TODO doesn't this need to be transposed also?
// fn spillover_to_comp(spillover: Spillover, ns: &[Shortname]) -> Option<Compensation> {
//     // Start by making a new square matrix for all measurements, since the older
//     // $COMP keyword couldn't specify measurements and thus covered all of them.
//     // Then assign the spillover matrix to the bigger full matrix, using the
//     // index of the measurement names. This will be a spillover matrix defined
//     // for all measurements. Anything absent from the original will have 0 in
//     // it's row/column except for the diagonal. Finally, invert this result to
//     // get the compensation matrix.
//     let n = ns.len();
//     let mut full_matrix = DMatrix::<f32>::identity(n, n);
//     // ASSUME spillover measurements are a subset of names supplied to function
//     let positions: Vec<_> = spillover
//         .measurements
//         .into_iter()
//         .enumerate()
//         .flat_map(|(i, m)| ns.iter().position(|x| *x == m).map(|x| (i, x)))
//         .collect();
//     for r in positions.iter() {
//         for c in positions.iter() {
//             full_matrix[(r.1, c.1)] = spillover.matrix[(r.0, c.0)]
//         }
//     }
//     // Matrix should be square, so if inverse fails that means that somehow it
//     // isn't full rank
//     full_matrix
//         .try_inverse()
//         .map(|matrix| Compensation { matrix })
// }

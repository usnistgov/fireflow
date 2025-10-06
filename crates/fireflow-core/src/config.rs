/// Main configuration for reading and writing FCS files.
///
/// By convention, this is "strict-by-default", meaning the default parameters
/// will be set such that only a fully-compliant FCS file can be read without
/// error. This greatly simplifies the API and internally reduces the likelihood
/// of "flipped flags."
///
/// Internal to the library, the main question that matters for whether to throw
/// a warning or error should be "does this adhere to the standard." If not, its
/// an error. This will work in most cases, with a few exceptions where the
/// standard is unclear.
use crate::header::Version;
use crate::segment::{
    AnalysisSegmentId, DataSegmentId, HeaderCorrection, OtherSegmentId, PrimaryTextSegmentId,
    SupplementalTextSegmentId, TEXTCorrection,
};
use crate::text::byteord::ByteOrd2_0;
use crate::text::index::MeasIndex;
use crate::text::keywords as kws;
use crate::validated::ascii_range::OtherWidth;
use crate::validated::datepattern::DatePattern;
use crate::validated::keys::{
    IndexedKey, KeyPatterns, KeyStringPairs, KeyStringValues, NonStdKeywords, NonStdKeywordsExt,
    NonStdMeasPattern, StdKey, StdKeywords,
};
use crate::validated::sub_pattern::SubPatterns;
use crate::validated::textdelim::TEXTDelim;
use crate::validated::timepattern::TimePattern;

use derive_more::{AsRef, Display, From, FromStr};
use regex::Regex;
use std::collections::HashSet;
use std::fs::File;
use std::io;
use std::path::PathBuf;
use std::str::FromStr;
use thiserror::Error;

#[derive(Default, Clone, AsRef, From)]
pub struct ReadHeaderConfig(pub HeaderConfigInner);

/// Instructions for reading the DATA segment.
#[derive(Default, Clone, AsRef)]
pub struct ReadRawTEXTConfig {
    #[as_ref(HeaderConfigInner, ReadHeaderAndTEXTConfig)]
    pub raw: ReadHeaderAndTEXTConfig,

    pub shared: SharedConfig,
}

#[derive(Default, Clone, AsRef)]
pub struct ReadStdTEXTConfig {
    #[as_ref(HeaderConfigInner, ReadHeaderAndTEXTConfig)]
    pub raw: ReadHeaderAndTEXTConfig,

    #[as_ref(StdTextReadConfig)]
    pub standard: StdTextReadConfig,

    #[as_ref(ReadTEXTOffsetsConfig)]
    pub offsets: ReadTEXTOffsetsConfig,

    #[as_ref(ReadLayoutConfig)]
    pub layout: ReadLayoutConfig,

    pub shared: SharedConfig,
}

#[derive(Default, Clone, AsRef)]
pub struct ReadRawDatasetConfig {
    #[as_ref(HeaderConfigInner, ReadHeaderAndTEXTConfig)]
    pub raw: ReadHeaderAndTEXTConfig,

    #[as_ref(ReadLayoutConfig)]
    pub layout: ReadLayoutConfig,

    #[as_ref(ReadTEXTOffsetsConfig)]
    pub offsets: ReadTEXTOffsetsConfig,

    #[as_ref(ReaderConfig)]
    pub data: ReaderConfig,

    pub shared: SharedConfig,
}

#[derive(Default, Clone, AsRef)]
pub struct NewCoreTEXTConfig {
    #[as_ref(StdTextReadConfig)]
    pub standard: StdTextReadConfig,

    #[as_ref(ReadLayoutConfig)]
    pub layout: ReadLayoutConfig,

    #[as_ref(SharedConfig)]
    pub shared: SharedConfig,
}

#[derive(Default, Clone, AsRef)]
pub struct ReadStdDatasetConfig {
    #[as_ref(HeaderConfigInner, ReadHeaderAndTEXTConfig)]
    pub raw: ReadHeaderAndTEXTConfig,

    #[as_ref(StdTextReadConfig)]
    pub standard: StdTextReadConfig,

    #[as_ref(ReadLayoutConfig)]
    pub layout: ReadLayoutConfig,

    #[as_ref(ReadTEXTOffsetsConfig)]
    pub offsets: ReadTEXTOffsetsConfig,

    #[as_ref(ReaderConfig)]
    pub data: ReaderConfig,

    pub shared: SharedConfig,
}

#[derive(Default, Clone, AsRef)]
pub struct ReadRawDatasetFromKeywordsConfig {
    #[as_ref(ReadLayoutConfig)]
    pub layout: ReadLayoutConfig,

    #[as_ref(ReaderConfig)]
    pub data: ReaderConfig,

    #[as_ref(ReadTEXTOffsetsConfig)]
    pub offsets: ReadTEXTOffsetsConfig,

    pub shared: SharedConfig,
}

#[derive(Default, Clone, AsRef)]
pub struct ReadStdDatasetFromKeywordsConfig {
    #[as_ref(StdTextReadConfig)]
    pub standard: StdTextReadConfig,

    #[as_ref(ReadLayoutConfig)]
    pub layout: ReadLayoutConfig,

    #[as_ref(ReadTEXTOffsetsConfig)]
    pub offsets: ReadTEXTOffsetsConfig,

    #[as_ref(ReaderConfig)]
    pub data: ReaderConfig,

    #[as_ref(SharedConfig)]
    pub shared: SharedConfig,
}

/// Instructions for reading the DATA segment.
#[derive(Default, Clone)]
pub struct DataReadConfig {
    /// Instructions to read and standardize TEXT.
    pub standard: StdTextReadConfig,

    // /// Shared configuration options
    // pub shared: SharedConfig,
    /// Configuration to make reader for DATA and ANALYSIS
    pub reader: ReaderConfig,
}

/// Instructions for reading the DATA/ANALYSIS segments
#[derive(Default, Clone)]
pub struct ReaderConfig {
    /// If `true`, allow event width to not perfectly divide DATA.
    ///
    /// In practice, having such a mismatch likely means either PnB or the DATA
    /// offsets are incorrect.
    ///
    /// Does not apply to delimited ASCII, which does not have a fixed width.
    pub allow_uneven_event_width: bool,

    /// If `true`, allow $TOT to not match number of events in DATA.
    ///
    /// For all but delimited ASCII layouts, $TOT is unnecessary and can be
    /// computed by dividing the bytes in DATA by the event width computed from
    /// all $PnB. If $TOT does not match this, it may indicate an issue. If
    /// `false`, throw an error on mismatch, and warning otherwise.
    pub allow_tot_mismatch: bool,
}

/// Configuration for writing an FCS file
#[derive(Clone, Default)]
pub struct WriteConfig {
    /// Delimiter for TEXT segment
    ///
    /// This should be an ASCII character in [1, 126]. Unlike the standard
    /// (which calls for newline), this will default to the record separator
    /// (character 30).
    pub delim: TEXTDelim,

    /// If true, skip check for conversion losses before writing data.
    ///
    /// Data in each column may be stored in several different types which may
    /// or may not totally coincide with the measurement type. For example, a
    /// measurement may be an 8-bit unsigned integer with a 4-bit bitmask, and
    /// the column may be stored as 32-bit floats within the polars dataframe.
    /// However, as long as the floats are only 0 to 2^4 - 1, no conversion
    /// losses will result. This allows the user more flexibility when
    /// manipulating the data for each measurement.
    ///
    /// Skipping this will result in slightly faster writing, as the data need
    /// to be enumerated once prior to writing in order to perform this check.
    /// Lossy conversion will be performed regardless, but warnings will be
    /// emitted if this is false.
    pub skip_conversion_check: bool,

    /// If ``true`` use 20 chars for OTHER offset width, otherwise 8.
    pub big_other: bool,
}

#[derive(Default, Clone)]
pub struct HeaderConfigInner {
    /// Corrections for primary TEXT segment
    pub text_correction: HeaderCorrection<PrimaryTextSegmentId>,

    /// Corrections for DATA segment
    pub data_correction: HeaderCorrection<DataSegmentId>,

    /// Corrections for ANALYSIS segment
    pub analysis_correction: HeaderCorrection<AnalysisSegmentId>,

    /// Corrections for OTHER segments if they exist.
    ///
    /// Each correction will be applied in order. If an offset does not need
    /// to be corrected, use 0,0. This will not affect the number of OTHER
    /// segments that are read; this is controlled by [`max_other`].
    pub other_corrections: Vec<HeaderCorrection<OtherSegmentId>>,

    /// Maximum number of OTHER segments that can be parsed.
    ///
    /// None means limitless.
    pub max_other: Option<usize>,

    /// Width (in bytes) to use when parsing OTHER offsets.
    ///
    /// In 3.2 this should be 8 bytes. In older versions this was not specified.
    /// In practice, vendors seem to use whatever width they want, presumably to
    /// make "large" numbers fit. As such, this must be an integer between 1 and
    /// 20 (corresponding to a theoretical max of 2^64) but will default to 8
    /// since this is most logical.
    pub other_width: OtherWidth,

    /// If true and a segments ending offset is zero, treat it as empty.
    ///
    /// HEADER offsets can only store up to 99,999,999 bytes. If an offset must
    /// be bigger, both offsets for the segment should be written in TEXT and
    /// the HEADER offsets should be both set to zero.
    ///
    /// Some files (incorrectly) only set the ending HEADER offset to zero in
    /// this case is too big. Since offsets are validated such that start <=
    /// end, this is invalid. This option will artificially "squish" the HEADER
    /// offset so it is actually 0,0 which will force the use of the
    /// corresponding offset in TEXT.
    ///
    /// This only applies to 3.0 and up. If this happens in a 2.0 file, it is
    /// just wrong and the only option to fix it is to directly override/edit
    /// the offsets. This also will only apply to DATA and ANALYSIS offsets,
    /// since the TEXT offsets themselves cannot be written in TEXT without
    /// unleashing the dreaded recursive doom loop monster.
    pub squish_offsets: bool,

    /// If true, allow negative values in a HEADER offset.
    ///
    /// An empty offset is supposed to be written as 0,0 according to the
    /// standard. However, this is actually nonsense given that the begin and
    /// end offsets point to the first and last byte; thus 0,0 points to
    /// bytes 0 and 0 for begin and end respectively, which is one byte and
    /// not zero. Therefore, some vendors (understandably) write an "empty"
    /// offset as 0,-1 which actually is zero bytes long. However, -1 is
    /// not a valid offset.
    ///
    /// This flag will treat any negative offset as a 0.
    pub allow_negative: bool,

    /// If true, truncate offsets that exceed the end of the file.
    ///
    /// In many cases, such offsets likely mean the file was incompletely
    /// written, which is a larger problem itself. Setting this to true will at
    /// least allow these files to be read.
    pub truncate_offsets: bool,
}

/// Instructions for reading the TEXT segment as raw key/value pairs.
// TODO add correction for $NEXTDATA
#[derive(Default, Clone, AsRef)]
#[allow(clippy::struct_excessive_bools)]
pub struct ReadHeaderAndTEXTConfig {
    /// Config for reading HEADER
    #[as_ref(HeaderConfigInner)]
    pub header: HeaderConfigInner,

    /// Override the version
    pub version_override: Option<Version>,

    /// Corrections for supplemental TEXT segment
    pub supp_text_correction: TEXTCorrection<SupplementalTextSegmentId>,

    /// If true, allow STEXT to exactly match the HEADER offsets for TEXT.
    ///
    /// Many files do not have (or need) STEXT, but a subset of these will
    /// duplicate the offsets of TEXT from the HEADER into the *STEXT keywords.
    /// According to the standard, these should be empty, which is why this is
    /// an error by default. If this flag is true, this becomes a warning.
    ///
    /// The STEXT offsets will be regardless of this flag if they are
    /// duplicated.
    pub allow_duplicated_supp_text: bool,

    /// If true, totally ignore STEXT and its offsets.
    ///
    /// This may be useful if STEXT is duplicated (or partly overlaps) with
    /// primary TEXT.
    pub ignore_supp_text: bool,

    /// If true, treat every delimiter as literal.
    ///
    /// The standard allows delimiters to be included in keys or values (words)
    /// if they are "escaped" with another delimiter. This also implies that
    /// delimiters can never start or end a word since it is impossible to
    /// unambiguously assign such escaped delimiters to either side of the real
    /// delimiter. This also means empty words are not allowed.
    ///
    /// Setting this to true will disable delimiter escaping; all delimiters will
    /// be literal delimiters that split words. This allows words to be empty
    /// and also disallows delimiters to be included in words at all. For some
    /// files, this is the correct interpretation, albeit not compliant.
    pub use_literal_delims: bool,

    /// If true, allow delimiter to be character outside 1-126.
    pub allow_non_ascii_delim: bool,

    /// If true, allow TEXT to not end with a delimiter.
    pub allow_missing_final_delim: bool,

    /// If true, allow non-unique keys to be present in TEXT.
    ///
    /// In any case, only the first value for a given key will be used. Setting
    /// this to true merely changes a duplicate key to emit a warning and not
    /// an error.
    pub allow_nonunique: bool,

    /// If true, allow TEXT to contain an odd number of words.
    ///
    /// Regardless, the final "dangling" word in the case of an odd number
    /// will be dropped as it has no obvious interpretation.
    pub allow_odd: bool,

    /// If true, allow keys with blank values.
    ///
    /// Only relevant if [`use_literal_delims`] is also true since blank values
    /// cannot exist when delimiters are escaped. Blank values will be dropped
    /// regardless of this flag; setting it to false will trigger an error,
    /// otherwise a warning.
    pub allow_empty: bool,

    /// If true, allow delimiters at word boundaries.
    ///
    /// Only relevant if [`literal_delims`] is false. While delimiters
    /// may be escaped and included in keys or values, it is impossible to tell
    /// within which word they are belong when the are next to a real delimiter,
    /// which is why they are "not allowed."
    ///
    /// Regardless of this value, delimiters at word boundaries will not be
    /// included due to their ambiguity. Setting this to true will emit an
    /// error rather than a warning if this is encountered.
    pub allow_delim_at_boundary: bool,

    /// If true, allow non-utf8 byte sequences in TEXT.
    ///
    /// Words with such bytes will be dropped regardless of this keyword.
    /// Setting this to true will emit an error rather than a warning in such
    /// cases.
    pub allow_non_utf8: bool,

    /// If true, interpret all bytes in TEXT as Latin-1 instead of UTF-8
    pub use_latin1: bool,

    /// If true, allow keys with non-ASCII characters.
    ///
    /// This only applies to non-standard keywords, as all standardized keywords
    /// may only contain letters, numbers, and start with '$'. Regardless, all
    /// compliant keys must only have ASCII. Setting this to true will emit
    /// an error when encountering such a key. If false, the key will be kept
    /// as a non-standard key.
    pub allow_non_ascii_keywords: bool,

    /// If true, allow STEXT offsets to be missing from TEXT.
    ///
    /// Does not affect FCS 3.2 since STEXT is optional there.
    pub allow_missing_supp_text: bool,

    /// If true, allow STEXT to use a different delimiter than TEXT.
    pub allow_supp_text_own_delim: bool,

    /// If true, allow $NEXTDATA to be missing.
    ///
    /// This is a required keyword in all versions. However, most files only
    /// have one dataset so this keyword does nothing. If true, a warning will
    /// be emitted rather than an error if this is missing.
    pub allow_missing_nextdata: bool,

    /// If true, trim whitespace from all values.
    ///
    /// This is mainly useful for the case of fixing offsets which are usually
    /// padded in order to make the TEXT segment a predictable length. These
    /// should be left-padded with numbers since the standard stipulates that
    /// offset values should only be numeric digits, but in many cases offsets
    /// are padded with spaces (on either side). Setting this to true will trim
    /// the spaces leaving just a number to be parsed.
    ///
    /// Blanks may be erroneously present on any keyword that has a fixed
    /// structure; setting this to true may allow these to be parsed correctly
    /// as well.
    ///
    /// Trimming will be done as soon as the bytes are read from the file, thus
    /// preceding any other repair steps. Furthermore, trimming values has a
    /// relatively small performance hit since no additional string allocations
    /// are needed. If anything, it may improve performance since values that
    /// are entirely whitespace will become empty and thus be dropped. Note
    /// that these will result in errors if [`allow_empty`] is false.
    pub trim_value_whitespace: bool,

    /// Remove standard keys from TEXT.
    ///
    /// Comparisons will be case-insensitive. Members of this list should not
    /// try to match the leading "$" as this is implied.
    ///
    /// This will be applied before [`rename_standard_keys`],
    /// [`promote_to_standard`], and [`demote_from_standard`].
    pub ignore_standard_keys: KeyPatterns,

    /// Rename standard keys in TEXT.
    ///
    /// Keys matching the first part of the pair will be replaced by the second.
    /// The leading "$" is implied so keys in this table should not include it.
    /// Comparisons are case-insensitive.
    ///
    /// Keys are renamed before [`promote_to_standard`] and
    /// [`demote_from_standard`] are applied.
    pub rename_standard_keys: KeyStringPairs,

    /// A list of nonstandard keywords to be "promoted" to standard.
    ///
    /// All matching keywords will be prefixed with a "$" and added to the pool
    /// of standard keywords to be processed downstream when deriving data
    /// layouts, measurement metadata, etc. Matching will be case-insensitive.
    pub promote_to_standard: KeyPatterns,

    /// A list of standard keywords to be "demoted" to non-standard.
    ///
    /// Only keywords starting with "$" will be considered. The "$" is implied
    /// when matching, so members of this list should not include it. Matching
    /// will be case-insensitive.
    ///
    /// Matching keywords will be taken out of the pool of standard keywords
    /// ("$" prefix will be removed) and not be considered as such when
    /// processed downstream.
    ///
    /// Useful for surgically correcting "pseudostandard" keywords without
    /// using [`allow_pseudostandard`], which is a crude sledgehammer.
    pub demote_from_standard: KeyPatterns,

    /// Replace values of standard keys.
    ///
    /// Keys will be matched in case-insensitive manner. The leading "$" is
    /// implied, so keys in this table should not include it.
    pub replace_standard_key_values: KeyStringValues,

    /// Append standard key/value pairs to those read from TEXT.
    ///
    /// This will be applied at the very end of TEXT processing, so no other
    /// key/value transformations will apply to it; they will be appended
    /// literally as-is. The "$" prefix is implied and should not be included.
    ///
    /// This will raise a warning or error if any keys are already present,
    /// and existing value will not be overwritten in such cases. This will also
    /// trigger a deviant keyword warning/error if they do not belong in the
    /// indicated version.
    pub append_standard_keywords: KeyStringValues,

    /// Apply substitution patterns to standard key values.
    ///
    /// This is like a substitution operation in sed or perl. Patterns matched
    /// with a regexp will be replaced, possibly with captures.
    pub substitute_standard_key_values: SubPatterns,
}

#[derive(Default, Clone)]
#[allow(clippy::struct_excessive_bools)]
pub struct ReadTEXTOffsetsConfig {
    /// Corrections for DATA offsets in TEXT segment
    pub text_data_correction: TEXTCorrection<DataSegmentId>,

    /// Corrections for ANALYSIS offsets in TEXT segment
    pub text_analysis_correction: TEXTCorrection<AnalysisSegmentId>,

    /// If true, ignore DATA offsets in TEXT.
    ///
    /// This may be useful if DATA offsets are different from those in HEADER,
    /// either inherently or after a correction. This obviously assumes the
    /// offsets in HEADER are correct.
    pub ignore_text_data_offsets: bool,

    /// If true, ignore ANALYSIS offsets in TEXT.
    ///
    /// This may be useful if ANALYSIS offsets are different from those in
    /// HEADER, either inherently or after a correction. This obviously assumes
    /// the offsets in HEADER are correct.
    pub ignore_text_analysis_offsets: bool,

    /// If true, throw error if offsets in HEADER and TEXT differ.
    ///
    /// Only applies to DATA and ANALYSIS offsets
    pub allow_header_text_offset_mismatch: bool,

    /// If true, throw error if required TEXT offsets are missing.
    ///
    /// Only applies to DATA and ANALYSIS offsets in versions 3.0 and 3.1. If
    /// missing these will be taken from HEADER.
    pub allow_missing_required_offsets: bool,

    /// If true, truncate TEXT offsets that exceed the end of the file.
    ///
    /// In many cases, such offsets likely mean the file was incompletely
    /// written, which is a larger problem itself. Setting this to true will at
    /// least allow these files to be read.
    pub truncate_text_offsets: bool,
}

/// Instructions for reading the TEXT segment in a standardized structure.
#[derive(Default, Clone)]
#[allow(clippy::struct_excessive_bools)]
pub struct StdTextReadConfig {
    /// If `true`, remove whitespace between commas where applicable.
    ///
    /// This will only affect keywords that are given as comma-separated lists,
    /// such as $PnE. Will fix the case where `"0, 0"` is supposed to be
    /// `"0,0"`.
    pub trim_intra_value_whitespace: bool,

    /// If given, a pattern to find/match the $PnN of the time measurement.
    ///
    /// If matched, the time measurement must conform to the requirements of the
    /// target FCS version, such as having $TIMESTEP present and having a PnE
    /// set to '0,0'.
    pub time_meas_pattern: Option<TimeMeasNamePattern>,

    /// If true, allow time to not be present even if we specify [`pattern`].
    pub allow_missing_time: bool,

    /// If ``true`` force, force scale to be linear for temporal measurement.
    pub force_time_linear: bool,

    /// If ``true``, ignore $PnG for the temporal measurement.
    ///
    /// The standard explicitly forbids gain from being set for the temporal
    /// channel. This library will allow gain to be 1.0 since this shouldn't
    /// hurt anything. However, some instruments set gain to be something other
    /// than 1.0, which is nonsense and can be ignored with this flag.
    pub ignore_time_gain: bool,

    /// Ignore optical keywords in time channel.
    ///
    /// These are keys which the standard does not explicitly forbid but are
    /// nonsense for the time measurement.
    ///
    /// This cannot ignore PnG; to remove that pass `ignore_time_gain`.
    pub ignore_time_optical_keys: HashSet<TemporalOpticalKey>,

    /// If ``true``, parse $SPILLOVER with indices rather than names.
    ///
    /// Indices will then be used to look up the names that should have been
    /// in their place.
    pub parse_indexed_spillover: bool,

    /// If supplied, will be used as an alternative pattern when parsing $DATE.
    ///
    /// It should have specifiers for year, month, and day as outlined in
    /// https://docs.rs/chrono/latest/chrono/format/strftime/index.html. If not
    /// supplied, $DATE will be parsed according to the standard pattern which
    /// is '%d-%b-%Y'.
    pub date_pattern: Option<DatePattern>,

    /// If supplied, will be used as an alternative pattern toe parse $BTIM/$ETIM.
    pub time_pattern: Option<TimePattern>,

    /// If true, allow non-standard keywords starting with '$'.
    ///
    /// The '$' prefix is reserved for standard keywords only. While little harm
    /// may come from violating this, having these keywords might signify that
    /// the version in the HEADER is wrong and that the file actually follows a
    /// different FCS standard (usually higher) in which these keywords are
    /// standard.
    pub allow_pseudostandard: bool,

    /// If true, allow unused standard keywords.
    ///
    /// These may arise if some $Pn* keywords are present which exceed $PAR or
    /// if $TIMESTEP is present but no time measurement is present.
    pub allow_unused_standard: bool,

    /// If true, allow optional keys to be dropped on error with a warning.
    pub allow_optional_dropping: bool,

    /// If true, throw an error if TEXT includes any deprecated features.
    ///
    /// If false, merely throw a warning.
    pub disallow_deprecated: bool,

    /// If true, try to fix log-scale $PnE and $GnE keywords.
    ///
    /// These keywords are both formatted like 'X,Y' where X and Y are floats.
    /// In the log case, both must be positive. Many files will incorrectly set
    /// Y to 0.0 and X to some positive number. Since Y denotes the minimum
    /// value of the log scale, 0 is meaningless.
    ///
    /// This fix will replace Y in such cases with 1.0, such that the value
    /// becomes 'X,1.0'.
    pub fix_log_scale_offsets: bool,

    /// If supplied, this pattern will be used to group "nonstandard" keywords
    /// with matching measurements.
    ///
    /// Usually this will be something like '^P%n.+' where '%n' will be
    /// substituted with the measurement index before using it as a regular
    /// expression to match keywords. It should not start with a "$" and must
    /// contain a literal '%n'.
    ///
    /// This will matching something like 'P7FOO' which would be 'FOO' for
    /// measurement 7. These may be used when converting between different
    /// FCS versions.
    pub nonstandard_measurement_pattern: Option<NonStdMeasPattern>,
}

#[derive(Default, Clone)]
pub struct ReadLayoutConfig {
    /// If given, override $PnB with the number of bytes in $BYTEORD.
    ///
    /// Some files set $PnB to match the bitmask. For example, a 16-bit column
    /// may only use 10 bits, so $PnB will be 10 and $PnR will be 1024. This
    /// will not work since $PnB must match the width of the real data.
    ///
    /// Setting this will force all $PnB to match $BYTEORD. Obviously this
    /// assumed $BYTEORD is correct. If not, override this using
    /// [`integer_byteord_override`]. All $PnB will still be read regardless of
    /// this flag, so this will not fix badly-formatted values (ie $PnB that
    /// aren't numbers or are out of range). These will require manual
    /// intervention.
    ///
    /// This only has an effect for FCS 2.0-3.0 where $DATATYPE=I.
    pub integer_widths_from_byteord: bool,

    /// If given, override the $BYTEORD keyword for 2.0-3.0 integer layouts.
    ///
    /// In some files the $BYTEORD does not match $PnB, all of which must be
    /// $BYTEORD * 8. This option will override $BYTEORD from the file. $BYTEORD
    /// will still be read, so this option will not salvage a badly-formatted
    /// $BYTEORD value, which will need a different intervention.
    ///
    /// Obviously this must match the actual layout of the numbers in DATA. If
    /// $PnB is also incorrect, use [`integer_widths_from_byteord`] to override
    /// those values as well.
    pub integer_byteord_override: Option<ByteOrd2_0>,

    /// If true, disallow bitmask to be truncated when converting from native type.
    ///
    /// This only applies to integer columns (ie DATATYPE=I and/or
    /// PnDATATYPE=I).
    ///
    /// Some files store $PnR as an large number (such as 2^128), sometimes much
    /// more than the $PnB would allow if matched to the type of the range. For
    /// integers, $PnR implies the bitmask, and a larger-than-$PnB number
    /// implies this bitmask should be all ones. Setting this flag to true will
    /// throw an error if $PnR is much higher than the type for $PnB (ie it
    /// needs to be truncated to make the bitmask).
    ///
    /// The standard is not clear on how this is supposed to work. Ideally, $PnR
    /// and $PnB should match in terms of type and bits to express said type.
    /// Due to the vagueness in the standard and the fact that the interpretation of
    /// large $PnR is fairly clear, this is not an error by default. Users might be
    /// interested in setting this to true if large $PnR values might indicated a typo
    /// or other issue.
    ///
    /// Note: this flag has nothing to do with the bitmask being applied to the
    /// actual data being read. This will happen regardless.
    pub disallow_range_truncation: bool,
}

/// Configuration options for both reading and writing
#[derive(Default, Clone)]
pub struct SharedConfig {
    /// If true, all warnings are considered to be fatal errors.
    pub warnings_are_errors: bool,

    /// If true, do not emit warnings.
    pub hide_warnings: bool,
}

/// A pattern to match the $PnN for the time measurement.
///
/// Defaults to matching "TIME" or "Time".
#[derive(Clone, FromStr, Display, Debug)]
pub struct TimeMeasNamePattern(pub Regex);

/// Measurement keywords which are not allowed for temporal measurements.
///
/// These can optionally be ignored via config.
#[derive(Clone, PartialEq, Eq, Hash)]
pub enum TemporalOpticalKey {
    /// PnF
    Filter,
    /// PnL
    Wavelength,
    /// PnO
    Power,
    /// PnT
    DetectorType,
    /// PnV
    DetectorVoltage,
    /// PnP
    PercentEmitted,
    /// PnCALIBRATION
    Calibration,
    /// PnDET
    DetectorName,
    /// PnTAG
    Tag,
    /// PnFEATURE
    Feature,
    /// PnANALYTE
    Analyte,
}

impl FromStr for TemporalOpticalKey {
    type Err = ParseTemporalOpticalKeyError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "F" => Ok(Self::Filter),
            "L" => Ok(Self::Wavelength),
            "O" => Ok(Self::Power),
            "T" => Ok(Self::DetectorType),
            "P" => Ok(Self::PercentEmitted),
            "V" => Ok(Self::DetectorVoltage),
            "CALIBRATION" => Ok(Self::Calibration),
            "DET" => Ok(Self::DetectorName),
            "TAG" => Ok(Self::Tag),
            "FEATURE" => Ok(Self::Feature),
            "ANALYTE" => Ok(Self::Analyte),
            _ => Err(ParseTemporalOpticalKeyError),
        }
    }
}

#[derive(Debug, Error)]
#[error(
    "must be one of  'F', 'L', 'O', 'T', 'P', 'V', \
     'CALIBRATION', 'DET', 'TAG', 'FEATURE', or 'ANALYTE'"
)]
pub struct ParseTemporalOpticalKeyError;

impl TemporalOpticalKey {
    pub(crate) fn std_key(&self, i: MeasIndex) -> StdKey {
        match self {
            Self::Filter => kws::Filter::std(i),
            // ASSUME this is the same for all versions
            Self::Wavelength => kws::Wavelength::std(i),
            Self::Power => kws::Power::std(i),
            Self::DetectorType => kws::DetectorType::std(i),
            Self::DetectorVoltage => kws::DetectorVoltage::std(i),
            Self::PercentEmitted => kws::PercentEmitted::std(i),
            // ASSUME this is the same for all versions
            Self::Calibration => kws::Calibration3_1::std(i),
            Self::DetectorName => kws::DetectorName::std(i),
            Self::Tag => kws::Tag::std(i),
            Self::Feature => kws::Feature::std(i),
            Self::Analyte => kws::Analyte::std(i),
        }
    }

    pub(crate) fn remove_keys(
        xs: &HashSet<Self>,
        kws: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        i: MeasIndex,
    ) {
        for x in xs {
            let k = x.std_key(i);
            nonstd.transfer_demoted(kws, k);
        }
    }
}

impl Default for TimeMeasNamePattern {
    fn default() -> Self {
        Self(Regex::new("^(TIME|Time)$").unwrap())
    }
}

/// State pertinent to reading a file
pub struct ReadState<C> {
    pub(crate) file_len: u64,
    pub(crate) conf: C,
}

impl<C> ReadState<C> {
    pub(crate) fn open(p: &PathBuf, conf: C) -> io::Result<(Self, File)> {
        let file = File::options().read(true).open(p)?;
        Self::init(&file, conf).map(|st| (st, file))
    }

    pub(crate) fn init(f: &File, conf: C) -> io::Result<Self> {
        f.metadata().map(|m| Self {
            file_len: m.len(),
            conf,
        })
    }
}

#[cfg(feature = "python")]
mod python {
    use crate::python::macros::{impl_from_py_via_fromstr, impl_value_err};
    use crate::segment::OffsetCorrection;

    use super::{ParseTemporalOpticalKeyError, TemporalOpticalKey, TimeMeasNamePattern};

    use pyo3::exceptions::PyValueError;
    use pyo3::prelude::*;

    impl_from_py_via_fromstr!(TemporalOpticalKey);
    impl_value_err!(ParseTemporalOpticalKeyError);

    impl<'py> FromPyObject<'py> for TimeMeasNamePattern {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let s: String = ob.extract()?;
            let n = s
                .parse::<TimeMeasNamePattern>()
                // this should be an error from regexp parsing
                .map_err(|e| PyValueError::new_err(e.to_string()))?;
            Ok(n)
        }
    }

    // offset corrections will be tuples like (i32, i32)
    impl<'py, I, S> FromPyObject<'py> for OffsetCorrection<I, S> {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let t: (i32, i32) = ob.extract()?;
            Ok(Self::from(t))
        }
    }
}

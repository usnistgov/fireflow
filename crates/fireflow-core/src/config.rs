//! Main configuration for reading and writing FCS files.
//!
//! By convention, this is "strict-by-default", meaning the default parameters
//! will be set such that only a fully-compliant FCS file can be read without
//! error. This greatly simplifies the API and internally reduces the likelihood
//! of "flipped flags."
//!
//! Internal to the library, the main question that matters for whether to throw
//! a warning or error should be "does this adhere to the standard." If not, it
//! is an error. This will work in most cases with a few exceptions where the
//! standard is unclear.

use crate::header::Version;
use crate::logging::{IOResult, ImpureError};
use crate::segment::{
    AnalysisSegmentId, DataSegmentId, HeaderCorrection, OtherSegmentId, PrimaryTextSegmentId,
    SupplementalTextSegmentId, TEXTCorrection,
};
use crate::text::index::MeasIndex;
use crate::text::keywords::{self as kws, AlphaNumType};
use crate::validated::ascii_range::OtherWidth;
use crate::validated::datepattern::DatePattern;
use crate::validated::keys::{
    IndexedKey as _, KeyPatterns, KeyStringPairs, KeyStringValues, NonStdKeywords,
    NonStdKeywordsExt as _, NonStdMeasPattern, StdKey, StdKeywords,
};
use crate::validated::sub_pattern::SubPatterns;
use crate::validated::textdelim::TEXTDelim;
use crate::validated::timepattern::TimePattern;

use derive_more::{AsRef, Display, From, FromStr, Into};
use derive_new::new;
use regex::Regex;
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Seek};
use std::path::PathBuf;
use std::str::FromStr;
use thiserror::Error;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{DisplayAsPyErr, FromInnerPyObject, FromPyString},
    pyo3::prelude::*,
};

#[derive(Default, Clone, AsRef, From)]
pub struct ReadHeaderConfig(pub ReadHeaderInnerConfig);

/// Instructions for reading the HEADER and TEXT segments in flat mode.
#[derive(Default, Clone, AsRef)]
pub struct ReadFlatTEXTConfig {
    #[as_ref(ReadHeaderInnerConfig, ReadHeaderAndTEXTConfig)]
    #[as_ref(TruncateOffsets)]
    #[as_ref(TEXTCorrection<SupplementalTextSegmentId>)]
    pub flat: ReadHeaderAndTEXTConfig,

    pub shared: ReadSharedConfig,
}

/// Instructions for reading the HEADER and TEXT segments in standard mode.
#[derive(Default, Clone, AsRef)]
pub struct ReadStdTEXTConfig {
    #[as_ref(ReadHeaderInnerConfig, ReadHeaderAndTEXTConfig)]
    #[as_ref(TruncateOffsets)]
    #[as_ref(TEXTCorrection<SupplementalTextSegmentId>)]
    pub flat: ReadHeaderAndTEXTConfig,

    #[as_ref(ReadStdKeywordsConfig)]
    pub standard: ReadStdKeywordsConfig,

    #[as_ref(ReadDataKeywordsConfig)]
    pub layout: ReadDataKeywordsConfig,

    #[as_ref(ReadSharedConfig)]
    pub shared: ReadSharedConfig,
}

/// Instructions for reading a dataset in flat mode.
#[derive(Default, Clone, AsRef)]
pub struct ReadFlatDatasetConfig {
    #[as_ref(ReadHeaderInnerConfig, ReadHeaderAndTEXTConfig)]
    #[as_ref(TruncateOffsets)]
    #[as_ref(TEXTCorrection<SupplementalTextSegmentId>)]
    pub flat: ReadHeaderAndTEXTConfig,

    #[as_ref(ReadDataKeywordsConfig)]
    pub layout: ReadDataKeywordsConfig,

    #[as_ref(ReadEventsConfig)]
    pub data: ReadEventsConfig,

    #[as_ref(ReadSharedConfig)]
    pub shared: ReadSharedConfig,
}

/// Instructions for reading a dataset in standard mode.
#[derive(Default, Clone, AsRef)]
pub struct ReadStdDatasetConfig {
    #[as_ref(ReadHeaderInnerConfig, ReadHeaderAndTEXTConfig)]
    #[as_ref(TruncateOffsets)]
    #[as_ref(TEXTCorrection<SupplementalTextSegmentId>)]
    pub flat: ReadHeaderAndTEXTConfig,

    #[as_ref(ReadStdKeywordsConfig)]
    pub standard: ReadStdKeywordsConfig,

    #[as_ref(ReadDataKeywordsConfig)]
    pub layout: ReadDataKeywordsConfig,

    #[as_ref(ReadEventsConfig)]
    pub data: ReadEventsConfig,

    #[as_ref(ReadSharedConfig)]
    pub shared: ReadSharedConfig,
}

/// Instructions for reading a dataset in flat mode with a given set of keywords.
#[derive(Default, Clone, AsRef)]
pub struct ReadFlatDatasetFromKeywordsConfig {
    #[as_ref(ReadDataKeywordsConfig)]
    pub layout: ReadDataKeywordsConfig,

    #[as_ref(ReadEventsConfig)]
    pub data: ReadEventsConfig,

    #[as_ref(ReadSharedConfig)]
    pub shared: ReadSharedConfig,
}

/// Instructions for building a new [`crate::core::CoreTEXT`] from keywords.
#[derive(Default, Clone, AsRef)]
pub struct NewCoreTEXTConfig {
    #[as_ref(ReadStdKeywordsConfig)]
    pub standard: ReadStdKeywordsConfig,

    #[as_ref(ReadDataKeywordsConfig)]
    pub layout: ReadDataKeywordsConfig,

    #[as_ref(ReadSharedConfig)]
    pub shared: ReadSharedConfig,
}

/// Instructions for building a new [`crate::core::CoreDataset`] from keywords.
#[derive(Default, Clone, AsRef)]
pub struct NewCoreDatasetConfig {
    #[as_ref(ReadStdKeywordsConfig)]
    pub standard: ReadStdKeywordsConfig,

    #[as_ref(ReadDataKeywordsConfig)]
    pub layout: ReadDataKeywordsConfig,

    #[as_ref(ReadEventsConfig)]
    pub data: ReadEventsConfig,

    #[as_ref(ReadSharedConfig)]
    pub shared: ReadSharedConfig,
}

/// Configuration for writing one or more HEADER+TEXT segments to file
#[derive(Clone, Copy, Default, new)]
pub struct WriteMultiTEXTConfig {
    pub inner: WriteTEXTInnerConfig,
    pub multi: WriteMultiConfig,
}

/// Configuration for writing one or more datasets to file
#[derive(Clone, Copy, Default, new)]
pub struct WriteMultiDatasetConfig {
    pub inner: WriteDatasetInnerConfig,
    pub multi: WriteMultiConfig,
}

/// Specific configuration for writing HEADER+TEXT
#[derive(Clone, Copy, Default, new)]
pub struct WriteTEXTInnerConfig {
    /// Delimiter for TEXT segment
    ///
    /// This should be an ASCII character in `[1, 126]`. Unlike the standard
    /// (which calls for `\n`), this will default to the record separator
    /// (character `30`).
    pub delim: TEXTDelim,

    /// If `true` use 20 chars for OTHER offset width, otherwise 8.
    pub big_other: BigOther,
}

/// Specific configuration for writing one dataset
#[derive(Clone, Copy, Default, new)]
pub struct WriteDatasetInnerConfig {
    pub text: WriteTEXTInnerConfig,

    /// If `true`, skip check for conversion losses before writing data.
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
    /// emitted if this is `false`.
    pub skip_conversion_check: SkipConversionCheck,
}

/// Options that apply to writing multiple datasets
#[derive(Clone, Copy, Default, new)]
pub struct WriteMultiConfig {
    /// If `true` make $NEXTDATA point to the next dataset.
    ///
    /// If `false` $NEXTDATA will be set to 0. This flag should only be set
    /// if a given file is to have multiple FCS datasets inside it.
    pub appendable: AppendableFlag,

    /// If `true` append to file rather than overwriting it.
    ///
    /// This should only be set if the previous dataset was written with
    /// the `appendable` set to `true`, which will set the previous dataset's
    /// $NEXTDATA value to be non-zero and point to the dataset which is to be
    /// written with this current configuration.
    pub append: AppendFlag,
}

/// Specific instructions for reading HEADER
#[derive(Default, Clone, AsRef)]
pub struct ReadHeaderInnerConfig {
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
    /// segments that are read; this is controlled by `max_other`.
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

    /// If `true` and a segments ending offset is zero, treat it as empty.
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
    pub squish_offsets: SquishOffsets,

    /// If `true`, allow negative values in a HEADER offset.
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
    pub allow_negative: AllowNegative,

    /// If `true`, truncate offsets that exceed the end of the file.
    ///
    /// In many cases, such offsets likely mean the file was incompletely
    /// written, which is a larger problem itself. Setting this to `true` will at
    /// least allow these files to be read.
    #[as_ref(TruncateOffsets)]
    pub truncate_offsets: TruncateOffsets,
}

/// Specific instructions for reading the TEXT segment as flat key/value pairs.
#[derive(Default, Clone, AsRef)]
pub struct ReadHeaderAndTEXTConfig {
    /// Config for reading HEADER
    #[as_ref(ReadHeaderInnerConfig)]
    #[as_ref(TruncateOffsets)]
    pub header: ReadHeaderInnerConfig,

    /// Override the version
    pub version_override: Option<Version>,

    /// Corrections for supplemental TEXT segment
    #[as_ref(TEXTCorrection<SupplementalTextSegmentId>)]
    pub supp_text_correction: TEXTCorrection<SupplementalTextSegmentId>,

    /// Correction to apply to $NEXTDATA.
    ///
    /// Will only be applied if $NEXTDATA is non-zero. If $NEXTDATA is negative
    /// after applying this option, it will be truncated to zero.
    pub nextdata_correction: i32,

    /// If `true`, allow STEXT to exactly match the HEADER offsets for TEXT.
    ///
    /// Many files do not have (or need) STEXT, but a subset of these will
    /// duplicate the offsets of TEXT from the HEADER into the *STEXT keywords.
    /// According to the standard, these should be empty, which is why this is
    /// an error by default. If this flag is `true`, this becomes a warning.
    ///
    /// The STEXT offsets will be ignored regardless of this flag if they are
    /// duplicated.
    pub allow_overlapping_supp_text: AllowDuplicatedSuppTEXT,

    /// If `true`, totally ignore STEXT and its offsets.
    ///
    /// This may be useful if STEXT is duplicated (or partly overlaps) with
    /// primary TEXT.
    pub ignore_supp_text: IgnoreSuppTEXT,

    /// Determine how to escape delims in TEXT.
    ///
    /// The standard allows delimiters to be included in keys or values (tokens)
    /// if they are "escaped" with another delimiter. This also implies that
    /// delimiters can never start or end a tokens since it is impossible to
    /// unambiguously assign such escaped delimiters to either side of the real
    /// delimiter. This also means empty tokens are not allowed.
    ///
    /// In reality, many files use delimiters as if they are not supposed to
    /// be escaped.
    ///
    /// If [`DelimEscapeMode::Escaped`] or [`DelimEscapeMode::Unescaped`],
    /// escape or do not escape delimiters respectively.
    ///
    /// If [`DelimEscapeMode::GuessEscaped`] or
    /// [`DelimEscapeMode::GuessUnescaped`], attempt to guess how delimiters
    /// should be treated, falling back to escaped or unescaped mode
    /// respectively if the choice is ambiguous. The determination will be made
    /// by first scanning TEXT to find all delimiter positions and choosing the
    /// mode which results in an even number of tokens with no delimiters in
    /// keys (escaped mode) and no blank keys (unescaped mode).
    ///
    /// Using the guessing algorithm has a significant performance penalty since
    /// TEXT needs to be parsed twice. Furthermore, this algorithm is heuristic
    /// and not guaranteed to succeed. An uneven number of tokens implies that
    /// TEXT is malformed which will likely be the case assuming that the ending
    /// offset for TEXT will be too high (if at all) therefore all delimiters
    /// should be in the TEXT segment and can be counted. Keys likely will not
    /// have escaped delimiters in them. Keys should almost never be blank in
    /// unescaped mode since `""` is almost never a sensible key value.
    ///
    /// The guessing algorithm is independent of
    /// [`Self::trim_trailing_whitespace`] since it will ignore everything after
    /// the last delimiter. It is also independent of [`Self::allow_odd`] and
    /// [`Self::allow_missing_final_delim`] which will trigger as normal if
    /// their respective violations are found.
    ///
    /// If unescaped mode ends up be used, then [`Self::allow_empty_values`] is
    /// implied to be `true`.
    pub delim_escape_mode: DelimEscapeMode,

    /// If `true`, allow delimiter to be character outside 1-126.
    pub allow_non_ascii_delim: AllowNonAsciiDelim,

    /// If `true`, allow TEXT to not end with a delimiter.
    pub allow_missing_final_delim: AllowMissingFinalDelim,

    /// If `true`, allow non-unique keys to be present in TEXT.
    ///
    /// In any case, only the first value for a given key will be used. Setting
    /// this to `true` merely changes a duplicate key to emit a warning and not
    /// an error.
    pub allow_nonunique: AllowNonunique,

    /// If `true`, allow TEXT to contain an odd number of tokens.
    ///
    /// Regardless, the final "dangling" token in the case of an odd number
    /// will be dropped as it has no obvious interpretation.
    pub allow_odd: AllowOdd,

    /// If `true`, allow blank keys.
    ///
    /// Only relevant if delimiters are unescaped since blank keys cannot exist
    /// when delimiters are escaped. Blank values will be dropped regardless of
    /// this flag; setting it to `false` will trigger an error, otherwise a
    /// warning.
    ///
    /// In practice blank values happen much more often than blank keys, so
    /// presence of blank keys probably indicates that token which is really
    /// a value is somehow being parsed as a key.
    pub allow_empty_keys: AllowEmptyKeys,

    /// If `true`, allow blank values.
    ///
    /// These can arise if delimiters are escaped,
    /// [`Self::trim_value_whitespace`] is `true`, and values which are entirely
    /// whitespace are trimmed to zero bytes. This is relatively common in
    /// practice despite being non-standard. Given this and the fact that
    /// whitespace generally has little meaning for keyword values, this flag is
    /// almost always safe to set as `true`.
    ///
    /// Blank values will be dropped regardless of this flag; setting it to
    /// `false` will trigger an error, otherwise a warning.
    ///
    /// If delimiters are unescaped, empty values are implied and this flag does
    /// nothing.
    pub allow_empty_values: AllowEmptyValues,

    /// If `true`, allow delimiters at token boundaries.
    ///
    /// Only relevant if `literal_delims` is `false`. While delimiters
    /// may be escaped and included in keys or values, it is impossible to tell
    /// within which token they are belong when the are next to a real delimiter,
    /// which is why they are "not allowed."
    ///
    /// Regardless of this value, delimiters at token boundaries will not be
    /// included due to their ambiguity. Setting this to `true` will emit an
    /// error rather than a warning if this is encountered.
    pub allow_delim_at_boundary: AllowDelimAtBoundary,

    /// If `true`, allow non-utf8 byte sequences in TEXT.
    ///
    /// Tokens with such bytes will be dropped regardless of this keyword.
    /// Setting this to `true` will emit an error rather than a warning in such
    /// cases.
    pub allow_non_utf8: AllowNonUtf8,

    /// If `true`, interpret all bytes in TEXT as Latin-1 instead of UTF-8
    pub use_latin1: UseLatin1,

    /// If `true`, allow keys with non-ASCII characters.
    ///
    /// This only applies to non-standard keywords, as all standardized keywords
    /// may only contain letters, numbers, and start with '$'. Regardless, all
    /// compliant keys must only have ASCII. Setting this to `true` will emit
    /// an error when encountering such a key. If `false`, the key will be kept
    /// as a non-standard key.
    pub allow_non_ascii_keywords: AllowNonAsciiKeywords,

    /// If `true`, allow STEXT offsets to be missing from TEXT.
    ///
    /// Does not affect FCS 3.2 since STEXT is optional there.
    pub allow_missing_supp_text: AllowMissingSuppTEXT,

    /// If `true`, allow STEXT to use a different delimiter than TEXT.
    pub allow_supp_text_own_delim: AllowSuppTEXTOwnDelim,

    /// If `true`, allow $NEXTDATA to be missing.
    ///
    /// This is a required keyword in all versions. However, most files only
    /// have one dataset so this keyword does nothing. If `true`, a warning will
    /// be emitted rather than an error if this is missing.
    pub allow_missing_nextdata: AllowMissingNextdata,

    /// If `true`, trim whitespace from all values.
    ///
    /// This is mainly useful for the case of fixing offsets which are usually
    /// padded in order to make the TEXT segment a predictable length. These
    /// should be left-padded with numbers since the standard stipulates that
    /// offset values should only be numeric digits, but in many cases offsets
    /// are padded with spaces (on either side). Setting this to `true` will
    /// trim the spaces leaving just a number to be parsed.
    ///
    /// Blanks may be erroneously present on any keyword that has a fixed
    /// structure; setting this to `true` may allow these to be parsed correctly
    /// as well.
    ///
    /// Trimming will be done as soon as the bytes are read from the file, thus
    /// preceding any other repair steps. Furthermore, trimming values has a
    /// relatively small performance hit since no additional string allocations
    /// are needed. If anything, it may improve performance since values that
    /// are entirely whitespace will become empty and thus be dropped. Note that
    /// these will result in errors if [`Self::allow_empty_values`] is `false`.
    pub trim_value_whitespace: TrimValueWhitespace,

    /// If `true` remove whitespace after TEXT.
    ///
    /// In order to make TEXT a predictable length, it seems some vendors just
    /// add padding at the end which will ensure the segment after it starts at
    /// a predictable offset. This allows the length of digits in TEXT (such as
    /// offsets) to vary within a given range.
    ///
    /// Unfortunately, it also trips off lots of errors because TEXT in these
    /// cases will not end with a delimiter.
    ///
    /// This flag will "move" the end of TEXT to the latest non-whitespace
    /// character prior to the offset actually given in HEADER.
    pub trim_trailing_whitespace: TrimTrailingWhitespace,

    /// Remove standard keys from TEXT.
    ///
    /// Comparisons will be case-insensitive. Members of this list should not
    /// try to match the leading "$" as this is implied.
    ///
    /// This will be applied before [`Self::rename_standard_keys`],
    /// [`Self::promote_to_standard`], and [`Self::demote_from_standard`].
    pub ignore_standard_keys: KeyPatterns,

    /// Rename standard keys in TEXT.
    ///
    /// Keys matching the first part of the pair will be replaced by the second.
    /// The leading "$" is implied so keys in this table should not include it.
    /// Comparisons are case-insensitive.
    ///
    /// Keys are renamed before [`Self::promote_to_standard`] and
    /// [`Self::demote_from_standard`] are applied.
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
    /// Useful for surgically correcting "pseudostandard" keywords without using
    /// [`ReadStdKeywordsConfig::allow_pseudostandard`], which is a crude
    /// sledgehammer.
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

/// Specific instructions for standardizing keywords from TEXT
#[derive(Clone)]
pub struct ReadStdKeywordsConfig {
    /// If `true`, force all $PnN to be unique if they are not already.
    ///
    /// All versions of the standards requires that all $PnN be unique.
    /// Furthermore, many data structures and operations and `fireflow` are
    /// impossible without a guarantee that names are unique.
    ///
    /// Setting this option will append incrementing digits to non-unique names
    /// until all names are unique. For instance, two keys names "X" will become
    /// "X0" and "X1".
    pub dedup_measurement_names: DedupMeasNames,

    /// If `true`, remove whitespace between commas where applicable.
    ///
    /// This will only affect keywords that are given as comma-separated lists,
    /// such as $PnE. Will fix the case where `"0, 0"` is supposed to be
    /// `"0,0"`.
    pub trim_intra_value_whitespace: TrimIntraValueWhitespace,

    /// If `true`, a pattern to find/match the $PnN of the time measurement.
    ///
    /// If matched, the time measurement must conform to the requirements of the
    /// target FCS version, such as having $TIMESTEP present and having a PnE
    /// set to `"0,0"`.
    pub time_meas_pattern: Option<TimeMeasNamePattern>,

    /// If `true`, allow time to be absent even if we specify `time_meas_pattern`.
    pub allow_missing_time: AllowMissingTime,

    /// If `true` force, force scale to be linear for temporal measurement.
    pub force_time_linear: ForceTimeLinear,

    /// If `true`, ignore $PnG for the temporal measurement.
    ///
    /// The standard explicitly forbids gain from being set for the temporal
    /// channel. This library will allow gain to be 1.0 since this shouldn't
    /// hurt anything. However, some instruments set gain to be something other
    /// than 1.0, which is nonsense and can be ignored with this flag.
    pub ignore_time_gain: IgnoreTimeGain,

    /// If `true`, ignore optical keywords in time channel.
    ///
    /// These are keys which the standard does not explicitly forbid but are
    /// nonsense for the time measurement.
    ///
    /// This cannot ignore PnG; to remove that pass `ignore_time_gain`.
    pub ignore_time_optical_keys: HashSet<TemporalOpticalKey>,

    /// If `true`, parse $SPILLOVER with indices rather than names.
    ///
    /// Indices will then be used to look up the names that should have been
    /// in their place.
    pub parse_indexed_spillover: ParseIndexedSpillover,

    /// If set, will be used as an alternative pattern when parsing $DATE.
    ///
    /// It should have specifiers for year, month, and day as outlined in
    /// [chrono](https://docs.rs/chrono/latest/chrono/format/strftime/index.html).
    /// If not supplied, $DATE will be parsed according to the standard pattern
    /// which is `"%d-%b-%Y"`.
    pub date_pattern: Option<DatePattern>,

    /// If `true`, will be used as an alternative pattern toe parse $BTIM/$ETIM.
    pub time_pattern: Option<TimePattern>,

    /// If set, will be used to parse $BEGINDATETIME and $ENDDATETIME.
    ///
    /// It should follow the format outline in
    /// [chrono](https://docs.rs/chrono/latest/chrono/format/strftime/index.html).
    /// If not supplied, timestamps will be parsed as an ISO-formatted timestamp
    /// possibly with a timezone.
    pub datetime_pattern: Option<String>,

    /// If set, will be used to parse $LAST_MODIFIED.
    ///
    /// It should follow the format outline in
    /// [chrono](https://docs.rs/chrono/latest/chrono/format/strftime/index.html).
    /// If not supplied, timestamps will be parsed according to the standard
    /// format which is `"%d-%b-%Y %H:%M:%S"` possibly with centiseconds after.
    pub last_modified_pattern: Option<String>,

    /// If `true`, capture other values for $PnFEATURE not mentioned in the standard.
    ///
    /// $PnFEATURE as described in the standard only explicitly mentions
    /// `"Area"`, `"Width"`, and `"Height"` as allowed values. It is not clear
    /// if these were intended as the only allowed values, but they make sense
    /// when describing any measurement which is physically a time-based
    /// detector response. However, some newer machines (particularly those with
    /// imaging capabilities) will consider something like `"Eccentricity"` as a
    /// "feature" which will thus be stored $PnFEATURE. However, this is a
    /// different physical measurement (pixels vs time-based response) and
    /// is thus distinct from area/width/height.
    ///
    /// Given that the standard clearly intended for area/width/height to be
    /// described with $PnFEATURE and that other values correspond to separate
    /// measurements, the default behavior is only to allow `"Area"`, `"Width"`,
    /// and `"Height"`. Anything else will result in an error.
    ///
    /// If `true`, other values for $PnFEATURE will be captured but will be
    /// separate from area/width/height and will be accessible using a different
    /// keyword.
    pub allow_other_feature: AllowOtherFeature,

    /// If `true`, allow non-standard keywords starting with `"$"`.
    ///
    /// The `"$`" prefix is reserved for standard keywords only. While little
    /// harm may come from violating this, having these keywords might signify
    /// that the version in the HEADER is wrong and that the file actually
    /// follows a different FCS standard (usually higher) in which these
    /// keywords are standard.
    pub allow_pseudostandard: AllowPseudostandard,

    /// If `true`, allow unused standard keywords.
    ///
    /// These may arise if some $Pn* keywords are present which exceed $PAR or
    /// if $TIMESTEP is present but no time measurement is present.
    pub allow_unused_standard: AllowUnusedStandard,

    /// If `true`, throw an error if TEXT includes any deprecated features.
    ///
    /// If `false`, merely throw a warning.
    pub disallow_deprecated: DisallowDeprecated,

    /// If `true`, try to fix log-scale $PnE and $GnE keywords.
    ///
    /// These keywords are both formatted like `"X,Y"` where `X` and `Y` are
    /// floats. In the log case, both must be positive. Many files will
    /// incorrectly set `Y` to 0.0 and `X` to some positive number. Since `Y`
    /// denotes the minimum value of the log scale, 0 is meaningless.
    ///
    /// This fix will replace `Y` in such cases with 1.0, such that the value
    /// becomes `"X,1.0"`.
    pub fix_log_scale_offsets: FixLogScaleOffsets,

    /// If `true`, require that $BEGINDATETIME and $ENDDATETIME have a timezone.
    ///
    /// The standards do not require that these keys use a timezone. However, it
    /// is ambiguous to not provide one. Without a timezone, timestamps will be
    /// parsed using localtime, which is location-dependent.
    ///
    /// This only affects FCS 3.2
    pub disallow_localtime: DisallowLocaltime,

    /// If `true`, this pattern will be used to group "nonstandard" keywords
    /// with matching measurements.
    ///
    /// Usually this will be something like `^"P%n.+"` where `%n` will be
    /// substituted with the measurement index before using it as a regular
    /// expression to match keywords. It should not start with a `"$"` and must
    /// contain a literal `"%n"`.
    ///
    /// This will matching something like `"P7FOO"` which would be `"FOO"` for
    /// measurement `7`. These may be used when converting between different
    /// FCS versions.
    pub nonstandard_measurement_pattern: Option<NonStdMeasPattern>,
}

impl Default for ReadStdKeywordsConfig {
    fn default() -> Self {
        Self {
            dedup_measurement_names: DedupMeasNames::default(),
            trim_intra_value_whitespace: TrimIntraValueWhitespace::default(),
            time_meas_pattern: None,
            allow_missing_time: AllowMissingTime::default(),
            force_time_linear: ForceTimeLinear::default(),
            ignore_time_gain: IgnoreTimeGain::default(),
            ignore_time_optical_keys: HashSet::default(),
            parse_indexed_spillover: ParseIndexedSpillover::default(),
            date_pattern: None,
            time_pattern: None,
            datetime_pattern: None,
            last_modified_pattern: None,
            allow_other_feature: AllowOtherFeature::default(),
            allow_pseudostandard: AllowPseudostandard::default(),
            allow_unused_standard: AllowUnusedStandard::default(),
            disallow_deprecated: DisallowDeprecated::default(),
            fix_log_scale_offsets: FixLogScaleOffsets::default(),
            disallow_localtime: DisallowLocaltime::default(),
            // this default impl exists entirely so that this can be Some(...)
            nonstandard_measurement_pattern: Some(NonStdMeasPattern::default()),
        }
    }
}

/// Specific instructions for reading a data layout.
///
/// Note that some of these are also when reading any keyword in standard mode.
/// Since the layout keywords always need to be read, and the rest only need to
/// be read specifically when building [`crate::core::CoreTEXT`] or
/// [`crate::core::CoreDataset`], these options are here since the layout is the
/// thing they have in common.
#[derive(Default, Clone, AsRef)]
pub struct ReadDataKeywordsConfig {
    /// Corrections for DATA offsets in TEXT segment
    #[as_ref(TEXTCorrection<DataSegmentId>)]
    pub text_data_correction: TEXTCorrection<DataSegmentId>,

    /// Corrections for ANALYSIS offsets in TEXT segment
    #[as_ref(TEXTCorrection<AnalysisSegmentId>)]
    pub text_analysis_correction: TEXTCorrection<AnalysisSegmentId>,

    /// If `true`, ignore DATA offsets in TEXT.
    ///
    /// This may be useful if DATA offsets are different from those in HEADER,
    /// either inherently or after a correction. This obviously assumes the
    /// offsets in HEADER are correct.
    #[as_ref(IgnoreTEXTDataOffsets)]
    pub ignore_text_data_offsets: IgnoreTEXTDataOffsets,

    /// If `true`, ignore ANALYSIS offsets in TEXT.
    ///
    /// This may be useful if ANALYSIS offsets are different from those in
    /// HEADER, either inherently or after a correction. This obviously assumes
    /// the offsets in HEADER are correct.
    #[as_ref(IgnoreTEXTAnalysisOffsets)]
    pub ignore_text_analysis_offsets: IgnoreTEXTAnalysisOffsets,

    /// If `true`, throw error if offsets in HEADER and TEXT differ.
    ///
    /// Only applies to DATA and ANALYSIS offsets
    #[as_ref(AllowHeaderTEXTOffsetMismatch)]
    pub allow_header_text_offset_mismatch: AllowHeaderTEXTOffsetMismatch,

    /// If `true`, throw error if required TEXT offsets are missing.
    ///
    /// Only applies to DATA and ANALYSIS offsets in versions 3.0 and 3.1. If
    /// missing these will be taken from HEADER.
    #[as_ref(AllowMissingRequiredOffsets)]
    pub allow_missing_required_offsets: AllowMissingRequiredOffsets,

    /// If `true`, truncate TEXT offsets that exceed the end of the file.
    ///
    /// In many cases, such offsets likely mean the file was incompletely
    /// written, which is a larger problem itself. Setting this to true will at
    /// least allow these files to be read.
    #[as_ref(TruncateOffsets)]
    pub truncate_text_offsets: TruncateOffsets,

    /// If `true`, allow optional keys to be dropped on error with a warning.
    ///
    /// Also used when parsing any keyword in standard mode.
    pub allow_optional_dropping: AllowOptionalDropping,

    /// If `true`, transfer dropped optional keys to nonstandard dict.
    ///
    /// Has no effect if [`Self::allow_optional_dropping`] is `false` as all
    /// dropped optional keywords will produce a fatal error.
    ///
    /// Also used when parsing any keyword in standard mode.
    pub transfer_dropped_optional: TransferDroppedOptional,

    /// If given, override $PnB with the number of bytes in $BYTEORD.
    ///
    /// Some files set $PnB to match the bitmask. For example, a 16-bit column
    /// may only use 10 bits, so $PnB will be 10 and $PnR will be 1024. This
    /// will not work since $PnB must match the width of the real data.
    ///
    /// Setting this will force all $PnB to match $BYTEORD. Obviously this
    /// assumed $BYTEORD is correct. If not, override this using
    /// [`Self::integer_byteord_override`]. All $PnB will still be read
    /// regardless of this flag, so this will not fix badly-formatted values (ie
    /// $PnB that aren't numbers or are out of range). These will require manual
    /// intervention.
    ///
    /// This only has an effect for FCS 2.0-3.0 where $DATATYPE=I.
    pub integer_widths_from_byteord: IntegerWidthsFromByteord,

    /// If given, override the $BYTEORD keyword for 2.0-3.0 integer layouts.
    ///
    /// In some files the $BYTEORD does not match $PnB, all of which must be
    /// $BYTEORD * 8. This option will override $BYTEORD from the file. $BYTEORD
    /// will still be read, so this option will not salvage a badly-formatted
    /// $BYTEORD value, which will need a different intervention.
    ///
    /// Obviously this must match the actual layout of the numbers in DATA. If
    /// $PnB is also incorrect, use [`Self::integer_widths_from_byteord`] to
    /// override those values as well.
    pub integer_byteord_override: Option<kws::ByteOrd2_0>,

    /// If `true`, disallow bitmask to be truncated when converting from native type.
    ///
    /// This only applies to integer columns (ie DATATYPE=I and/or
    /// PnDATATYPE=I).
    ///
    /// Some files store $PnR as an large number (such as 2^128), sometimes much
    /// more than the $PnB would allow if matched to the type of the range. For
    /// integers, $PnR implies the bitmask, and a larger-than-$PnB number
    /// implies this bitmask should be all ones. Setting this flag to `true`
    /// will throw an error if $PnR is much higher than the type for $PnB (ie it
    /// needs to be truncated to make the bitmask).
    ///
    /// The standard is not clear on how this is supposed to work. Ideally, $PnR
    /// and $PnB should match in terms of type and bits to express said type.
    /// Due to the vagueness in the standard and the fact that the
    /// interpretation of large $PnR is fairly clear, this is not an error by
    /// default. Users might be interested in setting this to `true` if large
    /// $PnR values might indicated a typo or other issue.
    ///
    /// Note: this flag has nothing to do with the bitmask being applied to the
    /// actual data being read. This will happen regardless.
    pub disallow_range_truncation: DisallowRangeTrunc,
}

/// Specific instructions for reading events from DATA segment
#[derive(Default, Clone)]
pub struct ReadEventsConfig {
    /// If `true`, allow event width to not perfectly divide DATA.
    ///
    /// In practice, having such a mismatch likely means either PnB or the DATA
    /// offsets are incorrect.
    ///
    /// Does not apply to delimited ASCII, which does not have a fixed width.
    pub allow_uneven_event_width: AllowUnevenEventWidth,

    /// If `true`, allow $TOT to not match number of events in DATA.
    ///
    /// For all but delimited ASCII layouts, $TOT is unnecessary and can be
    /// computed by dividing the bytes in DATA by the event width computed from
    /// all $PnB. If $TOT does not match this, it may indicate an issue. If
    /// `false`, throw an error on mismatch, and warning otherwise.
    pub allow_tot_mismatch: AllowTotMismatch,

    /// Control which measurements will be truncated via $PnR.
    pub truncate_event_values: TruncateEventValues,

    /// If `true`, forbid event values in DATA to exceed $PnR.
    ///
    /// Each column containing an overrange value will be reported, either as
    /// an error (`true`) or warning (`false`).
    ///
    /// This flag only has an effect if the column is not truncated according to
    /// [`Self::truncate_event_values`].
    pub disallow_over_range: DisallowOverRange,
}

/// Configuration options for across all reading functions
#[derive(Default, Clone)]
pub struct ReadSharedConfig {
    /// If `true`, all warnings are considered to be fatal errors.
    pub warnings_are_errors: bool,

    /// If `true`, do not emit warnings.
    pub hide_warnings: bool,
}

/// Choose how to escape delims in TEXT segment.
#[derive(Default, Clone, Copy)]
#[cfg_attr(feature = "python", derive(FromPyString))]
pub enum DelimEscapeMode {
    #[default]
    Escaped,
    Unescaped,
    GuessEscaped,
    GuessUnescaped,
}

impl FromStr for DelimEscapeMode {
    type Err = DelimEscapeModeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "escaped" => Ok(Self::Escaped),
            "unescaped" => Ok(Self::Unescaped),
            "guess_escaped" => Ok(Self::GuessEscaped),
            "guess_unescaped" => Ok(Self::GuessUnescaped),
            _ => Err(DelimEscapeModeError),
        }
    }
}

/// Error when parsing [`DelimEscapeMode`] from [`String`]
#[derive(Error, Debug)]
#[error("must be one of 'escaped', 'unescaped', 'guess_escaped', or 'guess_unescaped'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ConfigError))]
pub struct DelimEscapeModeError;

/// Choose which event types are truncated.
///
/// By default only truncate when $DATATYPE (or $PnDATATYPE) is "I".
#[derive(Default, Clone, Copy)]
#[cfg_attr(feature = "python", derive(FromPyString))]
pub enum TruncateEventValues {
    #[default]
    IntOnly,
    All,
    None,
}

impl FromStr for TruncateEventValues {
    type Err = TruncateEventValuesError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "int_only" => Ok(Self::IntOnly),
            "all" => Ok(Self::All),
            "none" => Ok(Self::None),
            _ => Err(TruncateEventValuesError),
        }
    }
}

/// Error when parsing [`TruncateEventValues`] from [`String`]
#[derive(Error, Debug)]
#[error("must be one of 'int_only', 'all', or 'none'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ConfigError))]
pub struct TruncateEventValuesError;

impl TruncateEventValues {
    pub(crate) fn matches_datatype(self, dt: AlphaNumType) -> bool {
        matches!(
            (self, dt),
            (Self::IntOnly, AlphaNumType::Integer) | (Self::All, _)
        )
    }
}

pub trait ConfigFlag {
    fn is_set(&self) -> bool;
}

pub trait ErrorFlag: ConfigFlag {
    const TRUE_IS_ERROR: bool;

    fn is_error(&self) -> bool {
        self.is_set() == Self::TRUE_IS_ERROR
    }
}

macro_rules! impl_config_flag {
    ($n:ident) => {
        #[derive(From, Clone, Copy, Default)]
        #[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
        pub struct $n(pub bool);

        impl ConfigFlag for $n {
            fn is_set(&self) -> bool {
                self.0
            }
        }
    };
}

macro_rules! impl_error_flag {
    (true_is_error $n:ident) => {
        impl_error_flag!($n, true);
    };

    (false_is_error $n:ident) => {
        impl_error_flag!($n, false);
    };

    ($n:ident, $true_is_error:expr) => {
        impl_config_flag!($n);

        impl ErrorFlag for $n {
            const TRUE_IS_ERROR: bool = $true_is_error;
        }
    };
}

impl_config_flag!(SquishOffsets);
impl_config_flag!(AllowNegative);
impl_config_flag!(TruncateOffsets);

impl_error_flag!(false_is_error AllowUnevenEventWidth);
impl_error_flag!(false_is_error AllowTotMismatch);
impl_error_flag!(true_is_error DisallowOverRange);

impl_error_flag!(false_is_error AllowDuplicatedSuppTEXT);
impl_error_flag!(false_is_error IgnoreSuppTEXT);
impl_error_flag!(false_is_error AllowNonAsciiDelim);
impl_error_flag!(false_is_error AllowMissingFinalDelim);
impl_error_flag!(false_is_error AllowNonunique);
impl_error_flag!(false_is_error AllowOdd);
impl_error_flag!(false_is_error AllowEmptyKeys);
impl_error_flag!(false_is_error AllowEmptyValues);
impl_error_flag!(false_is_error AllowDelimAtBoundary);
impl_error_flag!(false_is_error AllowNonUtf8);
impl_config_flag!(UseLatin1);
impl_error_flag!(false_is_error AllowNonAsciiKeywords);
impl_error_flag!(false_is_error AllowMissingSuppTEXT);
impl_error_flag!(false_is_error AllowSuppTEXTOwnDelim);
impl_error_flag!(false_is_error AllowMissingNextdata);
impl_config_flag!(TrimValueWhitespace);
impl_config_flag!(TrimTrailingWhitespace);
impl_config_flag!(IgnoreTEXTDataOffsets);
impl_config_flag!(IgnoreTEXTAnalysisOffsets);
impl_error_flag!(false_is_error AllowHeaderTEXTOffsetMismatch);
impl_error_flag!(false_is_error AllowMissingRequiredOffsets);

impl_config_flag!(DedupMeasNames);
impl_config_flag!(TrimIntraValueWhitespace);
impl_error_flag!(false_is_error AllowMissingTime);
impl_config_flag!(ForceTimeLinear);
impl_config_flag!(IgnoreTimeGain);
impl_config_flag!(ParseIndexedSpillover);
impl_error_flag!(false_is_error AllowOtherFeature);
impl_error_flag!(false_is_error AllowPseudostandard);
impl_error_flag!(false_is_error AllowUnusedStandard);
impl_error_flag!(false_is_error AllowOptionalDropping);
impl_config_flag!(IntegerWidthsFromByteord);
impl_config_flag!(TransferDroppedOptional);
impl_error_flag!(true_is_error DisallowDeprecated);
impl_config_flag!(FixLogScaleOffsets);
impl_error_flag!(true_is_error DisallowLocaltime);

impl_error_flag!(true_is_error DisallowRangeTrunc);

impl_error_flag!(false_is_error AllowLoss);

impl_config_flag!(SkipConversionCheck);
impl_config_flag!(BigOther);
impl_config_flag!(AppendableFlag);
impl_config_flag!(AppendFlag);

impl AppendFlag {
    pub(crate) fn file_options(self) -> OpenOptions {
        let mut opts = File::options();
        opts.create(true);
        if self.is_set() {
            opts.append(true)
        } else {
            opts.write(true).truncate(true)
        };
        opts
    }
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
#[cfg_attr(feature = "python", derive(FromPyString))]
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

/// Error when creating [`TemporalOpticalKey`] from string
#[derive(Debug, Error)]
#[error(
    "must be one of  'F', 'L', 'O', 'T', 'P', 'V', \
     'CALIBRATION', 'DET', 'TAG', 'FEATURE', or 'ANALYTE'"
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ConfigError))]
pub struct ParseTemporalOpticalKeyError;

impl TemporalOpticalKey {
    pub(crate) fn std_key(&self, i: MeasIndex) -> StdKey {
        match self {
            Self::Filter => kws::Filter::std(i),
            // NOTE this is $PnL for all versions
            Self::Wavelength => kws::Wavelength::std(i),
            Self::Power => kws::Power::std(i),
            Self::DetectorType => kws::DetectorType::std(i),
            Self::DetectorVoltage => kws::DetectorVoltage::std(i),
            Self::PercentEmitted => kws::PercentEmitted::std(i),
            // NOTE this is $PnCALIBRATION for all versions
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
#[derive(new)]
pub struct ReadState<C> {
    pub(crate) file_len: FileLen,
    pub(crate) dataset_offset: DatasetOffset,
    pub(crate) conf: C,
}

#[derive(From, Into, Clone, Copy, Debug, Display)]
pub(crate) struct FileLen(pub(crate) u64);

#[derive(From, Into, Clone, Copy, Debug, PartialEq, Default, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromInnerPyObject))]
pub struct DatasetOffset(pub u64);

impl<C> ReadState<C> {
    pub(crate) fn open(
        p: &PathBuf,
        dataset_offset: DatasetOffset,
        conf: C,
    ) -> IOResult<(Self, File), DatasetOffsetError> {
        let file = File::options().read(true).open(p)?;
        Self::init(&file, dataset_offset, conf).map(|st| (st, file))
    }

    pub(crate) fn init(
        f: &File,
        dataset_offset: DatasetOffset,
        conf: C,
    ) -> IOResult<Self, DatasetOffsetError> {
        let m = f.metadata()?;
        let fl = m.len().into();
        if u64::from(fl) < u64::from(dataset_offset) {
            let e = DatasetOffsetError(dataset_offset, fl);
            return Err(ImpureError::Pure(e));
        }
        Ok(Self::new(fl, dataset_offset, conf))
    }

    pub(crate) fn as_innner_ref<X>(&self) -> ReadState<&X>
    where
        C: AsRef<X>,
    {
        ReadState::new(self.file_len, self.dataset_offset, self.conf.as_ref())
    }

    pub(crate) fn remaining_bytes<R: Seek>(&self, h: &mut BufReader<R>) -> io::Result<u64> {
        let pos = h.stream_position()?;
        let remaining = u64::from(self.file_len) - pos;
        Ok(remaining)
    }
}

#[derive(Error, Debug)]
#[error("dataset offset ({0}) exceeds file length ({1})")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ConfigError))]
pub struct DatasetOffsetError(DatasetOffset, FileLen);

#[cfg(feature = "python")]
mod python {
    use crate::python::ConfigError;
    use crate::segment::OffsetCorrection;

    use super::TimeMeasNamePattern;

    use pyo3::prelude::*;

    impl<'py> FromPyObject<'py> for TimeMeasNamePattern {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let s: String = ob.extract()?;
            let n = s
                .parse::<Self>()
                .map_err(|e| ConfigError::new_err(e.to_string()))?;
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

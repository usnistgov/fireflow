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
use crate::logging::{IOResult, ImpureError, LogResult, WarningsAndErrorsResult};
use crate::segment::{
    AnalysisSegmentId, DataSegmentId, HeaderCorrection, OtherSegmentId, PrimaryTextSegmentId,
    SupplementalTextSegmentId, TEXTCorrection,
};
use crate::text::index::MeasIndex;
use crate::text::keywords::{self as kws, AlphaNumType};
use crate::validated::ascii_range::OtherWidth;
use crate::validated::datepattern::DatePattern;
use crate::validated::keys::{
    IndexedKey as _, KeyString, KeyStringsOrPatterns, NonStdKeywords, NonStdKeywordsExt as _,
    StdKey, StdKeywords,
};
use crate::validated::keystring_pairs::KeyStringPairs;
use crate::validated::nonstd_meas_pattern::NonStdMeasPattern;
use crate::validated::sub_pattern::SubPattern;
use crate::validated::textdelim::TEXTDelim;
use crate::validated::timepattern::TimePattern;

use derive_more::{AsRef, Display, From, FromStr, FromStrError, Into};
use derive_new::new;
use regex::Regex;
use std::collections::HashMap;
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
    /// make "large" numbers fit. As such, this must be an integer between 8 and
    /// 20 (corresponding to a theoretical max of 2^64) but will default to 8
    /// since this is most logical.
    pub other_width: OtherWidth,

    /// Guess the width for OTHER segments.
    ///
    /// In case a width can't be found, fall back to [`Self::other_width`].
    pub guess_other_width: GuessOtherWidth,

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

    // NOTE the only reason this is here and not in the Keywords configs is
    // because this is needed to read the supplemental TEXT offsets
    /// Use a different version than what is given in the HEADER.
    ///
    /// If [`VersionOverride::Force`], force the version to be the supplied
    /// version.
    ///
    /// If [`VersionOverride::AutoDetect`], try to detect the version given the
    /// keywords in TEXT. This variant further takes a strategy as specified by
    /// [`SelectVersionStrategy`] to select the "best" version of multiple
    /// choices can accommodate the given keywords. If
    /// [`SelectVersionStrategy::Latest`] or
    /// [`SelectVersionStrategy::Earliest`], use the latest of earliest
    /// available version respectively. If [`SelectVersionStrategy::Loose`] or
    /// [`SelectVersionStrategy::Strict`], choose the version which has the most
    /// or least optional keywords. This will fail if no version can accommodate
    /// all required keywords from *TEXT*.
    ///
    /// If [`None`], do not change the version from HEADER.
    pub version_override: Option<VersionOverride>,

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
    pub allow_overlapping_supp_text: AllowOverlappingSuppTEXT,

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
    /// The guessing algorithm will be run after [`Self::trim_text_end`] since
    /// this may remove the last delimiter if necessary. It is also independent
    /// of [`Self::allow_odd`] and [`Self::allow_missing_final_delim`] which
    /// will trigger as normal if their respective violations are found.
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

    // TODO not used
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

    /// Trim whitespace from all values.
    ///
    /// This is mainly useful for the case of fixing offsets which are usually
    /// padded in order to make the TEXT segment a predictable length. These
    /// should be left-padded with numbers since the standard stipulates that
    /// offset values should only be numeric digits, but in many cases offsets
    /// are padded with spaces (on either side). Setting this to `true` will
    /// trim the spaces leaving just a number to be parsed.
    ///
    /// Trimming will be done as soon as the bytes are read from the file, thus
    /// preceding any other repair steps. Furthermore, trimming values has a
    /// relatively small performance hit since no additional string allocations
    /// are needed. If anything, it may improve performance since values that
    /// are entirely whitespace will become empty and thus be dropped.
    pub trim_value_whitespace: TrimValueWhitespace,

    /// If `true`, trim extra characters off the end of TEXT.
    ///
    /// This does two things (in this order):
    ///
    /// First, it will move the ending offset to the last delimiter in TEXT,
    /// thereby removing any non-delimiter characters (usually spaces if
    /// present). These are usually added to make TEXT a predictable length.
    ///
    /// Second, it will decrease the offset by one if the number of delimiters
    /// is even and the number of final consecutive delimiters is more than one.
    /// This will effectively remove the last delimiter, which sometimes
    /// erroneously exists.
    pub trim_text_end: TrimTEXTEnd,

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
    /// [`ReadStdKeywordsConfig::process_pseudostandard`], which is a crude
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
    /// Furthermore, many data structures and operations in `fireflow` are
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

    /// A pattern to find/match the $PnN of the time measurement.
    ///
    /// If matched, the time measurement must conform to the requirements of the
    /// target FCS version, such as having $TIMESTEP present and having a PnE
    /// set to `"0,0"`.
    pub time_meas_pattern: Option<TimeMeasNamePattern>,

    /// Allow time to be absent even [`Self::time_meas_pattern`] is set.
    pub allow_missing_time: AllowMissingTime,

    /// Force $PnE to be linear (`"0.0"`).
    pub force_linear_scale: ForceLinearScale,

    /// Ignore optical keywords in time channel.
    ///
    /// These are keys which the standard does not explicitly forbid but are
    /// nonsense for the time measurement.
    ///
    /// In the case of $PnG, the value is allowed to be set to 1.0 since this
    /// equates to a no-op.
    pub ignore_time_optical_keys: HashSet<TemporalOpticalKey>,

    /// Choose what to do with optical keywords in the time channel when found.
    ///
    /// Does nothing unless keys are specified in
    /// [`Self::ignore_time_optical_keys`].
    pub process_time_optical_keys: ProcessTemporalOpticalKeys,

    /// Choose how to interpret measurements in $SPILLOVER.
    ///
    /// Some files use numbers/indices rather than names which point to $PnN.
    /// Only the latter is standards-compliant.
    pub spillover_measurement_mode: SpilloverMeasurementMode,

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

    /// Process non-standard keywords starting with `"$"`.
    ///
    /// The `"$`" prefix is reserved for standard keywords only. While little
    /// harm may come from violating this, having these keywords might signify
    /// that the version in the HEADER is wrong and that the file actually
    /// follows a different FCS standard (usually higher) in which these
    /// keywords are standard.
    pub process_pseudostandard: ProcessPseudostandard,

    /// If `true`, allow keywords that have indices greater than $PAR.
    ///
    /// For instance, if $PAR, is 10 then $P11V would be considered a
    /// non-standard keyword since it is not part of a relevant measurement.
    /// Setting this to `true` turns the existence of these into a warning
    /// rather than an error.
    pub process_hyper_par: ProcessHyperPar,

    /// If `true`, allow standard keywords from a different version.
    ///
    /// Such errors (warnings if `true`) can likely be solved by overriding the
    /// version.
    pub process_other_version: ProcessOtherVersion,

    /// If `true`, allow $TIMESTEP to be unused.
    ///
    /// In reality this probably means there was a time measurement given in
    /// the dataset but was its $PnN was not properly matched. Setting this
    /// to `true` will suppress the resulting error, but one should make sure
    /// that time is indeed really missing.
    pub process_extra_timestep: ProcessExtraTimestep,

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
    /// If `true` timestamps with missing timezones will cause the key to error
    /// and be dropped or demoted depending on the value of
    /// [`ReadDataKeywordsConfig::process_optional_failure`].
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
    /// This will match something like `"P7FOO"` which would be `"FOO"` for
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
            force_linear_scale: ForceLinearScale::default(),
            ignore_time_optical_keys: HashSet::default(),
            process_time_optical_keys: ProcessTemporalOpticalKeys::default(),
            spillover_measurement_mode: SpilloverMeasurementMode::default(),
            date_pattern: None,
            time_pattern: None,
            datetime_pattern: None,
            last_modified_pattern: None,
            allow_other_feature: AllowOtherFeature::default(),
            process_pseudostandard: ProcessPseudostandard::default(),
            process_hyper_par: ProcessHyperPar::default(),
            process_other_version: ProcessOtherVersion::default(),
            process_extra_timestep: ProcessExtraTimestep::default(),
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

    /// Choose how to deal with optional keywords which produce errors.
    ///
    /// Also used when parsing any keyword in standard mode.
    pub process_optional_failure: ProcessOptionalFailure,

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
    /// $PnR values might indicate a typo or other issue.
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

/// Configuration to deal with optional standard keywords that cause errors
#[derive(Clone, Copy, Default, FromStr)]
#[cfg_attr(feature = "python", derive(FromPyString))]
#[from_str(rename_all = "snake_case")]
#[from_str(error(ParseGuessOtherWidthError))]
pub enum GuessOtherWidth {
    /// Do not guess
    #[default]
    None,
    /// Guess, throw error on failure.
    Error,
    /// Guess, throw warning on failure.
    ///
    /// Fall back to [`ReadHeaderInnerConfig::other_width`].
    Warn,
    /// Guess, do not throw warning or error on failure.
    ///
    /// Fall back to [`ReadHeaderInnerConfig::other_width`].
    Silent,
}

/// Error when parsing [`ProcessKeywordFailure`] from [`String`]
#[derive(Error, Debug, From)]
#[error("must be one of 'none', 'error', 'warn', or 'silent'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ConfigError))]
#[from(FromStrError)]
pub struct ParseGuessOtherWidthError;

impl GuessOtherWidth {
    // TODO not DRY
    pub(crate) fn into_tri_flag(self) -> Option<DummyTriFlag> {
        let r = match self {
            Self::None => None,
            Self::Error => Some(TriFlag::False),
            Self::Warn => Some(TriFlag::True),
            Self::Silent => Some(TriFlag::Silent),
        };
        r.map(Into::into)
    }
}

/// Configuration to override/detect FCS version
#[derive(Clone, Copy)]
#[cfg_attr(feature = "python", derive(FromPyString))]
pub enum VersionOverride {
    Force(Version),
    AutoDetect(SelectVersionStrategy),
}

impl FromStr for VersionOverride {
    type Err = VersionOverrideError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(ret) = s.parse::<Version>() {
            Ok(Self::Force(ret))
        } else if let Ok(ret) = s.parse::<SelectVersionStrategy>() {
            Ok(Self::AutoDetect(ret))
        } else {
            Err(VersionOverrideError)
        }
    }
}

/// Error when parsing [`VersionOverride`] from [`String`]
#[derive(Error, Debug)]
#[error("must be an FCS version string or one of 'latest', 'earliest', 'loose', or 'strict'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ConfigError))]
pub struct VersionOverrideError;

macro_rules! impl_proc_key_fail {
    ($t:ident) => {
        #[derive(Clone, Copy, Default, FromStr, Into)]
        #[cfg_attr(feature = "python", derive(FromPyString))]
        pub struct $t(pub ProcessKeywordFailure);

        impl ErrorFlag for $t {
            fn is_error(&self) -> bool {
                matches!(&self.0, ProcessKeywordFailure::Error)
            }
        }
    };
}

impl_proc_key_fail!(ProcessOptionalFailure);
impl_proc_key_fail!(ProcessOtherVersion);
impl_proc_key_fail!(ProcessHyperPar);
impl_proc_key_fail!(ProcessPseudostandard);
impl_proc_key_fail!(ProcessExtraTimestep);

impl ProcessOptionalFailure {
    pub(crate) fn is_demote(self) -> bool {
        matches!(&self.0, ProcessKeywordFailure::Demote)
    }
}

/// Configuration to deal with optional standard keywords that cause errors
#[derive(Clone, Copy, Default, FromStr)]
#[from_str(rename_all = "snake_case")]
#[from_str(error(ProcessKeywordFailureError))]
pub enum ProcessKeywordFailure {
    /// Throw an error
    #[default]
    Error,
    /// Demote to nonstandard with warning
    Demote,
    /// Demote to nonstandard with no warning
    DemoteSilent,
    /// Drop with warning
    Drop,
    /// Drop with no warning
    DropSilent,
}

/// Error when parsing [`ProcessKeywordFailure`] from [`String`]
#[derive(Error, Debug, From)]
#[error("must be one of 'error', 'demote', 'drop', or 'drop_silent'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ConfigError))]
#[from(FromStrError)]
pub struct ProcessKeywordFailureError;

impl ProcessKeywordFailure {
    pub(crate) fn as_triflag(self) -> DummyTriFlag {
        let flag = match self {
            Self::Error => TriFlag::False,
            Self::Demote | Self::Drop => TriFlag::True,
            Self::DemoteSilent | Self::DropSilent => TriFlag::Silent,
        };
        flag.into()
    }

    pub(crate) fn is_demote(self) -> bool {
        matches!(self, Self::Demote | Self::DemoteSilent)
    }
}

/// Strategy to use when autodetecting FCS version
#[derive(Clone, Copy, FromStr)]
#[from_str(error(SelectVersionStrategyError))]
#[from_str(rename_all = "snake_case")]
pub enum SelectVersionStrategy {
    /// Choose the latest version
    Latest,
    /// Choose the earliest version
    Earliest,
    /// Choose the version with the most optional keywords
    Loose,
    /// Choose the version with the least optional keywords
    Strict,
}

/// Error when parsing [`SelectVersionStrategy`] from [`String`].
///
/// This is never used directly and exists to satisfy the [`FromStr`] impl for
/// [`SelectVersionStrategy`].
#[derive(From)]
#[from(FromStrError)]
pub struct SelectVersionStrategyError;

/// Choose how to escape delims in TEXT segment.
#[derive(Default, Clone, Copy, FromStr)]
#[cfg_attr(feature = "python", derive(FromPyString))]
#[from_str(error(DelimEscapeModeError))]
#[from_str(rename_all = "snake_case")]
pub enum DelimEscapeMode {
    /// Use escaped delimiters.
    #[default]
    Escaped,
    /// Use unescaped delimiters.
    Unescaped,
    /// Guess, falling back to escaped mode.
    GuessEscaped,
    /// Guess, falling back to unescaped mode.
    GuessUnescaped,
}

/// Error when parsing [`DelimEscapeMode`] from [`String`]
#[derive(Error, Debug, From)]
#[error("must be one of 'escaped', 'unescaped', 'guess_escaped', or 'guess_unescaped'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ConfigError))]
#[from(FromStrError)]
pub struct DelimEscapeModeError;

/// Choose how to trim values and deal with blanks that may result.
#[derive(Default, Clone, Copy, FromStr)]
#[cfg_attr(feature = "python", derive(FromPyString))]
#[from_str(error(TrimValueWhitespaceError))]
#[from_str(rename_all = "snake_case")]
pub enum TrimValueWhitespace {
    /// Do not trim at all.
    #[default]
    Notrim,
    /// Trim whitespace and throw error if blank is created.
    Trim,
    /// Trim whitespace and throw warning if blank is created.
    TrimBlankWarn,
    /// Trim whitespace and do nothing if blank is created.
    TrimBlankNowarn,
}

/// Error when parsing [`TrimValueWhitespace`] from [`String`]
#[derive(Error, Debug, From)]
#[error("must be one of 'notrim', 'trim', 'trim_blank_warn', or 'trim_blank_nowarn'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ConfigError))]
#[from(FromStrError)]
pub struct TrimValueWhitespaceError;

impl TrimValueWhitespace {
    /// Emit a flag for handling blank values after trimming.
    ///
    /// Will be `None` if trimming is not set.
    pub(crate) fn into_allow_empty_flag(self) -> Option<DummyTriFlag> {
        let f = match self {
            Self::Notrim => None,
            Self::Trim => Some(TriFlag::False),
            Self::TrimBlankWarn => Some(TriFlag::True),
            Self::TrimBlankNowarn => Some(TriFlag::Silent),
        };
        f.map(Into::into)
    }
}

/// Choose which $PnE to force as linear.
#[derive(Default, Clone, Copy, FromStr)]
#[cfg_attr(feature = "python", derive(FromPyString))]
#[from_str(error(ForceLinearScaleError))]
#[from_str(rename_all = "snake_case")]
pub enum ForceLinearScale {
    /// Do not force.
    #[default]
    None,
    /// Only force the temporal measurement.
    TimeOnly,
    /// Force all measurements.
    All,
}

/// Error when parsing [`TruncateEventValues`] from [`String`]
#[derive(Error, Debug, From)]
#[error("must be one of 'time_only', 'all', or 'none'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ConfigError))]
#[from(FromStrError)]
pub struct ForceLinearScaleError;

impl ForceLinearScale {
    pub(crate) fn time_selected(self) -> bool {
        matches!(self, Self::TimeOnly | Self::All)
    }
}

/// Choose what to do with optical keys in time measurement when found.
#[derive(Default, Clone, Copy, FromStr)]
#[cfg_attr(feature = "python", derive(FromPyString))]
#[from_str(error(ProcessTimeOpticalKeysError))]
#[from_str(rename_all = "snake_case")]
pub enum ProcessTemporalOpticalKeys {
    /// Demote to nonstandard with warning
    #[default]
    Demote,
    /// Demote to nonstandard with no warning
    DemoteSilent,
    /// Drop with warning
    Drop,
    /// Drop with no warning
    DropSilent,
}

/// Error when parsing [`ProcessTemporalOpticalKeys`] from [`String`]
#[derive(Error, Debug, From)]
#[error("must be one of 'demote', 'demote_silent', 'drop', or 'drop_silent'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ConfigError))]
#[from(FromStrError)]
pub struct ProcessTimeOpticalKeysError;

/// Choose how to parse measurements for $SPILLOVER key
#[derive(Default, Clone, Copy, FromStr)]
#[cfg_attr(feature = "python", derive(FromPyString))]
#[from_str(error(SpilloverMeasurementModeError))]
#[from_str(rename_all = "snake_case")]
pub enum SpilloverMeasurementMode {
    /// Interpret measurements as names which match $PnN.
    #[default]
    Named,
    /// Interpret measurements as 1-indices (numbers) which point to measurements.
    Indexed,
    /// Guess how measurements should be interpreted.
    ///
    /// If they are all numbers and all do not point to $PnN, interpret as
    /// indices, otherwise names.
    Guess,
}

/// Error when parsing [`SpilloverMeasurementMode`] from [`String`]
#[derive(Error, Debug, From)]
#[error("must be one of 'named', 'indexed', or 'guess'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ConfigError))]
#[from(FromStrError)]
pub struct SpilloverMeasurementModeError;

/// Choose which event types are truncated.
///
/// By default only truncate when $DATATYPE (or $PnDATATYPE) is "I".
#[derive(Default, Clone, Copy, FromStr)]
#[cfg_attr(feature = "python", derive(FromPyString))]
#[from_str(error(TruncateEventValuesError))]
#[from_str(rename_all = "snake_case")]
pub enum TruncateEventValues {
    /// Only truncate integer events.
    #[default]
    IntOnly,
    /// Truncate all events.
    All,
    /// Truncate no events.
    None,
}

/// Error when parsing [`TruncateEventValues`] from [`String`]
#[derive(Error, Debug, From)]
#[error("must be one of 'int_only', 'all', or 'none'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ConfigError))]
#[from(FromStrError)]
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

pub trait ErrorFlag {
    fn is_error(&self) -> bool;
}

pub trait TriErrorFlag: From<TriFlag> + Into<TriFlag> + Copy {
    const FALSE_IS_ERROR: bool;

    fn is_error(&self) -> Option<bool> {
        match (*self).into() {
            TriFlag::Silent => None,
            TriFlag::False => Some(Self::FALSE_IS_ERROR),
            TriFlag::True => Some(!Self::FALSE_IS_ERROR),
        }
    }

    fn from_partial_str(s: &str) -> Result<Self, PartialTriErrorFlagError> {
        let res = match s {
            "silent" => Ok(TriFlag::Silent),
            "true" => Ok(TriFlag::True),
            _ => Err(PartialTriErrorFlagError),
        };
        res.map(Self::from)
    }
}

/// Error when parsing a [`TriFlag`] from `"true"` or `"silent"`.
#[derive(Error, Debug)]
#[error("Must be one of 'silent' or 'true'")]
pub struct PartialTriErrorFlagError;

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

impl_config_flag!(SquishOffsets);
impl_config_flag!(AllowNegative);
impl_config_flag!(TruncateOffsets);

impl_config_flag!(IgnoreSuppTEXT);
impl_config_flag!(UseLatin1);
impl_config_flag!(TrimTEXTEnd);
impl_config_flag!(IgnoreTEXTDataOffsets);
impl_config_flag!(IgnoreTEXTAnalysisOffsets);

impl_config_flag!(DedupMeasNames);
impl_config_flag!(TrimIntraValueWhitespace);
impl_config_flag!(AllowOtherFeature);
impl_config_flag!(IntegerWidthsFromByteord);
impl_config_flag!(TransferDroppedOptional);
impl_config_flag!(FixLogScaleOffsets);
impl_config_flag!(DisallowLocaltime);

impl_config_flag!(SkipConversionCheck);
impl_config_flag!(BigOther);
impl_config_flag!(AppendableFlag);
impl_config_flag!(AppendFlag);

// TODO add docstrings
macro_rules! impl_tri_error_flag {
    (true_is_error $n:ident) => {
        impl_tri_error_flag!(_common $n, false);
    };

    (false_is_error $n:ident) => {
        impl_tri_error_flag!(_common $n, true);
    };

    (_common $n:ident, $false_is_err:expr) => {
        #[derive(From, Into, Clone, Copy, FromStr, Default)]
        #[cfg_attr(feature = "python", derive(FromPyString))]
        pub struct $n(pub TriFlag);

        impl TriErrorFlag for $n {
            const FALSE_IS_ERROR: bool = $false_is_err;
        }
    };
}

impl_tri_error_flag!(false_is_error AllowOverlappingSuppTEXT);
impl_tri_error_flag!(false_is_error AllowNonAsciiDelim);
impl_tri_error_flag!(false_is_error AllowMissingFinalDelim);
impl_tri_error_flag!(false_is_error AllowNonunique);
impl_tri_error_flag!(false_is_error AllowOdd);
impl_tri_error_flag!(false_is_error AllowEmptyKeys);
impl_tri_error_flag!(false_is_error AllowDelimAtBoundary);
impl_tri_error_flag!(false_is_error AllowNonUtf8);
impl_tri_error_flag!(false_is_error AllowNonAsciiKeywords);
impl_tri_error_flag!(false_is_error AllowMissingSuppTEXT);
impl_tri_error_flag!(false_is_error AllowSuppTEXTOwnDelim);
impl_tri_error_flag!(false_is_error AllowMissingNextdata);
impl_tri_error_flag!(false_is_error AllowUnevenEventWidth);
impl_tri_error_flag!(false_is_error AllowTotMismatch);
impl_tri_error_flag!(false_is_error AllowHeaderTEXTOffsetMismatch);
impl_tri_error_flag!(false_is_error AllowMissingRequiredOffsets);
impl_tri_error_flag!(false_is_error AllowMissingTime);

impl_tri_error_flag!(true_is_error DisallowDeprecated);
impl_tri_error_flag!(true_is_error DisallowRangeTrunc);
impl_tri_error_flag!(true_is_error DisallowOverRange);

// flag for controlling imperfect downgrades and upgrades
impl_tri_error_flag!(false_is_error AllowLoss);

/// Fake 3-way flag to use for non-public switchable errors
#[derive(From, Into, Clone, Copy)]
pub(crate) struct DummyTriFlag(pub(crate) TriFlag);

impl TriErrorFlag for DummyTriFlag {
    const FALSE_IS_ERROR: bool = true;
}

/// Tri-state flag to throw warning, throw error, or do nothing
#[derive(Clone, Copy, FromStr, Default)]
#[from_str(error(TriFlagError))]
#[from_str(rename_all = "snake_case")]
pub enum TriFlag {
    #[default]
    False,
    True,
    Silent,
}

/// Error when parsing [`TriFlag`] from [`String`]
#[derive(Error, Debug, From)]
#[error("must be one of 'false', 'true', or 'silent'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ConfigError))]
#[from(FromStrError)]
pub struct TriFlagError;

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
#[derive(Clone, Copy, PartialEq, Eq, Hash, Display, Debug)]
#[cfg_attr(feature = "python", derive(FromPyString))]
pub enum TemporalOpticalKey {
    /// PnG
    #[display("G")]
    Gain,
    /// PnF
    #[display("F")]
    Filter,
    /// PnL
    #[display("W")]
    Wavelength,
    /// PnO
    #[display("O")]
    Power,
    /// PnT
    #[display("T")]
    DetectorType,
    /// PnV
    #[display("V")]
    DetectorVoltage,
    /// PnP
    #[display("P")]
    PercentEmitted,
    /// PnCALIBRATION
    #[display("CALIBRATION")]
    Calibration,
    /// PnDET
    #[display("DET")]
    DetectorName,
    /// PnTAG
    #[display("TAG")]
    Tag,
    /// PnFEATURE
    #[display("FEATURE")]
    Feature,
    /// PnANALYTE
    #[display("ANALYTE")]
    Analyte,
}

impl FromStr for TemporalOpticalKey {
    type Err = ParseTemporalOpticalKeyError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "G" => Ok(Self::Gain),
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
    "must be one of  'G', 'F', 'L', 'O', 'T', 'P', 'V', \
     'CALIBRATION', 'DET', 'TAG', 'FEATURE', or 'ANALYTE'"
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ConfigError))]
pub struct ParseTemporalOpticalKeyError;

type TemporalOpticalResult = WarningsAndErrorsResult<
    Vec<(StdKey, String)>,
    (),
    TemporalHasOpticalKeyError,
    TemporalHasOpticalKeyError,
>;

impl TemporalOpticalKey {
    pub(crate) fn std_key(self, i: MeasIndex) -> StdKey {
        match self {
            Self::Gain => kws::Gain::std(i),
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

    fn remove_keys_inner(
        targets: &[Self],
        ignore: &HashSet<Self>,
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        i: MeasIndex,
        flag: ProcessTemporalOpticalKeys,
    ) -> TemporalOpticalResult {
        let mut es = vec![];
        let mut ws = vec![];
        let mut pairs = vec![];
        for t in targets {
            let k = t.std_key(i);
            let (demote, warn) = match flag {
                ProcessTemporalOpticalKeys::Demote => (true, true),
                ProcessTemporalOpticalKeys::DemoteSilent => (true, false),
                ProcessTemporalOpticalKeys::Drop => (false, true),
                ProcessTemporalOpticalKeys::DropSilent => (false, false),
            };
            if let Some(v) = std.remove(&k) {
                let err = || TemporalHasOpticalKeyError::new(i, *t);
                if ignore.contains(t) {
                    if demote {
                        nonstd.insert_demoted(k.clone(), v.clone());
                    }
                    if warn {
                        ws.push(err());
                    }
                    pairs.push((k, v));
                } else {
                    es.push(err());
                }
            }
        }
        let mut res = LogResult::new_err_from_iter(es, pairs);
        res.extend_commutative_warnings(ws);
        res
    }

    pub(crate) fn remove_keys_2_0(
        ignore: &HashSet<Self>,
        kws: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        i: MeasIndex,
        flag: ProcessTemporalOpticalKeys,
    ) -> TemporalOpticalResult {
        let targets = [
            Self::DetectorType,
            Self::DetectorVoltage,
            Self::Filter,
            Self::PercentEmitted,
            Self::Power,
            Self::Wavelength,
        ];
        Self::remove_keys_inner(&targets, ignore, kws, nonstd, i, flag)
    }

    pub(crate) fn remove_keys_3_0(
        ignore: &HashSet<Self>,
        kws: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        i: MeasIndex,
        flag: ProcessTemporalOpticalKeys,
    ) -> TemporalOpticalResult {
        let targets = [
            Self::Gain,
            Self::DetectorType,
            Self::DetectorVoltage,
            Self::Filter,
            Self::PercentEmitted,
            Self::Power,
            Self::Wavelength,
        ];
        Self::remove_keys_inner(&targets, ignore, kws, nonstd, i, flag)
    }

    pub(crate) fn remove_keys_3_1(
        ignore: &HashSet<Self>,
        kws: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        i: MeasIndex,
        flag: ProcessTemporalOpticalKeys,
    ) -> TemporalOpticalResult {
        let targets = [
            Self::Gain,
            Self::Calibration,
            Self::DetectorType,
            Self::DetectorVoltage,
            Self::Filter,
            Self::PercentEmitted,
            Self::Power,
            Self::Wavelength,
        ];
        Self::remove_keys_inner(&targets, ignore, kws, nonstd, i, flag)
    }

    pub(crate) fn remove_keys_3_2(
        ignore: &HashSet<Self>,
        kws: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        i: MeasIndex,
        flag: ProcessTemporalOpticalKeys,
    ) -> TemporalOpticalResult {
        let targets = [
            Self::Gain,
            Self::Analyte,
            Self::Calibration,
            Self::DetectorName,
            Self::DetectorType,
            Self::DetectorVoltage,
            Self::Feature,
            Self::Filter,
            Self::PercentEmitted,
            Self::Power,
            Self::Tag,
            Self::Wavelength,
        ];
        Self::remove_keys_inner(&targets, ignore, kws, nonstd, i, flag)
    }
}

/// Error when optical keyword is present in temporal measurement.
#[derive(Debug, Error, new)]
#[error("optical key $P{index}{key} found in temporal measurement")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::RelationalError))]
pub struct TemporalHasOpticalKeyError {
    index: MeasIndex,
    key: TemporalOpticalKey,
}

/// A map of [`KeyString`]/[`String`] pairs.
///
/// The main use case for this is to replace or add key values.
pub type KeyStringValues = HashMap<KeyString, String>;

/// A list of patterns that match [`crate::validated::keys::StdKey`]s or
/// [`crate::validated::keys::NonStdKey`]s.
pub type KeyPatterns = KeyStringsOrPatterns<()>;

pub type SubPatterns = KeyStringsOrPatterns<SubPattern>;

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
    use crate::validated::sub_pattern::SubPattern;

    use super::{KeyPatterns, SubPatterns, TimeMeasNamePattern};

    use pyo3::prelude::*;
    use std::collections::HashMap;

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

    // pass keypatterns via config as a tuple like ([String], [String]) where the
    // first member is literal strings and the second is regex patterns
    impl<'py> FromPyObject<'py> for KeyPatterns {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let (lits, pats): (Vec<String>, Vec<String>) = ob.extract()?;
            let ret = Self::try_from_literals_and_patterns(
                lits.into_iter().map(|x| (x, ())),
                pats.into_iter().map(|x| (x, ())),
            )?;
            Ok(ret)
        }
    }

    type _SubPattern = HashMap<String, SubPattern>;

    // pass subpatterns via config as a tuple like ({String, (...)}, {String, (...)})
    // where the first member is literal strings and the second is regex patterns
    impl<'py> FromPyObject<'py> for SubPatterns {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let (lits, pats): (_SubPattern, _SubPattern) = ob.extract()?;
            let ret = Self::try_from_literals_and_patterns(lits, pats)?;
            Ok(ret)
        }
    }
}

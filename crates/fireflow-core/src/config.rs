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
use crate::text::keywords as kws;
use crate::validated::ascii_range::OtherWidth;
use crate::validated::datepattern::DatePattern;
use crate::validated::keys::{
    KeyString, KeyStringsOrPatterns, NonStdKeywords, NonStdKeywordsExt as _, StdKey, StdKeywords,
};
use crate::validated::keystring_pairs::KeyStringPairs;
use crate::validated::nonstd_meas_pattern::NonStdMeasPattern;
use crate::validated::sub_pattern::SubPattern;
use crate::validated::textdelim::TEXTDelim;
use crate::validated::timepattern::TimePattern;

// pub export here to keep the docs for all configuration flags in this crate
pub use fireflow_types::config::{
    AllowHeaderTEXTOffsetMismatch, AllowHeaderTEXTOffsetMismatchError, DelimEscapeMode,
    DelimEscapeModeError, ForceLinearScale, ForceLinearScaleError, GuessOtherWidth,
    GuessOtherWidthError, ProcessKeywordFailureError, ProcessTemporalOpticalKeys,
    ProcessTemporalOpticalKeysError, SpilloverMeasurementMode, SpilloverMeasurementModeError,
    TemporalOpticalKey, TemporalOpticalKeyError, TriFlag, TriFlagError, TrimValueWhitespace,
    TrimValueWhitespaceError, TruncateEventValues, TruncateEventValuesError,
};

use fireflow_types::config::{
    ProcessKeywordFailure, VERSION_EARLIEST_LEVEL, VERSION_LATEST_LEVEL, VERSION_LOOSE_LEVEL,
    VERSION_STRICT_LEVEL,
};
use fireflow_types::config::{TIME_MEAS_NAME_PATTERN_DEFAULT, TIME_MEAS_NAME_PATTERN_NONE};

use derive_more::{AsRef, Display, From, FromStr, FromStrError, Into};
use derive_new::new;
use regex::{self, Regex};
use thiserror::Error;

use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Seek};
use std::path::PathBuf;
use std::str::FromStr;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{DisplayAsPyErr, FromInnerPyObject, FromPyString},
    fireflow_types::python as py,
    pyo3::prelude::*,
};

/// Instructions for reading the HEADER segment.
#[derive(Default, Clone, AsRef, From)]
pub struct ReadHeaderConfig {
    pub header: ReadHeaderInnerConfig,
    pub offset: ReadOffsetConfig,
}

/// Instructions for reading the HEADER and TEXT segments in flat mode.
#[derive(Default, Clone, AsRef)]
pub struct ReadFlatTEXTConfig {
    #[as_ref(ReadHeaderInnerConfig, ReadHeaderAndTEXTConfig)]
    pub flat: ReadHeaderAndTEXTConfig,

    #[as_ref(ReadOffsetConfig)]
    pub offset: ReadOffsetConfig,

    pub shared: ReadSharedConfig,
}

/// Instructions for reading the HEADER and TEXT segments in standard mode.
#[derive(Default, Clone, AsRef)]
pub struct ReadStdTEXTConfig {
    #[as_ref(ReadHeaderInnerConfig, ReadHeaderAndTEXTConfig)]
    pub flat: ReadHeaderAndTEXTConfig,

    #[as_ref(ReadOffsetConfig)]
    pub offset: ReadOffsetConfig,

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
    pub flat: ReadHeaderAndTEXTConfig,

    #[as_ref(ReadOffsetConfig)]
    pub offset: ReadOffsetConfig,

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
    pub flat: ReadHeaderAndTEXTConfig,

    #[as_ref(ReadOffsetConfig)]
    pub offset: ReadOffsetConfig,

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
    #[as_ref(ReadOffsetConfig)]
    pub offset: ReadOffsetConfig,

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
    #[as_ref(ReadOffsetConfig)]
    pub offset: ReadOffsetConfig,

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
}

/// Specific instructions for reading offsets
#[derive(Default, Clone, Copy)]
pub struct ReadOffsetConfig {
    /// Allow offsets that are like `X,X-1`.
    ///
    /// An empty offset is supposed to be written as 0,0 according to the
    /// standard. However, this is actually nonsense given that the begin and
    /// end offsets point to the first and last byte; thus 0,0 points to bytes 0
    /// and 0 for begin and end respectively, which is one byte and not zero.
    /// Therefore, some vendors (understandably) write an "empty" offset as 0,-1
    /// which actually is zero bytes long. However, -1 is not a valid offset.
    /// Additionally, some vendors do the same pattern for a non-zero offset,
    /// such as 1000,999.
    ///
    /// This flag will treat all such offsets as if they were written as `0,0`.
    pub allow_pseudoempty: AllowPseudoempty,

    /// Maximum that may be truncated from offsets that exceed EOF.
    ///
    /// For some files, the DATA ending offset is one greater than it should be,
    /// which means it points to the byte directly after the file ending. Set
    /// this to `1` to allow truncating this ending offset down by one byte.
    ///
    /// In other cases, offsets far beyond EOF likely mean the file was
    /// incompletely written, which is a larger problem itself. Setting this to
    /// a large value will at least allow these files to be read.
    pub truncate_offset_limit: TruncateOffsetLimit,

    /// Number of bytes to adjust ending offsets in case of overlap.
    ///
    /// If one segment overlaps another, it will often be because the two are
    /// adjacent and the final offset of the first segment is one greater than
    /// it should be, which also means it is equal to the beginning offset of
    /// the second segment. In basically all (sane) programming languages and
    /// related, this makes sense since the ending index is non-inclusive. This
    /// is not the way FCS works, thus it is a common mistake.
    ///
    /// If this is non-zero, the ending offset will be adjusted up to the number
    /// of indicated bytes such that the two offsets no longer overlap (if
    /// possible given the limit). For most cases, this only needs to be `1`.
    pub overlap_correction_limit: OverlapCorrectionLimit,

    // TODO move this to event config since it only applies to reading DATA
    /// The maximum number of bytes to correct DATA based on event width.
    ///
    /// For all but ASCII delimited layouts, dividing length of DATA by event
    /// width should exactly equal $TOT. In some cases, DATA will be too long by
    /// one byte, and thus this division will produce a remainder of 1. This
    /// flag will permit remainders up to a certain limit which will then be
    /// used to correct the ending offset so that DATA is a perfect multiple of
    /// event width.
    ///
    /// Note, the ending offset will only be decreased, so this assumes that the
    /// ending offset is between 0 and event width bytes too long. If it is too
    /// short, this will trigger a different error for $TOT not matching the
    /// computed number of events.
    pub data_remainder_limit: DataRemainderLimit,
}

/// Specific instructions for reading the TEXT segment as flat key/value pairs.
#[derive(Default, Clone, AsRef)]
pub struct ReadHeaderAndTEXTConfig {
    /// Config for reading HEADER
    #[as_ref(ReadHeaderInnerConfig)]
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
    pub allow_duplicated_supp_text: AllowDuplicatedSuppTEXT,

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
#[derive(Clone, Default)]
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
    pub time_meas_pattern: TimeMeasNamePattern,

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
    pub nonstandard_measurement_pattern: NonStdMeasPatternOpt,
}

/// Specific instructions for reading a data layout.
///
/// Note that some of these are also when reading any keyword in standard mode.
/// Since the layout keywords always need to be read, and the rest only need to
/// be read specifically when building [`crate::core::CoreTEXT`] or
/// [`crate::core::CoreDataset`], these options are here since the layout is the
/// thing they have in common.
#[derive(Default, Clone, Copy, AsRef)]
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
#[derive(Default, Clone, Copy)]
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
#[derive(Default, Clone, Copy)]
pub struct ReadSharedConfig {
    /// If `true`, all warnings are considered to be fatal errors.
    pub warnings_are_errors: bool,

    /// If `true`, do not emit warnings.
    pub hide_warnings: bool,
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
#[cfg_attr(feature = "python", pyerr(py::ConfigError))]
pub struct VersionOverrideError;

macro_rules! impl_proc_key_fail {
    ($t:ident) => {
        #[derive(Clone, Copy, Default, FromStr, Into, From)]
        #[cfg_attr(feature = "python", derive(FromPyString))]
        pub struct $t(pub ProcessKeywordFailure);

        impl ErrorFlag for $t {
            fn is_error(&self) -> bool {
                matches!(&self.0, ProcessKeywordFailure::Error)
            }
        }

        impl $t {
            pub(crate) fn as_triflag(self) -> DummyTriFlag {
                let flag = match self.0 {
                    ProcessKeywordFailure::Error => TriFlag::False,
                    ProcessKeywordFailure::DemoteWarn | ProcessKeywordFailure::DropWarn => {
                        TriFlag::True
                    }
                    ProcessKeywordFailure::DemoteSilent | ProcessKeywordFailure::DropSilent => {
                        TriFlag::Silent
                    }
                };
                flag.into()
            }

            pub(crate) fn is_demote(self) -> bool {
                matches!(
                    self.0,
                    ProcessKeywordFailure::DemoteWarn | ProcessKeywordFailure::DemoteSilent
                )
            }
        }
    };
}

impl_proc_key_fail!(ProcessOptionalFailure);
impl_proc_key_fail!(ProcessOtherVersion);
impl_proc_key_fail!(ProcessHyperPar);
impl_proc_key_fail!(ProcessPseudostandard);
impl_proc_key_fail!(ProcessExtraTimestep);

/// Strategy to use when autodetecting FCS version
#[derive(Clone, Copy)]
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

impl FromStr for SelectVersionStrategy {
    type Err = SelectVersionStrategyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            VERSION_LATEST_LEVEL => Ok(Self::Latest),
            VERSION_EARLIEST_LEVEL => Ok(Self::Earliest),
            VERSION_LOOSE_LEVEL => Ok(Self::Loose),
            VERSION_STRICT_LEVEL => Ok(Self::Strict),
            _ => Err(SelectVersionStrategyError),
        }
    }
}

/// Error when parsing [`SelectVersionStrategy`] from [`String`].
///
/// This is never used directly and exists to satisfy the [`FromStr`] impl for
/// [`SelectVersionStrategy`].
#[derive(From)]
#[from(FromStrError)]
pub struct SelectVersionStrategyError;

/// Overall strategy to read FCS files.
///
/// This is a "metaflag" which will activate individual flags in each
/// configuration struct. The exact flags to be activated will depend on the
/// struct. In all cases, this will activate the flags which emit warnings where
/// applicable. If one does not desire warnings, use
/// [`ReadSharedConfig::hide_warnings`].
///
/// In general, the different levels for this are a tradeoff between the ability
/// to read events from DATA vs preserving metadata.
#[derive(Clone, Copy, Default, FromStr)]
#[from_str(rename_all = "snake_case")]
#[from_str(error(ReadStrategyError))]
pub enum ReadStrategy {
    /// Follow the standard fully (configuration is totally default).
    ///
    /// Many files will fail this, but it is useful for validation.
    #[default]
    Strict,
    /// Use "safe" non-compliant parsing that is unlikely to result in data loss.
    ///
    /// This is likely a good option for many files.
    Scalpal,
    /// Use "unsafe" non-compliant parsing.
    ///
    /// This is the best option when all one cares about is reading DATA.
    /// Non-compliant metadata in TEXT will be skipped.
    Sledgehammer,
}

#[derive(Error, Debug, From)]
#[error("must be one of 'strict', 'scalpal', 'sledgehammer'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConfigError))]
#[from(FromStrError)]
pub struct ReadStrategyError;

pub trait HasStrategy {
    #[must_use]
    fn new_with_strategy(strat: ReadStrategy) -> Self
    where
        Self: Default,
    {
        let mut conf = Self::default();
        conf.with_strategy(strat);
        conf
    }

    fn with_strategy(&mut self, strat: ReadStrategy) {
        match strat {
            ReadStrategy::Strict => (),
            ReadStrategy::Scalpal => self.with_scalpal(),
            ReadStrategy::Sledgehammer => {
                self.with_scalpal();
                self.with_sledgehammer();
            }
        }
    }

    fn with_scalpal(&mut self);

    fn with_sledgehammer(&mut self) {}
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
impl_config_flag!(AllowPseudoempty);

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

impl_tri_error_flag!(false_is_error AllowDuplicatedSuppTEXT);
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

impl DummyTriFlag {
    /// Emit a flag for handling blank values after trimming.
    ///
    /// Will be `None` if trimming is not set.
    pub(crate) fn from_trim_value_whitespace(x: TrimValueWhitespace) -> Option<Self> {
        let f = match x {
            TrimValueWhitespace::Notrim => None,
            TrimValueWhitespace::Trim => Some(TriFlag::False),
            TrimValueWhitespace::TrimBlankWarn => Some(TriFlag::True),
            TrimValueWhitespace::TrimBlankSilent => Some(TriFlag::Silent),
        };
        f.map(Into::into)
    }

    // TODO not DRY
    pub(crate) fn from_guess_other_width(x: GuessOtherWidth) -> Option<Self> {
        let r = match x {
            GuessOtherWidth::None => None,
            GuessOtherWidth::Error => Some(TriFlag::False),
            GuessOtherWidth::Warn => Some(TriFlag::True),
            GuessOtherWidth::Silent => Some(TriFlag::Silent),
        };
        r.map(Into::into)
    }
}

impl TriErrorFlag for DummyTriFlag {
    const FALSE_IS_ERROR: bool = true;
}

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
#[derive(Clone)]
pub struct TimeMeasNamePattern(pub Option<Regex>);

impl fmt::Display for TimeMeasNamePattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        if let Some(s) = self.0.as_ref() {
            write!(f, "{s}")
        } else {
            f.write_str(TIME_MEAS_NAME_PATTERN_NONE)
        }
    }
}

impl FromStr for TimeMeasNamePattern {
    type Err = regex::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == TIME_MEAS_NAME_PATTERN_NONE {
            return Ok(Self(None));
        }
        s.parse::<Regex>().map(Some).map(Self)
    }
}

impl Default for TimeMeasNamePattern {
    fn default() -> Self {
        Self(Some(Regex::new(TIME_MEAS_NAME_PATTERN_DEFAULT).unwrap()))
    }
}

/// [`NonStdMeasPattern`] wrapper to implement non-None default.
#[derive(Clone)]
pub struct NonStdMeasPatternOpt(pub Option<NonStdMeasPattern>);

impl Default for NonStdMeasPatternOpt {
    fn default() -> Self {
        Self(Some(NonStdMeasPattern::default()))
    }
}

/// Error when optical keyword is present in temporal measurement.
#[derive(Debug, Error, new)]
#[error("optical key $P{index}{key} found in temporal measurement")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
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

/// The maximum number of bytes that an offset may be truncated if beyond EOF.
#[derive(Default, Clone, Copy, From, Into, FromStr)]
pub struct TruncateOffsetLimit(pub u64);

/// The maximum number of bytes an ending offset may be decreased to avoid overlap.
#[derive(Default, Clone, Copy, From, Into, FromStr)]
pub struct OverlapCorrectionLimit(pub u64);

/// The maximum number of bytes the DATA ending offset may be decreased based on event width.
#[derive(Default, Clone, Copy, From, Into, FromStr)]
pub struct DataRemainderLimit(pub u64);

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

    pub(crate) fn remaining_bytes<R: Seek>(&self, h: &mut BufReader<R>) -> io::Result<u64> {
        let pos = h.stream_position()?;
        let remaining = u64::from(self.file_len) - pos;
        Ok(remaining)
    }
}

#[derive(Error, Debug)]
#[error("dataset offset ({0}) exceeds file length ({1})")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConfigError))]
pub struct DatasetOffsetError(DatasetOffset, FileLen);

type TemporalOpticalResult = WarningsAndErrorsResult<
    Vec<(StdKey, String)>,
    (),
    TemporalHasOpticalKeyError,
    TemporalHasOpticalKeyError,
>;

pub(crate) fn remove_temporal_optical_keys(
    targets: &[TemporalOpticalKey],
    ignore: &HashSet<TemporalOpticalKey>,
    std: &mut StdKeywords,
    nonstd: &mut NonStdKeywords,
    i: MeasIndex,
    flag: ProcessTemporalOpticalKeys,
) -> TemporalOpticalResult {
    let mut es = vec![];
    let mut ws = vec![];
    let mut pairs = vec![];
    for t in targets {
        let k = StdKey::from_temporal_optical_key(*t, i);
        let (demote, warn) = match flag {
            ProcessTemporalOpticalKeys::DemoteWarn => (true, true),
            ProcessTemporalOpticalKeys::DemoteSilent => (true, false),
            ProcessTemporalOpticalKeys::DropWarn => (false, true),
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

impl HasStrategy for ReadHeaderConfig {
    fn with_scalpal(&mut self) {
        self.header.with_scalpal();
        self.offset.with_scalpal();
    }

    fn with_sledgehammer(&mut self) {
        self.header.with_sledgehammer();
        self.offset.with_sledgehammer();
    }
}

impl HasStrategy for ReadFlatTEXTConfig {
    fn with_scalpal(&mut self) {
        self.flat.with_scalpal();
        self.offset.with_scalpal();
    }

    fn with_sledgehammer(&mut self) {
        self.flat.with_sledgehammer();
        self.offset.with_sledgehammer();
    }
}

impl HasStrategy for ReadStdTEXTConfig {
    fn with_scalpal(&mut self) {
        self.flat.with_scalpal();
        self.offset.with_scalpal();
        self.standard.with_scalpal();
        self.layout.with_scalpal();
    }

    fn with_sledgehammer(&mut self) {
        self.flat.with_sledgehammer();
        self.offset.with_sledgehammer();
        self.standard.with_sledgehammer();
        self.layout.with_sledgehammer();
    }
}

impl HasStrategy for ReadFlatDatasetConfig {
    fn with_scalpal(&mut self) {
        self.flat.with_scalpal();
        self.offset.with_scalpal();
        self.layout.with_scalpal();
        self.data.with_scalpal();
    }

    fn with_sledgehammer(&mut self) {
        self.flat.with_sledgehammer();
        self.offset.with_sledgehammer();
        self.layout.with_sledgehammer();
        self.data.with_sledgehammer();
    }
}

impl HasStrategy for ReadStdDatasetConfig {
    fn with_scalpal(&mut self) {
        self.flat.with_scalpal();
        self.offset.with_scalpal();
        self.standard.with_scalpal();
        self.layout.with_scalpal();
        self.data.with_scalpal();
    }

    fn with_sledgehammer(&mut self) {
        self.flat.with_sledgehammer();
        self.offset.with_sledgehammer();
        self.standard.with_sledgehammer();
        self.layout.with_sledgehammer();
        self.data.with_sledgehammer();
    }
}

impl HasStrategy for ReadFlatDatasetFromKeywordsConfig {
    fn with_scalpal(&mut self) {
        self.offset.with_scalpal();
        self.layout.with_scalpal();
        self.data.with_scalpal();
    }

    fn with_sledgehammer(&mut self) {
        self.offset.with_sledgehammer();
        self.layout.with_sledgehammer();
        self.data.with_sledgehammer();
    }
}

impl HasStrategy for NewCoreTEXTConfig {
    fn with_scalpal(&mut self) {
        self.standard.with_scalpal();
        self.layout.with_scalpal();
    }

    fn with_sledgehammer(&mut self) {
        self.standard.with_sledgehammer();
        self.layout.with_sledgehammer();
    }
}

impl HasStrategy for NewCoreDatasetConfig {
    fn with_scalpal(&mut self) {
        self.offset.with_scalpal();
        self.standard.with_scalpal();
        self.layout.with_scalpal();
        self.data.with_scalpal();
    }

    fn with_sledgehammer(&mut self) {
        self.offset.with_sledgehammer();
        self.standard.with_sledgehammer();
        self.layout.with_sledgehammer();
        self.data.with_sledgehammer();
    }
}

impl HasStrategy for ReadHeaderInnerConfig {
    fn with_scalpal(&mut self) {
        self.guess_other_width = GuessOtherWidth::Warn;
        self.squish_offsets = true.into();
    }

    fn with_sledgehammer(&mut self) {
        self.max_other = Some(0);
    }
}

impl HasStrategy for ReadOffsetConfig {
    fn with_scalpal(&mut self) {
        self.allow_pseudoempty = true.into();
        // Allow automatic correction of off-by-one offset errors. This won't
        // always work but will likely take care of %80 of cases.
        self.truncate_offset_limit = 1.into();
        self.overlap_correction_limit = 1.into();
        self.data_remainder_limit = 1.into();
    }
}

impl HasStrategy for ReadHeaderAndTEXTConfig {
    fn with_scalpal(&mut self) {
        self.header.with_scalpal();
        self.version_override = Some(VersionOverride::AutoDetect(SelectVersionStrategy::Loose));
        self.delim_escape_mode = DelimEscapeMode::GuessEscaped;
        self.allow_duplicated_supp_text = TriFlag::True.into();
        self.allow_non_ascii_delim = TriFlag::True.into();
        self.allow_missing_final_delim = TriFlag::True.into();
        self.allow_nonunique = TriFlag::True.into();
        self.allow_odd = TriFlag::True.into();
        self.allow_empty_keys = TriFlag::True.into();
        self.allow_delim_at_boundary = TriFlag::True.into();
        self.allow_non_utf8 = TriFlag::True.into();
        self.allow_non_ascii_keywords = TriFlag::True.into();
        self.allow_missing_supp_text = TriFlag::True.into();
        self.allow_supp_text_own_delim = TriFlag::True.into();
        self.allow_missing_nextdata = TriFlag::True.into();
        self.trim_value_whitespace = TrimValueWhitespace::TrimBlankWarn;
        self.trim_text_end = true.into();
    }

    fn with_sledgehammer(&mut self) {
        self.header.with_sledgehammer();
        self.ignore_supp_text = true.into();
    }
}

impl HasStrategy for ReadStdKeywordsConfig {
    fn with_scalpal(&mut self) {
        self.dedup_measurement_names = true.into();
        self.trim_intra_value_whitespace = true.into();
        self.spillover_measurement_mode = SpilloverMeasurementMode::Guess;
        self.allow_other_feature = true.into();
        self.fix_log_scale_offsets = true.into();
        // This flag all optical keys as ignorable in the time measurement.
        // The next flag tells what to do with them (in this case, demote)
        self.ignore_time_optical_keys = TemporalOpticalKey::all();
        self.process_time_optical_keys = ProcessTemporalOpticalKeys::DemoteWarn;
        self.process_pseudostandard = ProcessKeywordFailure::DemoteWarn.into();
        self.process_hyper_par = ProcessKeywordFailure::DemoteWarn.into();
        self.process_other_version = ProcessKeywordFailure::DemoteWarn.into();
        self.process_extra_timestep = ProcessKeywordFailure::DemoteWarn.into();
    }

    fn with_sledgehammer(&mut self) {
        self.process_time_optical_keys = ProcessTemporalOpticalKeys::DropWarn;
        self.process_pseudostandard = ProcessKeywordFailure::DropWarn.into();
        self.process_hyper_par = ProcessKeywordFailure::DropWarn.into();
        self.process_other_version = ProcessKeywordFailure::DropWarn.into();
        self.process_extra_timestep = ProcessKeywordFailure::DropWarn.into();
        self.allow_missing_time = TriFlag::True.into();
        // This will make $PnE compatible with all layouts at the expense of
        // destroying any log-scaling information.
        self.force_linear_scale = ForceLinearScale::All;
    }
}

impl HasStrategy for ReadDataKeywordsConfig {
    fn with_scalpal(&mut self) {
        self.allow_header_text_offset_mismatch = AllowHeaderTEXTOffsetMismatch::HeaderWarn;
        self.allow_missing_required_offsets = TriFlag::True.into();
        self.process_optional_failure = ProcessKeywordFailure::DemoteWarn.into();
    }

    fn with_sledgehammer(&mut self) {
        self.process_optional_failure = ProcessKeywordFailure::DropWarn.into();
        self.ignore_text_analysis_offsets = true.into();
    }
}

impl HasStrategy for ReadEventsConfig {
    fn with_scalpal(&mut self) {
        self.allow_uneven_event_width = TriFlag::True.into();
        self.allow_tot_mismatch = TriFlag::True.into();
    }
}

#[cfg(feature = "python")]
mod python {
    use super::{KeyPatterns, NonStdMeasPatternOpt, SubPatterns, TimeMeasNamePattern};

    use crate::segment::OffsetCorrection;
    use crate::validated::nonstd_meas_pattern::NonStdMeasPattern;
    use crate::validated::sub_pattern::SubPattern;

    use fireflow_types::python::ConfigError;

    use pyo3::prelude::*;
    use std::collections::HashMap;

    impl<'py> FromPyObject<'py> for TimeMeasNamePattern {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let s: String = ob.extract()?;
            s.parse::<Self>()
                .map_err(|e| ConfigError::new_err(e.to_string()))
        }
    }

    impl<'py> FromPyObject<'py> for NonStdMeasPatternOpt {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            if ob.is_none() {
                Ok(Self(None))
            } else {
                let s: String = ob.extract()?;
                Ok(Self(Some(s.parse::<NonStdMeasPattern>()?)))
            }
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

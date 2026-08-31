use crate::{
    byteord::ConfigByteOrd,
    index::MeasIndex,
    keystring::KeyStringsOrPatterns,
    keywords::Version,
    macros::{impl_config_flag, impl_str_enum},
    ne_str,
    nonempty_string::NEStr,
    other_width::OtherWidth,
    ranged_float::PositiveFloat,
    segment::{
        AnalysisSegmentId, DataSegmentId, HeaderCorrection, OtherSegmentId, PrimaryTextSegmentId,
        SupplementalTextSegmentId, TEXTCorrection,
    },
    sub_pattern::SubPattern,
    textdelim::TEXTDelim,
};

use const_format::formatcp;
use derive_more::{AsRef, Display, From, FromStr, FromStrError, Into};
use derive_new::new;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use num_traits::One as _;
use regex::Regex;
use thiserror::Error;

use std::{
    collections::HashSet,
    fmt,
    fs::{File, OpenOptions},
    hash::Hash,
    num::NonZeroU8,
    str::FromStr,
};

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {
    crate::python as py,
    fireflow_core_proc::{
        DisplayAsPyErr, FromInnerPyObject, FromPyString, IntoPyString, TryFromPyObject,
    },
    pyo3::prelude::*,
};

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

    /// If `true` compute the CRC while writing
    pub compute_crc: ComputeWriteCRC,

    /// If `true`, overwrite $FIL with the supplied file name when writing.
    pub override_fil: OverrideFIL,
}

/// Specific configuration for writing one dataset
#[derive(Clone, Copy, Default, new)]
pub struct WriteDatasetInnerConfig {
    pub text: WriteTEXTInnerConfig,

    /// If `true`, allow integer event values in DATA to exceed bitmask before writing.
    pub allow_over_bitmask: AllowOverBitmask,

    /// If `true`, forbid event values in DATA to exceed $PnR before writing.
    pub disallow_over_range: DisallowOverRange,

    /// Set the size in bytes for the internal buffer used to write DATA.
    ///
    /// This is the same as [`ReadDatasetConfig::row_buffer_size`]; see there
    /// for details.
    pub row_buffer_size: RowBufferSize,
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
#[cfg_attr(feature = "python", derive(IntoPyObject))]
pub struct ReadHeaderInnerConfig {
    /// Corrections for primary TEXT offsets
    pub text_correction: HeaderCorrection<PrimaryTextSegmentId>,

    /// Corrections for DATA offsets
    pub data_correction: HeaderCorrection<DataSegmentId>,

    /// Corrections for ANALYSIS offsets
    pub analysis_correction: HeaderCorrection<AnalysisSegmentId>,

    /// Corrections for OTHER offset pairs if they exist.
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

    /// Guess the width for OTHER offsets.
    ///
    /// In case a width can't be found, fall back to [`Self::other_width`].
    pub guess_other_width: GuessOtherWidth,

    /// If `true` and the 2nd value of an offset pair is zero, treat as empty.
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
#[cfg_attr(feature = "python", derive(IntoPyObject))]
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

    /// Maximum to be truncated from offsets that exceed the end of the dataset.
    ///
    /// The end of a dataset is defined either by end of file (EOF) or $NEXTDATA
    /// if set.
    ///
    /// For some files, the DATA ending offset is one greater than it should be,
    /// which means it points to the byte directly after the dataset end. Set
    /// this to `1` to allow truncating this ending offset down by one byte.
    ///
    /// In other cases, offsets far beyond the dataset end likely mean the file
    /// was incompletely written, which is a larger problem itself. Setting this
    /// to a large value will at least allow these files to be read.
    pub dataset_overflow_limit: DatasetOverflowLimit,

    /// Number of bytes to adjust ending offsets in case of overlap.
    ///
    /// If one offset pair overlaps another, it will often be because the two
    /// are adjacent and the final offset of the first pair is one greater than
    /// it should be, which also means it is equal to the beginning offset of
    /// the second pair. In basically all (sane) programming languages and
    /// related, this makes sense since the ending index is non-inclusive. This
    /// is not the way FCS works, thus it is a common mistake.
    ///
    /// If this is non-zero, the ending offset will be adjusted up to the number
    /// of indicated bytes such that the two offsets no longer overlap (if
    /// possible given the limit). For most cases, this only needs to be `1`.
    pub overlap_correction_limit: OverlapCorrectionLimit,
}

/// Specific instructions for reading the TEXT segment as flat key/value pairs.
#[derive(Default, Clone, AsRef)]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
pub struct ReadHeaderAndTEXTConfig {
    // NOTE the only reason this is here and not in the Keywords configs is
    // because this is needed to read the supplemental TEXT offsets
    /// Use a different version than what is given in the HEADER.
    ///
    /// If [`None`], make no attempt to change the version from HEADER.
    pub version_override: Option<VersionOverride>,

    /// Corrections for supplemental TEXT offsets
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

    /// Totally ignore STEXT and its offsets.
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
    /// The guessing algorithm is independent of [`Self::allow_odd_tokens`] and
    /// [`Self::allow_even_delims`] which will trigger as normal if their
    /// respective violations are found.
    pub delim_escape_mode: DelimEscapeMode,

    /// Allow delimiter to be character outside 1-126.
    pub allow_non_ascii_delim: AllowNonAsciiDelim,

    /// Allow TEXT to contain an even number of delimiters.
    ///
    /// TEXT should only contain an odd number of delimiters. This is
    /// independent of escape mode.
    pub allow_even_delims: AllowEvenDelims,

    /// Allow TEXT to contain an odd number of tokens.
    ///
    /// The final "dangling" token in the odd case will be dropped as it has no
    /// obvious interpretation.
    pub allow_odd_tokens: AllowOddTokens,

    /// Allow non-unique keys to be present in TEXT.
    ///
    /// In any case, only the first value for a given key will be used. Setting
    /// this to `true` merely changes a duplicate key to emit a warning and not
    /// an error.
    pub allow_nonunique: AllowNonunique,

    /// Allow blank keys.
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

    /// Allow delimiters at token boundaries.
    ///
    /// This is only relevant for escaped mode.
    ///
    /// Regardless of this value, delimiters at token boundaries will not be
    /// included due to their ambiguity.
    pub allow_delim_at_boundary: AllowDelimAtBoundary,

    /// Choose the character encoding scheme for TEXT.
    pub use_encoding: UseEncoding,

    /// Allow keys with non-ASCII characters.
    ///
    /// This only applies to non-standard keywords, as all standardized keywords
    /// may only contain letters, numbers, and start with '$'. Regardless, all
    /// compliant keys must only have ASCII.
    pub allow_non_ascii_keys: AllowNonAsciiKeywords,

    /// Allow values with non-UTF8 characters.
    ///
    /// Tokens with such bytes will be dropped regardless of this keyword.
    pub allow_non_utf8_values: AllowNonUtf8,

    /// Allow STEXT offsets to be missing from TEXT.
    ///
    /// Does not affect FCS 3.2 since STEXT is optional there.
    pub allow_missing_supp_text: AllowMissingSuppTEXT,

    /// Allow STEXT to use a different delimiter than TEXT.
    pub allow_supp_text_own_delim: AllowSuppTEXTOwnDelim,

    /// Allow $NEXTDATA to be missing.
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
}

/// Specific instructions for standardizing keywords from TEXT
#[derive(Clone, Default)]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
pub struct ReadStdKeywordsConfig_<TMP, DP, TP, DTP, LMP> {
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
    pub time_meas_pattern: TMP,

    /// Allow time to be absent even [`Self::time_meas_pattern`] is set.
    pub allow_missing_time: AllowMissingTime,

    /// Set $TIMESTEP if it is not present and required.
    ///
    /// This will do nothing on FCS2.0 files since this version does not
    /// specify $TIMESTEP.
    pub add_missing_timestep: Option<PositiveFloat>,

    /// Force $PnE to be linear (`"0,0"`) if it is not already.
    ///
    /// Affected columns will never fail; any value they have will be mapped to
    /// `"0,0"`.
    ///
    /// This may be necessary for some files which set $DATATYPE to be `"F"` or
    /// `"D"` which do not allow log scaling.
    pub force_linear_scale: ForceLinearScale,

    /// Ignore optical keywords in time channel.
    ///
    /// These are keys which the standard does not explicitly forbid but are
    /// nonsense for the time measurement.
    ///
    /// In the case of $PnG, the value is allowed to be set to 1.0 since this
    /// equates to a no-op.
    pub ignore_optical_only_keys: OpticalOnlyKeys,

    /// Choose what to do with optical keywords in the time channel when found.
    ///
    /// Does nothing unless keys are specified in
    /// [`Self::ignore_optical_only_keys`].
    pub process_optical_only_keys: ProcessOpticalOnlyKeys,

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
    pub date_pattern: DP,

    /// If `true`, will be used as an alternative pattern toe parse $BTIM/$ETIM.
    pub time_pattern: TP,

    /// If set, will be used to parse $BEGINDATETIME and $ENDDATETIME.
    ///
    /// It should follow the format outline in
    /// [chrono](https://docs.rs/chrono/latest/chrono/format/strftime/index.html).
    /// If not supplied, timestamps will be parsed as an ISO-formatted timestamp
    /// possibly with a timezone.
    pub datetime_pattern: DTP,

    /// If set, will be used to parse $LAST_MODIFIED.
    ///
    /// It should follow the format outline in
    /// [chrono](https://docs.rs/chrono/latest/chrono/format/strftime/index.html).
    /// If not supplied, timestamps will be parsed according to the standard
    /// format which is `"%d-%b-%Y %H:%M:%S"` possibly with centiseconds after.
    pub last_modified_pattern: LMP,

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
}

/// Specific instructions for reading a data layout.
///
/// Note that some of these are also when reading any keyword in standard mode.
/// Since the layout keywords always need to be read, and the rest only need to
/// be read specifically when building [`crate::core::CoreTEXT`] or
/// [`crate::core::CoreDataset`], these options are here since the layout is the
/// thing they have in common.
#[derive(Default, Clone, AsRef)]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
pub struct ReadDataKeywordsConfig_<ISK, RSK, PTS, DFS, RSKV, ASK, SSKV> {
    /// Remove standard keys from TEXT.
    ///
    /// Comparisons will be case-insensitive. Members of this list should not
    /// try to match the leading "$" as this is implied.
    ///
    /// This will be applied before [`Self::rename_standard_keys`],
    /// [`Self::promote_to_standard`], and [`Self::demote_from_standard`].
    pub ignore_standard_keys: ISK,

    /// Rename standard keys in TEXT.
    ///
    /// Keys matching the first part of the pair will be replaced by the second.
    /// The leading "$" is implied so keys in this table should not include it.
    /// Comparisons are case-insensitive.
    ///
    /// Keys are renamed before [`Self::promote_to_standard`] and
    /// [`Self::demote_from_standard`] are applied.
    pub rename_standard_keys: RSK,

    /// A list of nonstandard keywords to be "promoted" to standard.
    ///
    /// All matching keywords will be prefixed with a "$" and added to the pool
    /// of standard keywords to be processed downstream when deriving data
    /// layouts, measurement metadata, etc. Matching will be case-insensitive.
    pub promote_to_standard: PTS,

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
    pub demote_from_standard: DFS,

    /// Replace values of standard keys.
    ///
    /// Keys will be matched in case-insensitive manner. The leading "$" is
    /// implied, so keys in this table should not include it.
    pub replace_standard_key_values: RSKV,

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
    pub append_standard_keywords: ASK,

    /// Apply substitution patterns to standard key values.
    ///
    /// This is like a substitution operation in sed or perl. Patterns matched
    /// with a regexp will be replaced, possibly with captures.
    pub substitute_standard_key_values: SSKV,

    /// Choose how to handle key collisions when repairing keywords.
    ///
    /// Non-unique keywords will not be kept in the final FCS file since each
    /// list of standard and non-standard keywords must be unique.
    pub allow_repair_non_unique: AllowRepairNonUnique,

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

    /// If given, fix $PnB values using $BYTEORD.
    ///
    /// This only has an effect for FCS 2.0-3.0 where $DATATYPE=I.
    pub int_width_override: IntWidthOverride,

    /// If given, override the $BYTEORD keyword for 2.0-3.0 integer layouts.
    ///
    /// In some files the $BYTEORD does not match $PnB, all of which must be
    /// $BYTEORD * 8. This option will override $BYTEORD from the file. $BYTEORD
    /// will still be read, so this option will not salvage a badly-formatted
    /// $BYTEORD value, which will need a different intervention.
    ///
    /// Obviously this must match the actual layout of the numbers in DATA. If
    /// $PnB is also incorrect, use [`Self::int_width_override`] to override those
    /// values as well.
    pub byteord_override: ByteordOverride,

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

/// Specific instructions for reading entire dataset in addition to TEXT.
#[derive(Default, Clone, Copy)]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
pub struct ReadDatasetConfig {
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

    /// If `true`, allow event width to not perfectly divide DATA.
    ///
    /// In practice, having such a mismatch likely means either &PnB or the DATA
    /// offsets are incorrect.
    ///
    /// Does not apply to delimited ASCII, which does not have a fixed width.
    ///
    /// This flag will do nothing if [`Self::data_remainder_limit`] is used to
    /// fix the DATA offsets such that they have no remainder.
    pub allow_uneven_event_width: AllowUnevenEventWidth,

    /// If `true`, allow $TOT to not match number of events in DATA.
    ///
    /// For all but delimited ASCII layouts, $TOT is unnecessary and can be
    /// computed by dividing the bytes in DATA by the event width computed from
    /// all $PnB. If $TOT does not match this, it may indicate an issue. If
    /// `false`, throw an error on mismatch, and warning otherwise.
    pub allow_tot_mismatch: AllowTotMismatch,

    /// Choose how to deal with bitmasks for integers.
    ///
    /// If 'true', throw error if integer is outside bitmask. If `false`
    /// truncate and throw warning. If `silent`, truncate with no warning.
    pub over_bitmask_action: OverBitmaskAction,

    /// How to handle overrange values.
    pub over_range_action: OverRangeAction,

    /// Permit the CRC word after the final segment to be missing.
    ///
    /// In FCS 3.0 and up, if the CRC is not stored then the 8 bytes after
    /// the last segment must be set to ASCII `0`.
    pub allow_missing_crc: AllowMissingCRC,

    /// Permit the computed checksum to not match the CRC word.
    pub allow_mismatch_crc: AllowMismatchCRC,

    /// Compute the CRC for the dataset.
    ///
    /// This is disabled by default because in reality most FCS files don't seem
    /// to use checksums, thus the added compute cost (mostly IO, see below for
    /// full explanation) is not worth it.
    ///
    /// The CRC will be computed for every byte in the dataset up to the end of
    /// the final segment in the dataset. This includes all "dead space" in
    /// between segments which may be filled with spaces, null characters, or
    /// whatever else the software decided to throw in. This entire byte region
    /// will be read from disk in one fell swoop to compute the checksum, but
    /// this also means that previously read segments for TEXT, DATA, etc will
    /// be read twice. In the future, this may be optimized, but for now it is
    /// the easiest and sanest way to compute this.
    pub compute_crc: ComputeCRC,

    /// If `true` read bytes which are between segments.
    pub read_intra_segment_dark_bytes: ReadIntraSegmentDarkBytes,

    /// If `true` read bytes between the end of this dataset and the next.
    pub read_post_dataset_dark_bytes: ReadPostDatasetDarkBytes,

    /// Set the size in bytes for the internal buffer used to read DATA.
    ///
    /// This is a performance tuning parameter which controls the
    /// cache-coherence of the data being read. Setting this too low will read
    /// DATA in smaller chunks which will produce more syscalls (slower);
    /// setting this too high will cause cache misses (also slower). It should
    /// generally be 90% of your CPU's L1D cache size.
    ///
    /// It defaults to `28_000` with the assumption that most CPUs have 32k L1D
    /// caches. Setting this to a higher value if your CPU has a larger cache
    /// may increase throughput.
    pub row_buffer_size: RowBufferSize,
}

/// Configuration options for across all reading functions
#[derive(Default, Clone, Copy)]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
pub struct ReadSharedConfig {
    /// If `true`, all warnings are considered to be fatal errors.
    pub warnings_are_errors: bool,

    /// If `true`, do not emit warnings.
    pub hide_warnings: bool,
}

// Declare configuration flags
//
// These are config values which can only be true or false. They are wrapped in
// their own type to aid documentation and extracting by reference if necessary.

pub trait ConfigFlag {
    fn is_set(&self) -> bool;
}

macro_rules! _impl_config_flag {
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

_impl_config_flag!(SquishOffsets);
_impl_config_flag!(AllowPseudoempty);

_impl_config_flag!(IgnoreSuppTEXT);
_impl_config_flag!(IgnoreTEXTDataOffsets);
_impl_config_flag!(IgnoreTEXTAnalysisOffsets);

_impl_config_flag!(DedupMeasNames);
_impl_config_flag!(TrimIntraValueWhitespace);
_impl_config_flag!(AllowOtherFeature);
_impl_config_flag!(IntegerWidthsFromByteord);
_impl_config_flag!(TransferDroppedOptional);
_impl_config_flag!(FixLogScaleOffsets);
_impl_config_flag!(DisallowLocaltime);
_impl_config_flag!(ReadIntraSegmentDarkBytes);
_impl_config_flag!(ReadPostDatasetDarkBytes);

_impl_config_flag!(SkipConversionCheck);
_impl_config_flag!(BigOther);
_impl_config_flag!(OverrideFIL);
_impl_config_flag!(ComputeWriteCRC);
_impl_config_flag!(AppendableFlag);
_impl_config_flag!(AppendFlag);

impl AppendFlag {
    #[must_use]
    pub fn file_options(self) -> OpenOptions {
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

// Declare tri-state flags
//
// These are flags which may be true, false, or "silent" (noop).
//
// Additionally, the false or true levels may represent an error state
// corresponding to "allow*" or "disallow*" flags.

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

/// Error when parsing a [`fireflow_types::config::TriFlag`] from `"true"` or `"silent"`.
#[derive(Error, Debug)]
#[error("Must be one of 'silent' or 'true'")]
pub struct PartialTriErrorFlagError;

macro_rules! impl_tri_error_flag {
    (true_is_error $n:ident) => {
        impl_tri_error_flag!(_common $n, false);
    };

    (false_is_error $n:ident) => {
        impl_tri_error_flag!(_common $n, true);
    };

    (_common $n:ident, $false_is_err:expr) => {
        #[derive(From, Into, Clone, Copy, FromStr, Display, Default)]
        #[cfg_attr(feature = "python", derive(FromPyString))]
        #[cfg_attr(feature = "python", derive(IntoPyString))]
        pub struct $n(pub TriFlag);

        impl TriErrorFlag for $n {
            const FALSE_IS_ERROR: bool = $false_is_err;
        }
    };
}

impl_tri_error_flag!(false_is_error AllowDuplicatedSuppTEXT);
impl_tri_error_flag!(false_is_error AllowNonAsciiDelim);
impl_tri_error_flag!(false_is_error AllowEvenDelims);
impl_tri_error_flag!(false_is_error AllowNonunique);
impl_tri_error_flag!(false_is_error AllowOddTokens);
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
impl_tri_error_flag!(false_is_error AllowRepairNonUnique);

impl_tri_error_flag!(true_is_error DisallowRangeTrunc);

// flag for controlling imperfect downgrades and upgrades
impl_tri_error_flag!(false_is_error AllowLoss);

// flag or controlling how to deal with overrange values in read-only case
impl_tri_error_flag!(true_is_error AllowOverBitmask);
impl_tri_error_flag!(true_is_error DisallowOverRange);

impl_tri_error_flag!(false_is_error AllowMissingCRC);
impl_tri_error_flag!(false_is_error AllowMismatchCRC);

/// Fake 3-way flag to use for non-public switchable errors
#[derive(From, Into, Clone, Copy)]
pub struct DummyTriFlag(pub(crate) TriFlag);

impl DummyTriFlag {
    /// Emit a flag for handling blank values after trimming.
    ///
    /// Will be `None` if trimming is not set.
    pub fn from_trim_value_whitespace(x: TrimValueWhitespace) -> Option<Self> {
        let f = match x {
            TrimValueWhitespace::Notrim => None,
            TrimValueWhitespace::Trim => Some(TriFlag::False),
            TrimValueWhitespace::TrimBlankWarn => Some(TriFlag::True),
            TrimValueWhitespace::TrimBlankSilent => Some(TriFlag::Silent),
        };
        f.map(Into::into)
    }

    pub fn from_guess_other_width(x: GuessOtherWidth) -> Option<Self> {
        let r = match x {
            GuessOtherWidth::None => None,
            GuessOtherWidth::Error => Some(TriFlag::False),
            GuessOtherWidth::Warn => Some(TriFlag::True),
            GuessOtherWidth::Silent => Some(TriFlag::Silent),
        };
        r.map(Into::into)
    }

    #[must_use]
    pub fn from_over_limit_action(x: OverLimitAction) -> Self {
        let f = match x {
            OverLimitAction::Error => TriFlag::False,
            OverLimitAction::Warn | OverLimitAction::TruncateWarn => TriFlag::True,
            OverLimitAction::Silent | OverLimitAction::TruncateSilent | OverLimitAction::None => {
                TriFlag::Silent
            }
        };
        f.into()
    }
}

impl TriErrorFlag for DummyTriFlag {
    const FALSE_IS_ERROR: bool = true;
}

// Declare misc configuration types

/// Configuration to override/detect FCS version.
#[derive(Clone, Copy)]
#[cfg_attr(feature = "python", derive(FromPyString))]
#[cfg_attr(feature = "python", derive(IntoPyString))]
pub enum VersionOverride {
    /// Force the version to one chosen by the user.
    Force(Version),

    /// Attempt to autodetect the version based on keyword presence/absence.
    ///
    /// Versions will be ranked and chosen in multiple stages.
    ///
    /// 1. Eliminate all versions that would result in missing required
    ///    keywords. If this results in zero versions, the selection fails.
    ///
    /// 2. Eliminate all versions that require dropping optional keywords. If
    ///    this results in zero versions, choose the version with the least
    ///    number of dropped optional keywords. If there are multiple versions,
    ///    compare using the criteria in (3) below. Also use (3) to break ties
    ///    for versions that have the same number of dropped keywords.
    ///
    /// 3. Rank versions based on the strategy encoded in this enum.
    ///    [`SelectVersionStrategy`] will provide the overall ranking function,
    ///    and `prioritize_current` will force the current version to the top
    ///    of the ranking if available.
    AutoDetect {
        strategy: SelectVersionStrategy,
        prioritize_current: bool,
    },
}

impl FromStr for VersionOverride {
    type Err = VersionOverrideError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(ret) = s.parse::<Version>() {
            Ok(Self::Force(ret))
        } else {
            let (t, p) = match s {
                VERSION_LATEST_LEVEL => (SelectVersionStrategy::Latest, false),
                VERSION_EARLIEST_LEVEL => (SelectVersionStrategy::Earliest, false),
                VERSION_LOOSE_LEVEL => (SelectVersionStrategy::Loose, false),
                VERSION_STRICT_LEVEL => (SelectVersionStrategy::Strict, false),
                VERSION_CURRENT_OR_LATEST_LEVEL => (SelectVersionStrategy::Latest, true),
                VERSION_CURRENT_OR_EARLIEST_LEVEL => (SelectVersionStrategy::Earliest, true),
                VERSION_CURRENT_OR_LOOSE_LEVEL => (SelectVersionStrategy::Loose, true),
                VERSION_CURRENT_OR_STRICT_LEVEL => (SelectVersionStrategy::Strict, true),
                _ => return Err(VersionOverrideError),
            };
            Ok(Self::AutoDetect {
                strategy: t,
                prioritize_current: p,
            })
        }
    }
}

impl fmt::Display for VersionOverride {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Self::Force(x) => x.fmt(f),
            Self::AutoDetect {
                strategy,
                prioritize_current,
            } => {
                let s = match (strategy, prioritize_current) {
                    (SelectVersionStrategy::Earliest, false) => VERSION_EARLIEST_LEVEL,
                    (SelectVersionStrategy::Latest, false) => VERSION_LATEST_LEVEL,
                    (SelectVersionStrategy::Loose, false) => VERSION_LOOSE_LEVEL,
                    (SelectVersionStrategy::Strict, false) => VERSION_STRICT_LEVEL,
                    (SelectVersionStrategy::Earliest, true) => VERSION_CURRENT_OR_EARLIEST_LEVEL,
                    (SelectVersionStrategy::Latest, true) => VERSION_CURRENT_OR_LATEST_LEVEL,
                    (SelectVersionStrategy::Loose, true) => VERSION_CURRENT_OR_LOOSE_LEVEL,
                    (SelectVersionStrategy::Strict, true) => VERSION_CURRENT_OR_STRICT_LEVEL,
                };
                write!(f, "{s}")
            }
        }
    }
}

/// Strategy to use when autodetecting FCS version.
///
/// This will only be used to break ties between two versions that have the same
/// number of keywords that must be dropped.
#[derive(Clone, Copy)]
pub enum SelectVersionStrategy {
    /// Choose the latest version.
    Latest,
    /// Choose the earliest version.
    Earliest,
    /// Choose the version with the most optional keywords.
    Loose,
    /// Choose the version with the least optional keywords.
    Strict,
}

/// Error when parsing [`SelectVersionStrategy`] from [`String`].
///
/// This is never used directly and exists to satisfy the [`FromStr`] impl for
/// [`SelectVersionStrategy`].
#[derive(From)]
#[from(FromStrError)]
pub struct SelectVersionStrategyError;

/// Error when parsing [`VersionOverride`] from [`String`]
#[derive(Error, Debug)]
#[error(
    "must be an FCS version string or one of '{}', '{}', '{}', '{}', '{}', \
     '{}', '{}', or '{}'.",
    VERSION_LATEST_LEVEL,
    VERSION_EARLIEST_LEVEL,
    VERSION_LOOSE_LEVEL,
    VERSION_STRICT_LEVEL,
    VERSION_CURRENT_OR_LATEST_LEVEL,
    VERSION_CURRENT_OR_EARLIEST_LEVEL,
    VERSION_CURRENT_OR_LOOSE_LEVEL,
    VERSION_CURRENT_OR_STRICT_LEVEL
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConfigError))]
pub struct VersionOverrideError;

pub const VERSION_STRATEGY_ALL_LEVELS: [&str; 8] = [
    VERSION_LATEST_LEVEL,
    VERSION_EARLIEST_LEVEL,
    VERSION_STRICT_LEVEL,
    VERSION_LOOSE_LEVEL,
    VERSION_CURRENT_OR_LATEST_LEVEL,
    VERSION_CURRENT_OR_EARLIEST_LEVEL,
    VERSION_CURRENT_OR_LOOSE_LEVEL,
    VERSION_CURRENT_OR_STRICT_LEVEL,
];

pub const VERSION_LATEST_LEVEL: &str = "latest";
pub const VERSION_EARLIEST_LEVEL: &str = "earliest";
pub const VERSION_LOOSE_LEVEL: &str = "loose";
pub const VERSION_STRICT_LEVEL: &str = "strict";
pub const VERSION_CURRENT_OR_LATEST_LEVEL: &str = "current_or_latest";
pub const VERSION_CURRENT_OR_EARLIEST_LEVEL: &str = "current_or_earliest";
pub const VERSION_CURRENT_OR_LOOSE_LEVEL: &str = "current_or_loose";
pub const VERSION_CURRENT_OR_STRICT_LEVEL: &str = "current_or_strict";

/// Fix $PnB for 2.0/3.0 integer layouts.
///
/// Some files set $PnB to the bits implied by $PnR (ie the bitmask). For
/// instance, if $PnR is 1024, $PnB is set to 10, which is incorrect since $PnB
/// must be a multiple of 8 (NOTE this is a restriction of this library; the
/// standard allows such $PnB values though exceedingly rare and not advised).
#[derive(Clone, Copy, Default)]
pub enum IntWidthOverride {
    /// Do nothing
    #[default]
    Never,
    /// Override with an explicit value for all $PnB.
    Explicit(NumericByteWidth),
    /// Round $PnB up to the next multiple of 8.
    NextByte,
}

/// Override $BYTEORD for FCS 2.0/3.0.
#[derive(Clone, Default)]
pub enum ByteordOverride {
    /// Do nothing
    #[default]
    None,
    /// Override with an explicit value for $BYTEORD.
    ///
    /// This will also set $PnB. It must match the constraints imposed by
    /// $DATATYPE.
    Explicit(ConfigByteOrd),
    /// Infer endian-ness from $BYTEORD, ignoring its length.
    ///
    /// Endian-ness is little if $BYTEORD is monotonic ascending, and big if
    /// monotonic descending. Length will be inferred from $PnB, which should
    /// all be the same. If $PnB is not a multiple of 8, this will fail.
    ///
    /// This is option is ignored for mixed $BYTEORD.
    Endian,
}

#[derive(
    Clone, Copy, PartialEq, Eq, Hash, TryFromPrimitive, IntoPrimitive, Debug, Display, FromStr,
)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[repr(u8)]
#[display("{}", u8::from(*self))]
pub enum NumericByteWidth {
    B1 = 1,
    B2,
    B3,
    B4,
    B5,
    B6,
    B7,
    B8,
}

impl NumericByteWidth {
    /// Return number of bytes needed to express the given u64.
    #[must_use]
    pub fn from_u64(x: u64) -> Self {
        // find position of most-significant non-zero byte
        x.to_le_bytes()
            .iter()
            .rposition(|i| *i > 0)
            .and_then(|i| u8::try_from(i + 1).ok())
            .and_then(|i| Self::try_from(i).ok())
            .unwrap_or(Self::B1)
    }
}

impl From<NumericByteWidth> for NonZeroU8 {
    fn from(value: NumericByteWidth) -> Self {
        // ASSUME this will never fail because Bytes is 1-8
        Self::new(u8::from(value)).unwrap()
    }
}

/// The size of the row buffer used to read DATA.
///
/// The minimum size is 4k.
#[derive(Clone, Copy, Into, Display)]
#[cfg_attr(feature = "python", derive(IntoPyObject, TryFromPyObject))]
pub struct RowBufferSize(usize);

impl Default for RowBufferSize {
    fn default() -> Self {
        // 90% of the most common L1D cache size which is 32k
        Self(28_000)
    }
}

/// Error when making new [`RowBufferSize`].
#[derive(Error, Debug)]
#[error("Row buffer size must be greater than {MIN_ROW_BUFFER_SIZE} bytes")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ConfigError))]
pub struct RowBufferSizeError;

impl FromStr for RowBufferSize {
    type Err = RowBufferSizeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_from(s.parse::<usize>().map_err(|_| RowBufferSizeError)?)
    }
}

impl TryFrom<usize> for RowBufferSize {
    type Error = RowBufferSizeError;
    fn try_from(value: usize) -> Result<Self, Self::Error> {
        if value < MIN_ROW_BUFFER_SIZE {
            Err(RowBufferSizeError)
        } else {
            Ok(Self(value))
        }
    }
}

const MIN_ROW_BUFFER_SIZE: usize = 4096;

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

/// Error when optical keyword is present in temporal measurement.
#[derive(Debug, Error, new, PartialEq, Clone)]
#[error("optical key $P{index}{key} found in temporal measurement")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct TemporalHasOpticalKeyError {
    index: MeasIndex,
    key: OpticalOnlyKey,
}

/// A list of patterns that match [`crate::validated::keys::StdKey`]s or
/// [`crate::validated::keys::NonStdKey`]s.
pub type KeyPatterns = KeyStringsOrPatterns<()>;

pub type SubPatterns = KeyStringsOrPatterns<SubPattern>;

/// The maximum number of bytes that an offset may be truncated if beyond EOF.
#[derive(Default, Clone, Copy, From, Into, FromStr)]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
pub struct DatasetOverflowLimit(pub u64);

/// The maximum number of bytes an ending offset may be decreased to avoid overlap.
#[derive(Default, Clone, Copy, From, Into, FromStr)]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
pub struct OverlapCorrectionLimit(pub u64);

/// The max length the DATA end offset may be decreased based on event width.
#[derive(Default, Clone, Copy, From, Into, FromStr)]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
pub struct DataRemainderLimit(pub u64);

/// Set of temporal optical keys.
#[derive(Clone, Default, From, Into)]
pub struct OpticalOnlyKeys(pub HashSet<OpticalOnlyKey>);

impl OpticalOnlyKeys {
    fn all() -> Self {
        let keys = [
            OpticalOnlyKey::Gain,
            OpticalOnlyKey::Analyte,
            OpticalOnlyKey::Calibration,
            OpticalOnlyKey::DetectorName,
            OpticalOnlyKey::DetectorType,
            OpticalOnlyKey::DetectorVoltage,
            OpticalOnlyKey::Feature,
            OpticalOnlyKey::Filter,
            OpticalOnlyKey::PercentEmitted,
            OpticalOnlyKey::Power,
            OpticalOnlyKey::Tag,
            OpticalOnlyKey::Wavelength,
        ];
        Self(keys.into_iter().collect())
    }
}

// Declare flags which are used for processing keys
//
// These are not special except for the fact that multiple flags follow the same
// pattern, hence macro.

macro_rules! impl_proc_key_fail {
    ($t:ident) => {
        #[derive(Clone, Copy, Default, FromStr, Display, Into, From)]
        #[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
        pub struct $t(pub ProcessKeywordFailure);

        impl ErrorFlag for $t {
            fn is_error(&self) -> bool {
                matches!(&self.0, ProcessKeywordFailure::Error)
            }
        }

        impl $t {
            pub fn as_triflag(self) -> DummyTriFlag {
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

            pub fn is_demote(self) -> bool {
                self.is_demote_or_drop() == Some(true)
            }

            pub fn is_demote_or_drop(self) -> Option<bool> {
                match self.0 {
                    ProcessKeywordFailure::DemoteWarn | ProcessKeywordFailure::DemoteSilent => {
                        Some(true)
                    }
                    ProcessKeywordFailure::DropWarn | ProcessKeywordFailure::DropSilent => {
                        Some(false)
                    }
                    ProcessKeywordFailure::Error => None,
                }
            }
        }
    };
}

impl_proc_key_fail!(ProcessOptionalFailure);
impl_proc_key_fail!(ProcessOtherVersion);
impl_proc_key_fail!(ProcessHyperPar);
impl_proc_key_fail!(ProcessPseudostandard);
impl_proc_key_fail!(ProcessExtraTimestep);

// Declare config values which are non-empty string enums.

/// An enum where each variant has a non-empty string value.
pub trait EnumStrIter<const LEN: usize>: Sized {
    const ITEMS: [Self; LEN];

    fn as_ne_str(&self) -> &'static NEStr;

    fn as_str(&self) -> &'static str {
        self.as_ne_str().as_ref()
    }

    fn first_ne_str() -> &'static NEStr {
        assert!(LEN > 0, "enum str literal is empty");
        Self::ITEMS[0].as_ne_str()
    }

    #[must_use]
    fn first_str() -> &'static str {
        Self::first_ne_str().as_str()
    }

    fn iter() -> impl Iterator<Item = Self> {
        Self::ITEMS.into_iter()
    }

    #[must_use]
    fn iter_str() -> impl Iterator<Item = &'static str> {
        Self::iter().map(|x| Self::as_str(&x))
    }
}

pub const TRI_FALSE_LEVEL: &NEStr = FALSE_LEVEL;
pub const TRI_TRUE_LEVEL: &NEStr = TRUE_LEVEL;
pub const TRI_SILENT_LEVEL: &NEStr = SILENT_LEVEL;

impl_config_flag!(
    /// Tri-state flag to throw warning, throw error, or do nothing
    pub TriFlag,
    /// Error when parsing [`TriFlag`] from [`String`]
    pub TriFlagError,
    False  => TRI_FALSE_LEVEL,
    True   => TRI_TRUE_LEVEL,
    Silent => TRI_SILENT_LEVEL
);

pub const ENCODING_UTF8_LEVEL: &NEStr = ne_str!("utf8");
pub const ENCODING_SINGLE_LEVEL: &NEStr = ne_str!("single");
pub const ENCODING_GUESS_LEVEL: &NEStr = ne_str!("guess");

impl_config_flag!(
    /// Choose how to interpret the characters in TEXT.
    ///
    /// If `utf8`, use UTF8. If `single`, use IANA ISO/IEC-8859-1), which will
    /// map each byte to a character, including those outside ASCII. This is
    /// useful if TEXT is encoded with non-UTF8 characters. If `guess`, assume
    /// UTF8 and fall back to IANA ISO/IEC-8859-1 if a non-UTF8 character is
    /// encountered.
    pub UseEncoding,
    /// Error when parsing [`UseEncoding`] from [`String`]
    pub UseEncodingError,
    Utf8   =>  ENCODING_UTF8_LEVEL,
    Single =>  ENCODING_SINGLE_LEVEL,
    Guess  => ENCODING_GUESS_LEVEL
);

/// The encoding to use when reading TEXT.
#[derive(Clone, Copy, Default)]
pub enum Encoding {
    #[default]
    Utf8,
    Single,
}

impl UseEncoding {
    /// Choose encoding to use to read `bytes`.
    ///
    /// Only read `bytes` if `Guess` is selected, in which case
    /// [`Encoding::Single`] will be returned if any bytes have the most
    /// significant bit set to 1.
    #[must_use]
    pub fn choose(&self, bytes: &[u8]) -> Encoding {
        match self {
            Self::Utf8 => Encoding::Utf8,
            Self::Single => Encoding::Single,
            Self::Guess => {
                if str::from_utf8(bytes).is_ok() {
                    Encoding::Utf8
                } else {
                    Encoding::Single
                }
            }
        }
    }
}

impl Encoding {
    #[must_use]
    pub fn is_multi(&self) -> bool {
        matches!(self, Self::Utf8)
    }
}

pub const OTHER_WIDTH_NONE_LEVEL: &NEStr = NONE_LEVEL;
pub const OTHER_WIDTH_ERROR_LEVEL: &NEStr = ERROR_LEVEL;
pub const OTHER_WIDTH_WARN_LEVEL: &NEStr = WARN_LEVEL;
pub const OTHER_WIDTH_SILENT_LEVEL: &NEStr = SILENT_LEVEL;

impl_config_flag!(
    /// Choose how to guess the width for OTHER segments.
    pub GuessOtherWidth,
    /// Error when parsing [`GuessOtherWidth`] from [`String`]
    pub GuessOtherWidthError,
    None   => OTHER_WIDTH_NONE_LEVEL,
    Error  => OTHER_WIDTH_ERROR_LEVEL,
    Warn   => OTHER_WIDTH_WARN_LEVEL,
    Silent => OTHER_WIDTH_SILENT_LEVEL
);

pub const KW_ERROR_LEVEL: &NEStr = ERROR_LEVEL;
pub const KW_DEMOTE_WARN_LEVEL: &NEStr = DEMOTE_WARN_LEVEL;
pub const KW_DEMOTE_SILENT_LEVEL: &NEStr = DEMOTE_SILENT_LEVEL;
pub const KW_DROP_WARN_LEVEL: &NEStr = DROP_WARN_LEVEL;
pub const KW_DROP_SILENT_LEVEL: &NEStr = DROP_SILENT_LEVEL;

impl_config_flag!(
    /// Configuration to deal with optional standard keywords that cause errors.
    pub ProcessKeywordFailure,
    /// Error when parsing [`ProcessKeywordFailure`] from [`String`]
    pub ProcessKeywordFailureError,
    Error        => KW_ERROR_LEVEL,
    DemoteWarn   => KW_DEMOTE_WARN_LEVEL,
    DemoteSilent => KW_DEMOTE_SILENT_LEVEL,
    DropWarn     => KW_DROP_WARN_LEVEL,
    DropSilent   => KW_DROP_SILENT_LEVEL
);

pub const DELIM_ESCAPED_LEVEL: &NEStr = ne_str!("escaped");
pub const DELIM_UNESCAPED_LEVEL: &NEStr = ne_str!("unescaped");
pub const DELIM_GUESS_ESCAPED_LEVEL: &NEStr = ne_str!("guess_escaped");
pub const DELIM_GUESS_UNESCAPED_LEVEL: &NEStr = ne_str!("guess_unescaped");

impl_config_flag!(
    /// Choose how to escape delims in TEXT segment.
    pub DelimEscapeMode,
    /// Error when parsing [`DelimEscapeMode`] from [`String`]
    pub DelimEscapeModeError,
    /// Use escaped delimiters.
    Escaped        => DELIM_ESCAPED_LEVEL,
    /// Use unescaped delimiters.
    Unescaped      => DELIM_UNESCAPED_LEVEL,
    /// Guess      => falling back to escaped mode.
    GuessEscaped   => DELIM_GUESS_ESCAPED_LEVEL,
    /// Guess      => falling back to unescaped mode.
    GuessUnescaped => DELIM_GUESS_UNESCAPED_LEVEL
);

pub const TRIM_NONE_LEVEL: &NEStr = ne_str!("notrim");
pub const TRIM_ERROR_LEVEL: &NEStr = ne_str!("trim");
pub const TRIM_BLANK_WARN_LEVEL: &NEStr = ne_str!("trim_blank_warn");
pub const TRIM_BLANK_SILENT_LEVEL: &NEStr = ne_str!("trim_blank_silent");

impl_config_flag!(
    /// Choose how to trim values and deal with blanks that may result.
    pub TrimValueWhitespace,
    /// Error when parsing [`TrimValueWhitespace`] from [`String`]
    pub TrimValueWhitespaceError,
    /// Do not trim at all.
    Notrim          => TRIM_NONE_LEVEL,
    /// Trim whitespace and throw error if blank is created.
    Trim            => TRIM_ERROR_LEVEL,
    /// Trim whitespace and throw warning if blank is created.
    TrimBlankWarn   => TRIM_BLANK_WARN_LEVEL,
    /// Trim whitespace and do nothing if blank is created.
    TrimBlankSilent => TRIM_BLANK_SILENT_LEVEL
);

pub const FORCE_LINEAR_NONE_LEVEL: &NEStr = NONE_LEVEL;
pub const FORCE_LINEAR_TIME_LEVEL: &NEStr = ne_str!("time_only");
pub const FORCE_LINEAR_NON_INT_LEVEL: &NEStr = ne_str!("all_non_int");
pub const FORCE_LINEAR_ALL_LEVEL: &NEStr = ALL_LEVEL;

impl_config_flag!(
    /// Choose which $PnE to force as linear.
    pub ForceLinearScale,
    /// Error when parsing [`ForceLinearScale`] from [`String`]
    pub ForceLinearScaleError,
    /// Do not force.
    None      => FORCE_LINEAR_NONE_LEVEL,
    /// Only force the temporal measurement.
    TimeOnly  => FORCE_LINEAR_TIME_LEVEL,
    /// Force all non-integer measurements and temporal measurement.
    AllNonInt => FORCE_LINEAR_NON_INT_LEVEL,
    /// Force all measurements.
    All       => FORCE_LINEAR_ALL_LEVEL
);

impl ForceLinearScale {
    #[must_use]
    pub fn time_selected(self) -> bool {
        !matches!(self, Self::None)
    }
}

pub const TMP_OPT_DEMOTE_WARN_LEVEL: &NEStr = DEMOTE_WARN_LEVEL;
pub const TMP_OPT_DEMOTE_SILENT_LEVEL: &NEStr = DEMOTE_SILENT_LEVEL;
pub const TMP_OPT_DROP_WARN_LEVEL: &NEStr = DROP_WARN_LEVEL;
pub const TMP_OPT_DROP_SILENT_LEVEL: &NEStr = DROP_SILENT_LEVEL;

impl_config_flag!(
    /// Choose what to do with optical keys in time measurement when found.
    pub ProcessOpticalOnlyKeys,
    /// Error when parsing [`ForceLinearScale`] from [`String`]
    pub ProcessOpticalOnlyKeysError,
    /// Demote to nonstandard with warning
    DemoteWarn   => TMP_OPT_DEMOTE_WARN_LEVEL,
    /// Demote to nonstandard with no warning
    DemoteSilent => TMP_OPT_DEMOTE_SILENT_LEVEL,
    /// Drop with warning
    DropWarn     => TMP_OPT_DROP_WARN_LEVEL,
    /// Drop with no warning
    DropSilent   => TMP_OPT_DROP_SILENT_LEVEL
);

pub const SPILLOVER_NAMED_LEVEL: &NEStr = ne_str!("named");
pub const SPILLOVER_INDEXED_LEVEL: &NEStr = ne_str!("indexed");
pub const SPILLOVER_GUESS_LEVEL: &NEStr = ne_str!("guess");

impl_config_flag!(
    /// Choose how to parse measurements for $SPILLOVER key
    pub SpilloverMeasurementMode,
    /// Error when parsing [`ForceLinearScale`] from [`String`]
    pub SpilloverMeasurementModeError,
    /// Interpret measurements as names which match $PnN.
    Named   => SPILLOVER_NAMED_LEVEL,
    /// Interpret measurements as 1-indices (numbers) which point to measurements.
    Indexed => SPILLOVER_INDEXED_LEVEL,
    /// Guess how measurements should be interpreted.
    ///
    /// If they are all numbers and all do not point to $PnN, interpret as
    /// indices, otherwise names.
    Guess   => SPILLOVER_GUESS_LEVEL
);

pub const OVER_LIMIT_ACTION_ERROR_LEVEL: &NEStr = ERROR_LEVEL;
pub const OVER_LIMIT_ACTION_WARN_LEVEL: &NEStr = WARN_LEVEL;
pub const OVER_LIMIT_ACTION_SILENT_LEVEL: &NEStr = SILENT_LEVEL;
pub const OVER_LIMIT_ACTION_TRUNCATE_SILENT_LEVEL: &NEStr = ne_str!("trunc_silent");
pub const OVER_LIMIT_ACTION_TRUNCATE_WARN_LEVEL: &NEStr = ne_str!("trunc_warn");
pub const OVER_LIMIT_ACTION_NONE_LEVEL: &NEStr = NONE_LEVEL;

impl_str_enum!(
    #[derive(Display)]
    #[display("{}", self.as_str())]
    #[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
    pub OverLimitAction,
    /// Error when parsing [`OverLimitAction`] from [`String`]
    #[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
    #[cfg_attr(feature = "python", pyerr(crate::python::ConfigError))]
    pub OverLimitActionError,
    /// Warn for values over limit
    Warn => OVER_LIMIT_ACTION_WARN_LEVEL,
    /// Error for values over limit.
    Error => OVER_LIMIT_ACTION_ERROR_LEVEL,
    /// Do not throw warning or error and do not truncate, report only in diagnostics.
    Silent => OVER_LIMIT_ACTION_SILENT_LEVEL,
    /// Truncate and throw warning.
    TruncateSilent => OVER_LIMIT_ACTION_TRUNCATE_SILENT_LEVEL,
    /// Truncate and throw warning.
    TruncateWarn => OVER_LIMIT_ACTION_TRUNCATE_WARN_LEVEL,
    /// Do nothing. This will disable all scanning which will save CPU cycles.
    None => OVER_LIMIT_ACTION_NONE_LEVEL

);

/// Choose what to do with values that exceed $PnR.
#[derive(Display, FromStr, Clone, Copy)]
#[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
pub struct OverRangeAction(pub OverLimitAction);

impl Default for OverRangeAction {
    fn default() -> Self {
        Self(OverLimitAction::Warn)
    }
}

/// Choose what to do with integer values that exceed their bitmask set by $PnR.
#[derive(Display, FromStr, Clone, Copy)]
#[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
pub struct OverBitmaskAction(pub OverLimitAction);

impl Default for OverBitmaskAction {
    fn default() -> Self {
        Self(OverLimitAction::TruncateWarn)
    }
}

/// The action that will be used to deal with values that exceed a range.
#[derive(Debug, Clone, Copy)]
pub enum OverLimitMode {
    /// Do nothing.
    None,
    /// Truncate values that are over and emit warning or error
    Truncate,
    /// Scan for values that are over and emit warning or error.
    ScanOnly,
}

impl OverLimitAction {
    #[must_use]
    pub fn mode(&self) -> OverLimitMode {
        match self {
            Self::Error | Self::Warn | Self::Silent => OverLimitMode::ScanOnly,
            Self::TruncateSilent | Self::TruncateWarn => OverLimitMode::Truncate,
            Self::None => OverLimitMode::None,
        }
    }
}

pub const MISMATCH_ERROR_LEVEL: &NEStr = ERROR_LEVEL;
pub const MISMATCH_HEADER_WARN_LEVEL: &NEStr = ne_str!("header_warn");
pub const MISMATCH_HEADER_SILENT_LEVEL: &NEStr = ne_str!("header_silent");
pub const MISMATCH_TEXT_WARN_LEVEL: &NEStr = ne_str!("text_warn");
pub const MISMATCH_TEXT_SILENT_LEVEL: &NEStr = ne_str!("text_silent");

impl_config_flag!(
    /// Choose which offsets to use between TEXT and HEADER if they mismatch.
    ///
    /// Only applies to DATA and ANALYSIS offsets in 3.0+
    pub AllowHeaderTEXTOffsetMismatch,
    /// Error when parsing [`AllowHeaderTEXTOffsetMismatch`] from [`String`]
    pub AllowHeaderTEXTOffsetMismatchError,
    /// Throw error on mismatch.
    Error        => MISMATCH_ERROR_LEVEL,
    /// Choose HEADER on mismatch and throw warning.
    HeaderWarn   => MISMATCH_HEADER_WARN_LEVEL,
    /// Choose HEADER on mismatch and do nothing.
    HeaderSilent => MISMATCH_HEADER_SILENT_LEVEL,
    /// Choose TEXT on mismatch and throw warning.
    TextWarn     => MISMATCH_TEXT_WARN_LEVEL,
    /// Choose TEXT on mismatch and do nothing.
    TextSilent   => MISMATCH_TEXT_SILENT_LEVEL
);

impl AllowHeaderTEXTOffsetMismatch {
    /// Return bool matrix representing chosen segment and warning.
    ///
    /// First bool is true if we want HEADER, otherwise TEXT. Second boolean
    /// is true if we want a warning, false for no warning.
    ///
    /// None means throw an error and none of the above matters.
    #[must_use]
    pub fn is_warning(self) -> Option<(bool, bool)> {
        match self {
            Self::Error => None,
            Self::HeaderWarn => Some((true, true)),
            Self::HeaderSilent => Some((true, false)),
            Self::TextWarn => Some((false, true)),
            Self::TextSilent => Some((false, false)),
        }
    }
}

pub const COMPUTE_CRC_NEVER_LEVEL: &NEStr = ne_str!("never");
pub const COMPUTE_CRC_TEST_LEVEL: &NEStr = ne_str!("test");
pub const COMPUTE_CRC_ALWAYS_LEVEL: &NEStr = ne_str!("always");

impl_config_flag!(
    /// When to compute the CRC for a dataset
    pub ComputeCRC,
    /// Error when parsing [`ComputeCRC`] from [`String`]
    pub ComputeCRCError,
    /// Never compute CRC.
    Never  => COMPUTE_CRC_NEVER_LEVEL,
    /// Always compute CRC.
    Always => COMPUTE_CRC_ALWAYS_LEVEL,
    /// Compute CRC only when the CRC word at the end of the dataset can be read.
    Test   => COMPUTE_CRC_TEST_LEVEL
);

const GAIN_LEVEL: &NEStr = ne_str!("G");
const FILTER_LEVEL: &NEStr = ne_str!("F");
const WAVELENGTH_LEVEL: &NEStr = ne_str!("L");
const POWER_LEVEL: &NEStr = ne_str!("O");
const DET_TYPE_LEVEL: &NEStr = ne_str!("T");
const DET_VOLTAGE_LEVEL: &NEStr = ne_str!("V");
const PCNT_EMIT_LEVEL: &NEStr = ne_str!("P");
const CALIBRATION_LEVEL: &NEStr = ne_str!("CALIBRATION");
const DET_NAME_LEVEL: &NEStr = ne_str!("DET");
const TAG_LEVEL: &NEStr = ne_str!("TAG");
const FEATURE_LEVEL: &NEStr = ne_str!("FEATURE");
const ANALYTE_LEVEL: &NEStr = ne_str!("ANALYTE");

impl_str_enum!(
    /// Disallowed and ignorable optical keywords for temporal measurements.
    #[derive(PartialEq, Eq, Debug, Hash, Display)]
    #[display("{}", self.as_str())]
    #[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
    pub OpticalOnlyKey,
    /// Error when creating [`OpticalOnlyKey`] from [`String`]
    #[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
    #[cfg_attr(feature = "python", pyerr(crate::python::ConfigError))]
    pub OpticalOnlyKeyError,
    /// Ignore $PnG
    Gain            => GAIN_LEVEL,
    /// Ignore $PnF
    Filter          => FILTER_LEVEL,
    /// Ignore $PnL
    Wavelength      => WAVELENGTH_LEVEL,
    /// Ignore $PnO
    Power           => POWER_LEVEL,
    /// Ignore $PnT
    DetectorType    => DET_TYPE_LEVEL,
    /// Ignore $PnV
    DetectorVoltage => DET_VOLTAGE_LEVEL,
    /// Ignore $PnP
    PercentEmitted  => PCNT_EMIT_LEVEL,
    /// Ignore $PnCALIBRATION
    Calibration     => CALIBRATION_LEVEL,
    /// Ignore $PnDET
    DetectorName    => DET_NAME_LEVEL,
    /// Ignore $PnTAG
    Tag             => TAG_LEVEL,
    /// Ignore $PnFEATURE
    Feature         => FEATURE_LEVEL,
    /// Ignore $PnANALYTE
    Analyte         => ANALYTE_LEVEL
);

impl OpticalOnlyKey {
    pub const TARGETS_2_0: [Self; 6] = [
        Self::DetectorType,
        Self::DetectorVoltage,
        Self::Filter,
        Self::PercentEmitted,
        Self::Power,
        Self::Wavelength,
    ];

    pub const TARGETS_3_0: [Self; 7] = [
        Self::Gain,
        Self::DetectorType,
        Self::DetectorVoltage,
        Self::Filter,
        Self::PercentEmitted,
        Self::Power,
        Self::Wavelength,
    ];

    pub const TARGETS_3_1: [Self; 8] = [
        Self::Gain,
        Self::Calibration,
        Self::DetectorType,
        Self::DetectorVoltage,
        Self::Filter,
        Self::PercentEmitted,
        Self::Power,
        Self::Wavelength,
    ];

    pub const TARGETS_3_2: [Self; 12] = [
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
}

pub const STD_KW_REQ_LEVEL: &NEStr = ne_str!("req_only");
pub const STD_KW_OPT_LEVEL: &NEStr = ne_str!("opt_opt");
pub const STD_KW_REQ_AND_OPT_LEVEL: &NEStr = ne_str!("both");

impl_str_enum!(
    /// Choose what kind of keywords to return (required vs optional).
    #[cfg_attr(feature = "python", derive(FromPyString))]
    pub IncludeReqOrOpt,
    /// Error when parsing [`IncludeReqOrOpt`] from [`String`]
    #[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
    #[cfg_attr(feature = "python", pyerr(crate::python::ConfigError))]
    pub IncludeReqOrOptError,
    /// Return required.
    Req_ => STD_KW_REQ_LEVEL,
    /// Return optional.
    Opt_ => STD_KW_OPT_LEVEL,
    /// Return both.
    Both => STD_KW_REQ_AND_OPT_LEVEL
);

pub const STD_KW_ROOT_LEVEL: &NEStr = ne_str!("req_only");
pub const STD_KW_MEAS_LEVEL: &NEStr = ne_str!("opt_opt");
pub const STD_KW_ROOT_AND_MEAS_LEVEL: &NEStr = ne_str!("both");

impl_str_enum!(
    /// Choose what kind of keywords to return (required vs optional).
    #[cfg_attr(feature = "python", derive(FromPyString))]
    pub IncludeRootOrMeas,
    /// Error when parsing [`IncludeRootOrMeas`] from [`String`]
    #[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
    #[cfg_attr(feature = "python", pyerr(crate::python::ConfigError))]
    pub IncludeRootOrMeasError,
    /// Return root.
    Root => STD_KW_ROOT_LEVEL,
    /// Return meas.
    Meas => STD_KW_MEAS_LEVEL,
    /// Return both.
    Both => STD_KW_ROOT_AND_MEAS_LEVEL
);

pub const READ_STRATEGY_STRICT_LEVEL: &NEStr = ne_str!("strict");
pub const READ_STRATEGY_SCALPEL_LEVEL: &NEStr = ne_str!("scalpel");
pub const READ_STRATEGY_SLEDGEHAMMER_LEVEL: &NEStr = ne_str!("sledgehammer");

impl_str_enum!(
    /// Overall strategy to read FCS files.
    ///
    /// This is a "metaflag" which will activate individual flags in each
    /// configuration struct. The exact flags to be activated will depend on the
    /// struct. In all cases, this will activate the flags which emit warnings
    /// where applicable. If one does not desire warnings, they can be
    /// suppressed elsewhere in the config.
    ///
    /// In general, the different levels for this are a tradeoff between the ability
    /// to read events from DATA vs preserving metadata.
    #[derive(Default)]
    pub ReadStrategy,
    /// Error when parsing [`ReadStrategy`] from [`String`]
    pub ReadStrategyError,
    /// Follow the standard fully (configuration is totally default).
    ///
    /// Many files will fail this, but it is useful for validation.
    #[default]
    Strict       => READ_STRATEGY_STRICT_LEVEL,
    /// Use "safe" non-compliant parsing that is unlikely to result in data loss.
    ///
    /// This is likely a good option for many files.
    Scalpel      => READ_STRATEGY_SCALPEL_LEVEL,
    /// Use "unsafe" non-compliant parsing.
    ///
    /// This is the best option when all one cares about is reading DATA.
    /// Non-compliant metadata in TEXT will be skipped.
    Sledgehammer => READ_STRATEGY_SLEDGEHAMMER_LEVEL
);

// Declare other useful constants used for configuration

/// A string which is used to denote "no time measurement pattern".
///
/// This is only used in interfaces that require a string for
/// [`ReadStdKeywordsConfig_::time_meas_pattern`] and an `Option` cannot be
/// used.
pub const TIME_MEAS_NAME_PATTERN_NONE: &str = "NoTime";

/// The default value for [`ReadStdKeywordsConfig_::time_meas_pattern`].
pub const TIME_MEAS_NAME_PATTERN_DEFAULT: &str = "^(TIME|Time)$";

/// Used as a separator to disambiguate $PnN that were duplicated
pub const DEDUP_PNN_SEP: char = '~';

/// The default format for $DATE.
///
/// The "%b" format is case-insensitive so this should work for "Jan", "JAN",
/// "jan", "jaN", etc.
pub const DEFAULT_DATE_FORMAT: &str = "%d-%b-%Y";

/// The default format for $LAST_MODIFIED.
pub const DEFAULT_LAST_MODIFIED_FORMAT: &str = "%d-%b-%Y %H:%M:%S";

/// The default format for $BTIM and $ETIM in FCS2.0
pub const BASE_TIME_FORMAT: &str = "%H:%M:%S";

/// The default format for $BTIM and $ETIM in FCS2.0
pub const DEFAULT_TIME_FORMAT_2_0: &str = BASE_TIME_FORMAT;

/// The default format for $BTIM and $ETIM in FCS3.0
pub const DEFAULT_TIME_FORMAT_3_0: &str = formatcp!("{BASE_TIME_FORMAT}:{BASE60_SECOND_SPEC}");

/// The default format for $BTIM and $ETIM in FCS3.1 (and up)
pub const DEFAULT_TIME_FORMAT_3_1: &str = formatcp!("{BASE_TIME_FORMAT}.{BASE100_SECOND_SPEC}");

/// A custom format specifier for base-60 seconds.
pub const BASE60_SECOND_SPEC: &str = "%!";

/// A custom format specifier for centiseconds.
pub const BASE100_SECOND_SPEC: &str = "%@";

// Implement presets for configuration structs
//
// These configuration structs are modestly intimidating, and only FCS
// super-nerds have time to look at all these flags and admire their beauty.
// Therefore, there are meta-flags which set lots of other flags to sane
// defaults. This is what most people should use.

/// A config struct which has presets (ie "strategies").
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
            ReadStrategy::Scalpel => self.with_scalpel(),
            ReadStrategy::Sledgehammer => {
                self.with_scalpel();
                self.with_sledgehammer();
            }
        }
    }

    fn with_scalpel(&mut self);

    fn with_sledgehammer(&mut self) {}
}

impl HasStrategy for ReadHeaderInnerConfig {
    fn with_scalpel(&mut self) {
        self.guess_other_width = GuessOtherWidth::Warn;
        self.squish_offsets = true.into();
    }

    fn with_sledgehammer(&mut self) {
        self.max_other = Some(0);
    }
}

impl HasStrategy for ReadOffsetConfig {
    fn with_scalpel(&mut self) {
        self.allow_pseudoempty = true.into();
        // Allow automatic correction of off-by-one offset errors. This won't
        // always work but will likely take care of 80% of cases.
        self.dataset_overflow_limit = 1.into();
        self.overlap_correction_limit = 1.into();
    }
}

impl HasStrategy for ReadHeaderAndTEXTConfig {
    fn with_scalpel(&mut self) {
        let strat = VersionOverride::AutoDetect {
            strategy: SelectVersionStrategy::Loose,
            prioritize_current: true,
        };

        self.version_override = Some(strat);
        self.delim_escape_mode = DelimEscapeMode::GuessEscaped;
        self.allow_duplicated_supp_text = TriFlag::True.into();
        self.allow_non_ascii_delim = TriFlag::True.into();
        self.allow_even_delims = TriFlag::True.into();
        self.allow_nonunique = TriFlag::True.into();
        self.allow_odd_tokens = TriFlag::True.into();
        self.allow_empty_keys = TriFlag::True.into();
        self.allow_delim_at_boundary = TriFlag::True.into();
        self.use_encoding = UseEncoding::Guess;
        self.allow_non_utf8_values = TriFlag::True.into();
        self.allow_non_ascii_keys = TriFlag::True.into();
        self.allow_missing_supp_text = TriFlag::True.into();
        self.allow_supp_text_own_delim = TriFlag::True.into();
        self.allow_missing_nextdata = TriFlag::True.into();
        self.trim_value_whitespace = TrimValueWhitespace::TrimBlankWarn;
    }

    fn with_sledgehammer(&mut self) {
        self.ignore_supp_text = true.into();
    }
}

impl<TMP, DP, TP, DTP, LMP> HasStrategy for ReadStdKeywordsConfig_<TMP, DP, TP, DTP, LMP> {
    fn with_scalpel(&mut self) {
        self.dedup_measurement_names = true.into();
        self.add_missing_timestep = Some(PositiveFloat::one());
        self.force_linear_scale = ForceLinearScale::AllNonInt;
        self.trim_intra_value_whitespace = true.into();
        self.spillover_measurement_mode = SpilloverMeasurementMode::Guess;
        self.allow_other_feature = true.into();
        self.fix_log_scale_offsets = true.into();
        // This flag all optical keys as ignorable in the time measurement.
        // The next flag tells what to do with them (in this case, demote)
        self.ignore_optical_only_keys = OpticalOnlyKeys::all();
        self.process_optical_only_keys = ProcessOpticalOnlyKeys::DemoteWarn;
        self.process_pseudostandard = ProcessKeywordFailure::DemoteWarn.into();
        self.process_hyper_par = ProcessKeywordFailure::DemoteWarn.into();
        self.process_other_version = ProcessKeywordFailure::DemoteWarn.into();
        self.process_extra_timestep = ProcessKeywordFailure::DemoteWarn.into();
    }

    fn with_sledgehammer(&mut self) {
        self.process_optical_only_keys = ProcessOpticalOnlyKeys::DropWarn;
        self.process_pseudostandard = ProcessKeywordFailure::DropWarn.into();
        self.process_hyper_par = ProcessKeywordFailure::DropWarn.into();
        self.process_other_version = ProcessKeywordFailure::DropWarn.into();
        self.process_extra_timestep = ProcessKeywordFailure::DropWarn.into();
        self.allow_missing_time = TriFlag::True.into();
    }
}

impl<ISK, RSK, PTS, DFS, RSKV, ASK, SSKV> HasStrategy
    for ReadDataKeywordsConfig_<ISK, RSK, PTS, DFS, RSKV, ASK, SSKV>
{
    fn with_scalpel(&mut self) {
        // Enable SPILL/SPILLOVER/$SPILL->$SPILLOVER mapping, which should be
        // fine for most/all files without doing any vendor-specific pattern
        // matching.
        //
        // This also what flowcore and flowIO do, see
        // https://github.com/RGLab/flowCore/blob/4935c7bf318697b3128ee50dae81018a6b246ab8/R/eval-methods.R#L649
        // and
        // https://github.com/whitews/FlowIO/blob/83d28a22d42235c10d17afb017250ee208afed95/src/flowio/flowdata.py#L761
        // self.promote_to_standard.push_promote_spillover();
        // self.rename_standard_keys.push_rename_spill_to_spillover();

        self.allow_header_text_offset_mismatch = AllowHeaderTEXTOffsetMismatch::HeaderWarn;
        self.allow_missing_required_offsets = TriFlag::True.into();
        self.process_optional_failure = ProcessKeywordFailure::DemoteWarn.into();
        self.int_width_override = IntWidthOverride::NextByte;
        self.byteord_override = ByteordOverride::Endian;
    }

    fn with_sledgehammer(&mut self) {
        self.process_optional_failure = ProcessKeywordFailure::DropWarn.into();
        self.ignore_text_analysis_offsets = true.into();
    }
}

impl HasStrategy for ReadDatasetConfig {
    fn with_scalpel(&mut self) {
        self.data_remainder_limit = 1.into();
        self.allow_uneven_event_width = TriFlag::True.into();
        self.allow_tot_mismatch = TriFlag::True.into();
        self.allow_missing_crc = TriFlag::True.into();
    }
}

// internal constants, many are shared between enums to keep the API simpler

const NONE_LEVEL: &NEStr = ne_str!("none");
const FALSE_LEVEL: &NEStr = ne_str!("false");
const TRUE_LEVEL: &NEStr = ne_str!("true");
const SILENT_LEVEL: &NEStr = ne_str!("silent");
const ERROR_LEVEL: &NEStr = ne_str!("error");
const WARN_LEVEL: &NEStr = ne_str!("warn");
const ALL_LEVEL: &NEStr = ne_str!("all");
const DEMOTE_WARN_LEVEL: &NEStr = ne_str!("demote_warn");
const DEMOTE_SILENT_LEVEL: &NEStr = ne_str!("demote_silent");
const DROP_WARN_LEVEL: &NEStr = ne_str!("drop_warn");
const DROP_SILENT_LEVEL: &NEStr = ne_str!("drop_silent");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes_from_u64() {
        assert_eq!(NumericByteWidth::B1, NumericByteWidth::from_u64(0));
        assert_eq!(NumericByteWidth::B1, NumericByteWidth::from_u64(0x00FF));
        assert_eq!(NumericByteWidth::B2, NumericByteWidth::from_u64(0x0100));
        assert_eq!(NumericByteWidth::B2, NumericByteWidth::from_u64(0xFFFF));
        assert_eq!(
            NumericByteWidth::B3,
            NumericByteWidth::from_u64(0x0001_0000)
        );
        assert_eq!(
            NumericByteWidth::B8,
            NumericByteWidth::from_u64(0xFFFF_FFFF_FFFF_FFFF)
        );
    }
}

#[cfg(feature = "python")]
pub use python::{
    BYTEORD_OVERRIDE_ENDIAN_LEVEL, BYTEORD_OVERRIDE_NONE_LEVEL, FIX_INT_WIDTH_NEVER_LEVEL,
    FIX_INT_WIDTH_NEXT_BYTE_LEVEL,
};

#[cfg(feature = "python")]
mod python {
    use super::{
        ByteordOverride, IntWidthOverride, KeyPatterns, NONE_LEVEL, OpticalOnlyKeys, SubPatterns,
        TimeMeasNamePattern,
    };

    use crate::{
        byteord::ConfigByteOrd,
        case_ins_regex::{LiteralOrPattern, LiteralOrPatternError},
        keystring::KeyStringOrPattern,
        ne_str,
        nonempty_string::NEStr,
        python::ConfigError,
        sub_pattern::SubPattern,
    };

    use hashbrown::HashMap;
    use pyo3::{
        IntoPyObjectExt as _,
        prelude::*,
        types::{PyDict, PyString},
    };
    use regex::Regex;

    use std::convert::Infallible;
    use std::fmt;
    use std::str::FromStr;

    impl<'py> FromPyObject<'_, 'py> for OpticalOnlyKeys {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            let xs: Vec<_> = obj.extract()?;
            Ok(Self(xs.into_iter().collect()))
        }
    }

    impl<'py> IntoPyObject<'py> for OpticalOnlyKeys {
        type Target = PyAny;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            self.0.into_iter().collect::<Vec<_>>().into_pyobject(py)
        }
    }

    // Don't use FromStr for this because it is more natural in Python to use
    // None for "not set"; FromStr maps None to "NoTime"
    impl<'py> FromPyObject<'_, 'py> for TimeMeasNamePattern {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            if obj.is_none() {
                Ok(Self(None))
            } else {
                let s: String = obj.extract()?;
                let r = s
                    .parse::<Regex>()
                    .map_err(|e| ConfigError::new_err(e.to_string()))?;
                Ok(Self(Some(r)))
            }
        }
    }

    impl<'py> IntoPyObject<'py> for TimeMeasNamePattern {
        type Target = PyAny;
        type Output = Bound<'py, Self::Target>;
        type Error = Infallible;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            self.0.map(|r| r.as_str().to_owned()).into_pyobject(py)
        }
    }

    impl<'py> FromPyObject<'_, 'py> for KeyPatterns {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            let xs: Vec<KeyStringOrPattern> = obj.extract()?;
            Ok(Self(xs.into_iter().map(|x| (x, ())).collect()))
        }
    }

    impl<'py> IntoPyObject<'py> for KeyPatterns {
        type Target = PyAny;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            self.0.keys().cloned().collect::<Vec<_>>().into_pyobject(py)
        }
    }

    type _SubPattern = HashMap<String, SubPattern>;

    impl<'py> FromPyObject<'_, 'py> for SubPatterns {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            Ok(Self(obj.extract::<HashMap<_, _>>()?))
        }
    }

    impl<'py> IntoPyObject<'py> for SubPatterns {
        type Target = PyDict;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            self.0.into_pyobject(py)
        }
    }

    impl<'py> FromPyObject<'_, 'py> for IntWidthOverride {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            if let Ok(n) = obj.extract::<u8>()
                && let Ok(bw) = n.try_into()
            {
                return Ok(Self::Explicit(bw));
            } else if let Ok(s) = obj.extract::<&NEStr>() {
                if s == FIX_INT_WIDTH_NEVER_LEVEL {
                    return Ok(Self::Never);
                } else if s == FIX_INT_WIDTH_NEXT_BYTE_LEVEL {
                    return Ok(Self::NextByte);
                }
            }
            Err(ConfigError::new_err(format!(
                "must be a an integer 1-8 or one of '{FIX_INT_WIDTH_NEVER_LEVEL}' \
                 or '{FIX_INT_WIDTH_NEXT_BYTE_LEVEL}'",
            )))
        }
    }

    impl<'py> IntoPyObject<'py> for IntWidthOverride {
        type Target = PyAny;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            match self {
                Self::Explicit(b) => u8::from(b).into_bound_py_any(py),
                Self::NextByte => FIX_INT_WIDTH_NEXT_BYTE_LEVEL.into_bound_py_any(py),
                Self::Never => FIX_INT_WIDTH_NEVER_LEVEL.into_bound_py_any(py),
            }
        }
    }

    impl<'py> FromPyObject<'_, 'py> for ByteordOverride {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            if let Ok(b) = obj.extract::<ConfigByteOrd>() {
                return Ok(Self::Explicit(b));
            } else if let Ok(s) = obj.extract::<&NEStr>() {
                if s == BYTEORD_OVERRIDE_NONE_LEVEL {
                    return Ok(Self::None);
                } else if s == BYTEORD_OVERRIDE_ENDIAN_LEVEL {
                    return Ok(Self::Endian);
                }
            }
            Err(ConfigError::new_err(format!(
                "must be a valid byte order or one of \
                 '{BYTEORD_OVERRIDE_ENDIAN_LEVEL}' or \
                 '{BYTEORD_OVERRIDE_NONE_LEVEL}'",
            )))
        }
    }

    impl<'py> IntoPyObject<'py> for ByteordOverride {
        type Target = PyAny;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            match self {
                Self::Explicit(b) => b.into_pyobject(py),
                Self::None => BYTEORD_OVERRIDE_NONE_LEVEL.into_bound_py_any(py),
                Self::Endian => BYTEORD_OVERRIDE_ENDIAN_LEVEL.into_bound_py_any(py),
            }
        }
    }

    // TODO make FromStr and ToStr derive work for these, which will
    // in turn require than the bounds attributes get cleaned up

    impl<'py, L> FromPyObject<'_, 'py> for LiteralOrPattern<L>
    where
        PyErr: From<LiteralOrPatternError<L::Err>>,
        L: FromStr,
        Self: FromStr<Err = LiteralOrPatternError<L::Err>>,
    {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            Ok(obj.extract::<String>()?.parse()?)
        }
    }

    impl<'py, L: fmt::Display> IntoPyObject<'py> for LiteralOrPattern<L> {
        type Target = PyString;
        type Output = Bound<'py, Self::Target>;
        type Error = Infallible;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            self.to_string().into_pyobject(py)
        }
    }

    pub const BYTEORD_OVERRIDE_NONE_LEVEL: &NEStr = NONE_LEVEL;
    pub const BYTEORD_OVERRIDE_ENDIAN_LEVEL: &NEStr = ne_str!("endian");

    pub const FIX_INT_WIDTH_NEVER_LEVEL: &NEStr = ne_str!("never");
    pub const FIX_INT_WIDTH_NEXT_BYTE_LEVEL: &NEStr = ne_str!("next_byte");
}

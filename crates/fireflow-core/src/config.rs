use crate::header::Version;
use crate::validated::datepattern::DatePattern;
use crate::validated::nonstandard::NonStdMeasPattern;
use crate::validated::pattern::TimePattern;
use crate::validated::shortname::*;
use crate::validated::textdelim::TEXTDelim;

#[derive(Default, Clone)]
pub struct HeaderConfig {
    /// Override the version
    pub version_override: Option<Version>,

    /// Corrections for primary TEXT segment
    pub text: OffsetCorrection,

    /// Corrections for DATA segment
    pub data: OffsetCorrection,

    /// Corrections for ANALYSIS segment
    pub analysis: OffsetCorrection,
}

#[derive(Default, Clone, Copy)]
pub struct OffsetCorrection {
    pub begin: i32,
    pub end: i32,
}

/// Instructions for reading the TEXT segment as raw key/value pairs.
// TODO add correction for $NEXTDATA
#[derive(Default, Clone)]
pub struct RawTextReadConfig {
    /// Config for reading HEADER
    pub header: HeaderConfig,

    /// Corrections for supplemental TEXT segment
    pub stext: OffsetCorrection,

    /// Will treat every delimiter as a literal delimiter rather than "escaping"
    /// double delimiters
    pub allow_double_delim: bool,

    /// If true, only ASCII characters 1-126 will be allowed for the delimiter
    pub force_ascii_delim: bool,

    /// If true, throw an error if the last byte of the TEXT segment is not
    /// a delimiter.
    pub enforce_final_delim: bool,

    /// If true, throw an error if any key in the TEXT segment is not unique
    pub enforce_unique: bool,

    /// If true, throw an error if the number or words in the TEXT segment is
    /// not an even number (ie there is a key with no value)
    pub enforce_even: bool,

    /// If true, throw an error if we encounter a key with a blank value.
    ///
    /// Only relevant if [`allow_double_delim`] is also true.
    pub enforce_nonempty: bool,

    /// If true, throw an error if we encounter a delimiter at a word boundary.
    ///
    /// Only relevant if [`allow_double_delim`] is also true. While delimiters
    /// may be escaped and included in keys or values, it is impossible to tell
    /// within which word they are belong when the are next to a real delimiter,
    /// which is why they are "not allowed".
    ///
    /// When set to false, these delimiters are simply not included in word.
    pub enforce_delim_nobound: bool,

    /// If true, throw an error if the parser encounters a bad UTF-8 byte when
    /// creating the key/value list. If false, merely drop the bad pair.
    pub enforce_utf8: bool,

    /// If true, throw error when encountering key with non-ASCII characters
    pub enforce_keyword_ascii: bool,

    /// If true, throw error if supplemental TEXT offsets are missing.
    ///
    /// Does not affect 3.2 since these are optional there.
    pub enforce_stext: bool,

    /// If true, error if delim differs between primary and supplemental TEXT.
    pub enforce_stext_delim: bool,

    /// If true, throw error if $NEXTDATA is missing.
    ///
    /// For now only reading the first dataset is supported, so this keyword
    /// does nothing. However, it is supposed to be present according to the
    /// standard.
    pub enforce_nextdata: bool,

    /// If true, throw error if offsets in HEADER and TEXT differ.
    ///
    /// Only applies to DATA and ANALYSIS offsets
    pub enforce_offset_match: bool,

    /// If true, throw error if required TEXT offsets are missing.
    ///
    /// Only applies to DATA and ANALYSIS offsets in versions 3.0 and 3.1. If
    /// missing these will be taken from HEADER.
    pub enforce_required_offsets: bool,

    /// If true, replace leading spaces in offset keywords with 0.
    ///
    ///These often need to be padded to make the DATA segment appear at a
    /// predictable offset. Many machines/programs will pad with spaces despite
    /// the spec requiring that all numeric fields be entirely numeric
    /// character.
    pub repair_offset_spaces: bool,

    /// If supplied, will be used as an alternative pattern when parsing $DATE.
    ///
    /// It should have specifiers for year, month, and day as outlined in
    /// https://docs.rs/chrono/latest/chrono/format/strftime/index.html. If not
    /// supplied, $DATE will be parsed according to the standard pattern which
    /// is '%d-%b-%Y'.
    pub date_pattern: Option<DatePattern>,
    // TODO add keyword and value overrides, something like a list of patterns
    // that can be used to alter each keyword
    // TODO allow lambda function to be supplied which will alter the kv list
}

/// Instructions for validating time-related properties.
#[derive(Default, Clone)]
pub struct TimeConfig {
    /// If given, a pattern to find/match the $PnN of the time measurement.
    ///
    /// If matched, the time measurement must conform to the requirements of the
    /// target FCS version, such as having $TIMESTEP present and having a PnE
    /// set to '0,0'.
    pub pattern: Option<TimePattern>,

    /// If true, will ensure that time measurement is present
    pub ensure: bool,

    /// If true, will ensure TIMESTEP is present if time measurement is also
    /// present.
    pub ensure_timestep: bool,

    /// If true, will ensure PnE is 0,0 for time measurement.
    pub ensure_linear: bool,

    /// If true, will ensure PnG is absent for time measurement.
    pub ensure_nogain: bool,
}

/// Instructions for reading the TEXT segment in a standardized structure.
#[derive(Default, Clone)]
pub struct StdTextReadConfig {
    /// Instructions to read HEADER and TEXT.
    pub raw: RawTextReadConfig,

    /// Time-related options.
    pub time: TimeConfig,

    /// Prefix to use when filling in missing $PnN values.
    ///
    /// This is only applicable to 2.0 and 3.0 since $PnN became required in
    /// 3.1. For cases where $PnN is missing, the name used for the measurement
    /// will be this prefix appended with the measurement index.
    pub shortname_prefix: ShortnamePrefix,

    /// If true, throw an error if TEXT includes any keywords that start with
    /// "$" which are not standard.
    pub disallow_deviant: bool,

    /// If true, throw an error if TEXT includes any keywords that do not
    /// start with "$".
    pub disallow_nonstandard: bool,

    /// If true, throw an error if TEXT includes any deprecated features
    pub disallow_deprecated: bool,

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
    // TODO add repair stuff
}

/// Instructions for reading the DATA segment.
#[derive(Default, Clone)]
pub struct DataReadConfig {
    /// Instructions to read and standardize TEXT.
    pub standard: StdTextReadConfig,

    /// Corrections for DATA offsets in TEXT segment
    pub data: OffsetCorrection,

    /// Corrections for ANALYSIS offsets in TEXT segment
    pub analysis: OffsetCorrection,

    /// If true, throw error when total event width does not evenly divide
    /// the DATA segment. Meaningless for delimited ASCII data.
    pub enforce_data_width_divisibility: bool,

    /// If true, throw error if the total number of events as computed by
    /// dividing DATA segment length event width doesn't match $TOT. Does
    /// nothing if $TOT not given, which may be the case in version 2.0.
    pub enforce_matching_tot: bool,
}

/// Configuration options that do not fit anywhere else
#[derive(Default, Clone)]
pub struct MiscReadConfig {
    /// If true, all warnings are considered to be fatal errors.
    pub warnings_are_errors: bool,

    /// If true, don't truncate the bitmask to whatever type it needs to be.
    pub bitmask_notruncate: bool,
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

    /// If true, check for conversion losses before writing data.
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
    /// emitted if this is true.
    pub check_conversion: bool,

    /// If true, disallow lossy data conversions
    ///
    /// Only has an effect if `check_conversion` is true. If this is also true,
    /// any lossy conversion will halt immediately and return an error to the
    /// user.
    pub disallow_lossy_conversions: bool,
}

pub trait Strict: Default {
    fn set_strict(self, strict: bool) -> Self {
        if strict {
            Self::set_strict_inner(self)
        } else {
            self
        }
    }

    fn set_strict_inner(self) -> Self;
}

impl Strict for RawTextReadConfig {
    fn set_strict_inner(self) -> Self {
        Self {
            force_ascii_delim: true,
            enforce_final_delim: true,
            enforce_unique: true,
            enforce_even: true,
            enforce_delim_nobound: true,
            enforce_utf8: true,
            enforce_keyword_ascii: true,
            enforce_stext: true,
            enforce_stext_delim: true,
            enforce_nextdata: true,
            enforce_offset_match: true,
            enforce_required_offsets: true,
            ..self
        }
    }
}

impl Strict for StdTextReadConfig {
    fn set_strict_inner(self) -> Self {
        Self {
            raw: RawTextReadConfig::set_strict_inner(self.raw),
            time: TimeConfig::set_strict_inner(self.time),
            disallow_deviant: true,
            disallow_nonstandard: true,
            ..self
        }
    }
}

impl Strict for TimeConfig {
    fn set_strict_inner(self) -> Self {
        Self {
            pattern: Some(TimePattern::default()),
            ensure: true,
            ensure_timestep: true,
            ensure_linear: true,
            ensure_nogain: true,
        }
    }
}

impl Strict for DataReadConfig {
    fn set_strict_inner(self) -> Self {
        Self {
            standard: StdTextReadConfig::set_strict_inner(self.standard),
            enforce_data_width_divisibility: true,
            enforce_matching_tot: true,
            ..self
        }
    }
}

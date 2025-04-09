use crate::datepattern::DatePattern;
use crate::ns_meas_pattern::NonStdMeasPattern;
use crate::shortname::Shortname;
use crate::textdelim::TEXTDelim;

/// Instructions for reading an FCS file.
#[derive(Default, Clone)]
pub struct Config {
    pub corrections: OffsetCorrections,
    pub raw: RawTextReadConfig,
    pub standard: StdTextReadConfig,
    pub data: DataReadConfig,
    pub misc: MiscReadConfig,
    pub write: WriteConfig,
}

/// Corrections for file offsets
///
/// Use these to fix errors caused by offsets pointing to the wrong location.
///
/// Each of these will be added to the offset values as parsed from the file.
/// Obviously the result must be greater than zero, and the resulting offset
/// pairs must not be flipped (begin > end).
///
/// These do nothing if the segment does not exist.
// TODO this will need to be repeated for each dataset once we include this
#[derive(Default, Clone)]
pub struct OffsetCorrections {
    /// Corrections for primary TEXT segment
    pub start_prim_text: i32,
    pub end_prim_text: i32,

    /// Corrections for supplemental TEXT segment
    pub start_supp_text: i32,
    pub end_supp_text: i32,

    /// Corrections for DATA segment
    pub start_data: i32,
    pub end_data: i32,

    /// Corrections for ANALYSIS segment
    pub start_analysis: i32,
    pub end_analysis: i32,

    /// Correction for $NEXTDATA
    pub nextdata: i32,
}

/// Instructions for reading the TEXT segment as raw key/value pairs.
#[derive(Default, Clone)]
pub struct RawTextReadConfig {
    /// Will treat every delimiter as a literal delimiter rather than "escaping"
    /// double delimiters
    pub no_delim_escape: bool,

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
    /// Only relevant if [`no_delim_escape`] is also true.
    pub enforce_nonempty: bool,

    /// If true, throw an error if the parser encounters a bad UTF-8 byte when
    /// creating the key/value list. If false, merely drop the bad pair.
    pub error_on_invalid_utf8: bool,

    /// If true, throw error when encoutering keyword with non-ASCII characters
    pub enfore_keyword_ascii: bool,

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

/// Instructions for reading the TEXT segment in a standardized structure.
#[derive(Default, Clone)]
pub struct StdTextReadConfig {
    /// If given, will be the $PnN used to identify the time channel.
    ///
    /// This is meaningless for FCS 2.0 and will be ignored in that case.
    ///
    /// Like all $PnN values, this must not contain commas.
    ///
    /// Will be used for the [`ensure_time*`] options below. If not given, skip
    /// time channel checking entirely.
    pub time_shortname: Option<Shortname>,

    /// If true, will ensure that time channel is present
    pub ensure_time: bool,

    /// If true, will ensure TIMESTEP is present if time channel is also
    /// present.
    pub ensure_time_timestep: bool,

    /// If true, will ensure PnE is 0,0 for time channel.
    pub ensure_time_linear: bool,

    /// If true, will ensure PnG is absent for time channel.
    pub ensure_time_nogain: bool,

    /// If true, throw an error if TEXT includes any keywords that start with
    /// "$" which are not standard.
    pub disallow_deviant: bool,

    /// If true, throw an error if TEXT includes any deprecated features
    pub disallow_deprecated: bool,

    /// If true, throw an error if TEXT includes any keywords that do not
    /// start with "$".
    pub disallow_nonstandard: bool,

    /// If supplied, this pattern will be used to group "nonstandard" keywords
    /// with matching measurements.
    ///
    /// Usually this will be something like '^P%n.+' where '%n' will be
    /// substituted with the measurement index before using it as a regular
    /// expression to match keywords. It should not start with a "$" and must
    /// contain a literal '%n'.
    ///
    /// This will matching something like 'P7FOO' which would be 'FOO' for
    /// measurement 7. This might be useful in the future when this code offers
    /// "upgrade" routines since these are often used to represent future
    /// keywords in an older version where the newer version cannot be used for
    /// some reason.
    pub nonstandard_measurement_pattern: Option<NonStdMeasPattern>,
    // TODO add repair stuff
}

/// Instructions for reading the DATA segment.
#[derive(Default, Clone)]
pub struct DataReadConfig {
    /// If true, throw error when total event width does not evenly divide
    /// the DATA segment. Meaningless for delimited ASCII data.
    pub enfore_data_width_divisibility: bool,

    /// If true, throw error if the total number of events as computed by
    /// dividing DATA segment length event width doesn't match $TOT. Does
    /// nothing if $TOT not given, which may be the case in version 2.0.
    pub enfore_matching_tot: bool,
}

/// Configuration options that do not fit anywhere else
#[derive(Default, Clone)]
pub struct MiscReadConfig {
    /// If true, all warnings are considered to be fatal errors.
    pub warnings_are_errors: bool,
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

    /// If true, disallow lossy data conversions
    ///
    /// Example, f32 -> u32
    pub disallow_lossy_conversions: bool,
}

/// Instructions for reading the TEXT segment as raw key/value pairs.
#[derive(Default, Clone)]
pub struct RawTextReader {
    /// Will adjust the offset of the start of the TEXT segment by `offset + n`.
    pub starttext_delta: i32,

    /// Will adjust the offset of the end of the TEXT segment by `offset + n`.
    pub endtext_delta: i32,

    pub startdata_delta: i32,
    pub enddata_delta: i32,

    pub start_stext_delta: i32,
    pub end_stext_delta: i32,

    pub start_analysis_delta: i32,
    pub end_analysis_delta: i32,

    /// If true, all raw text parsing warnings will be considered fatal errors
    /// which will halt the parsing routine.
    pub warnings_are_errors: bool,

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

    /// If true, throw error when total event width does not evenly divide
    /// the DATA segment. Meaningless for delimited ASCII data.
    pub enfore_data_width_divisibility: bool,

    /// If true, throw error if the total number of events as computed by
    /// dividing DATA segment length event width doesn't match $TOT. Does
    /// nothing if $TOT not given, which may be the case in version 2.0.
    pub enfore_matching_tot: bool,

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
    pub date_pattern: Option<String>,
    // TODO add keyword and value overrides, something like a list of patterns
    // that can be used to alter each keyword
    // TODO allow lambda function to be supplied which will alter the kv list
}

/// Instructions for reading the TEXT segment in a standardized structure.
#[derive(Default, Clone)]
pub struct StdTextReader {
    pub raw: RawTextReader,

    /// If true, all metadata standardization warnings will be considered fatal
    /// errors which will halt the parsing routine.
    pub warnings_are_errors: bool,

    /// If given, will be the $PnN used to identify the time channel. Means
    /// nothing for 2.0.
    ///
    /// Will be used for the [`ensure_time*`] options below. If not given, skip
    /// time channel checking entirely.
    pub time_shortname: Option<String>,

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
    /// expression to match keywords. It should not start with a "$".
    ///
    /// This will matching something like 'P7FOO' which would be 'FOO' for
    /// measurement 7. This might be useful in the future when this code offers
    /// "upgrade" routines since these are often used to represent future
    /// keywords in an older version where the newer version cannot be used for
    /// some reason.
    pub nonstandard_measurement_pattern: Option<String>,
    // TODO add repair stuff
}

/// Instructions for reading the DATA segment.
#[derive(Default)]
pub struct DataReader {
    /// Will adjust the offset of the start of the TEXT segment by `offset + n`.
    datastart_delta: u32,
    /// Will adjust the offset of the end of the TEXT segment by `offset + n`.
    dataend_delta: u32,
}

/// Instructions for reading an FCS file.
#[derive(Default)]
pub struct Reader {
    pub text: StdTextReader,
    pub data: DataReader,
}

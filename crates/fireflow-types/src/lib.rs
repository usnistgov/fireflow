pub mod config;
#[cfg(feature = "python")]
pub mod python;

use const_format::formatcp;

pub const TIME_MEAS_NAME_PATTERN_NONE: &str = "NoTime";

pub const TIME_MEAS_NAME_PATTERN_DEFAULT: &str = "^(TIME|Time)$";

// must be valid regexp
pub const NON_STD_MEAS_PAT_DEFAULT: &str = formatcp!("^P{NON_STD_MEAS_INDEX_PAT}");

pub const NON_STD_MEAS_INDEX_PAT: &str = "%n";

pub const DEDUP_PNN_SEP: &str = "~";

pub const DEFAULT_DATE_FORMAT: &str = "%d-%b-%Y";

pub const DEFAULT_LAST_MODIFIED_FORMAT: &str = "%d-%b-%Y %H:%M:%S";

pub const DEFAULT_TIME_FORMAT_2_0: &str = "%H:%M:%S";

pub const DEFAULT_TIME_FORMAT_3_0: &str =
    formatcp!("{DEFAULT_TIME_FORMAT_2_0}:{BASE60_SECOND_SPEC}");

pub const DEFAULT_TIME_FORMAT_3_1: &str =
    formatcp!("{DEFAULT_TIME_FORMAT_2_0}.{BASE100_SECOND_SPEC}");

pub const BASE60_SECOND_SPEC: &str = "%!";

pub const BASE100_SECOND_SPEC: &str = "%@";

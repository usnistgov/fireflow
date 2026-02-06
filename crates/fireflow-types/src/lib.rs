use const_format::formatcp;

pub const TIME_MEAS_NAME_PATTERN_NONE: &str = "NoTime";

pub const TIME_MEAS_NAME_PATTERN_DEFAULT: &str = "^(TIME|Time)$";

// must be valid regexp
pub const NON_STD_MEAS_PAT_DEFAULT: &str = formatcp!("^P{NON_STD_MEAS_INDEX_PAT}");

pub const NON_STD_MEAS_INDEX_PAT: &str = "%n";

pub const DEDUP_PNN_SEP: &str = "~";

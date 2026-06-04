use crate::ne_str;
use crate::nonempty_string::NEStr;

// use derive_more::Display;

// #[cfg(feature = "python")]
// use {
//     crate::python as py,
//     fireflow_core_proc::{DisplayAsPyErr, FromPyString, IntoPyString},
// };

// #[cfg(feature = "serde")]
// use serde::Serialize;

// TEXT offset keyword origins

pub const TEXT_OFFSET_ORIGIN_EMPTY_TEXT_LEVEL: &NEStr = ne_str!("empty_text");
pub const TEXT_OFFSET_ORIGIN_IGNORED_LEVEL: &NEStr = ne_str!("ignored");
pub const TEXT_OFFSET_ORIGIN_MISSING_LEVEL: &NEStr = ne_str!("missing");
pub const TEXT_OFFSET_ORIGIN_MATCH_LEVEL: &NEStr = ne_str!("match");
pub const TEXT_OFFSET_ORIGIN_MISMATCH_HEADER_LEVEL: &NEStr = ne_str!("mismatch_header");
pub const TEXT_OFFSET_ORIGIN_MISMATCH_TEXT_LEVEL: &NEStr = ne_str!("mismatch_text");
pub const TEXT_OFFSET_ORIGIN_EMPTY_HEADER_LEVEL: &NEStr = ne_str!("empty_header");

// segment name constants

pub const SEGMENT_NAME_PTEXT: &NEStr = ne_str!("primary_text");
pub const SEGMENT_NAME_STEXT: &NEStr = ne_str!("supp_text");
pub const SEGMENT_NAME_HDATA: &NEStr = ne_str!("header_data");
pub const SEGMENT_NAME_TDATA: &NEStr = ne_str!("text_data");
pub const SEGMENT_NAME_HANALYSIS: &NEStr = ne_str!("header_analysis");
pub const SEGMENT_NAME_TANALYSIS: &NEStr = ne_str!("text_analysis");

pub const HEADER_NAME_TEXT: &NEStr = ne_str!("text");
pub const HEADER_NAME_STEXT: &NEStr = ne_str!("supp_text");
pub const HEADER_NAME_DATA: &NEStr = ne_str!("data");
pub const HEADER_NAME_ANALYSIS: &NEStr = ne_str!("analysis");

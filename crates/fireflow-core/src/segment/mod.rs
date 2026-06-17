pub mod read;
pub(crate) mod write;

use crate::text::keywords::{Beginanalysis, Begindata, Beginstext, Endanalysis, Enddata, Endstext};
use crate::validated::ascii_uint::UintZeroPad20;
use crate::validated::keys::Key;

use std::num::ParseIntError;
use std::str::FromStr;

/// Denotes segment offsets came from HEADER
#[derive(Default, Debug, Clone, Copy, PartialEq)]
pub struct OffsetsFromHeader;

/// Denotes segment offsets came from TEXT
#[derive(Default, Debug, Clone, Copy, PartialEq)]
pub struct OffsetsFromTEXT;

/// Denotes segment offsets pertains to primary TEXT
#[derive(Default, Debug, Clone, Copy, PartialEq)]
pub struct PrimaryTextSegmentId;

/// Denotes segment offsets pertains to supplemental TEXT
#[derive(Default, Debug, Clone, Copy, PartialEq)]
pub struct SupplementalTextSegmentId;

/// Denotes segment offsets pertains to DATA
#[derive(Default, Debug, Clone, Copy, PartialEq)]
pub struct DataSegmentId;

/// Denotes segment offsets pertains to ANALYSIS
#[derive(Default, Debug, Clone, Copy, PartialEq)]
pub struct AnalysisSegmentId;

/// Denotes segment offsets pertains to OTHER
#[derive(Default, Debug, Clone, Copy, PartialEq)]
pub struct OtherSegmentId;

/// Operations to obtain optional segment from TEXT keywords
pub trait KeyedOffsets: Sized + Copy {
    type B: Key + Into<UintZeroPad20> + FromStr<Err = ParseIntError>;
    type E: Key + Into<UintZeroPad20> + FromStr<Err = ParseIntError>;
}

impl KeyedOffsets for AnalysisSegmentId {
    type B = Beginanalysis;
    type E = Endanalysis;
}

impl KeyedOffsets for DataSegmentId {
    type B = Begindata;
    type E = Enddata;
}

impl KeyedOffsets for SupplementalTextSegmentId {
    type B = Beginstext;
    type E = Endstext;
}

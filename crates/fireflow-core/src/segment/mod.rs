pub mod read;
pub(crate) mod write;

use crate::text::keywords::{Beginanalysis, Begindata, Beginstext, Endanalysis, Enddata, Endstext};
use crate::validated::ascii_uint::UintZeroPad20;
use crate::validated::keys::Key;

use fireflow_types::segment::{AnalysisSegmentId, DataSegmentId, SupplementalTextSegmentId};

use std::num::ParseIntError;
use std::str::FromStr;

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

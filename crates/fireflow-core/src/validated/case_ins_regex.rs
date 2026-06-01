use std::hash::{Hash, Hasher};

use derive_more::{AsRef, Display};
use regex::Regex;
use std::str::FromStr;

#[cfg(feature = "python")]
use fireflow_core_proc::IntoPyString;

/// A regex which ignores case when matching
#[derive(Clone, AsRef, Display, Debug)]
#[cfg_attr(feature = "python", derive(IntoPyString))]
pub struct CaseInsRegex(Regex);

impl PartialEq<Self> for CaseInsRegex {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_str() == other.0.as_str()
    }
}

impl Eq for CaseInsRegex {}

impl Hash for CaseInsRegex {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.0.as_str().hash(state);
    }
}

impl FromStr for CaseInsRegex {
    type Err = regex::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        regex::RegexBuilder::new(s)
            .case_insensitive(true)
            .build()
            .map(Self)
    }
}

use std::hash::{Hash, Hasher};

use derive_more::AsRef;
use regex::Regex;
use std::str::FromStr;

/// A regex which ignores case when matching
#[derive(Clone, AsRef)]
pub struct CaseInsRegex {
    /// Keep the original string used to make the pattern for Eq/Hash impls.
    ///
    /// Assume they will always match
    src: String,
    /// The pattern, validated to ignore case.
    #[as_ref(Regex)]
    pattern: Regex,
}

impl PartialEq<Self> for CaseInsRegex {
    fn eq(&self, other: &Self) -> bool {
        self.src == other.src
    }
}

impl Eq for CaseInsRegex {}

impl Hash for CaseInsRegex {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.src.hash(state);
    }
}

impl FromStr for CaseInsRegex {
    type Err = regex::Error;

    // TODO forbid blanks since these will match anything
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        regex::RegexBuilder::new(s)
            .case_insensitive(true)
            .build()
            .map(|pattern| Self {
                src: s.to_owned(),
                pattern,
            })
    }
}

use crate::macros::{newtype_asref, newtype_disp};

use serde::Serialize;
use std::borrow::Borrow;
use std::fmt;
use std::str::FromStr;

/// The value for the $PnN key (all versions).
///
/// This cannot contain commas.
#[derive(Clone, Serialize, Eq, PartialEq, Hash)]
pub struct Shortname(String);

/// A prefix that can be made into a shortname by appending an index
///
/// This cannot contain commas.
#[derive(Clone, Serialize, Eq, PartialEq, Hash)]
pub struct ShortnamePrefix(Shortname);

newtype_asref!(Shortname, str);
newtype_disp!(Shortname);

newtype_asref!(ShortnamePrefix, str);
newtype_disp!(ShortnamePrefix);

impl Borrow<str> for Shortname {
    fn borrow(&self) -> &str {
        self.0.as_str()
    }
}

impl Shortname {
    pub fn new_unchecked<T: AsRef<str>>(s: T) -> Self {
        Shortname(s.as_ref().to_owned())
    }

    pub fn from_index(n: usize) -> Self {
        Shortname(format!("M{n}"))
    }
}

impl FromStr for Shortname {
    type Err = ShortnameError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.contains(',') {
            Err(ShortnameError(s.to_string()))
        } else {
            Ok(Shortname(s.to_string()))
        }
    }
}

impl FromStr for ShortnamePrefix {
    type Err = ShortnameError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<Shortname>().map(ShortnamePrefix)
    }
}

impl ShortnamePrefix {
    // TODO use MeasIdx here?
    pub fn as_indexed(&self, i: usize) -> Shortname {
        // TODO +1?
        Shortname(format!("{}{i}", self))
    }
}

impl Default for ShortnamePrefix {
    fn default() -> ShortnamePrefix {
        ShortnamePrefix(Shortname("P".into()))
    }
}

pub struct ShortnameError(String);

impl fmt::Display for ShortnameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "commas are not allowed in name '{}'", self.0)
    }
}

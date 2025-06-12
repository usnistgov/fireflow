use crate::config::RawTextReadConfig;
use crate::error::*;
use crate::macros::{enum_from, enum_from_disp, match_many_to_one};
use crate::text::index::IndexFromOne;
use crate::validated::nonstandard::*;

use serde::Serialize;
use std::borrow::Borrow;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt;
use std::str;

/// Represents a standard key.
///
/// These are assumed to only contain ASCII with uppercase letters and start
/// with a dollar sign. The dollar sign is not actually stored but will be
/// appended when converting to a raw string.
///
/// The only way to make such a key is to parse it from a bytestring (which
/// can fail in numerous ways) or to make a type for the key and implement
/// one of the 'Key', 'IndexedKey', or 'BiIndexedKey' traits which can create
/// a key from thin-air.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize)]
pub struct StdKey(String);

/// A collection dump for parsed keywords of varying quality
#[derive(Default)]
pub struct ParsedKeywords {
    /// Standard keywords (with '$')
    pub std: StdKeywords,

    /// Non-standard keywords (without '$')
    pub nonstd: NonStdKeywords,

    /// Keywords that don't have ASCII keys (not allowed)
    pub non_ascii: NonAsciiPairs,

    /// Keywords that are not valid UTF-8 strings
    pub byte_pairs: BytesPairs,
}

/// 'ParsedKeywords' without the bad stuff
#[derive(Clone, Default, Serialize)]
pub struct ValidKeywords {
    pub std: StdKeywords,
    pub nonstd: NonStdKeywords,
}

/// A standard key
///
/// The constant traits is assumed to only have uppercase ASCII.
// TODO this will allow much cleaner code once const_trait_impl is stable
pub(crate) trait Key {
    const C: &'static str;

    fn std() -> StdKey {
        StdKey(Self::C.to_string())
    }

    fn len() -> u64 {
        (Self::C.len() + 1) as u64
    }
}

/// A standard key with on index
///
/// The constant traits are assumed to only have uppercase ASCII.
pub(crate) trait IndexedKey {
    const PREFIX: &'static str;
    const SUFFIX: &'static str;

    fn std(i: IndexFromOne) -> StdKey {
        // reserve enough space for prefix, suffix, and a number with 3 digits
        let n = Self::PREFIX.len() + 3 + Self::SUFFIX.len();
        let mut s = String::with_capacity(n);
        s.push_str(Self::PREFIX);
        s.push_str(i.to_string().as_str());
        s.push_str(Self::SUFFIX);
        StdKey(s)
    }

    fn std_blank() -> String {
        // reserve enough space for '$', prefix, suffix, and 'n'
        let n = Self::PREFIX.len() + 2 + Self::SUFFIX.len();
        let mut s = String::new();
        s.reserve_exact(n);
        s.push('$');
        s.push_str(Self::PREFIX);
        s.push('n');
        s.push_str(Self::SUFFIX);
        s
    }

    // /// Return true if a key matches the prefix/suffix.
    // ///
    // /// Specifically, test if string is like <PREFIX><N><SUFFIX> where
    // /// N is an integer greater than zero.
    // fn matches(other: &str, std: bool) -> bool {
    //     if std {
    //         other.strip_prefix("$")
    //     } else {
    //         Some(other)
    //     }
    //     .and_then(|s| s.strip_prefix(Self::PREFIX))
    //     .and_then(|s| s.strip_suffix(Self::SUFFIX))
    //     .and_then(|s| s.parse::<u32>().ok())
    //     .is_some_and(|x| x > 0)
    // }
}

/// A standard key with two indices
///
/// The constant traits are assumed to only have uppercase ASCII.
pub(crate) trait BiIndexedKey {
    const PREFIX: &'static str;
    const MIDDLE: &'static str;
    const SUFFIX: &'static str;

    fn std(i: IndexFromOne, j: IndexFromOne) -> StdKey {
        // reserve enough space for prefix, middle, suffix, and two numbers with
        // 2 digits
        let n = Self::PREFIX.len() + Self::MIDDLE.len() + Self::SUFFIX.len() + 4;
        let mut s = String::with_capacity(n);
        s.push_str(Self::PREFIX);
        s.push_str(i.to_string().as_str());
        s.push_str(Self::MIDDLE);
        s.push_str(j.to_string().as_str());
        s.push_str(Self::SUFFIX);
        StdKey(s)
    }

    // fn std_blank() -> String {
    //     // reserve enough space for '$', prefix, middle, suffix, and 'n'/'m'
    //     let n = Self::PREFIX.len() + 2 + Self::SUFFIX.len();
    //     let mut s = String::new();
    //     s.reserve_exact(n);
    //     s.push('$');
    //     s.push_str(Self::PREFIX);
    //     s.push('m');
    //     s.push_str(Self::MIDDLE);
    //     s.push('n');
    //     s.push_str(Self::SUFFIX);
    //     s
    // }

    // /// Return true if a key matches the prefix/suffix.
    // ///
    // /// Specifically, test if string is like <PREFIX><N><SUFFIX> where
    // /// N is an integer greater than zero.
    // fn matches(other: &str, std: bool) -> bool {
    //     if std {
    //         other.strip_prefix("$")
    //     } else {
    //         Some(other)
    //     }
    //     .and_then(|s| s.strip_prefix(Self::PREFIX))
    //     .and_then(|s| s.strip_suffix(Self::SUFFIX))
    //     .and_then(|s| s.parse::<u32>().ok())
    //     .is_some_and(|x| x > 0)
    // }
}

pub type StdKeywords = HashMap<StdKey, String>;
pub type NonAsciiPairs = Vec<(String, String)>;
pub type BytesPairs = Vec<(Vec<u8>, Vec<u8>)>;

impl AsRef<str> for StdKey {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

impl Borrow<str> for StdKey {
    fn borrow(&self) -> &str {
        self.0.borrow()
    }
}

impl fmt::Display for StdKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "${}", self.0)
    }
}

impl ParsedKeywords {
    pub(crate) fn insert(
        &mut self,
        k: &[u8],
        v: &[u8],
        conf: &RawTextReadConfig,
    ) -> Result<(), Leveled<KeywordInsertError>> {
        // ASSUME key and value are never blank since we checked both prior to
        // calling this. The FCS standards do not allow either to be blank.
        let n = k.len();
        match str::from_utf8(v) {
            Ok(vv) => {
                // Trim whitespace from value if desired. Warn (or half) if this
                // results in a blank.
                let value = if conf.trim_value_whitespace {
                    let trimmed = vv.trim();
                    if trimmed.is_empty() {
                        let w = BlankValueError(k.to_vec());
                        return Err(Leveled::new(w.into(), !conf.allow_empty));
                    } else {
                        trimmed.to_string()
                    }
                } else {
                    vv.to_string()
                };
                if n > 1 && k[0] == STD_PREFIX && is_printable_ascii(&k[1..]) {
                    // Standard key: starts with '$', check remaining chars are
                    // ASCII and convert lowercase to uppercase
                    let xs = k[1..].iter().copied().map(ascii_to_upper).collect();
                    let kk = StdKey(unsafe { String::from_utf8_unchecked(xs) });
                    match self.std.entry(kk) {
                        Entry::Occupied(e) => {
                            let w = StdPresent {
                                key: e.key().clone(),
                                value,
                            };
                            Err(Leveled::new(w.into(), !conf.allow_nonunique))
                        }
                        Entry::Vacant(e) => {
                            e.insert(value);
                            Ok(())
                        }
                    }
                } else if n > 0 && is_printable_ascii(k) {
                    // Non-standard key: does not start with '$' but is still
                    // ASCII
                    let kk = NonStdKey::into_unchecked(unsafe {
                        String::from_utf8_unchecked(k.to_vec())
                    });
                    match self.nonstd.entry(kk) {
                        Entry::Occupied(e) => {
                            let w = NonStdPresent {
                                key: e.key().clone(),
                                value,
                            };
                            Err(Leveled::new(w.into(), !conf.allow_nonunique))
                        }
                        Entry::Vacant(e) => {
                            e.insert(value);
                            Ok(())
                        }
                    }
                } else if let Ok(kk) = String::from_utf8(k.to_vec()) {
                    // Non-ascii key: these are technically not allowed but save
                    // them anyways in case the user cares. If key isn't UTF-8
                    // then give up.
                    self.non_ascii.push((kk, value));
                    Ok(())
                } else {
                    self.byte_pairs.push((k.to_vec(), value.into()));
                    Ok(())
                }
            }
            _ => {
                self.byte_pairs.push((k.to_vec(), v.to_vec()));
                Ok(())
            }
        }
    }
}

enum_from_disp!(
    #[derive(Debug)]
    pub KeywordInsertError,
    [StdPresent, StdPresent],
    [NonStdPresent, NonStdPresent],
    [Blank, BlankValueError]
);

#[derive(Debug)]
pub struct BlankValueError(pub Vec<u8>);

#[derive(Debug)]
pub struct StdPresent {
    key: StdKey,
    value: String,
}

#[derive(Debug)]
pub struct NonStdPresent {
    key: NonStdKey,
    value: String,
}

impl fmt::Display for StdPresent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "std key '{}' already present, has value '{}'",
            self.key, self.value
        )
    }
}

impl fmt::Display for NonStdPresent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "non-std key '{}' already present, has value '{}'",
            self.key, self.value
        )
    }
}

fn is_printable_ascii(xs: &[u8]) -> bool {
    xs.iter().all(|x| 32 <= *x && *x <= 126)
}

fn ascii_to_upper(x: u8) -> u8 {
    if (97..=122).contains(&x) {
        x - 32
    } else {
        x
    }
}

const STD_PREFIX: u8 = 36; // '$'

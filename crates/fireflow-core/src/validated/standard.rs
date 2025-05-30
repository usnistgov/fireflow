use crate::validated::nonstandard::*;

use serde::Serialize;
use std::borrow::Borrow;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

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
#[derive(Clone, PartialEq, Eq, Hash, Serialize)]
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

    fn len() -> usize {
        Self::C.len() + 1
    }
}

pub type ReqResult<T> = Result<T, ReqKeyError<<T as FromStr>::Err>>;
pub type OptResult<T> = Result<Option<T>, ParseKeyError<<T as FromStr>::Err>>;

/// Find a required standard key in a hash table
pub(crate) fn get_req<T>(kws: &StdKeywords, k: &StdKey) -> ReqResult<T>
where
    T: FromStr,
{
    match kws.get(k) {
        Some(v) => v
            .parse()
            .map_err(|error| ParseKeyError {
                error,
                key: k.clone(),
                value: v.clone(),
            })
            .map_err(ReqKeyError::Parse),
        None => Err(ReqKeyError::Missing(k.clone())),
    }
}

/// Find an optional standard key in a hash table
pub(crate) fn get_opt<T>(kws: &StdKeywords, k: &StdKey) -> OptResult<T>
where
    T: FromStr,
{
    kws.get(k)
        .map(|v| {
            v.parse().map_err(|error| ParseKeyError {
                error,
                key: k.clone(),
                value: v.clone(),
            })
        })
        .transpose()
}

/// Find a required standard key in a hash table and remove it
pub(crate) fn remove_req<T>(kws: &mut StdKeywords, k: &StdKey) -> ReqResult<T>
where
    T: FromStr,
{
    match kws.remove(k) {
        Some(v) => v
            .parse()
            .map_err(|error| ParseKeyError {
                error,
                key: k.clone(),
                value: v,
            })
            .map_err(ReqKeyError::Parse),
        None => Err(ReqKeyError::Missing(k.clone())),
    }
}

/// Find an optional standard key in a hash table and remove it
pub(crate) fn remove_opt<T>(kws: &mut StdKeywords, k: &StdKey) -> OptResult<T>
where
    T: FromStr,
{
    kws.remove(k)
        .map(|v| {
            v.parse().map_err(|error| ParseKeyError {
                error,
                key: k.clone(),
                value: v,
            })
        })
        .transpose()
}

/// A standard key with on index
///
/// The constant traits are assumed to only have uppercase ASCII.
pub(crate) trait IndexedKey {
    const PREFIX: &'static str;
    const SUFFIX: &'static str;

    fn std(i: MeasIdx) -> StdKey {
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

    fn std(i: usize, j: usize) -> StdKey {
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
    pub(crate) fn insert(&mut self, k: &[u8], v: &[u8]) -> Result<(), KeywordInsertError> {
        let n = k.len();
        let vv = v.to_vec();
        match String::from_utf8(vv) {
            Ok(vs) => {
                if n > 1 && k[0] == 36 && is_ascii(&k[1..]) {
                    // Standard key: starts with '$', check remaining chars are
                    // ASCII and convert lowercase to uppercase
                    let xs = k[1..]
                        .iter()
                        .map(|x| if (97..=122).contains(x) { *x - 32 } else { *x })
                        .collect();
                    let kk = StdKey(unsafe { String::from_utf8_unchecked(xs) });
                    match self.std.entry(kk) {
                        Entry::Occupied(e) => {
                            Err(KeywordInsertError::StdPresent(e.key().clone(), vs))
                        }
                        Entry::Vacant(e) => {
                            e.insert(vs);
                            Ok(())
                        }
                    }
                } else if n > 0 && is_ascii(k) {
                    // Non-standard key: does not start with '$' but is still
                    // ASCII
                    let kk = NonStdKey::into_unchecked(unsafe {
                        String::from_utf8_unchecked(k.to_vec())
                    });
                    match self.nonstd.entry(kk) {
                        Entry::Occupied(e) => {
                            Err(KeywordInsertError::NonStdPresent(e.key().clone(), vs))
                        }
                        Entry::Vacant(e) => {
                            e.insert(vs);
                            Ok(())
                        }
                    }
                } else if let Ok(kk) = String::from_utf8(k.to_vec()) {
                    // Non-ascii key: these are technically not allowed but save
                    // them anyways in case the user cares. If key isn't UTF-8
                    // then give up.
                    self.non_ascii.push((kk, vs));
                    Ok(())
                } else {
                    self.byte_pairs.push((k.to_vec(), vs.into()));
                    Ok(())
                }
            }
            Err(e) => {
                self.byte_pairs.push((k.to_vec(), e.into_bytes()));
                Ok(())
            }
        }
    }
}

pub enum KeywordInsertError {
    StdPresent(StdKey, String),
    NonStdPresent(NonStdKey, String),
}

impl fmt::Display for KeywordInsertError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            KeywordInsertError::StdPresent(k, v) => {
                write!(f, "std key '{k}' already present, has value '{v}'")
            }
            KeywordInsertError::NonStdPresent(k, v) => {
                write!(f, "non-std key '{k}' already present, has value '{v}'")
            }
        }
    }
}

pub struct ParseKeyError<E> {
    pub error: E,
    pub key: StdKey,
    pub value: String,
}

impl<E> fmt::Display for ParseKeyError<E>
where
    E: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "{} (key='{}', value='{}')",
            self.error, self.key, self.value
        )
    }
}

pub enum ReqKeyError<E> {
    Parse(ParseKeyError<E>),
    Missing(StdKey),
}

impl<E> fmt::Display for ReqKeyError<E>
where
    E: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            ReqKeyError::Parse(x) => x.fmt(f),
            ReqKeyError::Missing(k) => write!(f, "missing required key: {k}"),
        }
    }
}

fn is_ascii(xs: &[u8]) -> bool {
    xs.iter().all(|x| 36 <= *x && *x <= 126)
}

use crate::config::RawTextReadConfig;
use crate::error::*;
use crate::text::index::IndexFromOne;

use derive_more::{AsRef, Display, From};
use itertools::Itertools;
use regex::Regex;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::Hash;
use std::str;
use std::str::FromStr;
use unicase::Ascii;

#[cfg(feature = "serde")]
use serde::Serialize;

/// A standard key.
///
/// These may only contain ASCII and must start with "$". The "$" is not
/// actually stored but will be appended when converting to a ['String'].
#[derive(Clone, Debug, PartialEq, Eq, Hash, AsRef)]
#[as_ref(KeyString, str)]
pub struct StdKey(KeyString);

/// A non-standard key.
///
/// This cannot start with '$' and may only contain ASCII characters.
#[derive(Clone, Debug, AsRef, Display, PartialEq, Eq, Hash)]
#[as_ref(KeyString, str)]
pub struct NonStdKey(KeyString);

pub type NonStdPairs = Vec<(NonStdKey, String)>;
pub type NonStdKeywords = HashMap<NonStdKey, String>;

/// The internal string for a key (standard or nonstandard).
///
/// Must be non-empty and contain only ASCII characters. Comparisons will be
/// case-insensitive.
#[derive(Clone, Debug, AsRef, Display, PartialEq, Eq, Hash)]
#[as_ref(str)]
pub struct KeyString(Ascii<String>);

/// A String that matches part of a non-standard measurement key.
///
/// This will have exactly one '%n' and not start with a '$'. The
/// '%n' will be replaced by the measurement index which will be used
/// to match keywords.
#[derive(Clone, AsRef, Display)]
#[as_ref(str)]
pub struct NonStdMeasPattern(String);

/// A list of patterns that match standard or non-standard keys.
#[derive(Clone, Default)]
pub struct KeyPatterns(Vec<KeyStringOrPattern>);

/// Either a literal string or regexp which matches a standard/non-standard key.
///
/// This exists for performance and ergononic reasons; if the goal is simply to
/// match lots of strings literally, it is faster and easier to use a hash
/// table, otherwise we need to search linearly through an array of patterns.
#[derive(Clone)]
pub enum KeyStringOrPattern {
    Literal(KeyString),
    Pattern(CaseInsRegex),
}

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

pub type StdKeywords = HashMap<StdKey, String>;
pub type NonAsciiPairs = Vec<(String, String)>;
pub type BytesPairs = Vec<(Vec<u8>, Vec<u8>)>;

/// ['ParsedKeywords'] without the bad stuff
#[derive(Clone, Default)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct ValidKeywords {
    pub std: StdKeywords,
    pub nonstd: NonStdKeywords,
}

/// A string that should be used as the header in the measurement table.
#[derive(Display)]
pub struct MeasHeader(pub String);

/// A regular expression which matches a non-standard measurement key.
///
/// This must be derived from ['NonStdMeasPattern'].
#[derive(AsRef)]
#[as_ref(Regex)]
pub(crate) struct NonStdMeasRegex(CaseInsRegex);

/// A regex which ignores case when matching
#[derive(Clone, AsRef)]
pub struct CaseInsRegex(Regex);

/// A "compiled" object to match keys efficiently.
struct KeyMatcher<'a> {
    literal: HashSet<&'a KeyString>,
    pattern: Vec<&'a CaseInsRegex>,
}

/// A standard key
///
/// The constant traits is assumed to only contain ASCII characters.
// TODO const_trait_impl will be able to clean this up once stable
pub(crate) trait Key {
    const C: &'static str;

    fn std() -> StdKey {
        StdKey::new(Self::C.to_string())
    }

    fn len() -> u64 {
        (Self::C.len() + 1) as u64
    }
}

/// A standard key with on index
///
/// The constant traits are assumed to only contain ASCII characters.
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
        StdKey::new(s)
    }

    fn std_blank() -> MeasHeader {
        // reserve enough space for '$', prefix, suffix, and 'n'
        let n = Self::PREFIX.len() + 2 + Self::SUFFIX.len();
        let mut s = String::new();
        s.reserve_exact(n);
        s.push('$');
        s.push_str(Self::PREFIX);
        s.push('n');
        s.push_str(Self::SUFFIX);
        MeasHeader(s)
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
/// The constant traits are assumed to only contain ASCII characters.
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
        StdKey::new(s)
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

impl KeyString {
    fn new(s: String) -> Self {
        Self(Ascii::new(s))
    }

    fn from_bytes(xs: &[u8]) -> Self {
        Self::new(unsafe { String::from_utf8_unchecked(xs.to_vec()) })
    }
}

impl StdKey {
    fn new(s: String) -> Self {
        Self(KeyString::new(s))
    }
}

impl NonStdKey {
    fn new(s: String) -> Self {
        Self(KeyString::new(s))
    }
}

#[cfg(feature = "serde")]
impl Serialize for StdKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.as_ref().serialize(serializer)
    }
}

#[cfg(feature = "serde")]
impl Serialize for NonStdKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.as_ref().serialize(serializer)
    }
}

impl fmt::Display for StdKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "${}", self.0)
    }
}

impl FromStr for KeyString {
    type Err = AsciiStringError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            Err(AsciiStringError::Empty)
        } else if !is_printable_ascii(s.as_ref()) {
            Err(AsciiStringError::Ascii(s.to_string()))
        } else {
            Ok(Self(Ascii::new(s.to_string())))
        }
    }
}

impl FromStr for StdKey {
    type Err = StdKeyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<KeyString>()
            .map_err(StdKeyError::Ascii)
            .and_then(|ks| {
                // ASSUME this will not fail because we know the string is
                // non-empty
                let (y, ys) = ks.as_ref().as_bytes().split_first().unwrap();
                if ys.is_empty() {
                    Err(StdKeyError::Empty)
                } else if *y != STD_PREFIX {
                    Err(StdKeyError::Prefix(ks))
                } else {
                    // ASSUME this will not fail because we know the string has
                    // only ASCII bytes
                    Ok(Self(KeyString::from_bytes(ys)))
                }
            })
    }
}

impl FromStr for NonStdKey {
    type Err = NonStdKeyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<KeyString>()
            .map_err(NonStdKeyError::Ascii)
            .and_then(|ks| {
                if has_no_std_prefix(ks.as_ref().as_bytes()) {
                    Ok(Self::new(ks.to_string()))
                } else {
                    Err(NonStdKeyError::Prefix(ks))
                }
            })
    }
}

impl FromStr for NonStdMeasPattern {
    type Err = NonStdMeasPatternError;

    fn from_str(s: &str) -> Result<Self, NonStdMeasPatternError> {
        if has_no_std_prefix(s.as_bytes()) || s.match_indices("%n").count() == 1 {
            Ok(NonStdMeasPattern(s.to_string()))
        } else {
            Err(NonStdMeasPatternError(s.to_string()))
        }
    }
}

impl NonStdMeasPattern {
    pub(crate) fn apply_index(
        &self,
        n: IndexFromOne,
    ) -> Result<NonStdMeasRegex, NonStdMeasRegexError> {
        self.0
            .replace("%n", n.to_string().as_str())
            .as_str()
            .parse::<CaseInsRegex>()
            .map_err(|error| NonStdMeasRegexError { error, index: n })
            .map(NonStdMeasRegex)
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

impl KeyPatterns {
    pub fn extend(&mut self, other: Self) {
        self.0.extend(other.0)
    }

    pub fn try_from_literals(ss: Vec<String>) -> Result<Self, AsciiStringError> {
        ss.into_iter()
            .unique()
            .map(|s| s.parse::<KeyString>().map(KeyStringOrPattern::Literal))
            .collect::<Result<Vec<_>, _>>()
            .map(KeyPatterns)
    }

    pub fn try_from_patterns(ss: Vec<String>) -> Result<Self, regex::Error> {
        ss.into_iter()
            .unique()
            .map(|s| s.parse::<CaseInsRegex>().map(KeyStringOrPattern::Pattern))
            .collect::<Result<Vec<_>, _>>()
            .map(KeyPatterns)
    }

    fn as_matcher(&self) -> KeyMatcher<'_> {
        let (literal, pattern): (HashSet<_>, Vec<_>) = self
            .0
            .iter()
            .map(|x| match x {
                KeyStringOrPattern::Literal(l) => Ok(l),
                KeyStringOrPattern::Pattern(p) => Err(p),
            })
            .partition_result();
        KeyMatcher { literal, pattern }
    }
}

impl KeyMatcher<'_> {
    fn is_match(&self, other: &KeyString) -> bool {
        self.literal.contains(other)
            || self
                .pattern
                .iter()
                .any(|p| p.as_ref().is_match(other.as_ref()))
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

        let to_std = conf.promote_to_standard.as_matcher();
        let to_nonstd = conf.demote_from_standard.as_matcher();
        // TODO this also should skip keys before throwing a blank error
        let ignore = conf.ignore_standard_keys.as_matcher();

        match std::str::from_utf8(v) {
            Ok(vv) => {
                // Trim whitespace from value if desired. Warn (or halt) if this
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
                    // Standard key: starts with '$', check that remaining chars
                    // are ASCII
                    let kk = KeyString::from_bytes(&k[1..]);
                    if ignore.is_match(&kk) {
                        Ok(())
                    } else if to_nonstd.is_match(&kk) {
                        insert_nonunique(&mut self.nonstd, NonStdKey(kk), value, conf)
                    } else {
                        let rk = conf.rename_standard_keys.get(&kk).cloned().unwrap_or(kk);
                        insert_nonunique(&mut self.std, StdKey(rk), value, conf)
                    }
                } else if n > 0 && is_printable_ascii(k) {
                    // Non-standard key: does not start with '$' but is still
                    // ASCII
                    let kk = KeyString::from_bytes(k);
                    if to_std.is_match(&kk) {
                        insert_nonunique(&mut self.std, StdKey(kk), value, conf)
                    } else {
                        insert_nonunique(&mut self.nonstd, NonStdKey(kk), value, conf)
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

    pub(crate) fn append_std(
        &mut self,
        new: &HashMap<KeyString, String>,
        allow_nonunique: bool,
    ) -> MultiResult<(), Leveled<StdPresent>> {
        new.iter()
            .map(|(k, v)| match self.std.entry(StdKey(k.clone())) {
                Entry::Occupied(e) => {
                    let key = e.key().clone();
                    let value = v.clone();
                    let w = KeyPresent { key, value };
                    Err(Leveled::new(w, !allow_nonunique))
                }
                Entry::Vacant(e) => {
                    e.insert(v.clone());
                    Ok(())
                }
            })
            .gather()
            .void()
    }
}

#[derive(Debug, Display, From, PartialEq)]
pub enum KeywordInsertError {
    StdPresent(StdPresent),
    NonStdPresent(NonStdPresent),
    Blank(BlankValueError),
}

#[derive(Debug, PartialEq)]
pub struct BlankValueError(pub Vec<u8>);

#[derive(Debug, PartialEq)]
pub struct KeyPresent<T> {
    pub key: T,
    pub value: String,
}

pub type StdPresent = KeyPresent<StdKey>;
pub type NonStdPresent = KeyPresent<NonStdKey>;

#[derive(PartialEq, Debug)]
pub enum AsciiStringError {
    Ascii(String),
    Empty,
}

#[derive(From, PartialEq, Debug)]
pub enum StdKeyError {
    Ascii(AsciiStringError),
    Prefix(KeyString),
    Empty,
}

#[derive(From, PartialEq, Debug)]
pub enum NonStdKeyError {
    Ascii(AsciiStringError),
    Prefix(KeyString),
}

pub struct NonStdMeasKeyError(String);

#[derive(Debug)]
pub struct NonStdMeasPatternError(String);

pub struct NonStdMeasRegexError {
    error: regex::Error,
    index: IndexFromOne,
}

impl<T: fmt::Display> fmt::Display for KeyPresent<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "key '{}' already present, has value '{}'",
            self.key, self.value
        )
    }
}

impl fmt::Display for AsciiStringError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Self::Empty => f.write_str("Key string must not be empty"),
            Self::Ascii(s) => write!(f, "string should only have ASCII characters, found '{s}'",),
        }
    }
}

impl fmt::Display for StdKeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Self::Ascii(x) => x.fmt(f),
            Self::Prefix(s) => {
                write!(f, "Standard key must start with '$', found '{s}'")
            }
            Self::Empty => f.write_str("Standard key must not be empty, got '$'"),
        }
    }
}

impl fmt::Display for NonStdKeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Self::Ascii(x) => x.fmt(f),
            Self::Prefix(s) => {
                write!(f, "Non-standard key must not start with '$', found '{s}'")
            }
        }
    }
}

impl fmt::Display for NonStdMeasKeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "Non standard measurement pattern must not \
             start with '$', have only ASCII characters, \
             and should have one '%n', found '{}'",
            self.0
        )
    }
}

impl fmt::Display for NonStdMeasPatternError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "Non standard measurement pattern must not \
             start with '$' and should have one '%n', found '{}'",
            self.0
        )
    }
}

impl fmt::Display for NonStdMeasRegexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "Regexp error for measurement {}: {}",
            self.index, self.error
        )
    }
}

impl fmt::Display for BlankValueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let s = str::from_utf8(&self.0[..]).map_or_else(
            |_| format!("key's bytes were {}", self.0.iter().join(",")),
            |s| format!("key was {s}"),
        );
        write!(f, "skipping key with blank value, {s}")
    }
}

fn is_printable_ascii(xs: &[u8]) -> bool {
    xs.iter().all(|x| 32 <= *x && *x <= 126)
}

fn has_no_std_prefix(xs: &[u8]) -> bool {
    xs.first().is_some_and(|x| *x != STD_PREFIX)
}

fn insert_nonunique<K>(
    kws: &mut HashMap<K, String>,
    k: K,
    value: String,
    conf: &RawTextReadConfig,
) -> Result<(), Leveled<KeywordInsertError>>
where
    K: std::hash::Hash + Eq + Clone + AsRef<KeyString>,
    KeywordInsertError: From<KeyPresent<K>>,
{
    match kws.entry(k) {
        Entry::Occupied(e) => {
            let key = e.key().clone();
            let w = KeyPresent { key, value };
            Err(Leveled::new(w.into(), !conf.allow_nonunique))
        }
        Entry::Vacant(e) => {
            let v = conf
                .replace_standard_key_values
                .get(e.key().as_ref())
                .map(|v| v.to_string())
                .unwrap_or(value);
            e.insert(v);
            Ok(())
        }
    }
}

const STD_PREFIX: u8 = 36; // '$'

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fromstr_std_key() {
        let s = "$MAJESTY";
        let k = s.parse::<StdKey>().unwrap();
        assert_eq!(StdKey(KeyString(Ascii::new("MAJESTY".into()))), k);
        // reverse process should give back original string
        assert_eq!(k.to_string(), s.to_string());
        // and such a valid key should behave the same when inserted into
        // the hash table
        let mut p = ParsedKeywords::default();
        let res = p.insert(
            s.as_bytes(),
            b"of_the_night_sky",
            &RawTextReadConfig::default(),
        );
        assert_eq!(Ok(()), res);
        assert_eq!(
            s.to_string(),
            p.std.into_iter().next().unwrap().0.to_string()
        );
    }

    #[test]
    fn fromstr_std_key_nonascii() {
        let s = "$花冷え。"; // sugarsugarsugarsugarsugarsugarrrrrrrrr...
        let k = s.parse::<StdKey>();
        assert_eq!(
            Err(StdKeyError::Ascii(AsciiStringError::Ascii(s.into()))),
            k
        );
    }

    #[test]
    fn fromstr_std_key_noprefix() {
        let s = "IMBROKE";
        let k = s.parse::<StdKey>();
        assert_eq!(
            Err(StdKeyError::Prefix(KeyString(Ascii::new(s.to_string())))),
            k
        );
    }

    #[test]
    fn fromstr_std_key_blank() {
        let s = "";
        let k = s.parse::<StdKey>();
        assert_eq!(Err(StdKeyError::Ascii(AsciiStringError::Empty)), k);
    }

    #[test]
    fn fromstr_std_key_onlyprefix() {
        let s = "$";
        let k = s.parse::<StdKey>();
        assert_eq!(Err(StdKeyError::Empty), k);
    }

    #[test]
    fn fromstr_nonstd_key() {
        let s = "YTSEJAM";
        let k = s.parse::<NonStdKey>().unwrap();
        assert_eq!(NonStdKey(KeyString(Ascii::new("YTSEJAM".into()))), k);
        // reverse process should give back original string
        assert_eq!(k.to_string(), s.to_string());
        // and such a valid key should behave the same when inserted into
        // the hash table
        let mut p = ParsedKeywords::default();
        let res = p.insert(
            s.as_bytes(),
            b"the cake is a lie",
            &RawTextReadConfig::default(),
        );
        assert_eq!(Ok(()), res);
        assert_eq!(
            s.to_string(),
            p.nonstd.into_iter().next().unwrap().0.to_string()
        );
    }

    #[test]
    fn fromstr_nonstd_key_nonascii() {
        let s = "サイ";
        let k = s.parse::<NonStdKey>();
        assert_eq!(
            Err(NonStdKeyError::Ascii(AsciiStringError::Ascii(s.into()))),
            k
        );
    }

    #[test]
    fn fromstr_nonstd_key_hasprefix() {
        let s = "$IMRICH";
        let k = s.parse::<NonStdKey>();
        assert_eq!(
            Err(NonStdKeyError::Prefix(KeyString(Ascii::new(s.to_string())))),
            k
        );
    }

    #[test]
    fn fromstr_nonstd_key_blank() {
        let s = "";
        let k = s.parse::<NonStdKey>();
        assert_eq!(Err(NonStdKeyError::Ascii(AsciiStringError::Empty)), k);
    }
}

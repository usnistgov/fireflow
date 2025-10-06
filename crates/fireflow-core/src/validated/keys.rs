use crate::config::ReadHeaderAndTEXTConfig;
use crate::error::{ErrorIter, Leveled, MultiResult, ResultExt};
use crate::text::index::IndexFromOne;

use derive_more::{AsRef, Display, From};
use derive_new::new;
use itertools::Itertools;
use nonempty::NonEmpty;
use regex::Regex;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;
use std::str;
use std::str::FromStr;
use std::string::ToString;
use std::sync::OnceLock;
use thiserror::Error;
use unicase::Ascii;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use pyo3::prelude::*;

/// A standard key.
///
/// These may only contain ASCII and must start with "$". The "$" is not
/// actually stored but will be appended when converting to a [`String`].
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, AsRef, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[as_ref(KeyString, str)]
#[display("${_0}")]
pub struct StdKey(KeyString);

/// A non-standard key.
///
/// This cannot start with '$' and may only contain ASCII characters.
#[derive(Clone, Debug, AsRef, Display, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[as_ref(KeyString, str)]
pub struct NonStdKey(KeyString);

/// The internal string for a key (standard or nonstandard).
///
/// Must be non-empty and contain only ASCII characters. Comparisons will be
/// case-insensitive.
#[derive(Clone, Debug, AsRef, Display, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[as_ref(str)]
pub struct KeyString(Ascii<String>);

/// A map of keystring-keystring pairs.
///
/// The main use case for this is to rename keys.
///
/// This will be validated such that no pair has matching source and
/// destination.
#[derive(Clone, Debug, Default)]
pub struct KeyStringPairs(HashMap<KeyString, KeyString>);

/// A map of keystrings and strings.
///
/// The main use case for this is to replace or add key values.
pub type KeyStringValues = HashMap<KeyString, String>;

/// A String that matches part of a non-standard measurement key.
///
/// This will have exactly one '%n' and not start with a '$'. The
/// '%n' will be replaced by the measurement index which will be used
/// to match keywords.
#[derive(Clone, AsRef, Display)]
#[as_ref(str)]
pub struct NonStdMeasPattern(String);

/// A list of patterns that match standard or non-standard keys.
pub type KeyPatterns = KeyOrStringPatterns<()>;

/// A list of patterns that match standard or non-standard keys.
#[derive(Clone)]
pub struct KeyOrStringPatterns<T>(Vec<(KeyStringOrPattern, T)>);

impl<T> Default for KeyOrStringPatterns<T> {
    fn default() -> Self {
        Self(vec![])
    }
}

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

/// [`ParsedKeywords`] without the bad stuff
#[derive(Clone, Default, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(
    feature = "python",
    derive(FromPyObject, IntoPyObject),
    pyo3(from_item_all)
)]
pub struct ValidKeywords {
    #[cfg_attr(feature = "serde", serde(serialize_with = "serialize::ordered_map"))]
    pub std: StdKeywords,
    #[cfg_attr(feature = "serde", serde(serialize_with = "serialize::ordered_map"))]
    pub nonstd: NonStdKeywords,
}

/// A string that should be used as the header in the measurement table.
#[derive(Display)]
pub struct MeasHeader(pub String);

/// A regular expression which matches a non-standard measurement key.
///
/// This must be derived from [`NonStdMeasPattern`].
#[derive(AsRef)]
#[as_ref(Regex)]
pub(crate) struct NonStdMeasRegex(CaseInsRegex);

/// A regex which ignores case when matching
#[derive(Clone, AsRef)]
pub struct CaseInsRegex(Regex);

/// A "compiled" object to match keys efficiently.
pub(crate) struct KeyMatcher<'a, T> {
    literal: HashMap<&'a KeyString, T>,
    pattern: Vec<(&'a CaseInsRegex, T)>,
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

    fn std(i: impl Into<IndexFromOne>) -> StdKey {
        // reserve enough space for prefix, suffix, and a number with 3 digits
        let n = Self::PREFIX.len() + 3 + Self::SUFFIX.len();
        let mut s = String::with_capacity(n);
        s.push_str(Self::PREFIX);
        s.push_str(i.into().to_string().as_str());
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

    /// Build regexp matching "<PREFIX>n<SUFFIX>"
    fn regexp() -> CaseInsRegex {
        let mut s = String::new();
        s.push_str(Self::PREFIX);
        s.push_str("[0-9]+");
        s.push_str(Self::SUFFIX);
        // ASSUME this will never fail because pre/suffix should only be letters
        CaseInsRegex::from_str(s.as_str()).unwrap()
    }

    fn matches(other: &StdKey) -> bool {
        static RE: OnceLock<CaseInsRegex> = OnceLock::new();
        RE.get_or_init(|| Self::regexp()).0.is_match(other.as_ref())
    }
}

/// A standard key with two indices
///
/// The constant traits are assumed to only contain ASCII characters.
pub(crate) trait BiIndexedKey {
    const PREFIX: &'static str;
    const MIDDLE: &'static str;
    const SUFFIX: &'static str;

    fn std(i: impl Into<IndexFromOne>, j: impl Into<IndexFromOne>) -> StdKey {
        // reserve enough space for prefix, middle, suffix, and two numbers with
        // 2 digits
        let n = Self::PREFIX.len() + Self::MIDDLE.len() + Self::SUFFIX.len() + 4;
        let mut s = String::with_capacity(n);
        s.push_str(Self::PREFIX);
        s.push_str(i.into().to_string().as_str());
        s.push_str(Self::MIDDLE);
        s.push_str(j.into().to_string().as_str());
        s.push_str(Self::SUFFIX);
        StdKey::new(s)
    }

    /// Build regexp matching "<PREFIX>m<MIDDLE>n<SUFFIX>"
    fn regexp() -> CaseInsRegex {
        let mut s = String::new();
        s.push_str(Self::PREFIX);
        s.push_str("[0-9]+");
        s.push_str(Self::MIDDLE);
        s.push_str("[0-9]+");
        s.push_str(Self::SUFFIX);
        // ASSUME this will never fail because pre/suffix should only be letters
        CaseInsRegex::from_str(s.as_str()).unwrap()
    }

    fn matches(other: &StdKey) -> bool {
        static RE: OnceLock<CaseInsRegex> = OnceLock::new();
        RE.get_or_init(|| Self::regexp()).0.is_match(other.as_ref())
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
}

pub type NonStdKeywords = HashMap<NonStdKey, String>;

pub(crate) trait NonStdKeywordsExt {
    fn insert_demoted(&mut self, key: StdKey, value: String);

    fn transfer_demoted(&mut self, kws: &mut StdKeywords, key: StdKey) {
        if let Some(v) = kws.remove(&key) {
            self.insert_demoted(key, v);
        }
    }
}

impl NonStdKeywordsExt for HashMap<NonStdKey, String> {
    fn insert_demoted(&mut self, key: StdKey, value: String) {
        let mut k = NonStdKey(key.0);
        while self.contains_key(&k) {
            k.0.disambiguate();
        }
        let _ = self.insert(k, value);
    }
}

impl KeyString {
    fn new(s: String) -> Self {
        Self(Ascii::new(s))
    }

    fn disambiguate(&mut self) {
        self.0.push('_');
    }

    fn from_bytes_maybe(xs: &[u8], latin1: bool) -> Option<Self> {
        if latin1 {
            Some(Self::new(xs.iter().copied().map(char::from).collect()))
        } else if is_printable_ascii(xs) {
            Some(Self::from_bytes(xs))
        } else {
            None
        }
    }

    fn from_bytes(xs: &[u8]) -> Self {
        assert!(!xs.is_empty(), "cannot make KeyString with empty slice");
        Self::new(unsafe { String::from_utf8_unchecked(xs.to_vec()) })
    }
}

#[cfg(feature = "serde")]
impl Serialize for KeyString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.as_ref().serialize(serializer)
    }
}

impl TryFrom<HashMap<KeyString, KeyString>> for KeyStringPairs {
    type Error = KeyStringPairsError;

    fn try_from(value: HashMap<KeyString, KeyString>) -> Result<Self, Self::Error> {
        let mut names = vec![];
        for (k, v) in &value {
            if k == v {
                names.push(k.clone());
            }
        }
        if let Some(ns) = NonEmpty::from_vec(names) {
            Err(KeyStringPairsError(ns))
        } else {
            Ok(Self(value))
        }
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
        n: impl Into<IndexFromOne> + Clone,
    ) -> Result<NonStdMeasRegex, NonStdMeasRegexError> {
        self.0
            .replace("%n", n.clone().into().to_string().as_str())
            .as_str()
            .parse::<CaseInsRegex>()
            .map_err(|error| NonStdMeasRegexError::new(error, n))
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

impl<T> KeyOrStringPatterns<T> {
    pub fn extend(&mut self, other: Self) {
        self.0.extend(other.0);
    }

    #[cfg(feature = "python")]
    fn try_from_iter<F, E>(xs: impl IntoIterator<Item = (String, T)>, f: F) -> Result<Self, E>
    where
        F: Fn(&str) -> Result<KeyStringOrPattern, E>,
    {
        xs.into_iter()
            .collect::<HashMap<_, _>>()
            .into_iter()
            .map(|(k, v)| f(k.as_str()).map(|x| (x, v)))
            .collect::<Result<Vec<_>, _>>()
            .map(Self)
    }

    #[cfg(feature = "python")]
    pub(crate) fn try_from_literals(
        xs: impl IntoIterator<Item = (String, T)>,
    ) -> Result<Self, AsciiStringError> {
        Self::try_from_iter(xs, |k| {
            k.parse::<KeyString>().map(KeyStringOrPattern::Literal)
        })
    }

    #[cfg(feature = "python")]
    pub(crate) fn try_from_patterns(
        xs: impl IntoIterator<Item = (String, T)>,
    ) -> Result<Self, regex::Error> {
        Self::try_from_iter(xs, |k| {
            k.parse::<CaseInsRegex>().map(KeyStringOrPattern::Pattern)
        })
    }

    pub(crate) fn as_matcher(&self) -> KeyMatcher<'_, &T> {
        self.0.iter().map(|(k, v)| (k, v)).collect()
    }
}

impl KeyMatcher<'_, &()> {
    fn is_match(&self, other: &KeyString) -> bool {
        self.literal.contains_key(other)
            || self
                .pattern
                .iter()
                .any(|p| p.0.as_ref().is_match(other.as_ref()))
    }
}

impl<T> KeyMatcher<'_, T> {
    fn get(&self, other: &KeyString) -> Option<&T> {
        self.literal.get(other).or(self
            .pattern
            .iter()
            .find(|p| p.0.as_ref().is_match(other.as_ref()))
            .map(|x| &x.1))
    }
}

impl<'a, X> FromIterator<(&'a KeyStringOrPattern, X)> for KeyMatcher<'a, X> {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = (&'a KeyStringOrPattern, X)>,
    {
        let (literal, pattern): (HashMap<_, _>, Vec<_>) = iter
            .into_iter()
            .map(|(k, v)| match k {
                KeyStringOrPattern::Literal(l) => Ok((l, v)),
                KeyStringOrPattern::Pattern(p) => Err((p, v)),
            })
            .partition_result();
        Self { literal, pattern }
    }
}

impl ParsedKeywords {
    pub(crate) fn insert(
        &mut self,
        k: &[u8],
        v: &[u8],
        conf: &ReadHeaderAndTEXTConfig,
    ) -> Result<(), Leveled<KeywordInsertError>> {
        // ASSUME key and value are never blank since we checked both prior to
        // calling this. The FCS standards do not allow either to be blank.
        let to_std = conf.promote_to_standard.as_matcher();
        let to_nonstd = conf.demote_from_standard.as_matcher();
        // TODO this also should skip keys before throwing a blank error
        let ignore = conf.ignore_standard_keys.as_matcher();
        let subs = &conf.substitute_standard_key_values.as_matcher();
        let renames = &conf.rename_standard_keys.0;

        let blank_err = || {
            let w = BlankValueError(k.to_vec());
            Leveled::<KeywordInsertError>::new(w.into(), !conf.allow_empty)
        };

        let vv = if conf.use_latin1 {
            let it = v.iter().copied().map(char::from);
            if conf.trim_value_whitespace {
                let trimmed: String = it
                    .skip_while(char::is_ascii_whitespace)
                    .take_while(|x| !x.is_ascii_whitespace())
                    .collect();
                if trimmed.is_empty() {
                    return Err(blank_err());
                }
                Ok(trimmed)
            } else {
                Ok(it.collect())
            }
        } else {
            match str::from_utf8(v) {
                Ok(vv) => {
                    if conf.trim_value_whitespace {
                        let trimmed = vv.trim();
                        if trimmed.is_empty() {
                            return Err(blank_err());
                        }
                        Ok(trimmed.into())
                    } else {
                        Ok(vv.into())
                    }
                }
                Err(e) => Err(e),
            }
        };

        let parse_key = |s: &[u8]| {
            let (is_std, ss) = if let Some((&STD_PREFIX, sn)) = s.split_first()
                && !sn.is_empty()
            {
                (true, sn)
            } else {
                (false, s)
            };
            KeyString::from_bytes_maybe(ss, conf.use_latin1).map(|x| (is_std, x))
        };

        if let Ok(value) = vv {
            if let Some((is_std, kk)) = parse_key(k) {
                if is_std {
                    // Standard key: starts with '$', check that remaining chars
                    // are ASCII
                    if ignore.is_match(&kk) {
                        Ok(())
                    } else if to_nonstd.is_match(&kk) {
                        insert_nonunique(&mut self.nonstd, NonStdKey(kk), value, conf)
                    } else {
                        let rk = renames.get(&kk).cloned().unwrap_or(kk);
                        let rv = if let Some(s) = subs.get(&rk) {
                            s.sub(value.as_str())
                        } else {
                            value
                        };
                        insert_nonunique(&mut self.std, StdKey(rk), rv, conf)
                    }
                } else {
                    // Non-standard key: does not start with '$' but is still
                    // ASCII
                    if to_std.is_match(&kk) {
                        insert_nonunique(&mut self.std, StdKey(kk), value, conf)
                    } else {
                        insert_nonunique(&mut self.nonstd, NonStdKey(kk), value, conf)
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
        } else {
            self.byte_pairs.push((k.to_vec(), v.to_vec()));
            Ok(())
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

#[derive(Debug, Display, From, PartialEq, Error)]
pub enum KeywordInsertError {
    StdPresent(StdPresent),
    NonStdPresent(NonStdPresent),
    Blank(BlankValueError),
}

#[derive(Debug, PartialEq, Error)]
pub struct BlankValueError(pub Vec<u8>);

impl fmt::Display for BlankValueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let s = str::from_utf8(&self.0[..]).map_or_else(
            |_| format!("key's bytes were {}", self.0.iter().join(",")),
            |s| format!("key was {s}"),
        );
        write!(f, "skipping key with blank value, {s}")
    }
}

#[derive(Debug, PartialEq, Error)]
#[error("key '{key}' already present, has value '{value}'")]
pub struct KeyPresent<T> {
    pub key: T,
    pub value: String,
}

pub type StdPresent = KeyPresent<StdKey>;
pub type NonStdPresent = KeyPresent<NonStdKey>;

#[derive(PartialEq, Debug, Error)]
pub enum AsciiStringError {
    #[error("string should only have ASCII characters, found '{0}'")]
    Ascii(String),
    #[error("key string must not be empty")]
    Empty,
}

#[derive(From, PartialEq, Debug, Error)]
pub enum StdKeyError {
    #[error("{0}")]
    Ascii(AsciiStringError),
    #[error("standard key must start with '$', found '{0}'")]
    Prefix(KeyString),
    #[error("standard key must not be empty, got '$'")]
    Empty,
}

#[derive(From, PartialEq, Debug, Error)]
pub enum NonStdKeyError {
    #[error("{0}")]
    Ascii(AsciiStringError),
    #[error("non-standard key must not start with '$', found '{0}'")]
    Prefix(KeyString),
}

#[derive(Error, Debug)]
#[error(
    "non standard measurement pattern must not \
     start with '$' and should have one '%n', found '{0}'"
)]
pub struct NonStdMeasPatternError(String);

#[derive(Error, Debug, new)]
#[error("regexp error for measurement {index}: {error}")]
pub struct NonStdMeasRegexError {
    error: regex::Error,
    #[new(into)]
    index: IndexFromOne,
}

#[derive(Error, Debug)]
#[error("the following keys are paired with themselves: {}", .0.iter().join(","))]
pub struct KeyStringPairsError(NonEmpty<KeyString>);

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
    conf: &ReadHeaderAndTEXTConfig,
) -> Result<(), Leveled<KeywordInsertError>>
where
    K: Hash + Eq + Clone + AsRef<KeyString>,
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
                .map(ToString::to_string)
                .unwrap_or(value);
            e.insert(v);
            Ok(())
        }
    }
}

const STD_PREFIX: u8 = 36; // '$'

#[cfg(feature = "python")]
mod python {
    use super::{
        AsciiStringError, KeyPatterns, KeyString, KeyStringPairs, KeyStringPairsError, NonStdKey,
        NonStdKeyError, NonStdMeasPattern, NonStdMeasPatternError, StdKey, StdKeyError,
    };
    use crate::python::macros::{impl_from_py_via_fromstr, impl_to_py_via_display, impl_value_err};

    use pyo3::exceptions::PyValueError;
    use pyo3::prelude::*;
    use std::collections::HashMap;

    impl_from_py_via_fromstr!(NonStdMeasPattern);
    impl_value_err!(NonStdMeasPatternError);

    impl_from_py_via_fromstr!(StdKey);
    impl_to_py_via_display!(StdKey);
    impl_value_err!(StdKeyError);

    impl_from_py_via_fromstr!(NonStdKey);
    impl_to_py_via_display!(NonStdKey);
    impl_value_err!(NonStdKeyError);

    impl_from_py_via_fromstr!(KeyString);
    impl_to_py_via_display!(KeyString);
    impl_value_err!(AsciiStringError);

    // pass keypatterns via config as a tuple like ([String], [String]) where the
    // first member is literal strings and the second is regex patterns
    impl<'py> FromPyObject<'py> for KeyPatterns {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let (lits, pats): (Vec<String>, Vec<String>) = ob.extract()?;
            let mut ret = KeyPatterns::try_from_literals(lits.into_iter().map(|x| (x, ())))?;
            // this is just a regexp error
            let ps = KeyPatterns::try_from_patterns(pats.into_iter().map(|x| (x, ())))
                .map_err(|e| PyValueError::new_err(e.to_string()))?;
            ret.extend(ps);
            Ok(ret)
        }
    }

    impl<'py> FromPyObject<'py> for KeyStringPairs {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let xs: HashMap<KeyString, KeyString> = ob.extract()?;
            let ret = xs.try_into()?;
            Ok(ret)
        }
    }

    impl_value_err!(KeyStringPairsError);
}

#[cfg(feature = "serde")]
mod serialize {
    use serde::Serialize;
    use std::collections::{BTreeMap, HashMap};

    pub(crate) fn ordered_map<K: Serialize + Clone + Ord, S>(
        value: &HashMap<K, String>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let ordered: BTreeMap<K, _> = value.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        ordered.serialize(serializer)
    }
}

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
            &ReadHeaderAndTEXTConfig::default(),
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
            &ReadHeaderAndTEXTConfig::default(),
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

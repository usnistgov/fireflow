use crate::api::{FlatTEXTDiagnostics, HeaderAndSuppOffsets, SplitTEXTDiagnostics};
use crate::config::{
    AllowNonunique, ConfigFlag as _, DummyTriFlag, ReadHeaderAndTEXTConfig, TriErrorFlag as _,
    UseLatin1,
};
use crate::logging::{
    DeferredWarningsAndErrors, LogResult, SwitchableErrorResult, SwitchableErrorsResult,
    WarningOrErrorResult,
};
use crate::text::index::{IndexFromOne, MeasIndex};
use crate::text::keywords as kws;
use crate::text::optional::DisplayMaybe;
use crate::validated::case_ins_regex::CaseInsRegex;

use derive_more::{AsRef, Display, From};
use derive_new::new;
use fireflow_types::config::{PATTERN_DELIMITER, TemporalOpticalKey};
use itertools::Itertools as _;
use nonempty_collections::NESlice;
use std::borrow::Cow;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt;
use std::hash::Hash;
use std::marker::PhantomData;
use std::str::FromStr;
use std::string::ToString;
use std::sync::OnceLock;
use thiserror::Error;
use unicase::Ascii;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{
        AllIntoPyErr, DisplayAsPyErr, FromInnerPyObject, FromPyString, IntoPyString,
    },
    fireflow_types::python as py,
    pyo3::prelude::*,
};

/// A key from TEXT which is codified by the FCS standard.
///
/// These may only contain ASCII and must start with `"$"`. The `"$"` is not
/// actually stored but will be appended when converting to a [`String`].
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, AsRef, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
#[as_ref(KeyString, str)]
#[display("${_0}")]
pub struct StdKey(KeyString);

/// A key from TEXT which is not codified by the FCS standard.
///
/// This cannot start with `"$"` and may only contain ASCII characters.
#[derive(Clone, Debug, AsRef, Display, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
#[as_ref(KeyString, str)]
pub struct NonStdKey(KeyString);

/// The internal string for a key (standard or nonstandard).
///
/// Must be non-empty and contain only ASCII characters. Comparisons will be
/// case-insensitive.
#[derive(Clone, Debug, AsRef, Display, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
#[as_ref(str)]
pub struct KeyString(Ascii<String>);

/// A list of patterns that match [`StdKey`]s or [`NonStdKey`]s.
#[derive(Clone)]
pub struct KeyStringsOrPatterns<T>(pub HashMap<KeyStringOrPattern, T>);

impl<T> Default for KeyStringsOrPatterns<T> {
    fn default() -> Self {
        Self(HashMap::default())
    }
}

/// Either a literal string or regexp which matches a [`StdKey`]/[`NonStdKey`].
///
/// This exists for performance and ergononic reasons; if the goal is simply to
/// match lots of strings literally, it is faster and easier to use a hash
/// table, otherwise we need to search linearly through an array of patterns.
#[derive(Clone, PartialEq, Eq, Hash, Display)]
#[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
pub enum KeyStringOrPattern {
    Literal(KeyString),
    Pattern(CaseInsRegex),
}

impl FromStr for KeyStringOrPattern {
    type Err = KeyStringsOrPatternsError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(inner) = s
            .strip_prefix(PATTERN_DELIMITER)
            .and_then(|x| x.strip_suffix(PATTERN_DELIMITER))
        {
            let ret = inner.parse::<CaseInsRegex>().map_err(KeyRegexError)?;
            Ok(Self::Pattern(ret))
        } else {
            Ok(Self::Literal(s.parse::<KeyString>()?))
        }
    }
}

/// A collection of [`StdKey`]s and [`NonStdKey`]s and key/values with errors.
#[derive(Default)]
pub struct ParsedKeywords {
    /// Standard keywords (with '$')
    pub std: StdKeywords,

    /// Non-standard keywords (without '$')
    pub nonstd: NonStdKeywords,

    /// Keywords that failed for some reason.
    pub diag: ParsedKeywordsDiagnostic,
}

// TODO why pub?
#[derive(Default)]
pub struct ParsedKeywordsDiagnostic {
    /// Valid keys with non-UTF8 values.
    pub keys_with_non_utf8_values: Vec<(AnyKey, TruncatedBytes)>,

    /// Valid values with non-ASCII keys.
    pub values_with_non_ascii_keys: Vec<(TruncatedBytes, TruncatedString)>,

    /// Keywords that have invalid bytes in either key or value
    pub byte_pairs: Vec<(TruncatedBytes, TruncatedBytes)>,

    /// Standard keys which appear more than once with their values.
    pub non_unique_std_keywords: Vec<(StdKey, TruncatedString)>,

    /// Non-standard keys which appear more than once with their values.
    pub non_unique_nonstd_keywords: Vec<(NonStdKey, TruncatedString)>,

    /// Standard keys which were ignored
    pub ignored_std_keywords: Vec<(StdKey, StringOrBytes)>,

    /// Keys with empty values.
    ///
    /// The only way this can happen at this stage is if the value is entirely
    /// whitespace and is trimmed.
    pub keys_with_empty_trimmed_values: Vec<KeyOrBytes>,

    /// Keys with values that were trimmed
    ///
    /// The value included here is the original value.
    pub keys_with_trimmed_values: Vec<(KeyOrBytes, StringOrBytes)>,
}

/// Either a standard or non-standard key.
#[derive(Clone, Display, PartialEq, Debug, From)]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromPyObject))]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum AnyKey {
    Std(StdKey),
    NonStd(NonStdKey),
}

pub type StdKeywords = HashMap<StdKey, String>;

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

/// A "compiled" object to match keys efficiently.
pub(crate) struct KeyMatcher<'a, T> {
    literal: HashMap<&'a KeyString, T>,
    pattern: Vec<(&'a CaseInsRegex, T)>,
}

/// A either an ASCII key value or a non-ASCII byte sequence.
#[derive(Clone, Display, PartialEq, Debug, From)]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromPyObject))]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum KeyOrBytes {
    Ascii(AnyKey),
    Bytes(TruncatedBytes),
}

/// A either a UTF-8 string or a non-UTF-8 byte sequence.
#[derive(Clone, Display, PartialEq, Debug, From)]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromPyObject))]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum StringOrBytes {
    Utf8(TruncatedString),
    Bytes(TruncatedBytes),
}

impl Default for StringOrBytes {
    fn default() -> Self {
        Self::Utf8(TruncatedString::default())
    }
}

impl From<Vec<u8>> for StringOrBytes {
    fn from(value: Vec<u8>) -> Self {
        match String::from_utf8(value) {
            Ok(s) => Self::Utf8(TruncatedString(s)),
            Err(e) => Self::Bytes(TruncatedBytes(e.into_bytes())),
        }
    }
}

/// A [`StdKey`] without an index
///
/// The constant traits is validated to only contain ASCII characters.
// TODO const_trait_impl will be able to clean this up once stable
pub trait Key {
    const C: &'static str;

    const _CHECK: () = {
        assert!(is_alpha_underscore_str(Self::C), "C must only be letters");
    };

    #[must_use]
    #[allow(path_statements)]
    fn std() -> StdKey {
        Self::_CHECK;
        StdKey::new(Self::C.into())
    }

    fn self_std(&self) -> StdKey {
        Self::std()
    }

    #[must_use]
    fn len() -> u64 {
        u64::try_from(Self::C.len() + 1).unwrap()
    }
}

/// A [`StdKey`] with one index
///
/// The constant traits are validated to only contain ASCII characters.
pub trait IndexedKey {
    const PREFIX: &'static str;
    const SUFFIX: &'static str;

    const _CHECK_PREFIX: () = {
        assert!(
            is_alpha_underscore_str(Self::PREFIX),
            "PREFIX must only be letters"
        );
    };

    const _CHECK_SUFFIX: () = {
        assert!(
            is_alpha_underscore_str(Self::SUFFIX),
            "SUFFIX must only be letters"
        );
    };

    #[allow(path_statements)]
    fn std(i: impl Into<IndexFromOne>) -> StdKey {
        // reserve enough space for prefix, suffix, and a number with 3 digits
        let n = Self::PREFIX.len() + 3 + Self::SUFFIX.len();
        let mut s = String::with_capacity(n);
        s.push_str(Self::PREFIX);
        s.push_str(i.into().to_string().as_str());
        s.push_str(Self::SUFFIX);
        // trigger compile time error if pre/suffix are anything but letters/underscore
        Self::_CHECK_PREFIX;
        Self::_CHECK_SUFFIX;
        StdKey::new(s)
    }

    fn self_std(&self, i: impl Into<IndexFromOne>) -> StdKey {
        Self::std(i)
    }

    #[must_use]
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

    #[must_use]
    fn self_std_blank(&self) -> String {
        Self::std_blank()
    }

    /// Build regexp matching `"<PREFIX>n<SUFFIX>"`
    #[must_use]
    fn regexp() -> CaseInsRegex {
        let mut s = String::new();
        s.push_str(Self::PREFIX);
        s.push_str("[1-9][0-9]*");
        s.push_str(Self::SUFFIX);
        // ASSUME this will never fail because pre/suffix should only be letters
        CaseInsRegex::from_str(s.as_str()).unwrap()
    }

    fn matches(other: &StdKey) -> bool {
        static RE: OnceLock<CaseInsRegex> = OnceLock::new();
        RE.get_or_init(|| Self::regexp())
            .as_ref()
            .is_match(other.as_ref())
    }
}

/// A [`StdKey`] with two indices
///
/// The constant traits are validated to only contain ASCII characters.
pub trait BiIndexedKey {
    const PREFIX: &'static str;
    const MIDDLE: &'static str;
    const SUFFIX: &'static str;

    const _CHECK_PREFIX: () = {
        assert!(
            is_alpha_underscore_str(Self::PREFIX),
            "PREFIX must only be letters"
        );
    };

    const _CHECK_MIDDLE: () = {
        assert!(
            is_alpha_underscore_str(Self::MIDDLE),
            "MIDDLE must only be letters"
        );
    };

    const _CHECK_SUFFIX: () = {
        assert!(
            is_alpha_underscore_str(Self::SUFFIX),
            "SUFFIX must only be letters"
        );
    };

    #[allow(path_statements)]
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
        // trigger compile time error if pre/mid/suffix are anything but letters/underscore
        Self::_CHECK_PREFIX;
        Self::_CHECK_MIDDLE;
        Self::_CHECK_SUFFIX;
        StdKey::new(s)
    }

    /// Build regexp matching `"<PREFIX>m<MIDDLE>n<SUFFIX>"`
    #[must_use]
    fn regexp() -> CaseInsRegex {
        let mut s = String::new();
        s.push_str(Self::PREFIX);
        s.push_str("([1-9][0-9]*)");
        s.push_str(Self::MIDDLE);
        s.push_str("([1-9][0-9]*)");
        s.push_str(Self::SUFFIX);
        // ASSUME this will never fail because pre/suffix should only be letters
        CaseInsRegex::from_str(s.as_str()).unwrap()
    }

    fn matches(other: &StdKey) -> Option<(usize, usize)> {
        static RE: OnceLock<CaseInsRegex> = OnceLock::new();
        let c = RE
            .get_or_init(|| Self::regexp())
            .as_ref()
            .captures(other.as_ref())?;
        let (_, [m, n]) = c.extract();
        // ASSUME these won't fail because we match only digits
        Some((m.parse::<usize>().unwrap(), n.parse::<usize>().unwrap()))
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

pub(crate) trait AnyStdKey {
    fn as_std(&self) -> StdKey;
}

impl<T: Key> AnyStdKey for Key0<T> {
    fn as_std(&self) -> StdKey {
        T::std()
    }
}

impl<T: IndexedKey> AnyStdKey for Key1<T> {
    fn as_std(&self) -> StdKey {
        T::std(self.index)
    }
}

impl<T: BiIndexedKey> AnyStdKey for Key2<T> {
    fn as_std(&self) -> StdKey {
        let i = &self.index;
        T::std(i.i0, i.i1)
    }
}

/// A type representing a [`StdKey`].
///
/// This is useful because the value of the key is not actually stored, so this
/// is very fast and memory-efficient. If we stored the value itself, it would
/// be a [`String`] internally and allocated on the heap. We can get away with
/// this because the value of each [`StdKey`] is entirely encoded by the
/// [`Key`], [`IndexedKey`], and [`BiIndexedKey`] traits (with in index in the
/// latter two cases).
#[derive(Debug, new)]
pub struct SpecificKey<T, I> {
    index: I,
    _key: PhantomData<T>,
}

impl<T, I: Clone> Clone for SpecificKey<T, I> {
    fn clone(&self) -> Self {
        Self::new(self.index.clone())
    }
}

impl<T, I: Copy> Copy for SpecificKey<T, I> {}

pub type Key0<T> = SpecificKey<T, ()>;
pub type Key1<T> = SpecificKey<T, IndexFromOne>;
pub type Key2<T> = SpecificKey<T, BiIndex>;

impl<T> Default for Key0<T> {
    fn default() -> Self {
        Self::new(())
    }
}

impl<T> Key1<T> {
    pub(crate) fn new_i1(i: IndexFromOne) -> Self {
        Self::new(i)
    }
}

impl<T> Key2<T> {
    pub(crate) fn new_i2(i: IndexFromOne, j: IndexFromOne) -> Self {
        Self::new(BiIndex::new(i, j))
    }
}

/// Composite index for [`StdKey`] with two index values
#[derive(Debug, new)]
pub struct BiIndex {
    pub i0: IndexFromOne,
    pub i1: IndexFromOne,
}

impl<T: Key> fmt::Display for SpecificKey<T, ()> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", T::std())
    }
}

impl<T: IndexedKey> fmt::Display for SpecificKey<T, IndexFromOne> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", T::std(self.index))
    }
}

impl<T: BiIndexedKey> fmt::Display for SpecificKey<T, BiIndex> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let i = &self.index;
        write!(f, "{}", T::std(i.i0, i.i1))
    }
}

pub type NonStdKeywords = HashMap<NonStdKey, String>;

pub(crate) trait NonStdKeywordsExt {
    fn insert_demoted(&mut self, key: StdKey, value: String);

    fn insert_demoted_metaroot<T: Key + fmt::Display>(&mut self, value: &T) {
        self.insert_demoted(T::std(), value.to_string());
    }

    fn insert_demoted_metaroot_opt<T: Key + fmt::Display>(&mut self, value: Option<&T>) {
        if let Some(v) = value {
            self.insert_demoted(T::std(), v.to_string());
        }
    }

    fn insert_demoted_metaroot_maybe<T: Key + DisplayMaybe>(&mut self, value: &T) {
        if let Some(v) = value.display_maybe() {
            self.insert_demoted(T::std(), v);
        }
    }

    fn insert_demoted_meas<T: IndexedKey + fmt::Display>(&mut self, i: IndexFromOne, value: &T) {
        self.insert_demoted(T::std(i), value.to_string());
    }

    fn insert_demoted_meas_opt<T: IndexedKey + fmt::Display>(
        &mut self,
        i: IndexFromOne,
        value: Option<&T>,
    ) {
        if let Some(v) = value {
            self.insert_demoted_meas(i, v);
        }
    }

    fn insert_demoted_meas_maybe<T: IndexedKey + DisplayMaybe>(
        &mut self,
        i: IndexFromOne,
        value: &T,
    ) {
        if let Some(v) = value.display_maybe() {
            self.insert_demoted(T::std(i), v);
        }
    }

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

    fn from_bytes_maybe(xs: &NESlice<u8>, latin1: UseLatin1) -> Option<Self> {
        if latin1.is_set() {
            Some(Self::new(xs.iter().copied().map(char::from).collect()))
        } else if is_printable_ascii(xs.as_ref()) {
            // SAFETY: we just checked that the bytes are only ASCII chars
            Some(unsafe { Self::from_bytes(xs) })
        } else {
            None
        }
    }

    unsafe fn from_bytes(xs: &NESlice<u8>) -> Self {
        // SAFETY: this function is marked unsafe since the caller must check
        Self::new(unsafe { String::from_utf8_unchecked(xs.as_ref().to_vec()) })
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

impl StdKey {
    pub(crate) fn as_ascii_str(&self) -> Ascii<&str> {
        Ascii::new(self.0.0.as_ref())
    }

    fn new(s: String) -> Self {
        Self(KeyString::new(s))
    }

    pub(crate) fn from_temporal_optical_key(x: TemporalOpticalKey, i: MeasIndex) -> Self {
        match x {
            TemporalOpticalKey::Gain => kws::Gain::std(i),
            TemporalOpticalKey::Filter => kws::Filter::std(i),
            // NOTE this is $PnL for all versions
            TemporalOpticalKey::Wavelength => kws::Wavelength::std(i),
            TemporalOpticalKey::Power => kws::Power::std(i),
            TemporalOpticalKey::DetectorType => kws::DetectorType::std(i),
            TemporalOpticalKey::DetectorVoltage => kws::DetectorVoltage::std(i),
            TemporalOpticalKey::PercentEmitted => kws::PercentEmitted::std(i),
            // NOTE this is $PnCALIBRATION for all versions
            TemporalOpticalKey::Calibration => kws::Calibration3_1::std(i),
            TemporalOpticalKey::DetectorName => kws::DetectorName::std(i),
            TemporalOpticalKey::Tag => kws::Tag::std(i),
            TemporalOpticalKey::Feature => kws::Feature::std(i),
            TemporalOpticalKey::Analyte => kws::Analyte::std(i),
        }
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
            Err(AsciiStringError::Ascii(s.into()))
        } else {
            Ok(Self(Ascii::new(s.into())))
        }
    }
}

impl FromStr for StdKey {
    type Err = StdKeyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let ks = s.parse::<KeyString>().map_err(StdKeyError::Ascii)?;
        // ASSUME this will not fail because we know the string is
        // non-empty
        let (y, ys) = ks.as_ref().as_bytes().split_first().unwrap();
        if *y != STD_PREFIX {
            Err(StdKeyError::Prefix(ks))
        } else if let Some(zs) = NESlice::try_from_slice(ys) {
            // SAFETY: this will not fail because we know the string has only
            // ASCII bytes and we checked that the slice is non-empty
            Ok(Self(unsafe { KeyString::from_bytes(&zs) }))
        } else {
            Err(StdKeyError::Empty)
        }
    }
}

impl FromStr for NonStdKey {
    type Err = NonStdKeyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let ks = s.parse::<KeyString>().map_err(NonStdKeyError::Ascii)?;
        if has_no_std_prefix(ks.as_ref().as_bytes()) {
            Ok(Self::new(ks.to_string()))
        } else {
            Err(NonStdKeyError::Prefix(ks))
        }
    }
}

impl<T> FromIterator<(KeyStringOrPattern, T)> for KeyStringsOrPatterns<T> {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = (KeyStringOrPattern, T)>,
    {
        Self(iter.into_iter().collect())
    }
}

impl FromIterator<KeyStringOrPattern> for KeyStringsOrPatterns<()> {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = KeyStringOrPattern>,
    {
        Self(iter.into_iter().map(|x| (x, ())).collect())
    }
}

impl<T> KeyStringsOrPatterns<T> {
    pub(crate) fn as_matcher(&self) -> KeyMatcher<'_, &T> {
        self.0.iter().collect()
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

#[allow(clippy::too_many_lines)]
impl ParsedKeywords {
    pub(crate) fn insert(
        &mut self,
        key: &NESlice<u8>,
        val: &NESlice<u8>,
        conf: &ReadHeaderAndTEXTConfig,
    ) -> WarningOrErrorResult<(), (), KeywordInsertError, KeywordInsertError> {
        enum TrimResult {
            Trimmed(bool),
            Empty(DummyTriFlag),
        }

        enum KeyValueResult<'a> {
            Ignore(StdKey, StringOrBytes),
            Empty(KeyOrBytes, DummyTriFlag),
            NonEmpty(AnyKey, Cow<'a, str>, bool),
            NonUtf8Value(AnyKey, TruncatedBytes),
            NonAsciiKey(TruncatedBytes, TruncatedString, bool),
            BothInvalid(TruncatedBytes, TruncatedBytes),
        }

        let to_std = conf.promote_to_standard.as_matcher();
        let to_nonstd = conf.demote_from_standard.as_matcher();
        let ignore = conf.ignore_standard_keys.as_matcher();
        let subs = &conf.substitute_standard_key_values.as_matcher();
        let renames = &conf.rename_standard_keys.as_ref();

        let parse_key = |s: &NESlice<u8>| {
            if let Some((&STD_PREFIX, rest)) = s.as_ref().split_first()
                && let Some(sn) = NESlice::try_from_slice(rest)
            {
                Some((true, KeyString::from_bytes_maybe(&sn, conf.use_latin1)?))
            } else {
                Some((false, KeyString::from_bytes_maybe(s, conf.use_latin1)?))
            }
        };

        // TODO return nonempty string here after checking that it isn't empty
        let check_trim = |trimmed: &str, flag| {
            if trimmed.is_empty() {
                TrimResult::Empty(flag)
            } else {
                TrimResult::Trimmed(val.len().get() < trimmed.len())
            }
        };

        let parse_value = || {
            let flag = conf.trim_value_whitespace;
            let triflag = DummyTriFlag::from_trim_value_whitespace(flag);
            if conf.use_latin1.is_set() {
                let it = val.iter().copied().map(char::from);
                if let Some(tf) = triflag {
                    let trimmed: String = it
                        .skip_while(char::is_ascii_whitespace)
                        .take_while(|x| !x.is_ascii_whitespace())
                        .collect();
                    let tres = check_trim(trimmed.as_str(), tf);
                    Some((Cow::Owned(trimmed), tres))
                } else {
                    Some((Cow::Owned(it.collect()), TrimResult::Trimmed(false)))
                }
            } else if let Ok(vv) = str::from_utf8(val.as_ref()) {
                if let Some(tf) = triflag {
                    let trimmed = vv.trim();
                    let tres = check_trim(trimmed, tf);
                    Some((Cow::Borrowed(trimmed), tres))
                } else {
                    Some((Cow::Borrowed(vv), TrimResult::Trimmed(false)))
                }
            } else {
                None
            }
        };

        let kv_res = if let Some((is_std, kstr)) = parse_key(key) {
            if is_std {
                // Standard key: starts with '$' and is ASCII
                if ignore.is_match(&kstr) {
                    KeyValueResult::Ignore(StdKey(kstr), StringOrBytes::from(val.as_ref().to_vec()))
                } else {
                    let ak = AnyKey::Std(StdKey(kstr));
                    if let Some((value, trim_res)) = parse_value() {
                        match trim_res {
                            TrimResult::Empty(flag) => KeyValueResult::Empty(ak.into(), flag),
                            TrimResult::Trimmed(was_trimmed) => {
                                KeyValueResult::NonEmpty(ak, value, was_trimmed)
                            }
                        }
                    } else {
                        KeyValueResult::NonUtf8Value(ak, TruncatedBytes(val.as_ref().to_vec()))
                    }
                }
            } else {
                // Non-standard key: does not start with '$' and is ASCII
                let ak = AnyKey::NonStd(NonStdKey(kstr));
                if let Some((value, trim_res)) = parse_value() {
                    match trim_res {
                        TrimResult::Empty(flag) => KeyValueResult::Empty(ak.into(), flag),
                        TrimResult::Trimmed(was_trimmed) => {
                            KeyValueResult::NonEmpty(ak, value, was_trimmed)
                        }
                    }
                } else {
                    KeyValueResult::NonUtf8Value(ak, TruncatedBytes(val.as_ref().to_vec()))
                }
            }
        } else {
            // Non-ascii key with possibly non-Utf-8 value
            let kbytes = TruncatedBytes(key.as_ref().to_vec());
            if let Some((value, trim_res)) = parse_value() {
                match trim_res {
                    TrimResult::Empty(flag) => {
                        KeyValueResult::Empty(KeyOrBytes::from(kbytes), flag)
                    }
                    TrimResult::Trimmed(was_trimmed) => {
                        let tv = value.into_owned().into();
                        KeyValueResult::NonAsciiKey(kbytes, tv, was_trimmed)
                    }
                }
            } else {
                KeyValueResult::BothInvalid(kbytes, val.as_ref().to_vec().into())
            }
        };

        match kv_res {
            KeyValueResult::NonEmpty(k, v, was_trimmed) => {
                if was_trimmed {
                    let vo = StringOrBytes::from(TruncatedString(v.as_ref().to_owned()));
                    let pair = (k.clone().into(), vo);
                    self.diag.keys_with_trimmed_values.push(pair);
                }
                match k {
                    AnyKey::Std(StdKey(kstr)) => {
                        if to_nonstd.is_match(&kstr) {
                            let vo = v.into_owned();
                            self.insert_nonunique_nonstd(kstr, vo, conf)
                        } else {
                            let rk = renames.get(&kstr).cloned().unwrap_or(kstr);
                            let rv = if let Some(s) = subs.get(&rk) {
                                s.sub(v.as_ref())
                            } else {
                                v.into_owned()
                            };
                            self.insert_nonunique_std(rk, rv, conf)
                        }
                    }
                    AnyKey::NonStd(NonStdKey(kstr)) => {
                        let vo = v.into_owned();
                        if to_std.is_match(&kstr) {
                            self.insert_nonunique_std(kstr, vo, conf)
                        } else {
                            self.insert_nonunique_nonstd(kstr, vo, conf)
                        }
                    }
                }
            }
            KeyValueResult::Empty(k, flag) => {
                self.diag.keys_with_empty_trimmed_values.push(k.clone());
                let e = KeywordInsertError::from(BlankValueError(k));
                SwitchableErrorResult::new_switchable3((), (), e, flag)
                    .switchable_into_non_commutative()
            }
            KeyValueResult::NonAsciiKey(k, v, was_trimmed) => {
                if was_trimmed {
                    let vo = StringOrBytes::from(v.clone());
                    let pair = (k.clone().into(), vo);
                    self.diag.keys_with_trimmed_values.push(pair);
                }
                self.diag.values_with_non_ascii_keys.push((k, v));
                LogResult::new_ok(())
            }
            KeyValueResult::NonUtf8Value(k, v) => {
                self.diag.keys_with_non_utf8_values.push((k, v));
                LogResult::new_ok(())
            }
            KeyValueResult::BothInvalid(k, v) => {
                self.diag.byte_pairs.push((k, v));
                LogResult::new_ok(())
            }
            KeyValueResult::Ignore(k, v) => {
                self.diag.ignored_std_keywords.push((k, v));
                LogResult::new_ok(())
            }
        }
    }

    fn insert_nonunique_std(
        &mut self,
        k: KeyString,
        value: String,
        conf: &ReadHeaderAndTEXTConfig,
    ) -> WarningOrErrorResult<(), (), KeywordInsertError, KeywordInsertError> {
        Self::insert_nonunique(
            &mut self.std,
            &mut self.diag.non_unique_std_keywords,
            StdKey(k),
            value,
            conf,
        )
    }

    fn insert_nonunique_nonstd(
        &mut self,
        k: KeyString,
        value: String,
        conf: &ReadHeaderAndTEXTConfig,
    ) -> WarningOrErrorResult<(), (), KeywordInsertError, KeywordInsertError> {
        Self::insert_nonunique(
            &mut self.nonstd,
            &mut self.diag.non_unique_nonstd_keywords,
            NonStdKey(k),
            value,
            conf,
        )
    }

    fn insert_nonunique<K>(
        kws: &mut HashMap<K, String>,
        nonunique: &mut Vec<(K, TruncatedString)>,
        k: K,
        value: String,
        conf: &ReadHeaderAndTEXTConfig,
    ) -> WarningOrErrorResult<(), (), KeywordInsertError, KeywordInsertError>
    where
        K: Hash + Eq + Clone + AsRef<KeyString>,
        KeywordInsertError: From<KeyPresent<K>>,
    {
        let flag = conf.allow_nonunique;
        match kws.entry(k) {
            Entry::Occupied(ent) => {
                let key = ent.key().clone();
                let err = KeyPresent {
                    key: key.clone(),
                    value: value.clone(),
                };
                nonunique.push((key, TruncatedString(value)));
                LogResult::new_deferred_switchable3((), err.into(), flag)
                    .switchable_into_non_commutative()
            }
            Entry::Vacant(ent) => {
                let v = conf
                    .replace_standard_key_values
                    .get(ent.key().as_ref())
                    .map(ToString::to_string)
                    .unwrap_or(value);
                ent.insert(v);
                LogResult::new_ok(())
            }
        }
    }

    pub(crate) fn append_std(
        &mut self,
        new: &HashMap<KeyString, String>,
        flag: AllowNonunique,
    ) -> SwitchableErrorsResult<(), (), AllowNonunique, StdPresent> {
        let es = new
            .iter()
            .filter_map(|(k, v)| match self.std.entry(StdKey(k.clone())) {
                Entry::Occupied(e) => {
                    self.diag
                        .non_unique_std_keywords
                        .push((StdKey(k.clone()), TruncatedString(v.clone())));
                    Some(KeyPresent::new(e.key().clone(), v.clone()))
                }
                Entry::Vacant(e) => {
                    e.insert(v.clone());
                    None
                }
            });
        LogResult::new_switchable_iter3((), (), es, flag)
    }
}

impl ParsedKeywordsDiagnostic {
    pub(crate) fn into_flat_diag(
        self,
        header_supp: HeaderAndSuppOffsets,
        primary_split: SplitTEXTDiagnostics,
        supp_split: Option<SplitTEXTDiagnostics>,
        conf: &ReadHeaderAndTEXTConfig,
    ) -> DeferredWarningsAndErrors<
        FlatTEXTDiagnostics,
        InvalidKeywordCharsError,
        InvalidKeywordCharsError,
    > {
        // Throw errors or warnings for any keys or values that have invalid
        // chars. There are two flags for keys and values respectively. For any
        // pairs that have both an invalid key and invalid value, throw error if
        // either flag is set (likewise for warning).
        macro_rules! go_err {
            ($field:ident, $err:ident) => {
                self.$field
                    .iter()
                    .cloned()
                    .map(|(key, value)| $err { key, value })
                    .map(InvalidKeywordCharsError::from)
            };
        }

        let es_key = go_err!(values_with_non_ascii_keys, NonAsciiKeyError);
        let es_value = go_err!(keys_with_non_utf8_values, NonUtf8ValueError);
        let es_both = go_err!(byte_pairs, NonAsciiOrUtf8KeywordError);

        let key_flag = conf.allow_non_ascii_keys.is_error();
        let val_flag = conf.allow_non_utf8_values.is_error();

        let mut es = vec![];
        let mut ws = vec![];

        match key_flag {
            Some(true) => es.extend(es_key),
            Some(false) => ws.extend(es_key),
            None => (),
        }
        match val_flag {
            Some(true) => es.extend(es_value),
            Some(false) => ws.extend(es_value),
            None => (),
        }
        match key_flag.zip(val_flag).map(|(x, y)| x || y) {
            Some(true) => es.extend(es_both),
            Some(false) => ws.extend(es_both),
            None => (),
        }

        // Combine all keys/values with invalid chars into one list, since
        // use probably doesn't want to see three.

        macro_rules! go_byte_pairs {
            ($field:ident) => {
                self.$field
                    .into_iter()
                    .map(|(k, v)| (KeyOrBytes::from(k), StringOrBytes::from(v)))
            };
        }

        let ks = go_byte_pairs!(values_with_non_ascii_keys);
        let vs = go_byte_pairs!(keys_with_non_utf8_values);
        let bs = go_byte_pairs!(byte_pairs);

        let byte_pairs: Vec<_> = ks.chain(vs).chain(bs).collect();

        let ret = FlatTEXTDiagnostics {
            header_supp,
            byte_pairs,
            non_unique_std_keywords: self.non_unique_std_keywords,
            non_unique_nonstd_keywords: self.non_unique_nonstd_keywords,
            ignored_standard_keywords: self.ignored_std_keywords,
            keys_with_empty_trimmed_values: self.keys_with_empty_trimmed_values,
            keys_with_trimmed_values: self.keys_with_trimmed_values,
            primary_split,
            supp_split,
        };
        LogResult::new_ok(ret)
            .extend_deferred_errors(es)
            .set_commutative_warnings(ws)
    }
}

/// Error when parsing [`StdKey`] from string
#[derive(From, PartialEq, Debug, Error)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeyError))]
pub enum StdKeyError {
    #[error("{0}")]
    Ascii(AsciiStringError),
    #[error("standard key must start with '$', found '{0}'")]
    Prefix(KeyString),
    #[error("standard key must not be empty, got '$'")]
    Empty,
}

/// Error when parsing [`NonStdKey`] from string
#[derive(From, PartialEq, Debug, Error)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeyError))]
pub enum NonStdKeyError {
    #[error("{0}")]
    Ascii(AsciiStringError),
    #[error("non-standard key must not start with '$', found '{0}'")]
    Prefix(KeyString),
}

/// Error when parsing [`KeyString`] from string
#[derive(PartialEq, Debug, Error)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeyError))]
pub enum AsciiStringError {
    #[error("string should only have ASCII characters, found '{0}'")]
    Ascii(String),
    #[error("key string must not be empty")]
    Empty,
}

/// Error when parsing literal keys or pattern strings when building [`KeyStringsOrPatterns`]
#[derive(Debug, Display, From, PartialEq, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum KeyStringsOrPatternsError {
    Regexp(KeyRegexError),
    Ascii(AsciiStringError),
}

/// Error when parsing [`CaseInsRegex`] from string when building [`KeyStringsOrPatterns`]
#[derive(Debug, Display, From, PartialEq, Error)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConfigError))]
pub struct KeyRegexError(regex::Error);

/// Error when parsed keyword cannot be inserted into [`ParsedKeywords`]
#[derive(Debug, Display, From, PartialEq, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum KeywordInsertError {
    StdPresent(StdPresent),
    NonStdPresent(NonStdPresent),
    Blank(BlankValueError),
}

/// Error when key has blank value
#[derive(Debug, PartialEq, Error)]
#[error("skipping key {0} with blank value")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeyError))]
pub struct BlankValueError(pub KeyOrBytes);

/// Error when key is already present in hash table.
#[derive(Debug, PartialEq, Error, new)]
#[error("key '{key}' already present, has value '{value}'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeyError))]
#[cfg_attr(feature = "python", bound(T: fmt::Display))]
pub struct KeyPresent<T> {
    pub key: T,
    pub value: String,
}

/// Error when keyword has any invalid chars.
#[derive(Debug, Display, From, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum InvalidKeywordCharsError {
    Key(NonAsciiKeyError),
    Value(NonUtf8ValueError),
    Both(NonAsciiOrUtf8KeywordError),
}

/// Error when key or value with invalid UTF-8 characters is encountered
#[derive(Debug, Error)]
#[error("non ASCII key {key} and non UTF-8 value {value} encountered")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeyError))]
pub struct NonAsciiOrUtf8KeywordError {
    key: TruncatedBytes,
    value: TruncatedBytes,
}

/// Error when key is not ASCII
#[derive(Debug, Error)]
#[error("non ASCII key encountered with bytes {key} and value '{value}'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeyError))]
pub struct NonAsciiKeyError {
    key: TruncatedBytes,
    value: TruncatedString,
}

/// Error when value is not Utf8
#[derive(Debug, Error)]
#[error("non UTF-8 key encountered with bytes {value} and key '{key}'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeyError))]
pub struct NonUtf8ValueError {
    key: AnyKey,
    value: TruncatedBytes,
}

// TODO use NEVec for this
/// A [`Vec<u8>`] optimized for displaying in errors.
#[derive(Clone, From, PartialEq, Debug)]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct TruncatedBytes(pub Vec<u8>);

impl fmt::Display for TruncatedBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let mut s = String::new();
        for (i, &x) in self.0.iter().take(TRUNCATED_BYTES_LIMIT).enumerate() {
            // Display all 'easy' control characters with escaped
            // representation, display all printable chars as quoted characters,
            // and display the rest as plain numbers
            match x {
                0 => s.push_str("\\0"),
                7 => s.push_str("\\a"),
                8 => s.push_str("\\b"),
                9 => s.push_str("\\t"),
                10 => s.push_str("\\n"),
                11 => s.push_str("\\v"),
                12 => s.push_str("\\f"),
                13 => s.push_str("\\r"),
                27 => s.push_str("\\e"),
                c => {
                    if (32..=127).contains(&c) {
                        s.push('\'');
                        s.push(char::from(c));
                        s.push('\'');
                    } else {
                        let n = c.to_string();
                        s.push_str(n.as_str());
                    }
                }
            }
            if i + 1 < TRUNCATED_BYTES_LIMIT {
                s.push(',');
            }
        }
        if self.0.len() > TRUNCATED_BYTES_LIMIT {
            write!(f, "[{s},...]")
        } else {
            write!(f, "[{s}]")
        }
    }
}

// TODO make this a nonempty string
/// A normal [`String`] that will be shortened when displaying if too long.
#[derive(Clone, From, PartialEq, Debug, Default)]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct TruncatedString(pub String);

impl fmt::Display for TruncatedString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let n = self.0.chars().count();
        if n > TRUNCATED_STR_LIMIT {
            let s: String = self.0.chars().take(n).collect();
            write!(f, "{s}…(more)")
        } else {
            write!(f, "{}", self.0)
        }
    }
}

const TRUNCATED_BYTES_LIMIT: usize = 20;
const TRUNCATED_STR_LIMIT: usize = 20;

pub type StdPresent = KeyPresent<StdKey>;
pub type NonStdPresent = KeyPresent<NonStdKey>;

fn is_printable_ascii(xs: &[u8]) -> bool {
    xs.iter().all(|x| 32 <= *x && *x <= 126)
}

fn has_no_std_prefix(xs: &[u8]) -> bool {
    xs.first().is_some_and(|x| *x != STD_PREFIX)
}

const fn is_alpha_underscore_str(s: &str) -> bool {
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        let upper = c >= b'A' && c <= b'Z';
        let lower = c >= b'a' && c <= b'z';
        let underscore = c == b'_';
        if !(upper || lower || underscore) {
            return false;
        }
        i += 1;
    }
    true
}

const STD_PREFIX: u8 = 36; // '$'

#[cfg(feature = "serde")]
mod serialize {
    use serde::Serialize;
    use std::collections::{BTreeMap, HashMap};

    pub fn ordered_map<K: Serialize + Clone + Ord, S>(
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
    use nonempty_collections::NESlice;

    #[test]
    fn fromstr_std_key() {
        let s = "$MAJESTY";
        let k = s.parse::<StdKey>().unwrap();
        assert_eq!(StdKey(KeyString(Ascii::new("MAJESTY".into()))), k);
        // reverse process should give back original string
        assert_eq!(k.to_string(), s.to_owned());
        // and such a valid key should behave the same when inserted into
        // the hash table
        let mut p = ParsedKeywords::default();
        let res = p.insert(
            &NESlice::try_from_slice(s.as_bytes()).unwrap(),
            &NESlice::try_from_slice(b"of_the_night_sky").unwrap(),
            &ReadHeaderAndTEXTConfig::default(),
        );
        assert_eq!(LogResult::new_ok(()), res);
        assert_eq!(
            s.to_owned(),
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
        assert_eq!(Err(StdKeyError::Prefix(KeyString(Ascii::new(s.into())))), k);
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
        assert_eq!(k.to_string(), s.to_owned());
        // and such a valid key should behave the same when inserted into
        // the hash table
        let mut p = ParsedKeywords::default();
        let res = p.insert(
            &NESlice::try_from_slice(s.as_bytes()).unwrap(),
            &NESlice::try_from_slice(b"the cake is a lie").unwrap(),
            &ReadHeaderAndTEXTConfig::default(),
        );
        assert_eq!(LogResult::new_ok(()), res);
        assert_eq!(
            s.to_owned(),
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
            Err(NonStdKeyError::Prefix(KeyString(Ascii::new(s.into())))),
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

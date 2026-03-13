use crate::api::{FlatTEXTDiagnostics, HeaderAndSuppOffsets, SplitTEXTDiagnostics};
use crate::config::{
    AllowNonunique, ConfigFlag as _, DummyTriFlag, ReadHeaderAndTEXTConfig, TriErrorFlag as _,
    UseLatin1,
};
use crate::logging::{
    DeferredWarningsAndErrors, LogResult, SwitchableErrorResult, SwitchableErrorsResult,
    WarningOrErrorResult,
};
use crate::nonempty::FcsNEVec;
use crate::text::index::{IndexFromOne, MeasIndex};
use crate::text::keywords::{
    self as kws, AsStdKeywordPair, OptMeasKeyword, OptRootKeyword, ambassador_impl_AsStdKeywordPair,
};
use crate::validated::case_ins_regex::CaseInsRegex;
use crate::validated::sub_pattern::SubPattern;

use fireflow_types::config::{PATTERN_DELIMITER, TemporalOpticalKey};
use fireflow_types::keywords::{Version, VersionMembership};
use fireflow_types::ne_str;
use fireflow_types::nonempty_string::{
    DisplayableNE as _, NEAlt, NEConcat, NEConcat4, NEConcatR, NESliceExt as _, NEStr, NEString,
    ToDisplayNE, ToNE, ambassador_impl_ToDisplayNE,
};

use ambassador::{Delegate, delegatable_trait};
use derive_more::{AsRef, Display, From};
use derive_new::new;
use hashbrown::HashMap;
use hashbrown::hash_map::Entry;
use itertools::Itertools as _;
use nonempty_collections::{
    IntoIteratorExt as _, IntoNonEmptyIterator as _, NESlice, NEVec, iter::NonEmptyIterator as _,
};
use thiserror::Error;
use unicase::Ascii;

use std::borrow::Cow;
use std::fmt;
use std::hash::Hash;
use std::marker::PhantomData;
use std::str::FromStr;
use std::string::ToString;
use std::sync::OnceLock;

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

impl<'a> ToDisplayNE<'a> for StdKey {
    type NE = NEConcat<char, ToNE<&'a KeyString>>;
    fn to_ne(&'a self) -> Self::NE {
        NEConcat::new('$', ToNE(&self.0))
    }
}

/// A key from TEXT which is not codified by the FCS standard.
///
/// This cannot start with `"$"` and may only contain ASCII characters.
#[derive(Clone, Debug, AsRef, Display, PartialEq, Eq, Hash, PartialOrd, Ord, Delegate)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
#[as_ref(KeyString, str)]
#[delegate(ToDisplayNE<'a>, generics = "'a")]
pub struct NonStdKey(KeyString);

/// The internal string for a key (standard or nonstandard).
///
/// Must be non-empty and contain only ASCII characters. Comparisons will be
/// case-insensitive.
#[derive(Clone, Debug, AsRef, Display, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
#[as_ref(str)]
pub struct KeyString(Ascii<NEString>);

impl<'a> ToDisplayNE<'a> for KeyString {
    type NE = &'a NEString;
    fn to_ne(&'a self) -> Self::NE {
        &self.0
    }
}

/// A list of patterns that match [`StdKey`]s or [`NonStdKey`]s.
#[derive(Clone)]
pub struct KeyStringsOrPatterns<T>(pub HashMap<KeyStringOrPattern, T>);

impl<T> Default for KeyStringsOrPatterns<T> {
    fn default() -> Self {
        Self(HashMap::default())
    }
}

/// Either a literal string or regexp which matches a [`StdKey`]/[`NonStdKey`].
pub type KeyStringOrPattern = LiteralOrPattern<KeyString>;

/// Either a literal string or regexp.
///
/// This exists for performance and ergononic reasons; if the goal is simply to
/// match lots of strings literally, it is faster and easier to use a hash
/// table, otherwise we need to search linearly through an array of patterns.
#[derive(Clone, PartialEq, Eq, Hash, Display)]
pub enum LiteralOrPattern<L> {
    #[display("{_0}")]
    Literal(L),
    #[display("{PATTERN_DELIMITER}{_0}{PATTERN_DELIMITER}")]
    Pattern(CaseInsRegex),
}

impl<L: FromStr> FromStr for LiteralOrPattern<L> {
    type Err = LiteralOrPatternError<<L as FromStr>::Err>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(inner) = s
            .strip_prefix(PATTERN_DELIMITER)
            .and_then(|x| x.strip_suffix(PATTERN_DELIMITER))
        {
            let ret = inner
                .parse::<CaseInsRegex>()
                .map_err(KeyRegexError)
                .map_err(LiteralOrPatternError::Regexp)?;
            Ok(Self::Pattern(ret))
        } else {
            let ret = s.parse::<L>().map_err(LiteralOrPatternError::Literal)?;
            Ok(Self::Literal(ret))
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
    pub keys_with_non_utf8_values: Vec<(AnyKey, TruncatedNEBytes)>,

    /// Valid values with non-ASCII keys.
    pub values_with_non_ascii_keys: Vec<(TruncatedNEBytes, TruncatedNEString)>,

    /// Keywords that have invalid bytes in either key or value
    pub byte_pairs: Vec<(TruncatedNEBytes, TruncatedNEBytes)>,

    /// Standard keys which appear more than once with their values.
    pub non_unique_std_keywords: Vec<(StdKey, TruncatedNEString)>,

    /// Non-standard keys which appear more than once with their values.
    pub non_unique_nonstd_keywords: Vec<(NonStdKey, TruncatedNEString)>,

    /// Standard keys which were ignored
    pub ignored_std_keywords: Vec<(StdKey, NEStringOrBytes)>,

    /// Keys with empty values.
    ///
    /// The only way this can happen at this stage is if the value is entirely
    /// whitespace and is trimmed.
    pub keys_with_empty_trimmed_values: Vec<KeyOrBytes>,

    /// Keys with values that were trimmed
    ///
    /// The value included here is the original value.
    pub keys_with_trimmed_values: Vec<(KeyOrBytes, NEStringOrBytes)>,
}

/// Either a standard or non-standard key.
#[derive(Clone, Display, PartialEq, Debug, From)]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromPyObject))]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum AnyKey {
    Std(StdKey),
    NonStd(NonStdKey),
}

impl<'a> ToDisplayNE<'a> for AnyKey {
    type NE = NEAlt<ToNE<&'a StdKey>, ToNE<&'a NonStdKey>>;
    fn to_ne(&'a self) -> Self::NE {
        match self {
            Self::Std(x) => NEAlt::Left(ToNE(x)),
            Self::NonStd(x) => NEAlt::Right(ToNE(x)),
        }
    }
}

pub type StdKeywords = HashMap<StdKey, NEString>;

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
    Bytes(TruncatedNEBytes),
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

/// A either a UTF-8 string or a non-UTF-8 byte sequence (both non-empty).
#[derive(Clone, Display, PartialEq, Debug, From)]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromPyObject))]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum NEStringOrBytes {
    Utf8(TruncatedNEString),
    Bytes(TruncatedNEBytes),
}

impl<'a> From<NESlice<'a, u8>> for NEStringOrBytes {
    fn from(value: NESlice<'a, u8>) -> Self {
        Self::from(&value)
    }
}

impl<'a> From<&NESlice<'a, u8>> for NEStringOrBytes {
    fn from(value: &NESlice<'a, u8>) -> Self {
        Self::from(value.to_ne_vec())
    }
}

impl From<NEVec<u8>> for NEStringOrBytes {
    fn from(value: NEVec<u8>) -> Self {
        match NEString::from_utf8(value) {
            Ok(s) => Self::Utf8(TruncatedNEString(s)),
            Err(e) => Self::Bytes(TruncatedNEBytes::from(e.into_bytes())),
        }
    }
}

/// A [`Vec<u8>`] optimized for displaying in errors.
#[derive(Clone, From, PartialEq, Debug, Display)]
#[display("{}", trunc_bytes(self.0.as_ref()))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct TruncatedBytes(pub Vec<u8>);

/// A [`NEVec<u8>`] optimized for displaying in errors.
#[derive(Clone, From, PartialEq, Debug, Display)]
#[display("{}", trunc_bytes(self.0.0.as_ref()))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[from(NEVec<u8>, FcsNEVec<u8>)]
pub struct TruncatedNEBytes(pub FcsNEVec<u8>);

impl<'a> From<NESlice<'a, u8>> for TruncatedNEBytes {
    fn from(value: NESlice<'a, u8>) -> Self {
        Self::from(&value)
    }
}

impl<'a> From<&NESlice<'a, u8>> for TruncatedNEBytes {
    fn from(value: &NESlice<'a, u8>) -> Self {
        Self::from(value.to_ne_vec())
    }
}

/// A normal [`String`] that will be shortened when displaying if too long.
#[derive(Clone, From, PartialEq, Debug, Display, Default)]
#[display("{}", trunc_str(self.0.as_ref()))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct TruncatedString(pub String);

/// A normal [`NEString`] that will be shortened when displaying if too long.
#[derive(Clone, From, PartialEq, Debug, Display)]
#[display("{}", trunc_str(self.0.as_ref()))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct TruncatedNEString(pub NEString);

/// An FCS key with a specific version;
// TODO const_trait_impl will be able to clean this up once stable
pub trait VersionedKey: Sized {
    const VERS: VersionMembership;

    fn is_version(&self, version: Version) -> bool {
        version.is_member(Self::VERS)
    }
}

/// A [`StdKey`] without an index
///
/// The constant traits is validated to only contain ASCII characters.
pub trait Key: VersionedKey {
    const C: &'static NEStr;

    const _CHECK: () = {
        assert!(
            is_alpha_underscore_str(Self::C.as_str()),
            "C must only be letters"
        );
    };

    #[must_use]
    #[allow(path_statements)]
    fn std() -> StdKey {
        Self::_CHECK;
        let key = Key0::<Self>::default();
        StdKey::new(key.as_ne_string())
    }

    fn self_std(&self) -> StdKey {
        Self::std()
    }
}

pub enum PrefixSuffix {
    Prefix(&'static NEStr),
    Both(&'static NEStr, &'static NEStr),
}

impl PrefixSuffix {
    const fn as_str(&self) -> (&'static str, &'static str) {
        match self {
            Self::Prefix(x) => (x.as_str(), ""),
            Self::Both(x, y) => (x.as_str(), y.as_str()),
        }
    }
}

/// A [`StdKey`] with one index
///
/// The constant traits are validated to only contain ASCII characters.
pub trait IndexedKey: VersionedKey {
    const C: PrefixSuffix;

    const _CHECK: () = {
        let (s0, s1) = Self::C.as_str();
        assert!(is_alpha_underscore_str(s0), "prefix must only be letters");
        assert!(is_alpha_underscore_str(s1), "suffix must only be letters");
    };

    #[allow(path_statements)]
    fn std(i: impl Into<IndexFromOne>) -> StdKey {
        // trigger compile time error if pre/suffix are anything but letters/underscore
        Self::_CHECK;
        let key = Key1::<Self>::new_i1(i.into());
        StdKey::new(key.as_ne_string())
    }

    fn self_std(&self, i: impl Into<IndexFromOne>) -> StdKey {
        Self::std(i)
    }

    #[cfg(feature = "serde")]
    #[must_use]
    fn std_blank() -> String {
        let (s0, s1) = Self::C.as_str();
        format!("${s0}n{s1}")
    }

    // #[cfg(feature = "serde")]
    // #[must_use]
    // fn self_std_blank(&self) -> String {
    //     Self::std_blank()
    // }

    // /// Build regexp matching `"<PREFIX>n<SUFFIX>"`
    // #[must_use]
    // fn regexp() -> CaseInsRegex {
    //     let mut s = String::new();
    //     let (s0, s1) = Self::C.as_str();
    //     s.push_str(s0);
    //     s.push_str("[1-9][0-9]*");
    //     s.push_str(s1);
    //     // ASSUME this will never fail because pre/suffix should only be letters
    //     CaseInsRegex::from_str(s.as_str()).unwrap()
    // }

    // fn matches(other: &StdKey) -> bool {
    //     static RE: OnceLock<CaseInsRegex> = OnceLock::new();
    //     RE.get_or_init(|| Self::regexp())
    //         .as_ref()
    //         .is_match(other.as_ref())
    // }
}

/// A [`StdKey`] with two indices
///
/// The constant traits are validated to only contain ASCII characters.
pub trait BiIndexedKey: VersionedKey {
    const PREFIX: &'static NEStr;
    const MIDDLE: &'static NEStr;
    // we could add a suffix for completion's sake, but so far the only keyword
    // that requires this trait is DFCmTOn which doesn't have a suffix

    const _CHECK: () = {
        assert!(
            is_alpha_underscore_str(Self::PREFIX.as_str()),
            "PREFIX must only be letters"
        );
        assert!(
            is_alpha_underscore_str(Self::MIDDLE.as_str()),
            "MIDDLE must only be letters"
        );
    };

    #[allow(path_statements)]
    fn std(i: impl Into<IndexFromOne>, j: impl Into<IndexFromOne>) -> StdKey {
        // trigger compile time error if pre/mid/suffix are anything but letters/underscore
        Self::_CHECK;
        let key = Key2::<Self>::new_i2(i.into(), j.into());
        StdKey::new(key.as_ne_string())
    }

    /// Build regexp matching `"<PREFIX>m<MIDDLE>n<SUFFIX>"`
    #[must_use]
    fn regexp() -> CaseInsRegex {
        let mut s = String::new();
        s.push_str(Self::PREFIX.as_str());
        s.push_str("([1-9][0-9]*)");
        s.push_str(Self::MIDDLE.as_str());
        s.push_str("([1-9][0-9]*)");
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

/// A type representing a [`StdKey`].
///
/// This is useful because the value of the key is not actually stored, so this
/// is very fast and memory-efficient. If we stored the value itself, it would
/// be a [`String`] internally and allocated on the heap. We can get away with
/// this because the value of each [`StdKey`] is entirely encoded by the
/// [`Key`], [`IndexedKey`], and [`BiIndexedKey`] traits (with an index in the
/// latter two cases).
#[derive(Debug, new)]
pub struct SpecificKey<T, I> {
    index: I,
    _key: PhantomData<T>,
}

/// A [`SpecificKey`] which is prefixed with '$' when displayed.
#[derive(Display, From, Delegate, Debug)]
#[display("${_0}")]
#[delegate(AsStdKey)]
pub struct DollarKey<T, I>(pub SpecificKey<T, I>);

impl<T, I: Clone> Clone for SpecificKey<T, I> {
    fn clone(&self) -> Self {
        Self::new(self.index.clone())
    }
}

impl<T, I: Clone> Clone for DollarKey<T, I> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T, I: Copy> Copy for SpecificKey<T, I> {}
impl<T, I: Copy> Copy for DollarKey<T, I> {}

pub type Key0<T> = SpecificKey<T, ()>;
pub type Key1<T> = SpecificKey<T, IndexFromOne>;
pub type Key2<T> = SpecificKey<T, BiIndex>;

pub type DKey0<T> = DollarKey<T, ()>;
pub type DKey1<T> = DollarKey<T, IndexFromOne>;
pub type DKey2<T> = DollarKey<T, BiIndex>;

impl<T> Default for Key0<T> {
    fn default() -> Self {
        Self::new(())
    }
}

impl<T> Key1<T> {
    pub(crate) fn new_i1(i: impl Into<IndexFromOne>) -> Self {
        Self::new(i.into())
    }
}

impl<T> Key2<T> {
    pub(crate) fn new_i2(i: impl Into<IndexFromOne>, j: impl Into<IndexFromOne>) -> Self {
        Self::new(BiIndex::new(i.into(), j.into()))
    }
}

impl<T> Default for DKey0<T> {
    fn default() -> Self {
        Self(Key0::default())
    }
}

impl<T> DKey1<T> {
    pub(crate) fn new_i1(i: impl Into<IndexFromOne>) -> Self {
        Self(Key1::new_i1(i))
    }

    pub(crate) fn index(self) -> IndexFromOne {
        self.0.index
    }
}

impl<T> DKey2<T> {
    pub(crate) fn new_i2(i: impl Into<IndexFromOne>, j: impl Into<IndexFromOne>) -> Self {
        Self(Key2::new_i2(i, j))
    }

    pub(crate) fn index(&self) -> BiIndex {
        self.0.index
    }
}

/// Composite index for [`StdKey`] with two index values
#[derive(Debug, Clone, Copy, new)]
pub struct BiIndex {
    pub i0: IndexFromOne,
    pub i1: IndexFromOne,
}

#[delegatable_trait]
pub(crate) trait AsStdKey {
    fn as_std_key(&self) -> StdKey;
}

impl<T: Key> AsStdKey for SpecificKey<T, ()> {
    fn as_std_key(&self) -> StdKey {
        T::std()
    }
}

impl<T: IndexedKey> AsStdKey for SpecificKey<T, IndexFromOne> {
    fn as_std_key(&self) -> StdKey {
        T::std(self.index)
    }
}

impl<T: BiIndexedKey> AsStdKey for SpecificKey<T, BiIndex> {
    fn as_std_key(&self) -> StdKey {
        let i = &self.index;
        T::std(i.i0, i.i1)
    }
}

impl<T: Key> ToDisplayNE<'_> for Key0<T> {
    type NE = &'static NEStr;
    fn to_ne(&self) -> &'static NEStr {
        T::C
    }
}

impl<T: IndexedKey> ToDisplayNE<'_> for Key1<T> {
    type NE = NEConcatR<NEConcat<&'static NEStr, ToNE<IndexFromOne>>, &'static NEStr>;
    fn to_ne(&self) -> Self::NE {
        let (pre, suf) = match T::C {
            PrefixSuffix::Both(pre, suf) => (pre, Some(suf)),
            PrefixSuffix::Prefix(pre) => (pre, None),
        };
        NEConcat::new(pre, ToNE(self.index)).append(suf)
    }
}

impl<T: BiIndexedKey> ToDisplayNE<'_> for Key2<T> {
    type NE = NEConcat4<&'static NEStr, ToNE<IndexFromOne>, &'static NEStr, ToNE<IndexFromOne>>;
    fn to_ne(&self) -> Self::NE {
        let i = &self.index;
        NEConcat::new(T::PREFIX, ToNE(i.i0))
            .append(T::MIDDLE)
            .append(ToNE(i.i1))
    }
}

impl<K, I> ToDisplayNE<'_> for DollarKey<K, I>
where
    SpecificKey<K, I>: for<'b> ToDisplayNE<'b> + Copy,
{
    type NE = NEConcat<&'static NEStr, ToNE<SpecificKey<K, I>>>;
    fn to_ne(&self) -> Self::NE {
        NEConcat::new(ne_str!("$"), ToNE(self.0))
    }
}

impl<T: Key> fmt::Display for Key0<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", T::C)
    }
}

impl<T: IndexedKey> fmt::Display for Key1<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let (s0, s1) = T::C.as_str();
        write!(f, "{s0}{}{s1}", self.index)
    }
}

impl<T: BiIndexedKey> fmt::Display for Key2<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let i = &self.index;
        write!(f, "{}{}{}{}", T::PREFIX, i.i0, T::MIDDLE, i.i1)
    }
}

pub type NonStdKeywords = HashMap<NonStdKey, NEString>;

#[derive(From, Delegate)]
#[delegate(AsStdKeywordPair)]
pub(crate) enum StdOptKeyword<'a> {
    Root(OptRootKeyword<'a>),
    Meas(OptMeasKeyword<'a>),
}

pub(crate) trait NonStdKeywordsExt {
    fn insert_demoted(&mut self, key: StdKey, value: NEString);

    fn insert_demoted_keyword(&mut self, keyword: StdOptKeyword<'_>) {
        let (k, v) = keyword.as_std_key_pair();
        self.insert_demoted(k, v);
    }

    fn insert_demoted_keyword_opt(&mut self, keyword: Option<StdOptKeyword<'_>>) {
        if let Some(k) = keyword {
            self.insert_demoted_keyword(k);
        }
    }

    fn transfer_demoted(&mut self, kws: &mut StdKeywords, key: StdKey) {
        if let Some(v) = kws.remove(&key) {
            self.insert_demoted(key, v);
        }
    }
}

impl NonStdKeywordsExt for NonStdKeywords {
    fn insert_demoted(&mut self, key: StdKey, value: NEString) {
        let mut k = NonStdKey(key.0);
        while self.contains_key(&k) {
            k.0.disambiguate();
        }
        let ret = self.insert(k, value);
        debug_assert!(ret.is_none(), "key not disambiguated");
    }
}

impl KeyString {
    fn new(s: NEString) -> Self {
        Self(Ascii::new(s))
    }

    fn disambiguate(&mut self) {
        self.0.push('_');
    }

    fn from_bytes_maybe(xs: &NESlice<u8>, latin1: UseLatin1) -> Option<Self> {
        if latin1.is_set() {
            let ne = xs.into_nonempty_iter().copied().map(char::from).collect();
            Some(Self::new(ne))
        } else if is_printable_ascii(xs.as_ref()) {
            // SAFETY: we just checked that the bytes are only ASCII chars
            Some(unsafe { Self::from_bytes(xs) })
        } else {
            None
        }
    }

    unsafe fn from_bytes(xs: &NESlice<u8>) -> Self {
        let ne = xs.nonempty_iter().copied().collect();
        // SAFETY: this function is marked unsafe since the caller must check
        Self::new(unsafe { NEString::from_utf8_unchecked(ne) })
    }
}

#[cfg(feature = "serde")]
impl Serialize for KeyString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        AsRef::<str>::as_ref(self).serialize(serializer)
    }
}

impl StdKey {
    pub(crate) fn as_ascii_str(&self) -> Ascii<&str> {
        Ascii::new(self.0.0.as_ref())
    }

    fn new(s: NEString) -> Self {
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

impl FromStr for KeyString {
    type Err = AsciiStringError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(ne) = s.parse::<NEString>() {
            if is_printable_ascii(s.as_ref()) {
                Ok(Self(Ascii::new(ne)))
            } else {
                Err(AsciiStringError::Ascii(s.into()))
            }
        } else {
            Err(AsciiStringError::Empty)
        }
    }
}

impl FromStr for StdKey {
    type Err = StdKeyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let ks = s.parse::<KeyString>().map_err(StdKeyError::Ascii)?;
        let ne = ks.0.as_ne_str().as_bytes();
        let (y, ys) = ne.split_first();
        if *y != STD_PREFIX {
            Err(StdKeyError::Prefix(ks))
        } else if let Some(zs) = NESlice::try_from_slice(ys) {
            // SAFETY: this will not fail because we know the string has only
            // ASCII bytes
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
            Ok(Self(ks))
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
        enum TrimResult<'a> {
            Trimmed(Cow<'a, NEStr>, bool),
            Empty(DummyTriFlag),
        }

        enum KeyValueResult<'a> {
            Ignore(StdKey, NEStringOrBytes),
            Empty(KeyOrBytes, DummyTriFlag),
            NonEmpty(AnyKey, Cow<'a, NEStr>, bool),
            NonUtf8Value(AnyKey, TruncatedNEBytes),
            NonAsciiKey(TruncatedNEBytes, TruncatedNEString, bool),
            BothInvalid(TruncatedNEBytes, TruncatedNEBytes),
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

        let parse_value = || {
            let flag = conf.trim_value_whitespace;
            let triflag = DummyTriFlag::from_trim_value_whitespace(flag);
            if conf.use_latin1.is_set() {
                let it = val.into_nonempty_iter().copied().map(char::from);
                if let Some(tf) = triflag {
                    if let Some(ne) = it
                        .skip_while(char::is_ascii_whitespace)
                        .take_while(|x| !x.is_ascii_whitespace())
                        .try_into_nonempty_iter()
                    {
                        let s: NEString = ne.collect();
                        let was_trimmed = val.len() < s.len();
                        Some(TrimResult::Trimmed(Cow::Owned(s), was_trimmed))
                    } else {
                        Some(TrimResult::Empty(tf))
                    }
                } else {
                    Some(TrimResult::Trimmed(Cow::Owned(it.collect()), false))
                }
            } else if let Ok(vv) = NEStr::from_utf8(val) {
                if let Some(tf) = triflag {
                    if let Some(trimmed) = NEStr::try_new(vv.as_ref().trim()) {
                        let was_trimmed = vv.len() < trimmed.len();
                        Some(TrimResult::Trimmed(Cow::Borrowed(trimmed), was_trimmed))
                    } else {
                        Some(TrimResult::Empty(tf))
                    }
                } else {
                    Some(TrimResult::Trimmed(Cow::Borrowed(vv), false))
                }
            } else {
                None
            }
        };

        let kv_res = if let Some((is_std, kstr)) = parse_key(key) {
            if is_std {
                // Standard key: starts with '$' and is ASCII
                if ignore.is_match(&kstr) {
                    KeyValueResult::Ignore(StdKey(kstr), NEStringOrBytes::from(val))
                } else {
                    let ak = AnyKey::Std(StdKey(kstr));
                    if let Some(trim_res) = parse_value() {
                        match trim_res {
                            TrimResult::Empty(flag) => KeyValueResult::Empty(ak.into(), flag),
                            TrimResult::Trimmed(value, was_trimmed) => {
                                KeyValueResult::NonEmpty(ak, value, was_trimmed)
                            }
                        }
                    } else {
                        KeyValueResult::NonUtf8Value(ak, TruncatedNEBytes::from(val))
                    }
                }
            } else {
                // Non-standard key: does not start with '$' and is ASCII
                let ak = AnyKey::NonStd(NonStdKey(kstr));
                if let Some(trim_res) = parse_value() {
                    match trim_res {
                        TrimResult::Empty(flag) => KeyValueResult::Empty(ak.into(), flag),
                        TrimResult::Trimmed(value, was_trimmed) => {
                            KeyValueResult::NonEmpty(ak, value, was_trimmed)
                        }
                    }
                } else {
                    KeyValueResult::NonUtf8Value(ak, TruncatedNEBytes::from(val))
                }
            }
        } else {
            // Non-ascii key with possibly non-Utf-8 value
            let kbytes = TruncatedNEBytes::from(key);
            if let Some(trim_res) = parse_value() {
                match trim_res {
                    TrimResult::Empty(flag) => {
                        KeyValueResult::Empty(KeyOrBytes::from(kbytes), flag)
                    }
                    TrimResult::Trimmed(value, was_trimmed) => {
                        let tv = value.into_owned().into();
                        KeyValueResult::NonAsciiKey(kbytes, tv, was_trimmed)
                    }
                }
            } else {
                KeyValueResult::BothInvalid(kbytes, val.into())
            }
        };

        match kv_res {
            KeyValueResult::NonEmpty(k, v, was_trimmed) => {
                if was_trimmed {
                    let vo = NEStringOrBytes::from(TruncatedNEString(v.clone().into_owned()));
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
                            if let Some(&s) = subs.get(&rk) {
                                let sub_res = s.sub(v.as_ref().as_ref());
                                if let Ok(ne) = NEString::try_from(sub_res) {
                                    self.insert_nonunique_std(rk, ne, conf)
                                } else {
                                    let sk = StdKey(rk);
                                    let e =
                                        SubPatternEmptyError::new(sk, v.into_owned(), s.clone());
                                    LogResult::new_err(e.into())
                                }
                            } else {
                                self.insert_nonunique_std(rk, v.into_owned(), conf)
                            }
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
                    let vo = NEStringOrBytes::from(v.clone());
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
        value: NEString,
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
        value: NEString,
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
        kws: &mut HashMap<K, NEString>,
        nonunique: &mut Vec<(K, TruncatedNEString)>,
        k: K,
        value: NEString,
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
                nonunique.push((key, TruncatedNEString(value)));
                LogResult::new_deferred_switchable3((), err.into(), flag)
                    .switchable_into_non_commutative()
            }
            Entry::Vacant(ent) => {
                let v = conf
                    .replace_standard_key_values
                    .get(ent.key().as_ref())
                    .cloned()
                    .unwrap_or(value);
                ent.insert(v);
                LogResult::new_ok(())
            }
        }
    }

    pub(crate) fn append_std(
        &mut self,
        new: &HashMap<KeyString, NEString>,
        flag: AllowNonunique,
    ) -> SwitchableErrorsResult<(), (), AllowNonunique, StdPresent> {
        let es = new
            .iter()
            .filter_map(|(k, v)| match self.std.entry(StdKey(k.clone())) {
                Entry::Occupied(e) => {
                    self.diag
                        .non_unique_std_keywords
                        .push((StdKey(k.clone()), TruncatedNEString(v.clone())));
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
                    .map(|(k, v)| (KeyOrBytes::from(k), NEStringOrBytes::from(v)))
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
pub type KeyStringsOrPatternsError = LiteralOrPatternError<AsciiStringError>;

/// Error when parsing literal or pattern string.
#[derive(Debug, Display, PartialEq, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(E: Into<PyErr>))]
pub enum LiteralOrPatternError<E> {
    Regexp(KeyRegexError),
    Literal(E),
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
    Sub(SubPatternEmptyError),
}

/// Error when applying a [`SubPattern`] resulted in an empty string.
#[derive(Debug, PartialEq, Error, new)]
#[error(
    "applying substitution pattern '{pat}' to value '{value}' for key \
     '{key}' resulted in empty string"
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeyError))]
pub struct SubPatternEmptyError {
    key: StdKey,
    value: NEString,
    pat: SubPattern,
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
    pub value: NEString,
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
    key: TruncatedNEBytes,
    value: TruncatedNEBytes,
}

/// Error when key is not ASCII
#[derive(Debug, Error)]
#[error("non ASCII key encountered with bytes {key} and value '{value}'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeyError))]
pub struct NonAsciiKeyError {
    key: TruncatedNEBytes,
    value: TruncatedNEString,
}

/// Error when value is not Utf8
#[derive(Debug, Error)]
#[error("non UTF-8 key encountered with bytes {value} and key '{key}'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeyError))]
pub struct NonUtf8ValueError {
    key: AnyKey,
    value: TruncatedNEBytes,
}

fn trunc_bytes(xs: &[u8]) -> String {
    let mut s = String::new();
    for (i, &x) in xs.iter().take(TRUNCATED_BYTES_LIMIT).enumerate() {
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
    if xs.len() > TRUNCATED_BYTES_LIMIT {
        format!("[{s},...]")
    } else {
        format!("[{s}]")
    }
}

fn trunc_str(s: &str) -> Cow<'_, str> {
    let n = s.chars().count();
    if n > TRUNCATED_STR_LIMIT {
        let t: String = s.chars().take(n).collect();
        Cow::Owned(format!("{t}…(more)"))
    } else {
        Cow::Borrowed(s)
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
    use fireflow_types::nonempty_string::NEString;

    use hashbrown::HashMap;
    use serde::Serialize;

    use std::collections::BTreeMap;

    pub fn ordered_map<K: Serialize + Clone + Ord, S>(
        value: &HashMap<K, NEString>,
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
        assert_eq!(StdKey(KeyString(Ascii::new("MAJESTY".parse().unwrap()))), k);
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
        let e = StdKeyError::Ascii(AsciiStringError::Ascii(s.parse().unwrap()));
        assert_eq!(Err(e), k);
    }

    #[test]
    fn fromstr_std_key_noprefix() {
        let s = "IMBROKE";
        let k = s.parse::<StdKey>();
        let e = StdKeyError::Prefix(KeyString(Ascii::new(s.parse().unwrap())));
        assert_eq!(Err(e), k);
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
        let ns = NonStdKey(KeyString(Ascii::new("YTSEJAM".parse().unwrap())));
        assert_eq!(ns, k);
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
        let e = NonStdKeyError::Ascii(AsciiStringError::Ascii(s.parse().unwrap()));
        assert_eq!(Err(e), k);
    }

    #[test]
    fn fromstr_nonstd_key_hasprefix() {
        let s = "$IMRICH";
        let k = s.parse::<NonStdKey>();
        let e = NonStdKeyError::Prefix(KeyString(Ascii::new(s.parse().unwrap())));
        assert_eq!(Err(e), k);
    }

    #[test]
    fn fromstr_nonstd_key_blank() {
        let s = "";
        let k = s.parse::<NonStdKey>();
        assert_eq!(Err(NonStdKeyError::Ascii(AsciiStringError::Empty)), k);
    }
}

#[cfg(feature = "python")]
mod python {
    use super::{LiteralOrPattern, LiteralOrPatternError};

    use pyo3::{prelude::*, types::PyString};

    use std::convert::Infallible;
    use std::fmt;
    use std::str::FromStr;

    // TODO make FromStr and ToStr derive work for these, which will
    // in turn require than the bounds attributes get cleaned up

    impl<'py, L> FromPyObject<'py> for LiteralOrPattern<L>
    where
        PyErr: From<LiteralOrPatternError<L::Err>>,
        L: FromStr,
        Self: FromStr<Err = LiteralOrPatternError<L::Err>>,
    {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            Ok(ob.extract::<String>()?.parse()?)
        }
    }

    impl<'py, L: fmt::Display> IntoPyObject<'py> for LiteralOrPattern<L> {
        type Target = PyString;
        type Output = Bound<'py, Self::Target>;
        type Error = Infallible;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            self.to_string().into_pyobject(py)
        }
    }
}

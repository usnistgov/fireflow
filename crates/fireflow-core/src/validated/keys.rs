use crate::api::{FlatTEXTDiagnostics, HeaderAndSuppOffsets, SplitTEXTDiagnostics};
use crate::config::EvaledReadDataKeywordsConfig;
use crate::fixed_vec::OneOrTwo;
use crate::logging::{
    DeferredWarningsAndErrors, LogResult, WarningAndErrorResult, WarningsAndErrorsResult,
};
use crate::nonempty::FcsNEVec;
use crate::segment::read::HeaderOffsetsOverflow;
use crate::text::keyword_enum::{
    AsStdKeywordPair, OptMeasKeyword, OptRootKeyword, ambassador_impl_AsStdKeywordPair,
};

use fireflow_types::{
    case_ins_regex::CaseInsRegex,
    config::{
        DummyTriFlag, Encoding, OpticalOnlyKey, OpticalOnlyKeys, ProcessOpticalOnlyKeys,
        ReadHeaderAndTEXTConfig, TemporalHasOpticalKeyError, TriErrorFlag as _,
    },
    index::{BiMeasIndex, MeasIndex},
    keystring::{KeyString, KeyStringOrPattern, KeyStringsOrPatterns, NEAsciiStringError},
    ne_str,
    nonempty_string::{
        NEAlt, NEConcat, NESliceExt as _, NEStr, NEString, ToDisplayNE, ToNE,
        ambassador_impl_ToDisplayNE,
    },
    std_key::{PseudoStdKey, RealOrPseudoStdKey, STD_PREFIX, StdKey, ToStd},
    sub_pattern::SubPattern,
};

use ambassador::Delegate;
use derive_more::{AsRef, Display, From, Into};
use derive_new::new;
use derive_where::derive_where;
use hashbrown::HashMap;
use hashbrown::hash_map::Entry;
use itertools::Itertools as _;
use nonempty_collections::{
    IntoIteratorExt as _, IntoNonEmptyIterator as _, NESlice, NEVec, iter::NonEmptyIterator as _,
};
use thiserror::Error;

use std::borrow::Cow;
use std::fmt;
use std::hash::Hash;
use std::marker::PhantomData;
use std::str::FromStr;
use std::string::ToString;

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

// /// A key from TEXT which is codified by the FCS standard.
// ///
// /// These may only contain ASCII and must start with `"$"`. The `"$"` is not
// /// actually stored but will be appended when converting to a [`String`].
// #[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, AsRef, Display)]
// #[cfg_attr(feature = "serde", derive(Serialize))]
// #[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
// #[as_ref(KeyString, str, NEStr)]
// #[display("${_0}")]
// pub struct StdKey(KeyString);

// impl<'a> ToDisplayNE<'a> for StdKey {
//     type NE = NEConcat<char, ToNE<&'a KeyString>>;
//     fn to_ne(&'a self) -> Self::NE {
//         NEConcat::new('$', ToNE(&self.0))
//     }
// }

/// A key from TEXT which is not codified by the FCS standard.
///
/// This cannot start with `"$"` and may only contain ASCII characters.
#[derive(Clone, Debug, AsRef, Display, PartialEq, Eq, Hash, PartialOrd, Ord, Delegate)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
#[as_ref(KeyString, str, NEStr)]
#[delegate(ToDisplayNE<'a>, generics = "'a")]
pub struct NonStdKey(KeyString);

/// A collection of [`StdKey`]s and [`NonStdKey`]s and key/values with errors.
#[derive(Default)]
pub struct ParsedKeywords {
    /// Standard keywords (with '$')
    pub(crate) std: HashMap<StdKey, NEString>,

    /// Pseudostandard keywords (with '$' but not part of standard)
    pub(crate) pstd: HashMap<PseudoStdKey, NEString>,

    /// Non-standard keywords (without '$')
    pub(crate) nonstd: NonStdKeywords,

    /// Keywords that failed for some reason.
    pub(crate) diag: ParsedKeywordsDiagnostic,
}

/// Diagnostic output from repairing the keyword list.
#[derive(Clone, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[allow(clippy::too_many_arguments)]
pub struct RepairDiagnostics {
    /// Standard keys which appear more than once with their values.
    pub non_unique_std: Vec<(StdKey, TruncatedNEString)>,

    /// Non-standard keys which appear more than once with their values.
    pub non_unique_nonstd: Vec<(NonStdKey, TruncatedNEString)>,

    /// Standard keys which were demoted.
    pub demoted: Vec<StdKey>,

    /// Non-standard keys which were promoted.
    pub promoted: Vec<NonStdKey>,

    /// Standard keys which had values that were substituted.
    ///
    /// Values here are the original.
    pub subbed: Vec<(StdKey, TruncatedNEString)>,

    /// Standard keys which had values that were replaced.
    ///
    /// Values here are the original.
    pub replaced: Vec<(StdKey, TruncatedNEString)>,

    /// Standard keys which were renamed.
    ///
    /// First key in pair is the original.
    pub renamed: Vec<(StdKey, StdKey)>,

    /// Standard keys which were ignored.
    pub ignored: Vec<(StdKey, TruncatedNEString)>,

    /// Standard keys which were removed.
    pub removed: Vec<(StdKey, TruncatedNEString)>,
}

/// Either a standard or non-standard key.
#[derive(Clone, Display, PartialEq, Debug, From)]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromPyObject))]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum AnyKey {
    Std(RealOrPseudoStdKey),
    NonStd(NonStdKey),
}

/// A standard (non-pseudostandard) key or non-standard key.
#[derive(Clone, PartialEq, From)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum WritableKey {
    Std(StdKey),
    NonStd(NonStdKey),
}

impl<'a> ToDisplayNE<'a> for WritableKey {
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
    // TODO turn this into an abstraction over the real/pseudo-std split, it
    // would be good to avoid using a hash table as much as possible, different
    // classes of keywords can be put into different slots to avoid hashing and
    // also make it easier later when we standardize
    #[cfg_attr(feature = "serde", serde(serialize_with = "serialize::ordered_map"))]
    pub std: StdKeywords,
    #[cfg_attr(feature = "serde", serde(serialize_with = "serialize::ordered_map"))]
    pub nonstd: NonStdKeywords,
}

/// A string that should be used as the header in the measurement table.
#[derive(Display)]
pub struct MeasHeader(pub String);

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

impl StringOrBytes {
    pub(crate) fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Bytes(x) => &x.0[..],
            Self::Utf8(x) => x.0.as_bytes(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.as_bytes().len()
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

impl From<NEStringOrBytes> for StringOrBytes {
    fn from(value: NEStringOrBytes) -> Self {
        match value {
            NEStringOrBytes::Bytes(x) => Self::Bytes(x.into()),
            NEStringOrBytes::Utf8(x) => Self::Utf8(x.into()),
        }
    }
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
#[derive(Clone, From, PartialEq, Debug, Display, Into)]
#[display("{}", trunc_bytes(self.0.0.as_ref()))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[from(NEVec<u8>, FcsNEVec<u8>)]
#[into(Vec<u8>, NEVec<u8>, FcsNEVec<u8>)]
pub struct TruncatedNEBytes(pub FcsNEVec<u8>);

impl From<TruncatedNEBytes> for TruncatedBytes {
    fn from(value: TruncatedNEBytes) -> Self {
        Self::from(Vec::from(value))
    }
}

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
#[derive(Clone, From, PartialEq, Debug, Display, Into)]
#[display("{}", trunc_str(self.0.as_ref()))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[into(String, NEString)]
pub struct TruncatedNEString(pub NEString);

impl From<TruncatedNEString> for TruncatedString {
    fn from(value: TruncatedNEString) -> Self {
        Self::from(String::from(value))
    }
}

/// A type representing a [`StdKey`].
///
/// This is useful because the value of the key is not actually stored, so this
/// is very fast and memory-efficient. If we stored the value itself, it would
/// be a [`String`] internally and allocated on the heap. We can get away with
/// this because the value of each [`StdKey`] is entirely encoded by the
/// [`Key`], [`IndexedKey`], and [`BiIndexedKey`] traits (with an index in the
/// latter two cases).
#[derive(new)]
#[derive_where(Clone, Copy, Default, PartialEq, Eq, Debug; I)]
pub struct SpecificKey_<T, I> {
    index: I,
    _key: PhantomData<T>,
}

pub type SpecificKey<T> = SpecificKey_<T, <T as ValueToStdKey>::Index>;

impl<T: ValueToStdKey> ToDisplayNE<'_> for SpecificKey<T>
where
    Self: Into<StdKey> + Copy,
{
    type NE = ToNE<StdKey>;
    fn to_ne(&self) -> Self::NE {
        ToNE((*self).into())
    }
}

impl<T: ValueToStdKey> fmt::Display for SpecificKey<T>
where
    for<'a> &'a Self: Into<StdKey>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", self.into())
    }
}

impl<T: ValueToStdKey> From<SpecificKey<T>> for StdKey {
    fn from(value: SpecificKey<T>) -> Self {
        T::std(&value.index)
    }
}

impl<'a, T: ValueToStdKey> From<&'a SpecificKey<T>> for StdKey {
    fn from(value: &'a SpecificKey<T>) -> Self {
        T::std(&value.index)
    }
}

impl<T> SpecificKey_<T, BiMeasIndex> {
    pub(crate) fn new_i2(i: MeasIndex, j: MeasIndex) -> Self {
        Self::new(BiMeasIndex::new(i, j))
    }
}

/// A [`SpecificKey`] which is prefixed with '$' when displayed.
#[derive(Display, From)]
#[display("${_0}")]
#[derive_where(Clone, Copy, Default, PartialEq, Eq, Debug; I)]
pub struct DollarKey_<T, I>(pub SpecificKey_<T, I>);

pub type DollarKey<T> = DollarKey_<T, <T as ValueToStdKey>::Index>;

impl<T: ValueToStdKey> From<DollarKey<T>> for StdKey {
    fn from(value: DollarKey<T>) -> Self {
        value.0.into()
    }
}

impl<K: ValueToStdKey> ToDisplayNE<'_> for DollarKey<K>
where
    SpecificKey<K>: for<'b> ToDisplayNE<'b> + Copy,
{
    type NE = NEConcat<&'static NEStr, ToNE<SpecificKey<K>>>;
    fn to_ne(&self) -> Self::NE {
        NEConcat::new(ne_str!("$"), ToNE(self.0))
    }
}

impl<T, I> DollarKey_<T, I> {
    pub(crate) fn new(i: I) -> Self {
        Self(SpecificKey_::new(i))
    }

    pub(crate) fn index(self) -> I {
        self.0.index
    }
}

impl<T> DollarKey_<T, BiMeasIndex> {
    pub(crate) fn new_i2(i: MeasIndex, j: MeasIndex) -> Self {
        Self(SpecificKey_::new_i2(i, j))
    }
}

pub type NonStdKeywords = HashMap<NonStdKey, NEString>;

#[derive(From, Delegate)]
#[delegate(AsStdKeywordPair)]
pub(crate) enum StdOptKeyword<'a> {
    Root(OptRootKeyword<'a>),
    Meas(OptMeasKeyword<'a>),
}

/// Error when parsing [`NonStdKey`] from string
#[derive(From, PartialEq, Debug, Error, Clone)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeyError))]
pub enum NonStdKeyError {
    #[error("{0}")]
    Ascii(NEAsciiStringError),
    #[error("non-standard key must not start with '$', found '{0}'")]
    Prefix(KeyString),
}

/// Error when parsed keyword cannot be inserted into [`ParsedKeywords`]
#[derive(Debug, Display, From, PartialEq, Error, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum KeywordInsertError {
    StdPresent(StdPresent),
    PseudoStdPresent(PseudoStdPresent),
    NonStdPresent(NonStdPresent),
    Blank(BlankValueError),
}

pub type StdPresent = KeyPresent<StdKey>;
pub type PseudoStdPresent = KeyPresent<PseudoStdKey>;
pub type NonStdPresent = KeyPresent<NonStdKey>;

// /// Error when applying a [`SubPattern`] resulted in an empty string.
// #[derive(Debug, PartialEq, Error, new, Clone)]
// #[error(
//     "applying substitution pattern '{pat}' to value '{value}' for key \
//      '{key}' resulted in empty string"
// )]
// #[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
// #[cfg_attr(feature = "python", pyerr(py::ParseKeyError))]
// pub struct SubPatternEmptyError {
//     key: StdKey,
//     value: NEString,
//     pat: SubPattern,
// }

/// Error when key has blank value
#[derive(Debug, PartialEq, Error, Clone)]
#[error("skipping key {0} with blank value")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeyError))]
pub struct BlankValueError(pub KeyOrBytes);

/// Error when key is already present in hash table.
#[derive(Debug, PartialEq, Error, new, Clone)]
#[error("key '{key}' already present, has value '{value}'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeyError))]
#[cfg_attr(feature = "python", bound(T: fmt::Display))]
pub struct KeyPresent<T> {
    pub key: T,
    pub value: NEString,
}

/// Error when keyword has any invalid chars.
#[derive(Debug, Display, From, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum InvalidKeywordCharsError {
    Key(NonAsciiKeyError),
    Value(NonUtf8ValueError),
    Both(NonAsciiOrUtf8KeywordError),
}

/// Error when key or value with invalid UTF-8 characters is encountered
#[derive(Debug, Error, PartialEq, Clone)]
#[error("non ASCII key {key} and non UTF-8 value {value} encountered")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeyError))]
pub struct NonAsciiOrUtf8KeywordError {
    key: TruncatedNEBytes,
    value: TruncatedNEBytes,
}

/// Error when key is not ASCII
#[derive(Debug, Error, PartialEq, Clone)]
#[error("non ASCII key encountered with bytes {key} and value '{value}'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeyError))]
pub struct NonAsciiKeyError {
    key: TruncatedNEBytes,
    value: TruncatedNEString,
}

/// Error when value is not Utf8
#[derive(Debug, Error, PartialEq, Clone)]
#[error("non UTF-8 key encountered with bytes {value} and key '{key}'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeyError))]
pub struct NonUtf8ValueError {
    key: AnyKey,
    value: TruncatedNEBytes,
}

/// Error when keyword repair process resulted in colliding non-unique keys.
#[derive(Debug, Error, PartialEq, Clone)]
#[error(
    "the following keys were non-unique when demoting or promoting: {}",
    self.0.iter().join(","),
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct RepairCollisionError(NEVec<AnyKey>);

// TODO sealme in mod

/// A "compiled" object to match keys efficiently.
pub(crate) struct KeyMatcher<'a, T> {
    literal: HashMap<&'a KeyString, &'a T>,
    pattern: Vec<(&'a CaseInsRegex, &'a T)>,
}

impl<'a, T> KeyMatcher<'a, T> {
    pub(crate) fn from_keys(keys: &'a KeyStringsOrPatterns<T>) -> Self {
        keys.0.iter().collect()
    }
}

/// All compiled key matchers to prevent repeated allocations in loops
pub(crate) struct AllKeyMatchers<'a> {
    pub(crate) promote: KeyMatcher<'a, ()>,
    pub(crate) demote: KeyMatcher<'a, ()>,
    pub(crate) ignore: KeyMatcher<'a, ()>,
    pub(crate) subs: KeyMatcher<'a, SubPattern>,
}

impl<'a> AllKeyMatchers<'a> {
    pub(crate) fn from_config(conf: &'a EvaledReadDataKeywordsConfig) -> Self {
        Self {
            promote: KeyMatcher::from_keys(&conf.promote_nonstandard_keys),
            demote: KeyMatcher::from_keys(&conf.demote_standard_keys),
            ignore: KeyMatcher::from_keys(&conf.ignore_standard_keys),
            subs: KeyMatcher::from_keys(&conf.substitute_standard_key_values),
        }
    }
}

#[derive(Default)]
pub(crate) struct ParsedKeywordsDiagnostic {
    /// Valid keys with non-UTF8 values.
    pub(crate) keys_with_non_utf8_values: Vec<(AnyKey, TruncatedNEBytes)>,

    /// Valid values with non-ASCII keys.
    pub(crate) values_with_non_ascii_keys: Vec<(TruncatedNEBytes, TruncatedNEString)>,

    /// Keywords that have invalid bytes in either key or value
    pub(crate) byte_pairs: Vec<(TruncatedNEBytes, TruncatedNEBytes)>,

    /// Standard keys which appear more than once with their values.
    pub(crate) non_unique_std_keywords: Vec<(StdKey, TruncatedNEString)>,

    /// Pseudostandard keys which appear more than once with their values.
    pub(crate) non_unique_pstd_keywords: Vec<(PseudoStdKey, TruncatedNEString)>,

    /// Non-standard keys which appear more than once with their values.
    pub(crate) non_unique_nonstd_keywords: Vec<(NonStdKey, TruncatedNEString)>,

    /// Keys with empty values.
    ///
    /// The only way this can happen at this stage is if the value is entirely
    /// whitespace and is trimmed.
    pub(crate) keys_with_empty_trimmed_values: Vec<KeyOrBytes>,

    /// Keys with values that were trimmed
    ///
    /// The value included here is the original value.
    pub(crate) keys_with_trimmed_values: Vec<(KeyOrBytes, TruncatedNEString)>,
}

// Declare traits which map rust values to standardized keywords.

pub trait ValueToStdKey {
    type Index;
    type Id: ToStd<Index = Self::Index>;
    const STD: Self::Id;

    #[must_use]
    fn std(index: &Self::Index) -> StdKey {
        Self::STD.to_std(index)
    }
}

// Implement extension trait for processing nonstandard keywords in hash table.

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
}

impl NonStdKeywordsExt for NonStdKeywords {
    fn insert_demoted(&mut self, key: StdKey, value: NEString) {
        let mut k = NonStdKey(key.as_keystring());
        while self.contains_key(&k) {
            k.0.disambiguate();
        }
        assert!(self.insert(k, value).is_none(), "key not disambiguated");
    }
}

// Implement methods for nonstd key wrappers

impl FromStr for NonStdKey {
    type Err = NonStdKeyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let ks = s.parse::<KeyString>().map_err(NonStdKeyError::Ascii)?;
        if has_no_std_prefix(AsRef::<str>::as_ref(&ks).as_bytes()) {
            Ok(Self(ks))
        } else {
            Err(NonStdKeyError::Prefix(ks))
        }
    }
}

// Implement methods for key matcher

impl KeyMatcher<'_, ()> {
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
        self.literal.get(other).copied().or(self
            .pattern
            .iter()
            .find(|p| p.0.as_ref().is_match(other.as_ref()))
            .map(|(_, x)| *x))
    }
}

impl<'a, X> FromIterator<(&'a KeyStringOrPattern, &'a X)> for KeyMatcher<'a, X> {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = (&'a KeyStringOrPattern, &'a X)>,
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

// Implement methods for misc types

type OpticalOnlyResult = WarningsAndErrorsResult<
    Vec<(StdKey, NEString)>,
    (),
    TemporalHasOpticalKeyError,
    TemporalHasOpticalKeyError,
>;

/// Insert a key and value from buffer into appropriate hash table.
///
/// Return value will be Some((X, false)) if a warning as emitted, Some((X,
/// true)) if an error was emitted, and None if neither was emitted.
#[allow(clippy::too_many_lines)]
impl ParsedKeywords {
    pub(crate) fn insert(
        &mut self,
        key: &NESlice<u8>,
        val: &NESlice<u8>,
        encoding: Encoding,
        conf: &ReadHeaderAndTEXTConfig,
    ) -> Option<(KeywordInsertError, bool)> {
        enum TrimResult<'a> {
            Trimmed(Cow<'a, NEStr>, bool),
            Empty(DummyTriFlag),
        }

        enum KeyValueResult<'a> {
            Empty(KeyOrBytes, DummyTriFlag),
            NonEmpty(AnyKey, Cow<'a, NEStr>, bool),
            NonUtf8Value(AnyKey, TruncatedNEBytes),
            NonAsciiKey(TruncatedNEBytes, TruncatedNEString, bool),
            BothInvalid(TruncatedNEBytes, TruncatedNEBytes),
        }

        let parse_key = |s: &NESlice<u8>| {
            let single_byte = matches!(encoding, Encoding::Single);
            if let Some((&STD_PREFIX, rest)) = s.as_ref().split_first() {
                // TODO we may wish to distinguish an error between non-ASCII
                // and only a '$' keyword
                let ne = NESlice::try_from_slice(rest)?;
                let k = RealOrPseudoStdKey::from_bytes_maybe(&ne)?;
                Some(AnyKey::Std(k))
            } else {
                let k = KeyString::from_bytes_maybe(s, single_byte)?;
                Some(AnyKey::NonStd(NonStdKey(k)))
            }
        };

        let parse_value = || {
            let flag = conf.trim_value_whitespace;
            let triflag = DummyTriFlag::from_trim_value_whitespace(flag);
            match encoding {
                Encoding::Single => {
                    if let Some(tf) = triflag {
                        if let Some(ne) = val
                            .as_ref()
                            .trim_ascii()
                            .iter()
                            .copied()
                            .map(char::from)
                            .try_into_nonempty_iter()
                        {
                            let s: NEString = ne.collect();
                            let was_trimmed = val.len() < s.len();
                            Some(TrimResult::Trimmed(Cow::Owned(s), was_trimmed))
                        } else {
                            Some(TrimResult::Empty(tf))
                        }
                    } else {
                        let it = val.into_nonempty_iter().copied().map(char::from);
                        Some(TrimResult::Trimmed(Cow::Owned(it.collect()), false))
                    }
                }
                Encoding::Utf8 => {
                    if let Ok(vv) = NEStr::from_utf8(val) {
                        if let Some(tf) = triflag {
                            if let Some(trimmed) = NEStr::try_new(vv.as_ref().trim()) {
                                let was_trimmed = trimmed.len() < vv.len();
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
                }
            }
        };

        let kv_res = if let Some(parsed) = parse_key(key) {
            match parsed {
                AnyKey::Std(k) => {
                    if let Some(trim_res) = parse_value() {
                        match trim_res {
                            TrimResult::Empty(flag) => {
                                KeyValueResult::Empty(AnyKey::Std(k).into(), flag)
                            }
                            TrimResult::Trimmed(value, was_trimmed) => {
                                KeyValueResult::NonEmpty(k.into(), value, was_trimmed)
                            }
                        }
                    } else {
                        KeyValueResult::NonUtf8Value(k.into(), TruncatedNEBytes::from(val))
                    }
                }
                AnyKey::NonStd(k) => {
                    // Non-standard key: does not start with '$' and is ASCII
                    if let Some(trim_res) = parse_value() {
                        match trim_res {
                            TrimResult::Empty(flag) => {
                                KeyValueResult::Empty(AnyKey::NonStd(k).into(), flag)
                            }
                            TrimResult::Trimmed(value, was_trimmed) => {
                                KeyValueResult::NonEmpty(k.into(), value, was_trimmed)
                            }
                        }
                    } else {
                        KeyValueResult::NonUtf8Value(AnyKey::NonStd(k), TruncatedNEBytes::from(val))
                    }
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
                    let vo = TruncatedNEString(v.clone().into_owned());
                    let pair = (k.clone().into(), vo);
                    self.diag.keys_with_trimmed_values.push(pair);
                }
                let vo = v.into_owned();
                match k {
                    AnyKey::Std(k) => {
                        match k {
                            RealOrPseudoStdKey::Pseudo(p) => {
                                self.insert_nonunique_pstd(p, vo, conf);
                            }
                            RealOrPseudoStdKey::Real(p) => {
                                self.insert_nonunique_std(p, vo, conf);
                            }
                        }
                        None
                    }
                    AnyKey::NonStd(k) => self.insert_nonunique_nonstd(k, vo, conf),
                }
            }
            KeyValueResult::Empty(k, flag) => {
                self.diag.keys_with_empty_trimmed_values.push(k.clone());
                let e = KeywordInsertError::from(BlankValueError(k));
                flag.is_error().map(|is_err| (e, is_err))
            }
            KeyValueResult::NonAsciiKey(k, v, was_trimmed) => {
                if was_trimmed {
                    let pair = (k.clone().into(), v.clone());
                    self.diag.keys_with_trimmed_values.push(pair);
                }
                self.diag.values_with_non_ascii_keys.push((k, v));
                None
            }
            KeyValueResult::NonUtf8Value(k, v) => {
                self.diag.keys_with_non_utf8_values.push((k, v));
                None
            }
            KeyValueResult::BothInvalid(k, v) => {
                self.diag.byte_pairs.push((k, v));
                None
            }
        }
    }

    fn insert_nonunique_std(
        &mut self,
        k: StdKey,
        value: NEString,
        conf: &ReadHeaderAndTEXTConfig,
    ) -> Option<(KeywordInsertError, bool)> {
        Self::insert_nonunique(
            &mut self.std,
            &mut self.diag.non_unique_std_keywords,
            k,
            value,
            conf,
        )
    }

    fn insert_nonunique_pstd(
        &mut self,
        k: PseudoStdKey,
        value: NEString,
        conf: &ReadHeaderAndTEXTConfig,
    ) -> Option<(KeywordInsertError, bool)> {
        Self::insert_nonunique(
            &mut self.pstd,
            &mut self.diag.non_unique_pstd_keywords,
            k,
            value,
            conf,
        )
    }

    fn insert_nonunique_nonstd(
        &mut self,
        k: NonStdKey,
        value: NEString,
        conf: &ReadHeaderAndTEXTConfig,
    ) -> Option<(KeywordInsertError, bool)> {
        Self::insert_nonunique(
            &mut self.nonstd,
            &mut self.diag.non_unique_nonstd_keywords,
            k,
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
    ) -> Option<(KeywordInsertError, bool)>
    where
        K: Hash + Eq + Clone,
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
                flag.is_error().map(|is_err| (err.into(), is_err))
            }
            Entry::Vacant(ent) => {
                ent.insert(value);
                None
            }
        }
    }
}

impl ParsedKeywordsDiagnostic {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn into_flat_diag(
        self,
        header_supp: HeaderAndSuppOffsets,
        primary_text_eof_overflow: u64,
        header_overflows: Vec<HeaderOffsetsOverflow>,
        primary_split: SplitTEXTDiagnostics,
        supp_split: Option<SplitTEXTDiagnostics>,
        read_text_ns: u128,
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
            primary_text_overflow: primary_text_eof_overflow,
            header_overflows,
            byte_pairs,
            non_unique_std_keywords: self.non_unique_std_keywords,
            non_unique_nonstd_keywords: self.non_unique_nonstd_keywords,
            keys_with_empty_trimmed_values: self.keys_with_empty_trimmed_values,
            keys_with_trimmed_values: self.keys_with_trimmed_values,
            primary_split,
            supp_split,
            read_text_ns,
        };
        LogResult::new_ok(ret)
            .extend_deferred_errors(es)
            .set_commutative_warnings(ws)
    }
}

impl ValidKeywords {
    pub(crate) fn get_any(&self, k: &AnyKey) -> Option<&NEString> {
        unimplemented!()
        // match k {
        //     AnyKey::Std(k0) => match k0 {
        //         RealOrPseudoStdKey::Real(k1) => self.get_std(k1),
        //         RealOrPseudoStdKey::Pseudo(k1) => self.get_pstd(k1),
        //     },
        //     AnyKey::NonStd(k0) => self.get_nonstd(k0),
        // }
    }

    pub(crate) fn get_std(&self, k: &StdKey) -> Option<&NEString> {
        self.std.get(k)
    }

    pub(crate) fn get_nonstd(&self, k: &NonStdKey) -> Option<&NEString> {
        self.nonstd.get(k)
    }

    pub(crate) fn transfer_demoted(&mut self, key: StdKey) {
        if let Some(v) = self.std.remove(&key) {
            self.nonstd.insert_demoted(key, v);
        }
    }

    #[allow(clippy::too_many_lines)]
    pub(crate) fn repair(
        &mut self,
        conf: &EvaledReadDataKeywordsConfig,
    ) -> WarningAndErrorResult<RepairDiagnostics, (), RepairCollisionError, RepairCollisionError>
    {
        unimplemented!()
        // let matchers = AllKeyMatchers::from_config(conf);
        // let mut ignored = vec![];
        // let mut non_unique_std = vec![];
        // let mut non_unique_nonstd = vec![];
        // let mut removed = vec![];
        // let mut replaced = vec![];
        // let mut renamed = vec![];
        // let mut subbed = vec![];
        // let mut demoted = vec![];
        // let mut promoted = vec![];

        // // Update standard keys
        // self.std = mem::take(&mut self.std)
        //     .into_iter()
        //     .filter_map(|(k, v)| {
        //         // TODO this seem inefficient; every std key needs to be
        //         // converted to a string to make this work, which doesn't seem
        //         // right
        //         let ks = k.as_keystring();
        //         if matchers.ignore.is_match(&ks) {
        //             // First remove keys that should be flat-out ignored
        //             ignored.push((k, TruncatedNEString(v)));
        //             None
        //         } else if matchers.demote.is_match(&ks) {
        //             // Next remove keys that should be demoted and put them
        //             // in non-std.
        //             let nsk = NonStdKey(ks);
        //             if self.nonstd.contains_key(&nsk) {
        //                 non_unique_nonstd.push((nsk, TruncatedNEString(v)));
        //             } else {
        //                 demoted.push(k);
        //                 let _ = self.nonstd.insert(nsk, v);
        //             }
        //             None
        //         } else if let Some(s) = matchers.subs.get(&ks) {
        //             // Next try to sub the value of keys with matches; this
        //             // might produce a blank key which will effectively remove
        //             // it.
        //             if let Ok(vf) = NEString::try_from(s.sub(v.as_str())) {
        //                 subbed.push((k.clone(), TruncatedNEString(v)));
        //                 Some((k, vf))
        //             } else {
        //                 removed.push((k, TruncatedNEString(v)));
        //                 None
        //             }
        //         } else {
        //             Some((k, v))
        //         }
        //     })
        //     .map(|(k, v)| {
        //         // After removing everything we can, update values as needed.
        //         let replace = &conf.replace_standard_key_values;
        //         let ks = k.as_keystring();
        //         if let Some(vf) = replace.get(&ks).cloned() {
        //             replaced.push((k.clone(), TruncatedNEString(v)));
        //             (k, vf)
        //         } else {
        //             (k, v)
        //         }
        //     })
        //     .map(|(k, v)| {
        //         // Finally, rename keys. Assume that this name mapping is
        //         // validated such that we will never get a name collision.
        //         let to_rename = conf.rename_standard_keys.as_ref();
        //         let ks = k.as_keystring();
        //         if let Some(kf) = to_rename.get(&ks).cloned().map(StdKey) {
        //             renamed.push((k, kf.clone()));
        //             (kf, v)
        //         } else {
        //             (k, v)
        //         }
        //     })
        //     .collect();

        // // Update non-standard keys
        // let nonstd_removed = self
        //     .nonstd
        //     .extract_if(|k, _| matchers.promote.is_match(k.as_ref()));

        // for (k, v) in nonstd_removed {
        //     let sk = StdKey(k.0);
        //     if self.std.contains_key(&sk) {
        //         non_unique_std.push((sk, TruncatedNEString(v)));
        //     } else {
        //         promoted.push(NonStdKey(sk.0.clone()));
        //         let _ = self.std.insert(sk, v);
        //     }
        // }

        // let non_unique_appended = conf.append_standard_keywords.iter().filter_map(|(k, v)| {
        //     match self.std.entry(StdKey(k.clone())) {
        //         Entry::Occupied(e) => Some((e.key().clone(), TruncatedNEString(v.clone()))),
        //         Entry::Vacant(e) => {
        //             e.insert(v.clone());
        //             None
        //         }
        //     }
        // });
        // non_unique_std.extend(non_unique_appended);
        // let res = match conf.allow_repair_non_unique.is_error() {
        //     Some(is_err) => {
        //         let ss = non_unique_std.iter().cloned().map(|(k, _)| AnyKey::Std(k));
        //         let ns = non_unique_nonstd
        //             .iter()
        //             .cloned()
        //             .map(|(k, _)| AnyKey::NonStd(k));
        //         let xs = ss.chain(ns).collect();
        //         if let Some(ne) = NEVec::try_from_vec(xs) {
        //             let e = RepairCollisionError(ne);
        //             if is_err {
        //                 LogResult::new_err(e)
        //             } else {
        //                 LogResult::new_ok(()).set_commutative_warnings(Some(e))
        //             }
        //         } else {
        //             LogResult::new_ok(())
        //         }
        //     }
        //     None => LogResult::new_ok(()),
        // };

        // let ret = RepairDiagnostics {
        //     non_unique_std,
        //     non_unique_nonstd,
        //     demoted,
        //     promoted,
        //     subbed,
        //     replaced,
        //     renamed,
        //     ignored,
        //     removed,
        // };
        // res.set_ok_value(ret)
    }

    pub(crate) fn remove_optical_only(
        &mut self,
        targets: &[OpticalOnlyKey],
        keys: &OpticalOnlyKeys,
        i: MeasIndex,
        flag: ProcessOpticalOnlyKeys,
    ) -> OpticalOnlyResult {
        let mut es = vec![];
        let mut ws = vec![];
        let mut pairs = vec![];
        for t in targets {
            let k = StdKey::from_optical_only_key(*t, i);
            let (demote, warn) = match flag {
                ProcessOpticalOnlyKeys::DemoteWarn => (true, true),
                ProcessOpticalOnlyKeys::DemoteSilent => (true, false),
                ProcessOpticalOnlyKeys::DropWarn => (false, true),
                ProcessOpticalOnlyKeys::DropSilent => (false, false),
            };
            if let Some(v) = self.std.remove(&k) {
                let err = || TemporalHasOpticalKeyError::new(i, *t);
                if keys.0.contains(t) {
                    if demote {
                        self.nonstd.insert_demoted(k.clone(), v.clone());
                    }
                    if warn {
                        ws.push(err());
                    }
                    pairs.push((k, v));
                } else {
                    es.push(err());
                }
            }
        }
        let mut res = LogResult::new_from_err_iter(es, pairs, ());
        res.extend_commutative_warnings(ws);
        res
    }
}

// Declare misc free functions and constants

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

fn trunc_str(s: &str) -> String {
    let escape = |c| {
        let esc = |x| OneOrTwo::Two('\\', x);
        match c {
            '\0' => esc('0'),
            '\x07' => esc('a'),
            '\x08' => esc('b'),
            '\x09' => esc('t'),
            '\x0a' => esc('n'),
            '\x0b' => esc('v'),
            '\x0c' => esc('f'),
            '\x0d' => esc('r'),
            '\x1b' => esc('e'),
            x => OneOrTwo::One(x),
        }
    };
    let n = s.chars().count();
    if n > TRUNCATED_STR_LIMIT {
        let t: String = s.chars().take(n).flat_map(escape).collect();
        format!("{t}…(more)")
    } else {
        s.chars().flat_map(escape).collect()
    }
}

fn has_no_std_prefix(xs: &[u8]) -> bool {
    xs.first().is_some_and(|x| *x != STD_PREFIX)
}

const TRUNCATED_BYTES_LIMIT: usize = 20;
const TRUNCATED_STR_LIMIT: usize = 20;

#[cfg(feature = "serde")]
mod serialize {
    use fireflow_types::nonempty_string::NEString;

    use hashbrown::HashMap;
    use serde::Serialize;

    use std::collections::BTreeMap;

    pub(super) fn ordered_map<K, S>(
        value: &HashMap<K, NEString>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
        K: Serialize + Clone + Ord,
    {
        let ordered: BTreeMap<K, _> = value.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        ordered.serialize(serializer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fireflow_types::keystring::NEAsciiStringError;
    use nonempty_collections::NESlice;

    use proptest::prelude::*;

    const STD_KEY_STRAT: &str = "\\$[[:print:]]+";
    const NONSTD_KEY_STRAT: &str = "[[:print:]&&[^\\$]]\\$[[:print:]]*";

    impl Arbitrary for StdKey {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;
        fn arbitrary_with((): Self::Parameters) -> Self::Strategy {
            STD_KEY_STRAT.prop_map(|s| s.parse().unwrap()).boxed()
        }
    }

    impl Arbitrary for NonStdKey {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;
        fn arbitrary_with((): Self::Parameters) -> Self::Strategy {
            NONSTD_KEY_STRAT.prop_map(|s| s.parse().unwrap()).boxed()
        }
    }

    // TODO test various configurations for insertion

    proptest! {
        #[test]
        fn insert_std_key(s in STD_KEY_STRAT, v in any::<NEString>()) {
            let k = s.parse::<StdKey>().unwrap();
            let conf = ReadHeaderAndTEXTConfig::default();
            let mut p = ParsedKeywords::default();
            let res = p.insert(
                &NESlice::try_from_slice(s.as_bytes()).unwrap(),
                &v.as_ne_bytes(),
                Encoding::Utf8,
                &conf,
            );
            assert_eq!(None, res);
            assert_eq!(p.std.get(&k).map(NEString::as_ne_str), Some(v.as_ne_str()));
        }
    }

    proptest! {
        #[test]
        fn insert_nonstd_key(s in NONSTD_KEY_STRAT, v in any::<NEString>()) {
            let k = s.parse::<NonStdKey>().unwrap();
            let conf = ReadHeaderAndTEXTConfig::default();
            let mut p = ParsedKeywords::default();
            let res = p.insert(
                &NESlice::try_from_slice(s.as_bytes()).unwrap(),
                &v.as_ne_bytes(),
                Encoding::Utf8,
                &conf,
            );
            assert_eq!(None, res);
            assert_eq!(p.nonstd.get(&k).map(NEString::as_ne_str), Some(v.as_ne_str()));
        }
    }

    proptest! {
        #[test]
        fn fromstr_std_key(s in STD_KEY_STRAT) {
            // std key should always be stored without the dollar sign
            let k = s.parse::<StdKey>().expect("strategy should be valid");
            let s_noprefix = s.as_str().split_at(1).1;
            let k_str: &str = k.as_ref();
            assert_eq!(k_str, s_noprefix);
            // reverse process should produce same string (with $)
            assert_eq!(k.to_string(), s);
        }
    }

    proptest! {
        #[test]
        fn fromstr_nonstd_key(s in NONSTD_KEY_STRAT) {
            // nonstd key should always match the input
            let k = s.parse::<NonStdKey>().expect("strategy should be valid");
            let k_str: &str = k.as_ref();
            assert_eq!(k_str, s);
            // reverse process should produce same string (without $)
            assert_eq!(k.to_string(), s);
        }
    }

    #[test]
    fn fromstr_std_key_nonascii() {
        let s = "$花冷え。"; // sugarsugarsugarsugarsugarsugarrrrrrrrr...
        let k = s.parse::<StdKey>();
        let e = StdKeyError::Ascii(NEAsciiStringError::Ascii(s.parse().unwrap()));
        assert_eq!(Err(e), k);
    }

    proptest! {
        #[test]
        fn fromstr_std_key_noprefix(s in "[[:print:]&&[^\\$]][[:print:]]") {
            let k = s.parse::<StdKey>();
            let e = StdKeyError::Prefix(s.parse().unwrap());
            assert_eq!(Err(e), k);
        }
    }

    #[test]
    fn fromstr_std_key_blank() {
        let s = "";
        let k = s.parse::<StdKey>();
        assert_eq!(Err(StdKeyError::Ascii(NEAsciiStringError::Empty)), k);
    }

    #[test]
    fn fromstr_std_key_onlyprefix() {
        let s = "$";
        let k = s.parse::<StdKey>();
        assert_eq!(Err(StdKeyError::Empty), k);
    }

    #[test]
    fn fromstr_nonstd_key_nonascii() {
        let s = "サイ";
        let k = s.parse::<NonStdKey>();
        let e = NonStdKeyError::Ascii(NEAsciiStringError::Ascii(s.parse().unwrap()));
        assert_eq!(Err(e), k);
    }

    proptest! {
        #[test]
        fn fromstr_nonstd_key_hasprefix(s in "\\$[[:print:]]") {
            let k = s.parse::<NonStdKey>();
            let e = NonStdKeyError::Prefix(s.parse().unwrap());
            assert_eq!(Err(e), k);
        }
    }

    #[test]
    fn fromstr_nonstd_key_blank() {
        let s = "";
        let k = s.parse::<NonStdKey>();
        assert_eq!(Err(NonStdKeyError::Ascii(NEAsciiStringError::Empty)), k);
    }
}

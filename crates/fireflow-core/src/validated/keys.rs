use crate::fixed_vec::OneOrTwo;
use crate::std_index::index::StdKeywords;
use crate::text::keyword_enum::{
    AsStdKeywordPair, OptMeasKeyword, OptRootKeyword, ambassador_impl_AsStdKeywordPair,
};

use fireflow_types::{
    config::Encoding,
    index::{BiMeasIndex, MeasIndex},
    keystring::{KeyString, NEAsciiStringError},
    ne_str, nev,
    nonempty::{
        HasNELen, NEAlt, NEConcat, NESlice, NEStr, NEString, NEVec, ToDisplayNE, ToNE,
        ambassador_impl_ToDisplayNE,
    },
    std_key::{PseudoStdKey, RealOrPseudoStdKey, STD_PREFIX, StdKey, ToStd},
};

use ambassador::Delegate;
use derive_more::{AsRef, Display, From, Into};
use derive_new::new;
use derive_where::derive_where;
use hashbrown::HashMap;
use hashbrown::hash_map::Entry;
use thiserror::Error;

use std::borrow::Cow;
use std::fmt;
use std::hash::Hash;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::string::ToString;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{DisplayAsPyErr, FromInnerPyObject, FromPyString, IntoPyString},
    fireflow_types::python as py,
    pyo3::prelude::*,
};

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

#[derive(Default)]
struct ParsedKeywords0 {
    /// Standard keywords (with '$')
    pub(crate) std: HashMap<StdKey, NEString>,

    /// Pseudostandard keywords (with '$' but not part of standard)
    pub(crate) pstd: HashMap<PseudoStdKey, NEString>,

    /// Non-standard keywords (without '$')
    pub(crate) nonstd: NonStdKeywords,

    /// Keywords that failed for some reason.
    pub(crate) diag: ParsedKeywordsDiagnostic,
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
    pub std: StdKeywords,
    #[cfg_attr(feature = "serde", serde(serialize_with = "serialize::ordered_map"))]
    pub pstd: PseudoStdKeywords,
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

    pub(crate) fn into_ne(self) -> Option<NEStringOrBytes> {
        match self {
            Self::Bytes(x) => x.into_ne().map(NEStringOrBytes::Bytes),
            Self::Utf8(x) => x.into_ne().map(NEStringOrBytes::Utf8),
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

impl From<NEStringOrBytes> for StringOrBytes {
    fn from(value: NEStringOrBytes) -> Self {
        match value {
            NEStringOrBytes::Bytes(x) => Self::Bytes(x.into()),
            NEStringOrBytes::Utf8(x) => Self::Utf8(x.into()),
        }
    }
}

impl<'a> From<&'a NESlice<u8>> for NEStringOrBytes {
    fn from(value: &'a NESlice<u8>) -> Self {
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

impl TruncatedBytes {
    pub(crate) fn into_ne(self) -> Option<TruncatedNEBytes> {
        NEVec::try_from_vec(self.0).map(TruncatedNEBytes)
    }
}

/// A [`NEVec<u8>`] optimized for displaying in errors.
#[derive(Clone, From, PartialEq, Debug, Display, Into)]
#[display("{}", trunc_bytes(self.0.as_ref()))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[from(NEVec<u8>)]
#[into(Vec<u8>, NEVec<u8>)]
#[repr(transparent)]
pub struct TruncatedNEBytes(pub NEVec<u8>);

impl From<TruncatedNEBytes> for TruncatedBytes {
    fn from(value: TruncatedNEBytes) -> Self {
        Self::from(Vec::from(value))
    }
}

impl<'a> From<&'a NESlice<u8>> for TruncatedNEBytes {
    fn from(value: &'a NESlice<u8>) -> Self {
        Self::from(value.to_ne_vec())
    }
}

/// A normal [`String`] that will be shortened when displaying if too long.
#[derive(Clone, From, PartialEq, Debug, Display, Default)]
#[display("{}", trunc_str(self.0.as_ref()))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct TruncatedString(pub String);

impl TruncatedString {
    pub(crate) fn into_ne(self) -> Option<TruncatedNEString> {
        NEString::try_from(self.0).ok().map(TruncatedNEString)
    }
}

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

pub type PseudoStdKeywords = HashMap<PseudoStdKey, NEString>;

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
    pub(crate) keys_with_empty_trimmed_values: Vec<(KeyOrBytes, TruncatedNEString)>,

    /// Keys with values that were trimmed
    ///
    /// The value included here is the original value.
    pub(crate) keys_with_trimmed_values: Vec<(KeyOrBytes, TruncatedNEString)>,
}

#[derive(Default)]
pub(crate) struct ParsedNonStdKeywords {
    /// Nonstandard keywords (without '$').
    pub(crate) nonstd: NonStdKeywords,

    /// Pseudostdandard keywords (with '$' but still non-standard).
    pub(crate) pstd: PseudoStdKeywords,
}

// Declare traits which map rust values to standardized keywords.

pub trait ValueToStdKey {
    type Index;
    type Id: ToStd<Index = Self::Index>;
    const STD: Self::Id;

    fn std(index: &Self::Index) -> StdKey {
        Self::STD.to_std(index)
    }

    fn std_(&self, index: &Self::Index) -> StdKey {
        Self::std(index)
    }

    fn std0() -> StdKey
    where
        Self: ValueToStdKey<Index = ()>,
    {
        Self::std(&())
    }

    fn std0_(&self) -> StdKey
    where
        Self: ValueToStdKey<Index = ()>,
    {
        self.std_(&())
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

// Implement methods for misc types

/// Insert a key and value from buffer into appropriate hash table.
///
/// Return value will be Some((X, false)) if a warning as emitted, Some((X,
/// true)) if an error was emitted, and None if neither was emitted.
#[allow(clippy::too_many_lines)]
impl ParsedKeywords {
    // pub(crate) fn insert(
    //     &mut self,
    //     key: &NESlice<u8>,
    //     val: &NESlice<u8>,
    //     encoding: Encoding,
    //     conf: &ReadHeaderAndTEXTConfig,
    // ) -> Option<(KeywordInsertError, bool)> {
    //     enum TrimResult<'a> {
    //         Trimmed(Cow<'a, NEStr>, bool),
    //         Empty(DummyTriFlag),
    //     }

    //     enum KeyValueResult<'a> {
    //         Empty(KeyOrBytes, DummyTriFlag),
    //         NonEmpty(AnyKey, Cow<'a, NEStr>, bool),
    //         NonUtf8Value(AnyKey, TruncatedNEBytes),
    //         NonAsciiKey(TruncatedNEBytes, TruncatedNEString, bool),
    //         BothInvalid(TruncatedNEBytes, TruncatedNEBytes),
    //     }

    //     let parse_key = |s: &NESlice<u8>| {
    //         let single_byte = matches!(encoding, Encoding::Single);
    //         if let Some((&STD_PREFIX, rest)) = s.as_ref().split_first() {
    //             // TODO we may wish to distinguish an error between non-ASCII
    //             // and only a '$' keyword
    //             let ne = NESlice::try_from_slice(rest)?;
    //             let k = RealOrPseudoStdKey::from_bytes_maybe(&ne)?;
    //             Some(AnyKey::Std(k))
    //         } else {
    //             let k = KeyString::from_bytes_maybe(s, single_byte)?;
    //             Some(AnyKey::NonStd(NonStdKey(k)))
    //         }
    //     };

    //     let parse_value = || {
    //         let flag = conf.trim_value_whitespace;
    //         let triflag = DummyTriFlag::from_trim_value_whitespace(flag);
    //         match encoding {
    //             Encoding::Single => {
    //                 if let Some(tf) = triflag {
    //                     if let Some(ne) = val
    //                         .as_ref()
    //                         .trim_ascii()
    //                         .iter()
    //                         .copied()
    //                         .map(char::from)
    //                         .try_into_nonempty_iter()
    //                     {
    //                         let s: NEString = ne.collect();
    //                         let was_trimmed = val.len() < s.len();
    //                         Some(TrimResult::Trimmed(Cow::Owned(s), was_trimmed))
    //                     } else {
    //                         Some(TrimResult::Empty(tf))
    //                     }
    //                 } else {
    //                     let it = val.into_nonempty_iter().copied().map(char::from);
    //                     Some(TrimResult::Trimmed(Cow::Owned(it.collect()), false))
    //                 }
    //             }
    //             Encoding::Utf8 => {
    //                 if let Ok(vv) = NEStr::from_utf8(val) {
    //                     if let Some(tf) = triflag {
    //                         if let Some(trimmed) = NEStr::try_new(vv.as_str().trim()) {
    //                             let was_trimmed = trimmed.len() < vv.len();
    //                             Some(TrimResult::Trimmed(Cow::Borrowed(trimmed), was_trimmed))
    //                         } else {
    //                             Some(TrimResult::Empty(tf))
    //                         }
    //                     } else {
    //                         Some(TrimResult::Trimmed(Cow::Borrowed(vv), false))
    //                     }
    //                 } else {
    //                     None
    //                 }
    //             }
    //         }
    //     };

    //     let kv_res = if let Some(parsed) = parse_key(key) {
    //         match parsed {
    //             AnyKey::Std(k) => {
    //                 if let Some(trim_res) = parse_value() {
    //                     match trim_res {
    //                         TrimResult::Empty(flag) => {
    //                             KeyValueResult::Empty(AnyKey::Std(k).into(), flag)
    //                         }
    //                         TrimResult::Trimmed(value, was_trimmed) => {
    //                             KeyValueResult::NonEmpty(k.into(), value, was_trimmed)
    //                         }
    //                     }
    //                 } else {
    //                     KeyValueResult::NonUtf8Value(k.into(), TruncatedNEBytes::from(val))
    //                 }
    //             }
    //             AnyKey::NonStd(k) => {
    //                 // Non-standard key: does not start with '$' and is ASCII
    //                 if let Some(trim_res) = parse_value() {
    //                     match trim_res {
    //                         TrimResult::Empty(flag) => {
    //                             KeyValueResult::Empty(AnyKey::NonStd(k).into(), flag)
    //                         }
    //                         TrimResult::Trimmed(value, was_trimmed) => {
    //                             KeyValueResult::NonEmpty(k.into(), value, was_trimmed)
    //                         }
    //                     }
    //                 } else {
    //                     KeyValueResult::NonUtf8Value(AnyKey::NonStd(k), TruncatedNEBytes::from(val))
    //                 }
    //             }
    //         }
    //     } else {
    //         // Non-ascii key with possibly non-Utf-8 value
    //         let kbytes = TruncatedNEBytes::from(key);
    //         if let Some(trim_res) = parse_value() {
    //             match trim_res {
    //                 TrimResult::Empty(flag) => {
    //                     KeyValueResult::Empty(KeyOrBytes::from(kbytes), flag)
    //                 }
    //                 TrimResult::Trimmed(value, was_trimmed) => {
    //                     let tv = value.into_owned().into();
    //                     KeyValueResult::NonAsciiKey(kbytes, tv, was_trimmed)
    //                 }
    //             }
    //         } else {
    //             KeyValueResult::BothInvalid(kbytes, val.into())
    //         }
    //     };

    //     match kv_res {
    //         KeyValueResult::NonEmpty(k, v, was_trimmed) => {
    //             if was_trimmed {
    //                 let vo = TruncatedNEString(v.clone().into_owned());
    //                 let pair = (k.clone().into(), vo);
    //                 self.diag.keys_with_trimmed_values.push(pair);
    //             }
    //             let vo = v.into_owned();
    //             match k {
    //                 AnyKey::Std(k) => {
    //                     match k {
    //                         RealOrPseudoStdKey::Pseudo(p) => {
    //                             self.insert_nonunique_pstd(p, vo, conf);
    //                         }
    //                         RealOrPseudoStdKey::Real(p) => {
    //                             self.insert_nonunique_std(p, vo, conf);
    //                         }
    //                     }
    //                     None
    //                 }
    //                 AnyKey::NonStd(k) => self.insert_nonunique_nonstd(k, vo, conf),
    //             }
    //         }
    //         KeyValueResult::Empty(k, flag) => {
    //             self.diag.keys_with_empty_trimmed_values.push(k.clone());
    //             let e = KeywordInsertError::from(BlankValueError(k));
    //             flag.is_error().map(|is_err| (e, is_err))
    //         }
    //         KeyValueResult::NonAsciiKey(k, v, was_trimmed) => {
    //             if was_trimmed {
    //                 let pair = (k.clone().into(), v.clone());
    //                 self.diag.keys_with_trimmed_values.push(pair);
    //             }
    //             self.diag.values_with_non_ascii_keys.push((k, v));
    //             None
    //         }
    //         KeyValueResult::NonUtf8Value(k, v) => {
    //             self.diag.keys_with_non_utf8_values.push((k, v));
    //             None
    //         }
    //         KeyValueResult::BothInvalid(k, v) => {
    //             self.diag.byte_pairs.push((k, v));
    //             None
    //         }
    //     }
    // }

    // fn insert_nonunique_std(
    //     &mut self,
    //     k: StdKey,
    //     value: NEString,
    //     conf: &ReadHeaderAndTEXTConfig,
    // ) -> Option<(KeywordInsertError, bool)> {
    //     Self::insert_nonunique(
    //         &mut self.std,
    //         &mut self.diag.non_unique_std_keywords,
    //         k,
    //         value,
    //         conf,
    //     )
    // }

    // fn insert_nonunique_pstd(
    //     &mut self,
    //     k: PseudoStdKey,
    //     value: NEString,
    //     conf: &ReadHeaderAndTEXTConfig,
    // ) -> Option<(KeywordInsertError, bool)> {
    //     Self::insert_nonunique(
    //         &mut self.pstd,
    //         &mut self.diag.non_unique_pseudostd_keywords,
    //         k,
    //         value,
    //         conf,
    //     )
    // }

    // fn insert_nonunique_nonstd(
    //     &mut self,
    //     k: NonStdKey,
    //     value: NEString,
    //     conf: &ReadHeaderAndTEXTConfig,
    // ) -> Option<(KeywordInsertError, bool)> {
    //     Self::insert_nonunique(
    //         &mut self.nonstd,
    //         &mut self.diag.non_unique_nonstd_keywords,
    //         k,
    //         value,
    //         conf,
    //     )
    // }

    // fn insert_nonunique<K>(
    //     kws: &mut HashMap<K, NEString>,
    //     nonunique: &mut Vec<(K, TruncatedNEString)>,
    //     k: K,
    //     value: NEString,
    //     conf: &ReadHeaderAndTEXTConfig,
    // ) -> Option<(KeywordInsertError, bool)>
    // where
    //     K: Hash + Eq + Clone,
    //     KeywordInsertError: From<KeyPresent<K>>,
    // {
    //     let flag = conf.allow_nonunique;
    //     match kws.entry(k) {
    //         Entry::Occupied(ent) => {
    //             let key = ent.key().clone();
    //             let err = KeyPresent {
    //                 key: key.clone(),
    //                 value: value.clone(),
    //             };
    //             nonunique.push((key, TruncatedNEString(value)));
    //             flag.is_error().map(|is_err| (err.into(), is_err))
    //         }
    //         Entry::Vacant(ent) => {
    //             ent.insert(value);
    //             None
    //         }
    //     }
    // }
}

pub(crate) enum ParsedKeyword<'a> {
    // Valid std key value as a slice
    StdSlice(NonEmptyValue<StdKey, &'a NEStr>),
    // Valid std key value as owned value (used for values with latin1
    // characters and escaped delimiters)
    StdOwned(NonEmptyValue<StdKey, NEString>),
    // Valid non-std key and valid
    NonStd(NonEmptyValue<NonStdKey, NEString>),
    // Pseudostd key and value
    Pseudo(NonEmptyValue<PseudoStdKey, NEString>),
    // Key (any type or raw bytes) where value was trimmed to empty whitespace
    TrimmedEmptyValue(ParsedKey, NEString),
    // Valid key with invalid value
    NonUtf8Value(AnyKey, NEVec<u8>),
    // Invalid key with valid value
    NonAsciiKey(NonEmptyValue<NEVec<u8>, NEString>),
    // Invalid pair
    BothInvalid(NEVec<u8>, NEVec<u8>),
}

#[derive(new)]
pub(crate) struct NonEmptyValue<K, V> {
    pub(crate) key: K,
    pub(crate) value: V,
    pub(crate) original: Option<NEString>,
}

#[derive(Clone)]
enum ParsedKey {
    Std(StdKey),
    Pseudo(PseudoStdKey),
    NonStd(NonStdKey),
    Bytes(NEVec<u8>),
}

impl From<ParsedKey> for KeyOrBytes {
    fn from(value: ParsedKey) -> Self {
        match value {
            ParsedKey::Bytes(x) => Self::Bytes(TruncatedNEBytes(x)),
            ParsedKey::Pseudo(x) => Self::Ascii(AnyKey::Std(RealOrPseudoStdKey::Pseudo(x))),
            ParsedKey::Std(x) => Self::Ascii(AnyKey::Std(RealOrPseudoStdKey::Real(x))),
            ParsedKey::NonStd(x) => Self::Ascii(AnyKey::NonStd(x)),
        }
    }
}

impl ParsedKey {
    fn from_bytes(bytes: &NESlice<u8>, encoding: Encoding) -> Self {
        let single_byte = matches!(encoding, Encoding::Single);
        // TODO we may wish to distinguish an error between non-ASCII and only a
        // '$' keyword
        if let Some((&STD_PREFIX, rest)) = bytes.as_ref().split_first() {
            if let Some(ne) = NESlice::try_from_slice(rest) {
                if let Some(k) = RealOrPseudoStdKey::from_bytes_maybe(&ne) {
                    match k {
                        RealOrPseudoStdKey::Real(x) => Self::Std(x),
                        RealOrPseudoStdKey::Pseudo(x) => Self::Pseudo(x),
                    }
                } else {
                    Self::Bytes(bytes.to_ne_vec())
                }
            } else {
                Self::Bytes(nev![STD_PREFIX])
            }
        } else if let Some(k) = KeyString::from_bytes_maybe(bytes, single_byte) {
            Self::NonStd(NonStdKey(k))
        } else {
            Self::Bytes(bytes.to_ne_vec())
        }
    }
}

enum ParsedValue<'a> {
    Slice(&'a NEStr, Option<NEString>),
    Owned(NEString, Option<NEString>),
    Bytes(NEVec<u8>),
    Empty(NEString),
}

impl<'a> ParsedValue<'a> {
    fn into_owned<'b>(self) -> ParsedValue<'b> {
        match self {
            Self::Slice(s, o) => ParsedValue::Owned(s.to_owned(), o),
            Self::Owned(s, o) => ParsedValue::Owned(s, o),
            Self::Bytes(b) => ParsedValue::Bytes(b),
            Self::Empty(e) => ParsedValue::Empty(e),
        }
    }
}

pub(crate) trait ValueFromBytes<'a> {
    fn parse_from_bytes(self, trim: bool, encoding: Encoding) -> ParsedValue<'a>;
}

impl<'a> ValueFromBytes<'a> for &'a NESlice<u8> {
    fn parse_from_bytes(self, trim: bool, encoding: Encoding) -> ParsedValue<'a> {
        match encoding {
            Encoding::Single => {
                if trim {
                    if let Some(trimmed) = NESlice::try_from_slice(self.trim_latin1()) {
                        let original =
                            (trimmed.ne_len() < self.ne_len()).then(|| self.to_latin1_string());
                        if let Ok(v) = NEStr::from_utf8(trimmed) {
                            ParsedValue::Slice(v, original)
                        } else {
                            ParsedValue::Owned(trimmed.to_latin1_string(), original)
                        }
                    } else {
                        ParsedValue::Empty(self.to_latin1_string())
                    }
                } else if let Ok(v) = NEStr::from_utf8(self) {
                    ParsedValue::Slice(v, None)
                } else {
                    ParsedValue::Owned(self.to_latin1_string(), None)
                }
            }
            Encoding::Utf8 => {
                if let Ok(v) = NEStr::from_utf8(self) {
                    if trim {
                        if let Some(trimmed) = NEStr::try_new(v.trim_ascii()) {
                            let original = (trimmed.ne_len() < v.ne_len()).then(|| v.to_owned());
                            ParsedValue::Slice(trimmed, original)
                        } else {
                            ParsedValue::Empty(v.to_owned())
                        }
                    } else {
                        ParsedValue::Slice(v, None)
                    }
                } else {
                    ParsedValue::Bytes(self.to_ne_vec())
                }
            }
        }
    }
}

impl<'a> ValueFromBytes<'a> for NEDelimBytes<'a> {
    fn parse_from_bytes(self, trim: bool, encoding: Encoding) -> ParsedValue<'a> {
        match self.as_cow() {
            Cow::Borrowed(x) => x.parse_from_bytes(trim, encoding),
            Cow::Owned(x) => AsRef::<NESlice<u8>>::as_ref(&x)
                .parse_from_bytes(trim, encoding)
                .into_owned(),
        }
    }
}

pub(crate) struct NEDelimBytes<'a> {
    pub(crate) first: &'a NESlice<u8>,
    pub(crate) rest: Vec<(NonZeroUsize, &'a NESlice<u8>)>,
    pub(crate) delim: u8,
}

impl<'a> NEDelimBytes<'a> {
    pub(crate) fn init(first: &'a NESlice<u8>, delim: u8) -> Self {
        Self {
            first,
            rest: vec![],
            delim,
        }
    }

    pub(crate) fn append(&mut self, x: &'a NESlice<u8>, ndelim: NonZeroUsize) {
        self.rest.push((ndelim, x));
    }

    fn as_cow(&self) -> Cow<'a, NESlice<u8>> {
        if self.rest.is_empty() {
            Cow::Borrowed(self.first)
        } else {
            Cow::Owned(self.into_owned())
        }
    }

    pub(crate) fn into_owned(&self) -> NEVec<u8> {
        let mut buf = self.first.to_owned();
        for (n_delims, sub) in self.rest.iter().copied() {
            for _ in 0..n_delims.get() {
                buf.push(self.delim);
            }
            buf.extend(sub.iter().copied());
        }
        buf
    }
}

impl<'a> ParsedKeyword<'a> {
    pub(crate) fn from_pair<V>(key: &NESlice<u8>, val: V, trim: bool, encoding: Encoding) -> Self
    where
        V: ValueFromBytes<'a>,
    {
        let pk = ParsedKey::from_bytes(key, encoding);
        let pv = val.parse_from_bytes(trim, encoding);
        // This will throw away the trimmed value if it was computed in the case
        // of non-ascii keys. This is very rare so probably not worth
        // optimizing. The convenience of returning owned strings is worth it.
        match (pk, pv) {
            (ParsedKey::Std(k), ParsedValue::Slice(v, original)) => {
                Self::StdSlice(NonEmptyValue::new(k, v, original))
            }
            (ParsedKey::Std(k), ParsedValue::Owned(v, original)) => {
                Self::StdOwned(NonEmptyValue::new(k, v, original))
            }
            (ParsedKey::Pseudo(k), ParsedValue::Slice(v, original)) => {
                Self::Pseudo(NonEmptyValue::new(k, v.to_owned(), original))
            }
            (ParsedKey::Pseudo(k), ParsedValue::Owned(v, original)) => {
                Self::Pseudo(NonEmptyValue::new(k, v, original))
            }
            (ParsedKey::NonStd(k), ParsedValue::Slice(v, original)) => {
                Self::NonStd(NonEmptyValue::new(k, v.to_owned(), original))
            }
            (ParsedKey::NonStd(k), ParsedValue::Owned(v, original)) => {
                Self::NonStd(NonEmptyValue::new(k, v, original))
            }
            (ParsedKey::Bytes(k), ParsedValue::Slice(v, original)) => {
                Self::NonAsciiKey(NonEmptyValue::new(k, v.to_owned(), original))
            }
            (ParsedKey::Bytes(k), ParsedValue::Owned(v, original)) => {
                Self::NonAsciiKey(NonEmptyValue::new(k, v, original))
            }
            (ParsedKey::Bytes(k), ParsedValue::Bytes(v)) => Self::BothInvalid(k, v),
            (ParsedKey::Std(k), ParsedValue::Bytes(v)) => {
                Self::NonUtf8Value(AnyKey::Std(RealOrPseudoStdKey::Real(k)), v)
            }
            (ParsedKey::Pseudo(k), ParsedValue::Bytes(v)) => {
                Self::NonUtf8Value(AnyKey::Std(RealOrPseudoStdKey::Pseudo(k)), v)
            }
            (ParsedKey::NonStd(k), ParsedValue::Bytes(v)) => {
                Self::NonUtf8Value(AnyKey::NonStd(k), v)
            }
            (k, ParsedValue::Empty(original)) => Self::TrimmedEmptyValue(k, original),
        }
    }

    pub(crate) fn dispatch_slice_only(
        self,
        std: &mut Vec<(StdKey, &'a NEStr)>,
        nonstd: &mut ParsedNonStdKeywords,
        diag: &mut ParsedKeywordsDiagnostic,
    ) {
        let f_owned = |_| panic!("this should only be called when input is all slices");
        self.dispatch(std, nonstd, diag, |v| v, f_owned);
    }

    pub(crate) fn dispatch_slice_or_owned(
        self,
        std: &mut Vec<(StdKey, Cow<'a, NEStr>)>,
        nonstd: &mut ParsedNonStdKeywords,
        diag: &mut ParsedKeywordsDiagnostic,
    ) {
        self.dispatch(std, nonstd, diag, Cow::Borrowed, Cow::Owned);
    }

    fn dispatch<F0, F1, V>(
        self,
        std: &mut Vec<(StdKey, V)>,
        nonstd: &mut ParsedNonStdKeywords,
        diag: &mut ParsedKeywordsDiagnostic,
        f_slice: F0,
        f_owned: F1,
    ) where
        F0: FnOnce(&'a NEStr) -> V,
        F1: FnOnce(NEString) -> V,
    {
        match self {
            Self::StdSlice(kv) => {
                if let Some(o) = kv.original {
                    let k = AnyKey::Std(RealOrPseudoStdKey::Real(kv.key.clone()));
                    diag.keys_with_trimmed_values
                        .push((KeyOrBytes::from(k), o.into()));
                }
                std.push((kv.key, f_slice(kv.value)));
            }
            Self::StdOwned(kv) => {
                if let Some(o) = kv.original {
                    let k = AnyKey::Std(RealOrPseudoStdKey::Real(kv.key.clone()));
                    diag.keys_with_trimmed_values
                        .push((KeyOrBytes::from(k), o.into()));
                }
                std.push((kv.key, f_owned(kv.value)));
            }
            Self::NonStd(kv) => {
                if let Some(o) = kv.original {
                    let k = AnyKey::NonStd(kv.key.clone());
                    diag.keys_with_trimmed_values
                        .push((KeyOrBytes::from(k), o.into()));
                }
                match nonstd.nonstd.entry(kv.key) {
                    Entry::Occupied(e) => diag
                        .non_unique_nonstd_keywords
                        .push((e.key().clone(), kv.value.into())),
                    Entry::Vacant(e) => {
                        let _ = e.insert(kv.value);
                    }
                }
            }
            Self::Pseudo(kv) => {
                if let Some(o) = kv.original {
                    let k = AnyKey::Std(RealOrPseudoStdKey::Pseudo(kv.key.clone()));
                    diag.keys_with_trimmed_values
                        .push((KeyOrBytes::from(k), o.into()));
                }
                match nonstd.pstd.entry(kv.key) {
                    Entry::Occupied(e) => diag
                        .non_unique_pstd_keywords
                        .push((e.key().clone(), kv.value.into())),
                    Entry::Vacant(e) => {
                        let _ = e.insert(kv.value);
                    }
                }
            }
            Self::TrimmedEmptyValue(k, v) => {
                diag.keys_with_empty_trimmed_values
                    .push((k.clone().into(), v.into()));
            }
            Self::NonUtf8Value(k, v) => diag.keys_with_non_utf8_values.push((k, v.into())),
            Self::NonAsciiKey(kv) => {
                if let Some(o) = kv.original {
                    let k = TruncatedNEBytes::from(kv.key.clone());
                    diag.keys_with_trimmed_values
                        .push((KeyOrBytes::from(k), o.into()));
                }
                diag.values_with_non_ascii_keys
                    .push((kv.key.into(), kv.value.into()))
            }
            Self::BothInvalid(k, v) => diag.byte_pairs.push((k.into(), v.into())),
        }
    }

    pub(crate) fn count(&self, counts: &mut ParsedKeywordCounts) {
        match self {
            Self::StdSlice(kv) => {
                counts.n_std_slice_kws += 1;
                counts.n_trimmed += usize::from(kv.original.is_some());
            }
            Self::StdOwned(kv) => {
                counts.n_std_owned_kws += 1;
                counts.n_trimmed += usize::from(kv.original.is_some());
            }
            Self::NonStd(kv) => {
                counts.n_nonstd_keys += 1;
                counts.n_trimmed += usize::from(kv.original.is_some())
            }
            Self::Pseudo(kv) => {
                counts.n_pseudo_keys += 1;
                counts.n_trimmed += usize::from(kv.original.is_some())
            }
            Self::TrimmedEmptyValue(_, _) => counts.n_trimmed_empty_values += 1,
            Self::NonUtf8Value(_, _) => counts.n_non_utf8_values += 1,
            Self::NonAsciiKey(_) => counts.n_non_ascii_keys += 1,
            Self::BothInvalid(_, _) => counts.n_invalid_pairs += 1,
        }
    }
}

#[derive(Default)]
pub(crate) struct ParsedKeywordCounts {
    pub(crate) n_std_slice_kws: usize,
    pub(crate) n_std_owned_kws: usize,
    pub(crate) n_nonstd_keys: usize,
    pub(crate) n_pseudo_keys: usize,
    pub(crate) n_trimmed_empty_values: usize,
    pub(crate) n_non_utf8_values: usize,
    pub(crate) n_non_ascii_keys: usize,
    pub(crate) n_invalid_pairs: usize,
    pub(crate) n_trimmed: usize,
}

impl ParsedNonStdKeywords {
    pub(crate) fn reserve(&mut self, counts: &ParsedKeywordCounts) {
        self.nonstd.reserve(counts.n_nonstd_keys);
        self.pstd.reserve(counts.n_pseudo_keys);
    }
}

impl ParsedKeywordsDiagnostic {
    pub(crate) fn reserve(&mut self, counts: &ParsedKeywordCounts) {
        self.keys_with_non_utf8_values
            .reserve(counts.n_non_utf8_values);
        self.values_with_non_ascii_keys
            .reserve(counts.n_non_ascii_keys);
        self.byte_pairs.reserve(counts.n_invalid_pairs);
        self.keys_with_empty_trimmed_values
            .reserve(counts.n_trimmed_empty_values);
        self.keys_with_trimmed_values.reserve(counts.n_trimmed);
    }
}

impl ValidKeywords {
    pub(crate) fn get_any(&self, k: &AnyKey) -> Option<&NEStr> {
        match k {
            AnyKey::Std(k0) => match k0 {
                RealOrPseudoStdKey::Real(k1) => self.get_std(k1),
                RealOrPseudoStdKey::Pseudo(k1) => self.get_pstd(k1),
            },
            AnyKey::NonStd(k0) => self.get_nonstd(k0),
        }
    }

    pub(crate) fn get_std(&self, k: &StdKey) -> Option<&NEStr> {
        NEStr::try_new(self.std.get(k))
    }

    pub(crate) fn get_pstd(&self, k: &PseudoStdKey) -> Option<&NEStr> {
        self.pstd.get(k).map(|s| s.as_ne_str())
    }

    pub(crate) fn get_nonstd(&self, k: &NonStdKey) -> Option<&NEStr> {
        self.nonstd.get(k).map(|s| s.as_ne_str())
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
    use fireflow_types::nonempty::NEString;

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

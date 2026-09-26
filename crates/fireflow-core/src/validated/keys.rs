use crate::config::EvaledReadRepairKeywordsConfig;
use crate::fixed_vec::OneOrTwo;
use crate::logging::{DeferredWarningsAndErrors, LogResult};
use crate::std_index::index::RawStdKeyIndex;

use fireflow_types::config::Encoding;
use fireflow_types::index::{BiMeasIndex, MeasIndex};
use fireflow_types::keys::nonstd::{DollarWrap, NonStdKey};
use fireflow_types::keys::raw_std::{RawStdKey, ToStd};
use fireflow_types::keys::{AnyKey, PseudoNonStdKey, PseudoStdKey, StdKey};
use nonempty::{HasNELen as _, NEAlt, NESlice, NEStr, NEString, NEVec, ToDisplayNE, ToNE};

use derive_more::{Display, From, Into};
use derive_new::new;
use derive_where::derive_where;
use hashbrown::HashMap;
use hashbrown::hash_map::Entry;
use thiserror::Error;

use std::borrow::Cow;
use std::fmt;
use std::marker::PhantomData;
use std::num::NonZeroUsize;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr, FromInnerPyObject},
    fireflow_types::python as py,
    pyo3::prelude::*,
};

/// A standard (non-pseudostandard) key or non-standard key.
#[derive(Clone, PartialEq, From)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum WritableKey {
    Std(StdKey),
    PseudoNonStd(PseudoNonStdKey),
    NonStd(NonStdKey),
}

impl<'a> ToDisplayNE<'a> for WritableKey {
    type NE = NEAlt<ToNE<StdKey>, NEAlt<ToNE<PseudoNonStdKey>, ToNE<&'a NonStdKey>>>;
    fn to_ne(&'a self) -> Self::NE {
        match self {
            Self::Std(x) => NEAlt::Left(ToNE(*x)),
            Self::PseudoNonStd(x) => NEAlt::Right(NEAlt::Left(ToNE(*x))),
            Self::NonStd(x) => NEAlt::Right(NEAlt::Right(ToNE(x))),
        }
    }
}

/// All valid Keywords from TEXT.
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
    pub pnonstd: PseudoNonStdKeywords,
    #[cfg_attr(feature = "serde", serde(serialize_with = "serialize::ordered_map"))]
    pub pstd: PseudoStdKeywords,
    #[cfg_attr(feature = "serde", serde(serialize_with = "serialize::ordered_map"))]
    pub nonstd: NonStdKeywords,
}

/// A string that should be used as the header in the measurement table.
#[derive(Display)]
pub struct MeasHeader(pub String);

/// Either a valid key (with '$' for standard keys) or a non-ASCII byte sequence.
#[derive(Clone, Display, PartialEq, Debug, From)]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromPyObject))]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum DollarKeyOrBytes {
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
/// [`ValueToStdKey`] trait.
#[derive(new)]
#[derive_where(Clone, Copy, Default, PartialEq, Eq, Debug; I)]
// TODO clean this up with generic prefix DollarWrapper rather than buring the
// wrapper underneath another wrapper. Or just always map it to a prefixed
// standard key since that's how it will always be printed (I think)
pub struct SpecificKey_<T, I> {
    index: I,
    _key: PhantomData<T>,
}

pub type SpecificKey<T> = SpecificKey_<T, <T as ValueToStdKey>::Index>;

impl<T: ValueToStdKey> ToDisplayNE<'_> for SpecificKey<T>
where
    Self: Into<RawStdKey> + Copy,
{
    type NE = ToNE<RawStdKey>;
    fn to_ne(&self) -> Self::NE {
        ToNE((*self).into())
    }
}

impl<T: ValueToStdKey> fmt::Display for SpecificKey<T>
where
    for<'a> &'a Self: Into<RawStdKey>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", self.into())
    }
}

impl<T: ValueToStdKey> From<SpecificKey<T>> for RawStdKey {
    fn from(value: SpecificKey<T>) -> Self {
        T::std(&value.index).0
    }
}

impl<'a, T: ValueToStdKey> From<&'a SpecificKey<T>> for RawStdKey {
    fn from(value: &'a SpecificKey<T>) -> Self {
        T::std(&value.index).0
    }
}

impl<T> SpecificKey_<T, BiMeasIndex> {
    pub(crate) fn new_i2(i: MeasIndex, j: MeasIndex) -> Self {
        Self::new(BiMeasIndex::new(i, j))
    }
}

/// A [`SpecificKey`] which is prefixed with '$' when displayed.
#[derive(Display, From)]
#[derive_where(Clone, Copy, Default, PartialEq, Eq, Debug; I)]
pub struct DollarKey_<T, I>(pub DollarWrap<true, SpecificKey_<T, I>>);

pub type DollarKey<T> = DollarKey_<T, <T as ValueToStdKey>::Index>;

impl<T: ValueToStdKey> From<DollarKey<T>> for StdKey {
    fn from(value: DollarKey<T>) -> Self {
        Self(value.0.0.into())
    }
}

impl<K: ValueToStdKey> ToDisplayNE<'_> for DollarKey<K>
where
    SpecificKey<K>: for<'b> ToDisplayNE<'b> + Copy,
{
    type NE = ToNE<DollarWrap<true, SpecificKey<K>>>;
    fn to_ne(&self) -> Self::NE {
        ToNE(self.0)
    }
}

impl<T, I> DollarKey_<T, I> {
    pub(crate) fn new(i: I) -> Self {
        Self(DollarWrap(SpecificKey_::new(i)))
    }

    pub(crate) fn index(self) -> I {
        self.0.0.index
    }
}

impl<T> DollarKey_<T, BiMeasIndex> {
    pub(crate) fn new_i2(i: MeasIndex, j: MeasIndex) -> Self {
        Self(DollarWrap(SpecificKey_::new_i2(i, j)))
    }
}

pub type StdKeywords = RawStdKeyIndex<true>;

pub type PseudoNonStdKeywords = RawStdKeyIndex<false>;

pub type NonStdKeywords = HashMap<NonStdKey, NEString>;

pub type PseudoStdKeywords = HashMap<PseudoStdKey, NEString>;

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

    /// Pseudo-standard keys which appear more than once with their values.
    pub(crate) non_unique_pstd_keywords: Vec<(PseudoStdKey, TruncatedNEString)>,

    /// Pseudo-nonstandard keys which appear more than once with their values.
    pub(crate) non_unique_pnonstd_keywords: Vec<(PseudoNonStdKey, TruncatedNEString)>,

    /// Non-standard keys which appear more than once with their values.
    pub(crate) non_unique_nonstd_keywords: Vec<(NonStdKey, TruncatedNEString)>,

    /// Keys with empty values.
    ///
    /// The only way this can happen at this stage is if the value is entirely
    /// whitespace and is trimmed.
    pub(crate) keys_with_empty_trimmed_values: Vec<(DollarKeyOrBytes, TruncatedNEString)>,

    /// Keys with values that were trimmed
    ///
    /// The value included here is the original value.
    pub(crate) keys_with_trimmed_values: Vec<(DollarKeyOrBytes, TruncatedNEString)>,
}

#[derive(Default)]
pub(crate) struct ParsedNonStdKeywords {
    /// Nonstandard keywords (without '$').
    pub(crate) nonstd: NonStdKeywords,

    /// Pseudostdandard keywords (with '$' but still non-standard).
    pub(crate) pstd: PseudoStdKeywords,
}

/// Error when keyword repair process resulted in colliding non-unique keys.
#[derive(Debug, Display, Error, PartialEq, Clone, From)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum RepairError {
    RenameStd(RenameNonUniqueError),
    PromoteNonUnique(PromoteNonUniqueError),
    AppendNonUnique(AppendNonUniqueError),
}

/// Error when renaming standard keys which are not unique.
#[derive(new, Debug, Error, PartialEq, Clone)]
#[error("key {k0} could not be renamed to {k1} because {k1} already exists")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct RenameNonUniqueError {
    k0: AnyKey,
    k1: AnyKey,
}

/// Error when promoting keys which are not unique.
#[derive(new, Debug, Error, PartialEq, Clone)]
#[error(
    "non-standard key {key} with value {value} could not be promoted because \
     {key} already exists as a standard key."
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct PromoteNonUniqueError {
    key: PseudoNonStdKey,
    value: TruncatedNEString,
}

/// Error when appending keys which are not unique.
#[derive(new, Debug, Error, PartialEq, Clone)]
#[error(
    "standard {key} with value {value} could not be appended because \
     {key} already exists as a standard key."
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct AppendNonUniqueError {
    key: StdKey,
    value: TruncatedNEString,
}

/// Diagnostic output from repairing the keyword list.
#[derive(Clone, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[allow(clippy::too_many_arguments)]
pub struct RepairDiagnostics {
    /// Standard keys which were demoted.
    pub demoted: Vec<StdKey>,

    /// Non-standard keys which were promoted.
    pub promoted: Vec<PseudoNonStdKey>,

    /// Standard keys which had values that were substituted.
    ///
    /// Values here are the original.
    pub subbed: Vec<(StdKey, TruncatedNEString)>,

    /// Standard keys which had values that were replaced.
    ///
    /// Values here are the original.
    pub replaced: Vec<(StdKey, TruncatedNEString)>,

    /// Keys which were renamed.
    ///
    /// First key in pair is the original.
    pub renamed: Vec<(AnyKey, AnyKey)>,

    /// Keys not renamed because they collided with an existing key.
    pub renamed_non_unique: Vec<(AnyKey, AnyKey)>,

    /// Standard keys which were ignored.
    pub ignored: Vec<(StdKey, TruncatedNEString)>,

    /// Standard keys which were removed.
    ///
    /// This only happens when a substitution pattern returns a blank.
    pub removed: Vec<(StdKey, TruncatedNEString)>,

    /// Non-standard keys which collided with a standard key when promoted.
    ///
    /// These keys were not moved.
    pub promoted_non_unique: Vec<(PseudoNonStdKey, TruncatedNEString)>,

    /// Appended keys which collided with an existing standard key.
    pub appended_non_unique: Vec<(StdKey, TruncatedNEString)>,
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

    #[must_use]
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

// Implement methods for misc types

// TODO clean this up once we decide what data structure to use for
// std/pseudo-non-std keys. If we just use hash tables or something with owned
// values this slice business is just extra complexity for no gain
#[derive(Debug)]
pub(crate) enum ParsedKeyword<'a> {
    // Valid std key value as a slice
    StdSlice(NonEmptyValue<StdKey, &'a NEStr>),
    // Valid std key value as owned value (used for values with latin1
    // characters and escaped delimiters)
    StdOwned(NonEmptyValue<StdKey, NEString>),
    // Pseudo-non-std key and value (slice)
    PseudoNonStdSlice(NonEmptyValue<PseudoNonStdKey, &'a NEStr>),
    // Pseudo-non-std key and value (owned)
    PseudoNonStdOwned(NonEmptyValue<PseudoNonStdKey, NEString>),
    // Valid non-std key and valid
    NonStd(NonEmptyValue<NonStdKey, NEString>),
    // Pseudo-std key and value
    PseudoStd(NonEmptyValue<PseudoStdKey, NEString>),
    // Key (any type or raw bytes) where value was trimmed to empty whitespace
    TrimmedEmptyValue(DollarKeyOrBytes, NEString),
    // Valid key with invalid value
    NonUtf8Value(AnyKey, NEVec<u8>),
    // Invalid key with valid value
    NonAsciiKey(NonEmptyValue<NEVec<u8>, NEString>),
    // Invalid pair
    BothInvalid(NEVec<u8>, NEVec<u8>),
}

#[derive(new, Debug)]
pub(crate) struct NonEmptyValue<K, V> {
    pub(crate) key: K,
    pub(crate) value: V,
    pub(crate) original: Option<NEString>,
}

pub(crate) enum ParsedValue<'a> {
    Slice(&'a NEStr, Option<NEString>),
    Owned(NEString, Option<NEString>),
    Bytes(NEVec<u8>),
    Empty(NEString),
}

impl ParsedValue<'_> {
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
            Cow::Owned(self.as_owned())
        }
    }

    pub(crate) fn as_owned(&self) -> NEVec<u8> {
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
        let pk = AnyKey::from_bytes(key);
        let pv = val.parse_from_bytes(trim, encoding);
        // This will throw away the trimmed value if it was computed in the case
        // of non-ascii keys. This is very rare so probably not worth
        // optimizing. The convenience of returning owned strings is worth it.
        match (pk, pv) {
            (Ok(AnyKey::Std(k)), ParsedValue::Slice(v, original)) => {
                Self::StdSlice(NonEmptyValue::new(k, v, original))
            }
            (Ok(AnyKey::Std(k)), ParsedValue::Owned(v, original)) => {
                Self::StdOwned(NonEmptyValue::new(k, v, original))
            }
            (Ok(AnyKey::PseudoStd(k)), ParsedValue::Slice(v, original)) => {
                Self::PseudoStd(NonEmptyValue::new(k, v.to_owned(), original))
            }
            (Ok(AnyKey::PseudoStd(k)), ParsedValue::Owned(v, original)) => {
                Self::PseudoStd(NonEmptyValue::new(k, v, original))
            }
            (Ok(AnyKey::NonStd(k)), ParsedValue::Slice(v, original)) => {
                Self::NonStd(NonEmptyValue::new(k, v.to_owned(), original))
            }
            (Ok(AnyKey::NonStd(k)), ParsedValue::Owned(v, original)) => {
                Self::NonStd(NonEmptyValue::new(k, v, original))
            }
            (Ok(AnyKey::PseudoNonStd(k)), ParsedValue::Slice(v, original)) => {
                Self::PseudoNonStdSlice(NonEmptyValue::new(k, v, original))
            }
            (Ok(AnyKey::PseudoNonStd(k)), ParsedValue::Owned(v, original)) => {
                Self::PseudoNonStdOwned(NonEmptyValue::new(k, v, original))
            }
            (Err(k), ParsedValue::Slice(v, original)) => {
                Self::NonAsciiKey(NonEmptyValue::new(k, v.to_owned(), original))
            }
            (Err(k), ParsedValue::Owned(v, original)) => {
                Self::NonAsciiKey(NonEmptyValue::new(k, v, original))
            }
            (Err(k), ParsedValue::Bytes(v)) => Self::BothInvalid(k, v),
            (Ok(k), ParsedValue::Bytes(v)) => Self::NonUtf8Value(k, v),
            (k, ParsedValue::Empty(original)) => {
                let kb = match k {
                    Ok(x) => DollarKeyOrBytes::Ascii(x),
                    Err(x) => DollarKeyOrBytes::Bytes(TruncatedNEBytes(x)),
                };
                Self::TrimmedEmptyValue(kb, original)
            }
        }
    }

    pub(crate) fn dispatch_slice_only(
        self,
        std: &mut Vec<(StdKey, &'a NEStr)>,
        pnonstd: &mut Vec<(PseudoNonStdKey, &'a NEStr)>,
        nonstd: &mut ParsedNonStdKeywords,
        diag: &mut ParsedKeywordsDiagnostic,
    ) {
        let f_owned = |_| panic!("this should only be called when input is all slices");
        self.dispatch(std, pnonstd, nonstd, diag, |v| v, f_owned);
    }

    pub(crate) fn dispatch_slice_or_owned(
        self,
        std: &mut Vec<(StdKey, Cow<'a, NEStr>)>,
        pnonstd: &mut Vec<(PseudoNonStdKey, Cow<'a, NEStr>)>,
        nonstd: &mut ParsedNonStdKeywords,
        diag: &mut ParsedKeywordsDiagnostic,
    ) {
        self.dispatch(std, pnonstd, nonstd, diag, Cow::Borrowed, Cow::Owned);
    }

    fn dispatch<F0, F1, V>(
        self,
        std: &mut Vec<(StdKey, V)>,
        pnonstd: &mut Vec<(PseudoNonStdKey, V)>,
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
                    let k = AnyKey::Std(kv.key);
                    diag.keys_with_trimmed_values
                        .push((DollarKeyOrBytes::from(k), o.into()));
                }
                std.push((kv.key, f_slice(kv.value)));
            }
            Self::StdOwned(kv) => {
                if let Some(o) = kv.original {
                    let k = AnyKey::Std(kv.key);
                    diag.keys_with_trimmed_values
                        .push((DollarKeyOrBytes::from(k), o.into()));
                }
                std.push((kv.key, f_owned(kv.value)));
            }
            Self::PseudoNonStdSlice(kv) => {
                if let Some(o) = kv.original {
                    let k = AnyKey::PseudoNonStd(kv.key);
                    diag.keys_with_trimmed_values
                        .push((DollarKeyOrBytes::from(k), o.into()));
                }
                pnonstd.push((kv.key, f_slice(kv.value)));
            }
            Self::PseudoNonStdOwned(kv) => {
                if let Some(o) = kv.original {
                    let k = AnyKey::PseudoNonStd(kv.key);
                    diag.keys_with_trimmed_values
                        .push((DollarKeyOrBytes::from(k), o.into()));
                }
                pnonstd.push((kv.key, f_owned(kv.value)));
            }
            Self::NonStd(kv) => {
                if let Some(o) = kv.original {
                    let k = AnyKey::NonStd(kv.key.clone());
                    diag.keys_with_trimmed_values
                        .push((DollarKeyOrBytes::from(k), o.into()));
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
            Self::PseudoStd(kv) => {
                if let Some(o) = kv.original {
                    let k = AnyKey::PseudoStd(kv.key.clone());
                    diag.keys_with_trimmed_values
                        .push((DollarKeyOrBytes::from(k), o.into()));
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
                diag.keys_with_empty_trimmed_values.push((k, v.into()));
            }
            Self::NonUtf8Value(k, v) => diag.keys_with_non_utf8_values.push((k, v.into())),
            Self::NonAsciiKey(kv) => {
                if let Some(o) = kv.original {
                    let k = TruncatedNEBytes::from(kv.key.clone());
                    diag.keys_with_trimmed_values
                        .push((DollarKeyOrBytes::from(k), o.into()));
                }
                diag.values_with_non_ascii_keys
                    .push((kv.key.into(), kv.value.into()));
            }
            Self::BothInvalid(k, v) => diag.byte_pairs.push((k.into(), v.into())),
        }
    }

    pub(crate) fn count(&self, counts: &mut ParsedKeywordCounts) {
        match self {
            Self::StdSlice(kv) => {
                counts.std_slice_kws += 1;
                counts.trimmed += usize::from(kv.original.is_some());
            }
            Self::PseudoNonStdSlice(kv) => {
                counts.pnonstd_slice_kws += 1;
                counts.trimmed += usize::from(kv.original.is_some());
            }
            Self::PseudoNonStdOwned(kv) => {
                counts.pnonstd_owned_kws += 1;
                counts.trimmed += usize::from(kv.original.is_some());
            }
            Self::StdOwned(kv) => {
                counts.std_owned_kws += 1;
                counts.trimmed += usize::from(kv.original.is_some());
            }
            Self::NonStd(kv) => {
                counts.nonstd_keys += 1;
                counts.trimmed += usize::from(kv.original.is_some());
            }
            Self::PseudoStd(kv) => {
                counts.pstd_keys += 1;
                counts.trimmed += usize::from(kv.original.is_some());
            }
            Self::TrimmedEmptyValue(_, _) => counts.trimmed_empty_values += 1,
            Self::NonUtf8Value(_, _) => counts.non_utf8_values += 1,
            Self::NonAsciiKey(_) => counts.non_ascii_keys += 1,
            Self::BothInvalid(_, _) => counts.invalid_pairs += 1,
        }
    }
}

#[derive(Default)]
pub(crate) struct ParsedKeywordCounts {
    pub(crate) std_slice_kws: usize,
    pub(crate) std_owned_kws: usize,
    pub(crate) pnonstd_slice_kws: usize,
    pub(crate) pnonstd_owned_kws: usize,
    pub(crate) nonstd_keys: usize,
    pub(crate) pstd_keys: usize,
    pub(crate) trimmed_empty_values: usize,
    pub(crate) non_utf8_values: usize,
    pub(crate) non_ascii_keys: usize,
    pub(crate) invalid_pairs: usize,
    pub(crate) trimmed: usize,
}

impl ParsedNonStdKeywords {
    pub(crate) fn reserve(&mut self, counts: &ParsedKeywordCounts) {
        self.nonstd.reserve(counts.nonstd_keys);
        self.pstd.reserve(counts.pstd_keys);
    }
}

impl ParsedKeywordsDiagnostic {
    pub(crate) fn reserve(&mut self, counts: &ParsedKeywordCounts) {
        self.keys_with_non_utf8_values
            .reserve(counts.non_utf8_values);
        self.values_with_non_ascii_keys
            .reserve(counts.non_ascii_keys);
        self.byte_pairs.reserve(counts.invalid_pairs);
        self.keys_with_empty_trimmed_values
            .reserve(counts.trimmed_empty_values);
        self.keys_with_trimmed_values.reserve(counts.trimmed);
    }
}

impl ValidKeywords {
    #[allow(clippy::too_many_lines)]
    pub(crate) fn repair(
        &mut self,
        conf: &EvaledReadRepairKeywordsConfig,
    ) -> DeferredWarningsAndErrors<RepairDiagnostics, RepairError, RepairError> {
        let match_promote = conf.promote_nonstandard_keys.as_matcher();
        let match_demote = conf.demote_standard_keys.as_matcher();
        let match_ignore = conf.ignore_standard_keys.as_matcher();
        let match_subs = conf.substitute_standard_key_values.as_matcher();

        // ignore

        let mut ignored = vec![];

        for (k, ()) in &match_ignore.literals {
            if let Some(v) = self.std.delete(k) {
                ignored.push((*k, TruncatedNEString(v.to_owned())));
            }
        }

        if match_ignore.has_wildcards() {
            self.std.delete_when(
                |k| match_ignore.is_wildcard_match(&k),
                |k, v| ignored.push((DollarWrap(k), TruncatedNEString(v.to_owned()))),
            );
        }

        // rename

        let mut renamed = vec![];
        let mut renamed_non_unique = vec![];

        for (k0, k1) in &conf.rename_standard_keys {
            macro_rules! go {
                () => {
                    match k0 {
                        AnyKey::Std(k0_) => self.std.delete(k0_).map(|v| {
                            renamed.push((k0.clone(), k1.clone()));
                            Cow::Borrowed(v)
                        }),
                        AnyKey::PseudoNonStd(k0_) => self.pnonstd.delete(k0_).map(|v| {
                            renamed.push((k0.clone(), k1.clone()));
                            Cow::Borrowed(v)
                        }),
                        AnyKey::PseudoStd(k0_) => self.pstd.remove(k0_).map(|v| {
                            renamed.push((k0.clone(), k1.clone()));
                            Cow::Owned(v)
                        }),
                        AnyKey::NonStd(k0_) => self.nonstd.remove(k0_).map(|v| {
                            renamed.push((k0.clone(), k1.clone()));
                            Cow::Owned(v)
                        }),
                    }
                };
            }

            match k1 {
                AnyKey::Std(k1_) => {
                    if self.std.key_has_value(k1_) {
                        renamed_non_unique.push((k0.clone(), k1.clone()));
                    } else if let Some(v) = go!() {
                        let vf = v.into_owned();
                        // we checked above so this shouldn't return anything
                        let _ = self.std.insert(k1_, vf.as_ne_str());
                    }
                }
                AnyKey::PseudoNonStd(k1_) => {
                    if self.pnonstd.key_has_value(k1_) {
                        renamed_non_unique.push((k0.clone(), k1.clone()));
                    } else if let Some(v) = go!() {
                        let vf = v.into_owned();
                        // we checked above so this shouldn't return anything
                        let _ = self.pnonstd.insert(k1_, vf.as_ne_str());
                    }
                }
                AnyKey::PseudoStd(k1_) => {
                    if self.pstd.contains_key(k1_) {
                        renamed_non_unique.push((k0.clone(), k1.clone()));
                    } else if let Some(v) = go!() {
                        // we checked above so this shouldn't return anything
                        let _ = self.pstd.insert(k1_.clone(), v.into_owned());
                    }
                }
                AnyKey::NonStd(k1_) => {
                    if self.nonstd.contains_key(k1_) {
                        renamed_non_unique.push((k0.clone(), k1.clone()));
                    } else if let Some(v) = go!() {
                        // we checked above so this shouldn't return anything
                        let _ = self.nonstd.insert(k1_.clone(), v.into_owned());
                    }
                }
            }
        }

        // demote

        let mut demoted = vec![];

        for (k, ()) in &match_demote.literals {
            if let Some(v) = self.std.delete(k) {
                self.pnonstd
                    .insert_demoted(&mut self.nonstd, k, v.to_owned());
                demoted.push(*k);
            }
        }

        if match_demote.has_wildcards() {
            self.std.delete_when(
                |k| match_demote.is_wildcard_match(&k),
                |k, v| {
                    self.pnonstd
                        .insert_demoted(&mut self.nonstd, &DollarWrap(k), v.to_owned());
                    demoted.push(DollarWrap(k));
                },
            );
        }

        // promote

        let mut promote_non_unique = vec![];
        let mut promoted = vec![];

        for (k, ()) in &match_promote.literals {
            if let Some(v) = self.pnonstd.delete(k) {
                if let Some(vf) = self.std.insert(k.rewrap_ref(), v) {
                    promote_non_unique.push((*k, TruncatedNEString(vf.to_owned())));
                } else {
                    promoted.push(*k);
                }
            }
        }

        if match_promote.has_wildcards() {
            // self.pnonstd.replace_when(
            //     |k| match_promote.is_wildcard_match(&k) && !self.std.insert(k),
            //     |k, v| {
            //         if let Some(vf) = self.std.insert(k, v) {
            //             promote_non_unique.push((*k, TruncatedNEString(vf.to_owned())));
            //             true
            //         } else {
            //             promoted.push(*k);
            //             false
            //         }
            //     },
            // );
            // self.pnonstd.retain(|k, v| {
            //     if match_promote.is_wildcard_match(&k.0) {
            //         if let Some(vf) = self.std.insert(&k.0, v.as_ne_str()) {
            //             promote_non_unique.push((*k, TruncatedNEString(vf.to_owned())));
            //             true
            //         } else {
            //             promoted.push(*k);
            //             false
            //         }
            //     } else {
            //         true
            //     }
            // });
        }

        // replace

        let mut replaced = vec![];

        for (k, vf) in &conf.replace_standard_key_values {
            if let Some(v) = self.std.delete(k) {
                replaced.push((*k, TruncatedNEString(v.to_owned())));
                let _ = self.std.insert(k, vf.as_ne_str());
            }
        }

        // sub

        let mut removed = vec![];
        let mut subbed = vec![];

        for (k, subpat) in &match_subs.literals {
            if let Some(v) = self.std.delete(k) {
                if let Ok(vf) = NEString::try_from(subpat.sub(v.as_str())) {
                    subbed.push((*k, TruncatedNEString(v.to_owned())));
                    let _ = self.std.insert(k, vf.as_ne_str());
                } else {
                    removed.push((*k, TruncatedNEString(v.to_owned())));
                }
            }
        }

        if match_subs.has_wildcards() {
            self.std.replace_when(
                |k| match_subs.get_wildcard(&k),
                |k, v, subpat| {
                    let dk = DollarWrap(k);
                    if let Ok(vf) = NEString::try_from(subpat.sub(v.as_str())) {
                        subbed.push((dk, TruncatedNEString(v.to_owned())));
                        Some(vf)
                    } else {
                        removed.push((dk, TruncatedNEString(v.to_owned())));
                        None
                    }
                },
            );
        }

        // append

        let mut appended_non_unique = vec![];

        // TODO this is easy to optimize since we know the length of the inputs
        // and there are no pesky regex expressions
        for (k, v) in &conf.append_standard_keywords {
            if let Some(vf) = self.std.insert(k, v.as_ne_str()) {
                appended_non_unique.push((*k, TruncatedNEString(vf.to_owned())));
            }
        }

        // finalize

        let ret = RepairDiagnostics {
            demoted,
            promoted,
            subbed,
            replaced,
            renamed,
            renamed_non_unique,
            ignored,
            removed,
            promoted_non_unique: promote_non_unique,
            appended_non_unique,
        };

        let e0 = ret
            .renamed_non_unique
            .iter()
            .map(|(k0, k1)| RenameNonUniqueError::new(k0.clone(), k1.clone()))
            .map(RepairError::from);
        let e1 = ret
            .promoted_non_unique
            .iter()
            .map(|(k, v)| PromoteNonUniqueError::new(*k, v.clone()))
            .map(RepairError::from);
        let e2 = ret
            .appended_non_unique
            .iter()
            .map(|(k, v)| AppendNonUniqueError::new(*k, v.clone()))
            .map(RepairError::from);
        let es = e0.chain(e1).chain(e2);

        let flag = conf.allow_repair_non_unique;
        LogResult::new_deferred_switchable_iter3((), es, flag)
            .switchable_into_commutative()
            .set_deferred_value(ret)
    }

    pub(crate) fn get_any(&self, k: &AnyKey) -> Option<&NEStr> {
        match k {
            AnyKey::Std(k0) => self.get_std(k0),
            AnyKey::PseudoNonStd(k0) => self.get_pnonstd(k0),
            AnyKey::PseudoStd(k0) => self.get_pstd(k0),
            AnyKey::NonStd(k0) => self.get_nonstd(k0),
        }
    }

    pub(crate) fn get_std(&self, k: &StdKey) -> Option<&NEStr> {
        self.std.get(k)
    }

    pub(crate) fn get_pnonstd(&self, k: &PseudoNonStdKey) -> Option<&NEStr> {
        self.pnonstd.get(k)
    }

    pub(crate) fn get_pstd(&self, k: &PseudoStdKey) -> Option<&NEStr> {
        self.pstd.get(k).map(NEString::as_ne_str)
    }

    pub(crate) fn get_nonstd(&self, k: &NonStdKey) -> Option<&NEStr> {
        self.nonstd.get(k).map(NEString::as_ne_str)
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

const TRUNCATED_BYTES_LIMIT: usize = 20;
const TRUNCATED_STR_LIMIT: usize = 20;

#[cfg(feature = "serde")]
mod serialize {
    use nonempty::NEString;

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

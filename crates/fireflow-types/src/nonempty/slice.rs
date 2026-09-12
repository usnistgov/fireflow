use super::string::NEString;

use derive_more::{AsRef, Display, From, Into};
use nonempty_collections::{
    IntoNonEmptyIterator, NESlice as NESlice_, NEVec, NonEmptyIterator as _, slice::Iter as NEIter,
};

use std::{hash::Hash, iter, num::NonZeroUsize, ptr::from_ref, slice};

#[cfg(feature = "serde")]
use serde::Serialize;

/// A slice which can never be empty.
#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Display, Debug, AsRef)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[repr(transparent)]
pub struct NESlice<T>([T]);

/// Iterator of non-empty chunks of a [`NESlice`].
pub struct NEChunks<'a, T>(slice::Chunks<'a, T>);

impl<'a, T> IntoIterator for &'a NESlice<T> {
    type Item = &'a T;
    type IntoIter = slice::Iter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, T> IntoNonEmptyIterator for &'a NESlice<T> {
    type IntoNEIter = NEIter<'a, T>;
    fn into_nonempty_iter(self) -> Self::IntoNEIter {
        NESlice_::try_from_slice(&self.0)
            .unwrap()
            .into_nonempty_iter()
    }
}

impl<T> NESlice<T> {
    #[must_use]
    pub const fn try_from_slice(bytes: &[T]) -> Option<&Self> {
        if bytes.is_empty() {
            None
        } else {
            Some(Self::new_unchecked(bytes))
        }
    }

    #[must_use]
    pub const fn len(&self) -> NonZeroUsize {
        NonZeroUsize::new(self.0.len()).unwrap()
    }

    /// Return the first element
    #[must_use]
    pub const fn first(&self) -> &T {
        self.0.first().unwrap()
    }

    /// Return the last element
    #[must_use]
    pub const fn last(&self) -> &T {
        self.0.last().unwrap()
    }

    /// Split the first element from the rest.
    #[must_use]
    pub const fn split_first(&self) -> (&T, &[T]) {
        self.0.split_first().unwrap()
    }

    /// Split the last element from the rest.
    #[must_use]
    pub const fn split_last(&self) -> (&T, &[T]) {
        self.0.split_last().unwrap()
    }

    /// Convert to [`NEVec`]
    #[must_use]
    pub fn to_ne_vec(&self) -> NEVec<T>
    where
        T: Clone,
    {
        self.into_nonempty_iter().cloned().collect()
    }

    pub(crate) const fn new_unchecked(bytes: &[T]) -> &Self {
        let p: *const [T] = from_ref(bytes);
        // SAFETY: NESlice<T> and [T] have same layout
        unsafe {
            #[allow(clippy::as_conversions)]
            &*(p as *const Self)
        }
    }

    pub fn iter(&self) -> slice::Iter<'_, T> {
        self.0.iter()
    }

    pub fn nonempty_iter(&self) -> NEIter<'_, T> {
        self.into_nonempty_iter()
    }

    pub fn nonempty_chunks(&self, chunk_size: NonZeroUsize) -> NEChunks<'_, T> {
        NEChunks(self.0.chunks(chunk_size.get()))
    }
}

impl<'a, T> IntoIterator for NEChunks<'a, T> {
    type Item = &'a NESlice<T>;

    type IntoIter = iter::Map<slice::Chunks<'a, T>, fn(&'a [T]) -> &'a NESlice<T>>;

    fn into_iter(self) -> Self::IntoIter {
        self.0
            .map(|x| NESlice::try_from_slice(x).expect("sliced chunks will never be empty"))
    }
}

impl NESlice<u8> {
    /// Trim whitespace from start and end of string.
    ///
    /// This will strip 0x09, 0x0A, 0x0B, 0x0C, 0x0D, and 0x20.
    #[must_use]
    pub fn trim_ascii(&self) -> &[u8] {
        self.trim_ascii_start().trim_ascii_end()
    }

    /// Trim whitespace from start and end of string.
    ///
    /// This will strip 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x20 and 0xA0.
    #[must_use]
    pub fn trim_latin1(&self) -> &[u8] {
        trim_end(self.trim_latin1_start(), |b| is_latin1_whitespace(*b))
    }

    /// Trim whitespace from start of string.
    ///
    /// This will strip 0x09, 0x0A, 0x0B, 0x0C, 0x0D, and 0x20.
    #[must_use]
    pub fn trim_ascii_start(&self) -> &[u8] {
        trim_start(self.as_ref(), |b| is_ascii_whitespace_vtab(*b))
    }

    /// Trim whitespace from end of string.
    ///
    /// This will strip 0x09, 0x0A, 0x0B, 0x0C, 0x0D, and 0x20.
    #[must_use]
    pub fn trim_ascii_end(&self) -> &[u8] {
        trim_end(self.as_ref(), |b| is_ascii_whitespace_vtab(*b))
    }

    /// Trim whitespace from start of string.
    ///
    /// This will strip 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x20, and 0xA0.
    #[must_use]
    pub fn trim_latin1_start(&self) -> &[u8] {
        trim_start(self.as_ref(), |b| is_latin1_whitespace(*b))
    }

    /// Trim whitespace from end of string.
    ///
    /// This will strip 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x20, and 0xA0.
    #[must_use]
    pub fn trim_latin1_end(&self) -> &[u8] {
        trim_end(self.as_ref(), |b| is_latin1_whitespace(*b))
    }

    /// Convert to a string using Latin1 encoding.
    ///
    /// If the string is UTF8 or ASCII this is equivalent to converting
    /// using `ToOwned::to_owned`.
    #[must_use]
    pub fn to_latin1_string(&self) -> NEString {
        let s: String = self.0.iter().copied().map(char::from).collect();
        NEString::try_from(s).unwrap()
    }
}

/// Test if byte is whitespace.
///
/// IMPORTANT: unlike u8::is_ascii_whitespace, this will also consider vertical
/// tab to be whitespace.
const fn is_ascii_whitespace_vtab(byte: u8) -> bool {
    byte.is_ascii_whitespace() || matches!(byte, b'\x0B')
}

/// Test if byte is whitespace according to single byte encoding.
///
/// This will treat the 0xA0 (non-breaking space) as a space.
///
/// If this is used to trim a bytestring, it should only be assumed to be
/// encoded using a single-byte scheme (ISO/IEC 8859-1/Latin1, IANA ISO-8859-1,
/// or Windows-1252, which are all the same with regard to this character).
/// Removing this byte from the right might break UTF-8. since it starts with a
/// '0b10' prefix.
const fn is_latin1_whitespace(byte: u8) -> bool {
    is_ascii_whitespace_vtab(byte) || matches!(byte, b'\xA0')
}

fn trim_start<F, T>(xs: &[T], mut f: F) -> &[T]
where
    F: FnMut(&T) -> bool,
{
    let mut ys = xs;
    while let [first, rest @ ..] = ys {
        if f(first) {
            ys = rest;
        } else {
            break;
        }
    }
    ys
}

fn trim_end<F, T>(xs: &[T], mut f: F) -> &[T]
where
    F: FnMut(&T) -> bool,
{
    let mut ys = xs;
    while let [rest @ .., last] = ys {
        if f(last) {
            ys = rest;
        } else {
            break;
        }
    }
    ys
}

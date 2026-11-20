use crate::types::dataframe::HasLen;

use fireflow_types::keys::raw_std::{EnumIndex, RawStdKey};
use itertools::{EitherOrBoth, Itertools as _};
use nonempty::NEStr;

use derive_where::derive_where;

use std::array::from_fn;
use std::iter;
use std::marker::PhantomData;
use std::ops::{Index, IndexMut, Range};

pub type NestedEnumString<const LEN: usize, K> = NestedString<[Range<usize>; LEN], K>;

#[derive_where(Default, Clone, Debug; I)]
pub struct NestedString<I, K> {
    inner: Vec<u8>,
    offsets: I,
    _key: PhantomData<K>,
}

impl<I, K> PartialEq for NestedString<I, K>
where
    I: HasLen + Index<usize, Output = Range<usize>>,
    K: EnumIndex + PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.iter().zip_longest(other.iter()).all(|e| {
            if let EitherOrBoth::Both(x, y) = e {
                x == y
            } else {
                false
            }
        })
    }
}

pub(crate) struct Iter<'a, I, K: EnumIndex> {
    keys: K::Generator,
    inner: &'a NestedString<I, K>,
    index: usize,
}

pub(crate) type IterStd<'a, I, K> =
    iter::FilterMap<Iter<'a, I, K>, fn((K, &str)) -> Option<(RawStdKey, &NEStr)>>;

impl<const LEN: usize, K> NestedEnumString<LEN, K> {
    pub fn init_array(n_bytes: usize) -> Self {
        let offsets = from_fn(|_| 0..0);
        Self {
            inner: Vec::with_capacity(n_bytes),
            offsets,
            _key: PhantomData,
        }
    }

    /// Insert new value into index.
    ///
    /// If index already has a value, return new value and leave old value
    /// unchanged.
    pub(crate) fn try_insert_array<V>(&mut self, k: &K, v: V) -> Option<V>
    where
        V: AsRef<NEStr>,
        K: EnumIndex,
    {
        let i = k.offset();
        if self.get_index(i).is_some_and(str::is_empty) {
            self.push_ne(i, v.as_ref());
            None
        } else {
            Some(v)
        }
    }
}

impl<I, K> NestedString<I, K> {
    pub(crate) fn n_offsets(&self) -> usize
    where
        I: HasLen,
    {
        self.offsets.len()
    }

    pub(crate) fn get(&self, k: &K) -> Option<&str>
    where
        I: HasLen + Index<usize, Output = Range<usize>>,
        K: EnumIndex,
    {
        self.get_index(k.offset())
    }

    pub(crate) fn occupied(&self, k: &K) -> Option<bool>
    where
        I: HasLen + Index<usize, Output = Range<usize>>,
        K: EnumIndex,
    {
        self.get(k).map(|v| !v.is_empty())
    }

    pub(crate) fn get_index(&self, i: usize) -> Option<&str>
    where
        I: HasLen + Index<usize, Output = Range<usize>>,
    {
        (i < self.offsets.len()).then(|| self.get_index_unchecked(i))
    }

    pub(crate) fn get_index_unchecked(&self, i: usize) -> &str
    where
        I: Index<usize, Output = Range<usize>>,
    {
        self.get_range_unchecked(&self.offsets[i])
    }

    pub(crate) fn get_range_unchecked(&self, rng: &Range<usize>) -> &str
    where
        I: Index<usize, Output = Range<usize>>,
    {
        // SAFETY: this struct is validated such that each slice is a string
        unsafe { str::from_utf8_unchecked(&self.inner[rng.start..rng.end]) }
    }

    pub(crate) fn iter_std(&self) -> IterStd<'_, I, K>
    where
        I: HasLen + Index<usize, Output = Range<usize>>,
        K: EnumIndex + Into<RawStdKey>,
    {
        self.iter()
            .filter_map(|(k, v)| NEStr::try_new(v).map(|ne| (k.into(), ne)))
    }

    pub(crate) fn iter(&self) -> Iter<'_, I, K>
    where
        I: HasLen + Index<usize, Output = Range<usize>>,
        K: EnumIndex,
    {
        Iter {
            keys: K::generate(),
            inner: self,
            index: 0,
        }
    }

    pub(crate) fn replace_when<V, Fwhen, Fwith, T>(&mut self, mut fwhen: Fwhen, mut fwith: Fwith)
    where
        V: AsRef<NEStr>,
        I: HasLen + IndexMut<usize, Output = Range<usize>>,
        K: EnumIndex,
        Fwhen: FnMut(&K) -> Option<T>,
        Fwith: FnMut(&K, &NEStr, T) -> Option<V>,
    {
        for k in K::generate().take(self.offsets.len()) {
            if let Some(flag) = fwhen(&k)
                && let Some(v) = self.delete(&k)
                && let Some(new) = fwith(&k, v, flag)
            {
                let i = k.offset();
                self.push_ne(i, new.as_ref());
            }
        }
    }

    pub(crate) fn retain<F>(&mut self, mut f: F)
    where
        I: HasLen + IndexMut<usize, Output = Range<usize>>,
        K: EnumIndex,
        F: FnMut(&K, &NEStr) -> bool,
    {
        for k in K::generate().take(self.offsets.len()) {
            if let Some(v) = self.get(&k).and_then(NEStr::try_new)
                && !f(&k, v)
            {
                let _ = self.delete(&k);
            }
        }
    }

    /// Remove an entry from the index.
    ///
    /// The old data is not actually removed. Only the range that indexes
    /// the string pool will be reset for the given index.
    ///
    /// Return old data if available. Return none if nothing was deleted.
    pub(crate) fn delete(&mut self, k: &K) -> Option<&NEStr>
    where
        K: EnumIndex,
        I: HasLen + IndexMut<usize, Output = Range<usize>>,
    {
        let i = k.offset();
        let n = self.offsets.len();
        if i < n {
            let rng = self.offsets[i].clone();
            self.offsets[i] = 0..0;
            NEStr::try_new(self.get_range_unchecked(&rng))
        } else {
            None
        }
    }

    /// Put a key/value pair into the index.
    ///
    /// Order does not matter. Duplicates will be tracked and returned with the
    /// first value being kept.
    ///
    /// Keys are assumed to map to an offset, which in turn is expected to be a
    /// valid index in the offsets vector (which in turn provides the ranges for
    /// the inner string array). Any key in the input which maps to an offset
    /// that is not in the offset vector will trigger a panic.
    ///
    /// Values are assumed to reference to non-empty strings.
    pub(crate) fn push_dedup<V>(&mut self, k: &K, v: V) -> Option<V>
    where
        V: AsRef<NEStr>,
        K: EnumIndex,
        I: IndexMut<usize, Output = Range<usize>>,
    {
        let i = k.offset();
        let rng = &self.offsets[i];
        if rng.is_empty() {
            self.push_ne(i, v.as_ref());
            None
        } else {
            Some(v)
        }
    }

    /// Put a sequence of key/value pairs in the index.
    ///
    /// Order does not matter. Duplicates will be tracked and returned with the
    /// first value being kept.
    ///
    /// Keys are assumed to map to an offset, which in turn is expected to be a
    /// valid index in the offsets vector (which in turn provides the ranges for
    /// the inner string array). Any key in the input which maps to an offset
    /// that is not in the offset vector will trigger a panic.
    ///
    /// Values are assumed to reference to non-empty strings.
    pub(crate) fn extend_dedup<V>(&mut self, pairs: impl IntoIterator<Item = (K, V)>) -> Vec<(K, V)>
    where
        V: AsRef<NEStr>,
        K: EnumIndex,
        I: HasLen + Index<usize, Output = Range<usize>> + IndexMut<usize, Output = Range<usize>>,
    {
        let mut duplicates = vec![];
        for (k, v) in pairs {
            let i = k.offset();
            let rng = &self.offsets[i];
            if rng.is_empty() {
                self.push_ne(i, v.as_ref());
            } else {
                duplicates.push((k, v));
            }
        }
        duplicates
    }

    fn push_ne(&mut self, i: usize, v: &NEStr)
    where
        I: IndexMut<usize, Output = Range<usize>>,
    {
        let start = self.inner.len();
        self.inner.extend(v.as_str().as_bytes());
        let end = self.inner.len();
        self.offsets[i] = start..end;
    }
}

impl<'a, I, K> Iterator for Iter<'a, I, K>
where
    K: EnumIndex,
    I: HasLen + Index<usize, Output = Range<usize>>,
{
    type Item = (K, &'a str);

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.inner.offsets.len() {
            let k = self.keys.next()?;
            let s = self.inner.get_index_unchecked(self.index);
            self.index += 1;
            Some((k, s))
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // TODO this assumes the generator at least the same length as the
        // offset array (which should be true?)
        let s = self.inner.offsets.len().saturating_sub(self.index);
        (s, Some(s))
    }
}

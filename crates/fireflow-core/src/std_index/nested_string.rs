use crate::validated::dataframe::HasLen;

use fireflow_types::keys::raw_std::{EnumIndex, RawStdKey};
use nonempty::NEStr;

use derive_new::new;
use derive_where::derive_where;

use std::array::from_fn;
use std::iter;
use std::marker::PhantomData;
use std::ops::{Index, IndexMut, Range};

pub type NestedEnumString<const LEN: usize, K> = NestedString<[Range<usize>; LEN], (), K>;

pub type NestedVariableString<K, S> = NestedString<Vec<Range<usize>>, S, K>;

#[derive_where(Default, Clone, Debug, PartialEq, Eq; I, S)]
pub struct NestedString<I, S, K> {
    inner: Vec<u8>,
    offsets: I,
    sub_dimension: S,
    _key: PhantomData<K>,
}

#[derive(new, Clone, Copy, Default)]
pub struct NestedStringSize {
    /// The number of bytes to allocate to the inner buffer.
    ///
    /// This is intended for performance.
    pub n_bytes: usize,

    /// The number of offsets to include.
    ///
    /// This must exactly match the indices that will be used. Otherwise
    /// populating with data will panic.
    ///
    /// This is probably more than the number of strings which is equal to the
    /// number of non-empty offsets.
    pub n_offsets: usize,
}

pub(crate) struct Iter<'a, I, K: EnumIndex> {
    keys: K::Generator,
    inner: &'a NestedString<I, K::SubDimension, K>,
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
            sub_dimension: (),
            _key: PhantomData,
        }
    }

    // pub(crate) unsafe fn set_keys<V>(&mut self, pairs: impl IntoIterator<Item = (K, V)>)
    // where
    //     K: NumericEnum<LEN>,
    //     V: AsRef<NEStr>,
    // {
    //     let mut prev_i = 0;
    //     for (k, v) in pairs {
    //         let i = k.index();
    //         // Pad the index vector with previous length up until the index
    //         // to be added. These are blank strings that we skipped by not
    //         // explicitly passing a pair for it.
    //         for j in (prev_i + 1)..i {
    //             self.offsets[j] = self.inner.len();
    //         }
    //         self.offsets[i] = self.inner.len();
    //         self.inner.extend(v.as_ref().as_str().as_bytes());
    //         prev_i = i;
    //     }
    //     // Extend the unfilled right tail of the offsets so that the last value
    //     // added will end at the length of the inner buffer, and everything
    //     // after will be empty.
    //     if prev_i < self.offsets.len() && !self.inner.is_empty() {
    //         for j in (prev_i + 1)..self.offsets.len() {
    //             self.offsets[j] = self.inner.len();
    //         }
    //     }
    // }
}

impl<K, S> NestedVariableString<K, S> {
    pub fn init_var(size: &NestedStringSize, sub_dimension: S) -> Self {
        let mut offsets = vec![];
        offsets.resize_with(size.n_offsets, || 0..0);
        Self {
            inner: Vec::with_capacity(size.n_bytes),
            offsets,
            sub_dimension,
            _key: PhantomData,
        }
    }

    // /// # Safety
    // ///
    // /// - The index of each pair must be in order.
    // /// - This must only be called once on a freshly init-ed object.
    // pub(crate) unsafe fn extend_pairs<V>(&mut self, pairs: impl IntoIterator<Item = (usize, V)>)
    // where
    //     V: AsRef<NEStr>,
    // {
    //     for (i, v) in pairs {
    //         // Pad the index vector with previous length up until the index
    //         // to be added. These are blank strings that we skipped by not
    //         // explicitly passing a pair for it.
    //         for _ in self.offsets.len()..i {
    //             self.offsets.push(self.inner.len());
    //         }
    //         self.offsets.push(self.inner.len());
    //         self.inner.extend(v.as_ref().as_str().as_bytes());
    //     }
    // }
}

impl<I, S, K> NestedString<I, S, K> {
    // pub(crate) fn n_bytes(&self) -> usize {
    //     self.inner.len()
    // }

    pub(crate) fn n_offsets(&self) -> usize
    where
        I: HasLen,
    {
        self.offsets.len()
    }

    pub(crate) fn sub_dimension(&self) -> &S {
        &self.sub_dimension
    }

    pub(crate) fn get(&self, k: &K) -> Option<&str>
    where
        I: HasLen + Index<usize, Output = Range<usize>>,
        K: EnumIndex<SubDimension = S>,
    {
        self.get_index(k.offset(&self.sub_dimension))
    }

    pub(crate) fn occupied(&self, k: &K) -> Option<bool>
    where
        I: HasLen + Index<usize, Output = Range<usize>>,
        K: EnumIndex<SubDimension = S>,
    {
        self.get(k).map(|v| !v.is_empty())
    }

    // pub(crate) fn get_unchecked(&self, k: &K) -> &str
    // where
    //     I: HasLen + Index<usize, Output = usize>,
    //     K: EnumIndex<SubDimension = S>,
    // {
    //     self.get_index_unchecked(k.offset(&self.sub_dimension))
    // }

    pub(crate) fn get_index(&self, i: usize) -> Option<&str>
    where
        I: HasLen + Index<usize, Output = Range<usize>>,
    {
        (i < self.offsets.len()).then(|| self.get_index_unchecked(i))
    }

    pub(crate) fn get_index_unchecked(&self, i: usize) -> &str
    where
        I: HasLen + Index<usize, Output = Range<usize>>,
    {
        let n = self.offsets.len();
        assert!(i < n, "index out of bounds: {i}");
        let rng = &self.offsets[i];
        // SAFETY: this struct is validated such that each slice is a string
        unsafe { str::from_utf8_unchecked(&self.inner[rng.start..rng.end]) }
    }

    pub(crate) fn iter_std(&self) -> IterStd<'_, I, K>
    where
        I: HasLen + Index<usize, Output = Range<usize>>,
        K: EnumIndex<SubDimension = S> + Into<RawStdKey>,
    {
        self.iter()
            .filter_map(|(k, v)| NEStr::try_new(v).map(|ne| (k.into(), ne)))
    }

    pub(crate) fn iter(&self) -> Iter<'_, I, K>
    where
        I: HasLen + Index<usize, Output = Range<usize>>,
        K: EnumIndex<SubDimension = S>,
    {
        Iter {
            keys: K::generate(&self.sub_dimension),
            inner: self,
            index: 0,
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
        K: EnumIndex<SubDimension = S>,
        I: HasLen + Index<usize, Output = Range<usize>> + IndexMut<usize, Output = Range<usize>>,
    {
        let i = k.offset(&self.sub_dimension);
        let rng = &self.offsets[i];
        if rng.is_empty() {
            let start = self.inner.len();
            self.inner.extend(v.as_ref().as_str().as_bytes());
            let end = self.inner.len();
            self.offsets[i] = start..end;
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
        K: EnumIndex<SubDimension = S>,
        I: HasLen + Index<usize, Output = Range<usize>> + IndexMut<usize, Output = Range<usize>>,
    {
        let mut duplicates = vec![];
        for (k, v) in pairs {
            let i = k.offset(&self.sub_dimension);
            let rng = &self.offsets[i];
            if rng.is_empty() {
                let start = self.inner.len();
                self.inner.extend(v.as_ref().as_str().as_bytes());
                let end = self.inner.len();
                self.offsets[i] = start..end;
            } else {
                duplicates.push((k, v));
            }
        }
        duplicates
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

use crate::validated::dataframe::HasLen;

use fireflow_types::nonempty::NEStr;

use derive_new::new;
use derive_where::derive_where;
use fireflow_types::std_key::{AnyIndex, EnumIndex, StdKey};

use std::iter;
use std::marker::PhantomData;
use std::ops::Index;

pub type NestedEnumString<const LEN: usize, K> = NestedString<[usize; LEN], (), K>;

pub type NestedVariableString<K, S> = NestedString<Vec<usize>, S, K>;

#[derive_where(Default, Clone, Debug, PartialEq, Eq; I, S)]
pub struct NestedString<I, S, K> {
    inner: Vec<u8>,
    offsets: I,
    sub_dimension: S,
    _key: PhantomData<K>,
}

#[derive(new, Clone, Copy, Default)]
pub struct NestedStringSize {
    pub n_bytes: usize,
    pub n_strings: usize,
}

pub(crate) struct Iter<'a, I, K: AnyIndex> {
    keys: K::Generator,
    inner: &'a NestedString<I, K::SubDimension, K>,
    index: usize,
}

pub(crate) type IterStd<'a, I, K> =
    iter::FilterMap<Iter<'a, I, K>, fn((K, &str)) -> Option<(StdKey, &NEStr)>>;

impl<const LEN: usize, K> NestedEnumString<LEN, K> {
    pub fn init_array(n_bytes: usize) -> Self {
        Self {
            inner: Vec::with_capacity(n_bytes),
            offsets: [0; LEN],
            sub_dimension: (),
            _key: PhantomData,
        }
    }

    pub(crate) unsafe fn set_keys<V>(&mut self, pairs: impl IntoIterator<Item = (K, V)>)
    where
        K: EnumIndex<LEN>,
        V: AsRef<NEStr>,
    {
        for (k, v) in pairs {
            self.offsets[k.index()] = self.inner.len();
            self.inner.extend(v.as_ref().as_str().as_bytes());
        }
    }
}

impl<K, S> NestedVariableString<K, S> {
    pub fn init_var(size: &NestedStringSize, sub_dimension: S) -> Self {
        Self {
            inner: Vec::with_capacity(size.n_bytes),
            offsets: Vec::with_capacity(size.n_strings),
            sub_dimension,
            _key: PhantomData,
        }
    }

    /// # Safety
    ///
    /// - The index of each pair must be in order.
    /// - This must only be called once on a freshly init-ed object.
    pub(crate) unsafe fn extend_pairs<V>(&mut self, pairs: impl IntoIterator<Item = (usize, V)>)
    where
        V: AsRef<NEStr>,
    {
        for (i, v) in pairs {
            // Pad the index vector with previous length up until the index
            // to be added. These are blank strings that we skipped by not
            // explicitly passing a pair for it.
            for _ in self.offsets.len()..i {
                self.offsets.push(self.inner.len());
            }
            self.offsets.push(self.inner.len());
            self.inner.extend(v.as_ref().as_str().as_bytes());
        }
    }
}

impl<I, S, K> NestedString<I, S, K> {
    // pub(crate) fn n_bytes(&self) -> usize {
    //     self.inner.len()
    // }

    pub(crate) fn n_strings(&self) -> usize
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
        I: HasLen + Index<usize, Output = usize>,
        K: AnyIndex<SubDimension = S>,
    {
        self.get_index(k.offset(&self.sub_dimension))
    }

    pub(crate) fn occupied(&self, k: &K) -> Option<bool>
    where
        I: HasLen + Index<usize, Output = usize>,
        K: AnyIndex<SubDimension = S>,
    {
        self.get(k).map(|v| !v.is_empty())
    }

    pub(crate) fn get_unchecked(&self, k: &K) -> &str
    where
        I: HasLen + Index<usize, Output = usize>,
        K: AnyIndex<SubDimension = S>,
    {
        self.get_index_unchecked(k.offset(&self.sub_dimension))
    }

    pub(crate) fn get_index(&self, i: usize) -> Option<&str>
    where
        I: HasLen + Index<usize, Output = usize>,
    {
        (i < self.offsets.len()).then(|| self.get_index_unchecked(i))
    }

    pub(crate) fn get_index_unchecked(&self, i: usize) -> &str
    where
        I: HasLen + Index<usize, Output = usize>,
    {
        let n = self.offsets.len();
        assert!(i < n, "index out of bounds: {i}");
        let start = self.offsets[i];
        let end = if i == n - 1 {
            self.inner.len()
        } else {
            self.offsets[i + 1]
        };
        // SAFETY: this struct is validated such that each slice is a string
        unsafe { self.get_range(start, end) }
    }

    unsafe fn get_range(&self, start: usize, end: usize) -> &str {
        // SAFETY: this function is unsafe
        unsafe { str::from_utf8_unchecked(&self.inner[start..end]) }
    }

    pub(crate) fn iter_std(&self) -> IterStd<'_, I, K>
    where
        I: HasLen + Index<usize, Output = usize>,
        K: AnyIndex<SubDimension = S> + Into<StdKey>,
    {
        self.iter()
            .filter_map(|(k, v)| NEStr::try_new(v).map(|ne| (k.into(), ne)))
    }

    pub(crate) fn iter(&self) -> Iter<'_, I, K>
    where
        I: HasLen + Index<usize, Output = usize>,
        K: AnyIndex<SubDimension = S>,
    {
        Iter {
            keys: K::generate(&self.sub_dimension),
            inner: self,
            index: 0,
        }
    }
}

impl<'a, I, K> Iterator for Iter<'a, I, K>
where
    K: AnyIndex,
    I: HasLen + Index<usize, Output = usize>,
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

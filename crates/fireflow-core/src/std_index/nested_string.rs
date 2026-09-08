use crate::validated::dataframe::HasLen;

use fireflow_types::nonempty_string::NEStr;

use derive_new::new;
use fireflow_types::std_key::{AnyIndex, EnumIndex};
use itertools::Itertools as _;

use std::iter::once;
use std::marker::PhantomData;
use std::ops::Index;

pub type NestedEnumString<const LEN: usize, K> = NestedString<[usize; LEN], K>;

pub type NestedVariableString<K> = NestedString<Vec<usize>, K>;

pub struct NestedString<I, K> {
    inner: Vec<u8>,
    indices: I,
    _key: PhantomData<K>,
}

#[derive(new, Clone, Copy, Default)]
pub struct NestedStringSize {
    pub n_bytes: usize,
    pub n_strings: usize,
}

impl<const LEN: usize, K> NestedEnumString<LEN, K> {
    pub fn init_array(n_bytes: usize) -> Self {
        Self {
            inner: Vec::with_capacity(n_bytes),
            indices: [0; LEN],
            _key: PhantomData,
        }
    }

    pub(crate) unsafe fn set_keys<'a>(&mut self, pairs: impl IntoIterator<Item = (K, &'a NEStr)>)
    where
        K: EnumIndex<LEN>,
    {
        for (k, v) in pairs {
            self.indices[k.index()] = self.inner.len();
            self.inner.extend(v.as_str().as_bytes());
        }
    }

    pub(crate) unsafe fn set_key(&mut self, key: K, val: &NEStr)
    where
        K: EnumIndex<LEN>,
    {
        let start = self.inner.len();
        self.inner.extend(val.as_str().as_bytes());
        self.indices[key.index()] = start;
    }
}

impl<K> NestedVariableString<K> {
    pub fn init_var(size: &NestedStringSize) -> Self {
        Self {
            inner: Vec::with_capacity(size.n_bytes),
            indices: Vec::with_capacity(size.n_strings),
            _key: PhantomData,
        }
    }

    /// # Safety
    ///
    /// - The index of each pair must be in order.
    /// - This must only be called once on a freshly init-ed object.
    pub(crate) unsafe fn extend_pairs<'a>(
        &mut self,
        pairs: impl IntoIterator<Item = (usize, &'a NEStr)>,
    ) {
        for (i, v) in pairs {
            // Pad the index vector with previous length up until the index
            // to be added. These are blank strings that we skipped by not
            // explicitly passing a pair for it.
            for _ in self.indices.len()..i {
                self.indices.push(self.inner.len());
            }
            self.indices.push(self.inner.len());
            self.inner.extend(v.as_str().as_bytes());
        }
    }

    fn extend<'a>(&mut self, ss: impl IntoIterator<Item = &'a str>) {
        let mut prev_index = self.indices.last().copied().unwrap_or(0);
        for s in ss {
            let bs = s.as_bytes();
            self.inner.extend(bs);
            self.indices.push(prev_index);
            prev_index += bs.len();
        }
    }

    pub fn push(&mut self, s: &str) {
        let prev_index = self.indices.last().copied().unwrap_or(0);
        self.inner.extend(s.as_bytes());
        self.indices.push(prev_index);
    }
}

impl<I, K> NestedString<I, K> {
    pub fn n_bytes(&self) -> usize {
        self.inner.len()
    }

    pub fn n_strings(&self) -> usize
    where
        I: HasLen,
    {
        self.indices.len()
    }

    pub fn get(&self, k: &K, sub: &K::SubDimension) -> &str
    where
        I: HasLen + Index<usize, Output = usize>,
        K: AnyIndex,
    {
        let i = k.offset(sub);
        let n = self.indices.len();
        assert!(i < n, "index out of bounds: {i}");
        let start = self.indices[i];
        let end = if i == n - 1 {
            self.inner.len()
        } else {
            self.indices[i + 1]
        };
        // SAFETY: this struct is validated such that each slice is a string
        unsafe { self.get_range(start, end) }
    }

    pub fn get0(&self, k: &K) -> &str
    where
        I: HasLen + Index<usize, Output = usize>,
        K: AnyIndex<SubDimension = ()>,
    {
        self.get(k, &())
    }

    unsafe fn get_range(&self, start: usize, end: usize) -> &str {
        // SAFETY: this function is unsafe
        unsafe { str::from_utf8_unchecked(&self.inner[start..end]) }
    }

    pub(crate) fn iter_pairs(&self, sub: &K::SubDimension) -> impl Iterator<Item = (K, &NEStr)>
    where
        for<'a> &'a I: IntoIterator<Item = &'a usize>,
        K: AnyIndex,
    {
        K::generate(&sub).zip(self.iter())
    }

    fn iter(&self) -> impl Iterator<Item = &NEStr>
    where
        for<'a> &'a I: IntoIterator<Item = &'a usize>,
    {
        (&self.indices)
            .into_iter()
            .copied()
            .chain(once(self.inner.len()))
            .tuple_windows()
            .map(|(start, end)| {
                // SAFETY: this struct is validated such that each slice is a string
                unsafe { self.get_range(start, end) }
            })
            .filter_map(NEStr::try_new)
    }
}

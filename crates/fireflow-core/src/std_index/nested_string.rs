use crate::validated::dataframe::HasLen;

use fireflow_types::nonempty::NEStr;

use derive_new::new;
use derive_where::derive_where;
use fireflow_types::std_key::{AnyIndex, EnumIndex, StdKey};

use std::iter;
use std::marker::PhantomData;
use std::ops::Index;

pub type NestedEnumString<const LEN: usize, K> = NestedString<[usize; LEN], K>;

pub type NestedVariableString<K> = NestedString<Vec<usize>, K>;

#[derive_where(Default, Clone, Debug, PartialEq; I)]
pub struct NestedString<I, K> {
    inner: Vec<u8>,
    offsets: I,
    _key: PhantomData<K>,
}

#[derive(new, Clone, Copy, Default)]
pub struct NestedStringSize {
    pub n_bytes: usize,
    pub n_strings: usize,
}

pub(crate) struct Iter<'a, I, K> {
    inner: &'a NestedString<I, K>,
    index: usize,
}

pub(crate) type IterPairs<'a, I, K> = iter::Zip<<K as AnyIndex>::Generator, Iter<'a, I, K>>;

pub(crate) type IterKeywords<'a, I, K> = iter::Map<
    iter::Zip<<K as AnyIndex>::Generator, Iter<'a, I, K>>,
    fn((K, &NEStr)) -> (StdKey, &NEStr),
>;

pub(crate) type IterEnumKeywords<'a, const LEN: usize, K> = IterKeywords<'a, [usize; LEN], K>;

pub(crate) type IterVariableKeywords<'a, K> = IterKeywords<'a, Vec<usize>, K>;

impl<const LEN: usize, K> NestedEnumString<LEN, K> {
    pub fn init_array(n_bytes: usize) -> Self {
        Self {
            inner: Vec::with_capacity(n_bytes),
            offsets: [0; LEN],
            _key: PhantomData,
        }
    }

    pub(crate) unsafe fn set_keys<'a>(&mut self, pairs: impl IntoIterator<Item = (K, &'a NEStr)>)
    where
        K: EnumIndex<LEN>,
    {
        for (k, v) in pairs {
            self.offsets[k.index()] = self.inner.len();
            self.inner.extend(v.as_str().as_bytes());
        }
    }

    pub(crate) unsafe fn set_key(&mut self, key: K, val: &NEStr)
    where
        K: EnumIndex<LEN>,
    {
        let start = self.inner.len();
        self.inner.extend(val.as_str().as_bytes());
        self.offsets[key.index()] = start;
    }
}

impl<K> NestedVariableString<K> {
    pub fn init_var(size: &NestedStringSize) -> Self {
        Self {
            inner: Vec::with_capacity(size.n_bytes),
            offsets: Vec::with_capacity(size.n_strings),
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
            for _ in self.offsets.len()..i {
                self.offsets.push(self.inner.len());
            }
            self.offsets.push(self.inner.len());
            self.inner.extend(v.as_str().as_bytes());
        }
    }

    fn extend<'a>(&mut self, ss: impl IntoIterator<Item = &'a str>) {
        let mut prev_index = self.offsets.last().copied().unwrap_or(0);
        for s in ss {
            let bs = s.as_bytes();
            self.inner.extend(bs);
            self.offsets.push(prev_index);
            prev_index += bs.len();
        }
    }

    pub fn push(&mut self, s: &str) {
        let prev_index = self.offsets.last().copied().unwrap_or(0);
        self.inner.extend(s.as_bytes());
        self.offsets.push(prev_index);
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
        self.offsets.len()
    }

    pub fn get(&self, k: &K, sub: &K::SubDimension) -> &str
    where
        I: HasLen + Index<usize, Output = usize>,
        K: AnyIndex,
    {
        self.get_index(k.offset(sub))
    }

    pub fn get0(&self, k: &K) -> &str
    where
        I: HasLen + Index<usize, Output = usize>,
        K: AnyIndex<SubDimension = ()>,
    {
        self.get(k, &())
    }

    pub fn get_index(&self, i: usize) -> &str
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

    pub(crate) fn iter_keywords<'a>(&'a self, sub: &K::SubDimension) -> IterKeywords<'a, I, K>
    where
        I: HasLen + Index<usize, Output = usize>,
        K: AnyIndex + Into<StdKey>,
    {
        self.iter_pairs(sub).map(|(k, v)| (k.into(), v))
    }

    pub(crate) fn iter_pairs<'a>(&'a self, sub: &K::SubDimension) -> IterPairs<'a, I, K>
    where
        I: HasLen + Index<usize, Output = usize>,
        K: AnyIndex,
    {
        K::generate(&sub).zip(self.iter())
    }

    fn iter<'a>(&'a self) -> Iter<'a, I, K>
    where
        I: HasLen + Index<usize, Output = usize>,
    {
        Iter {
            inner: &self,
            index: 0,
        }
    }
}

impl<'a, I, K> Iterator for Iter<'a, I, K>
where
    I: HasLen + Index<usize, Output = usize>,
{
    type Item = &'a NEStr;

    fn next(&mut self) -> Option<Self::Item> {
        while self.index < self.inner.offsets.len() {
            let s = self.inner.get_index(self.index);
            self.index += 1;
            if let Some(ne) = NEStr::try_new(s) {
                return Some(ne);
            }
        }
        None
    }
}

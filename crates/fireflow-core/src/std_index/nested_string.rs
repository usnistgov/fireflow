use crate::validated::dataframe::HasLen;

use fireflow_types::nonempty_string::NEStr;

use derive_new::new;
use itertools::Itertools as _;
use strum::{EnumCount, IntoEnumIterator};

use std::iter::once;
use std::marker::PhantomData;
use std::ops::Index;

pub type NestedEnumString<const LEN: usize, K> = NestedString<[usize; LEN], K>;

pub type NestedVariableString = NestedString<Vec<usize>, ()>;

pub struct NestedString<I, K> {
    inner: Vec<u8>,
    indices: I,
    // size: NestedStringSize,
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
            // size: NestedStringSize::new(n_bytes, LEN),
            _key: PhantomData,
        }
    }

    // TODO make a test for instances of this to make sure the const and enum
    // are same length
    pub(crate) fn iter_keys(&self) -> impl Iterator<Item = (K, &str)>
    where
        K: EnumCount + IntoEnumIterator,
    {
        assert_eq!(
            K::COUNT,
            LEN,
            "array does not match enum length, this should not happen"
        );
        K::iter().zip(self.iter())
    }

    pub(crate) unsafe fn set_keys<'a>(&mut self, pairs: impl IntoIterator<Item = (K, &'a NEStr)>)
    where
        K: Into<usize>,
    {
        for (k, v) in pairs {
            self.indices[k.into()] = self.inner.len();
            self.inner.extend(v.as_str().as_bytes());
        }
    }

    pub(crate) unsafe fn set_key(&mut self, key: K, val: &NEStr)
    where
        K: Into<usize>,
    {
        let start = self.inner.len();
        self.inner.extend(val.as_str().as_bytes());
        self.indices[key.into()] = start
    }
}

impl NestedVariableString {
    pub fn init_var(size: &NestedStringSize) -> Self {
        Self {
            inner: Vec::with_capacity(size.n_bytes),
            indices: Vec::with_capacity(size.n_strings),
            // size,
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
    // pub fn n_bytes(&self) -> usize {
    //     self.size.n_bytes
    // }

    // pub fn n_strings(&self) -> usize {
    //     self.size.n_strings
    // }

    fn get(&self, i: usize) -> &str
    where
        I: HasLen + Index<usize, Output = usize>,
    {
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

    unsafe fn get_range(&self, start: usize, end: usize) -> &str {
        // SAFETY: this function is unsafe
        unsafe { str::from_utf8_unchecked(&self.inner[start..end]) }
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &str>
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
    }
}

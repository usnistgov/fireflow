use fireflow_types::{
    std_key::{AnyIndex, StdKey},
};

use crate::validated::dataframe::HasLen;

use super::nested_string::{IterKeywords, NestedString, NestedStringSize};

use std::{
    marker::PhantomData,
    ops::{Index, IndexMut},
};

pub type MaskedEnumString<const LEN: usize, K, M> = MaskedString<[usize; LEN], K, [M; LEN], M>;

pub type MaskedVariableString<K, M> = MaskedString<Vec<usize>, K, Vec<M>, M>;

pub struct MaskedString<I, K, C, M> {
    inner: NestedString<I, K>,
    mask: C,
    _mask_element: PhantomData<M>,
}

impl<const LEN: usize, K, M: Default + Copy> MaskedEnumString<LEN, K, M> {
    pub fn init_array(n_bytes: usize) -> Self {
        Self {
            inner: NestedString::init_array(n_bytes),
            mask: [M::default(); LEN],
            _mask_element: PhantomData,
        }
    }
}

impl<K, M: Default + Copy> MaskedVariableString<K, M> {
    pub fn init_var(size: &NestedStringSize) -> Self {
        Self {
            inner: NestedString::init_var(size),
            mask: vec![M::default(); size.n_strings],
            _mask_element: PhantomData,
        }
    }
}

impl<I, K, C, M> MaskedString<I, K, C, M> {
    pub fn get_value(&self, k: &K, sub: &K::SubDimension) -> &str
    where
        I: HasLen + Index<usize, Output = usize>,
        K: AnyIndex,
    {
        self.inner.get(k, sub)
    }

    // pub fn get_with<F, X>(&mut self, i: usize, f: F) -> X
    // where
    //     I: HasLen + Index<usize, Output = usize>,
    //     C: Index<usize, Output = M> + IndexMut<usize, Output = M>,
    //     for<'a> F: FnOnce(&M, &'a str) -> (M, X),
    // {
    //     let m0 = self.get_mask(i);
    //     let e0 = self.inner.get(i);
    //     let (m1, out) = f(m0, e0);
    //     self.set_mask(i, m1);
    //     out
    // }

    pub fn get_mask(&self, k: &K, sub: &K::SubDimension) -> &M
    where
        I: HasLen,
        C: Index<usize, Output = M>,
        K: AnyIndex,
    {
        let i = k.offset(sub);
        let n = self.inner.n_strings();
        assert!(i < n, "index out of bounds: {i}");
        &self.mask[i]
    }

    pub fn set_mask(&mut self, k: &K, sub: &K::SubDimension, m: M)
    where
        I: HasLen,
        C: IndexMut<usize, Output = M>,
        K: AnyIndex,
    {
        let i = k.offset(sub);
        let n = self.inner.n_strings();
        assert!(i < n, "index out of bounds: {i}");
        self.mask[i] = m;
    }

    pub(crate) fn iter_keywords<'a>(&'a self, sub: &K::SubDimension) -> IterKeywords<'a, I, K>
    where
        I: HasLen + Index<usize, Output = usize>,
        K: AnyIndex + Into<StdKey>,
    {
        self.inner.iter_keywords(sub)
    }
}

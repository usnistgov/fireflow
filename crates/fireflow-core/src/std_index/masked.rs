use crate::validated::dataframe::HasLen;

use super::nested_string::{NestedString, NestedStringSize};

use std::{
    marker::PhantomData,
    ops::{Index, IndexMut},
};

pub type MaskedEnumString<const LEN: usize, K, M> = MaskedString<[usize; LEN], K, [M; LEN], M>;

pub type MaskedVariableString<M> = MaskedString<Vec<usize>, (), Vec<M>, M>;

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

impl<M: Default + Copy> MaskedVariableString<M> {
    pub fn init_var(size: &NestedStringSize) -> Self {
        Self {
            inner: NestedString::init_var(size),
            mask: vec![M::default(); size.n_strings],
            _mask_element: PhantomData,
        }
    }
}

impl<I, K, C, M> MaskedString<I, K, C, M> {
    pub fn get_with<F>(&self, i: usize, f: F) -> (&M, &str)
    where
        I: HasLen + Index<usize, Output = usize>,
        C: Index<usize, Output = M>,
        for<'a, 'b> F: FnOnce(&'a M, &'b str) -> (&'a M, &'b str),
    {
        let m0 = self.get_mask(i);
        let e0 = self.inner.get(i);
        f(m0, e0)
    }

    pub fn get_with_mut<F, X>(&mut self, i: usize, f: F) -> X
    where
        I: HasLen + Index<usize, Output = usize>,
        C: Index<usize, Output = M> + IndexMut<usize, Output = M>,
        F: FnOnce(&M, &str) -> (M, X),
    {
        let m0 = self.get_mask(i);
        let e0 = self.inner.get(i);
        let (m1, out) = f(m0, e0);
        self.set_mask(i, m1);
        out
    }

    pub fn get_mask(&self, i: usize) -> &M
    where
        I: HasLen,
        C: Index<usize, Output = M>,
    {
        let n = self.inner.n_strings();
        assert!(i < n, "index out of bounds: {i}");
        &self.mask[i]
    }

    pub fn set_mask(&mut self, i: usize, m: M)
    where
        I: HasLen,
        C: IndexMut<usize, Output = M>,
    {
        let n = self.inner.n_strings();
        assert!(i < n, "index out of bounds: {i}");
        self.mask[i] = m;
    }
}

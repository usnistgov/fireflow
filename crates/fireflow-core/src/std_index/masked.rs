use crate::std_index::nested_string::{Iter, NestedEnumString, NestedString, NestedVariableString};
use crate::validated::dataframe::HasLen;

use fireflow_types::keys::raw_std::{EnumIndex, NumericEnum};
use nonempty::NEStr;

use derive_new::new;

use std::array::from_fn;
use std::fmt;
use std::marker::PhantomData;
use std::ops::{Index, IndexMut, Range};

use super::index::LookupAction;
use super::nested_string::NestedStringSize;

pub type MaskedEnumString<'a, const LEN: usize, K, M> =
    MaskedString<'a, [Range<usize>; LEN], (), K, [M; LEN], M>;

pub type MaskedVariableString<'a, K, S, M> = MaskedString<'a, Vec<Range<usize>>, S, K, Vec<M>, M>;

#[derive(new)]
pub(crate) struct MaskedString<'a, I, S, K, C, M> {
    inner: &'a NestedString<I, S, K>,
    mask: C,
    _mask_element: PhantomData<M>,
}

#[derive(Clone, Copy, Default, Debug)]
pub enum LookupMask {
    #[default]
    Unseen,
    Seen(LookupAction),
}

impl<const LEN: usize, K> MaskedEnumString<'_, LEN, K, LookupMask> {
    pub(crate) fn commit_array(self) -> NestedEnumString<LEN, K>
    where
        K: EnumIndex<SubDimension = ()> + NumericEnum<LEN>,
    {
        let n_bytes = self.iter_final().map(|(_, v)| v.len().get()).sum();
        let mut new = NestedString::init_array(n_bytes);
        let dups = new.extend_dedup(self.iter_final());
        assert!(dups.is_empty(), "there should be no duplicates");
        new
    }
}

impl<K, S> MaskedVariableString<'_, K, S, LookupMask> {
    pub(crate) fn commit_var(self) -> NestedVariableString<K, S>
    where
        S: Copy,
        K: EnumIndex<SubDimension = S>,
    {
        let n_bytes = self.iter_final().map(|(_, v)| v.len().get()).sum();
        let n_offsets = self.inner.n_offsets();
        let s = self.inner.sub_dimension();
        let size = NestedStringSize::new(n_bytes, n_offsets);
        let mut new = NestedString::init_var(&size, *s);
        let dups = new.extend_dedup(self.iter_final());
        assert!(dups.is_empty(), "there should be no duplicates");
        new
    }
}

impl<'a, const LEN: usize, K, M: Default> MaskedEnumString<'a, LEN, K, M> {
    pub fn init_array(inner: &'a NestedEnumString<LEN, K>) -> Self {
        let mask = from_fn(|_| M::default());
        Self::new(inner, mask)
    }
}

impl<'a, K, S, M: Default> MaskedVariableString<'a, K, S, M> {
    pub fn init_var(inner: &'a NestedVariableString<K, S>) -> Self {
        let n = inner.n_offsets();
        let mut mask = Vec::with_capacity(n);
        mask.resize_with(n, || M::default());
        Self {
            inner,
            mask,
            _mask_element: PhantomData,
        }
    }
}

impl<'a, I, S, K, C> MaskedString<'a, I, S, K, C, LookupMask> {
    pub(crate) fn parse_unseen<F, X>(&mut self, k: &K, f: F) -> Option<X>
    where
        F: FnOnce(&NEStr) -> (Option<LookupAction>, X),
        I: HasLen + Index<usize, Output = Range<usize>>,
        K: EnumIndex<SubDimension = S> + fmt::Debug,
        C: IndexMut<usize, Output = LookupMask>,
    {
        let (v, m) = self.get_value_and_mask_mut(k)?;
        let (action, ret) = f(m.with_unseen(k, v));
        let new_action = action.unwrap_or(LookupAction::None);
        *m = LookupMask::Seen(new_action);
        Some(ret)
    }

    pub(crate) fn remove_unseen(&mut self, k: &K) -> Option<&NEStr>
    where
        I: HasLen + Index<usize, Output = Range<usize>>,
        K: EnumIndex<SubDimension = S> + fmt::Debug,
        C: IndexMut<usize, Output = LookupMask>,
    {
        let (v, m) = self.get_value_and_mask_mut(k)?;
        let ret = m.with_unseen(k, v);
        *m = LookupMask::Seen(LookupAction::None);
        Some(ret)
    }

    pub(crate) fn get_unseen(&self, k: &K) -> Option<&NEStr>
    where
        I: HasLen + Index<usize, Output = Range<usize>>,
        K: EnumIndex<SubDimension = S> + fmt::Debug,
        C: Index<usize, Output = LookupMask>,
    {
        let m = self.get_mask(k)?;
        let v = self.get_value(k)?;
        Some(m.with_unseen(k, v))
    }

    pub(crate) fn set_lookup_action_seen(&mut self, k: &K, a: LookupAction)
    where
        I: HasLen + Index<usize, Output = Range<usize>>,
        K: EnumIndex<SubDimension = S>,
        C: IndexMut<usize, Output = LookupMask>,
    {
        if self.get_value(k).is_none() {
            panic!("attempted to set lookup action on empty value")
        } else {
            match self.get_mask_mut(k) {
                Some(LookupMask::Unseen) => {
                    panic!("attempted to set lookup action on unseen value")
                }
                Some(LookupMask::Seen(s)) => *s = a,
                None => panic!("index out of bounds"),
            }
        }
    }

    pub(crate) fn iter_final<'b>(&'b self) -> impl Iterator<Item = (K, &'a NEStr)>
    where
        I: HasLen + Index<usize, Output = Range<usize>>,
        K: EnumIndex<SubDimension = S>,
        &'b C: IntoIterator<Item = &'b LookupMask> + 'a,
    {
        self.iter_masked().filter_map(|(k, v, m)| match m {
            LookupMask::Unseen | LookupMask::Seen(LookupAction::None) => Some((k, v)),
            LookupMask::Seen(_) => None,
        })
    }

    pub(crate) fn iter_masked<'b>(&'b self) -> impl Iterator<Item = (K, &'a NEStr, &'b LookupMask)>
    where
        I: HasLen + Index<usize, Output = Range<usize>>,
        K: EnumIndex<SubDimension = S>,
        &'b C: IntoIterator<Item = &'b LookupMask> + 'a,
    {
        self.iter()
            .zip(&self.mask)
            .filter_map(|((k, v), m)| NEStr::try_new(v).map(|ne| (k, ne, m)))
    }
}

impl<I, S, K, C, M> MaskedString<'_, I, S, K, C, M> {
    fn get_value(&self, k: &K) -> Option<&NEStr>
    where
        I: HasLen + Index<usize, Output = Range<usize>>,
        K: EnumIndex<SubDimension = S>,
    {
        NEStr::try_new(self.inner.get(k)?)
    }

    fn get_mask(&self, k: &K) -> Option<&M>
    where
        I: HasLen,
        C: Index<usize, Output = M>,
        K: EnumIndex<SubDimension = S>,
    {
        let i = k.offset(self.inner.sub_dimension());
        let n = self.inner.n_offsets();
        (i < n).then(|| &self.mask[i])
    }

    fn get_value_and_mask_mut(&mut self, k: &K) -> Option<(&NEStr, &mut M)>
    where
        I: HasLen + Index<usize, Output = Range<usize>>,
        C: IndexMut<usize, Output = M>,
        K: EnumIndex<SubDimension = S>,
    {
        let i = k.offset(self.inner.sub_dimension());
        let n = self.inner.n_offsets();
        if i < n {
            let v = NEStr::try_new(self.inner.get_index_unchecked(i))?;
            Some((v, &mut self.mask[i]))
        } else {
            None
        }
    }

    fn get_mask_mut(&mut self, k: &K) -> Option<&mut M>
    where
        I: HasLen,
        C: IndexMut<usize, Output = M>,
        K: EnumIndex<SubDimension = S>,
    {
        let i = k.offset(self.inner.sub_dimension());
        let n = self.inner.n_offsets();
        (i < n).then(|| &mut self.mask[i])
    }

    pub(crate) fn iter(&self) -> Iter<'_, I, K>
    where
        I: HasLen + Index<usize, Output = Range<usize>>,
        K: EnumIndex<SubDimension = S>,
    {
        self.inner.iter()
    }
}

impl LookupMask {
    fn with_unseen<'a, K: fmt::Debug>(self, k: &K, v: &'a NEStr) -> &'a NEStr {
        match self {
            Self::Unseen => v,
            Self::Seen(_) => {
                panic!("tried to look up key {k:?} with value '{v}' which was already seen")
            }
        }
    }
}

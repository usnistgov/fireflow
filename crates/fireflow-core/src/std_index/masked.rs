use crate::std_index::nested_string::{Iter, NestedEnumString, NestedString, NestedVariableString};
use crate::validated::dataframe::HasLen;

use fireflow_types::std_key::{EnumIndex, NumericEnum, StdKey};
use nonempty::{NEStr, NEString};

use derive_new::new;

use std::array::from_fn;
use std::fmt;
use std::marker::PhantomData;
use std::ops::{Index, IndexMut};

use super::index::LookupAction;
use super::nested_string::NestedStringSize;

pub type MaskedEnumString<'a, const LEN: usize, K, M> =
    MaskedString<'a, [usize; LEN], (), K, [M; LEN], (), M>;

pub type MaskedVariableString<'a, K, S, M> =
    MaskedString<'a, Vec<usize>, S, K, Vec<M>, Vec<(K, M, NEString)>, M>;

#[derive(new)]
pub(crate) struct MaskedString<'a, I, S, K, C, A, M> {
    inner: &'a NestedString<I, S, K>,
    mask: C,
    appended: A,
    _mask_element: PhantomData<M>,
}

#[derive(new, Clone, Default, Debug)]
pub struct LookupMask {
    repair: RepairMask,
    status: LookupStatus,
}

#[derive(Clone, Default, Debug)]
pub(crate) enum RepairMask {
    #[default]
    Stored,
    Delete,
    Insert(NEString),
}

#[derive(Clone, Copy, Default, Debug)]
pub(crate) enum LookupStatus {
    #[default]
    Unseen,
    Seen(LookupAction),
}

pub(crate) trait AppendableContainer<K, M> {
    fn find_mask(&self, k: &K) -> Option<&M>;

    fn find_mask_mut(&mut self, k: &K) -> Option<&mut M>;

    fn find_value(&self, k: &K) -> Option<&NEStr>;

    fn find_value_and_mask_mut(&mut self, k: &K) -> Option<(&mut M, &NEStr)>;

    fn insert(&mut self, k: &K, v: NEString) -> Option<NEString>;
}

impl<K, M> AppendableContainer<K, M> for () {
    fn find_mask(&self, _: &K) -> Option<&M> {
        None
    }

    fn find_mask_mut(&mut self, _: &K) -> Option<&mut M> {
        None
    }

    fn find_value(&self, _: &K) -> Option<&NEStr> {
        None
    }

    fn find_value_and_mask_mut(&mut self, _: &K) -> Option<(&mut M, &NEStr)> {
        None
    }

    fn insert(&mut self, _: &K, v: NEString) -> Option<NEString> {
        Some(v)
    }
}

impl<K, M> AppendableContainer<K, M> for Vec<(K, M, NEString)>
where
    K: PartialEq + Copy,
    M: Default,
{
    fn find_mask(&self, k: &K) -> Option<&M> {
        self.iter()
            .position(|(k0, _, _)| k0 == k)
            .map(|i| &self[i].1)
    }

    fn find_mask_mut(&mut self, k: &K) -> Option<&mut M> {
        self.iter()
            .position(|(k0, _, _)| k0 == k)
            .map(|i| &mut self[i].1)
    }

    fn find_value(&self, k: &K) -> Option<&NEStr> {
        self.iter()
            .position(|(k0, _, _)| k0 == k)
            .map(|i| self[i].2.as_ne_str())
    }

    fn find_value_and_mask_mut(&mut self, k: &K) -> Option<(&mut M, &NEStr)> {
        self.iter().position(|(k0, _, _)| k0 == k).map(|i| {
            let this = &mut self[i];
            (&mut this.1, this.2.as_ne_str())
        })
    }

    fn insert(&mut self, k: &K, v: NEString) -> Option<NEString> {
        if self.iter().position(|(k0, _, _)| k0 == k).is_none() {
            self.push((*k, M::default(), v));
            None
        } else {
            Some(v)
        }
    }
}

impl<'a, const LEN: usize, K> MaskedEnumString<'a, LEN, K, RepairMask> {
    pub(crate) fn into_lookup_array(self) -> MaskedEnumString<'a, LEN, K, LookupMask> {
        let mask = self
            .mask
            .map(|s| LookupMask::new(s, LookupStatus::default()));
        MaskedString::new(self.inner, mask, ())
    }
}

impl<'a, K, S> MaskedVariableString<'a, K, S, RepairMask> {
    pub(crate) fn into_lookup_var(self) -> MaskedVariableString<'a, K, S, LookupMask> {
        let mask = self
            .mask
            .into_iter()
            .map(|s| LookupMask::new(s, LookupStatus::default()))
            .collect();
        let appended = self
            .appended
            .into_iter()
            .map(|(k, m, v)| (k, LookupMask::new(m, LookupStatus::default()), v))
            .collect();
        MaskedString::new(self.inner, mask, appended)
    }
}

impl<const LEN: usize, K> MaskedEnumString<'_, LEN, K, LookupMask> {
    pub(crate) fn commit_array(self) -> NestedEnumString<LEN, K>
    where
        K: EnumIndex<SubDimension = ()> + NumericEnum<LEN>,
    {
        let n_bytes = self.iter_final().map(|(_, v)| v.len().get()).sum();
        let mut new = NestedString::init_array(n_bytes);
        // SAFETY: input should be sorted and will not contain duplicates
        unsafe { new.set_keys(self.iter_final()) }
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
        let n_strings = self.iter_final().count();
        let s = self.inner.sub_dimension();
        let size = NestedStringSize::new(n_bytes, n_strings);
        let mut new = NestedString::init_var(&size, *s);
        let it = self.iter_final().map(|(k, v)| (k.offset(s), v));
        // SAFETY: input should be sorted and will not contain duplicates
        unsafe { new.extend_pairs(it) }
        new
    }
}

impl<'a, const LEN: usize, K, M: Default> MaskedEnumString<'a, LEN, K, M> {
    pub fn init_array(inner: &'a NestedEnumString<LEN, K>) -> Self {
        let mask = from_fn(|_| M::default());
        Self::new(inner, mask, ())
    }
}

impl<'a, K, S, M: Default> MaskedVariableString<'a, K, S, M> {
    pub fn init_var(inner: &'a NestedVariableString<K, S>) -> Self {
        let n = inner.n_strings();
        let mut mask = Vec::with_capacity(n);
        mask.resize_with(n, || M::default());
        Self {
            inner,
            mask,
            appended: vec![],
            _mask_element: PhantomData,
        }
    }
}

impl<I, S, K, C, A> MaskedString<'_, I, S, K, C, A, RepairMask> {
    pub(crate) fn delete(&mut self, k: &K) -> Option<&NEStr>
    where
        I: HasLen + Index<usize, Output = usize>,
        C: IndexMut<usize, Output = RepairMask>,
        K: EnumIndex<SubDimension = S>,
        A: AppendableContainer<K, RepairMask>,
    {
        if let Some((v, m)) = self.get_value_and_mask_mut(k)
            && let Some(ne) = NEStr::try_new(v)
        {
            *m = RepairMask::Delete;
            Some(ne)
        } else {
            None
        }
    }

    pub(crate) fn insert(&mut self, k: &K, v: NEString) -> Option<NEString>
    where
        I: HasLen + Index<usize, Output = usize>,
        C: IndexMut<usize, Output = RepairMask>,
        K: EnumIndex<SubDimension = S>,
        A: AppendableContainer<K, RepairMask>,
    {
        if self.key_has_value(k) {
            Some(v)
        } else if let Some(m) = self.get_mask_mut(k) {
            *m = RepairMask::Insert(v);
            None
        } else {
            self.appended.insert(k, v)
        }
    }

    pub(crate) fn key_has_value(&self, k: &K) -> bool
    where
        I: HasLen + Index<usize, Output = usize>,
        K: EnumIndex<SubDimension = S>,
        C: Index<usize, Output = RepairMask>,
        A: AppendableContainer<K, RepairMask>,
    {
        if let Some(m) = self.get_mask(k) {
            match m {
                RepairMask::Stored => self.occupied(k) == Some(true),
                RepairMask::Insert(_) => true,
                RepairMask::Delete => false,
            }
        } else {
            false
        }
    }

    // fn get_repair_value(&self, k: &K) -> Option<&NEStr>
    // where
    //     I: HasLen + Index<usize, Output = usize>,
    //     K: AnyIndex<SubDimension = S>,
    //     C: Index<usize, Output = LookupOverride>,
    // {
    //     let m = self.get_mask(k)?;
    //     let v = self.get_value(k)?;
    //     m.value(v)
    // }

    pub(crate) fn iter_ne_masked_mut<'b>(
        &'b mut self,
    ) -> impl Iterator<Item = (StdKey, &'b NEStr, &'b mut RepairMask)>
    where
        I: HasLen + Index<usize, Output = usize>,
        K: EnumIndex<SubDimension = S> + Into<StdKey>,
        &'b mut C: IntoIterator<Item = &'b mut RepairMask>,
    {
        self.iter_masked_mut()
            .filter_map(|(k, v, m)| NEStr::try_new(v).map(|ne| (k, ne, m)))
    }

    pub(crate) fn iter_masked_mut<'b>(
        &'b mut self,
    ) -> impl Iterator<Item = (StdKey, &'b str, &'b mut RepairMask)>
    where
        I: HasLen + Index<usize, Output = usize>,
        K: EnumIndex<SubDimension = S> + Into<StdKey>,
        &'b mut C: IntoIterator<Item = &'b mut RepairMask>,
    {
        self.inner
            .iter()
            .zip(&mut self.mask)
            .map(|((k, v), m)| (k.into(), v, m))
    }
}

impl<'a, I, S, K, C, A> MaskedString<'a, I, S, K, C, A, LookupMask> {
    pub(crate) fn parse_unseen<F, X>(&mut self, k: &K, f: F) -> Option<X>
    where
        F: FnOnce(&NEStr) -> (Option<LookupAction>, X),
        I: HasLen + Index<usize, Output = usize>,
        K: EnumIndex<SubDimension = S> + fmt::Debug,
        C: IndexMut<usize, Output = LookupMask>,
        A: AppendableContainer<K, LookupMask>,
    {
        let (v, m) = self.get_lookup_value_and_status_mut(k)?;
        if let Some(ne) = v {
            let (action, ret) = f(m.with_unseen(k, ne));
            let new_action = action.unwrap_or(LookupAction::None);
            *m = LookupStatus::Seen(new_action);
            Some(ret)
        } else {
            m.assert_empty_unseen();
            None
        }
    }

    pub(crate) fn remove_unseen(&mut self, k: &K) -> Option<&NEStr>
    where
        I: HasLen + Index<usize, Output = usize>,
        K: EnumIndex<SubDimension = S> + fmt::Debug,
        C: IndexMut<usize, Output = LookupMask>,
        A: AppendableContainer<K, LookupMask>,
    {
        let (v, m) = self.get_lookup_value_and_status_mut(k)?;
        if let Some(ne) = v {
            let ret = m.with_unseen(k, ne);
            *m = LookupStatus::Seen(LookupAction::None);
            Some(ret)
        } else {
            m.assert_empty_unseen();
            None
        }
    }

    pub(crate) fn get_unseen(&self, k: &K) -> Option<&NEStr>
    where
        I: HasLen + Index<usize, Output = usize>,
        K: EnumIndex<SubDimension = S> + fmt::Debug,
        C: Index<usize, Output = LookupMask>,
        A: AppendableContainer<K, LookupMask>,
    {
        let m = self.get_lookup_status(k)?;
        if let Some(ne) = self.get_lookup_value(k) {
            Some(m.with_unseen(k, ne))
        } else {
            m.assert_empty_unseen();
            None
        }
    }

    pub(crate) fn set_lookup_action_seen(&mut self, k: &K, a: LookupAction)
    where
        I: HasLen + Index<usize, Output = usize>,
        K: EnumIndex<SubDimension = S>,
        C: IndexMut<usize, Output = LookupMask>,
        A: AppendableContainer<K, LookupMask>,
    {
        if self.get_lookup_value(k).is_none() {
            panic!("attempted to set lookup action on empty value")
        } else {
            match self.get_lookup_status_mut(k) {
                Some(LookupStatus::Unseen) => {
                    panic!("attempted to set lookup action on unseen value")
                }
                Some(LookupStatus::Seen(s)) => *s = a,
                None => panic!("index out of bounds"),
            }
        }
    }

    pub(crate) fn iter_final<'b>(&'b self) -> impl Iterator<Item = (K, &'a NEStr)>
    where
        I: HasLen + Index<usize, Output = usize>,
        K: EnumIndex<SubDimension = S>,
        &'b C: IntoIterator<Item = &'b LookupMask> + 'a,
    {
        self.iter_masked().filter_map(|(k, v, m)| match m {
            LookupStatus::Unseen | LookupStatus::Seen(LookupAction::None) => Some((k, v)),
            LookupStatus::Seen(_) => None,
        })
    }

    pub(crate) fn iter_masked<'b>(
        &'b self,
    ) -> impl Iterator<Item = (K, &'a NEStr, &'b LookupStatus)>
    where
        I: HasLen + Index<usize, Output = usize>,
        K: EnumIndex<SubDimension = S>,
        &'b C: IntoIterator<Item = &'b LookupMask> + 'a,
    {
        self.iter()
            .zip(&self.mask)
            .filter_map(|((k, v), m)| m.repair.value(v).map(|ne| (k, ne, &m.status)))
    }

    fn get_lookup_value_and_status_mut(
        &mut self,
        k: &K,
    ) -> Option<(Option<&NEStr>, &mut LookupStatus)>
    where
        I: HasLen + Index<usize, Output = usize>,
        K: EnumIndex<SubDimension = S>,
        C: IndexMut<usize, Output = LookupMask>,
        A: AppendableContainer<K, LookupMask>,
    {
        let (v, m) = self.get_value_and_mask_mut(k)?;
        Some((m.repair.value(v), &mut m.status))
    }

    fn get_lookup_status(&self, k: &K) -> Option<&LookupStatus>
    where
        I: HasLen,
        K: EnumIndex<SubDimension = S>,
        C: Index<usize, Output = LookupMask>,
        A: AppendableContainer<K, LookupMask>,
    {
        Some(&self.get_mask(k)?.status)
    }

    fn get_lookup_status_mut(&mut self, k: &K) -> Option<&mut LookupStatus>
    where
        I: HasLen,
        K: EnumIndex<SubDimension = S>,
        C: IndexMut<usize, Output = LookupMask>,
        A: AppendableContainer<K, LookupMask>,
    {
        Some(&mut self.get_mask_mut(k)?.status)
    }

    fn get_lookup_value(&self, k: &K) -> Option<&NEStr>
    where
        I: HasLen + Index<usize, Output = usize>,
        K: EnumIndex<SubDimension = S>,
        C: Index<usize, Output = LookupMask>,
        A: AppendableContainer<K, LookupMask>,
    {
        let m = self.get_mask(k)?;
        let v = self.get_value(k)?;
        m.repair.value(v)
    }
}

impl<I, S, K, C, A, M> MaskedString<'_, I, S, K, C, A, M> {
    fn get_value(&self, k: &K) -> Option<&str>
    where
        I: HasLen + Index<usize, Output = usize>,
        K: EnumIndex<SubDimension = S>,
        A: AppendableContainer<K, M>,
    {
        if let Some(v) = self.inner.get(k) {
            Some(v)
        } else {
            self.appended.find_value(k).map(NEStr::as_str)
        }
    }

    fn get_mask(&self, k: &K) -> Option<&M>
    where
        I: HasLen,
        C: Index<usize, Output = M>,
        K: EnumIndex<SubDimension = S>,
        A: AppendableContainer<K, M>,
    {
        let i = k.offset(self.inner.sub_dimension());
        let n = self.inner.n_strings();
        if i < n {
            Some(&self.mask[i])
        } else {
            self.appended.find_mask(k)
        }
    }

    fn get_value_and_mask_mut(&mut self, k: &K) -> Option<(&str, &mut M)>
    where
        I: HasLen + Index<usize, Output = usize>,
        C: IndexMut<usize, Output = M>,
        K: EnumIndex<SubDimension = S>,
        A: AppendableContainer<K, M>,
    {
        let i = k.offset(self.inner.sub_dimension());
        let n = self.inner.n_strings();
        if i < n {
            Some((self.inner.get_index_unchecked(i), &mut self.mask[i]))
        } else {
            self.appended
                .find_value_and_mask_mut(k)
                .map(|(m, v)| (v.as_str(), m))
        }
    }

    fn get_mask_mut(&mut self, k: &K) -> Option<&mut M>
    where
        I: HasLen,
        C: IndexMut<usize, Output = M>,
        K: EnumIndex<SubDimension = S>,
        A: AppendableContainer<K, M>,
    {
        let i = k.offset(self.inner.sub_dimension());
        let n = self.inner.n_strings();
        if i < n {
            Some(&mut self.mask[i])
        } else {
            self.appended.find_mask_mut(k)
        }
    }

    // pub(crate) fn iter_std<'b>(&'b self) -> IterStd<'b, I, K>
    // where
    //     I: HasLen + Index<usize, Output = usize>,
    //     K: AnyIndex<SubDimension = S> + Into<StdKey>,
    // {
    //     self.inner.iter_std()
    // }

    pub(crate) fn iter(&self) -> Iter<'_, I, K>
    where
        I: HasLen + Index<usize, Output = usize>,
        K: EnumIndex<SubDimension = S>,
    {
        self.inner.iter()
    }

    pub(crate) fn occupied(&self, k: &K) -> Option<bool>
    where
        I: HasLen + Index<usize, Output = usize>,
        K: EnumIndex<SubDimension = S>,
    {
        self.inner.occupied(k)
    }
}

impl RepairMask {
    fn value<'a, 'b: 'a>(&'b self, stored: &'a str) -> Option<&'a NEStr> {
        match self {
            Self::Stored => NEStr::try_new(stored),
            Self::Delete => None,
            Self::Insert(ne) => Some(ne.as_ne_str()),
        }
    }
}

impl LookupStatus {
    fn with_unseen<'a, K: fmt::Debug>(self, k: &K, v: &'a NEStr) -> &'a NEStr {
        match self {
            Self::Unseen => v,
            Self::Seen(_) => {
                panic!("tried to look up key {k:?} with value '{v}' which was already seen")
            }
        }
    }

    fn assert_empty_unseen(self) {
        assert!(
            matches!(self, Self::Unseen),
            "empty value found with lookup status"
        );
    }
}

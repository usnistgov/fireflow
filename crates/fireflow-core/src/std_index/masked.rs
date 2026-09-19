use crate::std_index::nested_string::{Iter, NestedEnumString, NestedString, NestedVariableString};
use crate::validated::dataframe::HasLen;

use derive_new::new;
use fireflow_types::nonempty::NEStr;
use fireflow_types::{
    nonempty::NEString,
    std_key::{AnyIndex, StdKey},
};

use std::{
    array::from_fn,
    marker::PhantomData,
    ops::{Index, IndexMut},
};

use super::index::LookupAction;

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

#[derive(new, Clone, Default)]
pub struct LookupStatus {
    override_: LookupOverride,
    status: LookupStatus_,
}

#[derive(Clone, Default)]
pub(crate) enum LookupOverride {
    #[default]
    Stored,
    Delete,
    Insert(NEString),
}

#[derive(Clone, Copy, Default)]
pub(crate) enum LookupStatus_ {
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

impl<'a, const LEN: usize, K> MaskedEnumString<'a, LEN, K, LookupOverride> {
    pub(crate) fn into_lookup_array(self) -> MaskedEnumString<'a, LEN, K, LookupStatus> {
        let mask = self
            .mask
            .map(|s| LookupStatus::new(s, LookupStatus_::default()));
        MaskedString::new(self.inner, mask, ())
    }
}

impl<'a, K, S> MaskedVariableString<'a, K, S, LookupOverride> {
    pub(crate) fn into_lookup_var(self) -> MaskedVariableString<'a, K, S, LookupStatus> {
        let mask = self
            .mask
            .into_iter()
            .map(|s| LookupStatus::new(s, LookupStatus_::default()))
            .collect();
        let appended = self
            .appended
            .into_iter()
            .map(|(k, m, v)| (k, LookupStatus::new(m, LookupStatus_::default()), v))
            .collect();
        MaskedString::new(self.inner, mask, appended)
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

impl<I, S, K, C, A> MaskedString<'_, I, S, K, C, A, LookupOverride> {
    pub(crate) fn delete(&mut self, k: &K) -> Option<&NEStr>
    where
        I: HasLen + Index<usize, Output = usize>,
        C: IndexMut<usize, Output = LookupOverride>,
        K: AnyIndex<SubDimension = S>,
        A: AppendableContainer<K, LookupOverride>,
    {
        if let Some((v, m)) = self.get_value_and_mask_mut(k)
            && let Some(ne) = NEStr::try_new(v)
        {
            *m = LookupOverride::Delete;
            Some(ne)
        } else {
            None
        }
    }

    pub(crate) fn insert(&mut self, k: &K, v: NEString) -> Option<NEString>
    where
        I: HasLen + Index<usize, Output = usize>,
        C: IndexMut<usize, Output = LookupOverride>,
        K: AnyIndex<SubDimension = S>,
        A: AppendableContainer<K, LookupOverride>,
    {
        if self.key_has_value(k) {
            Some(v)
        } else if let Some(m) = self.get_mask_mut(k) {
            *m = LookupOverride::Insert(v);
            None
        } else {
            self.appended.insert(k, v)
        }
    }

    pub(crate) fn key_has_value(&self, k: &K) -> bool
    where
        I: HasLen + Index<usize, Output = usize>,
        K: AnyIndex<SubDimension = S>,
        C: Index<usize, Output = LookupOverride>,
        A: AppendableContainer<K, LookupOverride>,
    {
        if let Some(m) = self.get_mask(k) {
            match m {
                LookupOverride::Stored => self.occupied(k) == Some(true),
                LookupOverride::Insert(_) => true,
                LookupOverride::Delete => false,
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
    ) -> impl Iterator<Item = (StdKey, &'b NEStr, &'b mut LookupOverride)>
    where
        I: HasLen + Index<usize, Output = usize>,
        K: AnyIndex<SubDimension = S> + Into<StdKey>,
        &'b mut C: IntoIterator<Item = &'b mut LookupOverride>,
    {
        self.iter_masked_mut()
            .filter_map(|(k, v, m)| NEStr::try_new(v).map(|ne| (k, ne, m)))
    }

    pub(crate) fn iter_masked_mut<'b>(
        &'b mut self,
    ) -> impl Iterator<Item = (StdKey, &'b str, &'b mut LookupOverride)>
    where
        I: HasLen + Index<usize, Output = usize>,
        K: AnyIndex<SubDimension = S> + Into<StdKey>,
        &'b mut C: IntoIterator<Item = &'b mut LookupOverride>,
    {
        self.inner
            .iter()
            .zip(&mut self.mask)
            .map(|((k, v), m)| (k.into(), v, m))
    }
}

impl<'a, I, S, K, C, A> MaskedString<'a, I, S, K, C, A, LookupStatus> {
    pub(crate) fn parse_unseen<F, X>(&mut self, k: &K, f: F) -> Option<X>
    where
        F: FnOnce(&NEStr) -> (Option<LookupAction>, X),
        I: HasLen + Index<usize, Output = usize>,
        K: AnyIndex<SubDimension = S>,
        C: IndexMut<usize, Output = LookupStatus>,
        A: AppendableContainer<K, LookupStatus>,
    {
        let (v, m) = self.get_lookup_value_and_status_mut(k)?;
        if let Some(ne) = v {
            let (action, ret) = f(m.with_unseen(ne));
            let new_action = action.unwrap_or(LookupAction::None);
            *m = LookupStatus_::Seen(new_action);
            Some(ret)
        } else {
            m.assert_empty_unseen();
            None
        }
    }

    pub(crate) fn remove_unseen(&mut self, k: &K) -> Option<&NEStr>
    where
        I: HasLen + Index<usize, Output = usize>,
        K: AnyIndex<SubDimension = S>,
        C: IndexMut<usize, Output = LookupStatus>,
        A: AppendableContainer<K, LookupStatus>,
    {
        let (v, m) = self.get_lookup_value_and_status_mut(k)?;
        if let Some(ne) = v {
            let ret = m.with_unseen(ne);
            *m = LookupStatus_::Seen(LookupAction::None);
            Some(ret)
        } else {
            m.assert_empty_unseen();
            None
        }
    }

    pub(crate) fn get_unseen(&self, k: &K) -> Option<&NEStr>
    where
        I: HasLen + Index<usize, Output = usize>,
        K: AnyIndex<SubDimension = S>,
        C: Index<usize, Output = LookupStatus>,
        A: AppendableContainer<K, LookupStatus>,
    {
        let m = self.get_lookup_status(k)?;
        if let Some(ne) = self.get_lookup_value(k) {
            Some(m.with_unseen(ne))
        } else {
            m.assert_empty_unseen();
            None
        }
    }

    pub(crate) fn set_lookup_action_seen(&mut self, k: &K, a: LookupAction)
    where
        I: HasLen + Index<usize, Output = usize>,
        K: AnyIndex<SubDimension = S>,
        C: IndexMut<usize, Output = LookupStatus>,
        A: AppendableContainer<K, LookupStatus>,
    {
        if self.get_lookup_value(k).is_none() {
            panic!("attempted to set lookup action on empty value")
        } else {
            match self.get_lookup_status_mut(k) {
                Some(LookupStatus_::Unseen) => {
                    panic!("attempted to set lookup action on unseen value")
                }
                Some(LookupStatus_::Seen(s)) => *s = a,
                None => panic!("index out of bounds"),
            }
        }
    }

    pub(crate) fn iter_masked<'b>(
        &'b self,
    ) -> impl Iterator<Item = (K, &'a NEStr, &'b LookupStatus_)>
    where
        I: HasLen + Index<usize, Output = usize>,
        K: AnyIndex<SubDimension = S>,
        &'b C: IntoIterator<Item = &'b LookupStatus> + 'a,
    {
        self.iter()
            .zip(&self.mask)
            .filter_map(|((k, v), m)| m.override_.value(v).map(|ne| (k, ne, &m.status)))
    }

    fn get_lookup_value_and_status_mut(
        &mut self,
        k: &K,
    ) -> Option<(Option<&NEStr>, &mut LookupStatus_)>
    where
        I: HasLen + Index<usize, Output = usize>,
        K: AnyIndex<SubDimension = S>,
        C: IndexMut<usize, Output = LookupStatus>,
        A: AppendableContainer<K, LookupStatus>,
    {
        let (v, m) = self.get_value_and_mask_mut(k)?;
        Some((m.override_.value(v), &mut m.status))
    }

    fn get_lookup_status(&self, k: &K) -> Option<&LookupStatus_>
    where
        I: HasLen,
        K: AnyIndex<SubDimension = S>,
        C: Index<usize, Output = LookupStatus>,
        A: AppendableContainer<K, LookupStatus>,
    {
        Some(&self.get_mask(k)?.status)
    }

    fn get_lookup_status_mut(&mut self, k: &K) -> Option<&mut LookupStatus_>
    where
        I: HasLen,
        K: AnyIndex<SubDimension = S>,
        C: IndexMut<usize, Output = LookupStatus>,
        A: AppendableContainer<K, LookupStatus>,
    {
        Some(&mut self.get_mask_mut(k)?.status)
    }

    fn get_lookup_value(&self, k: &K) -> Option<&NEStr>
    where
        I: HasLen + Index<usize, Output = usize>,
        K: AnyIndex<SubDimension = S>,
        C: Index<usize, Output = LookupStatus>,
        A: AppendableContainer<K, LookupStatus>,
    {
        let m = self.get_mask(k)?;
        let v = self.get_value(k)?;
        m.override_.value(v)
    }
}

impl<I, S, K, C, A, M> MaskedString<'_, I, S, K, C, A, M> {
    fn get_value(&self, k: &K) -> Option<&str>
    where
        I: HasLen + Index<usize, Output = usize>,
        K: AnyIndex<SubDimension = S>,
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
        K: AnyIndex<SubDimension = S>,
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
        K: AnyIndex<SubDimension = S>,
        A: AppendableContainer<K, M>,
    {
        let i = k.offset(self.inner.sub_dimension());
        let n = self.inner.n_strings();
        if i < n {
            Some((self.inner.get_unchecked(k), &mut self.mask[i]))
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
        K: AnyIndex<SubDimension = S>,
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
        K: AnyIndex<SubDimension = S>,
    {
        self.inner.iter()
    }

    pub(crate) fn occupied(&self, k: &K) -> Option<bool>
    where
        I: HasLen + Index<usize, Output = usize>,
        K: AnyIndex<SubDimension = S>,
    {
        self.inner.occupied(k)
    }
}

impl LookupOverride {
    fn value<'a, 'b: 'a>(&'b self, stored: &'a str) -> Option<&'a NEStr> {
        match self {
            Self::Stored => NEStr::try_new(stored),
            Self::Delete => None,
            Self::Insert(ne) => Some(ne.as_ne_str()),
        }
    }
}

impl LookupStatus_ {
    fn with_unseen(self, v: &NEStr) -> &NEStr {
        match self {
            Self::Unseen => v,
            Self::Seen(_) => {
                panic!("tried to look up value '{v}' which was already seen")
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

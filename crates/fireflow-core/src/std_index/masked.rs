use crate::std_index::nested_string::{
    Iter, IterStd, NestedEnumString, NestedString, NestedVariableString,
};
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
    mem,
    ops::{Index, IndexMut},
};

use super::index::LookupAction;

pub type RepairEnumString<'a, const LEN: usize, K> =
    MaskedString<'a, [usize; LEN], (), K, [LookupOverride; LEN], LookupOverride>;

pub type RepairVariableString<'a, K, S> =
    MaskedString<'a, Vec<usize>, S, K, Vec<LookupOverride>, LookupOverride>;

pub type LookupEnumString<'a, const LEN: usize, K> =
    MaskedString<'a, [usize; LEN], (), K, [LookupStatus; LEN], LookupStatus>;

pub type LookupVariableString<'a, K, S> =
    MaskedString<'a, Vec<usize>, S, K, Vec<LookupStatus>, LookupStatus>;

#[derive(new)]
pub(crate) struct MaskedString<'a, I, S, K, C, M> {
    inner: &'a NestedString<I, S, K>,
    mask: C,
    _mask_element: PhantomData<M>,
}

// pub(crate) enum RepairStatusRef<'a, 'b> {
//     Empty(&'a mut EmptyStatus),
//     NonEmpty(&'a mut NonEmptyStatus, &'b NEStr),
// }

pub(crate) enum RepairStatus {
    Empty(EmptyStatus),
    NonEmpty(NonEmptyStatus),
}

pub(crate) enum EmptyStatus {
    EmptyValue,
    Delete(Delete),
}

pub(crate) enum NonEmptyStatus {
    Update(Update),
    Insert(Insert),
}

#[derive(Default)]
pub(crate) struct Update {
    promote: Option<DeleteAndPromote>,
    edit: Option<Edit>,
}

pub(crate) enum Insert {
    Explicit(NEString),
    Move(InsertAndEdit),
    Promote(InsertAndEdit),
}

pub(crate) struct InsertAndEdit {
    src: NEString,
    edit: Option<Edit>,
}

pub(crate) struct DeleteAndPromote {
    nonstd_val: NEString,
    deletion: Delete,
}

pub(crate) enum Edit {
    Replace(NEString, bool),
    Remove,
}

pub(crate) enum Delete {
    Drop,
    Demote,
    Move,
}

#[derive(new, Clone, Default)]
pub(crate) struct LookupStatus {
    override_: LookupOverride,
    status: LookupStatus_,
}

#[derive(Clone)]
pub(crate) enum LookupOverride {
    Stored,
    Delete,
    Insert(NEString),
}

#[derive(Clone, Copy)]
pub(crate) enum LookupStatus_ {
    Unseen,
    Seen(LookupAction),
}

impl<'a, const LEN: usize, K> RepairEnumString<'a, LEN, K> {
    pub(crate) fn init_repair_array(inner: &'a NestedEnumString<LEN, K>) -> Self
    where
        K: AnyIndex<SubDimension = ()>,
    {
        let mask = from_fn(|_| LookupOverride::default());
        Self::new(inner, mask)
    }

    pub(crate) fn into_lookup_array(self) -> LookupEnumString<'a, LEN, K> {
        let mask = self
            .mask
            .map(|s| LookupStatus::new(s, LookupStatus_::default()));
        MaskedString::new(self.inner, mask)
    }
}

impl<'a, K, S> RepairVariableString<'a, K, S> {
    pub(crate) fn init_repair_var(inner: &'a NestedVariableString<K, S>) -> Self
    where
        K: AnyIndex<SubDimension = S>,
    {
        let n = inner.n_strings();
        let mut mask = Vec::with_capacity(n);
        mask.resize_with(n, || LookupOverride::default());
        // for (k, v) in inner.iter() {
        //     if !v.is_empty() {
        //         mask[k.offset(inner.sub_dimension())] = RepairStatus::non_empty();
        //     }
        // }
        Self::new(inner, mask)
    }

    pub(crate) fn into_lookup_var(self) -> LookupVariableString<'a, K, S> {
        let mask = self
            .mask
            .into_iter()
            .map(|s| LookupStatus::new(s, LookupStatus_::default()))
            .collect();
        MaskedString::new(self.inner, mask)
    }
}

impl<'a, const LEN: usize, K> LookupEnumString<'a, LEN, K> {
    pub fn init_lookup_array(inner: &'a NestedEnumString<LEN, K>) -> Self {
        let mask = from_fn(|_| LookupStatus::default());
        Self::new(inner, mask)
    }
}

impl<'a, K, S> LookupVariableString<'a, K, S> {
    pub fn init_lookup_var(inner: &'a NestedVariableString<K, S>) -> Self {
        let n = inner.n_strings();
        Self {
            inner,
            mask: vec![LookupStatus::default(); n],
            _mask_element: PhantomData,
        }
    }
}

impl<'a, I, S, K, C> MaskedString<'a, I, S, K, C, LookupOverride> {
    pub(crate) fn delete(&mut self, k: &K) -> Option<&NEStr>
    where
        I: HasLen + Index<usize, Output = usize>,
        C: IndexMut<usize, Output = LookupOverride>,
        K: AnyIndex<SubDimension = S>,
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
    {
        if self.key_has_value(k) {
            Some(v)
        } else if let Some(m) = self.get_mask_mut(k) {
            *m = LookupOverride::Insert(v);
            None
        } else {
            // TODO append this to buffer which will exist *soon*
            None
        }
    }

    pub(crate) fn key_has_value(&self, k: &K) -> bool
    where
        I: HasLen + Index<usize, Output = usize>,
        K: AnyIndex<SubDimension = S>,
        C: Index<usize, Output = LookupOverride>,
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

    // fn get_repair_value_and_status_mut(
    //     &mut self,
    //     k: &K,
    // ) -> Option<(Option<&NEStr>, &mut LookupOverride)>
    // where
    //     I: HasLen + Index<usize, Output = usize>,
    //     K: AnyIndex<SubDimension = S>,
    //     C: IndexMut<usize, Output = LookupOverride>,
    // {
    //     let (v, m) = self.get_value_and_mask_mut(k)?;
    //     Some((m.value(v), m))
    // }

    fn get_repair_value(&self, k: &K) -> Option<&NEStr>
    where
        I: HasLen + Index<usize, Output = usize>,
        K: AnyIndex<SubDimension = S>,
        C: Index<usize, Output = LookupOverride>,
    {
        let m = self.get_mask(k)?;
        let v = self.get_value(k)?;
        m.value(v)
    }

    pub(crate) fn iter_ne_masked_mut<'b>(
        &'b mut self,
    ) -> impl Iterator<Item = (StdKey, &NEStr, &mut LookupOverride)>
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
    ) -> impl Iterator<Item = (StdKey, &str, &mut LookupOverride)>
    where
        I: HasLen + Index<usize, Output = usize>,
        K: AnyIndex<SubDimension = S> + Into<StdKey>,
        &'b mut C: IntoIterator<Item = &'b mut LookupOverride>,
    {
        self.inner
            .iter()
            .zip(self.mask.into_iter())
            .map(|((k, v), m)| (k.into(), v, m))
    }
}

impl<'a, I, S, K, C> MaskedString<'a, I, S, K, C, LookupStatus> {
    pub(crate) fn parse_unseen<F, X>(&mut self, k: &K, f: F) -> Option<X>
    where
        F: FnOnce(&NEStr) -> (Option<LookupAction>, X),
        I: HasLen + Index<usize, Output = usize>,
        K: AnyIndex<SubDimension = S>,
        C: IndexMut<usize, Output = LookupStatus>,
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
            .zip(self.mask.into_iter())
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
    {
        let (v, m) = self.get_value_and_mask_mut(k)?;
        Some((m.override_.value(v), &mut m.status))
    }

    fn get_lookup_status(&self, k: &K) -> Option<&LookupStatus_>
    where
        I: HasLen,
        K: AnyIndex<SubDimension = S>,
        C: Index<usize, Output = LookupStatus>,
    {
        Some(&self.get_mask(k)?.status)
    }

    fn get_lookup_status_mut(&mut self, k: &K) -> Option<&mut LookupStatus_>
    where
        I: HasLen,
        K: AnyIndex<SubDimension = S>,
        C: IndexMut<usize, Output = LookupStatus>,
    {
        Some(&mut self.get_mask_mut(k)?.status)
    }

    fn get_lookup_value(&self, k: &K) -> Option<&NEStr>
    where
        I: HasLen + Index<usize, Output = usize>,
        K: AnyIndex<SubDimension = S>,
        C: Index<usize, Output = LookupStatus>,
    {
        let m = self.get_mask(k)?;
        let v = self.get_value(k)?;
        m.override_.value(v)
    }
}

impl<'a, I, S, K, C, M> MaskedString<'a, I, S, K, C, M> {
    fn get_value(&self, k: &K) -> Option<&str>
    where
        I: HasLen + Index<usize, Output = usize>,
        K: AnyIndex<SubDimension = S>,
    {
        self.inner.get(k)
    }

    fn get_mask(&self, k: &K) -> Option<&M>
    where
        I: HasLen,
        C: Index<usize, Output = M>,
        K: AnyIndex<SubDimension = S>,
    {
        let i = k.offset(self.inner.sub_dimension());
        let n = self.inner.n_strings();
        assert!(i < n, "index out of bounds: {i}");
        (i < n).then_some(&self.mask[i])
    }

    fn get_value_and_mask_mut(&mut self, k: &K) -> Option<(&str, &mut M)>
    where
        I: HasLen + Index<usize, Output = usize>,
        C: IndexMut<usize, Output = M>,
        K: AnyIndex<SubDimension = S>,
    {
        let i = k.offset(self.inner.sub_dimension());
        let n = self.inner.n_strings();
        (i < n).then_some((self.inner.get_unchecked(k), &mut self.mask[i]))
    }

    fn get_mask_mut(&mut self, k: &K) -> Option<&mut M>
    where
        I: HasLen,
        C: IndexMut<usize, Output = M>,
        K: AnyIndex<SubDimension = S>,
    {
        let i = k.offset(self.inner.sub_dimension());
        let n = self.inner.n_strings();
        (i < n).then_some(&mut self.mask[i])
    }

    pub(crate) fn iter_std<'b>(&'b self) -> IterStd<'b, I, K>
    where
        I: HasLen + Index<usize, Output = usize>,
        K: AnyIndex<SubDimension = S> + Into<StdKey>,
    {
        self.inner.iter_std()
    }

    pub(crate) fn iter<'b>(&'b self) -> Iter<'b, I, K>
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

impl RepairStatus {
    pub(crate) fn non_empty() -> Self {
        Self::NonEmpty(NonEmptyStatus::Update(Update::default()))
    }

    pub(crate) fn finalize(self) -> LookupStatus {
        match self {
            Self::Empty(x) => x.finalize(),
            Self::NonEmpty(x) => x.finalize(),
        }
    }
}

impl Default for RepairStatus {
    fn default() -> Self {
        Self::Empty(EmptyStatus::EmptyValue)
    }
}

impl EmptyStatus {
    fn finalize(self) -> LookupStatus {
        match self {
            Self::EmptyValue => LookupStatus::default(),
            Self::Delete(_) => LookupStatus::new_delete(),
        }
    }
}

impl NonEmptyStatus {
    fn value<'b, 'c: 'b>(&'b self, stored: &'c str) -> Option<&'b NEStr> {
        match self {
            Self::Update(u) => {
                if let Some(e) = u.edit.as_ref() {
                    e.value()
                } else if let Some(p) = u.promote.as_ref() {
                    Some(p.nonstd_val.as_ne_str())
                } else {
                    Some(NEStr::try_new(stored).expect("stored value should not be empty"))
                }
            }
            Self::Insert(i) => i.value(),
        }
    }

    pub(crate) fn finalize(self) -> LookupStatus {
        match self {
            Self::Update(x) => x.finalize(),
            Self::Insert(x) => x.finalize(),
        }
    }
}

impl Update {
    pub(crate) fn finalize(self) -> LookupStatus {
        if let Some(edit) = self.edit {
            edit.finalize()
        } else if let Some(promote) = self.promote {
            promote.finalize()
        } else {
            LookupStatus::default()
        }
    }
}

impl Insert {
    fn value(&self) -> Option<&NEStr> {
        match self {
            Self::Explicit(e) => Some(e.as_ne_str()),
            Self::Move(m) => m.value(),
            Self::Promote(p) => p.value(),
        }
    }

    fn finalize(self) -> LookupStatus {
        match self {
            Self::Explicit(ne) => LookupStatus::new_insert(ne),
            Self::Move(m) => m.finalize(),
            Self::Promote(p) => p.finalize(),
        }
    }
}

impl InsertAndEdit {
    fn value(&self) -> Option<&NEStr> {
        if let Some(e) = self.edit.as_ref() {
            e.value()
        } else {
            Some(self.src.as_ne_str())
        }
    }

    fn finalize(self) -> LookupStatus {
        if let Some(edit) = self.edit {
            edit.finalize()
        } else {
            LookupStatus::new_insert(self.src)
        }
    }
}

impl Edit {
    fn value(&self) -> Option<&NEStr> {
        match self {
            Self::Replace(ne, _) => Some(ne.as_ne_str()),
            Self::Remove => None,
        }
    }

    pub(crate) fn finalize(self) -> LookupStatus {
        match self {
            Self::Replace(ne, _) => LookupStatus::new_insert(ne),
            Self::Remove => LookupStatus::new_delete(),
        }
    }
}

impl DeleteAndPromote {
    pub(crate) fn finalize(self) -> LookupStatus {
        LookupStatus::new_insert(self.nonstd_val)
    }
}

impl Default for LookupOverride {
    fn default() -> Self {
        Self::Stored
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

impl LookupStatus {
    fn new_insert(ne: NEString) -> Self {
        Self::new(LookupOverride::Insert(ne), LookupStatus_::default())
    }

    fn new_delete() -> Self {
        Self::new(LookupOverride::Delete, LookupStatus_::default())
    }
}

impl LookupStatus_ {
    fn with_unseen<'a, 'b>(&'b self, v: &'a NEStr) -> &'a NEStr {
        match self {
            Self::Unseen => v,
            Self::Seen(_) => {
                panic!("tried to look up value '{v}' which was already seen")
            }
        }
    }

    fn assert_empty_unseen(&self) {
        assert!(
            matches!(self, Self::Unseen),
            "empty value found with lookup status"
        )
    }

    pub(crate) fn dispatch<F0, F1, F2>(&self, mut f_unseen: F0, mut f_demote: F1, mut f_drop: F2)
    where
        F0: FnMut(),
        F1: FnMut(),
        F2: FnMut(),
    {
        match self {
            Self::Unseen => f_unseen(),
            Self::Seen(a) => match a {
                LookupAction::None => (),
                LookupAction::Demote => f_demote(),
                LookupAction::Drop => f_drop(),
            },
        }
    }
}

impl Default for LookupStatus_ {
    fn default() -> Self {
        Self::Unseen
    }
}

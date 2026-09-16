use crate::std_index::nested_string::{
    Iter, IterStd, NestedEnumString, NestedString, NestedVariableString,
};
use crate::validated::dataframe::HasLen;
use crate::validated::keys::NonStdKey;

use Edit::Remove;
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

pub type RepairEnumString<const LEN: usize, K> =
    MaskedString<[usize; LEN], (), K, [RepairStatus; LEN], RepairStatus>;

pub type RepairVariableString<K, S> =
    MaskedString<Vec<usize>, S, K, Vec<RepairStatus>, RepairStatus>;

pub type LookupEnumString<const LEN: usize, K> =
    MaskedString<[usize; LEN], (), K, [LookupStatus; LEN], LookupStatus>;

pub type LookupVariableString<K, S> =
    MaskedString<Vec<usize>, S, K, Vec<LookupStatus>, LookupStatus>;

pub struct MaskedString<I, S, K, C, M> {
    inner: NestedString<I, S, K>,
    mask: C,
    _mask_element: PhantomData<M>,
}

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

impl NonEmptyStatus {
    fn value<'b, 'c: 'b>(&'b self, stored: &'c str) -> Option<&'b NEStr> {
        match self {
            Self::Update(u) => {
                if let Some(e) = u.edit.as_ref() {
                    e.value()
                } else if let Some(p) = u.promote.as_ref() {
                    Some(p.nonstd_ref.as_ne_str())
                } else {
                    Some(NEStr::try_new(stored).expect("stored value should not be empty"))
                }
            }
            Self::Insert(i) => i.src.value(),
        }
    }

    // fn lookup_status(&self) -> &LookupStatus {
    //     match self {
    //         Self::Update(u) => &u.lookup,
    //         Self::Insert(i) => &i.lookup,
    //     }
    // }

    // fn lookup_status_mut(&mut self) -> &mut LookupStatus {
    //     match self {
    //         Self::Update(u) => &mut u.lookup,
    //         Self::Insert(i) => &mut i.lookup,
    //     }
    // }
}

impl RepairStatus {
    pub(crate) fn non_empty() -> Self {
        Self::NonEmpty(NonEmptyStatus::Update(Update::default()))
    }
}

#[derive(Default)]
pub(crate) struct Update {
    promote: Option<DeleteAndPromote>,
    edit: Option<Edit>,
}

pub(crate) struct Insert {
    src: InsertSrc,
}

pub(crate) enum InsertSrc {
    Explicit(NEString),
    Move(InsertMoved),
    Promote(InsertPromoted),
}

impl InsertSrc {
    fn value(&self) -> Option<&NEStr> {
        match self {
            Self::Explicit(e) => Some(e.as_ne_str()),
            Self::Move(m) => m.value(),
            Self::Promote(p) => p.value(),
        }
    }
}

pub(crate) struct InsertFrom<S> {
    src: S,
    edit: Option<Edit>,
}

pub(crate) type InsertPromoted = InsertFrom<NEString>;
pub(crate) type InsertMoved = InsertFrom<(StdKey, NEString)>;

impl InsertMoved {
    fn value(&self) -> Option<&NEStr> {
        if let Some(e) = self.edit.as_ref() {
            e.value()
        } else {
            Some(self.src.1.as_ne_str())
        }
    }
}

impl InsertPromoted {
    fn value(&self) -> Option<&NEStr> {
        if let Some(e) = self.edit.as_ref() {
            e.value()
        } else {
            Some(self.src.as_ne_str())
        }
    }
}

impl Edit {
    fn value(&self) -> Option<&NEStr> {
        match self {
            Edit::Replace(ne, _) => Some(ne.as_ne_str()),
            Remove => None,
        }
    }
}

pub(crate) struct DeleteAndPromote {
    nonstd_ref: NEString,
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

#[derive(Clone, Copy)]
pub(crate) enum LookupStatus {
    Unseen,
    Seen(LookupAction),
}

// pub(crate) type LookupStatus = Option<NonEmptyLookupStatus>;

impl LookupStatus {
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

impl Default for RepairStatus {
    fn default() -> Self {
        Self::Empty(EmptyStatus::EmptyValue)
    }
}

impl Default for LookupStatus {
    fn default() -> Self {
        Self::Unseen
    }
}

impl<const LEN: usize, K> RepairEnumString<LEN, K> {
    pub fn init_repair_array(inner: NestedEnumString<LEN, K>) -> Self
    where
        K: AnyIndex<SubDimension = ()>,
    {
        let mut mask = from_fn(|_| RepairStatus::default());
        for (k, v) in inner.iter() {
            if !v.is_empty() {
                mask[k.offset0()] = RepairStatus::non_empty();
            }
        }
        Self {
            inner,
            mask,
            _mask_element: PhantomData,
        }
    }
}

impl<K, S> RepairVariableString<K, S> {
    pub fn init_repair_var(inner: NestedVariableString<K, S>) -> Self
    where
        K: AnyIndex<SubDimension = S>,
    {
        let n = inner.n_strings();
        let mut mask = Vec::with_capacity(n);
        mask.resize_with(n, || RepairStatus::default());
        for (k, v) in inner.iter() {
            if !v.is_empty() {
                mask[k.offset(inner.sub_dimension())] = RepairStatus::non_empty();
            }
        }
        Self {
            inner,
            mask,
            _mask_element: PhantomData,
        }
    }
}

impl<const LEN: usize, K> LookupEnumString<LEN, K> {
    pub fn init_lookup_array(inner: NestedEnumString<LEN, K>) -> Self {
        Self {
            inner,
            mask: [LookupStatus::default(); LEN],
            _mask_element: PhantomData,
        }
    }
}

impl<K, S> LookupVariableString<K, S> {
    pub fn init_lookup_var(inner: NestedVariableString<K, S>) -> Self {
        let n = inner.n_strings();
        Self {
            inner,
            mask: vec![LookupStatus::default(); n],
            _mask_element: PhantomData,
        }
    }
}

impl<I, S, K, C> MaskedString<I, S, K, C, LookupStatus> {
    pub(crate) fn parse_unseen<F, X>(&mut self, k: &K, f: F) -> Option<X>
    where
        F: FnOnce(&NEStr) -> (Option<LookupAction>, X),
        I: HasLen + Index<usize, Output = usize>,
        K: AnyIndex<SubDimension = S>,
        C: IndexMut<usize, Output = LookupStatus>,
    {
        let (v, m) = self.get_value_and_mask_mut(k);
        if let Some(ne) = NEStr::try_new(v) {
            let (action, ret) = f(m.with_unseen(ne));
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
        K: AnyIndex<SubDimension = S>,
        C: IndexMut<usize, Output = LookupStatus>,
    {
        let (v, m) = self.get_value_and_mask_mut(k);
        if let Some(ne) = NEStr::try_new(v) {
            let ret = m.with_unseen(ne);
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
        K: AnyIndex<SubDimension = S>,
        C: Index<usize, Output = LookupStatus>,
    {
        let v = self.get_value(k);
        let m = self.get_mask(k);
        if let Some(ne) = NEStr::try_new(v) {
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
        if self.get_value(k).is_empty() {
            panic!("attempted to set lookup action on empty value")
        } else {
            match self.get_mask_mut(k) {
                LookupStatus::Unseen => {
                    panic!("attempted to set lookup action on unseen value")
                }
                LookupStatus::Seen(s) => *s = a,
            }
        }
    }

    pub(crate) fn iter_masked<'a>(
        &'a self,
    ) -> impl Iterator<Item = (K, &'a NEStr, &'a LookupStatus)>
    where
        I: HasLen + Index<usize, Output = usize>,
        K: AnyIndex<SubDimension = S>,
        &'a C: IntoIterator<Item = &'a LookupStatus>,
    {
        self.iter()
            .zip(self.mask.into_iter())
            .filter_map(|((k, v), m)| NEStr::try_new(v).map(|ne| (k, ne, m)))
    }
}

impl<I, S, K, C, M> MaskedString<I, S, K, C, M> {
    pub fn get_value(&self, k: &K) -> &str
    where
        I: HasLen + Index<usize, Output = usize>,
        K: AnyIndex<SubDimension = S>,
    {
        self.inner.get(k)
    }

    pub fn get_mask(&self, k: &K) -> &M
    where
        I: HasLen,
        C: Index<usize, Output = M>,
        K: AnyIndex<SubDimension = S>,
    {
        let i = k.offset(self.inner.sub_dimension());
        let n = self.inner.n_strings();
        assert!(i < n, "index out of bounds: {i}");
        &self.mask[i]
    }

    pub fn get_value_and_mask_mut(&mut self, k: &K) -> (&str, &mut M)
    where
        I: HasLen + Index<usize, Output = usize>,
        C: IndexMut<usize, Output = M>,
        K: AnyIndex<SubDimension = S>,
    {
        let i = k.offset(self.inner.sub_dimension());
        let n = self.inner.n_strings();
        assert!(i < n, "index out of bounds: {i}");
        (self.inner.get(k), &mut self.mask[i])
    }

    pub fn get_mask_mut(&mut self, k: &K) -> &mut M
    where
        I: HasLen,
        C: IndexMut<usize, Output = M>,
        K: AnyIndex<SubDimension = S>,
    {
        let i = k.offset(self.inner.sub_dimension());
        let n = self.inner.n_strings();
        assert!(i < n, "index out of bounds: {i}");
        &mut self.mask[i]
    }

    pub(crate) fn iter_std<'a>(&'a self) -> IterStd<'a, I, K>
    where
        I: HasLen + Index<usize, Output = usize>,
        K: AnyIndex<SubDimension = S> + Into<StdKey>,
    {
        self.inner.iter_std()
    }

    pub(crate) fn iter<'a>(&'a self) -> Iter<'a, I, K>
    where
        I: HasLen + Index<usize, Output = usize>,
        K: AnyIndex<SubDimension = S>,
    {
        self.inner.iter()
    }
}

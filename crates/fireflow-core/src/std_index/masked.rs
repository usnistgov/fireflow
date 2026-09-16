use crate::std_index::nested_string::{
    Iter, IterStd, NestedEnumString, NestedString, NestedVariableString,
};
use crate::validated::dataframe::HasLen;
use crate::validated::keys::NonStdKey;

use fireflow_types::nonempty::NEStr;
use fireflow_types::{
    nonempty::NEString,
    std_key::{AnyIndex, StdKey},
};

use hashbrown::hash_map::OccupiedEntry;

use std::{
    array::from_fn,
    marker::PhantomData,
    ops::{Index, IndexMut},
};

use super::index::LookupAction;

pub type MaskedEnumString<'a, const LEN: usize, K> =
    MaskedString<[usize; LEN], (), K, [Status<'a>; LEN], Status<'a>>;

pub type MaskedVariableString<'a, K, S> =
    MaskedString<Vec<usize>, S, K, Vec<Status<'a>>, Status<'a>>;

pub struct MaskedString<I, S, K, C, M> {
    inner: NestedString<I, S, K>,
    mask: C,
    _mask_element: PhantomData<M>,
}

pub(crate) enum Status<'a> {
    Empty(EmptyStatus),
    NonEmpty(NonEmptyStatus<'a>),
}

pub(crate) enum EmptyStatus {
    EmptyValue,
    Delete(Delete),
}

pub(crate) enum NonEmptyStatus<'a> {
    Update(Update<'a>),
    Insert(Insert<'a>),
}

impl<'a> NonEmptyStatus<'a> {
    fn parse_unseen<F, X>(&mut self, stored: &str, f: F) -> X
    where
        F: FnOnce(&NEStr) -> (Option<LookupAction>, X),
    {
        let (action, ret) = f(self.get_unseen(stored));
        let new_status = action.unwrap_or(LookupAction::None);
        *self.lookup_status_mut() = LookupStatus::Seen(new_status);
        ret
    }

    fn remove_unseen<'b, 'c: 'b>(&'b mut self, stored: &'c str) -> &'b NEStr {
        self.assert_unseen();
        *self.lookup_status_mut() = LookupStatus::Seen(LookupAction::None);
        self.value(stored)
    }

    fn get_unseen<'b, 'c: 'b>(&'b self, stored: &'c str) -> &'b NEStr {
        self.assert_unseen();
        self.value(stored)
    }

    fn set_lookup_action(&mut self, a: LookupAction) {
        *self.lookup_status_mut() = LookupStatus::Seen(a);
    }

    fn assert_unseen(&self) {
        assert!(
            matches!(self.lookup_status(), LookupStatus::Seen(_)),
            "tried to look up value which was already seen",
        )
    }

    fn value<'b, 'c: 'b>(&'b self, stored: &'c str) -> &'b NEStr {
        match self {
            Self::Update(u) => {
                if let Some(e) = u.edit.as_ref() {
                    e.new_value.as_ne_str()
                } else if let Some(p) = u.promote.as_ref() {
                    p.nonstd_ref.get().as_ne_str()
                } else {
                    NEStr::try_new(stored).expect("stored value should not be empty")
                }
            }
            Self::Insert(i) => i.src.value(),
        }
    }

    fn lookup_status(&self) -> &LookupStatus {
        match self {
            Self::Update(u) => &u.lookup,
            Self::Insert(i) => &i.lookup,
        }
    }

    fn lookup_status_mut(&mut self) -> &mut LookupStatus {
        match self {
            Self::Update(u) => &mut u.lookup,
            Self::Insert(i) => &mut i.lookup,
        }
    }
}

impl<'a> Status<'a> {
    pub(crate) fn non_empty() -> Self {
        Self::NonEmpty(NonEmptyStatus::Update(Update::default()))
    }
}

#[derive(Default)]
pub(crate) struct Update<'a> {
    promote: Option<DeleteAndPromote<'a>>,
    edit: Option<Edit>,
    lookup: LookupStatus,
}

pub(crate) struct Insert<'a> {
    src: InsertSrc<'a>,
    lookup: LookupStatus,
}

pub(crate) enum InsertSrc<'a> {
    Explicit(NEString),
    Move(InsertMoved),
    Promote(InsertPromoted<'a>),
}

impl<'a> InsertSrc<'a> {
    fn value(&self) -> &NEStr {
        match self {
            Self::Explicit(e) => e.as_ne_str(),
            Self::Move(m) => m.value(),
            Self::Promote(p) => p.src.get().as_ne_str(),
        }
    }
}

pub(crate) type NonStdRef<'a> = OccupiedEntry<'a, NonStdKey, NEString>;

pub(crate) struct InsertFrom<S> {
    src: S,
    edit: Option<Edit>,
}

pub(crate) type InsertPromoted<'a> = InsertFrom<NonStdRef<'a>>;
pub(crate) type InsertMoved = InsertFrom<(StdKey, NEString)>;

impl InsertMoved {
    fn value(&self) -> &NEStr {
        if let Some(e) = self.edit.as_ref() {
            e.new_value.as_ne_str()
        } else {
            self.src.1.as_ne_str()
        }
    }
}

pub(crate) struct DeleteAndPromote<'a> {
    nonstd_ref: NonStdRef<'a>,
    deletion: Delete,
}

pub(crate) struct Edit {
    new_value: NEString,
    from_sub: bool,
}

pub(crate) enum Delete {
    Drop,
    Defer,
    Move,
}

#[derive(Clone, Copy)]
enum LookupStatus {
    Unseen,
    Seen(LookupAction),
}

impl<'a> Default for Status<'a> {
    fn default() -> Self {
        Self::Empty(EmptyStatus::EmptyValue)
    }
}

impl<'a> Default for LookupStatus {
    fn default() -> Self {
        Self::Unseen
    }
}

impl<'a, const LEN: usize, K> MaskedEnumString<'a, LEN, K> {
    pub fn init_array(inner: NestedEnumString<LEN, K>) -> Self
    where
        K: AnyIndex<SubDimension = ()>,
    {
        let mut mask = from_fn(|_| Status::default());
        for (k, _) in inner.iter() {
            mask[k.offset0()] = Status::non_empty();
        }
        Self {
            inner,
            mask,
            _mask_element: PhantomData,
        }
    }
}

impl<'a, K, S> MaskedVariableString<'a, K, S> {
    pub fn init_var(inner: NestedVariableString<K, S>) -> Self
    where
        K: AnyIndex<SubDimension = S>,
    {
        let n = inner.n_strings();
        let mut mask = Vec::with_capacity(n);
        mask.resize_with(n, || Status::default());
        for (k, _) in inner.iter() {
            mask[k.offset(inner.sub_dimension())] = Status::non_empty();
        }
        Self {
            inner,
            mask,
            _mask_element: PhantomData,
        }
    }
}

impl<'a, I, S, K, C> MaskedString<I, S, K, C, Status<'a>> {
    pub(crate) fn parse_unseen<F, X>(&mut self, k: &K, f: F) -> Option<X>
    where
        F: FnOnce(&NEStr) -> (Option<LookupAction>, X),
        I: HasLen + Index<usize, Output = usize>,
        K: AnyIndex<SubDimension = S>,
        C: IndexMut<usize, Output = Status<'a>>,
    {
        let (v, m) = self.get_value_and_mask_mut(k);
        match m {
            Status::Empty(_) => None,
            Status::NonEmpty(m0) => Some(m0.parse_unseen(v, f)),
        }
    }

    pub(crate) fn remove_unseen(&mut self, k: &K) -> Option<&NEStr>
    where
        I: HasLen + Index<usize, Output = usize>,
        K: AnyIndex<SubDimension = S>,
        C: IndexMut<usize, Output = Status<'a>>,
    {
        let (v, m) = self.get_value_and_mask_mut(k);
        match m {
            Status::Empty(_) => None,
            Status::NonEmpty(n) => Some(n.remove_unseen(v)),
        }
    }

    pub(crate) fn get_unseen(&self, k: &K) -> Option<&NEStr>
    where
        I: HasLen + Index<usize, Output = usize>,
        K: AnyIndex<SubDimension = S>,
        C: Index<usize, Output = Status<'a>>,
    {
        match self.get_mask(k) {
            Status::Empty(_) => None,
            Status::NonEmpty(n) => Some(n.get_unseen(self.get_value(k))),
        }
    }

    pub(crate) fn set_lookup_action(&mut self, k: &K, a: LookupAction)
    where
        I: HasLen,
        K: AnyIndex<SubDimension = S>,
        C: IndexMut<usize, Output = Status<'a>>,
    {
        match self.get_mask_mut(k) {
            Status::Empty(_) => (),
            Status::NonEmpty(n) => n.set_lookup_action(a),
        }
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

    // pub(crate) fn iter_masked<'a>(&'a self) -> Iter<'a, I, K>
    // where
    //     I: HasLen + Index<usize, Output = usize>,
    //     K: AnyIndex<SubDimension = S>,
    // {
    //     self.inner
    //         .iter()
    //         .zip(self.mask.into_iter().map(|((k, v), m)| ()))
    // }
}

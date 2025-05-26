use crate::error::*;
use crate::validated::nonstandard::MeasIdx;
use crate::validated::shortname::{Shortname, ShortnamePrefix};

use serde::Serialize;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::Hash;
use std::marker::PhantomData;
use std::mem;

use Ordering::*;

/// A list of potentially named values with an optional "center value".
///
/// Each element is a pair consisting of a key and a value. The key is a
/// wrapper type which may have a name in it. If their is no name, that
/// element has a "default" name which is created from its index and a
/// supplied prefix, which is stored along with the data. Each name (including)
/// these "default" names) must be unique.
///
/// Additionally, up to one element may be designated the "center" value, which
/// must have a name (ie not in the same wrapper type as the others) and can
/// have a value type distinct from the rest.
///
/// All elements, including the center if it exists, are stored in a defined
/// order.
#[derive(Clone, Serialize)]
pub enum NamedVec<K, W, U, V> {
    // W is an associated type constructor defined by K, so we need to bind K
    // but won't actually use it, hence phantom hack thing
    Split(SplitVec<W, U, V>, PhantomData<K>),
    Unsplit(UnsplitVec<W, V>),
}

impl<K, W, U, V> Default for NamedVec<K, W, U, V> {
    fn default() -> Self {
        NamedVec::Unsplit(UnsplitVec {
            prefix: ShortnamePrefix::default(),
            members: vec![],
        })
    }
}

/// An error that allows returning of the original value
///
/// This is useful in TryFrom impl's where one wants to try a conversion but
/// re-use the input on failure.
// TODO this probably belongs somewhere else, more general than just this mod
pub struct TryFromErrorReset<E, T> {
    pub error: E,
    pub value: T,
}

pub struct IndexedElement<K, V> {
    pub index: MeasIdx,
    pub key: K,
    pub value: V,
}

#[derive(Clone, Serialize)]
pub struct SplitVec<K, U, V> {
    left: PairedVec<K, V>,
    center: Box<Center<U>>,
    right: PairedVec<K, V>,
    prefix: ShortnamePrefix,
}

#[derive(Clone, Serialize)]
pub struct UnsplitVec<K, V> {
    members: PairedVec<K, V>,
    prefix: ShortnamePrefix,
}

pub enum Element<U, V> {
    Center(U),
    NonCenter(V),
}

/// Encodes a type which might have something in it.
///
/// Intended to be used as a "type family" pattern.
pub trait MightHave {
    /// Concrete wrapper type which might have something
    type Wrapper<T>: From<T>;

    /// If true, the wrapper will always have a value.
    ///
    /// Obviously, the implementation needs to ensure this is in sync with the
    /// meaning of Wrapper<T>.
    const INFALLABLE: bool;

    /// Consume a value and return it as a wrapped value
    fn wrap<T>(n: T) -> Self::Wrapper<T> {
        n.into()
    }

    /// Consume a wrapped value and possibly return its contents.
    ///
    /// If no contents exist, return the original input so the caller can
    /// take back ownership.
    fn unwrap<T>(x: Self::Wrapper<T>) -> Result<T, Self::Wrapper<T>>;

    /// Borrow a wrapped value and return a new wrapper with borrowed contents.
    fn as_ref<T>(x: &Self::Wrapper<T>) -> Self::Wrapper<&T>;

    /// Consume a wrapped value and possibly return its contents.
    fn to_opt<T>(x: Self::Wrapper<T>) -> Option<T> {
        Self::unwrap(x).ok()
    }

    /// Borrow a wrapped value and possibly return borrowed contents.
    fn as_opt<T>(x: &Self::Wrapper<T>) -> Option<&T> {
        Self::to_opt(Self::as_ref(x))
    }
}

type PairedVec<K, V> = Vec<Pair<K, V>>;

#[derive(Clone, Serialize)]
pub struct Pair<K, V> {
    pub key: K,
    pub value: V,
}

type WrappedNamedVec<K, U, V> = NamedVec<K, <K as MightHave>::Wrapper<Shortname>, U, V>;

type WrappedPair<K, V> = Pair<<K as MightHave>::Wrapper<Shortname>, V>;

type WrappedPairedVec<K, V> = PairedVec<<K as MightHave>::Wrapper<Shortname>, V>;

type Center<U> = Pair<Shortname, U>;

type Either<K, U, V> = Element<(Shortname, U), (<K as MightHave>::Wrapper<Shortname>, V)>;

pub type EitherPair<K, U, V> =
    Element<Pair<Shortname, U>, Pair<<K as MightHave>::Wrapper<Shortname>, V>>;

pub type RawInput<K, U, V> = Vec<Either<K, U, V>>;

// TODO make shortnames inside borrowed
pub type NameMapping = HashMap<Shortname, Shortname>;

impl<K: MightHave, U, V> WrappedNamedVec<K, U, V> {
    /// Build new NamedVec using either center or non-center values.
    ///
    /// Must contain either one or zero center values, otherwise return error.
    /// All names within keys (including center) must be unique.
    pub fn try_new(
        xs: RawInput<K, U, V>,
        prefix: ShortnamePrefix,
    ) -> Result<WrappedNamedVec<K, U, V>, NewNamedVecError> {
        let names: Vec<_> = xs
            .iter()
            .map(|x| x.as_ref().both(|e| K::wrap(&e.0), |o| K::as_ref(&o.0)))
            .collect();
        if !prefix.all_unique::<K>(names) {
            return Err(NewNamedVecError::NonUnique);
        }
        let mut left = vec![];
        let mut center = None;
        let mut right = vec![];
        for x in xs {
            match x {
                Element::NonCenter(y) => {
                    let p = Pair {
                        key: y.0,
                        value: y.1,
                    };
                    if center.is_none() {
                        left.push(p);
                    } else {
                        right.push(p);
                    }
                }
                Element::Center(y) => {
                    if center.is_none() {
                        let cp = Pair {
                            key: y.0,
                            value: y.1,
                        };
                        center = Some(cp);
                    } else {
                        return Err(NewNamedVecError::MultiCenter);
                    }
                }
            }
        }
        let s = if let Some(c) = center {
            Self::new_split(left, c, right, prefix)
        } else {
            Self::new_unsplit(left, prefix)
        };
        Ok(s)
    }

    /// Return reference to center
    pub fn as_center(&self) -> Option<IndexedElement<&Shortname, &U>> {
        match self {
            NamedVec::Split(s, _) => Some(IndexedElement {
                index: s.left.len().into(),
                key: &s.center.key,
                value: &s.center.value,
            }),
            NamedVec::Unsplit(_) => None,
        }
    }

    /// Return mutable reference to center
    pub fn as_center_mut(&mut self) -> Option<IndexedElement<&mut Shortname, &mut U>> {
        match self {
            NamedVec::Split(s, _) => Some(IndexedElement {
                index: s.left.len().into(),
                key: &mut s.center.key,
                value: &mut s.center.value,
            }),
            NamedVec::Unsplit(_) => None,
        }
    }

    /// Return reference to prefix
    pub fn as_prefix(&self) -> &ShortnamePrefix {
        match self {
            NamedVec::Split(s, _) => &s.prefix,
            NamedVec::Unsplit(u) => &u.prefix,
        }
    }

    /// Set the prefix
    pub fn set_prefix(&mut self, prefix: ShortnamePrefix) {
        match self {
            NamedVec::Split(s, _) => {
                s.prefix = prefix;
            }
            NamedVec::Unsplit(u) => {
                u.prefix = prefix;
            }
        }
    }

    // pub fn into_iter(
    //     self,
    // ) -> impl IntoIterator<Item = (MeasIdx, Result<WrappedPair<K, V>, Pair<Shortname, U>>)> {
    //     let go =
    //         |xs: Vec<WrappedPair<K, V>>| xs.into_iter().enumerate().map(|(i, p)| (i.into(), Ok(p)));
    //     match self {
    //         NamedVec::Split(s, _) => {
    //             let c = (s.left.len().into(), Err(*s.center));
    //             go(s.left).chain(vec![c]).chain(go(s.right))
    //         }
    //         NamedVec::Unsplit(u) => go(u.members).chain(vec![]).chain(go(vec![])),
    //     }
    // }

    /// Return iterator over all elements with indices
    #[allow(clippy::type_complexity)]
    pub fn iter<'a>(
        &'a self,
    ) -> impl Iterator<
        Item = (
            MeasIdx,
            Element<&'a Pair<Shortname, U>, &'a WrappedPair<K, V>>,
        ),
    > + 'a {
        let go = |xs: &'a [WrappedPair<K, V>]| {
            xs.iter()
                .enumerate()
                .map(|(i, p)| (i.into(), Element::NonCenter(p)))
        };
        match self {
            NamedVec::Split(s, _) => {
                let c = (s.left.len().into(), Element::Center(&(*s.center)));
                go(&s.left).chain(vec![c]).chain(go(&s.right))
            }
            NamedVec::Unsplit(u) => go(&u.members).chain(vec![]).chain(go(&u.members[0..0])),
        }
    }

    pub fn iter_common_values<'a, T: 'a>(&'a self) -> impl Iterator<Item = (MeasIdx, &'a T)> + 'a
    where
        U: AsRef<T>,
        V: AsRef<T>,
    {
        self.iter()
            .map(|(i, x)| (i, x.both(|l| l.value.as_ref(), |r| r.value.as_ref())))
    }

    /// Return iterator over borrowed non-center values
    pub fn iter_non_center_values(&self) -> impl Iterator<Item = (MeasIdx, &V)> + '_ {
        self.iter()
            .flat_map(|(i, x)| x.non_center().map(|p| (i, &p.value)))
    }

    /// Return iterator over borrowed non-center keys
    pub fn iter_non_center_keys(&self) -> impl Iterator<Item = &K::Wrapper<Shortname>> + '_ {
        self.iter()
            .flat_map(|(_, x)| x.non_center().map(|p| &p.key))
    }

    /// Return all existing names in the vector with their indices
    pub fn indexed_names(&self) -> impl Iterator<Item = (MeasIdx, &Shortname)> + '_ {
        self.iter().flat_map(|(i, r)| {
            r.both(|x| Some(&x.key), |x| K::as_opt(&x.key))
                .map(|x| (i, x))
        })
    }

    /// Return iterator over key names with non-existent names as default.
    // TODO seems like we should give a different type for this
    pub fn iter_all_names(&self) -> impl Iterator<Item = Shortname> + '_ {
        let prefix = self.as_prefix();
        self.iter().map(|(i, r)| {
            r.both(
                |x| x.key.clone(),
                |x| K::as_opt(&x.key).cloned().unwrap_or(prefix.as_indexed(i)),
            )
        })
    }

    /// Alter values with a function and payload.
    ///
    /// Center and non-center values will be projected to a common type.
    pub fn alter_common_values_zip<F, X, R, T>(
        &mut self,
        xs: Vec<X>,
        f: F,
    ) -> Result<Vec<R>, KeyLengthError>
    where
        F: Fn(MeasIdx, &mut T, X) -> R,
        U: AsMut<T>,
        V: AsMut<T>,
    {
        self.alter_values_zip(
            xs,
            |v, x| f(v.index, v.value.as_mut(), x),
            |v, x| f(v.index, v.value.as_mut(), x),
        )
    }

    /// Alter values with a function
    ///
    /// Center and non-center values will be projected to a common type.
    pub fn alter_common_values<F, R, T>(&mut self, f: F) -> Vec<R>
    where
        F: Fn(MeasIdx, &mut T) -> R,
        U: AsMut<T>,
        V: AsMut<T>,
    {
        self.alter_values(
            |v| f(v.index, v.value.as_mut()),
            |v| f(v.index, v.value.as_mut()),
        )
    }

    /// Apply functions to values with payload, altering them in place.
    ///
    /// This will alter all values, including center and non-center values. The
    /// two functions apply to the different values contained. Return None
    /// if input vector is not the same length.
    pub fn alter_values_zip<F, G, X, R>(
        &mut self,
        xs: Vec<X>,
        f: F,
        g: G,
    ) -> Result<Vec<R>, KeyLengthError>
    where
        F: Fn(IndexedElement<&K::Wrapper<Shortname>, &mut V>, X) -> R,
        G: Fn(IndexedElement<&Shortname, &mut U>, X) -> R,
    {
        self.check_keys_length(&xs[..], true)?;
        let go = |zs: &mut PairedVec<K::Wrapper<Shortname>, V>, ys: Vec<X>, offset: usize| {
            zs.iter_mut()
                .zip(ys)
                .enumerate()
                .map(|(i, (y, x))| {
                    f(
                        IndexedElement {
                            index: (i + offset).into(),
                            key: &y.key,
                            value: &mut y.value,
                        },
                        x,
                    )
                })
                .collect()
        };
        let x = match self {
            NamedVec::Split(s, _) => {
                let nleft = s.left.len();
                let nright = s.right.len();
                let mut it = xs.into_iter();
                let left_r: Vec<_> = go(&mut s.left, it.by_ref().take(nleft).collect(), 0);
                let c = &mut s.center;
                let center_r = g(
                    IndexedElement {
                        index: nleft.into(),
                        key: &c.key,
                        value: &mut c.value,
                    },
                    it.next().unwrap(),
                );
                let right_r: Vec<_> =
                    go(&mut s.right, it.by_ref().take(nright).collect(), 1 + nleft);
                left_r
                    .into_iter()
                    .chain([center_r])
                    .chain(right_r)
                    .collect()
            }
            NamedVec::Unsplit(u) => go(&mut u.members, xs, 0),
        };
        Ok(x)
    }

    /// Apply function(s) to all values, altering them in place.
    pub fn alter_values<F, G, R>(&mut self, f: F, g: G) -> Vec<R>
    where
        F: Fn(IndexedElement<&K::Wrapper<Shortname>, &mut V>) -> R,
        G: Fn(IndexedElement<&Shortname, &mut U>) -> R,
    {
        let xs = vec![(); self.len()];
        self.alter_values_zip(xs, |x, _| f(x), |x, _| g(x)).unwrap()
    }

    /// Apply function to non-center values, altering them in place
    pub fn alter_non_center_values<F, X>(&mut self, f: F) -> Vec<X>
    where
        F: Fn(&mut V) -> X,
    {
        match self {
            NamedVec::Split(s, _) => s
                .left
                .iter_mut()
                .map(|p| f(&mut p.value))
                .chain(s.right.iter_mut().map(|p| f(&mut p.value)))
                .collect(),
            NamedVec::Unsplit(u) => u.members.iter_mut().map(|p| f(&mut p.value)).collect(),
        }
    }

    /// Apply function to non-center values with values, altering them in place
    pub fn alter_non_center_values_zip<E, F, X>(
        &mut self,
        xs: Vec<X>,
        f: F,
    ) -> Result<Vec<E>, KeyLengthError>
    where
        F: Fn(&mut V, X) -> E,
    {
        self.check_keys_length(&xs[..], false)?;
        let res = match self {
            NamedVec::Split(s, _) => {
                let nleft = s.left.len();
                let nright = s.right.len();
                let mut it = xs.into_iter();
                let left_r: Vec<_> = s
                    .left
                    .iter_mut()
                    .zip(it.by_ref().take(nleft))
                    .map(|(y, x)| f(&mut y.value, x))
                    .collect();
                let right_r: Vec<_> = s
                    .right
                    .iter_mut()
                    .zip(it.by_ref().take(nright))
                    .map(|(y, x)| f(&mut y.value, x))
                    .collect();
                left_r.into_iter().chain(right_r).collect()
            }
            NamedVec::Unsplit(u) => u
                .members
                .iter_mut()
                .zip(xs)
                .map(|(p, x)| f(&mut p.value, x))
                .collect(),
        };
        Ok(res)
    }

    /// Return position of center, if it exists
    pub fn center_index(&self) -> Option<MeasIdx> {
        match self {
            NamedVec::Split(s, _) => Some(s.left.len().into()),
            NamedVec::Unsplit(_) => None,
        }
    }

    /// Apply function over center value, possibly changing it's type
    pub fn map_center_value<F, W>(self, f: F) -> NamedVec<K, K::Wrapper<Shortname>, W, V>
    where
        F: Fn(IndexedElement<&Shortname, U>) -> W,
    {
        match self {
            NamedVec::Split(s, _) => {
                let c = s.center;
                let e = IndexedElement {
                    index: s.left.len().into(),
                    key: &c.key,
                    value: c.value,
                };
                let center = Pair {
                    value: f(e),
                    key: c.key,
                };
                NamedVec::new_split(s.left, center, s.right, s.prefix)
            }
            NamedVec::Unsplit(u) => NamedVec::Unsplit(u),
        }
    }

    /// Apply function over non-center values, possibly changing their type
    pub fn map_non_center_values<E, F, W>(
        self,
        f: F,
    ) -> MultiResult<WrappedNamedVec<K, U, W>, IndexedElementError<E>>
    where
        F: Fn(usize, V) -> Result<W, E>,
    {
        let go = |xs: WrappedPairedVec<K, V>, offset: usize| {
            xs.into_iter()
                .enumerate()
                .map(|(i, p)| {
                    let j = i + offset;
                    f(j, p.value)
                        .map(|value| Pair { key: p.key, value })
                        .map_err(|error| IndexedElementError {
                            index: j.into(),
                            error,
                        })
                })
                .gather()
        };
        match self {
            NamedVec::Split(s, _) => {
                let nleft = s.left.len();
                let lres = go(s.left, 0);
                let rres = go(s.right, nleft + 1);
                let (left, right) = lres.zip_mult(rres)?;
                Ok(NamedVec::new_split(left, *s.center, right, s.prefix))
            }
            NamedVec::Unsplit(u) => {
                let members = go(u.members, 0)?;
                Ok(NamedVec::new_unsplit(members, u.prefix))
            }
        }
    }

    /// Return number of all elements.
    pub fn len(&self) -> usize {
        self.iter().count()
    }

    /// Return number of non-center elements.
    pub fn len_non_center(&self) -> usize {
        self.iter().filter(|(_, r)| r.is_non_center()).count()
    }

    /// Return true if there are no contained elements.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get reference at position.
    #[allow(clippy::type_complexity)]
    pub fn get(
        &self,
        index: MeasIdx,
    ) -> Result<Element<(&Shortname, &U), (&K::Wrapper<Shortname>, &V)>, ElementIndexError> {
        let i = self.check_element_index(index, true)?;
        match self {
            NamedVec::Split(s, _) => {
                let left_len = s.left.len();
                match i.cmp(&left_len) {
                    Less => Ok(Element::NonCenter(&s.left[i])),
                    Equal => Ok(Element::Center(&s.center)),
                    Greater => Ok(Element::NonCenter(&s.left[i - left_len - 1])),
                }
            }
            NamedVec::Unsplit(u) => Ok(Element::NonCenter(&u.members[i])),
        }
        .map(|x| x.bimap(|p| (&p.key, &p.value), |p| (&p.key, &p.value)))
    }

    /// Get mutable reference at position.
    #[allow(clippy::type_complexity)]
    pub fn get_mut(
        &mut self,
        index: MeasIdx,
    ) -> Result<Element<(&Shortname, &mut U), (&K::Wrapper<Shortname>, &mut V)>, ElementIndexError>
    {
        let i = self.check_element_index(index, true)?;
        match self {
            NamedVec::Split(s, _) => {
                let left_len = s.left.len();
                match i.cmp(&left_len) {
                    Less => Ok(Element::NonCenter(&mut s.left[i])),
                    Equal => Ok(Element::Center(&mut s.center)),
                    Greater => Ok(Element::NonCenter(&mut s.left[i - left_len - 1])),
                }
            }
            NamedVec::Unsplit(u) => Ok(Element::NonCenter(&mut u.members[i])),
        }
        .map(|x| x.bimap(|p| (&p.key, &mut p.value), |p| (&p.key, &mut p.value)))
    }

    /// Get reference to value with name.
    pub fn get_name(&self, n: &Shortname) -> Option<(MeasIdx, Element<&U, &V>)> {
        if let Some(c) = self.as_center() {
            if c.key == n {
                return Some((c.index, Element::Center(c.value)));
            }
        }
        self.iter()
            .flat_map(|(i, r)| r.non_center().map(|x| (i, x)))
            .find(|(_, p)| K::as_opt(&p.key).is_some_and(|kn| kn == n))
            .map(|(i, p)| (i, Element::NonCenter(&p.value)))
    }

    /// Get mutable reference to value with name.
    pub fn get_name_mut(&mut self, n: &Shortname) -> Option<(MeasIdx, Element<&mut U, &mut V>)> {
        match self {
            NamedVec::Split(s, _) => {
                let nleft = s.left.len();
                Self::value_by_name_mut(&mut s.left, n)
                    .map(|(i, p)| (i.into(), Element::NonCenter(p)))
                    .or(if &s.center.key == n {
                        Some((nleft.into(), Element::Center(&mut s.center.value)))
                    } else {
                        None
                    })
                    .or(Self::value_by_name_mut(&mut s.right, n)
                        .map(|(i, p)| ((i + nleft + 1).into(), Element::NonCenter(p))))
            }
            NamedVec::Unsplit(u) => Self::value_by_name_mut(&mut u.members, n)
                .map(|(i, p)| (i.into(), Element::NonCenter(p))),
        }
    }

    /// Add a new non-center element at the end of the vector
    pub fn push(
        &mut self,
        key: K::Wrapper<Shortname>,
        value: V,
    ) -> Result<Shortname, NonUniqueKeyError> {
        let index = self.len().into();
        let (ckey, name) = self.check_key(key, index)?;
        let p = Pair { key: ckey, value };
        match self {
            NamedVec::Split(s, _) => s.right.push(p),
            NamedVec::Unsplit(u) => u.members.push(p),
        }
        Ok(name)
    }

    /// Insert a new non-center element at a given position.
    pub fn insert(
        &mut self,
        index: MeasIdx,
        key: K::Wrapper<Shortname>,
        value: V,
    ) -> Result<Shortname, InsertError> {
        let i = self
            .check_boundary_index(index)
            .map_err(InsertError::Index)?;
        let (ckey, name) = self.check_key(key, index).map_err(InsertError::NonUnique)?;
        let p = Pair { key: ckey, value };
        match self {
            NamedVec::Split(s, _) => {
                let ln = s.left.len();
                match i.cmp(&ln) {
                    Less => s.left.insert(i, p),
                    Equal => unreachable!(),
                    Greater => s.right.insert(i - ln - 1, p),
                }
            }
            NamedVec::Unsplit(u) => u.members.insert(i, p),
        }
        Ok(name)
    }

    /// Replace a non-center value with a new value at given position.
    ///
    /// Return value that was replaced.
    ///
    /// Return none if index is out of bounds. If index points to the center,
    /// convert it to a non-center value.
    pub fn replace_at(
        &mut self,
        index: MeasIdx,
        value: V,
    ) -> Result<Element<U, V>, ElementIndexError> {
        let i = self.check_element_index(index, true)?;
        let (newself, ret) = match mem::replace(self, dummy()) {
            NamedVec::Split(mut s, p) => {
                let ln = s.left.len();
                match i.cmp(&ln) {
                    Less => {
                        let ret = mem::replace(&mut s.left[i].value, value);
                        (NamedVec::Split(s, p), Element::NonCenter(ret))
                    }
                    Equal => {
                        let key = K::wrap(s.center.key);
                        let members = s
                            .left
                            .into_iter()
                            .chain([Pair { key, value }])
                            .chain(s.right)
                            .collect();
                        (
                            Self::new_unsplit(members, s.prefix),
                            Element::Center(s.center.value),
                        )
                    }
                    Greater => {
                        let ret = mem::replace(&mut s.left[i - ln - 1].value, value);
                        (NamedVec::Split(s, p), Element::NonCenter(ret))
                    }
                }
            }
            NamedVec::Unsplit(mut u) => {
                let ret = mem::replace(&mut u.members[i].value, value);
                (NamedVec::Unsplit(u), Element::NonCenter(ret))
            }
        };
        *self = newself;
        Ok(ret)
    }

    /// Replace a value with a new value with a given name.
    ///
    /// Return value that was replaced.
    ///
    /// Return none if name is not present.
    pub fn replace_named(&mut self, name: &Shortname, value: V) -> Option<Element<U, V>> {
        self.find_with_name(name)
            .and_then(|index| self.replace_at(index, value).ok())
    }

    pub fn replace_center_at(
        &mut self,
        index: MeasIdx,
        value: U,
    ) -> Result<Element<U, V>, SetCenterError>
    where
        V: From<U>,
    {
        let i = self
            .check_element_index(index, true)
            .map_err(SetCenterError::Index)?;
        if !self
            .get(index)
            .unwrap()
            .both(|_| true, |(n, _)| K::as_opt(n).is_some())
        {
            return Err(SetCenterError::NoName);
        }
        let to_center = |key: K::Wrapper<Shortname>, v| Pair {
            key: K::to_opt(key).unwrap(),
            value: v,
        };
        let (newself, ret) = match mem::replace(self, dummy()) {
            NamedVec::Split(s, _) => match split_at_index::<K, U, V>(s, i) {
                PartialSplit::Left(left, center, right, prefix) => {
                    let ret = left.selected.value;
                    let new_center = to_center(left.selected.key, value);
                    let new_right = left
                        .right
                        .into_iter()
                        .chain([Self::to_non_center(*center)])
                        .chain(right)
                        .collect();
                    (
                        Self::new_split(left.left, new_center, new_right, prefix),
                        Element::NonCenter(ret),
                    )
                }
                PartialSplit::Center(x) => {
                    let ret = x.center.value;
                    let center = Pair {
                        key: x.center.key,
                        value,
                    };
                    (
                        Self::new_split(x.left, center, x.right, x.prefix),
                        Element::Center(ret),
                    )
                }
                PartialSplit::Right(left, center, right, prefix) => {
                    let ret = right.selected.value;
                    let new_center = to_center(right.selected.key, value);
                    let new_left = left
                        .into_iter()
                        .chain([Self::to_non_center(*center)])
                        .chain(right.left)
                        .collect();
                    (
                        Self::new_split(new_left, new_center, right.right, prefix),
                        Element::NonCenter(ret),
                    )
                }
            },
            NamedVec::Unsplit(u) => {
                let x = split_paired_vec::<K, V>(u.members, i);
                let ret = x.selected.value;
                let center = to_center(x.selected.key, value);
                (
                    Self::new_split(x.left, center, x.right, u.prefix),
                    Element::NonCenter(ret),
                )
            }
        };
        *self = newself;
        Ok(ret)
    }

    /// Rename an element at index.
    ///
    /// If index points to the center element and the wrapped name contains
    /// nothing, the default name will be assigned. Return error if index is
    /// out of bounds or name is not unique. Return pair of old and new name
    /// on success.
    pub fn rename(
        &mut self,
        index: MeasIdx,
        key: K::Wrapper<Shortname>,
    ) -> Result<(Shortname, Shortname), RenameError> {
        let i = self
            .check_element_index(index, true)
            .map_err(RenameError::Index)?;
        let k = self
            .as_prefix()
            .as_opt_or_indexed::<K>(K::as_ref(&key), index);
        if self
            .iter_all_names()
            .enumerate()
            .any(|(j, n)| j != i && n == k)
        {
            Err(RenameError::NonUnique(NonUniqueKeyError { name: k }))
        } else {
            let old = match self {
                NamedVec::Split(s, _) => {
                    let ln = s.left.len();
                    match i.cmp(&ln) {
                        Less => mem::replace(&mut s.left[i].key, key),
                        Equal => K::wrap(mem::replace(&mut s.center.key, k.clone())),
                        Greater => mem::replace(&mut s.right[i - ln - 1].key, key),
                    }
                }
                NamedVec::Unsplit(u) => mem::replace(&mut u.members[i].key, key),
            };
            let old_k = self
                .as_prefix()
                .as_opt_or_indexed::<K>(K::as_ref(&old), index);
            Ok((old_k, k))
        }
    }

    /// Rename center element.
    ///
    /// Return previous name if center exists.
    pub fn rename_center(&mut self, name: Shortname) -> Option<Shortname> {
        match self {
            NamedVec::Split(s, _) => Some(mem::replace(&mut s.center.key, name)),
            NamedVec::Unsplit(_) => None,
        }
    }

    /// Push a new center element to the end of the vector
    ///
    /// Return error if center already exists.
    pub fn push_center(&mut self, name: Shortname, value: U) -> Result<(), InsertCenterError> {
        let key = self
            .check_name(name)
            .map_err(InsertError::NonUnique)
            .map_err(InsertCenterError::Insert)?;
        let p = Pair { key, value };
        let (newself, ret) = match mem::replace(self, dummy()) {
            NamedVec::Unsplit(u) => (NamedVec::new_split(u.members, p, vec![], u.prefix), Ok(())),
            s => (s, Err(InsertCenterError::Present)),
        };
        *self = newself;
        ret
    }

    /// Insert a new center element at a given position.
    ///
    /// Return error if center already exists.
    pub fn insert_center(
        &mut self,
        index: MeasIdx,
        name: Shortname,
        value: U,
    ) -> Result<(), InsertCenterError> {
        let i = self
            .check_boundary_index(index)
            .map_err(InsertError::Index)
            .map_err(InsertCenterError::Insert)?;
        let key = self
            .check_name(name)
            .map_err(InsertError::NonUnique)
            .map_err(InsertCenterError::Insert)?;
        let p = Pair { key, value };
        let (newself, ret) = match mem::replace(self, dummy()) {
            NamedVec::Unsplit(u) => {
                let mut it = u.members.into_iter();
                let left: Vec<_> = it.by_ref().take(i).collect();
                let right: Vec<_> = it.collect();
                (NamedVec::new_split(left, p, right, u.prefix), Ok(()))
            }
            s => (s, Err(InsertCenterError::Present)),
        };
        *self = newself;
        ret
    }

    /// Remove key/value pair by name.
    ///
    /// Return None if index is not found.
    pub fn remove_index(
        &mut self,
        index: MeasIdx,
    ) -> Result<EitherPair<K, U, V>, ElementIndexError> {
        let i = self.check_element_index(index, true)?;
        let (newself, ret) = match mem::replace(self, dummy()) {
            NamedVec::Split(mut s, p) => {
                let nleft = s.left.len();
                match i.cmp(&nleft) {
                    Less => {
                        let x = s.left.remove(i);
                        (NamedVec::Split(s, p), Ok(Element::NonCenter(x)))
                    }
                    Equal => {
                        let new = s.left.into_iter().chain(s.right).collect();
                        let ret = Ok(Element::Center(*s.center));
                        (NamedVec::new_unsplit(new, s.prefix), ret)
                    }
                    Greater => {
                        let x = s.right.remove(i - nleft - 1);
                        (NamedVec::Split(s, p), Ok(Element::NonCenter(x)))
                    }
                }
            }
            NamedVec::Unsplit(mut u) => {
                let x = u.members.remove(i);
                (NamedVec::Unsplit(u), Ok(Element::NonCenter(x)))
            }
        };
        *self = newself;
        ret
    }

    /// Remove key/value pair by name of key.
    ///
    /// Return None if name is not found.
    pub fn remove_name(&mut self, n: &Shortname) -> Option<(MeasIdx, Element<U, V>)> {
        let go = |xs: &mut Vec<_>| {
            if let Some(i) = Self::position_by_name(xs, n) {
                let p = xs.remove(i);
                Some((i.into(), p.value))
            } else {
                None
            }
        };
        let (newself, ret) = match mem::replace(self, dummy()) {
            NamedVec::Split(mut s, p) => {
                if let Some((i, v)) = go(&mut s.left).or(go(&mut s.right)) {
                    (NamedVec::Split(s, p), Some((i, Element::NonCenter(v))))
                } else if &s.center.key == n {
                    let i = s.left.len().into();
                    let xs = s.left.into_iter().chain(s.right).collect();
                    let new = NamedVec::new_unsplit(xs, s.prefix);
                    (new, Some((i, Element::Center(s.center.value))))
                } else {
                    (NamedVec::Split(s, p), None)
                }
            }
            NamedVec::Unsplit(mut u) => {
                let ret = go(&mut u.members);
                (
                    NamedVec::Unsplit(u),
                    ret.map(|(i, v)| (i, Element::NonCenter(v))),
                )
            }
        };
        *self = newself;
        ret
    }

    /// Set non-center keys to list
    ///
    /// The center key cannot be replaced by this method since the list will
    /// contain wrapped names which may or may not have a name inside, and
    /// the center value always has a name.
    ///
    /// List must be the same length as all non-center keys and must be unique
    /// (including the center key).
    pub fn set_non_center_keys(
        &mut self,
        ks: Vec<K::Wrapper<Shortname>>,
    ) -> Result<NameMapping, SetKeysError> {
        self.check_keys_length(&ks[..], false)
            .map_err(SetKeysError::Length)?;
        let center = self.as_center().map(|x| K::wrap(x.key));
        let all_keys = ks.iter().map(K::as_ref).chain(center).collect();
        if !self.as_prefix().all_unique::<K>(all_keys) {
            return Err(SetKeysError::NonUnique);
        }
        let mut mapping = HashMap::new();
        let mut go = |side: &mut WrappedPairedVec<K, V>, ks_side: Vec<K::Wrapper<Shortname>>| {
            for (p, k) in side.iter_mut().zip(ks_side) {
                if let (Some(old), Some(new)) = (K::as_opt(&p.key), K::as_opt(&k)) {
                    mapping.insert(old.clone(), new.clone());
                }
                p.key = k;
            }
        };
        match self {
            NamedVec::Split(s, _) => {
                let mut ks_left = ks;
                let ks_right = ks_left.split_off(s.left.len());
                go(&mut s.left, ks_left);
                go(&mut s.right, ks_right);
            }
            NamedVec::Unsplit(u) => go(&mut u.members, ks),
        }
        Ok(mapping)
    }

    /// Set all names to list of Shortnames
    ///
    /// This will update the center value along with everything else. Non-center
    /// keys will be wrapped such that they will contain a name.
    ///
    /// Supplied list must be unique and have the same length as the target
    /// vector.
    pub fn set_names(&mut self, ns: Vec<Shortname>) -> Result<NameMapping, SetKeysError> {
        self.check_keys_length(&ns[..], true)
            .map_err(SetKeysError::Length)?;
        if !all_unique(ns.iter()) {
            return Err(SetKeysError::NonUnique);
        }
        let mut mapping = HashMap::new();
        let mut go = |side: &mut WrappedPairedVec<K, V>, ns_side: Vec<Shortname>| {
            for (p, n) in side.iter_mut().zip(ns_side) {
                if let Some(old) = K::as_opt(&p.key) {
                    mapping.insert(old.clone(), n.clone());
                }
                p.key = K::wrap(n);
            }
        };
        match self {
            NamedVec::Split(s, _) => {
                let mut ns_left = ns;
                let mut ns_right = ns_left.split_off(s.left.len());
                // ASSUME this won't fail because we already checked length
                let n_center = ns_right.pop().unwrap();
                go(&mut s.left, ns_left);
                go(&mut s.right, ns_right);
                mapping.insert(s.center.key.clone(), n_center.clone());
                s.center.key = n_center;
            }
            NamedVec::Unsplit(u) => go(&mut u.members, ns),
        }
        Ok(mapping)
    }

    /// Set center to be the element with name if it exists.
    pub fn set_center_by_name(&mut self, n: &Shortname) -> bool
    where
        U: From<V>,
        V: From<U>,
    {
        self.find_with_name(n)
            .map(|index| self.set_center_by_index(index).unwrap())
            .unwrap_or(false)
    }

    /// Set center to be the element with index if it exists.
    pub fn set_center_by_index(&mut self, index: MeasIdx) -> Result<bool, SetCenterError>
    where
        U: From<V>,
        V: From<U>,
    {
        let i = self
            .check_element_index(index, true)
            .map_err(SetCenterError::Index)?;
        if !self
            .get(index)
            .unwrap()
            .both(|_| true, |(n, _)| K::as_opt(n).is_some())
        {
            return Err(SetCenterError::NoName);
        }
        let (newself, ret) = match mem::replace(self, dummy()) {
            NamedVec::Split(s, p) => match split_at_index::<K, U, V>(s, i) {
                PartialSplit::Left(left, center, right, prefix) => (
                    Self::new_split(
                        left.left,
                        Self::to_center(left.selected).unwrap(),
                        left.right
                            .into_iter()
                            .chain([Self::to_non_center(*center)])
                            .chain(right)
                            .collect(),
                        prefix,
                    ),
                    true,
                ),
                PartialSplit::Center(sc) => (NamedVec::Split(sc, p), false),
                PartialSplit::Right(left, center, right, prefix) => (
                    Self::new_split(
                        left.into_iter()
                            .chain([Self::to_non_center(*center)])
                            .chain(right.left)
                            .collect(),
                        Self::to_center(right.selected).unwrap(),
                        right.right,
                        prefix,
                    ),
                    true,
                ),
            },
            NamedVec::Unsplit(u) => {
                let x = split_paired_vec::<K, V>(u.members, i);
                let center = Self::to_center(x.selected).unwrap();
                (Self::new_split(x.left, center, x.right, u.prefix), true)
            }
        };
        *self = newself;
        Ok(ret)
    }

    /// Convert the center element into a non-center element.
    ///
    /// Has no effect if there already is no center element.
    ///
    /// Return true if vector is updated. The conversion process from center
    /// value to non-center value cannot fail.
    pub fn unset_center(&mut self) -> bool
    where
        V: From<U>,
    {
        let (newself, ret) = match mem::replace(self, dummy()) {
            NamedVec::Split(s, _) => {
                let members = s
                    .left
                    .into_iter()
                    .chain([Self::from_center(*s.center)])
                    .chain(s.right)
                    .collect();
                (Self::new_unsplit(members, s.prefix), true)
            }
            NamedVec::Unsplit(u) => (NamedVec::Unsplit(u), false),
        };
        *self = newself;
        ret
    }

    // TODO this seems like it could be more general (like "map_keys" or something)
    /// Unwrap and rewrap the non-center names of vector.
    ///
    /// This may fail if the original wrapped name cannot be converted.
    #[allow(clippy::type_complexity)]
    pub fn try_rewrapped<J>(
        self,
    ) -> MultiResult<
        WrappedNamedVec<J, U, V>,
        IndexedElementError<<J::Wrapper<Shortname> as TryFrom<K::Wrapper<Shortname>>>::Error>,
    >
    where
        J: MightHave,
        J::Wrapper<Shortname>: TryFrom<K::Wrapper<Shortname>>,
    {
        let go = |xs: WrappedPairedVec<K, V>, offset: usize| {
            xs.into_iter()
                .enumerate()
                .map(|(i, p)| {
                    Self::try_into_wrapper::<J>(p).map_err(|error| IndexedElementError {
                        error,
                        index: (i + offset).into(),
                    })
                })
                .gather()
        };
        let x = match self {
            NamedVec::Split(s, _) => {
                let offset = s.left.len() + 1;
                let lres = go(s.left, 0);
                let rres = go(s.right, offset);
                let (left, right) = lres.zip_mult(rres)?;
                NamedVec::new_split(left, *s.center, right, s.prefix)
            }
            NamedVec::Unsplit(u) => NamedVec::new_unsplit(go(u.members, 0)?, u.prefix),
        };
        Ok(x)
    }

    #[allow(clippy::type_complexity)]
    fn try_into_wrapper<J>(
        p: WrappedPair<K, V>,
    ) -> Result<WrappedPair<J, V>, <J::Wrapper<Shortname> as TryFrom<K::Wrapper<Shortname>>>::Error>
    where
        J: MightHave,
        J::Wrapper<Shortname>: TryFrom<K::Wrapper<Shortname>>,
    {
        Ok(Pair {
            key: p.key.try_into()?,
            value: p.value,
        })
    }

    fn from_center(p: Center<U>) -> WrappedPair<K, V>
    where
        V: From<U>,
    {
        Pair {
            key: K::wrap(p.key),
            value: p.value.into(),
        }
    }

    fn position_by_name(xs: &WrappedPairedVec<K, V>, n: &Shortname) -> Option<usize> {
        xs.iter()
            .position(|p| K::as_opt(&p.key).is_some_and(|kn| kn == n))
    }

    fn value_by_name_mut<'a>(
        xs: &'a mut WrappedPairedVec<K, V>,
        n: &Shortname,
    ) -> Option<(usize, &'a mut V)> {
        xs.iter_mut()
            .enumerate()
            .find(|(_, p)| K::as_opt(&p.key).is_some_and(|kn| kn == n))
            .map(|(i, p)| (i, &mut p.value))
    }

    fn check_key(
        &self,
        key: K::Wrapper<Shortname>,
        index: MeasIdx,
    ) -> Result<(K::Wrapper<Shortname>, Shortname), NonUniqueKeyError> {
        let name = self
            .as_prefix()
            .as_opt_or_indexed::<K>(K::as_ref(&key), index);
        if self.iter_all_names().any(|n| n == name) {
            Err(NonUniqueKeyError { name })
        } else {
            Ok((key, name))
        }
    }

    fn check_name(&self, name: Shortname) -> Result<Shortname, NonUniqueKeyError> {
        if self.iter_all_names().any(|n| n == name) {
            Err(NonUniqueKeyError { name })
        } else {
            Ok(name)
        }
    }

    fn check_element_index(
        &self,
        index: MeasIdx,
        include_center: bool,
    ) -> Result<usize, ElementIndexError> {
        let len = self.len();
        let i = index.into();
        if i < len {
            if let Some(j) = self.center_index() {
                if !include_center && usize::from(j) == i {
                    return Err(ElementIndexError {
                        index,
                        len,
                        center: Some(j),
                    });
                }
            }
            Ok(i)
        } else {
            Err(ElementIndexError {
                index,
                len,
                center: None,
            })
        }
    }

    fn check_boundary_index(&self, index: MeasIdx) -> Result<usize, BoundaryIndexError> {
        let len = self.len();
        let i = index.into();
        if i <= len {
            Ok(i)
        } else {
            Err(BoundaryIndexError { index, len })
        }
    }

    fn check_keys_length<X>(&self, xs: &[X], include_center: bool) -> Result<(), KeyLengthError> {
        let this_len = if include_center {
            self.len()
        } else {
            self.len_non_center()
        };
        let other_len = xs.len();
        if this_len != other_len {
            return Err(KeyLengthError {
                this_len,
                other_len,
                include_center,
            });
        }
        Ok(())
    }

    fn find_with_name(&self, name: &Shortname) -> Option<MeasIdx> {
        self.iter()
            .find(|(_, x)| {
                x.as_ref().both(
                    |l| &l.key == name,
                    |r| K::as_opt(&r.key).is_some_and(|k| k == name),
                )
            })
            .map(|(i, _)| i)
    }

    fn to_center(p: WrappedPair<K, V>) -> Option<Pair<Shortname, U>>
    where
        U: From<V>,
    {
        let value = p.value.into();
        K::to_opt(p.key).map(|key| Pair { key, value })
    }

    fn to_non_center(p: Pair<Shortname, U>) -> WrappedPair<K, V>
    where
        V: From<U>,
    {
        let key = K::wrap(p.key);
        let value = p.value.into();
        Pair { key, value }
    }

    fn new_split(
        left: WrappedPairedVec<K, V>,
        center: Center<U>,
        right: WrappedPairedVec<K, V>,
        prefix: ShortnamePrefix,
    ) -> Self {
        NamedVec::Split(
            SplitVec {
                left,
                center: Box::new(center),
                right,
                prefix,
            },
            PhantomData,
        )
    }

    fn new_unsplit(members: WrappedPairedVec<K, V>, prefix: ShortnamePrefix) -> Self {
        NamedVec::Unsplit(UnsplitVec { members, prefix })
    }
}

impl<U, V> Element<U, V> {
    pub fn bimap<F, G, X, Y>(self, f: F, g: G) -> Element<X, Y>
    where
        F: Fn(U) -> X,
        G: Fn(V) -> Y,
    {
        match self {
            Element::Center(u) => Element::Center(f(u)),
            Element::NonCenter(v) => Element::NonCenter(g(v)),
        }
    }

    pub fn both<F, G, X>(self, f: F, g: G) -> X
    where
        F: Fn(U) -> X,
        G: Fn(V) -> X,
    {
        match self {
            Element::Center(u) => f(u),
            Element::NonCenter(v) => g(v),
        }
    }

    pub fn as_ref(&self) -> Element<&U, &V> {
        match self {
            Element::Center(u) => Element::Center(u),
            Element::NonCenter(v) => Element::NonCenter(v),
        }
    }

    pub fn non_center(self) -> Option<V> {
        match self {
            Element::Center(_) => None,
            Element::NonCenter(v) => Some(v),
        }
    }

    pub fn center(self) -> Option<U> {
        match self {
            Element::Center(u) => Some(u),
            Element::NonCenter(_) => None,
        }
    }

    pub fn is_non_center(&self) -> bool {
        match self {
            Element::Center(_) => false,
            Element::NonCenter(_) => true,
        }
    }

    pub fn is_center(&self) -> bool {
        match self {
            Element::Center(_) => true,
            Element::NonCenter(_) => false,
        }
    }
}

impl ShortnamePrefix {
    fn as_opt_or_indexed<X: MightHave>(&self, x: X::Wrapper<&Shortname>, i: MeasIdx) -> Shortname {
        X::to_opt(x).cloned().unwrap_or(self.as_indexed(i))
    }

    fn all_unique<X: MightHave>(&self, xs: Vec<X::Wrapper<&Shortname>>) -> bool {
        all_unique(
            xs.into_iter()
                .enumerate()
                .map(|(i, x)| self.as_opt_or_indexed::<X>(x, i.into())),
        )
    }
}

fn all_unique<'a, T: Hash + Eq>(xs: impl Iterator<Item = T> + 'a) -> bool {
    let mut unique = HashSet::new();
    for x in xs {
        if unique.contains(&x) {
            return false;
        } else {
            unique.insert(x);
        }
    }
    true
}

// dummy value to use when mutating NamedVec in place
fn dummy<K, W, U, V>() -> NamedVec<K, W, U, V> {
    NamedVec::Unsplit(UnsplitVec {
        members: vec![],
        prefix: ShortnamePrefix::default(),
    })
}

enum PartialSplit<K, U, V> {
    Left(
        PairedSplit<K, V>,
        Box<Center<U>>,
        PairedVec<K, V>,
        ShortnamePrefix,
    ),
    Center(SplitVec<K, U, V>),
    Right(
        PairedVec<K, V>,
        Box<Center<U>>,
        PairedSplit<K, V>,
        ShortnamePrefix,
    ),
}

struct PairedSplit<K, V> {
    left: PairedVec<K, V>,
    selected: Pair<K, V>,
    right: PairedVec<K, V>,
}

fn split_paired_vec<K: MightHave, V>(
    xs: WrappedPairedVec<K, V>,
    index: usize,
) -> PairedSplit<<K as MightHave>::Wrapper<Shortname>, V> {
    let mut it = xs.into_iter();
    PairedSplit {
        left: it.by_ref().take(index).collect(),
        selected: it.next().unwrap(),
        right: it.collect(),
    }
}

fn split_at_index<K: MightHave, U, V>(
    s: SplitVec<<K as MightHave>::Wrapper<Shortname>, U, V>,
    index: usize,
) -> PartialSplit<<K as MightHave>::Wrapper<Shortname>, U, V> {
    let nleft = s.left.len();
    match index.cmp(&nleft) {
        Less => PartialSplit::Left(
            split_paired_vec::<K, V>(s.left, index),
            s.center,
            s.right,
            s.prefix,
        ),
        Equal => PartialSplit::Center(s),
        Greater => PartialSplit::Right(
            s.left,
            s.center,
            split_paired_vec::<K, V>(s.right, index - nleft - 1),
            s.prefix,
        ),
    }
}

#[derive(Debug)]
pub enum InsertError {
    Index(BoundaryIndexError),
    NonUnique(NonUniqueKeyError),
}

#[derive(Debug)]
pub enum InsertCenterError {
    Present,
    Insert(InsertError),
}

#[derive(Debug)]
pub enum RenameError {
    Index(ElementIndexError),
    NonUnique(NonUniqueKeyError),
}

pub enum SetKeysError {
    Length(KeyLengthError),
    NonUnique,
}

#[derive(Debug)]
pub enum SetCenterError {
    NoName,
    Index(ElementIndexError),
}

#[derive(Debug)]
pub struct NonUniqueKeyError {
    name: Shortname,
}

#[derive(Debug)]
pub struct ElementIndexError {
    index: MeasIdx, // refers to index of element
    len: usize,
    center: Option<MeasIdx>,
}

#[derive(Debug)]
pub struct BoundaryIndexError {
    pub index: MeasIdx, // refers to index between elements
    pub len: usize,
}

#[derive(Debug)]
pub struct KeyLengthError {
    this_len: usize,
    other_len: usize,
    include_center: bool,
}

impl KeyLengthError {
    pub fn empty(this_len: usize) -> Self {
        KeyLengthError {
            this_len,
            other_len: 0,
            include_center: true,
        }
    }
}

pub enum NewNamedVecError {
    NonUnique,
    MultiCenter,
}

// pub struct RewrapError<E> {
//     error: E,
//     index: MeasIdx,
// }

pub struct IndexedElementError<E> {
    error: E,
    index: MeasIdx,
}

impl<E> fmt::Display for IndexedElementError<E>
where
    E: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "error for element {}: {}", self.index, self.error)
    }
}

impl fmt::Display for NewNamedVecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let s = match self {
            NewNamedVecError::NonUnique => "names must be unique",
            NewNamedVecError::MultiCenter => "only zero or one center values allowed",
        };
        write!(f, "{s}")
    }
}

impl fmt::Display for SetCenterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            SetCenterError::NoName => write!(f, "index refers to element with no name"),
            SetCenterError::Index(i) => i.fmt(f),
        }
    }
}

impl fmt::Display for InsertCenterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            InsertCenterError::Insert(i) => i.fmt(f),
            InsertCenterError::Present => {
                write!(f, "Center already exists")
            }
        }
    }
}

impl fmt::Display for InsertError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            InsertError::Index(i) => i.fmt(f),
            InsertError::NonUnique(u) => u.fmt(f),
        }
    }
}

impl fmt::Display for RenameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            RenameError::Index(i) => i.fmt(f),
            RenameError::NonUnique(k) => {
                write!(f, "New key named '{k}' already in list")
            }
        }
    }
}

impl fmt::Display for ElementIndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let center = self
            .center
            .map_or("".into(), |c| format!(" and must not include {c}"));
        write!(
            f,
            "index must be 0 <= i < {}{}, got {}",
            self.len, center, self.index
        )
    }
}

impl fmt::Display for BoundaryIndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "index must be 0 <= i <= {}, got {}",
            self.len, self.index
        )
    }
}

impl fmt::Display for NonUniqueKeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "'{}' already present", self.name)
    }
}

impl fmt::Display for KeyLengthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let n = if self.include_center {
            self.this_len
        } else {
            self.this_len - 1
        };
        write!(
            f,
            "supplied list must be {n} elements long, got {}",
            self.other_len
        )
    }
}

impl fmt::Display for SetKeysError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            SetKeysError::Length(x) => x.fmt(f),
            SetKeysError::NonUnique => write!(f, "not all supplied keys are unique"),
        }
    }
}

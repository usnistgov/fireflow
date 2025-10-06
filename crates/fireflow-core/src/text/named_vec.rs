use crate::error::{
    DeferredExt as _, DeferredFailure, DeferredResult, ErrorIter as _, InfalliblePassthruExt as _,
    MultiResult, MultiResultExt as _, PassthruExt as _, PassthruResult, ResultExt as _, Tentative,
};
use crate::text::optional::MightHave;
use crate::validated::shortname::Shortname;

use super::index::{BoundaryIndexError, IndexError, IndexFromOne, MeasIndex};

use derive_more::{Display, From, Into};
use derive_new::new;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::Hash;
use std::marker::PhantomData;
use std::mem;
use thiserror::Error;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use pyo3::prelude::*;

use Ordering::{Equal, Greater, Less};

/// A list of potentially named values with an optional "center value".
///
/// Each element is a pair consisting of a key and a value. The key is a
/// wrapper type which may have a name in it. If there is no name, that
/// element has a default name of "Pn" where "n" is the index starting at 1.
/// Each name (including) these "default" names) must be unique.
///
/// Additionally, up to one element may be designated the "center" value, which
/// must have a name (ie not in the same wrapper type as the others) and can
/// have a value type distinct from the rest.
///
/// All elements, including the center if it exists, are stored in a defined
/// order.
#[derive(Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum NamedVec<K, W, U, V> {
    // W is an associated type constructor defined by K, so we need to bind K
    // but won't actually use it, hence phantom hack thing
    Split(SplitVec<W, U, V>, PhantomData<K>),
    Unsplit(UnsplitVec<W, V>),
}

impl<K, W, U, V> Default for NamedVec<K, W, U, V> {
    fn default() -> Self {
        NamedVec::Unsplit(UnsplitVec { members: vec![] })
    }
}

#[derive(new)]
pub struct IndexedElement<K, V> {
    pub index: MeasIndex,
    pub key: K,
    pub value: V,
}

#[derive(Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct SplitVec<K, U, V> {
    left: PairedVec<K, V>,
    center: Box<Center<U>>,
    right: PairedVec<K, V>,
}

#[derive(Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct UnsplitVec<K, V> {
    members: PairedVec<K, V>,
}

#[derive(Clone)]
#[cfg_attr(feature = "python", derive(FromPyObject, IntoPyObject))]
pub enum Element<U, V> {
    Center(U),
    NonCenter(V),
}

#[derive(Clone, From, Into)]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
pub struct NonCenterElement<V>(pub Element<(), V>);

type PairedVec<K, V> = Vec<Pair<K, V>>;

#[derive(Clone, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
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

#[derive(From, Into)]
pub struct Eithers<K: MightHave, U, V>(pub Vec<Either<K, U, V>>);

pub type NameMapping = HashMap<Shortname, Shortname>;

impl<K: MightHave, U, V> WrappedNamedVec<K, U, V> {
    /// Build new NamedVec using either center or non-center values.
    ///
    /// Must contain either one or zero center values, otherwise return error.
    /// All names within keys (including center) must be unique.
    pub(crate) fn try_new(
        xs: Eithers<K, U, V>,
    ) -> Result<WrappedNamedVec<K, U, V>, NewNamedVecError> {
        let names: Vec<_> =
            xs.0.iter()
                .map(|x| x.as_ref().both(|e| K::wrap(&e.0), |o| K::as_ref(&o.0)))
                .collect();
        if !all_unique_names::<K>(names) {
            return Err(NewNamedVecError::NonUnique);
        }
        let mut left = vec![];
        let mut center = None;
        let mut right = vec![];
        for x in xs.0 {
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
            Self::new_split(left, c, right)
        } else {
            Self::new_unsplit(left)
        };
        Ok(s)
    }

    /// Return reference to center
    pub(crate) fn as_center(&self) -> Option<IndexedElement<&Shortname, &U>> {
        match self {
            NamedVec::Split(s, _) => Some(IndexedElement::new(
                s.left.len().into(),
                &s.center.key,
                &s.center.value,
            )),
            NamedVec::Unsplit(_) => None,
        }
    }

    /// Return mutable reference to center
    pub fn as_center_mut(&mut self) -> Option<IndexedElement<&mut Shortname, &mut U>> {
        match self {
            NamedVec::Split(s, _) => Some(IndexedElement::new(
                s.left.len().into(),
                &mut s.center.key,
                &mut s.center.value,
            )),
            NamedVec::Unsplit(_) => None,
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
            MeasIndex,
            Element<&'a Pair<Shortname, U>, &'a WrappedPair<K, V>>,
        ),
    > + 'a {
        let go = |xs: &'a [WrappedPair<K, V>], offset: usize| {
            xs.iter()
                .zip(offset..)
                .map(|(p, i)| (i.into(), Element::NonCenter(p)))
        };
        match self {
            NamedVec::Split(s, _) => {
                let ln = s.left.len();
                let c = (ln.into(), Element::Center(&(*s.center)));
                go(&s.left, 0).chain(vec![c]).chain(go(&s.right, ln + 1))
            }
            NamedVec::Unsplit(u) => go(&u.members, 0)
                .chain(vec![])
                .chain(go(&u.members[0..0], 0)),
        }
    }

    pub(crate) fn iter_common_values<'a, T: 'a>(
        &'a self,
    ) -> impl Iterator<Item = (MeasIndex, &'a T)> + 'a
    where
        U: AsRef<T>,
        V: AsRef<T>,
    {
        self.iter()
            .map(|(i, x)| (i, x.both(|l| l.value.as_ref(), |r| r.value.as_ref())))
    }

    pub(crate) fn iter_with<'a, T, F, G>(
        &'a self,
        f: &'a F,
        g: &'a G,
    ) -> impl Iterator<Item = T> + 'a
    where
        F: Fn(MeasIndex, &'a Pair<Shortname, U>) -> T,
        G: Fn(MeasIndex, &'a WrappedPair<K, V>) -> T,
    {
        self.iter().map(|(i, e)| e.both(|x| f(i, x), |x| g(i, x)))
    }

    // /// Return iterator over borrowed non-center values
    // pub(crate) fn iter_non_center_values(&self) -> impl Iterator<Item = (MeasIndex, &V)> + '_ {
    //     self.iter()
    //         .flat_map(|(i, x)| x.non_center().map(|p| (i, &p.value)))
    // }

    // /// Return iterator over borrowed non-center keys
    // pub(crate) fn iter_non_center_keys(&self) -> impl Iterator<Item = &K::Wrapper<Shortname>> + '_ {
    //     self.iter()
    //         .flat_map(|(_, x)| x.non_center().map(|p| &p.key))
    // }

    /// Return all existing names in the vector with their indices
    pub(crate) fn indexed_names(&self) -> impl Iterator<Item = (MeasIndex, &Shortname)> + '_ {
        self.iter().filter_map(|(i, r)| {
            r.both(|x| Some(&x.key), |x| K::as_opt(&x.key))
                .map(|x| (i, x))
        })
    }

    // /// Return all existing non-center names in the vector with their indices
    // pub(crate) fn indexed_non_center_names(
    //     &self,
    // ) -> impl Iterator<Item = (MeasIndex, &Shortname)> + '_ {
    //     self.iter().flat_map(|(i, r)| {
    //         r.non_center()
    //             .and_then(|x| K::as_opt(&x.key))
    //             .map(|x| (i, x))
    //     })
    // }

    /// Return iterator over key names with non-existent names as default.
    // TODO seems like we should give a different type for this
    pub(crate) fn iter_all_names(&self) -> impl Iterator<Item = Shortname> + '_ {
        self.iter().map(|(i, r)| {
            r.both(
                |x| x.key.clone(),
                |x| K::as_opt(&x.key).cloned().unwrap_or(i.into()),
            )
        })
    }

    /// Alter values with a function and payload.
    ///
    /// Center and non-center values will be projected to a common type.
    pub(crate) fn alter_common_values_zip<F, X, R, T>(
        &mut self,
        xs: Vec<X>,
        f: F,
    ) -> Result<Vec<R>, KeyLengthError>
    where
        F: Fn(MeasIndex, &mut T, X) -> R,
        U: AsMut<T>,
        V: AsMut<T>,
    {
        self.alter_values_zip(
            xs,
            |v, x| f(v.index, v.value.as_mut(), x),
            |v, x| f(v.index, v.value.as_mut(), x),
        )
    }

    // /// Alter values with a function
    // ///
    // /// Center and non-center values will be projected to a common type.
    // pub(crate) fn alter_common_values<F, R, T>(&mut self, f: F) -> Vec<R>
    // where
    //     F: Fn(MeasIndex, &mut T) -> R,
    //     U: AsMut<T>,
    //     V: AsMut<T>,
    // {
    //     self.alter_values(
    //         |v| f(v.index, v.value.as_mut()),
    //         |v| f(v.index, v.value.as_mut()),
    //     )
    // }

    /// Apply functions to values with payload, altering them in place.
    ///
    /// This will alter all values, including center and non-center values. The
    /// two functions apply to the different values contained. Return None
    /// if input vector is not the same length.
    pub(crate) fn alter_values_zip<F, G, X, R>(
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
                        IndexedElement::new((i + offset).into(), &y.key, &mut y.value),
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
                    IndexedElement::new(nleft.into(), &c.key, &mut c.value),
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
    pub(crate) fn alter_values<F, G, R>(&mut self, f: F, g: G) -> Vec<R>
    where
        F: Fn(IndexedElement<&K::Wrapper<Shortname>, &mut V>) -> R,
        G: Fn(IndexedElement<&Shortname, &mut U>) -> R,
    {
        let xs = vec![(); self.len()];
        self.alter_values_zip(xs, |x, ()| f(x), |x, ()| g(x))
            .unwrap()
    }

    // /// Apply function to non-center values, altering them in place
    // pub(crate) fn alter_non_center_values<F, X>(&mut self, f: F) -> Vec<X>
    // where
    //     F: Fn(&mut V) -> X,
    // {
    //     match self {
    //         NamedVec::Split(s, _) => s
    //             .left
    //             .iter_mut()
    //             .map(|p| f(&mut p.value))
    //             .chain(s.right.iter_mut().map(|p| f(&mut p.value)))
    //             .collect(),
    //         NamedVec::Unsplit(u) => u.members.iter_mut().map(|p| f(&mut p.value)).collect(),
    //     }
    // }

    // /// Apply function to non-center values with values, altering them in place
    // pub(crate) fn alter_non_center_values_zip<E, F, X>(
    //     &mut self,
    //     xs: Vec<X>,
    //     f: F,
    // ) -> Result<Vec<E>, KeyLengthError>
    // where
    //     F: Fn(&mut V, X) -> E,
    // {
    //     self.check_keys_length(&xs[..], false)?;
    //     let res = match self {
    //         NamedVec::Split(s, _) => {
    //             let nleft = s.left.len();
    //             let nright = s.right.len();
    //             let mut it = xs.into_iter();
    //             let left_r: Vec<_> = s
    //                 .left
    //                 .iter_mut()
    //                 .zip(it.by_ref().take(nleft))
    //                 .map(|(y, x)| f(&mut y.value, x))
    //                 .collect();
    //             let right_r: Vec<_> = s
    //                 .right
    //                 .iter_mut()
    //                 .zip(it.by_ref().take(nright))
    //                 .map(|(y, x)| f(&mut y.value, x))
    //                 .collect();
    //             left_r.into_iter().chain(right_r).collect()
    //         }
    //         NamedVec::Unsplit(u) => u
    //             .members
    //             .iter_mut()
    //             .zip(xs)
    //             .map(|(p, x)| f(&mut p.value, x))
    //             .collect(),
    //     };
    //     Ok(res)
    // }

    /// Return position of center, if it exists
    pub(crate) fn center_index(&self) -> Option<MeasIndex> {
        match self {
            NamedVec::Split(s, _) => Some(s.left.len().into()),
            NamedVec::Unsplit(_) => None,
        }
    }

    /// Apply function over center value, possibly changing it's type
    #[allow(clippy::type_complexity)]
    pub(crate) fn map_center_value<F, X, W, E>(
        self,
        f: F,
    ) -> DeferredResult<NamedVec<K, K::Wrapper<Shortname>, X, V>, W, IndexedElementError<E>>
    where
        F: Fn(IndexedElement<&Shortname, U>) -> DeferredResult<X, W, E>,
    {
        match self {
            NamedVec::Split(s, _) => {
                let c = s.center;
                let index = s.left.len().into();
                let ckey = c.key;
                let e = IndexedElement::new(index, &ckey, c.value);
                f(e).def_map_value(|value| {
                    let center = Pair { value, key: ckey };
                    NamedVec::new_split(s.left, center, s.right)
                })
                .def_map_errors(|error| IndexedElementError { error, index })
            }
            NamedVec::Unsplit(u) => Ok(Tentative::new1(NamedVec::Unsplit(u))),
        }
    }

    /// Apply function over non-center values, possibly changing their type
    pub(crate) fn map_non_center_values<E, F, W, ToV>(
        self,
        f: F,
    ) -> DeferredResult<WrappedNamedVec<K, U, ToV>, W, IndexedElementError<E>>
    where
        F: Fn(MeasIndex, V) -> DeferredResult<ToV, W, E>,
    {
        let go = |xs: WrappedPairedVec<K, V>, offset: usize| {
            xs.into_iter()
                .enumerate()
                .map(|(i, p)| {
                    let j = i + offset;
                    f(j.into(), p.value)
                        .def_map_value(|value| Pair { key: p.key, value })
                        .def_map_errors(|error| IndexedElementError {
                            index: j.into(),
                            error,
                        })
                })
                .gather()
                .map_err(DeferredFailure::mconcat)
                .map(Tentative::mconcat)
        };
        match self {
            NamedVec::Split(s, _) => {
                let nleft = s.left.len();
                let lres = go(s.left, 0);
                let rres = go(s.right, nleft + 1);
                lres.def_zip(rres)
                    .def_map_value(|(left, right)| NamedVec::new_split(left, *s.center, right))
            }
            NamedVec::Unsplit(u) => {
                go(u.members, 0).def_map_value(|members| NamedVec::new_unsplit(members))
            }
        }
    }

    /// Return number of all elements.
    pub(crate) fn len(&self) -> usize {
        self.iter().count()
    }

    /// Return number of non-center elements.
    pub(crate) fn len_non_center(&self) -> usize {
        self.iter().filter(|(_, r)| r.is_non_center()).count()
    }

    /// Return true if there are no contained elements.
    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get reference at position.
    #[allow(clippy::type_complexity)]
    pub fn get(
        &self,
        index: MeasIndex,
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
        index: MeasIndex,
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

    // /// Get reference to value with name.
    // pub(crate) fn get_name(&self, n: &Shortname) -> Option<(MeasIndex, Element<&U, &V>)> {
    //     if let Some(c) = self.as_center() {
    //         if c.key == n {
    //             return Some((c.index, Element::Center(c.value)));
    //         }
    //     }
    //     self.iter()
    //         .flat_map(|(i, r)| r.non_center().map(|x| (i, x)))
    //         .find(|(_, p)| K::as_opt(&p.key).is_some_and(|kn| kn == n))
    //         .map(|(i, p)| (i, Element::NonCenter(&p.value)))
    // }

    // /// Get mutable reference to value with name.
    // pub(crate) fn get_name_mut(
    //     &mut self,
    //     n: &Shortname,
    // ) -> Option<(MeasIndex, Element<&mut U, &mut V>)> {
    //     match self {
    //         NamedVec::Split(s, _) => {
    //             let nleft = s.left.len();
    //             Self::value_by_name_mut(&mut s.left, n)
    //                 .map(|(i, p)| (i.into(), Element::NonCenter(p)))
    //                 .or(if &s.center.key == n {
    //                     Some((nleft.into(), Element::Center(&mut s.center.value)))
    //                 } else {
    //                     None
    //                 })
    //                 .or(Self::value_by_name_mut(&mut s.right, n)
    //                     .map(|(i, p)| ((i + nleft + 1).into(), Element::NonCenter(p))))
    //         }
    //         NamedVec::Unsplit(u) => Self::value_by_name_mut(&mut u.members, n)
    //             .map(|(i, p)| (i.into(), Element::NonCenter(p))),
    //     }
    // }

    /// Add a new non-center element at the end of the vector
    pub(crate) fn push(
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
    pub(crate) fn insert(
        &mut self,
        index: MeasIndex,
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
    pub(crate) fn replace_at(
        &mut self,
        index: MeasIndex,
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
                        (Self::new_unsplit(members), Element::Center(s.center.value))
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
    pub(crate) fn replace_named(&mut self, name: &Shortname, value: V) -> Option<Element<U, V>> {
        let index = self.find_with_name(name)?;
        self.replace_at(index, value).ok()
    }

    /// Rename an element at index.
    ///
    /// If index points to the center element and the wrapped name contains
    /// nothing, the default name will be assigned. Return error if index is
    /// out of bounds or name is not unique. Return pair of old and new name
    /// on success.
    pub(crate) fn rename(
        &mut self,
        index: MeasIndex,
        key: K::Wrapper<Shortname>,
    ) -> Result<(Shortname, Shortname), RenameError> {
        let i = self
            .check_element_index(index, true)
            .map_err(RenameError::Index)?;
        let k = to_opt_or_indexed::<K>(K::as_ref(&key), index);
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
            let old_k = to_opt_or_indexed::<K>(K::as_ref(&old), index);
            Ok((old_k, k))
        }
    }

    /// Rename center element.
    ///
    /// Return previous name if center exists.
    pub(crate) fn rename_center(&mut self, name: Shortname) -> Option<Shortname> {
        match self {
            NamedVec::Split(s, _) => Some(mem::replace(&mut s.center.key, name)),
            NamedVec::Unsplit(_) => None,
        }
    }

    /// Push a new center element to the end of the vector
    ///
    /// Return error if center already exists.
    pub(crate) fn push_center(
        &mut self,
        name: Shortname,
        value: U,
    ) -> Result<(), InsertCenterError> {
        let key = self
            .check_name(name)
            .map_err(InsertError::NonUnique)
            .map_err(InsertCenterError::Insert)?;
        let p = Pair { key, value };
        let (newself, ret) = match mem::replace(self, dummy()) {
            Self::Unsplit(u) => (NamedVec::new_split(u.members, p, vec![]), Ok(())),
            s @ Self::Split(_, _) => (s, Err(InsertCenterError::Present)),
        };
        *self = newself;
        ret
    }

    /// Insert a new center element at a given position.
    ///
    /// Return error if center already exists.
    pub(crate) fn insert_center(
        &mut self,
        index: MeasIndex,
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
            Self::Unsplit(u) => {
                let mut it = u.members.into_iter();
                let left: Vec<_> = it.by_ref().take(i).collect();
                let right: Vec<_> = it.collect();
                (NamedVec::new_split(left, p, right), Ok(()))
            }
            s @ Self::Split(_, _) => (s, Err(InsertCenterError::Present)),
        };
        *self = newself;
        ret
    }

    /// Remove key/value pair by name.
    pub(crate) fn remove_index(
        &mut self,
        index: MeasIndex,
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
                        (NamedVec::new_unsplit(new), ret)
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
    /// Return error if name not found.
    pub(crate) fn remove_name(
        &mut self,
        n: &Shortname,
    ) -> Result<(MeasIndex, Element<U, V>), KeyNotFoundError> {
        let go = |xs: &mut Vec<_>| {
            if let Some(i) = Self::position_by_name(xs, n) {
                let p = xs.remove(i);
                Ok((i.into(), p.value))
            } else {
                Err(KeyNotFoundError(n.clone()))
            }
        };
        let (newself, ret) = match mem::replace(self, dummy()) {
            NamedVec::Split(mut s, p) => {
                if let Ok((i, v)) = go(&mut s.left).or(go(&mut s.right)) {
                    (NamedVec::Split(s, p), Ok((i, Element::NonCenter(v))))
                } else if &s.center.key == n {
                    let i = s.left.len().into();
                    let xs = s.left.into_iter().chain(s.right).collect();
                    let new = NamedVec::new_unsplit(xs);
                    (new, Ok((i, Element::Center(s.center.value))))
                } else {
                    (NamedVec::Split(s, p), Err(KeyNotFoundError(n.clone())))
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

    /// Set keys to list
    ///
    /// If center key does not exist, return an error.
    ///
    /// List must be the same length as all non-center keys and must be unique
    /// (including the center key).
    pub(crate) fn set_keys(
        &mut self,
        ks: Vec<K::Wrapper<Shortname>>,
    ) -> Result<NameMapping, SetKeysError>
    where
        K::Wrapper<Shortname>: Clone,
    {
        self.check_keys_length(&ks[..], true)
            .map_err(SetNamesError::Length)?;
        if !all_unique_names::<K>(ks.iter().map(K::as_ref).collect()) {
            return Err(SetNamesError::NonUnique.into());
        }
        let mut mapping = HashMap::new();
        let mut go = |side: &mut WrappedPairedVec<K, V>, ks_side: Vec<K::Wrapper<Shortname>>| {
            for (p, k) in side.iter_mut().zip(ks_side) {
                let old = mem::replace(&mut p.key, k.clone());
                if let (Some(old_name), Some(new_name)) = (K::to_opt(old), K::to_opt(k)) {
                    mapping.insert(old_name, new_name);
                }
            }
        };
        match self {
            NamedVec::Split(s, _) => {
                let mut it = ks.into_iter();
                // ASSUME this won't fail because we checked length above
                let ks_left = it.by_ref().take(s.left.len()).collect();
                let center = it.by_ref().next().expect("center should be set");
                let ks_right = it.collect();
                if let Some(center_name) = K::to_opt(center) {
                    go(&mut s.left, ks_left);
                    s.center.key = center_name;
                    go(&mut s.right, ks_right);
                } else {
                    return Err(SetKeysError::MissingCenter);
                }
            }
            NamedVec::Unsplit(u) => go(&mut u.members, ks),
        }
        Ok(mapping)
    }

    // /// Set non-center keys to list
    // ///
    // /// The center key cannot be replaced by this method since the list will
    // /// contain wrapped names which may or may not have a name inside, and
    // /// the center value always has a name.
    // ///
    // /// List must be the same length as all non-center keys and must be unique
    // /// (including the center key).
    // pub(crate) fn set_non_center_keys(
    //     &mut self,
    //     ks: Vec<K::Wrapper<Shortname>>,
    // ) -> Result<NameMapping, SetNamesError>
    // where
    //     K::Wrapper<Shortname>: Clone,
    // {
    //     self.check_keys_length(&ks[..], false)
    //         .map_err(SetNamesError::Length)?;
    //     let center = self.as_center().map(|x| K::wrap(x.key));
    //     let all_keys = ks.iter().map(K::as_ref).chain(center).collect();
    //     if !self.as_prefix().all_unique::<K>(all_keys) {
    //         return Err(SetNamesError::NonUnique);
    //     }
    //     let mut mapping = HashMap::new();
    //     let mut go = |side: &mut WrappedPairedVec<K, V>, ks_side: Vec<K::Wrapper<Shortname>>| {
    //         for (p, k) in side.iter_mut().zip(ks_side) {
    //             let old = mem::replace(&mut p.key, k.clone());
    //             if let (Some(old_name), Some(new_name)) = (K::to_opt(old), K::to_opt(k)) {
    //                 mapping.insert(old_name, new_name);
    //             }
    //         }
    //     };
    //     match self {
    //         NamedVec::Split(s, _) => {
    //             let mut ks_left = ks;
    //             let ks_right = ks_left.split_off(s.left.len());
    //             go(&mut s.left, ks_left);
    //             go(&mut s.right, ks_right);
    //         }
    //         NamedVec::Unsplit(u) => go(&mut u.members, ks),
    //     }
    //     Ok(mapping)
    // }

    /// Set all names to list of Shortnames
    ///
    /// This will update the center value along with everything else. Non-center
    /// keys will be wrapped such that they will contain a name.
    ///
    /// Supplied list must be unique and have the same length as the target
    /// vector.
    pub(crate) fn set_names(&mut self, ns: Vec<Shortname>) -> Result<NameMapping, SetNamesError> {
        self.check_keys_length(&ns[..], true)
            .map_err(SetNamesError::Length)?;
        if !all_unique(ns.iter()) {
            return Err(SetNamesError::NonUnique);
        }
        let mut mapping = HashMap::new();
        let mut go = |side: &mut WrappedPairedVec<K, V>, ns_side: Vec<Shortname>| {
            for (p, n) in side.iter_mut().zip(ns_side) {
                let old = mem::replace(&mut p.key, K::wrap(n.clone()));
                if let Some(old_name) = K::to_opt(old) {
                    mapping.insert(old_name, n);
                }
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
                let old = mem::replace(&mut s.center.key, n_center.clone());
                mapping.insert(old, n_center);
            }
            NamedVec::Unsplit(u) => go(&mut u.members, ns),
        }
        Ok(mapping)
    }

    /// Replace any value with a center value with name.
    pub(crate) fn replace_center_by_name<F, W, E>(
        &mut self,
        n: &Shortname,
        value: U,
        to_v: F,
    ) -> DeferredResult<Option<Element<U, V>>, W, E>
    where
        F: FnOnce(MeasIndex, U) -> PassthruResult<V, Box<U>, W, E>,
        E: From<SetCenterError>,
    {
        Tentative::new1(self.find_with_name(n)).and_maybe(|x| {
            x.map_or(Ok(Tentative::new1(None)), |index| {
                self.replace_center_at(index, value, to_v)
                    .def_map_value(Some)
            })
        })
    }

    /// Replace any value with a center value with name.
    pub(crate) fn replace_center_by_name_nofail<F>(
        &mut self,
        n: &Shortname,
        value: U,
        to_v: F,
    ) -> Option<Element<U, V>>
    where
        F: FnOnce(MeasIndex, U) -> V,
    {
        self.find_with_name(n)
            .map(|index| self.replace_center_at_nofail(index, value, to_v).unwrap())
    }

    /// Replace any value with a center value under index.
    ///
    /// If successful, return the replaced value. If index points to a center
    /// element, return the replaced center value. If index points to non-center,
    /// convert the current center value to non-center value and replace/return
    /// the non-center value under index.
    ///
    /// Fail if name at index to be converted is blank or
    /// if the previous center value cannot be converted back to a non-center
    /// value.
    pub(crate) fn replace_center_at<F, W, E>(
        &mut self,
        index: MeasIndex,
        value: U,
        to_v: F,
    ) -> DeferredResult<Element<U, V>, W, E>
    where
        F: FnOnce(MeasIndex, U) -> PassthruResult<V, Box<U>, W, E>,
        E: From<SetCenterError>,
    {
        if !self
            .get(index)
            .unwrap()
            .both(|_| true, |(n, _)| K::as_opt(n).is_some())
        {
            return Err(DeferredFailure::new1(SetCenterError::NoName));
        }

        let tnt = self
            .check_element_index(index, true)
            .map_err(SetCenterError::Index)
            .into_deferred()?;

        tnt.and_maybe(|i| self.replace_center_at_inner(i.into(), value, to_v))
    }

    /// Replace any value with a center value under index.
    ///
    /// If successful, return the replaced value. If index points to a center
    /// element, return the replaced center value. If index points to non-center,
    /// convert the current center value to non-center value and replace/return
    /// the non-center value under index.
    ///
    /// Fail if name at index to be converted is blank or
    /// if the previous center value cannot be converted back to a non-center
    /// value.
    pub(crate) fn replace_center_at_nofail<F>(
        &mut self,
        index: MeasIndex,
        value: U,
        to_v: F,
    ) -> Result<Element<U, V>, SetCenterError>
    where
        F: FnOnce(MeasIndex, U) -> V,
    {
        if !self
            .get(index)
            .unwrap()
            .both(|_| true, |(n, _)| K::as_opt(n).is_some())
        {
            return Err(SetCenterError::NoName);
        }

        let i = self.check_element_index(index, true)?;

        let ret = self
            .replace_center_at_inner(i.into(), value, |j, u| {
                Ok(Tentative::new_infallible(to_v(j, u)))
            })
            .def_unwrap_infallible();
        Ok(ret)
    }

    fn replace_center_at_inner<F, W, E>(
        &mut self,
        index: MeasIndex,
        value: U,
        to_v: F,
    ) -> DeferredResult<Element<U, V>, W, E>
    where
        F: FnOnce(MeasIndex, U) -> PassthruResult<V, Box<U>, W, E>,
    {
        let res = match mem::replace(self, dummy()) {
            NamedVec::Split(s, _) => match split_at_index::<K, U, V>(s, index.into()) {
                PartialSplit::Left(left, center, right) => {
                    let center_key = center.key;
                    match to_v(index, center.value) {
                        Ok(pass) => Ok(pass.map(|old_center_value| {
                            let sp = Self::new_split_from_left(
                                left.left,
                                left.selected.key,
                                value,
                                left.right,
                                center_key,
                                old_center_value,
                                right,
                            )
                            .unwrap();
                            (sp, Element::NonCenter(left.selected.value))
                        })),
                        Err(fail) => Err(fail.map_passthru(|center_value| {
                            Self::recover_split_from_left(
                                left.left,
                                left.selected.key,
                                left.selected.value,
                                left.right,
                                center_key,
                                *center_value,
                                right,
                            )
                        })),
                    }
                }

                PartialSplit::Center(x) => {
                    let center = Pair {
                        key: x.center.key,
                        value,
                    };
                    Ok(Tentative::new1((
                        Self::new_split(x.left, center, x.right),
                        Element::Center(x.center.value),
                    )))
                }

                PartialSplit::Right(left, center, right) => {
                    let center_key = center.key;
                    match to_v(index, center.value) {
                        Ok(pass) => Ok(pass.map(|old_center_value| {
                            let sp = Self::new_split_from_right(
                                left,
                                center_key,
                                old_center_value,
                                right.left,
                                right.selected.key,
                                value,
                                right.right,
                            )
                            .unwrap();
                            (sp, Element::NonCenter(right.selected.value))
                        })),
                        Err(fail) => Err(fail.map_passthru(|center_value| {
                            Self::recover_split_from_right(
                                left,
                                center_key,
                                *center_value,
                                right.left,
                                right.selected.key,
                                right.selected.value,
                                right.right,
                            )
                        })),
                    }
                }
            },

            NamedVec::Unsplit(u) => {
                let x = split_paired_vec::<K, V>(u.members, index.into());
                let ret = x.selected.value;
                let center = Pair {
                    key: K::to_opt(x.selected.key).unwrap(),
                    value,
                };
                Ok(Tentative::new1((
                    Self::new_split(x.left, center, x.right),
                    Element::NonCenter(ret),
                )))
            }
        };

        match res {
            Ok(pass) => Ok(pass.map(|(newself, ret)| {
                *self = newself;
                ret
            })),
            Err(fail) => Err(fail.map_passthru(|newself| {
                *self = newself;
            })),
        }
    }

    /// Set center to be the element with name if it exists.
    pub(crate) fn set_center_by_name<Fswap, FtoU, W, E>(
        &mut self,
        n: &Shortname,
        swap: Fswap,
        to_u: FtoU,
    ) -> DeferredResult<bool, W, E>
    where
        Fswap: FnOnce(MeasIndex, U, V) -> PassthruResult<(V, U), Box<(U, V)>, W, E>,
        FtoU: FnOnce(MeasIndex, V) -> PassthruResult<U, Box<V>, W, E>,
        E: From<SetCenterError>,
    {
        Tentative::new1(self.find_with_name(n)).and_maybe(|x| {
            x.map_or(Ok(Tentative::new1(false)), |index| {
                self.set_center_by_index(index, swap, to_u)
            })
        })
    }

    /// Set center to be the element with index if it exists.
    pub(crate) fn set_center_by_index<Fswap, FtoU, W, E>(
        &mut self,
        index: MeasIndex,
        swap: Fswap,
        to_u: FtoU,
    ) -> DeferredResult<bool, W, E>
    where
        Fswap: FnOnce(MeasIndex, U, V) -> PassthruResult<(V, U), Box<(U, V)>, W, E>,
        FtoU: FnOnce(MeasIndex, V) -> PassthruResult<U, Box<V>, W, E>,
        E: From<SetCenterError>,
    {
        if !self
            .get(index)
            .unwrap()
            .both(|_| true, |(n, _)| K::as_opt(n).is_some())
        {
            return Err(DeferredFailure::new1(SetCenterError::NoName));
        }
        self.check_element_index(index, true)
            .map_err(SetCenterError::Index)
            .into_deferred()
            .def_and_tentatively(|i| match mem::replace(self, dummy()) {
                NamedVec::Split(s, p) => match split_at_index::<K, U, V>(s, i) {
                    PartialSplit::Left(left, center, right) => {
                        let center_key = center.key;
                        match swap(i.into(), center.value, left.selected.value) {
                            Ok(tnt) => tnt.map(|(right_value, center_value)| {
                                let sp = Self::new_split_from_left(
                                    left.left,
                                    left.selected.key,
                                    center_value,
                                    left.right,
                                    center_key,
                                    right_value,
                                    right,
                                )
                                .unwrap();
                                (sp, true)
                            }),
                            Err(fail) => {
                                fail.unfail().map(|x| *x).map(|(center_value, left_value)| {
                                    let sp = Self::recover_split_from_left(
                                        left.left,
                                        left.selected.key,
                                        left_value,
                                        left.right,
                                        center_key,
                                        center_value,
                                        right,
                                    );
                                    (sp, false)
                                })
                            }
                        }
                    }

                    PartialSplit::Center(sc) => Tentative::new1((NamedVec::Split(sc, p), false)),

                    PartialSplit::Right(left, center, right) => {
                        let center_key = center.key;
                        match swap(i.into(), center.value, right.selected.value) {
                            Ok(tnt) => tnt.map(|(right_value, center_value)| {
                                let sp = Self::new_split_from_right(
                                    left,
                                    center_key,
                                    right_value,
                                    right.left,
                                    right.selected.key,
                                    center_value,
                                    right.right,
                                )
                                .unwrap();
                                (sp, true)
                            }),
                            Err(fail) => {
                                fail.unfail()
                                    .map(|x| *x)
                                    .map(|(center_value, right_value)| {
                                        let sp = Self::recover_split_from_right(
                                            left,
                                            center_key,
                                            center_value,
                                            right.left,
                                            right.selected.key,
                                            right_value,
                                            right.right,
                                        );
                                        (sp, false)
                                    })
                            }
                        }
                    }
                },

                NamedVec::Unsplit(u) => {
                    let x = split_paired_vec::<K, V>(u.members, i);
                    match to_u(i.into(), x.selected.value) {
                        Ok(tnt) => tnt.map(|new_value| {
                            let center = Pair::new(K::to_opt(x.selected.key).unwrap(), new_value);
                            (Self::new_split(x.left, center, x.right), true)
                        }),
                        Err(fail) => fail.unfail().map(|old_value| {
                            let center = Pair::new(x.selected.key, *old_value);
                            let new = x.left.into_iter().chain([center]).chain(x.right).collect();
                            (Self::new_unsplit(new), false)
                        }),
                    }
                }
            })
            .def_map_value(|(newself, flag)| {
                *self = newself;
                flag
            })
    }

    /// Convert the center element into a non-center element.
    ///
    /// Has no effect if there already is no center element.
    ///
    /// Return true if vector is updated.
    pub(crate) fn unset_center<F, W, E, X>(&mut self, to_v: F) -> Tentative<Option<X>, W, E>
    where
        F: FnOnce(MeasIndex, U) -> PassthruResult<(V, X), Box<U>, W, E>,
    {
        match mem::replace(self, dummy()) {
            NamedVec::Split(s, _) => {
                let center_key = s.center.key;
                let index = (s.left.len()).into();
                match to_v(index, s.center.value) {
                    Ok(tnt) => tnt.map(|(value, ret)| {
                        let non_center = Pair {
                            key: K::wrap(center_key),
                            value,
                        };
                        let members = s
                            .left
                            .into_iter()
                            .chain([non_center])
                            .chain(s.right)
                            .collect();
                        (Self::new_unsplit(members), Some(ret))
                    }),
                    Err(fail) => fail.unfail().map(|value| {
                        let center = Pair {
                            key: center_key,
                            value: *value,
                        };
                        let sp = Self::new_split(s.left, center, s.right);
                        (sp, None)
                    }),
                }
            }
            NamedVec::Unsplit(u) => Tentative::new1((NamedVec::Unsplit(u), None)),
        }
        .map(|(newself, flag)| {
            *self = newself;
            flag
        })
    }

    // TODO this seems like it could be more general (like "map_keys" or something)
    /// Unwrap and rewrap the non-center names of vector.
    ///
    /// This may fail if the original wrapped name cannot be converted.
    #[allow(clippy::type_complexity)]
    pub(crate) fn try_rewrapped<J>(
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
                let (left, right) = lres.mult_zip(rres)?;
                NamedVec::new_split(left, *s.center, right)
            }
            NamedVec::Unsplit(u) => NamedVec::new_unsplit(go(u.members, 0)?),
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

    // fn from_center(p: Center<U>) -> WrappedPair<K, V>
    // where
    //     V: From<U>,
    // {
    //     Pair {
    //         key: K::wrap(p.key),
    //         value: p.value.into(),
    //     }
    // }

    fn position_by_name(xs: &WrappedPairedVec<K, V>, n: &Shortname) -> Option<usize> {
        xs.iter()
            .position(|p| K::as_opt(&p.key).is_some_and(|kn| kn == n))
    }

    // fn value_by_name_mut<'a>(
    //     xs: &'a mut WrappedPairedVec<K, V>,
    //     n: &Shortname,
    // ) -> Option<(usize, &'a mut V)> {
    //     xs.iter_mut()
    //         .enumerate()
    //         .find(|(_, p)| K::as_opt(&p.key).is_some_and(|kn| kn == n))
    //         .map(|(i, p)| (i, &mut p.value))
    // }

    fn check_key(
        &self,
        key: K::Wrapper<Shortname>,
        index: MeasIndex,
    ) -> Result<(K::Wrapper<Shortname>, Shortname), NonUniqueKeyError> {
        let name = to_opt_or_indexed::<K>(K::as_ref(&key), index);
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
        index: MeasIndex,
        include_center: bool,
    ) -> Result<usize, ElementIndexError> {
        let len = self.len();
        IndexFromOne::from(index).check_index(len).map_or_else(
            |e| {
                Err(ElementIndexError {
                    index: e,
                    center: None,
                })
            },
            |i| {
                if let Some(j) = self.center_index()
                    && !include_center
                    && usize::from(j) == i
                {
                    return Err(ElementIndexError {
                        index: IndexError {
                            index: i.into(),
                            len,
                        },
                        center: Some(j),
                    });
                }
                Ok(i)
            },
        )
    }

    fn check_boundary_index(&self, index: MeasIndex) -> Result<usize, BoundaryIndexError> {
        IndexFromOne::from(index).check_boundary_index(self.len())
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

    fn find_with_name(&self, name: &Shortname) -> Option<MeasIndex> {
        self.iter()
            .find(|(_, x)| {
                x.as_ref().both(
                    |l| &l.key == name,
                    |r| K::as_opt(&r.key).is_some_and(|k| k == name),
                )
            })
            .map(|(i, _)| i)
    }

    fn new_split(
        left: WrappedPairedVec<K, V>,
        center: Center<U>,
        right: WrappedPairedVec<K, V>,
    ) -> Self {
        NamedVec::Split(
            SplitVec {
                left,
                center: Box::new(center),
                right,
            },
            PhantomData,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new_split_from_left(
        left_left: WrappedPairedVec<K, V>,
        new_center_name: K::Wrapper<Shortname>,
        new_center_value: U,
        left_right: WrappedPairedVec<K, V>,
        old_center_name: Shortname,
        old_center_value: V,
        right: WrappedPairedVec<K, V>,
    ) -> Option<Self> {
        let new_center = Pair {
            key: K::to_opt(new_center_name)?,
            value: new_center_value,
        };
        let new_right = Pair {
            key: K::wrap(old_center_name),
            value: old_center_value,
        };
        Some(Self::new_split(
            left_left,
            new_center,
            left_right
                .into_iter()
                .chain([new_right])
                .chain(right)
                .collect(),
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn recover_split_from_left(
        left_left: WrappedPairedVec<K, V>,
        left_key: K::Wrapper<Shortname>,
        left_value: V,
        left_right: WrappedPairedVec<K, V>,
        center_key: Shortname,
        center_value: U,
        right: WrappedPairedVec<K, V>,
    ) -> Self {
        let center = Pair {
            key: center_key,
            value: center_value,
        };
        let new_left = Pair {
            key: left_key,
            value: left_value,
        };
        Self::new_split(
            left_left
                .into_iter()
                .chain([new_left])
                .chain(left_right)
                .collect(),
            center,
            right,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new_split_from_right(
        left: WrappedPairedVec<K, V>,
        old_center_name: Shortname,
        old_center_value: V,
        right_left: WrappedPairedVec<K, V>,
        new_center_key: K::Wrapper<Shortname>,
        new_center_value: U,
        right_right: WrappedPairedVec<K, V>,
    ) -> Option<Self> {
        let new_center = Pair {
            key: K::to_opt(new_center_key)?,
            value: new_center_value,
        };
        let new_left = Pair {
            key: K::wrap(old_center_name),
            value: old_center_value,
        };
        Some(Self::new_split(
            left.into_iter()
                .chain([new_left])
                .chain(right_left)
                .collect(),
            new_center,
            right_right,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn recover_split_from_right(
        left: WrappedPairedVec<K, V>,
        center_key: Shortname,
        center_value: U,
        right_left: WrappedPairedVec<K, V>,
        right_key: K::Wrapper<Shortname>,
        right_value: V,
        right_right: WrappedPairedVec<K, V>,
    ) -> Self {
        let center = Pair {
            key: center_key,
            value: center_value,
        };
        let new_right = Pair {
            key: right_key,
            value: right_value,
        };
        Self::new_split(
            left,
            center,
            right_left
                .into_iter()
                .chain([new_right])
                .chain(right_right)
                .collect(),
        )
    }

    fn new_unsplit(members: WrappedPairedVec<K, V>) -> Self {
        NamedVec::Unsplit(UnsplitVec { members })
    }
}

impl<K: MightHave, U, V> Clone for Eithers<K, U, V>
where
    K::Wrapper<Shortname>: Clone,
    U: Clone,
    V: Clone,
{
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<K: MightHave, U, V> Eithers<K, U, V> {
    pub(crate) fn non_center_names(&self) -> impl Iterator<Item = &Shortname> {
        self.0
            .iter()
            .filter_map(|x| x.as_ref().non_center().and_then(|v| K::as_opt(&v.0)))
    }

    #[must_use]
    pub fn inner_into<U0, V0>(self) -> Eithers<K, U0, V0>
    where
        U0: From<U>,
        V0: From<V>,
    {
        Eithers(
            self.0
                .into_iter()
                .map(|e| e.bimap(|(n, y)| (n, y.into()), |(n, y)| (n, y.into())))
                .collect(),
        )
    }
}

impl<U, V> Element<U, V> {
    pub fn inner_into<U1, V1>(self) -> Element<U1, V1>
    where
        U1: From<U>,
        V1: From<V>,
    {
        self.bimap(Into::into, Into::into)
    }

    pub fn unzip<K: MightHave>(e: EitherPair<K, U, V>) -> (K::Wrapper<Shortname>, Self) {
        e.both(
            |p| (K::wrap(p.key), Self::Center(p.value)),
            |p| (p.key, Self::NonCenter(p.value)),
        )
    }

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

    pub fn map_center<F, X>(self, f: F) -> Element<X, V>
    where
        F: Fn(U) -> X,
    {
        match self {
            Element::Center(u) => Element::Center(f(u)),
            Element::NonCenter(v) => Element::NonCenter(v),
        }
    }

    pub fn map_non_center<F, X>(self, f: F) -> Element<U, X>
    where
        F: Fn(V) -> X,
    {
        match self {
            Element::Center(u) => Element::Center(u),
            Element::NonCenter(v) => Element::NonCenter(f(v)),
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

fn to_opt_or_indexed<X: MightHave>(x: X::Wrapper<&Shortname>, i: MeasIndex) -> Shortname {
    X::to_opt(x).cloned().unwrap_or(i.into())
}

fn all_unique_names<X: MightHave>(xs: Vec<X::Wrapper<&Shortname>>) -> bool {
    all_unique(
        xs.into_iter()
            .enumerate()
            .map(|(i, x)| to_opt_or_indexed::<X>(x, i.into())),
    )
}

fn all_unique<'a, T: Hash + Eq>(xs: impl Iterator<Item = T> + 'a) -> bool {
    let mut unique = HashSet::new();
    for x in xs {
        if unique.contains(&x) {
            return false;
        }
        unique.insert(x);
    }
    true
}

// dummy value to use when mutating NamedVec in place
fn dummy<K, W, U, V>() -> NamedVec<K, W, U, V> {
    NamedVec::Unsplit(UnsplitVec { members: vec![] })
}

enum PartialSplit<K, U, V> {
    Left(PairedSplit<K, V>, Box<Center<U>>, PairedVec<K, V>),
    Center(SplitVec<K, U, V>),
    Right(PairedVec<K, V>, Box<Center<U>>, PairedSplit<K, V>),
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
        Less => PartialSplit::Left(split_paired_vec::<K, V>(s.left, index), s.center, s.right),
        Equal => PartialSplit::Center(s),
        Greater => PartialSplit::Right(
            s.left,
            s.center,
            split_paired_vec::<K, V>(s.right, index - nleft - 1),
        ),
    }
}

#[derive(Debug, Display, Error)]
pub enum InsertError {
    Index(BoundaryIndexError),
    NonUnique(NonUniqueKeyError),
}

#[derive(Debug, Display, Error)]
pub enum RenameError {
    Index(ElementIndexError),
    NonUnique(NonUniqueKeyError),
}

#[derive(Debug, Error)]
#[error("'{0}' matches no measurement")]
pub struct KeyNotFoundError(Shortname);

#[derive(Debug, Error)]
pub enum InsertCenterError {
    #[error("Center already exists")]
    Present,
    #[error("{0}")]
    Insert(InsertError),
}

#[derive(Debug, Error)]
pub enum SetKeysError {
    #[error("{0}")]
    Names(#[from] SetNamesError),
    #[error("center must not be missing")]
    MissingCenter,
}

#[derive(Debug, Error)]
pub enum SetNamesError {
    #[error("{0}")]
    Length(#[from] KeyLengthError),
    #[error("not all supplied keys are unique")]
    NonUnique,
}

#[derive(Debug, Error)]
pub enum SetCenterError {
    #[error("{0}")]
    Index(#[from] ElementIndexError),
    #[error("index refers to element with no name")]
    NoName,
}

#[derive(Debug, Error)]
#[error("'{name}' already present")]
pub struct NonUniqueKeyError {
    name: Shortname,
}

#[derive(Debug, Error)]
pub struct ElementIndexError {
    index: IndexError,
    center: Option<MeasIndex>,
}

impl fmt::Display for ElementIndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        if let Some(c) = self.center.as_ref() {
            write!(
                f,
                "0-index must be 0 <= i < {} and not include center at {c}, got {}",
                self.index.len,
                usize::from(self.index.index)
            )
        } else {
            self.index.fmt(f)
        }
    }
}

#[derive(Debug, Error)]
#[error(
    "supplied list must be {this_len} ({c}including center) \
     elements long, got {other_len}",
    c = if self.include_center { "" } else { "not " }
)]
pub struct KeyLengthError {
    this_len: usize,
    other_len: usize,
    include_center: bool,
}

#[derive(Debug, Error)]
pub enum NewNamedVecError {
    #[error("names must be unique")]
    NonUnique,
    #[error("only zero or one center values allowed")]
    MultiCenter,
}

#[derive(Debug, Error)]
#[error("error for element {index}: {error}")]
pub struct IndexedElementError<E> {
    error: E,
    index: MeasIndex,
}

#[cfg(feature = "python")]
mod python {
    use super::{
        Eithers, Element, ElementIndexError, KeyLengthError, KeyNotFoundError, NonCenterElement,
        SetCenterError, SetKeysError, SetNamesError,
    };
    use crate::python::macros::{impl_index_err, impl_pyreflow_err};
    use crate::text::optional::MightHave;
    use crate::validated::shortname::Shortname;
    use pyo3::prelude::*;
    use pyo3::types::PyTuple;

    impl_index_err!(ElementIndexError);
    impl_index_err!(KeyNotFoundError);

    // derive(FromPyObject) will get confused by the wrapper; this is trivial
    // otherwise
    impl<'py, K, U, V> FromPyObject<'py> for Eithers<K, U, V>
    where
        K: MightHave,
        K::Wrapper<Shortname>: FromPyObject<'py>,
        U: FromPyObject<'py>,
        V: FromPyObject<'py>,
    {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            Ok(Self(ob.extract()?))
        }
    }

    impl<'py, V> FromPyObject<'py> for NonCenterElement<V>
    where
        V: FromPyObject<'py>,
    {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            if let Ok(t) = ob.downcast::<PyTuple>()
                && t.is_empty()
            {
                return Ok(Self(Element::Center(())));
            }
            Ok(Self(Element::NonCenter(ob.extract::<V>()?)))
        }
    }

    impl_pyreflow_err!(KeyLengthError);
    impl_pyreflow_err!(SetNamesError);
    impl_pyreflow_err!(SetKeysError);
    impl_pyreflow_err!(SetCenterError);
}

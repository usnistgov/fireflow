use crate::logging::{
    CommutativeResult, CommutativeResultIter as _, ErrorGroup, ErrorResult, ErrorsResult,
    LogResult, ResultExt as _,
};
use crate::macros::def_summary;
use crate::text::index::{BoundaryIndexError, IndexError, IndexFromOne, MeasIndex};
use crate::text::optional::MightHave;
use crate::text::relational::{
    KeyToNameLinkError, LinkName, OpticalNamedLinkError, TemporalNamedLinkError,
};
use crate::validated::shortname::Shortname;

use nonempty_collections::{IntoIteratorExt as _, NEVec, iter::NonEmptyIterator as _};
use type_families::{
    BifunctorOnce, Functor, Monoid, Pointed, impl_functor_once, impl_kind1, impl_kind2,
};

use derive_more::{Display, From, Into};
use derive_new::new;
use hashbrown::HashMap;
use itertools::Itertools as _;
use thiserror::Error;

use std::borrow::Cow;
use std::cmp::Ordering::{Equal, Greater, Less};
use std::collections::HashSet;
use std::convert::Infallible;
use std::fmt;
use std::hash::Hash;
use std::iter::once;
use std::mem;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use pyo3::prelude::*;

use super::relational::{IndicesToRemove, OpticalNamesToRemove};

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr},
    fireflow_types::python as py,
};

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
#[derive(Clone, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[new(visibility(""))]
pub struct NamedVec<K, U, V> {
    left: PairedVec<K, V>,
    center_right: Option<CenterRightVec<K, U, V>>,
}

impl<K, U, V> Default for NamedVec<K, U, V> {
    fn default() -> Self {
        Self::new(vec![], None)
    }
}

/// The center and right elements of a [`NamedVec`].
#[derive(Clone, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[new(visibility(""))]
struct CenterRightVec<K, U, V> {
    center: Center<U>,
    right: PairedVec<K, V>,
}

/// An key/value pair with an index
#[derive(new)]
pub struct IndexedElement<K, V> {
    pub index: MeasIndex,
    pub key: K,
    pub value: V,
}

// TODO use itertools::Either
/// A member in [`NamedVec`], either a "center" or "non-center" value
#[derive(Clone)]
#[cfg_attr(feature = "python", derive(FromPyObject, IntoPyObject))]
pub enum Element<U, V> {
    Center(U),
    NonCenter(V),
}

impl_kind2!(pub ElementFamily, Element);

/// Standalone wrapper representing a center value in [`NamedVec`]
#[derive(Clone, From, Into)]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
pub struct NonCenterElement<V>(pub Element<(), V>);

impl_kind1!(pub NonCenterElementFamily, NonCenterElement);
impl_functor_once!(
    NonCenterElement,
    self,
    f,
    NonCenterElement(self.0.second_once(f))
);

type PairedVec<K, V> = Vec<Pair<K, V>>;

/// A key/value pair
#[derive(Clone, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct Pair<K, V> {
    pub key: K,
    pub value: V,
}

/// All names from [`NamedVec`] in a set-like structure.
#[derive(new)]
#[new(visibility = "pub(crate)")]
pub struct NamedSet<'a> {
    center: Option<&'a Shortname>,
    non_center: HashSet<&'a Shortname>,
}

type Center<U> = Pair<Shortname, U>;

pub(crate) type Either<K, U, V> = Element<(Shortname, U), (K, V)>;

pub type EitherPair<K, U, V> = Element<Pair<Shortname, U>, Pair<K, V>>;

pub type Eithers<K, U, V> = Vec<Either<K, U, V>>;

pub type NameMapping = HashMap<Shortname, Shortname>;

/// Error when inserting new element into [`NamedVec`]
#[derive(From, Debug, Display, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum InsertError {
    /// Index out of range
    Index(BoundaryIndexError),
    /// New name is not unique
    NonUnique(NamePresentError),
}

/// Error when renaming element's name at index in [`NamedVec`]
#[derive(Debug, Display, Error)]
pub enum RenameError {
    /// Index not found
    Index(ElementIndexError),
    /// Name change results in duplicates
    NonUnique(NamePresentError),
}

/// Error when inserting new center element into [`NamedVec`]
#[derive(Debug, Error, Display, From)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum InsertCenterError {
    Push(PushCenterError),
    Index(BoundaryIndexError),
}

/// Error when pushing new center element to the right of [`NamedVec`]
#[derive(Debug, Error, Display, From)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum PushCenterError {
    NonUnique(NamePresentError),
    Present(CenterPresentError),
}

/// Error when setting all keys in a [`NamedVec`].
///
/// This is distinct from setting "names" which are [`Shortname`]. "Keys"
/// are names in containers which may or may not contain them.
#[derive(Debug, Error, Display, From)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum SetKeysError {
    Names(SetNamesError),
    MissingCenter(MissingCenterError),
}

/// Error when setting names ([`Shortname`]) in a [`NamedVec`]
#[derive(Debug, Error, Display, From)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum SetNamesError {
    Length(InputLengthError),
    NonUnique(NonUniqueKeysError),
}

/// Error when assigning an element in [`NamedVec`] to be the center element
#[derive(Debug, Error, Display, From)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum SetCenterError {
    Index(ElementIndexError),
    NoName(NoNameError),
}

/// Error when assigning an element in [`NamedVec`] to be the center element
#[derive(Debug, Error, Display, From)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum SetValuesError {
    Length(InputLengthError),
    Set(ElementMismatchErrors),
}

/// Error when building new [`NamedVec`] from list of elements
#[derive(Debug, Error, Display, From)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum NewNamedVecError {
    NonUnique(NonUniqueKeysError),
    MultiCenter(CenterPresentError),
}

/// Error when setting/altering the elements of [`NamedVec`]
#[derive(Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(E: Into<Self>))]
pub enum SetElementsError<E> {
    Length(InputLengthError),
    Mismatch(E),
}

/// Error when the center element of [`NamedVec`] is already present
#[derive(Debug, Error)]
#[error("center value specified multiple times")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct CenterPresentError;

/// Error when element in [`NamedVec`] does not have a name but one is expected.
#[derive(Debug, Error)]
#[error("index refers to element with no name")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct NoNameError;

/// Error when the center element of [`NamedVec`] is missing but expected
#[derive(Debug, Error)]
#[error("center must not be missing")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct MissingCenterError;

/// Error when final state of keys in [`NamedVec`] results in duplicates
#[derive(Debug, Error)]
#[error("some $PnN are duplicated")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct NonUniqueKeysError;

/// Error when name in [`NamedVec`] is not found
#[derive(Debug, Error)]
#[error("'{0}' matches no measurement")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr), pyerr(PyKeyError))]
pub struct NameNotFoundError(pub Shortname);

/// Error when name is already present in [`NamedVec`]
#[derive(Debug, Error, new)]
#[error("'{name}' already present")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct NamePresentError {
    name: Shortname,
}

/// Error when index is out of bounds for [`NamedVec`], optionally including center.
#[derive(Debug, Error, new)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr), pyerr(PyIndexError))]
pub struct ElementIndexError {
    index: IndexError,
    center: Option<MeasIndex>,
}

/// Error when element types do not match in [`NamedVec`]
#[derive(Debug, Error, new)]
#[error(
    "attempted to set a {to} at {index} when {from} is needed",
    to = if self.original_is_center { "non-center" } else { "center" },
    from = if self.original_is_center { "center" } else { "non-center" }
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct ElementMismatchError {
    index: MeasIndex,
    original_is_center: bool,
}

pub type ElementMismatchErrors = ErrorGroup<ElementMismatchError, ElementMismatchSummary>;

def_summary!(pub ElementMismatchSummary, "could not set new values");

impl fmt::Display for ElementIndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let len = self.index.len;
        let x = usize::from(self.index.index);
        if let Some(c) = self.center.as_ref() {
            write!(
                f,
                "0-index must be 0 <= i < {len} and not \
                 include center at {c}, got {x}",
            )
        } else {
            write!(f, "0-index must be 0 <= i < {len}, got {x}")
        }
    }
}

/// Error when input collection does not match number of elements in [`NamedVec`]
#[derive(Debug, Error)]
#[error(
    "input must be {this_len} ({c}including center) elements long, got {other_len}",
    c = if self.include_center { "" } else { "not " }
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct InputLengthError {
    this_len: usize,
    other_len: usize,
    include_center: bool,
}

// Implement methods for NamedVec

impl<K, U, V> NamedVec<K, U, V> {
    /// Build new NamedVec using either center or non-center values.
    ///
    /// Must contain either one or zero center values, otherwise return error.
    /// All names within keys (including center) must be unique.
    pub(crate) fn try_new(
        xs: impl IntoIterator<Item = Either<K, U, V>>,
    ) -> Result<Self, NewNamedVecError>
    where
        K: MightHave<Shortname>,
    {
        let mut left = vec![];
        let mut center = None;
        let mut right = vec![];
        for x in xs {
            match x {
                Element::NonCenter(y) => {
                    let p = Pair::new(y.0, y.1);
                    if center.is_none() {
                        left.push(p);
                    } else {
                        right.push(p);
                    }
                }
                Element::Center(y) => {
                    if center.is_none() {
                        center = Some(Pair::new(y.0, y.1));
                    } else {
                        return Err(CenterPresentError.into());
                    }
                }
            }
        }
        let s = if let Some(c) = center {
            Self::new_split(left, c, right)
        } else {
            Self::new_unsplit(left)
        };
        // TODO make this a method
        let names = s
            .iter()
            .map(|x| x.as_ref().both(|e| Some(&e.key), |o| o.key.as_opt()));
        if !all_unique_names(names) {
            return Err(NonUniqueKeysError.into());
        }
        Ok(s)
    }

    /// Return all names as a [`NamedSet`]
    pub(crate) fn named_set(&self) -> NamedSet<'_>
    where
        K: MightHave<Shortname>,
    {
        let c = self.as_center().map(|e| e.key);
        let nc = self.indexed_non_center_names().map(|(_, n)| n).collect();
        NamedSet::new(c, nc)
    }

    /// Return reference to center
    pub(crate) fn as_center(&self) -> Option<IndexedElement<&Shortname, &U>> {
        let right = self.center_right.as_ref()?;
        Some(IndexedElement::new(
            self.left.len().into(),
            &right.center.key,
            &right.center.value,
        ))
    }

    /// Return mutable reference to center
    pub fn as_center_mut(&mut self) -> Option<IndexedElement<&mut Shortname, &mut U>> {
        let right = self.center_right.as_mut()?;
        Some(IndexedElement::new(
            self.left.len().into(),
            &mut right.center.key,
            &mut right.center.value,
        ))
    }

    // pub fn into_iter(
    //     self,
    // ) -> impl IntoIterator<Item = (MeasIdx, Result<Pair<K, V>, Pair<Shortname, U>>)> {
    //     let go =
    //         |xs: Vec<Pair<K, V>>| xs.into_iter().enumerate().map(|(i, p)| (i.into(), Ok(p)));
    //     match self {
    //         NamedVec::Split(s, _) => {
    //             let c = (s.left.len().into(), Err(*s.center));
    //             go(s.left).chain(vec![c]).chain(go(s.right))
    //         }
    //         NamedVec::Unsplit(u) => go(u.members).chain(vec![]).chain(go(vec![])),
    //     }
    // }

    /// Return iterator over all elements with indices
    pub fn iter(&self) -> impl Iterator<Item = Element<&Pair<Shortname, U>, &Pair<K, V>>> {
        let right = self.center_right.iter().flat_map(|r| {
            once(Element::Center(&r.center)).chain(r.right.iter().map(Element::NonCenter))
        });
        self.left.iter().map(Element::NonCenter).chain(right)
    }

    /// Return iterator over all elements with indices
    pub fn iter_mut(
        &mut self,
    ) -> impl Iterator<Item = Element<&mut Pair<Shortname, U>, &mut Pair<K, V>>> {
        let right = self.center_right.iter_mut().flat_map(|r| {
            once(Element::Center(&mut r.center)).chain(r.right.iter_mut().map(Element::NonCenter))
        });
        self.left.iter_mut().map(Element::NonCenter).chain(right)
    }

    pub(crate) fn iter_common_values<'a, T: 'a>(&'a self) -> impl Iterator<Item = &'a T> + 'a
    where
        U: AsRef<T>,
        V: AsRef<T>,
    {
        self.iter()
            .map(|x| x.both(|l| l.value.as_ref(), |r| r.value.as_ref()))
    }

    pub(crate) fn iter_with<'a, T, F, G>(
        &'a self,
        f: &'a F,
        g: &'a G,
    ) -> impl Iterator<Item = T> + 'a
    where
        F: Fn(MeasIndex, &'a Pair<Shortname, U>) -> T,
        G: Fn(MeasIndex, &'a Pair<K, V>) -> T,
    {
        self.iter()
            .enumerate()
            .map(|(i, e)| e.both(|x| f(i.into(), x), |x| g(i.into(), x)))
    }

    // /// Return iterator over borrowed non-center values
    // pub(crate) fn iter_non_center_values(&self) -> impl Iterator<Item = (MeasIndex, &V)> + '_ {
    //     self.iter()
    //         .flat_map(|(i, x)| x.non_center().map(|p| (i, &p.value)))
    // }

    // /// Return iterator over borrowed non-center keys
    // pub(crate) fn iter_non_center_keys(&self) -> impl Iterator<Item = &K> + '_ {
    //     self.iter()
    //         .flat_map(|(_, x)| x.non_center().map(|p| &p.key))
    // }

    /// Return all existing names in the vector with their indices
    pub(crate) fn indexed_opt_names(&self) -> impl Iterator<Item = Option<&Shortname>>
    where
        K: MightHave<Shortname>,
    {
        self.iter()
            .map(|r| r.both(|x| Some(&x.key), |x| x.key.as_opt()))
    }

    /// Return all existing names in the vector with their indices
    pub(crate) fn indexed_names(&self) -> impl Iterator<Item = (MeasIndex, &Shortname)> + '_
    where
        K: MightHave<Shortname>,
    {
        self.iter().enumerate().filter_map(|(i, r)| {
            r.both(|x| Some(&x.key), |x| x.key.as_opt())
                .map(|x| (i.into(), x))
        })
    }

    pub(crate) fn indexed_name_map(&self) -> HashMap<MeasIndex, &Shortname>
    where
        K: MightHave<Shortname>,
    {
        self.indexed_names().collect()
    }

    pub(crate) fn named_indices(&self) -> HashMap<&Shortname, MeasIndex>
    where
        K: MightHave<Shortname>,
    {
        self.indexed_names().map(|(i, m)| (m, i)).collect()
    }

    pub(crate) fn all_indices_and_names_to_remove(
        &self,
    ) -> (IndicesToRemove, OpticalNamesToRemove<'_>)
    where
        K: MightHave<Shortname>,
    {
        let (js, ns): (HashSet<_>, HashSet<_>) = self.indexed_non_center_names().unzip();
        (js.into(), ns.into())
    }

    /// Return all existing non-center names in the vector with their indices
    pub(crate) fn indexed_non_center_names(
        &self,
    ) -> impl Iterator<Item = (MeasIndex, &Shortname)> + '_
    where
        K: MightHave<Shortname>,
    {
        self.iter()
            .enumerate()
            .filter_map(|(i, r)| r.both(|_| None, |x| x.key.as_opt()).map(|x| (i.into(), x)))
    }

    /// Return iterator over key names with non-existent names as default.
    pub(crate) fn iter_all_names(&self) -> impl Iterator<Item = Shortname> + '_
    where
        K: MightHave<Shortname>,
    {
        self.iter().enumerate().map(|(i, r)| {
            r.both(
                |x| x.key.clone(),
                |x| x.key.as_opt().cloned().unwrap_or(MeasIndex::from(i).into()),
            )
        })
    }

    /// Alter values with a function and payload.
    ///
    /// Center and non-center values will be projected to a common type.
    pub(crate) fn alter_common_values_zip<F, X, R, T>(
        &mut self,
        xs: impl IntoIterator<Item = X>,
        f: F,
    ) -> Result<Vec<R>, InputLengthError>
    where
        F: Fn(MeasIndex, &mut T, X) -> R,
        U: AsMut<T>,
        V: AsMut<T>,
    {
        self.alter_values_zip(
            xs.into_iter().collect(),
            |v, x| f(v.index, v.value.as_mut(), x),
            |v, x| f(v.index, v.value.as_mut(), x),
        )
    }

    /// Set current values to new values.
    ///
    /// The center in the new vector must be in the same position as the old.
    pub(crate) fn set_values(&mut self, xs: Vec<Element<U, V>>) -> Result<(), SetValuesError> {
        // check length and center position before doing anything, otherwise
        // we would need to reset the new vector if any error is found
        self.check_keys_length(&xs[..], true)?;
        let errs = self
            .iter()
            .zip(xs.iter())
            .enumerate()
            .map(|(i, (old, new))| match (old, new) {
                (Element::Center(_), Element::NonCenter(_)) => Some((i, true)),
                (Element::NonCenter(_), Element::Center(_)) => Some((i, false)),
                _ => None,
            })
            .filter_map(|x| x.map(|(i, is_center)| ElementMismatchError::new(i.into(), is_center)));
        ErrorGroup::try_new(errs)?;
        let _ = self.alter_values_zip_nocheck(
            xs,
            |e, y| y.both(|z| *e.value = z, |_| ()),
            |e, y| y.both(|_| (), |z| *e.value = z),
        );
        Ok(())
    }

    /// Apply functions to values with payload, altering them in place.
    ///
    /// This will alter all values, including center and non-center values. The
    /// two functions apply to the different values contained. Return None
    /// if input vector is not the same length.
    pub(crate) fn alter_values_zip<G, F, X, R>(
        &mut self,
        xs: Vec<X>,
        f: F,
        g: G,
    ) -> Result<Vec<R>, InputLengthError>
    where
        F: Fn(IndexedElement<&Shortname, &mut U>, X) -> R,
        G: Fn(IndexedElement<&K, &mut V>, X) -> R,
    {
        self.check_keys_length(&xs[..], true)?;
        Ok(self.alter_values_zip_nocheck(xs, f, g))
    }

    fn alter_values_zip_nocheck<G, F, X, R>(
        &mut self,
        xs: impl IntoIterator<Item = X>,
        f: F,
        g: G,
    ) -> Vec<R>
    where
        F: Fn(IndexedElement<&Shortname, &mut U>, X) -> R,
        G: Fn(IndexedElement<&K, &mut V>, X) -> R,
    {
        let nleft = self.left.len();
        let mut it = xs.into_iter();
        let mut ret: Vec<_> =
            Self::alter_paired_vec(&mut self.left, it.by_ref().take(nleft), 0, &g).collect();
        if let Some(r) = self.center_right.as_mut() {
            let nright = r.right.len();
            let c = &mut r.center;
            let center_r = f(
                IndexedElement::new(nleft.into(), &c.key, &mut c.value),
                it.next().expect("length was checked above"),
            );
            let right_r =
                Self::alter_paired_vec(&mut r.right, it.by_ref().take(nright), 1 + nleft, &g);
            ret.push(center_r);
            ret.extend(right_r);
        }
        ret
    }

    pub(crate) fn alter_elements_zip<Fnoncenter, Fcenter, Ferror, X, Y, R, E, G>(
        &mut self,
        xs: Vec<Element<X, Y>>,
        g: G,
        f_noncenter: Fnoncenter,
        f_center: Fcenter,
        f_error: Ferror,
    ) -> Result<Vec<R>, SetElementsError<ErrorGroup<E, G>>>
    where
        Fnoncenter: Fn(IndexedElement<&K, &mut V>, Y) -> R,
        Fcenter: Fn(IndexedElement<&Shortname, &mut U>, X) -> R,
        Ferror: Fn(MeasIndex, bool) -> E,
    {
        let go = |zs, ys, offset| Self::alter_paired_vec(zs, ys, offset, &f_noncenter);

        let check_optical = |ys: Vec<Element<X, Y>>, offset: usize| {
            ys.into_iter()
                .enumerate()
                .map(|(i, x)| x.both(|_| ErrorsResult::new_err(i), ErrorsResult::new_ok))
                .sequence_commutative()
                // TODO make wrapper for bools like this
                .map_errors(|i| f_error((i + offset).into(), true))
        };

        let nleft = self.left.len();
        let mut it = xs.into_iter();
        let xs_left = it.by_ref().take(nleft).collect();
        let left_res = check_optical(xs_left, 0);

        let res = if let Some(r) = self.center_right.as_mut() {
            let x_center = it.by_ref().next().expect("length was checked above");
            let xs_right = it.collect();
            let center_res = x_center
                .center()
                .ok_or(f_error(nleft.into(), false))
                .into_log();
            let right_res = check_optical(xs_right, nleft + 1);
            left_res
                .zip3_commutative(center_res, right_res)
                .map_ok_value(|(ys_left, y_center, ys_right)| {
                    let left_out = go(&mut self.left, ys_left, 0);
                    let c = &mut r.center;
                    let center_index = IndexedElement::new(nleft.into(), &c.key, &mut c.value);
                    let center_out = f_center(center_index, y_center);
                    let right_out = go(&mut r.right, ys_right, 1 + nleft);
                    left_out.chain([center_out]).chain(right_out).collect()
                })
        } else {
            left_res.map_ok_value(|ys| go(&mut self.left, ys, 0).collect())
        };

        res.group_with(g)
            .resolve_nowarn()
            .map_err(SetElementsError::Mismatch)
    }

    /// Apply function(s) to all values, altering them in place.
    pub(crate) fn alter_values<F, G, R>(&mut self, f: F, g: G) -> Vec<R>
    where
        F: Fn(IndexedElement<&Shortname, &mut U>) -> R,
        G: Fn(IndexedElement<&K, &mut V>) -> R,
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
        self.center_right.is_some().then(|| self.left.len().into())
    }

    /// Apply function over center value, possibly changing it's type
    #[allow(clippy::type_complexity)]
    pub(crate) fn map_center_value<F, Uf, P, LWC, RWC, E, EC>(
        self,
        f: F,
    ) -> LogResult<NamedVec<K, Uf, V>, P, LWC, RWC, (), E, EC>
    where
        F: Fn(IndexedElement<&Shortname, U>) -> LogResult<Uf, P, LWC, RWC, (), E, EC>,
        EC: Functor<E>,
        LWC: Default,
    {
        if let Some(r) = self.center_right {
            let c = r.center;
            let index = self.left.len().into();
            let ckey = c.key;
            let e = IndexedElement::new(index, &ckey, c.value);
            f(e).map_ok_value(|value| {
                NamedVec::new_split(self.left, Pair::new(ckey, value), r.right)
            })
        } else {
            LogResult::new_ok(NamedVec::new_unsplit(self.left))
        }
    }

    /// Apply function over non-center values, possibly changing their type
    #[allow(clippy::type_complexity)]
    pub(crate) fn map_non_center_values<F, Vf, WC, E, EC>(
        self,
        f: F,
    ) -> CommutativeResult<NamedVec<K, U, Vf>, (), WC, E, EC>
    where
        F: Fn(MeasIndex, V) -> CommutativeResult<Vf, (), WC, E, EC>,
        WC: Monoid,
        EC: Functor<E> + IntoIterator<Item = E> + Extend<E>,
    {
        let go = |xs: PairedVec<K, V>, offset: usize| {
            xs.into_iter()
                .enumerate()
                .map(|(i, p)| {
                    let j = i + offset;
                    f(j.into(), p.value).map_ok_value(|value| Pair::new(p.key, value))
                })
                .sequence_commutative()
        };
        if let Some(r) = self.center_right {
            let nleft = self.left.len();
            let lres = go(self.left, 0);
            let rres = go(r.right, nleft + 1);
            lres.zip_commutative(rres)
                .map_ok_value(|(left, right)| NamedVec::new_split(left, r.center, right))
        } else {
            go(self.left, 0).map_ok_value(|left| NamedVec::new_unsplit(left))
        }
    }

    /// Return number of all elements.
    pub(crate) fn len(&self) -> usize {
        self.iter().count()
    }

    /// Return number of non-center elements.
    pub(crate) fn len_non_center(&self) -> usize {
        self.iter()
            .filter(|e| matches!(e, Element::NonCenter(_)))
            .count()
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
    ) -> Result<Element<(&Shortname, &U), (&K, &V)>, ElementIndexError> {
        let i = self.check_element_index(index, true)?;
        let l = &self.left;
        let r = &self.center_right.as_ref();
        let left_len = l.len();
        let ret = match i.cmp(&left_len) {
            Less => Element::NonCenter(&l[i]),
            Equal => Element::Center(&r.expect("index was checked").center),
            Greater => Element::NonCenter(&r.expect("index was checked").right[i - left_len - 1]),
        };
        Ok(ret.bimap_once(|p| (&p.key, &p.value), |p| (&p.key, &p.value)))
    }

    /// Get reference with name.
    pub fn get_name(&self, n: &Shortname) -> Result<(MeasIndex, Element<&U, &V>), NameNotFoundError>
    where
        K: MightHave<Shortname>,
    {
        self.iter()
            .enumerate()
            .find_map(|(i, e)| {
                let x = e.as_ref();
                x.both(
                    |t| &t.key == n,
                    |o| o.key.as_opt().is_some_and(|kn| kn == n),
                )
                .then_some((i.into(), e.bimap_once(|p| &p.value, |p| &p.value)))
            })
            .ok_or_else(|| NameNotFoundError(n.clone()))
    }

    // /// Get mutable reference at position.
    // #[allow(clippy::type_complexity)]
    // pub fn get_mut(
    //     &mut self,
    //     index: MeasIndex,
    // ) -> Result<Element<(&Shortname, &mut U), (&K, &mut V)>, ElementIndexError>
    // {
    //     let i = self.check_element_index(index, true)?;
    //     match self {
    //         Self::Split(s) => {
    //             let left_len = s.left.len();
    //             match i.cmp(&left_len) {
    //                 Less => Ok(Element::NonCenter(&mut s.left[i])),
    //                 Equal => Ok(Element::Center(&mut s.center)),
    //                 Greater => Ok(Element::NonCenter(&mut s.left[i - left_len - 1])),
    //             }
    //         }
    //         Self::Unsplit(u) => Ok(Element::NonCenter(&mut u.members[i])),
    //     }
    //     .map(|x| x.bimap(|p| (&p.key, &mut p.value), |p| (&p.key, &mut p.value)))
    // }

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
    //         Self::Split(s) => {
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
    //         Self::Unsplit(u) => Self::value_by_name_mut(&mut u.members, n)
    //             .map(|(i, p)| (i.into(), Element::NonCenter(p))),
    //     }
    // }

    /// Check if new name can be pushed
    pub(crate) fn check_push<'a>(&self, name: &'a K) -> Result<Cow<'a, Shortname>, NamePresentError>
    where
        K: MightHave<Shortname>,
    {
        let index = self.len().into();
        self.check_key(name, index)
    }

    /// Check if new name can be pushed
    pub(crate) fn check_insert<'a>(
        &self,
        index: MeasIndex,
        name: &'a K,
    ) -> ErrorsResult<Cow<'a, Shortname>, (), InsertError>
    where
        K: MightHave<Shortname>,
    {
        let a = self.check_boundary_index(index).map_err(InsertError::from);
        let b = self.check_key(name, index).map_err(InsertError::from);
        a.zip(b).map_ok_value(|((), k)| k).set_err_value(())
    }

    /// Add a new non-center element at the end of the vector.
    ///
    /// Does not guarantee keys are unique.
    pub(crate) fn push_nocheck(&mut self, key: K, value: V)
    where
        K: MightHave<Shortname>,
    {
        debug_assert!(self.check_push(&key).is_ok(), "Name is not unique");
        let p = Pair::new(key, value);
        if let Some(r) = self.center_right.as_mut() {
            r.right.push(p);
        } else {
            self.left.push(p);
        }
    }

    /// Insert a new non-center element at a given position.
    ///
    /// Will panic if index is out of bounds. Does not guarantee keys are unique.
    pub(crate) fn insert_nocheck(&mut self, index: MeasIndex, key: K, value: V)
    where
        K: MightHave<Shortname>,
    {
        // only check key here because index will panic if out of bounds
        debug_assert!(self.check_key(&key, index).is_ok(), "Name is not unique");
        let i = usize::from(index);
        let p = Pair::new(key, value);
        let ln = self.left.len();
        match i.cmp(&ln) {
            Less | Equal => self.left.insert(i, p),
            Greater => {
                let r = self.center_right.as_mut().expect("no center/right present");
                r.right.insert(i - ln - 1, p);
            }
        }
    }

    /// Replace a non-center value with a new value at given position.
    ///
    /// Return value that was replaced.
    ///
    /// Return error if index is out of bounds. If index points to the center,
    /// convert it to a non-center value.
    pub(crate) fn replace_at(
        &mut self,
        index: MeasIndex,
        value: V,
    ) -> Result<Element<U, V>, ElementIndexError>
    where
        K: Pointed<Shortname>,
    {
        let _ = self.check_element_index(index, true)?;
        Ok(self.replace_at_nocheck(index, value))
    }

    fn replace_at_nocheck(&mut self, index: MeasIndex, value: V) -> Element<U, V>
    where
        K: Pointed<Shortname>,
    {
        let i = usize::from(index);
        let ln = self.left.len();
        match i.cmp(&ln) {
            Less => Element::NonCenter(mem::replace(&mut self.left[i].value, value)),
            Equal => {
                let r = mem::take(&mut self.center_right).expect("index out of bounds");
                let key = K::wrap(r.center.key);
                self.left.push(Pair::new(key, value));
                self.left.extend(r.right);
                Element::Center(r.center.value)
            }
            Greater => {
                let r = self.center_right.as_mut().expect("index out of bounds");
                let ret = mem::replace(&mut r.right[i - ln - 1].value, value);
                Element::NonCenter(ret)
            }
        }
    }

    /// Replace a value with a new value with a given name.
    ///
    /// Return value that was replaced.
    ///
    /// Return none if name is not present.
    pub(crate) fn replace_named(
        &mut self,
        name: &Shortname,
        value: V,
    ) -> Result<Element<U, V>, NameNotFoundError>
    where
        K: MightHave<Shortname>,
    {
        let index = self.find_with_name(name)?;
        Ok(self.replace_at_nocheck(index, value))
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
        key: K,
    ) -> Result<(Shortname, Shortname), RenameError>
    where
        K: MightHave<Shortname>,
    {
        let i = self
            .check_element_index(index, true)
            .map_err(RenameError::Index)?;
        let k = to_opt_or_indexed(key.as_opt(), index);
        if self
            .iter_all_names()
            .enumerate()
            .any(|(j, n)| j != i && n == k)
        {
            return Err(RenameError::NonUnique(NamePresentError { name: k }));
        }
        let ln = self.left.len();
        let old = match i.cmp(&ln) {
            Less => mem::replace(&mut self.left[i].key, key),
            Equal => {
                let ck = &mut self
                    .center_right
                    .as_mut()
                    .expect("index was checked")
                    .center
                    .key;
                K::wrap(mem::replace(ck, k.clone()))
            }
            Greater => {
                let rk = &mut self.center_right.as_mut().expect("index was checked").right
                    [i - ln - 1]
                    .key;
                mem::replace(rk, key)
            }
        };
        let old_k = to_opt_or_indexed(old.as_opt(), index);
        Ok((old_k, k))
    }

    /// Rename center element.
    ///
    /// Return previous name if center exists.
    pub(crate) fn rename_center(&mut self, name: Shortname) -> Option<Shortname> {
        Some(mem::replace(
            &mut self.center_right.as_mut()?.center.key,
            name,
        ))
    }

    /// Test if new center with name can be pushed
    pub(crate) fn check_push_center(
        &self,
        name: &Shortname,
    ) -> ErrorsResult<(), (), PushCenterError>
    where
        K: MightHave<Shortname>,
    {
        let a = self.check_name(name).map_err(PushCenterError::from);
        let b = self
            .center_right
            .is_some()
            .then_some(CenterPresentError.into())
            .map_or(Ok(()), Err);
        a.zip(b).set_ok_value(())
    }

    /// Test if new center with name can be inserted at index
    pub(crate) fn check_insert_center(
        &self,
        index: MeasIndex,
        name: &Shortname,
    ) -> ErrorsResult<(), (), InsertCenterError>
    where
        K: MightHave<Shortname>,
    {
        let a = self
            .check_push_center(name)
            .map_errors(InsertCenterError::from);
        let b = self
            .check_boundary_index(index)
            .map_err(InsertCenterError::from)
            .into_nowarn();
        a.zip_commutative(b).set_ok_value(())
    }

    /// Push a new center element to the end of the vector
    ///
    /// Will noop if center is already present and will not guarantee
    /// name uniqueness.
    pub(crate) fn push_center_nocheck(&mut self, name: Shortname, value: U)
    where
        K: MightHave<Shortname>,
    {
        debug_assert!(self.check_name(&name).is_ok(), "Name is not unique");
        debug_assert!(self.center_right.is_none(), "Center already present");
        self.center_right = Some(CenterRightVec::new(Pair::new(name, value), vec![]));
    }

    /// Insert a new center element at a given position.
    ///
    /// Will noop if center already exists, will not guarantee name uniqueness,
    /// and will silently truncate any indices that are over length of vector.
    pub(crate) fn insert_center_nocheck(&mut self, index: MeasIndex, name: Shortname, value: U)
    where
        K: MightHave<Shortname>,
    {
        debug_assert!(self.check_name(&name).is_ok(), "Name is not unique");
        debug_assert!(self.center_right.is_none(), "Center already present");
        let i = usize::from(index);
        debug_assert!(i <= self.len(), "Index is out of bounds");
        let p = Pair::new(name, value);
        self.center_right = Some(CenterRightVec::new(p, self.left.split_off(i)));
    }

    /// Remove key/value pair by name.
    pub(crate) fn remove_index(
        &mut self,
        index: MeasIndex,
    ) -> Result<EitherPair<K, U, V>, ElementIndexError> {
        let i = self.check_element_index(index, true)?;
        let nleft = self.left.len();
        let ret = match i.cmp(&nleft) {
            Less => Element::NonCenter(self.left.remove(i)),
            Equal => {
                let r = mem::take(&mut self.center_right).expect("index was checked");
                self.left.extend(r.right);
                Element::Center(r.center)
            }
            Greater => {
                let r = self.center_right.as_mut().expect("index was checked");
                Element::NonCenter(r.right.remove(i - nleft - 1))
            }
        };
        Ok(ret)
    }

    /// Remove key/value pair by name of key.
    ///
    /// Return error if name not found.
    pub(crate) fn remove_name(
        &mut self,
        n: &Shortname,
    ) -> Result<(MeasIndex, Element<U, V>), NameNotFoundError>
    where
        K: MightHave<Shortname>,
    {
        let go = |xs: &mut Vec<_>| {
            let i = Self::position_by_name(xs, n)?;
            let p = xs.remove(i);
            Ok((i.into(), p.value))
        };

        match go(&mut self.left) {
            Ok((i, x)) => Ok((i, Element::NonCenter(x))),
            Err(e) => {
                if let Some(mut r) = mem::take(&mut self.center_right) {
                    if &r.center.key == n {
                        // if name matches center, return center and extend the
                        // left vector with the right vector
                        self.left.extend(r.right);
                        let i = self.left.len().into();
                        Ok((i, Element::Center(r.center.value)))
                    } else {
                        // if name matches in right vector, remove from right
                        // and put center+right back on tail of parent struct
                        let res = go(&mut r.right);
                        self.center_right = Some(r);
                        res.map(|(i, x)| (i, Element::NonCenter(x)))
                    }
                } else {
                    Err(e)
                }
            }
        }
    }

    /// Set keys to list
    ///
    /// If center key does not exist, return an error.
    ///
    /// List must be the same length as all non-center keys and must be unique
    /// (including the center key).
    pub(crate) fn set_keys(&mut self, ks: Vec<K>) -> Result<NameMapping, SetKeysError>
    where
        K: Clone + MightHave<Shortname>,
    {
        self.check_keys_length(&ks[..], true)
            .map_err(SetNamesError::Length)?;
        if !all_unique_names(ks.iter().map(MightHave::as_opt)) {
            return Err(SetNamesError::NonUnique(NonUniqueKeysError).into());
        }
        let mut mapping = HashMap::new();
        let mut go = |side: &mut PairedVec<K, V>, ks_side: Vec<K>| {
            for (p, k) in side.iter_mut().zip(ks_side) {
                let old = mem::replace(&mut p.key, k.clone());
                if let (Some(old_name), Some(new_name)) = (K::to_opt(old), K::to_opt(k)) {
                    mapping.insert(old_name, new_name);
                }
            }
        };
        let mut it = ks.into_iter();
        let ks_left = it.by_ref().take(self.left.len()).collect();

        if let Some(r) = self.center_right.as_mut() {
            let center = it.by_ref().next().expect("length was checked above");
            let ks_right = it.collect();
            if let Some(center_name) = K::to_opt(center) {
                go(&mut self.left, ks_left);
                r.center.key = center_name;
                go(&mut r.right, ks_right);
            } else {
                return Err(MissingCenterError.into());
            }
        } else {
            go(&mut self.left, ks_left);
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
    //     ks: Vec<K>,
    // ) -> Result<NameMapping, SetNamesError>
    // where
    //     K: Clone,
    // {
    //     self.check_keys_length(&ks[..], false)
    //         .map_err(SetNamesError::Length)?;
    //     let center = self.as_center().map(|x| K::wrap(x.key));
    //     let all_keys = ks.iter().map(K::as_ref).chain(center).collect();
    //     if !self.as_prefix().all_unique::<K>(all_keys) {
    //         return Err(SetNamesError::NonUnique);
    //     }
    //     let mut mapping = HashMap::new();
    //     let mut go = |side: &mut PairedVec<K, V>, ks_side: Vec<K>| {
    //         for (p, k) in side.iter_mut().zip(ks_side) {
    //             let old = mem::replace(&mut p.key, k.clone());
    //             if let (Some(old_name), Some(new_name)) = (K::to_opt(old), K::to_opt(k)) {
    //                 mapping.insert(old_name, new_name);
    //             }
    //         }
    //     };
    //     match self {
    //         Self::Split(s) => {
    //             let mut ks_left = ks;
    //             let ks_right = ks_left.split_off(s.left.len());
    //             go(&mut s.left, ks_left);
    //             go(&mut s.right, ks_right);
    //         }
    //         Self::Unsplit(u) => go(&mut u.members, ks),
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
    pub(crate) fn set_names(&mut self, ns: Vec<Shortname>) -> Result<NameMapping, SetNamesError>
    where
        K: MightHave<Shortname>,
    {
        self.check_keys_length(&ns[..], true)
            .map_err(SetNamesError::Length)?;
        if !all_unique(&ns) {
            return Err(NonUniqueKeysError.into());
        }
        let mut mapping = HashMap::new();
        let mut go = |side: &mut PairedVec<K, V>, ns_side: Vec<Shortname>| {
            for (p, n) in side.iter_mut().zip(ns_side) {
                let old = mem::replace(&mut p.key, K::wrap(n.clone()));
                if let Some(old_name) = old.to_opt() {
                    mapping.insert(old_name, n);
                }
            }
        };
        let mut it = ns.into_iter();
        let ns_left = it.by_ref().take(self.left.len()).collect();
        go(&mut self.left, ns_left);
        if let Some(r) = self.center_right.as_mut() {
            let n_center = it.next().expect("length was checked above");
            let ns_right = it.collect();
            go(&mut r.right, ns_right);
            let old = mem::replace(&mut r.center.key, n_center.clone());
            mapping.insert(old, n_center);
        }
        Ok(mapping)
    }

    /// Replace any value with a center value with name.
    pub(crate) fn replace_center_by_name<F, LWC, RWC, E, EC>(
        &mut self,
        n: &Shortname,
        value: U,
        to_v: F,
    ) -> LogResult<Element<U, V>, (), LWC, RWC, (), E, EC>
    where
        F: FnOnce(MeasIndex, U) -> LogResult<V, U, LWC, RWC, (), E, EC>,
        E: From<NameNotFoundError>,
        EC: Default,
        LWC: Default,
        RWC: Default,
        K: MightHave<Shortname>,
    {
        self.find_with_name(n)
            .map_err(E::from)
            .into_log()
            .nowarn_and_then(|index| self.replace_center_at_inner(index, value, to_v))
    }

    /// Replace any value with a center value with name.
    pub(crate) fn replace_center_by_name_nofail<F>(
        &mut self,
        n: &Shortname,
        value: U,
        to_v: F,
    ) -> Result<Element<U, V>, NameNotFoundError>
    where
        F: FnOnce(MeasIndex, U) -> V,
        K: MightHave<Shortname>,
    {
        let index = self.find_with_name(n)?;
        let res = self.replace_center_at_nofail(index, value, to_v);
        Ok(res.expect("index was checked"))
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
    pub(crate) fn replace_center_at<F, LWC, RWC, E, EC>(
        &mut self,
        index: MeasIndex,
        value: U,
        to_v: F,
    ) -> LogResult<Element<U, V>, (), LWC, RWC, (), E, EC>
    where
        F: FnOnce(MeasIndex, U) -> LogResult<V, U, LWC, RWC, (), E, EC>,
        E: From<SetCenterError>,
        LWC: Default,
        RWC: Default,
        EC: Default,
        K: MightHave<Shortname>,
    {
        self.get_index_if_named(index)
            .map_err(E::from)
            .into_log()
            .nowarn_and_then(|i| self.replace_center_at_inner(i.into(), value, to_v))
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
        K: MightHave<Shortname>,
    {
        let go = |j, u| ErrorResult::<_, _, Infallible>::new_ok(to_v(j, u));

        self.get_index_if_named(index).map(|i| {
            let res = self.replace_center_at_inner(i.into(), value, go);
            res.infallible_nowarn_into()
        })
    }

    fn alter_paired_vec<X, F, R>(
        xs: &mut PairedVec<K, V>,
        ys: impl IntoIterator<Item = X>,
        offset: usize,
        f: &F,
    ) -> impl Iterator<Item = R>
    where
        F: Fn(IndexedElement<&K, &mut V>, X) -> R,
    {
        // ASSUME both xs and ys are the same length
        xs.iter_mut().zip(ys).zip(offset..).map(|((y, x), i)| {
            let e = IndexedElement::new(i.into(), &y.key, &mut y.value);
            f(e, x)
        })
    }

    fn replace_center_at_inner<F, LWC, RWC, E, EC>(
        &mut self,
        index: MeasIndex,
        value: U,
        to_v: F,
    ) -> LogResult<Element<U, V>, (), LWC, RWC, (), E, EC>
    where
        F: FnOnce(MeasIndex, U) -> LogResult<V, U, LWC, RWC, (), E, EC>,
        LWC: Default,
        K: MightHave<Shortname>,
    {
        let res = match mem::take(self).split_at_index(index.into()) {
            Err(s) => match s {
                PartialSplit::Left(split) => to_v(index, split.center_value)
                    .inject_value((split.stable, split.selected_left_value))
                    .map_ok_value(|(new_right_val, (stable, old_left_val))| {
                        let sp = Self::new_split_from_left(value, new_right_val, stable);
                        (sp, Element::NonCenter(old_left_val))
                    })
                    .map_err_value(|(center_val, (stable, old_left_val))| {
                        Self::recover_split_from_left(old_left_val, center_val, stable)
                    }),

                PartialSplit::Center(x) => {
                    let center = Pair::new(x.center.key, value);
                    let sp = Self::new_split(x.left, center, x.right);
                    LogResult::new_ok((sp, Element::Center(x.center.value)))
                }

                PartialSplit::Right(split) => to_v(index, split.center_value)
                    .inject_value((split.stable, split.selected_right_value))
                    .map_ok_value(|(new_left_val, (stable, old_right_val))| {
                        let sp = Self::new_split_from_right(value, new_left_val, stable);
                        (sp, Element::NonCenter(old_right_val))
                    })
                    .map_err_value(|(center_val, (stable, old_right_val))| {
                        Self::recover_split_from_right(old_right_val, center_val, stable)
                    }),
            },

            Ok(u) => {
                let center = Pair::new(u.selected_name, value);
                let sp = Self::new_split(u.left, center, u.right);
                LogResult::new_ok((sp, Element::NonCenter(u.selected_value)))
            }
        };

        res.map_ok_value(|(newself, ret)| {
            *self = newself;
            ret
        })
        .map_err_value(|newself| *self = newself)
    }

    /// Set center to be the element with name if it exists.
    pub(crate) fn set_center_by_name<Fswap, FtoU, LWC, RWC, E, EC>(
        &mut self,
        n: &Shortname,
        swap: Fswap,
        to_u: FtoU,
    ) -> LogResult<bool, (), LWC, RWC, (), E, EC>
    where
        Fswap: FnOnce(
            (MeasIndex, U),
            (MeasIndex, V),
        ) -> LogResult<(V, U), (U, V), LWC, RWC, (), E, EC>,
        FtoU: FnOnce(MeasIndex, V) -> LogResult<U, V, LWC, RWC, (), E, EC>,
        E: From<NameNotFoundError>,
        EC: Default,
        LWC: Default,
        RWC: Default,
        K: MightHave<Shortname>,
    {
        self.find_with_name(n)
            .map_err(E::from)
            .into_log()
            .nowarn_and_then(|index| self.set_center_by_index_inner(index.into(), swap, to_u))
    }

    /// Set center to be the element with index if it exists.
    pub(crate) fn set_center_by_index<Fswap, FtoU, LWC, RWC, E, EC>(
        &mut self,
        index: MeasIndex,
        swap: Fswap,
        to_u: FtoU,
    ) -> LogResult<bool, (), LWC, RWC, (), E, EC>
    where
        Fswap: FnOnce(
            (MeasIndex, U),
            (MeasIndex, V),
        ) -> LogResult<(V, U), (U, V), LWC, RWC, (), E, EC>,
        FtoU: FnOnce(MeasIndex, V) -> LogResult<U, V, LWC, RWC, (), E, EC>,
        E: From<SetCenterError>,
        EC: Default,
        RWC: Default,
        LWC: Default,
        K: MightHave<Shortname>,
    {
        self.get_index_if_named(index)
            .map_err(E::from)
            .into_log()
            .nowarn_and_then(|i| self.set_center_by_index_inner(i, swap, to_u))
    }

    fn set_center_by_index_inner<Fswap, FtoU, LWC, RWC, E, EC>(
        &mut self,
        index: usize,
        swap: Fswap,
        to_u: FtoU,
    ) -> LogResult<bool, (), LWC, RWC, (), E, EC>
    where
        Fswap: FnOnce(
            (MeasIndex, U),
            (MeasIndex, V),
        ) -> LogResult<(V, U), (U, V), LWC, RWC, (), E, EC>,
        FtoU: FnOnce(MeasIndex, V) -> LogResult<U, V, LWC, RWC, (), E, EC>,
        EC: Default,
        RWC: Default,
        LWC: Default,
        K: MightHave<Shortname>,
    {
        // ASSUME index is valid
        let j = index.into();
        let res = match mem::take(self).split_at_index(index) {
            Err(s) => match s {
                PartialSplit::Left(split) => {
                    let old_center = (split.stable.center_index, split.center_value);
                    let new_center = (j, split.selected_left_value);
                    swap(old_center, new_center)
                        .inject_value(split.stable)
                        .map_ok_value(|((new_right_val, new_center_val), stable)| {
                            let sp =
                                Self::new_split_from_left(new_center_val, new_right_val, stable);
                            (sp, true)
                        })
                        .map_err_value(|((old_center_val, old_left_val), stable)| {
                            Self::recover_split_from_left(old_left_val, old_center_val, stable)
                        })
                }

                PartialSplit::Center(sc) => LogResult::new_ok((sc.into(), false)),

                PartialSplit::Right(split) => {
                    let old_center = (split.stable.center_index, split.center_value);
                    let new_center = (j, split.selected_right_value);
                    swap(old_center, new_center)
                        .inject_value(split.stable)
                        .map_ok_value(|((new_right_val, new_center_val), stable)| {
                            let sp =
                                Self::new_split_from_right(new_center_val, new_right_val, stable);
                            (sp, true)
                        })
                        .map_err_value(|((old_center_val, old_right_val), stable)| {
                            Self::recover_split_from_right(old_right_val, old_center_val, stable)
                        })
                }
            },

            Ok(u) => to_u(j, u.selected_value)
                .inject_value((u.left, u.selected_name, u.right))
                .map_ok_value(|(new_value, (left, name, right))| {
                    let center = Pair::new(name, new_value);
                    (Self::new_split(left, center, right), true)
                })
                .map_err_value(|(old_value, (left, name, right))| {
                    let center = Pair::new(K::wrap(name), old_value);
                    let new = left.into_iter().chain([center]).chain(right).collect();
                    Self::new_unsplit(new)
                }),
        };
        res.map_ok_value(|(newself, flag)| {
            *self = newself;
            flag
        })
        .map_err_value(|newself| *self = newself)
    }

    /// Convert the center element into a non-center element.
    ///
    /// Has no effect if there already is no center element.
    ///
    /// Return old center element if vector is updated.
    pub(crate) fn unset_center<F, X, LWC, RWC, E, EC>(
        &mut self,
        to_v: F,
    ) -> LogResult<Option<X>, (), LWC, RWC, (), E, EC>
    where
        F: FnOnce(MeasIndex, U) -> LogResult<(V, X), U, LWC, RWC, (), E, EC>,
        LWC: Default,
        K: Pointed<Shortname>,
    {
        if let Some(r) = mem::take(&mut self.center_right) {
            let index = self.left.len().into();
            to_v(index, r.center.value)
                .inject_value((r.center.key, r.right))
                .map_ok_value(|((value, ret), (center_key, right))| {
                    let non_center = Pair::new(K::wrap(center_key), value);
                    self.left.push(non_center);
                    self.left.extend(right);
                    Some(ret)
                })
                .map_err_value(|(value, (center_key, right))| {
                    let center = Pair::new(center_key, value);
                    self.center_right = Some(CenterRightVec::new(center, right));
                })
        } else {
            LogResult::new_ok(None)
        }
    }

    /// Unwrap and rewrap the non-center names of vector.
    ///
    /// This may fail if the original wrapped name cannot be converted.
    #[allow(clippy::type_complexity)]
    pub(crate) fn try_rewrapped<J, E, F>(self, f: F) -> ErrorsResult<NamedVec<J, U, V>, (), E>
    where
        F: Fn(MeasIndex, K) -> Result<J, E>,
    {
        let try_go = |i: usize, p: Pair<K, V>| Ok(Pair::new(f(i.into(), p.key)?, p.value));
        let go = |xs: PairedVec<K, V>, offset: usize| {
            xs.into_iter()
                .enumerate()
                .map(|(i, p)| try_go(i + offset, p).into_nowarn())
                .sequence_commutative()
        };

        if let Some(r) = self.center_right {
            let offset = self.left.len() + 1;
            let lres = go(self.left, 0);
            let rres = go(r.right, offset);
            lres.zip_commutative(rres)
                .map_ok_value(|(left, right)| NamedVec::new_split(left, r.center, right))
        } else {
            go(self.left, 0).map_ok_value(NamedVec::new_unsplit)
        }
    }

    // #[allow(clippy::type_complexity)]
    // fn try_into_wrapper<J>(p: Pair<K, V>) -> Result<Pair<J, V>, <J as TryFrom<K>>::Error>
    // where
    //     J: TryFrom<K>,
    // {
    //     Ok(Pair::new(p.key.try_into()?, p.value))
    // }

    // fn from_center(p: Center<U>) -> Pair<K, V>
    // where
    //     V: From<U>,
    // {
    //     Pair {
    //         key: K::wrap(p.key),
    //         value: p.value.into(),
    //     }
    // }

    fn position_by_name(xs: &PairedVec<K, V>, n: &Shortname) -> Result<usize, NameNotFoundError>
    where
        K: MightHave<Shortname>,
    {
        xs.iter()
            .position(|p| p.key.as_opt().is_some_and(|kn| kn == n))
            .ok_or(NameNotFoundError(n.to_owned()))
    }

    // fn value_by_name_mut<'a>(
    //     xs: &'a mut PairedVec<K, V>,
    //     n: &Shortname,
    // ) -> Option<(usize, &'a mut V)> {
    //     xs.iter_mut()
    //         .enumerate()
    //         .find(|(_, p)| K::as_opt(&p.key).is_some_and(|kn| kn == n))
    //         .map(|(i, p)| (i, &mut p.value))
    // }

    fn check_key<'a>(
        &self,
        key: &'a K,
        index: MeasIndex,
    ) -> Result<Cow<'a, Shortname>, NamePresentError>
    where
        K: MightHave<Shortname>,
    {
        let name = key
            .as_opt()
            .map_or_else(|| Cow::Owned(Shortname::from(index)), Cow::Borrowed);
        if self.iter_all_names().any(|n| &n == name.as_ref()) {
            Err(NamePresentError::new(name.into_owned()))
        } else {
            Ok(name)
        }
    }

    fn check_name(&self, name: &Shortname) -> Result<(), NamePresentError>
    where
        K: MightHave<Shortname>,
    {
        if self.iter_all_names().any(|n| &n == name) {
            Err(NamePresentError::new(name.clone()))
        } else {
            Ok(())
        }
    }

    fn check_element_index(
        &self,
        index: MeasIndex,
        include_center: bool,
    ) -> Result<usize, ElementIndexError> {
        let len = self.len();
        IndexFromOne::from(index).check_index(len).map_or_else(
            |e| Err(ElementIndexError::new(e, None)),
            |i| {
                if let Some(j) = self.center_index()
                    && !include_center
                    && usize::from(j) == i
                {
                    let e = IndexError::new(i.into(), len);
                    return Err(ElementIndexError::new(e, Some(j)));
                }
                Ok(i)
            },
        )
    }

    fn get_index_if_named(&self, index: MeasIndex) -> Result<usize, SetCenterError>
    where
        K: MightHave<Shortname>,
    {
        let i = self
            .check_element_index(index, true)
            .map_err(SetCenterError::Index)?;
        let has_name = self
            .get(index)
            .expect("index was checked to be in bounds")
            .both(|_| true, |(n, _)| n.as_opt().is_some());
        if has_name {
            Ok(i)
        } else {
            Err(NoNameError.into())
        }
    }

    fn check_boundary_index(&self, index: MeasIndex) -> Result<(), BoundaryIndexError> {
        IndexFromOne::from(index).check_boundary_index(self.len())
    }

    fn check_keys_length<X>(&self, xs: &[X], include_center: bool) -> Result<(), InputLengthError> {
        let this_len = if include_center {
            self.len()
        } else {
            self.len_non_center()
        };
        let other_len = xs.len();
        if this_len != other_len {
            return Err(InputLengthError {
                this_len,
                other_len,
                include_center,
            });
        }
        Ok(())
    }

    fn find_with_name(&self, name: &Shortname) -> Result<MeasIndex, NameNotFoundError>
    where
        K: MightHave<Shortname>,
    {
        self.iter()
            .find_position(|x| {
                x.as_ref().both(
                    |l| &l.key == name,
                    |r| r.key.as_opt().is_some_and(|k| k == name),
                )
            })
            .map(|(i, _)| i.into())
            .ok_or(NameNotFoundError(name.to_owned()))
    }

    fn new_split(left: PairedVec<K, V>, center: Center<U>, right: PairedVec<K, V>) -> Self {
        Self::new(left, Some(CenterRightVec::new(center, right)))
    }

    fn new_split_from_left(
        new_center_value: U,
        new_right_value: V,
        stable: LeftSplitStable<K, V>,
    ) -> Self
    where
        K: MightHave<Shortname>,
    {
        let new_center = Pair::new(stable.selected_left_key, new_center_value);
        let new_right_pair = Pair::new(K::wrap(stable.center_key), new_right_value);
        let new_right = stable
            .left_right
            .into_iter()
            .chain([new_right_pair])
            .chain(stable.right)
            .collect();
        Self::new_split(stable.left_left, new_center, new_right)
    }

    fn recover_split_from_left(
        old_left_value: V,
        old_center_value: U,
        stable: LeftSplitStable<K, V>,
    ) -> Self
    where
        K: MightHave<Shortname>,
    {
        let center = Pair::new(stable.center_key, old_center_value);
        let new_left_value = Pair::new(K::wrap(stable.selected_left_key), old_left_value);
        let new_left = stable
            .left_left
            .into_iter()
            .chain([new_left_value])
            .chain(stable.left_right)
            .collect();
        Self::new_split(new_left, center, stable.right)
    }

    fn new_split_from_right(
        new_center_value: U,
        new_left_value: V,
        stable: RightSplitStable<K, V>,
    ) -> Self
    where
        K: MightHave<Shortname>,
    {
        let new_center = Pair::new(stable.selected_right_key, new_center_value);
        let new_left_pair = Pair::new(K::wrap(stable.center_key), new_left_value);
        let new_left = stable
            .left
            .into_iter()
            .chain([new_left_pair])
            .chain(stable.right_left)
            .collect();
        Self::new_split(new_left, new_center, stable.right_right)
    }

    fn recover_split_from_right(
        old_right_value: V,
        old_center_value: U,
        stable: RightSplitStable<K, V>,
    ) -> Self
    where
        K: MightHave<Shortname>,
    {
        let center = Pair::new(stable.center_key, old_center_value);
        let new_right_value = Pair::new(K::wrap(stable.selected_right_key), old_right_value);
        let new_right = stable
            .right_left
            .into_iter()
            .chain([new_right_value])
            .chain(stable.right_right)
            .collect();
        Self::new_split(stable.left, center, new_right)
    }

    fn new_unsplit(left: PairedVec<K, V>) -> Self {
        Self::new(left, None)
    }

    fn try_into_split(self) -> Result<PairedVec<K, V>, SplitVec<K, U, V>> {
        if let Some(r) = self.center_right {
            Err(SplitVec::new(self.left, r.center, r.right))
        } else {
            Ok(self.left)
        }
    }

    #[allow(
        clippy::result_large_err,
        reason = "this isn't really an error and doesn't propagate up the stack, \
                  so the imbalance is contained"
    )]
    fn split_at_index(self, index: usize) -> Result<PairedSplit<K, V>, PartialSplit<K, U, V>>
    where
        K: MightHave<Shortname>,
    {
        self.try_into_split()
            .map(|x| PairedSplit::from_paired(x, index).expect("index points to existing name"))
            .map_err(|x| {
                x.split_at_index(index)
                    .expect("index points to existing name")
            })
    }
}

// Implement methods for NamedSet

pub(crate) enum NamedSetMembership {
    Center,
    NonCenter,
    None,
}

impl NamedSet<'_> {
    pub(crate) fn membership(&self, name: &Shortname) -> NamedSetMembership {
        if self.contains_non_center_name(name) {
            NamedSetMembership::NonCenter
        } else if self.contains_center_name(name) {
            NamedSetMembership::Center
        } else {
            NamedSetMembership::None
        }
    }

    pub(crate) fn error_names<'a>(
        &self,
        names: impl IntoIterator<Item = &'a Shortname>,
    ) -> (Option<Shortname>, Option<NEVec<Shortname>>) {
        let mut t = None;
        let ns = names
            .into_iter()
            .filter(|&n| match self.membership(n) {
                NamedSetMembership::None => true,
                NamedSetMembership::Center => {
                    t = Some(n.clone());
                    false
                }
                NamedSetMembership::NonCenter => false,
            })
            .cloned()
            .try_into_nonempty_iter()
            .map(|n| n.collect());
        (t, ns)
    }

    pub(crate) fn invalid_link_errors<'a, T>(
        &self,
        names: impl IntoIterator<Item = &'a Shortname>,
    ) -> impl Iterator<Item = KeyToNameLinkError<T>> {
        let (t, o) = self.error_names(names);
        let te = t
            .map(TemporalNamedLinkError::new_i0)
            .map(KeyToNameLinkError::Temporal);
        let oe = o
            .map(OpticalNamedLinkError::new_i0)
            .map(KeyToNameLinkError::Optical);
        [te, oe].into_iter().flatten()
    }

    pub(crate) fn error_link_name<'a>(
        &self,
        names: impl IntoIterator<Item = &'a Shortname>,
    ) -> Option<LinkName> {
        let (t, o) = self.error_names(names);
        match o {
            None => t.map(LinkName::Temporal),
            Some(ns) => Some(LinkName::Both(ns, t)),
        }
    }

    pub(crate) fn contains_non_center_name(&self, name: &Shortname) -> bool {
        self.non_center.contains(name)
    }

    fn contains_center_name(&self, name: &Shortname) -> bool {
        self.center.as_ref().is_some_and(|n| n == &name)
    }
}

// Implement methods for Pair

impl_kind2!(pub PairFamily, Pair);

impl<A, B> BifunctorOnce<A, B> for Pair<A, B> {
    fn first_once<F: FnOnce(A) -> C, C>(self, f: F) -> Pair<C, B> {
        Pair::new(f(self.key), self.value)
    }

    fn second_once<F: FnOnce(B) -> C, C>(self, f: F) -> Pair<A, C> {
        Pair::new(self.key, f(self.value))
    }
}

// Implement methods for Element

impl<A, B> BifunctorOnce<A, B> for Element<A, B> {
    fn first_once<F: FnOnce(A) -> C, C>(self, f: F) -> Element<C, B> {
        match self {
            Self::Center(x) => Element::Center(f(x)),
            Self::NonCenter(x) => Element::NonCenter(x),
        }
    }

    fn second_once<F: FnOnce(B) -> C, C>(self, f: F) -> Element<A, C> {
        match self {
            Self::Center(x) => Element::Center(x),
            Self::NonCenter(x) => Element::NonCenter(f(x)),
        }
    }
}

impl<K, U, V> EitherPair<K, U, V> {
    pub fn unzip(self) -> (K, Element<U, V>)
    where
        K: Pointed<Shortname>,
    {
        self.both(
            |p| (K::wrap(p.key), Element::Center(p.value)),
            |p| (p.key, Element::NonCenter(p.value)),
        )
    }
}

impl<K, U, V> Either<K, U, V> {
    pub fn values_into<Uf, Vf>(self) -> Either<K, Uf, Vf>
    where
        U: Into<Uf>,
        V: Into<Vf>,
    {
        self.first_once(|(k, v)| (k, v.into()))
            .second_once(|(k, v)| (k, v.into()))
    }
}

impl<U, V> Element<U, V> {
    pub fn both<F, G, X>(self, f: F, g: G) -> X
    where
        F: FnOnce(U) -> X,
        G: FnOnce(V) -> X,
    {
        match self {
            Self::Center(u) => f(u),
            Self::NonCenter(v) => g(v),
        }
    }

    pub fn as_ref(&self) -> Element<&U, &V> {
        match self {
            Self::Center(u) => Element::Center(u),
            Self::NonCenter(v) => Element::NonCenter(v),
        }
    }

    pub fn as_mut(&mut self) -> Element<&mut U, &mut V> {
        match self {
            Self::Center(u) => Element::Center(u),
            Self::NonCenter(v) => Element::NonCenter(v),
        }
    }

    pub fn non_center(self) -> Option<V> {
        match self {
            Self::Center(_) => None,
            Self::NonCenter(v) => Some(v),
        }
    }

    pub fn center(self) -> Option<U> {
        match self {
            Self::Center(u) => Some(u),
            Self::NonCenter(_) => None,
        }
    }

    pub fn is_center(&self) -> bool {
        self.as_ref().center().is_some()
    }
}

impl<X> Element<X, X> {
    pub fn unwrap(self) -> X {
        self.both(|x| x, |y| y)
    }
}

// Implement data types and methods to replace elements
//
// We have some methods which involve replacing an element with a new center
// element and converting the old center element to a non-center element using
// a fallible function. Since this is easiest to do with owned values, we need
// a way of deconstructing the named vec to "split" counterparts which also
// can be easily reconstructed into the original vector upon failure.

#[derive(new)]
struct SplitVec<K, U, V> {
    left: PairedVec<K, V>,
    center: Center<U>,
    right: PairedVec<K, V>,
}

impl<K, U, V> From<SplitVec<K, U, V>> for NamedVec<K, U, V> {
    fn from(value: SplitVec<K, U, V>) -> Self {
        Self::new_split(value.left, value.center, value.right)
    }
}

enum PartialSplit<K, U, V> {
    Left(LeftSplit<K, U, V>),
    Center(SplitVec<K, U, V>),
    Right(RightSplit<K, U, V>),
}

#[derive(new)]
struct LeftSplit<K, U, V> {
    selected_left_value: V,
    center_value: U,
    stable: LeftSplitStable<K, V>,
}

#[derive(new)]
struct RightSplit<K, U, V> {
    selected_right_value: V,
    center_value: U,
    stable: RightSplitStable<K, V>,
}

#[derive(new)]
struct LeftSplitStable<K, V> {
    left_left: PairedVec<K, V>,
    selected_left_key: Shortname,
    left_right: PairedVec<K, V>,
    center_key: Shortname,
    center_index: MeasIndex,
    right: PairedVec<K, V>,
}

#[derive(new)]
struct RightSplitStable<K, V> {
    left: PairedVec<K, V>,
    center_key: Shortname,
    center_index: MeasIndex,
    right_left: PairedVec<K, V>,
    selected_right_key: Shortname,
    right_right: PairedVec<K, V>,
}

struct PairedSplit<K, V> {
    left: PairedVec<K, V>,
    selected_name: Shortname,
    selected_value: V,
    right: PairedVec<K, V>,
}

impl<K, U, V> SplitVec<K, U, V> {
    fn split_at_index(self, index: usize) -> Option<PartialSplit<K, U, V>>
    where
        K: MightHave<Shortname>,
    {
        let nleft = self.left.len();
        match index.cmp(&nleft) {
            Less => {
                let split_left = PairedSplit::from_paired(self.left, index)?;
                let stable = LeftSplitStable::new(
                    split_left.left,
                    split_left.selected_name,
                    split_left.right,
                    self.center.key,
                    nleft.into(),
                    self.right,
                );
                let split = LeftSplit::new(split_left.selected_value, self.center.value, stable);
                Some(PartialSplit::Left(split))
            }
            Equal => Some(PartialSplit::Center(self)),
            Greater => {
                let split_right = PairedSplit::from_paired(self.right, index)?;
                let stable = RightSplitStable::new(
                    self.left,
                    self.center.key,
                    nleft.into(),
                    split_right.left,
                    split_right.selected_name,
                    split_right.right,
                );
                let split = RightSplit::new(split_right.selected_value, self.center.value, stable);
                Some(PartialSplit::Right(split))
            }
        }
    }
}

impl<K, V> PairedSplit<K, V> {
    fn from_paired(xs: PairedVec<K, V>, index: usize) -> Option<Self>
    where
        K: MightHave<Shortname>,
    {
        let mut it = xs.into_iter();
        let left = it.by_ref().take(index).collect();
        let p = it.by_ref().next()?;
        Some(Self {
            left,
            selected_name: K::to_opt(p.key)?,
            selected_value: p.value,
            right: it.collect(),
        })
    }
}

// Misc functions

fn to_opt_or_indexed(x: Option<&Shortname>, i: MeasIndex) -> Shortname {
    x.cloned().unwrap_or(i.into())
}

pub(crate) fn all_unique_names<'a>(xs: impl IntoIterator<Item = Option<&'a Shortname>>) -> bool {
    all_unique(
        xs.into_iter()
            .enumerate()
            .map(|(i, x)| to_opt_or_indexed(x, i.into())),
    )
}

fn all_unique<'a, T: Hash + Eq>(xs: impl IntoIterator<Item = T> + 'a) -> bool {
    let mut seen = HashSet::new();
    xs.into_iter().all(|x| seen.insert(x))
}

#[cfg(feature = "python")]
mod python {
    use super::{Element, NonCenterElement};
    use pyo3::prelude::*;
    use pyo3::types::PyTuple;

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
}

use crate::core::{IndexedKeyLossError, UnitaryKeyLossError};
use crate::text::index::IndexFromOne;
use crate::validated::keys::Key1;

use type_families::{Monoid, Pointed, Semigroup, Sibling1, impl_functor_once, impl_kind1};

use derive_more::{AsMut, AsRef, From};
use std::fmt;
use std::iter;
use std::marker::PhantomData;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use pyo3::prelude::*;

/// A value that always exists.
#[derive(Clone, PartialEq, AsRef, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
pub struct Identity<T>(pub T);

impl<T> IntoIterator for Identity<T> {
    type Item = T;
    type IntoIter = iter::Once<T>;
    fn into_iter(self) -> Self::IntoIter {
        iter::once(self.0)
    }
}

impl_kind1!(pub IdFamily, Identity);

impl_functor_once!(Identity, self, mut f, Identity(f(self.0)));

impl<A> Pointed<A> for Identity<A> {
    fn wrap(a: A) -> Self {
        Self(a)
    }
}

/// A value that never exists.
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct Nothing<T>(pub PhantomData<T>);

impl<T> IntoIterator for Nothing<T> {
    type Item = T;
    type IntoIter = iter::Empty<T>;
    fn into_iter(self) -> Self::IntoIter {
        iter::empty()
    }
}

impl_kind1!(pub NullFamily, Nothing);

impl<T> Default for Nothing<T> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<A> Semigroup for Nothing<A> {
    fn sappend(self, _: Self) -> Self {
        Self::default()
    }
}

impl<X> Monoid for Nothing<X> {}

impl_functor_once!(Nothing, self, _f, Nothing::default());

impl<A> Pointed<A> for Nothing<A> {
    fn wrap(_: A) -> Self {
        Self::default()
    }
}

/// A value that can either have one value or be empty.
///
/// This is like a bool but the `true` value is meant to have a displayed value
/// associated with it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, AsRef, AsMut, From, Default)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "serde", serde(into = "bool"))]
#[cfg_attr(feature = "serde", serde(bound = "T: Clone"))]
pub struct OptionalZST<T>(pub Option<T>);

impl<T: Default> From<bool> for OptionalZST<T> {
    fn from(value: bool) -> Self {
        Self(value.then_some(T::default()))
    }
}

impl<T> From<OptionalZST<T>> for bool {
    fn from(value: OptionalZST<T>) -> Self {
        value.0.is_some()
    }
}

pub(crate) trait IsDefault {
    fn is_default(&self) -> bool;
}

impl<T: Default + PartialEq> IsDefault for T {
    fn is_default(&self) -> bool {
        self == &T::default()
    }
}

pub(crate) trait CheckMaybe: Sized + IsDefault {
    type Inner;

    fn root_key_loss_error<E>(&self) -> Option<E>
    where
        E: From<UnitaryKeyLossError<Self::Inner>>,
    {
        (!self.is_default()).then_some(UnitaryKeyLossError::<Self::Inner>::default().into())
    }

    fn indexed_key_loss_error<E>(&self, i: impl Into<IndexFromOne>) -> Option<E>
    where
        E: From<IndexedKeyLossError<Self::Inner>>,
    {
        let k = Key1::new_i1(i.into());
        (!self.is_default()).then_some(IndexedKeyLossError::<Self::Inner>(k).into())
    }
}

impl<T: fmt::Display + PartialEq> CheckMaybe for Option<T> {
    type Inner = T;
}

/// Encodes a type which might have something in it.
///
/// Intended to be used as a "type family" pattern.
pub trait MightHave<A>: Pointed<A> + Sized {
    /// If true, the wrapper will always have a value.
    ///
    /// Obviously, the implementation needs to ensure this is in sync with the
    /// meaning of [`Wrapper<T>`](type_families::Kind1::Type).
    const INFALLABLE: bool;

    /// Consume a wrapped value and possibly return its contents.
    ///
    /// If no contents exist, return the original input so the caller can
    /// take back ownership.
    fn unwrap(self) -> Result<A, Self>;

    /// Borrow a wrapped value and return a new wrapper with borrowed contents.
    fn as_ref(&self) -> Sibling1<Self, &A>;

    /// Consume a wrapped value and possibly return its contents.
    fn to_opt(self) -> Option<A> {
        self.unwrap().ok()
    }

    /// Borrow a wrapped value and possibly return borrowed contents.
    fn as_opt(&self) -> Option<&A>;
}

impl<A> MightHave<A> for Option<A> {
    const INFALLABLE: bool = true;

    fn unwrap(self) -> Result<A, Self> {
        self.ok_or(None)
    }

    fn as_ref(&self) -> Option<&A> {
        Self::as_ref(self)
    }

    fn as_opt(&self) -> Option<&A> {
        Self::as_ref(self)
    }
}

#[derive(Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct AlwaysFamily;

impl<A> MightHave<A> for Identity<A> {
    const INFALLABLE: bool = true;

    fn unwrap(self) -> Result<A, Self> {
        Ok(self.0)
    }

    fn as_ref(&self) -> Identity<&A> {
        Identity(&self.0)
    }

    fn as_opt(&self) -> Option<&A> {
        Some(&self.0)
    }
}

impl<T> From<T> for Identity<T> {
    fn from(value: T) -> Self {
        Self(value)
    }
}

impl<T> From<Identity<T>> for Option<T> {
    fn from(value: Identity<T>) -> Self {
        Some(value.0)
    }
}

#[cfg(feature = "python")]
mod python {
    use super::{Identity, OptionalZST};

    use pyo3::prelude::*;
    use pyo3::types::PyBool;
    use std::convert::Infallible;

    impl<'py, T: FromPyObject<'py>> FromPyObject<'py> for Identity<T> {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            Ok(Self(ob.extract()?))
        }
    }

    impl<'py, T: Default> FromPyObject<'py> for OptionalZST<T> {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let x: bool = ob.extract()?;
            Ok(Self(x.then_some(T::default())))
        }
    }

    impl<'py, T> IntoPyObject<'py> for OptionalZST<T> {
        type Target = PyBool;
        type Output = Bound<'py, Self::Target>;
        type Error = Infallible;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            Ok(PyBool::new(py, self.0.is_some()).to_owned())
        }
    }
}

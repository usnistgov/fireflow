use crate::config::AllowLoss;
use crate::core::{AnyMetarootKeyLossError, IndexedKeyLossError, UnitaryKeyLossError};
use crate::logging::{ErrorResult, FungibleErrorResult, LogResult, WarningAndErrorResult};
use crate::type_families::{Applicative, Sibling1};
use crate::validated::keys::{IndexedKey, Key, MeasHeader};

use super::index::IndexFromOne;

use derive_more::{AsMut, AsRef, From, FromStr};
use std::fmt;
use std::iter;
use std::marker::PhantomData;
use std::string::ToString;
use thiserror::Error;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use pyo3::prelude::*;

/// A value that always exists.
#[derive(Clone, PartialEq, AsRef)]
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

/// A value that never exists.
#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct Nothing<T>(pub PhantomData<T>);

impl<T> IntoIterator for Nothing<T> {
    type Item = T;
    type IntoIter = iter::Empty<T>;
    fn into_iter(self) -> Self::IntoIter {
        iter::empty()
    }
}

impl<T> Default for Nothing<T> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

/// A string that is stored as-is but will not be displayed/written if blank.
#[derive(Debug, Clone, PartialEq, Eq, AsRef, AsMut, From, Default, FromStr)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromPyObject, IntoPyObject))]
#[as_ref(str)]
pub struct OptionalString(pub String);

/// A string that is stored as-is but will not be displayed/written if zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq, From, Default, FromStr)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromPyObject, IntoPyObject))]
pub struct OptionalInt<T>(pub T);

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

pub(crate) trait DisplayMaybe: IsDefault {
    fn display_maybe(&self) -> Option<String>;
}

pub(crate) trait KeywordPairMaybe: IsDefault + DisplayMaybe {
    type Inner;

    fn metaroot_opt_pair(&self) -> (String, Option<String>)
    where
        Self::Inner: Key,
    {
        (Self::Inner::std().to_string(), self.display_maybe())
    }

    fn meas_opt_pair(&self, i: impl Into<IndexFromOne>) -> (String, Option<String>)
    where
        Self::Inner: IndexedKey,
    {
        (Self::Inner::std(i).to_string(), self.display_maybe())
    }

    fn meas_opt_triple(&self, i: impl Into<IndexFromOne>) -> (MeasHeader, String, Option<String>)
    where
        Self::Inner: IndexedKey,
    {
        (
            Self::Inner::std_blank(),
            Self::Inner::std(i).to_string(),
            self.display_maybe(),
        )
    }
}

pub(crate) trait CheckMaybe: Sized + IsDefault {
    type Inner;

    fn check_key_transfer1(&self) -> Option<AnyMetarootKeyLossError>
    where
        AnyMetarootKeyLossError: From<UnitaryKeyLossError<Self::Inner>>,
    {
        (!self.is_default()).then_some(UnitaryKeyLossError::<Self::Inner>::new().into())
        // if self.is_default() {
        //     LogResult::new_ok(())
        // } else {
        //     let e = UnitaryKeyLossError::<Self::Inner>::new().into();
        //     LogResult::new_fungible((), (), e, allow_loss)
        // }
    }

    // TODO it would be simpler to just have all of these return an option<E>
    // and then eval them in one result
    fn check_key_transfer(
        &self,
        flag: AllowLoss,
    ) -> FungibleErrorResult<(), (), AllowLoss, AnyMetarootKeyLossError>
    where
        AnyMetarootKeyLossError: From<UnitaryKeyLossError<Self::Inner>>,
    {
        let e = UnitaryKeyLossError::<Self::Inner>::new().into();
        LogResult::new_fungible_ok_if(self.is_default(), (), (), e, flag)
        // if self.is_default() {
        //     LogResult::new_ok(())
        // } else {
        //     let e = UnitaryKeyLossError::<Self::Inner>::new().into();
        //     LogResult::new_fungible((), (), e, allow_loss)
        // }
    }

    fn check_indexed_key_transfer_fungible1<E>(&self, i: impl Into<IndexFromOne>) -> Option<E>
    where
        E: From<IndexedKeyLossError<Self::Inner>>,
    {
        (!self.is_default()).then_some(IndexedKeyLossError::<Self::Inner>::new(i).into())
    }

    fn check_indexed_key_transfer_fungible<E>(
        &self,
        i: impl Into<IndexFromOne>,
        flag: AllowLoss,
    ) -> FungibleErrorResult<(), (), AllowLoss, E>
    where
        E: From<IndexedKeyLossError<Self::Inner>>,
    {
        let e = IndexedKeyLossError::<Self::Inner>::new(i).into();
        LogResult::new_fungible_ok_if(self.is_default(), (), (), e, flag)
    }

    fn check_indexed_key_transfer<E>(&self, i: impl Into<IndexFromOne>) -> ErrorResult<(), (), E>
    where
        E: From<IndexedKeyLossError<Self::Inner>>,
    {
        let e = IndexedKeyLossError::<Self::Inner>::new(i).into();
        LogResult::new_non_fungible((), (), e, self.is_default())
    }

    fn check_indexed_key_transfer1<E>(&self, i: impl Into<IndexFromOne>) -> Option<E>
    where
        E: From<IndexedKeyLossError<Self::Inner>>,
    {
        (!self.is_default()).then_some(IndexedKeyLossError::<Self::Inner>::new(i).into())
    }
}

impl DisplayMaybe for OptionalString {
    fn display_maybe(&self) -> Option<String> {
        if self.0.is_empty() {
            None
        } else {
            Some(self.0.clone())
        }
    }
}

impl<T: fmt::Display + PartialEq + Default> DisplayMaybe for OptionalInt<T> {
    fn display_maybe(&self) -> Option<String> {
        if self.0 == T::default() {
            None
        } else {
            Some(self.0.to_string())
        }
    }
}

impl<T: fmt::Display + PartialEq + Default> DisplayMaybe for OptionalZST<T> {
    fn display_maybe(&self) -> Option<String> {
        self.0.as_ref().map(ToString::to_string)
    }
}

impl<T: fmt::Display + PartialEq> DisplayMaybe for Option<T> {
    fn display_maybe(&self) -> Option<String> {
        self.as_ref().map(ToString::to_string)
    }
}

impl<T: fmt::Display + PartialEq> KeywordPairMaybe for Option<T> {
    type Inner = T;
}

impl<T: fmt::Display + PartialEq> CheckMaybe for Option<T> {
    type Inner = T;
}

/// Encodes a type which might have something in it.
///
/// Intended to be used as a "type family" pattern.
pub trait MightHave<A>: Applicative<A> {
    /// If true, the wrapper will always have a value.
    ///
    /// Obviously, the implementation needs to ensure this is in sync with the
    /// meaning of Wrapper<T>.
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

impl<T> TryFrom<Option<T>> for Identity<T> {
    type Error = MaybeToAlwaysError;
    fn try_from(value: Option<T>) -> Result<Self, Self::Error> {
        value.ok_or(MaybeToAlwaysError).map(Identity)
    }
}

impl<T> From<Identity<T>> for Option<T> {
    fn from(value: Identity<T>) -> Self {
        Some(value.0)
    }
}

#[derive(Debug, Error)]
#[error("optional keyword value is blank")]
pub struct MaybeToAlwaysError;

#[cfg(feature = "python")]
mod python {
    use super::{Identity, OptionalZST};

    use pyo3::prelude::*;
    use pyo3::types::PyBool;
    use std::convert::Infallible;

    impl<'py, T> FromPyObject<'py> for Identity<T>
    where
        T: FromPyObject<'py>,
    {
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

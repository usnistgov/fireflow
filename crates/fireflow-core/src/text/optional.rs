use crate::core::{AnyMetarootKeyLossError, IndexedKeyLossError, UnitaryKeyLossError};
use crate::error::{BiTentative, Tentative};
use crate::validated::keys::{IndexedKey, Key, MeasHeader};

use super::index::IndexFromOne;

use derive_more::{AsMut, AsRef, From, FromStr};
use std::fmt;
use std::marker::PhantomData;
use std::string::ToString;
use thiserror::Error;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use pyo3::prelude::*;

/// A value that always exists.
#[derive(Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
pub struct AlwaysValue<T>(pub T);

/// A value that always exists.
#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct NeverValue<T>(pub PhantomData<T>);

impl<T> Default for NeverValue<T> {
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

    fn check_key_transfer(&self, allow_loss: bool) -> BiTentative<(), AnyMetarootKeyLossError>
    where
        AnyMetarootKeyLossError: From<UnitaryKeyLossError<Self::Inner>>,
    {
        if self.is_default() {
            Tentative::default()
        } else {
            Tentative::new_vec_either((), [UnitaryKeyLossError::<Self::Inner>::new()], !allow_loss)
        }
    }

    fn check_indexed_key_transfer_tnt<E>(
        &self,
        i: impl Into<IndexFromOne>,
        allow_loss: bool,
    ) -> BiTentative<(), E>
    where
        E: From<IndexedKeyLossError<Self::Inner>>,
    {
        if self.is_default() {
            Tentative::default()
        } else {
            let e = IndexedKeyLossError::<Self::Inner>::new(i);
            Tentative::new_vec_either((), [e], !allow_loss)
        }
    }

    fn check_indexed_key_transfer<E>(&self, i: impl Into<IndexFromOne>) -> Result<(), E>
    where
        E: From<IndexedKeyLossError<Self::Inner>>,
    {
        if self.is_default() {
            Ok(())
        } else {
            Err(IndexedKeyLossError::<Self::Inner>::new(i).into())
        }
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

    /// Apply function to inner value.
    fn map<T, T0, F>(x: Self::Wrapper<T>, f: F) -> Self::Wrapper<T0>
    where
        F: FnOnce(T) -> T0;
}

#[derive(Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct MaybeFamily;

impl MightHave for MaybeFamily {
    type Wrapper<T> = Option<T>;
    const INFALLABLE: bool = false;

    fn unwrap<T>(x: Self::Wrapper<T>) -> Result<T, Self::Wrapper<T>> {
        x.ok_or(None)
    }

    fn as_ref<T>(x: &Self::Wrapper<T>) -> Self::Wrapper<&T> {
        x.as_ref()
    }

    fn map<T, T0, F>(x: Self::Wrapper<T>, f: F) -> Self::Wrapper<T0>
    where
        F: FnOnce(T) -> T0,
    {
        x.map(f)
    }
}

#[derive(Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct AlwaysFamily;

impl MightHave for AlwaysFamily {
    type Wrapper<T> = AlwaysValue<T>;
    const INFALLABLE: bool = true;

    fn unwrap<T>(x: Self::Wrapper<T>) -> Result<T, Self::Wrapper<T>> {
        Ok(x.0)
    }

    fn as_ref<T>(x: &Self::Wrapper<T>) -> Self::Wrapper<&T> {
        AlwaysValue(&x.0)
    }

    fn map<T, T0, F>(x: Self::Wrapper<T>, f: F) -> Self::Wrapper<T0>
    where
        F: FnOnce(T) -> T0,
    {
        AlwaysValue(f(x.0))
    }
}

impl<T> From<T> for AlwaysValue<T> {
    fn from(value: T) -> Self {
        Self(value)
    }
}

impl<T> TryFrom<Option<T>> for AlwaysValue<T> {
    type Error = MaybeToAlwaysError;
    fn try_from(value: Option<T>) -> Result<Self, Self::Error> {
        value.ok_or(MaybeToAlwaysError).map(AlwaysValue)
    }
}

impl<T> From<AlwaysValue<T>> for Option<T> {
    fn from(value: AlwaysValue<T>) -> Self {
        Some(value.0)
    }
}

#[derive(Debug, Error)]
#[error("optional keyword value is blank")]
pub struct MaybeToAlwaysError;

#[cfg(feature = "python")]
mod python {
    use super::{AlwaysValue, OptionalZST};

    use pyo3::prelude::*;
    use pyo3::types::PyBool;
    use std::convert::Infallible;

    impl<'py, T> FromPyObject<'py> for AlwaysValue<T>
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

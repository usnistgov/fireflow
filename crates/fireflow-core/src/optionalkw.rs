use serde::Serialize;
use std::fmt;

/// Denotes that the value for a key is optional.
///
/// This is basically an Option but more obvious in what it indicates.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum OptionalKw<V> {
    Present(V),
    #[default]
    Absent,
}

use OptionalKw::*;

impl<V> OptionalKw<V> {
    pub fn as_ref(&self) -> OptionalKw<&V> {
        match self {
            OptionalKw::Present(x) => Present(x),
            Absent => Absent,
        }
    }
    pub fn into_option(self) -> Option<V> {
        match self {
            OptionalKw::Present(x) => Some(x),
            Absent => None,
        }
    }

    pub fn as_option(&self) -> Option<&V> {
        self.as_ref().into_option()
    }

    pub fn map<F, W>(self, f: F) -> OptionalKw<W>
    where
        F: Fn(V) -> W,
    {
        match self {
            OptionalKw::Present(x) => Present(f(x)),
            Absent => Absent,
        }
    }

    pub fn with_option<F, W>(self, f: F) -> OptionalKw<W>
    where
        F: FnOnce(Option<V>) -> Option<W>,
    {
        OptionalKw::<W>::from_option(f(self.into_option()))
    }

    pub fn with_ref_option<F, W>(self, f: F) -> OptionalKw<W>
    where
        F: FnOnce(Option<&V>) -> Option<W>,
    {
        OptionalKw::<&V>::with_option::<F, W>(self.as_ref(), f)
    }

    pub fn from_option(x: Option<V>) -> Self {
        x.map_or_else(|| Absent, |y| OptionalKw::Present(y))
    }
}

impl<V: fmt::Display> OptionalKw<V> {
    pub fn as_opt_string(&self) -> Option<String> {
        self.as_ref().into_option().map(|x| x.to_string())
    }
}

impl<T: Serialize> Serialize for OptionalKw<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self.as_ref() {
            Present(x) => serializer.serialize_some(x),
            Absent => serializer.serialize_none(),
        }
    }
}

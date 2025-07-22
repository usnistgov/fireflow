use derive_more::{Display, From, FromStr, Into};
use serde::Serialize;
use std::fmt;
use std::num::NonZeroUsize;

/// An index starting at 1, used as the basis for keyword indices
#[derive(Clone, Copy, Eq, PartialEq, PartialOrd, Debug, Serialize, Display, FromStr)]
pub struct IndexFromOne(NonZeroUsize);

impl From<usize> for IndexFromOne {
    fn from(value: usize) -> Self {
        IndexFromOne(NonZeroUsize::MIN.saturating_add(value))
    }
}

impl From<IndexFromOne> for usize {
    fn from(value: IndexFromOne) -> Self {
        usize::from(value.0) - 1
    }
}

macro_rules! newtype_index {
    ($(#[$attr:meta])* $t:ident) => {
        $(#[$attr])*
        #[derive(Clone, Copy, Eq, PartialEq, PartialOrd, Debug, Serialize,
                 FromStr, Display, From, Into)]
        pub struct $t(pub IndexFromOne);

        impl From<usize> for $t {
            fn from(value: usize) -> Self {
                Self(value.into())
            }
        }

        impl From<$t> for usize {
            fn from(value: $t) -> Self {
                value.0.into()
            }
        }
    };
}

impl IndexFromOne {
    pub(crate) fn check_index(&self, len: usize) -> Result<usize, IndexError> {
        let i = (*self).into();
        if i < len {
            Ok(i)
        } else {
            Err(IndexError { index: *self, len })
        }
    }

    pub(crate) fn check_boundary_index(&self, len: usize) -> Result<usize, BoundaryIndexError> {
        let i = (*self).into();
        if i <= len {
            Ok(i)
        } else {
            Err(BoundaryIndexError { index: *self, len })
        }
    }
}

newtype_index!(
    /// The 'n' in $Pn* keywords
    MeasIndex
);

newtype_index!(
    /// The 'n' in $Gn* keywords
    GateIndex
);

newtype_index!(
    /// The 'n' in $Rn* keywords
    RegionIndex
);

#[derive(Debug)]
pub struct IndexError {
    pub index: IndexFromOne, // refers to index of element
    pub len: usize,
}

#[derive(Debug)]
pub struct BoundaryIndexError {
    pub index: IndexFromOne, // refers to index between elements
    pub len: usize,
}

impl fmt::Display for IndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "index must be 0 <= i < {}, got {}", self.len, self.index)
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

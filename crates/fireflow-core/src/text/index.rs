use crate::macros::{newtype_disp, newtype_from, newtype_from_outer, newtype_fromstr};

use derive_more::{Display, FromStr};
use serde::Serialize;
use std::fmt;
use std::num::ParseIntError;

/// An index starting at 1, used as the basis for keyword indices
#[derive(Clone, Copy, Eq, PartialEq, PartialOrd, Debug, Serialize, Display, FromStr)]
pub struct IndexFromOne(usize);

impl From<usize> for IndexFromOne {
    fn from(value: usize) -> Self {
        IndexFromOne(value + 1)
    }
}

impl From<IndexFromOne> for usize {
    fn from(value: IndexFromOne) -> Self {
        value.0 - 1
    }
}

macro_rules! newtype_index {
    ($(#[$attr:meta])* $t:ident) => {
        $(#[$attr])*
        pub struct $t(pub IndexFromOne);

        newtype_disp!($t);
        newtype_from!($t, IndexFromOne);
        newtype_from_outer!($t, IndexFromOne);
        newtype_fromstr!($t, ParseIntError);

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
    #[derive(Clone, Copy, Eq, PartialEq, PartialOrd, Debug, Serialize)]
    MeasIndex
);

newtype_index!(
    /// The 'n' in $Gn* keywords
    #[derive(Clone, Copy, Eq, PartialEq, PartialOrd, Debug, Serialize)]
    GateIndex
);

newtype_index!(
    /// The 'n' in $Rn* keywords
    #[derive(Clone, Copy, Eq, PartialEq, PartialOrd, Debug, Serialize)]
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

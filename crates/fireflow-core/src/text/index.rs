use crate::macros::{newtype_disp, newtype_from, newtype_from_outer, newtype_fromstr};

use serde::Serialize;
use std::num::ParseIntError;

/// An index starting at 1, used as the basis for keyword indices
#[derive(Clone, Copy, Eq, PartialEq, PartialOrd, Debug, Serialize)]
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

newtype_disp!(IndexFromOne);
newtype_fromstr!(IndexFromOne, ParseIntError);

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

use derive_more::{Display, From, FromStr, Into};
use derive_new::new;
use std::num::NonZeroUsize;
use thiserror::Error;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{DisplayAsPyErr, FromInnerPyObject},
    pyo3::prelude::*,
};

/// An index starting at 1, used as the basis for keyword indices
#[derive(Clone, Copy, Eq, PartialEq, PartialOrd, Debug, Display, FromStr, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct IndexFromOne(NonZeroUsize);

impl From<usize> for IndexFromOne {
    fn from(value: usize) -> Self {
        Self(NonZeroUsize::MIN.saturating_add(value))
    }
}

impl From<IndexFromOne> for usize {
    fn from(value: IndexFromOne) -> Self {
        Self::from(value.0) - 1
    }
}

macro_rules! newtype_index {
    ($(#[$attr:meta])* $t:ident) => {
        $(#[$attr])*
        #[cfg_attr(feature = "serde", derive(Serialize))]
        #[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
        #[derive(Clone, Copy, Eq, PartialEq, PartialOrd, Debug,
                 FromStr, Display, From, Into, Hash)]
        #[from(IndexFromOne, usize)]
        #[into(IndexFromOne, usize)]
        pub struct $t(pub IndexFromOne);
    };
}

impl IndexFromOne {
    pub(crate) fn check_index(self, len: usize) -> Result<usize, IndexError> {
        let i = self.into();
        if i < len {
            Ok(i)
        } else {
            Err(IndexError::new(self, len))
        }
    }

    pub(crate) fn check_boundary_index(self, len: usize) -> Result<(), BoundaryIndexError> {
        if usize::from(self) <= len {
            Ok(())
        } else {
            Err(BoundaryIndexError::new(self, len))
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

/// Error when index referring to position of elements is out of range.
///
/// Used internally for creating more specific errors
#[derive(Debug, new)]
pub(crate) struct IndexError {
    pub index: IndexFromOne, // refers to index of element
    pub len: usize,
}

/// Error when index referring to position between elements is out of range.
#[derive(Debug, Error, new)]
#[error("0-index must be 0 <= i <= {len}, got {x}", x = usize::from(self.index))]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr), pyerr(PyIndexError))]
pub struct BoundaryIndexError {
    pub index: IndexFromOne, // refers to index between elements
    pub len: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZero;

    #[test]
    fn zero() {
        let i = 0_usize;
        let i0 = IndexFromOne::from(i);
        let i1 = usize::from(i0);
        assert_eq!(i, i1);
        assert_eq!(i0, IndexFromOne(NonZero::new(1).unwrap()));
    }
}

#[cfg(feature = "python")]
mod python {
    use super::IndexFromOne;
    use pyo3::prelude::*;
    use pyo3::types::PyInt;
    use std::convert::Infallible;

    impl<'py> FromPyObject<'py> for IndexFromOne {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let x: usize = ob.extract()?;
            Ok(x.into())
        }
    }

    impl<'py> IntoPyObject<'py> for IndexFromOne {
        type Target = PyInt;
        type Output = Bound<'py, PyInt>;
        type Error = Infallible;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            usize::from(self).into_pyobject(py)
        }
    }
}

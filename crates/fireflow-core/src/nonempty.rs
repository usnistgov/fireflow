use derive_more::{From, Into};
use itertools::Itertools as _;
use nonempty::NonEmpty;
use std::hash::Hash;

// A wrapper to bestow supernatural powers to "regular" non-empty. I may also
// make my own version of this so this makes that a bit easier if I end up
// deciding in favor.
#[derive(Into, From, PartialEq, Clone, Default, Debug)]
pub struct FCSNonEmpty<T>(pub NonEmpty<T>);

impl<X> FCSNonEmpty<X> {
    pub(crate) fn new(head: X) -> Self {
        Self(NonEmpty::new(head))
    }

    // pub(crate) fn new1(head: X, tail: Vec<X>) -> Self {
    //     Self(NonEmpty { head, tail })
    // }

    // fn enumerate(self) -> NonEmpty<(usize, Self::X)> {
    //     NonEmpty::collect(self.into_iter().enumerate()).unwrap()
    // }

    // fn map_results<F, E, Y>(self, f: F) -> MultiResult<NonEmpty<Y>, E>
    // where
    //     F: Fn(Self::X) -> Result<Y, E>,
    // {
    //     self.map(f)
    //         .into_iter()
    //         .gather()
    //         .map(|ys| NonEmpty::from_vec(ys).unwrap())
    // }

    pub(crate) fn unique(self) -> Self
    where
        X: Clone + Hash + Eq,
    {
        NonEmpty::collect(self.0.into_iter().unique())
            .unwrap()
            .into()
    }

    // fn remove(&mut self, index: IndexFromOne) -> Result<(), ClearOptionalOr<IndexError>> {
    //     index.check_index(self.len()).map_or_else(
    //         |e| Err(ClearOptionalOr::Error(e)),
    //         |i| {
    //             self.remove_nocheck(i.into())
    //                 .map_err(|_| ClearOptionalOr::Clear)
    //         },
    //     )
    // }

    // fn remove_nocheck(&mut self, index: IndexFromOne) -> Result<(), ClearOptional> {
    //     let i: usize = index.into();
    //     if i == 0 {
    //         let tail = std::mem::take(&mut self.tail);
    //         if let Some(xs) = NonEmpty::from_vec(tail) {
    //             *self = xs
    //         } else {
    //             return Err(ClearOptionalOr::Clear);
    //         }
    //     } else {
    //         self.tail.remove(i + 1);
    //     }
    //     Ok(())
    // }

    pub(crate) fn mode(&self) -> (&X, usize)
    where
        X: Eq,
    {
        let mut counts = NonEmpty::new((&self.0.head, 1));
        for d in &self.0.tail {
            if counts.last().0 == d {
                counts.last_mut().1 += 1;
            } else {
                counts.push((d, 1));
            }
        }
        let (mode, n) = counts.maximum_by_key(|x| x.1);
        (mode, *n)
    }
}

#[cfg(feature = "serde")]
mod serialize {
    use super::FCSNonEmpty;
    use serde::{ser::SerializeSeq as _, Serialize};

    impl<I: Serialize> Serialize for FCSNonEmpty<I> {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
            for e in self.0.iter() {
                seq.serialize_element(e)?;
            }
            seq.end()
        }
    }
}

#[cfg(feature = "python")]
mod python {
    use super::FCSNonEmpty;

    use nonempty::NonEmpty;
    use pyo3::exceptions::PyValueError;
    use pyo3::prelude::*;
    use pyo3::types::PyList;

    impl<'py, T> FromPyObject<'py> for FCSNonEmpty<T>
    where
        T: FromPyObject<'py>,
    {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let xs: Vec<T> = ob.extract()?;
            if let Some(ys) = NonEmpty::from_vec(xs) {
                Ok(ys.into())
            } else {
                Err(PyValueError::new_err("list must not be empty"))
            }
        }
    }

    impl<'py, T> IntoPyObject<'py> for FCSNonEmpty<T>
    where
        T: IntoPyObject<'py>,
    {
        type Target = PyList;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            PyList::new(py, Vec::from(self.0))
        }
    }
}

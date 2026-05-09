use crate::text::index::MeasIndex;

use derive_more::AsRef;
use nalgebra::DMatrix;
use nonempty_collections::NEVec;
use thiserror::Error;

use std::num::NonZeroUsize;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use fireflow_core_proc::DisplayAsPyErr;

/// A compensation matrix.
///
/// This is encoded in the $DFCiTOj keywords in 2.0 and $COMP in 3.0.
#[derive(Clone, AsRef, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct Compensation {
    /// Values in the comp matrix in row-major order. Assumed to be the
    /// same width and height as $PAR
    matrix: DMatrix<f32>,
    /// The size of the matrix. This is validated to be >= 2
    dim: NonZeroUsize,
}

impl TryFrom<DMatrix<f32>> for Compensation {
    type Error = NewCompError;

    fn try_from(matrix: DMatrix<f32>) -> Result<Self, Self::Error> {
        if !matrix.is_square() {
            Err(NewCompError::NotSquare)
        } else if !matrix.iter().all(|x| x.is_finite()) {
            Err(NewCompError::NotFinite)
        } else if let Some(dim) = NonZeroUsize::new(matrix.ncols())
            && dim.get() > 1
        {
            Ok(Self { matrix, dim })
        } else {
            Err(NewCompError::TooSmall)
        }
    }
}

impl Compensation {
    /// Add a new row/column corresponding to an identity transform.
    ///
    /// This is useful when inserting new measurements. Since $COMP needs to be
    /// the same width/height as the total number of measurements, adding a
    /// measurement means it needs a corresponding row/column in this matrix.
    ///
    /// The new row/column will be zeros everywhere except for the new index,
    /// meaning the new value will be 100% determined by itself and have no
    /// effect on existing measurements.
    ///
    /// Index is assumed to be valid. Will panic otherwise.
    pub(crate) fn insert_identity_by_index_unchecked(&mut self, index: MeasIndex) {
        let i = index.into();
        let mut new = self.matrix.clone().insert_row(i, 0.0).insert_column(i, 0.0);
        new[(i, i)] = 1.0;
        self.matrix = new;
    }

    pub(crate) fn matrix(&self) -> &DMatrix<f32> {
        &self.matrix
    }

    pub(crate) fn dim(&self) -> NonZeroUsize {
        self.dim
    }

    pub(crate) fn square_view(&self, n: usize) -> Option<Self> {
        let m: DMatrix<f32> = self.matrix.view((0, 0), (n, n)).into();
        Self::try_from(m).ok()
    }

    pub(crate) fn row_major_ne_vec(&self) -> NEVec<f32> {
        // DMatrix slices are column major, so transpose first to output
        // row-major
        NEVec::try_from_slice(self.matrix.transpose().as_slice())
            .expect("matrix should be at least 2x2")
    }
}

/// Error when making new compensation matrix from any float matrix.
#[derive(Debug, Error)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(
    feature = "python",
    pyerr(fireflow_types::python::InvalidKeywordValueError)
)]
pub enum NewCompError {
    #[error("compensation matrix must be square")]
    NotSquare,
    #[error("compensation matrix must be 2x2 or bigger")]
    TooSmall,
    #[error("compensation matrix may not have Nan, +Inf, or -Inf")]
    NotFinite,
}

#[cfg(test)]
mod tests {
    use super::*;
    use nalgebra::DMatrix;

    #[test]
    fn str_compensation_not_finite() {
        let m = DMatrix::from_row_slice(2, 2, &[0.0, 0.0, 0.0, f32::NAN]);
        assert!(Compensation::try_from(m).is_err());
    }

    #[test]
    fn str_compensation_not_square() {
        let m = DMatrix::from_row_slice(2, 3, &[0.0, 0.0, 0.0, 0.0, 0.0, 0.0]);
        assert!(Compensation::try_from(m).is_err());
    }
}

#[cfg(feature = "python")]
mod python {
    use super::Compensation;

    use numpy::{PyArray2, PyReadonlyArray2, ToPyArray as _};
    use pyo3::prelude::*;
    use std::convert::Infallible;

    impl<'py> FromPyObject<'py> for Compensation {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let x: PyReadonlyArray2<f32> = ob.extract()?;
            Ok(Self::try_from(x.as_matrix().into_owned())?)
        }
    }

    impl<'py> IntoPyObject<'py> for Compensation {
        type Target = PyArray2<f32>;
        type Output = Bound<'py, Self::Target>;
        type Error = Infallible;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            Ok(self.matrix.to_pyarray(py))
        }
    }
}

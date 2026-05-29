use crate::text::index::MeasIndex;

use derive_more::AsRef;
use ndarray::{Array2, s};
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
    matrix: Array2<f32>,
    /// The size of the matrix. This is validated to be >= 2
    dim: NonZeroUsize,
}

impl TryFrom<Array2<f32>> for Compensation {
    type Error = NewCompError;

    fn try_from(matrix: Array2<f32>) -> Result<Self, Self::Error> {
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
        let real_index = |j| {
            if j >= i { j + 1 } else { j }
        };
        let n = self.matrix.ncols();
        // Make new array with all zeros that has one extra row/col. Iterate
        // across rows and columns of old matrix and skip over the "inserted"
        // row and column, which will remain all zeros except for the new cell
        // at inserted row/inserted column index which will be 1.0.
        let mut new = Array2::zeros((n + 1, n + 1));
        for (rowi, row) in self.matrix.outer_iter().enumerate() {
            let real_rowi = real_index(rowi);
            for (coli, x) in row.iter().enumerate() {
                let real_coli = real_index(coli);
                new[(real_rowi, real_coli)] = *x;
            }
        }
        new[(i, i)] = 1.0;
        self.matrix = new;
    }

    pub(crate) fn matrix(&self) -> &Array2<f32> {
        &self.matrix
    }

    pub(crate) fn dim(&self) -> NonZeroUsize {
        self.dim
    }

    pub(crate) fn square_view(&self, n: usize) -> Option<Self> {
        let m = self.matrix.slice(s![..n, ..n]).into_owned();
        Self::try_from(m).ok()
    }

    pub(crate) fn row_major_ne_vec(&self) -> NEVec<f32> {
        // NDArrays are row-major, no need to transpose
        NEVec::try_from_slice(self.matrix.as_slice().expect("matrix should be contiguous"))
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
    use proptest::prelude::*;

    fn comp_vec() -> impl Strategy<Value = Vec<f32>> {
        (2_usize..50).prop_flat_map(|n| prop::collection::vec(f32::MIN..f32::MAX, n * n))
    }

    proptest! {
        #[test]
        fn str_compensation_valid(xs in comp_vec()) {
            let n = xs.len().isqrt();
            let m = Array2::from_shape_vec((n, n), xs).unwrap();
            assert!(Compensation::try_from(m).is_ok());
        }
    }

    #[test]
    fn str_compensation_not_nan() {
        let m = Array2::from_shape_vec((2, 2), vec![0.0, 0.0, 0.0, f32::NAN]).unwrap();
        assert!(Compensation::try_from(m).is_err());
    }

    #[test]
    fn str_compensation_not_inf() {
        let m = Array2::from_shape_vec((2, 2), vec![0.0, 0.0, 0.0, f32::INFINITY]).unwrap();
        assert!(Compensation::try_from(m).is_err());
    }

    #[test]
    fn str_compensation_not_neg_inf() {
        let m = Array2::from_shape_vec((2, 2), vec![0.0, 0.0, 0.0, f32::NEG_INFINITY]).unwrap();
        assert!(Compensation::try_from(m).is_err());
    }

    #[test]
    fn str_compensation_not_square() {
        let m = Array2::from_shape_vec((2, 3), vec![0.0, 0.0, 0.0, 0.0, 0.0, 0.0]).unwrap();
        assert!(Compensation::try_from(m).is_err());
    }
}

#[cfg(feature = "python")]
mod python {
    use super::Compensation;

    use numpy::{IntoPyArray as _, PyArray2, PyReadonlyArray2};
    use pyo3::prelude::*;
    use std::convert::Infallible;

    impl<'py> FromPyObject<'_, 'py> for Compensation {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            let x: PyReadonlyArray2<f32> = obj.extract()?;
            Ok(Self::try_from(x.as_array().into_owned())?)
        }
    }

    impl<'py> IntoPyObject<'py> for Compensation {
        type Target = PyArray2<f32>;
        type Output = Bound<'py, Self::Target>;
        type Error = Infallible;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            Ok(self.matrix.into_pyarray(py))
        }
    }
}

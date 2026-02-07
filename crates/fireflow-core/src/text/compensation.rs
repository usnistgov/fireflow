use crate::config::{ProcessOptionalFailure, ReadDataKeywordsConfig};
use crate::core::BiIndexedKeyLossError;
use crate::logging::{DeferredSwitchableErrors, LogResult, ResultExt as _};
use crate::text::index::MeasIndex;
use crate::text::keywords::{Dfc, Par};
use crate::text::relational::{
    Comp2_0Missing, ExistingIndexedLinkError, RemovedComp2_0Cell, RemovedLink,
};
use crate::validated::keys::{BiIndex, BiIndexedKey as _, Key2, SpecificKey, StdKeywords};

use derive_more::{AsRef, Display, From, Into};
use itertools::Itertools as _;
use nalgebra::DMatrix;
use nonempty::NonEmpty;
use std::fmt;
use thiserror::Error;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr, FromInnerPyObject};

use super::keywords::LookupDfcError;
use super::relational::BiIndexedKeyToIndexLinkError;

/// The aggregated values of the $DFCiTOj keywords (2.0 only)
#[derive(Clone, From, Into, AsRef, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromInnerPyObject))]
#[as_ref(DMatrix<f32>, Compensation)]
pub struct Compensation2_0(pub Compensation);

/// A compensation matrix.
///
/// This is encoded in the $DFCiTOj keywords in 2.0 and $COMP in 3.0.
#[derive(Clone, AsRef, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct Compensation {
    /// Values in the comp matrix in row-major order. Assumed to be the
    /// same width and height as $PAR
    matrix: DMatrix<f32>,
}

impl Compensation2_0 {
    pub(crate) fn lookup(
        kws: &mut StdKeywords,
        par: Par,
        conf: &ReadDataKeywordsConfig,
    ) -> DeferredSwitchableErrors<Option<Self>, ProcessOptionalFailure, LookupComp2_0Error> {
        // column = src measurement
        // row = target measurement
        // These are "flipped" in 2.0, where "column" goes TO the "row"
        let n = par.0;
        let flag = conf.process_optional_failure;
        let (xs, warnings): (Vec<_>, Vec<_>) = (0..n)
            .cartesian_product(0..n)
            .map(|(r, c)| {
                let k = SpecificKey::new_i2(c.into(), r.into());
                match Dfc::lookup(kws, k) {
                    Ok(x) => (x, None),
                    Err(w) => (None, Some(LookupComp2_0Error::Dfc(w))),
                }
            })
            .unzip();
        let res = if xs.iter().all(Option::is_none) || xs.is_empty() {
            LogResult::new_switchable_ok(None, flag)
        } else {
            let ys = xs.into_iter().map(|x| x.unwrap_or(0.0));
            let matrix = DMatrix::from_row_iterator(n, n, ys);
            Compensation::try_from(matrix)
                .map(|x| Some(Self(x)))
                .map_err(LookupComp2_0Error::Matrix)
                .into_deferred_switchable(flag)
        };
        res.extend_deferred_switchable_errors(warnings.into_iter().flatten())
    }

    pub fn non_zero_indices(&self) -> impl Iterator<Item = (MeasIndex, MeasIndex, f32)> {
        let m = &self.0.matrix;
        m.iter().enumerate().filter_map(|(i, &x)| {
            let n = m.ncols();
            if x == 0.0 {
                None
            } else {
                let row = i / n;
                let col = i % n;
                Some((col.into(), row.into(), x))
            }
        })
    }

    pub fn opt_keywords(&self) -> impl Iterator<Item = (String, String)> {
        self.non_zero_indices()
            .map(|(col, row, value)| (Dfc::std(row, col).to_string(), value.to_string()))
    }

    pub(crate) fn invalid_link_errors(
        &self,
        par: &Par,
    ) -> impl Iterator<Item = BiIndexedKeyToIndexLinkError<Dfc>> {
        // If $PAR is 1 or matrix is smaller than $PAR, use a cutoff of zero
        // since the entire matrix must be removed.
        self.non_zero_indices().filter_map(|(col, row, _)| {
            // TODO throw error if temporal measurement is anything other than ID
            let n = self.0.matrix.nrows();
            let bad_matrix = n < par.0 || par.0 < 2;
            let cutoff = if bad_matrix { 0 } else { par.0 };
            let k = Key2::new_i2(col.into(), row.into());
            let r = (usize::from(row) >= cutoff).then_some(row);
            let c = (usize::from(col) >= cutoff).then_some(col);
            NonEmpty::collect([r, c].into_iter().flatten())
                .map(|js| BiIndexedKeyToIndexLinkError::new(js, k))
        })
    }

    // NOTE this shouldn't do anything for a freshly made comp matrix since
    // the DFCmTOn lookups are bound by $PAR, so it impossible for the matrix
    // to be greater than $PAR. This will fire whenever we assign an external
    // matrix to the Core data struct.
    pub(crate) fn remove_invalid_link(src: &mut Option<Self>, par: Par) -> Option<RemovedLink> {
        // TODO throw error if temporal measurement is anything other than ID
        let c = src.as_mut()?;
        let n = c.0.matrix.nrows();
        // If $PAR is 1 or matrix is smaller than $PAR, use a cutoff of zero
        // since the entire matrix must be removed.
        let bad_matrix = n < par.0 || par.0 < 2;
        let cutoff = if bad_matrix { 0 } else { par.0 };
        // Scan through matrix and pull out all cells in rows/columns greater
        // or equal to cutoff and whose value is not zero. These are the keywords
        // to return.
        let es = c.non_zero_indices().filter_map(|(col, row, value)| {
            let which = match (usize::from(row) >= cutoff, usize::from(col) >= cutoff) {
                (true, true) => Some(Comp2_0Missing::Both),
                (true, false) => Some(Comp2_0Missing::Row),
                (false, true) => Some(Comp2_0Missing::Col),
                (false, false) => None,
            };
            which.map(|b| RemovedComp2_0Cell::new(row, col, value, b))
        });
        let ret = NonEmpty::collect(es).map(RemovedLink::Comp2_0);
        // If resulting matrix is less than 2x2, replace with None. Otherwise
        // truncate the matrix down to $PAR by $PAR
        if bad_matrix {
            *src = None;
        } else {
            c.0.matrix = c.0.matrix.view((0, 0), (par.0, par.0)).into();
        }
        ret
    }

    pub(crate) fn existing_links(
        &self,
    ) -> impl Iterator<Item = ExistingIndexedLinkError<Dfc, BiIndex>> {
        self.non_zero_indices().map(|(col, row, _)| {
            let xs = NonEmpty::from((col.into(), vec![row.into()]));
            ExistingIndexedLinkError::new(Key2::new_i2(col.into(), row.into()), xs)
        })
    }

    pub(crate) fn loss_errors(&self) -> impl Iterator<Item = BiIndexedKeyLossError<Dfc>> {
        self.non_zero_indices()
            .map(|(col, row, _)| BiIndexedKeyLossError(Key2::new_i2(col.into(), row.into())))
    }
}

impl TryFrom<DMatrix<f32>> for Compensation {
    type Error = NewCompError;

    fn try_from(matrix: DMatrix<f32>) -> Result<Self, Self::Error> {
        if !matrix.is_square() {
            Err(NewCompError::NotSquare)
        } else if matrix.ncols() < 2 {
            Err(NewCompError::TooSmall)
        } else if !matrix.iter().all(|x| x.is_finite()) {
            Err(NewCompError::NotFinite)
        } else {
            Ok(Self { matrix })
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

impl fmt::Display for Compensation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let n = self.matrix.ncols();
        // DMatrix slices are column major, so transpose first to output
        // row-major
        let xs = self.matrix.transpose().as_slice().iter().join(",");
        write!(f, "{n},{xs}")
    }
}

/// Error when parsing $DFCiTOj keywords for compensation matrix (2.0)
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupComp2_0Error {
    Dfc(LookupDfcError),
    Matrix(NewCompError),
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

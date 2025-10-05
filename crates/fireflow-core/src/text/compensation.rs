use crate::config::StdTextReadConfig;
use crate::error::*;
use crate::validated::keys::{BiIndexedKey, StdKey, StdKeywords};

use super::index::MeasIndex;
use super::keywords::{Dfc, Par};
use super::optional::*;
use super::parser::*;

use derive_more::{AsRef, Display, From, Into};
use itertools::Itertools;
use nalgebra::DMatrix;
use std::fmt;
use std::num::ParseFloatError;
use std::str::FromStr;
use thiserror::Error;

#[cfg(feature = "serde")]
use serde::Serialize;

/// The aggregated values of the $DFCiTOj keywords (2.0)
#[derive(Clone, From, Into, AsRef, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[as_ref(DMatrix<f32>, Compensation)]
pub struct Compensation2_0(pub Compensation);

/// The value of the $COMP keyword (3.0)
#[derive(Clone, From, Into, Display, AsRef, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[as_ref(DMatrix<f32>, Compensation)]
pub struct Compensation3_0(pub Compensation);

/// A compensation matrix.
///
/// This is encoded in the $DFCmTOn keywords in 2.0 and $COMP in 3.0.
#[derive(Clone, AsRef, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct Compensation {
    /// Values in the comp matrix in row-major order. Assumed to be the
    /// same width and height as $PAR
    matrix: DMatrix<f32>,
}

impl Compensation2_0 {
    pub(crate) fn lookup(kws: &mut StdKeywords, par: Par) -> LookupTentative<MaybeValue<Self>> {
        // column = src measurement
        // row = target measurement
        // These are "flipped" in 2.0, where "column" goes TO the "row"
        let n = par.0;
        let (xs, warnings): (Vec<_>, Vec<_>) = (0..n)
            .cartesian_product(0..n)
            .map(|(r, c)| {
                let k = Dfc::std(c, r);
                match lookup_dfc(kws, k) {
                    Ok(x) => (x, None),
                    Err(w) => (None, Some(LookupKeysWarning::Parse(w.inner_into()))),
                }
            })
            .unzip();
        let mut tnt = if xs.iter().all(|x| x.is_none()) || xs.is_empty() {
            Tentative::default()
        } else {
            let ys = xs.into_iter().map(|x| x.unwrap_or(0.0));
            let matrix = DMatrix::from_row_iterator(n, n, ys);
            Compensation::try_from(matrix)
                .map(|x| Some(Self(x)))
                .map_err(LookupKeysWarning::CompShape)
                .map_or(Tentative::default(), Tentative::new1)
        };
        tnt.extend_warnings(warnings.into_iter().flatten());
        tnt.map(MaybeValue)
    }

    pub fn opt_keywords(&self) -> Vec<(String, String)> {
        let m = &self.0.matrix;
        let n = m.ncols();
        m.iter()
            .enumerate()
            .flat_map(|(i, x)| {
                if *x != 0.0 {
                    let row = i / n;
                    let col = i % n;
                    Some((Dfc::std(row, col).to_string(), x.to_string()))
                } else {
                    None
                }
            })
            .collect()
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

impl FromStrStateful for Compensation3_0 {
    type Err = ParseCompError;
    type Payload<'a> = ();

    fn from_str_st(s: &str, _: (), conf: &StdTextReadConfig) -> Result<Self, Self::Err> {
        Self::from_str_delim(s, conf.trim_intra_value_whitespace)
    }
}

impl FromStr for Compensation3_0 {
    type Err = ParseCompError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_str_delim(s, false)
    }
}

impl FromStrDelim for Compensation3_0 {
    type Err = ParseCompError;
    const DELIM: char = ',';

    fn from_iter<'a>(mut ss: impl Iterator<Item = &'a str>) -> Result<Self, Self::Err> {
        if let Some(first) = ss.next().and_then(|x| x.parse::<usize>().ok()) {
            let n = first;
            let nn = n * n;
            let values: Vec<_> = ss.by_ref().take(nn).collect();
            let remainder = ss.by_ref().count();
            let total = values.len() + remainder;
            if total != nn {
                Err(ParseCompError::WrongLength {
                    expected: nn,
                    total,
                })
            } else {
                let fvalues: Vec<_> = values
                    .into_iter()
                    .filter_map(|x| x.parse::<f32>().ok())
                    .collect();
                if fvalues.len() != nn {
                    Err(ParseCompError::BadFloat)
                } else {
                    let matrix = DMatrix::from_row_iterator(n, n, fvalues);
                    Ok(Compensation::try_from(matrix).map(Self)?)
                }
            }
        } else {
            Err(ParseCompError::BadLength)
        }
    }
}

#[derive(Debug, Error)]
pub enum ParseCompError {
    #[error("Expected {expected} entries, found {total}")]
    WrongLength { total: usize, expected: usize },
    #[error("Could not determine length")]
    BadLength,
    #[error("Float could not be parsed")]
    BadFloat,
    #[error("{0}")]
    New(#[from] NewCompError),
}

#[derive(Debug, Error)]
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

pub(crate) fn lookup_dfc(
    kws: &mut StdKeywords,
    k: StdKey,
) -> Result<Option<f32>, OptKeyError<ParseFloatError>> {
    kws.remove(&k).map_or(Ok(None), |v| {
        v.parse::<f32>()
            .map_err(|e| OptKeyError {
                error: e,
                key: k,
                value: v.clone(),
            })
            .map(Some)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;
    use nalgebra::DMatrix;

    #[test]
    fn test_str_compensation() {
        assert_from_to_str::<Compensation3_0>("2,0,0,0,0");
        assert_from_to_str::<Compensation3_0>("3,0,0,0,0,0,0,0,0,0");
        assert_from_to_str::<Compensation3_0>("2,1.1,1,0,-1.5");
    }

    #[test]
    fn test_str_compensation_too_small() {
        assert!("1,0".parse::<Compensation3_0>().is_err());
    }

    #[test]
    fn test_str_compensation_mismatch() {
        assert!("2,0,0,0".parse::<Compensation3_0>().is_err());
    }

    #[test]
    fn test_str_compensation_badfloats() {
        assert!("2,zero,0,coconut".parse::<Compensation3_0>().is_err());
    }

    #[test]
    fn test_str_compensation_not_finite() {
        let m = DMatrix::from_row_slice(2, 2, &[0.0, 0.0, 0.0, f32::NAN]);
        assert!(Compensation::try_from(m).is_err());
    }

    #[test]
    fn test_str_compensation_not_square() {
        let m = DMatrix::from_row_slice(2, 3, &[0.0, 0.0, 0.0, 0.0, 0.0, 0.0]);
        assert!(Compensation::try_from(m).is_err());
    }
}

#[cfg(feature = "python")]
mod python {
    use crate::python::macros::{impl_from_py_transparent, impl_value_err};

    use super::{Compensation, Compensation2_0, Compensation3_0, NewCompError};

    use numpy::{PyArray2, PyReadonlyArray2, ToPyArray};
    use pyo3::prelude::*;

    impl_value_err!(NewCompError);

    impl<'py> FromPyObject<'py> for Compensation {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let x: PyReadonlyArray2<f32> = ob.extract()?;
            Ok(Self::try_from(x.as_matrix().into_owned())?)
        }
    }

    impl<'py> IntoPyObject<'py> for Compensation {
        type Target = PyArray2<f32>;
        type Output = Bound<'py, Self::Target>;
        type Error = std::convert::Infallible;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            Ok(self.matrix.to_pyarray(py))
        }
    }

    impl_from_py_transparent!(Compensation2_0);
    impl_from_py_transparent!(Compensation3_0);
}

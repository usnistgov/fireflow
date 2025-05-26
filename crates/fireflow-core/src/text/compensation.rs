use crate::validated::nonstandard::*;

use super::optionalkw::*;

use itertools::Itertools;
use nalgebra::DMatrix;
use serde::Serialize;
use std::fmt;
use std::str::FromStr;

/// A compensation matrix.
///
/// This is encoded in the $DFCmTOn keywords in 2.0 and $COMP in 3.0.
#[derive(Clone, Serialize)]
pub struct Compensation {
    /// Values in the comp matrix in row-major order. Assumed to be the
    /// same width and height as $PAR
    matrix: DMatrix<f32>,
}

impl Compensation {
    pub fn try_new(matrix: DMatrix<f32>) -> Result<Self, NewCompError> {
        if !matrix.is_square() {
            Err(NewCompError::NotSquare)
        } else if matrix.ncols() < 2 {
            Err(NewCompError::TooSmall)
        } else {
            Ok(Self { matrix })
        }
    }

    pub(crate) fn remove_by_index(&mut self, index: MeasIdx) -> Result<bool, ClearOptional> {
        let i: usize = index.into();
        let n = self.matrix.ncols();
        if i <= n {
            if n < 3 {
                Err(ClearOptional)
            } else {
                self.matrix = self.matrix.clone().remove_row(i).remove_column(i);
                Ok(true)
            }
        } else {
            Ok(false)
        }
    }

    pub fn matrix(&self) -> &DMatrix<f32> {
        &self.matrix
    }
}

impl FromStr for Compensation {
    type Err = ParseCompError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut xs = s.split(",");
        if let Some(first) = &xs.next().and_then(|x| x.parse::<usize>().ok()) {
            let n = *first;
            let nn = n * n;
            let values: Vec<_> = xs.by_ref().take(nn).collect();
            let remainder = xs.by_ref().count();
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
                    Ok(Compensation { matrix })
                }
            }
        } else {
            Err(ParseCompError::BadLength)
        }
    }
}

pub enum ParseCompError {
    WrongLength { total: usize, expected: usize },
    BadLength,
    BadFloat,
}

pub enum NewCompError {
    NotSquare,
    TooSmall,
}

impl fmt::Display for NewCompError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let s = match self {
            Self::NotSquare => "compensation matrix must be square",
            Self::TooSmall => "compensation matrix must be 2x2 or bigger",
        };
        write!(f, "{s}")
    }
}

impl fmt::Display for Compensation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let n = self.matrix.len();
        // DMatrix slices are column major, so transpose first to output
        // row-major
        let xs = self.matrix.transpose().as_slice().iter().join(",");
        write!(f, "{n},{xs}")
    }
}

impl fmt::Display for ParseCompError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            ParseCompError::BadFloat => write!(f, "Float could not be parsed"),
            ParseCompError::WrongLength { total, expected } => {
                write!(f, "Expected {expected} entries, found {total}")
            }
            ParseCompError::BadLength => write!(f, "Could not determine length"),
        }
    }
}

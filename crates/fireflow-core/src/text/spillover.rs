use crate::validated::shortname::*;

use super::named_vec::NameMapping;
use super::optional::ClearOptional;
use super::parser::OptLinkedKey;

use derive_more::AsRef;
use itertools::Itertools;
use nalgebra::DMatrix;
use std::collections::HashSet;
use std::fmt;
use std::str::FromStr;

#[cfg(feature = "serde")]
use serde::Serialize;

/// The spillover matrix from the $SPILLOVER keyword (3.1+)
#[derive(Clone, AsRef)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct Spillover {
    /// The measurements in the spillover matrix.
    ///
    /// Assumed to be a subset of the values in the $PnN keys and unique.
    #[as_ref([Shortname])]
    measurements: Vec<Shortname>,

    /// Numeric values in the spillover matrix in row-major order.
    #[as_ref]
    matrix: DMatrix<f32>,
}

impl Spillover {
    pub fn try_new(
        measurements: Vec<Shortname>,
        matrix: DMatrix<f32>,
    ) -> Result<Self, SpilloverError> {
        let n = measurements.len();
        let c = matrix.ncols();
        let r = matrix.nrows();
        if r != c {
            Err(SpilloverError::NonSquare)
        } else if n != r {
            Err(SpilloverError::NameLen)
        } else if measurements.iter().unique().count() != n {
            Err(SpilloverError::NonUnique)
        } else if n < 2 {
            Err(SpilloverError::TooSmall)
        } else {
            Ok(Self {
                measurements,
                matrix,
            })
        }
    }

    pub(crate) fn remove_by_name(&mut self, n: &Shortname) -> Result<bool, ClearOptional> {
        if let Some(i) = self.measurements.iter().position(|m| m == n) {
            if self.measurements.len() < 3 {
                Err(ClearOptional::default())
            } else {
                // TODO this looks expensive; it copies almost everything 3x;
                // good thing these matrices aren't that big (usually). The
                // alternative is to iterate over the matrix and populate a new
                // one while skipping certain elements.
                self.matrix = self.matrix.clone().remove_row(i).remove_column(i);
                Ok(true)
            }
        } else {
            Ok(false)
        }
    }

    pub(crate) fn table(&self, delim: &str) -> Vec<String> {
        let header0 = vec!["[-]"];
        let header = header0
            .into_iter()
            .chain(self.measurements.iter().map(|m| m.as_ref()))
            .join(delim);
        let lines = vec![header];
        let rows = self.matrix.row_iter().map(|xs| xs.iter().join(delim));
        lines.into_iter().chain(rows).collect()
    }

    pub(crate) fn print_table(&self, delim: &str) {
        for e in self.table(delim) {
            println!("{}", e);
        }
    }
}

impl fmt::Display for Spillover {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let n = self.measurements.len();
        // DMatrix slices are column major, so transpose first to output
        // row-major
        let xs = self.matrix.transpose().as_slice().iter().join(",");
        write!(f, "{n},{xs}")
    }
}

impl FromStr for Spillover {
    type Err = ParseSpilloverError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        {
            let mut xs = s.split(",");
            if let Some(first) = &xs.next().and_then(|x| x.parse::<usize>().ok()) {
                let n = *first;
                let nn = n * n;
                let expected = n + nn;
                // This should be safe since we split on commas
                let measurements: Vec<_> =
                    xs.by_ref().take(n).map(Shortname::new_unchecked).collect();
                let values: Vec<_> = xs.collect();
                let total = measurements.len() + values.len();
                if total != expected {
                    Err(ParseSpilloverError::WrongLength { total, expected })
                } else {
                    let fvalues: Vec<_> = values
                        .into_iter()
                        .filter_map(|x| x.parse::<f32>().ok())
                        .collect();
                    if fvalues.len() != nn {
                        Err(ParseSpilloverError::BadFloat)
                    } else {
                        let matrix = DMatrix::from_row_iterator(n, n, fvalues);
                        Spillover::try_new(measurements, matrix)
                            .map_err(ParseSpilloverError::Internal)
                    }
                }
            } else {
                Err(ParseSpilloverError::BadN)
            }
        }
    }
}

pub enum SpilloverError {
    NonSquare,
    NameLen,
    NonUnique,
    TooSmall,
}

pub enum ParseSpilloverError {
    WrongLength { total: usize, expected: usize },
    BadFloat,
    BadN,
    Internal(SpilloverError),
}

impl fmt::Display for ParseSpilloverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            ParseSpilloverError::WrongLength { total, expected } => {
                write!(f, "Expected {expected} entries, found {total}")
            }
            ParseSpilloverError::BadFloat => write!(f, "Float could not be parsed"),
            ParseSpilloverError::BadN => write!(f, "N could not be parsed"),
            ParseSpilloverError::Internal(i) => i.fmt(f),
        }
    }
}

impl fmt::Display for SpilloverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let s = match self {
            SpilloverError::NonSquare => "Matrix is not square",
            SpilloverError::NonUnique => "Names are not unique",
            SpilloverError::NameLen => "Name length does not match matrix dimensions",
            SpilloverError::TooSmall => "Matrix is less than 2x2",
        };
        write!(f, "{}", s)
    }
}

impl OptLinkedKey for Spillover {
    fn names(&self) -> HashSet<&Shortname> {
        self.measurements.iter().collect()
    }

    fn reassign(&mut self, mapping: &NameMapping) {
        // ASSUME mapping is such that new names will be unique
        for n in self.measurements.iter_mut() {
            if let Some(new) = mapping.get(n) {
                *n = (*new).clone();
            }
        }
    }
}

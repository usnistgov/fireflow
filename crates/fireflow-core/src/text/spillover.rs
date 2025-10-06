use crate::config::StdTextReadConfig;
use crate::error::ErrorIter;
use crate::validated::shortname::Shortname;

use super::index::MeasIndex;
use super::named_vec::NameMapping;
use super::parser::{FromStrStateful, LinkedNameError, OptLinkedKey};

use derive_more::{AsRef, Display, From};
use derive_new::new;
use itertools::Itertools;
use nalgebra::DMatrix;
use nonempty::NonEmpty;
use std::collections::HashSet;
use std::fmt;
use std::hash::Hash;
use std::num::ParseIntError;
use std::str::FromStr;
use thiserror::Error;

#[cfg(feature = "serde")]
use serde::Serialize;

pub type Spillover = GenericSpillover<Shortname>;

/// The spillover matrix from the $SPILLOVER keyword (3.1+)
#[derive(Clone, AsRef, PartialEq, Debug, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct GenericSpillover<T> {
    /// The measurements in the spillover matrix.
    ///
    /// Assumed to be a subset of the values in the $PnN keys and unique.
    #[as_ref([T])]
    measurements: Vec<T>,

    /// Numeric values in the spillover matrix in row-major order.
    #[as_ref]
    matrix: DMatrix<f32>,
}

impl Spillover {
    // pub(crate) fn remove_by_name(&mut self, n: &Shortname) -> ClearMaybe<bool> {
    //     if let Some(i) = self.measurements.iter().position(|m| m == n) {
    //         if self.measurements.len() < 3 {
    //             ClearMaybe::clear(true)
    //         } else {
    //             // TODO this looks expensive; it copies almost everything 3x;
    //             // good thing these matrices aren't that big (usually). The
    //             // alternative is to iterate over the matrix and populate a new
    //             // one while skipping certain elements.
    //             self.matrix = self.matrix.clone().remove_row(i).remove_column(i);
    //             ClearMaybe::new(true)
    //         }
    //     } else {
    //         ClearMaybe::new(false)
    //     }
    // }

    // pub(crate) fn table(&self, delim: &str) -> Vec<String> {
    //     let header0 = vec!["[-]"];
    //     let header = header0
    //         .into_iter()
    //         .chain(self.measurements.iter().map(|m| m.as_ref()))
    //         .join(delim);
    //     let lines = vec![header];
    //     let rows = self.matrix.row_iter().map(|xs| xs.iter().join(delim));
    //     lines.into_iter().chain(rows).collect()
    // }

    pub(crate) fn names_difference(
        &self,
        names: &HashSet<&Shortname>,
    ) -> impl Iterator<Item = &Shortname> {
        self.measurements.iter().filter(|n| !names.contains(n))
    }
}

impl GenericSpillover<MeasIndex> {
    pub(crate) fn try_into_named(
        self,
        names: &[&Shortname],
    ) -> Result<Spillover, SpilloverIndexError> {
        let ms = self
            .measurements
            .into_iter()
            .map(|i| names.get(usize::from(i)).ok_or(i).map(|&x| x.clone()))
            .gather()
            .map_err(SpilloverIndexError)?;
        Ok(Spillover {
            measurements: ms,
            matrix: self.matrix,
        })
    }
}

impl<T> GenericSpillover<T> {
    pub fn try_new(measurements: Vec<T>, matrix: DMatrix<f32>) -> Result<Self, NewSpilloverError>
    where
        T: Eq + Hash,
    {
        let n = measurements.len();
        let c = matrix.ncols();
        let r = matrix.nrows();
        if r != c {
            Err(NewSpilloverError::NonSquare)
        } else if n != r {
            Err(NewSpilloverError::NameLen)
        } else if measurements.iter().unique().count() != n {
            Err(NewSpilloverError::NonUnique)
        } else if n < 2 {
            Err(NewSpilloverError::TooSmall)
        } else {
            Ok(Self {
                measurements,
                matrix,
            })
        }
    }

    fn from_iter<'a, E, F, EM>(
        mut xs: impl Iterator<Item = &'a str>,
        parse_meas: F,
    ) -> Result<Self, E>
    where
        E: From<ParseGenericSpilloverError> + From<EM>,
        F: Fn(&str) -> Result<T, EM>,
        T: Eq + Hash,
    {
        if let Some(first) = xs.next().and_then(|x| x.parse::<usize>().ok()) {
            let n = first;
            let nn = n * n;
            let expected = n + nn;
            // This should be safe since we split on commas
            let measurements = xs
                .by_ref()
                .take(n)
                .map(parse_meas)
                .collect::<Result<Vec<_>, _>>()?;
            let values: Vec<_> = xs.collect();
            let total = measurements.len() + values.len();
            if total == expected {
                if let Ok(fvalues) = values
                    .into_iter()
                    .map(str::parse::<f32>)
                    .collect::<Result<Vec<_>, _>>()
                {
                    let matrix = DMatrix::from_row_iterator(n, n, fvalues);
                    Ok(Self::try_new(measurements, matrix)
                        .map_err(ParseGenericSpilloverError::New)?)
                } else {
                    Err(ParseGenericSpilloverError::BadFloat)?
                }
            } else {
                Err(ParseGenericSpilloverError::WrongLength { total, expected })?
            }
        } else {
            Err(ParseGenericSpilloverError::BadN)?
        }
    }

    fn from_str<E, F, EM>(s: &str, trim_intra: bool, parse_meas: F) -> Result<Self, E>
    where
        E: From<ParseGenericSpilloverError> + From<EM>,
        F: Fn(&str) -> Result<T, EM>,
        T: Eq + Hash,
    {
        let it = s.split(',');
        if trim_intra {
            Self::from_iter(it.map(str::trim), parse_meas)
        } else {
            Self::from_iter(it, parse_meas)
        }
    }
}

impl fmt::Display for Spillover {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let n = self.measurements.len();
        let names = self.measurements.iter().join(",");
        // DMatrix slices are column major, so transpose first to output
        // row-major
        let xs = self.matrix.transpose().as_slice().iter().join(",");
        write!(f, "{n},{names},{xs}")
    }
}

impl FromStrStateful for Spillover {
    type Err = ParseSpilloverError;
    type Payload<'a> = (&'a HashSet<&'a Shortname>, &'a [&'a Shortname]);

    fn from_str_st(
        s: &str,
        data: Self::Payload<'_>,
        conf: &StdTextReadConfig,
    ) -> Result<Self, Self::Err> {
        let (names, ordered_names) = data;
        if conf.parse_indexed_spillover {
            let go = |m: &str| m.parse::<MeasIndex>().map_err(MalformedIndexError);
            let m = GenericSpillover::from_str::<ParseSpilloverError, _, _>(
                s,
                conf.trim_intra_value_whitespace,
                go,
            )?;
            Ok(m.try_into_named(ordered_names)?)
        } else {
            let m = s.parse::<Spillover>()?;
            m.check_link(names)?;
            Ok(m)
        }
    }
}

impl FromStr for Spillover {
    type Err = ParseGenericSpilloverError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        {
            GenericSpillover::from_str(s, false, |m| Ok(Shortname::new_unchecked(m)))
        }
    }
}

#[derive(Debug, Error)]
pub enum NewSpilloverError {
    #[error("Matrix is not square")]
    NonSquare,
    #[error("Name length does not match matrix dimensions")]
    NameLen,
    #[error("Names are not unique")]
    NonUnique,
    #[error("Matrix is less than 2x2")]
    TooSmall,
}

#[derive(From, Debug, Display, Error)]
pub enum ParseSpilloverError {
    Generic(ParseGenericSpilloverError),
    BadIndex(MalformedIndexError),
    IndexLink(SpilloverIndexError),
    NamedLink(LinkedNameError),
}

#[derive(Debug, Error)]
pub enum ParseGenericSpilloverError {
    #[error("{0}")]
    New(NewSpilloverError),
    #[error("Expected {expected} entries, found {total}")]
    WrongLength { total: usize, expected: usize },
    #[error("Float could not be parsed")]
    BadFloat,
    #[error("N could not be parsed")]
    BadN,
}

#[derive(Debug, Error)]
#[error("error when parsing index for $SPILLOVER: {0}")]
pub struct MalformedIndexError(ParseIntError);

#[derive(Debug, Error)]
#[error("$SPILLOVER indices out of bounds: {}", .0.iter().join(","))]
pub struct SpilloverIndexError(NonEmpty<MeasIndex>);

impl OptLinkedKey for Spillover {
    fn names(&self) -> HashSet<&Shortname> {
        self.measurements.iter().collect()
    }

    fn reassign(&mut self, mapping: &NameMapping) {
        // ASSUME mapping is such that new names will be unique
        for n in &mut self.measurements {
            if let Some(new) = mapping.get(n) {
                *n = (*new).clone();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;

    #[test]
    fn test_str_compensation() {
        assert_from_to_str::<Spillover>("2,X,Y,0,0,0,0");
        assert_from_to_str::<Spillover>("3,X,Y,Z,0,0,0,0,0,0,0,0,0");
        assert_from_to_str::<Spillover>("2,X,Y,1.1,1,0,-1.5");
    }

    #[test]
    fn test_str_compensation_unique() {
        assert!("3,Y,Y,Z,0,0,0,0,0,0,0,0,0".parse::<Spillover>().is_err());
    }

    #[test]
    fn test_str_compensation_toosmall() {
        assert!("1,potato,0".parse::<Spillover>().is_err());
    }

    #[test]
    fn test_str_compensation_name_length() {
        assert!("2,moody,padfoot,prongs,0,0,0,0"
            .parse::<Spillover>()
            .is_err());
    }
}

#[cfg(feature = "python")]
mod python {
    use crate::python::macros::impl_value_err;
    use crate::validated::shortname::Shortname;

    use super::{NewSpilloverError, Spillover};

    use numpy::{PyReadonlyArray2, ToPyArray};
    use pyo3::{prelude::*, types::PyTuple};

    // TODO is this ok?
    impl_value_err!(NewSpilloverError);

    impl<'py> FromPyObject<'py> for Spillover {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let (measurements, arr): (Vec<Shortname>, PyReadonlyArray2<f32>) = ob.extract()?;
            let matrix = arr.as_matrix().into_owned();
            Ok(Self::try_new(measurements, matrix)?)
        }
    }

    impl<'py> IntoPyObject<'py> for Spillover {
        type Target = PyTuple;
        type Output = Bound<'py, PyTuple>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            let ms = self.measurements.into_pyobject(py)?;
            let mx = self.matrix.to_pyarray(py);
            (ms, mx).into_pyobject(py)
        }
    }
}

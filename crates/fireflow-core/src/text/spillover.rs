use crate::config::{ConfigFlag as _, ReadStdKeywordsConfig, TrimIntraValueWhitespace};
use crate::text::relational::{KeyToIndexLinkError, RemovedNamedLink};
use crate::validated::keys::Key0;
use crate::validated::shortname::Shortname;

use super::index::MeasIndex;
use super::lookup::FromStrWith;
use super::named_vec::{NameMapping, NamedSet, NamedSetMembership};
use super::relational::{
    ExistingNamedLinkError, KeyToNameLinkError, LinkName, OpticalNamedLinkError,
    OpticalNamesToRemove, TemporalNamedLinkError,
};

use derive_more::{AsRef, Display, From};
use derive_new::new;
use itertools::Itertools as _;
use nalgebra::DMatrix;
use nonempty::NonEmpty;
use std::fmt;
use std::hash::Hash;
use std::mem::take;
use std::num::ParseIntError;
use std::str::FromStr;
use thiserror::Error;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use fireflow_core_proc::DisplayAsPyErr;

/// The $SPILLOVER keyword (3.1+)
pub type Spillover = GenericSpillover<Shortname>;

/// A generic spillover matrix which can include any type for the measurement vector.
///
/// This is to allow parsing indices or names; only the latter is used in the
/// standard but many vendors use indices anyways. This structure allows both
/// to be parsed.
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
    pub(crate) fn reassign(&mut self, mapping: &NameMapping) {
        for n in &mut self.measurements {
            if let Some(new) = mapping.get(n) {
                *n = (*new).clone();
            }
        }
        debug_assert!(
            self.measurements.iter().unique().count() == self.measurements.len(),
            "reassigned names are not unique"
        );
    }

    /// Return error if any about-to-removed names are in spillover measurements
    pub(crate) fn existing_link_error(
        &self,
        names: &OpticalNamesToRemove<'_>,
    ) -> Option<ExistingNamedLinkError<Self, ()>> {
        let ns = self
            .measurements
            .iter()
            .filter(|n| names.as_ref().contains(n))
            .cloned();
        NonEmpty::collect(ns).map(|js| ExistingNamedLinkError::new(Key0::default(), js))
    }

    /// Return error if any names in matrix are not in measurement vector
    pub(crate) fn invalid_link_errors(
        &self,
        names: &NamedSet<'_>,
    ) -> impl Iterator<Item = KeyToNameLinkError<Self>> {
        let mut te = None;
        let ns = self
            .measurements
            .iter()
            .filter(|&n| match names.membership(n) {
                NamedSetMembership::None => true,
                NamedSetMembership::Center => {
                    te = Some(TemporalNamedLinkError::new_i0(n.clone()));
                    false
                }
                NamedSetMembership::NonCenter => false,
            })
            .cloned();
        let oe = NonEmpty::collect(ns)
            .map(OpticalNamedLinkError::new_i0)
            .map(KeyToNameLinkError::Optical);
        [te.map(KeyToNameLinkError::Temporal), oe]
            .into_iter()
            .flatten()
    }

    /// Remove $SPILLOVER if any names in matrix are not in measurement vector
    pub(crate) fn remove_invalid_link(
        src: &mut Option<Self>,
        names: &NamedSet<'_>,
    ) -> Option<RemovedNamedLink<Self>> {
        let s = src.as_ref()?;
        let mut t = None;
        let ns = s
            .measurements
            .iter()
            .filter(|&n| match names.membership(n) {
                NamedSetMembership::None => true,
                NamedSetMembership::Center => {
                    t = Some(n.clone());
                    false
                }
                NamedSetMembership::NonCenter => false,
            })
            .cloned();
        // ASSUME this won't fail since we filter out None above with ?
        NonEmpty::collect(ns)
            .map(|xs| RemovedNamedLink::new(take(src).unwrap(), LinkName::Both(xs, t)))
    }
}

impl GenericSpillover<MeasIndex> {
    pub(crate) fn try_into_named(
        self,
        names: &[&Shortname],
    ) -> Result<Spillover, KeyToIndexLinkError<Spillover>> {
        let mut it = self.measurements.into_iter();
        let mut ms = vec![];
        let mut missing = None;
        for i in it.by_ref() {
            if let Some(&n) = names.get(usize::from(i)) {
                ms.push(n.clone());
            } else {
                missing = Some(i);
                break;
            }
        }
        if let Some(i) = missing {
            let es = NonEmpty::from((i, it.collect::<Vec<_>>()));
            return Err(KeyToIndexLinkError::new_i0(es));
        }
        Ok(Spillover::new(ms, self.matrix))
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
                    Err(ParseGenericSpilloverError::BadFloat.into())
                }
            } else {
                Err(ParseGenericSpilloverError::WrongLength { total, expected }.into())
            }
        } else {
            Err(ParseGenericSpilloverError::BadN.into())
        }
    }

    fn from_str<E, F, EM>(s: &str, trim: TrimIntraValueWhitespace, parse_meas: F) -> Result<Self, E>
    where
        E: From<ParseGenericSpilloverError> + From<EM>,
        F: Fn(&str) -> Result<T, EM>,
        T: Eq + Hash,
    {
        let it = s.split(',');
        if trim.is_set() {
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

impl FromStrWith for Spillover {
    type Err = ParseSpilloverError;
    type Payload<'a> = &'a [&'a Shortname];

    fn from_str_with(
        s: &str,
        ordered_names: Self::Payload<'_>,
        conf: &ReadStdKeywordsConfig,
    ) -> Result<Self, Self::Err> {
        if conf.parse_indexed_spillover.is_set() {
            let go = |m: &str| m.parse::<MeasIndex>().map_err(MalformedIndexError);
            let m = GenericSpillover::from_str::<ParseSpilloverError, _, _>(
                s,
                conf.trim_intra_value_whitespace,
                go,
            )?;
            Ok(m.try_into_named(ordered_names)?)
        } else {
            let m = s.parse::<Self>()?;
            // m.check_link(names)?;
            Ok(m)
        }
    }
}

impl FromStr for Spillover {
    type Err = ParseGenericSpilloverError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_str(s, false.into(), |m| Ok(Shortname::new_unchecked(m)))
    }
}

/// Error when building a new [`Spillover`] value
#[derive(Debug, Error)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::InvalidKeywordValueError))]
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

/// Error when parsing [`Spillover`] from string
#[derive(From, Debug, Display, Error)]
pub enum ParseSpilloverError {
    Generic(ParseGenericSpilloverError),
    BadIndex(MalformedIndexError),
    IndexLink(KeyToIndexLinkError<Spillover>),
}

/// Error when parsing [`GenericSpillover`] from string
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

/// Error when parsing a measurement index in [`Spillover`]
///
/// Note that this is non-standard behavior. $SPILLOVER should refer to $PnN,
/// but many vendors refer to measurements using their indices instead.
#[derive(Debug, Error)]
#[error("error when parsing index for $SPILLOVER: {0}")]
pub struct MalformedIndexError(ParseIntError);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;

    #[test]
    fn str_compensation() {
        assert_from_to_str::<Spillover>("2,X,Y,0,0,0,0");
        assert_from_to_str::<Spillover>("3,X,Y,Z,0,0,0,0,0,0,0,0,0");
        assert_from_to_str::<Spillover>("2,X,Y,1.1,1,0,-1.5");
    }

    #[test]
    fn str_compensation_unique() {
        assert!("3,Y,Y,Z,0,0,0,0,0,0,0,0,0".parse::<Spillover>().is_err());
    }

    #[test]
    fn str_compensation_toosmall() {
        assert!("1,potato,0".parse::<Spillover>().is_err());
    }

    #[test]
    fn str_compensation_name_length() {
        assert!(
            "2,moody,padfoot,prongs,0,0,0,0"
                .parse::<Spillover>()
                .is_err()
        );
    }
}

#[cfg(feature = "python")]
mod python {
    use crate::validated::shortname::Shortname;

    use super::Spillover;

    use numpy::{PyReadonlyArray2, ToPyArray as _};
    use pyo3::{prelude::*, types::PyTuple};

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

use crate::config::{ConfigFlag as _, ReadStdKeywordsConfig, TrimIntraValueWhitespace};
use crate::text::relational::{KeyToIndexLinkError, RemovedNamedLink};
use crate::validated::keys::Key0;
use crate::validated::shortname::Shortname;
use crate::validated::textdelim::{DelimCollisionError, HasDelim, TEXTDelim};

use super::index::MeasIndex;
use super::lookup::{DiagnosedKeyword, FromStrWith, FromStrWithResult, Trimmed};
use super::named_vec::{NameMapping, NamedSet};
use super::relational::{ExistingNamedLinkError, KeyToNameLinkError, OpticalNamesToRemove};

use fireflow_types::config::SpilloverMeasurementMode;
use fireflow_types::nonempty_string::{NEConcat, NEConcat5, ToNE, NEDelim, ToDisplayNE};

use derive_more::{AsRef, Display, From};
use derive_new::new;
use itertools::Itertools as _;
use nalgebra::DMatrix;
use nonempty_collections::{IntoIteratorExt as _, NEVec, iter::NonEmptyIterator as _};
use thiserror::Error;

use std::fmt;
use std::hash::Hash;
use std::num::{NonZeroUsize, ParseIntError};

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {fireflow_core_proc::DisplayAsPyErr, fireflow_types::python as py};

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
    // TODO just use shortname for this because shortname will also match with numbers
    // TODO use NEVec
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
            .cloned()
            .try_into_nonempty_iter();
        ns.map(|js| ExistingNamedLinkError::new(Key0::default(), js.collect()))
    }

    /// Return error if any names in matrix are not in measurement vector
    pub(crate) fn invalid_link_errors(
        &self,
        names: &NamedSet<'_>,
    ) -> impl Iterator<Item = KeyToNameLinkError<Self>> {
        names.invalid_link_errors(&self.measurements)
    }

    /// Remove $SPILLOVER if any names in matrix are not in measurement vector
    pub(crate) fn remove_invalid_link(
        src: &mut Option<Self>,
        names: &NamedSet<'_>,
    ) -> Option<RemovedNamedLink<Self>> {
        let go = |s: &Self| names.error_link_name(&s.measurements);
        RemovedNamedLink::remove_invalid_link(src, go)
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
            let mut es = NEVec::new(i);
            es.extend(it);
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
}

impl<'a> GenericSpillover<&'a str> {
    fn try_from_iter(
        mut xs: impl Iterator<Item = &'a str>,
    ) -> Result<Self, ParseGenericSpilloverError> {
        if let Some(first) = xs.next().and_then(|x| x.parse::<usize>().ok()) {
            let n = first;
            let nn = n * n;
            let expected = n + nn;
            let measurements: Vec<_> = xs.by_ref().take(n).collect();
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
                    Err(ParseGenericSpilloverError::BadFloat)
                }
            } else {
                Err(ParseGenericSpilloverError::WrongLength { total, expected })
            }
        } else {
            Err(ParseGenericSpilloverError::BadN)
        }
    }

    fn from_str(
        s: &'a str,
        trim: TrimIntraValueWhitespace,
    ) -> Result<(Self, bool), ParseGenericSpilloverError> {
        let it = s.split(',');
        if trim.is_set() {
            let mut was_trimmed = false;
            Self::try_from_iter(it.map(|x| {
                let y = str::trim(x);
                was_trimmed = was_trimmed || y.len() < x.len();
                y
            }))
            .map(|x| (x, was_trimmed))
        } else {
            Self::try_from_iter(it).map(|x| (x, false))
        }
    }
}

impl HasDelim for Spillover {
    fn has_delim(&self, d: TEXTDelim) -> Option<DelimCollisionError> {
        self.measurements.iter().find_map(|m| m.has_delim(d))
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

impl<'a> ToDisplayNE<'a> for Spillover {
    type NE = NEConcat5<
        NonZeroUsize,
        char,
        NEDelim<NEVec<ToNE<&'a Shortname>>>,
        char,
        NEDelim<NEVec<f32>>,
    >;
    fn to_ne(&'a self) -> Self::NE {
        let n = NonZeroUsize::new(self.measurements.len()).expect("matrix should be 2x2");
        // TODO this can be cleaned up
        let names = self.measurements[..]
            .try_into_nonempty_iter()
            .expect("matrix should be 2x2")
            .map(ToNE)
            .collect();
        // DMatrix slices are column major, so transpose first to output
        // row-major
        let xs = NEVec::try_from_slice(self.matrix.transpose().as_slice())
            .expect("matrix should be 2x2");
        NEConcat::new(n, ',')
            .append(NEDelim::new(',', names))
            .append(',')
            .append(NEDelim::new(',', xs))
    }
}

impl FromStrWith for Spillover {
    type Err = ParseSpilloverError;
    type Payload<'a> = &'a [&'a Shortname];
    type Diagnostic = Trimmed;
    type Config = ReadStdKeywordsConfig;

    fn from_str_with(
        s: &str,
        ordered_names: Self::Payload<'_>,
        conf: &Self::Config,
    ) -> FromStrWithResult<Self> {
        let trim_flag = conf.trim_intra_value_whitespace;
        let (m, was_trimmed) = GenericSpillover::from_str(s, trim_flag)?;
        let d = was_trimmed.then(|| s.to_owned());
        let use_indices = match conf.spillover_measurement_mode {
            SpilloverMeasurementMode::Guess => m.measurements.iter().all(|x| {
                if let Ok(i) = x.parse::<MeasIndex>() {
                    let n = Shortname::new_unchecked(i.to_string());
                    !ordered_names.contains(&&n)
                } else {
                    false
                }
            }),
            SpilloverMeasurementMode::Indexed => true,
            SpilloverMeasurementMode::Named => false,
        };
        let ret = if use_indices {
            let new_ms = m
                .measurements
                .into_iter()
                .map(|x| x.parse::<MeasIndex>().map_err(MalformedIndexError))
                .collect::<Result<Vec<_>, _>>()?;
            GenericSpillover::new(new_ms, m.matrix).try_into_named(ordered_names)?
        } else {
            let new_ms = m
                .measurements
                .into_iter()
                .map(Shortname::new_unchecked)
                .collect();
            Self::new(new_ms, m.matrix)
        };
        Ok(DiagnosedKeyword::new(ret, d))
    }
}

/// Error when building a new [`Spillover`] value
#[derive(Debug, Error)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::InvalidKeywordValueError))]
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
    fn spillover() {
        let conf = ReadStdKeywordsConfig::default();
        let ns = [
            &"X".parse::<Shortname>().unwrap(),
            &"Y".parse::<Shortname>().unwrap(),
        ];
        assert_from_to_str_with::<Spillover>("2,X,Y,0,0,0,0", &ns, &conf);
        assert_from_to_str_with::<Spillover>("3,X,Y,Z,0,0,0,0,0,0,0,0,0", &ns, &conf);
        assert_from_to_str_with::<Spillover>("2,X,Y,1.1,1,0,-1.5", &ns, &conf);
    }

    #[test]
    fn spillover_indexed() {
        let conf = ReadStdKeywordsConfig {
            spillover_measurement_mode: SpilloverMeasurementMode::Indexed,
            ..Default::default()
        };
        let ns = [
            &"X".parse::<Shortname>().unwrap(),
            &"Y".parse::<Shortname>().unwrap(),
        ];
        let res = Spillover::from_str_with("2,1,2,0,0,0,0", &ns, &conf);
        let spill = res.unwrap().native.to_string();
        assert_eq!(spill.as_str(), "2,X,Y,0,0,0,0");
    }

    #[test]
    fn spillover_guess_indexed() {
        let conf = ReadStdKeywordsConfig {
            spillover_measurement_mode: SpilloverMeasurementMode::Guess,
            ..Default::default()
        };
        let ns = [
            &"X".parse::<Shortname>().unwrap(),
            &"Y".parse::<Shortname>().unwrap(),
        ];
        let res = Spillover::from_str_with("2,1,2,0,0,0,0", &ns, &conf);
        let spill = res.unwrap().native.to_string();
        assert_eq!(spill.as_str(), "2,X,Y,0,0,0,0");
    }

    #[test]
    fn spillover_guess_named() {
        let conf = ReadStdKeywordsConfig {
            spillover_measurement_mode: SpilloverMeasurementMode::Guess,
            ..Default::default()
        };
        let ns = [
            &"X".parse::<Shortname>().unwrap(),
            &"Y".parse::<Shortname>().unwrap(),
        ];
        assert_from_to_str_with::<Spillover>("2,X,Y,0,0,0,0", &ns, &conf);
    }

    #[test]
    fn spillover_trimmed() {
        let conf = ReadStdKeywordsConfig {
            trim_intra_value_whitespace: true.into(),
            ..Default::default()
        };
        let ns = [
            &"X".parse::<Shortname>().unwrap(),
            &"Y".parse::<Shortname>().unwrap(),
        ];
        let res = Spillover::from_str_with("2, X,  Y , 0, 0,    0, 0", &ns, &conf);
        let spill = res.unwrap().native.to_string();
        assert_eq!(spill.as_str(), "2,X,Y,0,0,0,0");
    }

    #[test]
    fn spillover_nonunique() {
        let conf = ReadStdKeywordsConfig::default();
        let ns = [
            &"X".parse::<Shortname>().unwrap(),
            &"Y".parse::<Shortname>().unwrap(),
        ];
        assert!(Spillover::from_str_with("3,Y,Y,Z,0,0,0,0,0,0,0,0,0", &ns, &conf).is_err());
    }

    #[test]
    fn spillover_toosmall() {
        let conf = ReadStdKeywordsConfig::default();
        let ns = [&"potato".parse::<Shortname>().unwrap()];
        assert!(Spillover::from_str_with("1,potato,0", &ns, &conf).is_err());
    }

    #[test]
    fn spillover_name_wrong_length() {
        let conf = ReadStdKeywordsConfig::default();
        let ns = [
            &"moody".parse::<Shortname>().unwrap(),
            &"padfoot".parse::<Shortname>().unwrap(),
            &"prongs".parse::<Shortname>().unwrap(),
        ];
        assert!(Spillover::from_str_with("2,moody,padfoot,prongs,0,0,0,0", &ns, &conf).is_err());
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

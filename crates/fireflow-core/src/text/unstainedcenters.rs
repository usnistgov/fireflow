use crate::validated::{keys, shortname::*};

use super::named_vec::NameMapping;
use super::optional::ClearMaybe;
use super::parser::OptLinkedKey;

use derive_more::Into;
use itertools::Itertools;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::str::FromStr;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use pyo3::prelude::*;

/// The value for the $UNSTAINEDCENTERS key (3.2+)
#[derive(Clone, Into, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
pub struct UnstainedCenters(HashMap<Shortname, f32>);

pub enum UnstainedCenterError {
    NonUnique,
    Empty,
}

pub enum ParseUnstainedCenterError {
    BadFloat,
    BadLength { total: usize, expected: usize },
    BadN,
    New(UnstainedCenterError),
}

impl TryFrom<Vec<(Shortname, f32)>> for UnstainedCenters {
    type Error = UnstainedCenterError;

    fn try_from(value: Vec<(Shortname, f32)>) -> Result<Self, Self::Error> {
        let n = value.len();
        if value.iter().map(|x| &x.0).unique().count() < n {
            Err(UnstainedCenterError::NonUnique)
        } else if n == 0 {
            Err(UnstainedCenterError::Empty)
        } else {
            Ok(Self(value.into_iter().collect()))
        }
    }
}

impl UnstainedCenters {
    pub fn new_1(k: Shortname, v: f32) -> Self {
        Self([(k, v)].into_iter().collect())
    }

    pub fn inner(&self) -> &HashMap<Shortname, f32> {
        &self.0
    }

    pub(crate) fn names_difference(
        &self,
        names: &HashSet<&Shortname>,
    ) -> impl Iterator<Item = &Shortname> {
        self.0.keys().filter(|n| !names.contains(n))
    }
}

impl FromStr for UnstainedCenters {
    type Err = ParseUnstainedCenterError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut xs = s.split(",");
        if let Some(n) = xs.next().and_then(|x| x.parse().ok()) {
            // This should be safe since we are splitting by commas
            let measurements: Vec<_> = xs.by_ref().take(n).map(Shortname::new_unchecked).collect();
            let values: Vec<_> = xs.by_ref().take(n).collect();
            let remainder = xs.by_ref().count();
            let total = values.len() + measurements.len() + remainder;
            let expected = 2 * n;
            if total == expected {
                let fvalues: Vec<_> = values
                    .into_iter()
                    .filter_map(|x| x.parse::<f32>().ok())
                    .collect();
                if fvalues.len() != n {
                    Err(ParseUnstainedCenterError::BadFloat)
                } else {
                    let ys: Vec<_> = measurements.into_iter().zip(fvalues).collect();
                    UnstainedCenters::try_from(ys).map_err(ParseUnstainedCenterError::New)
                }
            } else {
                Err(ParseUnstainedCenterError::BadLength { total, expected })
            }
        } else {
            Err(ParseUnstainedCenterError::BadN)
        }
    }
}

impl fmt::Display for UnstainedCenters {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let n = self.0.len();
        let (ms, vs): (Vec<&Shortname>, Vec<f32>) = self.0.iter().unzip();
        write!(f, "{n},{},{}", ms.iter().join(","), vs.iter().join(","))
    }
}

impl fmt::Display for UnstainedCenterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let s = match self {
            UnstainedCenterError::NonUnique => "Names are not unique",
            UnstainedCenterError::Empty => "Unstained centers must not be empty",
        };
        write!(f, "{}", s)
    }
}

impl fmt::Display for ParseUnstainedCenterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            ParseUnstainedCenterError::BadFloat => write!(f, "Error parsing float value(s)"),
            ParseUnstainedCenterError::BadLength { total, expected } => {
                write!(f, "Expected {expected} values, found {total}")
            }
            ParseUnstainedCenterError::BadN => write!(f, "Could not parse N"),
            ParseUnstainedCenterError::New(n) => n.fmt(f),
        }
    }
}

// TODO define in same mode as type
impl OptLinkedKey for UnstainedCenters {
    fn names(&self) -> HashSet<&Shortname> {
        self.0.keys().collect()
    }

    fn reassign(&mut self, mapping: &NameMapping) {
        // keys can't be mutated in place so need to rebuild the hashmap with
        // new keys from the mapping
        let new: HashMap<_, _> = self
            .0
            .iter()
            .map(|(k, v)| {
                (
                    mapping.get(k).map(|x| (*x).clone()).unwrap_or(k.clone()),
                    *v,
                )
            })
            .collect();
        self.0 = new;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;

    // TODO this is hard(er) to test since the order will be random
    #[test]
    fn test_unstained_centers() {
        assert_from_to_str::<UnstainedCenters>("1,X,0");
    }

    #[test]
    fn test_unstained_centers_wrong_len() {
        assert!("2,X,0".parse::<UnstainedCenters>().is_err());
    }

    #[test]
    fn test_unstained_centers_nonunique() {
        assert!("3,Y,Y,Z,0,0,0".parse::<UnstainedCenters>().is_err());
    }
}

#[cfg(feature = "python")]
mod python {
    use super::{UnstainedCenterError, UnstainedCenters};
    use crate::python::macros::impl_pyreflow_err;
    use crate::validated::shortname::Shortname;

    use pyo3::prelude::*;
    use std::collections::HashMap;

    impl_pyreflow_err!(UnstainedCenterError);

    impl<'py> FromPyObject<'py> for UnstainedCenters {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let us: HashMap<Shortname, f32> = ob.extract()?;
            Ok(us.into_iter().collect::<Vec<_>>().try_into()?)
        }
    }
}

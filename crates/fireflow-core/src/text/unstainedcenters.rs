use crate::macros::newtype_from_outer;
use crate::validated::shortname::*;

use super::named_vec::NameMapping;
use super::optionalkw::ClearOptional;
use super::parser::OptLinkedKey;

use itertools::Itertools;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::str::FromStr;

/// The value for the $UNSTAINEDCENTERS key (3.2+)
#[derive(Clone, Serialize)]
pub struct UnstainedCenters(HashMap<Shortname, f32>);

newtype_from_outer!(UnstainedCenters, HashMap<Shortname, f32>);

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

impl UnstainedCenters {
    pub fn new(pairs: Vec<(Shortname, f32)>) -> Result<Self, UnstainedCenterError> {
        let n = pairs.len();
        if pairs.iter().map(|x| &x.0).unique().count() < n {
            Err(UnstainedCenterError::NonUnique)
        } else if n == 0 {
            Err(UnstainedCenterError::Empty)
        } else {
            Ok(Self(pairs.into_iter().collect()))
        }
    }

    pub fn new_1(k: Shortname, v: f32) -> Self {
        Self([(k, v)].into_iter().collect())
    }

    pub fn inner(&self) -> &HashMap<Shortname, f32> {
        &self.0
    }

    pub(crate) fn insert(&mut self, k: Shortname, v: f32) -> Option<f32> {
        self.0.insert(k, v)
    }

    pub(crate) fn remove(&mut self, n: &Shortname) -> Result<Option<f32>, ClearOptional> {
        if self.0.len() == 1 {
            Err(ClearOptional)
        } else {
            Ok(self.0.remove(n))
        }
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
            if total != expected {
                let fvalues: Vec<_> = values
                    .into_iter()
                    .filter_map(|x| x.parse::<f32>().ok())
                    .collect();
                if fvalues.len() != n {
                    Err(ParseUnstainedCenterError::BadFloat)
                } else {
                    UnstainedCenters::new(measurements.into_iter().zip(fvalues).collect())
                        .map_err(ParseUnstainedCenterError::New)
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

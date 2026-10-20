use crate::text::lookup::{FromStrDelim, impl_from_str_with_delim};

use fireflow_types::ranged_float::PositiveFloat;
use nonempty::{NEConcat, NEConcat5, NEStr, ToDisplayNE, ne_str};

use thiserror::Error;

use std::num::ParseFloatError;

#[cfg(feature = "python")]
use {fireflow_core_proc::DisplayAsPyErr, fireflow_types::python as py};

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(test)]
use proptest_derive::Arbitrary;

/// The value for the $PnD key (3.1+)
#[derive(Clone, Copy, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(test, derive(Arbitrary))]
pub enum Display {
    /// Linear display (value like `"Linear,<lower>,<upper>"`)
    Lin(LinearDisplay),

    /// Logarithmic display (value like `"Logarithmic,<decades>,<offset>"`)
    Log {
        decades: PositiveFloat,
        offset: PositiveFloat,
    },
}

/// The value for the $PnD key when linear (3.1+)
#[derive(Clone, Copy, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct LinearDisplay {
    lower: f32,
    upper: f32,
}

impl ToDisplayNE<'_> for Display {
    type NE = NEConcat5<&'static NEStr, char, f32, char, f32>;
    fn to_ne(&self) -> Self::NE {
        let (m, x, y) = match self {
            Self::Lin(x) => (ne_str!("Linear"), x.lower, x.upper),
            Self::Log { offset, decades } => (
                ne_str!("Logarithmic"),
                f32::from(*decades),
                f32::from(*offset),
            ),
        };
        NEConcat::new(m, ',').append(x).append(',').append(y)
    }
}

impl FromStrDelim for Display {
    type Err = ParseDisplayError;
    const DELIM: char = ',';

    fn from_iter<'a>(mut iter: impl Iterator<Item = &'a str>) -> Result<Self, Self::Err> {
        let x0 = iter.next();
        let x1 = iter.next();
        let x2 = iter.next();
        let x3 = iter.next();
        match (x0, x1, x2, x3) {
            (Some(which), Some(s1), Some(s2), None) => {
                let f1 = s1.parse().map_err(ParseDisplayError::FloatError)?;
                let f2 = s2.parse().map_err(ParseDisplayError::FloatError)?;
                match which {
                    "Linear" => LinearDisplay::try_new(f1, f2)
                        .map_err(ParseDisplayError::Linear)
                        .map(Self::Lin),
                    "Logarithmic" => match (f1.try_into(), f2.try_into()) {
                        (Ok(decades), Ok(offset)) => Ok(Self::Log { decades, offset }),
                        _ => Err(ParseDisplayError::Log(f1, f2)),
                    },
                    _ => Err(ParseDisplayError::InvalidType),
                }
            }
            _ => Err(ParseDisplayError::FormatError),
        }
    }
}

impl_from_str_with_delim!(Display, ParseDisplayError);

impl LinearDisplay {
    fn try_new(lower: f32, upper: f32) -> Result<Self, NewLinearDisplayError> {
        if lower <= upper {
            Ok(Self { lower, upper })
        } else {
            Err(NewLinearDisplayError { lower, upper })
        }
    }
}

/// Error when parsing [`enum@Display`] from string
#[derive(Debug, Error, PartialEq, Clone)]
pub enum ParseDisplayError {
    #[error("{0}")]
    FloatError(ParseFloatError),
    #[error("Type must be either 'Logarithmic' or 'Linear'")]
    InvalidType,
    #[error("must be like 'string,f1,f2'")]
    FormatError,
    #[error("{0}")]
    Linear(NewLinearDisplayError),
    #[error("log must only use positive floats, got 'Logarithmic,{0},{1}'")]
    Log(f32, f32),
}

#[derive(Debug, Error, PartialEq, Clone)]
#[error("linear bounds out of order, got 'Linear,{lower},{upper}'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::InvalidKeywordValueError))]
pub struct NewLinearDisplayError {
    lower: f32,
    upper: f32,
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::config::EvaledReadStdKeywordsConfig;
    use crate::test::*;
    use crate::text::lookup::FromStrWith as _;

    use assert_matches::assert_matches;
    use proptest::prelude::*;

    // don't derive this since it is better for debug output if this is shorter,
    // and it isn't necessary for it to be anything more than 0-2 elements; this
    // is also more realistic in terms of the values that will really be seen in
    // the wild
    impl Arbitrary for LinearDisplay {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;
        fn arbitrary_with((): Self::Parameters) -> Self::Strategy {
            any::<(f32, f32)>()
                .prop_map(|(x, y)| {
                    let (a, b) = if x <= y { (x, y) } else { (y, x) };
                    Self::try_new(a, b).unwrap()
                })
                .boxed()
        }
    }

    #[test]
    fn pnd() {
        let conf = EvaledReadStdKeywordsConfig::default();
        assert_from_to_str_with::<Display>(ne_str!("Linear,0,1"), (), &conf);
        assert_from_to_str_with::<Display>(ne_str!("Logarithmic,1,1"), (), &conf);
        assert_from_to_str_with::<Display>(ne_str!("Logarithmic,1,0.1"), (), &conf);

        macro_rules! go {
            ($s:expr) => {
                Display::from_str_with(ne_str!($s), (), &conf)
            };
        }

        assert_matches!(go!("Linear,x,x"), Err(ParseDisplayError::FloatError(_)));
        assert_eq!(go!("LIN,0,1"), Err(ParseDisplayError::InvalidType));
        assert_eq!(go!("LOG,1,1"), Err(ParseDisplayError::InvalidType));
        assert_eq!(go!("Logicle,0,1,2,3"), Err(ParseDisplayError::FormatError));
        assert_eq!(
            go!("Linear,1.0,-1.0"),
            Err(ParseDisplayError::Linear(NewLinearDisplayError {
                lower: 1.0,
                upper: -1.0
            }))
        );
        assert_eq!(
            go!("Logarithmic,-1.0,1.0"),
            Err(ParseDisplayError::Log(-1.0, 1.0))
        );
    }

    #[test]
    fn pnd_commas() {
        let v = ne_str!("Linear, 0 , 1");
        let mut conf = EvaledReadStdKeywordsConfig::default();
        assert_matches!(
            Display::from_str_with(v, (), &conf),
            Err(ParseDisplayError::FloatError(_))
        );
        conf.trim_intra_value_whitespace = true.into();
        assert_from_to_str_almost_with::<Display>(v, "Linear,0,1", (), &conf);
    }
}

#[cfg(feature = "python")]
mod python {
    use super::{Display, LinearDisplay};

    use pyo3::prelude::*;
    use pyo3::types::PyTuple;

    // $PnD (3.1+) as a tuple like (bool, f32, f32) in python where 'bool' is true
    // if linear
    impl<'py> FromPyObject<'_, 'py> for Display {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            let (is_log, x0, x1): (bool, f32, f32) = obj.extract()?;
            let ret = if is_log {
                Self::Log {
                    offset: x0.try_into()?,
                    decades: x1.try_into()?,
                }
            } else {
                Self::Lin(LinearDisplay::try_new(x0, x1)?)
            };
            Ok(ret)
        }
    }

    impl<'py> IntoPyObject<'py> for Display {
        type Target = PyTuple;
        type Output = Bound<'py, <(bool, f32, f32) as IntoPyObject<'py>>::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            let ret = match self {
                Self::Lin(x) => (false, x.lower, x.upper),
                Self::Log { offset, decades } => (true, offset.into(), decades.into()),
            };
            ret.into_pyobject(py)
        }
    }
}

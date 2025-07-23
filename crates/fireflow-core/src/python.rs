use crate::config::TimePattern;
use crate::core::ScaleTransform;
use crate::header::{Version, VersionError};
use crate::text::keywords::{
    AlphaNumType, AlphaNumTypeError, Calibration3_1, Calibration3_2, Display, Feature,
    FeatureError, Mode, ModeError, NumType, NumTypeError, OpticalType, OpticalTypeError,
    Originality, OriginalityError, Unicode,
};
use crate::text::ranged_float::{NonNegFloat, PositiveFloat, RangedFloatError};
use crate::text::scale::{LogRangeError, Scale};
use crate::validated::ascii_range::{Chars, CharsError};
use crate::validated::datepattern::{DatePattern, DatePatternError};
use crate::validated::keys::{
    NonStdKey, NonStdKeyError, NonStdMeasPattern, NonStdMeasPatternError, StdKey, StdKeyError,
};
use crate::validated::shortname::{Shortname, ShortnameError};

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyFloat, PyString, PyTuple};
use pyo3::IntoPyObjectExt;
use std::convert::Infallible;

// Convert any error to a python ValueError using its display trait
macro_rules! impl_value_err {
    ($t:ident) => {
        impl From<$t> for PyErr {
            fn from(value: $t) -> Self {
                PyValueError::new_err(value.to_string())
            }
        }
    };
}

macro_rules! impl_try_from_py {
    ($t:ident, $inner:ident) => {
        impl<'py> FromPyObject<'py> for $t {
            fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
                let x: $inner = ob.extract()?;
                let y = x.try_into()?;
                Ok(y)
            }
        }
    };
}

// Convert string to rust type using FromStr trait; useful for traits which
// may wrap String but have some custom validation for its contents.
macro_rules! impl_str_from_py {
    ($t:ident) => {
        impl<'py> FromPyObject<'py> for $t {
            fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
                let x: String = ob.extract()?;
                let ret = x.parse()?;
                Ok(ret)
            }
        }
    };
}

// Convert rust type to python type using its Display trait
macro_rules! impl_str_to_py {
    ($t:ident) => {
        impl<'py> IntoPyObject<'py> for $t {
            type Target = PyString;
            type Output = Bound<'py, Self::Target>;
            type Error = Infallible;

            fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
                self.to_string().into_pyobject(py)
            }
        }
    };
}

macro_rules! impl_str_to_from_py {
    ($t:ident) => {
        impl_str_from_py!($t);
        impl_str_to_py!($t);
    };
}

impl_str_from_py!(NonStdMeasPattern);
impl_value_err!(NonStdMeasPatternError);

impl_str_from_py!(DatePattern);
impl_value_err!(DatePatternError);

impl_str_from_py!(Shortname);
impl_value_err!(ShortnameError);

impl_str_to_from_py!(Version);
impl_value_err!(VersionError);

impl_str_to_from_py!(Originality);
impl_value_err!(OriginalityError);

impl_str_to_from_py!(AlphaNumType);
impl_value_err!(AlphaNumTypeError);

impl_str_to_from_py!(NumType);
impl_value_err!(NumTypeError);

impl_str_to_from_py!(Feature);
impl_value_err!(FeatureError);

impl_str_to_from_py!(Mode);
impl_value_err!(ModeError);

impl_str_to_from_py!(OpticalType);
impl_value_err!(OpticalTypeError);

impl_str_to_from_py!(StdKey);
impl_value_err!(StdKeyError);

impl_str_to_from_py!(NonStdKey);
impl_value_err!(NonStdKeyError);

impl_value_err!(CharsError);

impl<'py> FromPyObject<'py> for Chars {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let x: u8 = ob.extract()?;
        let ret = Chars::try_from(x)?;
        Ok(ret)
    }
}

impl<'py> FromPyObject<'py> for TimePattern {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let s: String = ob.extract()?;
        let n = s
            .parse::<TimePattern>()
            // this should be an error from regexp parsing
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(n)
    }
}

impl_value_err!(RangedFloatError);
impl_try_from_py!(PositiveFloat, f32);
impl_try_from_py!(NonNegFloat, f32);

impl<'py> IntoPyObject<'py> for PositiveFloat {
    type Target = PyFloat;
    type Output = Bound<'py, <f32 as IntoPyObject<'py>>::Target>;
    type Error = Infallible;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        f32::from(self).into_pyobject(py)
    }
}

impl<'py> IntoPyObject<'py> for NonNegFloat {
    type Target = PyFloat;
    type Output = Bound<'py, <f32 as IntoPyObject<'py>>::Target>;
    type Error = Infallible;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        f32::from(self).into_pyobject(py)
    }
}

// $PnCALIBRATION (3.1) as (f32, String) tuple in python
impl<'py> FromPyObject<'py> for Calibration3_1 {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let (slope, unit): (PositiveFloat, String) = ob.extract()?;
        Ok(Self { slope, unit })
    }
}

impl<'py> IntoPyObject<'py> for Calibration3_1 {
    type Target = PyTuple;
    type Output = Bound<'py, <(PositiveFloat, String) as IntoPyObject<'py>>::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (self.slope, self.unit).into_pyobject(py)
    }
}

// $PnCALIBRATION (3.2) as (f32, f32, String) tuple in python
impl<'py> FromPyObject<'py> for Calibration3_2 {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let (slope, offset, unit): (PositiveFloat, f32, String) = ob.extract()?;
        Ok(Self {
            slope,
            offset,
            unit,
        })
    }
}

impl<'py> IntoPyObject<'py> for Calibration3_2 {
    type Target = PyTuple;
    type Output = Bound<'py, <(PositiveFloat, f32, String) as IntoPyObject<'py>>::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (self.slope, self.offset, self.unit).into_pyobject(py)
    }
}

// $PnE (2.0) as either () or (f32, f32) tuples in python
impl<'py> FromPyObject<'py> for Scale {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        if ob.is_instance_of::<PyTuple>() && ob.len()? == 0 {
            Ok(Self::Linear)
        } else {
            let (decades, offset): (f32, f32) = ob.extract()?;
            let ret = Self::try_new_log(decades, offset)?;
            Ok(ret)
        }
    }
}

impl<'py> IntoPyObject<'py> for Scale {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            Self::Linear => Ok(PyTuple::empty(py).into_any()),
            Self::Log(l) => (f32::from(l.decades), f32::from(l.offset)).into_bound_py_any(py),
        }
    }
}

impl_value_err!(LogRangeError);

// $PnE/$PnG (3.0+) as a tuple like (f32) or (f32, f32) in python
impl<'py> FromPyObject<'py> for ScaleTransform {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        if let Ok(gain) = ob.extract::<PositiveFloat>() {
            Ok(Self::Lin(gain))
        } else if let Ok(log) = ob.extract::<(f32, f32)>()?.try_into() {
            Ok(Self::Log(log))
        } else {
            // TODO make this into a general "argument value error"
            Err(PyValueError::new_err(
                "scale transform must be a positive \
                     float or a 2-tuple of positive floats",
            ))
        }
    }
}

impl<'py> IntoPyObject<'py> for ScaleTransform {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            Self::Lin(gain) => f32::from(gain).into_bound_py_any(py),
            Self::Log(l) => (f32::from(l.decades), f32::from(l.offset)).into_bound_py_any(py),
        }
    }
}

// $UNICODE (3.0) as a tuple like (f32, [String]) in python
impl<'py> FromPyObject<'py> for Unicode {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let (page, kws): (u32, Vec<String>) = ob.extract()?;
        Ok(Self { page, kws })
    }
}

impl<'py> IntoPyObject<'py> for Unicode {
    type Target = PyTuple;
    type Output = Bound<'py, <(u32, Vec<String>) as IntoPyObject<'py>>::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        (self.page, self.kws).into_pyobject(py)
    }
}

// $PnD (3.1+) as a tuple like (bool, f32, f32) in python where 'bool' is true
// if linear
impl<'py> FromPyObject<'py> for Display {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let (is_log, x0, x1): (bool, f32, f32) = ob.extract()?;
        let ret = if is_log {
            Self::Log {
                offset: x0,
                decades: x1,
            }
        } else {
            Self::Lin {
                lower: x0,
                upper: x1,
            }
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
            Self::Lin { lower, upper } => (false, lower, upper),
            Self::Log { offset, decades } => (true, offset, decades),
        };
        ret.into_pyobject(py)
    }
}

use derive_new::new;

use std::marker::PhantomData;

#[cfg(feature = "serde")]
use serde::Serialize;

/// Denotes a correction for a segment offset pair
#[derive(Default, Clone, Copy, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct OffsetsCorrection<I, S> {
    begin: i32,
    end: i32,
    #[cfg_attr(feature = "serde", serde(skip))]
    _id: PhantomData<I>,
    #[cfg_attr(feature = "serde", serde(skip))]
    _src: PhantomData<S>,
}

pub type HeaderCorrection<I> = OffsetsCorrection<I, OffsetsFromHeader>;
pub type TEXTCorrection<I> = OffsetsCorrection<I, OffsetsFromTEXT>;

/// Denotes segment offsets came from HEADER
#[derive(Default, Debug, Clone, Copy, PartialEq)]
pub struct OffsetsFromHeader;

/// Denotes segment offsets came from TEXT
#[derive(Default, Debug, Clone, Copy, PartialEq)]
pub struct OffsetsFromTEXT;

/// Denotes segment offsets pertains to primary TEXT
#[derive(Default, Debug, Clone, Copy, PartialEq)]
pub struct PrimaryTextSegmentId;

/// Denotes segment offsets pertains to supplemental TEXT
#[derive(Default, Debug, Clone, Copy, PartialEq)]
pub struct SupplementalTextSegmentId;

/// Denotes segment offsets pertains to DATA
#[derive(Default, Debug, Clone, Copy, PartialEq)]
pub struct DataSegmentId;

/// Denotes segment offsets pertains to ANALYSIS
#[derive(Default, Debug, Clone, Copy, PartialEq)]
pub struct AnalysisSegmentId;

/// Denotes segment offsets pertains to OTHER
#[derive(Default, Debug, Clone, Copy, PartialEq)]
pub struct OtherSegmentId;

// Implement methods for correction type

impl<I, S> OffsetsCorrection<I, S> {
    #[must_use]
    pub fn begin(&self) -> i32 {
        self.begin
    }

    #[must_use]
    pub fn end(&self) -> i32 {
        self.end
    }
}

impl<I, S> From<(i32, i32)> for OffsetsCorrection<I, S> {
    fn from(value: (i32, i32)) -> Self {
        Self::new(value.0, value.1)
    }
}

impl<I, S> From<(Option<i32>, Option<i32>)> for OffsetsCorrection<I, S> {
    fn from(value: (Option<i32>, Option<i32>)) -> Self {
        Self::from((value.0.unwrap_or_default(), value.1.unwrap_or_default()))
    }
}

#[cfg(feature = "python")]
mod python {
    use super::OffsetsCorrection;

    use pyo3::{prelude::*, types::PyTuple};

    // offset corrections will be tuples like (int, int)
    impl<'py, I, S> FromPyObject<'_, 'py> for OffsetsCorrection<I, S> {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            let t: (i32, i32) = obj.extract()?;
            Ok(Self::from(t))
        }
    }

    impl<'py, I, S> IntoPyObject<'py> for OffsetsCorrection<I, S> {
        type Target = PyTuple;
        type Output = Bound<'py, <(u64, u64) as IntoPyObject<'py>>::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            (self.begin, self.end).into_pyobject(py)
        }
    }
}

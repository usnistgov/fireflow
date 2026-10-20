use thiserror::Error;

#[cfg(feature = "python")]
use {fireflow_core_proc::DisplayAsPyErr, fireflow_types::python as py};

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(test)]
use proptest::prelude::*;

/// The value for the $PnD key when linear (3.1+)
#[derive(Clone, Copy, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct LinearDisplay {
    lower: f32,
    upper: f32,
}

impl LinearDisplay {
    pub fn try_new(lower: f32, upper: f32) -> Result<Self, NewLinearDisplayError> {
        if lower <= upper {
            Ok(Self { lower, upper })
        } else {
            Err(NewLinearDisplayError { lower, upper })
        }
    }

    #[must_use]
    pub fn lower(&self) -> f32 {
        self.lower
    }

    #[must_use]
    pub fn upper(&self) -> f32 {
        self.upper
    }
}

#[derive(Debug, Error, PartialEq, Clone)]
#[error("linear bounds out of order, got 'Linear,{lower},{upper}'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::InvalidKeywordValueError))]
pub struct NewLinearDisplayError {
    pub lower: f32,
    pub upper: f32,
}

#[cfg(test)]
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

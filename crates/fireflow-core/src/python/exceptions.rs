use crate::logging::{
    CommutativeResultExt, LogResult, LogResultExt, NowarnExt, NullFamily, ResolvableExt, ZeroOrMore,
};

use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyWarning};
use pyo3::prelude::*;
use std::convert::Infallible;
use std::ffi::CString;
use std::fmt;

create_exception!(
    _pyreflow,
    PyreflowException,
    PyException,
    "Exception created by internal pyreflow."
);

create_exception!(
    _pyreflow,
    PyreflowWarning,
    PyWarning,
    "Warning created by internal pyreflow."
);

pub trait PyResultExt: LogResultExt {
    fn py_termfail_resolve(self) -> PyResult<Self::V>
    where
        Self: CommutativeResultExt + ResolvableExt,
        Self::LW: fmt::Display,
        Self::E: Into<PyErr>,
    {
        let (warn, res) = self.resolve_cmt(emit_warnings, Into::into);
        warn?;
        res
    }

    fn py_termfail_resolve_nowarn(self) -> PyResult<Self::V>
    where
        Self: NowarnExt + ResolvableExt,
        Self::E: Into<PyErr>,
    {
        self.resolve_nowarn(Into::into)
    }

    fn py_term_resolve_noerror(self) -> PyResult<Self::V>
    where
        Self::LW: fmt::Display,
        Self: LogResultExt<E = Infallible, P = ()>,
    {
        let (value, warn) = self.infallible_with_warn_into(emit_warnings);
        warn?;
        Ok(value)
    }
}

impl<V, P, E, LW, RW, LWC: ZeroOrMore, RWC: ZeroOrMore> PyResultExt
    for LogResult<V, P, LW, RW, E, LWC, RWC, NullFamily>
{
}

fn emit_warnings<W>(ws: impl IntoIterator<Item = W>) -> PyResult<()>
where
    W: fmt::Display,
{
    Python::with_gil(|py| -> PyResult<()> {
        let wt = py.get_type::<PyreflowWarning>();
        for w in ws {
            let s = CString::new(w.to_string())?;
            PyErr::warn(py, &wt, &s, 0)?;
        }
        Ok(())
    })
}

use crate::error::{Terminal, TerminalFailure, TerminalResult};

use nonempty::NonEmpty;
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

pub trait PyTerminalResultExt {
    type V;

    fn py_term_resolve(self) -> PyResult<Self::V>;
}

impl<V, W: fmt::Display, E: fmt::Display, T: fmt::Display> PyTerminalResultExt
    for TerminalResult<V, W, E, T>
{
    type V = V;

    fn py_term_resolve(self) -> PyResult<Self::V> {
        self.map_or_else(|e| Err(handle_failure(e)), handle_warnings)
    }
}

pub trait PyTerminalNoWarnResultExt {
    type V;

    fn py_term_resolve_nowarn(self) -> PyResult<Self::V>;
}

impl<V, E: fmt::Display, T: fmt::Display> PyTerminalNoWarnResultExt
    for TerminalResult<V, Infallible, E, T>
{
    type V = V;

    fn py_term_resolve_nowarn(self) -> PyResult<Self::V> {
        self.map_err(handle_failure_nowarn).map(|x| x.inner())
    }
}

fn handle_warnings<X, W>(t: Terminal<X, W>) -> PyResult<X>
where
    W: fmt::Display,
{
    let (x, warn_res) = t.resolve(emit_warnings);
    warn_res?;
    Ok(x)
}

fn emit_warnings<W>(ws: Vec<W>) -> PyResult<()>
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

// TODO python has a way of handling multiple exceptions (ExceptionGroup)
// starting in 3.11
fn handle_failure<W, E, T>(f: TerminalFailure<W, E, T>) -> PyErr
where
    E: fmt::Display,
    T: fmt::Display,
    W: fmt::Display,
{
    let (warn_res, e) = f.resolve(emit_warnings, emit_failure);
    if let Err(w) = warn_res {
        w
    } else {
        e
    }
}

fn handle_failure_nowarn<E, T>(f: TerminalFailure<Infallible, E, T>) -> PyErr
where
    E: fmt::Display,
    T: fmt::Display,
{
    f.resolve(|_| (), emit_failure).1
}

fn emit_failure<E, T>(es: NonEmpty<E>, r: T) -> PyErr
where
    E: fmt::Display,
    T: fmt::Display,
{
    let s = {
        let xs: Vec<_> = [format!("Toplevel Error: {r}")]
            .into_iter()
            .chain(es.into_iter().map(|x| x.to_string()))
            .collect();
        xs[..].join("\n").to_string()
    };
    PyreflowException::new_err(s)
}

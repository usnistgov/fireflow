use crate::error::{GenNonEmpty, Terminal, TerminalFailure, TerminalResult, VecFamily};

use itertools::Itertools as _;
use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyWarning};
use pyo3::prelude::*;
use std::convert::Infallible;
use std::ffi::CString;
use std::fmt;
use std::iter::once;

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

// TODO make this work on generic version
pub trait PyTerminalResultExt {
    type V;

    fn py_termfail_resolve(self) -> PyResult<Self::V>;
}

impl<V, W: fmt::Display, E: fmt::Display, T: fmt::Display> PyTerminalResultExt
    for TerminalResult<V, W, E, T>
{
    type V = V;

    fn py_termfail_resolve(self) -> PyResult<Self::V> {
        self.map_or_else(|e| Err(handle_failure(e)), handle_warnings)
    }
}

pub trait PyTerminalNoWarnResultExt {
    type V;

    fn py_termfail_resolve_nowarn(self) -> PyResult<Self::V>;
}

impl<V, E: fmt::Display, T: fmt::Display> PyTerminalNoWarnResultExt
    for TerminalResult<V, Infallible, E, T>
{
    type V = V;

    fn py_termfail_resolve_nowarn(self) -> PyResult<Self::V> {
        self.map_err(handle_failure_nowarn).map(Terminal::inner)
    }
}

pub trait PyTerminalNoErrorResultExt {
    type V;

    fn py_term_resolve_noerror(self) -> PyResult<Self::V>;
}

impl<V, W: fmt::Display> PyTerminalNoErrorResultExt for Terminal<V, W> {
    type V = V;

    fn py_term_resolve_noerror(self) -> PyResult<Self::V> {
        handle_warnings(self)
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

fn handle_failure_nowarn<E, T>(f: TerminalFailure<Infallible, E, T>) -> PyErr
where
    E: fmt::Display,
    T: fmt::Display,
{
    f.resolve(|_| (), emit_failure).1
}

fn emit_failure<E, T>(es: GenNonEmpty<E, VecFamily>, r: T) -> PyErr
where
    E: fmt::Display,
    T: fmt::Display,
{
    let s = once(format!("Toplevel Error: {r}"))
        .chain(es.into_iter().map(|x| x.to_string()))
        .join("\n");
    PyreflowException::new_err(s)
}

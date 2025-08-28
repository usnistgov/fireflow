use derive_new::new;
use itertools::Itertools;
use proc_macro2::TokenStream;
use quote::{format_ident, quote, ToTokens};
use std::fmt;

#[derive(Clone, new)]
pub(crate) struct DocString {
    summary: String,
    paragraphs: Vec<String>,
    args: Vec<DocArg>,
    returns: Option<DocReturn>,
}

#[derive(Clone, new)]
pub(crate) struct DocArg {
    pub(crate) argtype: ArgType,
    pub(crate) argname: String,
    pub(crate) pytype: PyType,
    pub(crate) desc: String,
    pub(crate) default: Option<DocDefault>,
}

#[derive(Clone)]
pub(crate) enum DocDefault {
    Bool(bool),
    EmptyDict,
    EmptyList,
    Option,
    Other(TokenStream, String),
}

#[derive(Clone, new)]
pub(crate) struct DocReturn {
    rtype: PyType,
    desc: Option<String>,
}

#[derive(Clone)]
pub(crate) enum ArgType {
    Ivar,
    Param,
}

#[derive(Clone)]
pub(crate) enum PyType {
    Str,
    Bool,
    Bytes,
    Int,
    Float,
    None,
    Datetime,
    Date,
    Time,
    Option(Box<PyType>),
    Dict(Box<PyType>, Box<PyType>),
    Union(Box<PyType>, Box<PyType>, Vec<PyType>),
    Tuple(Vec<PyType>),
    List(Box<PyType>),
    Literal(&'static str, Vec<&'static str>),
    PyClass(String),
    Raw(String),
}

impl DocArg {
    pub(crate) fn new_ivar(argname: String, pytype: PyType, desc: String) -> Self {
        Self::new(ArgType::Ivar, argname, pytype, desc, None)
    }

    pub(crate) fn new_param(argname: String, pytype: PyType, desc: String) -> Self {
        Self::new(ArgType::Param, argname, pytype, desc, None)
    }

    pub(crate) fn new_ivar_def(
        argname: String,
        pytype: PyType,
        desc: String,
        def: DocDefault,
    ) -> Self {
        Self::new(ArgType::Ivar, argname, pytype, desc, Some(def))
    }

    pub(crate) fn new_param_def(
        argname: String,
        pytype: PyType,
        desc: String,
        def: DocDefault,
    ) -> Self {
        Self::new(ArgType::Param, argname, pytype, desc, Some(def))
    }

    pub(crate) fn default_matches(&self) -> Result<(), String> {
        if let Some(d) = self.default.as_ref() {
            if d.matches_pytype(&self.pytype) {
                Ok(())
            } else {
                Err(format!(
                    "Arg type '{}' does not match default type '{}'",
                    self.pytype,
                    d.as_type()
                ))
            }
        } else {
            Ok(())
        }
    }
}

impl DocDefault {
    fn as_rs_value(&self) -> TokenStream {
        match self {
            Self::Bool(x) => quote! {#x},
            Self::EmptyDict => quote! {std::collections::HashMap::new()},
            Self::EmptyList => quote! {vec![]},
            Self::Option => quote! {None},
            Self::Other(rs, _) => rs.clone(),
        }
    }

    fn as_py_value(&self) -> String {
        match self {
            Self::Bool(x) => if *x { "True" } else { "False" }.into(),
            Self::EmptyDict => "{}".to_string(),
            Self::EmptyList => "[]".to_string(),
            Self::Option => "None".to_string(),
            Self::Other(_, py) => py.clone(),
        }
    }

    // for error reporting
    fn as_type(&self) -> &'static str {
        match self {
            Self::Bool(_) => "bool",
            Self::EmptyDict => "dict",
            Self::EmptyList => "list",
            Self::Option => "option",
            Self::Other(_, _) => "raw",
        }
    }

    fn matches_pytype(&self, other: &PyType) -> bool {
        matches!(
            (self, other),
            (Self::Bool(_), PyType::Bool)
                | (Self::EmptyDict, PyType::Dict(_, _))
                | (Self::EmptyList, PyType::List(_))
                | (Self::Option, PyType::Option(_))
                | (Self::Other(_, _), _)
        )
    }
}

impl ArgType {
    fn as_typename(&self) -> &'static str {
        match self {
            Self::Ivar => "vartype",
            Self::Param => "type",
        }
    }
}

impl PyType {
    pub(crate) fn new_opt(x: PyType) -> Self {
        Self::Option(Box::new(x))
    }

    pub(crate) fn new_list(x: PyType) -> Self {
        Self::List(Box::new(x))
    }

    pub(crate) fn new_dict(k: PyType, v: PyType) -> Self {
        Self::Dict(Box::new(k), Box::new(v))
    }

    pub(crate) fn new_union2(x: PyType, y: PyType) -> Self {
        Self::new_union(vec![x, y])
    }

    pub(crate) fn new_union(xs: Vec<PyType>) -> Self {
        let mut it = xs.into_iter();
        let x0 = it.next().expect("Union cannot be empty");
        let x1 = it.next().expect("Union must have at least 2 types");
        let xs = it.collect();
        Self::Union(Box::new(x0), Box::new(x1), xs)
    }

    pub(crate) fn new_lit(xs: &[&'static str]) -> Self {
        let mut it = xs.iter();
        let x0 = it.next().expect("Literal cannot be empty");
        let xs = it.copied().collect();
        Self::Literal(x0, xs)
    }

    pub(crate) fn new_unit() -> Self {
        Self::Tuple(vec![])
    }

    fn is_oneword(&self) -> bool {
        !matches!(self, Self::Raw(_) | Self::PyClass(_))
    }
}

impl DocString {
    fn has_defaults(&self) -> Option<bool> {
        self.args
            .iter()
            .skip_while(|p| p.default.is_none())
            .try_fold(false, |has_def, next| {
                match (has_def, next.default.is_some()) {
                    // if we encounter a non-default after at least one
                    // default, return None (error) since this means we
                    // have default args after non-default args.
                    (true, false) => None,
                    (x, y) => Some(x || y),
                }
            })
    }

    pub(crate) fn doc(&self) -> TokenStream {
        let s = self.to_string();
        quote! {#[doc = #s]}
    }

    pub(crate) fn sig(&self) -> TokenStream {
        if let Err(e) = self
            .args
            .iter()
            .map(|a| a.default_matches())
            .collect::<Result<Vec<_>, _>>()
        {
            panic!("{e}")
        }
        if let Some(has_def) = self.has_defaults() {
            if has_def {
                let ps = &self.args;
                let (raw_sig, _txt_sig): (Vec<_>, Vec<_>) = ps
                    .iter()
                    .map(|a| {
                        let n = &a.argname;
                        let i = format_ident!("{n}");
                        if let Some(d) = a.default.as_ref() {
                            let r = d.as_rs_value();
                            let t = d.as_py_value();
                            (quote! {#i=#r}, format!("{n}={t}"))
                        } else {
                            (quote! {#i}, n.to_string())
                        }
                    })
                    .unzip();
                let txt_sig = format!("({})", _txt_sig.iter().join(", "));
                quote! {
                    #[pyo3(signature = (#(#raw_sig),*))]
                    #[pyo3(text_signature = #txt_sig)]
                }
            } else {
                quote! {}
            }
        } else {
            panic!("non-default args after default args");
        }
    }
}

impl ToTokens for DocString {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let doc = self.doc();
        let sig = self.sig();
        quote! {
            #doc
            #sig
        }
        .to_tokens(tokens);
    }
}

impl fmt::Display for DocString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let ps = self
            .paragraphs
            .iter()
            .map(|s| fmt_docstring_nonparam(s.as_str()));
        let xs = self.args.iter().map(|s| s.to_string());
        let r = self.returns.as_ref().into_iter().map(|s| s.to_string());
        let rest = ps.chain(xs).chain(r).join("\n\n");
        if self.summary.len() > LINE_LEN {
            panic!("summary is too long");
        }
        write!(f, "{}\n\n{rest}", self.summary)
    }
}

impl fmt::Display for DocArg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let at = &self.argtype;
        let pt = &self.pytype;
        let n = &self.argname;
        let d = &self.desc;
        if pt.is_oneword() {
            let s = fmt_docstring_param1(format!(":{at} {pt} {n}: {d}"));
            f.write_str(s.as_str())
        } else {
            let tn = self.argtype.as_typename();
            let s0 = fmt_docstring_param1(format!(":{at} {n}: {d}"));
            let s1 = fmt_docstring_param1(format!(":{tn} {n}: {pt}"));
            write!(f, "{s0}\n{s1}")
        }
    }
}

impl fmt::Display for DocReturn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let t = fmt_docstring_param1(format!(":rtype: {}", self.rtype));
        if let Some(d) = self
            .desc
            .as_ref()
            .map(|d| fmt_docstring_param1(format!(":returns: {d}")))
        {
            write!(f, "{d}\n{t}")
        } else {
            f.write_str(t.as_str())
        }
    }
}

impl fmt::Display for ArgType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let s = match self {
            Self::Ivar => "ivar",
            Self::Param => "param",
        };
        f.write_str(s)
    }
}

impl fmt::Display for PyType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Self::Bool => f.write_str("bool"),
            Self::Str => f.write_str("str"),
            Self::Int => f.write_str("int"),
            Self::Float => f.write_str("float"),
            Self::Bytes => f.write_str("Bytes"),
            Self::None => f.write_str("None"),
            Self::Date => f.write_str("datetime.date"),
            Self::Time => f.write_str("datetime.time"),
            Self::Datetime => f.write_str("datetime.datetime"),
            Self::Union(x, y, zs) => {
                let s = [x.as_ref(), y.as_ref()]
                    .into_iter()
                    .chain(zs.iter())
                    .join(" | ");
                f.write_str(s.as_str())
            }
            Self::Tuple(xs) => {
                if xs.is_empty() {
                    f.write_str("tuple[()]")
                } else {
                    write!(f, "tuple[{}]", xs.iter().join(", "))
                }
            }
            Self::Literal(x, xs) => {
                write!(f, "Literal[{}]", [x].into_iter().chain(xs).join(", "))
            }
            Self::List(x) => write!(f, "list[{x}]"),
            Self::Dict(x, y) => write!(f, "dict[{x}, {y}]"),
            Self::Option(x) => write!(f, "{x} | None"),
            Self::PyClass(s) => write!(f, ":py:class:`{s}`"),
            Self::Raw(s) => f.write_str(s.as_ref()),
        }
    }
}

// fn fmt_docstring_nonparam1(s: String) -> String {
//     fmt_docstring_nonparam(s.as_str())
// }

fn fmt_docstring_nonparam(s: &str) -> String {
    fmt_hanging_indent(LINE_LEN, 0, s)
}

fn fmt_docstring_param1(s: String) -> String {
    fmt_docstring_param(s.as_str())
}

fn fmt_docstring_param(s: &str) -> String {
    fmt_hanging_indent(LINE_LEN, 4, s)
}

fn fmt_hanging_indent(width: usize, indent: usize, s: &str) -> String {
    let i = " ".repeat(indent);
    let xs = s.split_whitespace().filter(|x| !x.is_empty());
    let mut line_len = 0;
    let mut tmp = vec![]; // buffer for current line
    let mut zs = vec![]; // buffer for indented lines
    for x in xs {
        // add length of word (without next space)
        line_len += x.len();
        // If length exceeds target width, reset length, join line buffer with
        // spaces, collect line in final line buffer, then make new line buffer
        // and initialize with a hanging indent. This will only happen if we hit
        // the target length at least once so the first line will never have a
        // hanging indent.
        //
        // Otherwise, add 1 to length to account for space after word.
        //
        // In all cases, add the next word to the line buffer, which may only
        // have a leading indent if it was reset immediately before.
        if line_len > width {
            zs.push(tmp.iter().join(" "));
            if indent > 0 {
                line_len = indent + x.len();
                tmp = vec![i.as_str()]
            } else {
                line_len = x.len();
                tmp = vec![]
            }
        } else {
            line_len += 1;
        }
        tmp.push(x);
    }
    zs.push(tmp.iter().join(" "));
    zs.iter().join("\n")
}

const LINE_LEN: usize = 72;

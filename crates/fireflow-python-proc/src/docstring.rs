use derive_more::{Display, From};
use derive_new::new;
use itertools::Itertools;
use proc_macro2::TokenStream;
use quote::{format_ident, quote, ToTokens};
use std::fmt;
use std::marker::PhantomData;

#[derive(Clone, new)]
pub(crate) struct DocString<A, R, S> {
    summary: String,
    paragraphs: Vec<String>,
    args: Vec<A>,
    returns: R,
    _selfarg: PhantomData<S>,
}

pub(crate) type ClassDocString = DocString<AnyDocArg, (), NoSelf>;
pub(crate) type MethodDocString = DocString<DocArgParam, Option<DocReturn>, SelfArg>;
pub(crate) type FunDocString = DocString<DocArgParam, Option<DocReturn>, NoSelf>;

// pub(crate) struct NoReturn;

pub(crate) struct NoSelf;

pub(crate) struct SelfArg;

pub(crate) trait IsSelfArg {
    const ARG: Option<&'static str>;
}

// pub(crate) trait IsReturn {
//     fn return_doc(&self) -> Option<String>;
// }

// impl IsReturn for NoReturn {
//     fn return_doc(&self) -> Option<String> {
//         None
//     }
// }

// impl IsReturn for DocReturn {
//     fn return_doc(&self) -> Option<String> {
//         Some(self.to_string())
//     }
// }

impl IsSelfArg for NoSelf {
    const ARG: Option<&'static str> = None;
}

impl IsSelfArg for SelfArg {
    const ARG: Option<&'static str> = Some("self");
}

#[derive(Clone, From, Display)]
pub(crate) enum AnyDocArg {
    RWIvar(DocArgRWIvar),
    ROIvar(DocArgROIvar),
    Param(DocArgParam),
}

pub(crate) type DocArgRWIvar = DocArg<ArgTypeIvar<false>>;
pub(crate) type DocArgROIvar = DocArg<ArgTypeIvar<true>>;
pub(crate) type DocArgParam = DocArg<ArgTypeParam>;

// #[derive(Clone)]
// pub enum DocSelf {
//     NoSelf,
//     PySelf,
//     // PyClass,
// }

#[derive(Clone, new)]
pub(crate) struct DocArg<T> {
    pub(crate) argname: String,
    pub(crate) pytype: PyType,
    pub(crate) desc: String,
    pub(crate) default: Option<DocDefault>,
    _argtype: PhantomData<T>,
}

#[derive(Clone)]
pub(crate) enum DocDefault {
    Bool(bool),
    EmptyDict,
    // EmptySet,
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
pub(crate) struct ArgTypeParam;

#[derive(Clone)]
pub(crate) struct ArgTypeIvar<const READONLY: bool>;

#[derive(Clone)]
pub(crate) enum PyType {
    Str,
    Bool,
    Bytes,
    Int,
    Float,
    None,
    Datetime,
    Decimal,
    Date,
    Time,
    Option(Box<PyType>),
    Dict(Box<PyType>, Box<PyType>),
    Union(Box<PyType>, Box<PyType>, Vec<PyType>),
    Tuple(Vec<PyType>),
    List(Box<PyType>),
    // Set(Box<PyType>),
    Literal(&'static str, Vec<&'static str>),
    PyClass(String),
}

impl DocArgROIvar {
    pub(crate) fn new_ivar_ro(argname: String, pytype: PyType, desc: String) -> Self {
        Self::new(argname, pytype, desc, None)
    }

    pub(crate) fn new_ivar_ro_def(
        argname: String,
        pytype: PyType,
        desc: String,
        def: DocDefault,
    ) -> Self {
        Self::new(argname, pytype, desc, Some(def))
    }
}

impl DocArgRWIvar {
    pub(crate) fn new_ivar_rw(argname: String, pytype: PyType, desc: String) -> Self {
        Self::new(argname, pytype, desc, None)
    }

    pub(crate) fn new_ivar_rw_def(
        argname: String,
        pytype: PyType,
        desc: String,
        def: DocDefault,
    ) -> Self {
        Self::new(argname, pytype, desc, Some(def))
    }
}

impl DocArgParam {
    pub(crate) fn new_param(argname: String, pytype: PyType, desc: String) -> Self {
        Self::new(argname, pytype, desc, None)
    }

    pub(crate) fn new_param_def(
        argname: String,
        pytype: PyType,
        desc: String,
        def: DocDefault,
    ) -> Self {
        Self::new(argname, pytype, desc, Some(def))
    }
}

impl<T> DocArg<T> {
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
            // Self::EmptySet => quote! {std::collections::HashSet::new()},
            Self::EmptyList => quote! {vec![]},
            Self::Option => quote! {None},
            Self::Other(rs, _) => rs.clone(),
        }
    }

    fn as_py_value(&self) -> String {
        match self {
            Self::Bool(x) => if *x { "True" } else { "False" }.into(),
            Self::EmptyDict => "{}".to_string(),
            // this isn't implemented yet: https://peps.python.org/pep-0802/#abstract
            // Self::EmptySet => "{/}".to_string(),
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
            // Self::EmptySet => "set",
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
                // | (Self::EmptySet, PyType::Set(_))
                | (Self::EmptyList, PyType::List(_))
                | (Self::Option, PyType::Option(_))
                | (Self::Other(_, _), _)
        )
    }
}

pub(crate) trait IsArgType {
    const TYPENAME: &str;
    const ARGTYPE: &str;

    fn readonly() -> bool;
}

impl<const READONLY: bool> IsArgType for ArgTypeIvar<READONLY> {
    const TYPENAME: &str = "vartype";
    const ARGTYPE: &str = "ivar";

    fn readonly() -> bool {
        READONLY
    }
}

impl IsArgType for ArgTypeParam {
    const TYPENAME: &str = "type";
    const ARGTYPE: &str = "param";

    fn readonly() -> bool {
        false
    }
}

pub(crate) trait IsDocArg {
    fn argname(&self) -> &str;

    fn pytype(&self) -> &PyType;

    fn desc(&self) -> &str;

    fn default(&self) -> Option<&DocDefault>;

    fn default_matches(&self) -> Result<(), String>;
}

impl<T> IsDocArg for DocArg<T> {
    fn argname(&self) -> &str {
        self.argname.as_str()
    }

    fn pytype(&self) -> &PyType {
        &self.pytype
    }

    fn desc(&self) -> &str {
        self.desc.as_str()
    }

    fn default(&self) -> Option<&DocDefault> {
        self.default.as_ref()
    }

    fn default_matches(&self) -> Result<(), String> {
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

impl IsDocArg for AnyDocArg {
    fn argname(&self) -> &str {
        match self {
            Self::RWIvar(x) => x.argname(),
            Self::ROIvar(x) => x.argname(),
            Self::Param(x) => x.argname(),
        }
    }

    fn pytype(&self) -> &PyType {
        match self {
            Self::RWIvar(x) => x.pytype(),
            Self::ROIvar(x) => x.pytype(),
            Self::Param(x) => x.pytype(),
        }
    }

    fn desc(&self) -> &str {
        match self {
            Self::RWIvar(x) => x.desc(),
            Self::ROIvar(x) => x.desc(),
            Self::Param(x) => x.desc(),
        }
    }

    fn default(&self) -> Option<&DocDefault> {
        match self {
            Self::RWIvar(x) => x.default(),
            Self::ROIvar(x) => x.default(),
            Self::Param(x) => x.default(),
        }
    }

    fn default_matches(&self) -> Result<(), String> {
        match self {
            Self::RWIvar(x) => x.default_matches(),
            Self::ROIvar(x) => x.default_matches(),
            Self::Param(x) => x.default_matches(),
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

    // pub(crate) fn new_set(x: PyType) -> Self {
    //     Self::Set(Box::new(x))
    // }

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

    // fn is_oneword(&self) -> bool {
    //     !matches!(
    //         self,
    //         Self::Option(_)
    //             | Self::Union(_, _, _)
    //             | Self::Literal(_, _)
    //             | Self::Tuple(_)
    //             | Self::List(_)
    //             | Self::Dict(_, _)
    //             | Self::Raw(_)
    //             | Self::PyClass(_)
    //     )
    // }
}

impl ClassDocString {
    pub(crate) fn new_class(
        summary: String,
        paragraphs: Vec<String>,
        args: Vec<AnyDocArg>,
    ) -> Self {
        Self::new(summary, paragraphs, args, ())
    }
}

impl MethodDocString {
    pub(crate) fn new_method(
        summary: String,
        paragraphs: Vec<String>,
        args: Vec<DocArgParam>,
        returns: Option<DocReturn>,
    ) -> Self {
        Self::new(summary, paragraphs, args, returns)
    }
}

impl FunDocString {
    pub(crate) fn new_fun(
        summary: String,
        paragraphs: Vec<String>,
        args: Vec<DocArgParam>,
        returns: Option<DocReturn>,
    ) -> Self {
        Self::new(summary, paragraphs, args, returns)
    }
}

impl<A, R, S> DocString<A, R, S> {
    fn has_defaults(&self) -> Option<bool>
    where
        A: IsDocArg,
    {
        self.args
            .iter()
            .skip_while(|p| p.default().is_none())
            .try_fold(false, |has_def, next| {
                match (has_def, next.default().is_some()) {
                    // if we encounter a non-default after at least one
                    // default, return None (error) since this means we
                    // have default args after non-default args.
                    (true, false) => None,
                    (x, y) => Some(x || y),
                }
            })
    }

    pub(crate) fn doc(&self) -> TokenStream
    where
        Self: fmt::Display,
    {
        let s = self.to_string();
        quote! {#[doc = #s]}
    }

    pub(crate) fn sig(&self) -> TokenStream
    where
        A: IsDocArg,
        S: IsSelfArg,
    {
        if let Err(e) = self
            .args
            .iter()
            .map(|a| a.default_matches())
            .collect::<Result<Vec<_>, _>>()
        {
            panic!("{e}")
        }
        if self.has_defaults().is_none() {
            panic!("non-default args after default args");
        }

        let ps = &self.args;
        let (raw_sig, _txt_sig): (Vec<_>, Vec<_>) = ps
            .iter()
            .map(|a| {
                let n = &a.argname();
                let i = format_ident!("{n}");
                if let Some(d) = a.default() {
                    let r = d.as_rs_value();
                    let t = d.as_py_value();
                    (quote! {#i=#r}, format!("{n}={t}"))
                } else {
                    (quote! {#i}, n.to_string())
                }
            })
            .unzip();
        let txt_sig = format!(
            "({})",
            S::ARG
                .into_iter()
                .chain(_txt_sig.iter().map(|s| s.as_str()))
                .join(", ")
        );
        quote! {
            #[pyo3(signature = (#(#raw_sig),*))]
            #[pyo3(text_signature = #txt_sig)]
        }
    }

    fn fmt_inner<F>(&self, f_return: F, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error>
    where
        A: fmt::Display,
        F: FnOnce(&R) -> Option<String>,
    {
        let ps = self
            .paragraphs
            .iter()
            .map(|s| fmt_docstring_nonparam(s.as_str()));
        let xs = self.args.iter().map(|s| s.to_string());
        let r = f_return(&self.returns).into_iter();
        let rest = ps.chain(xs).chain(r).join("\n\n");
        if self.summary.len() > LINE_LEN {
            panic!("summary is too long");
        }
        write!(f, "{}\n\n{rest}", self.summary)
    }
}

// impl DocSelf {
//     fn as_arg(&self) -> Option<String> {
//         match self {
//             DocSelf::NoSelf => None,
//             DocSelf::PySelf => Some("self".into()),
//             // DocSelf::PyClass => Some("cls".into()),
//         }
//     }
// }

impl<A, R, S> ToTokens for DocString<A, R, S>
where
    Self: fmt::Display,
    A: IsDocArg,
    S: IsSelfArg,
{
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

impl fmt::Display for ClassDocString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        self.fmt_inner(|()| None, f)
    }
}

impl<A: fmt::Display, S> fmt::Display for DocString<A, Option<DocReturn>, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        self.fmt_inner(|r| r.as_ref().map(|s| s.to_string()), f)
    }
}

impl<T: IsArgType + fmt::Display> fmt::Display for DocArg<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let ro = if T::readonly() { "(read-only) " } else { "" };
        let pt = &self.pytype;
        let n = &self.argname;
        let d = self
            .default
            .as_ref()
            .map(|d| d.as_py_value())
            .map_or(self.desc.to_string(), |def| {
                format!("{} Defaults to ``{def}``.", self.desc)
            });
        let tn = T::TYPENAME;
        let at = T::ARGTYPE;
        let s0 = fmt_docstring_param1(format!(":{at} {n}: {ro}{d}"));
        let s1 = fmt_docstring_param1(format!(":{tn} {n}: {pt}"));
        write!(f, "{s0}\n{s1}")
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

impl<const READONLY: bool> fmt::Display for ArgTypeIvar<READONLY> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.write_str("ivar")
    }
}

impl fmt::Display for ArgTypeParam {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.write_str("param")
    }
}

impl fmt::Display for PyType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        // see https://github.com/sphinx-doc/sphinx/blob/9a08711e0e18c63c609070aa0a79019b4db45a78/tests/test_util/test_util_typing.py
        // for cheat sheet on how these types should be represented in .rst syntax
        match self {
            Self::Bool => f.write_str(":py:class:`bool`"),
            Self::Str => f.write_str(":py:class:`str`"),
            Self::Int => f.write_str(":py:class:`int`"),
            Self::Float => f.write_str(":py:class:`float`"),
            Self::Bytes => f.write_str(":py:class:`bytes`"),
            Self::None => f.write_str("None"),
            Self::Date => f.write_str(":py:class:`~datetime.date`"),
            Self::Time => f.write_str(":py:class:`~datetime.time`"),
            Self::Datetime => f.write_str(":py:class:`~datetime.datetime`"),
            Self::Decimal => f.write_str(":py:class:`~decimal.Decimal`"),
            Self::Union(x, y, zs) => {
                let s = [x.as_ref(), y.as_ref()]
                    .into_iter()
                    .chain(zs.iter())
                    .join(" | ");
                f.write_str(s.as_str())
            }
            Self::Tuple(xs) => {
                let s = if xs.is_empty() {
                    "()".into()
                } else {
                    xs.iter().join(", ")
                };
                write!(f, ":py:class:`tuple`\\ [{s}]")
            }
            Self::Literal(x, xs) => {
                write!(
                    f,
                    ":obj:`~typing.Literal`\\ [{}]",
                    [x].into_iter()
                        .chain(xs)
                        .map(|s| format!("\"{s}\""))
                        .join(", ")
                )
            }
            Self::List(x) => write!(f, ":py:class:`list`\\ [{x}]"),
            // Self::Set(x) => write!(f, ":py:class:`set`\\ [{x}]"),
            Self::Dict(x, y) => write!(f, ":py:class:`dict`\\ [{x}, {y}]"),
            Self::Option(x) => write!(f, "{x} | None"),
            Self::PyClass(s) => write!(f, ":py:class:`{s}`"),
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

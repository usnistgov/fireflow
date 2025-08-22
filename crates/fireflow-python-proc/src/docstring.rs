use derive_new::new;
use itertools::Itertools;
use quote::{quote, ToTokens};
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
    argtype: ArgType,
    argname: String,
    pytype: String,
    desc: String,
}

#[derive(Clone)]
pub(crate) struct DocReturn {
    rtype: String,
    desc: String,
}

#[derive(Clone)]
pub(crate) enum ArgType {
    Ivar,
    Param,
}

impl DocArg {
    pub(crate) fn new_ivar(argname: String, pytype: String, desc: String) -> Self {
        Self::new(ArgType::Ivar, argname, pytype, desc)
    }

    pub(crate) fn new_param(argname: String, pytype: String, desc: String) -> Self {
        Self::new(ArgType::Param, argname, pytype, desc)
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

impl ToTokens for DocString {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let s = self.to_string();
        quote! { #[doc = #s] }.to_tokens(tokens);
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
        if pt.chars().all(char::is_alphanumeric) {
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
        let d = fmt_docstring_param(self.desc.as_str());
        let t = fmt_docstring_param(self.rtype.as_str());
        write!(f, ":returns: {d}\n:rtype: {t}")
    }
}

impl fmt::Display for ArgType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let s = match self {
            ArgType::Ivar => "ivar",
            ArgType::Param => "param",
        };
        f.write_str(s)
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

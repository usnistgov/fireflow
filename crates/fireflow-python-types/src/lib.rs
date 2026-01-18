#![allow(clippy::return_self_not_must_use)]
#![allow(clippy::must_use_candidate)]

use derive_more::{AsRef, Display, From};
use derive_new::new;
use itertools::Itertools as _;
use nonempty::NonEmpty;
use proc_macro2::TokenStream as TokenStream2;
use quote::{ToTokens, format_ident, quote};
use std::cmp::Ordering;
use std::fmt;
use std::hash::Hash;
use std::iter::{empty, once};
use std::marker::PhantomData;
use std::string::ToString;
use syn::{GenericArgument, Ident, Path, PathArguments, Type, parse_quote};

/// FCS Version
#[derive(PartialEq, Eq, PartialOrd, Clone, Copy)]
pub enum Version {
    FCS2_0,
    FCS3_0,
    FCS3_1,
    FCS3_2,
}

/// A docstring for any python function/method/class
#[derive(Clone, new)]
pub struct DocString<A, R, S> {
    pub summary: String,
    pub paragraphs: Vec<String>,
    pub args: A,
    pub returns: R,
    pub _selfarg: PhantomData<S>,
}

type ClassDocString = DocString<Vec<AnyDocArg>, (), NoSelf>;
type MethodDocString = DocString<Vec<DocArgParam>, Option<DocReturn<RetPyType>>, SelfArg>;
type FunDocString = DocString<Vec<DocArgParam>, Option<DocReturn<RetPyType>>, NoSelf>;
type IvarDocString = DocString<(), DocReturn<ArgPyType>, SelfArg>;

/// Represents a method which does not have a self arg
pub struct NoSelf;

/// Represents a method which has a self arg
pub struct SelfArg;

/// The origin of a segment
#[derive(Clone, Copy)]
pub enum SegmentSrc {
    Header,
    Any,
}

/// Any python argument documentation type
#[derive(Clone, From, Display)]
pub enum AnyDocArg {
    RWIvar(DocArgRWIvar),
    ROIvar(DocArgROIvar),
    Param(DocArgParam),
}

pub type DocArgParam = DocArg<NoMethods>;
pub type DocArgROIvar = DocArg<GetMethod>;

type DocArgRWIvar = DocArg<GetSetMethods>;

/// Python documentation for one argument
#[derive(Clone, new, AsRef)]
pub struct DocArg<T> {
    /// Name of the arg
    #[new(into)]
    pub argname: String,

    /// Python type of the arg
    #[as_ref(ArgPyType)]
    #[new(into)]
    pub pytype: ArgPyType,

    /// Description of the arg as to be shown in docs
    #[new(into)]
    pub desc: String,

    /// Default value of the arg
    pub default: Option<DocDefault>,

    /// Methods to get/set the arg
    pub methods: T,
}

/// Denotes that a Python argument does not have get/set methods
#[derive(Clone)]
pub struct NoMethods;

/// Get methods for a python argument
#[derive(Clone)]
pub struct GetMethod(TokenStream2);

/// Get and set methods for a python argument
#[derive(new, Clone)]
pub struct GetSetMethods {
    get: TokenStream2,
    set: TokenStream2,
}

/// Default value for a Python argument
#[derive(Clone)]
pub enum DocDefault {
    Auto,
    Int(usize),
    Str(String),
}

/// Return value for a python method/function
#[derive(Clone)]
pub struct DocReturn<T> {
    rtype: T,
    desc: Option<String>,
    // TODO this should always be empty for ivars
    exceptions: Vec<ReturnPyException>,
}

// this is equivalent to returning `()` in Rust and `None` in Python
impl<T> Default for DocReturn<PyType<T>>
where
    PyTuple<T>: Into<PyType<T>>,
{
    fn default() -> Self {
        Self::new(PyTuple::default().into())
    }
}

/// Documentation for a Python exception
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PyException {
    pyname: String,
    desc: Option<String>,
}

#[derive(Display)]
pub enum PyreflowError {
    #[display("FileLayoutError")]
    FileLayout,
    #[display("ParseKeyError")]
    ParseKey,
    #[display("ParseKeywordValueError")]
    ParseKeywordValue,
    #[display("InvalidKeywordValueError")]
    InvalidKeywordValue,
    #[display("ExtraKeywordError")]
    ExtraKeyword,
    #[display("FCSDeprecatedError")]
    FCSDeprecated,
    #[display("ConversionError")]
    Conversion,
    #[display("RelationalError")]
    Relational,
    #[display("EventDataError")]
    EventData,
    #[display("DataLossError")]
    DataLoss,
    #[display("ConfigError")]
    Config,
}

impl PyreflowError {
    #[must_use]
    pub fn fmt_ref(&self) -> String {
        format!(":py:exc:`~pyreflow.{self}`")
    }
}

/// A Python exceptiion returned from a function
#[derive(Clone, PartialEq, Eq, Hash, From)]
pub struct ReturnPyException(PyException);

/// A Python exception thrown when an argument value is converted into Rust
#[derive(Clone, PartialEq, Eq, Hash, new)]
pub struct ArgPyException {
    inner: PyException,
    argmod: ExcNameMod,
}

/// A wrapper that modifies the origin name of an exception
#[derive(Clone, PartialEq, Eq, Hash, Default, PartialOrd, Ord)]
enum ExcNameMod {
    #[default]
    NoMod,
    /// For tuples, adds "field 1 in {}"
    Field(NonEmpty<usize>, Box<Self>),
    /// For lists, adds "any in {}"
    List(Box<Self>),
    /// For dict keys, adds "dict key in {}"
    DictKey(Box<Self>),
    /// For dict keys, adds "dict value in {}"
    DictVal(Box<Self>),
}

/// A Python exception attached to at least one argmenent
#[derive(new)]
struct NamedPyException {
    names: NonEmpty<String>,
    inner: ArgPyException,
}

/// A Python type associated with an argument or return value
#[derive(Clone, From, Display)]
pub enum PyType<E> {
    #[from]
    Str(PyStr<E>),
    #[from]
    Bool(PyBool<E>),
    #[from]
    Bytes(PyBytes<E>),
    #[from(RsInt)]
    #[from(PyInt<E>)]
    Int(PyInt<E>),
    #[from(RsFloat)]
    #[from(PyFloat<E>)]
    Float(PyFloat<E>),
    #[from]
    Decimal(PyDecimal<E>),
    #[from]
    Datetime(PyDatetime<E>),
    #[from]
    Date(PyDate<E>),
    #[from]
    Time(PyTime<E>),
    #[from(PyOpt<E>)]
    Option(Box<PyOpt<E>>),
    #[from(PyDict<E>)]
    Dict(Box<PyDict<E>>),
    #[from]
    Tuple(PyTuple<E>),
    #[from(PyList<E>)]
    List(Box<PyList<E>>),
    #[from]
    Literal(PyLiteral),
    #[from]
    PyClass(PyClass<E>),
    #[from(PyUnion<E>)]
    Union(Box<PyUnion<E>>),
}

pub type ArgPyType = PyType<ArgPyException>;
type RetPyType = PyType<()>;

/// A "broken-down" python type.
///
/// This is mostly used to resolve the ambiguities that arise when dealing with
/// "Option" (in rust) as "X | None" (in Python) and the ugly implications this
/// has with Enums and Unions.
#[derive(PartialEq, Hash, Eq, Clone)]
enum PyAtom<R> {
    Str,
    Bool,
    Bytes,
    Int,
    Float,
    Decimal,
    Datetime,
    Date,
    Time,
    None,
    Dict(Box<Self>, Box<Self>),
    Tuple(Vec<Self>),
    List(Box<Self>),
    Literal(PyLiteral),
    PyClass(PyClass<R>),
    Union(Box<Self>, Box<Self>, Vec<Self>),
}

/// A Python 'int'
#[derive(Clone, new)]
pub struct PyInt<E> {
    rs: RsInt,
    rstype: Option<Path>,
    exc: Option<E>,
}

/// A Python 'float'
#[derive(Clone, From, new)]
pub struct PyFloat<E> {
    #[from]
    rs: RsFloat,
    rstype: Option<Path>,
    exc: Option<E>,
}

/// A Python 'str'
#[derive(Clone, new)]
pub struct PyStr<E> {
    rstype: Option<Path>,
    exc: Option<E>,
}

impl<E> Default for PyStr<E> {
    fn default() -> Self {
        Self::new(None, None)
    }
}

/// A Python 'bool'
#[derive(Clone, new)]
pub struct PyBool<E> {
    rstype: Option<Path>,
    exc: Option<E>,
}

impl<E> Default for PyBool<E> {
    fn default() -> Self {
        Self::new(None, None)
    }
}

/// A Python 'bytes'
#[derive(Clone, new)]
pub struct PyBytes<E> {
    rstype: Option<Path>,
    exc: Option<E>,
}

impl<E> Default for PyBytes<E> {
    fn default() -> Self {
        Self::new(None, None)
    }
}

/// A Python 'Decimal' class
#[derive(Clone, new)]
pub struct PyDecimal<E> {
    rstype: Option<Path>,
    exc: Option<E>,
}

impl<E> Default for PyDecimal<E> {
    fn default() -> Self {
        Self::new(None, None)
    }
}

/// A Python 'datetime.time' class
#[derive(Clone, Default, new)]
pub struct PyTime<E> {
    rstype: Option<Path>,
    exc: Option<E>,
}

/// A Python 'datetime.date' class
#[derive(Clone, Default, new)]
pub struct PyDate<E> {
    rstype: Option<Path>,
    exc: Option<E>,
}

/// A Python 'datetime.datetime' class
#[derive(Clone, new)]
pub struct PyDatetime<E> {
    rstype: Option<Path>,
    exc: Option<E>,
}

impl<E> Default for PyDatetime<E> {
    fn default() -> Self {
        Self::new(None, None)
    }
}

/// A Python 'typing.Literal'
#[derive(Clone, PartialEq, Hash, Eq, new)]
pub struct PyLiteral {
    #[new(into)]
    head: &'static str,
    #[new(into_iter = "&'static str")]
    tail: Vec<&'static str>,
    #[new(into)]
    rstype: Option<Path>,
}

/// A Python 'Optional[X]' aka 'X | None'
#[derive(Clone, new)]
pub struct PyOpt<R> {
    #[new(into)]
    inner: PyType<R>,
}

/// A Python 'dict[X, Y]'
#[derive(Clone, new)]
pub struct PyDict<E> {
    #[new(into)]
    key: PyType<E>,
    #[new(into)]
    value: PyType<E>,
    #[new(into)]
    rstype: Option<Path>,
    exc: Option<E>,
}

/// A Python 'list[X]'
#[derive(Clone, new)]
pub struct PyList<E> {
    #[new(into)]
    inner: PyType<E>,
    #[new(into)]
    rstype: Option<Path>,
    exc: Option<E>,
}

/// An arbitrary Python class
#[derive(Clone, new, PartialEq, Hash, Eq)]
pub struct PyClass<E> {
    #[new(into)]
    pyname: String,
    #[new(into)]
    rstype: Option<Path>,
    exc: Option<E>,
}

/// A Python 'Union[...]' aka 'A | B | ...'
#[derive(Clone, new)]
pub struct PyUnion<E> {
    #[new(into)]
    head0: PyType<E>,
    #[new(into)]
    head1: PyType<E>,
    tail: Vec<PyType<E>>,
    rstype: Path,
    exc: Option<E>,
}

/// A Python 'tuple[...]'
#[derive(Clone, new)]
pub struct PyTuple<E> {
    inner: Vec<PyType<E>>,
    rstype: Option<Path>,
    exc: Option<E>,
}

/// A rust integer type for use in making a python int more specific
#[derive(Clone)]
pub enum RsInt {
    U8,
    U16,
    U32,
    U64,
    I32,
    Usize,
    NonZeroU8,
    NonZeroUsize,
}

/// A rust float type for use in making a python float more specific
#[derive(Clone)]
pub enum RsFloat {
    F32,
    F64,
}

/// A type which represents 'Self' in python (or not)
trait IsSelfArg {
    const ARG: Option<&'static str>;
}

/// A type which can be converted to a tokenstream representing get/set rust methods
trait IsMethods {
    fn quoted_methods(&self) -> TokenStream2;
}

/// A Python type which has a Rust type (as a quoted path)
pub trait HasRustPath {
    fn as_rust_type(&self) -> Type;
}

/// Defines argument properties for given methods configurations
trait IsArgType {
    const TYPENAME: &str;
    const ARGTYPE: &str;

    fn readonly() -> Option<bool>;
}

/// General methods for args which may be documented
pub trait IsDocArg {
    fn argname(&self) -> &str;

    fn pytype(&self) -> &ArgPyType;

    // fn desc(&self) -> &str;

    fn default(&self) -> Option<&DocDefault>;

    fn fun_arg(&self) -> TokenStream2;

    fn ident(&self) -> Ident;

    fn ident_into(&self) -> TokenStream2;

    fn record_into(&self) -> TokenStream2;
}

/// A pytype which may be converted to a pyatom
trait AsPyAtom<R> {
    fn as_atom(&self) -> PyAtom<R>;
}

impl IsSelfArg for NoSelf {
    const ARG: Option<&'static str> = None;
}

impl IsSelfArg for SelfArg {
    const ARG: Option<&'static str> = Some("self");
}

impl IsMethods for NoMethods {
    fn quoted_methods(&self) -> TokenStream2 {
        quote!()
    }
}

impl IsMethods for GetMethod {
    fn quoted_methods(&self) -> TokenStream2 {
        let g = &self.0;
        quote! {
            #[getter]
            #g
        }
    }
}

impl IsMethods for GetSetMethods {
    fn quoted_methods(&self) -> TokenStream2 {
        let g = &self.get;
        let s = &self.set;
        quote! {
            #[getter]
            #g
            #[setter]
            #s
        }
    }
}

impl IsMethods for AnyDocArg {
    fn quoted_methods(&self) -> TokenStream2 {
        match self {
            Self::Param(x) => x.quoted_methods(),
            Self::ROIvar(x) => x.quoted_methods(),
            Self::RWIvar(x) => x.quoted_methods(),
        }
    }
}

impl<E> HasRustPath for PyType<E> {
    fn as_rust_type(&self) -> Type {
        match self {
            Self::Str(x) => x.as_rust_type(),
            Self::Bool(x) => x.as_rust_type(),
            Self::Bytes(x) => x.as_rust_type(),
            Self::Int(x) => x.as_rust_type(),
            Self::Float(x) => x.as_rust_type(),
            Self::Decimal(x) => x.as_rust_type(),
            Self::Datetime(x) => x.as_rust_type(),
            Self::Date(x) => x.as_rust_type(),
            Self::Time(x) => x.as_rust_type(),
            Self::Option(x) => x.as_rust_type(),
            Self::Dict(x) => x.as_rust_type(),
            Self::List(x) => x.as_rust_type(),
            Self::Tuple(x) => x.as_rust_type(),
            Self::Union(x) => x.as_rust_type(),
            Self::Literal(x) => x.as_rust_type(),
            Self::PyClass(x) => x.as_rust_type(),
        }
    }
}

macro_rules! impl_has_rust_path {
    ($t:ident, $p:path) => {
        impl<E> HasRustPath for $t<E> {
            fn as_rust_type(&self) -> Type {
                if let Some(x) = self.rstype.as_ref() {
                    parse_quote!(#x)
                } else {
                    parse_quote!($p)
                }
            }
        }
    };
}

impl_has_rust_path!(PyStr, String);
impl_has_rust_path!(PyBool, bool);
impl_has_rust_path!(PyBytes, Vec<u8>);
impl_has_rust_path!(PyDecimal, bigdecimal::BigDecimal);
impl_has_rust_path!(PyDate, chrono::NaiveDate);
impl_has_rust_path!(PyTime, chrono::NaiveTime);
impl_has_rust_path!(PyDatetime, chrono::DateTime<chrono::FixedOffset>);

impl<E> HasRustPath for PyOpt<E> {
    fn as_rust_type(&self) -> Type {
        let i = self.inner.as_rust_type();
        parse_quote!(Option<#i>)
    }
}

impl<E> HasRustPath for PyDict<E> {
    fn as_rust_type(&self) -> Type {
        if let Some(x) = self.rstype.as_ref() {
            parse_quote!(#x)
        } else {
            let k = &self.key.as_rust_type();
            let v = &self.value.as_rust_type();
            parse_quote!(std::collections::HashMap<#k, #v>)
        }
    }
}

impl<E> HasRustPath for PyTuple<E> {
    fn as_rust_type(&self) -> Type {
        if let Some(x) = self.rstype.as_ref() {
            parse_quote!(#x)
        } else {
            let vs: Vec<_> = self.inner.iter().map(HasRustPath::as_rust_type).collect();
            parse_quote!((#(#vs),*))
        }
    }
}

impl<E> HasRustPath for PyList<E> {
    fn as_rust_type(&self) -> Type {
        if let Some(x) = self.rstype.as_ref() {
            parse_quote!(#x)
        } else {
            let v = &self.inner.as_rust_type();
            parse_quote!(Vec<#v>)
        }
    }
}

impl<E> HasRustPath for PyClass<E> {
    fn as_rust_type(&self) -> Type {
        let x = self
            .rstype
            .as_ref()
            .expect("PyClass does not have a rust type");
        parse_quote!(#x)
    }
}

impl HasRustPath for PyLiteral {
    fn as_rust_type(&self) -> Type {
        let x = self
            .rstype
            .as_ref()
            .expect("PyLiteral does not have a rust type");
        parse_quote!(#x)
    }
}

impl<E> HasRustPath for PyUnion<E> {
    fn as_rust_type(&self) -> Type {
        let x = &self.rstype;
        parse_quote!(#x)
    }
}

macro_rules! impl_prim_num {
    ($t:ident) => {
        impl<E> HasRustPath for $t<E> {
            fn as_rust_type(&self) -> Type {
                if let Some(x) = self.rstype.as_ref() {
                    parse_quote!(#x)
                } else {
                    self.rs.as_rust_type()
                }
            }
        }
    };
}

impl_prim_num!(PyInt);
impl_prim_num!(PyFloat);

impl HasRustPath for RsInt {
    fn as_rust_type(&self) -> Type {
        match self {
            Self::U8 => parse_quote!(u8),
            Self::U16 => parse_quote!(u16),
            Self::U32 => parse_quote!(u32),
            Self::U64 => parse_quote!(u64),
            Self::Usize => parse_quote!(usize),
            Self::NonZeroU8 => parse_quote!(std::num::NonZeroU8),
            Self::NonZeroUsize => parse_quote!(std::num::NonZeroUsize),
            Self::I32 => parse_quote!(i32),
        }
    }
}

impl HasRustPath for RsFloat {
    fn as_rust_type(&self) -> Type {
        match self {
            Self::F32 => parse_quote!(f32),
            Self::F64 => parse_quote!(f64),
        }
    }
}

impl IsArgType for GetMethod {
    const TYPENAME: &str = "vartype";
    const ARGTYPE: &str = "ivar";

    fn readonly() -> Option<bool> {
        Some(true)
    }
}

impl IsArgType for GetSetMethods {
    const TYPENAME: &str = "vartype";
    const ARGTYPE: &str = "ivar";

    fn readonly() -> Option<bool> {
        Some(false)
    }
}

impl IsArgType for NoMethods {
    const TYPENAME: &str = "type";
    const ARGTYPE: &str = "param";

    fn readonly() -> Option<bool> {
        None
    }
}

impl<T> IsDocArg for DocArg<T> {
    fn argname(&self) -> &str {
        self.argname.as_str()
    }

    fn pytype(&self) -> &ArgPyType {
        &self.pytype
    }

    // fn desc(&self) -> &str {
    //     self.desc.as_str()
    // }

    fn default(&self) -> Option<&DocDefault> {
        self.default.as_ref()
    }

    fn fun_arg(&self) -> TokenStream2 {
        let n = format_ident!("{}", &self.argname);
        let t = &self.pytype.as_rust_type();
        quote!(#n: #t)
    }

    fn ident(&self) -> Ident {
        format_ident!("{}", &self.argname)
    }

    fn ident_into(&self) -> TokenStream2 {
        let n = self.ident();
        if unwrap_generic("Option", unwrap_type_as_path(&self.pytype.as_rust_type())).1 {
            quote! {#n.map(Into::into)}
        } else {
            quote! {#n.into()}
        }
    }

    fn record_into(&self) -> TokenStream2 {
        let n = self.ident();
        if unwrap_generic("Option", unwrap_type_as_path(&self.pytype.as_rust_type())).1 {
            quote! {#n: #n.map(Into::into)}
        } else {
            quote! {#n: #n.into()}
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

    fn pytype(&self) -> &ArgPyType {
        match self {
            Self::RWIvar(x) => x.pytype(),
            Self::ROIvar(x) => x.pytype(),
            Self::Param(x) => x.pytype(),
        }
    }

    // fn desc(&self) -> &str {
    //     match self {
    //         Self::RWIvar(x) => x.desc(),
    //         Self::ROIvar(x) => x.desc(),
    //         Self::Param(x) => x.desc(),
    //     }
    // }

    fn default(&self) -> Option<&DocDefault> {
        match self {
            Self::RWIvar(x) => x.default(),
            Self::ROIvar(x) => x.default(),
            Self::Param(x) => x.default(),
        }
    }

    fn fun_arg(&self) -> TokenStream2 {
        match self {
            Self::RWIvar(x) => x.fun_arg(),
            Self::ROIvar(x) => x.fun_arg(),
            Self::Param(x) => x.fun_arg(),
        }
    }

    fn ident(&self) -> Ident {
        match self {
            Self::RWIvar(x) => x.ident(),
            Self::ROIvar(x) => x.ident(),
            Self::Param(x) => x.ident(),
        }
    }

    fn ident_into(&self) -> TokenStream2 {
        match self {
            Self::RWIvar(x) => x.ident_into(),
            Self::ROIvar(x) => x.ident_into(),
            Self::Param(x) => x.ident_into(),
        }
    }

    fn record_into(&self) -> TokenStream2 {
        match self {
            Self::RWIvar(x) => x.record_into(),
            Self::ROIvar(x) => x.record_into(),
            Self::Param(x) => x.record_into(),
        }
    }
}

impl<R: Clone> AsPyAtom<R> for PyType<R> {
    fn as_atom(&self) -> PyAtom<R> {
        match self {
            Self::Bool(_) => PyAtom::Bool,
            Self::Bytes(_) => PyAtom::Bytes,
            Self::Str(_) => PyAtom::Str,
            Self::Int(_) => PyAtom::Int,
            Self::Float(_) => PyAtom::Float,
            Self::Decimal(_) => PyAtom::Decimal,
            Self::Date(_) => PyAtom::Date,
            Self::Time(_) => PyAtom::Time,
            Self::Datetime(_) => PyAtom::Datetime,
            Self::Literal(x) => PyAtom::Literal(x.clone()),
            Self::PyClass(x) => PyAtom::PyClass(x.clone()),
            Self::List(x) => x.as_atom(),
            Self::Dict(x) => x.as_atom(),
            Self::Option(x) => x.as_atom(),
            Self::Tuple(x) => x.as_atom(),
            Self::Union(x) => x.as_atom(),
        }
    }
}

impl<R: Clone> AsPyAtom<R> for PyList<R> {
    fn as_atom(&self) -> PyAtom<R> {
        PyAtom::List(self.inner.as_atom().into())
    }
}

impl<R: Clone> AsPyAtom<R> for PyDict<R> {
    fn as_atom(&self) -> PyAtom<R> {
        PyAtom::Dict(self.key.as_atom().into(), self.value.as_atom().into())
    }
}

impl<R: Clone> AsPyAtom<R> for PyOpt<R> {
    fn as_atom(&self) -> PyAtom<R> {
        PyAtom::Union(self.inner.as_atom().into(), PyAtom::None.into(), vec![])
    }
}

impl<R: Clone> AsPyAtom<R> for PyTuple<R> {
    fn as_atom(&self) -> PyAtom<R> {
        PyAtom::Tuple(self.inner.iter().map(AsPyAtom::as_atom).collect())
    }
}

impl<R: Clone> AsPyAtom<R> for PyUnion<R> {
    fn as_atom(&self) -> PyAtom<R> {
        let x0 = self.head0.as_atom();
        let x1 = self.head1.as_atom();
        let xs = self.tail.iter().map(AsPyAtom::as_atom).collect();
        PyAtom::Union(x0.into(), x1.into(), xs)
    }
}

impl<T> DocArg<T> {
    fn quoted_methods(&self) -> TokenStream2
    where
        T: IsMethods,
    {
        self.methods.quoted_methods()
    }

    fn def(self, def: DocDefault) -> Self {
        Self::new(
            self.argname,
            self.pytype,
            self.desc,
            Some(def),
            self.methods,
        )
    }

    pub fn def_auto(self) -> Self {
        self.def(DocDefault::Auto)
    }

    fn def_auto_if(self, test: bool) -> Self {
        if test { self.def_auto() } else { self }
    }
}

impl GetMethod {
    fn from_pytype(
        name: &str,
        pytype: &ArgPyType,
        f: impl FnOnce(&Ident, &ArgPyType) -> TokenStream2,
    ) -> Self {
        let get = format_ident!("{name}");
        let ret = pytype.as_rust_type();
        let body = f(&get, pytype);
        Self(quote! {
            fn #get(&self) -> #ret {
                #body
            }
        })
    }
}

impl GetSetMethods {
    fn from_pytype(
        name: &str,
        pytype: &ArgPyType,
        fallible: bool,
        f: impl FnOnce(&Ident, &ArgPyType) -> TokenStream2,
        g: impl FnOnce(&Ident, &ArgPyType) -> TokenStream2,
    ) -> Self {
        let i = format_ident!("{name}");
        let set = format_ident!("set_{name}");
        let ret = pytype.as_rust_type();
        let get_body = f(&i, pytype);
        let set_body = g(&i, pytype);
        let success = if fallible {
            quote!(PyResult<()>)
        } else {
            quote!(())
        };
        Self::new(
            quote! {
                fn #i(&self) -> #ret {
                    #get_body
                }
            },
            quote! {
                fn #set(&mut self, #i: #ret) -> #success {
                    #set_body
                }
            },
        )
    }
}

impl<T> DocReturn<T> {
    pub fn new(rtype: impl Into<T>) -> Self {
        Self {
            rtype: rtype.into(),
            desc: None,
            exceptions: vec![],
        }
    }

    pub fn desc(self, desc: impl fmt::Display) -> Self {
        Self {
            desc: Some(desc.to_string()),
            ..self
        }
    }

    pub fn exc(self, exceptions: impl IntoIterator<Item = impl Into<ReturnPyException>>) -> Self {
        Self {
            exceptions: exceptions.into_iter().map(Into::into).collect(),
            ..self
        }
    }
}

impl From<PyException> for ArgPyException {
    fn from(value: PyException) -> Self {
        Self::new(value, ExcNameMod::default())
    }
}

impl From<PyException> for () {
    fn from(_: PyException) {}
}

impl ExcNameMod {
    fn add_field(self, f: usize) -> Self {
        Self::Field(NonEmpty::new(f), self.into())
    }

    fn add_list(self) -> Self {
        Self::List(self.into())
    }

    fn add_dict_key(self) -> Self {
        Self::DictKey(self.into())
    }

    fn add_dict_val(self) -> Self {
        Self::DictVal(self.into())
    }

    fn fmt(&self, s: &str) -> String {
        match self {
            Self::NoMod => s.to_owned(),
            Self::Field(fs, i) => {
                let xs: Vec<_> = fs.into_iter().map(|x| x + 1).collect();
                let x = fmt_comma_sep_list(&xs[..], "or");
                format!("field {x} in {}", i.fmt(s))
            }
            Self::List(i) => format!("any in {}", i.fmt(s)),
            Self::DictKey(i) => format!("dict key in {}", i.fmt(s)),
            Self::DictVal(i) => format!("dict value in {}", i.fmt(s)),
        }
    }

    fn merge(xs: impl IntoIterator<Item = Self>) -> Vec<Self> {
        // group by top-level types taking field number into account
        let mut has_nomod = false;
        let mut field_trees = vec![];
        let mut list_trees = vec![];
        let mut dict_key_trees = vec![];
        let mut dict_val_trees = vec![];
        for x in xs {
            match x {
                Self::Field(f, t) => field_trees.push((f.head, *t)),
                Self::List(t) => list_trees.push(*t),
                Self::DictKey(t) => dict_key_trees.push(*t),
                Self::DictVal(t) => dict_val_trees.push(*t),
                Self::NoMod => has_nomod = true,
            }
        }

        // if we only have leaves, return early to avoid recursion
        if field_trees.is_empty()
            && list_trees.is_empty()
            && dict_key_trees.is_empty()
            && dict_val_trees.is_empty()
        {
            return has_nomod.then_some(Self::NoMod).into_iter().collect();
        }

        // split trees apart by field number, and group everything underneath
        let mut grouped_field_trees = vec![];

        for (i, ys) in field_trees.into_iter().into_group_map() {
            grouped_field_trees.extend(Self::merge(ys).into_iter().map(|x| (x, i)));
        }

        // now group field by the underlying tree and collect indices
        let grouped_field_trees_ = grouped_field_trees
            .into_iter()
            .into_group_map()
            .into_iter()
            .map(|(tree, fs)| {
                let fs_ = NonEmpty::collect(fs.into_iter().sorted()).unwrap();
                Self::Field(fs_, tree.into())
            });

        // the others are easy, just recurse and collect
        let list_trees_ = Self::merge(list_trees)
            .into_iter()
            .map(Box::new)
            .map(Self::List);
        let dict_key_trees_ = Self::merge(dict_key_trees)
            .into_iter()
            .map(Box::new)
            .map(Self::DictKey);
        let dict_val_trees_ = Self::merge(dict_val_trees)
            .into_iter()
            .map(Box::new)
            .map(Self::DictVal);

        // glue everything together
        has_nomod
            .then_some(Self::NoMod)
            .into_iter()
            .chain(grouped_field_trees_)
            .chain(list_trees_)
            .chain(dict_key_trees_)
            .chain(dict_val_trees_)
            .sorted()
            .collect()
    }
}

impl ArgPyException {
    fn map_mod<F>(self, f: F) -> Self
    where
        F: FnOnce(ExcNameMod) -> ExcNameMod,
    {
        Self::new(self.inner, f(self.argmod))
    }

    fn into_named(self, name: impl Into<String>) -> NamedPyException {
        NamedPyException {
            inner: self,
            names: NonEmpty::new(name.into()),
        }
    }
}

impl PyException {
    fn new(pyname: impl fmt::Display) -> Self {
        Self {
            pyname: pyname.to_string(),
            desc: None,
        }
    }

    fn new_value() -> Self {
        Self::new("ValueError")
    }

    pub fn new_key() -> Self {
        Self::new("KeyError")
    }

    pub fn new_index() -> Self {
        Self::new("IndexError")
    }

    fn new_overflow() -> Self {
        Self::new("OverflowError")
    }

    pub fn new_segment_overflow(version: Version) -> Self {
        let overflow_desc = if version == Version::FCS2_0 {
            "If *TEXT*, *DATA*, or *ANALYSIS* offsets \
             are greater than 99,999,999"
        } else {
            "If *TEXT* offsets are greater than 99,999,999 bytes"
        };
        Self::new_overflow().desc(overflow_desc)
    }

    pub fn new_other_overflow() -> Self {
        Self::new_overflow().desc(
            "If any *OTHER* offsets are greater than \
             99,999,999 and ``big_other`` is ``False``",
        )
    }

    pub fn new_data_loss() -> Self {
        Self::new_pyreflow(&PyreflowError::DataLoss).desc(
            "If any values in *DATA* segment need to be truncated to \
             fit layout and ``skip_conversion_check`` is ``False``",
        )
    }

    pub fn new_pyreflow(p: &PyreflowError) -> Self {
        Self::new(format!("~pyreflow.{p}"))
    }

    pub fn new_invalid_keyword() -> Self {
        Self::new_pyreflow(&PyreflowError::InvalidKeywordValue)
    }

    pub fn new_non_ascii() -> Self {
        Self::new_pyreflow(&PyreflowError::ParseKey).desc(
            "If any keys from *TEXT* contain non-ASCII characters and \
             ``allow_non_ascii_keywords`` is ``False``",
        )
    }

    fn new_config() -> Self {
        Self::new_pyreflow(&PyreflowError::Config)
    }

    pub fn new_extra() -> Self {
        Self::new_pyreflow(&PyreflowError::ExtraKeyword)
            .desc("If any standard keys are unused and not dropped by some other option")
    }

    pub fn new_deprecated() -> Self {
        Self::new_pyreflow(&PyreflowError::FCSDeprecated).desc(
            "If any keywords or their values are deprecated and \
             ``disallow_deprecated`` is ``True``",
        )
    }

    pub fn new_parse_keyval() -> Self {
        Self::new_pyreflow(&PyreflowError::ParseKeywordValue)
            .desc("If any keyword values could not be read from their string encoding")
    }

    pub fn new_event_data() -> Self {
        Self::new_pyreflow(&PyreflowError::EventData).desc("If values in *DATA* cannot be read")
    }

    pub fn new_existing() -> Self {
        Self::new_pyreflow(&PyreflowError::Relational).desc(
            "If keywords are set which refer to measurements and would be \
             invalidated if measurements were removed",
        )
    }

    pub fn desc(self, desc: impl fmt::Display) -> Self {
        Self {
            desc: Some(desc.to_string()),
            ..self
        }
    }
}

impl NamedPyException {
    // TODO keep arg order when sorting names
    fn merge(xs: impl IntoIterator<Item = Self>) -> Vec<Self> {
        xs.into_iter()
            .map(|x| ((x.names.head, x.inner.inner), x.inner.argmod))
            .into_group_map()
            .into_iter()
            .flat_map(|((name, exc), argmod)| {
                ExcNameMod::merge(argmod)
                    .into_iter()
                    .sorted()
                    .map(|a| ((a, exc.clone()), name.clone()))
                    .collect::<Vec<_>>()
            })
            .into_group_map()
            .into_iter()
            .sorted()
            .map(|((argmod, exc), names)| {
                Self::new(
                    NonEmpty::collect(names.into_iter().sorted()).unwrap(),
                    ArgPyException::new(exc, argmod),
                )
            })
            .collect()
    }
}

impl<R: Clone + PartialEq + Eq + Hash> PyAtom<R> {
    fn flatten_unions(self) -> Self {
        fn go<Q: Clone + PartialEq + Eq + Hash>(x: PyAtom<Q>) -> NonEmpty<PyAtom<Q>> {
            match x {
                PyAtom::Union(x0, x1, xs) => {
                    let ys = go(*x0)
                        .into_iter()
                        .chain(go(*x1))
                        .chain(xs.into_iter().flat_map(go));
                    NonEmpty::collect(ys).unwrap()
                }
                y => NonEmpty::new(y.flatten_unions()),
            }
        }
        match self {
            Self::Union(x0, x1, xs) => {
                let mut hasnone = false;
                let mut ys: Vec<_> = go(*x0)
                    .into_iter()
                    .chain(go(*x1))
                    .chain(xs.into_iter().flat_map(go))
                    .filter(|x| {
                        if x == &Self::None {
                            hasnone = true;
                            false
                        } else {
                            true
                        }
                    })
                    .unique()
                    .collect();
                if hasnone {
                    ys.push(Self::None);
                }
                let mut zs = ys.into_iter();
                // ASSUME this won't fail because if we have all Nones then
                // another None should be added
                let n0 = zs.next().unwrap();
                let n1 = zs.next().expect("Tried to flatten union of all 'None'");
                let ns = zs.collect();
                Self::Union(n0.into(), n1.into(), ns)
            }
            Self::List(x) => Self::List(x.flatten_unions().into()),
            Self::Dict(k, v) => Self::Dict(k.flatten_unions().into(), v.flatten_unions().into()),
            Self::Tuple(xs) => Self::Tuple(xs.into_iter().map(Self::flatten_unions).collect()),
            x => x,
        }
    }
}

impl<R> From<RsInt> for PyInt<R> {
    fn from(rs: RsInt) -> Self {
        Self::new(rs, None, None)
    }
}

impl<R> From<RsFloat> for PyFloat<R> {
    fn from(rs: RsFloat) -> Self {
        Self::new(rs, None, None)
    }
}

macro_rules! impl_py_prim_default {
    ($t:ident) => {
        impl<E> $t<E> {
            fn default() -> Self {
                Self::new(None, None)
            }
        }
    };
}

impl_py_prim_default!(PyStr);
impl_py_prim_default!(PyBool);
impl_py_prim_default!(PyBytes);
impl_py_prim_default!(PyDecimal);
impl_py_prim_default!(PyDate);
impl_py_prim_default!(PyTime);
impl_py_prim_default!(PyDatetime);

macro_rules! impl_py_prim_rstype {
    () => {
        pub fn rstype(self, rstype: Path) -> Self {
            Self::new(Some(rstype), self.exc)
        }
    };
}

macro_rules! impl_py_prim_exc {
    () => {
        pub fn exc(self, exc: impl Into<E>) -> Self {
            Self::new(self.rstype, Some(exc.into()))
        }
    };
}

macro_rules! impl_py_prim_doc_default {
    ($py:expr, $rs:path) => {
        fn doc_default(&self) -> (String, TokenStream2) {
            (
                $py,
                self.rstype
                    .as_ref()
                    .map_or(quote!($rs::default()), |y| quote!(#y::default()))
            )
        }
    };
}

macro_rules! impl_py_prim_map_exc {
    ($t:ident) => {
        fn map_exc<F: FnOnce(E) -> E1, E1>(self, f: F) -> $t<E1> {
            $t::new(self.rstype, self.exc.map(f))
        }
    };
}

macro_rules! impl_py_num_defaults {
    ($py:expr) => {
        fn doc_default(&self) -> (String, TokenStream2) {
            let rt = self.rs.as_rust_type();
            (
                $py,
                self.rstype
                    .as_ref()
                    .map_or(quote!(#rt::default()), |y| {
                        let z = path_strip_args(y.clone());
                        quote!(#z::default())
                    }),
            )
        }
    };
}

impl<E> PyInt<E> {
    pub fn rstype(self, rstype: Path) -> Self {
        Self::new(self.rs, Some(rstype), self.exc)
    }

    pub fn exc(self, exc: impl Into<E>) -> Self {
        Self::new(self.rs, self.rstype, Some(exc.into()))
    }

    fn no_exc(self) -> Self {
        Self::new(self.rs, self.rstype, None)
    }

    fn map_exc<F, E1>(self, f: F) -> PyInt<E1>
    where
        F: FnOnce(E) -> E1,
    {
        PyInt::new(self.rs, self.rstype, self.exc.map(f))
    }

    impl_py_num_defaults!("0".into());
}

impl<E: From<PyException>> PyInt<E> {
    pub fn new_nextdata() -> Self {
        let p = keyword_path("Nextdata");
        Self::new_int(RsInt::U64).rstype(p).no_exc()
    }

    pub fn new_meas_index() -> Self {
        let p = parse_quote!(fireflow_core::text::index::MeasIndex);
        Self::new_nonzero_usize().rstype(p).no_exc()
    }

    fn new_dataset_offset() -> Self {
        let p = parse_quote!(fireflow_core::config::DatasetOffset);
        Self::new_int(RsInt::U64).rstype(p).no_exc()
    }

    pub fn new_gate_index() -> Self {
        let p = parse_quote!(fireflow_core::text::index::GateIndex);
        Self::new_nonzero_usize().rstype(p).no_exc()
    }

    pub fn new_prefixed_meas_index() -> Self {
        let p = parse_quote!(fireflow_core::text::keywords::PrefixedMeasIndex);
        Self::new_nonzero_usize().rstype(p).no_exc()
    }

    pub fn new_u32() -> Self {
        Self::new_int(RsInt::U32)
    }

    fn new_nonzero_usize() -> Self {
        Self::new_int(RsInt::NonZeroUsize)
    }

    pub fn new_int(intkind: RsInt) -> Self {
        let e = PyException::new_overflow().desc(intkind.exc_desc());
        Self::from(intkind).exc(e)
    }

    pub fn new_ascii_range_value() -> Self {
        let p = parse_quote!(fireflow_core::validated::ascii_range::AsciiRangeValue);
        Self::new_int(RsInt::U64).rstype(p).no_exc()
    }

    pub fn new_bitmask_value64() -> Self {
        let p = parse_quote!(fireflow_core::validated::bitmask::BitmaskValue<u64>);
        Self::new_int(RsInt::U64).rstype(p).no_exc()
    }

    pub fn new_bitmask(nbytes: usize) -> Self {
        let i = format_ident!("Bitmask{:02}", nbytes * 8);
        let r = match nbytes {
            1 => RsInt::U8,
            2 => RsInt::U16,
            3 | 4 => RsInt::U32,
            5..=8 => RsInt::U64,
            _ => panic!("invalid number of uint bytes: {nbytes}"),
        };
        let e = PyException::new_invalid_keyword().desc(r.exc_desc());
        let path = parse_quote!(fireflow_core::validated::bitmask::#i);
        Self::from(r).rstype(path).exc(e)
    }
}

impl<E> PyFloat<E> {
    pub fn rstype(self, rstype: Path) -> Self {
        Self::new(self.rs, Some(rstype), self.exc)
    }

    pub fn exc(self, exc: impl Into<E>) -> Self {
        Self::new(self.rs, self.rstype, Some(exc.into()))
    }

    fn map_exc<F, E1>(self, f: F) -> PyFloat<E1>
    where
        F: FnOnce(E) -> E1,
    {
        PyFloat::new(self.rs, self.rstype, self.exc.map(f))
    }

    impl_py_num_defaults!("0.0".into());
}

impl<E: From<PyException>> PyFloat<E> {
    pub fn new_non_negative_float() -> Self {
        let e = PyException::new_invalid_keyword()
            .desc("if %x is negative, ``NaN``, ``inf``, or ``-inf``");
        Self::from(RsFloat::F32).exc(e)
    }

    pub fn new_positive_float() -> Self {
        let e = PyException::new_invalid_keyword()
            .desc("if %x is negative, ``0.0``, ``NaN``, ``inf``, or ``-inf``");
        Self::from(RsFloat::F32).exc(e)
    }

    pub fn new_float_range(nbytes: usize) -> Self {
        let i = format_ident!("F{:02}Range", nbytes * 8);
        let r = match nbytes {
            4 => RsFloat::F32,
            8 => RsFloat::F64,
            _ => panic!("invalid number of float bytes: {nbytes}"),
        };
        let msg = format!(
            "if %x is ``NaN``, ``inf``, ``-inf``, \
             or outside the bounds of a {}-bit float",
            nbytes * 8,
        );
        let e = PyException::new_invalid_keyword().desc(msg);
        let path = parse_quote!(fireflow_core::data::#i);
        Self::from(r).rstype(path).exc(e)
    }

    pub fn new_timestep() -> Self {
        let path = keyword_path("Timestep");
        Self::new_positive_float().rstype(path)
    }
}

impl<E> PyStr<E> {
    impl_py_prim_rstype!();
    impl_py_prim_exc!();
    impl_py_prim_doc_default!("\"\"".into(), String);
    impl_py_prim_map_exc!(PyStr);
}

impl<E: From<PyException>> PyStr<E> {
    pub fn new_shortname() -> Self {
        let path = parse_quote!(fireflow_core::validated::shortname::Shortname);
        let e = PyException::new_parse_keyval().desc("if %x is ``\"\"`` or contains commas");
        Self::default().rstype(path).exc(e)
    }

    fn new_keystring() -> Self {
        let path: Path = parse_quote!(fireflow_core::validated::keys::KeyString);
        let e = PyException::new_pyreflow(&PyreflowError::ParseKey)
            .desc("if %x contains non-ASCII characters or is empty");
        Self::default().rstype(path).exc(e)
    }

    fn new_regexp() -> Self {
        let desc = format!("if %x is not a valid regular expression as described in {REGEXP_REF}");
        let exc = PyException::new_config().desc(desc);
        Self::default().exc(exc)
    }

    pub fn new_meas_or_gate_index() -> Self {
        let path = parse_quote!(fireflow_core::text::keywords::MeasOrGateIndex);
        let e = PyException::new_pyreflow(&PyreflowError::ParseKeywordValue).desc(
            "if %x is not like ``P<X>`` or ``G<X>`` \
             where ``X`` is an integer one or greater",
        );
        Self::default().rstype(path).exc(e)
    }

    pub fn new_non_empty_str(path: Path) -> Self {
        let e = PyException::new_invalid_keyword().desc("if %x is empty");
        Self::default().rstype(path).exc(e)
    }

    pub fn new_feature() -> Self {
        let path = keyword_path("Feature");
        Self::default().rstype(path)
    }
}

impl<E> PyBool<E> {
    impl_py_prim_rstype!();
    impl_py_prim_doc_default!("False".into(), bool);
    impl_py_prim_map_exc!(PyBool);
}

impl<E> PyBytes<E> {
    impl_py_prim_rstype!();
    impl_py_prim_doc_default!("b\"\"".into(), Vec);
    impl_py_prim_map_exc!(PyBytes);
}

impl<E: From<PyException>> PyBytes<E> {
    fn new_analysis() -> Self {
        let r = parse_quote!(fireflow_core::core::Analysis);
        Self::default().rstype(r)
    }
}

impl<E> PyDecimal<E> {
    impl_py_prim_rstype!();
    impl_py_prim_doc_default!("0".into(), bigdecimal::BigDecimal);
    impl_py_prim_map_exc!(PyDecimal);
}

impl<E: From<PyException>> PyDecimal<E> {
    pub fn new_range() -> Self {
        let path = parse_quote!(fireflow_core::text::keywords::Range);
        Self::default().rstype(path)
    }
}

impl<E> PyDate<E> {
    impl_py_prim_map_exc!(PyDate);
}

impl<E> PyTime<E> {
    impl_py_prim_map_exc!(PyTime);
}

impl<E> PyDatetime<E> {
    impl_py_prim_rstype!();
    impl_py_prim_map_exc!(PyDatetime);
}

impl<E> PyDict<E> {
    fn new1(key: impl Into<PyType<E>>, value: impl Into<PyType<E>>) -> Self {
        Self::new(key.into(), value.into(), None, None)
    }

    impl_py_prim_doc_default!("{}".into(), std::collections::HashMap);

    fn map_exc<F: Clone + Fn(E) -> E1, E1>(self, f: F) -> PyDict<E1> {
        PyDict::new(
            self.key.map_exc(f.clone()),
            self.value.map_exc(f.clone()),
            self.rstype,
            self.exc.map(f),
        )
    }
}

impl<E: From<PyException>> PyDict<E> {
    fn new_keystring_pairs() -> Self {
        let path: Path = parse_quote!(fireflow_core::validated::keys::KeyStringPairs);
        // TODO exception if dict keys are not unique
        Self::new(PyStr::new_keystring(), PyStr::new_keystring(), path, None)
    }

    pub fn new_std_keywords() -> Self {
        let path = parse_quote!(fireflow_core::validated::keys::StdKey);
        let e = PyException::new_pyreflow(&PyreflowError::ParseKey).desc(
            "if %x is empty, does not start with \
             ``\"$\"``, or is only a ``\"$\"``",
        );
        Self::new1(PyStr::default().rstype(path).exc(e), PyStr::default())
    }

    pub fn new_nonstd_keywords() -> Self {
        let path = parse_quote!(fireflow_core::validated::keys::NonStdKey);
        let e = PyException::new_pyreflow(&PyreflowError::ParseKey)
            .desc("if %x is empty or starts with ``\"$\"``");
        Self::new1(PyStr::default().rstype(path).exc(e), PyStr::default())
    }

    pub fn new_keywords() -> Self {
        Self::new1(PyStr::default(), PyStr::default())
    }
}

impl<E> PyList<E> {
    pub fn new1(inner: impl Into<PyType<E>>) -> Self {
        Self::new(inner, None, None)
    }

    pub fn exc(self, exc: impl Into<E>) -> Self {
        Self::new(self.inner, self.rstype, Some(exc.into()))
    }

    impl_py_prim_doc_default!("[]".into(), Vec);

    fn map_exc<F: Clone + Fn(E) -> E1, E1>(self, f: F) -> PyList<E1> {
        PyList::new(self.inner.map_exc(f.clone()), self.rstype, self.exc.map(f))
    }
}

impl<E: From<PyException>> PyList<E> {
    fn new_non_empty(inner: impl Into<PyType<E>>, inner_path: &Path) -> Self {
        let nonempty = quote!(fireflow_core::nonempty::FCSNonEmpty);
        let e = PyException::new_invalid_keyword().desc("if %x is empty");
        Self::new(
            inner,
            Some(parse_quote!(#nonempty<#inner_path>)),
            Some(e.into()),
        )
    }

    fn new_others() -> Self {
        let path: Path = parse_quote!(fireflow_core::core::Others);
        Self::new(PyBytes::default(), path, None)
    }

    pub fn new_vertices() -> Self {
        let inner_path = keyword_path("Vertex");
        let inner = PyTuple::new2(vec![RsFloat::F32; 2]);
        Self::new_non_empty(inner, &inner_path)
    }
}

impl PyLiteral {
    pub fn new1(iter: impl IntoIterator<Item = &'static str>) -> Self {
        let mut it = iter.into_iter();
        let head = it.next().expect("Literal cannot be empty");
        Self::new(head, it, None)
    }

    pub fn new2(iter: impl IntoIterator<Item = &'static str>, rstype: Path) -> Self {
        let mut x = Self::new1(iter);
        x.rstype = Some(rstype);
        x
    }

    pub fn new_version() -> Self {
        let path = parse_quote!(fireflow_core::header::Version);
        Self::new2(ALL_VERSION_STRINGS, path)
    }

    fn new_version_override() -> Self {
        let path = config_path("VersionOverride");
        let vs = ALL_VERSION_STRINGS
            .into_iter()
            .chain(["latest", "earliest", "loose", "strict"]);
        Self::new2(vs, path)
    }

    fn new_temporal_optical_key() -> Self {
        Self::new2(
            [
                "F",
                "L",
                "O",
                "T",
                "P",
                "V",
                "CALIBRATION",
                "DET",
                "TAG",
                "FEATURE",
                "ANALYTE",
            ],
            parse_quote!(TemporalOpticalKeys),
        )
    }

    pub fn new_datatype() -> Self {
        let path = parse_quote!(fireflow_core::text::keywords::AlphaNumType);
        Self::new2(["A", "I", "F", "D"], path)
    }

    pub fn new_awh_feature() -> Self {
        let path = keyword_path("OpticalFeature");
        Self::new2(["Area", "Width", "Height"], path)
    }

    fn new_endian() -> Self {
        let endian: Path = parse_quote!(fireflow_core::text::byteord::Endian);
        Self::new2(["little", "big"], endian)
    }
}

impl<E> PyOpt<E> {
    fn doc_default() -> (String, TokenStream2) {
        ("None".into(), quote!(None))
    }

    pub fn wrap_if(inner: impl Into<PyType<E>>, test: bool) -> PyType<E> {
        if test {
            Self::new(inner).into()
        } else {
            inner.into()
        }
    }

    pub fn map_exc<F: Clone + Fn(E) -> E1, E1>(self, f: F) -> PyOpt<E1> {
        PyOpt::new(self.inner.map_exc(f))
    }
}

impl<E> Default for PyTuple<E> {
    fn default() -> Self {
        Self::new(vec![], None, None)
    }
}

impl<E> PyTuple<E> {
    pub fn new1(x: impl Into<PyType<E>>) -> Self {
        Self::new(vec![x.into()], None, None)
    }

    pub fn new2(iter: impl IntoIterator<Item = impl Into<PyType<E>>>) -> Self {
        Self::new(iter.into_iter().map(Into::into).collect(), None, None)
    }

    pub fn add_new(mut self, x: impl Into<PyType<E>>) -> Self {
        self.inner.push(x.into());
        self
    }

    pub fn rstype(self, rstype: Path) -> Self {
        Self::new(self.inner, Some(rstype), self.exc)
    }

    pub fn exc(self, exc: impl Into<E>) -> Self {
        Self::new(self.inner, self.rstype, Some(exc.into()))
    }

    fn map_exc<F: Clone + Fn(E) -> E1, E1>(self, f: F) -> PyTuple<E1> {
        PyTuple::new(
            self.inner
                .into_iter()
                .map(|x| x.map_exc(f.clone()))
                .collect(),
            self.rstype,
            self.exc.map(f),
        )
    }
}

impl<E: From<PyException>> PyTuple<E> {
    fn new_sub_patterns() -> Self {
        let path: Path = parse_quote!(fireflow_core::validated::sub_pattern::SubPatterns);
        let lit = PyDict::new1(PyStr::new_keystring(), Self::new_sub_pattern());
        let pat = PyDict::new1(PyStr::new_regexp(), Self::new_sub_pattern());
        Self::new2([lit, pat]).rstype(path)
    }

    fn new_sub_pattern() -> Self {
        let desc = "if references in replacement string in %x \
                    do not match captures in regular expression";
        let exc = PyException::new_config().desc(desc);
        Self::new1(PyStr::new_regexp())
            .add_new(PyStr::default())
            .add_new(PyBool::default())
            .exc(exc)
    }

    pub fn new_calibration3_1() -> Self {
        Self::new1(PyFloat::new_positive_float())
            .add_new(PyStr::default())
            .rstype(keyword_path("Calibration3_1"))
    }

    pub fn new_calibration3_2() -> Self {
        Self::new1(PyFloat::new_positive_float())
            .add_new(RsFloat::F32)
            .add_new(PyStr::default())
            .rstype(keyword_path("Calibration3_2"))
    }

    pub fn new_display() -> Self {
        let desc = "if %x represents a log display (field 1 is ``True``) and \
                    the two floats are not both positive";
        let exc = PyException::new_value().desc(desc);
        Self::new1(PyBool::default())
            .add_new(RsFloat::F32)
            .add_new(RsFloat::F32)
            .exc(exc)
            .rstype(keyword_path("Display"))
    }

    fn new_relative_segment(n: &str) -> Self {
        let t = format_ident!("{n}");
        let i = quote!(fireflow_core::segment::#t);
        let p = parse_quote!(fireflow_core::segment::RelativeSegment<#i>);
        let desc = "if %x has offsets which exceed the end of the file or \
                    are inverted (begin after end)";
        let exc = PyException::new_value().desc(desc);
        Self::new2(vec![RsInt::U64; 2]).exc(exc).rstype(p)
    }

    fn new_segment(n: &str) -> Self {
        let t = format_ident!("{n}");
        let p = parse_quote!(fireflow_core::segment::#t);
        let desc = "if %x has offsets which exceed the end of the file, \
                    are inverted (begin after end), or are either negative \
                    or greater than ``2**64-1``";
        let exc = PyException::new_value().desc(desc);
        // NOTE don't use ints with overflow exceptions since this is captured
        // in the overall exception for the entire type
        Self::new2(vec![RsInt::U64; 2]).exc(exc).rstype(p)
    }

    fn new_text_segment() -> Self {
        Self::new_segment("PrimaryTextSegment")
    }

    pub fn new_supp_text_segment() -> Self {
        Self::new_segment("SupplementalTextSegment")
    }

    fn new_other_segment() -> Self {
        Self::new_segment("OtherSegment20")
    }

    fn new_data_segment(src: SegmentSrc) -> Self {
        let id = match src {
            SegmentSrc::Header => "HeaderDataSegment",
            SegmentSrc::Any => "AnyDataSegment",
        };
        Self::new_segment(id)
    }

    fn new_analysis_segment(src: SegmentSrc) -> Self {
        let id = match src {
            SegmentSrc::Header => "HeaderAnalysisSegment",
            SegmentSrc::Any => "AnyAnalysisSegment",
        };
        Self::new_segment(id)
    }

    fn new_correction(is_header: bool, id: &str) -> Self {
        let path = correction_path(is_header, id);
        Self::new2([PyInt::new_int(RsInt::I32), PyInt::new_int(RsInt::I32)]).rstype(path)
    }

    fn new_tr() -> Self {
        let path = keyword_path("Trigger");
        Self::new1(PyInt::new_u32())
            .add_new(PyStr::new_shortname())
            .rstype(path)
    }

    fn new_meas(version: Version) -> Self {
        let name_pytype = PyType::new_versioned_shortname(version);
        let name_rstype = name_pytype.as_rust_type();
        let meas_opt_pyname = pyoptical(version);
        let meas_tmp_pyname = pytemporal(version);
        let meas_argtype =
            parse_quote!(PyEithers<#name_rstype, #meas_tmp_pyname, #meas_opt_pyname>);
        Self::new1(name_pytype)
            .add_new(PyUnion::new_measurement(version))
            .rstype(meas_argtype)
    }

    pub fn new_unigate() -> Self {
        Self::new2([PyDecimal::default(), PyDecimal::default()]).rstype(keyword_path("UniGate"))
    }

    fn new_key_patterns() -> Self {
        let path: Path = parse_quote!(fireflow_core::validated::keys::KeyPatterns);
        Self::new2([
            PyList::new1(PyStr::new_keystring()),
            PyList::new1(PyStr::new_regexp()),
        ])
        .rstype(path)
    }
}

impl<E> PyUnion<E> {
    fn new1<T, A>(iter: T, rstype: Path) -> Self
    where
        T: IntoIterator<Item = A>,
        A: Into<PyType<E>>,
    {
        let mut it = iter.into_iter();
        let x0 = it.next().expect("Union cannot be empty");
        let x1 = it.next().expect("Union must have at least 2 types");
        let xs = it.map(Into::into).collect();
        Self::new(x0, x1, xs, rstype, None)
    }

    pub fn new2(x: impl Into<PyType<E>>, y: impl Into<PyType<E>>, rstype: Path) -> Self {
        Self::new(x, y, vec![], rstype, None)
    }

    pub fn exc(self, exc: impl Into<E>) -> Self {
        Self::new(
            self.head0,
            self.head1,
            self.tail,
            self.rstype,
            Some(exc.into()),
        )
    }

    fn map_exc<F: Clone + Fn(E) -> E1, E1>(self, f: F) -> PyUnion<E1> {
        PyUnion::new(
            self.head0.map_exc(f.clone()),
            self.head1.map_exc(f.clone()),
            self.tail
                .into_iter()
                .map(|x| x.map_exc(f.clone()))
                .collect(),
            self.rstype,
            self.exc.map(f),
        )
    }
}

impl<E: From<PyException>> PyUnion<E> {
    pub fn new_measurement(version: Version) -> Self {
        let element_path = element_path(version);
        Self::new2(
            PyClass::new_optical(version),
            PyClass::new_temporal(version),
            element_path,
        )
    }

    pub fn new_scale(is_gate: bool) -> Self {
        let name = if is_gate { "GateScale" } else { "Scale" };
        let exc = PyException::new_invalid_keyword()
            .desc("if %x has log scale floats which are not both positive");
        Self::new2(
            PyTuple::default(),
            PyTuple::new2(vec![RsFloat::F32; 2]),
            keyword_path(name),
        )
        .exc(exc)
    }

    pub fn new_transform() -> Self {
        let path = parse_quote! {fireflow_core::core::ScaleTransform};
        let exc = PyException::new_invalid_keyword()
            .desc("if %x has log scale floats which are not both positive");
        // TODO the linear gain should also be positive
        Self::new2(RsFloat::F32, PyTuple::new2(vec![RsFloat::F32; 2]), path).exc(exc)
    }

    pub fn new_byteord(nbytes: usize) -> Self {
        let sizedbyteord_path: Path = parse_quote!(fireflow_core::text::byteord::SizedByteOrd);
        let exc = PyException::new_invalid_keyword().desc(format!(
            "if %x is not \"little\", \"big\", or a list of \
             all integers from 1 to {nbytes} in any order"
        ));
        let path = parse_quote!(#sizedbyteord_path<#nbytes>);
        Self::new2(PyLiteral::new_endian(), PyList::new1(RsInt::U32), path).exc(exc)
    }

    pub fn new_anycoretext() -> Self {
        Self::new1(
            ALL_VERSIONS.into_iter().map(PyClass::new_coretext),
            parse_quote!(PyAnyCoreTEXT),
        )
    }

    pub fn new_anycoredataset() -> Self {
        Self::new1(
            ALL_VERSIONS.into_iter().map(PyClass::new_coredataset),
            parse_quote!(PyAnyCoreDataset),
        )
    }
}

impl<E> PyClass<E> {
    pub fn new1(pyname: impl fmt::Display) -> Self {
        Self::new(pyname.to_string(), None, None)
    }

    pub fn rstype(self, rstype: Path) -> Self {
        Self::new(self.pyname, Some(rstype), None)
    }

    pub fn exc(self, exc: impl Into<E>) -> Self {
        Self::new(self.pyname, self.rstype, Some(exc.into()))
    }

    pub fn new_py(
        modpath: impl IntoIterator<Item = impl fmt::Display>,
        name: impl fmt::Display,
    ) -> Self {
        let pyname = format_ident!("Py{name}");
        let m = once("~pyreflow".into())
            .chain(modpath.into_iter().map(|x| x.to_string()))
            .chain([format!("{name}")])
            .join(".");
        Self::new1(m).rstype(parse_quote!(#pyname))
    }

    fn map_exc<F: FnOnce(E) -> E1, E1>(self, f: F) -> PyClass<E1> {
        PyClass::new(self.pyname, self.rstype, self.exc.map(f))
    }
}

impl<E: From<PyException>> PyClass<E> {
    pub fn new_optical(version: Version) -> Self {
        let n = format!("Optical{}", version.short_underscore());
        Self::new_py([""; 0], n)
    }

    pub fn new_temporal(version: Version) -> Self {
        let n = format!("Temporal{}", version.short_underscore());
        Self::new_py([""; 0], n)
    }

    pub fn new_dataframe(polars_type: bool) -> Self {
        let path = if polars_type {
            parse_quote!(pyo3_polars::PyDataFrame)
        } else {
            parse_quote!(fireflow_core::validated::dataframe::FCSDataFrame)
        };
        Self::new1("polars.DataFrame").rstype(path)
    }

    pub fn new_series() -> Self {
        let path: Path = parse_quote!(fireflow_core::validated::dataframe::AnyFCSColumn);
        Self::new1("polars.Series").rstype(path)
    }

    pub fn new_coretext(version: Version) -> Self {
        let v = version.short_underscore();
        Self::new_py([""; 0], format!("CoreTEXT{v}"))
    }

    pub fn new_coredataset(version: Version) -> Self {
        let v = version.short_underscore();
        Self::new_py([""; 0], format!("CoreDataset{v}"))
    }
}

impl<E> PyType<E> {
    fn map_exc<F: Clone + Fn(E) -> E1, E1>(self, f: F) -> PyType<E1> {
        match self {
            Self::Bool(x) => x.map_exc(f).into(),
            Self::Bytes(x) => x.map_exc(f).into(),
            Self::Str(x) => x.map_exc(f).into(),
            Self::Int(x) => x.map_exc(f).into(),
            Self::Float(x) => x.map_exc(f).into(),
            Self::Decimal(x) => x.map_exc(f).into(),
            Self::List(x) => x.map_exc(f).into(),
            Self::Dict(x) => x.map_exc(f).into(),
            Self::Date(x) => x.map_exc(f).into(),
            Self::Time(x) => x.map_exc(f).into(),
            Self::Datetime(x) => x.map_exc(f).into(),
            Self::PyClass(x) => x.map_exc(f).into(),
            Self::Option(x) => x.map_exc(f).into(),
            Self::Union(x) => x.map_exc(f).into(),
            Self::Tuple(xs) => xs.map_exc(f).into(),
            Self::Literal(x) => x.into(),
        }
    }

    fn doc_default(&self) -> (String, TokenStream2) {
        match self {
            Self::Bool(x) => x.doc_default(),
            Self::Bytes(x) => x.doc_default(),
            Self::Str(x) => x.doc_default(),
            Self::Int(x) => x.doc_default(),
            Self::Float(x) => x.doc_default(),
            Self::Decimal(x) => x.doc_default(),
            Self::List(x) => x.doc_default(),
            Self::Dict(x) => x.doc_default(),
            Self::Option(_) => PyOpt::<E>::doc_default(),
            Self::Literal(x) => {
                let rt = &x.rstype;
                (format!("\"{}\"", x.head), quote!(#rt::default()))
            }
            Self::Union(x) => {
                let rt = path_strip_args(x.rstype.clone());
                let (pt, _) = x.head0.doc_default();
                (pt, quote!(#rt::default()))
            }
            Self::Tuple(xs) => {
                let (ps, rs): (Vec<_>, Vec<_>) = xs.inner.iter().map(Self::doc_default).unzip();
                (
                    format!("({})", ps.into_iter().join(", ")),
                    xs.rstype.as_ref().map_or(quote!((#(#rs),*)), |y| {
                        let z = path_strip_args(y.clone());
                        quote!(#z::default())
                    }),
                )
            }
            Self::Date(_) => panic!("No default for date"),
            Self::Time(_) => panic!("No default for time"),
            Self::Datetime(_) => panic!("No default for datetime"),
            Self::PyClass(_) => panic!("No default for arbitrary class"),
        }
    }
}

impl<E: From<PyException>> PyType<E> {
    pub fn new_versioned_shortname(version: Version) -> Self {
        if version < Version::FCS3_1 {
            PyOpt::new(PyStr::new_shortname()).into()
        } else {
            let inner = quote!(fireflow_core::validated::shortname::Shortname);
            let outer = parse_quote!(fireflow_core::text::optional::Identity<#inner>);
            PyStr::new_shortname().rstype(outer).into()
        }
    }
}

impl ArgPyType {
    fn as_exceptions(&self) -> Vec<ArgPyException> {
        let go = |e: &Option<ArgPyException>| e.iter().cloned().collect();
        let walk = |mut acc: Vec<ArgPyException>, pt: &Self| {
            acc.extend(pt.as_exceptions());
            acc
        };
        // TODO clean this up
        match self {
            Self::Bool(x) => go(&x.exc),
            Self::Bytes(x) => go(&x.exc),
            Self::Str(x) => go(&x.exc),
            Self::Int(x) => go(&x.exc),
            Self::Float(x) => go(&x.exc),
            Self::Decimal(x) => go(&x.exc),
            Self::Date(x) => go(&x.exc),
            Self::Time(x) => go(&x.exc),
            Self::Datetime(x) => go(&x.exc),
            Self::PyClass(x) => go(&x.exc),
            Self::Option(x) => walk(vec![], &x.inner),
            Self::Union(x) => {
                let acc0 = x.exc.iter().cloned().collect();
                let acc1 = walk(walk(acc0, &x.head0), &x.head1);
                x.tail.iter().fold(acc1, walk)
            }
            Self::List(x) => {
                let y = x
                    .inner
                    .clone()
                    .map_exc(|e| e.map_mod(ExcNameMod::add_list))
                    .as_exceptions();
                x.exc.iter().cloned().chain(y).collect()
            }
            Self::Dict(x) => {
                let k = x
                    .key
                    .clone()
                    .map_exc(|e| e.map_mod(ExcNameMod::add_dict_key))
                    .as_exceptions();
                let v = x
                    .value
                    .clone()
                    .map_exc(|e| e.map_mod(ExcNameMod::add_dict_val))
                    .as_exceptions();
                x.exc.iter().cloned().chain(k).chain(v).collect()
            }
            Self::Tuple(xs) => {
                let fmt = |i, x: Self| x.map_exc(|e| e.map_mod(|m| ExcNameMod::add_field(m, i)));
                let mut ys = xs.inner.iter().cloned().enumerate().map(|(i, x)| fmt(i, x));
                let acc0 = xs.exc.clone().into_iter().collect();
                if let Some(y) = ys.next() {
                    let acc = walk(acc0, &y);
                    ys.fold(acc, |a, x| walk(a, &x))
                } else {
                    acc0
                }
            }
            Self::Literal(_) => vec![],
        }
    }
}

impl RsInt {
    fn lower(&self) -> &'static str {
        match self {
            Self::U8 | Self::U16 | Self::U32 | Self::U64 | Self::Usize => "0",
            Self::NonZeroU8 | Self::NonZeroUsize => "1",
            Self::I32 => "-2**31",
        }
    }

    fn upper(&self) -> String {
        match self {
            Self::U8 | Self::NonZeroU8 => "255".into(),
            Self::U16 => "2**16-1".into(),
            Self::U32 => "2**32-1".into(),
            Self::I32 => "2**31-1".into(),
            Self::U64 => "2**64-1".into(),
            Self::Usize | Self::NonZeroUsize => format!("2**{}-1", usize::BITS),
        }
    }

    fn exc_desc(&self) -> String {
        format!(
            "if %x is less than ``{}`` or greater than ``{}``",
            self.lower(),
            self.upper()
        )
    }
}

impl DocArgRWIvar {
    pub fn new_ivar_rw(
        argname: impl fmt::Display,
        pytype: impl Into<ArgPyType>,
        desc: impl fmt::Display,
        fallible: bool,
        f: impl FnOnce(&Ident, &ArgPyType) -> TokenStream2,
        g: impl FnOnce(&Ident, &ArgPyType) -> TokenStream2,
    ) -> Self {
        DocArgParam::new_param(argname, pytype, desc).into_rw(fallible, f, g)
    }

    pub fn new_opt_ivar_rw(
        argname: impl fmt::Display,
        pytype: impl Into<ArgPyType>,
        desc: impl fmt::Display,
        fallible: bool,
        f: impl FnOnce(&Ident, &ArgPyType) -> TokenStream2,
        g: impl FnOnce(&Ident, &ArgPyType) -> TokenStream2,
    ) -> Self {
        let pt = PyOpt::new(pytype.into());
        Self::new_ivar_rw(argname, pt, desc, fallible, f, g).def_auto()
    }

    pub fn new_kw_ivar<F, T>(kw: &str, name: &str, f: F, desc: Option<&str>, def: bool) -> Self
    where
        F: FnOnce(Path) -> T,
        T: Into<ArgPyType>,
    {
        let path = keyword_path(kw);
        let pytype: ArgPyType = f(path.clone()).into();

        let d = desc.map_or(format!("Value of *${}*.", name.to_uppercase()), Into::into);

        let get_f = |_: &Ident, pt: &ArgPyType| {
            let optional = matches!(pt, PyType::Option(_));
            let get_inner = format_ident!("{}", if optional { "metaroot_opt" } else { "metaroot" });
            let clone_inner = format_ident!("{}", if optional { "cloned" } else { "clone" });
            quote!(self.0.#get_inner::<#path>().#clone_inner())
        };
        let set_f = |n: &Ident, _: &ArgPyType| quote!(self.0.set_metaroot(#n));

        Self::new_ivar_rw(name, pytype, d, false, get_f, set_f).def_auto_if(def)
    }

    pub fn new_kw_ivar_str(kw: &str, name: &str) -> Self {
        Self::new_kw_ivar(kw, name, |p| PyStr::default().rstype(p), None, true)
    }

    pub fn new_meas_kw_ivar<F, T>(kw: &str, name: &str, f: F, desc: Option<&str>, def: bool) -> Self
    where
        F: FnOnce(Path) -> T,
        T: Into<ArgPyType>,
    {
        let path = keyword_path(kw);
        let pytype: ArgPyType = f(path).into();
        let full_path = pytype.as_rust_type();

        let d = desc.map_or(format!("Value of *${}*.", name.to_uppercase()), Into::into);

        let get_f = |_: &Ident, pt: &ArgPyType| {
            if matches!(pt, PyType::Option(_)) {
                quote! {
                    let x: &#full_path = self.0.as_ref();
                    x.as_ref().cloned()
                }
            } else {
                quote! {
                    let x: &#full_path = self.0.as_ref();
                    x.clone()
                }
            }
        };
        let set_f = |n: &Ident, _: &ArgPyType| quote!(*self.0.as_mut() = #n);

        Self::new_ivar_rw(name, pytype, d, false, get_f, set_f).def_auto_if(def)
    }

    pub fn new_kw_opt_ivar<F, T>(kw: &str, name: &str, f: F) -> Self
    where
        F: FnOnce(Path) -> T,
        T: Into<ArgPyType>,
    {
        Self::new_kw_ivar(kw, name, |p| PyOpt::new(f(p)), None, true)
    }

    pub fn new_meas_kw_ivar1<F, T>(kw: &str, name: &str, abbr: &str, f: F) -> Self
    where
        F: FnOnce(Path) -> T,
        T: Into<ArgPyType>,
    {
        let desc = format!("Value for *$Pn{abbr}*.");
        Self::new_meas_kw_ivar(kw, name, f, Some(desc.as_str()), true)
    }

    pub fn new_meas_kw_opt_ivar<F, T>(kw: &str, name: &str, abbr: &str, f: F) -> Self
    where
        F: FnOnce(Path) -> T,
        T: Into<ArgPyType>,
    {
        Self::new_meas_kw_ivar1(kw, name, abbr, |p| PyOpt::new(f(p)))
    }

    pub fn new_meas_kw_str(kw: &str, name: &str, abbr: &str) -> Self {
        Self::new_meas_kw_ivar1(kw, name, abbr, |p| PyStr::default().rstype(p))
    }

    pub fn new_layout_ivar(version: Version) -> Self {
        let ascii_layouts = ["FixedAsciiLayout", "DelimAsciiLayout"];
        let non_mixed_layouts = ["EndianUintLayout", "EndianF32Layout", "EndianF64Layout"];
        let ordered_layouts = [
            "OrderedUint08Layout",
            "OrderedUint16Layout",
            "OrderedUint24Layout",
            "OrderedUint32Layout",
            "OrderedUint40Layout",
            "OrderedUint48Layout",
            "OrderedUint56Layout",
            "OrderedUint64Layout",
            "OrderedF32Layout",
            "OrderedF64Layout",
        ];

        let layout_pytype = match version {
            Version::FCS3_2 => {
                let ys = ascii_layouts
                    .into_iter()
                    .chain(non_mixed_layouts)
                    .chain(["MixedLayout"])
                    .map(PyClass::new1);
                PyUnion::new1(ys, parse_quote!(PyLayout3_2))
            }
            Version::FCS3_1 => {
                let ys = ascii_layouts
                    .into_iter()
                    .chain(non_mixed_layouts)
                    .map(PyClass::new1);
                PyUnion::new1(ys, parse_quote!(PyNonMixedLayout))
            }
            _ => {
                let ys = ascii_layouts
                    .into_iter()
                    .chain(ordered_layouts)
                    .map(PyClass::new1);
                PyUnion::new1(ys, parse_quote!(PyOrderedLayout))
            }
        };
        let layout_desc = if version == Version::FCS3_2 {
            "Layout to describe data encoding. Represents *$PnB*, *$PnR*, *$BYTEORD*, \
             *$DATATYPE*, and *$PnDATATYPE*."
        } else {
            "Layout to describe data encoding. Represents *$PnB*, *$PnR*, *$BYTEORD*, \
             and *$DATATYPE*."
        };

        Self::new_ivar_rw(
            "layout",
            layout_pytype,
            layout_desc,
            true,
            |_, _| quote!(self.0.layout().clone().into()),
            |_, _| quote!(Ok(self.0.set_layout(layout.into())?)),
        )
    }

    pub fn new_df_ivar() -> Self {
        // use polars df here because we need to manually add names
        DocArg::new_data_param(true).into_rw(
            true,
            |_, pt| {
                let rt = pt.as_rust_type();
                quote! {
                    let ns = self.0.all_shortnames();
                    let data = self.0.data();
                    #rt(data.as_polars_dataframe(&ns[..]))
                }
            },
            |n, _| {
                quote! {
                    let d = #n.0.try_into()?;
                    Ok(self.0.set_data(d)?)
                }
            },
        )
    }

    pub fn new_analysis_ivar() -> Self {
        DocArg::new_analysis_param(true).into_rw(
            false,
            |_, _| quote!(self.0.analysis().clone()),
            |n, _| quote!(*self.0.analysis_mut() = #n.into()),
        )
    }

    pub fn new_others_ivar() -> Self {
        DocArg::new_others_param(true).into_rw(
            false,
            |_, _| quote!(self.0.others().clone()),
            |n, _| quote!(*self.0.others_mut() = #n.into()),
        )
    }

    pub fn new_timestamps_ivar() -> [Self; 3] {
        let make_time_ivar = |is_start: bool| {
            let name = if is_start { "btim" } else { "etim" };
            let get_naive = format_ident!("{name}_naive");
            let set_naive = format_ident!("set_{name}_naive");
            let desc = format!("Value of *${}*.", name.to_uppercase());
            Self::new_opt_ivar_rw(
                name,
                PyTime::default(),
                desc,
                true,
                |_, _| quote!(self.0.#get_naive()),
                |n, _| quote!(Ok(self.0.#set_naive(#n)?)),
            )
        };

        let date_arg = Self::new_opt_ivar_rw(
            "date",
            PyDate::default(),
            "Value of *$DATE*.",
            true,
            |_, _| quote!(self.0.date_naive()),
            |n, _| quote!(Ok(self.0.set_date_naive(#n)?)),
        );

        [make_time_ivar(true), make_time_ivar(false), date_arg]
    }

    pub fn new_datetime_ivar(is_start: bool) -> Self {
        let name = if is_start {
            "begindatetime"
        } else {
            "enddatetime"
        };
        let get = format_ident!("{name}");
        let set = format_ident!("set_{name}");
        Self::new_opt_ivar_rw(
            name,
            PyDatetime::default(),
            format!("Value for *${}*.", name.to_uppercase()),
            true,
            |_, _| quote!(self.0.#get()),
            |n, _| quote!(Ok(self.0.#set(#n)?)),
        )
    }

    pub fn new_comp_ivar(is_2_0: bool) -> Self {
        let rstype: Path = parse_quote!(fireflow_core::text::compensation::Compensation);
        let desc = if is_2_0 {
            "The compensation matrix. Must be a square array with number of \
             rows/columns equal to the number of measurements. Non-zero entries \
             will produce a *$DFCmTOn* keyword."
        } else {
            "The value of *$COMP*. Must be a square array with number of \
             rows/columns equal to the number of measurements."
        };
        Self::new_opt_ivar_rw(
            "comp",
            PyClass::new1("~numpy.ndarray").rstype(rstype),
            desc,
            true,
            |_, _| quote!(self.0.compensation().cloned()),
            |n, _| quote!(Ok(self.0.set_compensation(#n)?)),
        )
    }

    pub fn new_spillover_ivar() -> Self {
        let rstype: Path = parse_quote!(fireflow_core::text::spillover::Spillover);
        let matrix_exc = PyException::new_invalid_keyword()
            .desc("if %x is not a square matrix that is 2x2 or larger");
        let spill_exc = PyException::new_invalid_keyword().desc(
            "if matrix in %x does not have the same number of rows \
             and columns as the measurement vector",
        );
        // TODO add exception for when $PnN don't match
        Self::new_opt_ivar_rw(
            "spillover",
            PyTuple::new1(PyList::new1(PyStr::new_shortname()))
                .add_new(PyClass::new1("~numpy.ndarray").exc(matrix_exc))
                .rstype(rstype)
                .exc(spill_exc),
            "Value for *$SPILLOVER*. First element of tuple the list of measurement \
             names and the second is the matrix. Each measurement name must \
             correspond to a *$PnN*, must be unique, and the length of this list \
             must match the number of rows and columns of the matrix. The matrix \
             must be at least 2x2.",
            true,
            |_, _| quote!(self.0.spillover().map(|x| x.clone())),
            |n, _| quote!(Ok(self.0.set_spillover(#n)?)),
        )
    }

    pub fn new_csvflags_ivar() -> Self {
        let path: Path = parse_quote!(fireflow_core::core::CSVFlags);
        Self::new_ivar_rw(
            "csvflags",
            PyList::new(PyOpt::new(PyInt::new_u32()), path.clone(), None),
            "Subset flags. Each element in the list corresponds to *$CSVnFLAG* and \
             the length of the list corresponds to *$CSMODE*.",
            false,
            |_, _| quote!(self.0.metaroot::<#path>().clone()),
            |n, _| quote!(self.0.set_metaroot(#n)),
        )
        .def_auto()
    }

    // TODO exception for mismatch PnN
    pub fn new_trigger_ivar() -> Self {
        Self::new_opt_ivar_rw(
            "tr",
            PyTuple::new_tr(),
            "Value for *$TR*. First member of tuple is threshold and second is the \
             measurement name which must match a *$PnN*.",
            true,
            |_, _| quote!(self.0.metaroot_opt().cloned()),
            |n, _| quote!(Ok(self.0.set_trigger(#n)?)),
        )
    }

    pub fn new_unstainedcenters_ivar() -> Self {
        let path = keyword_path("UnstainedCenters");
        // TODO exceptions for links
        Self::new_ivar_rw(
            "unstainedcenters",
            PyDict::new(PyStr::new_shortname(), RsFloat::F32, path.clone(), None),
            "Value for *$UNSTAINEDCENTERS. Each key must match a *$PnN*.",
            true,
            |_, _| quote!(self.0.metaroot::<#path>().clone()),
            |n, _| quote!(Ok(self.0.set_unstained_centers(#n)?)),
        )
        .def_auto()
    }

    pub fn new_applied_gates_ivar(version: Version) -> Self {
        // TODO there are version-specific exceptions for link failures
        let collapsed_version = if version == Version::FCS3_1 {
            Version::FCS3_0
        } else {
            version
        };
        let vsu = collapsed_version.short_underscore();
        let rstype_inner = format_ident!("AppliedGates{vsu}");
        let rstype = format_ident!("Py{rstype_inner}");
        let gm_pytype = (collapsed_version < Version::FCS3_2)
            .then(|| PyList::new1(PyClass::new_py([""; 0], "GatedMeasurement")).into());
        let ur_pytype = PyClass::new1(format!("UnivariateRegion{vsu}"));
        let bv_pytype = PyClass::new1(format!("BivariateRegion{vsu}"));
        let reg_rstype = format_ident!("PyRegion{vsu}");
        let map_rstype = parse_quote!(PyRegionMapping<#reg_rstype>);
        let reg_pytype = PyDict::new(
            RsInt::NonZeroUsize,
            PyUnion::new2(ur_pytype, bv_pytype, parse_quote!(#reg_rstype)),
            Some(map_rstype),
            None,
        )
        .into();
        let gtype = PyType::from(PyOpt::new(PyStr::default()));
        let pytype = PyTuple::new2(gm_pytype.into_iter().chain([reg_pytype, gtype]))
            .rstype(parse_quote!(#rstype));

        let desc = if collapsed_version == Version::FCS2_0 {
            "Value for *$Gm*/$RnI/$RnW/$GATING/$GATE* keywords. The first member of \
             the tuple corresponds to the *$Gm\\** keywords, where *m* is given by \
             position in the list. The second member corresponds to the *$RnI* and \
             *$RnW* keywords and is a mapping of regions and windows to be used in \
             gating scheme. Keys in dictionary are the region indices (the *n* in \
             *$RnI* and *$RnW*). The values in the dictionary are either univariate \
             or bivariate gates and must correspond to an index in the list in the \
             first element. The third member corresponds to the *$GATING* keyword. \
             All 'Rn' in this string must reference a key in the dict of the second \
             member."
        } else if collapsed_version < Version::FCS3_2 {
            "Value for *$Gm*/$RnI/$RnW/$GATING/$GATE* keywords. The first member of \
             the tuple corresponds to the *$Gm\\** keywords, where *m* is given by \
             position in the list. The second member corresponds to the *$RnI* and \
             *$RnW* keywords and is a mapping of regions and windows to be used in \
             gating scheme. Keys in dictionary are the region indices (the *n* in \
             *$RnI* and *$RnW*). The values in the dictionary are either univariate \
             or bivariate gates and must correspond to an index in the list in the \
             first element or a physical measurement. The third member corresponds \
             to the *$GATING* keyword. All 'Rn' in this string must reference a key \
             in the dict of the second member."
        } else {
            "Value for *$RnI/$RnW/$GATING* keywords. The first member corresponds to \
             the *$RnI* and *$RnW* keywords and is a mapping of regions and windows \
             to be used in gating scheme. Keys in dictionary are the region indices \
             (the *n* in *$RnI* and *$RnW*). The values in the dictionary are either \
             univariate or bivariate gates and must correspond to a physical \
             measurement. The second member corresponds to the *$GATING* keyword. \
             All 'Rn' in this string must reference a key in the dict of the first \
             member."
        };

        let param = DocArgParam::new_param("applied_gates", pytype, desc).def_auto();

        if collapsed_version == Version::FCS2_0 {
            param.into_rw(
                false,
                |_, _| quote!(self.0.metaroot::<#rstype_inner>().clone().into()),
                |n, _| quote!(self.0.set_metaroot::<#rstype_inner>(#n.into())),
            )
        } else {
            let setter = format_ident!("set_applied_gates_{vsu}");
            param.into_rw(
                true,
                |_, _| quote!(self.0.metaroot::<#rstype_inner>().clone().into()),
                |n, _| quote!(Ok(self.0.#setter(#n.into())?)),
            )
        }
    }

    pub fn new_scale_ivar() -> Self {
        Self::new_opt_ivar_rw(
            "scale",
            PyUnion::new_scale(false),
            "Value for *$PnE*. Empty tuple means linear scale; 2-tuple encodes \
             decades and offset for log scale",
            false,
            |_, _| quote!(self.0.specific.scale.as_ref().map(|&x| x)),
            |n, _| quote!(self.0.specific.scale = #n.into()),
        )
    }

    pub fn new_transform_ivar() -> Self {
        Self::new_ivar_rw(
            "transform",
            PyUnion::new_transform(),
            "Value for *$PnE* and/or *$PnG*. Singleton float encodes gain (*$PnG*) \
             and implies linear scaling (ie *$PnE* is ``0,0``). 2-tuple encodes \
             decades and offset for log scale, and implies *$PnG* is not set.",
            false,
            |_, _| quote!(self.0.specific.scale),
            |n, _| quote!(self.0.specific.scale = #n),
        )
    }

    pub fn new_core_nonstandard_keywords_ivar() -> Self {
        Self::new_nonstandard_keywords_ivar(
            "Pairs of non-standard keyword values. Keys must not start with *$*.",
            |_, _| quote!(self.0.nonstandard_keywords().clone()),
            |n, _| quote!(self.0.set_nonstandard_keywords(#n)),
        )
    }

    pub fn new_meas_nonstandard_keywords_ivar() -> Self {
        Self::new_nonstandard_keywords_ivar(
            "Any non-standard keywords corresponding to this measurement. No keys \
             should start with *$*. Realistically each key should follow a pattern \
             corresponding to the measurement index, something like prefixing with \
             \"P\" followed by the index. This is not enforced.",
            |_, _| quote!(self.0.common.nonstandard_keywords.clone()),
            |n, _| quote!(self.0.common.nonstandard_keywords = #n),
        )
    }

    fn new_nonstandard_keywords_ivar(
        desc: &str,
        f: impl FnOnce(&Ident, &ArgPyType) -> TokenStream2,
        g: impl FnOnce(&Ident, &ArgPyType) -> TokenStream2,
    ) -> Self {
        Self::new_ivar_rw(
            "nonstandard_keywords",
            PyDict::new_nonstd_keywords(),
            desc,
            false,
            f,
            g,
        )
        .def_auto()
    }
}

impl DocArgROIvar {
    pub fn new_ivar_ro(
        argname: impl fmt::Display + Clone,
        pytype: impl Into<ArgPyType>,
        desc: impl fmt::Display,
        f: impl FnOnce(&Ident, &ArgPyType) -> TokenStream2,
    ) -> Self {
        DocArgParam::new_param(argname, pytype, desc).into_ro(f)
    }

    pub fn new_version_ivar() -> Self {
        Self::new_ivar_ro(
            "version",
            PyLiteral::new_version(),
            "The FCS version.",
            |_, _| quote!(self.0.version),
        )
    }

    pub fn new_endian_param(n: usize) -> Self {
        let xs = (1..=n).join(",");
        let ys = (1..=n).rev().join(",");
        Self::new_ivar_ro(
            "endian",
            PyLiteral::new_endian(),
            format!(
                "If ``\"big\"`` use big endian (``{ys}``) for encoding values; \
             if ``\"little\"`` use little endian (``{xs}``)."
            ),
            |_, _| quote!(*self.0.as_ref()),
        )
        .def_auto()
    }

    pub fn new_endian_ord_param(n: usize) -> Self {
        let xs = (1..=n).join(",");
        let ys = (1..=n).rev().join(",");
        let sizedbyteord_path = quote!(fireflow_core::text::byteord::SizedByteOrd);
        Self::new_ivar_ro(
            "endian",
            PyLiteral::new_endian(),
            format!(
                "If ``\"big\"`` use big endian (``{ys}``) for encoding values; \
             if ``\"little\"`` use little endian (``{xs}``)."
            ),
            |_, _| {
                quote! {
                    let m: #sizedbyteord_path<2> = *self.0.as_ref();
                    m.endian()
                }
            },
        )
        .def_auto()
    }
}

impl DocArgParam {
    pub fn new_param(
        argname: impl fmt::Display,
        pytype: impl Into<ArgPyType>,
        desc: impl fmt::Display,
    ) -> Self {
        let pt = pytype.into();
        Self::new(argname.to_string(), pt, desc.to_string(), None, NoMethods)
    }

    pub fn new_bool_param(name: impl fmt::Display, desc: impl fmt::Display) -> Self {
        Self::new_param(name, PyBool::default(), desc).def_auto()
    }

    fn new_tri_flag_param(
        name: impl fmt::Display,
        false_is_error: bool,
        ident_name: &str,
        desc: impl fmt::Display,
    ) -> Self {
        let path = config_path(ident_name);
        let (false_action, true_action) = if false_is_error {
            ("throw error", "throw warning")
        } else {
            ("throw warning", "throw error")
        };
        let d = format!(
            "{desc} If ``False``, {false_action}. If ``True``, \
             {true_action}. If ``\"silent\"``, do nothing."
        );
        let pt = PyUnion::new2(PyBool::default(), PyLiteral::new1(["silent"]), path);
        Self::new_param(name, pt, d).def_auto()
    }

    fn new_proc_kw_fail(
        name: impl fmt::Display,
        ident_name: &str,
        desc: impl fmt::Display,
    ) -> Self {
        let path = config_path(ident_name);
        let pt = PyLiteral::new2(["error", "demote", "drop", "drop_silent"], path);
        let d = format!(
            "{desc} Use ``\"error\"`` to throw error on failure, \
             ``\"demote\"`` to demote to non-standard, ``\"drop\"`` to drop \
             with warning, or ``\"drop_silent\"`` to drop with no warning"
        );
        Self::new_param(name, pt, d).def_auto()
    }

    fn new_opt_param(
        name: impl fmt::Display,
        pytype: impl Into<ArgPyType>,
        desc: impl fmt::Display,
    ) -> Self {
        Self::new_param(name, PyOpt::new(pytype), desc).def_auto()
    }

    pub fn into_ro(self, f: impl FnOnce(&Ident, &ArgPyType) -> TokenStream2) -> DocArgROIvar {
        let methods = GetMethod::from_pytype(self.argname.as_str(), &self.pytype, f);
        DocArgROIvar::new(self.argname, self.pytype, self.desc, self.default, methods)
    }

    fn into_rw(
        self,
        fallible: bool,
        f: impl FnOnce(&Ident, &ArgPyType) -> TokenStream2,
        g: impl FnOnce(&Ident, &ArgPyType) -> TokenStream2,
    ) -> DocArgRWIvar {
        let methods =
            GetSetMethods::from_pytype(self.argname.as_str(), &self.pytype, fallible, f, g);
        DocArgRWIvar::new(self.argname, self.pytype, self.desc, self.default, methods)
    }

    pub fn new_dataset_offset_param() -> Self {
        let desc = "Starting position in the file of the dataset to be read.";
        Self::new_param("dataset_offset", PyInt::new_dataset_offset(), desc).def_auto()
    }

    pub fn new_skip_param(desc: &str) -> Self {
        let pt = PyOpt::new(PyInt::new_int(RsInt::Usize));
        Self::new_param("skip", pt, desc).def_auto()
    }

    pub fn new_limit_param(desc: &str) -> Self {
        let pt = PyOpt::new(PyInt::new_int(RsInt::Usize));
        Self::new_param("limit", pt, desc).def_auto()
    }

    pub fn new_path_param(read: bool) -> Self {
        let s = if read { "read" } else { "written" };
        let pt = PyClass::new1("~pathlib.Path").rstype(parse_quote!(std::path::PathBuf));
        Self::new_param("path", pt, format!("Path to be {s}."))
    }

    pub fn new_version_param() -> Self {
        let desc = "Version to use when parsing *TEXT*.";
        Self::new_param("version", PyLiteral::new_version(), desc)
    }

    pub fn new_std_keywords_param() -> Self {
        Self::new_param("std", PyDict::new_std_keywords(), "Standard keywords.")
    }

    pub fn new_nonstd_keywords_param() -> Self {
        let desc = "Non-standard keywords.";
        Self::new_param("nonstd", PyDict::new_nonstd_keywords(), desc)
    }

    pub fn new_valid_keywords_param() -> Self {
        let desc = "Standard and non-standard keywords.";
        Self::new_param("kws", PyClass::new_py(["api"], "ValidKeywords"), desc)
    }

    pub fn new_extra_std_keywords_param() -> Self {
        let desc = "Extra keywords from *TEXT* standardization";
        Self::new_param("extra", PyClass::new_py(["api"], "ExtraStdKeywords"), desc)
    }

    pub fn new_dataset_segments_param() -> Self {
        let desc = "Offsets used to parse *DATA* and *ANALYSIS*.";
        Self::new_param(
            "dataset_segs",
            PyClass::new_py(["api"], "DatasetSegments"),
            desc,
        )
    }

    pub fn new_parse_output_param() -> Self {
        let desc = "Miscellaneous data obtained when parsing *TEXT*.";
        Self::new_param("parse", PyClass::new_py(["api"], "FlatTEXTParseData"), desc)
    }

    pub fn new_text_seg_param() -> Self {
        let desc = "The primary *TEXT* segment from *HEADER*.";
        Self::new_param("text_seg", PyTuple::new_text_segment(), desc)
    }

    pub fn new_rel_data_seg_param() -> Self {
        let desc = "The *DATA* segment from *HEADER*.";
        let seg = PyTuple::new_relative_segment("DataSegmentId");
        Self::new_param("data_seg", seg, desc)
    }

    pub fn new_rel_analysis_seg_param() -> Self {
        let desc = "The *ANALYSIS* segment from *HEADER*.";
        let seg = PyTuple::new_relative_segment("AnalysisSegmentId");
        Self::new_param("analysis_seg", seg, desc).def_auto()
    }

    pub fn new_rel_other_segs_param() -> Self {
        let seg = PyTuple::new_relative_segment("OtherSegmentId");
        Self::new_param(
            "other_segs",
            PyList::new1(seg),
            "The *OTHER* segments from *HEADER*.",
        )
        .def_auto()
    }

    pub fn new_data_seg_param(src: SegmentSrc) -> Self {
        let desc = format!("The *DATA* segment from {src}.");
        Self::new_param("data_seg", PyTuple::new_data_segment(src), desc)
    }

    pub fn new_analysis_seg_param(src: SegmentSrc, default: bool) -> Self {
        let desc = format!("The *ANALYSIS* segment from {src}.");
        Self::new_param("analysis_seg", PyTuple::new_analysis_segment(src), desc)
            .def_auto_if(default)
    }

    pub fn new_other_segs_param(default: bool) -> Self {
        Self::new_param(
            "other_segs",
            PyList::new1(PyTuple::new_other_segment()),
            "The *OTHER* segments from *HEADER*.",
        )
        .def_auto_if(default)
    }

    pub fn new_textdelim_param() -> Self {
        let path = parse_quote!(fireflow_core::validated::textdelim::TEXTDelim);
        let exc = PyException::new_config().desc("if %x is not between 1 and 126");
        let pytype = PyInt::from(RsInt::U8).rstype(path).exc(exc);
        let desc = "Delimiter to use when writing *TEXT*.";
        Self::new_param("delim", pytype, desc).def(DocDefault::Int(30))
    }

    pub fn new_big_other_param() -> Self {
        let desc = "If ``True`` use 20 chars for OTHER segment offsets, and 8 otherwise.";
        Self::new_bool_param("big_other", desc)
    }

    pub fn new_skip_conversion_check_param() -> Self {
        let conv_exc = PyreflowError::DataLoss.fmt_ref();
        Self::new_bool_param(
            "skip_conversion_check",
            format!(
                "Skip check to ensure that types of the dataframe match the \
                 columns (*$PnB*, *$DATATYPE*, etc). If this is ``False``, \
                 perform this check before writing, and raise {conv_exc} on \
                 failure. If ``True``, raise warnings as file is being \
                 written. Skipping this is faster since the data needs to be \
                 traversed twice to perform the conversion check, but may \
                 result in loss of precision and/or truncation."
            ),
        )
    }

    pub fn new_appendable_param() -> Self {
        Self::new_bool_param(
            "appendable",
            "If ``True``, set *$NEXTDATA* in written dataset so it points to \
             the next dataset. This obviously assumes the next dataset is actually \
             written, which will require another call to this method with ``append`` \
             set to ``True``.",
        )
    }

    pub fn new_append_param() -> Self {
        Self::new_bool_param(
            "append",
            "If ``True``, append this dataset to the end of the file if it exists \
             and already has at least one dataset in it. This assumes that the \
             previous dataset was written with ``appendable`` set to ``True`` so \
             that *$NEXTDATA* is properly set.",
        )
    }

    pub fn new_paired_measurements_param(version: Version) -> Self {
        let meas_desc = "Measurements corresponding to columns in FCS file. \
                         Temporal must be given zero or one times.";
        Self::new_param("measurements", PyTuple::new_meas(version), meas_desc)
    }

    pub fn new_measurements_param(version: Version) -> Self {
        let meas_desc = "Measurements corresponding to columns in FCS file. \
                         Temporal must be given zero or one times.";
        let pt = PyList::new1(PyUnion::new_measurement(version));
        Self::new_param("measurements", pt, meas_desc)
    }

    pub fn new_set_meas_param(version: Version) -> Self {
        let d = "The new measurements. The first member of the tuple corresponds to \
                 the measurement name and the second is the measurement object.";
        Self::new_param("measurements", PyTuple::new_meas(version), d)
    }

    pub fn new_allow_shared_names_param() -> Self {
        let exc = PyreflowError::Relational.fmt_ref();
        let d = format!(
            "If ``False``, raise {exc} if any non-measurement keywords reference \
             any *$PnN* keywords. If ``True`` raise {exc} if any non-measurement \
             keywords reference a *$PnN* which is not present in ``measurements``. \
             In other words, ``False`` forbids named references to exist, and \
             ``True`` allows named references to be updated. References cannot \
             be broken in either case."
        );
        Self::new_bool_param("allow_shared_names", d)
    }

    // TODO this can be specific to each version, for instance, we can call out
    // the exact keywords in each that may have references.
    pub fn new_skip_index_check_param() -> Self {
        let exc = PyreflowError::Relational.fmt_ref();
        let desc = format!(
            "If ``False``, raise {exc} if any non-measurement keyword \
             have an index reference to the current measurements. If \
             ``True`` allow such references to exist as long as they do \
             not break (which really means that the length of \
             ``measurements`` is such that existing indices are satisfied)."
        );
        Self::new_bool_param("skip_index_check", desc)
    }

    pub fn new_index_param(desc: &str) -> Self {
        Self::new_param("index", PyInt::new_meas_index(), desc)
    }

    pub fn new_col_param() -> Self {
        let d = "Data for measurement. Must be same length as existing columns.";
        Self::new_param("col", PyClass::new_series(), d)
    }

    pub fn new_name_param(short_desc: &str) -> Self {
        let desc = format!("{short_desc} Corresponds to *$PnN*.");
        Self::new_param("name", PyStr::new_shortname(), desc)
    }

    pub fn new_range_param() -> Self {
        let desc = "Range of measurement. Corresponds to *$PnR*.";
        Self::new_param("range", PyDecimal::new_range(), desc)
    }

    pub fn new_notrunc_param() -> Self {
        let exc = PyreflowError::Relational.fmt_ref();
        let desc = format!(
            "If ``False``, raise {exc} if ``range`` must be \
             truncated to fit into measurement type."
        );
        Self::new_bool_param("disallow_trunc", desc)
    }

    pub fn new_data_param(polars_type: bool) -> Self {
        let desc = "A dataframe encoding the contents of *DATA*. Number of \
                    columns must match number of measurements. May be empty. \
                    Types do not necessarily need to correspond to those in the \
                    data layout but mismatches may result in truncation.";
        let exc = PyException::new_pyreflow(&PyreflowError::EventData).desc(
            "If %x contains columns which are not \
             unsigned 8/16/32/64-bit integers or 32/64-bit floats",
        );
        let pt = PyClass::new_dataframe(polars_type).exc(exc);
        Self::new_param("data", pt, desc)
    }

    pub fn new_analysis_param(default: bool) -> Self {
        let desc = "Contents of the *ANALYSIS* segment.";
        Self::new_param("analysis", PyBytes::new_analysis(), desc).def_auto_if(default)
    }

    pub fn new_others_param(default: bool) -> Self {
        let desc = "A list of byte strings encoding the *OTHER* segments.";
        Self::new_param("others", PyList::new_others(), desc).def_auto_if(default)
    }

    pub fn new_read_header_config_params() -> (Path, Vec<Self>, Vec<TokenStream2>) {
        let conf = config_path("ReadHeaderInnerConfig");
        let ps = vec![
            Self::new_text_correction_param(),
            Self::new_data_correction_param(),
            Self::new_analysis_correction_param(),
            Self::new_other_corrections_param(),
            Self::new_max_other_param(),
            Self::new_other_width_param(),
            Self::new_squish_offsets_param(),
            Self::new_allow_negative_param(),
            Self::new_truncate_offsets_param(),
        ];
        let js = ps.iter().map(IsDocArg::record_into).collect();
        (conf, ps, js)
    }

    pub fn new_read_flat_config_params() -> (Path, Vec<Self>, Vec<TokenStream2>) {
        let conf = config_path("ReadHeaderAndTEXTConfig");
        let ps = vec![
            Self::new_version_override(),
            Self::new_supp_text_correction(),
            Self::new_nextdata_correction(),
            Self::new_allow_overlapping_supp_text(),
            Self::new_ignore_supp_text(),
            Self::new_delim_escape_mode(),
            Self::new_allow_non_ascii_delim(),
            Self::new_allow_missing_final_delim(),
            Self::new_allow_nonunique(),
            Self::new_allow_odd(),
            Self::new_allow_empty_keys(),
            Self::new_allow_empty_values(),
            Self::new_allow_delim_at_boundary(),
            Self::new_allow_non_utf8(),
            Self::new_use_latin1(),
            Self::new_allow_non_ascii_keywords(),
            Self::new_allow_missing_supp_text(),
            Self::new_allow_supp_text_own_delim(),
            Self::new_allow_missing_nextdata(),
            Self::new_trim_value_whitespace(),
            Self::new_trim_trailing_whitespace(),
            Self::new_ignore_standard_keys(),
            Self::new_promote_to_standard(),
            Self::new_demote_from_standard(),
            Self::new_rename_standard_keys(),
            Self::new_replace_standard_key_values(),
            Self::new_append_standard_keywords(),
            Self::new_substitute_standard_key_values(),
        ];
        let js = ps.iter().map(IsDocArg::record_into).collect();
        (conf, ps, js)
    }

    pub fn new_read_std_config_params(
        version: Option<Version>,
    ) -> (Path, Vec<Self>, Vec<TokenStream2>) {
        let parse_indexed_spillover = Self::new_parse_indexed_spillover_param();
        let disallow_localtime = Self::new_disallow_localtime_param();

        let std_common_args = [
            Self::new_dedup_meas_names_param(),
            Self::new_trim_intra_value_whitespace_param(),
            Self::new_time_meas_pattern_param(),
            Self::new_allow_missing_time_param(),
            Self::new_force_time_linear_param(),
            Self::new_ignore_time_optical_keys_param(),
            Self::new_date_pattern_param(),
            Self::new_time_pattern_param(version),
            Self::new_datetime_pattern_param(),
            Self::new_last_modified_pattern_param(),
            Self::new_allow_other_feature_param(),
            Self::new_process_pseudostandard_param(),
            Self::new_process_hyper_par_param(),
            Self::new_process_other_version_param(),
            Self::new_process_extra_timestep_param(),
            Self::new_disallow_deprecated_param(),
            Self::new_fix_log_scale_offsets_param(),
            Self::new_nonstandard_measurement_pattern_param(),
        ]
        .into_iter();

        let ps: Vec<_> = match version {
            Some(Version::FCS2_0 | Version::FCS3_0) => std_common_args.collect(),
            Some(Version::FCS3_1) => std_common_args.chain([parse_indexed_spillover]).collect(),
            _ => std_common_args
                .chain([parse_indexed_spillover, disallow_localtime])
                .collect(),
        };

        let conf = config_path("ReadStdKeywordsConfig");
        let js = ps.iter().map(IsDocArg::record_into).collect();
        (conf, ps, js)
    }

    pub fn new_read_layout_config_params(
        version: Option<Version>,
    ) -> (Path, Vec<Self>, Vec<TokenStream2>) {
        let offset_ps: Vec<_> = match version {
            // none of these apply to 2.0 since there are no offsets in TEXT
            Some(Version::FCS2_0) => vec![],
            _ => vec![
                Self::new_text_data_correction_param(),
                Self::new_text_analysis_correction_param(),
                Self::new_ignore_text_data_offsets_param(),
                Self::new_ignore_text_analysis_offsets_param(),
                Self::new_allow_header_text_offset_mismatch_param(),
                Self::new_allow_missing_required_offsets_param(version),
                Self::new_truncate_text_offsets_param(),
            ],
        };

        let process_optional_failure = Self::new_process_optional_failure();
        let integer_widths_from_byteord = Self::new_integer_widths_from_byteord_param();
        let integer_byteord_override = Self::new_integer_byteord_override_param();
        let disallow_range_truncation = Self::new_disallow_range_truncation_param();

        let layout_ps: Vec<_> = match version {
            Some(Version::FCS3_1 | Version::FCS3_2) => {
                [process_optional_failure, disallow_range_truncation]
                    .into_iter()
                    .collect()
            }
            _ => [
                process_optional_failure,
                integer_widths_from_byteord,
                integer_byteord_override,
                disallow_range_truncation,
            ]
            .into_iter()
            .collect(),
        };

        let conf = config_path("ReadDataKeywordsConfig");
        let ps: Vec<_> = offset_ps.into_iter().chain(layout_ps).collect();
        let js = ps.iter().map(IsDocArg::record_into).collect();
        (conf, ps, js)
    }

    pub fn new_read_events_config_params() -> (Path, Vec<Self>, Vec<TokenStream2>) {
        let allow_uneven_event_width = Self::new_allow_uneven_event_width_param();
        let allow_tot_mismatch = Self::new_allow_tot_mismatch_param();
        let truncate_event_values = Self::new_truncate_event_values();
        let disallow_over_range = Self::new_disallow_over_range();
        let conf = config_path("ReadEventsConfig");
        let ps = vec![
            allow_uneven_event_width,
            allow_tot_mismatch,
            truncate_event_values,
            disallow_over_range,
        ];
        let js = ps.iter().map(IsDocArg::record_into).collect();
        (conf, ps, js)
    }

    pub fn new_write_text_config_params() -> (Path, Vec<Self>, Vec<TokenStream2>) {
        let delim = Self::new_textdelim_param();
        let big_other = Self::new_big_other_param();
        let conf = config_path("WriteTEXTInnerConfig");
        let ps = vec![delim, big_other];
        let js = ps.iter().map(IsDocArg::record_into).collect();
        (conf, ps, js)
    }

    pub fn new_shared_config_params() -> (Path, Vec<Self>, Vec<TokenStream2>) {
        let conf = config_path("ReadSharedConfig");
        let warnings_are_errors = Self::new_warnings_are_errors_param();
        let hide_warnings = Self::new_hide_warnings_param();
        let ps = vec![warnings_are_errors, hide_warnings];
        let js = ps.iter().map(IsDocArg::record_into).collect();
        (conf, ps, js)
    }

    fn new_dedup_meas_names_param() -> Self {
        let d = "If ``True``, force all *$PnN* to be unique by appending \
                 ``\"~X\"`` to each duplicate and incrementing ``X`` starting \
                 at 0.";
        Self::new_bool_param("dedup_measurement_names", d)
    }

    fn new_trim_intra_value_whitespace_param() -> Self {
        let d = "If ``True``, trim whitespace between delimiters such as ``,`` \
                 and ``;`` within keyword value strings.";
        Self::new_bool_param("trim_intra_value_whitespace", d)
    }

    fn new_time_meas_pattern_param() -> Self {
        let path = parse_quote!(fireflow_core::config::TimeMeasNamePattern);
        let pytype = PyOpt::new(PyStr::new_regexp().rstype(path));
        let d = "A pattern to match the *$PnN* of the time measurement. \
                 If ``None``, do not try to find a time measurement.";
        Self::new_param("time_meas_pattern", pytype, d).def(DocDefault::Str("^(TIME|Time)$".into()))
    }

    fn new_allow_missing_time_param() -> Self {
        let d = "If ``True`` allow time measurement to be missing.";
        Self::new_bool_param("allow_missing_time", d)
    }

    fn new_force_time_linear_param() -> Self {
        let d = "If ``True`` force time measurement to be linear independent of *$PnE*.";
        Self::new_bool_param("force_time_linear", d)
    }

    fn new_ignore_time_optical_keys_param() -> Self {
        let p = PyList::new(
            PyLiteral::new_temporal_optical_key(),
            Some(parse_quote!(TemporalOpticalKeys)),
            None,
        );
        let d = "Ignore optical keys in temporal measurement. These keys are \
                 *$PnG* which is explicitly forbidden by the standard but \
                 allowed in this library to be set to ``\"1.0\"`` (noop), or \
                 others which are nonsensical for time measurements but are not \
                 explicitly forbidden in the the standard (such as *$PnL*). \
                 Provided keys are the string after the \"Pn\" in the \"PnX\" \
                 keywords.";
        Self::new_param("ignore_time_optical_keys", p, d).def_auto()
    }

    fn new_parse_indexed_spillover_param() -> Self {
        let d = "Parse $SPILLOVER with numeric indices rather than strings \
                 (ie names or *$PnN*)";
        Self::new_bool_param("parse_indexed_spillover", d)
    }

    fn new_date_pattern_param() -> Self {
        let path = parse_quote!(fireflow_core::validated::datepattern::DatePattern);
        let desc = format!(
            "if %x does not have year, month, and day specifiers \
             as outlined in {CHRONO_REF}"
        );
        let exc = PyException::new_config().desc(desc);
        let pytype = PyStr::default().rstype(path).exc(exc);
        let d = "If supplied, will be used as an alternative pattern when parsing \
                 *$DATE*. If not supplied, *$DATE* will be parsed according to \
                 the standard pattern which is ``%d-%b-%Y``.";
        Self::new_opt_param("date_pattern", pytype, d)
    }

    fn new_datetime_pattern_param() -> Self {
        let pytype = PyStr::default();
        let d = "If supplied, will be used as an alternative pattern when parsing \
                 *$BEGINDATETIME* and *ENDDATETIME*. The pattern must follow the \
                 format outlined in {CHRONO_REF}. If not supplied, these will \
                 be parsed as ISO timestamps with optional timezone.";
        Self::new_opt_param("datetime_pattern", pytype, d)
    }

    fn new_last_modified_pattern_param() -> Self {
        let pytype = PyStr::default();
        let d = "If supplied, will be used as an alternative pattern when parsing \
                 *$LAST_MODIFIED*. The pattern must follow the format outlined in \
                 {CHRONO_REF}. If not supplied, these will be parsed according to \
                 the default pattern which is  ``\"%d-%b-%Y %H:%M:%S\"`` possibly \
                 with centiseconds after.";
        Self::new_opt_param("last_modified_pattern", pytype, d)
    }

    fn new_allow_other_feature_param() -> Self {
        let d = "If ``true``, allow *$PnFEATURE* to be a value other than \
                 `\"Area\"`, `\"Width\"`, or `\"Height\"`.";
        Self::new_bool_param("allow_other_feature", d)
    }

    fn new_time_pattern_param(version: Option<Version>) -> Self {
        const CORE_PAT: &str = "%H:%M:%S";
        const SUB3_0: &str = "%!";
        const SUB3_1: &str = "%@";
        const NAME3_0: &str = "1/60 seconds";
        const NAME3_1: &str = "centiseconds";

        let fmt = |s: &str| format!("``\"{s}\"``");
        let sub3_0 = fmt(SUB3_0);
        let sub3_1 = fmt(SUB3_1);

        // format exception description
        let exc_desc = format!(
            "if %x does not have specifiers for hours, minutes, \
             seconds, and optionally sub-seconds (where {sub3_0} and {sub3_1} \
             correspond to {NAME3_0} and {NAME3_1} respectively) as outlined \
             in {CHRONO_REF}"
        );
        let exc = PyException::new_config().desc(exc_desc);

        // format arg description
        let std_pat = match version {
            None => "version-specific".into(),
            Some(Version::FCS2_0) => fmt(CORE_PAT),
            Some(Version::FCS3_0) => fmt(&format!("{CORE_PAT}:{SUB3_0}")),
            _ => fmt(&format!("{CORE_PAT}.{SUB3_1}")),
        };
        let line1 = "If supplied, will be used as an alternative pattern when \
                     parsing *$BTIM* and *$ETIM*.";
        let line2 = format!(
            "The values {sub3_0} or {sub3_1} may be used to \
             match {NAME3_0} or {NAME3_1} respectively."
        );
        let line3 = format!(
            "If not supplied, *$BTIM* and *$ETIM* will be parsed \
             according to the standard pattern which is {std_pat}."
        );
        let arg_desc = [line1.to_owned(), line2, line3].into_iter().join(" ");

        let path = parse_quote!(fireflow_core::validated::timepattern::TimePattern);
        let pytype = PyStr::default().rstype(path).exc(exc);
        Self::new_opt_param("time_pattern", pytype, arg_desc)
    }

    fn new_process_pseudostandard_param() -> Self {
        let d = "Process non-standard keywords with a leading *$*. The \
                 presence of such keywords often means the version in *HEADER* \
                 is incorrect.";
        Self::new_proc_kw_fail("process_pseudostandard", "ProcessPseudostandard", d)
    }

    fn new_process_hyper_par_param() -> Self {
        let d = "Process measurement keywords whose index is greater than *$PAR*.";
        Self::new_proc_kw_fail("process_hyper_par", "ProcessHyperPar", d)
    }

    fn new_process_other_version_param() -> Self {
        let d = "Process standard keywords from different FCS versions.";
        Self::new_proc_kw_fail("process_other_version", "ProcessOtherVersion", d)
    }

    fn new_process_extra_timestep_param() -> Self {
        let d = "Process *$TIMESTEP* to be present which may indicate \
                 a time measurement is present but not identified.";
        Self::new_proc_kw_fail("process_extra_timestep", "ProcessExtraTimestep", d)
    }

    fn new_process_optional_failure() -> Self {
        let d = "Process optional keys which cause an error.";
        Self::new_proc_kw_fail("process_optional_failure", "ProcessOptionalFailure", d)
    }

    fn new_disallow_deprecated_param() -> Self {
        let d = "If ``True`` throw error if a deprecated key is encountered.";
        Self::new_bool_param("disallow_deprecated", d)
    }

    fn new_fix_log_scale_offsets_param() -> Self {
        let d = "If ``True`` fix log-scale *PnE* and keywords which have zero offset \
                 (ie ``X,0.0`` where ``X`` is non-zero).";
        Self::new_bool_param("fix_log_scale_offsets", d)
    }

    fn new_disallow_localtime_param() -> Self {
        let d = "If ``true``, require that *$BEGINDATETIME* and *$ENDDATETIME* \
                 have a timezone if provided. This is not required by the \
                 standard, but not having a timezone is ambiguous since the \
                 absolute value of the timestamp is dependent on localtime and \
                 therefore is location-dependent. Only affects FCS 3.2.";
        Self::new_bool_param("disallow_localtime", d)
    }

    fn new_nonstandard_measurement_pattern_param() -> Self {
        let path = parse_quote!(fireflow_core::validated::keys::NonStdMeasPattern);
        let exc = PyException::new_config().desc("if %x does not have ``\"%n\"``");
        let pytype = PyStr::default().rstype(path).exc(exc);
        let d = format!(
            "Pattern to use when matching nonstandard measurement keys. Must \
             be a regular expression pattern with ``%n`` which will \
             represent the measurement index and should not start with *$*. \
             Otherwise should be a normal regular expression as defined in \
             {REGEXP_REF}."
        );
        Self::new_param("nonstandard_measurement_pattern", PyOpt::new(pytype), d)
            .def(DocDefault::Str("^P%n".into()))
    }

    fn new_integer_widths_from_byteord_param() -> Self {
        let d = "If ``True`` set all *$PnB* to the number of bytes from *$BYTEORD*. \
                 Only has an effect for FCS 2.0/3.0 where *$DATATYPE* is ``I``.";
        Self::new_bool_param("integer_widths_from_byteord", d)
    }

    fn new_integer_byteord_override_param() -> Self {
        let path = keyword_path("ByteOrd2_0");
        let exc = PyException::new_invalid_keyword().desc(
            "if %x is not a list of integers including all from 1 to ``N`` \
             where ``N`` is the length of the list (up to 8)",
        );
        Self::new_opt_param(
            "integer_byteord_override",
            PyList::new(RsInt::U32, Some(path), Some(exc.into())),
            "Override *$BYTEORD* for integer layouts.",
        )
    }

    fn new_disallow_range_truncation_param() -> Self {
        let d = "If ``True`` throw error if *$PnR* values need to be truncated \
                 to match the number of bytes specified by *$PnB* and *$DATATYPE*.";
        Self::new_bool_param("disallow_range_truncation", d)
    }

    fn new_config_correction_arg(name: &str, what: &str, is_header: bool, id: &str) -> Self {
        let location = if is_header { "HEADER" } else { "TEXT" };
        let d = format!("Corrections for {what} offsets in *{location}*.");
        Self::new_param(name, PyTuple::new_correction(is_header, id), d).def_auto()
    }

    fn new_text_correction_param() -> Self {
        Self::new_config_correction_arg("text_correction", "*TEXT*", true, "PrimaryTextSegmentId")
    }

    fn new_data_correction_param() -> Self {
        Self::new_config_correction_arg("data_correction", "*DATA*", true, "DataSegmentId")
    }

    fn new_analysis_correction_param() -> Self {
        Self::new_config_correction_arg(
            "analysis_correction",
            "*ANALYSIS*",
            true,
            "AnalysisSegmentId",
        )
    }

    fn new_other_corrections_param() -> Self {
        Self::new_param(
            "other_corrections",
            PyList::new1(PyTuple::new_correction(true, "OtherSegmentId")),
            "Corrections for OTHER offsets if they exist. Each correction will \
             be applied in order. If an offset does not need to be corrected, \
             use ``(0, 0)``. This will not affect the number of OTHER segments \
             that are read; this is controlled by ``max_other``.",
        )
        .def_auto()
    }

    fn new_max_other_param() -> Self {
        let desc = "Maximum number of OTHER segments that can be parsed. \
                    ``None`` means limitless.";
        Self::new_opt_param("max_other", RsInt::Usize, desc)
    }

    fn new_other_width_param() -> Self {
        let path = parse_quote!(fireflow_core::validated::ascii_range::OtherWidth);
        let e = PyException::new_config().desc("if %x is less than `1` and greater than `20`");
        let pt = PyInt::new_int(RsInt::NonZeroU8).rstype(path).exc(e);
        let desc = "Width (in bytes) to use when parsing *OTHER* offsets.";
        Self::new_param("other_width", pt, desc).def(DocDefault::Int(8))
    }

    // this only matters for 3.0+ files
    fn new_squish_offsets_param() -> Self {
        let d = "If ``True`` and a segment's ending offset is zero, treat entire \
                 offset as empty. This might happen if the ending offset is longer \
                 than 8 digits, in which case it must be written in *TEXT*. If this \
                 happens, the standards mandate that both offsets be written to \
                 *TEXT* and that the *HEADER* offsets be set to ``0,0``, so only \
                 writing one is an error unless this flag is set. This should only \
                 happen in FCS 3.0 files and above.";
        Self::new_bool_param("squish_offsets", d)
    }

    fn new_allow_negative_param() -> Self {
        let d = "If true, allow negative values in a HEADER offset. If negative \
                 offsets are found, they will be replaced with ``0``. Some files \
                 will denote an \"empty\" offset as ``0,-1``, which is logically \
                 correct since the last offset points to the last byte, thus ``0,0`` \
                 is actually 1 byte long. Unfortunately this is not what the \
                 standards say, so specifying ``0,-1`` is an error unless this \
                 flag is set.";
        Self::new_bool_param("allow_negative", d)
    }

    fn new_truncate_offsets_param() -> Self {
        let d = "If true, truncate offsets that exceed the end of the file. \
                 In some cases the DATA offset (usually) might exceed the end of the \
                 file by 1, which is usually a mistake and should be corrected with \
                 ``data_correction`` (or analogous for the offending offset). If this \
                 is not the case, the file is likely corrupted. This flag will allow \
                 such files to be read conveniently if desired.";
        Self::new_bool_param("truncate_offsets", d)
    }

    fn new_version_override() -> Self {
        let d = "Override the FCS version as seen in *HEADER*. Use an FCS \
                 version string like ``\"FCS3.2\"`` to force to a specific \
                 version. Alternatively, autodetect the version from keywords in \
                 *TEXT* using one of ``\"latest\"``, ``\"earliest\"``, \
                 ``\"strict\"``, or ``\"loose\"``. These will be used to select \
                 the latest version, earliest version, version with least \
                 optional keywords, or version with most optional keywords \
                 respectively in the event that more than one version can \
                 accommodate the keywords from *TEXT*. Autodetection will fail \
                 if no versions can be found which accommodate all required \
                 keywords in *TEXT*.";
        Self::new_opt_param("version_override", PyLiteral::new_version_override(), d)
    }

    fn new_supp_text_correction() -> Self {
        Self::new_config_correction_arg(
            "supp_text_correction",
            "Supplemental *TEXT*",
            false,
            "SupplementalTextSegmentId",
        )
    }

    fn new_nextdata_correction() -> Self {
        let d = "Correction for *$NEXTDATA*.";
        Self::new_param("nextdata_correction", PyInt::new_int(RsInt::I32), d).def_auto()
    }

    fn new_allow_overlapping_supp_text() -> Self {
        let exc = PyreflowError::FileLayout.fmt_ref();
        let d = format!(
            "If ``True`` allow supplemental *TEXT* offsets to overlap the \
             primary *TEXT* offsets from *HEADER* or *HEADER* itself and raise \
             a warning if such an overlap is found. Otherwise raise a {exc}. \
             The offsets will not be used if an overlap is found in either case."
        );
        Self::new_tri_flag_param(
            "allow_overlapping_supp_text",
            true,
            "AllowOverlappingSuppTEXT",
            d,
        )
    }

    fn new_ignore_supp_text() -> Self {
        Self::new_bool_param(
            "ignore_supp_text",
            "If ``True``, ignore supplemental *TEXT* entirely.",
        )
    }

    fn new_delim_escape_mode() -> Self {
        let path = config_path("DelimEscapeMode");
        let d = "Determine how to escape delims in *TEXT*. If ``\"escaped\"`` \
             or ``\"unescaped\"``, escape or do not escape delimiters \
             respectively. If ``\"guess_escaped\"`` or  ``\"guess_unescaped\"``, \
             attempt to guess how delimiters should be treated, falling back \
             to escaped or unescaped mode respectively if the choice is ambiguous.";
        let choices = ["escaped", "unescaped", "guess_escaped", "guess_unescaped"];
        let pt = PyLiteral::new2(choices, path);
        Self::new_param("delim_escape_mode", pt, d).def_auto()
    }

    fn new_allow_non_ascii_delim() -> Self {
        Self::new_bool_param(
            "allow_non_ascii_delim",
            "If ``True`` allow non-ASCII delimiters (outside 1-126).",
        )
    }

    fn new_allow_missing_final_delim() -> Self {
        let d = "If ``True`` allow *TEXT* to not end with a delimiter.";
        Self::new_bool_param("allow_missing_final_delim", d)
    }

    fn new_allow_nonunique() -> Self {
        let d = "If ``True`` allow non-unique keys in *TEXT*. In such cases, \
                 only the first key will be used regardless of this setting; ";
        Self::new_bool_param("allow_nonunique", d)
    }

    fn new_allow_odd() -> Self {
        let d = "If ``True``, allow *TEXT* to contain odd number of tokens. \
                 The last 'dangling' token will be dropped independent of this flag.";
        Self::new_bool_param("allow_odd", d)
    }

    fn new_allow_empty_keys() -> Self {
        let d = "If ``True`` allow keys to be blank. Only relevant if \
                 if delimiters are unescaped.";
        Self::new_bool_param("allow_empty_keys", d)
    }

    fn new_allow_empty_values() -> Self {
        let d = "If ``True`` allow values to be blank. Only relevant if \
                 ``trim_value_whitespace`` is ``True`` and value is \
                 entirely whitespace.";
        Self::new_bool_param("allow_empty_values", d)
    }

    fn new_allow_delim_at_boundary() -> Self {
        let d = "If ``True`` allow delimiters at token boundaries. The FCS standard \
                 forbids this because it is impossible to tell if such delimiters \
                 belong to the previous or the next token. Consequently, delimiters \
                 at boundaries will be dropped regardless of this flag. Setting \
                 this to ``True`` will turn this into a warning not an error. Only \
                 relevant if delimiters are escaped.";
        Self::new_bool_param("allow_delim_at_boundary", d)
    }

    fn new_allow_non_utf8() -> Self {
        let d = "If ``True`` allow non-UTF8 characters in *TEXT*. Tokens with such \
             characters will be dropped regardless; setting this to ``True`` \
             will turn these cases into warnings not errors.";
        Self::new_bool_param("allow_non_utf8", d)
    }

    fn new_use_latin1() -> Self {
        let d = "If ``True`` interpret all characters in *TEXT* as Latin-1 (aka \
             ISO/IEC 8859-1) instead of UTF-8.";
        Self::new_bool_param("use_latin1", d)
    }

    fn new_allow_non_ascii_keywords() -> Self {
        let d = "If ``True`` allow non-ASCII keys. This only applies to \
                 non-standard keywords, as all standardized keywords may only \
                 contain letters, numbers, and start with *$*. Regardless, all \
                 compliant keys must only have ASCII. Setting this to true will \
                 emit an error when encountering such a key. If false, the key will \
                 be kept as a non-standard key.";
        Self::new_bool_param("allow_non_ascii_keywords", d)
    }

    fn new_allow_missing_supp_text() -> Self {
        let d = "If ``True`` allow supplemental *TEXT* offsets to be missing from \
                 primary *TEXT*.";
        Self::new_bool_param("allow_missing_supp_text", d)
    }

    fn new_allow_supp_text_own_delim() -> Self {
        let d = "If ``True`` allow supplemental *TEXT* offsets to have a different \
                 delimiter compared to primary *TEXT*.";
        Self::new_bool_param("allow_supp_text_own_delim", d)
    }

    fn new_allow_missing_nextdata() -> Self {
        let d = "If ``True`` allow *$NEXTDATA* to be missing. This is a required \
                 keyword in all versions. However, most files only have one dataset \
                 in which case this keyword is meaningless.";
        Self::new_bool_param("allow_missing_nextdata", d)
    }

    fn new_trim_value_whitespace() -> Self {
        let d = "If ``True`` trim whitespace from all values. If performed, \
                 trimming precedes all other repair steps. Any values which are \
                 entirely spaces will become blanks, in which case it may also be \
                 sensible to enable ``allow_empty``.";
        Self::new_bool_param("trim_value_whitespace", d)
    }

    fn new_trim_trailing_whitespace() -> Self {
        let d = "If ``True`` trim whitespace off the end of *TEXT*. This will \
                 effectively move the ending offset of *TEXT* to the first \
                 non-whitespace byte immediately preceding the actual ending \
                 offset given in *HEADER*.";
        Self::new_bool_param("trim_trailing_whitespace", d)
    }

    fn new_ignore_standard_keys() -> Self {
        let d = "Remove standard keys from *TEXT*. The leading *$* is implied \
                 so do not include it.";
        Self::new_key_patterns_param("ignore_standard_keys", d)
    }

    fn new_promote_to_standard() -> Self {
        let d = "Promote nonstandard keys to standard keys in *TEXT*";
        Self::new_key_patterns_param("promote_to_standard", d)
    }

    fn new_demote_from_standard() -> Self {
        let d = "Demote nonstandard keys from standard keys in *TEXT*";
        Self::new_key_patterns_param("demote_from_standard", d)
    }

    fn new_key_patterns_param(argname: &str, desc: &str) -> Self {
        let common = format!(
            "The first member of the tuples is a list of strings which \
             match literally. The second member is a list of regular \
             expressions corresponding to {REGEXP_REF}."
        );
        let d = format!("{desc}. {common}");
        Self::new_param(argname, PyTuple::new_key_patterns(), d).def_auto()
    }

    fn new_rename_standard_keys() -> Self {
        let d = "Rename standard keys in *TEXT*. Keys matching the first part of \
                 the pair will be replaced by the second. Comparisons are case \
                 insensitive. The leading *$* is implied so do not include it.";
        Self::new_param("rename_standard_keys", PyDict::new_keystring_pairs(), d).def_auto()
    }

    fn new_replace_standard_key_values() -> Self {
        Self::new_param(
            "replace_standard_key_values",
            PyDict::new1(PyStr::new_keystring(), PyStr::default()),
            "Replace values for standard keys in *TEXT* Comparisons are case \
             insensitive. The leading *$* is implied so do not include it.",
        )
        .def_auto()
    }

    fn new_substitute_standard_key_values() -> Self {
        let d = "Apply sed-like substitution operation on matching standard \
                 keys. The leading *$* is implied when matching keys. The first \
                 dict corresponds to keys which are matched literally, and the \
                 second corresponds to keys which are matched via regular \
                 expression. The members in the 3-tuple values correspond to a \
                 regular expression, replacement string, and global flag \
                 respectively. The regular expression may contain capture \
                 expressions which must be matched exactly in the replacement \
                 string. If the global flag is ``True``, replace all found \
                 matches, otherwise only replace the first. Any references in \
                 replacement string must be given with surrounding brackets \
                 like ``\"${1}\"`` or ``\"${cygnus}\"``.";
        let p = PyTuple::new_sub_patterns();
        Self::new_param("substitute_standard_key_values", p, d).def_auto()
    }

    fn new_append_standard_keywords() -> Self {
        Self::new_param(
            "append_standard_keywords",
            PyDict::new1(PyStr::new_keystring(), PyStr::default()),
            "Append standard key/value pairs to *TEXT*. All keys and values \
             will be included as they appear here. The leading *$* is implied so \
             do not include it.",
        )
        .def_auto()
    }

    fn new_text_data_correction_param() -> Self {
        Self::new_config_correction_arg("text_data_correction", "*DATA*", false, "DataSegmentId")
    }

    fn new_text_analysis_correction_param() -> Self {
        Self::new_config_correction_arg(
            "text_analysis_correction",
            "*ANALYSIS*",
            false,
            "AnalysisSegmentId",
        )
    }

    fn new_ignore_text_data_offsets_param() -> Self {
        let d = "If ``True`` ignore *DATA* offsets in *TEXT*";
        Self::new_bool_param("ignore_text_data_offsets", d)
    }

    fn new_ignore_text_analysis_offsets_param() -> Self {
        let d = "If ``True`` ignore *ANALYSIS* offsets in *TEXT*";
        Self::new_bool_param("ignore_text_analysis_offsets", d)
    }

    fn new_allow_header_text_offset_mismatch_param() -> Self {
        let d = "If ``True`` allow *TEXT* and *HEADER* offsets to mismatch.";
        Self::new_bool_param("allow_header_text_offset_mismatch", d)
    }

    fn new_allow_missing_required_offsets_param(version: Option<Version>) -> Self {
        let s = match version {
            Some(Version::FCS3_2) => "*DATA*",
            Some(_) => "*DATA* and *ANALYSIS*",
            None => "*DATA* and *ANALYSIS* (3.1 or lower)",
        };
        Self::new_bool_param(
            "allow_missing_required_offsets",
            format!(
                "If ``True`` allow required {s} offsets in *TEXT* to be missing. \
                 If missing, fall back to offsets from *HEADER*."
            ),
        )
    }

    fn new_truncate_text_offsets_param() -> Self {
        let d = "If ``True`` truncate offsets that exceed end of file.";
        Self::new_bool_param("truncate_text_offsets", d)
    }

    fn new_allow_uneven_event_width_param() -> Self {
        let d = "If ``True`` allow event width to not perfectly divide length \
                 of *DATA*. Does not apply to delimited ASCII layouts. ";
        Self::new_bool_param("allow_uneven_event_width", d)
    }

    fn new_allow_tot_mismatch_param() -> Self {
        let d = "If ``True`` allow *$TOT* to not match number of events as \
                 computed by the event width and length of *DATA*. \
                 Does not apply to delimited ASCII layouts.";
        Self::new_bool_param("allow_tot_mismatch", d)
    }

    fn new_truncate_event_values() -> Self {
        let path = config_path("TruncateEventValues");
        let d = "Control which measurements will be truncated via *$PnR*";
        let pt = PyLiteral::new2(["int_only", "all", "none"], path);
        Self::new_param("truncate_event_values", pt, d).def_auto()
    }

    fn new_disallow_over_range() -> Self {
        let d = "Forbid event values in *DATA* to exceed *$PnR*. \
                 This flag only has an effect if the column is not truncated \
                 according to ``truncate_event_values``.";
        Self::new_tri_flag_param("disallow_over_range", false, "DisallowOverRange", d)
    }

    fn new_warnings_are_errors_param() -> Self {
        let d = "If ``True`` all warnings will be regarded as errors.";
        Self::new_bool_param("warnings_are_errors", d)
    }

    fn new_hide_warnings_param() -> Self {
        Self::new_bool_param("hide_warnings", "If ``True`` hide all warnings.")
    }
}

impl DocDefault {
    fn as_value(&self, pytype: &ArgPyType) -> (String, TokenStream2) {
        let err = || {
            panic!(
                "Arg type '{}' does not match default type '{}'",
                pytype,
                self.as_type()
            )
        };
        let py_str = |s| format!("\"{s}\"");
        match (self, pytype) {
            (Self::Auto, _) => pytype.doc_default(),
            (Self::Int(x), PyType::Int(_)) => (x.to_string(), pytype.doc_default().1),
            (Self::Str(x), PyType::Str(_)) => (py_str(x), pytype.doc_default().1),
            (dt, PyType::Option(pt)) => match (dt, &pt.inner) {
                (Self::Int(x), PyType::Int(y)) => (x.to_string(), y.doc_default().1),
                (Self::Str(x), PyType::Str(y)) => (py_str(x), y.doc_default().1),
                _ => err(),
            },
            _ => err(),
        }
    }

    // for error reporting
    fn as_type(&self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Str(_) => "str",
            Self::Int(_) => "int",
        }
    }
}

impl ClassDocString {
    pub fn new_class(summary: impl fmt::Display) -> Self {
        Self::new(summary.to_string(), vec![], vec![], ())
    }

    pub fn into_impl_class<F>(
        self,
        name: impl fmt::Display,
        path: &Path,
        constr: F,
    ) -> (Ident, TokenStream2)
    where
        F: FnOnce(TokenStream2) -> TokenStream2,
    {
        let (pyname, wrapped) = self.as_impl_wrapped(name, path);
        let sig = self.sig();
        let get_set_methods = self.quoted_methods();
        let new = constr(self.fun_args());
        let s = quote! {
            #wrapped

            #[pymethods]
            impl #pyname {
                #sig
                #[new]
                #[allow(clippy::too_many_arguments)]
                #new

                #get_set_methods

                // allow all classes to be deepcopy-ed
                fn __deepcopy__(&self, _memo: &Bound<'_, pyo3::PyAny>) -> Self {
                    self.clone()
                }
            }
        };
        (pyname, s)
    }

    fn as_impl_wrapped(&self, name: impl fmt::Display, path: &Path) -> (Ident, TokenStream2) {
        let doc = self.doc();
        let n = name.to_string();
        let pyname = format_ident!("Py{name}");
        let q = quote! {
            // pyo3 currently cannot add docstrings to __new__ methods, see
            // https://github.com/PyO3/pyo3/issues/4326
            //
            // workaround, put them on the structs themselves, which works but has the
            // disadvantage of being not next to the method def itself
            #doc
            #[pyclass(name = #n, eq)]
            #[derive(Clone, From, Into, PartialEq)]
            pub struct #pyname(#path);
        };
        (pyname, q)
    }
}

impl MethodDocString {
    pub fn new_method(summary: impl fmt::Display) -> Self {
        Self::new(summary.to_string(), vec![], vec![], None)
    }

    pub fn returns(self, returns: DocReturn<RetPyType>) -> Self {
        Self::new(self.summary, self.paragraphs, self.args, Some(returns))
    }
}

impl IvarDocString {
    pub fn new_ivar(summary: impl fmt::Display, ret_type: impl Into<ArgPyType>) -> Self {
        Self::new(summary.to_string(), vec![], (), DocReturn::new(ret_type))
    }

    pub fn ret_desc(self, desc: impl fmt::Display) -> Self {
        Self::new(self.summary, self.paragraphs, (), self.returns.desc(desc))
    }

    pub fn into_impl_get(
        mut self,
        class: &Ident,
        name: impl fmt::Display,
        f: impl FnOnce(&Ident, &ArgPyType) -> TokenStream2,
    ) -> TokenStream2 {
        self.append_summary_or_paragraph("read-only", "This attribute is read-only.");
        let i = format_ident!("{name}");
        let pt = &self.returns.rtype;
        let rt = pt.as_rust_type();
        let body = f(&i, pt);
        let doc = self.doc();
        quote! {
            #[pymethods]
            impl #class {
                #doc
                #[getter]
                fn #i(&self) -> #rt {
                    #body
                }
            }
        }
    }

    pub fn into_impl_get_set(
        mut self,
        class: &Ident,
        name: impl fmt::Display,
        fallible: bool,
        get_fun: impl FnOnce(&Ident, &ArgPyType) -> TokenStream2,
        set_fun: impl FnOnce(&Ident, &ArgPyType) -> TokenStream2,
    ) -> TokenStream2 {
        self.append_summary_or_paragraph("read-write", "This attribute is read-write.");
        let get = format_ident!("{name}");
        let set = format_ident!("set_{get}");
        let pt = &self.returns.rtype;
        let rt = pt.as_rust_type();
        let get_body = get_fun(&get, pt);
        let set_body = set_fun(&get, pt);
        let doc = self.doc();
        let ret = if fallible {
            quote!(PyResult<()>)
        } else {
            quote!(())
        };
        quote! {
            #[pymethods]
            impl #class {
                #doc
                #[getter]
                fn #get(&self) -> #rt {
                    #get_body
                }

                #[setter]
                fn #set(&mut self, #get: #rt) -> #ret {
                    #set_body
                }
            }
        }
    }
}

impl FunDocString {
    pub fn new_fun(summary: impl fmt::Display) -> Self {
        Self::new(summary.to_string(), vec![], vec![], None)
    }

    pub fn returns(self, returns: DocReturn<RetPyType>) -> Self {
        Self::new(self.summary, self.paragraphs, self.args, Some(returns))
    }
}

impl<A, S> DocString<A, Option<DocReturn<RetPyType>>, S> {
    pub fn ret_path(&self) -> TokenStream2 {
        self.returns
            .as_ref()
            .map(|x| {
                let inner = x.rtype.as_rust_type().to_token_stream();
                if x.exceptions.is_empty() {
                    inner
                } else {
                    quote!(PyResult<#inner>)
                }
            })
            .unwrap_or(quote!(()))
    }
}

impl<A, R, S> DocString<Vec<A>, R, S> {
    pub fn arg(mut self, arg: impl Into<A>) -> Self {
        self.args.push(arg.into());
        self
    }

    pub fn args(mut self, args: impl IntoIterator<Item = impl Into<A>>) -> Self {
        self.args.extend(args.into_iter().map(Into::into));
        self
    }

    /// Emit typed argument list for use in rust function signature
    pub fn fun_args(&self) -> TokenStream2
    where
        A: IsDocArg,
    {
        let xs: Vec<_> = self.args.iter().map(IsDocArg::fun_arg).collect();
        quote!(#(#xs),*)
    }

    /// Emit identifiers associated with function arguments
    pub fn idents(&self) -> TokenStream2
    where
        A: IsDocArg,
    {
        let xs: Vec<_> = self.args.iter().map(IsDocArg::ident).collect();
        quote!(#(#xs),*)
    }

    pub fn idents_into(&self) -> TokenStream2
    where
        A: IsDocArg,
    {
        let xs: Vec<_> = self.args.iter().map(IsDocArg::ident_into).collect();
        quote!(#(#xs),*)
    }

    /// Emit get/set methods associated with arguments (if any)
    fn quoted_methods(&self) -> TokenStream2
    where
        A: IsMethods,
    {
        let xs: Vec<_> = self.args.iter().map(IsMethods::quoted_methods).collect();
        quote!(#(#xs)*)
    }

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

    fn sig(&self) -> TokenStream2
    where
        A: IsDocArg,
        S: IsSelfArg,
    {
        assert!(
            self.has_defaults().is_some(),
            "non-default args after default args"
        );

        let ps = &self.args;
        let (flat_sig, txt_sig_): (Vec<_>, Vec<_>) = ps
            .iter()
            .map(|a| {
                let n = &a.argname();
                let i = format_ident!("{n}");
                if let Some(d) = a.default() {
                    let (t, r) = d.as_value(a.pytype());
                    // let t = d.as_py_value();
                    (quote! {#i=#r}, format!("{n}={t}"))
                } else {
                    (quote! {#i}, (*n).into())
                }
            })
            .unzip();
        let txt_sig = format!(
            "({})",
            S::ARG
                .into_iter()
                .chain(txt_sig_.iter().map(String::as_str))
                .join(", ")
        );
        quote! {
            #[pyo3(signature = (#(#flat_sig),*))]
            #[pyo3(text_signature = #txt_sig)]
        }
    }
}

impl<A, R, S> DocString<A, R, S> {
    pub fn para(mut self, paragraph: impl fmt::Display) -> Self {
        self.paragraphs.push(paragraph.to_string());
        self
    }

    pub fn paras(mut self, paragraphs: impl IntoIterator<Item = impl fmt::Display>) -> Self {
        self.paragraphs
            .extend(paragraphs.into_iter().map(|x| x.to_string()));
        self
    }

    fn doc(&self) -> TokenStream2
    where
        Self: fmt::Display,
    {
        let s = self.to_string();
        quote! {#[doc = #s]}
    }

    fn append_paragraph(&mut self, p: impl fmt::Display) {
        self.paragraphs.extend([p.to_string()]);
    }

    fn append_summary_or_paragraph(&mut self, suffix: impl fmt::Display, para: impl fmt::Display) {
        let new_summary = format!("{} ({suffix}).", self.summary.trim_end_matches('.'));
        if new_summary.len() > MAX_LINE_LEN {
            self.append_paragraph(para);
        } else {
            self.summary = new_summary;
        }
    }

    fn fmt_inner<'a, 'b, F0, F1, F2, F3, I0, I1, I2>(
        &'a self,
        f_args: F0,
        f_return: F1,
        f_args_exc: F2,
        f_return_exc: F3,
        f: &mut fmt::Formatter<'b>,
    ) -> Result<(), fmt::Error>
    where
        F0: FnOnce(&'a A) -> I0,
        F1: FnOnce(&'a R) -> Option<String>,
        F2: FnOnce(&'a A) -> I1,
        F3: FnOnce(&'a R) -> I2,
        I0: Iterator<Item = String> + 'a,
        I1: Iterator<Item = NamedPyException> + 'a,
        I2: Iterator<Item = &'a ReturnPyException> + 'a,
    {
        let ps = self
            .paragraphs
            .iter()
            .map(|s| fmt_docstring_nonparam(s.as_str()));
        let a = f_args(&self.args);
        let r = f_return(&self.returns);
        let rest = ps.chain(a).chain(r).join("\n\n");
        let arg_es = f_args_exc(&self.args).join("\n");
        let ret_es = f_return_exc(&self.returns).join("\n");
        assert!(self.summary.len() <= MAX_LINE_LEN, "summary is too long");
        write!(f, "{}\n\n{rest}\n\n{arg_es}\n\n{ret_es}", self.summary)
    }
}

impl<A, R, S> ToTokens for DocString<Vec<A>, R, S>
where
    Self: fmt::Display,
    A: IsDocArg,
    S: IsSelfArg,
{
    fn to_tokens(&self, tokens: &mut TokenStream2) {
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
        self.fmt_inner(
            |a| a.iter().map(ToString::to_string),
            |()| None,
            |a| {
                let es = a.iter().flat_map(|x| {
                    x.pytype()
                        .as_exceptions()
                        .into_iter()
                        .map(|e| e.into_named(x.argname()))
                });
                NamedPyException::merge(es).into_iter()
            },
            |()| empty(),
            f,
        )
    }
}

impl fmt::Display for IvarDocString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        self.fmt_inner(
            |()| empty(),
            |r| Some(r.to_string()),
            |()| empty(),
            |r| r.exceptions.iter(),
            f,
        )
    }
}

impl<A: fmt::Display + IsDocArg, S> fmt::Display
    for DocString<Vec<A>, Option<DocReturn<RetPyType>>, S>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        self.fmt_inner(
            |a| a.iter().map(ToString::to_string),
            |r| r.as_ref().map(ToString::to_string),
            |a| {
                let es = a.iter().flat_map(|x| {
                    x.pytype()
                        .as_exceptions()
                        .into_iter()
                        .map(|e| e.into_named(x.argname()))
                });
                NamedPyException::merge(es).into_iter()
            },
            |r| r.as_ref().map(|x| &x.exceptions).into_iter().flatten(),
            f,
        )
    }
}

impl fmt::Display for NamedPyException {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let pn = &self.inner.inner.pyname;
        let ns: Vec<_> = self.names.iter().map(|n| format!("``{n}``")).collect();
        let ns_ = fmt_comma_sep_list(&ns[..], "or");
        let n = self.inner.argmod.fmt(&ns_);
        if let Some(d) = self.inner.inner.desc.as_ref() {
            assert!(d.contains("%x"), "does not contain name ref ('%x'): {d}");
            let dd = d.replace("%x", &n);
            write!(f, ":raises {pn}: {dd}")
        } else {
            write!(f, ":raises {pn}:")
        }
    }
}

impl fmt::Display for ReturnPyException {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let pn = &self.0.pyname;
        if let Some(d) = self.0.desc.as_ref() {
            write!(f, ":raises {pn}: {d}")
        } else {
            write!(f, ":raises {pn}:")
        }
    }
}

impl<T: IsArgType> fmt::Display for DocArg<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let ro = match T::readonly() {
            Some(true) => "(read-only) ",
            Some(false) => "(read-write) ",
            None => "",
        };
        let pt = &self.pytype;
        let n = &self.argname;
        let d = self
            .default
            .as_ref()
            .map(|d| d.as_value(pt).0)
            .map_or(self.desc.clone(), |def| {
                format!("{} Defaults to ``{def}``.", self.desc)
            });
        let tn = T::TYPENAME;
        let at = T::ARGTYPE;
        let s0 = fmt_docstring_param(format!(":{at} {n}: {ro}{d}").as_str());
        let s1 = fmt_docstring_param(format!(":{tn} {n}: {pt}").as_str());
        write!(f, "{s0}\n{s1}")
    }
}

// TODO its a bit weird to totally ignore exceptions here, which makes it seem
// like this should be an inner type that wrapped with the stuff pertaining to
// exceptions
impl<T: fmt::Display> fmt::Display for DocReturn<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let t = fmt_docstring_param(format!(":rtype: {}", self.rtype).as_str());
        if let Some(d) = self
            .desc
            .as_ref()
            .map(|d| fmt_docstring_param(format!(":returns: {d}").as_str()))
        {
            write!(f, "{d}\n{t}")
        } else {
            f.write_str(t.as_str())
        }
    }
}

impl<R> fmt::Display for PyAtom<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Self::Str => PyStr::<R>::default().fmt(f),
            Self::Bool => PyBool::<R>::default().fmt(f),
            Self::Bytes => PyBytes::<R>::default().fmt(f),
            // dummy u8
            Self::Int => PyInt::<R>::from(RsInt::U8).fmt(f),
            // dummy f32
            Self::Float => PyFloat::<R>::from(RsFloat::F32).fmt(f),
            Self::Decimal => PyDecimal::<R>::default().fmt(f),
            Self::Datetime => PyDatetime::<R>::default().fmt(f),
            Self::Date => PyDate::<R>::default().fmt(f),
            Self::Time => PyTime::<R>::default().fmt(f),
            Self::None => f.write_str("None"),
            Self::Dict(k, v) => write!(f, ":py:class:`dict`\\ [{k}, {v}]"),
            Self::Tuple(xs) => {
                let s = if xs.is_empty() {
                    "()".into()
                } else {
                    xs.iter().join(", ")
                };
                write!(f, ":py:class:`tuple`\\ [{s}]")
            }
            Self::List(x) => write!(f, ":py:class:`list`\\ [{x}]"),
            Self::Literal(x) => x.fmt(f),
            Self::PyClass(x) => x.fmt(f),
            Self::Union(x0, x1, xs) => {
                let s = [&*(*x0), &*(*x1)].into_iter().chain(xs.iter()).join(" | ");
                write!(f, "{s}",)
            }
        }
    }
}

macro_rules! impl_display_pycomplex {
    ($t:ident) => {
        impl<R: Clone + PartialEq + Eq + Hash> fmt::Display for $t<R> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
                write!(f, "{}", self.as_atom().flatten_unions())
            }
        }
    };
}

impl_display_pycomplex!(PyOpt);
impl_display_pycomplex!(PyUnion);
impl_display_pycomplex!(PyDict);
impl_display_pycomplex!(PyList);
impl_display_pycomplex!(PyTuple);

impl fmt::Display for PyLiteral {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            ":obj:`~typing.Literal`\\ [{}]",
            once(&self.head)
                .chain(self.tail.iter())
                .map(|s| format!("\"{s}\""))
                .join(", ")
        )
    }
}

impl<R> fmt::Display for PyClass<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, ":py:class:`{}`", self.pyname)
    }
}

macro_rules! impl_display_pytype {
    ($t:ident, $s:expr) => {
        impl<R> fmt::Display for $t<R> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
                f.write_str($s)
            }
        }
    };
}

impl_display_pytype!(PyBool, ":py:class:`bool`");
impl_display_pytype!(PyStr, ":py:class:`str`");
impl_display_pytype!(PyBytes, ":py:class:`bytes`");
impl_display_pytype!(PyInt, ":py:class:`int`");
impl_display_pytype!(PyFloat, ":py:class:`float`");
impl_display_pytype!(PyDecimal, ":py:class:`~decimal.Decimal`");
impl_display_pytype!(PyDate, ":py:class:`~datetime.date`");
impl_display_pytype!(PyTime, ":py:class:`~datetime.time`");
impl_display_pytype!(PyDatetime, ":py:class:`~datetime.datetime`");

impl fmt::Display for SegmentSrc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let s = match self {
            Self::Header => "*HEADER*",
            Self::Any => "*HEADER* or *TEXT*",
        };
        f.write_str(s)
    }
}

impl Version {
    #[must_use]
    pub fn short(self) -> &'static str {
        match self {
            Self::FCS2_0 => "2.0",
            Self::FCS3_0 => "3.0",
            Self::FCS3_1 => "3.1",
            Self::FCS3_2 => "3.2",
        }
    }

    #[must_use]
    pub fn short_underscore(self) -> &'static str {
        match self {
            Self::FCS2_0 => "2_0",
            Self::FCS3_0 => "3_0",
            Self::FCS3_1 => "3_1",
            Self::FCS3_2 => "3_2",
        }
    }

    // #[must_use]
    // pub fn from_short(s: &str) -> Option<Self> {
    //     match s {
    //         "2.0" => Some(Self::FCS2_0),
    //         "3.0" => Some(Self::FCS3_0),
    //         "3.1" => Some(Self::FCS3_1),
    //         "3.2" => Some(Self::FCS3_2),
    //         _ => None,
    //     }
    // }

    #[must_use]
    pub fn from_short_underscore(s: &str) -> Option<Self> {
        match s {
            "2_0" => Some(Self::FCS2_0),
            "3_0" => Some(Self::FCS3_0),
            "3_1" => Some(Self::FCS3_1),
            "3_2" => Some(Self::FCS3_2),
            _ => None,
        }
    }
}

fn unwrap_generic<'a>(name: &str, ty: &'a Path) -> (&'a Path, bool) {
    if let Some(segment) = ty.segments.last()
        && segment.ident == name
        && let PathArguments::AngleBracketed(args) = &segment.arguments
        && let Some(GenericArgument::Type(Type::Path(inner_type))) = args.args.first()
    {
        return (&inner_type.path, true);
    }
    (ty, false)
}

fn unwrap_type_as_path(ty: &Type) -> &Path {
    if let Type::Path(p) = ty {
        &p.path
    } else {
        panic!("not a path")
    }
}

fn fmt_docstring_nonparam(s: &str) -> String {
    fmt_hanging_indent(MAX_LINE_LEN, 0, s)
}

fn fmt_docstring_param(s: &str) -> String {
    fmt_hanging_indent(MAX_LINE_LEN, 4, s)
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
                tmp = vec![i.as_str()];
            } else {
                line_len = x.len();
                tmp = vec![];
            }
        } else {
            line_len += 1;
        }
        tmp.push(x);
    }
    zs.push(tmp.iter().join(" "));
    zs.iter().join("\n")
}

fn fmt_comma_sep_list<X: fmt::Display>(xs: &[X], conj: &str) -> String {
    let n = xs.len();
    match n.cmp(&2) {
        Ordering::Less => xs.iter().join(""),
        Ordering::Equal => xs.iter().join(&format!(" {conj} ")),
        Ordering::Greater => {
            let mut it = xs.iter();
            let x0 = it.by_ref().take(n - 1).join(", ");
            let c = format!(", {conj} ");
            once(x0).chain(it.map(ToString::to_string)).join(&c)
        }
    }
}

pub fn path_strip_args(mut path: Path) -> Path {
    for segment in &mut path.segments {
        segment.arguments = PathArguments::None;
    }
    path
}

pub fn keyword_path(n: &str) -> Path {
    let t = format_ident!("{n}");
    parse_quote!(fireflow_core::text::keywords::#t)
}

pub fn config_path(n: &str) -> Path {
    let t = format_ident!("{n}");
    parse_quote!(fireflow_core::config::#t)
}

fn correction_path(is_header: bool, id: &str) -> Path {
    let src = if is_header {
        "SegmentFromHeader"
    } else {
        "SegmentFromTEXT"
    };
    let s = format_ident!("{src}");
    let i = format_ident!("{id}");
    let root = quote!(fireflow_core::segment);
    parse_quote! (#root::OffsetCorrection<#root::#i, #root::#s>)
}

fn element_path(version: Version) -> Path {
    let otype = pyoptical(version);
    let ttype = pytemporal(version);
    let element_path = quote!(fireflow_core::text::named_vec::Element);
    parse_quote!(#element_path<#ttype, #otype>)
}

fn pyoptical(version: Version) -> Ident {
    format_ident!("PyOptical{}", version.short_underscore())
}

fn pytemporal(version: Version) -> Ident {
    format_ident!("PyTemporal{}", version.short_underscore())
}

const MAX_LINE_LEN: usize = 72;

const CHRONO_REF: &str =
    "`chrono <https://docs.rs/chrono/latest/chrono/format/strftime/index.html>`__";

const REGEXP_REF: &str = "`regexp-syntax <https://docs.rs/regex/latest/regex/#syntax>`__";

const ALL_VERSION_STRINGS: [&str; 4] = ["FCS2.0", "FCS3.0", "FCS3.1", "FCS3.2"];

pub const ALL_VERSIONS: [Version; 4] = [
    Version::FCS2_0,
    Version::FCS3_0,
    Version::FCS3_1,
    Version::FCS3_2,
];

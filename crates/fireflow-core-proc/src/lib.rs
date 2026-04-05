use proc_macro::TokenStream;
use quote::quote;
use syn::{
    AngleBracketedGenericArguments, Attribute, Data, DeriveInput, Fields, GenericParam, Generics,
    Ident, Path, PathArguments, Token, Visibility, WherePredicate,
    parse::{Parse, ParseStream},
    parse_macro_input, parse_quote,
    punctuated::Punctuated,
};

#[proc_macro]
pub fn impl_generic_enum_from(input: TokenStream) -> TokenStream {
    let parsed = parse_macro_input!(input as ImplFrom);
    let name = parsed.type_path;
    let generics =
        if let PathArguments::AngleBracketed(args) = &name.segments.last().unwrap().arguments {
            args.args.iter().collect()
        } else {
            vec![]
        };

    let mut impls = vec![];

    for t in &parsed.targets {
        let src = &t.src;
        let var = &t.var;
        let q = quote! {
            impl<#(#generics),*> From<#src> for #name {
                fn from(value: #src) -> Self {
                    Self::#var(value)
                }
            }
        };
        impls.push(q);
    }

    quote!(#(#impls)*).into()
}

struct ImplFrom {
    type_path: Path,
    _comma_token: Token![,],
    targets: Punctuated<FromType, Token![,]>,
}

struct FromType {
    var: Ident,
    _arrow_token: Token![<-],
    src: Path,
}

impl Parse for ImplFrom {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let ret = Self {
            type_path: input.parse()?,
            _comma_token: input.parse()?,
            targets: Punctuated::parse_separated_nonempty(input)?,
        };
        Ok(ret)
    }
}

impl Parse for FromType {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let ret = Self {
            var: input.parse()?,
            _arrow_token: input.parse()?,
            src: input.parse()?,
        };
        Ok(ret)
    }
}

#[proc_macro_derive(AllIntoPyErr, attributes(bound))]
pub fn derive_into_pyerr(input: TokenStream) -> TokenStream {
    let parsed = parse_macro_input!(input as DeriveInput);
    let name = &parsed.ident;
    let generics = &parsed.generics;
    let gen_idents = generic_idents(generics);

    let bounds = parse_bounds(&parsed);

    let mut into_clauses = vec![];

    if let Data::Enum(e) = &parsed.data {
        for v in &e.variants {
            let i = &v.ident;
            if let Fields::Unnamed(fs) = &v.fields
                && fs.unnamed.len() == 1
            {
                let c = quote!(#name::#i(x) => x.into());
                into_clauses.push(c);
            } else {
                panic!("only variants with one unnamed field are allowed")
            }
        }
    } else {
        panic!("not an enum")
    }

    let ret = quote! {
        impl #generics From<#name<#(#gen_idents),*>> for pyo3::PyErr
        where
            #(#bounds),*
        {
            fn from(value: #name<#(#gen_idents),*>) -> Self {
                match value {
                    #(#into_clauses),*
                }
            }
        }
    };
    ret.into()
}

/// Convert Rust type to PyErr using Display
///
/// The exception type is determined by the pyerr attribute. If not path is
/// supplied, it is assumed to be from pyo3::exceptions (example, PyValueError).
#[proc_macro_derive(DisplayAsPyErr, attributes(pyerr, bound))]
pub fn derive_display_as_pyerr(input: TokenStream) -> TokenStream {
    let parsed = parse_macro_input!(input as DeriveInput);
    let name = &parsed.ident;
    let generics = &parsed.generics;
    let gen_idents = generic_idents(generics);

    let bounds = parse_bounds(&parsed);

    let epath: Path = parsed
        .attrs
        .iter()
        .find_map(|a| {
            let m = a.meta.require_list().ok()?;
            let x = (m.path.get_ident().unwrap() == "pyerr").then_some(&m.tokens)?;
            Some(parse_quote!(#x))
        })
        .expect("could not find 'pyerr' attribute");

    let full_epath: Path = if epath.segments.len() == 1 {
        parse_quote!(pyo3::exceptions::#epath)
    } else {
        epath
    };

    let ret = quote! {
        impl #generics From<#name<#(#gen_idents),*>> for pyo3::PyErr
        where
            #(#bounds),*
        {
            fn from(value: #name<#(#gen_idents),*>) -> Self {
                #full_epath::new_err(value.to_string())
            }
        }
    };
    ret.into()
}

/// Implement FromPyObject for a newtype with extraction on the inner type.
///
/// Unfortunately, derive(FromPyObject) only does half of this. It will extract
/// PyAny to the inner type but on failure will produce a generic error saying
/// something like "field blabla can't be extracted." This isn't very useful,
/// and I would rather give a python user a specialized exception that actually
/// corresponds to the error of the inner type. For example, I want an
/// OverflowError if I give a -1 to a function that takes a u8, instead of
/// simply saying "field 0 is wrong."
#[proc_macro_derive(FromInnerPyObject, attributes(bound))]
pub fn derive_from_py_transparent(input: TokenStream) -> TokenStream {
    let parsed = parse_macro_input!(input as DeriveInput);
    let name = &parsed.ident;
    let generics = &parsed.generics;
    let gen_idents = generic_idents(generics);
    let bounds = parse_bounds(&parsed);

    let ret = quote! {
        impl<'py, #(#gen_idents),*> pyo3::conversion::FromPyObject<'py> for #name<#(#gen_idents),*>
        where
            #(#bounds),*
        {
            fn extract_bound(ob: &pyo3::Bound<'py, pyo3::PyAny>) -> pyo3::PyResult<Self> {
                Ok(Self(pyo3::prelude::PyAnyMethods::extract(ob)?))
            }
        }
    };
    ret.into()
}

/// Implement IntoPyObject to PyString via Display for the Rust type
#[proc_macro_derive(IntoPyString)]
pub fn derive_to_py_via_display(input: TokenStream) -> TokenStream {
    let parsed = parse_macro_input!(input as DeriveInput);
    let name = &parsed.ident;

    let to_string = parse_quote!(ToString::to_string);
    into_str_pyobject(name, &to_string)
}

/// Implement IntoPyObject to PyString via DisplayNE for the Rust type
#[proc_macro_derive(IntoPyNEString)]
pub fn derive_to_py_via_ne_display(input: TokenStream) -> TokenStream {
    let parsed = parse_macro_input!(input as DeriveInput);
    let name = &parsed.ident;

    let to_string = parse_quote!(fireflow_types::nonempty_string::DisplayableNE::as_string);
    into_str_pyobject(name, &to_string)
}

fn into_str_pyobject(name: &Ident, to_string: &Path) -> TokenStream {
    quote! {
        impl<'py> pyo3::conversion::IntoPyObject<'py> for #name {
            type Target = pyo3::types::PyString;
            type Output = pyo3::Bound<'py, Self::Target>;
            type Error = std::convert::Infallible;

            fn into_pyobject(
                self,
                py: pyo3::marker::Python<'py>,
            ) -> Result<
                <Self as pyo3::conversion::IntoPyObject<'py>>::Output,
                <Self as pyo3::conversion::IntoPyObject<'py>>::Error
            > {
                #to_string(&self).into_pyobject(py)
            }
        }
    }
    .into()
}

/// Implement FromPyObject from PyString via FromStr for the Rust type
#[proc_macro_derive(FromPyString)]
pub fn derive_from_py_via_fromstr(input: TokenStream) -> TokenStream {
    let parsed = parse_macro_input!(input as DeriveInput);
    let name = &parsed.ident;

    let ret = quote! {
        impl<'py> pyo3::conversion::FromPyObject<'py> for #name {
            fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
                let x: String = pyo3::prelude::PyAnyMethods::extract(ob)?;
                let ret = x.parse()?;
                Ok(ret)
            }
        }
    };
    ret.into()
}

/// Implement FromPyObject for a sealed newtype via TryFrom
#[proc_macro_derive(TryFromPyObject)]
pub fn derive_try_from_py(input: TokenStream) -> TokenStream {
    let parsed = parse_macro_input!(input as DeriveInput);
    let name = &parsed.ident;

    let inner = if let Data::Struct(s) = &parsed.data {
        let fs = match &s.fields {
            Fields::Named(f) => &f.named,
            Fields::Unnamed(f) => &f.unnamed,
            Fields::Unit => panic!("must have one field"),
        };
        if fs.len() == 1 {
            let f0 = fs.first().unwrap();
            assert!(
                f0.vis == Visibility::Inherited,
                "inner field should be private"
            );
            &f0.ty
        } else {
            panic!("must have one field")
        }
    } else {
        panic!("must be a struct")
    };

    let ret = quote! {
        impl<'py> pyo3::FromPyObject<'py> for #name {
            fn extract_bound(ob: &pyo3::Bound<'py, pyo3::PyAny>) -> pyo3::PyResult<Self> {
                let x: #inner = pyo3::prelude::PyAnyMethods::extract(ob)?;
                let y = x.try_into()?;
                Ok(y)
            }
        }
    };
    ret.into()
}

fn parse_bounds(parsed: &DeriveInput) -> Vec<WherePredicate> {
    parsed
        .attrs
        .iter()
        .filter_map(|a| {
            let m = a.meta.require_list().ok()?;
            let x = (m.path.get_ident().unwrap() == "bound").then_some(&m.tokens)?;
            Some(parse_quote!(#x))
        })
        .collect()
}

fn generic_idents(gs: &Generics) -> Vec<&Ident> {
    gs.params
        .iter()
        .filter_map(|g| match g {
            GenericParam::Const(c) => Some(&c.ident),
            GenericParam::Type(p) => Some(&p.ident),
            GenericParam::Lifetime(_) => None,
        })
        .collect()
}

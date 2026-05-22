use proc_macro::TokenStream;
use quote::quote;
use syn::{
    Data, DeriveInput, Fields, GenericArgument, GenericParam, Generics, Ident, Path, PathArguments,
    Token, Type, Visibility, WherePredicate, parenthesized,
    parse::{Parse, ParseStream},
    parse_macro_input, parse_quote,
    punctuated::Punctuated,
    token::Paren,
};

#[proc_macro]
pub fn impl_generic_enum_from(input: TokenStream) -> TokenStream {
    let parsed = parse_macro_input!(input as ImplFrom);
    let name = &parsed.type_path.segments.first().unwrap().ident;
    let self_generics = if let PathArguments::AngleBracketed(args) =
        &parsed.type_path.segments.last().unwrap().arguments
    {
        args.args.iter().collect()
    } else {
        vec![]
    };

    let more_generics: Vec<_> = parsed.generics.params.iter().map(|c| quote!(#c)).collect();

    let mut impls = vec![];

    // #[allow(clippy::never_loop)]
    for t in &parsed.targets {
        let src = &t.src;
        let var = &t.var;
        // If one of the supplied linked parameters matches one of the
        // parameters in the self type, replace it with the From parameter (ie
        // the thing in From<...>)
        let mut local_generics = vec![];
        let local_self_generics: Vec<_> = self_generics
            .iter()
            .map(|&g| match (&t.link, g) {
                (Some(l), GenericArgument::Type(Type::Path(p)))
                    if p.path.segments.first().unwrap().ident == l.param =>
                {
                    quote!(#src)
                }
                _ => {
                    // if linked parameter is found, don't keep the param in the
                    // function-level generic list since this will be
                    // unconstrained
                    let ret = quote!(#g);
                    local_generics.push(ret.clone());
                    ret
                }
            })
            .collect();
        local_generics.extend(more_generics.clone());
        local_generics.extend(t.generics.params.iter().map(|c| quote!(#c)));
        let q = quote! {
            impl<#(#local_generics),*> From<#src> for #name<#(#local_self_generics),*> {
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
    generics: Generics,
    _comma_token0: Token![,],
    targets: Punctuated<FromType, Token![,]>,
}

struct FromType {
    var: Ident,
    link: Option<GenericLink>,
    generics: Generics,
    _arrow_token: Token![~],
    src: Path,
}

struct GenericLink {
    _paren: Paren,
    param: Ident,
}

impl Parse for ImplFrom {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let ret = Self {
            type_path: input.parse()?,
            generics: input.parse()?,
            _comma_token0: input.parse()?,
            targets: Punctuated::parse_separated_nonempty(input)?,
        };
        Ok(ret)
    }
}

impl Parse for FromType {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let var = input.parse()?;
        let link = input.peek(Paren).then(|| input.parse()).transpose()?;
        let ret = Self {
            var,
            link,
            generics: input.parse()?,
            _arrow_token: input.parse()?,
            src: input.parse()?,
        };
        Ok(ret)
    }
}

impl Parse for GenericLink {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let content;
        let paren = parenthesized!(content in input);
        let param = content.parse()?;
        let ret = Self {
            _paren: paren,
            param,
        };
        Ok(ret)
    }
}

#[proc_macro_derive(IntoInner, attributes(into_inner))]
pub fn derive_from_inner(input: TokenStream) -> TokenStream {
    let parsed = parse_macro_input!(input as DeriveInput);
    let name = &parsed.ident;
    let generics = &parsed.generics;
    let gen_idents = generic_idents(generics);

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

    let bounds: Vec<_> = gen_idents.iter().map(|i| quote!(#i: Into<Self>)).collect();

    let ret: Vec<_> = parsed
        .attrs
        .iter()
        .filter_map(|a| {
            let m = a.meta.require_list().ok()?;
            let x = (m.path.get_ident().unwrap() == "into_inner").then_some(&m.tokens)?;
            Some(parse_quote!(#x))
        })
        .map(|inner: Path| {
            quote! {
                impl #generics From<#name<#(#gen_idents),*>> for #inner
                where
                    #(#bounds),*
                {
                    fn from(value: #name<#(#gen_idents),*>) -> Self {
                        match value {
                            #(#into_clauses),*
                        }
                    }
                }
            }
        })
        .collect();
    quote!(#(#ret),*).into()
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
        impl<'a, 'py, #(#gen_idents),*> pyo3::conversion::FromPyObject<'a, 'py> for #name<#(#gen_idents),*>
        where
            #(#bounds),*
        {
            type Error = pyo3::PyErr;
            fn extract(obj: pyo3::Borrowed<'a, 'py, pyo3::PyAny>) -> pyo3::PyResult<Self> {
                Ok(Self(pyo3::conversion::FromPyObject::<'_, 'py>::extract(obj)?))
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
        impl<'py> pyo3::conversion::FromPyObject<'_, 'py> for #name {
            type Error = pyo3::PyErr;

            fn extract(obj: pyo3::Borrowed<'_, 'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
                let x: String = pyo3::conversion::FromPyObject::<'_, 'py>::extract(obj)?;
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
        impl<'py> pyo3::conversion::FromPyObject<'_, 'py> for #name {
            type Error = pyo3::PyErr;
            fn extract(obj: pyo3::Borrowed<'_, 'py, pyo3::PyAny>) -> pyo3::PyResult<Self> {
                let x: #inner = pyo3::conversion::FromPyObject::<'_, 'py>::extract(obj)?;
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

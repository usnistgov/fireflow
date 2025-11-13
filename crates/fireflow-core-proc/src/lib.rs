use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, parse_macro_input};

#[proc_macro_derive(IntoPyErr)]
pub fn derive_into_pyerr(input: TokenStream) -> TokenStream {
    let parsed = parse_macro_input!(input as DeriveInput);
    let name = &parsed.ident;

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
        impl From<#name> for pyo3::PyErr {
            fn from(value: #name) -> Self {
                match value {
                    #(#into_clauses),*
                }
            }
        }
    };
    ret.into()
}

#[proc_macro_derive(IntoBuiltinPyErr, attributes(pyerr))]
pub fn derive_into_builtin_pyerr(input: TokenStream) -> TokenStream {
    let parsed = parse_macro_input!(input as DeriveInput);
    let name = &parsed.ident;

    let ename = parsed
        .attrs
        .iter()
        .find_map(|a| {
            let m = a.meta.require_list().unwrap();
            (m.path.get_ident().unwrap() == "pyerr").then_some(&m.tokens)
        })
        .expect("could not find 'pyerr' attribute");

    let ret = quote! {
        impl From<#name> for pyo3::PyErr {
            fn from(value: #name) -> Self {
                pyo3::exceptions::#ename::new_err(value.to_string())
            }
        }
    };
    ret.into()
}

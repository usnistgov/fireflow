extern crate proc_macro;

use proc_macro::TokenStream;

use quote::{format_ident, quote};
use syn::{
    parse::{Parse, ParseStream},
    parse_macro_input,
    punctuated::Punctuated,
    token::Comma,
    LitStr, Path, Result, Token, Type,
};

#[proc_macro]
pub fn get_set_metaroot_proc(input: TokenStream) -> TokenStream {
    fn_proc_macro_impl(input)
}

#[derive(Debug)]
struct GetSetMetarootInfo {
    keyword: Path,
    pytype: LitStr,
    rstypes: Punctuated<Type, Token![,]>,
}

impl Parse for GetSetMetarootInfo {
    fn parse(input: ParseStream) -> Result<Self> {
        let keyword: Path = input.parse()?;
        let _: Comma = input.parse()?;
        let pytype: LitStr = input.parse()?;
        let _: Comma = input.parse()?;
        let pytypes = Punctuated::parse_terminated(input)?;
        Ok(Self {
            keyword,
            pytype,
            rstypes: pytypes,
        })
    }
}

fn fn_proc_macro_impl(input: TokenStream) -> TokenStream {
    let info = parse_macro_input!(input as GetSetMetarootInfo);
    let kw = &info.keyword;
    let kts = kw.segments.last().unwrap().ident.to_string();

    let doc_summary = format!("Value for *${}*", kts.to_uppercase());
    let doc_type = format!(":type: {}", info.pytype.value());
    let get = format_ident!("get_{}", kts.to_lowercase());
    let set = format_ident!("set_{}", kts.to_lowercase());

    let outputs: Vec<_> = info
        .rstypes
        .iter()
        .map(|t| {
            quote! {
                #[pymethods]
                impl #t {
                    #[doc = #doc_summary]
                    #[doc = ""]
                    #[doc = #doc_type]
                    #[getter]
                    fn #get(&self) -> #kw {
                        self.0.metaroot::<#kw>().clone()
                    }

                    #[setter]
                    fn #set(&mut self, x: #kw) {
                        self.0.set_metaroot(x)
                    }
                }
            }
        })
        .collect();

    quote! {#(#outputs)*}.into()
}

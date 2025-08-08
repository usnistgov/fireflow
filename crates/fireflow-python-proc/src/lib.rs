extern crate proc_macro;

use proc_macro::TokenStream;

use quote::{format_ident, quote};
use syn::{
    parse::{Parse, ParseStream},
    parse_macro_input,
    punctuated::Punctuated,
    token::Comma,
    GenericArgument, LitStr, Path, PathArguments, Result, Token, Type,
};

#[proc_macro]
pub fn get_set_metaroot(input: TokenStream) -> TokenStream {
    let info = parse_macro_input!(input as GetSetMetarootInfo);
    let kw = &info.keyword;
    let (kw_inner, optional) = unwrap_option(kw);
    let kts = info
        .name_override
        .map(|x| x.value())
        .unwrap_or(kw_inner.segments.last().unwrap().ident.to_string());

    let doc_summary = format!("Value for *${}*", kts.to_uppercase());
    let doc_type = format!(
        ":type: {}{}",
        info.pytype.value(),
        if optional { " | None" } else { "" }
    );
    let get = format_ident!("get_{}", kts.to_lowercase());
    let set = format_ident!("set_{}", kts.to_lowercase());
    let get_inner = format_ident!("{}", if optional { "metaroot_opt" } else { "metaroot" });
    let clone_inner = format_ident!("{}", if optional { "cloned" } else { "clone" });

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
                        self.0.#get_inner::<#kw_inner>().#clone_inner()
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

#[derive(Debug)]
struct GetSetMetarootInfo {
    keyword: Path,
    pytype: LitStr,
    name_override: Option<LitStr>,
    rstypes: Punctuated<Type, Token![,]>,
}

impl Parse for GetSetMetarootInfo {
    fn parse(input: ParseStream) -> Result<Self> {
        let keyword: Path = input.parse()?;
        let _: Comma = input.parse()?;
        let pytype: LitStr = input.parse()?;
        let _: Comma = input.parse()?;
        let name_override = if input.peek(LitStr) {
            let x = input.parse()?;
            let _: Comma = input.parse()?;
            Some(x)
        } else {
            None
        };
        let pytypes = Punctuated::parse_terminated(input)?;
        Ok(Self {
            keyword,
            pytype,
            name_override,
            rstypes: pytypes,
        })
    }
}

fn unwrap_option(ty: &Path) -> (&Path, bool) {
    if let Some(segment) = ty.segments.last() {
        if segment.ident == "Option" {
            if let PathArguments::AngleBracketed(args) = &segment.arguments {
                if let Some(GenericArgument::Type(Type::Path(inner_type))) = args.args.first() {
                    return (&inner_type.path, true);
                }
            }
        }
    }
    (ty, false)
}

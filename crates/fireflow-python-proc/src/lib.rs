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
    let kw = &info.kwtype;
    let (kw_inner, optional) = unwrap_generic("Option", kw);
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
        .parent_types
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

#[proc_macro]
pub fn get_set_all_meas_proc(input: TokenStream) -> TokenStream {
    let info = parse_macro_input!(input as GetSetAllMeas);
    let kw = &info.rstype;
    let (kw_mid, optical_only) = unwrap_generic("NonCenterElement", kw);
    let (kw_inner, optional) = unwrap_generic("Option", kw_mid);
    let s = info.suffix.value();

    let doc_summary = format!("Value of *$Pn{}* for all measurements.", s.to_uppercase());
    let doc_middle = if optical_only {
        "\n``()`` will be returned for time since this keyword is not defined there.\n"
    } else {
        "\n"
    };
    let doc_type = format!(
        ":type: list[{}]",
        info.pytype.value()
            + if optical_only { " | ()" } else { "" }
            + if optional { " | None" } else { "" },
    );
    let get = format_ident!("get_all_pn{}", s.to_lowercase());
    let set = format_ident!("set_all_pn{}", s.to_lowercase());

    let outputs: Vec<_> = info
        .parent_types
        .iter()
        .map(|t| {
            let kw = if optional {
                quote! {Option<#kw_inner>}
            } else {
                quote! {#kw_inner}
            };
            let fn_get = if optical_only {
                quote! {
                    fn #get(&self) -> Vec<NonCenterElement<#kw>> {
                        self.0
                            .optical_opt()
                            .map(|e| e.0.map_non_center(|x| x.cloned()).into())
                            .collect()
                    }
                }
            } else {
                quote! {
                    fn #get(&self) -> Vec<#kw> {
                        self.0.meas_opt().map(|x| x.cloned()).collect()
                    }
                }
            };
            let fn_set = if optical_only {
                quote! {
                    fn #set(&mut self, xs: Vec<NonCenterElement<#kw>>) -> PyResult<()> {
                        self.0.set_optical(xs).py_term_resolve_nowarn()
                    }
                }
            } else {
                quote! {
                    fn #set(&mut self, xs: Vec<#kw>) -> PyResult<()> {
                        Ok(self.0.set_meas(xs)?)
                    }
                }
            };
            quote! {
                #[pymethods]
                impl #t {
                    #[doc = #doc_summary]
                    #[doc = #doc_middle]
                    #[doc = #doc_type]
                    #[getter]
                    #fn_get

                    #[setter]
                    #fn_set
                }
            }
        })
        .collect();

    quote! {#(#outputs)*}.into()
}

#[proc_macro]
pub fn convert_methods_proc(input: TokenStream) -> TokenStream {
    let pytype: Path = parse_macro_input!(input);
    let name = pytype.segments.last().unwrap().ident.to_string();
    let (base, version) = name.as_str().split_at(name.len() - 3);
    let all_versions = ["2_0", "3_0", "3_1", "3_2"];
    if !all_versions.iter().any(|&v| v == version) {
        panic!("invalid version {version}")
    }
    let outputs: Vec<_> = all_versions
        .iter()
        .filter(|&&v| v != version)
        .map(|v| {
            let fn_name = format_ident!("version_{v}");
            let target_type = format_ident!("{base}{v}");
            let target_rs_type = target_type.to_string().replace("Py", "");
            let pretty_version = v.replace("_", ".");
            let doc_summary = format!("Convert to FCS {pretty_version}.");
            let doc_return = format!(":return: A new class conforming to FCS {pretty_version}");
            let doc_rtype = format!(":rtype: :class:`{target_rs_type}`");
            quote! {
                #[pymethods]
                impl #pytype {
                    #[doc = #doc_summary]
                    ///
                    /// Will raise an exception if target version requires data which
                    /// is not present in ``self``.
                    ///
                    /// :param bool force: If ``False``, do not proceed with
                    ///     conversion if it would result in data loss. This is
                    ///     most likely to happen when converting from a later
                    ///     to an earlier version, as many keywords from the
                    ///     later version may not exist in the earlier version.
                    ///     There is no place to keep these values so they must
                    ///     be discarded. Set to ``True`` to perform the
                    ///     conversion with such discarding; otherwise, remove
                    ///     the keywords manually before converting.
                    #[doc = #doc_return]
                    #[doc = #doc_rtype]
                    #[pyo3(signature = (force = false))]
                    fn #fn_name(&self, force: bool) -> PyResult<#target_type> {
                        self.0.clone().try_convert(force).py_term_resolve().map(|x| x.into())
                    }
                }
            }
        })
        .collect();

    quote! {#(#outputs)*}.into()
}

#[derive(Debug)]
struct GetSetMetarootInfo {
    kwtype: Path,
    pytype: LitStr,
    name_override: Option<LitStr>,
    parent_types: Punctuated<Type, Token![,]>,
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
            kwtype: keyword,
            pytype,
            name_override,
            parent_types: pytypes,
        })
    }
}

struct GetSetAllMeas {
    rstype: Path,
    suffix: LitStr,
    pytype: LitStr,
    parent_types: Punctuated<Type, Token![,]>,
}

impl Parse for GetSetAllMeas {
    fn parse(input: ParseStream) -> Result<Self> {
        let rstype: Path = input.parse()?;
        let _: Comma = input.parse()?;
        let suffix: LitStr = input.parse()?;
        let _: Comma = input.parse()?;
        let pytype: LitStr = input.parse()?;
        let _: Comma = input.parse()?;
        let parent_types = Punctuated::parse_terminated(input)?;
        Ok(Self {
            rstype,
            suffix,
            pytype,
            parent_types,
        })
    }
}

fn unwrap_generic<'a>(name: &str, ty: &'a Path) -> (&'a Path, bool) {
    if let Some(segment) = ty.segments.last() {
        if segment.ident == name {
            if let PathArguments::AngleBracketed(args) = &segment.arguments {
                if let Some(GenericArgument::Type(Type::Path(inner_type))) = args.args.first() {
                    return (&inner_type.path, true);
                }
            }
        }
    }
    (ty, false)
}

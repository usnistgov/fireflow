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
pub fn get_set_meas_common_proc(input: TokenStream) -> TokenStream {
    let info = parse_macro_input!(input as CommonMeasGetSet);
    let rstype = &info.rstype;
    let nametype = &info.nametype;
    let namefam = &info.namefam;
    let name = info.rstype.segments.last().unwrap().ident.to_string();
    let (_, version) = split_version(name.as_str());
    let otype = format_ident!("PyOptical{version}");
    let ttype = format_ident!("PyTemporal{version}");
    let potype = format!("Optical{version}");
    let pttype = format!("Temporal{version}");

    let rtype_get_temp = format!(":rtype: (int, str, :py:class:`{}`) | None", potype);
    let rtype_all_meas = format!(
        ":rtype: list[:py:class:`{}` | :py:class:`{}`]",
        pttype, potype
    );
    let rtype_remove_named_meas = format!(
        ":rtype: (int, :py:class:`{}` | :py:class:`{}`)",
        pttype, potype
    );
    let rtype_remove_index_meas = format!(
        ":rtype: (str, :py:class:`{}` | :py:class:`{}`)",
        pttype, potype
    );
    let rtype_get_meas = format!(":rtype: :py:class:`{}` | :py:class:`{}`", pttype, potype);

    let param_type_optical = format!(":type meas: :py:class:`{}`", potype);

    quote! {
        #[pymethods]
        impl #rstype {
            /// Get the temporal measurement if it exists.
            ///
            /// :return: Index, name, and measurement or ``None``
            #[doc = #rtype_get_temp]
            #[getter]
            fn get_temporal(&self) -> Option<(MeasIndex, Shortname, #ttype)> {
                self.0
                    .temporal()
                    .map(|t| (t.index, t.key.clone(), t.value.clone().into()))
            }

            /// Get all measurements.
            ///
            /// :return: list of measurements
            #[doc = #rtype_all_meas]
            #[getter]
            fn measurements(&self) -> Vec<Element<#ttype, #otype>> {
                // This might seem inefficient since we are cloning
                // everything, but if we want to map a python lambda
                // function over the measurements we would need to to do
                // this anyways, so simply returnig a copied list doesn't
                // lose anything and keeps this API simpler.
                let ms: &NamedVec<_, _, _, _> = self.0.as_ref();
                ms.iter()
                    .map(|(_, e)| e.bimap(|t| t.value.clone(), |o| o.value.clone()))
                    .map(|v| v.inner_into())
                    .collect()
            }

            /// Remove a measurement with a given name.
            ///
            /// Raise exception if name not found.
            ///
            /// :param str name: Name to remove. Must not contain commas.
            ///
            /// :return: Index and measurement object
            #[doc = #rtype_remove_named_meas]
            fn remove_measurement_by_name(
                &mut self,
                name: Shortname,
            ) -> PyResult<(MeasIndex, Element<#ttype, #otype>)> {
                Ok(self
                    .0
                    .remove_measurement_by_name(&name)
                    .map(|(i, x)| (i, x.inner_into()))?)
            }

            /// Remove a measurement with a given index.
            ///
            /// Raise exception if index not found.
            ///
            /// :param int index: Index to remove.
            ///
            /// :return: Name and measurement object
            #[doc = #rtype_remove_index_meas]
            fn remove_measurement_by_index(
                &mut self,
                index: MeasIndex,
            ) -> PyResult<(#nametype, Element<#ttype, #otype>)> {
                let r = self.0.remove_measurement_by_index(index)?;
                let (n, v) = Element::unzip::<#namefam>(r);
                Ok((n.0, v.inner_into()))
            }

            /// Return measurement at index.
            ///
            /// Raise exception if index not found.
            ///
            /// :param int index: Index to retrieve.
            ///
            /// :return: Measurement object.
            #[doc = #rtype_get_meas]
            fn measurement_at(&self, index: MeasIndex) -> PyResult<Element<#ttype, #otype>> {
                let ms: &NamedVec<_, _, _, _> = self.0.as_ref();
                let m = ms.get(index)?;
                Ok(m.bimap(|x| x.1.clone(), |x| x.1.clone()).inner_into())
            }

            /// Replace measurement at index with given optical measurement.
            ///
            /// Raise exception if index not found.
            ///
            /// :param int index: Index to replace.
            /// :param meas: Optical measurement to replace the measurement at ``index``.
            #[doc = #param_type_optical]
            ///
            /// :return: Replaced measurement object
            #[doc = #rtype_get_meas]
            fn replace_optical_at(
                &mut self,
                index: MeasIndex,
                meas: #otype,
            ) -> PyResult<Element<#ttype, #otype>> {
                let ret = self.0.replace_optical_at(index, meas.into())?;
                Ok(ret.inner_into())
            }

            /// Replace named measurement with given optical measurement.
            ///
            /// Raise exception if name not found.
            ///
            /// :param str name: Name to replace.
            /// :param meas: Optical measurement to replace the measurement with ``name``.
            #[doc = #param_type_optical]
            ///
            /// :return: Replaced measurement object.
            #[doc = #rtype_get_meas]
            fn replace_optical_named(
                &mut self,
                name: Shortname,
                meas: #otype,
            ) -> Option<Element<#ttype, #otype>> {
                self.0
                    .replace_optical_named(&name, meas.into())
                    .map(|r| r.inner_into())
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn convert_methods_proc(input: TokenStream) -> TokenStream {
    let pytype: Path = parse_macro_input!(input);
    let name = pytype.segments.last().unwrap().ident.to_string();
    let (base, version) = split_version(name.as_str());
    let outputs: Vec<_> = ALL_VERSIONS
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

struct CommonMeasGetSet {
    rstype: Path,
    nametype: Type,
    namefam: Type,
}

impl Parse for CommonMeasGetSet {
    fn parse(input: ParseStream) -> Result<Self> {
        let rstype: Path = input.parse()?;
        let _: Comma = input.parse()?;
        let nametype: Type = input.parse()?;
        let _: Comma = input.parse()?;
        let namefam: Type = input.parse()?;
        Ok(Self {
            rstype,
            nametype,
            namefam,
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

fn split_version(name: &str) -> (&str, &str) {
    let ret = name.split_at(name.len() - 3);
    if !ALL_VERSIONS.iter().any(|&v| v == ret.1) {
        panic!("invalid version {}", ret.1)
    }
    ret
}

const ALL_VERSIONS: [&str; 4] = ["2_0", "3_0", "3_1", "3_2"];

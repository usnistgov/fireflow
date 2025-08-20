extern crate proc_macro;

use proc_macro::TokenStream;

use itertools::Itertools;
use quote::{format_ident, quote};
use syn::{
    parenthesized,
    parse::{Parse, ParseStream},
    parse_macro_input,
    punctuated::Punctuated,
    token::Comma,
    GenericArgument, Ident, LitBool, LitStr, Path, PathArguments, Result, Token, Type,
};

#[proc_macro]
pub fn impl_new_core(input: TokenStream) -> TokenStream {
    let info = parse_macro_input!(input as NewCoreInfo);
    let coretext_rstype = info.coretext_type;
    let coredataset_rstype = info.coredataset_type;
    let fun = info.fun;
    let args = info.args;

    let coretext_name = coretext_rstype.segments.last().unwrap().ident.to_string();
    let coredataset_name = coredataset_rstype
        .segments
        .last()
        .unwrap()
        .ident
        .to_string();

    let version = split_version(&coretext_name).1.replace("_", ".");

    let coretext_pytype = format_ident!("Py{coretext_name}");
    let coredataset_pytype = format_ident!("Py{coredataset_name}");

    let coretext_summary = format!("Represents *TEXT* for an FCS {version} file.");
    let coredataset_summary =
        format!("Represents *TEXT*, *DATA*, *ANALYSIS* and *OTHER* for an FCS {version} file.");

    let meas = NewArgInfo::new(
        "measurements",
        info.meas_rstype,
        false,
        info.meas_pytype.value().as_str(),
        Some(info.meas_desc.value().as_str()),
        None,
    );

    let layout = NewArgInfo::new(
        "layout",
        info.layout_rstype,
        false,
        info.layout_pytype.value().as_str(),
        Some(info.layout_desc.value().as_str()),
        None,
    );

    let df = NewArgInfo::new(
        "df",
        info.df_type,
        false,
        "polars.DataFrame",
        Some(
            "A dataframe encoding the contents of *DATA*. Number of columns must
         match number of measurements. May be empty. Types do not necessarily
         need to correspond to those in the data layout but mismatches may
         result in truncation.",
        ),
        None,
    );

    let analysis = NewArgInfo::new(
        "analysis",
        info.analysis_type,
        true,
        "bytes",
        Some("A byte string encoding the *ANALYSIS* segment."),
        Some("b\"\""),
    );

    let others = NewArgInfo::new(
        "others",
        info.others_type,
        true,
        "list[bytes]",
        Some("Byte strings encoding the *OTHER* segments."),
        Some("[]"),
    );

    let coretext_funargs: Vec<_> = [&meas, &layout]
        .into_iter()
        .chain(args.as_slice())
        .map(|x| x.make_fun_arg())
        .collect();
    let coredataset_funargs: Vec<_> = [&meas, &layout, &df]
        .into_iter()
        .chain(args.as_slice())
        .chain([&analysis, &others])
        .map(|x| x.make_fun_arg())
        .collect();

    let coretext_inner_args: Vec<_> = [&meas, &layout]
        .into_iter()
        .chain(args.as_slice())
        .map(|x| x.make_argname())
        .collect();

    let _coretext_sig_args = [&meas, &layout]
        .into_iter()
        .chain(args.as_slice())
        .map(|x| x.make_sig())
        .join(",");
    let coretext_sig = format!("({_coretext_sig_args})");

    let _coredataset_sig_args = [&meas, &layout, &df]
        .into_iter()
        .chain(args.as_slice())
        .chain([&analysis, &others])
        .map(|x| x.make_sig())
        .join(",");
    let coredataset_sig = format!("({_coredataset_sig_args})");

    let coretext_params = [&meas, &layout]
        .into_iter()
        .chain(args.as_slice())
        .map(|x| x.fmt_arg_doc())
        .join("\n\n");
    let coredataset_params = [&meas, &layout, &df]
        .into_iter()
        .chain(args.as_slice())
        .chain([&analysis, &others])
        .map(|x| x.fmt_arg_doc())
        .join("\n\n");

    quote! {
        // TODO not dry, this is just pywrap!
        #[doc = #coretext_summary]
        ///
        #[doc = #coretext_params]
        #[pyclass(name = #coretext_name, eq)]
        #[derive(Clone, From, Into, PartialEq)]
        pub struct #coretext_pytype(#coretext_rstype);

        #[pymethods]
        impl #coretext_pytype {
            #[allow(clippy::too_many_arguments)]
            #[new]
            #[pyo3(text_signature = #coretext_sig)]
            fn new(#(#coretext_funargs),*) -> PyResult<Self> {
                Ok(#fun(#(#coretext_inner_args),*).mult_head()?.into())
            }
        }

        #[doc = #coredataset_summary]
        ///
        #[doc = #coredataset_params]
        #[pyclass(name = #coredataset_name, eq)]
        #[derive(Clone, From, Into, PartialEq)]
        pub struct #coredataset_pytype(#coredataset_rstype);

        #[pymethods]
        impl #coredataset_pytype {
            #[allow(clippy::too_many_arguments)]
            #[new]
            #[pyo3(text_signature = #coredataset_sig)]
            fn new(#(#coredataset_funargs),*) -> PyResult<Self> {
                let x = #fun(#(#coretext_inner_args),*).mult_head()?;
                Ok(x.into_coredataset(df, analysis, others)?.into())
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_new_meas(input: TokenStream) -> TokenStream {
    let info = parse_macro_input!(input as NewMeasInfo);
    let rstype = info.rstype;
    let fun = info.fun;
    let args = info.args;

    let name = rstype.segments.last().unwrap().ident.to_string();

    let (base, version) = split_version(&name);
    let pretty_version = version.replace("_", ".");
    let lower_basename = base.to_lowercase();

    let pytype = format_ident!("Py{name}");

    let summary =
        format!("Encodes FCS {pretty_version} *$Pn\\** keywords for {lower_basename} measurement.");

    let funargs: Vec<_> = args.iter().map(|x| x.make_fun_arg()).collect();

    let inner_args: Vec<_> = args.iter().map(|x| x.make_argname()).collect();

    let _sig_args = args.iter().map(|x| x.make_sig()).join(",");
    let sig = format!("({_sig_args})");

    let params = args.iter().map(|x| x.fmt_arg_doc()).join("\n\n");

    quote! {
        #[doc = #summary]
        ///
        #[doc = #params]
        #[pyclass(name = #name, eq)]
        #[derive(Clone, From, Into, PartialEq)]
        pub struct #pytype(#rstype);

        #[pymethods]
        impl #pytype {
            #[allow(clippy::too_many_arguments)]
            #[new]
            #[pyo3(text_signature = #sig)]
            fn new(#(#funargs),*) -> Self {
                #fun(#(#inner_args),*).into()
            }
        }
    }
    .into()
}

#[derive(Debug)]
struct NewMeasInfo {
    rstype: Path,
    fun: Path,
    args: Vec<NewArgInfo>,
}

impl Parse for NewMeasInfo {
    fn parse(input: ParseStream) -> Result<Self> {
        let rstype: Path = input.parse()?;
        let _: Comma = input.parse()?;
        let fun: Path = input.parse()?;
        let _: Comma = input.parse()?;
        let args: Punctuated<NewParenArg, Token![,]> = Punctuated::parse_terminated(input)?;
        Ok(Self {
            rstype,
            fun,
            args: args.into_iter().map(|x| x.0).collect(),
        })
    }
}

#[derive(Debug)]
struct NewCoreInfo {
    coretext_type: Path,
    coredataset_type: Path,
    df_type: Path,
    analysis_type: Path,
    others_type: Path,
    fun: Path,
    meas_rstype: Path,
    meas_pytype: LitStr,
    meas_desc: LitStr,
    layout_rstype: Path,
    layout_pytype: LitStr,
    layout_desc: LitStr,
    args: Vec<NewArgInfo>,
}

impl Parse for NewCoreInfo {
    fn parse(input: ParseStream) -> Result<Self> {
        let coretext_type: Path = input.parse()?;
        let _: Comma = input.parse()?;
        let coredataset_type: Path = input.parse()?;
        let _: Comma = input.parse()?;
        let df_type: Path = input.parse()?;
        let _: Comma = input.parse()?;
        let analysis_type: Path = input.parse()?;
        let _: Comma = input.parse()?;
        let others_type: Path = input.parse()?;
        let _: Comma = input.parse()?;
        let fun: Path = input.parse()?;
        let _: Comma = input.parse()?;
        let meas_rstype: Path = input.parse()?;
        let _: Comma = input.parse()?;
        let meas_pytype: LitStr = input.parse()?;
        let _: Comma = input.parse()?;
        let meas_desc: LitStr = input.parse()?;
        let _: Comma = input.parse()?;
        let layout_rstype: Path = input.parse()?;
        let _: Comma = input.parse()?;
        let layout_pytype: LitStr = input.parse()?;
        let _: Comma = input.parse()?;
        let layout_desc: LitStr = input.parse()?;
        let _: Comma = input.parse()?;
        let args: Punctuated<NewParenArg, Token![,]> = Punctuated::parse_terminated(input)?;
        Ok(Self {
            coretext_type,
            coredataset_type,
            df_type,
            analysis_type,
            others_type,
            fun,
            meas_rstype,
            meas_pytype,
            meas_desc,
            layout_rstype,
            layout_pytype,
            layout_desc,
            args: args.into_iter().map(|x| x.0).collect(),
        })
    }
}

struct NewParenArg(NewArgInfo);

impl Parse for NewParenArg {
    fn parse(input: ParseStream) -> Result<Self> {
        let content;
        parenthesized!(content in input);
        Ok(Self(content.parse()?))
    }
}

#[derive(Debug)]
struct NewArgInfo {
    argname: String,
    rstype: Path,
    isvar: bool,
    pytype: String,
    desc: Option<String>,
    default: Option<String>,
}

impl NewArgInfo {
    fn new(
        argname: &str,
        rstype: Path,
        isvar: bool,
        pytype: &str,
        desc: Option<&str>,
        default: Option<&str>,
    ) -> Self {
        Self {
            argname: argname.to_string(),
            rstype,
            isvar,
            pytype: pytype.to_string(),
            desc: desc.map(|x| x.to_string()),
            default: default.map(|x| x.to_string()),
        }
    }

    fn make_fun_arg(&self) -> proc_macro2::TokenStream {
        let argname = format_ident!("{}", &self.argname);
        let rstype = &self.rstype;
        quote! {#argname: #rstype}
    }

    fn make_argname(&self) -> proc_macro2::TokenStream {
        let n = format_ident!("{}", &self.argname);
        if unwrap_generic("Option", &self.rstype).1 {
            quote! {#n.map(|x| x.into())}
        } else {
            quote! {#n.into()}
        }
    }

    fn make_sig(&self) -> String {
        let n = &self.argname;
        let default = if let Some(default) = self.default.as_deref() {
            Some(default)
        } else {
            let rstype = &self.rstype.segments.last().unwrap().ident;
            if rstype == "Option" {
                Some("None")
            } else if rstype == "bool" {
                Some("False")
            } else if rstype == "HashMap" {
                Some("{}")
            } else {
                None
            }
        };
        default.map_or(n.to_string(), |d| format!("{n}={d}"))
    }

    fn fmt_arg_doc(&self) -> String {
        let rstype = &self.rstype.segments.last().unwrap().ident;
        let argname = &self.argname.to_string();
        let t = self.pytype.as_str();
        let pytype = if rstype == "Option" {
            format!("{t} | None")
        } else {
            t.to_string()
        };
        let desc = if let Some(d) = self.desc.as_ref() {
            d.replace("\n", "")
        } else {
            format!("Value for *${}*.", argname.to_uppercase())
        };
        let (op, ty) = if self.isvar {
            ("ivar", "vartype")
        } else {
            ("param", "type")
        };
        let pdesc = format!(":{op} {argname}: {desc}");
        let ptype = format!(":{ty} {argname}: {pytype}");
        format!("{pdesc}\n{ptype}")
    }
}

impl Parse for NewArgInfo {
    fn parse(input: ParseStream) -> Result<Self> {
        let argname = input.parse::<Ident>()?.to_string();
        let _: Comma = input.parse()?;
        let rstype: Path = input.parse()?;
        let _: Comma = input.parse()?;
        let isvar = input.parse::<LitBool>()?.value();
        let _: Comma = input.parse()?;
        let pytype = input.parse::<LitStr>()?.value();
        let desc = if input.peek(Token![,]) {
            let _: Comma = input.parse()?;
            Some(input.parse::<LitStr>()?.value())
        } else {
            None
        };
        let default = if input.peek(Token![,]) {
            let _: Comma = input.parse()?;
            Some(input.parse::<LitStr>()?.value())
        } else {
            None
        };
        Ok(Self {
            argname,
            rstype,
            isvar,
            pytype,
            desc,
            default,
        })
    }
}

#[proc_macro]
pub fn impl_get_set_metaroot(input: TokenStream) -> TokenStream {
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
pub fn impl_get_set_all_meas(input: TokenStream) -> TokenStream {
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
pub fn impl_get_set_meas_obj_common(input: TokenStream) -> TokenStream {
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
pub fn impl_convert_version(input: TokenStream) -> TokenStream {
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

#[proc_macro]
pub fn impl_meas_get_set(input: TokenStream) -> TokenStream {
    let info = parse_macro_input!(input as GetSetAllMeas);
    let kw = &info.rstype;
    let (_, optional) = unwrap_generic("Option", kw);
    let s = info.suffix.value();

    let doc_summary = format!("Value of *$Pn{}*.", s.to_uppercase());
    let doc_type = format!(
        ":type: list[{}]",
        info.pytype.value() + if optional { " | None" } else { "" },
    );
    let get = format_ident!("get_pn{}", s.to_lowercase());
    let set = format_ident!("set_pn{}", s.to_lowercase());

    let outputs: Vec<_> = info
        .parent_types
        .iter()
        .map(|t| {
            quote! {
                #[pymethods]
                impl #t {
                    #[doc = #doc_summary]
                    ///
                    #[doc = #doc_type]
                    #[getter]
                    fn #get(&self) -> #kw {
                        let x: &#kw = self.0.as_ref();
                        x.as_ref().cloned()
                    }

                    #[setter]
                    fn #set(&mut self, x: #kw) {
                        *self.0.as_mut() = x
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

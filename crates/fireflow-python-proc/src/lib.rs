extern crate proc_macro;

use proc_macro::TokenStream;

use itertools::Itertools;
use quote::{format_ident, quote, ToTokens};
use syn::{
    parenthesized,
    parse::{Parse, ParseStream},
    parse_macro_input, parse_str,
    punctuated::Punctuated,
    token::Comma,
    Expr, GenericArgument, Ident, LitBool, LitStr, Path, PathArguments, Result, Token, Type,
};

#[proc_macro]
pub fn impl_new_core(input: TokenStream) -> TokenStream {
    let info = parse_macro_input!(input as NewCoreInfo);
    let coretext_rstype = info.coretext_type;
    let coredataset_rstype = info.coredataset_type;
    let fun = info.fun;
    let args = info.args;

    let df_type = parse_str::<Path>("fireflow_core::validated::dataframe::FCSDataFrame").unwrap();
    let analysis_type = parse_str::<Path>("fireflow_core::core::Analysis").unwrap();
    let others_type = parse_str::<Path>("fireflow_core::core::Others").unwrap();

    let polars_df_type = quote! {pyo3_polars::PyDataFrame};

    let coretext_name = path_name(&coretext_rstype);
    let coredataset_name = path_name(&coredataset_rstype);

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
        df_type,
        false,
        "polars.DataFrame",
        Some(
            "A dataframe encoding the contents of *DATA*. Number of columns must \
             match number of measurements. May be empty. Types do not necessarily \
             need to correspond to those in the data layout but mismatches may \
             result in truncation.",
        ),
        None,
    );

    let analysis = NewArgInfo::new(
        "analysis",
        analysis_type.clone(),
        true,
        "bytes",
        Some("A byte string encoding the *ANALYSIS* segment."),
        Some(ArgDefault {
            rsval: quote! {#analysis_type::default()},
            pyval: "b\"\"".to_string(),
        }),
    );

    let others = NewArgInfo::new(
        "others",
        others_type.clone(),
        true,
        "list[bytes]",
        Some("Byte strings encoding the *OTHER* segments."),
        Some(ArgDefault {
            rsval: quote! {#others_type::default()},
            pyval: "[]".to_string(),
        }),
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

    let coretext_sig_args: Vec<_> = [&meas, &layout]
        .into_iter()
        .chain(args.as_slice())
        .map(|x| x.make_sig())
        .collect();

    let coredataset_sig_args: Vec<_> = [&meas, &layout, &df]
        .into_iter()
        .chain(args.as_slice())
        .chain([&analysis, &others])
        .map(|x| x.make_sig())
        .collect();

    let _coretext_txt_sig_args = [&meas, &layout]
        .into_iter()
        .chain(args.as_slice())
        .map(|x| x.make_txt_sig())
        .join(",");
    let coretext_txt_sig = format!("({_coretext_txt_sig_args})");

    let _coredataset_txt_sig_args = [&meas, &layout, &df]
        .into_iter()
        .chain(args.as_slice())
        .chain([&analysis, &others])
        .map(|x| x.make_txt_sig())
        .join(",");
    let coredataset_txt_sig = format!("({_coredataset_txt_sig_args})");

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
            #[pyo3(signature = (#(#coretext_sig_args),*))]
            #[pyo3(text_signature = #coretext_txt_sig)]
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
            #[pyo3(signature = (#(#coredataset_sig_args),*))]
            #[pyo3(text_signature = #coredataset_txt_sig)]
            fn new(#(#coredataset_funargs),*) -> PyResult<Self> {
                let x = #fun(#(#coretext_inner_args),*).mult_head()?;
                Ok(x.into_coredataset(df, analysis, others)?.into())
            }

            #[getter]
            fn data(&self) -> #polars_df_type {
                let ns = self.0.all_shortnames();
                let data = self.0.data();
                #polars_df_type(data.as_polars_dataframe(&ns[..]))
            }

            #[setter]
            fn set_data(&mut self, df: #polars_df_type) -> PyResult<()> {
                let data = df.0.try_into()?;
                Ok(self.0.set_data(data)?)
            }

            #[getter]
            fn analysis(&self) -> #analysis_type {
                self.0.analysis.clone()
            }

            #[setter]
            fn set_analysis(&mut self, xs: #analysis_type) {
                self.0.analysis = xs.into();
            }

            #[getter]
            fn others(&self) -> #others_type {
                self.0.others.clone()
            }

            #[setter]
            fn set_others(&mut self, xs: #others_type) {
                self.0.others = xs
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

    let name = path_name(&rstype);

    let (base, version) = split_version(&name);
    let pretty_version = version.replace("_", ".");
    let lower_basename = base.to_lowercase();

    let pytype = format_ident!("Py{name}");

    let summary =
        format!("Encodes FCS {pretty_version} *$Pn\\** keywords for {lower_basename} measurement.");

    let funargs: Vec<_> = args.iter().map(|x| x.make_fun_arg()).collect();

    let inner_args: Vec<_> = args.iter().map(|x| x.make_argname()).collect();

    let sig_args: Vec<_> = args.iter().map(|x| x.make_sig()).collect();

    let _txt_sig_args = args.iter().map(|x| x.make_txt_sig()).join(",");
    let txt_sig = format!("({_txt_sig_args})");

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
            #[pyo3(signature = (#(#sig_args),*), text_signature = #txt_sig)]
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
        let args: Punctuated<WrapParen<NewArgInfo>, Token![,]> =
            Punctuated::parse_terminated(input)?;
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
        let args: Punctuated<WrapParen<NewArgInfo>, Token![,]> =
            Punctuated::parse_terminated(input)?;
        Ok(Self {
            coretext_type,
            coredataset_type,
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

struct WrapParen<T>(T);

impl<T: Parse> Parse for WrapParen<T> {
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
    default: Option<ArgDefault>,
}

#[derive(Debug)]
struct ArgDefault {
    rsval: proc_macro2::TokenStream,
    pyval: String,
}

impl NewArgInfo {
    fn new(
        argname: &str,
        rstype: Path,
        isvar: bool,
        pytype: &str,
        desc: Option<&str>,
        default: Option<ArgDefault>,
    ) -> Self {
        Self {
            argname: argname.to_string(),
            rstype,
            isvar,
            pytype: pytype.to_string(),
            desc: desc.map(|x| x.to_string()),
            default,
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

    fn make_sig(&self) -> proc_macro2::TokenStream {
        let n = format_ident!("{}", &self.argname);
        let default = if let Some(default) = self.default.as_ref() {
            Some(default.rsval.to_token_stream())
        } else {
            let rstype = path_name(&self.rstype);
            if rstype == "Option" {
                Some(quote! {None})
            } else if rstype == "bool" {
                Some(quote! {false})
            } else if rstype == "HashMap" {
                Some(quote! {HashMap::new()})
            } else {
                None
            }
        };
        default.map_or(quote! {#n}, |d| quote! {#n=#d})
    }

    fn make_txt_sig(&self) -> String {
        let n = &self.argname;
        let default = if let Some(default) = self.default.as_ref() {
            Some(default.pyval.as_str())
        } else {
            let rstype = path_name(&self.rstype);
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
        let rstype = path_name(&self.rstype);
        let argname = &self.argname.to_string();
        let t = self.pytype.as_str();
        let pytype = if rstype == "Option" {
            format!("{t} | None")
        } else {
            t.to_string()
        };
        let desc = if let Some(d) = self.desc.as_ref() {
            d.to_string()
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
            Some(input.parse::<WrapParen<ArgDefault>>()?.0)
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

impl Parse for ArgDefault {
    fn parse(input: ParseStream) -> Result<Self> {
        let rsval = input.parse::<Expr>()?.to_token_stream();
        let _: Comma = input.parse()?;
        let pyval = input.parse::<LitStr>()?.value();
        Ok(Self { rsval, pyval })
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
        .unwrap_or(path_name(kw_inner));

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
    let coretext_type = &info.coretext_type;
    let coredataset_type = &info.coredataset_type;
    let nametype = &info.nametype;
    let namefam = &info.namefam;

    let coretext_name = path_name(coretext_type);
    let coredataset_name = path_name(coredataset_type);
    let (_, version) = split_version(coretext_name.as_str());

    if split_version(coredataset_name.as_str()).1 != version {
        panic!("versions do not match");
    }

    let otype = format_ident!("PyOptical{version}");
    let ttype = format_ident!("PyTemporal{version}");
    let potype = format!("Optical{version}");
    let pttype = format!("Temporal{version}");
    let poclass = format!(":py:class:`{potype}`");
    let ptclass = format!(":py:class:`{pttype}`");

    let pclasses = format!("{poclass} | {ptclass}");

    let param_type_opt = format!(":type meas: {poclass}");
    let param_type_tmp = format!(":type meas: {ptclass}");

    let rtype_get_temp = format!(":rtype: (int, str, {ptclass}`) | None");
    let rtype_all_meas = format!(":rtype: list[{pclasses}]");
    let rtype_remove_named_meas = format!(":rtype: (int, {pclasses})");
    let rtype_remove_index_meas = format!(":rtype: (str, {pclasses})");
    let rtype_get_meas = format!(":rtype: {pclasses}");

    let rtype_replace_tmp_named = format!("{rtype_get_meas} | None");

    let param_name = ":param str name: Name of measurement. Corresponds to *$PnN*. \
                      Must not contain commas.";
    let param_index = ":param int index: Position in measurement vector.";
    let param_range = ":param float range: Range of measurement. Corresponds to *$PnR*.";
    let param_notrunc = ":param bool notrunc: If ``False``, raise exception if \
                         ``range`` must be truncated to fit into measurement type.";
    let param_col = ":param col: Data for measurement. Must be same length as existing columns.\n\
                     :type col: :py:class:`polars.Series`";

    let push_meas_doc = |what: &str, param_type: &str, hasdata: bool| {
        let _param_col = if hasdata {
            format!("{param_col}\n")
        } else {
            "".to_string()
        };
        format!(
            "Push {what} measurement to the end of the measurement vector.\n\
             \n\
             :param meas: The measurement to push.\n\
             {param_type}\n\
             {_param_col}\n\
             {param_name}\n\
             {param_range}\n\
             {param_notrunc}"
        )
    };

    let insert_meas_doc = |what: &str, param_type: &str, hasdata: bool| {
        let _param_col = if hasdata {
            format!("{param_col}\n")
        } else {
            "".to_string()
        };
        format!(
            "Insert {what} measurement at position in measurement vector.\n\
             \n\
             {param_index}\n\
             :param meas: The measurement to insert.\n\
             {param_type}\n\
             {_param_col}\n\
             {param_name}\n\
             {param_range}\n\
             {param_notrunc}"
        )
    };

    let push_opt_doc = push_meas_doc("optical", param_type_opt.as_str(), false);
    let insert_opt_doc = insert_meas_doc("optical", param_type_opt.as_str(), false);
    let push_tmp_doc = push_meas_doc("temporal", param_type_tmp.as_str(), false);
    let insert_tmp_doc = insert_meas_doc("temporal", param_type_tmp.as_str(), false);
    let push_opt_data_doc = push_meas_doc("optical", param_type_opt.as_str(), true);
    let insert_opt_data_doc = insert_meas_doc("optical", param_type_opt.as_str(), true);
    let push_tmp_data_doc = push_meas_doc("temporal", param_type_tmp.as_str(), true);
    let insert_tmp_data_doc = insert_meas_doc("temporal", param_type_tmp.as_str(), true);

    // the temporal replacement functions for 3.2 are different because they
    // can fail if $PnTYPE is set
    let (replace_tmp_sig, replace_tmp_args, replace_tmp_at_body, replace_tmp_named_body) =
        if version == "3_2" {
            let go = |fun, x| {
                quote! {self
                .0
                .#fun(#x, meas.into(), force)
                .py_term_resolve()?}
            };
            (
                quote! {force = true},
                quote! {force: bool},
                go(quote! {replace_temporal_at_lossy}, quote! {index}),
                go(quote! {replace_temporal_named_lossy}, quote! {&name}),
            )
        } else {
            (
                quote! {},
                quote! {},
                quote! {self.0.replace_temporal_at(index, meas.into())?},
                quote! {self.0.replace_temporal_named(&name, meas.into())},
            )
        };

    let both = quote! {
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
        #[doc = #param_name]
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
        #[doc = #param_type_opt]
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
        #[doc = #param_name]
        /// :param meas: Optical measurement to replace the measurement with ``name``.
        #[doc = #param_type_opt]
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

        /// Replace measurement at index with temporal measurement.
        ///
        /// Raise exception if index is output of bounds or there is already
        /// a temporal measurement at a different index.
        ///
        /// :param int index: Index to be replaced.
        /// :param meas: Temporal measurement with which to replace.
        #[doc = #param_type_tmp]
        ///
        /// :return: Replaced measurement object.
        #[doc = #rtype_get_meas]
        #[pyo3(signature = (index, meas, #replace_tmp_sig))]
        fn replace_temporal_at(
            &mut self,
            index: MeasIndex,
            meas: #ttype,
            #replace_tmp_args
        ) -> PyResult<Element<#ttype, #otype>> {
            let ret = #replace_tmp_at_body;
            Ok(ret.inner_into())
        }

        /// Replace measurement with name with temporal measurement.
        ///
        /// Raise exception if name is not found or there is already
        /// a temporal measurement at a different index.
        ///
        #[doc = #param_name]
        /// :param meas: Temporal measurement with which to replace.
        #[doc = #param_type_tmp]
        ///
        /// :return: Replaced measurement object.
        #[doc = #rtype_replace_tmp_named]
        #[pyo3(signature = (name, meas, #replace_tmp_sig))]
        fn replace_temporal_named(
            &mut self,
            name: Shortname,
            meas: #ttype,
            #replace_tmp_args
        ) -> PyResult<Option<Element<#ttype, #otype>>> {
            let ret = #replace_tmp_named_body;
            Ok(ret.map(|r| r.inner_into()))
        }
    };

    let coretext_only = quote! {
        #[doc = #push_opt_doc]
        #[pyo3(signature = (meas, name, range, notrunc = false))]
        fn push_optical(
            &mut self,
            meas: #otype,
            name: #nametype,
            range: kws::Range,
            notrunc: bool,
        ) -> PyResult<()> {
            self.0
                .push_optical(name.into(), meas.into(), range, notrunc)
                .py_term_resolve()
                .void()
        }

        #[doc = #insert_opt_doc]
        #[pyo3(signature = (index, meas, name, range, notrunc = false))]
        fn insert_optical(
            &mut self,
            index: MeasIndex,
            meas: #otype,
            name: #nametype,
            range: kws::Range,
            notrunc: bool,
        ) -> PyResult<()> {
            self.0
                .insert_optical(index, name.into(), meas.into(), range, notrunc)
                .py_term_resolve()
                .void()
        }

        #[doc = #push_tmp_doc]
        #[pyo3(signature = (meas, name, range, notrunc = false))]
        fn push_temporal(
            &mut self,
            meas: #ttype,
            name: Shortname,
            range: kws::Range,
            notrunc: bool,
        ) -> PyResult<()> {
            self.0
                .push_temporal(name, meas.into(), range, notrunc)
                .py_term_resolve()
        }

        #[doc = #insert_tmp_doc]
        #[pyo3(signature = (index, meas, name, range, notrunc = false))]
        fn insert_temporal(
            &mut self,
            index: MeasIndex,
            meas: #ttype,
            name: Shortname,
            range: kws::Range,
            notrunc: bool,
        ) -> PyResult<()> {
            self.0
                .insert_temporal(index, name, meas.into(), range, notrunc)
                .py_term_resolve()
        }

        /// Remove measurements and clear the layout.
        ///
        /// This is equivalent to deleting all *$Pn\** keywords and setting
        /// *$PAR* to 0.
        ///
        /// Will raise exception if other keywords (such as *$TR*) reference
        /// a measurement.
        fn unset_measurements(&mut self) -> PyResult<()> {
            Ok(self.0.unset_measurements()?)
        }
    };

    let coredataset_only = quote! {
        #[doc = #push_opt_data_doc]
        #[pyo3(signature = (meas, col, name, range, notrunc = false))]
        fn push_optical(
            &mut self,
            meas: #otype,
            col: AnyFCSColumn,
            name: #nametype,
            range: kws::Range,
            notrunc: bool,
        ) -> PyResult<()> {
            self.0
                .push_optical(name.into(), meas.into(), col, range, notrunc)
                .py_term_resolve()
                .void()
        }

        #[doc = #insert_opt_data_doc]
        #[pyo3(signature = (index, meas, col, name, range, notrunc = false))]
        fn insert_optical(
            &mut self,
            index: MeasIndex,
            meas: #otype,
            col: AnyFCSColumn,
            name: #nametype,
            range: kws::Range,
            notrunc: bool,
        ) -> PyResult<()> {
            self.0
                .insert_optical(index, name.into(), meas.into(), col, range, notrunc)
                .py_term_resolve()
                .void()
        }

        #[doc = #push_tmp_data_doc]
        #[pyo3(signature = (meas, col, name, range, notrunc = false))]
        fn push_temporal(
            &mut self,
            meas: #ttype,
            col: AnyFCSColumn,
            name: Shortname,
            range: kws::Range,
            notrunc: bool,
        ) -> PyResult<()> {
            self.0
                .push_temporal(name, meas.into(), col, range, notrunc)
                .py_term_resolve()
        }

        #[doc = #insert_tmp_data_doc]
        #[pyo3(signature = (index, meas, col, name, range, notrunc = false))]
        fn insert_temporal(
            &mut self,
            index: MeasIndex,
            meas: #ttype,
            col: AnyFCSColumn,
            name: Shortname,
            range: kws::Range,
            notrunc: bool,
        ) -> PyResult<()> {
            self.0
                .insert_temporal(index, name, meas.into(), col, range, notrunc)
                .py_term_resolve()
        }

        /// Remove all measurements and their data.
        ///
        /// Raise exception if any keywords (such as *$TR*) reference a
        /// measurement.
        fn unset_data(&mut self) -> PyResult<()> {
            Ok(self.0.unset_data()?)
        }
    };

    quote! {
        #[pymethods]
        impl #coretext_type {
            #both
            #coretext_only
        }

        #[pymethods]
        impl #coredataset_type {
            #both
            #coredataset_only
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_convert_version(input: TokenStream) -> TokenStream {
    let pytype: Path = parse_macro_input!(input);
    let name = path_name(&pytype);
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
    name_override: Option<LitStr>,
    parent_types: Punctuated<Type, Token![,]>,
}

impl Parse for GetSetMetarootInfo {
    fn parse(input: ParseStream) -> Result<Self> {
        let keyword: Path = input.parse()?;
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
    coretext_type: Path,
    coredataset_type: Path,
    nametype: Type,
    namefam: Type,
}

impl Parse for CommonMeasGetSet {
    fn parse(input: ParseStream) -> Result<Self> {
        let coretext_type = input.parse::<Path>()?;
        let _: Comma = input.parse()?;
        let coredataset_type = input.parse::<Path>()?;
        let _: Comma = input.parse()?;
        let nametype: Type = input.parse()?;
        let _: Comma = input.parse()?;
        let namefam: Type = input.parse()?;
        Ok(Self {
            coretext_type,
            coredataset_type,
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

fn path_name(p: &Path) -> String {
    p.segments.last().unwrap().ident.to_string()
}

const ALL_VERSIONS: [&str; 4] = ["2_0", "3_0", "3_1", "3_2"];

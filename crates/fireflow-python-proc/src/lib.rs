extern crate proc_macro;

mod docstring;

use crate::docstring::{ArgType, DocArg, DocReturn, DocString, PyType};

use fireflow_core::header::Version;

use proc_macro::TokenStream;

use itertools::Itertools;
use nonempty::{nonempty, NonEmpty};
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
    let meas_rstype = &info.meas_rstype;
    let layout_rstype = &info.layout_rstype;
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

    let meas_pytype = PyType::Raw(info.meas_pytype.value());
    let layout_pytype = PyType::Raw(info.layout_pytype.value());
    let df_pytype = PyType::PyClass("polars.DataFrame".into());

    let meas = NewArgInfo::new(
        "measurements",
        meas_rstype.clone(),
        false,
        &meas_pytype,
        Some(info.meas_desc.value().as_str()),
        None,
    );

    let layout = NewArgInfo::new(
        "layout",
        layout_rstype.clone(),
        false,
        &layout_pytype,
        Some(info.layout_desc.value().as_str()),
        None,
    );

    let df = NewArgInfo::new(
        "df",
        df_type.clone(),
        false,
        &df_pytype,
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
        &PyType::Bytes,
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
        &PyType::new_list(PyType::Bytes),
        Some("Byte strings encoding the *OTHER* segments."),
        Some(ArgDefault {
            rsval: quote! {#others_type::default()},
            pyval: "[]".to_string(),
        }),
    );

    let coretext_args: Vec<_> = [&meas, &layout]
        .into_iter()
        .chain(args.as_slice())
        .collect();
    let coredataset_args: Vec<_> = [&meas, &layout, &df]
        .into_iter()
        .chain(args.as_slice())
        .chain([&analysis, &others])
        .collect();

    let coretext_funargs: Vec<_> = coretext_args.iter().map(|x| x.make_fun_arg()).collect();
    let coredataset_funargs: Vec<_> = coredataset_args.iter().map(|x| x.make_fun_arg()).collect();

    let coretext_inner_args: Vec<_> = coretext_args.iter().map(|x| x.make_argname()).collect();

    let coretext_sig_args: Vec<_> = coretext_args.iter().map(|x| x.make_sig()).collect();
    let coredataset_sig_args: Vec<_> = coredataset_args.iter().map(|x| x.make_sig()).collect();

    let _coretext_txt_sig_args = coretext_args.iter().map(|x| x.make_txt_sig()).join(",");
    let coretext_txt_sig = format!("({_coretext_txt_sig_args})");

    let _coredataset_txt_sig_args = coredataset_args.iter().map(|x| x.make_txt_sig()).join(",");
    let coredataset_txt_sig = format!("({_coredataset_txt_sig_args})");

    let coretext_params: Vec<_> = coretext_args.iter().map(|x| x.fmt_arg_doc()).collect();
    let coredataset_params: Vec<_> = coredataset_args.iter().map(|x| x.fmt_arg_doc()).collect();

    let coretext_doc = DocString::new(
        format!("Represents *TEXT* for an FCS {version} file."),
        vec![],
        coretext_params,
        None,
    );

    let coredataset_doc = DocString::new(
        format!("Represents one dataset in an FCS {version} file."),
        vec![],
        coredataset_params,
        None,
    );

    let param_type_set_meas = DocArg::new_param(
        "measurements".into(),
        meas_pytype,
        "The new measurements.".into(),
    );

    let param_type_set_layout =
        DocArg::new_param("layout".into(), layout_pytype, "The new layout.".into());

    let param_type_set_df = DocArg::new_param("df".to_string(), df_pytype, "The new data.".into());

    let param_allow_shared_names = DocArg::new_param(
        "allow_shared_named".into(),
        PyType::Bool,
        "If ``False``, raise exception if any non-measurement keywords reference \
         any *$PnN* keywords. If ``True`` raise exception if any non-measurement \
         keywords reference a *$PnN* which is not present in ``measurements``. \
         In other words, ``False`` forbids named references to exist, and \
         ``True`` allows named references to be updated. References cannot \
         be broken in either case."
            .into(),
    );

    // TODO this can be specific to each version, for instance, we can call out
    // the exact keywords in each that may have references.
    let param_skip_index_check = DocArg::new_param(
        "skip_index_check".into(),
        PyType::Bool,
        "If ``False``, raise exception if any non-measurement keyword have an \
         index reference to the current measurements. If ``True`` allow such \
         references to exist as long as they do not break (which really means \
         that the length of ``measurements`` is such that existing indices are \
         satisfied)."
            .into(),
    );

    let make_set_meas_doc = |is_dataset: bool| {
        let s = if is_dataset {
            "layout and dataframe"
        } else {
            "layout"
        };
        let ps = vec![format!(
            "Length of ``measurements`` must match number of columns in existing {s}."
        )];
        DocString::new(
            "Set all measurements at once.".into(),
            ps,
            vec![
                param_type_set_meas.clone(),
                param_allow_shared_names.clone(),
                param_skip_index_check.clone(),
            ],
            None,
        )
    };

    let make_set_meas_and_layout_doc = |is_dataset: bool| {
        let s = if is_dataset {
            " and both must match number of columns in existing dataframe"
        } else {
            ""
        };
        let ps = vec![
            "This is equivalent to updating all *$PnN* keywords at once.".into(),
            format!("Length of ``measurements`` must match number of columns in ``layout`` {s}."),
        ];
        DocString::new(
            "Set all measurements at once.".into(),
            ps,
            vec![
                param_type_set_meas.clone(),
                param_type_set_layout.clone(),
                param_allow_shared_names.clone(),
                param_skip_index_check.clone(),
            ],
            None,
        )
    };

    let coretext_set_meas_doc = make_set_meas_doc(false);
    let coretext_set_meas_and_layout_doc = make_set_meas_and_layout_doc(false);
    let coredataset_set_meas_doc = make_set_meas_doc(true);
    let coredataset_set_meas_and_layout_doc = make_set_meas_and_layout_doc(true);

    let set_meas_and_data_doc = DocString::new(
        "Set measurements and data at once.".into(),
        vec!["Length of ``measurements`` must match number of columns in ``df``.".into()],
        vec![
            param_type_set_meas,
            param_type_set_df,
            param_allow_shared_names,
            param_skip_index_check,
        ],
        None,
    );

    // TODO these might be better in a different macro that deals with the
    // non-setters.
    let set_meas_method = quote! {
        pub fn set_measurements(
            &mut self,
            measurements: #meas_rstype,
            allow_shared_names: bool,
            skip_index_check: bool,
        ) -> PyResult<()> {
            self.0
                .set_measurements(
                    measurements.0.inner_into(),
                    allow_shared_names,
                    skip_index_check,
                )
                .py_term_resolve_nowarn()
        }
    };

    let set_meas_and_layout_method = quote! {
        fn set_measurements_and_layout(
            &mut self,
            measurements: #meas_rstype,
            layout: #layout_rstype,
            allow_shared_names: bool,
            skip_index_check: bool,
        ) -> PyResult<()> {
            self.0
                .set_measurements_and_layout(
                    measurements.0.inner_into(),
                    layout.into(),
                    allow_shared_names,
                    skip_index_check,
                )
                .py_term_resolve_nowarn()
        }
    };

    let common = quote! {
        #[getter]
        fn get_layout(&self) -> #layout_rstype {
            self.0.layout().clone().into()
        }

        #[setter]
        fn set_layout(&mut self, layout: #layout_rstype) -> PyResult<()> {
            self.0.set_layout(layout.into()).py_term_resolve_nowarn()
        }
    };

    quote! {
        // TODO not dry, this is just pywrap!
        #coretext_doc
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

            #coretext_set_meas_doc
            #set_meas_method

            #coretext_set_meas_and_layout_doc
            #set_meas_and_layout_method

            #common
        }

        #coredataset_doc
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

            #coredataset_set_meas_doc
            #set_meas_method

            #coredataset_set_meas_and_layout_doc
            #set_meas_and_layout_method

            #set_meas_and_data_doc
            fn set_measurements_and_data(
                &mut self,
                measurements: #meas_rstype,
                df: #df_type,
                allow_shared_names: bool,
                skip_index_check: bool,
            ) -> PyResult<()> {
                self.0
                    .set_measurements_and_data(
                        measurements.0.inner_into(),
                        df,
                        allow_shared_names,
                        skip_index_check,
                    )
                    .py_term_resolve_nowarn()
            }

            #common
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_new_meas(input: TokenStream) -> TokenStream {
    let info = parse_macro_input!(input as NewMeasInfo);
    let args = info.args;

    let version = ugly_version(info.version);
    let base = if info.is_temporal {
        "Temporal"
    } else {
        "Optical"
    };

    let name = format!("{base}{version}");
    let path = format!("fireflow_core::core::{name}");
    let fun_path = format!("{path}::new_{version}");
    let rstype = parse_str::<Path>(path.as_str()).unwrap();
    let fun = parse_str::<Path>(fun_path.as_str()).unwrap();

    let (base, version) = split_version(&name);
    let pretty_version = version.replace("_", ".");
    let lower_basename = base.to_lowercase();

    let ln_path = "Option<fireflow_core::text::keywords::Longname>";
    let nonstd_path = "HashMap<fireflow_core::validated::keys::NonStdKey, String>";

    let ln_type = parse_str::<Path>(ln_path).unwrap();
    let nonstd_type = parse_str::<Path>(nonstd_path).unwrap();

    let longname = NewArgInfo::new(
        "longname",
        ln_type,
        true,
        &PyType::Str,
        Some("Value for *$PnS*."),
        None,
    );

    let nonstd = NewArgInfo::new(
        "nonstandard_keywords",
        nonstd_type,
        true,
        &PyType::Raw("dict[str, str]".into()),
        Some(
            "Any non-standard keywords corresponding to this measurement. No keys \
             should start with *$*. Realistically each key should follow a pattern \
             corresponding to the measurement index, something like prefixing with \
             \"P\" followed by the index. This is not enforced.",
        ),
        None,
    );

    let all_args: Vec<_> = args.iter().chain([&longname, &nonstd]).collect();

    let pytype = format_ident!("Py{name}");

    let funargs: Vec<_> = all_args.iter().map(|x| x.make_fun_arg()).collect();

    let inner_args: Vec<_> = all_args.iter().map(|x| x.make_argname()).collect();

    let sig_args: Vec<_> = all_args.iter().map(|x| x.make_sig()).collect();

    let _txt_sig_args = all_args.iter().map(|x| x.make_txt_sig()).join(",");
    let txt_sig = format!("({_txt_sig_args})");

    let params: Vec<_> = all_args.iter().map(|x| x.fmt_arg_doc()).collect();

    let doc = DocString::new(
        format!("FCS {pretty_version} *$Pn\\** keywords for {lower_basename} measurement."),
        vec![],
        params,
        None,
    );

    let get_set_timestep = if pretty_version != "2.0" && info.is_temporal {
        let t = quote! {fireflow_core::text::keywords::Timestep};
        quote! {
            #[getter]
            fn get_timestep(&self) -> #t {
                self.0.specific.timestep
            }

            #[setter]
            fn set_timestep(&mut self, timestep: #t) {
                self.0.specific.timestep = timestep
            }
        }
    } else {
        quote! {}
    };

    let get_set_scale = if info.is_temporal {
        quote! {}
    } else if pretty_version == "2.0" {
        let t = quote! {fireflow_core::text::scale::Scale};
        quote! {
            #[getter]
            fn get_scale(&self) -> Option<#t> {
                self.0.specific.scale.0.as_ref().map(|&x| x)
            }

            #[setter]
            fn set_scale(&mut self, x: Option<#t>) {
                self.0.specific.scale = x.into()
            }
        }
    } else {
        let t = quote! {fireflow_core::core::ScaleTransform};
        quote! {
            #[getter]
            fn get_transform(&self) -> #t {
                self.0.specific.scale
            }

            #[setter]
            fn set_transform(&mut self, transform: #t) {
                self.0.specific.scale = transform;
            }
        }
    };

    let nk = quote! {fireflow_core::validated::keys::NonStdKey};

    quote! {
        #doc
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

            #get_set_timestep
            #get_set_scale

            #[getter]
            fn nonstandard_keywords(&self) -> HashMap<#nk, String> {
                self.0.common.nonstandard_keywords.clone()
            }

            #[setter]
            fn set_nonstandard_keywords(&mut self, keyvals: HashMap<#nk, String>) {
                self.0.common.nonstandard_keywords = keyvals;
            }
        }
    }
    .into()
}

struct NewMeasInfo {
    version: Version,
    is_temporal: bool,
    args: Vec<NewArgInfo>,
}

impl Parse for NewMeasInfo {
    fn parse(input: ParseStream) -> Result<Self> {
        let v = input.parse::<LitStr>()?.value();
        let version = v.parse::<Version>().unwrap();
        let _: Comma = input.parse()?;
        let is_temporal = input.parse::<LitBool>()?.value();
        let _: Comma = input.parse()?;
        let args: Punctuated<WrapParen<NewArgInfo>, Token![,]> =
            Punctuated::parse_terminated(input)?;
        Ok(Self {
            version,
            is_temporal,
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
        pytype: &PyType,
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

    fn fmt_arg_doc(&self) -> DocArg {
        let rstype = path_name(&self.rstype);
        let argname = self.argname.to_string();
        let t = self.pytype.as_str();
        let pytype = if rstype == "Option" {
            PyType::Raw(format!("{t} | None"))
        } else {
            PyType::Raw(t.to_string())
        };
        let desc = if let Some(d) = self.desc.as_ref() {
            d.to_string()
        } else {
            format!("Value for *${}*.", argname.to_uppercase())
        };
        let at = if self.isvar {
            ArgType::Ivar
        } else {
            ArgType::Param
        };
        DocArg::new(at, argname, pytype, desc)
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

    let kw_doc = format!("*$Pn{}*", s.to_uppercase());

    let doc_summary = format!("Value of {kw_doc} for all measurements.");
    let doc_middle = if optical_only {
        vec![format!(
            "``()`` will be returned for time since {kw_doc} is not \
             defined for temporal measurements."
        )]
    } else {
        vec![]
    };
    let base_pytype = PyType::Raw(info.pytype.value());
    let tmp_pytype = if optical_only {
        Some(PyType::new_unit())
    } else {
        None
    };
    let none_pytype = if optional { Some(PyType::None) } else { None }.into_iter();

    let doc_type = PyType::new_list(PyType::new_union(NonEmpty::from((
        base_pytype,
        tmp_pytype.into_iter().chain(none_pytype).collect(),
    ))));
    let doc = DocString::new(
        doc_summary,
        doc_middle,
        vec![],
        Some(DocReturn::new(doc_type, None)),
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
                    #doc
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
    let opt_pytype = PyType::PyClass(format!("Optical{version}"));
    let tmp_pytype = PyType::PyClass(format!("Temporal{version}"));

    let meas_pytype = PyType::new_union2(opt_pytype.clone(), tmp_pytype.clone());

    let make_param_name = |short_desc: &str| {
        DocArg::new_param(
            "name".into(),
            PyType::Str,
            format!("{short_desc}. Corresponds to *$PnN*. Must not contain commas."),
        )
    };

    let make_param_index = |desc: &str| DocArg::new_param("index".into(), PyType::Int, desc.into());

    let make_return_meas = |desc: String| DocReturn::new(meas_pytype.clone(), Some(desc));

    let param_range = DocArg::new_param(
        "range".into(),
        PyType::Float,
        "Range of measurement. Corresponds to *$PnR*.".into(),
    );
    let param_notrunc = DocArg::new_param(
        "notrunc".into(),
        PyType::Bool,
        "If ``False``, raise exception if ``range`` must be truncated to fit \
         into measurement type."
            .into(),
    );
    let param_col = DocArg::new_param(
        "col".into(),
        PyType::PyClass("polars.Series".into()),
        "Data for measurement. Must be same length as existing columns.".into(),
    );

    let push_meas_doc = |is_optical: bool, meas_type: &PyType, hasdata: bool| {
        let what = if is_optical { "optical" } else { "temporal" };
        let param_meas = DocArg::new_param(
            "measurement".into(),
            meas_type.clone(),
            "The measurement to push.".into(),
        );
        let _param_col = if hasdata {
            Some(param_col.clone())
        } else {
            None
        };
        let ps: Vec<_> = [param_meas]
            .into_iter()
            .chain(_param_col)
            .chain([
                make_param_name("Name of new measurement."),
                param_range.clone(),
                param_notrunc.clone(),
            ])
            .collect();
        let summary = format!("Push {what} measurement to end of measurement vector.");
        DocString::new(summary, vec![], ps, None)
    };

    let insert_meas_doc = |is_optical: bool, meas_type: &PyType, hasdata: bool| {
        let what = if is_optical { "optical" } else { "temporal" };
        let param_meas = DocArg::new_param(
            "measurement".into(),
            meas_type.clone(),
            "The measurement to insert.".into(),
        );
        let _param_col = if hasdata {
            Some(param_col.clone())
        } else {
            None
        };
        let summary = format!("Insert {what} measurement at position in measurement vector.");
        let ps: Vec<_> = [
            make_param_index("Position at which to insert new measurement."),
            param_meas,
        ]
        .into_iter()
        .chain(_param_col)
        .chain([
            make_param_name("Name of new measurement."),
            param_range.clone(),
            param_notrunc.clone(),
        ])
        .collect();
        DocString::new(summary, vec![], ps, None)
    };

    let push_opt_doc = push_meas_doc(true, &opt_pytype, false);
    let insert_opt_doc = insert_meas_doc(true, &opt_pytype, false);
    let push_tmp_doc = push_meas_doc(false, &tmp_pytype, false);
    let insert_tmp_doc = insert_meas_doc(false, &tmp_pytype, false);
    let push_opt_data_doc = push_meas_doc(true, &opt_pytype, true);
    let insert_opt_data_doc = insert_meas_doc(true, &opt_pytype, true);
    let push_tmp_data_doc = push_meas_doc(false, &tmp_pytype, true);
    let insert_tmp_data_doc = insert_meas_doc(false, &tmp_pytype, true);

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

    let get_tmp_doc = DocString::new(
        "Get the temporal measurement if it exists.".into(),
        vec![],
        vec![],
        Some(DocReturn::new(
            PyType::new_opt(PyType::Tuple(vec![
                PyType::Int,
                PyType::Str,
                tmp_pytype.clone(),
            ])),
            Some("Index, name, and measurement or ``None``".into()),
        )),
    );

    let get_all_meas_doc = DocString::new(
        "Get all measurements.".into(),
        vec![],
        vec![],
        Some(DocReturn::new(
            PyType::new_list(meas_pytype.clone()),
            Some("List of measurements".into()),
        )),
    );

    let remove_meas_by_name_doc = DocString::new(
        "Remove a measurement with a given name.".into(),
        vec!["Raise exception if ``name`` not found.".into()],
        vec![make_param_name("Name to remove")],
        Some(DocReturn::new(
            PyType::Tuple(vec![PyType::Int, meas_pytype.clone()]),
            Some("Index and measurement object".into()),
        )),
    );

    let remove_meas_by_index_doc = DocString::new(
        "Remove a measurement with a given index.".into(),
        vec!["Raise exception if ``index`` not found.".into()],
        vec![make_param_index("Index to remove")],
        Some(DocReturn::new(
            PyType::Tuple(vec![PyType::Str, meas_pytype.clone()]),
            Some("Name and measurement object".into()),
        )),
    );

    let meas_at_doc = DocString::new(
        "Return measurement at index".into(),
        vec!["Raise exception if ``index`` not found.".into()],
        vec![make_param_index("Index to retrieve.")],
        Some(make_return_meas("Measurement object".into())),
    );

    let make_replace_doc = |is_optical: bool, is_index: bool| {
        let (i, i_param, m) = if is_index {
            (
                "index",
                make_param_index("Index to replace."),
                "measurement at index",
            )
        } else {
            (
                "name",
                make_param_name("Name to replace."),
                "named measurement",
            )
        };
        let (s, ss, t, other_pos) = if is_optical {
            ("optical", "Optical", opt_pytype.clone(), "")
        } else {
            (
                "temporal",
                "Temporal",
                tmp_pytype.clone(),
                " or there is already a temporal measurement in a different position",
            )
        };
        let meas_desc = format!("{ss} measurement to replace measurement at ``{i}``.");
        let sub = format!("Raise exception if ``{i}`` does not exist{other_pos}.");
        DocString::new(
            format!("Replace {m} with given {s} measurement."),
            vec![sub],
            vec![i_param, DocArg::new_param("meas".into(), t, meas_desc)],
            Some(make_return_meas("Replaced measurement object".into())),
        )
    };

    let replace_opt_at_doc = make_replace_doc(true, true);
    let replace_named_opt_doc = make_replace_doc(true, false);
    let replace_tmp_at_doc = make_replace_doc(false, true);
    let replace_named_tmp_doc = make_replace_doc(false, false);

    let unset_meas_doc = DocString::new(
        "Remove measurements and clear the layout.".into(),
        vec![
            "This is equivalent to deleting all *$Pn\\** keywords and setting *$PAR* to 0.".into(),
            "Will raise exception if other keywords (such as *$TR*) reference a measurement."
                .into(),
        ],
        vec![],
        None,
    );

    let unset_data_doc = DocString::new(
        "Remove all measurements and their data.".into(),
        vec!["Raise exception if any keywords (such as *$TR*) reference a measurement.".into()],
        vec![],
        None,
    );

    let both = quote! {
        #get_tmp_doc
        #[getter]
        fn get_temporal(&self) -> Option<(MeasIndex, Shortname, #ttype)> {
            self.0
                .temporal()
                .map(|t| (t.index, t.key.clone(), t.value.clone().into()))
        }

        #get_all_meas_doc
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

        #remove_meas_by_name_doc
        fn remove_measurement_by_name(
            &mut self,
            name: Shortname,
        ) -> PyResult<(MeasIndex, Element<#ttype, #otype>)> {
            Ok(self
               .0
               .remove_measurement_by_name(&name)
               .map(|(i, x)| (i, x.inner_into()))?)
        }

        #remove_meas_by_index_doc
        fn remove_measurement_by_index(
            &mut self,
            index: MeasIndex,
        ) -> PyResult<(#nametype, Element<#ttype, #otype>)> {
            let r = self.0.remove_measurement_by_index(index)?;
            let (n, v) = Element::unzip::<#namefam>(r);
            Ok((n.0, v.inner_into()))
        }

        // TODO this should return name as well
        #meas_at_doc
        fn measurement_at(&self, index: MeasIndex) -> PyResult<Element<#ttype, #otype>> {
            let ms: &NamedVec<_, _, _, _> = self.0.as_ref();
            let m = ms.get(index)?;
            Ok(m.bimap(|x| x.1.clone(), |x| x.1.clone()).inner_into())
        }

        // TODO return measurement with name

        #replace_opt_at_doc
        fn replace_optical_at(
            &mut self,
            index: MeasIndex,
            meas: #otype,
        ) -> PyResult<Element<#ttype, #otype>> {
            let ret = self.0.replace_optical_at(index, meas.into())?;
            Ok(ret.inner_into())
        }

        #replace_named_opt_doc
        fn replace_optical_named(
            &mut self,
            name: Shortname,
            meas: #otype,
        ) -> Option<Element<#ttype, #otype>> {
            self.0
                .replace_optical_named(&name, meas.into())
                .map(|r| r.inner_into())
        }

        #replace_tmp_at_doc
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

        #replace_named_tmp_doc
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
        #push_opt_doc
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

        #insert_opt_doc
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

        #push_tmp_doc
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

        #insert_tmp_doc
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

        #unset_meas_doc
        fn unset_measurements(&mut self) -> PyResult<()> {
            Ok(self.0.unset_measurements()?)
        }
    };

    let coredataset_only = quote! {
        #push_opt_data_doc
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

        #insert_opt_data_doc
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

        #push_tmp_data_doc
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

        #insert_tmp_data_doc
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

        #unset_data_doc
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
    let sub = "Will raise an exception if target version requires data which is \
               not present in ``self``.";
    let param_desc = "If ``False``, do not proceed with conversion if it would \
                      result in data loss. This is most likely to happen when \
                      converting from a later to an earlier version, as many \
                      keywords from the later version may not exist in the \
                      earlier version. There is no place to keep these values so \
                      they must be discarded. Set to ``True`` to perform the \
                      conversion with such discarding; otherwise, remove the \
                      keywords manually before converting.";
    let param = DocArg::new_param("force".into(), PyType::Bool, param_desc.into());
    let outputs: Vec<_> = ALL_VERSIONS
        .iter()
        .filter(|&&v| v != version)
        .map(|v| {
            let fn_name = format_ident!("version_{v}");
            let target_type = format_ident!("{base}{v}");
            let target_rs_type = target_type.to_string().replace("Py", "");
            let pretty_version = v.replace("_", ".");
            let doc = DocString::new(
                format!("Convert to FCS {pretty_version}."),
                vec![sub.into()],
                vec![param.clone()],
                Some(DocReturn::new(
                    PyType::PyClass(target_rs_type),
                    Some(format!("A new class conforming to FCS {pretty_version}")),
                )),
            );
            quote! {
                #[pymethods]
                impl #pytype {
                    #doc
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

    let base_type = PyType::Raw(info.pytype.value());
    let rtype = PyType::new_list(if optional {
        PyType::new_opt(base_type)
    } else {
        base_type
    });
    let doc = DocString::new(
        format!("Value of *$Pn{}*.", s.to_uppercase()),
        vec![],
        vec![],
        Some(DocReturn::new(rtype, None)),
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
                    #doc
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

#[proc_macro]
pub fn impl_gated_meas(_: TokenStream) -> TokenStream {
    let scale = DocArg::new_ivar(
        "scale".into(),
        PyType::new_union(nonempty![
            PyType::new_unit(),
            PyType::Tuple(vec![PyType::Float, PyType::Float]),
            PyType::None
        ]),
        "The *$GmE* keyword. ``()`` means linear scaling and 2-tuple \
         specifies decades and offset for log scaling."
            .into(),
    );
    let make_arg = |n: &str, kw: &str, t: PyType| {
        DocArg::new_ivar(
            n.into(),
            PyType::new_opt(t),
            format!("The *$Gm{kw}* keyword."),
        )
    };
    let filter = make_arg("filter", "F", PyType::Str);
    let shortname = make_arg("shortname", "N", PyType::Str);
    let percent_emitted = make_arg("percent_emitted", "P", PyType::Str);
    let range = make_arg("range", "R", PyType::Float);
    let longname = make_arg("longname", "S", PyType::Str);
    let detector_type = make_arg("detector_type", "T", PyType::Str);
    let detector_voltage = make_arg("detector_voltage", "V", PyType::Float);
    let doc = DocString::new(
        "The *$Gm\\** keywords for one gated measurement.".into(),
        vec![],
        vec![
            scale,
            filter,
            shortname,
            percent_emitted,
            range,
            longname,
            detector_type,
            detector_voltage,
        ],
        None,
    );

    let make_get_set = |n: &str, t: &str| {
        let get = format_ident!("get_{n}");
        let set = format_ident!("set_{n}");
        let inner = format_ident!("{n}");
        let s = format!("fireflow_core::text::keywords::{t}");
        let rstype = parse_str::<Path>(s.as_str()).unwrap();
        quote! {
            #[getter]
            fn #get(&self) -> Option<#rstype> {
                self.0.#inner.0.as_ref().cloned()
            }

            #[setter]
            fn #set(&mut self, x: Option<#rstype>) {
                self.0.#inner.0 = x.into();
            }
        }
    };

    let methods: Vec<_> = [
        ("scale", "GateScale"),
        ("filter", "GateFilter"),
        ("shortname", "GateShortname"),
        ("percent_emitted", "GatePercentEmitted"),
        ("longname", "GateLongname"),
        ("detector_type", "GateDetectorType"),
        ("detector_voltage", "GateDetectorVoltage"),
    ]
    .into_iter()
    .map(|(n, t)| make_get_set(n, t))
    .collect();

    quote! {
        // TODO not dry
        #doc
        #[pyclass(name = "GatedMeasurement", eq)]
        #[derive(Clone, From, Into, PartialEq)]
        pub struct PyGatedMeasurement(fireflow_core::text::gating::GatedMeasurement);

        // TODO this also doesn't seem DRY
        #[pymethods]
        impl PyGatedMeasurement {
            #[new]
            #[allow(clippy::too_many_arguments)]
            #[pyo3(signature = (
                scale = None,
                filter = None,
                shortname = None,
                percent_emitted = None,
                range = None,
                longname = None,
                detector_type = None,
                detector_voltage = None,
            ))]
            fn new(
                scale: Option<kws::GateScale>,
                filter: Option<kws::GateFilter>,
                shortname: Option<kws::GateShortname>,
                percent_emitted: Option<kws::GatePercentEmitted>,
                range: Option<kws::GateRange>,
                longname: Option<kws::GateLongname>,
                detector_type: Option<kws::GateDetectorType>,
                detector_voltage: Option<kws::GateDetectorVoltage>,
            ) -> Self {
                GatedMeasurement::new(
                    scale,
                    filter,
                    shortname,
                    percent_emitted,
                    range,
                    longname,
                    detector_type,
                    detector_voltage,
                )
                .into()
            }

            #(#methods)*
        }
    }
    .into()
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

fn ugly_version(v: Version) -> &'static str {
    match v {
        Version::FCS2_0 => "2_0",
        Version::FCS3_0 => "3_0",
        Version::FCS3_1 => "3_1",
        Version::FCS3_2 => "3_2",
    }
}

const ALL_VERSIONS: [&str; 4] = ["2_0", "3_0", "3_1", "3_2"];

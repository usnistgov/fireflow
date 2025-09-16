extern crate proc_macro;

mod docstring;

use crate::docstring::{DocArg, DocDefault, DocReturn, DocSelf, DocString, PyType};

use fireflow_core::header::Version;

use proc_macro::TokenStream;

use derive_new::new;
use itertools::Itertools;
use quote::{format_ident, quote};
use syn::{
    parse::{Parse, ParseStream},
    parse_macro_input, parse_quote,
    token::Comma,
    GenericArgument, Ident, LitBool, LitInt, Path, PathArguments, Result, Type,
};

#[proc_macro]
pub fn def_fcs_read_header(_: TokenStream) -> TokenStream {
    let fun_path = quote!(fireflow_core::api::fcs_read_header);
    let ret_path = quote!(fireflow_core::header::Header);
    let args = ArgData::header_config_args();
    let fun_args: Vec<_> = args.iter().map(|a| a.constr_arg()).collect();
    let inner_args: Vec<_> = args.iter().map(|a| a.inner_arg1()).collect();
    let conf_params: Vec<_> = args.iter().map(|a| a.doc.clone()).collect();
    let doc = DocString::new(
        "Read the *HEADER* of an FCS file.".into(),
        vec![],
        DocSelf::NoSelf,
        [path_param(true)].into_iter().chain(conf_params).collect(),
        Some(DocReturn::new(PyType::PyClass("Header".into()), None)),
    );
    quote! {
        #[pyfunction]
        #doc
        pub fn fcs_read_header(path: std::path::PathBuf, #(#fun_args),*) -> PyResult<#ret_path> {
            let inner = fireflow_core::config::HeaderConfigInner{
                #(#inner_args),*
            };
            let conf =  fireflow_core::config::ReadHeaderConfig(inner);
            #fun_path(&path, &conf).py_termfail_resolve_nowarn()
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_new_core(input: TokenStream) -> TokenStream {
    let info = parse_macro_input!(input as NewCoreInfo);
    let version = info.version;
    let vsu = version.short_underscore();
    let vs = version.short();

    let coretext_name = info.coretext_name;
    let coredataset_name = info.coredataset_name;
    let coretext_rstype = info.coretext_path;
    let coredataset_rstype = info.coredataset_path;

    let fun_name = format_ident!("try_new_{vsu}");
    let fun: Path = parse_quote!(#coretext_rstype::#fun_name);

    let meas = ArgData::new_measurements_arg(version);
    let layout = ArgData::new_layout_arg(version);
    let df = ArgData::new_df_arg();
    let analysis = ArgData::new_analysis_arg();
    let others = ArgData::new_others_arg();

    let mode = if version < Version::FCS3_2 {
        let t = PyType::new_lit(&["L", "U", "C"]);
        let m = quote!(fireflow_core::text::keywords::Mode::default());
        let d = DocDefault::Other(m, "L".into());
        ArgData::new_kw_arg("Mode", "mode", t, None, Some(d))
    } else {
        ArgData::new_kw_opt_arg("Mode3_2", "mode", PyType::new_lit(&["L"]))
    };

    let cyt = if version < Version::FCS3_2 {
        ArgData::new_kw_opt_arg("Cyt", "cyt", PyType::Str)
    } else {
        ArgData::new_kw_arg("Cyt", "cyt", PyType::Str, None, None)
    };

    let abrt = ArgData::new_kw_opt_arg("Abrt", "abrt", PyType::Int);
    let com = ArgData::new_kw_opt_arg("Com", "com", PyType::Str);
    let cells = ArgData::new_kw_opt_arg("Cells", "cells", PyType::Str);
    let exp = ArgData::new_kw_opt_arg("Exp", "exp", PyType::Str);
    let fil = ArgData::new_kw_opt_arg("Fil", "fil", PyType::Str);
    let inst = ArgData::new_kw_opt_arg("Inst", "inst", PyType::Str);
    let lost = ArgData::new_kw_opt_arg("Lost", "lost", PyType::Int);
    let op = ArgData::new_kw_opt_arg("Op", "op", PyType::Str);
    let proj = ArgData::new_kw_opt_arg("Proj", "proj", PyType::Str);
    let smno = ArgData::new_kw_opt_arg("Smno", "smno", PyType::Str);
    let src = ArgData::new_kw_opt_arg("Src", "src", PyType::Str);
    let sys = ArgData::new_kw_opt_arg("Sys", "sys", PyType::Str);
    let cytsn = ArgData::new_kw_opt_arg("Cytsn", "cytsn", PyType::Str);

    let unicode_pytype = PyType::Tuple(vec![PyType::Int, PyType::new_list(PyType::Str)]);
    let unicode = ArgData::new_kw_opt_arg("Unicode", "unicode", unicode_pytype);

    let csvbits = ArgData::new_kw_opt_arg("CSVBits", "csvbits", PyType::Int);
    let cstot = ArgData::new_kw_opt_arg("CSTot", "cstot", PyType::Int);

    let csvflags = ArgData::new_csvflags_arg();

    let all_subset = [csvbits, cstot, csvflags];

    let last_modifier = ArgData::new_kw_opt_arg("LastModifier", "last_modifier", PyType::Datetime);
    let last_modified = ArgData::new_kw_opt_arg("LastModified", "last_modified", PyType::Str);
    let originality = ArgData::new_kw_opt_arg(
        "Originality",
        "originality",
        PyType::new_lit(&["Original", "NonDataModified", "Appended", "DataModified"]),
    );

    let all_modified = [last_modifier, last_modified, originality];

    let plateid = ArgData::new_kw_opt_arg("Plateid", "plateid", PyType::Str);
    let platename = ArgData::new_kw_opt_arg("Platename", "platename", PyType::Str);
    let wellid = ArgData::new_kw_opt_arg("Wellid", "wellid", PyType::Str);

    let all_plate = [plateid, platename, wellid];

    let vol = ArgData::new_kw_opt_arg("Vol", "vol", PyType::Float);

    let comp_or_spill = match version {
        Version::FCS2_0 => ArgData::new_comp_arg(true),
        Version::FCS3_0 => ArgData::new_comp_arg(false),
        _ => ArgData::new_spillover_arg(),
    };

    let flowrate = ArgData::new_kw_opt_arg("Flowrate", "flowrate", PyType::Str);

    let carrierid = ArgData::new_kw_opt_arg("Carrierid", "carrierid", PyType::Str);
    let carriertype = ArgData::new_kw_opt_arg("Carriertype", "carriertype", PyType::Str);
    let locationid = ArgData::new_kw_opt_arg("Locationid", "locationid", PyType::Str);

    let all_carrier = [carrierid, carriertype, locationid];

    let unstainedcenters = ArgData::new_unstainedcenters_arg();
    let unstainedinfo = ArgData::new_kw_opt_arg("UnstainedInfo", "unstainedinfo", PyType::Str);

    let tr = ArgData::new_trigger_arg();

    let all_timestamps = match version {
        Version::FCS2_0 => ArgData::new_timestamps_args("FCSTime"),
        Version::FCS3_0 => ArgData::new_timestamps_args("FCSTime60"),
        Version::FCS3_1 | Version::FCS3_2 => ArgData::new_timestamps_args("FCSTime100"),
    };

    let all_datetimes = [
        ArgData::new_datetime_arg(true),
        ArgData::new_datetime_arg(false),
    ];

    let applied_gates = ArgData::new_applied_gates_arg(version);

    let nonstandard_keywords = ArgData::new_core_nonstandard_keywords_arg();

    let common_kws = [
        abrt,
        com,
        cells,
        exp,
        fil,
        inst,
        lost,
        op,
        proj,
        smno,
        src,
        sys,
        tr,
        applied_gates,
        nonstandard_keywords,
    ];

    let all_kws: Vec<_> = match version {
        Version::FCS2_0 => [mode, cyt, comp_or_spill]
            .into_iter()
            .chain(all_timestamps)
            .chain(common_kws)
            .collect(),
        Version::FCS3_0 => [mode, cyt, comp_or_spill]
            .into_iter()
            .chain(all_timestamps)
            .chain([cytsn, unicode])
            .chain(all_subset)
            .chain(common_kws)
            .collect(),
        Version::FCS3_1 => [mode, cyt]
            .into_iter()
            .chain(all_timestamps)
            .chain([cytsn, comp_or_spill])
            .chain(all_modified)
            .chain(all_plate)
            .chain([vol])
            .chain(all_subset)
            .chain(common_kws)
            .collect(),
        Version::FCS3_2 => [cyt, mode]
            .into_iter()
            .chain(all_timestamps)
            .chain(all_datetimes)
            .chain([cytsn, comp_or_spill])
            .chain(all_modified)
            .chain(all_plate)
            .chain([vol])
            .chain(all_carrier)
            .chain([unstainedinfo, unstainedcenters, flowrate])
            .chain(common_kws)
            .collect(),
    };

    let coretext_args: Vec<_> = [&meas, &layout].into_iter().chain(&all_kws).collect();
    let coredataset_args: Vec<_> = [&meas, &layout, &df]
        .into_iter()
        .chain(&all_kws)
        .chain([&analysis, &others])
        .collect();

    let coretext_ivar_methods: Vec<_> = coretext_args.iter().flat_map(|x| &x.methods).collect();
    let coredataset_ivar_methods: Vec<_> =
        coredataset_args.iter().flat_map(|x| &x.methods).collect();

    let coretext_params: Vec<_> = coretext_args.iter().map(|x| x.doc.clone()).collect();
    let coredataset_params: Vec<_> = coredataset_args.iter().map(|x| x.doc.clone()).collect();

    let coretext_funargs: Vec<_> = coretext_args.iter().map(|x| x.constr_arg()).collect();
    let coredataset_funargs: Vec<_> = coredataset_args.iter().map(|x| x.constr_arg()).collect();

    let coretext_inner_args: Vec<_> = coretext_args.iter().map(|x| x.inner_arg()).collect();

    let coretext_doc = DocString::new(
        format!("Represents *TEXT* for an FCS {vs} file."),
        vec![],
        DocSelf::NoSelf,
        coretext_params,
        None,
    );

    let coredataset_doc = DocString::new(
        format!("Represents one dataset in an FCS {vs} file."),
        vec![],
        DocSelf::NoSelf,
        coredataset_params,
        None,
    );

    let coretext_new = quote! {
        fn new(#(#coretext_funargs),*) -> PyResult<Self> {
            Ok(#fun(#(#coretext_inner_args),*).mult_head()?.into())
        }
    };

    let coredataset_new = quote! {
        fn new(#(#coredataset_funargs),*) -> PyResult<Self> {
            let x = #fun(#(#coretext_inner_args),*).mult_head()?;
            Ok(x.into_coredataset(df, analysis, others)?.into())
        }
    };

    let coretext_q = impl_new(
        coretext_name.to_string(),
        coretext_rstype,
        coretext_doc,
        coretext_new,
        quote!(#(#coretext_ivar_methods)*),
    )
    .1;

    let coredataset_q = impl_new(
        coredataset_name.to_string(),
        coredataset_rstype,
        coredataset_doc,
        coredataset_new,
        quote!(#(#coredataset_ivar_methods)*),
    )
    .1;

    quote! {
        #coretext_q
        #coredataset_q
    }
    .into()
}

#[proc_macro]
pub fn impl_core_version(input: TokenStream) -> TokenStream {
    let t = parse_macro_input!(input as Ident);
    let _ = split_ident_version_pycore(&t);
    let doc = DocString::new(
        "Show the FCS version.".into(),
        vec![],
        DocSelf::PySelf,
        vec![],
        Some(DocReturn::new(version_pytype(), None)),
    )
    .doc();

    quote! {
        #[pymethods]
        impl #t {
            #doc
            #[getter]
            fn version(&self) -> Version {
                self.0.fcs_version()
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_core_par(input: TokenStream) -> TokenStream {
    let t = parse_macro_input!(input as Ident);
    let _ = split_ident_version_pycore(&t);
    let doc = DocString::new(
        "The value for *$PAR*.".into(),
        vec![],
        DocSelf::PySelf,
        vec![],
        Some(DocReturn::new(PyType::Int, None)),
    )
    .doc();

    quote! {
        #[pymethods]
        impl #t {
            #doc
            #[getter]
            fn par(&self) -> usize {
                self.0.par().0
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_core_all_meas_nonstandard_keywords(input: TokenStream) -> TokenStream {
    let t = parse_macro_input!(input as Ident);
    let _ = split_ident_version_pycore(&t);

    let doc = DocString::new(
        "The non-standard keywords for each measurement.".into(),
        vec![],
        DocSelf::PySelf,
        vec![],
        Some(DocReturn::new(
            PyType::new_list(PyType::new_dict(PyType::Str, PyType::Str)),
            Some("A list of non-standard keyword dicts for each measurement.".into()),
        )),
    )
    .doc();

    let nsk = quote!(fireflow_core::validated::keys::NonStdKey);
    let ret = quote!(Vec<std::collections::HashMap<#nsk, String>>);

    quote! {
        #[pymethods]
        impl #t {
            #doc
            #[getter]
            fn get_all_meas_nonstandard_keywords(&self) -> #ret {
                self.0.get_meas_nonstandard().into_iter().map(|x| x.clone()).collect()
            }

            #[setter]
            fn set_all_meas_nonstandard_keywords(&mut self, ns: #ret) -> PyResult<()> {
                Ok(self.0.set_meas_nonstandard(ns)?)
            }
        }
    }
    .into()
}

// TODO make this return $TOT, $NEXTDATA, etc
#[proc_macro]
pub fn impl_core_standard_keywords(input: TokenStream) -> TokenStream {
    let t = parse_macro_input!(input as Ident);
    let _ = split_ident_version_pycore(&t);

    let make_param = |req: bool, root: bool| {
        let (x, a) = if req {
            ("req", "required")
        } else {
            ("opt", "non-required")
        };
        let (y, b) = if root {
            ("root", "non-measurement")
        } else {
            ("meas", "measurement")
        };
        DocArg::new_param_def(
            format!("exclude_{x}_{y}"),
            PyType::Bool,
            format!("Do not include {a} {b} keywords"),
            DocDefault::Bool(false),
        )
    };

    let doc = DocString::new(
        "Return standard keywords as string pairs.".into(),
        vec![
            "Each key will be prefixed with *$*.".into(),
            "This will not include *$TOT*, *$NEXTDATA* or any of the \
             offset keywords since these are not encoded in this class."
                .into(),
        ],
        DocSelf::PySelf,
        vec![
            make_param(true, true),
            make_param(false, true),
            make_param(true, false),
            make_param(false, false),
        ],
        Some(DocReturn::new(
            PyType::new_dict(PyType::Str, PyType::Str),
            Some("A list of standard keywords.".into()),
        )),
    );

    quote! {
        #[pymethods]
        impl #t {
            #doc
            fn standard_keywords(
                &self,
                exclude_req_root: bool,
                exclude_opt_root: bool,
                exclude_req_meas: bool,
                exclude_opt_meas: bool,
            ) -> HashMap<String, String> {
                self.0.standard_keywords(
                    exclude_req_root,
                    exclude_opt_root,
                    exclude_req_meas,
                    exclude_opt_meas
                )
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_core_set_tr_threshold(input: TokenStream) -> TokenStream {
    let t = parse_macro_input!(input as Ident);
    let _ = split_ident_version_pycore(&t);
    let doc = DocString::new(
        "Set the threshold for *$TR*.".into(),
        vec![],
        DocSelf::PySelf,
        vec![DocArg::new_param(
            "threshold".into(),
            PyType::Int,
            "The threshold to set.".into(),
        )],
        Some(DocReturn::new(
            PyType::Bool,
            Some("``True`` if trigger is set and was updated.".into()),
        )),
    );

    quote! {
        #[pymethods]
        impl #t {
            #doc
            fn set_trigger_threshold(&mut self, threshold: u32) -> bool {
                self.0.set_trigger_threshold(threshold)
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_core_write_text(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let version = split_ident_version_pycore(&i).1;
    let textdelim_path = textdelim_path();

    let write_2_0_warning = if version == Version::FCS2_0 {
        Some("Will raise exception if file cannot fit within 99,999,999 bytes.".into())
    } else {
        None
    };

    let write_text_doc = DocString::new(
        "Write data to path.".into(),
        ["Resulting FCS file will include *HEADER* and *TEXT*.".into()]
            .into_iter()
            .chain(write_2_0_warning.clone())
            .collect(),
        DocSelf::PySelf,
        vec![path_param(false), textdelim_param(), big_other_param()],
        None,
    );

    quote! {
        #[pymethods]
        impl #i {
            #write_text_doc
            fn write_text(
                &self,
                path: std::path::PathBuf,
                delim: #textdelim_path,
                big_other: bool
            ) -> PyResult<()> {
                let f = std::fs::File::options().write(true).create(true).open(path)?;
                let mut h = std::io::BufWriter::new(f);
                self.0.h_write_text(&mut h, delim, big_other).py_termfail_resolve_nowarn()
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_core_write_dataset(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let version = split_ident_version_pycore(&i).1;
    let textdelim_path = textdelim_path();

    let write_2_0_warning = if version == Version::FCS2_0 {
        Some("Will raise exception if file cannot fit within 99,999,999 bytes.".into())
    } else {
        None
    };

    let doc = DocString::new(
        "Write data as an FCS file.".into(),
        ["The resulting file will include *HEADER*, *TEXT*, *DATA*, \
            *ANALYSIS*, and *OTHER* as they present from this class."
            .into()]
        .into_iter()
        .chain(write_2_0_warning)
        .collect(),
        DocSelf::PySelf,
        vec![
            path_param(false),
            textdelim_param(),
            big_other_param(),
            DocArg::new_param_def(
                "skip_conversion_check".into(),
                PyType::Bool,
                "Skip check to ensure that types of the dataframe match the \
                 columns (*$PnB*, *$DATATYPE*, etc). If this is ``False``, \
                 perform this check before writing, and raise exception on \
                 failure. If ``True``, raise warnings as file is being \
                 written. Skipping this is faster since the data needs to be \
                 traversed twice to perform the conversion check, but may \
                 result in loss of precision and/or truncation."
                    .into(),
                DocDefault::Bool(false),
            ),
        ],
        None,
    );

    quote! {
        #[pymethods]
        impl #i {
            #doc
            fn write_dataset(
                &self,
                path: std::path::PathBuf,
                delim: #textdelim_path,
                big_other: bool,
                skip_conversion_check: bool,
            ) -> PyResult<()> {
                let f = std::fs::File::options().write(true).create(true).open(path)?;
                let mut h = std::io::BufWriter::new(f);
                let conf = fireflow_core::config::WriteConfig {
                    delim,
                    skip_conversion_check,
                    big_other,
                };
                self.0.h_write_dataset(&mut h, &conf).py_termfail_resolve()
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_core_all_peak_attrs(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let _ = split_ident_version_pycore(&i).1;

    let go = |k: &str, i: &str, name: &str| {
        let p = keyword_path(i);
        let doc = DocString::new(
            format!("The value of *$P{k}n* for all measurements."),
            vec![],
            DocSelf::PySelf,
            vec![],
            Some(DocReturn::new(PyType::new_list(PyType::Int), None)),
        )
        .doc();
        let get = format_ident!("get_all_{name}");
        let set = format_ident!("set_all_{name}");
        quote! {
            #doc
            #[getter]
            fn #get(&self) -> Vec<Option<#p>> {
                self.0
                    .get_temporal_optical::<Option<#p>>()
                    .map(|x| x.as_ref().copied())
                    .collect()
            }

            #[setter]
            fn #set(&mut self, xs: Vec<Option<#p>>) -> PyResult<()> {
                Ok(self.0.set_temporal_optical(xs)?)
            }
        }
    };

    let pkn = go("K", "PeakBin", "peak_bins");
    let pknn = go("KN", "PeakNumber", "peak_sizes");

    quote! {
        #[pymethods]
        impl #i {
            #pkn
            #pknn
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_core_all_shortnames_attr(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let _ = split_ident_version_pycore(&i).1;
    let shortname_path = shortname_path();

    let doc = DocString::new(
        "Value of *$PnN* for all measurements.".into(),
        vec!["Strings are unique and cannot contain commas.".into()],
        DocSelf::PySelf,
        vec![],
        Some(DocReturn::new(PyType::new_list(PyType::Str), None)),
    )
    .doc();

    quote! {
        #[pymethods]
        impl #i {
            #doc
            #[getter]
            fn get_all_shortnames(&self) -> Vec<#shortname_path> {
                self.0.all_shortnames()
            }

            #[setter]
            fn set_all_shortnames(&mut self, names: Vec<#shortname_path>) -> PyResult<()> {
                Ok(self.0.set_all_shortnames(names).void()?)
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_core_all_shortnames_maybe_attr(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let _ = split_ident_version_pycore(&i).1;
    let shortname_path = shortname_path();

    let doc = DocString::new(
        "The possibly-empty values of *$PnN* for all measurements.".into(),
        vec!["*$PnN* is optional for this FCS version so values may be ``None``.".into()],
        DocSelf::PySelf,
        vec![],
        Some(DocReturn::new(
            PyType::new_list(PyType::new_opt(PyType::Str)),
            None,
        )),
    )
    .doc();

    quote! {
        #[pymethods]
        impl #i {
            #doc
            #[getter]
            fn get_all_shortnames_maybe(&self) -> Vec<Option<#shortname_path>> {
                self.0
                    .shortnames_maybe()
                    .into_iter()
                    .map(|x| x.cloned())
                    .collect()
            }

            #[setter]
            fn set_all_shortnames_maybe(&mut self, names: Vec<Option<#shortname_path>>) -> PyResult<()> {
                Ok(self.0.set_measurement_shortnames_maybe(names).void()?)
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_core_get_set_timestep(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let _ = split_ident_version_pycore(&i).1;
    let timestep_path = keyword_path("Timestep");

    let t = PyType::new_opt(PyType::Float);
    let get_doc = DocString::new(
        "The value of *$TIMESTEP*".into(),
        vec![],
        DocSelf::PySelf,
        vec![],
        Some(DocReturn::new(t.clone(), None)),
    )
    .doc();
    let set_doc = DocString::new(
        "Set the *$TIMESTEP* if time measurement is present.".into(),
        vec![],
        DocSelf::PySelf,
        vec![DocArg::new_param(
            "timestep".into(),
            PyType::Float,
            "The timestep to set. Must be greater than zero.".into(),
        )],
        Some(DocReturn::new(
            t,
            Some("Previous *$TIMESTEP* if present.".into()),
        )),
    );
    let q = quote! {
        #get_doc
        #[getter]
        fn get_timestep(&self) -> Option<#timestep_path> {
            self.0.timestep().copied()
        }

        #set_doc
        fn set_timestep(&mut self, timestep: #timestep_path) -> Option<#timestep_path> {
            self.0.set_timestep(timestep)
        }
    };

    quote! {
        #[pymethods]
        impl #i {
            #q
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_core_set_temporal(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let version = split_ident_version_pycore(&i).1;
    let shortname_path = shortname_path();
    let timestep_path = keyword_path("Timestep");
    let meas_index_path = meas_index_path();

    let make_doc = |has_timestep: bool, has_index: bool| {
        let name = param_name("Name to set to temporal.");
        let index = DocArg::new_param("index".into(), PyType::Int, "Index to set".into());
        let (i, p) = if has_index {
            ("index", index)
        } else {
            ("name", name)
        };
        let timestep = if has_timestep {
            Some(DocArg::new_param(
                "timestep".into(),
                PyType::Float,
                "The value of *$TIMESTEP* to use.".into(),
            ))
        } else {
            None
        };
        let force = DocArg::new_param_def(
            "force".into(),
            PyType::Bool,
            "If ``True`` remove any optical-specific metadata (detectors, \
             lasers, etc) without raising an exception. Defauls to ``False``."
                .into(),
            DocDefault::Bool(false),
        );
        let ps = [p].into_iter().chain(timestep).chain([force]).collect();
        DocString::new(
            format!("Set the temporal measurement to a given {i}."),
            vec![],
            DocSelf::PySelf,
            ps,
            Some(DocReturn::new(
                PyType::Bool,
                Some(format!(
                    "``True`` if temporal measurement was set, which will \
                     happen for all cases except when the time measurement is \
                     already set to ``{i}``."
                )),
            )),
        )
    };

    let q = if version == Version::FCS2_0 {
        let name_doc = make_doc(false, false);
        let index_doc = make_doc(false, true);
        quote! {
            #name_doc
            fn set_temporal(&mut self, name: #shortname_path, force: bool) -> PyResult<bool> {
                self.0.set_temporal(&name, (), force).py_termfail_resolve()
            }

            #index_doc
            fn set_temporal_at(&mut self, index: #meas_index_path, force: bool) -> PyResult<bool> {
                self.0.set_temporal_at(index, (), force).py_termfail_resolve()
            }
        }
    } else {
        let name_doc = make_doc(true, false);
        let index_doc = make_doc(true, true);
        quote! {
            #name_doc
            fn set_temporal(
                &mut self,
                name: #shortname_path,
                timestep: #timestep_path,
                force: bool,
            ) -> PyResult<bool> {
                self.0
                    .set_temporal(&name, timestep, force)
                    .py_termfail_resolve()
            }

            #index_doc
            fn set_temporal_at(
                &mut self,
                index: #meas_index_path,
                timestep: #timestep_path,
                force: bool,
            ) -> PyResult<bool> {
                self.0
                    .set_temporal_at(index, timestep, force)
                    .py_termfail_resolve()
            }
        }
    };

    quote! {
        #[pymethods]
        impl #i {
            #q
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_core_unset_temporal(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let version = split_ident_version_pycore(&i).1;
    let timestep_path = keyword_path("Timestep");

    let make_doc = |has_timestep: bool, has_force: bool| {
        let s = "Convert the temporal measurement to an optical measurement.".into();
        let p = if has_force {
            Some(DocArg::new_param_def(
                "force".into(),
                PyType::Bool,
                "If ``True`` and current time measurement has data which cannot \
                 be converted to optical, force the conversion anyways. \
                 Otherwise raise an exception."
                    .into(),
                DocDefault::Bool(false),
            ))
        } else {
            None
        }
        .into_iter()
        .collect();
        let (rt, rd) = if has_timestep {
            (
                PyType::new_opt(PyType::Float),
                "Value of *$TIMESTEP* if time measurement was present.".into(),
            )
        } else {
            (
                PyType::Bool,
                "``True`` if temporal measurement was present and converted, \
                 ``False`` if there was not a temporal measurement."
                    .into(),
            )
        };
        DocString::new(
            s,
            vec![],
            DocSelf::PySelf,
            p,
            Some(DocReturn::new(rt, Some(rd))),
        )
    };

    let q = if version == Version::FCS2_0 {
        let doc = make_doc(false, false);
        quote! {
            #doc
            fn unset_temporal(&mut self) -> bool {
                self.0.unset_temporal().is_some()
            }
        }
    } else if version < Version::FCS3_2 {
        let doc = make_doc(true, false);
        quote! {
            #doc
            fn unset_temporal(&mut self) -> Option<#timestep_path> {
                self.0.unset_temporal()
            }
        }
    } else {
        let doc = make_doc(true, true);
        quote! {
            #doc
            fn unset_temporal(&mut self, force: bool) -> PyResult<Option<#timestep_path>> {
                self.0.unset_temporal_lossy(force).py_termfail_resolve()
            }
        }
    };

    quote! {
        #[pymethods]
        impl #i {
            #q
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_core_rename_temporal(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let _ = split_ident_version_pycore(&i).1;
    let shortname_path = shortname_path();

    let doc = DocString::new(
        "Rename temporal measurement if present.".into(),
        vec![],
        DocSelf::PySelf,
        vec![param_name("New name to assign.")],
        Some(DocReturn::new(
            PyType::new_opt(PyType::Bool),
            Some("Previous name if present".into()),
        )),
    );

    quote! {
        #[pymethods]
        impl #i {
            #doc
            fn rename_temporal(&mut self, name: #shortname_path) -> Option<#shortname_path> {
                self.0.rename_temporal(name)
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_core_all_transforms_attr(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let version = split_ident_version_pycore(&i).1;
    let scale_path = quote!(fireflow_core::text::scale::Scale);
    let xform_path = quote!(fireflow_core::core::ScaleTransform);

    let log_pytype = PyType::Tuple(vec![PyType::Float, PyType::Float]);
    let q = if version == Version::FCS2_0 {
        let s0 = "Will be ``()`` for linear scaling (``0,0`` in FCS encoding), \
                   a 2-tuple for log scaling, or ``None`` if missing."
            .into();
        let s1 = "The temporal measurement must always be ``()``. Setting it \
                  to another value will raise an exception."
            .into();
        // TODO this will probably end up not being DRY
        let doc = DocString::new(
            "The value for *$PnE* for all measurements.".into(),
            vec![s0, s1],
            DocSelf::PySelf,
            vec![],
            Some(DocReturn::new(
                PyType::new_list(PyType::new_union(vec![
                    PyType::new_unit(),
                    log_pytype,
                    PyType::None,
                ])),
                None,
            )),
        )
        .doc();
        quote! {
            #doc
            #[getter]
            fn get_all_scales(&self) -> Vec<Option<#scale_path>> {
                self.0.scales().collect()
            }

            #[setter]
            fn set_all_scales(&mut self, scales: Vec<Option<#scale_path>>) -> PyResult<()> {
                self.0.set_scales(scales).py_termfail_resolve_nowarn()
            }
        }
    } else {
        let sum = "The value for *$PnE* and/or *$PnG* for all measurements.";
        let s0 = "Collectively these keywords correspond to scale transforms.";
        let s1 = "If scaling is linear, return a float which corresponds to the \
                  value of *$PnG* when *$PnE* is ``0,0``. If scaling is logarithmic, \
                  return a pair of floats, corresponding to unset *$PnG* and the \
                  non-``0,0`` value of *$PnE*.";
        let s2 = "The FCS standards disallow any other combinations.";
        let s3 = "The temporal measurement will always be ``1.0``, corresponding \
                  to an identity transform. Setting it to another value will \
                  raise an exception.";
        let doc = DocString::new(
            sum.into(),
            vec![s0.into(), s1.into(), s2.into(), s3.into()],
            DocSelf::PySelf,
            vec![],
            Some(DocReturn::new(
                PyType::new_list(PyType::new_union2(PyType::Float, log_pytype)),
                None,
            )),
        )
        .doc();
        quote! {
            #doc
            #[getter]
            fn get_all_scale_transforms(&self) -> Vec<#xform_path> {
                self.0.transforms().collect()
            }

            #[setter]
            fn set_all_scale_transforms(&mut self, transforms: Vec<#xform_path>) -> PyResult<()> {
                self.0.set_transforms(transforms).py_termfail_resolve_nowarn()
            }
        }
    };

    quote! {
        #[pymethods]
        impl #i {
            #q
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_core_get_measurements(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let version = split_ident_version_pycore(&i).1;

    let element_path = element_path(version);
    let named_vec_path = quote!(fireflow_core::text::named_vec::NamedVec);

    let doc = DocString::new(
        "Get all measurements.".into(),
        vec![],
        DocSelf::PySelf,
        vec![],
        Some(DocReturn::new(
            PyType::new_list(measurement_pytype(version)),
            None,
        )),
    )
    .doc();

    quote! {
        #[pymethods]
        impl #i {
            #doc
            #[getter]
            fn measurements(&self) -> Vec<#element_path> {
                // This might seem inefficient since we are cloning
                // everything, but if we want to map a python lambda
                // function over the measurements we would need to to do
                // this anyways, so simply returnig a copied list doesn't
                // lose anything and keeps this API simpler.
                let ms: &#named_vec_path<_, _, _, _> = self.0.as_ref();
                ms.iter()
                    .map(|(_, e)| e.bimap(|t| t.value.clone(), |o| o.value.clone()))
                    .map(|v| v.inner_into())
                    .collect()
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_core_get_temporal(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let version = split_ident_version_pycore(&i).1;

    let ttype = pytemporal(version);
    let meas_index_path = meas_index_path();
    let shortname_path = shortname_path();

    let doc = DocString::new(
        "Get the temporal measurement if it exists.".into(),
        vec![],
        DocSelf::PySelf,
        vec![],
        Some(DocReturn::new(
            PyType::new_opt(PyType::Tuple(vec![
                PyType::Int,
                PyType::Str,
                temporal_pytype(version),
            ])),
            Some("Index, name, and measurement or ``None``".into()),
        )),
    )
    .doc();

    quote! {
        #[pymethods]
        impl #i {
            #doc
            #[getter]
            fn get_temporal(&self) -> Option<(#meas_index_path, #shortname_path, #ttype)> {
                self.0
                    .temporal()
                    .map(|t| (t.index, t.key.clone(), t.value.clone().into()))
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_core_get_measurement(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let version = split_ident_version_pycore(&i).1;

    let element_path = element_path(version);
    let named_vec_path = quote!(fireflow_core::text::named_vec::NamedVec);
    let meas_index_path = meas_index_path();

    let doc = DocString::new(
        "Return measurement at index.".into(),
        vec!["Raise exception if ``index`` not found.".into()],
        DocSelf::PySelf,
        vec![param_index("Index to retrieve.")],
        Some(DocReturn::new(measurement_pytype(version), None)),
    );

    quote! {
        #[pymethods]
        impl #i {
            // TODO this should return name as well
            #doc
            fn measurement_at(&self, index: #meas_index_path) -> PyResult<#element_path> {
                let ms: &#named_vec_path<_, _, _, _> = self.0.as_ref();
                let m = ms.get(index)?;
                Ok(m.bimap(|x| x.1.clone(), |x| x.1.clone()).inner_into())
            }

            // TODO return measurement with name
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_core_set_measurements(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let (is_dataset, version) = split_ident_version_pycore(&i);

    let meas_argtype = ArgData::new_measurements_arg(version).rstype;

    let s = if is_dataset {
        "layout and dataframe"
    } else {
        "layout"
    };
    let ps = vec![format!(
        "Length of ``measurements`` must match number of columns in existing {s}."
    )];
    let doc = DocString::new(
        "Set all measurements at once.".into(),
        ps,
        DocSelf::PySelf,
        vec![
            param_type_set_meas(version),
            param_allow_shared_names(),
            param_skip_index_check(),
        ],
        None,
    );

    quote! {
        #[pymethods]
        impl #i {
            #doc
            fn set_measurements(
                &mut self,
                measurements: #meas_argtype,
                allow_shared_names: bool,
                skip_index_check: bool,
            ) -> PyResult<()> {
                self.0
                    .set_measurements(
                        measurements.0.inner_into(),
                        allow_shared_names,
                        skip_index_check,
                    )
                    .py_termfail_resolve_nowarn()
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_core_push_measurement(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let (is_dataset, version) = split_ident_version_pycore(&i);

    let otype = pyoptical(version);
    let ttype = pytemporal(version);

    let range_path = keyword_path("Range");
    let ver_shortname_path = versioned_shortname_path(version);
    let any_fcs_col_path = quote!(fireflow_core::validated::dataframe::AnyFCSColumn);
    let shortname_path = shortname_path();

    let push_meas_doc = |is_optical: bool, hasdata: bool| {
        let (meas_type, what) = if is_optical {
            (optical_pytype(version), "optical")
        } else {
            (temporal_pytype(version), "temporal")
        };
        let param_meas = DocArg::new_param(
            "meas".into(),
            meas_type.clone(),
            "The measurement to push.".into(),
        );
        let _param_col = if hasdata { Some(param_col()) } else { None };
        let ps: Vec<_> = [param_meas]
            .into_iter()
            .chain(_param_col)
            .chain([
                param_name("Name of new measurement."),
                param_range(),
                param_notrunc(),
            ])
            .collect();
        let summary = format!("Push {what} measurement to end of measurement vector.");
        DocString::new(summary, vec![], DocSelf::PySelf, ps, None)
    };

    let push_opt_doc = push_meas_doc(true, false);
    let push_tmp_doc = push_meas_doc(false, false);
    let push_opt_data_doc = push_meas_doc(true, true);
    let push_tmp_data_doc = push_meas_doc(false, true);

    let q = if is_dataset {
        quote! {
            #push_opt_data_doc
            fn push_optical(
                &mut self,
                meas: #otype,
                col: #any_fcs_col_path,
                name: #ver_shortname_path,
                range: #range_path,
                notrunc: bool,
            ) -> PyResult<()> {
                self.0
                    .push_optical(name.into(), meas.into(), col, range, notrunc)
                    .py_termfail_resolve()
                    .void()
            }

            #push_tmp_data_doc
            fn push_temporal(
                &mut self,
                meas: #ttype,
                col: #any_fcs_col_path,
                name: #shortname_path,
                range: #range_path,
                notrunc: bool,
            ) -> PyResult<()> {
                self.0
                    .push_temporal(name, meas.into(), col, range, notrunc)
                    .py_termfail_resolve()
            }
        }
    } else {
        quote! {
            #push_opt_doc
            fn push_optical(
                &mut self,
                meas: #otype,
                name: #ver_shortname_path,
                range: #range_path,
                notrunc: bool,
            ) -> PyResult<()> {
                self.0
                    .push_optical(name.into(), meas.into(), range, notrunc)
                    .py_termfail_resolve()
                    .void()
            }

            #push_tmp_doc
            fn push_temporal(
                &mut self,
                meas: #ttype,
                name: #shortname_path,
                range: #range_path,
                notrunc: bool,
            ) -> PyResult<()> {
                self.0
                    .push_temporal(name, meas.into(), range, notrunc)
                    .py_termfail_resolve()
            }
        }
    };

    quote! {
        #[pymethods]
        impl #i {
            #q
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_core_remove_measurement(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let version = split_ident_version_pycore(&i).1;

    let element_path = element_path(version);
    let ver_shortname_path = versioned_shortname_path(version);
    let shortname_path = shortname_path();
    let family_path = versioned_family_path(version);
    let meas_index_path = meas_index_path();

    let by_name_doc = DocString::new(
        "Remove a measurement with a given name.".into(),
        vec!["Raise exception if ``name`` not found.".into()],
        DocSelf::PySelf,
        vec![param_name("Name to remove")],
        Some(DocReturn::new(
            PyType::Tuple(vec![PyType::Int, measurement_pytype(version)]),
            Some("Index and measurement object".into()),
        )),
    );

    let by_index_doc = DocString::new(
        "Remove a measurement with a given index.".into(),
        vec!["Raise exception if ``index`` not found.".into()],
        DocSelf::PySelf,
        vec![param_index("Index to remove")],
        Some(DocReturn::new(
            PyType::Tuple(vec![PyType::Str, measurement_pytype(version)]),
            Some("Name and measurement object".into()),
        )),
    );

    let bare_element_path = quote!(fireflow_core::text::named_vec::Element);

    quote! {
        #[pymethods]
        impl #i {
            #by_name_doc
            fn remove_measurement_by_name(
                &mut self,
                name: #shortname_path,
            ) -> PyResult<(#meas_index_path, #element_path)> {
                Ok(self
                   .0
                   .remove_measurement_by_name(&name)
                   .map(|(i, x)| (i, x.inner_into()))?)
            }

            #by_index_doc
            fn remove_measurement_by_index(
                &mut self,
                index: #meas_index_path,
            ) -> PyResult<(#ver_shortname_path, #element_path)> {
                let r = self.0.remove_measurement_by_index(index)?;
                let (n, v) = #bare_element_path::unzip::<#family_path>(r);
                Ok((n.0, v.inner_into()))
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_core_insert_measurement(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let (is_dataset, version) = split_ident_version_pycore(&i);

    let otype = pyoptical(version);
    let ttype = pytemporal(version);
    let meas_index_path = meas_index_path();
    let shortname_path = shortname_path();
    let ver_shortname_path = versioned_shortname_path(version);
    let range_path = keyword_path("Range");
    let any_fcs_col_path = quote!(fireflow_core::validated::dataframe::AnyFCSColumn);

    let insert_meas_doc = |is_optical: bool, hasdata: bool| {
        let (meas_type, what) = if is_optical {
            (optical_pytype(version), "optical")
        } else {
            (temporal_pytype(version), "temporal")
        };
        let param_meas = DocArg::new_param(
            "meas".into(),
            meas_type.clone(),
            "The measurement to insert.".into(),
        );
        let _param_col = if hasdata { Some(param_col()) } else { None };
        let summary = format!("Insert {what} measurement at position in measurement vector.");
        let ps: Vec<_> = [
            param_index("Position at which to insert new measurement."),
            param_meas,
        ]
        .into_iter()
        .chain(_param_col)
        .chain([
            param_name("Name of new measurement."),
            param_range(),
            param_notrunc(),
        ])
        .collect();
        DocString::new(summary, vec![], DocSelf::PySelf, ps, None)
    };

    let insert_opt_doc = insert_meas_doc(true, false);
    let insert_tmp_doc = insert_meas_doc(false, false);
    let insert_opt_data_doc = insert_meas_doc(true, true);
    let insert_tmp_data_doc = insert_meas_doc(false, true);

    let q = if is_dataset {
        quote! {
            #insert_opt_data_doc
            fn insert_optical(
                &mut self,
                index: #meas_index_path,
                meas: #otype,
                col: #any_fcs_col_path,
                name: #ver_shortname_path,
                range: #range_path,
                notrunc: bool,
            ) -> PyResult<()> {
                self.0
                    .insert_optical(index, name.into(), meas.into(), col, range, notrunc)
                    .py_termfail_resolve()
                    .void()
            }

            #insert_tmp_data_doc
            fn insert_temporal(
                &mut self,
                index: #meas_index_path,
                meas: #ttype,
                col: #any_fcs_col_path,
                name: #shortname_path,
                range: #range_path,
                notrunc: bool,
            ) -> PyResult<()> {
                self.0
                    .insert_temporal(index, name, meas.into(), col, range, notrunc)
                    .py_termfail_resolve()
            }
        }
    } else {
        quote! {
            #insert_opt_doc
            fn insert_optical(
                &mut self,
                index: #meas_index_path,
                meas: #otype,
                name: #ver_shortname_path,
                range: #range_path,
                notrunc: bool,
            ) -> PyResult<()> {
                self.0
                    .insert_optical(index, name.into(), meas.into(), range, notrunc)
                    .py_termfail_resolve()
                    .void()
            }

            #insert_tmp_doc
            fn insert_temporal(
                &mut self,
                index: #meas_index_path,
                meas: #ttype,
                name: #shortname_path,
                range: #range_path,
                notrunc: bool,
            ) -> PyResult<()> {
                self.0
                    .insert_temporal(index, name, meas.into(), range, notrunc)
                    .py_termfail_resolve()
            }
        }
    };

    quote! {
        #[pymethods]
        impl #i {
            #q
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_core_replace_optical(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let version = split_ident_version_pycore(&i).1;

    let otype = pyoptical(version);
    let meas_index_path = meas_index_path();
    let element_path = element_path(version);
    let shortname_path = shortname_path();

    let make_replace_doc = |is_index: bool| {
        let (i, i_param, m) = if is_index {
            (
                "index",
                param_index("Index to replace."),
                "measurement at index",
            )
        } else {
            ("name", param_name("Name to replace."), "named measurement")
        };
        let meas_desc = format!("Optical measurement to replace measurement at ``{i}``.");
        let sub = format!("Raise exception if ``{i}`` does not exist.");
        DocString::new(
            format!("Replace {m} with given optical measurement."),
            vec![sub],
            DocSelf::PySelf,
            vec![
                i_param,
                DocArg::new_param("meas".into(), optical_pytype(version), meas_desc),
            ],
            Some(DocReturn::new(
                measurement_pytype(version),
                Some("Replaced measurement object".into()),
            )),
        )
    };

    let replace_at_doc = make_replace_doc(true);
    let replace_named_doc = make_replace_doc(false);

    quote! {
        #[pymethods]
        impl #i {
            #replace_at_doc
            fn replace_optical_at(
                &mut self,
                index: #meas_index_path,
                meas: #otype,
            ) -> PyResult<#element_path> {
                let ret = self.0.replace_optical_at(index, meas.into())?;
                Ok(ret.inner_into())
            }

            #replace_named_doc
            fn replace_optical_named(
                &mut self,
                name: #shortname_path,
                meas: #otype,
            ) -> Option<#element_path> {
                self.0
                    .replace_optical_named(&name, meas.into())
                    .map(|r| r.inner_into())
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_core_replace_temporal(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let version = split_ident_version_pycore(&i).1;

    let ttype = pytemporal(version);
    let meas_index_path = meas_index_path();
    let element_path = element_path(version);
    let shortname_path = shortname_path();

    let force_param = DocArg::new_param_def(
        "force".into(),
        PyType::Bool,
        "If ``True``, do not raise exception if existing temporal measurement \
         cannot be converted to optical measurement."
            .into(),
        DocDefault::Bool(false),
    );

    // the temporal replacement functions for 3.2 are different because they
    // can fail if $PnTYPE is set
    let (replace_tmp_args, replace_tmp_at_body, replace_tmp_named_body, force) =
        if version == Version::FCS3_2 {
            let go = |fun, x| quote!(self.0.#fun(#x, meas.into(), force).py_termfail_resolve()?);
            (
                quote! {force: bool},
                go(quote! {replace_temporal_at_lossy}, quote! {index}),
                go(quote! {replace_temporal_named_lossy}, quote! {&name}),
                Some(force_param),
            )
        } else {
            (
                quote! {},
                quote! {self.0.replace_temporal_at(index, meas.into())?},
                quote! {self.0.replace_temporal_named(&name, meas.into())},
                None,
            )
        };

    let make_replace_doc = |is_index: bool| {
        let (i, i_param, m) = if is_index {
            (
                "index",
                param_index("Index to replace."),
                "measurement at index",
            )
        } else {
            ("name", param_name("Name to replace."), "named measurement")
        };
        let meas_desc = format!("Temporal measurement to replace measurement at ``{i}``.");
        let sub = format!(
            "Raise exception if ``{i}`` does not exist  or there \
             is already a temporal measurement in a different position."
        );
        let args = [
            i_param,
            DocArg::new_param("meas".into(), temporal_pytype(version), meas_desc),
        ];
        DocString::new(
            format!("Replace {m} with given temporal measurement."),
            vec![sub],
            DocSelf::PySelf,
            args.into_iter().chain(force.clone()).collect(),
            Some(DocReturn::new(
                measurement_pytype(version),
                Some("Replaced measurement object".into()),
            )),
        )
    };

    let replace_at_doc = make_replace_doc(true);
    let replace_named_doc = make_replace_doc(false);

    quote! {
        #[pymethods]
        impl #i {
            #replace_at_doc
            fn replace_temporal_at(
                &mut self,
                index: #meas_index_path,
                meas: #ttype,
                #replace_tmp_args
            ) -> PyResult<#element_path> {
                let ret = #replace_tmp_at_body;
                Ok(ret.inner_into())
            }

            #replace_named_doc
            fn replace_temporal_named(
                &mut self,
                name: #shortname_path,
                meas: #ttype,
                #replace_tmp_args
            ) -> PyResult<Option<#element_path>> {
                let ret = #replace_tmp_named_body;
                Ok(ret.map(|r| r.inner_into()))
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_coretext_from_kws(input: TokenStream) -> TokenStream {
    let path = parse_macro_input!(input as Path);
    let ident = path.segments.last().unwrap().ident.clone();
    let version = split_ident_version_checked("CoreTEXT", &ident);
    let pyname = format_ident!("Py{ident}");

    let std_args = ArgData::std_config_args(version);
    let layout_args = ArgData::layout_config_args(version);
    let shared_args = ArgData::shared_config_args();

    let sk = quote!(fireflow_core::validated::keys::StdKey);
    let nsk = quote!(fireflow_core::validated::keys::NonStdKey);
    let std = quote!(std::collections::HashMap<#sk, String>);
    let nonstd = quote!(std::collections::HashMap<#nsk, String>);

    let fun_args: Vec<_> = std_args
        .iter()
        .chain(layout_args.iter())
        .chain(shared_args.iter())
        .map(|a| a.constr_arg())
        .collect();

    let std_inner_args: Vec<_> = std_args.iter().map(|a| a.inner_arg1()).collect();
    let layout_inner_args: Vec<_> = layout_args.iter().map(|a| a.inner_arg1()).collect();
    let shared_inner_args: Vec<_> = shared_args.iter().map(|a| a.inner_arg1()).collect();

    let std_conf = quote!(fireflow_core::config::StdTextReadConfig);
    let layout_conf = quote!(fireflow_core::config::ReadLayoutConfig);
    let shared_conf = quote!(fireflow_core::config::SharedConfig);
    let core_conf = quote!(fireflow_core::config::NewCoreTEXTConfig);

    let other_kws = if version == Version::FCS2_0 {
        "*$TOT*"
    } else {
        "*$TOT*, *$BEGINDATA*, *$ENDDATA*, *$BEGINANALYSIS*, *$ENDANALYSIS*, \
         or *$TIMESTEP* (if time measurement not included)"
    };
    let no_kws = format!(
        "Must not contain any *$Pn\\** keywords not indexed by \
         *$PAR* or {other_kws}."
    );

    let std_param = DocArg::new_param(
        "std".into(),
        PyType::new_dict(PyType::Str, PyType::Str),
        format!("Standard keywords. {no_kws}"),
    );

    let nonstd_param = DocArg::new_param(
        "nonstd".into(),
        PyType::new_dict(PyType::Str, PyType::Str),
        "Non-Standard keywords.".into(),
    );

    let config_params = std_args
        .iter()
        .chain(layout_args.iter())
        .chain(shared_args.iter())
        .map(|a| a.doc.clone());

    let params = [std_param, nonstd_param]
        .into_iter()
        .chain(config_params)
        .collect();

    let doc = DocString::new(
        "Make new instance from keywords.".into(),
        vec![],
        // NOTE no need to write "cls" in sig (apparently)
        DocSelf::NoSelf,
        params,
        Some(DocReturn::new(PyType::PyClass(ident.to_string()), None)),
    );

    quote! {
        #[pymethods]
        impl #pyname {
            #[classmethod]
            #[allow(clippy::too_many_arguments)]
            #doc
            fn from_kws(
                _: &Bound<'_, pyo3::types::PyType>,
                std: #std,
                nonstd: #nonstd,
                #(#fun_args),*
            ) -> PyResult<Self> {
                let kws = fireflow_core::validated::keys::ValidKeywords { std, nonstd };
                #[allow(clippy::needless_update)]
                let standard = #std_conf {
                    #(#std_inner_args,)*
                    ..#std_conf::default()
                };
                #[allow(clippy::needless_update)]
                let layout = #layout_conf {
                    #(#layout_inner_args,)*
                    ..#layout_conf::default()
                };
                let shared = #shared_conf { #(#shared_inner_args),* };
                let conf = #core_conf { standard, layout, shared };
                Ok(Self(#path::new_from_keywords(kws, &conf).py_termfail_resolve()?))
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_coredataset_from_kws(input: TokenStream) -> TokenStream {
    let path = parse_macro_input!(input as Path);
    let ident = path.segments.last().unwrap().ident.clone();
    let version = split_ident_version_checked("CoreDataset", &ident);
    let pyname = format_ident!("Py{ident}");

    let std_args = ArgData::std_config_args(version);
    let layout_args = ArgData::layout_config_args(version);
    let offsets_args = ArgData::offsets_config_args(version);
    let reader_args = ArgData::reader_config_args();
    let shared_args = ArgData::shared_config_args();

    let config_args: Vec<_> = std_args
        .iter()
        .chain(layout_args.iter())
        .chain(offsets_args.iter())
        .chain(reader_args.iter())
        .chain(shared_args.iter())
        .collect();

    let std_inner_args: Vec<_> = std_args.iter().map(|a| a.inner_arg1()).collect();
    let layout_inner_args: Vec<_> = layout_args.iter().map(|a| a.inner_arg1()).collect();
    let offsets_inner_args: Vec<_> = offsets_args.iter().map(|a| a.inner_arg1()).collect();
    let reader_inner_args: Vec<_> = reader_args.iter().map(|a| a.inner_arg1()).collect();
    let shared_inner_args: Vec<_> = shared_args.iter().map(|a| a.inner_arg1()).collect();

    let std_conf = quote!(fireflow_core::config::StdTextReadConfig);
    let layout_conf = quote!(fireflow_core::config::ReadLayoutConfig);
    let offsets_conf = quote!(fireflow_core::config::ReadTEXTOffsetsConfig);
    let reader_conf = quote!(fireflow_core::config::ReaderConfig);
    let shared_conf = quote!(fireflow_core::config::SharedConfig);
    let core_conf = quote!(fireflow_core::config::ReadStdDatasetFromKeywordsConfig);

    let sk_path = quote!(fireflow_core::validated::keys::StdKey);
    let nsk_path = quote!(fireflow_core::validated::keys::NonStdKey);
    let std_path = quote!(std::collections::HashMap<#sk_path, String>);
    let nonstd_path = quote!(std::collections::HashMap<#nsk_path, String>);

    let data_seg_path = quote!(fireflow_core::segment::HeaderDataSegment);
    let analysis_seg_path = quote!(fireflow_core::segment::HeaderAnalysisSegment);
    let other_segs_path = quote!(Vec<fireflow_core::segment::OtherSegment20>);

    let path_param = path_param(true);

    let std_param = DocArg::new_param(
        "std".into(),
        PyType::new_dict(PyType::Str, PyType::Str),
        "Standard keywords.".into(),
    );

    let nonstd_param = DocArg::new_param(
        "nonstd".into(),
        PyType::new_dict(PyType::Str, PyType::Str),
        "Non-Standard keywords.".into(),
    );

    let data_seg_param = DocArg::new_param(
        "data_seg".into(),
        segment_pytype(),
        "The *DATA* segment from *HEADER*.".into(),
    );

    let analysis_seg_param = DocArg::new_param_def(
        "analysis_seg".into(),
        segment_pytype(),
        "The *ANALYSIS* segment from *HEADER*.".into(),
        DocDefault::Other(quote!(#analysis_seg_path::default()), "(0, 0)".into()),
    );

    let other_segs_param = DocArg::new_param_def(
        "other_segs".into(),
        PyType::new_list(segment_pytype()),
        "The *OTHER* segments from *HEADER*.".into(),
        DocDefault::EmptyList,
    );

    let config_params = config_args.iter().map(|a| a.doc.clone());

    let params = [
        path_param,
        std_param,
        nonstd_param,
        data_seg_param,
        analysis_seg_param,
        other_segs_param,
    ]
    .into_iter()
    .chain(config_params)
    .collect();

    let doc = DocString::new(
        "Make new instance from keywords.".into(),
        vec![],
        DocSelf::NoSelf,
        params,
        Some(DocReturn::new(PyType::PyClass(ident.to_string()), None)),
    );

    let fun_args: Vec<_> = config_args.iter().map(|a| a.constr_arg()).collect();

    quote! {
        #[pymethods]
        impl #pyname {
            #[classmethod]
            #[allow(clippy::too_many_arguments)]
            #doc
            fn from_kws(
                _: &Bound<'_, pyo3::types::PyType>,
                path: std::path::PathBuf,
                std: #std_path,
                nonstd: #nonstd_path,
                data_seg: #data_seg_path,
                analysis_seg: #analysis_seg_path,
                other_segs: #other_segs_path,
                #(#fun_args),*
            // ) -> PyResult<(Self, fireflow_core::core::StdDatasetWithKwsOutput)> {
            ) -> PyResult<Self> {
                let kws = fireflow_core::validated::keys::ValidKeywords { std, nonstd };
                #[allow(clippy::needless_update)]
                let standard = #std_conf {
                    #(#std_inner_args,)*
                    ..#std_conf::default()
                };
                #[allow(clippy::needless_update)]
                let layout = #layout_conf {
                    #(#layout_inner_args,)*
                    ..#layout_conf::default()
                };
                #[allow(clippy::needless_update)]
                let offsets = #offsets_conf {
                    #(#offsets_inner_args,)*
                    ..#offsets_conf::default()
                };
                #[allow(clippy::needless_update)]
                let data = #reader_conf {
                    #(#reader_inner_args,)*
                    ..#reader_conf::default()
                };
                #[allow(clippy::needless_update)]
                let shared = #shared_conf {
                    #(#shared_inner_args,)*
                    ..#shared_conf::default()
                };
                let conf = #core_conf { standard, layout, offsets, data, shared };
                let (core, uncore) = #path::new_from_keywords(
                    path, kws, data_seg, analysis_seg, &other_segs[..], &conf
                ).py_termfail_resolve()?;
                // Ok((Self(core), uncore))
                // TODO return more stuff from this
                Ok(core.into())
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_coretext_unset_measurements(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let _ = split_ident_version_checked("PyCoreTEXT", &i);

    let doc = DocString::new(
        "Remove measurements and clear the layout.".into(),
        vec![
            "This is equivalent to deleting all *$Pn\\** keywords and setting *$PAR* to 0.".into(),
            "Will raise exception if other keywords (such as *$TR*) reference a measurement."
                .into(),
        ],
        DocSelf::PySelf,
        vec![],
        None,
    );

    quote! {
        #[pymethods]
        impl #i {
            #doc
            fn unset_measurements(&mut self) -> PyResult<()> {
                Ok(self.0.unset_measurements()?)
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_coredataset_unset_data(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let _ = split_ident_version_checked("PyCoreDataset", &i);

    let doc = DocString::new(
        "Remove all measurements and their data.".into(),
        vec!["Raise exception if any keywords (such as *$TR*) reference a measurement.".into()],
        DocSelf::PySelf,
        vec![],
        None,
    );

    quote! {
        #[pymethods]
        impl #i {
            #doc
            fn unset_data(&mut self) -> PyResult<()> {
                Ok(self.0.unset_data()?)
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_coredataset_truncate_data(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let _ = split_ident_version_checked("PyCoreDataset", &i);

    let p = DocArg::new_param_def(
        "skip_conv_check".into(),
        PyType::Bool,
        "If ``True``, silently truncate data; otherwise return warnings when \
         truncation is performed."
            .into(),
        DocDefault::Bool(false),
    );

    let doc = DocString::new(
        "Coerce all values in DATA to fit within types specified in layout.".into(),
        vec!["This will always create a new copy of DATA in-place.".into()],
        DocSelf::PySelf,
        vec![p],
        None,
    );

    quote! {
        #[pymethods]
        impl #i {
            #doc
            fn truncate_data(&mut self, skip_conv_check: bool) -> PyResult<()> {
                self.0.truncate_data(skip_conv_check).py_term_resolve_noerror()
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_core_set_measurements_and_layout(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let (is_dataset, version) = split_ident_version_pycore(&i);

    let meas_argtype = ArgData::new_measurements_arg(version).rstype;

    let layout = ArgData::new_layout_arg(version);
    let layout_pytype = layout.doc.pytype;
    let layout_argtype = layout.rstype;

    let param_type_set_layout =
        DocArg::new_param("layout".into(), layout_pytype, "The new layout.".into());

    let s = if is_dataset {
        " and both must match number of columns in existing dataframe"
    } else {
        ""
    };
    let ps = vec![
        "This is equivalent to updating all *$PnN* keywords at once.".into(),
        format!("Length of ``measurements`` must match number of columns in ``layout`` {s}."),
    ];
    let doc = DocString::new(
        "Set all measurements at once.".into(),
        ps,
        DocSelf::PySelf,
        vec![
            param_type_set_meas(version),
            param_type_set_layout.clone(),
            param_allow_shared_names(),
            param_skip_index_check(),
        ],
        None,
    );

    quote! {
        #[pymethods]
        impl #i {
            #doc
            fn set_measurements_and_layout(
                &mut self,
                measurements: #meas_argtype,
                layout: #layout_argtype,
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
                    .py_termfail_resolve_nowarn()
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_coredataset_set_measurements_and_data(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let version = split_ident_version_checked("PyCoreDataset", &i);

    let fcs_df_path = fcs_df_path();

    let meas_argtype = ArgData::new_measurements_arg(version).rstype;

    let df_pytype = ArgData::new_df_arg().doc.pytype;

    let param_type_set_df = DocArg::new_param("df".into(), df_pytype, "The new data.".into());

    let doc = DocString::new(
        "Set measurements and data at once.".into(),
        vec!["Length of ``measurements`` must match number of columns in ``df``.".into()],
        DocSelf::PySelf,
        vec![
            param_type_set_meas(version),
            param_type_set_df,
            param_allow_shared_names(),
            param_skip_index_check(),
        ],
        None,
    );

    quote! {
        #[pymethods]
        impl #i {
            #doc
            fn set_measurements_and_data(
                &mut self,
                measurements: #meas_argtype,
                df: #fcs_df_path,
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
                    .py_termfail_resolve_nowarn()
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_coretext_to_dataset(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let version = split_ident_version_checked("PyCoreTEXT", &i);
    let to_name = format!("CoreDataset{}", version.short_underscore());
    let to_rstype = pycoredataset(version);
    let fcs_df_path = fcs_df_path();

    let df = ArgData::new_df_arg();
    let analysis = ArgData::new_analysis_arg();
    let others = ArgData::new_others_arg();

    let analysis_path = &analysis.rstype;
    let others_path = &others.rstype;

    let doc = DocString::new(
        "Convert to a dataset object.".into(),
        vec!["This will fully represent an FCS file, as opposed to just \
             representing *HEADER* and *TEXT*."
            .into()],
        DocSelf::PySelf,
        vec![df.doc, analysis.doc, others.doc],
        Some(DocReturn::new(PyType::PyClass(to_name), None)),
    );

    quote! {
        #[pymethods]
        impl #i {
            #doc
            fn to_dataset(
                &self,
                df: #fcs_df_path,
                analysis: #analysis_path,
                others: #others_path,
            ) -> PyResult<#to_rstype> {
                Ok(self
                   .0
                   .clone()
                   .into_coredataset(df, analysis, others)?
                   .into())
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_new_meas(input: TokenStream) -> TokenStream {
    let path: Path = syn::parse(input).unwrap();
    let name = path.segments.last().unwrap().ident.clone();
    let (base, version) = split_ident_version(&name);
    let is_temporal = match base.as_str() {
        "Temporal" => true,
        "Optical" => false,
        _ => panic!("must be either Optical or Temporal"),
    };

    let version_us = version.short_underscore();
    let version_s = version.short();

    let fun_ident = format_ident!("new_{version_us}");
    let fun = quote!(#path::#fun_ident);

    let lower_basename = base.to_lowercase();

    let scale = if version == Version::FCS2_0 {
        ArgData::new_scale_arg()
    } else {
        ArgData::new_transform_arg()
    };

    let wavelength = if version < Version::FCS3_1 {
        ArgData::new_meas_kw_opt_arg("Wavelength", "wavelength", "L", PyType::Float)
    } else {
        ArgData::new_meas_kw_opt_arg(
            "Wavelengths",
            "wavelengths",
            "L",
            PyType::new_list(PyType::Float),
        )
    };

    let bin = ArgData::new_meas_kw_arg(
        "PeakBin",
        "bin",
        PyType::Int,
        "Value of *$PKn*.".into(),
        Some(DocDefault::Option),
    );
    let size = ArgData::new_meas_kw_arg(
        "PeakNumber",
        "size",
        PyType::Int,
        "Value of *$PKNn*.".into(),
        Some(DocDefault::Option),
    );

    let all_peak = [bin, size];

    let filter = ArgData::new_meas_kw_opt_arg("Filter", "filter", "F", PyType::Str);

    let power = ArgData::new_meas_kw_opt_arg("Power", "power", "O", PyType::Float);

    let detector_type =
        ArgData::new_meas_kw_opt_arg("DetectorType", "detector_type", "T", PyType::Str);

    let percent_emitted =
        ArgData::new_meas_kw_opt_arg("PercentEmitted", "percent_emitted", "P", PyType::Str);

    let detector_voltage =
        ArgData::new_meas_kw_opt_arg("DetectorVoltage", "detector_voltage", "V", PyType::Float);

    let all_common_optical = [
        filter,
        power,
        detector_type,
        percent_emitted,
        detector_voltage,
    ];

    let calibration3_1 = ArgData::new_meas_kw_arg(
        "Calibration3_1",
        "calibration",
        calibration3_1_pytype(),
        Some("Value of *$PnCALIBRATION*. Tuple encodes slope and calibration units."),
        Some(DocDefault::Option),
    );

    let calibration3_2 = ArgData::new_meas_kw_arg(
        "Calibration3_2",
        "calibration",
        calibration3_2_pytype(),
        Some(
            "Value of *$PnCALIBRATION*. Tuple encodes slope, intercept, \
             and calibration units.",
        ),
        Some(DocDefault::Option),
    );

    let display = ArgData::new_meas_kw_arg(
        "Display",
        "display",
        display_pytype(),
        Some(
            "Value of *$PnD*. First member of tuple encodes linear or log display \
             (``False`` and ``True`` respectively). The float members encode \
             lower/upper and decades/offset for linear and log scaling respectively.",
        ),
        Some(DocDefault::Option),
    );

    let analyte = ArgData::new_meas_kw_opt_arg("Analyte", "analyte", "ANALYTE", PyType::Str);

    let feature = ArgData::new_meas_kw_opt_arg("Feature", "feature", "FEATURE", feature_pytype());

    let detector_name =
        ArgData::new_meas_kw_opt_arg("DetectorName", "detector_name", "DET", PyType::Str);

    let tag = ArgData::new_meas_kw_opt_arg("Tag", "tag", "TAG", PyType::Str);

    let measurement_type =
        ArgData::new_meas_kw_opt_arg("OpticalType", "measurement_type", "TYPE", PyType::Str);

    let has_scale_doc = DocArg::new_ivar_def(
        "has_scale".into(),
        PyType::Bool,
        "``True`` if *$PnE* is set to ``0,0``.".into(),
        DocDefault::Bool(false),
    );
    let has_scale_methods = quote! {
        #[getter]
        fn get_has_scale(&self) -> bool {
            self.0.specific.scale.0.is_some()
        }

        #[setter]
        fn set_has_scale(&mut self, has_scale: bool) {
            self.0.specific.scale = if has_scale {
                Some(fireflow_core::text::keywords::TemporalScale)
            } else {
                None
            }.into();
        }
    };
    let has_scale = ArgData::new(has_scale_doc, parse_quote!(bool), Some(has_scale_methods));

    let has_type_doc = DocArg::new_ivar_def(
        "has_type".into(),
        PyType::Bool,
        "``True`` if *$PnTYPE* is set to ``Time``.".into(),
        DocDefault::Bool(false),
    );
    let has_type_methods = quote! {
        #[getter]
        fn get_has_type(&self) -> bool {
            self.0.specific.measurement_type.0.is_some()
        }

        #[setter]
        fn set_has_type(&mut self, has_type: bool) {
            self.0.specific.measurement_type = if has_type {
                Some(fireflow_core::text::keywords::TemporalType)
            } else {
                None
            }.into();
        }
    };
    let has_type = ArgData::new(has_type_doc, parse_quote!(bool), Some(has_type_methods));

    let timestep_path = keyword_path("Timestep");
    let timestep_doc = DocArg::new_ivar(
        "timestep".into(),
        PyType::Float,
        "Value of *$TIMESTEP*.".into(),
    );
    let timestep = ArgData::new1(timestep_doc, timestep_path);

    let longname = ArgData::new_meas_kw_opt_arg("Longname", "longname", "S", PyType::Str);

    let nonstd = ArgData::new_meas_nonstandard_keywords_arg();

    let all_common = [longname, nonstd];

    let all_args: Vec<_> = match (version, is_temporal) {
        (Version::FCS2_0, true) => [has_scale]
            .into_iter()
            .chain(all_peak)
            .chain(all_common)
            .collect(),
        (Version::FCS3_0, true) => [timestep]
            .into_iter()
            .chain(all_peak)
            .chain(all_common)
            .collect(),
        (Version::FCS3_1, true) => [timestep, display]
            .into_iter()
            .chain(all_peak)
            .chain(all_common)
            .collect(),
        (Version::FCS3_2, true) => [timestep, display]
            .into_iter()
            .chain([has_type])
            .chain(all_common)
            .collect(),
        (Version::FCS2_0, false) => [scale, wavelength]
            .into_iter()
            .chain(all_peak)
            .chain(all_common_optical)
            .chain(all_common)
            .collect(),
        (Version::FCS3_0, false) => [scale, wavelength]
            .into_iter()
            .chain(all_peak)
            .chain(all_common_optical)
            .chain(all_common)
            .collect(),
        (Version::FCS3_1, false) => [scale, wavelength, calibration3_1, display]
            .into_iter()
            .chain(all_peak)
            .chain(all_common_optical)
            .chain(all_common)
            .collect(),
        (Version::FCS3_2, false) => [
            scale,
            wavelength,
            calibration3_2,
            display,
            analyte,
            feature,
            tag,
            measurement_type,
            detector_name,
        ]
        .into_iter()
        .chain(all_common_optical)
        .chain(all_common)
        .collect(),
    };

    let params = all_args.iter().map(|x| x.doc.clone()).collect();

    let funargs: Vec<_> = all_args.iter().map(|x| x.constr_arg()).collect();

    let inner_args: Vec<_> = all_args.iter().map(|x| x.inner_arg()).collect();

    let ivar_methods: Vec<_> = all_args.iter().flat_map(|x| &x.methods).collect();

    let doc = DocString::new(
        format!("FCS {version_s} *$Pn\\** keywords for {lower_basename} measurement."),
        vec![],
        DocSelf::PySelf,
        params,
        None,
    );

    let get_set_timestep = if version != Version::FCS2_0 && is_temporal {
        let t = keyword_path("Timestep");
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

    let new_method = quote! {
        fn new(#(#funargs),*) -> Self {
            #fun(#(#inner_args),*).into()
        }
    };

    let rest = quote! {
        #get_set_timestep
        #(#ivar_methods)*
    };

    impl_new(name.to_string(), path, doc, new_method, rest)
        .1
        .into()
}

struct NewCoreInfo {
    coretext_path: Path,
    coredataset_path: Path,
    coretext_name: Ident,
    coredataset_name: Ident,
    version: Version,
}

impl Parse for NewCoreInfo {
    fn parse(input: ParseStream) -> Result<Self> {
        let coretext_path = input.parse::<Path>()?;
        let _: Comma = input.parse()?;
        let coredataset_path = input.parse::<Path>()?;
        let coretext_name = coretext_path.segments.last().unwrap().ident.clone();
        let coredataset_name = coredataset_path.segments.last().unwrap().ident.clone();
        let v0 = split_ident_version_checked("CoreTEXT", &coretext_name);
        let v1 = split_ident_version_checked("CoreDataset", &coredataset_name);
        if v0 != v1 {
            panic!("Versions don't match");
        }
        Ok(Self {
            coretext_path,
            coredataset_path,
            coretext_name,
            coredataset_name,
            version: v0,
        })
    }
}

#[derive(new)]
struct ArgData {
    doc: DocArg,
    rstype: Path,
    methods: Option<proc_macro2::TokenStream>,
}

impl ArgData {
    fn new1(doc: DocArg, rstype: Path) -> Self {
        Self {
            doc,
            rstype,
            methods: None,
        }
    }

    fn constr_arg(&self) -> proc_macro2::TokenStream {
        let n = format_ident!("{}", &self.doc.argname);
        let t = &self.rstype;
        quote!(#n: #t)
    }

    fn inner_arg(&self) -> proc_macro2::TokenStream {
        let n = format_ident!("{}", &self.doc.argname);
        if unwrap_generic("Option", &self.rstype).1 {
            quote! {#n.map(|x| x.into())}
        } else {
            quote! {#n.into()}
        }
    }

    fn inner_arg1(&self) -> proc_macro2::TokenStream {
        // let n = format_ident!("{}", &self.doc.argname);
        // quote! {#n: #n.into()}
        let n = format_ident!("{}", &self.doc.argname);
        if unwrap_generic("Option", &self.rstype).1 {
            quote! {#n: #n.map(|x| x.into())}
        } else {
            quote! {#n: #n.into()}
        }
    }

    fn new_measurements_arg(version: Version) -> Self {
        let (fam_ident, name_pytype) = if version < Version::FCS3_1 {
            (format_ident!("MaybeFamily"), PyType::new_opt(PyType::Str))
        } else {
            (format_ident!("AlwaysFamily"), PyType::Str)
        };
        let fam_path = quote!(fireflow_core::text::optional::#fam_ident);
        let meas_opt_name = format!("Optical{}", version.short_underscore());
        let meas_tmp_name = format!("Temporal{}", version.short_underscore());
        let meas_opt_pyname = format_ident!("Py{meas_opt_name}");
        let meas_tmp_pyname = format_ident!("Py{meas_tmp_name}");
        let meas_pytype = PyType::Tuple(vec![
            name_pytype,
            PyType::new_union2(
                PyType::PyClass(meas_tmp_name),
                PyType::PyClass(meas_opt_name),
            ),
        ]);
        let meas_desc = "Measurements corresponding to columns in FCS file. \
                     Temporal must be given zero or one times.";
        let meas_doc =
            DocArg::new_param("measurements".into(), meas_pytype.clone(), meas_desc.into());
        let meas_argtype: Path =
            parse_quote!(PyEithers<#fam_path, #meas_tmp_pyname, #meas_opt_pyname>);

        Self::new1(meas_doc, meas_argtype.clone())
    }

    fn new_kw_arg(
        kw: &str,
        name: &str,
        pytype: PyType,
        desc: Option<&str>,
        def: Option<DocDefault>,
    ) -> Self {
        let path = keyword_path(kw);
        let get = format_ident!("get_{name}");
        let set = format_ident!("set_{name}");

        let _desc = desc.map_or(format!("Value of *${}*.", name.to_uppercase()), |d| {
            d.to_string()
        });

        let (optional, doc) = if let Some(d) = def {
            let optional = matches!(d, DocDefault::Option);
            let t = if optional {
                PyType::new_opt(pytype)
            } else {
                pytype
            };
            let a = DocArg::new_ivar_def(name.to_string(), t, _desc, d);
            (optional, a)
        } else {
            let a = DocArg::new_ivar(name.to_string(), pytype, _desc);
            (false, a)
        };

        let get_inner = format_ident!("{}", if optional { "metaroot_opt" } else { "metaroot" });
        let clone_inner = format_ident!("{}", if optional { "cloned" } else { "clone" });
        let full_kw = if optional {
            parse_quote! {Option<#path>}
        } else {
            path.clone()
        };

        let methods = quote! {
            #[getter]
            fn #get(&self) -> #full_kw {
                self.0.#get_inner::<#path>().#clone_inner()
            }

            #[setter]
            fn #set(&mut self, x: #full_kw) {
                self.0.set_metaroot(x)
            }
        };
        Self::new(doc, full_kw, Some(methods))
    }

    fn new_meas_kw_arg(
        kw: &str,
        name: &str,
        pytype: PyType,
        desc: Option<&str>,
        def: Option<DocDefault>,
    ) -> Self {
        let path = keyword_path(kw);
        let get = format_ident!("get_{name}");
        let set = format_ident!("set_{name}");

        let _desc = desc.map_or(format!("Value of *${}*.", name.to_uppercase()), |d| {
            d.to_string()
        });

        let (optional, doc) = if let Some(d) = def {
            let optional = matches!(d, DocDefault::Option);
            let t = if optional {
                PyType::new_opt(pytype)
            } else {
                pytype
            };
            let a = DocArg::new_ivar_def(name.to_string(), t, _desc, d);
            (optional, a)
        } else {
            let a = DocArg::new_ivar(name.to_string(), pytype, _desc);
            (false, a)
        };

        let full_kw = if optional {
            parse_quote! {Option<#path>}
        } else {
            path.clone()
        };

        let methods = quote! {
            #[getter]
            fn #get(&self) -> #full_kw {
                let x: &#full_kw = self.0.as_ref();
                x.as_ref().cloned()
            }

            #[setter]
            fn #set(&mut self, x: #full_kw) {
                *self.0.as_mut() = x
            }
        };
        Self::new(doc, full_kw, Some(methods))
    }

    fn new_kw_opt_arg(kw: &str, name: &str, pytype: PyType) -> Self {
        Self::new_kw_arg(kw, name, pytype, None, Some(DocDefault::Option))
    }

    fn new_meas_kw_opt_arg(kw: &str, name: &str, abbr: &str, pytype: PyType) -> Self {
        let desc = format!("Value for *$Pn{abbr}*.");
        Self::new_meas_kw_arg(
            kw,
            name,
            pytype,
            Some(desc.as_str()),
            Some(DocDefault::Option),
        )
    }

    fn new_layout_arg(version: Version) -> Self {
        let non_mixed_layouts = [
            "AsciiFixedLayout",
            "AsciiDelimLayout",
            "EndianUintLayout",
            "EndianF32Layout",
            "EndianF64Layout",
        ];

        let ordered_layouts = [
            "AsciiFixedLayout",
            "AsciiDelimLayout",
            "OrderedUint08Layout",
            "OrderedUint16Layout",
            "OrderedUint24Layout",
            "OrderedUint32Layout",
            "OrderedUint40Layout",
            "OrderedUint48Layout",
            "OrderedUint56Layout",
            "OrderedUint64Layout",
            "OrderedF32Layout",
            "OrderedF64Layout",
        ];

        let (layout_name, layout_pytype) = match version {
            Version::FCS3_2 => {
                let ys = non_mixed_layouts
                    .iter()
                    .chain(&["MixedLayout"])
                    .map(|x| PyType::PyClass(x.to_string()))
                    .collect();
                ("PyLayout3_2", PyType::new_union(ys))
            }
            Version::FCS3_1 => {
                let ys = non_mixed_layouts
                    .iter()
                    .map(|x| PyType::PyClass(x.to_string()))
                    .collect();
                ("PyNonMixedLayout", PyType::new_union(ys))
            }
            _ => {
                let ys = ordered_layouts
                    .iter()
                    .map(|x| PyType::PyClass(x.to_string()))
                    .collect();
                ("PyOrderedLayout", PyType::new_union(ys))
            }
        };
        let layout_ident = format_ident!("{layout_name}");
        let layout_argname = format_ident!("layout");
        let layout_desc = if version == Version::FCS3_2 {
            "Layout to describe data encoding. Represents *$PnB*, *$PnR*, *$BYTEORD*, \
             *$DATATYPE*, and *$PnDATATYPE*."
        } else {
            "Layout to describe data encoding. Represents *$PnB*, *$PnR*, *$BYTEORD*, \
             and *$DATATYPE*."
        };

        let layout_doc = DocArg::new_ivar(
            layout_argname.to_string(),
            layout_pytype.clone(),
            layout_desc.into(),
        );

        let methods = quote! {
            #[getter]
            fn get_layout(&self) -> #layout_ident {
                self.0.layout().clone().into()
            }

            #[setter]
            fn set_layout(&mut self, layout: #layout_ident) -> PyResult<()> {
                self.0.set_layout(layout.into()).py_termfail_resolve_nowarn()
            }
        };

        Self::new(layout_doc, parse_quote!(#layout_ident), Some(methods))
    }

    fn new_df_arg() -> Self {
        // TODO fix cross-ref in docs here
        let df_pytype = PyType::PyClass("polars.DataFrame".into());
        let df_doc = DocArg::new_param(
            "df".into(),
            df_pytype.clone(),
            "A dataframe encoding the contents of *DATA*. Number of columns must \
             match number of measurements. May be empty. Types do not necessarily \
             need to correspond to those in the data layout but mismatches may \
             result in truncation."
                .into(),
        );
        // use polars df here because we need to manually add names
        let polars_df_type = quote! {pyo3_polars::PyDataFrame};
        let methods = quote! {
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
        };

        Self::new(df_doc, fcs_df_path(), Some(methods))
    }

    fn new_analysis_arg() -> Self {
        let analysis_rstype = parse_quote!(fireflow_core::core::Analysis);
        let analysis_doc = DocArg::new_ivar_def(
            "analysis".into(),
            PyType::Bytes,
            "A byte string encoding the *ANALYSIS* segment".into(),
            DocDefault::Other(quote! {#analysis_rstype::default()}, "b\"\"".to_string()),
        );
        let methods = quote! {
            #[getter]
            fn analysis(&self) -> #analysis_rstype {
                self.0.analysis.clone()
            }

            #[setter]
            fn set_analysis(&mut self, xs: #analysis_rstype) {
                self.0.analysis = xs.into();
            }
        };
        Self::new(analysis_doc, analysis_rstype, Some(methods))
    }

    fn new_others_arg() -> Self {
        let others_rstype = parse_quote!(fireflow_core::core::Others);
        let others_doc = DocArg::new_ivar_def(
            "others".into(),
            PyType::new_list(PyType::Bytes),
            "A list of byte strings encoding the *OTHER* segments".into(),
            DocDefault::Other(quote!(#others_rstype::default()), "[]".to_string()),
        );
        let methods = quote! {
            #[getter]
            fn others(&self) -> #others_rstype {
                self.0.others.clone()
            }

            #[setter]
            fn set_others(&mut self, xs: #others_rstype) {
                self.0.others = xs
            }
        };
        Self::new(others_doc, others_rstype, Some(methods))
    }

    fn new_timestamps_args(time_name: &str) -> [Self; 3] {
        let nd = quote! {Option<chrono::NaiveDate>};
        let time_ident = format_ident!("{time_name}");
        let time_path = quote!(fireflow_core::text::timestamps::#time_ident);
        let date_rstype = parse_quote!(Option<fireflow_core::text::timestamps::FCSDate>);

        let make_time_ivar = |is_start: bool| {
            let nt = quote! {Option<chrono::NaiveTime>};
            let (name, wrap) = if is_start {
                ("btim", "Btim")
            } else {
                ("etim", "Etim")
            };
            let wrap_ident = format_ident!("{wrap}");
            let wrap_path = quote!(fireflow_core::text::timestamps::#wrap_ident);
            let rstype = parse_quote!(Option<#wrap_path<#time_path>>);
            let get = format_ident!("get_{name}");
            let set = format_ident!("set_{name}");
            let get_naive = format_ident!("{name}_naive");
            let set_naive = format_ident!("set_{name}_naive");
            let desc = format!("Value of *${}*.", name.to_uppercase());
            let doc = DocArg::new_ivar_def(
                name.into(),
                PyType::new_opt(PyType::Time),
                desc,
                DocDefault::Option,
            );
            let methods = quote! {
                #[getter]
                fn #get(&self) -> #nt {
                    self.0.#get_naive()
                }

                #[setter]
                fn #set(&mut self, x: #nt) -> PyResult<()> {
                    Ok(self.0.#set_naive(x)?)
                }
            };
            Self::new(doc, rstype, Some(methods))
        };

        let date_doc = DocArg::new_ivar_def(
            "date".into(),
            PyType::new_opt(PyType::Date),
            "Value of *$DATE*.".into(),
            DocDefault::Option,
        );

        let date_methods = quote! {
            #[getter]
            fn get_date(&self) -> #nd {
                self.0.date_naive()
            }

            #[setter]
            fn set_date(&mut self, x: #nd) -> PyResult<()> {
                Ok(self.0.set_date_naive(x)?)
            }
        };

        let date = Self::new(date_doc, date_rstype, Some(date_methods));

        [make_time_ivar(true), make_time_ivar(false), date]
    }

    fn new_datetime_arg(is_start: bool) -> Self {
        let dt = quote! {Option<chrono::DateTime<chrono::FixedOffset>>};
        let (name, type_name) = if is_start {
            ("begindatetime", "BeginDateTime")
        } else {
            ("enddatetime", "EndDateTime")
        };
        let type_ident = format_ident!("{type_name}");
        let rstype = parse_quote!(Option<fireflow_core::text::datetimes::#type_ident>);
        let get = format_ident!("{name}");
        let set = format_ident!("set_{name}");
        let doc = DocArg::new_ivar_def(
            name.into(),
            PyType::new_opt(PyType::Datetime),
            format!("Value for *${}*.", name.to_uppercase()),
            DocDefault::Option,
        );
        let methods = quote! {
            #[getter]
            fn #get(&self) -> #dt {
                self.0.#get()
            }

            #[setter]
            fn #set(&mut self, x: #dt) -> PyResult<()> {
                Ok(self.0.#set(x)?)
            }
        };
        Self::new(doc, rstype, Some(methods))
    }

    fn new_comp_arg(is_2_0: bool) -> Self {
        let rstype = parse_quote!(Option<fireflow_core::text::compensation::Compensation>);
        let methods = quote! {
            #[getter]
            fn get_compensation(&self) -> #rstype {
                self.0.compensation().cloned()
            }

            #[setter]
            fn set_compensation(&mut self, m: #rstype) -> PyResult<()> {
                Ok(self.0.set_compensation(m)?)
            }
        };
        let desc = if is_2_0 {
            "The compensation matrix. Must be a square array with number of \
         rows/columns equal to the number of measurements. Non-zero entries \
         will produce a *$DFCmTOn* keyword."
        } else {
            "The value of *$COMP*. Must be a square array with number of \
         rows/columns equal to the number of measurements."
        }
        .into();
        let doc = DocArg::new_ivar_def(
            "comp".into(),
            PyType::new_opt(PyType::PyClass("~numpy.ndarray".into())),
            desc,
            DocDefault::Option,
        );
        Self::new(doc, rstype, Some(methods))
    }

    fn new_spillover_arg() -> Self {
        let rstype = parse_quote!(Option<fireflow_core::text::spillover::Spillover>);
        let methods = quote! {
            #[getter]
            fn get_spillover(&self) -> #rstype {
                self.0.spillover().map(|x| x.clone())
            }

            #[setter]
            fn set_spillover(&mut self, spillover: #rstype) -> PyResult<()> {
                Ok(self.0.set_spillover(spillover)?)
            }
        };
        let doc = DocArg::new_ivar_def(
            "spillover".into(),
            PyType::new_opt(PyType::Tuple(vec![
                PyType::new_list(PyType::Str),
                PyType::PyClass("~numpy.ndarray".into()),
            ])),
            "Value for *$SPILLOVER*. First element of tuple the list of measurement \
         names and the second is the matrix. Each measurement name must \
         correspond to a *$PnN*, must be unique, and the length of this list \
         must match the number of rows and columns of the matrix. The matrix \
         must be at least 2x2."
                .into(),
            DocDefault::Option,
        );
        Self::new(doc, rstype, Some(methods))
    }

    fn new_csvflags_arg() -> Self {
        let path = quote!(fireflow_core::core::CSVFlags);
        let rstype = parse_quote!(Option<#path>);
        let doc = DocArg::new_ivar_def(
            "csvflags".into(),
            PyType::new_opt(PyType::new_list(PyType::new_opt(PyType::Int))),
            "Subset flags. Each element in the list corresponds to *$CSVnFLAG* and \
         the length of the list corresponds to *$CSMODE*."
                .into(),
            DocDefault::Option,
        );
        let methods = quote! {
            #[getter]
            fn get_csvflags(&self) -> #rstype {
                self.0.metaroot_opt::<#path>().cloned()
            }

            #[setter]
            fn set_csvflags(&mut self, x: #rstype) {
                self.0.set_metaroot(x)
            }
        };

        Self::new(doc, rstype, Some(methods))
    }

    fn new_trigger_arg() -> Self {
        let path = keyword_path("Trigger");
        let rstype = parse_quote! {Option<#path>};

        let doc = DocArg::new_ivar_def(
            "tr".into(),
            PyType::new_opt(PyType::Tuple(vec![PyType::Int, PyType::Str])),
            "Value for *$TR*. First member of tuple is threshold and second is the \
             measurement name which must match a *$PnN*."
                .into(),
            DocDefault::Option,
        );

        let methods = quote! {
            #[getter]
            fn trigger(&self) -> #rstype {
                self.0.metaroot_opt().cloned()
            }

            #[setter]
            fn set_trigger(&mut self, tr: #rstype) -> PyResult<()> {
                Ok(self.0.set_trigger(tr)?)
            }
        };

        Self::new(doc, rstype, Some(methods))
    }

    fn new_unstainedcenters_arg() -> Self {
        let doc = DocArg::new_ivar_def(
            "unstainedcenters".into(),
            PyType::new_opt(PyType::new_dict(PyType::Str, PyType::Float)),
            "Value for *$UNSTAINEDCENTERS. Each key must match a *$PnN*.".into(),
            DocDefault::Option,
        );
        let path = quote!(fireflow_core::text::unstainedcenters::UnstainedCenters);
        let rstype = parse_quote!(Option<#path>);
        let methods = quote! {
            #[getter]
            fn get_unstained_centers(&self) -> #rstype {
                self.0.metaroot_opt::<#path>().map(|y| y.clone())
            }

            #[setter]
            fn set_unstained_centers(&mut self, us: #rstype) -> PyResult<()> {
                self.0.set_unstained_centers(us).py_termfail_resolve_nowarn()
            }
        };
        Self::new(doc, rstype, Some(methods))
    }

    fn new_applied_gates_arg(version: Version) -> Self {
        let collapsed_version = if version == Version::FCS3_1 {
            Version::FCS3_0
        } else {
            version
        };
        let vsu = collapsed_version.short_underscore();
        let rstype_inner = format_ident!("AppliedGates{vsu}");
        let rstype = format_ident!("Py{rstype_inner}");
        let gmtype = if collapsed_version < Version::FCS3_2 {
            Some(PyType::new_list(PyType::PyClass("GatedMeasurement".into())))
        } else {
            None
        };
        let urtype = PyType::PyClass(format!("UnivariateRegion{vsu}"));
        let bvtype = PyType::PyClass(format!("BivariateRegion{vsu}"));
        let rtype = PyType::new_dict(PyType::Int, PyType::new_union2(urtype, bvtype));
        let gtype = PyType::new_opt(PyType::Str);
        let pytype = PyType::Tuple(gmtype.into_iter().chain([rtype, gtype]).collect());

        let desc = if collapsed_version == Version::FCS2_0 {
            "Value for *$Gm*/$RnI/$RnW/$GATING/$GATE* keywords. The first member of \
         the tuple corresponds to the *$Gm\\** keywords, where *m* is given by \
         position in the list. The second member corresponds to the *$RnI* and \
         *$RnW* keywords and is a mapping of regions and windows to be used in \
         gating scheme. Keys in dictionary are the region indices (the *n* in \
         *$RnI* and *$RnW*). The values in the dictionary are either univariate \
         or bivariate gates and must correspond to an index in the list in the \
         first element. The third member corresponds to the *$GATING* keyword. \
         All 'Rn' in this string must reference a key in the dict of the second \
         member."
        } else if collapsed_version < Version::FCS3_2 {
            "Value for *$Gm*/$RnI/$RnW/$GATING/$GATE* keywords. The first member of \
         the tuple corresponds to the *$Gm\\** keywords, where *m* is given by \
         position in the list. The second member corresponds to the *$RnI* and \
         *$RnW* keywords and is a mapping of regions and windows to be used in \
         gating scheme. Keys in dictionary are the region indices (the *n* in \
         *$RnI* and *$RnW*). The values in the dictionary are either univariate \
         or bivariate gates and must correspond to an index in the list in the \
         first element or a physical measurement. The third member corresponds \
         to the *$GATING* keyword. All 'Rn' in this string must reference a key \
         in the dict of the second member."
        } else {
            "Value for *$RnI/$RnW/$GATING* keywords. The first member corresponds to \
         the *$RnI* and *$RnW* keywords and is a mapping of regions and windows \
         to be used in gating scheme. Keys in dictionary are the region indices \
         (the *n* in *$RnI* and *$RnW*). The values in the dictionary are either \
         univariate or bivariate gates and must correspond to a physical \
         measurement. The second member corresponds to the *$GATING* keyword. \
         All 'Rn' in this string must reference a key in the dict of the first \
         member."
        }
        .into();

        let pydef = if version < Version::FCS3_2 {
            "([], {}, None)"
        } else {
            "({}, None)"
        };

        let def = DocDefault::Other(quote!(#rstype::default()), pydef.into());

        let doc = DocArg::new_ivar_def("applied_gates".into(), pytype, desc, def);

        let setter_body = if collapsed_version == Version::FCS2_0 {
            quote! {
                fn set_applied_gates(&mut self, ag: #rstype) {
                    self.0.set_metaroot::<#rstype_inner>(ag.into())
                }
            }
        } else {
            let setter = format_ident!("set_applied_gates_{vsu}");
            quote! {
                fn set_applied_gates(&mut self, ag: #rstype) -> PyResult<()> {
                    Ok(self.0.#setter(ag.into())?)
                }
            }
        };

        let methods = quote! {
            #[getter]
            fn get_applied_gates(&self) -> #rstype {
                self.0.metaroot::<#rstype_inner>().clone().into()
            }

            #[setter]
            #setter_body
        };

        Self::new(doc, parse_quote!(#rstype), Some(methods))
    }

    fn new_scale_arg() -> Self {
        let doc = DocArg::new_ivar_def(
            "scale".into(),
            PyType::new_opt(PyType::Tuple(vec![
                PyType::new_unit(),
                PyType::Tuple(vec![PyType::Float, PyType::Float]),
            ])),
            "Value for *$PnE*. Empty tuple means linear scale; 2-tuple encodes \
             decades and offset for log scale"
                .into(),
            DocDefault::Option,
        );
        let rstype = parse_quote! {Option<fireflow_core::text::scale::Scale>};
        let methods = quote! {
            #[getter]
            fn get_scale(&self) -> #rstype {
                self.0.specific.scale.0.as_ref().map(|&x| x)
            }

            #[setter]
            fn set_scale(&mut self, x: #rstype) {
                self.0.specific.scale = x.into()
            }
        };
        Self::new(doc, rstype, Some(methods))
    }

    fn new_transform_arg() -> Self {
        let doc = DocArg::new_ivar(
            "transform".into(),
            PyType::Tuple(vec![
                PyType::Float,
                PyType::Tuple(vec![PyType::Float, PyType::Float]),
            ]),
            "Value for *$PnE* and/or *$PnG*. Singleton float encodes gain (*$PnG*) \
             and implies linear scaling (ie *$PnE* is ``0,0``). 2-tuple encodes \
             decades and offset for log scale, and implies *$PnG* is not set."
                .into(),
        );
        let rstype = parse_quote! {fireflow_core::core::ScaleTransform};
        let methods = quote! {
            #[getter]
            fn get_transform(&self) -> #rstype {
                self.0.specific.scale
            }

            #[setter]
            fn set_transform(&mut self, transform: #rstype) {
                self.0.specific.scale = transform;
            }
        };
        Self::new(doc, rstype, Some(methods))
    }

    fn new_core_nonstandard_keywords_arg() -> Self {
        Self::new_nonstandard_keywords_arg(
            "Pairs of non-standard keyword values. Keys must not start with *$*.",
            quote!(self.0.metaroot),
        )
    }

    fn new_meas_nonstandard_keywords_arg() -> Self {
        Self::new_nonstandard_keywords_arg(
            "Any non-standard keywords corresponding to this measurement. No keys \
             should start with *$*. Realistically each key should follow a pattern \
             corresponding to the measurement index, something like prefixing with \
             \"P\" followed by the index. This is not enforced.",
            quote!(self.0.common),
        )
    }

    fn new_nonstandard_keywords_arg(desc: &str, root: proc_macro2::TokenStream) -> Self {
        let nsk = quote!(fireflow_core::validated::keys::NonStdKey);
        let fun_arg = parse_quote!(std::collections::HashMap<#nsk, String>);
        let doc = DocArg::new_ivar_def(
            "nonstandard_keywords".into(),
            PyType::new_dict(PyType::Str, PyType::Str),
            desc.into(),
            DocDefault::EmptyDict,
        );
        let methods = quote! {
            #[getter]
            fn get_nonstandard_keywords(&self) -> #fun_arg {
                #root.nonstandard_keywords.clone()
            }

            #[setter]
            fn set_nonstandard_keywords(&mut self, kws: #fun_arg) {
                #root.nonstandard_keywords = kws;
            }
        };
        Self::new(doc, fun_arg, Some(methods))
    }

    fn new_config_arg(
        name: String,
        pytype: PyType,
        desc: String,
        def: DocDefault,
        rstype: Path,
    ) -> Self {
        Self::new1(DocArg::new_param_def(name, pytype, desc, def), rstype)
    }

    fn new_config_bool_arg(name: String, desc: String) -> Self {
        ArgData::new_config_arg(
            name,
            PyType::Bool,
            desc,
            DocDefault::Bool(false),
            parse_quote!(bool),
        )
    }

    fn new_config_opt_arg(name: String, pytype: PyType, desc: String, path: Path) -> Self {
        ArgData::new_config_arg(
            name,
            PyType::new_opt(pytype),
            desc,
            DocDefault::Option,
            parse_quote!(Option<#path>),
        )
    }

    fn header_config_args() -> Vec<Self> {
        vec![
            ArgData::text_correction_arg(),
            ArgData::data_correction_arg(),
            ArgData::analysis_correction_arg(),
            ArgData::other_corrections_arg(),
            ArgData::max_other_arg(),
            ArgData::other_width_arg(),
            ArgData::squish_offsets_arg(),
            ArgData::allow_negative_arg(),
            ArgData::truncate_offsets_arg(),
        ]
    }

    fn std_config_args(version: Version) -> Vec<Self> {
        let trim_intra_value_whitespace = ArgData::trim_intra_value_whitespace();
        let time_meas_pattern = ArgData::time_meas_pattern_arg();
        let allow_missing_time = ArgData::allow_missing_time_arg();
        let force_time_linear = ArgData::force_time_linear_arg();
        let ignore_time_gain = ArgData::ignore_time_gain_arg();
        let ignore_time_optical_keys = ArgData::ignore_time_optical_keys_arg();
        let parse_indexed_spillover = ArgData::parse_indexed_spillover_arg();
        let date_pattern = ArgData::date_pattern_arg();
        let time_pattern = ArgData::time_pattern_arg();
        let allow_pseudostandard = ArgData::allow_pseudostandard_arg();
        let allow_unused_standard = ArgData::allow_unused_standard_arg();
        let disallow_deprecated = ArgData::disallow_deprecated_arg();
        let fix_log_scale_offsets = ArgData::fix_log_scale_offsets_arg();
        let nonstandard_measurement_pattern = ArgData::nonstandard_measurement_pattern_arg();

        let std_common_args = [
            trim_intra_value_whitespace,
            time_meas_pattern,
            allow_missing_time,
            force_time_linear,
            ignore_time_optical_keys,
            date_pattern,
            time_pattern,
            allow_pseudostandard,
            allow_unused_standard,
            disallow_deprecated,
            fix_log_scale_offsets,
            nonstandard_measurement_pattern,
        ]
        .into_iter();

        match version {
            Version::FCS2_0 => std_common_args.collect(),
            Version::FCS3_0 => std_common_args.chain([ignore_time_gain]).collect(),
            _ => std_common_args
                .chain([ignore_time_gain, parse_indexed_spillover])
                .collect(),
        }
    }

    fn layout_config_args(version: Version) -> Vec<Self> {
        let integer_widths_from_byteord = ArgData::integer_widths_from_byteord_arg();
        let integer_byteord_override = ArgData::integer_byteord_override_arg();
        let disallow_range_truncation = ArgData::disallow_range_truncation_arg();

        match version {
            Version::FCS2_0 | Version::FCS3_0 => [
                integer_widths_from_byteord,
                integer_byteord_override,
                disallow_range_truncation,
            ]
            .into_iter()
            .collect(),
            _ => [disallow_range_truncation].into_iter().collect(),
        }
    }

    fn offsets_config_args(version: Version) -> Vec<Self> {
        let text_data_correction = ArgData::text_data_correction();
        let text_analysis_correction = ArgData::text_analysis_correction();
        let ignore_text_data_offsets = ArgData::ignore_text_data_offsets();
        let ignore_text_analysis_offsets = ArgData::ignore_text_analysis_offsets();
        let allow_header_text_offset_mismatch = ArgData::allow_header_text_offset_mismatch();
        let allow_missing_required_offsets = ArgData::allow_missing_required_offsets(version);
        let truncate_text_offsets = ArgData::truncate_text_offsets();

        match version {
            // none of these apply to 2.0 since there are no offsets in TEXT
            Version::FCS2_0 => vec![],
            _ => vec![
                text_data_correction,
                text_analysis_correction,
                ignore_text_data_offsets,
                ignore_text_analysis_offsets,
                allow_missing_required_offsets,
                allow_header_text_offset_mismatch,
                truncate_text_offsets,
            ],
        }
    }

    fn reader_config_args() -> Vec<Self> {
        let allow_uneven_event_width = ArgData::allow_uneven_event_width();
        let allow_tot_mismatch = ArgData::allow_tot_mismatch();
        vec![allow_uneven_event_width, allow_tot_mismatch]
    }

    fn shared_config_args() -> Vec<Self> {
        let warnings_are_errors = ArgData::warnings_are_errors_arg();

        vec![warnings_are_errors]
    }

    fn trim_intra_value_whitespace() -> Self {
        ArgData::new_config_bool_arg(
            "trim_intra_value_whitespace".into(),
            "If ``True``, trim whitespace between delimiters such as ``,`` \
             and ``;`` within keyword value strings."
                .into(),
        )
    }

    fn time_meas_pattern_arg() -> Self {
        ArgData::new_config_opt_arg(
            "time_meas_pattern".into(),
            PyType::Str,
            format!(
                "A pattern to match the *$PnN* of the time measurement. Must be \
                a regular expression following syntax described in {REGEXP_REF}. \
                If ``None``, do not try to find a time measurement."
            ),
            parse_quote!(fireflow_core::config::TimeMeasNamePattern),
        )
    }

    fn allow_missing_time_arg() -> Self {
        ArgData::new_config_bool_arg(
            "allow_missing_time".into(),
            "If ``True`` allow time measurement to be missing.".into(),
        )
    }

    fn force_time_linear_arg() -> Self {
        ArgData::new_config_bool_arg(
            "force_time_linear".into(),
            "If ``True`` force time measurement to be linear independent of *$PnE*.".into(),
        )
    }

    fn ignore_time_gain_arg() -> Self {
        ArgData::new_config_bool_arg(
            "ignore_time_gain".into(),
            "If ``True`` ignore the *$PnG* (gain) keyword. This keyword should not \
             be set according to the standard} however, this library will allow \
             gain to be 1.0 since this equates to identity. If gain is not 1.0, \
             this is nonsense and it can be ignored with this flag."
                .into(),
        )
    }

    fn ignore_time_optical_keys_arg() -> Self {
        ArgData::new_config_arg(
            "ignore_time_optical_keys".into(),
            PyType::new_list(temporal_optical_key_pytype()),
            "Ignore optical keys in temporal measurement. These keys are \
             nonsensical for time measurements but are not explicitly forbidden in \
             the the standard. Provided keys are the string after the \"Pn\" in \
             the \"PnX\" keywords."
                .into(),
            DocDefault::Other(quote!(TemporalOpticalKeys::default()), "[]".into()),
            parse_quote!(TemporalOpticalKeys),
        )
    }

    fn parse_indexed_spillover_arg() -> Self {
        ArgData::new_config_bool_arg(
            "parse_indexed_spillover".into(),
            "Parse $SPILLOVER with numeric indices rather than strings \
             (ie names or *$PnN*)"
                .into(),
        )
    }

    fn date_pattern_arg() -> Self {
        ArgData::new_config_opt_arg(
            "date_pattern".into(),
            PyType::Str,
            format!(
                "If supplied, will be used as an alternative pattern when \
                 parsing *$DATE*. It should have specifiers for year, month, and \
                 day as outlined in {CHRONO_REF}. If not supplied, *$DATE* will \
                 be parsed according to the standard pattern which is \
                 ``%d-%b-%Y``."
            ),
            parse_quote!(fireflow_core::validated::datepattern::DatePattern),
        )
    }

    // TODO make this version specific
    fn time_pattern_arg() -> Self {
        ArgData::new_config_opt_arg(
            "time_pattern".into(),
            PyType::Str,
            format!(
                "If supplied, will be used as an alternative pattern when \
                 parsing *$BTIM* and *$ETIM*. It should have specifiers for \
                 hours, minutes, and seconds as outlined in {CHRONO_REF}. It may \
                 optionally also have a sub-seconds specifier as shown in the \
                 same link. Furthermore, the specifiers '%!' and %@' may be used \
                 to match 1/60 and centiseconds respectively. If not supplied, \
                 *$BTIM* and *$ETIM* will be parsed according to the standard \
                 pattern which is version-specific."
            ),
            parse_quote!(fireflow_core::validated::timepattern::TimePattern),
        )
    }

    fn allow_pseudostandard_arg() -> Self {
        ArgData::new_config_bool_arg(
            "allow_pseudostandard".into(),
            "If ``True`` allow non-standard keywords with a leading *$*. The \
             presence of such keywords often means the version in *HEADER* \
             is incorrect."
                .into(),
        )
    }

    fn allow_unused_standard_arg() -> Self {
        ArgData::new_config_bool_arg(
            "allow_unused_standard".into(),
            "If ``True`` allow unused standard keywords to be present.".into(),
        )
    }

    fn disallow_deprecated_arg() -> Self {
        ArgData::new_config_bool_arg(
            "disallow_deprecated".into(),
            "If ``True`` throw error if a deprecated key is encountered.".into(),
        )
    }

    fn fix_log_scale_offsets_arg() -> Self {
        ArgData::new_config_bool_arg(
            "fix_log_scale_offsets".into(),
            "If ``True`` fix log-scale *PnE* and keywords which have zero offset \
             (ie ``X,0,0`` where ``X`` is non-zero)."
                .into(),
        )
    }

    fn nonstandard_measurement_pattern_arg() -> Self {
        ArgData::new_config_opt_arg(
            "nonstandard_measurement_pattern".into(),
            PyType::Str,
            format!(
                "Pattern to use when matching nonstandard measurement keys. Must \
                 be a regular expression pattern with ``%n`` which will \
                 represent the measurement index and should not start with *$*. \
                 Otherwise should be a normal regular expression as defined in \
                 {REGEXP_REF}."
            ),
            parse_quote!(fireflow_core::validated::keys::NonStdMeasPattern),
        )
    }

    fn integer_widths_from_byteord_arg() -> Self {
        ArgData::new_config_bool_arg(
            "integer_widths_from_byteord".into(),
            "If ``True`` set all *$PnB* to the number of bytes from *$BYTEORD*. \
             Only has an effect for FCS 2.0/3.0 where *$DATATYPE* is ``I``."
                .into(),
        )
    }

    fn integer_byteord_override_arg() -> Self {
        ArgData::new_config_opt_arg(
            "integer_byteord_override".into(),
            PyType::new_list(PyType::Int),
            "Override *$BYTEORD* for integer layouts.".into(),
            parse_quote!(fireflow_core::text::byteord::ByteOrd2_0),
        )
    }

    fn disallow_range_truncation_arg() -> Self {
        ArgData::new_config_bool_arg(
            "disallow_range_truncation".into(),
            "If ``True`` throw error if *$PnR* values need to be truncated \
             to match the number of bytes specified by *$PnB* and *$DATATYPE*."
                .into(),
        )
    }

    fn new_config_correction_arg(name: &str, what: &str, location: &str, rstype: Path) -> Self {
        ArgData::new_config_arg(
            name.into(),
            correction_pytype(),
            format!("Corrections for *{what}* offsets in *{location}*"),
            DocDefault::Other(
                quote!(fireflow_core::segment::OffsetCorrection::default()),
                "(0, 0)".into(),
            ),
            parse_quote!(fireflow_core::segment::#rstype),
        )
    }

    fn text_correction_arg() -> Self {
        Self::new_config_correction_arg(
            "text_correction",
            "TEXT",
            "HEADER",
            parse_quote!(HeaderCorrection<fireflow_core::segment::PrimaryTextSegmentId>),
        )
    }

    fn data_correction_arg() -> Self {
        Self::new_config_correction_arg(
            "data_correction",
            "DATA",
            "HEADER",
            parse_quote!(HeaderCorrection<fireflow_core::segment::DataSegmentId>),
        )
    }

    fn analysis_correction_arg() -> Self {
        Self::new_config_correction_arg(
            "analysis_correction",
            "ANALYSIS",
            "HEADER",
            parse_quote!(HeaderCorrection<fireflow_core::segment::AnalysisSegmentId>),
        )
    }

    fn other_corrections_arg() -> Self {
        let id_path = quote!(fireflow_core::segment::OtherSegmentId);
        let corr_path = quote!(fireflow_core::segment::HeaderCorrection);
        Self::new_config_arg(
            "other_corrections".into(),
            PyType::new_list(correction_pytype()),
            "Corrections for OTHER offsets if they exist. Each correction will \
             be applied in order. If an offset does not need to be corrected, \
             use ``(0, 0)``. This will not affect the number of OTHER segments \
             that are read; this is controlled by ``max_other``."
                .into(),
            DocDefault::EmptyList,
            parse_quote!(Vec<#corr_path<#id_path>>),
        )
    }

    fn max_other_arg() -> Self {
        Self::new_config_opt_arg(
            "max_other".into(),
            PyType::Int,
            "Maximum number of OTHER segments that can be parsed. \
             ``None`` means limitless."
                .into(),
            parse_quote!(usize),
        )
    }

    fn other_width_arg() -> Self {
        let path = parse_quote!(fireflow_core::validated::ascii_range::OtherWidth);
        Self::new_config_arg(
            "other_width".into(),
            PyType::Int,
            "Maximum number of OTHER segments that can be parsed. \
             ``None`` means limitless."
                .into(),
            DocDefault::Other(quote!(#path::default()), "8".into()),
            path,
        )
    }

    // this only matters for 3.0+ files
    fn squish_offsets_arg() -> Self {
        Self::new_config_bool_arg(
            "squish_offsets".into(),
            "If ``True`` and a segment's ending offset is zero, treat entire \
             offset as empty. This might happen if the ending offset is longer \
             than 8 digits, in which case it must be written in *TEXT*. If this \
             happens, the standards mandate that both offsets be written to \
             *TEXT* and that the *HEADER* offsets be set to ``0,0``, so only \
             writing one is an error unless this flag is set. This should only \
             happen in FCS 3.0 files and above."
                .into(),
        )
    }

    fn allow_negative_arg() -> Self {
        Self::new_config_bool_arg(
            "allow_negative".into(),
            "If true, allow negative values in a HEADER offset. If negative \
             offsets are found, they will be replaced with ``0``. Some files \
             will denote an \"empty\" offset as ``0,-1``, which is logically \
             correct since the last offset points to the last byte, thus ``0,0`` \
             is actually 1 byte long. Unfortunately this is not what the \
             standards say, so specifying ``0,-1`` is an error unless this \
             flag is set."
                .into(),
        )
    }

    fn truncate_offsets_arg() -> Self {
        Self::new_config_bool_arg(
            "truncate_offsets".into(),
            "If true, truncate offsets that exceed the end of the file. \
             In some cases the DATA offset (usually) might exceed the end of the \
             file by 1, which is usually a mistake and should be corrected with \
             ``data_correction`` (or analogous for the offending offset). If this \
             is not the case, the file is likely corrupted. This flag will allow \
             such files to be read conveniently if desired."
                .into(),
        )
    }

    fn text_data_correction() -> Self {
        Self::new_config_correction_arg(
            "text_data_correction",
            "DATA",
            "TEXT",
            parse_quote!(TEXTCorrection<fireflow_core::segment::DataSegmentId>),
        )
    }

    fn text_analysis_correction() -> Self {
        Self::new_config_correction_arg(
            "text_analysis_correction",
            "ANALYSIS",
            "TEXT",
            parse_quote!(TEXTCorrection<fireflow_core::segment::AnalysisSegmentId>),
        )
    }

    fn ignore_text_data_offsets() -> Self {
        ArgData::new_config_bool_arg(
            "ignore_text_data_offsets".into(),
            "If ``True`` ignore *DATA* offsets in *TEXT*".into(),
        )
    }

    fn ignore_text_analysis_offsets() -> Self {
        ArgData::new_config_bool_arg(
            "ignore_text_analysis_offsets".into(),
            "If ``True`` ignore *ANALYSIS* offsets in *TEXT*".into(),
        )
    }

    fn allow_header_text_offset_mismatch() -> Self {
        ArgData::new_config_bool_arg(
            "allow_header_text_offset_mismatch".into(),
            "If ``True`` allow *TEXT* and *HEADER* offsets to mismatch.".into(),
        )
    }

    fn allow_missing_required_offsets(version: Version) -> Self {
        let s = if version >= Version::FCS3_2 {
            "*DATA*"
        } else {
            "*DATA* and *ANALYSIS*"
        };
        ArgData::new_config_bool_arg(
            "allow_missing_required_offsets".into(),
            format!(
                "If ``True`` allow required {s} offsets in *TEXT* to be missing. \
                 If missing, fall back to offsets from *HEADER*."
            ),
        )
    }

    fn truncate_text_offsets() -> Self {
        ArgData::new_config_bool_arg(
            "truncate_text_offsets".into(),
            "If ``True`` truncate offsets that exceed end of file.".into(),
        )
    }

    fn allow_uneven_event_width() -> Self {
        ArgData::new_config_bool_arg(
            "allow_uneven_event_width".into(),
            "If ``True`` allow event width to not perfectly divide length of *DATA*. \
            Does not apply to delimited ASCII layouts. "
                .into(),
        )
    }

    fn allow_tot_mismatch() -> Self {
        ArgData::new_config_bool_arg(
            "allow_tot_mismatch".into(),
            "If ``True`` allow *$TOT* to not match number of events as \
             computed by the event width and length of *DATA*. \
             Does not apply to delimited ASCII layouts."
                .into(),
        )
    }

    fn warnings_are_errors_arg() -> Self {
        ArgData::new_config_bool_arg(
            "warnings_are_errors".into(),
            "If ``True`` all warnings will be regarded as errors.".into(),
        )
    }
}

#[proc_macro]
pub fn impl_core_all_pns(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_meas_attr(&i, "Longname", "longnames", "S", PyType::Str)
}

#[proc_macro]
pub fn impl_core_all_pnf(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_optical_attr(&i, "Filter", "filters", "F", PyType::Str)
}

#[proc_macro]
pub fn impl_core_all_pno(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_optical_attr(&i, "Power", "powers", "O", PyType::Float)
}

#[proc_macro]
pub fn impl_core_all_pnp(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_optical_attr(&i, "PercentEmitted", "percents_emitted", "P", PyType::Str)
}

#[proc_macro]
pub fn impl_core_all_pnt(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_optical_attr(&i, "DetectorType", "detector_types", "T", PyType::Str)
}

#[proc_macro]
pub fn impl_core_all_pnv(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_optical_attr(
        &i,
        "DetectorVoltage",
        "detector_voltages",
        "V",
        PyType::Float,
    )
}

#[proc_macro]
pub fn impl_core_all_pnl_old(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_optical_attr(&i, "Wavelength", "wavelengths", "L", PyType::Float)
}

#[proc_macro]
pub fn impl_core_all_pnl_new(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_optical_attr(
        &i,
        "Wavelengths",
        "wavelengths",
        "L",
        PyType::new_list(PyType::Float),
    )
}

#[proc_macro]
pub fn impl_core_all_pnd(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_meas_attr(&i, "Display", "displays", "D", display_pytype())
}

#[proc_macro]
pub fn impl_core_all_pndet(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_optical_attr(&i, "DetectorName", "detector_names", "DET", PyType::Str)
}

#[proc_macro]
pub fn impl_core_all_pncal3_1(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_optical_attr(
        &i,
        "Calibration3_1",
        "calibrations",
        "CALIBRATION",
        calibration3_1_pytype(),
    )
}

#[proc_macro]
pub fn impl_core_all_pncal3_2(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_optical_attr(
        &i,
        "Calibration3_2",
        "calibrations",
        "CALIBRATION",
        calibration3_2_pytype(),
    )
}

#[proc_macro]
pub fn impl_core_all_pntag(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_optical_attr(&i, "Tag", "tags", "TAG", PyType::Str)
}

#[proc_macro]
pub fn impl_core_all_pntype(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_optical_attr(&i, "OpticalType", "measurement_types", "TYPE", PyType::Str)
}

#[proc_macro]
pub fn impl_core_all_pnfeature(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_optical_attr(&i, "Feature", "features", "FEATURE", feature_pytype())
}

#[proc_macro]
pub fn impl_core_all_pnanalyte(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_optical_attr(&i, "Analyte", "analytes", "ANALYTE", PyType::Str)
}

fn core_all_optical_attr(
    t: &Ident,
    kw: &str,
    name: &str,
    suffix: &str,
    base_pytype: PyType,
) -> TokenStream {
    core_all_meas_attr1(t, kw, name, suffix, base_pytype, true, true)
}

fn core_all_meas_attr(
    t: &Ident,
    kw: &str,
    name: &str,
    suffix: &str,
    base_pytype: PyType,
) -> TokenStream {
    core_all_meas_attr1(t, kw, name, suffix, base_pytype, true, false)
}

fn core_all_meas_attr1(
    t: &Ident,
    kw: &str,
    name: &str,
    suffix: &str,
    base_pytype: PyType,
    is_optional: bool,
    optical_only: bool,
) -> TokenStream {
    let kw_doc = format!("*$Pn{suffix}*");
    let kw_inner = keyword_path(kw);

    let doc_summary = format!("Value of {kw_doc} for all measurements.");
    let doc_middle = if optical_only {
        vec![format!(
            "``()`` will be returned for time since {kw_doc} is not \
             defined for temporal measurements."
        )]
    } else {
        vec![]
    };

    let tmp_pytype = if optical_only {
        PyType::new_union2(base_pytype, PyType::new_unit())
    } else {
        base_pytype
    };

    let doc_type = if is_optional {
        PyType::new_opt(tmp_pytype)
    } else {
        tmp_pytype
    };

    let doc = DocString::new(
        doc_summary,
        doc_middle,
        DocSelf::PySelf,
        vec![],
        Some(DocReturn::new(doc_type, None)),
    )
    .doc();

    let get = format_ident!("get_all_{name}");
    let set = format_ident!("set_all_{name}");

    let kw = if is_optional {
        quote! {Option<#kw_inner>}
    } else {
        quote! {#kw_inner}
    };

    let nce_path = quote!(fireflow_core::text::named_vec::NonCenterElement);

    let (fn_get, fn_set) = if optical_only {
        (
            quote! {
                fn #get(&self) -> Vec<#nce_path<#kw>> {
                    self.0
                        .optical_opt()
                        .map(|e| e.0.map_non_center(|x| x.cloned()).into())
                        .collect()
                }
            },
            quote! {
                fn #set(&mut self, xs: Vec<#nce_path<#kw>>) -> PyResult<()> {
                    self.0.set_optical(xs).py_termfail_resolve_nowarn()
                }
            },
        )
    } else {
        (
            quote! {
                fn #get(&self) -> Vec<#kw> {
                    self.0.meas_opt().map(|x| x.cloned()).collect()
                }
            },
            quote! {
                fn #set(&mut self, xs: Vec<#kw>) -> PyResult<()> {
                    Ok(self.0.set_meas(xs)?)
                }
            },
        )
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
    .into()
}

#[proc_macro]
pub fn impl_core_to_version_x_y(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let (is_dataset, version) = split_ident_version_pycore(&i);
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
    let base = if is_dataset {
        "CoreDataset"
    } else {
        "CoreTEXT"
    };
    let outputs: Vec<_> = ALL_VERSIONS
        .iter()
        .filter(|&&v| v != version)
        .map(|v| {
            let vsu = v.short_underscore();
            let vs = v.short();
            let fn_name = format_ident!("to_version_{vsu}");
            let target_type = format_ident!("{base}{vsu}");
            let target_pytype = format_ident!("Py{target_type}");
            let param = DocArg::new_param_def(
                "force".into(),
                PyType::Bool,
                param_desc.into(),
                DocDefault::Bool(false),
            );
            let doc = DocString::new(
                format!("Convert to FCS {vs}."),
                vec![sub.into()],
                DocSelf::PySelf,
                vec![param],
                Some(DocReturn::new(
                    PyType::PyClass(target_type.to_string()),
                    Some(format!("A new class conforming to FCS {vs}")),
                )),
            );
            quote! {
                #doc
                fn #fn_name(&self, force: bool) -> PyResult<#target_pytype> {
                    self.0.clone().try_convert(force).py_termfail_resolve().map(|x| x.into())
                }
            }
        })
        .collect();

    quote! {
        #[pymethods]
        impl #i {
            #(#outputs)*
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_gated_meas(input: TokenStream) -> TokenStream {
    let path: Path = syn::parse(input).unwrap();
    let name = path.segments.last().unwrap().ident.clone();

    let make_get_set = |n: &str, t: &str| {
        let get = format_ident!("get_{n}");
        let set = format_ident!("set_{n}");
        let inner = format_ident!("{n}");
        let rstype = keyword_path(t);
        let path: Path = parse_quote!(Option<#rstype>);
        let methods = quote! {
            #[getter]
            fn #get(&self) -> #path {
                self.0.#inner.0.as_ref().cloned()
            }

            #[setter]
            fn #set(&mut self, x: #path) {
                self.0.#inner.0 = x.into();
            }
        };
        (path, methods)
    };

    let scale_doc = DocArg::new_ivar_def(
        "scale".into(),
        PyType::new_opt(PyType::new_union2(
            PyType::new_unit(),
            PyType::Tuple(vec![PyType::Float, PyType::Float]),
        )),
        "The *$GmE* keyword. ``()`` means linear scaling and 2-tuple \
         specifies decades and offset for log scaling."
            .into(),
        DocDefault::Option,
    );
    let (scale_path, scale_methods) = make_get_set("scale", "GateScale");
    let scale = ArgData::new(scale_doc, scale_path, Some(scale_methods));

    let make_arg = |n: &str, kw: &str, t: &str, p: PyType| {
        let (path, methods) = make_get_set(n, t);
        let doc = DocArg::new_ivar_def(
            n.into(),
            PyType::new_opt(p),
            format!("The *$Gm{kw}* keyword."),
            DocDefault::Option,
        );
        ArgData::new(doc, path, Some(methods))
    };
    let filter = make_arg("filter", "F", "GateFilter", PyType::Str);
    let shortname = make_arg("shortname", "N", "GateShortname", PyType::Str);
    let percent_emitted = make_arg("percent_emitted", "P", "GatePercentEmitted", PyType::Str);
    let range = make_arg("range", "R", "GateRange", PyType::Float);
    let longname = make_arg("longname", "S", "GateLongname", PyType::Str);
    let detector_type = make_arg("detector_type", "T", "GateDetectorType", PyType::Str);
    let detector_voltage = make_arg(
        "detector_voltage",
        "V",
        "GateDetectorVoltage",
        PyType::Float,
    );

    let all_args = [
        scale,
        filter,
        shortname,
        percent_emitted,
        range,
        longname,
        detector_type,
        detector_voltage,
    ];

    let ps = all_args.iter().map(|x| x.doc.clone()).collect();
    let summary = "The *$Gm\\** keywords for one gated measurement.".into();
    let doc = DocString::new(summary, vec![], DocSelf::NoSelf, ps, None);

    let fun_args: Vec<_> = all_args.iter().map(|x| x.constr_arg()).collect();
    let inner_args: Vec<_> = all_args.iter().map(|x| x.inner_arg()).collect();
    let methods: Vec<_> = all_args.iter().map(|x| x.methods.clone()).collect();

    let new = quote! {
        fn new(#(#fun_args),*) -> Self {
            #path::new(#(#inner_args),*).into()
        }
    };

    let rest = quote! {
        #(#methods)*
    };

    impl_new(name.to_string(), path, doc, new, rest).1.into()
}

#[proc_macro]
pub fn impl_new_fixed_ascii_layout(input: TokenStream) -> TokenStream {
    let path: Path = syn::parse(input).unwrap();
    let name = path.segments.last().unwrap().ident.clone();
    let bare_path = path_strip_args(path.clone());

    let chars_param = DocArg::new_ivar(
        "ranges".into(),
        PyType::new_list(PyType::Int),
        "The range for each measurement. Equivalent to *$PnR*. The value of \
         *$PnB* will be derived from these and will be equivalent to the number \
         of digits for each value."
            .into(),
    );

    let constr_doc = DocString::new(
        "A fixed-width ASCII layout.".into(),
        vec![],
        DocSelf::NoSelf,
        vec![chars_param],
        None,
    );

    let constr = quote! {
        fn new(ranges: Vec<u64>) -> Self {
            #bare_path::new_ascii_u64(ranges).into()
        }
    };

    let char_widths_doc = DocString::new(
        "The width of each measurement in number of chars (read only).".into(),
        vec![
            "Equivalent to *$PnB*, which is the number of chars/digits used \
             to encode data for a given measurement."
                .into(),
        ],
        DocSelf::PySelf,
        vec![],
        Some(DocReturn::new(PyType::new_list(PyType::Int), None)),
    )
    .doc();

    let datatype = make_layout_datatype("A");

    let rest = quote! {
        #[getter]
        fn ranges(&self) -> Vec<u64> {
            self.0.columns().iter().map(|c| c.value()).collect()
        }

        #char_widths_doc
        #[getter]
        fn char_widths(&self) -> Vec<u64> {
            self.0
                .widths()
                .into_iter()
                .map(|x| u64::from(u8::from(x)))
                .collect()
        }

        #datatype
    };

    impl_new(name.to_string(), path, constr_doc, constr, rest)
        .1
        .into()
}

#[proc_macro]
pub fn impl_new_delim_ascii_layout(input: TokenStream) -> TokenStream {
    let path: Path = syn::parse(input).unwrap();
    let name = path.segments.last().unwrap().ident.clone();
    let bare_path = path_strip_args(path.clone());

    let ranges_param = DocArg::new_ivar(
        "ranges".into(),
        PyType::new_list(PyType::Int),
        "The range for each measurement. Equivalent to the *$PnR* \
         keyword. This is not used internally and thus only represents \
         documentation at the user level."
            .into(),
    );

    let constr_doc = DocString::new(
        "A delimited ASCII layout.".into(),
        vec![],
        DocSelf::NoSelf,
        vec![ranges_param],
        None,
    );

    let constr = quote! {
        fn new(ranges: Vec<u64>) -> Self {
            #bare_path::new(ranges).into()
        }
    };

    let datatype = make_layout_datatype("A");

    let rest = quote! {
        #[getter]
        fn ranges(&self) -> Vec<u64> {
            self.0.ranges.clone()
        }

        #datatype
    };

    impl_new(name.to_string(), path, constr_doc, constr, rest)
        .1
        .into()
}

#[proc_macro]
pub fn impl_new_ordered_layout(input: TokenStream) -> TokenStream {
    let info = parse_macro_input!(input as OrderedLayoutInfo);
    let nbytes = info.nbytes;
    let is_float = info.is_float;
    let nbits = nbytes * 8;

    let (range_pytype, range_desc, what, base, range_path, dt) = if is_float {
        let range = format_ident!("F{:02}Range", nbits);
        let range_desc = "The range for each measurement. Corresponds to *$PnR*. \
                          This is not used internally so only serves for users' \
                          own purposes.";
        (
            PyType::Decimal,
            range_desc,
            "float",
            "F",
            quote!(fireflow_core::data::#range),
            if nbytes == 4 { "F" } else { "D" },
        )
    } else {
        let bitmask = format_ident!("Bitmask{:02}", nbits);
        let range_desc = "The range for each measurement. Corresponds to \
                          *$PnR* - 1, which implies that the value for each \
                          measurement must be less than or equal to the values \
                          in ``ranges``. A bitmask will be created which \
                          corresponds to one less the next power of 2.";
        (
            PyType::Int,
            range_desc,
            "integer",
            "Uint",
            quote!(fireflow_core::validated::bitmask::#bitmask),
            "I",
        )
    };
    let known_tot_path = quote!(fireflow_core::data::KnownTot);
    let ordered_layout_path = quote!(fireflow_core::data::OrderedLayout);
    let fixed_layout_path = quote!(fireflow_core::data::FixedLayout);
    let sizedbyteord_path = quote!(fireflow_core::text::byteord::SizedByteOrd);

    let full_layout_path = parse_quote!(#ordered_layout_path<#range_path, #known_tot_path>);

    let layout_name = format!("Ordered{base}{:02}Layout", nbits);

    let summary = format!("{nbits}-bit ordered {what} layout");

    let range_param = DocArg::new_ivar(
        "ranges".into(),
        PyType::new_list(range_pytype),
        range_desc.into(),
    );

    let byteord_param = DocArg::new_ivar_def(
        "byteord".into(),
        PyType::new_union2(
            PyType::new_lit(&["big", "little"]),
            PyType::new_list(PyType::Int),
        ),
        format!(
            "The byte order to use when encoding values. Must be ``\"big\"``, \
             ``\"little\"``, or a list of all integers between 1 and {nbytes} \
             in any order."
        ),
        DocDefault::Other(quote!(#sizedbyteord_path::default()), "\"little\"".into()),
    );

    let is_big_param = make_endian_param(2);

    let widths = make_byte_width(nbytes);
    let datatype = make_layout_datatype(dt);

    let ranges = quote! {
        #[getter]
        fn ranges(&self) -> Vec<#range_path> {
            self.0.columns().iter().map(|c| c.clone()).collect()
        }
    };

    let make_constr_doc =
        |ps| DocString::new(format!("{summary}."), vec![], DocSelf::NoSelf, ps, None);

    // make different constructors and getters for u8 and u16 since the byteord
    // for these can be simplified
    let (constr, constr_doc, byteord) = match (is_float, nbytes) {
        // u8 doesn't need byteord since only one is possible
        (false, 1) => {
            let constr = quote! {
                fn new(ranges: Vec<#range_path>) -> Self {
                    #fixed_layout_path::new(ranges, #sizedbyteord_path::default()).into()
                }
            };
            let constr_doc = make_constr_doc(vec![range_param.clone()]);
            (constr, constr_doc, quote!())
        }

        // u16 only has two combinations (big and little) so don't allow a list
        // for byteord
        (false, 2) => {
            let endian = quote!(fireflow_core::text::byteord::Endian);
            let constr_doc = make_constr_doc(vec![range_param.clone(), is_big_param]);
            let constr = quote! {
                fn new(ranges: Vec<#range_path>, endian: #endian) -> Self {
                    let b = #sizedbyteord_path::Endian(endian);
                    #fixed_layout_path::new(ranges, b).into()
                }
            };
            let byteord = quote! {
                #[getter]
                fn endian(&self) -> #endian {
                    let m: #sizedbyteord_path<2> = *self.0.as_ref();
                    m.endian()
                }
            };
            (constr, constr_doc, byteord)
        }

        // everything else needs the "full" version of byteord, which is big,
        // little, and mixed (a list)
        _ => {
            let constr_doc = make_constr_doc(vec![range_param.clone(), byteord_param]);
            let constr = quote! {
                fn new(ranges: Vec<#range_path>, byteord: #sizedbyteord_path<#nbytes>) -> Self {
                    #fixed_layout_path::new(ranges, byteord).into()
                }
            };
            let byteord = quote! {
                #[getter]
                fn byteord(&self) -> #sizedbyteord_path<#nbytes> {
                    *self.0.as_ref()
                }
            };
            (constr, constr_doc, byteord)
        }
    };

    let rest = quote! {
        #ranges
        #byteord
        #widths
        #datatype
    };

    impl_new(layout_name, full_layout_path, constr_doc, constr, rest)
        .1
        .into()
}

#[proc_macro]
pub fn impl_new_endian_float_layout(input: TokenStream) -> TokenStream {
    let nbytes = parse_macro_input!(input as LitInt)
        .base10_parse::<usize>()
        .expect("Must be an integer");
    let nbits = nbytes * 8;
    let range = format_ident!("F{:02}Range", nbits);
    let range_path = quote!(fireflow_core::data::#range);

    let nomeasdt_path = quote!(fireflow_core::data::NoMeasDatatype);
    let endian_layout_path = quote!(fireflow_core::data::EndianLayout);
    let fixed_layout_path = quote!(fireflow_core::data::FixedLayout);
    let endian = quote!(fireflow_core::text::byteord::Endian);

    let full_layout_path = parse_quote!(#endian_layout_path<#range_path, #nomeasdt_path>);

    let layout_name = format!("EndianF{:02}Layout", nbits);

    let range_param = DocArg::new_ivar(
        "ranges".into(),
        PyType::new_list(PyType::Decimal),
        "The range for each measurement. Corresponds to *$PnR*. This is not \
         used internally so only serves the users' own purposes."
            .into(),
    );

    let is_big_param = make_endian_param(4);

    let constr_doc = DocString::new(
        format!("{nbits}-bit endian float layout"),
        vec![],
        DocSelf::NoSelf,
        vec![range_param.clone(), is_big_param],
        None,
    );

    let constr = quote! {
        fn new(ranges: Vec<#range_path>, endian: #endian) -> Self {
            #fixed_layout_path::new(ranges, endian).into()
        }
    };

    let widths = make_byte_width(nbytes);
    let datatype = make_layout_datatype(if nbytes == 4 { "F" } else { "D" });

    let rest = quote! {
        #[getter]
        fn ranges(&self) -> Vec<#range_path> {
            self.0.columns().iter().map(|c| c.clone()).collect()
        }

        #[getter]
        fn endian(&self) -> #endian {
            *self.0.as_ref()
        }

        #widths
        #datatype
    };

    impl_new(layout_name, full_layout_path, constr_doc, constr, rest)
        .1
        .into()
}

#[proc_macro]
pub fn impl_new_endian_uint_layout(_: TokenStream) -> TokenStream {
    let name = format_ident!("EndianUintLayout");

    let fixed = quote!(fireflow_core::data::FixedLayout);
    let bitmask = quote!(fireflow_core::data::AnyNullBitmask);
    let nomeasdt = quote!(fireflow_core::data::NoMeasDatatype);
    let endian_layout = quote!(fireflow_core::data::EndianLayout);
    let endian = quote!(fireflow_core::text::byteord::Endian);
    let layout_path = parse_quote!(#endian_layout<#bitmask, #nomeasdt>);

    let ranges_param = DocArg::new_ivar(
        "ranges".into(),
        PyType::new_list(PyType::Int),
        "The range of each measurement. Corresponds to the *$PnR* \
         keyword less one. The number of bytes used to encode each \
         measurement (*$PnB*) will be the minimum required to express this \
         value. For instance, a value of ``1023`` will set *$PnB* to ``16``, \
         will set *$PnR* to ``1024``, and encode values for this measurement as \
         16-bit integers. The values of a measurement will be less than or \
         equal to this value."
            .into(),
    );

    let is_big_param = make_endian_param(4);

    let constr_doc = DocString::new(
        "A mixed-width integer layout.".into(),
        vec![],
        DocSelf::NoSelf,
        vec![ranges_param, is_big_param],
        None,
    );

    let constr = quote! {
        fn new(ranges: Vec<u64>, endian: #endian) -> Self {
            let rs = ranges.into_iter().map(#bitmask::from).collect();
            #fixed::new(rs, endian).into()
        }
    };

    let datatype = make_layout_datatype("I");

    let rest = quote! {
        #[getter]
        fn ranges(&self) -> Vec<u64> {
            self.0.columns().iter().map(|c| u64::from(*c)).collect()
        }

        #[getter]
        fn endian(&self) -> #endian {
            *self.0.as_ref()
        }

        #datatype
    };

    impl_new(name.to_string(), layout_path, constr_doc, constr, rest)
        .1
        .into()
}

#[proc_macro]
pub fn impl_new_mixed_layout(_: TokenStream) -> TokenStream {
    let name = format_ident!("MixedLayout");
    let layout_path = parse_quote!(fireflow_core::data::#name);

    let null = quote!(fireflow_core::data::NullMixedType);
    let fixed = quote!(fireflow_core::data::FixedLayout);
    let endian = quote!(fireflow_core::text::byteord::Endian);

    let types_param = DocArg::new_ivar(
        "typed_ranges".into(),
        PyType::new_list(PyType::new_union2(
            PyType::Tuple(vec![PyType::new_lit(&["A", "I"]), PyType::Int]),
            PyType::Tuple(vec![PyType::new_lit(&["F", "D"]), PyType::Decimal]),
        )),
        "The type and range for each measurement corresponding to *$DATATYPE* \
         and/or *$PnDATATYPE* and *$PnR* respectively. These are given \
         as 2-tuples like ``(<type>, <range>)`` where ``type`` is one of \
         ``\"A\"``, ``\"I\"``, ``\"F\"``, or ``\"D\"`` corresponding to Ascii, \
         Integer, Float, or Double datatypes respectively."
            .into(),
    );

    let is_big_param = make_endian_param(4);

    let constr_doc = DocString::new(
        "A mixed-type layout.".into(),
        vec![],
        DocSelf::NoSelf,
        vec![types_param, is_big_param],
        None,
    );

    let constr = quote! {
        fn new(typed_ranges: Vec<#null>, endian: #endian) -> Self {
            #fixed::new(typed_ranges, endian).into()
        }
    };

    let rest = quote! {
        #[getter]
        fn typed_ranges(&self) -> Vec<#null> {
            self.0.columns().iter().map(|c| c.clone()).collect()
        }

        #[getter]
        fn endian(&self) -> #endian {
            *self.0.as_ref()
        }
    };

    impl_new(name.to_string(), layout_path, constr_doc, constr, rest)
        .1
        .into()
}

fn make_endian_param(n: usize) -> DocArg {
    let xs = (1..(n + 1)).join(",");
    let ys = (1..(n + 1)).rev().join(",");
    let endian = quote!(fireflow_core::text::byteord::Endian);
    DocArg::new_ivar_def(
        "endian".into(),
        PyType::new_lit(&["big", "little"]),
        format!(
            "If ``\"big\"`` use big endian (``{ys}``) for encoding values; \
             if ``\"little\"`` use little endian (``{xs}``)."
        ),
        DocDefault::Other(quote!(#endian::Little), "\"little\"".into()),
    )
}

fn make_byte_width(nbytes: usize) -> proc_macro2::TokenStream {
    let s0 = format!("Will always return ``{nbytes}``.");
    let s1 = "This corresponds to the value of *$PnB* divided by 8, which are \
              all the same for this layout."
        .into();
    let doc = DocString::new(
        "The width of each measurement in bytes (read only).".into(),
        vec![s0, s1],
        DocSelf::PySelf,
        vec![],
        Some(DocReturn::new(PyType::Int, None)),
    )
    .doc();
    quote! {
        #doc
        #[getter]
        fn byte_width(&self) -> usize {
            #nbytes
        }
    }
}

#[proc_macro]
pub fn impl_layout_byte_widths(input: TokenStream) -> TokenStream {
    let t = parse_macro_input!(input as Ident);

    let doc = DocString::new(
        "The width of each measurement in bytes (read-only).".into(),
        vec![
            "This corresponds to the value of *$PnB* for each measurement \
             divided by 8. Values for each measurement may be different."
                .into(),
        ],
        DocSelf::PySelf,
        vec![],
        Some(DocReturn::new(PyType::new_list(PyType::Int), None)),
    )
    .doc();

    quote! {
        #[pymethods]
        impl #t {
            #doc
            #[getter]
            fn byte_widths(&self) -> Vec<u32> {
                self.0
                    .widths()
                    .into_iter()
                    .map(|x| u32::from(u8::from(x)))
                    .collect()
            }
        }
    }
    .into()
}

fn make_layout_datatype(dt: &str) -> proc_macro2::TokenStream {
    let doc = DocString::new(
        "The value of *$DATATYPE* (read-only).".into(),
        vec![format!("Will always return ``\"{dt}\"``.")],
        DocSelf::PySelf,
        vec![],
        Some(DocReturn::new(datatype_pytype(), None)),
    )
    .doc();
    quote! {
        #doc
        #[getter]
        fn datatype(&self) -> fireflow_core::text::keywords::AlphaNumType {
            self.0.datatype().into()
        }
    }
}

struct OrderedLayoutInfo {
    nbytes: usize,
    is_float: bool,
}

impl Parse for OrderedLayoutInfo {
    fn parse(input: ParseStream) -> Result<Self> {
        let nbytes = input
            .parse::<LitInt>()?
            .base10_parse::<usize>()
            .expect("Number of bytes must be an unsigned integer");
        let _: Comma = input.parse()?;
        let is_float = input.parse::<LitBool>()?.value();
        Ok(Self { nbytes, is_float })
    }
}

#[proc_macro]
pub fn impl_new_gate_uni_regions(input: TokenStream) -> TokenStream {
    let path: Path = syn::parse(input).unwrap();
    make_gate_region(path, true)
}

#[proc_macro]
pub fn impl_new_gate_bi_regions(input: TokenStream) -> TokenStream {
    let path: Path = syn::parse(input).unwrap();
    make_gate_region(path, false)
}

fn make_gate_region(path: Path, is_uni: bool) -> TokenStream {
    let index_name = if is_uni { "index" } else { "x/y indices" };
    let region_ident = path.segments.last().unwrap().ident.clone();

    let index_path_inner = if let PathArguments::AngleBracketed(xs) =
        path.segments.last().unwrap().arguments.clone()
    {
        if let GenericArgument::Type(Type::Path(p)) = xs.args.first().unwrap() {
            p.path.clone()
        } else {
            panic!("could not get index type")
        }
    } else {
        panic!("no generic args")
    };

    let index_rstype_inner = index_path_inner.segments.last().unwrap().ident.clone();
    let index_rsname = index_rstype_inner.to_string();

    let index_pair = keyword_path("IndexPair");
    let nonempty = quote!(fireflow_core::nonempty::FCSNonEmpty);

    let (summary_version, suffix, index_desc, index_pytype_inner) = match index_rsname.as_str() {
        "GateIndex" => (
            "2.0",
            "2_0",
            format!(
                "The {index_name} corresponding to a gating measurement \
                 (the *m* in the *$Gm\\** keywords)."
            ),
            PyType::Int,
        ),
        "MeasOrGateIndex" => {
            let k = if is_uni { "Must" } else { "Each must" };
            (
                "3.0/3.1",
                "3_0",
                format!(
                    "The {index_name} corresponding to either a gating or a physical \
                     measurement (the *m* and *n* in the *$Gm\\** or *$Pn\\** \
                     keywords). {k} be a string like either ``Gm`` or ``Pn`` where \
                     ``m`` is an integer and the prefix corresponds to a gating or \
                     physical measurement respectively."
                ),
                PyType::Str,
            )
        }
        "PrefixedMeasIndex" => (
            "3.2",
            "3_2",
            format!(
                "The {index_name} corresponding to a physical measurement \
                 (the *n* in the *$Pn\\** keywords)."
            ),
            PyType::Int,
        ),
        _ => panic!("unknown index type"),
    };

    let (
        region_name,
        gate_rstype,
        index_rstype,
        index_pytype,
        gate_argname,
        gate_pytype,
        gate_desc,
    ) = if is_uni {
        (
            "univariate",
            keyword_path("UniGate"),
            index_path_inner.clone(),
            index_pytype_inner,
            format_ident!("gate"),
            PyType::Tuple(vec![PyType::Float; 2]),
            "The lower and upper bounds of the gate.".into(),
        )
    } else {
        let v = keyword_path("Vertex");
        (
            "bivariate",
            parse_quote!(#nonempty<#v>),
            parse_quote!(#index_pair<#index_path_inner>),
            PyType::Tuple(vec![index_pytype_inner; 2]),
            format_ident!("vertices"),
            PyType::new_list(PyType::Tuple(vec![PyType::Float; 2])),
            "The vertices of a polygon gate. Must not be empty.".into(),
        )
    };

    let summary = format!("Make a new FCS {summary_version}-compatible {region_name} region",);

    // TODO these are actually read-only variables
    let index_param = DocArg::new_ivar("index".into(), index_pytype, index_desc);

    let gate_param = DocArg::new_ivar(gate_argname.to_string(), gate_pytype, gate_desc);

    let name = format!("{region_ident}{suffix}");

    let doc = DocString::new(
        summary,
        vec![],
        DocSelf::NoSelf,
        vec![index_param, gate_param],
        None,
    );

    let bare_path = path_strip_args(path.clone());

    let new = quote! {
        fn new(index: #index_rstype, #gate_argname: #gate_rstype) -> Self {
            #bare_path { index, #gate_argname }.into()
        }
    };

    let rest = quote! {
        #[getter]
        fn index(&self) -> #index_rstype {
            self.0.index
        }

        #[getter]
        fn #gate_argname(&self) -> #gate_rstype {
            self.0.#gate_argname.clone()
        }
    };

    impl_new(name, path, doc, new, rest).1.into()
}

fn impl_new(
    name: String,
    path: Path,
    d: DocString,
    constr: proc_macro2::TokenStream,
    rest: proc_macro2::TokenStream,
) -> (Ident, proc_macro2::TokenStream) {
    let doc = d.doc();
    let sig = d.sig();
    let pyname = format_ident!("Py{name}");
    let s = quote! {
        // pyo3 currently cannot add docstrings to __new__ methods, see
        // https://github.com/PyO3/pyo3/issues/4326
        //
        // workaround, put them on the structs themselves, which works but has the
        // disadvantage of being not next to the method def itself
        #doc
        #[pyclass(name = #name, eq)]
        #[derive(Clone, From, Into, PartialEq)]
        pub struct #pyname(#path);

        // TODO automatically make args here
        #[pymethods]
        impl #pyname {
            #sig
            #[new]
            #[allow(clippy::too_many_arguments)]
            #constr

            #rest
        }
    };
    (pyname, s)
}

fn unwrap_generic<'a>(name: &str, ty: &'a Path) -> (&'a Path, bool) {
    if let Some(segment) = ty.segments.last()
        && segment.ident == name
        && let PathArguments::AngleBracketed(args) = &segment.arguments
        && let Some(GenericArgument::Type(Type::Path(inner_type))) = args.args.first()
    {
        return (&inner_type.path, true);
    }
    (ty, false)
}

fn split_ident_version(name: &Ident) -> (String, Version) {
    let n = name.to_string();
    let (ret, v) = n.split_at(n.len() - 3);
    let version = Version::from_short_underscore(v).expect("version should be like 'X_Y'");
    (ret.to_string(), version)
}

fn split_ident_version_checked(which: &'static str, name: &Ident) -> Version {
    let (n, v) = split_ident_version(name);
    if n.as_str() != which {
        panic!("identifier should be like '{which}X_Y'")
    }
    v
}

fn split_ident_version_pycore(name: &Ident) -> (bool, Version) {
    let (base, version) = split_ident_version(name);

    if !(base == "PyCoreTEXT" || base == "PyCoreDataset") {
        panic!("must be PyCore(TEXT|Dataset)X_Y")
    }
    (base == "PyCoreDataset", version)
}

fn path_strip_args(mut path: Path) -> Path {
    for segment in path.segments.iter_mut() {
        segment.arguments = PathArguments::None;
    }
    path
}

fn path_param(read: bool) -> DocArg {
    let s = if read { "read" } else { "written" };
    DocArg::new_param(
        "path".into(),
        PyType::PyClass("~pathlib.Path".into()),
        format!("Path to be {s}"),
    )
}

fn textdelim_param() -> DocArg {
    let t = textdelim_path();
    DocArg::new_param_def(
        "delim".into(),
        PyType::Int,
        "Delimiter to use when writing *TEXT*.".into(),
        DocDefault::Other(quote! {#t::default()}, "30".into()),
    )
}

fn big_other_param() -> DocArg {
    DocArg::new_param_def(
        "big_other".into(),
        PyType::Bool,
        "If ``True`` use 20 chars for OTHER segment offsets, and 8 otherwise.".into(),
        DocDefault::Bool(false),
    )
}

fn param_type_set_meas(version: Version) -> DocArg {
    let meas_pytype = ArgData::new_measurements_arg(version).doc.pytype;
    DocArg::new_param(
        "measurements".into(),
        meas_pytype,
        "The new measurements.".into(),
    )
}

fn param_allow_shared_names() -> DocArg {
    DocArg::new_param_def(
        "allow_shared_names".into(),
        PyType::Bool,
        "If ``False``, raise exception if any non-measurement keywords reference \
         any *$PnN* keywords. If ``True`` raise exception if any non-measurement \
         keywords reference a *$PnN* which is not present in ``measurements``. \
         In other words, ``False`` forbids named references to exist, and \
         ``True`` allows named references to be updated. References cannot \
         be broken in either case."
            .into(),
        DocDefault::Bool(false),
    )
}

// TODO this can be specific to each version, for instance, we can call out
// the exact keywords in each that may have references.
fn param_skip_index_check() -> DocArg {
    DocArg::new_param_def(
        "skip_index_check".into(),
        PyType::Bool,
        "If ``False``, raise exception if any non-measurement keyword have an \
         index reference to the current measurements. If ``True`` allow such \
         references to exist as long as they do not break (which really means \
         that the length of ``measurements`` is such that existing indices are \
         satisfied)."
            .into(),
        DocDefault::Bool(false),
    )
}

fn param_index(desc: &str) -> DocArg {
    DocArg::new_param("index".into(), PyType::Int, desc.into())
}

fn param_col() -> DocArg {
    DocArg::new_param(
        "col".into(),
        PyType::PyClass("polars.Series".into()),
        "Data for measurement. Must be same length as existing columns.".into(),
    )
}

fn param_name(short_desc: &str) -> DocArg {
    DocArg::new_param(
        "name".into(),
        PyType::Str,
        format!("{short_desc}. Corresponds to *$PnN*. Must not contain commas."),
    )
}

fn param_range() -> DocArg {
    DocArg::new_param(
        "range".into(),
        PyType::Float,
        "Range of measurement. Corresponds to *$PnR*.".into(),
    )
}

fn param_notrunc() -> DocArg {
    DocArg::new_param_def(
        "notrunc".into(),
        PyType::Bool,
        "If ``False``, raise exception if ``range`` must be truncated to fit \
         into measurement type."
            .into(),
        DocDefault::Bool(false),
    )
}

fn optical_pytype(version: Version) -> PyType {
    PyType::PyClass(format!("Optical{}", version.short_underscore()))
}

fn temporal_pytype(version: Version) -> PyType {
    PyType::PyClass(format!("Temporal{}", version.short_underscore()))
}

fn measurement_pytype(version: Version) -> PyType {
    PyType::new_union2(optical_pytype(version), temporal_pytype(version))
}

fn version_pytype() -> PyType {
    PyType::new_lit(&["FCS2.0", "FCS3.0", "FCS3.1", "FCS3.2"])
}

fn temporal_optical_key_pytype() -> PyType {
    PyType::new_lit(&[
        "F",
        "L",
        "O",
        "T",
        "P",
        "V",
        "CALIBRATION",
        "DET",
        "TAG",
        "FEATURE",
        "ANALYTE",
    ])
}

fn datatype_pytype() -> PyType {
    PyType::new_lit(&["A", "I", "F", "D"])
}

fn display_pytype() -> PyType {
    PyType::Tuple(vec![PyType::Bool, PyType::Float, PyType::Float])
}

fn feature_pytype() -> PyType {
    PyType::new_lit(&["Area", "Width", "Height"])
}

fn calibration3_1_pytype() -> PyType {
    PyType::Tuple(vec![PyType::Float, PyType::Str])
}

fn calibration3_2_pytype() -> PyType {
    PyType::Tuple(vec![PyType::Float, PyType::Float, PyType::Str])
}

fn segment_pytype() -> PyType {
    PyType::Tuple(vec![PyType::Int, PyType::Int])
}

fn correction_pytype() -> PyType {
    PyType::Tuple(vec![PyType::Int, PyType::Int])
}

fn element_path(version: Version) -> Path {
    let otype = pyoptical(version);
    let ttype = pytemporal(version);
    let element_path = quote!(fireflow_core::text::named_vec::Element);
    parse_quote!(#element_path<#ttype, #otype>)
}

fn meas_index_path() -> Path {
    parse_quote!(fireflow_core::text::index::MeasIndex)
}

fn keyword_path(n: &str) -> Path {
    let t = format_ident!("{n}");
    parse_quote!(fireflow_core::text::keywords::#t)
}

fn fcs_df_path() -> Path {
    parse_quote!(fireflow_core::validated::dataframe::FCSDataFrame)
}

fn textdelim_path() -> Path {
    parse_quote!(fireflow_core::validated::textdelim::TEXTDelim)
}

fn shortname_path() -> Path {
    parse_quote!(fireflow_core::validated::shortname::Shortname)
}

fn versioned_shortname_path(version: Version) -> Path {
    let shortname_path = shortname_path();
    match version {
        Version::FCS2_0 | Version::FCS3_0 => parse_quote!(Option<#shortname_path>),
        _ => shortname_path,
    }
}

fn versioned_family_path(version: Version) -> Path {
    let root = quote!(fireflow_core::text::optional);
    match version {
        Version::FCS2_0 | Version::FCS3_0 => parse_quote!(#root::MaybeFamily),
        _ => parse_quote!(#root::AlwaysFamily),
    }
}

fn pyoptical(version: Version) -> Ident {
    format_ident!("PyOptical{}", version.short_underscore())
}

fn pytemporal(version: Version) -> Ident {
    format_ident!("PyTemporal{}", version.short_underscore())
}

fn pycoredataset(version: Version) -> Ident {
    format_ident!("PyCoreDataset{}", version.short_underscore())
}

const ALL_VERSIONS: [Version; 4] = [
    Version::FCS2_0,
    Version::FCS3_0,
    Version::FCS3_1,
    Version::FCS3_2,
];

const CHRONO_REF: &str =
    "`chrono <https://docs.rs/chrono/latest/chrono/format/strftime/index.html>`__";

const REGEXP_REF: &str = "`regexp-syntax <https://docs.rs/regex-syntax/latest/regex_syntax/>`__";

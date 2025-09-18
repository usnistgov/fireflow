extern crate proc_macro;

use fireflow_core::header::Version;

use derive_more::{Display, From};
use derive_new::new;
use itertools::Itertools;
use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote, ToTokens};
use std::fmt;
use std::marker::PhantomData;
use syn::{
    parse::{Parse, ParseStream},
    parse_macro_input, parse_quote,
    token::Comma,
    GenericArgument, Ident, LitBool, LitInt, Path, PathArguments, Type,
};

#[proc_macro]
pub fn def_fcs_read_header(_: TokenStream) -> TokenStream {
    let fun_path = quote!(fireflow_core::api::fcs_read_header);
    let ret_path = quote!(fireflow_core::header::Header);
    let args = DocArgParam::new_header_config_param();
    let inner_args: Vec<_> = args.iter().map(|a| a.record_into()).collect();
    let doc = DocString::new_fun(
        "Read the *HEADER* of an FCS file.".into(),
        vec![],
        [DocArg::new_path_param(true)]
            .into_iter()
            .chain(args)
            .collect(),
        Some(DocReturn::new(PyType::PyClass("Header".into()), None)),
    );
    let fun_args = doc.fun_args();
    quote! {
        #[pyfunction]
        #doc
        pub fn fcs_read_header(#fun_args) -> PyResult<#ret_path> {
            let inner = fireflow_core::config::HeaderConfigInner{
                #(#inner_args),*
            };
            let conf =  fireflow_core::config::ReadHeaderConfig(inner);
            #fun_path(&path, &conf).py_termfail_resolve_nowarn()
        }
    }
    .into()
}

// #[proc_macro]
// pub fn impl_py_header_segments(input: TokenStream) -> TokenStream {
//     let path = parse_macro_input!(input as Path);
//     let name = path.segments.last().unwrap().ident;
//     let doc = DocString::new(
//         "The segments from *HEADER*".into(),
//         vec![],
//         DocSelf::NoSelf,
//         vec![],
//         None,
//     );
//     let (pyname, wrapped) = impl_pywrap(name.to_string(), path, &doc);
//     let text_q = quote! {
//         #[getter]
//         fn text(&self) -> fireflow_core::segment::PrimaryTextSegment {
//             self.0.text
//         }
//     };
//     quote! {
//         #wrapped

//         #[pymethods]
//         impl #pyname {
//             #(#getters)
//         }
//     }
//     .into()
// }

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

    let meas = DocArg::new_measurements_param(version).into();
    let layout = DocArg::new_layout_ivar(version).into();
    let df = DocArg::new_df_ivar().into();
    let analysis = DocArg::new_analysis_ivar().into();
    let others = DocArg::new_others_ivar().into();

    let mode = if version < Version::FCS3_2 {
        let t = PyType::new_lit(&["L", "U", "C"]);
        let m = quote!(fireflow_core::text::keywords::Mode::default());
        let d = DocDefault::Other(m, "\"L\"".into());
        DocArg::new_kw_ivar("Mode", "mode", t, None, Some(d))
    } else {
        DocArg::new_kw_opt_ivar("Mode3_2", "mode", PyType::new_lit(&["L"]))
    };

    let cyt = if version < Version::FCS3_2 {
        DocArg::new_kw_opt_ivar("Cyt", "cyt", PyType::Str)
    } else {
        DocArg::new_kw_ivar("Cyt", "cyt", PyType::Str, None, None)
    };

    let abrt = DocArg::new_kw_opt_ivar("Abrt", "abrt", PyType::Int);
    let com = DocArg::new_kw_opt_ivar("Com", "com", PyType::Str);
    let cells = DocArg::new_kw_opt_ivar("Cells", "cells", PyType::Str);
    let exp = DocArg::new_kw_opt_ivar("Exp", "exp", PyType::Str);
    let fil = DocArg::new_kw_opt_ivar("Fil", "fil", PyType::Str);
    let inst = DocArg::new_kw_opt_ivar("Inst", "inst", PyType::Str);
    let lost = DocArg::new_kw_opt_ivar("Lost", "lost", PyType::Int);
    let op = DocArg::new_kw_opt_ivar("Op", "op", PyType::Str);
    let proj = DocArg::new_kw_opt_ivar("Proj", "proj", PyType::Str);
    let smno = DocArg::new_kw_opt_ivar("Smno", "smno", PyType::Str);
    let src = DocArg::new_kw_opt_ivar("Src", "src", PyType::Str);
    let sys = DocArg::new_kw_opt_ivar("Sys", "sys", PyType::Str);
    let cytsn = DocArg::new_kw_opt_ivar("Cytsn", "cytsn", PyType::Str);

    let unicode_pytype = PyType::Tuple(vec![PyType::Int, PyType::new_list(PyType::Str)]);
    let unicode = DocArg::new_kw_opt_ivar("Unicode", "unicode", unicode_pytype);

    let csvbits = DocArg::new_kw_opt_ivar("CSVBits", "csvbits", PyType::Int);
    let cstot = DocArg::new_kw_opt_ivar("CSTot", "cstot", PyType::Int);

    let csvflags = DocArg::new_csvflags_ivar();

    let all_subset = [csvbits, cstot, csvflags];

    let last_modifier = DocArg::new_kw_opt_ivar("LastModifier", "last_modifier", PyType::Datetime);
    let last_modified = DocArg::new_kw_opt_ivar("LastModified", "last_modified", PyType::Str);
    let originality = DocArg::new_kw_opt_ivar(
        "Originality",
        "originality",
        PyType::new_lit(&["Original", "NonDataModified", "Appended", "DataModified"]),
    );

    let all_modified = [last_modifier, last_modified, originality];

    let plateid = DocArg::new_kw_opt_ivar("Plateid", "plateid", PyType::Str);
    let platename = DocArg::new_kw_opt_ivar("Platename", "platename", PyType::Str);
    let wellid = DocArg::new_kw_opt_ivar("Wellid", "wellid", PyType::Str);

    let all_plate = [plateid, platename, wellid];

    let vol = DocArg::new_kw_opt_ivar("Vol", "vol", PyType::Float);

    let comp_or_spill = match version {
        Version::FCS2_0 => DocArg::new_comp_ivar(true),
        Version::FCS3_0 => DocArg::new_comp_ivar(false),
        _ => DocArg::new_spillover_ivar(),
    };

    let flowrate = DocArg::new_kw_opt_ivar("Flowrate", "flowrate", PyType::Str);

    let carrierid = DocArg::new_kw_opt_ivar("Carrierid", "carrierid", PyType::Str);
    let carriertype = DocArg::new_kw_opt_ivar("Carriertype", "carriertype", PyType::Str);
    let locationid = DocArg::new_kw_opt_ivar("Locationid", "locationid", PyType::Str);

    let all_carrier = [carrierid, carriertype, locationid];

    let unstainedcenters = DocArg::new_unstainedcenters_ivar();
    let unstainedinfo = DocArg::new_kw_opt_ivar("UnstainedInfo", "unstainedinfo", PyType::Str);

    let tr = DocArg::new_trigger_ivar();

    let all_timestamps = match version {
        Version::FCS2_0 => DocArg::new_timestamps_ivar("FCSTime"),
        Version::FCS3_0 => DocArg::new_timestamps_ivar("FCSTime60"),
        Version::FCS3_1 | Version::FCS3_2 => DocArg::new_timestamps_ivar("FCSTime100"),
    };

    let all_datetimes = [
        DocArg::new_datetime_ivar(true),
        DocArg::new_datetime_ivar(false),
    ];

    let applied_gates = DocArg::new_applied_gates_ivar(version);

    let nonstandard_keywords = DocArg::new_core_nonstandard_keywords_ivar();

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

    let all_kws: Vec<AnyDocArg> = match version {
        Version::FCS2_0 => [mode, cyt, comp_or_spill]
            .into_iter()
            .chain(all_timestamps)
            .chain(common_kws)
            .map(|x| x.into())
            .collect(),
        Version::FCS3_0 => [mode, cyt, comp_or_spill]
            .into_iter()
            .chain(all_timestamps)
            .chain([cytsn, unicode])
            .chain(all_subset)
            .chain(common_kws)
            .map(|x| x.into())
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
            .map(|x| x.into())
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
            .map(|x| x.into())
            .collect(),
    };

    let meas_layout_args = [meas, layout];
    let coretext_args: Vec<_> = meas_layout_args
        .clone()
        .into_iter()
        .chain(all_kws.clone())
        .collect();
    let coredataset_args: Vec<_> = meas_layout_args
        .into_iter()
        .chain([df])
        .chain(all_kws)
        .chain([analysis, others])
        .collect();

    let coretext_inner_args: Vec<_> = coretext_args.iter().map(|x| x.ident_into()).collect();

    let coretext_doc = DocString::new_class(
        format!("Represents *TEXT* for an FCS {vs} file."),
        vec![],
        coretext_args,
    );

    let coredataset_doc = DocString::new_class(
        format!("Represents one dataset in an FCS {vs} file."),
        vec![],
        coredataset_args,
    );

    let coretext_new = |fun_args| {
        quote! {
            fn new(#fun_args) -> PyResult<Self> {
                Ok(#fun(#(#coretext_inner_args),*).mult_head()?.into())
            }
        }
    };

    let coredataset_new = |fun_args| {
        quote! {
            fn new(#fun_args) -> PyResult<Self> {
                let x = #fun(#(#coretext_inner_args),*).mult_head()?;
                Ok(x.into_coredataset(data, analysis, others)?.into())
            }
        }
    };

    let coretext_q = impl_new(
        coretext_name.to_string(),
        coretext_rstype,
        coretext_doc,
        coretext_new,
        quote!(),
    )
    .1;

    let coredataset_q = impl_new(
        coredataset_name.to_string(),
        coredataset_rstype,
        coredataset_doc,
        coredataset_new,
        quote!(),
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
    let doc = DocString::new_method(
        "Show the FCS version.".into(),
        vec![],
        vec![],
        Some(DocReturn::new(PyType::new_version(), None)),
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
    let doc = DocString::new_method(
        "The value for *$PAR*.".into(),
        vec![],
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

    let doc = DocString::new_method(
        "The non-standard keywords for each measurement.".into(),
        vec![],
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
        DocArg::new_bool_param(
            format!("exclude_{x}_{y}"),
            format!("Do not include {a} {b} keywords"),
        )
    };

    let doc = DocString::new_method(
        "Return standard keywords as string pairs.".into(),
        vec![
            "Each key will be prefixed with *$*.".into(),
            "This will not include *$TOT*, *$NEXTDATA* or any of the \
             offset keywords since these are not encoded in this class."
                .into(),
        ],
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

    let fun_args = doc.fun_args();
    let inner_args = doc.idents_into();

    quote! {
        #[pymethods]
        impl #t {
            #doc
            fn standard_keywords(&self, #fun_args) -> HashMap<String, String> {
                self.0.standard_keywords(#inner_args)
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_core_set_tr_threshold(input: TokenStream) -> TokenStream {
    let t = parse_macro_input!(input as Ident);
    let _ = split_ident_version_pycore(&t);
    let p = DocArg::new_param(
        "threshold".into(),
        PyType::Int,
        parse_quote!(u32),
        "The threshold to set.".into(),
    );
    let doc = DocString::new_method(
        "Set the threshold for *$TR*.".into(),
        vec![],
        vec![p],
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

    let write_2_0_warning = if version == Version::FCS2_0 {
        Some("Will raise exception if file cannot fit within 99,999,999 bytes.".into())
    } else {
        None
    };

    let doc = DocString::new_method(
        "Write data to path.".into(),
        ["Resulting FCS file will include *HEADER* and *TEXT*.".into()]
            .into_iter()
            .chain(write_2_0_warning.clone())
            .collect(),
        vec![
            DocArg::new_path_param(false),
            DocArg::new_textdelim_param(),
            DocArg::new_big_other_param(),
        ],
        None,
    );

    let fun_args = doc.fun_args();

    quote! {
        #[pymethods]
        impl #i {
            #doc
            fn write_text(&self, #fun_args) -> PyResult<()> {
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

    let write_2_0_warning = if version == Version::FCS2_0 {
        Some("Will raise exception if file cannot fit within 99,999,999 bytes.".into())
    } else {
        None
    };

    let doc = DocString::new_method(
        "Write data as an FCS file.".into(),
        ["The resulting file will include *HEADER*, *TEXT*, *DATA*, \
            *ANALYSIS*, and *OTHER* as they present from this class."
            .into()]
        .into_iter()
        .chain(write_2_0_warning)
        .collect(),
        vec![
            DocArg::new_path_param(false),
            DocArg::new_textdelim_param(),
            DocArg::new_big_other_param(),
            DocArg::new_bool_param(
                "skip_conversion_check".into(),
                "Skip check to ensure that types of the dataframe match the \
                 columns (*$PnB*, *$DATATYPE*, etc). If this is ``False``, \
                 perform this check before writing, and raise exception on \
                 failure. If ``True``, raise warnings as file is being \
                 written. Skipping this is faster since the data needs to be \
                 traversed twice to perform the conversion check, but may \
                 result in loss of precision and/or truncation."
                    .into(),
            ),
        ],
        None,
    );

    let fun_args = doc.fun_args();

    quote! {
        #[pymethods]
        impl #i {
            #doc
            fn write_dataset(&self, #fun_args) -> PyResult<()> {
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
        let doc = DocString::new_method(
            format!("The value of *$P{k}n* for all measurements."),
            vec![],
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

    let doc = DocString::new_method(
        "Value of *$PnN* for all measurements.".into(),
        vec!["Strings are unique and cannot contain commas.".into()],
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

// TODO this can be abstracted into a generic getter/setter method which
// makes a standalone impl
#[proc_macro]
pub fn impl_core_all_shortnames_maybe_attr(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let _ = split_ident_version_pycore(&i).1;
    let shortname_path = shortname_path();

    let doc = DocString::new_method(
        "The possibly-empty values of *$PnN* for all measurements.".into(),
        vec!["*$PnN* is optional for this FCS version so values may be ``None``.".into()],
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
    let get_doc = DocString::new_method(
        "The value of *$TIMESTEP*".into(),
        vec![],
        vec![],
        Some(DocReturn::new(t.clone(), None)),
    )
    .doc();
    let param = DocArg::new_param(
        "timestep".into(),
        PyType::Float,
        timestep_path.clone(),
        "The timestep to set. Must be greater than zero.".into(),
    );
    let set_doc = DocString::new_method(
        "Set the *$TIMESTEP* if time measurement is present.".into(),
        vec![],
        vec![param],
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
    let timestep_path = keyword_path("Timestep");
    let meas_index_path = meas_index_path();

    let make_doc = |has_timestep: bool, has_index: bool| {
        let name = DocArg::new_name_param("Name to set to temporal.");
        let index = DocArg::new_param(
            "index".into(),
            PyType::Int,
            meas_index_path.clone(),
            "Index to set".into(),
        );
        let (i, p) = if has_index {
            ("index", index)
        } else {
            ("name", name)
        };
        let timestep = if has_timestep {
            Some(DocArg::new_param(
                "timestep".into(),
                PyType::Float,
                timestep_path.clone(),
                "The value of *$TIMESTEP* to use.".into(),
            ))
        } else {
            None
        };
        let force = DocArg::new_bool_param(
            "force".into(),
            "If ``True`` remove any optical-specific metadata (detectors, \
             lasers, etc) without raising an exception. Defauls to ``False``."
                .into(),
        );
        let ps = [p].into_iter().chain(timestep).chain([force]).collect();
        DocString::new_method(
            format!("Set the temporal measurement to a given {i}."),
            vec![],
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
        let name_fun_args = name_doc.fun_args();
        let index_fun_args = index_doc.fun_args();
        quote! {
            #name_doc
            fn set_temporal(&mut self, #name_fun_args) -> PyResult<bool> {
                self.0.set_temporal(&name, (), force).py_termfail_resolve()
            }

            #index_doc
            fn set_temporal_at(&mut self, #index_fun_args) -> PyResult<bool> {
                self.0.set_temporal_at(index, (), force).py_termfail_resolve()
            }
        }
    } else {
        let name_doc = make_doc(true, false);
        let index_doc = make_doc(true, true);
        let name_fun_args = name_doc.fun_args();
        let index_fun_args = index_doc.fun_args();
        quote! {
            #name_doc
            fn set_temporal(&mut self, #name_fun_args) -> PyResult<bool> {
                self.0
                    .set_temporal(&name, timestep, force)
                    .py_termfail_resolve()
            }

            #index_doc
            fn set_temporal_at(&mut self, #index_fun_args) -> PyResult<bool> {
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
            Some(DocArg::new_bool_param(
                "force".into(),
                "If ``True`` and current time measurement has data which cannot \
                 be converted to optical, force the conversion anyways. \
                 Otherwise raise an exception."
                    .into(),
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
        DocString::new_method(s, vec![], p, Some(DocReturn::new(rt, Some(rd))))
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

// TODO can do the same thing for return that we are currently doing for args
// to keep them in sync
#[proc_macro]
pub fn impl_core_rename_temporal(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let _ = split_ident_version_pycore(&i).1;
    let shortname_path = shortname_path();

    let doc = DocString::new_method(
        "Rename temporal measurement if present.".into(),
        vec![],
        vec![DocArg::new_name_param("New name to assign.")],
        Some(DocReturn::new(
            PyType::new_opt(PyType::Str),
            Some("Previous name if present".into()),
        )),
    );

    let fun_args = doc.fun_args();

    quote! {
        #[pymethods]
        impl #i {
            #doc
            fn rename_temporal(&mut self, #fun_args) -> Option<#shortname_path> {
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
        let doc = DocString::new_method(
            "The value for *$PnE* for all measurements.".into(),
            vec![s0, s1],
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
        let doc = DocString::new_method(
            sum.into(),
            vec![s0.into(), s1.into(), s2.into(), s3.into()],
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

    let doc = DocString::new_method(
        "Get all measurements.".into(),
        vec![],
        vec![],
        Some(DocReturn::new(
            PyType::new_list(PyType::new_measurement(version)),
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

    let doc = DocString::new_method(
        "Get the temporal measurement if it exists.".into(),
        vec![],
        vec![],
        Some(DocReturn::new(
            PyType::new_opt(PyType::Tuple(vec![
                PyType::Int,
                PyType::Str,
                PyType::new_temporal(version),
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

    let doc = DocString::new_method(
        "Return measurement at index.".into(),
        vec!["Raise exception if ``index`` not found.".into()],
        vec![DocArg::new_index_param("Index to retrieve.")],
        Some(DocReturn::new(PyType::new_measurement(version), None)),
    );

    let fun_args = doc.fun_args();

    quote! {
        #[pymethods]
        impl #i {
            // TODO this should return name as well
            #doc
            fn measurement_at(&self, #fun_args) -> PyResult<#element_path> {
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

    let s = if is_dataset {
        "layout and dataframe"
    } else {
        "layout"
    };
    let ps = vec![format!(
        "Length of ``measurements`` must match number of columns in existing {s}."
    )];
    let doc = DocString::new_method(
        "Set all measurements at once.".into(),
        ps,
        vec![
            DocArg::new_type_set_meas_param(version),
            DocArg::new_allow_shared_names_param(),
            DocArg::new_skip_index_check_param(),
        ],
        None,
    );

    let fun_args = doc.fun_args();

    quote! {
        #[pymethods]
        impl #i {
            #doc
            fn set_measurements(&mut self, #fun_args) -> PyResult<()> {
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

    let push_meas_doc = |is_optical: bool, hasdata: bool| {
        let (meas_type, what, rstype) = if is_optical {
            (PyType::new_optical(version), "optical", pyoptical(version))
        } else {
            (
                PyType::new_temporal(version),
                "temporal",
                pytemporal(version),
            )
        };
        let param_meas = DocArg::new_param(
            "meas".into(),
            meas_type,
            parse_quote!(#rstype),
            "The measurement to push.".into(),
        );
        let col_param = if hasdata {
            Some(DocArg::new_col_param())
        } else {
            None
        };
        let ps: Vec<_> = [
            DocArg::new_name_param("Name of new measurement."),
            param_meas,
        ]
        .into_iter()
        .chain(col_param)
        .chain([DocArg::new_range_param(), DocArg::new_notrunc_param()])
        .collect();
        let summary = format!("Push {what} measurement to end of measurement vector.");
        DocString::new_method(summary, vec![], ps, None)
    };

    let opt_doc = push_meas_doc(true, is_dataset);
    let tmp_doc = push_meas_doc(false, is_dataset);

    let opt_fun_args = opt_doc.fun_args();
    let tmp_fun_args = tmp_doc.fun_args();

    let opt_inner_args = opt_doc.idents_into();
    let tmp_inner_args = tmp_doc.idents_into();

    quote! {
        #[pymethods]
        impl #i {
            #opt_doc
            fn push_optical(&mut self, #opt_fun_args) -> PyResult<()> {
                self.0
                    .push_optical(#opt_inner_args)
                    .py_termfail_resolve()
                    .void()
            }

            #tmp_doc
            fn push_temporal(&mut self, #tmp_fun_args) -> PyResult<()> {
                self.0
                    .push_temporal(#tmp_inner_args)
                    .py_termfail_resolve()
            }
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
    let family_path = versioned_family_path(version);
    let meas_index_path = meas_index_path();

    let by_name_doc = DocString::new_method(
        "Remove a measurement with a given name.".into(),
        vec!["Raise exception if ``name`` not found.".into()],
        vec![DocArg::new_name_param("Name to remove")],
        Some(DocReturn::new(
            PyType::Tuple(vec![PyType::Int, PyType::new_measurement(version)]),
            Some("Index and measurement object".into()),
        )),
    );

    let by_index_doc = DocString::new_method(
        "Remove a measurement with a given index.".into(),
        vec!["Raise exception if ``index`` not found.".into()],
        vec![DocArg::new_index_param("Index to remove")],
        Some(DocReturn::new(
            PyType::Tuple(vec![PyType::Str, PyType::new_measurement(version)]),
            Some("Name and measurement object".into()),
        )),
    );

    let bare_element_path = quote!(fireflow_core::text::named_vec::Element);

    let name_arg = by_name_doc.fun_args();
    let index_arg = by_index_doc.fun_args();

    let name_ident = by_name_doc.idents();
    let index_ident = by_index_doc.idents();

    quote! {
        #[pymethods]
        impl #i {
            #by_name_doc
            fn remove_measurement_by_name(
                &mut self,
                #name_arg
            ) -> PyResult<(#meas_index_path, #element_path)> {
                Ok(self
                   .0
                   .remove_measurement_by_name(&#name_ident)
                   .map(|(i, x)| (i, x.inner_into()))?)
            }

            #by_index_doc
            fn remove_measurement_by_index(
                &mut self,
                #index_arg
            ) -> PyResult<(#ver_shortname_path, #element_path)> {
                let r = self.0.remove_measurement_by_index(#index_ident)?;
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

    // TODO not DRY
    let insert_meas_doc = |is_optical: bool, hasdata: bool| {
        let (meas_type, what, rstype) = if is_optical {
            (PyType::new_optical(version), "optical", pyoptical(version))
        } else {
            (
                PyType::new_temporal(version),
                "temporal",
                pytemporal(version),
            )
        };
        let param_meas = DocArg::new_param(
            "meas".into(),
            meas_type.clone(),
            parse_quote!(#rstype),
            "The measurement to insert.".into(),
        );
        let col_param = if hasdata {
            Some(DocArg::new_col_param())
        } else {
            None
        };
        let summary = format!("Insert {what} measurement at position in measurement vector.");
        let ps: Vec<_> = [
            DocArg::new_index_param("Position at which to insert new measurement."),
            DocArg::new_name_param("Name of new measurement."),
            param_meas,
        ]
        .into_iter()
        .chain(col_param)
        .chain([DocArg::new_range_param(), DocArg::new_notrunc_param()])
        .collect();
        DocString::new_method(summary, vec![], ps, None)
    };

    let opt_doc = insert_meas_doc(true, is_dataset);
    let tmp_doc = insert_meas_doc(false, is_dataset);

    let opt_fun_args = opt_doc.fun_args();
    let tmp_fun_args = tmp_doc.fun_args();

    let opt_inner_args = opt_doc.idents_into();
    let tmp_inner_args = tmp_doc.idents_into();

    quote! {
        #[pymethods]
        impl #i {
            #opt_doc
            fn insert_optical(
                &mut self,
                #opt_fun_args
            ) -> PyResult<()> {
                self.0
                    .insert_optical(#opt_inner_args)
                    .py_termfail_resolve()
                    .void()
            }

            #tmp_doc
            fn insert_temporal(
                &mut self,
                #tmp_fun_args
            ) -> PyResult<()> {
                self.0
                    .insert_temporal(#tmp_inner_args)
                    .py_termfail_resolve()
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_core_replace_optical(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let version = split_ident_version_pycore(&i).1;

    let otype = pyoptical(version);
    let element_path = element_path(version);

    let make_replace_doc = |is_index: bool| {
        let (i, i_param, m) = if is_index {
            (
                "index",
                DocArg::new_index_param("Index to replace."),
                "measurement at index",
            )
        } else {
            (
                "name",
                DocArg::new_name_param("Name to replace."),
                "named measurement",
            )
        };
        let meas_desc = format!("Optical measurement to replace measurement at ``{i}``.");
        let sub = format!("Raise exception if ``{i}`` does not exist.");
        DocString::new_method(
            format!("Replace {m} with given optical measurement."),
            vec![sub],
            vec![
                i_param,
                DocArg::new_param(
                    "meas".into(),
                    PyType::new_optical(version),
                    parse_quote!(#otype),
                    meas_desc,
                ),
            ],
            Some(DocReturn::new(
                PyType::new_measurement(version),
                Some("Replaced measurement object".into()),
            )),
        )
    };

    let replace_at_doc = make_replace_doc(true);
    let replace_named_doc = make_replace_doc(false);

    let index_fun_args = replace_at_doc.fun_args();
    let name_fun_args = replace_named_doc.fun_args();

    quote! {
        #[pymethods]
        impl #i {
            #replace_at_doc
            fn replace_optical_at(&mut self, #index_fun_args) -> PyResult<#element_path> {
                Ok(self.0.replace_optical_at(index, meas.into())?.inner_into())
            }

            #replace_named_doc
            fn replace_optical_named(&mut self, #name_fun_args) -> Option<#element_path> {
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
    let element_path = element_path(version);

    let force_param = DocArg::new_bool_param(
        "force".into(),
        "If ``True``, do not raise exception if existing temporal measurement \
         cannot be converted to optical measurement."
            .into(),
    );

    // the temporal replacement functions for 3.2 are different because they
    // can fail if $PnTYPE is set
    let (replace_tmp_at_body, replace_tmp_named_body, force) = if version == Version::FCS3_2 {
        let go = |fun, x| quote!(self.0.#fun(#x, meas.into(), force).py_termfail_resolve()?);
        (
            go(quote! {replace_temporal_at_lossy}, quote! {index}),
            go(quote! {replace_temporal_named_lossy}, quote! {&name}),
            Some(force_param),
        )
    } else {
        (
            quote! {self.0.replace_temporal_at(index, meas.into())?},
            quote! {self.0.replace_temporal_named(&name, meas.into())},
            None,
        )
    };

    let make_replace_doc = |is_index: bool| {
        let (i_param, m) = if is_index {
            (
                DocArg::new_index_param("Index to replace."),
                "measurement at index",
            )
        } else {
            (
                DocArg::new_name_param("Name to replace."),
                "named measurement",
            )
        };
        let i = &i_param.argname;
        let meas_desc = format!("Temporal measurement to replace measurement at ``{i}``.");
        let sub = format!(
            "Raise exception if ``{i}`` does not exist  or there \
             is already a temporal measurement in a different position."
        );
        let args = [
            i_param,
            DocArg::new_param(
                "meas".into(),
                PyType::new_temporal(version),
                parse_quote!(#ttype),
                meas_desc,
            ),
        ];
        DocString::new_method(
            format!("Replace {m} with given temporal measurement."),
            vec![sub],
            args.into_iter().chain(force.clone()).collect(),
            Some(DocReturn::new(
                PyType::new_measurement(version),
                Some("Replaced measurement object".into()),
            )),
        )
    };

    let replace_at_doc = make_replace_doc(true);
    let replace_named_doc = make_replace_doc(false);

    let index_fun_args = replace_at_doc.fun_args();
    let name_fun_args = replace_named_doc.fun_args();

    quote! {
        #[pymethods]
        impl #i {
            #replace_at_doc
            fn replace_temporal_at(
                &mut self,
                #index_fun_args
            ) -> PyResult<#element_path> {
                let ret = #replace_tmp_at_body;
                Ok(ret.inner_into())
            }

            #replace_named_doc
            fn replace_temporal_named(
                &mut self,
                #name_fun_args
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

    let std_args = DocArg::new_std_config_params(version);
    let layout_args = DocArg::new_layout_config_params(version);
    let shared_args = DocArg::new_shared_config_params();

    let sk = quote!(fireflow_core::validated::keys::StdKey);
    let nsk = quote!(fireflow_core::validated::keys::NonStdKey);
    let std: Path = parse_quote!(std::collections::HashMap<#sk, String>);
    let nonstd: Path = parse_quote!(std::collections::HashMap<#nsk, String>);

    let std_inner_args: Vec<_> = std_args.iter().map(|a| a.record_into()).collect();
    let layout_inner_args: Vec<_> = layout_args.iter().map(|a| a.record_into()).collect();
    let shared_inner_args: Vec<_> = shared_args.iter().map(|a| a.record_into()).collect();

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
        std.clone(),
        format!("Standard keywords. {no_kws}"),
    );

    let nonstd_param = DocArg::new_param(
        "nonstd".into(),
        PyType::new_dict(PyType::Str, PyType::Str),
        nonstd.clone(),
        "Non-Standard keywords.".into(),
    );

    let config_params = std_args
        .iter()
        .chain(layout_args.iter())
        .chain(shared_args.iter())
        .cloned();

    let params = [std_param, nonstd_param]
        .into_iter()
        .chain(config_params)
        .collect();

    let doc = DocString::new_fun(
        "Make new instance from keywords.".into(),
        vec![],
        params,
        Some(DocReturn::new(PyType::PyClass(ident.to_string()), None)),
    );

    let fun_args = doc.fun_args();

    quote! {
        #[pymethods]
        impl #pyname {
            #[classmethod]
            #[allow(clippy::too_many_arguments)]
            #doc
            fn from_kws(_: &Bound<'_, pyo3::types::PyType>, #fun_args) -> PyResult<Self> {
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

    let std_args = DocArg::new_std_config_params(version);
    let layout_args = DocArg::new_layout_config_params(version);
    let offsets_args = DocArg::new_offsets_config_params(version);
    let reader_args = DocArg::new_reader_config_params();
    let shared_args = DocArg::new_shared_config_params();

    let std_inner_args: Vec<_> = std_args.iter().map(|a| a.record_into()).collect();
    let layout_inner_args: Vec<_> = layout_args.iter().map(|a| a.record_into()).collect();
    let offsets_inner_args: Vec<_> = offsets_args.iter().map(|a| a.record_into()).collect();
    let reader_inner_args: Vec<_> = reader_args.iter().map(|a| a.record_into()).collect();
    let shared_inner_args: Vec<_> = shared_args.iter().map(|a| a.record_into()).collect();

    let config_args: Vec<_> = std_args
        .into_iter()
        .chain(layout_args)
        .chain(offsets_args)
        .chain(reader_args)
        .chain(shared_args)
        .collect();

    let std_conf = quote!(fireflow_core::config::StdTextReadConfig);
    let layout_conf = quote!(fireflow_core::config::ReadLayoutConfig);
    let offsets_conf = quote!(fireflow_core::config::ReadTEXTOffsetsConfig);
    let reader_conf = quote!(fireflow_core::config::ReaderConfig);
    let shared_conf = quote!(fireflow_core::config::SharedConfig);
    let core_conf = quote!(fireflow_core::config::ReadStdDatasetFromKeywordsConfig);

    let sk_path = quote!(fireflow_core::validated::keys::StdKey);
    let nsk_path = quote!(fireflow_core::validated::keys::NonStdKey);
    let std_path: Path = parse_quote!(std::collections::HashMap<#sk_path, String>);
    let nonstd_path: Path = parse_quote!(std::collections::HashMap<#nsk_path, String>);

    let data_seg_path: Path = parse_quote!(fireflow_core::segment::HeaderDataSegment);
    let analysis_seg_path: Path = parse_quote!(fireflow_core::segment::HeaderAnalysisSegment);
    let other_segs_path: Path = parse_quote!(Vec<fireflow_core::segment::OtherSegment20>);

    let path_param = DocArg::new_path_param(true);

    let std_param = DocArg::new_param(
        "std".into(),
        PyType::new_dict(PyType::Str, PyType::Str),
        std_path.clone(),
        "Standard keywords.".into(),
    );

    let nonstd_param = DocArg::new_param(
        "nonstd".into(),
        PyType::new_dict(PyType::Str, PyType::Str),
        nonstd_path.clone(),
        "Non-Standard keywords.".into(),
    );

    let data_seg_param = DocArg::new_param(
        "data_seg".into(),
        PyType::new_segment(),
        data_seg_path.clone(),
        "The *DATA* segment from *HEADER*.".into(),
    );

    let analysis_seg_param = DocArg::new_param_def(
        "analysis_seg".into(),
        PyType::new_segment(),
        analysis_seg_path.clone(),
        "The *ANALYSIS* segment from *HEADER*.".into(),
        DocDefault::Other(quote!(#analysis_seg_path::default()), "(0, 0)".into()),
    );

    let other_segs_param = DocArg::new_param_def(
        "other_segs".into(),
        PyType::new_list(PyType::new_segment()),
        other_segs_path.clone(),
        "The *OTHER* segments from *HEADER*.".into(),
        DocDefault::EmptyList,
    );

    let all_args = [
        path_param,
        std_param,
        nonstd_param,
        data_seg_param,
        analysis_seg_param,
        other_segs_param,
    ]
    .into_iter()
    .chain(config_args)
    .collect();

    let doc = DocString::new_fun(
        "Make new instance from keywords.".into(),
        vec![],
        all_args,
        Some(DocReturn::new(PyType::PyClass(ident.to_string()), None)),
    );

    let fun_args = doc.fun_args();

    quote! {
        #[pymethods]
        impl #pyname {
            #[classmethod]
            #[allow(clippy::too_many_arguments)]
            #doc
            fn from_kws(_: &Bound<'_, pyo3::types::PyType>, #fun_args) -> PyResult<Self> {
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
    let s = "Remove measurements and clear the layout.";
    let p0 = "This is equivalent to deleting all *$Pn\\** keywords and setting \
              *$PAR* to 0.";
    let p1 = "Will raise exception if other keywords (such as *$TR*) reference \
              a measurement.";

    let doc = DocString::new_method(s.into(), vec![p0.into(), p1.into()], vec![], None);

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

    let doc = DocString::new_method(
        "Remove all measurements and their data.".into(),
        vec!["Raise exception if any keywords (such as *$TR*) reference a measurement.".into()],
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

    let p = DocArg::new_bool_param(
        "skip_conv_check".into(),
        "If ``True``, silently truncate data; otherwise return warnings when \
         truncation is performed."
            .into(),
    );

    let doc = DocString::new_method(
        "Coerce all values in DATA to fit within types specified in layout.".into(),
        vec!["This will always create a new copy of DATA in-place.".into()],
        vec![p],
        None,
    );

    let fun_arg = doc.fun_args();
    let inner_arg = doc.idents();

    quote! {
        #[pymethods]
        impl #i {
            #doc
            fn truncate_data(&mut self, #fun_arg) -> PyResult<()> {
                self.0.truncate_data(#inner_arg).py_term_resolve_noerror()
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_core_set_measurements_and_layout(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let (is_dataset, version) = split_ident_version_pycore(&i);

    let layout = DocArg::new_layout_ivar(version);
    let layout_argtype = layout.rstype;

    let param_type_set_layout = DocArg::new_param(
        "layout".into(),
        layout.pytype,
        layout_argtype.clone(),
        "The new layout.".into(),
    );

    let s = if is_dataset {
        " and both must match number of columns in existing dataframe"
    } else {
        ""
    };
    let ps = vec![
        "This is equivalent to updating all *$PnN* keywords at once.".into(),
        format!("Length of ``measurements`` must match number of columns in ``layout`` {s}."),
    ];
    let doc = DocString::new_method(
        "Set all measurements at once.".into(),
        ps,
        vec![
            DocArg::new_type_set_meas_param(version),
            param_type_set_layout.clone(),
            DocArg::new_allow_shared_names_param(),
            DocArg::new_skip_index_check_param(),
        ],
        None,
    );

    let fun_args = doc.fun_args();

    quote! {
        #[pymethods]
        impl #i {
            #doc
            fn set_measurements_and_layout(&mut self, #fun_args) -> PyResult<()> {
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

    let df_pytype = DocArg::new_df_ivar().pytype;

    let param_type_set_df = DocArg::new_param(
        "data".into(),
        df_pytype,
        fcs_df_path(),
        "The new data.".into(),
    );

    let doc = DocString::new_method(
        "Set measurements and data at once.".into(),
        vec!["Length of ``measurements`` must match number of columns in ``data``.".into()],
        vec![
            DocArg::new_type_set_meas_param(version),
            param_type_set_df,
            DocArg::new_allow_shared_names_param(),
            DocArg::new_skip_index_check_param(),
        ],
        None,
    );

    let fun_args = doc.fun_args();

    quote! {
        #[pymethods]
        impl #i {
            #doc
            fn set_measurements_and_data(&mut self, #fun_args) -> PyResult<()> {
                self.0
                    .set_measurements_and_data(
                        measurements.0.inner_into(),
                        data,
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

    let data = DocArg::new_data_param();
    let analysis = DocArg::new_analysis_param();
    let others = DocArg::new_others_param();

    let doc = DocString::new_method(
        "Convert to a dataset object.".into(),
        vec!["This will fully represent an FCS file, as opposed to just \
             representing *HEADER* and *TEXT*."
            .into()],
        vec![data, analysis, others],
        Some(DocReturn::new(PyType::PyClass(to_name), None)),
    );

    let fun_args = doc.fun_args();
    let inner_args = doc.idents();

    quote! {
        #[pymethods]
        impl #i {
            #doc
            fn to_dataset(&self, #fun_args) -> PyResult<#to_rstype> {
                Ok(self.0.clone().into_coredataset(#inner_args)?.into())
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
        DocArg::new_scale_ivar()
    } else {
        DocArg::new_transform_ivar()
    };

    let wavelength = if version < Version::FCS3_1 {
        DocArg::new_meas_kw_opt_ivar("Wavelength", "wavelength", "L", PyType::Float)
    } else {
        DocArg::new_meas_kw_opt_ivar(
            "Wavelengths",
            "wavelengths",
            "L",
            PyType::new_list(PyType::Float),
        )
    };

    let bin = DocArg::new_meas_kw_ivar(
        "PeakBin",
        "bin",
        PyType::Int,
        "Value of *$PKn*.".into(),
        Some(DocDefault::Option),
    );
    let size = DocArg::new_meas_kw_ivar(
        "PeakNumber",
        "size",
        PyType::Int,
        "Value of *$PKNn*.".into(),
        Some(DocDefault::Option),
    );

    let all_peak = [bin, size];

    let filter = DocArg::new_meas_kw_opt_ivar("Filter", "filter", "F", PyType::Str);

    let power = DocArg::new_meas_kw_opt_ivar("Power", "power", "O", PyType::Float);

    let detector_type =
        DocArg::new_meas_kw_opt_ivar("DetectorType", "detector_type", "T", PyType::Str);

    let percent_emitted =
        DocArg::new_meas_kw_opt_ivar("PercentEmitted", "percent_emitted", "P", PyType::Str);

    let detector_voltage =
        DocArg::new_meas_kw_opt_ivar("DetectorVoltage", "detector_voltage", "V", PyType::Float);

    let all_common_optical = [
        filter,
        power,
        detector_type,
        percent_emitted,
        detector_voltage,
    ];

    let calibration3_1 = DocArg::new_meas_kw_ivar(
        "Calibration3_1",
        "calibration",
        PyType::new_calibration3_1(),
        Some("Value of *$PnCALIBRATION*. Tuple encodes slope and calibration units."),
        Some(DocDefault::Option),
    );

    let calibration3_2 = DocArg::new_meas_kw_ivar(
        "Calibration3_2",
        "calibration",
        PyType::new_calibration3_2(),
        Some(
            "Value of *$PnCALIBRATION*. Tuple encodes slope, intercept, \
             and calibration units.",
        ),
        Some(DocDefault::Option),
    );

    let display = DocArg::new_meas_kw_ivar(
        "Display",
        "display",
        PyType::new_display(),
        Some(
            "Value of *$PnD*. First member of tuple encodes linear or log display \
             (``False`` and ``True`` respectively). The float members encode \
             lower/upper and decades/offset for linear and log scaling respectively.",
        ),
        Some(DocDefault::Option),
    );

    let analyte = DocArg::new_meas_kw_opt_ivar("Analyte", "analyte", "ANALYTE", PyType::Str);

    let feature =
        DocArg::new_meas_kw_opt_ivar("Feature", "feature", "FEATURE", PyType::new_feature());

    let detector_name =
        DocArg::new_meas_kw_opt_ivar("DetectorName", "detector_name", "DET", PyType::Str);

    let tag = DocArg::new_meas_kw_opt_ivar("Tag", "tag", "TAG", PyType::Str);

    let measurement_type =
        DocArg::new_meas_kw_opt_ivar("OpticalType", "measurement_type", "TYPE", PyType::Str);

    let has_scale_methods = GetSetMethods::new(
        quote! {
            fn get_has_scale(&self) -> bool {
                self.0.specific.scale.0.is_some()
            }
        },
        quote! {
            fn set_has_scale(&mut self, has_scale: bool) {
                self.0.specific.scale = if has_scale {
                    Some(fireflow_core::text::keywords::TemporalScale)
                } else {
                    None
                }.into();
            }
        },
    );
    let has_scale = DocArg::new_bool_param(
        "has_scale".into(),
        "``True`` if *$PnE* is set to ``0,0``.".into(),
    )
    .into_rw(has_scale_methods);

    let has_type_methods = GetSetMethods::new(
        quote! {
            fn get_has_type(&self) -> bool {
                self.0.specific.measurement_type.0.is_some()
            }
        },
        quote! {
            fn set_has_type(&mut self, has_type: bool) {
                self.0.specific.measurement_type = if has_type {
                    Some(fireflow_core::text::keywords::TemporalType)
                } else {
                    None
                }.into();
            }
        },
    );
    let has_type = DocArg::new_bool_param(
        "has_type".into(),
        "``True`` if *$PnTYPE* is set to ``\"Time\"``.".into(),
    )
    .into_rw(has_type_methods);

    let timestep_path = keyword_path("Timestep");
    let timestep_methods = GetSetMethods::new(
        quote! {
            fn get_timestep(&self) -> #timestep_path {
                self.0.specific.timestep
            }
        },
        quote! {
            fn set_timestep(&mut self, timestep: #timestep_path) {
                self.0.specific.timestep = timestep
            }
        },
    );
    let timestep = DocArg::new_ivar_rw(
        "timestep".into(),
        PyType::Float,
        timestep_path.clone(),
        "Value of *$TIMESTEP*.".into(),
        timestep_methods,
    );

    let longname = DocArg::new_meas_kw_opt_ivar("Longname", "longname", "S", PyType::Str);

    let nonstd = DocArg::new_meas_nonstandard_keywords_ivar();

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

    let inner_args: Vec<_> = all_args.iter().map(|x| x.ident_into()).collect();

    let doc = DocString::new_class(
        format!("FCS {version_s} *$Pn\\** keywords for {lower_basename} measurement."),
        vec![],
        all_args.into_iter().map(|x| x.into()).collect(),
    );

    let new_method = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #fun(#(#inner_args),*).into()
            }
        }
    };

    impl_new(name.to_string(), path, doc, new_method, quote!())
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
    fn parse(input: ParseStream) -> syn::Result<Self> {
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

impl<T> DocArg<T> {
    fn quoted_methods(&self) -> TokenStream2
    where
        T: IsMethods,
    {
        self.methods.quoted_methods()
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
    core_all_meas_attr(&i, "Display", "displays", "D", PyType::new_display())
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
        PyType::new_calibration3_1(),
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
        PyType::new_calibration3_2(),
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
    core_all_optical_attr(&i, "Feature", "features", "FEATURE", PyType::new_feature())
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

    let doc = DocString::new_method(
        doc_summary,
        doc_middle,
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
            let param = DocArg::new_bool_param("force".into(), param_desc.into());
            let doc = DocString::new_method(
                format!("Convert to FCS {vs}."),
                vec![sub.into()],
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
        let methods = GetSetMethods::new(
            quote! {
                fn #get(&self) -> Option<#rstype> {
                    self.0.#inner.0.as_ref().cloned()
                }
            },
            quote! {
                fn #set(&mut self, x: Option<#rstype>) {
                    self.0.#inner.0 = x.into();
                }
            },
        );
        (rstype, methods)
    };

    let (scale_path, scale_methods) = make_get_set("scale", "GateScale");
    let scale = DocArg::new_opt_ivar_rw(
        "scale".into(),
        PyType::new_scale(),
        scale_path,
        "The *$GmE* keyword. ``()`` means linear scaling and 2-tuple \
         specifies decades and offset for log scaling."
            .into(),
        scale_methods,
    );

    let make_arg = |n: &str, kw: &str, t: &str, p: PyType| {
        let (path, methods) = make_get_set(n, t);
        DocArg::new_opt_ivar_rw(
            n.into(),
            p,
            path,
            format!("The *$Gm{kw}* keyword."),
            methods,
        )
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

    let ps = all_args.iter().map(|x| x.clone().into()).collect();
    let summary = "The *$Gm\\** keywords for one gated measurement.".into();
    let doc = DocString::new_class(summary, vec![], ps);

    let inner_args: Vec<_> = all_args.iter().map(|x| x.ident_into()).collect();

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #path::new(#(#inner_args),*).into()
            }
        }
    };

    impl_new(name.to_string(), path.clone(), doc, new, quote!())
        .1
        .into()
}

#[proc_macro]
pub fn impl_new_fixed_ascii_layout(input: TokenStream) -> TokenStream {
    let path: Path = syn::parse(input).unwrap();
    let name = path.segments.last().unwrap().ident.clone();
    let bare_path = path_strip_args(path.clone());

    let chars_getter = GetMethod(quote! {
        fn ranges(&self) -> Vec<u64> {
            self.0.columns().iter().map(|c| c.value()).collect()
        }
    });

    let chars_param = DocArg::new_ivar_ro(
        "ranges".into(),
        PyType::new_list(PyType::Int),
        parse_quote!(Vec<u64>),
        "The range for each measurement. Equivalent to *$PnR*. The value of \
         *$PnB* will be derived from these and will be equivalent to the number \
         of digits for each value."
            .into(),
        chars_getter,
    );

    let constr_doc = DocString::new_class(
        "A fixed-width ASCII layout.".into(),
        vec![],
        vec![chars_param.into()],
    );

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #bare_path::new_ascii_u64(ranges).into()
            }
        }
    };

    let char_widths_doc = DocString::new_method(
        "The width of each measurement in number of chars (read only).".into(),
        vec![
            "Equivalent to *$PnB*, which is the number of chars/digits used \
             to encode data for a given measurement."
                .into(),
        ],
        vec![],
        Some(DocReturn::new(PyType::new_list(PyType::Int), None)),
    )
    .doc();

    let datatype = make_layout_datatype("A");

    let rest = quote! {
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

    impl_new(name.to_string(), path, constr_doc, new, rest)
        .1
        .into()
}

#[proc_macro]
pub fn impl_new_delim_ascii_layout(input: TokenStream) -> TokenStream {
    let path: Path = syn::parse(input).unwrap();
    let name = path.segments.last().unwrap().ident.clone();
    let bare_path = path_strip_args(path.clone());

    let ranges_getter = GetMethod(quote! {
        fn ranges(&self) -> Vec<u64> {
            self.0.ranges.clone()
        }
    });

    let ranges_param = DocArg::new_ivar_ro(
        "ranges".into(),
        PyType::new_list(PyType::Int),
        parse_quote!(Vec<u64>),
        "The range for each measurement. Equivalent to the *$PnR* \
         keyword. This is not used internally and thus only represents \
         documentation at the user level."
            .into(),
        ranges_getter,
    );

    let constr_doc = DocString::new_class(
        "A delimited ASCII layout.".into(),
        vec![],
        vec![ranges_param.into()],
    );

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #bare_path::new(ranges).into()
            }
        }
    };

    let datatype = make_layout_datatype("A");

    impl_new(name.to_string(), path, constr_doc, new, datatype)
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

    let full_layout_path: Path = parse_quote!(#ordered_layout_path<#range_path, #known_tot_path>);

    let layout_name = format!("Ordered{base}{:02}Layout", nbits);

    let summary = format!("{nbits}-bit ordered {what} layout.");

    let ranges_getter = GetMethod(quote! {
        fn ranges(&self) -> Vec<#range_path> {
            self.0.columns().iter().map(|c| c.clone()).collect()
        }
    });

    let range_param = DocArg::new_ivar_ro(
        "ranges".into(),
        PyType::new_list(range_pytype),
        parse_quote!(Vec<#range_path>),
        range_desc.into(),
        ranges_getter,
    );

    let byteord_getter = GetMethod(quote! {
        fn byteord(&self) -> #sizedbyteord_path<#nbytes> {
            *self.0.as_ref()
        }
    });

    let byteord_param = DocArg::new_ivar_ro_def(
        "byteord".into(),
        PyType::new_union2(
            PyType::new_lit(&["big", "little"]),
            PyType::new_list(PyType::Int),
        ),
        parse_quote!(#sizedbyteord_path<#nbytes>),
        format!(
            "The byte order to use when encoding values. Must be ``\"big\"``, \
             ``\"little\"``, or a list of all integers between 1 and {nbytes} \
             in any order."
        ),
        DocDefault::Other(quote!(#sizedbyteord_path::default()), "\"little\"".into()),
        byteord_getter,
    );

    let is_big_param = make_endian_ord_param(2);

    let widths = make_byte_width(nbytes);
    let datatype = make_layout_datatype(dt);

    let rest = quote! {
        #widths
        #datatype
    };

    let make_doc = |args| DocString::new_class(summary, vec![], args);

    // make different constructors and getters for u8 and u16 since the byteord
    // for these can be simplified
    match (is_float, nbytes) {
        // u8 doesn't need byteord since only one is possible
        (false, 1) => {
            let doc = make_doc(vec![range_param.into()]);
            let new = |fun_args| {
                quote! {
                    fn new(#fun_args) -> Self {
                        #fixed_layout_path::new(ranges, #sizedbyteord_path::default()).into()
                    }
                }
            };
            impl_new(layout_name, full_layout_path, doc, new, rest)
        }

        // u16 only has two combinations (big and little) so don't allow a list
        // for byteord
        (false, 2) => {
            let doc = make_doc(vec![range_param.into(), is_big_param.into()]);
            let new = |fun_args| {
                quote! {
                    fn new(#fun_args) -> Self {
                        let b = #sizedbyteord_path::Endian(endian);
                        #fixed_layout_path::new(ranges, b).into()
                    }
                }
            };
            impl_new(layout_name, full_layout_path, doc, new, rest)
        }

        // everything else needs the "full" version of byteord, which is big,
        // little, and mixed (a list)
        _ => {
            let doc = make_doc(vec![range_param.into(), byteord_param.into()]);
            let new = |fun_args| {
                quote! {
                    fn new(#fun_args) -> Self {
                        #fixed_layout_path::new(ranges, byteord).into()
                    }
                }
            };
            impl_new(layout_name, full_layout_path, doc, new, rest)
        }
    }
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

    let full_layout_path = parse_quote!(#endian_layout_path<#range_path, #nomeasdt_path>);

    let layout_name = format!("EndianF{:02}Layout", nbits);

    let ranges_getter = GetMethod(quote! {
        fn ranges(&self) -> Vec<#range_path> {
            self.0.columns().iter().map(|c| c.clone()).collect()
        }
    });

    let range_param = DocArg::new_ivar_ro(
        "ranges".into(),
        PyType::new_list(PyType::Decimal),
        parse_quote!(Vec<#range_path>),
        "The range for each measurement. Corresponds to *$PnR*. This is not \
         used internally so only serves the users' own purposes."
            .into(),
        ranges_getter,
    );

    let is_big_param = make_endian_param(4);

    let constr_doc = DocString::new_class(
        format!("{nbits}-bit endian float layout"),
        vec![],
        vec![range_param.clone().into(), is_big_param.into()],
    );

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #fixed_layout_path::new(ranges, endian).into()
            }
        }
    };

    let widths = make_byte_width(nbytes);
    let datatype = make_layout_datatype(if nbytes == 4 { "F" } else { "D" });

    let rest = quote! {
        #widths
        #datatype
    };

    impl_new(layout_name, full_layout_path, constr_doc, new, rest)
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
    let layout_path = parse_quote!(#endian_layout<#bitmask, #nomeasdt>);

    let ranges_getter = GetMethod(quote! {
        fn ranges(&self) -> Vec<u64> {
            self.0.columns().iter().map(|c| u64::from(*c)).collect()
        }
    });

    let ranges_param: DocArgROIvar = DocArg::new_ivar_ro(
        "ranges".into(),
        PyType::new_list(PyType::Int),
        parse_quote!(Vec<u64>),
        "The range of each measurement. Corresponds to the *$PnR* \
         keyword less one. The number of bytes used to encode each \
         measurement (*$PnB*) will be the minimum required to express this \
         value. For instance, a value of ``1023`` will set *$PnB* to ``16``, \
         will set *$PnR* to ``1024``, and encode values for this measurement as \
         16-bit integers. The values of a measurement will be less than or \
         equal to this value."
            .into(),
        ranges_getter,
    );

    let is_big_param = make_endian_param(4);

    let constr_doc = DocString::new_class(
        "A mixed-width integer layout.".into(),
        vec![],
        vec![ranges_param.into(), is_big_param.into()],
    );

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                let rs = ranges.into_iter().map(#bitmask::from).collect();
                #fixed::new(rs, endian).into()
            }
        }
    };

    let datatype = make_layout_datatype("I");

    impl_new(name.to_string(), layout_path, constr_doc, new, datatype)
        .1
        .into()
}

#[proc_macro]
pub fn impl_new_mixed_layout(_: TokenStream) -> TokenStream {
    let name = format_ident!("MixedLayout");
    let layout_path = parse_quote!(fireflow_core::data::#name);

    let null = quote!(fireflow_core::data::NullMixedType);
    let fixed = quote!(fireflow_core::data::FixedLayout);

    let types_getter = GetMethod(quote! {
        fn typed_ranges(&self) -> Vec<#null> {
            self.0.columns().iter().map(|c| c.clone()).collect()
        }
    });

    let types_param: DocArgROIvar = DocArg::new_ivar_ro(
        "typed_ranges".into(),
        PyType::new_list(PyType::new_union2(
            PyType::Tuple(vec![PyType::new_lit(&["A", "I"]), PyType::Int]),
            PyType::Tuple(vec![PyType::new_lit(&["F", "D"]), PyType::Decimal]),
        )),
        parse_quote!(Vec<#null>),
        "The type and range for each measurement corresponding to *$DATATYPE* \
         and/or *$PnDATATYPE* and *$PnR* respectively. These are given \
         as 2-tuples like ``(<type>, <range>)`` where ``type`` is one of \
         ``\"A\"``, ``\"I\"``, ``\"F\"``, or ``\"D\"`` corresponding to Ascii, \
         Integer, Float, or Double datatypes respectively."
            .into(),
        types_getter,
    );

    let is_big_param = make_endian_param(4);

    let constr_doc = DocString::new_class(
        "A mixed-type layout.".into(),
        vec![],
        vec![types_param.into(), is_big_param.into()],
    );

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #fixed::new(typed_ranges, endian).into()
            }
        }
    };

    impl_new(name.to_string(), layout_path, constr_doc, new, quote!())
        .1
        .into()
}

// TODO not DRY
fn make_endian_ord_param(n: usize) -> DocArgROIvar {
    let xs = (1..(n + 1)).join(",");
    let ys = (1..(n + 1)).rev().join(",");
    let endian = parse_quote!(fireflow_core::text::byteord::Endian);
    let sizedbyteord_path = quote!(fireflow_core::text::byteord::SizedByteOrd);
    let getter = GetMethod(quote! {
        fn endian(&self) -> #endian {
            let m: #sizedbyteord_path<2> = *self.0.as_ref();
            m.endian()
        }
    });
    let d = quote!(#endian::Little);
    DocArg::new_ivar_ro_def(
        "endian".into(),
        PyType::new_lit(&["big", "little"]),
        endian,
        format!(
            "If ``\"big\"`` use big endian (``{ys}``) for encoding values; \
             if ``\"little\"`` use little endian (``{xs}``)."
        ),
        DocDefault::Other(d, "\"little\"".into()),
        getter,
    )
}

fn make_endian_param(n: usize) -> DocArgROIvar {
    let xs = (1..(n + 1)).join(",");
    let ys = (1..(n + 1)).rev().join(",");
    let endian = parse_quote!(fireflow_core::text::byteord::Endian);
    let getter = GetMethod(quote! {
        fn endian(&self) -> #endian {
            *self.0.as_ref()
        }
    });
    let d = quote!(#endian::Little);
    DocArg::new_ivar_ro_def(
        "endian".into(),
        PyType::new_lit(&["big", "little"]),
        endian,
        format!(
            "If ``\"big\"`` use big endian (``{ys}``) for encoding values; \
             if ``\"little\"`` use little endian (``{xs}``)."
        ),
        DocDefault::Other(d, "\"little\"".into()),
        getter,
    )
}

fn make_byte_width(nbytes: usize) -> TokenStream2 {
    let s0 = format!("Will always return ``{nbytes}``.");
    let s1 = "This corresponds to the value of *$PnB* divided by 8, which are \
              all the same for this layout."
        .into();
    let doc = DocString::new_method(
        "The width of each measurement in bytes (read only).".into(),
        vec![s0, s1],
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

    let doc = DocString::new_method(
        "The width of each measurement in bytes (read-only).".into(),
        vec![
            "This corresponds to the value of *$PnB* for each measurement \
             divided by 8. Values for each measurement may be different."
                .into(),
        ],
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

fn make_layout_datatype(dt: &str) -> TokenStream2 {
    let doc = DocString::new_method(
        "The value of *$DATATYPE* (read-only).".into(),
        vec![format!("Will always return ``\"{dt}\"``.")],
        vec![],
        Some(DocReturn::new(PyType::new_datatype(), None)),
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
    fn parse(input: ParseStream) -> syn::Result<Self> {
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

    let index_getter = GetMethod(quote! {
        fn index(&self) -> #index_rstype {
            self.0.index
        }
    });
    let gate_getter = GetMethod(quote! {
        fn #gate_argname(&self) -> #gate_rstype {
            self.0.#gate_argname.clone()
        }
    });

    let index_arg = DocArg::new_ivar_ro(
        "index".into(),
        index_pytype,
        index_rstype,
        index_desc,
        index_getter,
    );
    let gate_arg = DocArg::new_ivar_ro(
        gate_argname.to_string(),
        gate_pytype,
        gate_rstype,
        gate_desc,
        gate_getter,
    );

    let doc = DocString::new_class(summary, vec![], vec![index_arg.into(), gate_arg.into()]);

    let name = format!("{region_ident}{suffix}");

    let bare_path = path_strip_args(path.clone());

    let inner_args: Vec<_> = doc.args.iter().map(|a| a.record_into()).collect();

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #bare_path { #(#inner_args),* }.into()
            }
        }
    };

    impl_new(name, path, doc, new, quote!()).1.into()
}

fn impl_new<F>(
    name: String,
    path: Path,
    d: ClassDocString,
    constr: F,
    rest: TokenStream2,
) -> (Ident, TokenStream2)
where
    F: FnOnce(TokenStream2) -> TokenStream2,
{
    let (pyname, wrapped) = impl_pywrap(name, path, &d);
    let sig = d.sig();
    let get_set_methods = d.quoted_methods();
    let new = constr(d.fun_args());
    let s = quote! {
        #wrapped

        // TODO automatically make args here
        #[pymethods]
        impl #pyname {
            #sig
            #[new]
            #[allow(clippy::too_many_arguments)]
            #new

            #get_set_methods

            #rest
        }
    };
    (pyname, s)
}

fn impl_pywrap(name: String, path: Path, d: &ClassDocString) -> (Ident, TokenStream2) {
    let doc = d.doc();
    let pyname = format_ident!("Py{name}");
    let q = quote! {
        // pyo3 currently cannot add docstrings to __new__ methods, see
        // https://github.com/PyO3/pyo3/issues/4326
        //
        // workaround, put them on the structs themselves, which works but has the
        // disadvantage of being not next to the method def itself
        #doc
        #[pyclass(name = #name, eq)]
        #[derive(Clone, From, Into, PartialEq)]
        pub struct #pyname(#path);
    };
    (pyname, q)
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

fn analysis_path() -> Path {
    parse_quote!(fireflow_core::core::Analysis)
}

fn others_path() -> Path {
    parse_quote!(fireflow_core::core::Others)
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

#[derive(Clone, new)]
struct DocString<A, R, S> {
    summary: String,
    paragraphs: Vec<String>,
    args: Vec<A>,
    returns: R,
    _selfarg: PhantomData<S>,
}

type ClassDocString = DocString<AnyDocArg, (), NoSelf>;
type MethodDocString = DocString<DocArgParam, Option<DocReturn>, SelfArg>;
type FunDocString = DocString<DocArgParam, Option<DocReturn>, NoSelf>;

struct NoSelf;

struct SelfArg;

trait IsSelfArg {
    const ARG: Option<&'static str>;
}

impl IsSelfArg for NoSelf {
    const ARG: Option<&'static str> = None;
}

impl IsSelfArg for SelfArg {
    const ARG: Option<&'static str> = Some("self");
}

#[derive(Clone, From, Display)]
enum AnyDocArg {
    RWIvar(DocArgRWIvar),
    ROIvar(DocArgROIvar),
    Param(DocArgParam),
}

type DocArgRWIvar = DocArg<GetSetMethods>;
type DocArgROIvar = DocArg<GetMethod>;
type DocArgParam = DocArg<NoMethods>;

#[derive(Clone, new)]
struct DocArg<T> {
    argname: String,
    pytype: PyType,
    rstype: Path,
    desc: String,
    default: Option<DocDefault>,
    methods: T,
}

#[derive(Clone)]
struct NoMethods;

#[derive(Clone)]
struct GetMethod(TokenStream2);

#[derive(new, Clone)]
struct GetSetMethods {
    get: TokenStream2,
    set: TokenStream2,
}

trait IsMethods {
    fn quoted_methods(&self) -> TokenStream2;
}

impl IsMethods for NoMethods {
    fn quoted_methods(&self) -> TokenStream2 {
        quote!()
    }
}

impl IsMethods for GetMethod {
    fn quoted_methods(&self) -> TokenStream2 {
        let g = &self.0;
        quote! {
            #[getter]
            #g
        }
    }
}

impl IsMethods for GetSetMethods {
    fn quoted_methods(&self) -> TokenStream2 {
        let g = &self.get;
        let s = &self.set;
        quote! {
            #[getter]
            #g
            #[setter]
            #s
        }
    }
}

impl IsMethods for AnyDocArg {
    fn quoted_methods(&self) -> TokenStream2 {
        match self {
            Self::Param(x) => x.quoted_methods(),
            Self::ROIvar(x) => x.quoted_methods(),
            Self::RWIvar(x) => x.quoted_methods(),
        }
    }
}

#[derive(Clone)]
enum DocDefault {
    Bool(bool),
    EmptyDict,
    // EmptySet,
    EmptyList,
    Option,
    Other(TokenStream2, String),
}

#[derive(Clone, new)]
struct DocReturn {
    rtype: PyType,
    desc: Option<String>,
}

#[derive(Clone)]
enum PyType {
    Str,
    Bool,
    Bytes,
    Int,
    Float,
    None,
    Datetime,
    Decimal,
    Date,
    Time,
    Option(Box<PyType>),
    Dict(Box<PyType>, Box<PyType>),
    Union(Box<PyType>, Box<PyType>, Vec<PyType>),
    Tuple(Vec<PyType>),
    List(Box<PyType>),
    // Set(Box<PyType>),
    Literal(&'static str, Vec<&'static str>),
    PyClass(String),
}

impl DocArgROIvar {
    fn new_ivar_ro(
        argname: String,
        pytype: PyType,
        rstype: Path,
        desc: String,
        method: GetMethod,
    ) -> Self {
        Self::new(argname, pytype, rstype, desc, None, method)
    }

    fn new_ivar_ro_def(
        argname: String,
        pytype: PyType,
        rstype: Path,
        desc: String,
        def: DocDefault,
        method: GetMethod,
    ) -> Self {
        Self::new(argname, pytype, rstype, desc, Some(def), method)
    }
}

impl DocArgRWIvar {
    fn new_ivar_rw(
        argname: String,
        pytype: PyType,
        rstype: Path,
        desc: String,
        methods: GetSetMethods,
    ) -> Self {
        Self::new(argname, pytype, rstype, desc, None, methods)
    }

    fn new_ivar_rw_def(
        argname: String,
        pytype: PyType,
        rstype: Path,
        desc: String,
        def: DocDefault,
        methods: GetSetMethods,
    ) -> Self {
        Self::new(argname, pytype, rstype, desc, Some(def), methods)
    }

    fn new_opt_ivar_rw(
        argname: String,
        pytype: PyType,
        rstype: Path,
        desc: String,
        methods: GetSetMethods,
    ) -> Self {
        Self::new(
            argname,
            PyType::new_opt(pytype),
            parse_quote!(Option<#rstype>),
            desc,
            Some(DocDefault::Option),
            methods,
        )
    }

    fn new_kw_ivar(
        kw: &str,
        name: &str,
        pytype: PyType,
        desc: Option<&str>,
        def: Option<DocDefault>,
    ) -> Self {
        let path = keyword_path(kw);
        let get = format_ident!("get_{name}");
        let set = format_ident!("set_{name}");
        let n = name.to_string();

        let _desc = desc.map_or(format!("Value of *${}*.", name.to_uppercase()), |d| {
            d.to_string()
        });

        let make_methods = |optional: bool| {
            let get_inner = format_ident!("{}", if optional { "metaroot_opt" } else { "metaroot" });
            let clone_inner = format_ident!("{}", if optional { "cloned" } else { "clone" });
            let full_kw = if optional {
                parse_quote! {Option<#path>}
            } else {
                path.clone()
            };

            let m = GetSetMethods::new(
                quote! {
                    fn #get(&self) -> #full_kw {
                        self.0.#get_inner::<#path>().#clone_inner()
                    }
                },
                quote! {
                    fn #set(&mut self, x: #full_kw) {
                        self.0.set_metaroot(x)
                    }
                },
            );
            (full_kw, m)
        };

        if let Some(d) = def {
            let optional = matches!(d, DocDefault::Option);
            let t = if optional {
                PyType::new_opt(pytype)
            } else {
                pytype
            };
            let (p, m) = make_methods(optional);
            Self::new_ivar_rw_def(n, t, p, _desc, d, m)
        } else {
            let (p, m) = make_methods(false);
            Self::new_ivar_rw(n, pytype, p, _desc, m)
        }
    }

    fn new_meas_kw_ivar(
        kw: &str,
        name: &str,
        pytype: PyType,
        desc: Option<&str>,
        def: Option<DocDefault>,
    ) -> Self {
        let path = keyword_path(kw);
        let get = format_ident!("get_{name}");
        let set = format_ident!("set_{name}");
        let n = name.to_string();

        let _desc = desc.map_or(format!("Value of *${}*.", name.to_uppercase()), |d| {
            d.to_string()
        });

        let make_methods = |optional: bool| {
            let full_kw = if optional {
                parse_quote! {Option<#path>}
            } else {
                path.clone()
            };

            let m = GetSetMethods::new(
                quote! {
                    fn #get(&self) -> #full_kw {
                        let x: &#full_kw = self.0.as_ref();
                        x.as_ref().cloned()
                    }
                },
                quote! {
                    fn #set(&mut self, x: #full_kw) {
                        *self.0.as_mut() = x
                    }
                },
            );
            (full_kw, m)
        };

        if let Some(d) = def {
            let optional = matches!(d, DocDefault::Option);
            let t = if optional {
                PyType::new_opt(pytype)
            } else {
                pytype
            };
            let (p, m) = make_methods(optional);
            DocArg::new_ivar_rw_def(n, t, p, _desc, d, m)
        } else {
            let (p, m) = make_methods(false);
            DocArg::new_ivar_rw(n, pytype, p, _desc, m)
        }
    }

    fn new_kw_opt_ivar(kw: &str, name: &str, pytype: PyType) -> Self {
        Self::new_kw_ivar(kw, name, pytype, None, Some(DocDefault::Option))
    }

    fn new_meas_kw_opt_ivar(kw: &str, name: &str, abbr: &str, pytype: PyType) -> Self {
        let desc = format!("Value for *$Pn{abbr}*.");
        Self::new_meas_kw_ivar(
            kw,
            name,
            pytype,
            Some(desc.as_str()),
            Some(DocDefault::Option),
        )
    }

    fn new_layout_ivar(version: Version) -> Self {
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

        let methods = GetSetMethods::new(
            quote! {
                fn get_layout(&self) -> #layout_ident {
                    self.0.layout().clone().into()
                }
            },
            quote! {
                fn set_layout(&mut self, layout: #layout_ident) -> PyResult<()> {
                    self.0.set_layout(layout.into()).py_termfail_resolve_nowarn()
                }
            },
        );

        DocArg::new_ivar_rw(
            layout_argname.to_string(),
            layout_pytype.clone(),
            parse_quote!(#layout_ident),
            layout_desc.into(),
            methods,
        )
    }

    fn new_df_ivar() -> Self {
        // use polars df here because we need to manually add names
        let polars_df_type = quote! {pyo3_polars::PyDataFrame};
        let methods = GetSetMethods::new(
            quote! {
                fn data(&self) -> #polars_df_type {
                    let ns = self.0.all_shortnames();
                    let data = self.0.data();
                    #polars_df_type(data.as_polars_dataframe(&ns[..]))
                }
            },
            quote! {
                fn set_data(&mut self, df: #polars_df_type) -> PyResult<()> {
                    let data = df.0.try_into()?;
                    Ok(self.0.set_data(data)?)
                }
            },
        );
        DocArg::new_data_param().into_rw(methods)
    }

    fn new_analysis_ivar() -> Self {
        let analysis_rstype = analysis_path();
        let methods = GetSetMethods::new(
            quote! {
                fn analysis(&self) -> #analysis_rstype {
                    self.0.analysis.clone()
                }
            },
            quote! {
                fn set_analysis(&mut self, xs: #analysis_rstype) {
                    self.0.analysis = xs.into();
                }
            },
        );
        DocArg::new_analysis_param().into_rw(methods)
    }

    fn new_others_ivar() -> Self {
        let others_rstype = others_path();
        let methods = GetSetMethods::new(
            quote! {
                fn others(&self) -> #others_rstype {
                    self.0.others.clone()
                }
            },
            quote! {
                fn set_others(&mut self, xs: #others_rstype) {
                    self.0.others = xs
                }
            },
        );
        DocArg::new_others_param().into_rw(methods)
    }

    fn new_timestamps_ivar(time_name: &str) -> [Self; 3] {
        let nd = quote! {Option<chrono::NaiveDate>};
        let time_ident = format_ident!("{time_name}");
        let time_path = quote!(fireflow_core::text::timestamps::#time_ident);
        let date_rstype = parse_quote!(fireflow_core::text::timestamps::FCSDate);

        let make_time_ivar = |is_start: bool| {
            let nt = quote! {Option<chrono::NaiveTime>};
            let (name, wrap) = if is_start {
                ("btim", "Btim")
            } else {
                ("etim", "Etim")
            };
            let wrap_ident = format_ident!("{wrap}");
            let wrap_path = quote!(fireflow_core::text::timestamps::#wrap_ident);
            let rstype = parse_quote!(#wrap_path<#time_path>);
            let get = format_ident!("get_{name}");
            let set = format_ident!("set_{name}");
            let get_naive = format_ident!("{name}_naive");
            let set_naive = format_ident!("set_{name}_naive");
            let desc = format!("Value of *${}*.", name.to_uppercase());
            let methods = GetSetMethods::new(
                quote! {
                    fn #get(&self) -> #nt {
                        self.0.#get_naive()
                    }
                },
                quote! {
                    fn #set(&mut self, x: #nt) -> PyResult<()> {
                        Ok(self.0.#set_naive(x)?)
                    }
                },
            );
            DocArg::new_opt_ivar_rw(name.into(), PyType::Time, rstype, desc, methods)
        };

        let date_methods = GetSetMethods::new(
            quote! {
                fn get_date(&self) -> #nd {
                    self.0.date_naive()
                }
            },
            quote! {
                fn set_date(&mut self, x: #nd) -> PyResult<()> {
                    Ok(self.0.set_date_naive(x)?)
                }
            },
        );

        let date_arg = DocArg::new_opt_ivar_rw(
            "date".into(),
            PyType::Date,
            date_rstype,
            "Value of *$DATE*.".into(),
            date_methods,
        );

        [make_time_ivar(true), make_time_ivar(false), date_arg]
    }

    fn new_datetime_ivar(is_start: bool) -> Self {
        let dt = quote! {Option<chrono::DateTime<chrono::FixedOffset>>};
        let (name, type_name) = if is_start {
            ("begindatetime", "BeginDateTime")
        } else {
            ("enddatetime", "EndDateTime")
        };
        let type_ident = format_ident!("{type_name}");
        let rstype = parse_quote!(fireflow_core::text::datetimes::#type_ident);
        let get = format_ident!("{name}");
        let set = format_ident!("set_{name}");
        let methods = GetSetMethods::new(
            quote! {
                fn #get(&self) -> #dt {
                    self.0.#get()
                }
            },
            quote! {
                fn #set(&mut self, x: #dt) -> PyResult<()> {
                    Ok(self.0.#set(x)?)
                }
            },
        );
        DocArg::new_opt_ivar_rw(
            name.into(),
            PyType::Datetime,
            rstype,
            format!("Value for *${}*.", name.to_uppercase()),
            methods,
        )
    }

    fn new_comp_ivar(is_2_0: bool) -> Self {
        let rstype = parse_quote!(fireflow_core::text::compensation::Compensation);
        let methods = GetSetMethods::new(
            quote! {
                fn get_compensation(&self) -> Option<#rstype> {
                    self.0.compensation().cloned()
                }
            },
            quote! {
                fn set_compensation(&mut self, m: Option<#rstype>) -> PyResult<()> {
                    Ok(self.0.set_compensation(m)?)
                }
            },
        );
        let desc = if is_2_0 {
            "The compensation matrix. Must be a square array with number of \
             rows/columns equal to the number of measurements. Non-zero entries \
             will produce a *$DFCmTOn* keyword."
        } else {
            "The value of *$COMP*. Must be a square array with number of \
             rows/columns equal to the number of measurements."
        }
        .into();
        DocArg::new_opt_ivar_rw(
            "comp".into(),
            PyType::PyClass("~numpy.ndarray".into()),
            rstype,
            desc,
            methods,
        )
    }

    fn new_spillover_ivar() -> Self {
        let rstype = parse_quote!(fireflow_core::text::spillover::Spillover);
        let methods = GetSetMethods::new(
            quote! {
                fn get_spillover(&self) -> Option<#rstype> {
                    self.0.spillover().map(|x| x.clone())
                }
            },
            quote! {
                fn set_spillover(&mut self, spillover: Option<#rstype>) -> PyResult<()> {
                    Ok(self.0.set_spillover(spillover)?)
                }
            },
        );
        DocArg::new_opt_ivar_rw(
            "spillover".into(),
            PyType::Tuple(vec![
                PyType::new_list(PyType::Str),
                PyType::PyClass("~numpy.ndarray".into()),
            ]),
            rstype,
            "Value for *$SPILLOVER*. First element of tuple the list of measurement \
             names and the second is the matrix. Each measurement name must \
             correspond to a *$PnN*, must be unique, and the length of this list \
             must match the number of rows and columns of the matrix. The matrix \
             must be at least 2x2."
                .into(),
            methods,
        )
    }

    fn new_csvflags_ivar() -> Self {
        let path = parse_quote!(fireflow_core::core::CSVFlags);
        let rstype = quote!(Option<#path>);
        let methods = GetSetMethods::new(
            quote! {
                fn get_csvflags(&self) -> #rstype {
                    self.0.metaroot_opt::<#path>().cloned()
                }
            },
            quote! {
                fn set_csvflags(&mut self, x: #rstype) {
                    self.0.set_metaroot(x)
                }
            },
        );
        DocArg::new_opt_ivar_rw(
            "csvflags".into(),
            PyType::new_list(PyType::new_opt(PyType::Int)),
            path,
            "Subset flags. Each element in the list corresponds to *$CSVnFLAG* and \
             the length of the list corresponds to *$CSMODE*."
                .into(),
            methods,
        )
    }

    fn new_trigger_ivar() -> Self {
        let path = keyword_path("Trigger");
        let rstype = quote!(Option<#path>);

        let methods = GetSetMethods::new(
            quote! {
                fn trigger(&self) -> #rstype {
                    self.0.metaroot_opt().cloned()
                }
            },
            quote! {
                fn set_trigger(&mut self, tr: #rstype) -> PyResult<()> {
                    Ok(self.0.set_trigger(tr)?)
                }
            },
        );

        DocArg::new_opt_ivar_rw(
            "tr".into(),
            PyType::new_opt(PyType::Tuple(vec![PyType::Int, PyType::Str])),
            path,
            "Value for *$TR*. First member of tuple is threshold and second is the \
             measurement name which must match a *$PnN*."
                .into(),
            methods,
        )
    }

    fn new_unstainedcenters_ivar() -> Self {
        let path = parse_quote!(fireflow_core::text::unstainedcenters::UnstainedCenters);
        let rstype = quote!(Option<#path>);
        let methods = GetSetMethods::new(
            quote! {
                fn get_unstained_centers(&self) -> #rstype {
                    self.0.metaroot_opt::<#path>().map(|y| y.clone())
                }
            },
            quote! {
                fn set_unstained_centers(&mut self, us: #rstype) -> PyResult<()> {
                    self.0.set_unstained_centers(us).py_termfail_resolve_nowarn()
                }
            },
        );
        DocArg::new_opt_ivar_rw(
            "unstainedcenters".into(),
            PyType::new_dict(PyType::Str, PyType::Float),
            path,
            "Value for *$UNSTAINEDCENTERS. Each key must match a *$PnN*.".into(),
            methods,
        )
    }

    fn new_applied_gates_ivar(version: Version) -> Self {
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

        let methods = GetSetMethods::new(
            quote! {
                fn get_applied_gates(&self) -> #rstype {
                    self.0.metaroot::<#rstype_inner>().clone().into()
                }
            },
            setter_body,
        );

        DocArg::new_ivar_rw_def(
            "applied_gates".into(),
            pytype,
            parse_quote!(#rstype),
            desc,
            def,
            methods,
        )
    }

    fn new_scale_ivar() -> Self {
        let rstype = parse_quote! {fireflow_core::text::scale::Scale};
        let methods = GetSetMethods::new(
            quote! {
                fn get_scale(&self) -> Option<#rstype> {
                    self.0.specific.scale.0.as_ref().map(|&x| x)
                }
            },
            quote! {
                fn set_scale(&mut self, x: Option<#rstype>) {
                    self.0.specific.scale = x.into()
                }
            },
        );
        DocArg::new_opt_ivar_rw(
            "scale".into(),
            PyType::new_scale(),
            rstype,
            "Value for *$PnE*. Empty tuple means linear scale; 2-tuple encodes \
             decades and offset for log scale"
                .into(),
            methods,
        )
    }

    fn new_transform_ivar() -> Self {
        let rstype = parse_quote! {fireflow_core::core::ScaleTransform};
        let methods = GetSetMethods::new(
            quote! {
                fn get_transform(&self) -> #rstype {
                    self.0.specific.scale
                }
            },
            quote! {
                fn set_transform(&mut self, transform: #rstype) {
                    self.0.specific.scale = transform;
                }
            },
        );
        DocArg::new_ivar_rw(
            "transform".into(),
            PyType::Tuple(vec![
                PyType::Float,
                PyType::Tuple(vec![PyType::Float, PyType::Float]),
            ]),
            rstype,
            "Value for *$PnE* and/or *$PnG*. Singleton float encodes gain (*$PnG*) \
             and implies linear scaling (ie *$PnE* is ``0,0``). 2-tuple encodes \
             decades and offset for log scale, and implies *$PnG* is not set."
                .into(),
            methods,
        )
    }

    fn new_core_nonstandard_keywords_ivar() -> Self {
        Self::new_nonstandard_keywords_ivar(
            "Pairs of non-standard keyword values. Keys must not start with *$*.",
            quote!(self.0.metaroot),
        )
    }

    fn new_meas_nonstandard_keywords_ivar() -> Self {
        Self::new_nonstandard_keywords_ivar(
            "Any non-standard keywords corresponding to this measurement. No keys \
             should start with *$*. Realistically each key should follow a pattern \
             corresponding to the measurement index, something like prefixing with \
             \"P\" followed by the index. This is not enforced.",
            quote!(self.0.common),
        )
    }

    fn new_nonstandard_keywords_ivar(desc: &str, root: TokenStream2) -> Self {
        let nsk = quote!(fireflow_core::validated::keys::NonStdKey);
        let rstype = parse_quote!(std::collections::HashMap<#nsk, String>);
        let methods = GetSetMethods::new(
            quote! {
                fn get_nonstandard_keywords(&self) -> #rstype {
                    #root.nonstandard_keywords.clone()
                }
            },
            quote! {
                fn set_nonstandard_keywords(&mut self, kws: #rstype) {
                    #root.nonstandard_keywords = kws;
                }
            },
        );

        DocArg::new_ivar_rw_def(
            "nonstandard_keywords".into(),
            PyType::new_dict(PyType::Str, PyType::Str),
            rstype,
            desc.into(),
            DocDefault::EmptyDict,
            methods,
        )
    }
}

impl DocArgParam {
    fn new_param(argname: String, pytype: PyType, rstype: Path, desc: String) -> Self {
        Self::new(argname, pytype, rstype, desc, None, NoMethods)
    }

    fn new_param_def(
        argname: String,
        pytype: PyType,
        rstype: Path,
        desc: String,
        def: DocDefault,
    ) -> Self {
        Self::new(argname, pytype, rstype, desc, Some(def), NoMethods)
    }

    // fn into_ro(self, m: GetMethod) -> DocArgROIvar {
    //     DocArgROIvar::new(
    //         self.argname,
    //         self.pytype,
    //         self.rstype,
    //         self.desc,
    //         self.default,
    //         m,
    //     )
    // }

    fn into_rw(self, m: GetSetMethods) -> DocArgRWIvar {
        DocArgRWIvar::new(
            self.argname,
            self.pytype,
            self.rstype,
            self.desc,
            self.default,
            m,
        )
    }

    fn new_path_param(read: bool) -> DocArgParam {
        let s = if read { "read" } else { "written" };
        Self::new_param(
            "path".into(),
            PyType::PyClass("~pathlib.Path".into()),
            parse_quote!(std::path::PathBuf),
            format!("Path to be {s}"),
        )
    }

    fn new_textdelim_param() -> DocArgParam {
        let t = textdelim_path();
        Self::new_param_def(
            "delim".into(),
            PyType::Int,
            textdelim_path(),
            "Delimiter to use when writing *TEXT*.".into(),
            DocDefault::Other(quote! {#t::default()}, "30".into()),
        )
    }

    fn new_big_other_param() -> DocArgParam {
        Self::new_bool_param(
            "big_other".into(),
            "If ``True`` use 20 chars for OTHER segment offsets, and 8 otherwise.".into(),
        )
    }

    // TODO ???
    fn new_type_set_meas_param(version: Version) -> DocArgParam {
        let a = Self::new_measurements_param(version);
        Self::new_param(
            "measurements".into(),
            a.pytype,
            a.rstype,
            "The new measurements.".into(),
        )
    }

    fn new_allow_shared_names_param() -> DocArgParam {
        Self::new_bool_param(
            "allow_shared_names".into(),
            "If ``False``, raise exception if any non-measurement keywords reference \
             any *$PnN* keywords. If ``True`` raise exception if any non-measurement \
             keywords reference a *$PnN* which is not present in ``measurements``. \
             In other words, ``False`` forbids named references to exist, and \
             ``True`` allows named references to be updated. References cannot \
             be broken in either case."
                .into(),
        )
    }

    // TODO this can be specific to each version, for instance, we can call out
    // the exact keywords in each that may have references.
    fn new_skip_index_check_param() -> DocArgParam {
        Self::new_bool_param(
            "skip_index_check".into(),
            "If ``False``, raise exception if any non-measurement keyword have an \
             index reference to the current measurements. If ``True`` allow such \
             references to exist as long as they do not break (which really means \
             that the length of ``measurements`` is such that existing indices are \
             satisfied)."
                .into(),
        )
    }

    fn new_index_param(desc: &str) -> DocArgParam {
        Self::new_param("index".into(), PyType::Int, meas_index_path(), desc.into())
    }

    fn new_col_param() -> DocArgParam {
        Self::new_param(
            "col".into(),
            PyType::PyClass("polars.Series".into()),
            parse_quote!(fireflow_core::validated::dataframe::AnyFCSColumn),
            "Data for measurement. Must be same length as existing columns.".into(),
        )
    }

    fn new_name_param(short_desc: &str) -> DocArgParam {
        Self::new_param(
            "name".into(),
            PyType::Str,
            shortname_path(),
            format!("{short_desc}. Corresponds to *$PnN*. Must not contain commas."),
        )
    }

    fn new_range_param() -> DocArgParam {
        Self::new_param(
            "range".into(),
            PyType::Float,
            keyword_path("Range"),
            "Range of measurement. Corresponds to *$PnR*.".into(),
        )
    }

    fn new_notrunc_param() -> DocArgParam {
        Self::new_bool_param(
            "notrunc".into(),
            "If ``False``, raise exception if ``range`` must be truncated to fit \
             into measurement type."
                .into(),
        )
    }

    fn new_data_param() -> DocArgParam {
        // TODO fix cross-ref in docs here
        let df_pytype = PyType::PyClass("polars.DataFrame".into());
        Self::new_param(
            "data".into(),
            df_pytype,
            fcs_df_path(),
            "A dataframe encoding the contents of *DATA*. Number of columns must \
         match number of measurements. May be empty. Types do not necessarily \
         need to correspond to those in the data layout but mismatches may \
         result in truncation."
                .into(),
        )
    }

    fn new_analysis_param() -> DocArgParam {
        let analysis_rstype = analysis_path();
        let d = quote! {#analysis_rstype::default()};
        Self::new_param_def(
            "analysis".into(),
            PyType::Bytes,
            analysis_rstype,
            "A byte string encoding the *ANALYSIS* segment".into(),
            DocDefault::Other(d, "b\"\"".to_string()),
        )
    }

    fn new_others_param() -> DocArgParam {
        let others_rstype = others_path();
        let d = quote!(#others_rstype::default());
        Self::new_param_def(
            "others".into(),
            PyType::new_list(PyType::Bytes),
            others_rstype,
            "A list of byte strings encoding the *OTHER* segments".into(),
            DocDefault::Other(d, "[]".to_string()),
        )
    }

    fn new_measurements_param(version: Version) -> Self {
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
        let meas_argtype: Path =
            parse_quote!(PyEithers<#fam_path, #meas_tmp_pyname, #meas_opt_pyname>);

        Self::new_param(
            "measurements".into(),
            meas_pytype.clone(),
            meas_argtype,
            meas_desc.into(),
        )
    }

    fn new_bool_param(name: String, desc: String) -> Self {
        Self::new_param_def(
            name,
            PyType::Bool,
            parse_quote!(bool),
            desc,
            DocDefault::Bool(false),
        )
    }

    fn new_opt_param(name: String, pytype: PyType, desc: String, path: Path) -> Self {
        Self::new_param_def(
            name,
            PyType::new_opt(pytype),
            parse_quote!(Option<#path>),
            desc,
            DocDefault::Option,
        )
    }

    fn new_header_config_param() -> Vec<Self> {
        vec![
            Self::new_text_correction_param(),
            Self::new_data_correction_param(),
            Self::new_analysis_correction_param(),
            Self::new_other_corrections_param(),
            Self::new_max_other_param(),
            Self::new_other_width_param(),
            Self::new_squish_offsets_param(),
            Self::new_allow_negative_param(),
            Self::new_truncate_offsets_param(),
        ]
    }

    fn new_std_config_params(version: Version) -> Vec<Self> {
        let trim_intra_value_whitespace = Self::new_trim_intra_value_whitespace_param();
        let time_meas_pattern = Self::new_time_meas_pattern_param();
        let allow_missing_time = Self::new_allow_missing_time_param();
        let force_time_linear = Self::new_force_time_linear_param();
        let ignore_time_gain = Self::new_ignore_time_gain_param();
        let ignore_time_optical_keys = Self::new_ignore_time_optical_keys_param();
        let parse_indexed_spillover = Self::new_parse_indexed_spillover_param();
        let date_pattern = Self::new_date_pattern_param();
        let time_pattern = Self::new_time_pattern_param();
        let allow_pseudostandard = Self::new_allow_pseudostandard_param();
        let allow_unused_standard = Self::new_allow_unused_standard_param();
        let disallow_deprecated = Self::new_disallow_deprecated_param();
        let fix_log_scale_offsets = Self::new_fix_log_scale_offsets_param();
        let nonstandard_measurement_pattern = Self::new_nonstandard_measurement_pattern_param();

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

    fn new_layout_config_params(version: Version) -> Vec<Self> {
        let integer_widths_from_byteord = Self::new_integer_widths_from_byteord_param();
        let integer_byteord_override = Self::new_integer_byteord_override_param();
        let disallow_range_truncation = Self::new_disallow_range_truncation_param();

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

    fn new_offsets_config_params(version: Version) -> Vec<Self> {
        let text_data_correction = Self::new_text_data_correction_param();
        let text_analysis_correction = Self::new_text_analysis_correction_param();
        let ignore_text_data_offsets = Self::new_ignore_text_data_offsets_param();
        let ignore_text_analysis_offsets = Self::new_ignore_text_analysis_offsets_param();
        let allow_header_text_offset_mismatch = Self::new_allow_header_text_offset_mismatch_param();
        let allow_missing_required_offsets =
            Self::new_allow_missing_required_offsets_param(version);
        let truncate_text_offsets = Self::new_truncate_text_offsets_param();

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

    fn new_reader_config_params() -> Vec<Self> {
        let allow_uneven_event_width = Self::new_allow_uneven_event_width_param();
        let allow_tot_mismatch = Self::new_allow_tot_mismatch_param();
        vec![allow_uneven_event_width, allow_tot_mismatch]
    }

    fn new_shared_config_params() -> Vec<Self> {
        let warnings_are_errors = Self::new_warnings_are_errors_param();
        vec![warnings_are_errors]
    }

    fn new_trim_intra_value_whitespace_param() -> Self {
        Self::new_bool_param(
            "trim_intra_value_whitespace".into(),
            "If ``True``, trim whitespace between delimiters such as ``,`` \
             and ``;`` within keyword value strings."
                .into(),
        )
    }

    fn new_time_meas_pattern_param() -> Self {
        Self::new_opt_param(
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

    fn new_allow_missing_time_param() -> Self {
        Self::new_bool_param(
            "allow_missing_time".into(),
            "If ``True`` allow time measurement to be missing.".into(),
        )
    }

    fn new_force_time_linear_param() -> Self {
        Self::new_bool_param(
            "force_time_linear".into(),
            "If ``True`` force time measurement to be linear independent of *$PnE*.".into(),
        )
    }

    fn new_ignore_time_gain_param() -> Self {
        Self::new_bool_param(
            "ignore_time_gain".into(),
            "If ``True`` ignore the *$PnG* (gain) keyword. This keyword should not \
             be set according to the standard} however, this library will allow \
             gain to be 1.0 since this equates to identity. If gain is not 1.0, \
             this is nonsense and it can be ignored with this flag."
                .into(),
        )
    }

    fn new_ignore_time_optical_keys_param() -> Self {
        Self::new_param_def(
            "ignore_time_optical_keys".into(),
            PyType::new_list(PyType::new_temporal_optical_key()),
            parse_quote!(TemporalOpticalKeys),
            "Ignore optical keys in temporal measurement. These keys are \
             nonsensical for time measurements but are not explicitly forbidden in \
             the the standard. Provided keys are the string after the \"Pn\" in \
             the \"PnX\" keywords."
                .into(),
            DocDefault::Other(quote!(TemporalOpticalKeys::default()), "[]".into()),
        )
    }

    fn new_parse_indexed_spillover_param() -> Self {
        Self::new_bool_param(
            "parse_indexed_spillover".into(),
            "Parse $SPILLOVER with numeric indices rather than strings \
             (ie names or *$PnN*)"
                .into(),
        )
    }

    fn new_date_pattern_param() -> Self {
        Self::new_opt_param(
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
    fn new_time_pattern_param() -> Self {
        Self::new_opt_param(
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

    fn new_allow_pseudostandard_param() -> Self {
        Self::new_bool_param(
            "allow_pseudostandard".into(),
            "If ``True`` allow non-standard keywords with a leading *$*. The \
             presence of such keywords often means the version in *HEADER* \
             is incorrect."
                .into(),
        )
    }

    fn new_allow_unused_standard_param() -> Self {
        Self::new_bool_param(
            "allow_unused_standard".into(),
            "If ``True`` allow unused standard keywords to be present.".into(),
        )
    }

    fn new_disallow_deprecated_param() -> Self {
        Self::new_bool_param(
            "disallow_deprecated".into(),
            "If ``True`` throw error if a deprecated key is encountered.".into(),
        )
    }

    fn new_fix_log_scale_offsets_param() -> Self {
        Self::new_bool_param(
            "fix_log_scale_offsets".into(),
            "If ``True`` fix log-scale *PnE* and keywords which have zero offset \
             (ie ``X,0,0`` where ``X`` is non-zero)."
                .into(),
        )
    }

    fn new_nonstandard_measurement_pattern_param() -> Self {
        Self::new_opt_param(
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

    fn new_integer_widths_from_byteord_param() -> Self {
        Self::new_bool_param(
            "integer_widths_from_byteord".into(),
            "If ``True`` set all *$PnB* to the number of bytes from *$BYTEORD*. \
             Only has an effect for FCS 2.0/3.0 where *$DATATYPE* is ``I``."
                .into(),
        )
    }

    fn new_integer_byteord_override_param() -> Self {
        Self::new_opt_param(
            "integer_byteord_override".into(),
            PyType::new_list(PyType::Int),
            "Override *$BYTEORD* for integer layouts.".into(),
            parse_quote!(fireflow_core::text::byteord::ByteOrd2_0),
        )
    }

    fn new_disallow_range_truncation_param() -> Self {
        Self::new_bool_param(
            "disallow_range_truncation".into(),
            "If ``True`` throw error if *$PnR* values need to be truncated \
             to match the number of bytes specified by *$PnB* and *$DATATYPE*."
                .into(),
        )
    }

    fn new_config_correction_arg(name: &str, what: &str, location: &str, rstype: Path) -> Self {
        Self::new_param_def(
            name.into(),
            PyType::new_correction(),
            parse_quote!(fireflow_core::segment::#rstype),
            format!("Corrections for *{what}* offsets in *{location}*"),
            DocDefault::Other(
                quote!(fireflow_core::segment::OffsetCorrection::default()),
                "(0, 0)".into(),
            ),
        )
    }

    fn new_text_correction_param() -> Self {
        Self::new_config_correction_arg(
            "text_correction",
            "TEXT",
            "HEADER",
            parse_quote!(HeaderCorrection<fireflow_core::segment::PrimaryTextSegmentId>),
        )
    }

    fn new_data_correction_param() -> Self {
        Self::new_config_correction_arg(
            "data_correction",
            "DATA",
            "HEADER",
            parse_quote!(HeaderCorrection<fireflow_core::segment::DataSegmentId>),
        )
    }

    fn new_analysis_correction_param() -> Self {
        Self::new_config_correction_arg(
            "analysis_correction",
            "ANALYSIS",
            "HEADER",
            parse_quote!(HeaderCorrection<fireflow_core::segment::AnalysisSegmentId>),
        )
    }

    fn new_other_corrections_param() -> Self {
        let id_path = quote!(fireflow_core::segment::OtherSegmentId);
        let corr_path = quote!(fireflow_core::segment::HeaderCorrection);
        Self::new_param_def(
            "other_corrections".into(),
            PyType::new_list(PyType::new_correction()),
            parse_quote!(Vec<#corr_path<#id_path>>),
            "Corrections for OTHER offsets if they exist. Each correction will \
             be applied in order. If an offset does not need to be corrected, \
             use ``(0, 0)``. This will not affect the number of OTHER segments \
             that are read; this is controlled by ``max_other``."
                .into(),
            DocDefault::EmptyList,
        )
    }

    fn new_max_other_param() -> Self {
        Self::new_opt_param(
            "max_other".into(),
            PyType::Int,
            "Maximum number of OTHER segments that can be parsed. \
             ``None`` means limitless."
                .into(),
            parse_quote!(usize),
        )
    }

    fn new_other_width_param() -> Self {
        let path = parse_quote!(fireflow_core::validated::ascii_range::OtherWidth);
        let d = quote!(#path::default());
        Self::new_param_def(
            "other_width".into(),
            PyType::Int,
            path,
            "Maximum number of OTHER segments that can be parsed. \
             ``None`` means limitless."
                .into(),
            DocDefault::Other(d, "8".into()),
        )
    }

    // this only matters for 3.0+ files
    fn new_squish_offsets_param() -> Self {
        Self::new_bool_param(
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

    fn new_allow_negative_param() -> Self {
        Self::new_bool_param(
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

    fn new_truncate_offsets_param() -> Self {
        Self::new_bool_param(
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

    fn new_text_data_correction_param() -> Self {
        Self::new_config_correction_arg(
            "text_data_correction",
            "DATA",
            "TEXT",
            parse_quote!(TEXTCorrection<fireflow_core::segment::DataSegmentId>),
        )
    }

    fn new_text_analysis_correction_param() -> Self {
        Self::new_config_correction_arg(
            "text_analysis_correction",
            "ANALYSIS",
            "TEXT",
            parse_quote!(TEXTCorrection<fireflow_core::segment::AnalysisSegmentId>),
        )
    }

    fn new_ignore_text_data_offsets_param() -> Self {
        Self::new_bool_param(
            "ignore_text_data_offsets".into(),
            "If ``True`` ignore *DATA* offsets in *TEXT*".into(),
        )
    }

    fn new_ignore_text_analysis_offsets_param() -> Self {
        Self::new_bool_param(
            "ignore_text_analysis_offsets".into(),
            "If ``True`` ignore *ANALYSIS* offsets in *TEXT*".into(),
        )
    }

    fn new_allow_header_text_offset_mismatch_param() -> Self {
        Self::new_bool_param(
            "allow_header_text_offset_mismatch".into(),
            "If ``True`` allow *TEXT* and *HEADER* offsets to mismatch.".into(),
        )
    }

    fn new_allow_missing_required_offsets_param(version: Version) -> Self {
        let s = if version >= Version::FCS3_2 {
            "*DATA*"
        } else {
            "*DATA* and *ANALYSIS*"
        };
        Self::new_bool_param(
            "allow_missing_required_offsets".into(),
            format!(
                "If ``True`` allow required {s} offsets in *TEXT* to be missing. \
                 If missing, fall back to offsets from *HEADER*."
            ),
        )
    }

    fn new_truncate_text_offsets_param() -> Self {
        Self::new_bool_param(
            "truncate_text_offsets".into(),
            "If ``True`` truncate offsets that exceed end of file.".into(),
        )
    }

    fn new_allow_uneven_event_width_param() -> Self {
        Self::new_bool_param(
            "allow_uneven_event_width".into(),
            "If ``True`` allow event width to not perfectly divide length of *DATA*. \
            Does not apply to delimited ASCII layouts. "
                .into(),
        )
    }

    fn new_allow_tot_mismatch_param() -> Self {
        Self::new_bool_param(
            "allow_tot_mismatch".into(),
            "If ``True`` allow *$TOT* to not match number of events as \
             computed by the event width and length of *DATA*. \
             Does not apply to delimited ASCII layouts."
                .into(),
        )
    }

    fn new_warnings_are_errors_param() -> Self {
        Self::new_bool_param(
            "warnings_are_errors".into(),
            "If ``True`` all warnings will be regarded as errors.".into(),
        )
    }
}

impl<T> DocArg<T> {
    fn default_matches(&self) -> Result<(), String> {
        if let Some(d) = self.default.as_ref() {
            if d.matches_pytype(&self.pytype) {
                Ok(())
            } else {
                Err(format!(
                    "Arg type '{}' does not match default type '{}'",
                    self.pytype,
                    d.as_type()
                ))
            }
        } else {
            Ok(())
        }
    }
}

impl DocDefault {
    fn as_rs_value(&self) -> TokenStream2 {
        match self {
            Self::Bool(x) => quote! {#x},
            Self::EmptyDict => quote! {std::collections::HashMap::new()},
            // Self::EmptySet => quote! {std::collections::HashSet::new()},
            Self::EmptyList => quote! {vec![]},
            Self::Option => quote! {None},
            Self::Other(rs, _) => rs.clone(),
        }
    }

    fn as_py_value(&self) -> String {
        match self {
            Self::Bool(x) => if *x { "True" } else { "False" }.into(),
            Self::EmptyDict => "{}".to_string(),
            // this isn't implemented yet: https://peps.python.org/pep-0802/#abstract
            // Self::EmptySet => "{/}".to_string(),
            Self::EmptyList => "[]".to_string(),
            Self::Option => "None".to_string(),
            Self::Other(_, py) => py.clone(),
        }
    }

    // for error reporting
    fn as_type(&self) -> &'static str {
        match self {
            Self::Bool(_) => "bool",
            Self::EmptyDict => "dict",
            // Self::EmptySet => "set",
            Self::EmptyList => "list",
            Self::Option => "option",
            Self::Other(_, _) => "raw",
        }
    }

    fn matches_pytype(&self, other: &PyType) -> bool {
        matches!(
            (self, other),
            (Self::Bool(_), PyType::Bool)
                | (Self::EmptyDict, PyType::Dict(_, _))
                // | (Self::EmptySet, PyType::Set(_))
                | (Self::EmptyList, PyType::List(_))
                | (Self::Option, PyType::Option(_))
                | (Self::Other(_, _), _)
        )
    }
}

trait IsArgType {
    const TYPENAME: &str;
    const ARGTYPE: &str;

    fn readonly() -> bool;
}

impl IsArgType for GetMethod {
    const TYPENAME: &str = "vartype";
    const ARGTYPE: &str = "ivar";

    fn readonly() -> bool {
        true
    }
}

impl IsArgType for GetSetMethods {
    const TYPENAME: &str = "vartype";
    const ARGTYPE: &str = "ivar";

    fn readonly() -> bool {
        false
    }
}

impl IsArgType for NoMethods {
    const TYPENAME: &str = "type";
    const ARGTYPE: &str = "param";

    fn readonly() -> bool {
        false
    }
}

trait IsDocArg {
    fn argname(&self) -> &str;

    // fn pytype(&self) -> &PyType;

    // fn desc(&self) -> &str;

    fn default(&self) -> Option<&DocDefault>;

    fn default_matches(&self) -> Result<(), String>;

    fn fun_arg(&self) -> TokenStream2;

    fn ident(&self) -> Ident;

    fn ident_into(&self) -> TokenStream2;

    fn record_into(&self) -> TokenStream2;
}

impl<T> IsDocArg for DocArg<T> {
    fn argname(&self) -> &str {
        self.argname.as_str()
    }

    // fn pytype(&self) -> &PyType {
    //     &self.pytype
    // }

    // fn desc(&self) -> &str {
    //     self.desc.as_str()
    // }

    fn default(&self) -> Option<&DocDefault> {
        self.default.as_ref()
    }

    fn default_matches(&self) -> Result<(), String> {
        if let Some(d) = self.default.as_ref() {
            if d.matches_pytype(&self.pytype) {
                Ok(())
            } else {
                Err(format!(
                    "Arg type '{}' does not match default type '{}'",
                    self.pytype,
                    d.as_type()
                ))
            }
        } else {
            Ok(())
        }
    }

    fn fun_arg(&self) -> TokenStream2 {
        let n = format_ident!("{}", &self.argname);
        let t = &self.rstype;
        quote!(#n: #t)
    }

    fn ident(&self) -> Ident {
        format_ident!("{}", &self.argname)
    }

    fn ident_into(&self) -> TokenStream2 {
        let n = self.ident();
        if unwrap_generic("Option", &self.rstype).1 {
            quote! {#n.map(|x| x.into())}
        } else {
            quote! {#n.into()}
        }
    }

    fn record_into(&self) -> TokenStream2 {
        let n = self.ident();
        if unwrap_generic("Option", &self.rstype).1 {
            quote! {#n: #n.map(|x| x.into())}
        } else {
            quote! {#n: #n.into()}
        }
    }
}

impl IsDocArg for AnyDocArg {
    fn argname(&self) -> &str {
        match self {
            Self::RWIvar(x) => x.argname(),
            Self::ROIvar(x) => x.argname(),
            Self::Param(x) => x.argname(),
        }
    }

    // fn pytype(&self) -> &PyType {
    //     match self {
    //         Self::RWIvar(x) => x.pytype(),
    //         Self::ROIvar(x) => x.pytype(),
    //         Self::Param(x) => x.pytype(),
    //     }
    // }

    // fn desc(&self) -> &str {
    //     match self {
    //         Self::RWIvar(x) => x.desc(),
    //         Self::ROIvar(x) => x.desc(),
    //         Self::Param(x) => x.desc(),
    //     }
    // }

    fn default(&self) -> Option<&DocDefault> {
        match self {
            Self::RWIvar(x) => x.default(),
            Self::ROIvar(x) => x.default(),
            Self::Param(x) => x.default(),
        }
    }

    fn default_matches(&self) -> Result<(), String> {
        match self {
            Self::RWIvar(x) => x.default_matches(),
            Self::ROIvar(x) => x.default_matches(),
            Self::Param(x) => x.default_matches(),
        }
    }

    fn fun_arg(&self) -> TokenStream2 {
        match self {
            Self::RWIvar(x) => x.fun_arg(),
            Self::ROIvar(x) => x.fun_arg(),
            Self::Param(x) => x.fun_arg(),
        }
    }

    fn ident(&self) -> Ident {
        match self {
            Self::RWIvar(x) => x.ident(),
            Self::ROIvar(x) => x.ident(),
            Self::Param(x) => x.ident(),
        }
    }

    fn ident_into(&self) -> TokenStream2 {
        match self {
            Self::RWIvar(x) => x.ident_into(),
            Self::ROIvar(x) => x.ident_into(),
            Self::Param(x) => x.ident_into(),
        }
    }

    fn record_into(&self) -> TokenStream2 {
        match self {
            Self::RWIvar(x) => x.record_into(),
            Self::ROIvar(x) => x.record_into(),
            Self::Param(x) => x.record_into(),
        }
    }
}

impl PyType {
    fn new_opt(x: PyType) -> Self {
        Self::Option(Box::new(x))
    }

    fn new_list(x: PyType) -> Self {
        Self::List(Box::new(x))
    }

    //  fn new_set(x: PyType) -> Self {
    //     Self::Set(Box::new(x))
    // }

    fn new_dict(k: PyType, v: PyType) -> Self {
        Self::Dict(Box::new(k), Box::new(v))
    }

    fn new_union2(x: PyType, y: PyType) -> Self {
        Self::new_union(vec![x, y])
    }

    fn new_union(xs: Vec<PyType>) -> Self {
        let mut it = xs.into_iter();
        let x0 = it.next().expect("Union cannot be empty");
        let x1 = it.next().expect("Union must have at least 2 types");
        let xs = it.collect();
        Self::Union(Box::new(x0), Box::new(x1), xs)
    }

    fn new_lit(xs: &[&'static str]) -> Self {
        let mut it = xs.iter();
        let x0 = it.next().expect("Literal cannot be empty");
        let xs = it.copied().collect();
        Self::Literal(x0, xs)
    }

    fn new_unit() -> Self {
        Self::Tuple(vec![])
    }

    fn new_optical(version: Version) -> Self {
        Self::PyClass(format!("Optical{}", version.short_underscore()))
    }

    fn new_temporal(version: Version) -> Self {
        Self::PyClass(format!("Temporal{}", version.short_underscore()))
    }

    fn new_measurement(version: Version) -> Self {
        Self::new_union2(Self::new_optical(version), Self::new_temporal(version))
    }

    fn new_version() -> Self {
        Self::new_lit(&["FCS2.0", "FCS3.0", "FCS3.1", "FCS3.2"])
    }

    fn new_temporal_optical_key() -> Self {
        Self::new_lit(&[
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

    fn new_datatype() -> Self {
        Self::new_lit(&["A", "I", "F", "D"])
    }

    fn new_display() -> Self {
        Self::Tuple(vec![Self::Bool, Self::Float, Self::Float])
    }

    fn new_feature() -> Self {
        Self::new_lit(&["Area", "Width", "Height"])
    }

    fn new_calibration3_1() -> Self {
        Self::Tuple(vec![Self::Float, Self::Str])
    }

    fn new_calibration3_2() -> Self {
        Self::Tuple(vec![Self::Float, Self::Float, Self::Str])
    }

    fn new_segment() -> Self {
        Self::Tuple(vec![Self::Int, Self::Int])
    }

    fn new_correction() -> Self {
        Self::Tuple(vec![Self::Int, Self::Int])
    }

    fn new_scale() -> Self {
        Self::new_union2(
            Self::new_unit(),
            Self::Tuple(vec![Self::Float, Self::Float]),
        )
    }
}

impl ClassDocString {
    fn new_class(summary: String, paragraphs: Vec<String>, args: Vec<AnyDocArg>) -> Self {
        Self::new(summary, paragraphs, args, ())
    }
}

impl MethodDocString {
    fn new_method(
        summary: String,
        paragraphs: Vec<String>,
        args: Vec<DocArgParam>,
        returns: Option<DocReturn>,
    ) -> Self {
        Self::new(summary, paragraphs, args, returns)
    }
}

impl FunDocString {
    fn new_fun(
        summary: String,
        paragraphs: Vec<String>,
        args: Vec<DocArgParam>,
        returns: Option<DocReturn>,
    ) -> Self {
        Self::new(summary, paragraphs, args, returns)
    }
}

impl<A, R, S> DocString<A, R, S> {
    /// Emit typed argument list for use in rust function signature
    fn fun_args(&self) -> TokenStream2
    where
        A: IsDocArg,
    {
        let xs: Vec<_> = self.args.iter().map(|a| a.fun_arg()).collect();
        quote!(#(#xs),*)
    }

    /// Emit identifiers associated with function arguments
    fn idents(&self) -> TokenStream2
    where
        A: IsDocArg,
    {
        let xs: Vec<_> = self.args.iter().map(|a| a.ident()).collect();
        quote!(#(#xs),*)
    }

    fn idents_into(&self) -> TokenStream2
    where
        A: IsDocArg,
    {
        let xs: Vec<_> = self.args.iter().map(|a| a.ident_into()).collect();
        quote!(#(#xs),*)
    }

    /// Emit get/set methods associated with arguments (if any)
    fn quoted_methods(&self) -> TokenStream2
    where
        A: IsMethods,
    {
        let xs: Vec<_> = self.args.iter().map(|a| a.quoted_methods()).collect();
        quote!(#(#xs)*)
    }

    fn has_defaults(&self) -> Option<bool>
    where
        A: IsDocArg,
    {
        self.args
            .iter()
            .skip_while(|p| p.default().is_none())
            .try_fold(false, |has_def, next| {
                match (has_def, next.default().is_some()) {
                    // if we encounter a non-default after at least one
                    // default, return None (error) since this means we
                    // have default args after non-default args.
                    (true, false) => None,
                    (x, y) => Some(x || y),
                }
            })
    }

    fn doc(&self) -> TokenStream2
    where
        Self: fmt::Display,
    {
        let s = self.to_string();
        quote! {#[doc = #s]}
    }

    fn sig(&self) -> TokenStream2
    where
        A: IsDocArg,
        S: IsSelfArg,
    {
        if let Err(e) = self
            .args
            .iter()
            .map(|a| a.default_matches())
            .collect::<Result<Vec<_>, _>>()
        {
            panic!("{e}")
        }
        if self.has_defaults().is_none() {
            panic!("non-default args after default args");
        }

        let ps = &self.args;
        let (raw_sig, _txt_sig): (Vec<_>, Vec<_>) = ps
            .iter()
            .map(|a| {
                let n = &a.argname();
                let i = format_ident!("{n}");
                if let Some(d) = a.default() {
                    let r = d.as_rs_value();
                    let t = d.as_py_value();
                    (quote! {#i=#r}, format!("{n}={t}"))
                } else {
                    (quote! {#i}, n.to_string())
                }
            })
            .unzip();
        let txt_sig = format!(
            "({})",
            S::ARG
                .into_iter()
                .chain(_txt_sig.iter().map(|s| s.as_str()))
                .join(", ")
        );
        quote! {
            #[pyo3(signature = (#(#raw_sig),*))]
            #[pyo3(text_signature = #txt_sig)]
        }
    }

    fn fmt_inner<F>(&self, f_return: F, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error>
    where
        A: fmt::Display,
        F: FnOnce(&R) -> Option<String>,
    {
        let ps = self
            .paragraphs
            .iter()
            .map(|s| fmt_docstring_nonparam(s.as_str()));
        let xs = self.args.iter().map(|s| s.to_string());
        let r = f_return(&self.returns).into_iter();
        let rest = ps.chain(xs).chain(r).join("\n\n");
        if self.summary.len() > LINE_LEN {
            panic!("summary is too long");
        }
        write!(f, "{}\n\n{rest}", self.summary)
    }
}

impl<A, R, S> ToTokens for DocString<A, R, S>
where
    Self: fmt::Display,
    A: IsDocArg,
    S: IsSelfArg,
{
    fn to_tokens(&self, tokens: &mut TokenStream2) {
        let doc = self.doc();
        let sig = self.sig();
        quote! {
            #doc
            #sig
        }
        .to_tokens(tokens);
    }
}

impl fmt::Display for ClassDocString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        self.fmt_inner(|()| None, f)
    }
}

impl<A: fmt::Display, S> fmt::Display for DocString<A, Option<DocReturn>, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        self.fmt_inner(|r| r.as_ref().map(|s| s.to_string()), f)
    }
}

impl<T: IsArgType> fmt::Display for DocArg<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let ro = if T::readonly() { "(read-only) " } else { "" };
        let pt = &self.pytype;
        let n = &self.argname;
        let d = self
            .default
            .as_ref()
            .map(|d| d.as_py_value())
            .map_or(self.desc.to_string(), |def| {
                format!("{} Defaults to ``{def}``.", self.desc)
            });
        let tn = T::TYPENAME;
        let at = T::ARGTYPE;
        let s0 = fmt_docstring_param1(format!(":{at} {n}: {ro}{d}"));
        let s1 = fmt_docstring_param1(format!(":{tn} {n}: {pt}"));
        write!(f, "{s0}\n{s1}")
    }
}

impl fmt::Display for DocReturn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let t = fmt_docstring_param1(format!(":rtype: {}", self.rtype));
        if let Some(d) = self
            .desc
            .as_ref()
            .map(|d| fmt_docstring_param1(format!(":returns: {d}")))
        {
            write!(f, "{d}\n{t}")
        } else {
            f.write_str(t.as_str())
        }
    }
}

impl fmt::Display for PyType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        // see https://github.com/sphinx-doc/sphinx/blob/9a08711e0e18c63c609070aa0a79019b4db45a78/tests/test_util/test_util_typing.py
        // for cheat sheet on how these types should be represented in .rst syntax
        match self {
            Self::Bool => f.write_str(":py:class:`bool`"),
            Self::Str => f.write_str(":py:class:`str`"),
            Self::Int => f.write_str(":py:class:`int`"),
            Self::Float => f.write_str(":py:class:`float`"),
            Self::Bytes => f.write_str(":py:class:`bytes`"),
            Self::None => f.write_str("None"),
            Self::Date => f.write_str(":py:class:`~datetime.date`"),
            Self::Time => f.write_str(":py:class:`~datetime.time`"),
            Self::Datetime => f.write_str(":py:class:`~datetime.datetime`"),
            Self::Decimal => f.write_str(":py:class:`~decimal.Decimal`"),
            Self::Union(x, y, zs) => {
                let s = [x.as_ref(), y.as_ref()]
                    .into_iter()
                    .chain(zs.iter())
                    .join(" | ");
                f.write_str(s.as_str())
            }
            Self::Tuple(xs) => {
                let s = if xs.is_empty() {
                    "()".into()
                } else {
                    xs.iter().join(", ")
                };
                write!(f, ":py:class:`tuple`\\ [{s}]")
            }
            Self::Literal(x, xs) => {
                write!(
                    f,
                    ":obj:`~typing.Literal`\\ [{}]",
                    [x].into_iter()
                        .chain(xs)
                        .map(|s| format!("\"{s}\""))
                        .join(", ")
                )
            }
            Self::List(x) => write!(f, ":py:class:`list`\\ [{x}]"),
            // Self::Set(x) => write!(f, ":py:class:`set`\\ [{x}]"),
            Self::Dict(x, y) => write!(f, ":py:class:`dict`\\ [{x}, {y}]"),
            Self::Option(x) => write!(f, "{x} | None"),
            Self::PyClass(s) => write!(f, ":py:class:`{s}`"),
        }
    }
}

fn fmt_docstring_nonparam(s: &str) -> String {
    fmt_hanging_indent(LINE_LEN, 0, s)
}

fn fmt_docstring_param1(s: String) -> String {
    fmt_docstring_param(s.as_str())
}

fn fmt_docstring_param(s: &str) -> String {
    fmt_hanging_indent(LINE_LEN, 4, s)
}

fn fmt_hanging_indent(width: usize, indent: usize, s: &str) -> String {
    let i = " ".repeat(indent);
    let xs = s.split_whitespace().filter(|x| !x.is_empty());
    let mut line_len = 0;
    let mut tmp = vec![]; // buffer for current line
    let mut zs = vec![]; // buffer for indented lines
    for x in xs {
        // add length of word (without next space)
        line_len += x.len();
        // If length exceeds target width, reset length, join line buffer with
        // spaces, collect line in final line buffer, then make new line buffer
        // and initialize with a hanging indent. This will only happen if we hit
        // the target length at least once so the first line will never have a
        // hanging indent.
        //
        // Otherwise, add 1 to length to account for space after word.
        //
        // In all cases, add the next word to the line buffer, which may only
        // have a leading indent if it was reset immediately before.
        if line_len > width {
            zs.push(tmp.iter().join(" "));
            if indent > 0 {
                line_len = indent + x.len();
                tmp = vec![i.as_str()]
            } else {
                line_len = x.len();
                tmp = vec![]
            }
        } else {
            line_len += 1;
        }
        tmp.push(x);
    }
    zs.push(tmp.iter().join(" "));
    zs.iter().join("\n")
}

const LINE_LEN: usize = 72;

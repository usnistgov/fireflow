extern crate proc_macro;

use fireflow_python_types::{
    ALL_VERSIONS, AnyDocArg, ArgPyType, DocArg, DocArgParam, DocArgROIvar, DocReturn, DocString,
    HasRustPath as _, IsDocArg, PyBool, PyBytes, PyClass, PyDatetime, PyDecimal, PyDict,
    PyException, PyFloat, PyInt, PyList, PyLiteral, PyOpt, PyStr, PyTuple, PyType, PyUnion,
    PyreflowError, RsInt, SegmentSrc, Version, config_path, keyword_path, path_strip_args,
};

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use std::iter::once;
use std::string::ToString as _;
use syn::{
    GenericArgument, Ident, LitBool, LitInt, Path, PathArguments, Type,
    parse::{Parse, ParseStream},
    parse_macro_input, parse_quote,
    token::Comma,
};

#[proc_macro]
pub fn def_fcs_read_header(input: TokenStream) -> TokenStream {
    let fun_path = parse_macro_input!(input as Path);

    let conf_path = config_path("ReadHeaderConfig");

    let (conf_inner_path, args, inner_args) = DocArgParam::new_read_header_config_params();

    let exc = PyException::new_pyreflow(&PyreflowError::FileLayout)
        .desc("if *HEADER* segment is unparsable");

    let doc = DocString::new_fun("Read the *HEADER* of an FCS file.")
        .arg(DocArg::new_path_param(true))
        .args(args)
        .arg(DocArg::new_dataset_offset_param())
        .returns(DocReturn::new(PyClass::new_py(["api"], "Header")).exc([exc]));

    let fun_args = doc.fun_args();
    let ret_path = doc.ret_path();

    quote! {
        #[pyfunction]
        #doc
        #[allow(clippy::too_many_arguments)]
        pub fn fcs_read_header(#fun_args) -> #ret_path {
            let conf = #conf_path(#conf_inner_path { #(#inner_args),* });
            Ok(#fun_path(&path, dataset_offset, &conf)?.into())
        }
    }
    .into()
}

#[proc_macro]
pub fn def_fcs_read_flat_text(input: TokenStream) -> TokenStream {
    let parsed = parse_macro_input!(input as ReadPaths2);
    let fun_one_path = &parsed.path0;
    let fun_many_path = &parsed.path1;

    let conf_path = config_path("ReadFlatTEXTConfig");

    let path_arg = DocArg::new_path_param(true);
    let (header_conf, header_args, header_recs) = DocArgParam::new_read_header_config_params();
    let (flat_conf, flat_args, flat_recs) = DocArgParam::new_read_flat_config_params();
    let (shared_conf, shared_args, shared_recs) = DocArgParam::new_shared_config_params();
    let dataset_offset_arg = DocArg::new_dataset_offset_param();

    let skip_arg = DocArg::new_skip_param("Number of datasets to skip");
    let limit_arg = DocArg::new_limit_param("Parse up to this many datasets");

    let conf_args: Vec<_> = header_args
        .into_iter()
        .chain(flat_args)
        .chain(shared_args)
        .collect();

    let exc0 = PyException::new_pyreflow(&PyreflowError::FileLayout)
        .desc("If *HEADER* or *TEXT* are not parsable");
    let exc1 = PyException::new_non_ascii();
    let xs = [exc0, exc1];

    let ret_pt = PyClass::new_py(["api"], "FlatTEXTOutput");

    let one_doc = DocString::new_fun("Read *HEADER* and *TEXT* from first dataset in FCS file.")
        .arg(path_arg.clone())
        .args(conf_args.clone())
        .arg(dataset_offset_arg)
        .returns(DocReturn::new(ret_pt.clone()).exc(xs.clone()));

    let many_doc =
        DocString::new_fun("Read *HEADER* and *TEXT* from multiple datasets in FCS file.")
            .arg(path_arg)
            .arg(skip_arg)
            .arg(limit_arg)
            .args(conf_args)
            .returns(DocReturn::new(PyList::new1(ret_pt)).exc(xs));

    let one_fun_args = one_doc.fun_args();
    let one_ret_path = one_doc.ret_path();

    let many_fun_args = many_doc.fun_args();
    let many_ret_path = many_doc.ret_path();

    let conf_q = quote! {
        let header = #header_conf { #(#header_recs),* };
        let flat = #flat_conf { header, #(#flat_recs),* };
        let shared = #shared_conf { #(#shared_recs),* };
        let conf = #conf_path { flat, shared };
    };

    quote! {
        #[pyfunction]
        #one_doc
        #[allow(clippy::too_many_arguments)]
        pub fn fcs_read_flat_text(#one_fun_args) -> #one_ret_path {
            #conf_q
            Ok(#fun_one_path(&path, dataset_offset, &conf).py_resolve_commutative()?.into())
        }

        #[pyfunction]
        #many_doc
        #[allow(clippy::too_many_arguments)]
        pub fn fcs_read_flat_texts(#many_fun_args) -> #many_ret_path {
            #conf_q
            let xs = #fun_many_path(&path, skip, limit, &conf).py_resolve_commutative()?;
            Ok(xs.fmap(Into::into))
        }
    }
    .into()
}

#[proc_macro]
pub fn def_fcs_read_std_text(input: TokenStream) -> TokenStream {
    let parsed = parse_macro_input!(input as ReadPaths2);
    let fun_one_path = &parsed.path0;
    let fun_many_path = &parsed.path1;

    let conf_path = config_path("ReadStdTEXTConfig");

    let path_arg = DocArg::new_path_param(true);
    let (header_conf, header_args, header_recs) = DocArgParam::new_read_header_config_params();
    let (flat_conf, flat_args, flat_recs) = DocArgParam::new_read_flat_config_params();
    let (std_conf, std_args, std_recs) = DocArgParam::new_read_std_config_params(None);
    let (layout_conf, layout_args, layout_recs) = DocArgParam::new_read_layout_config_params(None);
    let (shared_conf, shared_args, shared_recs) = DocArgParam::new_shared_config_params();
    let dataset_offset_arg = DocArg::new_dataset_offset_param();

    let conf_args = header_args
        .into_iter()
        .chain(flat_args)
        .chain(std_args)
        .chain(layout_args)
        .chain(shared_args);

    let skip_arg = DocArg::new_skip_param(
        "Number of datasets to skip. The *HEADER* and *TEXT* from skipped \
         datasets will still be read to obtain *$NEXTDATA* for the next \
         dataset in the file.",
    );
    let limit_arg = DocArg::new_limit_param("Parse up to this many datasets");

    let exc0 = PyException::new_pyreflow(&PyreflowError::FileLayout)
        .desc("If *HEADER* or *TEXT* are unparsable");
    let exc1 = PyException::new_non_ascii();
    let exc2 = PyException::new_extra();
    let exc3 = PyException::new_deprecated();
    let exc4 = PyException::new_parse_keyval();
    let exc5 = PyException::new_pyreflow(&PyreflowError::Relational)
        .desc("If keywords that are referenced by other keywords are missing");

    let xs = [exc0, exc1, exc2, exc3, exc4, exc5];

    let pt_ret = PyTuple::new1(PyUnion::new_anycoretext())
        .add_new(PyClass::new_py(["api"], "StdTEXTOutput"));

    let one_doc = DocString::new_fun("Read standardized *TEXT* from first dataset in FCS file.")
        .arg(path_arg.clone())
        .args(conf_args.clone())
        .arg(dataset_offset_arg)
        .returns(DocReturn::new(pt_ret.clone()).exc(xs.clone()));

    let many_doc =
        DocString::new_fun("Read standardized *TEXT* from multiple datasets in FCS file.")
            .arg(path_arg)
            .arg(skip_arg)
            .arg(limit_arg)
            .args(conf_args)
            .returns(DocReturn::new(PyList::new1(pt_ret)).exc(xs));

    let one_fun_args = one_doc.fun_args();
    let one_ret_path = one_doc.ret_path();
    let many_fun_args = many_doc.fun_args();
    let many_ret_path = many_doc.ret_path();

    let conf_q = quote! {
        let header = #header_conf { #(#header_recs),* };
        let flat = #flat_conf { header, #(#flat_recs),* };
        let standard = #std_conf { #(#std_recs),* };
        let layout = #layout_conf { #(#layout_recs),* };
        let shared = #shared_conf { #(#shared_recs),* };
        let conf = #conf_path { flat, standard, layout, shared };
    };

    quote! {
        #[pyfunction]
        #one_doc
        #[allow(clippy::too_many_arguments)]
        pub fn fcs_read_std_text(#one_fun_args) -> #one_ret_path {
            #conf_q
            let (core, data) = #fun_one_path(&path, dataset_offset, &conf).py_resolve_commutative()?;
            Ok((core.into(), data.into()))
        }

        #[pyfunction]
        #many_doc
        #[allow(clippy::too_many_arguments)]
        pub fn fcs_read_std_texts(#many_fun_args) -> #many_ret_path {
            #conf_q
            let xs = #fun_many_path(&path, skip, limit, &conf).py_resolve_commutative()?;
            Ok(xs.fmap(|(c, d)| (c.into(), d.into())))
        }
    }
    .into()
}

#[proc_macro]
pub fn def_fcs_read_flat_dataset(input: TokenStream) -> TokenStream {
    let parsed = parse_macro_input!(input as ReadPaths3);
    let fun_one_path = &parsed.path0;
    let fun_many_path = &parsed.path1;
    let fun_smry_path = &parsed.path2;

    let conf_path = config_path("ReadFlatDatasetConfig");

    let path_arg = DocArg::new_path_param(true);
    let (header_conf, header_args, header_recs) = DocArgParam::new_read_header_config_params();
    let (flat_conf, flat_args, flat_recs) = DocArgParam::new_read_flat_config_params();
    let (layout_conf, layout_args, layout_recs) = DocArgParam::new_read_layout_config_params(None);
    let (data_conf, data_args, data_recs) = DocArgParam::new_read_events_config_params();
    let (shared_conf, shared_args, shared_recs) = DocArgParam::new_shared_config_params();
    let dataset_offset_arg = DocArg::new_dataset_offset_param();

    let skip_arg = DocArg::new_skip_param(
        "Number of datasets to skip. The *HEADER* and *TEXT* from skipped \
         datasets will still be read to obtain *$NEXTDATA* for the next \
         dataset in the file.",
    );
    let limit_arg = DocArg::new_limit_param("Parse up to this many datasets");

    let conf_args = header_args
        .into_iter()
        .chain(flat_args)
        .chain(layout_args)
        .chain(data_args)
        .chain(shared_args);

    let exc0 = PyException::new_pyreflow(&PyreflowError::FileLayout)
        .desc("If *HEADER*, *TEXT*, or *DATA* are unparsable");
    let exc1 = PyException::new_non_ascii();
    // the only deprecated keyval that should be read here is $DATATYPE when its
    // value is A for 3.1+
    let exc2 = PyException::new_deprecated()
        .desc("If an ASCII layout is used and FCS version is 3.1 or 3.2");
    let exc3 = PyException::new_parse_keyval();
    let exc4 = PyException::new_pyreflow(&PyreflowError::Relational)
        .desc("If keywords are incompatible with indicated layout of *DATA*");
    let exc5 = PyException::new_event_data();

    let xs = [exc0, exc1, exc2, exc3, exc4, exc5];

    let pt_data_ret = PyClass::new_py(["api"], "FlatDatasetOutput");
    let pt_smry_ret = PyClass::new_py(["api"], "DatasetSummary");

    let one_doc = DocString::new_fun("Read one dataset from FCS file in flat mode.")
        .arg(path_arg.clone())
        .args(conf_args.clone())
        .arg(dataset_offset_arg)
        .returns(DocReturn::new(pt_data_ret.clone()).exc(xs.clone()));

    let many_doc = DocString::new_fun("Read multiple datasets from FCS file in flat mode.")
        .arg(path_arg.clone())
        .arg(skip_arg.clone())
        .arg(limit_arg.clone())
        .args(conf_args.clone())
        .returns(DocReturn::new(PyList::new1(pt_data_ret)).exc(xs.clone()));

    let smry_doc = DocString::new_fun("Summarize datasets in FCS file.")
        .arg(path_arg)
        .arg(skip_arg)
        .arg(limit_arg)
        .args(conf_args)
        .returns(DocReturn::new(PyList::new1(pt_smry_ret)).exc(xs));

    let one_fun_args = one_doc.fun_args();
    let one_ret_path = one_doc.ret_path();
    let many_fun_args = many_doc.fun_args();
    let many_ret_path = many_doc.ret_path();
    let smry_fun_args = smry_doc.fun_args();
    let smry_ret_path = smry_doc.ret_path();

    let conf_q = quote! {
        let header = #header_conf { #(#header_recs),* };
        let flat = #flat_conf { header, #(#flat_recs),* };
        let layout = #layout_conf { #(#layout_recs),* };
        let data = #data_conf { #(#data_recs),* };
        let shared = #shared_conf { #(#shared_recs),* };
        let conf = #conf_path { flat, layout, data, shared };
    };

    quote! {
        #[pyfunction]
        #one_doc
        #[allow(clippy::too_many_arguments)]
        pub fn fcs_read_flat_dataset(#one_fun_args) -> #one_ret_path {
            #conf_q
            Ok(#fun_one_path(&path, dataset_offset, &conf).py_resolve_commutative()?.into())
        }

        #[pyfunction]
        #many_doc
        #[allow(clippy::too_many_arguments)]
        pub fn fcs_read_flat_datasets(#many_fun_args) -> #many_ret_path {
            #conf_q
            let xs = #fun_many_path(&path, skip, limit, &conf).py_resolve_commutative()?;
            Ok(xs.fmap(Into::into))
        }

        #[pyfunction]
        #smry_doc
        #[allow(clippy::too_many_arguments)]
        pub fn fcs_summarize(#smry_fun_args) -> #smry_ret_path {
            #conf_q
            let xs = #fun_smry_path(&path, skip, limit, &conf).py_resolve_commutative()?;
            Ok(xs.fmap(Into::into))
        }
    }
    .into()
}

#[proc_macro]
pub fn def_fcs_read_std_dataset(input: TokenStream) -> TokenStream {
    let parsed = parse_macro_input!(input as ReadPaths2);
    let fun_one_path = &parsed.path0;
    let fun_many_path = &parsed.path1;

    let conf_path = config_path("ReadStdDatasetConfig");

    let path_arg = DocArg::new_path_param(true);
    let (header_conf, header_args, header_recs) = DocArgParam::new_read_header_config_params();
    let (flat_conf, flat_args, flat_recs) = DocArgParam::new_read_flat_config_params();
    let (std_conf, std_args, std_recs) = DocArgParam::new_read_std_config_params(None);
    let (layout_conf, layout_args, layout_recs) = DocArgParam::new_read_layout_config_params(None);
    let (data_conf, data_args, data_recs) = DocArgParam::new_read_events_config_params();
    let (shared_conf, shared_args, shared_recs) = DocArgParam::new_shared_config_params();
    let dataset_offset_arg = DocArg::new_dataset_offset_param();

    let conf_args = header_args
        .into_iter()
        .chain(flat_args)
        .chain(std_args)
        .chain(layout_args)
        .chain(data_args)
        .chain(shared_args);

    let skip_arg = DocArg::new_skip_param(
        "Number of datasets to skip. The *HEADER* and *TEXT* from skipped \
         datasets will still be read to obtain *$NEXTDATA* for the next \
         dataset in the file.",
    );
    let limit_arg = DocArg::new_limit_param("Parse up to this many datasets");

    let exc0 = PyException::new_pyreflow(&PyreflowError::FileLayout)
        .desc("If *HEADER*, *TEXT*, or *DATA* are unparsable");
    let exc1 = PyException::new_non_ascii();
    let exc2 = PyException::new_deprecated();
    let exc3 = PyException::new_parse_keyval();
    let exc4 = PyException::new_pyreflow(&PyreflowError::Relational).desc(
        "If keywords are incompatible with indicated layout of *DATA* or \
         if keywords that are referenced by other keywords do not exist",
    );
    let exc5 = PyException::new_event_data();
    let exc6 = PyException::new_extra();

    let xs = [exc0, exc1, exc2, exc3, exc4, exc5, exc6];

    let pt_ret = PyTuple::new1(PyUnion::new_anycoredataset())
        .add_new(PyClass::new_py(["api"], "StdDatasetOutput"));

    let one_doc = DocString::new_fun("Read one standardized dataset from FCS file.")
        .arg(path_arg.clone())
        .args(conf_args.clone())
        .arg(dataset_offset_arg)
        .returns(DocReturn::new(pt_ret.clone()).exc(xs.clone()));

    let many_doc = DocString::new_fun("Read multiple standardized datasets from FCS file.")
        .arg(path_arg)
        .arg(skip_arg)
        .arg(limit_arg)
        .args(conf_args)
        .returns(DocReturn::new(PyList::new1(pt_ret)).exc(xs));

    let one_fun_args = one_doc.fun_args();
    let one_ret_path = one_doc.ret_path();
    let many_fun_args = many_doc.fun_args();
    let many_ret_path = many_doc.ret_path();

    let conf_q = quote! {
        let header = #header_conf { #(#header_recs),* };
        let flat = #flat_conf { header, #(#flat_recs),* };
        let standard = #std_conf { #(#std_recs),* };
        let layout = #layout_conf { #(#layout_recs),* };
        let data = #data_conf { #(#data_recs),* };
        let shared = #shared_conf { #(#shared_recs),* };
        let conf = #conf_path { flat, standard, layout, data, shared };
    };

    quote! {
        #[pyfunction]
        #one_doc
        #[allow(clippy::too_many_arguments)]
        pub fn fcs_read_std_dataset(#one_fun_args) -> #one_ret_path {
            #conf_q
            let (core, data) = #fun_one_path(&path, dataset_offset, &conf).py_resolve_commutative()?;
            Ok((core.into(), data.into()))
        }

        #[pyfunction]
        #many_doc
        #[allow(clippy::too_many_arguments)]
        pub fn fcs_read_std_datasets(#many_fun_args) -> #many_ret_path {
            #conf_q
            let xs = #fun_many_path(&path, skip, limit, &conf).py_resolve_commutative()?;
            Ok(xs.fmap(|(c, d)| (c.into(), d.into())))
        }
    }
    .into()
}

#[proc_macro]
pub fn def_fcs_read_flat_dataset_with_keywords(input: TokenStream) -> TokenStream {
    let fun_path = parse_macro_input!(input as Path);

    let conf_path = config_path("ReadFlatDatasetFromKeywordsConfig");

    let path_arg = DocArg::new_path_param(true);
    let version_arg = DocArg::new_version_param();
    let std_arg = DocArg::new_std_keywords_param();
    let data_arg = DocArg::new_rel_data_seg_param();
    let analysis_arg = DocArg::new_rel_analysis_seg_param();
    let other_arg = DocArg::new_rel_other_segs_param();
    let dataset_offset_arg = DocArg::new_dataset_offset_param();

    let (layout_conf, layout_args, layout_recs) = DocArgParam::new_read_layout_config_params(None);
    let (data_conf, data_args, data_recs) = DocArgParam::new_read_events_config_params();
    let (shared_conf, shared_args, shared_recs) = DocArgParam::new_shared_config_params();

    let exc0 =
        PyException::new_pyreflow(&PyreflowError::FileLayout).desc("If *DATA* is unparsable");
    // the only deprecated keyval that should be read here is $DATATYPE when its
    // value is A for 3.1+
    let exc1 = PyException::new_deprecated()
        .desc("If an ASCII layout is used and FCS version is 3.1 or 3.2");
    let exc2 = PyException::new_parse_keyval();
    let exc3 = PyException::new_pyreflow(&PyreflowError::Relational)
        .desc("If keywords are incompatible with indicated layout of *DATA*");
    let exc4 = PyException::new_event_data();

    let xs = [exc0, exc1, exc2, exc3, exc4];

    let doc = DocString::new_fun("Read dataset from FCS file from keywords in flat mode.")
        .arg(path_arg)
        .arg(version_arg)
        .arg(std_arg)
        .arg(data_arg)
        .arg(analysis_arg)
        .arg(other_arg)
        .args(layout_args)
        .args(data_args)
        .args(shared_args)
        .arg(dataset_offset_arg)
        .returns(DocReturn::new(PyClass::new_py(["api"], "FlatDatasetWithKwsOutput")).exc(xs));

    let fun_args = doc.fun_args();
    let ret_path = doc.ret_path();

    quote! {
        #[pyfunction]
        #doc
        #[allow(clippy::too_many_arguments)]
        pub fn fcs_read_flat_dataset_with_keywords(#fun_args) -> #ret_path {
            let layout = #layout_conf { #(#layout_recs),* };
            let data = #data_conf { #(#data_recs),* };
            let shared = #shared_conf { #(#shared_recs),* };
            let conf = #conf_path { layout, data, shared };
            let ret = #fun_path(
                &path, version, &std, data_seg, analysis_seg, other_segs, dataset_offset, &conf
            ).py_resolve_commutative()?;
            Ok(ret.into())
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_py_header(input: TokenStream) -> TokenStream {
    let path = parse_macro_input!(input as Path);
    let name = path.segments.last().unwrap().ident.clone();

    let version = DocArgROIvar::new_version_ivar();

    let segments = DocArgROIvar::new_ivar_ro(
        "segments",
        PyClass::new_py(["api"], "HeaderSegments"),
        "The segments from *HEADER*.",
        |_, _| quote!(self.0.segments.clone().into()),
    );

    let args = [version, segments];

    let doc = DocString::new_class("The *HEADER* segment from an FCS dataset.").args(args);
    let inner_args = doc.idents_into();

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #path::new(#inner_args).into()
            }
        }
    };
    doc.into_impl_class(name, &path, new).1.into()
}

#[proc_macro]
pub fn impl_py_valid_keywords(input: TokenStream) -> TokenStream {
    let path = parse_macro_input!(input as Path);
    let name = path.segments.last().unwrap().ident.clone();

    let std = DocArg::new_std_keywords_param().into_ro(|_, _| quote!(self.0.std.clone().into()));
    let nonstd =
        DocArg::new_nonstd_keywords_param().into_ro(|_, _| quote!(self.0.nonstd.clone().into()));

    let args = [std, nonstd];

    let doc = DocString::new_class("Standard and non-standard keywords.").args(args);

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #path::new(std, nonstd).into()
            }
        }
    };
    doc.into_impl_class(name, &path, new).1.into()
}

#[proc_macro]
pub fn impl_py_header_segments(input: TokenStream) -> TokenStream {
    let path = parse_macro_input!(input as Path);
    let bare_path = path_strip_args(path.clone());
    let name = path.segments.last().unwrap().ident.clone();

    let text = DocArg::new_text_seg_param().into_ro(|_, _| quote!(self.0.text));
    let data = DocArg::new_data_seg_param(SegmentSrc::Header).into_ro(|_, _| quote!(self.0.data));
    let analysis = DocArg::new_analysis_seg_param(SegmentSrc::Header, false)
        .into_ro(|_, _| quote!(self.0.analysis));

    let other = DocArg::new_other_segs_param(false).into_ro(|_, _| quote!(self.0.other.clone()));

    let args = [text, data, analysis, other];

    let doc = DocString::new_class("The segments from *HEADER*").args(args);
    let inner_args = doc.idents();

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #bare_path::new(#inner_args).into()
            }
        }
    };
    doc.into_impl_class(name, &path, new).1.into()
}

#[proc_macro]
pub fn impl_py_flat_text_output(input: TokenStream) -> TokenStream {
    let path = parse_macro_input!(input as Path);
    let name = path.segments.last().unwrap().ident.clone();

    let version = DocArgROIvar::new_version_ivar();

    let kws =
        DocArg::new_valid_keywords_param().into_ro(|_, _| quote!(self.0.keywords.clone().into()));

    let parse =
        DocArg::new_parse_output_param().into_ro(|_, _| quote!(self.0.parse.clone().into()));

    let args = [version, kws, parse];

    let doc = DocString::new_class("Parsed *HEADER* and *TEXT*.").args(args);

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #path::new(version, kws.into(), parse.into()).into()
            }
        }
    };
    doc.into_impl_class(name, &path, new).1.into()
}

#[proc_macro]
pub fn impl_py_flat_dataset_output(input: TokenStream) -> TokenStream {
    let path = parse_macro_input!(input as Path);
    let name = path.segments.last().unwrap().ident.clone();

    let text = DocArg::new_ivar_ro(
        "text",
        PyClass::new_py(["api"], "FlatTEXTOutput"),
        "Parsed *TEXT* segment.",
        |_, _| quote!(self.0.text.clone().into()),
    );

    let dataset = DocArg::new_ivar_ro(
        "dataset",
        PyClass::new_py(["api"], "FlatDatasetWithKwsOutput"),
        "Parsed *DATA*, *ANALYSIS*, and *OTHER* segments.",
        |_, _| quote!(self.0.dataset.clone().into()),
    );

    let args = [text, dataset];

    let doc = DocString::new_class("Dataset from FCS file parsed with flat mode.").args(args);

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #path::new(text.into(), dataset.into()).into()
            }
        }
    };
    doc.into_impl_class(name, &path, new).1.into()
}

#[proc_macro]
pub fn impl_py_flat_dataset_with_kws_output(input: TokenStream) -> TokenStream {
    let path = parse_macro_input!(input as Path);
    let name = path.segments.last().unwrap().ident.clone();

    let data = DocArg::new_data_param(false).into_ro(|_, _| quote!(self.0.data.clone()));
    let analysis =
        DocArg::new_analysis_param(false).into_ro(|_, _| quote!(self.0.analysis.clone()));
    let others = DocArg::new_others_param(false).into_ro(|_, _| quote!(self.0.others.clone()));
    let dataset_segs =
        DocArg::new_dataset_segments_param().into_ro(|_, _| quote!(self.0.dataset_segments.into()));

    let args = [data, analysis, others, dataset_segs];
    let doc = DocString::new_class("Dataset from parsing flat *TEXT*.").args(args);

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #path::new(data, analysis, others, dataset_segs.into()).into()
            }
        }
    };
    doc.into_impl_class(name, &path, new).1.into()
}

#[proc_macro]
pub fn impl_py_extra_std_keywords(input: TokenStream) -> TokenStream {
    let path = parse_macro_input!(input as Path);
    let name = path.segments.last().unwrap().ident.clone();

    let pseudostandard = DocArgROIvar::new_ivar_ro(
        "pseudostandard",
        PyDict::new_std_keywords(),
        "Keywords which start with *$* but are not part of the standard.",
        |_, _| quote!(self.0.pseudostandard.clone()),
    );

    let hyper_par = DocArgROIvar::new_ivar_ro(
        "hyper_par",
        PyDict::new_std_keywords(),
        "Measurement keywords which are part of the standard but have an index outside *$PAR*.",
        |_, _| quote!(self.0.hyper_par.clone()),
    );

    let hyper_gate = DocArgROIvar::new_ivar_ro(
        "hyper_gate",
        PyDict::new_std_keywords(),
        "Gating keywords which are part of the standard but have an index outside *$GATE*.",
        |_, _| quote!(self.0.hyper_gate.clone()),
    );

    let other_version = DocArgROIvar::new_ivar_ro(
        "other_version",
        PyDict::new_std_keywords(),
        "Keywords which are from a different FCS version.",
        |_, _| quote!(self.0.other_version.clone()),
    );

    let timestep = DocArgROIvar::new_ivar_ro(
        "timestep",
        PyOpt::new(PyStr::default()),
        "Unused *$TIMESTEP* keyword",
        |_, _| quote!(self.0.timestep.clone()),
    );

    let doc = DocString::new_class("Extra keywords from *TEXT* standardization.").args([
        pseudostandard,
        hyper_par,
        hyper_gate,
        other_version,
        timestep,
    ]);

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #path::new(pseudostandard, hyper_par, hyper_gate, other_version, timestep).into()
            }
        }
    };
    doc.into_impl_class(name, &path, new).1.into()
}

#[proc_macro]
pub fn impl_py_dataset_segments(input: TokenStream) -> TokenStream {
    let path = parse_macro_input!(input as Path);
    let name = path.segments.last().unwrap().ident.clone();

    let data = DocArg::new_data_seg_param(SegmentSrc::Any).into_ro(|_, _| quote!(self.0.data));
    let analysis = DocArg::new_analysis_seg_param(SegmentSrc::Any, false)
        .into_ro(|_, _| quote!(self.0.analysis));

    let doc =
        DocString::new_class("Segments used to parse *DATA* and *ANALYSIS*").args([data, analysis]);

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #path::new(data_seg, analysis_seg).into()
            }
        }
    };
    doc.into_impl_class(name, &path, new).1.into()
}

#[proc_macro]
pub fn impl_py_std_text_output(input: TokenStream) -> TokenStream {
    let path = parse_macro_input!(input as Path);
    let name = path.segments.last().unwrap().ident.clone();

    let tot = DocArgROIvar::new_ivar_ro(
        "tot",
        PyOpt::new(PyInt::new_int(RsInt::Usize).rstype(keyword_path("Tot"))),
        "Value of *$TOT* from *TEXT*.",
        |_, _| quote!(self.0.tot.as_ref().copied()),
    );

    let dataset_segs =
        DocArg::new_dataset_segments_param().into_ro(|_, _| quote!(self.0.dataset_segments.into()));

    let extra =
        DocArg::new_extra_std_keywords_param().into_ro(|_, _| quote!(self.0.extra.clone().into()));

    let parse = DocArgROIvar::new_ivar_ro(
        "parse",
        PyClass::new_py(["api"], "FlatTEXTParseData"),
        "Miscellaneous data when parsing *TEXT*.",
        |_, _| quote!(self.0.parse.clone().into()),
    );

    let args = [tot, dataset_segs, extra, parse];
    let doc = DocString::new_class("Miscellaneous data when standardizing *TEXT*.").args(args);

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #path::new(tot, dataset_segs.into(), extra.into(), parse.into()).into()
            }
        }
    };
    doc.into_impl_class(name, &path, new).1.into()
}

#[proc_macro]
pub fn impl_py_std_dataset_output(input: TokenStream) -> TokenStream {
    let path = parse_macro_input!(input as Path);
    let name = path.segments.last().unwrap().ident.clone();

    let dataset = DocArgROIvar::new_ivar_ro(
        "dataset",
        PyClass::new_py(["api"], "StdDatasetWithKwsOutput"),
        "Data from parsing standardized *DATA*, *ANALYSIS*, and *OTHER* segments.",
        |_, _| quote!(self.0.dataset.clone().into()),
    );

    let parse = DocArgROIvar::new_ivar_ro(
        "parse",
        PyClass::new_py(["api"], "FlatTEXTParseData"),
        "Miscellaneous data when parsing *TEXT*.",
        |_, _| quote!(self.0.parse.clone().into()),
    );

    let args = [dataset, parse];

    let doc = DocString::new_class("Miscellaneous data when standardizing *TEXT*.").args(args);

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #path::new(dataset.into(), parse.into()).into()
            }
        }
    };
    doc.into_impl_class(name, &path, new).1.into()
}

#[proc_macro]
pub fn impl_py_std_dataset_with_kws_output(input: TokenStream) -> TokenStream {
    let path = parse_macro_input!(input as Path);
    let name = path.segments.last().unwrap().ident.clone();

    let dataset_segs =
        DocArg::new_dataset_segments_param().into_ro(|_, _| quote!(self.0.dataset_segments.into()));

    let extra =
        DocArg::new_extra_std_keywords_param().into_ro(|_, _| quote!(self.0.extra.clone().into()));

    let doc = DocString::new_class("Miscellaneous data when standardizing *TEXT* from keywords.")
        .args([dataset_segs, extra]);

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #path::new(dataset_segs.into(), extra.into()).into()
            }
        }
    };
    doc.into_impl_class(name, &path, new).1.into()
}

#[proc_macro]
pub fn impl_py_flat_text_parse_data(input: TokenStream) -> TokenStream {
    let path = parse_macro_input!(input as Path);
    let name = path.segments.last().unwrap().ident.clone();

    let segments = DocArgROIvar::new_ivar_ro(
        "header_segments",
        PyClass::new_py(["api"], "HeaderSegments"),
        "Segments from *HEADER*.",
        |_, _| quote!(self.0.header_segments.clone().into()),
    );

    let supp = DocArgROIvar::new_ivar_ro(
        "supp_text",
        PyOpt::new(PyTuple::new_supp_text_segment()),
        "Supplemental *TEXT* offsets if given.",
        |_, _| quote!(self.0.supp_text.as_ref().copied()),
    );

    let nextdata = DocArgROIvar::new_ivar_ro(
        "nextdata",
        PyOpt::new(RsInt::U64),
        "The value of *$NEXTDATA*.",
        |_, _| quote!(self.0.nextdata),
    );

    let delim = DocArgROIvar::new_ivar_ro(
        "delimiter",
        RsInt::U8,
        "Delimiter used to parse *TEXT*.",
        |_, _| quote!(self.0.delimiter),
    );

    let non_ascii = DocArgROIvar::new_ivar_ro(
        "non_ascii",
        PyList::new1(PyTuple::new2(vec![PyStr::default(); 2])),
        "Keywords with a non-ASCII but still valid UTF-8 key.",
        |_, _| quote!(self.0.non_ascii.clone()),
    );

    let byte_pairs = DocArgROIvar::new_ivar_ro(
        "byte_pairs",
        PyList::new1(PyTuple::new2(vec![PyBytes::default(); 2])),
        "Keywords with invalid UTF-8 characters.",
        |_, _| quote!(self.0.byte_pairs.clone()),
    );

    let primary_escaped = DocArgROIvar::new_ivar_ro(
        "primary_escaped",
        PyBool::default(),
        "``True`` if primary *TEXT* delimiters were escaped.",
        |_, _| quote!(self.0.primary_escaped),
    );

    let supp_escaped = DocArgROIvar::new_ivar_ro(
        "supp_escaped",
        PyOpt::new(PyBool::default()),
        "``True`` if supp *TEXT* delimiters were escaped.",
        |_, _| quote!(self.0.supp_escaped),
    );

    let args = [
        segments,
        supp,
        nextdata,
        delim,
        non_ascii,
        byte_pairs,
        primary_escaped,
        supp_escaped,
    ];

    let doc = DocString::new_class("Miscellaneous data obtained when parsing *TEXT*.").args(args);
    let inner_args = doc.idents_into();

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #path::new(#inner_args).into()
            }
        }
    };
    doc.into_impl_class(name, &path, new).1.into()
}

#[proc_macro]
pub fn impl_py_dataset_summary(input: TokenStream) -> TokenStream {
    let path = parse_macro_input!(input as Path);
    let name = path.segments.last().unwrap().ident.clone();

    let version = DocArgROIvar::new_version_ivar();

    let text_len = DocArgROIvar::new_ivar_ro(
        "text_len",
        RsInt::U64,
        "Length of *TEXT* (in bytes)",
        |_, _| quote!(self.0.text_len),
    );

    let data_len = DocArgROIvar::new_ivar_ro(
        "data_len",
        RsInt::U64,
        "Length of *DATA* (in bytes)",
        |_, _| quote!(self.0.data_len),
    );

    let analysis_len = DocArgROIvar::new_ivar_ro(
        "analysis_len",
        RsInt::U64,
        "Length of *ANALYSIS* (in bytes)",
        |_, _| quote!(self.0.analysis_len),
    );

    let n_events = DocArgROIvar::new_ivar_ro(
        "n_events",
        RsInt::Usize,
        "Number of events (*$TOT*)",
        |_, _| quote!(self.0.n_events),
    );

    let n_measurements = DocArgROIvar::new_ivar_ro(
        "n_measurements",
        RsInt::Usize,
        "Number of measurements (*$PAR*)",
        |_, _| quote!(self.0.n_measurements),
    );

    let n_other = DocArgROIvar::new_ivar_ro(
        "n_other",
        RsInt::Usize,
        "Number of *OTHER* segments",
        |_, _| quote!(self.0.n_other),
    );

    let others_len = DocArgROIvar::new_ivar_ro(
        "others_len",
        RsInt::Usize,
        "Total length of *OTHER* segments (in bytes)",
        |_, _| quote!(self.0.others_len),
    );

    let datatype = DocArgROIvar::new_ivar_ro(
        "datatype",
        PyOpt::new(PyLiteral::new_datatype()),
        "The value of *$DATATYPE*",
        |_, _| quote!(self.0.datatype),
    );

    let args = [
        version,
        text_len,
        data_len,
        analysis_len,
        n_events,
        n_measurements,
        n_other,
        others_len,
        datatype,
    ];

    let doc = DocString::new_class("High-level data describing an FCS dataset").args(args);
    let inner_args = doc.idents_into();

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #path::new(#inner_args).into()
            }
        }
    };
    doc.into_impl_class(name, &path, new).1.into()
}

#[proc_macro]
#[allow(clippy::too_many_lines)]
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

    let meas: AnyDocArg = DocArg::new_paired_measurements_param(version).into();
    let layout: AnyDocArg = DocArg::new_layout_ivar(version).into();
    let data: AnyDocArg = DocArg::new_df_ivar().into();
    let analysis: AnyDocArg = DocArg::new_analysis_ivar().into();
    let others = DocArg::new_others_ivar().into();

    let mode = if version < Version::FCS3_2 {
        let t = |p| PyLiteral::new2(["L", "U", "C"], p);
        DocArg::new_kw_ivar("Mode", "mode", t, None, true)
    } else {
        DocArg::new_kw_opt_ivar("Mode3_2", "mode", |p| PyLiteral::new2(["L"], p))
    };

    let cyt = if version < Version::FCS3_2 {
        DocArg::new_kw_ivar_str("Cyt", "cyt")
    } else {
        DocArg::new_kw_ivar("Cyt3_2", "cyt", PyStr::new_non_empty_str, None, false)
    };

    let py_float = |p| PyFloat::new_non_negative_float().rstype(p);
    let py_int = |p| PyInt::new_u32().rstype(p);

    let abrt = DocArg::new_kw_opt_ivar("Abrt", "abrt", py_int);
    let com = DocArg::new_kw_ivar_str("Com", "com");
    let cells = DocArg::new_kw_ivar_str("Cells", "cells");
    let exp = DocArg::new_kw_ivar_str("Exp", "exp");
    let fil = DocArg::new_kw_ivar_str("Fil", "fil");
    let inst = DocArg::new_kw_ivar_str("Inst", "inst");
    let lost = DocArg::new_kw_opt_ivar("Lost", "lost", py_int);
    let op = DocArg::new_kw_ivar_str("Op", "op");
    let proj = DocArg::new_kw_ivar_str("Proj", "proj");
    let smno = DocArg::new_kw_ivar_str("Smno", "smno");
    let src = DocArg::new_kw_ivar_str("Src", "src");
    let sys = DocArg::new_kw_ivar_str("Sys", "sys");
    let cytsn = DocArg::new_kw_ivar_str("Cytsn", "cytsn");

    let unicode_pytype = |p| {
        PyTuple::new1(RsInt::U32)
            .add_new(PyList::new1(PyStr::default()))
            .rstype(p)
    };
    let unicode = DocArg::new_kw_opt_ivar("Unicode", "unicode", unicode_pytype);

    let csvbits = DocArg::new_kw_ivar("CSVBits", "csvbits", py_int, None, true);
    let cstot = DocArg::new_kw_ivar("CSTot", "cstot", py_int, None, true);

    let csvflags = DocArg::new_csvflags_ivar();

    let all_subset = [csvbits, cstot, csvflags];

    let last_modifier = DocArg::new_kw_ivar_str("LastModifier", "last_modifier");
    let last_mod_date = DocArg::new_kw_opt_ivar("LastModified", "last_modified", |p| {
        PyDatetime::default().rstype(p)
    });
    let originality = DocArg::new_kw_opt_ivar("Originality", "originality", |p| {
        PyLiteral::new2(
            ["Original", "NonDataModified", "Appended", "DataModified"],
            p,
        )
    });

    let all_modified = [last_modifier, last_mod_date, originality];

    let plateid = DocArg::new_kw_ivar_str("Plateid", "plateid");
    let platename = DocArg::new_kw_ivar_str("Platename", "platename");
    let wellid = DocArg::new_kw_ivar_str("Wellid", "wellid");

    let all_plate = [plateid, platename, wellid];

    let vol = DocArg::new_kw_opt_ivar("Vol", "vol", py_float);

    let comp_or_spill = match version {
        Version::FCS2_0 => DocArg::new_comp_ivar(true),
        Version::FCS3_0 => DocArg::new_comp_ivar(false),
        _ => DocArg::new_spillover_ivar(),
    };

    let flowrate = DocArg::new_kw_ivar_str("Flowrate", "flowrate");

    let carrierid = DocArg::new_kw_ivar_str("Carrierid", "carrierid");
    let carriertype = DocArg::new_kw_ivar_str("Carriertype", "carriertype");
    let locationid = DocArg::new_kw_ivar_str("Locationid", "locationid");

    let all_carrier = [carrierid, carriertype, locationid];

    let unstainedcenters = DocArg::new_unstainedcenters_ivar();
    let unstainedinfo = DocArg::new_kw_ivar_str("UnstainedInfo", "unstainedinfo");

    let tr = DocArg::new_trigger_ivar();

    let all_timestamps = DocArg::new_timestamps_ivar();

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
            .map(Into::into)
            .collect(),
        Version::FCS3_0 => [mode, cyt, comp_or_spill]
            .into_iter()
            .chain(all_timestamps)
            .chain([cytsn, unicode])
            .chain(all_subset)
            .chain(common_kws)
            .map(Into::into)
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
            .map(Into::into)
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
            .map(Into::into)
            .collect(),
    };

    let meas_layout_args = [meas, layout];

    let coretext_doc = DocString::new_class(format!("Represents *TEXT* for an FCS {vs} file."))
        .args(meas_layout_args.clone())
        .args(all_kws.clone());

    let coredataset_doc =
        DocString::new_class(format!("Represents one dataset in an FCS {vs} file."))
            .args(meas_layout_args)
            .arg(data)
            .args(all_kws)
            .args([analysis, others]);

    let coretext_inner_args = coretext_doc.idents_into();

    let coretext_new = |fun_args| {
        quote! {
            fn new(#fun_args) -> PyResult<Self> {
                let ret = #fun(#coretext_inner_args)
                    .group_with(fireflow_core::core::NewCoreTEXTSummary)
                    .resolve_nowarn()?;
                Ok(ret.into())
            }
        }
    };

    let coredataset_new = |fun_args| {
        quote! {
            fn new(#fun_args) -> PyResult<Self> {
                let x = #fun(#coretext_inner_args)
                    .group_with(fireflow_core::core::NewCoreDatasetSummary)
                    .resolve_nowarn()?;
                Ok(x.into_coredataset(data.0.try_into()?, analysis, others)?.into())
            }
        }
    };

    let (_, coretext_q) =
        coretext_doc.into_impl_class(coretext_name, &coretext_rstype, coretext_new);

    let (_, coredataset_q) =
        coredataset_doc.into_impl_class(coredataset_name, &coredataset_rstype, coredataset_new);

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
    let doc = DocString::new_ivar("Show the FCS version.", PyLiteral::new_version());
    doc.into_impl_get(&t, "version", |_, _| quote!(self.0.fcs_version()))
        .into()
}

#[proc_macro]
pub fn impl_core_par(input: TokenStream) -> TokenStream {
    let t = parse_macro_input!(input as Ident);
    let _ = split_ident_version_pycore(&t);
    let doc = DocString::new_ivar("The value for *$PAR*.", RsInt::Usize);
    doc.into_impl_get(&t, "par", |_, _| quote!(self.0.par().0))
        .into()
}

#[proc_macro]
pub fn impl_core_all_meas_nonstandard_keywords(input: TokenStream) -> TokenStream {
    let t = parse_macro_input!(input as Ident);
    let _ = split_ident_version_pycore(&t);

    let doc = DocString::new_ivar(
        "The non-standard keywords for each measurement.",
        PyList::new1(PyDict::new_nonstd_keywords()),
    );

    doc.into_impl_get_set(
        &t,
        "all_meas_nonstandard_keywords",
        true,
        |_, _| quote!(self.0.get_meas_nonstandard().clone().fmap(Clone::clone)),
        |n, _| quote!(Ok(self.0.set_meas_nonstandard(#n)?)),
    )
    .into()
}

#[proc_macro]
pub fn impl_core_standard_keywords(input: TokenStream) -> TokenStream {
    let ident = parse_macro_input!(input as Ident);
    let _ = split_ident_version_pycore(&ident);

    let req_or_opt_path = parse_quote!(fireflow_core::core::IncludeReqOrOpt);
    let root_or_meas_path = parse_quote!(fireflow_core::core::IncludeRootOrMeas);

    let req_or_opt = DocArg::new_param(
        "req_or_opt",
        PyLiteral::new2(["both", "req_only", "opt_only"], req_or_opt_path),
        "Selects if required, optional, or both keywords should be returned",
    );

    let root_or_meas = DocArg::new_param(
        "root_or_meas",
        PyLiteral::new2(["both", "req_only", "opt_only"], root_or_meas_path),
        "Selects if required, optional, or both keywords should be returned",
    );

    let doc = DocString::new_method("Return standard keywords as string pairs.")
        .para("Each key will be prefixed with *$*.")
        .para(
            "This will not include *$TOT*, *$NEXTDATA*, or any of the \
             offset keywords since these only matter if the dataset is written.",
        )
        .arg(req_or_opt)
        .arg(root_or_meas)
        .returns(DocReturn::new(PyDict::new_keywords()).desc("A list of standard keywords."));

    let fun_args = doc.fun_args();
    let inner_args = doc.idents_into();
    let ret_path = doc.ret_path();

    quote! {
        #[pymethods]
        impl #ident {
            #doc
            fn standard_keywords(&self, #fun_args) -> #ret_path {
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
    let p = DocArg::new_param("threshold", RsInt::U32, "The threshold to set.");
    let doc = DocString::new_method("Set the threshold for *$TR*.")
        .arg(p)
        .returns(
            DocReturn::new(PyBool::default()).desc("``True`` if trigger is set and was updated."),
        );

    let fun_arg = doc.fun_args();
    let ret_path = doc.ret_path();

    quote! {
        #[pymethods]
        impl #t {
            #doc
            fn set_trigger_threshold(&mut self, #fun_arg) -> #ret_path {
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

    let exc0 = PyException::new_segment_overflow(version);
    let exc1 = PyException::new_other_overflow();

    let nextdata = PyInt::new_nextdata();
    let ret = DocReturn::new(nextdata)
        .exc([exc0, exc1])
        .desc("the value of $NEXTDATA as written to the dataset");

    let doc = DocString::new_method("Write data to path.")
        .para("Resulting FCS file will include *HEADER* and *TEXT*.")
        .arg(DocArg::new_path_param(false))
        .arg(DocArg::new_textdelim_param())
        .arg(DocArg::new_big_other_param())
        .arg(DocArg::new_appendable_param())
        .arg(DocArg::new_append_param())
        .returns(ret);

    let fun_args = doc.fun_args();
    let ret_path = doc.ret_path();

    quote! {
        #[pymethods]
        impl #i {
            #doc
            fn write_text(&self, #fun_args) -> #ret_path {
                let tconf = fireflow_core::config::WriteTEXTInnerConfig::new(
                    delim,
                    big_other.into(),
                );
                let mconf = fireflow_core::config::WriteMultiConfig::new(
                    appendable.into(),
                    append.into(),
                );
                let conf = fireflow_core::config::WriteMultiTEXTConfig::new(
                    tconf,
                    mconf,
                );
                Ok(self.0.write_text(&path, &conf)?)
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_core_write_dataset(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let version = split_ident_version_pycore(&i).1;

    let exc0 = PyException::new_segment_overflow(version);
    let exc1 = PyException::new_other_overflow();

    let nextdata = PyInt::new_nextdata();
    let ret = DocReturn::new(nextdata)
        .exc([exc0, exc1])
        .desc("the value of *$NEXTDATA* which would point to next dataset if written");

    let doc = DocString::new_method("Write data as an FCS file.")
        .para(
            "The resulting file will include *HEADER*, *TEXT*, *DATA*, \
             *ANALYSIS*, and *OTHER* as they present from this class.",
        )
        .arg(DocArg::new_path_param(false))
        .arg(DocArg::new_textdelim_param())
        .arg(DocArg::new_big_other_param())
        .arg(DocArg::new_skip_conversion_check_param())
        .arg(DocArg::new_appendable_param())
        .arg(DocArg::new_append_param())
        .returns(ret);

    let fun_args = doc.fun_args();
    let ret_path = doc.ret_path();

    quote! {
        #[pymethods]
        impl #i {
            #doc
            fn write_dataset(&self, #fun_args) -> #ret_path {
                let tconf = fireflow_core::config::WriteTEXTInnerConfig::new(
                    delim,
                    big_other.into(),
                );
                let dconf = fireflow_core::config::WriteDatasetInnerConfig::new(
                    tconf,
                    skip_conversion_check.into(),
                );
                let mconf = fireflow_core::config::WriteMultiConfig::new(
                    appendable.into(),
                    append.into(),
                );
                let conf = fireflow_core::config::WriteMultiDatasetConfig::new(
                    dconf,
                    mconf,
                );
                self.0.write_dataset(&path, &conf).py_resolve_commutative()
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_core_all_peak_attrs(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let _ = split_ident_version_pycore(&i).1;

    let go = |k: &str, kw: &str, name: &str| {
        let p = keyword_path(kw);
        let pt = PyOpt::new(PyInt::new_u32().rstype(p));
        let inner = pt.as_rust_type();
        let doc = DocString::new_ivar(
            format!("The value of *$P{k}n* for all measurements."),
            PyList::new1(pt),
        );

        doc.into_impl_get_set(
            &i,
            format!("all_{name}"),
            true,
            |_, _| {
                quote! {
                    self.0
                        .get_temporal_optical::<#inner, #inner>()
                        .map(|x| x.unwrap().as_ref().copied())
                        .collect()
                }
            },
            |n, _| quote!(Ok(self.0.set_temporal_optical(#n)?)),
        )
    };

    let pkn = go("K", "PeakBin", "peak_bins");
    let pknn = go("KN", "PeakIndex", "peak_sizes");

    quote! {
        #pkn
        #pknn
    }
    .into()
}

#[proc_macro]
pub fn impl_core_all_shortnames_attr(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let _ = split_ident_version_pycore(&i).1;

    let doc = DocString::new_ivar(
        "Value of *$PnN* for all measurements.",
        PyList::new1(PyStr::new_shortname()),
    )
    .para("Strings are unique and cannot contain commas.");

    doc.into_impl_get_set(
        &i,
        "all_shortnames",
        true,
        |_, _| quote!(self.0.all_shortnames()),
        |n, _| quote!(Ok(self.0.set_all_shortnames(#n).map(|_| ())?)),
    )
    .into()
}

#[proc_macro]
pub fn impl_core_all_shortnames_maybe_attr(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let _ = split_ident_version_pycore(&i).1;

    let doc = DocString::new_ivar(
        "The possibly-empty values of *$PnN* for all measurements.",
        PyList::new1(PyOpt::new(PyStr::new_shortname())),
    )
    .para("*$PnN* is optional for this FCS version so values may be ``None``.");

    doc.into_impl_get_set(
        &i,
        "all_shortnames_maybe",
        true,
        |_, _| quote!(self.0.shortnames_maybe().fmap(|x| x.cloned())),
        |n, _| quote!(Ok(self.0.set_measurement_shortnames_maybe(#n).map(|_| ())?)),
    )
    .into()
}

#[proc_macro]
pub fn impl_core_get_set_timestep(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let _ = split_ident_version_pycore(&i).1;

    let t = PyOpt::new(PyFloat::new_timestep());
    let get_doc = DocString::new_ivar("The value of *$TIMESTEP*", t.clone());

    let getq = get_doc.into_impl_get(&i, "timestep", |_, _| quote!(self.0.timestep().copied()));

    let param = DocArg::new_param(
        "timestep",
        PyFloat::new_timestep(),
        "The timestep to set. Must be greater than zero.",
    );
    let set_doc = DocString::new_method("Set the *$TIMESTEP* if time measurement is present.")
        .arg(param)
        .returns(DocReturn::new(t.map_exc(|_| ())).desc("Previous *$TIMESTEP* if present."));

    let set_ret = set_doc.ret_path();
    let set_fun_arg = set_doc.fun_args();

    let setq = quote! {
        #[pymethods]
        impl #i {
            #set_doc
            fn set_timestep(&mut self, #set_fun_arg) -> #set_ret {
                self.0.set_timestep(timestep)
            }
        }
    };

    quote!(#getq #setq).into()
}

// TODO there are many exceptions on these functions (and others like it) that
// are not documented. For instance, if the name is not found it should produce
// a keyerror.
#[proc_macro]
pub fn impl_core_set_temporal(input: TokenStream) -> TokenStream {
    let ident: Ident = syn::parse(input).unwrap();
    let version = split_ident_version_pycore(&ident).1;

    let make_doc = |has_timestep: bool, has_index: bool| {
        let name = DocArg::new_name_param("Name to set to temporal.");
        let index = DocArg::new_param("index", PyInt::new_meas_index(), "Index to set.");
        let (i, p) = if has_index {
            ("index", index)
        } else {
            ("name", name)
        };
        let timestep = has_timestep.then_some(DocArg::new_param(
            "timestep",
            PyFloat::new_timestep(),
            "The value of *$TIMESTEP* to use.",
        ));
        let exc = PyreflowError::Conversion.fmt_ref();
        let allow_loss = DocArg::new_bool_param(
            "allow_loss",
            format!(
                "If ``True`` remove any optical-specific metadata (detectors, \
                 lasers, etc) without raising an {exc} if an optical measurement \
                 must be converted."
            ),
        );
        DocString::new_method(format!("Set the temporal measurement to a given {i}."))
            .args(once(p).chain(timestep).chain([allow_loss]))
            .returns(DocReturn::new(PyBool::default()).desc(format!(
                "``True`` if temporal measurement was set, which will \
                 happen for all cases except when the time measurement is \
                 already set to ``{i}``."
            )))
    };

    let q = if version == Version::FCS2_0 {
        let name_doc = make_doc(false, false);
        let index_doc = make_doc(false, true);
        let name_fun_args = name_doc.fun_args();
        let index_fun_args = index_doc.fun_args();
        quote! {
            #name_doc
            fn set_temporal(&mut self, #name_fun_args) -> PyResult<bool> {
                self.0.set_temporal(&name, (), allow_loss).py_resolve_non_commutative()
            }

            #index_doc
            fn set_temporal_at(&mut self, #index_fun_args) -> PyResult<bool> {
                self.0.set_temporal_at(index, (), allow_loss).py_resolve_non_commutative()
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
                    .set_temporal(&name, timestep, allow_loss)
                    .py_resolve_non_commutative()
            }

            #index_doc
            fn set_temporal_at(&mut self, #index_fun_args) -> PyResult<bool> {
                self.0
                    .set_temporal_at(index, timestep, allow_loss)
                    .py_resolve_non_commutative()
            }
        }
    };

    quote! {
        #[pymethods]
        impl #ident {
            #q
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_core_unset_temporal(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let version = split_ident_version_pycore(&i).1;

    let exc = PyreflowError::Conversion.fmt_ref();

    let make_doc = |has_timestep: bool, has_allow_loss: bool| {
        let s = "Convert the temporal measurement to an optical measurement.";
        let p = has_allow_loss
            .then_some(DocArg::new_bool_param(
                "allow_loss",
                format!(
                    "If ``True`` and current time measurement has data which cannot \
                     be converted to optical, force the conversion anyways. \
                     Otherwise raise {exc}."
                ),
            ))
            .into_iter();
        let (rt, rd) = if has_timestep {
            (
                PyOpt::new(PyFloat::new_timestep()).into(),
                "Value of *$TIMESTEP* if time measurement was present.",
            )
        } else {
            (
                PyType::from(PyBool::default()),
                "``True`` if temporal measurement was present and converted, \
                 ``False`` if there was not a temporal measurement.",
            )
        };
        DocString::new_method(s)
            .args(p)
            .returns(DocReturn::new(rt).desc(rd))
    };

    let q = if version == Version::FCS2_0 {
        let doc = make_doc(false, false);
        let ret = doc.ret_path();
        quote! {
            #doc
            fn unset_temporal(&mut self) -> #ret {
                self.0.unset_temporal().is_some()
            }
        }
    } else if version < Version::FCS3_2 {
        let doc = make_doc(true, false);
        let ret = doc.ret_path();
        quote! {
            #doc
            fn unset_temporal(&mut self) -> #ret {
                self.0.unset_temporal()
            }
        }
    } else {
        let doc = make_doc(true, true);
        let ret = doc.ret_path();
        quote! {
            #doc
            fn unset_temporal(&mut self, allow_loss: bool) -> PyResult<#ret> {
                self.0.unset_temporal_lossy(allow_loss).py_resolve_non_commutative()
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

    let doc = DocString::new_method("Rename temporal measurement if present.")
        .arg(DocArg::new_name_param("New name to assign."))
        .returns(
            DocReturn::new(PyOpt::new(PyStr::new_shortname())).desc("Previous name if present."),
        );

    let fun_args = doc.fun_args();
    let ret_path = doc.ret_path();

    quote! {
        #[pymethods]
        impl #i {
            #doc
            fn rename_temporal(&mut self, #fun_args) -> #ret_path {
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

    let exc = PyreflowError::Relational.fmt_ref();

    if version == Version::FCS2_0 {
        let s0 = "Will be ``()`` for linear scaling (``0,0`` in FCS encoding), \
                   a 2-tuple for log scaling, or ``None`` if missing.";
        let s1 = format!(
            "The temporal measurement must always be ``()``. \
             Setting it to another value will raise {exc}."
        );
        let doc = DocString::new_ivar(
            "The value for *$PnE* for all measurements.",
            PyList::new1(PyOpt::new(PyUnion::new_scale(false))),
        )
        .paras([s0.into(), s1]);

        doc.into_impl_get_set(
            &i,
            "all_scales",
            true,
            |_, _| quote!(self.0.scales().collect()),
            |n, _| quote!(Ok(self.0.set_scales(#n)?)),
        )
    } else {
        let sum = "The value for *$PnE* and/or *$PnG* for all measurements.";
        let s0 = "Collectively these keywords correspond to scale transforms.";
        let s1 = "If scaling is linear, return a float which corresponds to the \
                  value of *$PnG* when *$PnE* is ``0,0``. If scaling is logarithmic, \
                  return a pair of floats, corresponding to unset *$PnG* and the \
                  non-``0,0`` value of *$PnE*.";
        let s2 = "The FCS standards disallow any other combinations.";
        let s3 = format!(
            "The temporal measurement will always be ``1.0``, corresponding \
             to an identity transform. Setting it to another value will \
             raise {exc}."
        );
        let ss = [s0, s1, s2, s3.as_str()];
        let doc = DocString::new_ivar(sum, PyList::new1(PyUnion::new_transform())).paras(ss);

        doc.into_impl_get_set(
            &i,
            "all_scale_transforms",
            true,
            |_, _| quote!(self.0.transforms().collect()),
            |n, _| quote!(Ok(self.0.set_transforms(#n)?)),
        )
    }
    .into()
}

#[proc_macro]
pub fn impl_core_get_measurements(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let version = split_ident_version_pycore(&i).1;

    let doc = DocString::new_ivar(
        "All measurements.",
        PyList::new1(PyUnion::new_measurement(version)),
    );

    doc.into_impl_get_set(
        &i,
        "measurements",
        true,
        |_, _| {
            quote! {
                // This might seem inefficient since we are cloning everything,
                // but if we want to map a python lambda function over the
                // measurements we would need to to do this anyways, so simply
                // returning a copied list doesn't lose anything and keeps this
                // API simpler.
                self.0
                    .measurements()
                    .iter()
                    .map(|e| e.bimap_once(|t| t.value.clone(), |o| o.value.clone()))
                    .map(|v| v.bimap_into_once())
                    .collect()
            }
        },
        |n, _| {
            quote! {
                let ms = #n.into_iter().map(|m| m.bimap_into_once()).collect();
                Ok(self.0.set_measurements(ms)?)
            }
        },
    )
    .into()
}

#[proc_macro]
pub fn impl_core_get_temporal(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let version = split_ident_version_pycore(&i).1;

    let doc = DocString::new_ivar(
        "The temporal measurement if it exists.",
        PyOpt::new(
            PyTuple::new1(PyInt::new_meas_index())
                .add_new(PyStr::new_shortname())
                .add_new(PyClass::new_temporal(version)),
        ),
    )
    .ret_desc("Index, name, and measurement or ``None``.");

    doc.into_impl_get(&i, "temporal", |_, _| {
        quote! {
            self.0
                .temporal()
                .map(|t| (t.index, t.key.clone(), t.value.clone().into()))
        }
    })
    .into()
}

#[proc_macro]
pub fn impl_core_get_measurement(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let version = split_ident_version_pycore(&i).1;

    let exc = PyException::new_index().desc("If ``index`` not found");
    let doc = DocString::new_method("Return measurement at index.")
        .arg(DocArg::new_index_param("Index to retrieve."))
        .returns(DocReturn::new(PyUnion::new_measurement(version)).exc([exc]));

    let fun_args = doc.fun_args();
    let ret = doc.ret_path();

    quote! {
        #[pymethods]
        impl #i {
            #doc
            fn measurement_at(&self, #fun_args) -> #ret {
                let m = self.0.measurements().get(index)?;
                Ok(m.bimap_once(|x| x.1.clone(), |x| x.1.clone()).bimap_into_once())
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_core_get_named_measurement(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let version = split_ident_version_pycore(&i).1;

    let exc = PyException::new_key().desc("If ``name`` not found");
    let doc = DocString::new_method("Return measurement with name.")
        .arg(DocArg::new_name_param("Name to retrieve."))
        .returns(DocReturn::new(PyUnion::new_measurement(version)).exc([exc]));

    let fun_args = doc.fun_args();
    let ret = doc.ret_path();

    quote! {
        #[pymethods]
        impl #i {
            #doc
            fn measurement_named(&self, #fun_args) -> #ret {
                let (_, m) = self.0.measurements().get_name(&name)?;
                Ok(m.bimap_once(|x| x.clone(), |x| x.clone()).bimap_into_once())
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_core_set_named_measurements(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let (is_dataset, version) = split_ident_version_pycore(&i);

    let s = if is_dataset {
        "layout and dataframe"
    } else {
        "layout"
    };
    let ps = [format!(
        "Length of ``measurements`` must match number of columns in existing {s}."
    )];
    let doc = DocString::new_method("Set all measurements at once.")
        .paras(ps)
        .arg(DocArg::new_set_meas_param(version))
        .arg(DocArg::new_allow_shared_names_param())
        .arg(DocArg::new_skip_index_check_param());

    let fun_args = doc.fun_args();

    quote! {
        #[pymethods]
        impl #i {
            #doc
            fn set_named_measurements(&mut self, #fun_args) -> PyResult<()> {
                let ret = self.0
                    .set_named_measurements(
                        measurements.into(),
                        allow_shared_names,
                        skip_index_check,
                    )?;
                Ok(ret)
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
        let (meas_type, what) = if is_optical {
            (PyClass::new_optical(version), "optical")
        } else {
            (PyClass::new_temporal(version), "temporal")
        };
        let param_meas = DocArg::new_param("meas", meas_type, "The measurement to push.");
        let col_param = hasdata.then_some(DocArg::new_col_param());
        let summary = format!("Push {what} measurement to end of measurement vector.");
        DocString::new_method(summary)
            .arg(DocArg::new_name_param("Name of new measurement."))
            .arg(param_meas)
            .args(col_param)
            .args([DocArg::new_range_param(), DocArg::new_notrunc_param()])
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
                    .py_resolve_commutative()
                    .map(|_| ())
            }

            #tmp_doc
            fn push_temporal(&mut self, #tmp_fun_args) -> PyResult<()> {
                self.0
                    .push_temporal(#tmp_inner_args)
                    .py_resolve_commutative()
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_core_remove_measurement(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let (is_dataset, version) = split_ident_version_pycore(&i);

    let make_ret = |is_index: bool| {
        // NOTE this is not a typo, these are supposed to be flipped
        let name_or_index = if is_index {
            PyType::new_versioned_shortname(version)
        } else {
            PyInt::new_meas_index().into()
        };
        let ret = if is_dataset {
            PyTuple::new1(name_or_index)
                .add_new(PyUnion::new_measurement(version))
                .add_new(PyClass::new_series())
                .add_new(PyDecimal::new_range())
        } else {
            PyTuple::new1(name_or_index)
                .add_new(PyUnion::new_measurement(version))
                .add_new(PyDecimal::new_range())
        };
        let (which, exc) = if is_index {
            let exc = PyException::new_index().desc("If ``index`` not found");
            ("Index", exc)
        } else {
            let exc = PyException::new_key().desc("If ``name`` not found");
            ("Name", exc)
        };
        let desc = if is_dataset {
            format!("{which}, measurement object, data, and range.")
        } else {
            format!("{which}, measurement object, and range.")
        };
        DocReturn::new(ret).desc(desc).exc([exc])
    };

    let by_name_doc = DocString::new_method("Remove a measurement with a given name.")
        .arg(DocArg::new_name_param("Name to remove."))
        .returns(make_ret(false));

    let by_index_doc = DocString::new_method("Remove a measurement with a given index.")
        .arg(DocArg::new_index_param("Index to remove."))
        .returns(make_ret(true));

    let name_arg = by_name_doc.fun_args();
    let index_arg = by_index_doc.fun_args();

    let name_ident = by_name_doc.idents();
    let index_ident = by_index_doc.idents();

    let name_ret = by_name_doc.ret_path();
    let index_ret = by_index_doc.ret_path();

    let name_mapper = if is_dataset {
        quote!(|(i, x, c, r)| (i, x.bimap_into_once(), c, r))
    } else {
        quote!(|(i, x, r)| (i, x.bimap_into_once(), r))
    };

    let index_body = if is_dataset {
        quote! {
            let (p, c, r) = self.0.remove_measurement_by_index(#index_ident)?;
            let (n, v) = p.unzip();
            Ok((n, v.bimap_into_once(), c, r))
        }
    } else {
        quote! {
            let (p, r) = self.0.remove_measurement_by_index(#index_ident)?;
            let (n, v) = p.unzip();
            Ok((n, v.bimap_into_once(), r))
        }
    };

    quote! {
        #[pymethods]
        impl #i {
            #by_name_doc
            fn remove_measurement_by_name(
                &mut self,
                #name_arg
            ) -> #name_ret {
                Ok(self
                   .0
                   .remove_measurement_by_name(&#name_ident)
                   .map(#name_mapper)?)
            }

            #by_index_doc
            fn remove_measurement_by_index(
                &mut self,
                #index_arg
            ) -> #index_ret {
                #index_body
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
        let (meas_type, what) = if is_optical {
            (PyClass::new_optical(version), "optical")
        } else {
            (PyClass::new_temporal(version), "temporal")
        };
        let param_meas = DocArg::new_param("meas", meas_type, "The measurement to insert.");
        let col_param = hasdata.then_some(DocArg::new_col_param());
        let summary = format!("Insert {what} measurement at position in measurement vector.");
        DocString::new_method(summary)
            .arg(DocArg::new_index_param(
                "Position at which to insert new measurement.",
            ))
            .arg(DocArg::new_name_param("Name of new measurement."))
            .arg(param_meas)
            .args(col_param)
            .args([DocArg::new_range_param(), DocArg::new_notrunc_param()])
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
                    .py_resolve_commutative()
                    .map(|_| ())
            }

            #tmp_doc
            fn insert_temporal(
                &mut self,
                #tmp_fun_args
            ) -> PyResult<()> {
                self.0
                    .insert_temporal(#tmp_inner_args)
                    .py_resolve_commutative()
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_core_replace_optical(input: TokenStream) -> TokenStream {
    let ident: Ident = syn::parse(input).unwrap();
    let version = split_ident_version_pycore(&ident).1;

    let make_replace_doc = |is_index: bool| {
        let (i_param, m, e) = if is_index {
            (
                DocArg::new_index_param("Index to replace."),
                "measurement at index",
                PyException::new_index(),
            )
        } else {
            (
                DocArg::new_name_param("Name to replace."),
                "named measurement",
                PyException::new_key(),
            )
        };
        let i = &i_param.argname;
        let meas_desc = format!("Optical measurement to replace measurement at ``{i}``.");
        let exc_desc = format!("If ``{i}`` does not exist.");
        let exc = e.desc(exc_desc);
        let ret = PyUnion::new_measurement(version);
        DocString::new_method(format!("Replace {m} with given optical measurement."))
            .arg(i_param)
            .arg(DocArg::new_param(
                "meas",
                PyClass::new_optical(version),
                meas_desc,
            ))
            .returns(
                DocReturn::new(ret)
                    .desc("Replaced measurement object.")
                    .exc([exc]),
            )
    };

    let replace_at_doc = make_replace_doc(true);
    let replace_named_doc = make_replace_doc(false);

    let index_fun_args = replace_at_doc.fun_args();
    let name_fun_args = replace_named_doc.fun_args();

    let index_ret = replace_at_doc.ret_path();
    let named_ret = replace_named_doc.ret_path();

    quote! {
        #[pymethods]
        impl #ident {
            #replace_at_doc
            fn replace_optical_at(&mut self, #index_fun_args) -> #index_ret {
                Ok(self.0.replace_optical_at(index, meas.into())?.bimap_into_once())
            }

            #replace_named_doc
            fn replace_optical_named(&mut self, #name_fun_args) -> #named_ret {
                Ok(self.0.replace_optical_named(&name, meas.into())?.bimap_into_once())
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_core_replace_temporal(input: TokenStream) -> TokenStream {
    let ident: Ident = syn::parse(input).unwrap();
    let version = split_ident_version_pycore(&ident).1;

    // the temporal replacement functions for 3.2 are different because they
    // can fail if $PnTYPE is set
    let (replace_tmp_at_body, replace_tmp_named_body, allow_loss) = if version == Version::FCS3_2 {
        let exc = PyreflowError::Conversion.fmt_ref();
        let allow_loss_param = DocArg::new_bool_param(
            "allow_loss",
            format!(
                "If ``False``, raise {exc} if conversion from temporal \
                     measurement to optical measurement is necessary and data \
                     keywords must be dropped."
            ),
        );
        let go =
            |fun, x| quote!(self.0.#fun(#x, meas.into(), allow_loss).py_resolve_non_commutative()?);
        (
            go(quote! {replace_temporal_at_lossy}, quote! {index}),
            go(quote! {replace_temporal_named_lossy}, quote! {&name}),
            Some(allow_loss_param),
        )
    } else {
        (
            quote! {self.0.replace_temporal_at(index, meas.into())?},
            quote! {self.0.replace_temporal_named(&name, meas.into())?},
            None,
        )
    };

    let make_replace_doc = |is_index: bool| {
        let (i_param, m, e) = if is_index {
            (
                DocArg::new_index_param("Index to replace."),
                "measurement at index",
                PyException::new_index(),
            )
        } else {
            (
                DocArg::new_name_param("Name to replace."),
                "named measurement",
                PyException::new_key(),
            )
        };
        let i = &i_param.argname;
        let meas_desc = format!("Temporal measurement to replace measurement at ``{i}``.");
        let exc0 = e.desc(format!("If ``{i}`` does not exist"));
        let exc1 = PyException::new_pyreflow(&PyreflowError::Relational)
            .desc("If a temporal measurement already exists at a different position");
        let xs = [exc0, exc1];
        let ret = PyUnion::new_measurement(version);
        let meas = DocArg::new_param("meas", PyClass::new_temporal(version), meas_desc);
        let dret = DocReturn::new(ret)
            .desc("Replaced measurement object.")
            .exc(xs);
        DocString::new_method(format!("Replace {m} with given temporal measurement."))
            .arg(i_param)
            .arg(meas)
            .args(allow_loss.clone())
            .returns(dret)
    };

    let replace_at_doc = make_replace_doc(true);
    let replace_named_doc = make_replace_doc(false);

    let index_fun_args = replace_at_doc.fun_args();
    let name_fun_args = replace_named_doc.fun_args();

    let index_ret = replace_at_doc.ret_path();
    let named_ret = replace_named_doc.ret_path();

    quote! {
        #[pymethods]
        impl #ident {
            #replace_at_doc
            fn replace_temporal_at(
                &mut self,
                #index_fun_args
            ) -> #index_ret {
                let ret = #replace_tmp_at_body;
                Ok(ret.bimap_into_once())
            }

            #replace_named_doc
            fn replace_temporal_named(
                &mut self,
                #name_fun_args
            ) -> #named_ret {
                let ret = #replace_tmp_named_body;
                Ok(ret.bimap_into_once())
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

    let core_conf = config_path("NewCoreTEXTConfig");

    let v = Some(version);
    let (std_conf, std_args, std_recs) = DocArgParam::new_read_std_config_params(v);
    let (layout_conf, layout_args, layout_recs) = DocArgParam::new_read_layout_config_params(v);
    let (shared_conf, shared_args, shared_recs) = DocArgParam::new_shared_config_params();

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
        "std",
        PyDict::new_std_keywords(),
        format!("Standard keywords. {no_kws}"),
    );

    let nonstd_param = DocArg::new_param(
        "nonstd",
        PyDict::new_nonstd_keywords(),
        "Non-Standard keywords.",
    );

    let exc0 = PyException::new_parse_keyval();
    let exc1 = PyException::new_pyreflow(&PyreflowError::Relational)
        .desc("If keywords that are referenced by other keywords do not exist");
    let exc2 = PyException::new_extra();

    let xs = [exc0, exc1, exc2];

    let doc = DocString::new_fun("Make new instance from keywords.")
        .args([std_param, nonstd_param])
        .args(std_args)
        .args(layout_args)
        .args(shared_args)
        .returns(
            DocReturn::new(PyTuple::new2([
                PyClass::new_coretext(version),
                PyClass::new_py(["api"], "ExtraStdKeywords"),
            ]))
            .exc(xs),
        );

    let fun_args = doc.fun_args();
    let ret_path = doc.ret_path();

    quote! {
        #[pymethods]
        impl #pyname {
            #[classmethod]
            #[allow(clippy::too_many_arguments)]
            #doc
            fn from_kws(_: &Bound<'_, pyo3::types::PyType>, #fun_args) -> #ret_path {
                let kws = fireflow_core::validated::keys::ValidKeywords { std, nonstd };
                #[allow(clippy::needless_update)]
                let standard = #std_conf {
                    #(#std_recs,)*
                    ..#std_conf::default()
                };
                #[allow(clippy::needless_update)]
                let layout = #layout_conf {
                    #(#layout_recs,)*
                    ..#layout_conf::default()
                };
                let shared = #shared_conf { #(#shared_recs),* };
                let conf = #core_conf { standard, layout, shared };
                let (core, uncore) = #path::new_from_keywords(kws, &conf).py_resolve_commutative()?;
                Ok((core.into(), uncore.into()))
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

    let core_conf = config_path("NewCoreDatasetConfig");

    let v = Some(version);
    let (std_conf, std_args, std_recs) = DocArgParam::new_read_std_config_params(v);
    let (layout_conf, layout_args, layout_recs) = DocArgParam::new_read_layout_config_params(v);
    let (data_conf, data_args, data_recs) = DocArgParam::new_read_events_config_params();
    let (shared_conf, shared_args, shared_recs) = DocArgParam::new_shared_config_params();

    let config_args: Vec<_> = std_args
        .into_iter()
        .chain(layout_args)
        .chain(data_args)
        .chain(shared_args)
        .collect();

    let path_param = DocArg::new_path_param(true);

    let std_param = DocArg::new_param("std", PyDict::new_std_keywords(), "Standard keywords.");

    let nonstd_param = DocArg::new_param(
        "nonstd",
        PyDict::new_nonstd_keywords(),
        "Non-Standard keywords.",
    );

    let data_seg_param = DocArg::new_rel_data_seg_param();
    let analysis_seg_param = DocArg::new_rel_analysis_seg_param();
    let other_segs_param = DocArg::new_rel_other_segs_param();
    let dataset_offset_param = DocArg::new_dataset_offset_param();

    let exc0 = PyException::new_deprecated();
    let exc1 = PyException::new_parse_keyval();
    let exc2 = PyException::new_pyreflow(&PyreflowError::Relational).desc(
        "If keywords are incompatible with indicated layout of *DATA* or \
         if keywords that are referenced by other keywords do not exist",
    );
    let exc3 = PyException::new_event_data();
    let exc4 = PyException::new_extra();

    let xs = [exc0, exc1, exc2, exc3, exc4];

    let doc = DocString::new_fun("Make new instance from keywords.")
        .arg(path_param)
        .arg(std_param)
        .arg(nonstd_param)
        .arg(data_seg_param)
        .arg(analysis_seg_param)
        .arg(other_segs_param)
        .args(config_args)
        .arg(dataset_offset_param)
        .returns(
            DocReturn::new(PyTuple::new2([
                PyClass::new_coredataset(version),
                PyClass::new_py(["api"], "StdDatasetWithKwsOutput"),
            ]))
            .exc(xs),
        );

    let fun_args = doc.fun_args();
    let ret_path = doc.ret_path();

    quote! {
        #[pymethods]
        impl #pyname {
            #[classmethod]
            #[allow(clippy::too_many_arguments)]
            #doc
            fn from_kws(_: &Bound<'_, pyo3::types::PyType>, #fun_args) -> #ret_path {
                let kws = fireflow_core::validated::keys::ValidKeywords { std, nonstd };
                #[allow(clippy::needless_update)]
                let standard = #std_conf {
                    #(#std_recs,)*
                    ..#std_conf::default()
                };
                #[allow(clippy::needless_update)]
                let layout = #layout_conf {
                    #(#layout_recs,)*
                    ..#layout_conf::default()
                };
                #[allow(clippy::needless_update)]
                let data = #data_conf {
                    #(#data_recs,)*
                    ..#data_conf::default()
                };
                #[allow(clippy::needless_update)]
                let shared = #shared_conf {
                    #(#shared_recs,)*
                    ..#shared_conf::default()
                };
                let conf = #core_conf { standard, layout, data, shared };
                let (core, uncore) = #path::new_from_keywords(
                    &path, kws, data_seg, analysis_seg, other_segs, dataset_offset, &conf
                ).py_resolve_commutative()?;
                Ok((core.into(), uncore.into()))
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_coretext_write_multi(input: TokenStream) -> TokenStream {
    let path = parse_macro_input!(input as Path);
    let ident = path.segments.last().unwrap().ident.clone();
    let version = split_ident_version_checked("CoreTEXT", &ident);
    let pyname = format_ident!("Py{ident}");

    let path_arg = DocArg::new_path_param(false);
    let cores_arg = DocArg::new_param(
        "datasets",
        PyList::new1(PyClass::new_coretext(version)),
        "datasets to write",
    );

    let (conf, args, recs) = DocArgParam::new_write_text_config_params();

    let exc0 = PyException::new_segment_overflow(version);
    let exc1 = PyException::new_other_overflow();

    let xs = [exc0, exc1];

    let ret = DocReturn::new(PyOpt::new(PyInt::new_nextdata()))
        .desc("the value of *$NEXTDATA* as written in the last dataset")
        .exc(xs);

    let doc = DocString::new_fun("Write multiple datasets to path.")
        .para("The resulting file will have *HEADER* and *TEXT* from each object")
        .arg(path_arg)
        .arg(cores_arg)
        .args(args)
        .returns(ret);

    let fun_args = doc.fun_args();
    let ret_path = doc.ret_path();

    quote! {
        #[pymethods]
        impl #pyname {
            #[classmethod]
            #doc
            fn write_texts(_: &Bound<'_, pyo3::types::PyType>, #fun_args) -> #ret_path {
                let conf = #conf { #(#recs),* };
                let cs = datasets.fmap(|c| c.0);
                Ok(#path::write_texts(&path, &cs[..], &conf)?)
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_coredataset_write_multi(input: TokenStream) -> TokenStream {
    let path = parse_macro_input!(input as Path);
    let ident = path.segments.last().unwrap().ident.clone();
    let version = split_ident_version_checked("CoreDataset", &ident);
    let pyname = format_ident!("Py{ident}");

    let path_arg = DocArg::new_path_param(false);
    let cores_arg = DocArg::new_param(
        "datasets",
        PyList::new1(PyClass::new_coredataset(version)),
        "datasets to write",
    );

    let exc0 = PyException::new_segment_overflow(version);
    let exc1 = PyException::new_other_overflow();

    let xs = [exc0, exc1];

    let ret = DocReturn::new(PyOpt::new(PyInt::new_nextdata()))
        .desc("the value of *$NEXTDATA* as written in the last dataset if written")
        .exc(xs);

    let doc = DocString::new_fun("Write multiple datasets to path.")
        .para(
            "The resulting file will include *HEADER*, *TEXT*, *DATA*, \
             *ANALYSIS*, and *OTHER* as they present from this class.",
        )
        .arg(path_arg)
        .arg(cores_arg)
        .arg(DocArg::new_textdelim_param())
        .arg(DocArg::new_big_other_param())
        .arg(DocArg::new_skip_conversion_check_param())
        .returns(ret);

    let fun_args = doc.fun_args();
    let ret_path = doc.ret_path();

    quote! {
        #[pymethods]
        impl #pyname {
            #[classmethod]
            #doc
            fn write_datasets(_: &Bound<'_, pyo3::types::PyType>, #fun_args) -> #ret_path {
                let tconf = fireflow_core::config::WriteTEXTInnerConfig::new(
                    delim,
                    big_other.into(),
                );
                let dconf = fireflow_core::config::WriteDatasetInnerConfig::new(
                    tconf,
                    skip_conversion_check.into(),
                );
                let cs = datasets.fmap(|c| c.0);
                #path::write_datasets(&path, &cs[..], &dconf).py_resolve_commutative()
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
              *$PAR* to ``0``.";

    let exc = PyException::new_existing();
    let ret = DocReturn::new(PyTuple::default()).exc([exc]);
    let doc = DocString::new_method(s).paras([p0]).returns(ret);

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

    let exc = PyException::new_existing();
    let ret = DocReturn::new(PyTuple::default()).exc([exc]);
    let doc = DocString::new_method("Remove all measurements and their data.").returns(ret);

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
        "skip_conv_check",
        "If ``True``, silently truncate data; otherwise return warnings when \
         truncation is performed.",
    );

    let exc = PyException::new_data_loss();

    let doc =
        DocString::new_method("Coerce all values in DATA to fit within types specified in layout.")
            .para("This will always create a new copy of DATA in-place.")
            .arg(p)
            .returns(DocReturn::new(PyTuple::default()).exc([exc]));

    let fun_arg = doc.fun_args();
    let inner_arg = doc.idents();

    quote! {
        #[pymethods]
        impl #i {
            #doc
            fn truncate_data(&mut self, #fun_arg) -> PyResult<()> {
                self.0.truncate_data(#inner_arg).py_resolve_warnings()
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

    let param_type_set_layout = DocArg::new_param("layout", layout.pytype, "The new layout.");

    let s = if is_dataset {
        " and both must match number of columns in existing dataframe"
    } else {
        ""
    };
    let length_para =
        format!("Length of ``measurements`` must match number of columns in ``layout`` {s}.");

    let named_doc = DocString::new_method("Set all measurements, names, and layout at once.")
        .para(length_para.clone())
        .arg(DocArg::new_set_meas_param(version))
        .arg(param_type_set_layout.clone())
        .arg(DocArg::new_allow_shared_names_param())
        .arg(DocArg::new_skip_index_check_param());

    let unnamed_doc = DocString::new_method("Set all measurements and layout at once.")
        .para(length_para)
        .arg(DocArg::new_measurements_param(version))
        .arg(param_type_set_layout);

    let named_fun_args = named_doc.fun_args();
    let unnamed_fun_args = unnamed_doc.fun_args();

    quote! {
        #[pymethods]
        impl #i {
            #named_doc
            fn set_named_measurements_and_layout(&mut self, #named_fun_args) -> PyResult<()> {
                let ret = self.0
                    .set_named_measurements_and_layout(
                        measurements.into(),
                        layout.into(),
                        allow_shared_names,
                        skip_index_check,
                    )?;
                Ok(ret)
            }

            #unnamed_doc
            fn set_measurements_and_layout(&mut self, #unnamed_fun_args) -> PyResult<()> {
                let ms = measurements.into_iter().map(|m| m.bimap_into_once()).collect();
                let ret = self.0.set_measurements_and_layout(ms, layout.into())?;
                Ok(ret)
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_coredataset_set_named_measurements_and_data(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let version = split_ident_version_checked("PyCoreDataset", &i);

    let param_type_set_df =
        DocArg::new_param("data", PyClass::new_dataframe(false), "The new data.");
    let len_para = "Length of ``measurements`` must match number of columns in ``data``.";

    let named_doc = DocString::new_method("Set measurements, names, and data at once.")
        .para(len_para)
        .arg(DocArg::new_set_meas_param(version))
        .arg(param_type_set_df.clone())
        .arg(DocArg::new_allow_shared_names_param())
        .arg(DocArg::new_skip_index_check_param());

    let unnamed_doc = DocString::new_method("Set measurements and data at once.")
        .para(len_para)
        .arg(DocArg::new_measurements_param(version))
        .arg(param_type_set_df);

    let named_fun_args = named_doc.fun_args();
    let unnamed_fun_args = unnamed_doc.fun_args();

    quote! {
        #[pymethods]
        impl #i {
            #named_doc
            fn set_named_measurements_and_data(&mut self, #named_fun_args) -> PyResult<()> {
                let ret = self.0
                    .set_named_measurements_and_data(
                        measurements.into(),
                        data,
                        allow_shared_names,
                        skip_index_check,
                    )?;
                Ok(ret)
            }

            #unnamed_doc
            fn set_measurements_and_data(&mut self, #unnamed_fun_args) -> PyResult<()> {
                let ms = measurements.into_iter().map(|m| m.bimap_into_once()).collect();
                let ret = self.0.set_measurements_and_data(ms, data)?;
                Ok(ret)
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_coredataset_set_measurements_layout_and_data(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let version = split_ident_version_checked("PyCoreDataset", &i);

    let layout = DocArg::new_layout_ivar(version);

    let param_type_set_layout = DocArg::new_param("layout", layout.pytype, "The new layout.");

    let param_type_set_df =
        DocArg::new_param("data", PyClass::new_dataframe(false), "The new data.");
    let len_para =
        "Length of ``measurements`` and ``layout`` must match number of columns in ``data``.";

    let doc = DocString::new_method("Set measurements, layout, and data at once.")
        .para(len_para)
        .arg(DocArg::new_measurements_param(version))
        .arg(param_type_set_layout)
        .arg(param_type_set_df);

    let fun_args = doc.fun_args();

    quote! {
        #[pymethods]
        impl #i {
            #doc
            fn set_measurements_layout_and_data(&mut self, #fun_args) -> PyResult<()> {
                let ms = measurements.into_iter().map(|m| m.bimap_into_once()).collect();
                let ret = self.0.set_measurements_layout_and_data(ms, layout.into(), data)?;
                Ok(ret)
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_coretext_to_dataset(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let version = split_ident_version_checked("PyCoreTEXT", &i);

    let doc = DocString::new_method("Convert to a dataset object.")
        .para(
            "This will fully represent an FCS file, as opposed to \
             just representing *HEADER* and *TEXT*.",
        )
        .arg(DocArg::new_data_param(false))
        .arg(DocArg::new_analysis_param(true))
        .arg(DocArg::new_others_param(true))
        .returns(DocReturn::new(PyClass::new_coredataset(version)));

    let fun_args = doc.fun_args();
    let inner_args = doc.idents();
    let ret_path = doc.ret_path();

    quote! {
        #[pymethods]
        impl #i {
            #doc
            fn to_dataset(&self, #fun_args) -> PyResult<#ret_path> {
                Ok(self.0.clone().into_coredataset(#inner_args)?.into())
            }
        }
    }
    .into()
}

// NOTE None of these types will trigger a compile error on mismatch because
// the are encapsulated by a newtype which doesn't have a direct mapping to a
// type in python. The only way to ensure things line up is using unit tests.
#[proc_macro]
#[allow(clippy::too_many_lines)]
pub fn impl_new_meas(input: TokenStream) -> TokenStream {
    let path: Path = syn::parse(input).unwrap();
    let name = path.segments.last().unwrap().ident.clone();
    let (base, version) = split_ident_version(&name);
    let is_temporal = match base.as_str() {
        "Temporal" => true,
        "Optical" => false,
        _ => panic!("must be either Optical or Temporal"),
    };

    let version_us_short = version.short_underscore();
    let version_short = version.short();

    let fun_ident = format_ident!("new_{version_us_short}");
    let fun = quote!(#path::#fun_ident);

    let lower_basename = base.to_lowercase();

    let scale = if version == Version::FCS2_0 {
        DocArg::new_scale_ivar()
    } else {
        DocArg::new_transform_ivar()
    };

    let wavelength = if version < Version::FCS3_1 {
        DocArg::new_meas_kw_opt_ivar("Wavelength", "wavelength", "L", |p| {
            PyFloat::new_positive_float().rstype(p)
        })
    } else {
        DocArg::new_meas_kw_ivar1("Wavelengths", "wavelengths", "L", |p| {
            PyList::new(PyFloat::new_positive_float(), p, None)
        })
    };

    let bin = DocArg::new_meas_kw_ivar(
        "PeakBin",
        "bin",
        |p| PyOpt::new(PyInt::new_u32().rstype(p)),
        "Value of *$PKn*.".into(),
        true,
    );
    let size = DocArg::new_meas_kw_ivar(
        "PeakIndex",
        "size",
        |p| PyOpt::new(PyInt::new_u32().rstype(p)),
        "Value of *$PKNn*.".into(),
        true,
    );

    let all_peak = [bin, size];

    let filter = DocArg::new_meas_kw_str("Filter", "filter", "F");

    let py_float = |p| PyFloat::new_non_negative_float().rstype(p);

    let power = DocArg::new_meas_kw_opt_ivar("Power", "power", "O", py_float);

    let detector_type = DocArg::new_meas_kw_str("DetectorType", "detector_type", "T");

    let percent_emitted =
        DocArg::new_meas_kw_opt_ivar("PercentEmitted", "percent_emitted", "P", py_float);

    let detector_voltage =
        DocArg::new_meas_kw_opt_ivar("DetectorVoltage", "detector_voltage", "V", py_float);

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
        |_| PyOpt::new(PyTuple::new_calibration3_1()),
        Some("Value of *$PnCALIBRATION*. Tuple encodes slope and calibration units."),
        true,
    );

    let calibration3_2 = DocArg::new_meas_kw_ivar(
        "Calibration3_2",
        "calibration",
        |_| PyOpt::new(PyTuple::new_calibration3_2()),
        Some(
            "Value of *$PnCALIBRATION*. Tuple encodes slope, intercept, \
             and calibration units.",
        ),
        true,
    );

    let display = DocArg::new_meas_kw_ivar(
        "Display",
        "display",
        |_| PyOpt::new(PyTuple::new_display()),
        Some(
            "Value of *$PnD*. First member of tuple encodes linear or log display \
             (``False`` and ``True`` respectively). The float members encode \
             lower/upper and decades/offset for linear and log scaling respectively.",
        ),
        true,
    );

    let analyte = DocArg::new_meas_kw_str("Analyte", "analyte", "ANALYTE");

    let feature =
        DocArg::new_meas_kw_opt_ivar("Feature", "feature", "FEATURE", |_| PyStr::new_feature());

    let detector_name = DocArg::new_meas_kw_str("DetectorName", "detector_name", "DET");

    let tag = DocArg::new_meas_kw_str("Tag", "tag", "TAG");

    let measurement_type = DocArg::new_meas_kw_str("OpticalType", "measurement_type", "TYPE");

    let has_type = DocArg::new_meas_kw_ivar1("TemporalType", "has_type", "TYPE", |p| {
        PyBool::default().rstype(p)
    });

    let has_scale = DocArg::new_meas_kw_ivar1("TemporalScale2_0", "has_scale", "E", |p| {
        PyBool::default().rstype(p)
    });

    let timestep = DocArg::new_ivar_rw(
        "timestep",
        PyFloat::new_timestep(),
        "Value of *$TIMESTEP*.",
        false,
        |_, _| quote!(self.0.specific.timestep),
        |_, _| quote!(self.0.specific.timestep = timestep),
    );

    let longname = DocArg::new_meas_kw_str("Longname", "longname", "S");

    let nonstd = DocArg::new_meas_nonstandard_keywords_ivar();

    let all_common = [longname, nonstd];

    let all_args: Vec<_> = match (version, is_temporal) {
        (Version::FCS2_0, true) => once(has_scale).chain(all_peak).chain(all_common).collect(),
        (Version::FCS3_0, true) => once(timestep).chain(all_peak).chain(all_common).collect(),
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
        (Version::FCS2_0 | Version::FCS3_0, false) => [scale, wavelength]
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

    let s = format!("FCS {version_short} *$Pn\\** keywords for {lower_basename} measurement.");
    let doc = DocString::new_class(s).args(all_args);

    let inner_args = doc.idents_into();

    let new_method = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #fun(#inner_args).into()
            }
        }
    };

    doc.into_impl_class(name, &path, new_method).1.into()
}

#[proc_macro]
pub fn impl_core_all_pns(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_meas_attr1(
        &i,
        "Longname",
        "longnames",
        "S",
        |p| PyStr::default().rstype(p),
        false,
        false,
    )
}

#[proc_macro]
pub fn impl_core_all_pnf(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_meas_attr1(
        &i,
        "Filter",
        "filters",
        "F",
        |p| PyStr::default().rstype(p),
        false,
        true,
    )
}

#[proc_macro]
pub fn impl_core_all_pno(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_optical_attr(&i, "Power", "powers", "O", |p| {
        PyFloat::new_non_negative_float().rstype(p)
    })
}

#[proc_macro]
pub fn impl_core_all_pnp(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_optical_attr(&i, "PercentEmitted", "percents_emitted", "P", |p| {
        PyFloat::new_non_negative_float().rstype(p)
    })
}

#[proc_macro]
pub fn impl_core_all_pnt(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_meas_attr1(
        &i,
        "DetectorType",
        "detector_types",
        "T",
        |p| PyStr::default().rstype(p),
        false,
        true,
    )
}

#[proc_macro]
pub fn impl_core_all_pnv(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_optical_attr(&i, "DetectorVoltage", "detector_voltages", "V", |p| {
        PyFloat::new_non_negative_float().rstype(p)
    })
}

#[proc_macro]
pub fn impl_core_all_pnl_old(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_optical_attr(&i, "Wavelength", "wavelengths", "L", |p| {
        PyFloat::new_positive_float().rstype(p)
    })
}

#[proc_macro]
pub fn impl_core_all_pnl_new(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_meas_attr1(
        &i,
        "Wavelengths",
        "wavelengths",
        "L",
        |p| PyList::new(PyFloat::new_non_negative_float(), p, None),
        false,
        true,
    )
}

#[proc_macro]
pub fn impl_core_all_pnd(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_meas_attr(&i, "Display", "displays", "D", |_| PyTuple::new_display())
}

#[proc_macro]
pub fn impl_core_all_pndet(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_meas_attr1(
        &i,
        "DetectorName",
        "detector_names",
        "DET",
        |p| PyStr::default().rstype(p),
        false,
        true,
    )
}

#[proc_macro]
pub fn impl_core_all_pncal3_1(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_optical_attr(&i, "Calibration3_1", "calibrations", "CALIBRATION", |_| {
        PyTuple::new_calibration3_1()
    })
}

#[proc_macro]
pub fn impl_core_all_pncal3_2(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_optical_attr(&i, "Calibration3_2", "calibrations", "CALIBRATION", |_| {
        PyTuple::new_calibration3_2()
    })
}

#[proc_macro]
pub fn impl_core_all_pntag(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_meas_attr1(
        &i,
        "Tag",
        "tags",
        "TAG",
        |p| PyStr::default().rstype(p),
        false,
        true,
    )
}

#[proc_macro]
pub fn impl_core_all_pntype(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();

    let opt_pytype = PyStr::default().rstype(keyword_path("OpticalType"));
    let tmp_pytype = PyBool::default().rstype(keyword_path("TemporalType"));

    let inner_opt_rstype = opt_pytype.as_rust_type();
    let inner_tmp_rstype = tmp_pytype.as_rust_type();

    let doc_summary = "Value of *$PnTYPE* for all measurements.";
    let doc_middle = "A bool will be returned for the time measurement where \
                      ``True`` indicates it is set to ``\"Time\"``.";

    let nce_path =
        parse_quote!(fireflow_core::text::named_vec::Element<#inner_tmp_rstype, #inner_opt_rstype>);

    // TODO exception if time channel in the wrong spot
    let full_pytype = PyUnion::new2(opt_pytype, tmp_pytype, nce_path);

    let doc = DocString::new_ivar(doc_summary, PyList::new1(full_pytype)).para(doc_middle);

    doc.into_impl_get_set(
        &i,
        "all_measurement_types",
        true,
        |_, _| {
            quote! {
                self.0
                    .get_temporal_optical::<#inner_tmp_rstype, #inner_opt_rstype>()
                    .map(|e| e.bimap_once(|x| x.clone(), |y| y.clone()))
                    .collect()
            }
        },
        |n, _| quote!(Ok(self.0.set_temporal_optical2(#n)?)),
    )
    .into()
}

#[proc_macro]
pub fn impl_core_all_awh_pnfeature(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();

    let inner_pytype = PyOpt::new(PyLiteral::new_awh_feature());

    let inner_rstype = inner_pytype.as_rust_type();

    let doc_summary = "Value of *$PnFEATURE* (area/width/height) for all measurements.";
    let p0 = "This should be the preferred way to get and set this keyword if \
              one knows that only ``\"Area\"``, ``\"Width\"``, and ``\"Height\"`` \
              will be used for this dataset since it has a well-defined type.";
    let p1 = "``()`` will be returned for the time measurement.";

    let nce_path = parse_quote!(fireflow_core::text::named_vec::NonCenterElement<#inner_rstype>);

    // TODO exception if time channel is in the wrong spot
    let full_pytype = PyUnion::new2(inner_pytype, PyTuple::default(), nce_path);

    let doc = DocString::new_ivar(doc_summary, PyList::new1(full_pytype)).paras([p0, p1]);

    doc.into_impl_get_set(
        &i,
        "all_awh_features",
        true,
        |_, _| quote!(self.0.awh_features().collect()),
        |n, _| quote!(Ok(self.0.set_awh_features(#n)?)),
    )
    .into()
}

#[proc_macro]
pub fn impl_core_get_all_other_pnfeature(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();

    let inner_pytype = PyOpt::new(PyStr::default());
    let inner_rstype = inner_pytype.as_rust_type();

    let doc_summary = "Value of *$PnFEATURE* (not area/width/height) for all measurements.";
    let p0 = "Values which are not ``\"Area\"``, ``\"Width\"``, and ``\"Height\"`` \
              will be returned as ``None``.";
    let p1 = "``()`` will be returned for the time measurement.";

    let nce_path = parse_quote!(fireflow_core::text::named_vec::NonCenterElement<#inner_rstype>);

    let full_pytype = PyUnion::new2(inner_pytype, PyTuple::default(), nce_path);

    let doc = DocString::new_ivar(doc_summary, PyList::new1(full_pytype)).paras([p0, p1]);

    doc.into_impl_get(&i, "all_other_features", |_, _| {
        quote!(
            self.0
                .other_features()
                .map(|x| x.fmap_once(|y| y.fmap_once(|z| z.to_owned())))
                .collect()
        )
    })
    .into()
}

#[proc_macro]
pub fn impl_core_all_pnfeature(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_optical_attr(&i, "Feature", "features", "FEATURE", |_| {
        PyStr::new_feature()
    })
}

#[proc_macro]
pub fn impl_core_all_pnanalyte(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_meas_attr1(
        &i,
        "Analyte",
        "analytes",
        "ANALYTE",
        |p| PyStr::default().rstype(p),
        false,
        true,
    )
}

#[proc_macro]
pub fn impl_meas_awh_pnfeature(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();

    let pytype = PyOpt::new(PyLiteral::new_awh_feature());

    let doc_summary = "Value of *$PnFEATURE* (area/width/height).";
    let p = "This should be the preferred way to get and set this keyword if \
             one knows that only ``\"Area\"``, ``\"Width\"``, and ``\"Height\"`` \
             will be used since it has a well-defined type.";

    let doc = DocString::new_ivar(doc_summary, pytype).para(p);

    doc.into_impl_get_set(
        &i,
        "awh_feature",
        false,
        |_, _| quote!(self.0.awh_feature()),
        |n, _| quote!(self.0.set_awh_feature(#n)),
    )
    .into()
}

fn core_all_optical_attr<F, T>(t: &Ident, kw: &str, name: &str, suffix: &str, f: F) -> TokenStream
where
    F: FnOnce(Path) -> T,
    T: Into<ArgPyType>,
{
    core_all_meas_attr1(t, kw, name, suffix, f, true, true)
}

fn core_all_meas_attr<F, T>(t: &Ident, kw: &str, name: &str, suffix: &str, f: F) -> TokenStream
where
    F: FnOnce(Path) -> T,
    T: Into<ArgPyType>,
{
    core_all_meas_attr1(t, kw, name, suffix, f, true, false)
}

fn core_all_meas_attr1<F, T>(
    t: &Ident,
    kw: &str,
    name: &str,
    suffix: &str,
    f: F,
    is_optional: bool,
    optical_only: bool,
) -> TokenStream
where
    F: FnOnce(Path) -> T,
    T: Into<ArgPyType>,
{
    let kw_doc = format!("*$Pn{suffix}*");
    let base_pytype: ArgPyType = f(keyword_path(kw)).into();

    let doc_summary = format!("Value of {kw_doc} for all measurements.");
    let doc_middle = optical_only.then_some(format!(
        "``()`` will be returned for time since {kw_doc} is not \
         defined for temporal measurements."
    ));

    let inner_pytype = PyOpt::wrap_if(base_pytype, is_optional);

    let inner_rstype = inner_pytype.as_rust_type();

    let nce_path = parse_quote!(fireflow_core::text::named_vec::NonCenterElement<#inner_rstype>);

    // TODO exception if time channel is in the wrong spot
    let full_pytype = if optical_only {
        PyUnion::new2(inner_pytype, PyTuple::default(), nce_path).into()
    } else {
        inner_pytype
    };

    let doc = DocString::new_ivar(doc_summary, PyList::new1(full_pytype)).paras(doc_middle);

    let get_optical_body = if is_optional {
        quote! {
            self.0
                .optical_opt()
                .map(|e| e.0.second_once(|x| x.cloned()).into())
                .collect()
        }
    } else {
        quote! {
            self.0
                .optical::<#inner_rstype>()
                .map(|e| e.0.second_once(|x| x.clone()).into())
                .collect()
        }
    };

    let get_body = if is_optional {
        quote!(self.0.meas_opt().map(|x| x.cloned()).collect())
    } else {
        quote!(self.0.meas::<#inner_rstype>().cloned().collect())
    };

    doc.into_impl_get_set(
        t,
        format!("all_{name}"),
        true,
        |_, _| {
            if optical_only {
                get_optical_body
            } else {
                get_body
            }
        },
        |n, _| {
            if optical_only {
                quote!(Ok(self.0.set_optical(#n)?))
            } else {
                quote!(Ok(self.0.set_meas(#n)?))
            }
        },
    )
    .into()
}

#[proc_macro]
pub fn impl_core_to_version_x_y(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let (is_dataset, version) = split_ident_version_pycore(&i);
    let param_desc = "If ``False``, do not proceed with conversion if it would \
                      result in data loss. This is most likely to happen when \
                      converting from a later to an earlier version, as many \
                      keywords from the later version may not exist in the \
                      earlier version. There is no place to keep these values so \
                      they must be discarded. Set to ``True`` to perform the \
                      conversion with such discarding; otherwise, remove the \
                      keywords manually before converting.";
    let outputs: Vec<_> = ALL_VERSIONS
        .iter()
        .filter(|&&v| v != version)
        .map(|&v| {
            let vsu = v.short_underscore();
            let vs = v.short();
            let fn_name = format_ident!("to_version_{vsu}");
            let target_type = if is_dataset {
                PyClass::new_coredataset(v)
            } else {
                PyClass::new_coretext(v)
            };
            let exc0 = PyException::new_pyreflow(&PyreflowError::Conversion).desc(format!(
                "If keywords which are unsupported in FCS {vs} exist in current \
                 data and ``allow_loss`` is ``False``"
            ));
            let exc1 = PyException::new_pyreflow(&PyreflowError::Conversion).desc(format!(
                "If optional keywords are that are missing in current \
                 version are required in FCS {vs}"
            ));
            let target_pytype = target_type.as_rust_type();
            let param = DocArg::new_bool_param("allow_loss", param_desc);
            let doc = DocString::new_method(format!("Convert to FCS {vs}."))
                .arg(param)
                .returns(
                    DocReturn::new(target_type)
                        .desc(format!("A new class conforming to FCS {vs}."))
                        .exc([exc0, exc1]),
                );
            quote! {
                #doc
                fn #fn_name(&self, allow_loss: bool) -> PyResult<#target_pytype> {
                    self.0.clone().try_convert(allow_loss).py_resolve_commutative().map(Into::into)
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

    let scale = DocArg::new_opt_ivar_rw(
        "scale",
        PyUnion::new_scale(true),
        "The *$GmE* keyword. ``()`` means linear scaling and 2-tuple \
         specifies decades and offset for log scaling.",
        false,
        |n, _| quote!(self.0.#n.as_ref().cloned()),
        |n, _| quote!(self.0.#n = #n.into()),
    );

    let make_arg_str = |kw_name: &str, kw_sym: &str, t: &str| {
        let kw_path = keyword_path(t);
        DocArg::new_ivar_rw(
            kw_name,
            PyStr::default().rstype(kw_path),
            format!("The *$Gm{kw_sym}* keyword."),
            false,
            |n, _| quote!(self.0.#n.clone()),
            |n, _| quote!(self.0.#n = #n),
        )
        .def_auto()
    };

    let filter = make_arg_str("filter", "F", "GateFilter");
    let longname = make_arg_str("longname", "S", "GateLongname");
    let detector_type = make_arg_str("detector_type", "T", "GateDetectorType");

    let make_arg_opt = |kw_name: &str, kw_sym: &str, pytype: ArgPyType| {
        DocArg::new_opt_ivar_rw(
            kw_name,
            pytype,
            format!("The *$Gm{kw_sym}* keyword."),
            false,
            |n, _| quote!(self.0.#n.as_ref().cloned()),
            |n, _| quote!(self.0.#n = #n),
        )
    };

    let make_arg_float = |kw_name: &str, kw_sym: &str, t: &str| {
        let kw_path = keyword_path(t);
        let pytype = PyFloat::new_non_negative_float().rstype(kw_path);
        DocArg::new_opt_ivar_rw(
            kw_name,
            pytype,
            format!("The *$Gm{kw_sym}* keyword."),
            false,
            |n, _| quote!(self.0.#n.as_ref().cloned()),
            |n, _| quote!(self.0.#n = #n),
        )
    };

    let percent_emitted = make_arg_float("percent_emitted", "P", "GatePercentEmitted");
    let detector_voltage = make_arg_float("detector_voltage", "V", "GateDetectorVoltage");

    let shortname_pytype = PyStr::new_shortname().rstype(keyword_path("GateShortname"));
    let shortname = make_arg_opt("shortname", "N", shortname_pytype.into());

    let range_pytype = PyDecimal::new_range().rstype(keyword_path("GateRange"));
    let range = make_arg_opt("range", "R", range_pytype.into());

    let summary = "The *$Gm\\** keywords for one gated measurement.";
    let doc = DocString::new_class(summary)
        .arg(scale)
        .arg(filter)
        .arg(shortname)
        .arg(percent_emitted)
        .arg(range)
        .arg(longname)
        .arg(detector_type)
        .arg(detector_voltage);

    let inner_args = doc.idents_into();

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #path::new(#inner_args).into()
            }
        }
    };

    doc.into_impl_class(name, &path, new).1.into()
}

#[proc_macro]
pub fn impl_new_fixed_ascii_layout(input: TokenStream) -> TokenStream {
    let path: Path = syn::parse(input).unwrap();
    let name = path.segments.last().unwrap().ident.clone();
    let bare_path = path_strip_args(path.clone());

    let chars_param = DocArg::new_ivar_ro(
        "ranges",
        PyList::new1(PyInt::new_ascii_range_value()),
        "The range for each measurement. Equivalent to *$PnR*. The value of \
         *$PnB* will be derived from these and will be equivalent to the number \
         of digits for each value.",
        |_, _| quote!(self.0.columns().iter().map(|c| c.value()).collect()),
    );

    let doc = DocString::new_class("A fixed-width ASCII layout.").arg(chars_param);

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #bare_path::new_ascii_u64(ranges).into()
            }
        }
    };

    let (pyname, class) = doc.into_impl_class(name, &path, new);

    let char_widths_doc =
        DocString::new_ivar("The width of each measurement.", PyList::new1(RsInt::U64)).para(
            "Equivalent to *$PnB*, which is the number of chars/digits used \
             to encode data for a given measurement.",
        );

    let char_widths = char_widths_doc.into_impl_get(&pyname, "char_widths", |_, _| {
        quote!(self.0.widths().fmap(|x| u64::from(u8::from(x))))
    });

    let datatype = make_layout_datatype(&pyname, "A");

    quote! {
        #class
        #char_widths
        #datatype
    }
    .into()
}

#[proc_macro]
pub fn impl_new_delim_ascii_layout(input: TokenStream) -> TokenStream {
    let path: Path = syn::parse(input).unwrap();
    let name = path.segments.last().unwrap().ident.clone();
    let bare_path = path_strip_args(path.clone());

    let ranges_param = DocArg::new_ivar_ro(
        "ranges",
        PyList::new1(PyInt::new_ascii_range_value()),
        "The range for each measurement. Equivalent to the *$PnR* keyword. \
         This is not used internally.",
        |_, _| quote!(self.0.as_ref().to_vec()),
    );

    let doc = DocString::new_class("A delimited ASCII layout.").arg(ranges_param);

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #bare_path::new(ranges).into()
            }
        }
    };

    let (pyname, class) = doc.into_impl_class(name, &path, new);
    let datatype = make_layout_datatype(&pyname, "A");
    quote!(#class #datatype).into()
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
            PyFloat::new_float_range(nbytes).into(),
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
            PyType::from(PyInt::new_bitmask(nbytes)),
            range_desc,
            "integer",
            "Uint",
            quote!(fireflow_core::validated::bitmask::#bitmask),
            "I",
        )
    };
    let tot_path = keyword_path("Tot");
    let known_tot_path = quote!(fireflow_core::text::optional::Identity<#tot_path>);
    let ordered_layout_path = quote!(fireflow_core::data::OrderedLayout);
    let fixed_layout_path = quote!(fireflow_core::data::FixedLayout);
    let sizedbyteord_path: Path = parse_quote!(fireflow_core::text::byteord::SizedByteOrd);

    let full_layout_path: Path = parse_quote!(#ordered_layout_path<#range_path, #known_tot_path>);

    let layout_name = format!("Ordered{base}{nbits:02}Layout");

    let summary = format!("{nbits}-bit ordered {what} layout.");

    let range_param =
        DocArg::new_ivar_ro("ranges", PyList::new1(range_pytype), range_desc, |_, _| {
            quote!(self.0.columns().iter().map(|c| c.clone()).collect())
        });

    let byteord_param = DocArg::new_ivar_ro(
        "byteord",
        PyUnion::new_byteord(nbytes),
        "The byte order to use when encoding values.",
        |_, _| quote!(*self.0.as_ref()),
    )
    .def_auto();

    let is_big_param = DocArgROIvar::new_endian_ord_param(2);

    let make_doc = |args| DocString::new_class(summary).args(args);

    // make different constructors and getters for u8 and u16 since the byteord
    // for these can be simplified
    let (pyname, class) = match (is_float, nbytes) {
        // u8 doesn't need byteord since only one is possible
        (false, 1) => {
            let doc = make_doc(vec![range_param]);
            let new = |fun_args| {
                quote! {
                    fn new(#fun_args) -> Self {
                        #fixed_layout_path::new(ranges, #sizedbyteord_path::default()).into()
                    }
                }
            };
            doc.into_impl_class(layout_name, &full_layout_path, new)
        }

        // u16 only has two combinations (big and little) so don't allow a list
        // for byteord
        (false, 2) => {
            let doc = make_doc(vec![range_param, is_big_param]);
            let new = |fun_args| {
                quote! {
                    fn new(#fun_args) -> Self {
                        let b = #sizedbyteord_path::Endian(endian);
                        #fixed_layout_path::new(ranges, b).into()
                    }
                }
            };
            doc.into_impl_class(layout_name, &full_layout_path, new)
        }

        // everything else needs the "full" version of byteord, which is big,
        // little, and mixed (a list)
        _ => {
            let doc = make_doc(vec![range_param, byteord_param]);
            let new = |fun_args| {
                quote! {
                    fn new(#fun_args) -> Self {
                        #fixed_layout_path::new(ranges, byteord).into()
                    }
                }
            };
            doc.into_impl_class(layout_name, &full_layout_path, new)
        }
    };

    let widths = make_byte_width(&pyname, nbytes);
    let datatype = make_layout_datatype(&pyname, dt);
    quote!(#class #widths #datatype).into()
}

#[proc_macro]
pub fn impl_new_endian_float_layout(input: TokenStream) -> TokenStream {
    let nbytes = parse_macro_input!(input as LitInt)
        .base10_parse::<usize>()
        .expect("Must be an integer");
    let nbits = nbytes * 8;
    let range = format_ident!("F{:02}Range", nbits);
    let range_path: Path = parse_quote!(fireflow_core::data::#range);

    let numtype_path = keyword_path("NumType");
    let nomeasdt_path = quote!(fireflow_core::text::optional::Nothing<#numtype_path>);
    let endian_layout_path = quote!(fireflow_core::data::EndianLayout);
    let fixed_layout_path = quote!(fireflow_core::data::FixedLayout);

    let full_layout_path = parse_quote!(#endian_layout_path<#range_path, #nomeasdt_path>);

    let layout_name = format!("EndianF{nbits:02}Layout");

    let range_param = DocArg::new_ivar_ro(
        "ranges",
        PyList::new1(PyFloat::new_float_range(nbytes)),
        "The range for each measurement. Corresponds to *$PnR*. This is not \
         used internally.",
        |_, _| quote!(self.0.columns().iter().map(|c| c.clone()).collect()),
    );

    let is_big_param = DocArgROIvar::new_endian_param(4);

    let doc = DocString::new_class(format!("{nbits}-bit endian float layout"))
        .args([range_param, is_big_param]);

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #fixed_layout_path::new(ranges, endian).into()
            }
        }
    };

    let (pyname, class) = doc.into_impl_class(layout_name, &full_layout_path, new);

    let widths = make_byte_width(&pyname, nbytes);
    let datatype = make_layout_datatype(&pyname, if nbytes == 4 { "F" } else { "D" });

    quote!(#class #widths #datatype).into()
}

#[proc_macro]
pub fn impl_new_endian_uint_layout(_: TokenStream) -> TokenStream {
    let name = format_ident!("EndianUintLayout");

    let fixed = quote!(fireflow_core::data::FixedLayout);
    let bitmask = quote!(fireflow_core::data::AnyNullBitmask);
    let numtype_path = keyword_path("NumType");
    let nomeasdt = quote!(fireflow_core::text::optional::Nothing<#numtype_path>);
    let endian_layout = quote!(fireflow_core::data::EndianLayout);
    let layout_path = parse_quote!(#endian_layout<#bitmask, #nomeasdt>);

    let ranges_param: DocArgROIvar = DocArg::new_ivar_ro(
        "ranges",
        PyList::new1(PyInt::new_bitmask_value64()),
        "The range of each measurement. Corresponds to the *$PnR* \
         keyword less one. The number of bytes used to encode each \
         measurement (*$PnB*) will be the minimum required to express this \
         value. For instance, a value of ``1023`` will set *$PnB* to ``16``, \
         will set *$PnR* to ``1024``, and encode values for this measurement as \
         16-bit integers. The values of a measurement will be less than or \
         equal to this value.",
        |_, _| quote!(self.0.columns().iter().map(|c| (*c).into()).collect()),
    );

    let is_big_param = DocArgROIvar::new_endian_param(4);

    let doc =
        DocString::new_class("A mixed-width integer layout.").args([ranges_param, is_big_param]);

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                let rs = ranges.fmap(#bitmask::from);
                #fixed::new(rs, endian).into()
            }
        }
    };

    let (pyname, class) = doc.into_impl_class(name, &layout_path, new);
    let datatype = make_layout_datatype(&pyname, "I");
    quote!(#class #datatype).into()
}

#[proc_macro]
pub fn impl_new_mixed_layout(_: TokenStream) -> TokenStream {
    let name = format_ident!("MixedLayout");
    let layout_path = parse_quote!(fireflow_core::data::#name);

    let null = quote!(fireflow_core::data::NullMixedType);
    let fixed = quote!(fireflow_core::data::FixedLayout);

    let desc = "if field 2 of %x is less than ``0`` or greater than ``2**64-1`` \
                when field 1 is ``\"A\"`` or ``\"I\"``";
    let exc = PyException::new_invalid_keyword().desc(desc);
    let range_pytype = PyList::new1(PyUnion::new2(
        PyTuple::new1(PyLiteral::new1(["A", "I"])).add_new(RsInt::U64),
        PyTuple::new1(PyLiteral::new1(["F", "D"])).add_new(PyDecimal::default()),
        parse_quote!(#null),
    ))
    .exc(exc);
    let types_param: DocArgROIvar = DocArg::new_ivar_ro(
        "typed_ranges",
        range_pytype,
        "The type and range for each measurement corresponding to *$DATATYPE* \
         and/or *$PnDATATYPE* and *$PnR* respectively. These are given \
         as 2-tuples like ``(<type>, <range>)`` where ``type`` is one of \
         ``\"A\"``, ``\"I\"``, ``\"F\"``, or ``\"D\"`` corresponding to Ascii, \
         Integer, Float, or Double datatypes respectively.",
        |_, _| quote!(self.0.columns().iter().map(|c| c.clone()).collect()),
    );

    let is_big_param = DocArgROIvar::new_endian_param(4);

    let doc = DocString::new_class("A mixed-type layout.").args([types_param, is_big_param]);

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #fixed::new(typed_ranges, endian).into()
            }
        }
    };

    doc.into_impl_class(name, &layout_path, new).1.into()
}

#[proc_macro]
pub fn impl_layout_byte_widths(input: TokenStream) -> TokenStream {
    let t = parse_macro_input!(input as Ident);

    let doc = DocString::new_ivar(
        "The width of each measurement in bytes.",
        PyList::new1(RsInt::U32),
    )
    .para(
        "This corresponds to the value of *$PnB* for each measurement \
         divided by 8. Values for each measurement may be different.",
    );

    doc.into_impl_get(&t, "byte_widths", |_, _| {
        quote!(self.0.widths().fmap(|x| u32::from(u8::from(x)) / 8))
    })
    .into()
}

#[proc_macro]
pub fn impl_new_gate_uni_regions(input: TokenStream) -> TokenStream {
    let path: Path = syn::parse(input).unwrap();
    make_gate_region(&path, true)
}

#[proc_macro]
pub fn impl_new_gate_bi_regions(input: TokenStream) -> TokenStream {
    let path: Path = syn::parse(input).unwrap();
    make_gate_region(&path, false)
}

// TODO doc exceptions here
fn make_gate_region(path: &Path, is_uni: bool) -> TokenStream {
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

    let (summary_version, suffix, index_desc, index_pytype_inner) = match index_rsname.as_str() {
        "GateIndex" => (
            "2.0",
            "2_0",
            format!(
                "The {index_name} corresponding to a gating measurement \
                 (the *m* in the *$Gm\\** keywords)."
            ),
            PyInt::new_gate_index().into(),
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
                PyType::from(PyStr::new_meas_or_gate_index()),
            )
        }
        "PrefixedMeasIndex" => (
            "3.2",
            "3_2",
            format!(
                "The {index_name} corresponding to a physical measurement \
                 (the *n* in the *$Pn\\** keywords)."
            ),
            PyInt::new_prefixed_meas_index().into(),
        ),
        _ => panic!("unknown index type"),
    };

    let (region_name, index_pytype, gate_argname, gate_pytype, gate_desc) = if is_uni {
        (
            "univariate",
            index_pytype_inner,
            "gate",
            PyType::from(PyTuple::new_unigate()),
            "The lower and upper bounds of the gate.",
        )
    } else {
        (
            "bivariate",
            PyTuple::new2(vec![index_pytype_inner; 2])
                .rstype(parse_quote!(#index_pair<#index_path_inner>))
                .into(),
            "vertices",
            PyList::new_vertices().into(),
            "The vertices of a polygon gate. Must not be empty.",
        )
    };

    let summary = format!("Make a new FCS {summary_version}-compatible {region_name} region",);

    let index_arg = DocArg::new_ivar_ro("index", index_pytype, index_desc, |_, _| {
        quote!(self.0.index)
    });
    let gate_arg = DocArg::new_ivar_ro(
        gate_argname,
        gate_pytype,
        gate_desc,
        |n, _| quote!(self.0.#n.clone()),
    );

    let doc = DocString::new_class(summary).args([index_arg, gate_arg]);

    let name = format!("{region_ident}{suffix}");
    let bare_path = path_strip_args(path.clone());
    let inner_args: Vec<_> = doc.args.iter().map(IsDocArg::record_into).collect();

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #bare_path { #(#inner_args),* }.into()
            }
        }
    };

    doc.into_impl_class(name, path, new).1.into()
}

/// Macro args for implementing read functions for both position and multiple datasets
struct ReadPaths2 {
    path0: Path,
    path1: Path,
}

impl Parse for ReadPaths2 {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let path0 = input.parse::<Path>()?;
        let _: Comma = input.parse()?;
        let path1 = input.parse::<Path>()?;
        Ok(Self { path0, path1 })
    }
}

/// Macro args to parse 3 paths
struct ReadPaths3 {
    path0: Path,
    path1: Path,
    path2: Path,
}

impl Parse for ReadPaths3 {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let path0 = input.parse::<Path>()?;
        let _: Comma = input.parse()?;
        let path1 = input.parse::<Path>()?;
        let _: Comma = input.parse()?;
        let path2 = input.parse::<Path>()?;
        Ok(Self {
            path0,
            path1,
            path2,
        })
    }
}

/// Macro args for implementing new Core* classes
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
        assert!(v0 == v1, "Versions don't match");
        Ok(Self {
            coretext_path,
            coredataset_path,
            coretext_name,
            coredataset_name,
            version: v0,
        })
    }
}

/// Macro args for implementing new ordered layout
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

fn split_ident_version(name: &Ident) -> (String, Version) {
    let n = name.to_string();
    let (ret, v) = n.split_at(n.len() - 3);
    let version = Version::from_short_underscore(v).expect("version should be like 'X_Y'");
    (ret.into(), version)
}

fn split_ident_version_checked(which: &'static str, name: &Ident) -> Version {
    let (n, v) = split_ident_version(name);
    assert!(
        n.as_str() == which,
        "identifier should be like '{which}X_Y'"
    );
    v
}

fn split_ident_version_pycore(name: &Ident) -> (bool, Version) {
    let (base, version) = split_ident_version(name);
    assert!(
        base == "PyCoreTEXT" || base == "PyCoreDataset",
        "must be PyCore(TEXT|Dataset)X_Y"
    );
    (base == "PyCoreDataset", version)
}

fn make_layout_datatype(pyname: &Ident, dt: &str) -> TokenStream2 {
    let doc = DocString::new_ivar("The value of *$DATATYPE*.", PyLiteral::new_datatype())
        .paras([format!("Will always return ``\"{dt}\"``.")]);
    doc.into_impl_get(pyname, "datatype", |_, _| quote!(self.0.datatype().into()))
}

fn make_byte_width(pyname: &Ident, nbytes: usize) -> TokenStream2 {
    let s0 = format!("Will always return ``{nbytes}``.");
    let s1 = "This corresponds to the value of *$PnB* divided by 8, which are \
              all equal for this layout."
        .into();
    let doc = DocString::new_ivar("The width of each measurement in bytes.", RsInt::Usize)
        .paras([s0, s1]);

    doc.into_impl_get(pyname, "byte_width", |_, _| quote!(#nbytes))
}

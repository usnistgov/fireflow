extern crate proc_macro;

use fireflow_types::config::{self as tc, EnumStrIter as _};
use fireflow_types::keywords as tk;
use fireflow_types::python::{
    COL_TYPE_ASCII, COL_TYPE_F32, COL_TYPE_F64, ColumnType, IntegerWidth,
};

use const_format::formatcp;
use derive_more::{AsRef, Display, From};
use derive_new::new;
use itertools::Itertools as _;
use nonempty_collections::{
    NEVec,
    iter::{IntoNonEmptyIterator as _, NonEmptyIterator as _},
};
use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{ToTokens, format_ident, quote};
use syn::{
    GenericArgument, Ident, LitInt, LitStr, Path, PathArguments, Type,
    parse::{Parse, ParseStream},
    parse_macro_input, parse_quote,
    token::Comma,
};

use std::cmp::Ordering;
use std::fmt;
use std::hash::Hash;
use std::iter::{empty, once};
use std::marker::PhantomData;
use std::string::ToString;

/// Italic RST format
macro_rules! italic {
    ($s:expr) => {
        formatcp!("*{}*", $s)
    };
}

fn italic(s: impl fmt::Display) -> String {
    format!("*{s}*")
}

/// Format for FCS keywords
macro_rules! fcs_kw {
    ($s:expr) => {
        italic!($s)
    };
}

fn fcs_kw(s: impl fmt::Display) -> String {
    italic(s)
}

/// Format for segments
macro_rules! fcs_seg {
    ($s:expr) => {
        italic!($s)
    };
}

/// Format for python code and FCS literals
macro_rules! code {
    ($s:expr) => {
        formatcp!("``{}``", $s)
    };
}

fn code(s: impl fmt::Display) -> String {
    format!("``{s}``")
}

/// Format for python arguments
macro_rules! arg {
    ($s:expr) => {
        code!($s)
    };
}

fn arg(s: impl fmt::Display) -> String {
    code(s)
}

/// Format for literal python strings (to avoid annoying quotes)
macro_rules! code_str {
    ($s:expr) => {
        code!(formatcp!("\"{}\"", $s))
    };
}

fn code_str(s: impl fmt::Display) -> String {
    code(format!("\"{s}\""))
}

#[proc_macro]
pub fn def_fcs_read_header(input: TokenStream) -> TokenStream {
    let fun_path = parse_macro_input!(input as Path);

    let conf_path = config_path("ReadHeaderConfig");

    let (header_conf, header_args, header_recs) = DocArgParam::new_read_header_config_params();
    let (offset_conf, offset_args, offset_recs) = DocArgParam::new_read_offset_config_params(None);

    let exc = PyException::new_pyreflow(PyreflowError::FileLayout)
        .desc(format!("if {HEADER} segment is unparsable"));

    let doc = DocString::new_fun(format!("Read the {HEADER} of an FCS file."))
        .arg(DocArg::new_path_param(true))
        .args(header_args)
        .args(offset_args)
        .arg(DocArg::new_dataset_offset_param())
        .returns(DocReturn::new(PyClass::new_py(["api"], "Header")).exc([exc]));

    let fun_args = doc.fun_args();
    let ret_path = doc.ret_path();

    quote! {
        #[pyfunction]
        #doc
        #[allow(clippy::too_many_arguments)]
        pub fn fcs_read_header(#fun_args) -> #ret_path {
            let header = #header_conf { #(#header_recs),* };
            let offset = #offset_conf { #(#offset_recs),* };
            let conf = #conf_path { header, offset };
            let header = #fun_path(&path, dataset_offset, &conf).py_resolve_commutative()?;
            Ok(header.into())
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
    let (offset_conf, offset_args, offset_recs) = DocArgParam::new_read_offset_config_params(None);
    let (flat_conf, flat_args, flat_recs) = DocArgParam::new_read_flat_config_params();
    let (shared_conf, shared_args, shared_recs) = DocArgParam::new_shared_config_params();
    let dataset_offset_arg = DocArg::new_dataset_offset_param();

    let skip_arg = DocArg::new_skip_param("Number of datasets to skip");
    let limit_arg = DocArg::new_limit_param("Parse up to this many datasets");

    let conf_args: Vec<_> = header_args
        .into_iter()
        .chain(offset_args)
        .chain(flat_args)
        .chain(shared_args)
        .collect();

    let exc0 = PyException::new_pyreflow(PyreflowError::FileLayout)
        .desc(format!("If {HEADER} or {TEXT} are not parsable"));
    let xs = [exc0];

    let ret_pt = PyClass::new_py(["api"], "FlatTEXTOutput");

    let one_doc = DocString::new_fun(format!(
        "Read {HEADER} and {TEXT} from first dataset in FCS file."
    ))
    .arg(path_arg.clone())
    .args(conf_args.clone())
    .arg(dataset_offset_arg)
    .returns(DocReturn::new(ret_pt.clone()).exc(xs.clone()));

    let many_doc = DocString::new_fun(format!(
        "Read {HEADER} and {TEXT} from multiple datasets in FCS file."
    ))
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
        let offset = #offset_conf { #(#offset_recs),* };
        let flat = #flat_conf { #(#flat_recs),* };
        let shared = #shared_conf { #(#shared_recs),* };
        let conf = #conf_path { header, flat, offset, shared };
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
            Ok(type_families::Functor::fmap(xs, Into::into))
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
    let (offset_conf, offset_args, offset_recs) = DocArgParam::new_read_offset_config_params(None);
    let (flat_conf, flat_args, flat_recs) = DocArgParam::new_read_flat_config_params();
    let (std_conf, std_args, std_recs) = DocArgParam::new_read_std_config_params(None);
    let (layout_conf, layout_args, layout_recs) =
        DocArgParam::new_read_data_schema_config_params(None);
    let (shared_conf, shared_args, shared_recs) = DocArgParam::new_shared_config_params();
    let dataset_offset_arg = DocArg::new_dataset_offset_param();

    let conf_args = header_args
        .into_iter()
        .chain(offset_args)
        .chain(flat_args)
        .chain(std_args)
        .chain(layout_args)
        .chain(shared_args);

    let skip_arg = DocArg::new_skip_param(format!(
        "Number of datasets to skip. The {HEADER} and {TEXT} from skipped \
         datasets will still be read to obtain {NEXTDATA} for the next \
         dataset in the file.",
    ));
    let limit_arg = DocArg::new_limit_param("Parse up to this many datasets");

    let exc0 = PyException::new_pyreflow(PyreflowError::FileLayout)
        .desc(format!("If {HEADER} or {TEXT} are unparsable"));
    let exc1 = PyException::new_extra();
    let exc2 = PyException::new_parse_keyval();
    let exc3 = PyException::new_pyreflow(PyreflowError::Relational)
        .desc("If keywords that are referenced by other keywords are missing");

    let xs = [exc0, exc1, exc2, exc3];

    let pt_ret =
        PyTuple::new1(PyUnion::new_anycoretext()).add(PyClass::new_py(["api"], "StdTEXTOutput"));

    let one_doc = DocString::new_fun(format!(
        "Read standardized {TEXT} from first dataset in FCS file."
    ))
    .arg(path_arg.clone())
    .args(conf_args.clone())
    .arg(dataset_offset_arg)
    .returns(DocReturn::new(pt_ret.clone()).exc(xs.clone()));

    let many_doc = DocString::new_fun(format!(
        "Read standardized {TEXT} from multiple datasets in FCS file."
    ))
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
        let offset = #offset_conf { #(#offset_recs),* };
        let flat = #flat_conf {  #(#flat_recs),* };
        let standard = #std_conf { #(#std_recs),* };
        let layout = #layout_conf { #(#layout_recs),* };
        let shared = #shared_conf { #(#shared_recs),* };
        let conf = #conf_path { header, flat, offset, standard, layout, shared };
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
            Ok(type_families::Functor::fmap(xs, |(c, d)| (c.into(), d.into())))
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
    let (offset_conf, offset_args, offset_recs) = DocArgParam::new_read_offset_config_params(None);
    let (flat_conf, flat_args, flat_recs) = DocArgParam::new_read_flat_config_params();
    let (layout_conf, layout_args, layout_recs) =
        DocArgParam::new_read_data_schema_config_params(None);
    let (data_conf, data_args, data_recs) = DocArgParam::new_read_events_config_params();
    let (shared_conf, shared_args, shared_recs) = DocArgParam::new_shared_config_params();
    let dataset_offset_arg = DocArg::new_dataset_offset_param();

    let skip_arg = DocArg::new_skip_param(format!(
        "Number of datasets to skip. The {HEADER} and {TEXT} from skipped \
         datasets will still be read to obtain {NEXTDATA} for the next \
         dataset in the file.",
    ));
    let limit_arg = DocArg::new_limit_param("Parse up to this many datasets");

    let conf_args = header_args
        .into_iter()
        .chain(offset_args)
        .chain(flat_args)
        .chain(layout_args)
        .chain(data_args)
        .chain(shared_args);

    let exc0 = PyException::new_pyreflow(PyreflowError::FileLayout)
        .desc(format!("If {HEADER}, {TEXT}, or {DATA} are unparsable"));
    // the only deprecated keyval that should be read here is $DATATYPE when its
    // value is A for 3.1+
    let exc1 = PyException::new_parse_keyval();
    let exc2 = PyException::new_pyreflow(PyreflowError::Relational).desc(format!(
        "If keywords are incompatible with indicated data schema for {DATA}"
    ));
    let exc3 = PyException::new_event_data();

    let xs = [exc0, exc1, exc2, exc3];

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
        let offset = #offset_conf { #(#offset_recs),* };
        let flat = #flat_conf { #(#flat_recs),* };
        let layout = #layout_conf { #(#layout_recs),* };
        let data = #data_conf { #(#data_recs),* };
        let shared = #shared_conf { #(#shared_recs),* };
        let conf = #conf_path { header, flat, offset, layout, data, shared };
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
            Ok(type_families::Functor::fmap(xs, Into::into))
        }

        #[pyfunction]
        #smry_doc
        #[allow(clippy::too_many_arguments)]
        pub fn fcs_summarize(#smry_fun_args) -> #smry_ret_path {
            #conf_q
            let xs = #fun_smry_path(&path, skip, limit, &conf).py_resolve_commutative()?;
            Ok(type_families::Functor::fmap(xs, Into::into))
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
    let (offset_conf, offset_args, offset_recs) = DocArgParam::new_read_offset_config_params(None);
    let (flat_conf, flat_args, flat_recs) = DocArgParam::new_read_flat_config_params();
    let (std_conf, std_args, std_recs) = DocArgParam::new_read_std_config_params(None);
    let (layout_conf, layout_args, layout_recs) =
        DocArgParam::new_read_data_schema_config_params(None);
    let (data_conf, data_args, data_recs) = DocArgParam::new_read_events_config_params();
    let (shared_conf, shared_args, shared_recs) = DocArgParam::new_shared_config_params();
    let dataset_offset_arg = DocArg::new_dataset_offset_param();

    let conf_args = header_args
        .into_iter()
        .chain(offset_args)
        .chain(flat_args)
        .chain(std_args)
        .chain(layout_args)
        .chain(data_args)
        .chain(shared_args);

    let skip_arg = DocArg::new_skip_param(format!(
        "Number of datasets to skip. The {HEADER} and {TEXT} from skipped \
         datasets will still be read to obtain {NEXTDATA} for the next \
         dataset in the file.",
    ));
    let limit_arg = DocArg::new_limit_param("Parse up to this many datasets");

    let exc0 = PyException::new_pyreflow(PyreflowError::FileLayout)
        .desc(format!("If {HEADER}, {TEXT}, or {DATA} are unparsable"));
    let exc1 = PyException::new_parse_keyval();
    let exc2 = PyException::new_pyreflow(PyreflowError::Relational).desc(format!(
        "If keywords are incompatible with indicated layout of {DATA} or \
         if keywords that are referenced by other keywords do not exist"
    ));
    let exc3 = PyException::new_event_data();
    let exc4 = PyException::new_extra();

    let xs = [exc0, exc1, exc2, exc3, exc4];

    let pt_ret = PyTuple::new1(PyUnion::new_anycoredataset())
        .add(PyClass::new_py(["api"], "StdDatasetOutput"));

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
        let offset = #offset_conf { #(#offset_recs),* };
        let flat = #flat_conf { #(#flat_recs),* };
        let standard = #std_conf { #(#std_recs),* };
        let layout = #layout_conf { #(#layout_recs),* };
        let data = #data_conf { #(#data_recs),* };
        let shared = #shared_conf { #(#shared_recs),* };
        let conf = #conf_path { header, flat, offset, standard, layout, data, shared };
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
            Ok(type_families::Functor::fmap(xs, |(c, d)| (c.into(), d.into())))
        }
    }
    .into()
}

#[proc_macro]
pub fn def_fcs_read_flat_dataset_with_keywords(input: TokenStream) -> TokenStream {
    let fun_path = parse_macro_input!(input as Path);

    let conf_path = config_path("ReadFlatDatasetFromKeywordsConfig");

    let path_arg = DocArg::new_path_param(true);
    let header_arg = DocArg::new_header_and_supp_param();
    let std_arg = DocArg::new_std_keywords_param();
    let dataset_offset_arg = DocArg::new_dataset_offset_param();

    let (offset_conf, offset_args, offset_recs) = DocArgParam::new_read_offset_config_params(None);
    let (layout_conf, layout_args, layout_recs) =
        DocArgParam::new_read_data_schema_config_params(None);
    let (data_conf, data_args, data_recs) = DocArgParam::new_read_events_config_params();
    let (shared_conf, shared_args, shared_recs) = DocArgParam::new_shared_config_params();

    let exc0 = PyException::new_pyreflow(PyreflowError::FileLayout)
        .desc(format!("If {DATA} is unparsable"));
    // the only deprecated keyval that should be read here is $DATATYPE when its
    // value is A for 3.1+
    let exc1 = PyException::new_parse_keyval();
    let exc2 = PyException::new_pyreflow(PyreflowError::Relational).desc(format!(
        "If keywords are incompatible with indicated data schema for {DATA}"
    ));
    let exc3 = PyException::new_event_data();

    let xs = [exc0, exc1, exc2, exc3];

    let doc = DocString::new_fun("Read dataset from FCS file from keywords in flat mode.")
        .arg(path_arg)
        .arg(header_arg)
        .arg(std_arg)
        .args(offset_args)
        .args(layout_args)
        .args(data_args)
        .args(shared_args)
        .arg(dataset_offset_arg)
        .returns(DocReturn::new(PyClass::new_py(["api"], "NewFlatDatasetFromKwsOutput")).exc(xs));

    let fun_args = doc.fun_args();
    let ret_path = doc.ret_path();

    quote! {
        #[pyfunction]
        #doc
        #[allow(clippy::too_many_arguments)]
        pub fn fcs_read_flat_dataset_with_keywords(#fun_args) -> #ret_path {
            let offset = #offset_conf { #(#offset_recs),* };
            let layout = #layout_conf { #(#layout_recs),* };
            let data = #data_conf { #(#data_recs),* };
            let shared = #shared_conf { #(#shared_recs),* };
            let conf = #conf_path { offset, layout, data, shared };
            let ret = #fun_path(
                &path, header.into(), &std, dataset_offset, &conf
            ).py_resolve_commutative()?;
            Ok(ret.into())
        }
    }
    .into()
}

#[proc_macro]
pub fn def_fcs_write_datasets(input: TokenStream) -> TokenStream {
    let fun_path = parse_macro_input!(input as Path);

    let path_arg = DocArg::new_path_param(false);
    let cores_arg = DocArg::new_param(
        "datasets",
        PyList::new1(PyUnion::new_anycoredataset()),
        "datasets to write",
    );

    let exc0 = PyException::new_segment_overflow(None);
    let exc1 = PyException::new_other_overflow();

    let xs = [exc0, exc1];

    let ret = DocReturn::new(PyOpt::new1(PyInt::new_nextdata()))
        .desc(format!(
            "the value of {NEXTDATA} as written in the last dataset if written"
        ))
        .exc(xs);

    let doc = DocString::new_fun("Write multiple datasets to path.")
        .para(format!(
            "The resulting file will include {HEADER}, {TEXT}, {DATA}, \
             {ANALYSIS}, and {OTHER} as present in this class."
        ))
        .arg(path_arg)
        .arg(cores_arg)
        .arg(DocArg::new_textdelim_param())
        .arg(DocArg::new_big_other_param())
        .arg(DocArg::new_checked_range_datatypes())
        .arg(DocArg::new_disallow_over_range())
        .arg(DocArg::new_row_buffer_size(false))
        .returns(ret);

    let fun_args = doc.fun_args();
    let ret_path = doc.ret_path();

    quote! {
        #[pyfunction]
        #doc
        pub fn fcs_write_datasets(#fun_args) -> #ret_path {
            let tconf = fireflow_core::config::WriteTEXTInnerConfig::new(
                delim,
                big_other.into(),
            );
            let dconf = fireflow_core::config::WriteDatasetInnerConfig::new(
                tconf,
                checked_range_datatypes.into(),
                disallow_over_range.into(),
                row_buffer_size,
            );
            let cs = type_families::Functor::fmap(datasets, Into::into);
            #fun_path(&path, &cs[..], &dconf).py_resolve_commutative()
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_config_defaults(input: TokenStream) -> TokenStream {
    let path = parse_macro_input!(input as Path);
    let name = path.segments.last().unwrap().ident.clone();
    let name_str = name.to_string();
    let pyname = format_ident!("Py{name}");

    let strat: Path = parse_quote!(fireflow_types::config::ReadStrategy);
    let has_strat: Path = parse_quote!(fireflow_core::config::HasStrategy);

    let api_fun = |s| format!("`~pyreflow.{s}`");

    let predoc = match name_str.as_str() {
        "ReadHeaderConfig" => {
            DocString::new_class(format!("Config for {}.", api_fun("fcs_read_header")))
        }
        "ReadFlatTEXTConfig" => DocString::new_class(format!("Config for reading flat {TEXT}."))
            .para(format!(
                "Can be used with {} and {}.",
                api_fun("fcs_read_flat_text"),
                api_fun("fcs_read_flat_texts")
            )),
        "ReadStdTEXTConfig" => {
            DocString::new_class(format!("Config for reading standardized {TEXT}.")).para(format!(
                "Can be used with {} and {}.",
                api_fun("fcs_read_std_text"),
                api_fun("fcs_read_std_texts")
            ))
        }
        "ReadFlatDatasetConfig" => DocString::new_class(format!(
            "Config for reading flat {TEXT} and {DATA}."
        ))
        .para(format!(
            "Can be used with {} and {}.",
            api_fun("fcs_read_flat_dataset"),
            api_fun("fcs_read_flat_datasets")
        )),
        "ReadStdDatasetConfig" => DocString::new_class(format!(
            "Config for reading flat {TEXT} and {DATA}."
        ))
        .para(format!(
            "Can be used with {} and {}.",
            api_fun("fcs_read_std_dataset"),
            api_fun("fcs_read_std_datasets")
        )),
        "ReadFlatDatasetFromKeywordsConfig" => DocString::new_class(format!(
            "Config for {}.",
            api_fun("fcs_read_flat_dataset_with_keywords")
        )),
        "NewCoreTEXTConfig" => DocString::new_class("Config for :py:func:`CoreTEXT*.from_kws`."),
        "NewCoreDatasetConfig" => {
            DocString::new_class("Config for :py:func:`CoreDataset*.from_kws`.")
        }
        s => panic!("unsupported type '{s}'"),
    };

    let doc = predoc.doc();

    let q = quote! {
        #doc
        #[pyclass(name = #name_str)]
        pub struct #pyname;

        #[pymethods]
        impl #pyname {
            /// Return standards-compliant configuration.
            ///
            /// :rtype: :py:class:`dict`\ [:py:class:`str`, :obj:`~typing.Any`]
            #[classmethod]
            fn strict(_: &Bound<'_, pyo3::types::PyType>) -> #path {
                #has_strat::new_with_strategy(#strat::Strict)
            }

            /// Return non-compliant configuration optimized to preserve data.
            ///
            /// All non-trivial metadata (ie whitespace, blank keys, etc) will be
            /// preserved.
            ///
            /// :rtype: :py:class:`dict`\ [:py:class:`str`, :obj:`~typing.Any`]
            #[classmethod]
            fn scalpal(_: &Bound<'_, pyo3::types::PyType>) -> #path {
                #has_strat::new_with_strategy(#strat::Scalpal)
            }

            /// Return non-compliant configuration optimized to read data.
            ///
            /// Metadata may be destroyed or dropped.
            ///
            /// :rtype: :py:class:`dict`\ [:py:class:`str`, :obj:`~typing.Any`]
            #[classmethod]
            fn sledgehammer(_: &Bound<'_, pyo3::types::PyType>) -> #path {
                #has_strat::new_with_strategy(#strat::Sledgehammer)
            }
        }
    };
    q.into()
}

#[proc_macro]
pub fn impl_py_header(input: TokenStream) -> TokenStream {
    let path = parse_macro_input!(input as Path);
    let name = path.segments.last().unwrap().ident.clone();

    let version = DocArgROIvar::new_version_ivar();

    let segments = DocArgROIvar::new_ivar_ro(
        "segments",
        PyClass::new_py(["api"], "ParsedHeaderSegments"),
        format!("The segments from {HEADER}."),
        |_, _| quote!(self.0.segments.clone().into()),
    );

    let uncorrected_segments = DocArgROIvar::new_ivar_ro(
        "uncorrected_segments",
        PyClass::new_py(["api"], "UncorrectedHeaderSegments"),
        format!("The uncorrected segments from {HEADER}."),
        |_, _| quote!(self.0.uncorrected_segments.clone().into()),
    );

    let args = [version, segments, uncorrected_segments];

    let doc = DocString::new_class(format!("The {HEADER} segment from an FCS dataset.")).args(args);
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

    let text = DocArg::new_text_seg_param().into_ro(|_, _| quote!(*self.0.as_ref()));
    let data =
        DocArg::new_data_seg_param(SegmentSrc::Header).into_ro(|_, _| quote!(*self.0.as_ref()));
    let analysis = DocArg::new_analysis_seg_param(SegmentSrc::Header, false)
        .into_ro(|_, _| quote!(*self.0.as_ref()));

    let other = DocArg::new_other_segs_param().into_ro(|_, _| {
        quote! {
            let os: &Option<_> = self.0.as_ref();
            os.clone().map(|(os, w)| (os.into(), w))
        }
    });

    let args = [text, data, analysis, other];

    let doc = DocString::new_class(format!("The segments from {HEADER}.")).args(args);

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> PyResult<Self> {
                let x = #bare_path::try_new(
                    text_seg,
                    data_seg,
                    analysis_seg,
                    other_segs.map(|(os, w)| (os.0, w)),
                )?;
                Ok(x.into())
            }
        }
    };
    doc.into_impl_class(name, &path, new).1.into()
}

#[proc_macro]
pub fn impl_py_uncorrected_header_segments(input: TokenStream) -> TokenStream {
    let path = parse_macro_input!(input as Path);
    let bare_path = path_strip_args(path.clone());
    let name = path.segments.last().unwrap().ident.clone();

    let text = DocArg::new_uncorrected_seg_param(
        "text_seg",
        AnySegment::PrimaryTEXT,
        UncorrSegmentSrc::Header,
    )
    .into_ro(|_, _| quote!(self.0.text));
    let data =
        DocArg::new_uncorrected_seg_param("data_seg", AnySegment::Data, UncorrSegmentSrc::Header)
            .into_ro(|_, _| quote!(self.0.data));
    let analysis = DocArg::new_uncorrected_seg_param(
        "analysis_seg",
        AnySegment::Analysis,
        UncorrSegmentSrc::Header,
    )
    .into_ro(|_, _| quote!(self.0.analysis));

    let other = DocArg::new_param(
        "other_segs",
        PyList::new1(PyTuple::new_uncorrected_segment()),
        format!("The uncorrected {OTHER} segments from {HEADER}."),
    )
    .into_ro(|_, _| quote!(self.0.other.clone()));

    let args = [text, data, analysis, other];

    let doc = DocString::new_class(format!("The uncorrected segments from {HEADER}")).args(args);
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

    let kws =
        DocArg::new_valid_keywords_param().into_ro(|_, _| quote!(self.0.keywords.clone().into()));

    let flat = DocArg::new_flat_diagnostics_param()
        .into_ro(|_, _| quote!(self.0.flat_diagnostics.clone().into()));

    let args = [kws, flat];

    let doc = DocString::new_class(format!("Parsed {HEADER} and {TEXT}.")).args(args);

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #path::new(kws.into(), flat_diagnostics.into()).into()
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
        format!("Parsed {TEXT} segment."),
        |n, _| quote!(self.0.#n.clone().into()),
    );

    let dataset = DocArgROIvar::new_dataset_ivar(false);

    let scores = DocArg::new_version_scores_param();

    let args = [text, dataset, scores];

    let doc = DocString::new_class("Dataset from FCS file parsed with flat mode.").args(args);

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #path::new(
                    text.into(),
                    dataset.into(),
                    version_scores.map(|(a, b, c, d)| (a.into(), b.into(), c.into(), d.into()))
                ).into()
            }
        }
    };
    doc.into_impl_class(name, &path, new).1.into()
}

#[proc_macro]
pub fn impl_py_flat_dataset_with_kws_output(input: TokenStream) -> TokenStream {
    let path = parse_macro_input!(input as Path);
    let name = path.segments.last().unwrap().ident.clone();

    let data = DocArg::new_data_param(false).into_ro(|_, _| quote!(self.0.data.clone().into()));
    let analysis =
        DocArg::new_analysis_param(false).into_ro(|_, _| quote!(self.0.analysis.clone()));
    let others = DocArg::new_others_param(false).into_ro(|_, _| quote!(self.0.others.clone()));
    let dataset_segs =
        DocArg::new_dataset_segments_param().into_ro(|_, _| quote!(self.0.dataset_segments.into()));
    let event = DocArg::new_event_diagnostics_param()
        .into_ro(|_, _| quote!(self.0.events_diagnostics.clone().into()));

    let args = [data, analysis, others, dataset_segs, event];
    let doc = DocString::new_class(format!("Dataset from parsing flat {TEXT}.")).args(args);

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #path::new(
                    data.into(),
                    analysis,
                    others,
                    dataset_segs.into(),
                    events_diagnostics.into()
                ).into()
            }
        }
    };
    doc.into_impl_class(name, &path, new).1.into()
}

#[proc_macro]
pub fn impl_py_new_flat_dataset_with_kws_output(input: TokenStream) -> TokenStream {
    let path = parse_macro_input!(input as Path);
    let name = path.segments.last().unwrap().ident.clone();

    let dataset = DocArgROIvar::new_dataset_ivar(false);

    let header = DocArgROIvar::new_ivar_ro(
        "header",
        PyClass::new_py(["api"], "ParsedHeaderSegments"),
        format!("(Possibly modified) offsets used to parse {HEADER}."),
        |_, _| quote!(self.0.header.clone().into()),
    );

    let args = [dataset, header];
    let doc = DocString::new_class(format!(
        "Output of using keywords to crate new standardized {TEXT} and {DATA}."
    ))
    .args(args);

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #path::new(dataset.into(), header.into()).into()
            }
        }
    };
    doc.into_impl_class(name, &path, new).1.into()
}

#[proc_macro]
pub fn impl_py_read_events_diagnostics(input: TokenStream) -> TokenStream {
    let path = parse_macro_input!(input as Path);
    let name = path.segments.last().unwrap().ident.clone();

    let event_width = DocArgROIvar::new_ivar_ro(
        "event_width",
        PyOpt::new1(RsInt::U64),
        "The width of one event in bytes (if not ASCII delimited).",
        |_, _| quote!(self.0.event_width.clone()),
    );

    let event_data_remainder = DocArgROIvar::new_ivar_ro(
        "event_data_remainder",
        PyOpt::new1(RsInt::U64),
        "The remainder after dividing length of DATA by event width.",
        |_, _| quote!(self.0.event_data_remainder.clone()),
    );

    let tot_event_mismatch = DocArgROIvar::new_ivar_ro(
        "tot_event_mismatch",
        PyOpt::new1(PyBool::default()),
        format!("{TRUE} if {TOT} does not match the number of events computed via event width."),
        |_, _| quote!(self.0.tot_event_mismatch.clone()),
    );

    let truncated_columns = DocArgROIvar::new_ivar_ro(
        "overrange_columns",
        PyList::new_overrange_columns(),
        format!(
            "Columns for which at least one event was out of range via {PNR}. \
             Each index corresponds to a column in {DATA}. Elements will be \
             {NONE} if not overrange at all, otherwise the first integer \
             is the first row that is overrange and the second boolean is \
             {TRUE} if the value was truncated."
        ),
        |_, _| quote!(self.0.overrange_columns.clone()),
    );

    let args = [
        event_width,
        event_data_remainder,
        tot_event_mismatch,
        truncated_columns,
    ];
    let doc =
        DocString::new_class(format!("Diagnostic output from reading {DATA} segment.")).args(args);
    let inner_args = doc.idents();

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
pub fn impl_py_keyword_version_score(input: TokenStream) -> TokenStream {
    let path = parse_macro_input!(input as Path);
    let name = path.segments.last().unwrap().ident.clone();

    let param = |argname, doc| {
        DocArgROIvar::new_ivar_ro(argname, RsInt::Usize, doc, |n, _| quote!(self.0.#n))
    };

    let good_req = param(
        "good_req",
        "Number of required keywords expected to be in this version and found.",
    );

    let good_opt = param(
        "good_opt",
        "Number of optional keywords expected to be in this version and found.",
    );

    let drop = param(
        "drop",
        "Number of keywords (opt or req) that must be dropped for this version.",
    );

    let missing_opt = param(
        "missing_opt",
        "Number of optional keywords that are missing in this version.",
    );

    let missing_req = param(
        "missing_req",
        "Number of required keywords that are missing in this version.",
    );

    let missing_absent = param(
        "missing_absent",
        "Number of keywords that are expected to be missing for this version.",
    );

    let args = [
        good_req,
        good_opt,
        drop,
        missing_opt,
        missing_req,
        missing_absent,
    ];
    let doc =
        DocString::new_class("Score generated when guessing version from keywords.").args(args);
    let inner_args = doc.idents();

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
pub fn impl_py_std_diagnostics(input: TokenStream) -> TokenStream {
    let path = parse_macro_input!(input as Path);
    let name = path.segments.last().unwrap().ident.clone();

    let pseudostandard = DocArgROIvar::new_ivar_ro(
        "pseudostandard",
        PyDict::new_std_keywords(),
        format!("Keywords which start with {DOLLAR_STR} but are not part of the standard."),
        |_, _| quote!(self.0.pseudostandard.clone()),
    );

    let hyper_par = DocArgROIvar::new_ivar_ro(
        "hyper_par",
        PyDict::new_std_keywords(),
        format!(
            "Measurement keywords which are part of the standard but have an index outside {PAR}."
        ),
        |_, _| quote!(self.0.hyper_par.clone()),
    );

    let hyper_gate = DocArgROIvar::new_ivar_ro(
        "hyper_gate",
        PyDict::new_std_keywords(),
        format!("Gating keywords which are part of the standard but have an index outside {GATE}.",),
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
        PyOpt::new1(PyStr::new_ne_str()),
        format!("Unused {TIMESTEP} keyword"),
        |_, _| quote!(self.0.timestep.clone()),
    );

    let original_names = DocArgROIvar::new_ivar_ro(
        "original_names",
        PyList::new1(PyOpt::new1(PyStr::new_shortname())),
        format!("Original {PNN} if they were renamed."),
        |_, _| quote!(self.0.original_names.clone()),
    );

    let scale = DocArgROIvar::new_ivar_ro(
        "scale",
        PyList::new1(PyOpt::new_scale_fix()),
        format!("Diagnostic data from parsing {PNE} keywords."),
        |_, _| quote!(self.0.scale.clone()),
    );

    let gate_scale = DocArgROIvar::new_ivar_ro(
        "gate_scale",
        PyList::new1(PyOpt::new_gate_scale_fix()),
        format!("Diagnostic data from parsing {GME} keywords."),
        |_, _| quote!(self.0.gate_scale.clone()),
    );

    let trimmed = DocArgROIvar::new_ivar_ro(
        "trimmed",
        PyList::new1(PyTuple::new1(PyStr::new_std_keyword()).add(PyStr::new_ne_str())),
        "Keywords which had whitespace between commas trimmed.",
        |_, _| quote!(self.0.trimmed.clone()),
    );

    let tmp_opt_pairs = DocArgROIvar::new_ivar_ro(
        "temporal_optical_pairs",
        PyList::new1(PyTuple::new1(PyStr::new_std_keyword()).add(PyStr::new_ne_str())),
        "Optical keys that were found in the temporal measurement.",
        |_, _| quote!(self.0.temporal_optical_pairs.clone()),
    );

    let timestep_added = DocArgROIvar::new_ivar_ro(
        "timestep_added",
        PyBool::default(),
        "{TRUE} if {TIMESTEP} was missing and added via configuration.",
        |_, _| quote!(self.0.timestep_added),
    );

    let doc =
        DocString::new_class(format!("Diagnostic output from {TEXT} standardization.")).args([
            pseudostandard,
            hyper_par,
            hyper_gate,
            other_version,
            timestep,
            original_names,
            scale,
            gate_scale,
            trimmed,
            tmp_opt_pairs,
            timestep_added,
        ]);
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
pub fn impl_py_dataset_segments(input: TokenStream) -> TokenStream {
    let path = parse_macro_input!(input as Path);
    let name = path.segments.last().unwrap().ident.clone();

    let data = DocArg::new_data_seg_param(SegmentSrc::Any).into_ro(|_, _| quote!(self.0.data));
    let analysis = DocArg::new_analysis_seg_param(SegmentSrc::Any, false)
        .into_ro(|_, _| quote!(self.0.analysis));
    let data_uncorrected = DocArg::new_uncorrected_seg_param(
        "data_seg_uncorrected",
        AnySegment::Data,
        UncorrSegmentSrc::Text,
    )
    .into_ro(|_, _| quote!(self.0.data_uncorrected));
    let analysis_uncorrected = DocArg::new_uncorrected_seg_param(
        "analysis_seg_uncorrected",
        AnySegment::Analysis,
        UncorrSegmentSrc::Text,
    )
    .into_ro(|_, _| quote!(self.0.analysis_uncorrected));

    let args = [data, analysis, data_uncorrected, analysis_uncorrected];
    let doc =
        DocString::new_class(format!("Segments used to parse {DATA} and {ANALYSIS}")).args(args);
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
pub fn impl_py_std_text_output(input: TokenStream) -> TokenStream {
    let path = parse_macro_input!(input as Path);
    let name = path.segments.last().unwrap().ident.clone();

    let tot = DocArgROIvar::new_ivar_ro(
        "tot",
        PyOpt::new1(PyInt::new_int(RsInt::Usize).rstype(keyword_path("Tot"))),
        format!("Value of {TOT} from {TEXT}."),
        |_, _| quote!(self.0.tot.as_ref().copied()),
    );

    let dataset_segs =
        DocArg::new_dataset_segments_param().into_ro(|_, _| quote!(self.0.dataset_segments.into()));

    let std = DocArg::new_std_diagnostics_param()
        .into_ro(|_, _| quote!(self.0.std_diagnostics.clone().into()));

    let flat = DocArg::new_flat_diagnostics_param()
        .into_ro(|_, _| quote!(self.0.flat_diagnostics.clone().into()));

    let scores = DocArg::new_version_scores_param();

    let args = [tot, dataset_segs, std, flat, scores];
    let doc =
        DocString::new_class(format!("Miscellaneous data when standardizing {TEXT}.")).args(args);

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #path::new(
                    tot,
                    dataset_segs.into(),
                    std_diagnostics.into(),
                    flat_diagnostics.into(),
                    version_scores.map(|(a, b, c, d)| (a.into(), b.into(), c.into(), d.into()))
                ).into()
            }
        }
    };
    doc.into_impl_class(name, &path, new).1.into()
}

#[proc_macro]
pub fn impl_py_std_dataset_output(input: TokenStream) -> TokenStream {
    let path = parse_macro_input!(input as Path);
    let name = path.segments.last().unwrap().ident.clone();

    let dataset = DocArgROIvar::new_dataset_ivar(true);

    let flat = DocArg::new_flat_diagnostics_param()
        .into_ro(|_, _| quote!(self.0.flat_diagnostics.clone().into()));

    let scores = DocArg::new_version_scores_param();

    let args = [dataset, flat, scores];

    let doc =
        DocString::new_class(format!("Miscellaneous data when standardizing {TEXT}.")).args(args);

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #path::new(
                    dataset.into(),
                    flat_diagnostics.into(),
                    version_scores.map(|(a, b, c, d)| (a.into(), b.into(), c.into(), d.into()))
                ).into()
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
    let std = DocArg::new_std_diagnostics_param()
        .into_ro(|_, _| quote!(self.0.std_diagnostics.clone().into()));
    let event = DocArg::new_event_diagnostics_param()
        .into_ro(|_, _| quote!(self.0.events_diagnostics.clone().into()));

    let doc = DocString::new_class(format!(
        "Miscellaneous data when standardizing {TEXT} from keywords."
    ))
    .args([dataset_segs, std, event]);

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #path::new(
                    dataset_segs.into(),
                    std_diagnostics.into(),
                    events_diagnostics.into()
                ).into()
            }
        }
    };
    doc.into_impl_class(name, &path, new).1.into()
}

#[proc_macro]
pub fn impl_py_new_std_dataset_with_kws_output(input: TokenStream) -> TokenStream {
    let path = parse_macro_input!(input as Path);
    let name = path.segments.last().unwrap().ident.clone();

    let dataset = DocArgROIvar::new_dataset_ivar(true);

    let header = DocArgROIvar::new_ivar_ro(
        "header",
        PyClass::new_py(["api"], "ParsedHeaderSegments"),
        format!("(Possibly modified) offsets used to parse {HEADER}."),
        |_, _| quote!(self.0.header.clone().into()),
    );

    let args = [dataset, header];
    let doc = DocString::new_class(format!(
        "Output of using keywords to crate new standardized {TEXT} and {DATA}."
    ))
    .args(args);

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #path::new(dataset.into(), header.into()).into()
            }
        }
    };
    doc.into_impl_class(name, &path, new).1.into()
}

#[allow(clippy::too_many_lines)]
#[proc_macro]
pub fn impl_py_header_supp(input: TokenStream) -> TokenStream {
    let path = parse_macro_input!(input as Path);
    let name = path.segments.last().unwrap().ident.clone();

    let header = DocArg::new_header_param().into_ro(|_, _| quote!(self.0.header.clone().into()));

    let supp = DocArgROIvar::new_ivar_ro(
        "supp_text",
        PyOpt::new1(
            PyTuple::new1(PyOpt::new1(PyTuple::new_supp_text_segment()))
                .add(PyTuple::new_uncorrected_segment()),
        ),
        format!("Supplemental {TEXT} offsets if given (corrected and uncorrected)."),
        |_, _| quote!(self.0.supp_text.as_ref().copied()),
    );

    let rstype = keyword_path("Nextdata");
    let nextdata = DocArgROIvar::new_ivar_ro(
        "nextdata",
        PyOpt::new1(PyInt::from(RsInt::U64).rstype(rstype)),
        format!("The value of {NEXTDATA}."),
        |_, _| quote!(self.0.nextdata),
    );

    let args = [header, supp, nextdata];

    let doc = DocString::new_class(format!("{HEADER} data and supplemental offsets.")).args(args);
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

#[allow(clippy::too_many_lines)]
#[proc_macro]
pub fn impl_py_flat_text_diagnostics(input: TokenStream) -> TokenStream {
    let path = parse_macro_input!(input as Path);
    let name = path.segments.last().unwrap().ident.clone();

    let header_supp = DocArgROIvar::new_ivar_ro(
        "header_supp",
        PyClass::new_py(["api"], "HeaderAndSuppOffsets"),
        format!("{HEADER} data and supplemental {TEXT} offsets."),
        |_, _| quote!(self.0.header_supp.clone().into()),
    );

    let byte_pairs = DocArgROIvar::new_ivar_ro(
        "byte_pairs",
        PyList::new1(
            PyTuple::new1(PyUnion::new_key_or_bytes()).add(PyUnion::new_ne_string_or_bytes()),
        ),
        "Keywords with keys that are not ASCII or values that are not UTF-8.",
        |_, _| quote!(self.0.byte_pairs.clone()),
    );

    let non_unique_std = DocArgROIvar::new_ivar_ro(
        "non_unique_std_keywords",
        PyList::new1(PyTuple::new2([
            PyType::from(PyStr::new_std_keyword()),
            PyStr::new_ne_truncated_str().into(),
        ])),
        format!("Standard keys which already appeared in {TEXT} previously."),
        |_, _| quote!(self.0.non_unique_std_keywords.clone()),
    );

    let non_unique_nonstd = DocArgROIvar::new_ivar_ro(
        "non_unique_nonstd_keywords",
        PyList::new1(PyTuple::new2([
            PyType::from(PyStr::new_nonstd_keyword()),
            PyStr::new_ne_truncated_str().into(),
        ])),
        format!("Nonstandard keys which already appeared in {TEXT} previously."),
        |_, _| quote!(self.0.non_unique_nonstd_keywords.clone()),
    );

    let ignored = DocArgROIvar::new_ivar_ro(
        "ignored_standard_keywords",
        PyList::new1(PyTuple::new2([
            PyType::from(PyStr::new_std_keyword()),
            PyUnion::new_ne_string_or_bytes().into(),
        ])),
        "Standard keys which were ignored by the user.",
        |_, _| quote!(self.0.ignored_standard_keywords.clone()),
    );

    let trimmed_empty = DocArgROIvar::new_ivar_ro(
        "keys_with_empty_trimmed_values",
        PyList::new1(PyUnion::new_key_or_bytes()),
        "Keys with empty values as a result of trimming whitespace.",
        |_, _| quote!(self.0.keys_with_empty_trimmed_values.clone()),
    );

    let trimmed = DocArgROIvar::new_ivar_ro(
        "keys_with_trimmed_values",
        PyList::new1(
            PyTuple::new1(PyUnion::new_key_or_bytes()).add(PyUnion::new_ne_string_or_bytes()),
        ),
        "Keys with values that are not empty after whitespace was trimmed off.",
        |_, _| quote!(self.0.keys_with_trimmed_values.clone()),
    );

    let primary_split = DocArgROIvar::new_ivar_ro(
        "primary_split",
        PyClass::new_py(["api"], "SplitTEXTDiagnostics"),
        format!("Additional parsing diagnostics for primary {TEXT}."),
        |_, _| quote!(self.0.primary_split.clone().into()),
    );

    let supp_split = DocArgROIvar::new_ivar_ro(
        "supp_split",
        PyOpt::new1(PyClass::new_py(["api"], "SplitTEXTDiagnostics")),
        format!("Additional parsing diagnostics for supplemental {TEXT}."),
        |_, _| quote!(self.0.supp_split.as_ref().map(|x| x.clone().into())),
    );

    let args = [
        header_supp,
        byte_pairs,
        non_unique_std,
        non_unique_nonstd,
        ignored,
        trimmed_empty,
        trimmed,
        primary_split,
        supp_split,
    ];

    let doc = DocString::new_class(format!("Diagnostic data from parsing {TEXT}.")).args(args);
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
pub fn impl_py_split_text_diagnostics(input: TokenStream) -> TokenStream {
    let path = parse_macro_input!(input as Path);
    let name = path.segments.last().unwrap().ident.clone();

    let delim = DocArgROIvar::new_ivar_ro(
        "delimiter",
        RsInt::U8,
        format!("Delimiter used to parse {TEXT}."),
        |_, _| quote!(self.0.delimiter),
    );

    let escaped = DocArgROIvar::new_ivar_ro(
        "escaped",
        PyBool::default(),
        format!("{TRUE} if delimiters were escaped."),
        |_, _| quote!(self.0.escaped),
    );

    let keys_with_blank_values = DocArgROIvar::new_ivar_ro(
        "keys_with_blank_values",
        PyList::new1(PyUnion::new_ne_string_or_bytes()),
        "Keys which have blank values (relatively common).",
        |_, _| quote!(self.0.keys_with_blank_values.clone()),
    );

    let values_with_blank_keys = DocArgROIvar::new_ivar_ro(
        "values_with_blank_keys",
        PyList::new1(PyUnion::new_ne_string_or_bytes()),
        "Values which have blank keys (relatively rare).",
        |_, _| quote!(self.0.values_with_blank_keys.clone()),
    );

    let skipped_pairs = DocArgROIvar::new_ivar_ro(
        "skipped_pairs",
        RsInt::Usize,
        "Number of key/value pairs that were skipped because both were blank.",
        |_, _| quote!(self.0.skipped_pairs),
    );

    let tokens_with_boundary_delims = DocArgROIvar::new_ivar_ro(
        "tokens_with_boundary_delims",
        PyList::new1(PyUnion::new_ne_string_or_bytes()),
        "Tokens (keys or values) which have delimiters at their boundary.",
        |_, _| quote!(self.0.tokens_with_boundary_delims.clone()),
    );

    let last_odd_token = DocArgROIvar::new_ivar_ro(
        "last_odd_token",
        PyUnion::new_string_or_bytes(),
        "Last token if the number of tokens is odd (empty if not present).",
        |_, _| quote!(self.0.last_odd_token.clone()),
    );

    let has_even_delims = DocArgROIvar::new_ivar_ro(
        "has_even_delims",
        PyBool::default(),
        format!("{TRUE} if {TEXT} has an even number of delimiters."),
        |_, _| quote!(self.0.has_even_delims),
    );

    let extra_leading_delims = DocArgROIvar::new_ivar_ro(
        "extra_leading_delims",
        RsInt::Usize,
        format!("The number of delimiters at the front of {TEXT} (excluding the first)."),
        |_, _| quote!(self.0.extra_leading_delims),
    );

    let args = [
        delim,
        escaped,
        keys_with_blank_values,
        values_with_blank_keys,
        skipped_pairs,
        tokens_with_boundary_delims,
        last_odd_token,
        has_even_delims,
        extra_leading_delims,
    ];

    let doc = DocString::new_class(format!(
        "Diagnostic data when parsing a specific {TEXT} segment."
    ))
    .args(args);
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

    let seg_len = |argname, which| {
        DocArgROIvar::new_ivar_ro(
            argname,
            RsInt::U64,
            format!("Length of {which} (in bytes)"),
            |n, _| quote!(self.0.#n),
        )
    };

    let text_len = seg_len("text_len", TEXT);
    let data_len = seg_len("data_len", DATA);
    let analysis_len = seg_len("analysis_len", ANALYSIS);

    let n_events = DocArgROIvar::new_ivar_ro(
        "n_events",
        RsInt::Usize,
        format!("Number of events ({TOT})"),
        |_, _| quote!(self.0.n_events),
    );

    let n_measurements = DocArgROIvar::new_ivar_ro(
        "n_measurements",
        RsInt::Usize,
        format!("Number of measurements ({PAR})"),
        |_, _| quote!(self.0.n_measurements),
    );

    let n_other = DocArgROIvar::new_ivar_ro(
        "n_other",
        RsInt::Usize,
        format!("Number of {OTHER} segments"),
        |_, _| quote!(self.0.n_other),
    );

    let others_len = DocArgROIvar::new_ivar_ro(
        "others_len",
        RsInt::Usize,
        format!("Total length of {OTHER} segments (in bytes)"),
        |_, _| quote!(self.0.others_len),
    );

    let datatype = DocArgROIvar::new_ivar_ro(
        "datatype",
        PyOpt::new1(PyLiteral::new_datatype()),
        format!("The value of {DATATYPE}"),
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
    let layout: AnyDocArg = DocArg::new_data_schema_ivar(version).into();
    let data: AnyDocArg = DocArg::new_df_ivar().into();
    let analysis: AnyDocArg = DocArg::new_analysis_ivar().into();
    let others = DocArg::new_others_ivar().into();

    let mode_kw = if version < Version::FCS3_2 {
        Kw::Mode
    } else {
        Kw::Mode3_2
    };
    let mode = DocArg::new_kw_ivar1(mode_kw);

    let cyt = if version < Version::FCS3_2 {
        DocArg::new_kw_ivar1(Kw::Cyt)
    } else {
        DocArg::new_kw_ivar(Kw::Cyt3_2, false)
    };

    let abrt = DocArg::new_kw_ivar1(Kw::Abrt);
    let com = DocArg::new_kw_ivar1(Kw::Com);
    let cells = DocArg::new_kw_ivar1(Kw::Cells);
    let exp = DocArg::new_kw_ivar1(Kw::Exp);
    let fil = DocArg::new_kw_ivar1(Kw::Fil);
    let inst = DocArg::new_kw_ivar1(Kw::Inst);
    let lost = DocArg::new_kw_ivar1(Kw::Lost);
    let op = DocArg::new_kw_ivar1(Kw::Op);
    let proj = DocArg::new_kw_ivar1(Kw::Proj);
    let smno = DocArg::new_kw_ivar1(Kw::Smno);
    let src = DocArg::new_kw_ivar1(Kw::Src);
    let sys = DocArg::new_kw_ivar1(Kw::Sys);
    let cytsn = DocArg::new_kw_ivar1(Kw::Cytsn);

    let unicode = DocArg::new_kw_ivar1(Kw::Unicode);

    let csvbits = DocArg::new_kw_ivar1(Kw::CSVBits);
    let cstot = DocArg::new_kw_ivar1(Kw::CSTot);
    let csvflags = DocArg::new_csvflags_ivar();

    let all_subset = [csvbits, cstot, csvflags];

    let last_modifier = DocArg::new_kw_ivar1(Kw::LastModifier);
    let last_mod_date = DocArg::new_kw_ivar1(Kw::LastModified);
    let originality = DocArg::new_kw_ivar1(Kw::Originality);

    let all_modified = [last_modifier, last_mod_date, originality];

    let plateid = DocArg::new_kw_ivar1(Kw::Plateid);
    let platename = DocArg::new_kw_ivar1(Kw::Platename);
    let wellid = DocArg::new_kw_ivar1(Kw::Wellid);

    let all_plate = [plateid, platename, wellid];

    let vol = DocArg::new_kw_ivar1(Kw::Vol);

    let comp_or_spill = match version {
        Version::FCS2_0 => DocArg::new_comp_ivar(true),
        Version::FCS3_0 => DocArg::new_comp_ivar(false),
        _ => DocArg::new_spillover_ivar(),
    };

    let flowrate = DocArg::new_kw_ivar1(Kw::Flowrate);

    let carrierid = DocArg::new_kw_ivar1(Kw::Carrierid);
    let carriertype = DocArg::new_kw_ivar1(Kw::Carriertype);
    let locationid = DocArg::new_kw_ivar1(Kw::Locationid);

    let all_carrier = [carrierid, carriertype, locationid];

    let unstainedcenters = DocArg::new_unstainedcenters_ivar();
    let unstainedinfo = DocArg::new_kw_ivar1(Kw::UnstainedInfo);

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

    let coretext_doc = DocString::new_class(format!("Represents {TEXT} for an FCS {vs} file."))
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
                let d = PyFCSDataFrame::try_from(data)?;
                Ok(x.into_coredataset(d.0, analysis, others)?.into())
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
    let doc = DocString::new_ivar(format!("The value for {PAR}."), RsInt::Usize);
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
        |_, _| {
            quote! {
                let ns = self.0.get_meas_nonstandard().clone();
                type_families::Functor::fmap(ns, Clone::clone)
            }
        },
        |n, _| quote!(Ok(self.0.set_meas_nonstandard(#n)?)),
    )
    .into()
}

#[proc_macro]
pub fn impl_core_standard_keywords(input: TokenStream) -> TokenStream {
    let ident = parse_macro_input!(input as Ident);
    let _ = split_ident_version_pycore(&ident);

    let req_or_opt_path = parse_quote!(fireflow_types::config::IncludeReqOrOpt);
    let root_or_meas_path = parse_quote!(fireflow_types::config::IncludeRootOrMeas);

    let req_or_opt = DocArg::new_param(
        "req_or_opt",
        PyLiteral::new1(tc::IncludeReqOrOpt::iter_str()).rstype(req_or_opt_path),
        "Selects if required, optional, or both keywords should be returned",
    );

    let root_or_meas = DocArg::new_param(
        "root_or_meas",
        PyLiteral::new1(tc::IncludeRootOrMeas::iter_str()).rstype(root_or_meas_path),
        "Selects if required, optional, or both keywords should be returned",
    );

    let doc = DocString::new_method("Return standard keywords as string pairs.")
        .para(format!("Each key will be prefixed with {DOLLAR_STR}."))
        .para(format!(
            "This will not include {TOT}, {NEXTDATA}, or any of the \
             offset keywords since these only matter if the dataset is written.",
        ))
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
    let doc = DocString::new_method(format!("Set the threshold for {tr}.", tr = Kw::Tr.kw()))
        .arg(p)
        .returns(
            DocReturn::new(PyBool::default())
                .desc(format!("{TRUE} if trigger is set and was updated.")),
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

    let exc0 = PyException::new_segment_overflow(Some(version));
    let exc1 = PyException::new_other_overflow();

    let nextdata = PyInt::new_nextdata();
    let ret = DocReturn::new(nextdata)
        .exc([exc0, exc1])
        .desc(format!("the value of {NEXTDATA} as written to the dataset"));

    let doc = DocString::new_method("Write data to path.")
        .para(format!(
            "Resulting FCS file will include {HEADER} and {TEXT}."
        ))
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

    let exc0 = PyException::new_segment_overflow(Some(version));
    let exc1 = PyException::new_other_overflow();

    let nextdata = PyInt::new_nextdata();
    let ret = DocReturn::new(nextdata).exc([exc0, exc1]).desc(format!(
        "the value of {NEXTDATA} which would point to next dataset if written"
    ));

    let doc = DocString::new_method("Write data as an FCS file.")
        .para(format!(
            "The resulting file will include {HEADER}, {TEXT}, {DATA}, \
             {ANALYSIS}, and {OTHER} as present in this class."
        ))
        .arg(DocArg::new_path_param(false))
        .arg(DocArg::new_textdelim_param())
        .arg(DocArg::new_big_other_param())
        .arg(DocArg::new_checked_range_datatypes())
        .arg(DocArg::new_disallow_over_range())
        .arg(DocArg::new_row_buffer_size(false))
        .arg(DocArg::new_appendable_param())
        .arg(DocArg::new_append_param())
        .returns(ret);

    let fun_args = doc.fun_args();
    let ret_path = doc.ret_path();

    quote! {
        #[pymethods]
        impl #i {
            #doc
            #[allow(clippy::too_many_arguments)]
            fn write_dataset(&self, #fun_args) -> #ret_path {
                let tconf = fireflow_core::config::WriteTEXTInnerConfig::new(
                    delim,
                    big_other.into(),
                );
                let dconf = fireflow_core::config::WriteDatasetInnerConfig::new(
                    tconf,
                    checked_range_datatypes.into(),
                    disallow_over_range.into(),
                    row_buffer_size,
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
pub fn impl_core_all_shortnames_attr(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let _ = split_ident_version_pycore(&i).1;

    let doc = DocString::new_ivar(
        format!("Value of {PNN} for all measurements."),
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
        format!("The possibly-empty values of {PNN} for all measurements."),
        PyList::new1(PyOpt::new1(PyStr::new_shortname())),
    )
    .para(format!(
        "{PNN} is optional for this FCS version so values may be {NONE}."
    ));

    doc.into_impl_get_set(
        &i,
        "all_shortnames_maybe",
        true,
        |_, _| {
            quote! {
                let ns = self.0.shortnames_maybe();
                type_families::Functor::fmap(ns, |x| x.cloned())
            }
        },
        |n, _| quote!(Ok(self.0.set_measurement_shortnames_maybe(#n).map(|_| ())?)),
    )
    .into()
}

#[proc_macro]
pub fn impl_core_get_set_timestep(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let _ = split_ident_version_pycore(&i).1;

    let t = PyOpt::new1(PyFloat::new_timestep());
    let get_doc = DocString::new_ivar(format!("The value of {TIMESTEP}"), t.clone());

    let getq = get_doc.into_impl_get(&i, "timestep", |_, _| quote!(self.0.timestep().copied()));

    let param = DocArg::new_param(
        "timestep",
        PyFloat::new_timestep(),
        "The timestep to set. Must be greater than zero.",
    );
    let set_doc = DocString::new_method(format!(
        "Set the {TIMESTEP} if time measurement is present."
    ))
    .arg(param)
    .returns(DocReturn::new(t.map_exc(|_| ())).desc(format!("Previous {TIMESTEP} if present.")));

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
            format!("The value of {TIMESTEP} to use."),
        ));
        let allow_loss = DocArg::new_allow_loss_param(
            "Choose what happens if optical-specific metadata (detectors, \
             lasers, etc) are found.",
        );
        DocString::new_method(format!("Set the temporal measurement to a given {i}."))
            .args(once(p).chain(timestep).chain([allow_loss]))
            .returns(DocReturn::new(PyBool::default()).desc(format!(
                "{TRUE} if temporal measurement was set, which will \
                 happen for all cases except when the time measurement is \
                 already set to {index_or_name}.",
                index_or_name = code(i),
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

    let make_doc = |has_timestep: bool, has_allow_loss: bool| {
        let s = "Convert the temporal measurement to an optical measurement.";
        let p = has_allow_loss
            .then_some(DocArg::new_allow_loss_param(
                "Choose what happens if temporal measurement cannot be \
                 converted to optical without data loss.",
            ))
            .into_iter();
        let (rt, rd) = if has_timestep {
            (
                PyOpt::new1(PyFloat::new_timestep()).into(),
                format!("Value of {TIMESTEP} if time measurement was present.",),
            )
        } else {
            (
                PyType::from(PyBool::default()),
                format!(
                    "{TRUE} if temporal measurement was present and converted, \
                     {FALSE} if there was not a temporal measurement."
                ),
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
            fn unset_temporal(&mut self, allow_loss: fireflow_core::config::AllowLoss) -> PyResult<#ret> {
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
            DocReturn::new(PyOpt::new1(PyStr::new_shortname())).desc("Previous name if present."),
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

    let linear = code("0,0");

    let exc = PyreflowError::Relational.fmt_ref();

    if version == Version::FCS2_0 {
        let s0 = format!(
            "Will be {UNIT} for linear scaling ({linear} in FCS encoding), \
             a 2-tuple for log scaling, or {NONE} if missing.",
        );
        let s1 = format!(
            "The temporal measurement must always be {UNIT}. \
             Setting it to another value will raise {exc}."
        );
        let doc = DocString::new_ivar(
            format!("The value for {PNE} for all measurements."),
            PyList::new1(PyOpt::new1(PyUnion::new_scale(false))),
        )
        .paras([s0, s1]);

        doc.into_impl_get_set(
            &i,
            "all_scales",
            true,
            |_, _| quote!(self.0.scales().collect()),
            |n, _| quote!(Ok(self.0.set_scales(#n)?)),
        )
    } else {
        let sum = format!("The value for {PNE} and/or {PNG} for all measurements.");
        let s0 = "Collectively these keywords correspond to scale transforms.";
        let s1 = format!(
            "If scaling is linear, return a float which corresponds to the \
             value of {PNG} when {PNE} is {linear}. If scaling is logarithmic, \
             return a pair of floats, corresponding to unset {PNG} and the \
             non-{linear} value of {PNE}."
        );
        let s2 = "The FCS standards disallow any other combinations.";
        let s3 = format!(
            "The temporal measurement will always be {unity}, corresponding \
             to an identity transform. Setting it to another value will \
             raise {exc}.",
            unity = code("1.0"),
        );
        let ss = [s0.into(), s1, s2.into(), s3];
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
                    .map(|e| {
                        type_families::BifunctorOnce::bimap_once(
                            e,
                            |t| t.value.clone(), |o| o.value.clone()
                        )
                    })
                    .map(type_families::BifunctorOnce::bimap_into_once)
                    .collect()
            }
        },
        |n, _| {
            quote! {
                let ms = #n
                    .into_iter()
                    .map(type_families::BifunctorOnce::bimap_into_once)
                    .collect();
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
        PyOpt::new1(
            PyTuple::new1(PyInt::new_meas_index())
                .add(PyStr::new_shortname())
                .add(PyClass::new_temporal(version)),
        ),
    )
    .ret_desc(format!("Index, name, and measurement or {NONE}."));

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

    let exc = PyException::new_index().desc(format!("If {index} not found", index = arg("index")));
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
                use type_families::{BifunctorOnce as _};
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

    let exc = PyException::new_key().desc(format!("If {name} not found", name = arg("name")));
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
                use type_families::{BifunctorOnce as _};
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
        "data schema and dataframe"
    } else {
        "data schema"
    };
    let ps = [format!(
        "Length of {measurements} must match number of columns in existing {s}.",
        measurements = arg(MEASUREMENTS),
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

    let rng = DocArg::new_any_range_param(version);

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
            .arg(rng.clone())
            .args(col_param)
    };

    let opt_doc = push_meas_doc(true, is_dataset);
    let tmp_doc = push_meas_doc(false, is_dataset);

    let opt_fun_args = opt_doc.fun_args();
    let tmp_fun_args = tmp_doc.fun_args();

    let range_series_method = match version {
        Version::FCS2_0 | Version::FCS3_0 => quote!(col.with_range(range)),
        Version::FCS3_1 => quote!(col.with_bitmask_range(range)?),
        Version::FCS3_2 => quote!(col.with_mixed_range(range)?),
    };

    let inner_q = if is_dataset {
        quote! {
            #opt_doc
            fn push_optical(&mut self, #opt_fun_args) -> PyResult<()> {
                let rng_col = #range_series_method;
                let _ = self.0.push_optical(name.into(), meas.into(), rng_col)?;
                Ok(())
            }

            #tmp_doc
            fn push_temporal(&mut self, #tmp_fun_args) -> PyResult<()> {
                let rng_col = #range_series_method;
                self.0.push_temporal(name.into(), meas.into(), rng_col)?;
                Ok(())
            }
        }
    } else {
        quote! {
            #opt_doc
            fn push_optical(&mut self, #opt_fun_args) -> PyResult<()> {
                let _ = self.0.push_optical(name.into(), meas.into(), range)?;
                Ok(())
            }

            #tmp_doc
            fn push_temporal(&mut self, #tmp_fun_args) -> PyResult<()> {
                self.0.push_temporal(name.into(), meas.into(), range)?;
                Ok(())
            }
        }
    };

    quote! {
        #[pymethods]
        impl #i {
            #inner_q
        }
    }
    .into()
}

#[proc_macro]
#[allow(clippy::too_many_lines)]
pub fn impl_core_remove_measurement(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let (is_dataset, version) = split_ident_version_pycore(&i);

    let meas = PyUnion::new_measurement(version);
    let rng = PyUnion::new_full_range();
    let int_widths = PyOpt::new1(PyLiteral::new_integer_width());
    let col_type = PyOpt::new1(PyLiteral::new_column_type());

    let make_ret = |is_index: bool| {
        // NOTE this is not a typo, these are supposed to be flipped
        let name_or_index = if is_index {
            PyType::new_versioned_shortname(version)
        } else {
            PyInt::new_meas_index().into()
        };
        let pre_ret = if is_dataset {
            PyTuple::new1(name_or_index)
                .add(meas.clone())
                .add(PyClass::new_series())
                .add(rng.clone())
        } else {
            PyTuple::new1(name_or_index)
                .add(meas.clone())
                .add(rng.clone())
        };
        let ret = match version {
            Version::FCS2_0 | Version::FCS3_0 => pre_ret,
            Version::FCS3_1 => pre_ret.add(int_widths.clone()),
            Version::FCS3_2 => pre_ret.add(col_type.clone()),
        };

        let (which, argname) = if is_index {
            ("Index", "index")
        } else {
            ("Name", "name")
        };
        let exc = PyException::new_index().desc(format!(
            "If {index_or_name} not found",
            index_or_name = arg(argname)
        ));
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

    let bimap_into_once = quote!(type_families::BifunctorOnce::bimap_into_once);

    // split the range into FullRange (either float or int) and its type if it
    // exists. This will minimize "surprises" when data schemas are
    // auto-normalized after removing a column. If going from a complex to
    // simple layout, the first removal will have a type and subsequent won't.
    // Rather than return unions in python with or without the type, split the
    // type out and return as a separate arg wrapped in None. User can ignore if
    // desired (which will be most of the type probably).
    //
    // This only applies to 3.1 and 3.2
    let name_mapper = if is_dataset {
        match version {
            Version::FCS2_0 | Version::FCS3_0 => quote! {
                |(i, x, c, r)| (i, #bimap_into_once(x), c.into(), r)
            },
            Version::FCS3_1 => quote! {
                |(i, x, c, r)| {
                    let (rr, t) = split_bitmask_range(r);
                    (i, #bimap_into_once(x), c.into(), rr, t)
                }
            },
            Version::FCS3_2 => quote! {
                |(i, x, c, r)| {
                    let (rr, t) = split_mixed_range(r);
                    (i, #bimap_into_once(x), c.into(), rr, t)
                }
            },
        }
    } else {
        match version {
            Version::FCS2_0 | Version::FCS3_0 => quote! {
                |(i, x, r)| (i, #bimap_into_once(x), r)
            },
            Version::FCS3_1 => quote! {
                |(i, x, r)| {
                    let (rr, t) = split_bitmask_range(r);
                    (i, #bimap_into_once(x), rr, t)
                }
            },
            Version::FCS3_2 => quote! {
                |(i, x, r)| {
                    let (rr, t) = split_mixed_range(r);
                    (i, #bimap_into_once(x), rr, t)
                }
            },
        }
    };

    let index_mapper = if is_dataset {
        match version {
            Version::FCS2_0 | Version::FCS3_0 => quote! {
                |(p, c, r)| {
                    let (n, v) = p.unzip();
                    (n, #bimap_into_once(v), c.into(), r)
                }
            },
            Version::FCS3_1 => quote! {
                |(p, c, r)| {
                    let (n, v) = p.unzip();
                    let (rr, t) = split_bitmask_range(r);
                    (n, #bimap_into_once(v), c.into(), rr, t)
                }
            },
            Version::FCS3_2 => quote! {
                |(p, c, r)| {
                    let (n, v) = p.unzip();
                    let (rr, t) = split_mixed_range(r);
                    (n, #bimap_into_once(v), c.into(), rr, t)
                }
            },
        }
    } else {
        match version {
            Version::FCS2_0 | Version::FCS3_0 => quote! {
                |(p, r)| {
                    let (n, v) = p.unzip();
                    (n, #bimap_into_once(v), r)
                }
            },
            Version::FCS3_1 => quote! {
                |(p, r)| {
                    let (n, v) = p.unzip();
                    let (rr, t) = split_bitmask_range(r);
                    (n, #bimap_into_once(v), rr, t)
                }
            },
            Version::FCS3_2 => quote! {
                |(p, r)| {
                    let (n, v) = p.unzip();
                    let (rr, t) = split_mixed_range(r);
                    (n, #bimap_into_once(v), rr, t)
                }
            },
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
                Ok(self.0.remove_measurement_by_name(&#name_ident).map(#name_mapper)?)
            }

            #by_index_doc
            fn remove_measurement_by_index(
                &mut self,
                #index_arg
            ) -> #index_ret {
                Ok(self.0.remove_measurement_by_index(#index_ident).map(#index_mapper)?)
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_core_insert_measurement(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let (is_dataset, version) = split_ident_version_pycore(&i);

    let rng = DocArg::new_any_range_param(version);

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
            .arg(rng.clone())
            .args(col_param)
    };

    let opt_doc = insert_meas_doc(true, is_dataset);
    let tmp_doc = insert_meas_doc(false, is_dataset);

    let opt_fun_args = opt_doc.fun_args();
    let tmp_fun_args = tmp_doc.fun_args();

    let range_series_method = match version {
        Version::FCS2_0 | Version::FCS3_0 => quote!(col.with_range(range)),
        Version::FCS3_1 => quote!(col.with_bitmask_range(range)?),
        Version::FCS3_2 => quote!(col.with_mixed_range(range)?),
    };

    let inner_q = if is_dataset {
        quote! {
            #opt_doc
            fn insert_optical(
                &mut self,
                #opt_fun_args
            ) -> PyResult<()> {
                let rng_col = #range_series_method;
                let _ = self.0.insert_optical(index.into(), name.into(), meas.into(), rng_col)?;
                Ok(())
            }

            #tmp_doc
            fn insert_temporal(
                &mut self,
                #tmp_fun_args
            ) -> PyResult<()> {
                let rng_col = #range_series_method;
                self.0.insert_temporal(index.into(), name.into(), meas.into(), rng_col)?;
                Ok(())
            }
        }
    } else {
        quote! {
            #opt_doc
            fn insert_optical(
                &mut self,
                #opt_fun_args
            ) -> PyResult<()> {
                let _ = self.0.insert_optical(index.into(), name.into(), meas.into(), range)?;
                Ok(())
            }

            #tmp_doc
            fn insert_temporal(
                &mut self,
                #tmp_fun_args
            ) -> PyResult<()> {
                self.0.insert_temporal(index.into(), name.into(), meas.into(), range)?;
                Ok(())
            }
        }
    };

    quote! {
        #[pymethods]
        impl #i {
            #inner_q
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
        let i = arg(&i_param.argname);
        let meas_desc = format!("Optical measurement to replace measurement at {i}.");
        let exc_desc = format!("If {i} does not exist.");
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
                let ret = self.0.replace_optical_at(index, meas.into())?;
                Ok(type_families::BifunctorOnce::bimap_into_once(ret))
            }

            #replace_named_doc
            fn replace_optical_named(&mut self, #name_fun_args) -> #named_ret {
                let ret = self.0.replace_optical_named(&name, meas.into())?;
                Ok(type_families::BifunctorOnce::bimap_into_once(ret))
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
        let allow_loss_param = DocArg::new_allow_loss_param(
            "Choose what happens if conversion from temporal measurement to \
             optical measurement is necessary and data loss will occur.",
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
        let i = arg(&i_param.argname);
        let meas_desc = format!("Temporal measurement to replace measurement at {i}.");
        let exc0 = e.desc(format!("If {i} does not exist"));
        let exc1 = PyException::new_pyreflow(PyreflowError::Relational)
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
                Ok(type_families::BifunctorOnce::bimap_into_once(ret))
            }

            #replace_named_doc
            fn replace_temporal_named(
                &mut self,
                #name_fun_args
            ) -> #named_ret {
                let ret = #replace_tmp_named_body;
                Ok(type_families::BifunctorOnce::bimap_into_once(ret))
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
    let (layout_conf, layout_args, layout_recs) =
        DocArgParam::new_read_data_schema_config_params(v);
    let (shared_conf, shared_args, shared_recs) = DocArgParam::new_shared_config_params();

    let other_kws = if version == Version::FCS2_0 {
        TOT
    } else {
        formatcp!(
            "{TOT}, {}, {}, {}, {}, or {TIMESTEP} (if time measurement not included)",
            fcs_kw!("$BEGINDATA"),
            fcs_kw!("$ENDDATA"),
            fcs_kw!("$BEGINANALYSIS"),
            fcs_kw!("$ENDANALYSIS")
        )
    };
    let no_kws =
        format!("Must not contain any {PN_ANY} keywords not indexed by {PAR} or {other_kws}.",);

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
    let exc1 = PyException::new_pyreflow(PyreflowError::Relational)
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
                PyClass::new_py(["api"], "StdTEXTDiagnostics"),
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
#[allow(clippy::too_many_lines)]
pub fn impl_coredataset_from_kws(input: TokenStream) -> TokenStream {
    let path = parse_macro_input!(input as Path);
    let ident = path.segments.last().unwrap().ident.clone();
    let version = split_ident_version_checked("CoreDataset", &ident);
    let pyname = format_ident!("Py{ident}");

    let core_conf = config_path("NewCoreDatasetConfig");

    let v = Some(version);
    let (offset_conf, offset_args, offset_recs) = DocArgParam::new_read_offset_config_params(v);
    let (std_conf, std_args, std_recs) = DocArgParam::new_read_std_config_params(v);
    let (layout_conf, layout_args, layout_recs) =
        DocArgParam::new_read_data_schema_config_params(v);
    let (data_conf, data_args, data_recs) = DocArgParam::new_read_events_config_params();
    let (shared_conf, shared_args, shared_recs) = DocArgParam::new_shared_config_params();

    let config_args: Vec<_> = offset_args
        .into_iter()
        .chain(std_args)
        .chain(layout_args)
        .chain(data_args)
        .chain(shared_args)
        .collect();

    let path_param = DocArg::new_path_param(true);
    let header_param = DocArg::new_header_and_supp_param();
    let std_param = DocArg::new_param("std", PyDict::new_std_keywords(), "Standard keywords.");
    let nonstd_param = DocArg::new_param(
        "nonstd",
        PyDict::new_nonstd_keywords(),
        "Non-Standard keywords.",
    );
    let dataset_offset_param = DocArg::new_dataset_offset_param();

    let exc0 = PyException::new_parse_keyval();
    let exc1 = PyException::new_pyreflow(PyreflowError::Relational).desc(format!(
        "If keywords are incompatible with indicated data schema for {DATA} or \
         if keywords that are referenced by other keywords do not exist",
    ));
    let exc2 = PyException::new_event_data();
    let exc3 = PyException::new_extra();

    let xs = [exc0, exc1, exc2, exc3];

    let doc = DocString::new_fun("Make new instance from keywords.")
        .arg(path_param)
        .arg(header_param)
        .arg(std_param)
        .arg(nonstd_param)
        .args(config_args)
        .arg(dataset_offset_param)
        .returns(
            DocReturn::new(PyTuple::new2([
                PyClass::new_coredataset(version),
                PyClass::new_py(["api"], "NewStdDatasetFromKwsOutput"),
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
                let offset = #offset_conf {
                    #(#offset_recs,)*
                    ..#offset_conf::default()
                };
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
                let conf = #core_conf { offset, standard, layout, data, shared };
                let (core, uncore) = #path::new_from_keywords(
                    &path,
                    header.into(),
                    kws,
                    dataset_offset,
                    &conf
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

    let exc0 = PyException::new_segment_overflow(Some(version));
    let exc1 = PyException::new_other_overflow();

    let xs = [exc0, exc1];

    let ret = DocReturn::new(PyOpt::new1(PyInt::new_nextdata()))
        .desc(format!(
            "the value of {NEXTDATA} as written in the last dataset"
        ))
        .exc(xs);

    let doc = DocString::new_fun("Write multiple datasets to path.")
        .para(format!(
            "The resulting file will have {HEADER} and {TEXT} from each object"
        ))
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
                let cs = type_families::Functor::fmap(datasets, |c| c.0);
                Ok(#path::write_texts(&path, &cs[..], &conf)?)
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_coretext_unset_measurements(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let _ = split_ident_version_checked("PyCoreTEXT", &i);
    let s = "Remove measurements and clear data.";
    let p0 = format!(
        "This is equivalent to deleting all {PN_ANY} keywords and setting \
         {PAR} to {zero}.",
        zero = code(0_u8)
    );

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
pub fn impl_coredataset_check_ranges(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let _ = split_ident_version_checked("PyCoreDataset", &i);

    let check_param = DocArg::new_checked_range_datatypes();
    let action_param = DocArg::new_over_range_action();

    let exc = PyException::new_data_loss();

    let ret = DocReturn::new(PyList::new1(PyOpt::new1(RsInt::Usize)))
        .exc([exc])
        .desc(format!(
            "The columns that were overrange. List indices \
             correspond to columns. {NONE} is returned is not truncated. \
             Index of first overrange row is returned."
        ));

    let doc =
        DocString::new_method("Coerce all values in DATA to fit within types specified in layout.")
            .para("This will always create a new copy of DATA in-place.")
            .arg(check_param)
            .arg(action_param)
            .returns(ret);

    let fun_arg = doc.fun_args();
    let inner_arg = doc.idents();
    let ret_path = doc.ret_path();

    quote! {
        #[pymethods]
        impl #i {
            #doc
            fn check_ranges(&mut self, #fun_arg) -> #ret_path {
                self.0.check_ranges(#inner_arg).py_resolve_commutative()
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_core_set_measurements_and_data_schema(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let (is_dataset, version) = split_ident_version_pycore(&i);

    let data_schema = DocArg::new_data_schema_ivar(version);
    let measurements = DocArg::new_measurements_param(version);

    let param_type_set_data_schema =
        DocArg::new_param("data_schema", data_schema.pytype, "The new data schema.");

    let s = if is_dataset {
        " and both must match number of columns in existing dataframe"
    } else {
        ""
    };
    let length_para = format!(
        "Length of {measurements_arg} must match number of columns in {data_schema_arg} {s}.",
        measurements_arg = arg(&measurements.argname),
        data_schema_arg = arg(&data_schema.argname)
    );

    let named_doc = DocString::new_method("Set all measurements, names, and data schema at once.")
        .para(length_para.clone())
        .arg(DocArg::new_set_meas_param(version))
        .arg(param_type_set_data_schema.clone())
        .arg(DocArg::new_allow_shared_names_param())
        .arg(DocArg::new_skip_index_check_param());

    let unnamed_doc = DocString::new_method("Set all measurements and data schema at once.")
        .para(length_para)
        .arg(measurements)
        .arg(param_type_set_data_schema);

    let named_fun_args = named_doc.fun_args();
    let unnamed_fun_args = unnamed_doc.fun_args();

    quote! {
        #[pymethods]
        impl #i {
            #named_doc
            fn set_named_measurements_and_data_schema(&mut self, #named_fun_args) -> PyResult<()> {
                let ret = self.0
                    .set_named_measurements_and_data_schema(
                        measurements.into(),
                        data_schema.into(),
                        allow_shared_names,
                        skip_index_check,
                    )?;
                Ok(ret)
            }

            #unnamed_doc
            fn set_measurements_and_data_schema(&mut self, #unnamed_fun_args) -> PyResult<()> {
                let ms = measurements
                    .into_iter()
                    .map(type_families::BifunctorOnce::bimap_into_once)
                    .collect();
                let ret = self.0.set_measurements_and_data_schema(ms, data_schema.into())?;
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

    let measurements = DocArg::new_measurements_param(version);

    let param_type_set_df =
        DocArg::new_param("data", PyClass::new_dataframe(false), "The new data.");

    let len_para = format!(
        "Length of {measurements_arg} must match number of columns in {data_arg}.",
        measurements_arg = arg(&measurements.argname),
        data_arg = arg(&param_type_set_df.argname)
    );

    let named_doc = DocString::new_method("Set measurements, names, and data at once.")
        .para(len_para.clone())
        .arg(DocArg::new_set_meas_param(version))
        .arg(param_type_set_df.clone())
        .arg(DocArg::new_allow_shared_names_param())
        .arg(DocArg::new_skip_index_check_param());

    let unnamed_doc = DocString::new_method("Set measurements and data at once.")
        .para(len_para)
        .arg(measurements)
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
                        data.into(),
                        allow_shared_names,
                        skip_index_check,
                    )?;
                Ok(ret)
            }

            #unnamed_doc
            fn set_measurements_and_data(&mut self, #unnamed_fun_args) -> PyResult<()> {
                let ms = measurements
                    .into_iter()
                    .map(type_families::BifunctorOnce::bimap_into_once)
                    .collect();
                Ok(self.0.set_measurements_and_data(ms, data.into())?)
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_coredataset_set_measurements_data_schema_and_data(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let version = split_ident_version_checked("PyCoreDataset", &i);

    let measurements = DocArg::new_measurements_param(version);
    let data_schema = DocArg::new_data_schema_ivar(version);

    let param_type_set_data_schema =
        DocArg::new_param("data_schema", data_schema.pytype, "The new data schema.");

    let param_type_set_df =
        DocArg::new_param("data", PyClass::new_dataframe(false), "The new data.");

    let len_para = format!(
        "Length of {measurements_arg} and {data_schema_arg} must match number of columns in {data_arg}.",
        measurements_arg = arg(&measurements.argname),
        data_schema_arg = arg(&data_schema.argname),
        data_arg = arg(&param_type_set_df.argname),
    );

    let doc = DocString::new_method("Set measurements, data schema, and data at once.")
        .para(len_para)
        .arg(measurements)
        .arg(param_type_set_data_schema)
        .arg(param_type_set_df);

    let fun_args = doc.fun_args();

    quote! {
        #[pymethods]
        impl #i {
            #doc
            fn set_measurements_data_schema_and_data(&mut self, #fun_args) -> PyResult<()> {
                let ms = measurements
                    .into_iter()
                    .map(type_families::BifunctorOnce::bimap_into_once)
                    .collect();
                Ok(self.0.set_measurements_data_schema_and_data(ms, data_schema.into(), data.into())?)
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
        .para(format!(
            "This will fully represent an FCS file, as opposed to \
             just representing {HEADER} and {TEXT}."
        ))
        .arg(DocArg::new_data_param(false))
        .arg(DocArg::new_analysis_param(true))
        .arg(DocArg::new_others_param(true))
        .returns(DocReturn::new(PyClass::new_coredataset(version)));

    let fun_args = doc.fun_args();
    let ret_path = doc.ret_path();

    quote! {
        #[pymethods]
        impl #i {
            #doc
            fn to_dataset(&self, #fun_args) -> PyResult<#ret_path> {
                let ret = self.0.clone().into_coredataset(data.into(), analysis, others)?;
                Ok(ret.into())
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
        DocArg::new_meas_kw_ivar1(MeasKw::PnL2_0)
    } else {
        DocArg::new_meas_kw_ivar1(MeasKw::PnL3_1)
    };

    let bin = DocArg::new_meas_kw_ivar1(MeasKw::PKn);
    let size = DocArg::new_meas_kw_ivar1(MeasKw::PKNn);

    let all_peak = [bin, size];

    let filter = DocArg::new_meas_kw_ivar1(MeasKw::PnF);
    let power = DocArg::new_meas_kw_ivar1(MeasKw::PnO);
    let detector_type = DocArg::new_meas_kw_ivar1(MeasKw::PnT);
    let percent_emitted = DocArg::new_meas_kw_ivar1(MeasKw::PnP);
    let detector_voltage = DocArg::new_meas_kw_ivar1(MeasKw::PnV);

    let all_common_optical = [
        filter,
        power,
        detector_type,
        percent_emitted,
        detector_voltage,
    ];

    let calibration3_1 = DocArg::new_meas_kw_ivar(
        MeasKw::PnCALIBRATION3_1,
        Some("Tuple encodes slope and calibration units."),
    );

    let calibration3_2 = DocArg::new_meas_kw_ivar(
        MeasKw::PnCALIBRATION3_2,
        Some("Tuple encodes slope, intercept, and calibration units."),
    );

    let display = DocArg::new_meas_kw_ivar(
        MeasKw::PnD,
        Some(formatcp!(
            "First member of tuple encodes linear or log display \
             ({FALSE} and {TRUE} respectively). The float members encode \
             lower/upper and decades/offset for linear and log scaling respectively.",
        )),
    );

    let analyte = DocArg::new_meas_kw_ivar1(MeasKw::PnANALYTE);
    let feature = DocArg::new_meas_kw_ivar1(MeasKw::PnFEATURE);
    let detector_name = DocArg::new_meas_kw_ivar1(MeasKw::PnDET);
    let tag = DocArg::new_meas_kw_ivar1(MeasKw::PnTAG);
    let measurement_type = DocArg::new_meas_kw_ivar1(MeasKw::PnTYPEOptical);
    let has_type = DocArg::new_meas_kw_ivar1(MeasKw::PnTYPETemporal);
    let has_scale = DocArg::new_meas_kw_ivar1(MeasKw::PnETemporal);

    let timestep = DocArg::new_ivar_rw(
        "timestep",
        PyFloat::new_timestep(),
        format!("Value of {TIMESTEP}."),
        false,
        |_, _| quote!(*self.0.as_ref()),
        |_, _| quote!(*self.0.as_mut() = timestep),
    );

    let longname = DocArg::new_meas_kw_ivar1(MeasKw::PnS);
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

    let s = format!("FCS {version_short} {PN_ANY} keywords for {lower_basename} measurement.");
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
pub fn impl_core_all_pkn(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_meas_attr(&i, MeasKw::PKn)
}

#[proc_macro]
pub fn impl_core_all_pknn(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_meas_attr(&i, MeasKw::PKNn)
}

#[proc_macro]
pub fn impl_core_all_pns(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_meas_attr(&i, MeasKw::PnS)
}

#[proc_macro]
pub fn impl_core_all_pnf(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_meas_attr(&i, MeasKw::PnF)
}

#[proc_macro]
pub fn impl_core_all_pno(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_meas_attr(&i, MeasKw::PnO)
}

#[proc_macro]
pub fn impl_core_all_pnp(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_meas_attr(&i, MeasKw::PnP)
}

#[proc_macro]
pub fn impl_core_all_pnt(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_meas_attr(&i, MeasKw::PnT)
}

#[proc_macro]
pub fn impl_core_all_pnv(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_meas_attr(&i, MeasKw::PnV)
}

#[proc_macro]
pub fn impl_core_all_pnl_old(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_meas_attr(&i, MeasKw::PnL2_0)
}

#[proc_macro]
pub fn impl_core_all_pnl_new(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_meas_attr(&i, MeasKw::PnL3_1)
}

#[proc_macro]
pub fn impl_core_all_pnd(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_meas_attr(&i, MeasKw::PnD)
}

#[proc_macro]
pub fn impl_core_all_pndet(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_meas_attr(&i, MeasKw::PnDET)
}

#[proc_macro]
pub fn impl_core_all_pncal3_1(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_meas_attr(&i, MeasKw::PnCALIBRATION3_1)
}

#[proc_macro]
pub fn impl_core_all_pncal3_2(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_meas_attr(&i, MeasKw::PnCALIBRATION3_2)
}

#[proc_macro]
pub fn impl_core_all_pntag(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_meas_attr(&i, MeasKw::PnTAG)
}

#[proc_macro]
pub fn impl_core_all_pntype(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();

    let opt_pytype = PyStr::default().rstype(keyword_path("OpticalType"));
    let tmp_pytype = PyBool::default().rstype(keyword_path("TemporalType"));

    let inner_opt_rstype = opt_pytype.as_rust_type();
    let inner_tmp_rstype = tmp_pytype.as_rust_type();

    let doc_summary = format!("Value of {PNTYPE} for all measurements.");
    let doc_middle = format!(
        "A bool will be returned for the time measurement where \
         {TRUE} indicates it is set to {time}.",
        time = code_str("Time"),
    );

    let nce_path =
        parse_quote!(fireflow_core::text::named_vec::Element<#inner_tmp_rstype, #inner_opt_rstype>);

    // TODO exception if time channel in the wrong spot
    let full_pytype = PyUnion::new2(opt_pytype, tmp_pytype).rstype(nce_path);

    let doc = DocString::new_ivar(doc_summary, PyList::new1(full_pytype)).para(doc_middle);

    doc.into_impl_get_set(
        &i,
        "all_measurement_types",
        true,
        |_, _| {
            quote! {
                use type_families::{BifunctorOnce as _};
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

    let inner_pytype = PyOpt::new1(PyLiteral::new_awh_feature());

    let inner_rstype = inner_pytype.as_rust_type();

    let doc_summary = format!("Value of {PNFEATURE} (area/width/height) for all measurements.");
    let p0 = format!(
        "This should be the preferred way to get and set this keyword if one \
         knows that only {FEATURE_AREA_STR}, {FEATURE_WIDTH_STR}, and \
         {FEATURE_HEIGHT_STR} will be used for this dataset since it has a \
         well-defined type."
    );
    let p1 = format!("{UNIT} will be returned for the time measurement.");

    let nce_path = parse_quote!(fireflow_core::text::named_vec::NonCenterElement<#inner_rstype>);

    // TODO exception if time channel is in the wrong spot
    let full_pytype = PyUnion::new2(inner_pytype, PyTuple::default()).rstype(nce_path);

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

    let inner_pytype = PyOpt::new1(PyStr::default());
    let inner_rstype = inner_pytype.as_rust_type();

    let doc_summary = format!("Value of {PNFEATURE} (not area/width/height) for all measurements.");
    let p0 = format!(
        "Values which are not {FEATURE_AREA_STR}, {FEATURE_WIDTH_STR}, and \
         {FEATURE_HEIGHT_STR} will be returned as {NONE}."
    );
    let p1 = format!("{UNIT} will be returned for the time measurement.");

    let nce_path = parse_quote!(fireflow_core::text::named_vec::NonCenterElement<#inner_rstype>);

    let full_pytype = PyUnion::new2(inner_pytype, PyTuple::default()).rstype(nce_path);

    let doc = DocString::new_ivar(doc_summary, PyList::new1(full_pytype)).paras([p0, p1]);

    doc.into_impl_get(&i, "all_other_features", |_, _| {
        quote!(
            self.0
                .other_features()
                .map(|x| {
                    type_families::FunctorOnce::fmap_once(x, |y| {
                        type_families::FunctorOnce::fmap_once(y, |z| z.to_owned())
                    })
                })
                .collect()
        )
    })
    .into()
}

#[proc_macro]
pub fn impl_core_all_pnfeature(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_meas_attr(&i, MeasKw::PnFEATURE)
}

#[proc_macro]
pub fn impl_core_all_pnanalyte(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_meas_attr(&i, MeasKw::PnANALYTE)
}

#[proc_macro]
pub fn impl_meas_awh_pnfeature(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();

    let pytype = PyOpt::new1(PyLiteral::new_awh_feature());

    let doc_summary = format!("Value of {PNFEATURE} (area/width/height).");
    let p = format!(
        "This should be the preferred way to get and set this keyword if one \
         knows that only {FEATURE_AREA_STR}, {FEATURE_WIDTH_STR}, and \
         {FEATURE_HEIGHT_STR} will be used since it has a well-defined type."
    );

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

fn core_all_meas_attr(t: &Ident, kw: MeasKw) -> TokenStream {
    let kw_doc = kw.kw();
    let inner_pytype = kw.as_pytype();
    let is_optional = matches!(&inner_pytype, PyType::Option(_));
    let optical_only = kw.optical_only();

    let doc_summary = format!("Value of {kw_doc} for all measurements.");
    let doc_middle = optical_only.then_some(format!(
        "{UNIT} will be returned for time since {kw_doc} is not \
         defined for temporal measurements."
    ));

    let inner_rstype = inner_pytype.as_rust_type();

    let nce_path = parse_quote!(fireflow_core::text::named_vec::NonCenterElement<#inner_rstype>);

    // TODO exception if time channel is in the wrong spot
    let full_pytype = if optical_only {
        PyUnion::new2(inner_pytype, PyTuple::default())
            .rstype(nce_path)
            .into()
    } else {
        inner_pytype
    };

    let doc = DocString::new_ivar(doc_summary, PyList::new1(full_pytype)).paras(doc_middle);

    let second_once = quote!(type_families::BifunctorOnce::second_once);

    let get_optical_body = if is_optional {
        quote! {
            self.0
                .optical_opt()
                .map(|e| #second_once(e.0, |x| x.cloned()).into())
                .collect()
        }
    } else {
        quote! {
            self.0
                .optical::<#inner_rstype>()
                .map(|e| #second_once(e.0, |x| x.clone()).into())
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
        format!("all_{}", kw.fun_plural_name()),
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
    let param_desc = "Choose what happens if conversion would result in data loss. \
                      This is most likely to happen when converting from a later \
                      to an earlier version, as many keywords from the later \
                      version may not exist in the earlier version. There is no \
                      place to keep these values so they must be discarded.";
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
            let param = DocArg::new_allow_loss_param(param_desc);
            let exc0 = PyException::new_pyreflow(PyreflowError::Conversion).desc(format!(
                "If keywords which are unsupported in FCS {vs} exist in current \
                 data and {loss_arg} is {FALSE}",
                loss_arg = arg(&param.argname),
            ));
            let exc1 = PyException::new_pyreflow(PyreflowError::Conversion).desc(format!(
                "If optional keywords are that are missing in current \
                 version are required in FCS {vs}"
            ));
            let target_pytype = target_type.as_rust_type();
            let doc = DocString::new_method(format!("Convert to FCS {vs}."))
                .arg(param)
                .returns(
                    DocReturn::new(target_type)
                        .desc(format!("A new class conforming to FCS {vs}."))
                        .exc([exc0, exc1]),
                );
            quote! {
                #doc
                fn #fn_name(
                    &self,
                    allow_loss: fireflow_core::config::AllowLoss
                ) -> PyResult<#target_pytype> {
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
        format!(
            "The {gme} keyword. {UNIT} means linear scaling and 2-tuple \
             specifies decades and offset for log scaling.",
            gme = fcs_kw("$GmE")
        ),
        false,
        |n, _| quote!(self.0.#n.as_ref().cloned()),
        |n, _| quote!(self.0.#n = #n.into()),
    );

    let make_arg_str = |kw_name: &str, kw_sym: &str, t: &str| {
        let kw_path = keyword_path(t);
        DocArg::new_ivar_rw(
            kw_name,
            PyStr::default().rstype(kw_path),
            format!("The {} keyword.", fcs_kw(format!("$Gm{kw_sym}"))),
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
            format!("The {} keyword.", fcs_kw(format!("$Gm{kw_sym}"))),
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
            format!("The {} keyword.", fcs_kw(format!("$Gm{kw_sym}"))),
            false,
            |n, _| quote!(self.0.#n.as_ref().cloned()),
            |n, _| quote!(self.0.#n = #n),
        )
    };

    let percent_emitted = make_arg_float("percent_emitted", "P", "GatePercentEmitted");
    let detector_voltage = make_arg_float("detector_voltage", "V", "GateDetectorVoltage");

    let shortname_pytype = PyStr::new_shortname().rstype(keyword_path("GateShortname"));
    let shortname = make_arg_opt("shortname", "N", shortname_pytype.into());

    let range_pytype = PyDecimal::new_gate_range();
    let range = make_arg_opt("range", "R", range_pytype.into());

    let summary = format!("The {GM_ANY} keywords for one gated measurement.");
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
pub fn impl_new_fixed_ascii_data_schema(input: TokenStream) -> TokenStream {
    let parsed = parse_macro_input!(input as NamedPath);
    let path = parsed.path;
    let name = parsed.name;
    let bare_path = path_strip_args(path.clone());

    let chars_param = DocArg::new_ivar_ro(
        "ranges",
        PyList::new1(PyInt::new_ascii_range_value()),
        format!(
            "The range for each measurement. Equivalent to {PNR}. The value of \
             {PNB} will be derived from these and will be equivalent to the \
             number of digits for each value."
        ),
        |_, _| quote!(self.0.columns().iter().map(|c| c.value()).collect()),
    );

    let doc = DocString::new_class("A fixed-width ASCII data schema.").arg(chars_param);

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #bare_path::new_ascii_u64(ranges).into()
            }
        }
    };

    let (pyname, class) = doc.into_impl_class(name.value(), &path, new);

    let char_widths_doc =
        DocString::new_ivar("The width of each measurement.", PyList::new1(RsInt::U64)).para(
            format!(
                "Equivalent to {PNB}, which is the number of chars/digits used \
                 to encode data for a given measurement."
            ),
        );

    let char_widths = char_widths_doc.into_impl_get(&pyname, "char_widths", |_, _| {
        quote! {
            type_families::Functor::fmap(self.0.widths(), |x| u64::from(u8::from(x)))
        }
    });

    let datatype = make_data_schema_datatype(&pyname, "A");

    quote! {
        #class
        #char_widths
        #datatype
    }
    .into()
}

#[proc_macro]
pub fn impl_new_delim_ascii_data_schema(input: TokenStream) -> TokenStream {
    let parsed = parse_macro_input!(input as NamedPath);
    let path = &parsed.path;
    let name = parsed.name;
    let bare_path = path_strip_args(path.clone());

    let ranges_param = DocArg::new_ivar_ro(
        "ranges",
        PyList::new1(PyInt::new_delim_ascii_range()),
        format!(
            "The range for each measurement. Equivalent to the {PNR} keyword. \
             This is not used internally."
        ),
        |_, _| {
            quote! {
                let cs: &[_] = self.0.as_ref();
                cs.to_vec()
            }
        },
    );

    let doc = DocString::new_class("A delimited ASCII data schema.").arg(ranges_param);

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #bare_path::new_ascii(ranges).into()
            }
        }
    };

    let (pyname, class) = doc.into_impl_class(name.value(), path, new);
    let datatype = make_data_schema_datatype(&pyname, "A");
    quote!(#class #datatype).into()
}

#[proc_macro]
#[allow(clippy::too_many_lines)]
pub fn impl_new_ordered_float_data_schema(input: TokenStream) -> TokenStream {
    const RANGES: &str = "ranges";
    let parsed = parse_macro_input!(input as SizedDataSchemaPath);
    let path = parsed.named_path.path;
    let name = parsed.named_path.name;
    let nbytes = parsed.nbytes;
    let nbits = nbytes * 8;
    let bare_path = path_strip_args(path.clone());
    let dt = if nbytes == 4 { "F" } else { "D" };

    let summary = format!("{nbits}-bit ordered float data schema.");

    let range_param = DocArg::new_ivar_ro(
        RANGES,
        PyList::new1(PyFloat::new_float_range(nbytes)),
        format!(
            "The range for each measurement. Corresponds to {PNR}. \
             This is not used internally so only serves for users' \
             own purposes.",
        ),
        |_, _| quote!(self.0.columns().iter().map(|c| c.clone()).collect()),
    );

    let byteord_param = DocArg::new_ivar_ro(
        "byteord",
        PyUnion::new_byteord(Some(nbytes)),
        "The byte order to use when encoding values.",
        |_, _| quote!(PyByteOrder::from(self.0.byte_order())),
    )
    .def_auto();

    let make_doc = |args| DocString::new_class(summary).args(args);

    let doc = make_doc(vec![range_param, byteord_param]);
    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #bare_path::new(ranges, byteord.into()).into()
            }
        }
    };
    let (pyname, class) = doc.into_impl_class(name.value(), &path, new);

    let widths = make_byte_width(&pyname, nbytes);
    let datatype = make_data_schema_datatype(&pyname, dt);
    quote!(#class #widths #datatype).into()
}

#[proc_macro]
pub fn impl_new_endian_float_data_schema(input: TokenStream) -> TokenStream {
    let parsed = parse_macro_input!(input as SizedDataSchemaPath);
    let path = parsed.named_path.path;
    let name = parsed.named_path.name;
    let nbytes = parsed.nbytes;
    let nbits = nbytes * 8;
    let bare_path = path_strip_args(path.clone());
    let dt = if nbytes == 4 { "F" } else { "D" };

    let range_param = DocArg::new_ivar_ro(
        "ranges",
        PyList::new1(PyFloat::new_float_range(nbytes)),
        format!(
            "The range for each measurement. Corresponds to {PNR}. This is not \
             used internally."
        ),
        |_, _| quote!(self.0.columns().iter().map(|c| c.clone()).collect()),
    );

    let is_big_param = DocArgROIvar::new_endian_param(4, false);

    let doc = DocString::new_class(format!("{nbits}-bit endian float data schema"))
        .args([range_param, is_big_param]);

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #bare_path::new(ranges, endian).into()
            }
        }
    };

    let (pyname, class) = doc.into_impl_class(name.value(), &path, new);

    let widths = make_byte_width(&pyname, nbytes);
    let datatype = make_data_schema_datatype(&pyname, dt);

    quote!(#class #widths #datatype).into()
}

#[proc_macro]
pub fn impl_new_ordered_uint_data_schema(input: TokenStream) -> TokenStream {
    let parsed = parse_macro_input!(input as NamedPath);
    let name = parsed.name;
    let path = parsed.path;
    let bare_path = path_strip_args(path.clone());

    let ranges_param = DocArg::new_uint_ranges_ivar();
    let width_param = DocArg::new_byte_width_ivar();
    let byteord_param = DocArg::new_ivar_ro(
        "byteord",
        PyUnion::new_byteord(None),
        "The byte order to use when encoding values.",
        |_, _| quote!(self.0.byte_order().into()),
    )
    .def_auto();

    let doc =
        DocString::new_class("An integer data schema with any byte order and a single width.")
            .arg(ranges_param)
            .arg(width_param)
            .arg(byteord_param);

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> PyResult<Self> {
                Ok(#bare_path::new_ordered_uint(ranges, &byte_width, byteord)?.into())
            }
        }
    };

    let (pyname, class) = doc.into_impl_class(name.value(), &path, new);
    let datatype = make_data_schema_datatype(&pyname, "I");
    quote!(#class #datatype).into()
}

#[proc_macro]
pub fn impl_new_single_uint_data_schema(input: TokenStream) -> TokenStream {
    let parsed = parse_macro_input!(input as NamedPath);
    let name = parsed.name;
    let path = parsed.path;
    let bare_path = path_strip_args(path.clone());

    let ranges_param = DocArg::new_uint_ranges_ivar();
    let width_param = DocArg::new_byte_width_ivar();
    let is_big_param = DocArgROIvar::new_endian_param(4, false);

    let doc = DocString::new_class("A mixed-width integer data schema.")
        .arg(ranges_param)
        .arg(width_param)
        .arg(is_big_param);

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> PyResult<Self> {
                Ok(#bare_path::new_single_uint(ranges, &byte_width, endian)?.into())
            }
        }
    };

    let (pyname, class) = doc.into_impl_class(name.value(), &path, new);
    let datatype = make_data_schema_datatype(&pyname, "I");
    quote!(#class #datatype).into()
}

#[proc_macro]
pub fn impl_new_variable_uint_data_schema(input: TokenStream) -> TokenStream {
    let parsed = parse_macro_input!(input as NamedPath);
    let name = parsed.name;
    let path = parsed.path;
    let bare_path = path_strip_args(path.clone());

    let ranges_param: DocArgROIvar = DocArg::new_ivar_ro(
        "ranges",
        PyList::new1(PyTuple::new_variable_bitmask()),
        format!(
            "The range of each measurement. The first member of each tuple \
             indicates the number of bytes ({PNB}) to encode the {PNR} value \
             and data in the column. The second member corresponds to the {PNR} \
             keyword less one."
        ),
        |_, _| quote!(self.0.columns().iter().map(|c| (*c).into()).collect()),
    );

    let is_big_param = DocArgROIvar::new_endian_param(4, false);

    let doc = DocString::new_class("A mixed-width integer data schema.")
        .args([ranges_param, is_big_param]);

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #bare_path::new(ranges, endian).into()
            }
        }
    };

    let (pyname, class) = doc.into_impl_class(name.value(), &path, new);
    let datatype = make_data_schema_datatype(&pyname, "I");
    quote!(#class #datatype).into()
}

#[proc_macro]
pub fn impl_new_mixed_data_schema(input: TokenStream) -> TokenStream {
    let parsed = parse_macro_input!(input as NamedPath);
    let name = parsed.name;
    let path = parsed.path;
    let bare_path = path_strip_args(path.clone());

    let dt_ascii = code("A");
    let dt_int = code("I**");
    let dt_float = code("F**");

    let range_pytype = PyList::new1(PyUnion::new_mixed_range());
    let types_param: DocArgROIvar = DocArg::new_ivar_ro(
        "typed_ranges",
        range_pytype,
        format!(
            "The type and range for each measurement corresponding to {DATATYPE} \
             and/or {PNDATATYPE} and {PNR} respectively. These are given \
             as 2-tuples like {tuple_pattern} where {tuple_first} is one of \
             {dt_ascii}, {dt_int}, or {dt_float} corresponding to Ascii, \
             unsigned integer, or float datatypes respectively. For integers and \
             floats, the {stars} encode the size, which must be 08-64 (in multiples \
             of 8) and 32/64 respectively.",
            tuple_pattern = code("(<type>, <range>)"),
            tuple_first = code("type"),
            stars = code("**"),
        ),
        |_, _| quote!(self.0.columns().iter().map(|c| c.clone()).collect()),
    );

    let is_big_param = DocArgROIvar::new_endian_param(4, false);

    let doc = DocString::new_class("A mixed-type data schema.").args([types_param, is_big_param]);

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #bare_path::new(typed_ranges, endian).into()
            }
        }
    };

    doc.into_impl_class(name.value(), &path, new).1.into()
}

#[proc_macro]
pub fn impl_data_schema_byte_widths(input: TokenStream) -> TokenStream {
    let t = parse_macro_input!(input as Ident);

    let doc = DocString::new_ivar(
        "The width of each measurement in bytes.",
        PyList::new1(RsInt::U32),
    )
    .para(format!(
        "This corresponds to the value of {PNB} for each measurement \
         divided by 8. Values for each measurement may be different."
    ));

    doc.into_impl_get(&t, "byte_widths", |_, _| {
        quote! {
            type_families::Functor::fmap(self.0.widths(), |x| u32::from(u8::from(x)) / 8)
        }
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
#[allow(clippy::too_many_lines)]
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
                 (the {m} in the {GM_ANY} keywords).",
                m = fcs_kw("m")
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
                     measurement (the {m} and {n} in the {GM_ANY} or {PN_ANY} \
                     keywords). {k} be a string like either {gi} or {pi} where \
                     {i} is an integer and the prefix corresponds to a gating or \
                     physical measurement respectively.",
                    m = fcs_kw("m"),
                    n = fcs_kw("n"),
                    gi = code_str("G<I>"),
                    pi = code_str("P<I>"),
                    i = code("<I>"),
                ),
                PyType::from(PyStr::new_meas_or_gate_index()),
            )
        }
        "PrefixedMeasIndex" => (
            "3.2",
            "3_2",
            format!(
                "The {index_name} corresponding to a physical measurement \
                 (the {n} in the {PN_ANY} keywords).",
                n = fcs_kw("n"),
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

/// Macro arg for implementing a python class around a path.
///
/// Unlike using the path directly, the name of the python class does not need
/// to match the path.
struct NamedPath {
    name: LitStr,
    _comma: Comma,
    path: Path,
}

impl Parse for NamedPath {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        Ok(Self {
            name: input.parse()?,
            _comma: input.parse()?,
            path: input.parse()?,
        })
    }
}

/// Macro arg for making data schema classes with a fixed size.
struct SizedDataSchemaPath {
    named_path: NamedPath,
    nbytes: usize,
}

impl Parse for SizedDataSchemaPath {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let named_path = input.parse()?;
        let _: Comma = input.parse()?;
        let nbytes = input
            .parse::<LitInt>()?
            .base10_parse::<usize>()
            .expect("Number of bytes must be an unsigned integer");
        Ok(Self { named_path, nbytes })
    }
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

// /// Macro args for implementing new ordered data schema
// struct OrderedDataSchemaInfo {
//     nbytes: usize,
//     is_float: bool,
// }

// impl Parse for OrderedDataSchemaInfo {
//     fn parse(input: ParseStream) -> syn::Result<Self> {
//         let nbytes = input
//             .parse::<LitInt>()?
//             .base10_parse::<usize>()
//             .expect("Number of bytes must be an unsigned integer");
//         let _: Comma = input.parse()?;
//         let is_float = input.parse::<LitBool>()?.value();
//         Ok(Self { nbytes, is_float })
//     }
// }

/// A docstring for any python function/method/class
#[derive(Clone, new)]
struct DocString<A, R, S> {
    summary: String,
    paragraphs: Vec<String>,
    args: A,
    returns: R,
    _selfarg: PhantomData<S>,
}

type ClassDocString = DocString<Vec<AnyDocArg>, (), NoSelf>;
type MethodDocString = DocString<Vec<DocArgParam>, Option<DocReturn<RetPyType>>, SelfArg>;
type FunDocString = DocString<Vec<DocArgParam>, Option<DocReturn<RetPyType>>, NoSelf>;
type IvarDocString = DocString<(), DocReturn<ArgPyType>, SelfArg>;

/// Represents a method which does not have a self arg
struct NoSelf;

/// Represents a method which has a self arg
struct SelfArg;

/// The origin of a segment
#[derive(Clone, Copy)]
enum SegmentSrc {
    Header,
    Any,
}

/// The origin of a uncorrected segment
#[derive(Clone, Copy)]
enum UncorrSegmentSrc {
    Header,
    Text,
}

/// Any python argument documentation type
#[derive(Clone, From, Display)]
enum AnyDocArg {
    RWIvar(DocArgRWIvar),
    ROIvar(DocArgROIvar),
    Param(DocArgParam),
}

type DocArgRWIvar = DocArg<GetSetMethods>;
type DocArgROIvar = DocArg<GetMethod>;
type DocArgParam = DocArg<NoMethods>;

/// Python documentation for one argument
#[derive(Clone, new, AsRef)]
struct DocArg<T> {
    /// Name of the arg
    #[new(into)]
    argname: String,

    /// Python type of the arg
    #[as_ref(ArgPyType)]
    #[new(into)]
    pytype: ArgPyType,

    /// Description of the arg as to be shown in docs
    #[new(into)]
    desc: String,

    /// Default value of the arg
    default: Option<DocDefault>,

    /// Methods to get/set the arg
    methods: T,
}

/// Denotes that a Python argument does not have get/set methods
#[derive(Clone)]
struct NoMethods;

/// Get methods for a python argument
#[derive(Clone)]
struct GetMethod(TokenStream2);

/// Get and set methods for a python argument
#[derive(new, Clone)]
struct GetSetMethods {
    get: TokenStream2,
    set: TokenStream2,
}

/// Default value for a Python argument
#[derive(Clone)]
enum DocDefault {
    Auto,
    Int(usize),
    Str(String),
}

/// Return value for a python method/function
#[derive(Clone)]
struct DocReturn<T> {
    rtype: T,
    desc: Option<String>,
    // TODO this should always be empty for ivars
    exceptions: Vec<ReturnPyException>,
}

// this is equivalent to returning `()` in Rust and `None` in Python
impl<T> Default for DocReturn<PyType<T>>
where
    PyTuple<T>: Into<PyType<T>>,
{
    fn default() -> Self {
        Self::new(PyTuple::default().into())
    }
}

/// Documentation for a Python exception
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct PyException {
    pyname: String,
    desc: Option<String>,
}

#[derive(Display, Clone, Copy)]
enum PyreflowError {
    #[display("FileLayoutError")]
    FileLayout,
    #[display("ParseKeyError")]
    ParseKey,
    #[display("ParseKeywordValueError")]
    ParseKeywordValue,
    #[display("InvalidKeywordValueError")]
    InvalidKeywordValue,
    #[display("ExtraKeywordError")]
    ExtraKeyword,
    #[display("ConversionError")]
    Conversion,
    #[display("RelationalError")]
    Relational,
    #[display("EventDataError")]
    EventData,
    #[display("DataLossError")]
    DataLoss,
    #[display("ConfigError")]
    Config,
}

impl PyreflowError {
    fn fmt_ref(self) -> String {
        format!(":py:exc:`~pyreflow.{self}`")
    }
}

/// A Python exceptiion returned from a function
#[derive(Clone, PartialEq, Eq, Hash, From)]
struct ReturnPyException(PyException);

/// A Python exceptiion thrown when an argument value is converted into Rust
#[derive(Clone, PartialEq, Eq, Hash, new)]
struct ArgPyException {
    inner: PyException,
    argmod: ExcNameMod,
}

/// A wrapper that modifies the origin name of an exception
#[derive(Clone, PartialEq, Eq, Hash, Default, PartialOrd, Ord)]
enum ExcNameMod {
    #[default]
    NoMod,
    /// For tuples, adds "field 1 in {}"
    Field(NEVec<usize>, Box<Self>),
    /// For lists, adds "any in {}"
    List(Box<Self>),
    /// For dict keys, adds "dict key in {}"
    DictKey(Box<Self>),
    /// For dict keys, adds "dict value in {}"
    DictVal(Box<Self>),
}

/// A Python exception attached to at least one argmenent
#[derive(new)]
struct NamedPyException {
    names: NEVec<String>,
    inner: ArgPyException,
}

/// A Python type associated with an argument or return value
#[derive(Clone, From, Display)]
enum PyType<E> {
    #[from]
    Str(PyStr<E>),
    #[from]
    Bool(PyBool<E>),
    #[from]
    Bytes(PyBytes<E>),
    #[from(RsInt)]
    #[from(PyInt<E>)]
    Int(PyInt<E>),
    #[from(RsFloat)]
    #[from(PyFloat<E>)]
    Float(PyFloat<E>),
    #[from]
    Decimal(PyDecimal<E>),
    #[from]
    Datetime(PyDatetime<E>),
    #[from]
    Date(PyDate<E>),
    #[from]
    Time(PyTime<E>),
    #[from(PyOpt<E>)]
    Option(Box<PyOpt<E>>),
    #[from(PyDict<E>)]
    Dict(Box<PyDict<E>>),
    #[from]
    Tuple(PyTuple<E>),
    #[from(PyList<E>)]
    List(Box<PyList<E>>),
    #[from]
    Literal(PyLiteral),
    #[from]
    PyClass(PyClass<E>),
    #[from(PyUnion<E>)]
    Union(Box<PyUnion<E>>),
}

type ArgPyType = PyType<ArgPyException>;
type RetPyType = PyType<()>;

/// A "broken-down" python type.
///
/// This is mostly used to resolve the ambiguities that arise when dealing with
/// "Option" (in rust) as "X | None" (in Python) and the ugly implications this
/// has with Enums and Unions.
#[derive(PartialEq, Hash, Eq, Clone)]
enum PyAtom<R> {
    Str,
    Bool,
    Bytes,
    Int,
    Float,
    Decimal,
    Datetime,
    Date,
    Time,
    None,
    Dict(Box<Self>, Box<Self>),
    Tuple(Vec<Self>),
    List(Box<Self>),
    Literal(PyLiteral),
    PyClass(PyClass<R>),
    Union(Box<Self>, Box<Self>, Vec<Self>),
}

/// A Python 'int'
#[derive(Clone, new)]
struct PyInt<E> {
    rs: RsInt,
    rstype: Option<Path>,
    exc: Option<E>,
}

/// A Python 'float'
#[derive(Clone, From, new)]
struct PyFloat<E> {
    #[from]
    rs: RsFloat,
    rstype: Option<Path>,
    exc: Option<E>,
}

/// A Python 'str'
#[derive(Clone, Default, new)]
struct PyStr<E> {
    rstype: Option<Path>,
    exc: Option<E>,
}

/// A Python 'bool'
#[derive(Clone, Default, new)]
struct PyBool<E> {
    rstype: Option<Path>,
    exc: Option<E>,
}

/// A Python 'bytes'
#[derive(Clone, Default, new)]
struct PyBytes<E> {
    rstype: Option<Path>,
    exc: Option<E>,
}

/// A Python 'Decimal' class
#[derive(Clone, Default, new)]
struct PyDecimal<E> {
    rstype: Option<Path>,
    exc: Option<E>,
}

/// A Python 'datetime.time' class
#[derive(Clone, Default, new)]
struct PyTime<E> {
    rstype: Option<Path>,
    exc: Option<E>,
}

/// A Python 'datetime.date' class
#[derive(Clone, Default, new)]
struct PyDate<E> {
    rstype: Option<Path>,
    exc: Option<E>,
}

/// A Python 'datetime.datetime' class
#[derive(Clone, Default, new)]
struct PyDatetime<E> {
    rstype: Option<Path>,
    exc: Option<E>,
}

/// A Python 'typing.Literal'
#[derive(Clone, PartialEq, Hash, Eq, new)]
struct PyLiteral {
    #[new(into)]
    head: &'static str,
    #[new(into_iter = "&'static str")]
    tail: Vec<&'static str>,
    #[new(into)]
    rstype: Option<Path>,
}

/// A Python 'Optional[X]' aka 'X | None'
#[derive(Clone, new)]
struct PyOpt<R> {
    #[new(into)]
    inner: PyType<R>,
    #[new(into)]
    rstype: Option<Path>,
    // hack to get the inner default if the rust type above defaults to it
    // rather than None
    default_from_inner: bool,
}

/// A Python 'dict[X, Y]'
#[derive(Clone, new)]
struct PyDict<E> {
    #[new(into)]
    key: PyType<E>,
    #[new(into)]
    value: PyType<E>,
    #[new(into)]
    rstype: Option<Path>,
    exc: Option<E>,
}

/// A Python 'list[X]'
#[derive(Clone, new)]
struct PyList<E> {
    #[new(into)]
    inner: PyType<E>,
    #[new(into)]
    rstype: Option<Path>,
    exc: Option<E>,
}

/// An arbitrary Python class
#[derive(Clone, new, PartialEq, Hash, Eq)]
struct PyClass<E> {
    #[new(into)]
    pyname: String,
    #[new(into)]
    rstype: Option<Path>,
    exc: Option<E>,
}

/// A Python 'Union[...]' aka 'A | B | ...'
#[derive(Clone, new)]
struct PyUnion<E> {
    #[new(into)]
    head0: PyType<E>,
    #[new(into)]
    head1: PyType<E>,
    tail: Vec<PyType<E>>,
    rstype: Option<Path>,
    exc: Option<E>,
}

/// A Python 'tuple[...]'
#[derive(Clone, new)]
struct PyTuple<E> {
    inner: Vec<PyType<E>>,
    rstype: Option<Path>,
    exc: Option<E>,
}

/// A rust integer type for use in making a python int more specific
#[derive(Clone)]
enum RsInt {
    U8,
    U32,
    U64,
    I32,
    I128,
    Usize,
    NonZeroU8,
    NonZeroUsize,
}

/// A rust float type for use in making a python float more specific
#[derive(Clone)]
enum RsFloat {
    F32,
    F64,
}

/// Any segment (not HEADER).
#[derive(Clone, Copy)]
enum AnySegment {
    PrimaryTEXT,
    SuppTEXT,
    Data,
    Analysis,
    Other,
}

/// Any "simple" metaroot keyword that can be accessed with one ivar.
#[derive(Clone, Copy)]
enum Kw {
    Mode,
    Mode3_2,
    Cyt,
    Cyt3_2,
    Abrt,
    Com,
    Cells,
    Exp,
    Fil,
    Inst,
    Lost,
    Op,
    Proj,
    Smno,
    Src,
    Sys,
    Cytsn,
    Unicode,
    CSVBits,
    CSTot,
    LastModifier,
    LastModified,
    Originality,
    Plateid,
    Platename,
    Wellid,
    Vol,
    Flowrate,
    Carrierid,
    Carriertype,
    Locationid,
    UnstainedInfo,
    Spillover,
    UnstainedCenters,
    Tr,
}

/// Any "simple" measurement keyword that can be accessed with one ivar.
#[derive(Clone, Copy)]
enum MeasKw {
    PnETemporal,
    PnS,
    PnF,
    PnL2_0,
    PnL3_1,
    PnO,
    PnT,
    PnP,
    PnV,
    PnCALIBRATION3_1,
    PnCALIBRATION3_2,
    PnD,
    PnDET,
    PnTAG,
    PnTYPETemporal,
    PnTYPEOptical,
    PnFEATURE,
    PnANALYTE,
    PKn,
    PKNn,
}

/// FCS Version
#[derive(PartialEq, Eq, PartialOrd, Clone, Copy)]
enum Version {
    FCS2_0,
    FCS3_0,
    FCS3_1,
    FCS3_2,
}

/// A type which represents 'Self' in python (or not)
trait IsSelfArg {
    const ARG: Option<&'static str>;
}

/// A type which can be converted to a tokenstream representing get/set rust methods
trait IsMethods {
    fn quoted_methods(&self) -> TokenStream2;
}

/// A Python type which has a Rust type (as a quoted path)
trait HasRustPath {
    fn as_rust_type(&self) -> Type;
}

/// Defines argument properties for given methods configurations
trait IsArgType {
    const TYPENAME: &str;
    const ARGTYPE: &str;

    fn readonly() -> Option<bool>;
}

/// General methods for args which may be documented
trait IsDocArg {
    fn argname(&self) -> &str;

    fn pytype(&self) -> &ArgPyType;

    // fn desc(&self) -> &str;

    fn default(&self) -> Option<&DocDefault>;

    fn fun_arg(&self) -> TokenStream2;

    fn ident(&self) -> Ident;

    fn ident_into(&self) -> TokenStream2;

    fn record_into(&self) -> TokenStream2;
}

/// A pytype which may be converted to a pyatom
trait AsPyAtom<R> {
    fn as_atom(&self) -> PyAtom<R>;
}

impl IsSelfArg for NoSelf {
    const ARG: Option<&'static str> = None;
}

impl IsSelfArg for SelfArg {
    const ARG: Option<&'static str> = Some("self");
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

impl<E> HasRustPath for PyType<E> {
    fn as_rust_type(&self) -> Type {
        match self {
            Self::Str(x) => x.as_rust_type(),
            Self::Bool(x) => x.as_rust_type(),
            Self::Bytes(x) => x.as_rust_type(),
            Self::Int(x) => x.as_rust_type(),
            Self::Float(x) => x.as_rust_type(),
            Self::Decimal(x) => x.as_rust_type(),
            Self::Datetime(x) => x.as_rust_type(),
            Self::Date(x) => x.as_rust_type(),
            Self::Time(x) => x.as_rust_type(),
            Self::Option(x) => x.as_rust_type(),
            Self::Dict(x) => x.as_rust_type(),
            Self::List(x) => x.as_rust_type(),
            Self::Tuple(x) => x.as_rust_type(),
            Self::Union(x) => x.as_rust_type(),
            Self::Literal(x) => x.as_rust_type(),
            Self::PyClass(x) => x.as_rust_type(),
        }
    }
}

macro_rules! impl_has_rust_path {
    ($t:ident, $p:path) => {
        impl<E> HasRustPath for $t<E> {
            fn as_rust_type(&self) -> Type {
                if let Some(x) = self.rstype.as_ref() {
                    parse_quote!(#x)
                } else {
                    parse_quote!($p)
                }
            }
        }
    };
}

impl_has_rust_path!(PyStr, String);
impl_has_rust_path!(PyBool, bool);
impl_has_rust_path!(PyBytes, Vec<u8>);
impl_has_rust_path!(PyDecimal, bigdecimal::BigDecimal);
impl_has_rust_path!(PyDate, chrono::NaiveDate);
impl_has_rust_path!(PyTime, chrono::NaiveTime);
impl_has_rust_path!(PyDatetime, chrono::DateTime<chrono::FixedOffset>);

impl<E> HasRustPath for PyOpt<E> {
    fn as_rust_type(&self) -> Type {
        if let Some(x) = self.rstype.as_ref() {
            parse_quote!(#x)
        } else {
            let i = self.inner.as_rust_type();
            parse_quote!(Option<#i>)
        }
    }
}

impl<E> HasRustPath for PyDict<E> {
    fn as_rust_type(&self) -> Type {
        if let Some(x) = self.rstype.as_ref() {
            parse_quote!(#x)
        } else {
            let k = &self.key.as_rust_type();
            let v = &self.value.as_rust_type();
            parse_quote!(hashbrown::HashMap<#k, #v>)
        }
    }
}

impl<E> HasRustPath for PyTuple<E> {
    fn as_rust_type(&self) -> Type {
        if let Some(x) = self.rstype.as_ref() {
            parse_quote!(#x)
        } else {
            let vs: Vec<_> = self.inner.iter().map(HasRustPath::as_rust_type).collect();
            parse_quote!((#(#vs),*))
        }
    }
}

impl<E> HasRustPath for PyList<E> {
    fn as_rust_type(&self) -> Type {
        if let Some(x) = self.rstype.as_ref() {
            parse_quote!(#x)
        } else {
            let v = &self.inner.as_rust_type();
            parse_quote!(Vec<#v>)
        }
    }
}

impl<E> HasRustPath for PyClass<E> {
    fn as_rust_type(&self) -> Type {
        let x = self
            .rstype
            .as_ref()
            .expect("PyClass does not have a rust type");
        parse_quote!(#x)
    }
}

impl HasRustPath for PyLiteral {
    fn as_rust_type(&self) -> Type {
        let x = self
            .rstype
            .as_ref()
            .expect("PyLiteral does not have a rust type");
        parse_quote!(#x)
    }
}

impl<E> HasRustPath for PyUnion<E> {
    fn as_rust_type(&self) -> Type {
        let x = &self
            .rstype
            .as_ref()
            .expect("PyUnion does not have a rust type");
        parse_quote!(#x)
    }
}

macro_rules! impl_prim_num {
    ($t:ident) => {
        impl<E> HasRustPath for $t<E> {
            fn as_rust_type(&self) -> Type {
                if let Some(x) = self.rstype.as_ref() {
                    parse_quote!(#x)
                } else {
                    self.rs.as_rust_type()
                }
            }
        }
    };
}

impl_prim_num!(PyInt);
impl_prim_num!(PyFloat);

impl HasRustPath for RsInt {
    fn as_rust_type(&self) -> Type {
        match self {
            Self::U8 => parse_quote!(u8),
            Self::U32 => parse_quote!(u32),
            Self::U64 => parse_quote!(u64),
            Self::Usize => parse_quote!(usize),
            Self::NonZeroU8 => parse_quote!(std::num::NonZeroU8),
            Self::NonZeroUsize => parse_quote!(std::num::NonZeroUsize),
            Self::I32 => parse_quote!(i32),
            Self::I128 => parse_quote!(i128),
        }
    }
}

impl HasRustPath for RsFloat {
    fn as_rust_type(&self) -> Type {
        match self {
            Self::F32 => parse_quote!(f32),
            Self::F64 => parse_quote!(f64),
        }
    }
}

impl IsArgType for GetMethod {
    const TYPENAME: &str = "vartype";
    const ARGTYPE: &str = "ivar";

    fn readonly() -> Option<bool> {
        Some(true)
    }
}

impl IsArgType for GetSetMethods {
    const TYPENAME: &str = "vartype";
    const ARGTYPE: &str = "ivar";

    fn readonly() -> Option<bool> {
        Some(false)
    }
}

impl IsArgType for NoMethods {
    const TYPENAME: &str = "type";
    const ARGTYPE: &str = "param";

    fn readonly() -> Option<bool> {
        None
    }
}

impl<T> IsDocArg for DocArg<T> {
    fn argname(&self) -> &str {
        self.argname.as_str()
    }

    fn pytype(&self) -> &ArgPyType {
        &self.pytype
    }

    // fn desc(&self) -> &str {
    //     self.desc.as_str()
    // }

    fn default(&self) -> Option<&DocDefault> {
        self.default.as_ref()
    }

    fn fun_arg(&self) -> TokenStream2 {
        let n = format_ident!("{}", &self.argname);
        let t = &self.pytype.as_rust_type();
        quote!(#n: #t)
    }

    fn ident(&self) -> Ident {
        format_ident!("{}", &self.argname)
    }

    fn ident_into(&self) -> TokenStream2 {
        let n = self.ident();
        if unwrap_generic("Option", unwrap_type_as_path(&self.pytype.as_rust_type())).1 {
            quote! {#n.map(Into::into)}
        } else {
            quote! {#n.into()}
        }
    }

    fn record_into(&self) -> TokenStream2 {
        let n = self.ident();
        if unwrap_generic("Option", unwrap_type_as_path(&self.pytype.as_rust_type())).1 {
            quote! {#n: #n.map(Into::into)}
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

    fn pytype(&self) -> &ArgPyType {
        match self {
            Self::RWIvar(x) => x.pytype(),
            Self::ROIvar(x) => x.pytype(),
            Self::Param(x) => x.pytype(),
        }
    }

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

impl<R: Clone> AsPyAtom<R> for PyType<R> {
    fn as_atom(&self) -> PyAtom<R> {
        match self {
            Self::Bool(_) => PyAtom::Bool,
            Self::Bytes(_) => PyAtom::Bytes,
            Self::Str(_) => PyAtom::Str,
            Self::Int(_) => PyAtom::Int,
            Self::Float(_) => PyAtom::Float,
            Self::Decimal(_) => PyAtom::Decimal,
            Self::Date(_) => PyAtom::Date,
            Self::Time(_) => PyAtom::Time,
            Self::Datetime(_) => PyAtom::Datetime,
            Self::Literal(x) => PyAtom::Literal(x.clone()),
            Self::PyClass(x) => PyAtom::PyClass(x.clone()),
            Self::List(x) => x.as_atom(),
            Self::Dict(x) => x.as_atom(),
            Self::Option(x) => x.as_atom(),
            Self::Tuple(x) => x.as_atom(),
            Self::Union(x) => x.as_atom(),
        }
    }
}

impl<R: Clone> AsPyAtom<R> for PyList<R> {
    fn as_atom(&self) -> PyAtom<R> {
        PyAtom::List(self.inner.as_atom().into())
    }
}

impl<R: Clone> AsPyAtom<R> for PyDict<R> {
    fn as_atom(&self) -> PyAtom<R> {
        PyAtom::Dict(self.key.as_atom().into(), self.value.as_atom().into())
    }
}

impl<R: Clone> AsPyAtom<R> for PyOpt<R> {
    fn as_atom(&self) -> PyAtom<R> {
        PyAtom::Union(self.inner.as_atom().into(), PyAtom::None.into(), vec![])
    }
}

impl<R: Clone> AsPyAtom<R> for PyTuple<R> {
    fn as_atom(&self) -> PyAtom<R> {
        PyAtom::Tuple(self.inner.iter().map(AsPyAtom::as_atom).collect())
    }
}

impl<R: Clone> AsPyAtom<R> for PyUnion<R> {
    fn as_atom(&self) -> PyAtom<R> {
        let x0 = self.head0.as_atom();
        let x1 = self.head1.as_atom();
        let xs = self.tail.iter().map(AsPyAtom::as_atom).collect();
        PyAtom::Union(x0.into(), x1.into(), xs)
    }
}

impl<T> DocArg<T> {
    fn quoted_methods(&self) -> TokenStream2
    where
        T: IsMethods,
    {
        self.methods.quoted_methods()
    }

    fn def(self, def: DocDefault) -> Self {
        Self::new(
            self.argname,
            self.pytype,
            self.desc,
            Some(def),
            self.methods,
        )
    }

    fn def_auto(self) -> Self {
        self.def(DocDefault::Auto)
    }

    fn def_auto_if(self, test: bool) -> Self {
        if test { self.def_auto() } else { self }
    }
}

impl GetMethod {
    fn from_pytype(
        name: &str,
        pytype: &ArgPyType,
        f: impl FnOnce(&Ident, &ArgPyType) -> TokenStream2,
    ) -> Self {
        let get = format_ident!("{name}");
        let ret = pytype.as_rust_type();
        let body = f(&get, pytype);
        Self(quote! {
            fn #get(&self) -> #ret {
                #body
            }
        })
    }
}

impl GetSetMethods {
    fn from_pytype(
        name: &str,
        pytype: &ArgPyType,
        fallible: bool,
        f: impl FnOnce(&Ident, &ArgPyType) -> TokenStream2,
        g: impl FnOnce(&Ident, &ArgPyType) -> TokenStream2,
    ) -> Self {
        let i = format_ident!("{name}");
        let set = format_ident!("set_{name}");
        let ret = pytype.as_rust_type();
        let get_body = f(&i, pytype);
        let set_body = g(&i, pytype);
        let success = if fallible {
            quote!(PyResult<()>)
        } else {
            quote!(())
        };
        Self::new(
            quote! {
                fn #i(&self) -> #ret {
                    #get_body
                }
            },
            quote! {
                fn #set(&mut self, #i: #ret) -> #success {
                    #set_body
                }
            },
        )
    }
}

impl<T> DocReturn<T> {
    fn new(rtype: impl Into<T>) -> Self {
        Self {
            rtype: rtype.into(),
            desc: None,
            exceptions: vec![],
        }
    }

    fn desc(self, desc: impl fmt::Display) -> Self {
        Self {
            desc: Some(desc.to_string()),
            ..self
        }
    }

    fn exc(self, exceptions: impl IntoIterator<Item = impl Into<ReturnPyException>>) -> Self {
        Self {
            exceptions: exceptions.into_iter().map(Into::into).collect(),
            ..self
        }
    }
}

impl From<PyException> for ArgPyException {
    fn from(value: PyException) -> Self {
        Self::new(value, ExcNameMod::default())
    }
}

impl From<PyException> for () {
    fn from(_: PyException) {}
}

impl ExcNameMod {
    fn add_field(self, f: usize) -> Self {
        Self::Field(NEVec::new(f), self.into())
    }

    fn add_list(self) -> Self {
        Self::List(self.into())
    }

    fn add_dict_key(self) -> Self {
        Self::DictKey(self.into())
    }

    fn add_dict_val(self) -> Self {
        Self::DictVal(self.into())
    }

    fn fmt(&self, s: &str) -> String {
        match self {
            Self::NoMod => s.to_owned(),
            Self::Field(fs, i) => {
                let xs: Vec<_> = fs.into_iter().map(|x| x + 1).collect();
                let x = fmt_comma_sep_list(&xs[..], "or");
                format!("field {x} in {}", i.fmt(s))
            }
            Self::List(i) => format!("any in {}", i.fmt(s)),
            Self::DictKey(i) => format!("dict key in {}", i.fmt(s)),
            Self::DictVal(i) => format!("dict value in {}", i.fmt(s)),
        }
    }

    fn merge(xs: impl IntoIterator<Item = Self>) -> Vec<Self> {
        // group by top-level types taking field number into account
        let mut has_nomod = false;
        let mut field_trees = vec![];
        let mut list_trees = vec![];
        let mut dict_key_trees = vec![];
        let mut dict_val_trees = vec![];
        for x in xs {
            match x {
                Self::Field(f, t) => field_trees.push((f.into_nonempty_iter().next().0, *t)),
                Self::List(t) => list_trees.push(*t),
                Self::DictKey(t) => dict_key_trees.push(*t),
                Self::DictVal(t) => dict_val_trees.push(*t),
                Self::NoMod => has_nomod = true,
            }
        }

        // if we only have leaves, return early to avoid recursion
        if field_trees.is_empty()
            && list_trees.is_empty()
            && dict_key_trees.is_empty()
            && dict_val_trees.is_empty()
        {
            return has_nomod.then_some(Self::NoMod).into_iter().collect();
        }

        // split trees apart by field number, and group everything underneath
        let mut grouped_field_trees = vec![];

        for (i, ys) in field_trees.into_iter().into_group_map() {
            grouped_field_trees.extend(Self::merge(ys).into_iter().map(|x| (x, i)));
        }

        // now group field by the underlying tree and collect indices
        let grouped_field_trees_ = grouped_field_trees
            .into_iter()
            .into_group_map()
            .into_iter()
            .map(|(tree, fs)| {
                // TODO this could probably be cleaned up
                let fs_ = NEVec::try_from_vec(fs.into_iter().sorted().collect()).unwrap();
                Self::Field(fs_, tree.into())
            });

        // the others are easy, just recurse and collect
        let list_trees_ = Self::merge(list_trees)
            .into_iter()
            .map(Box::new)
            .map(Self::List);
        let dict_key_trees_ = Self::merge(dict_key_trees)
            .into_iter()
            .map(Box::new)
            .map(Self::DictKey);
        let dict_val_trees_ = Self::merge(dict_val_trees)
            .into_iter()
            .map(Box::new)
            .map(Self::DictVal);

        // glue everything together
        has_nomod
            .then_some(Self::NoMod)
            .into_iter()
            .chain(grouped_field_trees_)
            .chain(list_trees_)
            .chain(dict_key_trees_)
            .chain(dict_val_trees_)
            .sorted()
            .collect()
    }
}

impl ArgPyException {
    fn map_mod<F>(self, f: F) -> Self
    where
        F: FnOnce(ExcNameMod) -> ExcNameMod,
    {
        Self::new(self.inner, f(self.argmod))
    }

    fn into_named(self, name: impl Into<String>) -> NamedPyException {
        NamedPyException {
            inner: self,
            names: NEVec::new(name.into()),
        }
    }
}

impl PyException {
    fn new(pyname: impl fmt::Display) -> Self {
        Self {
            pyname: pyname.to_string(),
            desc: None,
        }
    }

    fn new_value() -> Self {
        Self::new("ValueError")
    }

    fn new_key() -> Self {
        Self::new("KeyError")
    }

    fn new_index() -> Self {
        Self::new("IndexError")
    }

    fn new_overflow() -> Self {
        Self::new("OverflowError")
    }

    fn new_segment_overflow(version: Option<Version>) -> Self {
        let overflow_desc = match version {
            None => format!(
                "If {TEXT} (all versions), {DATA} (2.0 only), or {ANALYSIS} \
                 (2.0 only) end offset is greater than 99,999,999 bytes"
            ),
            Some(Version::FCS2_0) => format!(
                "If {TEXT}, {DATA}, or {ANALYSIS} end offset \
                 is greater than 99,999,999 bytes"
            ),
            Some(_) => format!("If {TEXT} ending offset is greater than 99,999,999 bytes"),
        };
        Self::new_overflow().desc(overflow_desc)
    }

    fn new_other_overflow() -> Self {
        let d = format!(
            "If any {OTHER} end offsets are greater than \
             99,999,999 and {big_other} is {FALSE}",
            big_other = arg!(BIG_OTHER)
        );
        Self::new_overflow().desc(d)
    }

    fn new_data_loss() -> Self {
        Self::new_pyreflow(PyreflowError::DataLoss).desc(format!(
            "If any values in {DATA} segment need to be truncated to \
             fit layout data_schema"
        ))
    }

    fn new_pyreflow(p: PyreflowError) -> Self {
        Self::new(format!("~pyreflow.{p}"))
    }

    fn new_invalid_keyword() -> Self {
        Self::new_pyreflow(PyreflowError::InvalidKeywordValue)
    }

    fn new_config() -> Self {
        Self::new_pyreflow(PyreflowError::Config)
    }

    fn new_extra() -> Self {
        Self::new_pyreflow(PyreflowError::ExtraKeyword)
            .desc("If any standard keys are unused and not dropped by some other option")
    }

    fn new_parse_keyval() -> Self {
        Self::new_pyreflow(PyreflowError::ParseKeywordValue)
            .desc("If any keyword values could not be read from their string encoding")
    }

    fn new_event_data() -> Self {
        Self::new_pyreflow(PyreflowError::EventData)
            .desc(format!("If values in {DATA} cannot be read"))
    }

    fn new_existing() -> Self {
        Self::new_pyreflow(PyreflowError::Relational).desc(
            "If keywords are set which refer to measurements and would be \
             invalidated if measurements were removed",
        )
    }

    fn desc(self, desc: impl fmt::Display) -> Self {
        Self {
            desc: Some(desc.to_string()),
            ..self
        }
    }
}

impl NamedPyException {
    // TODO keep arg order when sorting names
    fn merge(xs: impl IntoIterator<Item = Self>) -> Vec<Self> {
        xs.into_iter()
            .map(|x| {
                (
                    (x.names.into_nonempty_iter().next().0, x.inner.inner),
                    x.inner.argmod,
                )
            })
            .into_group_map()
            .into_iter()
            .flat_map(|((name, exc), argmod)| {
                ExcNameMod::merge(argmod)
                    .into_iter()
                    .sorted()
                    .map(|a| ((a, exc.clone()), name.clone()))
                    .collect::<Vec<_>>()
            })
            .into_group_map()
            .into_iter()
            .sorted()
            .map(|((argmod, exc), names)| {
                Self::new(
                    // TODO this could probably be cleaned up
                    NEVec::try_from_vec(names.into_iter().sorted().collect()).unwrap(),
                    ArgPyException::new(exc, argmod),
                )
            })
            .collect()
    }
}

impl<R: Clone + PartialEq + Eq + Hash> PyAtom<R> {
    fn flatten_unions(self) -> Self {
        fn go<Q: Clone + PartialEq + Eq + Hash>(x: PyAtom<Q>) -> NEVec<PyAtom<Q>> {
            match x {
                PyAtom::Union(x0, x1, xs) => {
                    let mut ys = go(*x0);
                    ys.extend(go(*x1));
                    ys.extend(xs.into_iter().flat_map(go));
                    ys
                }
                y => NEVec::new(y.flatten_unions()),
            }
        }
        match self {
            Self::Union(x0, x1, xs) => {
                let mut hasnone = false;
                let mut ys: Vec<_> = go(*x0)
                    .into_iter()
                    .chain(go(*x1))
                    .chain(xs.into_iter().flat_map(go))
                    .filter(|x| {
                        if x == &Self::None {
                            hasnone = true;
                            false
                        } else {
                            true
                        }
                    })
                    .unique()
                    .collect();
                if hasnone {
                    ys.push(Self::None);
                }
                let mut zs = ys.into_iter();
                // ASSUME this won't fail because if we have all Nones then
                // another None should be added
                let n0 = zs.next().unwrap();
                let n1 = zs.next().expect("Tried to flatten union of all 'None'");
                let ns = zs.collect();
                Self::Union(n0.into(), n1.into(), ns)
            }
            Self::List(x) => Self::List(x.flatten_unions().into()),
            Self::Dict(k, v) => Self::Dict(k.flatten_unions().into(), v.flatten_unions().into()),
            Self::Tuple(xs) => Self::Tuple(xs.into_iter().map(Self::flatten_unions).collect()),
            x => x,
        }
    }
}

impl<R> From<RsInt> for PyInt<R> {
    fn from(rs: RsInt) -> Self {
        Self::new(rs, None, None)
    }
}

impl<R> From<RsFloat> for PyFloat<R> {
    fn from(rs: RsFloat) -> Self {
        Self::new(rs, None, None)
    }
}

macro_rules! impl_py_prim_default {
    ($t:ident) => {
        impl<E> $t<E> {
            fn default() -> Self {
                Self::new(None, None)
            }
        }
    };
}

impl_py_prim_default!(PyStr);
impl_py_prim_default!(PyBool);
impl_py_prim_default!(PyBytes);
impl_py_prim_default!(PyDecimal);
impl_py_prim_default!(PyDate);
impl_py_prim_default!(PyTime);
impl_py_prim_default!(PyDatetime);

macro_rules! impl_py_prim_rstype {
    () => {
        fn rstype(self, rstype: Path) -> Self {
            Self::new(Some(rstype), self.exc)
        }
    };
}

macro_rules! impl_py_prim_exc {
    () => {
        fn exc(self, exc: impl Into<E>) -> Self {
            Self::new(self.rstype, Some(exc.into()))
        }
    };
}

macro_rules! impl_py_prim_doc_default {
    ($py:expr, $rs:path) => {
        fn doc_default(&self) -> (String, TokenStream2) {
            (
                $py,
                self.rstype
                    .as_ref()
                    .map_or(quote!($rs::default()), |y| quote!(#y::default()))
            )
        }
    };
}

macro_rules! impl_py_prim_map_exc {
    ($t:ident) => {
        fn map_exc<F: FnOnce(E) -> E1, E1>(self, f: F) -> $t<E1> {
            $t::new(self.rstype, self.exc.map(f))
        }
    };
}

macro_rules! impl_py_num_defaults {
    ($py:expr) => {
        fn doc_default(&self) -> (String, TokenStream2) {
            let rt = self.rs.as_rust_type();
            (
                $py,
                self.rstype
                    .as_ref()
                    .map_or(quote!(#rt::default()), |y| {
                        let z = path_strip_args(y.clone());
                        quote!(#z::default())
                    }),
            )
        }
    };
}

impl<E> PyInt<E> {
    fn rstype(self, rstype: Path) -> Self {
        Self::new(self.rs, Some(rstype), self.exc)
    }

    fn exc(self, exc: impl Into<E>) -> Self {
        Self::new(self.rs, self.rstype, Some(exc.into()))
    }

    fn no_exc(self) -> Self {
        Self::new(self.rs, self.rstype, None)
    }

    fn map_exc<F, E1>(self, f: F) -> PyInt<E1>
    where
        F: FnOnce(E) -> E1,
    {
        PyInt::new(self.rs, self.rstype, self.exc.map(f))
    }

    impl_py_num_defaults!("0".into());
}

impl<E: From<PyException>> PyInt<E> {
    fn new_nextdata() -> Self {
        let p = keyword_path("Nextdata");
        Self::new_int(RsInt::U64).rstype(p)
    }

    fn new_meas_index() -> Self {
        let p = parse_quote!(fireflow_core::text::index::MeasIndex);
        Self::new_nonzero_usize().rstype(p)
    }

    fn new_dataset_offset() -> Self {
        let p = parse_quote!(fireflow_core::config::DatasetOffset);
        Self::new_int(RsInt::U64).rstype(p).no_exc()
    }

    fn new_gate_index() -> Self {
        let p = parse_quote!(fireflow_core::text::index::GateIndex);
        Self::new_nonzero_usize().rstype(p).no_exc()
    }

    fn new_prefixed_meas_index() -> Self {
        let p = parse_quote!(fireflow_core::text::keywords::PrefixedMeasIndex);
        Self::new_nonzero_usize().rstype(p).no_exc()
    }

    fn new_u32() -> Self {
        Self::new_int(RsInt::U32)
    }

    fn new_nonzero_usize() -> Self {
        Self::new_int(RsInt::NonZeroUsize)
    }

    fn new_int(intkind: RsInt) -> Self {
        Self::from(intkind)
    }

    fn new_other_width() -> Self {
        let path = parse_quote!(fireflow_core::validated::ascii_range::OtherWidth);
        let d = format!(
            "if {ARG_TOKEN} is less than {min} and greater than {max}",
            min = code("8"),
            max = code("20")
        );
        let e = PyException::new_config().desc(d);
        Self::new_int(RsInt::NonZeroU8).rstype(path).exc(e)
    }

    fn new_ascii_range_value() -> Self {
        let p = parse_quote!(fireflow_core::validated::ascii_range::AsciiRangeValue);
        Self::new_int(RsInt::U64).rstype(p)
    }

    fn new_delim_ascii_range() -> Self {
        let p = parse_quote!(fireflow_core::validated::ascii_range::DelimAsciiRange);
        Self::new_int(RsInt::U64).rstype(p)
    }

    fn new_full_int_range() -> Self {
        let path = parse_quote!(fireflow_core::data::FullIntRange);
        Self::from(RsInt::U64).rstype(path)
    }
}

impl<E> PyFloat<E> {
    fn rstype(self, rstype: Path) -> Self {
        Self::new(self.rs, Some(rstype), self.exc)
    }

    fn exc(self, exc: impl Into<E>) -> Self {
        Self::new(self.rs, self.rstype, Some(exc.into()))
    }

    fn map_exc<F, E1>(self, f: F) -> PyFloat<E1>
    where
        F: FnOnce(E) -> E1,
    {
        PyFloat::new(self.rs, self.rstype, self.exc.map(f))
    }

    impl_py_num_defaults!("0.0".into());
}

impl<E: From<PyException>> PyFloat<E> {
    fn new_non_negative_float() -> Self {
        let d = format!("if {ARG_TOKEN} is negative, {NAN}, {INF}, or {NEG_INF}");
        let e = PyException::new_invalid_keyword().desc(d);
        Self::from(RsFloat::F32).exc(e)
    }

    fn new_positive_float() -> Self {
        let d = format!(
            "if {ARG_TOKEN} is negative, {zero}, {NAN}, {INF}, or {NEG_INF}",
            zero = code("0.0")
        );
        let e = PyException::new_invalid_keyword().desc(d);
        Self::from(RsFloat::F32).exc(e)
    }

    fn new_float_range(nbytes: usize) -> Self {
        let i = format_ident!("F{:02}Range", nbytes * 8);
        let r = match nbytes {
            4 => RsFloat::F32,
            8 => RsFloat::F64,
            _ => panic!("invalid number of float bytes: {nbytes}"),
        };
        let msg = format!(
            "if {ARG_TOKEN} is {NAN}, {INF}, {NEG_INF}, \
             or outside the bounds of a {}-bit float",
            nbytes * 8,
        );
        let e = PyException::new_invalid_keyword().desc(msg);
        let path = parse_quote!(fireflow_core::data::#i);
        Self::from(r).rstype(path).exc(e)
    }

    fn new_timestep() -> Self {
        let path = keyword_path("Timestep");
        Self::new_positive_float().rstype(path)
    }
}

impl<E> PyStr<E> {
    impl_py_prim_rstype!();
    impl_py_prim_exc!();
    impl_py_prim_doc_default!("\"\"".into(), String);
    impl_py_prim_map_exc!(PyStr);
}

impl<E: From<PyException>> PyStr<E> {
    fn new_shortname() -> Self {
        let path = parse_quote!(fireflow_core::validated::shortname::Shortname);
        let d = format!(
            "if {ARG_TOKEN} is {blank} or contains commas",
            blank = code_str("")
        );
        let e = PyException::new_parse_keyval().desc(d);
        Self::default().rstype(path).exc(e)
    }

    fn new_keystring() -> Self {
        let path: Path = parse_quote!(fireflow_core::validated::keys::KeyString);
        let d = format!("if {ARG_TOKEN} contains non-ASCII characters or is empty");
        let e = PyException::new_pyreflow(PyreflowError::ParseKey).desc(d);
        Self::default().rstype(path).exc(e)
    }

    fn new_keystring_or_pattern() -> Self {
        let path: Path = parse_quote!(fireflow_core::validated::keys::KeyStringOrPattern);
        let d = format!(
            "if {ARG_TOKEN} contains non-ASCII characters, is empty, or is an invalid regex"
        );
        // TODO this exception is wrong for regexp
        let e = PyException::new_pyreflow(PyreflowError::ParseKey).desc(d);
        Self::default().rstype(path).exc(e)
    }

    fn new_ne_truncated_str() -> Self {
        let path = parse_quote!(fireflow_core::validated::keys::TruncatedNEString);
        Self::default().rstype(path)
    }

    fn new_std_keyword() -> Self {
        let path = parse_quote!(fireflow_core::validated::keys::StdKey);
        let d = format!(
            "if {ARG_TOKEN} is empty, does not start with {DOLLAR_STR}, \
             or is only a {DOLLAR_STR}"
        );
        let e = PyException::new_pyreflow(PyreflowError::ParseKey).desc(d);
        Self::default().rstype(path).exc(e)
    }

    fn new_nonstd_keyword() -> Self {
        let path = parse_quote!(fireflow_core::validated::keys::NonStdKey);
        let d = format!("if {ARG_TOKEN} is empty or starts with {DOLLAR_STR}");
        let e = PyException::new_pyreflow(PyreflowError::ParseKey).desc(d);
        Self::default().rstype(path).exc(e)
    }

    fn new_regexp() -> Self {
        let desc = format!(
            "if {ARG_TOKEN} is not a valid regular expression \
             as described in {REGEXP_REF}"
        );
        let exc = PyException::new_config().desc(desc);
        Self::default().exc(exc)
    }

    fn new_meas_or_gate_index() -> Self {
        let path = parse_quote!(fireflow_core::text::keywords::MeasOrGateIndex);
        let d = format!(
            "if {ARG_TOKEN} is not like {px} or {gx} \
             where {x} is an integer one or greater",
            px = code("P<X>"),
            gx = code("P<X>"),
            x = code("X"),
        );
        let e = PyException::new_pyreflow(PyreflowError::ParseKeywordValue).desc(d);
        Self::default().rstype(path).exc(e)
    }

    fn new_ne_str() -> Self {
        let path: Path = parse_quote!(fireflow_types::nonempty_string::NEString);
        Self::new_ne_str_inner(path)
    }

    fn new_ne_str_inner(path: Path) -> Self {
        let d = format!("if {ARG_TOKEN} is empty");
        let e = PyException::new_invalid_keyword().desc(d);
        Self::default().rstype(path).exc(e)
    }
}

impl<E> PyBool<E> {
    impl_py_prim_rstype!();
    impl_py_prim_doc_default!("False".into(), bool);
    impl_py_prim_map_exc!(PyBool);
}

impl<E> PyBytes<E> {
    impl_py_prim_rstype!();
    impl_py_prim_doc_default!("b\"\"".into(), Vec);
    impl_py_prim_map_exc!(PyBytes);
}

impl<E: From<PyException>> PyBytes<E> {
    fn new_analysis() -> Self {
        let r = parse_quote!(fireflow_core::core::Analysis);
        Self::default().rstype(r)
    }
}

impl<E> PyDecimal<E> {
    impl_py_prim_rstype!();
    impl_py_prim_doc_default!("0".into(), bigdecimal::BigDecimal);
    impl_py_prim_map_exc!(PyDecimal);
}

impl<E: From<PyException>> PyDecimal<E> {
    fn new_gate_range() -> Self {
        let path = keyword_path("GateRange");
        Self::default().rstype(path)
    }
}

impl<E> PyDate<E> {
    impl_py_prim_map_exc!(PyDate);
}

impl<E> PyTime<E> {
    impl_py_prim_map_exc!(PyTime);
}

impl<E> PyDatetime<E> {
    impl_py_prim_rstype!();
    impl_py_prim_map_exc!(PyDatetime);
}

impl<E> PyDict<E> {
    fn new1(key: impl Into<PyType<E>>, value: impl Into<PyType<E>>) -> Self {
        Self::new(key.into(), value.into(), None, None)
    }

    fn rstype(self, rstype: Path) -> Self {
        Self::new(self.key, self.value, Some(rstype), self.exc)
    }

    impl_py_prim_doc_default!("{}".into(), hashbrown::HashMap);

    fn map_exc<F: Clone + Fn(E) -> E1, E1>(self, f: F) -> PyDict<E1> {
        PyDict::new(
            self.key.map_exc(f.clone()),
            self.value.map_exc(f.clone()),
            self.rstype,
            self.exc.map(f),
        )
    }
}

impl<E: From<PyException>> PyDict<E> {
    fn new_keystring_pairs() -> Self {
        let path: Path = parse_quote!(fireflow_core::validated::keystring_pairs::KeyStringPairs);
        // TODO exception if dict keys are not unique
        Self::new(PyStr::new_keystring(), PyStr::new_keystring(), path, None)
    }

    fn new_std_keywords() -> Self {
        Self::new1(PyStr::new_std_keyword(), PyStr::new_ne_str())
    }

    fn new_nonstd_keywords() -> Self {
        Self::new1(PyStr::new_nonstd_keyword(), PyStr::new_ne_str())
    }

    fn new_keywords() -> Self {
        Self::new1(PyStr::new_ne_str(), PyStr::new_ne_str())
    }

    fn new_sub_patterns() -> Self {
        let path = config_path("SubPatterns");
        let k = PyStr::new_keystring_or_pattern();
        let v = PyTuple::new_sub_pattern();
        Self::new1(k, v).rstype(path)
    }
}

impl<E> PyList<E> {
    fn new1(inner: impl Into<PyType<E>>) -> Self {
        Self::new(inner, None, None)
    }

    // fn exc(self, exc: impl Into<E>) -> Self {
    //     Self::new(self.inner, self.rstype, Some(exc.into()))
    // }

    fn rstype(self, rstype: Path) -> Self {
        Self::new(self.inner, Some(rstype), self.exc)
    }

    impl_py_prim_doc_default!("[]".into(), Vec);

    fn map_exc<F: Clone + Fn(E) -> E1, E1>(self, f: F) -> PyList<E1> {
        PyList::new(self.inner.map_exc(f.clone()), self.rstype, self.exc.map(f))
    }
}

impl<E: From<PyException>> PyList<E> {
    fn new_non_empty(inner: impl Into<PyType<E>>, inner_path: &Path) -> Self {
        let nonempty = quote!(fireflow_core::nonempty::FcsNEVec);
        let d = format!("if {ARG_TOKEN} is empty");
        let e = PyException::new_invalid_keyword().desc(d);
        Self::new(
            inner,
            Some(parse_quote!(#nonempty<#inner_path>)),
            Some(e.into()),
        )
    }

    fn new_others() -> Self {
        let path: Path = parse_quote!(fireflow_core::core::Others);
        Self::new(PyBytes::default(), path, None)
    }

    fn new_vertices() -> Self {
        let inner_path = keyword_path("Vertex");
        let inner = PyTuple::new2(vec![RsFloat::F32; 2]);
        Self::new_non_empty(inner, &inner_path)
    }

    fn new_key_patterns() -> Self {
        let path = config_path("KeyPatterns");
        Self::new1(PyStr::new_keystring_or_pattern()).rstype(path)
    }

    fn new_overrange_columns() -> Self {
        Self::new1(PyOpt::new1(
            PyTuple::new1(RsInt::Usize).add(PyBool::default()),
        ))
    }
}

impl PyLiteral {
    fn new1(iter: impl IntoIterator<Item = &'static str>) -> Self {
        let mut it = iter.into_iter();
        let head = it.next().expect("Literal cannot be empty");
        Self::new(head, it, None)
    }

    fn rstype(mut self, rstype: Path) -> Self {
        self.rstype = Some(rstype);
        self
    }

    fn new_version() -> Self {
        let path = parse_quote!(fireflow_types::keywords::Version);
        Self::new1(ALL_VERSION_STRINGS).rstype(path)
    }

    fn new_version_override() -> Self {
        let path = config_path("VersionOverride");
        let vs = ALL_VERSION_STRINGS
            .into_iter()
            .chain(tc::VERSION_STRATEGY_ALL_LEVELS);
        Self::new1(vs).rstype(path)
    }

    fn new_temporal_optical_key() -> Self {
        Self::new1(tc::TemporalOpticalKey::iter_str())
            .rstype(parse_quote!(fireflow_core::config::TemporalOpticalKeys))
    }

    fn new_datatype() -> Self {
        let path = parse_quote!(fireflow_core::text::keywords::AlphaNumType);
        Self::new1(["A", "I", "F", "D"]).rstype(path)
    }

    fn new_awh_feature() -> Self {
        let path = parse_quote!(fireflow_types::keywords::OpticalFeature);
        Self::new1(tk::OpticalFeature::iter_str()).rstype(path)
    }

    fn new_endian() -> Self {
        let endian: Path = parse_quote!(fireflow_core::text::byteord::Endian);
        Self::new1([tk::BYTEORD_LITTLE, tk::BYTEORD_BIG]).rstype(endian)
    }

    fn new_scale_fix() -> Self {
        Self::new1([
            tk::SCALE_DIAGNOSTIC_TRIMMED,
            tk::SCALE_DIAGNOSTIC_LOG,
            tk::SCALE_DIAGNOSTIC_TRIMMED_LOG,
            tk::SCALE_DIAGNOSTIC_FORCED,
        ])
    }

    fn new_gate_scale_fix() -> Self {
        Self::new1([
            tk::SCALE_DIAGNOSTIC_TRIMMED,
            tk::SCALE_DIAGNOSTIC_LOG,
            tk::SCALE_DIAGNOSTIC_TRIMMED_LOG,
        ])
    }

    fn new_tri_flag(name: &str) -> Self {
        let path = config_path(name);
        Self::new1(tc::TriFlag::iter_str()).rstype(path)
    }

    fn new_integer_width() -> Self {
        let path = parse_quote!(fireflow_types::python::IntegerWidth);
        Self::new1(IntegerWidth::iter_str()).rstype(path)
    }

    fn new_column_type() -> Self {
        let path = parse_quote!(fireflow_types::python::ColumnType);
        Self::new1(ColumnType::iter_str()).rstype(path)
    }
}

impl<E> PyOpt<E> {
    fn new1(x: impl Into<PyType<E>>) -> Self {
        Self::new(x, None, false)
    }

    fn rstype(self, rstype: Path) -> Self {
        Self::new(self.inner, Some(rstype), self.default_from_inner)
    }

    fn default_from_inner(self) -> Self {
        Self::new(self.inner, self.rstype, true)
    }

    fn doc_default(&self) -> (String, TokenStream2) {
        if self.default_from_inner {
            match self.rstype.as_ref() {
                None => self.inner.doc_default(),
                Some(rs) => (self.inner.doc_default().0, quote!(#rs::default())),
            }
        } else {
            ("None".into(), quote!(None))
        }
    }

    fn map_exc<F: Clone + Fn(E) -> E1, E1>(self, f: F) -> PyOpt<E1> {
        PyOpt::new(self.inner.map_exc(f), self.rstype, self.default_from_inner)
    }
}

impl<E: From<PyException>> PyOpt<E> {
    fn new_scale_fix() -> Self {
        let path = keyword_path("AnyMeasScaleFix");
        let inner = PyTuple::new1(PyStr::new_ne_str()).add(PyLiteral::new_scale_fix());
        Self::new1(inner).rstype(path)
    }

    fn new_gate_scale_fix() -> Self {
        let path = keyword_path("ScaleFix");
        let inner = PyTuple::new1(PyStr::new_ne_str()).add(PyLiteral::new_gate_scale_fix());
        Self::new1(inner).rstype(path)
    }
}

impl<E> Default for PyTuple<E> {
    fn default() -> Self {
        Self::new(vec![], None, None)
    }
}

impl<E> PyTuple<E> {
    fn new1(x: impl Into<PyType<E>>) -> Self {
        Self::new(vec![x.into()], None, None)
    }

    fn new2(iter: impl IntoIterator<Item = impl Into<PyType<E>>>) -> Self {
        Self::new(iter.into_iter().map(Into::into).collect(), None, None)
    }

    fn add(mut self, x: impl Into<PyType<E>>) -> Self {
        self.inner.push(x.into());
        self
    }

    fn rstype(self, rstype: Path) -> Self {
        Self::new(self.inner, Some(rstype), self.exc)
    }

    fn exc(self, exc: impl Into<E>) -> Self {
        Self::new(self.inner, self.rstype, Some(exc.into()))
    }

    fn map_exc<F: Clone + Fn(E) -> E1, E1>(self, f: F) -> PyTuple<E1> {
        PyTuple::new(
            self.inner
                .into_iter()
                .map(|x| x.map_exc(f.clone()))
                .collect(),
            self.rstype,
            self.exc.map(f),
        )
    }
}

impl<E: From<PyException>> PyTuple<E> {
    fn new_sub_pattern() -> Self {
        let desc = format!(
            "if references in replacement string in {ARG_TOKEN} \
             do not match captures in regular expression"
        );
        let exc = PyException::new_config().desc(desc);
        Self::new1(PyStr::new_regexp())
            .add(PyStr::default())
            .add(PyBool::default())
            .exc(exc)
    }

    fn new_uncorrected_segment() -> Self {
        let p = parse_quote!(fireflow_core::segment::UncorrectedSegment);
        Self::new2(vec![RsInt::I128; 2]).rstype(p)
    }

    fn new_segment(n: &str) -> Self {
        let t = format_ident!("{n}");
        let p = parse_quote!(fireflow_core::segment::#t);
        let desc = format!(
            "if {ARG_TOKEN} has offsets which exceed the end of the file, \
             are inverted (begin after end), or are either negative \
             or greater than {max}",
            max = code("2**64-1")
        );
        let exc = PyException::new_value().desc(desc);
        // NOTE don't use ints with overflow exceptions since this is captured
        // in the overall exception for the entire type
        Self::new2(vec![RsInt::U64; 2]).exc(exc).rstype(p)
    }

    fn new_text_segment() -> Self {
        Self::new_segment("PrimaryTextSegment")
    }

    fn new_supp_text_segment() -> Self {
        Self::new_segment("SupplementalTextSegment")
    }

    fn new_other_segment() -> Self {
        Self::new_segment("OtherSegment20")
    }

    fn new_data_segment(src: SegmentSrc) -> Self {
        let id = match src {
            SegmentSrc::Header => "HeaderDataSegment",
            SegmentSrc::Any => "AnyDataSegment",
        };
        Self::new_segment(id)
    }

    fn new_analysis_segment(src: SegmentSrc) -> Self {
        let id = match src {
            SegmentSrc::Header => "HeaderAnalysisSegment",
            SegmentSrc::Any => "AnyAnalysisSegment",
        };
        Self::new_segment(id)
    }

    fn new_correction(seg: AnySegment, is_header: bool) -> Self {
        let path = seg.correction_path(is_header);
        Self::new2([PyInt::new_int(RsInt::I32), PyInt::new_int(RsInt::I32)]).rstype(path)
    }

    fn new_meas(version: Version) -> Self {
        let name_pytype = PyType::new_versioned_shortname(version);
        let name_rstype = name_pytype.as_rust_type();
        let meas_opt_pyname = pyoptical(version);
        let meas_tmp_pyname = pytemporal(version);
        let meas_argtype =
            parse_quote!(PyEithers<#name_rstype, #meas_tmp_pyname, #meas_opt_pyname>);
        Self::new1(name_pytype)
            .add(PyUnion::new_measurement_nopath(version))
            .rstype(meas_argtype)
    }

    fn new_unigate() -> Self {
        Self::new2([PyDecimal::default(), PyDecimal::default()]).rstype(keyword_path("UniGate"))
    }

    fn new_variable_bitmask() -> Self {
        let path = quote!(fireflow_core::data::VariableBitmask);
        Self::new1(PyLiteral::new_integer_width())
            .add(RsInt::U64)
            .rstype(parse_quote!(#path))
    }
}

impl<T, E> FromIterator<T> for PyUnion<E>
where
    T: Into<PyType<E>>,
{
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut it = iter.into_iter();
        let x0 = it.next().expect("Union cannot be empty");
        let x1 = it.next().expect("Union must have at least 2 types");
        let xs = it.map(Into::into).collect();
        Self::new(x0, x1, xs, None, None)
    }
}

impl<E> PyUnion<E> {
    fn new2(x: impl Into<PyType<E>>, y: impl Into<PyType<E>>) -> Self {
        Self::new(x, y, vec![], None, None)
    }

    fn rstype(mut self, rstype: Path) -> Self {
        self.rstype = Some(rstype);
        self
    }

    fn exc(self, exc: impl Into<E>) -> Self {
        Self::new(
            self.head0,
            self.head1,
            self.tail,
            self.rstype,
            Some(exc.into()),
        )
    }

    fn map_exc<F: Clone + Fn(E) -> E1, E1>(self, f: F) -> PyUnion<E1> {
        PyUnion::new(
            self.head0.map_exc(f.clone()),
            self.head1.map_exc(f.clone()),
            self.tail
                .into_iter()
                .map(|x| x.map_exc(f.clone()))
                .collect(),
            self.rstype,
            self.exc.map(f),
        )
    }
}

impl<E: From<PyException>> PyUnion<E> {
    fn new_measurement_nopath(version: Version) -> Self {
        Self::new2(
            PyClass::new_optical(version),
            PyClass::new_temporal(version),
        )
    }

    fn new_measurement(version: Version) -> Self {
        let element_path = element_path(version);
        Self::new_measurement_nopath(version).rstype(element_path)
    }

    fn new_scale(is_gate: bool) -> Self {
        let name = if is_gate { "GateScale" } else { "Scale" };
        let d = format!("if {ARG_TOKEN} has log scale floats which are not both positive");
        let exc = PyException::new_invalid_keyword().desc(d);
        Self::new2(PyTuple::default(), PyTuple::new2(vec![RsFloat::F32; 2]))
            .rstype(keyword_path(name))
            .exc(exc)
    }

    fn new_transform() -> Self {
        let path = parse_quote! {fireflow_core::core::ScaleTransform};
        let d = format!("if {ARG_TOKEN} has log scale floats which are not both positive");
        let exc = PyException::new_invalid_keyword().desc(d);
        // TODO the linear gain should also be positive
        Self::new2(RsFloat::F32, PyTuple::new2(vec![RsFloat::F32; 2]))
            .rstype(path)
            .exc(exc)
    }

    fn new_byteord(nbytes: Option<usize>) -> Self {
        let (path, exc_desc) = if let Some(n) = nbytes {
            let sizedbyteord_path: Path = parse_quote!(PyByteOrder);
            let p = parse_quote!(#sizedbyteord_path<#n>);
            let d = format!(
                "if {ARG_TOKEN} is not {BYTEORD_LITTLE_STR}, {BYTEORD_BIG_STR}, \
                 or a list of all integers from 1 to {n} in any order"
            );
            (p, d)
        } else {
            let p = parse_quote!(fireflow_core::text::byteord::AnyByteOrder);
            let d = format!(
                "if {ARG_TOKEN} is not {BYTEORD_LITTLE_STR}, {BYTEORD_BIG_STR}, \
                 or a list of unique integers in any order"
            );
            (p, d)
        };
        let exc = PyException::new_invalid_keyword().desc(exc_desc);
        Self::new2(PyLiteral::new_endian(), PyList::new1(RsInt::U32))
            .rstype(path)
            .exc(exc)
    }

    fn new_anycoretext() -> Self {
        ALL_VERSIONS
            .into_iter()
            .map(PyClass::new_coretext)
            .collect::<Self>()
            .rstype(parse_quote!(PyAnyCoreTEXT))
    }

    fn new_anycoredataset() -> Self {
        ALL_VERSIONS
            .into_iter()
            .map(PyClass::new_coredataset)
            .collect::<Self>()
            .rstype(parse_quote!(PyAnyCoreDataset))
    }

    fn new_ne_string_or_bytes() -> Self {
        let path = parse_quote!(fireflow_core::validated::keys::NEStringOrBytes);
        Self::new2(PyStr::default(), PyBytes::default()).rstype(path)
    }

    fn new_string_or_bytes() -> Self {
        let path = parse_quote!(fireflow_core::validated::keys::StringOrBytes);
        Self::new2(PyStr::default(), PyBytes::default()).rstype(path)
    }

    fn new_key_or_bytes() -> Self {
        let path = parse_quote!(fireflow_core::validated::keys::KeyOrBytes);
        Self::new2(PyStr::default(), PyBytes::default()).rstype(path)
    }

    fn new_mixed_range() -> Self {
        let path = quote!(fireflow_core::data::MixedRange);
        Self::new2(
            PyTuple::new1(PyLiteral::new1(
                IntegerWidth::iter_str().chain([COL_TYPE_ASCII.as_str()]),
            ))
            .add(RsInt::U64),
            PyTuple::new1(PyLiteral::new1([
                COL_TYPE_F32.as_str(),
                COL_TYPE_F64.as_str(),
            ]))
            .add(RsFloat::F64),
        )
        .rstype(parse_quote!(#path))
    }

    fn new_any_range(version: Version) -> Self {
        match version {
            Version::FCS2_0 | Version::FCS3_0 => Self::new_full_range(),
            Version::FCS3_1 => Self::new_range_or_bitmask_range(),
            Version::FCS3_2 => Self::new_range_or_mixed_range(),
        }
    }

    fn new_full_range() -> Self {
        let path = parse_quote!(fireflow_core::data::FullRange);
        Self::new2(RsInt::U64, RsFloat::F64).rstype(path)
    }

    fn new_range_or_bitmask_range() -> Self {
        let path = quote!(fireflow_core::data::MaybeTypedVariableBitmask);
        let ints = PyTuple::new1(PyLiteral::new1(IntegerWidth::iter_str()))
            .add(RsInt::U64)
            .into();
        let rng = PyType::from(Self::new_full_range());
        Self::from_iter([ints, rng]).rstype(parse_quote!(#path))
    }

    fn new_range_or_mixed_range() -> Self {
        let path = quote!(fireflow_core::data::MaybeTypedMixedRange);
        let mixed = PyType::from(Self::new_mixed_range());
        let rng = PyType::from(Self::new_full_range());
        Self::from_iter([mixed, rng]).rstype(parse_quote!(#path))
    }
}

impl<E> PyClass<E> {
    fn new1(pyname: impl fmt::Display) -> Self {
        Self::new(pyname.to_string(), None, None)
    }

    fn rstype(self, rstype: Path) -> Self {
        Self::new(self.pyname, Some(rstype), None)
    }

    fn new_py(
        modpath: impl IntoIterator<Item = impl fmt::Display>,
        name: impl fmt::Display,
    ) -> Self {
        let pyname = format_ident!("Py{name}");
        let m = once("~pyreflow".into())
            .chain(modpath.into_iter().map(|x| x.to_string()))
            .chain([format!("{name}")])
            .join(".");
        Self::new1(m).rstype(parse_quote!(#pyname))
    }

    fn exc(self, e: impl Into<E>) -> Self {
        Self::new(self.pyname, self.rstype, Some(e.into()))
    }

    fn map_exc<F: FnOnce(E) -> E1, E1>(self, f: F) -> PyClass<E1> {
        PyClass::new(self.pyname, self.rstype, self.exc.map(f))
    }
}

impl<E: From<PyException>> PyClass<E> {
    fn new_optical(version: Version) -> Self {
        let n = format!("Optical{}", version.short_underscore());
        Self::new_py([""; 0], n)
    }

    fn new_temporal(version: Version) -> Self {
        let n = format!("Temporal{}", version.short_underscore());
        Self::new_py([""; 0], n)
    }

    fn new_dataframe(polars_type: bool) -> Self {
        let path = if polars_type {
            parse_quote!(pyo3_polars::PyDataFrame)
        } else {
            // ASSUME this is in scope
            parse_quote!(PyFCSDataFrame)
        };
        Self::new1("polars.DataFrame").rstype(path)
    }

    fn new_series() -> Self {
        // ASSUME this is in scope
        let path: Path = parse_quote!(PyAnyFCSColumn);
        Self::new1("polars.Series").rstype(path)
    }

    fn new_coretext(version: Version) -> Self {
        let v = version.short_underscore();
        Self::new_py([""; 0], format!("CoreTEXT{v}"))
    }

    fn new_coredataset(version: Version) -> Self {
        let v = version.short_underscore();
        Self::new_py([""; 0], format!("CoreDataset{v}"))
    }
}

impl<E> PyType<E> {
    fn map_exc<F: Clone + Fn(E) -> E1, E1>(self, f: F) -> PyType<E1> {
        match self {
            Self::Bool(x) => x.map_exc(f).into(),
            Self::Bytes(x) => x.map_exc(f).into(),
            Self::Str(x) => x.map_exc(f).into(),
            Self::Int(x) => x.map_exc(f).into(),
            Self::Float(x) => x.map_exc(f).into(),
            Self::Decimal(x) => x.map_exc(f).into(),
            Self::List(x) => x.map_exc(f).into(),
            Self::Dict(x) => x.map_exc(f).into(),
            Self::Date(x) => x.map_exc(f).into(),
            Self::Time(x) => x.map_exc(f).into(),
            Self::Datetime(x) => x.map_exc(f).into(),
            Self::PyClass(x) => x.map_exc(f).into(),
            Self::Option(x) => x.map_exc(f).into(),
            Self::Union(x) => x.map_exc(f).into(),
            Self::Tuple(xs) => xs.map_exc(f).into(),
            Self::Literal(x) => x.into(),
        }
    }

    fn doc_default(&self) -> (String, TokenStream2) {
        match self {
            Self::Bool(x) => x.doc_default(),
            Self::Bytes(x) => x.doc_default(),
            Self::Str(x) => x.doc_default(),
            Self::Int(x) => x.doc_default(),
            Self::Float(x) => x.doc_default(),
            Self::Decimal(x) => x.doc_default(),
            Self::List(x) => x.doc_default(),
            Self::Dict(x) => x.doc_default(),
            Self::Option(x) => x.doc_default(),
            Self::Literal(x) => {
                let rt = &x.rstype;
                (format!("\"{}\"", x.head), quote!(#rt::default()))
            }
            Self::Union(x) => {
                let rt = x.rstype.as_ref().map(|p| path_strip_args(p.clone()));
                let (pt, _) = x.head0.doc_default();
                (pt, quote!(#rt::default()))
            }
            Self::Tuple(xs) => {
                let (ps, rs): (Vec<_>, Vec<_>) = xs.inner.iter().map(Self::doc_default).unzip();
                (
                    format!("({})", ps.into_iter().join(", ")),
                    xs.rstype.as_ref().map_or(quote!((#(#rs),*)), |y| {
                        let z = path_strip_args(y.clone());
                        quote!(#z::default())
                    }),
                )
            }
            Self::Date(_) => panic!("No default for date"),
            Self::Time(_) => panic!("No default for time"),
            Self::Datetime(_) => panic!("No default for datetime"),
            Self::PyClass(_) => panic!("No default for arbitrary class"),
        }
    }
}

impl<E: From<PyException>> PyType<E> {
    fn new_versioned_shortname(version: Version) -> Self {
        if version < Version::FCS3_1 {
            PyOpt::new1(PyStr::new_shortname()).into()
        } else {
            let inner = quote!(fireflow_core::validated::shortname::Shortname);
            let outer = parse_quote!(fireflow_core::text::optional::Identity<#inner>);
            PyStr::new_shortname().rstype(outer).into()
        }
    }
}

impl ArgPyType {
    fn as_exceptions(&self) -> Vec<ArgPyException> {
        let go = |e: &Option<ArgPyException>| e.iter().cloned().collect();
        let walk = |mut acc: Vec<ArgPyException>, pt: &Self| {
            acc.extend(pt.as_exceptions());
            acc
        };
        // TODO clean this up
        match self {
            Self::Bool(x) => go(&x.exc),
            Self::Bytes(x) => go(&x.exc),
            Self::Str(x) => go(&x.exc),
            Self::Int(x) => go(&x.exc),
            Self::Float(x) => go(&x.exc),
            Self::Decimal(x) => go(&x.exc),
            Self::Date(x) => go(&x.exc),
            Self::Time(x) => go(&x.exc),
            Self::Datetime(x) => go(&x.exc),
            Self::PyClass(x) => go(&x.exc),
            Self::Option(x) => walk(vec![], &x.inner),
            Self::Union(x) => {
                let acc0 = x.exc.iter().cloned().collect();
                let acc1 = walk(walk(acc0, &x.head0), &x.head1);
                x.tail.iter().fold(acc1, walk)
            }
            Self::List(x) => {
                let y = x
                    .inner
                    .clone()
                    .map_exc(|e| e.map_mod(ExcNameMod::add_list))
                    .as_exceptions();
                x.exc.iter().cloned().chain(y).collect()
            }
            Self::Dict(x) => {
                let k = x
                    .key
                    .clone()
                    .map_exc(|e| e.map_mod(ExcNameMod::add_dict_key))
                    .as_exceptions();
                let v = x
                    .value
                    .clone()
                    .map_exc(|e| e.map_mod(ExcNameMod::add_dict_val))
                    .as_exceptions();
                x.exc.iter().cloned().chain(k).chain(v).collect()
            }
            Self::Tuple(xs) => {
                let fmt = |i, x: Self| x.map_exc(|e| e.map_mod(|m| ExcNameMod::add_field(m, i)));
                let mut ys = xs.inner.iter().cloned().enumerate().map(|(i, x)| fmt(i, x));
                let acc0 = xs.exc.clone().into_iter().collect();
                if let Some(y) = ys.next() {
                    let acc = walk(acc0, &y);
                    ys.fold(acc, |a, x| walk(a, &x))
                } else {
                    acc0
                }
            }
            Self::Literal(_) => vec![],
        }
    }
}

// impl RsInt {
//     fn lower(&self) -> &'static str {
//         match self {
//             Self::U8 | Self::U16 | Self::U32 | Self::U64 | Self::Usize => "0",
//             Self::NonZeroU8 | Self::NonZeroUsize => "1",
//             Self::I32 => "-2**31",
//             Self::I128 => "-2**127",
//         }
//     }

//     fn upper(&self) -> String {
//         match self {
//             Self::U8 | Self::NonZeroU8 => "255".into(),
//             Self::U16 => "2**16-1".into(),
//             Self::U32 => "2**32-1".into(),
//             Self::I32 => "2**31-1".into(),
//             Self::I128 => "2**127-1".into(),
//             Self::U64 => "2**64-1".into(),
//             Self::Usize | Self::NonZeroUsize => format!("2**{}-1", usize::BITS),
//         }
//     }

//     fn exc_desc(&self) -> String {
//         format!(
//             "if {ARG_TOKEN} is less than {} or greater than {}",
//             code(self.lower()),
//             code(self.upper())
//         )
//     }
// }

impl DocArgRWIvar {
    fn new_ivar_rw(
        argname: impl fmt::Display,
        pytype: impl Into<ArgPyType>,
        desc: impl fmt::Display,
        fallible: bool,
        f: impl FnOnce(&Ident, &ArgPyType) -> TokenStream2,
        g: impl FnOnce(&Ident, &ArgPyType) -> TokenStream2,
    ) -> Self {
        DocArgParam::new_param(argname, pytype, desc).into_rw(fallible, f, g)
    }

    fn new_opt_ivar_rw(
        argname: impl fmt::Display,
        pytype: impl Into<ArgPyType>,
        desc: impl fmt::Display,
        fallible: bool,
        f: impl FnOnce(&Ident, &ArgPyType) -> TokenStream2,
        g: impl FnOnce(&Ident, &ArgPyType) -> TokenStream2,
    ) -> Self {
        let pt = PyOpt::new1(pytype.into());
        Self::new_ivar_rw(argname, pt, desc, fallible, f, g).def_auto()
    }

    fn new_kw_ivar(kw: Kw, def: bool) -> Self {
        let path = kw.type_name();

        let get_f = |_: &Ident, pt: &ArgPyType| {
            let optional = matches!(pt, PyType::Option(_));
            let get_inner = format_ident!("{}", if optional { "metaroot_opt" } else { "metaroot" });
            let clone_inner = format_ident!("{}", if optional { "cloned" } else { "clone" });
            quote!(self.0.#get_inner::<#path>().#clone_inner())
        };
        let set_f = |n: &Ident, _: &ArgPyType| quote!(self.0.set_metaroot(#n));

        DocArgParam::new_kw_param(kw, None, def).into_rw(false, get_f, set_f)
    }

    fn new_kw_ivar1(kw: Kw) -> Self {
        Self::new_kw_ivar(kw, true)
    }

    fn new_meas_kw_ivar(kw: MeasKw, desc: Option<&str>) -> Self {
        let pytype = kw.as_pytype();
        let full_path = pytype.as_rust_type();

        let preamble = format!("Value of {}.", kw.kw());
        let d = match desc {
            None => preamble,
            Some(d) => format!("{preamble} {d}"),
        };

        let get_f = |_: &Ident, pt: &ArgPyType| {
            if matches!(pt, PyType::Option(_)) {
                quote! {
                    let x: &#full_path = self.0.as_ref();
                    x.as_ref().cloned()
                }
            } else {
                quote! {
                    let x: &#full_path = self.0.as_ref();
                    x.clone()
                }
            }
        };
        let set_f = |n: &Ident, _: &ArgPyType| quote!(*self.0.as_mut() = #n);

        Self::new_ivar_rw(kw.fun_singular_name(), pytype, d, false, get_f, set_f).def_auto()
    }

    fn new_meas_kw_ivar1(kw: MeasKw) -> Self {
        Self::new_meas_kw_ivar(kw, None)
    }

    fn new_data_schema_ivar(version: Version) -> Self {
        let ascii_schema = ["FixedAsciiDataSchema", "DelimAsciiHeaders"];
        let non_mixed_schema = [
            "VariableUintDataSchema",
            "SingleUintDataSchema",
            "EndianF32DataSchema",
            "EndianF64DataSchema",
        ];
        let ordered_schema = [
            "OrderedUintDataSchema",
            "OrderedF32DataSchema",
            "OrderedF64DataSchema",
        ];

        let data_schema_pytype = match version {
            Version::FCS3_2 => {
                let u: PyUnion<_> = ascii_schema
                    .into_iter()
                    .chain(non_mixed_schema)
                    .chain(["MixedDataSchema"])
                    .map(PyClass::new1)
                    .collect();
                u.rstype(parse_quote!(PyDataSchema3_2))
            }
            Version::FCS3_1 => {
                let u: PyUnion<_> = ascii_schema
                    .into_iter()
                    .chain(non_mixed_schema)
                    .map(PyClass::new1)
                    .collect();
                u.rstype(parse_quote!(PyNonMixedDataSchema))
            }
            _ => {
                let u: PyUnion<_> = ascii_schema
                    .into_iter()
                    .chain(ordered_schema)
                    .map(PyClass::new1)
                    .collect();
                u.rstype(parse_quote!(PyOrderedDataSchema))
            }
        };
        let data_schema_desc = if version == Version::FCS3_2 {
            format!(
                "Schema to describe data encoding. Represents {PNB}, {PNR}, {BYTEORD}, \
                 {DATATYPE}, and {PNDATATYPE}"
            )
        } else {
            format!(
                "Schema to describe data encoding. Represents {PNB}, {PNR}, {BYTEORD}, \
                 and {DATATYPE}."
            )
        };

        Self::new_ivar_rw(
            "data_schema",
            data_schema_pytype,
            data_schema_desc,
            true,
            |_, _| quote!(self.0.data_schema().clone().into()),
            |_, _| quote!(Ok(self.0.set_data_schema(data_schema.into())?)),
        )
    }

    fn new_df_ivar() -> Self {
        // use polars df here because we need to manually add names
        DocArg::new_data_param(true).into_rw(
            true,
            |_, pt| {
                let rt = pt.as_rust_type();
                quote! {
                    let ns = self.0.all_shortnames();
                    let data = PyFCSDataFrame(self.0.data().clone());
                    #rt(data.as_polars_dataframe(&ns[..]))
                }
            },
            |n, _| {
                quote! {
                    let d = PyFCSDataFrame::try_from(#n)?;
                    Ok(self.0.set_data(d.0)?)
                }
            },
        )
    }

    fn new_analysis_ivar() -> Self {
        DocArg::new_analysis_param(true).into_rw(
            false,
            |_, _| quote!(self.0.analysis().clone()),
            |n, _| quote!(*self.0.analysis_mut() = #n.into()),
        )
    }

    fn new_others_ivar() -> Self {
        DocArg::new_others_param(true).into_rw(
            false,
            |_, _| quote!(self.0.others().clone()),
            |n, _| quote!(*self.0.others_mut() = #n.into()),
        )
    }

    fn new_timestamps_ivar() -> [Self; 3] {
        let make_time_ivar = |is_start: bool| {
            let (kw, name) = if is_start {
                (BTIM, "btim")
            } else {
                (ETIM, "etim")
            };
            let get_naive = format_ident!("{name}_naive");
            let set_naive = format_ident!("set_{name}_naive");
            let desc = format!("Value of {kw}.");
            Self::new_opt_ivar_rw(
                name,
                PyTime::default(),
                desc,
                true,
                |_, _| quote!(self.0.#get_naive()),
                |n, _| quote!(Ok(self.0.#set_naive(#n)?)),
            )
        };

        let date_arg = Self::new_opt_ivar_rw(
            "date",
            PyDate::default(),
            format!("Value of {DATE}."),
            true,
            |_, _| quote!(self.0.date_naive()),
            |n, _| quote!(Ok(self.0.set_date_naive(#n)?)),
        );

        [make_time_ivar(true), make_time_ivar(false), date_arg]
    }

    fn new_datetime_ivar(is_start: bool) -> Self {
        let (kw, name) = if is_start {
            (BEGINDATETIME, "begindatetime")
        } else {
            (ENDDATETIME, "enddatetime")
        };
        let get = format_ident!("{name}");
        let set = format_ident!("set_{name}");
        Self::new_opt_ivar_rw(
            name,
            PyDatetime::default(),
            format!("Value for {kw}."),
            true,
            |_, _| quote!(self.0.#get()),
            |n, _| quote!(Ok(self.0.#set(#n)?)),
        )
    }

    fn new_comp_ivar(is_2_0: bool) -> Self {
        let rstype: Path = parse_quote!(fireflow_core::validated::compensation::Compensation);
        let desc = if is_2_0 {
            format!(
                "The compensation matrix. Must be a square array with number of \
                 rows/columns equal to the number of measurements. Non-zero \
                 entries will produce a {dfc} keyword.",
                dfc = fcs_kw("$DFCmTOn")
            )
        } else {
            format!(
                "The value of {comp}. Must be a square array with number of \
                 rows/columns equal to the number of measurements.",
                comp = fcs_kw("$COMP")
            )
        };
        Self::new_opt_ivar_rw(
            "comp",
            PyClass::new1("~numpy.ndarray").rstype(rstype),
            desc,
            true,
            |_, _| quote!(self.0.compensation().cloned()),
            |n, _| quote!(Ok(self.0.set_compensation(#n)?)),
        )
    }

    fn new_spillover_ivar() -> Self {
        let desc = formatcp!(
            "First element of tuple the list of measurement names and the second \
             is the matrix. Each measurement name must correspond to a {PNN}, \
             must be unique, and the length of this list must match the number \
             of rows and columns of the matrix. The matrix must be at least 2x2."
        );
        DocArgParam::new_kw_param(Kw::Spillover, Some(desc), true).into_rw(
            true,
            |_, _| quote!(self.0.spillover().map(|x| x.clone())),
            |n, _| quote!(Ok(self.0.set_spillover(#n)?)),
        )
    }

    fn new_csvflags_ivar() -> Self {
        let path: Path = parse_quote!(fireflow_core::core::CSVFlags);
        Self::new_ivar_rw(
            "csvflags",
            PyList::new(PyOpt::new1(PyInt::new_u32()), path.clone(), None),
            format!(
                "Subset flags. Each element in the list corresponds to {csvnflag} \
                 and the length of the list corresponds to {csmode}.",
                csvnflag = fcs_kw("$CSVnFLAG"),
                csmode = fcs_kw("$CSMODE"),
            ),
            false,
            |_, _| quote!(self.0.metaroot::<#path>().clone()),
            |n, _| quote!(self.0.set_metaroot(#n)),
        )
        .def_auto()
    }

    // TODO exception for mismatch PnN
    fn new_trigger_ivar() -> Self {
        let desc = formatcp!(
            "First member of tuple is threshold and second \
             is the measurement name which must match a {PNN}."
        );
        DocArg::new_kw_param(Kw::Tr, Some(desc), true).into_rw(
            true,
            |_, _| quote!(self.0.metaroot_opt().cloned()),
            |n, _| quote!(Ok(self.0.set_trigger(#n)?)),
        )
    }

    fn new_unstainedcenters_ivar() -> Self {
        let path = keyword_path("UnstainedCenters");
        // TODO exceptions for links
        let desc = Some("Each key must match a {PNN}.");
        DocArg::new_kw_param(Kw::UnstainedCenters, desc, true).into_rw(
            true,
            |_, _| quote!(self.0.metaroot::<#path>().clone()),
            |n, _| quote!(Ok(self.0.set_unstained_centers(#n)?)),
        )
    }

    fn new_applied_gates_ivar(version: Version) -> Self {
        // TODO there are version-specific exceptions for link failures
        let collapsed_version = if version == Version::FCS3_1 {
            Version::FCS3_0
        } else {
            version
        };
        let vsu = collapsed_version.short_underscore();
        let rstype_inner = format_ident!("AppliedGates{vsu}");
        let full_rstype: Path = parse_quote!(fireflow_core::text::gating::#rstype_inner);
        let rstype = format_ident!("Py{rstype_inner}");
        let gm_pytype = (collapsed_version < Version::FCS3_2)
            .then(|| PyList::new1(PyClass::new_py([""; 0], "GatedMeasurement")).into());
        let ur_pytype = PyClass::new1(format!("UnivariateRegion{vsu}"));
        let bv_pytype = PyClass::new1(format!("BivariateRegion{vsu}"));
        let reg_rstype = format_ident!("PyRegion{vsu}");
        let map_rstype = parse_quote!(PyRegionMapping<#reg_rstype>);
        let reg_pytype = PyDict::new(
            RsInt::NonZeroUsize,
            PyUnion::new2(ur_pytype, bv_pytype),
            Some(map_rstype),
            None,
        )
        .into();
        let gtype = PyType::from(PyOpt::new1(PyStr::default()));
        let pytype = PyTuple::new2(gm_pytype.into_iter().chain([reg_pytype, gtype]))
            .rstype(parse_quote!(#rstype));

        let desc = if collapsed_version == Version::FCS2_0 {
            format!(
                "Value for {GM_ANY}/{RN_ANY}/{GATING}/{GATE} keywords. The first member of \
                 the tuple corresponds to the {GM_ANY} keywords, where {m} is given by \
                 position in the list. The second member corresponds to the {RNI} and \
                 {RNW} keywords and is a mapping of regions and windows to be used in \
                 gating scheme. Keys in dictionary are the region indices (the {n} in \
                 {RN_ANY}). The values in the dictionary are either univariate \
                 or bivariate gates and must correspond to an index in the list in the \
                 first element. The third member corresponds to the {GATING} keyword. \
                 All {rn} in this string must reference a key in the dict of the second \
                 member.",
                m = fcs_kw("m"),
                n = fcs_kw("n"),
                rn = code_str("Rn"),
            )
        } else if collapsed_version < Version::FCS3_2 {
            format!(
                "Value for {GM_ANY}/{RN_ANY}/{GATING}/{GATE} keywords. The first member of \
                 the tuple corresponds to the {GM_ANY} keywords, where {m} is given by \
                 position in the list. The second member corresponds to the {RNI} and \
                 {RNW} keywords and is a mapping of regions and windows to be used in \
                 gating scheme. Keys in dictionary are the region indices (the {n} in \
                 {RN_ANY}). The values in the dictionary are either univariate \
                 or bivariate gates and must correspond to an index in the list in the \
                 first element or a physical measurement. The third member corresponds \
                 to the {GATING} keyword. All {rn} in this string must reference a key \
                 in the dict of the second member.",
                m = fcs_kw("m"),
                n = fcs_kw("n"),
                rn = code_str("Rn"),
            )
        } else {
            format!(
                "Value for {RN_ANY}/{GATING} keywords. The first member corresponds to \
                 the {RNI} and {RNW} keywords and is a mapping of regions and windows \
                 to be used in gating scheme. Keys in dictionary are the region indices \
                 (the {n} in {RN_ANY}). The values in the dictionary are either \
                 univariate or bivariate gates and must correspond to a physical \
                 measurement. The second member corresponds to the {GATING} keyword. \
                 All {rn} in this string must reference a key in the dict of the first \
                 member.",
                n = fcs_kw("n"),
                rn = code_str("Rn"),
            )
        };

        let param = DocArgParam::new_param("applied_gates", pytype, desc).def_auto();

        if collapsed_version == Version::FCS2_0 {
            param.into_rw(
                false,
                |_, _| quote!(self.0.metaroot::<#full_rstype>().clone().into()),
                |n, _| quote!(self.0.set_metaroot::<#full_rstype>(#n.into())),
            )
        } else {
            let setter = format_ident!("set_applied_gates_{vsu}");
            param.into_rw(
                true,
                |_, _| quote!(self.0.metaroot::<#full_rstype>().clone().into()),
                |n, _| quote!(Ok(self.0.#setter(#n.into())?)),
            )
        }
    }

    fn new_scale_ivar() -> Self {
        Self::new_opt_ivar_rw(
            "scale",
            PyUnion::new_scale(false),
            format!(
                "Value for {PNE}. Empty tuple means linear scale; 2-tuple encodes \
                 decades and offset for log scale"
            ),
            false,
            |_, _| quote!(*self.0.as_ref()),
            |n, _| quote!(*self.0.scale_mut() = #n.into()),
        )
    }

    fn new_transform_ivar() -> Self {
        let d = format!(
            "Value for {PNE} and/or {PNG}. Singleton float encodes gain ({PNG}) \
             and implies linear scaling (ie {PNE} is {linear}). 2-tuple encodes \
             decades and offset for log scale, and implies {PNG} is not set.",
            linear = code("0,0"),
        );
        Self::new_ivar_rw(
            "transform",
            PyUnion::new_transform(),
            d,
            false,
            |_, _| quote!(*self.0.as_ref()),
            |n, _| quote!(*self.0.scale_mut() = #n),
        )
    }

    fn new_core_nonstandard_keywords_ivar() -> Self {
        let d =
            format!("Pairs of non-standard keyword values. Keys must not start with {DOLLAR_STR}.");
        Self::new_nonstandard_keywords_ivar(
            d.as_str(),
            |_, _| quote!(self.0.nonstandard_keywords().clone()),
            |n, _| quote!(self.0.set_nonstandard_keywords(#n)),
        )
    }

    fn new_meas_nonstandard_keywords_ivar() -> Self {
        let d = format!(
            "Any non-standard keywords corresponding to this measurement. No keys \
             should start with {DOLLAR_STR}. Realistically each key should follow \
             a pattern corresponding to the measurement index, something like \
             prefixing with {p} followed by the index. This is not enforced.",
            p = code_str("P"),
        );
        let path = quote!(fireflow_core::validated::keys::NonStdKeywords);
        Self::new_nonstandard_keywords_ivar(
            d.as_str(),
            |_, _| quote!(AsRef::<#path>::as_ref(&self.0).clone()),
            |n, _| quote!(*self.0.as_mut() = #n),
        )
    }

    fn new_nonstandard_keywords_ivar(
        desc: &str,
        f: impl FnOnce(&Ident, &ArgPyType) -> TokenStream2,
        g: impl FnOnce(&Ident, &ArgPyType) -> TokenStream2,
    ) -> Self {
        let p = PyDict::new_nonstd_keywords();
        Self::new_ivar_rw("nonstandard_keywords", p, desc, false, f, g).def_auto()
    }
}

impl DocArgROIvar {
    fn new_ivar_ro(
        argname: impl fmt::Display + Clone,
        pytype: impl Into<ArgPyType>,
        desc: impl fmt::Display,
        f: impl FnOnce(&Ident, &ArgPyType) -> TokenStream2,
    ) -> Self {
        DocArgParam::new_param(argname, pytype, desc).into_ro(f)
    }

    fn new_version_ivar() -> Self {
        let p = PyLiteral::new_version();
        let d = "The FCS version.";
        Self::new_ivar_ro("version", p, d, |_, _| quote!(self.0.version))
    }

    fn new_endian_param(n: usize, is_ord: bool) -> Self {
        let xs = (1..=n).join(",");
        let ys = (1..=n).rev().join(",");
        let body = if is_ord {
            let sizedbyteord_path = quote!(fireflow_core::text::byteord::ArrayByteOrd);
            quote! {
                let m: #sizedbyteord_path<[u8; 2]> = *self.0.as_ref();
                m.endian()
            }
        } else {
            quote!(self.0.byte_order())
        };
        let d = format!(
            "If {BYTEORD_BIG_STR} use big endian ({bigval}) for encoding values; \
             if {BYTEORD_LITTLE_STR} use little endian ({littleval}).",
            bigval = code(ys),
            littleval = code(xs),
        );
        Self::new_ivar_ro("endian", PyLiteral::new_endian(), d, |_, _| body).def_auto()
    }

    fn new_version_scores_param() -> Self {
        let desc = "Scores generated if version was guessed.";
        let s = PyClass::new_py(["api"], "KeywordVersionScore");
        let t = PyTuple::new2(vec![s; 4]);
        let p = PyOpt::new1(t);
        DocArgParam::new_param("version_scores", p, desc).into_ro(|_, _| {
            quote!(self.0.version_scores.clone().map(|(a, b, c, d)| (
                a.into(),
                b.into(),
                c.into(),
                d.into()
            )))
        })
    }

    fn new_dataset_ivar(is_std: bool) -> Self {
        let (class_name, dataset_type) = if is_std {
            ("StdDatasetFromKwsOutput", "std")
        } else {
            ("FlatDatasetFromKwsOutput", "flat")
        };
        Self::new_ivar_ro(
            "dataset",
            PyClass::new_py(["api"], class_name),
            format!("Output when making {dataset_type} {TEXT} and {DATA}."),
            |n, _| quote!(self.0.#n.clone().into()),
        )
    }

    fn new_uint_ranges_ivar() -> Self {
        Self::new_ivar_ro(
            UINT_RANGES,
            PyList::new1(PyInt::new_full_int_range()),
            format!(
                "The maximum value of each measurement. Corresponds to the {PNR} \
                 keyword less one."
            ),
            |_, _| quote!(fireflow_core::data::LayoutRanges::ranges(&self.0)),
        )
    }

    fn new_byte_width_ivar() -> Self {
        let bytes_path = parse_quote!(fireflow_core::text::byteord::ArgBytes);
        Self::new_ivar_ro(
            "byte_width",
            PyInt::from(RsInt::U8).rstype(bytes_path),
            format!(
                "The width of the data schema in bytes. Must be an integer 1 to 8. \
                 All in {arg} must be able to fit within the allotted bytes.",
                arg = arg(UINT_RANGES),
            ),
            |_, _| quote!(self.0.byte_width()),
        )
        .def(DocDefault::Int(4))
    }
}

impl DocArgParam {
    fn new_param(
        argname: impl fmt::Display,
        pytype: impl Into<ArgPyType>,
        desc: impl fmt::Display,
    ) -> Self {
        let pt = pytype.into();
        Self::new(argname.to_string(), pt, desc.to_string(), None, NoMethods)
    }

    fn new_kw_param(kw: Kw, desc: Option<&str>, def: bool) -> Self {
        let preamble = format!("Value of {}.", kw.kw());
        let d = match desc {
            None => preamble,
            Some(d) => format!("{preamble} {d}"),
        };
        Self::new_param(kw.fun_name(), kw.as_pytype(), d).def_auto_if(def)
    }

    fn new_bool_param(name: impl fmt::Display, desc: impl fmt::Display) -> Self {
        Self::new_param(name, PyBool::default(), desc).def_auto()
    }

    fn new_tri_flag_param(
        name: impl fmt::Display,
        false_is_error: bool,
        ident_name: &str,
        desc: impl fmt::Display,
        exc: PyreflowError,
    ) -> Self {
        let e = format!("raise {}", exc.fmt_ref());
        let w = "throw warning".into();
        let (false_action, true_action) = if false_is_error { (e, w) } else { (w, e) };
        let d = format!(
            "{desc} If {false_}, {false_action}. If {true_}, {true_action}. \
             If {silent}, do nothing.",
            false_ = code_str(tc::TRI_FALSE_LEVEL),
            true_ = code_str(tc::TRI_TRUE_LEVEL),
            silent = code_str(tc::TRI_SILENT_LEVEL),
        );
        let pt = PyLiteral::new_tri_flag(ident_name);
        Self::new_param(name, pt, d).def_auto()
    }

    fn new_proc_kw_fail(
        name: impl fmt::Display,
        ident_name: &str,
        desc: impl fmt::Display,
    ) -> Self {
        let path = config_path(ident_name);
        let pt = PyLiteral::new1(tc::ProcessKeywordFailure::iter_str()).rstype(path);
        let d = format!(
            "{desc} Use {error} to throw error on failure, {demote} to demote \
             to non-standard with warning, {demote_silent} to demote to \
             non-standard with no warning, {drop} to drop with warning, or \
             {drop_silent} to drop with no warning",
            error = code_str(tc::KW_ERROR_LEVEL),
            demote = code_str(tc::KW_DEMOTE_WARN_LEVEL),
            demote_silent = code_str(tc::KW_DEMOTE_SILENT_LEVEL),
            drop = code_str(tc::KW_DROP_WARN_LEVEL),
            drop_silent = code_str(tc::KW_DROP_SILENT_LEVEL),
        );
        Self::new_param(name, pt, d).def_auto()
    }

    fn new_opt_param(
        name: impl fmt::Display,
        pytype: impl Into<ArgPyType>,
        desc: impl fmt::Display,
    ) -> Self {
        Self::new_param(name, PyOpt::new1(pytype), desc).def_auto()
    }

    fn into_ro(self, f: impl FnOnce(&Ident, &ArgPyType) -> TokenStream2) -> DocArgROIvar {
        let methods = GetMethod::from_pytype(self.argname.as_str(), &self.pytype, f);
        DocArgROIvar::new(self.argname, self.pytype, self.desc, self.default, methods)
    }

    fn into_rw(
        self,
        fallible: bool,
        f: impl FnOnce(&Ident, &ArgPyType) -> TokenStream2,
        g: impl FnOnce(&Ident, &ArgPyType) -> TokenStream2,
    ) -> DocArgRWIvar {
        let methods =
            GetSetMethods::from_pytype(self.argname.as_str(), &self.pytype, fallible, f, g);
        DocArgRWIvar::new(self.argname, self.pytype, self.desc, self.default, methods)
    }

    fn new_dataset_offset_param() -> Self {
        let desc = "Starting position in the file of the dataset to be read.";
        Self::new_param("dataset_offset", PyInt::new_dataset_offset(), desc).def_auto()
    }

    fn new_skip_param(desc: impl fmt::Display) -> Self {
        let pt = PyOpt::new1(PyInt::new_int(RsInt::Usize));
        Self::new_param("skip", pt, desc).def_auto()
    }

    fn new_limit_param(desc: &str) -> Self {
        let pt = PyOpt::new1(PyInt::new_int(RsInt::Usize));
        Self::new_param("limit", pt, desc).def_auto()
    }

    fn new_path_param(read: bool) -> Self {
        let s = if read { "read" } else { "written" };
        let pt = PyClass::new1("~pathlib.Path").rstype(parse_quote!(std::path::PathBuf));
        Self::new_param("path", pt, format!("Path to be {s}."))
    }

    fn new_header_param() -> Self {
        let d = format!("The {HEADER} from parsed file");
        Self::new_param("header", PyClass::new_py(["api"], "Header"), d)
    }

    fn new_header_and_supp_param() -> Self {
        let d = format!("The {HEADER} and supplemental {TEXT} offsets from parsed file");
        let p = PyClass::new_py(["api"], "HeaderAndSuppOffsets");
        Self::new_param("header", p, d)
    }

    fn new_std_keywords_param() -> Self {
        Self::new_param("std", PyDict::new_std_keywords(), "Standard keywords.")
    }

    fn new_nonstd_keywords_param() -> Self {
        let desc = "Non-standard keywords.";
        Self::new_param("nonstd", PyDict::new_nonstd_keywords(), desc)
    }

    fn new_valid_keywords_param() -> Self {
        let desc = "Standard and non-standard keywords.";
        Self::new_param("kws", PyClass::new_py(["api"], "ValidKeywords"), desc)
    }

    fn new_std_diagnostics_param() -> Self {
        let desc = format!("Diagnostic output from {TEXT} standardization");
        let p = PyClass::new_py(["api"], "StdTEXTDiagnostics");
        Self::new_param("std_diagnostics", p, desc)
    }

    fn new_dataset_segments_param() -> Self {
        let desc = format!("Offsets used to parse {DATA} and {ANALYSIS}.");
        let p = PyClass::new_py(["api"], "DatasetSegments");
        Self::new_param("dataset_segs", p, desc)
    }

    fn new_flat_diagnostics_param() -> Self {
        let desc = format!("Diagnostic data obtained when parsing {TEXT}.");
        let p = PyClass::new_py(["api"], "FlatTEXTDiagnostics");
        Self::new_param("flat_diagnostics", p, desc)
    }

    fn new_event_diagnostics_param() -> Self {
        let d = format!("Diagnostic output from parsing {DATA} segment.");
        let p = PyClass::new_py(["api"], "EventsDiagnostics");
        Self::new_param("events_diagnostics", p, d)
    }

    fn new_uncorrected_seg_param(argname: &str, seg: AnySegment, src: UncorrSegmentSrc) -> Self {
        let optional = matches!(src, UncorrSegmentSrc::Text);
        let (pt, end) = if optional {
            (
                PyType::from(PyOpt::new1(PyTuple::new_uncorrected_segment())),
                " (if found)",
            )
        } else {
            (PyTuple::new_uncorrected_segment().into(), "")
        };
        let desc = format!("The uncorrected {} segment from {src}{end}.", seg.name());
        Self::new_param(argname, pt, desc)
    }

    fn new_text_seg_param() -> Self {
        let desc = format!("The primary {TEXT} segment from {HEADER}.");
        Self::new_param("text_seg", PyTuple::new_text_segment(), desc)
    }

    fn new_data_seg_param(src: SegmentSrc) -> Self {
        let desc = format!("The {DATA} segment from {src}.");
        Self::new_param("data_seg", PyTuple::new_data_segment(src), desc)
    }

    fn new_analysis_seg_param(src: SegmentSrc, default: bool) -> Self {
        let desc = format!("The {ANALYSIS} segment from {src}.");
        let p = PyTuple::new_analysis_segment(src);
        Self::new_param("analysis_seg", p, desc).def_auto_if(default)
    }

    fn new_other_segs_param() -> Self {
        let seg = PyTuple::new_other_segment();
        let width = PyInt::new_other_width();
        let rstype = seg.rstype.clone().expect("no rstype for OTHER seg");
        let pt = PyOpt::new1(PyTuple::new1(PyList::new_non_empty(seg, &rstype)).add(width));
        let d = format!("The {OTHER} segments from {HEADER}.");
        Self::new_param("other_segs", pt, d)
    }

    fn new_textdelim_param() -> Self {
        let path = parse_quote!(fireflow_core::validated::textdelim::TEXTDelim);
        let d = format!("if {ARG_TOKEN} is not between 1 and 126");
        let exc = PyException::new_config().desc(d);
        let pytype = PyInt::from(RsInt::U8).rstype(path).exc(exc);
        let desc = format!("Delimiter to use when writing {TEXT}.");
        Self::new_param("delim", pytype, desc).def(DocDefault::Int(30))
    }

    fn new_big_other_param() -> Self {
        let desc = format!("If {TRUE} use 20 chars for {OTHER} segment offsets, and 8 otherwise.");
        Self::new_bool_param(BIG_OTHER, desc)
    }

    fn new_appendable_param() -> Self {
        const APPENDABLE: &str = "appendable";
        let d = format!(
            "If {TRUE}, set {NEXTDATA} in written dataset so it points to the \
             next dataset. This assumes the next dataset is written, which will \
             require another call to this method with {arg} set to {TRUE}.",
            arg = arg(APPENDABLE),
        );
        Self::new_bool_param(APPENDABLE, d)
    }

    fn new_append_param() -> Self {
        const APPEND: &str = "append";
        let d = format!(
            "If {TRUE}, append this dataset to the end of the file if it exists \
             and already has at least one dataset in it. This assumes that the \
             previous dataset was written with {arg} set to {TRUE} so \
             that {NEXTDATA} is properly set.",
            arg = arg(APPEND)
        );
        Self::new_bool_param(APPEND, d)
    }

    fn new_paired_measurements_param(version: Version) -> Self {
        let meas_desc = "Measurements corresponding to columns in FCS file. \
                         Temporal must be given zero or one times.";
        Self::new_param(MEASUREMENTS, PyTuple::new_meas(version), meas_desc)
    }

    fn new_measurements_param(version: Version) -> Self {
        let meas_desc = "Measurements corresponding to columns in FCS file. \
                         Temporal must be given zero or one times.";
        let pt = PyList::new1(PyUnion::new_measurement(version));
        Self::new_param(MEASUREMENTS, pt, meas_desc)
    }

    fn new_set_meas_param(version: Version) -> Self {
        let d = "The new measurements. The first member of the tuple corresponds to \
                 the measurement name and the second is the measurement object.";
        Self::new_param(MEASUREMENTS, PyTuple::new_meas(version), d)
    }

    fn new_allow_shared_names_param() -> Self {
        let exc = PyreflowError::Relational.fmt_ref();
        let d = format!(
            "If {FALSE}, raise {exc} if any non-measurement keywords reference \
             any {PNN} keywords. If {TRUE} raise {exc} if any non-measurement \
             keywords reference a {PNN} which is not present in {measurements}. \
             In other words, {FALSE} forbids named references to exist, and \
             {TRUE} allows named references to be updated. References cannot \
             be broken in either case.",
            measurements = arg(MEASUREMENTS)
        );
        Self::new_bool_param("allow_shared_names", d)
    }

    // TODO this can be specific to each version, for instance, we can call out
    // the exact keywords in each that may have references.
    fn new_skip_index_check_param() -> Self {
        let exc = PyreflowError::Relational.fmt_ref();
        let desc = format!(
            "If {FALSE}, raise {exc} if any non-measurement keyword \
             have an index reference to the current measurements. If \
             {TRUE} allow such references to exist as long as they do \
             not break (which really means that the length of \
             {measurements} is such that existing indices are satisfied).",
            measurements = arg(MEASUREMENTS)
        );
        Self::new_bool_param("skip_index_check", desc)
    }

    fn new_index_param(desc: &str) -> Self {
        Self::new_param("index", PyInt::new_meas_index(), desc)
    }

    fn new_col_param() -> Self {
        let d = "Data for measurement. Must be same length as existing columns.";
        Self::new_param("col", PyClass::new_series(), d)
    }

    fn new_name_param(short_desc: &str) -> Self {
        let desc = format!("{short_desc} Corresponds to {PNN}.");
        Self::new_param("name", PyStr::new_shortname(), desc)
    }

    fn new_any_range_param(version: Version) -> Self {
        let desc = format!("Range of measurement. Corresponds to {PNR}.");
        Self::new_param("range", PyUnion::new_any_range(version), desc)
    }

    // fn new_notrunc_param() -> Self {
    //     let d = "Disallow range to be truncated if required to fit in column's data type.";
    //     let e = PyreflowError::InvalidKeywordValue;
    //     Self::new_tri_flag_param("disallow_trunc", false, "DisallowRangeTrunc", d, e)
    // }

    fn new_allow_loss_param(desc: impl fmt::Display) -> Self {
        let e = PyreflowError::Conversion;
        Self::new_tri_flag_param("allow_loss", true, "AllowLoss", desc, e)
    }

    fn new_data_param(polars_type: bool) -> Self {
        let desc = format!(
            "A dataframe encoding the contents of {DATA}. Number of \
             columns must match number of measurements. May be empty. \
             Types do not necessarily need to correspond to those in the \
             data schema but mismatches may result in truncation."
        );
        let d = format!(
            "If {ARG_TOKEN} contains columns which are not \
             unsigned 8/16/32/64-bit integers or 32/64-bit floats"
        );
        let exc = PyException::new_pyreflow(PyreflowError::EventData).desc(d);
        let pt = PyClass::new_dataframe(polars_type).exc(exc);
        Self::new_param("data", pt, desc)
    }

    fn new_analysis_param(default: bool) -> Self {
        let desc = format!("Contents of the {ANALYSIS} segment.");
        Self::new_param("analysis", PyBytes::new_analysis(), desc).def_auto_if(default)
    }

    fn new_others_param(default: bool) -> Self {
        let desc = format!("A list of byte strings encoding the {OTHER} segments.");
        Self::new_param("others", PyList::new_others(), desc).def_auto_if(default)
    }

    fn new_read_header_config_params() -> (Path, Vec<Self>, Vec<TokenStream2>) {
        let conf = config_path("ReadHeaderInnerConfig");
        let ps = vec![
            Self::new_text_correction_param(),
            Self::new_data_correction_param(),
            Self::new_analysis_correction_param(),
            Self::new_other_corrections_param(),
            Self::new_max_other_param(),
            Self::new_other_width_param(),
            Self::new_guess_other_width_param(),
            Self::new_squish_offsets_param(),
        ];
        let js = ps.iter().map(IsDocArg::record_into).collect();
        (conf, ps, js)
    }

    fn new_read_offset_config_params(
        version: Option<Version>,
    ) -> (Path, Vec<Self>, Vec<TokenStream2>) {
        let conf = config_path("ReadOffsetConfig");
        // This switch will only be used for functions that don't deal with
        // HEADER so any offsets in that case are limited to TEXT which aren't
        // present in 2.0
        let ps = if version == Some(Version::FCS2_0) {
            vec![]
        } else {
            vec![
                Self::new_allow_pseudoempty_param(),
                Self::new_truncate_offset_limit_param(),
                Self::new_overlap_correction_limit_param(),
            ]
        };
        let js = ps.iter().map(IsDocArg::record_into).collect();
        (conf, ps, js)
    }

    fn new_read_flat_config_params() -> (Path, Vec<Self>, Vec<TokenStream2>) {
        let conf = config_path("ReadHeaderAndTEXTConfig");
        let ps = vec![
            Self::new_version_override(),
            Self::new_supp_text_correction(),
            Self::new_nextdata_correction(),
            Self::new_allow_duplicate_supp_text(),
            Self::new_ignore_supp_text(),
            Self::new_delim_escape_mode(),
            Self::new_allow_non_ascii_delim(),
            Self::new_allow_nonunique(),
            Self::new_allow_even_delims(),
            Self::new_allow_odd_tokens(),
            Self::new_allow_empty_keys(),
            Self::new_allow_delim_at_boundary(),
            Self::new_use_latin1(),
            Self::new_allow_non_ascii_keys(),
            Self::new_allow_non_utf8_values(),
            Self::new_allow_missing_supp_text(),
            Self::new_allow_supp_text_own_delim(),
            Self::new_allow_missing_nextdata(),
            Self::new_trim_value_whitespace(),
            Self::new_ignore_standard_keys(),
            Self::new_promote_to_standard(),
            Self::new_demote_from_standard(),
            Self::new_rename_standard_keys(),
            Self::new_replace_standard_key_values(),
            Self::new_append_standard_keywords(),
            Self::new_substitute_standard_key_values(),
        ];
        let js = ps.iter().map(IsDocArg::record_into).collect();
        (conf, ps, js)
    }

    fn new_read_std_config_params(
        version: Option<Version>,
    ) -> (Path, Vec<Self>, Vec<TokenStream2>) {
        let parse_indexed_spillover = Self::new_spillover_meas_mode_param();
        let disallow_localtime = Self::new_disallow_localtime_param();
        let add_missing_timestep = Self::new_add_missing_timestep_param();

        let std_common_args = [
            Self::new_dedup_meas_names_param(),
            Self::new_trim_intra_value_whitespace_param(),
            Self::new_time_meas_pattern_param(),
            Self::new_allow_missing_time_param(),
            Self::new_force_linear_scale_param(),
            Self::new_ignore_time_optical_keys_param(),
            Self::new_process_time_optical_keys_param(),
            Self::new_date_pattern_param(),
            Self::new_time_pattern_param(version),
            Self::new_datetime_pattern_param(),
            Self::new_last_modified_pattern_param(),
            Self::new_allow_other_feature_param(),
            Self::new_process_pseudostandard_param(),
            Self::new_process_hyper_par_param(),
            Self::new_process_other_version_param(),
            Self::new_process_extra_timestep_param(),
            Self::new_fix_log_scale_offsets_param(),
            Self::new_nonstandard_measurement_pattern_param(),
        ]
        .into_iter();

        let ps: Vec<_> = match version {
            Some(Version::FCS2_0) => std_common_args.collect(),
            Some(Version::FCS3_0) => std_common_args.chain([add_missing_timestep]).collect(),
            Some(Version::FCS3_1) => std_common_args
                .chain([add_missing_timestep, parse_indexed_spillover])
                .collect(),
            _ => std_common_args
                .chain([
                    add_missing_timestep,
                    parse_indexed_spillover,
                    disallow_localtime,
                ])
                .collect(),
        };

        let conf = config_path("ReadStdKeywordsConfig");
        let js = ps.iter().map(IsDocArg::record_into).collect();
        (conf, ps, js)
    }

    fn new_read_data_schema_config_params(
        version: Option<Version>,
    ) -> (Path, Vec<Self>, Vec<TokenStream2>) {
        let offset_ps: Vec<_> = match version {
            // none of these apply to 2.0 since there are no offsets in TEXT
            Some(Version::FCS2_0) => vec![],
            _ => vec![
                Self::new_text_data_correction_param(),
                Self::new_text_analysis_correction_param(),
                Self::new_ignore_text_data_offsets_param(),
                Self::new_ignore_text_analysis_offsets_param(),
                Self::new_allow_header_text_offset_mismatch_param(),
                Self::new_allow_missing_required_offsets_param(version),
            ],
        };

        let process_optional_failure = Self::new_process_optional_failure();
        let integer_widths_from_byteord = Self::new_integer_widths_from_byteord_param();
        let integer_byteord_override = Self::new_integer_byteord_override_param();
        let disallow_range_truncation = Self::new_disallow_range_truncation_param();

        let data_schema_ps: Vec<_> = match version {
            Some(Version::FCS3_1 | Version::FCS3_2) => {
                [process_optional_failure, disallow_range_truncation]
                    .into_iter()
                    .collect()
            }
            _ => [
                process_optional_failure,
                integer_widths_from_byteord,
                integer_byteord_override,
                disallow_range_truncation,
            ]
            .into_iter()
            .collect(),
        };

        let conf = config_path("ReadDataKeywordsConfig");
        let ps: Vec<_> = offset_ps.into_iter().chain(data_schema_ps).collect();
        let js = ps.iter().map(IsDocArg::record_into).collect();
        (conf, ps, js)
    }

    fn new_read_events_config_params() -> (Path, Vec<Self>, Vec<TokenStream2>) {
        let conf = config_path("ReadEventsConfig");
        let ps = vec![
            Self::new_data_remainder_limit_param(),
            Self::new_allow_uneven_event_width_param(),
            Self::new_allow_tot_mismatch_param(),
            Self::new_checked_range_datatypes(),
            Self::new_over_range_action(),
            Self::new_row_buffer_size(true),
        ];
        let js = ps.iter().map(IsDocArg::record_into).collect();
        (conf, ps, js)
    }

    fn new_write_text_config_params() -> (Path, Vec<Self>, Vec<TokenStream2>) {
        let delim = Self::new_textdelim_param();
        let big_other = Self::new_big_other_param();
        let conf = config_path("WriteTEXTInnerConfig");
        let ps = vec![delim, big_other];
        let js = ps.iter().map(IsDocArg::record_into).collect();
        (conf, ps, js)
    }

    fn new_shared_config_params() -> (Path, Vec<Self>, Vec<TokenStream2>) {
        let conf = config_path("ReadSharedConfig");
        let warnings_are_errors = Self::new_warnings_are_errors_param();
        let hide_warnings = Self::new_hide_warnings_param();
        let ps = vec![warnings_are_errors, hide_warnings];
        let js = ps.iter().map(IsDocArg::record_into).collect();
        (conf, ps, js)
    }

    fn new_dedup_meas_names_param() -> Self {
        let d = format!(
            "If {TRUE}, force all {PNN} to be unique by appending \
             {suffix} to each duplicate and incrementing {x} starting at 0.",
            suffix = code_str(format!("{}X", tc::DEDUP_PNN_SEP)),
            x = code("X"),
        );
        Self::new_bool_param("dedup_measurement_names", d)
    }

    fn new_trim_intra_value_whitespace_param() -> Self {
        let d = format!(
            "If {TRUE}, trim whitespace between delimiters such as {comma} \
             and {semicolon} within keyword value strings.",
            comma = code_str(","),
            semicolon = code_str(";"),
        );
        Self::new_bool_param("trim_intra_value_whitespace", d)
    }

    fn new_time_meas_pattern_param() -> Self {
        let path: Path = parse_quote!(fireflow_core::config::TimeMeasNamePattern);
        let pytype = PyOpt::new1(PyStr::new_regexp().rstype(path.clone()))
            .default_from_inner()
            .rstype(path);
        let d = format!(
            "A pattern to match the {PNN} of the time measurement. \
             If {none}, do not try to find a time measurement.",
            none = code_str(tc::TIME_MEAS_NAME_PATTERN_NONE),
        );
        Self::new_param("time_meas_pattern", pytype, d)
            .def(DocDefault::Str(tc::TIME_MEAS_NAME_PATTERN_DEFAULT.into()))
    }

    fn new_allow_missing_time_param() -> Self {
        let d = "Choose what to do when time measurement is be missing.";
        let exc = PyreflowError::Relational;
        Self::new_tri_flag_param("allow_missing_time", true, "AllowMissingTime", d, exc)
    }

    fn new_add_missing_timestep_param() -> Self {
        let d = format!(
            "Set {TIMESTEP} if it is not present and required. \
             This will do nothing on FCS2.0 files since this version \
             does not specify {TIMESTEP}."
        );
        let pt = PyOpt::new1(PyFloat::new_timestep());
        Self::new_param("add_missing_timestep", pt, d).def_auto()
    }

    fn new_force_linear_scale_param() -> Self {
        let path = types_config_path("ForceLinearScale");
        let pt = PyLiteral::new1(tc::ForceLinearScale::iter_str()).rstype(path);
        let d = format!(
            "Force {PNE} to be linear. Use {time} to only \
             change the temporal measurement, {non_int} to change all \
             non-integer measurements and temporal measurement, {all} to change \
             all measurements, and {none} to change no measurements.",
            time = code_str(tc::FORCE_LINEAR_TIME_LEVEL),
            non_int = code_str(tc::FORCE_LINEAR_NON_INT_LEVEL),
            all = code_str(tc::FORCE_LINEAR_ALL_LEVEL),
            none = code_str(tc::FORCE_LINEAR_NONE_LEVEL),
        );
        Self::new_param("force_linear_scale", pt, d).def_auto()
    }

    fn new_ignore_time_optical_keys_param() -> Self {
        let p = PyList::new(
            PyLiteral::new_temporal_optical_key(),
            Some(parse_quote!(fireflow_core::config::TemporalOpticalKeys)),
            None,
        );
        let d = format!(
            "Ignore optical keys in temporal measurement. These keys are \
             {PNG} which is explicitly forbidden by the standard but \
             allowed in this library to be set to {noop} (noop), or \
             others which are nonsensical for time measurements but are not \
             explicitly forbidden in the the standard (such as {pnl}). \
             Provided keys are the string after the {pn} in the {pnx} \
             keywords.",
            pnl = fcs_kw(tk::PNL),
            noop = code("1.0"),
            pn = code_str("Pn"),
            pnx = code_str("PnX"),
        );
        Self::new_param(IGNORE_TIME_OPTICAL_KEYS, p, d).def_auto()
    }

    fn new_process_time_optical_keys_param() -> Self {
        let d = format!(
            "Choose how to handle optical keys found in temporal measurements. \
             Does nothing unless keys are specified in {other_arg}. \
             Pass {demote}, {demote_silent}, {drop}, or \
             {drop_silent} to demote found keys to nonstandard (with \
             or without warning) or drop keys entirely (with or without \
             warning) respectively.",
            other_arg = arg(IGNORE_TIME_OPTICAL_KEYS),
            demote = code_str(tc::TMP_OPT_DEMOTE_WARN_LEVEL),
            demote_silent = code_str(tc::TMP_OPT_DEMOTE_SILENT_LEVEL),
            drop = code_str(tc::TMP_OPT_DROP_WARN_LEVEL),
            drop_silent = code_str(tc::TMP_OPT_DROP_SILENT_LEVEL),
        );
        let path = types_config_path("ProcessTemporalOpticalKeys");
        let pt = PyLiteral::new1(tc::ProcessTemporalOpticalKeys::iter_str()).rstype(path);
        Self::new_param("process_time_optical_keys", pt, d).def_auto()
    }

    fn new_spillover_meas_mode_param() -> Self {
        let d = format!(
            "Choose how to interpret measurement strings in {spillover}. \
             Set to {named} to interpret as names which link to \
             {PNN}. Set to {indexed} to interpret as 1-indices which \
             point to measurements. Set to {guess} to automatically \
             choose the prior two modes.",
            spillover = Kw::Spillover.kw(),
            named = code_str(tc::SPILLOVER_NAMED_LEVEL),
            indexed = code_str(tc::SPILLOVER_INDEXED_LEVEL),
            guess = code_str(tc::SPILLOVER_GUESS_LEVEL),
        );
        let path = types_config_path("SpilloverMeasurementMode");
        let pt = PyLiteral::new1(tc::SpilloverMeasurementMode::iter_str()).rstype(path);
        Self::new_param("spillover_measurement_mode", pt, d).def_auto()
    }

    fn new_date_pattern_param() -> Self {
        let path = parse_quote!(fireflow_core::validated::datepattern::DatePattern);
        let desc = format!(
            "if {ARG_TOKEN} does not have year, month, and day specifiers \
             as outlined in {CHRONO_REF}"
        );
        let exc = PyException::new_config().desc(desc);
        let pytype = PyStr::default().rstype(path).exc(exc);
        let d = format!(
            "If supplied, will be used as an alternative pattern when parsing \
             {DATE}. If not supplied, {DATE} will be parsed according to \
             the standard pattern which is {pat}.",
            pat = code_str(tc::DEFAULT_DATE_FORMAT),
        );
        Self::new_opt_param("date_pattern", pytype, d)
    }

    fn new_datetime_pattern_param() -> Self {
        let pytype = PyStr::default();
        let d = format!(
            "If supplied, will be used as an alternative pattern when parsing \
             {BEGINDATETIME} and {ENDDATETIME}. The pattern must follow the \
             format outlined in {CHRONO_REF}. If not supplied, these will \
             be parsed as ISO timestamps with optional timezone."
        );
        Self::new_opt_param("datetime_pattern", pytype, d)
    }

    fn new_last_modified_pattern_param() -> Self {
        let pytype = PyStr::default();
        let d = format!(
            "If supplied, will be used as an alternative pattern when parsing \
             {last_mod}. The pattern must follow the format outlined in \
             {CHRONO_REF}. If not supplied, these will be parsed according to \
             the default pattern which is {pat} possibly with centiseconds after.",
            pat = code_str(tc::DEFAULT_LAST_MODIFIED_FORMAT),
            last_mod = fcs_kw(tk::LAST_MODIFIED),
        );
        Self::new_opt_param("last_modified_pattern", pytype, d)
    }

    fn new_allow_other_feature_param() -> Self {
        let d = format!(
            "If {TRUE}, allow {PNFEATURE} to be a value other than \
             {FEATURE_AREA_STR}, {FEATURE_WIDTH_STR}, or {FEATURE_HEIGHT_STR}."
        );
        Self::new_bool_param("allow_other_feature", d)
    }

    fn new_time_pattern_param(version: Option<Version>) -> Self {
        const NAME3_0: &str = "1/60 seconds";
        const NAME3_1: &str = "centiseconds";

        // format exception description
        let exc_desc = format!(
            "if {ARG_TOKEN} does not have specifiers for hours, minutes, \
             seconds, and optionally sub-seconds (where {b60} and {b100} \
             correspond to {NAME3_0} and {NAME3_1} respectively) as outlined \
             in {CHRONO_REF}",
            b60 = code_str(tc::BASE60_SECOND_SPEC),
            b100 = code_str(tc::BASE100_SECOND_SPEC),
        );
        let exc = PyException::new_config().desc(exc_desc);

        let fmt2_0 = code_str(tc::DEFAULT_TIME_FORMAT_2_0);
        let fmt3_0 = code_str(tc::DEFAULT_TIME_FORMAT_3_0);
        let fmt3_1 = code_str(tc::DEFAULT_TIME_FORMAT_3_1);

        // format arg description
        let std_pat = match version {
            None => format!("{fmt2_0} for 2.0, {fmt3_0} for 3.0 and {fmt3_1} for 3.1 and up"),
            Some(Version::FCS2_0) => fmt2_0,
            Some(Version::FCS3_0) => fmt3_0,
            _ => fmt3_1,
        };
        let line1 = "If supplied, will be used as an alternative pattern when \
                     parsing {BTIM} and {ETIM}.";
        let line2 = format!(
            "The values {b60} or {b100} may be used to match \
             {NAME3_0} or {NAME3_1} respectively.",
            b60 = code_str(tc::BASE60_SECOND_SPEC),
            b100 = code_str(tc::BASE100_SECOND_SPEC),
        );
        let line3 = format!(
            "If not supplied, {BTIM} and {ETIM} will be parsed \
             according to the standard pattern which is {std_pat}.",
        );
        let arg_desc = [line1.to_owned(), line2, line3].into_iter().join(" ");

        let path = parse_quote!(fireflow_core::validated::timepattern::TimePattern);
        let pytype = PyStr::default().rstype(path).exc(exc);
        Self::new_opt_param("time_pattern", pytype, arg_desc)
    }

    fn new_process_pseudostandard_param() -> Self {
        let d = format!(
            "Process non-standard keywords with a leading {DOLLAR_STR}. The \
             presence of such keywords often means the version in {HEADER} \
             is incorrect."
        );
        Self::new_proc_kw_fail("process_pseudostandard", "ProcessPseudostandard", d)
    }

    fn new_process_hyper_par_param() -> Self {
        let d = format!("Process measurement keywords whose index is greater than {PAR}.");
        Self::new_proc_kw_fail("process_hyper_par", "ProcessHyperPar", d)
    }

    fn new_process_other_version_param() -> Self {
        let d = "Process standard keywords from different FCS versions.";
        Self::new_proc_kw_fail("process_other_version", "ProcessOtherVersion", d)
    }

    fn new_process_extra_timestep_param() -> Self {
        let d = format!(
            "Process {TIMESTEP} to be present which may indicate \
             a time measurement is present but not identified."
        );
        Self::new_proc_kw_fail("process_extra_timestep", "ProcessExtraTimestep", d)
    }

    fn new_process_optional_failure() -> Self {
        let d = "Process optional keys which cause an error.";
        Self::new_proc_kw_fail("process_optional_failure", "ProcessOptionalFailure", d)
    }

    fn new_fix_log_scale_offsets_param() -> Self {
        let d = format!(
            "If {TRUE} fix log-scale {PNE} and keywords which have zero offset \
             (ie {x_zero} where {x} is non-zero).",
            x_zero = code("<X>,0.0"),
            x = code("X"),
        );
        Self::new_bool_param("fix_log_scale_offsets", d)
    }

    fn new_disallow_localtime_param() -> Self {
        let d = format!(
            "If {TRUE}, require that {BEGINDATETIME} and {ENDDATETIME} \
             have a timezone if provided. This is not required by the \
             standard, but not having a timezone is ambiguous since the \
             absolute value of the timestamp is dependent on localtime and \
             therefore is location-dependent. Only affects FCS 3.2."
        );
        Self::new_bool_param("disallow_localtime", d)
    }

    fn new_nonstandard_measurement_pattern_param() -> Self {
        let path = config_path("NonStdMeasPatternOpt");
        let pat = code_str(tc::NON_STD_MEAS_INDEX_PAT);
        let ed = format!("if {ARG_TOKEN} does not have {pat}");
        let exc = PyException::new_config().desc(ed);
        // TODO this is really weird, why is path specified twice?
        let pytype = PyOpt::new1(PyStr::default().exc(exc).rstype(path.clone()))
            .default_from_inner()
            .rstype(path);
        let d = format!(
            "Pattern to use when matching nonstandard measurement keys. \
             Values that start and end with {delim} will be \
             interpreted as regular expressions, otherwise as literal strings \
             to be used as an exact prefix match. If a regular expression, it \
             must include {pat} which will represent the measurement index. \
             Otherwise should be a normal regular expression as defined in \
             {REGEXP_REF}.",
            delim = tc::PATTERN_DELIMITER
        );
        Self::new_param("nonstandard_measurement_pattern", pytype, d)
            .def(DocDefault::Str(tc::NON_STD_MEAS_PAT_DEFAULT.into()))
    }

    fn new_integer_widths_from_byteord_param() -> Self {
        let d = format!(
            "If {TRUE} set all {PNB} to the number of bytes from {BYTEORD}. \
             Only has an effect for FCS 2.0/3.0 where {DATATYPE} is {int}.",
            int = code("I"),
        );
        Self::new_bool_param("integer_widths_from_byteord", d)
    }

    fn new_integer_byteord_override_param() -> Self {
        let path = keyword_path("ByteOrd2_0");
        let d = format!(
            "if {ARG_TOKEN} is not a list of integers including all from 1 to {n} \
             where {n} is the length of the list (up to 8)",
            n = code("N"),
        );
        let exc = PyException::new_invalid_keyword().desc(d);
        Self::new_opt_param(
            "integer_byteord_override",
            PyList::new(RsInt::U32, Some(path), Some(exc.into())),
            format!("Override {BYTEORD} for integer data schemas."),
        )
    }

    fn new_disallow_range_truncation_param() -> Self {
        let n = "disallow_range_truncation";
        let d = format!(
            "Choose how to handle {PNR} values that need to be truncated \
             to match the number of bytes specified by {PNB} and {DATATYPE}."
        );
        let e = PyreflowError::Relational;
        Self::new_tri_flag_param(n, false, "DisallowRangeTrunc", d, e)
    }

    fn new_config_correction_arg(name: &str, what: AnySegment, is_header: bool) -> Self {
        let location = if is_header { HEADER } else { TEXT };
        let d = format!("Corrections for {} offsets in {location}.", what.name());
        Self::new_param(name, PyTuple::new_correction(what, is_header), d).def_auto()
    }

    fn new_text_correction_param() -> Self {
        Self::new_config_correction_arg("text_correction", AnySegment::PrimaryTEXT, true)
    }

    fn new_data_correction_param() -> Self {
        Self::new_config_correction_arg("data_correction", AnySegment::Data, true)
    }

    fn new_analysis_correction_param() -> Self {
        Self::new_config_correction_arg("analysis_correction", AnySegment::Analysis, true)
    }

    fn new_other_corrections_param() -> Self {
        Self::new_param(
            "other_corrections",
            PyList::new1(PyTuple::new_correction(AnySegment::Other, true)),
            format!(
                "Corrections for {OTHER} offsets if they exist. Each correction will \
                 be applied in order. If an offset does not need to be corrected, \
                 use {zero_zero}. This will not affect the number of {OTHER} segments \
                 that are read; this is controlled by {max_other}.",
                zero_zero = code("(0,0)"),
                max_other = arg(MAX_OTHER),
            ),
        )
        .def_auto()
    }

    fn new_max_other_param() -> Self {
        let desc = format!(
            "Maximum number of {OTHER} segments that can be parsed. \
             {NONE} means limitless."
        );
        Self::new_opt_param(MAX_OTHER, RsInt::Usize, desc)
    }

    fn new_other_width_param() -> Self {
        let pt = PyInt::new_other_width();
        let desc = format!("Width (in bytes) to use when parsing {OTHER} offsets.");
        Self::new_param(OTHER_WIDTH, pt, desc).def(DocDefault::Int(8))
    }

    fn new_guess_other_width_param() -> Self {
        let path = types_config_path("GuessOtherWidth");
        let pt = PyLiteral::new1(tc::GuessOtherWidth::iter_str()).rstype(path);
        let d = format!(
            "Guess the width of {OTHER} segments. Valid values are {none} \
             (no guessing) or {error}, {warn} or {silent} which will guess and \
             throw an error, warning, or nothing on failure. For {warn} and \
             {silent}, failure will fall back to the 8 or whatever was given in \
             {other_arg}",
            other_arg = arg(OTHER_WIDTH),
            none = code_str(tc::OTHER_WIDTH_NONE_LEVEL),
            error = code_str(tc::OTHER_WIDTH_ERROR_LEVEL),
            warn = code_str(tc::OTHER_WIDTH_WARN_LEVEL),
            silent = code_str(tc::OTHER_WIDTH_SILENT_LEVEL),
        );
        Self::new_param("guess_other_width", pt, d).def_auto()
    }

    // this only matters for 3.0+ files
    fn new_squish_offsets_param() -> Self {
        let d = format!(
            "If {TRUE} and a segment's ending offset is zero, treat entire \
             offset as empty. This might happen if the ending offset is longer \
             than 8 digits, in which case it must be written in {TEXT}. If this \
             happens, the standards mandate that both offsets be written to \
             {TEXT} and that the {HEADER} offsets be set to {empty}, so only \
             writing one is an error unless this flag is set. This should only \
             happen in FCS 3.0 files and above.",
            empty = code("0,0"),
        );
        Self::new_bool_param("squish_offsets", d)
    }

    fn new_allow_pseudoempty_param() -> Self {
        let d = format!(
            "If {TRUE}, allow offsets like {x_x_minus_one}. Some files \
             will denote an \"empty\" offset as {fake_empty0} or {fake_empty1000}, \
             which is logically correct since the last offset points to the \
             last byte, thus {empty} is actually 1 byte long. If this flat \
             is set, such offsets will be treated as if they were {empty}.",
            x_x_minus_one = code("X,X-1"),
            fake_empty0 = code("0,-1"),
            fake_empty1000 = code("1000,999"),
            empty = code("0,0"),
        );
        Self::new_bool_param("allow_pseudoempty", d)
    }

    fn new_truncate_offset_limit_param() -> Self {
        let d = "Limit by which offsets can be truncated if they exceed end of file.";
        Self::new_param("truncate_offset_limit", RsInt::U64, d).def_auto()
    }

    fn new_overlap_correction_limit_param() -> Self {
        let d = "Limit by which ending segment offset can be truncated if \
                 they overlap another offset.";
        Self::new_param("overlap_correction_limit", RsInt::U64, d).def_auto()
    }

    fn new_data_remainder_limit_param() -> Self {
        let d = format!(
            "Limit by which ending {DATA} offset can be truncated if \
             its length modulo event width produces a remainder."
        );
        Self::new_param("data_remainder_limit", RsInt::U64, d).def_auto()
    }

    fn new_version_override() -> Self {
        let d = format!(
            "Override the FCS version as seen in {HEADER}. Use an FCS \
             version string like {verstr} to force to a specific version. \
             Alternatively, autodetect the version from keywords in {TEXT} \
             using one of {latest}, {earliest}, {strict}, or {loose}. These \
             will be used to select the latest version, earliest version, \
             version with least optional keywords, or version with most optional \
             keywords respectively in the event that more than one version can \
             accommodate the keywords from {TEXT}. Autodetection will fail \
             if no versions can be found which accommodate all required \
             keywords in {TEXT}.",
            verstr = code_str("FCS3.2"),
            latest = code_str(tc::VERSION_LATEST_LEVEL),
            earliest = code_str(tc::VERSION_EARLIEST_LEVEL),
            strict = code_str(tc::VERSION_STRICT_LEVEL),
            loose = code_str(tc::VERSION_LOOSE_LEVEL),
        );
        Self::new_opt_param("version_override", PyLiteral::new_version_override(), d)
    }

    fn new_supp_text_correction() -> Self {
        Self::new_config_correction_arg("supp_text_correction", AnySegment::SuppTEXT, false)
    }

    fn new_nextdata_correction() -> Self {
        let d = format!("Correction for {NEXTDATA}.");
        Self::new_param("nextdata_correction", PyInt::new_int(RsInt::I32), d).def_auto()
    }

    fn new_allow_duplicate_supp_text() -> Self {
        let n = "allow_duplicated_supp_text";
        let d = format!(
            "Choose what happens if supplemental {TEXT} offsets overlap the \
             primary {TEXT} offsets from {HEADER} or {HEADER}. The offsets \
             will not be used if an overlap is found."
        );
        let e = PyreflowError::FileLayout;
        Self::new_tri_flag_param(n, true, "AllowDuplicatedSuppTEXT", d, e)
    }

    fn new_ignore_supp_text() -> Self {
        let d = format!("If {TRUE}, ignore supplemental {TEXT} entirely.");
        Self::new_bool_param("ignore_supp_text", d)
    }

    fn new_delim_escape_mode() -> Self {
        let path = types_config_path("DelimEscapeMode");
        let d = format!(
            "Determine how to escape delims in {TEXT}. If {escaped} \
             or {unescaped}, escape or do not escape delimiters \
             respectively. If {guess_escaped} or  {guess_unescaped}, \
             attempt to guess how delimiters should be treated, falling back \
             to escaped or unescaped mode respectively if the choice is ambiguous.",
            escaped = code_str(tc::DELIM_ESCAPED_LEVEL),
            unescaped = code_str(tc::DELIM_UNESCAPED_LEVEL),
            guess_escaped = code_str(tc::DELIM_GUESS_ESCAPED_LEVEL),
            guess_unescaped = code_str(tc::DELIM_GUESS_UNESCAPED_LEVEL),
        );
        let pt = PyLiteral::new1(tc::DelimEscapeMode::iter_str()).rstype(path);
        Self::new_param("delim_escape_mode", pt, d).def_auto()
    }

    fn new_allow_non_ascii_delim() -> Self {
        let n = "allow_non_ascii_delim";
        let d = "Choose how to handle non-ASCII delimiters (outside 1-126).";
        let e = PyreflowError::FileLayout;
        Self::new_tri_flag_param(n, true, "AllowNonAsciiDelim", d, e)
    }

    fn new_allow_even_delims() -> Self {
        let n = "allow_even_delims";
        let d = format!("Choose what happens if {TEXT} has an even number of delimiters.");
        let e = PyreflowError::FileLayout;
        Self::new_tri_flag_param(n, true, "AllowEvenDelims", d, e)
    }

    fn new_allow_nonunique() -> Self {
        let d = format!(
            "Choose how to handle non-unique keys in {TEXT}. In such cases, \
             only the first will be used regardless of this setting."
        );
        let e = PyreflowError::ParseKey;
        Self::new_tri_flag_param("allow_nonunique", true, "AllowNonunique", d, e)
    }

    fn new_allow_odd_tokens() -> Self {
        let d = format!(
            "Choose what happens if {TEXT} contains an odd number of tokens. \
             The last 'dangling' token will be dropped regardless."
        );
        let e = PyreflowError::FileLayout;
        Self::new_tri_flag_param("allow_odd_tokens", true, "AllowOddTokens", d, e)
    }

    fn new_allow_empty_keys() -> Self {
        let d = "Choose what happens if any keys are blank. Only relevant if \
                 if delimiters are unescaped.";
        let e = PyreflowError::FileLayout;
        Self::new_tri_flag_param("allow_empty_keys", true, "AllowEmptyKeys", d, e)
    }

    fn new_allow_delim_at_boundary() -> Self {
        let n = "allow_delim_at_boundary";
        let d = "Choose what happens if there are delimiters at token boundaries. \
                 The FCS standard forbids this because it is impossible to tell \
                 if such delimiters belong to the previous or the next token. \
                 Consequently, delimiters at boundaries will be dropped regardless \
                 of this flag. Only relevant if delimiters are escaped.";
        let e = PyreflowError::FileLayout;
        Self::new_tri_flag_param(n, true, "AllowDelimAtBoundary", d, e)
    }

    fn new_use_latin1() -> Self {
        let d = format!(
            "If {TRUE} interpret all characters in {TEXT} as Latin-1 (aka \
             ISO/IEC 8859-1) instead of UTF-8."
        );
        Self::new_bool_param("use_latin1", d)
    }

    fn new_allow_non_ascii_keys() -> Self {
        let n = "allow_non_ascii_keys";
        let d = "Choose how to handle non-ASCII keys. This only applies to \
                 non-standard keywords, as all standardized keywords may only \
                 contain letters, numbers, and start with {DOLLAR_STR}. Regardless, all \
                 compliant keys must only have ASCII.";
        let e = PyreflowError::FileLayout;
        Self::new_tri_flag_param(n, true, "AllowNonAsciiKeywords", d, e)
    }

    fn new_allow_non_utf8_values() -> Self {
        let d = format!(
            "Choose what happens if non-UTF8 characters are in {TEXT}. \
             Tokens with such characters will be dropped regardless."
        );
        let e = PyreflowError::FileLayout;
        Self::new_tri_flag_param("allow_non_utf8_values", true, "AllowNonUtf8", d, e)
    }

    fn new_allow_missing_supp_text() -> Self {
        let n = "allow_missing_supp_text";
        let d = format!(
            "Choose how to handle supplemental missing {TEXT} offsets in \
             primary {TEXT}."
        );
        let e = PyreflowError::FileLayout;
        Self::new_tri_flag_param(n, true, "AllowMissingSuppTEXT", d, e)
    }

    fn new_allow_supp_text_own_delim() -> Self {
        let n = "allow_supp_text_own_delim";
        let d = format!(
            "Choose what happens if supplemental {TEXT} has a different \
             delimiter compared to primary {TEXT}."
        );
        let e = PyreflowError::FileLayout;
        Self::new_tri_flag_param(n, true, "AllowSuppTEXTOwnDelim", d, e)
    }

    fn new_allow_missing_nextdata() -> Self {
        let n = "allow_missing_nextdata";
        let d = format!(
            "Choose how to handle missing {NEXTDATA}. This is a required \
             keyword in all versions. However, most files only have one dataset \
             in which case this keyword is meaningless."
        );
        let e = PyreflowError::FileLayout;
        Self::new_tri_flag_param(n, true, "AllowMissingNextdata", d, e)
    }

    fn new_trim_value_whitespace() -> Self {
        let d = format!(
            "Trim whitespace from beginning and end of all values. This may \
             create blank values if the starting string is entirely whitespace. \
             Set to {notrim} to not trim at all. Set to {trim}, {trim_blank_warn}, \
             or {trim_blank_nowarn} to enable trimming and throw error, warning, \
             or nothing when trimming results in a blank.",
            notrim = code_str(tc::TRIM_NONE_LEVEL),
            trim = code_str(tc::TRIM_ERROR_LEVEL),
            trim_blank_warn = code_str(tc::TRIM_BLANK_WARN_LEVEL),
            trim_blank_nowarn = code_str(tc::TRIM_BLANK_SILENT_LEVEL),
        );
        let rstype = types_config_path("TrimValueWhitespace");
        let pt = PyLiteral::new1(tc::TrimValueWhitespace::iter_str()).rstype(rstype);
        Self::new_param("trim_value_whitespace", pt, d).def_auto()
    }

    fn new_ignore_standard_keys() -> Self {
        let d = format!(
            "Remove standard keys from {TEXT}. The leading {DOLLAR_STR} \
             is implied so do not include it."
        );
        Self::new_key_patterns_param("ignore_standard_keys", d)
    }

    fn new_promote_to_standard() -> Self {
        let d = format!("Promote nonstandard keys to standard keys in {TEXT}.");
        Self::new_key_patterns_param("promote_to_standard", d)
    }

    fn new_demote_from_standard() -> Self {
        let d = format!("Demote nonstandard keys from standard keys in {TEXT}.");
        Self::new_key_patterns_param("demote_from_standard", d)
    }

    fn new_key_patterns_param(argname: &str, desc: impl fmt::Display) -> Self {
        let common = format!(
            "Values that start and end with {delim} will be \
             interpreted as regular expressions.",
            delim = tc::PATTERN_DELIMITER
        );
        let d = format!("{desc} {common}");
        Self::new_param(argname, PyList::new_key_patterns(), d).def_auto()
    }

    fn new_rename_standard_keys() -> Self {
        let d = format!(
            "Rename standard keys in {TEXT}. Keys matching the first part of \
             the pair will be replaced by the second. Comparisons are case \
             insensitive. The leading {DOLLAR_STR} is implied so do not include it."
        );
        Self::new_param("rename_standard_keys", PyDict::new_keystring_pairs(), d).def_auto()
    }

    fn new_replace_standard_key_values() -> Self {
        Self::new_param(
            "replace_standard_key_values",
            PyDict::new1(PyStr::new_keystring(), PyStr::new_ne_str()),
            format!(
                "Replace values for standard keys in {TEXT} Comparisons are case \
                 insensitive. The leading {DOLLAR_STR} is implied so do not include it."
            ),
        )
        .def_auto()
    }

    fn new_substitute_standard_key_values() -> Self {
        let d = format!(
            "Apply sed-like substitution operation on matching standard keys. \
             The leading {DOLLAR_STR} is implied when matching keys. The first \
             dict corresponds to keys which are matched literally, and the \
             second corresponds to keys which are matched via regular \
             expression. The members in the 3-tuple values correspond to a \
             regular expression, replacement string, and global flag \
             respectively. The regular expression may contain capture \
             expressions which must be matched exactly in the replacement \
             string. If the global flag is {TRUE}, replace all found \
             matches, otherwise only replace the first. Any references in \
             replacement string must be given with surrounding brackets \
             like {bracket0} or {bracket1}.",
            bracket0 = code_str("${1}"),
            bracket1 = code_str("${cygnus}"),
        );
        let p = PyDict::new_sub_patterns();
        Self::new_param("substitute_standard_key_values", p, d).def_auto()
    }

    fn new_append_standard_keywords() -> Self {
        Self::new_param(
            "append_standard_keywords",
            PyDict::new1(PyStr::new_keystring(), PyStr::new_ne_str()),
            format!(
                "Append standard key/value pairs to {TEXT}. All keys and values \
                 will be included as they appear here. The leading {DOLLAR_STR} \
                 is implied so do not include it."
            ),
        )
        .def_auto()
    }

    fn new_text_data_correction_param() -> Self {
        Self::new_config_correction_arg("text_data_correction", AnySegment::Data, false)
    }

    fn new_text_analysis_correction_param() -> Self {
        Self::new_config_correction_arg("text_analysis_correction", AnySegment::Analysis, false)
    }

    fn new_ignore_text_data_offsets_param() -> Self {
        let d = format!("If {TRUE} ignore {DATA} offsets in {TEXT}");
        Self::new_bool_param("ignore_text_data_offsets", d)
    }

    fn new_ignore_text_analysis_offsets_param() -> Self {
        let d = format!("If {TRUE} ignore {ANALYSIS} offsets in {TEXT}");
        Self::new_bool_param("ignore_text_analysis_offsets", d)
    }

    fn new_allow_header_text_offset_mismatch_param() -> Self {
        let exc = PyreflowError::FileLayout.fmt_ref();
        let n = "allow_header_text_offset_mismatch";
        let d = format!(
            "Allow {HEADER} and {TEXT} offsets to be different. If \
             {header_warn} or {header_silent}, choose {HEADER} and throw \
             a warning or nothing on mismatch. If {text_warn} or {text_silent} \
             behave analogously for {TEXT}. If {error} throw {exc}",
            header_warn = code_str(tc::MISMATCH_HEADER_WARN_LEVEL),
            header_silent = code_str(tc::MISMATCH_HEADER_SILENT_LEVEL),
            text_warn = code_str(tc::MISMATCH_TEXT_WARN_LEVEL),
            text_silent = code_str(tc::MISMATCH_TEXT_SILENT_LEVEL),
            error = code_str(tc::MISMATCH_ERROR_LEVEL),
        );
        let path = types_config_path("AllowHeaderTEXTOffsetMismatch");
        let pt = PyLiteral::new1(tc::AllowHeaderTEXTOffsetMismatch::iter_str()).rstype(path);
        Self::new_param(n, pt, d).def_auto()
    }

    fn new_allow_missing_required_offsets_param(version: Option<Version>) -> Self {
        let n = "allow_missing_required_offsets";
        let s = match version {
            Some(Version::FCS3_2) => DATA.into(),
            Some(_) => format!("{DATA} and {ANALYSIS}"),
            None => format!("{DATA} and {ANALYSIS} (3.1 or lower)"),
        };
        let d = format!(
            "Choose what happens when required {s} offsets in {TEXT} are be missing. \
             If missing, fall back to offsets from {HEADER}."
        );
        let e = PyreflowError::FileLayout;
        Self::new_tri_flag_param(n, true, "AllowMissingRequiredOffsets", d, e)
    }

    fn new_allow_uneven_event_width_param() -> Self {
        let n = "allow_uneven_event_width";
        let d = format!(
            "Choose what to do when event width does not perfectly divide length \
             of {DATA}. Does not apply to delimited ASCII data schema."
        );
        let e = PyreflowError::FileLayout;
        Self::new_tri_flag_param(n, true, "AllowUnevenEventWidth", d, e)
    }

    fn new_allow_tot_mismatch_param() -> Self {
        let d = format!(
            "Choose what happens when {TOT} does not match number of events as \
             computed by the event width and length of {DATA}. \
             Does not apply to delimited ASCII data schema."
        );
        let e = PyreflowError::FileLayout;
        Self::new_tri_flag_param("allow_tot_mismatch", true, "AllowTotMismatch", d, e)
    }

    fn new_checked_range_datatypes() -> Self {
        let path = types_config_path("CheckedRangeDatatypes");
        let d = format!(
            "Control which measurements will be checked via {PNR}. If \
             {int}, check integer measurements only. If {all}, check all \
             measurements. If {none}, check nothing.",
            int = code_str(tc::CHECK_RANGE_INT_ONLY_LEVEL),
            all = code_str(tc::CHECK_RANGE_ALL_LEVEL),
            none = code_str(tc::CHECK_RANGE_NONE_LEVEL),
        );
        let pt = PyLiteral::new1(tc::CheckedRangeDatatypes::iter_str()).rstype(path);
        Self::new_param(CHECKED_RANGE_DATATYPES, pt, d).def_auto()
    }

    fn new_disallow_over_range() -> Self {
        let n = "disallow_over_range";
        let d = format!(
            "Choose how to report event values in {DATA} which exceed {PNR}. \
             This only has an effect if the column is checked \
             according to {arg}.",
            arg = arg(CHECKED_RANGE_DATATYPES),
        );
        let e = PyreflowError::EventData;
        Self::new_tri_flag_param(n, false, "DisallowOverRange", d, e)
    }

    fn new_over_range_action() -> Self {
        let n = "over_range_action";
        let d = format!(
            "Choose what to do with event values in {DATA} which exceed {PNR}. \
             This only has an effect if the column is checked \
             according to {arg}. Pass {error} to emit error, {warn} to emit \
             warning, {silent} to do nothing, {trunc_warn} to truncate and emit \
             warning, and {trunc_silent} to truncate with no warning.",
            arg = arg(CHECKED_RANGE_DATATYPES),
            error = code_str(tc::OVERRANGE_ACTION_ERROR_LEVEL),
            warn = code_str(tc::OVERRANGE_ACTION_WARN_LEVEL),
            silent = code_str(tc::OVERRANGE_ACTION_SILENT_LEVEL),
            trunc_warn = code_str(tc::OVERRANGE_ACTION_TRUNCATE_WARN_LEVEL),
            trunc_silent = code_str(tc::OVERRANGE_ACTION_TRUNCATE_SILENT_LEVEL),
        );
        let path = types_config_path("OverRangeAction");
        let pt = PyLiteral::new1(tc::OverRangeAction::iter_str()).rstype(path);
        Self::new_param(n, pt, d).def_auto()
    }

    fn new_row_buffer_size(is_reader: bool) -> Self {
        let act = if is_reader { "read" } else { "write" };
        let d = format!(
            "Set the size in bytes for the internal buffer used to {act} {DATA}. \
             This is a performance parameter that balances read syscalls (too low) \
             and cache misses (too high). It should generally be 90% of the CPU's \
             L1D cache size."
        );
        let path = parse_quote!(fireflow_types::config::RowBufferSize);
        let pt = PyInt::new_int(RsInt::Usize).rstype(path);
        let def = tc::RowBufferSize::default().into();
        Self::new_param("row_buffer_size", pt, d).def(DocDefault::Int(def))
    }

    fn new_warnings_are_errors_param() -> Self {
        let d = format!("If {TRUE} all warnings will be regarded as errors.");
        Self::new_bool_param("warnings_are_errors", d)
    }

    fn new_hide_warnings_param() -> Self {
        Self::new_bool_param("hide_warnings", format!("If {TRUE} hide all warnings."))
    }
}

impl DocDefault {
    fn as_value(&self, pytype: &ArgPyType) -> (String, TokenStream2) {
        let err = || {
            panic!(
                "Arg type '{}' does not match default type '{}'",
                pytype,
                self.as_type()
            )
        };
        let py_str = |s| format!("\"{s}\"");
        match (self, pytype) {
            (Self::Auto, _) => pytype.doc_default(),
            (Self::Int(x), PyType::Int(_)) => (x.to_string(), pytype.doc_default().1),
            (Self::Str(x), PyType::Str(_)) => (py_str(x), pytype.doc_default().1),
            (dt, PyType::Option(pt)) => match (dt, &pt.inner) {
                (Self::Int(x), PyType::Int(y)) => (x.to_string(), y.doc_default().1),
                (Self::Str(x), PyType::Str(y)) => (py_str(x), y.doc_default().1),
                _ => err(),
            },
            _ => err(),
        }
    }

    // for error reporting
    fn as_type(&self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Str(_) => "str",
            Self::Int(_) => "int",
        }
    }
}

impl ClassDocString {
    fn new_class(summary: impl fmt::Display) -> Self {
        Self::new(summary.to_string(), vec![], vec![], ())
    }

    fn into_impl_class<F>(
        self,
        name: impl fmt::Display,
        path: &Path,
        constr: F,
    ) -> (Ident, TokenStream2)
    where
        F: FnOnce(TokenStream2) -> TokenStream2,
    {
        let (pyname, wrapped) = self.as_impl_wrapped(name, path);
        let sig = self.sig();
        let get_set_methods = self.quoted_methods();
        let new = constr(self.fun_args());
        let s = quote! {
            #wrapped

            #[pymethods]
            impl #pyname {
                #sig
                #[new]
                #[allow(clippy::too_many_arguments)]
                #new

                #get_set_methods

                // allow all classes to be deepcopy-ed
                fn __deepcopy__(&self, _memo: &Bound<'_, pyo3::PyAny>) -> Self {
                    self.clone()
                }
            }
        };
        (pyname, s)
    }

    fn as_impl_wrapped(&self, name: impl fmt::Display, path: &Path) -> (Ident, TokenStream2) {
        let doc = self.doc();
        let n = name.to_string();
        let pyname = format_ident!("Py{name}");
        let q = quote! {
            // pyo3 currently cannot add docstrings to __new__ methods, see
            // https://github.com/PyO3/pyo3/issues/4326
            //
            // workaround, put them on the structs themselves, which works but has the
            // disadvantage of being not next to the method def itself
            #doc
            #[pyclass(name = #n, eq)]
            #[derive(Clone, From, Into, PartialEq)]
            pub struct #pyname(#path);
        };
        (pyname, q)
    }
}

impl MethodDocString {
    fn new_method(summary: impl fmt::Display) -> Self {
        Self::new(summary.to_string(), vec![], vec![], None)
    }

    fn returns(self, returns: DocReturn<RetPyType>) -> Self {
        Self::new(self.summary, self.paragraphs, self.args, Some(returns))
    }
}

impl IvarDocString {
    fn new_ivar(summary: impl fmt::Display, ret_type: impl Into<ArgPyType>) -> Self {
        Self::new(summary.to_string(), vec![], (), DocReturn::new(ret_type))
    }

    fn ret_desc(self, desc: impl fmt::Display) -> Self {
        Self::new(self.summary, self.paragraphs, (), self.returns.desc(desc))
    }

    fn into_impl_get(
        mut self,
        class: &Ident,
        name: impl fmt::Display,
        f: impl FnOnce(&Ident, &ArgPyType) -> TokenStream2,
    ) -> TokenStream2 {
        self.append_summary_or_paragraph("read-only", "This attribute is read-only.");
        let i = format_ident!("{name}");
        let pt = &self.returns.rtype;
        let rt = pt.as_rust_type();
        let body = f(&i, pt);
        let doc = self.doc();
        quote! {
            #[pymethods]
            impl #class {
                #doc
                #[getter]
                fn #i(&self) -> #rt {
                    #body
                }
            }
        }
    }

    fn into_impl_get_set(
        mut self,
        class: &Ident,
        name: impl fmt::Display,
        fallible: bool,
        get_fun: impl FnOnce(&Ident, &ArgPyType) -> TokenStream2,
        set_fun: impl FnOnce(&Ident, &ArgPyType) -> TokenStream2,
    ) -> TokenStream2 {
        self.append_summary_or_paragraph("read-write", "This attribute is read-write.");
        let get = format_ident!("{name}");
        let set = format_ident!("set_{get}");
        let pt = &self.returns.rtype;
        let rt = pt.as_rust_type();
        let get_body = get_fun(&get, pt);
        let set_body = set_fun(&get, pt);
        let doc = self.doc();
        let ret = if fallible {
            quote!(PyResult<()>)
        } else {
            quote!(())
        };
        quote! {
            #[pymethods]
            impl #class {
                #doc
                #[getter]
                fn #get(&self) -> #rt {
                    #get_body
                }

                #[setter]
                fn #set(&mut self, #get: #rt) -> #ret {
                    #set_body
                }
            }
        }
    }
}

impl FunDocString {
    fn new_fun(summary: impl fmt::Display) -> Self {
        Self::new(summary.to_string(), vec![], vec![], None)
    }

    fn returns(self, returns: DocReturn<RetPyType>) -> Self {
        Self::new(self.summary, self.paragraphs, self.args, Some(returns))
    }
}

impl<A, S> DocString<A, Option<DocReturn<RetPyType>>, S> {
    fn ret_path(&self) -> TokenStream2 {
        self.returns
            .as_ref()
            .map(|x| {
                let inner = x.rtype.as_rust_type().to_token_stream();
                if x.exceptions.is_empty() {
                    inner
                } else {
                    quote!(PyResult<#inner>)
                }
            })
            .unwrap_or(quote!(()))
    }
}

impl<A, R, S> DocString<Vec<A>, R, S> {
    fn arg(mut self, arg: impl Into<A>) -> Self {
        self.args.push(arg.into());
        self
    }

    fn args(mut self, args: impl IntoIterator<Item = impl Into<A>>) -> Self {
        self.args.extend(args.into_iter().map(Into::into));
        self
    }

    /// Emit typed argument list for use in rust function signature
    fn fun_args(&self) -> TokenStream2
    where
        A: IsDocArg,
    {
        let xs: Vec<_> = self.args.iter().map(IsDocArg::fun_arg).collect();
        quote!(#(#xs),*)
    }

    /// Emit identifiers associated with function arguments
    fn idents(&self) -> TokenStream2
    where
        A: IsDocArg,
    {
        let xs: Vec<_> = self.args.iter().map(IsDocArg::ident).collect();
        quote!(#(#xs),*)
    }

    fn idents_into(&self) -> TokenStream2
    where
        A: IsDocArg,
    {
        let xs: Vec<_> = self.args.iter().map(IsDocArg::ident_into).collect();
        quote!(#(#xs),*)
    }

    /// Emit get/set methods associated with arguments (if any)
    fn quoted_methods(&self) -> TokenStream2
    where
        A: IsMethods,
    {
        let xs: Vec<_> = self.args.iter().map(IsMethods::quoted_methods).collect();
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

    fn sig(&self) -> TokenStream2
    where
        A: IsDocArg,
        S: IsSelfArg,
    {
        assert!(
            self.has_defaults().is_some(),
            "non-default args after default args"
        );

        let ps = &self.args;
        let (flat_sig, txt_sig_): (Vec<_>, Vec<_>) = ps
            .iter()
            .map(|a| {
                let n = &a.argname();
                let i = format_ident!("{n}");
                if let Some(d) = a.default() {
                    let (t, r) = d.as_value(a.pytype());
                    // let t = d.as_py_value();
                    (quote! {#i=#r}, format!("{n}={t}"))
                } else {
                    (quote! {#i}, (*n).into())
                }
            })
            .unzip();
        let txt_sig = format!(
            "({})",
            S::ARG
                .into_iter()
                .chain(txt_sig_.iter().map(String::as_str))
                .join(", ")
        );
        quote! {
            #[pyo3(signature = (#(#flat_sig),*))]
            #[pyo3(text_signature = #txt_sig)]
        }
    }
}

impl<A, R, S> DocString<A, R, S> {
    fn para(mut self, paragraph: impl fmt::Display) -> Self {
        self.paragraphs.push(paragraph.to_string());
        self
    }

    fn paras(mut self, paragraphs: impl IntoIterator<Item = impl fmt::Display>) -> Self {
        self.paragraphs
            .extend(paragraphs.into_iter().map(|x| x.to_string()));
        self
    }

    fn doc(&self) -> TokenStream2
    where
        Self: fmt::Display,
    {
        let s = self.to_string();
        quote! {#[doc = #s]}
    }

    fn append_paragraph(&mut self, p: impl fmt::Display) {
        self.paragraphs.extend([p.to_string()]);
    }

    fn append_summary_or_paragraph(&mut self, suffix: impl fmt::Display, para: impl fmt::Display) {
        let new_summary = format!("{} ({suffix}).", self.summary.trim_end_matches('.'));
        if new_summary.len() > MAX_LINE_LEN {
            self.append_paragraph(para);
        } else {
            self.summary = new_summary;
        }
    }

    fn fmt_inner<'a, 'b, F0, F1, F2, F3, I0, I1, I2>(
        &'a self,
        f_args: F0,
        f_return: F1,
        f_args_exc: F2,
        f_return_exc: F3,
        f: &mut fmt::Formatter<'b>,
    ) -> Result<(), fmt::Error>
    where
        F0: FnOnce(&'a A) -> I0,
        F1: FnOnce(&'a R) -> Option<String>,
        F2: FnOnce(&'a A) -> I1,
        F3: FnOnce(&'a R) -> I2,
        I0: Iterator<Item = String> + 'a,
        I1: Iterator<Item = NamedPyException> + 'a,
        I2: Iterator<Item = &'a ReturnPyException> + 'a,
    {
        let ps = self
            .paragraphs
            .iter()
            .map(|s| fmt_docstring_nonparam(s.as_str()));
        let a = f_args(&self.args);
        let r = f_return(&self.returns);
        let rest = ps.chain(a).chain(r).join("\n\n");
        let arg_es = f_args_exc(&self.args).join("\n");
        let ret_es = f_return_exc(&self.returns).join("\n");
        assert!(self.summary.len() <= MAX_LINE_LEN, "summary is too long");
        write!(f, "{}\n\n{rest}\n\n{arg_es}\n\n{ret_es}", self.summary)
    }
}

impl<A, R, S> ToTokens for DocString<Vec<A>, R, S>
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
        self.fmt_inner(
            |a| a.iter().map(ToString::to_string),
            |()| None,
            |a| {
                let es = a.iter().flat_map(|x| {
                    x.pytype()
                        .as_exceptions()
                        .into_iter()
                        .map(|e| e.into_named(x.argname()))
                });
                NamedPyException::merge(es).into_iter()
            },
            |()| empty(),
            f,
        )
    }
}

impl fmt::Display for IvarDocString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        self.fmt_inner(
            |()| empty(),
            |r| Some(r.to_string()),
            |()| empty(),
            |r| r.exceptions.iter(),
            f,
        )
    }
}

impl<A: fmt::Display + IsDocArg, S> fmt::Display
    for DocString<Vec<A>, Option<DocReturn<RetPyType>>, S>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        self.fmt_inner(
            |a| a.iter().map(ToString::to_string),
            |r| r.as_ref().map(ToString::to_string),
            |a| {
                let es = a.iter().flat_map(|x| {
                    x.pytype()
                        .as_exceptions()
                        .into_iter()
                        .map(|e| e.into_named(x.argname()))
                });
                NamedPyException::merge(es).into_iter()
            },
            |r| r.as_ref().map(|x| &x.exceptions).into_iter().flatten(),
            f,
        )
    }
}

impl fmt::Display for NamedPyException {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let pn = &self.inner.inner.pyname;
        let ns: Vec<_> = self.names.iter().map(arg).collect();
        let ns_ = fmt_comma_sep_list(&ns[..], "or");
        let n = self.inner.argmod.fmt(&ns_);
        if let Some(d) = self.inner.inner.desc.as_ref() {
            assert!(
                d.contains(ARG_TOKEN),
                "does not contain name ref ('{ARG_TOKEN}'): {d}"
            );
            let dd = d.replace(ARG_TOKEN, &n);
            write!(f, ":raises {pn}: {dd}")
        } else {
            write!(f, ":raises {pn}:")
        }
    }
}

impl fmt::Display for ReturnPyException {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let pn = &self.0.pyname;
        if let Some(d) = self.0.desc.as_ref() {
            write!(f, ":raises {pn}: {d}")
        } else {
            write!(f, ":raises {pn}:")
        }
    }
}

impl<T: IsArgType> fmt::Display for DocArg<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let ro = match T::readonly() {
            Some(true) => "(read-only) ",
            Some(false) => "(read-write) ",
            None => "",
        };
        let pt = &self.pytype;
        let n = &self.argname;
        let d = self
            .default
            .as_ref()
            .map(|d| d.as_value(pt).0)
            .map_or(self.desc.clone(), |def| {
                format!("{} Defaults to {}.", self.desc, code(def))
            });
        let tn = T::TYPENAME;
        let at = T::ARGTYPE;
        let s0 = fmt_docstring_param(format!(":{at} {n}: {ro}{d}").as_str());
        let s1 = fmt_docstring_param(format!(":{tn} {n}: {pt}").as_str());
        write!(f, "{s0}\n{s1}")
    }
}

// TODO its a bit weird to totally ignore exceptions here, which makes it seem
// like this should be an inner type that wrapped with the stuff pertaining to
// exceptions
impl<T: fmt::Display> fmt::Display for DocReturn<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let t = fmt_docstring_param(format!(":rtype: {}", self.rtype).as_str());
        if let Some(d) = self
            .desc
            .as_ref()
            .map(|d| fmt_docstring_param(format!(":returns: {d}").as_str()))
        {
            write!(f, "{d}\n{t}")
        } else {
            f.write_str(t.as_str())
        }
    }
}

impl<R> fmt::Display for PyAtom<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Self::Str => PyStr::<R>::default().fmt(f),
            Self::Bool => PyBool::<R>::default().fmt(f),
            Self::Bytes => PyBytes::<R>::default().fmt(f),
            // dummy u8
            Self::Int => PyInt::<R>::from(RsInt::U8).fmt(f),
            // dummy f32
            Self::Float => PyFloat::<R>::from(RsFloat::F32).fmt(f),
            Self::Decimal => PyDecimal::<R>::default().fmt(f),
            Self::Datetime => PyDatetime::<R>::default().fmt(f),
            Self::Date => PyDate::<R>::default().fmt(f),
            Self::Time => PyTime::<R>::default().fmt(f),
            Self::None => f.write_str("None"),
            Self::Dict(k, v) => write!(f, ":py:class:`dict`\\ [{k}, {v}]"),
            Self::Tuple(xs) => {
                let s = if xs.is_empty() {
                    "()".into()
                } else {
                    xs.iter().join(", ")
                };
                write!(f, ":py:class:`tuple`\\ [{s}]")
            }
            Self::List(x) => write!(f, ":py:class:`list`\\ [{x}]"),
            Self::Literal(x) => x.fmt(f),
            Self::PyClass(x) => x.fmt(f),
            Self::Union(x0, x1, xs) => {
                let s = [&*(*x0), &*(*x1)].into_iter().chain(xs.iter()).join(" | ");
                write!(f, "{s}",)
            }
        }
    }
}

macro_rules! impl_display_pycomplex {
    ($t:ident) => {
        impl<R: Clone + PartialEq + Eq + Hash> fmt::Display for $t<R> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
                write!(f, "{}", self.as_atom().flatten_unions())
            }
        }
    };
}

impl_display_pycomplex!(PyOpt);
impl_display_pycomplex!(PyUnion);
impl_display_pycomplex!(PyDict);
impl_display_pycomplex!(PyList);
impl_display_pycomplex!(PyTuple);

impl fmt::Display for PyLiteral {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            ":obj:`~typing.Literal`\\ [{}]",
            once(&self.head)
                .chain(self.tail.iter())
                .map(|s| format!("\"{s}\""))
                .join(", ")
        )
    }
}

impl<R> fmt::Display for PyClass<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, ":py:class:`{}`", self.pyname)
    }
}

macro_rules! impl_display_pytype {
    ($t:ident, $s:expr) => {
        impl<R> fmt::Display for $t<R> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
                write!(f, ":py:class:`{}`", $s)
            }
        }
    };
}

impl_display_pytype!(PyBool, "bool");
impl_display_pytype!(PyStr, "str");
impl_display_pytype!(PyBytes, "bytes");
impl_display_pytype!(PyInt, "int");
impl_display_pytype!(PyFloat, "float");
impl_display_pytype!(PyDecimal, "~decimal.Decimal");
impl_display_pytype!(PyDate, "~datetime.date");
impl_display_pytype!(PyTime, "~datetime.time");
impl_display_pytype!(PyDatetime, "~datetime.datetime");

impl fmt::Display for SegmentSrc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let s = match self {
            Self::Header => HEADER,
            Self::Any => formatcp!("{HEADER} or {TEXT}"),
        };
        f.write_str(s)
    }
}

impl fmt::Display for UncorrSegmentSrc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let s = match self {
            Self::Header => HEADER,
            Self::Text => TEXT,
        };
        f.write_str(s)
    }
}

fn fmt_docstring_nonparam(s: &str) -> String {
    fmt_hanging_indent(MAX_LINE_LEN, 0, s)
}

fn fmt_docstring_param(s: &str) -> String {
    fmt_hanging_indent(MAX_LINE_LEN, 4, s)
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
                tmp = vec![i.as_str()];
            } else {
                line_len = x.len();
                tmp = vec![];
            }
        } else {
            line_len += 1;
        }
        tmp.push(x);
    }
    zs.push(tmp.iter().join(" "));
    zs.iter().join("\n")
}

fn fmt_comma_sep_list<X: fmt::Display>(xs: &[X], conj: &str) -> String {
    let n = xs.len();
    match n.cmp(&2) {
        Ordering::Less => xs.iter().join(""),
        Ordering::Equal => xs.iter().join(&format!(" {conj} ")),
        Ordering::Greater => {
            let mut it = xs.iter();
            let x0 = it.by_ref().take(n - 1).join(", ");
            let c = format!(", {conj} ");
            once(x0).chain(it.map(ToString::to_string)).join(&c)
        }
    }
}

fn unwrap_type_as_path(ty: &Type) -> &Path {
    if let Type::Path(p) = ty {
        &p.path
    } else {
        panic!("not a path")
    }
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

fn path_strip_args(mut path: Path) -> Path {
    for segment in &mut path.segments {
        segment.arguments = PathArguments::None;
    }
    path
}

fn element_path(version: Version) -> Path {
    let otype = pyoptical(version);
    let ttype = pytemporal(version);
    let element_path = quote!(fireflow_core::text::named_vec::Element);
    parse_quote!(#element_path<#ttype, #otype>)
}

fn keyword_path(n: &str) -> Path {
    let t = format_ident!("{n}");
    parse_quote!(fireflow_core::text::keywords::#t)
}

fn config_path(n: &str) -> Path {
    let t = format_ident!("{n}");
    parse_quote!(fireflow_core::config::#t)
}

fn types_config_path(n: &str) -> Path {
    let t = format_ident!("{n}");
    parse_quote!(fireflow_types::config::#t)
}

fn pyoptical(version: Version) -> Ident {
    format_ident!("PyOptical{}", version.short_underscore())
}

fn pytemporal(version: Version) -> Ident {
    format_ident!("PyTemporal{}", version.short_underscore())
}

fn make_data_schema_datatype(pyname: &Ident, dt: &str) -> TokenStream2 {
    let d = format!("The value of {DATATYPE}.");
    let doc = DocString::new_ivar(d, PyLiteral::new_datatype())
        .paras([format!("Will always return {dt}.", dt = code_str(dt))]);
    doc.into_impl_get(pyname, "datatype", |_, _| quote!(self.0.datatype().into()))
}

fn make_byte_width(pyname: &Ident, nbytes: usize) -> TokenStream2 {
    let s0 = format!("Will always return {}.", code(nbytes));
    let s1 = format!(
        "This corresponds to the value of {PNB} divided by 8, which are \
         all equal for this data schema.",
    );
    let doc = DocString::new_ivar("The width of each measurement in bytes.", RsInt::Usize)
        .paras([s0, s1]);

    doc.into_impl_get(pyname, "byte_width", |_, _| quote!(#nbytes))
}

impl Version {
    #[must_use]
    fn short(self) -> &'static str {
        match self {
            Self::FCS2_0 => "2.0",
            Self::FCS3_0 => "3.0",
            Self::FCS3_1 => "3.1",
            Self::FCS3_2 => "3.2",
        }
    }

    #[must_use]
    fn short_underscore(self) -> &'static str {
        match self {
            Self::FCS2_0 => "2_0",
            Self::FCS3_0 => "3_0",
            Self::FCS3_1 => "3_1",
            Self::FCS3_2 => "3_2",
        }
    }

    // #[must_use]
    // pub fn from_short(s: &str) -> Option<Self> {
    //     match s {
    //         "2.0" => Some(Self::FCS2_0),
    //         "3.0" => Some(Self::FCS3_0),
    //         "3.1" => Some(Self::FCS3_1),
    //         "3.2" => Some(Self::FCS3_2),
    //         _ => None,
    //     }
    // }

    #[must_use]
    fn from_short_underscore(s: &str) -> Option<Self> {
        match s {
            "2_0" => Some(Self::FCS2_0),
            "3_0" => Some(Self::FCS3_0),
            "3_1" => Some(Self::FCS3_1),
            "3_2" => Some(Self::FCS3_2),
            _ => None,
        }
    }
}

impl AnySegment {
    fn name(self) -> &'static str {
        match self {
            Self::PrimaryTEXT => formatcp!("Primary {TEXT}"),
            Self::SuppTEXT => formatcp!("Supplemental {TEXT}"),
            Self::Data => DATA,
            Self::Analysis => ANALYSIS,
            Self::Other => OTHER,
        }
    }

    fn id(self) -> Ident {
        let id = match self {
            Self::PrimaryTEXT => "PrimaryTextSegmentId",
            Self::SuppTEXT => "SupplementalTextSegmentId",
            Self::Data => "DataSegmentId",
            Self::Analysis => "AnalysisSegmentId",
            Self::Other => "OtherSegmentId",
        };
        format_ident!("{id}")
    }

    fn correction_path(self, is_header: bool) -> Path {
        let src = if is_header {
            "SegmentFromHeader"
        } else {
            "SegmentFromTEXT"
        };
        let s = format_ident!("{src}");
        let i = self.id();
        let root = quote!(fireflow_core::segment);
        parse_quote! (#root::OffsetCorrection<#root::#i, #root::#s>)
    }
}

impl Kw {
    fn fun_name(self) -> String {
        self.base_name().to_lowercase().replace('$', "")
    }

    fn kw(self) -> String {
        fcs_kw(self.base_name())
    }

    fn type_name(self) -> Path {
        let n = match self {
            Self::Mode => "Mode",
            Self::Mode3_2 => "Mode3_2",
            Self::Cyt => "Cyt",
            Self::Cyt3_2 => "Cyt3_2",
            Self::Abrt => "Abrt",
            Self::Com => "Com",
            Self::Cells => "Cells",
            Self::Exp => "Exp",
            Self::Fil => "Fil",
            Self::Inst => "Inst",
            Self::Lost => "Lost",
            Self::Op => "Op",
            Self::Proj => "Proj",
            Self::Smno => "Smno",
            Self::Src => "Src",
            Self::Sys => "Sys",
            Self::Cytsn => "Cytsn",
            Self::Unicode => "Unicode",
            Self::CSVBits => "CSVBits",
            Self::CSTot => "CSTot",
            Self::LastModifier => "LastModifier",
            Self::LastModified => "LastModified",
            Self::Originality => "Originality",
            Self::Plateid => "Plateid",
            Self::Platename => "Platename",
            Self::Wellid => "Wellid",
            Self::Vol => "Vol",
            Self::Flowrate => "Flowrate",
            Self::Carrierid => "Carrierid",
            Self::Carriertype => "Carriertype",
            Self::Locationid => "Locationid",
            Self::UnstainedInfo => "UnstainedInfo",
            Self::Spillover => return parse_quote!(fireflow_core::text::spillover::Spillover),
            Self::UnstainedCenters => "UnstainedCenters",
            Self::Tr => "Trigger",
        };
        keyword_path(n)
    }

    const fn base_name(self) -> &'static str {
        match self {
            Self::Mode | Self::Mode3_2 => tk::MODE,
            Self::Cyt | Self::Cyt3_2 => tk::CYT,
            Self::Abrt => tk::ABRT,
            Self::Com => tk::COM,
            Self::Cells => tk::CELLS,
            Self::Exp => tk::EXP,
            Self::Fil => tk::FIL,
            Self::Inst => tk::INST,
            Self::Lost => tk::LOST,
            Self::Op => tk::OP,
            Self::Proj => tk::PROJ,
            Self::Smno => tk::SMNO,
            Self::Src => tk::SRC,
            Self::Sys => tk::SYS,
            Self::Cytsn => tk::CYTSN,
            Self::Unicode => tk::UNICODE,
            Self::CSVBits => tk::CSVBITS,
            Self::CSTot => tk::CSTOT,
            Self::LastModifier => tk::LAST_MODIFIER,
            Self::LastModified => tk::LAST_MODIFIED,
            Self::Originality => tk::ORIGINALITY,
            Self::Plateid => tk::PLATEID,
            Self::Platename => tk::PLATENAME,
            Self::Wellid => tk::WELLID,
            Self::Vol => tk::VOL,
            Self::Flowrate => tk::FLOWRATE,
            Self::Carrierid => tk::CARRIERID,
            Self::Carriertype => tk::CARRIERTYPE,
            Self::Locationid => tk::LOCATIONID,
            Self::UnstainedInfo => tk::UNSTAINEDINFO,
            Self::Spillover => tk::SPILLOVER,
            Self::UnstainedCenters => tk::UNSTAINEDCENTERS,
            Self::Tr => tk::TR,
        }
    }

    fn as_pytype<E>(self) -> PyType<E>
    where
        E: From<PyException>,
    {
        let path = self.type_name();
        match self {
            Self::Mode => PyLiteral::new1(["L", "U", "C"]).rstype(path).into(),
            Self::Mode3_2 => PyOpt::new1(PyLiteral::new1(["L"]).rstype(path)).into(),
            Self::Cyt
            | Self::Com
            | Self::Cells
            | Self::Exp
            | Self::Fil
            | Self::Inst
            | Self::Op
            | Self::Proj
            | Self::Smno
            | Self::Src
            | Self::Sys
            | Self::Cytsn
            | Self::LastModifier
            | Self::Plateid
            | Self::Platename
            | Self::Wellid
            | Self::Flowrate
            | Self::Carrierid
            | Self::Carriertype
            | Self::Locationid
            | Self::UnstainedInfo => PyStr::default().rstype(path).into(),
            Self::Cyt3_2 => PyStr::new_ne_str_inner(path).into(),
            Self::Abrt | Self::Lost => PyOpt::new1(PyInt::new_u32().rstype(path)).into(),
            Self::CSVBits | Self::CSTot => PyInt::new_u32().rstype(path).into(),
            Self::Unicode => {
                let inner = PyTuple::new1(RsInt::U32).add(PyList::new1(PyStr::default()));
                PyOpt::new1(inner.rstype(path)).into()
            }
            Self::LastModified => PyOpt::new1(PyDatetime::default().rstype(path)).into(),
            Self::Originality => {
                let choices = ["Original", "NonDataModified", "Appended", "DataModified"];
                PyOpt::new1(PyLiteral::new1(choices).rstype(path)).into()
            }
            Self::Vol => PyOpt::new1(PyFloat::new_non_negative_float().rstype(path)).into(),
            Self::Spillover => {
                // TODO add exception for when $PnN don't match
                let ed = format!("if {ARG_TOKEN} is not a square matrix that is 2x2 or larger");
                let matrix_exc = PyException::new_invalid_keyword().desc(ed);
                let d = format!(
                    "if matrix in {ARG_TOKEN} does not have the same number of rows \
                     and columns as the measurement vector",
                );
                let spill_exc = PyException::new_invalid_keyword().desc(d);
                let inner = PyTuple::new1(PyList::new1(PyStr::new_shortname()))
                    .add(PyClass::new1("~numpy.ndarray").exc(matrix_exc))
                    .exc(spill_exc);
                PyOpt::new1(inner.rstype(path)).into()
            }
            Self::UnstainedCenters => {
                PyDict::new(PyStr::new_shortname(), RsFloat::F32, path, None).into()
            }
            Self::Tr => {
                let inner = PyTuple::new1(PyInt::new_u32()).add(PyStr::new_shortname());
                PyOpt::new1(inner.rstype(path)).into()
            }
        }
    }
}

impl MeasKw {
    fn type_name(self) -> Path {
        let n = match self {
            Self::PnETemporal => "TemporalScale2_0",
            Self::PnS => "Longname",
            Self::PnF => "Filter",
            Self::PnL2_0 => "Wavelength",
            Self::PnL3_1 => "Wavelengths",
            Self::PnO => "Power",
            Self::PnT => "DetectorType",
            Self::PnP => "PercentEmitted",
            Self::PnV => "DetectorVoltage",
            Self::PnCALIBRATION3_1 => "Calibration3_1",
            Self::PnCALIBRATION3_2 => "Calibration3_2",
            Self::PnD => "Display",
            Self::PnDET => "DetectorName",
            Self::PnTAG => "Tag",
            Self::PnTYPETemporal => "TemporalType",
            Self::PnTYPEOptical => "OpticalType",
            Self::PnFEATURE => "Feature",
            Self::PnANALYTE => "Analyte",
            Self::PKn => "PeakBin",
            Self::PKNn => "PeakIndex",
        };
        keyword_path(n)
    }

    const fn fun_singular_name(self) -> &'static str {
        match self {
            Self::PnETemporal => "has_scale",
            Self::PnS => "longname",
            Self::PnF => "filter",
            Self::PnL2_0 => "wavelength",
            Self::PnL3_1 => "wavelengths",
            Self::PnO => "power",
            Self::PnT => "detector_type",
            Self::PnP => "percent_emitted",
            Self::PnV => "detector_voltage",
            Self::PnCALIBRATION3_1 | Self::PnCALIBRATION3_2 => "calibration",
            Self::PnD => "display",
            Self::PnDET => "detector_name",
            Self::PnTAG => "tag",
            Self::PnTYPETemporal => "has_type",
            Self::PnTYPEOptical => "measurement_type",
            Self::PnFEATURE => "feature",
            Self::PnANALYTE => "analyte",
            Self::PKn => "bin",
            Self::PKNn => "size",
        }
    }

    const fn kw(self) -> &'static str {
        match self {
            Self::PnETemporal => PNE,
            Self::PnS => fcs_kw!(tk::PNS),
            Self::PnF => fcs_kw!(tk::PNF),
            Self::PnL2_0 | Self::PnL3_1 => fcs_kw!(tk::PNL),
            Self::PnO => fcs_kw!(tk::PNO),
            Self::PnT => fcs_kw!(tk::PNT),
            Self::PnP => fcs_kw!(tk::PNP),
            Self::PnV => fcs_kw!(tk::PNV),
            Self::PnCALIBRATION3_1 | Self::PnCALIBRATION3_2 => fcs_kw!(tk::PNCALIBRATION),
            Self::PnD => fcs_kw!(tk::PND),
            Self::PnDET => fcs_kw!(tk::PNDET),
            Self::PnTAG => fcs_kw!(tk::PNTAG),
            Self::PnTYPETemporal | Self::PnTYPEOptical => PNTYPE,
            Self::PnFEATURE => PNFEATURE,
            Self::PnANALYTE => fcs_kw!(tk::PNANALYTE),
            Self::PKn => fcs_kw!(tk::PKN),
            Self::PKNn => fcs_kw!(tk::PKNN),
        }
    }

    const fn fun_plural_name(self) -> &'static str {
        match self {
            Self::PnS => "longnames",
            Self::PnF => "filters",
            Self::PnL2_0 | Self::PnL3_1 => "wavelengths",
            Self::PnO => "powers",
            Self::PnT => "detector_types",
            Self::PnP => "percents_emitted",
            Self::PnV => "detector_voltages",
            Self::PnCALIBRATION3_1 | Self::PnCALIBRATION3_2 => "calibrations",
            Self::PnD => "displays",
            Self::PnDET => "detector_names",
            Self::PnTAG => "tags",
            Self::PnFEATURE => "features",
            Self::PnANALYTE => "analytes",
            Self::PKn => "peak_bins",
            Self::PKNn => "peak_sizes",
            _ => panic!("plural names should not be used for this"),
        }
    }

    fn as_pytype<E>(self) -> PyType<E>
    where
        E: From<PyException>,
    {
        let path = self.type_name();
        let pf = PyFloat::new_positive_float();
        match self {
            Self::PnETemporal | Self::PnTYPETemporal => PyBool::default().rstype(path).into(),
            Self::PnS
            | Self::PnF
            | Self::PnT
            | Self::PnDET
            | Self::PnTAG
            | Self::PnANALYTE
            | Self::PnTYPEOptical => PyStr::default().rstype(path).into(),
            Self::PnFEATURE => PyOpt::new1(PyStr::default().rstype(path)).into(),
            Self::PnL2_0 => PyOpt::new1(pf.rstype(path)).into(),
            Self::PnL3_1 => PyList::new(pf, path, None).into(),
            Self::PnO | Self::PnP | Self::PnV => {
                PyOpt::new1(PyFloat::new_non_negative_float().rstype(path)).into()
            }
            Self::PnCALIBRATION3_1 => {
                let inner = PyTuple::new1(pf).add(PyStr::default());
                PyOpt::new1(inner.rstype(path)).into()
            }
            Self::PnCALIBRATION3_2 => {
                let inner = PyTuple::new1(pf).add(RsFloat::F32).add(PyStr::default());
                PyOpt::new1(inner.rstype(path)).into()
            }
            Self::PnD => {
                let desc = format!(
                    "if {ARG_TOKEN} represents a log display (field 1 is \
                     {TRUE}) and the two floats are not both positive"
                );
                let exc = PyException::new_value().desc(desc);
                let inner = PyTuple::new1(PyBool::default())
                    .add(RsFloat::F32)
                    .add(RsFloat::F32);
                PyOpt::new1(inner.exc(exc).rstype(path)).into()
            }
            Self::PKn | Self::PKNn => PyOpt::new1(PyInt::new_u32().rstype(path)).into(),
        }
    }

    fn optical_only(self) -> bool {
        !matches!(self, Self::PnS | Self::PnD | Self::PKn | Self::PKNn)
    }
}

const MAX_LINE_LEN: usize = 72;

const ALL_VERSIONS: [Version; 4] = [
    Version::FCS2_0,
    Version::FCS3_0,
    Version::FCS3_1,
    Version::FCS3_2,
];

const ALL_VERSION_STRINGS: [&str; 4] = ["FCS2.0", "FCS3.0", "FCS3.1", "FCS3.2"];

/// String to replace with argument name in exceptions attached to arguments.
const ARG_TOKEN: &str = "%x";

// formatted links used in many places

const CHRONO_REF: &str =
    "`chrono <https://docs.rs/chrono/latest/chrono/format/strftime/index.html>`__";

const REGEXP_REF: &str = "`regexp-syntax <https://docs.rs/regex/latest/regex/#syntax>`__";

// formatted python constants used all over the place

const TRUE: &str = code!("True");
const FALSE: &str = code!("False");
const NONE: &str = code!("None");
const UNIT: &str = code!("()");
const NAN: &str = code!("NaN");
const INF: &str = code!("inf");
const NEG_INF: &str = code!("-inf");
const DOLLAR_STR: &str = code_str!("$");
const FEATURE_AREA_STR: &str = code_str!(tk::OPT_FEATURE_AREA.as_str());
const FEATURE_WIDTH_STR: &str = code_str!(tk::OPT_FEATURE_WIDTH.as_str());
const FEATURE_HEIGHT_STR: &str = code_str!(tk::OPT_FEATURE_HEIGHT.as_str());
const BYTEORD_LITTLE_STR: &str = code_str!(tk::BYTEORD_LITTLE);
const BYTEORD_BIG_STR: &str = code_str!(tk::BYTEORD_BIG);

// argument names that are referenced in doc strings

const BIG_OTHER: &str = "big_other";
const MEASUREMENTS: &str = "measurements";
const MAX_OTHER: &str = "max_other";
const CHECKED_RANGE_DATATYPES: &str = "checked_range_datatypes";
const IGNORE_TIME_OPTICAL_KEYS: &str = "ignore_time_optical_keys";
const OTHER_WIDTH: &str = "other_width";
const UINT_RANGES: &str = "ranges";

// formatted segment names

const HEADER: &str = fcs_seg!("HEADER");
const TEXT: &str = fcs_seg!("TEXT");
const DATA: &str = fcs_seg!("DATA");
const ANALYSIS: &str = fcs_seg!("ANALYSIS");
const OTHER: &str = fcs_seg!("OTHER");

// formatted keywords

const PN_ANY: &str = fcs_kw!("$Pn\\*");
const GM_ANY: &str = fcs_kw!("$Gm\\*");
const RN_ANY: &str = fcs_kw!("$Rn\\*");

const NEXTDATA: &str = fcs_kw!(tk::NEXTDATA);
const DATATYPE: &str = fcs_kw!(tk::DATATYPE);
const BYTEORD: &str = fcs_kw!(tk::BYTEORD);
const TOT: &str = fcs_kw!(tk::TOT);
const TIMESTEP: &str = fcs_kw!(tk::TIMESTEP);
const DATE: &str = fcs_kw!(tk::DATE);
const BTIM: &str = fcs_kw!(tk::BTIM);
const ETIM: &str = fcs_kw!(tk::ETIM);
const BEGINDATETIME: &str = fcs_kw!(tk::BEGINDATETIME);
const ENDDATETIME: &str = fcs_kw!(tk::ENDDATETIME);
const PAR: &str = fcs_kw!(tk::PAR);
const GATE: &str = fcs_kw!(tk::GATE);
const GATING: &str = fcs_kw!(tk::GATING);
const RNI: &str = fcs_kw!(tk::RNI);
const RNW: &str = fcs_kw!(tk::RNW);
const PNR: &str = fcs_kw!(tk::PNR);
const PNB: &str = fcs_kw!(tk::PNB);
const PNN: &str = fcs_kw!(tk::PNN);
const PNE: &str = fcs_kw!(tk::PNE);
const GME: &str = fcs_kw!(tk::GME);
const PNG: &str = fcs_kw!(tk::PNG);
const PNDATATYPE: &str = fcs_kw!(tk::PNDATATYPE);
const PNFEATURE: &str = fcs_kw!(tk::PNFEATURE);
const PNTYPE: &str = fcs_kw!(tk::PNTYPE);

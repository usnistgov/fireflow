extern crate proc_macro;

use fireflow_core::header::Version;

use derive_more::{AsRef, Display, From};
use derive_new::new;
use itertools::Itertools as _;
use nonempty::NonEmpty;
use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote, ToTokens};
use std::cmp::Ordering;
use std::fmt;
use std::hash::Hash;
use std::iter::{empty, once};
use std::marker::PhantomData;
use std::string::ToString;
use syn::{
    parse::{Parse, ParseStream},
    parse_macro_input, parse_quote,
    token::Comma,
    GenericArgument, Ident, LitBool, LitInt, Path, PathArguments, Type,
};

#[proc_macro]
pub fn def_fcs_read_header(input: TokenStream) -> TokenStream {
    let fun_path = parse_macro_input!(input as Path);

    let conf_path = config_path("ReadHeaderConfig");

    let (conf_inner_path, args, inner_args) = DocArgParam::new_header_config_params();

    let exc = PyException::new("PyreflowException");

    let doc = DocString::new_fun(
        "Read the *HEADER* of an FCS file.",
        [""; 0],
        once(DocArg::new_path_param(true)).chain(args),
        Some(DocReturn::new(PyClass::new_py(["api"], "Header")).exc([exc])),
    );

    let fun_args = doc.fun_args();
    let ret_path = doc.ret_path();

    quote! {
        #[pyfunction]
        #doc
        #[allow(clippy::too_many_arguments)]
        pub fn fcs_read_header(#fun_args) -> #ret_path {
            let conf = #conf_path(#conf_inner_path { #(#inner_args),* });
            Ok(#fun_path(&path, &conf).py_termfail_resolve_nowarn()?.into())
        }
    }
    .into()
}

#[proc_macro]
pub fn def_fcs_read_raw_text(input: TokenStream) -> TokenStream {
    let fun_path = parse_macro_input!(input as Path);

    let conf_path = config_path("ReadRawTEXTConfig");

    let path_arg = DocArg::new_path_param(true);
    let (header_conf, header_args, header_recs) = DocArgParam::new_header_config_params();
    let (raw_conf, raw_args, raw_recs) = DocArgParam::new_raw_config_params();
    let (shared_conf, shared_args, shared_recs) = DocArgParam::new_shared_config_params();

    let exc = PyException::new("PyreflowException");

    let doc = DocString::new_fun(
        "Read *HEADER* and *TEXT* as key/value pairs from FCS file.",
        [""; 0],
        once(path_arg)
            .chain(header_args)
            .chain(raw_args)
            .chain(shared_args),
        Some(DocReturn::new(PyClass::new_py(["api"], "RawTEXTOutput")).exc([exc])),
    );

    let fun_args = doc.fun_args();
    let ret_path = doc.ret_path();

    quote! {
        #[pyfunction]
        #doc
        #[allow(clippy::too_many_arguments)]
        pub fn fcs_read_raw_text(#fun_args) -> #ret_path {
            let header = #header_conf { #(#header_recs),* };
            let raw = #raw_conf { header, #(#raw_recs),* };
            let shared = #shared_conf { #(#shared_recs),* };
            let conf = #conf_path { raw, shared };
            Ok(#fun_path(&path, &conf).py_termfail_resolve()?.into())
        }
    }
    .into()
}

#[proc_macro]
pub fn def_fcs_read_std_text(input: TokenStream) -> TokenStream {
    let fun_path = parse_macro_input!(input as Path);

    let conf_path = config_path("ReadStdTEXTConfig");

    let path_arg = DocArg::new_path_param(true);
    let (header_conf, header_args, header_recs) = DocArgParam::new_header_config_params();
    let (raw_conf, raw_args, raw_recs) = DocArgParam::new_raw_config_params();
    let (std_conf, std_args, std_recs) = DocArgParam::new_std_config_params(None);
    let (offsets_conf, offsets_args, offsets_recs) = DocArgParam::new_offsets_config_params(None);
    let (layout_conf, layout_args, layout_recs) = DocArgParam::new_layout_config_params(None);
    let (shared_conf, shared_args, shared_recs) = DocArgParam::new_shared_config_params();

    let exc = PyException::new("PyreflowException");

    let doc = DocString::new_fun(
        "Read *HEADER* and standardized *TEXT* from FCS file.",
        [""; 0],
        once(path_arg)
            .chain(header_args)
            .chain(raw_args)
            .chain(std_args)
            .chain(offsets_args)
            .chain(layout_args)
            .chain(shared_args),
        Some(
            DocReturn::new(PyTuple::new1([
                PyType::new_anycoretext(),
                PyClass::new_py(["api"], "StdTEXTOutput").into(),
            ]))
            .exc([exc]),
        ),
    );

    let fun_args = doc.fun_args();
    let ret_path = doc.ret_path();

    quote! {
        #[pyfunction]
        #doc
        #[allow(clippy::too_many_arguments)]
        pub fn fcs_read_std_text(#fun_args) -> #ret_path {
            let header = #header_conf { #(#header_recs),* };
            let raw = #raw_conf { header, #(#raw_recs),* };
            let standard = #std_conf { #(#std_recs),* };
            let offsets = #offsets_conf { #(#offsets_recs),* };
            let layout = #layout_conf { #(#layout_recs),* };
            let shared = #shared_conf { #(#shared_recs),* };
            let conf = #conf_path { raw, standard, offsets, layout, shared };
            let (core, data) = #fun_path(&path, &conf).py_termfail_resolve()?;
            Ok((core.into(), data.into()))
        }
    }
    .into()
}

#[proc_macro]
pub fn def_fcs_read_raw_dataset(input: TokenStream) -> TokenStream {
    let fun_path = parse_macro_input!(input as Path);

    let conf_path = config_path("ReadRawDatasetConfig");

    let path_arg = DocArg::new_path_param(true);
    let (header_conf, header_args, header_recs) = DocArgParam::new_header_config_params();
    let (raw_conf, raw_args, raw_recs) = DocArgParam::new_raw_config_params();
    let (layout_conf, layout_args, layout_recs) = DocArgParam::new_layout_config_params(None);
    let (offsets_conf, offsets_args, offsets_recs) = DocArgParam::new_offsets_config_params(None);
    let (data_conf, data_args, data_recs) = DocArgParam::new_reader_config_params();
    let (shared_conf, shared_args, shared_recs) = DocArgParam::new_shared_config_params();

    let exc = PyException::new("PyreflowException");

    let doc = DocString::new_fun(
        "Read raw dataset from FCS file.",
        [""; 0],
        once(path_arg)
            .chain(header_args)
            .chain(raw_args)
            .chain(offsets_args)
            .chain(layout_args)
            .chain(data_args)
            .chain(shared_args),
        Some(DocReturn::new(PyClass::new_py(["api"], "RawDatasetOutput")).exc([exc])),
    );

    let fun_args = doc.fun_args();
    let ret_path = doc.ret_path();

    quote! {
        #[pyfunction]
        #doc
        #[allow(clippy::too_many_arguments)]
        pub fn fcs_read_raw_dataset(#fun_args) -> #ret_path {
            let header = #header_conf { #(#header_recs),* };
            let raw = #raw_conf { header, #(#raw_recs),* };
            let layout = #layout_conf { #(#layout_recs),* };
            let offsets = #offsets_conf { #(#offsets_recs),* };
            let data = #data_conf { #(#data_recs),* };
            let shared = #shared_conf { #(#shared_recs),* };
            let conf = #conf_path { raw, layout, offsets, data, shared };
            Ok(#fun_path(&path, &conf).py_termfail_resolve()?.into())
        }
    }
    .into()
}

#[proc_macro]
pub fn def_fcs_read_std_dataset(input: TokenStream) -> TokenStream {
    let fun_path = parse_macro_input!(input as Path);

    let conf_path = config_path("ReadStdDatasetConfig");

    let path_arg = DocArg::new_path_param(true);
    let (header_conf, header_args, header_recs) = DocArgParam::new_header_config_params();
    let (raw_conf, raw_args, raw_recs) = DocArgParam::new_raw_config_params();
    let (std_conf, std_args, std_recs) = DocArgParam::new_std_config_params(None);
    let (offsets_conf, offsets_args, offsets_recs) = DocArgParam::new_offsets_config_params(None);
    let (layout_conf, layout_args, layout_recs) = DocArgParam::new_layout_config_params(None);
    let (data_conf, data_args, data_recs) = DocArgParam::new_reader_config_params();
    let (shared_conf, shared_args, shared_recs) = DocArgParam::new_shared_config_params();

    let exc = PyException::new("PyreflowException");

    let doc = DocString::new_fun(
        "Read standardized dataset from FCS file.",
        [""; 0],
        once(path_arg)
            .chain(header_args)
            .chain(raw_args)
            .chain(std_args)
            .chain(offsets_args)
            .chain(layout_args)
            .chain(data_args)
            .chain(shared_args),
        Some(
            DocReturn::new(PyTuple::new1([
                PyType::new_anycoredataset(),
                PyClass::new_py(["api"], "StdDatasetOutput").into(),
            ]))
            .exc([exc]),
        ),
    );

    let fun_args = doc.fun_args();
    let ret_path = doc.ret_path();

    quote! {
        #[pyfunction]
        #doc
        #[allow(clippy::too_many_arguments)]
        pub fn fcs_read_std_dataset(#fun_args) -> #ret_path {
            let header = #header_conf { #(#header_recs),* };
            let raw = #raw_conf { header, #(#raw_recs),* };
            let standard = #std_conf { #(#std_recs),* };
            let offsets = #offsets_conf { #(#offsets_recs),* };
            let layout = #layout_conf { #(#layout_recs),* };
            let data = #data_conf { #(#data_recs),* };
            let shared = #shared_conf { #(#shared_recs),* };
            let conf = #conf_path { raw, standard, offsets, layout, data, shared };
            let (core, data) = #fun_path(&path, &conf).py_termfail_resolve()?;
            Ok((core.into(), data.into()))
        }
    }
    .into()
}

#[proc_macro]
pub fn def_fcs_read_raw_dataset_with_keywords(input: TokenStream) -> TokenStream {
    let fun_path = parse_macro_input!(input as Path);

    let conf_path = config_path("ReadRawDatasetFromKeywordsConfig");

    let path_arg = DocArg::new_path_param(true);
    let version_arg = DocArg::new_version_param();
    let std_arg = DocArg::new_std_keywords_param();
    let data_arg = DocArg::new_data_seg_param(SegmentSrc::Header);
    let analysis_arg = DocArg::new_analysis_seg_param(SegmentSrc::Header, true);
    let other_arg = DocArg::new_other_segs_param(true);

    let (offsets_conf, offsets_args, offsets_recs) = DocArgParam::new_offsets_config_params(None);
    let (layout_conf, layout_args, layout_recs) = DocArgParam::new_layout_config_params(None);
    let (data_conf, data_args, data_recs) = DocArgParam::new_reader_config_params();
    let (shared_conf, shared_args, shared_recs) = DocArgParam::new_shared_config_params();

    let exc = PyException::new("PyreflowException");

    let doc = DocString::new_fun(
        "Read raw dataset from FCS file from keywords.",
        [""; 0],
        [
            path_arg,
            version_arg,
            std_arg,
            data_arg,
            analysis_arg,
            other_arg,
        ]
        .into_iter()
        .chain(offsets_args)
        .chain(layout_args)
        .chain(data_args)
        .chain(shared_args),
        Some(DocReturn::new(PyClass::new_py(["api"], "RawDatasetWithKwsOutput")).exc([exc])),
    );

    let fun_args = doc.fun_args();
    let ret_path = doc.ret_path();

    quote! {
        #[pyfunction]
        #doc
        #[allow(clippy::too_many_arguments)]
        pub fn fcs_read_raw_dataset_with_keywords(#fun_args) -> #ret_path {
            let offsets = #offsets_conf { #(#offsets_recs),* };
            let layout = #layout_conf { #(#layout_recs),* };
            let data = #data_conf { #(#data_recs),* };
            let shared = #shared_conf { #(#shared_recs),* };
            let conf = #conf_path { offsets, layout, data, shared };
            let ret = #fun_path(
                &path, version, &std, data_seg, analysis_seg, &other_segs[..], &conf
            ).py_termfail_resolve()?;
            Ok(ret.into())
        }
    }
    .into()
}

#[proc_macro]
pub fn def_fcs_read_std_dataset_with_keywords(input: TokenStream) -> TokenStream {
    let fun_path = parse_macro_input!(input as Path);

    let conf_path = config_path("ReadStdDatasetFromKeywordsConfig");

    let path_arg = DocArg::new_path_param(true);
    let version_arg = DocArg::new_version_param();
    let std_arg = DocArg::new_std_keywords_param();
    let nonstd_arg = DocArg::new_nonstd_keywords_param();
    let data_arg = DocArg::new_data_seg_param(SegmentSrc::Header);
    let analysis_arg = DocArg::new_analysis_seg_param(SegmentSrc::Header, true);
    let other_arg = DocArg::new_other_segs_param(true);

    let (std_conf, std_args, std_recs) = DocArgParam::new_std_config_params(None);
    let (offsets_conf, offsets_args, offsets_recs) = DocArgParam::new_offsets_config_params(None);
    let (layout_conf, layout_args, layout_recs) = DocArgParam::new_layout_config_params(None);
    let (data_conf, data_args, data_recs) = DocArgParam::new_reader_config_params();
    let (shared_conf, shared_args, shared_recs) = DocArgParam::new_shared_config_params();

    let exc = PyException::new("PyreflowException");

    let doc = DocString::new_fun(
        "Read standardized dataset from FCS file.",
        [""; 0],
        [
            path_arg,
            version_arg,
            std_arg,
            nonstd_arg,
            data_arg,
            analysis_arg,
            other_arg,
        ]
        .into_iter()
        .chain(std_args)
        .chain(offsets_args)
        .chain(layout_args)
        .chain(data_args)
        .chain(shared_args),
        Some(
            DocReturn::new(PyTuple::new1([
                PyType::new_anycoredataset(),
                PyClass::new_py(["api"], "StdDatasetWithKwsOutput").into(),
            ]))
            .exc([exc]),
        ),
    );

    let fun_args = doc.fun_args();
    let ret_path = doc.ret_path();

    quote! {
        #[pyfunction]
        #doc
        #[allow(clippy::too_many_arguments)]
        pub fn fcs_read_std_dataset_with_keywords(#fun_args) -> #ret_path {
            let kws = fireflow_core::validated::keys::ValidKeywords::new(std, nonstd);
            let standard = #std_conf { #(#std_recs),* };
            let offsets = #offsets_conf { #(#offsets_recs),* };
            let layout = #layout_conf { #(#layout_recs),* };
            let data = #data_conf { #(#data_recs),* };
            let shared = #shared_conf { #(#shared_recs),* };
            let conf = #conf_path { standard, offsets, layout, data, shared };
            let (core, data) = #fun_path(
                &path, version, kws, data_seg, analysis_seg, &other_segs[..], &conf
            ).py_termfail_resolve()?;
            Ok((core.into(), data.into()))
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

    let doc = DocString::new_class("The *HEADER* segment from an FCS dataset.", [""; 0], args);
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

    let doc = DocString::new_class("Standard and non-standard keywords.", [""; 0], args);

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

    let doc = DocString::new_class("The segments from *HEADER*", [""; 0], args);
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
pub fn impl_py_raw_text_output(input: TokenStream) -> TokenStream {
    let path = parse_macro_input!(input as Path);
    let name = path.segments.last().unwrap().ident.clone();

    let version = DocArgROIvar::new_version_ivar();

    let kws =
        DocArg::new_valid_keywords_param().into_ro(|_, _| quote!(self.0.keywords.clone().into()));

    let parse =
        DocArg::new_parse_output_param().into_ro(|_, _| quote!(self.0.parse.clone().into()));

    let args = [version, kws, parse];

    let doc = DocString::new_class("Parsed *HEADER* and *TEXT*.", [""; 0], args);

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
pub fn impl_py_raw_dataset_output(input: TokenStream) -> TokenStream {
    let path = parse_macro_input!(input as Path);
    let name = path.segments.last().unwrap().ident.clone();

    let text = DocArg::new_ivar_ro(
        "text",
        PyClass::new_py(["api"], "RawTEXTOutput"),
        "Parsed *TEXT* segment.",
        |_, _| quote!(self.0.text.clone().into()),
    );

    let dataset = DocArg::new_ivar_ro(
        "dataset",
        PyClass::new_py(["api"], "RawDatasetWithKwsOutput"),
        "Parsed *DATA*, *ANALYSIS*, and *OTHER* segments.",
        |_, _| quote!(self.0.dataset.clone().into()),
    );

    let args = [text, dataset];

    let doc = DocString::new_class("Parsed raw dataset from FCS file.", [""; 0], args);

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
pub fn impl_py_raw_dataset_with_kws_output(input: TokenStream) -> TokenStream {
    let path = parse_macro_input!(input as Path);
    let name = path.segments.last().unwrap().ident.clone();

    let data = DocArg::new_data_param(false).into_ro(|_, _| quote!(self.0.data.clone()));
    let analysis =
        DocArg::new_analysis_param(false).into_ro(|_, _| quote!(self.0.analysis.clone()));
    let others = DocArg::new_others_param(false).into_ro(|_, _| quote!(self.0.others.clone()));
    let dataset_segs =
        DocArg::new_dataset_segments_param().into_ro(|_, _| quote!(self.0.dataset_segments.into()));

    let doc = DocString::new_class(
        "Dataset from parsing raw *TEXT*.",
        [""; 0],
        [data, analysis, others, dataset_segs],
    );

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
        PyType::new_std_keywords(),
        "Keywords which start with *$* but are not part of the standard.",
        |_, _| quote!(self.0.pseudostandard.clone()),
    );

    let unused = DocArgROIvar::new_ivar_ro(
        "unused",
        PyType::new_std_keywords(),
        "Keywords which are part of the standard but were not used.",
        |_, _| quote!(self.0.unused.clone()),
    );

    let doc = DocString::new_class(
        "Extra keywords from *TEXT* standardization.",
        [""; 0],
        [pseudostandard, unused],
    );

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #path::new(pseudostandard, unused).into()
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

    let doc = DocString::new_class(
        "Segments used to parse *DATA* and *ANALYSIS*",
        [""; 0],
        [data, analysis],
    );

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
        PyClass::new_py(["api"], "RawTEXTParseData"),
        "Miscellaneous data when parsing *TEXT*.",
        |_, _| quote!(self.0.parse.clone().into()),
    );

    let doc = DocString::new_class(
        "Miscellaneous data when standardizing *TEXT*.",
        [""; 0],
        [tot, dataset_segs, extra, parse],
    );

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
        PyClass::new_py(["api"], "RawTEXTParseData"),
        "Miscellaneous data when parsing *TEXT*.",
        |_, _| quote!(self.0.parse.clone().into()),
    );

    let args = [dataset, parse];

    let doc = DocString::new_class(
        "Miscellaneous data when standardizing *TEXT*.",
        [""; 0],
        args,
    );

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

    let doc = DocString::new_class(
        "Miscellaneous data when standardizing *TEXT* from keywords.",
        [""; 0],
        [dataset_segs, extra],
    );

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
pub fn impl_py_raw_text_parse_data(input: TokenStream) -> TokenStream {
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
        PyOpt::new(RsInt::U32),
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
        PyList::new1(PyTuple::new1([PyStr::default(), PyStr::default()])),
        "Keywords with a non-ASCII but still valid UTF-8 key.",
        |_, _| quote!(self.0.non_ascii.clone()),
    );

    let byte_pairs = DocArgROIvar::new_ivar_ro(
        "byte_pairs",
        PyList::new1(PyTuple::new1([PyBytes::default(), PyBytes::default()])),
        "Keywords with invalid UTF-8 characters.",
        |_, _| quote!(self.0.byte_pairs.clone()),
    );

    let args = [segments, supp, nextdata, delim, non_ascii, byte_pairs];

    let doc = DocString::new_class(
        "Miscellaneous data obtained when parsing *TEXT*.",
        [""; 0],
        args,
    );
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

    let meas = DocArg::new_measurements_param(version).into();
    let layout = DocArg::new_layout_ivar(version).into();
    let data = DocArg::new_df_ivar().into();
    let analysis = DocArg::new_analysis_ivar().into();
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
        DocArg::new_kw_ivar("Cyt3_2", "cyt", PyType::new_non_empty_str, None, false)
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
        PyTuple::new1([
            PyType::from(RsInt::U32),
            PyList::new1(PyStr::default()).into(),
        ])
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
    let coretext_args: Vec<_> = meas_layout_args
        .clone()
        .into_iter()
        .chain(all_kws.clone())
        .collect();
    let coredataset_args: Vec<_> = meas_layout_args
        .into_iter()
        .chain([data])
        .chain(all_kws)
        .chain([analysis, others])
        .collect();

    let coretext_inner_args: Vec<_> = coretext_args.iter().map(IsDocArg::ident_into).collect();

    let coretext_doc = DocString::new_class(
        format!("Represents *TEXT* for an FCS {vs} file."),
        [""; 0],
        coretext_args,
    );

    let coredataset_doc = DocString::new_class(
        format!("Represents one dataset in an FCS {vs} file."),
        [""; 0],
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
    let doc = DocString::new_ivar(
        "Show the FCS version.",
        [""; 0],
        DocReturn::new(PyType::new_version()),
    );
    doc.into_impl_get(&t, "version", |_, _| quote!(self.0.fcs_version()))
        .into()
}

#[proc_macro]
pub fn impl_core_par(input: TokenStream) -> TokenStream {
    let t = parse_macro_input!(input as Ident);
    let _ = split_ident_version_pycore(&t);
    let doc = DocString::new_ivar(
        "The value for *$PAR*.",
        [""; 0],
        DocReturn::new(RsInt::Usize),
    );
    doc.into_impl_get(&t, "par", |_, _| quote!(self.0.par().0))
        .into()
}

#[proc_macro]
pub fn impl_core_all_meas_nonstandard_keywords(input: TokenStream) -> TokenStream {
    let t = parse_macro_input!(input as Ident);
    let _ = split_ident_version_pycore(&t);

    let doc = DocString::new_ivar(
        "The non-standard keywords for each measurement.",
        [""; 0],
        DocReturn::new(PyList::new1(PyType::new_nonstd_keywords())),
    );

    doc.into_impl_get_set(
        &t,
        "all_meas_nonstandard_keywords",
        true,
        |_, _| {
            quote!(self
                .0
                .get_meas_nonstandard()
                .into_iter()
                .map(|x| x.clone())
                .collect())
        },
        |n, _| quote!(Ok(self.0.set_meas_nonstandard(#n)?)),
    )
    .into()
}

#[proc_macro]
pub fn impl_core_standard_keywords(input: TokenStream) -> TokenStream {
    let ident = parse_macro_input!(input as Ident);
    let _ = split_ident_version_pycore(&ident);

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
            format!("Do not include {a} {b} keywords."),
        )
    };

    let doc = DocString::new_method(
        "Return standard keywords as string pairs.",
        [
            "Each key will be prefixed with *$*.",
            "This will not include *$TOT*, *$NEXTDATA* or any of the \
             offset keywords since these are not encoded in this class.",
        ],
        [(true, true), (false, true), (true, false), (false, false)].map(|(x, y)| make_param(x, y)),
        Some(DocReturn::new(PyType::new_keywords()).desc("A list of standard keywords.")),
    );

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
    let doc = DocString::new_method(
        "Set the threshold for *$TR*.",
        [""; 0],
        [p],
        Some(DocReturn::new(PyBool::default()).desc("``True`` if trigger is set and was updated.")),
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

    let write_2_0_warning = (version == Version::FCS2_0)
        .then_some("Will raise exception if file cannot fit within 99,999,999 bytes.");

    let doc = DocString::new_method(
        "Write data to path.",
        once("Resulting FCS file will include *HEADER* and *TEXT*.").chain(write_2_0_warning),
        [
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

    let write_2_0_warning = (version == Version::FCS2_0)
        .then_some("Will raise exception if file cannot fit within 99,999,999 bytes.");

    let doc = DocString::new_method(
        "Write data as an FCS file.",
        once(
            "The resulting file will include *HEADER*, *TEXT*, *DATA*, \
            *ANALYSIS*, and *OTHER* as they present from this class.",
        )
        .chain(write_2_0_warning),
        [
            DocArg::new_path_param(false),
            DocArg::new_textdelim_param(),
            DocArg::new_big_other_param(),
            DocArg::new_bool_param(
                "skip_conversion_check",
                "Skip check to ensure that types of the dataframe match the \
                 columns (*$PnB*, *$DATATYPE*, etc). If this is ``False``, \
                 perform this check before writing, and raise exception on \
                 failure. If ``True``, raise warnings as file is being \
                 written. Skipping this is faster since the data needs to be \
                 traversed twice to perform the conversion check, but may \
                 result in loss of precision and/or truncation.",
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

    let go = |k: &str, kw: &str, name: &str| {
        let p = keyword_path(kw);
        let pt = PyOpt::new(PyInt::new_u32().rstype(p));
        let inner = pt.as_rust_type();
        let doc = DocString::new_ivar(
            format!("The value of *$P{k}n* for all measurements."),
            [""; 0],
            DocReturn::new(PyList::new1(pt)),
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
        ["Strings are unique and cannot contain commas."],
        DocReturn::new(PyList::new1(PyStr::new_shortname())),
    );

    doc.into_impl_get_set(
        &i,
        "all_shortnames",
        true,
        |_, _| quote!(self.0.all_shortnames()),
        |n, _| quote!(Ok(self.0.set_all_shortnames(#n).void()?)),
    )
    .into()
}

#[proc_macro]
pub fn impl_core_all_shortnames_maybe_attr(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let _ = split_ident_version_pycore(&i).1;

    let doc = DocString::new_ivar(
        "The possibly-empty values of *$PnN* for all measurements.",
        ["*$PnN* is optional for this FCS version so values may be ``None``."],
        DocReturn::new(PyList::new1(PyOpt::new(PyStr::new_shortname()))),
    );

    doc.into_impl_get_set(
        &i,
        "all_shortnames_maybe",
        true,
        |_, _| {
            quote! {
                self.0
                    .shortnames_maybe()
                    .into_iter()
                    .map(|x| x.cloned())
                    .collect()
            }
        },
        |n, _| quote!(Ok(self.0.set_measurement_shortnames_maybe(#n).void()?)),
    )
    .into()
}

#[proc_macro]
pub fn impl_core_get_set_timestep(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let _ = split_ident_version_pycore(&i).1;

    let t = PyOpt::new(PyType::new_timestep());
    let get_doc = DocString::new_ivar(
        "The value of *$TIMESTEP*",
        [""; 0],
        DocReturn::new(t.clone()),
    );

    let getq = get_doc.into_impl_get(&i, "timestep", |_, _| quote!(self.0.timestep().copied()));

    let param = DocArg::new_param(
        "timestep",
        PyType::new_timestep(),
        "The timestep to set. Must be greater than zero.",
    );
    let set_doc = DocString::new_method(
        "Set the *$TIMESTEP* if time measurement is present.",
        [""; 0],
        [param],
        Some(DocReturn::new(t.map_exc(|_| ())).desc("Previous *$TIMESTEP* if present.")),
    );

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
            PyType::new_timestep(),
            "The value of *$TIMESTEP* to use.",
        ));
        let allow_loss = DocArg::new_bool_param(
            "allow_loss",
            "If ``True`` remove any optical-specific metadata (detectors, \
             lasers, etc) without raising an exception.",
        );
        DocString::new_method(
            format!("Set the temporal measurement to a given {i}."),
            [""; 0],
            once(p).chain(timestep).chain([allow_loss]),
            Some(DocReturn::new(PyBool::default()).desc(format!(
                "``True`` if temporal measurement was set, which will \
                 happen for all cases except when the time measurement is \
                 already set to ``{i}``."
            ))),
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
                self.0.set_temporal(&name, (), allow_loss).py_termfail_resolve()
            }

            #index_doc
            fn set_temporal_at(&mut self, #index_fun_args) -> PyResult<bool> {
                self.0.set_temporal_at(index, (), allow_loss).py_termfail_resolve()
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
                    .py_termfail_resolve()
            }

            #index_doc
            fn set_temporal_at(&mut self, #index_fun_args) -> PyResult<bool> {
                self.0
                    .set_temporal_at(index, timestep, allow_loss)
                    .py_termfail_resolve()
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
            .then_some(DocArg::new_bool_param(
                "allow_loss",
                "If ``True`` and current time measurement has data which cannot \
                 be converted to optical, force the conversion anyways. \
                 Otherwise raise an exception.",
            ))
            .into_iter();
        let (rt, rd) = if has_timestep {
            (
                PyOpt::new(PyType::new_timestep()).into(),
                "Value of *$TIMESTEP* if time measurement was present.",
            )
        } else {
            (
                PyType::from(PyBool::default()),
                "``True`` if temporal measurement was present and converted, \
                 ``False`` if there was not a temporal measurement.",
            )
        };
        DocString::new_method(
            s,
            [""; 0],
            p,
            Some(DocReturn::new(rt.map_exc(|_| ())).desc(rd)),
        )
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
                self.0.unset_temporal_lossy(allow_loss).py_termfail_resolve()
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

    let doc = DocString::new_method(
        "Rename temporal measurement if present.",
        [""; 0],
        [DocArg::new_name_param("New name to assign.")],
        Some(DocReturn::new(PyOpt::new(PyStr::new_shortname())).desc("Previous name if present.")),
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

    if version == Version::FCS2_0 {
        let s0 = "Will be ``()`` for linear scaling (``0,0`` in FCS encoding), \
                   a 2-tuple for log scaling, or ``None`` if missing.";
        let s1 = "The temporal measurement must always be ``()``. Setting it \
                  to another value will raise an exception.";
        let doc = DocString::new_ivar(
            "The value for *$PnE* for all measurements.",
            [s0, s1],
            DocReturn::new(PyList::new1(PyOpt::new(PyType::new_scale(false)))),
        );

        doc.into_impl_get_set(
            &i,
            "all_scales",
            true,
            |_, _| quote!(self.0.scales().collect()),
            |n, _| quote!(self.0.set_scales(#n).py_termfail_resolve_nowarn()),
        )
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
        let doc = DocString::new_ivar(
            sum,
            [s0, s1, s2, s3],
            DocReturn::new(PyList::new1(PyType::new_transform())),
        );

        doc.into_impl_get_set(
            &i,
            "all_scale_transforms",
            true,
            |_, _| quote!(self.0.transforms().collect()),
            |n, _| quote!(self.0.set_transforms(#n).py_termfail_resolve_nowarn()),
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
        [""; 0],
        DocReturn::new(PyList::new1(PyType::new_measurement(version))),
    );

    doc.into_impl_get(&i, "measurements", |_, _| {
        quote! {
            // This might seem inefficient since we are cloning
            // everything, but if we want to map a python lambda
            // function over the measurements we would need to to do
            // this anyways, so simply returnig a copied list doesn't
            // lose anything and keeps this API simpler.
            self.0
                .measurements()
                .iter()
                .map(|e| e.bimap(|t| t.value.clone(), |o| o.value.clone()))
                .map(|v| v.inner_into())
                .collect()
        }
    })
    .into()
}

#[proc_macro]
pub fn impl_core_get_temporal(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let version = split_ident_version_pycore(&i).1;

    let doc = DocString::new_ivar(
        "The temporal measurement if it exists.",
        [""; 0],
        DocReturn::new(PyOpt::new(PyTuple::new1([
            PyInt::new_meas_index().into(),
            PyStr::new_shortname().into(),
            PyType::new_temporal(version),
        ])))
        .desc("Index, name, and measurement or ``None``."),
    );

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

    let doc = DocString::new_method(
        "Return measurement at index.",
        ["Raise exception if ``index`` not found."],
        [DocArg::new_index_param("Index to retrieve.")],
        Some(DocReturn::new(
            PyType::new_measurement(version).map_exc(|_| ()),
        )),
    );

    let fun_args = doc.fun_args();
    let ret = doc.ret_path();

    quote! {
        #[pymethods]
        impl #i {
            #doc
            fn measurement_at(&self, #fun_args) -> PyResult<#ret> {
                let m = self.0.measurements().get(index)?;
                Ok(m.bimap(|x| x.1.clone(), |x| x.1.clone()).inner_into())
            }
        }
    }
    .into()
}

#[proc_macro]
pub fn impl_core_get_named_measurement(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    let version = split_ident_version_pycore(&i).1;

    let doc = DocString::new_method(
        "Return measurement with name.",
        ["Raise exception if ``name`` not found."],
        [DocArg::new_name_param("Name to retrieve.")],
        Some(DocReturn::new(
            PyType::new_measurement(version).map_exc(|_| ()),
        )),
    );

    let fun_args = doc.fun_args();
    let ret = doc.ret_path();

    quote! {
        #[pymethods]
        impl #i {
            #doc
            fn measurement_named(&self, #fun_args) -> PyResult<#ret> {
                let (_, m) = self.0.measurements().get_name(&name)?;
                Ok(m.bimap(|x| x.clone(), |x| x.clone()).inner_into())
            }
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
        "Set all measurements at once.",
        ps,
        vec![
            DocArg::new_set_meas_param(version),
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
        let (meas_type, what) = if is_optical {
            (PyType::new_optical(version), "optical")
        } else {
            (PyType::new_temporal(version), "temporal")
        };
        let param_meas = DocArg::new_param("meas", meas_type, "The measurement to push.");
        let col_param = hasdata.then_some(DocArg::new_col_param());
        let ps = [
            DocArg::new_name_param("Name of new measurement."),
            param_meas,
        ]
        .into_iter()
        .chain(col_param)
        .chain([DocArg::new_range_param(), DocArg::new_notrunc_param()]);
        let summary = format!("Push {what} measurement to end of measurement vector.");
        DocString::new_method(summary, [""; 0], ps, None)
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

    let family_path = versioned_family_path(version);
    let element_path = quote!(fireflow_core::text::named_vec::Element);

    let by_name_doc = DocString::new_method(
        "Remove a measurement with a given name.",
        ["Raise exception if ``name`` not found."],
        [DocArg::new_name_param("Name to remove.")],
        Some(
            DocReturn::new(
                PyTuple::new1([
                    PyInt::new_meas_index().into(),
                    PyType::new_measurement(version),
                ])
                .map_exc(|_| ()),
            )
            .desc("Index and measurement object."),
        ),
    );

    let by_index_doc = DocString::new_method(
        "Remove a measurement with a given index.",
        ["Raise exception if ``index`` not found."],
        [DocArg::new_index_param("Index to remove.")],
        Some(
            DocReturn::new(
                PyTuple::new1([
                    PyType::new_versioned_shortname(version),
                    PyType::new_measurement(version),
                ])
                .map_exc(|_| ()),
            )
            .desc("Name and measurement object."),
        ),
    );

    let name_arg = by_name_doc.fun_args();
    let index_arg = by_index_doc.fun_args();

    let name_ident = by_name_doc.idents();
    let index_ident = by_index_doc.idents();

    let name_ret = by_name_doc.ret_path();
    let index_ret = by_index_doc.ret_path();

    quote! {
        #[pymethods]
        impl #i {
            #by_name_doc
            fn remove_measurement_by_name(
                &mut self,
                #name_arg
            ) -> PyResult<#name_ret> {
                Ok(self
                   .0
                   .remove_measurement_by_name(&#name_ident)
                   .map(|(i, x)| (i, x.inner_into()))?)
            }

            #by_index_doc
            fn remove_measurement_by_index(
                &mut self,
                #index_arg
            ) -> PyResult<#index_ret> {
                let r = self.0.remove_measurement_by_index(#index_ident)?;
                let (n, v) = #element_path::unzip::<#family_path>(r);
                Ok((n, v.inner_into()))
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
            (PyType::new_optical(version), "optical")
        } else {
            (PyType::new_temporal(version), "temporal")
        };
        let param_meas = DocArg::new_param("meas", meas_type, "The measurement to insert.");
        let col_param = hasdata.then_some(DocArg::new_col_param());
        let summary = format!("Insert {what} measurement at position in measurement vector.");
        let ps = [
            DocArg::new_index_param("Position at which to insert new measurement."),
            DocArg::new_name_param("Name of new measurement."),
            param_meas,
        ]
        .into_iter()
        .chain(col_param)
        .chain([DocArg::new_range_param(), DocArg::new_notrunc_param()]);
        DocString::new_method(summary, [""; 0], ps, None)
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
    let ident: Ident = syn::parse(input).unwrap();
    let version = split_ident_version_pycore(&ident).1;

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
        let meas_desc = format!("Optical measurement to replace measurement at ``{i}``.");
        let sub = format!("Raise exception if ``{i}`` does not exist.");
        let ret = PyOpt::wrap_if(PyType::new_measurement(version), !is_index);
        DocString::new_method(
            format!("Replace {m} with given optical measurement."),
            [sub],
            [
                i_param,
                DocArg::new_param("meas", PyType::new_optical(version), meas_desc),
            ],
            Some(DocReturn::new(ret.map_exc(|_| ())).desc("Replaced measurement object.")),
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
            fn replace_optical_at(&mut self, #index_fun_args) -> PyResult<#index_ret> {
                Ok(self.0.replace_optical_at(index, meas.into())?.inner_into())
            }

            #replace_named_doc
            fn replace_optical_named(&mut self, #name_fun_args) -> #named_ret {
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
    let ident: Ident = syn::parse(input).unwrap();
    let version = split_ident_version_pycore(&ident).1;

    let allow_loss_param = DocArg::new_bool_param(
        "allow_loss",
        "If ``True``, do not raise exception if existing temporal measurement \
         cannot be converted to optical measurement.",
    );

    // the temporal replacement functions for 3.2 are different because they
    // can fail if $PnTYPE is set
    let (replace_tmp_at_body, replace_tmp_named_body, allow_loss) = if version == Version::FCS3_2 {
        let go = |fun, x| quote!(self.0.#fun(#x, meas.into(), allow_loss).py_termfail_resolve()?);
        (
            go(quote! {replace_temporal_at_lossy}, quote! {index}),
            go(quote! {replace_temporal_named_lossy}, quote! {&name}),
            Some(allow_loss_param),
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
            DocArg::new_param("meas", PyType::new_temporal(version), meas_desc),
        ];
        let ret = PyOpt::wrap_if(PyType::new_measurement(version), !is_index);
        DocString::new_method(
            format!("Replace {m} with given temporal measurement."),
            [sub],
            args.into_iter().chain(allow_loss.clone()),
            Some(DocReturn::new(ret.map_exc(|_| ())).desc("Replaced measurement object.")),
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
            fn replace_temporal_at(
                &mut self,
                #index_fun_args
            ) -> PyResult<#index_ret> {
                let ret = #replace_tmp_at_body;
                Ok(ret.inner_into())
            }

            #replace_named_doc
            fn replace_temporal_named(
                &mut self,
                #name_fun_args
            ) -> PyResult<#named_ret> {
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

    let core_conf = config_path("NewCoreTEXTConfig");

    let (std_conf, std_args, std_recs) = DocArgParam::new_std_config_params(Some(version));
    let (layout_conf, layout_args, layout_recs) =
        DocArgParam::new_layout_config_params(Some(version));
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
        PyType::new_std_keywords(),
        format!("Standard keywords. {no_kws}"),
    );

    let nonstd_param = DocArg::new_param(
        "nonstd",
        PyType::new_nonstd_keywords(),
        "Non-Standard keywords.",
    );

    let doc = DocString::new_fun(
        "Make new instance from keywords.",
        [""; 0],
        [std_param, nonstd_param]
            .into_iter()
            .chain(std_args)
            .chain(layout_args)
            .chain(shared_args),
        Some(DocReturn::new(PyTuple::new1([
            PyType::new_coretext(version),
            PyClass::new_py(["api"], "ExtraStdKeywords").into(),
        ]))),
    );

    let fun_args = doc.fun_args();
    let ret_path = doc.ret_path();

    quote! {
        #[pymethods]
        impl #pyname {
            #[classmethod]
            #[allow(clippy::too_many_arguments)]
            #doc
            fn from_kws(_: &Bound<'_, pyo3::types::PyType>, #fun_args) -> PyResult<#ret_path> {
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
                let (core, uncore) = #path::new_from_keywords(kws, &conf).py_termfail_resolve()?;
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

    let core_conf = config_path("ReadStdDatasetFromKeywordsConfig");

    let (std_conf, std_args, std_recs) = DocArgParam::new_std_config_params(Some(version));
    let (layout_conf, layout_args, layout_recs) =
        DocArgParam::new_layout_config_params(Some(version));
    let (offsets_conf, offsets_args, offsets_recs) =
        DocArgParam::new_offsets_config_params(Some(version));
    let (data_conf, data_args, data_recs) = DocArgParam::new_reader_config_params();
    let (shared_conf, shared_args, shared_recs) = DocArgParam::new_shared_config_params();

    let config_args: Vec<_> = std_args
        .into_iter()
        .chain(layout_args)
        .chain(offsets_args)
        .chain(data_args)
        .chain(shared_args)
        .collect();

    let path_param = DocArg::new_path_param(true);

    let std_param = DocArg::new_param("std", PyType::new_std_keywords(), "Standard keywords.");

    let nonstd_param = DocArg::new_param(
        "nonstd",
        PyType::new_nonstd_keywords(),
        "Non-Standard keywords.",
    );

    let data_seg_param = DocArg::new_data_seg_param(SegmentSrc::Header);
    let analysis_seg_param = DocArg::new_analysis_seg_param(SegmentSrc::Header, true);
    let other_segs_param = DocArg::new_other_segs_param(true);

    let all_args = [
        path_param,
        std_param,
        nonstd_param,
        data_seg_param,
        analysis_seg_param,
        other_segs_param,
    ]
    .into_iter()
    .chain(config_args);

    let doc = DocString::new_fun(
        "Make new instance from keywords.",
        [""; 0],
        all_args,
        Some(DocReturn::new(PyTuple::new1([
            PyType::new_coredataset(version),
            PyClass::new_py(["api"], "StdDatasetWithKwsOutput").into(),
        ]))),
    );

    let fun_args = doc.fun_args();
    let ret_path = doc.ret_path();

    quote! {
        #[pymethods]
        impl #pyname {
            #[classmethod]
            #[allow(clippy::too_many_arguments)]
            #doc
            fn from_kws(_: &Bound<'_, pyo3::types::PyType>, #fun_args) -> PyResult<#ret_path> {
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
                let offsets = #offsets_conf {
                    #(#offsets_recs,)*
                    ..#offsets_conf::default()
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
                let conf = #core_conf { standard, layout, offsets, data, shared };
                let (core, uncore) = #path::new_from_keywords(
                    path, kws, data_seg, analysis_seg, &other_segs[..], &conf
                ).py_termfail_resolve()?;
                Ok((core.into(), uncore.into()))
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
    let p1 = "Will raise exception if other keywords (such as *$TR*) reference \
              a measurement.";

    let doc = DocString::new_method(s, [p0, p1], [], None);

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
        "Remove all measurements and their data.",
        ["Raise exception if any keywords (such as *$TR*) reference a measurement."],
        [],
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
        "skip_conv_check",
        "If ``True``, silently truncate data; otherwise return warnings when \
         truncation is performed.",
    );

    let doc = DocString::new_method(
        "Coerce all values in DATA to fit within types specified in layout.",
        ["This will always create a new copy of DATA in-place."],
        [p],
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

    let param_type_set_layout = DocArg::new_param("layout", layout.pytype, "The new layout.");

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
        "Set all measurements at once.",
        ps,
        [
            DocArg::new_set_meas_param(version),
            param_type_set_layout,
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

    let param_type_set_df =
        DocArg::new_param("data", PyType::new_dataframe(false), "The new data.");

    let doc = DocString::new_method(
        "Set measurements and data at once.",
        ["Length of ``measurements`` must match number of columns in ``data``."],
        [
            DocArg::new_set_meas_param(version),
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

    let data = DocArg::new_data_param(false);
    let analysis = DocArg::new_analysis_param(true);
    let others = DocArg::new_others_param(true);

    let doc = DocString::new_method(
        "Convert to a dataset object.",
        ["This will fully represent an FCS file, as opposed to just \
          representing *HEADER* and *TEXT*."],
        [data, analysis, others],
        Some(DocReturn::new(PyType::new_coredataset(version))),
    );

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

// TODO it seems like there is no compile error if the types don't match between
// the python and rs interfaces. This seems sketchy
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
        DocArg::new_meas_kw_opt_ivar("Feature", "feature", "FEATURE", |_| PyType::new_feature());

    let detector_name = DocArg::new_meas_kw_str("DetectorName", "detector_name", "DET");

    let tag = DocArg::new_meas_kw_str("Tag", "tag", "TAG");

    let measurement_type = DocArg::new_meas_kw_str("OpticalType", "measurement_type", "TYPE");

    let has_type = DocArg::new_meas_kw_ivar1("TemporalType", "has_type", "TYPE", |p| {
        PyBool::default().rstype(p)
    });

    let has_scale = DocArg::new_meas_kw_ivar1("TemporalScale", "has_scale", "E", |p| {
        PyBool::default().rstype(p)
    });

    let timestep = DocArg::new_ivar_rw(
        "timestep",
        PyType::new_timestep(),
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

    let inner_args: Vec<_> = all_args.iter().map(IsDocArg::ident_into).collect();

    let doc = DocString::new_class(
        format!("FCS {version_short} *$Pn\\** keywords for {lower_basename} measurement."),
        [""; 0],
        all_args,
    );

    let new_method = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #fun(#(#inner_args),*).into()
            }
        }
    };

    doc.into_impl_class(name, &path, new_method).1.into()
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

    let doc = DocString::new_ivar(
        doc_summary,
        [doc_middle],
        DocReturn::new(PyList::new1(full_pytype)),
    );

    doc.into_impl_get_set(
        &i,
        "all_measurement_types",
        true,
        |_, _| {
            quote! {
                self.0
                    .get_temporal_optical::<#inner_tmp_rstype, #inner_opt_rstype>()
                    .map(|e| e.bimap(|x| x.clone(), |y| y.clone()))
                    .collect()
            }
        },
        |n, _| quote!(self.0.set_temporal_optical2(#n).py_termfail_resolve_nowarn()),
    )
    .into()
}

#[proc_macro]
pub fn impl_core_all_pnfeature(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_optical_attr(&i, "Feature", "features", "FEATURE", |_| {
        PyType::new_feature()
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

    let doc = DocString::new_ivar(
        doc_summary,
        doc_middle,
        DocReturn::new(PyList::new1(full_pytype)),
    );

    let get_optical_body = if is_optional {
        quote! {
            self.0
                .optical_opt()
                .map(|e| e.0.map_non_center(|x| x.cloned()).into())
                .collect()
        }
    } else {
        quote! {
            self.0
                .optical::<#inner_rstype>()
                .map(|e| e.0.map_non_center(|x| x.clone()).into())
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
                quote!(self.0.set_optical(#n).py_termfail_resolve_nowarn())
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
    let outputs: Vec<_> = ALL_VERSIONS
        .iter()
        .filter(|&&v| v != version)
        .map(|&v| {
            let vsu = v.short_underscore();
            let vs = v.short();
            let fn_name = format_ident!("to_version_{vsu}");
            let target_type = if is_dataset {
                PyType::new_coredataset(v)
            } else {
                PyType::new_coretext(v)
            };
            let target_pytype = target_type.as_rust_type();
            let param = DocArg::new_bool_param("allow_loss", param_desc);
            let doc = DocString::new_method(
                format!("Convert to FCS {vs}."),
                [sub],
                [param],
                Some(
                    DocReturn::new(target_type)
                        .desc(format!("A new class conforming to FCS {vs}.")),
                ),
            );
            quote! {
                #doc
                fn #fn_name(&self, allow_loss: bool) -> PyResult<#target_pytype> {
                    self.0.clone().try_convert(allow_loss).py_termfail_resolve().map(Into::into)
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
        PyType::new_scale(true),
        "The *$GmE* keyword. ``()`` means linear scaling and 2-tuple \
         specifies decades and offset for log scaling.",
        false,
        |n, _| quote!(self.0.#n.as_ref().cloned()),
        |n, _| quote!(self.0.#n = #n.into()),
    );

    let make_arg_str = |kw_name: &str, kw_sym: &str, t: &str| {
        let kw_path = keyword_path(t);
        DocArg::new_ivar_rw_def(
            kw_name,
            PyStr::default().rstype(kw_path),
            format!("The *$Gm{kw_sym}* keyword."),
            DocDefault::Auto,
            false,
            |n, _| quote!(self.0.#n.clone()),
            |n, _| quote!(self.0.#n = #n),
        )
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

    let all_args = [
        scale,
        filter,
        shortname,
        percent_emitted,
        range,
        longname,
        detector_type,
        detector_voltage,
    ]
    .map(AnyDocArg::from);

    let inner_args: Vec<_> = all_args.iter().map(IsDocArg::ident_into).collect();

    let summary = "The *$Gm\\** keywords for one gated measurement.";
    let doc = DocString::new_class(summary, [""; 0], all_args);

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #path::new(#(#inner_args),*).into()
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
        PyList::new1(RsInt::U64),
        "The range for each measurement. Equivalent to *$PnR*. The value of \
         *$PnB* will be derived from these and will be equivalent to the number \
         of digits for each value.",
        |_, _| quote!(self.0.columns().iter().map(|c| c.value()).collect()),
    );

    let doc = DocString::new_class("A fixed-width ASCII layout.", [""; 0], [chars_param]);

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #bare_path::new_ascii_u64(ranges).into()
            }
        }
    };

    let (pyname, class) = doc.into_impl_class(name, &path, new);

    let char_widths_doc = DocString::new_ivar(
        "The width of each measurement.",
        [
            "Equivalent to *$PnB*, which is the number of chars/digits used \
             to encode data for a given measurement.",
        ],
        DocReturn::new(PyList::new1(RsInt::U64)),
    );

    let char_widths = char_widths_doc.into_impl_get(&pyname, "char_widths", |_, _| {
        quote! {
            self.0
                .widths()
                .into_iter()
                .map(|x| u64::from(u8::from(x)))
                .collect()
        }
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
        PyList::new1(RsInt::U64),
        "The range for each measurement. Equivalent to the *$PnR* keyword. \
         This is not used internally.",
        |_, _| quote!(self.0.as_ref().to_vec()),
    );

    let doc = DocString::new_class("A delimited ASCII layout.", [""; 0], [ranges_param]);

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
            PyType::new_float_range(nbytes),
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
            PyType::new_bitmask(nbytes),
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
    let sizedbyteord_path: Path = parse_quote!(fireflow_core::text::byteord::SizedByteOrd);

    let full_layout_path: Path = parse_quote!(#ordered_layout_path<#range_path, #known_tot_path>);

    let layout_name = format!("Ordered{base}{nbits:02}Layout");

    let summary = format!("{nbits}-bit ordered {what} layout.");

    let range_param =
        DocArg::new_ivar_ro("ranges", PyList::new1(range_pytype), range_desc, |_, _| {
            quote!(self.0.columns().iter().map(|c| c.clone()).collect())
        });

    let byteord_param = DocArg::new_ivar_ro_def(
        "byteord",
        PyType::new_byteord(nbytes),
        "The byte order to use when encoding values.",
        DocDefault::Auto,
        |_, _| quote!(*self.0.as_ref()),
    );

    let is_big_param = make_endian_ord_param(2);

    let make_doc = |args| DocString::new_class(summary, [""; 0], args);

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

    let nomeasdt_path = quote!(fireflow_core::data::NoMeasDatatype);
    let endian_layout_path = quote!(fireflow_core::data::EndianLayout);
    let fixed_layout_path = quote!(fireflow_core::data::FixedLayout);

    let full_layout_path = parse_quote!(#endian_layout_path<#range_path, #nomeasdt_path>);

    let layout_name = format!("EndianF{nbits:02}Layout");

    let range_param = DocArg::new_ivar_ro(
        "ranges",
        PyList::new1(PyType::new_float_range(nbytes)),
        "The range for each measurement. Corresponds to *$PnR*. This is not \
         used internally.",
        |_, _| quote!(self.0.columns().iter().map(|c| c.clone()).collect()),
    );

    let is_big_param = make_endian_param(4);

    let doc = DocString::new_class(
        format!("{nbits}-bit endian float layout"),
        [""; 0],
        [range_param, is_big_param],
    );

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
    let nomeasdt = quote!(fireflow_core::data::NoMeasDatatype);
    let endian_layout = quote!(fireflow_core::data::EndianLayout);
    let layout_path = parse_quote!(#endian_layout<#bitmask, #nomeasdt>);

    let ranges_param: DocArgROIvar = DocArg::new_ivar_ro(
        "ranges",
        PyList::new1(PyInt::new_int(RsInt::U64)),
        "The range of each measurement. Corresponds to the *$PnR* \
         keyword less one. The number of bytes used to encode each \
         measurement (*$PnB*) will be the minimum required to express this \
         value. For instance, a value of ``1023`` will set *$PnB* to ``16``, \
         will set *$PnR* to ``1024``, and encode values for this measurement as \
         16-bit integers. The values of a measurement will be less than or \
         equal to this value.",
        |_, _| quote!(self.0.columns().iter().map(|c| u64::from(*c)).collect()),
    );

    let is_big_param = make_endian_param(4);

    let doc = DocString::new_class(
        "A mixed-width integer layout.",
        [""; 0],
        [ranges_param, is_big_param],
    );

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                let rs = ranges.into_iter().map(#bitmask::from).collect();
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

    // TODO exception if invalid range types
    let types_param: DocArgROIvar = DocArg::new_ivar_ro(
        "typed_ranges",
        PyList::new1(PyUnion::new2(
            PyTuple::new1([PyLiteral::new1(["A", "I"]).into(), PyType::from(RsInt::U64)]),
            PyTuple::new1([
                PyLiteral::new1(["F", "D"]).into(),
                PyType::from(PyDecimal::default()),
            ]),
            parse_quote!(#null),
        )),
        "The type and range for each measurement corresponding to *$DATATYPE* \
         and/or *$PnDATATYPE* and *$PnR* respectively. These are given \
         as 2-tuples like ``(<type>, <range>)`` where ``type`` is one of \
         ``\"A\"``, ``\"I\"``, ``\"F\"``, or ``\"D\"`` corresponding to Ascii, \
         Integer, Float, or Double datatypes respectively.",
        |_, _| quote!(self.0.columns().iter().map(|c| c.clone()).collect()),
    );

    let is_big_param = make_endian_param(4);

    let doc = DocString::new_class("A mixed-type layout.", [""; 0], [types_param, is_big_param]);

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #fixed::new(typed_ranges, endian).into()
            }
        }
    };

    doc.into_impl_class(name, &layout_path, new).1.into()
}

// TODO not DRY
fn make_endian_ord_param(n: usize) -> DocArgROIvar {
    let xs = (1..=n).join(",");
    let ys = (1..=n).rev().join(",");
    let sizedbyteord_path = quote!(fireflow_core::text::byteord::SizedByteOrd);
    DocArg::new_ivar_ro_def(
        "endian",
        PyType::new_endian(),
        format!(
            "If ``\"big\"`` use big endian (``{ys}``) for encoding values; \
             if ``\"little\"`` use little endian (``{xs}``)."
        ),
        DocDefault::Auto,
        |_, _| {
            quote! {
                let m: #sizedbyteord_path<2> = *self.0.as_ref();
                m.endian()
            }
        },
    )
}

fn make_endian_param(n: usize) -> DocArgROIvar {
    let xs = (1..=n).join(",");
    let ys = (1..=n).rev().join(",");
    DocArg::new_ivar_ro_def(
        "endian",
        PyType::new_endian(),
        format!(
            "If ``\"big\"`` use big endian (``{ys}``) for encoding values; \
             if ``\"little\"`` use little endian (``{xs}``)."
        ),
        DocDefault::Auto,
        |_, _| quote!(*self.0.as_ref()),
    )
}

fn make_byte_width(pyname: &Ident, nbytes: usize) -> TokenStream2 {
    let s0 = format!("Will always return ``{nbytes}``.");
    let s1 = "This corresponds to the value of *$PnB* divided by 8, which are \
              all equal for this layout."
        .into();
    let doc = DocString::new_ivar(
        "The width of each measurement in bytes.",
        [s0, s1],
        DocReturn::new(RsInt::Usize),
    );

    doc.into_impl_get(pyname, "byte_width", |_, _| quote!(#nbytes))
}

#[proc_macro]
pub fn impl_layout_byte_widths(input: TokenStream) -> TokenStream {
    let t = parse_macro_input!(input as Ident);

    let doc = DocString::new_ivar(
        "The width of each measurement in bytes.",
        [
            "This corresponds to the value of *$PnB* for each measurement \
             divided by 8. Values for each measurement may be different.",
        ],
        DocReturn::new(PyList::new1(RsInt::U32)),
    );

    doc.into_impl_get(&t, "byte_widths", |_, _| {
        quote! {
            self.0
                .widths()
                .into_iter()
                .map(|x| u32::from(u8::from(x)))
                .collect()
        }
    })
    .into()
}

fn make_layout_datatype(pyname: &Ident, dt: &str) -> TokenStream2 {
    let doc = DocString::new_ivar(
        "The value of *$DATATYPE*.",
        [format!("Will always return ``\"{dt}\"``.")],
        DocReturn::new(PyType::new_datatype()),
    );
    doc.into_impl_get(pyname, "datatype", |_, _| quote!(self.0.datatype().into()))
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
    make_gate_region(&path, true)
}

#[proc_macro]
pub fn impl_new_gate_bi_regions(input: TokenStream) -> TokenStream {
    let path: Path = syn::parse(input).unwrap();
    make_gate_region(&path, false)
}

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
                PyType::new_meas_or_gate_index(),
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
            PyType::new_unigate(),
            "The lower and upper bounds of the gate.",
        )
    } else {
        (
            "bivariate",
            PyTuple::new1(vec![index_pytype_inner; 2])
                .rstype(parse_quote!(#index_pair<#index_path_inner>))
                .into(),
            "vertices",
            PyType::new_vertices(),
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

    let doc = DocString::new_class(summary, [""; 0], [index_arg, gate_arg]);

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

fn correction_path(is_header: bool, id: &str) -> Path {
    let src = if is_header {
        "SegmentFromHeader"
    } else {
        "SegmentFromTEXT"
    };
    let s = format_ident!("{src}");
    let i = format_ident!("{id}");
    let root = quote!(fireflow_core::segment);
    parse_quote! (#root::OffsetCorrection<#root::#i, #root::#s>)
}

fn config_path(n: &str) -> Path {
    let t = format_ident!("{n}");
    parse_quote!(fireflow_core::config::#t)
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

#[derive(Clone, Copy)]
enum SegmentSrc {
    Header,
    // Text,
    Any,
}

impl fmt::Display for SegmentSrc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let s = match self {
            Self::Header => "*HEADER*",
            // Self::Text => "*TEXT*",
            Self::Any => "*HEADER* or *TEXT*",
        };
        f.write_str(s)
    }
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

#[derive(Clone, new, AsRef)]
struct DocArg<T> {
    #[new(into)]
    argname: String,
    #[as_ref(ArgPyType)]
    #[new(into)]
    pytype: ArgPyType,
    #[new(into)]
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
    Auto,
    Int(usize),
    Str(String),
}

#[derive(Clone)]
struct DocReturn<T> {
    rtype: T,
    desc: Option<String>,
    // TODO this should always be empty for ivars
    exceptions: Vec<ReturnPyException>,
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

#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct PyException {
    pyname: String,
    desc: Option<String>,
}

#[derive(Clone, PartialEq, Eq, Hash, From)]
struct ReturnPyException(PyException);

#[derive(Clone, PartialEq, Eq, Hash, new)]
struct ArgPyException {
    inner: PyException,
    argmod: ExcNameMod,
}

#[derive(Clone, PartialEq, Eq, Hash, Default, PartialOrd, Ord)]
enum ExcNameMod {
    #[default]
    NoMod,
    Field(NonEmpty<usize>, Box<Self>),
    List(Box<Self>),
    DictKey(Box<Self>),
    DictVal(Box<Self>),
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
        Self::Field(NonEmpty::new(f), self.into())
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
                Self::Field(f, t) => field_trees.push((f.head, *t)),
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
                let fs_ = NonEmpty::collect(fs.into_iter().sorted()).unwrap();
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
            names: NonEmpty::new(name.into()),
        }
    }
}

#[derive(new)]
struct NamedPyException {
    names: NonEmpty<String>,
    inner: ArgPyException,
}

impl PyException {
    fn new(pyname: impl fmt::Display) -> Self {
        Self {
            pyname: pyname.to_string(),
            desc: None,
        }
    }

    fn new_value_error() -> Self {
        Self::new("ValueError")
    }

    fn new_overflow_error() -> Self {
        Self::new("OverflowError")
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
            .map(|x| ((x.names.head, x.inner.inner), x.inner.argmod))
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
                    NonEmpty::collect(names.into_iter().sorted()).unwrap(),
                    ArgPyException::new(exc, argmod),
                )
            })
            .collect()
    }
}

type ArgPyType = PyType<ArgPyException>;
type RetPyType = PyType<()>;

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
    Dict(Box<PyAtom<R>>, Box<PyAtom<R>>),
    Tuple(Vec<PyAtom<R>>),
    List(Box<PyAtom<R>>),
    Literal(PyLiteral),
    PyClass(PyClass<R>),
    Union(Box<PyAtom<R>>, Box<PyAtom<R>>, Vec<PyAtom<R>>),
}

impl<R: Clone + PartialEq + Eq + Hash> PyAtom<R> {
    fn flatten_unions(self) -> Self {
        fn go<Q: Clone + PartialEq + Eq + Hash>(x: PyAtom<Q>) -> NonEmpty<PyAtom<Q>> {
            match x {
                PyAtom::Union(x0, x1, xs) => {
                    let ys = go(*x0)
                        .into_iter()
                        .chain(go(*x1))
                        .chain(xs.into_iter().flat_map(go));
                    NonEmpty::collect(ys).unwrap()
                }
                y => NonEmpty::new(y.flatten_unions()),
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

#[derive(Clone, new)]
struct PyInt<E> {
    rs: RsInt,
    rstype: Option<Path>,
    exc: Option<E>,
}

#[derive(Clone, From, new)]
struct PyFloat<E> {
    #[from]
    rs: RsFloat,
    rstype: Option<Path>,
    exc: Option<E>,
}

#[derive(Clone, Default, new)]
struct PyStr<E> {
    rstype: Option<Path>,
    exc: Option<E>,
}

#[derive(Clone, Default, new)]
struct PyBool<E> {
    rstype: Option<Path>,
    exc: Option<E>,
}

#[derive(Clone, Default, new)]
struct PyBytes<E> {
    rstype: Option<Path>,
    exc: Option<E>,
}

#[derive(Clone, Default, new)]
struct PyDecimal<E> {
    rstype: Option<Path>,
    exc: Option<E>,
}

#[derive(Clone, Default, new)]
struct PyTime<E> {
    rstype: Option<Path>,
    exc: Option<E>,
}

#[derive(Clone, Default, new)]
struct PyDate<E> {
    rstype: Option<Path>,
    exc: Option<E>,
}

#[derive(Clone, Default, new)]
struct PyDatetime<E> {
    rstype: Option<Path>,
    exc: Option<E>,
}

#[derive(Clone, PartialEq, Hash, Eq, new)]
struct PyLiteral {
    #[new(into)]
    head: &'static str,
    #[new(into_iter = "&'static str")]
    tail: Vec<&'static str>,
    #[new(into)]
    rstype: Option<Path>,
}

#[derive(Clone, new)]
struct PyOpt<R> {
    #[new(into)]
    inner: PyType<R>,
}

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

#[derive(Clone, new)]
struct PyList<E> {
    #[new(into)]
    inner: PyType<E>,
    #[new(into)]
    rstype: Option<Path>,
    exc: Option<E>,
}

#[derive(Clone, new, PartialEq, Hash, Eq)]
struct PyClass<E> {
    #[new(into)]
    pyname: String,
    #[new(into)]
    rstype: Option<Path>,
    exc: Option<E>,
}

#[derive(Clone, new)]
struct PyUnion<E> {
    #[new(into)]
    head0: PyType<E>,
    #[new(into)]
    head1: PyType<E>,
    tail: Vec<PyType<E>>,
    rstype: Path,
    exc: Option<E>,
}

#[derive(Clone, new)]
struct PyTuple<E> {
    inner: Vec<PyType<E>>,
    rstype: Option<Path>,
    exc: Option<E>,
}

impl<E> Default for PyTuple<E> {
    fn default() -> Self {
        Self::new(vec![], None, None)
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

macro_rules! impl_py_prim_defaults {
    ($py:expr, $rs:path) => {
        fn defaults(&self) -> (String, TokenStream2) {
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
        fn defaults(&self) -> (String, TokenStream2) {
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

impl<E> PyInt<E> {
    fn rstype(self, rstype: Path) -> Self {
        Self::new(self.rs, Some(rstype), self.exc)
    }

    fn exc(self, exc: impl Into<E>) -> Self {
        Self::new(self.rs, self.rstype, Some(exc.into()))
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
    fn new_meas_index() -> Self {
        let p = parse_quote!(fireflow_core::text::index::MeasIndex);
        Self::new_nonzero_usize().rstype(p)
    }

    fn new_gate_index() -> Self {
        let p = parse_quote!(fireflow_core::text::index::GateIndex);
        Self::new_nonzero_usize().rstype(p)
    }

    fn new_prefixed_meas_index() -> Self {
        let p = parse_quote!(fireflow_core::text::keywords::PrefixedMeasIndex);
        Self::new_nonzero_usize().rstype(p)
    }

    fn new_u32() -> Self {
        Self::new_int(RsInt::U32)
    }

    fn new_nonzero_usize() -> Self {
        Self::new_int(RsInt::NonZeroUsize)
    }

    fn new_int(intkind: RsInt) -> Self {
        let e = PyException::new_overflow_error().desc(intkind.exc_desc());
        Self::from(intkind).exc(e)
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
        let e =
            PyException::new_value_error().desc("if %x is negative, ``NaN``, ``inf``, or ``-inf``");
        Self::from(RsFloat::F32).exc(e)
    }

    fn new_positive_float() -> Self {
        let e = PyException::new_value_error()
            .desc("if %x is negative, ``0.0``, ``NaN``, ``inf``, or ``-inf``");
        Self::from(RsFloat::F32).exc(e)
    }
}

impl_py_prim_default!(PyStr);
impl_py_prim_default!(PyBool);
impl_py_prim_default!(PyBytes);
impl_py_prim_default!(PyDecimal);
impl_py_prim_default!(PyDate);
impl_py_prim_default!(PyTime);
impl_py_prim_default!(PyDatetime);

impl<E> PyStr<E> {
    impl_py_prim_rstype!();
    impl_py_prim_exc!();
    impl_py_prim_defaults!("\"\"".into(), String);
    impl_py_prim_map_exc!(PyStr);
}

impl<E: From<PyException>> PyStr<E> {
    fn new_shortname() -> Self {
        let path = parse_quote!(fireflow_core::validated::shortname::Shortname);
        let e = PyException::new_value_error().desc("if %x is ``\"\"`` or contains commas");
        Self::default().rstype(path).exc(e)
    }

    fn new_keystring() -> Self {
        let path: Path = parse_quote!(fireflow_core::validated::keys::KeyString);
        let e =
            PyException::new_value_error().desc("if %x contains non-ASCII characters or is empty");
        Self::default().rstype(path).exc(e)
    }

    fn new_regexp() -> Self {
        let desc = format!("if %x is not a valid regular expression as described in {REGEXP_REF}");
        let exc = PyException::new_value_error().desc(desc);
        Self::default().exc(exc)
    }
}

impl<E> PyBool<E> {
    impl_py_prim_rstype!();
    impl_py_prim_defaults!("False".into(), bool);
    impl_py_prim_map_exc!(PyBool);
}

impl<E> PyBytes<E> {
    impl_py_prim_rstype!();
    impl_py_prim_defaults!("b\"\"".into(), Vec);
    impl_py_prim_map_exc!(PyBytes);
}

impl<E> PyDecimal<E> {
    impl_py_prim_rstype!();
    impl_py_prim_defaults!("0".into(), bigdecimal::BigDecimal);
    impl_py_prim_map_exc!(PyDecimal);
}

impl<E: From<PyException>> PyDecimal<E> {
    fn new_range() -> Self {
        let path = parse_quote!(fireflow_core::text::keywords::Range);
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

    impl_py_prim_defaults!("{}".into(), std::collections::HashMap);

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
        let path: Path = parse_quote!(fireflow_core::validated::keys::KeyStringPairs);
        // TODO exception if dict keys are not unique
        Self::new(PyStr::new_keystring(), PyStr::new_keystring(), path, None)
    }
}

impl<E> PyList<E> {
    fn new1(inner: impl Into<PyType<E>>) -> Self {
        Self::new(inner, None, None)
    }

    // TODO impl rstype?

    // fn exc(self, exc: impl Into<E>) -> Self {
    //     Self::new(self.inner, self.rstype, Some(exc.into()))
    // }

    impl_py_prim_defaults!("[]".into(), Vec);

    fn map_exc<F: Clone + Fn(E) -> E1, E1>(self, f: F) -> PyList<E1> {
        PyList::new(self.inner.map_exc(f.clone()), self.rstype, self.exc.map(f))
    }
}

impl<E: From<PyException>> PyList<E> {
    fn new_non_empty(inner: impl Into<PyType<E>>, inner_path: &Path) -> Self {
        let nonempty = quote!(fireflow_core::nonempty::FCSNonEmpty);
        let e = PyException::new_value_error().desc("if %x is empty");
        Self::new(
            inner,
            Some(parse_quote!(#nonempty<#inner_path>)),
            Some(e.into()),
        )
    }
}

impl PyLiteral {
    fn new1(iter: impl IntoIterator<Item = &'static str>) -> Self {
        let mut it = iter.into_iter();
        let head = it.next().expect("Literal cannot be empty");
        Self::new(head, it, None)
    }

    fn new2(iter: impl IntoIterator<Item = &'static str>, rstype: Path) -> Self {
        let mut x = Self::new1(iter);
        x.rstype = Some(rstype);
        x
    }
}

impl<E> PyOpt<E> {
    fn defaults() -> (String, TokenStream2) {
        ("None".into(), quote!(None))
    }

    fn wrap_if(inner: impl Into<PyType<E>>, test: bool) -> PyType<E> {
        if test {
            Self::new(inner).into()
        } else {
            inner.into()
        }
    }

    fn map_exc<F: Clone + Fn(E) -> E1, E1>(self, f: F) -> PyOpt<E1> {
        PyOpt::new(self.inner.map_exc(f))
    }
}

impl<E> PyTuple<E> {
    fn new1(iter: impl IntoIterator<Item = impl Into<PyType<E>>>) -> Self {
        Self::new(iter.into_iter().map(Into::into).collect(), None, None)
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
    fn new_sub_patterns() -> Self {
        let path: Path = parse_quote!(fireflow_core::validated::sub_pattern::SubPatterns);
        let lit = PyDict::new1(PyStr::new_keystring(), Self::new_sub_pattern());
        let pat = PyDict::new1(PyStr::new_regexp(), Self::new_sub_pattern());
        Self::new1([lit, pat]).rstype(path)
    }

    fn new_sub_pattern() -> Self {
        let desc = "if references in replacement string in %x \
                    do not match captures in regular expression";
        let exc = PyException::new_value_error().desc(desc);
        Self::new1([
            PyStr::new_regexp().into(),
            PyStr::default().into(),
            PyType::from(PyBool::default()),
        ])
        .exc(exc)
    }

    fn new_calibration3_1() -> Self {
        Self::new1([
            PyType::from(PyFloat::new_positive_float()),
            PyStr::default().into(),
        ])
        .rstype(keyword_path("Calibration3_1"))
    }

    fn new_calibration3_2() -> Self {
        Self::new1([
            PyType::from(PyFloat::new_positive_float()),
            RsFloat::F32.into(),
            PyStr::default().into(),
        ])
        .rstype(keyword_path("Calibration3_2"))
    }

    fn new_display() -> Self {
        let desc = "if %x represents a log display (field 1 is ``True``) and \
                    the two floats are not both positive";
        let exc = PyException::new_value_error().desc(desc);
        Self::new1([
            PyType::from(PyBool::default()),
            RsFloat::F32.into(),
            RsFloat::F32.into(),
        ])
        .exc(exc)
        .rstype(keyword_path("Display"))
    }

    fn new_segment(n: &str) -> Self {
        let t = format_ident!("{n}");
        let p = parse_quote!(fireflow_core::segment::#t);
        let desc = "if %x has offsets which exceed the end of the file, \
                    are inverted (begin after end), or are either negative \
                    or greater than ``2**64-1``";
        let exc = PyException::new_value_error().desc(desc);
        // NOTE don't use ints with overflow exceptions since this is captured
        // in the overall exception for the entire type
        Self::new1([RsInt::U64, RsInt::U64]).exc(exc).rstype(p)
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

    fn new_correction(is_header: bool, id: &str) -> Self {
        let path = correction_path(is_header, id);
        Self::new1([PyInt::new_int(RsInt::I32), PyInt::new_int(RsInt::I32)]).rstype(path)
    }
}

impl<E> PyUnion<E> {
    fn new1<T, A>(iter: T, rstype: Path) -> Self
    where
        T: IntoIterator<Item = A>,
        A: Into<PyType<E>>,
    {
        let mut it = iter.into_iter();
        let x0 = it.next().expect("Union cannot be empty");
        let x1 = it.next().expect("Union must have at least 2 types");
        let xs = it.map(Into::into).collect();
        Self::new(x0, x1, xs, rstype, None)
    }

    fn new2(x: impl Into<PyType<E>>, y: impl Into<PyType<E>>, rstype: Path) -> Self {
        Self::new(x, y, vec![], rstype, None)
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

impl<E> PyClass<E> {
    fn new1(pyname: impl fmt::Display) -> Self {
        Self::new(pyname.to_string(), None, None)
    }

    fn rstype(self, rstype: Path) -> Self {
        Self::new(self.pyname, Some(rstype), None)
    }

    fn exc(self, exc: impl Into<E>) -> Self {
        Self::new(self.pyname, self.rstype, Some(exc.into()))
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

    fn map_exc<F: FnOnce(E) -> E1, E1>(self, f: F) -> PyClass<E1> {
        PyClass::new(self.pyname, self.rstype, self.exc.map(f))
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
        let i = self.inner.as_rust_type();
        parse_quote!(Option<#i>)
    }
}

impl<E> HasRustPath for PyDict<E> {
    fn as_rust_type(&self) -> Type {
        if let Some(x) = self.rstype.as_ref() {
            parse_quote!(#x)
        } else {
            let k = &self.key.as_rust_type();
            let v = &self.value.as_rust_type();
            parse_quote!(std::collections::HashMap<#k, #v>)
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
        let x = &self.rstype;
        parse_quote!(#x)
    }
}

trait HasRustPath {
    fn as_rust_type(&self) -> Type;
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
            Self::U16 => parse_quote!(u16),
            Self::U32 => parse_quote!(u32),
            Self::U64 => parse_quote!(u64),
            Self::Usize => parse_quote!(usize),
            Self::NonZeroU8 => parse_quote!(std::num::NonZeroU8),
            Self::NonZeroUsize => parse_quote!(std::num::NonZeroUsize),
            Self::I32 => parse_quote!(i32),
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

#[derive(Clone)]
enum RsInt {
    U8,
    U16,
    U32,
    U64,
    I32,
    Usize,
    NonZeroU8,
    NonZeroUsize,
}

impl RsInt {
    fn lower(&self) -> &'static str {
        match self {
            Self::U8 | Self::U16 | Self::U32 | Self::U64 | Self::Usize => "0",
            Self::NonZeroU8 | Self::NonZeroUsize => "1",
            Self::I32 => "-2**31",
        }
    }

    fn upper(&self) -> String {
        match self {
            Self::U8 | Self::NonZeroU8 => "255".into(),
            Self::U16 => "2**16-1".into(),
            Self::U32 => "2**32-1".into(),
            Self::I32 => "2**31-1".into(),
            Self::U64 => "2**64-1".into(),
            Self::Usize | Self::NonZeroUsize => format!("2**{}-1", usize::BITS),
        }
    }

    fn exc_desc(&self) -> String {
        format!(
            "if %x is less than ``{}`` or greater than ``{}``",
            self.lower(),
            self.upper()
        )
    }
}

#[derive(Clone)]
enum RsFloat {
    F32,
    F64,
}

impl DocArgROIvar {
    fn new_ivar_ro(
        argname: impl fmt::Display + Clone,
        pytype: impl Into<ArgPyType>,
        desc: impl fmt::Display,
        f: impl FnOnce(&Ident, &ArgPyType) -> TokenStream2,
    ) -> Self {
        let pt = pytype.into();
        let a = argname.to_string();
        let method = GetMethod::from_pytype(a.as_str(), &pt, f);
        Self::new(a, pt, desc.to_string(), None, method)
    }

    fn new_ivar_ro_def(
        argname: impl fmt::Display,
        pytype: impl Into<ArgPyType>,
        desc: impl fmt::Display,
        def: DocDefault,
        f: impl FnOnce(&Ident, &ArgPyType) -> TokenStream2,
    ) -> Self {
        let pt = pytype.into();
        let a = argname.to_string();
        let method = GetMethod::from_pytype(a.as_str(), &pt, f);
        Self::new(a, pt, desc.to_string(), Some(def), method)
    }

    fn new_version_ivar() -> Self {
        Self::new_ivar_ro(
            "version",
            PyType::new_version(),
            "The FCS version.",
            |_, _| quote!(self.0.version),
        )
    }
}

impl DocArgRWIvar {
    fn new_ivar_rw(
        argname: impl fmt::Display,
        pytype: impl Into<ArgPyType>,
        desc: impl fmt::Display,
        fallible: bool,
        f: impl FnOnce(&Ident, &ArgPyType) -> TokenStream2,
        g: impl FnOnce(&Ident, &ArgPyType) -> TokenStream2,
    ) -> Self {
        let pt = pytype.into();
        let name = argname.to_string();
        let methods = GetSetMethods::from_pytype(name.as_str(), &pt, fallible, f, g);
        Self::new(name, pt, desc.to_string(), None, methods)
    }

    fn new_ivar_rw_def(
        argname: impl fmt::Display,
        pytype: impl Into<ArgPyType>,
        desc: impl fmt::Display,
        def: DocDefault,
        fallible: bool,
        f: impl FnOnce(&Ident, &ArgPyType) -> TokenStream2,
        g: impl FnOnce(&Ident, &ArgPyType) -> TokenStream2,
    ) -> Self {
        let pt = pytype.into();
        let name = argname.to_string();
        let methods = GetSetMethods::from_pytype(name.as_str(), &pt, fallible, f, g);
        Self::new(name, pt, desc.to_string(), Some(def), methods)
    }

    fn new_opt_ivar_rw(
        argname: impl fmt::Display,
        pytype: impl Into<ArgPyType>,
        desc: impl fmt::Display,
        fallible: bool,
        f: impl FnOnce(&Ident, &ArgPyType) -> TokenStream2,
        g: impl FnOnce(&Ident, &ArgPyType) -> TokenStream2,
    ) -> Self {
        let pt = PyOpt::new(pytype.into());
        Self::new_ivar_rw_def(argname, pt, desc, DocDefault::Auto, fallible, f, g)
    }

    fn new_kw_ivar<F, T>(kw: &str, name: &str, f: F, desc: Option<&str>, def: bool) -> Self
    where
        F: FnOnce(Path) -> T,
        T: Into<ArgPyType>,
    {
        let path = keyword_path(kw);
        let pytype: ArgPyType = f(path.clone()).into();

        let d = desc.map_or(format!("Value of *${}*.", name.to_uppercase()), Into::into);

        let get_f = |_: &Ident, pt: &ArgPyType| {
            let optional = matches!(pt, PyType::Option(_));
            let get_inner = format_ident!("{}", if optional { "metaroot_opt" } else { "metaroot" });
            let clone_inner = format_ident!("{}", if optional { "cloned" } else { "clone" });
            quote!(self.0.#get_inner::<#path>().#clone_inner())
        };
        let set_f = |n: &Ident, _: &ArgPyType| quote!(self.0.set_metaroot(#n));

        if def {
            Self::new_ivar_rw_def(name, pytype, d, DocDefault::Auto, false, get_f, set_f)
        } else {
            Self::new_ivar_rw(name, pytype, d, false, get_f, set_f)
        }
    }

    fn new_kw_ivar_str(kw: &str, name: &str) -> Self {
        Self::new_kw_ivar(kw, name, |p| PyStr::default().rstype(p), None, true)
    }

    fn new_meas_kw_ivar<F, T>(kw: &str, name: &str, f: F, desc: Option<&str>, def: bool) -> Self
    where
        F: FnOnce(Path) -> T,
        T: Into<ArgPyType>,
    {
        let path = keyword_path(kw);
        let pytype: ArgPyType = f(path).into();
        let full_path = pytype.as_rust_type();

        let d = desc.map_or(format!("Value of *${}*.", name.to_uppercase()), Into::into);

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

        if def {
            Self::new_ivar_rw_def(name, pytype, d, DocDefault::Auto, false, get_f, set_f)
        } else {
            Self::new_ivar_rw(name, pytype, d, false, get_f, set_f)
        }
    }

    fn new_kw_opt_ivar<F, T>(kw: &str, name: &str, f: F) -> Self
    where
        F: FnOnce(Path) -> T,
        T: Into<ArgPyType>,
    {
        Self::new_kw_ivar(kw, name, |p| PyOpt::new(f(p)), None, true)
    }

    fn new_meas_kw_ivar1<F, T>(kw: &str, name: &str, abbr: &str, f: F) -> Self
    where
        F: FnOnce(Path) -> T,
        T: Into<ArgPyType>,
    {
        let desc = format!("Value for *$Pn{abbr}*.");
        Self::new_meas_kw_ivar(kw, name, f, Some(desc.as_str()), true)
    }

    fn new_meas_kw_opt_ivar<F, T>(kw: &str, name: &str, abbr: &str, f: F) -> Self
    where
        F: FnOnce(Path) -> T,
        T: Into<ArgPyType>,
    {
        Self::new_meas_kw_ivar1(kw, name, abbr, |p| PyOpt::new(f(p)))
    }

    fn new_meas_kw_str(kw: &str, name: &str, abbr: &str) -> Self {
        Self::new_meas_kw_ivar1(kw, name, abbr, |p| PyStr::default().rstype(p))
    }

    fn new_layout_ivar(version: Version) -> Self {
        let ascii_layouts = ["FixedAsciiLayout", "DelimAsciiLayout"];
        let non_mixed_layouts = ["EndianUintLayout", "EndianF32Layout", "EndianF64Layout"];
        let ordered_layouts = [
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

        let layout_pytype = match version {
            Version::FCS3_2 => {
                let ys = ascii_layouts
                    .into_iter()
                    .chain(non_mixed_layouts)
                    .chain(["MixedLayout"])
                    .map(PyClass::new1);
                PyUnion::new1(ys, parse_quote!(PyLayout3_2))
            }
            Version::FCS3_1 => {
                let ys = ascii_layouts
                    .into_iter()
                    .chain(non_mixed_layouts)
                    .map(PyClass::new1);
                PyUnion::new1(ys, parse_quote!(PyNonMixedLayout))
            }
            _ => {
                let ys = ascii_layouts
                    .into_iter()
                    .chain(ordered_layouts)
                    .map(PyClass::new1);
                PyUnion::new1(ys, parse_quote!(PyOrderedLayout))
            }
        };
        let layout_desc = if version == Version::FCS3_2 {
            "Layout to describe data encoding. Represents *$PnB*, *$PnR*, *$BYTEORD*, \
             *$DATATYPE*, and *$PnDATATYPE*."
        } else {
            "Layout to describe data encoding. Represents *$PnB*, *$PnR*, *$BYTEORD*, \
             and *$DATATYPE*."
        };

        Self::new_ivar_rw(
            "layout",
            layout_pytype,
            layout_desc,
            true,
            |_, _| quote!(self.0.layout().clone().into()),
            |_, _| {
                quote!(self
                    .0
                    .set_layout(layout.into())
                    .py_termfail_resolve_nowarn())
            },
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
                    let data = self.0.data();
                    #rt(data.as_polars_dataframe(&ns[..]))
                }
            },
            |n, _| {
                quote! {
                    let d = #n.0.try_into()?;
                    Ok(self.0.set_data(d)?)
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
            let name = if is_start { "btim" } else { "etim" };
            let get_naive = format_ident!("{name}_naive");
            let set_naive = format_ident!("set_{name}_naive");
            let desc = format!("Value of *${}*.", name.to_uppercase());
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
            "Value of *$DATE*.",
            true,
            |_, _| quote!(self.0.date_naive()),
            |n, _| quote!(Ok(self.0.set_date_naive(#n)?)),
        );

        [make_time_ivar(true), make_time_ivar(false), date_arg]
    }

    fn new_datetime_ivar(is_start: bool) -> Self {
        let name = if is_start {
            "begindatetime"
        } else {
            "enddatetime"
        };
        let get = format_ident!("{name}");
        let set = format_ident!("set_{name}");
        Self::new_opt_ivar_rw(
            name,
            PyDatetime::default(),
            format!("Value for *${}*.", name.to_uppercase()),
            true,
            |_, _| quote!(self.0.#get()),
            |n, _| quote!(Ok(self.0.#set(#n)?)),
        )
    }

    fn new_comp_ivar(is_2_0: bool) -> Self {
        let rstype: Path = parse_quote!(fireflow_core::text::compensation::Compensation);
        let desc = if is_2_0 {
            "The compensation matrix. Must be a square array with number of \
             rows/columns equal to the number of measurements. Non-zero entries \
             will produce a *$DFCmTOn* keyword."
        } else {
            "The value of *$COMP*. Must be a square array with number of \
             rows/columns equal to the number of measurements."
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
        let rstype: Path = parse_quote!(fireflow_core::text::spillover::Spillover);
        let matrix_exc = PyException::new_value_error()
            .desc("if %x is not a square matrix that is 2x2 or larger");
        let spill_exc = PyException::new_value_error().desc(
            "if matrix in %x does not have the same number of rows \
             and columns as the measurement vector",
        );
        // TODO return exception if PnN don't match
        // TODO exception on non-unique names
        Self::new_opt_ivar_rw(
            "spillover",
            PyTuple::new1([
                PyType::from(PyList::new1(PyStr::new_shortname())),
                PyClass::new1("~numpy.ndarray").exc(matrix_exc).into(),
            ])
            .rstype(rstype)
            .exc(spill_exc),
            "Value for *$SPILLOVER*. First element of tuple the list of measurement \
             names and the second is the matrix. Each measurement name must \
             correspond to a *$PnN*, must be unique, and the length of this list \
             must match the number of rows and columns of the matrix. The matrix \
             must be at least 2x2.",
            true,
            |_, _| quote!(self.0.spillover().map(|x| x.clone())),
            |n, _| quote!(Ok(self.0.set_spillover(#n)?)),
        )
    }

    fn new_csvflags_ivar() -> Self {
        let path: Path = parse_quote!(fireflow_core::core::CSVFlags);
        Self::new_ivar_rw_def(
            "csvflags",
            PyList::new(PyOpt::new(PyInt::new_u32()), path.clone(), None),
            "Subset flags. Each element in the list corresponds to *$CSVnFLAG* and \
             the length of the list corresponds to *$CSMODE*.",
            DocDefault::Auto,
            false,
            |_, _| quote!(self.0.metaroot::<#path>().clone()),
            |n, _| quote!(self.0.set_metaroot(#n)),
        )
    }

    // TODO exception for mismatch PnN
    fn new_trigger_ivar() -> Self {
        Self::new_opt_ivar_rw(
            "tr",
            PyType::new_tr(),
            "Value for *$TR*. First member of tuple is threshold and second is the \
             measurement name which must match a *$PnN*.",
            true,
            |_, _| quote!(self.0.metaroot_opt().cloned()),
            |n, _| quote!(Ok(self.0.set_trigger(#n)?)),
        )
    }

    fn new_unstainedcenters_ivar() -> Self {
        let path = keyword_path("UnstainedCenters");
        // TODO exceptions for links
        Self::new_ivar_rw_def(
            "unstainedcenters",
            PyDict::new(PyStr::new_shortname(), RsFloat::F32, path.clone(), None),
            "Value for *$UNSTAINEDCENTERS. Each key must match a *$PnN*.",
            DocDefault::Auto,
            true,
            |_, _| quote!(self.0.metaroot::<#path>().clone()),
            |n, _| {
                quote!(self
                    .0
                    .set_unstained_centers(#n)
                    .py_termfail_resolve_nowarn())
            },
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
        let rstype = format_ident!("Py{rstype_inner}");
        let gm_pytype = (collapsed_version < Version::FCS3_2)
            .then(|| PyList::new1(PyClass::new_py([""; 0], "GatedMeasurement")).into());
        let ur_pytype = PyClass::new1(format!("UnivariateRegion{vsu}"));
        let bv_pytype = PyClass::new1(format!("BivariateRegion{vsu}"));
        let reg_rstype = format_ident!("PyRegion{vsu}");
        let map_rstype = parse_quote!(PyRegionMapping<#reg_rstype>);
        let reg_pytype = PyDict::new(
            RsInt::NonZeroUsize,
            PyUnion::new2(ur_pytype, bv_pytype, parse_quote!(#reg_rstype)),
            Some(map_rstype),
            None,
        )
        .into();
        let gtype = PyType::from(PyOpt::new(PyStr::default()));
        let pytype = PyTuple::new1(gm_pytype.into_iter().chain([reg_pytype, gtype]))
            .rstype(parse_quote!(#rstype));

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
        };

        if collapsed_version == Version::FCS2_0 {
            Self::new_ivar_rw_def(
                "applied_gates",
                pytype,
                desc,
                DocDefault::Auto,
                false,
                |_, _| quote!(self.0.metaroot::<#rstype_inner>().clone().into()),
                |n, _| quote!(self.0.set_metaroot::<#rstype_inner>(#n.into())),
            )
        } else {
            let setter = format_ident!("set_applied_gates_{vsu}");
            Self::new_ivar_rw_def(
                "applied_gates",
                pytype,
                desc,
                DocDefault::Auto,
                true,
                |_, _| quote!(self.0.metaroot::<#rstype_inner>().clone().into()),
                |n, _| quote!(Ok(self.0.#setter(#n.into())?)),
            )
        }
    }

    fn new_scale_ivar() -> Self {
        Self::new_opt_ivar_rw(
            "scale",
            PyType::new_scale(false),
            "Value for *$PnE*. Empty tuple means linear scale; 2-tuple encodes \
             decades and offset for log scale",
            false,
            |_, _| quote!(self.0.specific.scale.as_ref().map(|&x| x)),
            |n, _| quote!(self.0.specific.scale = #n.into()),
        )
    }

    fn new_transform_ivar() -> Self {
        Self::new_ivar_rw(
            "transform",
            PyType::new_transform(),
            "Value for *$PnE* and/or *$PnG*. Singleton float encodes gain (*$PnG*) \
             and implies linear scaling (ie *$PnE* is ``0,0``). 2-tuple encodes \
             decades and offset for log scale, and implies *$PnG* is not set.",
            false,
            |_, _| quote!(self.0.specific.scale),
            |n, _| quote!(self.0.specific.scale = #n),
        )
    }

    fn new_core_nonstandard_keywords_ivar() -> Self {
        Self::new_nonstandard_keywords_ivar(
            "Pairs of non-standard keyword values. Keys must not start with *$*.",
            |_, _| quote!(self.0.nonstandard_keywords().clone()),
            |n, _| quote!(self.0.set_nonstandard_keywords(#n)),
        )
    }

    fn new_meas_nonstandard_keywords_ivar() -> Self {
        Self::new_nonstandard_keywords_ivar(
            "Any non-standard keywords corresponding to this measurement. No keys \
             should start with *$*. Realistically each key should follow a pattern \
             corresponding to the measurement index, something like prefixing with \
             \"P\" followed by the index. This is not enforced.",
            |_, _| quote!(self.0.common.nonstandard_keywords.clone()),
            |n, _| quote!(self.0.common.nonstandard_keywords = #n),
        )
    }

    fn new_nonstandard_keywords_ivar(
        desc: &str,
        f: impl FnOnce(&Ident, &ArgPyType) -> TokenStream2,
        g: impl FnOnce(&Ident, &ArgPyType) -> TokenStream2,
    ) -> Self {
        Self::new_ivar_rw_def(
            "nonstandard_keywords",
            PyType::new_nonstd_keywords(),
            desc,
            DocDefault::Auto,
            false,
            f,
            g,
        )
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

    fn new_param_def(
        argname: impl fmt::Display,
        pytype: impl Into<ArgPyType>,
        desc: impl fmt::Display,
        def: DocDefault,
    ) -> Self {
        let pt = pytype.into();
        Self::new(
            argname.to_string(),
            pt,
            desc.to_string(),
            Some(def),
            NoMethods,
        )
    }

    fn new_bool_param(name: impl fmt::Display, desc: impl fmt::Display) -> Self {
        Self::new_param_def(name, PyBool::default(), desc, DocDefault::Auto)
    }

    fn new_opt_param(
        name: impl fmt::Display,
        pytype: impl Into<ArgPyType>,
        desc: impl fmt::Display,
    ) -> Self {
        Self::new_param_def(name, PyOpt::new(pytype), desc, DocDefault::Auto)
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

    fn new_path_param(read: bool) -> Self {
        let s = if read { "read" } else { "written" };
        let pt = PyClass::new1("~pathlib.Path").rstype(parse_quote!(std::path::PathBuf));
        Self::new_param("path", pt, format!("Path to be {s}."))
    }

    fn new_version_param() -> Self {
        let desc = "Version to use when parsing *TEXT*.";
        Self::new_param("version", PyType::new_version(), desc)
    }

    fn new_std_keywords_param() -> Self {
        Self::new_param("std", PyType::new_std_keywords(), "Standard keywords.")
    }

    fn new_nonstd_keywords_param() -> Self {
        let desc = "Non-standard keywords.";
        Self::new_param("nonstd", PyType::new_nonstd_keywords(), desc)
    }

    fn new_valid_keywords_param() -> Self {
        Self::new_param(
            "kws",
            PyClass::new_py(["api"], "ValidKeywords"),
            "Standard and non-standard keywords.",
        )
    }

    fn new_extra_std_keywords_param() -> Self {
        Self::new_param(
            "extra",
            PyClass::new_py(["api"], "ExtraStdKeywords"),
            "Extra keywords from *TEXT* standardization",
        )
    }

    fn new_dataset_segments_param() -> Self {
        Self::new_param(
            "dataset_segs",
            PyClass::new_py(["api"], "DatasetSegments"),
            "Offsets used to parse *DATA* and *ANALYSIS*.",
        )
    }

    fn new_parse_output_param() -> Self {
        Self::new_param(
            "parse",
            PyClass::new_py(["api"], "RawTEXTParseData"),
            "Miscellaneous data obtained when parsing *TEXT*.",
        )
    }

    fn new_text_seg_param() -> Self {
        Self::new_param(
            "text_seg",
            PyTuple::new_text_segment(),
            "The primary *TEXT* segment from *HEADER*.",
        )
    }

    fn new_data_seg_param(src: SegmentSrc) -> Self {
        Self::new_param(
            "data_seg",
            PyTuple::new_data_segment(src),
            format!("The *DATA* segment from {src}."),
        )
    }

    fn new_analysis_seg_param(src: SegmentSrc, default: bool) -> Self {
        Self::new(
            "analysis_seg",
            PyTuple::new_analysis_segment(src),
            format!("The *DATA* segment from {src}."),
            default.then_some(DocDefault::Auto),
            NoMethods,
        )
    }

    fn new_other_segs_param(default: bool) -> Self {
        Self::new(
            "other_segs",
            PyList::new1(PyTuple::new_other_segment()),
            "The *OTHER* segments from *HEADER*.",
            default.then_some(DocDefault::Auto),
            NoMethods,
        )
    }

    fn new_textdelim_param() -> Self {
        let path = parse_quote!(fireflow_core::validated::textdelim::TEXTDelim);
        let exc = PyException::new_value_error().desc("if %x is not between 1 and 126");
        let pytype = PyInt::from(RsInt::U8).rstype(path).exc(exc);
        let desc = "Delimiter to use when writing *TEXT*.";
        Self::new_param_def("delim", pytype, desc, DocDefault::Int(30))
    }

    fn new_big_other_param() -> Self {
        let desc = "If ``True`` use 20 chars for OTHER segment offsets, and 8 otherwise.";
        Self::new_bool_param("big_other", desc)
    }

    fn new_measurements_param(version: Version) -> Self {
        let meas_desc = "Measurements corresponding to columns in FCS file. \
                         Temporal must be given zero or one times.";
        Self::new_param("measurements", PyType::new_meas(version), meas_desc)
    }

    fn new_set_meas_param(version: Version) -> Self {
        Self::new_param(
            "measurements",
            PyType::new_meas(version),
            "The new measurements. The first member of the tuple corresponds to \
             the measurement name and the second is the measurement object.",
        )
    }

    fn new_allow_shared_names_param() -> Self {
        Self::new_bool_param(
            "allow_shared_names",
            "If ``False``, raise exception if any non-measurement keywords reference \
             any *$PnN* keywords. If ``True`` raise exception if any non-measurement \
             keywords reference a *$PnN* which is not present in ``measurements``. \
             In other words, ``False`` forbids named references to exist, and \
             ``True`` allows named references to be updated. References cannot \
             be broken in either case.",
        )
    }

    // TODO this can be specific to each version, for instance, we can call out
    // the exact keywords in each that may have references.
    fn new_skip_index_check_param() -> Self {
        let desc = "If ``False``, raise exception if any non-measurement keyword \
                    have an index reference to the current measurements. If \
                    ``True`` allow such references to exist as long as they do \
                    not break (which really means that the length of \
                    ``measurements`` is such that existing indices are satisfied).";
        Self::new_bool_param("skip_index_check", desc)
    }

    fn new_index_param(desc: &str) -> Self {
        Self::new_param("index", PyInt::new_meas_index(), desc)
    }

    fn new_col_param() -> Self {
        let path: Path = parse_quote!(fireflow_core::validated::dataframe::AnyFCSColumn);
        Self::new_param(
            "col",
            PyClass::new1("polars.Series").rstype(path),
            "Data for measurement. Must be same length as existing columns.",
        )
    }

    fn new_name_param(short_desc: &str) -> Self {
        let desc = format!("{short_desc} Corresponds to *$PnN*.");
        Self::new_param("name", PyStr::new_shortname(), desc)
    }

    fn new_range_param() -> Self {
        let desc = "Range of measurement. Corresponds to *$PnR*.";
        Self::new_param("range", PyDecimal::new_range(), desc)
    }

    fn new_notrunc_param() -> Self {
        let desc = "If ``False``, raise exception if ``range`` must be \
                    truncated to fit into measurement type.";
        Self::new_bool_param("disallow_trunc", desc)
    }

    fn new_data_param(polars_type: bool) -> Self {
        let desc = "A dataframe encoding the contents of *DATA*. Number of \
                    columns must match number of measurements. May be empty. \
                    Types do not necessarily need to correspond to those in the \
                    data layout but mismatches may result in truncation.";
        Self::new_param("data", PyType::new_dataframe(polars_type), desc)
    }

    fn new_analysis_param(default: bool) -> Self {
        Self::new(
            "analysis",
            PyType::new_analysis(),
            "Contents of the *ANALYSIS* segment.",
            default.then_some(DocDefault::Auto),
            NoMethods,
        )
    }

    fn new_others_param(default: bool) -> Self {
        Self::new(
            "others",
            PyType::new_others(),
            "A list of byte strings encoding the *OTHER* segments.",
            default.then_some(DocDefault::Auto),
            NoMethods,
        )
    }

    fn new_header_config_params() -> (Path, Vec<Self>, Vec<TokenStream2>) {
        let conf = config_path("HeaderConfigInner");
        let ps = vec![
            Self::new_text_correction_param(),
            Self::new_data_correction_param(),
            Self::new_analysis_correction_param(),
            Self::new_other_corrections_param(),
            Self::new_max_other_param(),
            Self::new_other_width_param(),
            Self::new_squish_offsets_param(),
            Self::new_allow_negative_param(),
            Self::new_truncate_offsets_param(),
        ];
        let js = ps.iter().map(IsDocArg::record_into).collect();
        (conf, ps, js)
    }

    fn new_raw_config_params() -> (Path, Vec<Self>, Vec<TokenStream2>) {
        let conf = config_path("ReadHeaderAndTEXTConfig");
        let ps = vec![
            Self::new_version_override(),
            Self::new_supp_text_correction(),
            Self::new_allow_duplicated_supp_text(),
            Self::new_ignore_supp_text(),
            Self::new_use_literal_delims(),
            Self::new_allow_non_ascii_delim(),
            Self::new_allow_missing_final_delim(),
            Self::new_allow_nonunique(),
            Self::new_allow_odd(),
            Self::new_allow_empty(),
            Self::new_allow_delim_at_boundary(),
            Self::new_allow_non_utf8(),
            Self::new_use_latin1(),
            Self::new_allow_non_ascii_keywords(),
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

    fn new_std_config_params(version: Option<Version>) -> (Path, Vec<Self>, Vec<TokenStream2>) {
        let trim_intra_value_whitespace = Self::new_trim_intra_value_whitespace_param();
        let time_meas_pattern = Self::new_time_meas_pattern_param();
        let allow_missing_time = Self::new_allow_missing_time_param();
        let force_time_linear = Self::new_force_time_linear_param();
        let ignore_time_gain = Self::new_ignore_time_gain_param();
        let ignore_time_optical_keys = Self::new_ignore_time_optical_keys_param();
        let parse_indexed_spillover = Self::new_parse_indexed_spillover_param();
        let date_pattern = Self::new_date_pattern_param();
        let time_pattern = Self::new_time_pattern_param(version);
        let allow_pseudostandard = Self::new_allow_pseudostandard_param();
        let allow_unused_standard = Self::new_allow_unused_standard_param();
        let allow_optional_dropping = Self::new_allow_optional_dropping();
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
            allow_optional_dropping,
            disallow_deprecated,
            fix_log_scale_offsets,
            nonstandard_measurement_pattern,
        ]
        .into_iter();

        let ps: Vec<_> = match version {
            Some(Version::FCS2_0) => std_common_args.collect(),
            Some(Version::FCS3_0) => std_common_args.chain([ignore_time_gain]).collect(),
            _ => std_common_args
                .chain([ignore_time_gain, parse_indexed_spillover])
                .collect(),
        };

        let conf = config_path("StdTextReadConfig");
        let js = ps.iter().map(IsDocArg::record_into).collect();
        (conf, ps, js)
    }

    fn new_layout_config_params(version: Option<Version>) -> (Path, Vec<Self>, Vec<TokenStream2>) {
        let integer_widths_from_byteord = Self::new_integer_widths_from_byteord_param();
        let integer_byteord_override = Self::new_integer_byteord_override_param();
        let disallow_range_truncation = Self::new_disallow_range_truncation_param();

        let ps: Vec<_> = match version {
            Some(Version::FCS3_1 | Version::FCS3_2) => once(disallow_range_truncation).collect(),
            _ => [
                integer_widths_from_byteord,
                integer_byteord_override,
                disallow_range_truncation,
            ]
            .into_iter()
            .collect(),
        };

        let conf = config_path("ReadLayoutConfig");
        let js = ps.iter().map(IsDocArg::record_into).collect();
        (conf, ps, js)
    }

    fn new_offsets_config_params(version: Option<Version>) -> (Path, Vec<Self>, Vec<TokenStream2>) {
        let text_data_correction = Self::new_text_data_correction_param();
        let text_analysis_correction = Self::new_text_analysis_correction_param();
        let ignore_text_data_offsets = Self::new_ignore_text_data_offsets_param();
        let ignore_text_analysis_offsets = Self::new_ignore_text_analysis_offsets_param();
        let allow_header_text_offset_mismatch = Self::new_allow_header_text_offset_mismatch_param();
        let allow_missing_required_offsets =
            Self::new_allow_missing_required_offsets_param(version);
        let truncate_text_offsets = Self::new_truncate_text_offsets_param();

        let ps: Vec<_> = match version {
            // none of these apply to 2.0 since there are no offsets in TEXT
            Some(Version::FCS2_0) => vec![],
            _ => vec![
                text_data_correction,
                text_analysis_correction,
                ignore_text_data_offsets,
                ignore_text_analysis_offsets,
                allow_missing_required_offsets,
                allow_header_text_offset_mismatch,
                truncate_text_offsets,
            ],
        };

        let conf = config_path("ReadTEXTOffsetsConfig");
        let js = ps.iter().map(IsDocArg::record_into).collect();
        (conf, ps, js)
    }

    fn new_reader_config_params() -> (Path, Vec<Self>, Vec<TokenStream2>) {
        let allow_uneven_event_width = Self::new_allow_uneven_event_width_param();
        let allow_tot_mismatch = Self::new_allow_tot_mismatch_param();
        let conf = config_path("ReaderConfig");
        let ps = vec![allow_uneven_event_width, allow_tot_mismatch];
        let js = ps.iter().map(IsDocArg::record_into).collect();
        (conf, ps, js)
    }

    fn new_shared_config_params() -> (Path, Vec<Self>, Vec<TokenStream2>) {
        let conf = config_path("SharedConfig");
        let warnings_are_errors = Self::new_warnings_are_errors_param();
        let hide_warnings = Self::new_hide_warnings_param();
        let ps = vec![warnings_are_errors, hide_warnings];
        let js = ps.iter().map(IsDocArg::record_into).collect();
        (conf, ps, js)
    }

    fn new_trim_intra_value_whitespace_param() -> Self {
        Self::new_bool_param(
            "trim_intra_value_whitespace",
            "If ``True``, trim whitespace between delimiters such as ``,`` \
             and ``;`` within keyword value strings.",
        )
    }

    fn new_time_meas_pattern_param() -> Self {
        let path = parse_quote!(fireflow_core::config::TimeMeasNamePattern);
        let pytype = PyOpt::new(PyStr::new_regexp().rstype(path));
        Self::new_param_def(
            "time_meas_pattern",
            pytype,
            "A pattern to match the *$PnN* of the time measurement. \
             If ``None``, do not try to find a time measurement.",
            DocDefault::Str("^(TIME|Time)$".into()),
        )
    }

    fn new_allow_missing_time_param() -> Self {
        Self::new_bool_param(
            "allow_missing_time",
            "If ``True`` allow time measurement to be missing.",
        )
    }

    fn new_force_time_linear_param() -> Self {
        Self::new_bool_param(
            "force_time_linear",
            "If ``True`` force time measurement to be linear independent of *$PnE*.",
        )
    }

    fn new_ignore_time_gain_param() -> Self {
        Self::new_bool_param(
            "ignore_time_gain",
            "If ``True`` ignore the *$PnG* (gain) keyword. This keyword should not \
             be set according to the standard} however, this library will allow \
             gain to be 1.0 since this equates to identity. If gain is not 1.0, \
             this is nonsense and it can be ignored with this flag.",
        )
    }

    fn new_ignore_time_optical_keys_param() -> Self {
        Self::new_param_def(
            "ignore_time_optical_keys",
            PyList::new(
                PyType::new_temporal_optical_key(),
                Some(parse_quote!(TemporalOpticalKeys)),
                None,
            ),
            "Ignore optical keys in temporal measurement. These keys are \
             nonsensical for time measurements but are not explicitly forbidden in \
             the the standard. Provided keys are the string after the \"Pn\" in \
             the \"PnX\" keywords.",
            DocDefault::Auto,
        )
    }

    fn new_parse_indexed_spillover_param() -> Self {
        Self::new_bool_param(
            "parse_indexed_spillover",
            "Parse $SPILLOVER with numeric indices rather than strings \
             (ie names or *$PnN*)",
        )
    }

    fn new_date_pattern_param() -> Self {
        let path = parse_quote!(fireflow_core::validated::datepattern::DatePattern);
        let desc = format!(
            "if %x does not have year, month, and day specifiers \
             as outlined in {CHRONO_REF}"
        );
        let exc = PyException::new_value_error().desc(desc);
        let pytype = PyStr::default().rstype(path).exc(exc);
        Self::new_opt_param(
            "date_pattern",
            pytype,
            "If supplied, will be used as an alternative pattern when parsing \
             *$DATE*. If not supplied, *$DATE* will be parsed according to \
             the standard pattern which is ``%d-%b-%Y``.",
        )
    }

    fn new_time_pattern_param(version: Option<Version>) -> Self {
        const CORE_PAT: &str = "%H:%M:%S";
        const SUB3_0: &str = "%!";
        const SUB3_1: &str = "%@";
        const NAME3_0: &str = "1/60 seconds";
        const NAME3_1: &str = "centiseconds";

        let fmt = |s: &str| format!("``\"{s}\"``");
        let sub3_0 = fmt(SUB3_0);
        let sub3_1 = fmt(SUB3_1);

        // format exception description
        let exc_desc = format!(
            "if %x does not have specifiers for hours, minutes, \
             seconds, and optionally sub-seconds (where {sub3_0} and {sub3_1} \
             correspond to {NAME3_0} and {NAME3_1} respectively) as outlined \
             in {CHRONO_REF}"
        );
        let exc = PyException::new_value_error().desc(exc_desc);

        // format arg description
        let std_pat = match version {
            None => "version-specific".into(),
            Some(Version::FCS2_0) => fmt(CORE_PAT),
            Some(Version::FCS3_0) => fmt(&format!("{CORE_PAT}:{SUB3_0}")),
            _ => fmt(&format!("{CORE_PAT}.{SUB3_1}")),
        };
        let line1 = "If supplied, will be used as an alternative pattern when \
                     parsing *$BTIM* and *$ETIM*.";
        let line2 = format!(
            "The values {sub3_0} or {sub3_1} may be used to \
             match {NAME3_0} or {NAME3_1} respectively."
        );
        let line3 = format!(
            "If not supplied, *$BTIM* and *$ETIM* will be parsed \
             according to the standard pattern which is {std_pat}."
        );
        let arg_desc = [line1.to_owned(), line2, line3].into_iter().join(" ");

        let path = parse_quote!(fireflow_core::validated::timepattern::TimePattern);
        let pytype = PyStr::default().rstype(path).exc(exc);
        Self::new_opt_param("time_pattern", pytype, arg_desc)
    }

    fn new_allow_pseudostandard_param() -> Self {
        Self::new_bool_param(
            "allow_pseudostandard",
            "If ``True`` allow non-standard keywords with a leading *$*. The \
             presence of such keywords often means the version in *HEADER* \
             is incorrect.",
        )
    }

    fn new_allow_unused_standard_param() -> Self {
        Self::new_bool_param(
            "allow_unused_standard",
            "If ``True`` allow unused standard keywords to be present.",
        )
    }

    fn new_allow_optional_dropping() -> Self {
        Self::new_bool_param(
            "allow_optional_dropping",
            "If ``True`` drop optional keys that cause an error and emit \
             warning instead.",
        )
    }

    fn new_disallow_deprecated_param() -> Self {
        Self::new_bool_param(
            "disallow_deprecated",
            "If ``True`` throw error if a deprecated key is encountered.",
        )
    }

    fn new_fix_log_scale_offsets_param() -> Self {
        Self::new_bool_param(
            "fix_log_scale_offsets",
            "If ``True`` fix log-scale *PnE* and keywords which have zero offset \
             (ie ``X,0.0`` where ``X`` is non-zero).",
        )
    }

    fn new_nonstandard_measurement_pattern_param() -> Self {
        let path = parse_quote!(fireflow_core::validated::keys::NonStdMeasPattern);
        // TODO could clean this up with default regexp exception
        let exc = PyException::new_value_error().desc("if %x does not have ``\"%n\"``");
        let pytype = PyStr::default().rstype(path).exc(exc);
        Self::new_opt_param(
            "nonstandard_measurement_pattern",
            pytype,
            format!(
                "Pattern to use when matching nonstandard measurement keys. Must \
                 be a regular expression pattern with ``%n`` which will \
                 represent the measurement index and should not start with *$*. \
                 Otherwise should be a normal regular expression as defined in \
                 {REGEXP_REF}."
            ),
        )
    }

    fn new_integer_widths_from_byteord_param() -> Self {
        Self::new_bool_param(
            "integer_widths_from_byteord",
            "If ``True`` set all *$PnB* to the number of bytes from *$BYTEORD*. \
             Only has an effect for FCS 2.0/3.0 where *$DATATYPE* is ``I``.",
        )
    }

    fn new_integer_byteord_override_param() -> Self {
        let path = parse_quote!(fireflow_core::text::byteord::ByteOrd2_0);
        let exc = PyException::new_value_error().desc(
            "if %x is not a list of integers including all from 1 to ``N`` \
             where ``N`` is the length of the list (up to 8)",
        );
        Self::new_opt_param(
            "integer_byteord_override",
            PyList::new(RsInt::U32, Some(path), Some(exc.into())),
            "Override *$BYTEORD* for integer layouts.",
        )
    }

    fn new_disallow_range_truncation_param() -> Self {
        Self::new_bool_param(
            "disallow_range_truncation",
            "If ``True`` throw error if *$PnR* values need to be truncated \
             to match the number of bytes specified by *$PnB* and *$DATATYPE*.",
        )
    }

    fn new_config_correction_arg(name: &str, what: &str, is_header: bool, id: &str) -> Self {
        let location = if is_header { "HEADER" } else { "TEXT" };
        Self::new_param_def(
            name,
            PyTuple::new_correction(is_header, id),
            format!("Corrections for {what} offsets in *{location}*."),
            DocDefault::Auto,
        )
    }

    fn new_text_correction_param() -> Self {
        Self::new_config_correction_arg("text_correction", "*TEXT*", true, "PrimaryTextSegmentId")
    }

    fn new_data_correction_param() -> Self {
        Self::new_config_correction_arg("data_correction", "*DATA*", true, "DataSegmentId")
    }

    fn new_analysis_correction_param() -> Self {
        Self::new_config_correction_arg(
            "analysis_correction",
            "*ANALYSIS*",
            true,
            "AnalysisSegmentId",
        )
    }

    fn new_other_corrections_param() -> Self {
        Self::new_param_def(
            "other_corrections",
            PyList::new1(PyTuple::new_correction(true, "OtherSegmentId")),
            "Corrections for OTHER offsets if they exist. Each correction will \
             be applied in order. If an offset does not need to be corrected, \
             use ``(0, 0)``. This will not affect the number of OTHER segments \
             that are read; this is controlled by ``max_other``.",
            DocDefault::Auto,
        )
    }

    fn new_max_other_param() -> Self {
        Self::new_opt_param(
            "max_other",
            RsInt::Usize,
            "Maximum number of OTHER segments that can be parsed. \
             ``None`` means limitless.",
        )
    }

    fn new_other_width_param() -> Self {
        let path = parse_quote!(fireflow_core::validated::ascii_range::OtherWidth);
        let pt = PyInt::new_int(RsInt::NonZeroU8).rstype(path);
        Self::new_param_def(
            "other_width",
            pt,
            "Width (in bytes) to use when parsing *OTHER* offsets.",
            DocDefault::Int(8),
        )
    }

    // this only matters for 3.0+ files
    fn new_squish_offsets_param() -> Self {
        Self::new_bool_param(
            "squish_offsets",
            "If ``True`` and a segment's ending offset is zero, treat entire \
             offset as empty. This might happen if the ending offset is longer \
             than 8 digits, in which case it must be written in *TEXT*. If this \
             happens, the standards mandate that both offsets be written to \
             *TEXT* and that the *HEADER* offsets be set to ``0,0``, so only \
             writing one is an error unless this flag is set. This should only \
             happen in FCS 3.0 files and above.",
        )
    }

    fn new_allow_negative_param() -> Self {
        Self::new_bool_param(
            "allow_negative",
            "If true, allow negative values in a HEADER offset. If negative \
             offsets are found, they will be replaced with ``0``. Some files \
             will denote an \"empty\" offset as ``0,-1``, which is logically \
             correct since the last offset points to the last byte, thus ``0,0`` \
             is actually 1 byte long. Unfortunately this is not what the \
             standards say, so specifying ``0,-1`` is an error unless this \
             flag is set.",
        )
    }

    fn new_truncate_offsets_param() -> Self {
        Self::new_bool_param(
            "truncate_offsets",
            "If true, truncate offsets that exceed the end of the file. \
             In some cases the DATA offset (usually) might exceed the end of the \
             file by 1, which is usually a mistake and should be corrected with \
             ``data_correction`` (or analogous for the offending offset). If this \
             is not the case, the file is likely corrupted. This flag will allow \
             such files to be read conveniently if desired.",
        )
    }

    fn new_version_override() -> Self {
        Self::new_opt_param(
            "version_override",
            PyType::new_version(),
            "Override the FCS version as seen in *HEADER*.",
        )
    }

    fn new_supp_text_correction() -> Self {
        Self::new_config_correction_arg(
            "supp_text_correction",
            "Supplemental *TEXT*",
            false,
            "SupplementalTextSegmentId",
        )
    }

    fn new_allow_duplicated_supp_text() -> Self {
        Self::new_bool_param(
            "allow_duplicated_supp_text",
            "If ``True`` allow supplemental *TEXT* offsets to match the primary \
             *TEXT* offsets from *HEADER*. Some vendors will duplicate these \
             two segments despite supplemental *TEXT* not being present, which \
             is incorrect.",
        )
    }

    fn new_ignore_supp_text() -> Self {
        Self::new_bool_param(
            "ignore_supp_text",
            "If ``True``, ignore supplemental *TEXT* entirely.",
        )
    }

    fn new_use_literal_delims() -> Self {
        Self::new_bool_param(
            "use_literal_delims",
            "If ``True``, treat every delimiter as literal (turn off escaping). \
             Without escaping, delimiters cannot be included in keys or values, \
             but empty values become possible. Use this option for files where \
             unescaped delimiters results in the 'correct' interpretation of *TEXT*.",
        )
    }

    fn new_allow_non_ascii_delim() -> Self {
        Self::new_bool_param(
            "allow_non_ascii_delim",
            "If ``True`` allow non-ASCII delimiters (outside 1-126).",
        )
    }

    fn new_allow_missing_final_delim() -> Self {
        Self::new_bool_param(
            "allow_missing_final_delim",
            "If ``True`` allow *TEXT* to not end with a delimiter.",
        )
    }

    fn new_allow_nonunique() -> Self {
        Self::new_bool_param(
            "allow_nonunique",
            "If ``True`` allow non-unique keys in *TEXT*. In such cases, \
             only the first key will be used regardless of this setting; ",
        )
    }

    fn new_allow_odd() -> Self {
        Self::new_bool_param(
            "allow_odd",
            "If ``True``, allow *TEXT* to contain odd number of words. \
             The last 'dangling' word will be dropped independent of this flag.",
        )
    }

    fn new_allow_empty() -> Self {
        Self::new_bool_param(
            "allow_empty",
            "If ``True`` allow keys with blank values. Only relevant if \
             ``use_literal_delims`` is also ``True``.",
        )
    }

    fn new_allow_delim_at_boundary() -> Self {
        Self::new_bool_param(
            "allow_delim_at_boundary",
            "If ``True`` allow delimiters at word boundaries. The FCS standard \
             forbids this because it is impossible to tell if such delimiters \
             belong to the previous or the next word. Consequently, delimiters \
             at boundaries will be dropped regardless of this flag. Setting \
             this to ``True`` will turn this into a warning not an error. Only \
             relevant if ``use_literal_delims`` is ``False``.",
        )
    }

    fn new_allow_non_utf8() -> Self {
        Self::new_bool_param(
            "allow_non_utf8",
            "If ``True`` allow non-UTF8 characters in *TEXT*. Words with such \
             characters will be dropped regardless; setting this to ``True`` \
             will turn these cases into warnings not errors.",
        )
    }

    fn new_use_latin1() -> Self {
        Self::new_bool_param(
            "use_latin1",
            "If ``True`` interpret all characters in *TEXT* as Latin-1 (aka \
             ISO/IEC 8859-1) instead of UTF-8.",
        )
    }

    fn new_allow_non_ascii_keywords() -> Self {
        Self::new_bool_param(
            "allow_non_ascii_keywords",
            "If ``True`` allow non-ASCII keys. This only applies to \
             non-standard keywords, as all standardized keywords may only \
             contain letters, numbers, and start with *$*. Regardless, all \
             compliant keys must only have ASCII. Setting this to true will \
             emit an error when encountering such a key. If false, the key will \
             be kept as a non-standard key.",
        )
    }

    fn new_allow_missing_supp_text() -> Self {
        Self::new_bool_param(
            "allow_missing_supp_text",
            "If ``True`` allow supplemental *TEXT* offsets to be missing from \
             primary *TEXT*.",
        )
    }

    fn new_allow_supp_text_own_delim() -> Self {
        Self::new_bool_param(
            "allow_supp_text_own_delim",
            "If ``True`` allow supplemental *TEXT* offsets to have a different \
             delimiter compared to primary *TEXT*.",
        )
    }

    fn new_allow_missing_nextdata() -> Self {
        Self::new_bool_param(
            "allow_missing_nextdata",
            "If ``True`` allow *$NEXTDATA* to be missing. This is a required \
             keyword in all versions. However, most files only have one dataset \
             in which case this keyword is meaningless.",
        )
    }

    fn new_trim_value_whitespace() -> Self {
        Self::new_bool_param(
            "trim_value_whitespace",
            "If ``True`` trim whitespace from all values. If performed, \
             trimming precedes all other repair steps. Any values which are \
             entirely spaces will become blanks, in which case it may also be \
             sensible to enable ``allow_empty``.",
        )
    }

    fn new_ignore_standard_keys() -> Self {
        Self::new_key_patterns_param(
            "ignore_standard_keys",
            "Remove standard keys from *TEXT*. The leading *$* is implied \
             so do not include it.",
        )
    }

    fn new_promote_to_standard() -> Self {
        Self::new_key_patterns_param(
            "promote_to_standard",
            "Promote nonstandard keys to standard keys in *TEXT*",
        )
    }

    fn new_demote_from_standard() -> Self {
        Self::new_key_patterns_param(
            "demote_from_standard",
            "Demote nonstandard keys from standard keys in *TEXT*",
        )
    }

    fn new_key_patterns_param(argname: &str, desc: &str) -> Self {
        let common = format!(
            "The first member of the tuples is a list of strings which \
             match literally. The second member is a list of regular \
             expressions corresponding to {REGEXP_REF}."
        );
        let d = format!("{desc}. {common}");
        Self::new_param_def(argname, PyType::new_key_patterns(), d, DocDefault::Auto)
    }

    fn new_rename_standard_keys() -> Self {
        Self::new_param_def(
            "rename_standard_keys",
            PyDict::new_keystring_pairs(),
            "Rename standard keys in *TEXT*. Keys matching the first part of \
             the pair will be replaced by the second. Comparisons are case \
             insensitive. The leading *$* is implied so do not include it.",
            DocDefault::Auto,
        )
    }

    fn new_replace_standard_key_values() -> Self {
        Self::new_param_def(
            "replace_standard_key_values",
            PyDict::new1(PyStr::new_keystring(), PyStr::default()),
            "Replace values for standard keys in *TEXT* Comparisons are case \
             insensitive. The leading *$* is implied so do not include it.",
            DocDefault::Auto,
        )
    }

    fn new_substitute_standard_key_values() -> Self {
        Self::new_param_def(
            "substitute_standard_key_values",
            PyTuple::new_sub_patterns(),
            "Apply sed-like substitution operation on matching standard \
             keys. The leading *$* is implied when matching keys. The first \
             dict corresponds to keys which are matched literally, and the \
             second corresponds to keys which are matched via regular \
             expression. The members in the 3-tuple values correspond to a \
             regular expression, replacement string, and global flag \
             respectively. The regular expression may contain capture \
             expressions which must be matched exactly in the replacement \
             string. If the global flag is ``True``, replace all found \
             matches, otherwise only replace the first. Any references in \
             replacement string must be given with surrounding brackets \
             like ``\"${{1}}\"`` or ``\"${{cygnus}}\"``.",
            DocDefault::Auto,
        )
    }

    fn new_append_standard_keywords() -> Self {
        Self::new_param_def(
            "append_standard_keywords",
            PyDict::new1(PyStr::new_keystring(), PyStr::default()),
            "Append standard key/value pairs to *TEXT*. All keys and values \
             will be included as they appear here. The leading *$* is implied so \
             do not include it.",
            DocDefault::Auto,
        )
    }

    fn new_text_data_correction_param() -> Self {
        Self::new_config_correction_arg("text_data_correction", "*DATA*", false, "DataSegmentId")
    }

    fn new_text_analysis_correction_param() -> Self {
        Self::new_config_correction_arg(
            "text_analysis_correction",
            "*ANALYSIS*",
            false,
            "AnalysisSegmentId",
        )
    }

    fn new_ignore_text_data_offsets_param() -> Self {
        Self::new_bool_param(
            "ignore_text_data_offsets",
            "If ``True`` ignore *DATA* offsets in *TEXT*",
        )
    }

    fn new_ignore_text_analysis_offsets_param() -> Self {
        Self::new_bool_param(
            "ignore_text_analysis_offsets",
            "If ``True`` ignore *ANALYSIS* offsets in *TEXT*",
        )
    }

    fn new_allow_header_text_offset_mismatch_param() -> Self {
        Self::new_bool_param(
            "allow_header_text_offset_mismatch",
            "If ``True`` allow *TEXT* and *HEADER* offsets to mismatch.",
        )
    }

    fn new_allow_missing_required_offsets_param(version: Option<Version>) -> Self {
        let s = match version {
            Some(Version::FCS3_2) => "*DATA*",
            Some(_) => "*DATA* and *ANALYSIS*",
            None => "*DATA* and *ANALYSIS* (3.1 or lower)",
        };
        Self::new_bool_param(
            "allow_missing_required_offsets",
            format!(
                "If ``True`` allow required {s} offsets in *TEXT* to be missing. \
                 If missing, fall back to offsets from *HEADER*."
            ),
        )
    }

    fn new_truncate_text_offsets_param() -> Self {
        Self::new_bool_param(
            "truncate_text_offsets",
            "If ``True`` truncate offsets that exceed end of file.",
        )
    }

    fn new_allow_uneven_event_width_param() -> Self {
        Self::new_bool_param(
            "allow_uneven_event_width",
            "If ``True`` allow event width to not perfectly divide length of *DATA*. \
            Does not apply to delimited ASCII layouts. ",
        )
    }

    fn new_allow_tot_mismatch_param() -> Self {
        Self::new_bool_param(
            "allow_tot_mismatch",
            "If ``True`` allow *$TOT* to not match number of events as \
             computed by the event width and length of *DATA*. \
             Does not apply to delimited ASCII layouts.",
        )
    }

    fn new_warnings_are_errors_param() -> Self {
        Self::new_bool_param(
            "warnings_are_errors",
            "If ``True`` all warnings will be regarded as errors.",
        )
    }

    fn new_hide_warnings_param() -> Self {
        Self::new_bool_param("hide_warnings", "If ``True`` hide all warnings.")
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
            (Self::Auto, _) => pytype.defaults(),
            (Self::Int(x), PyType::Int(_)) => (x.to_string(), pytype.defaults().1),
            (Self::Str(x), PyType::Str(_)) => (py_str(x), pytype.defaults().1),
            (dt, PyType::Option(pt)) => match (dt, &pt.inner) {
                (Self::Int(x), PyType::Int(y)) => (x.to_string(), y.defaults().1),
                (Self::Str(x), PyType::Str(y)) => (py_str(x), y.defaults().1),
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

trait IsArgType {
    const TYPENAME: &str;
    const ARGTYPE: &str;

    fn readonly() -> Option<bool>;
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

    fn defaults(&self) -> (String, TokenStream2) {
        match self {
            Self::Bool(x) => x.defaults(),
            Self::Bytes(x) => x.defaults(),
            Self::Str(x) => x.defaults(),
            Self::Int(x) => x.defaults(),
            Self::Float(x) => x.defaults(),
            Self::Decimal(x) => x.defaults(),
            Self::List(x) => x.defaults(),
            Self::Dict(x) => x.defaults(),
            Self::Option(_) => PyOpt::<E>::defaults(),
            Self::Literal(x) => {
                let rt = &x.rstype;
                (format!("\"{}\"", x.head), quote!(#rt::default()))
            }
            Self::Union(x) => {
                let rt = path_strip_args(x.rstype.clone());
                let (pt, _) = x.head0.defaults();
                (pt, quote!(#rt::default()))
            }
            Self::Tuple(xs) => {
                let (ps, rs): (Vec<_>, Vec<_>) = xs.inner.iter().map(Self::defaults).unzip();
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

impl ArgPyType {
    fn new_optical(version: Version) -> Self {
        let n = format!("Optical{}", version.short_underscore());
        PyClass::new_py([""; 0], n).into()
    }

    fn new_temporal(version: Version) -> Self {
        let n = format!("Temporal{}", version.short_underscore());
        PyClass::new_py([""; 0], n).into()
    }

    fn new_measurement(version: Version) -> Self {
        let element_path = element_path(version);
        PyUnion::new2(
            Self::new_optical(version),
            Self::new_temporal(version),
            element_path,
        )
        .into()
    }

    fn new_scale(is_gate: bool) -> Self {
        let path = if is_gate {
            keyword_path("GateScale")
        } else {
            parse_quote!(fireflow_core::text::scale::Scale)
        };
        let exc = PyException::new_value_error()
            .desc("if %x has log scale floats which are not both positive");
        PyUnion::new2(
            PyTuple::default(),
            PyTuple::new1([RsFloat::F32, RsFloat::F32]),
            path,
        )
        .exc(exc)
        .into()
    }

    fn new_transform() -> Self {
        let path = parse_quote! {fireflow_core::core::ScaleTransform};
        let exc = PyException::new_value_error()
            .desc("if %x has log scale floats which are not both positive");
        // TODO the linear gain should also be positive
        PyUnion::new2(
            RsFloat::F32,
            PyTuple::new1([RsFloat::F32, RsFloat::F32]),
            path,
        )
        .exc(exc)
        .into()
    }

    fn new_key_patterns() -> Self {
        let path: Path = parse_quote!(fireflow_core::validated::keys::KeyPatterns);
        PyTuple::new1([
            PyList::new1(PyStr::new_keystring()),
            PyList::new1(PyStr::new_regexp()),
        ])
        .rstype(path)
        .into()
    }

    fn new_std_keywords() -> Self {
        let path = parse_quote!(fireflow_core::validated::keys::StdKey);
        let e = PyException::new_value_error().desc(
            "if %x is empty, does not start with \
             ``\"$\"``, or is only a ``\"$\"``",
        );
        PyDict::new1(PyStr::default().rstype(path).exc(e), PyStr::default()).into()
    }

    fn new_nonstd_keywords() -> Self {
        let path = parse_quote!(fireflow_core::validated::keys::NonStdKey);
        let e = PyException::new_value_error().desc("if %x is empty or starts with ``\"$\"``");
        PyDict::new1(PyStr::default().rstype(path).exc(e), PyStr::default()).into()
    }

    fn new_versioned_shortname(version: Version) -> Self {
        if version < Version::FCS3_1 {
            PyOpt::new(PyStr::new_shortname()).into()
        } else {
            let inner = quote!(fireflow_core::validated::shortname::Shortname);
            let outer = parse_quote!(fireflow_core::text::optional::AlwaysValue<#inner>);
            PyStr::new_shortname().rstype(outer).into()
        }
    }

    fn new_meas_or_gate_index() -> Self {
        let path = parse_quote!(fireflow_core::text::keywords::MeasOrGateIndex);
        let e = PyException::new_value_error().desc(
            "if %x is not like ``P<X>`` or ``G<X>`` \
             where ``X`` is an integer one or greater",
        );
        PyStr::default().rstype(path).exc(e).into()
    }

    fn new_analysis() -> Self {
        let r = parse_quote!(fireflow_core::core::Analysis);
        PyBytes::default().rstype(r).into()
    }

    fn new_others() -> Self {
        let path: Path = parse_quote!(fireflow_core::core::Others);
        PyList::new(PyBytes::default(), path, None).into()
    }

    fn new_dataframe(polars_type: bool) -> Self {
        let path = if polars_type {
            parse_quote!(pyo3_polars::PyDataFrame)
        } else {
            parse_quote!(fireflow_core::validated::dataframe::FCSDataFrame)
        };
        PyClass::new1("polars.DataFrame").rstype(path).into()
    }

    fn new_bitmask(nbytes: usize) -> Self {
        let i = format_ident!("Bitmask{:02}", nbytes * 8);
        let r = match nbytes {
            1 => RsInt::U8,
            2 => RsInt::U16,
            3 | 4 => RsInt::U32,
            5..=8 => RsInt::U64,
            _ => panic!("invalid number of uint bytes: {nbytes}"),
        };
        let e = PyException::new_value_error().desc(r.exc_desc());
        let path = parse_quote!(fireflow_core::validated::bitmask::#i);
        PyInt::from(r).rstype(path).exc(e).into()
    }

    fn new_byteord(nbytes: usize) -> Self {
        let sizedbyteord_path: Path = parse_quote!(fireflow_core::text::byteord::SizedByteOrd);
        let exc = PyException::new_value_error().desc(format!(
            "if %x is not \"little\", \"big\", or a list of \
             all integers from 1 to {nbytes} in any order"
        ));
        let path = parse_quote!(#sizedbyteord_path<#nbytes>);
        PyUnion::new2(Self::new_endian(), PyList::new1(RsInt::U32), path)
            .exc(exc)
            .into()
    }

    fn new_float_range(nbytes: usize) -> Self {
        let i = format_ident!("F{:02}Range", nbytes * 8);
        let r = match nbytes {
            4 => RsFloat::F32,
            8 => RsFloat::F64,
            _ => panic!("invalid number of float bytes: {nbytes}"),
        };
        let msg = format!(
            "if %x is ``NaN``, ``inf``, ``-inf``, \
             or outside the bounds of a {}-bit float",
            nbytes * 8,
        );
        let e = PyException::new_value_error().desc(msg);
        let path = parse_quote!(fireflow_core::data::#i);
        PyFloat::from(r).rstype(path).exc(e).into()
    }

    fn new_timestep() -> Self {
        let path = keyword_path("Timestep");
        PyFloat::new_positive_float().rstype(path).into()
    }

    fn new_non_empty_str(path: Path) -> Self {
        let e = PyException::new_value_error().desc("if %x is empty");
        PyStr::default().rstype(path).exc(e).into()
    }

    fn new_tr() -> Self {
        let path = keyword_path("Trigger");
        PyTuple::new1([Self::from(PyInt::new_u32()), PyStr::new_shortname().into()])
            .rstype(path)
            .into()
    }

    fn new_meas(version: Version) -> Self {
        let (fam_ident, name_pytype) = if version < Version::FCS3_1 {
            (
                format_ident!("MaybeFamily"),
                PyOpt::new(PyStr::new_shortname()).into(),
            )
        } else {
            (format_ident!("AlwaysFamily"), PyStr::new_shortname().into())
        };
        let fam_path = quote!(fireflow_core::text::optional::#fam_ident);
        let meas_opt_pyname = pyoptical(version);
        let meas_tmp_pyname = pytemporal(version);
        let meas_argtype = parse_quote!(PyEithers<#fam_path, #meas_tmp_pyname, #meas_opt_pyname>);
        PyTuple::new1([name_pytype, Self::new_measurement(version)])
            .rstype(meas_argtype)
            .into()
    }

    fn new_unigate() -> Self {
        PyTuple::new1([PyDecimal::default(), PyDecimal::default()])
            .rstype(keyword_path("UniGate"))
            .into()
    }

    fn new_vertices() -> Self {
        let inner_path = keyword_path("Vertex");
        let inner = PyTuple::new1([RsFloat::F32, RsFloat::F32]);
        PyList::new_non_empty(inner, &inner_path).into()
    }

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
                if let Some(y) = ys.next() {
                    let acc = walk(vec![], &y);
                    ys.fold(acc, |a, x| walk(a, &x))
                } else {
                    vec![]
                }
            }
            Self::Literal(_) => vec![],
        }
    }
}

impl RetPyType {
    fn new_coretext(version: Version) -> Self {
        let v = version.short_underscore();
        PyClass::new_py([""; 0], format!("CoreTEXT{v}")).into()
    }

    fn new_coredataset(version: Version) -> Self {
        let v = version.short_underscore();
        PyClass::new_py([""; 0], format!("CoreDataset{v}")).into()
    }

    fn new_anycoretext() -> Self {
        PyUnion::new1(
            ALL_VERSIONS.into_iter().map(Self::new_coretext),
            parse_quote!(PyAnyCoreTEXT),
        )
        .into()
    }

    fn new_anycoredataset() -> Self {
        PyUnion::new1(
            ALL_VERSIONS.into_iter().map(Self::new_coredataset),
            parse_quote!(PyAnyCoreDataset),
        )
        .into()
    }
}

impl<E> PyType<E> {
    fn new_version() -> Self {
        let path = parse_quote!(fireflow_core::header::Version);
        PyLiteral::new2(["FCS2.0", "FCS3.0", "FCS3.1", "FCS3.2"], path).into()
    }

    fn new_temporal_optical_key() -> Self {
        PyLiteral::new2(
            [
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
            ],
            parse_quote!(TemporalOpticalKeys),
        )
        .into()
    }

    fn new_datatype() -> Self {
        let path = parse_quote!(fireflow_core::text::keywords::AlphaNumType);
        PyLiteral::new2(["A", "I", "F", "D"], path).into()
    }

    fn new_feature() -> Self {
        let path = keyword_path("Feature");
        PyLiteral::new2(["Area", "Width", "Height"], path).into()
    }

    fn new_keywords() -> Self {
        PyDict::new1(PyStr::default(), PyStr::default()).into()
    }

    fn new_endian() -> Self {
        let endian: Path = parse_quote!(fireflow_core::text::byteord::Endian);
        PyLiteral::new2(["little", "big"], endian).into()
    }
}

trait AsPyAtom<R> {
    fn as_atom(&self) -> PyAtom<R>;
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

impl ClassDocString {
    fn new_class(
        summary: impl fmt::Display,
        paragraphs: impl IntoIterator<Item = impl fmt::Display>,
        args: impl IntoIterator<Item = impl Into<AnyDocArg>>,
    ) -> Self {
        Self::new(
            summary.to_string(),
            paragraphs.into_iter().map(|x| x.to_string()).collect(),
            args.into_iter().map(Into::into).collect(),
            (),
        )
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
    fn new_method(
        summary: impl fmt::Display,
        paragraphs: impl IntoIterator<Item = impl fmt::Display>,
        args: impl IntoIterator<Item = DocArgParam>,
        returns: Option<DocReturn<RetPyType>>,
    ) -> Self {
        Self::new(
            summary.to_string(),
            paragraphs.into_iter().map(|x| x.to_string()).collect(),
            args.into_iter().collect(),
            returns,
        )
    }
}

impl IvarDocString {
    fn new_ivar(
        summary: impl fmt::Display,
        paragraphs: impl IntoIterator<Item = impl fmt::Display>,
        returns: DocReturn<ArgPyType>,
    ) -> Self {
        Self::new(
            summary.to_string(),
            paragraphs.into_iter().map(|x| x.to_string()).collect(),
            (),
            returns,
        )
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
    fn new_fun<S>(
        summary: impl fmt::Display,
        paragraphs: impl IntoIterator<Item = S>,
        args: impl IntoIterator<Item = DocArgParam>,
        returns: Option<DocReturn<RetPyType>>,
    ) -> Self
    where
        S: fmt::Display,
    {
        Self::new(
            summary.to_string(),
            paragraphs.into_iter().map(|x| x.to_string()).collect(),
            args.into_iter().collect(),
            returns,
        )
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
        let (raw_sig, txt_sig_): (Vec<_>, Vec<_>) = ps
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
            #[pyo3(signature = (#(#raw_sig),*))]
            #[pyo3(text_signature = #txt_sig)]
        }
    }
}

impl<A, R, S> DocString<A, R, S> {
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
        let ns: Vec<_> = self.names.iter().map(|n| format!("``{n}``")).collect();
        let ns_ = fmt_comma_sep_list(&ns[..], "or");
        let n = self.inner.argmod.fmt(&ns_);
        if let Some(d) = self.inner.inner.desc.as_ref() {
            assert!(d.contains("%x"), "does not contain name ref: {d}");
            let dd = d.replace("%x", &n);
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
            .map_or(self.desc.to_string(), |def| {
                format!("{} Defaults to ``{def}``.", self.desc)
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
                f.write_str($s)
            }
        }
    };
}

impl_display_pytype!(PyBool, ":py:class:`bool`");
impl_display_pytype!(PyStr, ":py:class:`str`");
impl_display_pytype!(PyBytes, ":py:class:`bytes`");
impl_display_pytype!(PyInt, ":py:class:`int`");
impl_display_pytype!(PyFloat, ":py:class:`float`");
impl_display_pytype!(PyDecimal, ":py:class:`~decimal.Decimal`");
impl_display_pytype!(PyDate, ":py:class:`~datetime.date`");
impl_display_pytype!(PyTime, ":py:class:`~datetime.time`");
impl_display_pytype!(PyDatetime, ":py:class:`~datetime.datetime`");

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

const MAX_LINE_LEN: usize = 72;

const ALL_VERSIONS: [Version; 4] = [
    Version::FCS2_0,
    Version::FCS3_0,
    Version::FCS3_1,
    Version::FCS3_2,
];

const CHRONO_REF: &str =
    "`chrono <https://docs.rs/chrono/latest/chrono/format/strftime/index.html>`__";

const REGEXP_REF: &str = "`regexp-syntax <https://docs.rs/regex/latest/regex/#syntax>`__";

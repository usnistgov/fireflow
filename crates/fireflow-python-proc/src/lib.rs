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
pub fn def_fcs_read_header(input: TokenStream) -> TokenStream {
    let fun_path = parse_macro_input!(input as Path);
    let args = DocArgParam::new_header_config_params();
    let inner_args: Vec<_> = args.iter().map(|a| a.record_into()).collect();
    let doc = DocString::new_fun(
        "Read the *HEADER* of an FCS file.",
        [""; 0],
        [DocArg::new_path_param(true)].into_iter().chain(args),
        Some(DocReturn::new(PyClass::new_py("Header"))),
    );
    let fun_args = doc.fun_args();
    let conf_inner_path = config_path("HeaderConfigInner");
    let conf_path = config_path("ReadHeaderConfig");
    let ret_path = doc.ret_path();
    quote! {
        #[pyfunction]
        #doc
        #[allow(clippy::too_many_arguments)]
        pub fn fcs_read_header(#fun_args) -> PyResult<#ret_path> {
            let conf = #conf_path(#conf_inner_path { #(#inner_args),* });
            Ok(#fun_path(&path, &conf).py_termfail_resolve_nowarn()?.into())
        }
    }
    .into()
}

#[proc_macro]
pub fn def_fcs_read_raw_text(input: TokenStream) -> TokenStream {
    let fun_path = parse_macro_input!(input as Path);

    let header_conf_path = config_path("HeaderConfigInner");
    let raw_conf_path = config_path("ReadHeaderAndTEXTConfig");
    let shared_conf_path = config_path("SharedConfig");
    let conf_path = config_path("ReadRawTEXTConfig");

    let path_arg = DocArg::new_path_param(true);
    let header_args = DocArgParam::new_header_config_params();
    let raw_args = DocArgParam::new_raw_config_params();
    let shared_args = DocArgParam::new_shared_config_params();

    let header_inner_args: Vec<_> = header_args.iter().map(|a| a.record_into()).collect();
    let raw_inner_args: Vec<_> = raw_args.iter().map(|a| a.record_into()).collect();
    let shared_inner_args: Vec<_> = shared_args.iter().map(|a| a.record_into()).collect();

    let doc = DocString::new_fun(
        "Read *HEADER* and *TEXT* as key/value pairs from FCS file.",
        [""; 0],
        [path_arg]
            .into_iter()
            .chain(header_args)
            .chain(raw_args)
            .chain(shared_args),
        Some(DocReturn::new(PyClass::new_py("RawTEXTOutput"))),
    );

    let fun_args = doc.fun_args();
    let ret_path = doc.ret_path();

    quote! {
        #[pyfunction]
        #doc
        #[allow(clippy::too_many_arguments)]
        pub fn fcs_read_raw_text(#fun_args) -> PyResult<#ret_path> {
            let header = #header_conf_path { #(#header_inner_args),* };
            let raw = #raw_conf_path { header, #(#raw_inner_args),* };
            let shared = #shared_conf_path { #(#shared_inner_args),* };
            let conf = #conf_path { raw, shared };
            Ok(#fun_path(&path, &conf).py_termfail_resolve()?.into())
        }
    }
    .into()
}

#[proc_macro]
pub fn def_fcs_read_std_text(input: TokenStream) -> TokenStream {
    let fun_path = parse_macro_input!(input as Path);

    let header_conf_path = config_path("HeaderConfigInner");
    let raw_conf_path = config_path("ReadHeaderAndTEXTConfig");
    let std_conf_path = config_path("StdTextReadConfig");
    let offsets_conf_path = config_path("ReadTEXTOffsetsConfig");
    let layout_conf_path = config_path("ReadLayoutConfig");
    let shared_conf_path = config_path("SharedConfig");
    let conf_path = config_path("ReadStdTEXTConfig");

    let path_arg = DocArg::new_path_param(true);
    let header_args = DocArgParam::new_header_config_params();
    let raw_args = DocArgParam::new_raw_config_params();
    let std_args = DocArgParam::new_std_config_params(None);
    let offsets_args = DocArgParam::new_offsets_config_params(None);
    let layout_args = DocArgParam::new_layout_config_params(None);
    let shared_args = DocArgParam::new_shared_config_params();

    let header_inner_args: Vec<_> = header_args.iter().map(|a| a.record_into()).collect();
    let raw_inner_args: Vec<_> = raw_args.iter().map(|a| a.record_into()).collect();
    let std_inner_args: Vec<_> = std_args.iter().map(|a| a.record_into()).collect();
    let offsets_inner_args: Vec<_> = offsets_args.iter().map(|a| a.record_into()).collect();
    let layout_inner_args: Vec<_> = layout_args.iter().map(|a| a.record_into()).collect();
    let shared_inner_args: Vec<_> = shared_args.iter().map(|a| a.record_into()).collect();

    let doc = DocString::new_fun(
        "Read *HEADER* and standardized *TEXT* from FCS file.",
        [""; 0],
        [path_arg]
            .into_iter()
            .chain(header_args)
            .chain(raw_args)
            .chain(std_args)
            .chain(offsets_args)
            .chain(layout_args)
            .chain(shared_args),
        Some(DocReturn::new(PyTuple::new([
            PyType::new_anycoretext(),
            PyClass::new_py("StdTEXTOutput").into(),
        ]))),
    );

    let fun_args = doc.fun_args();
    let ret_path = doc.ret_path();

    quote! {
        #[pyfunction]
        #doc
        #[allow(clippy::too_many_arguments)]
        pub fn fcs_read_std_text(#fun_args) -> PyResult<#ret_path> {
            let header = #header_conf_path { #(#header_inner_args),* };
            let raw = #raw_conf_path { header, #(#raw_inner_args),* };
            let standard = #std_conf_path { #(#std_inner_args),* };
            let offsets = #offsets_conf_path { #(#offsets_inner_args),* };
            let layout = #layout_conf_path { #(#layout_inner_args),* };
            let shared = #shared_conf_path { #(#shared_inner_args),* };
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

    let header_conf_path = config_path("HeaderConfigInner");
    let raw_conf_path = config_path("ReadHeaderAndTEXTConfig");
    let layout_conf_path = config_path("ReadLayoutConfig");
    let offsets_conf_path = config_path("ReadTEXTOffsetsConfig");
    let data_conf_path = config_path("ReaderConfig");
    let shared_conf_path = config_path("SharedConfig");
    let conf_path = config_path("ReadRawDatasetConfig");

    let path_arg = DocArg::new_path_param(true);
    let header_args = DocArgParam::new_header_config_params();
    let raw_args = DocArgParam::new_raw_config_params();
    let layout_args = DocArgParam::new_layout_config_params(None);
    let offsets_args = DocArgParam::new_offsets_config_params(None);
    let data_args = DocArgParam::new_reader_config_params();
    let shared_args = DocArgParam::new_shared_config_params();

    let header_inner_args: Vec<_> = header_args.iter().map(|a| a.record_into()).collect();
    let raw_inner_args: Vec<_> = raw_args.iter().map(|a| a.record_into()).collect();
    let layout_inner_args: Vec<_> = layout_args.iter().map(|a| a.record_into()).collect();
    let offsets_inner_args: Vec<_> = offsets_args.iter().map(|a| a.record_into()).collect();
    let data_inner_args: Vec<_> = data_args.iter().map(|a| a.record_into()).collect();
    let shared_inner_args: Vec<_> = shared_args.iter().map(|a| a.record_into()).collect();

    let doc = DocString::new_fun(
        "Read raw dataset from FCS file.",
        [""; 0],
        [path_arg]
            .into_iter()
            .chain(header_args)
            .chain(raw_args)
            .chain(offsets_args)
            .chain(layout_args)
            .chain(data_args)
            .chain(shared_args),
        Some(DocReturn::new(PyClass::new_py("RawDatasetOutput"))),
    );

    let fun_args = doc.fun_args();
    let ret_path = doc.ret_path();

    quote! {
        #[pyfunction]
        #doc
        #[allow(clippy::too_many_arguments)]
        pub fn fcs_read_raw_dataset(#fun_args) -> PyResult<#ret_path> {
            let header = #header_conf_path { #(#header_inner_args),* };
            let raw = #raw_conf_path { header, #(#raw_inner_args),* };
            let layout = #layout_conf_path { #(#layout_inner_args),* };
            let offsets = #offsets_conf_path { #(#offsets_inner_args),* };
            let data = #data_conf_path { #(#data_inner_args),* };
            let shared = #shared_conf_path { #(#shared_inner_args),* };
            let conf = #conf_path { raw, layout, offsets, data, shared };
            Ok(#fun_path(&path, &conf).py_termfail_resolve()?.into())
        }
    }
    .into()
}

#[proc_macro]
pub fn def_fcs_read_std_dataset(input: TokenStream) -> TokenStream {
    let fun_path = parse_macro_input!(input as Path);

    let header_conf_path = config_path("HeaderConfigInner");
    let raw_conf_path = config_path("ReadHeaderAndTEXTConfig");
    let std_conf_path = config_path("StdTextReadConfig");
    let offsets_conf_path = config_path("ReadTEXTOffsetsConfig");
    let layout_conf_path = config_path("ReadLayoutConfig");
    let shared_conf_path = config_path("SharedConfig");
    let data_conf_path = config_path("ReaderConfig");
    let conf_path = config_path("ReadStdDatasetConfig");

    let path_arg = DocArg::new_path_param(true);
    let header_args = DocArgParam::new_header_config_params();
    let raw_args = DocArgParam::new_raw_config_params();
    let std_args = DocArgParam::new_std_config_params(None);
    let offsets_args = DocArgParam::new_offsets_config_params(None);
    let layout_args = DocArgParam::new_layout_config_params(None);
    let data_args = DocArgParam::new_reader_config_params();
    let shared_args = DocArgParam::new_shared_config_params();

    let header_inner_args: Vec<_> = header_args.iter().map(|a| a.record_into()).collect();
    let raw_inner_args: Vec<_> = raw_args.iter().map(|a| a.record_into()).collect();
    let std_inner_args: Vec<_> = std_args.iter().map(|a| a.record_into()).collect();
    let offsets_inner_args: Vec<_> = offsets_args.iter().map(|a| a.record_into()).collect();
    let layout_inner_args: Vec<_> = layout_args.iter().map(|a| a.record_into()).collect();
    let data_inner_args: Vec<_> = data_args.iter().map(|a| a.record_into()).collect();
    let shared_inner_args: Vec<_> = shared_args.iter().map(|a| a.record_into()).collect();

    let doc = DocString::new_fun(
        "Read standardized dataset from FCS file.",
        [""; 0],
        [path_arg]
            .into_iter()
            .chain(header_args)
            .chain(raw_args)
            .chain(std_args)
            .chain(offsets_args)
            .chain(layout_args)
            .chain(data_args)
            .chain(shared_args),
        Some(DocReturn::new(PyTuple::new([
            PyType::new_anycoredataset(),
            PyClass::new_py("StdDatasetOutput").into(),
        ]))),
    );

    let fun_args = doc.fun_args();
    let ret_path = doc.ret_path();

    quote! {
        #[pyfunction]
        #doc
        #[allow(clippy::too_many_arguments)]
        pub fn fcs_read_std_dataset(#fun_args) -> PyResult<#ret_path> {
            let header = #header_conf_path { #(#header_inner_args),* };
            let raw = #raw_conf_path { header, #(#raw_inner_args),* };
            let standard = #std_conf_path { #(#std_inner_args),* };
            let offsets = #offsets_conf_path { #(#offsets_inner_args),* };
            let layout = #layout_conf_path { #(#layout_inner_args),* };
            let data = #data_conf_path { #(#data_inner_args),* };
            let shared = #shared_conf_path { #(#shared_inner_args),* };
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

    let offsets_conf_path = config_path("ReadTEXTOffsetsConfig");
    let layout_conf_path = config_path("ReadLayoutConfig");
    let shared_conf_path = config_path("SharedConfig");
    let data_conf_path = config_path("ReaderConfig");
    let conf_path = config_path("ReadRawDatasetFromKeywordsConfig");

    let path_arg = DocArg::new_path_param(true);
    let version_arg = DocArg::new_version_param();
    let std_arg = DocArg::new_std_keywords_param();
    let data_arg = DocArg::new_data_seg_param(SegmentSrc::Header);
    let analysis_arg = DocArg::new_analysis_seg_param(SegmentSrc::Header, true);
    let other_arg = DocArg::new_other_segs_param(true);

    let offsets_args = DocArgParam::new_offsets_config_params(None);
    let layout_args = DocArgParam::new_layout_config_params(None);
    let data_args = DocArgParam::new_reader_config_params();
    let shared_args = DocArgParam::new_shared_config_params();

    let offsets_inner_args: Vec<_> = offsets_args.iter().map(|a| a.record_into()).collect();
    let layout_inner_args: Vec<_> = layout_args.iter().map(|a| a.record_into()).collect();
    let data_inner_args: Vec<_> = data_args.iter().map(|a| a.record_into()).collect();
    let shared_inner_args: Vec<_> = shared_args.iter().map(|a| a.record_into()).collect();

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
        Some(DocReturn::new(PyClass::new_py("RawDatasetWithKwsOutput"))),
    );

    let fun_args = doc.fun_args();
    let ret_path = doc.ret_path();

    quote! {
        #[pyfunction]
        #doc
        #[allow(clippy::too_many_arguments)]
        pub fn fcs_read_raw_dataset_with_keywords(#fun_args) -> PyResult<#ret_path> {
            let offsets = #offsets_conf_path { #(#offsets_inner_args),* };
            let layout = #layout_conf_path { #(#layout_inner_args),* };
            let data = #data_conf_path { #(#data_inner_args),* };
            let shared = #shared_conf_path { #(#shared_inner_args),* };
            let conf = #conf_path { offsets, layout, data, shared };
            let ret = #fun_path(
                &path, version, &std, data_seg, analysis_seg, other_segs, &conf
            ).py_termfail_resolve()?;
            Ok(ret.into())
        }
    }
    .into()
}

#[proc_macro]
pub fn def_fcs_read_std_dataset_with_keywords(input: TokenStream) -> TokenStream {
    let fun_path = parse_macro_input!(input as Path);

    let std_conf_path = config_path("StdTextReadConfig");
    let offsets_conf_path = config_path("ReadTEXTOffsetsConfig");
    let layout_conf_path = config_path("ReadLayoutConfig");
    let shared_conf_path = config_path("SharedConfig");
    let data_conf_path = config_path("ReaderConfig");
    let conf_path = config_path("ReadStdDatasetFromKeywordsConfig");

    let path_arg = DocArg::new_path_param(true);
    let version_arg = DocArg::new_version_param();
    let std_arg = DocArg::new_std_keywords_param();
    let nonstd_arg = DocArg::new_nonstd_keywords_param();
    let data_arg = DocArg::new_data_seg_param(SegmentSrc::Header);
    let analysis_arg = DocArg::new_analysis_seg_param(SegmentSrc::Header, true);
    let other_arg = DocArg::new_other_segs_param(true);

    let std_args = DocArgParam::new_std_config_params(None);
    let offsets_args = DocArgParam::new_offsets_config_params(None);
    let layout_args = DocArgParam::new_layout_config_params(None);
    let data_args = DocArgParam::new_reader_config_params();
    let shared_args = DocArgParam::new_shared_config_params();

    let std_inner_args: Vec<_> = std_args.iter().map(|a| a.record_into()).collect();
    let offsets_inner_args: Vec<_> = offsets_args.iter().map(|a| a.record_into()).collect();
    let layout_inner_args: Vec<_> = layout_args.iter().map(|a| a.record_into()).collect();
    let data_inner_args: Vec<_> = data_args.iter().map(|a| a.record_into()).collect();
    let shared_inner_args: Vec<_> = shared_args.iter().map(|a| a.record_into()).collect();

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
        Some(DocReturn::new(PyTuple::new([
            PyClass::new_py("AnyCoreDataset"),
            PyClass::new_py("StdDatasetWithKwsOutput"),
        ]))),
    );

    let fun_args = doc.fun_args();
    let ret_path = doc.ret_path();

    quote! {
        #[pyfunction]
        #doc
        #[allow(clippy::too_many_arguments)]
        pub fn fcs_read_std_dataset_with_keywords(#fun_args) -> PyResult<#ret_path> {
            let kws = fireflow_core::validated::keys::ValidKeywords::new(std, nonstd);
            let standard = #std_conf_path { #(#std_inner_args),* };
            let offsets = #offsets_conf_path { #(#offsets_inner_args),* };
            let layout = #layout_conf_path { #(#layout_inner_args),* };
            let data = #data_conf_path { #(#data_inner_args),* };
            let shared = #shared_conf_path { #(#shared_inner_args),* };
            let conf = #conf_path { standard, offsets, layout, data, shared };
            let (core, data) = #fun_path(
                &path, version, kws, data_seg, analysis_seg, other_segs, &conf
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
        PyClass::new_py("HeaderSegments"),
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
    doc.into_impl_class(name.to_string(), path.clone(), new, quote!())
        .1
        .into()
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
    doc.into_impl_class(name.to_string(), path, new, quote!())
        .1
        .into()
}

#[proc_macro]
pub fn impl_py_raw_text_output(input: TokenStream) -> TokenStream {
    let path = parse_macro_input!(input as Path);
    let name = path.segments.last().unwrap().ident.clone();

    let version = DocArgROIvar::new_version_ivar();

    let std =
        DocArg::new_std_keywords_param().into_ro(|_, _| quote!(self.0.keywords.std.clone().into()));

    let nonstd = DocArg::new_nonstd_keywords_param()
        .into_ro(|_, _| quote!(self.0.keywords.nonstd.clone().into()));

    let parse =
        DocArg::new_parse_output_param().into_ro(|_, _| quote!(self.0.parse.clone().into()));

    let args = [version, std, nonstd, parse];

    let doc = DocString::new_class("Parsed *HEADER* and *TEXT*.", [""; 0], args);

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                let kws = fireflow_core::validated::keys::ValidKeywords::new(std, nonstd);
                #path::new(version, kws, parse.into()).into()
            }
        }
    };
    doc.into_impl_class(name.to_string(), path.clone(), new, quote!())
        .1
        .into()
}

#[proc_macro]
pub fn impl_py_raw_dataset_output(input: TokenStream) -> TokenStream {
    let path = parse_macro_input!(input as Path);
    let name = path.segments.last().unwrap().ident.clone();

    let version = DocArg::new_version_param().into_ro(|_, _| quote!(self.0.text.version));

    let data = DocArg::new_data_param(false).into_ro(|_, _| quote!(self.0.dataset.data.clone()));
    let analysis =
        DocArg::new_analysis_param(false).into_ro(|_, _| quote!(self.0.dataset.analysis.clone()));
    let others =
        DocArg::new_others_param(false).into_ro(|_, _| quote!(self.0.dataset.others.clone()));

    let data_seg =
        DocArg::new_data_seg_param(SegmentSrc::Any).into_ro(|_, _| quote!(self.0.dataset.data_seg));
    let analysis_seg = DocArg::new_analysis_seg_param(SegmentSrc::Any, false)
        .into_ro(|_, _| quote!(self.0.dataset.analysis_seg));

    let std = DocArg::new_std_keywords_param()
        .into_ro(|_, _| quote!(self.0.text.keywords.std.clone().into()));

    let nonstd = DocArg::new_nonstd_keywords_param()
        .into_ro(|_, _| quote!(self.0.text.keywords.nonstd.clone().into()));

    let parse =
        DocArg::new_parse_output_param().into_ro(|_, _| quote!(self.0.text.parse.clone().into()));

    let args = [
        version,
        data,
        analysis,
        others,
        data_seg,
        analysis_seg,
        std,
        nonstd,
        parse,
    ];

    let doc = DocString::new_class("Parsed *HEADER* and *TEXT*.", [""; 0], args);

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                let kws = fireflow_core::validated::keys::ValidKeywords::new(std, nonstd);
                let text = fireflow_core::api::RawTEXTOutput::new(version, kws, parse.into());
                let dataset = fireflow_core::api::RawDatasetWithKwsOutput::new(
                    data, analysis, others, data_seg, analysis_seg
                );
                #path::new(text, dataset).into()
            }
        }
    };
    doc.into_impl_class(name.to_string(), path.clone(), new, quote!())
        .1
        .into()
}

#[proc_macro]
pub fn impl_py_raw_dataset_with_kws_output(input: TokenStream) -> TokenStream {
    let path = parse_macro_input!(input as Path);
    let name = path.segments.last().unwrap().ident.clone();

    let data = DocArg::new_data_param(false).into_ro(|_, _| quote!(self.0.data.clone()));
    let analysis =
        DocArg::new_analysis_param(false).into_ro(|_, _| quote!(self.0.analysis.clone()));
    let others = DocArg::new_others_param(false).into_ro(|_, _| quote!(self.0.others.clone()));
    let data_seg =
        DocArg::new_data_seg_param(SegmentSrc::Any).into_ro(|_, _| quote!(self.0.data_seg));
    let analysis_seg = DocArg::new_analysis_seg_param(SegmentSrc::Any, false)
        .into_ro(|_, _| quote!(self.0.analysis_seg));

    let args = [data, analysis, others, data_seg, analysis_seg];

    let doc = DocString::new_class("Dataset from parsing raw *TEXT*.", [""; 0], args);

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #path::new(data, analysis, others, data_seg, analysis_seg).into()
            }
        }
    };
    doc.into_impl_class(name.to_string(), path.clone(), new, quote!())
        .1
        .into()
}

#[proc_macro]
pub fn impl_py_std_text_output(input: TokenStream) -> TokenStream {
    let path = parse_macro_input!(input as Path);
    let name = path.segments.last().unwrap().ident.clone();

    let tot = DocArgROIvar::new_ivar_ro(
        "tot",
        PyOpt::new(PyInt::new(RsInt::Usize, keyword_path("Tot"))),
        "Value of *$TOT* from *TEXT*.",
        |_, _| quote!(self.0.tot.as_ref().copied()),
    );

    let data = DocArg::new_data_seg_param(SegmentSrc::Any).into_ro(|_, _| quote!(self.0.data));
    let analysis = DocArg::new_analysis_seg_param(SegmentSrc::Any, false)
        .into_ro(|_, _| quote!(self.0.analysis));

    let pseudostandard = DocArgROIvar::new_ivar_ro(
        "pseudostandard",
        PyType::new_std_keywords(),
        "Keywords which start with *$* but are not part of the standard.",
        |_, _| quote!(self.0.extra.pseudostandard.clone()),
    );

    let unused = DocArgROIvar::new_ivar_ro(
        "unused",
        PyType::new_std_keywords(),
        "Keywords which are part of the standard but were not used.",
        |_, _| quote!(self.0.extra.unused.clone()),
    );

    let parse = DocArgROIvar::new_ivar_ro(
        "parse",
        PyClass::new_py("RawTEXTParseData"),
        "Miscellaneous data when parsing *TEXT*.",
        |_, _| quote!(self.0.parse.clone().into()),
    );

    let args = [tot, data, analysis, pseudostandard, unused, parse];

    let doc = DocString::new_class(
        "Miscellaneous data when standardizing *TEXT*.",
        [""; 0],
        args,
    );

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                let extra = fireflow_core::text::parser::ExtraStdKeywords::new(pseudostandard, unused);
                #path::new(tot, data_seg, analysis_seg, extra, parse.into()).into()
            }
        }
    };
    doc.into_impl_class(name.to_string(), path.clone(), new, quote!())
        .1
        .into()
}

#[proc_macro]
pub fn impl_py_std_dataset_output(input: TokenStream) -> TokenStream {
    let path = parse_macro_input!(input as Path);
    let name = path.segments.last().unwrap().ident.clone();

    let data = DocArg::new_data_seg_param(SegmentSrc::Any)
        .into_ro(|_, _| quote!(self.0.dataset.standardized.data_seg));

    let analysis = DocArg::new_analysis_seg_param(SegmentSrc::Any, false)
        .into_ro(|_, _| quote!(self.0.dataset.standardized.analysis_seg));

    let pseudostandard = DocArgROIvar::new_ivar_ro(
        "pseudostandard",
        PyType::new_std_keywords(),
        "Keywords which start with *$* but are not part of the standard.",
        |_, _| quote!(self.0.dataset.extra.pseudostandard.clone()),
    );

    let unused = DocArgROIvar::new_ivar_ro(
        "unused",
        PyType::new_std_keywords(),
        "Keywords which are part of the standard but were not used.",
        |_, _| quote!(self.0.dataset.extra.unused.clone()),
    );

    let parse = DocArgROIvar::new_ivar_ro(
        "parse",
        PyClass::new_py("RawTEXTParseData"),
        "Miscellaneous data when parsing *TEXT*.",
        |_, _| quote!(self.0.parse.clone().into()),
    );

    let args = [data, analysis, pseudostandard, unused, parse];

    let doc = DocString::new_class(
        "Miscellaneous data when standardizing *TEXT*.",
        [""; 0],
        args,
    );

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                let segs = fireflow_core::core::DatasetSegments::new(data_seg, analysis_seg);
                let extra = fireflow_core::text::parser::ExtraStdKeywords::new(pseudostandard, unused);
                let std = fireflow_core::core::StdDatasetWithKwsOutput::new(segs, extra);
                #path::new(std, parse.into()).into()
            }
        }
    };
    doc.into_impl_class(name.to_string(), path.clone(), new, quote!())
        .1
        .into()
}

#[proc_macro]
pub fn impl_py_std_dataset_with_kws_output(input: TokenStream) -> TokenStream {
    let path = parse_macro_input!(input as Path);
    let name = path.segments.last().unwrap().ident.clone();

    let data = DocArg::new_data_seg_param(SegmentSrc::Any)
        .into_ro(|_, _| quote!(self.0.standardized.data_seg));

    let analysis = DocArg::new_analysis_seg_param(SegmentSrc::Any, false)
        .into_ro(|_, _| quote!(self.0.standardized.analysis_seg));

    let pseudostandard = DocArgROIvar::new_ivar_ro(
        "pseudostandard",
        PyType::new_std_keywords(),
        "Keywords which start with *$* but are not part of the standard.",
        |_, _| quote!(self.0.extra.pseudostandard.clone()),
    );

    let unused = DocArgROIvar::new_ivar_ro(
        "unused",
        PyType::new_std_keywords(),
        "Keywords which are part of the standard but were not used.",
        |_, _| quote!(self.0.extra.unused.clone()),
    );

    let args = [data, analysis, pseudostandard, unused];

    let doc = DocString::new_class(
        "Miscellaneous data when standardizing *TEXT* from keywords.",
        [""; 0],
        args,
    );

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                let segs = fireflow_core::core::DatasetSegments::new(data_seg, analysis_seg);
                let extra = fireflow_core::text::parser::ExtraStdKeywords::new(pseudostandard, unused);
                #path::new(segs, extra).into()
            }
        }
    };
    doc.into_impl_class(name.to_string(), path.clone(), new, quote!())
        .1
        .into()
}

#[proc_macro]
pub fn impl_py_raw_text_parse_data(input: TokenStream) -> TokenStream {
    let path = parse_macro_input!(input as Path);
    let name = path.segments.last().unwrap().ident.clone();

    let segments = DocArgROIvar::new_ivar_ro(
        "header_segments",
        PyClass::new_py("HeaderSegments"),
        "Segments from *HEADER*.",
        |_, _| quote!(self.0.header_segments.clone().into()),
    );

    let supp = DocArgROIvar::new_ivar_ro(
        "supp_text",
        PyOpt::new(PyType::new_supp_text_segment()),
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
        PyList::new(PyTuple::new([PyStr::new(), PyStr::new()])),
        "Keywords with a non-ASCII but still valid UTF-8 key.",
        |_, _| quote!(self.0.non_ascii.clone()),
    );

    let byte_pairs = DocArgROIvar::new_ivar_ro(
        "byte_pairs",
        PyList::new(PyTuple::new([PyBytes::new(), PyBytes::new()])),
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
    doc.into_impl_class(name.to_string(), path.clone(), new, quote!())
        .1
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

    let meas = DocArg::new_measurements_param(version).into();
    let layout = DocArg::new_layout_ivar(version).into();
    let data = DocArg::new_df_ivar().into();
    let analysis = DocArg::new_analysis_ivar().into();
    let others = DocArg::new_others_ivar().into();

    let mode = if version < Version::FCS3_2 {
        let t = |p| PyLiteral::new1(["L", "U", "C"], p);
        DocArg::new_kw_ivar("Mode", "mode", t, None, true)
    } else {
        DocArg::new_kw_opt_ivar("Mode3_2", "mode", |p| PyLiteral::new1(["L"], p))
    };

    let cyt = if version < Version::FCS3_2 {
        DocArg::new_kw_opt_ivar("Cyt", "cyt", PyStr::new1)
    } else {
        DocArg::new_kw_ivar("Cyt", "cyt", PyStr::new1, None, false)
    };

    let to_pyint = |p| PyInt::new(RsInt::U32, p);

    let abrt = DocArg::new_kw_opt_ivar("Abrt", "abrt", to_pyint);
    let com = DocArg::new_kw_opt_ivar("Com", "com", PyStr::new1);
    let cells = DocArg::new_kw_opt_ivar("Cells", "cells", PyStr::new1);
    let exp = DocArg::new_kw_opt_ivar("Exp", "exp", PyStr::new1);
    let fil = DocArg::new_kw_opt_ivar("Fil", "fil", PyStr::new1);
    let inst = DocArg::new_kw_opt_ivar("Inst", "inst", PyStr::new1);
    let lost = DocArg::new_kw_opt_ivar("Lost", "lost", to_pyint);
    let op = DocArg::new_kw_opt_ivar("Op", "op", PyStr::new1);
    let proj = DocArg::new_kw_opt_ivar("Proj", "proj", PyStr::new1);
    let smno = DocArg::new_kw_opt_ivar("Smno", "smno", PyStr::new1);
    let src = DocArg::new_kw_opt_ivar("Src", "src", PyStr::new1);
    let sys = DocArg::new_kw_opt_ivar("Sys", "sys", PyStr::new1);
    let cytsn = DocArg::new_kw_opt_ivar("Cytsn", "cytsn", PyStr::new1);

    let unicode_pytype = |p| {
        PyTuple::new1(
            [PyType::from(RsInt::U32), PyList::new(PyStr::new()).into()],
            p,
        )
    };
    let unicode = DocArg::new_kw_opt_ivar("Unicode", "unicode", unicode_pytype);

    let csvbits = DocArg::new_kw_opt_ivar("CSVBits", "csvbits", to_pyint);
    let cstot = DocArg::new_kw_opt_ivar("CSTot", "cstot", to_pyint);

    let csvflags = DocArg::new_csvflags_ivar();

    let all_subset = [csvbits, cstot, csvflags];

    let last_modifier = DocArg::new_kw_opt_ivar("LastModifier", "last_modifier", PyDatetime::new1);
    let last_modified = DocArg::new_kw_opt_ivar("LastModified", "last_modified", PyStr::new1);
    let originality = DocArg::new_kw_opt_ivar("Originality", "originality", |p| {
        PyLiteral::new1(
            ["Original", "NonDataModified", "Appended", "DataModified"],
            p,
        )
    });

    let all_modified = [last_modifier, last_modified, originality];

    let plateid = DocArg::new_kw_opt_ivar("Plateid", "plateid", PyStr::new1);
    let platename = DocArg::new_kw_opt_ivar("Platename", "platename", PyStr::new1);
    let wellid = DocArg::new_kw_opt_ivar("Wellid", "wellid", PyStr::new1);

    let all_plate = [plateid, platename, wellid];

    let vol = DocArg::new_kw_opt_ivar("Vol", "vol", |p| PyFloat::new(RsFloat::F32, p));

    let comp_or_spill = match version {
        Version::FCS2_0 => DocArg::new_comp_ivar(true),
        Version::FCS3_0 => DocArg::new_comp_ivar(false),
        _ => DocArg::new_spillover_ivar(),
    };

    let flowrate = DocArg::new_kw_opt_ivar("Flowrate", "flowrate", PyStr::new1);

    let carrierid = DocArg::new_kw_opt_ivar("Carrierid", "carrierid", PyStr::new1);
    let carriertype = DocArg::new_kw_opt_ivar("Carriertype", "carriertype", PyStr::new1);
    let locationid = DocArg::new_kw_opt_ivar("Locationid", "locationid", PyStr::new1);

    let all_carrier = [carrierid, carriertype, locationid];

    let unstainedcenters = DocArg::new_unstainedcenters_ivar();
    let unstainedinfo = DocArg::new_kw_opt_ivar("UnstainedInfo", "unstainedinfo", PyStr::new1);

    let tr = DocArg::new_trigger_ivar();

    let all_timestamps = match version {
        Version::FCS2_0 => DocArg::new_timestamps_ivar(),
        Version::FCS3_0 => DocArg::new_timestamps_ivar(),
        Version::FCS3_1 | Version::FCS3_2 => DocArg::new_timestamps_ivar(),
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
        .chain([data])
        .chain(all_kws)
        .chain([analysis, others])
        .collect();

    let coretext_inner_args: Vec<_> = coretext_args.iter().map(|x| x.ident_into()).collect();

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

    let (_, coretext_q) = coretext_doc.into_impl_class(
        coretext_name.to_string(),
        coretext_rstype,
        coretext_new,
        quote!(),
    );

    let (_, coredataset_q) = coredataset_doc.into_impl_class(
        coredataset_name.to_string(),
        coredataset_rstype,
        coredataset_new,
        quote!(),
    );

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
        DocReturn::new(PyList::new(PyType::new_nonstd_keywords())),
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
        Some(DocReturn::new1(
            PyType::new_keywords(),
            "A list of standard keywords.",
        )),
    );

    let fun_args = doc.fun_args();
    let inner_args = doc.idents_into();
    let ret_path = doc.ret_path();

    quote! {
        #[pymethods]
        impl #t {
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
        Some(DocReturn::new1(
            PyBool::new(),
            "``True`` if trigger is set and was updated.",
        )),
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

    let write_2_0_warning = if version == Version::FCS2_0 {
        Some("Will raise exception if file cannot fit within 99,999,999 bytes.")
    } else {
        None
    };

    let doc = DocString::new_method(
        "Write data to path.",
        ["Resulting FCS file will include *HEADER* and *TEXT*."]
            .into_iter()
            .chain(write_2_0_warning),
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

    let write_2_0_warning = if version == Version::FCS2_0 {
        Some("Will raise exception if file cannot fit within 99,999,999 bytes.")
    } else {
        None
    };

    let doc = DocString::new_method(
        "Write data as an FCS file.",
        ["The resulting file will include *HEADER*, *TEXT*, *DATA*, \
            *ANALYSIS*, and *OTHER* as they present from this class."]
        .into_iter()
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
        let pt = PyOpt::new(PyInt::new(RsInt::U32, p));
        let inner = pt.as_rust_type();
        let doc = DocString::new_ivar(
            format!("The value of *$P{k}n* for all measurements."),
            [""; 0],
            DocReturn::new(PyList::new(pt)),
        );

        doc.into_impl_get_set(
            &i,
            format!("all_{name}"),
            true,
            |_, _| {
                quote! {
                    self.0
                        .get_temporal_optical::<#inner>()
                        .map(|x| x.as_ref().copied())
                        .collect()
                }
            },
            |n, _| quote!(Ok(self.0.set_temporal_optical(#n)?)),
        )
    };

    let pkn = go("K", "PeakBin", "peak_bins");
    let pknn = go("KN", "PeakNumber", "peak_sizes");

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
        DocReturn::new(PyList::new(PyType::new_shortname())),
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
        DocReturn::new(PyList::new(PyOpt::new(PyType::new_shortname()))),
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
        Some(DocReturn::new1(t, "Previous *$TIMESTEP* if present.")),
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
    let i: Ident = syn::parse(input).unwrap();
    let version = split_ident_version_pycore(&i).1;

    let make_doc = |has_timestep: bool, has_index: bool| {
        let name = DocArg::new_name_param("Name to set to temporal.");
        let index = DocArg::new_param("index", PyType::new_meas_index(), "Index to set");
        let (i, p) = if has_index {
            ("index", index)
        } else {
            ("name", name)
        };
        let timestep = if has_timestep {
            Some(DocArg::new_param(
                "timestep",
                PyType::new_timestep(),
                "The value of *$TIMESTEP* to use.",
            ))
        } else {
            None
        };
        let force = DocArg::new_bool_param(
            "force",
            "If ``True`` remove any optical-specific metadata (detectors, \
             lasers, etc) without raising an exception. Defauls to ``False``.",
        );
        DocString::new_method(
            format!("Set the temporal measurement to a given {i}."),
            [""; 0],
            [p].into_iter().chain(timestep).chain([force]),
            Some(DocReturn::new1(
                PyBool::new(),
                format!(
                    "``True`` if temporal measurement was set, which will \
                     happen for all cases except when the time measurement is \
                     already set to ``{i}``."
                ),
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

    let make_doc = |has_timestep: bool, has_force: bool| {
        let s = "Convert the temporal measurement to an optical measurement.";
        let p = if has_force {
            Some(DocArg::new_bool_param(
                "force",
                "If ``True`` and current time measurement has data which cannot \
                 be converted to optical, force the conversion anyways. \
                 Otherwise raise an exception.",
            ))
        } else {
            None
        }
        .into_iter();
        let (rt, rd) = if has_timestep {
            (
                PyOpt::new(PyType::new_timestep()).into(),
                "Value of *$TIMESTEP* if time measurement was present.",
            )
        } else {
            (
                PyType::from(PyBool::new()),
                "``True`` if temporal measurement was present and converted, \
                 ``False`` if there was not a temporal measurement.",
            )
        };
        DocString::new_method(s, [""; 0], p, Some(DocReturn::new1(rt, rd)))
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
            fn unset_temporal(&mut self, force: bool) -> PyResult<#ret> {
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

    let doc = DocString::new_method(
        "Rename temporal measurement if present.",
        [""; 0],
        [DocArg::new_name_param("New name to assign.")],
        Some(DocReturn::new1(
            PyOpt::new(PyType::new_shortname()),
            "Previous name if present.",
        )),
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
            DocReturn::new(PyList::new(PyOpt::new(PyType::new_scale(false)))),
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
            DocReturn::new(PyList::new(PyType::new_transform())),
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

    let named_vec_path = quote!(fireflow_core::text::named_vec::NamedVec);

    let doc = DocString::new_ivar(
        "All measurements.",
        [""; 0],
        DocReturn::new(PyList::new(PyType::new_measurement(version))),
    );

    doc.into_impl_get(&i, "measurements", |_, _| {
        quote! {
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
        DocReturn::new1(
            PyOpt::new(PyTuple::new([
                PyType::new_meas_index(),
                PyType::new_shortname(),
                PyType::new_temporal(version),
            ])),
            "Index, name, and measurement or ``None``.",
        ),
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

    let named_vec_path = quote!(fireflow_core::text::named_vec::NamedVec);

    let doc = DocString::new_method(
        "Return measurement at index.",
        ["Raise exception if ``index`` not found."],
        [DocArg::new_index_param("Index to retrieve.")],
        Some(DocReturn::new(PyType::new_measurement(version))),
    );

    let fun_args = doc.fun_args();
    let ret = doc.ret_path();

    quote! {
        #[pymethods]
        impl #i {
            // TODO this should return name as well
            #doc
            fn measurement_at(&self, #fun_args) -> PyResult<#ret> {
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
        let col_param = if hasdata {
            Some(DocArg::new_col_param())
        } else {
            None
        };
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
        Some(DocReturn::new1(
            PyTuple::new([PyType::new_meas_index(), PyType::new_measurement(version)]),
            "Index and measurement object.",
        )),
    );

    let by_index_doc = DocString::new_method(
        "Remove a measurement with a given index.",
        ["Raise exception if ``index`` not found."],
        [DocArg::new_index_param("Index to remove")],
        Some(DocReturn::new1(
            PyTuple::new([
                PyType::new_versioned_shortname(version),
                PyType::new_measurement(version),
            ]),
            "Name and measurement object.",
        )),
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
        let (meas_type, what) = if is_optical {
            (PyType::new_optical(version), "optical")
        } else {
            (PyType::new_temporal(version), "temporal")
        };
        let param_meas = DocArg::new_param("meas", meas_type.clone(), "The measurement to insert.");
        let col_param = if hasdata {
            Some(DocArg::new_col_param())
        } else {
            None
        };
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
    let i: Ident = syn::parse(input).unwrap();
    let version = split_ident_version_pycore(&i).1;

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
            Some(DocReturn::new1(ret, "Replaced measurement object.")),
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
        impl #i {
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
    let i: Ident = syn::parse(input).unwrap();
    let version = split_ident_version_pycore(&i).1;

    let force_param = DocArg::new_bool_param(
        "force",
        "If ``True``, do not raise exception if existing temporal measurement \
         cannot be converted to optical measurement.",
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
            DocArg::new_param("meas", PyType::new_temporal(version), meas_desc),
        ];
        let ret = PyOpt::wrap_if(PyType::new_measurement(version), !is_index);
        DocString::new_method(
            format!("Replace {m} with given temporal measurement."),
            [sub],
            args.into_iter().chain(force.clone()),
            Some(DocReturn::new1(ret, "Replaced measurement object.")),
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
        impl #i {
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

    let std_args = DocArg::new_std_config_params(Some(version));
    let layout_args = DocArg::new_layout_config_params(Some(version));
    let shared_args = DocArg::new_shared_config_params();

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
        "std",
        PyType::new_std_keywords(),
        format!("Standard keywords. {no_kws}"),
    );

    let nonstd_param = DocArg::new_param(
        "nonstd",
        PyType::new_nonstd_keywords(),
        "Non-Standard keywords.",
    );

    let config_params = std_args
        .iter()
        .chain(layout_args.iter())
        .chain(shared_args.iter())
        .cloned();

    let doc = DocString::new_fun(
        "Make new instance from keywords.",
        [""; 0],
        [std_param, nonstd_param].into_iter().chain(config_params),
        Some(DocReturn::new(PyClass::new_py(ident.to_string()))),
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

    let std_args = DocArg::new_std_config_params(Some(version));
    let layout_args = DocArg::new_layout_config_params(Some(version));
    let offsets_args = DocArg::new_offsets_config_params(Some(version));
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
        Some(DocReturn::new(PyClass::new_py(ident.to_string()))),
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
    let to_name = format!("CoreDataset{}", version.short_underscore());
    let to_rstype = pycoredataset(version);

    let data = DocArg::new_data_param(false);
    let analysis = DocArg::new_analysis_param(true);
    let others = DocArg::new_others_param(true);

    let doc = DocString::new_method(
        "Convert to a dataset object.",
        ["This will fully represent an FCS file, as opposed to just \
          representing *HEADER* and *TEXT*."],
        [data, analysis, others],
        Some(DocReturn::new(PyClass::new_py(to_name))),
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

    let to_pyfloat = |p| PyFloat::new(RsFloat::F32, p);
    let to_pyuint = |p| PyInt::new(RsInt::U32, p);

    let wavelength = if version < Version::FCS3_1 {
        DocArg::new_meas_kw_opt_ivar("Wavelength", "wavelength", "L", to_pyfloat)
    } else {
        DocArg::new_meas_kw_opt_ivar("Wavelengths", "wavelengths", "L", to_pyfloat)
    };

    let bin = DocArg::new_meas_kw_ivar(
        "PeakBin",
        "bin",
        |p| PyOpt::new(to_pyuint(p)),
        "Value of *$PKn*.".into(),
        true,
    );
    let size = DocArg::new_meas_kw_ivar(
        "PeakNumber",
        "size",
        |p| PyOpt::new(to_pyuint(p)),
        "Value of *$PKNn*.".into(),
        true,
    );

    let all_peak = [bin, size];

    let filter = DocArg::new_meas_kw_opt_ivar("Filter", "filter", "F", PyStr::new1);

    let power = DocArg::new_meas_kw_opt_ivar("Power", "power", "O", to_pyuint);

    let detector_type =
        DocArg::new_meas_kw_opt_ivar("DetectorType", "detector_type", "T", PyStr::new1);

    let percent_emitted =
        DocArg::new_meas_kw_opt_ivar("PercentEmitted", "percent_emitted", "P", PyStr::new1);

    let detector_voltage =
        DocArg::new_meas_kw_opt_ivar("DetectorVoltage", "detector_voltage", "V", to_pyfloat);

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
        |_| PyOpt::new(PyType::new_calibration3_1()),
        Some("Value of *$PnCALIBRATION*. Tuple encodes slope and calibration units."),
        true,
    );

    let calibration3_2 = DocArg::new_meas_kw_ivar(
        "Calibration3_2",
        "calibration",
        |_| PyOpt::new(PyType::new_calibration3_2()),
        Some(
            "Value of *$PnCALIBRATION*. Tuple encodes slope, intercept, \
             and calibration units.",
        ),
        true,
    );

    let display = DocArg::new_meas_kw_ivar(
        "Display",
        "display",
        |_| PyOpt::new(PyType::new_display()),
        Some(
            "Value of *$PnD*. First member of tuple encodes linear or log display \
             (``False`` and ``True`` respectively). The float members encode \
             lower/upper and decades/offset for linear and log scaling respectively.",
        ),
        true,
    );

    let analyte = DocArg::new_meas_kw_opt_ivar("Analyte", "analyte", "ANALYTE", PyStr::new1);

    let feature =
        DocArg::new_meas_kw_opt_ivar("Feature", "feature", "FEATURE", |_| PyType::new_feature());

    let detector_name =
        DocArg::new_meas_kw_opt_ivar("DetectorName", "detector_name", "DET", PyStr::new1);

    let tag = DocArg::new_meas_kw_opt_ivar("Tag", "tag", "TAG", PyStr::new1);

    let measurement_type =
        DocArg::new_meas_kw_opt_ivar("OpticalType", "measurement_type", "TYPE", PyStr::new1);

    let has_scale = DocArg::new_bool_param("has_scale", "``True`` if *$PnE* is set to ``0,0``.")
        .into_rw(
            false,
            |_, _| quote!(self.0.specific.scale.0.is_some()),
            |n, _| {
                quote! {
                    self.0.specific.scale = if #n {
                        Some(fireflow_core::text::keywords::TemporalScale)
                    } else {
                        None
                    }.into();
                }
            },
        );

    let has_type =
        DocArg::new_bool_param("has_type", "``True`` if *$PnTYPE* is set to ``\"Time\"``.")
            .into_rw(
                false,
                |_, _| quote!(self.0.specific.measurement_type.0.is_some()),
                |n, _| {
                    quote! {
                        self.0.specific.measurement_type = if #n {
                            Some(fireflow_core::text::keywords::TemporalType)
                        } else {
                            None
                        }.into();
                    }
                },
            );

    let timestep = DocArg::new_ivar_rw(
        "timestep",
        PyType::new_timestep(),
        "Value of *$TIMESTEP*.",
        false,
        |_, _| quote!(self.0.specific.timestep),
        |_, _| quote!(self.0.specific.timestep = timestep),
    );

    let longname = DocArg::new_meas_kw_opt_ivar("Longname", "longname", "S", PyStr::new1);

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

    doc.into_impl_class(name.to_string(), path, new_method, quote!())
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
    core_all_meas_attr(&i, "Longname", "longnames", "S", PyStr::new1)
}

#[proc_macro]
pub fn impl_core_all_pnf(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_optical_attr(&i, "Filter", "filters", "F", PyStr::new1)
}

#[proc_macro]
pub fn impl_core_all_pno(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_optical_attr(&i, "Power", "powers", "O", |p| {
        PyFloat::new(RsFloat::F32, p)
    })
}

#[proc_macro]
pub fn impl_core_all_pnp(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_optical_attr(&i, "PercentEmitted", "percents_emitted", "P", PyStr::new1)
}

#[proc_macro]
pub fn impl_core_all_pnt(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_optical_attr(&i, "DetectorType", "detector_types", "T", PyStr::new1)
}

#[proc_macro]
pub fn impl_core_all_pnv(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_optical_attr(&i, "DetectorVoltage", "detector_voltages", "V", |p| {
        PyFloat::new(RsFloat::F32, p)
    })
}

#[proc_macro]
pub fn impl_core_all_pnl_old(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_optical_attr(&i, "Wavelength", "wavelengths", "L", |p| {
        PyFloat::new(RsFloat::F32, p)
    })
}

#[proc_macro]
pub fn impl_core_all_pnl_new(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_optical_attr(&i, "Wavelengths", "wavelengths", "L", |p| {
        PyList::new1(RsFloat::F32, p)
    })
}

#[proc_macro]
pub fn impl_core_all_pnd(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_meas_attr(&i, "Display", "displays", "D", |_| PyType::new_display())
}

#[proc_macro]
pub fn impl_core_all_pndet(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_optical_attr(&i, "DetectorName", "detector_names", "DET", PyStr::new1)
}

#[proc_macro]
pub fn impl_core_all_pncal3_1(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_optical_attr(&i, "Calibration3_1", "calibrations", "CALIBRATION", |_| {
        PyType::new_calibration3_1()
    })
}

#[proc_macro]
pub fn impl_core_all_pncal3_2(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_optical_attr(&i, "Calibration3_2", "calibrations", "CALIBRATION", |_| {
        PyType::new_calibration3_2()
    })
}

#[proc_macro]
pub fn impl_core_all_pntag(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_optical_attr(&i, "Tag", "tags", "TAG", PyStr::new1)
}

#[proc_macro]
pub fn impl_core_all_pntype(input: TokenStream) -> TokenStream {
    let i: Ident = syn::parse(input).unwrap();
    core_all_optical_attr(&i, "OpticalType", "measurement_types", "TYPE", PyStr::new1)
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
    core_all_optical_attr(&i, "Analyte", "analytes", "ANALYTE", PyStr::new1)
}

fn core_all_optical_attr<F, T>(t: &Ident, kw: &str, name: &str, suffix: &str, f: F) -> TokenStream
where
    F: FnOnce(Path) -> T,
    T: Into<PyType>,
{
    core_all_meas_attr1(t, kw, name, suffix, f, true, true)
}

fn core_all_meas_attr<F, T>(t: &Ident, kw: &str, name: &str, suffix: &str, f: F) -> TokenStream
where
    F: FnOnce(Path) -> T,
    T: Into<PyType>,
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
    T: Into<PyType>,
{
    let kw_doc = format!("*$Pn{suffix}*");
    let base_pytype: PyType = f(keyword_path(kw)).into();

    let doc_summary = format!("Value of {kw_doc} for all measurements.");
    let doc_middle = if optical_only {
        Some(format!(
            "``()`` will be returned for time since {kw_doc} is not \
             defined for temporal measurements."
        ))
    } else {
        None
    };

    let inner_pytype = PyOpt::wrap_if(base_pytype, is_optional);

    let inner_rstype = inner_pytype.as_rust_type();

    let nce_path = parse_quote!(fireflow_core::text::named_vec::NonCenterElement<#inner_rstype>);

    let full_pytype = if optical_only {
        PyUnion::new2(inner_pytype, PyTuple::default(), nce_path).into()
    } else {
        inner_pytype
    };

    let doc = DocString::new_ivar(
        doc_summary,
        doc_middle,
        DocReturn::new(PyList::new(full_pytype)),
    );

    doc.into_impl_get_set(
        t,
        format!("all_{name}"),
        true,
        |_, _| {
            if optical_only {
                quote! {
                    self.0
                        .optical_opt()
                        .map(|e| e.0.map_non_center(|x| x.cloned()).into())
                        .collect()
                }
            } else {
                quote!(self.0.meas_opt().map(|x| x.cloned()).collect())
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
            let param = DocArg::new_bool_param("force", param_desc);
            let doc = DocString::new_method(
                format!("Convert to FCS {vs}."),
                [sub],
                [param],
                Some(DocReturn::new1(
                    PyClass::new_py(target_type.to_string()),
                    format!("A new class conforming to FCS {vs}."),
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

    let scale = DocArg::new_opt_ivar_rw(
        "scale",
        PyType::new_scale(true),
        "The *$GmE* keyword. ``()`` means linear scaling and 2-tuple \
         specifies decades and offset for log scaling.",
        false,
        |n, _| quote!(self.0.#n.0.as_ref().cloned()),
        |n, _| quote!(self.0.#n.0 = #n.into()),
    );

    let make_arg_str = |n: &str, kw: &str, t: &str| {
        let path = keyword_path(t);
        DocArg::new_opt_ivar_rw(
            n,
            PyStr::new1(path),
            format!("The *$Gm{kw}* keyword."),
            false,
            |n, _| quote!(self.0.#n.0.as_ref().cloned()),
            |n, _| quote!(self.0.#n.0 = #n.into()),
        )
    };
    // TODO wet
    let make_arg_float = |n: &str, kw: &str, t: &str| {
        let path = keyword_path(t);
        DocArg::new_opt_ivar_rw(
            n,
            PyFloat::new(RsFloat::F32, path),
            format!("The *$Gm{kw}* keyword."),
            false,
            |n, _| quote!(self.0.#n.0.as_ref().cloned()),
            |n, _| quote!(self.0.#n.0 = #n.into()),
        )
    };
    let filter = make_arg_str("filter", "F", "GateFilter");
    let shortname = make_arg_str("shortname", "N", "GateShortname");
    let percent_emitted = make_arg_str("percent_emitted", "P", "GatePercentEmitted");
    // TODO isn't this a decimal?
    let range = make_arg_float("range", "R", "GateRange");
    let longname = make_arg_str("longname", "S", "GateLongname");
    let detector_type = make_arg_str("detector_type", "T", "GateDetectorType");
    let detector_voltage = make_arg_float("detector_voltage", "V", "GateDetectorVoltage");

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

    let inner_args: Vec<_> = all_args.iter().map(|x| x.ident_into()).collect();

    let summary = "The *$Gm\\** keywords for one gated measurement.";
    let doc = DocString::new_class(summary, [""; 0], all_args);

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #path::new(#(#inner_args),*).into()
            }
        }
    };

    doc.into_impl_class(name.to_string(), path.clone(), new, quote!())
        .1
        .into()
}

#[proc_macro]
pub fn impl_new_fixed_ascii_layout(input: TokenStream) -> TokenStream {
    let path: Path = syn::parse(input).unwrap();
    let name = path.segments.last().unwrap().ident.clone();
    let bare_path = path_strip_args(path.clone());

    let chars_param = DocArg::new_ivar_ro(
        "ranges",
        PyList::new(RsInt::U64),
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

    let (pyname, class) = doc.into_impl_class(name.to_string(), path, new, quote!());

    let char_widths_doc = DocString::new_ivar(
        "The width of each measurement.",
        [
            "Equivalent to *$PnB*, which is the number of chars/digits used \
             to encode data for a given measurement.",
        ],
        DocReturn::new(PyList::new(RsInt::U64)),
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
        PyList::new(RsInt::U64),
        "The range for each measurement. Equivalent to the *$PnR* keyword. \
         This is not used internally.",
        |_, _| quote!(self.0.ranges.clone()),
    );

    let doc = DocString::new_class("A delimited ASCII layout.", [""; 0], [ranges_param]);

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #bare_path::new(ranges).into()
            }
        }
    };

    let (pyname, class) = doc.into_impl_class(name.to_string(), path, new, quote!());
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

    let layout_name = format!("Ordered{base}{:02}Layout", nbits);

    let summary = format!("{nbits}-bit ordered {what} layout.");

    let range_param =
        DocArg::new_ivar_ro("ranges", PyList::new(range_pytype), range_desc, |_, _| {
            quote!(self.0.columns().iter().map(|c| c.clone()).collect())
        });

    let byteord_param = DocArg::new_ivar_ro_def(
        "byteord",
        PyUnion::new2(
            PyType::new_endian(),
            PyList::new(RsInt::U32),
            parse_quote!(#sizedbyteord_path<#nbytes>),
        ),
        format!(
            "The byte order to use when encoding values. Must be ``\"big\"``, \
             ``\"little\"``, or a list of all integers from 1 to {nbytes} \
             in any order."
        ),
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
            doc.into_impl_class(layout_name, full_layout_path, new, quote!())
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
            doc.into_impl_class(layout_name, full_layout_path, new, quote!())
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
            doc.into_impl_class(layout_name, full_layout_path, new, quote!())
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

    let layout_name = format!("EndianF{:02}Layout", nbits);

    let range_param = DocArg::new_ivar_ro(
        "ranges",
        PyList::new(PyType::new_float_range(nbytes)),
        "The range for each measurement. Corresponds to *$PnR*. This is not \
         used internally.",
        |_, _| quote!(self.0.columns().iter().map(|c| c.clone()).collect()),
    );

    let is_big_param = make_endian_param(4);

    let doc = DocString::new_class(
        format!("{nbits}-bit endian float layout"),
        [""; 0],
        [range_param.clone(), is_big_param],
    );

    let new = |fun_args| {
        quote! {
            fn new(#fun_args) -> Self {
                #fixed_layout_path::new(ranges, endian).into()
            }
        }
    };

    let (pyname, class) = doc.into_impl_class(layout_name, full_layout_path, new, quote!());

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
        PyList::new(RsInt::U64),
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

    let (pyname, class) = doc.into_impl_class(name.to_string(), layout_path, new, quote!());
    let datatype = make_layout_datatype(&pyname, "I");
    quote!(#class #datatype).into()
}

#[proc_macro]
pub fn impl_new_mixed_layout(_: TokenStream) -> TokenStream {
    let name = format_ident!("MixedLayout");
    let layout_path = parse_quote!(fireflow_core::data::#name);

    let null = quote!(fireflow_core::data::NullMixedType);
    let fixed = quote!(fireflow_core::data::FixedLayout);

    let types_param: DocArgROIvar = DocArg::new_ivar_ro(
        "typed_ranges",
        PyList::new(PyUnion::new2(
            PyTuple::new([PyLiteral::new(["A", "I"]).into(), PyType::from(RsInt::U64)]),
            PyTuple::new([
                PyLiteral::new(["F", "D"]).into(),
                PyType::from(PyDecimal::new()),
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

    doc.into_impl_class(name.to_string(), layout_path, new, quote!())
        .1
        .into()
}

// TODO not DRY
fn make_endian_ord_param(n: usize) -> DocArgROIvar {
    let xs = (1..(n + 1)).join(",");
    let ys = (1..(n + 1)).rev().join(",");
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
    let xs = (1..(n + 1)).join(",");
    let ys = (1..(n + 1)).rev().join(",");
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
        DocReturn::new(PyList::new(RsInt::U32)),
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
            PyType::new_gate_index(),
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
            PyType::new_prefixed_meas_index(),
        ),
        _ => panic!("unknown index type"),
    };

    let (region_name, index_pytype, gate_argname, gate_pytype, gate_desc) = if is_uni {
        (
            "univariate",
            index_pytype_inner,
            "gate",
            PyType::from(PyTuple::new1(
                [RsFloat::F32, RsFloat::F32],
                keyword_path("UniGate"),
            )),
            "The lower and upper bounds of the gate.",
        )
    } else {
        let v = keyword_path("Vertex");
        (
            "bivariate",
            PyTuple::new1(
                vec![index_pytype_inner; 2],
                parse_quote!(#index_pair<#index_path_inner>),
            )
            .into(),
            "vertices",
            PyList::new1(
                PyTuple::new([RsFloat::F32, RsFloat::F32]),
                parse_quote!(#nonempty<#v>),
            )
            .into(),
            "The vertices of a polygon gate. Must not be empty.",
        )
    };

    let summary = format!("Make a new FCS {summary_version}-compatible {region_name} region",);

    let index_arg = DocArg::new_ivar_ro("index", index_pytype, index_desc, |_, _| {
        quote!(self.0.index)
    });
    let gate_arg = DocArg::new_ivar_ro(
        gate_argname.to_string(),
        gate_pytype,
        gate_desc,
        |n, _| quote!(self.0.#n.clone()),
    );

    let doc = DocString::new_class(summary, [""; 0], [index_arg, gate_arg]);

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

    doc.into_impl_class(name, path, new, quote!()).1.into()
}

fn wrap_path_to_type(p: Path) -> Type {
    parse_quote!(#p)
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

fn pycoredataset(version: Version) -> Ident {
    format_ident!("PyCoreDataset{}", version.short_underscore())
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
type MethodDocString = DocString<Vec<DocArgParam>, Option<DocReturn>, SelfArg>;
type FunDocString = DocString<Vec<DocArgParam>, Option<DocReturn>, NoSelf>;
type IvarDocString = DocString<(), DocReturn, SelfArg>;

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

#[derive(Clone, new)]
struct DocArg<T> {
    #[new(into)]
    argname: String,
    #[new(into)]
    pytype: PyType,
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
        pytype: &PyType,
        f: impl FnOnce(&Ident, &PyType) -> TokenStream2,
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
        pytype: &PyType,
        fallible: bool,
        f: impl FnOnce(&Ident, &PyType) -> TokenStream2,
        g: impl FnOnce(&Ident, &PyType) -> TokenStream2,
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
struct DocReturn {
    rtype: PyType,
    desc: Option<String>,
}

impl DocReturn {
    fn new(rtype: impl Into<PyType>) -> Self {
        Self {
            rtype: rtype.into(),
            desc: None,
        }
    }

    fn new1(rtype: impl Into<PyType>, desc: impl Into<String>) -> Self {
        Self {
            rtype: rtype.into(),
            desc: Some(desc.into()),
        }
    }
}

#[derive(Clone, From, Display)]
enum PyType {
    #[from]
    Str(PyStr),
    #[from]
    Bool(PyBool),
    #[from]
    Bytes(PyBytes),
    #[from(RsInt)]
    #[from(PyInt)]
    Int(PyInt),
    #[from(RsFloat)]
    #[from(PyFloat)]
    Float(PyFloat),
    #[from]
    Decimal(PyDecimal),
    #[from]
    Datetime(PyDatetime),
    #[from]
    Date(PyDate),
    #[from]
    Time(PyTime),
    #[from(PyOpt)]
    Option(Box<PyOpt>),
    #[from(PyDict)]
    Dict(Box<PyDict>),
    #[from]
    Tuple(PyTuple),
    #[from(PyList)]
    List(Box<PyList>),
    // Set(Box<PyType>),
    #[from]
    Literal(PyLiteral),
    #[from]
    PyClass(PyClass),
    #[from(PyUnion)]
    Union(Box<PyUnion>),
}

#[derive(Clone)]
struct PyInt {
    rs: RsInt,
    rstype: Option<Path>,
}

#[derive(Clone, From)]
struct PyFloat {
    #[from]
    rs: RsFloat,
    rstype: Option<Path>,
}

#[derive(Clone)]
struct PyStr {
    rstype: Option<Path>,
}

#[derive(Clone)]
struct PyBool {
    rstype: Option<Path>,
}

#[derive(Clone)]
struct PyBytes {
    rstype: Option<Path>,
}

#[derive(Clone)]
struct PyDecimal {
    rstype: Option<Path>,
}

#[derive(Clone)]
struct PyTime {
    rstype: Option<Path>,
}

#[derive(Clone)]
struct PyDate {
    rstype: Option<Path>,
}

#[derive(Clone)]
struct PyDatetime {
    rstype: Option<Path>,
}

#[derive(Clone)]
struct PyLiteral {
    head: &'static str,
    tail: Vec<&'static str>,
    rstype: Option<Path>,
}

#[derive(Clone)]
struct PyOpt {
    inner: PyType,
    rstype: Option<Path>,
}

#[derive(Clone)]
struct PyDict {
    key: PyType,
    value: PyType,
    rstype: Option<Path>,
}

#[derive(Clone)]
struct PyList {
    inner: PyType,
    rstype: Option<Path>,
}

#[derive(Clone, new)]
struct PyClass {
    #[new(into)]
    pyname: String,
    rstype: Option<Path>,
}

#[derive(Clone)]
struct PyUnion {
    head0: PyType,
    head1: PyType,
    tail: Vec<PyType>,
    rstype: Path,
}

#[derive(Clone, Default)]
struct PyTuple {
    inner: Vec<PyType>,
    rstype: Option<Path>,
}

macro_rules! impl_py_prim_new {
    () => {
        fn new() -> Self {
            Self { rstype: None }
        }
    };
}

macro_rules! impl_py_prim_new1 {
    () => {
        fn new1(rstype: Path) -> Self {
            Self {
                rstype: Some(rstype),
            }
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

impl PyInt {
    fn new(rs: RsInt, rstype: Path) -> Self {
        Self {
            rs,
            rstype: Some(rstype),
        }
    }

    impl_py_num_defaults!("0".into());
}

impl PyFloat {
    fn new(rs: RsFloat, rstype: Path) -> Self {
        Self {
            rs,
            rstype: Some(rstype),
        }
    }

    impl_py_num_defaults!("0.0".into());
}

impl PyStr {
    impl_py_prim_new!();
    impl_py_prim_new1!();
    impl_py_prim_defaults!("\"\"".into(), String);
}

impl PyBool {
    impl_py_prim_new!();
    // impl_py_prim_new1!();
    impl_py_prim_defaults!("False".into(), bool);
}

impl PyBytes {
    impl_py_prim_new!();
    impl_py_prim_new1!();
    impl_py_prim_defaults!("b\"\"".into(), Vec);
}

impl PyDecimal {
    impl_py_prim_new!();
    impl_py_prim_new1!();
    impl_py_prim_defaults!("0".into(), bigdecimal::BigDecimal);
}

impl PyDate {
    impl_py_prim_new!();
    // impl_py_prim_new1!();
}

impl PyTime {
    impl_py_prim_new!();
    // impl_py_prim_new1!();
}

impl PyDatetime {
    impl_py_prim_new!();
    impl_py_prim_new1!();
}

impl PyDict {
    fn new(key: impl Into<PyType>, value: impl Into<PyType>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
            rstype: None,
        }
    }

    fn new1(key: impl Into<PyType>, value: impl Into<PyType>, rstype: Path) -> Self {
        let mut x = Self::new(key, value);
        x.rstype = Some(rstype);
        x
    }

    impl_py_prim_defaults!("{}".into(), std::collections::HashMap);
}

impl PyList {
    fn new(inner: impl Into<PyType>) -> Self {
        Self {
            inner: inner.into(),
            rstype: None,
        }
    }

    fn new1(inner: impl Into<PyType>, rstype: Path) -> Self {
        Self {
            inner: inner.into(),
            rstype: Some(rstype),
        }
    }

    impl_py_prim_defaults!("[]".into(), Vec);
}

impl HasRustPath for PyType {
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
        impl HasRustPath for $t {
            fn as_rust_type(&self) -> Type {
                if let Some(x) = self.rstype.as_ref() {
                    wrap_path_to_type(x.clone())
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

impl HasRustPath for PyOpt {
    fn as_rust_type(&self) -> Type {
        if let Some(x) = self.rstype.as_ref() {
            wrap_path_to_type(x.clone())
        } else {
            let i = self.inner.as_rust_type();
            parse_quote!(Option<#i>)
        }
    }
}

impl HasRustPath for PyDict {
    fn as_rust_type(&self) -> Type {
        if let Some(x) = self.rstype.as_ref() {
            wrap_path_to_type(x.clone())
        } else {
            let k = &self.key.as_rust_type();
            let v = &self.value.as_rust_type();
            parse_quote!(std::collections::HashMap<#k, #v>)
        }
    }
}

impl HasRustPath for PyTuple {
    fn as_rust_type(&self) -> Type {
        if let Some(x) = self.rstype.as_ref() {
            wrap_path_to_type(x.clone())
        } else {
            let vs: Vec<_> = self.inner.iter().map(|x| x.as_rust_type()).collect();
            parse_quote!((#(#vs),*))
        }
    }
}

impl HasRustPath for PyList {
    fn as_rust_type(&self) -> Type {
        if let Some(x) = self.rstype.as_ref() {
            wrap_path_to_type(x.clone())
        } else {
            let v = &self.inner.as_rust_type();
            parse_quote!(Vec<#v>)
        }
    }
}

impl HasRustPath for PyClass {
    fn as_rust_type(&self) -> Type {
        let x = self
            .rstype
            .as_ref()
            .expect("PyClass does not have a rust type")
            .clone();
        wrap_path_to_type(x)
    }
}

impl HasRustPath for PyLiteral {
    fn as_rust_type(&self) -> Type {
        let x = self
            .rstype
            .as_ref()
            .expect("PyLiteral does not have a rust type")
            .clone();
        wrap_path_to_type(x)
    }
}

impl HasRustPath for PyUnion {
    fn as_rust_type(&self) -> Type {
        wrap_path_to_type(self.rstype.clone())
    }
}

trait HasRustPath {
    fn as_rust_type(&self) -> Type;
}

impl HasRustPath for PyInt {
    fn as_rust_type(&self) -> Type {
        if let Some(path) = self.rstype.as_ref() {
            wrap_path_to_type(path.clone())
        } else {
            self.rs.as_rust_type()
        }
    }
}

impl HasRustPath for PyFloat {
    fn as_rust_type(&self) -> Type {
        if let Some(path) = self.rstype.as_ref() {
            wrap_path_to_type(path.clone())
        } else {
            self.rs.as_rust_type()
        }
    }
}

impl From<RsInt> for PyInt {
    fn from(rs: RsInt) -> Self {
        Self { rs, rstype: None }
    }
}

impl From<RsFloat> for PyFloat {
    fn from(rs: RsFloat) -> Self {
        Self { rs, rstype: None }
    }
}

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

#[derive(Clone)]
enum RsFloat {
    F32,
    F64,
}

impl PyLiteral {
    fn new(iter: impl IntoIterator<Item = &'static str>) -> Self {
        let mut it = iter.into_iter();
        let head = it.next().expect("Literal cannot be empty");
        let tail = it.collect();
        Self {
            head,
            tail,
            rstype: None,
        }
    }

    fn new1(iter: impl IntoIterator<Item = &'static str>, rstype: Path) -> Self {
        let mut x = Self::new(iter);
        x.rstype = Some(rstype);
        x
    }
}

impl PyOpt {
    fn new(inner: impl Into<PyType>) -> Self {
        Self {
            inner: inner.into(),
            rstype: None,
        }
    }

    fn defaults(&self) -> (String, TokenStream2) {
        (
            "None".into(),
            self.rstype
                .as_ref()
                .map_or(quote!(None), |y| quote!(#y::default())),
        )
    }

    fn wrap_if(inner: impl Into<PyType>, test: bool) -> PyType {
        if test {
            Self::new(inner).into()
        } else {
            inner.into()
        }
    }

    // fn new1(inner: impl Into<PyType>, rstype: Path) -> Self {
    //     Self {
    //         inner: inner.into(),
    //         rstype: Some(rstype),
    //     }
    // }
}

impl PyTuple {
    fn new(iter: impl IntoIterator<Item = impl Into<PyType>>) -> Self {
        Self {
            inner: iter.into_iter().map(|x| x.into()).collect(),
            rstype: None,
        }
    }

    fn new1(iter: impl IntoIterator<Item = impl Into<PyType>>, rstype: Path) -> Self {
        let mut x = Self::new(iter);
        x.rstype = Some(rstype);
        x
    }
}

impl PyUnion {
    fn new<T, A>(iter: T, rstype: Path) -> Self
    where
        T: IntoIterator<Item = A>,
        A: Into<PyType>,
    {
        let mut it = iter.into_iter();
        let x0 = it.next().expect("Union cannot be empty");
        let x1 = it.next().expect("Union must have at least 2 types");
        let xs = it.map(|x| x.into()).collect();
        Self {
            head0: x0.into(),
            head1: x1.into(),
            tail: xs,
            rstype,
        }
    }
}

impl PyUnion {
    fn new2(x: impl Into<PyType>, y: impl Into<PyType>, rstype: Path) -> Self {
        Self {
            head0: x.into(),
            head1: y.into(),
            tail: vec![],
            rstype,
        }
    }
}

impl PyClass {
    fn new1(pyname: impl Into<String>) -> Self {
        Self::new(pyname, None)
    }

    fn new2(pyname: impl Into<String>, rstype: Path) -> Self {
        Self::new(pyname, Some(rstype))
    }

    fn new_py(name: impl fmt::Display + Into<String>) -> Self {
        let pyname = format_ident!("Py{name}");
        Self::new2(name, parse_quote!(#pyname))
    }
}

impl DocArgROIvar {
    fn new_ivar_ro(
        argname: impl Into<String> + Clone,
        pytype: impl Into<PyType>,
        desc: impl Into<String>,
        f: impl FnOnce(&Ident, &PyType) -> TokenStream2,
    ) -> Self {
        let pt = pytype.into();
        let a = argname.into();
        let method = GetMethod::from_pytype(a.as_str(), &pt, f);
        Self::new(a, pt, desc, None, method)
    }

    fn new_ivar_ro_def(
        argname: impl Into<String>,
        pytype: impl Into<PyType>,
        desc: impl Into<String>,
        def: DocDefault,
        f: impl FnOnce(&Ident, &PyType) -> TokenStream2,
    ) -> Self {
        let pt = pytype.into();
        let a = argname.into();
        let method = GetMethod::from_pytype(a.as_str(), &pt, f);
        Self::new(a, pt, desc, Some(def), method)
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
        argname: impl Into<String>,
        pytype: impl Into<PyType>,
        desc: impl Into<String>,
        fallible: bool,
        f: impl FnOnce(&Ident, &PyType) -> TokenStream2,
        g: impl FnOnce(&Ident, &PyType) -> TokenStream2,
    ) -> Self {
        let pt = pytype.into();
        let name = argname.into();
        let methods = GetSetMethods::from_pytype(name.as_str(), &pt, fallible, f, g);
        Self::new(name, pt, desc, None, methods)
    }

    fn new_ivar_rw_def(
        argname: impl Into<String>,
        pytype: impl Into<PyType>,
        desc: impl Into<String>,
        def: DocDefault,
        fallible: bool,
        f: impl FnOnce(&Ident, &PyType) -> TokenStream2,
        g: impl FnOnce(&Ident, &PyType) -> TokenStream2,
    ) -> Self {
        let pt = pytype.into();
        let name = argname.into();
        let methods = GetSetMethods::from_pytype(name.as_str(), &pt, fallible, f, g);
        Self::new(name, pt, desc, Some(def), methods)
    }

    fn new_opt_ivar_rw(
        argname: impl Into<String>,
        pytype: impl Into<PyType>,
        desc: impl Into<String>,
        fallible: bool,
        f: impl FnOnce(&Ident, &PyType) -> TokenStream2,
        g: impl FnOnce(&Ident, &PyType) -> TokenStream2,
    ) -> Self {
        let pt = PyOpt::new(pytype.into());
        Self::new_ivar_rw_def(argname, pt, desc, DocDefault::Auto, fallible, f, g)
    }

    fn new_kw_ivar<F, T>(kw: &str, name: &str, f: F, desc: Option<&str>, def: bool) -> Self
    where
        F: FnOnce(Path) -> T,
        T: Into<PyType>,
    {
        let path = keyword_path(kw);
        let pytype: PyType = f(path.clone()).into();

        let d = desc.map_or(format!("Value of *${}*.", name.to_uppercase()), |d| {
            d.to_string()
        });

        let get_f = |_: &Ident, pytype: &PyType| {
            let optional = matches!(pytype, PyType::Option(_));
            let get_inner = format_ident!("{}", if optional { "metaroot_opt" } else { "metaroot" });
            let clone_inner = format_ident!("{}", if optional { "cloned" } else { "clone" });
            quote!(self.0.#get_inner::<#path>().#clone_inner())
        };
        let set_f = |n: &Ident, _: &PyType| quote!(self.0.set_metaroot(#n));

        if def {
            Self::new_ivar_rw_def(name, pytype, d, DocDefault::Auto, false, get_f, set_f)
        } else {
            Self::new_ivar_rw(name, pytype, d, false, get_f, set_f)
        }
    }

    fn new_meas_kw_ivar<F, T>(kw: &str, name: &str, f: F, desc: Option<&str>, def: bool) -> Self
    where
        F: FnOnce(Path) -> T,
        T: Into<PyType>,
    {
        let path = keyword_path(kw);
        let pytype: PyType = f(path.clone()).into();
        let full_path = pytype.as_rust_type();

        let d = desc.map_or(format!("Value of *${}*.", name.to_uppercase()), |d| {
            d.to_string()
        });

        let get_f = |_: &Ident, _: &PyType| {
            quote! {
                let x: &#full_path = self.0.as_ref();
                x.as_ref().cloned()
            }
        };
        let set_f = |n: &Ident, _: &PyType| quote!(*self.0.as_mut() = #n);

        if def {
            DocArg::new_ivar_rw_def(name, pytype, d, DocDefault::Auto, false, get_f, set_f)
        } else {
            DocArg::new_ivar_rw(name, pytype, d, false, get_f, set_f)
        }
    }

    fn new_kw_opt_ivar<F, T>(kw: &str, name: &str, f: F) -> Self
    where
        F: FnOnce(Path) -> T,
        T: Into<PyType>,
    {
        Self::new_kw_ivar(kw, name, |p| PyOpt::new(f(p)), None, true)
    }

    fn new_meas_kw_opt_ivar<F, T>(kw: &str, name: &str, abbr: &str, f: F) -> Self
    where
        F: FnOnce(Path) -> T,
        T: Into<PyType>,
    {
        let desc = format!("Value for *$Pn{abbr}*.");
        Self::new_meas_kw_ivar(kw, name, |p| PyOpt::new(f(p)), Some(desc.as_str()), true)
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
                PyUnion::new(ys, parse_quote!(PyLayout3_2))
            }
            Version::FCS3_1 => {
                let ys = ascii_layouts
                    .into_iter()
                    .chain(non_mixed_layouts)
                    .map(PyClass::new1);
                PyUnion::new(ys, parse_quote!(PyNonMixedLayout))
            }
            _ => {
                let ys = ascii_layouts
                    .into_iter()
                    .chain(ordered_layouts)
                    .map(PyClass::new1);
                PyUnion::new(ys, parse_quote!(PyOrderedLayout))
            }
        };
        let layout_desc = if version == Version::FCS3_2 {
            "Layout to describe data encoding. Represents *$PnB*, *$PnR*, *$BYTEORD*, \
             *$DATATYPE*, and *$PnDATATYPE*."
        } else {
            "Layout to describe data encoding. Represents *$PnB*, *$PnR*, *$BYTEORD*, \
             and *$DATATYPE*."
        };

        DocArg::new_ivar_rw(
            "layout",
            layout_pytype.clone(),
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
            |_, _| quote!(self.0.analysis.clone()),
            |n, _| quote!(self.0.analysis = #n.into()),
        )
    }

    fn new_others_ivar() -> Self {
        DocArg::new_others_param(true).into_rw(
            false,
            |_, _| quote!(self.0.others.clone()),
            |n, _| quote!(self.0.others = #n.into()),
        )
    }

    fn new_timestamps_ivar() -> [Self; 3] {
        let make_time_ivar = |is_start: bool| {
            let name = if is_start { "btim" } else { "etim" };
            let get_naive = format_ident!("{name}_naive");
            let set_naive = format_ident!("set_{name}_naive");
            let desc = format!("Value of *${}*.", name.to_uppercase());
            DocArg::new_opt_ivar_rw(
                name,
                PyTime::new(),
                desc,
                true,
                |_, _| quote!(self.0.#get_naive()),
                |n, _| quote!(Ok(self.0.#set_naive(#n)?)),
            )
        };

        let date_arg = DocArg::new_opt_ivar_rw(
            "date",
            PyDate::new(),
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
        DocArg::new_opt_ivar_rw(
            name,
            PyDatetime::new(),
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
        DocArg::new_opt_ivar_rw(
            "comp",
            PyClass::new2("~numpy.ndarray", rstype),
            desc,
            true,
            |_, _| quote!(self.0.compensation().cloned()),
            |n, _| quote!(Ok(self.0.set_compensation(#n)?)),
        )
    }

    fn new_spillover_ivar() -> Self {
        let rstype: Path = parse_quote!(fireflow_core::text::spillover::Spillover);
        DocArg::new_opt_ivar_rw(
            "spillover",
            PyTuple::new1(
                [
                    PyType::from(PyList::new(PyStr::new())),
                    PyClass::new1("~numpy.ndarray").into(),
                ],
                rstype.clone(),
            ),
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
        DocArg::new_opt_ivar_rw(
            "csvflags",
            PyList::new1(PyOpt::new(RsInt::U32), path.clone()),
            "Subset flags. Each element in the list corresponds to *$CSVnFLAG* and \
             the length of the list corresponds to *$CSMODE*.",
            false,
            |_, _| quote!(self.0.metaroot_opt::<#path>().cloned()),
            |n, _| quote!(self.0.set_metaroot(#n)),
        )
    }

    fn new_trigger_ivar() -> Self {
        DocArg::new_opt_ivar_rw(
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
        let path: Path = parse_quote!(fireflow_core::text::unstainedcenters::UnstainedCenters);
        DocArg::new_opt_ivar_rw(
            "unstainedcenters",
            PyDict::new1(PyStr::new(), RsFloat::F32, path.clone()),
            "Value for *$UNSTAINEDCENTERS. Each key must match a *$PnN*.",
            true,
            |_, _| quote!(self.0.metaroot_opt::<#path>().map(|y| y.clone())),
            |n, _| {
                quote!(self
                    .0
                    .set_unstained_centers(#n)
                    .py_termfail_resolve_nowarn())
            },
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
            Some(PyList::new(PyClass::new_py("GatedMeasurement")).into())
        } else {
            None
        };
        let urtype = PyClass::new1(format!("UnivariateRegion{vsu}"));
        let bvtype = PyClass::new1(format!("BivariateRegion{vsu}"));
        let regtype = format_ident!("PyRegion{vsu}");
        let maptype = parse_quote!(PyRegionMapping<#regtype>);
        let rtype = PyDict::new1(
            RsInt::NonZeroUsize,
            PyUnion::new2(urtype, bvtype, parse_quote!(#regtype)),
            maptype,
        )
        .into();
        let gtype = PyType::from(PyOpt::new(PyStr::new()));
        let pytype = PyTuple::new1(
            gmtype.into_iter().chain([rtype, gtype]),
            parse_quote!(#rstype),
        );

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
            DocArg::new_ivar_rw_def(
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
            DocArg::new_ivar_rw_def(
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
        DocArg::new_opt_ivar_rw(
            "scale",
            PyType::new_scale(false),
            "Value for *$PnE*. Empty tuple means linear scale; 2-tuple encodes \
             decades and offset for log scale",
            false,
            |_, _| quote!(self.0.specific.scale.0.as_ref().map(|&x| x)),
            |n, _| quote!(self.0.specific.scale = #n.into()),
        )
    }

    fn new_transform_ivar() -> Self {
        DocArg::new_ivar_rw(
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
        DocArg::new_ivar_rw_def(
            "nonstandard_keywords",
            PyType::new_nonstd_keywords(),
            desc,
            DocDefault::Auto,
            false,
            |_, _| quote!(#root.nonstandard_keywords.clone()),
            |n, _| quote!(#root.nonstandard_keywords = #n),
        )
    }
}

impl DocArgParam {
    fn new_param(
        argname: impl Into<String>,
        pytype: impl Into<PyType>,
        desc: impl Into<String>,
    ) -> Self {
        let pt = pytype.into();
        Self::new(argname, pt, desc, None, NoMethods)
    }

    fn new_param_def(
        argname: impl Into<String>,
        pytype: impl Into<PyType>,
        desc: impl Into<String>,
        def: DocDefault,
    ) -> Self {
        let pt = pytype.into();
        Self::new(argname, pt, desc, Some(def), NoMethods)
    }

    fn new_bool_param(name: impl Into<String>, desc: impl Into<String>) -> Self {
        Self::new_param_def(name, PyBool::new(), desc, DocDefault::Auto)
    }

    fn new_opt_param(
        name: impl Into<String>,
        pytype: impl Into<PyType>,
        desc: impl Into<String>,
    ) -> Self {
        Self::new_param_def(name, PyOpt::new(pytype), desc, DocDefault::Auto)
    }

    fn into_ro(self, f: impl FnOnce(&Ident, &PyType) -> TokenStream2) -> DocArgROIvar {
        let methods = GetMethod::from_pytype(self.argname.as_str(), &self.pytype, f);
        DocArgROIvar::new(self.argname, self.pytype, self.desc, self.default, methods)
    }

    fn into_rw(
        self,
        fallible: bool,
        f: impl FnOnce(&Ident, &PyType) -> TokenStream2,
        g: impl FnOnce(&Ident, &PyType) -> TokenStream2,
    ) -> DocArgRWIvar {
        let methods =
            GetSetMethods::from_pytype(self.argname.as_str(), &self.pytype, fallible, f, g);
        DocArgRWIvar::new(self.argname, self.pytype, self.desc, self.default, methods)
    }

    fn new_path_param(read: bool) -> Self {
        let s = if read { "read" } else { "written" };
        Self::new_param(
            "path",
            PyClass::new2("~pathlib.Path", parse_quote!(std::path::PathBuf)),
            format!("Path to be {s}."),
        )
    }

    fn new_version_param() -> Self {
        Self::new_param(
            "version",
            PyType::new_version(),
            "Version to use when parsing *TEXT*.",
        )
    }

    fn new_std_keywords_param() -> Self {
        DocArg::new_param("std", PyType::new_std_keywords(), "Standard keywords.")
    }

    fn new_nonstd_keywords_param() -> Self {
        DocArg::new_param(
            "nonstd",
            PyType::new_nonstd_keywords(),
            "Non-standard keywords.",
        )
    }

    fn new_parse_output_param() -> Self {
        DocArg::new_param(
            "parse",
            PyClass::new_py("RawTEXTParseData"),
            "Miscellaneous data obtained when parsing *TEXT*.",
        )
    }

    fn new_text_seg_param() -> Self {
        DocArg::new_param(
            "text_seg",
            PyType::new_text_segment(),
            "The primary *TEXT* segment from *HEADER*.",
        )
    }

    fn new_data_seg_param(src: SegmentSrc) -> Self {
        DocArg::new_param(
            "data_seg",
            PyType::new_data_segment(src),
            format!("The *DATA* segment from {src}."),
        )
    }

    fn new_analysis_seg_param(src: SegmentSrc, default: bool) -> Self {
        let mut p = DocArg::new_param(
            "analysis_seg",
            PyType::new_analysis_segment(src),
            format!("The *DATA* segment from {src}."),
        );
        if default {
            p.default = Some(DocDefault::Auto);
        }
        p
    }

    fn new_other_segs_param(default: bool) -> Self {
        let mut p = DocArg::new_param(
            "other_segs",
            PyList::new(PyType::new_other_segment()),
            "The *OTHER* segments from *HEADER*.",
        );
        if default {
            p.default = Some(DocDefault::Auto);
        }
        p
    }

    fn new_textdelim_param() -> Self {
        let path = parse_quote!(fireflow_core::validated::textdelim::TEXTDelim);
        Self::new_param_def(
            "delim",
            PyInt::new(RsInt::U8, path),
            "Delimiter to use when writing *TEXT*.",
            DocDefault::Int(30),
        )
    }

    fn new_big_other_param() -> Self {
        Self::new_bool_param(
            "big_other",
            "If ``True`` use 20 chars for OTHER segment offsets, and 8 otherwise.",
        )
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
            "The new measurements.",
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
        Self::new_bool_param(
            "skip_index_check",
            "If ``False``, raise exception if any non-measurement keyword have an \
             index reference to the current measurements. If ``True`` allow such \
             references to exist as long as they do not break (which really means \
             that the length of ``measurements`` is such that existing indices are \
             satisfied).",
        )
    }

    fn new_index_param(desc: &str) -> Self {
        Self::new_param("index", PyType::new_meas_index(), desc)
    }

    fn new_col_param() -> Self {
        let path: Path = parse_quote!(fireflow_core::validated::dataframe::AnyFCSColumn);
        Self::new_param(
            "col",
            PyClass::new2("polars.Series", path),
            "Data for measurement. Must be same length as existing columns.",
        )
    }

    fn new_name_param(short_desc: &str) -> Self {
        Self::new_param(
            "name",
            PyType::new_shortname(),
            format!("{short_desc} Corresponds to *$PnN*. Must not contain commas."),
        )
    }

    fn new_range_param() -> Self {
        Self::new_param(
            "range",
            PyType::new_range(),
            "Range of measurement. Corresponds to *$PnR*.",
        )
    }

    fn new_notrunc_param() -> Self {
        Self::new_bool_param(
            "notrunc",
            "If ``False``, raise exception if ``range`` must be truncated to fit \
             into measurement type.",
        )
    }

    fn new_data_param(polars_type: bool) -> Self {
        Self::new_param(
            "data",
            PyType::new_dataframe(polars_type),
            "A dataframe encoding the contents of *DATA*. Number of columns must \
             match number of measurements. May be empty. Types do not necessarily \
             need to correspond to those in the data layout but mismatches may \
             result in truncation.",
        )
    }

    fn new_analysis_param(default: bool) -> Self {
        Self::new(
            "analysis",
            PyType::new_analysis(),
            "Contents of the *ANALYSIS* segment.",
            if default {
                Some(DocDefault::Auto)
            } else {
                None
            },
            NoMethods,
        )
    }

    fn new_others_param(default: bool) -> Self {
        Self::new(
            "others",
            PyType::new_others(),
            "A list of byte strings encoding the *OTHER* segments.",
            if default {
                Some(DocDefault::Auto)
            } else {
                None
            },
            NoMethods,
        )
    }

    fn new_header_config_params() -> Vec<Self> {
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

    fn new_raw_config_params() -> Vec<Self> {
        vec![
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
        ]
    }

    fn new_std_config_params(version: Option<Version>) -> Vec<Self> {
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
            Some(Version::FCS2_0) => std_common_args.collect(),
            Some(Version::FCS3_0) => std_common_args.chain([ignore_time_gain]).collect(),
            _ => std_common_args
                .chain([ignore_time_gain, parse_indexed_spillover])
                .collect(),
        }
    }

    fn new_layout_config_params(version: Option<Version>) -> Vec<Self> {
        let integer_widths_from_byteord = Self::new_integer_widths_from_byteord_param();
        let integer_byteord_override = Self::new_integer_byteord_override_param();
        let disallow_range_truncation = Self::new_disallow_range_truncation_param();

        match version {
            Some(Version::FCS3_1) | Some(Version::FCS3_2) => {
                [disallow_range_truncation].into_iter().collect()
            }
            _ => [
                integer_widths_from_byteord,
                integer_byteord_override,
                disallow_range_truncation,
            ]
            .into_iter()
            .collect(),
        }
    }

    fn new_offsets_config_params(version: Option<Version>) -> Vec<Self> {
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
            "trim_intra_value_whitespace",
            "If ``True``, trim whitespace between delimiters such as ``,`` \
             and ``;`` within keyword value strings.",
        )
    }

    fn new_time_meas_pattern_param() -> Self {
        Self::new_param_def(
            "time_meas_pattern",
            PyOpt::new(PyStr::new1(parse_quote!(
                fireflow_core::config::TimeMeasNamePattern
            ))),
            format!(
                "A pattern to match the *$PnN* of the time measurement. Must be \
                a regular expression following syntax described in {REGEXP_REF}. \
                If ``None``, do not try to find a time measurement."
            ),
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
            PyList::new1(
                PyType::new_temporal_optical_key(),
                parse_quote!(TemporalOpticalKeys),
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
        Self::new_opt_param(
            "date_pattern",
            PyStr::new1(parse_quote!(
                fireflow_core::validated::datepattern::DatePattern
            )),
            format!(
                "If supplied, will be used as an alternative pattern when \
                 parsing *$DATE*. It should have specifiers for year, month, and \
                 day as outlined in {CHRONO_REF}. If not supplied, *$DATE* will \
                 be parsed according to the standard pattern which is \
                 ``%d-%b-%Y``."
            ),
        )
    }

    // TODO make this version specific
    fn new_time_pattern_param() -> Self {
        Self::new_opt_param(
            "time_pattern",
            PyStr::new1(parse_quote!(
                fireflow_core::validated::timepattern::TimePattern
            )),
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
        )
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
        Self::new_opt_param(
            "nonstandard_measurement_pattern",
            PyStr::new1(parse_quote!(
                fireflow_core::validated::keys::NonStdMeasPattern
            )),
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
        Self::new_opt_param(
            "integer_byteord_override",
            PyList::new1(
                RsInt::U32,
                parse_quote!(fireflow_core::text::byteord::ByteOrd2_0),
            ),
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
            PyType::new_correction(is_header, id),
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
            PyList::new(PyType::new_correction(true, "OtherSegmentId")),
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
        let path: Path = parse_quote!(fireflow_core::validated::ascii_range::OtherWidth);
        Self::new_param_def(
            "other_width",
            PyInt::new(RsInt::NonZeroU8, path.clone()),
            "Maximum number of OTHER segments that can be parsed. \
             ``None`` means limitless.",
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
        let common = "The first member of the tuples is a list of strings which \
                      match literally. The second member is a list of regular \
                      expressions corresponding to {REGEXP_REF}.";
        let d = format!("{desc}. {common}");
        Self::new_param_def(argname, PyType::new_key_patterns(), d, DocDefault::Auto)
    }

    fn new_rename_standard_keys() -> Self {
        let path: Path = parse_quote!(fireflow_core::validated::keys::KeyStringPairs);
        Self::new_param_def(
            "rename_standard_keys",
            PyDict::new1(PyStr::new(), PyStr::new(), path),
            "Rename standard keys in *TEXT*. Keys matching the first part of \
             the pair will be replaced by the second. Comparisons are case \
             insensitive. The leading *$* is implied so do not include it.",
            DocDefault::Auto,
        )
    }

    fn new_replace_standard_key_values() -> Self {
        let path: Path = parse_quote!(fireflow_core::validated::keys::KeyStringValues);
        Self::new_param_def(
            "replace_standard_key_values",
            PyDict::new1(PyStr::new(), PyStr::new(), path),
            "Replace values for standard keys in *TEXT* Comparisons are case \
             insensitive. The leading *$* is implied so do not include it.",
            DocDefault::Auto,
        )
    }

    fn new_append_standard_keywords() -> Self {
        let path: Path = parse_quote!(fireflow_core::validated::keys::KeyStringValues);
        Self::new_param_def(
            "append_standard_keywords",
            PyDict::new1(PyStr::new(), PyStr::new(), path),
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
}

impl DocDefault {
    fn as_value(&self, pytype: &PyType) -> (String, TokenStream2) {
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

    fn pytype(&self) -> &PyType;

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

    fn pytype(&self) -> &PyType {
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
            quote! {#n.map(|x| x.into())}
        } else {
            quote! {#n.into()}
        }
    }

    fn record_into(&self) -> TokenStream2 {
        let n = self.ident();
        if unwrap_generic("Option", unwrap_type_as_path(&self.pytype.as_rust_type())).1 {
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

    fn pytype(&self) -> &PyType {
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

impl PyType {
    fn new_optical(version: Version) -> Self {
        let n = format!("Optical{}", version.short_underscore());
        PyClass::new_py(n).into()
    }

    fn new_temporal(version: Version) -> Self {
        let n = format!("Temporal{}", version.short_underscore());
        PyClass::new_py(n).into()
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

    fn new_version() -> Self {
        let path = parse_quote!(fireflow_core::header::Version);
        PyLiteral::new1(["FCS2.0", "FCS3.0", "FCS3.1", "FCS3.2"], path).into()
    }

    fn new_temporal_optical_key() -> Self {
        PyLiteral::new1(
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
        PyLiteral::new1(["A", "I", "F", "D"], path).into()
    }

    fn new_display() -> Self {
        PyTuple::new1(
            [
                PyType::from(PyBool::new()),
                RsFloat::F32.into(),
                RsFloat::F32.into(),
            ],
            keyword_path("Display"),
        )
        .into()
    }

    fn new_feature() -> Self {
        let path = keyword_path("Feature");
        PyLiteral::new1(["Area", "Width", "Height"], path).into()
    }

    fn new_calibration3_1() -> Self {
        PyTuple::new1(
            [PyType::from(RsFloat::F32), PyStr::new().into()],
            keyword_path("Calibration3_1"),
        )
        .into()
    }

    fn new_calibration3_2() -> Self {
        PyTuple::new1(
            [
                PyType::from(RsFloat::F32),
                RsFloat::F32.into(),
                PyStr::new().into(),
            ],
            keyword_path("Calibration3_2"),
        )
        .into()
    }

    fn new_text_segment() -> Self {
        PyType::new_segment("PrimaryTextSegment")
    }

    fn new_supp_text_segment() -> Self {
        PyType::new_segment("SupplementalTextSegment")
    }

    fn new_other_segment() -> Self {
        PyType::new_segment("OtherSegment20")
    }

    fn new_data_segment(src: SegmentSrc) -> Self {
        let id = match src {
            SegmentSrc::Header => "HeaderDataSegment",
            // SegmentSrc::Text => "TEXTDataSegment",
            SegmentSrc::Any => "AnyDataSegment",
        };
        PyType::new_segment(id)
    }

    fn new_analysis_segment(src: SegmentSrc) -> Self {
        let id = match src {
            SegmentSrc::Header => "HeaderAnalysisSegment",
            // SegmentSrc::Text => "TEXTAnalysisSegment",
            SegmentSrc::Any => "AnyAnalysisSegment",
        };
        PyType::new_segment(id)
    }

    fn new_segment(n: &str) -> Self {
        let t = format_ident!("{n}");
        let p = parse_quote!(fireflow_core::segment::#t);
        PyTuple::new1([RsInt::U64, RsInt::U64], p).into()
    }

    fn new_correction(is_header: bool, id: &str) -> Self {
        let path = correction_path(is_header, id);
        PyTuple::new1([RsInt::I32, RsInt::I32], path).into()
    }

    fn new_scale(is_gate: bool) -> Self {
        let p = if is_gate {
            keyword_path("GateScale")
        } else {
            parse_quote!(fireflow_core::text::scale::Scale)
        };
        PyUnion::new2(
            PyTuple::default(),
            PyTuple::new([RsFloat::F32, RsFloat::F32]),
            p,
        )
        .into()
    }

    fn new_transform() -> Self {
        let rstype = parse_quote! {fireflow_core::core::ScaleTransform};
        PyUnion::new2(
            RsFloat::F32,
            PyTuple::new([RsFloat::F32, RsFloat::F32]),
            rstype,
        )
        .into()
    }

    fn new_key_patterns() -> Self {
        let path: Path = parse_quote!(fireflow_core::validated::keys::KeyPatterns);
        PyTuple::new1([PyList::new(PyStr::new()), PyList::new(PyStr::new())], path).into()
    }

    fn new_keywords() -> Self {
        PyDict::new(PyStr::new(), PyStr::new()).into()
    }

    fn new_std_keywords() -> Self {
        let path = parse_quote!(fireflow_core::validated::keys::StdKey);
        PyDict::new(PyStr::new1(path), PyStr::new()).into()
    }

    fn new_nonstd_keywords() -> Self {
        let path = parse_quote!(fireflow_core::validated::keys::NonStdKey);
        PyDict::new(PyStr::new1(path), PyStr::new()).into()
    }

    fn new_shortname() -> Self {
        let path = parse_quote!(fireflow_core::validated::shortname::Shortname);
        PyStr::new1(path).into()
    }

    fn new_versioned_shortname(version: Version) -> Self {
        PyOpt::wrap_if(Self::new_shortname(), version < Version::FCS3_1)
    }

    fn new_meas_index() -> Self {
        let path = parse_quote!(fireflow_core::text::index::MeasIndex);
        PyInt::new(RsInt::NonZeroUsize, path).into()
    }

    fn new_gate_index() -> Self {
        let p = parse_quote!(fireflow_core::text::index::GateIndex);
        PyInt::new(RsInt::NonZeroUsize, p).into()
    }

    fn new_meas_or_gate_index() -> Self {
        let p = parse_quote!(fireflow_core::text::keywords::MeasOrGateIndex);
        PyStr::new1(p).into()
    }

    fn new_prefixed_meas_index() -> Self {
        let p = parse_quote!(fireflow_core::text::keywords::PrefixedMeasIndex);
        PyInt::new(RsInt::NonZeroUsize, p).into()
    }

    fn new_analysis() -> Self {
        let path = parse_quote!(fireflow_core::core::Analysis);
        PyBytes::new1(path).into()
    }

    fn new_others() -> Self {
        let path = parse_quote!(fireflow_core::core::Others);
        PyList::new1(PyBytes::new(), path).into()
    }

    fn new_dataframe(polars_type: bool) -> Self {
        let path = if polars_type {
            parse_quote!(pyo3_polars::PyDataFrame)
        } else {
            parse_quote!(fireflow_core::validated::dataframe::FCSDataFrame)
        };
        PyClass::new2("polars.DataFrame", path).into()
    }

    fn new_range() -> Self {
        let p = parse_quote!(fireflow_core::text::keywords::Range);
        PyDecimal::new1(p).into()
    }

    fn new_bitmask(nbytes: usize) -> Self {
        let i = format_ident!("Bitmask{:02}", nbytes * 8);
        let p = parse_quote!(fireflow_core::validated::bitmask::#i);
        let r = match nbytes {
            1 => RsInt::U8,
            2 => RsInt::U16,
            3 => RsInt::U32,
            4 => RsInt::U32,
            5 => RsInt::U64,
            6 => RsInt::U64,
            7 => RsInt::U64,
            8 => RsInt::U64,
            _ => panic!("invalid number of uint bytes: {nbytes}"),
        };
        PyInt::new(r, p).into()
    }

    fn new_float_range(nbytes: usize) -> Self {
        let i = format_ident!("F{:02}Range", nbytes * 8);
        let p = parse_quote!(fireflow_core::data::#i);
        let r = match nbytes {
            4 => RsFloat::F32,
            8 => RsFloat::F64,
            _ => panic!("invalid number of float bytes: {nbytes}"),
        };
        PyFloat::new(r, p).into()
    }

    fn new_timestep() -> Self {
        PyFloat::new(RsFloat::F32, keyword_path("Timestep")).into()
    }

    fn new_tr() -> Self {
        let path = keyword_path("Trigger");
        PyTuple::new1([PyType::from(RsInt::U32), PyStr::new().into()], path).into()
    }

    fn new_endian() -> Self {
        let endian: Path = parse_quote!(fireflow_core::text::byteord::Endian);
        PyLiteral::new1(["little", "big"], endian).into()
    }

    fn new_meas(version: Version) -> Self {
        let (fam_ident, name_pytype) = if version < Version::FCS3_1 {
            (
                format_ident!("MaybeFamily"),
                PyOpt::new(PyStr::new()).into(),
            )
        } else {
            (format_ident!("AlwaysFamily"), PyType::from(PyStr::new()))
        };
        let fam_path = quote!(fireflow_core::text::optional::#fam_ident);
        let meas_opt_pyname = pyoptical(version);
        let meas_tmp_pyname = pytemporal(version);
        let meas_argtype: Path =
            parse_quote!(PyEithers<#fam_path, #meas_tmp_pyname, #meas_opt_pyname>);
        PyTuple::new1(
            [name_pytype, PyType::new_measurement(version)],
            meas_argtype.clone(),
        )
        .into()
    }

    fn new_anycoretext() -> Self {
        PyUnion::new(
            ALL_VERSIONS
                .iter()
                .map(|v| PyClass::new_py(format!("CoreTEXT{}", v.short_underscore()))),
            parse_quote!(PyAnyCoreTEXT),
        )
        .into()
    }

    fn new_anycoredataset() -> Self {
        PyUnion::new(
            ALL_VERSIONS
                .iter()
                .map(|v| PyClass::new_py(format!("CoreDataset{}", v.short_underscore()))),
            parse_quote!(PyAnyCoreDataset),
        )
        .into()
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
            Self::Option(x) => x.defaults(),
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
                let (ps, rs): (Vec<_>, Vec<_>) = xs.inner.iter().map(|x| x.defaults()).unzip();
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

impl ClassDocString {
    fn new_class(
        summary: impl Into<String>,
        paragraphs: impl IntoIterator<Item = impl Into<String>>,
        args: impl IntoIterator<Item = impl Into<AnyDocArg>>,
    ) -> Self {
        Self::new(
            summary.into(),
            paragraphs.into_iter().map(|x| x.into()).collect(),
            args.into_iter().map(|x| x.into()).collect(),
            (),
        )
    }

    fn into_impl_class<F>(
        self,
        name: String,
        path: Path,
        constr: F,
        rest: TokenStream2,
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

                #rest
            }
        };
        (pyname, s)
    }

    fn as_impl_wrapped(&self, name: String, path: Path) -> (Ident, TokenStream2) {
        let doc = self.doc();
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
}

impl MethodDocString {
    fn new_method(
        summary: impl Into<String>,
        paragraphs: impl IntoIterator<Item = impl Into<String>>,
        args: impl IntoIterator<Item = DocArgParam>,
        returns: Option<DocReturn>,
    ) -> Self {
        Self::new(
            summary.into(),
            paragraphs.into_iter().map(|x| x.into()).collect(),
            args.into_iter().collect(),
            returns,
        )
    }
}

impl IvarDocString {
    fn new_ivar(
        summary: impl Into<String>,
        paragraphs: impl IntoIterator<Item = impl Into<String>>,
        returns: DocReturn,
    ) -> Self {
        Self::new(
            summary.into(),
            paragraphs.into_iter().map(|x| x.into()).collect(),
            (),
            returns,
        )
    }

    fn into_impl_get(
        mut self,
        class: &Ident,
        name: impl Into<String>,
        f: impl FnOnce(&Ident, &PyType) -> TokenStream2,
    ) -> TokenStream2 {
        self.append_summary_or_paragraph("read-only", "This attribute is read-only.");
        let i = format_ident!("{}", name.into());
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
        name: impl Into<String>,
        fallible: bool,
        getf: impl FnOnce(&Ident, &PyType) -> TokenStream2,
        setf: impl FnOnce(&Ident, &PyType) -> TokenStream2,
    ) -> TokenStream2 {
        self.append_summary_or_paragraph("read-write", "This attribute is read-write.");
        let get = format_ident!("{}", name.into());
        let set = format_ident!("set_{get}");
        let pt = &self.returns.rtype;
        let rt = pt.as_rust_type();
        let get_body = getf(&get, pt);
        let set_body = setf(&get, pt);
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
        summary: impl Into<String>,
        paragraphs: impl IntoIterator<Item = S>,
        args: impl IntoIterator<Item = DocArgParam>,
        returns: Option<DocReturn>,
    ) -> Self
    where
        S: Into<String>,
    {
        Self::new(
            summary.into(),
            paragraphs.into_iter().map(|x| x.into()).collect(),
            args.into_iter().collect(),
            returns,
        )
    }
}

impl<A, S> DocString<A, Option<DocReturn>, S> {
    fn ret_path(&self) -> TokenStream2 {
        self.returns
            .as_ref()
            .map(|x| x.rtype.as_rust_type().to_token_stream())
            .unwrap_or(quote!(()))
    }
}

impl<A, R, S> DocString<Vec<A>, R, S> {
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

    fn sig(&self) -> TokenStream2
    where
        A: IsDocArg,
        S: IsSelfArg,
    {
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
                    let (t, r) = d.as_value(a.pytype());
                    // let t = d.as_py_value();
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
}

impl<A, R, S> DocString<A, R, S> {
    fn doc(&self) -> TokenStream2
    where
        Self: fmt::Display,
    {
        let s = self.to_string();
        quote! {#[doc = #s]}
    }

    fn append_paragraph(&mut self, p: impl Into<String>) {
        self.paragraphs.extend([p.into()]);
    }

    fn append_summary_or_paragraph(&mut self, suffix: impl fmt::Display, para: impl Into<String>) {
        let new_summary = format!("{} ({suffix}).", self.summary.trim_end_matches("."));
        if new_summary.len() > LINE_LEN {
            self.append_paragraph(para)
        } else {
            self.summary = new_summary
        }
    }

    fn fmt_inner<'a, 'b, F, G, I>(
        &'a self,
        f_args: F,
        f_return: G,
        f: &mut fmt::Formatter<'b>,
    ) -> Result<(), fmt::Error>
    where
        F: FnOnce(&'a A) -> I,
        G: FnOnce(&'a R) -> Option<String>,
        I: Iterator<Item = String> + 'a,
    {
        let ps = self
            .paragraphs
            .iter()
            .map(|s| fmt_docstring_nonparam(s.as_str()));
        let a = f_args(&self.args);
        let r = f_return(&self.returns);
        let rest = ps.chain(a).chain(r).join("\n\n");
        if self.summary.len() > LINE_LEN {
            panic!("summary is too long");
        }
        write!(f, "{}\n\n{rest}", self.summary)
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
        self.fmt_inner(|a| a.iter().map(|s| s.to_string()), |()| None, f)
    }
}

impl fmt::Display for IvarDocString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        self.fmt_inner(|()| [].into_iter(), |r| Some(r.to_string()), f)
    }
}

impl<A: fmt::Display, S> fmt::Display for DocString<Vec<A>, Option<DocReturn>, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        self.fmt_inner(
            |a| a.iter().map(|s| s.to_string()),
            |r| r.as_ref().map(|s| s.to_string()),
            f,
        )
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

impl fmt::Display for PyOpt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{} | None", self.inner)
    }
}

impl fmt::Display for PyBool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.write_str(":py:class:`bool`")
    }
}

impl fmt::Display for PyStr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.write_str(":py:class:`str`")
    }
}

impl fmt::Display for PyBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.write_str(":py:class:`bytes`")
    }
}

impl fmt::Display for PyInt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.write_str(":py:class:`int`")
    }
}

impl fmt::Display for PyFloat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.write_str(":py:class:`float`")
    }
}

impl fmt::Display for PyDecimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.write_str(":py:class:`~decimal.Decimal`")
    }
}

impl fmt::Display for PyDate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.write_str(":py:class:`~datetime.date`")
    }
}

impl fmt::Display for PyTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.write_str(":py:class:`~datetime.time`")
    }
}

impl fmt::Display for PyDatetime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.write_str(":py:class:`~datetime.datetime`")
    }
}

impl fmt::Display for PyLiteral {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            ":obj:`~typing.Literal`\\ [{}]",
            [&self.head]
                .into_iter()
                .chain(self.tail.iter())
                .map(|s| format!("\"{s}\""))
                .join(", ")
        )
    }
}

impl fmt::Display for PyUnion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let s = [&self.head0, &self.head1]
            .into_iter()
            .chain(self.tail.iter())
            .join(" | ");
        f.write_str(s.as_str())
    }
}

impl fmt::Display for PyDict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, ":py:class:`dict`\\ [{}, {}]", self.key, self.value)
    }
}

impl fmt::Display for PyList {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, ":py:class:`list`\\ [{}]", self.inner)
    }
}

impl fmt::Display for PyTuple {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let s = if self.inner.is_empty() {
            "()".into()
        } else {
            self.inner.iter().join(", ")
        };
        write!(f, ":py:class:`tuple`\\ [{s}]")
    }
}

impl fmt::Display for PyClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, ":py:class:`{}`", self.pyname)
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

const ALL_VERSIONS: [Version; 4] = [
    Version::FCS2_0,
    Version::FCS3_0,
    Version::FCS3_1,
    Version::FCS3_2,
];

const CHRONO_REF: &str =
    "`chrono <https://docs.rs/chrono/latest/chrono/format/strftime/index.html>`__";

const REGEXP_REF: &str = "`regexp-syntax <https://docs.rs/regex-syntax/latest/regex_syntax/>`__";

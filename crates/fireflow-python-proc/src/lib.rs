extern crate proc_macro;

mod docstring;

use crate::docstring::{ArgType, DocArg, DocDefault, DocReturn, DocString, PyType};

use fireflow_core::header::Version;

use proc_macro::TokenStream;

use derive_new::new;
use itertools::Itertools;
use quote::{format_ident, quote, ToTokens};
use syn::{
    parenthesized,
    parse::{Parse, ParseStream},
    parse_macro_input, parse_quote, parse_str,
    punctuated::Punctuated,
    token::Comma,
    Expr, GenericArgument, Ident, LitBool, LitInt, LitStr, Path, PathArguments, Result, Token,
    Type,
};

#[proc_macro]
pub fn impl_new_core(input: TokenStream) -> TokenStream {
    let info = parse_macro_input!(input as NewCoreInfo);
    let coretext_rstype = info.coretext_type;
    let coredataset_rstype = info.coredataset_type;
    let fun = info.fun;

    let df_type = parse_str::<Path>("fireflow_core::validated::dataframe::FCSDataFrame").unwrap();
    let analysis_type = parse_str::<Path>("fireflow_core::core::Analysis").unwrap();
    let others_type = parse_str::<Path>("fireflow_core::core::Others").unwrap();

    let polars_df_type = quote! {pyo3_polars::PyDataFrame};

    let coretext_name = path_name(&coretext_rstype);
    let coredataset_name = path_name(&coredataset_rstype);

    let version = split_version(&coretext_name).1;
    let v = version.short();

    let coretext_pytype = format_ident!("Py{coretext_name}");
    let coredataset_pytype = format_ident!("Py{coredataset_name}");

    let meas = make_measurements(version);
    let meas_pytype = &meas.doc.pytype;
    let meas_argtype = &meas.rstype;

    let layout = make_layout(version);
    let layout_pytype = &layout.doc.pytype;
    let layout_ident = &layout.rstype;

    let df = make_df();
    let df_pytype = &df.doc.pytype;

    let analysis = make_analysis();

    let others = make_others();

    let param_type_set_meas = DocArg::new_param(
        "measurements".into(),
        meas_pytype.clone(),
        "The new measurements.".into(),
    );

    let param_type_set_layout = DocArg::new_param(
        "layout".into(),
        layout_pytype.clone(),
        "The new layout.".into(),
    );

    let param_type_set_df =
        DocArg::new_param("df".to_string(), df_pytype.clone(), "The new data.".into());

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
                .py_term_resolve_nowarn()
        }
    };

    let set_meas_and_layout_method = quote! {
        fn set_measurements_and_layout(
            &mut self,
            measurements: #meas_argtype,
            layout: #layout_ident,
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

    let get_all_pnn_doc = DocString::new(
        "Value of *$PnN* for all measurements.".into(),
        vec!["Strings are unique and cannot contain commas.".into()],
        vec![],
        Some(DocReturn::new(PyType::new_list(PyType::Str), None)),
    );

    let textdelim_type =
        parse_str::<Path>("fireflow_core::validated::textdelim::TEXTDelim").unwrap();

    let path_param = DocArg::new_param(
        "path".into(),
        PyType::PyClass("pathlib.path".into()),
        "Path to be written".into(),
    );

    let textdelim_param = DocArg::new_param_def(
        "delim".into(),
        PyType::Int,
        "Delimiter to use when writing *TEXT*. Defaults to 30 (record separator).".into(),
        DocDefault::Other(quote! {#textdelim_type::default()}, "30".into()),
    );

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
        vec![path_param.clone(), textdelim_param.clone()],
        None,
    );

    let par_doc = DocString::new(
        "The value for *$PAR*.".into(),
        vec![],
        vec![],
        Some(DocReturn::new(PyType::Int, None)),
    );

    let set_tr_threshold_doc = DocString::new(
        "Set the threshold for *$TR*.".into(),
        vec![],
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

    let rename_temporal_doc = DocString::new(
        "Rename temporal measurement if present.".into(),
        vec![],
        // TODO kinda not DRY
        vec![DocArg::new_param(
            "name".into(),
            PyType::Str,
            "New name to assign. Must not have commas.".into(),
        )],
        Some(DocReturn::new(
            PyType::new_opt(PyType::Bool),
            Some("Previous name if present".into()),
        )),
    );

    let shortname_type =
        parse_str::<Path>("fireflow_core::validated::shortname::Shortname").unwrap();

    let timestep_type = parse_str::<Path>("fireflow_core::text::keywords::Timestep").unwrap();

    let get_set_all_pnn_maybe = if version < Version::FCS3_1 {
        let doc = DocString::new(
            "The possibly-empty values of *$PnN* for all measurements.".into(),
            vec!["*$PnN* is optional for this FCS version so values may be ``None``.".into()],
            vec![],
            Some(DocReturn::new(
                PyType::new_list(PyType::new_opt(PyType::Str)),
                None,
            )),
        );
        quote! {
            #doc
            #[getter]
            fn get_all_pnn_maybe(&self) -> Vec<Option<#shortname_type>> {
                self.0
                    .shortnames_maybe()
                    .into_iter()
                    .map(|x| x.cloned())
                    .collect()
            }

            #[setter]
            fn set_all_pnn_maybe(&mut self, names: Vec<Option<#shortname_type>>) -> PyResult<()> {
                Ok(self.0.set_measurement_shortnames_maybe(names).void()?)
            }
        }
    } else {
        quote! {}
    };

    let write_dataset_doc = DocString::new(
        "Write data as an FCS file.".into(),
        ["The resulting file will include *HEADER*, *TEXT*, *DATA*, \
            *ANALYSIS*, and *OTHER* as they present from this class."
            .into()]
        .into_iter()
        .chain(write_2_0_warning)
        .collect(),
        vec![
            path_param,
            textdelim_param,
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

    let write_dataset_mtd = quote! {
        #write_dataset_doc
        fn write_dataset(
            &self,
            path: PathBuf,
            delim: #textdelim_type,
            skip_conversion_check: bool,
        ) -> PyResult<()> {
            let f = File::options().write(true).create(true).open(path)?;
            let mut h = BufWriter::new(f);
            let conf = cfg::WriteConfig {
                delim,
                skip_conversion_check,
            };
            self.0.h_write_dataset(&mut h, &conf).py_term_resolve()
        }
    };

    let make_set_temporal_doc = |has_timestep: bool, has_index: bool| {
        let name = DocArg::new_param(
            "name".into(),
            PyType::Str,
            "Name to set. Must be a *$PnN* which is present.".into(),
        );
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

    let set_temporal_mtds = if version == Version::FCS2_0 {
        let name_doc = make_set_temporal_doc(false, false);
        let index_doc = make_set_temporal_doc(false, true);
        quote! {
            #name_doc
            fn set_temporal(&mut self, name: #shortname_type, force: bool) -> PyResult<bool> {
                self.0.set_temporal(&name, (), force).py_term_resolve()
            }

            #index_doc
            fn set_temporal_at(&mut self, index: MeasIndex, force: bool) -> PyResult<bool> {
                self.0.set_temporal_at(index, (), force).py_term_resolve()
            }
        }
    } else {
        let name_doc = make_set_temporal_doc(true, false);
        let index_doc = make_set_temporal_doc(true, true);
        quote! {
            #name_doc
            fn set_temporal(
                &mut self,
                name: #shortname_type,
                timestep: #timestep_type,
                force: bool,
            ) -> PyResult<bool> {
                self.0
                    .set_temporal(&name, timestep, force)
                    .py_term_resolve()
            }

            #index_doc
            fn set_temporal_at(
                &mut self,
                index: MeasIndex,
                timestep: #timestep_type,
                force: bool,
            ) -> PyResult<bool> {
                self.0
                    .set_temporal_at(index, timestep, force)
                    .py_term_resolve()
            }
        }
    };

    let make_unset_temporal_doc = |has_timestep: bool, has_force: bool| {
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
        DocString::new(s, vec![], p, Some(DocReturn::new(rt, Some(rd))))
    };

    let unset_temporal_mtd = if version == Version::FCS2_0 {
        let doc = make_unset_temporal_doc(false, false);
        quote! {
            #doc
            fn unset_temporal(&mut self) -> bool {
                self.0.unset_temporal().is_some()
            }
        }
    } else if version < Version::FCS3_2 {
        let doc = make_unset_temporal_doc(true, false);
        quote! {
            #doc
            fn unset_temporal(&mut self) -> Option<#timestep_type> {
                self.0.unset_temporal()
            }
        }
    } else {
        let doc = make_unset_temporal_doc(true, true);
        quote! {
            #doc
            fn unset_temporal(&mut self, force: bool) -> PyResult<Option<#timestep_type>> {
                self.0.unset_temporal_lossy(force).py_term_resolve()
            }
        }
    };

    let get_set_scale = if version == Version::FCS2_0 {
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
            vec![],
            Some(DocReturn::new(
                PyType::new_list(PyType::new_union(vec![
                    PyType::new_unit(),
                    PyType::Tuple(vec![PyType::Float, PyType::Float]),
                    PyType::None,
                ])),
                None,
            )),
        );
        quote! {
            #doc
            #[getter]
            fn get_scales(&self) -> Vec<Option<Scale>> {
                self.0.scales().collect()
            }

            #[setter]
            fn set_scales(&mut self, scales: Vec<Option<Scale>>) -> PyResult<()> {
                self.0.set_scales(scales).py_term_resolve_nowarn()
            }
        }
    } else {
        let sum = "The value for *$PnE* and/or *$PnG* for all measurements.".into();
        let s0 = "Collectively these keywords correspond to scale transforms.".into();
        let s1 = "If scaling is linear, return a float which corresponds to the \
                  value of *$PnG* when *$PnE* is ``0,0``. If scaling is logarithmic, \
                  return a pair of floats, corresponding to unset *$PnG* and the \
                  non-``0,0`` value of *$PnE*."
            .into();
        let s2 = "The FCS standards disallow any other combinations.".into();
        let s3 = "The temporal measurement will always be ``1.0``, corresponding \
                  to an identity transform. Setting it to another value will \
                  raise an exception."
            .into();
        let doc = DocString::new(
            sum,
            vec![s0, s1, s2, s3],
            vec![],
            Some(DocReturn::new(
                PyType::new_list(PyType::new_union2(
                    PyType::Float,
                    PyType::Tuple(vec![PyType::Float, PyType::Float]),
                )),
                None,
            )),
        );
        quote! {
            #doc
            #[getter]
            fn get_transforms(&self) -> Vec<core::ScaleTransform> {
                self.0.transforms().collect()
            }

            #[setter]
            fn set_transforms(&mut self, transforms: Vec<core::ScaleTransform>) -> PyResult<()> {
                self.0.set_transforms(transforms).py_term_resolve_nowarn()
            }
        }
    };

    let get_set_timestep = if version == Version::FCS2_0 {
        quote! {}
    } else {
        let t = PyType::new_opt(PyType::Float);
        let get_doc = DocString::new(
            "The value of *$TIMESTEP*".into(),
            vec![],
            vec![],
            Some(DocReturn::new(t.clone(), None)),
        );
        let set_doc = DocString::new(
            "Set the *$TIMESTEP* if time measurement is present.".into(),
            vec![],
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
        quote! {
            #get_doc
            #[getter]
            fn get_timestep(&self) -> Option<#timestep_type> {
                self.0.timestep().copied()
            }

            #set_doc
            fn set_timestep(&mut self, timestep: #timestep_type) -> Option<#timestep_type> {
                self.0.set_timestep(timestep)
            }
        }
    };

    let get_set_all_peak = if version < Version::FCS3_2 {
        let pkn_doc = DocString::new(
            "The value of *$PKn* for all measurements.".into(),
            vec![],
            vec![],
            Some(DocReturn::new(PyType::new_list(PyType::Int), None)),
        );
        let pknn_doc = DocString::new(
            "The value of *$PKNn* for all measurements.".into(),
            vec![],
            vec![],
            Some(DocReturn::new(PyType::new_list(PyType::Int), None)),
        );
        quote! {
            #pkn_doc
            #[getter]
            fn get_all_pkn(&self) -> Vec<Option<kws::PeakBin>> {
                self.0
                    .get_temporal_optical::<Option<kws::PeakBin>>()
                    .map(|x| x.as_ref().copied())
                    .collect()
            }

            #[setter]
            fn set_all_pkn(&mut self, xs: Vec<Option<kws::PeakBin>>) -> PyResult<()> {
                Ok(self.0.set_temporal_optical(xs)?)
            }

            #pknn_doc
            #[getter]
            fn get_all_pknn(&self) -> Vec<Option<kws::PeakNumber>> {
                self.0
                    .get_temporal_optical::<Option<kws::PeakNumber>>()
                    .map(|x| x.as_ref().copied())
                    .collect()
            }

            #[setter]
            fn set_all_pknn(&mut self, xs: Vec<Option<kws::PeakNumber>>) -> PyResult<()> {
                Ok(self.0.set_temporal_optical(xs)?)
            }
        }
    } else {
        quote! {}
    };

    let to_dataset_doc = DocString::new(
        "Convert to a dataset object.".into(),
        vec!["This will fully represent an FCS file, as opposed to just \
             representing *HEADER* and *TEXT*."
            .into()],
        // TODO reuse previous args here
        vec![
            DocArg::new_param(
                "df".into(),
                df_pytype.clone(),
                "Columns corresponding to *DATA*".into(),
            ),
            DocArg::new_param_def(
                "analysis".into(),
                PyType::Bytes,
                "Bytes corresponding to *ANALYSIS*".into(),
                DocDefault::Other(quote! {#analysis_type::default()}, "b\"\"".into()),
            ),
            DocArg::new_param_def(
                "others".into(),
                PyType::new_list(PyType::Bytes),
                "Bytes corresponding to *OTHER* segments".into(),
                DocDefault::Other(quote! {#others_type::default()}, "[]".into()),
            ),
        ],
        Some(DocReturn::new(
            PyType::PyClass(coredataset_pytype.to_string()),
            None,
        )),
    );

    let to_dataset_mtd = quote! {
        #to_dataset_doc
        fn to_dataset(
            &self,
            df: FCSDataFrame,
            analysis: core::Analysis,
            others: core::Others,
        ) -> PyResult<#coredataset_pytype> {
            Ok(self
               .0
               .clone()
               .into_coredataset(df, analysis, others)?
               .into())
        }
    };

    let mode = if version < Version::FCS3_2 {
        let t = PyType::Literal("L".into(), vec!["U".into(), "C".into()]);
        // TODO make default arg
        make_ivar_metaroot("Mode", "mode", t, None, None)
    } else {
        let t = PyType::Literal("L".into(), vec![]);
        make_ivar_metaroot("Mode3_2", "mode", t, None, Some(DocDefault::Option))
    };

    let cyt = if version < Version::FCS3_2 {
        make_ivar_metaroot("Cyt", "cyt", PyType::Str, None, Some(DocDefault::Option))
    } else {
        make_ivar_metaroot("Cyt", "cyt", PyType::Str, None, None)
    };

    let abrt = make_ivar_metaroot("Abrt", "abrt", PyType::Int, None, Some(DocDefault::Option));

    let com = make_ivar_metaroot("Com", "com", PyType::Str, None, Some(DocDefault::Option));

    let cells = make_ivar_metaroot(
        "Cells",
        "cells",
        PyType::Str,
        None,
        Some(DocDefault::Option),
    );

    let exp = make_ivar_metaroot("Exp", "exp", PyType::Str, None, Some(DocDefault::Option));

    let fil = make_ivar_metaroot("Fil", "fil", PyType::Str, None, Some(DocDefault::Option));

    let inst = make_ivar_metaroot("Inst", "inst", PyType::Str, None, Some(DocDefault::Option));

    let lost = make_ivar_metaroot("Lost", "lost", PyType::Int, None, Some(DocDefault::Option));

    let op = make_ivar_metaroot("Op", "op", PyType::Str, None, Some(DocDefault::Option));

    let proj = make_ivar_metaroot("Proj", "proj", PyType::Str, None, Some(DocDefault::Option));

    let smno = make_ivar_metaroot("Smno", "smno", PyType::Str, None, Some(DocDefault::Option));

    let src = make_ivar_metaroot("Src", "src", PyType::Str, None, Some(DocDefault::Option));

    let sys = make_ivar_metaroot("Sys", "sys", PyType::Str, None, Some(DocDefault::Option));

    let cytsn = make_ivar_metaroot(
        "Cytsn",
        "cytsn",
        PyType::Str,
        None,
        Some(DocDefault::Option),
    );

    let unicode = make_ivar_metaroot(
        "Unicode",
        "unicode",
        PyType::Tuple(vec![PyType::Int, PyType::new_list(PyType::Str)]),
        None,
        Some(DocDefault::Option),
    );

    let csvbits = make_ivar_metaroot(
        "CSVBits",
        "csvbits",
        PyType::Int,
        None,
        Some(DocDefault::Option),
    );

    let cstot = make_ivar_metaroot(
        "CSTot",
        "cstot",
        PyType::Int,
        None,
        Some(DocDefault::Option),
    );

    let csvflags = make_csvflags();

    let all_subset = [csvbits, cstot, csvflags];

    let last_modifier = make_ivar_metaroot(
        "LastModifier",
        "last_modifier",
        PyType::Datetime,
        None,
        Some(DocDefault::Option),
    );

    let last_modified = make_ivar_metaroot(
        "LastModified",
        "last_modified",
        PyType::Str,
        None,
        Some(DocDefault::Option),
    );

    let originality = make_ivar_metaroot(
        "Originality",
        "originality",
        PyType::Literal(
            "Original".into(),
            vec![
                "NonDataModified".into(),
                "Appended".into(),
                "DataModified".into(),
            ],
        ),
        None,
        Some(DocDefault::Option),
    );

    let all_modified = [last_modifier, last_modified, originality];

    let plateid = make_ivar_metaroot(
        "Plateid",
        "plateid",
        PyType::Str,
        None,
        Some(DocDefault::Option),
    );

    let platename = make_ivar_metaroot(
        "Platename",
        "platename",
        PyType::Str,
        None,
        Some(DocDefault::Option),
    );

    let wellid = make_ivar_metaroot(
        "Wellid",
        "wellid",
        PyType::Str,
        None,
        Some(DocDefault::Option),
    );

    let all_plate = [plateid, platename, wellid];

    let vol = make_ivar_metaroot("Vol", "vol", PyType::Float, None, Some(DocDefault::Option));

    let comp_or_spill = match version {
        Version::FCS2_0 => make_comp(true),
        Version::FCS3_0 => make_comp(false),
        _ => make_spillover(),
    };

    let flowrate = make_ivar_metaroot(
        "Flowrate",
        "flowrate",
        PyType::Str,
        None,
        Some(DocDefault::Option),
    );

    let carrierid = make_ivar_metaroot(
        "Carrierid",
        "carrierid",
        PyType::Str,
        None,
        Some(DocDefault::Option),
    );

    let carriertype = make_ivar_metaroot(
        "Carriertype",
        "carriertype",
        PyType::Str,
        None,
        Some(DocDefault::Option),
    );

    let locationid = make_ivar_metaroot(
        "Locationid",
        "locationid",
        PyType::Str,
        None,
        Some(DocDefault::Option),
    );

    let all_carrier = [carrierid, carriertype, locationid];

    let unstainedcenters = make_unstainedcenters();

    let unstainedinfo = make_ivar_metaroot(
        "UnstainedInfo",
        "unstainedinfo",
        PyType::Str,
        None,
        Some(DocDefault::Option),
    );

    let tr = make_trigger();

    let all_timestamps = match version {
        Version::FCS2_0 => make_timestamps("FCSTime"),
        Version::FCS3_0 => make_timestamps("FCSTime60"),
        Version::FCS3_1 | Version::FCS3_2 => make_timestamps("FCSTime100"),
    };

    let all_datetimes = [make_datetime(true), make_datetime(false)];

    let applied_gates = make_applied_gates(version);

    let nonstandard_keywords = make_nonstandard_keywords();

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
        format!("Represents *TEXT* for an FCS {v} file."),
        vec![],
        coretext_params,
        None,
    );

    let coredataset_doc = DocString::new(
        format!("Represents one dataset in an FCS {v} file."),
        vec![],
        coredataset_params,
        None,
    );

    // methods which apply to both Coretext* and CoreDataset*
    let common = quote! {

        #par_doc
        #[getter]
        fn par(&self) -> usize {
            self.0.par().0
        }

        #rename_temporal_doc
        fn rename_temporal(&mut self, name: #shortname_type) -> Option<#shortname_type> {
            self.0.rename_temporal(name)
        }

        #set_tr_threshold_doc
        fn set_trigger_threshold(&mut self, threshold: u32) -> bool {
            self.0.set_trigger_threshold(threshold)
        }

        #get_all_pnn_doc
        #[getter]
        fn get_all_pnn(&self) -> Vec<#shortname_type> {
            self.0.all_shortnames()
        }

        #[setter]
        fn set_all_pnn(&mut self, names: Vec<#shortname_type>) -> PyResult<()> {
            Ok(self.0.set_all_shortnames(names).void()?)
        }

        #get_set_all_pnn_maybe

        #[getter]
        fn get_layout(&self) -> #layout_ident {
            self.0.layout().clone().into()
        }

        #[setter]
        fn set_layout(&mut self, layout: #layout_ident) -> PyResult<()> {
            self.0.set_layout(layout.into()).py_term_resolve_nowarn()
        }

        #write_text_doc
        fn write_text(&self, path: PathBuf, delim: #textdelim_type) -> PyResult<()> {
            let f = File::options().write(true).create(true).open(path)?;
            let mut h = BufWriter::new(f);
            self.0.h_write_text(&mut h, delim).py_term_resolve_nowarn()
        }

        #set_temporal_mtds
        #unset_temporal_mtd
        #get_set_scale
        #get_set_timestep
        #get_set_all_peak
    };

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

    let coretext_rest = quote! {
        #coretext_set_meas_doc
        #set_meas_method
        #coretext_set_meas_and_layout_doc
        #set_meas_and_layout_method
        #common
        #to_dataset_mtd
        #(#coretext_ivar_methods)*
    };

    let coredataset_rest = quote! {
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

        #coredataset_set_meas_doc
        #set_meas_method

        #coredataset_set_meas_and_layout_doc
        #set_meas_and_layout_method

        #set_meas_and_data_doc
        fn set_measurements_and_data(
            &mut self,
            measurements: #meas_argtype,
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

        #write_dataset_mtd

        #(#coredataset_ivar_methods)*
    };

    let coretext_q = impl_new(
        coretext_name,
        coretext_rstype,
        coretext_doc,
        coretext_new,
        coretext_rest,
    )
    .1;

    let coredataset_q = impl_new(
        coredataset_name,
        coredataset_rstype,
        coredataset_doc,
        coredataset_new,
        coredataset_rest,
    )
    .1;

    quote! {
        #coretext_q
        #coredataset_q
    }
    .into()
}

#[proc_macro]
pub fn impl_new_meas(input: TokenStream) -> TokenStream {
    let info = parse_macro_input!(input as NewMeasInfo);
    let args = info.args;

    let base = if info.is_temporal {
        "Temporal"
    } else {
        "Optical"
    };

    let version_us = info.version.short_underscore();
    let version = info.version.short();

    let name = format!("{base}{version_us}");
    let path = format!("fireflow_core::core::{name}");
    let fun_path = format!("{path}::new_{version_us}");
    let rstype = parse_str::<Path>(path.as_str()).unwrap();
    let fun = parse_str::<Path>(fun_path.as_str()).unwrap();

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
        format!("FCS {version} *$Pn\\** keywords for {lower_basename} measurement."),
        vec![],
        params,
        None,
    );

    let get_set_timestep = if info.version != Version::FCS2_0 && info.is_temporal {
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
    } else if info.version == Version::FCS2_0 {
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
}

impl Parse for NewCoreInfo {
    fn parse(input: ParseStream) -> Result<Self> {
        let coretext_type: Path = input.parse()?;
        let _: Comma = input.parse()?;
        let coredataset_type: Path = input.parse()?;
        let _: Comma = input.parse()?;
        let fun: Path = input.parse()?;
        Ok(Self {
            coretext_type,
            coredataset_type,
            fun,
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
        DocArg::new(at, argname, pytype, desc, None)
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
}

fn make_ivar_metaroot(
    kw: &str,
    name: &str,
    pytype: PyType,
    desc: Option<&str>,
    def: Option<DocDefault>,
) -> ArgData {
    let spath = format!("fireflow_core::text::keywords::{kw}");
    let path = parse_str::<Path>(spath.as_str()).expect("not a valid path");
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
    ArgData {
        doc,
        rstype: full_kw,
        methods: Some(methods),
    }
}

fn make_measurements(version: Version) -> ArgData {
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
    let meas_doc = DocArg::new_param("measurements".into(), meas_pytype.clone(), meas_desc.into());
    let meas_argtype: Path = parse_quote!(PyEithers<#fam_path, #meas_tmp_pyname, #meas_opt_pyname>);

    ArgData::new1(meas_doc, meas_argtype.clone())
}

fn make_layout(version: Version) -> ArgData {
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

    let layout_doc = DocArg::new_param(
        layout_argname.to_string(),
        layout_pytype.clone(),
        layout_desc.into(),
    );

    ArgData::new1(layout_doc, parse_quote!(#layout_ident))
}

fn make_df() -> ArgData {
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
    let fcs_df_path = parse_quote!(fireflow_core::validated::dataframe::FCSDataFrame);

    ArgData::new1(df_doc, fcs_df_path)
}

fn make_analysis() -> ArgData {
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
    ArgData::new(analysis_doc, analysis_rstype, Some(methods))
}

fn make_others() -> ArgData {
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
    ArgData::new(others_doc, others_rstype, Some(methods))
}

fn make_timestamps(time_name: &str) -> [ArgData; 3] {
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
        ArgData {
            doc,
            rstype,
            methods: Some(methods),
        }
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

    let date = ArgData {
        doc: date_doc,
        rstype: date_rstype,
        methods: Some(date_methods),
    };

    [make_time_ivar(true), make_time_ivar(false), date]
}

fn make_datetime(is_start: bool) -> ArgData {
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
    ArgData {
        doc,
        rstype,
        methods: Some(methods),
    }
}

fn make_comp(is_2_0: bool) -> ArgData {
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
        PyType::new_opt(PyType::PyClass("numpy.ndarray".into())),
        desc,
        DocDefault::Option,
    );
    ArgData {
        doc,
        rstype,
        methods: Some(methods),
    }
}

fn make_spillover() -> ArgData {
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
            PyType::PyClass("numpy.ndarray".into()),
        ])),
        "Value for *$SPILLOVER*. First element of tuple the list of measurement \
         names and the second is the matrix. Each measurement name must \
         correspond to a *$PnN*, must be unique, and the length of this list \
         must match the number of rows and columns of the matrix. The matrix \
         must be at least 2x2."
            .into(),
        DocDefault::Option,
    );
    ArgData {
        doc,
        rstype,
        methods: Some(methods),
    }
}

fn make_csvflags() -> ArgData {
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

    ArgData {
        doc,
        rstype,
        methods: Some(methods),
    }
}

fn make_trigger() -> ArgData {
    let rstype = parse_quote! {Option<fireflow_core::text::keywords::Trigger>};

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

    ArgData {
        rstype,
        doc,
        methods: Some(methods),
    }
}

fn make_unstainedcenters() -> ArgData {
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
            self.0.set_unstained_centers(us).py_term_resolve_nowarn()
        }
    };
    ArgData {
        doc,
        rstype,
        methods: Some(methods),
    }
}

fn make_applied_gates(version: Version) -> ArgData {
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

    ArgData {
        doc,
        rstype: parse_quote!(#rstype),
        methods: Some(methods),
    }
}

fn make_nonstandard_keywords() -> ArgData {
    let nsk = quote!(fireflow_core::validated::keys::NonStdKey);
    let fun_arg = parse_quote!(std::collections::HashMap<#nsk, String>);
    let doc = DocArg::new_ivar_def(
        "nonstandard_keywords".into(),
        PyType::new_dict(PyType::Str, PyType::Str),
        "Pairs of non-standard keyword values. Keys must not start with *$*.".into(),
        DocDefault::EmptyDict,
    );
    let methods = quote! {
        #[getter]
        fn get_nonstandard_keywords(&self) -> #fun_arg {
            self.0.metaroot.nonstandard_keywords.clone()
        }

        #[setter]
        fn set_nonstandard_keywords(&mut self, kws: #fun_arg) {
            self.0.metaroot.nonstandard_keywords = kws;
        }
    };
    ArgData {
        doc,
        rstype: fun_arg,
        methods: Some(methods),
    }
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
        PyType::new_union2(base_pytype, PyType::new_unit())
    } else {
        base_pytype
    };

    let doc_type = if optional {
        PyType::new_opt(tmp_pytype)
    } else {
        tmp_pytype
    };

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
    let vu = version.short_underscore();

    if split_version(coredataset_name.as_str()).1 != version {
        panic!("versions do not match");
    }

    let otype = format_ident!("PyOptical{vu}");
    let ttype = format_ident!("PyTemporal{vu}");
    let opt_pytype = PyType::PyClass(format!("Optical{vu}"));
    let tmp_pytype = PyType::PyClass(format!("Temporal{vu}"));

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
    let param_notrunc = DocArg::new_param_def(
        "notrunc".into(),
        PyType::Bool,
        "If ``False``, raise exception if ``range`` must be truncated to fit \
         into measurement type."
            .into(),
        DocDefault::Bool(false),
    );
    let param_col = DocArg::new_param(
        "col".into(),
        PyType::PyClass("polars.Series".into()),
        "Data for measurement. Must be same length as existing columns.".into(),
    );

    let push_meas_doc = |is_optical: bool, meas_type: &PyType, hasdata: bool| {
        let what = if is_optical { "optical" } else { "temporal" };
        let param_meas = DocArg::new_param(
            "meas".into(),
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
            "meas".into(),
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
        if version == Version::FCS3_2 {
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
    let param = DocArg::new_param_def(
        "force".into(),
        PyType::Bool,
        param_desc.into(),
        DocDefault::Bool(false),
    );
    let outputs: Vec<_> = ALL_VERSIONS
        .iter()
        .filter(|&&v| v != version)
        .map(|v| {
            let vsu = v.short_underscore();
            let vs = v.short();
            let fn_name = format_ident!("version_{vsu}");
            let target_type = format_ident!("{base}{vsu}");
            let target_rs_type = target_type.to_string().replace("Py", "");
            let doc = DocString::new(
                format!("Convert to FCS {vs}."),
                vec![sub.into()],
                vec![param.clone()],
                Some(DocReturn::new(
                    PyType::PyClass(target_rs_type),
                    Some(format!("A new class conforming to FCS {vs}")),
                )),
            );
            quote! {
                #[pymethods]
                impl #pytype {
                    #doc
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
    let scale = DocArg::new_ivar_def(
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
    let make_arg = |n: &str, kw: &str, t: PyType| {
        DocArg::new_ivar_def(
            n.into(),
            PyType::new_opt(t),
            format!("The *$Gm{kw}* keyword."),
            DocDefault::Option,
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

    let docstring = doc.doc();
    let signature = doc.sig();

    quote! {
        // TODO not dry
        #docstring
        #[pyclass(name = "GatedMeasurement", eq)]
        #[derive(Clone, From, Into, PartialEq)]
        pub struct PyGatedMeasurement(fireflow_core::text::gating::GatedMeasurement);

        // TODO this also doesn't seem DRY
        #[pymethods]
        impl PyGatedMeasurement {
            #[new]
            #[allow(clippy::too_many_arguments)]
            // TODO this sig is separate from the docstring :/
            #signature
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

#[proc_macro]
pub fn impl_new_ordered_layout(input: TokenStream) -> TokenStream {
    let nbytes = parse_macro_input!(input as LitInt)
        .base10_parse::<usize>()
        .expect("nbytes must be an integer");
    let nbits = nbytes * 8;
    let make_path = |x: &str| {
        let s = format!("fireflow_core::data::{x}");
        let p = parse_str::<Path>(s.as_str()).unwrap();
        (s, p)
    };
    let bitmask_spath = format!("fireflow_core::validated::bitmask::Bitmask{:02}", nbits);
    let bitmask_path = parse_str::<Path>(bitmask_spath.as_str()).unwrap();
    let known_tot_spath = make_path("KnownTot").0;

    let layout_type = format!("OrderedLayout<{bitmask_spath}, {known_tot_spath}>");
    let layout_path = make_path(layout_type.as_str()).1;

    let layout_name = format!("OrderedUint{:02}Layout", nbits);

    let fixed_layout_path = make_path("FixedLayout").1;
    let sizedbyteord_path: Path = parse_quote!(fireflow_core::text::byteord::SizedByteOrd);

    let summary = format!("An {nbits}-bit ordered integer layout");

    let range_param = DocArg::new_param(
        "ranges".into(),
        PyType::new_list(PyType::Int),
        "The range for each measurement. Corresponds to *$PnR* - 1, which \
         implies that the value for each measurement must be less than or \
         equal to the values in ``ranges``. A bitmask will be created which \
         corresponds to one less the next power of 2."
            .into(),
    );

    let is_big_param = DocArg::new_param_def(
        "is_big".into(),
        PyType::Bool,
        "If ``True`` use big endian for encoding values, otherwise use little endian.".into(),
        DocDefault::Bool(false),
    );

    let byteord_param = DocArg::new_param(
        "byteord".into(),
        PyType::new_list(PyType::Int),
        "The byte order to use when encoding values. Must be a list of indices starting at 0."
            .into(),
    );

    let constr_doc = DocString::new(
        format!("{summary}."),
        vec![],
        vec![range_param.clone(), is_big_param],
        None,
    );

    let byteord_doc = DocString::new(
        format!("{summary} with a specific byteord."),
        vec![],
        vec![range_param.clone(), byteord_param],
        None,
    );

    let constr = quote! {
            fn new(ranges: Vec<#bitmask_path>, is_big: bool) -> Self {
                #fixed_layout_path::new_endian_uint(ranges, is_big.into()).into()
            }
    };

    let rest = quote! {
            #byteord_doc
            #[classmethod]
            fn new_ordered(
                _: &Bound<'_, PyType>,
                ranges: Vec<#bitmask_path>,
                byteord: #sizedbyteord_path<#nbytes>,
            ) -> Self {
                #fixed_layout_path::new(ranges, byteord).into()
            }
    };

    impl_new(layout_name, layout_path, constr_doc, constr, rest)
        .1
        .into()
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

fn split_version(name: &str) -> (&str, Version) {
    let (ret, v) = name.split_at(name.len() - 3);
    (
        ret,
        Version::from_short_underscore(v).expect("version should be like 'X_Y'"),
    )
}

fn path_name(p: &Path) -> String {
    p.segments.last().unwrap().ident.to_string()
}

const ALL_VERSIONS: [Version; 4] = [
    Version::FCS2_0,
    Version::FCS3_0,
    Version::FCS3_1,
    Version::FCS3_2,
];

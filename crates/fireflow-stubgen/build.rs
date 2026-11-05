//! Generate stub file for python interface.
//!
//! This is somewhat crude; however, the main thing we need to ensure is correct
//! is the return types since those aren't checked by stubtest. This is ensured
//! by the the proc macro crate that made these stubs; that process has a
//! stuctured pattern that is easy to automate. The only thing left to do here
//! is add the stub/identifier to the pyi file. If we screw this up, stubtest
//! will yell loudly because it is smart enough to check presence/absence of
//! names.

use derive_new::new;
use fireflow_python as ff;
use minijinja::{Environment, UndefinedBehavior, context, path_loader};
use serde::Serialize;

use std::env;
use std::error::Error;
use std::fs::File;
use std::io::BufWriter;
use std::path::{Path, PathBuf};

#[derive(new, Serialize)]
struct ClassAndMethodsStubs {
    class: &'static str,
    methods: Vec<&'static str>,
}

#[allow(clippy::too_many_lines)]
fn render_common_issues(tmpl_env: &Environment, outdir: &Path) -> Result<(), Box<dyn Error>> {
    let path = outdir.join("_pyreflow.pyi");
    let file = BufWriter::new(File::create(&path).unwrap());

    let tmpl = tmpl_env.get_template("_pyreflow.j2")?;

    macro_rules! methods_stubs {
        ($t:ident, $($var:ident),*) => {
            vec![$(ff::$t::$var),*]
        };
    }

    let coretext2_0_method_stubs = methods_stubs!(
        PyCoreTEXT2_0,
        ALL_DETECTOR_TYPES_STUB,
        ALL_DETECTOR_VOLTAGES_STUB,
        ALL_FILTERS_STUB,
        ALL_LONGNAMES_STUB,
        ALL_PEAK_BINS_STUB,
        ALL_PEAK_SIZES_STUB,
        ALL_PERCENTS_EMITTED_STUB,
        ALL_POWERS_STUB,
        ALL_SCALES_STUB,
        ALL_SHORTNAMES_MAYBE_STUB,
        ALL_SHORTNAMES_STUB,
        ALL_WAVELENGTHS_STUB,
        FROM_KWS_STUB,
        INSERT_OPTICAL_STUB,
        INSERT_TEMPORAL_STUB,
        MEASUREMENTS_STUB,
        MEASUREMENT_AT_STUB,
        MEASUREMENT_NAMED_STUB,
        PAR_STUB,
        PUSH_OPTICAL_STUB,
        PUSH_TEMPORAL_STUB,
        SET_MEASUREMENTS_AND_DATA_SCHEMA_STUB,
        SET_NAMED_MEASUREMENTS_AND_DATA_SCHEMA_STUB,
        SET_NAMED_MEASUREMENTS_STUB,
        SET_TEMPORAL_AT_STUB,
        SET_TEMPORAL_STUB,
        SET_TRIGGER_THRESHOLD_STUB,
        STANDARD_KEYWORDS_STUB,
        REMOVE_MEASUREMENT_BY_INDEX_STUB,
        REMOVE_MEASUREMENT_BY_NAME_STUB,
        RENAME_TEMPORAL_STUB,
        REPLACE_OPTICAL_AT_STUB,
        REPLACE_OPTICAL_NAMED_STUB,
        REPLACE_TEMPORAL_AT_STUB,
        REPLACE_TEMPORAL_NAMED_STUB,
        TEMPORAL_STUB,
        TO_DATASET_STUB,
        TO_VERSION_3_0_STUB,
        TO_VERSION_3_1_STUB,
        TO_VERSION_3_2_STUB,
        VERSION_STUB,
        UNSET_MEASUREMENTS_STUB,
        UNSET_TEMPORAL_STUB,
        WRITE_TEXTS_STUB,
        WRITE_TEXT_STUB
    );

    let coretext3_0_method_stubs = methods_stubs!(
        PyCoreTEXT3_0,
        ALL_DETECTOR_TYPES_STUB,
        ALL_DETECTOR_VOLTAGES_STUB,
        ALL_FILTERS_STUB,
        ALL_LONGNAMES_STUB,
        ALL_PEAK_BINS_STUB,
        ALL_PEAK_SIZES_STUB,
        ALL_PERCENTS_EMITTED_STUB,
        ALL_POWERS_STUB,
        ALL_SCALES_STUB,
        ALL_SHORTNAMES_MAYBE_STUB,
        ALL_SHORTNAMES_STUB,
        ALL_WAVELENGTHS_STUB,
        FROM_KWS_STUB,
        INSERT_OPTICAL_STUB,
        INSERT_TEMPORAL_STUB,
        MEASUREMENTS_STUB,
        MEASUREMENT_AT_STUB,
        MEASUREMENT_NAMED_STUB,
        PAR_STUB,
        PUSH_OPTICAL_STUB,
        PUSH_TEMPORAL_STUB,
        SET_MEASUREMENTS_AND_DATA_SCHEMA_STUB,
        SET_NAMED_MEASUREMENTS_AND_DATA_SCHEMA_STUB,
        SET_NAMED_MEASUREMENTS_STUB,
        SET_TEMPORAL_AT_STUB,
        SET_TEMPORAL_STUB,
        SET_TIMESTEP_STUB,
        SET_TRIGGER_THRESHOLD_STUB,
        STANDARD_KEYWORDS_STUB,
        REMOVE_MEASUREMENT_BY_INDEX_STUB,
        REMOVE_MEASUREMENT_BY_NAME_STUB,
        RENAME_TEMPORAL_STUB,
        REPLACE_OPTICAL_AT_STUB,
        REPLACE_OPTICAL_NAMED_STUB,
        REPLACE_TEMPORAL_AT_STUB,
        REPLACE_TEMPORAL_NAMED_STUB,
        TEMPORAL_STUB,
        TIMESTEP_STUB,
        TO_DATASET_STUB,
        TO_VERSION_2_0_STUB,
        TO_VERSION_3_1_STUB,
        TO_VERSION_3_2_STUB,
        VERSION_STUB,
        UNSET_MEASUREMENTS_STUB,
        UNSET_TEMPORAL_STUB,
        WRITE_TEXTS_STUB,
        WRITE_TEXT_STUB
    );

    let coretext3_1_method_stubs = methods_stubs!(
        PyCoreTEXT3_1,
        ALL_CALIBRATIONS_STUB,
        ALL_DETECTOR_TYPES_STUB,
        ALL_DETECTOR_VOLTAGES_STUB,
        ALL_DISPLAYS_STUB,
        ALL_FILTERS_STUB,
        ALL_LONGNAMES_STUB,
        ALL_PEAK_BINS_STUB,
        ALL_PEAK_SIZES_STUB,
        ALL_PERCENTS_EMITTED_STUB,
        ALL_POWERS_STUB,
        ALL_SCALES_STUB,
        ALL_SHORTNAMES_STUB,
        ALL_WAVELENGTHS_STUB,
        FROM_KWS_STUB,
        INSERT_OPTICAL_STUB,
        INSERT_TEMPORAL_STUB,
        MEASUREMENTS_STUB,
        MEASUREMENT_AT_STUB,
        MEASUREMENT_NAMED_STUB,
        PAR_STUB,
        PUSH_OPTICAL_STUB,
        PUSH_TEMPORAL_STUB,
        SET_MEASUREMENTS_AND_DATA_SCHEMA_STUB,
        SET_NAMED_MEASUREMENTS_AND_DATA_SCHEMA_STUB,
        SET_NAMED_MEASUREMENTS_STUB,
        SET_TEMPORAL_AT_STUB,
        SET_TEMPORAL_STUB,
        SET_TIMESTEP_STUB,
        SET_TRIGGER_THRESHOLD_STUB,
        STANDARD_KEYWORDS_STUB,
        REMOVE_MEASUREMENT_BY_INDEX_STUB,
        REMOVE_MEASUREMENT_BY_NAME_STUB,
        RENAME_TEMPORAL_STUB,
        REPLACE_OPTICAL_AT_STUB,
        REPLACE_OPTICAL_NAMED_STUB,
        REPLACE_TEMPORAL_AT_STUB,
        REPLACE_TEMPORAL_NAMED_STUB,
        TEMPORAL_STUB,
        TIMESTEP_STUB,
        TO_DATASET_STUB,
        TO_VERSION_2_0_STUB,
        TO_VERSION_3_0_STUB,
        TO_VERSION_3_2_STUB,
        VERSION_STUB,
        UNSET_MEASUREMENTS_STUB,
        UNSET_TEMPORAL_STUB,
        WRITE_TEXTS_STUB,
        WRITE_TEXT_STUB
    );

    let coretext3_2_method_stubs = methods_stubs!(
        PyCoreTEXT3_2,
        ALL_ANALYTES_STUB,
        ALL_AWH_FEATURES_STUB,
        ALL_CALIBRATIONS_STUB,
        ALL_DETECTOR_NAMES_STUB,
        ALL_DETECTOR_TYPES_STUB,
        ALL_DETECTOR_VOLTAGES_STUB,
        ALL_DISPLAYS_STUB,
        ALL_FEATURES_STUB,
        ALL_FILTERS_STUB,
        ALL_LONGNAMES_STUB,
        ALL_MEASUREMENT_TYPES_STUB,
        ALL_OTHER_FEATURES_STUB,
        ALL_PERCENTS_EMITTED_STUB,
        ALL_POWERS_STUB,
        ALL_SCALES_STUB,
        ALL_SHORTNAMES_STUB,
        ALL_TAGS_STUB,
        ALL_WAVELENGTHS_STUB,
        FROM_KWS_STUB,
        INSERT_OPTICAL_STUB,
        INSERT_TEMPORAL_STUB,
        MEASUREMENTS_STUB,
        MEASUREMENT_AT_STUB,
        MEASUREMENT_NAMED_STUB,
        PAR_STUB,
        PUSH_OPTICAL_STUB,
        PUSH_TEMPORAL_STUB,
        SET_MEASUREMENTS_AND_DATA_SCHEMA_STUB,
        SET_NAMED_MEASUREMENTS_AND_DATA_SCHEMA_STUB,
        SET_NAMED_MEASUREMENTS_STUB,
        SET_TEMPORAL_AT_STUB,
        SET_TEMPORAL_STUB,
        SET_TIMESTEP_STUB,
        SET_TRIGGER_THRESHOLD_STUB,
        STANDARD_KEYWORDS_STUB,
        REMOVE_MEASUREMENT_BY_INDEX_STUB,
        REMOVE_MEASUREMENT_BY_NAME_STUB,
        RENAME_TEMPORAL_STUB,
        REPLACE_OPTICAL_AT_STUB,
        REPLACE_OPTICAL_NAMED_STUB,
        REPLACE_TEMPORAL_AT_STUB,
        REPLACE_TEMPORAL_NAMED_STUB,
        TEMPORAL_STUB,
        TIMESTEP_STUB,
        TO_DATASET_STUB,
        TO_VERSION_2_0_STUB,
        TO_VERSION_3_0_STUB,
        TO_VERSION_3_1_STUB,
        VERSION_STUB,
        UNSET_MEASUREMENTS_STUB,
        UNSET_TEMPORAL_STUB,
        WRITE_TEXTS_STUB,
        WRITE_TEXT_STUB
    );

    let coredataset2_0_method_stubs = methods_stubs!(
        PyCoreDataset2_0,
        ALL_DETECTOR_TYPES_STUB,
        ALL_DETECTOR_VOLTAGES_STUB,
        ALL_FILTERS_STUB,
        ALL_LONGNAMES_STUB,
        ALL_PEAK_BINS_STUB,
        ALL_PEAK_SIZES_STUB,
        ALL_PERCENTS_EMITTED_STUB,
        ALL_POWERS_STUB,
        ALL_SCALES_STUB,
        ALL_SHORTNAMES_MAYBE_STUB,
        ALL_SHORTNAMES_STUB,
        ALL_WAVELENGTHS_STUB,
        CHECK_RANGES_STUB,
        FROM_KWS_STUB,
        INSERT_OPTICAL_STUB,
        INSERT_TEMPORAL_STUB,
        MEASUREMENTS_STUB,
        MEASUREMENT_AT_STUB,
        MEASUREMENT_NAMED_STUB,
        PAR_STUB,
        PUSH_OPTICAL_STUB,
        PUSH_TEMPORAL_STUB,
        SET_MEASUREMENTS_AND_DATA_STUB,
        SET_MEASUREMENTS_AND_DATA_SCHEMA_STUB,
        SET_MEASUREMENTS_DATA_SCHEMA_AND_DATA_STUB,
        SET_NAMED_MEASUREMENTS_AND_DATA_STUB,
        SET_NAMED_MEASUREMENTS_AND_DATA_SCHEMA_STUB,
        SET_NAMED_MEASUREMENTS_STUB,
        SET_TEMPORAL_AT_STUB,
        SET_TEMPORAL_STUB,
        SET_TRIGGER_THRESHOLD_STUB,
        STANDARD_KEYWORDS_STUB,
        REMOVE_MEASUREMENT_BY_INDEX_STUB,
        REMOVE_MEASUREMENT_BY_NAME_STUB,
        RENAME_TEMPORAL_STUB,
        REPLACE_OPTICAL_AT_STUB,
        REPLACE_OPTICAL_NAMED_STUB,
        REPLACE_TEMPORAL_AT_STUB,
        REPLACE_TEMPORAL_NAMED_STUB,
        TEMPORAL_STUB,
        TO_TEXT_STUB,
        TO_VERSION_3_0_STUB,
        TO_VERSION_3_1_STUB,
        TO_VERSION_3_2_STUB,
        VERSION_STUB,
        UNSET_DATA_STUB,
        UNSET_TEMPORAL_STUB,
        WRITE_TEXT_STUB,
        WRITE_DATASET_STUB
    );

    let coredataset3_0_method_stubs = methods_stubs!(
        PyCoreDataset3_0,
        ALL_DETECTOR_TYPES_STUB,
        ALL_DETECTOR_VOLTAGES_STUB,
        ALL_FILTERS_STUB,
        ALL_LONGNAMES_STUB,
        ALL_PEAK_BINS_STUB,
        ALL_PEAK_SIZES_STUB,
        ALL_PERCENTS_EMITTED_STUB,
        ALL_POWERS_STUB,
        ALL_SCALES_STUB,
        ALL_SHORTNAMES_MAYBE_STUB,
        ALL_SHORTNAMES_STUB,
        ALL_WAVELENGTHS_STUB,
        CHECK_RANGES_STUB,
        FROM_KWS_STUB,
        INSERT_OPTICAL_STUB,
        INSERT_TEMPORAL_STUB,
        MEASUREMENTS_STUB,
        MEASUREMENT_AT_STUB,
        MEASUREMENT_NAMED_STUB,
        PAR_STUB,
        PUSH_OPTICAL_STUB,
        PUSH_TEMPORAL_STUB,
        SET_MEASUREMENTS_AND_DATA_STUB,
        SET_MEASUREMENTS_AND_DATA_SCHEMA_STUB,
        SET_MEASUREMENTS_DATA_SCHEMA_AND_DATA_STUB,
        SET_NAMED_MEASUREMENTS_AND_DATA_STUB,
        SET_NAMED_MEASUREMENTS_AND_DATA_SCHEMA_STUB,
        SET_NAMED_MEASUREMENTS_STUB,
        SET_TEMPORAL_AT_STUB,
        SET_TEMPORAL_STUB,
        SET_TIMESTEP_STUB,
        SET_TRIGGER_THRESHOLD_STUB,
        STANDARD_KEYWORDS_STUB,
        REMOVE_MEASUREMENT_BY_INDEX_STUB,
        REMOVE_MEASUREMENT_BY_NAME_STUB,
        RENAME_TEMPORAL_STUB,
        REPLACE_OPTICAL_AT_STUB,
        REPLACE_OPTICAL_NAMED_STUB,
        REPLACE_TEMPORAL_AT_STUB,
        REPLACE_TEMPORAL_NAMED_STUB,
        TEMPORAL_STUB,
        TIMESTEP_STUB,
        TO_TEXT_STUB,
        TO_VERSION_2_0_STUB,
        TO_VERSION_3_1_STUB,
        TO_VERSION_3_2_STUB,
        VERSION_STUB,
        UNSET_DATA_STUB,
        UNSET_TEMPORAL_STUB,
        WRITE_TEXT_STUB,
        WRITE_DATASET_STUB
    );

    let coredataset3_1_method_stubs = methods_stubs!(
        PyCoreDataset3_1,
        ALL_CALIBRATIONS_STUB,
        ALL_DETECTOR_TYPES_STUB,
        ALL_DETECTOR_VOLTAGES_STUB,
        ALL_DISPLAYS_STUB,
        ALL_FILTERS_STUB,
        ALL_LONGNAMES_STUB,
        ALL_PEAK_BINS_STUB,
        ALL_PEAK_SIZES_STUB,
        ALL_PERCENTS_EMITTED_STUB,
        ALL_POWERS_STUB,
        ALL_SCALES_STUB,
        ALL_SHORTNAMES_STUB,
        ALL_WAVELENGTHS_STUB,
        CHECK_RANGES_STUB,
        FROM_KWS_STUB,
        INSERT_OPTICAL_STUB,
        INSERT_TEMPORAL_STUB,
        MEASUREMENTS_STUB,
        MEASUREMENT_AT_STUB,
        MEASUREMENT_NAMED_STUB,
        PAR_STUB,
        PUSH_OPTICAL_STUB,
        PUSH_TEMPORAL_STUB,
        SET_MEASUREMENTS_AND_DATA_STUB,
        SET_MEASUREMENTS_AND_DATA_SCHEMA_STUB,
        SET_MEASUREMENTS_DATA_SCHEMA_AND_DATA_STUB,
        SET_NAMED_MEASUREMENTS_AND_DATA_STUB,
        SET_NAMED_MEASUREMENTS_AND_DATA_SCHEMA_STUB,
        SET_NAMED_MEASUREMENTS_STUB,
        SET_TEMPORAL_AT_STUB,
        SET_TEMPORAL_STUB,
        SET_TIMESTEP_STUB,
        SET_TRIGGER_THRESHOLD_STUB,
        STANDARD_KEYWORDS_STUB,
        REMOVE_MEASUREMENT_BY_INDEX_STUB,
        REMOVE_MEASUREMENT_BY_NAME_STUB,
        RENAME_TEMPORAL_STUB,
        REPLACE_OPTICAL_AT_STUB,
        REPLACE_OPTICAL_NAMED_STUB,
        REPLACE_TEMPORAL_AT_STUB,
        REPLACE_TEMPORAL_NAMED_STUB,
        TEMPORAL_STUB,
        TIMESTEP_STUB,
        TO_TEXT_STUB,
        TO_VERSION_2_0_STUB,
        TO_VERSION_3_0_STUB,
        TO_VERSION_3_2_STUB,
        VERSION_STUB,
        UNSET_DATA_STUB,
        UNSET_TEMPORAL_STUB,
        WRITE_TEXT_STUB,
        WRITE_DATASET_STUB
    );

    let coredataset3_2_method_stubs = methods_stubs!(
        PyCoreDataset3_2,
        ALL_ANALYTES_STUB,
        ALL_AWH_FEATURES_STUB,
        ALL_CALIBRATIONS_STUB,
        ALL_DETECTOR_NAMES_STUB,
        ALL_DETECTOR_TYPES_STUB,
        ALL_DETECTOR_VOLTAGES_STUB,
        ALL_DISPLAYS_STUB,
        ALL_FEATURES_STUB,
        ALL_FILTERS_STUB,
        ALL_LONGNAMES_STUB,
        ALL_MEASUREMENT_TYPES_STUB,
        ALL_OTHER_FEATURES_STUB,
        ALL_PERCENTS_EMITTED_STUB,
        ALL_POWERS_STUB,
        ALL_SCALES_STUB,
        ALL_SHORTNAMES_STUB,
        ALL_TAGS_STUB,
        ALL_WAVELENGTHS_STUB,
        CHECK_RANGES_STUB,
        FROM_KWS_STUB,
        INSERT_OPTICAL_STUB,
        INSERT_TEMPORAL_STUB,
        MEASUREMENTS_STUB,
        MEASUREMENT_AT_STUB,
        MEASUREMENT_NAMED_STUB,
        PAR_STUB,
        PUSH_OPTICAL_STUB,
        PUSH_TEMPORAL_STUB,
        SET_MEASUREMENTS_AND_DATA_STUB,
        SET_MEASUREMENTS_AND_DATA_SCHEMA_STUB,
        SET_MEASUREMENTS_DATA_SCHEMA_AND_DATA_STUB,
        SET_NAMED_MEASUREMENTS_AND_DATA_STUB,
        SET_NAMED_MEASUREMENTS_AND_DATA_SCHEMA_STUB,
        SET_NAMED_MEASUREMENTS_STUB,
        SET_TEMPORAL_AT_STUB,
        SET_TEMPORAL_STUB,
        SET_TIMESTEP_STUB,
        SET_TRIGGER_THRESHOLD_STUB,
        STANDARD_KEYWORDS_STUB,
        REMOVE_MEASUREMENT_BY_INDEX_STUB,
        REMOVE_MEASUREMENT_BY_NAME_STUB,
        RENAME_TEMPORAL_STUB,
        REPLACE_OPTICAL_AT_STUB,
        REPLACE_OPTICAL_NAMED_STUB,
        REPLACE_TEMPORAL_AT_STUB,
        REPLACE_TEMPORAL_NAMED_STUB,
        TEMPORAL_STUB,
        TIMESTEP_STUB,
        TO_TEXT_STUB,
        TO_VERSION_2_0_STUB,
        TO_VERSION_3_0_STUB,
        TO_VERSION_3_1_STUB,
        VERSION_STUB,
        UNSET_DATA_STUB,
        UNSET_TEMPORAL_STUB,
        WRITE_TEXT_STUB,
        WRITE_DATASET_STUB
    );

    let core_stubs = vec![
        ClassAndMethodsStubs::new(ff::PyCoreTEXT2_0::CLASS_STUB, coretext2_0_method_stubs),
        ClassAndMethodsStubs::new(ff::PyCoreTEXT3_0::CLASS_STUB, coretext3_0_method_stubs),
        ClassAndMethodsStubs::new(ff::PyCoreTEXT3_1::CLASS_STUB, coretext3_1_method_stubs),
        ClassAndMethodsStubs::new(ff::PyCoreTEXT3_2::CLASS_STUB, coretext3_2_method_stubs),
        ClassAndMethodsStubs::new(
            ff::PyCoreDataset2_0::CLASS_STUB,
            coredataset2_0_method_stubs,
        ),
        ClassAndMethodsStubs::new(
            ff::PyCoreDataset3_0::CLASS_STUB,
            coredataset3_0_method_stubs,
        ),
        ClassAndMethodsStubs::new(
            ff::PyCoreDataset3_1::CLASS_STUB,
            coredataset3_1_method_stubs,
        ),
        ClassAndMethodsStubs::new(
            ff::PyCoreDataset3_2::CLASS_STUB,
            coredataset3_2_method_stubs,
        ),
    ];

    let meas_stubs = [
        ClassAndMethodsStubs::new(ff::PyOptical2_0::CLASS_STUB, vec![]),
        ClassAndMethodsStubs::new(ff::PyOptical3_0::CLASS_STUB, vec![]),
        ClassAndMethodsStubs::new(ff::PyOptical3_1::CLASS_STUB, vec![]),
        ClassAndMethodsStubs::new(
            ff::PyOptical3_2::CLASS_STUB,
            vec![ff::PyOptical3_2::AWH_FEATURE_STUB],
        ),
        ClassAndMethodsStubs::new(ff::PyTemporal2_0::CLASS_STUB, vec![]),
        ClassAndMethodsStubs::new(ff::PyTemporal3_0::CLASS_STUB, vec![]),
        ClassAndMethodsStubs::new(ff::PyTemporal3_1::CLASS_STUB, vec![]),
        ClassAndMethodsStubs::new(ff::PyTemporal3_2::CLASS_STUB, vec![]),
    ];

    macro_rules! uint_stubs {
        ($t:ident) => {
            ClassAndMethodsStubs::new(
                ff::$t::CLASS_STUB,
                vec![ff::$t::DATATYPE_STUB, ff::$t::IS_FLOAT_STUB],
            )
        };
    }

    macro_rules! float_stubs {
        ($t:ident) => {
            ClassAndMethodsStubs::new(
                ff::$t::CLASS_STUB,
                vec![
                    ff::$t::DATATYPE_STUB,
                    ff::$t::IS_FLOAT_STUB,
                    ff::$t::BYTE_WIDTH_STUB,
                ],
            )
        };
    }

    let schema_stubs = vec![
        ClassAndMethodsStubs::new(
            ff::PyFixedAsciiDataSchema::CLASS_STUB,
            vec![
                ff::PyFixedAsciiDataSchema::DATATYPE_STUB,
                ff::PyFixedAsciiDataSchema::CHAR_WIDTHS_STUB,
            ],
        ),
        ClassAndMethodsStubs::new(
            ff::PyDelimAsciiDataSchema::CLASS_STUB,
            vec![ff::PyDelimAsciiDataSchema::DATATYPE_STUB],
        ),
        uint_stubs!(PyOrderedUintDataSchema),
        float_stubs!(PyOrderedF32DataSchema),
        float_stubs!(PyOrderedF64DataSchema),
        uint_stubs!(PySingleUintDataSchema),
        float_stubs!(PyBigLittleF32DataSchema),
        float_stubs!(PyBigLittleF64DataSchema),
        ClassAndMethodsStubs::new(
            ff::PyVariableUintDataSchema::CLASS_STUB,
            vec![
                ff::PyVariableUintDataSchema::IS_FLOAT_STUB,
                ff::PyVariableUintDataSchema::DATATYPE_STUB,
                ff::PyVariableUintDataSchema::BYTE_WIDTHS_STUB,
            ],
        ),
        ClassAndMethodsStubs::new(
            ff::PyMixedDataSchema::CLASS_STUB,
            vec![ff::PyMixedDataSchema::BYTE_WIDTHS_STUB],
        ),
    ];

    let gating_stubs = vec![
        ff::PyUnivariateRegion2_0::CLASS_STUB,
        ff::PyUnivariateRegion3_0::CLASS_STUB,
        ff::PyUnivariateRegion3_2::CLASS_STUB,
        ff::PyBivariateRegion2_0::CLASS_STUB,
        ff::PyBivariateRegion3_0::CLASS_STUB,
        ff::PyBivariateRegion3_2::CLASS_STUB,
        ff::PyGatedMeasurement::CLASS_STUB,
    ];

    let function_stubs = [
        ff::FCS_READ_HEADER_STUB,
        ff::FCS_READ_FLAT_TEXT_STUB,
        ff::FCS_READ_FLAT_TEXTS_STUB,
        ff::FCS_READ_STD_TEXT_STUB,
        ff::FCS_READ_STD_TEXTS_STUB,
        ff::FCS_READ_FLAT_DATASET_STUB,
        ff::FCS_READ_FLAT_DATASETS_STUB,
        ff::FCS_READ_STD_DATASET_STUB,
        ff::FCS_READ_STD_DATASETS_STUB,
        ff::FCS_READ_FLAT_DATASET_WITH_KEYWORDS_STUB,
        ff::FCS_SUMMARIZE_STUB,
        ff::FCS_WRITE_DATASETS_STUB,
        ff::FCS_SCAN_DATASET_BOUNDARIES_STUB,
    ];

    let misc_class_stubs = vec![
        ff::PyHeader::CLASS_STUB,
        ff::PyFinalHeaderOffsets::CLASS_STUB,
        ff::PyOriginalHeaderOffsets::CLASS_STUB,
        ff::PyValidKeywords::CLASS_STUB,
        ff::PyStdTEXTDiagnostics::CLASS_STUB,
        ff::PyDataSchemaDiagnostics::CLASS_STUB,
        ff::PyDatasetOffsets::CLASS_STUB,
        ff::PyFlatTEXTOutput::CLASS_STUB,
        ff::PyHeaderAndSuppOffsets::CLASS_STUB,
        ff::PyFlatDatasetOutput::CLASS_STUB,
        ff::PyFlatTEXTDiagnostics::CLASS_STUB,
        ff::PySplitTEXTDiagnostics::CLASS_STUB,
        ff::PyFlatDatasetFromKwsOutput::CLASS_STUB,
        ff::PyNewFlatDatasetFromKwsOutput::CLASS_STUB,
        ff::PySuppTEXTOffsetsOutput::CLASS_STUB,
        ff::PyTEXTOffsetsOrigin::CLASS_STUB,
        ff::PyRepairDiagnostics::CLASS_STUB,
        ff::PyDatasetDiagnostics::CLASS_STUB,
        ff::PyIntraSegmentDarkBytes::CLASS_STUB,
        ff::PyKeywordVersionScore::CLASS_STUB,
        ff::PyStdTEXTOutput::CLASS_STUB,
        ff::PyStdDatasetOutput::CLASS_STUB,
        ff::PyStdDatasetFromKwsOutput::CLASS_STUB,
        ff::PyNewStdDatasetFromKwsOutput::CLASS_STUB,
        ff::PyDatasetSummary::CLASS_STUB,
        ff::PyBuildInfo::CLASS_STUB,
        ff::PyHeaderOffsetsOverflow::CLASS_STUB,
        ff::PyTextOffsetsOverflow::CLASS_STUB,
        ff::PySuppOffsetsOverflow::CLASS_STUB,
        ff::PyHeaderToHeaderOffsetsOverlap::CLASS_STUB,
        ff::PyTextToHeaderOffsetsOverlap::CLASS_STUB,
        ff::PySuppToHeaderOffsetsOverlap::CLASS_STUB,
        ff::PyTextToHeaderOrSuppOffsetsOverlap::CLASS_STUB,
    ];

    let class_methods_stubs: Vec<_> = core_stubs
        .into_iter()
        .chain(meas_stubs)
        .chain(schema_stubs)
        .collect();

    let context = context!(
        class_methods_stubs => class_methods_stubs,
        gating_stubs => gating_stubs,
        function_stubs => function_stubs,
        misc_class_stubs => misc_class_stubs,
    );

    let _ = tmpl.render_captured_to(context, file)?;

    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let manifest_dir = PathBuf::from(
        env::var_os("CARGO_MANIFEST_DIR")
            .expect("Cargo must set CARGO_MANIFEST_DIR for build scripts"),
    );

    let mut tmpl_env = Environment::new();
    tmpl_env.set_undefined_behavior(UndefinedBehavior::Strict);
    tmpl_env.set_loader(path_loader(manifest_dir.join("templates")));

    let outdir_var = env::var("OUT_DIR").expect("OUT_DIR should be defined in environment");
    let outdir = Path::new(&outdir_var);

    render_common_issues(&tmpl_env, outdir)?;

    Ok(())
}

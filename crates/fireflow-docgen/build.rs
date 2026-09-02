use fireflow_types::args::underscore as fa;
use minijinja::{Environment, UndefinedBehavior, context, path_loader};

use std::env;
use std::error::Error;
use std::fs::File;
use std::io::BufWriter;
use std::path::{Path, PathBuf};

fn render_common_issues(tmpl_env: &Environment, outdir: &Path) -> Result<(), Box<dyn Error>> {
    let path = outdir.join("COMMON_ISSUES.md");
    let file = BufWriter::new(File::create(&path).unwrap());

    let tmpl = tmpl_env.get_template("COMMON_ISSUES.j2")?;

    let context = context!(
        overlap_correction_limit => fa::OVERLAP_CORRECTION_LIMIT,
        squish_offsets => fa::SQUISH_OFFSETS,
        allow_pseudoempty => fa::ALLOW_PSEUDOEMPTY,
        allow_missing_required_offsets => fa::ALLOW_MISSING_REQUIRED_OFFSETS,
        allow_duplicated_supp_text => fa::ALLOW_DUPLICATED_SUPP_TEXT,
        dataset_overflow_limit => fa::DATASET_OVERFLOW_LIMIT,
        text_correction => fa::TEXT_CORR,
        data_correction => fa::DATA_CORR,
        analysis_correction => fa::ANALYSIS_CORR,
        other_corrections => fa::OTHER_CORRS,
        text_data_correction => fa::TEXT_DATA_CORR,
        text_analysis_correction => fa::TEXT_ANALYSIS_CORR,
        supp_text_correction => fa::SUPP_TEXT_COR,
        allow_header_text_offset_mismatch => fa::ALLOW_HEADER_TEXT_OFFSET_MISMATCH,
        ignore_text_data_offsets => fa::IGNORE_TEXT_DATA_OFFSETS,
        ignore_text_analysis_offsets => fa::IGNORE_TEXT_ANALYSIS_OFFSETS,
        ignore_supp_text => fa::IGNORE_SUPP_TEXT,
        allow_missing_supp_text => fa::ALLOW_MISSING_SUPP_TEXT,
        allow_missing_nextdata => fa::ALLOW_MISSING_NEXTDATA,
        scan => fa::SCAN,
        version_override => fa::VERSION_OVERRIDE,
        ignore_standard_keys => fa::IGNORE_STD_KEYS,
        demote_from_standard => fa::DEMOTE_FROM_STD,
        promote_to_standard => fa::PROMOTE_TO_STD,
        append_standard_keywords => fa::APPEND_STD_KEYWORDS,
        substitute_standard_key_values => fa::SUB_STD_KEY_VALS,
        rename_standard_keys => fa::RENAME_STD_KEYS,
        process_optional_failure => fa::PROCESS_OPTIONAL_FAILURE,
        process_pseudostandard => fa::PROCESS_PSEUDOSTANDARD,
        process_hyper_par => fa::PROCESS_HYPER_PAR,
        process_other_version => fa::PROCESS_OTHER_VERSION,
        process_extra_timestep => fa::PROCESS_EXTRA_TIMESTEP,
        dedup_measurement_names => fa::DEDUP_MEAS_NAMES,
        spillover_measurement_mode => fa::SPILLOVER_MEASUREMENT_MODE,
        date_pattern => fa::DATE_PATTERN,
        time_pattern => fa::TIME_PATTERN,
        datetime_pattern => fa::DATETIME_PATTERN,
        last_modified_pattern => fa::LAST_MODIFIED_PATTERN,
        force_linear_scale => fa::FORCE_LINEAR_SCALE,
        fix_log_scale_offsets => fa::FIX_LOG_SCALE_OFFSETS,
        byteord_override => fa::BYTEORD_OVERRIDE,
        int_width_override => fa::INT_WIDTH_OVERRIDE,
        disallow_range_truncation => fa::DISALLOW_RANGE_TRUNCATION,
        allow_other_feature => fa::ALLOW_OTHER_FEATURE,
        trim_value_whitespace => fa::TRIM_VALUE_WHITESPACE,
        trim_intra_value_whitespace => fa::TRIM_INTRA_VALUE_WHITESPACE,
        replace_standard_key_values => fa::REPLACE_STD_KEY_VALS,
        time_meas_pattern => fa::TIME_MEAS_PATTERN,
        allow_missing_time => fa::ALLOW_MISSING_TIME,
        add_missing_timestep => fa::ADD_MISSING_TIMESTEP,
        ignore_time_optical_keys => fa::IGNORE_OPTICAL_ONLY_KEYS,
        process_time_optical_keys => fa::PROCESS_OPTICAL_ONLY_KEYS,
        guess_other_width => fa::GUESS_OTHER_WIDTH,
        other_width => fa::OTHER_WIDTH,
        max_other => fa::MAX_OTHER,
        allow_odd_tokens => fa::ALLOW_ODD_TOKENS,
        allow_even_delims => fa::ALLOW_EVEN_DELIMS,
        delim_escape_mode => fa::DELIM_ESCAPE_MODE,
        allow_non_ascii_delim => fa::ALLOW_NON_ASCII_DELIM,
        allow_delim_at_boundary => fa::ALLOW_DELIM_AT_BOUNDARY,
        allow_supp_text_own_delim => fa::ALLOW_SUPP_TEXT_OWN_DELIM,
        allow_nonunique => fa::ALLOW_NONUNIQUE,
        allow_repair_non_unique => fa::ALLOW_REPAIR_NON_UNIQUE,
        allow_empty_keys => fa::ALLOW_EMPTY_KEYS,
        allow_non_ascii_keys => fa::ALLOW_NON_ASCII_KEYS,
        allow_non_utf8_values => fa::ALLOW_NON_UTF8_VALUES,
        use_encoding => fa::USE_ENCODING,
        allow_tot_mismatch => fa::ALLOW_TOT_MISMATCH,
        overlap_correction_limit => fa::OVERLAP_CORRECTION_LIMIT,
        data_remainder_limit => fa::DATA_REMAINDER_LIMIT,
        allow_uneven_event_width => fa::ALLOW_UNEVEN_EVENT_WIDTH,
        over_bitmask_action => fa::OVER_BITMASK_ACTION,
        over_range_action => fa::OVER_RANGE_ACTION,
        allow_missing_crc => fa::ALLOW_MISSING_CRC,
        compute_crc => fa::COMPUTE_CRC,
        allow_mismatch_crc => fa::ALLOW_MISMATCH_CRC,
        read_intra_segment_dark_bytes => fa::READ_INTRA_SEGMENT_DARK_BYTES,
        read_post_dataset_dark_bytes => fa::READ_POST_DATASET_DARK_BYTES,
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

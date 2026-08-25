pub mod dash;
pub mod underscore;

#[macro_export]
macro_rules! make_args {
    ($replace:expr) => {
        macro_rules! string_arg {
            ($conf:path, $field:ident) => {{
                // compile-time existence check; fails if field is renamed
                const _: usize = std::mem::offset_of!($conf, $field);
                const_format::str_replace!(stringify!($field), "_", $replace)
            }};
        }

        // header config flags

        macro_rules! header_arg {
            ($field:ident) => {
                string_arg!($crate::config::ReadHeaderInnerConfig, $field)
            };
        }

        pub const TEXT_CORR: &str = header_arg!(text_correction);
        pub const DATA_CORR: &str = header_arg!(data_correction);
        pub const ANALYSIS_CORR: &str = header_arg!(analysis_correction);
        pub const OTHER_CORR: &str = header_arg!(other_corrections);
        pub const MAX_OTHER: &str = header_arg!(max_other);
        pub const OTHER_WIDTH: &str = header_arg!(other_width);
        pub const GUESS_OTHER_WIDTH: &str = header_arg!(guess_other_width);
        pub const SQUISH_OFFSETS: &str = header_arg!(squish_offsets);

        // offset config flags

        macro_rules! offset_arg {
            ($field:ident) => {
                string_arg!($crate::config::ReadOffsetConfig, $field)
            };
        }

        pub const ALLOW_PSEUDOEMPTY: &str = offset_arg!(allow_pseudoempty);
        pub const DATASET_OVERFLOW_LIMIT: &str = offset_arg!(dataset_overflow_limit);
        pub const OVERLAP_CORRECTION_LIMIT: &str = offset_arg!(overlap_correction_limit);

        // flat text config flags

        macro_rules! header_text_arg {
            ($field:ident) => {
                string_arg!($crate::config::ReadHeaderAndTEXTConfig, $field)
            };
        }

        pub const VERSION_OVERRIDE: &str = header_text_arg!(version_override);
        pub const SUPP_TEXT_COR: &str = header_text_arg!(supp_text_correction);
        pub const NEXTDATA_COR: &str = header_text_arg!(nextdata_correction);
        pub const ALLOW_DUPLICATED_SUPP_TEXT: &str = header_text_arg!(allow_duplicated_supp_text);
        pub const IGNORE_SUPP_TEXT: &str = header_text_arg!(ignore_supp_text);
        pub const DELIM_ESCAPE_MODE: &str = header_text_arg!(delim_escape_mode);
        pub const ALLOW_NON_ASCII_DELIM: &str = header_text_arg!(allow_non_ascii_delim);
        pub const ALLOW_EVEN_DELIMS: &str = header_text_arg!(allow_even_delims);
        pub const ALLOW_NONUNIQUE: &str = header_text_arg!(allow_nonunique);
        pub const ALLOW_ODD_TOKENS: &str = header_text_arg!(allow_odd_tokens);
        pub const ALLOW_EMPTY_KEYS: &str = header_text_arg!(allow_empty_keys);
        pub const ALLOW_DELIM_AT_BOUNDARY: &str = header_text_arg!(allow_delim_at_boundary);
        pub const USE_ENCODING: &str = header_text_arg!(use_encoding);
        pub const ALLOW_NON_ASCII_KEYS: &str = header_text_arg!(allow_non_ascii_keys);
        pub const ALLOW_NON_UTF8_VALUES: &str = header_text_arg!(allow_non_utf8_values);
        pub const ALLOW_MISSING_SUPP_TEXT: &str = header_text_arg!(allow_missing_supp_text);
        pub const ALLOW_SUPP_TEXT_OWN_DELIM: &str = header_text_arg!(allow_supp_text_own_delim);
        pub const ALLOW_MISSING_NEXTDATA: &str = header_text_arg!(allow_missing_nextdata);
        pub const TRIM_VALUE_WHITESPACE: &str = header_text_arg!(trim_value_whitespace);

        // std keyword config flags

        macro_rules! std_kw_arg {
            ($field:ident) => {
                string_arg!(
                    $crate::config::ReadStdKeywordsConfig_::<(), (), (), (), ()>,
                    $field
                )
            };
        }

        pub const DEDUP_MEAS_NAMES: &str = std_kw_arg!(dedup_measurement_names);
        pub const TRIM_INTRA_VALUE_WHITESPACE: &str = std_kw_arg!(trim_intra_value_whitespace);
        pub const TIME_MEAS_PATTERN: &str = std_kw_arg!(time_meas_pattern);
        pub const ALLOW_MISSING_TIME: &str = std_kw_arg!(allow_missing_time);
        pub const ADD_MISSING_TIMESTEP: &str = std_kw_arg!(add_missing_timestep);
        pub const FORCE_LINEAR_SCALE: &str = std_kw_arg!(force_linear_scale);
        pub const IGNORE_OPTICAL_ONLY_KEYS: &str = std_kw_arg!(ignore_optical_only_keys);
        pub const PROCESS_OPTICAL_ONLY_KEYS: &str = std_kw_arg!(process_optical_only_keys);
        pub const SPILLOVER_MEASUREMENT_MODE: &str = std_kw_arg!(spillover_measurement_mode);
        pub const DATE_PATTERN: &str = std_kw_arg!(date_pattern);
        pub const TIME_PATTERN: &str = std_kw_arg!(time_pattern);
        pub const DATETIME_PATTERN: &str = std_kw_arg!(datetime_pattern);
        pub const LAST_MODIFIED_PATTERN: &str = std_kw_arg!(last_modified_pattern);
        pub const ALLOW_OTHER_FEATURE: &str = std_kw_arg!(allow_other_feature);
        pub const PROCESS_PSEUDOSTANDARD: &str = std_kw_arg!(process_pseudostandard);
        pub const PROCESS_HYPER_PAR: &str = std_kw_arg!(process_hyper_par);
        pub const PROCESS_OTHER_VERSION: &str = std_kw_arg!(process_other_version);
        pub const PROCESS_EXTRA_TIMESTEP: &str = std_kw_arg!(process_extra_timestep);
        pub const FIX_LOG_SCALE_OFFSETS: &str = std_kw_arg!(fix_log_scale_offsets);
        pub const DISALLOW_LOCALTIME: &str = std_kw_arg!(disallow_localtime);

        // data keyword config flags

        macro_rules! data_kw_arg {
            ($field:ident) => {
                string_arg!(
                    $crate::config::ReadDataKeywordsConfig_::<(), (), (), (), (), (), ()>,
                    $field
                )
            };
        }

        pub const IGNORE_STD_KEYS: &str = data_kw_arg!(ignore_standard_keys);
        pub const PROMOTE_TO_STD: &str = data_kw_arg!(promote_to_standard);
        pub const DEMOTE_FROM_STD: &str = data_kw_arg!(demote_from_standard);
        pub const RENAME_STD_KEYS: &str = data_kw_arg!(rename_standard_keys);
        pub const REPLACE_STD_KEY_VALS: &str = data_kw_arg!(replace_standard_key_values);
        pub const APPEND_STD_KEYWORDS: &str = data_kw_arg!(append_standard_keywords);
        pub const SUB_STD_KEY_VALS: &str = data_kw_arg!(substitute_standard_key_values);
        pub const ALLOW_REPAIR_NON_UNIQUE: &str = data_kw_arg!(allow_repair_non_unique);
        pub const TEXT_DATA_CORR: &str = data_kw_arg!(text_data_correction);
        pub const TEXT_ANALYSIS_CORR: &str = data_kw_arg!(text_analysis_correction);
        pub const IGNORE_TEXT_DATA_OFFSETS: &str = data_kw_arg!(ignore_text_data_offsets);
        pub const IGNORE_TEXT_ANALYSIS_OFFSETS: &str = data_kw_arg!(ignore_text_analysis_offsets);
        pub const ALLOW_HEADER_TEXT_OFFSET_MISMATCH: &str =
            data_kw_arg!(allow_header_text_offset_mismatch);
        pub const ALLOW_MISSING_REQUIRED_OFFSETS: &str =
            data_kw_arg!(allow_missing_required_offsets);
        pub const PROCESS_OPTIONAL_FAILURE: &str = data_kw_arg!(process_optional_failure);
        pub const INT_WIDTH_OVERRIDE: &str = data_kw_arg!(int_width_override);
        pub const BYTEORD_OVERRIDE: &str = data_kw_arg!(byteord_override);
        pub const DISALLOW_RANGE_TRUNCATION: &str = data_kw_arg!(disallow_range_truncation);

        // read data config flags

        macro_rules! dataset_arg {
            ($field:ident) => {
                string_arg!($crate::config::ReadDatasetConfig, $field)
            };
        }

        pub const DATA_REMAINDER_LIMIT: &str = dataset_arg!(data_remainder_limit);
        pub const ALLOW_UNEVEN_EVENT_WIDTH: &str = dataset_arg!(allow_uneven_event_width);
        pub const ALLOW_TOT_MISMATCH: &str = dataset_arg!(allow_tot_mismatch);
        pub const OVER_BITMASK_ACTION: &str = dataset_arg!(over_bitmask_action);
        pub const OVER_RANGE_ACTION: &str = dataset_arg!(over_range_action);
        pub const ALLOW_MISSING_CRC: &str = dataset_arg!(allow_missing_crc);
        pub const ALLOW_MISMATCH_CRC: &str = dataset_arg!(allow_mismatch_crc);
        pub const COMPUTE_CRC: &str = dataset_arg!(compute_crc);
        pub const READ_INTRA_SEGMENT_DARK_BYTES: &str = dataset_arg!(read_intra_segment_dark_bytes);
        pub const READ_POST_DATASET_DARK_BYTES: &str = dataset_arg!(read_post_dataset_dark_bytes);
        pub const ROW_BUFFER_SIZE: &str = dataset_arg!(row_buffer_size);

        // shared config flags

        macro_rules! shared_arg {
            ($field:ident) => {
                string_arg!($crate::config::ReadSharedConfig, $field)
            };
        }

        pub const WARNINGS_ARE_ERRORS: &str = shared_arg!(warnings_are_errors);
        pub const HIDE_WARNINGS: &str = shared_arg!(hide_warnings);

        // write text inner args

        macro_rules! write_text_arg {
            ($field:ident) => {
                string_arg!($crate::config::WriteTEXTInnerConfig, $field)
            };
        }

        pub const DELIM: &str = write_text_arg!(delim);
        pub const BIG_OTHER: &str = write_text_arg!(big_other);
        pub const W_COMPUTE_CRC: &str = write_text_arg!(compute_crc);
        pub const OVERRIDE_FIL: &str = write_text_arg!(override_fil);

        // write dataset inner args

        macro_rules! write_dataset_arg {
            ($field:ident) => {
                string_arg!($crate::config::WriteDatasetInnerConfig, $field)
            };
        }

        pub const ALLOW_OVER_BITMASK: &str = write_dataset_arg!(allow_over_bitmask);
        pub const DISALLOW_OVER_RANGE: &str = write_dataset_arg!(disallow_over_range);
        pub const W_ROW_BUFFER_SIZE: &str = write_dataset_arg!(row_buffer_size);

        // write multi args

        macro_rules! write_multi_arg {
            ($field:ident) => {
                string_arg!($crate::config::WriteMultiConfig, $field)
            };
        }

        pub const APPENDABLE: &str = write_multi_arg!(appendable);
        pub const APPEND: &str = write_multi_arg!(append);
    };
}

pub(crate) use make_args;

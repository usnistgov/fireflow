//! Main configuration for reading and writing FCS files.
//!
//! By convention, this is "strict-by-default", meaning the default parameters
//! will be set such that only a fully-compliant FCS file can be read without
//! error. This greatly simplifies the API and internally reduces the likelihood
//! of "flipped flags."
//!
//! Internal to the library, the main question that matters for whether to throw
//! a warning or error should be "does this adhere to the standard." If not, it
//! is an error. This will work in most cases with a few exceptions where the
//! standard is unclear.

use crate::logging::{ErrorsResult, ResultExt as _};
use crate::selector::{AppendableSelector, Selector};
use crate::validated::keys::ValidKeywords;

use fireflow_types::{
    config::{
        HasStrategy, KeyPatterns, KeyStringValues, KeyStringsOrPatterns, LiteralOrPattern,
        NonUniqueKeyError, ReadDataKeywordsConfig_, ReadDatasetConfig, ReadHeaderAndTEXTConfig,
        ReadHeaderInnerConfig, ReadOffsetConfig, ReadSharedConfig, ReadStdKeywordsConfig_,
        SubPatterns, TimeMeasNamePattern, WriteDatasetInnerConfig, WriteMultiConfig,
        WriteTEXTInnerConfig, checked_iter_to_hashmap,
    },
    datepattern::DatePattern,
    keystring::KeyString,
    keystring_pairs::{KeyStringPairs, KeyStringPairsError},
    timepattern::TimePattern,
};

use derive_more::{AsRef, Display, From};
use derive_new::new;
use hashbrown::HashMap;
use nonempty_collections::NEVec;
use thiserror::Error;

#[cfg(feature = "python")]
use fireflow_core_proc::AllIntoPyErr;

/// Instructions for reading the HEADER segment.
#[derive(Default, Clone, AsRef, From)]
pub struct ReadHeaderConfig {
    pub header: ReadHeaderInnerConfig,
    pub offset: ReadOffsetConfig,
}

/// Instructions for reading the HEADER and TEXT segments in flat mode.
#[derive(Default, Clone, AsRef)]
pub struct ReadFlatTEXTConfig {
    #[as_ref(ReadHeaderInnerConfig)]
    pub header: ReadHeaderInnerConfig,

    #[as_ref(ReadHeaderAndTEXTConfig)]
    pub flat: ReadHeaderAndTEXTConfig,

    #[as_ref(ReadOffsetConfig)]
    pub offset: ReadOffsetConfig,

    pub shared: ReadSharedConfig,
}

/// Instructions for reading the HEADER and TEXT segments in standard mode.
#[derive(Default, Clone, AsRef)]
pub struct ReadStdTEXTConfig {
    #[as_ref(ReadHeaderInnerConfig)]
    pub header: ReadHeaderInnerConfig,

    #[as_ref(ReadHeaderAndTEXTConfig)]
    pub flat: ReadHeaderAndTEXTConfig,

    #[as_ref(ReadOffsetConfig)]
    pub offset: ReadOffsetConfig,

    #[as_ref(ReadStdKeywordsConfig)]
    pub standard: ReadStdKeywordsConfig,

    #[as_ref(ReadDataKeywordsConfig)]
    pub layout: ReadDataKeywordsConfig,

    #[as_ref(ReadSharedConfig)]
    pub shared: ReadSharedConfig,
}

/// Instructions for reading a dataset in flat mode.
#[derive(Default, Clone, AsRef)]
pub struct ReadFlatDatasetConfig {
    #[as_ref(ReadHeaderInnerConfig)]
    pub header: ReadHeaderInnerConfig,

    #[as_ref(ReadHeaderAndTEXTConfig)]
    pub flat: ReadHeaderAndTEXTConfig,

    #[as_ref(ReadOffsetConfig)]
    pub offset: ReadOffsetConfig,

    #[as_ref(ReadDataKeywordsConfig)]
    pub layout: ReadDataKeywordsConfig,

    #[as_ref(ReadDatasetConfig)]
    pub data: ReadDatasetConfig,

    #[as_ref(ReadSharedConfig)]
    pub shared: ReadSharedConfig,
}

/// Instructions for reading a dataset in standard mode.
#[derive(Default, Clone, AsRef)]
pub struct ReadStdDatasetConfig {
    #[as_ref(ReadHeaderInnerConfig)]
    pub header: ReadHeaderInnerConfig,

    #[as_ref(ReadHeaderAndTEXTConfig)]
    pub flat: ReadHeaderAndTEXTConfig,

    #[as_ref(ReadOffsetConfig)]
    pub offset: ReadOffsetConfig,

    #[as_ref(ReadStdKeywordsConfig)]
    pub standard: ReadStdKeywordsConfig,

    #[as_ref(ReadDataKeywordsConfig)]
    pub layout: ReadDataKeywordsConfig,

    #[as_ref(ReadDatasetConfig)]
    pub data: ReadDatasetConfig,

    #[as_ref(ReadSharedConfig)]
    pub shared: ReadSharedConfig,
}

/// Instructions for reading a dataset in flat mode with a given set of keywords.
#[derive(Default, Clone, AsRef)]
pub struct ReadFlatDatasetFromKeywordsConfig {
    #[as_ref(ReadOffsetConfig)]
    pub offset: ReadOffsetConfig,

    #[as_ref(ReadDataKeywordsConfig)]
    pub layout: ReadDataKeywordsConfig,

    #[as_ref(ReadDatasetConfig)]
    pub data: ReadDatasetConfig,

    #[as_ref(ReadSharedConfig)]
    pub shared: ReadSharedConfig,
}

/// Instructions for building a new [`crate::core::CoreTEXT`] from keywords.
#[derive(Default, Clone, AsRef)]
pub struct NewCoreTEXTConfig {
    #[as_ref(ReadStdKeywordsConfig)]
    pub standard: ReadStdKeywordsConfig,

    #[as_ref(ReadDataKeywordsConfig)]
    pub layout: ReadDataKeywordsConfig,

    #[as_ref(ReadSharedConfig)]
    pub shared: ReadSharedConfig,
}

/// Instructions for building a new [`crate::core::CoreDataset`] from keywords.
#[derive(Default, Clone, AsRef)]
pub struct NewCoreDatasetConfig {
    #[as_ref(ReadOffsetConfig)]
    pub offset: ReadOffsetConfig,

    #[as_ref(ReadStdKeywordsConfig)]
    pub standard: ReadStdKeywordsConfig,

    #[as_ref(ReadDataKeywordsConfig)]
    pub layout: ReadDataKeywordsConfig,

    #[as_ref(ReadDatasetConfig)]
    pub data: ReadDatasetConfig,

    #[as_ref(ReadSharedConfig)]
    pub shared: ReadSharedConfig,
}

/// Configuration for writing one or more HEADER+TEXT segments to file
#[derive(Clone, Copy, Default, new)]
pub struct WriteMultiTEXTConfig {
    pub inner: WriteTEXTInnerConfig,
    pub multi: WriteMultiConfig,
}

/// Configuration for writing one or more datasets to file
#[derive(Clone, Copy, Default, new)]
pub struct WriteMultiDatasetConfig {
    pub inner: WriteDatasetInnerConfig,
    pub multi: WriteMultiConfig,
}

pub type ReadStdKeywordsConfig = ReadStdKeywordsConfig_<
    Selector<TimeMeasNamePattern>,
    Selector<Option<DatePattern>>,
    Selector<Option<TimePattern>>,
    Selector<Option<String>>,
    Selector<Option<String>>,
>;

pub type ReadDataKeywordsConfig = ReadDataKeywordsConfig_<
    AppendableSelector<KeyPatterns>,
    AppendableSelector<KeyStringPairs>,
    AppendableSelector<KeyPatterns>,
    AppendableSelector<KeyPatterns>,
    AppendableSelector<KeyStringValues>,
    AppendableSelector<KeyStringValues>,
    AppendableSelector<SubPatterns>,
>;

pub type EvaledReadStdKeywordsConfig = ReadStdKeywordsConfig_<
    TimeMeasNamePattern,
    Option<DatePattern>,
    Option<TimePattern>,
    Option<String>,
    Option<String>,
>;

pub type EvaledReadDataKeywordsConfig = ReadDataKeywordsConfig_<
    KeyPatterns,
    KeyStringPairs,
    KeyPatterns,
    KeyPatterns,
    KeyStringValues,
    KeyStringValues,
    SubPatterns,
>;

pub(crate) fn eval_std_conf(
    conf: &ReadStdKeywordsConfig,
    kws: &ValidKeywords,
) -> EvaledReadStdKeywordsConfig {
    ReadStdKeywordsConfig_ {
        dedup_measurement_names: conf.dedup_measurement_names,
        trim_intra_value_whitespace: conf.trim_intra_value_whitespace,
        time_meas_pattern: conf.time_meas_pattern.eval(kws),
        allow_missing_time: conf.allow_missing_time,
        add_missing_timestep: conf.add_missing_timestep,
        force_linear_scale: conf.force_linear_scale,
        ignore_optical_only_keys: conf.ignore_optical_only_keys.clone(),
        process_optical_only_keys: conf.process_optical_only_keys,
        spillover_measurement_mode: conf.spillover_measurement_mode,
        date_pattern: conf.date_pattern.eval(kws),
        time_pattern: conf.time_pattern.eval(kws),
        datetime_pattern: conf.datetime_pattern.eval(kws),
        last_modified_pattern: conf.last_modified_pattern.eval(kws),
        allow_other_feature: conf.allow_other_feature,
        process_pseudostandard: conf.process_pseudostandard,
        process_hyper_par: conf.process_hyper_par,
        process_other_version: conf.process_other_version,
        process_extra_timestep: conf.process_extra_timestep,
        fix_log_scale_offsets: conf.fix_log_scale_offsets,
        disallow_localtime: conf.disallow_localtime,
    }
}

pub(crate) fn eval_data_conf(
    conf: &ReadDataKeywordsConfig,
    kws: &ValidKeywords,
) -> ErrorsResult<EvaledReadDataKeywordsConfig, (), AppendRepairFlagError> {
    let go_str_pairs = |xs: NEVec<KeyStringPairs>| {
        let checked = checked_iter_to_hashmap(xs.into_iter().flat_map(KeyStringPairs::into_iter))?;
        KeyStringPairs::try_from(checked).map_err(AppendRepairFlagError::KeyStringPairsValid)
    };
    let go_val = |xs: NEVec<KeyStringValues>| {
        let res = checked_iter_to_hashmap(xs.into_iter().flat_map(HashMap::into_iter))?;
        Ok(res)
    };

    macro_rules! go_keystr {
        ($field:ident) => {
            conf.$field
                .try_eval(kws, |xs| Ok(KeyStringsOrPatterns::from_many(xs)?))
                .into_nowarn()
        };
    }

    let rename_res = conf
        .rename_standard_keys
        .try_eval(kws, go_str_pairs)
        .into_nowarn();

    let sub_res = go_keystr!(substitute_standard_key_values);
    let ignore_res = go_keystr!(ignore_standard_keys);
    let promote_res = go_keystr!(promote_to_standard);
    let demote_res = go_keystr!(demote_from_standard);

    let replace_res = conf
        .replace_standard_key_values
        .try_eval(kws, go_val)
        .into_nowarn();
    let append_res = conf
        .append_standard_keywords
        .try_eval(kws, go_val)
        .into_nowarn();

    rename_res
        .zip4_commutative(sub_res, ignore_res, promote_res)
        .zip4_commutative(demote_res, replace_res, append_res)
        .map_ok_value(
            |((rename, sub, ignore, promote), demote, replace, append)| ReadDataKeywordsConfig_ {
                ignore_standard_keys: ignore,
                rename_standard_keys: rename,
                promote_to_standard: promote,
                demote_from_standard: demote,
                replace_standard_key_values: replace,
                append_standard_keywords: append,
                substitute_standard_key_values: sub,
                allow_repair_non_unique: conf.allow_repair_non_unique,
                text_data_correction: conf.text_data_correction,
                text_analysis_correction: conf.text_analysis_correction,
                ignore_text_data_offsets: conf.ignore_text_data_offsets,
                ignore_text_analysis_offsets: conf.ignore_text_analysis_offsets,
                allow_header_text_offset_mismatch: conf.allow_header_text_offset_mismatch,
                allow_missing_required_offsets: conf.allow_missing_required_offsets,
                process_optional_failure: conf.process_optional_failure,
                int_width_override: conf.int_width_override,
                byteord_override: conf.byteord_override.clone(),
                disallow_range_truncation: conf.disallow_range_truncation,
            },
        )
}

#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum AppendRepairFlagError {
    KeyPattern(NonUniqueKeyError<LiteralOrPattern<KeyString>>),
    KeyStringPairsHash(NonUniqueKeyError<KeyString>),
    KeyStringPairsValid(KeyStringPairsError),
}

impl HasStrategy for ReadHeaderConfig {
    fn with_scalpal(&mut self) {
        self.header.with_scalpal();
        self.offset.with_scalpal();
    }

    fn with_sledgehammer(&mut self) {
        self.header.with_sledgehammer();
        self.offset.with_sledgehammer();
    }
}

impl HasStrategy for ReadFlatTEXTConfig {
    fn with_scalpal(&mut self) {
        self.header.with_scalpal();
        self.flat.with_scalpal();
        self.offset.with_scalpal();
    }

    fn with_sledgehammer(&mut self) {
        self.header.with_sledgehammer();
        self.flat.with_sledgehammer();
        self.offset.with_sledgehammer();
    }
}

impl HasStrategy for ReadStdTEXTConfig {
    fn with_scalpal(&mut self) {
        self.header.with_scalpal();
        self.flat.with_scalpal();
        self.offset.with_scalpal();
        self.standard.with_scalpal();
        self.layout.with_scalpal();
    }

    fn with_sledgehammer(&mut self) {
        self.header.with_sledgehammer();
        self.flat.with_sledgehammer();
        self.offset.with_sledgehammer();
        self.standard.with_sledgehammer();
        self.layout.with_sledgehammer();
    }
}

impl HasStrategy for ReadFlatDatasetConfig {
    fn with_scalpal(&mut self) {
        self.header.with_scalpal();
        self.flat.with_scalpal();
        self.offset.with_scalpal();
        self.layout.with_scalpal();
        self.data.with_scalpal();
    }

    fn with_sledgehammer(&mut self) {
        self.header.with_sledgehammer();
        self.flat.with_sledgehammer();
        self.offset.with_sledgehammer();
        self.layout.with_sledgehammer();
        self.data.with_sledgehammer();
    }
}

impl HasStrategy for ReadStdDatasetConfig {
    fn with_scalpal(&mut self) {
        self.header.with_scalpal();
        self.flat.with_scalpal();
        self.offset.with_scalpal();
        self.standard.with_scalpal();
        self.layout.with_scalpal();
        self.data.with_scalpal();
    }

    fn with_sledgehammer(&mut self) {
        self.header.with_sledgehammer();
        self.flat.with_sledgehammer();
        self.offset.with_sledgehammer();
        self.standard.with_sledgehammer();
        self.layout.with_sledgehammer();
        self.data.with_sledgehammer();
    }
}

impl HasStrategy for ReadFlatDatasetFromKeywordsConfig {
    fn with_scalpal(&mut self) {
        self.offset.with_scalpal();
        self.layout.with_scalpal();
        self.data.with_scalpal();
    }

    fn with_sledgehammer(&mut self) {
        self.offset.with_sledgehammer();
        self.layout.with_sledgehammer();
        self.data.with_sledgehammer();
    }
}

impl HasStrategy for NewCoreTEXTConfig {
    fn with_scalpal(&mut self) {
        self.standard.with_scalpal();
        self.layout.with_scalpal();
    }

    fn with_sledgehammer(&mut self) {
        self.standard.with_sledgehammer();
        self.layout.with_sledgehammer();
    }
}

impl HasStrategy for NewCoreDatasetConfig {
    fn with_scalpal(&mut self) {
        self.offset.with_scalpal();
        self.standard.with_scalpal();
        self.layout.with_scalpal();
        self.data.with_scalpal();
    }

    fn with_sledgehammer(&mut self) {
        self.offset.with_sledgehammer();
        self.standard.with_sledgehammer();
        self.layout.with_sledgehammer();
        self.data.with_sledgehammer();
    }
}

#[cfg(feature = "python")]
mod python {
    use super::{
        NewCoreDatasetConfig, NewCoreTEXTConfig, ReadFlatDatasetConfig,
        ReadFlatDatasetFromKeywordsConfig, ReadFlatTEXTConfig, ReadHeaderConfig,
        ReadStdDatasetConfig, ReadStdTEXTConfig,
    };

    use pyo3::{prelude::*, types::PyDict};

    macro_rules! impl_into_flat_dict {
        ($t:ident, $($field:ident),*) => {
            impl<'py> IntoPyObject<'py> for $t {
                type Target = PyDict;
                type Output = Bound<'py, Self::Target>;
                type Error = PyErr;

                fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
                    let result = PyDict::new(py);
                    $(
                        for (k, v) in self.$field.into_pyobject(py)?.iter() {
                            result.set_item(k, v)?;
                        }
                    )*
                    Ok(result)
                }
            }
        };
    }

    impl_into_flat_dict!(ReadHeaderConfig, header, offset);

    impl_into_flat_dict!(ReadFlatTEXTConfig, header, flat, offset, shared);

    impl_into_flat_dict!(
        ReadStdTEXTConfig,
        header,
        flat,
        offset,
        standard,
        layout,
        shared
    );

    impl_into_flat_dict!(ReadFlatDatasetConfig, header, flat, offset, data, shared);

    impl_into_flat_dict!(
        ReadStdDatasetConfig,
        header,
        flat,
        offset,
        standard,
        layout,
        data,
        shared
    );

    impl_into_flat_dict!(
        ReadFlatDatasetFromKeywordsConfig,
        offset,
        layout,
        data,
        shared
    );

    impl_into_flat_dict!(NewCoreTEXTConfig, standard, layout, shared);

    impl_into_flat_dict!(NewCoreDatasetConfig, offset, standard, layout, data, shared);
}

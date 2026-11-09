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

use crate::api::ValidKeywords;
use crate::logging::{ErrorsResult, ResultExt as _};
use crate::selector::{AppendableSelector, Selector};

use fireflow_types::config::{
    HasStrategy, ReadDataKeywordsConfig, ReadDatasetConfig, ReadHeaderAndTEXTConfig,
    ReadHeaderInnerConfig, ReadOffsetConfig, ReadRepairKeywordsConfig_, ReadSharedConfig,
    ReadStdKeywordsConfig_, TimeMeasNamePattern, WriteDatasetInnerConfig, WriteMultiConfig,
    WriteTEXTInnerConfig,
};
use fireflow_types::datepattern::DatePattern;
use fireflow_types::keys::{AnyKey, StdKey};
use fireflow_types::keystring_pairs::{KeyPairs, KeyPairsError};
use fireflow_types::std_pattern::{
    NonUniqueKeyError, StdKeyOrPattern, StdKeyPatterns, StdKeysOrPatterns, SubPatterns,
    checked_iter_to_hashmap,
};
use fireflow_types::timepattern::TimePattern;
use nonempty::{NEString, NEVec};

use derive_more::{AsRef, Display, From};
use derive_new::new;
use hashbrown::HashMap;
use thiserror::Error;

#[cfg(feature = "python")]
use fireflow_core_proc::{AllIntoPyErr, IntoFlatPyDict};

#[cfg(feature = "serde")]
use serde::Serialize;

/// Instructions for reading the HEADER segment.
#[derive(Default, Clone, AsRef, From)]
#[cfg_attr(feature = "python", derive(IntoFlatPyDict))]
pub struct ReadHeaderConfig {
    pub header: ReadHeaderInnerConfig,
    pub offset: ReadOffsetConfig,
}

/// Instructions for reading the HEADER and TEXT segments in flat mode.
#[derive(Default, Clone, AsRef)]
#[cfg_attr(feature = "python", derive(IntoFlatPyDict))]
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
#[cfg_attr(feature = "python", derive(IntoFlatPyDict))]
pub struct ReadStdTEXTConfig {
    #[as_ref(ReadHeaderInnerConfig)]
    pub header: ReadHeaderInnerConfig,

    #[as_ref(ReadHeaderAndTEXTConfig)]
    pub flat: ReadHeaderAndTEXTConfig,

    #[as_ref(ReadOffsetConfig)]
    pub offset: ReadOffsetConfig,

    #[as_ref(ReadRepairKeywordsConfig)]
    pub repair: ReadRepairKeywordsConfig,

    #[as_ref(ReadStdKeywordsConfig)]
    pub standard: ReadStdKeywordsConfig,

    #[as_ref(ReadDataKeywordsConfig)]
    pub layout: ReadDataKeywordsConfig,

    #[as_ref(ReadSharedConfig)]
    pub shared: ReadSharedConfig,
}

/// Instructions for reading a dataset in flat mode.
#[derive(Default, Clone, AsRef)]
#[cfg_attr(feature = "python", derive(IntoFlatPyDict))]
pub struct ReadFlatDatasetConfig {
    #[as_ref(ReadHeaderInnerConfig)]
    pub header: ReadHeaderInnerConfig,

    #[as_ref(ReadHeaderAndTEXTConfig)]
    pub flat: ReadHeaderAndTEXTConfig,

    #[as_ref(ReadOffsetConfig)]
    pub offset: ReadOffsetConfig,

    #[as_ref(ReadRepairKeywordsConfig)]
    pub repair: ReadRepairKeywordsConfig,

    #[as_ref(ReadDataKeywordsConfig)]
    pub layout: ReadDataKeywordsConfig,

    #[as_ref(ReadDatasetConfig)]
    pub data: ReadDatasetConfig,

    #[as_ref(ReadSharedConfig)]
    pub shared: ReadSharedConfig,
}

/// Instructions for reading a dataset in standard mode.
#[derive(Default, Clone, AsRef)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoFlatPyDict))]
pub struct ReadStdDatasetConfig {
    #[as_ref(ReadHeaderInnerConfig)]
    pub header: ReadHeaderInnerConfig,

    #[as_ref(ReadHeaderAndTEXTConfig)]
    pub flat: ReadHeaderAndTEXTConfig,

    #[as_ref(ReadOffsetConfig)]
    pub offset: ReadOffsetConfig,

    #[as_ref(ReadRepairKeywordsConfig)]
    pub repair: ReadRepairKeywordsConfig,

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
#[cfg_attr(feature = "python", derive(IntoFlatPyDict))]
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
#[cfg_attr(feature = "python", derive(IntoFlatPyDict))]
pub struct NewCoreTEXTConfig {
    #[as_ref(EvaledReadStdKeywordsConfig)]
    pub standard: EvaledReadStdKeywordsConfig,

    #[as_ref(ReadDataKeywordsConfig)]
    pub layout: ReadDataKeywordsConfig,

    #[as_ref(ReadSharedConfig)]
    pub shared: ReadSharedConfig,
}

/// Instructions for building a new [`crate::core::CoreDataset`] from keywords.
#[derive(Default, Clone, AsRef)]
#[cfg_attr(feature = "python", derive(IntoFlatPyDict))]
pub struct NewCoreDatasetConfig {
    #[as_ref(ReadOffsetConfig)]
    pub offset: ReadOffsetConfig,

    #[as_ref(EvaledReadStdKeywordsConfig)]
    pub standard: EvaledReadStdKeywordsConfig,

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

pub type ReadRepairKeywordsConfig = ReadRepairKeywordsConfig_<
    AppendableSelector<StdKeyPatterns<true>>,
    AppendableSelector<KeyPairs>,
    AppendableSelector<StdKeyPatterns<false>>,
    AppendableSelector<StdKeyPatterns<true>>,
    AppendableSelector<StdKeyValues>,
    AppendableSelector<StdKeyValues>,
    AppendableSelector<SubPatterns>,
>;

pub type EvaledReadStdKeywordsConfig = ReadStdKeywordsConfig_<
    TimeMeasNamePattern,
    Option<DatePattern>,
    Option<TimePattern>,
    Option<String>,
    Option<String>,
>;

pub type EvaledReadRepairKeywordsConfig = ReadRepairKeywordsConfig_<
    StdKeyPatterns<true>,
    KeyPairs,
    StdKeyPatterns<false>,
    StdKeyPatterns<true>,
    StdKeyValues,
    StdKeyValues,
    SubPatterns,
>;

/// A map of [`StdKey`]/[`NEString`] pairs.
///
/// The main use case for this is to replace or add key values.
pub type StdKeyValues = HashMap<StdKey, NEString>;

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

pub(crate) fn eval_repair_conf(
    conf: &ReadRepairKeywordsConfig,
    kws: &ValidKeywords,
) -> ErrorsResult<EvaledReadRepairKeywordsConfig, (), AppendRepairFlagError> {
    let go_str_pairs = |xs: NEVec<KeyPairs>| {
        let checked = checked_iter_to_hashmap(xs.into_iter().flat_map(KeyPairs::into_iter))?;
        KeyPairs::try_from(checked).map_err(AppendRepairFlagError::KeyStringPairsValid)
    };
    let go_val = |xs: NEVec<StdKeyValues>| {
        let res = checked_iter_to_hashmap(xs.into_iter().flat_map(HashMap::into_iter))?;
        Ok(res)
    };

    macro_rules! go_keystr {
        ($field:ident) => {
            conf.$field
                .try_eval(kws, |xs| Ok(StdKeysOrPatterns::from_many(xs)?))
                .into_nowarn()
        };
    }

    let rename_res = conf
        .rename_standard_keys
        .try_eval(kws, go_str_pairs)
        .into_nowarn();

    let sub_res = go_keystr!(substitute_standard_key_values);
    let ignore_res = go_keystr!(ignore_standard_keys);
    let promote_res = go_keystr!(promote_nonstandard_keys);
    let demote_res = go_keystr!(demote_standard_keys);

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
            |((rename, sub, ignore, promote), demote, replace, append)| ReadRepairKeywordsConfig_ {
                ignore_standard_keys: ignore,
                rename_standard_keys: rename,
                promote_nonstandard_keys: promote,
                demote_standard_keys: demote,
                replace_standard_key_values: replace,
                append_standard_keywords: append,
                substitute_standard_key_values: sub,
                promote_all_nonstandard_keys: conf.promote_all_nonstandard_keys,
                allow_repair_non_unique: conf.allow_repair_non_unique,
            },
        )
}

#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum AppendRepairFlagError {
    Std(NonUniqueKeyError<StdKeyOrPattern<true>>),
    PseudoNonStd(NonUniqueKeyError<StdKeyOrPattern<false>>),
    NonUniqueStd(NonUniqueKeyError<StdKey>),
    NonUniqueMaybeStd(NonUniqueKeyError<AnyKey>),
    KeyStringPairsValid(KeyPairsError),
}

impl HasStrategy for ReadHeaderConfig {
    fn with_scalpel(&mut self) {
        self.header.with_scalpel();
        self.offset.with_scalpel();
    }

    fn with_sledgehammer(&mut self) {
        self.header.with_sledgehammer();
        self.offset.with_sledgehammer();
    }
}

impl HasStrategy for ReadFlatTEXTConfig {
    fn with_scalpel(&mut self) {
        self.header.with_scalpel();
        self.flat.with_scalpel();
        self.offset.with_scalpel();
    }

    fn with_sledgehammer(&mut self) {
        self.header.with_sledgehammer();
        self.flat.with_sledgehammer();
        self.offset.with_sledgehammer();
    }
}

impl HasStrategy for ReadStdTEXTConfig {
    fn with_scalpel(&mut self) {
        self.header.with_scalpel();
        self.flat.with_scalpel();
        self.offset.with_scalpel();
        self.standard.with_scalpel();
        self.layout.with_scalpel();
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
    fn with_scalpel(&mut self) {
        self.header.with_scalpel();
        self.flat.with_scalpel();
        self.offset.with_scalpel();
        self.layout.with_scalpel();
        self.data.with_scalpel();
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
    fn with_scalpel(&mut self) {
        self.header.with_scalpel();
        self.flat.with_scalpel();
        self.offset.with_scalpel();
        self.standard.with_scalpel();
        self.layout.with_scalpel();
        self.data.with_scalpel();
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
    fn with_scalpel(&mut self) {
        self.offset.with_scalpel();
        self.layout.with_scalpel();
        self.data.with_scalpel();
    }

    fn with_sledgehammer(&mut self) {
        self.offset.with_sledgehammer();
        self.layout.with_sledgehammer();
        self.data.with_sledgehammer();
    }
}

impl HasStrategy for NewCoreTEXTConfig {
    fn with_scalpel(&mut self) {
        self.standard.with_scalpel();
        self.layout.with_scalpel();
    }

    fn with_sledgehammer(&mut self) {
        self.standard.with_sledgehammer();
        self.layout.with_sledgehammer();
    }
}

impl HasStrategy for NewCoreDatasetConfig {
    fn with_scalpel(&mut self) {
        self.offset.with_scalpel();
        self.standard.with_scalpel();
        self.layout.with_scalpel();
        self.data.with_scalpel();
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
    use super::{NewCoreDatasetConfig, NewCoreTEXTConfig};

    use fireflow_types::config::HasStrategy as _;
    use fireflow_types::keywords::Version;
    use fireflow_types::{args::underscore as ta, config::ReadStrategy};

    use pyo3::{prelude::*, types::PyDict};

    impl NewCoreTEXTConfig {
        /// Return dict for this config struct with version-specific keys.
        pub fn dump_model_version(
            version: Version,
            strategy: ReadStrategy,
            py: Python<'_>,
        ) -> PyResult<Bound<'_, PyDict>> {
            let conf = Self::new_with_strategy(strategy);
            // Make a full dict
            let dict = conf.into_pyobject(py)?;
            // Remove the stuff we don't want based on version.
            new_core_text_remove_by_version(&dict, version)?;
            Ok(dict)
        }
    }

    impl NewCoreDatasetConfig {
        /// Return dict for this config struct with version-specific keys.
        pub fn dump_model_version(
            version: Version,
            strategy: ReadStrategy,
            py: Python<'_>,
        ) -> PyResult<Bound<'_, PyDict>> {
            let conf = Self::new_with_strategy(strategy);
            let dict = conf.into_pyobject(py)?;
            // This config struct is a superset of NewCoreTEXTConfig, so remove
            // these keys first.
            new_core_text_remove_by_version(&dict, version)?;
            if version == Version::FCS2_0 {
                // These only apply to the DATA and ANALYSIS offset pairs in
                // TEXT since this configuration won't be used to read HEADER or
                // keywords from TEXT. Since 2.0 doesn't have these keywords,
                // remove these options.
                dict.del_item(ta::ALLOW_PSEUDOEMPTY)?;
                dict.del_item(ta::DATASET_OVERFLOW_LIMIT)?;
                dict.del_item(ta::OVERLAP_CORRECTION_LIMIT)?;

                // FCS 2.0 doesn't have a CRC. None of these options apply.
                dict.del_item(ta::ALLOW_MISSING_CRC)?;
                dict.del_item(ta::ALLOW_MISMATCH_CRC)?;
                dict.del_item(ta::COMPUTE_CRC)?;
            }
            Ok(dict)
        }
    }

    fn new_core_text_remove_by_version(dict: &Bound<'_, PyDict>, version: Version) -> PyResult<()> {
        if version == Version::FCS2_0 {
            // 2.0 doesn't have $TIMESTEP.
            dict.del_item(ta::ADD_MISSING_TIMESTEP)?;

            // 2.0 doesn't allow offsets in TEXT. None of these options apply.
            dict.del_item(ta::TEXT_DATA_CORR)?;
            dict.del_item(ta::TEXT_ANALYSIS_CORR)?;
            dict.del_item(ta::IGNORE_TEXT_DATA_OFFSETS)?;
            dict.del_item(ta::IGNORE_TEXT_ANALYSIS_OFFSETS)?;
            dict.del_item(ta::ALLOW_HEADER_TEXT_OFFSET_MISMATCH)?;
            dict.del_item(ta::ALLOW_MISSING_REQUIRED_OFFSETS)?;
        }

        if version < Version::FCS3_2 {
            // This is only pertinent to 3.2 which has $BEGIN/ENDDATETIME
            dict.del_item(ta::DISALLOW_LOCALTIME)?;
        }

        if version < Version::FCS3_1 {
            //   $SPILLOVER is 3.1
            dict.del_item(ta::SPILLOVER_MEASUREMENT_MODE)?;
        }

        if version > Version::FCS3_0 {
            // These two options only apply to ordered integer data schemas in
            // 2.0/3.0. The $BYTEORD keyword change in 3.1 makes this irrelevant
            // for later versions.
            dict.del_item(ta::INT_WIDTH_OVERRIDE)?;
            dict.del_item(ta::BYTEORD_OVERRIDE)?;
        }

        Ok(())
    }
}

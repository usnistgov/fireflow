use built::write_built_file;

use std::env;
use std::fs;
use std::path::PathBuf;

fn main() {
    // Custom compile-time variables that define values embedded into the final
    // binary.

    // APP_INDEX_CUTOFF: upper limit to the indices stored in indexed keywords.
    //
    // Any keyword with an index (Pn*, Gn*, etc) will be capped at this number.
    // The internal data structures which store these assume densely packed
    // keywords and thus will store an offset for each index up to the max. This
    // is a problem is there is only one keyword with an index of say 100000000,
    // which will uselessly allocate GB of space for nothing.
    //
    // Since the Pn* keywords are the only indexed keywords that matter in 99%
    // of cases, this cutoff effectively controls the number of parameters that
    // are allowed in a file. At the time of writing, A8/S8 files have the
    // largest set of parameters at ~400.
    println!("cargo:rerun-if-env-changed=APP_INDEX_CUTOFF");

    let raw = match env::var("APP_INDEX_CUTOFF") {
        Ok(value) => value,
        Err(env::VarError::NotPresent) => "10000".to_owned(),
        Err(err) => panic!("invalid APP_INDEX_CUTOFF: {err}"),
    };

    let capacity: usize = raw.parse().unwrap();

    let out_dir = PathBuf::from(env::var_os("OUT_DIR").unwrap());

    fs::write(
        out_dir.join("build_config.rs"),
        format!("pub const INDEX_CUTOFF: usize = {capacity};\n"),
    )
    .unwrap();

    write_built_file().expect("failed to write built file");

    // Effective target features chosen by rustc (result of target-cpu +
    // any explicit target-feature flags + baseline for the target triple).
    let features = env::var("CARGO_CFG_TARGET_FEATURE").unwrap_or_default();
    println!("cargo:rustc-env=FIREFLOW_TARGET_FEATURES={features}");

    // Re-run build.rs if either changes.
    println!("cargo:rerun-if-env-changed=CARGO_CFG_TARGET_FEATURE");
}

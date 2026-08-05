use built::write_built_file;
use std::env;

fn main() {
    write_built_file().expect("failed to write built file");

    // Effective target features chosen by rustc (result of target-cpu +
    // any explicit target-feature flags + baseline for the target triple).
    let features = env::var("CARGO_CFG_TARGET_FEATURE").unwrap_or_default();
    println!("cargo:rustc-env=FIREFLOW_TARGET_FEATURES={features}");

    // Re-run build.rs if either changes.
    println!("cargo:rerun-if-env-changed=CARGO_CFG_TARGET_FEATURE");
}

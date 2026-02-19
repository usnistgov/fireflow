use unicase::Ascii;

// The string primitives for almost all keywords are compiled in a build script
// as string constants and included here. This is done in order to put these
// strings into a pre-compiled hash table which will be used for version
// autodetection and sorting through unused keywords efficiently.
//
// This will also instantiate constants for all keywords and their components:
// - root keyword base strings will be like <NAME>_KW
// - root keywords (with $) will be named just like the base string
// - meas suffixes will be like <NAME>_KW_SUFFIX
// - meas keywords with $ and "n" will be like PN<SUFFIX>
include!(concat!(env!("OUT_DIR"), "/kw_map.rs"));

// other keywords not in build script
pub const PKN: &str = "$PKn";
pub const PKNN: &str = "$PKNn";
pub const RNI: &str = "$RNI";
pub const RNW: &str = "$RNW";

/// Data structure to classify root (non-indexed) keywords.
///
/// For optional keywords this simply records the version in which a given
/// keyword is valid. Some specific keywords ($CYT, $TOT, etc) are explicitly
/// encoded since they are optional or required (or missing entirely) depending
/// on version. $BYTEORD is included because a non-endian value implies 2.0/3.0.
/// $MODE is included because its value and optionality is different between 3.1
/// and 3.2
#[derive(Clone, Copy)]
pub enum RootKeywordClass {
    OptAny,
    OptGE3_1,
    OptGE3_2,
    OptEQ3_0or3_1,
    OptEQ3_0,
    OptLE3_1,
    Mode,
    Cyt,
    Tot,
    Timestep,
    Byteord,
    Begindata,
    Enddata,
    Beginanalysis,
    Endanalysis,
    Beginstext,
    Endstext,
}

pub enum MeasKeywordClass {
    OptAny,
    OptGE3_0,
    OptGE3_1,
    OptGE3_2,
    Scale,
    Shortname,
    Wavelength,
}

// BYTEORD big/little flags

pub const BYTEORD_BIG: &str = "big";
pub const BYTEORD_LITTLE: &str = "little";

// Scale Diagnostic flags

pub const SCALE_DIAGNOSTIC_FORCED: &str = "forced";
pub const SCALE_DIAGNOSTIC_LOG: &str = "log";
pub const SCALE_DIAGNOSTIC_TRIMMED: &str = "trimmed";
pub const SCALE_DIAGNOSTIC_TRIMMED_LOG: &str = "trimmed_log";

pub const TEMPORAL_SCALE_DIAGNOSTIC_FORCED: &str = "forced";
pub const TEMPORAL_SCALE_DIAGNOSTIC_TRIMMED: &str = "trimmed";

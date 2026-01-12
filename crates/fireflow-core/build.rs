use std::env;
use std::fs::File;
use std::io::{self, BufWriter, Write as _};
use std::path::Path;
use unicase::UniCase;

fn write_kw_map(file: &mut BufWriter<File>) -> io::Result<()> {
    let mut m = phf_codegen::Map::new();
    let i = "KW_MAP";
    let vt = "VersionClass";

    let any_version = [
        ("MODE_KW", "MODE"),
        ("ABRT_KW", "ABRT"),
        ("BTIM_KW", "BTIM"),
        ("BYTEORD_KW", "BYTEORD"),
        ("CYT_KW", "CYT"),
        ("CYTSN_KW", "CYTSN"),
        ("COM_KW", "COM"),
        ("CELLS_KW", "CELLS"),
        ("DATATYPE_KW", "DATATYPE"),
        ("DATE_KW", "DATE"),
        ("ETIM_KW", "ETIM"),
        ("EXP_KW", "EXP"),
        ("FIL_KW", "FIL"),
        ("GATING_KW", "GATING"),
        ("INST_KW", "INST"),
        ("LOST_KW", "LOST"),
        ("NEXTDATA_KW", "NEXTDATA"),
        ("OP_KW", "OP"),
        ("PAR_KW", "PAR"),
        ("PROJ_KW", "PROJ"),
        ("SMNO_KW", "SMNO"),
        ("SRC_KW", "SRC"),
        ("SYS_KW", "SYS"),
        ("TOT_KW", "TOT"),
        ("TR_KW", "TR"),
    ];

    let min_3_0 = [
        ("BEGINANALYSIS_KW", "BEGINANALYSIS"),
        ("BEGINDATA_KW", "BEGINDATA"),
        ("BEGINSTEXT_KW", "BEGINSTEXT"),
        ("ENDANALYSIS_KW", "ENDANALYSIS"),
        ("ENDDATA_KW", "ENDDATA"),
        ("ENDSTEXT_KW", "ENDSTEXT"),
        ("TIMESTEP_KW", "TIMESTEP"),
    ];

    let min_3_1 = [
        ("LAST_MODIFIER_KW", "LAST_MODIFIER"),
        ("ORIGINALITY_KW", "ORIGINALITY"),
        ("LAST_MODIFIED_KW", "LAST_MODIFIED"),
        ("PLATEID_KW", "PLATEID"),
        ("PLATENAME_KW", "PLATENAME"),
        ("WELLID_KW", "WELLID"),
        ("SPILLOVER_KW", "SPILLOVER"),
        ("VOL_KW", "VOL"),
    ];

    let min_3_2 = [
        ("CARRIERID_KW", "CARRIERID"),
        ("CARRIERTYPE_KW", "CARRIERTYPE"),
        ("LOCATIONID_KW", "LOCATIONID"),
        ("BEGINDATETIME_KW", "BEGINDATETIME"),
        ("ENDDATETIME_KW", "ENDDATETIME"),
        ("UNSTAINEDCENTERS_KW", "UNSTAINEDCENTERS"),
        ("UNSTAINEDINFO_KW", "UNSTAINEDINFO"),
        ("FLOWRATE_KW", "FLOWRATE"),
    ];

    let max_3_1 = [("GATE_KW", "GATE")];

    let is3_0or3_1 = [
        ("CSMODE_KW", "CSMODE"),
        ("CSTOT_KW", "CSTOT"),
        ("CSVBITS_KW", "CSVBITS"),
    ];

    let only3_0 = [("UNICODE_KW", "UNICODE"), ("COMP_KW", "COMP")];

    macro_rules! go {
        ($pairs:expr, $class:expr) => {
            for (k, v) in $pairs {
                writeln!(file, "pub(crate) const {k}: &str = \"{v}\";").unwrap();
                m.entry(UniCase::ascii(v), $class);
            }
        };
    }

    go!(&any_version, "VersionClass::Any");
    go!(&min_3_0, "VersionClass::GE(Version::FCS3_0)");
    go!(&min_3_1, "VersionClass::GE(Version::FCS3_1)");
    go!(&min_3_2, "VersionClass::GE(Version::FCS3_2)");
    go!(&is3_0or3_1, "VersionClass::Is3_0or3_1");
    go!(&only3_0, "VersionClass::EQ(Version::FCS3_0)");
    go!(&max_3_1, "VersionClass::LE(Version::FCS3_1)");

    let b = m.build();

    writeln!(file, "#[allow(clippy::unreadable_literal)]")?;
    writeln!(
        file,
        "static {i}: phf::Map<unicase::UniCase<&'static str>, {vt}> = {b};"
    )
}

fn write_meas_kw_map(file: &mut BufWriter<File>) -> io::Result<()> {
    let mut meas_map = phf_codegen::Map::new();
    let mut gate_set = phf_codegen::Set::new();
    let meas_map_ident = "MEAS_SUFFIX_MAP";
    let gate_set_ident = "GATE_SUFFIX_SET";
    let meas_value = "VersionClass";

    let any_version = [
        ("SCALE_KW_SUFFIX", "E", true),
        ("WAVELENGTH_KW_SUFFIX", "L", false),
        ("WIDTH_KW_SUFFIX", "B", false),
        ("FILTER_KW_SUFFIX", "F", true),
        ("POWER_KW_SUFFIX", "O", false),
        ("PERCENT_EMITTED_KW_SUFFIX", "P", true),
        ("RANGE_KW_SUFFIX", "R", true),
        ("LONGNAME_KW_SUFFIX", "S", true),
        ("DET_TYPE_KW_SUFFIX", "T", true),
        ("DET_VOLTAGE_KW_SUFFIX", "V", true),
        ("SHORTNAME_KW_SUFFIX", "N", true),
    ];

    let min_3_0 = [("GAIN_KW_SUFFIX", "G", false)];

    let min_3_1 = [
        ("DISPLAY_KW_SUFFIX", "D", false),
        ("CALIBRATION_KW_SUFFIX", "CALIBRATION", false),
    ];

    let min_3_2 = [
        ("FEATURE_KW_SUFFIX", "FEATURE", false),
        ("TYPE_KW_SUFFIX", "TYPE", false),
        ("DATATYPE_KW_SUFFIX", "DATATYPE", false),
        ("ANALYTE_KW_SUFFIX", "ANALYTE", false),
        ("TAG_KW_SUFFIX", "TAG", false),
        ("DET_NAME_KW_SUFFIX", "DET", false),
    ];

    macro_rules! go {
        ($pairs:expr, $class:expr) => {
            for (k, v, also_gate) in $pairs {
                writeln!(file, "pub(crate) const {k}: &str = \"{v}\";").unwrap();
                meas_map.entry(UniCase::ascii(v), $class);
                if also_gate {
                    gate_set.entry(UniCase::ascii(v));
                }
            }
        };
    }

    go!(any_version, "VersionClass::Any");
    go!(min_3_0, "VersionClass::GE(Version::FCS3_0)");
    go!(min_3_1, "VersionClass::GE(Version::FCS3_1)");
    go!(min_3_2, "VersionClass::GE(Version::FCS3_2)");

    let mb = meas_map.build();
    let gb = gate_set.build();

    writeln!(file, "#[allow(clippy::unreadable_literal)]")?;
    writeln!(
        file,
        "static {meas_map_ident}: phf::Map<UniCase<&'static str>, {meas_value}> = {mb};"
    )?;
    writeln!(file, "#[allow(clippy::unreadable_literal)]")?;
    writeln!(
        file,
        "static {gate_set_ident}: phf::Set<UniCase<&'static str>> = {gb};"
    )
}

fn main() {
    let path = Path::new(&env::var("OUT_DIR").unwrap()).join("kw_map.rs");
    let mut file = BufWriter::new(File::create(&path).unwrap());
    write_kw_map(&mut file).unwrap();
    write_meas_kw_map(&mut file).unwrap();
}

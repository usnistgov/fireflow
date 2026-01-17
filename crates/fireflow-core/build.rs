use std::env;
use std::fs::File;
use std::io::{self, BufWriter, Write as _};
use std::path::Path;
use unicase::Ascii;

fn write_kw_map(file: &mut BufWriter<File>) -> io::Result<()> {
    let mut m = phf_codegen::Map::new();
    let i = "KW_MAP";
    let vt = "RootKeywordClass";

    let special = [
        ("BYTEORD", "RootKeywordClass::Byteord"),
        ("MODE", "RootKeywordClass::Mode"),
        ("CYT", "RootKeywordClass::Cyt"),
        ("TOT", "RootKeywordClass::Tot"),
        ("BEGINANALYSIS", "RootKeywordClass::Beginanalysis"),
        ("BEGINDATA", "RootKeywordClass::Begindata"),
        ("BEGINSTEXT", "RootKeywordClass::Beginstext"),
        ("ENDANALYSIS", "RootKeywordClass::Endanalysis"),
        ("ENDDATA", "RootKeywordClass::Enddata"),
        ("ENDSTEXT", "RootKeywordClass::Endstext"),
        ("TIMESTEP", "RootKeywordClass::Timestep"),
    ];

    let any_version = [
        "ABRT", "BTIM", "CYTSN", "COM", "CELLS", "DATATYPE", "DATE", "ETIM", "EXP", "FIL",
        "GATING", "INST", "LOST", "NEXTDATA", "OP", "PAR", "PROJ", "SMNO", "SRC", "SYS", "TR",
    ];

    let min_3_1 = [
        "LAST_MODIFIER",
        "ORIGINALITY",
        "LAST_MODIFIED",
        "PLATEID",
        "PLATENAME",
        "WELLID",
        "SPILLOVER",
        "VOL",
    ];

    let min_3_2 = [
        "CARRIERID",
        "CARRIERTYPE",
        "LOCATIONID",
        "BEGINDATETIME",
        "ENDDATETIME",
        "UNSTAINEDCENTERS",
        "UNSTAINEDINFO",
        "FLOWRATE",
    ];

    let max_3_1 = ["GATE"];

    let is3_0or3_1 = ["CSMODE", "CSTOT", "CSVBITS"];

    let only3_0 = ["UNICODE", "COMP"];

    macro_rules! go {
        ($pairs:expr, $class:expr) => {
            for v in $pairs {
                writeln!(file, "pub(crate) const {v}_KW: &str = \"{v}\";")?;
                m.entry(Ascii::new(v), $class);
            }
        };
    }

    for (v, p) in special {
        m.entry(Ascii::new(v), p);
        writeln!(file, "pub(crate) const {v}_KW: &str = \"{v}\";")?;
    }

    go!(&any_version, "RootKeywordClass::OptAny");
    go!(&min_3_1, "RootKeywordClass::OptGE3_1");
    go!(&min_3_2, "RootKeywordClass::OptGE3_2");
    go!(&is3_0or3_1, "RootKeywordClass::OptEQ3_0or3_1");
    go!(&only3_0, "RootKeywordClass::OptEQ3_0");
    go!(&max_3_1, "RootKeywordClass::OptLE3_1");

    let b = m.build();

    writeln!(file, "#[allow(clippy::unreadable_literal)]")?;
    writeln!(
        file,
        "static {i}: phf::Map<unicase::Ascii<&'static str>, {vt}> = {b};"
    )
}

fn write_meas_kw_map(file: &mut BufWriter<File>) -> io::Result<()> {
    let mut meas_map = phf_codegen::Map::new();
    let mut gate_set = phf_codegen::Set::new();
    let meas_map_ident = "MEAS_SUFFIX_MAP";
    let gate_set_ident = "GATE_SUFFIX_SET";
    let meas_value = "MeasKeywordClass";

    let special = [
        ("SCALE_KW_SUFFIX", "E", true, "MeasKeywordClass::Scale"),
        (
            "WAVELENGTH_KW_SUFFIX",
            "L",
            false,
            "MeasKeywordClass::Wavelength",
        ),
        (
            "SHORTNAME_KW_SUFFIX",
            "N",
            true,
            "MeasKeywordClass::Shortname",
        ),
    ];

    let any_version = [
        ("WIDTH_KW_SUFFIX", "B", false),
        ("FILTER_KW_SUFFIX", "F", true),
        ("POWER_KW_SUFFIX", "O", false),
        ("PERCENT_EMITTED_KW_SUFFIX", "P", true),
        ("RANGE_KW_SUFFIX", "R", true),
        ("LONGNAME_KW_SUFFIX", "S", true),
        ("DET_TYPE_KW_SUFFIX", "T", true),
        ("DET_VOLTAGE_KW_SUFFIX", "V", true),
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
                meas_map.entry(Ascii::new(v), $class);
                if also_gate {
                    gate_set.entry(Ascii::new(v));
                }
            }
        };
    }

    go!(any_version, "MeasKeywordClass::OptAny");
    go!(min_3_0, "MeasKeywordClass::OptGE3_0");
    go!(min_3_1, "MeasKeywordClass::OptGE3_1");
    go!(min_3_2, "MeasKeywordClass::OptGE3_2");

    for (k, v, also_gate, var) in special {
        writeln!(file, "pub(crate) const {k}: &str = \"{v}\";").unwrap();
        meas_map.entry(Ascii::new(v), var);
        if also_gate {
            gate_set.entry(Ascii::new(v));
        }
    }

    let mb = meas_map.build();
    let gb = gate_set.build();

    writeln!(file, "#[allow(clippy::unreadable_literal)]")?;
    writeln!(
        file,
        "static {meas_map_ident}: phf::Map<Ascii<&'static str>, {meas_value}> = {mb};"
    )?;
    writeln!(file, "#[allow(clippy::unreadable_literal)]")?;
    writeln!(
        file,
        "static {gate_set_ident}: phf::Set<Ascii<&'static str>> = {gb};"
    )
}

fn main() {
    let path = Path::new(&env::var("OUT_DIR").unwrap()).join("kw_map.rs");
    let mut file = BufWriter::new(File::create(&path).unwrap());
    write_kw_map(&mut file).unwrap();
    write_meas_kw_map(&mut file).unwrap();
}

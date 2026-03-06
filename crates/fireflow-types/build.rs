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

    let write_kw = |f: &mut BufWriter<File>, v: &str, class: &str| -> io::Result<()> {
        writeln!(f, "pub const {v}: &str = \"${v}\";")?;
        writeln!(f, "pub const {v}_KW: &str = \"{v}\";")?;
        writeln!(
            f,
            "pub const {v}_VERS: VersionMembership = {class}.membership();"
        )?;
        Ok(())
    };

    macro_rules! go {
        ($pairs:expr, $class:expr) => {
            for v in $pairs {
                write_kw(file, v, $class)?;
                m.entry(Ascii::new(v), $class);
            }
        };
    }

    for (v, p) in special {
        write_kw(file, v, p)?;
        m.entry(Ascii::new(v), p);
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
        "pub static {i}: phf::Map<unicase::Ascii<&'static str>, {vt}> = {b};"
    )
}

fn write_meas_kw_map(file: &mut BufWriter<File>) -> io::Result<()> {
    let mut meas_map = phf_codegen::Map::new();
    let mut gate_set = phf_codegen::Set::new();
    let meas_map_ident = "MEAS_SUFFIX_MAP";
    let gate_set_ident = "GATE_SUFFIX_SET";
    let meas_value = "MeasKeywordClass";

    let special = [
        ("SCALE", "E", true, "MeasKeywordClass::Scale"),
        ("WAVELENGTH", "L", false, "MeasKeywordClass::Wavelength"),
        ("SHORTNAME", "N", true, "MeasKeywordClass::Shortname"),
    ];

    let any_version = [
        ("WIDTH", "B", false),
        ("FILTER", "F", true),
        ("POWER", "O", false),
        ("PERCENT_EMITTED", "P", true),
        ("RANGE", "R", true),
        ("LONGNAME", "S", true),
        ("DET_TYPE", "T", true),
        ("DET_VOLTAGE", "V", true),
    ];

    let min_3_0 = [("GAIN", "G", false)];

    let min_3_1 = [
        ("DISPLAY", "D", false),
        ("CALIBRATION", "CALIBRATION", false),
    ];

    let min_3_2 = [
        ("FEATURE", "FEATURE", false),
        ("TYPE", "TYPE", false),
        ("DATATYPE", "DATATYPE", false),
        ("ANALYTE", "ANALYTE", false),
        ("TAG", "TAG", false),
        ("DET_NAME", "DET", false),
    ];

    macro_rules! write_kw {
        ($k:expr, $v:expr, $class:expr) => {
            writeln!(file, "pub const PN{v}: &str = \"$Pn{v}\";", v = $v)?;
            writeln!(
                file,
                "pub const {k}_KW_SUFFIX: &str = \"{v}\";",
                k = $k,
                v = $v
            )?;
            writeln!(
                file,
                "pub const PN{v}_VERS: VersionMembership = {class}.membership();",
                v = $v,
                class = $class,
            )?;
        };
    }

    macro_rules! go_inner {
        ($k:expr, $v:expr, $also_gate:expr, $class:expr) => {{
            write_kw!($k, $v, $class);
            meas_map.entry(Ascii::new($v), $class);
            if $also_gate {
                writeln!(file, "pub const GM{v}: &str = \"$Gm{v}\";", v = $v)?;
                gate_set.entry(Ascii::new($v));
            }
        }};
    }

    macro_rules! go {
        ($pairs:expr, $class:expr) => {
            for (k, v, also_gate) in $pairs {
                go_inner!(k, v, also_gate, $class)
            }
        };
    }

    go!(any_version, "MeasKeywordClass::OptAny");
    go!(min_3_0, "MeasKeywordClass::OptGE3_0");
    go!(min_3_1, "MeasKeywordClass::OptGE3_1");
    go!(min_3_2, "MeasKeywordClass::OptGE3_2");

    for (k, v, also_gate, var) in special {
        go_inner!(k, v, also_gate, var);
    }

    let mb = meas_map.build();
    let gb = gate_set.build();

    writeln!(file, "#[allow(clippy::unreadable_literal)]")?;
    writeln!(
        file,
        "pub static {meas_map_ident}: phf::Map<Ascii<&'static str>, {meas_value}> = {mb};"
    )?;
    writeln!(file, "#[allow(clippy::unreadable_literal)]")?;
    writeln!(
        file,
        "pub static {gate_set_ident}: phf::Set<Ascii<&'static str>> = {gb};"
    )
}

fn main() {
    let path = Path::new(&env::var("OUT_DIR").unwrap()).join("kw_map.rs");
    let mut file = BufWriter::new(File::create(&path).unwrap());
    write_kw_map(&mut file).unwrap();
    write_meas_kw_map(&mut file).unwrap();
}
